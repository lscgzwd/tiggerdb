// Copyright (c) 2024 TigerDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handler

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// DocumentVersion 文档版本信息
type DocumentVersion struct {
	Version     int64     `json:"version"`      // 文档版本号，每次更新递增
	SeqNo       int64     `json:"seq_no"`       // 序列号，全局唯一，每次操作递增
	PrimaryTerm int64     `json:"primary_term"` // 主分片任期，用于协调跨分片写入
	UpdatedAt   time.Time `json:"updated_at"`   // 最后更新时间
}

// VersionManager 文档版本管理器
// 负责管理文档的版本信息（_version、_seq_no、_primary_term）
// 实现ES兼容的版本控制机制，支持乐观并发控制
type VersionManager struct {
	// 全局序列号生成器（原子操作，保证线程安全）
	globalSeqNo int64

	// 主分片任期（单节点模式下固定为1）
	primaryTerm int64

	// 文档版本存储：indexName -> docID -> DocumentVersion
	versions map[string]map[string]*DocumentVersion

	// 读写锁保护versions map
	mutex sync.RWMutex
}

// NewVersionManager 创建版本管理器
func NewVersionManager() *VersionManager {
	return &VersionManager{
		globalSeqNo: 0,
		primaryTerm: 1, // 单节点模式下固定为1
		versions:    make(map[string]map[string]*DocumentVersion),
	}
}

// GetVersion 获取文档版本信息
// 如果文档不存在，返回nil
func (vm *VersionManager) GetVersion(indexName, docID string) *DocumentVersion {
	vm.mutex.RLock()
	defer vm.mutex.RUnlock()

	indexVersions, exists := vm.versions[indexName]
	if !exists {
		return nil
	}

	version, exists := indexVersions[docID]
	if !exists {
		return nil
	}

	// 返回副本，避免外部修改
	return &DocumentVersion{
		Version:     version.Version,
		SeqNo:       version.SeqNo,
		PrimaryTerm: version.PrimaryTerm,
		UpdatedAt:   version.UpdatedAt,
	}
}

// IncrementVersion 递增文档版本（用于更新操作）
// 如果文档不存在，创建新版本（version=1）
// 返回新的版本信息
func (vm *VersionManager) IncrementVersion(indexName, docID string) *DocumentVersion {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	// 确保索引map存在
	if _, exists := vm.versions[indexName]; !exists {
		vm.versions[indexName] = make(map[string]*DocumentVersion)
	}

	// 获取或创建文档版本
	version, exists := vm.versions[indexName][docID]
	if !exists {
		// 新文档，版本从1开始
		version = &DocumentVersion{
			Version:     1,
			SeqNo:       atomic.AddInt64(&vm.globalSeqNo, 1),
			PrimaryTerm: vm.primaryTerm,
			UpdatedAt:   time.Now(),
		}
		vm.versions[indexName][docID] = version
	} else {
		// 更新文档，版本递增
		version.Version++
		version.SeqNo = atomic.AddInt64(&vm.globalSeqNo, 1)
		version.UpdatedAt = time.Now()
	}

	// 返回副本
	return &DocumentVersion{
		Version:     version.Version,
		SeqNo:       version.SeqNo,
		PrimaryTerm: version.PrimaryTerm,
		UpdatedAt:   version.UpdatedAt,
	}
}

// CreateVersion 创建新文档版本（用于创建操作）
// 总是创建version=1的新版本
// 返回新的版本信息
func (vm *VersionManager) CreateVersion(indexName, docID string) *DocumentVersion {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	// 确保索引map存在
	if _, exists := vm.versions[indexName]; !exists {
		vm.versions[indexName] = make(map[string]*DocumentVersion)
	}

	// 创建新版本
	version := &DocumentVersion{
		Version:     1,
		SeqNo:       atomic.AddInt64(&vm.globalSeqNo, 1),
		PrimaryTerm: vm.primaryTerm,
		UpdatedAt:   time.Now(),
	}
	vm.versions[indexName][docID] = version

	// 返回副本
	return &DocumentVersion{
		Version:     version.Version,
		SeqNo:       version.SeqNo,
		PrimaryTerm: version.PrimaryTerm,
		UpdatedAt:   version.UpdatedAt,
	}
}

// DeleteVersion 删除文档版本（用于删除操作）
// 返回删除前的版本信息（如果存在）
func (vm *VersionManager) DeleteVersion(indexName, docID string) *DocumentVersion {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	indexVersions, exists := vm.versions[indexName]
	if !exists {
		return nil
	}

	version, exists := indexVersions[docID]
	if !exists {
		return nil
	}

	// 删除版本信息
	delete(indexVersions, docID)

	// 如果索引下没有文档了，删除索引map
	if len(indexVersions) == 0 {
		delete(vm.versions, indexName)
	}

	// 返回删除前的版本信息副本
	return &DocumentVersion{
		Version:     version.Version,
		SeqNo:       version.SeqNo,
		PrimaryTerm: version.PrimaryTerm,
		UpdatedAt:   version.UpdatedAt,
	}
}

// CheckVersionConflict 检查版本冲突（用于乐观并发控制）
// 如果expectedVersion不为0，检查当前版本是否匹配
// 如果expectedSeqNo不为0，检查当前seq_no是否匹配
// 如果expectedPrimaryTerm不为0，检查当前primary_term是否匹配
// 返回冲突信息（如果存在）
func (vm *VersionManager) CheckVersionConflict(
	indexName, docID string,
	expectedVersion, expectedSeqNo, expectedPrimaryTerm int64,
) error {
	vm.mutex.RLock()
	defer vm.mutex.RUnlock()

	indexVersions, exists := vm.versions[indexName]
	if !exists {
		// 文档不存在，如果期望版本不为0，则冲突
		if expectedVersion != 0 {
			return &VersionConflictError{
				IndexName:       indexName,
				DocID:           docID,
				ExpectedVersion: expectedVersion,
				ActualVersion:   0,
				Reason:          "document not found",
			}
		}
		return nil
	}

	version, exists := indexVersions[docID]
	if !exists {
		// 文档不存在，如果期望版本不为0，则冲突
		if expectedVersion != 0 {
			return &VersionConflictError{
				IndexName:       indexName,
				DocID:           docID,
				ExpectedVersion: expectedVersion,
				ActualVersion:   0,
				Reason:          "document not found",
			}
		}
		return nil
	}

	// 检查版本号
	if expectedVersion != 0 && version.Version != expectedVersion {
		return &VersionConflictError{
			IndexName:       indexName,
			DocID:           docID,
			ExpectedVersion: expectedVersion,
			ActualVersion:   version.Version,
			Reason:          "version conflict",
		}
	}

	// 检查序列号
	if expectedSeqNo != 0 && version.SeqNo != expectedSeqNo {
		return &VersionConflictError{
			IndexName:     indexName,
			DocID:         docID,
			ExpectedSeqNo: expectedSeqNo,
			ActualSeqNo:   version.SeqNo,
			Reason:        "sequence number conflict",
		}
	}

	// 检查主分片任期
	if expectedPrimaryTerm != 0 && version.PrimaryTerm != expectedPrimaryTerm {
		return &VersionConflictError{
			IndexName:           indexName,
			DocID:               docID,
			ExpectedPrimaryTerm: expectedPrimaryTerm,
			ActualPrimaryTerm:   version.PrimaryTerm,
			Reason:              "primary term conflict",
		}
	}

	return nil
}

// VersionConflictError 版本冲突错误
type VersionConflictError struct {
	IndexName           string
	DocID               string
	ExpectedVersion     int64
	ActualVersion       int64
	ExpectedSeqNo       int64
	ActualSeqNo         int64
	ExpectedPrimaryTerm int64
	ActualPrimaryTerm   int64
	Reason              string
}

func (e *VersionConflictError) Error() string {
	if e.ExpectedVersion != 0 {
		return "version conflict: expected version " + strconv.FormatInt(e.ExpectedVersion, 10) +
			", actual version " + strconv.FormatInt(e.ActualVersion, 10) + " (" + e.Reason + ")"
	}
	if e.ExpectedSeqNo != 0 {
		return "sequence number conflict: expected seq_no " + strconv.FormatInt(e.ExpectedSeqNo, 10) +
			", actual seq_no " + strconv.FormatInt(e.ActualSeqNo, 10) + " (" + e.Reason + ")"
	}
	if e.ExpectedPrimaryTerm != 0 {
		return "primary term conflict: expected primary_term " + strconv.FormatInt(e.ExpectedPrimaryTerm, 10) +
			", actual primary_term " + strconv.FormatInt(e.ActualPrimaryTerm, 10) + " (" + e.Reason + ")"
	}
	return "version conflict: " + e.Reason
}
