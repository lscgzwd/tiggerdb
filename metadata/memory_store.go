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

package metadata

import (
	"sync"
)

// MemoryMetadataStore 基于内存的元数据存储实现
type MemoryMetadataStore struct {
	config    *MetadataStoreConfig
	indexes   map[string]*IndexMetadata
	tables    map[string]map[string]*TableMetadata // indexName -> tableName -> metadata
	version   int64
	mu        sync.RWMutex
	versionMu sync.RWMutex
}

// NewMemoryMetadataStore 创建基于内存的元数据存储
func NewMemoryMetadataStore(config *MetadataStoreConfig) (*MemoryMetadataStore, error) {
	if config == nil {
		config = DefaultMetadataStoreConfig()
	}

	if config.StorageType != "memory" {
		return nil, &UnsupportedStorageTypeError{Type: config.StorageType}
	}

	return &MemoryMetadataStore{
		config:  config,
		indexes: make(map[string]*IndexMetadata),
		tables:  make(map[string]map[string]*TableMetadata),
		version: 1,
	}, nil
}

// SaveIndexMetadata 保存索引元数据
func (mms *MemoryMetadataStore) SaveIndexMetadata(indexName string, metadata *IndexMetadata) error {
	mms.mu.Lock()
	defer mms.mu.Unlock()

	mms.indexes[indexName] = metadata
	mms.incrementVersion()

	return nil
}

// GetIndexMetadata 获取索引元数据
func (mms *MemoryMetadataStore) GetIndexMetadata(indexName string) (*IndexMetadata, error) {
	mms.mu.RLock()
	defer mms.mu.RUnlock()

	if metadata, exists := mms.indexes[indexName]; exists {
		return metadata, nil
	}

	return nil, &MetadataNotFoundError{
		ResourceType: "index",
		ResourceName: indexName,
	}
}

// DeleteIndexMetadata 删除索引元数据
func (mms *MemoryMetadataStore) DeleteIndexMetadata(indexName string) error {
	mms.mu.Lock()
	defer mms.mu.Unlock()

	delete(mms.indexes, indexName)
	delete(mms.tables, indexName)
	mms.incrementVersion()

	return nil
}

// ListIndexMetadata 列出所有索引元数据
func (mms *MemoryMetadataStore) ListIndexMetadata() ([]*IndexMetadata, error) {
	mms.mu.RLock()
	defer mms.mu.RUnlock()

	var result []*IndexMetadata
	for _, metadata := range mms.indexes {
		result = append(result, metadata)
	}

	return result, nil
}

// SaveTableMetadata 保存表元数据
func (mms *MemoryMetadataStore) SaveTableMetadata(indexName, tableName string, metadata *TableMetadata) error {
	mms.mu.Lock()
	defer mms.mu.Unlock()

	if mms.tables[indexName] == nil {
		mms.tables[indexName] = make(map[string]*TableMetadata)
	}

	mms.tables[indexName][tableName] = metadata
	mms.incrementVersion()

	return nil
}

// GetTableMetadata 获取表元数据
func (mms *MemoryMetadataStore) GetTableMetadata(indexName, tableName string) (*TableMetadata, error) {
	mms.mu.RLock()
	defer mms.mu.RUnlock()

	if indexTables, exists := mms.tables[indexName]; exists {
		if metadata, exists := indexTables[tableName]; exists {
			return metadata, nil
		}
	}

	return nil, &MetadataNotFoundError{
		ResourceType: "table",
		ResourceName: indexName + "/" + tableName,
	}
}

// DeleteTableMetadata 删除表元数据
func (mms *MemoryMetadataStore) DeleteTableMetadata(indexName, tableName string) error {
	mms.mu.Lock()
	defer mms.mu.Unlock()

	if indexTables, exists := mms.tables[indexName]; exists {
		delete(indexTables, tableName)
	}
	mms.incrementVersion()

	return nil
}

// ListTableMetadata 列出指定索引的所有表元数据
func (mms *MemoryMetadataStore) ListTableMetadata(indexName string) ([]*TableMetadata, error) {
	mms.mu.RLock()
	defer mms.mu.RUnlock()

	var result []*TableMetadata
	if indexTables, exists := mms.tables[indexName]; exists {
		for _, metadata := range indexTables {
			result = append(result, metadata)
		}
	}

	return result, nil
}

// GetLatestVersion 获取最新版本
func (mms *MemoryMetadataStore) GetLatestVersion() (int64, error) {
	mms.versionMu.RLock()
	defer mms.versionMu.RUnlock()

	return mms.version, nil
}

// CreateSnapshot 创建快照（内存存储的快照只是版本标记）
func (mms *MemoryMetadataStore) CreateSnapshot(version int64) error {
	// 内存存储不支持持久化快照，这里只是记录版本
	mms.versionMu.Lock()
	mms.version = version
	mms.versionMu.Unlock()

	return nil
}

// RestoreSnapshot 恢复快照（内存存储不支持恢复）
func (mms *MemoryMetadataStore) RestoreSnapshot(version int64) error {
	// 内存存储不支持恢复快照
	return &UnsupportedOperationError{
		Operation: "RestoreSnapshot",
		Reason:    "memory store does not support snapshot restoration",
	}
}

// Close 关闭存储
func (mms *MemoryMetadataStore) Close() error {
	mms.mu.Lock()
	defer mms.mu.Unlock()

	// 清空所有数据
	mms.indexes = make(map[string]*IndexMetadata)
	mms.tables = make(map[string]map[string]*TableMetadata)
	mms.version = 1

	return nil
}

// incrementVersion 递增版本号
func (mms *MemoryMetadataStore) incrementVersion() {
	mms.versionMu.Lock()
	mms.version++
	mms.versionMu.Unlock()
}
