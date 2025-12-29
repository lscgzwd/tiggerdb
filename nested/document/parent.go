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

package document

import (
	"fmt"
	"sync"
	"time"
)

// ParentDocumentManager 父文档管理器
type ParentDocumentManager struct {
	documents map[string]*ParentDocument
	mu        sync.RWMutex
}

// NewParentDocumentManager 创建父文档管理器
func NewParentDocumentManager() *ParentDocumentManager {
	return &ParentDocumentManager{
		documents: make(map[string]*ParentDocument),
	}
}

// ParentDocument 父文档结构
type ParentDocument struct {
	ID           string                       `json:"_id"`
	NestedDocs   map[string][]*NestedDocument `json:"_nested_docs"` // path -> documents
	RootFields   map[string]interface{}       `json:"_root_fields"` // 非嵌套字段
	Timestamp    time.Time                    `json:"_timestamp"`
	LastModified time.Time                    `json:"_last_modified"`

	// 统计信息
	NestedCount int `json:"_nested_count"` // 嵌套文档总数
	PathCount   int `json:"_path_count"`   // 不同路径的数量
}

// NewParentDocument 创建新的父文档
func NewParentDocument(id string) *ParentDocument {
	return &ParentDocument{
		ID:           id,
		NestedDocs:   make(map[string][]*NestedDocument),
		RootFields:   make(map[string]interface{}),
		Timestamp:    time.Now(),
		LastModified: time.Now(),
		NestedCount:  0,
		PathCount:    0,
	}
}

// AddNestedDocument 添加嵌套文档
func (pd *ParentDocument) AddNestedDocument(doc *NestedDocument) error {
	if doc.ParentID != pd.ID {
		return fmt.Errorf("document parent ID mismatch: expected %s, got %s", pd.ID, doc.ParentID)
	}

	if pd.NestedDocs[doc.Path] == nil {
		pd.NestedDocs[doc.Path] = make([]*NestedDocument, 0)
		pd.PathCount++
	}

	// 检查位置是否已存在
	for _, existingDoc := range pd.NestedDocs[doc.Path] {
		if existingDoc.Position == doc.Position {
			return fmt.Errorf("document at path %s position %d already exists", doc.Path, doc.Position)
		}
	}

	pd.NestedDocs[doc.Path] = append(pd.NestedDocs[doc.Path], doc)
	pd.NestedCount++
	pd.LastModified = time.Now()

	return nil
}

// RemoveNestedDocument 移除嵌套文档
func (pd *ParentDocument) RemoveNestedDocument(path string, position int) error {
	docs, exists := pd.NestedDocs[path]
	if !exists {
		return fmt.Errorf("path %s not found", path)
	}

	for i, doc := range docs {
		if doc.Position == position {
			// 移除文档
			pd.NestedDocs[path] = append(docs[:i], docs[i+1:]...)
			pd.NestedCount--

			// 如果该路径没有文档了，删除路径
			if len(pd.NestedDocs[path]) == 0 {
				delete(pd.NestedDocs, path)
				pd.PathCount--
			}

			pd.LastModified = time.Now()
			return nil
		}
	}

	return fmt.Errorf("document at path %s position %d not found", path, position)
}

// GetNestedDocuments 获取指定路径的嵌套文档
func (pd *ParentDocument) GetNestedDocuments(path string) []*NestedDocument {
	docs, exists := pd.NestedDocs[path]
	if !exists {
		return []*NestedDocument{}
	}
	return docs
}

// GetNestedDocument 获取指定路径和位置的嵌套文档
func (pd *ParentDocument) GetNestedDocument(path string, position int) *NestedDocument {
	docs := pd.GetNestedDocuments(path)
	for _, doc := range docs {
		if doc.Position == position {
			return doc
		}
	}
	return nil
}

// GetAllNestedDocuments 获取所有嵌套文档
func (pd *ParentDocument) GetAllNestedDocuments() []*NestedDocument {
	var allDocs []*NestedDocument
	for _, docs := range pd.NestedDocs {
		allDocs = append(allDocs, docs...)
	}
	return allDocs
}

// GetNestedPaths 获取所有嵌套路径
func (pd *ParentDocument) GetNestedPaths() []string {
	paths := make([]string, 0, len(pd.NestedDocs))
	for path := range pd.NestedDocs {
		paths = append(paths, path)
	}
	return paths
}

// SetRootField 设置根字段
func (pd *ParentDocument) SetRootField(name string, value interface{}) {
	pd.RootFields[name] = value
	pd.LastModified = time.Now()
}

// GetRootField 获取根字段
func (pd *ParentDocument) GetRootField(name string) (interface{}, bool) {
	value, exists := pd.RootFields[name]
	return value, exists
}

// RemoveRootField 移除根字段
func (pd *ParentDocument) RemoveRootField(name string) {
	delete(pd.RootFields, name)
	pd.LastModified = time.Now()
}

// GetRootFields 获取所有根字段
func (pd *ParentDocument) GetRootFields() map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range pd.RootFields {
		result[k] = v
	}
	return result
}

// HasNestedDocuments 检查是否有嵌套文档
func (pd *ParentDocument) HasNestedDocuments() bool {
	return pd.NestedCount > 0
}

// HasNestedPath 检查是否有指定路径的嵌套文档
func (pd *ParentDocument) HasNestedPath(path string) bool {
	_, exists := pd.NestedDocs[path]
	return exists
}

// GetNestedDocumentCount 获取嵌套文档数量
func (pd *ParentDocument) GetNestedDocumentCount() int {
	return pd.NestedCount
}

// GetPathCount 获取路径数量
func (pd *ParentDocument) GetPathCount() int {
	return pd.PathCount
}

// Validate 验证父文档
func (pd *ParentDocument) Validate() error {
	if pd.ID == "" {
		return fmt.Errorf("parent document ID cannot be empty")
	}

	// 验证嵌套文档
	for path, docs := range pd.NestedDocs {
		if path == "" {
			return fmt.Errorf("nested path cannot be empty")
		}

		positionSet := make(map[int]bool)
		for _, doc := range docs {
			if err := doc.Validate(); err != nil {
				return fmt.Errorf("invalid nested document: %w", err)
			}

			if doc.ParentID != pd.ID {
				return fmt.Errorf("nested document parent ID mismatch")
			}

			if doc.Path != path {
				return fmt.Errorf("nested document path mismatch")
			}

			if positionSet[doc.Position] {
				return fmt.Errorf("duplicate position %d in path %s", doc.Position, path)
			}
			positionSet[doc.Position] = true
		}
	}

	return nil
}

// Clone 克隆父文档
func (pd *ParentDocument) Clone() *ParentDocument {
	clone := *pd

	// 深拷贝嵌套文档
	clone.NestedDocs = make(map[string][]*NestedDocument)
	for path, docs := range pd.NestedDocs {
		clone.NestedDocs[path] = make([]*NestedDocument, len(docs))
		for i, doc := range docs {
			clone.NestedDocs[path][i] = doc.Clone()
		}
	}

	// 深拷贝根字段
	clone.RootFields = make(map[string]interface{})
	for k, v := range pd.RootFields {
		clone.RootFields[k] = v
	}

	return &clone
}

// GetSize 获取文档总大小
func (pd *ParentDocument) GetSize() int64 {
	size := int64(len(pd.ID))

	// 计算根字段大小
	for k, v := range pd.RootFields {
		size += int64(len(k))
		if str, ok := v.(string); ok {
			size += int64(len(str))
		} else {
			size += 16 // 其他类型的估算大小
		}
	}

	// 计算嵌套文档大小
	for _, docs := range pd.NestedDocs {
		for _, doc := range docs {
			size += doc.GetSize()
		}
	}

	return size
}

// UpdateTimestamp 更新时间戳
func (pd *ParentDocument) UpdateTimestamp() {
	pd.LastModified = time.Now()
}

// Register 注册父文档到管理器
func (pdm *ParentDocumentManager) Register(doc *ParentDocument) error {
	pdm.mu.Lock()
	defer pdm.mu.Unlock()

	if doc.ID == "" {
		return fmt.Errorf("document ID cannot be empty")
	}

	if err := doc.Validate(); err != nil {
		return fmt.Errorf("invalid document: %w", err)
	}

	pdm.documents[doc.ID] = doc
	return nil
}

// Unregister 从管理器中移除父文档
func (pdm *ParentDocumentManager) Unregister(id string) {
	pdm.mu.Lock()
	defer pdm.mu.Unlock()

	delete(pdm.documents, id)
}

// Get 获取父文档
func (pdm *ParentDocumentManager) Get(id string) (*ParentDocument, bool) {
	pdm.mu.RLock()
	defer pdm.mu.RUnlock()

	doc, exists := pdm.documents[id]
	return doc, exists
}

// List 列出所有父文档
func (pdm *ParentDocumentManager) List() []*ParentDocument {
	pdm.mu.RLock()
	defer pdm.mu.RUnlock()

	docs := make([]*ParentDocument, 0, len(pdm.documents))
	for _, doc := range pdm.documents {
		docs = append(docs, doc)
	}
	return docs
}

// Count 获取父文档数量
func (pdm *ParentDocumentManager) Count() int {
	pdm.mu.RLock()
	defer pdm.mu.RUnlock()

	return len(pdm.documents)
}

// Clear 清空所有父文档
func (pdm *ParentDocumentManager) Clear() {
	pdm.mu.Lock()
	defer pdm.mu.Unlock()

	pdm.documents = make(map[string]*ParentDocument)
}

// GetDocumentsByNestedPath 获取包含指定嵌套路径的文档
func (pdm *ParentDocumentManager) GetDocumentsByNestedPath(path string) []*ParentDocument {
	pdm.mu.RLock()
	defer pdm.mu.RUnlock()

	var result []*ParentDocument
	for _, doc := range pdm.documents {
		if doc.HasNestedPath(path) {
			result = append(result, doc)
		}
	}
	return result
}

// GetStatistics 获取统计信息
func (pdm *ParentDocumentManager) GetStatistics() *ParentDocumentStatistics {
	pdm.mu.RLock()
	defer pdm.mu.RUnlock()

	stats := &ParentDocumentStatistics{
		TotalDocuments:  0,
		TotalNestedDocs: 0,
		TotalPaths:      0,
		PathCounts:      make(map[string]int),
	}

	for _, doc := range pdm.documents {
		stats.TotalDocuments++
		stats.TotalNestedDocs += doc.GetNestedDocumentCount()
		stats.TotalPaths += doc.GetPathCount()

		for path := range doc.NestedDocs {
			stats.PathCounts[path]++
		}
	}

	return stats
}

// ParentDocumentStatistics 父文档统计信息
type ParentDocumentStatistics struct {
	TotalDocuments  int            `json:"total_documents"`
	TotalNestedDocs int            `json:"total_nested_docs"`
	TotalPaths      int            `json:"total_paths"`
	PathCounts      map[string]int `json:"path_counts"`
}

// CleanupExpiredDocuments 清理过期文档
func (pdm *ParentDocumentManager) CleanupExpiredDocuments(maxAge time.Duration) int {
	pdm.mu.Lock()
	defer pdm.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	removed := 0

	for id, doc := range pdm.documents {
		if doc.LastModified.Before(cutoff) {
			delete(pdm.documents, id)
			removed++
		}
	}

	return removed
}
