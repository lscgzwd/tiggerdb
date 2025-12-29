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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// NestedDocument 嵌套文档结构
type NestedDocument struct {
	// 文档标识
	ID       string `json:"_id"`
	ParentID string `json:"_parent"`
	Path     string `json:"_path"`     // 嵌套路径，如 "user.addresses"
	Position int    `json:"_position"` // 在数组中的位置

	// 文档内容
	Fields map[string]interface{} `json:"_fields"`

	// 元数据
	NestedPath []string  `json:"_nested_path"` // 完整的嵌套路径数组
	Timestamp  time.Time `json:"_timestamp"`   // 创建时间戳

	// 关联信息
	RootDocumentID string `json:"_root_id"` // 根文档ID
}

// NewNestedDocument 创建新的嵌套文档
func NewNestedDocument(parentID, path string, position int, fields map[string]interface{}) *NestedDocument {
	nestedPath := strings.Split(path, ".")

	doc := &NestedDocument{
		ID:             generateNestedDocumentID(parentID, path, position),
		ParentID:       parentID,
		Path:           path,
		Position:       position,
		Fields:         make(map[string]interface{}),
		NestedPath:     nestedPath,
		Timestamp:      time.Now(),
		RootDocumentID: parentID, // 假设parentID就是根文档ID
	}

	// 深拷贝字段
	for k, v := range fields {
		doc.Fields[k] = v
	}

	return doc
}

// generateNestedDocumentID 生成嵌套文档ID
func generateNestedDocumentID(parentID, path string, position int) string {
	return fmt.Sprintf("%s#%s#%d", parentID, path, position)
}

// ParseNestedDocumentID 解析嵌套文档ID
func ParseNestedDocumentID(nestedID string) (parentID, path string, position int, err error) {
	parts := strings.Split(nestedID, "#")
	if len(parts) != 3 {
		return "", "", 0, fmt.Errorf("invalid nested document ID format: %s", nestedID)
	}

	parentID = parts[0]
	path = parts[1]

	// 解析位置
	if pos, err := strconv.Atoi(parts[2]); err != nil {
		return "", "", 0, fmt.Errorf("invalid position in nested document ID: %s", parts[2])
	} else {
		position = pos
	}

	return parentID, path, position, nil
}

// Validate 验证嵌套文档
func (nd *NestedDocument) Validate() error {
	if nd.ID == "" {
		return fmt.Errorf("nested document ID cannot be empty")
	}

	if nd.ParentID == "" {
		return fmt.Errorf("parent ID cannot be empty")
	}

	if nd.Path == "" {
		return fmt.Errorf("path cannot be empty")
	}

	if nd.Position < 0 {
		return fmt.Errorf("position cannot be negative")
	}

	if nd.Fields == nil {
		return fmt.Errorf("fields cannot be nil")
	}

	// 验证ID格式
	parentID, path, position, err := ParseNestedDocumentID(nd.ID)
	if err != nil {
		return fmt.Errorf("invalid document ID: %w", err)
	}

	if parentID != nd.ParentID {
		return fmt.Errorf("parent ID mismatch in document ID")
	}

	if path != nd.Path {
		return fmt.Errorf("path mismatch in document ID")
	}

	if position != nd.Position {
		return fmt.Errorf("position mismatch in document ID")
	}

	return nil
}

// GetField 获取字段值
func (nd *NestedDocument) GetField(name string) (interface{}, bool) {
	value, exists := nd.Fields[name]
	return value, exists
}

// SetField 设置字段值
func (nd *NestedDocument) SetField(name string, value interface{}) {
	if nd.Fields == nil {
		nd.Fields = make(map[string]interface{})
	}
	nd.Fields[name] = value
}

// RemoveField 移除字段
func (nd *NestedDocument) RemoveField(name string) {
	if nd.Fields != nil {
		delete(nd.Fields, name)
	}
}

// ListFields 列出所有字段
func (nd *NestedDocument) ListFields() map[string]interface{} {
	if nd.Fields == nil {
		return make(map[string]interface{})
	}

	// 返回副本
	result := make(map[string]interface{})
	for k, v := range nd.Fields {
		result[k] = v
	}
	return result
}

// HasField 检查是否包含字段
func (nd *NestedDocument) HasField(name string) bool {
	if nd.Fields == nil {
		return false
	}
	_, exists := nd.Fields[name]
	return exists
}

// GetNestedLevel 获取嵌套级别
func (nd *NestedDocument) GetNestedLevel() int {
	return len(nd.NestedPath)
}

// IsRootLevel 检查是否为根级别嵌套
func (nd *NestedDocument) IsRootLevel() bool {
	return nd.GetNestedLevel() == 1
}

// GetParentPath 获取父路径
func (nd *NestedDocument) GetParentPath() string {
	if len(nd.NestedPath) <= 1 {
		return ""
	}
	return strings.Join(nd.NestedPath[:len(nd.NestedPath)-1], ".")
}

// GetChildPaths 获取可能的子路径
func (nd *NestedDocument) GetChildPaths() []string {
	var childPaths []string
	currentPath := nd.Path

	// 为每个字段生成可能的子路径
	for fieldName := range nd.Fields {
		childPaths = append(childPaths, currentPath+"."+fieldName)
	}

	return childPaths
}

// Clone 克隆嵌套文档
func (nd *NestedDocument) Clone() *NestedDocument {
	clone := *nd

	// 深拷贝Fields
	if nd.Fields != nil {
		clone.Fields = make(map[string]interface{})
		for k, v := range nd.Fields {
			clone.Fields[k] = v
		}
	}

	// 深拷贝NestedPath
	if nd.NestedPath != nil {
		clone.NestedPath = make([]string, len(nd.NestedPath))
		copy(clone.NestedPath, nd.NestedPath)
	}

	return &clone
}

// ToJSON 转换为JSON
func (nd *NestedDocument) ToJSON() ([]byte, error) {
	return json.Marshal(nd)
}

// FromJSON 从JSON创建
func FromJSON(data []byte) (*NestedDocument, error) {
	var doc NestedDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}

	if err := doc.Validate(); err != nil {
		return nil, fmt.Errorf("invalid nested document: %w", err)
	}

	return &doc, nil
}

// Merge 合并另一个嵌套文档
func (nd *NestedDocument) Merge(other *NestedDocument) error {
	if other == nil {
		return nil
	}

	// 验证文档兼容性
	if nd.ParentID != other.ParentID {
		return fmt.Errorf("cannot merge documents with different parent IDs")
	}

	if nd.Path != other.Path {
		return fmt.Errorf("cannot merge documents with different paths")
	}

	if nd.Position != other.Position {
		return fmt.Errorf("cannot merge documents with different positions")
	}

	// 合并字段
	for key, value := range other.Fields {
		nd.Fields[key] = value
	}

	// 更新时间戳
	nd.Timestamp = time.Now()

	return nil
}

// GetSize 获取文档大小（近似值）
func (nd *NestedDocument) GetSize() int64 {
	size := int64(len(nd.ID) + len(nd.ParentID) + len(nd.Path) + len(nd.RootDocumentID))

	// 估算字段大小
	for k, v := range nd.Fields {
		size += int64(len(k))
		if str, ok := v.(string); ok {
			size += int64(len(str))
		} else {
			// 其他类型粗略估算
			size += 16
		}
	}

	return size
}

// IsExpired 检查文档是否过期
func (nd *NestedDocument) IsExpired(maxAge time.Duration) bool {
	return time.Since(nd.Timestamp) > maxAge
}

// UpdateTimestamp 更新时间戳
func (nd *NestedDocument) UpdateTimestamp() {
	nd.Timestamp = time.Now()
}

// NestedDocumentCollection 嵌套文档集合
type NestedDocumentCollection struct {
	Documents []*NestedDocument `json:"documents"`
	ParentID  string            `json:"parent_id"`
	Path      string            `json:"path"`
}

// NewNestedDocumentCollection 创建嵌套文档集合
func NewNestedDocumentCollection(parentID, path string) *NestedDocumentCollection {
	return &NestedDocumentCollection{
		Documents: make([]*NestedDocument, 0),
		ParentID:  parentID,
		Path:      path,
	}
}

// AddDocument 添加文档
func (ndc *NestedDocumentCollection) AddDocument(doc *NestedDocument) error {
	if doc.ParentID != ndc.ParentID {
		return fmt.Errorf("document parent ID mismatch")
	}

	if doc.Path != ndc.Path {
		return fmt.Errorf("document path mismatch")
	}

	ndc.Documents = append(ndc.Documents, doc)
	return nil
}

// RemoveDocument 移除文档
func (ndc *NestedDocumentCollection) RemoveDocument(position int) {
	if position >= 0 && position < len(ndc.Documents) {
		ndc.Documents = append(ndc.Documents[:position], ndc.Documents[position+1:]...)
	}
}

// GetDocument 获取文档
func (ndc *NestedDocumentCollection) GetDocument(position int) *NestedDocument {
	if position >= 0 && position < len(ndc.Documents) {
		return ndc.Documents[position]
	}
	return nil
}

// Size 获取集合大小
func (ndc *NestedDocumentCollection) Size() int {
	return len(ndc.Documents)
}

// IsEmpty 检查是否为空
func (ndc *NestedDocumentCollection) IsEmpty() bool {
	return len(ndc.Documents) == 0
}

// Clear 清空集合
func (ndc *NestedDocumentCollection) Clear() {
	ndc.Documents = make([]*NestedDocument, 0)
}

// SortByPosition 按位置排序
func (ndc *NestedDocumentCollection) SortByPosition() {
	// 简单的冒泡排序
	for i := 0; i < len(ndc.Documents)-1; i++ {
		for j := 0; j < len(ndc.Documents)-i-1; j++ {
			if ndc.Documents[j].Position > ndc.Documents[j+1].Position {
				ndc.Documents[j], ndc.Documents[j+1] = ndc.Documents[j+1], ndc.Documents[j]
			}
		}
	}
}

// FindByPosition 按位置查找
func (ndc *NestedDocumentCollection) FindByPosition(position int) *NestedDocument {
	for _, doc := range ndc.Documents {
		if doc.Position == position {
			return doc
		}
	}
	return nil
}

// GetPositions 获取所有位置
func (ndc *NestedDocumentCollection) GetPositions() []int {
	positions := make([]int, len(ndc.Documents))
	for i, doc := range ndc.Documents {
		positions[i] = doc.Position
	}
	return positions
}
