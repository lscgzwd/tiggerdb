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

package mapping

import (
	"fmt"

	"github.com/lscgzwd/tiggerdb/mapping"
)

// NestedFieldMapping 嵌套字段映射
type NestedFieldMapping struct {
	// 基础字段映射
	Type string `json:"type"` // 必须为"nested"

	// 嵌套对象属性定义
	Properties map[string]interface{} `json:"properties,omitempty"`

	// 是否包含在父文档中（用于搜索优化）
	IncludeInParent *bool `json:"include_in_parent,omitempty"`

	// 是否包含在根文档中（用于聚合优化）
	IncludeInRoot *bool `json:"include_in_root,omitempty"`

	// 动态映射策略
	Dynamic *bool `json:"dynamic,omitempty"`

	// 嵌套字段的分析器配置（如果需要）
	Analyzer string `json:"analyzer,omitempty"`

	// 存储选项
	Store bool `json:"store,omitempty"`

	// 索引选项
	Index *bool `json:"index,omitempty"`

	// 文档值选项
	DocValues *bool `json:"doc_values,omitempty"`
}

// NewNestedFieldMapping 创建新的嵌套字段映射
func NewNestedFieldMapping() *NestedFieldMapping {
	return &NestedFieldMapping{
		Type:            "nested",
		Properties:      make(map[string]interface{}),
		IncludeInParent: BoolPtr(false),
		IncludeInRoot:   BoolPtr(true),
		Dynamic:         BoolPtr(true),
		Store:           false,
		Index:           BoolPtr(true),
		DocValues:       BoolPtr(false),
	}
}

// Validate 验证嵌套字段映射
func (nfm *NestedFieldMapping) Validate() error {
	if nfm.Type != "nested" {
		return fmt.Errorf("nested field mapping type must be 'nested', got '%s'", nfm.Type)
	}

	if nfm.Properties == nil {
		return fmt.Errorf("nested field mapping must have properties")
	}

	// 验证嵌套深度限制（避免无限递归）
	if err := nfm.validateNestedDepth(nfm.Properties, 0, 10); err != nil {
		return fmt.Errorf("nested field validation failed: %w", err)
	}

	// 验证属性冲突
	if nfm.IncludeInParent != nil && *nfm.IncludeInParent &&
		nfm.IncludeInRoot != nil && !*nfm.IncludeInRoot {
		return fmt.Errorf("cannot include in parent but exclude from root")
	}

	return nil
}

// validateNestedDepth 验证嵌套深度
func (nfm *NestedFieldMapping) validateNestedDepth(properties map[string]interface{}, currentDepth, maxDepth int) error {
	if currentDepth >= maxDepth {
		return fmt.Errorf("nested depth exceeds maximum limit of %d", maxDepth)
	}

	for _, propValue := range properties {
		if propMap, ok := propValue.(map[string]interface{}); ok {
			if propType, exists := propMap["type"]; exists {
				if typeStr, ok := propType.(string); ok && typeStr == "nested" {
					// 递归验证嵌套字段
					if nestedProps, ok := propMap["properties"].(map[string]interface{}); ok {
						if err := nfm.validateNestedDepth(nestedProps, currentDepth+1, maxDepth); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

// GetProperty 获取属性定义
func (nfm *NestedFieldMapping) GetProperty(name string) (interface{}, bool) {
	if nfm.Properties == nil {
		return nil, false
	}
	value, exists := nfm.Properties[name]
	return value, exists
}

// SetProperty 设置属性定义
func (nfm *NestedFieldMapping) SetProperty(name string, value interface{}) {
	if nfm.Properties == nil {
		nfm.Properties = make(map[string]interface{})
	}
	nfm.Properties[name] = value
}

// RemoveProperty 移除属性定义
func (nfm *NestedFieldMapping) RemoveProperty(name string) {
	if nfm.Properties != nil {
		delete(nfm.Properties, name)
	}
}

// ListProperties 列出所有属性
func (nfm *NestedFieldMapping) ListProperties() map[string]interface{} {
	if nfm.Properties == nil {
		return make(map[string]interface{})
	}

	// 返回副本以避免外部修改
	result := make(map[string]interface{})
	for k, v := range nfm.Properties {
		result[k] = v
	}
	return result
}

// GetNestedPath 获取嵌套路径（用于构建完整的字段路径）
func (nfm *NestedFieldMapping) GetNestedPath(parentPath string, fieldName string) string {
	if parentPath == "" {
		return fieldName
	}
	return parentPath + "." + fieldName
}

// IsIncludedInParent 检查是否包含在父文档中
func (nfm *NestedFieldMapping) IsIncludedInParent() bool {
	return nfm.IncludeInParent != nil && *nfm.IncludeInParent
}

// IsIncludedInRoot 检查是否包含在根文档中
func (nfm *NestedFieldMapping) IsIncludedInRoot() bool {
	return nfm.IncludeInRoot == nil || *nfm.IncludeInRoot
}

// IsDynamic 检查是否支持动态映射
func (nfm *NestedFieldMapping) IsDynamic() bool {
	return nfm.Dynamic == nil || *nfm.Dynamic
}

// ShouldIndex 检查是否应该索引
func (nfm *NestedFieldMapping) ShouldIndex() bool {
	return nfm.Index == nil || *nfm.Index
}

// ShouldStore 检查是否应该存储
func (nfm *NestedFieldMapping) ShouldStore() bool {
	return nfm.Store
}

// ShouldUseDocValues 检查是否应该使用文档值
func (nfm *NestedFieldMapping) ShouldUseDocValues() bool {
	return nfm.DocValues != nil && *nfm.DocValues
}

// Clone 克隆嵌套字段映射
func (nfm *NestedFieldMapping) Clone() *NestedFieldMapping {
	clone := *nfm

	// 深拷贝Properties
	if nfm.Properties != nil {
		clone.Properties = make(map[string]interface{})
		for k, v := range nfm.Properties {
			clone.Properties[k] = v // 这里假设值是不可变的或者外部不会修改
		}
	}

	// 深拷贝指针类型字段
	if nfm.IncludeInParent != nil {
		clone.IncludeInParent = BoolPtr(*nfm.IncludeInParent)
	}
	if nfm.IncludeInRoot != nil {
		clone.IncludeInRoot = BoolPtr(*nfm.IncludeInRoot)
	}
	if nfm.Dynamic != nil {
		clone.Dynamic = BoolPtr(*nfm.Dynamic)
	}
	if nfm.Index != nil {
		clone.Index = BoolPtr(*nfm.Index)
	}
	if nfm.DocValues != nil {
		clone.DocValues = BoolPtr(*nfm.DocValues)
	}

	return &clone
}

// ToStandardMapping 转换为标准字段映射（用于兼容性）
func (nfm *NestedFieldMapping) ToStandardMapping() *mapping.FieldMapping {
	// 创建一个文本字段映射作为嵌套字段的表示
	fieldMapping := mapping.NewTextFieldMapping()

	// 设置基本属性
	fieldMapping.Store = nfm.ShouldStore()
	fieldMapping.Index = nfm.ShouldIndex()
	fieldMapping.DocValues = nfm.ShouldUseDocValues()

	if nfm.Analyzer != "" {
		fieldMapping.Analyzer = nfm.Analyzer
	}

	return fieldMapping
}

// BoolPtr 创建布尔指针的辅助函数
func BoolPtr(b bool) *bool {
	return &b
}

// StringPtr 创建字符串指针的辅助函数
func StringPtr(s string) *string {
	return &s
}
