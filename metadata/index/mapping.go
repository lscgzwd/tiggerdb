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

package index

import (
	"fmt"
)

// CreateIndex 创建索引
func (imm *IndexMetadataManager) CreateIndex(indexName string, settings map[string]interface{}, mappings map[string]interface{}) error {
	if indexName == "" {
		return fmt.Errorf("index name cannot be empty")
	}

	// 验证索引名称
	if err := imm.validateIndexName(indexName); err != nil {
		return err
	}

	// 验证映射
	if err := imm.validateMappings(mappings); err != nil {
		return err
	}

	// 这里应该实现索引的持久化存储逻辑
	// 暂时返回成功
	return nil
}

// GetIndex 获取索引信息
func (imm *IndexMetadataManager) GetIndex(indexName string) (map[string]interface{}, error) {
	if indexName == "" {
		return nil, fmt.Errorf("index name cannot be empty")
	}

	// 这里应该实现从持久化存储获取索引信息的逻辑
	// 暂时返回模拟数据
	return map[string]interface{}{
		"index": map[string]interface{}{
			"name":    indexName,
			"status":  "active",
			"created": "2024-01-01T00:00:00Z",
		},
	}, nil
}

// DeleteIndex 删除索引
func (imm *IndexMetadataManager) DeleteIndex(indexName string) error {
	if indexName == "" {
		return fmt.Errorf("index name cannot be empty")
	}

	// 这里应该实现索引的删除逻辑
	// 暂时返回成功
	return nil
}

// GetMapping 获取索引映射
func (imm *IndexMetadataManager) GetMapping(indexName string) (map[string]interface{}, error) {
	if indexName == "" {
		return nil, fmt.Errorf("index name cannot be empty")
	}

	// 这里应该实现从持久化存储获取映射信息的逻辑
	// 暂时返回模拟数据
	return map[string]interface{}{
		"properties": map[string]interface{}{
			"field1": map[string]interface{}{
				"type": "text",
			},
		},
	}, nil
}

// UpdateMapping 更新索引映射
func (imm *IndexMetadataManager) UpdateMapping(indexName string, mappings map[string]interface{}) error {
	if indexName == "" {
		return fmt.Errorf("index name cannot be empty")
	}

	// 验证映射
	if err := imm.validateMappings(mappings); err != nil {
		return err
	}

	// 这里应该实现映射的更新逻辑
	// 暂时返回成功
	return nil
}

// ListIndices 列出所有索引
func (imm *IndexMetadataManager) ListIndices() ([]string, error) {
	// 这里应该实现列出所有索引的逻辑
	// 暂时返回空列表
	return []string{}, nil
}

// IndexExists 检查索引是否存在
func (imm *IndexMetadataManager) IndexExists(indexName string) bool {
	if indexName == "" {
		return false
	}

	// 这里应该实现检查索引是否存在的逻辑
	// 暂时总是返回true
	return true
}

// validateIndexName 验证索引名称
func (imm *IndexMetadataManager) validateIndexName(name string) error {
	if len(name) > 100 {
		return fmt.Errorf("index name too long (max 100 characters)")
	}

	// 检查字符合法性
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-') {
			return fmt.Errorf("index name contains invalid character: %c", r)
		}
	}

	return nil
}

// validateMappings 验证映射配置
func (imm *IndexMetadataManager) validateMappings(mappings map[string]interface{}) error {
	if mappings == nil {
		return nil
	}

	properties, ok := mappings["properties"]
	if !ok {
		return nil // 没有properties是允许的
	}

	props, ok := properties.(map[string]interface{})
	if !ok {
		return fmt.Errorf("properties must be an object")
	}

	// 验证每个字段的映射
	for fieldName, fieldMapping := range props {
		if err := imm.validateFieldMapping(fieldName, fieldMapping); err != nil {
			return fmt.Errorf("invalid mapping for field %s: %w", fieldName, err)
		}
	}

	return nil
}

// validateFieldMapping 验证字段映射
func (imm *IndexMetadataManager) validateFieldMapping(fieldName string, fieldMapping interface{}) error {
	fieldMap, ok := fieldMapping.(map[string]interface{})
	if !ok {
		return fmt.Errorf("field mapping must be an object")
	}

	fieldType, ok := fieldMap["type"]
	if !ok {
		return fmt.Errorf("field type is required")
	}

	typeStr, ok := fieldType.(string)
	if !ok {
		return fmt.Errorf("field type must be a string")
	}

	// 验证字段类型
	validTypes := []string{
		"text", "keyword", "long", "integer", "short", "byte",
		"double", "float", "boolean", "date", "binary", "object",
		"nested", "geo_point", "geo_shape", "join", "percolator",
	}

	valid := false
	for _, validType := range validTypes {
		if typeStr == validType {
			valid = true
			break
		}
	}

	if !valid {
		return fmt.Errorf("unsupported field type: %s", typeStr)
	}

	return nil
}
