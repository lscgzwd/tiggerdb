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
	"strings"
)

// NestedMappingValidator 嵌套映射验证器
type NestedMappingValidator struct {
	maxDepth    int
	maxFields   int
	allowCycles bool
}

// NewNestedMappingValidator 创建嵌套映射验证器
func NewNestedMappingValidator() *NestedMappingValidator {
	return &NestedMappingValidator{
		maxDepth:    10,    // 默认最大嵌套深度
		maxFields:   1000,  // 默认最大字段数量
		allowCycles: false, // 默认不允许循环引用
	}
}

// SetMaxDepth 设置最大嵌套深度
func (nmv *NestedMappingValidator) SetMaxDepth(depth int) {
	if depth > 0 {
		nmv.maxDepth = depth
	}
}

// SetMaxFields 设置最大字段数量
func (nmv *NestedMappingValidator) SetMaxFields(fields int) {
	if fields > 0 {
		nmv.maxFields = fields
	}
}

// SetAllowCycles 设置是否允许循环引用
func (nmv *NestedMappingValidator) SetAllowCycles(allow bool) {
	nmv.allowCycles = allow
}

// ValidateMapping 验证嵌套映射
func (nmv *NestedMappingValidator) ValidateMapping(mapping *NestedFieldMapping) error {
	if mapping == nil {
		return fmt.Errorf("mapping cannot be nil")
	}

	// 验证基本结构
	if err := nmv.validateBasicStructure(mapping); err != nil {
		return fmt.Errorf("basic structure validation failed: %w", err)
	}

	// 验证属性
	if err := nmv.validateProperties(mapping.Properties, 0); err != nil {
		return fmt.Errorf("properties validation failed: %w", err)
	}

	// 验证配置一致性
	if err := nmv.validateConfigurationConsistency(mapping); err != nil {
		return fmt.Errorf("configuration consistency validation failed: %w", err)
	}

	return nil
}

// validateBasicStructure 验证基本结构
func (nmv *NestedMappingValidator) validateBasicStructure(mapping *NestedFieldMapping) error {
	if mapping.Type != "nested" {
		return fmt.Errorf("invalid mapping type: expected 'nested', got '%s'", mapping.Type)
	}

	if mapping.Properties == nil {
		return fmt.Errorf("properties cannot be nil")
	}

	return nil
}

// validateProperties 验证属性
func (nmv *NestedMappingValidator) validateProperties(properties map[string]interface{}, currentDepth int) error {
	if currentDepth > nmv.maxDepth {
		return fmt.Errorf("nested depth %d exceeds maximum allowed depth %d", currentDepth, nmv.maxDepth)
	}

	fieldCount := 0
	visitedPaths := make(map[string]bool)

	for fieldName, fieldDef := range properties {
		fieldCount++
		if fieldCount > nmv.maxFields {
			return fmt.Errorf("field count %d exceeds maximum allowed fields %d", fieldCount, nmv.maxFields)
		}

		// 验证字段名
		if err := nmv.validateFieldName(fieldName); err != nil {
			return fmt.Errorf("invalid field name '%s': %w", fieldName, err)
		}

		// 验证字段定义
		if err := nmv.validateFieldDefinition(fieldName, fieldDef, currentDepth, visitedPaths); err != nil {
			return fmt.Errorf("invalid field definition for '%s': %w", fieldName, err)
		}
	}

	return nil
}

// validateFieldName 验证字段名
func (nmv *NestedMappingValidator) validateFieldName(fieldName string) error {
	if fieldName == "" {
		return fmt.Errorf("field name cannot be empty")
	}

	if len(fieldName) > 255 {
		return fmt.Errorf("field name too long (max 255 characters)")
	}

	// 检查是否包含非法字符
	if strings.ContainsAny(fieldName, "\t\n\r") {
		return fmt.Errorf("field name cannot contain whitespace characters")
	}

	// 检查是否以点开头（保留给嵌套路径）
	if strings.HasPrefix(fieldName, ".") {
		return fmt.Errorf("field name cannot start with dot")
	}

	return nil
}

// validateFieldDefinition 验证字段定义
func (nmv *NestedMappingValidator) validateFieldDefinition(fieldName string, fieldDef interface{}, currentDepth int, visitedPaths map[string]bool) error {
	fieldMap, ok := fieldDef.(map[string]interface{})
	if !ok {
		return fmt.Errorf("field definition must be an object")
	}

	fieldType, exists := fieldMap["type"]
	if !exists {
		return fmt.Errorf("field type is required")
	}

	typeStr, ok := fieldType.(string)
	if !ok {
		return fmt.Errorf("field type must be a string")
	}

	// 根据字段类型进行特定验证
	switch typeStr {
	case "nested":
		return nmv.validateNestedField(fieldName, fieldMap, currentDepth, visitedPaths)
	case "object":
		return nmv.validateObjectField(fieldName, fieldMap, currentDepth, visitedPaths)
	default:
		return nmv.validateSimpleField(fieldName, fieldMap, typeStr)
	}
}

// validateNestedField 验证嵌套字段
func (nmv *NestedMappingValidator) validateNestedField(fieldName string, fieldMap map[string]interface{}, currentDepth int, visitedPaths map[string]bool) error {
	// 检查循环引用
	path := fmt.Sprintf("nested:%s:%d", fieldName, currentDepth)
	if visitedPaths[path] && !nmv.allowCycles {
		return fmt.Errorf("circular reference detected in nested field '%s'", fieldName)
	}
	visitedPaths[path] = true
	defer func() { delete(visitedPaths, path) }()

	// 验证嵌套字段的properties
	nestedProps, exists := fieldMap["properties"]
	if !exists {
		return fmt.Errorf("nested field '%s' must have properties", fieldName)
	}

	nestedPropsMap, ok := nestedProps.(map[string]interface{})
	if !ok {
		return fmt.Errorf("nested field '%s' properties must be an object", fieldName)
	}

	// 递归验证嵌套属性
	return nmv.validateProperties(nestedPropsMap, currentDepth+1)
}

// validateObjectField 验证对象字段
func (nmv *NestedMappingValidator) validateObjectField(fieldName string, fieldMap map[string]interface{}, currentDepth int, visitedPaths map[string]bool) error {
	// 对象字段可以有properties，但不是强制的
	if props, exists := fieldMap["properties"]; exists {
		if propsMap, ok := props.(map[string]interface{}); ok {
			return nmv.validateProperties(propsMap, currentDepth+1)
		}
		return fmt.Errorf("object field '%s' properties must be an object", fieldName)
	}

	return nil
}

// validateSimpleField 验证简单字段
func (nmv *NestedMappingValidator) validateSimpleField(fieldName string, fieldMap map[string]interface{}, fieldType string) error {
	// 验证字段类型是否支持
	supportedTypes := []string{
		"text", "keyword", "long", "integer", "short", "byte",
		"double", "float", "boolean", "date", "date_nanos",
		"ip", "binary", "vector",
	}

	isSupported := false
	for _, supportedType := range supportedTypes {
		if fieldType == supportedType {
			isSupported = true
			break
		}
	}

	if !isSupported {
		return fmt.Errorf("unsupported field type '%s' for field '%s'", fieldType, fieldName)
	}

	// 类型特定的验证
	switch fieldType {
	case "vector":
		return nmv.validateVectorField(fieldName, fieldMap)
	case "date", "date_nanos":
		return nmv.validateDateField(fieldName, fieldMap)
	}

	return nil
}

// validateVectorField 验证向量字段
func (nmv *NestedMappingValidator) validateVectorField(fieldName string, fieldMap map[string]interface{}) error {
	// 检查向量维度
	if dims, exists := fieldMap["dims"]; exists {
		if dimsFloat, ok := dims.(float64); ok {
			if dimsFloat <= 0 || dimsFloat > 4096 {
				return fmt.Errorf("vector field '%s' dims must be between 1 and 4096", fieldName)
			}
		} else {
			return fmt.Errorf("vector field '%s' dims must be a number", fieldName)
		}
	}

	return nil
}

// validateDateField 验证日期字段
func (nmv *NestedMappingValidator) validateDateField(fieldName string, fieldMap map[string]interface{}) error {
	// 检查格式
	if format, exists := fieldMap["format"]; exists {
		if formatStr, ok := format.(string); ok {
			// 验证格式字符串的合法性（简化检查）
			if strings.Contains(formatStr, "yyyy") || strings.Contains(formatStr, "MM") || strings.Contains(formatStr, "dd") {
				// 看起来像是一个合理的日期格式
			} else {
				return fmt.Errorf("invalid date format '%s' for field '%s'", formatStr, fieldName)
			}
		}
	}

	return nil
}

// validateConfigurationConsistency 验证配置一致性
func (nmv *NestedMappingValidator) validateConfigurationConsistency(mapping *NestedFieldMapping) error {
	// 检查IncludeInParent和IncludeInRoot的组合
	if mapping.IncludeInParent != nil && *mapping.IncludeInParent &&
		mapping.IncludeInRoot != nil && !*mapping.IncludeInRoot {
		return fmt.Errorf("cannot include in parent but exclude from root")
	}

	// 检查索引和存储的组合
	if mapping.Index != nil && !*mapping.Index && mapping.DocValues != nil && *mapping.DocValues {
		return fmt.Errorf("cannot use doc_values when field is not indexed")
	}

	return nil
}

// ValidateMappingUpdate 验证映射更新
func (nmv *NestedMappingValidator) ValidateMappingUpdate(oldMapping, newMapping *NestedFieldMapping) error {
	// 验证新映射本身
	if err := nmv.ValidateMapping(newMapping); err != nil {
		return fmt.Errorf("new mapping validation failed: %w", err)
	}

	// 检查向后兼容性
	if err := nmv.checkBackwardCompatibility(oldMapping, newMapping); err != nil {
		return fmt.Errorf("backward compatibility check failed: %w", err)
	}

	return nil
}

// checkBackwardCompatibility 检查向后兼容性
func (nmv *NestedMappingValidator) checkBackwardCompatibility(oldMapping, newMapping *NestedFieldMapping) error {
	// 检查字段删除（可能破坏现有查询）
	oldFields := make(map[string]bool)
	for fieldName := range oldMapping.Properties {
		oldFields[fieldName] = true
	}

	for fieldName := range newMapping.Properties {
		if !oldFields[fieldName] {
			// 新增字段通常是兼容的
			continue
		}
		delete(oldFields, fieldName)
	}

	// 如果有字段被删除，检查是否安全删除
	for deletedField := range oldFields {
		if nmv.isCriticalField(deletedField, oldMapping) {
			return fmt.Errorf("cannot delete critical field '%s'", deletedField)
		}
	}

	return nil
}

// isCriticalField 检查是否为关键字段
func (nmv *NestedMappingValidator) isCriticalField(fieldName string, mapping *NestedFieldMapping) bool {
	// 这里可以根据业务逻辑定义哪些字段是关键的
	// 例如：ID字段、时间戳字段等

	criticalFields := []string{"id", "_id", "created_at", "updated_at"}
	for _, critical := range criticalFields {
		if fieldName == critical {
			return true
		}
	}

	return false
}
