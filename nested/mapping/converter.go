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
)

// NestedMappingConverter 嵌套映射转换器
type NestedMappingConverter struct {
	validator *NestedMappingValidator
}

// NewNestedMappingConverter 创建嵌套映射转换器
func NewNestedMappingConverter() *NestedMappingConverter {
	return &NestedMappingConverter{
		validator: NewNestedMappingValidator(),
	}
}

// ConvertToStandardMapping 将嵌套映射转换为标准映射
func (nmc *NestedMappingConverter) ConvertToStandardMapping(nestedMapping *NestedFieldMapping) (map[string]interface{}, error) {
	if err := nmc.validator.ValidateMapping(nestedMapping); err != nil {
		return nil, fmt.Errorf("invalid nested mapping: %w", err)
	}

	// 创建标准ES映射
	standardMapping := make(map[string]interface{})

	// 设置类型
	standardMapping["type"] = "nested"

	// 转换属性
	if len(nestedMapping.Properties) > 0 {
		properties := make(map[string]interface{})
		for fieldName, fieldDef := range nestedMapping.Properties {
			convertedField, err := nmc.convertFieldDefinition(fieldDef)
			if err != nil {
				return nil, fmt.Errorf("failed to convert field '%s': %w", fieldName, err)
			}
			properties[fieldName] = convertedField
		}
		standardMapping["properties"] = properties
	}

	// 设置其他配置
	if nestedMapping.IncludeInParent != nil {
		standardMapping["include_in_parent"] = *nestedMapping.IncludeInParent
	}
	if nestedMapping.IncludeInRoot != nil {
		standardMapping["include_in_root"] = *nestedMapping.IncludeInRoot
	}
	if nestedMapping.Dynamic != nil {
		standardMapping["dynamic"] = *nestedMapping.Dynamic
	}

	return standardMapping, nil
}

// ConvertFromStandardMapping 从标准映射转换为嵌套映射
func (nmc *NestedMappingConverter) ConvertFromStandardMapping(standardMapping map[string]interface{}) (*NestedFieldMapping, error) {
	// 验证类型
	if mappingType, ok := standardMapping["type"].(string); !ok || mappingType != "nested" {
		return nil, fmt.Errorf("not a nested mapping")
	}

	nestedMapping := NewNestedFieldMapping()

	// 转换属性
	if properties, ok := standardMapping["properties"].(map[string]interface{}); ok {
		nestedMapping.Properties = properties
	}

	// 转换配置
	if includeInParent, ok := standardMapping["include_in_parent"].(bool); ok {
		nestedMapping.IncludeInParent = &includeInParent
	}
	if includeInRoot, ok := standardMapping["include_in_root"].(bool); ok {
		nestedMapping.IncludeInRoot = &includeInRoot
	}
	if dynamic, ok := standardMapping["dynamic"].(bool); ok {
		nestedMapping.Dynamic = &dynamic
	}

	// 验证转换后的映射
	if err := nmc.validator.ValidateMapping(nestedMapping); err != nil {
		return nil, fmt.Errorf("converted mapping validation failed: %w", err)
	}

	return nestedMapping, nil
}

// convertFieldDefinition 转换字段定义
func (nmc *NestedMappingConverter) convertFieldDefinition(fieldDef interface{}) (interface{}, error) {
	fieldMap, ok := fieldDef.(map[string]interface{})
	if !ok {
		return fieldDef, nil // 保持原样
	}

	// 深拷贝字段定义
	convertedField := make(map[string]interface{})
	for k, v := range fieldMap {
		convertedField[k] = v
	}

	// 如果是嵌套类型，递归转换
	if fieldType, ok := fieldMap["type"].(string); ok && fieldType == "nested" {
		if nestedProps, ok := fieldMap["properties"].(map[string]interface{}); ok {
			convertedProps := make(map[string]interface{})
			for propName, propDef := range nestedProps {
				convertedProp, err := nmc.convertFieldDefinition(propDef)
				if err != nil {
					return nil, fmt.Errorf("failed to convert nested property '%s': %w", propName, err)
				}
				convertedProps[propName] = convertedProp
			}
			convertedField["properties"] = convertedProps
		}
	}

	return convertedField, nil
}

// FlattenNestedMapping 扁平化嵌套映射（用于索引优化）
func (nmc *NestedMappingConverter) FlattenNestedMapping(nestedMapping *NestedFieldMapping, parentPath string) (map[string]interface{}, error) {
	if err := nmc.validator.ValidateMapping(nestedMapping); err != nil {
		return nil, fmt.Errorf("invalid nested mapping: %w", err)
	}

	flattened := make(map[string]interface{})

	// 递归扁平化所有字段
	err := nmc.flattenProperties(nestedMapping.Properties, parentPath, flattened)
	if err != nil {
		return nil, fmt.Errorf("failed to flatten properties: %w", err)
	}

	return flattened, nil
}

// flattenProperties 递归扁平化属性
func (nmc *NestedMappingConverter) flattenProperties(properties map[string]interface{}, parentPath string, flattened map[string]interface{}) error {
	for fieldName, fieldDef := range properties {
		fullPath := fieldName
		if parentPath != "" {
			fullPath = parentPath + "." + fieldName
		}

		fieldMap, ok := fieldDef.(map[string]interface{})
		if !ok {
			// 简单值，直接添加到扁平化结果
			flattened[fullPath] = fieldDef
			continue
		}

		// 检查是否为嵌套类型
		if fieldType, ok := fieldMap["type"].(string); ok && fieldType == "nested" {
			// 嵌套类型，递归处理
			if nestedProps, ok := fieldMap["properties"].(map[string]interface{}); ok {
				if err := nmc.flattenProperties(nestedProps, fullPath, flattened); err != nil {
					return err
				}
			}
		} else {
			// 非嵌套类型，直接添加到结果
			flattened[fullPath] = fieldDef
		}
	}

	return nil
}

// MergeNestedMappings 合并多个嵌套映射
func (nmc *NestedMappingConverter) MergeNestedMappings(mappings ...*NestedFieldMapping) (*NestedFieldMapping, error) {
	if len(mappings) == 0 {
		return NewNestedFieldMapping(), nil
	}

	if len(mappings) == 1 {
		return mappings[0].Clone(), nil
	}

	// 从第一个映射开始
	result := mappings[0].Clone()

	// 合并其他映射
	for i := 1; i < len(mappings); i++ {
		if err := nmc.mergeSingleMapping(result, mappings[i]); err != nil {
			return nil, fmt.Errorf("failed to merge mapping %d: %w", i, err)
		}
	}

	return result, nil
}

// mergeSingleMapping 合并单个映射
func (nmc *NestedMappingConverter) mergeSingleMapping(target, source *NestedFieldMapping) error {
	// 合并属性
	for fieldName, fieldDef := range source.Properties {
		if existingDef, exists := target.Properties[fieldName]; exists {
			// 字段已存在，检查冲突
			if err := nmc.checkFieldConflict(fieldName, existingDef, fieldDef); err != nil {
				return err
			}
		} else {
			// 新字段，直接添加
			target.Properties[fieldName] = fieldDef
		}
	}

	// 合并配置（使用source的配置覆盖target）
	if source.IncludeInParent != nil {
		target.IncludeInParent = source.IncludeInParent
	}
	if source.IncludeInRoot != nil {
		target.IncludeInRoot = source.IncludeInRoot
	}
	if source.Dynamic != nil {
		target.Dynamic = source.Dynamic
	}
	if source.Analyzer != "" {
		target.Analyzer = source.Analyzer
	}

	return nil
}

// checkFieldConflict 检查字段冲突
func (nmc *NestedMappingConverter) checkFieldConflict(fieldName string, existing, new interface{}) error {
	existingMap, existingOk := existing.(map[string]interface{})
	newMap, newOk := new.(map[string]interface{})

	// 如果都不是对象类型，则认为是冲突
	if !existingOk || !newOk {
		return fmt.Errorf("field '%s' type conflict: cannot merge different types", fieldName)
	}

	// 检查类型是否相同
	existingType, existingHasType := existingMap["type"]
	newType, newHasType := newMap["type"]

	if existingHasType && newHasType {
		if existingType != newType {
			return fmt.Errorf("field '%s' type conflict: %v vs %v", fieldName, existingType, newType)
		}
	}

	// 如果都是嵌套类型，递归检查
	if existingType == "nested" && newType == "nested" {
		existingProps, existingOk := existingMap["properties"].(map[string]interface{})
		newProps, newOk := newMap["properties"].(map[string]interface{})

		if existingOk && newOk {
			return nmc.checkPropertiesConflict(fieldName, existingProps, newProps)
		}
	}

	return nil
}

// checkPropertiesConflict 检查属性冲突
func (nmc *NestedMappingConverter) checkPropertiesConflict(parentPath string, existing, new map[string]interface{}) error {
	for fieldName, newDef := range new {
		if existingDef, exists := existing[fieldName]; exists {
			fullPath := parentPath + "." + fieldName
			if err := nmc.checkFieldConflict(fullPath, existingDef, newDef); err != nil {
				return err
			}
		}
	}
	return nil
}

// ExtractNestedPaths 提取嵌套路径
func (nmc *NestedMappingConverter) ExtractNestedPaths(mapping *NestedFieldMapping) ([]string, error) {
	if err := nmc.validator.ValidateMapping(mapping); err != nil {
		return nil, fmt.Errorf("invalid mapping: %w", err)
	}

	var paths []string
	err := nmc.extractPathsRecursive(mapping.Properties, "", &paths)
	if err != nil {
		return nil, fmt.Errorf("failed to extract paths: %w", err)
	}

	return paths, nil
}

// extractPathsRecursive 递归提取路径
func (nmc *NestedMappingConverter) extractPathsRecursive(properties map[string]interface{}, currentPath string, paths *[]string) error {
	for fieldName, fieldDef := range properties {
		fieldPath := fieldName
		if currentPath != "" {
			fieldPath = currentPath + "." + fieldName
		}

		fieldMap, ok := fieldDef.(map[string]interface{})
		if !ok {
			continue
		}

		fieldType, hasType := fieldMap["type"]
		if hasType && fieldType == "nested" {
			*paths = append(*paths, fieldPath)
			// 递归处理嵌套字段
			if nestedProps, ok := fieldMap["properties"].(map[string]interface{}); ok {
				if err := nmc.extractPathsRecursive(nestedProps, fieldPath, paths); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// OptimizeMapping 优化嵌套映射
func (nmc *NestedMappingConverter) OptimizeMapping(mapping *NestedFieldMapping) (*NestedFieldMapping, error) {
	if err := nmc.validator.ValidateMapping(mapping); err != nil {
		return nil, fmt.Errorf("invalid mapping: %w", err)
	}

	optimized := mapping.Clone()

	// 优化策略：
	// 1. 移除未使用的配置
	// 2. 简化嵌套结构
	// 3. 压缩字段定义

	// 移除默认值配置
	nmc.removeDefaultConfigurations(optimized)

	// 优化字段定义
	if err := nmc.optimizeFieldDefinitions(optimized.Properties); err != nil {
		return nil, fmt.Errorf("failed to optimize field definitions: %w", err)
	}

	return optimized, nil
}

// removeDefaultConfigurations 移除默认配置
func (nmc *NestedMappingConverter) removeDefaultConfigurations(mapping *NestedFieldMapping) {
	// 移除默认的IncludeInParent（false）
	if mapping.IncludeInParent != nil && !*mapping.IncludeInParent {
		mapping.IncludeInParent = nil
	}

	// 移除默认的IncludeInRoot（true）
	if mapping.IncludeInRoot != nil && *mapping.IncludeInRoot {
		mapping.IncludeInRoot = nil
	}

	// 移除默认的Dynamic（true）
	if mapping.Dynamic != nil && *mapping.Dynamic {
		mapping.Dynamic = nil
	}

	// 移除默认的Index（true）
	if mapping.Index != nil && *mapping.Index {
		mapping.Index = nil
	}

	// 移除空的分析器
	if mapping.Analyzer == "" {
		mapping.Analyzer = ""
	}
}

// optimizeFieldDefinitions 优化字段定义
func (nmc *NestedMappingConverter) optimizeFieldDefinitions(properties map[string]interface{}) error {
	for fieldName, fieldDef := range properties {
		fieldMap, ok := fieldDef.(map[string]interface{})
		if !ok {
			continue
		}

		// 递归优化嵌套字段
		if fieldType, ok := fieldMap["type"].(string); ok && fieldType == "nested" {
			if nestedProps, ok := fieldMap["properties"].(map[string]interface{}); ok {
				if err := nmc.optimizeFieldDefinitions(nestedProps); err != nil {
					return fmt.Errorf("failed to optimize nested field '%s': %w", fieldName, err)
				}
			}
		}

		// 移除字段级别的默认配置
		nmc.removeFieldDefaultConfigurations(fieldMap)
	}

	return nil
}

// removeFieldDefaultConfigurations 移除字段默认配置
func (nmc *NestedMappingConverter) removeFieldDefaultConfigurations(fieldMap map[string]interface{}) {
	// 移除默认的store（false）
	if store, ok := fieldMap["store"].(bool); ok && !store {
		delete(fieldMap, "store")
	}

	// 移除默认的doc_values（false for nested）
	if docValues, ok := fieldMap["doc_values"].(bool); ok && !docValues {
		delete(fieldMap, "doc_values")
	}

	// 移除空的分析器
	if analyzer, ok := fieldMap["analyzer"].(string); ok && analyzer == "" {
		delete(fieldMap, "analyzer")
	}
}
