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

package test

import (
	"fmt"
	"testing"

	"github.com/lscgzwd/tiggerdb/nested/mapping"
)

func TestNewNestedFieldMapping(t *testing.T) {
	nfm := mapping.NewNestedFieldMapping()

	if nfm.Type != "nested" {
		t.Errorf("Expected type 'nested', got '%s'", nfm.Type)
	}

	if nfm.Properties == nil {
		t.Error("Properties should not be nil")
	}

	if nfm.IncludeInParent == nil || *nfm.IncludeInParent != false {
		t.Error("IncludeInParent should default to false")
	}

	if nfm.IncludeInRoot == nil || *nfm.IncludeInRoot != true {
		t.Error("IncludeInRoot should default to true")
	}
}

func TestNestedFieldMapping_Validate(t *testing.T) {
	// 有效的映射
	nfm := mapping.NewNestedFieldMapping()
	nfm.Properties["field1"] = map[string]interface{}{
		"type": "text",
	}

	err := nfm.Validate()
	if err != nil {
		t.Errorf("Valid mapping should not return error: %v", err)
	}

	// 无效的类型
	nfm.Type = "invalid"
	err = nfm.Validate()
	if err == nil {
		t.Error("Invalid type should return error")
	}
	nfm.Type = "nested" // 恢复

	// nil属性
	nfm.Properties = nil
	err = nfm.Validate()
	if err == nil {
		t.Error("Nil properties should return error")
	}
}

func TestNestedFieldMapping_GetSetProperty(t *testing.T) {
	nfm := mapping.NewNestedFieldMapping()

	// 设置属性
	testValue := map[string]interface{}{
		"type": "keyword",
	}
	nfm.SetProperty("test_field", testValue)

	// 获取属性
	value, exists := nfm.GetProperty("test_field")
	if !exists {
		t.Error("Property should exist")
	}

	if value == nil {
		t.Error("Property value should not be nil")
	}

	// 获取不存在的属性
	_, exists = nfm.GetProperty("nonexistent")
	if exists {
		t.Error("Nonexistent property should not exist")
	}
}

func TestNestedFieldMapping_RemoveProperty(t *testing.T) {
	nfm := mapping.NewNestedFieldMapping()

	// 添加属性
	nfm.SetProperty("test_field", map[string]interface{}{"type": "text"})

	// 验证存在
	_, exists := nfm.GetProperty("test_field")
	if !exists {
		t.Error("Property should exist before removal")
	}

	// 移除属性
	nfm.RemoveProperty("test_field")

	// 验证不存在
	_, exists = nfm.GetProperty("test_field")
	if exists {
		t.Error("Property should not exist after removal")
	}
}

func TestNestedFieldMapping_Clone(t *testing.T) {
	original := mapping.NewNestedFieldMapping()
	original.SetProperty("field1", map[string]interface{}{
		"type": "text",
	})
	original.Analyzer = "standard"

	clone := original.Clone()

	// 验证克隆的独立性
	if clone == original {
		t.Error("Clone should be a different instance")
	}

	// 修改克隆不应该影响原对象
	clone.SetProperty("field2", map[string]interface{}{
		"type": "keyword",
	})

	_, exists := original.GetProperty("field2")
	if exists {
		t.Error("Original should not be affected by clone modification")
	}

	// 验证值相等
	if clone.Type != original.Type {
		t.Error("Clone type should equal original")
	}

	if clone.Analyzer != original.Analyzer {
		t.Error("Clone analyzer should equal original")
	}
}

func TestNestedMappingValidator_ValidateMapping(t *testing.T) {
	validator := mapping.NewNestedMappingValidator()

	// 有效的映射
	validMapping := mapping.NewNestedFieldMapping()
	validMapping.Properties["user"] = map[string]interface{}{
		"properties": map[string]interface{}{
			"name": map[string]interface{}{
				"type": "text",
			},
			"age": map[string]interface{}{
				"type": "long",
			},
		},
	}

	err := validator.ValidateMapping(validMapping)
	if err != nil {
		t.Errorf("Valid mapping should not return error: %v", err)
	}

	// 无效的映射 - 空类型
	invalidMapping := mapping.NewNestedFieldMapping()
	invalidMapping.Type = ""

	err = validator.ValidateMapping(invalidMapping)
	if err == nil {
		t.Error("Invalid mapping should return error")
	}
}

func TestNestedMappingValidator_ValidateMapping_DepthLimit(t *testing.T) {
	validator := mapping.NewNestedMappingValidator()

	// 创建深度超过限制的映射
	deepMapping := mapping.NewNestedFieldMapping()

	// 构建11层深度的嵌套
	current := deepMapping.Properties
	for i := 0; i < 11; i++ {
		levelName := fmt.Sprintf("level_%d", i)
		current[levelName] = map[string]interface{}{
			"type":       "nested",
			"properties": map[string]interface{}{},
		}
		current = current[levelName].(map[string]interface{})["properties"].(map[string]interface{})
	}

	err := validator.ValidateMapping(deepMapping)
	if err == nil {
		t.Error("Deep mapping should exceed depth limit")
	}
}

func TestNestedMappingConverter_ConvertToStandardMapping(t *testing.T) {
	converter := mapping.NewNestedMappingConverter()

	nestedMapping := mapping.NewNestedFieldMapping()
	nestedMapping.Properties["field1"] = map[string]interface{}{
		"type": "text",
	}

	standardMapping, err := converter.ConvertToStandardMapping(nestedMapping)
	if err != nil {
		t.Errorf("Conversion should not fail: %v", err)
	}

	if standardMapping["type"] != "nested" {
		t.Errorf("Standard mapping type should be 'nested', got '%v'", standardMapping["type"])
	}

	properties, exists := standardMapping["properties"]
	if !exists {
		t.Error("Standard mapping should have properties")
	}

	propsMap, ok := properties.(map[string]interface{})
	if !ok {
		t.Error("Properties should be a map")
	}

	if _, exists := propsMap["field1"]; !exists {
		t.Error("Converted mapping should contain field1")
	}
}

func TestNestedMappingConverter_ConvertFromStandardMapping(t *testing.T) {
	converter := mapping.NewNestedMappingConverter()

	standardMapping := map[string]interface{}{
		"type": "nested",
		"properties": map[string]interface{}{
			"field1": map[string]interface{}{
				"type": "text",
			},
		},
		"include_in_parent": false,
		"include_in_root":   true,
	}

	nestedMapping, err := converter.ConvertFromStandardMapping(standardMapping)
	if err != nil {
		t.Errorf("Conversion should not fail: %v", err)
	}

	if nestedMapping.Type != "nested" {
		t.Errorf("Nested mapping type should be 'nested', got '%s'", nestedMapping.Type)
	}

	if *nestedMapping.IncludeInParent != false {
		t.Error("IncludeInParent should be false")
	}

	if *nestedMapping.IncludeInRoot != true {
		t.Error("IncludeInRoot should be true")
	}
}

func TestNestedMappingConverter_FlattenNestedMapping(t *testing.T) {
	converter := mapping.NewNestedMappingConverter()

	nestedMapping := mapping.NewNestedFieldMapping()
	nestedMapping.Properties["user"] = map[string]interface{}{
		"type": "nested",
		"properties": map[string]interface{}{
			"name": map[string]interface{}{
				"type": "text",
			},
			"address": map[string]interface{}{
				"type": "nested",
				"properties": map[string]interface{}{
					"street": map[string]interface{}{
						"type": "text",
					},
				},
			},
		},
	}

	flattened, err := converter.FlattenNestedMapping(nestedMapping, "")
	if err != nil {
		t.Errorf("Flattening should not fail: %v", err)
	}

	// 验证扁平化结果
	expectedPaths := []string{"user", "user.name", "user.address", "user.address.street"}

	for _, path := range expectedPaths {
		if _, exists := flattened[path]; !exists {
			t.Errorf("Flattened mapping should contain path: %s", path)
		}
	}
}

func TestNestedMappingConverter_MergeNestedMappings(t *testing.T) {
	converter := mapping.NewNestedMappingConverter()

	mapping1 := mapping.NewNestedFieldMapping()
	mapping1.SetProperty("field1", map[string]interface{}{
		"type": "text",
	})

	mapping2 := mapping.NewNestedFieldMapping()
	mapping2.SetProperty("field2", map[string]interface{}{
		"type": "keyword",
	})

	merged, err := converter.MergeNestedMappings(mapping1, mapping2)
	if err != nil {
		t.Errorf("Merging should not fail: %v", err)
	}

	// 验证合并结果
	if _, exists := merged.GetProperty("field1"); !exists {
		t.Error("Merged mapping should contain field1")
	}

	if _, exists := merged.GetProperty("field2"); !exists {
		t.Error("Merged mapping should contain field2")
	}
}

func TestNestedMappingConverter_OptimizeMapping(t *testing.T) {
	converter := mapping.NewNestedMappingConverter()

	original := mapping.NewNestedFieldMapping()
	original.IncludeInParent = mapping.BoolPtr(false) // 默认值
	original.IncludeInRoot = mapping.BoolPtr(true)    // 默认值
	original.Dynamic = mapping.BoolPtr(true)          // 默认值
	original.Index = mapping.BoolPtr(true)            // 默认值
	original.Analyzer = ""                            // 空值
	original.SetProperty("field1", map[string]interface{}{
		"type":     "text",
		"store":    false, // 默认值
		"index":    true,  // 默认值
		"analyzer": "",    // 空值
	})

	optimized, err := converter.OptimizeMapping(original)
	if err != nil {
		t.Errorf("Optimization should not fail: %v", err)
	}

	// 验证优化结果
	if optimized.IncludeInParent != nil {
		t.Error("Default IncludeInParent should be removed")
	}

	if optimized.IncludeInRoot != nil {
		t.Error("Default IncludeInRoot should be removed")
	}

	if optimized.Dynamic != nil {
		t.Error("Default Dynamic should be removed")
	}

	if optimized.Index != nil {
		t.Error("Default Index should be removed")
	}
}
