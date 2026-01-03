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
	"strings"

	"github.com/lscgzwd/tiggerdb/logger"
)

// extractCopyToConfig 从mapping中提取copy_to配置
// 返回一个map：源字段名 -> 目标字段名列表
func extractCopyToConfig(mapping map[string]interface{}) map[string][]string {
	copyToMap := make(map[string][]string)

	// 递归处理properties
	var processProperties func(props map[string]interface{}, fieldPath string)
	processProperties = func(props map[string]interface{}, fieldPath string) {
		for fieldName, fieldDef := range props {
			fieldMap, ok := fieldDef.(map[string]interface{})
			if !ok {
				continue
			}

			// 构建完整字段路径
			var fullFieldPath string
			if fieldPath == "" {
				fullFieldPath = fieldName
			} else {
				fullFieldPath = fieldPath + "." + fieldName
			}

			// 检查是否有copy_to配置
			if copyTo, ok := fieldMap["copy_to"]; ok {
				var copyToFields []string
				if copyToStr, ok := copyTo.(string); ok {
					copyToFields = []string{copyToStr}
				} else if copyToArr, ok := copyTo.([]interface{}); ok {
					copyToFields = make([]string, 0, len(copyToArr))
					for _, item := range copyToArr {
						if str, ok := item.(string); ok {
							copyToFields = append(copyToFields, str)
						}
					}
				}
				if len(copyToFields) > 0 {
					copyToMap[fullFieldPath] = copyToFields
					logger.Debug("Found copy_to config: %s -> %v", fullFieldPath, copyToFields)
				}
			}

			// 递归处理嵌套字段和multi-fields
			if nestedProps, ok := fieldMap["properties"].(map[string]interface{}); ok {
				processProperties(nestedProps, fullFieldPath)
			}
			if multiFields, ok := fieldMap["fields"].(map[string]interface{}); ok {
				processProperties(multiFields, fullFieldPath)
			}
		}
	}

	// 从mapping中提取properties
	if properties, ok := mapping["properties"].(map[string]interface{}); ok {
		processProperties(properties, "")
	}

	return copyToMap
}

// applyCopyTo 应用copy_to规则到文档数据
// copyToMap: 源字段名 -> 目标字段名列表
// docData: 文档数据（会被修改）
func applyCopyTo(copyToMap map[string][]string, docData map[string]interface{}) {
	if len(copyToMap) == 0 {
		return
	}

	// 递归获取字段值（支持嵌套字段）
	var getFieldValue func(data map[string]interface{}, fieldPath string) interface{}
	getFieldValue = func(data map[string]interface{}, fieldPath string) interface{} {
		parts := strings.Split(fieldPath, ".")
		current := data
		for i, part := range parts {
			if i == len(parts)-1 {
				// 最后一个部分，返回字段值
				return current[part]
			}
			// 中间部分，继续深入
			if next, ok := current[part].(map[string]interface{}); ok {
				current = next
			} else {
				return nil
			}
		}
		return nil
	}

	// 递归设置字段值（支持嵌套字段）
	var setFieldValue func(data map[string]interface{}, fieldPath string, value interface{})
	setFieldValue = func(data map[string]interface{}, fieldPath string, value interface{}) {
		parts := strings.Split(fieldPath, ".")
		current := data
		for i, part := range parts {
			if i == len(parts)-1 {
				// 最后一个部分，设置字段值
				// 如果目标字段已存在且是数组，追加；否则创建数组
				if existing, ok := current[part]; ok {
					if arr, ok := existing.([]interface{}); ok {
						// 已存在数组，追加值
						current[part] = append(arr, value)
					} else {
						// 已存在非数组值，转换为数组
						current[part] = []interface{}{existing, value}
					}
				} else {
					// 字段不存在，直接设置
					current[part] = value
				}
			} else {
				// 中间部分，确保路径存在
				if next, ok := current[part].(map[string]interface{}); ok {
					current = next
				} else {
					// 路径不存在，创建
					current[part] = make(map[string]interface{})
					current = current[part].(map[string]interface{})
				}
			}
		}
	}

	// 应用所有copy_to规则
	for sourceField, targetFields := range copyToMap {
		// 获取源字段值
		sourceValue := getFieldValue(docData, sourceField)
		if sourceValue == nil {
			continue
		}

		// 复制到所有目标字段
		for _, targetField := range targetFields {
			setFieldValue(docData, targetField, sourceValue)
		}
	}
}
