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
	"github.com/lscgzwd/tiggerdb/nested/document"
)

// NestedDocumentHelper 嵌套文档处理辅助工具
// 提供统一的嵌套文档处理逻辑，供多个handler复用
type NestedDocumentHelper struct{}

// NewNestedDocumentHelper 创建新的嵌套文档处理辅助工具
func NewNestedDocumentHelper() *NestedDocumentHelper {
	return &NestedDocumentHelper{}
}

// ProcessNestedDocuments 处理嵌套文档
// 从文档中提取嵌套字段，创建NestedDocument结构
func (h *NestedDocumentHelper) ProcessNestedDocuments(parentID string, docBody map[string]interface{}) (map[string]interface{}, []*document.NestedDocument, error) {
	docData := make(map[string]interface{})
	nestedDocs := make([]*document.NestedDocument, 0)

	// 遍历文档字段，查找嵌套结构
	for fieldName, fieldValue := range docBody {
		// 跳过ES元数据字段
		if fieldName == "_id" || fieldName == "_version" || fieldName == "_source" {
			continue
		}

		// 检查是否为数组（可能是嵌套文档数组）
		if arr, ok := fieldValue.([]interface{}); ok {
			// 检查数组元素是否为对象（嵌套文档）
			if len(arr) > 0 {
				if _, ok := arr[0].(map[string]interface{}); ok {
					// 这是一个嵌套文档数组
					for position, elem := range arr {
						if elemMap, ok := elem.(map[string]interface{}); ok {
							nestedDoc := document.NewNestedDocument(
								parentID,
								fieldName,
								position,
								elemMap,
							)
							nestedDocs = append(nestedDocs, nestedDoc)
						}
					}
					// 在docData中保留字段，但标记为嵌套
					docData[fieldName] = fieldValue
					continue
				}
			}
		}

		// 检查是否为对象（可能是嵌套对象）
		if obj, ok := fieldValue.(map[string]interface{}); ok {
			// 递归处理嵌套对象
			_, nested, err := h.ProcessNestedDocuments(parentID, obj)
			if err != nil {
				return nil, nil, err
			}
			nestedDocs = append(nestedDocs, nested...)
			docData[fieldName] = fieldValue
			continue
		}

		// 普通字段
		docData[fieldName] = fieldValue
	}

	return docData, nestedDocs, nil
}
