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
	"fmt"
	"sort"
	"strings"
)

// CompositeAggregationConfig Composite聚合配置
type CompositeAggregationConfig struct {
	Size     int
	Sources  []CompositeSource
	AfterKey map[string]interface{} // 用于分页
}

// CompositeSource Composite聚合的源
type CompositeSource struct {
	Name  string
	Terms *CompositeTermsSource
}

// CompositeTermsSource Composite聚合的terms源
type CompositeTermsSource struct {
	Field         string
	MissingBucket bool
}

// parseCompositeAggregation 解析composite聚合
// ES格式: {"composite": {"size": 1000, "sources": [{"field_name": {"terms": {"field": "field_name", "missing_bucket": true}}}], "after": {...}}}
func (h *DocumentHandler) parseCompositeAggregation(config map[string]interface{}) (*CompositeAggregationConfig, error) {
	compositeAgg := &CompositeAggregationConfig{
		Size:    1000, // 默认大小
		Sources: make([]CompositeSource, 0),
	}

	// 解析size
	if sizeVal, ok := config["size"].(float64); ok {
		compositeAgg.Size = int(sizeVal)
	} else if sizeVal, ok := config["size"].(int); ok {
		compositeAgg.Size = sizeVal
	}

	// 解析after（用于分页）
	if afterRaw, ok := config["after"].(map[string]interface{}); ok {
		compositeAgg.AfterKey = afterRaw
	}

	// 解析sources
	sourcesRaw, ok := config["sources"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("composite aggregation requires a 'sources' parameter")
	}

	for _, sourceRaw := range sourcesRaw {
		sourceMap, ok := sourceRaw.(map[string]interface{})
		if !ok {
			continue
		}

		// 每个source是一个对象，key是source名称，value是配置
		for sourceName, sourceConfigRaw := range sourceMap {
			sourceConfig, ok := sourceConfigRaw.(map[string]interface{})
			if !ok {
				continue
			}

			// 检查是否有terms配置
			if termsRaw, ok := sourceConfig["terms"].(map[string]interface{}); ok {
				field, _ := termsRaw["field"].(string)
				missingBucket := false
				if mb, ok := termsRaw["missing_bucket"].(bool); ok {
					missingBucket = mb
				}

				compositeAgg.Sources = append(compositeAgg.Sources, CompositeSource{
					Name: sourceName,
					Terms: &CompositeTermsSource{
						Field:         field,
						MissingBucket: missingBucket,
					},
				})
			}
		}
	}

	return compositeAgg, nil
}

// CompositeKey 表示一个组合键
type CompositeKey struct {
	Values map[string]interface{}
	Count  int64
}

// CompositeKeyString 生成组合键的字符串表示（用于排序和比较）
func (ck *CompositeKey) String(sources []CompositeSource) string {
	var parts []string
	for _, source := range sources {
		val := ck.Values[source.Name]
		if val == nil {
			parts = append(parts, "\x00") // null 排在最前
		} else {
			parts = append(parts, fmt.Sprintf("%v", val))
		}
	}
	return strings.Join(parts, "\x01")
}

// CompareCompositeKeys 比较两个组合键（用于分页过滤）
// 返回: -1 if a < b, 0 if a == b, 1 if a > b
func CompareCompositeKeys(a, b map[string]interface{}, sources []CompositeSource) int {
	for _, source := range sources {
		aVal := a[source.Name]
		bVal := b[source.Name]

		// 处理 nil
		if aVal == nil && bVal == nil {
			continue
		}
		if aVal == nil {
			return -1
		}
		if bVal == nil {
			return 1
		}

		// 字符串比较
		aStr := fmt.Sprintf("%v", aVal)
		bStr := fmt.Sprintf("%v", bVal)
		if aStr < bStr {
			return -1
		}
		if aStr > bStr {
			return 1
		}
	}
	return 0
}

// SortCompositeKeys 对组合键进行排序
func SortCompositeKeys(keys []CompositeKey, sources []CompositeSource) {
	sort.Slice(keys, func(i, j int) bool {
		return CompareCompositeKeys(keys[i].Values, keys[j].Values, sources) < 0
	})
}
