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
	"time"

	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/logger"
	"github.com/lscgzwd/tiggerdb/search/query"
)

// buildNestedAggregationsForBucket 为bucket构建嵌套聚合
func (h *DocumentHandler) buildNestedAggregationsForBucket(
	parentAggName string,
	bucketKey interface{},
	subAggs map[string]map[string]interface{},
	idx bleve.Index,
	bucketQuery query.Query,
) map[string]interface{} {
	if len(subAggs) == 0 {
		return nil
	}

	logger.Debug("buildNestedAggregationsForBucket: parentAgg=[%s], bucketKey=%v, subAggs count=%d", parentAggName, bucketKey, len(subAggs))

	// P2-1: 使用结构体封装返回值
	parsedSubAggs, err := h.parseAggregations(subAggs)
	if err != nil {
		logger.Warn("Failed to parse nested aggregations for bucket [%s]: %v", parentAggName, err)
		return nil
	}

	if parsedSubAggs == nil {
		return nil
	}

	metricsCount := 0
	if parsedSubAggs.MetricsInfo != nil {
		metricsCount = len(parsedSubAggs.MetricsInfo.Aggregations)
	}
	compositeCount := 0
	if parsedSubAggs.CompositeInfo != nil {
		compositeCount = len(parsedSubAggs.CompositeInfo.Aggregations)
	}
	nestedCount := 0
	if parsedSubAggs.NestedInfo != nil {
		nestedCount = len(parsedSubAggs.NestedInfo.SubAggregations)
	}
	logger.Debug("buildNestedAggregationsForBucket: parsed facets=%d, metrics=%d, composite=%d, nested=%d", len(parsedSubAggs.Facets), metricsCount, compositeCount, nestedCount)

	// 创建搜索请求
	searchReq := bleve.NewSearchRequest(bucketQuery)
	searchReq.Size = 0 // 不需要返回文档，只需要聚合结果
	if len(parsedSubAggs.Facets) > 0 {
		searchReq.Facets = parsedSubAggs.Facets
	}

	// 执行搜索
	searchResult, err := idx.Search(searchReq)
	if err != nil {
		logger.Warn("Failed to execute nested aggregation search for bucket [%s]: %v", parentAggName, err)
		return nil
	}

	// 构建聚合响应
	result := make(map[string]interface{})

	// 处理bucket聚合（terms, range, date_range）
	if len(searchResult.Facets) > 0 {
		facetAggs := h.buildAggregations(searchResult.Facets, parsedSubAggs.CompositeInfo, parsedSubAggs.NestedInfo, parsedSubAggs.TopHitsInfo, parsedSubAggs.NestedFieldInfo, idx, bucketQuery)
		for k, v := range facetAggs {
			result[k] = v
		}
	}

	// 处理top_hits聚合
	if parsedSubAggs.TopHitsInfo != nil && len(parsedSubAggs.TopHitsInfo.Aggregations) > 0 {
		for topHitsName, topHitsConfig := range parsedSubAggs.TopHitsInfo.Aggregations {
			topHitsResult := h.buildTopHitsAggregation(topHitsConfig, idx, bucketQuery)
			if topHitsResult != nil {
				result[topHitsName] = topHitsResult
			}
		}
	}

	// 处理nested字段聚合
	if parsedSubAggs.NestedFieldInfo != nil && len(parsedSubAggs.NestedFieldInfo.Aggregations) > 0 {
		for nestedFieldName, nestedFieldConfig := range parsedSubAggs.NestedFieldInfo.Aggregations {
			logger.Debug("buildNestedAggregationsForBucket: processing nested field aggregation [%s], path=[%s]", nestedFieldName, nestedFieldConfig.Path)
			// 在bucket查询范围内执行nested字段聚合
			nestedFieldAggs := h.buildNestedFieldAggregations(&NestedFieldAggregationInfo{
				Aggregations: map[string]*NestedFieldAggregationConfig{
					nestedFieldName: nestedFieldConfig,
				},
			}, idx, bucketQuery)
			if nestedFieldResult, ok := nestedFieldAggs[nestedFieldName]; ok {
				result[nestedFieldName] = nestedFieldResult
			}
		}
	}

	// 处理metrics聚合
	if parsedSubAggs.MetricsInfo != nil && len(parsedSubAggs.MetricsInfo.Aggregations) > 0 {
		// 性能优化：使用Fields机制，在搜索时直接获取需要的字段，避免搜索后再获取文档
		// 这比搜索后再逐个获取文档更高效，因为：
		// 1. 字段数据已经在hit.Fields中，类型已转换好
		// 2. 避免了Document()调用的开销
		// 3. Bleve内部可能对Reader有优化和复用
		var metricsAggs map[string]interface{}
		var err error

		// 收集所有metrics聚合需要的字段
		fieldsNeeded := make(map[string]bool)
		fieldsList := make([]string, 0)
		for _, spec := range parsedSubAggs.MetricsInfo.Aggregations {
			if spec.Field != "" && !fieldsNeeded[spec.Field] {
				fieldsNeeded[spec.Field] = true
				fieldsList = append(fieldsList, spec.Field)
			}
		}

		// 获取所有匹配的文档来计算metrics
		allDocsReq := bleve.NewSearchRequest(bucketQuery)
		allDocsReq.Size = 10000 // 限制大小，避免内存问题
		if len(fieldsList) > 0 {
			// 使用Fields机制，让Bleve在搜索时自动填充hit.Fields
			allDocsReq.Fields = fieldsList
		}
		allDocsResult, err := idx.Search(allDocsReq)
		if err == nil {
			// 性能优化：如果使用了Fields机制，直接从hit.Fields提取字段值
			// 这避免了Document()调用的开销，性能提升显著
			docCache := make(map[string]map[string]interface{})
			if len(fieldsList) > 0 && len(allDocsResult.Hits) > 0 {
				// 使用Fields机制：直接从hit.Fields提取字段值
				for _, hit := range allDocsResult.Hits {
					if hit.Fields == nil {
						continue
					}
					doc := make(map[string]interface{}, len(fieldsNeeded))
					for field := range fieldsNeeded {
						if val, ok := hit.Fields[field]; ok {
							doc[field] = val
						}
					}
					if len(doc) > 0 {
						docCache[hit.ID] = doc
					}
				}
			} else {
				// 回退方案：如果Fields机制不可用，使用IndexReader复用
				advancedIdx, idxErr := idx.Advanced()
				if idxErr == nil {
					reader, readerErr := advancedIdx.Reader()
					if readerErr == nil {
						defer reader.Close()
						// 复用同一个Reader逐个获取文档
						for _, hit := range allDocsResult.Hits {
							doc, docErr := reader.Document(hit.ID)
							if docErr == nil && doc != nil {
								docCache[hit.ID] = h.extractDocumentFields(doc)
							}
						}
					}
				}
				// 如果使用IndexReader失败，回退到原来的方法
				if len(docCache) == 0 {
					for _, hit := range allDocsResult.Hits {
						doc, docErr := idx.Document(hit.ID) // 每次调用都会创建新Reader
						if docErr == nil && doc != nil {
							docCache[hit.ID] = h.extractDocumentFields(doc)
						}
					}
				}
			}

			metricsAggs, err = h.calculateMetricsAggregationsWithCache(allDocsResult, parsedSubAggs.MetricsInfo.Aggregations, docCache)
			if err == nil {
				for k, v := range metricsAggs {
					result[k] = v
				}
			}
		}
	}

	return result
}

// buildTermQueryForBucket 为bucket构建term查询
func (h *DocumentHandler) buildTermQueryForBucket(fieldName string, key interface{}) query.Query {
	// 根据key的类型构建不同的查询
	switch v := key.(type) {
	case string:
		tq := query.NewTermQuery(v)
		tq.SetField(fieldName)
		return tq
	case int64:
		// 对于数字类型，使用DisjunctionQuery同时匹配数字和字符串
		vFloat := float64(v)
		nrq := query.NewNumericRangeQuery(&vFloat, &vFloat)
		nrq.SetField(fieldName)
		queries := []query.Query{nrq}
		tq := query.NewTermQuery(fmt.Sprintf("%d", v))
		tq.SetField(fieldName)
		queries = append(queries, tq)
		disjQuery := query.NewDisjunctionQuery(queries)
		disjQuery.SetMin(1)
		return disjQuery
	case float64:
		// 对于浮点数类型
		nrq := query.NewNumericRangeQuery(&v, &v)
		nrq.SetField(fieldName)
		queries := []query.Query{nrq}
		tq := query.NewTermQuery(fmt.Sprintf("%g", v))
		tq.SetField(fieldName)
		queries = append(queries, tq)
		disjQuery := query.NewDisjunctionQuery(queries)
		disjQuery.SetMin(1)
		return disjQuery
	case bool:
		tq := query.NewTermQuery(fmt.Sprintf("%t", v))
		tq.SetField(fieldName)
		return tq
	default:
		// 尝试转换为字符串
		termStr := fmt.Sprintf("%v", v)
		tq := query.NewTermQuery(termStr)
		tq.SetField(fieldName)
		return tq
	}
}

// buildNumericRangeQueryForBucket 为numeric range bucket构建范围查询
func (h *DocumentHandler) buildNumericRangeQueryForBucket(fieldName string, min, max *float64) query.Query {
	nrq := query.NewNumericRangeQuery(min, max)
	nrq.SetField(fieldName)
	return nrq
}

// buildDateRangeQueryForBucket 为date range bucket构建日期范围查询
func (h *DocumentHandler) buildDateRangeQueryForBucket(fieldName string, startStr, endStr *string) query.Query {
	var startTime, endTime time.Time
	hasStart := false
	hasEnd := false

	if startStr != nil && *startStr != "" {
		// 尝试解析日期字符串
		if t, err := time.Parse(time.RFC3339, *startStr); err == nil {
			startTime = t
			hasStart = true
		} else if t, err := time.Parse("2006-01-02", *startStr); err == nil {
			startTime = t
			hasStart = true
		}
	}

	if endStr != nil && *endStr != "" {
		if t, err := time.Parse(time.RFC3339, *endStr); err == nil {
			endTime = t
			hasEnd = true
		} else if t, err := time.Parse("2006-01-02", *endStr); err == nil {
			endTime = t
			hasEnd = true
		}
	}

	if !hasStart && !hasEnd {
		return nil
	}

	// 如果没有开始时间，使用零值（表示无下限）
	// 如果没有结束时间，使用零值（表示无上限）
	drq := query.NewDateRangeQuery(startTime, endTime)
	drq.SetField(fieldName)
	return drq
}
