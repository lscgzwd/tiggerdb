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

	// 解析子聚合配置
	facets, metricsInfo, compositeInfo, nestedInfo, _, topHitsInfo, nestedFieldInfo, err := h.parseAggregations(subAggs)
	if err != nil {
		logger.Warn("Failed to parse nested aggregations for bucket [%s]: %v", parentAggName, err)
		return nil
	}

	metricsCount := 0
	if metricsInfo != nil {
		metricsCount = len(metricsInfo.Aggregations)
	}
	compositeCount := 0
	if compositeInfo != nil {
		compositeCount = len(compositeInfo.Aggregations)
	}
	nestedCount := 0
	if nestedInfo != nil {
		nestedCount = len(nestedInfo.SubAggregations)
	}
	logger.Debug("buildNestedAggregationsForBucket: parsed facets=%d, metrics=%d, composite=%d, nested=%d", len(facets), metricsCount, compositeCount, nestedCount)

	// 创建搜索请求
	searchReq := bleve.NewSearchRequest(bucketQuery)
	searchReq.Size = 0 // 不需要返回文档，只需要聚合结果
	if len(facets) > 0 {
		searchReq.Facets = facets
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
		facetAggs := h.buildAggregations(searchResult.Facets, compositeInfo, nestedInfo, topHitsInfo, nestedFieldInfo, idx, bucketQuery)
		for k, v := range facetAggs {
			result[k] = v
		}
	}

	// 处理top_hits聚合
	if topHitsInfo != nil && len(topHitsInfo.Aggregations) > 0 {
		for topHitsName, topHitsConfig := range topHitsInfo.Aggregations {
			topHitsResult := h.buildTopHitsAggregation(topHitsConfig, idx, bucketQuery)
			if topHitsResult != nil {
				result[topHitsName] = topHitsResult
			}
		}
	}

	// 处理nested字段聚合
	if nestedFieldInfo != nil && len(nestedFieldInfo.Aggregations) > 0 {
		for nestedFieldName, nestedFieldConfig := range nestedFieldInfo.Aggregations {
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
	if metricsInfo != nil && len(metricsInfo.Aggregations) > 0 {
		// 获取所有匹配的文档来计算metrics
		allDocsReq := bleve.NewSearchRequest(bucketQuery)
		allDocsReq.Size = 10000 // 限制大小，避免内存问题
		allDocsResult, err := idx.Search(allDocsReq)
		if err == nil {
			// 创建docCache（复用已有的逻辑）
			docCache := make(map[string]map[string]interface{})
			for _, hit := range allDocsResult.Hits {
				doc, err := idx.Document(hit.ID)
				if err == nil && doc != nil {
					docCache[hit.ID] = h.extractDocumentFields(doc)
				}
			}
			metricsAggs, err := h.calculateMetricsAggregationsWithCache(allDocsResult, metricsInfo.Aggregations, docCache)
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
