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

	"github.com/lscgzwd/tiggerdb/logger"
	"github.com/lscgzwd/tiggerdb/protocols/es/search/dsl"
	"github.com/lscgzwd/tiggerdb/search/query"

	bleve "github.com/lscgzwd/tiggerdb"
)

// MetricsAggregationInfo Metrics聚合信息（用于后续计算）
type MetricsAggregationInfo struct {
	Aggregations map[string]MetricsAggregationSpec
}

// CompositeAggregationInfo Composite聚合信息
type CompositeAggregationInfo struct {
	Aggregations map[string]*CompositeAggregationConfig
}

// NestedAggregationInfo 嵌套聚合信息
type NestedAggregationInfo struct {
	// 父聚合名称 -> 子聚合配置
	// 例如: {"status_agg": {"category_agg": {...}}}
	SubAggregations map[string]map[string]map[string]interface{}
	// 父聚合名称 -> 字段名映射（用于构建bucket查询）
	FieldMapping map[string]string
}

// FilterAggregationInfo Filter聚合信息
type FilterAggregationInfo struct {
	// 聚合名称 -> Filter聚合配置
	Aggregations map[string]*FilterAggregationConfig
}

// FilterAggregationConfig Filter聚合配置
type FilterAggregationConfig struct {
	FilterQuery     query.Query                       // Filter查询
	SubAggregations map[string]map[string]interface{} // 子聚合配置
}

// TopHitsAggregationInfo TopHits聚合信息
type TopHitsAggregationInfo struct {
	// 聚合名称 -> TopHits聚合配置
	Aggregations map[string]*TopHitsAggregationConfig
}

// TopHitsAggregationConfig TopHits聚合配置
type TopHitsAggregationConfig struct {
	Size      int                    // 返回的文档数量（默认3）
	Sort      []interface{}          // 排序方式
	Source    interface{}            // 返回的字段（_source配置）
	Highlight map[string]interface{} // 高亮配置
}

// NestedFieldAggregationInfo Nested字段聚合信息
type NestedFieldAggregationInfo struct {
	// 聚合名称 -> Nested字段聚合配置
	Aggregations map[string]*NestedFieldAggregationConfig
}

// NestedFieldAggregationConfig Nested字段聚合配置
type NestedFieldAggregationConfig struct {
	Path            string                            // Nested字段路径
	SubAggregations map[string]map[string]interface{} // 子聚合配置
}

// ScriptedMetricAggregationInfo 脚本指标聚合信息
type ScriptedMetricAggregationInfo struct {
	Aggregations map[string]*ScriptedMetricAggregationConfig
}

// ScriptedMetricAggregationConfig 脚本指标聚合配置
// ES格式: {"scripted_metric": {"init_script": "...", "map_script": "...", "combine_script": "...", "reduce_script": "..."}}
type ScriptedMetricAggregationConfig struct {
	InitScript    string                 // 初始化脚本
	MapScript     string                 // 映射脚本（每个文档执行）
	CombineScript string                 // 合并脚本（每个分片执行）
	ReduceScript  string                 // 归约脚本（协调节点执行）
	Params        map[string]interface{} // 脚本参数
}

// BucketScriptAggregationInfo Bucket脚本聚合信息
type BucketScriptAggregationInfo struct {
	Aggregations map[string]*BucketScriptAggregationConfig
}

// BucketScriptAggregationConfig Bucket脚本聚合配置
// ES格式: {"bucket_script": {"buckets_path": {"var1": "agg1", "var2": "agg2"}, "script": "..."}}
type BucketScriptAggregationConfig struct {
	BucketsPath map[string]string      // 变量名 -> 聚合路径
	Script      string                 // 计算脚本
	GapPolicy   string                 // 空值策略: skip, insert_zeros
	Format      string                 // 输出格式
	Params      map[string]interface{} // 脚本参数
}

// AggregationConfig 聚合配置（包含主聚合和嵌套聚合）
type AggregationConfig struct {
	Type            string                            // 聚合类型: terms, range, date_range, avg, sum, etc.
	Config          map[string]interface{}            // 聚合配置
	SubAggregations map[string]map[string]interface{} // 嵌套聚合（aggs 或 aggregations）
}

// ParsedAggregations P2-1: 聚合解析结果结构体（封装所有返回值，提升可维护性）
// 替代原来的8个返回值，使代码更清晰、易于扩展
// 新增聚合类型只需在此结构体中添加字段，无需修改函数签名和所有调用点
type ParsedAggregations struct {
	Facets             bleve.FacetsRequest            // Bleve facets请求（用于terms、range等）
	MetricsInfo        *MetricsAggregationInfo        // Metrics聚合信息（avg、sum、min、max等）
	CompositeInfo      *CompositeAggregationInfo      // Composite聚合信息
	NestedInfo         *NestedAggregationInfo         // 嵌套聚合信息
	FilterInfo         *FilterAggregationInfo         // Filter聚合信息
	TopHitsInfo        *TopHitsAggregationInfo        // TopHits聚合信息
	NestedFieldInfo    *NestedFieldAggregationInfo    // Nested字段聚合信息
	ScriptedMetricInfo *ScriptedMetricAggregationInfo // Scripted Metric聚合信息
	BucketScriptInfo   *BucketScriptAggregationInfo   // Bucket Script聚合信息
}

// parseAggregations P2-1: 解析ES聚合请求并转换为bleve FacetsRequest
// 使用ParsedAggregations结构体封装返回值，提升可维护性和扩展性
func (h *DocumentHandler) parseAggregations(aggs map[string]map[string]interface{}) (*ParsedAggregations, error) {
	if len(aggs) == 0 {
		return &ParsedAggregations{}, nil
	}

	facets := make(bleve.FacetsRequest)
	metricsAggs := make(map[string]MetricsAggregationSpec)
	compositeAggs := make(map[string]*CompositeAggregationConfig)
	nestedAggs := make(map[string]map[string]map[string]interface{})
	filterAggs := make(map[string]*FilterAggregationConfig)
	topHitsAggs := make(map[string]*TopHitsAggregationConfig)
	nestedFieldAggs := make(map[string]*NestedFieldAggregationConfig)
	scriptedMetricAggs := make(map[string]*ScriptedMetricAggregationConfig)
	bucketScriptAggs := make(map[string]*BucketScriptAggregationConfig)
	fieldMapping := make(map[string]string) // 聚合名称 -> 字段名

	for aggName, aggSpec := range aggs {
		if len(aggSpec) == 0 {
			continue
		}

		// 解析聚合配置（包括嵌套聚合）
		aggConfig, err := h.parseSingleAggregation(aggName, aggSpec)
		if err != nil {
			logger.Warn("Failed to parse aggregation [%s]: %v", aggName, err)
			continue
		}

		// 处理嵌套聚合
		if len(aggConfig.SubAggregations) > 0 {
			nestedAggs[aggName] = aggConfig.SubAggregations
		}

		// 处理主聚合
		switch aggConfig.Type {
		case "terms":
			// Terms聚合: {"terms": {"field": "tags", "size": 10}}
			facetReq, err := h.parseTermsAggregation(aggConfig.Config)
			if err != nil {
				logger.Warn("Failed to parse terms aggregation [%s]: %v", aggName, err)
				continue
			}
			facets[aggName] = facetReq
			// 保存字段名映射
			if field, ok := aggConfig.Config["field"].(string); ok {
				fieldMapping[aggName] = field
			}

		case "range":
			// Range聚合: {"range": {"field": "price", "ranges": [...]}}
			facetReq, err := h.parseRangeAggregation(aggConfig.Config)
			if err != nil {
				logger.Warn("Failed to parse range aggregation [%s]: %v", aggName, err)
				continue
			}
			facets[aggName] = facetReq
			// 保存字段名映射
			if field, ok := aggConfig.Config["field"].(string); ok {
				fieldMapping[aggName] = field
			}

		case "date_range":
			// Date Range聚合: {"date_range": {"field": "date", "ranges": [...]}}
			facetReq, err := h.parseDateRangeAggregation(aggConfig.Config)
			if err != nil {
				logger.Warn("Failed to parse date_range aggregation [%s]: %v", aggName, err)
				continue
			}
			facets[aggName] = facetReq
			// 保存字段名映射
			if field, ok := aggConfig.Config["field"].(string); ok {
				fieldMapping[aggName] = field
			}

		case "composite":
			// Composite聚合: {"composite": {"size": 1000, "sources": [...]}}
			// 注意：Bleve不直接支持composite聚合，需要手动实现
			compositeAgg, err := h.parseCompositeAggregation(aggConfig.Config)
			if err != nil {
				logger.Warn("Failed to parse composite aggregation [%s]: %v", aggName, err)
				continue
			}
			// 存储composite聚合配置，后续在buildAggregations中处理
			compositeAggs[aggName] = compositeAgg
			// 为每个source创建terms facet，用于后续组合
			for _, source := range compositeAgg.Sources {
				if source.Terms != nil {
					facetName := fmt.Sprintf("%s_%s", aggName, source.Name)
					facetReq, err := h.parseTermsAggregation(map[string]interface{}{
						"field": source.Terms.Field,
						"size":  compositeAgg.Size,
					})
					if err == nil {
						facets[facetName] = facetReq
					}
				}
			}

		case "avg", "sum", "min", "max", "stats", "cardinality":
			// Metrics聚合: {"avg": {"field": "price"}}
			// Cardinality聚合: {"cardinality": {"field": "user_id", "precision_threshold": 100}}
			// 注意：bleve不直接支持metrics聚合，需要从搜索结果中计算
			field, ok := aggConfig.Config["field"].(string)
			if !ok || field == "" {
				logger.Warn("Metrics aggregation [%s] type [%s] requires a 'field' parameter", aggName, aggConfig.Type)
				continue
			}
			spec := MetricsAggregationSpec{
				Type:  aggConfig.Type,
				Field: field,
			}
			// 对于cardinality聚合，解析precision_threshold参数
			if aggConfig.Type == "cardinality" {
				if precisionThreshold, ok := aggConfig.Config["precision_threshold"].(float64); ok {
					spec.PrecisionThreshold = int(precisionThreshold)
				} else if precisionThreshold, ok := aggConfig.Config["precision_threshold"].(int); ok {
					spec.PrecisionThreshold = precisionThreshold
				}
			}
			metricsAggs[aggName] = spec

		case "filter":
			// Filter聚合: {"filter": {"term": {"status": "fixed"}}, "aggs": {...}}
			filterAgg, err := h.parseFilterAggregation(aggConfig.Config, aggConfig.SubAggregations)
			if err != nil {
				logger.Warn("Failed to parse filter aggregation [%s]: %v", aggName, err)
				continue
			}
			filterAggs[aggName] = filterAgg

		case "top_hits":
			// TopHits聚合: {"top_hits": {"size": 5, "sort": [...], "_source": {...}}}
			topHitsAgg, err := h.parseTopHitsAggregation(aggConfig.Config)
			if err != nil {
				logger.Warn("Failed to parse top_hits aggregation [%s]: %v", aggName, err)
				continue
			}
			topHitsAggs[aggName] = topHitsAgg

		case "nested":
			// Nested字段聚合: {"nested": {"path": "comments"}, "aggs": {...}}
			nestedFieldAgg, err := h.parseNestedFieldAggregation(aggConfig.Config, aggConfig.SubAggregations)
			if err != nil {
				logger.Warn("Failed to parse nested field aggregation [%s]: %v", aggName, err)
				continue
			}
			nestedFieldAggs[aggName] = nestedFieldAgg

		case "scripted_metric":
			// Scripted Metric聚合: {"scripted_metric": {"init_script": "...", "map_script": "...", ...}}
			scriptedMetricAgg := ParseScriptedMetricAggregation(aggConfig.Config)
			scriptedMetricAggs[aggName] = scriptedMetricAgg
			logger.Debug("parseAggregations: found scripted_metric aggregation [%s]", aggName)

		case "bucket_script":
			// Bucket Script聚合: {"bucket_script": {"buckets_path": {...}, "script": "..."}}
			bucketScriptAgg := ParseBucketScriptAggregation(aggConfig.Config)
			bucketScriptAggs[aggName] = bucketScriptAgg
			logger.Debug("parseAggregations: found bucket_script aggregation [%s]", aggName)

		default:
			logger.Warn("Unsupported aggregation type [%s] for aggregation [%s]", aggConfig.Type, aggName)
		}
	}

	var metricsInfo *MetricsAggregationInfo
	if len(metricsAggs) > 0 {
		metricsInfo = &MetricsAggregationInfo{
			Aggregations: metricsAggs,
		}
	}

	var compositeInfo *CompositeAggregationInfo
	if len(compositeAggs) > 0 {
		compositeInfo = &CompositeAggregationInfo{
			Aggregations: compositeAggs,
		}
	}

	var nestedInfo *NestedAggregationInfo
	if len(nestedAggs) > 0 {
		nestedInfo = &NestedAggregationInfo{
			SubAggregations: nestedAggs,
			FieldMapping:    fieldMapping,
		}
		logger.Debug("parseAggregations: found nested aggregations, count=%d, fields=%v", len(nestedAggs), fieldMapping)
	}

	var filterInfo *FilterAggregationInfo
	if len(filterAggs) > 0 {
		filterInfo = &FilterAggregationInfo{
			Aggregations: filterAggs,
		}
		logger.Debug("parseAggregations: found filter aggregations, count=%d", len(filterAggs))
	}

	var topHitsInfo *TopHitsAggregationInfo
	if len(topHitsAggs) > 0 {
		topHitsInfo = &TopHitsAggregationInfo{
			Aggregations: topHitsAggs,
		}
		logger.Debug("parseAggregations: found top_hits aggregations, count=%d", len(topHitsAggs))
	}

	var nestedFieldInfo *NestedFieldAggregationInfo
	if len(nestedFieldAggs) > 0 {
		nestedFieldInfo = &NestedFieldAggregationInfo{
			Aggregations: nestedFieldAggs,
		}
		logger.Debug("parseAggregations: found nested field aggregations, count=%d", len(nestedFieldAggs))
	}

	var scriptedMetricInfo *ScriptedMetricAggregationInfo
	if len(scriptedMetricAggs) > 0 {
		scriptedMetricInfo = &ScriptedMetricAggregationInfo{
			Aggregations: scriptedMetricAggs,
		}
		logger.Debug("parseAggregations: found scripted_metric aggregations, count=%d", len(scriptedMetricAggs))
	}

	var bucketScriptInfo *BucketScriptAggregationInfo
	if len(bucketScriptAggs) > 0 {
		bucketScriptInfo = &BucketScriptAggregationInfo{
			Aggregations: bucketScriptAggs,
		}
		logger.Debug("parseAggregations: found bucket_script aggregations, count=%d", len(bucketScriptAggs))
	}

	// P2-1: 返回封装的结构体，替代原来的8个返回值
	// 优势：新增聚合类型只需在此结构体中添加字段，无需修改函数签名和所有调用点
	return &ParsedAggregations{
		Facets:             facets,
		MetricsInfo:        metricsInfo,
		CompositeInfo:      compositeInfo,
		NestedInfo:         nestedInfo,
		FilterInfo:         filterInfo,
		TopHitsInfo:        topHitsInfo,
		NestedFieldInfo:    nestedFieldInfo,
		ScriptedMetricInfo: scriptedMetricInfo,
		BucketScriptInfo:   bucketScriptInfo,
	}, nil
}

// parseSingleAggregation 解析单个聚合配置（包括嵌套聚合）
// ES格式: {"terms": {"field": "status"}, "aggs": {"sub_agg": {"terms": {"field": "category"}}}}
func (h *DocumentHandler) parseSingleAggregation(aggName string, aggSpec map[string]interface{}) (*AggregationConfig, error) {
	config := &AggregationConfig{
		SubAggregations: make(map[string]map[string]interface{}),
	}

	// 查找聚合类型（terms, range, date_range, avg, sum, etc.）
	var aggType string
	var aggConfig map[string]interface{}

	for key, value := range aggSpec {
		// 跳过嵌套聚合容器（必须在检查聚合类型之前）
		if key == "aggs" || key == "aggregations" {
			// 解析嵌套聚合
			if subAggs, ok := value.(map[string]interface{}); ok {
				// 转换为 map[string]map[string]interface{} 格式
				for subAggName, subAggSpec := range subAggs {
					if subAggSpecMap, ok := subAggSpec.(map[string]interface{}); ok {
						config.SubAggregations[subAggName] = subAggSpecMap
					}
				}
			}
			continue
		}

		// 检查是否是聚合类型（必须是map类型）
		if valueMap, ok := value.(map[string]interface{}); ok {
			// 确认这不是嵌套聚合容器
			if key != "aggs" && key != "aggregations" {
				aggType = key
				aggConfig = valueMap
				break
			}
		}
	}

	if aggType == "" {
		return nil, fmt.Errorf("no aggregation type found for [%s]", aggName)
	}

	config.Type = aggType
	config.Config = aggConfig

	return config, nil
}

// parseTermsAggregation 解析terms聚合
// ES格式: {"terms": {"field": "tags", "size": 10}}
func (h *DocumentHandler) parseTermsAggregation(config map[string]interface{}) (*bleve.FacetRequest, error) {
	field, ok := config["field"].(string)
	if !ok || field == "" {
		return nil, fmt.Errorf("terms aggregation requires a 'field' parameter")
	}

	size := 10 // 默认大小
	if sizeVal, ok := config["size"].(float64); ok {
		size = int(sizeVal)
	} else if sizeVal, ok := config["size"].(int); ok {
		size = sizeVal
	}

	return bleve.NewFacetRequest(field, size), nil
}

// parseRangeAggregation 解析numeric range聚合
// ES格式: {"range": {"field": "price", "ranges": [{"to": 35}, {"from": 35, "to": 50}, {"from": 50}]}}
func (h *DocumentHandler) parseRangeAggregation(config map[string]interface{}) (*bleve.FacetRequest, error) {
	field, ok := config["field"].(string)
	if !ok || field == "" {
		return nil, fmt.Errorf("range aggregation requires a 'field' parameter")
	}

	ranges, ok := config["ranges"].([]interface{})
	if !ok || len(ranges) == 0 {
		return nil, fmt.Errorf("range aggregation requires a 'ranges' parameter")
	}

	facetReq := bleve.NewFacetRequest(field, len(ranges))

	for i, rangeSpec := range ranges {
		rangeMap, ok := rangeSpec.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid range specification at index %d", i)
		}

		var min, max *float64
		var name string

		// 解析from
		if fromVal, ok := rangeMap["from"].(float64); ok {
			min = &fromVal
		} else if fromVal, ok := rangeMap["from"].(int); ok {
			f := float64(fromVal)
			min = &f
		}

		// 解析to
		if toVal, ok := rangeMap["to"].(float64); ok {
			max = &toVal
		} else if toVal, ok := rangeMap["to"].(int); ok {
			f := float64(toVal)
			max = &f
		}

		// 解析key（可选，用于命名范围）
		if keyVal, ok := rangeMap["key"].(string); ok {
			name = keyVal
		} else {
			// 如果没有key，生成一个默认名称
			if min != nil && max != nil {
				name = fmt.Sprintf("%.0f-%.0f", *min, *max)
			} else if min != nil {
				name = fmt.Sprintf("%.0f+", *min)
			} else if max != nil {
				name = fmt.Sprintf("*-%.0f", *max)
			} else {
				name = fmt.Sprintf("range_%d", i)
			}
		}

		facetReq.AddNumericRange(name, min, max)
	}

	return facetReq, nil
}

// parseDateRangeAggregation 解析date range聚合
// ES格式: {"date_range": {"field": "date", "ranges": [{"to": "now"}, {"from": "now-1d"}]}}
func (h *DocumentHandler) parseDateRangeAggregation(config map[string]interface{}) (*bleve.FacetRequest, error) {
	field, ok := config["field"].(string)
	if !ok || field == "" {
		return nil, fmt.Errorf("date_range aggregation requires a 'field' parameter")
	}

	ranges, ok := config["ranges"].([]interface{})
	if !ok || len(ranges) == 0 {
		return nil, fmt.Errorf("date_range aggregation requires a 'ranges' parameter")
	}

	facetReq := bleve.NewFacetRequest(field, len(ranges))

	for i, rangeSpec := range ranges {
		rangeMap, ok := rangeSpec.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid date range specification at index %d", i)
		}

		var start, end *string
		var name string

		// 解析from
		if fromVal, ok := rangeMap["from"].(string); ok {
			start = &fromVal
		}

		// 解析to
		if toVal, ok := rangeMap["to"].(string); ok {
			end = &toVal
		}

		// 解析key（可选）
		if keyVal, ok := rangeMap["key"].(string); ok {
			name = keyVal
		} else {
			// 生成默认名称
			if start != nil && end != nil {
				name = fmt.Sprintf("%s-%s", *start, *end)
			} else if start != nil {
				name = fmt.Sprintf("%s+", *start)
			} else if end != nil {
				name = fmt.Sprintf("*-%s", *end)
			} else {
				name = fmt.Sprintf("date_range_%d", i)
			}
		}

		// 解析format（可选，用于日期解析器）
		format := ""
		if formatVal, ok := rangeMap["format"].(string); ok {
			format = formatVal
		}

		if format != "" {
			facetReq.AddDateTimeRangeStringWithParser(name, start, end, format)
		} else {
			facetReq.AddDateTimeRangeString(name, start, end)
		}
	}

	return facetReq, nil
}

// parseFilterAggregation 解析filter聚合
// ES格式: {"filter": {"term": {"status": "fixed"}}, "aggs": {...}}
// 注意：config 参数就是 filter 查询本身（例如 {"term": {"status": "fixed"}}）
func (h *DocumentHandler) parseFilterAggregation(config map[string]interface{}, subAggs map[string]map[string]interface{}) (*FilterAggregationConfig, error) {
	// config 本身就是 filter 查询，不需要再查找 "filter" 键
	if len(config) == 0 {
		return nil, fmt.Errorf("filter aggregation requires a 'filter' query")
	}

	// 使用DSL解析器解析查询
	parser := dsl.NewQueryParser()
	filterQuery, err := parser.ParseQuery(config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse filter query: %w", err)
	}

	return &FilterAggregationConfig{
		FilterQuery:     filterQuery,
		SubAggregations: subAggs,
	}, nil
}

// parseTopHitsAggregation 解析top_hits聚合
// ES格式: {"top_hits": {"size": 5, "sort": [...], "_source": {...}}}
func (h *DocumentHandler) parseTopHitsAggregation(config map[string]interface{}) (*TopHitsAggregationConfig, error) {
	topHitsConfig := &TopHitsAggregationConfig{
		Size: 3, // 默认返回3个文档
	}

	// 解析size
	if sizeVal, ok := config["size"]; ok {
		if size, ok := sizeVal.(float64); ok {
			topHitsConfig.Size = int(size)
		} else if size, ok := sizeVal.(int); ok {
			topHitsConfig.Size = size
		}
	}

	// 解析sort
	if sortVal, ok := config["sort"]; ok {
		if sort, ok := sortVal.([]interface{}); ok {
			topHitsConfig.Sort = sort
		}
	}

	// 解析_source
	if sourceVal, ok := config["_source"]; ok {
		topHitsConfig.Source = sourceVal
	}

	// 解析highlight
	if highlightVal, ok := config["highlight"]; ok {
		if highlight, ok := highlightVal.(map[string]interface{}); ok {
			topHitsConfig.Highlight = highlight
		}
	}

	return topHitsConfig, nil
}

// parseNestedFieldAggregation 解析nested字段聚合
// ES格式: {"nested": {"path": "comments"}, "aggs": {...}}
func (h *DocumentHandler) parseNestedFieldAggregation(config map[string]interface{}, subAggs map[string]map[string]interface{}) (*NestedFieldAggregationConfig, error) {
	// 解析path（必需）
	path, ok := config["path"].(string)
	if !ok || path == "" {
		return nil, fmt.Errorf("nested field aggregation requires a 'path' parameter")
	}

	return &NestedFieldAggregationConfig{
		Path:            path,
		SubAggregations: subAggs,
	}, nil
}
