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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lscgzwd/tiggerdb/logger"
	"github.com/lscgzwd/tiggerdb/script"

	"github.com/gorilla/mux"
	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/numeric"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/common"
	"github.com/lscgzwd/tiggerdb/protocols/es/search/dsl"
	"github.com/lscgzwd/tiggerdb/search"
	"github.com/lscgzwd/tiggerdb/search/query"
)

// SourceConfig _source字段配置
type SourceConfig struct {
	Includes []string `json:"includes,omitempty"`
	Excludes []string `json:"excludes,omitempty"`
}

// SearchRequest ES搜索请求
// 注意：ES 官方支持 "aggs" 和 "aggregations" 两种写法，需要手动处理
type SearchRequest struct {
	Query        map[string]interface{}            `json:"query,omitempty"`
	From         int                               `json:"from,omitempty"`
	Size         int                               `json:"size,omitempty"`
	Sort         []interface{}                     `json:"sort,omitempty"`
	Source       interface{}                       `json:"_source,omitempty"`       // 支持: []string, SourceConfig, bool
	Fields       []string                          `json:"fields,omitempty"`        // 兼容字段
	ScriptFields map[string]interface{}            `json:"script_fields,omitempty"` // 脚本计算字段
	Highlight    map[string]interface{}            `json:"highlight,omitempty"`
	Aggregations map[string]map[string]interface{} `json:"-"` // 手动解析，支持 aggs 和 aggregations
	PostFilter   map[string]interface{}            `json:"post_filter,omitempty"`
	MinScore     *float64                          `json:"min_score,omitempty"`
	Explain      bool                              `json:"explain,omitempty"`
	SearchAfter  []interface{}                     `json:"search_after,omitempty"` // 支持 search_after 分页
}

// searchRequestRaw 用于解析原始 JSON，支持 aggs 和 aggregations 两种格式
type searchRequestRaw struct {
	Query        map[string]interface{}            `json:"query,omitempty"`
	From         int                               `json:"from,omitempty"`
	Size         int                               `json:"size,omitempty"`
	Sort         []interface{}                     `json:"sort,omitempty"`
	Source       interface{}                       `json:"_source,omitempty"`
	Fields       []string                          `json:"fields,omitempty"`
	ScriptFields map[string]interface{}            `json:"script_fields,omitempty"` // 脚本计算字段
	Highlight    map[string]interface{}            `json:"highlight,omitempty"`
	Aggs         map[string]map[string]interface{} `json:"aggs,omitempty"`         // ES 短格式
	Aggregations map[string]map[string]interface{} `json:"aggregations,omitempty"` // ES 完整格式
	PostFilter   map[string]interface{}            `json:"post_filter,omitempty"`
	MinScore     *float64                          `json:"min_score,omitempty"`
	Explain      bool                              `json:"explain,omitempty"`
	SearchAfter  []interface{}                     `json:"search_after,omitempty"`
}

// UnmarshalJSON 自定义 JSON 解析，支持 aggs 和 aggregations 两种格式
func (s *SearchRequest) UnmarshalJSON(data []byte) error {
	var raw searchRequestRaw
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	s.Query = raw.Query
	s.From = raw.From
	s.Size = raw.Size
	s.Sort = raw.Sort
	s.Source = raw.Source
	s.Fields = raw.Fields
	s.ScriptFields = raw.ScriptFields
	s.Highlight = raw.Highlight
	s.PostFilter = raw.PostFilter
	s.MinScore = raw.MinScore
	s.Explain = raw.Explain
	s.SearchAfter = raw.SearchAfter

	// ES 官方支持 aggs 和 aggregations 两种写法，优先使用 aggregations
	if raw.Aggregations != nil {
		s.Aggregations = raw.Aggregations
	} else if raw.Aggs != nil {
		s.Aggregations = raw.Aggs
	}

	return nil
}

// Search 搜索API
// GET /<index>/_search
// POST /<index>/_search
func (h *DocumentHandler) Search(w http.ResponseWriter, r *http.Request) {
	// 读取请求体（移除DEBUG日志以提高性能）
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("Failed to read request body: %v", err)
		common.HandleError(w, common.NewBadRequestError("failed to read request body: "+err.Error()))
		return
	}
	// 重新设置body以便后续处理
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	indexName := mux.Vars(r)["index"]

	// 验证索引名称
	if err := common.ValidateIndexName(indexName); err != nil {
		common.HandleError(w, common.NewBadRequestError(err.Error()))
		return
	}

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 获取索引实例
	idx, err := h.indexMgr.GetIndex(indexName)
	if err != nil {
		logger.Error("Failed to get index [%s]: %v", indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to get index: "+err.Error()))
		return
	}

	// 检查是否有 scroll 参数
	scrollStr := r.URL.Query().Get("scroll")

	// 解析搜索请求
	var searchReq SearchRequest
	if r.Method == http.MethodPost {
		// POST请求，从请求体读取（兼容 chunked）
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&searchReq); err != nil && err != io.EOF {
			common.HandleError(w, common.NewBadRequestError("invalid JSON body: "+err.Error()))
			return
		}
	} else {
		// GET请求，从查询参数读取
		searchReq.From, _ = strconv.Atoi(r.URL.Query().Get("from"))
		searchReq.Size, _ = strconv.Atoi(r.URL.Query().Get("size"))
		if searchReq.Size == 0 {
			searchReq.Size = 10 // 默认10条
		}
	}

	// 执行搜索
	searchResponse, err := h.executeSearchInternal(idx, indexName, &searchReq)
	if err != nil {
		if apiErr, ok := err.(common.APIError); ok {
			common.HandleError(w, apiErr)
		} else {
			common.HandleError(w, common.NewInternalServerError(err.Error()))
		}
		return
	}

	// 如果指定了 scroll，创建 scroll context 并添加到响应
	if scrollStr != "" {
		scrollTTL, err := parseScrollTTL(scrollStr)
		if err != nil {
			common.HandleError(w, common.NewBadRequestError("invalid scroll parameter: "+err.Error()))
			return
		}

		// 如果scroll没有指定sort，使用默认sort（按_id升序），确保scroll可以正常工作
		scrollSort := searchReq.Sort
		if len(scrollSort) == 0 {
			// 使用默认sort：按_id升序
			scrollSort = []interface{}{map[string]interface{}{"_id": map[string]interface{}{"order": "asc"}}}
			// 更新searchReq的Sort，确保返回的hits包含sort值
			searchReq.Sort = scrollSort
		}

		scrollMgr := GetScrollManager()
		scrollCtx, err := scrollMgr.CreateScrollContext(
			indexName,
			searchReq.Query,
			scrollSort,
			searchReq.Source,
			searchReq.Size,
			searchReq.Aggregations,
			scrollTTL,
		)
		if err != nil {
			common.HandleError(w, common.NewInternalServerError("failed to create scroll context: "+err.Error()))
			return
		}

		// 将 scroll_id 添加到响应中
		searchResponse["_scroll_id"] = scrollCtx.ScrollID

		// 如果有结果，记录最后一个结果的 sort 值（用于下一次 scroll）
		if hitsWrapper, ok := searchResponse["hits"].(map[string]interface{}); ok {
			// hits["hits"] 的类型是 []map[string]interface{}，不是 []interface{}
			if hitsList, ok := hitsWrapper["hits"].([]map[string]interface{}); ok && len(hitsList) > 0 {
				// 更新 scroll context 的 From（用于 from 分页方式）
				scrollCtx.From = searchReq.From + len(hitsList)
				logger.Info("Scroll context [%s] updated: From=%d (was %d, hits=%d)", scrollCtx.ScrollID, scrollCtx.From, searchReq.From, len(hitsList))

				// 转换最后一个 hit
				lastHit := hitsList[len(hitsList)-1]
				if sortVals, ok := lastHit["sort"].([]interface{}); ok && len(sortVals) > 0 {
					scrollCtx.LastSort = sortVals
					logger.Info("Scroll context [%s] LastSort set to: %v", scrollCtx.ScrollID, sortVals)
				} else {
					logger.Warn("Scroll context [%s] no sort values in last hit, will use From pagination", scrollCtx.ScrollID)
				}
			} else {
				logger.Info("Scroll context [%s] no hits returned (hitsList type: %T)", scrollCtx.ScrollID, hitsWrapper["hits"])
			}
		}
	}

	// 返回响应 - Search API 需要直接返回搜索响应，不使用通用响应格式包装
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(searchResponse); err != nil {
		logger.Error("Failed to encode search response: %v", err)
	}
}

// executeSearchInternal 执行搜索的核心逻辑（供Search和MultiSearch复用）
func (h *DocumentHandler) executeSearchInternal(idx bleve.Index, indexName string, searchReq *SearchRequest) (map[string]interface{}, error) {
	// 设置默认 Size
	if searchReq.Size <= 0 {
		searchReq.Size = 10 // 默认10条
	}

	// 创建Query DSL解析器
	parser := dsl.NewQueryParser()

	// 解析查询
	var bleveQuery query.Query
	if searchReq.Query != nil {
		// 打印原始查询请求（使用 Info 级别方便调试）
		queryJSON, _ := json.MarshalIndent(searchReq.Query, "", "  ")
		logger.Info("executeSearchInternal [%s] - Original query JSON:\n%s", indexName, string(queryJSON))

		var err error
		bleveQuery, err = parser.ParseQuery(searchReq.Query)
		if err != nil {
			logger.Error("Failed to parse query: %v", err)
			return nil, common.NewBadRequestError("failed to parse query: " + err.Error())
		}
		// 打印解析后的查询类型
		logger.Info("executeSearchInternal [%s] - Parsed query type: %T", indexName, bleveQuery)

		// 检查并处理 join 查询（has_child/has_parent）
		bleveQuery, err = h.processJoinQueries(idx, bleveQuery)
		if err != nil {
			logger.Error("Failed to process join queries: %v", err)
			return nil, common.NewBadRequestError("failed to process join query: " + err.Error())
		}
	} else {
		// 默认match_all查询
		bleveQuery = query.NewMatchAllQuery()
	}

	// 构建bleve搜索请求
	bleveReq := bleve.NewSearchRequest(bleveQuery)
	bleveReq.From = searchReq.From
	bleveReq.Size = searchReq.Size

	// 打印查询详情
	logger.Info("executeSearchInternal [%s] - Search request: From=%d, Size=%d, QueryType=%T", indexName, bleveReq.From, bleveReq.Size, bleveQuery)
	// 打印 Bleve 查询的 JSON 表示
	if bleveQueryJSON, err := json.Marshal(bleveQuery); err == nil {
		logger.Info("executeSearchInternal [%s] - Bleve Query JSON: %s", indexName, string(bleveQueryJSON))
	}

	// 如果有metrics聚合且size=0，需要获取所有文档用于计算
	// 设置一个合理的size限制（避免内存问题）
	hasMetricsAgg := false
	if searchReq.Aggregations != nil {
		for _, aggSpec := range searchReq.Aggregations {
			for aggType := range aggSpec {
				if aggType == "avg" || aggType == "sum" || aggType == "min" || aggType == "max" || aggType == "stats" || aggType == "cardinality" {
					hasMetricsAgg = true
					break
				}
			}
			if hasMetricsAgg {
				break
			}
		}
	}

	if searchReq.Size == 0 {
		if hasMetricsAgg {
			// 如果有metrics聚合，设置一个较大的size以获取所有文档用于计算
			// 限制最大为10000，避免内存问题
			bleveReq.Size = 10000
			logger.Info("Size=0 with metrics aggregation, setting size to 10000 for calculation")
		} else {
			bleveReq.Size = 10 // 默认10条
		}
	}

	// 解析 _source 字段（用于后续过滤）
	requestedFields := h.parseSourceField(searchReq.Source)
	if len(requestedFields) == 0 && len(searchReq.Fields) > 0 {
		requestedFields = searchReq.Fields
	}
	// 注意：不设置 bleveReq.Fields，让 Bleve 只返回 ID 和 Score
	// 字段加载将在搜索完成后，只为返回的 Size 个结果获取
	bleveReq.Explain = searchReq.Explain

	// 解析排序
	if len(searchReq.Sort) > 0 {
		sortOrder, err := h.parseSort(searchReq.Sort)
		if err != nil {
			logger.Error("Failed to parse sort: %v", err)
			return nil, common.NewBadRequestError("failed to parse sort: " + err.Error())
		}
		bleveReq.SortByCustom(sortOrder)
	}

	// 处理 search_after 分页
	if len(searchReq.SearchAfter) > 0 {
		// 验证 search_after 长度必须与 sort 长度一致
		if len(searchReq.Sort) == 0 {
			return nil, common.NewBadRequestError("search_after requires sort to be specified")
		}
		if len(searchReq.SearchAfter) != len(searchReq.Sort) {
			return nil, common.NewBadRequestError(fmt.Sprintf("search_after length (%d) must match sort length (%d)", len(searchReq.SearchAfter), len(searchReq.Sort)))
		}
		// 验证不能同时使用 from 和 search_after
		if searchReq.From != 0 {
			return nil, common.NewBadRequestError("cannot use search_after with from != 0")
		}
		// 将 search_after 转换为 []string（Bleve 需要）
		searchAfterStrs := make([]string, len(searchReq.SearchAfter))
		for i, v := range searchReq.SearchAfter {
			searchAfterStrs[i] = fmt.Sprintf("%v", v)
		}
		bleveReq.SetSearchAfter(searchAfterStrs)
	}

	// 解析高亮
	if searchReq.Highlight != nil {
		highlightReq, err := h.parseHighlight(searchReq.Highlight)
		if err != nil {
			logger.Warn("Failed to parse highlight: %v", err)
		} else {
			bleveReq.Highlight = highlightReq
		}
	}

	// 解析最小分数（bleve不支持MinScore，使用filter实现）
	// TODO: 实现min_score过滤
	if searchReq.MinScore != nil {
		logger.Warn("min_score is not fully supported, filtering will be done post-search")
	}

	// 解析聚合
	var metricsAggInfo *MetricsAggregationInfo
	var compositeAggInfo *CompositeAggregationInfo
	var nestedAggInfo *NestedAggregationInfo
	var filterAggInfo *FilterAggregationInfo
	var topHitsAggInfo *TopHitsAggregationInfo
	var nestedFieldAggInfo *NestedFieldAggregationInfo
	if searchReq.Aggregations != nil {
		facets, metricsInfo, compositeInfo, nestedInfo, filterInfo, topHitsInfo, nestedFieldInfo, err := h.parseAggregations(searchReq.Aggregations)
		if err != nil {
			logger.Warn("Failed to parse aggregations: %v", err)
		} else {
			if len(facets) > 0 {
				bleveReq.Facets = facets
			}
			metricsAggInfo = metricsInfo
			compositeAggInfo = compositeInfo
			nestedAggInfo = nestedInfo
			filterAggInfo = filterInfo
			topHitsAggInfo = topHitsInfo
			nestedFieldAggInfo = nestedFieldInfo
		}
	}

	// 执行搜索
	startTime := time.Now()
	searchResult, err := idx.Search(bleveReq)
	if err != nil {
		logger.Error("Failed to search index [%s]: %v", indexName, err)
		return nil, common.NewInternalServerError("failed to search: " + err.Error())
	}
	took := time.Since(startTime).Milliseconds()
	// 打印搜索结果摘要（使用 Info 级别方便调试）
	logger.Info("executeSearchInternal [%s] - Search result: Total=%d, Hits=%d, MaxScore=%f, Took=%dms",
		indexName, searchResult.Total, len(searchResult.Hits), searchResult.MaxScore, took)
	// 打印前5个结果的ID和分数
	for i, hit := range searchResult.Hits {
		if i >= 5 {
			break
		}
		logger.Info("executeSearchInternal [%s] - Hit[%d]: ID=%s, Score=%f", indexName, i, hit.ID, hit.Score)
	}

	// 构建ES格式的响应
	hits := make([]map[string]interface{}, 0, len(searchResult.Hits))

	// 性能优化：使用单个 IndexReader 批量获取文档，避免为每个文档创建新的 Reader
	var docCache map[string]map[string]interface{}
	if len(searchResult.Hits) > 0 {
		docCache = make(map[string]map[string]interface{}, len(searchResult.Hits))

		// 获取底层索引并创建单个 Reader
		advancedIdx, err := idx.Advanced()
		if err == nil {
			reader, err := advancedIdx.Reader()
			if err == nil {
				defer reader.Close()
				// 使用单个 Reader 顺序获取所有文档
				for _, hit := range searchResult.Hits {
					doc, err := reader.Document(hit.ID)
					if err == nil && doc != nil {
						docCache[hit.ID] = h.extractDocumentFields(doc)
					}
				}
			}
		}

		// 如果上面的方法失败，回退到原来的方法
		if len(docCache) == 0 {
			for _, hit := range searchResult.Hits {
				doc, err := idx.Document(hit.ID)
				if err == nil && doc != nil {
					docCache[hit.ID] = h.extractDocumentFields(doc)
				}
			}
		}
	}

	for _, hit := range searchResult.Hits {
		hitData := map[string]interface{}{
			"_index": indexName,
			"_id":    hit.ID,
			"_score": hit.Score,
		}

		// 添加_source字段
		if doc, ok := docCache[hit.ID]; ok {
			// 根据用户请求的字段进行过滤
			if len(requestedFields) > 0 {
				filteredSource := make(map[string]interface{})
				for _, field := range requestedFields {
					if val, ok := doc[field]; ok {
						filteredSource[field] = val
					}
				}
				hitData["_source"] = filteredSource
			} else {
				hitData["_source"] = doc
			}
		} else {
			hitData["_source"] = map[string]interface{}{}
		}

		// 添加高亮字段
		if len(hit.Fragments) > 0 {
			hitData["highlight"] = hit.Fragments
		}

		// 添加explanation（如果请求了）
		if searchReq.Explain && hit.Expl != nil {
			hitData["_explanation"] = h.buildExplanation(hit.Expl)
		}

		// 添加sort值
		if len(hit.Sort) > 0 {
			hitData["sort"] = hit.Sort
		}

		// 处理 script_fields
		if len(searchReq.ScriptFields) > 0 {
			scriptFieldsResult := h.computeScriptFields(searchReq.ScriptFields, doc, hit.Score)
			if len(scriptFieldsResult) > 0 {
				hitData["fields"] = scriptFieldsResult
			}
		}

		hits = append(hits, hitData)
	}

	// 构建符合 ES 官方格式的响应（字段顺序与 ES 官方一致）
	searchResponse := map[string]interface{}{
		"_shards": map[string]interface{}{
			"total":      1,
			"successful": 1,
			"skipped":    0,
			"failed":     0,
		},
		"hits": map[string]interface{}{
			"total": map[string]interface{}{
				"value":    searchResult.Total,
				"relation": "eq",
			},
			"max_score": searchResult.MaxScore,
			"hits":      hits,
		},
		"timed_out": false,
		"took":      took,
		// 如果使用 search_after，在最后一个 hit 的 sort 值可以作为下一次的 search_after
		// ES 客户端会自动处理，这里不需要额外返回
	}

	// 添加聚合结果（如果请求了）
	if searchReq.Aggregations != nil {
		aggs := make(map[string]interface{})

		// 处理composite聚合（优先处理，因为需要特殊格式）
		if compositeAggInfo != nil && len(compositeAggInfo.Aggregations) > 0 {
			// 检查是否有多字段 composite 聚合
			hasMultiSourceComposite := false
			for _, cfg := range compositeAggInfo.Aggregations {
				if len(cfg.Sources) > 1 {
					hasMultiSourceComposite = true
					break
				}
			}

			if hasMultiSourceComposite {
				// 多字段 composite 聚合需要遍历所有文档
				// 获取所有匹配的文档
				allDocs, err := h.fetchAllDocsForCompositeAgg(idx, bleveReq.Query, compositeAggInfo)
				if err != nil {
					logger.Warn("Failed to fetch docs for composite aggregation: %v", err)
				} else {
					compositeAggs := h.buildCompositeAggregationsFromDocs(allDocs, compositeAggInfo.Aggregations)
					for k, v := range compositeAggs {
						aggs[k] = v
					}
				}
			} else {
				// 单字段 composite 聚合可以使用 facets
				compositeAggs := h.buildCompositeAggregations(searchResult.Facets, compositeAggInfo.Aggregations)
				for k, v := range compositeAggs {
					aggs[k] = v
				}
			}

			// 确保所有请求的composite聚合都有响应（即使没有数据）
			for aggName := range compositeAggInfo.Aggregations {
				if _, exists := aggs[aggName]; !exists {
					aggs[aggName] = map[string]interface{}{
						"buckets": []interface{}{},
					}
				}
			}
		}

		// 添加bucket聚合结果（来自bleve facets，排除composite相关的facet）
		if len(searchResult.Facets) > 0 {
			facetAggs := h.buildAggregations(searchResult.Facets, compositeAggInfo, nestedAggInfo, topHitsAggInfo, nestedFieldAggInfo, idx, bleveReq.Query)
			for k, v := range facetAggs {
				// 避免覆盖composite聚合
				if _, exists := aggs[k]; !exists {
					aggs[k] = v
				}
			}
		}

		// 计算并添加metrics聚合结果（复用已获取的文档数据）
		if metricsAggInfo != nil && len(metricsAggInfo.Aggregations) > 0 {
			metricsAggs, err := h.calculateMetricsAggregationsWithCache(searchResult, metricsAggInfo.Aggregations, docCache)
			if err != nil {
				logger.Warn("Failed to calculate metrics aggregations: %v", err)
			} else {
				for k, v := range metricsAggs {
					aggs[k] = v
				}
			}
		}

		// 处理filter聚合
		if filterAggInfo != nil && len(filterAggInfo.Aggregations) > 0 {
			filterAggs := h.buildFilterAggregations(filterAggInfo, idx, bleveReq.Query)
			for k, v := range filterAggs {
				aggs[k] = v
			}
		}

		// 处理nested字段聚合
		if nestedFieldAggInfo != nil && len(nestedFieldAggInfo.Aggregations) > 0 {
			nestedFieldAggs := h.buildNestedFieldAggregations(nestedFieldAggInfo, idx, bleveReq.Query)
			for k, v := range nestedFieldAggs {
				aggs[k] = v
			}
		}

		// 确保所有请求的聚合都有响应（即使没有数据）
		for aggName, aggSpec := range searchReq.Aggregations {
			if _, exists := aggs[aggName]; !exists {
				// 根据聚合类型返回默认结构
				for aggType := range aggSpec {
					switch aggType {
					case "composite":
						aggs[aggName] = map[string]interface{}{
							"buckets": []interface{}{},
						}
					case "terms":
						aggs[aggName] = map[string]interface{}{
							"buckets": []interface{}{},
						}
					case "range", "date_range":
						aggs[aggName] = map[string]interface{}{
							"buckets": []interface{}{},
						}
					case "avg", "sum", "min", "max", "stats", "cardinality":
						aggs[aggName] = map[string]interface{}{
							"value": nil,
						}
					default:
						aggs[aggName] = map[string]interface{}{}
					}
					break
				}
			}
		}

		searchResponse["aggregations"] = aggs
	}

	return searchResponse, nil
}

// parseSort 解析ES排序格式并转换为bleve SortOrder
func (h *DocumentHandler) parseSort(sortSpec []interface{}) (search.SortOrder, error) {
	if len(sortSpec) == 0 {
		return nil, nil
	}

	// 如果第一个元素是字符串，使用简单格式：["field1", "-field2"]
	if _, ok := sortSpec[0].(string); ok {
		sortStrings := make([]string, len(sortSpec))
		for i, s := range sortSpec {
			if str, ok := s.(string); ok {
				sortStrings[i] = str
			} else {
				return nil, fmt.Errorf("invalid sort format: expected string, got %T", s)
			}
		}
		return search.ParseSortOrderStrings(sortStrings), nil
	}

	// 复杂格式：[{"field": {"order": "desc"}}, "_score", {"_script": {...}}]
	sortOrder := make(search.SortOrder, 0, len(sortSpec))
	for _, item := range sortSpec {
		if str, ok := item.(string); ok {
			// 简单字符串格式
			sortOrder = append(sortOrder, search.ParseSearchSortString(str))
		} else if obj, ok := item.(map[string]interface{}); ok {
			// 检查是否是脚本排序
			if scriptSpec, ok := obj["_script"].(map[string]interface{}); ok {
				scriptSort, err := h.parseScriptSort(scriptSpec)
				if err != nil {
					return nil, err
				}
				sortOrder = append(sortOrder, scriptSort)
				continue
			}

			// 对象格式：{"field": {"order": "desc"}}
			for field, spec := range obj {
				if specMap, ok := spec.(map[string]interface{}); ok {
					// 解析order
					order := "asc"
					if o, ok := specMap["order"].(string); ok {
						order = o
					}
					desc := order == "desc"
					sortOrder = append(sortOrder, &search.SortField{
						Field: field,
						Desc:  desc,
					})
				} else {
					// 简单格式：{"field": "asc"}
					order := "asc"
					if str, ok := spec.(string); ok {
						order = str
					}
					desc := order == "desc"
					sortOrder = append(sortOrder, &search.SortField{
						Field: field,
						Desc:  desc,
					})
				}
			}
		} else {
			return nil, fmt.Errorf("invalid sort format: expected string or object, got %T", item)
		}
	}

	return sortOrder, nil
}

// parseScriptSort 解析脚本排序
// ES格式: {"_script": {"type": "number", "script": {"source": "...", "params": {...}}, "order": "desc"}}
func (h *DocumentHandler) parseScriptSort(spec map[string]interface{}) (search.SearchSort, error) {
	order := "asc"
	if o, ok := spec["order"].(string); ok {
		order = o
	}
	desc := order == "desc"

	// 解析脚本
	scriptData, ok := spec["script"]
	if !ok {
		return nil, fmt.Errorf("_script sort must have 'script' field")
	}

	s, err := script.ParseScript(scriptData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse script sort: %w", err)
	}

	return &search.SortScript{
		Script: s,
		Desc:   desc,
	}, nil
}

// computeScriptFields 计算脚本字段
// ES格式: {"script_fields": {"field_name": {"script": {"source": "..."}}}}
func (h *DocumentHandler) computeScriptFields(scriptFields map[string]interface{}, doc map[string]interface{}, score float64) map[string]interface{} {
	if len(scriptFields) == 0 {
		return nil
	}

	result := make(map[string]interface{})
	engine := script.NewEngine()

	for fieldName, fieldSpec := range scriptFields {
		specMap, ok := fieldSpec.(map[string]interface{})
		if !ok {
			continue
		}

		// 解析脚本
		var scriptData interface{}
		if s, ok := specMap["script"]; ok {
			scriptData = s
		} else {
			continue
		}

		s, err := script.ParseScript(scriptData)
		if err != nil {
			logger.Warn("Failed to parse script field [%s]: %v", fieldName, err)
			continue
		}

		// 创建执行上下文
		ctx := script.NewContext(doc, doc, s.Params)
		ctx.Score = score

		// 执行脚本
		value, err := engine.Execute(s, ctx)
		if err != nil {
			logger.Warn("Failed to execute script field [%s]: %v", fieldName, err)
			continue
		}

		// ES 返回格式：每个字段的值是数组
		result[fieldName] = []interface{}{value}
	}

	return result
}

// parseHighlight 解析ES高亮格式并转换为bleve HighlightRequest
func (h *DocumentHandler) parseHighlight(highlightSpec map[string]interface{}) (*bleve.HighlightRequest, error) {
	highlightReq := bleve.NewHighlight()

	// 解析fields
	if fields, ok := highlightSpec["fields"].(map[string]interface{}); ok {
		for field := range fields {
			highlightReq.AddField(field)
		}
	} else if fields, ok := highlightSpec["fields"].([]interface{}); ok {
		for _, field := range fields {
			if fieldStr, ok := field.(string); ok {
				highlightReq.AddField(fieldStr)
			}
		}
	}

	// 解析style
	if style, ok := highlightSpec["style"].(string); ok {
		highlightReq = bleve.NewHighlightWithStyle(style)
		// 重新添加fields
		if fields, ok := highlightSpec["fields"].(map[string]interface{}); ok {
			for field := range fields {
				highlightReq.AddField(field)
			}
		} else if fields, ok := highlightSpec["fields"].([]interface{}); ok {
			for _, field := range fields {
				if fieldStr, ok := field.(string); ok {
					highlightReq.AddField(fieldStr)
				}
			}
		}
	}

	return highlightReq, nil
}

// buildExplanation 构建explanation响应
func (h *DocumentHandler) buildExplanation(expl *search.Explanation) map[string]interface{} {
	if expl == nil {
		return nil
	}

	result := map[string]interface{}{
		"value":       expl.Value,
		"description": expl.Message,
	}

	if len(expl.Children) > 0 {
		children := make([]map[string]interface{}, len(expl.Children))
		for i, child := range expl.Children {
			children[i] = h.buildExplanation(child)
		}
		result["details"] = children
	}

	return result
}

// buildCompositeAggregationsFromDocs 从文档中构建composite聚合响应
// 这是正确的实现，通过遍历文档计算多字段组合
func (h *DocumentHandler) buildCompositeAggregationsFromDocs(
	docs []map[string]interface{},
	compositeAggs map[string]*CompositeAggregationConfig,
) map[string]interface{} {
	aggs := make(map[string]interface{})

	for aggName, compositeAgg := range compositeAggs {
		// 使用 map 统计每个组合的文档数
		combinationCounts := make(map[string]*CompositeKey)

		for _, doc := range docs {
			// 提取每个 source 的值
			key := make(map[string]interface{})
			for _, source := range compositeAgg.Sources {
				fieldName := source.Terms.Field
				val := getNestedFieldValue(doc, fieldName)
				if val == nil && !source.Terms.MissingBucket {
					// 如果值为空且不允许 missing bucket，跳过这个文档
					key = nil
					break
				}
				key[source.Name] = val
			}

			if key == nil {
				continue
			}

			// 生成组合键的字符串表示
			ck := &CompositeKey{Values: key, Count: 1}
			keyStr := ck.String(compositeAgg.Sources)

			if existing, exists := combinationCounts[keyStr]; exists {
				existing.Count++
			} else {
				combinationCounts[keyStr] = ck
			}
		}

		// 转换为切片并排序
		keys := make([]CompositeKey, 0, len(combinationCounts))
		for _, ck := range combinationCounts {
			keys = append(keys, *ck)
		}
		SortCompositeKeys(keys, compositeAgg.Sources)

		// 应用 after 过滤（分页）
		if compositeAgg.AfterKey != nil {
			filteredKeys := make([]CompositeKey, 0)
			for _, ck := range keys {
				if CompareCompositeKeys(ck.Values, compositeAgg.AfterKey, compositeAgg.Sources) > 0 {
					filteredKeys = append(filteredKeys, ck)
				}
			}
			keys = filteredKeys
		}

		// 应用 size 限制
		if len(keys) > compositeAgg.Size {
			keys = keys[:compositeAgg.Size]
		}

		// 构建 buckets
		buckets := make([]map[string]interface{}, 0, len(keys))
		for _, ck := range keys {
			buckets = append(buckets, map[string]interface{}{
				"key":       ck.Values,
				"doc_count": ck.Count,
			})
		}

		// 构建 after_key（最后一个 bucket 的 key）
		result := map[string]interface{}{
			"buckets": buckets,
		}
		if len(keys) > 0 {
			result["after_key"] = keys[len(keys)-1].Values
		}

		aggs[aggName] = result
	}

	return aggs
}

// buildFilterAggregations 构建filter聚合响应
func (h *DocumentHandler) buildFilterAggregations(filterInfo *FilterAggregationInfo, idx bleve.Index, baseQuery query.Query) map[string]interface{} {
	aggs := make(map[string]interface{})

	for aggName, filterAgg := range filterInfo.Aggregations {
		logger.Debug("buildFilterAggregations: processing filter aggregation [%s]", aggName)

		// 组合基础查询和filter查询
		combinedQuery := query.NewBooleanQuery([]query.Query{baseQuery, filterAgg.FilterQuery}, nil, nil)

		// 执行搜索获取匹配的文档数
		searchReq := bleve.NewSearchRequest(combinedQuery)
		searchReq.Size = 0 // 不需要返回文档，只需要总数
		searchResult, err := idx.Search(searchReq)
		if err != nil {
			logger.Warn("Failed to execute filter aggregation search for [%s]: %v", aggName, err)
			aggs[aggName] = map[string]interface{}{
				"doc_count": 0,
			}
			continue
		}

		docCount := searchResult.Total

		// 构建结果
		result := map[string]interface{}{
			"doc_count": docCount,
		}

		// 如果有子聚合，执行子聚合
		if len(filterAgg.SubAggregations) > 0 {
			logger.Debug("buildFilterAggregations: processing sub-aggregations for [%s], count=%d", aggName, len(filterAgg.SubAggregations))

			// 解析子聚合
			facets, metricsInfo, compositeInfo, nestedInfo, _, topHitsInfo, nestedFieldInfo, err := h.parseAggregations(filterAgg.SubAggregations)
			if err != nil {
				logger.Warn("Failed to parse sub-aggregations for filter [%s]: %v", aggName, err)
			} else {
				// 创建搜索请求执行子聚合
				subSearchReq := bleve.NewSearchRequest(combinedQuery)
				subSearchReq.Size = 0
				if len(facets) > 0 {
					subSearchReq.Facets = facets
				}

				subSearchResult, err := idx.Search(subSearchReq)
				if err != nil {
					logger.Warn("Failed to execute sub-aggregations for filter [%s]: %v", aggName, err)
				} else {
					// 构建子聚合结果
					subAggs := make(map[string]interface{})

					// 处理bucket聚合（terms, range, date_range）
					if len(subSearchResult.Facets) > 0 {
						facetAggs := h.buildAggregations(subSearchResult.Facets, compositeInfo, nestedInfo, topHitsInfo, nestedFieldInfo, idx, combinedQuery)
						for k, v := range facetAggs {
							subAggs[k] = v
						}
					}

					// 处理top_hits聚合
					if topHitsInfo != nil && len(topHitsInfo.Aggregations) > 0 {
						for topHitsName, topHitsConfig := range topHitsInfo.Aggregations {
							topHitsResult := h.buildTopHitsAggregation(topHitsConfig, idx, combinedQuery)
							if topHitsResult != nil {
								subAggs[topHitsName] = topHitsResult
							}
						}
					}

					// 处理nested字段聚合
					if nestedFieldInfo != nil && len(nestedFieldInfo.Aggregations) > 0 {
						for nestedFieldName, nestedFieldConfig := range nestedFieldInfo.Aggregations {
							logger.Debug("buildFilterAggregations: processing nested field aggregation [%s], path=[%s]", nestedFieldName, nestedFieldConfig.Path)
							// TODO: 实现nested字段聚合逻辑
							logger.Warn("Nested field aggregation [%s] is not fully implemented yet", nestedFieldName)
						}
					}

					// 处理metrics聚合
					if metricsInfo != nil && len(metricsInfo.Aggregations) > 0 {
						// 获取所有匹配的文档来计算metrics
						allDocsReq := bleve.NewSearchRequest(combinedQuery)
						allDocsReq.Size = 10000 // 限制大小，避免内存问题
						allDocsResult, err := idx.Search(allDocsReq)
						if err == nil {
							// 创建docCache
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
									subAggs[k] = v
								}
							}
						}
					}

					// 处理composite聚合
					if compositeInfo != nil && len(compositeInfo.Aggregations) > 0 {
						// 检查是否有多字段 composite 聚合
						hasMultiSourceComposite := false
						for _, cfg := range compositeInfo.Aggregations {
							if len(cfg.Sources) > 1 {
								hasMultiSourceComposite = true
								break
							}
						}

						if hasMultiSourceComposite {
							allDocs, err := h.fetchAllDocsForCompositeAgg(idx, combinedQuery, compositeInfo)
							if err == nil {
								compositeAggs := h.buildCompositeAggregationsFromDocs(allDocs, compositeInfo.Aggregations)
								for k, v := range compositeAggs {
									subAggs[k] = v
								}
							}
						} else {
							compositeAggs := h.buildCompositeAggregations(subSearchResult.Facets, compositeInfo.Aggregations)
							for k, v := range compositeAggs {
								subAggs[k] = v
							}
						}
					}

					if len(subAggs) > 0 {
						result["aggregations"] = subAggs
					}
				}
			}
		}

		aggs[aggName] = result
	}

	return aggs
}

// buildTopHitsAggregation 构建top_hits聚合响应
func (h *DocumentHandler) buildTopHitsAggregation(config *TopHitsAggregationConfig, idx bleve.Index, bucketQuery query.Query) map[string]interface{} {
	// 创建搜索请求
	searchReq := bleve.NewSearchRequest(bucketQuery)
	searchReq.Size = config.Size
	if config.Size == 0 {
		searchReq.Size = 3 // 默认3个文档
	}

	// 解析排序
	if len(config.Sort) > 0 {
		sortOrder, err := h.parseSort(config.Sort)
		if err == nil && len(sortOrder) > 0 {
			searchReq.SortByCustom(sortOrder)
		}
	}

	// 解析高亮
	if len(config.Highlight) > 0 {
		highlightReq, err := h.parseHighlight(config.Highlight)
		if err == nil {
			searchReq.Highlight = highlightReq
		}
	}

	// 执行搜索
	searchResult, err := idx.Search(searchReq)
	if err != nil {
		logger.Warn("Failed to execute top_hits aggregation search: %v", err)
		return nil
	}

	// 构建hits
	hits := make([]map[string]interface{}, 0, len(searchResult.Hits))
	requestedFields := h.parseSourceField(config.Source)

	// 获取文档数据
	docCache := make(map[string]map[string]interface{})
	for _, hit := range searchResult.Hits {
		doc, err := idx.Document(hit.ID)
		if err == nil && doc != nil {
			docCache[hit.ID] = h.extractDocumentFields(doc)
		}
	}

	for _, hit := range searchResult.Hits {
		hitData := map[string]interface{}{
			"_index": "",
			"_id":    hit.ID,
			"_score": hit.Score,
		}

		// 添加_source字段
		if doc, ok := docCache[hit.ID]; ok {
			if len(requestedFields) > 0 {
				filteredSource := make(map[string]interface{})
				for _, field := range requestedFields {
					if val, ok := doc[field]; ok {
						filteredSource[field] = val
					}
				}
				hitData["_source"] = filteredSource
			} else {
				hitData["_source"] = doc
			}
		} else {
			hitData["_source"] = map[string]interface{}{}
		}

		// 添加高亮字段
		if len(hit.Fragments) > 0 {
			hitData["highlight"] = hit.Fragments
		}

		// 添加sort值
		if len(hit.Sort) > 0 {
			hitData["sort"] = hit.Sort
		}

		hits = append(hits, hitData)
	}

	return map[string]interface{}{
		"hits": map[string]interface{}{
			"total": map[string]interface{}{
				"value":    searchResult.Total,
				"relation": "eq",
			},
			"max_score": searchResult.MaxScore,
			"hits":      hits,
		},
	}
}

// buildNestedFieldAggregations 构建nested字段聚合响应
func (h *DocumentHandler) buildNestedFieldAggregations(nestedFieldInfo *NestedFieldAggregationInfo, idx bleve.Index, baseQuery query.Query) map[string]interface{} {
	aggs := make(map[string]interface{})

	for aggName, nestedFieldConfig := range nestedFieldInfo.Aggregations {
		logger.Debug("buildNestedFieldAggregations: processing nested field aggregation [%s], path=[%s]", aggName, nestedFieldConfig.Path)

		// 构建nested查询：在nested字段路径内执行match_all查询
		// ES的nested聚合实际上是在nested字段范围内执行聚合
		// 我们需要构建一个nested查询来限制聚合范围
		nestedQueryMap := map[string]interface{}{
			"path":  nestedFieldConfig.Path,
			"query": map[string]interface{}{"match_all": map[string]interface{}{}},
		}

		// 使用DSL解析器解析nested查询
		parser := dsl.NewQueryParser()
		nestedQuery, err := parser.ParseQuery(map[string]interface{}{
			"nested": nestedQueryMap,
		})
		if err != nil {
			logger.Warn("Failed to parse nested query for aggregation [%s]: %v", aggName, err)
			aggs[aggName] = map[string]interface{}{
				"doc_count": 0,
			}
			continue
		}

		// 组合基础查询和nested查询
		combinedQuery := query.NewBooleanQuery([]query.Query{baseQuery, nestedQuery}, nil, nil)

		// 执行搜索获取匹配的文档数
		searchReq := bleve.NewSearchRequest(combinedQuery)
		searchReq.Size = 0 // 不需要返回文档，只需要总数
		searchResult, err := idx.Search(searchReq)
		if err != nil {
			logger.Warn("Failed to execute nested field aggregation search for [%s]: %v", aggName, err)
			aggs[aggName] = map[string]interface{}{
				"doc_count": 0,
			}
			continue
		}

		docCount := searchResult.Total

		// 构建结果
		result := map[string]interface{}{
			"doc_count": docCount,
		}

		// 如果有子聚合，执行子聚合
		if len(nestedFieldConfig.SubAggregations) > 0 {
			logger.Debug("buildNestedFieldAggregations: processing sub-aggregations for [%s], count=%d", aggName, len(nestedFieldConfig.SubAggregations))

			// 解析子聚合
			facets, metricsInfo, compositeInfo, nestedInfo, filterInfo, topHitsInfo, subNestedFieldInfo, err := h.parseAggregations(nestedFieldConfig.SubAggregations)
			if err != nil {
				logger.Warn("Failed to parse sub-aggregations for nested field [%s]: %v", aggName, err)
			} else {
				// 创建搜索请求执行子聚合
				subSearchReq := bleve.NewSearchRequest(combinedQuery)
				subSearchReq.Size = 0
				if len(facets) > 0 {
					subSearchReq.Facets = facets
				}

				subSearchResult, err := idx.Search(subSearchReq)
				if err != nil {
					logger.Warn("Failed to execute sub-aggregations for nested field [%s]: %v", aggName, err)
				} else {
					// 构建子聚合结果
					subAggs := make(map[string]interface{})

					// 处理bucket聚合（terms, range, date_range）
					if len(subSearchResult.Facets) > 0 {
						facetAggs := h.buildAggregations(subSearchResult.Facets, compositeInfo, nestedInfo, topHitsInfo, subNestedFieldInfo, idx, combinedQuery)
						for k, v := range facetAggs {
							subAggs[k] = v
						}
					}

					// 处理metrics聚合
					if metricsInfo != nil && len(metricsInfo.Aggregations) > 0 {
						// 获取所有匹配的文档来计算metrics
						allDocsReq := bleve.NewSearchRequest(combinedQuery)
						allDocsReq.Size = 10000 // 限制大小，避免内存问题
						allDocsResult, err := idx.Search(allDocsReq)
						if err == nil {
							// 创建docCache
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
									subAggs[k] = v
								}
							}
						}
					}

					// 处理composite聚合
					if compositeInfo != nil && len(compositeInfo.Aggregations) > 0 {
						// 检查是否有多字段 composite 聚合
						hasMultiSourceComposite := false
						for _, cfg := range compositeInfo.Aggregations {
							if len(cfg.Sources) > 1 {
								hasMultiSourceComposite = true
								break
							}
						}

						if hasMultiSourceComposite {
							allDocs, err := h.fetchAllDocsForCompositeAgg(idx, combinedQuery, compositeInfo)
							if err == nil {
								compositeAggs := h.buildCompositeAggregationsFromDocs(allDocs, compositeInfo.Aggregations)
								for k, v := range compositeAggs {
									subAggs[k] = v
								}
							}
						} else {
							compositeAggs := h.buildCompositeAggregations(subSearchResult.Facets, compositeInfo.Aggregations)
							for k, v := range compositeAggs {
								subAggs[k] = v
							}
						}
					}

					// 处理filter聚合
					if filterInfo != nil && len(filterInfo.Aggregations) > 0 {
						filterAggs := h.buildFilterAggregations(filterInfo, idx, combinedQuery)
						for k, v := range filterAggs {
							subAggs[k] = v
						}
					}

					// 处理top_hits聚合
					if topHitsInfo != nil && len(topHitsInfo.Aggregations) > 0 {
						for topHitsName, topHitsConfig := range topHitsInfo.Aggregations {
							topHitsResult := h.buildTopHitsAggregation(topHitsConfig, idx, combinedQuery)
							if topHitsResult != nil {
								subAggs[topHitsName] = topHitsResult
							}
						}
					}

					// 处理嵌套的nested字段聚合（递归）
					if subNestedFieldInfo != nil && len(subNestedFieldInfo.Aggregations) > 0 {
						subNestedFieldAggs := h.buildNestedFieldAggregations(subNestedFieldInfo, idx, combinedQuery)
						for k, v := range subNestedFieldAggs {
							subAggs[k] = v
						}
					}

					if len(subAggs) > 0 {
						result["aggregations"] = subAggs
					}
				}
			}
		}

		aggs[aggName] = result
	}

	return aggs
}

// getNestedFieldValue 获取嵌套字段的值
func getNestedFieldValue(doc map[string]interface{}, fieldPath string) interface{} {
	parts := strings.Split(fieldPath, ".")
	var current interface{} = doc

	for _, part := range parts {
		if m, ok := current.(map[string]interface{}); ok {
			current = m[part]
		} else {
			return nil
		}
	}

	return current
}

// fetchAllDocsForCompositeAgg 获取所有匹配的文档用于 composite 聚合计算
func (h *DocumentHandler) fetchAllDocsForCompositeAgg(
	idx bleve.Index,
	query query.Query,
	compositeAggInfo *CompositeAggregationInfo,
) ([]map[string]interface{}, error) {
	// 收集所有需要的字段
	fieldsNeeded := make(map[string]bool)
	for _, cfg := range compositeAggInfo.Aggregations {
		for _, source := range cfg.Sources {
			fieldsNeeded[source.Terms.Field] = true
		}
	}

	// 创建搜索请求，获取所有匹配的文档
	// 使用分页避免内存问题
	const batchSize = 10000
	var allDocs []map[string]interface{}

	advancedIdx, err := idx.Advanced()
	if err != nil {
		return nil, fmt.Errorf("failed to get advanced index: %w", err)
	}

	reader, err := advancedIdx.Reader()
	if err != nil {
		return nil, fmt.Errorf("failed to get index reader: %w", err)
	}
	defer reader.Close()

	// 使用 bleve 搜索获取所有匹配的文档 ID
	from := 0
	for {
		searchReq := bleve.NewSearchRequestOptions(query, batchSize, from, false)
		searchReq.Fields = []string{} // 不需要返回字段，我们会单独获取

		result, err := idx.Search(searchReq)
		if err != nil {
			return nil, fmt.Errorf("failed to search: %w", err)
		}

		if len(result.Hits) == 0 {
			break
		}

		// 获取每个文档的字段值
		for _, hit := range result.Hits {
			doc, err := reader.Document(hit.ID)
			if err != nil || doc == nil {
				continue
			}

			docFields := h.extractDocumentFields(doc)
			// 只保留需要的字段
			filteredDoc := make(map[string]interface{})
			for field := range fieldsNeeded {
				if val := getNestedFieldValue(docFields, field); val != nil {
					filteredDoc[field] = val
				}
			}
			allDocs = append(allDocs, filteredDoc)
		}

		from += batchSize
		if len(result.Hits) < batchSize {
			break
		}

		// 安全限制：最多处理 100 万文档
		if from >= 1000000 {
			logger.Warn("Composite aggregation hit 1M document limit")
			break
		}
	}

	return allDocs, nil
}

// buildCompositeAggregations 构建composite聚合响应（兼容旧接口，使用 facets）
func (h *DocumentHandler) buildCompositeAggregations(facets search.FacetResults, compositeAggs map[string]*CompositeAggregationConfig) map[string]interface{} {
	aggs := make(map[string]interface{})

	for aggName, compositeAgg := range compositeAggs {
		// 构建composite聚合的buckets
		buckets := make([]map[string]interface{}, 0)

		// 从facets中提取每个source的结果
		sourceFacets := make(map[string]*search.FacetResult)
		for _, source := range compositeAgg.Sources {
			facetName := fmt.Sprintf("%s_%s", aggName, source.Name)
			if facet, exists := facets[facetName]; exists {
				sourceFacets[source.Name] = facet
			}
		}

		// 如果只有一个source，直接使用其结果
		if len(compositeAgg.Sources) == 1 {
			firstSource := compositeAgg.Sources[0]
			if facet, exists := sourceFacets[firstSource.Name]; exists && facet.Terms != nil {
				terms := facet.Terms.Terms()
				for _, term := range terms {
					key := make(map[string]interface{})
					key[firstSource.Name] = term.Term

					buckets = append(buckets, map[string]interface{}{
						"key":       key,
						"doc_count": term.Count,
					})
				}
			}
		} else if len(compositeAgg.Sources) > 1 {
			// 多个source时，facets 无法正确计算组合
			// 返回空结果，需要使用 buildCompositeAggregationsFromDocs
			logger.Warn("Composite aggregation with multiple sources requires document iteration, returning empty result")
		}

		aggs[aggName] = map[string]interface{}{
			"buckets": buckets,
		}
	}

	return aggs
}

// buildAggregations 构建聚合响应（支持嵌套聚合）
func (h *DocumentHandler) buildAggregations(facets search.FacetResults, compositeAggInfo *CompositeAggregationInfo, nestedAggInfo *NestedAggregationInfo, topHitsAggInfo *TopHitsAggregationInfo, nestedFieldAggInfo *NestedFieldAggregationInfo, idx bleve.Index, baseQuery query.Query) map[string]interface{} {
	aggs := make(map[string]interface{})

	// 收集所有composite相关的facet名称，用于过滤
	compositeFacetNames := make(map[string]bool)
	if compositeAggInfo != nil {
		for aggName, compositeAgg := range compositeAggInfo.Aggregations {
			for _, source := range compositeAgg.Sources {
				facetName := fmt.Sprintf("%s_%s", aggName, source.Name)
				compositeFacetNames[facetName] = true
			}
		}
	}

	for name, facet := range facets {
		// 跳过composite相关的facet（它们已经在buildCompositeAggregations中处理）
		if compositeFacetNames[name] {
			continue
		}

		agg := map[string]interface{}{}

		// 处理term facets
		if facet.Terms != nil {
			terms := facet.Terms.Terms()
			if len(terms) > 0 {
				buckets := make([]map[string]interface{}, 0, len(terms))
				for i, term := range terms {
					// 关键修复：转换term.Term的类型以匹配ES行为
					// 添加调试日志以诊断编码问题
					if logger.IsDebugEnabled() {
						logger.Debug("buildAggregations: facet[%s] term[%d] - Term: %q (type: %T, bytes: %v, len: %d)",
							name, i, term.Term, term.Term, []byte(term.Term), len(term.Term))
					}
					key := h.convertFacetTermToTypedValue(term.Term)
					// 如果key是空字符串，说明是shift>0的PrefixCoded term，应该被过滤掉
					if keyStr, ok := key.(string); ok && keyStr == "" {
						if logger.IsDebugEnabled() {
							logger.Debug("buildAggregations: filtering out term[%d] (shift>0 PrefixCoded term)", i)
						}
						continue
					}
					bucket := map[string]interface{}{
						"key":       key, // 使用转换后的类型化值
						"doc_count": term.Count,
					}

					// 处理嵌套聚合：为当前bucket执行子聚合
					if nestedAggInfo != nil {
						if subAggs, hasSubAggs := nestedAggInfo.SubAggregations[name]; hasSubAggs {
							logger.Debug("buildAggregations: processing nested aggregations for parentAgg=[%s], bucketKey=%v, subAggs count=%d", name, key, len(subAggs))
							// 获取该bucket对应的字段名
							fieldName, hasField := nestedAggInfo.FieldMapping[name]
							if hasField {
								// 为bucket创建term查询
								bucketQuery := h.buildTermQueryForBucket(fieldName, key)
								if bucketQuery != nil {
									// 组合基础查询和bucket查询
									combinedQuery := query.NewBooleanQuery([]query.Query{baseQuery, bucketQuery}, nil, nil)
									// 执行子聚合
									subAggResults := h.buildNestedAggregationsForBucket(name, key, subAggs, idx, combinedQuery)
									if len(subAggResults) > 0 {
										bucket["aggregations"] = subAggResults
										logger.Debug("buildAggregations: added nested aggregations to bucket, result count=%d", len(subAggResults))
									} else {
										logger.Debug("buildAggregations: nested aggregations returned empty result for bucket")
									}
								} else {
									logger.Debug("buildAggregations: failed to build bucket query for field=[%s], key=%v", fieldName, key)
								}
							} else {
								logger.Debug("buildAggregations: no field mapping found for parentAgg=[%s]", name)
							}
						}
					}

					buckets = append(buckets, bucket)
				}
				agg["buckets"] = buckets
			}
		}

		// 处理numeric range facets
		if len(facet.NumericRanges) > 0 {
			buckets := make([]map[string]interface{}, len(facet.NumericRanges))
			for i, nr := range facet.NumericRanges {
				bucket := map[string]interface{}{
					"key":       nr.Name,
					"from":      nr.Min,
					"to":        nr.Max,
					"doc_count": nr.Count,
				}

				// 处理嵌套聚合（range聚合的嵌套）
				if nestedAggInfo != nil {
					if subAggs, hasSubAggs := nestedAggInfo.SubAggregations[name]; hasSubAggs {
						// 获取字段名
						fieldName, hasField := nestedAggInfo.FieldMapping[name]
						if hasField {
							// 为range bucket创建范围查询
							rangeQuery := h.buildNumericRangeQueryForBucket(fieldName, nr.Min, nr.Max)
							if rangeQuery != nil {
								combinedQuery := query.NewBooleanQuery([]query.Query{baseQuery, rangeQuery}, nil, nil)
								subAggResults := h.buildNestedAggregationsForBucket(name, nr.Name, subAggs, idx, combinedQuery)
								if len(subAggResults) > 0 {
									bucket["aggregations"] = subAggResults
								}
							}
						}
					}
				}

				buckets[i] = bucket
			}
			agg["buckets"] = buckets
		}

		// 处理date range facets
		if len(facet.DateRanges) > 0 {
			buckets := make([]map[string]interface{}, len(facet.DateRanges))
			for i, dr := range facet.DateRanges {
				bucket := map[string]interface{}{
					"key":       dr.Name,
					"from":      dr.Start,
					"to":        dr.End,
					"doc_count": dr.Count,
				}

				// 处理嵌套聚合（date range聚合的嵌套）
				if nestedAggInfo != nil {
					if subAggs, hasSubAggs := nestedAggInfo.SubAggregations[name]; hasSubAggs {
						// 获取字段名
						fieldName, hasField := nestedAggInfo.FieldMapping[name]
						if hasField {
							// 为date range bucket创建日期范围查询
							dateRangeQuery := h.buildDateRangeQueryForBucket(fieldName, dr.Start, dr.End)
							if dateRangeQuery != nil {
								combinedQuery := query.NewBooleanQuery([]query.Query{baseQuery, dateRangeQuery}, nil, nil)
								subAggResults := h.buildNestedAggregationsForBucket(name, dr.Name, subAggs, idx, combinedQuery)
								if len(subAggResults) > 0 {
									bucket["aggregations"] = subAggResults
								}
							}
						}
					}
				}

				buckets[i] = bucket
			}
			agg["buckets"] = buckets
		}

		aggs[name] = agg
	}

	return aggs
}

// convertFacetTermToTypedValue 将Bleve facet返回的term转换为适当的类型
// ES的terms聚合应该返回与原始字段类型相同类型的值
// 关键：Bleve的数字字段使用PrefixCoded编码，需要先解码
func (h *DocumentHandler) convertFacetTermToTypedValue(term interface{}) interface{} {
	// 处理不同类型的term输入
	var termBytes []byte
	switch v := term.(type) {
	case string:
		termBytes = []byte(v)
	case []byte:
		termBytes = v
	default:
		// 尝试转换为字符串
		termStr := fmt.Sprintf("%v", v)
		termBytes = []byte(termStr)
		logger.Warn("convertFacetTermToTypedValue: unexpected term type %T, value: %v", v, v)
	}

	// 关键修复：先检查是否是PrefixCoded编码的数字
	// PrefixCoded的第一个字节在0x20-0x5F范围内（ShiftStartInt64 + shift）
	if len(termBytes) > 0 {
		firstByte := termBytes[0]
		// PrefixCoded的第一个字节范围：0x20 (shift=0) 到 0x5F (shift=63)
		if firstByte >= 0x20 && firstByte <= 0x5F {
			// 尝试作为PrefixCoded解码
			// 注意：Bleve的数字字段在倒排索引中使用PrefixCoded编码
			// 只有shift=0的term代表完整的数字值，shift>0的term是用于范围查询优化的中间值
			// 对于terms聚合，我们只需要shift=0的完整值
			if valid, shift := numeric.ValidPrefixCodedTermBytes(termBytes); valid {
				// 只处理shift=0的完整值（参考NumericFacetBuilder的实现）
				if shift == 0 {
					if i64, err := numeric.PrefixCoded(termBytes).Int64(); err == nil {
						// 转换为float64（ES的数字类型）
						floatVal := numeric.Int64ToFloat64(i64)

						// 添加调试日志
						if logger.IsDebugEnabled() {
							logger.Debug("convertFacetTermToTypedValue: decoded PrefixCoded term (shift=0, bytes=%v, len=%d) -> int64=%d, float64=%v",
								termBytes, len(termBytes), i64, floatVal)
						}

						// 如果是整数，返回整数类型
						if floatVal == float64(int64(floatVal)) {
							return int64(floatVal)
						}
						return floatVal
					} else {
						// 解码失败，记录警告
						logger.Warn("convertFacetTermToTypedValue: PrefixCoded term (shift=0) decode failed: %v, bytes=%v",
							err, termBytes)
					}
				} else {
					// shift>0的term，跳过（这些是用于范围查询优化的中间值，不应该出现在facet结果中）
					// 如果出现了，说明Bleve的实现有问题，或者字段映射有问题
					if logger.IsDebugEnabled() {
						logger.Debug("convertFacetTermToTypedValue: skipping PrefixCoded term with shift=%d (only shift=0 terms should appear in facets), bytes=%v",
							shift, termBytes)
					}
					// 返回空字符串，这样这个bucket会被过滤掉
					return ""
				}
			} else {
				// 第一个字节在范围内，但验证失败
				// 可能是长度不匹配（term被截断或包含额外数据）
				if logger.IsDebugEnabled() {
					logger.Debug("convertFacetTermToTypedValue: first byte in PrefixCoded range (0x%02x) but validation failed, bytes=%v (len=%d)",
						firstByte, termBytes, len(termBytes))
				}
			}
		}
	}

	// 如果不是PrefixCoded，按字符串处理
	termStr := string(termBytes)
	termStr = strings.TrimSpace(termStr)

	if termStr == "" {
		return ""
	}

	// 尝试转换为数字类型（可能是文本字段存储的数字字符串）
	// 优先尝试整数
	if intVal, err := strconv.Atoi(termStr); err == nil {
		return intVal
	}
	// 尝试浮点数
	if floatVal, err := strconv.ParseFloat(termStr, 64); err == nil {
		// 如果是整数形式的浮点数，返回整数
		if floatVal == float64(int64(floatVal)) {
			return int64(floatVal)
		}
		return floatVal
	}
	// 尝试转换为布尔类型
	if boolVal, err := strconv.ParseBool(termStr); err == nil {
		return boolVal
	}
	// 如果都不是，返回原始字符串
	return termStr
}

// GlobalSearch 全局搜索API - 在所有索引中搜索
// GET /_search
// POST /_search
func (h *DocumentHandler) GlobalSearch(w http.ResponseWriter, r *http.Request) {
	// 简单实现：返回空结果（用于测试路由是否工作）
	searchResponse := map[string]interface{}{
		"took":      0,
		"timed_out": false,
		"_shards": map[string]interface{}{
			"total":      0,
			"successful": 0,
			"skipped":    0,
			"failed":     0,
		},
		"hits": map[string]interface{}{
			"total": map[string]interface{}{
				"value":    0,
				"relation": "eq",
			},
			"max_score": nil,
			"hits":      []map[string]interface{}{},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(searchResponse); err != nil {
		logger.Error("Failed to encode global search response: %v", err)
	}
}

// parseSourceField 解析 _source 字段
// 支持格式：
// - []string: ["field1", "field2"]
// - SourceConfig: {"includes": ["field1"], "excludes": ["field2"]}
// - bool: true/false
// - nil: 返回所有字段
func (h *DocumentHandler) parseSourceField(source interface{}) []string {
	if source == nil {
		return nil // 返回所有字段
	}

	// 处理布尔值
	if include, ok := source.(bool); ok {
		if !include {
			return []string{} // false: 不返回任何字段
		}
		return nil // true: 返回所有字段
	}

	// 处理字符串数组
	if fields, ok := source.([]interface{}); ok {
		result := make([]string, 0, len(fields))
		for _, field := range fields {
			if str, ok := field.(string); ok {
				result = append(result, str)
			}
		}
		return result
	}

	// 处理对象格式 {"includes": [...], "excludes": [...]}
	if sourceMap, ok := source.(map[string]interface{}); ok {
		includes := make([]string, 0)
		if includesRaw, ok := sourceMap["includes"].([]interface{}); ok {
			for _, field := range includesRaw {
				if str, ok := field.(string); ok {
					includes = append(includes, str)
				}
			}
		}
		// 注意：excludes 在 Bleve 中不支持，这里只返回 includes
		// 如果需要支持 excludes，需要在返回结果时过滤
		return includes
	}

	return nil
}

// processJoinQueries 处理查询中的 join 查询（has_child/has_parent/percolate）
// 递归遍历查询树，找到并展开特殊查询
func (h *DocumentHandler) processJoinQueries(idx bleve.Index, q query.Query) (query.Query, error) {
	// 检查当前查询是否是 join 查询
	if info := dsl.GetJoinQueryInfo(q); info != nil {
		// 清理注册的 join 查询信息
		defer dsl.UnregisterJoinQuery(q)

		// 根据类型执行两阶段查询
		switch info.Type {
		case dsl.JoinQueryTypeHasChild:
			return dsl.ExecuteHasChildQuery(nil, idx, info.TypeName, info.InnerQuery, info.Boost)
		case dsl.JoinQueryTypeHasParent:
			return dsl.ExecuteHasParentQuery(nil, idx, info.TypeName, info.InnerQuery, info.Boost)
		}
	}

	// 检查当前查询是否是 percolate 查询
	if info := dsl.GetPercolateQueryInfo(q); info != nil {
		// 清理注册的 percolate 查询信息
		defer dsl.UnregisterPercolateQuery(q)

		// 执行 percolate 查询
		return dsl.ExecutePercolateQuery(nil, idx, info)
	}

	// 递归处理子查询
	switch tq := q.(type) {
	case *query.ConjunctionQuery:
		for i, child := range tq.Conjuncts {
			processed, err := h.processJoinQueries(idx, child)
			if err != nil {
				return nil, err
			}
			tq.Conjuncts[i] = processed
		}
		return tq, nil

	case *query.DisjunctionQuery:
		for i, child := range tq.Disjuncts {
			processed, err := h.processJoinQueries(idx, child)
			if err != nil {
				return nil, err
			}
			tq.Disjuncts[i] = processed
		}
		return tq, nil

	case *query.BooleanQuery:
		if tq.Must != nil {
			processed, err := h.processJoinQueries(idx, tq.Must)
			if err != nil {
				return nil, err
			}
			tq.Must = processed
		}
		if tq.Should != nil {
			processed, err := h.processJoinQueries(idx, tq.Should)
			if err != nil {
				return nil, err
			}
			tq.Should = processed
		}
		if tq.MustNot != nil {
			processed, err := h.processJoinQueries(idx, tq.MustNot)
			if err != nil {
				return nil, err
			}
			tq.MustNot = processed
		}
		return tq, nil
	}

	return q, nil
}
