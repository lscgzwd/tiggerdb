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

package dsl

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/lscgzwd/tiggerdb/logger"
	"github.com/lscgzwd/tiggerdb/script"
	"github.com/lscgzwd/tiggerdb/search/query"
)

// QueryParser ES Query DSL解析器
type QueryParser struct {
	optimizer *QueryOptimizer      // 查询优化器
	registry  *QueryParserRegistry // P2-2: 查询解析策略注册表（策略模式）
}

// NewQueryParser 创建新的查询解析器
func NewQueryParser() *QueryParser {
	parser := &QueryParser{
		optimizer: NewQueryOptimizer(),      // 默认启用优化器
		registry:  NewQueryParserRegistry(), // P2-2: 初始化策略注册表
	}
	// P2-2: 设置所有策略的parser引用
	parser.setupStrategies()
	return parser
}

// setupStrategies P2-2: 设置所有策略的parser引用
func (p *QueryParser) setupStrategies() {
	for _, strategy := range p.registry.strategies {
		if baseStrategy, ok := strategy.(interface{ SetParser(*QueryParser) }); ok {
			baseStrategy.SetParser(p)
		}
	}
}

// SetOptimizerEnabled 设置是否启用查询优化器
func (p *QueryParser) SetOptimizerEnabled(enabled bool) {
	if p.optimizer == nil {
		p.optimizer = NewQueryOptimizer()
	}
	p.optimizer.SetEnabled(enabled)
}

// normalizeFieldName 规范化字段名
// ES 中 .keyword 后缀表示使用 keyword 子字段进行精确匹配
// 但 Bleve 没有这个概念，所以需要去除 .keyword 后缀
// 同样，.text 后缀也需要去除
func (p *QueryParser) normalizeFieldName(field string) string {
	// 去除 .keyword 后缀
	if strings.HasSuffix(field, ".keyword") {
		return strings.TrimSuffix(field, ".keyword")
	}
	// 去除 .text 后缀
	if strings.HasSuffix(field, ".text") {
		return strings.TrimSuffix(field, ".text")
	}
	return field
}

// ParseQuery 解析ES Query DSL并转换为bleve Query
// ES 查询 DSL 规范：query 对象应该只包含一个查询类型
// 例如：{"match": {...}} 或 {"bool": {...}}，不应该同时包含多个查询类型
func (p *QueryParser) ParseQuery(queryMap map[string]interface{}) (query.Query, error) {
	// 调试：打印解析前的查询结构
	if logger.IsDebugEnabled() {
		queryJSON, _ := json.MarshalIndent(queryMap, "", "  ")
		logger.Debug("ParseQuery - Input query map:\n%s", string(queryJSON))
	}
	if len(queryMap) == 0 {
		// 返回match_all查询
		return query.NewMatchAllQuery(), nil
	}

	// 检查是否包含多个查询类型（ES 规范不允许）
	if len(queryMap) > 1 {
		queryTypes := make([]string, 0, len(queryMap))
		for queryType := range queryMap {
			queryTypes = append(queryTypes, queryType)
		}
		return nil, fmt.Errorf("query object must contain exactly one query type, but found %d types: %v", len(queryMap), queryTypes)
	}

	// P2-2: 使用策略模式解析查询（替代大的switch语句，提升可维护性和扩展性）
	for queryType, queryBody := range queryMap {
		// 从注册表获取策略
		strategy, exists := p.registry.Get(queryType)
		if !exists {
			logger.Warn("Unsupported query type: %s", queryType)
			return nil, fmt.Errorf("unsupported query type: %s", queryType)
		}

		// 使用策略解析查询
		parsedQuery, err := strategy.Parse(queryBody)

		// 如果解析出错，直接返回错误
		if err != nil {
			return nil, err
		}

		// 应用查询优化器
		if parsedQuery != nil && p.optimizer != nil {
			optimizedQuery, optErr := p.optimizer.Optimize(parsedQuery)
			if optErr != nil {
				// 优化失败不影响查询，返回原查询
				logger.Warn("Query optimization failed: %v", optErr)
				return parsedQuery, nil
			}
			return optimizedQuery, nil
		}

		return parsedQuery, nil
	}

	// 理论上不应该到达这里（因为 len(queryMap) > 0 且已处理）
	return query.NewMatchAllQuery(), nil
}

// toFloat64 将interface{}转换为float64
func (p *QueryParser) toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case string:
		return strconv.ParseFloat(val, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

// addPathPrefixToQuery 为查询中的所有字段添加路径前缀（用于嵌套查询）
func (p *QueryParser) addPathPrefixToQuery(q query.Query, pathPrefix string) {
	if q == nil {
		return
	}

	// 处理FieldableQuery（有字段的查询）
	if fieldable, ok := q.(query.FieldableQuery); ok {
		currentField := fieldable.Field()
		if currentField != "" && !strings.HasPrefix(currentField, pathPrefix+".") {
			// 添加路径前缀（避免重复添加）
			prefixedField := pathPrefix + "." + currentField
			fieldable.SetField(prefixedField)
		}
		return
	}

	// 处理BooleanQuery（需要递归处理子查询）
	if boolQuery, ok := q.(*query.BooleanQuery); ok {
		// 处理must查询（可能是ConjunctionQuery）
		if boolQuery.Must != nil {
			p.addPathPrefixToQuery(boolQuery.Must, pathPrefix)
		}
		// 处理should查询（可能是DisjunctionQuery）
		if boolQuery.Should != nil {
			p.addPathPrefixToQuery(boolQuery.Should, pathPrefix)
		}
		// 处理must_not查询（可能是DisjunctionQuery）
		if boolQuery.MustNot != nil {
			p.addPathPrefixToQuery(boolQuery.MustNot, pathPrefix)
		}
		// 处理filter查询
		if boolQuery.Filter != nil {
			p.addPathPrefixToQuery(boolQuery.Filter, pathPrefix)
		}
		return
	}

	// 处理DisjunctionQuery（OR查询）
	if disjQuery, ok := q.(*query.DisjunctionQuery); ok {
		for i := range disjQuery.Disjuncts {
			p.addPathPrefixToQuery(disjQuery.Disjuncts[i], pathPrefix)
		}
		return
	}

	// 处理ConjunctionQuery（AND查询）
	if conjQuery, ok := q.(*query.ConjunctionQuery); ok {
		for i := range conjQuery.Conjuncts {
			p.addPathPrefixToQuery(conjQuery.Conjuncts[i], pathPrefix)
		}
		return
	}

	// 其他查询类型（如match_all）不需要处理字段
}

// ========== 基础查询类型 ==========

// ========== 高级查询类型 ==========

// parseMoreLikeThis 解析more_like_this查询
// ES格式: {"more_like_this": {"fields": [...], "like": [...], "min_term_freq": 1}}
// 实现：使用match查询（简化实现，不支持相似度计算）
func (p *QueryParser) parseMoreLikeThis(body interface{}) (query.Query, error) {
	// more_like_this查询需要复杂的相似度计算，简化实现：使用match查询
	// 如果提供了like字段，尝试提取文本
	if mltMap, ok := body.(map[string]interface{}); ok {
		if likeVal, ok := mltMap["like"]; ok {
			// 尝试从like中提取文本
			if likeArr, ok := likeVal.([]interface{}); ok && len(likeArr) > 0 {
				if likeMap, ok := likeArr[0].(map[string]interface{}); ok {
					if text, ok := likeMap["_source"].(string); ok {
						// 创建match查询
						return p.parseMatch(map[string]interface{}{"_all": text})
					}
				}
			}
		}
	}
	return query.NewMatchAllQuery(), nil
}

// parseScript 解析script查询
// ES格式: {"script": {"script": {"source": "...", "params": {...}}}}
func (p *QueryParser) parseScript(body interface{}) (query.Query, error) {
	scriptMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("script query body must be a map")
	}

	// 解析脚本对象
	var scriptData interface{}
	if s, ok := scriptMap["script"]; ok {
		scriptData = s
	} else {
		// 如果没有嵌套的 script 字段，整个 body 就是脚本
		scriptData = scriptMap
	}

	s, err := script.ParseScript(scriptData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse script: %w", err)
	}

	return query.NewScriptQueryFromScript(s), nil
}

// parseScriptScore 解析script_score查询
// ES格式: {"script_score": {"query": {...}, "script": {...}}}
func (p *QueryParser) parseScriptScore(body interface{}) (query.Query, error) {
	scriptMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("script_score query must be an object")
	}

	// 解析内部查询
	var innerQuery query.Query
	var err error
	if innerQueryMap, ok := scriptMap["query"].(map[string]interface{}); ok {
		innerQuery, err = p.ParseQuery(innerQueryMap)
		if err != nil {
			return nil, fmt.Errorf("failed to parse script_score inner query: %w", err)
		}
	} else {
		innerQuery = query.NewMatchAllQuery()
	}

	// 解析脚本
	var scriptData interface{}
	if s, ok := scriptMap["script"]; ok {
		scriptData = s
	} else {
		// 没有脚本，返回内部查询
		return innerQuery, nil
	}

	s, err := script.ParseScript(scriptData)
	if err != nil {
		logger.Warn("Failed to parse script_score script: %v, using inner query", err)
		return innerQuery, nil
	}

	// 创建 ScriptScoreQuery
	ssq := query.NewScriptScoreQuery(innerQuery, s)

	// 处理 boost
	if boost, ok := scriptMap["boost"].(float64); ok {
		ssq.SetBoost(boost)
	}

	// 处理 min_score
	if minScore, ok := scriptMap["min_score"].(float64); ok {
		ssq.SetMinScore(minScore)
	}

	return ssq, nil
}

// parseFunctionScore 解析function_score查询
// ES格式: {"function_score": {"query": {...}, "functions": [...], "score_mode": "...", "boost_mode": "..."}}
func (p *QueryParser) parseFunctionScore(body interface{}) (query.Query, error) {
	funcMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("function_score query must be an object")
	}

	// 解析内部查询
	var innerQuery query.Query
	var err error
	if innerQueryMap, ok := funcMap["query"].(map[string]interface{}); ok {
		innerQuery, err = p.ParseQuery(innerQueryMap)
		if err != nil {
			return nil, fmt.Errorf("failed to parse function_score inner query: %w", err)
		}
	} else {
		innerQuery = query.NewMatchAllQuery()
	}

	// 创建 FunctionScoreQuery
	fsq := query.NewFunctionScoreQuery(innerQuery)

	// 解析 score_mode
	if sm, ok := funcMap["score_mode"].(string); ok {
		fsq.SetScoreMode(query.ScoreMode(sm))
	}

	// 解析 boost_mode
	if bm, ok := funcMap["boost_mode"].(string); ok {
		fsq.SetBoostMode(query.BoostMode(bm))
	}

	// 解析 max_boost
	if maxBoost, ok := funcMap["max_boost"].(float64); ok {
		fsq.SetMaxBoost(maxBoost)
	}

	// 解析 min_score
	if minScore, ok := funcMap["min_score"].(float64); ok {
		fsq.SetMinScore(minScore)
	}

	// 解析 boost
	if boost, ok := funcMap["boost"].(float64); ok {
		fsq.SetBoost(boost)
	}

	// 解析 functions 数组
	if functions, ok := funcMap["functions"].([]interface{}); ok {
		for _, fn := range functions {
			fnMap, ok := fn.(map[string]interface{})
			if !ok {
				continue
			}
			p.parseFunctionScoreFunction(fsq, fnMap)
		}
	}

	// 解析单个函数（非数组形式）
	p.parseSingleFunction(fsq, funcMap)

	return fsq, nil
}

// parseFunctionScoreFunction 解析单个评分函数
func (p *QueryParser) parseFunctionScoreFunction(fsq *query.FunctionScoreQuery, fnMap map[string]interface{}) {
	var filter query.Query
	var scoreFn query.ScoreFunction
	weight := 1.0

	// 解析过滤器
	if filterMap, ok := fnMap["filter"].(map[string]interface{}); ok {
		var err error
		filter, err = p.ParseQuery(filterMap)
		if err != nil {
			filter = nil
		}
	}

	// 解析权重
	if w, ok := fnMap["weight"].(float64); ok {
		weight = w
	}

	// 解析 script_score
	if scriptMap, ok := fnMap["script_score"].(map[string]interface{}); ok {
		if scriptData, ok := scriptMap["script"]; ok {
			s, err := script.ParseScript(scriptData)
			if err == nil {
				scoreFn = query.NewScriptScoreFunction(s)
			}
		}
	}

	// 解析 field_value_factor
	if fvf, ok := fnMap["field_value_factor"].(map[string]interface{}); ok {
		field := ""
		factor := 1.0
		modifier := "none"
		missing := 1.0

		if f, ok := fvf["field"].(string); ok {
			field = f
		}
		if f, ok := fvf["factor"].(float64); ok {
			factor = f
		}
		if m, ok := fvf["modifier"].(string); ok {
			modifier = m
		}
		if m, ok := fvf["missing"].(float64); ok {
			missing = m
		}

		if field != "" {
			scoreFn = query.NewFieldValueFactorFunction(field, factor, modifier, missing)
		}
	}

	// 解析 random_score
	if rs, ok := fnMap["random_score"].(map[string]interface{}); ok {
		seed := int64(0)
		field := ""
		if s, ok := rs["seed"].(float64); ok {
			seed = int64(s)
		}
		if f, ok := rs["field"].(string); ok {
			field = f
		}
		scoreFn = query.NewRandomScoreFunction(seed, field)
	}

	// 解析衰减函数 (linear, exp, gauss)
	for _, decayType := range []string{"linear", "exp", "gauss"} {
		if decay, ok := fnMap[decayType].(map[string]interface{}); ok {
			for field, spec := range decay {
				specMap, ok := spec.(map[string]interface{})
				if !ok {
					continue
				}
				origin := 0.0
				scale := 1.0
				offset := 0.0
				decayVal := 0.5

				if o, ok := specMap["origin"].(float64); ok {
					origin = o
				}
				if s, ok := specMap["scale"].(float64); ok {
					scale = s
				}
				if o, ok := specMap["offset"].(float64); ok {
					offset = o
				}
				if d, ok := specMap["decay"].(float64); ok {
					decayVal = d
				}

				scoreFn = query.NewDecayFunction(field, origin, scale, offset, decayVal, decayType)
				break
			}
		}
	}

	// 如果只有权重，创建权重函数
	if scoreFn == nil && weight != 1.0 {
		scoreFn = query.NewWeightFunction(weight)
		weight = 1.0
	}

	if scoreFn != nil {
		fsq.AddFunction(filter, scoreFn, weight)
	}
}

// parseSingleFunction 解析单个函数（非数组形式）
func (p *QueryParser) parseSingleFunction(fsq *query.FunctionScoreQuery, funcMap map[string]interface{}) {
	// 检查是否有顶层的评分函数
	fnMap := make(map[string]interface{})

	if scriptMap, ok := funcMap["script_score"].(map[string]interface{}); ok {
		fnMap["script_score"] = scriptMap
	}
	if fvf, ok := funcMap["field_value_factor"].(map[string]interface{}); ok {
		fnMap["field_value_factor"] = fvf
	}
	if rs, ok := funcMap["random_score"].(map[string]interface{}); ok {
		fnMap["random_score"] = rs
	}
	for _, decayType := range []string{"linear", "exp", "gauss"} {
		if decay, ok := funcMap[decayType].(map[string]interface{}); ok {
			fnMap[decayType] = decay
		}
	}

	if len(fnMap) > 0 {
		p.parseFunctionScoreFunction(fsq, fnMap)
	}
}

// parsePinned 解析pinned查询
// ES格式: {"pinned": {"ids": [...], "organic": {...}}}
// 实现：使用ids查询和bool查询组合
func (p *QueryParser) parsePinned(body interface{}) (query.Query, error) {
	pinnedMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("pinned query must be an object")
	}

	// 解析pinned ids
	var pinnedIds []string
	if idsVal, ok := pinnedMap["ids"].([]interface{}); ok {
		for _, id := range idsVal {
			if idStr, ok := id.(string); ok {
				pinnedIds = append(pinnedIds, idStr)
			}
		}
	}

	// 解析organic查询
	var organicQuery query.Query
	if organicVal, ok := pinnedMap["organic"].(map[string]interface{}); ok {
		var err error
		organicQuery, err = p.ParseQuery(organicVal)
		if err != nil {
			return nil, fmt.Errorf("failed to parse pinned organic query: %w", err)
		}
	} else {
		organicQuery = query.NewMatchAllQuery()
	}

	// 组合查询：pinned ids OR organic query
	if len(pinnedIds) > 0 {
		pinnedQuery := query.NewDocIDQuery(pinnedIds)
		disjQuery := query.NewDisjunctionQuery([]query.Query{pinnedQuery, organicQuery})
		disjQuery.SetMin(1) // 至少匹配一个子查询
		return disjQuery, nil
	}

	return organicQuery, nil
}

// parseWrapper 解析wrapper查询
// ES格式: {"wrapper": {"query": "base64_encoded_query"}}
// 实现：解码base64并解析内部查询
func (p *QueryParser) parseWrapper(body interface{}) (query.Query, error) {
	wrapperMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("wrapper query must be an object")
	}

	// 获取base64编码的查询字符串
	encodedQuery, ok := wrapperMap["query"].(string)
	if !ok || encodedQuery == "" {
		return nil, fmt.Errorf("wrapper query must have 'query' field with base64 encoded string")
	}

	// 解码base64
	decodedBytes, err := base64.StdEncoding.DecodeString(encodedQuery)
	if err != nil {
		// 尝试使用URL安全的base64解码
		decodedBytes, err = base64.URLEncoding.DecodeString(encodedQuery)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64 query: %w", err)
		}
	}

	// 解析JSON
	var queryMap map[string]interface{}
	if err := json.Unmarshal(decodedBytes, &queryMap); err != nil {
		return nil, fmt.Errorf("failed to parse decoded query JSON: %w", err)
	}

	logger.Debug("parseWrapper - Decoded query: %v", queryMap)

	// 递归解析内部查询
	return p.ParseQuery(queryMap)
}

// parsePercolate 解析percolate查询
// ES格式: {"percolate": {"field": "query", "document": {...}}}
// 或者: {"percolate": {"field": "query", "documents": [...]}}
// 实现：完整版本 - 从索引中获取存储的查询并与提供的文档进行匹配
func (p *QueryParser) parsePercolate(body interface{}) (query.Query, error) {
	percolateMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("percolate query must be an object")
	}

	// 获取 percolator 字段名
	field, _ := percolateMap["field"].(string)
	if field == "" {
		field = "query" // 默认字段名
	}

	// 尝试获取单个文档
	document, hasDoc := percolateMap["document"].(map[string]interface{})

	// 尝试获取多个文档
	var documents []map[string]interface{}
	if docsRaw, ok := percolateMap["documents"].([]interface{}); ok {
		for _, doc := range docsRaw {
			if docMap, ok := doc.(map[string]interface{}); ok {
				documents = append(documents, docMap)
			}
		}
	}
	hasDocs := len(documents) > 0

	if !hasDoc && !hasDocs {
		// 尝试获取文档 ID（从索引中获取文档）
		if docID, ok := percolateMap["id"].(string); ok {
			// 简化实现：返回一个匹配该文档 ID 的查询
			idQuery := query.NewDocIDQuery([]string{docID})
			logger.Warn("percolate query with document ID: requires fetching document from index")
			return idQuery, nil
		}
		return nil, fmt.Errorf("percolate query must have 'document', 'documents', or 'id' field")
	}

	// 获取 boost
	boost := 1.0
	if b, ok := percolateMap["boost"].(float64); ok {
		boost = b
	}

	// 创建一个占位查询（match_all），并注册 percolate 查询信息
	// 实际的查询执行将在搜索执行时进行
	placeholderQuery := query.NewMatchAllQuery()
	placeholderQuery.SetBoost(boost)

	// 注册 percolate 查询信息
	RegisterPercolateQuery(placeholderQuery, &PercolateQueryInfo{
		Field:     field,
		Document:  document,
		Documents: documents,
		Boost:     boost,
	})

	logger.Debug("parsePercolate - Registered percolate query for field '%s'", field)

	return placeholderQuery, nil
}

// ========== 父子文档查询 ==========

// parseHasChild 解析has_child查询
// ES格式: {"has_child": {"type": "child_type", "query": {...}}}
// 实现：创建标记查询，在搜索执行时进行两阶段查询
// 返回有匹配子文档的父文档
func (p *QueryParser) parseHasChild(body interface{}) (query.Query, error) {
	hasChildMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("has_child query must be an object")
	}

	// 获取子文档类型
	childType, _ := hasChildMap["type"].(string)
	if childType == "" {
		return nil, fmt.Errorf("has_child query must have 'type' field")
	}

	// 解析内部查询
	innerQueryMap, ok := hasChildMap["query"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("has_child query must have 'query' field")
	}

	parsedInnerQuery, err := p.ParseQuery(innerQueryMap)
	if err != nil {
		return nil, fmt.Errorf("failed to parse has_child inner query: %w", err)
	}

	// 获取 boost
	boost := 1.0
	if b, ok := hasChildMap["boost"].(float64); ok {
		boost = b
	}

	// 创建一个占位查询（match_all），并注册 join 查询信息
	// 实际的两阶段查询将在搜索执行时进行
	placeholderQuery := query.NewMatchAllQuery()
	placeholderQuery.SetBoost(boost)

	// 注册 join 查询信息
	RegisterJoinQuery(placeholderQuery, &JoinQueryInfo{
		Type:       JoinQueryTypeHasChild,
		TypeName:   childType,
		InnerQuery: parsedInnerQuery,
		Boost:      boost,
	})

	logger.Debug("parseHasChild - Registered has_child query for child type '%s'", childType)

	return placeholderQuery, nil
}

// parseHasParent 解析has_parent查询
// ES格式: {"has_parent": {"parent_type": "parent_type", "query": {...}}}
// 实现：创建标记查询，在搜索执行时进行两阶段查询
// 返回有匹配父文档的子文档
func (p *QueryParser) parseHasParent(body interface{}) (query.Query, error) {
	hasParentMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("has_parent query must be an object")
	}

	// 获取父文档类型
	parentType, _ := hasParentMap["parent_type"].(string)
	if parentType == "" {
		return nil, fmt.Errorf("has_parent query must have 'parent_type' field")
	}

	// 解析内部查询
	innerQueryMap, ok := hasParentMap["query"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("has_parent query must have 'query' field")
	}

	parsedInnerQuery, err := p.ParseQuery(innerQueryMap)
	if err != nil {
		return nil, fmt.Errorf("failed to parse has_parent inner query: %w", err)
	}

	// 获取 boost
	boost := 1.0
	if b, ok := hasParentMap["boost"].(float64); ok {
		boost = b
	}

	// 创建一个占位查询（match_all），并注册 join 查询信息
	// 实际的两阶段查询将在搜索执行时进行
	placeholderQuery := query.NewMatchAllQuery()
	placeholderQuery.SetBoost(boost)

	// 注册 join 查询信息
	RegisterJoinQuery(placeholderQuery, &JoinQueryInfo{
		Type:       JoinQueryTypeHasParent,
		TypeName:   parentType,
		InnerQuery: parsedInnerQuery,
		Boost:      boost,
	})

	logger.Debug("parseHasParent - Registered has_parent query for parent type '%s'", parentType)

	return placeholderQuery, nil
}
