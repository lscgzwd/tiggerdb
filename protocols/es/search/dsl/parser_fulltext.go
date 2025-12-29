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
	"fmt"
	"strings"

	"github.com/lscgzwd/tiggerdb/logger"
	"github.com/lscgzwd/tiggerdb/search/query"
)

// ========== 全文查询类型 ==========

// parseMatchAll 解析match_all查询
func (p *QueryParser) parseMatchAll(body interface{}) (query.Query, error) {
	return query.NewMatchAllQuery(), nil
}

// parseMatch 解析match查询
// ES格式: {"match": {"field": "value"}} 或 {"match": {"field": {"query": "value", "operator": "and"}}}
func (p *QueryParser) parseMatch(body interface{}) (query.Query, error) {
	matchMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("match query body must be a map")
	}

	for field, value := range matchMap {
		field = p.normalizeFieldName(field)

		var queryText string
		var operator string = "or"
		var boost float64 = 1.0

		switch v := value.(type) {
		case string:
			queryText = v
		case map[string]interface{}:
			if q, ok := v["query"].(string); ok {
				queryText = q
			} else if q, ok := v["query"].(float64); ok {
				queryText = fmt.Sprintf("%v", q)
			}
			if op, ok := v["operator"].(string); ok {
				operator = strings.ToLower(op)
			}
			if b, ok := v["boost"].(float64); ok {
				boost = b
			}
		case float64:
			queryText = fmt.Sprintf("%v", v)
		case bool:
			queryText = fmt.Sprintf("%v", v)
		default:
			return nil, fmt.Errorf("invalid match query value type: %T", value)
		}

		if queryText == "" {
			return query.NewMatchNoneQuery(), nil
		}

		matchQuery := query.NewMatchQuery(queryText)
		matchQuery.SetField(field)
		matchQuery.SetBoost(boost)

		if operator == "and" {
			matchQuery.SetOperator(query.MatchQueryOperatorAnd)
		}

		return matchQuery, nil
	}

	return nil, fmt.Errorf("match query must have a field")
}

// parseMatchPhrase 解析match_phrase查询
func (p *QueryParser) parseMatchPhrase(body interface{}) (query.Query, error) {
	matchMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("match_phrase query body must be a map")
	}

	for field, value := range matchMap {
		field = p.normalizeFieldName(field)

		var phraseText string
		switch v := value.(type) {
		case string:
			phraseText = v
		case map[string]interface{}:
			if q, ok := v["query"].(string); ok {
				phraseText = q
			}
		}

		if phraseText == "" {
			return query.NewMatchNoneQuery(), nil
		}

		phraseQuery := query.NewMatchPhraseQuery(phraseText)
		phraseQuery.SetField(field)
		return phraseQuery, nil
	}

	return nil, fmt.Errorf("match_phrase query must have a field")
}

// parseMatchPhrasePrefix 解析match_phrase_prefix查询
func (p *QueryParser) parseMatchPhrasePrefix(body interface{}) (query.Query, error) {
	matchMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("match_phrase_prefix query body must be a map")
	}

	for field, value := range matchMap {
		field = p.normalizeFieldName(field)

		var queryText string
		switch v := value.(type) {
		case string:
			queryText = v
		case map[string]interface{}:
			if q, ok := v["query"].(string); ok {
				queryText = q
			}
		}

		if queryText == "" {
			return query.NewMatchNoneQuery(), nil
		}

		phraseQuery := query.NewMatchPhraseQuery(queryText)
		phraseQuery.SetField(field)
		return phraseQuery, nil
	}

	return nil, fmt.Errorf("match_phrase_prefix query must have a field")
}

// parseMultiMatch 解析multi_match查询
func (p *QueryParser) parseMultiMatch(body interface{}) (query.Query, error) {
	multiMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("multi_match query body must be a map")
	}

	queryText, _ := multiMap["query"].(string)
	if queryText == "" {
		return query.NewMatchNoneQuery(), nil
	}

	fields, _ := multiMap["fields"].([]interface{})
	if len(fields) == 0 {
		matchQuery := query.NewMatchQuery(queryText)
		return matchQuery, nil
	}

	var queries []query.Query
	for _, f := range fields {
		if fieldName, ok := f.(string); ok {
			fieldName = p.normalizeFieldName(fieldName)
			mq := query.NewMatchQuery(queryText)
			mq.SetField(fieldName)
			queries = append(queries, mq)
		}
	}

	if len(queries) == 1 {
		return queries[0], nil
	}

	disjQuery := query.NewDisjunctionQuery(queries)
	disjQuery.SetMin(1)
	return disjQuery, nil
}

// parseQueryString 解析query_string查询
func (p *QueryParser) parseQueryString(body interface{}) (query.Query, error) {
	qsMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("query_string query body must be a map")
	}

	queryText, _ := qsMap["query"].(string)
	if queryText == "" {
		return query.NewMatchNoneQuery(), nil
	}

	defaultField, _ := qsMap["default_field"].(string)
	defaultField = p.normalizeFieldName(defaultField)

	// 如果有 default_field，构建 field:query 格式
	var qs *query.QueryStringQuery
	if defaultField != "" {
		qs = query.NewQueryStringQuery(defaultField + ":" + queryText)
	} else {
		qs = query.NewQueryStringQuery(queryText)
	}

	if boost, ok := qsMap["boost"].(float64); ok {
		qs.SetBoost(boost)
	}

	return qs, nil
}

// parseSimpleQueryString 解析simple_query_string查询
// ES格式: {"simple_query_string": {"query": "text", "fields": [...]}}
// 实现：使用query_string查询（简化实现）
func (p *QueryParser) parseSimpleQueryString(body interface{}) (query.Query, error) {
	return p.parseQueryString(body)
}

// parseMatchNone 解析match_none查询
// ES格式: {"match_none": {}}
func (p *QueryParser) parseMatchNone(body interface{}) (query.Query, error) {
	matchNoneQuery := query.NewMatchNoneQuery()

	if bodyMap, ok := body.(map[string]interface{}); ok {
		if boost, ok := bodyMap["boost"].(float64); ok {
			matchNoneQuery.SetBoost(boost)
		}
	}

	return matchNoneQuery, nil
}

// parseMatchBoolPrefix 解析match_bool_prefix查询
// ES格式: {"match_bool_prefix": {"field": "query text"}}
// 实现：使用match查询（简化实现）
func (p *QueryParser) parseMatchBoolPrefix(body interface{}) (query.Query, error) {
	return p.parseMatch(body)
}

// parseCommon 解析common terms查询
// ES格式: {"common": {"field": {"query": "text", "cutoff_frequency": 0.001}}}
// 注意：common terms 查询在 ES 7.x 中已弃用
func (p *QueryParser) parseCommon(body interface{}) (query.Query, error) {
	logger.Warn("common terms query is deprecated, using match query instead")
	return p.parseMatch(body)
}
