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

package search

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/lscgzwd/tiggerdb/search/query"
)

// QueryParser ES Query DSL解析器
type QueryParser struct{}

// NewQueryParser 创建新的查询解析器
func NewQueryParser() *QueryParser {
	return &QueryParser{}
}

// ParseQuery 解析ES Query DSL并转换为bleve Query
// ES 查询 DSL 规范：query 对象应该只包含一个查询类型
// 例如：{"match": {...}} 或 {"bool": {...}}，不应该同时包含多个查询类型
func (p *QueryParser) ParseQuery(queryMap map[string]interface{}) (query.Query, error) {
	if queryMap == nil || len(queryMap) == 0 {
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

	// 遍历查询类型（此时应该只有一个）
	for queryType, queryBody := range queryMap {
		switch queryType {
		case "match_all":
			return p.parseMatchAll(queryBody)
		case "match":
			return p.parseMatch(queryBody)
		case "match_phrase":
			return p.parseMatchPhrase(queryBody)
		case "match_phrase_prefix":
			return p.parseMatchPhrasePrefix(queryBody)
		case "term":
			return p.parseTerm(queryBody)
		case "terms":
			return p.parseTerms(queryBody)
		case "range":
			return p.parseRange(queryBody)
		case "bool":
			return p.parseBool(queryBody)
		case "wildcard":
			return p.parseWildcard(queryBody)
		case "prefix":
			return p.parsePrefix(queryBody)
		case "fuzzy":
			return p.parseFuzzy(queryBody)
		case "regexp":
			return p.parseRegexp(queryBody)
		case "exists":
			return p.parseExists(queryBody)
		case "nested":
			return p.parseNested(queryBody)
		case "multi_match":
			return p.parseMultiMatch(queryBody)
		case "query_string":
			return p.parseQueryString(queryBody)
		default:
			log.Printf("WARN: Unsupported query type: %s", queryType)
			return nil, fmt.Errorf("unsupported query type: %s", queryType)
		}
	}

	// 理论上不应该到达这里（因为 len(queryMap) > 0 且已处理）
	return query.NewMatchAllQuery(), nil
}

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
		var queryStr string
		var operator string = "or" // 默认OR

		// 处理简单格式: {"field": "value"}
		if strValue, ok := value.(string); ok {
			queryStr = strValue
		} else if valueMap, ok := value.(map[string]interface{}); ok {
			// 处理复杂格式: {"field": {"query": "value", "operator": "and"}}
			if q, ok := valueMap["query"].(string); ok {
				queryStr = q
			} else {
				return nil, fmt.Errorf("match query must have 'query' field")
			}
			if op, ok := valueMap["operator"].(string); ok {
				operator = op
			}
		} else {
			return nil, fmt.Errorf("invalid match query value type")
		}

		matchQuery := query.NewMatchQuery(queryStr)
		matchQuery.SetField(field)

		// 设置操作符
		if operator == "and" {
			matchQuery.SetOperator(query.MatchQueryOperatorAnd)
		} else {
			matchQuery.SetOperator(query.MatchQueryOperatorOr)
		}

		return matchQuery, nil
	}

	return nil, fmt.Errorf("match query must have at least one field")
}

// parseMatchPhrase 解析match_phrase查询
func (p *QueryParser) parseMatchPhrase(body interface{}) (query.Query, error) {
	matchMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("match_phrase query body must be a map")
	}

	for field, value := range matchMap {
		var phraseStr string

		if strValue, ok := value.(string); ok {
			phraseStr = strValue
		} else if valueMap, ok := value.(map[string]interface{}); ok {
			if q, ok := valueMap["query"].(string); ok {
				phraseStr = q
			} else {
				return nil, fmt.Errorf("match_phrase query must have 'query' field")
			}
		} else {
			return nil, fmt.Errorf("invalid match_phrase query value type")
		}

		phraseQuery := query.NewMatchPhraseQuery(phraseStr)
		phraseQuery.SetField(field)
		return phraseQuery, nil
	}

	return nil, fmt.Errorf("match_phrase query must have at least one field")
}

// parseMatchPhrasePrefix 解析match_phrase_prefix查询
func (p *QueryParser) parseMatchPhrasePrefix(body interface{}) (query.Query, error) {
	matchMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("match_phrase_prefix query body must be a map")
	}

	for field, value := range matchMap {
		var phraseStr string

		if strValue, ok := value.(string); ok {
			phraseStr = strValue
		} else if valueMap, ok := value.(map[string]interface{}); ok {
			if q, ok := valueMap["query"].(string); ok {
				phraseStr = q
			} else {
				return nil, fmt.Errorf("match_phrase_prefix query must have 'query' field")
			}
		} else {
			return nil, fmt.Errorf("invalid match_phrase_prefix query value type")
		}

		// bleve没有直接的match_phrase_prefix，使用prefix查询
		prefixQuery := query.NewPrefixQuery(phraseStr)
		prefixQuery.SetField(field)
		return prefixQuery, nil
	}

	return nil, fmt.Errorf("match_phrase_prefix query must have at least one field")
}

// parseTerm 解析term查询
func (p *QueryParser) parseTerm(body interface{}) (query.Query, error) {
	termMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("term query body must be a map")
	}

	for field, value := range termMap {
		var termValue string

		if strValue, ok := value.(string); ok {
			termValue = strValue
		} else if valueMap, ok := value.(map[string]interface{}); ok {
			if v, ok := valueMap["value"].(string); ok {
				termValue = v
			} else {
				return nil, fmt.Errorf("term query must have 'value' field")
			}
		} else {
			// 尝试转换为字符串
			termValue = fmt.Sprintf("%v", value)
		}

		termQuery := query.NewTermQuery(termValue)
		termQuery.SetField(field)
		return termQuery, nil
	}

	return nil, fmt.Errorf("term query must have at least one field")
}

// parseTerms 解析terms查询（多值term查询）
func (p *QueryParser) parseTerms(body interface{}) (query.Query, error) {
	termsMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("terms query body must be a map")
	}

	var queries []query.Query

	for field, value := range termsMap {
		var termValues []string

		if arr, ok := value.([]interface{}); ok {
			termValues = make([]string, 0, len(arr))
			for _, v := range arr {
				termValues = append(termValues, fmt.Sprintf("%v", v))
			}
		} else if valueMap, ok := value.(map[string]interface{}); ok {
			if arr, ok := valueMap["value"].([]interface{}); ok {
				termValues = make([]string, 0, len(arr))
				for _, v := range arr {
					termValues = append(termValues, fmt.Sprintf("%v", v))
				}
			}
		} else {
			return nil, fmt.Errorf("terms query value must be an array")
		}

		// 为每个term值创建查询，使用should组合（OR）
		for _, termValue := range termValues {
			termQuery := query.NewTermQuery(termValue)
			termQuery.SetField(field)
			queries = append(queries, termQuery)
		}
	}

	if len(queries) == 0 {
		return nil, fmt.Errorf("terms query must have at least one value")
	}

	// 使用DisjunctionQuery组合（OR逻辑）
	return query.NewDisjunctionQuery(queries), nil
}

// parseRange 解析range查询
func (p *QueryParser) parseRange(body interface{}) (query.Query, error) {
	rangeMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("range query body must be a map")
	}

	for field, value := range rangeMap {
		rangeSpec, ok := value.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("range query value must be a map")
		}

		var min, max *float64
		var minInclusive, maxInclusive *bool

		// 解析范围值
		if gte, ok := rangeSpec["gte"]; ok {
			if num, err := p.toFloat64(gte); err == nil {
				min = &num
				inc := true
				minInclusive = &inc
			}
		} else if gt, ok := rangeSpec["gt"]; ok {
			if num, err := p.toFloat64(gt); err == nil {
				min = &num
				inc := false
				minInclusive = &inc
			}
		}

		if lte, ok := rangeSpec["lte"]; ok {
			if num, err := p.toFloat64(lte); err == nil {
				max = &num
				inc := true
				maxInclusive = &inc
			}
		} else if lt, ok := rangeSpec["lt"]; ok {
			if num, err := p.toFloat64(lt); err == nil {
				max = &num
				inc := false
				maxInclusive = &inc
			}
		}

		// 创建数值范围查询
		if min != nil || max != nil {
			rangeQuery := query.NewNumericRangeInclusiveQuery(min, max, minInclusive, maxInclusive)
			rangeQuery.SetField(field)
			return rangeQuery, nil
		}

		// 如果没有数值范围，尝试字符串范围
		var minStr, maxStr string
		if gte, ok := rangeSpec["gte"].(string); ok {
			minStr = gte
		} else if gt, ok := rangeSpec["gt"].(string); ok {
			minStr = gt
		}
		if lte, ok := rangeSpec["lte"].(string); ok {
			maxStr = lte
		} else if lt, ok := rangeSpec["lt"].(string); ok {
			maxStr = lt
		}

		if minStr != "" || maxStr != "" {
			minInc := rangeSpec["gte"] != nil
			maxInc := rangeSpec["lte"] != nil
			termRangeQuery := query.NewTermRangeInclusiveQuery(minStr, maxStr, &minInc, &maxInc)
			termRangeQuery.SetField(field)
			return termRangeQuery, nil
		}

		return nil, fmt.Errorf("range query must have at least one range parameter (gte, gt, lte, lt)")
	}

	return nil, fmt.Errorf("range query must have at least one field")
}

// parseBool 解析bool查询
func (p *QueryParser) parseBool(body interface{}) (query.Query, error) {
	boolMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("bool query body must be a map")
	}

	var mustQueries, shouldQueries, mustNotQueries, filterQueries []query.Query

	// 解析must子句
	if must, ok := boolMap["must"].([]interface{}); ok {
		for _, q := range must {
			if qMap, ok := q.(map[string]interface{}); ok {
				parsed, err := p.ParseQuery(qMap)
				if err != nil {
					return nil, fmt.Errorf("failed to parse must query: %w", err)
				}
				mustQueries = append(mustQueries, parsed)
			}
		}
	} else if must, ok := boolMap["must"].(map[string]interface{}); ok {
		// 单个must查询
		parsed, err := p.ParseQuery(must)
		if err != nil {
			return nil, fmt.Errorf("failed to parse must query: %w", err)
		}
		mustQueries = append(mustQueries, parsed)
	}

	// 解析should子句
	if should, ok := boolMap["should"].([]interface{}); ok {
		for _, q := range should {
			if qMap, ok := q.(map[string]interface{}); ok {
				parsed, err := p.ParseQuery(qMap)
				if err != nil {
					return nil, fmt.Errorf("failed to parse should query: %w", err)
				}
				shouldQueries = append(shouldQueries, parsed)
			}
		}
	} else if should, ok := boolMap["should"].(map[string]interface{}); ok {
		parsed, err := p.ParseQuery(should)
		if err != nil {
			return nil, fmt.Errorf("failed to parse should query: %w", err)
		}
		shouldQueries = append(shouldQueries, parsed)
	}

	// 解析must_not子句
	if mustNot, ok := boolMap["must_not"].([]interface{}); ok {
		for _, q := range mustNot {
			if qMap, ok := q.(map[string]interface{}); ok {
				parsed, err := p.ParseQuery(qMap)
				if err != nil {
					return nil, fmt.Errorf("failed to parse must_not query: %w", err)
				}
				mustNotQueries = append(mustNotQueries, parsed)
			}
		}
	} else if mustNot, ok := boolMap["must_not"].(map[string]interface{}); ok {
		parsed, err := p.ParseQuery(mustNot)
		if err != nil {
			return nil, fmt.Errorf("failed to parse must_not query: %w", err)
		}
		mustNotQueries = append(mustNotQueries, parsed)
	}

	// 解析filter子句（与must类似，但不影响评分）
	if filter, ok := boolMap["filter"].([]interface{}); ok {
		for _, q := range filter {
			if qMap, ok := q.(map[string]interface{}); ok {
				parsed, err := p.ParseQuery(qMap)
				if err != nil {
					return nil, fmt.Errorf("failed to parse filter query: %w", err)
				}
				filterQueries = append(filterQueries, parsed)
			}
		}
	} else if filter, ok := boolMap["filter"].(map[string]interface{}); ok {
		parsed, err := p.ParseQuery(filter)
		if err != nil {
			return nil, fmt.Errorf("failed to parse filter query: %w", err)
		}
		filterQueries = append(filterQueries, parsed)
	}

	// 合并filter到must（filter不影响评分，但bleve中filter和must在BooleanQuery中处理方式相同）
	allMust := append(mustQueries, filterQueries...)

	// 创建BooleanQuery
	boolQuery := query.NewBooleanQuery(allMust, shouldQueries, mustNotQueries)

	// 设置minimum_should_match
	if minShould, ok := boolMap["minimum_should_match"].(int); ok {
		// bleve的BooleanQuery不支持minimum_should_match，这里记录日志
		log.Printf("WARN: minimum_should_match (%d) is not fully supported in bleve", minShould)
	}

	return boolQuery, nil
}

// parseWildcard 解析wildcard查询
func (p *QueryParser) parseWildcard(body interface{}) (query.Query, error) {
	wildcardMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("wildcard query body must be a map")
	}

	for field, value := range wildcardMap {
		var wildcardValue string

		if strValue, ok := value.(string); ok {
			wildcardValue = strValue
		} else if valueMap, ok := value.(map[string]interface{}); ok {
			if v, ok := valueMap["value"].(string); ok {
				wildcardValue = v
			} else {
				return nil, fmt.Errorf("wildcard query must have 'value' field")
			}
		} else {
			return nil, fmt.Errorf("invalid wildcard query value type")
		}

		wildcardQuery := query.NewWildcardQuery(wildcardValue)
		wildcardQuery.SetField(field)
		return wildcardQuery, nil
	}

	return nil, fmt.Errorf("wildcard query must have at least one field")
}

// parsePrefix 解析prefix查询
func (p *QueryParser) parsePrefix(body interface{}) (query.Query, error) {
	prefixMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("prefix query body must be a map")
	}

	for field, value := range prefixMap {
		var prefixValue string

		if strValue, ok := value.(string); ok {
			prefixValue = strValue
		} else if valueMap, ok := value.(map[string]interface{}); ok {
			if v, ok := valueMap["value"].(string); ok {
				prefixValue = v
			} else {
				return nil, fmt.Errorf("prefix query must have 'value' field")
			}
		} else {
			return nil, fmt.Errorf("invalid prefix query value type")
		}

		prefixQuery := query.NewPrefixQuery(prefixValue)
		prefixQuery.SetField(field)
		return prefixQuery, nil
	}

	return nil, fmt.Errorf("prefix query must have at least one field")
}

// parseFuzzy 解析fuzzy查询
func (p *QueryParser) parseFuzzy(body interface{}) (query.Query, error) {
	fuzzyMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("fuzzy query body must be a map")
	}

	for field, value := range fuzzyMap {
		var fuzzyValue string
		var fuzziness int = 2 // 默认fuzziness

		if strValue, ok := value.(string); ok {
			fuzzyValue = strValue
		} else if valueMap, ok := value.(map[string]interface{}); ok {
			if v, ok := valueMap["value"].(string); ok {
				fuzzyValue = v
			} else {
				return nil, fmt.Errorf("fuzzy query must have 'value' field")
			}
			if f, ok := valueMap["fuzziness"].(int); ok {
				fuzziness = f
			} else if f, ok := valueMap["fuzziness"].(float64); ok {
				fuzziness = int(f)
			}
		} else {
			return nil, fmt.Errorf("invalid fuzzy query value type")
		}

		fuzzyQuery := query.NewFuzzyQuery(fuzzyValue)
		fuzzyQuery.SetField(field)
		fuzzyQuery.SetFuzziness(fuzziness)
		return fuzzyQuery, nil
	}

	return nil, fmt.Errorf("fuzzy query must have at least one field")
}

// parseRegexp 解析regexp查询
func (p *QueryParser) parseRegexp(body interface{}) (query.Query, error) {
	regexpMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("regexp query body must be a map")
	}

	for field, value := range regexpMap {
		var regexpValue string

		if strValue, ok := value.(string); ok {
			regexpValue = strValue
		} else if valueMap, ok := value.(map[string]interface{}); ok {
			if v, ok := valueMap["value"].(string); ok {
				regexpValue = v
			} else {
				return nil, fmt.Errorf("regexp query must have 'value' field")
			}
		} else {
			return nil, fmt.Errorf("invalid regexp query value type")
		}

		regexpQuery := query.NewRegexpQuery(regexpValue)
		regexpQuery.SetField(field)
		return regexpQuery, nil
	}

	return nil, fmt.Errorf("regexp query must have at least one field")
}

// parseExists 解析exists查询
func (p *QueryParser) parseExists(body interface{}) (query.Query, error) {
	existsMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("exists query body must be a map")
	}

	if field, ok := existsMap["field"].(string); ok {
		// exists查询：字段存在且不为null
		// 使用wildcard查询匹配所有值（简单实现）
		// 更好的实现应该使用term查询匹配非空值
		wildcardQuery := query.NewWildcardQuery("*")
		wildcardQuery.SetField(field)
		return wildcardQuery, nil
	}

	return nil, fmt.Errorf("exists query must have 'field' parameter")
}

// parseNested 解析nested查询（嵌套文档查询）
func (p *QueryParser) parseNested(body interface{}) (query.Query, error) {
	nestedMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("nested query body must be a map")
	}

	path, ok := nestedMap["path"].(string)
	if !ok {
		return nil, fmt.Errorf("nested query must have 'path' parameter")
	}

	queryMap, ok := nestedMap["query"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("nested query must have 'query' parameter")
	}

	// 解析嵌套查询
	nestedQuery, err := p.ParseQuery(queryMap)
	if err != nil {
		return nil, fmt.Errorf("failed to parse nested query: %w", err)
	}

	// TODO: 实现真正的nested查询逻辑
	// 当前实现：在字段名前加上路径前缀
	// 例如：path="user.addresses", field="city" -> "user.addresses.city"
	log.Printf("WARN: Nested query for path [%s] - full nested query support requires additional implementation", path)

	return nestedQuery, nil
}

// parseMultiMatch 解析multi_match查询
func (p *QueryParser) parseMultiMatch(body interface{}) (query.Query, error) {
	multiMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("multi_match query body must be a map")
	}

	queryStr, ok := multiMap["query"].(string)
	if !ok {
		return nil, fmt.Errorf("multi_match query must have 'query' parameter")
	}

	fields, ok := multiMap["fields"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("multi_match query must have 'fields' parameter")
	}

	// 为每个字段创建match查询，使用should组合（OR）
	var queries []query.Query
	for _, field := range fields {
		if fieldStr, ok := field.(string); ok {
			matchQuery := query.NewMatchQuery(queryStr)
			matchQuery.SetField(fieldStr)
			queries = append(queries, matchQuery)
		}
	}

	if len(queries) == 0 {
		return nil, fmt.Errorf("multi_match query must have at least one field")
	}

	// 使用DisjunctionQuery组合
	return query.NewDisjunctionQuery(queries), nil
}

// parseQueryString 解析query_string查询
func (p *QueryParser) parseQueryString(body interface{}) (query.Query, error) {
	qsMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("query_string query body must be a map")
	}

	queryStr, ok := qsMap["query"].(string)
	if !ok {
		return nil, fmt.Errorf("query_string query must have 'query' parameter")
	}

	// 优化：如果查询字符串是 "*" 或空字符串，转换为 match_all 查询
	queryStr = strings.TrimSpace(queryStr)
	if queryStr == "" || queryStr == "*" {
		return query.NewMatchAllQuery(), nil
	}

	// 处理 fields 参数：如果指定了 fields，需要为每个字段创建查询
	if fieldsRaw, ok := qsMap["fields"].([]interface{}); ok && len(fieldsRaw) > 0 {
		// 提取字段列表
		fields := make([]string, 0, len(fieldsRaw))
		for _, f := range fieldsRaw {
			if fieldStr, ok := f.(string); ok {
				// 处理通配符字段 "*" 和 "*.*"
				if fieldStr == "*" || fieldStr == "*.*" {
					// 通配符字段：使用 match_all 查询
					return query.NewMatchAllQuery(), nil
				}
				fields = append(fields, fieldStr)
			}
		}

		// 如果有指定字段，为每个字段创建查询并使用 DisjunctionQuery 组合
		if len(fields) > 0 {
			var queries []query.Query
			for _, field := range fields {
				// 构建 field:query 格式的查询字符串
				fieldQueryStr := field + ":" + queryStr
				fieldQuery := query.NewQueryStringQuery(fieldQueryStr)
				queries = append(queries, fieldQuery)
			}
			if len(queries) > 0 {
				return query.NewDisjunctionQuery(queries), nil
			}
		}
	}

	// 处理 default_field 参数
	if defaultField, ok := qsMap["default_field"].(string); ok {
		// 如果指定了 default_field，构建 field:query 格式
		if defaultField == "*" || defaultField == "*.*" {
			// 通配符字段：使用 match_all 查询
			return query.NewMatchAllQuery(), nil
		}
		fieldQueryStr := defaultField + ":" + queryStr
		return query.NewQueryStringQuery(fieldQueryStr), nil
	}

	// 使用bleve的QueryStringQuery（默认在所有字段中搜索）
	qsQuery := query.NewQueryStringQuery(queryStr)

	return qsQuery, nil
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
