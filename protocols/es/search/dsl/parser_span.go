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

	"github.com/lscgzwd/tiggerdb/search/query"
)

// ========== Span查询类型 ==========

// parseSpanTerm 解析span_term查询
func (p *QueryParser) parseSpanTerm(body interface{}) (query.Query, error) {
	return p.parseTerm(body)
}

// parseSpanNear 解析span_near查询
func (p *QueryParser) parseSpanNear(body interface{}) (query.Query, error) {
	spanMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("span_near query must be an object")
	}

	clauses, ok := spanMap["clauses"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("span_near query must have 'clauses' array")
	}

	conjuncts := make([]query.Query, 0, len(clauses))
	for _, clause := range clauses {
		clauseMap, ok := clause.(map[string]interface{})
		if !ok {
			continue
		}
		parsedQuery, err := p.ParseQuery(clauseMap)
		if err != nil {
			return nil, fmt.Errorf("failed to parse span_near clause: %w", err)
		}
		conjuncts = append(conjuncts, parsedQuery)
	}

	if len(conjuncts) == 0 {
		return query.NewMatchNoneQuery(), nil
	}

	conjQuery := query.NewConjunctionQuery(conjuncts)

	if boost, ok := spanMap["boost"].(float64); ok {
		conjQuery.SetBoost(boost)
	}

	return conjQuery, nil
}

// parseSpanOr 解析span_or查询
func (p *QueryParser) parseSpanOr(body interface{}) (query.Query, error) {
	spanMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("span_or query must be an object")
	}

	clauses, ok := spanMap["clauses"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("span_or query must have 'clauses' array")
	}

	disjuncts := make([]query.Query, 0, len(clauses))
	for _, clause := range clauses {
		clauseMap, ok := clause.(map[string]interface{})
		if !ok {
			continue
		}
		parsedQuery, err := p.ParseQuery(clauseMap)
		if err != nil {
			return nil, fmt.Errorf("failed to parse span_or clause: %w", err)
		}
		disjuncts = append(disjuncts, parsedQuery)
	}

	if len(disjuncts) == 0 {
		return query.NewMatchNoneQuery(), nil
	}

	disjQuery := query.NewDisjunctionQuery(disjuncts)
	disjQuery.SetMin(1)

	if boost, ok := spanMap["boost"].(float64); ok {
		disjQuery.SetBoost(boost)
	}

	return disjQuery, nil
}

// parseSpanNot 解析span_not查询
func (p *QueryParser) parseSpanNot(body interface{}) (query.Query, error) {
	spanMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("span_not query must be an object")
	}

	var includeQuery query.Query
	if includeVal, ok := spanMap["include"].(map[string]interface{}); ok {
		var err error
		includeQuery, err = p.ParseQuery(includeVal)
		if err != nil {
			return nil, fmt.Errorf("failed to parse span_not include: %w", err)
		}
	} else {
		includeQuery = query.NewMatchAllQuery()
	}

	var excludeQuery query.Query
	if excludeVal, ok := spanMap["exclude"].(map[string]interface{}); ok {
		var err error
		excludeQuery, err = p.ParseQuery(excludeVal)
		if err != nil {
			return nil, fmt.Errorf("failed to parse span_not exclude: %w", err)
		}
	}

	mustQueries := []query.Query{includeQuery}
	var mustNotQueries []query.Query
	if excludeQuery != nil {
		mustNotQueries = []query.Query{excludeQuery}
	}
	boolQuery := query.NewBooleanQuery(mustQueries, nil, mustNotQueries)

	if boost, ok := spanMap["boost"].(float64); ok {
		boolQuery.SetBoost(boost)
	}

	return boolQuery, nil
}

// parseSpanFirst 解析span_first查询
func (p *QueryParser) parseSpanFirst(body interface{}) (query.Query, error) {
	spanMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("span_first query must be an object")
	}

	if matchVal, ok := spanMap["match"].(map[string]interface{}); ok {
		return p.ParseQuery(matchVal)
	}

	return query.NewMatchAllQuery(), nil
}

// parseSpanContaining 解析span_containing查询
func (p *QueryParser) parseSpanContaining(body interface{}) (query.Query, error) {
	spanMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("span_containing query must be an object")
	}

	var bigQuery, littleQuery query.Query
	if bigVal, ok := spanMap["big"].(map[string]interface{}); ok {
		var err error
		bigQuery, err = p.ParseQuery(bigVal)
		if err != nil {
			return nil, fmt.Errorf("failed to parse span_containing big: %w", err)
		}
	} else {
		bigQuery = query.NewMatchAllQuery()
	}

	if littleVal, ok := spanMap["little"].(map[string]interface{}); ok {
		var err error
		littleQuery, err = p.ParseQuery(littleVal)
		if err != nil {
			return nil, fmt.Errorf("failed to parse span_containing little: %w", err)
		}
	} else {
		littleQuery = query.NewMatchAllQuery()
	}

	conjQuery := query.NewConjunctionQuery([]query.Query{bigQuery, littleQuery})

	if boost, ok := spanMap["boost"].(float64); ok {
		conjQuery.SetBoost(boost)
	}

	return conjQuery, nil
}

// parseSpanWithin 解析span_within查询
func (p *QueryParser) parseSpanWithin(body interface{}) (query.Query, error) {
	return p.parseSpanContaining(body)
}

// parseSpanMulti 解析span_multi查询
func (p *QueryParser) parseSpanMulti(body interface{}) (query.Query, error) {
	spanMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("span_multi query must be an object")
	}

	if matchVal, ok := spanMap["match"].(map[string]interface{}); ok {
		return p.ParseQuery(matchVal)
	}

	return query.NewMatchAllQuery(), nil
}
