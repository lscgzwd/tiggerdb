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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/lscgzwd/tiggerdb/logger"
	"github.com/lscgzwd/tiggerdb/search/query"
)

// ========== 复合查询类型 ==========

// parseBool 解析bool查询
func (p *QueryParser) parseBool(body interface{}) (query.Query, error) {
	boolMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("bool query body must be a map")
	}

	if logger.IsDebugEnabled() {
		boolJSON, _ := json.Marshal(boolMap)
		logger.Debug("parseBool - Input bool query: %s", string(boolJSON))
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

	// 解析filter子句
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

	allMust := append(mustQueries, filterQueries...)

	if logger.IsDebugEnabled() {
		logger.Debug("parseBool - Parsed clauses: must=%d, should=%d, must_not=%d, filter=%d, allMust=%d",
			len(mustQueries), len(shouldQueries), len(mustNotQueries), len(filterQueries), len(allMust))
	}

	boolQuery := query.NewBooleanQuery(allMust, shouldQueries, mustNotQueries)

	var minShould float64
	if minShouldRaw, ok := boolMap["minimum_should_match"]; ok && len(shouldQueries) > 0 {
		switch v := minShouldRaw.(type) {
		case int:
			minShould = float64(v)
		case int64:
			minShould = float64(v)
		case float64:
			minShould = v
		case string:
			if strings.HasSuffix(v, "%") {
				percentStr := strings.TrimSuffix(v, "%")
				if percent, err := strconv.ParseFloat(percentStr, 64); err == nil {
					minShould = float64(len(shouldQueries)) * percent / 100.0
				} else {
					minShould = 1.0
				}
			} else {
				if parsed, err := strconv.ParseFloat(v, 64); err == nil {
					minShould = parsed
				} else {
					minShould = 1.0
				}
			}
		default:
			minShould = 1.0
		}

		if minShould > float64(len(shouldQueries)) {
			minShould = float64(len(shouldQueries))
		}

		if minShould > 0 {
			boolQuery.SetMinShould(minShould)
		}
	} else if len(shouldQueries) > 0 && len(allMust) == 0 {
		minShould = 1.0
		boolQuery.SetMinShould(minShould)
	}

	return boolQuery, nil
}

// parseNested 解析nested查询
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

	nestedQuery, err := p.ParseQuery(queryMap)
	if err != nil {
		return nil, fmt.Errorf("failed to parse nested query: %w", err)
	}

	p.addPathPrefixToQuery(nestedQuery, path)

	if scoreMode, ok := nestedMap["score_mode"].(string); ok {
		validModes := map[string]bool{"avg": true, "sum": true, "max": true, "min": true, "none": true}
		if !validModes[scoreMode] {
			logger.Warn("Invalid score_mode '%s' in nested query, using default 'avg'", scoreMode)
		}
	}

	return nestedQuery, nil
}

// parseConstantScore 解析constant_score查询
func (p *QueryParser) parseConstantScore(body interface{}) (query.Query, error) {
	constantMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("constant_score query must be an object")
	}

	filterQuery, ok := constantMap["filter"]
	if !ok {
		return nil, fmt.Errorf("constant_score query must have 'filter' field")
	}

	filterMap, ok := filterQuery.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("constant_score filter must be an object")
	}

	innerQuery, err := p.ParseQuery(filterMap)
	if err != nil {
		return nil, fmt.Errorf("failed to parse constant_score filter: %w", err)
	}

	boost := 1.0
	if boostVal, ok := constantMap["boost"].(float64); ok {
		boost = boostVal
	}
	if boostable, ok := innerQuery.(query.BoostableQuery); ok {
		boostable.SetBoost(boost)
	}

	return innerQuery, nil
}

// parseDisMax 解析dis_max查询
func (p *QueryParser) parseDisMax(body interface{}) (query.Query, error) {
	disMaxMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("dis_max query must be an object")
	}

	queries, ok := disMaxMap["queries"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("dis_max query must have 'queries' array")
	}

	disjuncts := make([]query.Query, 0, len(queries))
	for _, q := range queries {
		qMap, ok := q.(map[string]interface{})
		if !ok {
			continue
		}
		parsedQuery, err := p.ParseQuery(qMap)
		if err != nil {
			return nil, fmt.Errorf("failed to parse dis_max query: %w", err)
		}
		disjuncts = append(disjuncts, parsedQuery)
	}

	if len(disjuncts) == 0 {
		return query.NewMatchNoneQuery(), nil
	}

	disjQuery := query.NewDisjunctionQuery(disjuncts)
	disjQuery.SetMin(1)

	if boost, ok := disMaxMap["boost"].(float64); ok {
		disjQuery.SetBoost(boost)
	}

	return disjQuery, nil
}
