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
	"math"

	"github.com/lscgzwd/tiggerdb/logger"
	"github.com/lscgzwd/tiggerdb/search/query"
)

// ========== 存在性查询类型 ==========

// parseExists 解析exists查询
func (p *QueryParser) parseExists(body interface{}) (query.Query, error) {
	existsMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("exists query body must be a map")
	}

	if field, ok := existsMap["field"].(string); ok {
		field = p.normalizeFieldName(field)

		var queries []query.Query

		wildcardQuery := query.NewWildcardQuery("*")
		wildcardQuery.SetField(field)
		queries = append(queries, wildcardQuery)

		minFloat := -math.MaxFloat64
		maxFloat := math.MaxFloat64
		inclusive := true
		numQuery := query.NewNumericRangeInclusiveQuery(&minFloat, &maxFloat, &inclusive, &inclusive)
		numQuery.SetField(field)
		queries = append(queries, numQuery)

		disjQuery := query.NewDisjunctionQuery(queries)
		disjQuery.SetMin(1)
		if logger.IsDebugEnabled() {
			logger.Debug("parseExists [%s] - Created DisjunctionQuery with WildcardQuery and NumericRangeQuery (min=1)", field)
		}
		return disjQuery, nil
	}

	return nil, fmt.Errorf("exists query must have 'field' parameter")
}

// parseIds 解析ids查询
func (p *QueryParser) parseIds(body interface{}) (query.Query, error) {
	idsMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("ids query must be an object")
	}

	values, ok := idsMap["values"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("ids query must have 'values' array")
	}

	ids := make([]string, 0, len(values))
	for _, v := range values {
		if id, ok := v.(string); ok {
			ids = append(ids, id)
		}
	}

	if len(ids) == 0 {
		return query.NewMatchNoneQuery(), nil
	}

	docIDQuery := query.NewDocIDQuery(ids)

	if boost, ok := idsMap["boost"].(float64); ok {
		docIDQuery.SetBoost(boost)
	}

	return docIDQuery, nil
}
