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
	"strconv"

	"github.com/lscgzwd/tiggerdb/logger"
	"github.com/lscgzwd/tiggerdb/search/query"
)

// ========== 精确查询类型 ==========

// parseTerm 解析term查询
func (p *QueryParser) parseTerm(body interface{}) (query.Query, error) {
	termMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("term query body must be a map")
	}

	for field, value := range termMap {
		field = p.normalizeFieldName(field)
		var termValue interface{}

		if valueMap, ok := value.(map[string]interface{}); ok {
			if v, exists := valueMap["value"]; exists {
				termValue = v
			} else {
				termValue = value
			}
			if logger.IsDebugEnabled() {
				logger.Debug("parseTerm [%s] - Value is a map, extracted value: %v (type: %T)", field, termValue, termValue)
			}
		} else {
			termValue = value
			if logger.IsDebugEnabled() {
				logger.Debug("parseTerm [%s] - Value is direct: %v (type: %T)", field, termValue, termValue)
			}
		}

		switch v := termValue.(type) {
		case float64:
			var queries []query.Query
			inclusive := true
			numQuery := query.NewNumericRangeInclusiveQuery(&v, &v, &inclusive, &inclusive)
			numQuery.SetField(field)
			queries = append(queries, numQuery)

			termStr := strconv.FormatFloat(v, 'g', -1, 64)
			termQuery := query.NewTermQuery(termStr)
			termQuery.SetField(field)
			queries = append(queries, termQuery)

			disjQuery := query.NewDisjunctionQuery(queries)
			disjQuery.SetMin(1)
			return disjQuery, nil
		case int:
			var queries []query.Query
			floatVal := float64(v)
			inclusive := true
			numQuery := query.NewNumericRangeInclusiveQuery(&floatVal, &floatVal, &inclusive, &inclusive)
			numQuery.SetField(field)
			queries = append(queries, numQuery)

			termStr := strconv.Itoa(v)
			termQuery := query.NewTermQuery(termStr)
			termQuery.SetField(field)
			queries = append(queries, termQuery)

			disjQuery := query.NewDisjunctionQuery(queries)
			disjQuery.SetMin(1)
			return disjQuery, nil
		case int64:
			var queries []query.Query
			floatVal := float64(v)
			inclusive := true
			numQuery := query.NewNumericRangeInclusiveQuery(&floatVal, &floatVal, &inclusive, &inclusive)
			numQuery.SetField(field)
			queries = append(queries, numQuery)

			termStr := strconv.FormatInt(v, 10)
			termQuery := query.NewTermQuery(termStr)
			termQuery.SetField(field)
			queries = append(queries, termQuery)

			disjQuery := query.NewDisjunctionQuery(queries)
			disjQuery.SetMin(1)
			return disjQuery, nil
		case string:
			if numVal, err := strconv.ParseFloat(v, 64); err == nil {
				var queries []query.Query
				inclusive := true
				numQuery := query.NewNumericRangeInclusiveQuery(&numVal, &numVal, &inclusive, &inclusive)
				numQuery.SetField(field)
				queries = append(queries, numQuery)

				termQuery := query.NewTermQuery(v)
				termQuery.SetField(field)
				queries = append(queries, termQuery)

				disjQuery := query.NewDisjunctionQuery(queries)
				disjQuery.SetMin(1)
				return disjQuery, nil
			}

			termQuery := query.NewTermQuery(v)
			termQuery.SetField(field)
			return termQuery, nil
		case bool:
			boolStr := fmt.Sprintf("%v", v)
			termQuery := query.NewTermQuery(boolStr)
			termQuery.SetField(field)
			return termQuery, nil
		default:
			queryValue := fmt.Sprintf("%v", v)
			termQuery := query.NewTermQuery(queryValue)
			termQuery.SetField(field)
			return termQuery, nil
		}
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
		field = p.normalizeFieldName(field)
		var termValues []interface{}

		if arr, ok := value.([]interface{}); ok {
			termValues = arr
		} else if valueMap, ok := value.(map[string]interface{}); ok {
			if arr, ok := valueMap["value"].([]interface{}); ok {
				termValues = arr
			}
		} else {
			return nil, fmt.Errorf("terms query value must be an array")
		}

		if len(termValues) == 0 {
			continue
		}

		seenValues := make(map[string]bool)
		uniqueValues := make([]interface{}, 0, len(termValues))

		for _, termValue := range termValues {
			var key string
			switch v := termValue.(type) {
			case string:
				key = "s:" + v
			case float64:
				key = fmt.Sprintf("f:%g", v)
			case int:
				key = fmt.Sprintf("i:%d", v)
			case int64:
				key = fmt.Sprintf("i64:%d", v)
			default:
				key = fmt.Sprintf("o:%v", v)
			}

			if !seenValues[key] {
				seenValues[key] = true
				uniqueValues = append(uniqueValues, termValue)
			}
		}

		for _, termValue := range uniqueValues {
			var termQueries []query.Query

			switch v := termValue.(type) {
			case float64:
				inclusive := true
				numQuery := query.NewNumericRangeInclusiveQuery(&v, &v, &inclusive, &inclusive)
				numQuery.SetField(field)
				termQueries = append(termQueries, numQuery)

				termStr := strconv.FormatFloat(v, 'g', -1, 64)
				tq := query.NewTermQuery(termStr)
				tq.SetField(field)
				termQueries = append(termQueries, tq)
			case int:
				floatVal := float64(v)
				inclusive := true
				numQuery := query.NewNumericRangeInclusiveQuery(&floatVal, &floatVal, &inclusive, &inclusive)
				numQuery.SetField(field)
				termQueries = append(termQueries, numQuery)

				termStr := strconv.Itoa(v)
				tq := query.NewTermQuery(termStr)
				tq.SetField(field)
				termQueries = append(termQueries, tq)
			case int64:
				floatVal := float64(v)
				inclusive := true
				numQuery := query.NewNumericRangeInclusiveQuery(&floatVal, &floatVal, &inclusive, &inclusive)
				numQuery.SetField(field)
				termQueries = append(termQueries, numQuery)

				termStr := strconv.FormatInt(v, 10)
				tq := query.NewTermQuery(termStr)
				tq.SetField(field)
				termQueries = append(termQueries, tq)
			case string:
				tq := query.NewTermQuery(v)
				tq.SetField(field)
				termQueries = append(termQueries, tq)

				if numVal, err := strconv.ParseFloat(v, 64); err == nil {
					inclusive := true
					numQuery := query.NewNumericRangeInclusiveQuery(&numVal, &numVal, &inclusive, &inclusive)
					numQuery.SetField(field)
					termQueries = append(termQueries, numQuery)
				}
			default:
				queryValue := fmt.Sprintf("%v", v)
				tq := query.NewTermQuery(queryValue)
				tq.SetField(field)
				termQueries = append(termQueries, tq)
			}

			if len(termQueries) == 1 {
				queries = append(queries, termQueries[0])
			} else if len(termQueries) > 1 {
				disjQuery := query.NewDisjunctionQuery(termQueries)
				disjQuery.SetMin(1)
				queries = append(queries, disjQuery)
			}
		}
	}

	if len(queries) == 0 {
		return nil, fmt.Errorf("terms query must have at least one value")
	}

	disjQuery := query.NewDisjunctionQuery(queries)
	disjQuery.SetMin(1)
	return disjQuery, nil
}

// parseRange 解析range查询
func (p *QueryParser) parseRange(body interface{}) (query.Query, error) {
	rangeMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("range query body must be a map")
	}

	for field, value := range rangeMap {
		field = p.normalizeFieldName(field)
		rangeSpec, ok := value.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("range query value must be a map")
		}

		var min, max *float64
		var minInclusive, maxInclusive *bool

		hasStandardFormat := false
		if gte, ok := rangeSpec["gte"]; ok && gte != nil {
			if num, err := p.toFloat64(gte); err == nil {
				min = &num
				inc := true
				minInclusive = &inc
				hasStandardFormat = true
			}
		} else if gt, ok := rangeSpec["gt"]; ok && gt != nil {
			if num, err := p.toFloat64(gt); err == nil {
				min = &num
				inc := false
				minInclusive = &inc
				hasStandardFormat = true
			}
		}

		if lte, ok := rangeSpec["lte"]; ok && lte != nil {
			if num, err := p.toFloat64(lte); err == nil {
				max = &num
				inc := true
				maxInclusive = &inc
				hasStandardFormat = true
			}
		} else if lt, ok := rangeSpec["lt"]; ok && lt != nil {
			if num, err := p.toFloat64(lt); err == nil {
				max = &num
				inc := false
				maxInclusive = &inc
				hasStandardFormat = true
			}
		}

		if !hasStandardFormat {
			if from, ok := rangeSpec["from"]; ok && from != nil {
				if num, err := p.toFloat64(from); err == nil {
					min = &num
					includeLower := true
					if il, ok := rangeSpec["include_lower"].(bool); ok {
						includeLower = il
					}
					minInclusive = &includeLower
				}
			}

			if to, ok := rangeSpec["to"]; ok && to != nil {
				if num, err := p.toFloat64(to); err == nil {
					max = &num
					includeUpper := true
					if iu, ok := rangeSpec["include_upper"].(bool); ok {
						includeUpper = iu
					}
					maxInclusive = &includeUpper
				}
			}
		}

		if min != nil || max != nil {
			rangeQuery := query.NewNumericRangeInclusiveQuery(min, max, minInclusive, maxInclusive)
			rangeQuery.SetField(field)
			return rangeQuery, nil
		}

		var minStr, maxStr string
		var minStrInc, maxStrInc *bool

		hasStrStandardFormat := false
		if gte, ok := rangeSpec["gte"]; ok && gte != nil {
			if gteStr, ok := gte.(string); ok {
				minStr = gteStr
				inc := true
				minStrInc = &inc
				hasStrStandardFormat = true
			}
		} else if gt, ok := rangeSpec["gt"]; ok && gt != nil {
			if gtStr, ok := gt.(string); ok {
				minStr = gtStr
				inc := false
				minStrInc = &inc
				hasStrStandardFormat = true
			}
		}

		if lte, ok := rangeSpec["lte"]; ok && lte != nil {
			if lteStr, ok := lte.(string); ok {
				maxStr = lteStr
				inc := true
				maxStrInc = &inc
				hasStrStandardFormat = true
			}
		} else if lt, ok := rangeSpec["lt"]; ok && lt != nil {
			if ltStr, ok := lt.(string); ok {
				maxStr = ltStr
				inc := false
				maxStrInc = &inc
				hasStrStandardFormat = true
			}
		}

		if !hasStrStandardFormat {
			if from, ok := rangeSpec["from"]; ok && from != nil {
				if fromStr, ok := from.(string); ok {
					minStr = fromStr
					includeLower := true
					if il, ok := rangeSpec["include_lower"].(bool); ok {
						includeLower = il
					}
					minStrInc = &includeLower
				}
			}

			if to, ok := rangeSpec["to"]; ok && to != nil {
				if toStr, ok := to.(string); ok {
					maxStr = toStr
					includeUpper := true
					if iu, ok := rangeSpec["include_upper"].(bool); ok {
						includeUpper = iu
					}
					maxStrInc = &includeUpper
				}
			}
		}

		if minStr != "" || maxStr != "" {
			termRangeQuery := query.NewTermRangeInclusiveQuery(minStr, maxStr, minStrInc, maxStrInc)
			termRangeQuery.SetField(field)
			return termRangeQuery, nil
		}

		return nil, fmt.Errorf("range query must have at least one range parameter")
	}

	return nil, fmt.Errorf("range query must have at least one field")
}
