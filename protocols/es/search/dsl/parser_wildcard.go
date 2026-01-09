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

// ========== 模式匹配查询类型 ==========

// parseWildcard 解析wildcard查询
func (p *QueryParser) parseWildcard(body interface{}) (query.Query, error) {
	wildcardMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("wildcard query body must be a map")
	}

	for field, value := range wildcardMap {
		field = p.normalizeFieldName(field)
		var wildcardValue string
		caseInsensitive := true // 默认启用大小写不敏感（ES 默认行为）

		if strValue, ok := value.(string); ok {
			wildcardValue = strValue
		} else if valueMap, ok := value.(map[string]interface{}); ok {
			if v, ok := valueMap["value"].(string); ok {
				wildcardValue = v
			} else {
				return nil, fmt.Errorf("wildcard query must have 'value' field")
			}
			// 如果明确指定了 case_insensitive=false，则禁用大小写不敏感
			if ci, ok := valueMap["case_insensitive"].(bool); ok {
				caseInsensitive = ci
			}
		} else {
			return nil, fmt.Errorf("invalid wildcard query value type")
		}

		// 默认启用大小写不敏感，将查询值转换为小写
		// 因为索引中的词通常是小写的（经过 lowercase filter）
		if caseInsensitive {
			wildcardValue = strings.ToLower(wildcardValue)
			if logger.IsDebugEnabled() {
				logger.Debug("parseWildcard [%s] - case_insensitive=true, converted value to lowercase: %s", field, wildcardValue)
			}
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
		field = p.normalizeFieldName(field)
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
		field = p.normalizeFieldName(field)
		var fuzzyValue string
		var fuzziness int = 2

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
		field = p.normalizeFieldName(field)
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
