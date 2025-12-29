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
	"testing"

	"github.com/lscgzwd/tiggerdb/search/query"
)

// TestParseScriptQuery 测试 script 查询解析
func TestParseScriptQuery(t *testing.T) {
	parser := NewQueryParser()

	tests := []struct {
		name    string
		query   map[string]interface{}
		wantErr bool
	}{
		{
			name: "script query with source",
			query: map[string]interface{}{
				"script": map[string]interface{}{
					"script": map[string]interface{}{
						"source": "doc['price'].value > 100",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "script query with params",
			query: map[string]interface{}{
				"script": map[string]interface{}{
					"script": map[string]interface{}{
						"source": "doc['price'].value > params.threshold",
						"params": map[string]interface{}{
							"threshold": 100,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "script query inline format",
			query: map[string]interface{}{
				"script": map[string]interface{}{
					"script": "doc['price'].value > 100",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && q == nil {
				t.Error("ParseQuery() returned nil query without error")
			}
			// 验证返回的是 ScriptQuery 类型
			if !tt.wantErr {
				if _, ok := q.(*query.ScriptQuery); !ok {
					t.Errorf("ParseQuery() returned %T, want *query.ScriptQuery", q)
				}
			}
		})
	}
}

// TestParseScriptScoreQuery 测试 script_score 查询解析
func TestParseScriptScoreQuery(t *testing.T) {
	parser := NewQueryParser()

	tests := []struct {
		name    string
		query   map[string]interface{}
		wantErr bool
	}{
		{
			name: "script_score with inner query",
			query: map[string]interface{}{
				"script_score": map[string]interface{}{
					"query": map[string]interface{}{
						"match_all": map[string]interface{}{},
					},
					"script": map[string]interface{}{
						"source": "_score * doc['popularity'].value",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "script_score with params",
			query: map[string]interface{}{
				"script_score": map[string]interface{}{
					"query": map[string]interface{}{
						"match": map[string]interface{}{
							"title": "test",
						},
					},
					"script": map[string]interface{}{
						"source": "_score * params.boost",
						"params": map[string]interface{}{
							"boost": 2.0,
						},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && q == nil {
				t.Error("ParseQuery() returned nil query without error")
			}
		})
	}
}

// TestParseBoolWithScript 测试 bool 查询中嵌套 script 查询
func TestParseBoolWithScript(t *testing.T) {
	parser := NewQueryParser()

	query := map[string]interface{}{
		"bool": map[string]interface{}{
			"must": []interface{}{
				map[string]interface{}{
					"match": map[string]interface{}{
						"title": "elasticsearch",
					},
				},
			},
			"filter": []interface{}{
				map[string]interface{}{
					"script": map[string]interface{}{
						"script": map[string]interface{}{
							"source": "doc['price'].value > params.min && doc['price'].value < params.max",
							"params": map[string]interface{}{
								"min": 10,
								"max": 100,
							},
						},
					},
				},
			},
		},
	}

	q, err := parser.ParseQuery(query)
	if err != nil {
		t.Errorf("ParseQuery() error = %v", err)
		return
	}
	if q == nil {
		t.Error("ParseQuery() returned nil query")
	}
}

// TestParseComplexScriptExpressions 测试复杂脚本表达式
func TestParseComplexScriptExpressions(t *testing.T) {
	parser := NewQueryParser()

	tests := []struct {
		name   string
		script string
	}{
		{
			name:   "arithmetic expression",
			script: "doc['price'].value * 0.9 + 10",
		},
		{
			name:   "comparison expression",
			script: "doc['price'].value >= 100 && doc['quantity'].value > 0",
		},
		{
			name:   "ternary expression",
			script: "doc['status'].value == 'active' ? 1 : 0",
		},
		{
			name:   "math function",
			script: "Math.sqrt(doc['area'].value)",
		},
		{
			name:   "string method",
			script: "doc['name'].value.toLowerCase().contains('test')",
		},
		{
			name:   "nested expression",
			script: "(doc['price'].value - params.base) / params.scale",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := map[string]interface{}{
				"script": map[string]interface{}{
					"script": map[string]interface{}{
						"source": tt.script,
						"params": map[string]interface{}{
							"base":  100,
							"scale": 10,
						},
					},
				},
			}

			q, err := parser.ParseQuery(query)
			if err != nil {
				t.Errorf("ParseQuery() error = %v for script: %s", err, tt.script)
				return
			}
			if q == nil {
				t.Errorf("ParseQuery() returned nil for script: %s", tt.script)
			}
		})
	}
}

// TestESScriptQueryFormats 测试 ES 兼容的各种 script 查询格式
func TestESScriptQueryFormats(t *testing.T) {
	parser := NewQueryParser()

	tests := []struct {
		name    string
		query   map[string]interface{}
		wantErr bool
	}{
		{
			name: "ES 7.x format - script in script",
			query: map[string]interface{}{
				"script": map[string]interface{}{
					"script": map[string]interface{}{
						"source": "doc['price'].value > 100",
						"lang":   "painless",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "ES format - direct source",
			query: map[string]interface{}{
				"script": map[string]interface{}{
					"source": "doc['price'].value > 100",
				},
			},
			wantErr: false,
		},
		{
			name: "ES format - inline (deprecated but supported)",
			query: map[string]interface{}{
				"script": map[string]interface{}{
					"inline": "doc['price'].value > 100",
				},
			},
			wantErr: false,
		},
		{
			name: "ES format - string shorthand",
			query: map[string]interface{}{
				"script": map[string]interface{}{
					"script": "doc['price'].value > 100",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && q == nil {
				t.Error("ParseQuery() returned nil query without error")
			}
		})
	}
}

// TestParseFunctionScoreQuery 测试 function_score 查询解析
func TestParseFunctionScoreQuery(t *testing.T) {
	parser := NewQueryParser()

	tests := []struct {
		name    string
		query   map[string]interface{}
		wantErr bool
	}{
		{
			name: "function_score with script_score",
			query: map[string]interface{}{
				"function_score": map[string]interface{}{
					"query": map[string]interface{}{
						"match_all": map[string]interface{}{},
					},
					"script_score": map[string]interface{}{
						"script": map[string]interface{}{
							"source": "_score * doc['popularity'].value",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "function_score with field_value_factor",
			query: map[string]interface{}{
				"function_score": map[string]interface{}{
					"query": map[string]interface{}{
						"match": map[string]interface{}{
							"title": "test",
						},
					},
					"field_value_factor": map[string]interface{}{
						"field":    "popularity",
						"factor":   1.2,
						"modifier": "sqrt",
						"missing":  1,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "function_score with functions array",
			query: map[string]interface{}{
				"function_score": map[string]interface{}{
					"query": map[string]interface{}{
						"match_all": map[string]interface{}{},
					},
					"functions": []interface{}{
						map[string]interface{}{
							"filter": map[string]interface{}{
								"term": map[string]interface{}{
									"category": "premium",
								},
							},
							"weight": 2.0,
						},
						map[string]interface{}{
							"field_value_factor": map[string]interface{}{
								"field":    "likes",
								"modifier": "log1p",
							},
						},
					},
					"score_mode": "sum",
					"boost_mode": "multiply",
				},
			},
			wantErr: false,
		},
		{
			name: "function_score with decay function",
			query: map[string]interface{}{
				"function_score": map[string]interface{}{
					"query": map[string]interface{}{
						"match_all": map[string]interface{}{},
					},
					"functions": []interface{}{
						map[string]interface{}{
							"gauss": map[string]interface{}{
								"price": map[string]interface{}{
									"origin": 100.0,
									"scale":  50.0,
									"offset": 10.0,
									"decay":  0.5,
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "function_score with random_score",
			query: map[string]interface{}{
				"function_score": map[string]interface{}{
					"query": map[string]interface{}{
						"match_all": map[string]interface{}{},
					},
					"random_score": map[string]interface{}{
						"seed":  12345.0,
						"field": "_seq_no",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "function_score with min_score and max_boost",
			query: map[string]interface{}{
				"function_score": map[string]interface{}{
					"query": map[string]interface{}{
						"match_all": map[string]interface{}{},
					},
					"script_score": map[string]interface{}{
						"script": map[string]interface{}{
							"source": "_score * 2",
						},
					},
					"min_score":  0.5,
					"max_boost":  10.0,
					"boost_mode": "replace",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && q == nil {
				t.Error("ParseQuery() returned nil query without error")
			}
			// 验证返回的是 FunctionScoreQuery 类型
			if !tt.wantErr {
				if _, ok := q.(*query.FunctionScoreQuery); !ok {
					t.Errorf("ParseQuery() returned %T, want *query.FunctionScoreQuery", q)
				}
			}
		})
	}
}

// TestScriptScoreQueryWithMinScore 测试带 min_score 的 script_score 查询
func TestScriptScoreQueryWithMinScore(t *testing.T) {
	parser := NewQueryParser()

	query := map[string]interface{}{
		"script_score": map[string]interface{}{
			"query": map[string]interface{}{
				"match_all": map[string]interface{}{},
			},
			"script": map[string]interface{}{
				"source": "_score * doc['boost'].value",
				"params": map[string]interface{}{
					"factor": 2.0,
				},
			},
			"min_score": 1.0,
			"boost":     1.5,
		},
	}

	q, err := parser.ParseQuery(query)
	if err != nil {
		t.Errorf("ParseQuery() error = %v", err)
		return
	}
	if q == nil {
		t.Error("ParseQuery() returned nil query")
	}
}
