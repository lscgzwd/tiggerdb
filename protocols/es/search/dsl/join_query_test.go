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
	"os"
	"testing"

	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/search/query"
)

// TestParseWrapper 测试 wrapper 查询解析
func TestParseWrapper(t *testing.T) {
	parser := NewQueryParser()

	tests := []struct {
		name        string
		innerQuery  map[string]interface{}
		expectError bool
	}{
		{
			name: "simple match query",
			innerQuery: map[string]interface{}{
				"match": map[string]interface{}{
					"title": "test",
				},
			},
			expectError: false,
		},
		{
			name: "term query",
			innerQuery: map[string]interface{}{
				"term": map[string]interface{}{
					"status": "active",
				},
			},
			expectError: false,
		},
		{
			name: "bool query",
			innerQuery: map[string]interface{}{
				"bool": map[string]interface{}{
					"must": []interface{}{
						map[string]interface{}{
							"match": map[string]interface{}{
								"title": "test",
							},
						},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 序列化内部查询为 JSON
			innerJSON, err := json.Marshal(tt.innerQuery)
			if err != nil {
				t.Fatalf("Failed to marshal inner query: %v", err)
			}

			// Base64 编码
			encoded := base64.StdEncoding.EncodeToString(innerJSON)

			// 创建 wrapper 查询
			wrapperQuery := map[string]interface{}{
				"wrapper": map[string]interface{}{
					"query": encoded,
				},
			}

			// 解析查询
			result, err := parser.ParseQuery(wrapperQuery)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if result == nil {
				t.Fatal("Expected non-nil result")
			}

			t.Logf("Parsed query type: %T", result)
		})
	}
}

// TestParseHasChild 测试 has_child 查询解析
func TestParseHasChild(t *testing.T) {
	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectError bool
	}{
		{
			name: "valid has_child query",
			query: map[string]interface{}{
				"has_child": map[string]interface{}{
					"type": "answer",
					"query": map[string]interface{}{
						"match": map[string]interface{}{
							"body": "elasticsearch",
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "has_child without type",
			query: map[string]interface{}{
				"has_child": map[string]interface{}{
					"query": map[string]interface{}{
						"match_all": map[string]interface{}{},
					},
				},
			},
			expectError: true,
		},
		{
			name: "has_child without query",
			query: map[string]interface{}{
				"has_child": map[string]interface{}{
					"type": "answer",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.ParseQuery(tt.query)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if result == nil {
				t.Fatal("Expected non-nil result")
			}

			// 验证查询已注册
			info := GetJoinQueryInfo(result)
			if info == nil {
				t.Fatal("Expected join query info to be registered")
			}

			if info.Type != JoinQueryTypeHasChild {
				t.Errorf("Expected JoinQueryTypeHasChild, got %v", info.Type)
			}

			if info.TypeName != "answer" {
				t.Errorf("Expected type name 'answer', got '%s'", info.TypeName)
			}

			// 清理
			UnregisterJoinQuery(result)
		})
	}
}

// TestParseHasParent 测试 has_parent 查询解析
func TestParseHasParent(t *testing.T) {
	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectError bool
	}{
		{
			name: "valid has_parent query",
			query: map[string]interface{}{
				"has_parent": map[string]interface{}{
					"parent_type": "question",
					"query": map[string]interface{}{
						"match": map[string]interface{}{
							"title": "elasticsearch",
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "has_parent without parent_type",
			query: map[string]interface{}{
				"has_parent": map[string]interface{}{
					"query": map[string]interface{}{
						"match_all": map[string]interface{}{},
					},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.ParseQuery(tt.query)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if result == nil {
				t.Fatal("Expected non-nil result")
			}

			// 验证查询已注册
			info := GetJoinQueryInfo(result)
			if info == nil {
				t.Fatal("Expected join query info to be registered")
			}

			if info.Type != JoinQueryTypeHasParent {
				t.Errorf("Expected JoinQueryTypeHasParent, got %v", info.Type)
			}

			// 清理
			UnregisterJoinQuery(result)
		})
	}
}

// TestParsePercolate 测试 percolate 查询解析
func TestParsePercolate(t *testing.T) {
	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectError bool
	}{
		{
			name: "percolate with document",
			query: map[string]interface{}{
				"percolate": map[string]interface{}{
					"field": "query",
					"document": map[string]interface{}{
						"title":   "Test Document",
						"content": "This is a test document for percolate query",
					},
				},
			},
			expectError: false,
		},
		{
			name: "percolate with multiple documents",
			query: map[string]interface{}{
				"percolate": map[string]interface{}{
					"field": "query",
					"documents": []interface{}{
						map[string]interface{}{
							"title": "Doc 1",
						},
						map[string]interface{}{
							"title": "Doc 2",
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "percolate without document",
			query: map[string]interface{}{
				"percolate": map[string]interface{}{
					"field": "query",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.ParseQuery(tt.query)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if result == nil {
				t.Fatal("Expected non-nil result")
			}

			// 验证查询已注册
			info := GetPercolateQueryInfo(result)
			if info == nil {
				t.Fatal("Expected percolate query info to be registered")
			}

			// 清理
			UnregisterPercolateQuery(result)
		})
	}
}

// TestMatchFieldValue 测试字段值匹配
func TestMatchFieldValue(t *testing.T) {
	tests := []struct {
		name       string
		fieldValue interface{}
		queryValue string
		expected   bool
	}{
		{
			name:       "string exact match",
			fieldValue: "hello world",
			queryValue: "hello",
			expected:   true,
		},
		{
			name:       "string case insensitive",
			fieldValue: "Hello World",
			queryValue: "hello",
			expected:   true,
		},
		{
			name:       "string no match",
			fieldValue: "hello world",
			queryValue: "foo",
			expected:   false,
		},
		{
			name:       "number match",
			fieldValue: float64(42),
			queryValue: "42",
			expected:   true,
		},
		{
			name:       "boolean true",
			fieldValue: true,
			queryValue: "true",
			expected:   true,
		},
		{
			name:       "array with match",
			fieldValue: []interface{}{"apple", "banana", "cherry"},
			queryValue: "banana",
			expected:   true,
		},
		{
			name:       "array no match",
			fieldValue: []interface{}{"apple", "banana", "cherry"},
			queryValue: "orange",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchFieldValue(tt.fieldValue, tt.queryValue)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestMatchDocumentAgainstQuery 测试文档与查询的匹配
func TestMatchDocumentAgainstQuery(t *testing.T) {
	doc := map[string]interface{}{
		"title":   "Elasticsearch Guide",
		"content": "This is a comprehensive guide to Elasticsearch",
		"status":  "published",
		"views":   float64(100),
	}

	tests := []struct {
		name     string
		query    query.Query
		expected bool
	}{
		{
			name:     "match_all query",
			query:    query.NewMatchAllQuery(),
			expected: true,
		},
		{
			name:     "match_none query",
			query:    query.NewMatchNoneQuery(),
			expected: false,
		},
		{
			name: "term query match",
			query: func() query.Query {
				q := query.NewTermQuery("published")
				q.SetField("status")
				return q
			}(),
			expected: true,
		},
		{
			name: "term query no match",
			query: func() query.Query {
				q := query.NewTermQuery("draft")
				q.SetField("status")
				return q
			}(),
			expected: false,
		},
		{
			name: "match query",
			query: func() query.Query {
				q := query.NewMatchQuery("elasticsearch")
				q.SetField("title")
				return q
			}(),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchDocumentAgainstQuery(nil, doc, tt.query)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestHasChildQueryExecution 测试 has_child 查询的完整执行
func TestHasChildQueryExecution(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_test_has_child_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建索引
	idx, err := bleve.New(tempDir, bleve.NewIndexMapping())
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	defer idx.Close()

	// 索引父文档
	parentDoc := map[string]interface{}{
		"_join_name": "question",
		"title":      "What is Elasticsearch?",
	}
	if err := idx.Index("q1", parentDoc); err != nil {
		t.Fatalf("Failed to index parent document: %v", err)
	}

	// 索引子文档
	childDocs := []struct {
		id  string
		doc map[string]interface{}
	}{
		{
			id: "a1",
			doc: map[string]interface{}{
				"_join_name":   "answer",
				"_join_parent": "q1",
				"body":         "Elasticsearch is a search engine",
			},
		},
		{
			id: "a2",
			doc: map[string]interface{}{
				"_join_name":   "answer",
				"_join_parent": "q1",
				"body":         "It is built on Lucene",
			},
		},
	}

	for _, cd := range childDocs {
		if err := idx.Index(cd.id, cd.doc); err != nil {
			t.Fatalf("Failed to index child document %s: %v", cd.id, err)
		}
	}

	// 创建内部查询
	innerQuery := query.NewMatchQuery("search engine")
	innerQuery.SetField("body")

	// 执行 has_child 查询
	resultQuery, err := ExecuteHasChildQuery(nil, idx, "answer", innerQuery, 1.0)
	if err != nil {
		t.Fatalf("Failed to execute has_child query: %v", err)
	}

	// 验证结果是 DocIDQuery 或 MatchNoneQuery
	switch q := resultQuery.(type) {
	case *query.DocIDQuery:
		t.Logf("Result query has %d document IDs", len(q.IDs))
		// 验证返回的是父文档 ID
		found := false
		for _, id := range q.IDs {
			if id == "q1" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected parent document 'q1' in results")
		}
	case *query.MatchNoneQuery:
		t.Log("No matching parent documents found")
	default:
		t.Errorf("Unexpected query type: %T", resultQuery)
	}
}

// TestHasParentQueryExecution 测试 has_parent 查询的完整执行
func TestHasParentQueryExecution(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_test_has_parent_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建索引
	idx, err := bleve.New(tempDir, bleve.NewIndexMapping())
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	defer idx.Close()

	// 索引父文档
	parentDoc := map[string]interface{}{
		"_join_name": "question",
		"title":      "What is Elasticsearch?",
		"tags":       []string{"search", "database"},
	}
	if err := idx.Index("q1", parentDoc); err != nil {
		t.Fatalf("Failed to index parent document: %v", err)
	}

	// 索引子文档
	childDoc := map[string]interface{}{
		"_join_name":   "answer",
		"_join_parent": "q1",
		"body":         "Elasticsearch is a search engine",
	}
	if err := idx.Index("a1", childDoc); err != nil {
		t.Fatalf("Failed to index child document: %v", err)
	}

	// 创建内部查询
	innerQuery := query.NewMatchQuery("elasticsearch")
	innerQuery.SetField("title")

	// 执行 has_parent 查询
	resultQuery, err := ExecuteHasParentQuery(nil, idx, "question", innerQuery, 1.0)
	if err != nil {
		t.Fatalf("Failed to execute has_parent query: %v", err)
	}

	t.Logf("Result query type: %T", resultQuery)
}
