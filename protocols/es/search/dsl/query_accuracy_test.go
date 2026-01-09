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
	"os"
	"sort"
	"testing"

	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/mapping"
)

// setupTestIndex 创建测试索引并填充测试数据
func setupTestIndex(t *testing.T) (bleve.Index, func()) {
	tempDir, err := os.MkdirTemp("", "tigerdb_query_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	idx, err := bleve.New(tempDir, mapping.NewIndexMapping())
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create index: %v", err)
	}

	// 索引测试文档
	testDocs := []struct {
		id   string
		data map[string]interface{}
	}{
		{
			id: "doc1",
			data: map[string]interface{}{
				"title":       "Introduction to Elasticsearch",
				"content":     "Elasticsearch is a distributed search and analytics engine",
				"author":      "John Smith",
				"category":    "technology",
				"tags":        []string{"search", "elasticsearch", "database"},
				"views":       1500,
				"rating":      4.5,
				"published":   true,
				"create_date": "2024-01-15",
			},
		},
		{
			id: "doc2",
			data: map[string]interface{}{
				"title":       "TigerDB User Guide",
				"content":     "TigerDB is compatible with Elasticsearch API and provides high performance",
				"author":      "Jane Doe",
				"category":    "technology",
				"tags":        []string{"tigerdb", "search", "guide"},
				"views":       800,
				"rating":      4.8,
				"published":   true,
				"create_date": "2024-02-20",
			},
		},
		{
			id: "doc3",
			data: map[string]interface{}{
				"title":       "Redis Caching Strategies",
				"content":     "Redis is an in-memory data structure store used as cache",
				"author":      "Bob Wilson",
				"category":    "technology",
				"tags":        []string{"redis", "cache", "performance"},
				"views":       2000,
				"rating":      4.2,
				"published":   true,
				"create_date": "2024-03-10",
			},
		},
		{
			id: "doc4",
			data: map[string]interface{}{
				"title":       "Draft Article on MongoDB",
				"content":     "MongoDB is a document database with scalability and flexibility",
				"author":      "John Smith",
				"category":    "technology",
				"tags":        []string{"mongodb", "database", "nosql"},
				"views":       300,
				"rating":      3.5,
				"published":   false,
				"create_date": "2024-04-01",
			},
		},
		{
			id: "doc5",
			data: map[string]interface{}{
				"title":       "Cooking with Python",
				"content":     "Learn to create recipes using Python programming language",
				"author":      "Alice Brown",
				"category":    "programming",
				"tags":        []string{"python", "cooking", "tutorial"},
				"views":       500,
				"rating":      4.0,
				"published":   true,
				"create_date": "2024-05-15",
			},
		},
	}

	for _, td := range testDocs {
		if err := idx.Index(td.id, td.data); err != nil {
			idx.Close()
			os.RemoveAll(tempDir)
			t.Fatalf("Failed to index document %s: %v", td.id, err)
		}
	}

	cleanup := func() {
		idx.Close()
		os.RemoveAll(tempDir)
	}

	return idx, cleanup
}

// verifySearchResults 验证搜索结果
func verifySearchResults(t *testing.T, gotIDs, expectedIDs []string, testName string) {
	sort.Strings(gotIDs)
	sort.Strings(expectedIDs)

	if len(gotIDs) != len(expectedIDs) {
		t.Errorf("[%s] Expected %d results, got %d. Expected: %v, Got: %v",
			testName, len(expectedIDs), len(gotIDs), expectedIDs, gotIDs)
		return
	}

	for i, expected := range expectedIDs {
		if i >= len(gotIDs) || gotIDs[i] != expected {
			t.Errorf("[%s] Result mismatch: expected %v, got %v", testName, expectedIDs, gotIDs)
			return
		}
	}
}

// ========== 全文查询测试 ==========

// TestMatchQueryAccuracy 测试 match 查询的数据准确性
func TestMatchQueryAccuracy(t *testing.T) {
	idx, cleanup := setupTestIndex(t)
	defer cleanup()

	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectedIDs []string
	}{
		{
			name: "match title with elasticsearch",
			query: map[string]interface{}{
				"match": map[string]interface{}{
					"title": "elasticsearch",
				},
			},
			expectedIDs: []string{"doc1"},
		},
		{
			name: "match content with search",
			query: map[string]interface{}{
				"match": map[string]interface{}{
					"content": "search",
				},
			},
			// 注意：doc1 的 content 包含独立的 "search" 词："Elasticsearch is a distributed search and analytics engine"
			// doc2 的 content 是 "TigerDB is compatible with Elasticsearch API and provides high performance"
			// "Elasticsearch" 被分词为一个词 "elasticsearch"，不包含独立的 "search" 词
			// 所以只匹配 doc1
			expectedIDs: []string{"doc1"},
		},
		{
			name: "match author John Smith",
			query: map[string]interface{}{
				"match": map[string]interface{}{
					"author": "John Smith",
				},
			},
			expectedIDs: []string{"doc1", "doc4"},
		},
		{
			name: "match non-existent term",
			query: map[string]interface{}{
				"match": map[string]interface{}{
					"title": "nonexistent12345",
				},
			},
			expectedIDs: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bleveQuery, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			searchReq := bleve.NewSearchRequest(bleveQuery)
			searchReq.Size = 100

			result, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			gotIDs := make([]string, 0, len(result.Hits))
			for _, hit := range result.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			verifySearchResults(t, gotIDs, tt.expectedIDs, tt.name)
		})
	}
}

// TestMatchPhraseQueryAccuracy 测试 match_phrase 查询的数据准确性
func TestMatchPhraseQueryAccuracy(t *testing.T) {
	idx, cleanup := setupTestIndex(t)
	defer cleanup()

	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectedIDs []string
	}{
		{
			name: "match_phrase exact phrase",
			query: map[string]interface{}{
				"match_phrase": map[string]interface{}{
					"content": "distributed search",
				},
			},
			expectedIDs: []string{"doc1"},
		},
		{
			name: "match_phrase in-memory data",
			query: map[string]interface{}{
				"match_phrase": map[string]interface{}{
					"content": "in-memory data",
				},
			},
			expectedIDs: []string{"doc3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bleveQuery, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			searchReq := bleve.NewSearchRequest(bleveQuery)
			searchReq.Size = 100

			result, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			gotIDs := make([]string, 0, len(result.Hits))
			for _, hit := range result.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			verifySearchResults(t, gotIDs, tt.expectedIDs, tt.name)
		})
	}
}

// TestMultiMatchQueryAccuracy 测试 multi_match 查询的数据准确性
func TestMultiMatchQueryAccuracy(t *testing.T) {
	idx, cleanup := setupTestIndex(t)
	defer cleanup()

	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectedIDs []string
	}{
		{
			name: "multi_match across title and content",
			query: map[string]interface{}{
				"multi_match": map[string]interface{}{
					"query":  "elasticsearch",
					"fields": []interface{}{"title", "content"},
				},
			},
			expectedIDs: []string{"doc1", "doc2"},
		},
		{
			name: "multi_match redis",
			query: map[string]interface{}{
				"multi_match": map[string]interface{}{
					"query":  "redis cache",
					"fields": []interface{}{"title", "content", "tags"},
				},
			},
			expectedIDs: []string{"doc3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bleveQuery, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			searchReq := bleve.NewSearchRequest(bleveQuery)
			searchReq.Size = 100

			result, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			gotIDs := make([]string, 0, len(result.Hits))
			for _, hit := range result.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			verifySearchResults(t, gotIDs, tt.expectedIDs, tt.name)
		})
	}
}

// ========== 精确查询测试 ==========

// TestTermQueryAccuracy 测试 term 查询的数据准确性
func TestTermQueryAccuracy(t *testing.T) {
	idx, cleanup := setupTestIndex(t)
	defer cleanup()

	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectedIDs []string
	}{
		{
			name: "term query on category",
			query: map[string]interface{}{
				"term": map[string]interface{}{
					"category": "technology",
				},
			},
			expectedIDs: []string{"doc1", "doc2", "doc3", "doc4"},
		},
		{
			name: "term query on programming category",
			query: map[string]interface{}{
				"term": map[string]interface{}{
					"category": "programming",
				},
			},
			expectedIDs: []string{"doc5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bleveQuery, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			searchReq := bleve.NewSearchRequest(bleveQuery)
			searchReq.Size = 100

			result, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			gotIDs := make([]string, 0, len(result.Hits))
			for _, hit := range result.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			verifySearchResults(t, gotIDs, tt.expectedIDs, tt.name)
		})
	}
}

// TestTermsQueryAccuracy 测试 terms 查询的数据准确性
func TestTermsQueryAccuracy(t *testing.T) {
	idx, cleanup := setupTestIndex(t)
	defer cleanup()

	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectedIDs []string
	}{
		{
			name: "terms query on author",
			query: map[string]interface{}{
				"terms": map[string]interface{}{
					"author": []interface{}{"John Smith", "Jane Doe"},
				},
			},
			expectedIDs: []string{"doc1", "doc2", "doc4"},
		},
		{
			name: "terms query on category",
			query: map[string]interface{}{
				"terms": map[string]interface{}{
					"category": []interface{}{"technology", "programming"},
				},
			},
			expectedIDs: []string{"doc1", "doc2", "doc3", "doc4", "doc5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bleveQuery, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			searchReq := bleve.NewSearchRequest(bleveQuery)
			searchReq.Size = 100

			result, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			gotIDs := make([]string, 0, len(result.Hits))
			for _, hit := range result.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			verifySearchResults(t, gotIDs, tt.expectedIDs, tt.name)
		})
	}
}

// TestRangeQueryAccuracy 测试 range 查询的数据准确性
func TestRangeQueryAccuracy(t *testing.T) {
	idx, cleanup := setupTestIndex(t)
	defer cleanup()

	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectedIDs []string
	}{
		{
			name: "range query views >= 1000",
			query: map[string]interface{}{
				"range": map[string]interface{}{
					"views": map[string]interface{}{
						"gte": 1000,
					},
				},
			},
			expectedIDs: []string{"doc1", "doc3"},
		},
		{
			name: "range query views 500-1000",
			query: map[string]interface{}{
				"range": map[string]interface{}{
					"views": map[string]interface{}{
						"gte": 500,
						"lte": 1000,
					},
				},
			},
			expectedIDs: []string{"doc2", "doc5"},
		},
		{
			name: "range query rating > 4.0",
			query: map[string]interface{}{
				"range": map[string]interface{}{
					"rating": map[string]interface{}{
						"gt": 4.0,
					},
				},
			},
			expectedIDs: []string{"doc1", "doc2", "doc3"},
		},
		{
			name: "range query views < 500",
			query: map[string]interface{}{
				"range": map[string]interface{}{
					"views": map[string]interface{}{
						"lt": 500,
					},
				},
			},
			expectedIDs: []string{"doc4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bleveQuery, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			searchReq := bleve.NewSearchRequest(bleveQuery)
			searchReq.Size = 100

			result, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			gotIDs := make([]string, 0, len(result.Hits))
			for _, hit := range result.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			verifySearchResults(t, gotIDs, tt.expectedIDs, tt.name)
		})
	}
}

// TestExistsQueryAccuracy 测试 exists 查询的数据准确性
func TestExistsQueryAccuracy(t *testing.T) {
	idx, cleanup := setupTestIndex(t)
	defer cleanup()

	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectedIDs []string
	}{
		{
			name: "exists query on title",
			query: map[string]interface{}{
				"exists": map[string]interface{}{
					"field": "title",
				},
			},
			expectedIDs: []string{"doc1", "doc2", "doc3", "doc4", "doc5"},
		},
		{
			name: "exists query on tags",
			query: map[string]interface{}{
				"exists": map[string]interface{}{
					"field": "tags",
				},
			},
			expectedIDs: []string{"doc1", "doc2", "doc3", "doc4", "doc5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bleveQuery, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			searchReq := bleve.NewSearchRequest(bleveQuery)
			searchReq.Size = 100

			result, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			gotIDs := make([]string, 0, len(result.Hits))
			for _, hit := range result.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			verifySearchResults(t, gotIDs, tt.expectedIDs, tt.name)
		})
	}
}

// TestIdsQueryAccuracy 测试 ids 查询的数据准确性
func TestIdsQueryAccuracy(t *testing.T) {
	idx, cleanup := setupTestIndex(t)
	defer cleanup()

	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectedIDs []string
	}{
		{
			name: "ids query with specific ids",
			query: map[string]interface{}{
				"ids": map[string]interface{}{
					"values": []interface{}{"doc1", "doc3", "doc5"},
				},
			},
			expectedIDs: []string{"doc1", "doc3", "doc5"},
		},
		{
			name: "ids query with single id",
			query: map[string]interface{}{
				"ids": map[string]interface{}{
					"values": []interface{}{"doc2"},
				},
			},
			expectedIDs: []string{"doc2"},
		},
		{
			name: "ids query with non-existent id",
			query: map[string]interface{}{
				"ids": map[string]interface{}{
					"values": []interface{}{"doc999"},
				},
			},
			expectedIDs: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bleveQuery, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			searchReq := bleve.NewSearchRequest(bleveQuery)
			searchReq.Size = 100

			result, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			gotIDs := make([]string, 0, len(result.Hits))
			for _, hit := range result.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			verifySearchResults(t, gotIDs, tt.expectedIDs, tt.name)
		})
	}
}

// ========== 复合查询测试 ==========

// TestBoolQueryAccuracy 测试 bool 查询的数据准确性
func TestBoolQueryAccuracy(t *testing.T) {
	idx, cleanup := setupTestIndex(t)
	defer cleanup()

	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectedIDs []string
	}{
		{
			name: "bool must query",
			query: map[string]interface{}{
				"bool": map[string]interface{}{
					"must": []interface{}{
						map[string]interface{}{
							"term": map[string]interface{}{
								"category": "technology",
							},
						},
						map[string]interface{}{
							"term": map[string]interface{}{
								"published": "true",
							},
						},
					},
				},
			},
			expectedIDs: []string{"doc1", "doc2", "doc3"},
		},
		{
			name: "bool should query",
			query: map[string]interface{}{
				"bool": map[string]interface{}{
					"should": []interface{}{
						map[string]interface{}{
							"match": map[string]interface{}{
								"title": "elasticsearch",
							},
						},
						map[string]interface{}{
							"match": map[string]interface{}{
								"title": "redis",
							},
						},
					},
					"minimum_should_match": 1,
				},
			},
			expectedIDs: []string{"doc1", "doc3"},
		},
		{
			name: "bool must_not query",
			query: map[string]interface{}{
				"bool": map[string]interface{}{
					"must": []interface{}{
						map[string]interface{}{
							"term": map[string]interface{}{
								"category": "technology",
							},
						},
					},
					"must_not": []interface{}{
						map[string]interface{}{
							"term": map[string]interface{}{
								"published": "false",
							},
						},
					},
				},
			},
			expectedIDs: []string{"doc1", "doc2", "doc3"},
		},
		{
			name: "bool filter query",
			query: map[string]interface{}{
				"bool": map[string]interface{}{
					"filter": []interface{}{
						map[string]interface{}{
							"range": map[string]interface{}{
								"views": map[string]interface{}{
									"gte": 1000,
								},
							},
						},
					},
				},
			},
			expectedIDs: []string{"doc1", "doc3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bleveQuery, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			searchReq := bleve.NewSearchRequest(bleveQuery)
			searchReq.Size = 100

			result, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			gotIDs := make([]string, 0, len(result.Hits))
			for _, hit := range result.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			verifySearchResults(t, gotIDs, tt.expectedIDs, tt.name)
		})
	}
}

// TestWildcardQueryAccuracy 测试 wildcard 查询的数据准确性
func TestWildcardQueryAccuracy(t *testing.T) {
	idx, cleanup := setupTestIndex(t)
	defer cleanup()

	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectedIDs []string
	}{
		{
			name: "wildcard query with *",
			query: map[string]interface{}{
				"wildcard": map[string]interface{}{
					"title": "*Guide*",
				},
			},
			expectedIDs: []string{"doc2"},
		},
		{
			name: "wildcard query with ?",
			query: map[string]interface{}{
				"wildcard": map[string]interface{}{
					"category": "technolog?",
				},
			},
			expectedIDs: []string{"doc1", "doc2", "doc3", "doc4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bleveQuery, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			searchReq := bleve.NewSearchRequest(bleveQuery)
			searchReq.Size = 100

			result, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			gotIDs := make([]string, 0, len(result.Hits))
			for _, hit := range result.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			verifySearchResults(t, gotIDs, tt.expectedIDs, tt.name)
		})
	}
}

// TestPrefixQueryAccuracy 测试 prefix 查询的数据准确性
func TestPrefixQueryAccuracy(t *testing.T) {
	idx, cleanup := setupTestIndex(t)
	defer cleanup()

	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectedIDs []string
	}{
		{
			name: "prefix query on category",
			query: map[string]interface{}{
				"prefix": map[string]interface{}{
					"category": "tech",
				},
			},
			expectedIDs: []string{"doc1", "doc2", "doc3", "doc4"},
		},
		{
			name: "prefix query on author",
			query: map[string]interface{}{
				"prefix": map[string]interface{}{
					"author": "john",
				},
			},
			expectedIDs: []string{"doc1", "doc4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bleveQuery, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			searchReq := bleve.NewSearchRequest(bleveQuery)
			searchReq.Size = 100

			result, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			gotIDs := make([]string, 0, len(result.Hits))
			for _, hit := range result.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			verifySearchResults(t, gotIDs, tt.expectedIDs, tt.name)
		})
	}
}

// TestMatchAllMatchNoneAccuracy 测试 match_all 和 match_none 查询
func TestMatchAllMatchNoneAccuracy(t *testing.T) {
	idx, cleanup := setupTestIndex(t)
	defer cleanup()

	parser := NewQueryParser()

	t.Run("match_all returns all documents", func(t *testing.T) {
		query := map[string]interface{}{
			"match_all": map[string]interface{}{},
		}

		bleveQuery, err := parser.ParseQuery(query)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		searchReq := bleve.NewSearchRequest(bleveQuery)
		searchReq.Size = 100

		result, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		if result.Total != 5 {
			t.Errorf("Expected 5 documents, got %d", result.Total)
		}
	})

	t.Run("match_none returns no documents", func(t *testing.T) {
		query := map[string]interface{}{
			"match_none": map[string]interface{}{},
		}

		bleveQuery, err := parser.ParseQuery(query)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		searchReq := bleve.NewSearchRequest(bleveQuery)
		searchReq.Size = 100

		result, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		if result.Total != 0 {
			t.Errorf("Expected 0 documents, got %d", result.Total)
		}
	})
}

// TestQueryStringAccuracy 测试 query_string 查询的数据准确性
func TestQueryStringAccuracy(t *testing.T) {
	idx, cleanup := setupTestIndex(t)
	defer cleanup()

	parser := NewQueryParser()

	tests := []struct {
		name        string
		query       map[string]interface{}
		expectedIDs []string
	}{
		{
			name: "query_string simple",
			query: map[string]interface{}{
				"query_string": map[string]interface{}{
					"query":         "elasticsearch",
					"default_field": "title",
				},
			},
			expectedIDs: []string{"doc1"},
		},
		{
			name: "query_string with AND",
			query: map[string]interface{}{
				"query_string": map[string]interface{}{
					"query":         "search AND engine",
					"default_field": "content",
				},
			},
			expectedIDs: []string{"doc1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bleveQuery, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			searchReq := bleve.NewSearchRequest(bleveQuery)
			searchReq.Size = 100

			result, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			gotIDs := make([]string, 0, len(result.Hits))
			for _, hit := range result.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			verifySearchResults(t, gotIDs, tt.expectedIDs, tt.name)
		})
	}
}
