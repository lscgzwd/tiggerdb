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
	"os"
	"sort"
	"testing"

	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/mapping"
	"github.com/lscgzwd/tiggerdb/search/query"
)

// TestHasChildQueryDataAccuracy 测试 has_child 查询的数据准确性
// 验证：存入父子文档，通过 has_child 查询能准确返回有匹配子文档的父文档
func TestHasChildQueryDataAccuracy(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_integration_has_child_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建索引映射
	indexMapping := mapping.NewIndexMapping()

	// 创建索引
	idx, err := bleve.New(tempDir, indexMapping)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	defer idx.Close()

	// 测试数据：3个问题(父文档)，每个问题有不同数量的答案(子文档)
	testData := []struct {
		parentID    string
		parentTitle string
		answers     []struct {
			id      string
			body    string
			upvotes int
		}
	}{
		{
			parentID:    "q1",
			parentTitle: "What is Elasticsearch?",
			answers: []struct {
				id      string
				body    string
				upvotes int
			}{
				{id: "a1", body: "Elasticsearch is a distributed search engine", upvotes: 100},
				{id: "a2", body: "It is built on Apache Lucene", upvotes: 50},
			},
		},
		{
			parentID:    "q2",
			parentTitle: "How to use TigerDB?",
			answers: []struct {
				id      string
				body    string
				upvotes int
			}{
				{id: "a3", body: "TigerDB is compatible with Elasticsearch API", upvotes: 80},
			},
		},
		{
			parentID:    "q3",
			parentTitle: "What is Redis?",
			answers: []struct {
				id      string
				body    string
				upvotes int
			}{
				{id: "a4", body: "Redis is an in-memory data store", upvotes: 120},
				{id: "a5", body: "Redis supports various data structures", upvotes: 60},
				{id: "a6", body: "Redis can be used as cache", upvotes: 40},
			},
		},
	}

	// 索引所有文档
	for _, td := range testData {
		// 索引父文档
		parentDoc := map[string]interface{}{
			"_join_name": "question",
			"title":      td.parentTitle,
		}
		if err := idx.Index(td.parentID, parentDoc); err != nil {
			t.Fatalf("Failed to index parent document %s: %v", td.parentID, err)
		}

		// 索引子文档
		for _, ans := range td.answers {
			childDoc := map[string]interface{}{
				"_join_name":   "answer",
				"_join_parent": td.parentID,
				"body":         ans.body,
				"upvotes":      ans.upvotes,
			}
			if err := idx.Index(ans.id, childDoc); err != nil {
				t.Fatalf("Failed to index child document %s: %v", ans.id, err)
			}
		}
	}

	// 测试用例
	tests := []struct {
		name              string
		childQueryField   string
		childQueryValue   string
		expectedParentIDs []string
	}{
		{
			name:              "查找包含'search engine'答案的问题",
			childQueryField:   "body",
			childQueryValue:   "search engine",
			expectedParentIDs: []string{"q1"}, // 只有 q1 的答案包含 "search engine"
		},
		{
			name:              "查找包含'Elasticsearch'答案的问题",
			childQueryField:   "body",
			childQueryValue:   "elasticsearch",
			expectedParentIDs: []string{"q1", "q2"}, // q1 和 q2 的答案包含相关内容
		},
		{
			name:              "查找包含'Redis'答案的问题",
			childQueryField:   "body",
			childQueryValue:   "redis",
			expectedParentIDs: []string{"q3"}, // 只有 q3 的答案包含 "Redis"
		},
		{
			name:              "查找包含'cache'答案的问题",
			childQueryField:   "body",
			childQueryValue:   "cache",
			expectedParentIDs: []string{"q3"}, // 只有 q3 的答案包含 "cache"
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建内部查询
			innerQuery := query.NewMatchQuery(tt.childQueryValue)
			innerQuery.SetField(tt.childQueryField)

			// 执行 has_child 查询
			resultQuery, err := ExecuteHasChildQuery(nil, idx, "answer", innerQuery, 1.0)
			if err != nil {
				t.Fatalf("ExecuteHasChildQuery failed: %v", err)
			}

			// 使用结果查询执行搜索
			searchReq := bleve.NewSearchRequest(resultQuery)
			searchReq.Size = 100

			searchResult, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			// 收集结果 ID
			gotIDs := make([]string, 0, len(searchResult.Hits))
			for _, hit := range searchResult.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			// 排序以便比较
			sort.Strings(gotIDs)
			sort.Strings(tt.expectedParentIDs)

			// 验证结果
			t.Logf("Expected: %v, Got: %v", tt.expectedParentIDs, gotIDs)

			if len(gotIDs) != len(tt.expectedParentIDs) {
				t.Errorf("Expected %d results, got %d", len(tt.expectedParentIDs), len(gotIDs))
				return
			}

			for i, expected := range tt.expectedParentIDs {
				if i >= len(gotIDs) || gotIDs[i] != expected {
					t.Errorf("Result mismatch at index %d: expected %s, got %s", i, expected, gotIDs[i])
				}
			}
		})
	}
}

// TestHasParentQueryDataAccuracy 测试 has_parent 查询的数据准确性
// 验证：存入父子文档，通过 has_parent 查询能准确返回有匹配父文档的子文档
func TestHasParentQueryDataAccuracy(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_integration_has_parent_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建索引
	idx, err := bleve.New(tempDir, mapping.NewIndexMapping())
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	defer idx.Close()

	// 测试数据：分类(父文档)和产品(子文档)
	categories := []struct {
		id       string
		name     string
		products []struct {
			id    string
			name  string
			price float64
		}
	}{
		{
			id:   "cat1",
			name: "Electronics",
			products: []struct {
				id    string
				name  string
				price float64
			}{
				{id: "p1", name: "Laptop", price: 999.99},
				{id: "p2", name: "Phone", price: 699.99},
			},
		},
		{
			id:   "cat2",
			name: "Books",
			products: []struct {
				id    string
				name  string
				price float64
			}{
				{id: "p3", name: "Programming Book", price: 49.99},
				{id: "p4", name: "Novel", price: 19.99},
			},
		},
		{
			id:   "cat3",
			name: "Sports",
			products: []struct {
				id    string
				name  string
				price float64
			}{
				{id: "p5", name: "Basketball", price: 29.99},
			},
		},
	}

	// 索引所有文档
	for _, cat := range categories {
		// 索引父文档（分类）
		parentDoc := map[string]interface{}{
			"_join_name": "category",
			"name":       cat.name,
		}
		if err := idx.Index(cat.id, parentDoc); err != nil {
			t.Fatalf("Failed to index category %s: %v", cat.id, err)
		}

		// 索引子文档（产品）
		for _, prod := range cat.products {
			childDoc := map[string]interface{}{
				"_join_name":   "product",
				"_join_parent": cat.id,
				"name":         prod.name,
				"price":        prod.price,
			}
			if err := idx.Index(prod.id, childDoc); err != nil {
				t.Fatalf("Failed to index product %s: %v", prod.id, err)
			}
		}
	}

	// 测试用例
	tests := []struct {
		name               string
		parentQueryField   string
		parentQueryValue   string
		expectedChildIDs   []string
		expectedChildCount int
	}{
		{
			name:               "查找Electronics分类下的产品",
			parentQueryField:   "name",
			parentQueryValue:   "electronics",
			expectedChildIDs:   []string{"p1", "p2"},
			expectedChildCount: 2,
		},
		{
			name:               "查找Books分类下的产品",
			parentQueryField:   "name",
			parentQueryValue:   "books",
			expectedChildIDs:   []string{"p3", "p4"},
			expectedChildCount: 2,
		},
		{
			name:               "查找Sports分类下的产品",
			parentQueryField:   "name",
			parentQueryValue:   "sports",
			expectedChildIDs:   []string{"p5"},
			expectedChildCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建内部查询
			innerQuery := query.NewMatchQuery(tt.parentQueryValue)
			innerQuery.SetField(tt.parentQueryField)

			// 执行 has_parent 查询
			resultQuery, err := ExecuteHasParentQuery(nil, idx, "category", innerQuery, 1.0)
			if err != nil {
				t.Fatalf("ExecuteHasParentQuery failed: %v", err)
			}

			// 使用结果查询执行搜索
			searchReq := bleve.NewSearchRequest(resultQuery)
			searchReq.Size = 100

			searchResult, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			// 收集结果 ID
			gotIDs := make([]string, 0, len(searchResult.Hits))
			for _, hit := range searchResult.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			// 排序以便比较
			sort.Strings(gotIDs)
			sort.Strings(tt.expectedChildIDs)

			t.Logf("Expected: %v, Got: %v", tt.expectedChildIDs, gotIDs)

			// 验证数量
			if len(gotIDs) != tt.expectedChildCount {
				t.Errorf("Expected %d results, got %d", tt.expectedChildCount, len(gotIDs))
			}

			// 验证 ID 匹配
			for i, expected := range tt.expectedChildIDs {
				if i >= len(gotIDs) {
					t.Errorf("Missing expected result: %s", expected)
					continue
				}
				if gotIDs[i] != expected {
					t.Errorf("Result mismatch at index %d: expected %s, got %s", i, expected, gotIDs[i])
				}
			}
		})
	}
}

// TestPercolateQueryDataAccuracy 测试 percolate 查询的数据准确性
// 验证：存入查询文档，通过 percolate 能准确匹配文档与存储的查询
func TestPercolateQueryDataAccuracy(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_integration_percolate_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建索引
	idx, err := bleve.New(tempDir, mapping.NewIndexMapping())
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	defer idx.Close()

	// 存储查询文档（模拟 percolator 字段）
	storedQueries := []struct {
		id    string
		query map[string]interface{}
		desc  string
	}{
		{
			id: "query1",
			query: map[string]interface{}{
				"match": map[string]interface{}{
					"title": "elasticsearch",
				},
			},
			desc: "匹配标题包含 elasticsearch 的文档",
		},
		{
			id: "query2",
			query: map[string]interface{}{
				"match": map[string]interface{}{
					"content": "search engine",
				},
			},
			desc: "匹配内容包含 search engine 的文档",
		},
		{
			id: "query3",
			query: map[string]interface{}{
				"match": map[string]interface{}{
					"tags": "database",
				},
			},
			desc: "匹配标签包含 database 的文档",
		},
	}

	// 索引存储的查询
	for _, sq := range storedQueries {
		queryJSON, _ := json.Marshal(sq.query)
		doc := map[string]interface{}{
			"_has_percolator":   "true",
			"_percolator_query": string(queryJSON),
			"description":       sq.desc,
		}
		if err := idx.Index(sq.id, doc); err != nil {
			t.Fatalf("Failed to index stored query %s: %v", sq.id, err)
		}
	}

	// 测试用例
	tests := []struct {
		name             string
		document         map[string]interface{}
		expectedQueryIDs []string
	}{
		{
			name: "文档匹配 query1 (elasticsearch in title)",
			document: map[string]interface{}{
				"title":   "Introduction to Elasticsearch",
				"content": "This is a guide about data storage",
				"tags":    "tutorial",
			},
			expectedQueryIDs: []string{"query1"},
		},
		{
			name: "文档匹配 query2 (search engine in content)",
			document: map[string]interface{}{
				"title":   "Building Applications",
				"content": "How to build a search engine",
				"tags":    "development",
			},
			expectedQueryIDs: []string{"query2"},
		},
		{
			name: "文档匹配 query1 和 query3",
			document: map[string]interface{}{
				"title":   "Elasticsearch as a Database",
				"content": "Using ES for data storage",
				"tags":    "database",
			},
			expectedQueryIDs: []string{"query1", "query3"},
		},
		{
			name: "文档不匹配任何查询",
			document: map[string]interface{}{
				"title":   "Cooking Recipes",
				"content": "How to make pasta",
				"tags":    "food",
			},
			expectedQueryIDs: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建 percolate 查询信息
			info := &PercolateQueryInfo{
				Field:    "query",
				Document: tt.document,
				Boost:    1.0,
			}

			// 执行 percolate 查询
			resultQuery, err := ExecutePercolateQuery(nil, idx, info)
			if err != nil {
				t.Fatalf("ExecutePercolateQuery failed: %v", err)
			}

			// 使用结果查询执行搜索
			searchReq := bleve.NewSearchRequest(resultQuery)
			searchReq.Size = 100

			searchResult, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			// 收集结果 ID
			gotIDs := make([]string, 0, len(searchResult.Hits))
			for _, hit := range searchResult.Hits {
				gotIDs = append(gotIDs, hit.ID)
			}

			// 排序以便比较
			sort.Strings(gotIDs)
			sort.Strings(tt.expectedQueryIDs)

			t.Logf("Expected: %v, Got: %v", tt.expectedQueryIDs, gotIDs)

			// 验证数量
			if len(gotIDs) != len(tt.expectedQueryIDs) {
				t.Errorf("Expected %d matching queries, got %d", len(tt.expectedQueryIDs), len(gotIDs))
			}

			// 验证 ID 匹配
			for _, expected := range tt.expectedQueryIDs {
				found := false
				for _, got := range gotIDs {
					if got == expected {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected query %s to match, but it didn't", expected)
				}
			}
		})
	}
}

// TestJoinQueryWithAggregation 测试父子查询与聚合的结合
func TestJoinQueryWithAggregation(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_integration_join_agg_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建索引
	idx, err := bleve.New(tempDir, mapping.NewIndexMapping())
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	defer idx.Close()

	// 索引部门（父文档）和员工（子文档）
	departments := []struct {
		id        string
		name      string
		employees []struct {
			id     string
			name   string
			salary float64
		}
	}{
		{
			id:   "dept1",
			name: "Engineering",
			employees: []struct {
				id     string
				name   string
				salary float64
			}{
				{id: "emp1", name: "Alice", salary: 80000},
				{id: "emp2", name: "Bob", salary: 90000},
				{id: "emp3", name: "Charlie", salary: 75000},
			},
		},
		{
			id:   "dept2",
			name: "Marketing",
			employees: []struct {
				id     string
				name   string
				salary float64
			}{
				{id: "emp4", name: "Diana", salary: 70000},
				{id: "emp5", name: "Eve", salary: 65000},
			},
		},
	}

	// 索引文档
	for _, dept := range departments {
		parentDoc := map[string]interface{}{
			"_join_name": "department",
			"name":       dept.name,
		}
		if err := idx.Index(dept.id, parentDoc); err != nil {
			t.Fatalf("Failed to index department %s: %v", dept.id, err)
		}

		for _, emp := range dept.employees {
			childDoc := map[string]interface{}{
				"_join_name":   "employee",
				"_join_parent": dept.id,
				"name":         emp.name,
				"salary":       emp.salary,
			}
			if err := idx.Index(emp.id, childDoc); err != nil {
				t.Fatalf("Failed to index employee %s: %v", emp.id, err)
			}
		}
	}

	// 测试：查找有高薪员工(>70000)的部门
	t.Run("查找有高薪员工的部门", func(t *testing.T) {
		// 创建内部查询：salary > 70000
		min := float64(70000)
		innerQuery := query.NewNumericRangeQuery(&min, nil)
		innerQuery.SetField("salary")

		// 执行 has_child 查询
		resultQuery, err := ExecuteHasChildQuery(nil, idx, "employee", innerQuery, 1.0)
		if err != nil {
			t.Fatalf("ExecuteHasChildQuery failed: %v", err)
		}

		// 执行搜索
		searchReq := bleve.NewSearchRequest(resultQuery)
		searchReq.Size = 100

		searchResult, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		// 验证结果
		t.Logf("Found %d departments with high-salary employees", searchResult.Total)

		// 应该找到两个部门（Engineering 有 3 个高薪员工，Marketing 有 1 个）
		expectedDepts := map[string]bool{"dept1": true, "dept2": true}
		for _, hit := range searchResult.Hits {
			if !expectedDepts[hit.ID] {
				t.Errorf("Unexpected department in results: %s", hit.ID)
			}
			delete(expectedDepts, hit.ID)
		}

		for deptID := range expectedDepts {
			t.Errorf("Expected department %s not found in results", deptID)
		}
	})

	// 测试：统计每个部门的员工数量
	t.Run("统计部门员工数量", func(t *testing.T) {
		// 查询所有员工类型的文档
		typeQuery := query.NewTermQuery("employee")
		typeQuery.SetField("_join_name")

		searchReq := bleve.NewSearchRequest(typeQuery)
		searchReq.Size = 100

		searchResult, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		// 统计员工数量
		t.Logf("Total employees found: %d", searchResult.Total)

		// 验证总数
		if searchResult.Total != 5 {
			t.Errorf("Expected 5 employees, got %d", searchResult.Total)
		}
	})
}

// TestDataIntegrityAfterIndexing 测试索引后数据的完整性
func TestDataIntegrityAfterIndexing(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_integration_integrity_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建索引
	idx, err := bleve.New(tempDir, mapping.NewIndexMapping())
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	defer idx.Close()

	// 准备测试数据
	testDocs := []struct {
		id   string
		data map[string]interface{}
	}{
		{
			id: "doc1",
			data: map[string]interface{}{
				"title":    "Document One",
				"content":  "This is the first document",
				"category": "test",
				"count":    10,
				"active":   true,
			},
		},
		{
			id: "doc2",
			data: map[string]interface{}{
				"title":    "Document Two",
				"content":  "This is the second document",
				"category": "test",
				"count":    20,
				"active":   false,
			},
		},
		{
			id: "doc3",
			data: map[string]interface{}{
				"title":    "Document Three",
				"content":  "This is the third document",
				"category": "production",
				"count":    30,
				"active":   true,
			},
		},
	}

	// 索引文档
	for _, td := range testDocs {
		if err := idx.Index(td.id, td.data); err != nil {
			t.Fatalf("Failed to index document %s: %v", td.id, err)
		}
	}

	// 验证文档数量
	t.Run("验证文档总数", func(t *testing.T) {
		count, err := idx.DocCount()
		if err != nil {
			t.Fatalf("Failed to get doc count: %v", err)
		}
		if count != uint64(len(testDocs)) {
			t.Errorf("Expected %d documents, got %d", len(testDocs), count)
		}
	})

	// 验证 term 查询准确性
	t.Run("验证 term 查询", func(t *testing.T) {
		termQuery := query.NewTermQuery("test")
		termQuery.SetField("category")

		searchReq := bleve.NewSearchRequest(termQuery)
		searchResult, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		// 应该找到 2 个 category=test 的文档
		if searchResult.Total != 2 {
			t.Errorf("Expected 2 documents with category=test, got %d", searchResult.Total)
		}
	})

	// 验证 range 查询准确性
	t.Run("验证 range 查询", func(t *testing.T) {
		min := float64(15)
		max := float64(25)
		rangeQuery := query.NewNumericRangeQuery(&min, &max)
		rangeQuery.SetField("count")

		searchReq := bleve.NewSearchRequest(rangeQuery)
		searchResult, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		// 应该找到 1 个 count 在 15-25 之间的文档 (doc2 count=20)
		if searchResult.Total != 1 {
			t.Errorf("Expected 1 document with count in [15,25], got %d", searchResult.Total)
		}

		if len(searchResult.Hits) > 0 && searchResult.Hits[0].ID != "doc2" {
			t.Errorf("Expected doc2, got %s", searchResult.Hits[0].ID)
		}
	})

	// 验证 bool 查询准确性
	t.Run("验证 bool 查询", func(t *testing.T) {
		// active=true AND category=test
		activeQuery := query.NewTermQuery("true")
		activeQuery.SetField("active")

		categoryQuery := query.NewTermQuery("test")
		categoryQuery.SetField("category")

		boolQuery := query.NewConjunctionQuery([]query.Query{activeQuery, categoryQuery})

		searchReq := bleve.NewSearchRequest(boolQuery)
		searchResult, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		// 应该找到 1 个文档 (doc1: active=true, category=test)
		if searchResult.Total != 1 {
			t.Errorf("Expected 1 document with active=true AND category=test, got %d", searchResult.Total)
		}

		if len(searchResult.Hits) > 0 && searchResult.Hits[0].ID != "doc1" {
			t.Errorf("Expected doc1, got %s", searchResult.Hits[0].ID)
		}
	})
}
