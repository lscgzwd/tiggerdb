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
	"testing"

	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/mapping"
	"github.com/lscgzwd/tiggerdb/search/query"
)

// setupAggregationTestIndex 创建聚合测试索引
func setupAggregationTestIndex(t *testing.T) (bleve.Index, func()) {
	tempDir, err := os.MkdirTemp("", "tigerdb_agg_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	idx, err := bleve.New(tempDir, mapping.NewIndexMapping())
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create index: %v", err)
	}

	// 索引测试数据：8个产品
	testDocs := []struct {
		id   string
		data map[string]interface{}
	}{
		{
			id: "prod1",
			data: map[string]interface{}{
				"name":     "Laptop Pro",
				"category": "electronics",
				"brand":    "TechCorp",
				"price":    1299.99,
				"quantity": 50,
				"rating":   4.5,
			},
		},
		{
			id: "prod2",
			data: map[string]interface{}{
				"name":     "Smartphone X",
				"category": "electronics",
				"brand":    "TechCorp",
				"price":    899.99,
				"quantity": 100,
				"rating":   4.8,
			},
		},
		{
			id: "prod3",
			data: map[string]interface{}{
				"name":     "Wireless Headphones",
				"category": "electronics",
				"brand":    "AudioMax",
				"price":    199.99,
				"quantity": 200,
				"rating":   4.2,
			},
		},
		{
			id: "prod4",
			data: map[string]interface{}{
				"name":     "Running Shoes",
				"category": "sports",
				"brand":    "SportFit",
				"price":    129.99,
				"quantity": 75,
				"rating":   4.6,
			},
		},
		{
			id: "prod5",
			data: map[string]interface{}{
				"name":     "Yoga Mat",
				"category": "sports",
				"brand":    "FitLife",
				"price":    49.99,
				"quantity": 150,
				"rating":   4.3,
			},
		},
		{
			id: "prod6",
			data: map[string]interface{}{
				"name":     "Coffee Maker",
				"category": "home",
				"brand":    "HomeComfort",
				"price":    79.99,
				"quantity": 80,
				"rating":   4.1,
			},
		},
		{
			id: "prod7",
			data: map[string]interface{}{
				"name":     "Blender",
				"category": "home",
				"brand":    "HomeComfort",
				"price":    59.99,
				"quantity": 60,
				"rating":   3.9,
			},
		},
		{
			id: "prod8",
			data: map[string]interface{}{
				"name":     "Gaming Console",
				"category": "electronics",
				"brand":    "GameTech",
				"price":    499.99,
				"quantity": 30,
				"rating":   4.7,
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

// TestTermsAggregationDataAccuracy 测试 terms 聚合的数据准确性
func TestTermsAggregationDataAccuracy(t *testing.T) {
	idx, cleanup := setupAggregationTestIndex(t)
	defer cleanup()

	t.Run("terms aggregation on category field", func(t *testing.T) {
		// 创建搜索请求
		searchReq := bleve.NewSearchRequest(query.NewMatchAllQuery())
		searchReq.Size = 0

		// 添加 terms facet
		categoryFacet := bleve.NewFacetRequest("category", 10)
		searchReq.AddFacet("categories", categoryFacet)

		// 执行搜索
		result, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		// 验证结果
		categoryResult, ok := result.Facets["categories"]
		if !ok {
			t.Fatal("No categories facet in result")
		}

		// 预期结果：electronics=4, sports=2, home=2
		expectedCounts := map[string]int{
			"electronics": 4,
			"sports":      2,
			"home":        2,
		}

		t.Logf("Total: %d, Terms: %d", categoryResult.Total, len(categoryResult.Terms.Terms()))

		for _, term := range categoryResult.Terms.Terms() {
			expected, exists := expectedCounts[term.Term]
			if !exists {
				t.Errorf("Unexpected category: %s with count %d", term.Term, term.Count)
				continue
			}

			if term.Count != expected {
				t.Errorf("Category %s: expected %d, got %d", term.Term, expected, term.Count)
			} else {
				t.Logf("Category %s: count=%d ✓", term.Term, term.Count)
			}
		}
	})

	t.Run("terms aggregation on brand field", func(t *testing.T) {
		searchReq := bleve.NewSearchRequest(query.NewMatchAllQuery())
		searchReq.Size = 0

		brandFacet := bleve.NewFacetRequest("brand", 10)
		searchReq.AddFacet("brands", brandFacet)

		result, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		brandResult, ok := result.Facets["brands"]
		if !ok {
			t.Fatal("No brands facet in result")
		}

		// 预期结果
		expectedCounts := map[string]int{
			"TechCorp":    2,
			"HomeComfort": 2,
			"AudioMax":    1,
			"SportFit":    1,
			"FitLife":     1,
			"GameTech":    1,
		}

		t.Logf("Total brands: %d", len(brandResult.Terms.Terms()))

		for _, term := range brandResult.Terms.Terms() {
			expected, exists := expectedCounts[term.Term]
			if exists && term.Count != expected {
				t.Errorf("Brand %s: expected %d, got %d", term.Term, expected, term.Count)
			} else {
				t.Logf("Brand %s: count=%d ✓", term.Term, term.Count)
			}
		}
	})
}

// TestNumericRangeAggregationAccuracy 测试数值范围聚合的数据准确性
func TestNumericRangeAggregationAccuracy(t *testing.T) {
	idx, cleanup := setupAggregationTestIndex(t)
	defer cleanup()

	t.Run("numeric range aggregation on price", func(t *testing.T) {
		searchReq := bleve.NewSearchRequest(query.NewMatchAllQuery())
		searchReq.Size = 0

		// 创建数值范围 facet
		priceFacet := bleve.NewFacetRequest("price", 10)
		priceFacet.AddNumericRange("cheap", nil, floatPtr(100))
		priceFacet.AddNumericRange("medium", floatPtr(100), floatPtr(500))
		priceFacet.AddNumericRange("expensive", floatPtr(500), nil)
		searchReq.AddFacet("price_ranges", priceFacet)

		result, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		priceResult, ok := result.Facets["price_ranges"]
		if !ok {
			t.Fatal("No price_ranges facet in result")
		}

		// 预期结果：
		// cheap (< 100): Yoga Mat(49.99), Coffee Maker(79.99), Blender(59.99) = 3
		// medium (100-500): Wireless Headphones(199.99), Running Shoes(129.99), Gaming Console(499.99) = 3
		// expensive (>= 500): Laptop Pro(1299.99), Smartphone X(899.99) = 2
		expectedCounts := map[string]int{
			"cheap":     3,
			"medium":    3,
			"expensive": 2,
		}

		t.Logf("Price ranges: %d", len(priceResult.NumericRanges))

		for _, nr := range priceResult.NumericRanges {
			expected, exists := expectedCounts[nr.Name]
			if exists && nr.Count != expected {
				t.Errorf("Price range %s: expected %d, got %d", nr.Name, expected, nr.Count)
			} else {
				t.Logf("Price range %s: count=%d ✓", nr.Name, nr.Count)
			}
		}
	})
}

// TestFilteredAggregationAccuracy 测试带过滤条件的聚合准确性
func TestFilteredAggregationAccuracy(t *testing.T) {
	idx, cleanup := setupAggregationTestIndex(t)
	defer cleanup()

	t.Run("terms aggregation with query filter", func(t *testing.T) {
		// 只查询 electronics 类别的产品
		termQuery := query.NewTermQuery("electronics")
		termQuery.SetField("category")

		searchReq := bleve.NewSearchRequest(termQuery)
		searchReq.Size = 0

		brandFacet := bleve.NewFacetRequest("brand", 10)
		searchReq.AddFacet("brands", brandFacet)

		result, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		// 验证总数（应该是4个电子产品）
		if result.Total != 4 {
			t.Errorf("Expected 4 electronics products, got %d", result.Total)
		} else {
			t.Logf("Total electronics products: %d ✓", result.Total)
		}

		brandResult, ok := result.Facets["brands"]
		if !ok {
			t.Fatal("No brands facet in result")
		}

		// 预期结果（仅 electronics）：TechCorp=2, AudioMax=1, GameTech=1
		expectedCounts := map[string]int{
			"TechCorp": 2,
			"AudioMax": 1,
			"GameTech": 1,
		}

		for _, term := range brandResult.Terms.Terms() {
			expected, exists := expectedCounts[term.Term]
			if exists && term.Count != expected {
				t.Errorf("Brand %s: expected %d, got %d", term.Term, expected, term.Count)
			} else {
				t.Logf("Brand %s (electronics): count=%d ✓", term.Term, term.Count)
			}
		}
	})

	t.Run("range query with terms aggregation", func(t *testing.T) {
		// 只查询价格 >= 100 的产品
		min := float64(100)
		rangeQuery := query.NewNumericRangeQuery(&min, nil)
		rangeQuery.SetField("price")

		searchReq := bleve.NewSearchRequest(rangeQuery)
		searchReq.Size = 0

		categoryFacet := bleve.NewFacetRequest("category", 10)
		searchReq.AddFacet("categories", categoryFacet)

		result, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		// 预期总数：5 (Laptop, Smartphone, Headphones, Shoes, Console)
		if result.Total != 5 {
			t.Errorf("Expected 5 products with price >= 100, got %d", result.Total)
		} else {
			t.Logf("Products with price >= 100: %d ✓", result.Total)
		}

		categoryResult, ok := result.Facets["categories"]
		if !ok {
			t.Fatal("No categories facet in result")
		}

		// 预期结果（价格 >= 100）：electronics=4, sports=1
		expectedCounts := map[string]int{
			"electronics": 4,
			"sports":      1,
		}

		for _, term := range categoryResult.Terms.Terms() {
			expected, exists := expectedCounts[term.Term]
			if exists && term.Count != expected {
				t.Errorf("Category %s: expected %d, got %d", term.Term, expected, term.Count)
			} else {
				t.Logf("Category %s (price >= 100): count=%d ✓", term.Term, term.Count)
			}
		}
	})
}

// TestDocCountAccuracy 测试文档计数准确性
func TestDocCountAccuracy(t *testing.T) {
	idx, cleanup := setupAggregationTestIndex(t)
	defer cleanup()

	t.Run("total document count", func(t *testing.T) {
		count, err := idx.DocCount()
		if err != nil {
			t.Fatalf("Failed to get doc count: %v", err)
		}

		if count != 8 {
			t.Errorf("Expected 8 documents, got %d", count)
		} else {
			t.Logf("Total documents: %d ✓", count)
		}
	})

	t.Run("search total matches count", func(t *testing.T) {
		searchReq := bleve.NewSearchRequest(query.NewMatchAllQuery())
		searchReq.Size = 0

		result, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		if result.Total != 8 {
			t.Errorf("Expected 8 total matches, got %d", result.Total)
		} else {
			t.Logf("Total matches: %d ✓", result.Total)
		}
	})

	t.Run("filtered count accuracy", func(t *testing.T) {
		// 测试各种过滤条件的计数准确性
		tests := []struct {
			name     string
			query    query.Query
			expected uint64
		}{
			{
				name: "electronics category",
				query: func() query.Query {
					q := query.NewTermQuery("electronics")
					q.SetField("category")
					return q
				}(),
				expected: 4,
			},
			{
				name: "sports category",
				query: func() query.Query {
					q := query.NewTermQuery("sports")
					q.SetField("category")
					return q
				}(),
				expected: 2,
			},
			{
				name: "home category",
				query: func() query.Query {
					q := query.NewTermQuery("home")
					q.SetField("category")
					return q
				}(),
				expected: 2,
			},
			{
				name: "TechCorp brand",
				query: func() query.Query {
					q := query.NewTermQuery("TechCorp")
					q.SetField("brand")
					return q
				}(),
				expected: 2,
			},
		}

		for _, tt := range tests {
			searchReq := bleve.NewSearchRequest(tt.query)
			searchReq.Size = 0

			result, err := idx.Search(searchReq)
			if err != nil {
				t.Fatalf("Search failed for %s: %v", tt.name, err)
			}

			if result.Total != tt.expected {
				t.Errorf("%s: expected %d, got %d", tt.name, tt.expected, result.Total)
			} else {
				t.Logf("%s: count=%d ✓", tt.name, result.Total)
			}
		}
	})
}

// TestSortAccuracy 测试排序准确性
func TestSortAccuracy(t *testing.T) {
	idx, cleanup := setupAggregationTestIndex(t)
	defer cleanup()

	t.Run("sort by price ascending", func(t *testing.T) {
		searchReq := bleve.NewSearchRequest(query.NewMatchAllQuery())
		searchReq.Size = 8
		searchReq.SortBy([]string{"price"})
		searchReq.Fields = []string{"name", "price"}

		result, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		// 预期顺序（按价格升序）：
		// Yoga Mat(49.99), Blender(59.99), Coffee Maker(79.99), Running Shoes(129.99),
		// Wireless Headphones(199.99), Gaming Console(499.99), Smartphone X(899.99), Laptop Pro(1299.99)
		expectedOrder := []string{"prod5", "prod7", "prod6", "prod4", "prod3", "prod8", "prod2", "prod1"}

		if len(result.Hits) != len(expectedOrder) {
			t.Errorf("Expected %d hits, got %d", len(expectedOrder), len(result.Hits))
			return
		}

		for i, hit := range result.Hits {
			if hit.ID != expectedOrder[i] {
				t.Errorf("Position %d: expected %s, got %s", i, expectedOrder[i], hit.ID)
			} else {
				t.Logf("Position %d: %s ✓", i, hit.ID)
			}
		}
	})

	t.Run("sort by price descending", func(t *testing.T) {
		searchReq := bleve.NewSearchRequest(query.NewMatchAllQuery())
		searchReq.Size = 8
		searchReq.SortBy([]string{"-price"})

		result, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		// 预期顺序（按价格降序）
		expectedOrder := []string{"prod1", "prod2", "prod8", "prod3", "prod4", "prod6", "prod7", "prod5"}

		if len(result.Hits) != len(expectedOrder) {
			t.Errorf("Expected %d hits, got %d", len(expectedOrder), len(result.Hits))
			return
		}

		for i, hit := range result.Hits {
			if hit.ID != expectedOrder[i] {
				t.Errorf("Position %d: expected %s, got %s", i, expectedOrder[i], hit.ID)
			} else {
				t.Logf("Position %d: %s ✓", i, hit.ID)
			}
		}
	})
}

// TestPaginationAccuracy 测试分页准确性
func TestPaginationAccuracy(t *testing.T) {
	idx, cleanup := setupAggregationTestIndex(t)
	defer cleanup()

	t.Run("pagination with from and size", func(t *testing.T) {
		// 按价格排序，获取第3-5条记录
		searchReq := bleve.NewSearchRequest(query.NewMatchAllQuery())
		searchReq.SortBy([]string{"price"})
		searchReq.From = 2
		searchReq.Size = 3

		result, err := idx.Search(searchReq)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		// 完整顺序: prod5, prod7, prod6, prod4, prod3, prod8, prod2, prod1
		// From=2, Size=3 应该返回: prod6, prod4, prod3
		expectedIDs := []string{"prod6", "prod4", "prod3"}

		if len(result.Hits) != len(expectedIDs) {
			t.Errorf("Expected %d hits, got %d", len(expectedIDs), len(result.Hits))
			return
		}

		for i, hit := range result.Hits {
			if hit.ID != expectedIDs[i] {
				t.Errorf("Position %d: expected %s, got %s", i, expectedIDs[i], hit.ID)
			} else {
				t.Logf("Position %d: %s ✓", i, hit.ID)
			}
		}

		// 验证总数仍然是8
		if result.Total != 8 {
			t.Errorf("Expected total 8, got %d", result.Total)
		}
	})
}

// floatPtr 返回 float64 指针
func floatPtr(f float64) *float64 {
	return &f
}
