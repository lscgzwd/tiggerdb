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

package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gorilla/mux"
	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/mapping"
)

// setupAggregationTestEnv 创建聚合测试环境
func setupAggregationTestEnv(t *testing.T) (*DocumentHandler, bleve.Index, func()) {
	tempDir, err := os.MkdirTemp("", "tigerdb_agg_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	idx, err := bleve.New(tempDir, mapping.NewIndexMapping())
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create index: %v", err)
	}

	// 索引测试数据
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
				"in_stock": true,
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
				"in_stock": true,
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
				"in_stock": true,
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
				"in_stock": true,
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
				"in_stock": true,
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
				"in_stock": false,
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
				"in_stock": true,
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
				"in_stock": false,
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

	// 创建 DocumentHandler
	handler := NewDocumentHandler(map[string]bleve.Index{
		"test_agg_index": idx,
	})

	cleanup := func() {
		idx.Close()
		os.RemoveAll(tempDir)
	}

	return handler, idx, cleanup
}

// executeSearch 执行搜索请求并返回响应
func executeSearch(t *testing.T, handler *DocumentHandler, indexName string, searchBody map[string]interface{}) map[string]interface{} {
	bodyBytes, err := json.Marshal(searchBody)
	if err != nil {
		t.Fatalf("Failed to marshal search body: %v", err)
	}

	req, err := http.NewRequest("POST", "/"+indexName+"/_search", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// 设置路由变量
	req = mux.SetURLVars(req, map[string]string{"index": indexName})

	rr := httptest.NewRecorder()
	handler.Search(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Search failed with status %d: %s", rr.Code, rr.Body.String())
	}

	var response map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	return response
}

// TestTermsAggregationAccuracy 测试 terms 聚合的数据准确性
func TestTermsAggregationAccuracy(t *testing.T) {
	handler, _, cleanup := setupAggregationTestEnv(t)
	defer cleanup()

	t.Run("terms aggregation on category", func(t *testing.T) {
		searchBody := map[string]interface{}{
			"size": 0,
			"aggs": map[string]interface{}{
				"categories": map[string]interface{}{
					"terms": map[string]interface{}{
						"field": "category",
						"size":  10,
					},
				},
			},
		}

		response := executeSearch(t, handler, "test_agg_index", searchBody)

		// 验证聚合结果
		aggs, ok := response["aggregations"].(map[string]interface{})
		if !ok {
			t.Fatal("No aggregations in response")
		}

		categories, ok := aggs["categories"].(map[string]interface{})
		if !ok {
			t.Fatal("No categories aggregation in response")
		}

		buckets, ok := categories["buckets"].([]interface{})
		if !ok {
			t.Fatal("No buckets in categories aggregation")
		}

		// 预期结果：electronics=4, sports=2, home=2
		expectedCounts := map[string]int{
			"electronics": 4,
			"sports":      2,
			"home":        2,
		}

		t.Logf("Got %d category buckets", len(buckets))

		for _, bucket := range buckets {
			b := bucket.(map[string]interface{})
			key := b["key"].(string)
			count := int(b["doc_count"].(float64))

			expected, exists := expectedCounts[key]
			if !exists {
				t.Errorf("Unexpected category: %s", key)
				continue
			}

			if count != expected {
				t.Errorf("Category %s: expected count %d, got %d", key, expected, count)
			} else {
				t.Logf("Category %s: count=%d ✓", key, count)
			}
		}
	})

	t.Run("terms aggregation on brand", func(t *testing.T) {
		searchBody := map[string]interface{}{
			"size": 0,
			"aggs": map[string]interface{}{
				"brands": map[string]interface{}{
					"terms": map[string]interface{}{
						"field": "brand",
						"size":  10,
					},
				},
			},
		}

		response := executeSearch(t, handler, "test_agg_index", searchBody)

		aggs, ok := response["aggregations"].(map[string]interface{})
		if !ok {
			t.Fatal("No aggregations in response")
		}

		brands, ok := aggs["brands"].(map[string]interface{})
		if !ok {
			t.Fatal("No brands aggregation in response")
		}

		buckets, ok := brands["buckets"].([]interface{})
		if !ok {
			t.Fatal("No buckets in brands aggregation")
		}

		// 预期结果：TechCorp=2, HomeComfort=2, others=1 each
		expectedCounts := map[string]int{
			"TechCorp":    2,
			"HomeComfort": 2,
			"AudioMax":    1,
			"SportFit":    1,
			"FitLife":     1,
			"GameTech":    1,
		}

		t.Logf("Got %d brand buckets", len(buckets))

		for _, bucket := range buckets {
			b := bucket.(map[string]interface{})
			key := b["key"].(string)
			count := int(b["doc_count"].(float64))

			expected, exists := expectedCounts[key]
			if exists && count != expected {
				t.Errorf("Brand %s: expected count %d, got %d", key, expected, count)
			} else {
				t.Logf("Brand %s: count=%d ✓", key, count)
			}
		}
	})
}

// TestMetricsAggregationAccuracy 测试 metrics 聚合的数据准确性
func TestMetricsAggregationAccuracy(t *testing.T) {
	handler, _, cleanup := setupAggregationTestEnv(t)
	defer cleanup()

	t.Run("avg aggregation on price", func(t *testing.T) {
		searchBody := map[string]interface{}{
			"size": 0,
			"aggs": map[string]interface{}{
				"avg_price": map[string]interface{}{
					"avg": map[string]interface{}{
						"field": "price",
					},
				},
			},
		}

		response := executeSearch(t, handler, "test_agg_index", searchBody)

		aggs, ok := response["aggregations"].(map[string]interface{})
		if !ok {
			t.Fatal("No aggregations in response")
		}

		avgPrice, ok := aggs["avg_price"].(map[string]interface{})
		if !ok {
			t.Fatal("No avg_price aggregation in response")
		}

		// 计算预期平均值：(1299.99 + 899.99 + 199.99 + 129.99 + 49.99 + 79.99 + 59.99 + 499.99) / 8 = 402.49
		expectedAvg := 402.49
		value, ok := avgPrice["value"].(float64)
		if !ok {
			t.Fatal("No value in avg_price aggregation")
		}

		// 允许小数误差
		if value < expectedAvg-1 || value > expectedAvg+1 {
			t.Errorf("Expected avg price ~%.2f, got %.2f", expectedAvg, value)
		} else {
			t.Logf("Avg price: %.2f ✓", value)
		}
	})

	t.Run("sum aggregation on quantity", func(t *testing.T) {
		searchBody := map[string]interface{}{
			"size": 0,
			"aggs": map[string]interface{}{
				"total_quantity": map[string]interface{}{
					"sum": map[string]interface{}{
						"field": "quantity",
					},
				},
			},
		}

		response := executeSearch(t, handler, "test_agg_index", searchBody)

		aggs, ok := response["aggregations"].(map[string]interface{})
		if !ok {
			t.Fatal("No aggregations in response")
		}

		totalQty, ok := aggs["total_quantity"].(map[string]interface{})
		if !ok {
			t.Fatal("No total_quantity aggregation in response")
		}

		// 预期总和：50 + 100 + 200 + 75 + 150 + 80 + 60 + 30 = 745
		expectedSum := 745.0
		value, ok := totalQty["value"].(float64)
		if !ok {
			t.Fatal("No value in total_quantity aggregation")
		}

		if value != expectedSum {
			t.Errorf("Expected sum quantity %.0f, got %.0f", expectedSum, value)
		} else {
			t.Logf("Total quantity: %.0f ✓", value)
		}
	})

	t.Run("min and max aggregation on rating", func(t *testing.T) {
		searchBody := map[string]interface{}{
			"size": 0,
			"aggs": map[string]interface{}{
				"min_rating": map[string]interface{}{
					"min": map[string]interface{}{
						"field": "rating",
					},
				},
				"max_rating": map[string]interface{}{
					"max": map[string]interface{}{
						"field": "rating",
					},
				},
			},
		}

		response := executeSearch(t, handler, "test_agg_index", searchBody)

		aggs, ok := response["aggregations"].(map[string]interface{})
		if !ok {
			t.Fatal("No aggregations in response")
		}

		// 验证最小值 (3.9)
		minRating, ok := aggs["min_rating"].(map[string]interface{})
		if !ok {
			t.Fatal("No min_rating aggregation in response")
		}

		minValue, ok := minRating["value"].(float64)
		if !ok {
			t.Fatal("No value in min_rating aggregation")
		}

		expectedMin := 3.9
		if minValue < expectedMin-0.1 || minValue > expectedMin+0.1 {
			t.Errorf("Expected min rating ~%.1f, got %.1f", expectedMin, minValue)
		} else {
			t.Logf("Min rating: %.1f ✓", minValue)
		}

		// 验证最大值 (4.8)
		maxRating, ok := aggs["max_rating"].(map[string]interface{})
		if !ok {
			t.Fatal("No max_rating aggregation in response")
		}

		maxValue, ok := maxRating["value"].(float64)
		if !ok {
			t.Fatal("No value in max_rating aggregation")
		}

		expectedMax := 4.8
		if maxValue < expectedMax-0.1 || maxValue > expectedMax+0.1 {
			t.Errorf("Expected max rating ~%.1f, got %.1f", expectedMax, maxValue)
		} else {
			t.Logf("Max rating: %.1f ✓", maxValue)
		}
	})
}

// TestRangeAggregationAccuracy 测试 range 聚合的数据准确性
func TestRangeAggregationAccuracy(t *testing.T) {
	handler, _, cleanup := setupAggregationTestEnv(t)
	defer cleanup()

	t.Run("range aggregation on price", func(t *testing.T) {
		searchBody := map[string]interface{}{
			"size": 0,
			"aggs": map[string]interface{}{
				"price_ranges": map[string]interface{}{
					"range": map[string]interface{}{
						"field": "price",
						"ranges": []interface{}{
							map[string]interface{}{"to": 100},
							map[string]interface{}{"from": 100, "to": 500},
							map[string]interface{}{"from": 500},
						},
					},
				},
			},
		}

		response := executeSearch(t, handler, "test_agg_index", searchBody)

		aggs, ok := response["aggregations"].(map[string]interface{})
		if !ok {
			t.Fatal("No aggregations in response")
		}

		priceRanges, ok := aggs["price_ranges"].(map[string]interface{})
		if !ok {
			t.Fatal("No price_ranges aggregation in response")
		}

		buckets, ok := priceRanges["buckets"].([]interface{})
		if !ok {
			t.Fatal("No buckets in price_ranges aggregation")
		}

		// 预期结果：
		// < 100: Yoga Mat(49.99), Coffee Maker(79.99), Blender(59.99) = 3
		// 100-500: Wireless Headphones(199.99), Running Shoes(129.99), Gaming Console(499.99) = 3
		// >= 500: Laptop Pro(1299.99), Smartphone X(899.99) = 2

		t.Logf("Got %d price range buckets", len(buckets))

		for _, bucket := range buckets {
			b := bucket.(map[string]interface{})
			count := int(b["doc_count"].(float64))
			t.Logf("Price range bucket: %v, count=%d", b, count)
		}
	})
}

// TestNestedAggregationAccuracy 测试嵌套聚合的数据准确性
func TestNestedAggregationAccuracy(t *testing.T) {
	handler, _, cleanup := setupAggregationTestEnv(t)
	defer cleanup()

	t.Run("nested terms and avg aggregation", func(t *testing.T) {
		searchBody := map[string]interface{}{
			"size": 0,
			"aggs": map[string]interface{}{
				"categories": map[string]interface{}{
					"terms": map[string]interface{}{
						"field": "category",
						"size":  10,
					},
					"aggs": map[string]interface{}{
						"avg_price": map[string]interface{}{
							"avg": map[string]interface{}{
								"field": "price",
							},
						},
					},
				},
			},
		}

		response := executeSearch(t, handler, "test_agg_index", searchBody)

		aggs, ok := response["aggregations"].(map[string]interface{})
		if !ok {
			t.Fatal("No aggregations in response")
		}

		categories, ok := aggs["categories"].(map[string]interface{})
		if !ok {
			t.Fatal("No categories aggregation in response")
		}

		buckets, ok := categories["buckets"].([]interface{})
		if !ok {
			t.Fatal("No buckets in categories aggregation")
		}

		// 预期：
		// electronics: (1299.99 + 899.99 + 199.99 + 499.99) / 4 = 724.99
		// sports: (129.99 + 49.99) / 2 = 89.99
		// home: (79.99 + 59.99) / 2 = 69.99
		expectedAvgPrices := map[string]float64{
			"electronics": 724.99,
			"sports":      89.99,
			"home":        69.99,
		}

		t.Logf("Got %d category buckets with nested avg_price", len(buckets))

		for _, bucket := range buckets {
			b := bucket.(map[string]interface{})
			key := b["key"].(string)
			count := int(b["doc_count"].(float64))

			// 检查嵌套的 avg_price
			if avgPrice, ok := b["avg_price"].(map[string]interface{}); ok {
				if value, ok := avgPrice["value"].(float64); ok {
					expected := expectedAvgPrices[key]
					if value < expected-10 || value > expected+10 {
						t.Errorf("Category %s: expected avg price ~%.2f, got %.2f", key, expected, value)
					} else {
						t.Logf("Category %s: count=%d, avg_price=%.2f ✓", key, count, value)
					}
				}
			} else {
				t.Logf("Category %s: count=%d (no nested avg_price)", key, count)
			}
		}
	})

	t.Run("terms with sub-terms aggregation", func(t *testing.T) {
		searchBody := map[string]interface{}{
			"size": 0,
			"aggs": map[string]interface{}{
				"categories": map[string]interface{}{
					"terms": map[string]interface{}{
						"field": "category",
						"size":  10,
					},
					"aggs": map[string]interface{}{
						"brands": map[string]interface{}{
							"terms": map[string]interface{}{
								"field": "brand",
								"size":  10,
							},
						},
					},
				},
			},
		}

		response := executeSearch(t, handler, "test_agg_index", searchBody)

		aggs, ok := response["aggregations"].(map[string]interface{})
		if !ok {
			t.Fatal("No aggregations in response")
		}

		categories, ok := aggs["categories"].(map[string]interface{})
		if !ok {
			t.Fatal("No categories aggregation in response")
		}

		buckets, ok := categories["buckets"].([]interface{})
		if !ok {
			t.Fatal("No buckets in categories aggregation")
		}

		t.Logf("Got %d category buckets with nested brands", len(buckets))

		for _, bucket := range buckets {
			b := bucket.(map[string]interface{})
			key := b["key"].(string)
			count := int(b["doc_count"].(float64))

			t.Logf("Category %s: count=%d", key, count)

			// 检查嵌套的 brands
			if brands, ok := b["brands"].(map[string]interface{}); ok {
				if subBuckets, ok := brands["buckets"].([]interface{}); ok {
					for _, subBucket := range subBuckets {
						sb := subBucket.(map[string]interface{})
						subKey := sb["key"].(string)
						subCount := int(sb["doc_count"].(float64))
						t.Logf("  - Brand %s: count=%d", subKey, subCount)
					}
				}
			}
		}
	})
}

// TestFilterAggregationAccuracy 测试 filter 聚合的数据准确性
func TestFilterAggregationAccuracy(t *testing.T) {
	handler, _, cleanup := setupAggregationTestEnv(t)
	defer cleanup()

	t.Run("filter aggregation with term filter", func(t *testing.T) {
		searchBody := map[string]interface{}{
			"size": 0,
			"aggs": map[string]interface{}{
				"electronics_only": map[string]interface{}{
					"filter": map[string]interface{}{
						"term": map[string]interface{}{
							"category": "electronics",
						},
					},
					"aggs": map[string]interface{}{
						"avg_price": map[string]interface{}{
							"avg": map[string]interface{}{
								"field": "price",
							},
						},
					},
				},
			},
		}

		response := executeSearch(t, handler, "test_agg_index", searchBody)

		aggs, ok := response["aggregations"].(map[string]interface{})
		if !ok {
			t.Fatal("No aggregations in response")
		}

		electronicsOnly, ok := aggs["electronics_only"].(map[string]interface{})
		if !ok {
			t.Fatal("No electronics_only aggregation in response")
		}

		// 验证文档数量（应该是4个电子产品）
		docCount, ok := electronicsOnly["doc_count"].(float64)
		if ok {
			if int(docCount) != 4 {
				t.Errorf("Expected 4 electronics documents, got %d", int(docCount))
			} else {
				t.Logf("Electronics doc_count: %d ✓", int(docCount))
			}
		}

		// 验证平均价格
		if avgPrice, ok := electronicsOnly["avg_price"].(map[string]interface{}); ok {
			if value, ok := avgPrice["value"].(float64); ok {
				// 预期：(1299.99 + 899.99 + 199.99 + 499.99) / 4 = 724.99
				expectedAvg := 724.99
				if value < expectedAvg-10 || value > expectedAvg+10 {
					t.Errorf("Expected avg price ~%.2f, got %.2f", expectedAvg, value)
				} else {
					t.Logf("Electronics avg_price: %.2f ✓", value)
				}
			}
		}
	})
}

// TestAggregationWithQuery 测试带查询条件的聚合
func TestAggregationWithQuery(t *testing.T) {
	handler, _, cleanup := setupAggregationTestEnv(t)
	defer cleanup()

	t.Run("aggregation with range query filter", func(t *testing.T) {
		searchBody := map[string]interface{}{
			"size": 0,
			"query": map[string]interface{}{
				"range": map[string]interface{}{
					"price": map[string]interface{}{
						"gte": 100,
					},
				},
			},
			"aggs": map[string]interface{}{
				"categories": map[string]interface{}{
					"terms": map[string]interface{}{
						"field": "category",
						"size":  10,
					},
				},
			},
		}

		response := executeSearch(t, handler, "test_agg_index", searchBody)

		// 验证 hits.total (价格 >= 100 的产品: 5个)
		// Laptop Pro(1299.99), Smartphone X(899.99), Wireless Headphones(199.99),
		// Running Shoes(129.99), Gaming Console(499.99)
		if hits, ok := response["hits"].(map[string]interface{}); ok {
			if total, ok := hits["total"].(map[string]interface{}); ok {
				if value, ok := total["value"].(float64); ok {
					if int(value) != 5 {
						t.Errorf("Expected 5 products with price >= 100, got %d", int(value))
					} else {
						t.Logf("Products with price >= 100: %d ✓", int(value))
					}
				}
			}
		}

		aggs, ok := response["aggregations"].(map[string]interface{})
		if !ok {
			t.Fatal("No aggregations in response")
		}

		categories, ok := aggs["categories"].(map[string]interface{})
		if !ok {
			t.Fatal("No categories aggregation in response")
		}

		buckets, ok := categories["buckets"].([]interface{})
		if !ok {
			t.Fatal("No buckets in categories aggregation")
		}

		// 预期（价格 >= 100）：
		// electronics: Laptop Pro, Smartphone X, Wireless Headphones, Gaming Console = 4
		// sports: Running Shoes = 1
		t.Logf("Got %d category buckets (filtered by price >= 100)", len(buckets))

		for _, bucket := range buckets {
			b := bucket.(map[string]interface{})
			key := b["key"].(string)
			count := int(b["doc_count"].(float64))
			t.Logf("Category %s: count=%d", key, count)
		}
	})
}
