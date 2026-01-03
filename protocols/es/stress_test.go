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

//go:build stress
// +build stress

package es

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStress_ConcurrentSearches 压力测试：并发搜索
func TestStress_ConcurrentSearches(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	_, baseURL, cleanup := setupTestServer(t)
	defer cleanup()

	indexName := "stress_search_index"
	setupStressIndex(baseURL, indexName, 100000, t) // 10万文档

	concurrency := 50
	duration := 30 * time.Second
	searchBody := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"title": "test",
			},
		},
		"size": 10,
	}
	bodyBytes, _ := json.Marshal(searchBody)

	var successCount int64
	var errorCount int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 启动并发goroutine
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					resp, err := http.Post(ts.BaseURL+"/"+indexName+"/_search", "application/json", bytes.NewReader(bodyBytes))
					if err != nil {
						atomic.AddInt64(&errorCount, 1)
						continue
					}
					resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						atomic.AddInt64(&successCount, 1)
					} else {
						atomic.AddInt64(&errorCount, 1)
					}
				}
			}
		}()
	}

	// 运行指定时间
	time.Sleep(duration)
	close(stop)
	wg.Wait()

	t.Logf("Stress test results:")
	t.Logf("  Duration: %v", duration)
	t.Logf("  Concurrency: %d", concurrency)
	t.Logf("  Success: %d", successCount)
	t.Logf("  Errors: %d", errorCount)
	t.Logf("  QPS: %.2f", float64(successCount)/duration.Seconds())

	if errorCount > successCount/10 {
		t.Errorf("Too many errors: %d errors vs %d successes", errorCount, successCount)
	}
}

// TestStress_ConcurrentBulk 压力测试：并发批量操作
func TestStress_ConcurrentBulk(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	_, baseURL, cleanup := setupTestServer(t)
	defer cleanup()

	indexName := "stress_bulk_index"

	// 创建索引
	reqBody := map[string]interface{}{}
	bodyBytes, _ := json.Marshal(reqBody)
	http.Post(baseURL+"/"+indexName, "application/json", bytes.NewReader(bodyBytes))

	concurrency := 20
	operationsPerGoroutine := 100
	batchSize := 100

	var successCount int64
	var errorCount int64
	var wg sync.WaitGroup

	// 启动并发goroutine
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for op := 0; op < operationsPerGoroutine; op++ {
				bulkData := ""
				for j := 0; j < batchSize; j++ {
					docID := goroutineID*operationsPerGoroutine*batchSize + op*batchSize + j
					bulkData += fmt.Sprintf(`{"index":{"_index":"%s","_id":"%d"}}
{"title":"stress test doc %d","value":%d}
`, indexName, docID, docID, docID)
				}

				resp, err := http.Post(baseURL+"/_bulk", "application/x-ndjson", bytes.NewReader([]byte(bulkData)))
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					continue
				}
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					atomic.AddInt64(&successCount, 1)
				} else {
					atomic.AddInt64(&errorCount, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Bulk stress test results:")
	t.Logf("  Concurrency: %d", concurrency)
	t.Logf("  Operations per goroutine: %d", operationsPerGoroutine)
	t.Logf("  Batch size: %d", batchSize)
	t.Logf("  Total documents: %d", concurrency*operationsPerGoroutine*batchSize)
	t.Logf("  Success: %d", successCount)
	t.Logf("  Errors: %d", errorCount)

	if errorCount > successCount/10 {
		t.Errorf("Too many errors: %d errors vs %d successes", errorCount, successCount)
	}
}

// TestStress_MixedOperations 压力测试：混合操作（搜索+索引+更新）
func TestStress_MixedOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	_, baseURL, cleanup := setupTestServer(t)
	defer cleanup()

	indexName := "stress_mixed_index"
	setupStressIndex(baseURL, indexName, 50000, t) // 5万文档

	concurrency := 30
	duration := 20 * time.Second

	var searchCount int64
	var indexCount int64
	var updateCount int64
	var errorCount int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 搜索goroutine
	for i := 0; i < concurrency/3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			searchBody := map[string]interface{}{
				"query": map[string]interface{}{
					"match": map[string]interface{}{
						"title": "test",
					},
				},
			}
			bodyBytes, _ := json.Marshal(searchBody)
			for {
				select {
				case <-stop:
					return
				default:
					resp, err := http.Post(ts.BaseURL+"/"+indexName+"/_search", "application/json", bytes.NewReader(bodyBytes))
					if err != nil {
						atomic.AddInt64(&errorCount, 1)
						continue
					}
					resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						atomic.AddInt64(&searchCount, 1)
					} else {
						atomic.AddInt64(&errorCount, 1)
					}
				}
			}
		}()
	}

	// 索引goroutine
	for i := 0; i < concurrency/3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			docID := 1000000 + id // 从大ID开始避免冲突
			for {
				select {
				case <-stop:
					return
				default:
					docBody := map[string]interface{}{
						"title": fmt.Sprintf("stress doc %d", docID),
						"value": docID,
					}
					bodyBytes, _ := json.Marshal(docBody)
					resp, err := http.Post(fmt.Sprintf("%s/%s/_doc/%d", baseURL, indexName, docID), "application/json", bytes.NewReader(bodyBytes))
					if err != nil {
						atomic.AddInt64(&errorCount, 1)
						continue
					}
					resp.Body.Close()
					if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
						atomic.AddInt64(&indexCount, 1)
						docID++
					} else {
						atomic.AddInt64(&errorCount, 1)
					}
				}
			}
		}(i)
	}

	// 更新goroutine
	for i := 0; i < concurrency/3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			updateBody := map[string]interface{}{
				"doc": map[string]interface{}{
					"value": time.Now().Unix(),
				},
			}
			bodyBytes, _ := json.Marshal(updateBody)
			for {
				select {
				case <-stop:
					return
				default:
					docID := fmt.Sprintf("%d", time.Now().UnixNano()%50000)
					req, _ := http.NewRequest("POST", fmt.Sprintf("%s/%s/_update/%s", baseURL, indexName, docID), bytes.NewReader(bodyBytes))
					req.Header.Set("Content-Type", "application/json")
					resp, err := http.DefaultClient.Do(req)
					if err != nil {
						atomic.AddInt64(&errorCount, 1)
						continue
					}
					resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						atomic.AddInt64(&updateCount, 1)
					} else {
						atomic.AddInt64(&errorCount, 1)
					}
				}
			}
		}()
	}

	// 运行指定时间
	time.Sleep(duration)
	close(stop)
	wg.Wait()

	t.Logf("Mixed operations stress test results:")
	t.Logf("  Duration: %v", duration)
	t.Logf("  Concurrency: %d", concurrency)
	t.Logf("  Searches: %d", searchCount)
	t.Logf("  Indexes: %d", indexCount)
	t.Logf("  Updates: %d", updateCount)
	t.Logf("  Errors: %d", errorCount)
	t.Logf("  Total QPS: %.2f", float64(searchCount+indexCount+updateCount)/duration.Seconds())

	if errorCount > (searchCount+indexCount+updateCount)/10 {
		t.Errorf("Too many errors: %d errors vs %d total operations", errorCount, searchCount+indexCount+updateCount)
	}
}

// TestStress_LargeDataset 压力测试：大数据集查询
func TestStress_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	_, baseURL, cleanup := setupTestServer(t)
	defer cleanup()

	indexName := "stress_large_index"
	docCount := 300000 // 30万文档
	setupStressIndex(baseURL, indexName, docCount, t)

	// 测试各种查询
	queries := []map[string]interface{}{
		{
			"query": map[string]interface{}{
				"match_all": map[string]interface{}{},
			},
			"size": 10,
		},
		{
			"query": map[string]interface{}{
				"match": map[string]interface{}{
					"title": "test",
				},
			},
			"size": 10,
		},
		{
			"query": map[string]interface{}{
				"range": map[string]interface{}{
					"value": map[string]interface{}{
						"gte": 1000,
						"lte": 10000,
					},
				},
			},
			"size": 10,
		},
	}

	for i, query := range queries {
		start := time.Now()
		bodyBytes, _ := json.Marshal(query)
		resp, err := http.Post(baseURL+"/"+indexName+"/_search", "application/json", bytes.NewReader(bodyBytes))
		if err != nil {
			t.Fatalf("Query %d failed: %v", i, err)
		}
		resp.Body.Close()
		duration := time.Since(start)

		t.Logf("Query %d: %v (target: <100ms)", i, duration)
		if duration > 200*time.Millisecond {
			t.Errorf("Query %d took too long: %v (target: <100ms)", i, duration)
		}
	}
}

// setupStressIndex 设置压力测试索引和数据
func setupStressIndex(baseURL, indexName string, docCount int, t *testing.T) {
	// 创建索引
	reqBody := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"title": map[string]interface{}{
					"type": "text",
				},
				"value": map[string]interface{}{
					"type": "long",
				},
			},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)
	resp, err := http.Post(baseURL+"/"+indexName, "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	resp.Body.Close()

	// 批量索引文档
	batchSize := 5000
	totalBatches := (docCount + batchSize - 1) / batchSize
	t.Logf("Indexing %d documents in %d batches...", docCount, totalBatches)

	for batch := 0; batch < totalBatches; batch++ {
		bulkData := ""
		batchStart := batch * batchSize
		batchEnd := batchStart + batchSize
		if batchEnd > docCount {
			batchEnd = docCount
		}

		for i := batchStart; i < batchEnd; i++ {
			bulkData += fmt.Sprintf(`{"index":{"_index":"%s","_id":"%d"}}
{"title":"test document %d","value":%d}
`, indexName, i, i, i)
		}

		resp, err := http.Post(baseURL+"/_bulk", "application/x-ndjson", bytes.NewReader([]byte(bulkData)))
		if err != nil {
			t.Fatalf("Failed to bulk index batch %d: %v", batch, err)
		}
		resp.Body.Close()

		if (batch+1)%10 == 0 {
			t.Logf("  Indexed %d/%d documents...", batchEnd, docCount)
		}
	}

	// 等待索引完成
	t.Logf("Waiting for indexing to complete...")
	time.Sleep(2 * time.Second)
	t.Logf("Index setup complete: %d documents", docCount)
}
