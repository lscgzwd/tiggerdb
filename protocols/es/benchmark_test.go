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

package es

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/lscgzwd/tiggerdb/directory"
	"github.com/lscgzwd/tiggerdb/metadata"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/server"
)

// BenchmarkSearch_SimpleQuery 基准测试：简单查询性能
func BenchmarkSearch_SimpleQuery(b *testing.B) {
	_, baseURL, cleanup := setupTestServerForBenchmark(b)
	defer cleanup()

	indexName := "bench_search_index"
	setupBenchmarkIndex(baseURL, indexName, 10000, b) // 1万文档

	searchBody := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"title": "test",
			},
		},
		"size": 10,
	}
	bodyBytes, _ := json.Marshal(searchBody)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		resp, err := http.Post(baseURL+"/"+indexName+"/_search", "application/json", bytes.NewReader(bodyBytes))
		if err != nil {
			b.Fatalf("Search failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("Search returned status %d", resp.StatusCode)
		}
	}
}

// BenchmarkSearch_ComplexQuery 基准测试：复杂查询性能
func BenchmarkSearch_ComplexQuery(b *testing.B) {
	_, baseURL, cleanup := setupTestServerForBenchmark(b)
	defer cleanup()

	indexName := "bench_complex_index"
	setupBenchmarkIndex(baseURL, indexName, 10000, b)

	searchBody := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"match": map[string]interface{}{
							"title": "test",
						},
					},
					map[string]interface{}{
						"range": map[string]interface{}{
							"price": map[string]interface{}{
								"gte": 10,
								"lte": 1000,
							},
						},
					},
				},
			},
		},
		"size": 10,
	}
	bodyBytes, _ := json.Marshal(searchBody)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		resp, err := http.Post(baseURL+"/"+indexName+"/_search", "application/json", bytes.NewReader(bodyBytes))
		if err != nil {
			b.Fatalf("Search failed: %v", err)
		}
		resp.Body.Close()
	}
}

// BenchmarkSearch_WithAggregations 基准测试：带聚合的查询性能
func BenchmarkSearch_WithAggregations(b *testing.B) {
	_, baseURL, cleanup := setupTestServerForBenchmark(b)
	defer cleanup()

	indexName := "bench_agg_index"
	setupBenchmarkIndex(baseURL, indexName, 10000, b)

	searchBody := map[string]interface{}{
		"size": 0,
		"aggs": map[string]interface{}{
			"categories": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "category",
					"size":  10,
				},
			},
			"avg_price": map[string]interface{}{
				"avg": map[string]interface{}{
					"field": "price",
				},
			},
		},
	}
	bodyBytes, _ := json.Marshal(searchBody)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		resp, err := http.Post(baseURL+"/"+indexName+"/_search", "application/json", bytes.NewReader(bodyBytes))
		if err != nil {
			b.Fatalf("Search failed: %v", err)
		}
		resp.Body.Close()
	}
}

// BenchmarkBulk_IndexDocuments 基准测试：批量索引性能
func BenchmarkBulk_IndexDocuments(b *testing.B) {
	_, baseURL, cleanup := setupTestServerForBenchmark(b)
	defer cleanup()

	indexName := "bench_bulk_index"

	// 创建索引
	reqBody := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"title": map[string]interface{}{
					"type": "text",
				},
				"price": map[string]interface{}{
					"type": "double",
				},
			},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)
	http.Post(baseURL+"/"+indexName, "application/json", bytes.NewReader(bodyBytes))

	// 准备批量数据（每批100条）
	batchSize := 100
	bulkData := ""
	for i := 0; i < batchSize; i++ {
		bulkData += fmt.Sprintf(`{"index":{"_index":"%s","_id":"%d"}}
{"title":"test document %d","price":%d}
`, indexName, i, i, i*10)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		resp, err := http.Post(baseURL+"/_bulk", "application/x-ndjson", bytes.NewReader([]byte(bulkData)))
		if err != nil {
			b.Fatalf("Bulk failed: %v", err)
		}
		resp.Body.Close()
	}
}

// BenchmarkCount_SimpleQuery 基准测试：Count查询性能
func BenchmarkCount_SimpleQuery(b *testing.B) {
	_, baseURL, cleanup := setupTestServerForBenchmark(b)
	defer cleanup()

	indexName := "bench_count_index"
	setupBenchmarkIndex(baseURL, indexName, 10000, b)

	queryBody := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	bodyBytes, _ := json.Marshal(queryBody)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		resp, err := http.Post(baseURL+"/"+indexName+"/_count", "application/json", bytes.NewReader(bodyBytes))
		if err != nil {
			b.Fatalf("Count failed: %v", err)
		}
		resp.Body.Close()
	}
}

// setupBenchmarkIndex 设置基准测试索引和数据
func setupBenchmarkIndex(baseURL, indexName string, docCount int, b *testing.B) {
	// 创建索引
	reqBody := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"title": map[string]interface{}{
					"type": "text",
				},
				"category": map[string]interface{}{
					"type": "keyword",
				},
				"price": map[string]interface{}{
					"type": "double",
				},
			},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)
	resp, err := http.Post(baseURL+"/"+indexName, "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}
	resp.Body.Close()

	// 批量索引文档
	batchSize := 1000
	for batch := 0; batch < docCount/batchSize; batch++ {
		bulkData := ""
		for i := 0; i < batchSize; i++ {
			docID := batch*batchSize + i
			category := "category_" + fmt.Sprintf("%d", docID%10)
			bulkData += fmt.Sprintf(`{"index":{"_index":"%s","_id":"%d"}}
{"title":"test document %d","category":"%s","price":%d}
`, indexName, docID, docID, category, docID*10)
		}
		resp, err := http.Post(baseURL+"/_bulk", "application/x-ndjson", bytes.NewReader([]byte(bulkData)))
		if err != nil {
			b.Fatalf("Failed to bulk index: %v", err)
		}
		resp.Body.Close()
	}

	// 等待索引完成
	time.Sleep(500 * time.Millisecond)
}

// setupTestServerForBenchmark 为基准测试创建测试服务器
func setupTestServerForBenchmark(b *testing.B) (*ESServer, string, func()) {
	tempDir, err := os.MkdirTemp("", "tigerdb_bench_*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}

	dirConfig := directory.DefaultDirectoryConfig(tempDir)
	dirMgr, err := directory.NewDirectoryManager(dirConfig)
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create directory manager: %v", err)
	}

	metaConfig := &metadata.MetadataStoreConfig{
		StorageType:      "file",
		FilePath:         tempDir,
		EnableCache:      true,
		EnableVersioning: true,
	}
	metaStore, err := metadata.NewFileMetadataStore(metaConfig)
	if err != nil {
		dirMgr.Cleanup()
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create metadata store: %v", err)
	}

	config := DefaultConfig()
	config.ServerConfig = server.DefaultServerConfig()
	config.ServerConfig.Port = 0
	config.ServerConfig.LogLevel = "error"

	esSrv, err := NewServer(dirMgr, metaStore, config)
	if err != nil {
		metaStore.Close()
		dirMgr.Cleanup()
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create ES server: %v", err)
	}

	go func() {
		esSrv.Start()
	}()

	time.Sleep(100 * time.Millisecond)

	baseURL := "http://" + esSrv.Address()

	cleanup := func() {
		esSrv.Stop()
		metaStore.Close()
		dirMgr.Cleanup()
		os.RemoveAll(tempDir)
	}

	return esSrv, baseURL, cleanup
}
