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

// setupTestServer 创建测试服务器
func setupTestServer(t *testing.T) (*ESServer, string, func()) {
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	dirConfig := directory.DefaultDirectoryConfig(tempDir)
	dirMgr, err := directory.NewDirectoryManager(dirConfig)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create directory manager: %v", err)
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
		t.Fatalf("Failed to create metadata store: %v", err)
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
		t.Fatalf("Failed to create ES server: %v", err)
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

// TestESIntegration_CompleteWorkflow 测试完整的ES工作流程
func TestESIntegration_CompleteWorkflow(t *testing.T) {
	// 创建测试服务器
	_, baseURL, cleanup := setupTestServer(t)
	defer cleanup()

	indexName := "test_integration_index"
	docID := "test_doc_1"

	// 1. 创建索引
	t.Run("CreateIndex", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"settings": map[string]interface{}{
				"number_of_shards": 1,
			},
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
		req, _ := http.NewRequest("PUT", baseURL+"/"+indexName, bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to create index: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}
	})

	// 2. 索引文档
	t.Run("IndexDocument", func(t *testing.T) {
		docBody := map[string]interface{}{
			"title": "Test Product",
			"price": 99.99,
		}
		bodyBytes, _ := json.Marshal(docBody)
		req, _ := http.NewRequest("PUT", baseURL+"/"+indexName+"/_doc/"+docID, bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to index document: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 201 or 200, got %d", resp.StatusCode)
		}
	})

	// 3. 获取文档
	t.Run("GetDocument", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/" + indexName + "/_doc/" + docID)
		if err != nil {
			t.Fatalf("Failed to get document: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if result["_id"] != docID {
			t.Errorf("Expected document ID %s, got %v", docID, result["_id"])
		}
	})

	// 4. 搜索文档
	t.Run("SearchDocument", func(t *testing.T) {
		searchBody := map[string]interface{}{
			"query": map[string]interface{}{
				"match": map[string]interface{}{
					"title": "Test",
				},
			},
		}
		bodyBytes, _ := json.Marshal(searchBody)
		resp, err := http.Post(baseURL+"/"+indexName+"/_search", "application/json", bytes.NewReader(bodyBytes))
		if err != nil {
			t.Fatalf("Failed to search: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		hits, ok := result["hits"].(map[string]interface{})
		if !ok {
			t.Fatal("Invalid hits structure")
		}

		total, ok := hits["total"].(map[string]interface{})
		if !ok {
			t.Fatal("Invalid total structure")
		}

		if total["value"].(float64) < 1 {
			t.Errorf("Expected at least 1 hit, got %v", total["value"])
		}
	})

	// 5. 更新文档
	t.Run("UpdateDocument", func(t *testing.T) {
		updateBody := map[string]interface{}{
			"doc": map[string]interface{}{
				"price": 89.99,
			},
		}
		bodyBytes, _ := json.Marshal(updateBody)
		req, _ := http.NewRequest("POST", baseURL+"/"+indexName+"/_update/"+docID, bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to update document: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}
	})

	// 6. 删除文档
	t.Run("DeleteDocument", func(t *testing.T) {
		req, _ := http.NewRequest("DELETE", baseURL+"/"+indexName+"/_doc/"+docID, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to delete document: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}
	})

	// 7. 删除索引
	t.Run("DeleteIndex", func(t *testing.T) {
		req, _ := http.NewRequest("DELETE", baseURL+"/"+indexName, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to delete index: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}
	})
}

// TestESIntegration_BulkOperations 测试批量操作
func TestESIntegration_BulkOperations(t *testing.T) {
	_, baseURL, cleanup := setupTestServer(t)
	defer cleanup()

	indexName := "test_bulk_index"

	// 创建索引（使用 PUT 方法）
	reqBody := map[string]interface{}{}
	bodyBytes, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("PUT", baseURL+"/"+indexName, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to create index: status %d", resp.StatusCode)
	}
	// 等待索引创建完成
	time.Sleep(100 * time.Millisecond)

	// 准备批量数据
	bulkData := `{"index":{"_index":"` + indexName + `","_id":"1"}}
{"name":"doc1","value":1}
{"index":{"_index":"` + indexName + `","_id":"2"}}
{"name":"doc2","value":2}
{"index":{"_index":"` + indexName + `","_id":"3"}}
{"name":"doc3","value":3}
`

	// 执行批量操作
	resp, err = http.Post(baseURL+"/_bulk", "application/x-ndjson", bytes.NewReader([]byte(bulkData)))
	if err != nil {
		t.Fatalf("Failed to execute bulk: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	// 验证文档数量
	time.Sleep(100 * time.Millisecond) // 等待索引完成
	searchBody := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"size": 0,
	}
	searchBodyBytes, _ := json.Marshal(searchBody)
	resp, err = http.Post(baseURL+"/"+indexName+"/_search", "application/json", bytes.NewReader(searchBodyBytes))
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode search response: %v", err)
	}

	// 检查是否有错误
	if errorInfo, ok := result["error"].(map[string]interface{}); ok {
		t.Fatalf("Search failed with error: %v", errorInfo)
	}

	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		t.Fatalf("Invalid search response format: %v", result)
	}

	total, ok := hits["total"].(map[string]interface{})
	if !ok {
		// 尝试直接获取 total 值（可能是数字格式）
		if totalVal, ok := hits["total"].(float64); ok {
			if totalVal != 3 {
				t.Errorf("Expected 3 documents, got %v", totalVal)
			}
			return
		}
		t.Fatalf("Invalid total format in search response: %v", hits["total"])
	}

	if totalVal, ok := total["value"].(float64); ok {
		if totalVal != 3 {
			t.Errorf("Expected 3 documents, got %v", totalVal)
		}
	} else {
		t.Errorf("Invalid total value format: %v", total["value"])
	}
}

// TestESIntegration_Aggregations 测试聚合功能
func TestESIntegration_Aggregations(t *testing.T) {
	_, baseURL, cleanup := setupTestServer(t)
	defer cleanup()

	indexName := "test_agg_index"

	// 创建索引
	reqBody := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
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
	req, _ := http.NewRequest("PUT", baseURL+"/"+indexName, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	http.DefaultClient.Do(req)
	time.Sleep(100 * time.Millisecond) // 等待索引创建完成

	// 索引测试数据
	docs := []map[string]interface{}{
		{"category": "electronics", "price": 100.0},
		{"category": "electronics", "price": 200.0},
		{"category": "books", "price": 50.0},
	}
	for i, doc := range docs {
		bodyBytes, _ := json.Marshal(doc)
		req, _ := http.NewRequest("PUT", fmt.Sprintf("%s/%s/_doc/%d", baseURL, indexName, i), bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		http.DefaultClient.Do(req)
	}
	time.Sleep(100 * time.Millisecond) // 等待索引完成

	time.Sleep(200 * time.Millisecond) // 等待索引完成

	// 执行聚合查询
	searchBody := map[string]interface{}{
		"size": 0,
		"aggs": map[string]interface{}{
			"categories": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "category",
				},
			},
			"avg_price": map[string]interface{}{
				"avg": map[string]interface{}{
					"field": "price",
				},
			},
		},
	}
	bodyBytes, _ = json.Marshal(searchBody)
	resp, err := http.Post(baseURL+"/"+indexName+"/_search", "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	aggs, ok := result["aggregations"].(map[string]interface{})
	if !ok {
		t.Fatal("Missing aggregations in response")
	}

	if _, ok := aggs["categories"]; !ok {
		t.Error("Missing categories aggregation")
	}

	if _, ok := aggs["avg_price"]; !ok {
		t.Error("Missing avg_price aggregation")
	}
}
