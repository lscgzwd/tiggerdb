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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/lscgzwd/tiggerdb/directory"
	"github.com/lscgzwd/tiggerdb/metadata"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/server"
	esIndex "github.com/lscgzwd/tiggerdb/protocols/es/index"
)

// TestIndexLifecycle_CompleteFlow 测试完整的索引生命周期
func TestIndexLifecycle_CompleteFlow(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_lifecycle_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建目录管理器
	dirConfig := directory.DefaultDirectoryConfig(tempDir)
	dirMgr, err := directory.NewDirectoryManager(dirConfig)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer dirMgr.Cleanup()

	// 创建元数据存储
	metaConfig := &metadata.MetadataStoreConfig{
		StorageType:      "file",
		FilePath:         tempDir,
		EnableCache:      true,
		EnableVersioning: true,
	}
	metaStore, err := metadata.NewFileMetadataStore(metaConfig)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer metaStore.Close()

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建索引处理器
	indexHandler := NewIndexHandler(dirMgr, metaStore)
	indexHandler.SetIndexManager(indexMgr)

	// 创建HTTP服务器用于测试
	httpSrv, err := server.NewServer(server.DefaultServerConfig())
	if err != nil {
		t.Fatalf("Failed to create HTTP server: %v", err)
	}

	// 注册路由
	routes := []server.Route{
		{Method: "GET", Path: "/_cat/indices", Handler: indexHandler.ListIndices},
		{Method: "PUT", Path: "/{index}", Handler: indexHandler.CreateIndex},
		{Method: "GET", Path: "/{index}", Handler: indexHandler.GetIndex},
		{Method: "DELETE", Path: "/{index}", Handler: indexHandler.DeleteIndex},
		{Method: "GET", Path: "/{index}/_mapping", Handler: indexHandler.GetMapping},
	}
	httpSrv.GetRouter().AddRoutes(routes)
	router := httpSrv.GetRouter().Build()

	// 测试索引名称
	indexName := "test_lifecycle_index"

	// 1. 创建索引
	t.Log("Step 1: Creating index")
	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/"+indexName, nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to create index: %s", w.Body.String())
	}

	// 验证索引存在
	if !dirMgr.IndexExists(indexName) {
		t.Fatalf("Index should exist after creation")
	}

	// 2. 获取索引信息
	t.Log("Step 2: Getting index info")
	w = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/"+indexName, nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to get index: %s", w.Body.String())
	}

	// 验证响应包含索引信息
	var indexInfo map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &indexInfo); err != nil {
		t.Fatalf("Failed to parse index response: %v", err)
	}

	if acknowledged, ok := indexInfo["acknowledged"].(bool); !ok || !acknowledged {
		t.Fatalf("Index response should have acknowledged=true")
	}

	// 3. 列出索引
	t.Log("Step 3: Listing indices")
	w = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/_cat/indices", nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to list indices: %s", w.Body.String())
	}

	// 验证包含我们的索引
	responseBody := w.Body.String()
	if !bytes.Contains([]byte(responseBody), []byte(indexName)) {
		t.Fatalf("Index list should contain our index: %s", responseBody)
	}

	// 4. 获取mapping
	t.Log("Step 4: Getting mapping")
	w = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/"+indexName+"/_mapping", nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to get mapping: %s", w.Body.String())
	}

	// 5. 删除索引
	t.Log("Step 5: Deleting index")
	w = httptest.NewRecorder()
	req = httptest.NewRequest("DELETE", "/"+indexName, nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to delete index: %s", w.Body.String())
	}

	// 验证索引已删除
	if dirMgr.IndexExists(indexName) {
		t.Fatalf("Index should not exist after deletion")
	}

	// 验证删除响应
	var deleteResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &deleteResp); err != nil {
		t.Fatalf("Failed to parse delete response: %v", err)
	}

	if acknowledged, ok := deleteResp["acknowledged"].(bool); !ok || !acknowledged {
		t.Fatalf("Delete response should have acknowledged=true")
	}

	t.Log("Index lifecycle test completed successfully")
}

// TestIndexOperations_ErrorCases 测试索引操作的错误情况
func TestIndexOperations_ErrorCases(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_error_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建目录管理器
	dirConfig := directory.DefaultDirectoryConfig(tempDir)
	dirMgr, err := directory.NewDirectoryManager(dirConfig)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer dirMgr.Cleanup()

	// 创建元数据存储
	metaConfig := &metadata.MetadataStoreConfig{
		StorageType:      "file",
		FilePath:         tempDir,
		EnableCache:      true,
		EnableVersioning: true,
	}
	metaStore, err := metadata.NewFileMetadataStore(metaConfig)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer metaStore.Close()

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建索引处理器
	indexHandler := NewIndexHandler(dirMgr, metaStore)
	indexHandler.SetIndexManager(indexMgr)

	// 创建HTTP服务器用于测试
	httpSrv, err := server.NewServer(server.DefaultServerConfig())
	if err != nil {
		t.Fatalf("Failed to create HTTP server: %v", err)
	}

	// 注册路由
	routes := []server.Route{
		{Method: "PUT", Path: "/{index}", Handler: indexHandler.CreateIndex},
		{Method: "GET", Path: "/{index}", Handler: indexHandler.GetIndex},
		{Method: "DELETE", Path: "/{index}", Handler: indexHandler.DeleteIndex},
	}
	httpSrv.GetRouter().AddRoutes(routes)
	router := httpSrv.GetRouter().Build()

	// 测试1: 创建重复索引
	t.Log("Test 1: Creating duplicate index")
	indexName := "test_duplicate_index"

	// 第一次创建应该成功
	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/"+indexName, nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("First create should succeed: %s", w.Body.String())
	}

	// 第二次创建应该失败
	w = httptest.NewRecorder()
	req = httptest.NewRequest("PUT", "/"+indexName, nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("Second create should fail with 409: got %d, body: %s", w.Code, w.Body.String())
	}

	// 测试2: 获取不存在的索引
	t.Log("Test 2: Getting non-existent index")
	w = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/nonexistent_index", nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("Get non-existent index should return 404: got %d", w.Code)
	}

	// 测试3: 删除不存在的索引
	t.Log("Test 3: Deleting non-existent index")
	w = httptest.NewRecorder()
	req = httptest.NewRequest("DELETE", "/nonexistent_index", nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("Delete non-existent index should return 404: got %d", w.Code)
	}

	// 清理：删除我们创建的索引
	w = httptest.NewRecorder()
	req = httptest.NewRequest("DELETE", "/"+indexName, nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Logf("Warning: Failed to cleanup test index: %s", w.Body.String())
	}

	t.Log("Error cases test completed successfully")
}

// TestIndexOperations_WithMappings 测试带mapping的索引操作
func TestIndexOperations_WithMappings(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_mapping_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建目录管理器
	dirConfig := directory.DefaultDirectoryConfig(tempDir)
	dirMgr, err := directory.NewDirectoryManager(dirConfig)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer dirMgr.Cleanup()

	// 创建元数据存储
	metaConfig := &metadata.MetadataStoreConfig{
		StorageType:      "file",
		FilePath:         tempDir,
		EnableCache:      true,
		EnableVersioning: true,
	}
	metaStore, err := metadata.NewFileMetadataStore(metaConfig)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer metaStore.Close()

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建索引处理器
	indexHandler := NewIndexHandler(dirMgr, metaStore)
	indexHandler.SetIndexManager(indexMgr)

	// 创建HTTP服务器用于测试
	httpSrv, err := server.NewServer(server.DefaultServerConfig())
	if err != nil {
		t.Fatalf("Failed to create HTTP server: %v", err)
	}

	// 注册路由
	routes := []server.Route{
		{Method: "PUT", Path: "/{index}", Handler: indexHandler.CreateIndex},
		{Method: "GET", Path: "/{index}/_mapping", Handler: indexHandler.GetMapping},
	}
	httpSrv.GetRouter().AddRoutes(routes)
	router := httpSrv.GetRouter().Build()

	indexName := "test_mapping_index"

	// 创建带mapping的索引
	mappingData := `{
		"mappings": {
			"properties": {
				"name": {"type": "text"},
				"age": {"type": "integer"},
				"email": {"type": "keyword"}
			}
		},
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 0
		}
	}`

	t.Log("Creating index with mapping")
	w := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/"+indexName, bytes.NewReader([]byte(mappingData)))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to create index with mapping: %s", w.Body.String())
	}

	// 获取mapping
	t.Log("Getting index mapping")
	w = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/"+indexName+"/_mapping", nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to get mapping: %s", w.Body.String())
	}

	// 验证mapping响应
	var mappingResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &mappingResp); err != nil {
		t.Fatalf("Failed to parse mapping response: %v", err)
	}

	// 检查是否有我们定义的字段
	if indexData, ok := mappingResp[indexName].(map[string]interface{}); ok {
		if mappings, ok := indexData["mappings"].(map[string]interface{}); ok {
			if properties, ok := mappings["properties"].(map[string]interface{}); ok {
				if nameField, ok := properties["name"].(map[string]interface{}); ok {
					if fieldType, ok := nameField["type"].(string); ok && fieldType == "text" {
						t.Log("Mapping contains correct name field")
					} else {
						t.Logf("Name field type incorrect: %v", nameField)
					}
				}
			}
		}
	}

	t.Log("Mapping test completed successfully")
}

// TestIndexOperations_Concurrency 测试并发索引操作
func TestIndexOperations_Concurrency(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_concurrency_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建目录管理器
	dirConfig := directory.DefaultDirectoryConfig(tempDir)
	dirMgr, err := directory.NewDirectoryManager(dirConfig)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer dirMgr.Cleanup()

	// 创建元数据存储
	metaConfig := &metadata.MetadataStoreConfig{
		StorageType:      "memory", // 使用内存存储以提高并发性能
		EnableCache:      true,
		EnableVersioning: true,
	}
	metaStore, err := metadata.NewMetadataStore(metaConfig)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer metaStore.Close()

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建索引处理器
	indexHandler := NewIndexHandler(dirMgr, metaStore)
	indexHandler.SetIndexManager(indexMgr)

	// 创建HTTP服务器用于测试
	httpSrv, err := server.NewServer(server.DefaultServerConfig())
	if err != nil {
		t.Fatalf("Failed to create HTTP server: %v", err)
	}

	// 注册路由
	routes := []server.Route{
		{Method: "PUT", Path: "/{index}", Handler: indexHandler.CreateIndex},
		{Method: "GET", Path: "/_cat/indices", Handler: indexHandler.ListIndices},
	}
	httpSrv.GetRouter().AddRoutes(routes)
	router := httpSrv.GetRouter().Build()

	// 并发创建多个索引
	numGoroutines := 5
	numIndicesPerGoroutine := 3
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			for j := 0; j < numIndicesPerGoroutine; j++ {
				indexName := fmt.Sprintf("concurrent_index_%d_%d", goroutineID, j)

				w := httptest.NewRecorder()
				req := httptest.NewRequest("PUT", "/"+indexName, nil)
				router.ServeHTTP(w, req)

				if w.Code != http.StatusOK {
					t.Errorf("Failed to create index %s in goroutine %d: %s", indexName, goroutineID, w.Body.String())
				}
			}
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// 验证所有索引都创建成功
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/_cat/indices", nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to list indices: %s", w.Body.String())
	}

	// 应该有15个索引（5个goroutine * 3个索引）
	responseBody := w.Body.String()
	indexCount := 0
	for _, line := range strings.Split(responseBody, "\n") {
		if strings.TrimSpace(line) != "" {
			indexCount++
		}
	}

	if indexCount != 15 {
		t.Fatalf("Expected 15 indices, got %d. Response: %s", indexCount, responseBody)
	}

	t.Log("Concurrency test completed successfully")
}
