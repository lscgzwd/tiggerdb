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
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/lscgzwd/tiggerdb/directory"
	"github.com/lscgzwd/tiggerdb/metadata"
	esIndex "github.com/lscgzwd/tiggerdb/protocols/es/index"
)

// setupSearchTestEnvironment 创建搜索测试环境
func setupSearchTestEnvironment(t *testing.T) (*DocumentHandler, func(), string) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_search_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// 设置目录配置
	config := &directory.DirectoryConfig{
		BaseDir:           tempDir,
		DirPerm:           0755,
		MaxIndices:        10,
		MaxTables:         100,
		MaxAge:            time.Hour,
		EnableAutoCleanup: false,
	}

	// 创建目录管理器
	dirMgr, err := directory.NewDirectoryManager(config)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create directory manager: %v", err)
	}

	// 创建元数据存储
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

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建文档处理器
	docHandler := NewDocumentHandler(indexMgr, dirMgr, metaStore)

	// 创建测试索引
	indexName := "test_search_index"

	// 创建索引处理器
	indexHandler := NewIndexHandler(dirMgr, metaStore)
	indexHandler.SetIndexManager(indexMgr)

	// 创建索引
	req := httptest.NewRequest("PUT", "/"+indexName, nil)
	req = mux.SetURLVars(req, map[string]string{"index": indexName})
	w := httptest.NewRecorder()
	indexHandler.CreateIndex(w, req)
	if w.Code != http.StatusOK {
		metaStore.Close()
		dirMgr.Cleanup()
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create index: %s", w.Body.String())
	}

	// 索引测试数据
	testData := `{"index":{"_index":"` + indexName + `","_id":"doc1"}}
{"name":"apple","category":"fruit","price":1.5}
{"index":{"_index":"` + indexName + `","_id":"doc2"}}
{"name":"banana","category":"fruit","price":2.0}
{"index":{"_index":"` + indexName + `","_id":"doc3"}}
{"name":"carrot","category":"vegetable","price":1.0}
{"index":{"_index":"` + indexName + `","_id":"doc4"}}
{"name":"laptop","category":"electronics","price":999.99}
{"index":{"_index":"` + indexName + `","_id":"doc5"}}
{"name":"book","category":"books","price":25.0,"nested":{"author":"John Doe","year":2023}}}`

	dataReq := httptest.NewRequest("POST", "/_bulk", strings.NewReader(testData))
	dataReq.Header.Set("Content-Type", "application/x-ndjson")
	dataW := httptest.NewRecorder()
	docHandler.Bulk(dataW, dataReq)

	if dataW.Code != http.StatusOK {
		metaStore.Close()
		dirMgr.Cleanup()
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to index test data: %s", dataW.Body.String())
	}

	// 等待索引刷新
	time.Sleep(100 * time.Millisecond)

	cleanup := func() {
		metaStore.Close()
		dirMgr.Cleanup()
		os.RemoveAll(tempDir)
	}

	return docHandler, cleanup, indexName
}

// TestDocumentHandler_Search_MatchAll 测试match_all查询
func TestDocumentHandler_Search_MatchAll(t *testing.T) {
	docHandler, cleanup, indexName := setupSearchTestEnvironment(t)
	defer cleanup()

	// 执行match_all查询
	searchData := `{"query":{"match_all":{}}}`
	searchReq := httptest.NewRequest("POST", "/"+indexName+"/_search", strings.NewReader(searchData))
	searchReq.Header.Set("Content-Type", "application/json")
	searchReq = mux.SetURLVars(searchReq, map[string]string{"index": indexName})
	searchW := httptest.NewRecorder()
	docHandler.Search(searchW, searchReq)

	if searchW.Code != http.StatusOK {
		t.Fatalf("Search failed: %s", searchW.Body.String())
	}

	// 验证返回了5个文档
	if !strings.Contains(searchW.Body.String(), `"total":{"value":5,"relation":"eq"}`) {
		t.Fatalf("Expected 5 documents, got: %s", searchW.Body.String())
	}

	t.Log("Search match_all test passed")
}

// TestDocumentHandler_Search_Match 测试match查询
func TestDocumentHandler_Search_Match(t *testing.T) {
	docHandler, cleanup, indexName := setupSearchTestEnvironment(t)
	defer cleanup()

	// 执行match查询
	searchData := `{"query":{"match":{"name":"apple"}}}`
	searchReq := httptest.NewRequest("POST", "/"+indexName+"/_search", strings.NewReader(searchData))
	searchReq.Header.Set("Content-Type", "application/json")
	searchReq = mux.SetURLVars(searchReq, map[string]string{"index": indexName})
	searchW := httptest.NewRecorder()
	docHandler.Search(searchW, searchReq)

	if searchW.Code != http.StatusOK {
		t.Fatalf("Search failed: %s", searchW.Body.String())
	}

	// 验证返回了1个文档
	if !strings.Contains(searchW.Body.String(), `"total":{"value":1,"relation":"eq"}`) {
		t.Fatalf("Expected 1 document, got: %s", searchW.Body.String())
	}

	// 验证返回了apple文档
	if !strings.Contains(searchW.Body.String(), `"name":"apple"`) {
		t.Fatalf("Expected apple document, got: %s", searchW.Body.String())
	}

	t.Log("Search match test passed")
}

// TestDocumentHandler_Search_Term 测试term查询
func TestDocumentHandler_Search_Term(t *testing.T) {
	docHandler, cleanup, indexName := setupSearchTestEnvironment(t)
	defer cleanup()

	// 执行term查询
	searchData := `{"query":{"term":{"category":"fruit"}}}`
	searchReq := httptest.NewRequest("POST", "/"+indexName+"/_search", strings.NewReader(searchData))
	searchReq.Header.Set("Content-Type", "application/json")
	searchReq = mux.SetURLVars(searchReq, map[string]string{"index": indexName})
	searchW := httptest.NewRecorder()
	docHandler.Search(searchW, searchReq)

	if searchW.Code != http.StatusOK {
		t.Fatalf("Search failed: %s", searchW.Body.String())
	}

	// 验证返回了2个文档（apple和banana都是fruit）
	if !strings.Contains(searchW.Body.String(), `"total":{"value":2,"relation":"eq"}`) {
		t.Fatalf("Expected 2 documents, got: %s", searchW.Body.String())
	}

	t.Log("Search term test passed")
}

// TestDocumentHandler_Search_Range 测试range查询
func TestDocumentHandler_Search_Range(t *testing.T) {
	docHandler, cleanup, indexName := setupSearchTestEnvironment(t)
	defer cleanup()

	// 执行range查询
	searchData := `{"query":{"range":{"price":{"gte":1.0,"lte":3.0}}}}`
	searchReq := httptest.NewRequest("POST", "/"+indexName+"/_search", strings.NewReader(searchData))
	searchReq.Header.Set("Content-Type", "application/json")
	searchReq = mux.SetURLVars(searchReq, map[string]string{"index": indexName})
	searchW := httptest.NewRecorder()
	docHandler.Search(searchW, searchReq)

	if searchW.Code != http.StatusOK {
		t.Fatalf("Search failed: %s", searchW.Body.String())
	}

	// 验证返回了3个文档（价格在1.0-3.0之间的）
	if !strings.Contains(searchW.Body.String(), `"total":{"value":3,"relation":"eq"}`) {
		t.Fatalf("Expected 3 documents, got: %s", searchW.Body.String())
	}

	t.Log("Search range test passed")
}

// TestDocumentHandler_Search_Bool 测试bool查询
func TestDocumentHandler_Search_Bool(t *testing.T) {
	docHandler, cleanup, indexName := setupSearchTestEnvironment(t)
	defer cleanup()

	// 执行bool查询
	searchData := `{"query":{"bool":{"must":[{"match":{"category":"fruit"}}],"must_not":[{"match":{"name":"banana"}}]}}}`
	searchReq := httptest.NewRequest("POST", "/"+indexName+"/_search", strings.NewReader(searchData))
	searchReq.Header.Set("Content-Type", "application/json")
	searchReq = mux.SetURLVars(searchReq, map[string]string{"index": indexName})
	searchW := httptest.NewRecorder()
	docHandler.Search(searchW, searchReq)

	if searchW.Code != http.StatusOK {
		t.Fatalf("Search failed: %s", searchW.Body.String())
	}

	// 验证返回了1个文档（只有apple是fruit且不是banana）
	if !strings.Contains(searchW.Body.String(), `"total":{"value":1,"relation":"eq"}`) {
		t.Fatalf("Expected 1 document, got: %s", searchW.Body.String())
	}

	// 验证返回了apple文档
	if !strings.Contains(searchW.Body.String(), `"name":"apple"`) {
		t.Fatalf("Expected apple document, got: %s", searchW.Body.String())
	}

	t.Log("Search bool test passed")
}

// TestDocumentHandler_Search_Sort 测试排序
func TestDocumentHandler_Search_Sort(t *testing.T) {
	docHandler, cleanup, indexName := setupSearchTestEnvironment(t)
	defer cleanup()

	// 执行带排序的查询
	searchData := `{"query":{"match_all":{}},"sort":[{"price":{"order":"asc"}}]}`
	searchReq := httptest.NewRequest("POST", "/"+indexName+"/_search", strings.NewReader(searchData))
	searchReq.Header.Set("Content-Type", "application/json")
	searchReq = mux.SetURLVars(searchReq, map[string]string{"index": indexName})
	searchW := httptest.NewRecorder()
	docHandler.Search(searchW, searchReq)

	if searchW.Code != http.StatusOK {
		t.Fatalf("Search failed: %s", searchW.Body.String())
	}

	// 验证返回了5个文档
	if !strings.Contains(searchW.Body.String(), `"total":{"value":5,"relation":"eq"}`) {
		t.Fatalf("Expected 5 documents, got: %s", searchW.Body.String())
	}

	t.Log("Search sort test passed")
}

// TestDocumentHandler_Search_Pagination 测试分页
func TestDocumentHandler_Search_Pagination(t *testing.T) {
	docHandler, cleanup, indexName := setupSearchTestEnvironment(t)
	defer cleanup()

	// 执行带分页的查询
	searchData := `{"query":{"match_all":{}},"from":1,"size":2}`
	searchReq := httptest.NewRequest("POST", "/"+indexName+"/_search", strings.NewReader(searchData))
	searchReq.Header.Set("Content-Type", "application/json")
	searchReq = mux.SetURLVars(searchReq, map[string]string{"index": indexName})
	searchW := httptest.NewRecorder()
	docHandler.Search(searchW, searchReq)

	if searchW.Code != http.StatusOK {
		t.Fatalf("Search failed: %s", searchW.Body.String())
	}

	// 验证返回了5个文档但只显示2个
	if !strings.Contains(searchW.Body.String(), `"total":{"value":5,"relation":"eq"}`) {
		t.Fatalf("Expected 5 total documents, got: %s", searchW.Body.String())
	}

	t.Log("Search pagination test passed")
}

// TestDocumentHandler_Search_GET 测试GET请求搜索
func TestDocumentHandler_Search_GET(t *testing.T) {
	docHandler, cleanup, indexName := setupSearchTestEnvironment(t)
	defer cleanup()

	// 执行GET请求搜索
	searchReq := httptest.NewRequest("GET", "/"+indexName+"/_search?q=name:apple", nil)
	searchReq = mux.SetURLVars(searchReq, map[string]string{"index": indexName})
	searchW := httptest.NewRecorder()
	docHandler.Search(searchW, searchReq)

	if searchW.Code != http.StatusOK {
		t.Fatalf("Search failed: %s", searchW.Body.String())
	}

	// 验证返回了1个文档
	if !strings.Contains(searchW.Body.String(), `"total":{"value":1,"relation":"eq"}`) {
		t.Fatalf("Expected 1 document, got: %s", searchW.Body.String())
	}

	t.Log("Search GET test passed")
}

// TestDocumentHandler_Search_InvalidQuery 测试无效查询
func TestDocumentHandler_Search_InvalidQuery(t *testing.T) {
	docHandler, cleanup, indexName := setupSearchTestEnvironment(t)
	defer cleanup()

	// 执行无效查询
	searchData := `{"query":{"unsupported_query":{"field":"value"}}}`
	searchReq := httptest.NewRequest("POST", "/"+indexName+"/_search", strings.NewReader(searchData))
	searchReq.Header.Set("Content-Type", "application/json")
	searchReq = mux.SetURLVars(searchReq, map[string]string{"index": indexName})
	searchW := httptest.NewRecorder()
	docHandler.Search(searchW, searchReq)

	// 应该返回错误状态码
	if searchW.Code == http.StatusOK {
		t.Fatalf("Expected error for invalid query, but got success")
	}

	t.Log("Search invalid query test passed")
}

// TestDocumentHandler_Search_IndexNotFound 测试索引不存在
func TestDocumentHandler_Search_IndexNotFound(t *testing.T) {
	docHandler, cleanup, _ := setupSearchTestEnvironment(t)
	defer cleanup()

	// 搜索不存在的索引
	searchData := `{"query":{"match_all":{}}}`
	searchReq := httptest.NewRequest("POST", "/nonexistent_index/_search", strings.NewReader(searchData))
	searchReq.Header.Set("Content-Type", "application/json")
	searchReq = mux.SetURLVars(searchReq, map[string]string{"index": "nonexistent_index"})
	searchW := httptest.NewRecorder()
	docHandler.Search(searchW, searchReq)

	if searchW.Code != http.StatusNotFound {
		t.Fatalf("Expected 404 for non-existent index, got status %d", searchW.Code)
	}

	t.Log("Search index not found test passed")
}

// TestDocumentHandler_Search_Fields 测试字段过滤
func TestDocumentHandler_Search_Fields(t *testing.T) {
	docHandler, cleanup, indexName := setupSearchTestEnvironment(t)
	defer cleanup()

	// 执行带字段过滤的查询
	searchData := `{"query":{"match_all":{}},"_source":["name","category"]}`
	searchReq := httptest.NewRequest("POST", "/"+indexName+"/_search", strings.NewReader(searchData))
	searchReq.Header.Set("Content-Type", "application/json")
	searchReq = mux.SetURLVars(searchReq, map[string]string{"index": indexName})
	searchW := httptest.NewRecorder()
	docHandler.Search(searchW, searchReq)

	if searchW.Code != http.StatusOK {
		t.Fatalf("Search failed: %s", searchW.Body.String())
	}

	// 验证返回了5个文档
	if !strings.Contains(searchW.Body.String(), `"total":{"value":5,"relation":"eq"}`) {
		t.Fatalf("Expected 5 documents, got: %s", searchW.Body.String())
	}

	t.Log("Search fields test passed")
}
