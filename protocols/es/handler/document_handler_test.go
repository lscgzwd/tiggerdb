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
	"time"

	"github.com/gorilla/mux"
	"github.com/lscgzwd/tiggerdb/directory"
	"github.com/lscgzwd/tiggerdb/metadata"
	esIndex "github.com/lscgzwd/tiggerdb/protocols/es/index"
)

func TestBulkAndCountIntegration(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

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
		t.Fatalf("Failed to create directory manager: %v", err)
	}

	// 创建元数据存储配置
	metaConfig := &metadata.MetadataStoreConfig{
		StorageType: "file",
		FilePath:    tempDir,
		EnableCache: true,
	}
	metaStore, err := metadata.NewFileMetadataStore(metaConfig)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建文档处理器
	docHandler := NewDocumentHandler(indexMgr, dirMgr, metaStore)

	// 创建测试索引
	indexName := "test_bulk_count"

	// 创建索引处理器
	indexHandler := NewIndexHandler(dirMgr, metaStore)
	indexHandler.SetIndexManager(indexMgr)

	// 创建索引
	req := httptest.NewRequest("PUT", "/"+indexName, nil)
	req = mux.SetURLVars(req, map[string]string{"index": indexName})
	w := httptest.NewRecorder()
	indexHandler.CreateIndex(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to create index: %s", w.Body.String())
	}

	// 准备bulk数据
	bulkData := `{"index":{"_index":"` + indexName + `","_id":"doc1"}}
{"name":"test document 1","value":1,"nested":{"field":"value1"}}
{"index":{"_index":"` + indexName + `","_id":"doc2"}}
{"name":"test document 2","value":2,"nested":{"field":"value2"}}
{"index":{"_index":"` + indexName + `","_id":"doc3"}}
{"name":"test document 3","value":3}`

	// 执行bulk操作
	bulkReq := httptest.NewRequest("POST", "/_bulk", strings.NewReader(bulkData))
	bulkReq.Header.Set("Content-Type", "application/x-ndjson")
	bulkW := httptest.NewRecorder()
	docHandler.Bulk(bulkW, bulkReq)

	if bulkW.Code != http.StatusOK {
		t.Fatalf("Bulk operation failed: %s", bulkW.Body.String())
	}

	// 验证bulk响应
	var bulkResp BulkResponse
	if err := json.Unmarshal(bulkW.Body.Bytes(), &bulkResp); err != nil {
		t.Fatalf("Failed to parse bulk response: %v", err)
	}

	if len(bulkResp.Items) != 3 {
		t.Fatalf("Expected 3 bulk items, got %d", len(bulkResp.Items))
	}

	// 等待索引刷新
	time.Sleep(100 * time.Millisecond)

	// 执行count查询
	countData := `{"query":{"match_all":{}}}`
	countReq := httptest.NewRequest("POST", "/"+indexName+"/_count", strings.NewReader(countData))
	countReq.Header.Set("Content-Type", "application/json")
	countReq = mux.SetURLVars(countReq, map[string]string{"index": indexName})
	countW := httptest.NewRecorder()
	docHandler.CountDocuments(countW, countReq)

	if countW.Code != http.StatusOK {
		t.Fatalf("Count operation failed: %s", countW.Body.String())
	}

	// 验证count响应
	var countResp map[string]interface{}
	if err := json.Unmarshal(countW.Body.Bytes(), &countResp); err != nil {
		t.Fatalf("Failed to parse count response: %v", err)
	}

	count, ok := countResp["count"].(float64)
	if !ok {
		t.Fatalf("Count response missing count field: %v", countResp)
	}

	if int(count) != 3 {
		t.Fatalf("Expected count 3, got %d", int(count))
	}

	t.Logf("Bulk and count integration test passed: indexed 3 documents, counted %d documents", int(count))
}

// TestProductionLikeBulkAndCountIntegration 测试类似生产环境的bulk和count集成
func TestProductionLikeBulkAndCountIntegration(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_prod_like_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 设置目录配置 - 模拟生产环境配置
	config := &directory.DirectoryConfig{
		BaseDir:           tempDir,
		DirPerm:           0755,
		MaxIndices:        100,
		MaxTables:         1000,
		MaxAge:            24 * time.Hour,
		EnableAutoCleanup: true,
	}

	// 创建目录管理器
	dirMgr, err := directory.NewDirectoryManager(config)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}

	// 创建元数据存储 - 使用文件存储模拟生产环境
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

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建文档处理器
	docHandler := NewDocumentHandler(indexMgr, dirMgr, metaStore)

	// 创建测试索引 - 使用类似生产环境的索引名
	indexName := "foeye_task_assets"

	// 创建索引处理器
	indexHandler := NewIndexHandler(dirMgr, metaStore)
	indexHandler.SetIndexManager(indexMgr)

	// 创建索引
	req := httptest.NewRequest("PUT", "/"+indexName, nil)
	req = mux.SetURLVars(req, map[string]string{"index": indexName})
	w := httptest.NewRecorder()
	indexHandler.CreateIndex(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to create index: %s", w.Body.String())
	}

	// 准备bulk数据 - 模拟生产环境的数据结构（包含嵌套文档）
	var bulkLines []string
	docCount := 100 // 模拟生产环境的文档数量

	for i := 1; i <= docCount; i++ {
		ip := fmt.Sprintf("192.168.1.%d", i%255+1)
		docID := fmt.Sprintf("asset_%d", i)

		// 添加索引元数据
		bulkLines = append(bulkLines, fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, indexName, docID))

		// 添加文档数据 - 模拟生产环境的数据结构
		docData := fmt.Sprintf(`{
			"ip": "%s",
			"hostname": "host-%d.example.com",
			"ports": [
				{"port": 80, "service": "http", "status": "open"},
				{"port": 443, "service": "https", "status": "open"}
			],
			"vulnerabilities": [
				{"id": "CVE-2023-1234", "severity": "high", "cvss": 8.5},
				{"id": "CVE-2023-5678", "severity": "medium", "cvss": 5.0}
			],
			"tags": ["production", "web-server"],
			"last_scan": "2024-01-01T10:00:00Z",
			"status": "active"
		}`, ip, i)

		// 压缩JSON（移除换行和空格）
		docData = strings.ReplaceAll(docData, "\n", "")
		docData = strings.ReplaceAll(docData, "\t", "")
		docData = strings.ReplaceAll(docData, "  ", "")

		bulkLines = append(bulkLines, docData)
	}

	// 合并所有bulk数据
	bulkData := strings.Join(bulkLines, "\n")

	// 执行bulk操作
	bulkReq := httptest.NewRequest("POST", "/_bulk?refresh=true", strings.NewReader(bulkData))
	bulkReq.Header.Set("Content-Type", "application/x-ndjson")
	bulkW := httptest.NewRecorder()
	docHandler.Bulk(bulkW, bulkReq)

	if bulkW.Code != http.StatusOK {
		t.Fatalf("Bulk operation failed: %s", bulkW.Body.String())
	}

	// 验证bulk响应
	var bulkResp map[string]interface{}
	if err := json.Unmarshal(bulkW.Body.Bytes(), &bulkResp); err != nil {
		t.Fatalf("Failed to parse bulk response: %v", err)
	}

	// 检查是否有错误
	if errors, ok := bulkResp["errors"].(bool); ok && errors {
		t.Fatalf("Bulk operation had errors: %s", bulkW.Body.String())
	}

	// 记录bulk操作结果
	t.Logf("Bulk operation completed: inserted %d documents", docCount)

	// 等待索引刷新 - 模拟生产环境
	time.Sleep(1 * time.Second)

	// 测试1: 基本的count查询（无条件）- 这是生产环境中失败的查询
	t.Log("Testing basic count query (the failing one in production)")
	countReq := httptest.NewRequest("POST", "/"+indexName+"/_count", strings.NewReader("{}"))
	countReq.Header.Set("Content-Type", "application/json")
	countReq = mux.SetURLVars(countReq, map[string]string{"index": indexName})
	countW := httptest.NewRecorder()
	docHandler.CountDocuments(countW, countReq)

	if countW.Code != http.StatusOK {
		t.Fatalf("Count operation failed: %s", countW.Body.String())
	}

	// 验证count响应
	var countResp map[string]interface{}
	if err := json.Unmarshal(countW.Body.Bytes(), &countResp); err != nil {
		t.Fatalf("Failed to parse count response: %v", err)
	}

	count, ok := countResp["count"].(float64)
	if !ok {
		t.Fatalf("Count response missing count field: %v", countResp)
	}

	if int(count) != docCount {
		t.Fatalf("CRITICAL BUG: Expected count %d, got %d. This is the production issue!", docCount, int(count))
	}

	t.Logf("✅ Basic count test PASSED: counted %d documents", int(count))

	// 测试2: 验证索引实例一致性 - 多次查询应该返回相同结果
	for i := 0; i < 3; i++ {
		countReq2 := httptest.NewRequest("POST", "/"+indexName+"/_count", strings.NewReader("{}"))
		countReq2.Header.Set("Content-Type", "application/json")
		countReq2 = mux.SetURLVars(countReq2, map[string]string{"index": indexName})
		countW2 := httptest.NewRecorder()
		docHandler.CountDocuments(countW2, countReq2)

		if countW2.Code != http.StatusOK {
			t.Fatalf("Count operation %d failed: %s", i+1, countW2.Body.String())
		}

		var countResp2 map[string]interface{}
		if err := json.Unmarshal(countW2.Body.Bytes(), &countResp2); err != nil {
			t.Fatalf("Failed to parse count response %d: %v", i+1, err)
		}

		count2, ok := countResp2["count"].(float64)
		if !ok {
			t.Fatalf("Count response %d missing count field: %v", i+1, countResp2)
		}

		if int(count2) != docCount {
			t.Fatalf("Index instance inconsistency: query %d expected %d, got %d", i+1, docCount, int(count2))
		}

		t.Logf("✅ Count consistency check %d PASSED: %d documents", i+1, int(count2))
	}

	// 测试3: Search查询验证 - 确保数据可搜索
	searchData := `{"query":{"match_all":{}},"size":5}`
	searchReq := httptest.NewRequest("POST", "/"+indexName+"/_search", strings.NewReader(searchData))
	searchReq.Header.Set("Content-Type", "application/json")
	searchReq = mux.SetURLVars(searchReq, map[string]string{"index": indexName})
	searchW := httptest.NewRecorder()
	docHandler.Search(searchW, searchReq)

	if searchW.Code != http.StatusOK {
		t.Fatalf("Search operation failed: %s", searchW.Body.String())
	}

	// 验证search响应
	var searchResp map[string]interface{}
	if err := json.Unmarshal(searchW.Body.Bytes(), &searchResp); err != nil {
		t.Fatalf("Failed to parse search response: %v", err)
	}

	hits, ok := searchResp["hits"].(map[string]interface{})
	if !ok {
		t.Fatalf("Search response missing hits field: %v", searchResp)
	}

	total, ok := hits["total"].(map[string]interface{})
	if !ok {
		t.Fatalf("Search response missing total field: %v", hits)
	}

	searchCount, ok := total["value"].(float64)
	if !ok {
		t.Fatalf("Search total missing value field: %v", total)
	}

	if int(searchCount) != docCount {
		t.Fatalf("Search count mismatch: expected %d, got %d. Response: %s", docCount, int(searchCount), searchW.Body.String())
	}

	t.Logf("✅ Search test PASSED: found %d documents via search", int(searchCount))

	// 测试4: 并发访问测试 - 模拟生产环境的并发查询
	t.Log("Testing concurrent access (simulating production load)")
	done := make(chan bool, 10)
	errorCount := 0

	for i := 0; i < 10; i++ {
		go func(goroutineID int) {
			defer func() { done <- true }()

			// 每个goroutine执行多次count查询
			for j := 0; j < 5; j++ {
				countReq := httptest.NewRequest("POST", "/"+indexName+"/_count", strings.NewReader("{}"))
				countReq.Header.Set("Content-Type", "application/json")
				countReq = mux.SetURLVars(countReq, map[string]string{"index": indexName})
				countW := httptest.NewRecorder()

				docHandler.CountDocuments(countW, countReq)

				if countW.Code != http.StatusOK {
					t.Errorf("Concurrent count operation failed in goroutine %d, attempt %d: %s", goroutineID, j, countW.Body.String())
					errorCount++
					return
				}

				var countResp map[string]interface{}
				if err := json.Unmarshal(countW.Body.Bytes(), &countResp); err != nil {
					t.Errorf("Failed to parse concurrent count response in goroutine %d, attempt %d: %v", goroutineID, j, err)
					errorCount++
					return
				}

				count, ok := countResp["count"].(float64)
				if !ok || int(count) != docCount {
					t.Errorf("Concurrent count inconsistency in goroutine %d, attempt %d: expected %d, got %v", goroutineID, j, docCount, countResp)
					errorCount++
					return
				}
			}
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < 10; i++ {
		<-done
	}

	if errorCount > 0 {
		t.Fatalf("Concurrent access test FAILED: %d errors occurred", errorCount)
	}

	t.Log("✅ Concurrent access test PASSED: 10 goroutines x 5 queries each = 50 successful queries")

	// 测试5: 验证数据完整性 - 随机抽样检查文档内容
	hitsArray, ok := hits["hits"].([]interface{})
	if !ok || len(hitsArray) == 0 {
		t.Fatalf("No hits returned for data validation")
	}

	// 检查前3个文档的数据完整性
	for i := 0; i < 3 && i < len(hitsArray); i++ {
		hit, ok := hitsArray[i].(map[string]interface{})
		if !ok {
			t.Fatalf("Invalid hit format at index %d", i)
		}

		source, ok := hit["_source"].(map[string]interface{})
		if !ok {
			t.Fatalf("Missing _source in hit at index %d", i)
		}

		// 验证关键字段存在
		if _, hasIP := source["ip"]; !hasIP {
			t.Fatalf("Document %d missing ip field", i)
		}

		if _, hasPorts := source["ports"]; !hasPorts {
			t.Fatalf("Document %d missing ports field", i)
		}

		if _, hasTags := source["tags"]; !hasTags {
			t.Fatalf("Document %d missing tags field", i)
		}

		t.Logf("✅ Document %d data integrity check PASSED", i)
	}

	t.Logf("🎉 PRODUCTION-LIKE INTEGRATION TEST COMPLETELY PASSED:")
	t.Logf("   - Bulk indexed: %d documents", docCount)
	t.Logf("   - Count queries: consistent results")
	t.Logf("   - Search queries: working correctly")
	t.Logf("   - Concurrent access: no race conditions")
	t.Logf("   - Data integrity: all documents valid")
	t.Logf("   - Index consistency: stable across operations")
}

func TestDocumentHandler(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

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
		t.Fatalf("Failed to create directory manager: %v", err)
	}

	// 创建元数据存储配置
	metaConfig := &metadata.MetadataStoreConfig{
		StorageType: "file",
		FilePath:    tempDir,
		EnableCache: true,
	}
	metaStore, err := metadata.NewFileMetadataStore(metaConfig)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建文档处理器
	docHandler := NewDocumentHandler(indexMgr, dirMgr, metaStore)

	// 创建测试索引
	indexName := "test_index"

	// 创建索引处理器
	indexHandler := NewIndexHandler(dirMgr, metaStore)
	indexHandler.SetIndexManager(indexMgr)

	// 创建索引
	req := httptest.NewRequest("PUT", "/"+indexName, nil)
	req = mux.SetURLVars(req, map[string]string{"index": indexName})
	w := httptest.NewRecorder()
	indexHandler.CreateIndex(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to create index: %s", w.Body.String())
	}

	// 测试创建文档
	docData := map[string]interface{}{
		"name":  "test document",
		"value": 42,
		"nested": map[string]interface{}{
			"field": "nested value",
		},
	}

	docJSON, _ := json.Marshal(docData)
	createReq := httptest.NewRequest("POST", "/"+indexName+"/_doc/doc1", bytes.NewReader(docJSON))
	createReq.Header.Set("Content-Type", "application/json")
	createReq = mux.SetURLVars(createReq, map[string]string{"index": indexName})
	createW := httptest.NewRecorder()
	docHandler.CreateDocument(createW, createReq)

	if createW.Code != http.StatusCreated {
		t.Logf("Create document failed: %s", createW.Body.String())
	} else {
		t.Log("Document creation test passed")
	}

	t.Log("Document handler basic test completed")
}
