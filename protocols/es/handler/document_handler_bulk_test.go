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

// TestBulk_Index 测试bulk index操作
func TestBulk_Index(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_bulk_test_*")
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

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建文档处理器
	docHandler := NewDocumentHandler(indexMgr, dirMgr, metaStore)

	// 创建测试索引
	indexName := "test_bulk_index"

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

	// 准备bulk数据 - index操作
	bulkData := `{"index":{"_index":"` + indexName + `","_id":"doc1"}}
{"name":"test document 1","value":1}
{"index":{"_index":"` + indexName + `","_id":"doc2"}}
{"name":"test document 2","value":2}`

	// 执行bulk操作
	bulkReq := httptest.NewRequest("POST", "/_bulk", strings.NewReader(bulkData))
	bulkReq.Header.Set("Content-Type", "application/x-ndjson")
	bulkW := httptest.NewRecorder()
	docHandler.Bulk(bulkW, bulkReq)

	if bulkW.Code != http.StatusOK {
		t.Fatalf("Bulk operation failed: %s", bulkW.Body.String())
	}

	// 验证响应
	if !strings.Contains(bulkW.Body.String(), `"status":201`) {
		t.Fatalf("Bulk operation should create documents with status 201")
	}

	t.Log("Bulk index operation test passed")
}

// TestBulk_Create 测试bulk create操作
func TestBulk_Create(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_bulk_test_*")
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

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建文档处理器
	docHandler := NewDocumentHandler(indexMgr, dirMgr, metaStore)

	// 创建测试索引
	indexName := "test_bulk_create"

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

	// 准备bulk数据 - create操作
	bulkData := `{"create":{"_index":"` + indexName + `","_id":"doc1"}}
{"name":"test document 1","value":1}
{"create":{"_index":"` + indexName + `","_id":"doc2"}}
{"name":"test document 2","value":2}`

	// 执行bulk操作
	bulkReq := httptest.NewRequest("POST", "/_bulk", strings.NewReader(bulkData))
	bulkReq.Header.Set("Content-Type", "application/x-ndjson")
	bulkW := httptest.NewRecorder()
	docHandler.Bulk(bulkW, bulkReq)

	if bulkW.Code != http.StatusOK {
		t.Fatalf("Bulk operation failed: %s", bulkW.Body.String())
	}

	// 验证响应
	if !strings.Contains(bulkW.Body.String(), `"status":201`) {
		t.Fatalf("Bulk operation should create documents with status 201")
	}

	t.Log("Bulk create operation test passed")
}

// TestBulk_Delete 测试bulk delete操作
func TestBulk_Delete(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_bulk_test_*")
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

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建文档处理器
	docHandler := NewDocumentHandler(indexMgr, dirMgr, metaStore)

	// 创建测试索引
	indexName := "test_bulk_delete"

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

	// 先创建文档
	createData := `{"index":{"_index":"` + indexName + `","_id":"doc1"}}
{"name":"test document 1","value":1}`

	createReq := httptest.NewRequest("POST", "/_bulk", strings.NewReader(createData))
	createReq.Header.Set("Content-Type", "application/x-ndjson")
	createW := httptest.NewRecorder()
	docHandler.Bulk(createW, createReq)

	if createW.Code != http.StatusOK {
		t.Fatalf("Create document failed: %s", createW.Body.String())
	}

	// 准备bulk数据 - delete操作
	bulkData := `{"delete":{"_index":"` + indexName + `","_id":"doc1"}}`

	// 执行bulk操作
	bulkReq := httptest.NewRequest("POST", "/_bulk", strings.NewReader(bulkData))
	bulkReq.Header.Set("Content-Type", "application/x-ndjson")
	bulkW := httptest.NewRecorder()
	docHandler.Bulk(bulkW, bulkReq)

	if bulkW.Code != http.StatusOK {
		t.Fatalf("Bulk operation failed: %s", bulkW.Body.String())
	}

	// 验证响应
	if !strings.Contains(bulkW.Body.String(), `"status":200`) {
		t.Fatalf("Bulk operation should delete documents with status 200")
	}

	t.Log("Bulk delete operation test passed")
}

// TestBulk_InvalidContentType 测试无效的Content-Type
func TestBulk_InvalidContentType(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_bulk_test_*")
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

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建文档处理器
	docHandler := NewDocumentHandler(indexMgr, dirMgr, metaStore)

	// 使用错误的Content-Type
	bulkReq := httptest.NewRequest("POST", "/_bulk", strings.NewReader("test"))
	bulkReq.Header.Set("Content-Type", "application/json") // 应该是 application/x-ndjson
	bulkW := httptest.NewRecorder()
	docHandler.Bulk(bulkW, bulkReq)

	if bulkW.Code != http.StatusBadRequest {
		t.Fatalf("Expected bad request for invalid content type, got status %d", bulkW.Code)
	}

	t.Log("Bulk invalid content type test passed")
}

// TestBulk_MissingIndex 测试索引不存在的情况
func TestBulk_MissingIndex(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_bulk_test_*")
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

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建文档处理器
	docHandler := NewDocumentHandler(indexMgr, dirMgr, metaStore)

	// 准备bulk数据 - 使用不存在的索引
	bulkData := `{"index":{"_index":"nonexistent_index","_id":"doc1"}}
{"name":"test document 1","value":1}`

	// 执行bulk操作
	bulkReq := httptest.NewRequest("POST", "/_bulk", strings.NewReader(bulkData))
	bulkReq.Header.Set("Content-Type", "application/x-ndjson")
	bulkW := httptest.NewRecorder()
	docHandler.Bulk(bulkW, bulkReq)

	if bulkW.Code != http.StatusOK {
		t.Fatalf("Bulk operation should succeed even with missing index errors: %s", bulkW.Body.String())
	}

	// 验证响应包含错误
	if !strings.Contains(bulkW.Body.String(), `"status":404`) {
		t.Fatalf("Bulk operation should return 404 for missing index")
	}

	t.Log("Bulk missing index test passed")
}

// TestBulk_IndexNotFound 测试索引不存在的情况
func TestBulk_IndexNotFound(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_bulk_test_*")
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

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建文档处理器
	docHandler := NewDocumentHandler(indexMgr, dirMgr, metaStore)

	// 准备bulk数据 - 使用不存在的索引
	bulkData := `{"index":{"_index":"nonexistent_index","_id":"doc1"}}
{"name":"test document 1","value":1}`

	// 执行bulk操作
	bulkReq := httptest.NewRequest("POST", "/_bulk", strings.NewReader(bulkData))
	bulkReq.Header.Set("Content-Type", "application/x-ndjson")
	bulkW := httptest.NewRecorder()
	docHandler.Bulk(bulkW, bulkReq)

	if bulkW.Code != http.StatusOK {
		t.Fatalf("Bulk operation should succeed even with index not found errors: %s", bulkW.Body.String())
	}

	// 验证响应包含错误
	if !strings.Contains(bulkW.Body.String(), `"status":404`) {
		t.Fatalf("Bulk operation should return 404 for index not found")
	}

	t.Log("Bulk index not found test passed")
}

// TestBulk_AutoID 测试自动ID生成
func TestBulk_AutoID(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_bulk_test_*")
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

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建文档处理器
	docHandler := NewDocumentHandler(indexMgr, dirMgr, metaStore)

	// 创建测试索引
	indexName := "test_bulk_autoid"

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

	// 准备bulk数据 - 不指定ID，应该自动生成
	bulkData := `{"index":{"_index":"` + indexName + `"}}
{"name":"test document","value":1}`

	// 执行bulk操作
	bulkReq := httptest.NewRequest("POST", "/_bulk", strings.NewReader(bulkData))
	bulkReq.Header.Set("Content-Type", "application/x-ndjson")
	bulkW := httptest.NewRecorder()
	docHandler.Bulk(bulkW, bulkReq)

	if bulkW.Code != http.StatusOK {
		t.Fatalf("Bulk operation failed: %s", bulkW.Body.String())
	}

	// 验证响应包含自动生成的ID
	if !strings.Contains(bulkW.Body.String(), `"_id":`) {
		t.Fatalf("Bulk operation should generate automatic ID")
	}

	t.Log("Bulk auto ID test passed")
}
