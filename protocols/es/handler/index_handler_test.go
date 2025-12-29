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

	"github.com/lscgzwd/tiggerdb/directory"
	"github.com/lscgzwd/tiggerdb/metadata"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/server"
	esIndex "github.com/lscgzwd/tiggerdb/protocols/es/index"
)

// TestIndexRoutes_BasicFlow 测试索引路由的基本流程
// 使用真实的 IndexHandler 实现，确保路由和处理器正常工作
func TestIndexRoutes_BasicFlow(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_routes_test_*")
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
		StorageType:      "memory",
		EnableCache:      true,
		EnableVersioning: true,
	}
	metaStore, err := metadata.NewMetadataStore(metaConfig)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer metaStore.Close()

	// 创建索引处理器和服务器
	indexHandler := NewIndexHandler(dirMgr, metaStore)
	httpSrv, _ := server.NewServer(server.DefaultServerConfig())
	router := httpSrv.GetRouter()

	// 直接注册路由（避免循环导入）
	routes := []server.Route{
		{Method: "GET", Path: "/_cat/indices", Handler: indexHandler.ListIndices},
		{Method: "PUT", Path: "/{index}", Handler: indexHandler.CreateIndex},
		{Method: "GET", Path: "/{index}", Handler: indexHandler.GetIndex},
		{Method: "DELETE", Path: "/{index}", Handler: indexHandler.DeleteIndex},
		{Method: "GET", Path: "/{index}/_mapping", Handler: indexHandler.GetMapping},
	}
	router.AddRoutes(routes)
	mux := router.Build()

	// List indices
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/_cat/indices", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}

	// Create index
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("PUT", "/my_index", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}

	// Get index
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/my_index", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}

	// Get mapping
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/my_index/_mapping", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}

	// Delete index
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("DELETE", "/my_index", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}
}

func TestIndexHandler_CreateIndex(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_index_test_*")
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
		StorageType:      "memory",
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
	httpSrv, _ := server.NewServer(server.DefaultServerConfig())
	router := httpSrv.GetRouter()
	routes := []server.Route{
		{Method: "GET", Path: "/_cat/indices", Handler: indexHandler.ListIndices},
		{Method: "PUT", Path: "/{index}", Handler: indexHandler.CreateIndex},
		{Method: "GET", Path: "/{index}", Handler: indexHandler.GetIndex},
		{Method: "DELETE", Path: "/{index}", Handler: indexHandler.DeleteIndex},
		{Method: "GET", Path: "/{index}/_mapping", Handler: indexHandler.GetMapping},
	}
	router.AddRoutes(routes)
	mux := router.Build()

	// 测试创建索引
	indexName := "test_index"
	reqBody := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"title": map[string]interface{}{
					"type": "text",
				},
			},
		},
		"settings": map[string]interface{}{
			"number_of_shards": 1,
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)
	req := httptest.NewRequest("PUT", "/"+indexName, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d, body: %s", w.Code, w.Body.String())
	}

	// 验证索引已创建
	if !dirMgr.IndexExists(indexName) {
		t.Fatal("Index should exist after creation")
	}

	// 验证元数据已保存
	indexMeta, err := metaStore.GetIndexMetadata(indexName)
	if err != nil {
		t.Fatalf("Failed to get index metadata: %v", err)
	}
	if indexMeta.Name != indexName {
		t.Fatalf("Expected index name %s, got %s", indexName, indexMeta.Name)
	}
}

func TestIndexHandler_GetIndex(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_index_test_*")
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
		StorageType:      "memory",
		EnableCache:      true,
		EnableVersioning: true,
	}
	metaStore, err := metadata.NewMetadataStore(metaConfig)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer metaStore.Close()

	// 创建索引
	indexName := "test_index"
	if err := dirMgr.CreateIndex(indexName); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建索引处理器
	indexHandler := NewIndexHandler(dirMgr, metaStore)
	indexHandler.SetIndexManager(indexMgr)
	httpSrv, _ := server.NewServer(server.DefaultServerConfig())
	router := httpSrv.GetRouter()
	routes := []server.Route{
		{Method: "GET", Path: "/_cat/indices", Handler: indexHandler.ListIndices},
		{Method: "PUT", Path: "/{index}", Handler: indexHandler.CreateIndex},
		{Method: "GET", Path: "/{index}", Handler: indexHandler.GetIndex},
		{Method: "DELETE", Path: "/{index}", Handler: indexHandler.DeleteIndex},
		{Method: "GET", Path: "/{index}/_mapping", Handler: indexHandler.GetMapping},
	}
	router.AddRoutes(routes)
	mux := router.Build()

	// 测试获取索引
	req := httptest.NewRequest("GET", "/"+indexName, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d, body: %s", w.Code, w.Body.String())
	}
}

func TestIndexHandler_DeleteIndex(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_index_test_*")
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
		StorageType:      "memory",
		EnableCache:      true,
		EnableVersioning: true,
	}
	metaStore, err := metadata.NewMetadataStore(metaConfig)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer metaStore.Close()

	// 创建索引
	indexName := "test_index"
	if err := dirMgr.CreateIndex(indexName); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建索引处理器
	indexHandler := NewIndexHandler(dirMgr, metaStore)
	indexHandler.SetIndexManager(indexMgr)
	httpSrv, _ := server.NewServer(server.DefaultServerConfig())
	router := httpSrv.GetRouter()
	routes := []server.Route{
		{Method: "GET", Path: "/_cat/indices", Handler: indexHandler.ListIndices},
		{Method: "PUT", Path: "/{index}", Handler: indexHandler.CreateIndex},
		{Method: "GET", Path: "/{index}", Handler: indexHandler.GetIndex},
		{Method: "DELETE", Path: "/{index}", Handler: indexHandler.DeleteIndex},
		{Method: "GET", Path: "/{index}/_mapping", Handler: indexHandler.GetMapping},
	}
	router.AddRoutes(routes)
	mux := router.Build()

	// 测试删除索引
	req := httptest.NewRequest("DELETE", "/"+indexName, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d, body: %s", w.Code, w.Body.String())
	}

	// 验证索引已删除
	if dirMgr.IndexExists(indexName) {
		t.Fatal("Index should not exist after deletion")
	}
}

func TestIndexHandler_ListIndices(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_index_test_*")
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
		StorageType:      "memory",
		EnableCache:      true,
		EnableVersioning: true,
	}
	metaStore, err := metadata.NewMetadataStore(metaConfig)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer metaStore.Close()

	// 创建几个索引
	indices := []string{"index1", "index2", "index3"}
	for _, indexName := range indices {
		if err := dirMgr.CreateIndex(indexName); err != nil {
			t.Fatalf("Failed to create index %s: %v", indexName, err)
		}
	}

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建索引处理器
	indexHandler := NewIndexHandler(dirMgr, metaStore)
	indexHandler.SetIndexManager(indexMgr)
	httpSrv, _ := server.NewServer(server.DefaultServerConfig())
	router := httpSrv.GetRouter()
	routes := []server.Route{
		{Method: "GET", Path: "/_cat/indices", Handler: indexHandler.ListIndices},
		{Method: "PUT", Path: "/{index}", Handler: indexHandler.CreateIndex},
		{Method: "GET", Path: "/{index}", Handler: indexHandler.GetIndex},
		{Method: "DELETE", Path: "/{index}", Handler: indexHandler.DeleteIndex},
		{Method: "GET", Path: "/{index}/_mapping", Handler: indexHandler.GetMapping},
	}
	router.AddRoutes(routes)
	mux := router.Build()

	// 测试列出索引
	req := httptest.NewRequest("GET", "/_cat/indices", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d, body: %s", w.Code, w.Body.String())
	}
}

func TestIndexHandler_CreateIndex_AlreadyExists(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_index_test_*")
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
		StorageType:      "memory",
		EnableCache:      true,
		EnableVersioning: true,
	}
	metaStore, err := metadata.NewMetadataStore(metaConfig)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer metaStore.Close()

	// 先创建索引
	indexName := "test_index"
	if err := dirMgr.CreateIndex(indexName); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建索引处理器
	indexHandler := NewIndexHandler(dirMgr, metaStore)
	indexHandler.SetIndexManager(indexMgr)
	httpSrv, _ := server.NewServer(server.DefaultServerConfig())
	router := httpSrv.GetRouter()
	routes := []server.Route{
		{Method: "GET", Path: "/_cat/indices", Handler: indexHandler.ListIndices},
		{Method: "PUT", Path: "/{index}", Handler: indexHandler.CreateIndex},
		{Method: "GET", Path: "/{index}", Handler: indexHandler.GetIndex},
		{Method: "DELETE", Path: "/{index}", Handler: indexHandler.DeleteIndex},
		{Method: "GET", Path: "/{index}/_mapping", Handler: indexHandler.GetMapping},
	}
	router.AddRoutes(routes)
	mux := router.Build()

	// 尝试再次创建相同索引
	req := httptest.NewRequest("PUT", "/"+indexName, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409 got %d, body: %s", w.Code, w.Body.String())
	}
}

func TestIndexHandler_GetIndex_NotFound(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_index_test_*")
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
		StorageType:      "memory",
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
	httpSrv, _ := server.NewServer(server.DefaultServerConfig())
	router := httpSrv.GetRouter()
	routes := []server.Route{
		{Method: "GET", Path: "/_cat/indices", Handler: indexHandler.ListIndices},
		{Method: "PUT", Path: "/{index}", Handler: indexHandler.CreateIndex},
		{Method: "GET", Path: "/{index}", Handler: indexHandler.GetIndex},
		{Method: "DELETE", Path: "/{index}", Handler: indexHandler.DeleteIndex},
		{Method: "GET", Path: "/{index}/_mapping", Handler: indexHandler.GetMapping},
	}
	router.AddRoutes(routes)
	mux := router.Build()

	// 测试获取不存在的索引
	req := httptest.NewRequest("GET", "/nonexistent_index", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 got %d, body: %s", w.Code, w.Body.String())
	}
}

func TestIndexHandler_GetMapping_NotFound(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_index_test_*")
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
		StorageType:      "memory",
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
	httpSrv, _ := server.NewServer(server.DefaultServerConfig())
	router := httpSrv.GetRouter()
	routes := []server.Route{
		{Method: "GET", Path: "/_cat/indices", Handler: indexHandler.ListIndices},
		{Method: "PUT", Path: "/{index}", Handler: indexHandler.CreateIndex},
		{Method: "GET", Path: "/{index}", Handler: indexHandler.GetIndex},
		{Method: "DELETE", Path: "/{index}", Handler: indexHandler.DeleteIndex},
		{Method: "GET", Path: "/{index}/_mapping", Handler: indexHandler.GetMapping},
	}
	router.AddRoutes(routes)
	mux := router.Build()

	// 测试获取不存在的索引的映射
	req := httptest.NewRequest("GET", "/nonexistent_index/_mapping", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 got %d, body: %s", w.Code, w.Body.String())
	}
}

func TestIndexHandler_GetMapping_WithMetadata(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_index_test_*")
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
		StorageType:      "memory",
		EnableCache:      true,
		EnableVersioning: true,
	}
	metaStore, err := metadata.NewMetadataStore(metaConfig)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer metaStore.Close()

	// 创建索引
	indexName := "test_index"
	if err := dirMgr.CreateIndex(indexName); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// 保存元数据
	mapping := map[string]interface{}{
		"properties": map[string]interface{}{
			"title": map[string]interface{}{
				"type": "text",
			},
		},
	}
	indexMeta := &metadata.IndexMetadata{
		Name:     indexName,
		Mapping:  mapping,
		Settings: make(map[string]interface{}),
		Aliases:  []string{},
		Version:  1,
	}
	if err := metaStore.SaveIndexMetadata(indexName, indexMeta); err != nil {
		t.Fatalf("Failed to save metadata: %v", err)
	}

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建索引处理器
	indexHandler := NewIndexHandler(dirMgr, metaStore)
	indexHandler.SetIndexManager(indexMgr)
	httpSrv, _ := server.NewServer(server.DefaultServerConfig())
	router := httpSrv.GetRouter()
	routes := []server.Route{
		{Method: "GET", Path: "/_cat/indices", Handler: indexHandler.ListIndices},
		{Method: "PUT", Path: "/{index}", Handler: indexHandler.CreateIndex},
		{Method: "GET", Path: "/{index}", Handler: indexHandler.GetIndex},
		{Method: "DELETE", Path: "/{index}", Handler: indexHandler.DeleteIndex},
		{Method: "GET", Path: "/{index}/_mapping", Handler: indexHandler.GetMapping},
	}
	router.AddRoutes(routes)
	mux := router.Build()

	// 测试获取映射
	req := httptest.NewRequest("GET", "/"+indexName+"/_mapping", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d, body: %s", w.Code, w.Body.String())
	}
}
