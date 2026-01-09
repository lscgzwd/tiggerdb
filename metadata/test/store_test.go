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

package test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lscgzwd/tiggerdb/metadata"
)

func TestNewMetadataStore_FileStore(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_metadata_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := &metadata.MetadataStoreConfig{
		StorageType: "file",
		FilePath:    tempDir,
		EnableCache: true,
	}

	store, err := metadata.NewMetadataStore(config)
	if err != nil {
		t.Fatalf("Failed to create file metadata store: %v", err)
	}
	defer store.Close()

	if store == nil {
		t.Fatal("Metadata store is nil")
	}

	// 验证是FileMetadataStore类型
	if _, ok := store.(*metadata.FileMetadataStore); !ok {
		t.Fatal("Expected FileMetadataStore type")
	}
}

func TestNewMetadataStore_MemoryStore(t *testing.T) {
	config := &metadata.MetadataStoreConfig{
		StorageType: "memory",
		EnableCache: true,
	}

	store, err := metadata.NewMetadataStore(config)
	if err != nil {
		t.Fatalf("Failed to create memory metadata store: %v", err)
	}
	defer store.Close()

	if store == nil {
		t.Fatal("Metadata store is nil")
	}

	// 验证是MemoryMetadataStore类型
	if _, ok := store.(*metadata.MemoryMetadataStore); !ok {
		t.Fatal("Expected MemoryMetadataStore type")
	}
}

func TestNewMetadataStore_InvalidType(t *testing.T) {
	config := &metadata.MetadataStoreConfig{
		StorageType: "invalid",
	}

	store, err := metadata.NewMetadataStore(config)
	if err == nil {
		t.Fatal("Expected error for invalid storage type")
	}
	if store != nil {
		t.Fatal("Store should be nil for invalid type")
	}

	// 验证错误类型
	if _, ok := err.(*metadata.UnsupportedStorageTypeError); !ok {
		t.Fatalf("Expected UnsupportedStorageTypeError, got %T", err)
	}
}

func TestFileMetadataStore_IndexOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_metadata_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := &metadata.MetadataStoreConfig{
		StorageType: "file",
		FilePath:    tempDir,
		EnableCache: true,
	}

	store, err := metadata.NewMetadataStore(config)
	if err != nil {
		t.Fatalf("Failed to create file metadata store: %v", err)
	}
	defer store.Close()

	indexName := "test_index"

	// 创建索引元数据
	indexMetadata := &metadata.IndexMetadata{
		Name:      indexName,
		Mapping:   map[string]interface{}{"properties": map[string]interface{}{}},
		Settings:  map[string]interface{}{"refresh_interval": "1s"},
		Aliases:   []string{},
		Version:   1,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// 测试保存
	err = store.SaveIndexMetadata(indexName, indexMetadata)
	if err != nil {
		t.Fatalf("Failed to save index metadata: %v", err)
	}

	// 测试获取
	retrieved, err := store.GetIndexMetadata(indexName)
	if err != nil {
		t.Fatalf("Failed to get index metadata: %v", err)
	}

	if retrieved.Name != indexName {
		t.Fatalf("Expected index name %s, got %s", indexName, retrieved.Name)
	}

	// 验证文件是否创建
	metadataPath := filepath.Join(tempDir, "indexes", indexName, "metadata.json")
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		t.Fatalf("Metadata file should exist: %s", metadataPath)
	}

	// 测试删除
	err = store.DeleteIndexMetadata(indexName)
	if err != nil {
		t.Fatalf("Failed to delete index metadata: %v", err)
	}

	// 验证删除后获取失败
	_, err = store.GetIndexMetadata(indexName)
	if err == nil {
		t.Fatal("Expected error when getting deleted index metadata")
	}

	// 验证错误类型
	if _, ok := err.(*metadata.MetadataNotFoundError); !ok {
		t.Fatalf("Expected MetadataNotFoundError, got %T", err)
	}
}

func TestFileMetadataStore_TableOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_metadata_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := &metadata.MetadataStoreConfig{
		StorageType: "file",
		FilePath:    tempDir,
		EnableCache: true,
	}

	store, err := metadata.NewMetadataStore(config)
	if err != nil {
		t.Fatalf("Failed to create file metadata store: %v", err)
	}
	defer store.Close()

	indexName := "test_index"
	tableName := "test_table"

	// 创建表元数据
	schema := &metadata.TableSchema{
		Columns: []*metadata.TableColumn{
			{
				Name:     "id",
				Type:     "int",
				Nullable: false,
			},
			{
				Name:     "name",
				Type:     "varchar",
				Length:   100,
				Nullable: true,
			},
		},
	}

	tableMetadata := &metadata.TableMetadata{
		Name:        tableName,
		Schema:      schema,
		Constraints: []*metadata.TableConstraint{},
		Indexes:     []*metadata.TableIndex{},
		Version:     1,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	// 测试保存
	err = store.SaveTableMetadata(indexName, tableName, tableMetadata)
	if err != nil {
		t.Fatalf("Failed to save table metadata: %v", err)
	}

	// 测试获取
	retrieved, err := store.GetTableMetadata(indexName, tableName)
	if err != nil {
		t.Fatalf("Failed to get table metadata: %v", err)
	}

	if retrieved.Name != tableName {
		t.Fatalf("Expected table name %s, got %s", tableName, retrieved.Name)
	}

	if len(retrieved.Schema.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(retrieved.Schema.Columns))
	}

	// 验证文件是否创建
	metadataPath := filepath.Join(tempDir, "indexes", indexName, "tables", tableName, "metadata.json")
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		t.Fatalf("Metadata file should exist: %s", metadataPath)
	}

	// 测试删除
	err = store.DeleteTableMetadata(indexName, tableName)
	if err != nil {
		t.Fatalf("Failed to delete table metadata: %v", err)
	}

	// 验证删除后获取失败
	_, err = store.GetTableMetadata(indexName, tableName)
	if err == nil {
		t.Fatal("Expected error when getting deleted table metadata")
	}
}

func TestFileMetadataStore_ListOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_metadata_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := &metadata.MetadataStoreConfig{
		StorageType: "file",
		FilePath:    tempDir,
		EnableCache: true,
	}

	store, err := metadata.NewMetadataStore(config)
	if err != nil {
		t.Fatalf("Failed to create file metadata store: %v", err)
	}
	defer store.Close()

	// 创建多个索引
	indexNames := []string{"index1", "index2", "index3"}
	for _, indexName := range indexNames {
		indexMetadata := &metadata.IndexMetadata{
			Name:      indexName,
			Mapping:   map[string]interface{}{"properties": map[string]interface{}{}},
			Settings:  map[string]interface{}{},
			Aliases:   []string{},
			Version:   1,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		err = store.SaveIndexMetadata(indexName, indexMetadata)
		if err != nil {
			t.Fatalf("Failed to save index %s: %v", indexName, err)
		}
	}

	// 测试列出索引
	indices, err := store.ListIndexMetadata()
	if err != nil {
		t.Fatalf("Failed to list indices: %v", err)
	}

	if len(indices) != len(indexNames) {
		t.Fatalf("Expected %d indices, got %d", len(indexNames), len(indices))
	}

	// 验证所有索引都在列表中
	found := make(map[string]bool)
	for _, index := range indices {
		found[index.Name] = true
	}

	for _, expected := range indexNames {
		if !found[expected] {
			t.Fatalf("Index %s not found in list", expected)
		}
	}

	// 创建表
	indexName := "index1"
	tableNames := []string{"table1", "table2"}
	for _, tableName := range tableNames {
		schema := &metadata.TableSchema{
			Columns: []*metadata.TableColumn{
				{Name: "id", Type: "int", Nullable: false},
			},
		}
		tableMetadata := &metadata.TableMetadata{
			Name:        tableName,
			Schema:      schema,
			Constraints: []*metadata.TableConstraint{},
			Indexes:     []*metadata.TableIndex{},
			Version:     1,
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}
		err = store.SaveTableMetadata(indexName, tableName, tableMetadata)
		if err != nil {
			t.Fatalf("Failed to save table %s: %v", tableName, err)
		}
	}

	// 测试列出表
	tables, err := store.ListTableMetadata(indexName)
	if err != nil {
		t.Fatalf("Failed to list tables: %v", err)
	}

	if len(tables) != len(tableNames) {
		t.Fatalf("Expected %d tables, got %d", len(tableNames), len(tables))
	}
}

func TestMemoryMetadataStore_BasicOperations(t *testing.T) {
	config := &metadata.MetadataStoreConfig{
		StorageType: "memory",
		EnableCache: false,
	}

	store, err := metadata.NewMetadataStore(config)
	if err != nil {
		t.Fatalf("Failed to create memory metadata store: %v", err)
	}
	defer store.Close()

	indexName := "test_index"

	// 创建索引元数据
	indexMetadata := &metadata.IndexMetadata{
		Name:      indexName,
		Mapping:   map[string]interface{}{"properties": map[string]interface{}{}},
		Settings:  map[string]interface{}{},
		Aliases:   []string{},
		Version:   1,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// 测试保存
	err = store.SaveIndexMetadata(indexName, indexMetadata)
	if err != nil {
		t.Fatalf("Failed to save index metadata: %v", err)
	}

	// 测试获取
	retrieved, err := store.GetIndexMetadata(indexName)
	if err != nil {
		t.Fatalf("Failed to get index metadata: %v", err)
	}

	if retrieved.Name != indexName {
		t.Fatalf("Expected index name %s, got %s", indexName, retrieved.Name)
	}

	// 测试版本
	version, err := store.GetLatestVersion()
	if err != nil {
		t.Fatalf("Failed to get latest version: %v", err)
	}

	if version < 1 {
		t.Fatalf("Expected version >= 1, got %d", version)
	}
}

func TestMetadataStore_VersionOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_metadata_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := &metadata.MetadataStoreConfig{
		StorageType:      "file",
		FilePath:         tempDir,
		EnableCache:      true,
		EnableVersioning: true,
	}

	store, err := metadata.NewMetadataStore(config)
	if err != nil {
		t.Fatalf("Failed to create file metadata store: %v", err)
	}
	defer store.Close()

	// 获取初始版本
	initialVersion, err := store.GetLatestVersion()
	if err != nil {
		t.Fatalf("Failed to get initial version: %v", err)
	}

	// 创建快照
	err = store.CreateSnapshot(initialVersion)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// 验证快照文件存在
	snapshotPath := filepath.Join(tempDir, "versions", "v1", "snapshot.json")
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Fatalf("Snapshot file should exist: %s", snapshotPath)
	}

	// 保存一些元数据来改变版本
	indexMetadata := &metadata.IndexMetadata{
		Name:      "test_index",
		Mapping:   map[string]interface{}{"properties": map[string]interface{}{}},
		Settings:  map[string]interface{}{},
		Aliases:   []string{},
		Version:   1,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	err = store.SaveIndexMetadata("test_index", indexMetadata)
	if err != nil {
		t.Fatalf("Failed to save index metadata: %v", err)
	}

	// 获取新版本
	newVersion, err := store.GetLatestVersion()
	if err != nil {
		t.Fatalf("Failed to get new version: %v", err)
	}

	if newVersion <= initialVersion {
		t.Fatalf("Expected new version > initial version, got %d <= %d", newVersion, initialVersion)
	}
}

func TestMetadataStore_Errors(t *testing.T) {
	config := &metadata.MetadataStoreConfig{
		StorageType: "memory",
	}

	store, err := metadata.NewMetadataStore(config)
	if err != nil {
		t.Fatalf("Failed to create memory metadata store: %v", err)
	}
	defer store.Close()

	// 测试获取不存在的索引
	_, err = store.GetIndexMetadata("nonexistent")
	if err == nil {
		t.Fatal("Expected error when getting nonexistent index")
	}

	if _, ok := err.(*metadata.MetadataNotFoundError); !ok {
		t.Fatalf("Expected MetadataNotFoundError, got %T", err)
	}

	// 测试获取不存在的�?	_, err = store.GetTableMetadata("index", "nonexistent")
	if err == nil {
		t.Fatal("Expected error when getting nonexistent table")
	}

	if _, ok := err.(*metadata.MetadataNotFoundError); !ok {
		t.Fatalf("Expected MetadataNotFoundError, got %T", err)
	}
}

func TestMemoryMetadataStore_RestoreSnapshot(t *testing.T) {
	config := &metadata.MetadataStoreConfig{
		StorageType: "memory",
	}

	store, err := metadata.NewMetadataStore(config)
	if err != nil {
		t.Fatalf("Failed to create memory metadata store: %v", err)
	}
	defer store.Close()

	// 内存存储不支持恢复快照
	err = store.RestoreSnapshot(1)
	if err == nil {
		t.Fatal("Expected error when restoring snapshot in memory store")
	}

	if _, ok := err.(*metadata.UnsupportedOperationError); !ok {
		t.Fatalf("Expected UnsupportedOperationError, got %T", err)
	}
}
