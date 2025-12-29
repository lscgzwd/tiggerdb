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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/lscgzwd/tiggerdb/directory"
)

func TestNewDirectoryManager(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := directory.DefaultDirectoryConfig(tempDir)

	manager, err := directory.NewDirectoryManager(config)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer manager.Cleanup()

	if manager == nil {
		t.Fatal("Directory manager is nil")
	}
}

func TestNewDirectoryManager_InvalidConfig(t *testing.T) {
	// 测试无效配置
	manager, err := directory.NewDirectoryManager(nil)
	if err == nil {
		t.Fatal("Expected error for nil config")
	}
	if manager != nil {
		t.Fatal("Manager should be nil for invalid config")
	}
}

func TestDirectoryManager_CreateAndDeleteIndex(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := directory.DefaultDirectoryConfig(tempDir)
	manager, err := directory.NewDirectoryManager(config)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer manager.Cleanup()

	indexName := "test_index"

	// 测试创建索引
	err = manager.CreateIndex(indexName)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// 验证索引存在
	if !manager.IndexExists(indexName) {
		t.Fatal("Index should exist after creation")
	}

	// 验证索引路径存在
	indexPath := manager.GetIndexPath(indexName)
	if indexPath == "" {
		t.Fatal("Index path should not be empty")
	}
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Fatalf("Index directory should exist: %s", indexPath)
	}

	// 测试重复创建应该失败
	err = manager.CreateIndex(indexName)
	if err == nil {
		t.Fatal("Creating existing index should fail")
	}

	// 测试删除索引
	err = manager.DeleteIndex(indexName)
	if err != nil {
		t.Fatalf("Failed to delete index: %v", err)
	}

	// 验证索引不存�?	if manager.IndexExists(indexName) {
		t.Fatal("Index should not exist after deletion")
	}
}

func TestDirectoryManager_IndexOperations_EdgeCases(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := directory.DefaultDirectoryConfig(tempDir)
	manager, err := directory.NewDirectoryManager(config)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer manager.Cleanup()

	// 测试空索引名
	err = manager.CreateIndex("")
	if err == nil {
		t.Fatal("Creating index with empty name should fail")
	}

	// 测试无效索引�?	invalidNames := []string{".hidden", "invalid.name", "invalid/name", "invalid name"}
	for _, name := range invalidNames {
		err = manager.CreateIndex(name)
		if err == nil {
			t.Fatalf("Creating index with invalid name should fail: %s", name)
		}
	}

	// 测试删除不存在的索引
	err = manager.DeleteIndex("nonexistent")
	if err == nil {
		t.Fatal("Deleting nonexistent index should fail")
	}

	// 测试检查不存在的索�?	if manager.IndexExists("nonexistent") {
		t.Fatal("Nonexistent index should not exist")
	}
}

func TestDirectoryManager_CreateAndDeleteTable(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := directory.DefaultDirectoryConfig(tempDir)
	manager, err := directory.NewDirectoryManager(config)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer manager.Cleanup()

	indexName := "test_index"
	tableName := "test_table"

	// 先创建索�?	err = manager.CreateIndex(indexName)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// 测试创建�?	err = manager.CreateTable(indexName, tableName)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 验证表存�?	if !manager.TableExists(indexName, tableName) {
		t.Fatal("Table should exist after creation")
	}

	// 验证表路径存�?	tablePath := manager.GetTablePath(indexName, tableName)
	if tablePath == "" {
		t.Fatal("Table path should not be empty")
	}
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		t.Fatalf("Table directory should exist: %s", tablePath)
	}

	// 测试重复创建应该失败
	err = manager.CreateTable(indexName, tableName)
	if err == nil {
		t.Fatal("Creating existing table should fail")
	}

	// 测试删除�?	err = manager.DeleteTable(indexName, tableName)
	if err != nil {
		t.Fatalf("Failed to delete table: %v", err)
	}

	// 验证表不存在
	if manager.TableExists(indexName, tableName) {
		t.Fatal("Table should not exist after deletion")
	}
}

func TestDirectoryManager_TableOperations_EdgeCases(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := directory.DefaultDirectoryConfig(tempDir)
	manager, err := directory.NewDirectoryManager(config)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer manager.Cleanup()

	// 创建测试索引
	indexName := "test_index"
	err = manager.CreateIndex(indexName)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// 测试空表�?	err = manager.CreateTable(indexName, "")
	if err == nil {
		t.Fatal("Creating table with empty name should fail")
	}

	// 测试不存在的索引
	err = manager.CreateTable("nonexistent", "table")
	if err == nil {
		t.Fatal("Creating table in nonexistent index should fail")
	}

	// 测试删除不存在的�?	err = manager.DeleteTable(indexName, "nonexistent")
	if err == nil {
		t.Fatal("Deleting nonexistent table should fail")
	}

	// 测试检查不存在的表
	if manager.TableExists(indexName, "nonexistent") {
		t.Fatal("Nonexistent table should not exist")
	}
}

func TestDirectoryManager_ListOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := directory.DefaultDirectoryConfig(tempDir)
	manager, err := directory.NewDirectoryManager(config)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer manager.Cleanup()

	// 测试列出空索引列�?	indices, err := manager.ListIndices()
	if err != nil {
		t.Fatalf("Failed to list indices: %v", err)
	}
	if len(indices) != 0 {
		t.Fatalf("Expected 0 indices, got %d", len(indices))
	}

	// 创建一些索�?	indexNames := []string{"index1", "index2", "index3"}
	for _, name := range indexNames {
		err = manager.CreateIndex(name)
		if err != nil {
			t.Fatalf("Failed to create index %s: %v", name, err)
		}
	}

	// 验证索引列表
	indices, err = manager.ListIndices()
	if err != nil {
		t.Fatalf("Failed to list indices: %v", err)
	}
	if len(indices) != len(indexNames) {
		t.Fatalf("Expected %d indices, got %d", len(indexNames), len(indices))
	}

	// 验证所有索引都在列表中
	for _, expected := range indexNames {
		found := false
		for _, actual := range indices {
			if actual == expected {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Index %s not found in list", expected)
		}
	}

	// 测试列出�?	tables, err := manager.ListTables(indexNames[0])
	if err != nil {
		t.Fatalf("Failed to list tables: %v", err)
	}
	if len(tables) != 0 {
		t.Fatalf("Expected 0 tables, got %d", len(tables))
	}

	// 创建一些表
	tableNames := []string{"table1", "table2"}
	for _, name := range tableNames {
		err = manager.CreateTable(indexNames[0], name)
		if err != nil {
			t.Fatalf("Failed to create table %s: %v", name, err)
		}
	}

	// 验证表列�?	tables, err = manager.ListTables(indexNames[0])
	if err != nil {
		t.Fatalf("Failed to list tables: %v", err)
	}
	if len(tables) != len(tableNames) {
		t.Fatalf("Expected %d tables, got %d", len(tableNames), len(tables))
	}
}

func TestDirectoryManager_PathGeneration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := directory.DefaultDirectoryConfig(tempDir)
	manager, err := directory.NewDirectoryManager(config)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer manager.Cleanup()

	indexName := "test_index"
	tableName := "test_table"

	// 测试索引路径
	indexPath := manager.GetIndexPath(indexName)
	expectedIndexPath := filepath.Join(tempDir, "indices", indexName)
	if indexPath != expectedIndexPath {
		t.Fatalf("Expected index path %s, got %s", expectedIndexPath, indexPath)
	}

	// 测试表路�?	tablePath := manager.GetTablePath(indexName, tableName)
	expectedTablePath := filepath.Join(tempDir, "indices", indexName, "tables", tableName)
	if tablePath != expectedTablePath {
		t.Fatalf("Expected table path %s, got %s", expectedTablePath, tablePath)
	}

	// 测试数据路径
	dataPath := manager.GetIndexDataPath(indexName)
	expectedDataPath := filepath.Join(tempDir, "indices", indexName, "data")
	if dataPath != expectedDataPath {
		t.Fatalf("Expected data path %s, got %s", expectedDataPath, dataPath)
	}

	// 测试元数据路�?	metadataPath := manager.GetIndexMetadataPath(indexName)
	expectedMetadataPath := filepath.Join(tempDir, "indices", indexName, "metadata.json")
	if metadataPath != expectedMetadataPath {
		t.Fatalf("Expected metadata path %s, got %s", expectedMetadataPath, metadataPath)
	}
}

func TestDirectoryManager_InvalidPathNames(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := directory.DefaultDirectoryConfig(tempDir)
	manager, err := directory.NewDirectoryManager(config)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer manager.Cleanup()

	// 测试无效路径名应该返回空字符�?	invalidNames := []string{
		"",
		".hidden",
		"invalid.name",
		"invalid/name",
		"invalid name",
		"a_very_long_name_that_exceeds_the_maximum_length_limit_of_255_characters_and_should_be_rejected_by_the_validation_function_but_is_actually_shorter_than_expected_so_we_need_to_make_it_even_longer_by_adding_more_text_until_it_reaches_256_characters_or_more_to_properly_test_the_length_validation_functionality" +
			"_and_this_additional_text_should_make_it_long_enough_to_exceed_the_limit_and_be_rejected_by_the_validation_function_as_expected_by_the_test_case_design_and_requirements",
	}
	for _, name := range invalidNames {
		if manager.GetIndexPath(name) != "" {
			t.Fatalf("Invalid name should return empty path: %s", name)
		}
	}
}

func TestDirectoryManager_ConfigLimits(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建限制�?个索引的配置
	config := directory.DefaultDirectoryConfig(tempDir)
	config.MaxIndices = 1

	manager, err := directory.NewDirectoryManager(config)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer manager.Cleanup()

	// 创建第一个索引应该成�?	err = manager.CreateIndex("index1")
	if err != nil {
		t.Fatalf("Failed to create first index: %v", err)
	}

	// 创建第二个索引应该失�?	err = manager.CreateIndex("index2")
	if err == nil {
		t.Fatal("Creating second index should fail due to limit")
	}
}

func TestDirectoryManager_Stats(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := directory.DefaultDirectoryConfig(tempDir)
	manager, err := directory.NewDirectoryManager(config)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer manager.Cleanup()

	// 获取空统计信�?	stats, err := manager.GetStats()
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}

	if stats.TotalIndices != 0 {
		t.Fatalf("Expected 0 total indices, got %d", stats.TotalIndices)
	}

	// 创建一些索引和�?	err = manager.CreateIndex("index1")
	if err != nil {
		t.Fatalf("Failed to create index1: %v", err)
	}

	err = manager.CreateIndex("index2")
	if err != nil {
		t.Fatalf("Failed to create index2: %v", err)
	}

	err = manager.CreateTable("index1", "table1")
	if err != nil {
		t.Fatalf("Failed to create table1: %v", err)
	}

	// 获取统计信息
	stats, err = manager.GetStats()
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}

	if stats.TotalIndices != 2 {
		t.Fatalf("Expected 2 total indices, got %d", stats.TotalIndices)
	}

	if stats.TotalTables != 1 {
		t.Fatalf("Expected 1 total table, got %d", stats.TotalTables)
	}

	// 验证索引统计
	if len(stats.Indices) != 2 {
		t.Fatalf("Expected 2 index stats, got %d", len(stats.Indices))
	}
}

func TestDirectoryManager_ConcurrentOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := directory.DefaultDirectoryConfig(tempDir)
	config.MaxIndices = 10 // 允许更多索引用于并发测试

	manager, err := directory.NewDirectoryManager(config)
	if err != nil {
		t.Fatalf("Failed to create directory manager: %v", err)
	}
	defer manager.Cleanup()

	// 并发创建索引
	done := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		go func(id int) {
			indexName := fmt.Sprintf("concurrent_index_%d", id)
			err := manager.CreateIndex(indexName)
			if err != nil {
				t.Errorf("Failed to create concurrent index %s: %v", indexName, err)
			}
			done <- true
		}(i)
	}

	// 等待所有协程完�?	for i := 0; i < 5; i++ {
		<-done
	}

	// 验证所有索引都创建成功
	indices, err := manager.ListIndices()
	if err != nil {
		t.Fatalf("Failed to list indices: %v", err)
	}

	if len(indices) != 5 {
		t.Fatalf("Expected 5 indices, got %d", len(indices))
	}
}
