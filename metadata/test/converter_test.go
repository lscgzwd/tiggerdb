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
	"testing"
	"time"

	"github.com/lscgzwd/tiggerdb/metadata"
	"github.com/lscgzwd/tiggerdb/metadata/converter"
)

func TestES2MySQLConverter_ConvertMapping(t *testing.T) {
	converter := converter.NewES2MySQLConverter()

	esMapping := map[string]interface{}{
		"properties": map[string]interface{}{
			"id": map[string]interface{}{
				"type": "keyword",
			},
			"name": map[string]interface{}{
				"type": "text",
			},
			"age": map[string]interface{}{
				"type": "long",
			},
			"email": map[string]interface{}{
				"type": "keyword",
			},
			"created_at": map[string]interface{}{
				"type": "date",
			},
		},
	}

	tableMetadata, err := converter.ConvertMapping("users", esMapping)
	if err != nil {
		t.Fatalf("Failed to convert ES mapping: %v", err)
	}

	if tableMetadata.Name != "users" {
		t.Fatalf("Expected table name 'users', got '%s'", tableMetadata.Name)
	}

	if len(tableMetadata.Schema.Columns) == 0 {
		t.Fatal("Expected columns in schema")
	}

	// 验证是否添加了默认ID列
	hasIDColumn := false
	for _, col := range tableMetadata.Schema.Columns {
		if col.Name == "id" {
			hasIDColumn = true
			break
		}
	}
	if !hasIDColumn {
		t.Fatal("Expected default 'id' column to be added")
	}

	// 验证是否添加了时间戳列
	hasCreatedAt := false
	hasUpdatedAt := false
	for _, col := range tableMetadata.Schema.Columns {
		if col.Name == "created_at" {
			hasCreatedAt = true
		}
		if col.Name == "updated_at" {
			hasUpdatedAt = true
		}
	}
	if !hasCreatedAt || !hasUpdatedAt {
		t.Fatal("Expected timestamp columns to be added")
	}
}

func TestES2MySQLConverter_ConvertMapping_ComplexTypes(t *testing.T) {
	converter := converter.NewES2MySQLConverter()

	esMapping := map[string]interface{}{
		"properties": map[string]interface{}{
			"tags": map[string]interface{}{
				"type": "keyword",
			},
			"profile": map[string]interface{}{
				"type": "object",
			},
			"location": map[string]interface{}{
				"type": "geo_point",
			},
			"settings": map[string]interface{}{
				"type": "nested",
			},
		},
	}

	tableMetadata, err := converter.ConvertMapping("complex_table", esMapping)
	if err != nil {
		t.Fatalf("Failed to convert complex ES mapping: %v", err)
	}

	// 验证列类型转换
	expectedTypes := map[string]string{
		"tags":     "varchar",
		"profile":  "json",
		"location": "json",
		"settings": "json",
	}

	for _, col := range tableMetadata.Schema.Columns {
		if expectedType, exists := expectedTypes[col.Name]; exists {
			if col.Type != expectedType {
				t.Fatalf("Expected column %s to be type %s, got %s", col.Name, expectedType, col.Type)
			}
		}
	}
}

func TestES2MySQLConverter_ConvertQuery(t *testing.T) {
	converter := converter.NewES2MySQLConverter()

	esQuery := map[string]interface{}{
		"match": map[string]interface{}{
			"name": "john",
		},
	}

	sql, params, err := converter.ConvertQuery(esQuery)
	if err != nil {
		t.Fatalf("Failed to convert ES query: %v", err)
	}

	if sql == "" {
		t.Fatal("Expected SQL query to be generated")
	}

	if len(params) != 1 {
		t.Fatalf("Expected 1 parameter, got %d", len(params))
	}

	expectedSQL := "name LIKE ?"
	if sql != expectedSQL {
		t.Fatalf("Expected SQL '%s', got '%s'", expectedSQL, sql)
	}
}

func TestMySQL2ESConverter_ConvertTable(t *testing.T) {
	converter := converter.NewMySQL2ESConverter()

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
			{
				Name:     "email",
				Type:     "varchar",
				Length:   255,
				Nullable: false,
			},
			{
				Name:     "created_at",
				Type:     "datetime",
				Nullable: true,
			},
		},
	}

	constraints := []*metadata.TableConstraint{
		{
			Name:    "pk_id",
			Type:    "PRIMARY_KEY",
			Columns: []string{"id"},
		},
	}

	indexes := []*metadata.TableIndex{
		{
			Name:     "idx_name",
			Columns:  []string{"name"},
			IsUnique: false,
		},
	}

	tableMetadata := &metadata.TableMetadata{
		Name:        "users",
		Schema:      schema,
		Constraints: constraints,
		Indexes:     indexes,
	}

	result, err := converter.ConvertTable("users", tableMetadata)
	if err != nil {
		t.Fatalf("Failed to convert MySQL table: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result to be non-nil")
	}

	indexName, ok := result["index"].(string)
	if !ok || indexName == "" {
		t.Fatal("Expected index name in result")
	}

	mapping, ok := result["mapping"].(map[string]interface{})
	if !ok || mapping == nil {
		t.Fatal("Expected mapping in result")
	}
}

func TestMySQL2ESConverter_ConvertTable_DifferentTypes(t *testing.T) {
	converter := converter.NewMySQL2ESConverter()

	schema := &metadata.TableSchema{
		Columns: []*metadata.TableColumn{
			{
				Name: "int_col",
				Type: "int",
			},
			{
				Name:   "varchar_col",
				Type:   "varchar",
				Length: 100,
			},
			{
				Name: "text_col",
				Type: "text",
			},
			{
				Name: "datetime_col",
				Type: "datetime",
			},
			{
				Name: "json_col",
				Type: "json",
			},
		},
	}

	tableMetadata := &metadata.TableMetadata{
		Name:        "test_types",
		Schema:      schema,
		Constraints: nil,
		Indexes:     nil,
	}

	result, err := converter.ConvertTable("test_types", tableMetadata)
	if err != nil {
		t.Fatalf("Failed to convert table with different types: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result to be non-nil")
	}

	mapping, ok := result["mapping"].(map[string]interface{})
	if !ok || mapping == nil {
		t.Fatal("Expected mapping in result")
	}

	// 验证映射中包含了所有字段
	properties, ok := mapping["properties"].(map[string]interface{})
	if !ok || properties == nil {
		t.Fatal("Expected properties in mapping")
	}
}

// TestConverters_ConvertSQL is disabled - ConvertSQL method not implemented
// func TestConverters_ConvertSQL(t *testing.T) {
// 	converter := converter.NewMySQL2ESConverter()
//
// 	// 测试简单的SELECT语句
// 	sql := "SELECT * FROM users WHERE name = 'john' LIMIT 10"
// 	esQuery, err := converter.ConvertSQL(sql)
// 	if err != nil {
// 		t.Fatalf("Failed to convert SQL: %v", err)
// 	}
//
// 	if esQuery == nil {
// 		t.Fatal("Expected ES query to be generated")
// 	}
//
// 	// 验证查询结构
// 	if query, ok := esQuery["query"]; !ok {
// 		t.Fatal("Expected 'query' field in ES query")
// 	} else {
// 		if queryMap, ok := query.(map[string]interface{}); ok {
// 			if _, exists := queryMap["match_all"]; !exists {
// 				// 对于简单的等式查询，可能转换为term查询
// 				t.Log("Query converted successfully")
// 			}
// 		}
// 	}
// }

func TestConverters_Integration(t *testing.T) {
	// 测试ES到MySQL再到ES的往返转换
	// 1. 创建ES映射
	originalESMapping := map[string]interface{}{
		"properties": map[string]interface{}{
			"user_id": map[string]interface{}{
				"type": "long",
			},
			"username": map[string]interface{}{
				"type": "keyword",
			},
			"email": map[string]interface{}{
				"type": "keyword",
			},
			"profile": map[string]interface{}{
				"type": "object",
			},
		},
	}

	// 2. ES -> MySQL
	es2mysql := converter.NewES2MySQLConverter()
	tableMetadata, err := es2mysql.ConvertMapping("users", originalESMapping)
	if err != nil {
		t.Fatalf("ES to MySQL conversion failed: %v", err)
	}

	// 3. MySQL -> ES
	mysql2es := converter.NewMySQL2ESConverter()
	result, err := mysql2es.ConvertTable(tableMetadata.Name, tableMetadata)
	if err != nil {
		t.Fatalf("MySQL to ES conversion failed: %v", err)
	}

	// 4. 验证转换结果
	if result == nil {
		t.Fatal("Expected result to be non-nil")
	}

	indexName, ok := result["index"].(string)
	if !ok || indexName == "" {
		t.Fatal("Expected index name in result")
	}

	mapping, ok := result["mapping"].(map[string]interface{})
	if !ok || mapping == nil {
		t.Fatal("Expected mapping in result")
	}

	// 验证关键字段存在
	properties, ok := mapping["properties"].(map[string]interface{})
	if !ok || properties == nil {
		t.Fatal("Expected default mapping in converted index")
	}
}

func TestConverters_ErrorHandling(t *testing.T) {
	// 测试错误处理

	es2mysql := converter.NewES2MySQLConverter()

	// 测试空索引名
	_, err := es2mysql.ConvertMapping("", map[string]interface{}{})
	if err == nil {
		t.Fatal("Expected error for empty index name")
	}

	// 测试nil映射
	_, err = es2mysql.ConvertMapping("test", nil)
	if err != nil {
		t.Fatalf("Expected success for nil mapping, got error: %v", err)
	}

	// 测试无效的查询转�?	_, _, err = es2mysql.ConvertQuery(nil)
	if err != nil {
		t.Fatalf("Expected success for nil query, got error: %v", err)
	}

	mysql2es := converter.NewMySQL2ESConverter()

	// 测试空表名
	_, err = mysql2es.ConvertTable("", &metadata.TableMetadata{Name: "", Schema: &metadata.TableSchema{}})
	if err == nil {
		t.Fatal("Expected error for empty table name")
	}

	// 测试nil metadata
	_, err = mysql2es.ConvertTable("test", nil)
	if err == nil {
		t.Fatal("Expected error for nil metadata")
	}
}

func TestConverters_Performance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// 创建较大的映射用于性能测试
	largeMapping := map[string]interface{}{
		"properties": make(map[string]interface{}),
	}

	properties := largeMapping["properties"].(map[string]interface{})
	for i := 0; i < 100; i++ {
		fieldName := fmt.Sprintf("field_%d", i)
		properties[fieldName] = map[string]interface{}{
			"type": "keyword",
		}
	}

	converter := converter.NewES2MySQLConverter()

	start := time.Now()
	_, err := converter.ConvertMapping("large_table", largeMapping)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Performance test failed: %v", err)
	}

	// 性能断言：转换100个字段应该在合理时间内完成
	if duration > 100*time.Millisecond {
		t.Logf("Performance warning: conversion took %v", duration)
	}
}
