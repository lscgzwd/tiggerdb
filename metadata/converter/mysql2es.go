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

package converter

import (
	"fmt"
	"strings"

	"github.com/lscgzwd/tiggerdb/metadata"
)

// MySQL2ESConverter MySQL到ES转换器
type MySQL2ESConverter struct {
}

// NewMySQL2ESConverter 创建MySQL到ES转换器
func NewMySQL2ESConverter() *MySQL2ESConverter {
	return &MySQL2ESConverter{}
}

// ConvertTable 将MySQL表转换为ES索引
func (c *MySQL2ESConverter) ConvertTable(tableName string, tableMetadata *metadata.TableMetadata) (map[string]interface{}, error) {
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	if tableMetadata == nil {
		return nil, fmt.Errorf("table metadata cannot be nil")
	}

	// 生成索引名（将表名转换为索引名格式）
	indexName := c.convertTableNameToIndexName(tableName)

	// 转换表结构为ES映射
	mapping, err := c.convertTableToESMapping(tableMetadata)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"index":   indexName,
		"mapping": mapping,
	}, nil
}

// convertTableToESMapping 将MySQL表转换为ES映射
func (c *MySQL2ESConverter) convertTableToESMapping(tableMetadata *metadata.TableMetadata) (map[string]interface{}, error) {
	mapping := map[string]interface{}{
		"properties": make(map[string]interface{}),
	}

	properties := mapping["properties"].(map[string]interface{})

	if tableMetadata.Schema != nil {
		for _, column := range tableMetadata.Schema.Columns {
			fieldMapping, err := c.convertColumnToESField(column)
			if err != nil {
				return nil, fmt.Errorf("failed to convert column %s: %w", column.Name, err)
			}
			properties[column.Name] = fieldMapping
		}
	}

	return mapping, nil
}

// convertColumnToESField 将MySQL列转换为ES字段
func (c *MySQL2ESConverter) convertColumnToESField(column *metadata.TableColumn) (map[string]interface{}, error) {
	field := make(map[string]interface{})

	switch strings.ToLower(column.Type) {
	case "varchar", "char", "text", "mediumtext", "longtext":
		field["type"] = "text"
		if column.Length > 0 && column.Length <= 256 {
			field["analyzer"] = "keyword"
		}
	case "int", "integer", "bigint", "tinyint", "smallint":
		field["type"] = "long"
	case "decimal", "numeric", "float", "double":
		field["type"] = "double"
	case "boolean", "bool":
		field["type"] = "boolean"
	case "date":
		field["type"] = "date"
		field["format"] = "yyyy-MM-dd"
	case "datetime", "timestamp":
		field["type"] = "date"
		field["format"] = "yyyy-MM-dd HH:mm:ss"
	case "json":
		field["type"] = "object"
	default:
		field["type"] = "keyword"
	}

	return field, nil
}

// convertTableNameToIndexName 将MySQL表名转换为ES索引名
func (c *MySQL2ESConverter) convertTableNameToIndexName(tableName string) string {
	// 将下划线转换为连字符，并确保以小写字母开头
	indexName := strings.ReplaceAll(tableName, "_", "-")

	// 如果以数字开头，添加前缀
	if len(indexName) > 0 && indexName[0] >= '0' && indexName[0] <= '9' {
		indexName = "idx-" + indexName
	}

	return strings.ToLower(indexName)
}

// ConvertQuery 将MySQL查询转换为ES查询
func (c *MySQL2ESConverter) ConvertQuery(sql string, params []interface{}) (map[string]interface{}, error) {
	// 这里应该实现SQL到ES查询的转换
	// 暂时返回简单的match_all查询
	return map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}, nil
}