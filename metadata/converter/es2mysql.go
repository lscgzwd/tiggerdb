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

// ES2MySQLConverter ES到MySQL转换器
type ES2MySQLConverter struct {
}

// NewES2MySQLConverter 创建ES到MySQL转换器
func NewES2MySQLConverter() *ES2MySQLConverter {
	return &ES2MySQLConverter{}
}

// ConvertMapping 将ES映射转换为MySQL表结构
func (c *ES2MySQLConverter) ConvertMapping(indexName string, esMapping map[string]interface{}) (*metadata.TableMetadata, error) {
	if indexName == "" {
		return nil, fmt.Errorf("index name cannot be empty")
	}

	// 生成表名（将索引名转换为表名格式）
	tableName := c.convertIndexNameToTableName(indexName)

	tableMetadata := &metadata.TableMetadata{
		Name: tableName,
	}

	// 转换映射
	schema, err := c.convertESMappingToSchema(esMapping)
	if err != nil {
		return nil, err
	}

	tableMetadata.Schema = schema
	return tableMetadata, nil
}

// convertESMappingToSchema 将ES映射转换为MySQL Schema
func (c *ES2MySQLConverter) convertESMappingToSchema(esMapping map[string]interface{}) (*metadata.TableSchema, error) {
	schema := &metadata.TableSchema{}

	if esMapping == nil {
		return schema, nil
	}

	properties, ok := esMapping["properties"]
	if !ok {
		return schema, nil
	}

	props, ok := properties.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("properties must be an object")
	}

	// 转换字段
	for fieldName, fieldDef := range props {
		column, err := c.convertESFieldToColumn(fieldName, fieldDef)
		if err != nil {
			return nil, err
		}
		schema.Columns = append(schema.Columns, column)
	}

	return schema, nil
}

// convertESFieldToColumn 将ES字段转换为MySQL列
func (c *ES2MySQLConverter) convertESFieldToColumn(fieldName string, fieldDef interface{}) (*metadata.TableColumn, error) {
	column := &metadata.TableColumn{
		Name: c.convertFieldNameToColumnName(fieldName),
	}

	fieldMap, ok := fieldDef.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("field definition must be an object")
	}

	fieldType, ok := fieldMap["type"]
	if !ok {
		return nil, fmt.Errorf("field type is required")
	}

	typeStr, ok := fieldType.(string)
	if !ok {
		return nil, fmt.Errorf("field type must be a string")
	}

	// 根据ES类型转换为MySQL类型
	switch typeStr {
	case "text", "keyword":
		column.Type = "VARCHAR"
		column.Length = 255 // 默认长度
	case "long":
		column.Type = "BIGINT"
	case "integer":
		column.Type = "INT"
	case "short":
		column.Type = "SMALLINT"
	case "byte":
		column.Type = "TINYINT"
	case "double":
		column.Type = "DOUBLE"
	case "float":
		column.Type = "FLOAT"
	case "boolean":
		column.Type = "TINYINT"
		column.Length = 1
	case "date":
		column.Type = "DATETIME"
	case "binary":
		column.Type = "BLOB"
	default:
		column.Type = "VARCHAR"
		column.Length = 255
	}

	return column, nil
}

// convertIndexNameToTableName 将ES索引名转换为MySQL表名
func (c *ES2MySQLConverter) convertIndexNameToTableName(indexName string) string {
	// 将连字符转换为下划线，并确保以字母开头
	tableName := strings.ReplaceAll(indexName, "-", "_")

	// 如果不以字母开头，添加前缀
	if len(tableName) > 0 && (tableName[0] < 'a' || tableName[0] > 'z') && (tableName[0] < 'A' || tableName[0] > 'Z') {
		tableName = "tbl_" + tableName
	}

	return tableName
}

// convertFieldNameToColumnName 将ES字段名转换为MySQL列名
func (c *ES2MySQLConverter) convertFieldNameToColumnName(fieldName string) string {
	// 将点号转换为下划线
	columnName := strings.ReplaceAll(fieldName, ".", "_")

	// 处理特殊字符
	columnName = strings.ReplaceAll(columnName, "-", "_")
	columnName = strings.ReplaceAll(columnName, " ", "_")

	return columnName
}

// ConvertQuery 将ES查询转换为MySQL WHERE子句
func (c *ES2MySQLConverter) ConvertQuery(esQuery map[string]interface{}) (string, []interface{}, error) {
	return c.convertESQueryToSQL(esQuery, 0)
}

// convertESQueryToSQL 将ES查询转换为SQL
func (c *ES2MySQLConverter) convertESQueryToSQL(query map[string]interface{}, paramIndex int) (string, []interface{}, error) {
	if len(query) == 0 {
		return "1=1", []interface{}{}, nil
	}

	// 这里应该实现完整的ES查询到SQL的转换
	// 暂时返回简单的实现
	return "1=1", []interface{}{}, nil
}

// convertRangeQuery 转换范围查询
func (c *ES2MySQLConverter) convertRangeQuery(columnName string, rangeMap map[string]interface{}) []string {
	var conditions []string

	if min, ok := rangeMap["gte"]; ok {
		if val, ok := min.(string); ok {
			conditions = append(conditions, fmt.Sprintf("%s >= ?", columnName))
			_ = val // 参数值
		}
	}

	if max, ok := rangeMap["lte"]; ok {
		if val, ok := max.(string); ok {
			conditions = append(conditions, fmt.Sprintf("%s <= ?", columnName))
			_ = val // 参数值
		}
	}

	return conditions
}

// convertBoolQuery 转换布尔查询
func (c *ES2MySQLConverter) convertBoolQuery(boolDef map[string]interface{}) (string, []interface{}, error) {
	var conditions []string
	var params []interface{}

	if must, ok := boolDef["must"]; ok {
		if mustArray, ok := must.([]interface{}); ok {
			for _, query := range mustArray {
				if queryMap, ok := query.(map[string]interface{}); ok {
					cond, ps, err := c.convertESQueryToSQL(queryMap, len(params))
					if err != nil {
						return "", nil, err
					}
					conditions = append(conditions, cond)
					params = append(params, ps...)
				}
			}
		}
	}

	if len(conditions) == 0 {
		return "1=1", []interface{}{}, nil
	}

	return strings.Join(conditions, " AND "), params, nil
}
