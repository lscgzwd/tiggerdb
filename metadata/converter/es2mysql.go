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

	// 添加默认的时间戳列
	if tableMetadata.Schema == nil {
		tableMetadata.Schema = &metadata.TableSchema{}
	}
	if tableMetadata.Schema.Columns == nil {
		tableMetadata.Schema.Columns = []*metadata.TableColumn{}
	}

	// 添加 created_at 和 updated_at 列
	tableMetadata.Schema.Columns = append(tableMetadata.Schema.Columns,
		&metadata.TableColumn{
			Name:     "created_at",
			Type:     "DATETIME",
			Nullable: true,
		},
		&metadata.TableColumn{
			Name:     "updated_at",
			Type:     "DATETIME",
			Nullable: true,
		},
	)

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

	// 根据ES类型转换为MySQL类型（使用小写）
	switch typeStr {
	case "text", "keyword":
		column.Type = "varchar"
		column.Length = 255 // 默认长度
	case "long":
		column.Type = "bigint"
	case "integer":
		column.Type = "int"
	case "short":
		column.Type = "smallint"
	case "byte":
		column.Type = "tinyint"
	case "double":
		column.Type = "double"
	case "float":
		column.Type = "float"
	case "boolean":
		column.Type = "tinyint"
		column.Length = 1
	case "date":
		column.Type = "datetime"
	case "binary":
		column.Type = "blob"
	case "object":
		column.Type = "json"
	case "nested":
		column.Type = "json"
	case "geo_point", "geo_shape":
		column.Type = "json"
	default:
		column.Type = "varchar"
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

	// 处理 match 查询
	if match, ok := query["match"]; ok {
		return c.convertMatchQuery(match)
	}

	// 处理 bool 查询
	if boolQuery, ok := query["bool"]; ok {
		if boolMap, ok := boolQuery.(map[string]interface{}); ok {
			return c.convertBoolQuery(boolMap)
		}
	}

	// 处理 range 查询
	if rangeQuery, ok := query["range"]; ok {
		if rangeMap, ok := rangeQuery.(map[string]interface{}); ok {
			return c.convertRangeQueryToSQL(rangeMap)
		}
	}

	// 默认返回
	return "1=1", []interface{}{}, nil
}

// convertMatchQuery 转换 match 查询
func (c *ES2MySQLConverter) convertMatchQuery(match interface{}) (string, []interface{}, error) {
	matchMap, ok := match.(map[string]interface{})
	if !ok {
		return "1=1", []interface{}{}, nil
	}

	var conditions []string
	var params []interface{}

	for field, value := range matchMap {
		conditions = append(conditions, fmt.Sprintf("%s LIKE ?", field))
		params = append(params, value)
	}

	if len(conditions) == 0 {
		return "1=1", []interface{}{}, nil
	}

	return strings.Join(conditions, " AND "), params, nil
}

// convertRangeQueryToSQL 转换范围查询为SQL
func (c *ES2MySQLConverter) convertRangeQueryToSQL(rangeMap map[string]interface{}) (string, []interface{}, error) {
	var conditions []string
	var params []interface{}

	for field, rangeDef := range rangeMap {
		if rangeDefMap, ok := rangeDef.(map[string]interface{}); ok {
			if min, ok := rangeDefMap["gte"]; ok {
				conditions = append(conditions, fmt.Sprintf("%s >= ?", field))
				params = append(params, min)
			}
			if max, ok := rangeDefMap["lte"]; ok {
				conditions = append(conditions, fmt.Sprintf("%s <= ?", field))
				params = append(params, max)
			}
		}
	}

	if len(conditions) == 0 {
		return "1=1", []interface{}{}, nil
	}

	return strings.Join(conditions, " AND "), params, nil
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
