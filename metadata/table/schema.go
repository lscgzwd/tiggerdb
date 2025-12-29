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

package table

import (
	"fmt"
	"strings"

	"github.com/lscgzwd/tiggerdb/metadata"
)

// SchemaManager MySQL Schema管理器
type SchemaManager struct {
}

// NewSchemaManager 创建Schema管理器
func NewSchemaManager() *SchemaManager {
	return &SchemaManager{}
}

// Validate 验证表结构
func (sm *SchemaManager) Validate(schema *metadata.TableSchema) error {
	if schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}

	if len(schema.Columns) == 0 {
		return fmt.Errorf("schema must have at least one column")
	}

	columnNames := make(map[string]bool)
	primaryKeyColumns := []string{}

	for _, column := range schema.Columns {
		// 验证列定义
		if err := sm.validateColumn(column); err != nil {
			return fmt.Errorf("invalid column %s: %w", column.Name, err)
		}

		// 检查列名重复
		if columnNames[column.Name] {
			return fmt.Errorf("duplicate column name: %s", column.Name)
		}
		columnNames[column.Name] = true

		// 收集主键列
		// 这里需要扩展TableConstraint来识别主键约束
		// 暂时跳过
	}

	// 验证主键约束
	if len(primaryKeyColumns) == 0 {
		return fmt.Errorf("table must have a primary key")
	}

	return nil
}

// validateColumn 验证列定义
func (sm *SchemaManager) validateColumn(column *metadata.TableColumn) error {
	if column.Name == "" {
		return fmt.Errorf("column name cannot be empty")
	}

	// 验证列名格式
	if err := sm.validateColumnName(column.Name); err != nil {
		return err
	}

	// 验证列类型
	if err := sm.validateColumnType(column); err != nil {
		return err
	}

	// 验证默认值
	if err := sm.validateDefaultValue(column); err != nil {
		return err
	}

	// 验证长度约束
	if err := sm.validateLengthConstraints(column); err != nil {
		return err
	}

	return nil
}

// validateColumnName 验证列名
func (sm *SchemaManager) validateColumnName(name string) error {
	if len(name) > 64 {
		return fmt.Errorf("column name too long (max 64 characters)")
	}

	// 检查是否为保留字（简化版本）
	reservedWords := []string{"select", "insert", "update", "delete", "create", "drop", "alter", "table", "index"}
	for _, word := range reservedWords {
		if strings.ToLower(name) == word {
			return fmt.Errorf("column name cannot be reserved word: %s", name)
		}
	}

	// 检查字符合法性
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_') {
			return fmt.Errorf("column name contains invalid character: %c", r)
		}
	}

	return nil
}

// validateColumnType 验证列类型
func (sm *SchemaManager) validateColumnType(column *metadata.TableColumn) error {
	switch strings.ToLower(column.Type) {
	case "varchar", "char":
		if column.Length <= 0 || column.Length > 65535 {
			return fmt.Errorf("varchar length must be between 1 and 65535")
		}
	case "text", "mediumtext", "longtext":
		// text类型不需要指定长度
	case "int", "integer":
		if column.Length != 0 && (column.Length < 1 || column.Length > 11) {
			return fmt.Errorf("int display width must be between 1 and 11")
		}
	case "bigint":
		if column.Length != 0 && (column.Length < 1 || column.Length > 20) {
			return fmt.Errorf("bigint display width must be between 1 and 20")
		}
	case "tinyint":
		if column.Length != 0 && (column.Length < 1 || column.Length > 4) {
			return fmt.Errorf("tinyint display width must be between 1 and 4")
		}
	case "smallint":
		if column.Length != 0 && (column.Length < 1 || column.Length > 6) {
			return fmt.Errorf("smallint display width must be between 1 and 6")
		}
	case "decimal", "numeric":
		if column.Precision <= 0 || column.Precision > 65 {
			return fmt.Errorf("decimal precision must be between 1 and 65")
		}
		if column.Scale < 0 || column.Scale > 30 {
			return fmt.Errorf("decimal scale must be between 0 and 30")
		}
		if column.Scale > column.Precision {
			return fmt.Errorf("decimal scale cannot be greater than precision")
		}
	case "float", "double":
		// float/double 不需要额外的验证
	case "boolean", "bool":
		// boolean 类型
	case "date", "datetime", "timestamp":
		// 时间类型
	case "json":
		// JSON类型
	default:
		return fmt.Errorf("unsupported column type: %s", column.Type)
	}

	return nil
}

// validateDefaultValue 验证默认值
func (sm *SchemaManager) validateDefaultValue(column *metadata.TableColumn) error {
	if column.DefaultValue == nil {
		return nil // NULL默认值是允许的
	}

	// 根据列类型验证默认值
	switch strings.ToLower(column.Type) {
	case "varchar", "char", "text", "mediumtext", "longtext":
		if _, ok := column.DefaultValue.(string); !ok {
			return fmt.Errorf("default value must be a string for text types")
		}
	case "int", "integer", "bigint", "tinyint", "smallint":
		switch column.DefaultValue.(type) {
		case int, int32, int64, float32, float64:
			// 数字类型
		default:
			return fmt.Errorf("default value must be a number for integer types")
		}
	case "decimal", "numeric", "float", "double":
		switch column.DefaultValue.(type) {
		case int, int32, int64, float32, float64:
			// 数字类型
		default:
			return fmt.Errorf("default value must be a number for decimal/float types")
		}
	case "boolean", "bool":
		if _, ok := column.DefaultValue.(bool); !ok {
			return fmt.Errorf("default value must be a boolean")
		}
	case "date", "datetime", "timestamp":
		if _, ok := column.DefaultValue.(string); !ok {
			return fmt.Errorf("default value must be a string for date/time types")
		}
	case "json":
		// JSON类型可以是任何值
	default:
		return fmt.Errorf("cannot validate default value for unsupported type: %s", column.Type)
	}

	return nil
}

// validateLengthConstraints 验证长度约束
func (sm *SchemaManager) validateLengthConstraints(column *metadata.TableColumn) error {
	switch strings.ToLower(column.Type) {
	case "varchar", "char":
		if column.Length == 0 {
			return fmt.Errorf("length is required for varchar/char types")
		}
	case "decimal", "numeric":
		if column.Precision == 0 {
			return fmt.Errorf("precision is required for decimal/numeric types")
		}
	}

	return nil
}

// GenerateCreateTableSQL 生成创建表的SQL语句
func (sm *SchemaManager) GenerateCreateTableSQL(indexName, tableName string, schema *metadata.TableSchema, constraints []*metadata.TableConstraint, indexes []*metadata.TableIndex) (string, error) {
	if schema == nil {
		return "", fmt.Errorf("schema cannot be nil")
	}

	var sql strings.Builder
	sql.WriteString(fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n", indexName, tableName))

	// 生成列定义
	for i, column := range schema.Columns {
		columnSQL, err := sm.generateColumnSQL(column)
		if err != nil {
			return "", fmt.Errorf("failed to generate column SQL for %s: %w", column.Name, err)
		}

		sql.WriteString("  ")
		sql.WriteString(columnSQL)

		if i < len(schema.Columns)-1 || len(constraints) > 0 {
			sql.WriteString(",")
		}
		sql.WriteString("\n")
	}

	// 生成约束定义
	for i, constraint := range constraints {
		constraintSQL, err := sm.generateConstraintSQL(constraint)
		if err != nil {
			return "", fmt.Errorf("failed to generate constraint SQL for %s: %w", constraint.Name, err)
		}

		sql.WriteString("  ")
		sql.WriteString(constraintSQL)

		if i < len(constraints)-1 {
			sql.WriteString(",")
		}
		sql.WriteString("\n")
	}

	sql.WriteString(")")

	// 生成索引定义（表外索引）
	if len(indexes) > 0 {
		sql.WriteString(";\n")
		for _, index := range indexes {
			indexSQL, err := sm.generateIndexSQL(indexName, tableName, index)
			if err != nil {
				return "", fmt.Errorf("failed to generate index SQL for %s: %w", index.Name, err)
			}
			sql.WriteString(indexSQL)
			sql.WriteString(";\n")
		}
	} else {
		sql.WriteString(";")
	}

	return sql.String(), nil
}

// generateColumnSQL 生成列的SQL定义
func (sm *SchemaManager) generateColumnSQL(column *metadata.TableColumn) (string, error) {
	var sql strings.Builder

	// 列名
	sql.WriteString(fmt.Sprintf("`%s` ", column.Name))

	// 列类型
	typeSQL, err := sm.generateColumnTypeSQL(column)
	if err != nil {
		return "", err
	}
	sql.WriteString(typeSQL)

	// NULL/NOT NULL
	if !column.Nullable {
		sql.WriteString(" NOT NULL")
	}

	// 默认值
	if column.DefaultValue != nil {
		defaultSQL, err := sm.generateDefaultValueSQL(column.DefaultValue)
		if err != nil {
			return "", err
		}
		sql.WriteString(defaultSQL)
	}

	// 注释
	if column.Comment != "" {
		sql.WriteString(fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(column.Comment, "'", "''")))
	}

	return sql.String(), nil
}

// generateColumnTypeSQL 生成列类型的SQL
func (sm *SchemaManager) generateColumnTypeSQL(column *metadata.TableColumn) (string, error) {
	switch strings.ToLower(column.Type) {
	case "varchar":
		return fmt.Sprintf("VARCHAR(%d)", column.Length), nil
	case "char":
		return fmt.Sprintf("CHAR(%d)", column.Length), nil
	case "text":
		return "TEXT", nil
	case "mediumtext":
		return "MEDIUMTEXT", nil
	case "longtext":
		return "LONGTEXT", nil
	case "int", "integer":
		if column.Length > 0 {
			return fmt.Sprintf("INT(%d)", column.Length), nil
		}
		return "INT", nil
	case "bigint":
		if column.Length > 0 {
			return fmt.Sprintf("BIGINT(%d)", column.Length), nil
		}
		return "BIGINT", nil
	case "tinyint":
		if column.Length > 0 {
			return fmt.Sprintf("TINYINT(%d)", column.Length), nil
		}
		return "TINYINT", nil
	case "smallint":
		if column.Length > 0 {
			return fmt.Sprintf("SMALLINT(%d)", column.Length), nil
		}
		return "SMALLINT", nil
	case "decimal", "numeric":
		return fmt.Sprintf("DECIMAL(%d,%d)", column.Precision, column.Scale), nil
	case "float":
		return "FLOAT", nil
	case "double":
		return "DOUBLE", nil
	case "boolean", "bool":
		return "TINYINT(1)", nil
	case "date":
		return "DATE", nil
	case "datetime":
		return "DATETIME", nil
	case "timestamp":
		return "TIMESTAMP", nil
	case "json":
		return "JSON", nil
	default:
		return "", fmt.Errorf("unsupported column type: %s", column.Type)
	}
}

// generateDefaultValueSQL 生成默认值的SQL
func (sm *SchemaManager) generateDefaultValueSQL(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf(" DEFAULT '%s'", strings.ReplaceAll(v, "'", "''")), nil
	case int, int32, int64:
		return fmt.Sprintf(" DEFAULT %d", v), nil
	case float32, float64:
		return fmt.Sprintf(" DEFAULT %g", v), nil
	case bool:
		if v {
			return " DEFAULT 1", nil
		}
		return " DEFAULT 0", nil
	default:
		return "", fmt.Errorf("unsupported default value type: %T", value)
	}
}

// generateConstraintSQL 生成约束的SQL定义
func (sm *SchemaManager) generateConstraintSQL(constraint *metadata.TableConstraint) (string, error) {
	switch strings.ToUpper(constraint.Type) {
	case "PRIMARY_KEY":
		columns := sm.quoteColumnNames(constraint.Columns)
		return fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(columns, ", ")), nil
	case "UNIQUE":
		columns := sm.quoteColumnNames(constraint.Columns)
		return fmt.Sprintf("UNIQUE KEY `%s` (%s)", constraint.Name, strings.Join(columns, ", ")), nil
	case "FOREIGN_KEY":
		// 这里需要扩展constraint.Definition来包含引用表信息
		// 暂时简化实现
		columns := sm.quoteColumnNames(constraint.Columns)
		return fmt.Sprintf("FOREIGN KEY `%s` (%s) REFERENCES ...", constraint.Name, strings.Join(columns, ", ")), nil
	case "CHECK":
		// 检查约束
		definition, ok := constraint.Definition["expression"].(string)
		if !ok {
			return "", fmt.Errorf("check constraint must have expression")
		}
		return fmt.Sprintf("CHECK (%s)", definition), nil
	default:
		return "", fmt.Errorf("unsupported constraint type: %s", constraint.Type)
	}
}

// generateIndexSQL 生成索引的SQL定义
func (sm *SchemaManager) generateIndexSQL(indexName, tableName string, index *metadata.TableIndex) (string, error) {
	var sql strings.Builder

	if index.IsPrimary {
		// 主键索引已经内联定义
		return "", nil
	}

	if index.IsUnique {
		sql.WriteString("CREATE UNIQUE INDEX ")
	} else {
		sql.WriteString("CREATE INDEX ")
	}

	sql.WriteString(fmt.Sprintf("`%s` ON `%s`.`%s` ", index.Name, indexName, tableName))

	columns := sm.quoteColumnNames(index.Columns)
	sql.WriteString(fmt.Sprintf("(%s)", strings.Join(columns, ", ")))

	return sql.String(), nil
}

// quoteColumnNames 为列名添加反引号
func (sm *SchemaManager) quoteColumnNames(columns []string) []string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = "`" + col + "`"
	}
	return quoted
}

// ConvertToESMapping 将MySQL Schema转换为ES Mapping
func (sm *SchemaManager) ConvertToESMapping(schema *metadata.TableSchema) (map[string]interface{}, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	mapping := map[string]interface{}{
		"properties": make(map[string]interface{}),
	}

	properties := mapping["properties"].(map[string]interface{})

	for _, column := range schema.Columns {
		fieldMapping, err := sm.convertColumnToESField(column)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %s to ES field: %w", column.Name, err)
		}
		properties[column.Name] = fieldMapping
	}

	return mapping, nil
}

// convertColumnToESField 将MySQL列转换为ES字段映射
func (sm *SchemaManager) convertColumnToESField(column *metadata.TableColumn) (map[string]interface{}, error) {
	field := make(map[string]interface{})

	switch strings.ToLower(column.Type) {
	case "varchar", "char", "text", "mediumtext", "longtext":
		field["type"] = "text"
		if column.Length > 0 && column.Length <= 256 {
			field["analyzer"] = "keyword" // 短文本使用keyword分析器
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
		field["type"] = "keyword" // 默认作为keyword处理
	}

	// 设置是否可索引
	if !column.Nullable {
		field["index"] = true
	}

	// 设置是否存储
	field["store"] = true // 默认存储，实际可根据需要调整

	return field, nil
}
