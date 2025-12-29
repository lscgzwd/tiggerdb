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

package metadata

import (
	"fmt"
	"time"
)

// TableMetadataStore 表元数据存储接口
type TableMetadataStore interface {
	SaveTableMetadata(indexName, tableName string, metadata *TableMetadata) error
	GetTableMetadata(indexName, tableName string) (*TableMetadata, error)
	DeleteTableMetadata(indexName, tableName string) error
	ListTableMetadata(indexName string) ([]*TableMetadata, error)
}

// MetadataStore 元数据存储接口
type MetadataStore interface {
	// 索引元数据操作
	SaveIndexMetadata(indexName string, metadata *IndexMetadata) error
	GetIndexMetadata(indexName string) (*IndexMetadata, error)
	DeleteIndexMetadata(indexName string) error
	ListIndexMetadata() ([]*IndexMetadata, error)

	// 表元数据操作
	TableMetadataStore

	// 版本管理
	GetLatestVersion() (int64, error)
	CreateSnapshot(version int64) error
	RestoreSnapshot(version int64) error

	// 关闭存储
	Close() error
}

// IndexMetadata 索引元数据
type IndexMetadata struct {
	Name          string                 `json:"name"`
	Mapping       map[string]interface{} `json:"mapping"`        // ES mapping
	Settings      map[string]interface{} `json:"settings"`       // 索引设置
	Aliases       []string               `json:"aliases"`        // 索引别名
	JoinRelations *JoinRelations         `json:"join_relations"` // 父子文档关系定义
	Version       int64                  `json:"version"`
	CreatedAt     time.Time              `json:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at"`
}

// JoinRelations 父子文档关系定义
type JoinRelations struct {
	FieldName string              `json:"field_name"` // join 字段名称
	Relations map[string][]string `json:"relations"`  // 父类型 -> 子类型列表
}

// GetParentTypes 获取所有父类型
func (jr *JoinRelations) GetParentTypes() []string {
	if jr == nil || jr.Relations == nil {
		return nil
	}
	types := make([]string, 0, len(jr.Relations))
	for parentType := range jr.Relations {
		types = append(types, parentType)
	}
	return types
}

// GetChildTypes 获取指定父类型的所有子类型
func (jr *JoinRelations) GetChildTypes(parentType string) []string {
	if jr == nil || jr.Relations == nil {
		return nil
	}
	return jr.Relations[parentType]
}

// IsParentType 检查是否为父类型
func (jr *JoinRelations) IsParentType(typeName string) bool {
	if jr == nil || jr.Relations == nil {
		return false
	}
	_, exists := jr.Relations[typeName]
	return exists
}

// IsChildType 检查是否为子类型，并返回其父类型
func (jr *JoinRelations) IsChildType(typeName string) (bool, string) {
	if jr == nil || jr.Relations == nil {
		return false, ""
	}
	for parentType, childTypes := range jr.Relations {
		for _, childType := range childTypes {
			if childType == typeName {
				return true, parentType
			}
		}
	}
	return false, ""
}

// TableMetadata 表元数据
type TableMetadata struct {
	Name        string             `json:"name"`
	Schema      *TableSchema       `json:"schema"`
	Constraints []*TableConstraint `json:"constraints"`
	Indexes     []*TableIndex      `json:"indexes"`
	Version     int64              `json:"version"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
}

// TableSchema 表结构定义
type TableSchema struct {
	Columns []*TableColumn `json:"columns"`
}

// Validate 验证表结构
func (ts *TableSchema) Validate() error {
	if ts == nil {
		return fmt.Errorf("table schema cannot be nil")
	}

	if len(ts.Columns) == 0 {
		return fmt.Errorf("table must have at least one column")
	}

	columnNames := make(map[string]bool)
	for _, column := range ts.Columns {
		if column == nil {
			return fmt.Errorf("column cannot be nil")
		}

		if columnNames[column.Name] {
			return fmt.Errorf("duplicate column name: %s", column.Name)
		}
		columnNames[column.Name] = true

		if err := column.Validate(); err != nil {
			return fmt.Errorf("invalid column %s: %w", column.Name, err)
		}
	}

	return nil
}

// TableColumn 表列定义
type TableColumn struct {
	Name         string      `json:"name"`
	Type         string      `json:"type"` // varchar, int, bigint, float, double, boolean, datetime, text, json
	Length       int         `json:"length,omitempty"`
	Precision    int         `json:"precision,omitempty"`
	Scale        int         `json:"scale,omitempty"`
	Nullable     bool        `json:"nullable"`
	DefaultValue interface{} `json:"default_value,omitempty"`
	Comment      string      `json:"comment,omitempty"`
}

// Validate 验证列定义
func (tc *TableColumn) Validate() error {
	if tc == nil {
		return fmt.Errorf("table column cannot be nil")
	}

	if tc.Name == "" {
		return fmt.Errorf("column name cannot be empty")
	}

	// 验证列名格式（简单检查）
	if len(tc.Name) > 64 {
		return fmt.Errorf("column name too long: %s", tc.Name)
	}

	// 验证列类型
	validTypes := []string{"varchar", "int", "bigint", "float", "double", "boolean", "datetime", "text", "json"}
	isValidType := false
	for _, validType := range validTypes {
		if tc.Type == validType {
			isValidType = true
			break
		}
	}

	if !isValidType {
		return fmt.Errorf("invalid column type: %s", tc.Type)
	}

	// 类型特定的验证
	switch tc.Type {
	case "varchar":
		if tc.Length <= 0 {
			tc.Length = 255 // 默认长度
		}
		if tc.Length > 65535 {
			return fmt.Errorf("varchar length too large: %d", tc.Length)
		}
	}

	return nil
}

// TableConstraint 表约束
type TableConstraint struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"` // PRIMARY_KEY, UNIQUE, FOREIGN_KEY, CHECK
	Columns    []string               `json:"columns"`
	Definition map[string]interface{} `json:"definition"` // 约束的具体定义
}

// Validate 验证约束定义
func (tc *TableConstraint) Validate() error {
	if tc == nil {
		return fmt.Errorf("table constraint cannot be nil")
	}

	if tc.Name == "" {
		return fmt.Errorf("constraint name cannot be empty")
	}

	if tc.Type == "" {
		return fmt.Errorf("constraint type cannot be empty")
	}

	// 验证约束类型
	validTypes := []string{"PRIMARY_KEY", "UNIQUE", "FOREIGN_KEY", "CHECK"}
	isValidType := false
	for _, validType := range validTypes {
		if tc.Type == validType {
			isValidType = true
			break
		}
	}

	if !isValidType {
		return fmt.Errorf("invalid constraint type: %s", tc.Type)
	}

	// 类型特定的验证
	switch tc.Type {
	case "PRIMARY_KEY", "UNIQUE":
		if len(tc.Columns) == 0 {
			return fmt.Errorf("%s constraint must include at least one column", tc.Type)
		}
	case "FOREIGN_KEY":
		if len(tc.Columns) == 0 {
			return fmt.Errorf("foreign key must specify local columns")
		}
		// 检查引用信息
		if tc.Definition == nil {
			return fmt.Errorf("foreign key definition cannot be nil")
		}
	case "CHECK":
		if tc.Definition == nil || tc.Definition["expression"] == nil {
			return fmt.Errorf("check constraint must have an expression")
		}
	}

	return nil
}

// TableIndex 表索引
type TableIndex struct {
	Name       string   `json:"name"`
	Type       string   `json:"type"` // BTREE, HASH, FULLTEXT, SPATIAL
	Columns    []string `json:"columns"`
	IsUnique   bool     `json:"is_unique"`
	IsPrimary  bool     `json:"is_primary"`
	Definition string   `json:"definition"` // 索引定义SQL或表达式
}

// Validate 验证索引定义
func (ti *TableIndex) Validate() error {
	if ti == nil {
		return fmt.Errorf("table index cannot be nil")
	}

	if ti.Name == "" {
		return fmt.Errorf("index name cannot be empty")
	}

	if len(ti.Columns) == 0 {
		return fmt.Errorf("index must include at least one column")
	}

	// 验证索引类型
	validTypes := []string{"BTREE", "HASH", "FULLTEXT", "SPATIAL"}
	isValidType := false
	for _, validType := range validTypes {
		if ti.Type == validType {
			isValidType = true
			break
		}
	}

	if !isValidType {
		return fmt.Errorf("invalid index type: %s", ti.Type)
	}

	// 类型特定的验证
	switch ti.Type {
	case "FULLTEXT":
		if ti.IsUnique {
			return fmt.Errorf("fulltext index cannot be unique")
		}
	case "SPATIAL":
		if ti.IsUnique {
			return fmt.Errorf("spatial index cannot be unique")
		}
		if len(ti.Columns) != 1 {
			return fmt.Errorf("spatial index must have exactly one column")
		}
	}

	// 主键索引的特殊验证
	if ti.IsPrimary {
		if !ti.IsUnique {
			return fmt.Errorf("primary index must be unique")
		}
	}

	return nil
}

// MetadataStoreConfig 元数据存储配置
type MetadataStoreConfig struct {
	// 存储类型：file, memory, database
	StorageType string
	// 文件存储路径（当StorageType为file时）
	FilePath string
	// 是否启用缓存
	EnableCache bool
	// 缓存大小
	CacheSize int
	// 是否启用版本控制
	EnableVersioning bool
}

// DefaultMetadataStoreConfig 返回默认配置
func DefaultMetadataStoreConfig() *MetadataStoreConfig {
	return &MetadataStoreConfig{
		StorageType:      "file",
		EnableCache:      true,
		CacheSize:        1000,
		EnableVersioning: true,
	}
}

// NewMetadataStore 创建元数据存储实例
func NewMetadataStore(config *MetadataStoreConfig) (MetadataStore, error) {
	if config == nil {
		config = DefaultMetadataStoreConfig()
	}

	switch config.StorageType {
	case "file":
		return NewFileMetadataStore(config)
	case "memory":
		return NewMemoryMetadataStore(config)
	default:
		return nil, &UnsupportedStorageTypeError{Type: config.StorageType}
	}
}

// UnsupportedStorageTypeError 不支持的存储类型错误
type UnsupportedStorageTypeError struct {
	Type string
}

func (e *UnsupportedStorageTypeError) Error() string {
	return "unsupported storage type: " + e.Type
}

// MetadataNotFoundError 元数据未找到错误
type MetadataNotFoundError struct {
	ResourceType string
	ResourceName string
}

func (e *MetadataNotFoundError) Error() string {
	return e.ResourceType + " metadata not found: " + e.ResourceName
}

// VersionConflictError 版本冲突错误
type VersionConflictError struct {
	ResourceType   string
	ResourceName   string
	CurrentVersion int64
	NewVersion     int64
}

func (e *VersionConflictError) Error() string {
	return "version conflict for " + e.ResourceType + " " + e.ResourceName +
		": current=" + string(rune(e.CurrentVersion)) + ", new=" + string(rune(e.NewVersion))
}
