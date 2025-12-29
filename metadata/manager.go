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

// TableMetadataManager 表元数据管理器
type TableMetadataManager struct {
	store MetadataStore
}

// NewTableMetadataManager 创建表元数据管理器
func NewTableMetadataManager(store MetadataStore) *TableMetadataManager {
	return &TableMetadataManager{
		store: store,
	}
}

// CreateTable 创建表元数据
func (tmm *TableMetadataManager) CreateTable(indexName, tableName string, schema *TableSchema) (*TableMetadata, error) {
	if indexName == "" || tableName == "" {
		return nil, fmt.Errorf("index name and table name cannot be empty")
	}

	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	// 验证schema
	if err := schema.Validate(); err != nil {
		return nil, fmt.Errorf("invalid schema: %w", err)
	}

	existing, err := tmm.store.GetTableMetadata(indexName, tableName)
	if err == nil && existing != nil {
		return nil, fmt.Errorf("table %s/%s already exists", indexName, tableName)
	}

	metadata := &TableMetadata{
		Name:        tableName,
		Schema:      schema,
		Constraints: []*TableConstraint{},
		Indexes:     []*TableIndex{},
		Version:     1,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	err = tmm.store.SaveTableMetadata(indexName, tableName, metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to save table metadata: %w", err)
	}

	return metadata, nil
}

// GetTable 获取表元数据
func (tmm *TableMetadataManager) GetTable(indexName, tableName string) (*TableMetadata, error) {
	return tmm.store.GetTableMetadata(indexName, tableName)
}

// UpdateTable 更新表元数据
func (tmm *TableMetadataManager) UpdateTable(indexName, tableName string, updates *TableMetadataUpdate) error {
	existing, err := tmm.store.GetTableMetadata(indexName, tableName)
	if err != nil {
		return fmt.Errorf("failed to get existing table metadata: %w", err)
	}

	// 应用更新
	if updates.Schema != nil {
		if err := updates.Schema.Validate(); err != nil {
			return fmt.Errorf("invalid schema update: %w", err)
		}
		existing.Schema = updates.Schema
	}

	if updates.Constraints != nil {
		existing.Constraints = updates.Constraints
	}

	if updates.Indexes != nil {
		existing.Indexes = updates.Indexes
	}

	existing.Version++
	existing.UpdatedAt = time.Now()

	err = tmm.store.SaveTableMetadata(indexName, tableName, existing)
	if err != nil {
		return fmt.Errorf("failed to save updated table metadata: %w", err)
	}

	return nil
}

// DeleteTable 删除表元数据
func (tmm *TableMetadataManager) DeleteTable(indexName, tableName string) error {
	return tmm.store.DeleteTableMetadata(indexName, tableName)
}

// ListTables 列出索引中的所有表
func (tmm *TableMetadataManager) ListTables(indexName string) ([]*TableMetadata, error) {
	return tmm.store.ListTableMetadata(indexName)
}

// AddColumn 添加列
func (tmm *TableMetadataManager) AddColumn(indexName, tableName string, column *TableColumn) error {
	if column == nil {
		return fmt.Errorf("column cannot be nil")
	}

	if err := column.Validate(); err != nil {
		return fmt.Errorf("invalid column: %w", err)
	}

	metadata, err := tmm.store.GetTableMetadata(indexName, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}

	// 检查列名是否已存在
	for _, existingCol := range metadata.Schema.Columns {
		if existingCol.Name == column.Name {
			return fmt.Errorf("column %s already exists", column.Name)
		}
	}

	metadata.Schema.Columns = append(metadata.Schema.Columns, column)
	metadata.Version++
	metadata.UpdatedAt = time.Now()

	err = tmm.store.SaveTableMetadata(indexName, tableName, metadata)
	if err != nil {
		return fmt.Errorf("failed to save table metadata with new column: %w", err)
	}

	return nil
}

// DropColumn 删除列
func (tmm *TableMetadataManager) DropColumn(indexName, tableName, columnName string) error {
	metadata, err := tmm.store.GetTableMetadata(indexName, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}

	// 查找并删除列
	for i, col := range metadata.Schema.Columns {
		if col.Name == columnName {
			metadata.Schema.Columns = append(metadata.Schema.Columns[:i], metadata.Schema.Columns[i+1:]...)
			metadata.Version++
			metadata.UpdatedAt = time.Now()

			err = tmm.store.SaveTableMetadata(indexName, tableName, metadata)
			if err != nil {
				return fmt.Errorf("failed to save table metadata after column removal: %w", err)
			}
			return nil
		}
	}

	return fmt.Errorf("column %s not found", columnName)
}

// AddConstraint 添加约束
func (tmm *TableMetadataManager) AddConstraint(indexName, tableName string, constraint *TableConstraint) error {
	if constraint == nil {
		return fmt.Errorf("constraint cannot be nil")
	}

	if err := constraint.Validate(); err != nil {
		return fmt.Errorf("invalid constraint: %w", err)
	}

	metadata, err := tmm.store.GetTableMetadata(indexName, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}

	// 检查约束名是否已存在
	for _, existingConstraint := range metadata.Constraints {
		if existingConstraint.Name == constraint.Name {
			return fmt.Errorf("constraint %s already exists", constraint.Name)
		}
	}

	metadata.Constraints = append(metadata.Constraints, constraint)
	metadata.Version++
	metadata.UpdatedAt = time.Now()

	err = tmm.store.SaveTableMetadata(indexName, tableName, metadata)
	if err != nil {
		return fmt.Errorf("failed to save table metadata with new constraint: %w", err)
	}

	return nil
}

// DropConstraint 删除约束
func (tmm *TableMetadataManager) DropConstraint(indexName, tableName, constraintName string) error {
	metadata, err := tmm.store.GetTableMetadata(indexName, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}

	// 查找并删除约束
	for i, constraint := range metadata.Constraints {
		if constraint.Name == constraintName {
			metadata.Constraints = append(metadata.Constraints[:i], metadata.Constraints[i+1:]...)
			metadata.Version++
			metadata.UpdatedAt = time.Now()

			err = tmm.store.SaveTableMetadata(indexName, tableName, metadata)
			if err != nil {
				return fmt.Errorf("failed to save table metadata after constraint removal: %w", err)
			}
			return nil
		}
	}

	return fmt.Errorf("constraint %s not found", constraintName)
}

// TableMetadataUpdate 表元数据更新结构
type TableMetadataUpdate struct {
	Schema      *TableSchema       `json:"schema,omitempty"`
	Constraints []*TableConstraint `json:"constraints,omitempty"`
	Indexes     []*TableIndex      `json:"indexes,omitempty"`
}

// Validate 验证表元数据更新
func (tmu *TableMetadataUpdate) Validate() error {
	if tmu.Schema != nil {
		if err := tmu.Schema.Validate(); err != nil {
			return fmt.Errorf("invalid schema: %w", err)
		}
	}

	if tmu.Constraints != nil {
		for _, constraint := range tmu.Constraints {
			if err := constraint.Validate(); err != nil {
				return fmt.Errorf("invalid constraint %s: %w", constraint.Name, err)
			}
		}
	}

	if tmu.Indexes != nil {
		for _, index := range tmu.Indexes {
			if err := index.Validate(); err != nil {
				return fmt.Errorf("invalid index %s: %w", index.Name, err)
			}
		}
	}

	return nil
}
