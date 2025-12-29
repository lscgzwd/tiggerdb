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

// IndexValidator 索引验证器
type IndexValidator struct{}

// NewIndexValidator 创建索引验证器
func NewIndexValidator() *IndexValidator {
	return &IndexValidator{}
}

// ValidateIndex 验证索引定义
func (iv *IndexValidator) ValidateIndex(index *metadata.TableIndex) error {
	if index.Name == "" {
		return fmt.Errorf("index name cannot be empty")
	}

	if len(index.Columns) == 0 {
		return fmt.Errorf("index must have at least one column")
	}

	// 验证索引名称格式
	if err := iv.validateIndexName(index.Name); err != nil {
		return err
	}

	// 验证列定义
	for _, column := range index.Columns {
		if column == "" {
			return fmt.Errorf("index column name cannot be empty")
		}
	}

	// 检查重复列
	seen := make(map[string]bool)
	for _, col := range index.Columns {
		if seen[col] {
			return fmt.Errorf("duplicate column in index: %s", col)
		}
		seen[col] = true
	}

	return nil
}

// validateIndexName 验证索引名称
func (iv *IndexValidator) validateIndexName(name string) error {
	if len(name) > 64 {
		return fmt.Errorf("index name too long (max 64 characters)")
	}

	// 检查字符合法性
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_') {
			return fmt.Errorf("index name contains invalid character: %c", r)
		}
	}

	return nil
}

// ValidateIndexCompatibility 验证索引兼容性
func (iv *IndexValidator) ValidateIndexCompatibility(indexes []*metadata.TableIndex, constraints []*metadata.TableConstraint) error {
	// 检查是否有重复的索引名称
	indexNames := make(map[string]bool)
	for _, index := range indexes {
		if indexNames[index.Name] {
			return fmt.Errorf("duplicate index name: %s", index.Name)
		}
		indexNames[index.Name] = true
	}

	// 检查索引是否与约束冲突
	for _, constraint := range constraints {
		switch strings.ToUpper(constraint.Type) {
		case "PRIMARY_KEY":
			// 主键会自动创建索引
			constraintColumns := constraint.Columns
			for _, index := range indexes {
				if index.IsPrimary && iv.columnsEqual(index.Columns, constraintColumns) {
					return fmt.Errorf("primary key index already exists for columns: %v", constraintColumns)
				}
			}
		case "UNIQUE":
			// 唯一约束会自动创建索引
			constraintColumns := constraint.Columns
			for _, index := range indexes {
				if index.IsUnique && iv.columnsEqual(index.Columns, constraintColumns) {
					return fmt.Errorf("unique constraint index already exists for columns: %v", constraintColumns)
				}
			}
		}
	}

	return nil
}

// columnsEqual 检查两个列列表是否相等
func (iv *IndexValidator) columnsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ValidateIndexPerformance 验证索引性能
func (iv *IndexValidator) ValidateIndexPerformance(index *metadata.TableIndex) error {
	// 简单的性能检查
	columnCount := len(index.Columns)

	if columnCount > 16 {
		return fmt.Errorf("index has too many columns (%d), max recommended is 16", columnCount)
	}

	// 检查是否包含文本类型的列（在复合索引中不推荐）
	// 这里需要访问列定义，暂时跳过

	return nil
}
