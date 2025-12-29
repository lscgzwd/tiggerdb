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
)

// ConstraintValidator 约束验证器
type ConstraintValidator struct{}

// NewConstraintValidator 创建约束验证器
func NewConstraintValidator() *ConstraintValidator {
	return &ConstraintValidator{}
}

// ValidatePrimaryKey 验证主键约束
func (cv *ConstraintValidator) ValidatePrimaryKey(columnNames []string) error {
	if len(columnNames) == 0 {
		return fmt.Errorf("primary key must have at least one column")
	}

	// 检查重复列
	seen := make(map[string]bool)
	for _, col := range columnNames {
		if seen[col] {
			return fmt.Errorf("duplicate column in primary key: %s", col)
		}
		seen[col] = true
	}

	return nil
}

// ValidateUnique 验证唯一约束
func (cv *ConstraintValidator) ValidateUnique(columnNames []string) error {
	if len(columnNames) == 0 {
		return fmt.Errorf("unique constraint must have at least one column")
	}

	// 检查重复列
	seen := make(map[string]bool)
	for _, col := range columnNames {
		if seen[col] {
			return fmt.Errorf("duplicate column in unique constraint: %s", col)
		}
		seen[col] = true
	}

	return nil
}

// ValidateForeignKey 验证外键约束
func (cv *ConstraintValidator) ValidateForeignKey(localColumns []string, refTable string, refColumns []interface{}) error {
	if len(localColumns) == 0 || len(refColumns) == 0 {
		return fmt.Errorf("foreign key must reference at least one column")
	}

	if len(localColumns) != len(refColumns) {
		return fmt.Errorf("foreign key local and reference column count must match")
	}

	if refTable == "" {
		return fmt.Errorf("foreign key must specify reference table")
	}

	// 检查本地列是否有重复
	seen := make(map[string]bool)
	for _, col := range localColumns {
		if seen[col] {
			return fmt.Errorf("duplicate local column in foreign key: %s", col)
		}
		seen[col] = true
	}

	return nil
}

// ValidateCheck 验证检查约束
func (cv *ConstraintValidator) ValidateCheck(expression string) error {
	if expression == "" {
		return fmt.Errorf("check constraint must have an expression")
	}

	// 基本的表达式验证（可以扩展）
	if len(expression) < 3 {
		return fmt.Errorf("check constraint expression too short")
	}

	return nil
}
