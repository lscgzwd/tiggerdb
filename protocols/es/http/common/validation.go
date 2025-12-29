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

package common

import (
	"fmt"
	"regexp"
	"strings"
)

// ValidationError 验证错误
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error for field '%s': %s", e.Field, e.Message)
}

// ValidateIndexName 验证索引名称
func ValidateIndexName(indexName string) error {
	if indexName == "" {
		return &ValidationError{Field: "index", Message: "index name cannot be empty"}
	}

	if len(indexName) > 255 {
		return &ValidationError{Field: "index", Message: "index name too long (max 255 characters)"}
	}

	// ES索引名称规则：只能包含小写字母、数字、-、_，不能以-开头
	validName := regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
	if !validName.MatchString(indexName) {
		return &ValidationError{
			Field:   "index",
			Message: "invalid index name format (only lowercase letters, numbers, hyphens, and underscores allowed)",
		}
	}

	// 不能包含连续的点
	if strings.Contains(indexName, "..") {
		return &ValidationError{Field: "index", Message: "index name cannot contain consecutive dots"}
	}

	return nil
}

// ValidateDocumentID 验证文档ID
func ValidateDocumentID(id string) error {
	if id == "" {
		return &ValidationError{Field: "id", Message: "document ID cannot be empty"}
	}

	if len(id) > 512 {
		return &ValidationError{Field: "id", Message: "document ID too long (max 512 characters)"}
	}

	// 检查是否包含控制字符
	for _, r := range id {
		if r < 32 {
			return &ValidationError{Field: "id", Message: "document ID cannot contain control characters"}
		}
	}

	return nil
}
