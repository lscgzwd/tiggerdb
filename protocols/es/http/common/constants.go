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

const (
	// MaxDocumentBodySize 单个文档请求体的最大大小（10MB）
	MaxDocumentBodySize = 10 * 1024 * 1024

	// MaxBulkBodySize 批量操作请求体的最大大小（100MB）
	MaxBulkBodySize = 100 * 1024 * 1024

	// MaxSearchBodySize 搜索请求体的最大大小（10MB）
	MaxSearchBodySize = 10 * 1024 * 1024

	// MaxIndexBodySize 索引创建请求体的最大大小（10MB）
	MaxIndexBodySize = 10 * 1024 * 1024
)
