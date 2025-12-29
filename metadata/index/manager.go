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

package index

import (
	"github.com/lscgzwd/tiggerdb/metadata"
)

// IndexMetadataManager 索引元数据管理器
type IndexMetadataManager struct {
	store *metadata.MetadataStore
}

// NewIndexMetadataManager 创建索引元数据管理器
func NewIndexMetadataManager(store *metadata.MetadataStore) *IndexMetadataManager {
	return &IndexMetadataManager{
		store: store,
	}
}
