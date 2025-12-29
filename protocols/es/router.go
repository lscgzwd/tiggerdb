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

package es

import "net/http"

// IndexHandler ES索引处理器接口
type IndexHandler interface {
	ListIndices(w http.ResponseWriter, r *http.Request)
	CreateIndex(w http.ResponseWriter, r *http.Request)
	GetIndex(w http.ResponseWriter, r *http.Request)
	HeadIndex(w http.ResponseWriter, r *http.Request)
	DeleteIndex(w http.ResponseWriter, r *http.Request)
	GetMapping(w http.ResponseWriter, r *http.Request)
	UpdateMapping(w http.ResponseWriter, r *http.Request)
	GetSettings(w http.ResponseWriter, r *http.Request)
	UpdateSettings(w http.ResponseWriter, r *http.Request)
	CloseIndex(w http.ResponseWriter, r *http.Request)
	OpenIndex(w http.ResponseWriter, r *http.Request)
	RefreshIndex(w http.ResponseWriter, r *http.Request)
	FlushIndex(w http.ResponseWriter, r *http.Request)
	GetAlias(w http.ResponseWriter, r *http.Request)
	PutAlias(w http.ResponseWriter, r *http.Request)
	DeleteAlias(w http.ResponseWriter, r *http.Request)
	GetAllAliases(w http.ResponseWriter, r *http.Request)
	GetAliasByName(w http.ResponseWriter, r *http.Request)
	UpdateAliases(w http.ResponseWriter, r *http.Request)
}

// DocumentHandler ES文档处理器接口
type DocumentHandler interface {
	CreateDocument(w http.ResponseWriter, r *http.Request)
	IndexDocument(w http.ResponseWriter, r *http.Request)
	GetDocument(w http.ResponseWriter, r *http.Request)
	DeleteDocument(w http.ResponseWriter, r *http.Request)
	HeadDocument(w http.ResponseWriter, r *http.Request)
	UpdateDocument(w http.ResponseWriter, r *http.Request)
	CountDocuments(w http.ResponseWriter, r *http.Request)
	Bulk(w http.ResponseWriter, r *http.Request)
	Search(w http.ResponseWriter, r *http.Request)
	GlobalSearch(w http.ResponseWriter, r *http.Request)
	MultiSearch(w http.ResponseWriter, r *http.Request)
	MultiGet(w http.ResponseWriter, r *http.Request)
}

// ClusterHandler ES集群处理器接口
type ClusterHandler interface {
	Ping(w http.ResponseWriter, r *http.Request)
	ClusterHealth(w http.ResponseWriter, r *http.Request)
	ClusterState(w http.ResponseWriter, r *http.Request)
	ClusterStats(w http.ResponseWriter, r *http.Request)
	NodesInfo(w http.ResponseWriter, r *http.Request)
	CatNodes(w http.ResponseWriter, r *http.Request)
	CatIndices(w http.ResponseWriter, r *http.Request)
	CatShards(w http.ResponseWriter, r *http.Request)
}
