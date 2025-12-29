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

package handler

import (
	"encoding/json"
	"github.com/lscgzwd/tiggerdb/logger"
	"net/http"

	"github.com/gorilla/mux"
	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/directory"
	"github.com/lscgzwd/tiggerdb/metadata"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/common"
	es "github.com/lscgzwd/tiggerdb/protocols/es/index"
)

// StatsHandler 统计信息处理器
type StatsHandler struct {
	indexMgr  *es.IndexManager
	dirMgr    directory.DirectoryManager
	metaStore metadata.MetadataStore
}

// NewStatsHandler 创建新的统计信息处理器
func NewStatsHandler(indexMgr *es.IndexManager, dirMgr directory.DirectoryManager, metaStore metadata.MetadataStore) *StatsHandler {
	return &StatsHandler{
		indexMgr:  indexMgr,
		dirMgr:    dirMgr,
		metaStore: metaStore,
	}
}

// GetStats 获取索引统计信息
// GET /_stats
func (h *StatsHandler) GetStats(w http.ResponseWriter, r *http.Request) {
	// 获取所有索引名称
	indices, err := h.dirMgr.ListIndices()
	if err != nil {
		logger.Error("Failed to list indices: %v", err)
		common.HandleError(w, common.NewInternalServerError("failed to list indices: "+err.Error()))
		return
	}

	// 构建统计信息
	stats := map[string]interface{}{
		"_all": map[string]interface{}{
			"primaries": map[string]interface{}{},
			"total":     map[string]interface{}{},
		},
		"indices": make(map[string]interface{}),
	}

	allPrimaries := map[string]interface{}{
		"docs": map[string]interface{}{
			"count":   int64(0),
			"deleted": int64(0),
		},
		"store": map[string]interface{}{
			"size_in_bytes": 0,
		},
		"indexing": map[string]interface{}{
			"index_total":           0,
			"index_time_in_millis":  0,
			"index_current":         0,
			"index_failed":          0,
			"delete_total":          0,
			"delete_time_in_millis": 0,
			"delete_current":        0,
		},
		"search": map[string]interface{}{
			"query_total":          0,
			"query_time_in_millis": 0,
			"query_current":        0,
			"fetch_total":          0,
			"fetch_time_in_millis": 0,
			"fetch_current":        0,
		},
	}

	indicesStats := make(map[string]interface{})

	// 遍历所有索引，收集统计信息
	for _, indexName := range indices {
		idx, err := h.indexMgr.GetIndex(indexName)
		if err != nil {
			logger.Warn("Failed to get index [%s] for stats: %v", indexName, err)
			continue
		}

		// 获取索引统计信息
		indexStats := h.getIndexStats(idx, indexName)
		indicesStats[indexName] = indexStats

		// 累加到总计
		if primaries, ok := indexStats["primaries"].(map[string]interface{}); ok {
			if docs, ok := primaries["docs"].(map[string]interface{}); ok {
				if count, ok := docs["count"].(int64); ok {
					currentCount := allPrimaries["docs"].(map[string]interface{})["count"].(int64)
					allPrimaries["docs"].(map[string]interface{})["count"] = currentCount + count
				}
			}
		}
	}

	stats["indices"] = indicesStats
	stats["_all"].(map[string]interface{})["primaries"] = allPrimaries
	stats["_all"].(map[string]interface{})["total"] = allPrimaries // 单节点模式下，total = primaries

	// 直接返回响应，不使用通用响应格式
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(stats); err != nil {
		logger.Error("Failed to encode stats response: %v", err)
	}
}

// getIndexStats 获取单个索引的统计信息
func (h *StatsHandler) getIndexStats(idx bleve.Index, indexName string) map[string]interface{} {
	// 获取文档总数
	searchReq := bleve.NewSearchRequest(bleve.NewMatchAllQuery())
	searchReq.Size = 0
	searchResult, err := idx.Search(searchReq)
	docCount := int64(0)
	if err == nil {
		docCount = int64(searchResult.Total)
	}

	// 获取索引统计信息（bleve 不直接提供存储大小等信息，使用默认值）
	stats := map[string]interface{}{
		"primaries": map[string]interface{}{
			"docs": map[string]interface{}{
				"count":   docCount,
				"deleted": 0,
			},
			"store": map[string]interface{}{
				"size_in_bytes": 0, // bleve 不直接提供此信息
			},
			"indexing": map[string]interface{}{
				"index_total":           0,
				"index_time_in_millis":  0,
				"index_current":         0,
				"index_failed":          0,
				"delete_total":          0,
				"delete_time_in_millis": 0,
				"delete_current":        0,
			},
			"search": map[string]interface{}{
				"query_total":          0,
				"query_time_in_millis": 0,
				"query_current":        0,
				"fetch_total":          0,
				"fetch_time_in_millis": 0,
				"fetch_current":        0,
			},
		},
		"total": map[string]interface{}{
			"docs": map[string]interface{}{
				"count":   docCount,
				"deleted": 0,
			},
			"store": map[string]interface{}{
				"size_in_bytes": 0,
			},
			"indexing": map[string]interface{}{
				"index_total":           0,
				"index_time_in_millis":  0,
				"index_current":         0,
				"index_failed":          0,
				"delete_total":          0,
				"delete_time_in_millis": 0,
				"delete_current":        0,
			},
			"search": map[string]interface{}{
				"query_total":          0,
				"query_time_in_millis": 0,
				"query_current":        0,
				"fetch_total":          0,
				"fetch_time_in_millis": 0,
				"fetch_current":        0,
			},
		},
	}

	return stats
}

// GetIndexStats 获取单个索引的统计信息
// GET /{index}/_stats
func (h *StatsHandler) GetIndexStats(w http.ResponseWriter, r *http.Request) {
	// 从URL路径中获取索引名称
	vars := mux.Vars(r)
	indexName := vars["index"]

	// 验证索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 获取索引实例
	idx, err := h.indexMgr.GetIndex(indexName)
	if err != nil {
		common.HandleError(w, common.NewInternalServerError("failed to get index: "+err.Error()))
		return
	}
	defer idx.Close()

	// 获取索引统计信息
	indexStats := h.getIndexStats(idx, indexName)

	// 构建响应（单个索引的stats格式）
	// indexStats 已经包含了 primaries 和 total，直接使用
	primaries, ok1 := indexStats["primaries"].(map[string]interface{})
	total, ok2 := indexStats["total"].(map[string]interface{})

	if !ok1 || !ok2 {
		logger.Error("Failed to extract primaries/total from indexStats: primaries=%v, total=%v", ok1, ok2)
		common.HandleError(w, common.NewInternalServerError("failed to extract stats"))
		return
	}

	stats := map[string]interface{}{
		"_all": map[string]interface{}{
			"primaries": primaries,
			"total":     total,
		},
		"indices": map[string]interface{}{
			indexName: indexStats,
		},
	}

	// 返回响应
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(stats); err != nil {
		logger.Error("Failed to encode index stats response: %v", err)
	}
}

// GetInfo 获取系统基本信息
// GET /_info
func (h *StatsHandler) GetInfo(w http.ResponseWriter, r *http.Request) {
	info := map[string]interface{}{
		"name":         "TigerDB",
		"cluster_name": ClusterName,
		"cluster_uuid": ClusterUUID,
		"version": map[string]interface{}{
			"number":                              ESVersionNumber,
			"build_flavor":                        "default",
			"build_type":                          "release",
			"build_hash":                          ESBuildHash,
			"build_date":                          ESBuildDate,
			"build_snapshot":                      false,
			"lucene_version":                      ESLuceneVersion,
			"minimum_wire_compatibility_version":  ESMinimumWireCompatibilityVersion,
			"minimum_index_compatibility_version": ESMinimumIndexCompatibilityVersion,
		},
		"tagline": "You know, for search",
	}

	// 直接返回响应，不使用通用响应格式
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(info); err != nil {
		logger.Error("Failed to encode info response: %v", err)
	}
}
