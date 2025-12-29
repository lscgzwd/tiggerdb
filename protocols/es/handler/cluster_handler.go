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
	"fmt"
	"net/http"
	"strings"

	"github.com/lscgzwd/tiggerdb/directory"
	"github.com/lscgzwd/tiggerdb/logger"
	"github.com/lscgzwd/tiggerdb/metadata"
	es "github.com/lscgzwd/tiggerdb/protocols/es/index"
)

// ClusterHandler 集群处理器
type ClusterHandler struct {
	indexMgr  *es.IndexManager
	dirMgr    directory.DirectoryManager
	metaStore metadata.MetadataStore
}

// ClusterHealthResponse 集群健康响应结构体 - 按照 Elasticsearch 标准顺序定义字段
type ClusterHealthResponse struct {
	ClusterName                 string  `json:"cluster_name"`
	Status                      string  `json:"status"`
	TimedOut                    bool    `json:"timed_out"`
	NumberOfNodes               int     `json:"number_of_nodes"`
	NumberOfDataNodes           int     `json:"number_of_data_nodes"`
	ActivePrimaryShards         int     `json:"active_primary_shards"`
	ActiveShards                int     `json:"active_shards"`
	RelocatingShards            int     `json:"relocating_shards"`
	InitializingShards          int     `json:"initializing_shards"`
	UnassignedShards            int     `json:"unassigned_shards"`
	DelayedUnassignedShards     int     `json:"delayed_unassigned_shards"`
	NumberOfPendingTasks        int     `json:"number_of_pending_tasks"`
	NumberOfInFlightFetch       int     `json:"number_of_in_flight_fetch"`
	TaskMaxWaitingInQueueMillis int     `json:"task_max_waiting_in_queue_millis"`
	ActiveShardsPercentAsNumber float64 `json:"active_shards_percent_as_number"`
}

// NewClusterHandler 创建新的集群处理器
func NewClusterHandler(indexMgr *es.IndexManager, dirMgr directory.DirectoryManager, metaStore metadata.MetadataStore) *ClusterHandler {
	return &ClusterHandler{
		indexMgr:  indexMgr,
		dirMgr:    dirMgr,
		metaStore: metaStore,
	}
}

// Ping 检查服务器是否可用
// GET /_ping, HEAD /_ping
func (h *ClusterHandler) Ping(w http.ResponseWriter, r *http.Request) {
	// Elasticsearch 的 _ping 端点返回简单的 JSON 响应
	response := map[string]interface{}{
		"name":         NodeName,
		"cluster_name": ClusterName,
		"version": map[string]interface{}{
			"number": ESVersionNumber,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// HEAD 请求不返回响应体
	if r.Method != http.MethodHead {
		encoder := json.NewEncoder(w)
		if err := encoder.Encode(response); err != nil {
			logger.Error("Failed to encode ping response: %v", err)
		}
	}
}

// ClusterHealth 获取集群健康状态
// GET /_cluster/health
func (h *ClusterHandler) ClusterHealth(w http.ResponseWriter, r *http.Request) {
	// 获取所有索引
	indices, err := h.dirMgr.ListIndices()
	if err != nil {
		logger.Error("Failed to list indices for cluster health: %v", err)
		// 返回默认的健康状态
		indices = []string{}
	}

	// 计算集群状态
	clusterStatus := ClusterStatusGreen // 单节点模式下，始终返回green状态
	activePrimaryShards := len(indices)
	activeShards := len(indices)

	// 使用结构体构建响应，确保类型安全和可维护性
	response := ClusterHealthResponse{
		ClusterName:                 ClusterName,
		Status:                      clusterStatus,
		TimedOut:                    false,
		NumberOfNodes:               1,
		NumberOfDataNodes:           1,
		ActivePrimaryShards:         activePrimaryShards,
		ActiveShards:                activeShards,
		RelocatingShards:            0,
		InitializingShards:          0,
		UnassignedShards:            0,
		DelayedUnassignedShards:     0,
		NumberOfPendingTasks:        0,
		NumberOfInFlightFetch:       0,
		TaskMaxWaitingInQueueMillis: 0,
		ActiveShardsPercentAsNumber: ActiveShardsPercent,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(response); err != nil {
		logger.Error("Failed to encode cluster health response: %v", err)
	}
}

// ClusterState 获取集群状态
// GET /_cluster/state
func (h *ClusterHandler) ClusterState(w http.ResponseWriter, r *http.Request) {
	// 获取所有索引
	indices, err := h.dirMgr.ListIndices()
	if err != nil {
		logger.Error("Failed to list indices for cluster state: %v", err)
		indices = []string{}
	}

	// 构建索引状态
	indicesState := make(map[string]interface{})
	for _, indexName := range indices {
		indicesState[indexName] = map[string]interface{}{
			"aliases": []string{},
			"mappings": map[string]interface{}{
				"_doc": map[string]interface{}{
					"properties": map[string]interface{}{},
				},
			},
			"settings": map[string]interface{}{
				"index": map[string]interface{}{
					"number_of_shards":   1,
					"number_of_replicas": 0,
				},
			},
		}
	}

	// 构建集群状态响应
	state := map[string]interface{}{
		"cluster_name": ClusterName,
		"version":      1,
		"state_uuid":   ClusterUUID + "-state",
		"master_node":  NodeName,
		"blocks":       map[string]interface{}{},
		"nodes": map[string]interface{}{
			NodeName: map[string]interface{}{
				"name":              NodeName,
				"transport_address": NodeTransportAddress,
				"attributes":        map[string]interface{}{},
				"roles":             []string{"master", "data"},
			},
		},
		"metadata": map[string]interface{}{
			"cluster_uuid":           ClusterUUID,
			"cluster_uuid_committed": true,
			"templates":              map[string]interface{}{},
			"indices":                indicesState,
		},
		"routing_table": map[string]interface{}{
			"indices": make(map[string]interface{}),
		},
		"routing_nodes": map[string]interface{}{
			"unassigned": []interface{}{},
			"nodes": map[string]interface{}{
				NodeName: []interface{}{},
			},
		},
	}

	// 直接返回响应，不使用通用响应格式
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(state); err != nil {
		logger.Error("Failed to encode cluster state response: %v", err)
	}
}

// NodesInfo 获取节点信息
// GET /_nodes
func (h *ClusterHandler) NodesInfo(w http.ResponseWriter, r *http.Request) {
	nodes := map[string]interface{}{
		"cluster_name": ClusterName,
		"nodes": map[string]interface{}{
			NodeName: map[string]interface{}{
				"name":                  NodeName,
				"transport_address":     NodeTransportAddress,
				"host":                  "127.0.0.1",
				"ip":                    "127.0.0.1",
				"version":               ESVersionNumber,
				"build_flavor":          "default",
				"build_type":            "release",
				"build_hash":            ESBuildHash,
				"total_indexing_buffer": 104857600,
				"roles":                 []string{"master", "data", "ingest"},
				"attributes": map[string]interface{}{
					"ml.machine_memory": "17179869184",
					"ml.max_open_jobs":  "512",
					"xpack.installed":   "true",
				},
				"settings": map[string]interface{}{
					"pidfile": "/var/run/elasticsearch/elasticsearch.pid",
					"path": map[string]interface{}{
						"home": "/usr/share/elasticsearch",
						"logs": "/var/log/elasticsearch",
						"data": []string{"/var/lib/elasticsearch"},
						"repo": []string{"/var/lib/elasticsearch/snapshots"},
					},
					"cluster": map[string]interface{}{
						"name": ClusterName,
					},
					"node": map[string]interface{}{
						"name": NodeName,
					},
				},
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(nodes); err != nil {
		logger.Error("Failed to encode nodes info response: %v", err)
	}
}

// CatNodes 获取节点列表（cat API格式）
// GET /_cat/nodes
func (h *ClusterHandler) CatNodes(w http.ResponseWriter, r *http.Request) {
	// 简单的cat格式响应
	catResponse := "127.0.0.1 mdi * tigerdb-node-1\n"

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(catResponse))
}

// CatIndices 获取索引列表（cat API格式）
// GET /_cat/indices
func (h *ClusterHandler) CatIndices(w http.ResponseWriter, r *http.Request) {
	// 获取所有索引
	indices, err := h.dirMgr.ListIndices()
	if err != nil {
		logger.Error("Failed to list indices for cat indices: %v", err)
		indices = []string{}
	}

	// 解析查询参数 h= 指定返回的列（ES cat API 标准参数）
	queryParams := r.URL.Query()
	columnsParam := queryParams.Get("h")
	var columns []string
	if columnsParam != "" {
		columns = strings.Split(columnsParam, ",")
		// 清理列名（去除空格）
		for i := range columns {
			columns[i] = strings.TrimSpace(columns[i])
		}
	}

	// 默认返回JSON格式（elasticvue期望的格式）
	// 确保 result 是空切片而不是 nil，避免返回 null
	result := make([]map[string]interface{}, 0, len(indices))
	for _, indexName := range indices {
		item := make(map[string]interface{})

		// 如果指定了列，只返回指定的列；否则返回所有列
		if len(columns) > 0 {
			for _, col := range columns {
				switch col {
				case "index":
					item["index"] = indexName
				case "health":
					item["health"] = "green"
				case "status":
					item["status"] = "open"
				case "uuid":
					item["uuid"] = "N/A"
				case "pri":
					item["pri"] = "1"
				case "rep":
					item["rep"] = "0"
				case "docs.count":
					item["docs.count"] = "0"
				case "store.size":
					item["store.size"] = "0b"
				case "sc", "searchable_snapshots":
					item["sc"] = "0b"
				case "cd", "completion.size":
					item["cd"] = "0b"
				}
			}
		} else {
			// 默认返回所有列
			item["health"] = "green"
			item["status"] = "open"
			item["index"] = indexName
			item["uuid"] = "N/A"
			item["pri"] = "1"
			item["rep"] = "0"
			item["docs.count"] = "0"
			item["store.size"] = "0b"
		}

		result = append(result, item)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	// result 是空切片 []，不会返回 null
	if err := encoder.Encode(result); err != nil {
		logger.Error("Failed to encode cat indices JSON response: %v", err)
		// 如果编码失败，至少返回空数组
		w.Write([]byte("[]"))
	}
}

// ClusterStats 获取集群统计信息
// GET /_cluster/stats
func (h *ClusterHandler) ClusterStats(w http.ResponseWriter, r *http.Request) {
	// 获取所有索引
	indices, err := h.dirMgr.ListIndices()
	if err != nil {
		logger.Error("Failed to list indices for cluster stats: %v", err)
		indices = []string{}
	}

	// 构建集群统计响应
	stats := map[string]interface{}{
		"cluster_name": ClusterName,
		"cluster_uuid": ClusterUUID,
		"timestamp":    1732790400000, // 示例时间戳
		"status":       "green",
		"indices": map[string]interface{}{
			"count": len(indices),
			"shards": map[string]interface{}{
				"total":        len(indices),
				"primaries":    len(indices),
				"replicas":     0,
				"active":       len(indices),
				"unassigned":   0,
				"initializing": 0,
				"relocating":   0,
			},
			"docs": map[string]interface{}{
				"count":   0,
				"deleted": 0,
			},
			"store": map[string]interface{}{
				"size_in_bytes":     0,
				"reserved_in_bytes": 0,
			},
		},
		"nodes": map[string]interface{}{
			"count": map[string]interface{}{
				"total":                 1,
				"data":                  1,
				"coordinating_only":     1,
				"master":                1,
				"ingest":                1,
				"voting_only":           0,
				"search":                1,
				"ml":                    0,
				"remote_cluster_client": 0,
				"transform":             0,
			},
			"versions": []string{ESVersionNumber},
			"os": map[string]interface{}{
				"available_processors": 8,
				"allocated_processors": 8,
				"names": []map[string]interface{}{
					{
						"name":  "Linux",
						"count": 1,
					},
				},
				"pretty_names": []map[string]interface{}{
					{
						"pretty_name": "Ubuntu 20.04.6 LTS",
						"count":       1,
					},
				},
				"architectures": []map[string]interface{}{
					{
						"arch":  "x86_64",
						"count": 1,
					},
				},
			},
			"process": map[string]interface{}{
				"cpu": map[string]interface{}{
					"percent": 0,
				},
				"open_file_descriptors": map[string]interface{}{
					"min": 1024,
					"max": 1024,
					"avg": 1024,
				},
			},
			"jvm": map[string]interface{}{
				"max_uptime_in_millis": 3600000,
				"versions": []map[string]interface{}{
					{
						"version":           "11.0.19",
						"vm_name":           "OpenJDK 64-Bit Server VM",
						"vm_version":        "11.0.19+7",
						"vm_vendor":         "Eclipse Adoptium",
						"bundled_jdk":       true,
						"using_bundled_jdk": true,
						"count":             1,
					},
				},
				"mem": map[string]interface{}{
					"heap_used_in_bytes": 1073741824,
					"heap_max_in_bytes":  2147483648,
				},
				"threads": 50,
			},
			"fs": map[string]interface{}{
				"total_in_bytes":     107374182400,
				"free_in_bytes":      85899345920,
				"available_in_bytes": 75161927680,
			},
			"plugins": []interface{}{},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(stats); err != nil {
		logger.Error("Failed to encode cluster stats response: %v", err)
	}
}

// CatShards 获取分片列表（cat API格式）
// GET /_cat/shards
func (h *ClusterHandler) CatShards(w http.ResponseWriter, r *http.Request) {
	// 获取所有索引
	indices, err := h.dirMgr.ListIndices()
	if err != nil {
		logger.Error("Failed to list indices for cat shards: %v", err)
		indices = []string{}
	}

	var catResponse strings.Builder
	// 预分配容量，减少内存分配
	catResponse.Grow(len(indices) * 80) // 估算每行约80字节
	for _, indexName := range indices {
		// 为每个索引添加一个主分片，使用fmt.Fprintf直接写入Builder以提高性能
		fmt.Fprintf(&catResponse, "%s 0 p STARTED 0 0 0b 127.0.0.1 %s\n", indexName, NodeName)
	}

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(catResponse.String()))
}
