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
	"bytes"
	"encoding/json"
	"io"
	"github.com/lscgzwd/tiggerdb/logger"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/common"
	"github.com/lscgzwd/tiggerdb/protocols/es/search/dsl"
)

// DeleteByQueryRequest 删除查询请求
type DeleteByQueryRequest struct {
	Query             map[string]interface{} `json:"query"`
	Conflicts         string                 `json:"conflicts,omitempty"`           // proceed 或 abort
	Slices            interface{}            `json:"slices,omitempty"`              // auto, 数字, 或手动切片
	MaxDocs           int                    `json:"max_docs,omitempty"`            // 最大删除文档数
	Refresh           string                 `json:"refresh,omitempty"`             // true, false, wait_for
	WaitForCompletion bool                   `json:"wait_for_completion,omitempty"` // 是否等待完成
}

// DeleteByQueryResponse 删除查询响应
type DeleteByQueryResponse struct {
	Took                 int64                  `json:"took"`
	TimedOut             bool                   `json:"timed_out"`
	Total                int64                  `json:"total"`
	Deleted              int64                  `json:"deleted"`
	Batches              int64                  `json:"batches"`
	VersionConflicts     int64                  `json:"version_conflicts"`
	Noops                int64                  `json:"noops"`
	Retries              map[string]interface{} `json:"retries"`
	ThrottledMillis      int64                  `json:"throttled_millis"`
	RequestsPerSecond    float64                `json:"requests_per_second"`
	ThrottledUntilMillis int64                  `json:"throttled_until_millis"`
	Failures             []interface{}          `json:"failures"`
}

// DeleteByQuery 根据查询条件删除文档
// POST /{index}/_delete_by_query
func (h *DocumentHandler) DeleteByQuery(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 验证索引名称
	if err := common.ValidateIndexName(indexName); err != nil {
		common.HandleError(w, common.NewBadRequestError(err.Error()))
		return
	}

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 获取索引实例
	idx, err := h.indexMgr.GetIndex(indexName)
	if err != nil {
		logger.Error("Failed to get index [%s]: %v", indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to get index: "+err.Error()))
		return
	}

	// 读取请求体
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("Failed to read delete_by_query request body: %v", err)
		common.HandleError(w, common.NewBadRequestError("failed to read request body: "+err.Error()))
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	// 解析请求
	var req DeleteByQueryRequest
	if len(bodyBytes) > 0 {
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&req); err != nil && err != io.EOF {
			common.HandleError(w, common.NewBadRequestError("invalid JSON body: "+err.Error()))
			return
		}
	}

	// 如果没有查询条件，返回错误
	if len(req.Query) == 0 {
		common.HandleError(w, common.NewBadRequestError("query is required"))
		return
	}

	// 解析查询
	parser := dsl.NewQueryParser()
	bleveQuery, err := parser.ParseQuery(req.Query)
	if err != nil {
		logger.Error("Failed to parse query: %v", err)
		common.HandleError(w, common.NewBadRequestError("invalid query: "+err.Error()))
		return
	}

	// 创建搜索请求以查找匹配的文档
	searchRequest := bleve.NewSearchRequest(bleveQuery)
	searchRequest.Size = 1000              // 每次处理1000个文档
	searchRequest.Fields = []string{"_id"} // 只需要ID字段

	startTime := time.Now()
	totalDeleted := int64(0)
	batches := int64(0)
	versionConflicts := int64(0)
	maxDocs := req.MaxDocs
	if maxDocs <= 0 {
		maxDocs = 10000000 // 默认最大1000万，防止误删
	}

	// 使用scroll方式批量删除（对于大数据量更高效）
	from := 0
	for {
		searchRequest.From = from
		searchRequest.Size = 1000

		// 执行搜索
		searchResults, err := idx.Search(searchRequest)
		if err != nil {
			logger.Error("Failed to search documents for deletion: %v", err)
			common.HandleError(w, common.NewInternalServerError("failed to search documents: "+err.Error()))
			return
		}

		// 如果没有结果，结束
		if len(searchResults.Hits) == 0 {
			break
		}

		// 批量删除
		batch := idx.NewBatch()
		batchSize := 0
		for _, hit := range searchResults.Hits {
			if totalDeleted >= int64(maxDocs) {
				break
			}
			batch.Delete(hit.ID)
			batchSize++
			totalDeleted++
		}

		// 执行批量删除
		if batchSize > 0 {
			if err := idx.Batch(batch); err != nil {
				logger.Error("Failed to execute delete batch: %v", err)
				versionConflicts += int64(batchSize)
			} else {
				batches++
			}
		}

		// 如果已达到最大文档数，结束
		if totalDeleted >= int64(maxDocs) {
			break
		}

		// 如果结果数小于请求的size，说明已经处理完所有数据
		if len(searchResults.Hits) < searchRequest.Size {
			break
		}

		from += len(searchResults.Hits)
	}

	took := time.Since(startTime).Milliseconds()

	// 计算 RequestsPerSecond，避免除以零导致的 NaN
	var requestsPerSecond float64
	if took > 0 {
		requestsPerSecond = float64(totalDeleted) / float64(took) * 1000.0
	} else {
		// 如果 took 为 0，设置为 0 而不是 NaN
		requestsPerSecond = 0
	}

	// 构建响应
	response := DeleteByQueryResponse{
		Took:                 took,
		TimedOut:             false,
		Total:                totalDeleted,
		Deleted:              totalDeleted,
		Batches:              batches,
		VersionConflicts:     versionConflicts,
		Noops:                0,
		Retries:              make(map[string]interface{}),
		ThrottledMillis:      0,
		RequestsPerSecond:    requestsPerSecond,
		ThrottledUntilMillis: 0,
		Failures:             []interface{}{},
	}

	// 返回响应
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(response); err != nil {
		logger.Error("Failed to encode delete_by_query response: %v", err)
	}
}
