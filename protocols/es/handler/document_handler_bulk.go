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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"github.com/lscgzwd/tiggerdb/logger"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/common"
)

// BulkRequest 批量操作请求
type BulkRequest struct {
	Index       string                 `json:"index,omitempty"`
	ID          string                 `json:"id,omitempty"`
	Doc         map[string]interface{} `json:"doc,omitempty"`
	Source      map[string]interface{} `json:"_source,omitempty"`
	Action      string                 `json:"action"` // index, create, update, delete
	Version     int64                  `json:"version,omitempty"`
	DocAsUpsert bool                   `json:"doc_as_upsert,omitempty"` // update操作时，如果文档不存在，将doc作为新文档插入
}

// BulkResponse 批量操作响应
type BulkResponse struct {
	Took   int64                    `json:"took"`
	Errors bool                     `json:"errors"`
	Items  []map[string]interface{} `json:"items"`
}

// Bulk 批量操作API
// POST /_bulk
// POST /{index}/_bulk (支持在URL中指定默认索引)
func (h *DocumentHandler) Bulk(w http.ResponseWriter, r *http.Request) {
	// 检查URL中是否包含索引名称（/{index}/_bulk格式）
	defaultIndex := mux.Vars(r)["index"]
	// 读取请求体（移除DEBUG日志以提高性能）
	bodyBytes, readErr := io.ReadAll(r.Body)
	if readErr != nil {
		logger.Error("Failed to read bulk request body: %v", readErr)
		common.HandleError(w, common.NewBadRequestError("failed to read request body: "+readErr.Error()))
		return
	}
	// 重新设置body以便后续处理
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	// 检查Content-Type
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/x-ndjson" {
		common.HandleError(w, common.NewBadRequestError("Content-Type must be application/x-ndjson"))
		return
	}

	// 检查请求体大小（兼容 chunked，可选：io.LimitReader 强制限制）

	// 读取请求体
	reader := bufio.NewReader(r.Body)
	defer r.Body.Close()

	// 解析批量操作
	bulkItems := make([]BulkRequest, 0)
	lineNum := 0

	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Error("Failed to read bulk request line %d: %v", lineNum, err)
			common.HandleError(w, common.NewBadRequestError(fmt.Sprintf("failed to read bulk request: %v", err)))
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		lineNum++

		// 解析JSON行
		var jsonLine map[string]interface{}
		if err := json.Unmarshal([]byte(line), &jsonLine); err != nil {
			logger.Error("Failed to parse JSON line %d: %v", lineNum, err)
			common.HandleError(w, common.NewBadRequestError(fmt.Sprintf("failed to parse JSON line %d: %v", lineNum, err)))
			return
		}

		// 判断是操作行还是数据行
		isActionLine := false
		var action string
		var meta map[string]interface{}

		// 检查是否是操作行（只包含一个ES操作类型的键）
		for key, value := range jsonLine {
			if metaMap, ok := value.(map[string]interface{}); ok {
				// 检查是否包含ES特定的元数据字段
				if _, hasIndex := metaMap["_index"]; hasIndex {
					isActionLine = true
					action = key
					meta = metaMap
					break
				}
			}
		}

		if isActionLine {
			// 这是操作行，创建新的bulk请求
			var bulkReq BulkRequest
			bulkReq.Action = action
			if idx, ok := meta["_index"].(string); ok {
				bulkReq.Index = idx
			} else if defaultIndex != "" {
				// 如果URL中指定了默认索引，且操作行中没有_index，使用默认索引
				bulkReq.Index = defaultIndex
			}
			if id, ok := meta["_id"].(string); ok {
				bulkReq.ID = id
			}
			bulkItems = append(bulkItems, bulkReq)
		} else {
			// 这是数据行，添加到最后一个bulk请求
			if len(bulkItems) > 0 {
				lastReq := &bulkItems[len(bulkItems)-1]
				// index、create和update操作都需要数据行
				if lastReq.Action == "index" || lastReq.Action == "create" {
					lastReq.Source = jsonLine
				} else if lastReq.Action == "update" {
					// update操作的数据格式通常是 {"doc": {...}, "doc_as_upsert": true} 或 {"script": {...}}
					// jsonLine已经是map[string]interface{}类型，直接使用
					// 如果包含doc字段，提取它
					if doc, ok := jsonLine["doc"].(map[string]interface{}); ok {
						lastReq.Doc = doc
					} else {
						// 如果没有doc字段，将整个数据作为Source（向后兼容）
						lastReq.Source = jsonLine
					}
					// 检查是否有doc_as_upsert字段
					if docAsUpsert, ok := jsonLine["doc_as_upsert"].(bool); ok {
						lastReq.DocAsUpsert = docAsUpsert
					}
				}
			}
		}
	}

	// 检查是否需要刷新索引（从查询参数）
	refresh := r.URL.Query().Get("refresh")
	shouldRefresh := refresh == "true" || refresh == "wait_for"

	// 对于大量数据，使用流式响应避免超时
	// 判断是否需要流式响应：如果操作数量超过阈值，使用流式响应
	// 降低阈值，因为即使中等大小的批量操作也可能导致超时
	// 性能优化：降低阈值，确保所有批量操作都能及时响应
	useStreaming := len(bulkItems) > 100 // 超过100个操作使用流式响应

	if useStreaming {
		h.writeBulkResponseStreaming(w, bulkItems, shouldRefresh)
	} else {
		h.writeBulkResponseSync(w, bulkItems, shouldRefresh)
	}
}

// executeBulkOperations 执行批量操作
// 优化：按索引分组，使用Batch批量处理，减少segment数量
func (h *DocumentHandler) executeBulkOperations(bulkItems []BulkRequest) []map[string]interface{} {
	results := make([]map[string]interface{}, 0, len(bulkItems))

	// 按索引分组操作
	indexBatches := make(map[string][]BulkRequest)
	for _, item := range bulkItems {
		if item.Index != "" {
			indexBatches[item.Index] = append(indexBatches[item.Index], item)
		}
	}

	// 对每个索引，尝试批量处理
	for indexName, items := range indexBatches {
		// 获取索引实例
		idx, err := h.indexMgr.GetIndex(indexName)
		if err != nil {
			// 如果索引不存在，回退到单个处理
			for _, item := range items {
				result := h.executeBulkOperation(item)
				results = append(results, result)
			}
			continue
		}

		// 尝试批量处理：收集所有可以批量处理的操作
		batchResults := h.executeBulkOperationsBatch(idx, indexName, items)
		results = append(results, batchResults...)
	}

	return results
}

// executeBulkOperationsBatch 使用Batch批量处理同一索引的多个操作
func (h *DocumentHandler) executeBulkOperationsBatch(idx bleve.Index, indexName string, items []BulkRequest) []map[string]interface{} {
	results := make([]map[string]interface{}, 0, len(items))

	// 创建Batch
	batch := idx.NewBatch()

	// 记录每个操作在batch中的位置，用于后续构建响应
	type batchOp struct {
		item   BulkRequest
		index  bool // 是否是index/create操作
		delete bool // 是否是delete操作
	}
	batchOps := make([]batchOp, 0, len(items))

	// 收集所有可以批量处理的操作
	for _, item := range items {
		// 验证请求
		if _, err := h.validateBulkRequest(item); err != nil {
			// 验证失败，单独处理
			result := h.executeBulkOperation(item)
			results = append(results, result)
			continue
		}

		switch item.Action {
		case "index", "create":
			docID := item.ID
			if docID == "" {
				docID = uuid.New().String()
			}

			docBody := item.Source
			if docBody == nil {
				docBody = item.Doc
			}
			if docBody == nil {
				docBody = make(map[string]interface{})
			}

			// 准备索引数据
			sourceJSON, _ := json.Marshal(docBody)
			indexData := map[string]interface{}{
				"_source": string(sourceJSON),
			}
			for k, v := range docBody {
				indexData[k] = v
			}

			// 添加到batch
			if err := batch.Index(docID, indexData); err != nil {
				// 添加到batch失败，单独处理
				result := h.executeBulkOperation(item)
				results = append(results, result)
				continue
			}

			batchOps = append(batchOps, batchOp{item: item, index: true})

		case "delete":
			if item.ID != "" {
				batch.Delete(item.ID)
				batchOps = append(batchOps, batchOp{item: item, delete: true})
			} else {
				// ID为空，单独处理
				result := h.executeBulkOperation(item)
				results = append(results, result)
			}

		case "update":
			// 性能优化：支持批量 update 操作
			// 如果设置了 doc_as_upsert，直接使用 doc 作为新文档
			if item.DocAsUpsert {
				docID := item.ID
				if docID == "" {
					docID = uuid.New().String()
				}

				docBody := item.Doc
				if docBody == nil {
					docBody = item.Source
				}
				if docBody == nil {
					docBody = make(map[string]interface{})
				}

				// 准备索引数据
				sourceJSON, _ := json.Marshal(docBody)
				indexData := map[string]interface{}{
					"_source": string(sourceJSON),
				}
				for k, v := range docBody {
					indexData[k] = v
				}

				// 添加到batch
				if err := batch.Index(docID, indexData); err != nil {
					// 添加到batch失败，单独处理
					result := h.executeBulkOperation(item)
					results = append(results, result)
					continue
				}

				batchOps = append(batchOps, batchOp{item: item, index: true})
			} else {
				// 对于普通 update，简化处理：直接使用 doc 作为完整文档
				// 性能优化：不检查文档是否存在，不合并现有数据
				docID := item.ID
				if docID == "" {
					// ID为空，单独处理
					result := h.executeBulkOperation(item)
					results = append(results, result)
					continue
				}

				updateData := item.Doc
				if updateData == nil {
					updateData = item.Source
				}
				if updateData == nil {
					updateData = make(map[string]interface{})
				}

				// 准备索引数据
				sourceJSON, _ := json.Marshal(updateData)
				indexData := map[string]interface{}{
					"_source": string(sourceJSON),
				}
				for k, v := range updateData {
					indexData[k] = v
				}

				// 添加到batch
				if err := batch.Index(docID, indexData); err != nil {
					// 添加到batch失败，单独处理
					result := h.executeBulkOperation(item)
					results = append(results, result)
					continue
				}

				batchOps = append(batchOps, batchOp{item: item, index: true})
			}
		}
	}

	// 执行batch（如果有操作）
	if len(batchOps) > 0 {
		if err := idx.Batch(batch); err != nil {
			// batch执行失败，回退到单个处理
			for _, op := range batchOps {
				result := h.executeBulkOperation(op.item)
				results = append(results, result)
			}
		} else {
			// batch执行成功，构建响应
			for _, op := range batchOps {
				var opResult map[string]interface{}
				if op.index {
					// 根据操作类型确定 result 和 status
					result := "created"
					status := http.StatusCreated
					if op.item.Action == "update" {
						result = "updated"
						status = http.StatusOK
					}

					opResult = map[string]interface{}{
						"_index":        indexName,
						"_id":           op.item.ID,
						"_version":      1,
						"result":        result,
						"_shards":       map[string]interface{}{"total": 1, "successful": 1, "failed": 0},
						"_seq_no":       0,
						"_primary_term": 1,
						"status":        status,
					}
				} else if op.delete {
					opResult = map[string]interface{}{
						"_index":        indexName,
						"_id":           op.item.ID,
						"_version":      1,
						"result":        "deleted",
						"_shards":       map[string]interface{}{"total": 1, "successful": 1, "failed": 0},
						"_seq_no":       0,
						"_primary_term": 1,
						"status":        http.StatusOK,
					}
				}

				result := make(map[string]interface{})
				result[op.item.Action] = opResult
				results = append(results, result)
			}
		}
	}

	return results
}

// executeBulkOperation 执行单个批量操作
func (h *DocumentHandler) executeBulkOperation(item BulkRequest) map[string]interface{} {
	result := make(map[string]interface{})

	// 验证请求并获取索引实例
	idx, errorResult := h.validateBulkRequest(item)
	if errorResult != nil {
		// 记录验证失败的错误详情
		if status, ok := errorResult["status"].(int); ok {
			if status >= 400 {
				if errInfo, ok := errorResult["error"].(map[string]interface{}); ok {
					if reason, ok := errInfo["reason"].(string); ok {
						logger.Error("Bulk operation [%s] failed for index [%s], doc [%s]: %s",
							item.Action, item.Index, item.ID, reason)
					}
				}
			}
		}
		// 确保错误响应包含_index和_id字段（与Elasticsearch格式一致）
		errorResult["_index"] = item.Index
		if item.ID != "" {
			errorResult["_id"] = item.ID
		}
		result[item.Action] = errorResult
		return result
	}

	// 根据操作类型执行
	var opResult map[string]interface{}
	switch item.Action {
	case "index", "create":
		opResult = h.executeBulkIndexOperation(item, idx)
	case "update":
		opResult = h.executeBulkUpdateOperation(item, idx)
	case "delete":
		opResult = h.executeBulkDeleteOperation(item, idx)
	default:
		logger.Error("Unsupported bulk action [%s] for index [%s], doc [%s]", item.Action, item.Index, item.ID)
		opResult = map[string]interface{}{
			"_index": item.Index,
			"_id":    item.ID,
			"status": http.StatusBadRequest,
			"error": map[string]interface{}{
				"type":   "illegal_argument_exception",
				"reason": fmt.Sprintf("unsupported bulk action: %s", item.Action),
			},
		}
	}

	// 记录操作结果（仅记录失败的情况）
	if status, ok := opResult["status"].(int); ok && status >= 400 {
		if errInfo, ok := opResult["error"].(map[string]interface{}); ok {
			if reason, ok := errInfo["reason"].(string); ok {
				logger.Error("Bulk operation [%s] failed for index [%s], doc [%s]: %s",
					item.Action, item.Index, item.ID, reason)
			}
		}
	}

	result[item.Action] = opResult
	return result
}

// writeBulkResponseStreaming 流式方式写入 bulk 响应（避免超时）
func (h *DocumentHandler) writeBulkResponseStreaming(w http.ResponseWriter, bulkItems []BulkRequest, shouldRefresh bool) {
	// 提前发送响应头，让客户端知道连接正常
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	// 使用 Flusher 实现流式写入
	flusher, ok := w.(http.Flusher)
	if !ok {
		// 如果不支持 Flusher，回退到同步方式
		h.writeBulkResponseSync(w, bulkItems, shouldRefresh)
		return
	}

	// 开始流式响应
	startTime := time.Now()

	// 写入响应开始部分（只写入 items 数组开始，took 和 errors 在最后添加）
	// 这样可以避免 JSON 格式错误
	w.Write([]byte("{\"items\":["))
	flusher.Flush() // 立即刷新，让客户端知道连接正常

	// 分批处理，边处理边返回
	batchSize := 1000 // 每批处理1000个操作
	successCount := 0
	errorCount := 0
	hasErrors := false
	firstItem := true

	for i := 0; i < len(bulkItems); i += batchSize {
		end := i + batchSize
		if end > len(bulkItems) {
			end = len(bulkItems)
		}
		batch := bulkItems[i:end]

		// 处理当前批次
		batchResults := h.executeBulkOperations(batch)

		// 流式写入当前批次的结果
		itemCount := 0
		for _, resultItem := range batchResults {
			if !firstItem {
				w.Write([]byte(","))
			}
			firstItem = false

			// 手动编码 JSON（避免 Encoder 添加换行符）
			itemJSON, err := json.Marshal(resultItem)
			if err != nil {
				logger.Error("Failed to marshal bulk response item: %v", err)
				continue
			}
			if _, err := w.Write(itemJSON); err != nil {
				logger.Error("Failed to write bulk response: %v", err)
				return // 写入失败，停止处理
			}

			// 统计成功/失败
			for _, itemResult := range resultItem {
				if itemMap, ok := itemResult.(map[string]interface{}); ok {
					if status, ok := itemMap["status"].(int); ok {
						if status >= 400 {
							hasErrors = true
							errorCount++
						} else {
							successCount++
						}
					}
				}
			}

			itemCount++
			// 每处理50个操作刷新一次，避免缓冲区积累和超时
			if itemCount%50 == 0 {
				flusher.Flush()
			}
		}

		// 每批处理完后刷新缓冲区
		flusher.Flush()
	}

	// 写入响应结束部分
	took := time.Since(startTime).Milliseconds()

	// 写入结束部分，添加 took 和 errors
	// 格式：],\"errors\":...,\"took\":...}
	endJSON := fmt.Sprintf("],\"errors\":%t,\"took\":%d}", hasErrors, took)
	if _, err := w.Write([]byte(endJSON)); err != nil {
		logger.Error("Failed to write bulk response end: %v", err)
		return
	}

	// 刷新最终数据
	flusher.Flush()

	// 仅在出错时记录批量操作统计信息
	if len(bulkItems) > 0 && errorCount > 0 {
		logger.Warn("Bulk operation completed with errors: %d items processed, %d succeeded, %d failed (took %dms)",
			len(bulkItems), successCount, errorCount, took)
	}
}

// writeBulkResponseSync 同步方式写入 bulk 响应（用于小批量操作）
func (h *DocumentHandler) writeBulkResponseSync(w http.ResponseWriter, bulkItems []BulkRequest, shouldRefresh bool) {
	// 执行批量操作
	startTime := time.Now()
	results := h.executeBulkOperations(bulkItems)
	took := time.Since(startTime).Milliseconds()

	// 如果需要刷新，刷新所有涉及的索引
	if shouldRefresh {
		indexesToRefresh := make(map[string]bool)
		for _, item := range bulkItems {
			if item.Index != "" {
				indexesToRefresh[item.Index] = true
			}
		}
		// Bleve索引是实时更新的，不需要显式刷新
	}

	// 构建响应
	bulkResp := BulkResponse{
		Took:   took,
		Errors: false,
		Items:  results,
	}

	// 检查是否有错误并统计成功/失败数量
	successCount := 0
	errorCount := 0
	for _, resultItem := range results {
		// 遍历每个结果项（每个项是一个map，包含action作为key）
		for _, itemResult := range resultItem {
			if itemMap, ok := itemResult.(map[string]interface{}); ok {
				if status, ok := itemMap["status"].(int); ok {
					if status >= 400 {
						bulkResp.Errors = true
						errorCount++
					} else {
						successCount++
					}
				}
			}
		}
	}

	// 仅在出错时记录批量操作统计信息
	if len(bulkItems) > 0 && errorCount > 0 {
		logger.Warn("Bulk operation completed with errors: %d items processed, %d succeeded, %d failed (took %dms)",
			len(bulkItems), successCount, errorCount, took)
	}

	// 返回响应
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	// 对于大数据量，分块写入避免一次性写入导致超时
	if len(results) > 1000 {
		// 大数据量：手动构建 JSON，分块写入
		w.Write([]byte(fmt.Sprintf("{\"took\":%d,\"errors\":%t,\"items\":[", took, bulkResp.Errors)))
		for i, resultItem := range results {
			if i > 0 {
				w.Write([]byte(","))
			}
			itemJSON, err := json.Marshal(resultItem)
			if err != nil {
				logger.Error("Failed to marshal bulk response item: %v", err)
				continue
			}
			w.Write(itemJSON)
			// 每100个item刷新一次
			if (i+1)%100 == 0 {
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
			}
		}
		w.Write([]byte("]}"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	} else {
		// 小数据量：直接编码
		encoder := json.NewEncoder(w)
		if err := encoder.Encode(bulkResp); err != nil {
			logger.Error("Failed to encode bulk response: %v", err)
		}
	}
}
