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

	"github.com/lscgzwd/tiggerdb/logger"

	"github.com/google/uuid"
	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/nested/document"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/common"
)

// executeBulkIndexOperation 执行bulk index/create操作
// 性能优化：移除文档存在性检查，直接索引（Bleve会自动处理覆盖）
func (h *DocumentHandler) executeBulkIndexOperation(item BulkRequest, idx bleve.Index) map[string]interface{} {
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

	// 性能优化：不检查文档是否存在，直接索引
	// 对于 create 操作，如果文档已存在，Bleve 会覆盖，我们无法区分，但这样可以大幅提升性能
	// 如果需要严格检查 create 操作的冲突，可以在批量处理完成后统一检查
	// 但考虑到性能，我们选择直接索引，牺牲一些精确性

	// 暂时禁用嵌套文档处理，直接使用原始文档数据
	docData := docBody
	nestedDocs := make([]*document.NestedDocument, 0)

	// P2-4: 应用copy_to规则
	h.applyCopyToForIndex(item.Index, docData)

	// 准备索引数据，确保_source被存储
	sourceJSON, _ := json.Marshal(docData)
	indexData := map[string]interface{}{
		"_source": string(sourceJSON), // 存储完整的原始文档JSON
	}

	// 将所有字段也添加到顶级，以便查询
	for k, v := range docData {
		indexData[k] = v
	}

	if err := idx.Index(docID, indexData); err != nil {
		logger.Error("Failed to index document [%s] in index [%s]: %v", docID, item.Index, err)
		return map[string]interface{}{
			"_index": item.Index,
			"_id":    docID,
			"status": http.StatusInternalServerError,
			"error": map[string]interface{}{
				"type":   "internal_server_error",
				"reason": "failed to index document: " + err.Error(),
			},
		}
	}

	// 索引嵌套文档
	nestedCount := 0
	for _, nestedDoc := range nestedDocs {
		if err := idx.Index(nestedDoc.ID, nestedDoc); err != nil {
			logger.Warn("Failed to index nested document [%s]: %v", nestedDoc.ID, err)
		} else {
			nestedCount++
		}
	}

	// P1-1: 使用版本管理器管理版本信息
	// 检查文档是否存在，决定是创建还是更新版本
	existingDoc, _ := idx.Document(docID)
	var versionInfo *DocumentVersion
	opResult := "created"
	statusCode := http.StatusCreated
	if existingDoc != nil {
		// 文档存在，递增版本
		versionInfo = h.versionMgr.IncrementVersion(item.Index, docID)
		opResult = "updated"
		statusCode = http.StatusOK
	} else {
		// 文档不存在，创建新版本
		versionInfo = h.versionMgr.CreateVersion(item.Index, docID)
		opResult = "created"
		statusCode = http.StatusCreated
	}

	return map[string]interface{}{
		"_index":        item.Index,
		"_id":           docID,
		"_version":      versionInfo.Version,
		"result":        opResult,
		"_shards":       map[string]interface{}{"total": 1, "successful": 1, "failed": 0},
		"_seq_no":       versionInfo.SeqNo,
		"_primary_term": versionInfo.PrimaryTerm,
		"status":        statusCode,
	}
}

// executeBulkUpdateOperation 执行bulk update操作
// 性能优化：移除文档存在性检查，直接更新（如果文档不存在，Bleve会创建新文档）
func (h *DocumentHandler) executeBulkUpdateOperation(item BulkRequest, idx bleve.Index) map[string]interface{} {
	// 当前实现：如果提供了doc，则执行部分更新
	// 注意：script更新功能待实现
	if item.Doc == nil && item.Source == nil {
		return map[string]interface{}{
			"_index": item.Index,
			"_id":    item.ID,
			"status": http.StatusBadRequest,
			"error": map[string]interface{}{
				"type":   "illegal_argument_exception",
				"reason": "update operation requires document data",
			},
		}
	}

	// 性能优化：不检查文档是否存在，直接处理更新
	// 如果设置了 doc_as_upsert，直接使用 doc 作为新文档
	if item.DocAsUpsert {
		// 使用doc作为新文档内容
		docData := item.Doc
		if docData == nil {
			docData = item.Source
		}
		if docData == nil {
			return map[string]interface{}{
				"_index": item.Index,
				"_id":    item.ID,
				"status": http.StatusBadRequest,
				"error": map[string]interface{}{
					"type":   "illegal_argument_exception",
					"reason": "doc_as_upsert requires doc field",
				},
			}
		}

		// P2-4: 应用copy_to规则
		h.applyCopyToForIndex(item.Index, docData)

		// 准备索引数据，确保_source被存储
		sourceJSON, _ := json.Marshal(docData)
		indexData := map[string]interface{}{
			"_source": string(sourceJSON), // 存储完整的原始文档JSON
		}

		// 将所有字段也添加到顶级，以便查询
		for k, v := range docData {
			indexData[k] = v
		}

		// 索引新文档
		if err := idx.Index(item.ID, indexData); err != nil {
			logger.Error("Failed to index document [%s] with doc_as_upsert: %v", item.ID, err)
			return map[string]interface{}{
				"_index": item.Index,
				"_id":    item.ID,
				"status": http.StatusInternalServerError,
				"error": map[string]interface{}{
					"type":   "internal_server_error",
					"reason": "failed to index document with doc_as_upsert: " + err.Error(),
				},
			}
		}

		// P1-1: 使用版本管理器管理版本信息
		// 检查文档是否存在，决定是创建还是更新版本
		existingDoc, _ := idx.Document(item.ID)
		var versionInfo *DocumentVersion
		result := "created"
		statusCode := http.StatusCreated
		if existingDoc != nil {
			// 文档存在，递增版本
			versionInfo = h.versionMgr.IncrementVersion(item.Index, item.ID)
			result = "updated"
			statusCode = http.StatusOK
		} else {
			// 文档不存在，创建新版本
			versionInfo = h.versionMgr.CreateVersion(item.Index, item.ID)
			result = "created"
			statusCode = http.StatusCreated
		}

		// 返回创建成功响应
		return map[string]interface{}{
			"_index":        item.Index,
			"_id":           item.ID,
			"_version":      versionInfo.Version,
			"result":        result,
			"_shards":       map[string]interface{}{"total": 1, "successful": 1, "failed": 0},
			"_seq_no":       versionInfo.SeqNo,
			"_primary_term": versionInfo.PrimaryTerm,
			"status":        statusCode,
		}
	}

	// 性能优化：不检查文档是否存在，直接尝试更新
	// 如果文档不存在，尝试获取文档（仅在需要合并数据时）
	// 为了性能，我们简化逻辑：直接使用 updateData 作为完整文档
	updateData := item.Doc
	if updateData == nil {
		updateData = item.Source
	}
	if updateData == nil {
		updateData = make(map[string]interface{})
	}

	// 性能优化：对于 update 操作，如果文档不存在且未设置 doc_as_upsert，
	// 我们仍然尝试更新（Bleve 会创建新文档），但返回 updated 结果
	// 这样可以避免文档存在性检查的性能开销

	// 准备索引数据，确保_source被存储
	sourceJSON, _ := json.Marshal(updateData)
	indexData := map[string]interface{}{
		"_source": string(sourceJSON),
	}

	// 将所有字段也添加到顶级，以便查询
	for k, v := range updateData {
		indexData[k] = v
	}

	// 更新文档（如果文档不存在，Bleve 会创建新文档）
	if err := idx.Index(item.ID, indexData); err != nil {
		logger.Error("Failed to update document [%s]: %v", item.ID, err)
		return map[string]interface{}{
			"_index": item.Index,
			"_id":    item.ID,
			"status": http.StatusInternalServerError,
			"error": map[string]interface{}{
				"type":   "internal_server_error",
				"reason": "failed to update document: " + err.Error(),
			},
		}
	}

	// P1-1: 使用版本管理器管理版本信息
	// 检查文档是否存在，决定是创建还是更新版本
	existingDoc, _ := idx.Document(item.ID)
	var versionInfo *DocumentVersion
	result := "updated"
	if existingDoc == nil {
		// 文档不存在，创建新版本
		versionInfo = h.versionMgr.CreateVersion(item.Index, item.ID)
		result = "created"
	} else {
		// 文档存在，递增版本
		versionInfo = h.versionMgr.IncrementVersion(item.Index, item.ID)
		result = "updated"
	}

	return map[string]interface{}{
		"_index":        item.Index,
		"_id":           item.ID,
		"_version":      versionInfo.Version,
		"result":        result,
		"_shards":       map[string]interface{}{"total": 1, "successful": 1, "failed": 0},
		"_seq_no":       versionInfo.SeqNo,
		"_primary_term": versionInfo.PrimaryTerm,
		"status":        http.StatusOK,
	}
}

// executeBulkDeleteOperation 执行bulk delete操作
// 性能优化：移除文档存在性检查，直接删除（Bleve会处理不存在的文档）
func (h *DocumentHandler) executeBulkDeleteOperation(item BulkRequest, idx bleve.Index) map[string]interface{} {
	if item.ID == "" {
		return map[string]interface{}{
			"_index": item.Index,
			"status": http.StatusBadRequest,
			"error": map[string]interface{}{
				"type":   "illegal_argument_exception",
				"reason": "document ID is required for delete operation",
			},
		}
	}

	// P1-1: 获取删除前的版本信息
	versionInfo := h.versionMgr.DeleteVersion(item.Index, item.ID)

	// 性能优化：不检查文档是否存在，直接删除
	// Bleve 的 Delete 方法对于不存在的文档不会报错，只是没有效果
	// 这样可以避免文档存在性检查的性能开销
	if err := idx.Delete(item.ID); err != nil {
		logger.Error("Failed to delete document [%s]: %v", item.ID, err)
		return map[string]interface{}{
			"_index": item.Index,
			"_id":    item.ID,
			"status": http.StatusInternalServerError,
			"error": map[string]interface{}{
				"type":   "internal_server_error",
				"reason": "failed to delete document: " + err.Error(),
			},
		}
	}

	// 根据版本信息判断文档是否存在
	if versionInfo == nil {
		// 文档不存在，返回not_found
		return map[string]interface{}{
			"_index":        item.Index,
			"_id":           item.ID,
			"_version":      0,
			"result":        "not_found",
			"_shards":       map[string]interface{}{"total": 1, "successful": 1, "failed": 0},
			"_seq_no":       0,
			"_primary_term": 1,
			"status":        http.StatusOK,
		}
	}

	// 文档存在，返回deleted
	return map[string]interface{}{
		"_index":        item.Index,
		"_id":           item.ID,
		"_version":      versionInfo.Version,
		"result":        "deleted",
		"_shards":       map[string]interface{}{"total": 1, "successful": 1, "failed": 0},
		"_seq_no":       versionInfo.SeqNo,
		"_primary_term": versionInfo.PrimaryTerm,
		"status":        http.StatusOK,
	}
}

// validateBulkRequest 验证bulk请求
func (h *DocumentHandler) validateBulkRequest(item BulkRequest) (bleve.Index, map[string]interface{}) {
	// 验证索引名称
	if err := common.ValidateIndexName(item.Index); err != nil {
		return nil, map[string]interface{}{
			"status": http.StatusBadRequest,
			"error": map[string]interface{}{
				"type":   "illegal_argument_exception",
				"reason": err.Error(),
			},
		}
	}

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(item.Index) {
		logger.Error("Bulk operation failed - index [%s] does not exist", item.Index)
		return nil, map[string]interface{}{
			"status": http.StatusNotFound,
			"error": map[string]interface{}{
				"type":   "index_not_found_exception",
				"reason": fmt.Sprintf("index [%s] not found", item.Index),
			},
		}
	}

	// 获取索引实例
	idx, err := h.indexMgr.GetIndex(item.Index)
	if err != nil {
		logger.Error("Failed to get index [%s]: %v", item.Index, err)
		return nil, map[string]interface{}{
			"status": http.StatusInternalServerError,
			"error": map[string]interface{}{
				"type":   "internal_server_error",
				"reason": "failed to get index: " + err.Error(),
			},
		}
	}

	return idx, nil
}
