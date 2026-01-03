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
	"net/http"
	"strings"
	"time"

	"github.com/lscgzwd/tiggerdb/logger"
	"github.com/lscgzwd/tiggerdb/script"

	index "github.com/blevesearch/bleve_index_api"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/directory"
	"github.com/lscgzwd/tiggerdb/metadata"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/common"
	es "github.com/lscgzwd/tiggerdb/protocols/es/index"
	"github.com/lscgzwd/tiggerdb/protocols/es/search/dsl"
	"github.com/lscgzwd/tiggerdb/search/query"
)

// DocumentHandler 文档处理器
type DocumentHandler struct {
	indexMgr        *es.IndexManager
	dirMgr          directory.DirectoryManager
	metaStore       metadata.MetadataStore
	nestedDocHelper *NestedDocumentHelper // 嵌套文档处理辅助工具
	versionMgr      *VersionManager       // 文档版本管理器
	taskMgr         *TaskManager          // 任务管理器
}

// NewDocumentHandler 创建新的文档处理器
func NewDocumentHandler(indexMgr *es.IndexManager, dirMgr directory.DirectoryManager, metaStore metadata.MetadataStore) *DocumentHandler {
	return &DocumentHandler{
		indexMgr:        indexMgr,
		dirMgr:          dirMgr,
		metaStore:       metaStore,
		nestedDocHelper: NewNestedDocumentHelper(),
		versionMgr:      NewVersionManager(), // 初始化版本管理器
		taskMgr:         NewTaskManager(),    // 初始化任务管理器
	}
}

// applyCopyToForIndex 为指定索引应用copy_to规则到文档数据
func (h *DocumentHandler) applyCopyToForIndex(indexName string, docData map[string]interface{}) {
	// 获取索引元数据
	indexMeta, err := h.metaStore.GetIndexMetadata(indexName)
	if err != nil {
		logger.Debug("Failed to get index metadata for copy_to: %v", err)
		return
	}

	// 提取copy_to配置
	copyToMap := extractCopyToConfig(indexMeta.Mapping)
	if len(copyToMap) == 0 {
		return
	}

	// 应用copy_to规则
	applyCopyTo(copyToMap, docData)
}

// CreateDocument 创建文档（自动ID）
// POST /<index>/_doc
func (h *DocumentHandler) CreateDocument(w http.ResponseWriter, r *http.Request) {
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

	// 生成自动ID
	docID := uuid.New().String()

	// 解析请求体（兼容 chunked）
	var docBody map[string]interface{}
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&docBody); err != nil {
		if err == io.EOF {
			docBody = make(map[string]interface{})
		} else {
			common.HandleError(w, common.NewBadRequestError("invalid JSON body: "+err.Error()))
			return
		}
	}

	// 获取索引实例
	idx, err := h.indexMgr.GetIndex(indexName)
	if err != nil {
		logger.Error("Failed to get index [%s]: %v", indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to get index: "+err.Error()))
		return
	}

	// 处理嵌套文档
	docData, nestedDocs, err := h.nestedDocHelper.ProcessNestedDocuments(docID, docBody)
	if err != nil {
		logger.Error("Failed to process nested documents: %v", err)
		common.HandleError(w, common.NewBadRequestError("failed to process nested documents: "+err.Error()))
		return
	}

	// P2-4: 应用copy_to规则
	h.applyCopyToForIndex(indexName, docData)

	// 索引主文档
	if err := idx.Index(docID, docData); err != nil {
		logger.Error("Failed to index document [%s] in index [%s]: %v", docID, indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to index document: "+err.Error()))
		return
	}

	// 索引嵌套文档
	for _, nestedDoc := range nestedDocs {
		if err := idx.Index(nestedDoc.ID, nestedDoc); err != nil {
			logger.Warn("Failed to index nested document [%s]: %v", nestedDoc.ID, err)
			// 继续处理其他嵌套文档，不中断流程
		}
	}

	// P1-1: 使用版本管理器创建版本信息
	versionInfo := h.versionMgr.CreateVersion(indexName, docID)

	// 返回成功响应（包含真实版本信息）
	resp := common.SuccessResponse().
		WithIndex(indexName).
		WithID(docID).
		WithResult("created").
		WithVersion(versionInfo.Version).
		WithSeqNo(versionInfo.SeqNo).
		WithPrimaryTerm(versionInfo.PrimaryTerm)
	common.HandleSuccess(w, resp, http.StatusCreated)
}

// IndexDocument 索引文档（指定ID）
// PUT /<index>/_doc/<id>
func (h *DocumentHandler) IndexDocument(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	docID := mux.Vars(r)["id"]

	// 验证索引名称和文档ID
	if err := common.ValidateIndexName(indexName); err != nil {
		common.HandleError(w, common.NewBadRequestError(err.Error()))
		return
	}
	if err := common.ValidateDocumentID(docID); err != nil {
		common.HandleError(w, common.NewBadRequestError(err.Error()))
		return
	}

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 解析请求体（兼容 chunked）
	var docBody map[string]interface{}
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&docBody); err != nil {
		if err == io.EOF {
			docBody = make(map[string]interface{})
		} else {
			common.HandleError(w, common.NewBadRequestError("invalid JSON body: "+err.Error()))
			return
		}
	}

	// 获取索引实例
	idx, err := h.indexMgr.GetIndex(indexName)
	if err != nil {
		logger.Error("Failed to get index [%s]: %v", indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to get index: "+err.Error()))
		return
	}

	// 检查文档是否已存在
	existingDoc, err := idx.Document(docID)
	docExists := err == nil && existingDoc != nil

	// 处理嵌套文档
	docData, nestedDocs, err := h.nestedDocHelper.ProcessNestedDocuments(docID, docBody)
	if err != nil {
		logger.Error("Failed to process nested documents: %v", err)
		common.HandleError(w, common.NewBadRequestError("failed to process nested documents: "+err.Error()))
		return
	}

	// 索引主文档
	if err := idx.Index(docID, docData); err != nil {
		logger.Error("Failed to index document [%s] in index [%s]: %v", docID, indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to index document: "+err.Error()))
		return
	}

	// 索引嵌套文档
	for _, nestedDoc := range nestedDocs {
		if err := idx.Index(nestedDoc.ID, nestedDoc); err != nil {
			logger.Warn("Failed to index nested document [%s]: %v", nestedDoc.ID, err)
		}
	}

	// P1-1: 使用版本管理器管理版本信息
	var versionInfo *DocumentVersion
	result := "created"
	statusCode := http.StatusCreated
	if docExists {
		// 文档已存在，递增版本
		versionInfo = h.versionMgr.IncrementVersion(indexName, docID)
		result = "updated"
		statusCode = http.StatusOK
	} else {
		// 文档不存在，创建新版本
		versionInfo = h.versionMgr.CreateVersion(indexName, docID)
	}

	// 返回成功响应（包含真实版本信息）
	resp := common.SuccessResponse().
		WithIndex(indexName).
		WithID(docID).
		WithResult(result).
		WithVersion(versionInfo.Version).
		WithSeqNo(versionInfo.SeqNo).
		WithPrimaryTerm(versionInfo.PrimaryTerm)
	common.HandleSuccess(w, resp, statusCode)
}

// GetDocument 获取文档
// GET /<index>/_doc/<id>
func (h *DocumentHandler) GetDocument(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	docID := mux.Vars(r)["id"]

	// 验证索引名称和文档ID
	if err := common.ValidateIndexName(indexName); err != nil {
		common.HandleError(w, common.NewBadRequestError(err.Error()))
		return
	}
	if err := common.ValidateDocumentID(docID); err != nil {
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

	// 获取文档
	doc, err := idx.Document(docID)
	if err != nil || doc == nil {
		// ES规范：文档不存在时返回404，但响应体包含 found: false
		notFoundResponse := map[string]interface{}{
			"_index": indexName,
			"_id":    docID,
			"found":  false,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		encoder := json.NewEncoder(w)
		if err := encoder.Encode(notFoundResponse); err != nil {
			logger.Error("Failed to encode not found response: %v", err)
		}
		return
	}

	// 提取文档字段
	docData := h.extractDocumentFields(doc)

	// P1-1: 获取文档版本信息
	versionInfo := h.versionMgr.GetVersion(indexName, docID)
	version := int64(1)
	seqNo := int64(0)
	primaryTerm := int64(1)
	if versionInfo != nil {
		version = versionInfo.Version
		seqNo = versionInfo.SeqNo
		primaryTerm = versionInfo.PrimaryTerm
	}

	// 构建ES格式的_get响应
	getResponse := map[string]interface{}{
		"_index":        indexName,
		"_id":           docID,
		"_version":      version,
		"_seq_no":       seqNo,
		"_primary_term": primaryTerm,
		"found":         true,
		"_source":       docData, // 使用实际提取的文档数据
	}

	// 直接返回响应，不使用通用响应格式
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(getResponse); err != nil {
		logger.Error("Failed to encode get response: %v", err)
	}
}

// MultiGet 批量获取文档
// GET /_mget
// POST /_mget
// GET /{index}/_mget
// POST /{index}/_mget
func (h *DocumentHandler) MultiGet(w http.ResponseWriter, r *http.Request) {
	// 获取索引名称（可能为空，表示全局搜索）
	indexName := mux.Vars(r)["index"]

	// 解析请求体（兼容 chunked）
	var requestBody map[string]interface{}
	if r.Method == http.MethodPost {
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&requestBody); err != nil && err != io.EOF {
			common.HandleError(w, common.NewBadRequestError("invalid JSON body: "+err.Error()))
			return
		}
	} else {
		// GET 请求，从查询参数解析
		// ES 的 GET /_mget 支持 ids 参数：?ids=id1,id2,id3
		idsParam := r.URL.Query().Get("ids")
		if idsParam != "" {
			// 解析逗号分隔的 ID 列表
			ids := strings.Split(idsParam, ",")
			docs := make([]map[string]interface{}, 0, len(ids))
			for _, id := range ids {
				id = strings.TrimSpace(id)
				if id != "" {
					doc := map[string]interface{}{
						"_id": id,
					}
					if indexName != "" {
						doc["_index"] = indexName
					}
					docs = append(docs, doc)
				}
			}
			requestBody = map[string]interface{}{
				"docs": docs,
			}
		} else {
			common.HandleError(w, common.NewBadRequestError("request body is required for POST, or 'ids' parameter for GET"))
			return
		}
	}

	// 解析 docs 数组
	docs, ok := requestBody["docs"].([]interface{})
	if !ok {
		common.HandleError(w, common.NewBadRequestError("request body must contain 'docs' array"))
		return
	}

	// 性能优化：按索引分组文档，复用 IndexReader
	type docRequest struct {
		index     int // 原始顺序
		indexName string
		docID     string
		source    interface{} // _source 参数
	}

	// 解析所有文档请求并按索引分组
	indexGroups := make(map[string][]docRequest)
	responses := make([]map[string]interface{}, len(docs))
	errorResponses := make(map[int]map[string]interface{}) // 存储错误响应

	for i, docItem := range docs {
		docMap, ok := docItem.(map[string]interface{})
		if !ok {
			if docID, ok := docItem.(string); ok {
				docMap = map[string]interface{}{"_id": docID}
				if indexName != "" {
					docMap["_index"] = indexName
				}
			} else {
				logger.Warn("Invalid doc item in mget request: %v", docItem)
				errorResponses[i] = map[string]interface{}{
					"error": map[string]interface{}{"type": "illegal_argument_exception", "reason": "invalid doc item"},
				}
				continue
			}
		}

		docIndexName := indexName
		if idx, ok := docMap["_index"].(string); ok && idx != "" {
			docIndexName = idx
		}

		docID, ok := docMap["_id"].(string)
		if !ok {
			errorResponses[i] = map[string]interface{}{
				"error": map[string]interface{}{"type": "illegal_argument_exception", "reason": "missing _id in doc"},
			}
			continue
		}

		if docIndexName == "" {
			errorResponses[i] = map[string]interface{}{
				"error": map[string]interface{}{"type": "illegal_argument_exception", "reason": "missing _index in doc"},
			}
			continue
		}

		indexGroups[docIndexName] = append(indexGroups[docIndexName], docRequest{
			index:     i,
			indexName: docIndexName,
			docID:     docID,
			source:    docMap["_source"],
		})
	}

	// 按索引批量获取文档，复用 IndexReader
	for idxName, requests := range indexGroups {
		if !h.dirMgr.IndexExists(idxName) {
			for _, req := range requests {
				responses[req.index] = map[string]interface{}{"_index": idxName, "_id": req.docID, "found": false}
			}
			continue
		}

		idx, err := h.indexMgr.GetIndex(idxName)
		if err != nil {
			for _, req := range requests {
				responses[req.index] = map[string]interface{}{
					"_index": idxName, "_id": req.docID,
					"error": map[string]interface{}{"type": "index_not_found_exception", "reason": err.Error()},
				}
			}
			continue
		}

		// 性能优化：复用单个IndexReader，避免每次调用idx.Document()都创建新的Reader
		// 注意：这仍然是逐个获取文档，但复用Reader可以减少Reader创建/关闭的开销
		var reader index.IndexReader
		advancedIdx, err := idx.Advanced()
		if err == nil {
			reader, err = advancedIdx.Reader()
			if err == nil {
				defer reader.Close() // ✅ 使用defer确保资源关闭
			}
		}

		for _, req := range requests {
			var doc index.Document
			var docErr error
			if reader != nil {
				doc, docErr = reader.Document(req.docID)
			} else {
				// 回退到逐个获取（每次调用idx.Document()都会创建新Reader）
				doc, docErr = idx.Document(req.docID)
			}

			if docErr != nil || doc == nil {
				responses[req.index] = map[string]interface{}{"_index": idxName, "_id": req.docID, "found": false}
				continue
			}

			docData := h.extractDocumentFields(doc)
			docData = h.filterSourceFields(docData, req.source)

			// P1-1: 获取文档版本信息
			versionInfo := h.versionMgr.GetVersion(idxName, req.docID)
			version := int64(1)
			seqNo := int64(0)
			primaryTerm := int64(1)
			if versionInfo != nil {
				version = versionInfo.Version
				seqNo = versionInfo.SeqNo
				primaryTerm = versionInfo.PrimaryTerm
			}

			responseItem := map[string]interface{}{
				"_index":        idxName,
				"_id":           req.docID,
				"_version":      version,
				"_seq_no":       seqNo,
				"_primary_term": primaryTerm,
				"found":         true,
			}
			if docData != nil {
				responseItem["_source"] = docData
			}
			responses[req.index] = responseItem
		}
	}

	// 填充错误响应
	for i, errResp := range errorResponses {
		responses[i] = errResp
	}

	// 构建 ES 格式响应
	mgetResponse := map[string]interface{}{
		"docs": responses,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(mgetResponse); err != nil {
		logger.Error("Failed to encode mget response: %v", err)
	}
}

// filterSourceFields 根据 _source 参数过滤字段
func (h *DocumentHandler) filterSourceFields(docData map[string]interface{}, source interface{}) map[string]interface{} {
	if source == nil {
		return docData
	}
	if sourceFields, ok := source.([]interface{}); ok {
		filteredData := make(map[string]interface{})
		for _, field := range sourceFields {
			if fieldStr, ok := field.(string); ok {
				if val, exists := docData[fieldStr]; exists {
					filteredData[fieldStr] = val
				}
			}
		}
		return filteredData
	}
	if sourceBool, ok := source.(bool); ok && !sourceBool {
		return nil
	}
	return docData
}

// DeleteDocument 删除文档
// DELETE /<index>/_doc/<id>
func (h *DocumentHandler) DeleteDocument(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	docID := mux.Vars(r)["id"]

	// 验证索引名称和文档ID
	if err := common.ValidateIndexName(indexName); err != nil {
		common.HandleError(w, common.NewBadRequestError(err.Error()))
		return
	}
	if err := common.ValidateDocumentID(docID); err != nil {
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

	// 删除文档（ES规范：删除操作是幂等的，即使文档不存在也返回成功）
	// 先检查文档是否存在，如果不存在返回not_found结果，但状态码仍然是200
	doc, err := idx.Document(docID)
	if err != nil || doc == nil {
		// 文档不存在，但根据ES规范，删除操作应该返回200，result为not_found
		resp := common.SuccessResponse().
			WithIndex(indexName).
			WithID(docID).
			WithResult("not_found").
			WithVersion(0)
		common.HandleSuccess(w, resp, http.StatusOK)
		return
	}

	// P1-1: 获取删除前的版本信息
	versionInfo := h.versionMgr.DeleteVersion(indexName, docID)

	// 删除文档
	if err := idx.Delete(docID); err != nil {
		logger.Error("Failed to delete document [%s] from index [%s]: %v", docID, indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to delete document: "+err.Error()))
		return
	}

	// 返回成功响应（包含删除前的版本信息）
	resp := common.SuccessResponse().
		WithIndex(indexName).
		WithID(docID).
		WithResult("deleted").
		WithVersion(versionInfo.Version).
		WithSeqNo(versionInfo.SeqNo).
		WithPrimaryTerm(versionInfo.PrimaryTerm)
	common.HandleSuccess(w, resp, http.StatusOK)
}

// HeadDocument 检查文档存在性
// HEAD /<index>/_doc/<id>
func (h *DocumentHandler) HeadDocument(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	docID := mux.Vars(r)["id"]

	// 验证索引名称和文档ID
	if err := common.ValidateIndexName(indexName); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := common.ValidateDocumentID(docID); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// 获取索引实例
	idx, err := h.indexMgr.GetIndex(indexName)
	if err != nil {
		logger.Error("Failed to get index [%s]: %v", indexName, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 检查文档是否存在
	doc, err := idx.Document(docID)
	if err != nil || doc == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// 文档存在
	w.WriteHeader(http.StatusOK)
}

// UpdateDocument 更新文档（部分更新）
// POST /{index}/_update/{id}
func (h *DocumentHandler) UpdateDocument(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	docID := mux.Vars(r)["id"]

	// 验证索引名称和文档ID
	if err := common.ValidateIndexName(indexName); err != nil {
		common.HandleError(w, common.NewBadRequestError(err.Error()))
		return
	}
	if err := common.ValidateDocumentID(docID); err != nil {
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

	// 解析请求体（ES update API格式：{"doc": {...}, "doc_as_upsert": true, ...}）
	var requestBody map[string]interface{}
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&requestBody); err != nil {
		if err == io.EOF {
			common.HandleError(w, common.NewBadRequestError("request body is required"))
			return
		}
		common.HandleError(w, common.NewBadRequestError("invalid JSON body: "+err.Error()))
		return
	}

	// 检查文档是否存在
	existingDoc, err := idx.Document(docID)
	docExists := err == nil && existingDoc != nil

	// P1-2: 处理doc_as_upsert选项（如果文档不存在且doc_as_upsert为true，则创建文档）
	docAsUpsert, _ := requestBody["doc_as_upsert"].(bool)
	if !docExists && docAsUpsert {
		// 文档不存在且doc_as_upsert=true，创建新文档
		var newDoc map[string]interface{}
		if doc, ok := requestBody["doc"].(map[string]interface{}); ok {
			newDoc = doc
		} else {
			// 如果没有doc字段，使用整个请求体（排除doc_as_upsert等元数据字段）
			newDoc = make(map[string]interface{})
			for k, v := range requestBody {
				if k != "doc_as_upsert" && k != "script" {
					newDoc[k] = v
				}
			}
		}

		// 处理嵌套文档
		docData, nestedDocs, err := h.nestedDocHelper.ProcessNestedDocuments(docID, newDoc)
		if err != nil {
			logger.Error("Failed to process nested documents: %v", err)
			common.HandleError(w, common.NewBadRequestError("failed to process nested documents: "+err.Error()))
			return
		}

		// P2-4: 应用copy_to规则
		h.applyCopyToForIndex(indexName, docData)

		// 索引主文档
		if err := idx.Index(docID, docData); err != nil {
			logger.Error("Failed to index document [%s] in index [%s]: %v", docID, indexName, err)
			common.HandleError(w, common.NewInternalServerError("failed to index document: "+err.Error()))
			return
		}

		// 索引嵌套文档
		for _, nestedDoc := range nestedDocs {
			if err := idx.Index(nestedDoc.ID, nestedDoc); err != nil {
				logger.Warn("Failed to index nested document [%s]: %v", nestedDoc.ID, err)
			}
		}

		// P1-1: 使用版本管理器创建版本信息
		versionInfo := h.versionMgr.CreateVersion(indexName, docID)

		// 返回created结果
		resp := common.SuccessResponse().
			WithIndex(indexName).
			WithID(docID).
			WithResult("created").
			WithVersion(versionInfo.Version).
			WithSeqNo(versionInfo.SeqNo).
			WithPrimaryTerm(versionInfo.PrimaryTerm)
		common.HandleSuccess(w, resp, http.StatusCreated)
		return
	}

	// 文档不存在且doc_as_upsert=false，返回404
	if !docExists {
		common.HandleError(w, common.NewDocumentNotFoundError(indexName, docID))
		return
	}

	// 提取现有文档数据
	existingData := h.extractDocumentFields(existingDoc)

	// 提取要更新的字段
	var updateData map[string]interface{}
	if doc, ok := requestBody["doc"].(map[string]interface{}); ok {
		updateData = doc
	} else {
		// 如果没有doc字段，直接使用请求体
		updateData = requestBody
	}

	// 处理script更新（ES update API支持script）
	if scriptData, ok := requestBody["script"]; ok {
		s, err := script.ParseScript(scriptData)
		if err != nil {
			logger.Error("Failed to parse update script: %v", err)
			common.HandleError(w, common.NewBadRequestError("failed to parse script: "+err.Error()))
			return
		}

		// 执行脚本更新
		engine := script.NewEngine()
		ctx := script.NewContext(existingData, existingData, s.Params)
		_, err = engine.Execute(s, ctx)
		if err != nil {
			logger.Error("Failed to execute update script: %v", err)
			common.HandleError(w, common.NewBadRequestError("failed to execute script: "+err.Error()))
			return
		}

		// 脚本执行后，ctx.Source 包含更新后的数据
		existingData = ctx.Source
		updateData = nil // 不使用 doc 更新
	}

	// 合并数据（更新现有字段，添加新字段）
	for k, v := range updateData {
		existingData[k] = v
	}

	// 处理嵌套文档
	docData, nestedDocs, err := h.nestedDocHelper.ProcessNestedDocuments(docID, existingData)
	if err != nil {
		logger.Error("Failed to process nested documents: %v", err)
		common.HandleError(w, common.NewBadRequestError("failed to process nested documents: "+err.Error()))
		return
	}

	// P2-4: 应用copy_to规则
	h.applyCopyToForIndex(indexName, docData)

	// 更新主文档
	if err := idx.Index(docID, docData); err != nil {
		logger.Error("Failed to update document [%s] in index [%s]: %v", docID, indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to update document: "+err.Error()))
		return
	}

	// 更新嵌套文档
	for _, nestedDoc := range nestedDocs {
		if err := idx.Index(nestedDoc.ID, nestedDoc); err != nil {
			logger.Warn("Failed to update nested document [%s]: %v", nestedDoc.ID, err)
		}
	}

	// P1-1: 使用版本管理器递增版本信息
	versionInfo := h.versionMgr.IncrementVersion(indexName, docID)

	// 返回成功响应（包含真实版本信息）
	resp := common.SuccessResponse().
		WithIndex(indexName).
		WithID(docID).
		WithResult("updated").
		WithVersion(versionInfo.Version).
		WithSeqNo(versionInfo.SeqNo).
		WithPrimaryTerm(versionInfo.PrimaryTerm)
	common.HandleSuccess(w, resp, http.StatusOK)
}

// CountDocuments 统计文档数量
// GET /{index}/_count
// POST /{index}/_count
func (h *DocumentHandler) CountDocuments(w http.ResponseWriter, r *http.Request) {
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

	// 解析查询请求（如果提供了的话）
	var countQuery map[string]interface{}
	if r.Method == http.MethodPost {
		// POST请求，从请求体读取查询条件（兼容 chunked）
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&countQuery); err != nil && err != io.EOF {
			common.HandleError(w, common.NewBadRequestError("invalid JSON body: "+err.Error()))
			return
		}
	}

	// 解析查询条件并执行查询
	var bleveQuery query.Query
	var rootDocCount int

	if countQuery != nil && countQuery["query"] != nil {
		// 有查询条件：精确返回匹配查询的文档总数（符合ES规范）
		// ES的count API不应该有size限制，应该返回精确的文档数
		parser := dsl.NewQueryParser()
		queryObj, ok := countQuery["query"].(map[string]interface{})
		if !ok {
			common.HandleError(w, common.NewBadRequestError("query must be an object"))
			return
		}
		parsedQuery, err := parser.ParseQuery(queryObj)
		if err != nil {
			logger.Error("Failed to parse query for count [%s]: %v", indexName, err)
			common.HandleError(w, common.NewBadRequestError("failed to parse query: "+err.Error()))
			return
		}
		bleveQuery = parsedQuery

		// ES规范：count API返回匹配查询的精确文档数
		// 不设置size限制，直接使用SearchRequest获取精确的total
		countSearchReq := bleve.NewSearchRequest(bleveQuery)
		countSearchReq.Size = 0            // 不需要返回文档，只需要总数
		countSearchReq.Fields = []string{} // 不需要字段

		countResult, err := idx.Search(countSearchReq)
		if err != nil {
			logger.Error("Failed to execute count query for index [%s]: %v", indexName, err)
			common.HandleError(w, common.NewInternalServerError("failed to count documents: "+err.Error()))
			return
		}

		// 直接返回精确的匹配文档数（ES规范）
		rootDocCount = int(countResult.Total)
		logger.Debug("Count with query for [%s]: matched %d documents", indexName, rootDocCount)
	} else {
		// 性能优化：直接使用Bleve的DocCount统计文档总数
		// 这避免了加载10000条文档的开销
		// 注意：DocCount返回的是所有文档数量（包括嵌套文档）
		// TigerDB的嵌套文档ID格式为"parent_id#nested_id"，需要过滤嵌套文档
		// 这里为了性能，先返回全部count，客户端如果需要精确的根文档数可以自己过滤
		allDocsResult, err := idx.Search(bleve.NewSearchRequest(query.NewMatchAllQuery()))
		if err != nil {
			logger.Error("Failed to get document stats for counting in index [%s]: %v", indexName, err)
			common.HandleError(w, common.NewInternalServerError("failed to count documents: "+err.Error()))
			return
		}

		// 使用DocCount更高效
		docCount := uint64(len(allDocsResult.Hits))
		if allDocsResult.Total > 0 {
			docCount = allDocsResult.Total
		}

		// 尝试估算根文档数（对于小数据集）
		// 大数据集返回总count，避免性能问题
		if docCount < 100000 {
			// 对于小数据集，搜索所有文档并统计根文档
			sampleSearchReq := bleve.NewSearchRequest(query.NewMatchAllQuery())
			sampleSearchReq.Size = int(docCount)
			sampleSearchReq.Fields = []string{}
			sampleResult, sampleErr := idx.Search(sampleSearchReq)
			if sampleErr == nil {
				for _, hit := range sampleResult.Hits {
					if !strings.Contains(hit.ID, "#") {
						rootDocCount++
					}
				}

				// 基于比例估算
				if uint64(len(sampleResult.Hits)) < sampleResult.Total {
					rootRatio := float64(rootDocCount) / float64(len(sampleResult.Hits))
					rootDocCount = int(float64(docCount) * rootRatio)
				}
			}
		} else {
			// 大数据集：返回总count作为近似值
			// 这是一个合理的折中方案，避免性能问题
			rootDocCount = int(docCount)
			logger.Debug("Large dataset detected, returning approximate count for index [%s]: %d documents", indexName, docCount)
		}

		// 移除频繁的count日志输出以提高性能
	}

	// 构建ES格式响应
	countResponse := map[string]interface{}{
		"count": rootDocCount,
		"_shards": map[string]interface{}{
			"total":      1,
			"successful": 1,
			"skipped":    0,
			"failed":     0,
		},
	}

	// 直接返回响应，不使用通用响应格式
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(countResponse); err != nil {
		logger.Error("Failed to encode count response: %v", err)
	}
}

// extractDocumentFields 从bleve Document中提取字段
// 使用Bleve提供的类型化方法，保持原样存取特性
func (h *DocumentHandler) extractDocumentFields(doc index.Document) map[string]interface{} {
	result := make(map[string]interface{})

	// 首先检查是否有_source字段，如果有，直接使用它
	var sourceData map[string]interface{}
	doc.VisitFields(func(field index.Field) {
		if field.Name() == "_source" {
			if textField, ok := field.(index.TextField); ok {
				sourceJSON := textField.Text()
				if err := json.Unmarshal([]byte(sourceJSON), &sourceData); err == nil {
					// 成功解析_source，直接返回
					for k, v := range sourceData {
						result[k] = v
					}
					return
				}
			}
		}
	})

	// 如果_source存在且成功解析，直接返回
	if len(result) > 0 {
		return result
	}

	// 否则，使用原来的逻辑提取字段
	doc.VisitFields(func(field index.Field) {
		fieldName := field.Name()

		// 跳过真正的内部字段（系统保留字段）
		// 注意：_fields.xxx 格式的字段是嵌套文档的实际字段，不应该被跳过
		internalFields := map[string]bool{
			"_id":          true,
			"_type":        true,
			"_source":      true, // 现在_source已经被特殊处理了
			"_nested_path": true,
			"_parent":      true,
			"_path":        true,
			"_position":    true,
			"_root_id":     true,
			"_timestamp":   true,
		}

		// 检查是否是内部字段（精确匹配，不包括 _fields.xxx）
		if internalFields[fieldName] {
			return
		}

		// 处理 _fields.xxx 格式的字段（嵌套文档字段）
		// 将 _fields.xxx 转换为 xxx
		if strings.HasPrefix(fieldName, "_fields.") {
			fieldName = strings.TrimPrefix(fieldName, "_fields.")
		}

		// 使用类型断言，根据字段类型调用相应的方法获取值
		// 这样能保持Bleve的原样存取特性：存什么类型，取什么类型
		var value interface{}
		switch field := field.(type) {
		case index.TextField:
			// 文本字段：直接返回字符串
			value = field.Text()
		case index.NumericField:
			// 数字字段：返回float64
			if num, err := field.Number(); err == nil {
				value = num
			} else {
				logger.Warn("Failed to get number from NumericField [%s]: %v", fieldName, err)
				return
			}
		case index.DateTimeField:
			// 日期时间字段：返回RFC3339格式字符串
			if datetime, layout, err := field.DateTime(); err == nil {
				if layout == "" {
					// 默认使用RFC3339格式
					value = datetime.Format(time.RFC3339)
				} else {
					// 使用存储时的layout格式
					value = datetime.Format(layout)
				}
			} else {
				logger.Warn("Failed to get datetime from DateTimeField [%s]: %v", fieldName, err)
				return
			}
		case index.BooleanField:
			// 布尔字段：返回bool
			if boolean, err := field.Boolean(); err == nil {
				value = boolean
			} else {
				logger.Warn("Failed to get boolean from BooleanField [%s]: %v", fieldName, err)
				return
			}
		case index.GeoPointField:
			// 地理点字段：返回[lon, lat]数组
			if lon, err := field.Lon(); err == nil {
				if lat, err := field.Lat(); err == nil {
					value = []float64{lon, lat}
				} else {
					logger.Warn("Failed to get lat from GeoPointField [%s]: %v", fieldName, err)
					return
				}
			} else {
				logger.Warn("Failed to get lon from GeoPointField [%s]: %v", fieldName, err)
				return
			}
		case index.GeoShapeField:
			// 地理形状字段：返回GeoJSON对象
			if v, err := field.GeoShape(); err == nil {
				value = v
			} else {
				logger.Warn("Failed to get geoshape from GeoShapeField [%s]: %v", fieldName, err)
				return
			}
		case index.IPField:
			// IP字段：返回IP字符串
			if ip, err := field.IP(); err == nil {
				value = ip.String()
			} else {
				logger.Warn("Failed to get IP from IPField [%s]: %v", fieldName, err)
				return
			}
		default:
			// 未知类型：尝试使用Value()获取原始字节，然后尝试解析
			// 这应该很少发生，因为Bleve的字段类型应该都被上面的case覆盖了
			logger.Warn("Unknown field type [%s]: %T, using Value()", fieldName, field)
			// Value() 返回 []byte，直接转换为字符串
			if rawBytes := field.Value(); len(rawBytes) > 0 {
				value = string(rawBytes)
			}
		}

		if value != nil {
			result[fieldName] = value
		}
	})

	return result
}

// ClearScroll 清除 scroll context
// DELETE /_search/scroll
func (h *DocumentHandler) ClearScroll(w http.ResponseWriter, r *http.Request) {
	// 读取请求体
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("Failed to read clear scroll request body: %v", err)
		common.HandleError(w, common.NewBadRequestError("failed to read request body: "+err.Error()))
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	var clearReq struct {
		ScrollIDs []string `json:"scroll_id,omitempty"` // 单个scroll_id（字符串）
	}

	// 从请求体读取
	if len(bodyBytes) > 0 {
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&clearReq); err != nil && err != io.EOF {
			// 尝试解析为数组格式
			var scrollIDsArray []string
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			decoder2 := json.NewDecoder(r.Body)
			if err2 := decoder2.Decode(&scrollIDsArray); err2 == nil {
				clearReq.ScrollIDs = scrollIDsArray
			} else {
				// 尝试解析为单个字符串
				var scrollIDStr string
				r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
				decoder3 := json.NewDecoder(r.Body)
				if err3 := decoder3.Decode(&scrollIDStr); err3 == nil {
					clearReq.ScrollIDs = []string{scrollIDStr}
				} else {
					common.HandleError(w, common.NewBadRequestError("invalid JSON body: "+err.Error()))
					return
				}
			}
		}
	} else {
		// 从查询参数读取
		scrollIDParam := r.URL.Query().Get("scroll_id")
		if scrollIDParam != "" {
			clearReq.ScrollIDs = []string{scrollIDParam}
		}
	}

	// 如果没有指定scroll_id，清除所有scroll context
	scrollMgr := GetScrollManager()
	if len(clearReq.ScrollIDs) == 0 {
		// ES规范：如果没有指定scroll_id，清除所有scroll context
		// 这里我们返回成功，但不实际清除所有（避免影响其他正在使用的scroll）
		resp := map[string]interface{}{
			"succeeded": true,
			"num_freed": 0,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		encoder.Encode(resp)
		return
	}

	// 删除指定的scroll context
	freedCount := 0
	for _, scrollID := range clearReq.ScrollIDs {
		scrollID = strings.TrimSpace(scrollID)
		if scrollID == "" {
			continue
		}
		// 检查是否存在
		if _, err := scrollMgr.GetScrollContext(scrollID); err == nil {
			scrollMgr.DeleteScrollContext(scrollID)
			freedCount++
		}
	}

	// 返回成功响应
	resp := map[string]interface{}{
		"succeeded": true,
		"num_freed": freedCount,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	encoder.Encode(resp)
}

// Scroll Scroll API - 使用 scroll_id 获取下一页结果
// POST /_search/scroll
// GET /_search/scroll
func (h *DocumentHandler) Scroll(w http.ResponseWriter, r *http.Request) {
	logger.Info("Scroll API called: method=%s, url=%s", r.Method, r.URL.String())
	// 读取请求体
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("Failed to read scroll request body: %v", err)
		common.HandleError(w, common.NewBadRequestError("failed to read request body: "+err.Error()))
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	var scrollReq struct {
		Scroll   string      `json:"scroll,omitempty"`    // 可选：更新 scroll TTL
		ScrollID interface{} `json:"scroll_id,omitempty"` // scroll_id 可能是字符串或数组
	}

	// 从请求体或查询参数读取 scroll_id
	if r.Method == http.MethodPost && len(bodyBytes) > 0 {
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&scrollReq); err != nil && err != io.EOF {
			common.HandleError(w, common.NewBadRequestError("invalid JSON body: "+err.Error()))
			return
		}
	} else {
		// GET 请求或 POST 请求但 body 为空，从查询参数读取
		scrollIDParam := r.URL.Query().Get("scroll_id")
		if scrollIDParam != "" {
			scrollReq.ScrollID = scrollIDParam
		}
		scrollReq.Scroll = r.URL.Query().Get("scroll")
	}

	// 解析 scroll_id（支持字符串或数组格式）
	var scrollID string
	if scrollReq.ScrollID != nil {
		switch v := scrollReq.ScrollID.(type) {
		case string:
			scrollID = v
		case []interface{}:
			// ES支持数组格式，取第一个
			if len(v) > 0 {
				if id, ok := v[0].(string); ok {
					scrollID = id
				}
			}
		case []string:
			// 字符串数组格式
			if len(v) > 0 {
				scrollID = v[0]
			}
		}
	}

	if scrollID == "" {
		common.HandleError(w, common.NewBadRequestError("scroll_id is required"))
		return
	}

	logger.Info("Scroll request for scroll_id: %s", scrollID)

	// 获取 scroll context
	scrollMgr := GetScrollManager()
	scrollCtx, err := scrollMgr.GetScrollContext(scrollID)
	if err != nil {
		logger.Warn("Scroll context lookup failed for [%s]: %v", scrollID, err)
		common.HandleError(w, common.NewBadRequestError("scroll context not found or expired: "+err.Error()))
		return
	}
	logger.Info("Scroll context found for [%s], index=%s, from=%d, size=%d", scrollID, scrollCtx.IndexName, scrollCtx.From, scrollCtx.Size)

	// 更新过期时间（每次 scroll 请求都应该刷新 TTL）
	if scrollReq.Scroll != "" {
		scrollTTL, err := parseScrollTTL(scrollReq.Scroll)
		if err != nil {
			common.HandleError(w, common.NewBadRequestError("invalid scroll parameter: "+err.Error()))
			return
		}
		scrollCtx.ExpiresAt = time.Now().Add(scrollTTL)
	} else {
		// 如果没有指定新的 scroll TTL，使用默认的 1 分钟刷新
		scrollCtx.ExpiresAt = time.Now().Add(1 * time.Minute)
	}

	// 获取索引实例
	idx, err := h.indexMgr.GetIndex(scrollCtx.IndexName)
	if err != nil {
		logger.Error("Failed to get index [%s]: %v", scrollCtx.IndexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to get index: "+err.Error()))
		return
	}

	// 构建搜索请求（使用保存的查询参数）
	searchReq := &SearchRequest{
		Query:        scrollCtx.Query,
		Sort:         scrollCtx.Sort,
		Source:       scrollCtx.Source,
		Size:         scrollCtx.Size,
		Aggregations: scrollCtx.Aggregations,
	}

	// 性能优化：支持两种scroll方式
	// 方式1：search_after（性能优先，用于顺序遍历大数据集，O(1)复杂度）
	// 方式2：from（兼容优先，支持跳页，但深分页性能差，O(N)复杂度）
	// 注意：ES的scroll API主要设计用于顺序遍历，不推荐跳页
	// 但为了兼容旧客户端，我们保留from方式作为备选

	// 判断使用哪种方式
	// 如果有LastSort，说明客户端使用search_after方式，继续使用
	// 如果没有LastSort且From>0，使用from方式（兼容旧客户端）
	useSearchAfter := false
	if scrollCtx.LastSort != nil {
		useSearchAfter = true
	} else if scrollCtx.From > 0 {
		useSearchAfter = false // 旧客户端使用from方式
	} else {
		// 第一次scroll，优先使用search_after以获得最佳性能
		useSearchAfter = true
	}

	if useSearchAfter {
		// 使用search_after（性能优先，顺序遍历）
		if len(scrollCtx.Sort) > 0 {
			searchReq.SearchAfter = scrollCtx.LastSort
			logger.Debug("Scroll [%s] using search_after (sequential mode)", scrollID)
		} else {
			// 没有sort配置时，使用默认sort并使用search_after
			// 默认sort：按_id升序，确保scroll结果稳定
			defaultSort := []interface{}{
				map[string]interface{}{"_id": map[string]interface{}{"order": "asc"}},
			}
			searchReq.Sort = defaultSort
			searchReq.SearchAfter = scrollCtx.LastSort
			logger.Debug("Scroll [%s] using search_after with default sort: %v", scrollID, scrollCtx.LastSort)
		}
	} else {
		// 使用from分页（兼容旧客户端，支持跳页，但深分页性能差）
		searchReq.From = scrollCtx.From
		logger.Debug("Scroll [%s] using from pagination (supports jumping but slow for deep pages)", scrollID)
		// 确保有sort配置，以便后续可以使用search_after
		if len(scrollCtx.Sort) > 0 {
			searchReq.Sort = scrollCtx.Sort
		}
	}

	// 执行搜索
	searchResponse, err := h.executeSearchInternal(idx, scrollCtx.IndexName, searchReq)
	if err != nil {
		if apiErr, ok := err.(common.APIError); ok {
			common.HandleError(w, apiErr)
		} else {
			common.HandleError(w, common.NewInternalServerError(err.Error()))
		}
		return
	}

	// 更新 scroll context（记录最后一个结果的 sort 值）
	// 添加 scroll_id 到响应
	searchResponse["_scroll_id"] = scrollID

	// 检查是否有结果，以及是否还有更多数据
	hitsCount := 0
	if hitsWrapper, ok := searchResponse["hits"].(map[string]interface{}); ok {
		// hits["hits"] 的类型是 []map[string]interface{}，不是 []interface{}
		if hitsList, ok := hitsWrapper["hits"].([]map[string]interface{}); ok {
			hitsCount = len(hitsList)
			// 如果有结果，更新 last_sort
			if hitsCount > 0 {
				// 转换最后一个 hit
				lastHit := hitsList[hitsCount-1]
				if sortVals, ok := lastHit["sort"].([]interface{}); ok && len(sortVals) > 0 {
					scrollMgr.UpdateScrollContext(scrollID, sortVals)
				} else {
					// 如果没有sort值，使用from分页
					scrollMgr.UpdateScrollContext(scrollID, nil)
				}
			}
		} else {
			logger.Warn("Scroll [%s] hits type assertion failed: %T", scrollID, hitsWrapper["hits"])
		}
	}

	// 只有当没有结果时才删除 scroll context
	// 注意：不能根据 hitsCount < scrollCtx.Size 来判断，因为客户端可能还需要继续 scroll
	// 让客户端自己决定何时清除 scroll context（通过 DELETE /_search/scroll）
	// 或者等待 scroll context 自然过期
	if hitsCount == 0 {
		scrollMgr.DeleteScrollContext(scrollID)
		logger.Info("Scroll [%s] completed, no more results", scrollID)
	}

	// 返回响应
	// 设置响应头，确保连接正确关闭
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(searchResponse); err != nil {
		logger.Error("Failed to encode scroll response: %v", err)
	}
}
