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
	"encoding/json"
	"fmt"
	"io"
	"github.com/lscgzwd/tiggerdb/logger"
	"net/http"
	"strings"

	"github.com/lscgzwd/tiggerdb/protocols/es/http/common"
)

// MultiSearch 多索引搜索API
// POST /_msearch
// ES的msearch API使用NDJSON格式：每两行为一组，第一行是header（包含index和可选的preference、routing），第二行是查询体
func (h *DocumentHandler) MultiSearch(w http.ResponseWriter, r *http.Request) {
	// 检查Content-Type
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/x-ndjson" && contentType != "application/json" {
		common.HandleError(w, common.NewBadRequestError("Content-Type must be application/x-ndjson"))
		return
	}

	// 检查请求体大小（兼容 chunked，可考虑 io.LimitReader）

	// 读取请求体
	reader := bufio.NewReader(r.Body)
	defer r.Body.Close()

	// 解析多搜索请求
	searchRequests, err := h.parseMultiSearchRequest(reader)
	if err != nil {
		common.HandleError(w, err)
		return
	}

	// 执行多个搜索请求
	results := make([]map[string]interface{}, 0, len(searchRequests))
	for _, req := range searchRequests {
		result := h.executeSingleMultiSearch(req)
		results = append(results, result)
	}

	// 构建响应（ES msearch响应格式：每个结果一行JSON）
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	for _, result := range results {
		if err := encoder.Encode(result); err != nil {
			logger.Error("Failed to encode multi-search result: %v", err)
			break
		}
	}
}

// MultiSearchRequest 多搜索请求
type MultiSearchRequest struct {
	Header map[string]interface{} // header行：包含index等
	Body   map[string]interface{} // 查询体行：包含query等
}

// parseMultiSearchRequest 解析多搜索请求（NDJSON格式）
func (h *DocumentHandler) parseMultiSearchRequest(reader *bufio.Reader) ([]MultiSearchRequest, error) {
	searchRequests := make([]MultiSearchRequest, 0)
	lineNum := 0
	var currentHeader map[string]interface{}

	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			// 如果还有未处理的header，说明格式错误
			if currentHeader != nil {
				logger.Warn("Incomplete multi-search request at end of body")
				// 使用默认查询完成请求
				searchRequests = append(searchRequests, MultiSearchRequest{
					Header: currentHeader,
					Body:   map[string]interface{}{"query": map[string]interface{}{"match_all": map[string]interface{}{}}},
				})
			}
			break
		}
		if err != nil {
			logger.Error("Failed to read multi-search request line %d: %v", lineNum, err)
			return nil, common.NewBadRequestError("failed to read request: " + err.Error())
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
			return nil, common.NewBadRequestError(fmt.Sprintf("invalid JSON at line %d: %v", lineNum, err))
		}

		// 判断是header行还是查询体行
		// Header行包含index字段，查询体行包含query等字段
		if _, hasIndex := jsonLine["index"]; hasIndex {
			// 这是header行
			if currentHeader != nil {
				// 上一个请求没有查询体，使用默认查询
				searchRequests = append(searchRequests, MultiSearchRequest{
					Header: currentHeader,
					Body:   map[string]interface{}{"query": map[string]interface{}{"match_all": map[string]interface{}{}}},
				})
			}
			currentHeader = jsonLine
		} else {
			// 这是查询体行
			if currentHeader == nil {
				return nil, common.NewBadRequestError(fmt.Sprintf("query body without header at line %d", lineNum))
			}
			searchRequests = append(searchRequests, MultiSearchRequest{
				Header: currentHeader,
				Body:   jsonLine,
			})
			currentHeader = nil
		}
	}

	return searchRequests, nil
}

// executeSingleMultiSearch 执行单个多搜索请求
func (h *DocumentHandler) executeSingleMultiSearch(req MultiSearchRequest) map[string]interface{} {
	// 获取索引名称
	indexName, ok := req.Header["index"].(string)
	if !ok || indexName == "" {
		return map[string]interface{}{
			"error": map[string]interface{}{
				"type":   "illegal_argument_exception",
				"reason": "missing or invalid index in header",
			},
		}
	}

	// 验证索引名称
	if err := common.ValidateIndexName(indexName); err != nil {
		return map[string]interface{}{
			"error": map[string]interface{}{
				"type":   "illegal_argument_exception",
				"reason": err.Error(),
			},
		}
	}

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		return map[string]interface{}{
			"error": map[string]interface{}{
				"type":   "index_not_found_exception",
				"reason": "no such index [" + indexName + "]",
			},
		}
	}

	// 获取索引实例
	idx, err := h.indexMgr.GetIndex(indexName)
	if err != nil {
		logger.Error("Failed to get index [%s] for multi-search: %v", indexName, err)
		return map[string]interface{}{
			"error": map[string]interface{}{
				"type":   "internal_server_error",
				"reason": "failed to get index: " + err.Error(),
			},
		}
	}

	// 解析查询体为SearchRequest格式
	var searchReq SearchRequest
	if bodyBytes, err := json.Marshal(req.Body); err == nil {
		if err := json.Unmarshal(bodyBytes, &searchReq); err != nil {
			logger.Error("Failed to parse search request body: %v", err)
			return map[string]interface{}{
				"error": map[string]interface{}{
					"type":   "illegal_argument_exception",
					"reason": "invalid search request body: " + err.Error(),
				},
			}
		}
	}

	// 执行搜索（复用Search方法的逻辑）
	result, err := h.executeSearchInternal(idx, indexName, &searchReq)
	if err != nil {
		logger.Error("Failed to execute search for index [%s]: %v", indexName, err)
		// 将错误转换为ES格式
		errorType := "internal_server_error"
		reason := err.Error()
		if apiErr, ok := err.(common.APIError); ok {
			errorType = apiErr.Type()
			reason = apiErr.Error()
		}
		return map[string]interface{}{
			"error": map[string]interface{}{
				"type":   errorType,
				"reason": reason,
			},
		}
	}

	return result
}
