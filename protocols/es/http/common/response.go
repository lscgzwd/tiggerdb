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

// Package common 提供ES协议专用的HTTP响应和错误处理
// 注意：此包仅用于ES协议，Redis和MySQL协议使用TCP，不需要HTTP响应格式
package common

import (
	"encoding/json"
	"net/http"
)

// Response ES兼容的统一响应格式
type Response struct {
	Took     int64       `json:"took,omitempty"`      // 执行时间(毫秒)
	TimedOut bool        `json:"timed_out,omitempty"` // 是否超时
	Shards   *ShardsInfo `json:"_shards,omitempty"`   // 分片信息

	// 索引操作响应
	Acknowledged bool   `json:"acknowledged,omitempty"`  // 是否确认
	Index        string `json:"_index,omitempty"`        // 索引名
	Id           string `json:"_id,omitempty"`           // 文档ID
	Version      int64  `json:"_version,omitempty"`      // 版本号
	Result       string `json:"result,omitempty"`        // 操作结果
	SeqNo        int64  `json:"_seq_no,omitempty"`       // 序列号
	PrimaryTerm  int64  `json:"_primary_term,omitempty"` // 主分片term

	// 搜索响应
	Hits         *HitsInfo   `json:"hits,omitempty"`         // 命中结果
	Aggregations interface{} `json:"aggregations,omitempty"` // 聚合结果

	// 错误响应
	Error *ErrorInfo `json:"error,omitempty"` // 错误信息

	// 索引信息
	Status  string                 `json:"status,omitempty"`  // 状态
	Indices map[string]interface{} `json:"indices,omitempty"` // 索引信息

	// 通用数据字段
	Data interface{} `json:"-"` // 额外数据，不序列化到JSON
}

// ShardsInfo 分片信息
type ShardsInfo struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
}

// HitsInfo 搜索命中信息
type HitsInfo struct {
	Total    *TotalInfo    `json:"total"`
	MaxScore float64       `json:"max_score,omitempty"`
	Hits     []interface{} `json:"hits"`
}

// TotalInfo 总数信息
type TotalInfo struct {
	Value    int64  `json:"value"`
	Relation string `json:"relation"`
}

// ErrorInfo 错误信息
type ErrorInfo struct {
	Type      string `json:"type"`
	Reason    string `json:"reason"`
	Index     string `json:"index,omitempty"`
	IndexUUID string `json:"index_uuid,omitempty"`
	Shard     string `json:"shard,omitempty"`
}

// NewResponse 创建新的响应
func NewResponse() *Response {
	return &Response{
		Took:     0,
		TimedOut: false,
	}
}

// WithTook 设置执行时间
func (r *Response) WithTook(took int64) *Response {
	r.Took = took
	return r
}

// WithTimeout 设置超时状态
func (r *Response) WithTimeout(timeout bool) *Response {
	r.TimedOut = timeout
	return r
}

// WithShards 设置分片信息
func (r *Response) WithShards(total, successful, skipped, failed int) *Response {
	r.Shards = &ShardsInfo{
		Total:      total,
		Successful: successful,
		Skipped:    skipped,
		Failed:     failed,
	}
	return r
}

// WithIndex 设置索引信息
func (r *Response) WithIndex(index string) *Response {
	r.Index = index
	return r
}

// WithID 设置文档ID
func (r *Response) WithID(id string) *Response {
	r.Id = id
	return r
}

// WithVersion 设置版本号
func (r *Response) WithVersion(version int64) *Response {
	r.Version = version
	return r
}

// WithResult 设置操作结果
func (r *Response) WithResult(result string) *Response {
	r.Result = result
	return r
}

// WithAcknowledged 设置确认状态
func (r *Response) WithAcknowledged(acknowledged bool) *Response {
	r.Acknowledged = acknowledged
	return r
}

// WithHits 设置搜索命中结果
func (r *Response) WithHits(total int64, maxScore float64, hits []interface{}) *Response {
	r.Hits = &HitsInfo{
		Total: &TotalInfo{
			Value:    total,
			Relation: "eq",
		},
		MaxScore: maxScore,
		Hits:     hits,
	}
	return r
}

// WithError 设置错误信息
func (r *Response) WithError(errType, reason string) *Response {
	r.Error = &ErrorInfo{
		Type:   errType,
		Reason: reason,
	}
	return r
}

// WithData 设置额外数据
func (r *Response) WithData(data interface{}) *Response {
	r.Data = data
	return r
}

// WriteJSON 将响应写入HTTP响应
func (r *Response) WriteJSON(w http.ResponseWriter, statusCode int) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ") // 开发环境格式化输出

	// 如果Data字段有值，将其内容合并到响应中
	if r.Data != nil {
		// 使用反射或直接构建map，避免双重序列化以提高性能
		responseMap := r.toMap()

		// 将Data内容合并到响应map中
		if dataMap, ok := r.Data.(map[string]interface{}); ok {
			for k, v := range dataMap {
				responseMap[k] = v
			}
		} else {
			// 如果Data不是map，直接添加
			responseMap["data"] = r.Data
		}

		return encoder.Encode(responseMap)
	}

	return encoder.Encode(r)
}

// toMap 将Response结构转换为map，避免双重序列化
func (r *Response) toMap() map[string]interface{} {
	result := make(map[string]interface{})

	if r.Took > 0 {
		result["took"] = r.Took
	}
	if r.TimedOut {
		result["timed_out"] = r.TimedOut
	}
	if r.Shards != nil {
		result["_shards"] = r.Shards
	}
	if r.Acknowledged {
		result["acknowledged"] = r.Acknowledged
	}
	if r.Index != "" {
		result["_index"] = r.Index
	}
	if r.Id != "" {
		result["_id"] = r.Id
	}
	if r.Version > 0 {
		result["_version"] = r.Version
	}
	if r.Result != "" {
		result["result"] = r.Result
	}
	if r.SeqNo > 0 {
		result["_seq_no"] = r.SeqNo
	}
	if r.PrimaryTerm > 0 {
		result["_primary_term"] = r.PrimaryTerm
	}
	if r.Hits != nil {
		result["hits"] = r.Hits
	}
	if r.Aggregations != nil {
		result["aggregations"] = r.Aggregations
	}
	if r.Error != nil {
		result["error"] = r.Error
	}
	if r.Status != "" {
		result["status"] = r.Status
	}
	if r.Indices != nil {
		result["indices"] = r.Indices
	}

	return result
}

// SuccessResponse 创建成功响应
func SuccessResponse() *Response {
	return NewResponse().WithAcknowledged(true)
}

// ErrorResponse 创建错误响应
func ErrorResponse(errType, reason string) *Response {
	return NewResponse().WithError(errType, reason)
}
