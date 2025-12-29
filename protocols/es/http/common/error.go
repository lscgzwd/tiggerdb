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

package common

import (
	"fmt"
	"log"
	"net/http"
)

// APIError API错误接口
type APIError interface {
	Error() string
	Type() string
	StatusCode() int
	Response() *Response
}

// BaseError 基础错误结构体
type BaseError struct {
	ErrType    string
	Message    string
	HTTPStatus int
	Index      string
	IndexUUID  string
	Shard      string
}

// Error 实现error接口
func (e *BaseError) Error() string {
	return e.Message
}

// Type 返回错误类型
func (e *BaseError) Type() string {
	return e.ErrType
}

// StatusCode 返回HTTP状态码
func (e *BaseError) StatusCode() int {
	return e.HTTPStatus
}

// Response 返回ES格式的错误响应
func (e *BaseError) Response() *Response {
	resp := ErrorResponse(e.ErrType, e.Message)
	if resp.Error != nil {
		resp.Error.Index = e.Index
		resp.Error.IndexUUID = e.IndexUUID
		resp.Error.Shard = e.Shard
	}
	return resp
}

// 预定义错误类型

// NewIndexNotFoundError 索引不存在错误
func NewIndexNotFoundError(index string) APIError {
	return &BaseError{
		ErrType:    "index_not_found_exception",
		Message:    fmt.Sprintf("no such index [%s]", index),
		HTTPStatus: http.StatusNotFound,
		Index:      index,
	}
}

// NewDocumentNotFoundError 文档不存在错误
func NewDocumentNotFoundError(index, id string) APIError {
	return &BaseError{
		ErrType:    "not_found",
		Message:    fmt.Sprintf("document [%s] not found in index [%s]", id, index),
		HTTPStatus: http.StatusNotFound,
		Index:      index,
	}
}

// NewBadRequestError 请求参数错误
func NewBadRequestError(message string) APIError {
	return &BaseError{
		ErrType:    "illegal_argument_exception",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}

// NewInternalServerError 服务器内部错误
func NewInternalServerError(message string) APIError {
	return &BaseError{
		ErrType:    "internal_server_error",
		Message:    message,
		HTTPStatus: http.StatusInternalServerError,
	}
}

// NewConflictError 冲突错误
func NewConflictError(message string) APIError {
	return &BaseError{
		ErrType:    "version_conflict_engine_exception",
		Message:    message,
		HTTPStatus: http.StatusConflict,
	}
}

// HandleError 处理错误并写入HTTP响应
func HandleError(w http.ResponseWriter, err error) {
	if apiErr, ok := err.(APIError); ok {
		if writeErr := apiErr.Response().WriteJSON(w, apiErr.StatusCode()); writeErr != nil {
			log.Printf("ERROR: Failed to write error response: %v (original error: %v)", writeErr, err)
		}
		return
	}

	// 处理非API错误，作为内部服务器错误
	internalErr := NewInternalServerError(err.Error())
	if writeErr := internalErr.Response().WriteJSON(w, internalErr.StatusCode()); writeErr != nil {
		log.Printf("ERROR: Failed to write internal error response: %v (original error: %v)", writeErr, err)
	}
}

// HandleSuccess 处理成功响应
func HandleSuccess(w http.ResponseWriter, response *Response, statusCode int) {
	if statusCode == 0 {
		statusCode = http.StatusOK
	}
	if err := response.WriteJSON(w, statusCode); err != nil {
		log.Printf("ERROR: Failed to write success response: %v", err)
	}
}
