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
	"runtime"
	"strings"
	"sync"
)

// P2-6: 开发模式配置（全局）
var (
	devMode     bool
	devModeOnce sync.Once
	devModeMu   sync.RWMutex
)

// SetDevMode 设置开发模式（P2-6新增）
func SetDevMode(enabled bool) {
	devModeMu.Lock()
	defer devModeMu.Unlock()
	devMode = enabled
}

// IsDevMode 检查是否是开发模式（P2-6新增）
func IsDevMode() bool {
	devModeMu.RLock()
	defer devModeMu.RUnlock()
	return devMode
}

// APIError API错误接口
type APIError interface {
	Error() string
	Type() string
	StatusCode() int
	Response() *Response
}

// BaseError 基础错误结构体（P2-6: 增强错误响应）
type BaseError struct {
	ErrType    string                 // 错误类型
	Message    string                 // 错误消息
	HTTPStatus int                    // HTTP状态码
	Code       string                 // 错误码（P2-6新增）
	Index      string                 // 索引名
	IndexUUID  string                 // 索引UUID
	Shard      string                 // 分片信息
	Context    map[string]interface{} // 错误上下文（P2-6新增）
	RootCause  error                  // 根因错误（P2-6新增）
	StackTrace []string               // 堆栈跟踪（开发模式，P2-6新增）
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

// Response 返回ES格式的错误响应（P2-6: 增强错误响应）
func (e *BaseError) Response() *Response {
	resp := ErrorResponse(e.ErrType, e.Message)
	if resp.Error != nil {
		resp.Error.Index = e.Index
		resp.Error.IndexUUID = e.IndexUUID
		resp.Error.Shard = e.Shard
		resp.Error.Code = e.Code
		resp.Error.Context = e.Context
		resp.Error.Stack = e.StackTrace

		// 添加根因错误
		if e.RootCause != nil {
			if rootErr, ok := e.RootCause.(*BaseError); ok {
				resp.Error.RootCause = []*ErrorInfo{
					{
						Type:   rootErr.ErrType,
						Reason: rootErr.Message,
						Code:   rootErr.Code,
					},
				}
			} else {
				resp.Error.RootCause = []*ErrorInfo{
					{
						Type:   "internal_error",
						Reason: e.RootCause.Error(),
					},
				}
			}
		}
	}
	return resp
}

// WithCode 设置错误码（P2-6新增）
func (e *BaseError) WithCode(code string) *BaseError {
	e.Code = code
	return e
}

// WithContext 设置错误上下文（P2-6新增）
func (e *BaseError) WithContext(key string, value interface{}) *BaseError {
	if e.Context == nil {
		e.Context = make(map[string]interface{})
	}
	e.Context[key] = value
	return e
}

// WithContextMap 批量设置错误上下文（P2-6新增）
func (e *BaseError) WithContextMap(ctx map[string]interface{}) *BaseError {
	if e.Context == nil {
		e.Context = make(map[string]interface{})
	}
	for k, v := range ctx {
		e.Context[k] = v
	}
	return e
}

// WithRootCause 设置根因错误（P2-6新增）
func (e *BaseError) WithRootCause(cause error) *BaseError {
	e.RootCause = cause
	return e
}

// WithStackTrace 设置堆栈跟踪（开发模式，P2-6新增）
func (e *BaseError) WithStackTrace(skip int) *BaseError {
	e.StackTrace = captureStackTrace(skip)
	return e
}

// captureStackTrace 捕获堆栈跟踪
func captureStackTrace(skip int) []string {
	stack := make([]string, 0)
	pc := make([]uintptr, 32)
	n := runtime.Callers(skip+2, pc)

	for i := 0; i < n; i++ {
		fn := runtime.FuncForPC(pc[i])
		if fn != nil {
			file, line := fn.FileLine(pc[i])
			funcName := fn.Name()
			// 简化函数名（移除包路径）
			if idx := strings.LastIndex(funcName, "."); idx >= 0 {
				funcName = funcName[idx+1:]
			}
			stack = append(stack, fmt.Sprintf("%s:%d %s", file, line, funcName))
		}
	}

	return stack
}

// 预定义错误类型

// NewIndexNotFoundError 索引不存在错误（P2-6: 增强错误响应）
func NewIndexNotFoundError(index string) APIError {
	return &BaseError{
		ErrType:    "index_not_found_exception",
		Message:    fmt.Sprintf("no such index [%s]", index),
		HTTPStatus: http.StatusNotFound,
		Code:       "INDEX_NOT_FOUND",
		Index:      index,
	}
}

// NewDocumentNotFoundError 文档不存在错误（P2-6: 增强错误响应）
func NewDocumentNotFoundError(index, id string) APIError {
	return &BaseError{
		ErrType:    "not_found",
		Message:    fmt.Sprintf("document [%s] not found in index [%s]", id, index),
		HTTPStatus: http.StatusNotFound,
		Code:       "DOCUMENT_NOT_FOUND",
		Index:      index,
		Context: map[string]interface{}{
			"document_id": id,
			"index":       index,
		},
	}
}

// NewBadRequestError 请求参数错误（P2-6: 增强错误响应）
func NewBadRequestError(message string) APIError {
	return &BaseError{
		ErrType:    "illegal_argument_exception",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
		Code:       "BAD_REQUEST",
	}
}

// NewInternalServerError 服务器内部错误（P2-6: 增强错误响应）
func NewInternalServerError(message string) APIError {
	return &BaseError{
		ErrType:    "internal_server_error",
		Message:    message,
		HTTPStatus: http.StatusInternalServerError,
		Code:       "INTERNAL_ERROR",
	}
}

// NewConflictError 冲突错误（P2-6: 增强错误响应）
func NewConflictError(message string) APIError {
	return &BaseError{
		ErrType:    "version_conflict_engine_exception",
		Message:    message,
		HTTPStatus: http.StatusConflict,
		Code:       "VERSION_CONFLICT",
	}
}

// NewUnauthorizedError 未授权错误（P2-6: 增强错误响应）
func NewUnauthorizedError(message string) APIError {
	return &BaseError{
		ErrType:    "security_exception",
		Message:    message,
		HTTPStatus: http.StatusUnauthorized,
		Code:       "UNAUTHORIZED",
	}
}

// NewForbiddenError 禁止访问错误（P2-6: 增强错误响应）
func NewForbiddenError(message string) APIError {
	return &BaseError{
		ErrType:    "security_exception",
		Message:    message,
		HTTPStatus: http.StatusForbidden,
		Code:       "FORBIDDEN",
	}
}

// NewNotFoundError 未找到错误（通用，P2-6: 增强错误响应）
func NewNotFoundError(message string) APIError {
	return &BaseError{
		ErrType:    "not_found",
		Message:    message,
		HTTPStatus: http.StatusNotFound,
		Code:       "NOT_FOUND",
	}
}

// HandleError 处理错误并写入HTTP响应（P2-6: 增强错误响应）
// devMode: 开发模式，如果为true，会包含堆栈信息（可选参数，默认使用全局配置）
func HandleError(w http.ResponseWriter, err error, devMode ...bool) {
	isDevMode := IsDevMode()
	if len(devMode) > 0 {
		isDevMode = devMode[0]
	}

	if apiErr, ok := err.(APIError); ok {
		// 如果是BaseError且开发模式，添加堆栈信息
		if baseErr, ok := apiErr.(*BaseError); ok && isDevMode && len(baseErr.StackTrace) == 0 {
			baseErr.WithStackTrace(2)
		}

		if writeErr := apiErr.Response().WriteJSON(w, apiErr.StatusCode()); writeErr != nil {
			log.Printf("ERROR: Failed to write error response: %v (original error: %v)", writeErr, err)
		}
		return
	}

	// 处理非API错误，作为内部服务器错误
	internalErr := NewInternalServerError(err.Error())
	if isDevMode {
		internalErr.(*BaseError).WithStackTrace(2)
		internalErr.(*BaseError).WithRootCause(err)
	}
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
