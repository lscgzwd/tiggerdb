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

package server

import (
	"compress/gzip"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/lscgzwd/tiggerdb/protocols/es/http/common"
)

// Middleware 中间件函数类型
type Middleware func(http.HandlerFunc) http.HandlerFunc

// LoggingMiddleware 请求日志中间件
func LoggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// 包装ResponseWriter以捕获状态码
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// 调用下一个处理器
		next(rw, r)

		// 记录请求日志（仅在错误或慢请求时输出，减少日志量以提高性能）
		duration := time.Since(start)
		if rw.statusCode >= 400 || duration > 1*time.Second {
			log.Printf("[%s] %s %s %d %v",
				r.Method,
				r.RequestURI,
				r.RemoteAddr,
				rw.statusCode,
				duration,
			)
		}
	}
}

// CORSMiddleware CORS跨域中间件
func CORSMiddleware(allowedOrigins []string) Middleware {
	allowAll := len(allowedOrigins) == 1 && allowedOrigins[0] == "*"

	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			// 检查Origin是否被允许
			allowed := allowAll
			if !allowed {
				for _, allowedOrigin := range allowedOrigins {
					if origin == allowedOrigin {
						allowed = true
						break
					}
				}
			}

			if allowed {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")
				w.Header().Set("Access-Control-Max-Age", "86400") // 24小时
				// 告知缓存代理不同Origin的响应可能不同
				w.Header().Add("Vary", "Origin")
			}

			// 处理预检请求
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}

			next(w, r)
		}
	}
}

// RateLimitMiddleware 限流中间件
func RateLimitMiddleware(rpm int) Middleware {
	if rpm <= 0 {
		// 禁用限流
		return func(next http.HandlerFunc) http.HandlerFunc {
			return next
		}
	}

	// 简化的令牌桶算法实现
	// 在生产环境中，应该使用更复杂的限流算法
	tokens := make(chan struct{}, rpm)
	go func() {
		ticker := time.NewTicker(time.Minute / time.Duration(rpm))
		defer ticker.Stop()
		for range ticker.C {
			select {
			case tokens <- struct{}{}:
			default:
			}
		}
	}()

	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-tokens:
				next(w, r)
			default:
				if err := common.ErrorResponse("rate_limit_exceeded", "too many requests").WriteJSON(w, http.StatusTooManyRequests); err != nil {
					log.Printf("ERROR: Failed to write rate limit error response: %v", err)
				}
			}
		}
	}
}

// RecoveryMiddleware 错误恢复中间件
func RecoveryMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("ERROR: Panic recovered: %v", err)
				if writeErr := common.NewInternalServerError("internal server error").Response().WriteJSON(w, http.StatusInternalServerError); writeErr != nil {
					log.Printf("ERROR: Failed to write panic recovery error response: %v (panic: %v)", writeErr, err)
				}
			}
		}()

		next(w, r)
	}
}

// RequestSizeLimitMiddleware 请求大小限制中间件
func RequestSizeLimitMiddleware(maxSize int64) Middleware {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			// 检查 ContentLength（如果存在且大于限制，直接拒绝）
			// 注意：对于 chunked 编码，ContentLength 为 -1，需要依赖 MaxBytesReader
			if r.ContentLength > 0 && r.ContentLength > maxSize {
				if err := common.NewBadRequestError("request body too large").Response().WriteJSON(w, http.StatusBadRequest); err != nil {
					log.Printf("ERROR: Failed to write request size limit error response: %v", err)
				}
				return
			}

			// 设置最大读取大小
			// http.MaxBytesReader 会在读取时限制大小，即使 ContentLength 未知（chunked 编码）
			// 如果超过限制，会返回 "http: request body too large" 错误
			r.Body = http.MaxBytesReader(w, r.Body, maxSize)
			next(w, r)
		}
	}
}

// TimeoutMiddleware 超时中间件
func TimeoutMiddleware(timeout time.Duration) Middleware {
	return func(next http.HandlerFunc) http.HandlerFunc {
		// 使用标准库http.TimeoutHandler避免竞态写入
		return func(w http.ResponseWriter, r *http.Request) {
			if timeout <= 0 {
				next(w, r)
				return
			}
			h := http.TimeoutHandler(http.HandlerFunc(next), timeout, "request timeout")
			h.ServeHTTP(w, r)
		}
	}
}

// MetricsMiddleware 指标收集中间件
func MetricsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// 包装ResponseWriter以捕获状态码
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// 调用下一个处理器
		next(rw, r)

		// 收集指标（移除频繁的日志输出以提高性能，仅在错误时记录）
		duration := time.Since(start)
		if rw.statusCode >= 400 || duration > 1*time.Second {
			log.Printf("METRICS: %s %s %d %v", r.Method, r.URL.Path, rw.statusCode, duration)
		}
	}
}

// SecurityHeadersMiddleware 安全头中间件
func SecurityHeadersMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 设置安全头
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")

		next(w, r)
	}
}

// GzipDecompressMiddleware gzip解压缩中间件
// 自动解压缩Content-Encoding为gzip的请求体
func GzipDecompressMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 检查Content-Encoding头
		contentEncoding := r.Header.Get("Content-Encoding")
		if strings.Contains(contentEncoding, "gzip") {
			// 创建gzip读取器
			gzipReader, err := gzip.NewReader(r.Body)
			if err != nil {
				log.Printf("ERROR: Failed to create gzip reader: %v", err)
				if writeErr := common.NewBadRequestError("invalid gzip content: "+err.Error()).Response().WriteJSON(w, http.StatusBadRequest); writeErr != nil {
					log.Printf("ERROR: Failed to write gzip error response: %v", writeErr)
				}
				return
			}
			defer gzipReader.Close()

			// 替换请求体为解压缩后的流
			r.Body = io.NopCloser(gzipReader)
			// 移除Content-Encoding头，因为已经解压缩
			r.Header.Del("Content-Encoding")
			// Content-Length不再准确，设置为-1（表示未知）
			r.ContentLength = -1
		}

		next(w, r)
	}
}

// responseWriter 包装ResponseWriter以捕获状态码
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// ChainMiddleware 链式组合多个中间件
func ChainMiddleware(middlewares ...Middleware) Middleware {
	return func(next http.HandlerFunc) http.HandlerFunc {
		// 反向应用中间件 (洋葱模型)
		for i := len(middlewares) - 1; i >= 0; i-- {
			next = middlewares[i](next)
		}
		return next
	}
}

// DefaultMiddlewareStack 返回默认中间件栈
func DefaultMiddlewareStack(config *ServerConfig) Middleware {
	middlewares := []Middleware{
		RecoveryMiddleware,
		GzipDecompressMiddleware, // gzip解压缩应该在请求大小限制之前
		LoggingMiddleware,
		SecurityHeadersMiddleware,
		RequestSizeLimitMiddleware(config.MaxRequestSize),
	}

	if config.EnableCORS {
		middlewares = append(middlewares, CORSMiddleware(config.CORSOrigins))
	}

	if config.EnableRateLimit {
		middlewares = append(middlewares, RateLimitMiddleware(config.RateLimitRPM))
	}

	if config.EnableMetrics {
		middlewares = append(middlewares, MetricsMiddleware)
	}

	return ChainMiddleware(middlewares...)
}
