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

package middleware

import (
	"encoding/base64"
	"net/http"
	"strings"

	"github.com/lscgzwd/tiggerdb/logger"
)

// AuthConfig 认证配置
type AuthConfig struct {
	Enabled  bool            // 是否启用认证
	Type     string          // 认证类型: "basic", "bearer", "apikey"
	Username string          // Basic Auth 用户名
	Password string          // Basic Auth 密码
	ApiKeys  map[string]bool // API Key 列表
	Realm    string          // 认证域
}

// DefaultAuthConfig 返回默认认证配置
func DefaultAuthConfig() *AuthConfig {
	return &AuthConfig{
		Enabled:  false,
		Type:     "basic",
		Username: "",
		Password: "",
		ApiKeys:  make(map[string]bool),
		Realm:    "TigerDB",
	}
}

// AuthMiddleware 创建认证中间件
func AuthMiddleware(config *AuthConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 检查是否启用认证
			if !config.Enabled {
				// 认证未启用，直接放行
				next.ServeHTTP(w, r)
				return
			}

			// 检查是否需要跳过认证的路径
			if shouldSkipAuth(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			// 执行认证检查
			if !checkAuth(r, config) {
				// 认证失败
				logger.Warn("Authentication failed for %s from %s", r.URL.Path, r.RemoteAddr)
				w.Header().Set("WWW-Authenticate", `Basic realm="`+config.Realm+`"`)
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte("Unauthorized"))
				return
			}

			// 认证成功，继续处理请求
			next.ServeHTTP(w, r)
		})
	}
}

// checkAuth 检查请求的认证信息
func checkAuth(r *http.Request, config *AuthConfig) bool {
	// 检查认证类型
	switch config.Type {
	case "basic":
		// Basic Auth 需要 Authorization 头
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			return false
		}
		return checkBasicAuth(authHeader, config.Username, config.Password)
	case "bearer":
		// Bearer Token 需要 Authorization 头
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			return false
		}
		return checkBearerAuth(authHeader, config.ApiKeys)
	case "apikey":
		// API Key 使用 X-API-Key 头
		return checkAPIKeyAuth(r, config.ApiKeys)
	default:
		return false
	}
}

// checkBasicAuth 检查 Basic Auth
func checkBasicAuth(authHeader, username, password string) bool {
	// 解析 Authorization 头: "Basic base64(username:password)"
	if !strings.HasPrefix(authHeader, "Basic ") {
		return false
	}

	encoded := strings.TrimPrefix(authHeader, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		logger.Debug("Failed to decode Basic Auth: %v", err)
		return false
	}

	// 解析 username:password
	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return false
	}

	if parts[0] != username || parts[1] != password {
		logger.Debug("Basic Auth credentials mismatch")
		return false
	}

	return true
}

// checkBearerAuth 检查 Bearer Token
func checkBearerAuth(authHeader string, apiKeys map[string]bool) bool {
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return false
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")
	return apiKeys[token]
}

// checkAPIKeyAuth 检查 API Key
func checkAPIKeyAuth(r *http.Request, apiKeys map[string]bool) bool {
	apiKey := r.Header.Get("X-API-Key")
	if apiKey == "" {
		return false
	}
	return apiKeys[apiKey]
}

// shouldSkipAuth 检查是否需要跳过认证
func shouldSkipAuth(path string) bool {
	// 不需要认证的路径列表
	skipPaths := []string{
		"/",                // 根路径
		"/_ping",           // 健康检查
		"/_cluster/health", // 集群健康
		"/_cat",            // Cat API（某些环境可能公开）
	}

	// 检查路径是否在跳过列表中
	for _, skipPath := range skipPaths {
		if path == skipPath || strings.HasPrefix(path, skipPath) {
			return true
		}
	}

	return false
}
