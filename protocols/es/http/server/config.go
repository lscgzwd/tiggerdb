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
	"fmt"
	"time"
)

// ServerConfig HTTP服务器配置
type ServerConfig struct {
	// 基础配置
	Host string `json:"host" yaml:"host"` // 监听主机，默认"0.0.0.0"
	Port int    `json:"port" yaml:"port"` // 监听端口，默认9200

	// 超时配置
	ReadTimeout       time.Duration `json:"read_timeout" yaml:"read_timeout"`               // 读取超时，默认30s
	WriteTimeout      time.Duration `json:"write_timeout" yaml:"write_timeout"`             // 写入超时，默认30s
	IdleTimeout       time.Duration `json:"idle_timeout" yaml:"idle_timeout"`               // 空闲超时，默认60s
	ReadHeaderTimeout time.Duration `json:"read_header_timeout" yaml:"read_header_timeout"` // 读取头超时，默认10s

	// 连接限制
	MaxHeaderBytes int `json:"max_header_bytes" yaml:"max_header_bytes"` // 最大头字节数，默认1MB
	MaxConnections int `json:"max_connections" yaml:"max_connections"`   // 最大连接数，默认1000

	// TLS配置
	TLSEnable   bool   `json:"tls_enable" yaml:"tls_enable"`       // 是否启用TLS，默认false
	TLSCertFile string `json:"tls_cert_file" yaml:"tls_cert_file"` // TLS证书文件
	TLSKeyFile  string `json:"tls_key_file" yaml:"tls_key_file"`   // TLS密钥文件

	// 中间件配置
	EnableCORS      bool     `json:"enable_cors" yaml:"enable_cors"`             // 是否启用CORS，默认true
	CORSOrigins     []string `json:"cors_origins" yaml:"cors_origins"`           // CORS允许的源，默认["*"]
	EnableRateLimit bool     `json:"enable_rate_limit" yaml:"enable_rate_limit"` // 是否启用限流，默认false
	RateLimitRPM    int      `json:"rate_limit_rpm" yaml:"rate_limit_rpm"`       // 每分钟请求限制，默认1000

	// 日志配置
	LogLevel    string `json:"log_level" yaml:"log_level"`         // 日志级别，默认"info"
	LogFormat   string `json:"log_format" yaml:"log_format"`       // 日志格式，默认"json"
	LogFile     string `json:"log_file" yaml:"log_file"`           // 日志文件，默认stdout
	LogMaxSize  int    `json:"log_max_size" yaml:"log_max_size"`   // 日志文件最大大小(MB)，默认100
	LogMaxFiles int    `json:"log_max_files" yaml:"log_max_files"` // 日志文件最大数量，默认10

	// 监控配置
	EnableMetrics bool   `json:"enable_metrics" yaml:"enable_metrics"` // 是否启用指标收集，默认true
	MetricsPath   string `json:"metrics_path" yaml:"metrics_path"`     // 指标端点路径，默认"/_metrics"
	HealthPath    string `json:"health_path" yaml:"health_path"`       // 健康检查路径，默认"/_health"

	// API配置
	MaxRequestSize int64  `json:"max_request_size" yaml:"max_request_size"` // 最大请求大小，默认100MB（Elasticsearch标准）
	EnableSwagger  bool   `json:"enable_swagger" yaml:"enable_swagger"`     // 是否启用Swagger，默认false
	SwaggerPath    string `json:"swagger_path" yaml:"swagger_path"`         // Swagger路径，默认"/swagger"

	// 其他配置
	ShutdownTimeout time.Duration `json:"shutdown_timeout" yaml:"shutdown_timeout"` // 关闭超时，默认30s
}

// DefaultServerConfig 返回默认服务器配置
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		// 基础配置
		Host: "0.0.0.0",
		Port: 9200,

		// 超时配置
		ReadTimeout:       30 * time.Second,  // 读取超时 30 秒
		WriteTimeout:      120 * time.Second, // 写入超时 120 秒，允许慢查询
		IdleTimeout:       300 * time.Second, // 空闲超时 5 分钟，避免频繁断开 Keep-Alive 连接
		ReadHeaderTimeout: 10 * time.Second,

		// 连接限制
		MaxHeaderBytes: 1 << 20, // 1MB
		MaxConnections: 1000,

		// TLS配置
		TLSEnable: false,

		// 中间件配置
		EnableCORS:      true,
		CORSOrigins:     []string{"*"},
		EnableRateLimit: false,
		RateLimitRPM:    1000,

		// 日志配置
		LogLevel:    "info",
		LogFormat:   "json",
		LogFile:     "",
		LogMaxSize:  100,
		LogMaxFiles: 10,

		// 监控配置
		EnableMetrics: true,
		MetricsPath:   "/_metrics",
		HealthPath:    "/_health",

		// API配置
		MaxRequestSize: 500 << 20, // 500MB (增加bulk请求大小限制，支持更大的批量操作)
		EnableSwagger:  false,
		SwaggerPath:    "/swagger",

		// 其他配置
		ShutdownTimeout: 30 * time.Second,
	}
}

// Validate 验证配置
func (c *ServerConfig) Validate() error {
	// 允许端口 0（系统自动分配）用于测试场景
	if c.Port != 0 && (c.Port < 1 || c.Port > 65535) {
		return fmt.Errorf("invalid port: %d (must be between 1 and 65535, or 0 for automatic assignment)", c.Port)
	}

	if c.MaxConnections < 1 {
		return fmt.Errorf("max_connections must be greater than 0")
	}

	if c.RateLimitRPM < 0 {
		return fmt.Errorf("rate_limit_rpm cannot be negative")
	}

	if c.MaxRequestSize <= 0 {
		return fmt.Errorf("max_request_size must be greater than 0")
	}

	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown_timeout must be greater than 0")
	}

	return nil
}

// Address 返回服务器监听地址
func (c *ServerConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// BaseURL 返回服务器基础URL
func (c *ServerConfig) BaseURL() string {
	scheme := "http"
	if c.TLSEnable {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s:%d", scheme, c.Host, c.Port)
}

// Clone 克隆配置
func (c *ServerConfig) Clone() *ServerConfig {
	clone := *c

	// 深拷贝切片
	if c.CORSOrigins != nil {
		clone.CORSOrigins = make([]string, len(c.CORSOrigins))
		copy(clone.CORSOrigins, c.CORSOrigins)
	}

	return &clone
}
