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

package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/lscgzwd/tiggerdb/protocols/es"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/server"
)

// GlobalConfig 全局配置结构
// 包含所有协议共享的配置和各个协议的特定配置
type GlobalConfig struct {
	// 核心配置（所有协议共享）
	DataDir string `yaml:"data_dir" json:"data_dir"` // 数据目录，所有协议共享

	// 协议配置
	ES         *es.Config        `yaml:"es,omitempty" json:"es,omitempty"`                 // Elasticsearch 协议配置
	Redis      *RedisConfig      `yaml:"redis,omitempty" json:"redis,omitempty"`           // Redis 协议配置（预留）
	MySQL      *MySQLConfig      `yaml:"mysql,omitempty" json:"mysql,omitempty"`           // MySQL 协议配置（预留）
	PostgreSQL *PostgreSQLConfig `yaml:"postgresql,omitempty" json:"postgresql,omitempty"` // PostgreSQL 协议配置（预留）

	// 日志配置（全局）
	Log *LogConfig `yaml:"log,omitempty" json:"log,omitempty"`

	// 监控配置（全局）
	Metrics *MetricsConfig `yaml:"metrics,omitempty" json:"metrics,omitempty"`
}

// LogConfig 日志配置
type LogConfig struct {
	Level           string `yaml:"level" json:"level"`                       // 日志级别：debug, info, warn, error, silent
	Output          string `yaml:"output" json:"output"`                     // 输出目标：stdout, stderr, 或文件路径
	Format          string `yaml:"format" json:"format"`                     // 日志格式：text, json
	EnableCaller    bool   `yaml:"enable_caller" json:"enable_caller"`       // 是否显示调用位置（文件:行号）
	EnableTimestamp bool   `yaml:"enable_timestamp" json:"enable_timestamp"` // 是否显示时间戳
	MaxSize         int    `yaml:"max_size" json:"max_size"`                 // 单个日志文件的最大大小（MB）
	MaxBackups      int    `yaml:"max_backups" json:"max_backups"`           // 保留的旧日志文件数量
	MaxAge          int    `yaml:"max_age" json:"max_age"`                   // 保留旧日志文件的最大天数
	Compress        bool   `yaml:"compress" json:"compress"`                 // 是否压缩旧日志文件
}

// MetricsConfig 监控配置
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"` // 是否启用监控
	Path    string `yaml:"path" json:"path"`       // 监控端点路径
}

// RedisConfig Redis 协议配置（预留）
type RedisConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"` // 是否启用 Redis 协议
	Host    string `yaml:"host" json:"host"`       // 监听地址
	Port    int    `yaml:"port" json:"port"`       // 监听端口
	// 更多 Redis 配置...
}

// MySQLConfig MySQL 协议配置（预留）
type MySQLConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"` // 是否启用 MySQL 协议
	Host    string `yaml:"host" json:"host"`       // 监听地址
	Port    int    `yaml:"port" json:"port"`       // 监听端口
	// 更多 MySQL 配置...
}

// PostgreSQLConfig PostgreSQL 协议配置（预留）
type PostgreSQLConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"` // 是否启用 PostgreSQL 协议
	Host    string `yaml:"host" json:"host"`       // 监听地址
	Port    int    `yaml:"port" json:"port"`       // 监听端口
	// 更多 PostgreSQL 配置...
}

// DefaultGlobalConfig 返回默认全局配置
func DefaultGlobalConfig() *GlobalConfig {
	return &GlobalConfig{
		DataDir: "./data", // 默认数据目录
		ES: &es.Config{
			Enabled:      true,
			ServerConfig: server.DefaultServerConfig(),
		},
		Redis: &RedisConfig{
			Enabled: false,
			Host:    "0.0.0.0",
			Port:    6379,
		},
		MySQL: &MySQLConfig{
			Enabled: false,
			Host:    "0.0.0.0",
			Port:    3306,
		},
		PostgreSQL: &PostgreSQLConfig{
			Enabled: false,
			Host:    "0.0.0.0",
			Port:    5432,
		},
		Log: &LogConfig{
			Level:           "info",
			Output:          "stdout",
			Format:          "text",
			EnableCaller:    false,
			EnableTimestamp: true,
			MaxSize:         100,
			MaxBackups:      3,
			MaxAge:          7,
			Compress:        true,
		},
		Metrics: &MetricsConfig{
			Enabled: true,
			Path:    "/_metrics",
		},
	}
}

// Validate 验证配置
func (c *GlobalConfig) Validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("data_dir cannot be empty")
	}

	// 验证数据目录路径（可以是相对路径或绝对路径）
	if !filepath.IsAbs(c.DataDir) {
		// 相对路径，转换为绝对路径
		absPath, err := filepath.Abs(c.DataDir)
		if err != nil {
			return fmt.Errorf("failed to resolve data_dir path: %w", err)
		}
		c.DataDir = absPath
	}

	// 验证 ES 配置
	if c.ES != nil {
		if err := c.ES.Validate(); err != nil {
			return fmt.Errorf("invalid ES config: %w", err)
		}
	}

	// 验证 Redis 配置（如果启用）
	if c.Redis != nil && c.Redis.Enabled {
		if c.Redis.Port < 1 || c.Redis.Port > 65535 {
			return fmt.Errorf("invalid Redis port: %d", c.Redis.Port)
		}
	}

	// 验证 MySQL 配置（如果启用）
	if c.MySQL != nil && c.MySQL.Enabled {
		if c.MySQL.Port < 1 || c.MySQL.Port > 65535 {
			return fmt.Errorf("invalid MySQL port: %d", c.MySQL.Port)
		}
	}

	// 验证 PostgreSQL 配置（如果启用）
	if c.PostgreSQL != nil && c.PostgreSQL.Enabled {
		if c.PostgreSQL.Port < 1 || c.PostgreSQL.Port > 65535 {
			return fmt.Errorf("invalid PostgreSQL port: %d", c.PostgreSQL.Port)
		}
	}

	return nil
}

// GetDataDir 获取数据目录
func (c *GlobalConfig) GetDataDir() string {
	if c.DataDir != "" {
		return c.DataDir
	}
	return "./data"
}

// ApplyEnvOverrides 应用环境变量覆盖
func (c *GlobalConfig) ApplyEnvOverrides() {
	// 数据目录
	if dataDir := os.Getenv("TIGERDB_DATA_DIR"); dataDir != "" {
		c.DataDir = dataDir
	}

	// ES 协议配置
	if c.ES != nil && c.ES.ServerConfig != nil {
		if host := os.Getenv("TIGERDB_ES_HOST"); host != "" {
			c.ES.ServerConfig.Host = host
		}
		if portStr := os.Getenv("TIGERDB_ES_PORT"); portStr != "" {
			if port, err := parseInt(portStr); err == nil {
				c.ES.ServerConfig.Port = port
			}
		}
		if enabled := os.Getenv("TIGERDB_ES_ENABLED"); enabled != "" {
			c.ES.Enabled = enabled == "true" || enabled == "1"
		}
	}

	// Redis 协议配置
	if c.Redis != nil {
		if enabled := os.Getenv("TIGERDB_REDIS_ENABLED"); enabled != "" {
			c.Redis.Enabled = enabled == "true" || enabled == "1"
		}
		if host := os.Getenv("TIGERDB_REDIS_HOST"); host != "" {
			c.Redis.Host = host
		}
		if portStr := os.Getenv("TIGERDB_REDIS_PORT"); portStr != "" {
			if port, err := parseInt(portStr); err == nil {
				c.Redis.Port = port
			}
		}
	}

	// MySQL 协议配置
	if c.MySQL != nil {
		if enabled := os.Getenv("TIGERDB_MYSQL_ENABLED"); enabled != "" {
			c.MySQL.Enabled = enabled == "true" || enabled == "1"
		}
		if host := os.Getenv("TIGERDB_MYSQL_HOST"); host != "" {
			c.MySQL.Host = host
		}
		if portStr := os.Getenv("TIGERDB_MYSQL_PORT"); portStr != "" {
			if port, err := parseInt(portStr); err == nil {
				c.MySQL.Port = port
			}
		}
	}

	// PostgreSQL 协议配置
	if c.PostgreSQL != nil {
		if enabled := os.Getenv("TIGERDB_POSTGRESQL_ENABLED"); enabled != "" {
			c.PostgreSQL.Enabled = enabled == "true" || enabled == "1"
		}
		if host := os.Getenv("TIGERDB_POSTGRESQL_HOST"); host != "" {
			c.PostgreSQL.Host = host
		}
		if portStr := os.Getenv("TIGERDB_POSTGRESQL_PORT"); portStr != "" {
			if port, err := parseInt(portStr); err == nil {
				c.PostgreSQL.Port = port
			}
		}
	}
}

// parseInt 解析整数（辅助函数）
func parseInt(s string) (int, error) {
	var result int
	_, err := fmt.Sscanf(s, "%d", &result)
	return result, err
}
