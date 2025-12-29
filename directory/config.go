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

package directory

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// DirectoryConfig 目录配置
type DirectoryConfig struct {
	// 基础配置
	BaseDir    string `json:"base_dir"`
	MaxIndices int    `json:"max_indices,omitempty"` // 最大索引数量，0表示无限制
	MaxTables  int    `json:"max_tables,omitempty"`  // 每个索引最大表数量，0表示无限制

	// 存储配置
	EnableCompression bool   `json:"enable_compression,omitempty"` // 是否启用压缩
	StorageType       string `json:"storage_type,omitempty"`       // 存储类型：disk, memory

	// 权限配置
	DirPerm  os.FileMode `json:"dir_perm,omitempty"`  // 目录权限，默认0755
	FilePerm os.FileMode `json:"file_perm,omitempty"` // 文件权限，默认0644

	// 清理配置
	EnableAutoCleanup bool          `json:"enable_auto_cleanup,omitempty"` // 是否启用自动清理
	CleanupInterval   time.Duration `json:"cleanup_interval,omitempty"`    // 清理间隔
	MaxAge            time.Duration `json:"max_age,omitempty"`             // 最大年龄

	// 监控配置
	EnableMetrics bool `json:"enable_metrics,omitempty"` // 是否启用指标收集

	// 创建时间
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// DefaultDirectoryConfig 返回默认配置
func DefaultDirectoryConfig(baseDir string) *DirectoryConfig {
	return &DirectoryConfig{
		BaseDir:           baseDir,
		MaxIndices:        1000, // 默认最大1000个索引
		MaxTables:         100,  // 默认每个索引最大100个表
		EnableCompression: false,
		StorageType:       "disk",
		DirPerm:           0755,
		FilePerm:          0644,
		EnableAutoCleanup: false,
		CleanupInterval:   24 * time.Hour,      // 默认24小时清理一次
		MaxAge:            30 * 24 * time.Hour, // 默认30天
		EnableMetrics:     true,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}
}

// Validate 验证配置
func (c *DirectoryConfig) Validate() error {
	if c.BaseDir == "" {
		return fmt.Errorf("base_dir cannot be empty")
	}

	if c.MaxIndices < 0 {
		return fmt.Errorf("max_indices cannot be negative")
	}

	if c.MaxTables < 0 {
		return fmt.Errorf("max_tables cannot be negative")
	}

	if c.DirPerm == 0 {
		c.DirPerm = 0755
	}

	if c.FilePerm == 0 {
		c.FilePerm = 0644
	}

	if c.StorageType == "" {
		c.StorageType = "disk"
	}

	if c.StorageType != "disk" && c.StorageType != "memory" {
		return fmt.Errorf("invalid storage_type: %s, must be 'disk' or 'memory'", c.StorageType)
	}

	if c.CleanupInterval <= 0 {
		c.CleanupInterval = 24 * time.Hour
	}

	if c.MaxAge <= 0 {
		c.MaxAge = 30 * 24 * time.Hour
	}

	return nil
}

// Save 保存配置到文件
func (c *DirectoryConfig) Save(path string) error {
	if err := c.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	c.UpdatedAt = time.Now()

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// 确保目录存在
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	// 写入文件
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// Load 从文件加载配置
func LoadDirectoryConfig(path string) (*DirectoryConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config DirectoryConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &config, nil
}

// IsExpired 检查配置是否过期（用于缓存）
func (c *DirectoryConfig) IsExpired(maxAge time.Duration) bool {
	return time.Since(c.UpdatedAt) > maxAge
}

// Clone 克隆配置
func (c *DirectoryConfig) Clone() *DirectoryConfig {
	clone := *c
	return &clone
}

// Merge 合并配置，src中的非零值会覆盖dst
func (c *DirectoryConfig) Merge(src *DirectoryConfig) {
	if src.BaseDir != "" {
		c.BaseDir = src.BaseDir
	}
	if src.MaxIndices != 0 {
		c.MaxIndices = src.MaxIndices
	}
	if src.MaxTables != 0 {
		c.MaxTables = src.MaxTables
	}
	if src.StorageType != "" {
		c.StorageType = src.StorageType
	}
	if src.DirPerm != 0 {
		c.DirPerm = src.DirPerm
	}
	if src.FilePerm != 0 {
		c.FilePerm = src.FilePerm
	}
	if src.CleanupInterval != 0 {
		c.CleanupInterval = src.CleanupInterval
	}
	if src.MaxAge != 0 {
		c.MaxAge = src.MaxAge
	}

	// 布尔值总是被覆盖
	c.EnableCompression = src.EnableCompression
	c.EnableAutoCleanup = src.EnableAutoCleanup
	c.EnableMetrics = src.EnableMetrics

	c.UpdatedAt = time.Now()
}

// GetConfigPath 获取配置文件的标准路径
func (c *DirectoryConfig) GetConfigPath() string {
	return filepath.Join(c.BaseDir, "directory.json")
}

// GetGlobalConfigPath 获取全局配置文件的路径
func GetGlobalConfigPath(baseDir string) string {
	return filepath.Join(baseDir, "directory.json")
}
