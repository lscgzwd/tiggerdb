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
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// PathManager 路径生成和管理逻辑
type PathManager struct {
	baseDir string // 基础目录
}

// NewPathManager 创建新的路径管理器
func NewPathManager(baseDir string) *PathManager {
	// 确保使用绝对路径
	absPath, err := filepath.Abs(baseDir)
	if err != nil {
		// 如果转换失败，使用原始路径
		absPath = baseDir
	}

	return &PathManager{
		baseDir: absPath,
	}
}

// GetBaseDir 获取基础目录
func (pm *PathManager) GetBaseDir() string {
	return pm.baseDir
}

// GetIndexPath 获取索引目录路径
// 格式: {baseDir}/indices/{indexName}/
func (pm *PathManager) GetIndexPath(indexName string) string {
	if indexName == "" {
		return ""
	}

	// 验证索引名称合法性
	if !isValidName(indexName) {
		return ""
	}

	return filepath.Join(pm.baseDir, "indices", indexName)
}

// GetTablePath 获取表目录路径
// 格式: {baseDir}/indices/{indexName}/tables/{tableName}/
func (pm *PathManager) GetTablePath(indexName, tableName string) string {
	if indexName == "" || tableName == "" {
		return ""
	}

	// 验证名称合法性
	if !isValidName(indexName) || !isValidName(tableName) {
		return ""
	}

	return filepath.Join(pm.baseDir, "indices", indexName, "tables", tableName)
}

// GetIndexMetadataPath 获取索引元数据路径
// 格式: {baseDir}/indices/{indexName}/metadata.json
func (pm *PathManager) GetIndexMetadataPath(indexName string) string {
	if indexName == "" {
		return ""
	}

	if !isValidName(indexName) {
		return ""
	}

	return filepath.Join(pm.baseDir, "indices", indexName, "metadata.json")
}

// GetTableMetadataPath 获取表元数据路径
// 格式: {baseDir}/indices/{indexName}/tables/{tableName}/metadata.json
func (pm *PathManager) GetTableMetadataPath(indexName, tableName string) string {
	if indexName == "" || tableName == "" {
		return ""
	}

	if !isValidName(indexName) || !isValidName(tableName) {
		return ""
	}

	return filepath.Join(pm.baseDir, "indices", indexName, "tables", tableName, "metadata.json")
}

// GetIndexDataPath 获取索引数据路径
// 格式: {baseDir}/indices/{indexName}/data/
func (pm *PathManager) GetIndexDataPath(indexName string) string {
	if indexName == "" {
		return ""
	}

	if !isValidName(indexName) {
		return ""
	}

	return filepath.Join(pm.baseDir, "indices", indexName, "data")
}

// GetTableDataPath 获取表数据路径
// 格式: {baseDir}/indices/{indexName}/tables/{tableName}/data/
func (pm *PathManager) GetTableDataPath(indexName, tableName string) string {
	if indexName == "" || tableName == "" {
		return ""
	}

	if !isValidName(indexName) || !isValidName(tableName) {
		return ""
	}

	return filepath.Join(pm.baseDir, "indices", indexName, "tables", tableName, "data")
}

// GetIndexLockPath 获取索引锁文件路径
// 格式: {baseDir}/indices/{indexName}/.lock
func (pm *PathManager) GetIndexLockPath(indexName string) string {
	if indexName == "" {
		return ""
	}

	if !isValidName(indexName) {
		return ""
	}

	return filepath.Join(pm.baseDir, "indices", indexName, ".lock")
}

// GetTableLockPath 获取表锁文件路径
// 格式: {baseDir}/indices/{indexName}/tables/{tableName}/.lock
func (pm *PathManager) GetTableLockPath(indexName, tableName string) string {
	if indexName == "" || tableName == "" {
		return ""
	}

	if !isValidName(indexName) || !isValidName(tableName) {
		return ""
	}

	return filepath.Join(pm.baseDir, "indices", indexName, "tables", tableName, ".lock")
}

// GetIndexConfigPath 获取索引配置文件路径
// 格式: {baseDir}/indices/{indexName}/config.json
func (pm *PathManager) GetIndexConfigPath(indexName string) string {
	if indexName == "" {
		return ""
	}

	if !isValidName(indexName) {
		return ""
	}

	return filepath.Join(pm.baseDir, "indices", indexName, "config.json")
}

// GetTableConfigPath 获取表配置文件路径
// 格式: {baseDir}/indices/{indexName}/tables/{tableName}/config.json
func (pm *PathManager) GetTableConfigPath(indexName, tableName string) string {
	if indexName == "" || tableName == "" {
		return ""
	}

	if !isValidName(indexName) || !isValidName(tableName) {
		return ""
	}

	return filepath.Join(pm.baseDir, "indices", indexName, "tables", tableName, "config.json")
}

// EnsureDir 确保目录存在，如果不存在则创建
func (pm *PathManager) EnsureDir(path string) error {
	if path == "" {
		return fmt.Errorf("path cannot be empty")
	}

	// 检查是否是符号链接，如果是则验证目标存在
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			// 是符号链接，检查目标是否存在
			if target, err := os.Readlink(path); err == nil {
				if _, err := os.Stat(target); err != nil {
					return fmt.Errorf("symlink target does not exist: %s -> %s", path, target)
				}
			}
		}
		return nil // 路径已存在
	}

	// 创建目录
	return os.MkdirAll(path, 0755)
}

// IsValidPath 验证路径是否有效
func (pm *PathManager) IsValidPath(path string) bool {
	if path == "" {
		return false
	}

	// 检查路径是否在基础目录下
	relPath, err := filepath.Rel(pm.baseDir, path)
	if err != nil {
		return false
	}

	// 防止路径遍历攻击
	if strings.HasPrefix(relPath, "..") {
		return false
	}

	return true
}

// ListIndices 列出所有索引目录
func (pm *PathManager) ListIndices() ([]string, error) {
	indicesDir := filepath.Join(pm.baseDir, "indices")

	entries, err := os.ReadDir(indicesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	var indices []string
	for _, entry := range entries {
		if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
			indices = append(indices, entry.Name())
		}
	}

	return indices, nil
}

// ListTables 列出指定索引下的所有表目录
func (pm *PathManager) ListTables(indexName string) ([]string, error) {
	if indexName == "" || !isValidName(indexName) {
		return nil, fmt.Errorf("invalid index name: %s", indexName)
	}

	tablesDir := filepath.Join(pm.baseDir, "indices", indexName, "tables")

	entries, err := os.ReadDir(tablesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	var tables []string
	for _, entry := range entries {
		if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
			tables = append(tables, entry.Name())
		}
	}

	return tables, nil
}

// isValidName 验证名称是否有效
// 规则：只能包含字母、数字、下划线和连字符，不能以点开头
func isValidName(name string) bool {
	if name == "" {
		return false
	}

	// 长度限制
	if len(name) > 255 {
		return false
	}

	// 不能以点开头
	if strings.HasPrefix(name, ".") {
		return false
	}

	// 只允许字母、数字、下划线、连字符
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-') {
			return false
		}
	}

	return true
}
