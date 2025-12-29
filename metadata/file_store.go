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

package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/lscgzwd/tiggerdb/logger"
)

// FileMetadataStore 基于文件的元数据存储实现
type FileMetadataStore struct {
	config    *MetadataStoreConfig
	baseDir   string
	indexes   map[string]*IndexMetadata
	indexesMu sync.RWMutex
	tables    map[string]map[string]*TableMetadata // indexName -> tableName -> metadata
	cache     map[string]interface{}
	cacheMu   sync.RWMutex
	version   int64
	versionMu sync.RWMutex
}

// NewFileMetadataStore 创建基于文件的元数据存储
func NewFileMetadataStore(config *MetadataStoreConfig) (*FileMetadataStore, error) {
	if config == nil {
		config = DefaultMetadataStoreConfig()
	}

	if config.StorageType != "file" {
		return nil, fmt.Errorf("invalid storage type for file store: %s", config.StorageType)
	}

	if config.FilePath == "" {
		return nil, fmt.Errorf("file path cannot be empty for file store")
	}

	store := &FileMetadataStore{
		config:  config,
		baseDir: config.FilePath,
		indexes: make(map[string]*IndexMetadata),
		tables:  make(map[string]map[string]*TableMetadata),
		cache:   make(map[string]interface{}),
		version: 1,
	}

	// 初始化目录结构
	if err := store.initializeDirectories(); err != nil {
		return nil, fmt.Errorf("failed to initialize directories: %w", err)
	}

	// 加载现有元数据
	if err := store.loadExistingMetadata(); err != nil {
		return nil, fmt.Errorf("failed to load existing metadata: %w", err)
	}

	return store, nil
}

// initializeDirectories 初始化目录结构
func (fms *FileMetadataStore) initializeDirectories() error {
	// 创建基础目录
	if err := os.MkdirAll(fms.baseDir, 0755); err != nil {
		return fmt.Errorf("failed to create base directory: %w", err)
	}

	// 创建索引目录
	indexesDir := filepath.Join(fms.baseDir, "indexes")
	if err := os.MkdirAll(indexesDir, 0755); err != nil {
		return fmt.Errorf("failed to create indexes directory: %w", err)
	}

	// 创建版本目录
	versionsDir := filepath.Join(fms.baseDir, "versions")
	if err := os.MkdirAll(versionsDir, 0755); err != nil {
		return fmt.Errorf("failed to create versions directory: %w", err)
	}

	return nil
}

// loadExistingMetadata 加载现有元数据
func (fms *FileMetadataStore) loadExistingMetadata() error {
	// 加载索引元数据
	indexesDir := filepath.Join(fms.baseDir, "indexes")
	entries, err := os.ReadDir(indexesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 目录不存在，说明没有现有元数据
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			indexName := entry.Name()
			metadata, err := fms.loadIndexMetadata(indexName)
			if err != nil {
				// 记录错误但继续加载其他索引
				continue
			}
			fms.indexesMu.Lock()
			fms.indexes[indexName] = metadata
			fms.indexesMu.Unlock()

			// 加载表的元数据
			if err := fms.loadTableMetadataForIndex(indexName); err != nil {
				continue
			}
		}
	}

	return nil
}

// loadIndexMetadata 加载索引元数据
func (fms *FileMetadataStore) loadIndexMetadata(indexName string) (*IndexMetadata, error) {
	metadataPath := filepath.Join(fms.baseDir, "indexes", indexName, "metadata.json")

	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, err
	}

	var metadata IndexMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// loadTableMetadataForIndex 加载指定索引的所有表元数据
func (fms *FileMetadataStore) loadTableMetadataForIndex(indexName string) error {
	tablesDir := filepath.Join(fms.baseDir, "indexes", indexName, "tables")

	entries, err := os.ReadDir(tablesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	fms.tables[indexName] = make(map[string]*TableMetadata)

	for _, entry := range entries {
		if entry.IsDir() {
			tableName := entry.Name()
			metadata, err := fms.loadTableMetadata(indexName, tableName)
			if err != nil {
				continue
			}
			fms.tables[indexName][tableName] = metadata
		}
	}

	return nil
}

// loadTableMetadata 加载表元数据
func (fms *FileMetadataStore) loadTableMetadata(indexName, tableName string) (*TableMetadata, error) {
	metadataPath := filepath.Join(fms.baseDir, "indexes", indexName, "tables", tableName, "metadata.json")

	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, err
	}

	var metadata TableMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// SaveIndexMetadata 保存索引元数据
func (fms *FileMetadataStore) SaveIndexMetadata(indexName string, metadata *IndexMetadata) error {
	// 记录调用栈（用于调试，找出是谁调用了 SaveIndexMetadata）
	// 放在最前面，这样即使没有变化也能看到调用栈
	logLevel := os.Getenv("TIGERDB_DEBUG_METADATA")
	if logLevel == "true" {
		// 获取调用栈（跳过当前函数和 SaveIndexMetadata）
		stack := make([]byte, 4096)
		n := runtime.Stack(stack, false)
		logger.Debug("SaveIndexMetadata [%s] - Call stack (TIGERDB_DEBUG_METADATA=%s):\n%s", indexName, logLevel, stack[:n])
	} else if logLevel != "" {
		// 如果环境变量设置了但不是 "true"，也记录一下
		logger.Debug("SaveIndexMetadata [%s] - TIGERDB_DEBUG_METADATA is set to '%s' (expected 'true')", indexName, logLevel)
	}

	// 更新内存缓存（写锁）
	fms.indexesMu.Lock()
	existingMetadata := fms.indexes[indexName]
	fms.indexes[indexName] = metadata
	fms.indexesMu.Unlock()

	// 检查数据是否真的发生了变化
	if existingMetadata != nil {
		// 比较关键字段，如果没变化就不保存
		if existingMetadata.Name == metadata.Name &&
			existingMetadata.Version == metadata.Version &&
			equalMaps(existingMetadata.Mapping, metadata.Mapping) &&
			equalMaps(existingMetadata.Settings, metadata.Settings) &&
			equalStringSlices(existingMetadata.Aliases, metadata.Aliases) {
			// 数据没有变化，只更新内存缓存，不保存到文件
			logger.Debug("SaveIndexMetadata [%s] - No changes detected, skipping file save", indexName)
			return nil
		}
		// 记录变化的原因（用于调试）
		if existingMetadata.Name != metadata.Name {
			logger.Debug("SaveIndexMetadata [%s] - Name changed: %s -> %s", indexName, existingMetadata.Name, metadata.Name)
		}
		if existingMetadata.Version != metadata.Version {
			logger.Debug("SaveIndexMetadata [%s] - Version changed: %d -> %d", indexName, existingMetadata.Version, metadata.Version)
		}
		if !equalMaps(existingMetadata.Mapping, metadata.Mapping) {
			logger.Debug("SaveIndexMetadata [%s] - Mapping changed", indexName)
		}
		if !equalMaps(existingMetadata.Settings, metadata.Settings) {
			logger.Debug("SaveIndexMetadata [%s] - Settings changed", indexName)
		}
		if !equalStringSlices(existingMetadata.Aliases, metadata.Aliases) {
			logger.Debug("SaveIndexMetadata [%s] - Aliases changed: %v -> %v", indexName, existingMetadata.Aliases, metadata.Aliases)
		}
	} else {
		logger.Debug("SaveIndexMetadata [%s] - New metadata (no existing metadata)", indexName)
	}

	// 更新缓存（写锁）
	fms.cacheMu.Lock()
	defer fms.cacheMu.Unlock()

	// 保存到文件
	if err := fms.saveIndexMetadataToFile(indexName, metadata); err != nil {
		return err
	}

	// 更新版本
	fms.incrementVersion()

	// 清空相关缓存
	fms.clearCache(fmt.Sprintf("index_%s", indexName))

	return nil
}

// equalMaps 比较两个 map 是否相等（深度比较）
func equalMaps(m1, m2 map[string]interface{}) bool {
	if len(m1) != len(m2) {
		return false
	}
	for k, v1 := range m1 {
		v2, ok := m2[k]
		if !ok {
			return false
		}
		if !equalValues(v1, v2) {
			return false
		}
	}
	return true
}

// equalValues 比较两个值是否相等（支持嵌套 map 和 slice）
func equalValues(v1, v2 interface{}) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}

	// 类型断言
	switch val1 := v1.(type) {
	case map[string]interface{}:
		val2, ok := v2.(map[string]interface{})
		if !ok {
			return false
		}
		return equalMaps(val1, val2)
	case []interface{}:
		val2, ok := v2.([]interface{})
		if !ok {
			return false
		}
		return equalSlices(val1, val2)
	case []string:
		val2, ok := v2.([]string)
		if !ok {
			return false
		}
		return equalStringSlices(val1, val2)
	default:
		return v1 == v2
	}
}

// equalSlices 比较两个 []interface{} 是否相等
func equalSlices(s1, s2 []interface{}) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := range s1 {
		if !equalValues(s1[i], s2[i]) {
			return false
		}
	}
	return true
}

// equalStringSlices 比较两个 []string 是否相等
func equalStringSlices(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}

// saveIndexMetadataToFile 保存索引元数据到文件
func (fms *FileMetadataStore) saveIndexMetadataToFile(indexName string, metadata *IndexMetadata) error {
	indexDir := filepath.Join(fms.baseDir, "indexes", indexName)
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return err
	}

	metadataPath := filepath.Join(indexDir, "metadata.json")

	// 调试：记录保存前的 mapping 字段数量
	if props, ok := metadata.Mapping["properties"].(map[string]interface{}); ok {
		logger.Debug("saveIndexMetadataToFile [%s] - Before marshal, mapping has %d properties", indexName, len(props))
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}

	// 调试：验证序列化后的数据
	var verify IndexMetadata
	if err := json.Unmarshal(data, &verify); err == nil {
		if props, ok := verify.Mapping["properties"].(map[string]interface{}); ok {
			logger.Debug("saveIndexMetadataToFile [%s] - After marshal/unmarshal, mapping has %d properties", indexName, len(props))
		}
	}

	return os.WriteFile(metadataPath, data, 0644)
}

// GetIndexMetadata 获取索引元数据
func (fms *FileMetadataStore) GetIndexMetadata(indexName string) (*IndexMetadata, error) {
	// 检查缓存（读锁）
	if fms.config.EnableCache {
		fms.cacheMu.RLock()
		if cached, exists := fms.cache[fmt.Sprintf("index_%s", indexName)]; exists {
			fms.cacheMu.RUnlock()
			if metadata, ok := cached.(*IndexMetadata); ok {
				return metadata, nil
			}
		} else {
			fms.cacheMu.RUnlock()
		}
	}

	// 从内存中获取（读锁）
	fms.indexesMu.RLock()
	metadata, exists := fms.indexes[indexName]
	fms.indexesMu.RUnlock()

	if exists {
		// 更新缓存（写锁）
		if fms.config.EnableCache {
			fms.cacheMu.Lock()
			fms.cache[fmt.Sprintf("index_%s", indexName)] = metadata
			fms.cacheMu.Unlock()
		}
		return metadata, nil
	}

	// 内存中没有，尝试从文件加载
	metadata, err := fms.loadIndexMetadata(indexName)
	if err != nil {
		return nil, &MetadataNotFoundError{
			ResourceType: "index",
			ResourceName: indexName,
		}
	}

	// 加载成功后，更新内存缓存
	fms.indexesMu.Lock()
	fms.indexes[indexName] = metadata
	fms.indexesMu.Unlock()

	// 更新缓存
	if fms.config.EnableCache {
		fms.cacheMu.Lock()
		fms.cache[fmt.Sprintf("index_%s", indexName)] = metadata
		fms.cacheMu.Unlock()
	}

	return metadata, nil
}

// DeleteIndexMetadata 删除索引元数据
func (fms *FileMetadataStore) DeleteIndexMetadata(indexName string) error {
	// 从内存中删除（写锁）
	fms.indexesMu.Lock()
	delete(fms.indexes, indexName)
	fms.indexesMu.Unlock()

	// 更新缓存（写锁）
	fms.cacheMu.Lock()
	defer fms.cacheMu.Unlock()

	// 从tables中删除（注意：tables也需要锁保护，但这里先修复indexes的问题）
	delete(fms.tables, indexName)

	// 删除文件
	indexDir := filepath.Join(fms.baseDir, "indexes", indexName)
	if err := os.RemoveAll(indexDir); err != nil {
		return err
	}

	// 更新版本
	fms.incrementVersion()

	// 清空相关缓存
	fms.clearCache(fmt.Sprintf("index_%s", indexName))

	return nil
}

// ListIndexMetadata 列出所有索引元数据
func (fms *FileMetadataStore) ListIndexMetadata() ([]*IndexMetadata, error) {
	// 从内存中读取（读锁）
	fms.indexesMu.RLock()
	var result []*IndexMetadata
	for _, metadata := range fms.indexes {
		result = append(result, metadata)
	}
	fms.indexesMu.RUnlock()

	return result, nil
}

// SaveTableMetadata 保存表元数据
func (fms *FileMetadataStore) SaveTableMetadata(indexName, tableName string, metadata *TableMetadata) error {
	fms.cacheMu.Lock()
	defer fms.cacheMu.Unlock()

	// 确保索引表映射存在
	if fms.tables[indexName] == nil {
		fms.tables[indexName] = make(map[string]*TableMetadata)
	}

	// 更新内存缓存
	fms.tables[indexName][tableName] = metadata

	// 保存到文件
	if err := fms.saveTableMetadataToFile(indexName, tableName, metadata); err != nil {
		return err
	}

	// 更新版本
	fms.incrementVersion()

	// 清空相关缓存
	fms.clearCache(fmt.Sprintf("table_%s_%s", indexName, tableName))

	return nil
}

// saveTableMetadataToFile 保存表元数据到文件
func (fms *FileMetadataStore) saveTableMetadataToFile(indexName, tableName string, metadata *TableMetadata) error {
	tableDir := filepath.Join(fms.baseDir, "indexes", indexName, "tables", tableName)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		return err
	}

	metadataPath := filepath.Join(tableDir, "metadata.json")

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(metadataPath, data, 0644)
}

// GetTableMetadata 获取表元数据
func (fms *FileMetadataStore) GetTableMetadata(indexName, tableName string) (*TableMetadata, error) {
	fms.cacheMu.RLock()
	defer fms.cacheMu.RUnlock()

	// 检查缓存
	if fms.config.EnableCache {
		cacheKey := fmt.Sprintf("table_%s_%s", indexName, tableName)
		if cached, exists := fms.cache[cacheKey]; exists {
			if metadata, ok := cached.(*TableMetadata); ok {
				return metadata, nil
			}
		}
	}

	// 从内存中获取
	if indexTables, exists := fms.tables[indexName]; exists {
		if metadata, exists := indexTables[tableName]; exists {
			// 更新缓存
			if fms.config.EnableCache {
				cacheKey := fmt.Sprintf("table_%s_%s", indexName, tableName)
				fms.cache[cacheKey] = metadata
			}
			return metadata, nil
		}
	}

	return nil, &MetadataNotFoundError{
		ResourceType: "table",
		ResourceName: fmt.Sprintf("%s/%s", indexName, tableName),
	}
}

// DeleteTableMetadata 删除表元数据
func (fms *FileMetadataStore) DeleteTableMetadata(indexName, tableName string) error {
	fms.cacheMu.Lock()
	defer fms.cacheMu.Unlock()

	// 从内存中删除
	if indexTables, exists := fms.tables[indexName]; exists {
		delete(indexTables, tableName)
	}

	// 删除文件
	tableDir := filepath.Join(fms.baseDir, "indexes", indexName, "tables", tableName)
	if err := os.RemoveAll(tableDir); err != nil {
		return err
	}

	// 更新版本
	fms.incrementVersion()

	// 清空相关缓存
	fms.clearCache(fmt.Sprintf("table_%s_%s", indexName, tableName))

	return nil
}

// ListTableMetadata 列出指定索引的所有表元数据
func (fms *FileMetadataStore) ListTableMetadata(indexName string) ([]*TableMetadata, error) {
	fms.cacheMu.RLock()
	defer fms.cacheMu.RUnlock()

	var result []*TableMetadata
	if indexTables, exists := fms.tables[indexName]; exists {
		for _, metadata := range indexTables {
			result = append(result, metadata)
		}
	}

	return result, nil
}

// GetLatestVersion 获取最新版本
func (fms *FileMetadataStore) GetLatestVersion() (int64, error) {
	fms.versionMu.RLock()
	defer fms.versionMu.RUnlock()

	return fms.version, nil
}

// CreateSnapshot 创建快照
func (fms *FileMetadataStore) CreateSnapshot(version int64) error {
	fms.versionMu.Lock()
	defer fms.versionMu.Unlock()

	snapshotDir := filepath.Join(fms.baseDir, "versions", fmt.Sprintf("v%d", version))
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return err
	}

	// 复制当前元数据到快照目录
	// 这里简化实现，实际应该复制所有元数据文件
	snapshot := map[string]interface{}{
		"version":    version,
		"created_at": time.Now(),
		"indexes":    fms.indexes,
		"tables":     fms.tables,
	}

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}

	snapshotPath := filepath.Join(snapshotDir, "snapshot.json")
	return os.WriteFile(snapshotPath, data, 0644)
}

// RestoreSnapshot 恢复快照
func (fms *FileMetadataStore) RestoreSnapshot(version int64) error {
	fms.versionMu.Lock()
	defer fms.versionMu.Unlock()

	snapshotPath := filepath.Join(fms.baseDir, "versions", fmt.Sprintf("v%d", version), "snapshot.json")

	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		return err
	}

	var snapshot map[string]interface{}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	// 这里简化实现，实际应该恢复索引和表元数据
	// 并重新加载到内存中

	fms.version = version

	return nil
}

// Close 关闭存储
func (fms *FileMetadataStore) Close() error {
	// 文件存储不需要特殊的关闭操作
	// 可以在这里执行清理操作
	fms.cacheMu.Lock()
	fms.cache = make(map[string]interface{})
	fms.cacheMu.Unlock()

	return nil
}

// incrementVersion 递增版本号
func (fms *FileMetadataStore) incrementVersion() {
	fms.versionMu.Lock()
	fms.version++
	fms.versionMu.Unlock()
}

// clearCache 清空缓存
func (fms *FileMetadataStore) clearCache(key string) {
	if fms.config.EnableCache {
		delete(fms.cache, key)
	}
}
