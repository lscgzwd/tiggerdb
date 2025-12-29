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
	"sync"
	"time"
)

// DirectoryManager 目录管理器接口
type DirectoryManager interface {
	// 索引目录管理
	CreateIndex(indexName string) error
	DeleteIndex(indexName string) error
	IndexExists(indexName string) bool
	ListIndices() ([]string, error)

	// 表目录管理
	CreateTable(indexName, tableName string) error
	DeleteTable(indexName, tableName string) error
	TableExists(indexName, tableName string) bool
	ListTables(indexName string) ([]string, error)

	// 路径获取
	GetIndexPath(indexName string) string
	GetTablePath(indexName, tableName string) string
	GetIndexDataPath(indexName string) string
	GetTableDataPath(indexName, tableName string) string

	// 元数据路径
	GetIndexMetadataPath(indexName string) string
	GetTableMetadataPath(indexName, tableName string) string

	// 锁文件管理
	GetIndexLockPath(indexName string) string
	GetTableLockPath(indexName, tableName string) string

	// 配置文件管理
	GetIndexConfigPath(indexName string) string
	GetTableConfigPath(indexName, tableName string) string

	// 迁移操作
	MigrateIndex(srcIndexName, dstIndexName string) error
	MigrateTable(srcIndexName, srcTableName, dstIndexName, dstTableName string) error

	// 清理操作
	Cleanup() error
	CleanupIndex(indexName string) error
	CleanupTable(indexName, tableName string) error

	// 统计信息
	GetStats() (*DirectoryStats, error)
}

// DirectoryStats 目录统计信息
type DirectoryStats struct {
	TotalIndices    int          `json:"total_indices"`
	TotalTables     int          `json:"total_tables"`
	TotalSize       int64        `json:"total_size"`
	Indices         []IndexStats `json:"indices"`
	LastCleanupTime time.Time    `json:"last_cleanup_time"`
}

// IndexStats 索引统计信息
type IndexStats struct {
	Name       string       `json:"name"`
	TableCount int          `json:"table_count"`
	TotalSize  int64        `json:"total_size"`
	Tables     []TableStats `json:"tables"`
	CreatedAt  time.Time    `json:"created_at"`
	ModifiedAt time.Time    `json:"modified_at"`
}

// TableStats 表统计信息
type TableStats struct {
	Name       string    `json:"name"`
	Size       int64     `json:"size"`
	FileCount  int       `json:"file_count"`
	CreatedAt  time.Time `json:"created_at"`
	ModifiedAt time.Time `json:"modified_at"`
}

// DefaultDirectoryManager 默认目录管理器实现
type DefaultDirectoryManager struct {
	config    *DirectoryConfig
	pathMgr   *PathManager
	fs        FileSystem
	dirOps    *DirectoryOperations
	migrator  *DirectoryMigrator
	mu        sync.RWMutex
	cleanupMu sync.Mutex
}

// NewDirectoryManager 创建新的目录管理器
func NewDirectoryManager(config *DirectoryConfig) (DirectoryManager, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	pathMgr := NewPathManager(config.BaseDir)
	fs := NewDefaultFileSystem()
	dirOps := NewDirectoryOperations(fs)
	migrator := NewDirectoryMigrator(fs, DefaultMigrationOptions())

	manager := &DefaultDirectoryManager{
		config:   config,
		pathMgr:  pathMgr,
		fs:       fs,
		dirOps:   dirOps,
		migrator: migrator,
	}

	// 初始化基础目录结构
	if err := manager.initializeDirectories(); err != nil {
		return nil, fmt.Errorf("failed to initialize directories: %w", err)
	}

	return manager, nil
}

// initializeDirectories 初始化基础目录结构
func (dm *DefaultDirectoryManager) initializeDirectories() error {
	// 创建基础目录
	baseDir := dm.config.BaseDir
	if err := dm.dirOps.CreateDirIfNotExists(baseDir, dm.config.DirPerm); err != nil {
		return fmt.Errorf("failed to create base directory: %w", err)
	}

	// 创建indices目录
	indicesDir := filepath.Join(baseDir, "indices")
	if err := dm.dirOps.CreateDirIfNotExists(indicesDir, dm.config.DirPerm); err != nil {
		return fmt.Errorf("failed to create indices directory: %w", err)
	}

	return nil
}

// CreateIndex 创建索引目录
func (dm *DefaultDirectoryManager) CreateIndex(indexName string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if indexName == "" {
		return fmt.Errorf("index name cannot be empty")
	}

	if !isValidName(indexName) {
		return fmt.Errorf("invalid index name: %s", indexName)
	}

	// 检查索引数量限制
	if dm.config.MaxIndices > 0 {
		indices, err := dm.pathMgr.ListIndices()
		if err != nil {
			return fmt.Errorf("failed to list indices: %w", err)
		}
		if len(indices) >= dm.config.MaxIndices {
			return fmt.Errorf("maximum number of indices (%d) reached", dm.config.MaxIndices)
		}
	}

	// 尝试创建索引目录（原子操作，如果已存在则失败）
	indexPath := dm.GetIndexPath(indexName)
	if err := dm.fs.CreateDirExclusive(indexPath, dm.config.DirPerm); err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("index already exists: %s", indexName)
		}
		return fmt.Errorf("failed to create index directory: %w", err)
	}

	// 创建子目录
	dataPath := dm.GetIndexDataPath(indexName)
	if err := dm.fs.CreateDir(dataPath, dm.config.DirPerm); err != nil {
		return fmt.Errorf("failed to create index data directory: %w", err)
	}

	// 创建 store 子目录用于 Bleve 索引
	storePath := filepath.Join(dm.GetIndexPath(indexName), "store")
	if err := dm.fs.CreateDir(storePath, dm.config.DirPerm); err != nil {
		return fmt.Errorf("failed to create index store directory: %w", err)
	}

	return nil
}

// DeleteIndex 删除索引目录
func (dm *DefaultDirectoryManager) DeleteIndex(indexName string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if indexName == "" {
		return fmt.Errorf("index name cannot be empty")
	}

	if !dm.IndexExists(indexName) {
		return fmt.Errorf("index does not exist: %s", indexName)
	}

	// 检查是否有表存在（直接调用pathMgr避免死锁，因为已持有写锁）
	tables, err := dm.pathMgr.ListTables(indexName)
	if err != nil {
		return fmt.Errorf("failed to list tables: %w", err)
	}
	if len(tables) > 0 {
		return fmt.Errorf("cannot delete index with existing tables: %s", indexName)
	}

	indexPath := dm.GetIndexPath(indexName)
	if err := dm.fs.RemoveDir(indexPath); err != nil {
		return fmt.Errorf("failed to remove index directory: %w", err)
	}

	return nil
}

// IndexExists 检查索引是否存在
func (dm *DefaultDirectoryManager) IndexExists(indexName string) bool {
	if indexName == "" {
		return false
	}
	indexPath := dm.GetIndexPath(indexName)
	return dm.fs.DirExists(indexPath)
}

// ListIndices 列出所有索引
func (dm *DefaultDirectoryManager) ListIndices() ([]string, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	return dm.pathMgr.ListIndices()
}

// CreateTable 创建表目录
func (dm *DefaultDirectoryManager) CreateTable(indexName, tableName string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if indexName == "" || tableName == "" {
		return fmt.Errorf("index name and table name cannot be empty")
	}

	if !isValidName(indexName) || !isValidName(tableName) {
		return fmt.Errorf("invalid index or table name: %s/%s", indexName, tableName)
	}

	// 检查索引是否存在
	if !dm.IndexExists(indexName) {
		return fmt.Errorf("index does not exist: %s", indexName)
	}

	// 检查表数量限制
	if dm.config.MaxTables > 0 {
		tables, err := dm.pathMgr.ListTables(indexName)
		if err != nil {
			return fmt.Errorf("failed to list tables: %w", err)
		}
		if len(tables) >= dm.config.MaxTables {
			return fmt.Errorf("maximum number of tables (%d) reached for index %s", dm.config.MaxTables, indexName)
		}
	}

	// 确保表父目录存在
	tablesDir := filepath.Dir(dm.GetTablePath(indexName, tableName))
	if err := dm.fs.CreateDir(tablesDir, dm.config.DirPerm); err != nil {
		return fmt.Errorf("failed to create tables directory: %w", err)
	}

	// 尝试创建表目录（原子操作，如果已存在则失败）
	tablePath := dm.GetTablePath(indexName, tableName)
	if err := dm.fs.CreateDirExclusive(tablePath, dm.config.DirPerm); err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("table already exists: %s/%s", indexName, tableName)
		}
		return fmt.Errorf("failed to create table directory: %w", err)
	}

	// 创建子目录
	dataPath := dm.GetTableDataPath(indexName, tableName)
	if err := dm.fs.CreateDir(dataPath, dm.config.DirPerm); err != nil {
		return fmt.Errorf("failed to create table data directory: %w", err)
	}

	return nil
}

// DeleteTable 删除表目录
func (dm *DefaultDirectoryManager) DeleteTable(indexName, tableName string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if indexName == "" || tableName == "" {
		return fmt.Errorf("index name and table name cannot be empty")
	}

	if !dm.TableExists(indexName, tableName) {
		return fmt.Errorf("table does not exist: %s/%s", indexName, tableName)
	}

	tablePath := dm.GetTablePath(indexName, tableName)
	if err := dm.fs.RemoveDir(tablePath); err != nil {
		return fmt.Errorf("failed to remove table directory: %w", err)
	}

	return nil
}

// TableExists 检查表是否存在
func (dm *DefaultDirectoryManager) TableExists(indexName, tableName string) bool {
	if indexName == "" || tableName == "" {
		return false
	}
	tablePath := dm.GetTablePath(indexName, tableName)
	return dm.fs.DirExists(tablePath)
}

// ListTables 列出指定索引的所有表
func (dm *DefaultDirectoryManager) ListTables(indexName string) ([]string, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if indexName == "" {
		return nil, fmt.Errorf("index name cannot be empty")
	}

	if !dm.IndexExists(indexName) {
		return nil, fmt.Errorf("index does not exist: %s", indexName)
	}

	return dm.pathMgr.ListTables(indexName)
}

// 路径获取方法
func (dm *DefaultDirectoryManager) GetIndexPath(indexName string) string {
	return dm.pathMgr.GetIndexPath(indexName)
}

func (dm *DefaultDirectoryManager) GetTablePath(indexName, tableName string) string {
	return dm.pathMgr.GetTablePath(indexName, tableName)
}

func (dm *DefaultDirectoryManager) GetIndexDataPath(indexName string) string {
	return dm.pathMgr.GetIndexDataPath(indexName)
}

func (dm *DefaultDirectoryManager) GetTableDataPath(indexName, tableName string) string {
	return dm.pathMgr.GetTableDataPath(indexName, tableName)
}

func (dm *DefaultDirectoryManager) GetIndexMetadataPath(indexName string) string {
	return dm.pathMgr.GetIndexMetadataPath(indexName)
}

func (dm *DefaultDirectoryManager) GetTableMetadataPath(indexName, tableName string) string {
	return dm.pathMgr.GetTableMetadataPath(indexName, tableName)
}

func (dm *DefaultDirectoryManager) GetIndexLockPath(indexName string) string {
	return dm.pathMgr.GetIndexLockPath(indexName)
}

func (dm *DefaultDirectoryManager) GetTableLockPath(indexName, tableName string) string {
	return dm.pathMgr.GetTableLockPath(indexName, tableName)
}

func (dm *DefaultDirectoryManager) GetIndexConfigPath(indexName string) string {
	return dm.pathMgr.GetIndexConfigPath(indexName)
}

func (dm *DefaultDirectoryManager) GetTableConfigPath(indexName, tableName string) string {
	return dm.pathMgr.GetTableConfigPath(indexName, tableName)
}

// MigrateIndex 迁移索引
func (dm *DefaultDirectoryManager) MigrateIndex(srcIndexName, dstIndexName string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	return dm.migrator.MigrateIndex(srcIndexName, dstIndexName, dm.pathMgr)
}

// MigrateTable 迁移表
func (dm *DefaultDirectoryManager) MigrateTable(srcIndexName, srcTableName, dstIndexName, dstTableName string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	return dm.migrator.MigrateTable(srcIndexName, srcTableName, dstIndexName, dstTableName, dm.pathMgr)
}

// Cleanup 清理所有目录
func (dm *DefaultDirectoryManager) Cleanup() error {
	dm.cleanupMu.Lock()
	defer dm.cleanupMu.Unlock()

	dm.mu.Lock()
	defer dm.mu.Unlock()

	// 清理临时文件和过期文件
	if dm.config.EnableAutoCleanup {
		return dm.cleanupDirectory(dm.config.BaseDir)
	}

	return nil
}

// CleanupIndex 清理指定索引
func (dm *DefaultDirectoryManager) CleanupIndex(indexName string) error {
	dm.cleanupMu.Lock()
	defer dm.cleanupMu.Unlock()

	dm.mu.Lock()
	defer dm.mu.Unlock()

	if !dm.IndexExists(indexName) {
		return fmt.Errorf("index does not exist: %s", indexName)
	}

	indexPath := dm.GetIndexPath(indexName)
	return dm.cleanupDirectory(indexPath)
}

// CleanupTable 清理指定表
func (dm *DefaultDirectoryManager) CleanupTable(indexName, tableName string) error {
	dm.cleanupMu.Lock()
	defer dm.cleanupMu.Unlock()

	dm.mu.Lock()
	defer dm.mu.Unlock()

	if !dm.TableExists(indexName, tableName) {
		return fmt.Errorf("table does not exist: %s/%s", indexName, tableName)
	}

	tablePath := dm.GetTablePath(indexName, tableName)
	return dm.cleanupDirectory(tablePath)
}

// cleanupDirectory 清理目录
func (dm *DefaultDirectoryManager) cleanupDirectory(path string) error {
	if dm.config.EnableAutoCleanup {
		return dm.dirOps.CleanOldFiles(path, dm.config.MaxAge)
	}
	return nil
}

// GetStats 获取统计信息
func (dm *DefaultDirectoryManager) GetStats() (*DirectoryStats, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	stats := &DirectoryStats{
		LastCleanupTime: time.Now(), // 这里应该从持久化存储中读取
	}

	// 获取所有索引
	indices, err := dm.ListIndices()
	if err != nil {
		return nil, fmt.Errorf("failed to list indices: %w", err)
	}

	stats.TotalIndices = len(indices)

	for _, indexName := range indices {
		indexStats, err := dm.getIndexStats(indexName)
		if err != nil {
			return nil, fmt.Errorf("failed to get index stats for %s: %w", indexName, err)
		}
		stats.TotalSize += indexStats.TotalSize
		stats.TotalTables += indexStats.TableCount
		stats.Indices = append(stats.Indices, *indexStats)
	}

	return stats, nil
}

// getIndexStats 获取索引统计信息
func (dm *DefaultDirectoryManager) getIndexStats(indexName string) (*IndexStats, error) {
	indexPath := dm.GetIndexPath(indexName)

	// 获取索引创建时间
	info, err := dm.fs.Stat(indexPath)
	if err != nil {
		return nil, err
	}

	stats := &IndexStats{
		Name:       indexName,
		CreatedAt:  info.ModTime(),
		ModifiedAt: info.ModTime(),
	}

	// 获取表信息
	tables, err := dm.ListTables(indexName)
	if err != nil {
		return nil, err
	}

	stats.TableCount = len(tables)

	for _, tableName := range tables {
		tableStats, err := dm.getTableStats(indexName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get table stats for %s/%s: %w", indexName, tableName, err)
		}
		stats.TotalSize += tableStats.Size
		stats.Tables = append(stats.Tables, *tableStats)
	}

	return stats, nil
}

// getTableStats 获取表统计信息
func (dm *DefaultDirectoryManager) getTableStats(indexName, tableName string) (*TableStats, error) {
	tablePath := dm.GetTablePath(indexName, tableName)

	// 获取表大小和文件数量
	var size int64
	var fileCount int

	err := dm.fs.Walk(tablePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
			fileCount++
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	info, err := dm.fs.Stat(tablePath)
	if err != nil {
		return nil, err
	}

	return &TableStats{
		Name:      tableName,
		Size:      size,
		FileCount: fileCount,
		CreatedAt: info.ModTime(),
	}, nil
}
