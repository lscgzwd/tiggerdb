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
	"io"
	"os"
	"path/filepath"
	"sync"
)

// MigrationOptions 迁移选项
type MigrationOptions struct {
	// 是否覆盖目标目录
	Overwrite bool
	// 是否在迁移后删除源目录
	RemoveSource bool
	// 并发数
	Concurrency int
	// 进度回调
	ProgressCallback func(current, total int64)
	// 是否验证迁移结果
	ValidateResult bool
}

// DefaultMigrationOptions 返回默认迁移选项
func DefaultMigrationOptions() *MigrationOptions {
	return &MigrationOptions{
		Overwrite:        false,
		RemoveSource:     false,
		Concurrency:      4,
		ProgressCallback: nil,
		ValidateResult:   true,
	}
}

// DirectoryMigrator 目录迁移器
type DirectoryMigrator struct {
	fs   FileSystem
	opts *MigrationOptions
}

// NewDirectoryMigrator 创建目录迁移器
func NewDirectoryMigrator(fs FileSystem, opts *MigrationOptions) *DirectoryMigrator {
	if fs == nil {
		fs = NewDefaultFileSystem()
	}
	if opts == nil {
		opts = DefaultMigrationOptions()
	}
	return &DirectoryMigrator{
		fs:   fs,
		opts: opts,
	}
}

// MigrateIndex 迁移索引目录
func (dm *DirectoryMigrator) MigrateIndex(srcIndexName, dstIndexName string, pm *PathManager) error {
	srcPath := pm.GetIndexPath(srcIndexName)
	dstPath := pm.GetIndexPath(dstIndexName)

	if srcPath == "" || dstPath == "" {
		return fmt.Errorf("invalid index names: src=%s, dst=%s", srcIndexName, dstIndexName)
	}

	return dm.MigrateDir(srcPath, dstPath)
}

// MigrateTable 迁移表目录
func (dm *DirectoryMigrator) MigrateTable(srcIndexName, srcTableName, dstIndexName, dstTableName string, pm *PathManager) error {
	srcPath := pm.GetTablePath(srcIndexName, srcTableName)
	dstPath := pm.GetTablePath(dstIndexName, dstTableName)

	if srcPath == "" || dstPath == "" {
		return fmt.Errorf("invalid index/table names: src=%s/%s, dst=%s/%s",
			srcIndexName, srcTableName, dstIndexName, dstTableName)
	}

	return dm.MigrateDir(srcPath, dstPath)
}

// MigrateDir 迁移目录
func (dm *DirectoryMigrator) MigrateDir(srcPath, dstPath string) error {
	// 检查源目录是否存在
	if !dm.fs.DirExists(srcPath) {
		return fmt.Errorf("source directory does not exist: %s", srcPath)
	}

	// 检查目标目录
	if dm.fs.DirExists(dstPath) {
		if !dm.opts.Overwrite {
			return fmt.Errorf("destination directory already exists: %s", dstPath)
		}
		// 删除现有目标目录
		if err := dm.fs.RemoveDir(dstPath); err != nil {
			return fmt.Errorf("failed to remove existing destination: %w", err)
		}
	}

	// 获取源目录大小用于进度跟踪
	var totalSize int64
	if dm.opts.ProgressCallback != nil {
		size, err := dm.getDirSize(srcPath)
		if err != nil {
			return fmt.Errorf("failed to get source directory size: %w", err)
		}
		totalSize = size
	}

	// 执行迁移
	var copiedSize int64
	err := dm.migrateDirRecursive(srcPath, dstPath, &copiedSize, totalSize)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	// 验证结果
	if dm.opts.ValidateResult {
		if err := dm.validateMigration(srcPath, dstPath); err != nil {
			return fmt.Errorf("migration validation failed: %w", err)
		}
	}

	// 删除源目录（如果需要）
	if dm.opts.RemoveSource {
		if err := dm.fs.RemoveDir(srcPath); err != nil {
			return fmt.Errorf("failed to remove source directory: %w", err)
		}
	}

	return nil
}

// migrateDirRecursive 递归迁移目录
func (dm *DirectoryMigrator) migrateDirRecursive(srcPath, dstPath string, copiedSize *int64, totalSize int64) error {
	// 创建目标目录
	if err := dm.fs.CreateDir(dstPath, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	entries, err := dm.fs.ListDir(srcPath)
	if err != nil {
		return fmt.Errorf("failed to list source directory: %w", err)
	}

	// 使用工作池进行并发迁移
	if dm.opts.Concurrency > 1 {
		return dm.migrateConcurrent(srcPath, dstPath, entries, copiedSize, totalSize)
	}

	// 顺序迁移
	for _, entry := range entries {
		if err := dm.migrateEntry(srcPath, dstPath, entry, copiedSize, totalSize); err != nil {
			return err
		}
	}

	return nil
}

// migrateConcurrent 并发迁移
func (dm *DirectoryMigrator) migrateConcurrent(srcPath, dstPath string, entries []os.DirEntry, copiedSize *int64, totalSize int64) error {
	type job struct {
		entry os.DirEntry
	}

	jobs := make(chan job, len(entries))
	results := make(chan error, len(entries))

	// 启动工作协程
	var wg sync.WaitGroup
	for i := 0; i < dm.opts.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				err := dm.migrateEntry(srcPath, dstPath, j.entry, copiedSize, totalSize)
				results <- err
			}
		}()
	}

	// 发送任务
	for _, entry := range entries {
		jobs <- job{entry: entry}
	}
	close(jobs)

	// 等待所有任务完成
	go func() {
		wg.Wait()
		close(results)
	}()

	// 收集结果
	for err := range results {
		if err != nil {
			return err
		}
	}

	return nil
}

// migrateEntry 迁移单个条目
func (dm *DirectoryMigrator) migrateEntry(srcPath, dstPath string, entry os.DirEntry, copiedSize *int64, totalSize int64) error {
	srcEntryPath := filepath.Join(srcPath, entry.Name())
	dstEntryPath := filepath.Join(dstPath, entry.Name())

	if entry.IsDir() {
		// 递归迁移子目录
		return dm.migrateDirRecursive(srcEntryPath, dstEntryPath, copiedSize, totalSize)
	}

	// 迁移文件
	srcFile, err := dm.fs.OpenFile(srcEntryPath, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open source file %s: %w", srcEntryPath, err)
	}
	defer srcFile.Close()

	dstFile, err := dm.fs.CreateFile(dstEntryPath, 0644)
	if err != nil {
		return fmt.Errorf("failed to create destination file %s: %w", dstEntryPath, err)
	}
	defer dstFile.Close()

	// 复制文件内容
	copied, err := dm.copyFileWithProgress(srcFile, dstFile, copiedSize, totalSize)
	if err != nil {
		return fmt.Errorf("failed to copy file %s: %w", srcEntryPath, err)
	}

	*copiedSize += copied

	return nil
}

// copyFileWithProgress 带进度复制文件
func (dm *DirectoryMigrator) copyFileWithProgress(src, dst *os.File, copiedSize *int64, totalSize int64) (int64, error) {
	buf := make([]byte, 32*1024) // 32KB buffer
	var total int64

	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
				return total, writeErr
			}
			total += int64(n)

			// 进度回调
			if dm.opts.ProgressCallback != nil {
				dm.opts.ProgressCallback(*copiedSize+total, totalSize)
			}
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			return total, err
		}
	}

	return total, nil
}

// getDirSize 获取目录大小
func (dm *DirectoryMigrator) getDirSize(path string) (int64, error) {
	var size int64
	err := dm.fs.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// validateMigration 验证迁移结果
func (dm *DirectoryMigrator) validateMigration(srcPath, dstPath string) error {
	// 比较文件数量
	srcCount, err := dm.countFiles(srcPath)
	if err != nil {
		return fmt.Errorf("failed to count source files: %w", err)
	}

	dstCount, err := dm.countFiles(dstPath)
	if err != nil {
		return fmt.Errorf("failed to count destination files: %w", err)
	}

	if srcCount != dstCount {
		return fmt.Errorf("file count mismatch: source=%d, destination=%d", srcCount, dstCount)
	}

	// 比较总大小
	srcSize, err := dm.getDirSize(srcPath)
	if err != nil {
		return fmt.Errorf("failed to get source size: %w", err)
	}

	dstSize, err := dm.getDirSize(dstPath)
	if err != nil {
		return fmt.Errorf("failed to get destination size: %w", err)
	}

	if srcSize != dstSize {
		return fmt.Errorf("size mismatch: source=%d, destination=%d", srcSize, dstSize)
	}

	return nil
}

// countFiles 统计文件数量
func (dm *DirectoryMigrator) countFiles(path string) (int, error) {
	count := 0
	err := dm.fs.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			count++
		}
		return nil
	})
	return count, err
}

// MigrateIndexToNewLocation 迁移索引到新位置
func (dm *DirectoryMigrator) MigrateIndexToNewLocation(oldBaseDir, newBaseDir, indexName string) error {
	oldPM := NewPathManager(oldBaseDir)
	newPM := NewPathManager(newBaseDir)

	oldPath := oldPM.GetIndexPath(indexName)
	newPath := newPM.GetIndexPath(indexName)

	return dm.MigrateDir(oldPath, newPath)
}

// BatchMigrateIndices 批量迁移索引
func (dm *DirectoryMigrator) BatchMigrateIndices(indices []string, oldBaseDir, newBaseDir string) error {
	oldPM := NewPathManager(oldBaseDir)
	newPM := NewPathManager(newBaseDir)

	for _, indexName := range indices {
		if err := dm.MigrateIndex(indexName, indexName, oldPM); err != nil {
			return fmt.Errorf("failed to migrate index %s: %w", indexName, err)
		}

		// 迁移到新位置
		oldPath := oldPM.GetIndexPath(indexName)
		newPath := newPM.GetIndexPath(indexName)

		if err := dm.MigrateDir(oldPath, newPath); err != nil {
			return fmt.Errorf("failed to move index %s to new location: %w", indexName, err)
		}
	}

	return nil
}
