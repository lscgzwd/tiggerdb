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
	"strings"
	"time"
)

// FileSystem 文件系统操作接口
type FileSystem interface {
	// 目录操作
	CreateDir(path string, perm os.FileMode) error
	CreateDirExclusive(path string, perm os.FileMode) error
	RemoveDir(path string) error
	DirExists(path string) bool
	ListDir(path string) ([]os.DirEntry, error)

	// 文件操作
	CreateFile(path string, perm os.FileMode) (*os.File, error)
	OpenFile(path string, flag int, perm os.FileMode) (*os.File, error)
	ReadFile(path string) ([]byte, error)
	WriteFile(path string, data []byte, perm os.FileMode) error
	RemoveFile(path string) error
	FileExists(path string) bool
	FileSize(path string) (int64, error)
	FileModTime(path string) (time.Time, error)

	// 路径操作
	Abs(path string) (string, error)
	Rel(basepath, targpath string) (string, error)
	Walk(root string, walkFn filepath.WalkFunc) error

	// 权限操作
	Chmod(path string, mode os.FileMode) error
	Stat(path string) (os.FileInfo, error)
	Lstat(path string) (os.FileInfo, error)
}

// DefaultFileSystem 默认文件系统实现
type DefaultFileSystem struct{}

// NewDefaultFileSystem 创建默认文件系统
func NewDefaultFileSystem() FileSystem {
	return &DefaultFileSystem{}
}

// CreateDir 创建目录
func (fs *DefaultFileSystem) CreateDir(path string, perm os.FileMode) error {
	if perm == 0 {
		perm = 0755
	}
	return os.MkdirAll(path, perm)
}

// CreateDirExclusive 创建目录（如果已存在则失败）
func (fs *DefaultFileSystem) CreateDirExclusive(path string, perm os.FileMode) error {
	if perm == 0 {
		perm = 0755
	}
	return os.Mkdir(path, perm)
}

// RemoveDir 删除目录
func (fs *DefaultFileSystem) RemoveDir(path string) error {
	return os.RemoveAll(path)
}

// DirExists 检查目录是否存在
func (fs *DefaultFileSystem) DirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// ListDir 列出目录内容
func (fs *DefaultFileSystem) ListDir(path string) ([]os.DirEntry, error) {
	return os.ReadDir(path)
}

// CreateFile 创建文件
func (fs *DefaultFileSystem) CreateFile(path string, perm os.FileMode) (*os.File, error) {
	if perm == 0 {
		perm = 0644
	}

	// 确保父目录存在
	dir := filepath.Dir(path)
	if err := fs.CreateDir(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create parent directory: %w", err)
	}

	return os.Create(path)
}

// OpenFile 打开文件
func (fs *DefaultFileSystem) OpenFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag, perm)
}

// ReadFile 读取文件
func (fs *DefaultFileSystem) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

// WriteFile 写入文件
func (fs *DefaultFileSystem) WriteFile(path string, data []byte, perm os.FileMode) error {
	if perm == 0 {
		perm = 0644
	}

	// 确保父目录存在
	dir := filepath.Dir(path)
	if err := fs.CreateDir(dir, 0755); err != nil {
		return fmt.Errorf("failed to create parent directory: %w", err)
	}

	return os.WriteFile(path, data, perm)
}

// RemoveFile 删除文件
func (fs *DefaultFileSystem) RemoveFile(path string) error {
	return os.Remove(path)
}

// FileExists 检查文件是否存在
func (fs *DefaultFileSystem) FileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// FileSize 获取文件大小
func (fs *DefaultFileSystem) FileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// FileModTime 获取文件修改时间
func (fs *DefaultFileSystem) FileModTime(path string) (time.Time, error) {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime(), nil
}

// Abs 获取绝对路径
func (fs *DefaultFileSystem) Abs(path string) (string, error) {
	return filepath.Abs(path)
}

// Rel 获取相对路径
func (fs *DefaultFileSystem) Rel(basepath, targpath string) (string, error) {
	return filepath.Rel(basepath, targpath)
}

// Walk 遍历目录
func (fs *DefaultFileSystem) Walk(root string, walkFn filepath.WalkFunc) error {
	return filepath.Walk(root, walkFn)
}

// Chmod 修改权限
func (fs *DefaultFileSystem) Chmod(path string, mode os.FileMode) error {
	return os.Chmod(path, mode)
}

// Stat 获取文件信息
func (fs *DefaultFileSystem) Stat(path string) (os.FileInfo, error) {
	return os.Stat(path)
}

// Lstat 获取文件信息（不跟随符号链接）
func (fs *DefaultFileSystem) Lstat(path string) (os.FileInfo, error) {
	return os.Lstat(path)
}

// DirectoryOperations 目录操作工具
type DirectoryOperations struct {
	fs FileSystem
}

// NewDirectoryOperations 创建目录操作工具
func NewDirectoryOperations(fs FileSystem) *DirectoryOperations {
	if fs == nil {
		fs = NewDefaultFileSystem()
	}
	return &DirectoryOperations{fs: fs}
}

// CreateDirIfNotExists 如果目录不存在则创建
func (do *DirectoryOperations) CreateDirIfNotExists(path string, perm os.FileMode) error {
	if !do.fs.DirExists(path) {
		return do.fs.CreateDir(path, perm)
	}
	return nil
}

// IsEmptyDir 检查目录是否为空
func (do *DirectoryOperations) IsEmptyDir(path string) (bool, error) {
	if !do.fs.DirExists(path) {
		return false, fmt.Errorf("directory does not exist: %s", path)
	}

	entries, err := do.fs.ListDir(path)
	if err != nil {
		return false, err
	}

	// 过滤掉隐藏文件和特殊文件
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".") {
			return false, nil
		}
	}

	return true, nil
}

// RemoveDirIfEmpty 如果目录为空则删除
func (do *DirectoryOperations) RemoveDirIfEmpty(path string) error {
	isEmpty, err := do.IsEmptyDir(path)
	if err != nil {
		return err
	}

	if isEmpty {
		return do.fs.RemoveDir(path)
	}

	return fmt.Errorf("directory is not empty: %s", path)
}

// CopyDir 复制目录
func (do *DirectoryOperations) CopyDir(src, dst string) error {
	return do.fs.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 计算目标路径
		relPath, err := do.fs.Rel(src, path)
		if err != nil {
			return err
		}
		targetPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return do.CreateDirIfNotExists(targetPath, 0755)
		}

		// 复制文件
		return do.copyFile(path, targetPath)
	})
}

// copyFile 复制文件
func (do *DirectoryOperations) copyFile(src, dst string) error {
	srcFile, err := do.fs.OpenFile(src, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := do.fs.CreateFile(dst, 0644)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

// GetDirSize 获取目录大小
func (do *DirectoryOperations) GetDirSize(path string) (int64, error) {
	var size int64

	err := do.fs.Walk(path, func(path string, info os.FileInfo, err error) error {
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

// CleanOldFiles 清理旧文件
func (do *DirectoryOperations) CleanOldFiles(path string, maxAge time.Duration) error {
	cutoff := time.Now().Add(-maxAge)

	return do.fs.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && info.ModTime().Before(cutoff) {
			return do.fs.RemoveFile(path)
		}

		return nil
	})
}

// ValidateSymlinks 验证符号链接
func (do *DirectoryOperations) ValidateSymlinks(path string) error {
	return do.fs.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.Mode()&os.ModeSymlink != 0 {
			// 检查符号链接目标是否存在
			target, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("failed to read symlink %s: %w", path, err)
			}

			if _, err := os.Stat(target); err != nil {
				return fmt.Errorf("symlink target does not exist: %s -> %s", path, target)
			}
		}

		return nil
	})
}
