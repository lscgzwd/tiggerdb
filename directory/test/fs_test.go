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

package test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lscgzwd/tiggerdb/directory"
)

func TestDefaultFileSystem_DirectoryOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_fs_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fs := directory.NewDefaultFileSystem()

	testDir := filepath.Join(tempDir, "test_dir")

	// 测试创建目录
	err = fs.CreateDir(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// 测试目录存在性检查
	if !fs.DirExists(testDir) {
		t.Fatal("Directory should exist after creation")
	}

	// 测试不存在的目录
	if fs.DirExists(filepath.Join(tempDir, "nonexistent")) {
		t.Fatal("Nonexistent directory should not exist")
	}

	// 测试列出目录
	entries, err := fs.ListDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to list directory: %v", err)
	}

	if len(entries) == 0 {
		t.Fatal("Directory should contain at least one entry")
	}

	// 查找我们创建的目录
	found := false
	for _, entry := range entries {
		if entry.Name() == "test_dir" && entry.IsDir() {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Created directory not found in listing")
	}

	// 测试删除目录
	err = fs.RemoveDir(testDir)
	if err != nil {
		t.Fatalf("Failed to remove directory: %v", err)
	}

	if fs.DirExists(testDir) {
		t.Fatal("Directory should not exist after removal")
	}
}

func TestDefaultFileSystem_FileOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_fs_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fs := directory.NewDefaultFileSystem()

	testFile := filepath.Join(tempDir, "test_file.txt")
	testContent := []byte("Hello, TigerDB!")

	// 测试创建和写入文件
	file, err := fs.CreateFile(testFile, 0644)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	_, err = file.Write(testContent)
	if err != nil {
		t.Fatalf("Failed to write to file: %v", err)
	}
	file.Close()

	// 测试文件存在性检查
	if !fs.FileExists(testFile) {
		t.Fatal("File should exist after creation")
	}

	// 测试读取文件
	readContent, err := fs.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	if !bytes.Equal(readContent, testContent) {
		t.Fatalf("File content mismatch. Expected: %s, Got: %s", testContent, readContent)
	}

	// 测试文件大小
	size, err := fs.FileSize(testFile)
	if err != nil {
		t.Fatalf("Failed to get file size: %v", err)
	}

	if size != int64(len(testContent)) {
		t.Fatalf("File size mismatch. Expected: %d, Got: %d", len(testContent), size)
	}

	// 测试文件修改时间
	modTime, err := fs.FileModTime(testFile)
	if err != nil {
		t.Fatalf("Failed to get file mod time: %v", err)
	}

	if modTime.IsZero() {
		t.Fatal("File modification time should not be zero")
	}

	// 测试删除文件
	err = fs.RemoveFile(testFile)
	if err != nil {
		t.Fatalf("Failed to remove file: %v", err)
	}

	if fs.FileExists(testFile) {
		t.Fatal("File should not exist after removal")
	}
}

func TestDefaultFileSystem_WriteFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_fs_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fs := directory.NewDefaultFileSystem()

	testFile := filepath.Join(tempDir, "write_test.txt")
	testContent := []byte("Content written via WriteFile")

	// 测试WriteFile方法
	err = fs.WriteFile(testFile, testContent, 0644)
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// 验证文件存在
	if !fs.FileExists(testFile) {
		t.Fatal("File should exist after WriteFile")
	}

	// 验证内容
	readContent, err := fs.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read written file: %v", err)
	}

	if !bytes.Equal(readContent, testContent) {
		t.Fatalf("Written file content mismatch. Expected: %s, Got: %s", testContent, readContent)
	}
}

func TestDefaultFileSystem_PathOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_fs_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fs := directory.NewDefaultFileSystem()

	// 测试绝对路径转换
	absPath, err := fs.Abs(".")
	if err != nil {
		t.Fatalf("Failed to get absolute path: %v", err)
	}

	if !filepath.IsAbs(absPath) {
		t.Fatal("Abs should return absolute path")
	}

	// 测试相对路径计算
	relPath, err := fs.Rel(tempDir, filepath.Join(tempDir, "subdir", "file.txt"))
	if err != nil {
		t.Fatalf("Failed to get relative path: %v", err)
	}

	expectedRelPath := filepath.Join("subdir", "file.txt")
	if relPath != expectedRelPath {
		t.Fatalf("Relative path mismatch. Expected: %s, Got: %s", expectedRelPath, relPath)
	}
}

func TestDirectoryOperations_CreateDirIfNotExists(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_fs_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fs := directory.NewDefaultFileSystem()
	dirOps := directory.NewDirectoryOperations(fs)

	testDir := filepath.Join(tempDir, "test_create_dir")

	// 目录不存在时创建
	err = dirOps.CreateDirIfNotExists(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	if !fs.DirExists(testDir) {
		t.Fatal("Directory should exist after creation")
	}

	// 目录已存在时不报�?	err = dirOps.CreateDirIfNotExists(testDir, 0755)
	if err != nil {
		t.Fatalf("Should not error when directory already exists: %v", err)
	}
}

func TestDirectoryOperations_IsEmptyDir(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_fs_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fs := directory.NewDefaultFileSystem()
	dirOps := directory.NewDirectoryOperations(fs)

	// 测试空目录
	empty, err := dirOps.IsEmptyDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to check empty dir: %v", err)
	}
	if !empty {
		t.Fatal("Temp directory should be empty")
	}

	// 创建文件
	testFile := filepath.Join(tempDir, "test.txt")
	err = fs.WriteFile(testFile, []byte("test"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// 测试非空目录
	empty, err = dirOps.IsEmptyDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to check non-empty dir: %v", err)
	}
	if empty {
		t.Fatal("Directory with file should not be empty")
	}

	// 测试不存在的目录
	nonExistentDir := filepath.Join(tempDir, "nonexistent")
	_, err = dirOps.IsEmptyDir(nonExistentDir)
	if err == nil {
		t.Fatal("Checking nonexistent directory should error")
	}
}

func TestDirectoryOperations_RemoveDirIfEmpty(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_fs_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fs := directory.NewDefaultFileSystem()
	dirOps := directory.NewDirectoryOperations(fs)

	// 创建空子目录
	emptyDir := filepath.Join(tempDir, "empty_dir")
	err = fs.CreateDir(emptyDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create empty dir: %v", err)
	}

	// 删除空目录
	err = dirOps.RemoveDirIfEmpty(emptyDir)
	if err != nil {
		t.Fatalf("Failed to remove empty dir: %v", err)
	}

	if fs.DirExists(emptyDir) {
		t.Fatal("Empty directory should be removed")
	}

	// 创建非空子目录
	nonEmptyDir := filepath.Join(tempDir, "nonempty_dir")
	err = fs.CreateDir(nonEmptyDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create nonempty dir: %v", err)
	}

	// 在目录中创建文件
	testFile := filepath.Join(nonEmptyDir, "test.txt")
	err = fs.WriteFile(testFile, []byte("test"), 0644)
	if err != nil {
		t.Fatalf("Failed to create file in dir: %v", err)
	}

	// 尝试删除非空目录
	err = dirOps.RemoveDirIfEmpty(nonEmptyDir)
	if err == nil {
		t.Fatal("Removing nonempty directory should fail")
	}

	if !fs.DirExists(nonEmptyDir) {
		t.Fatal("Nonempty directory should not be removed")
	}
}

func TestDirectoryOperations_GetDirSize(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_fs_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fs := directory.NewDefaultFileSystem()
	dirOps := directory.NewDirectoryOperations(fs)

	// 测试空目录大小
	size, err := dirOps.GetDirSize(tempDir)
	if err != nil {
		t.Fatalf("Failed to get empty dir size: %v", err)
	}
	if size != 0 {
		t.Fatalf("Empty directory size should be 0, got %d", size)
	}

	// 创建测试文件
	testContent1 := []byte("Hello")
	testContent2 := []byte("World!")

	testFile1 := filepath.Join(tempDir, "file1.txt")
	testFile2 := filepath.Join(tempDir, "file2.txt")

	err = fs.WriteFile(testFile1, testContent1, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file 1: %v", err)
	}

	err = fs.WriteFile(testFile2, testContent2, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file 2: %v", err)
	}

	// 测试包含文件的目录大小
	size, err = dirOps.GetDirSize(tempDir)
	if err != nil {
		t.Fatalf("Failed to get dir size with files: %v", err)
	}

	expectedSize := int64(len(testContent1) + len(testContent2))
	if size != expectedSize {
		t.Fatalf("Directory size mismatch. Expected: %d, Got: %d", expectedSize, size)
	}
}

func TestDirectoryOperations_CleanOldFiles(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tigerdb_fs_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fs := directory.NewDefaultFileSystem()
	dirOps := directory.NewDirectoryOperations(fs)

	// 创建新文件
	newFile := filepath.Join(tempDir, "new.txt")
	err = fs.WriteFile(newFile, []byte("new"), 0644)
	if err != nil {
		t.Fatalf("Failed to create new file: %v", err)
	}

	// 创建旧文件（修改时间设为过去）
	oldFile := filepath.Join(tempDir, "old.txt")
	err = fs.WriteFile(oldFile, []byte("old"), 0644)
	if err != nil {
		t.Fatalf("Failed to create old file: %v", err)
	}

	// 修改旧文件的时间为30天前
	oldTime := time.Now().Add(-30 * 24 * time.Hour)
	err = os.Chtimes(oldFile, oldTime, oldTime)
	if err != nil {
		t.Fatalf("Failed to change file time: %v", err)
	}

	// 清理7天前的文件
	err = dirOps.CleanOldFiles(tempDir, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("Failed to clean old files: %v", err)
	}

	// 验证新文件仍然存在
	if !fs.FileExists(newFile) {
		t.Fatal("New file should still exist")
	}

	// 验证旧文件被删除
	if fs.FileExists(oldFile) {
		t.Fatal("Old file should have been deleted")
	}
}
