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

package es

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/directory"
	"github.com/lscgzwd/tiggerdb/metadata"
)

// IndexManager 管理索引的bleve Index实例
type IndexManager struct {
	dirMgr      directory.DirectoryManager
	metaStore   metadata.MetadataStore
	indices     sync.Map   // 索引名称 -> bleve.Index（无锁并发安全）
	indexStatus sync.Map   // 索引名称 -> bool（是否存在）
	openMu      sync.Mutex // 仅用于打开索引时的互斥
}

// NewIndexManager 创建新的索引管理器
func NewIndexManager(dirMgr directory.DirectoryManager, metaStore metadata.MetadataStore) *IndexManager {
	return &IndexManager{
		dirMgr:    dirMgr,
		metaStore: metaStore,
	}
}

// GetIndex 获取或打开索引
func (im *IndexManager) GetIndex(indexName string) (bleve.Index, error) {
	// 快速路径：从 sync.Map 获取已缓存的索引（无锁）
	if val, exists := im.indices.Load(indexName); exists {
		return val.(bleve.Index), nil
	}

	// 检查索引状态缓存
	if val, exists := im.indexStatus.Load(indexName); exists {
		if !val.(bool) {
			return nil, fmt.Errorf("index [%s] not found", indexName)
		}
	}

	// 慢路径：需要打开索引，使用互斥锁避免重复打开
	im.openMu.Lock()
	defer im.openMu.Unlock()

	// 双重检查：可能在等待锁时已被其他 goroutine 打开
	if val, exists := im.indices.Load(indexName); exists {
		return val.(bleve.Index), nil
	}

	// 检查索引是否存在于目录中
	indexExists := im.dirMgr.IndexExists(indexName)
	im.indexStatus.Store(indexName, indexExists)

	if !indexExists {
		return nil, fmt.Errorf("index [%s] not found", indexName)
	}

	// 获取索引路径
	indexPath := im.dirMgr.GetIndexPath(indexName)
	if indexPath == "" {
		return nil, fmt.Errorf("invalid index path for index [%s]", indexName)
	}

	// 构建完整的索引存储路径
	storePath := filepath.Join(indexPath, "store")

	// 检查存储路径是否存在
	info, err := os.Stat(storePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("index [%s] exists in directory but store is missing (path: %s)", indexName, storePath)
		}
		return nil, fmt.Errorf("failed to stat index store path [%s]: %w", storePath, err)
	}

	// 确保是目录
	if !info.IsDir() {
		return nil, fmt.Errorf("index store path [%s] is not a directory", storePath)
	}

	// 打开已存在的索引
	idx, err := bleve.Open(storePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open index [%s] at path %s: %w", indexName, storePath, err)
	}

	// 缓存索引实例
	im.indices.Store(indexName, idx)

	return idx, nil
}

// CloseIndex 关闭索引
func (im *IndexManager) CloseIndex(indexName string) error {
	val, exists := im.indices.Load(indexName)
	if !exists {
		return nil // 已经关闭或不存在
	}

	idx := val.(bleve.Index)
	if err := idx.Close(); err != nil {
		log.Printf("WARN: Failed to close index [%s]: %v", indexName, err)
	}

	im.indices.Delete(indexName)
	im.indexStatus.Delete(indexName)
	return nil
}

// CloseAll 关闭所有索引
func (im *IndexManager) CloseAll() error {
	var lastErr error
	im.indices.Range(func(key, value interface{}) bool {
		name := key.(string)
		idx := value.(bleve.Index)
		if err := idx.Close(); err != nil {
			log.Printf("WARN: Failed to close index [%s]: %v", name, err)
			lastErr = err
		}
		im.indices.Delete(key)
		return true
	})
	return lastErr
}

// RemoveIndex 移除索引（从缓存中移除，不关闭）
func (im *IndexManager) RemoveIndex(indexName string) {
	im.indices.Delete(indexName)
	im.indexStatus.Delete(indexName)
}

// InvalidateIndexStatus 使索引状态缓存失效（当索引被创建或删除时调用）
func (im *IndexManager) InvalidateIndexStatus(indexName string) {
	im.indexStatus.Delete(indexName)
}

// PreloadAllIndices 预加载所有索引（启动时调用，触发后台合并任务）
func (im *IndexManager) PreloadAllIndices() {
	indices, err := im.dirMgr.ListIndices()
	if err != nil {
		log.Printf("[IndexManager] Failed to list indices: %v", err)
		return
	}
	log.Printf("[IndexManager] Preloading %d indices...", len(indices))

	// 并发预加载，每个索引一个 goroutine
	var wg sync.WaitGroup
	// 限制并发数，避免同时打开太多索引
	semaphore := make(chan struct{}, 5)

	for _, indexName := range indices {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			semaphore <- struct{}{}        // 获取信号量
			defer func() { <-semaphore }() // 释放信号量

			_, err := im.GetIndex(name)
			if err != nil {
				log.Printf("[IndexManager] Failed to preload index [%s]: %v", name, err)
			} else {
				log.Printf("[IndexManager] Preloaded index [%s]", name)
			}
		}(indexName)
	}

	wg.Wait()
	log.Printf("[IndexManager] Preload complete")
}
