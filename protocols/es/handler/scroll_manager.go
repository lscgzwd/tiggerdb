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

package handler

import (
	"fmt"
	"sync"
	"time"

	"github.com/lscgzwd/tiggerdb/logger"

	"github.com/google/uuid"
)

// ScrollContext 存储 scroll 搜索的上下文信息
type ScrollContext struct {
	ScrollID     string                            `json:"scroll_id"`
	IndexName    string                            `json:"index_name"`
	Query        map[string]interface{}            `json:"query"`
	Sort         []interface{}                     `json:"sort"`
	Source       interface{}                       `json:"_source,omitempty"`
	Size         int                               `json:"size"`
	From         int                               `json:"from"`
	Aggregations map[string]map[string]interface{} `json:"aggs,omitempty"`
	LastSort     []interface{}                     `json:"last_sort,omitempty"` // 最后一个结果的 sort 值，用于 search_after
	ExpiresAt    time.Time                         `json:"expires_at"`
	CreatedAt    time.Time                         `json:"created_at"`
}

// ScrollManager 管理 scroll 上下文
type ScrollManager struct {
	contexts map[string]*ScrollContext
	mutex    sync.RWMutex
	// 定期清理过期 scroll context 的 goroutine
	cleanupTicker *time.Ticker
	stopCleanup   chan bool
}

var (
	globalScrollManager *ScrollManager
	scrollManagerOnce   sync.Once
)

// GetScrollManager 获取全局 scroll manager 实例（单例）
func GetScrollManager() *ScrollManager {
	scrollManagerOnce.Do(func() {
		globalScrollManager = &ScrollManager{
			contexts:      make(map[string]*ScrollContext),
			cleanupTicker: time.NewTicker(1 * time.Minute), // 每分钟清理一次
			stopCleanup:   make(chan bool),
		}
		// 启动清理 goroutine
		go globalScrollManager.cleanupExpired()
	})
	return globalScrollManager
}

// CreateScrollContext 创建新的 scroll context
func (sm *ScrollManager) CreateScrollContext(
	indexName string,
	query map[string]interface{},
	sort []interface{},
	source interface{},
	size int,
	aggs map[string]map[string]interface{},
	scrollTTL time.Duration,
) (*ScrollContext, error) {
	scrollID := uuid.New().String()
	now := time.Now()

	ctx := &ScrollContext{
		ScrollID:     scrollID,
		IndexName:    indexName,
		Query:        query,
		Sort:         sort,
		Source:       source,
		Size:         size,
		From:         0,
		Aggregations: aggs,
		ExpiresAt:    now.Add(scrollTTL),
		CreatedAt:    now,
	}

	sm.mutex.Lock()
	sm.contexts[scrollID] = ctx
	contextCount := len(sm.contexts)
	// 打印 manager 地址，确认是否是同一个实例
	managerAddr := fmt.Sprintf("%p", sm)
	sm.mutex.Unlock()

	// 添加日志帮助调试
	logger.Info("Created scroll context [%s] for index [%s], TTL=%v, total contexts=%d, manager=%s",
		scrollID, indexName, scrollTTL, contextCount, managerAddr)
	return ctx, nil
}

// GetScrollContext 获取 scroll context
func (sm *ScrollManager) GetScrollContext(scrollID string) (*ScrollContext, error) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	ctx, exists := sm.contexts[scrollID]
	managerAddr := fmt.Sprintf("%p", sm)
	if !exists {
		// 添加调试日志，打印所有现有的 scroll ID
		existingIDs := make([]string, 0, len(sm.contexts))
		for id := range sm.contexts {
			existingIDs = append(existingIDs, id[:8]+"...") // 只打印前8个字符
		}
		logger.Warn("Scroll context [%s] not found, total contexts: %d, existing: %v, manager=%s", scrollID, len(sm.contexts), existingIDs, managerAddr)
		return nil, fmt.Errorf("scroll context [%s] not found", scrollID)
	}

	// 检查是否过期
	if time.Now().After(ctx.ExpiresAt) {
		// 异步删除过期 context
		go sm.DeleteScrollContext(scrollID)
		logger.Warn("Scroll context [%s] expired, created: %v, expires: %v", scrollID, ctx.CreatedAt, ctx.ExpiresAt)
		return nil, fmt.Errorf("scroll context [%s] has expired", scrollID)
	}

	return ctx, nil
}

// UpdateScrollContext 更新 scroll context（用于记录最后一个结果的 sort 值）
// lastSort: 最后一个结果的sort值，如果为nil表示使用from分页方式
func (sm *ScrollManager) UpdateScrollContext(scrollID string, lastSort []interface{}) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	ctx, exists := sm.contexts[scrollID]
	if !exists {
		return fmt.Errorf("scroll context [%s] not found", scrollID)
	}

	if time.Now().After(ctx.ExpiresAt) {
		delete(sm.contexts, scrollID)
		return fmt.Errorf("scroll context [%s] has expired", scrollID)
	}

	ctx.LastSort = lastSort
	// 只有在使用from分页方式时才更新From（lastSort为nil表示使用from分页）
	// 如果使用search_after（lastSort不为nil），From不应该被更新，因为search_after不依赖From
	if lastSort == nil {
		ctx.From += ctx.Size // 使用from分页时，更新from位置
	}
	// 如果lastSort不为nil，说明使用search_after，From保持不变
	return nil
}

// DeleteScrollContext 删除 scroll context
func (sm *ScrollManager) DeleteScrollContext(scrollID string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	if _, exists := sm.contexts[scrollID]; exists {
		delete(sm.contexts, scrollID)
		logger.Info("Deleted scroll context [%s], remaining contexts=%d", scrollID, len(sm.contexts))
	}
}

// cleanupExpired 定期清理过期的 scroll context
func (sm *ScrollManager) cleanupExpired() {
	for {
		select {
		case <-sm.cleanupTicker.C:
			sm.mutex.Lock()
			now := time.Now()
			expiredCount := 0
			for id, ctx := range sm.contexts {
				if now.After(ctx.ExpiresAt) {
					delete(sm.contexts, id)
					expiredCount++
				}
			}
			if expiredCount > 0 {
				logger.Info("Cleaned up %d expired scroll contexts", expiredCount)
			}
			sm.mutex.Unlock()
		case <-sm.stopCleanup:
			return
		}
	}
}

// Stop 停止清理 goroutine（用于优雅关闭）
func (sm *ScrollManager) Stop() {
	sm.cleanupTicker.Stop()
	sm.stopCleanup <- true
}

// parseScrollTTL 解析 scroll TTL 字符串（如 "1m", "5m", "1h"）
func parseScrollTTL(scrollStr string) (time.Duration, error) {
	if scrollStr == "" {
		return 1 * time.Minute, nil // 默认 1 分钟
	}

	// 解析格式：数字 + 单位（m=分钟, h=小时, d=天）
	var duration time.Duration
	var unit string
	var value int64

	// 简单解析：假设格式为 "1m", "5m", "1h" 等
	_, err := fmt.Sscanf(scrollStr, "%d%s", &value, &unit)
	if err != nil {
		return 0, fmt.Errorf("invalid scroll TTL format: %s", scrollStr)
	}

	switch unit {
	case "m", "M":
		duration = time.Duration(value) * time.Minute
	case "h", "H":
		duration = time.Duration(value) * time.Hour
	case "d", "D":
		duration = time.Duration(value) * 24 * time.Hour
	case "s", "S":
		duration = time.Duration(value) * time.Second
	default:
		return 0, fmt.Errorf("unsupported scroll TTL unit: %s", unit)
	}

	if duration <= 0 {
		return 0, fmt.Errorf("scroll TTL must be positive")
	}

	// 限制最大 TTL 为 1 小时（避免内存泄漏）
	maxTTL := 1 * time.Hour
	if duration > maxTTL {
		logger.Warn("Scroll TTL %v exceeds maximum %v, using maximum", duration, maxTTL)
		duration = maxTTL
	}

	return duration, nil
}
