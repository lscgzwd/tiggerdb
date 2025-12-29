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

package script

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

// CompiledScript 预编译的脚本
type CompiledScript struct {
	Source    string                 // 原始脚本源码
	Hash      string                 // 脚本哈希（用于缓存键）
	Params    map[string]interface{} // 默认参数
	CreatedAt time.Time              // 创建时间
	LastUsed  time.Time              // 最后使用时间
	UseCount  int64                  // 使用次数
}

// ScriptCache 脚本缓存
type ScriptCache struct {
	mu      sync.RWMutex
	scripts map[string]*CompiledScript
	maxSize int           // 最大缓存数量
	ttl     time.Duration // 缓存过期时间
	hits    int64         // 命中次数
	misses  int64         // 未命中次数
}

// 全局缓存实例
var globalCache = NewScriptCache(1000, 30*time.Minute)

// NewScriptCache 创建脚本缓存
func NewScriptCache(maxSize int, ttl time.Duration) *ScriptCache {
	cache := &ScriptCache{
		scripts: make(map[string]*CompiledScript),
		maxSize: maxSize,
		ttl:     ttl,
	}
	// 启动后台清理协程
	go cache.cleanupLoop()
	return cache
}

// GetGlobalCache 获取全局缓存
func GetGlobalCache() *ScriptCache {
	return globalCache
}

// hashScript 计算脚本哈希
func hashScript(source string) string {
	h := sha256.New()
	h.Write([]byte(source))
	return hex.EncodeToString(h.Sum(nil))[:16] // 使用前16个字符
}

// Get 获取缓存的脚本
func (c *ScriptCache) Get(source string) (*CompiledScript, bool) {
	hash := hashScript(source)

	c.mu.RLock()
	script, ok := c.scripts[hash]
	c.mu.RUnlock()

	if !ok {
		c.mu.Lock()
		c.misses++
		c.mu.Unlock()
		return nil, false
	}

	// 检查是否过期
	if time.Since(script.LastUsed) > c.ttl {
		c.mu.Lock()
		delete(c.scripts, hash)
		c.misses++
		c.mu.Unlock()
		return nil, false
	}

	// 更新使用信息
	c.mu.Lock()
	script.LastUsed = time.Now()
	script.UseCount++
	c.hits++
	c.mu.Unlock()

	return script, true
}

// Put 缓存脚本
func (c *ScriptCache) Put(source string, params map[string]interface{}) *CompiledScript {
	hash := hashScript(source)

	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查是否需要清理
	if len(c.scripts) >= c.maxSize {
		c.evictOldest()
	}

	script := &CompiledScript{
		Source:    source,
		Hash:      hash,
		Params:    params,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
		UseCount:  1,
	}

	c.scripts[hash] = script
	return script
}

// evictOldest 移除最旧的缓存（LRU策略）
func (c *ScriptCache) evictOldest() {
	var oldestHash string
	var oldestTime time.Time

	for hash, script := range c.scripts {
		if oldestHash == "" || script.LastUsed.Before(oldestTime) {
			oldestHash = hash
			oldestTime = script.LastUsed
		}
	}

	if oldestHash != "" {
		delete(c.scripts, oldestHash)
	}
}

// cleanupLoop 后台清理过期缓存
func (c *ScriptCache) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.cleanup()
	}
}

// cleanup 清理过期缓存
func (c *ScriptCache) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for hash, script := range c.scripts {
		if now.Sub(script.LastUsed) > c.ttl {
			delete(c.scripts, hash)
		}
	}
}

// Stats 返回缓存统计信息
func (c *ScriptCache) Stats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hitRate := 0.0
	total := c.hits + c.misses
	if total > 0 {
		hitRate = float64(c.hits) / float64(total) * 100
	}

	return map[string]interface{}{
		"size":     len(c.scripts),
		"max_size": c.maxSize,
		"hits":     c.hits,
		"misses":   c.misses,
		"hit_rate": hitRate,
	}
}

// Clear 清空缓存
func (c *ScriptCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.scripts = make(map[string]*CompiledScript)
	c.hits = 0
	c.misses = 0
}

// Size 返回当前缓存大小
func (c *ScriptCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.scripts)
}
