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

package search

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/lscgzwd/tiggerdb/nested/index"
)

// Collector 基础收集器接口
type Collector interface {
	Collect(ctx context.Context, doc *index.IndexedDocument, score float64) error
	Results() []*index.QueryResult
	Reset()
	SetMaxResults(max int)
}

// NestedCollector 嵌套结果收集器
type NestedCollector struct {
	parentCollector Collector
	childCollectors []Collector
	coordinator     *NestedSearchCoordinator
	mu              sync.Mutex
}

// NewNestedCollector 创建嵌套收集器
func NewNestedCollector(parentCollector Collector) *NestedCollector {
	return &NestedCollector{
		parentCollector: parentCollector,
		childCollectors: make([]Collector, 0),
		coordinator:     NewNestedSearchCoordinator(),
	}
}

// Collect 收集嵌套搜索结果
func (nc *NestedCollector) Collect(ctx context.Context, doc *index.IndexedDocument, score float64) error {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	// 如果是父文档，添加到父收集器
	if doc.Path == "" || doc.Position == 0 {
		return nc.parentCollector.Collect(ctx, doc, score)
	}

	// 如果是嵌套文档，根据路径分发到相应的子收集器
	path := doc.Path
	if collector := nc.getOrCreateChildCollector(path); collector != nil {
		return collector.Collect(ctx, doc, score)
	}

	return fmt.Errorf("failed to get collector for path: %s", path)
}

// getOrCreateChildCollector 获取或创建子收集器
func (nc *NestedCollector) getOrCreateChildCollector(path string) Collector {
	// 查找现有收集器
	for _, collector := range nc.childCollectors {
		// 这里需要根据具体实现来判断收集器是否匹配路径
		// 暂时返回第一个收集器
		if collector != nil {
			return collector
		}
	}

	// 创建新的子收集器
	childCollector := nc.createChildCollector(path)
	if childCollector != nil {
		nc.childCollectors = append(nc.childCollectors, childCollector)
	}

	return childCollector
}

// createChildCollector 创建子收集器
func (nc *NestedCollector) createChildCollector(path string) Collector {
	// 创建一个简单的内存收集器
	return NewMemoryCollector()
}

// Results 获取所有结果
func (nc *NestedCollector) Results() []*index.QueryResult {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	var allResults []*index.QueryResult

	// 获取父文档结果
	parentResults := nc.parentCollector.Results()
	allResults = append(allResults, parentResults...)

	// 获取所有子文档结果
	for _, childCollector := range nc.childCollectors {
		childResults := childCollector.Results()
		allResults = append(allResults, childResults...)
	}

	// 合并和排序结果
	return nc.mergeAndSortResults(allResults)
}

// mergeAndSortResults 合并和排序结果
func (nc *NestedCollector) mergeAndSortResults(results []*index.QueryResult) []*index.QueryResult {
	if len(results) <= 1 {
		return results
	}

	// 按分数降序排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	return results
}

// Reset 重置收集器
func (nc *NestedCollector) Reset() {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	nc.parentCollector.Reset()
	for _, collector := range nc.childCollectors {
		collector.Reset()
	}
	nc.childCollectors = nc.childCollectors[:0]
}

// SetMaxResults 设置最大结果数
func (nc *NestedCollector) SetMaxResults(max int) {
	nc.parentCollector.SetMaxResults(max)
	for _, collector := range nc.childCollectors {
		collector.SetMaxResults(max)
	}
}

// AddChildCollector 添加子收集器
func (nc *NestedCollector) AddChildCollector(path string, collector Collector) {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	nc.childCollectors = append(nc.childCollectors, collector)
}

// RemoveChildCollector 移除子收集器
func (nc *NestedCollector) RemoveChildCollector(path string) {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	for i, collector := range nc.childCollectors {
		// 这里需要根据具体实现来判断是否匹配路径
		_ = collector // 暂时忽略
		nc.childCollectors = append(nc.childCollectors[:i], nc.childCollectors[i+1:]...)
		break
	}
}

// GetChildCollector 获取子收集器
func (nc *NestedCollector) GetChildCollector(path string) Collector {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	for _, collector := range nc.childCollectors {
		// 这里需要根据具体实现来判断是否匹配路径
		return collector
	}
	return nil
}

// MemoryCollector 内存收集器实现
type MemoryCollector struct {
	results    []*index.QueryResult
	maxResults int
	mu         sync.Mutex
}

// NewMemoryCollector 创建内存收集器
func NewMemoryCollector() *MemoryCollector {
	return &MemoryCollector{
		results:    make([]*index.QueryResult, 0),
		maxResults: 10000, // 默认最大结果数
	}
}

// Collect 收集结果
func (mc *MemoryCollector) Collect(ctx context.Context, doc *index.IndexedDocument, score float64) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	result := &index.QueryResult{
		Document: doc,
		Score:    score,
	}

	mc.results = append(mc.results, result)

	// 如果超过最大结果数，移除分数最低的结果
	if len(mc.results) > mc.maxResults {
		mc.trimToMaxResults()
	}

	return nil
}

// trimToMaxResults 修剪到最大结果数
func (mc *MemoryCollector) trimToMaxResults() {
	if len(mc.results) <= mc.maxResults {
		return
	}

	// 按分数排序，保留前N个
	sort.Slice(mc.results, func(i, j int) bool {
		return mc.results[i].Score > mc.results[j].Score
	})

	mc.results = mc.results[:mc.maxResults]
}

// Results 获取结果
func (mc *MemoryCollector) Results() []*index.QueryResult {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	// 返回副本
	results := make([]*index.QueryResult, len(mc.results))
	copy(results, mc.results)
	return results
}

// Reset 重置收集器
func (mc *MemoryCollector) Reset() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.results = mc.results[:0]
}

// SetMaxResults 设置最大结果数
func (mc *MemoryCollector) SetMaxResults(max int) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.maxResults = max
	if len(mc.results) > max {
		mc.trimToMaxResults()
	}
}

// NestedSearchCoordinator 嵌套搜索协调器
type NestedSearchCoordinator struct {
	activeCollectors map[string]*NestedCollector
	mu               sync.RWMutex
}

// NewNestedSearchCoordinator 创建嵌套搜索协调器
func NewNestedSearchCoordinator() *NestedSearchCoordinator {
	return &NestedSearchCoordinator{
		activeCollectors: make(map[string]*NestedCollector),
	}
}

// RegisterCollector 注册收集器
func (nsc *NestedSearchCoordinator) RegisterCollector(queryID string, collector *NestedCollector) {
	nsc.mu.Lock()
	defer nsc.mu.Unlock()

	nsc.activeCollectors[queryID] = collector
}

// UnregisterCollector 取消注册收集器
func (nsc *NestedSearchCoordinator) UnregisterCollector(queryID string) {
	nsc.mu.Lock()
	defer nsc.mu.Unlock()

	delete(nsc.activeCollectors, queryID)
}

// GetCollector 获取收集器
func (nsc *NestedSearchCoordinator) GetCollector(queryID string) *NestedCollector {
	nsc.mu.RLock()
	defer nsc.mu.RUnlock()

	return nsc.activeCollectors[queryID]
}

// CleanupInactiveCollectors 清理非活跃收集器
func (nsc *NestedSearchCoordinator) CleanupInactiveCollectors() {
	nsc.mu.Lock()
	defer nsc.mu.Unlock()

	// 这里可以实现基于时间的清理逻辑
	// 暂时保留所有收集器
}

// GetStats 获取统计信息
func (nsc *NestedSearchCoordinator) GetStats() map[string]interface{} {
	nsc.mu.RLock()
	defer nsc.mu.RUnlock()

	return map[string]interface{}{
		"active_collectors": len(nsc.activeCollectors),
	}
}

// NestedResultAggregator 嵌套结果聚合器
type NestedResultAggregator struct {
	parentResults []*index.QueryResult
	childResults  map[string][]*index.QueryResult // path -> results
	scoreMode     string
}

// NewNestedResultAggregator 创建嵌套结果聚合器
func NewNestedResultAggregator(scoreMode string) *NestedResultAggregator {
	return &NestedResultAggregator{
		parentResults: make([]*index.QueryResult, 0),
		childResults:  make(map[string][]*index.QueryResult),
		scoreMode:     scoreMode,
	}
}

// AddParentResult 添加父结果
func (nra *NestedResultAggregator) AddParentResult(result *index.QueryResult) {
	nra.parentResults = append(nra.parentResults, result)
}

// AddChildResult 添加子结果
func (nra *NestedResultAggregator) AddChildResult(path string, result *index.QueryResult) {
	if nra.childResults[path] == nil {
		nra.childResults[path] = make([]*index.QueryResult, 0)
	}
	nra.childResults[path] = append(nra.childResults[path], result)
}

// Aggregate 聚合结果
func (nra *NestedResultAggregator) Aggregate() []*index.QueryResult {
	var finalResults []*index.QueryResult

	// 根据score_mode聚合结果
	switch nra.scoreMode {
	case "avg":
		finalResults = nra.aggregateByAvgScore()
	case "sum":
		finalResults = nra.aggregateBySumScore()
	case "max":
		finalResults = nra.aggregateByMaxScore()
	case "min":
		finalResults = nra.aggregateByMinScore()
	default:
		// 默认使用父文档结果
		finalResults = nra.parentResults
	}

	// 按分数排序
	sort.Slice(finalResults, func(i, j int) bool {
		return finalResults[i].Score > finalResults[j].Score
	})

	return finalResults
}

// aggregateByAvgScore 按平均分数聚合
func (nra *NestedResultAggregator) aggregateByAvgScore() []*index.QueryResult {
	resultMap := make(map[string]*index.QueryResult)

	// 处理父结果
	for _, result := range nra.parentResults {
		docID := result.Document.ID
		resultMap[docID] = result
	}

	// 处理子结果，按文档ID聚合
	for _, results := range nra.childResults {
		for _, result := range results {
			docID := result.Document.ParentID
			if existing, exists := resultMap[docID]; exists {
				// 计算平均分数
				existing.Score = (existing.Score + result.Score) / 2
			} else {
				// 创建新的父文档结果
				parentResult := &index.QueryResult{
					Document: &index.IndexedDocument{
						ID: docID,
					},
					Score: result.Score,
				}
				resultMap[docID] = parentResult
			}
		}
	}

	var finalResults []*index.QueryResult
	for _, result := range resultMap {
		finalResults = append(finalResults, result)
	}

	return finalResults
}

// aggregateBySumScore 按总分数聚合
func (nra *NestedResultAggregator) aggregateBySumScore() []*index.QueryResult {
	resultMap := make(map[string]*index.QueryResult)

	// 处理父结果
	for _, result := range nra.parentResults {
		docID := result.Document.ID
		resultMap[docID] = result
	}

	// 处理子结果，按文档ID累加分数
	for _, results := range nra.childResults {
		for _, result := range results {
			docID := result.Document.ParentID
			if existing, exists := resultMap[docID]; exists {
				existing.Score += result.Score
			} else {
				parentResult := &index.QueryResult{
					Document: &index.IndexedDocument{
						ID: docID,
					},
					Score: result.Score,
				}
				resultMap[docID] = parentResult
			}
		}
	}

	var finalResults []*index.QueryResult
	for _, result := range resultMap {
		finalResults = append(finalResults, result)
	}

	return finalResults
}

// aggregateByMaxScore 按最大分数聚合
func (nra *NestedResultAggregator) aggregateByMaxScore() []*index.QueryResult {
	resultMap := make(map[string]*index.QueryResult)

	// 处理父结果
	for _, result := range nra.parentResults {
		docID := result.Document.ID
		resultMap[docID] = result
	}

	// 处理子结果，取最大分数
	for _, results := range nra.childResults {
		for _, result := range results {
			docID := result.Document.ParentID
			if existing, exists := resultMap[docID]; exists {
				if result.Score > existing.Score {
					existing.Score = result.Score
				}
			} else {
				parentResult := &index.QueryResult{
					Document: &index.IndexedDocument{
						ID: docID,
					},
					Score: result.Score,
				}
				resultMap[docID] = parentResult
			}
		}
	}

	var finalResults []*index.QueryResult
	for _, result := range resultMap {
		finalResults = append(finalResults, result)
	}

	return finalResults
}

// aggregateByMinScore 按最小分数聚合
func (nra *NestedResultAggregator) aggregateByMinScore() []*index.QueryResult {
	resultMap := make(map[string]*index.QueryResult)

	// 处理父结果
	for _, result := range nra.parentResults {
		docID := result.Document.ID
		resultMap[docID] = result
	}

	// 处理子结果，取最小分数
	for _, results := range nra.childResults {
		for _, result := range results {
			docID := result.Document.ParentID
			if existing, exists := resultMap[docID]; exists {
				if result.Score < existing.Score {
					existing.Score = result.Score
				}
			} else {
				parentResult := &index.QueryResult{
					Document: &index.IndexedDocument{
						ID: docID,
					},
					Score: result.Score,
				}
				resultMap[docID] = parentResult
			}
		}
	}

	var finalResults []*index.QueryResult
	for _, result := range resultMap {
		finalResults = append(finalResults, result)
	}

	return finalResults
}
