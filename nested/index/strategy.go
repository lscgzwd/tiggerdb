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

package index

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lscgzwd/tiggerdb/nested/document"
)

// IndexStrategy 索引策略接口
type IndexStrategy interface {
	// 索引嵌套文档
	Index(parentDoc *document.ParentDocument) ([]*IndexedDocument, error)

	// 查询嵌套文档
	Query(query *NestedQuery) ([]*QueryResult, error)

	// 删除嵌套文档
	Delete(parentDocID string) error

	// 更新嵌套文档
	Update(parentDoc *document.ParentDocument) ([]*IndexedDocument, error)

	// 获取统计信息
	GetStats() *IndexStats
}

// NestedQuery 嵌套查询
type NestedQuery struct {
	Path      string      `json:"path"`
	Query     interface{} `json:"query"`
	ScoreMode string      `json:"score_mode,omitempty"`
	InnerHits *InnerHits  `json:"inner_hits,omitempty"`
}

// InnerHits 内部命中
type InnerHits struct {
	Name string   `json:"name,omitempty"`
	Size int      `json:"size,omitempty"`
	From int      `json:"from,omitempty"`
	Sort []string `json:"sort,omitempty"`
}

// IndexedDocument 索引文档
type IndexedDocument struct {
	ID       string                 `json:"_id"`
	ParentID string                 `json:"_parent"`
	Path     string                 `json:"_path"`
	Position int                    `json:"_position"`
	Fields   map[string]interface{} `json:"_fields"`
	Score    float64                `json:"_score,omitempty"`
}

// QueryResult 查询结果
type QueryResult struct {
	Document   *IndexedDocument    `json:"document"`
	Score      float64             `json:"score"`
	Highlights map[string][]string `json:"highlights,omitempty"`
}

// IndexStats 索引统计
type IndexStats struct {
	TotalDocuments int64            `json:"total_documents"`
	PathStats      map[string]int64 `json:"path_stats"`
	LastUpdate     time.Time        `json:"last_update"`
}

// IndependentIndexStrategy 独立索引策略
// 每个嵌套文档作为独立的文档进行索引
type IndependentIndexStrategy struct {
	documents   map[string]*IndexedDocument // id -> document
	pathIndex   map[string][]string         // path -> []documentIDs
	parentIndex map[string][]string         // parentID -> []documentIDs
	stats       *IndexStats
}

// NewIndependentIndexStrategy 创建独立索引策略
func NewIndependentIndexStrategy() *IndependentIndexStrategy {
	return &IndependentIndexStrategy{
		documents:   make(map[string]*IndexedDocument),
		pathIndex:   make(map[string][]string),
		parentIndex: make(map[string][]string),
		stats: &IndexStats{
			PathStats:  make(map[string]int64),
			LastUpdate: time.Now(),
		},
	}
}

// Index 索引嵌套文档
func (iis *IndependentIndexStrategy) Index(parentDoc *document.ParentDocument) ([]*IndexedDocument, error) {
	if parentDoc == nil {
		return nil, fmt.Errorf("parent document cannot be nil")
	}

	var indexedDocs []*IndexedDocument

	// 为每个嵌套文档创建索引文档
	for path, nestedDocs := range parentDoc.NestedDocs {
		for _, nestedDoc := range nestedDocs {
			indexedDoc := &IndexedDocument{
				ID:       nestedDoc.ID,
				ParentID: nestedDoc.ParentID,
				Path:     nestedDoc.Path,
				Position: nestedDoc.Position,
				Fields:   nestedDoc.Fields,
			}

			// 添加到主索引
			iis.documents[indexedDoc.ID] = indexedDoc

			// 添加到路径索引
			if iis.pathIndex[path] == nil {
				iis.pathIndex[path] = make([]string, 0)
			}
			iis.pathIndex[path] = append(iis.pathIndex[path], indexedDoc.ID)

			// 添加到父文档索引
			parentKey := parentDoc.ID
			if iis.parentIndex[parentKey] == nil {
				iis.parentIndex[parentKey] = make([]string, 0)
			}
			iis.parentIndex[parentKey] = append(iis.parentIndex[parentKey], indexedDoc.ID)

			indexedDocs = append(indexedDocs, indexedDoc)
		}
	}

	iis.updateStats()
	return indexedDocs, nil
}

// Query 查询嵌套文档
func (iis *IndependentIndexStrategy) Query(query *NestedQuery) ([]*QueryResult, error) {
	if query == nil {
		return nil, fmt.Errorf("query cannot be nil")
	}

	var results []*QueryResult

	// 获取指定路径的文档
	docIDs, exists := iis.pathIndex[query.Path]
	if !exists {
		return results, nil
	}

	// 简单实现：返回所有匹配路径的文档
	// 实际应该根据query.Query进行过滤
	for _, docID := range docIDs {
		if doc, exists := iis.documents[docID]; exists {
			result := &QueryResult{
				Document: doc,
				Score:    1.0, // 默认分数
			}
			results = append(results, result)
		}
	}

	return results, nil
}

// Delete 删除嵌套文档
func (iis *IndependentIndexStrategy) Delete(parentDocID string) error {
	docIDs, exists := iis.parentIndex[parentDocID]
	if !exists {
		return nil
	}

	// 从所有索引中移除
	for _, docID := range docIDs {
		if doc, exists := iis.documents[docID]; exists {
			// 从路径索引中移除
			if pathDocs, exists := iis.pathIndex[doc.Path]; exists {
				iis.pathIndex[doc.Path] = removeFromSlice(pathDocs, docID)
				if len(iis.pathIndex[doc.Path]) == 0 {
					delete(iis.pathIndex, doc.Path)
				}
			}
		}
		delete(iis.documents, docID)
	}

	delete(iis.parentIndex, parentDocID)
	iis.updateStats()

	return nil
}

// Update 更新嵌套文档
func (iis *IndependentIndexStrategy) Update(parentDoc *document.ParentDocument) ([]*IndexedDocument, error) {
	// 先删除旧文档
	if err := iis.Delete(parentDoc.ID); err != nil {
		return nil, err
	}

	// 再索引新文档
	return iis.Index(parentDoc)
}

// GetStats 获取统计信息
func (iis *IndependentIndexStrategy) GetStats() *IndexStats {
	return iis.stats
}

// updateStats 更新统计信息
func (iis *IndependentIndexStrategy) updateStats() {
	iis.stats.TotalDocuments = int64(len(iis.documents))
	iis.stats.LastUpdate = time.Now()

	// 更新路径统计
	iis.stats.PathStats = make(map[string]int64)
	for path, docIDs := range iis.pathIndex {
		iis.stats.PathStats[path] = int64(len(docIDs))
	}
}

// removeFromSlice 从切片中移除元素
func removeFromSlice(slice []string, item string) []string {
	for i, v := range slice {
		if v == item {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

// HierarchicalIndexStrategy 层次索引策略
// 保持嵌套文档的层次结构
type HierarchicalIndexStrategy struct {
	rootDocuments map[string]*document.ParentDocument
	stats         *IndexStats
}

// NewHierarchicalIndexStrategy 创建层次索引策略
func NewHierarchicalIndexStrategy() *HierarchicalIndexStrategy {
	return &HierarchicalIndexStrategy{
		rootDocuments: make(map[string]*document.ParentDocument),
		stats: &IndexStats{
			PathStats:  make(map[string]int64),
			LastUpdate: time.Now(),
		},
	}
}

// Index 索引嵌套文档
func (his *HierarchicalIndexStrategy) Index(parentDoc *document.ParentDocument) ([]*IndexedDocument, error) {
	if parentDoc == nil {
		return nil, fmt.Errorf("parent document cannot be nil")
	}

	his.rootDocuments[parentDoc.ID] = parentDoc
	his.updateStats()

	// 返回所有嵌套文档的索引表示
	var indexedDocs []*IndexedDocument
	for _, nestedDocs := range parentDoc.NestedDocs {
		for _, nestedDoc := range nestedDocs {
			indexedDoc := &IndexedDocument{
				ID:       nestedDoc.ID,
				ParentID: nestedDoc.ParentID,
				Path:     nestedDoc.Path,
				Position: nestedDoc.Position,
				Fields:   nestedDoc.Fields,
			}
			indexedDocs = append(indexedDocs, indexedDoc)
		}
	}

	return indexedDocs, nil
}

// Query 查询嵌套文档
func (his *HierarchicalIndexStrategy) Query(query *NestedQuery) ([]*QueryResult, error) {
	var results []*QueryResult

	// 遍历所有根文档
	for _, rootDoc := range his.rootDocuments {
		nestedDocs := rootDoc.GetNestedDocuments(query.Path)
		for _, nestedDoc := range nestedDocs {
			// 简单匹配，实际应该根据query.Query进行过滤
			indexedDoc := &IndexedDocument{
				ID:       nestedDoc.ID,
				ParentID: nestedDoc.ParentID,
				Path:     nestedDoc.Path,
				Position: nestedDoc.Position,
				Fields:   nestedDoc.Fields,
			}

			result := &QueryResult{
				Document: indexedDoc,
				Score:    1.0,
			}
			results = append(results, result)
		}
	}

	return results, nil
}

// Delete 删除嵌套文档
func (his *HierarchicalIndexStrategy) Delete(parentDocID string) error {
	delete(his.rootDocuments, parentDocID)
	his.updateStats()
	return nil
}

// Update 更新嵌套文档
func (his *HierarchicalIndexStrategy) Update(parentDoc *document.ParentDocument) ([]*IndexedDocument, error) {
	return his.Index(parentDoc)
}

// GetStats 获取统计信息
func (his *HierarchicalIndexStrategy) GetStats() *IndexStats {
	return his.stats
}

// updateStats 更新统计信息
func (his *HierarchicalIndexStrategy) updateStats() {
	totalDocs := int64(0)
	pathStats := make(map[string]int64)

	for _, rootDoc := range his.rootDocuments {
		totalDocs += int64(rootDoc.GetNestedDocumentCount())
		for path := range rootDoc.NestedDocs {
			pathStats[path]++
		}
	}

	his.stats.TotalDocuments = totalDocs
	his.stats.PathStats = pathStats
	his.stats.LastUpdate = time.Now()
}

// IndexStrategyFactory 索引策略工厂
type IndexStrategyFactory struct{}

// CreateStrategy 创建索引策略
func (isf *IndexStrategyFactory) CreateStrategy(strategyType string) (IndexStrategy, error) {
	switch strings.ToLower(strategyType) {
	case "independent":
		return NewIndependentIndexStrategy(), nil
	case "hierarchical":
		return NewHierarchicalIndexStrategy(), nil
	default:
		return nil, fmt.Errorf("unsupported strategy type: %s", strategyType)
	}
}

// GetAvailableStrategies 获取可用策略
func (isf *IndexStrategyFactory) GetAvailableStrategies() []string {
	return []string{"independent", "hierarchical"}
}

// OptimizeQuery 优化查询
func OptimizeQuery(query *NestedQuery, strategy IndexStrategy) (*NestedQuery, error) {
	optimized := *query

	// 根据策略进行优化
	switch s := strategy.(type) {
	case *IndependentIndexStrategy:
		return optimizeForIndependent(optimized, s)
	case *HierarchicalIndexStrategy:
		return optimizeForHierarchical(optimized, s)
	default:
		return &optimized, nil
	}
}

// optimizeForIndependent 为独立索引策略优化查询
func optimizeForIndependent(query NestedQuery, strategy *IndependentIndexStrategy) (*NestedQuery, error) {
	// 检查路径是否存在
	if _, exists := strategy.pathIndex[query.Path]; !exists {
		// 路径不存在，可以提前返回空结果
		return &query, nil
	}

	// 可以添加更多优化逻辑
	return &query, nil
}

// optimizeForHierarchical 为层次索引策略优化查询
func optimizeForHierarchical(query NestedQuery, strategy *HierarchicalIndexStrategy) (*NestedQuery, error) {
	// 层次结构的优化逻辑
	return &query, nil
}

// MergeResults 合并查询结果
func MergeResults(results []*QueryResult, scoreMode string) []*QueryResult {
	if len(results) <= 1 {
		return results
	}

	switch strings.ToLower(scoreMode) {
	case "avg":
		return mergeByAverageScore(results)
	case "max":
		return mergeByMaxScore(results)
	case "min":
		return mergeByMinScore(results)
	case "sum":
		return mergeBySumScore(results)
	default:
		return results
	}
}

// mergeByAverageScore 按平均分数合并
func mergeByAverageScore(results []*QueryResult) []*QueryResult {
	// 按文档ID分组
	docGroups := make(map[string][]*QueryResult)

	for _, result := range results {
		docID := result.Document.ID
		docGroups[docID] = append(docGroups[docID], result)
	}

	var merged []*QueryResult
	for _, group := range docGroups {
		if len(group) == 1 {
			merged = append(merged, group[0])
			continue
		}

		// 计算平均分数
		totalScore := 0.0
		for _, result := range group {
			totalScore += result.Score
		}
		avgScore := totalScore / float64(len(group))

		// 使用第一个结果，但更新分数
		mergedResult := *group[0]
		mergedResult.Score = avgScore
		merged = append(merged, &mergedResult)
	}

	// 按分数排序
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score > merged[j].Score
	})

	return merged
}

// mergeByMaxScore 按最大分数合并
func mergeByMaxScore(results []*QueryResult) []*QueryResult {
	docGroups := make(map[string][]*QueryResult)

	for _, result := range results {
		docID := result.Document.ID
		docGroups[docID] = append(docGroups[docID], result)
	}

	var merged []*QueryResult
	for _, group := range docGroups {
		// 找到最大分数
		maxResult := group[0]
		for _, result := range group[1:] {
			if result.Score > maxResult.Score {
				maxResult = result
			}
		}
		merged = append(merged, maxResult)
	}

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score > merged[j].Score
	})

	return merged
}

// mergeByMinScore 按最小分数合并
func mergeByMinScore(results []*QueryResult) []*QueryResult {
	docGroups := make(map[string][]*QueryResult)

	for _, result := range results {
		docID := result.Document.ID
		docGroups[docID] = append(docGroups[docID], result)
	}

	var merged []*QueryResult
	for _, group := range docGroups {
		// 找到最小分数
		minResult := group[0]
		for _, result := range group[1:] {
			if result.Score < minResult.Score {
				minResult = result
			}
		}
		merged = append(merged, minResult)
	}

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score > merged[j].Score
	})

	return merged
}

// mergeBySumScore 按总分数合并
func mergeBySumScore(results []*QueryResult) []*QueryResult {
	docGroups := make(map[string][]*QueryResult)

	for _, result := range results {
		docID := result.Document.ID
		docGroups[docID] = append(docGroups[docID], result)
	}

	var merged []*QueryResult
	for _, group := range docGroups {
		if len(group) == 1 {
			merged = append(merged, group[0])
			continue
		}

		// 计算总分数
		totalScore := 0.0
		for _, result := range group {
			totalScore += result.Score
		}

		// 使用第一个结果，但更新分数
		mergedResult := *group[0]
		mergedResult.Score = totalScore
		merged = append(merged, &mergedResult)
	}

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score > merged[j].Score
	})

	return merged
}
