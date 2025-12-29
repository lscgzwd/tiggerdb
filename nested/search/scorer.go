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
	"math"
	"sync"

	"github.com/lscgzwd/tiggerdb/nested/index"
)

// Scorer 评分器接口
type Scorer interface {
	Score(doc *index.IndexedDocument) float64
	Explain(doc *index.IndexedDocument) *ScoreExplanation
	SetBoost(boost float64)
	SetQueryNorm(norm float64)
}

// NestedScorer 嵌套评分计算器
type NestedScorer struct {
	parentScorer Scorer
	childScorers map[string]Scorer // path -> scorer
	scoreMode    string
	scoreCache   map[string]float64
	cacheMutex   sync.RWMutex
	boost        float64
	queryNorm    float64
}

// ScoreExplanation 评分解释
type ScoreExplanation struct {
	Value       float64             `json:"value"`
	Description string              `json:"description"`
	Details     []*ScoreExplanation `json:"details,omitempty"`
}

// NewScoreExplanation 创建评分解释
func NewScoreExplanation(value float64, description string) *ScoreExplanation {
	return &ScoreExplanation{
		Value:       value,
		Description: description,
		Details:     make([]*ScoreExplanation, 0),
	}
}

// AddDetail 添加评分细节
func (se *ScoreExplanation) AddDetail(detail *ScoreExplanation) {
	se.Details = append(se.Details, detail)
}

// NewNestedScorer 创建嵌套评分器
func NewNestedScorer(parentScorer Scorer, scoreMode string) *NestedScorer {
	return &NestedScorer{
		parentScorer: parentScorer,
		childScorers: make(map[string]Scorer),
		scoreMode:    scoreMode,
		scoreCache:   make(map[string]float64),
		boost:        1.0,
		queryNorm:    1.0,
	}
}

// Score 计算嵌套文档评分
func (ns *NestedScorer) Score(doc *index.IndexedDocument) float64 {
	if doc == nil {
		return 0.0
	}

	// 检查缓存
	cacheKey := ns.getCacheKey(doc)
	if cachedScore, exists := ns.getCachedScore(cacheKey); exists {
		return cachedScore
	}

	var score float64

	if doc.Path == "" || doc.Position == 0 {
		// 父文档评分
		score = ns.scoreParentDocument(doc)
	} else {
		// 子文档评分
		score = ns.scoreChildDocument(doc)
	}

	// 应用boost和queryNorm
	score = score * ns.boost * ns.queryNorm

	// 缓存结果
	ns.setCachedScore(cacheKey, score)

	return score
}

// scoreParentDocument 评分父文档
func (ns *NestedScorer) scoreParentDocument(doc *index.IndexedDocument) float64 {
	if ns.parentScorer == nil {
		return 0.0
	}

	parentScore := ns.parentScorer.Score(doc)

	// 可以根据嵌套文档的数量和评分进行调整
	// 这里暂时只返回父评分器计算的分数
	return parentScore
}

// scoreChildDocument 评分子文档
func (ns *NestedScorer) scoreChildDocument(doc *index.IndexedDocument) float64 {
	childScorer, exists := ns.childScorers[doc.Path]
	if !exists {
		// 如果没有特定路径的评分器，使用默认评分
		return ns.defaultChildScore(doc)
	}

	childScore := childScorer.Score(doc)

	// 根据scoreMode调整评分
	switch ns.scoreMode {
	case "avg":
		// 子文档评分已经是平均值
		return childScore
	case "sum":
		// 子文档评分已经是总和
		return childScore
	case "max":
		// 子文档评分已经是最大值
		return childScore
	case "min":
		// 子文档评分已经是最小值
		return childScore
	default:
		return childScore
	}
}

// defaultChildScore 默认子文档评分
func (ns *NestedScorer) defaultChildScore(doc *index.IndexedDocument) float64 {
	// 简单的默认评分逻辑
	// 可以根据文档的匹配度、重要性等因素计算

	// 基础分数
	baseScore := 1.0

	// 根据嵌套深度调整分数（越深的文档分数越低）
	depthPenalty := 1.0 / (1.0 + float64(len(doc.Path)/10))

	// 根据位置调整分数（越靠前的文档分数越高）
	positionBonus := 1.0 / (1.0 + float64(doc.Position)/10)

	return baseScore * depthPenalty * positionBonus
}

// Explain 解释评分
func (ns *NestedScorer) Explain(doc *index.IndexedDocument) *ScoreExplanation {
	explanation := NewScoreExplanation(0.0, "nested score")

	if doc.Path == "" || doc.Position == 0 {
		// 父文档评分解释
		if ns.parentScorer != nil {
			parentExplanation := ns.parentScorer.Explain(doc)
			if parentExplanation != nil {
				explanation.AddDetail(parentExplanation)
				explanation.Value += parentExplanation.Value
			}
		}
	} else {
		// 子文档评分解释
		if childScorer, exists := ns.childScorers[doc.Path]; exists {
			childExplanation := childScorer.Explain(doc)
			if childExplanation != nil {
				explanation.AddDetail(childExplanation)
				explanation.Value += childExplanation.Value
			}
		} else {
			// 默认评分解释
			defaultScore := ns.defaultChildScore(doc)
			defaultExplanation := NewScoreExplanation(defaultScore, "default child score")
			explanation.AddDetail(defaultExplanation)
			explanation.Value += defaultScore
		}
	}

	// 添加boost和queryNorm的解释
	if ns.boost != 1.0 {
		boostExplanation := NewScoreExplanation(ns.boost, "boost")
		explanation.AddDetail(boostExplanation)
		explanation.Value *= ns.boost
	}

	if ns.queryNorm != 1.0 {
		normExplanation := NewScoreExplanation(ns.queryNorm, "query norm")
		explanation.AddDetail(normExplanation)
		explanation.Value *= ns.queryNorm
	}

	explanation.Description = "nested score: " + explanation.Description
	return explanation
}

// SetBoost 设置boost值
func (ns *NestedScorer) SetBoost(boost float64) {
	ns.boost = boost
	if ns.parentScorer != nil {
		ns.parentScorer.SetBoost(boost)
	}
	for _, scorer := range ns.childScorers {
		scorer.SetBoost(boost)
	}
}

// SetQueryNorm 设置查询范数
func (ns *NestedScorer) SetQueryNorm(norm float64) {
	ns.queryNorm = norm
	if ns.parentScorer != nil {
		ns.parentScorer.SetQueryNorm(norm)
	}
	for _, scorer := range ns.childScorers {
		scorer.SetQueryNorm(norm)
	}
}

// AddChildScorer 添加子评分器
func (ns *NestedScorer) AddChildScorer(path string, scorer Scorer) {
	ns.childScorers[path] = scorer
}

// RemoveChildScorer 移除子评分器
func (ns *NestedScorer) RemoveChildScorer(path string) {
	delete(ns.childScorers, path)
}

// GetChildScorer 获取子评分器
func (ns *NestedScorer) GetChildScorer(path string) Scorer {
	return ns.childScorers[path]
}

// SetScoreMode 设置评分模式
func (ns *NestedScorer) SetScoreMode(mode string) {
	ns.scoreMode = mode
}

// getCacheKey 获取缓存键
func (ns *NestedScorer) getCacheKey(doc *index.IndexedDocument) string {
	return doc.ID + "_" + ns.scoreMode
}

// getCachedScore 获取缓存的分数
func (ns *NestedScorer) getCachedScore(key string) (float64, bool) {
	ns.cacheMutex.RLock()
	defer ns.cacheMutex.RUnlock()

	score, exists := ns.scoreCache[key]
	return score, exists
}

// setCachedScore 设置缓存的分数
func (ns *NestedScorer) setCachedScore(key string, score float64) {
	ns.cacheMutex.Lock()
	defer ns.cacheMutex.Unlock()

	ns.scoreCache[key] = score
}

// ClearCache 清空缓存
func (ns *NestedScorer) ClearCache() {
	ns.cacheMutex.Lock()
	defer ns.cacheMutex.Unlock()

	ns.scoreCache = make(map[string]float64)
}

// TFIDFScorer TF-IDF评分器实现
type TFIDFScorer struct {
	fieldWeights map[string]float64 // 字段权重
	termFreqs    map[string]float64 // 词频
	docFreqs     map[string]float64 // 文档频率
	totalDocs    int64              // 总文档数
	boost        float64
	queryNorm    float64
}

// NewTFIDFScorer 创建TF-IDF评分器
func NewTFIDFScorer() *TFIDFScorer {
	return &TFIDFScorer{
		fieldWeights: make(map[string]float64),
		termFreqs:    make(map[string]float64),
		docFreqs:     make(map[string]float64),
		totalDocs:    1, // 避免除零错误
		boost:        1.0,
		queryNorm:    1.0,
	}
}

// Score 计算TF-IDF分数
func (tfs *TFIDFScorer) Score(doc *index.IndexedDocument) float64 {
	if doc == nil || len(doc.Fields) == 0 {
		return 0.0
	}

	score := 0.0

	for fieldName, fieldValue := range doc.Fields {
		fieldWeight := tfs.fieldWeights[fieldName]
		if fieldWeight == 0 {
			fieldWeight = 1.0 // 默认权重
		}

		// 简化的TF-IDF计算
		// 实际实现应该考虑更复杂的评分算法
		fieldScore := tfs.calculateFieldScore(fieldName, fieldValue)
		score += fieldScore * fieldWeight
	}

	return score * tfs.boost * tfs.queryNorm
}

// calculateFieldScore 计算字段分数
func (tfs *TFIDFScorer) calculateFieldScore(fieldName string, fieldValue interface{}) float64 {
	// 简化的字段评分逻辑
	// 实际应该基于词频、逆文档频率等计算

	// 假设fieldValue是字符串，进行简单的长度评分
	if str, ok := fieldValue.(string); ok {
		return math.Log(float64(len(str))+1) / math.Log(float64(tfs.totalDocs)+1)
	}

	return 1.0
}

// Explain 解释TF-IDF评分
func (tfs *TFIDFScorer) Explain(doc *index.IndexedDocument) *ScoreExplanation {
	score := tfs.Score(doc)
	explanation := NewScoreExplanation(score, "tfidf score")

	for fieldName, fieldValue := range doc.Fields {
		fieldScore := tfs.calculateFieldScore(fieldName, fieldValue)
		fieldWeight := tfs.fieldWeights[fieldName]
		if fieldWeight == 0 {
			fieldWeight = 1.0
		}

		fieldExplanation := NewScoreExplanation(fieldScore*fieldWeight,
			"field '"+fieldName+"' score")
		explanation.AddDetail(fieldExplanation)
	}

	if tfs.boost != 1.0 {
		boostExplanation := NewScoreExplanation(tfs.boost, "boost")
		explanation.AddDetail(boostExplanation)
	}

	if tfs.queryNorm != 1.0 {
		normExplanation := NewScoreExplanation(tfs.queryNorm, "query norm")
		explanation.AddDetail(normExplanation)
	}

	return explanation
}

// SetBoost 设置boost值
func (tfs *TFIDFScorer) SetBoost(boost float64) {
	tfs.boost = boost
}

// SetQueryNorm 设置查询范数
func (tfs *TFIDFScorer) SetQueryNorm(norm float64) {
	tfs.queryNorm = norm
}

// SetFieldWeight 设置字段权重
func (tfs *TFIDFScorer) SetFieldWeight(fieldName string, weight float64) {
	tfs.fieldWeights[fieldName] = weight
}

// SetTotalDocs 设置总文档数
func (tfs *TFIDFScorer) SetTotalDocs(total int64) {
	tfs.totalDocs = total
}

// BM25Scorer BM25评分器实现
type BM25Scorer struct {
	k1           float64            // BM25参数k1
	b            float64            // BM25参数b
	fieldWeights map[string]float64 // 字段权重
	avgDocLen    float64            // 平均文档长度
	boost        float64
	queryNorm    float64
}

// NewBM25Scorer 创建BM25评分器
func NewBM25Scorer() *BM25Scorer {
	return &BM25Scorer{
		k1:           1.2,  // 默认k1值
		b:            0.75, // 默认b值
		fieldWeights: make(map[string]float64),
		avgDocLen:    100.0, // 默认平均文档长度
		boost:        1.0,
		queryNorm:    1.0,
	}
}

// Score 计算BM25分数
func (bms *BM25Scorer) Score(doc *index.IndexedDocument) float64 {
	if doc == nil || len(doc.Fields) == 0 {
		return 0.0
	}

	score := 0.0

	for fieldName, fieldValue := range doc.Fields {
		fieldWeight := bms.fieldWeights[fieldName]
		if fieldWeight == 0 {
			fieldWeight = 1.0
		}

		fieldScore := bms.calculateBM25Score(fieldName, fieldValue)
		score += fieldScore * fieldWeight
	}

	return score * bms.boost * bms.queryNorm
}

// calculateBM25Score 计算BM25分数
func (bms *BM25Scorer) calculateBM25Score(fieldName string, fieldValue interface{}) float64 {
	// 简化的BM25计算
	// 实际实现需要考虑词频、文档频率、文档长度等

	tf := 1.0               // 词频，简化处理
	df := 1.0               // 文档频率，简化处理
	docLen := bms.avgDocLen // 文档长度

	// BM25公式: (k1+1)*tf / (k1*(1-b+b*docLen/avgDocLen)+tf) * idf
	idf := math.Log((float64(bms.avgDocLen) + 1) / df)
	numerator := (bms.k1 + 1) * tf
	denominator := bms.k1*(1-bms.b+bms.b*docLen/bms.avgDocLen) + tf

	return (numerator / denominator) * idf
}

// Explain 解释BM25评分
func (bms *BM25Scorer) Explain(doc *index.IndexedDocument) *ScoreExplanation {
	score := bms.Score(doc)
	explanation := NewScoreExplanation(score, "bm25 score")

	for fieldName := range doc.Fields {
		fieldScore := bms.calculateBM25Score(fieldName, nil)
		fieldWeight := bms.fieldWeights[fieldName]
		if fieldWeight == 0 {
			fieldWeight = 1.0
		}

		fieldExplanation := NewScoreExplanation(fieldScore*fieldWeight,
			"field '"+fieldName+"' bm25 score")
		explanation.AddDetail(fieldExplanation)
	}

	if bms.boost != 1.0 {
		boostExplanation := NewScoreExplanation(bms.boost, "boost")
		explanation.AddDetail(boostExplanation)
	}

	if bms.queryNorm != 1.0 {
		normExplanation := NewScoreExplanation(bms.queryNorm, "query norm")
		explanation.AddDetail(normExplanation)
	}

	return explanation
}

// SetBoost 设置boost值
func (bms *BM25Scorer) SetBoost(boost float64) {
	bms.boost = boost
}

// SetQueryNorm 设置查询范数
func (bms *BM25Scorer) SetQueryNorm(norm float64) {
	bms.queryNorm = norm
}

// SetBM25Parameters 设置BM25参数
func (bms *BM25Scorer) SetBM25Parameters(k1, b float64) {
	bms.k1 = k1
	bms.b = b
}

// SetAverageDocLength 设置平均文档长度
func (bms *BM25Scorer) SetAverageDocLength(avgLen float64) {
	bms.avgDocLen = avgLen
}
