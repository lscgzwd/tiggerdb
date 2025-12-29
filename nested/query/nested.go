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

package query

import (
	"fmt"
	"reflect"

	"github.com/lscgzwd/tiggerdb/nested/index"
)

// NestedQuery 嵌套查询实现
type NestedQuery struct {
	Path      string      `json:"path"`
	Query     interface{} `json:"query"`
	ScoreMode string      `json:"score_mode,omitempty"`
	InnerHits *InnerHits  `json:"inner_hits,omitempty"`
}

// InnerHits 内部命中配置
type InnerHits struct {
	Name   string   `json:"name,omitempty"`
	Size   int      `json:"size,omitempty"`
	From   int      `json:"from,omitempty"`
	Sort   []string `json:"sort,omitempty"`
	Source []string `json:"_source,omitempty"`
}

// NewNestedQuery 创建新的嵌套查询
func NewNestedQuery(path string, query interface{}) *NestedQuery {
	return &NestedQuery{
		Path:      path,
		Query:     query,
		ScoreMode: "avg", // 默认使用平均分数
	}
}

// Validate 验证嵌套查询
func (nq *NestedQuery) Validate() error {
	if nq.Path == "" {
		return fmt.Errorf("nested query path cannot be empty")
	}

	if nq.Query == nil {
		return fmt.Errorf("nested query cannot be nil")
	}

	// 验证score_mode
	validScoreModes := []string{"avg", "sum", "max", "min", "none"}
	if nq.ScoreMode != "" {
		valid := false
		for _, mode := range validScoreModes {
			if nq.ScoreMode == mode {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("invalid score_mode: %s", nq.ScoreMode)
		}
	}

	// 验证inner_hits
	if nq.InnerHits != nil {
		if err := nq.InnerHits.Validate(); err != nil {
			return fmt.Errorf("invalid inner_hits: %w", err)
		}
	}

	return nil
}

// Execute 执行嵌套查询
func (nq *NestedQuery) Execute(strategy index.IndexStrategy) ([]*index.QueryResult, error) {
	if err := nq.Validate(); err != nil {
		return nil, fmt.Errorf("invalid nested query: %w", err)
	}

	// 转换为索引层的查询
	indexQuery := &index.NestedQuery{
		Path:      nq.Path,
		Query:     nq.Query,
		ScoreMode: nq.ScoreMode,
	}

	if nq.InnerHits != nil {
		indexQuery.InnerHits = &index.InnerHits{
			Name: nq.InnerHits.Name,
			Size: nq.InnerHits.Size,
			From: nq.InnerHits.From,
			Sort: nq.InnerHits.Sort,
		}
	}

	// 执行查询
	results, err := strategy.Query(indexQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to execute nested query: %w", err)
	}

	// 应用分数模式
	results = index.MergeResults(results, nq.ScoreMode)

	// 应用inner_hits限制
	if nq.InnerHits != nil {
		results = nq.applyInnerHits(results)
	}

	return results, nil
}

// applyInnerHits 应用inner_hits限制
func (nq *NestedQuery) applyInnerHits(results []*index.QueryResult) []*index.QueryResult {
	if nq.InnerHits == nil {
		return results
	}

	innerHits := nq.InnerHits

	// 应用from
	if innerHits.From > 0 && innerHits.From < len(results) {
		results = results[innerHits.From:]
	}

	// 应用size
	if innerHits.Size > 0 && innerHits.Size < len(results) {
		results = results[:innerHits.Size]
	}

	// 这里可以添加排序逻辑
	// if len(innerHits.Sort) > 0 {
	//     // 实现排序
	// }

	return results
}

// HasChildQuery Has Child查询
type HasChildQuery struct {
	Type        string      `json:"type"`
	Query       interface{} `json:"query"`
	ScoreMode   string      `json:"score_mode,omitempty"`
	InnerHits   *InnerHits  `json:"inner_hits,omitempty"`
	MaxChildren int         `json:"max_children,omitempty"`
	MinChildren int         `json:"min_children,omitempty"`
}

// NewHasChildQuery 创建Has Child查询
func NewHasChildQuery(childType string, query interface{}) *HasChildQuery {
	return &HasChildQuery{
		Type:        childType,
		Query:       query,
		ScoreMode:   "avg",
		MaxChildren: -1, // -1表示无限大
		MinChildren: 0,
	}
}

// Validate 验证Has Child查询
func (hcq *HasChildQuery) Validate() error {
	if hcq.Type == "" {
		return fmt.Errorf("has_child query type cannot be empty")
	}

	if hcq.Query == nil {
		return fmt.Errorf("has_child query cannot be nil")
	}

	if hcq.MinChildren < 0 {
		return fmt.Errorf("min_children cannot be negative")
	}

	if hcq.MaxChildren >= 0 && hcq.MaxChildren < hcq.MinChildren {
		return fmt.Errorf("max_children cannot be less than min_children")
	}

	return nil
}

// Execute 执行Has Child查询
func (hcq *HasChildQuery) Execute(strategy index.IndexStrategy) ([]*index.QueryResult, error) {
	if err := hcq.Validate(); err != nil {
		return nil, fmt.Errorf("invalid has_child query: %w", err)
	}

	// 查找具有匹配子文档的父文档
	// 这需要策略支持更复杂的查询
	// 临时实现：执行嵌套查询并返回父文档
	nestedQuery := &NestedQuery{
		Path:      hcq.Type, // 使用type作为path
		Query:     hcq.Query,
		ScoreMode: hcq.ScoreMode,
		InnerHits: hcq.InnerHits,
	}

	results, err := nestedQuery.Execute(strategy)
	if err != nil {
		return nil, err
	}

	// 转换为父文档结果
	var parentResults []*index.QueryResult
	parentMap := make(map[string]*index.QueryResult)

	for _, result := range results {
		parentID := result.Document.ParentID

		if existing, exists := parentMap[parentID]; exists {
			// 合并分数
			existing.Score = (existing.Score + result.Score) / 2
		} else {
			// 创建新的父文档结果
			parentResult := &index.QueryResult{
				Document: &index.IndexedDocument{
					ID:       parentID,
					ParentID: parentID,
					Path:     "",
					Position: 0,
					Fields:   make(map[string]interface{}),
				},
				Score: result.Score,
			}
			parentMap[parentID] = parentResult
			parentResults = append(parentResults, parentResult)
		}
	}

	return parentResults, nil
}

// HasParentQuery Has Parent查询
type HasParentQuery struct {
	ParentType string      `json:"parent_type"`
	Query      interface{} `json:"query"`
	Score      bool        `json:"score,omitempty"`
	InnerHits  *InnerHits  `json:"inner_hits,omitempty"`
}

// NewHasParentQuery 创建Has Parent查询
func NewHasParentQuery(parentType string, query interface{}) *HasParentQuery {
	return &HasParentQuery{
		ParentType: parentType,
		Query:      query,
		Score:      true,
	}
}

// Validate 验证Has Parent查询
func (hpq *HasParentQuery) Validate() error {
	if hpq.ParentType == "" {
		return fmt.Errorf("has_parent query parent_type cannot be empty")
	}

	if hpq.Query == nil {
		return fmt.Errorf("has_parent query cannot be nil")
	}

	return nil
}

// Execute 执行Has Parent查询
func (hpq *HasParentQuery) Execute(strategy index.IndexStrategy) ([]*index.QueryResult, error) {
	if err := hpq.Validate(); err != nil {
		return nil, fmt.Errorf("invalid has_parent query: %w", err)
	}

	// 这里需要实现查找具有匹配父文档的子文档的逻辑
	// 临时实现返回空结果
	return []*index.QueryResult{}, nil
}

// Validate 验证InnerHits
func (ih *InnerHits) Validate() error {
	if ih.Size < 0 {
		return fmt.Errorf("inner_hits size cannot be negative")
	}

	if ih.From < 0 {
		return fmt.Errorf("inner_hits from cannot be negative")
	}

	if ih.Size > 10000 {
		return fmt.Errorf("inner_hits size cannot exceed 10000")
	}

	return nil
}

// QueryBuilder 查询构建器
type QueryBuilder struct{}

// NewQueryBuilder 创建查询构建器
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

// BuildNestedQuery 构建嵌套查询
func (qb *QueryBuilder) BuildNestedQuery(path string, query interface{}, options map[string]interface{}) (*NestedQuery, error) {
	nestedQuery := NewNestedQuery(path, query)

	// 应用选项
	if scoreMode, ok := options["score_mode"].(string); ok {
		nestedQuery.ScoreMode = scoreMode
	}

	if innerHits, ok := options["inner_hits"].(map[string]interface{}); ok {
		nestedQuery.InnerHits = qb.buildInnerHits(innerHits)
	}

	if err := nestedQuery.Validate(); err != nil {
		return nil, err
	}

	return nestedQuery, nil
}

// BuildHasChildQuery 构建Has Child查询
func (qb *QueryBuilder) BuildHasChildQuery(childType string, query interface{}, options map[string]interface{}) (*HasChildQuery, error) {
	hasChildQuery := NewHasChildQuery(childType, query)

	// 应用选项
	if scoreMode, ok := options["score_mode"].(string); ok {
		hasChildQuery.ScoreMode = scoreMode
	}

	if innerHits, ok := options["inner_hits"].(map[string]interface{}); ok {
		hasChildQuery.InnerHits = qb.buildInnerHits(innerHits)
	}

	if maxChildren, ok := options["max_children"].(int); ok {
		hasChildQuery.MaxChildren = maxChildren
	}

	if minChildren, ok := options["min_children"].(int); ok {
		hasChildQuery.MinChildren = minChildren
	}

	if err := hasChildQuery.Validate(); err != nil {
		return nil, err
	}

	return hasChildQuery, nil
}

// BuildHasParentQuery 构建Has Parent查询
func (qb *QueryBuilder) BuildHasParentQuery(parentType string, query interface{}, options map[string]interface{}) (*HasParentQuery, error) {
	hasParentQuery := NewHasParentQuery(parentType, query)

	// 应用选项
	if score, ok := options["score"].(bool); ok {
		hasParentQuery.Score = score
	}

	if innerHits, ok := options["inner_hits"].(map[string]interface{}); ok {
		hasParentQuery.InnerHits = qb.buildInnerHits(innerHits)
	}

	if err := hasParentQuery.Validate(); err != nil {
		return nil, err
	}

	return hasParentQuery, nil
}

// buildInnerHits 构建InnerHits
func (qb *QueryBuilder) buildInnerHits(innerHitsMap map[string]interface{}) *InnerHits {
	innerHits := &InnerHits{}

	if name, ok := innerHitsMap["name"].(string); ok {
		innerHits.Name = name
	}

	if size, ok := innerHitsMap["size"].(float64); ok {
		innerHits.Size = int(size)
	}

	if from, ok := innerHitsMap["from"].(float64); ok {
		innerHits.From = int(from)
	}

	if sort, ok := innerHitsMap["sort"].([]interface{}); ok {
		innerHits.Sort = make([]string, len(sort))
		for i, s := range sort {
			if str, ok := s.(string); ok {
				innerHits.Sort[i] = str
			}
		}
	}

	if source, ok := innerHitsMap["_source"].([]interface{}); ok {
		innerHits.Source = make([]string, len(source))
		for i, s := range source {
			if str, ok := s.(string); ok {
				innerHits.Source[i] = str
			}
		}
	}

	return innerHits
}

// QueryExecutor 查询执行器
type QueryExecutor struct {
	strategy index.IndexStrategy
	builder  *QueryBuilder
}

// NewQueryExecutor 创建查询执行器
func NewQueryExecutor(strategy index.IndexStrategy) *QueryExecutor {
	return &QueryExecutor{
		strategy: strategy,
		builder:  NewQueryBuilder(),
	}
}

// ExecuteNestedQuery 执行嵌套查询
func (qe *QueryExecutor) ExecuteNestedQuery(path string, query interface{}, options map[string]interface{}) ([]*index.QueryResult, error) {
	nestedQuery, err := qe.builder.BuildNestedQuery(path, query, options)
	if err != nil {
		return nil, err
	}

	return nestedQuery.Execute(qe.strategy)
}

// ExecuteHasChildQuery 执行Has Child查询
func (qe *QueryExecutor) ExecuteHasChildQuery(childType string, query interface{}, options map[string]interface{}) ([]*index.QueryResult, error) {
	hasChildQuery, err := qe.builder.BuildHasChildQuery(childType, query, options)
	if err != nil {
		return nil, err
	}

	return hasChildQuery.Execute(qe.strategy)
}

// ExecuteHasParentQuery 执行Has Parent查询
func (qe *QueryExecutor) ExecuteHasParentQuery(parentType string, query interface{}, options map[string]interface{}) ([]*index.QueryResult, error) {
	hasParentQuery, err := qe.builder.BuildHasParentQuery(parentType, query, options)
	if err != nil {
		return nil, err
	}

	return hasParentQuery.Execute(qe.strategy)
}

// ParseQuery 解析查询
func (qe *QueryExecutor) ParseQuery(queryMap map[string]interface{}) (interface{}, error) {
	// 识别查询类型
	if nested, ok := queryMap["nested"].(map[string]interface{}); ok {
		path, _ := nested["path"].(string)
		subQuery, _ := nested["query"]
		scoreMode, _ := nested["score_mode"].(string)

		options := map[string]interface{}{
			"score_mode": scoreMode,
		}

		if innerHits, ok := nested["inner_hits"].(map[string]interface{}); ok {
			options["inner_hits"] = innerHits
		}

		return qe.builder.BuildNestedQuery(path, subQuery, options)
	}

	if hasChild, ok := queryMap["has_child"].(map[string]interface{}); ok {
		childType, _ := hasChild["type"].(string)
		query, _ := hasChild["query"]

		options := make(map[string]interface{})
		if scoreMode, ok := hasChild["score_mode"].(string); ok {
			options["score_mode"] = scoreMode
		}
		if innerHits, ok := hasChild["inner_hits"].(map[string]interface{}); ok {
			options["inner_hits"] = innerHits
		}

		return qe.builder.BuildHasChildQuery(childType, query, options)
	}

	if hasParent, ok := queryMap["has_parent"].(map[string]interface{}); ok {
		parentType, _ := hasParent["parent_type"].(string)
		query, _ := hasParent["query"]

		options := make(map[string]interface{})
		if score, ok := hasParent["score"].(bool); ok {
			options["score"] = score
		}
		if innerHits, ok := hasParent["inner_hits"].(map[string]interface{}); ok {
			options["inner_hits"] = innerHits
		}

		return qe.builder.BuildHasParentQuery(parentType, query, options)
	}

	return nil, fmt.Errorf("unsupported query type: %v", reflect.TypeOf(queryMap))
}
