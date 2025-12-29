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

package dsl

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/logger"
	"github.com/lscgzwd/tiggerdb/search/query"
)

// JoinQueryRegistry 注册和存储 join 查询信息
// 由于 Bleve 的 Query 接口限制，我们需要在外部存储 join 查询的元数据
var (
	joinQueryRegistry = make(map[query.Query]*JoinQueryInfo)
	joinQueryMutex    sync.RWMutex
)

// RegisterJoinQuery 注册一个 join 查询
func RegisterJoinQuery(q query.Query, info *JoinQueryInfo) {
	joinQueryMutex.Lock()
	defer joinQueryMutex.Unlock()
	joinQueryRegistry[q] = info
}

// GetJoinQueryInfo 获取 join 查询的信息
func GetJoinQueryInfo(q query.Query) *JoinQueryInfo {
	joinQueryMutex.RLock()
	defer joinQueryMutex.RUnlock()
	return joinQueryRegistry[q]
}

// UnregisterJoinQuery 注销一个 join 查询
func UnregisterJoinQuery(q query.Query) {
	joinQueryMutex.Lock()
	defer joinQueryMutex.Unlock()
	delete(joinQueryRegistry, q)
}

// FindJoinQueries 递归查找查询树中的所有 join 查询
func FindJoinQueries(q query.Query) []*JoinQueryInfo {
	var results []*JoinQueryInfo

	// 检查当前查询是否是 join 查询
	if info := GetJoinQueryInfo(q); info != nil {
		results = append(results, info)
	}

	// 递归检查子查询
	switch tq := q.(type) {
	case *query.ConjunctionQuery:
		for _, child := range tq.Conjuncts {
			results = append(results, FindJoinQueries(child)...)
		}
	case *query.DisjunctionQuery:
		for _, child := range tq.Disjuncts {
			results = append(results, FindJoinQueries(child)...)
		}
	case *query.BooleanQuery:
		if tq.Must != nil {
			results = append(results, FindJoinQueries(tq.Must)...)
		}
		if tq.Should != nil {
			results = append(results, FindJoinQueries(tq.Should)...)
		}
		if tq.MustNot != nil {
			results = append(results, FindJoinQueries(tq.MustNot)...)
		}
	}

	return results
}

// JoinQueryType 标识 join 查询的类型
type JoinQueryType int

const (
	JoinQueryTypeHasChild JoinQueryType = iota
	JoinQueryTypeHasParent
)

// JoinQueryInfo 存储 join 查询的信息
type JoinQueryInfo struct {
	Type       JoinQueryType
	TypeName   string // 子类型名（has_child）或父类型名（has_parent）
	InnerQuery query.Query
	Boost      float64
}

// PercolateQueryInfo 存储 percolate 查询的信息
type PercolateQueryInfo struct {
	Field     string                   // percolator 字段名
	Document  map[string]interface{}   // 要匹配的单个文档
	Documents []map[string]interface{} // 要匹配的多个文档
	Boost     float64
}

// percolateQueryRegistry 存储 percolate 查询信息
var (
	percolateQueryRegistry = make(map[query.Query]*PercolateQueryInfo)
	percolateQueryMutex    sync.RWMutex
)

// RegisterPercolateQuery 注册一个 percolate 查询
func RegisterPercolateQuery(q query.Query, info *PercolateQueryInfo) {
	percolateQueryMutex.Lock()
	defer percolateQueryMutex.Unlock()
	percolateQueryRegistry[q] = info
}

// GetPercolateQueryInfo 获取 percolate 查询的信息
func GetPercolateQueryInfo(q query.Query) *PercolateQueryInfo {
	percolateQueryMutex.RLock()
	defer percolateQueryMutex.RUnlock()
	return percolateQueryRegistry[q]
}

// UnregisterPercolateQuery 注销一个 percolate 查询
func UnregisterPercolateQuery(q query.Query) {
	percolateQueryMutex.Lock()
	defer percolateQueryMutex.Unlock()
	delete(percolateQueryRegistry, q)
}

// ExecuteHasChildQuery 执行 has_child 查询
// 两阶段查询：
// 1. 执行内部查询找到匹配的子文档
// 2. 获取子文档的 _join_parent 字段值（父文档ID）
// 3. 返回匹配这些父文档ID的查询
func ExecuteHasChildQuery(ctx context.Context, idx bleve.Index, childType string, innerQuery query.Query, boost float64) (query.Query, error) {
	// 第一阶段：创建子文档查询
	// 子文档的 _join_name 字段应该等于 childType
	typeQuery := query.NewTermQuery(childType)
	typeQuery.SetField("_join_name")

	// 组合：子文档类型 AND 内部查询
	childQuery := query.NewConjunctionQuery([]query.Query{typeQuery, innerQuery})

	// 执行子查询
	searchReq := bleve.NewSearchRequest(childQuery)
	searchReq.Size = 10000 // 获取足够多的子文档
	searchReq.Fields = []string{"_join_parent"}

	searchResult, err := idx.Search(searchReq)
	if err != nil {
		return nil, err
	}

	// 收集所有父文档ID
	parentIDs := make(map[string]struct{})
	for _, hit := range searchResult.Hits {
		if parentID, ok := hit.Fields["_join_parent"].(string); ok && parentID != "" {
			parentIDs[parentID] = struct{}{}
		}
	}

	logger.Debug("ExecuteHasChildQuery - Found %d matching children, %d unique parent IDs", searchResult.Total, len(parentIDs))

	// 如果没有找到任何父文档ID，返回 match_none
	if len(parentIDs) == 0 {
		return query.NewMatchNoneQuery(), nil
	}

	// 第二阶段：创建查询匹配这些父文档
	parentIDList := make([]string, 0, len(parentIDs))
	for id := range parentIDs {
		parentIDList = append(parentIDList, id)
	}

	// 使用 DocIDQuery 匹配父文档
	parentQuery := query.NewDocIDQuery(parentIDList)
	parentQuery.SetBoost(boost)

	return parentQuery, nil
}

// ExecuteHasParentQuery 执行 has_parent 查询
// 两阶段查询：
// 1. 执行内部查询找到匹配的父文档
// 2. 获取父文档ID
// 3. 返回匹配 _join_parent 等于这些ID的子文档查询
func ExecuteHasParentQuery(ctx context.Context, idx bleve.Index, parentType string, innerQuery query.Query, boost float64) (query.Query, error) {
	// 第一阶段：创建父文档查询
	// 父文档的 _join_name 字段应该等于 parentType
	typeQuery := query.NewTermQuery(parentType)
	typeQuery.SetField("_join_name")

	// 组合：父文档类型 AND 内部查询
	parentQuery := query.NewConjunctionQuery([]query.Query{typeQuery, innerQuery})

	// 执行父查询
	searchReq := bleve.NewSearchRequest(parentQuery)
	searchReq.Size = 10000 // 获取足够多的父文档

	searchResult, err := idx.Search(searchReq)
	if err != nil {
		return nil, err
	}

	// 收集所有父文档ID
	parentIDs := make([]string, 0, len(searchResult.Hits))
	for _, hit := range searchResult.Hits {
		parentIDs = append(parentIDs, hit.ID)
	}

	logger.Debug("ExecuteHasParentQuery - Found %d matching parents", len(parentIDs))

	// 如果没有找到任何父文档，返回 match_none
	if len(parentIDs) == 0 {
		return query.NewMatchNoneQuery(), nil
	}

	// 第二阶段：创建查询匹配这些父文档的子文档
	// 子文档的 _join_parent 字段应该等于某个父文档ID
	var childQueries []query.Query
	for _, parentID := range parentIDs {
		termQuery := query.NewTermQuery(parentID)
		termQuery.SetField("_join_parent")
		childQueries = append(childQueries, termQuery)
	}

	// 使用 DisjunctionQuery（OR）组合所有子查询
	if len(childQueries) == 1 {
		return childQueries[0], nil
	}

	disjQuery := query.NewDisjunctionQuery(childQueries)
	disjQuery.SetMin(1)
	disjQuery.SetBoost(boost)

	return disjQuery, nil
}

// ExecutePercolateQuery 执行 percolate 查询
// 两阶段查询：
// 1. 从索引中获取所有包含 percolator 字段的文档（存储的查询）
// 2. 对每个存储的查询，检查提供的文档是否匹配
// 3. 返回匹配的查询文档ID
func ExecutePercolateQuery(ctx context.Context, idx bleve.Index, info *PercolateQueryInfo) (query.Query, error) {
	// 第一阶段：获取所有包含 percolator 字段的文档
	// 这些文档的 _percolator_query 字段存储了序列化的查询
	percolatorFieldQuery := query.NewTermQuery("true")
	percolatorFieldQuery.SetField("_has_percolator")

	searchReq := bleve.NewSearchRequest(percolatorFieldQuery)
	searchReq.Size = 10000 // 获取所有存储的查询
	searchReq.Fields = []string{"_percolator_query"}

	searchResult, err := idx.Search(searchReq)
	if err != nil {
		return nil, err
	}

	logger.Debug("ExecutePercolateQuery - Found %d stored queries", searchResult.Total)

	// 如果没有存储的查询，返回 match_none
	if searchResult.Total == 0 {
		return query.NewMatchNoneQuery(), nil
	}

	// 准备要匹配的文档
	var documents []map[string]interface{}
	if info.Document != nil {
		documents = append(documents, info.Document)
	}
	documents = append(documents, info.Documents...)

	if len(documents) == 0 {
		return query.NewMatchNoneQuery(), nil
	}

	// 第二阶段：对每个存储的查询，检查文档是否匹配
	matchedQueryIDs := make([]string, 0)

	for _, hit := range searchResult.Hits {
		// 获取存储的查询 JSON
		queryJSON, ok := hit.Fields["_percolator_query"].(string)
		if !ok || queryJSON == "" {
			continue
		}

		// 解析查询
		var queryMap map[string]interface{}
		if err := json.Unmarshal([]byte(queryJSON), &queryMap); err != nil {
			logger.Warn("Failed to parse stored query in document %s: %v", hit.ID, err)
			continue
		}

		// 创建查询解析器并解析存储的查询
		parser := NewQueryParser()
		storedQuery, err := parser.ParseQuery(queryMap)
		if err != nil {
			logger.Warn("Failed to create query from stored query in document %s: %v", hit.ID, err)
			continue
		}

		// 检查每个文档是否匹配存储的查询
		for _, doc := range documents {
			if matchDocumentAgainstQuery(idx, doc, storedQuery) {
				matchedQueryIDs = append(matchedQueryIDs, hit.ID)
				break // 文档匹配了这个查询，不需要继续检查其他文档
			}
		}
	}

	logger.Debug("ExecutePercolateQuery - %d queries matched", len(matchedQueryIDs))

	// 如果没有匹配的查询，返回 match_none
	if len(matchedQueryIDs) == 0 {
		return query.NewMatchNoneQuery(), nil
	}

	// 返回匹配的查询文档ID
	resultQuery := query.NewDocIDQuery(matchedQueryIDs)
	resultQuery.SetBoost(info.Boost)

	return resultQuery, nil
}

// matchDocumentAgainstQuery 检查文档是否匹配查询
// 这是一个简化的实现，通过临时索引文档并执行查询来检查匹配
func matchDocumentAgainstQuery(idx bleve.Index, doc map[string]interface{}, q query.Query) bool {
	// 简化实现：将文档字段与查询条件进行匹配
	// 真正的实现需要创建临时索引或使用内存匹配

	// 对于简单的 term/match 查询，我们可以直接检查字段值
	switch tq := q.(type) {
	case *query.TermQuery:
		fieldValue, ok := doc[tq.FieldVal]
		if !ok {
			return false
		}
		return matchFieldValue(fieldValue, tq.Term)

	case *query.MatchQuery:
		fieldValue, ok := doc[tq.FieldVal]
		if !ok {
			return false
		}
		return matchFieldValue(fieldValue, tq.Match)

	case *query.MatchAllQuery:
		return true

	case *query.MatchNoneQuery:
		return false

	case *query.ConjunctionQuery:
		// AND 查询：所有子查询都必须匹配
		for _, child := range tq.Conjuncts {
			if !matchDocumentAgainstQuery(idx, doc, child) {
				return false
			}
		}
		return true

	case *query.DisjunctionQuery:
		// OR 查询：至少一个子查询匹配
		for _, child := range tq.Disjuncts {
			if matchDocumentAgainstQuery(idx, doc, child) {
				return true
			}
		}
		return false

	case *query.BooleanQuery:
		// Must 条件
		if tq.Must != nil && !matchDocumentAgainstQuery(idx, doc, tq.Must) {
			return false
		}
		// Should 条件（至少一个）
		if tq.Should != nil {
			if disjQuery, ok := tq.Should.(*query.DisjunctionQuery); ok {
				matched := false
				for _, child := range disjQuery.Disjuncts {
					if matchDocumentAgainstQuery(idx, doc, child) {
						matched = true
						break
					}
				}
				if !matched {
					return false
				}
			}
		}
		// MustNot 条件
		if tq.MustNot != nil && matchDocumentAgainstQuery(idx, doc, tq.MustNot) {
			return false
		}
		return true

	default:
		// 对于复杂查询，默认返回 true（简化实现）
		logger.Debug("matchDocumentAgainstQuery - Unsupported query type: %T, assuming match", q)
		return true
	}
}

// matchFieldValue 检查字段值是否匹配查询值
func matchFieldValue(fieldValue interface{}, queryValue string) bool {
	switch v := fieldValue.(type) {
	case string:
		return strings.Contains(strings.ToLower(v), strings.ToLower(queryValue))
	case float64:
		return fmt.Sprintf("%v", v) == queryValue
	case bool:
		return fmt.Sprintf("%v", v) == queryValue
	case []interface{}:
		for _, item := range v {
			if matchFieldValue(item, queryValue) {
				return true
			}
		}
		return false
	default:
		return fmt.Sprintf("%v", v) == queryValue
	}
}
