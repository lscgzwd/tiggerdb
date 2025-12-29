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
	"context"
	"fmt"

	index "github.com/blevesearch/bleve_index_api"
	"github.com/lscgzwd/tiggerdb/mapping"
	"github.com/lscgzwd/tiggerdb/search"
)

// HasChildQuery 实现 ES 的 has_child 查询
// 返回有匹配子文档的父文档
type HasChildQuery struct {
	childType  string  // 子文档类型
	innerQuery Query   // 内部查询（匹配子文档的条件）
	boost      float64 // 权重
}

// NewHasChildQuery 创建一个新的 has_child 查询
func NewHasChildQuery(childType string, innerQuery Query) *HasChildQuery {
	return &HasChildQuery{
		childType:  childType,
		innerQuery: innerQuery,
		boost:      1.0,
	}
}

// SetBoost 设置权重
func (q *HasChildQuery) SetBoost(b float64) {
	q.boost = b
}

// Boost 返回权重
func (q *HasChildQuery) Boost() float64 {
	return q.boost
}

// ChildType 返回子文档类型
func (q *HasChildQuery) ChildType() string {
	return q.childType
}

// InnerQuery 返回内部查询
func (q *HasChildQuery) InnerQuery() Query {
	return q.innerQuery
}

// Searcher 实现 Query 接口
// 两阶段查询：
// 1. 执行内部查询找到匹配的子文档
// 2. 获取子文档的 _join_parent 字段值（父文档ID）
// 3. 返回这些父文档
func (q *HasChildQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	// 第一阶段：创建子文档类型过滤查询
	// 子文档的 _join_name 字段应该等于 childType
	typeQuery := NewTermQuery(q.childType)
	typeQuery.SetField("_join_name")

	// 组合：子文档类型 AND 内部查询
	childQuery := NewConjunctionQuery([]Query{typeQuery, q.innerQuery})

	// 执行子查询获取匹配的子文档
	childSearcher, err := childQuery.Searcher(ctx, i, m, options)
	if err != nil {
		return nil, fmt.Errorf("failed to create child searcher: %w", err)
	}

	// 收集所有匹配子文档的父文档ID
	parentIDs := make(map[string]struct{})
	searchCtx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(childSearcher.DocumentMatchPoolSize(), 0),
	}
	for {
		match, err := childSearcher.Next(searchCtx)
		if err != nil {
			childSearcher.Close()
			return nil, fmt.Errorf("failed to get next child match: %w", err)
		}
		if match == nil {
			break
		}

		// 获取子文档的 _join_parent 字段值
		doc, err := i.Document(match.ID)
		if err != nil {
			continue
		}

		// 从文档中提取 _join_parent 字段
		doc.VisitFields(func(field index.Field) {
			if field.Name() == "_join_parent" {
				parentIDs[string(field.Value())] = struct{}{}
			}
		})
	}
	childSearcher.Close()

	// 如果没有找到任何父文档ID，返回空结果
	if len(parentIDs) == 0 {
		return NewMatchNoneQuery().Searcher(ctx, i, m, options)
	}

	// 第二阶段：创建查询匹配这些父文档
	parentIDList := make([]string, 0, len(parentIDs))
	for id := range parentIDs {
		parentIDList = append(parentIDList, id)
	}

	// 使用 DocIDQuery 匹配父文档
	parentQuery := NewDocIDQuery(parentIDList)
	parentQuery.SetBoost(q.boost)

	return parentQuery.Searcher(ctx, i, m, options)
}
