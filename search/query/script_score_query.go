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

	index "github.com/blevesearch/bleve_index_api"
	"github.com/lscgzwd/tiggerdb/mapping"
	"github.com/lscgzwd/tiggerdb/script"
	"github.com/lscgzwd/tiggerdb/search"
)

// ScriptScoreQuery 实现基于脚本的评分查询
// ES格式: {"script_score": {"query": {...}, "script": {...}}}
type ScriptScoreQuery struct {
	innerQuery Query          // 内部查询
	script     *script.Script // 评分脚本
	boost      float64        // 权重
	minScore   float64        // 最小分数阈值
}

// NewScriptScoreQuery 创建新的脚本评分查询
func NewScriptScoreQuery(innerQuery Query, s *script.Script) *ScriptScoreQuery {
	return &ScriptScoreQuery{
		innerQuery: innerQuery,
		script:     s,
		boost:      1.0,
	}
}

// SetBoost 设置权重
func (q *ScriptScoreQuery) SetBoost(b float64) {
	q.boost = b
}

// Boost 返回权重
func (q *ScriptScoreQuery) Boost() float64 {
	return q.boost
}

// SetMinScore 设置最小分数阈值
func (q *ScriptScoreQuery) SetMinScore(minScore float64) {
	q.minScore = minScore
}

// InnerQuery 返回内部查询
func (q *ScriptScoreQuery) InnerQuery() Query {
	return q.innerQuery
}

// Script 返回脚本对象
func (q *ScriptScoreQuery) Script() *script.Script {
	return q.script
}

// Searcher 实现 Query 接口
func (q *ScriptScoreQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	// 获取内部查询的搜索器
	innerSearcher, err := q.innerQuery.Searcher(ctx, i, m, options)
	if err != nil {
		return nil, err
	}

	// 返回脚本评分搜索器
	return NewScriptScoreSearcher(innerSearcher, q.script, i, q.boost, q.minScore)
}

// ScriptScoreSearcher 脚本评分搜索器
type ScriptScoreSearcher struct {
	inner    search.Searcher
	script   *script.Script
	engine   *script.Engine
	reader   index.IndexReader
	boost    float64
	minScore float64
}

// NewScriptScoreSearcher 创建脚本评分搜索器
func NewScriptScoreSearcher(inner search.Searcher, s *script.Script, reader index.IndexReader, boost, minScore float64) (*ScriptScoreSearcher, error) {
	return &ScriptScoreSearcher{
		inner:    inner,
		script:   s,
		engine:   script.NewEngine(),
		reader:   reader,
		boost:    boost,
		minScore: minScore,
	}, nil
}

// Next 返回下一个匹配的文档，使用脚本计算评分
func (s *ScriptScoreSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	for {
		match, err := s.inner.Next(ctx)
		if err != nil {
			return nil, err
		}
		if match == nil {
			return nil, nil
		}

		// 获取文档内容
		doc, err := s.reader.Document(match.ID)
		if err != nil {
			// 文档获取失败，使用原始评分
			match.Score = match.Score * s.boost
			if s.minScore > 0 && match.Score < s.minScore {
				continue
			}
			return match, nil
		}

		// 构建脚本上下文
		docFields := make(map[string]interface{})
		doc.VisitFields(func(field index.Field) {
			docFields[field.Name()] = string(field.Value())
		})

		scriptCtx := script.NewContext(docFields, docFields, s.script.Params)
		scriptCtx.Score = match.Score

		// 执行脚本计算新评分
		newScore, err := s.engine.ExecuteScore(s.script, scriptCtx)
		if err != nil {
			// 脚本执行失败，使用原始评分
			match.Score = match.Score * s.boost
		} else {
			match.Score = newScore * s.boost
		}

		// 检查最小分数阈值
		if s.minScore > 0 && match.Score < s.minScore {
			continue
		}

		return match, nil
	}
}

// Advance 跳到指定文档
func (s *ScriptScoreSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	match, err := s.inner.Advance(ctx, ID)
	if err != nil {
		return nil, err
	}
	if match == nil {
		return nil, nil
	}

	// 获取文档并计算评分
	doc, err := s.reader.Document(match.ID)
	if err != nil {
		match.Score = match.Score * s.boost
		return match, nil
	}

	docFields := make(map[string]interface{})
	doc.VisitFields(func(field index.Field) {
		docFields[field.Name()] = string(field.Value())
	})

	scriptCtx := script.NewContext(docFields, docFields, s.script.Params)
	scriptCtx.Score = match.Score

	newScore, err := s.engine.ExecuteScore(s.script, scriptCtx)
	if err != nil {
		match.Score = match.Score * s.boost
	} else {
		match.Score = newScore * s.boost
	}

	return match, nil
}

// Close 关闭搜索器
func (s *ScriptScoreSearcher) Close() error {
	return s.inner.Close()
}

// Count 返回文档数量
func (s *ScriptScoreSearcher) Count() uint64 {
	return s.inner.Count()
}

// Min 返回最小评分
func (s *ScriptScoreSearcher) Min() int {
	return s.inner.Min()
}

// DocumentMatchPoolSize 返回文档匹配池大小
func (s *ScriptScoreSearcher) DocumentMatchPoolSize() int {
	return s.inner.DocumentMatchPoolSize()
}

// Weight 返回权重
func (s *ScriptScoreSearcher) Weight() float64 {
	return s.boost
}

// SetQueryNorm 设置查询规范化因子
func (s *ScriptScoreSearcher) SetQueryNorm(qnorm float64) {
	s.inner.SetQueryNorm(qnorm)
}

// Size 返回大小
func (s *ScriptScoreSearcher) Size() int {
	return s.inner.Size()
}
