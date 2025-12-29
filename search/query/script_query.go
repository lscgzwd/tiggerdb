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

// ScriptQuery 实现基于脚本的查询过滤
type ScriptQuery struct {
	script *script.Script
	boost  float64
}

// NewScriptQuery 创建新的脚本查询
func NewScriptQuery(source string, params map[string]interface{}) *ScriptQuery {
	return &ScriptQuery{
		script: script.NewScript(source, params),
		boost:  1.0,
	}
}

// NewScriptQueryFromScript 从 Script 对象创建查询
func NewScriptQueryFromScript(s *script.Script) *ScriptQuery {
	return &ScriptQuery{
		script: s,
		boost:  1.0,
	}
}

// SetBoost 设置权重
func (q *ScriptQuery) SetBoost(b float64) {
	q.boost = b
}

// Boost 返回权重
func (q *ScriptQuery) Boost() float64 {
	return q.boost
}

// Script 返回脚本对象
func (q *ScriptQuery) Script() *script.Script {
	return q.script
}

// Searcher 实现 Query 接口
// ScriptQuery 使用 MatchAllSearcher 获取所有文档，然后通过脚本过滤
func (q *ScriptQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	// 创建 MatchAll 搜索器获取所有文档
	baseQuery := NewMatchAllQuery()
	baseSearcher, err := baseQuery.Searcher(ctx, i, m, options)
	if err != nil {
		return nil, err
	}

	// 返回脚本过滤搜索器
	return NewScriptFilterSearcher(baseSearcher, q.script, i, q.boost)
}

// ScriptFilterSearcher 脚本过滤搜索器
type ScriptFilterSearcher struct {
	base    search.Searcher
	script  *script.Script
	engine  *script.Engine
	reader  index.IndexReader
	boost   float64
	current *search.DocumentMatch
}

// NewScriptFilterSearcher 创建脚本过滤搜索器
func NewScriptFilterSearcher(base search.Searcher, s *script.Script, reader index.IndexReader, boost float64) (*ScriptFilterSearcher, error) {
	return &ScriptFilterSearcher{
		base:   base,
		script: s,
		engine: script.NewEngine(),
		reader: reader,
		boost:  boost,
	}, nil
}

// Next 返回下一个匹配的文档
func (s *ScriptFilterSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	for {
		match, err := s.base.Next(ctx)
		if err != nil {
			return nil, err
		}
		if match == nil {
			return nil, nil
		}

		// 获取文档内容
		doc, err := s.reader.Document(match.ID)
		if err != nil {
			continue
		}

		// 构建脚本上下文
		docFields := make(map[string]interface{})
		doc.VisitFields(func(field index.Field) {
			docFields[field.Name()] = string(field.Value())
		})

		scriptCtx := script.NewContext(docFields, docFields, s.script.Params)
		scriptCtx.Score = match.Score

		// 执行脚本过滤
		passed, err := s.engine.ExecuteFilter(s.script, scriptCtx)
		if err != nil {
			continue
		}

		if passed {
			match.Score = match.Score * s.boost
			return match, nil
		}
	}
}

// Advance 跳到指定文档
func (s *ScriptFilterSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	match, err := s.base.Advance(ctx, ID)
	if err != nil {
		return nil, err
	}
	if match == nil {
		return nil, nil
	}

	// 获取文档内容并执行脚本
	doc, err := s.reader.Document(match.ID)
	if err != nil {
		return s.Next(ctx)
	}

	docFields := make(map[string]interface{})
	doc.VisitFields(func(field index.Field) {
		docFields[field.Name()] = string(field.Value())
	})

	scriptCtx := script.NewContext(docFields, docFields, s.script.Params)
	scriptCtx.Score = match.Score

	passed, err := s.engine.ExecuteFilter(s.script, scriptCtx)
	if err != nil || !passed {
		return s.Next(ctx)
	}

	match.Score = match.Score * s.boost
	return match, nil
}

// Close 关闭搜索器
func (s *ScriptFilterSearcher) Close() error {
	return s.base.Close()
}

// Count 返回文档数量（近似值）
func (s *ScriptFilterSearcher) Count() uint64 {
	return s.base.Count()
}

// Min 返回最小评分
func (s *ScriptFilterSearcher) Min() int {
	return s.base.Min()
}

// DocumentMatchPoolSize 返回文档匹配池大小
func (s *ScriptFilterSearcher) DocumentMatchPoolSize() int {
	return s.base.DocumentMatchPoolSize()
}

// Weight 返回权重
func (s *ScriptFilterSearcher) Weight() float64 {
	return s.boost
}

// SetQueryNorm 设置查询规范化因子
func (s *ScriptFilterSearcher) SetQueryNorm(qnorm float64) {
	s.base.SetQueryNorm(qnorm)
}

// Size 返回大小
func (s *ScriptFilterSearcher) Size() int {
	return s.base.Size()
}
