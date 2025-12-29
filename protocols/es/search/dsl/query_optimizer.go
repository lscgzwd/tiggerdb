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
	"github.com/lscgzwd/tiggerdb/search/query"
)

// QueryOptimizer 查询优化器
// 保守实现，只做明显有益的优化，避免负优化
type QueryOptimizer struct {
	enabled bool // 是否启用优化器（默认启用，但可以关闭）
}

// NewQueryOptimizer 创建查询优化器
func NewQueryOptimizer() *QueryOptimizer {
	return &QueryOptimizer{
		enabled: true, // 默认启用
	}
}

// SetEnabled 设置是否启用优化器
func (o *QueryOptimizer) SetEnabled(enabled bool) {
	o.enabled = enabled
}

// Optimize 优化查询
// 返回优化后的查询，如果不需要优化则返回原查询
func (o *QueryOptimizer) Optimize(q query.Query) (query.Query, error) {
	if !o.enabled {
		return q, nil
	}

	if q == nil {
		return q, nil
	}

	// 根据查询类型进行优化
	switch v := q.(type) {
	case *query.BooleanQuery:
		return o.optimizeBooleanQuery(v)
	case *query.ConjunctionQuery:
		return o.optimizeConjunctionQuery(v)
	case *query.DisjunctionQuery:
		return o.optimizeDisjunctionQuery(v)
	default:
		// 其他查询类型暂不优化
		return q, nil
	}
}

// optimizeBooleanQuery 优化Boolean查询
// 优化策略：
// 1. 如果must是match_all，可以移除（但保留filter）
// 2. 优化should子句的顺序（将高选择性查询放在前面）
func (o *QueryOptimizer) optimizeBooleanQuery(bq *query.BooleanQuery) (query.Query, error) {
	// 获取must、should、mustNot
	var mustQueries []query.Query
	var shouldQueries []query.Query
	var mustNotQueries []query.Query

	if bq.Must != nil {
		if conj, ok := bq.Must.(*query.ConjunctionQuery); ok {
			mustQueries = conj.Conjuncts
		} else {
			mustQueries = []query.Query{bq.Must}
		}
	}

	if bq.Should != nil {
		if disj, ok := bq.Should.(*query.DisjunctionQuery); ok {
			shouldQueries = disj.Disjuncts
		} else {
			shouldQueries = []query.Query{bq.Should}
		}
	}

	if bq.MustNot != nil {
		if disj, ok := bq.MustNot.(*query.DisjunctionQuery); ok {
			mustNotQueries = disj.Disjuncts
		} else {
			mustNotQueries = []query.Query{bq.MustNot}
		}
	}

	// 优化1: 移除must中的match_all（如果只有match_all且没有其他must条件）
	// 注意：如果must只有match_all，可以移除，但如果有其他must条件，match_all可以忽略
	optimizedMust := make([]query.Query, 0, len(mustQueries))
	for _, mq := range mustQueries {
		if _, isMatchAll := mq.(*query.MatchAllQuery); !isMatchAll {
			optimizedMust = append(optimizedMust, mq)
		}
		// 如果是match_all，跳过（不添加到optimizedMust）
	}

	// 优化2: 优化should子句的顺序
	// 将高选择性查询（term/terms）放在前面，低选择性查询（match）放在后面
	optimizedShould := o.optimizeShouldOrder(shouldQueries)

	// 优化3: 合并相同字段的term查询为terms查询（在should中）
	optimizedShould = o.mergeTermQueries(optimizedShould)

	// 如果优化后没有must，但有should，且should只有一个，可以简化为should
	// 但这是语义改变，不进行此优化

	// 如果所有查询都被优化掉了，返回match_all
	if len(optimizedMust) == 0 && len(optimizedShould) == 0 && len(mustNotQueries) == 0 {
		return query.NewMatchAllQuery(), nil
	}

	// 如果只有must且只有一个，可以返回must本身（但这是语义改变，不进行此优化）

	// 重新构建BooleanQuery
	// NewBooleanQuery需要[]Query，直接传入优化后的数组
	finalMustQueries := optimizedMust
	finalShouldQueries := optimizedShould
	finalMustNotQueries := mustNotQueries

	// 如果优化后和原查询相同，返回原查询（避免不必要的对象创建）
	// 简化比较：只比较数量（避免深度比较的开销）
	originalMustCount := 0
	if bq.Must != nil {
		if conj, ok := bq.Must.(*query.ConjunctionQuery); ok {
			originalMustCount = len(conj.Conjuncts)
		} else {
			originalMustCount = 1
		}
	}
	originalShouldCount := 0
	if bq.Should != nil {
		if disj, ok := bq.Should.(*query.DisjunctionQuery); ok {
			originalShouldCount = len(disj.Disjuncts)
		} else {
			originalShouldCount = 1
		}
	}
	originalMustNotCount := 0
	if bq.MustNot != nil {
		if disj, ok := bq.MustNot.(*query.DisjunctionQuery); ok {
			originalMustNotCount = len(disj.Disjuncts)
		} else {
			originalMustNotCount = 1
		}
	}

	if len(finalMustQueries) == originalMustCount &&
		len(finalShouldQueries) == originalShouldCount &&
		len(finalMustNotQueries) == originalMustNotCount {
		// 数量相同，假设内容相同（避免深度比较的开销）
		return bq, nil
	}

	return query.NewBooleanQuery(finalMustQueries, finalShouldQueries, finalMustNotQueries), nil
}

// optimizeShouldOrder 优化should子句的顺序
// 策略：将高选择性查询放在前面
// 选择性从高到低：term > terms > range > match > match_phrase > match_all
func (o *QueryOptimizer) optimizeShouldOrder(shouldQueries []query.Query) []query.Query {
	if len(shouldQueries) <= 1 {
		return shouldQueries
	}

	// 计算每个查询的选择性分数（分数越高，选择性越高，应该放在前面）
	type queryWithScore struct {
		q     query.Query
		score int
	}

	queriesWithScore := make([]queryWithScore, 0, len(shouldQueries))
	for _, q := range shouldQueries {
		score := o.estimateSelectivity(q)
		queriesWithScore = append(queriesWithScore, queryWithScore{q: q, score: score})
	}

	// 按分数降序排序（高选择性在前）
	for i := 0; i < len(queriesWithScore)-1; i++ {
		for j := i + 1; j < len(queriesWithScore); j++ {
			if queriesWithScore[i].score < queriesWithScore[j].score {
				queriesWithScore[i], queriesWithScore[j] = queriesWithScore[j], queriesWithScore[i]
			}
		}
	}

	// 提取排序后的查询
	result := make([]query.Query, 0, len(queriesWithScore))
	for _, qws := range queriesWithScore {
		result = append(result, qws.q)
	}

	return result
}

// estimateSelectivity 估算查询的选择性
// 返回选择性分数（分数越高，选择性越高）
func (o *QueryOptimizer) estimateSelectivity(q query.Query) int {
	switch v := q.(type) {
	case *query.TermQuery:
		return 100 // term查询选择性最高
	case *query.DisjunctionQuery:
		// terms查询（DisjunctionQuery包含多个TermQuery）
		// 检查是否都是TermQuery
		allTerms := true
		for _, subq := range v.Disjuncts {
			if _, ok := subq.(*query.TermQuery); !ok {
				allTerms = false
				break
			}
		}
		if allTerms {
			return 90 // terms查询选择性较高
		}
		return 50 // 其他DisjunctionQuery
	case *query.NumericRangeQuery:
		return 80 // range查询选择性较高
	case *query.MatchQuery:
		return 30 // match查询选择性较低
	case *query.MatchPhraseQuery:
		return 20 // match_phrase查询选择性更低
	case *query.MatchAllQuery:
		return 0 // match_all选择性最低
	case *query.BooleanQuery:
		// 对于BooleanQuery，取must的选择性（如果有）
		if v.Must != nil {
			return o.estimateSelectivity(v.Must)
		}
		return 40 // 默认中等选择性
	default:
		return 50 // 未知查询类型，默认中等选择性
	}
}

// mergeTermQueries 合并相同字段的term查询为terms查询
// 只合并should中的term查询，避免语义改变
func (o *QueryOptimizer) mergeTermQueries(queries []query.Query) []query.Query {
	if len(queries) <= 1 {
		return queries
	}

	// 按字段分组term查询
	fieldTerms := make(map[string][]string) // field -> []term values
	otherQueries := make([]query.Query, 0)  // 非term查询

	for _, q := range queries {
		if termQuery, ok := q.(*query.TermQuery); ok {
			field := termQuery.Field()
			term := termQuery.Term // Term是字段，不是方法
			if field != "" {
				fieldTerms[field] = append(fieldTerms[field], term)
			} else {
				// 没有字段的term查询，保留原样
				otherQueries = append(otherQueries, q)
			}
		} else {
			// 非term查询，保留原样
			otherQueries = append(otherQueries, q)
		}
	}

	// 合并相同字段的term查询
	result := make([]query.Query, 0, len(fieldTerms)+len(otherQueries))
	for field, terms := range fieldTerms {
		if len(terms) == 1 {
			// 只有一个term，保持为TermQuery
			termQuery := query.NewTermQuery(terms[0])
			termQuery.SetField(field)
			result = append(result, termQuery)
		} else {
			// 多个term，合并为DisjunctionQuery（相当于terms查询）
			termQueries := make([]query.Query, 0, len(terms))
			for _, term := range terms {
				termQuery := query.NewTermQuery(term)
				termQuery.SetField(field)
				termQueries = append(termQueries, termQuery)
			}
			result = append(result, query.NewDisjunctionQuery(termQueries))
		}
	}

	// 添加其他查询
	result = append(result, otherQueries...)

	return result
}

// optimizeConjunctionQuery 优化Conjunction查询
// 优化策略：
// 1. 移除match_all（不影响结果）
// 2. 如果只有一个查询，返回该查询本身
func (o *QueryOptimizer) optimizeConjunctionQuery(cq *query.ConjunctionQuery) (query.Query, error) {
	if len(cq.Conjuncts) == 0 {
		return query.NewMatchAllQuery(), nil
	}

	// 移除match_all
	optimized := make([]query.Query, 0, len(cq.Conjuncts))
	for _, q := range cq.Conjuncts {
		if _, isMatchAll := q.(*query.MatchAllQuery); !isMatchAll {
			optimized = append(optimized, q)
		}
	}

	if len(optimized) == 0 {
		return query.NewMatchAllQuery(), nil
	}

	if len(optimized) == 1 {
		return optimized[0], nil
	}

	// 如果优化后和原查询相同，返回原查询
	if len(optimized) == len(cq.Conjuncts) {
		// 简单检查：如果数量相同，假设内容相同（避免深度比较的开销）
		return cq, nil
	}

	return query.NewConjunctionQuery(optimized), nil
}

// optimizeDisjunctionQuery 优化Disjunction查询
// 优化策略：
// 1. 如果包含match_all，可以简化为match_all（但这是语义改变，不进行此优化）
// 2. 优化子查询的顺序
func (o *QueryOptimizer) optimizeDisjunctionQuery(dq *query.DisjunctionQuery) (query.Query, error) {
	if len(dq.Disjuncts) == 0 {
		return query.NewMatchNoneQuery(), nil
	}

	// 优化子查询的顺序（将高选择性查询放在前面）
	optimized := o.optimizeShouldOrder(dq.Disjuncts)

	// 合并相同字段的term查询
	optimized = o.mergeTermQueries(optimized)

	if len(optimized) == 1 {
		return optimized[0], nil
	}

	// 如果优化后和原查询相同，返回原查询
	if len(optimized) == len(dq.Disjuncts) {
		return dq, nil
	}

	return query.NewDisjunctionQuery(optimized), nil
}
