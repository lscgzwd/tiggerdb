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
	"math"

	index "github.com/blevesearch/bleve_index_api"
	"github.com/lscgzwd/tiggerdb/mapping"
	"github.com/lscgzwd/tiggerdb/script"
	"github.com/lscgzwd/tiggerdb/search"
)

// ScoreMode 评分模式
type ScoreMode string

const (
	ScoreModeMultiply ScoreMode = "multiply"
	ScoreModeSum      ScoreMode = "sum"
	ScoreModeAvg      ScoreMode = "avg"
	ScoreModeFirst    ScoreMode = "first"
	ScoreModeMax      ScoreMode = "max"
	ScoreModeMin      ScoreMode = "min"
)

// BoostMode 提升模式
type BoostMode string

const (
	BoostModeMultiply BoostMode = "multiply"
	BoostModeReplace  BoostMode = "replace"
	BoostModeSum      BoostMode = "sum"
	BoostModeAvg      BoostMode = "avg"
	BoostModeMax      BoostMode = "max"
	BoostModeMin      BoostMode = "min"
)

// ScoreFunction 评分函数接口
type ScoreFunction interface {
	Score(doc map[string]interface{}, originalScore float64) float64
}

// ScriptScoreFunction 脚本评分函数
type ScriptScoreFunction struct {
	Script *script.Script
	engine *script.Engine
}

func NewScriptScoreFunction(s *script.Script) *ScriptScoreFunction {
	return &ScriptScoreFunction{
		Script: s,
		engine: script.NewEngine(),
	}
}

func (f *ScriptScoreFunction) Score(doc map[string]interface{}, originalScore float64) float64 {
	ctx := script.NewContext(doc, doc, f.Script.Params)
	ctx.Score = originalScore
	score, err := f.engine.ExecuteScore(f.Script, ctx)
	if err != nil {
		return originalScore
	}
	return score
}

// FieldValueFactorFunction 字段值因子函数
type FieldValueFactorFunction struct {
	Field    string
	Factor   float64
	Modifier string // none, log, log1p, log2p, ln, ln1p, ln2p, square, sqrt, reciprocal
	Missing  float64
}

func NewFieldValueFactorFunction(field string, factor float64, modifier string, missing float64) *FieldValueFactorFunction {
	if factor == 0 {
		factor = 1
	}
	if modifier == "" {
		modifier = "none"
	}
	return &FieldValueFactorFunction{
		Field:    field,
		Factor:   factor,
		Modifier: modifier,
		Missing:  missing,
	}
}

func (f *FieldValueFactorFunction) Score(doc map[string]interface{}, originalScore float64) float64 {
	value := f.Missing
	if v, ok := doc[f.Field]; ok {
		value = toFloat64(v)
	}

	// 应用因子
	value = value * f.Factor

	// 应用修饰符
	switch f.Modifier {
	case "log":
		if value > 0 {
			value = math.Log10(value)
		}
	case "log1p":
		value = math.Log10(value + 1)
	case "log2p":
		value = math.Log10(value + 2)
	case "ln":
		if value > 0 {
			value = math.Log(value)
		}
	case "ln1p":
		value = math.Log(value + 1)
	case "ln2p":
		value = math.Log(value + 2)
	case "square":
		value = value * value
	case "sqrt":
		if value >= 0 {
			value = math.Sqrt(value)
		}
	case "reciprocal":
		if value != 0 {
			value = 1 / value
		}
	}

	return value
}

// DecayFunction 衰减函数
type DecayFunction struct {
	Field     string
	Origin    float64
	Scale     float64
	Offset    float64
	Decay     float64
	DecayType string // linear, exp, gauss
}

func NewDecayFunction(field string, origin, scale, offset, decay float64, decayType string) *DecayFunction {
	if decay == 0 {
		decay = 0.5
	}
	if decayType == "" {
		decayType = "gauss"
	}
	return &DecayFunction{
		Field:     field,
		Origin:    origin,
		Scale:     scale,
		Offset:    offset,
		Decay:     decay,
		DecayType: decayType,
	}
}

func (f *DecayFunction) Score(doc map[string]interface{}, originalScore float64) float64 {
	value := 0.0
	if v, ok := doc[f.Field]; ok {
		value = toFloat64(v)
	}

	// 计算距离
	distance := math.Abs(value - f.Origin)
	if distance <= f.Offset {
		return 1.0
	}
	distance = distance - f.Offset

	// 根据衰减类型计算分数
	switch f.DecayType {
	case "linear":
		return math.Max(0, 1-distance/f.Scale*(1-f.Decay))
	case "exp":
		return math.Pow(f.Decay, distance/f.Scale)
	case "gauss":
		sigma := f.Scale / math.Sqrt(2*math.Log(1/f.Decay))
		return math.Exp(-distance * distance / (2 * sigma * sigma))
	default:
		return 1.0
	}
}

// RandomScoreFunction 随机评分函数
type RandomScoreFunction struct {
	Seed  int64
	Field string
}

func NewRandomScoreFunction(seed int64, field string) *RandomScoreFunction {
	return &RandomScoreFunction{
		Seed:  seed,
		Field: field,
	}
}

func (f *RandomScoreFunction) Score(doc map[string]interface{}, originalScore float64) float64 {
	// 简单的伪随机实现
	var hash int64
	if f.Field != "" {
		if v, ok := doc[f.Field].(string); ok {
			for _, c := range v {
				hash = hash*31 + int64(c)
			}
		}
	}
	hash = (hash+f.Seed)*1103515245 + 12345
	return float64((hash>>16)&0x7fff) / 32768.0
}

// WeightFunction 权重函数
type WeightFunction struct {
	Weight float64
}

func NewWeightFunction(weight float64) *WeightFunction {
	return &WeightFunction{Weight: weight}
}

func (f *WeightFunction) Score(doc map[string]interface{}, originalScore float64) float64 {
	return f.Weight
}

// FunctionScoreQuery 函数评分查询
type FunctionScoreQuery struct {
	innerQuery Query
	functions  []FunctionWithFilter
	scoreMode  ScoreMode
	boostMode  BoostMode
	maxBoost   float64
	minScore   float64
	boost      float64
}

// FunctionWithFilter 带过滤器的评分函数
type FunctionWithFilter struct {
	Filter   Query
	Function ScoreFunction
	Weight   float64
}

// NewFunctionScoreQuery 创建新的函数评分查询
func NewFunctionScoreQuery(innerQuery Query) *FunctionScoreQuery {
	return &FunctionScoreQuery{
		innerQuery: innerQuery,
		scoreMode:  ScoreModeMultiply,
		boostMode:  BoostModeMultiply,
		maxBoost:   math.MaxFloat64,
		boost:      1.0,
	}
}

// AddFunction 添加评分函数
func (q *FunctionScoreQuery) AddFunction(filter Query, fn ScoreFunction, weight float64) {
	if weight == 0 {
		weight = 1
	}
	q.functions = append(q.functions, FunctionWithFilter{
		Filter:   filter,
		Function: fn,
		Weight:   weight,
	})
}

// SetScoreMode 设置评分模式
func (q *FunctionScoreQuery) SetScoreMode(mode ScoreMode) {
	q.scoreMode = mode
}

// SetBoostMode 设置提升模式
func (q *FunctionScoreQuery) SetBoostMode(mode BoostMode) {
	q.boostMode = mode
}

// SetMaxBoost 设置最大提升值
func (q *FunctionScoreQuery) SetMaxBoost(maxBoost float64) {
	q.maxBoost = maxBoost
}

// SetMinScore 设置最小分数
func (q *FunctionScoreQuery) SetMinScore(minScore float64) {
	q.minScore = minScore
}

// SetBoost 设置权重
func (q *FunctionScoreQuery) SetBoost(b float64) {
	q.boost = b
}

// Boost 返回权重
func (q *FunctionScoreQuery) Boost() float64 {
	return q.boost
}

// Searcher 实现 Query 接口
func (q *FunctionScoreQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	innerSearcher, err := q.innerQuery.Searcher(ctx, i, m, options)
	if err != nil {
		return nil, err
	}

	return NewFunctionScoreSearcher(innerSearcher, q, i)
}

// FunctionScoreSearcher 函数评分搜索器
type FunctionScoreSearcher struct {
	inner  search.Searcher
	query  *FunctionScoreQuery
	reader index.IndexReader
}

// NewFunctionScoreSearcher 创建函数评分搜索器
func NewFunctionScoreSearcher(inner search.Searcher, query *FunctionScoreQuery, reader index.IndexReader) (*FunctionScoreSearcher, error) {
	return &FunctionScoreSearcher{
		inner:  inner,
		query:  query,
		reader: reader,
	}, nil
}

// Next 返回下一个匹配的文档
func (s *FunctionScoreSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
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
			continue
		}

		docFields := make(map[string]interface{})
		doc.VisitFields(func(field index.Field) {
			docFields[field.Name()] = string(field.Value())
		})

		// 计算新评分
		newScore := s.calculateScore(docFields, match.Score)
		match.Score = newScore

		// 检查最小分数
		if s.query.minScore > 0 && match.Score < s.query.minScore {
			continue
		}

		return match, nil
	}
}

// calculateScore 计算最终评分
func (s *FunctionScoreSearcher) calculateScore(doc map[string]interface{}, originalScore float64) float64 {
	if len(s.query.functions) == 0 {
		return originalScore * s.query.boost
	}

	var scores []float64
	for _, fn := range s.query.functions {
		// TODO: 检查过滤器是否匹配（简化实现，忽略过滤器）
		score := fn.Function.Score(doc, originalScore) * fn.Weight
		scores = append(scores, score)
	}

	// 组合函数评分
	var functionScore float64
	switch s.query.scoreMode {
	case ScoreModeMultiply:
		functionScore = 1
		for _, sc := range scores {
			functionScore *= sc
		}
	case ScoreModeSum:
		for _, sc := range scores {
			functionScore += sc
		}
	case ScoreModeAvg:
		for _, sc := range scores {
			functionScore += sc
		}
		if len(scores) > 0 {
			functionScore /= float64(len(scores))
		}
	case ScoreModeFirst:
		if len(scores) > 0 {
			functionScore = scores[0]
		}
	case ScoreModeMax:
		for _, sc := range scores {
			if sc > functionScore {
				functionScore = sc
			}
		}
	case ScoreModeMin:
		functionScore = math.MaxFloat64
		for _, sc := range scores {
			if sc < functionScore {
				functionScore = sc
			}
		}
	}

	// 应用 max_boost
	if functionScore > s.query.maxBoost {
		functionScore = s.query.maxBoost
	}

	// 组合原始评分和函数评分
	var finalScore float64
	switch s.query.boostMode {
	case BoostModeMultiply:
		finalScore = originalScore * functionScore
	case BoostModeReplace:
		finalScore = functionScore
	case BoostModeSum:
		finalScore = originalScore + functionScore
	case BoostModeAvg:
		finalScore = (originalScore + functionScore) / 2
	case BoostModeMax:
		finalScore = math.Max(originalScore, functionScore)
	case BoostModeMin:
		finalScore = math.Min(originalScore, functionScore)
	}

	return finalScore * s.query.boost
}

// Advance 跳到指定文档
func (s *FunctionScoreSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	match, err := s.inner.Advance(ctx, ID)
	if err != nil || match == nil {
		return match, err
	}

	doc, err := s.reader.Document(match.ID)
	if err != nil {
		return match, nil
	}

	docFields := make(map[string]interface{})
	doc.VisitFields(func(field index.Field) {
		docFields[field.Name()] = string(field.Value())
	})

	match.Score = s.calculateScore(docFields, match.Score)
	return match, nil
}

func (s *FunctionScoreSearcher) Close() error               { return s.inner.Close() }
func (s *FunctionScoreSearcher) Count() uint64              { return s.inner.Count() }
func (s *FunctionScoreSearcher) Min() int                   { return s.inner.Min() }
func (s *FunctionScoreSearcher) DocumentMatchPoolSize() int { return s.inner.DocumentMatchPoolSize() }
func (s *FunctionScoreSearcher) Weight() float64            { return s.query.boost }
func (s *FunctionScoreSearcher) SetQueryNorm(qnorm float64) { s.inner.SetQueryNorm(qnorm) }
func (s *FunctionScoreSearcher) Size() int                  { return s.inner.Size() }

// toFloat64 辅助函数
func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case string:
		var f float64
		_, _ = fmt.Sscanf(val, "%f", &f)
		return f
	default:
		return 0
	}
}
