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

// QueryParserStrategy P2-2: 查询解析策略接口（策略模式，支持插件化扩展）
// 每个查询类型实现此接口，使查询解析器易于扩展和维护
type QueryParserStrategy interface {
	// Parse 解析查询体并返回Bleve Query
	Parse(body interface{}) (query.Query, error)
	// QueryType 返回查询类型名称（如 "match", "term", "bool" 等）
	QueryType() string
}

// QueryParserRegistry P2-2: 查询解析器注册表（支持动态注册新的查询类型）
type QueryParserRegistry struct {
	strategies map[string]QueryParserStrategy
}

// NewQueryParserRegistry 创建查询解析器注册表
func NewQueryParserRegistry() *QueryParserRegistry {
	registry := &QueryParserRegistry{
		strategies: make(map[string]QueryParserStrategy),
	}
	// 注册所有内置查询类型
	registry.RegisterDefaultStrategies()
	return registry
}

// Register 注册查询解析策略
func (r *QueryParserRegistry) Register(strategy QueryParserStrategy) {
	r.strategies[strategy.QueryType()] = strategy
}

// Get 获取查询解析策略
func (r *QueryParserRegistry) Get(queryType string) (QueryParserStrategy, bool) {
	strategy, exists := r.strategies[queryType]
	return strategy, exists
}

// RegisterDefaultStrategies 注册所有默认查询解析策略
func (r *QueryParserRegistry) RegisterDefaultStrategies() {
	// 基础查询类型
	r.Register(&MatchAllStrategy{})
	r.Register(&MatchStrategy{})
	r.Register(&MatchPhraseStrategy{})
	r.Register(&MatchPhrasePrefixStrategy{})
	r.Register(&TermStrategy{})
	r.Register(&TermsStrategy{})
	r.Register(&RangeStrategy{})
	r.Register(&WildcardStrategy{})
	r.Register(&PrefixStrategy{})
	r.Register(&FuzzyStrategy{})
	r.Register(&RegexpStrategy{})
	r.Register(&ExistsStrategy{})
	r.Register(&MatchNoneStrategy{})
	r.Register(&MatchBoolPrefixStrategy{})

	// 复合查询类型
	r.Register(&BoolStrategy{})
	r.Register(&ConstantScoreStrategy{})
	r.Register(&DisMaxStrategy{})

	// 全文查询类型
	r.Register(&MultiMatchStrategy{})
	r.Register(&QueryStringStrategy{})
	r.Register(&SimpleQueryStringStrategy{})
	r.Register(&CommonStrategy{})
	r.Register(&MoreLikeThisStrategy{})

	// 嵌套查询类型
	r.Register(&NestedStrategy{})

	// 地理查询类型
	r.Register(&GeoBoundingBoxStrategy{})
	r.Register(&GeoDistanceStrategy{})
	r.Register(&GeoPolygonStrategy{})
	r.Register(&GeoShapeStrategy{})

	// 脚本查询类型
	r.Register(&ScriptStrategy{})
	r.Register(&ScriptScoreStrategy{})
	r.Register(&FunctionScoreStrategy{})

	// 其他查询类型
	r.Register(&IdsStrategy{})
	r.Register(&PinnedStrategy{})
	r.Register(&WrapperStrategy{})
	r.Register(&PercolateStrategy{})

	// Span查询类型
	r.Register(&SpanTermStrategy{})
	r.Register(&SpanNearStrategy{})
	r.Register(&SpanOrStrategy{})
	r.Register(&SpanNotStrategy{})
	r.Register(&SpanFirstStrategy{})
	r.Register(&SpanContainingStrategy{})
	r.Register(&SpanWithinStrategy{})
	r.Register(&SpanMultiStrategy{})

	// 父子查询类型
	r.Register(&HasChildStrategy{})
	r.Register(&HasParentStrategy{})
}

// BaseStrategy 基础策略（包含parser引用，供子策略使用）
type BaseStrategy struct {
	parser *QueryParser
}

// SetParser 设置parser引用（在QueryParser初始化时调用）
func (s *BaseStrategy) SetParser(p *QueryParser) {
	s.parser = p
}

// 所有策略的基础实现（每个策略需要实现Parse和QueryType方法）

// MatchAllStrategy match_all查询策略
type MatchAllStrategy struct {
	BaseStrategy
}

func (s *MatchAllStrategy) QueryType() string { return "match_all" }
func (s *MatchAllStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseMatchAll(body)
}

// MatchStrategy match查询策略
type MatchStrategy struct {
	BaseStrategy
}

func (s *MatchStrategy) QueryType() string { return "match" }
func (s *MatchStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseMatch(body)
}

// MatchPhraseStrategy match_phrase查询策略
type MatchPhraseStrategy struct {
	BaseStrategy
}

func (s *MatchPhraseStrategy) QueryType() string { return "match_phrase" }
func (s *MatchPhraseStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseMatchPhrase(body)
}

// MatchPhrasePrefixStrategy match_phrase_prefix查询策略
type MatchPhrasePrefixStrategy struct {
	BaseStrategy
}

func (s *MatchPhrasePrefixStrategy) QueryType() string { return "match_phrase_prefix" }
func (s *MatchPhrasePrefixStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseMatchPhrasePrefix(body)
}

// TermStrategy term查询策略
type TermStrategy struct {
	BaseStrategy
}

func (s *TermStrategy) QueryType() string { return "term" }
func (s *TermStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseTerm(body)
}

// TermsStrategy terms查询策略
type TermsStrategy struct {
	BaseStrategy
}

func (s *TermsStrategy) QueryType() string { return "terms" }
func (s *TermsStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseTerms(body)
}

// RangeStrategy range查询策略
type RangeStrategy struct {
	BaseStrategy
}

func (s *RangeStrategy) QueryType() string { return "range" }
func (s *RangeStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseRange(body)
}

// WildcardStrategy wildcard查询策略
type WildcardStrategy struct {
	BaseStrategy
}

func (s *WildcardStrategy) QueryType() string { return "wildcard" }
func (s *WildcardStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseWildcard(body)
}

// PrefixStrategy prefix查询策略
type PrefixStrategy struct {
	BaseStrategy
}

func (s *PrefixStrategy) QueryType() string { return "prefix" }
func (s *PrefixStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parsePrefix(body)
}

// FuzzyStrategy fuzzy查询策略
type FuzzyStrategy struct {
	BaseStrategy
}

func (s *FuzzyStrategy) QueryType() string { return "fuzzy" }
func (s *FuzzyStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseFuzzy(body)
}

// RegexpStrategy regexp查询策略
type RegexpStrategy struct {
	BaseStrategy
}

func (s *RegexpStrategy) QueryType() string { return "regexp" }
func (s *RegexpStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseRegexp(body)
}

// ExistsStrategy exists查询策略
type ExistsStrategy struct {
	BaseStrategy
}

func (s *ExistsStrategy) QueryType() string { return "exists" }
func (s *ExistsStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseExists(body)
}

// MatchNoneStrategy match_none查询策略
type MatchNoneStrategy struct {
	BaseStrategy
}

func (s *MatchNoneStrategy) QueryType() string { return "match_none" }
func (s *MatchNoneStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseMatchNone(body)
}

// MatchBoolPrefixStrategy match_bool_prefix查询策略
type MatchBoolPrefixStrategy struct {
	BaseStrategy
}

func (s *MatchBoolPrefixStrategy) QueryType() string { return "match_bool_prefix" }
func (s *MatchBoolPrefixStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseMatchBoolPrefix(body)
}

// BoolStrategy bool查询策略
type BoolStrategy struct {
	BaseStrategy
}

func (s *BoolStrategy) QueryType() string { return "bool" }
func (s *BoolStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseBool(body)
}

// ConstantScoreStrategy constant_score查询策略
type ConstantScoreStrategy struct {
	BaseStrategy
}

func (s *ConstantScoreStrategy) QueryType() string { return "constant_score" }
func (s *ConstantScoreStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseConstantScore(body)
}

// DisMaxStrategy dis_max查询策略
type DisMaxStrategy struct {
	BaseStrategy
}

func (s *DisMaxStrategy) QueryType() string { return "dis_max" }
func (s *DisMaxStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseDisMax(body)
}

// MultiMatchStrategy multi_match查询策略
type MultiMatchStrategy struct {
	BaseStrategy
}

func (s *MultiMatchStrategy) QueryType() string { return "multi_match" }
func (s *MultiMatchStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseMultiMatch(body)
}

// QueryStringStrategy query_string查询策略
type QueryStringStrategy struct {
	BaseStrategy
}

func (s *QueryStringStrategy) QueryType() string { return "query_string" }
func (s *QueryStringStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseQueryString(body)
}

// SimpleQueryStringStrategy simple_query_string查询策略
type SimpleQueryStringStrategy struct {
	BaseStrategy
}

func (s *SimpleQueryStringStrategy) QueryType() string { return "simple_query_string" }
func (s *SimpleQueryStringStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseSimpleQueryString(body)
}

// CommonStrategy common查询策略
type CommonStrategy struct {
	BaseStrategy
}

func (s *CommonStrategy) QueryType() string { return "common" }
func (s *CommonStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseCommon(body)
}

// MoreLikeThisStrategy more_like_this查询策略
type MoreLikeThisStrategy struct {
	BaseStrategy
}

func (s *MoreLikeThisStrategy) QueryType() string { return "more_like_this" }
func (s *MoreLikeThisStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseMoreLikeThis(body)
}

// NestedStrategy nested查询策略
type NestedStrategy struct {
	BaseStrategy
}

func (s *NestedStrategy) QueryType() string { return "nested" }
func (s *NestedStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseNested(body)
}

// GeoBoundingBoxStrategy geo_bounding_box查询策略
type GeoBoundingBoxStrategy struct {
	BaseStrategy
}

func (s *GeoBoundingBoxStrategy) QueryType() string { return "geo_bounding_box" }
func (s *GeoBoundingBoxStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseGeoBoundingBox(body)
}

// GeoDistanceStrategy geo_distance查询策略
type GeoDistanceStrategy struct {
	BaseStrategy
}

func (s *GeoDistanceStrategy) QueryType() string { return "geo_distance" }
func (s *GeoDistanceStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseGeoDistance(body)
}

// GeoPolygonStrategy geo_polygon查询策略
type GeoPolygonStrategy struct {
	BaseStrategy
}

func (s *GeoPolygonStrategy) QueryType() string { return "geo_polygon" }
func (s *GeoPolygonStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseGeoPolygon(body)
}

// GeoShapeStrategy geo_shape查询策略
type GeoShapeStrategy struct {
	BaseStrategy
}

func (s *GeoShapeStrategy) QueryType() string { return "geo_shape" }
func (s *GeoShapeStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseGeoShape(body)
}

// ScriptStrategy script查询策略
type ScriptStrategy struct {
	BaseStrategy
}

func (s *ScriptStrategy) QueryType() string { return "script" }
func (s *ScriptStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseScript(body)
}

// ScriptScoreStrategy script_score查询策略
type ScriptScoreStrategy struct {
	BaseStrategy
}

func (s *ScriptScoreStrategy) QueryType() string { return "script_score" }
func (s *ScriptScoreStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseScriptScore(body)
}

// FunctionScoreStrategy function_score查询策略
type FunctionScoreStrategy struct {
	BaseStrategy
}

func (s *FunctionScoreStrategy) QueryType() string { return "function_score" }
func (s *FunctionScoreStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseFunctionScore(body)
}

// IdsStrategy ids查询策略
type IdsStrategy struct {
	BaseStrategy
}

func (s *IdsStrategy) QueryType() string { return "ids" }
func (s *IdsStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseIds(body)
}

// PinnedStrategy pinned查询策略
type PinnedStrategy struct {
	BaseStrategy
}

func (s *PinnedStrategy) QueryType() string { return "pinned" }
func (s *PinnedStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parsePinned(body)
}

// WrapperStrategy wrapper查询策略
type WrapperStrategy struct {
	BaseStrategy
}

func (s *WrapperStrategy) QueryType() string { return "wrapper" }
func (s *WrapperStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseWrapper(body)
}

// PercolateStrategy percolate查询策略
type PercolateStrategy struct {
	BaseStrategy
}

func (s *PercolateStrategy) QueryType() string { return "percolate" }
func (s *PercolateStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parsePercolate(body)
}

// SpanTermStrategy span_term查询策略
type SpanTermStrategy struct {
	BaseStrategy
}

func (s *SpanTermStrategy) QueryType() string { return "span_term" }
func (s *SpanTermStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseSpanTerm(body)
}

// SpanNearStrategy span_near查询策略
type SpanNearStrategy struct {
	BaseStrategy
}

func (s *SpanNearStrategy) QueryType() string { return "span_near" }
func (s *SpanNearStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseSpanNear(body)
}

// SpanOrStrategy span_or查询策略
type SpanOrStrategy struct {
	BaseStrategy
}

func (s *SpanOrStrategy) QueryType() string { return "span_or" }
func (s *SpanOrStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseSpanOr(body)
}

// SpanNotStrategy span_not查询策略
type SpanNotStrategy struct {
	BaseStrategy
}

func (s *SpanNotStrategy) QueryType() string { return "span_not" }
func (s *SpanNotStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseSpanNot(body)
}

// SpanFirstStrategy span_first查询策略
type SpanFirstStrategy struct {
	BaseStrategy
}

func (s *SpanFirstStrategy) QueryType() string { return "span_first" }
func (s *SpanFirstStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseSpanFirst(body)
}

// SpanContainingStrategy span_containing查询策略
type SpanContainingStrategy struct {
	BaseStrategy
}

func (s *SpanContainingStrategy) QueryType() string { return "span_containing" }
func (s *SpanContainingStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseSpanContaining(body)
}

// SpanWithinStrategy span_within查询策略
type SpanWithinStrategy struct {
	BaseStrategy
}

func (s *SpanWithinStrategy) QueryType() string { return "span_within" }
func (s *SpanWithinStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseSpanWithin(body)
}

// SpanMultiStrategy span_multi查询策略
type SpanMultiStrategy struct {
	BaseStrategy
}

func (s *SpanMultiStrategy) QueryType() string { return "span_multi" }
func (s *SpanMultiStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseSpanMulti(body)
}

// HasChildStrategy has_child查询策略
type HasChildStrategy struct {
	BaseStrategy
}

func (s *HasChildStrategy) QueryType() string { return "has_child" }
func (s *HasChildStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseHasChild(body)
}

// HasParentStrategy has_parent查询策略
type HasParentStrategy struct {
	BaseStrategy
}

func (s *HasParentStrategy) QueryType() string { return "has_parent" }
func (s *HasParentStrategy) Parse(body interface{}) (query.Query, error) {
	return s.parser.parseHasParent(body)
}
