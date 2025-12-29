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
	"encoding/json"
	"strconv"

	"github.com/lscgzwd/tiggerdb/script"
)

// SortScript 基于脚本的排序
type SortScript struct {
	Script *script.Script
	Desc   bool
	values map[string]float64
	engine *script.Engine
}

// NewSortScript 创建脚本排序
func NewSortScript(s *script.Script, desc bool) *SortScript {
	return &SortScript{
		Script: s,
		Desc:   desc,
		values: make(map[string]float64),
		engine: script.NewEngine(),
	}
}

// UpdateVisitor 更新访问器
func (s *SortScript) UpdateVisitor(field string, term []byte) {
	// 脚本排序不依赖字段访问器
}

// Value 返回文档的排序值
func (s *SortScript) Value(d *DocumentMatch) string {
	if d == nil {
		return ""
	}

	// 从缓存中获取值
	if val, ok := s.values[d.ID]; ok {
		return strconv.FormatFloat(val, 'f', -1, 64)
	}

	// 计算脚本值
	ctx := script.NewContext(nil, nil, s.Script.Params)
	ctx.Score = d.Score

	// 将文档字段添加到上下文
	if d.Fields != nil {
		docFields := make(map[string]interface{})
		for k, v := range d.Fields {
			docFields[k] = v
		}
		ctx.Doc = docFields
		ctx.Source = docFields
	}

	val, err := s.engine.ExecuteScore(s.Script, ctx)
	if err != nil {
		return "0"
	}

	s.values[d.ID] = val
	return strconv.FormatFloat(val, 'f', -1, 64)
}

// DecodeValue 解码排序值
func (s *SortScript) DecodeValue(value string) string {
	return value
}

// Descending 返回是否降序
func (s *SortScript) Descending() bool {
	return s.Desc
}

// RequiresDocID 是否需要文档ID
func (s *SortScript) RequiresDocID() bool {
	return true
}

// RequiresScoring 是否需要评分
func (s *SortScript) RequiresScoring() bool {
	return true
}

// RequiresFields 是否需要字段
func (s *SortScript) RequiresFields() []string {
	return nil
}

// Reverse 反转排序方向
func (s *SortScript) Reverse() {
	s.Desc = !s.Desc
}

// MarshalJSON JSON 序列化
func (s *SortScript) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"_script": map[string]interface{}{
			"script": s.Script.Source,
			"order":  map[bool]string{true: "desc", false: "asc"}[s.Desc],
		},
	})
}

// Copy 复制排序器
func (s *SortScript) Copy() SearchSort {
	return &SortScript{
		Script: s.Script,
		Desc:   s.Desc,
		values: make(map[string]float64),
		engine: script.NewEngine(),
	}
}
