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

package script

import (
	"math"
	"testing"
)

func TestParseScript(t *testing.T) {
	tests := []struct {
		name    string
		input   interface{}
		wantErr bool
	}{
		{
			name:    "string source",
			input:   "doc['price'].value > 100",
			wantErr: false,
		},
		{
			name: "map with source",
			input: map[string]interface{}{
				"source": "doc['price'].value > 100",
				"lang":   "painless",
			},
			wantErr: false,
		},
		{
			name: "map with inline",
			input: map[string]interface{}{
				"inline": "doc['price'].value > 100",
			},
			wantErr: false,
		},
		{
			name: "map with params",
			input: map[string]interface{}{
				"source": "doc['price'].value > params.threshold",
				"params": map[string]interface{}{
					"threshold": 100,
				},
			},
			wantErr: false,
		},
		{
			name:    "empty map",
			input:   map[string]interface{}{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script, err := ParseScript(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseScript() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && script == nil {
				t.Error("ParseScript() returned nil script without error")
			}
		})
	}
}

func TestEngineExecuteFilter(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name   string
		source string
		doc    map[string]interface{}
		params map[string]interface{}
		want   bool
	}{
		{
			name:   "simple comparison gt",
			source: "doc['price'].value > 100",
			doc:    map[string]interface{}{"price": 150.0},
			want:   true,
		},
		{
			name:   "simple comparison lt",
			source: "doc['price'].value < 100",
			doc:    map[string]interface{}{"price": 150.0},
			want:   false,
		},
		{
			name:   "comparison with params",
			source: "doc['price'].value > params.threshold",
			doc:    map[string]interface{}{"price": 150.0},
			params: map[string]interface{}{"threshold": 100.0},
			want:   true,
		},
		{
			name:   "equality check",
			source: "doc['status'].value == 'active'",
			doc:    map[string]interface{}{"status": "active"},
			want:   true,
		},
		{
			name:   "logical and",
			source: "doc['price'].value > 100 && doc['quantity'].value > 0",
			doc:    map[string]interface{}{"price": 150.0, "quantity": 10.0},
			want:   true,
		},
		{
			name:   "logical or",
			source: "doc['price'].value > 200 || doc['quantity'].value > 0",
			doc:    map[string]interface{}{"price": 150.0, "quantity": 10.0},
			want:   true,
		},
		{
			name:   "boolean literal true",
			source: "true",
			doc:    map[string]interface{}{},
			want:   true,
		},
		{
			name:   "boolean literal false",
			source: "false",
			doc:    map[string]interface{}{},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, tt.params)
			ctx := NewContext(tt.doc, tt.doc, tt.params)
			got, err := engine.ExecuteFilter(script, ctx)
			if err != nil {
				t.Errorf("ExecuteFilter() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("ExecuteFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEngineExecuteScore(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name   string
		source string
		doc    map[string]interface{}
		params map[string]interface{}
		score  float64
		want   float64
	}{
		{
			name:   "field value",
			source: "doc['price'].value",
			doc:    map[string]interface{}{"price": 150.0},
			want:   150.0,
		},
		{
			name:   "arithmetic add",
			source: "doc['price'].value + 10",
			doc:    map[string]interface{}{"price": 150.0},
			want:   160.0,
		},
		{
			name:   "arithmetic multiply",
			source: "doc['price'].value * 2",
			doc:    map[string]interface{}{"price": 100.0},
			want:   200.0,
		},
		{
			name:   "score based",
			source: "_score * 2",
			doc:    map[string]interface{}{},
			score:  1.5,
			want:   3.0,
		},
		{
			name:   "params value",
			source: "params.boost",
			doc:    map[string]interface{}{},
			params: map[string]interface{}{"boost": 2.5},
			want:   2.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, tt.params)
			ctx := NewContext(tt.doc, tt.doc, tt.params)
			ctx.Score = tt.score
			got, err := engine.ExecuteScore(script, ctx)
			if err != nil {
				t.Errorf("ExecuteScore() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("ExecuteScore() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEngineExecuteUpdate(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name      string
		source    string
		initial   map[string]interface{}
		params    map[string]interface{}
		wantField string
		wantValue interface{}
	}{
		{
			name:      "simple assignment",
			source:    "ctx._source.status = 'updated'",
			initial:   map[string]interface{}{"status": "pending"},
			wantField: "status",
			wantValue: "updated",
		},
		{
			name:      "increment",
			source:    "ctx._source.count += 1",
			initial:   map[string]interface{}{"count": 5.0},
			wantField: "count",
			wantValue: 6.0,
		},
		{
			name:      "decrement",
			source:    "ctx._source.stock -= 10",
			initial:   map[string]interface{}{"stock": 100.0},
			wantField: "stock",
			wantValue: 90.0,
		},
		{
			name:      "multiply assign",
			source:    "ctx._source.price *= 1.1",
			initial:   map[string]interface{}{"price": 100.0},
			wantField: "price",
			wantValue: 110.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, tt.params)
			ctx := NewContext(nil, tt.initial, tt.params)
			_, err := engine.Execute(script, ctx)
			if err != nil {
				t.Errorf("Execute() error = %v", err)
				return
			}
			if ctx.Source[tt.wantField] != tt.wantValue {
				t.Errorf("Execute() %s = %v, want %v", tt.wantField, ctx.Source[tt.wantField], tt.wantValue)
			}
		})
	}
}

// TestParenthesesExpression 测试括号表达式
func TestParenthesesExpression(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name   string
		source string
		doc    map[string]interface{}
		want   float64
	}{
		{
			name:   "simple parentheses",
			source: "(10 + 5)",
			doc:    map[string]interface{}{},
			want:   15,
		},
		{
			name:   "nested parentheses",
			source: "((2 + 3) * 4)",
			doc:    map[string]interface{}{},
			want:   20,
		},
		{
			name:   "parentheses with field",
			source: "(doc['price'].value + 10) * 2",
			doc:    map[string]interface{}{"price": 50.0},
			want:   120,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, nil)
			ctx := NewContext(tt.doc, tt.doc, nil)
			got, err := engine.ExecuteScore(script, ctx)
			if err != nil {
				t.Errorf("ExecuteScore() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("ExecuteScore() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestTernaryOperator 测试三元运算符
func TestTernaryOperator(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name   string
		source string
		doc    map[string]interface{}
		want   interface{}
	}{
		{
			name:   "ternary true condition",
			source: "true ? 'yes' : 'no'",
			doc:    map[string]interface{}{},
			want:   "yes",
		},
		{
			name:   "ternary false condition",
			source: "false ? 'yes' : 'no'",
			doc:    map[string]interface{}{},
			want:   "no",
		},
		{
			name:   "ternary with comparison",
			source: "doc['price'].value > 100 ? 'expensive' : 'cheap'",
			doc:    map[string]interface{}{"price": 150.0},
			want:   "expensive",
		},
		{
			name:   "ternary with numbers",
			source: "doc['quantity'].value > 0 ? doc['price'].value : 0",
			doc:    map[string]interface{}{"price": 99.0, "quantity": 5.0},
			want:   99.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, nil)
			ctx := NewContext(tt.doc, tt.doc, nil)
			got, err := engine.Execute(script, ctx)
			if err != nil {
				t.Errorf("Execute() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestStringMethods 测试字符串方法
func TestStringMethods(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name   string
		source string
		doc    map[string]interface{}
		want   interface{}
	}{
		{
			name:   "string length",
			source: "'hello'.length()",
			doc:    map[string]interface{}{},
			want:   5.0,
		},
		{
			name:   "string contains true",
			source: "'hello world'.contains('world')",
			doc:    map[string]interface{}{},
			want:   true,
		},
		{
			name:   "string contains false",
			source: "'hello world'.contains('foo')",
			doc:    map[string]interface{}{},
			want:   false,
		},
		{
			name:   "string startsWith",
			source: "'hello world'.startsWith('hello')",
			doc:    map[string]interface{}{},
			want:   true,
		},
		{
			name:   "string endsWith",
			source: "'hello world'.endsWith('world')",
			doc:    map[string]interface{}{},
			want:   true,
		},
		{
			name:   "string toLowerCase",
			source: "'HELLO'.toLowerCase()",
			doc:    map[string]interface{}{},
			want:   "hello",
		},
		{
			name:   "string toUpperCase",
			source: "'hello'.toUpperCase()",
			doc:    map[string]interface{}{},
			want:   "HELLO",
		},
		{
			name:   "string trim",
			source: "'  hello  '.trim()",
			doc:    map[string]interface{}{},
			want:   "hello",
		},
		{
			name:   "string substring",
			source: "'hello world'.substring(0, 5)",
			doc:    map[string]interface{}{},
			want:   "hello",
		},
		{
			name:   "string indexOf",
			source: "'hello world'.indexOf('world')",
			doc:    map[string]interface{}{},
			want:   6.0,
		},
		{
			name:   "field string method",
			source: "doc['name'].value.toLowerCase()",
			doc:    map[string]interface{}{"name": "JOHN"},
			want:   "john",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, nil)
			ctx := NewContext(tt.doc, tt.doc, nil)
			got, err := engine.Execute(script, ctx)
			if err != nil {
				t.Errorf("Execute() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestMathFunctions 测试 Math 函数
func TestMathFunctions(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name   string
		source string
		want   float64
		delta  float64 // 允许的误差
	}{
		{
			name:   "Math.abs positive",
			source: "Math.abs(5)",
			want:   5,
		},
		{
			name:   "Math.abs negative",
			source: "Math.abs(-5)",
			want:   5,
		},
		{
			name:   "Math.ceil",
			source: "Math.ceil(4.3)",
			want:   5,
		},
		{
			name:   "Math.floor",
			source: "Math.floor(4.7)",
			want:   4,
		},
		{
			name:   "Math.round",
			source: "Math.round(4.5)",
			want:   5,
		},
		{
			name:   "Math.sqrt",
			source: "Math.sqrt(16)",
			want:   4,
		},
		{
			name:   "Math.pow",
			source: "Math.pow(2, 3)",
			want:   8,
		},
		{
			name:   "Math.min",
			source: "Math.min(5, 3, 8, 1)",
			want:   1,
		},
		{
			name:   "Math.max",
			source: "Math.max(5, 3, 8, 1)",
			want:   8,
		},
		{
			name:   "Math.log",
			source: "Math.log(1)",
			want:   0,
		},
		{
			name:   "Math.exp",
			source: "Math.exp(0)",
			want:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, nil)
			ctx := NewContext(nil, nil, nil)
			got, err := engine.ExecuteScore(script, ctx)
			if err != nil {
				t.Errorf("ExecuteScore() error = %v", err)
				return
			}
			delta := tt.delta
			if delta == 0 {
				delta = 0.0001
			}
			if math.Abs(got-tt.want) > delta {
				t.Errorf("ExecuteScore() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestComplexExpressions 测试复杂表达式
func TestComplexExpressions(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name   string
		source string
		doc    map[string]interface{}
		params map[string]interface{}
		want   interface{}
	}{
		{
			name:   "complex arithmetic",
			source: "doc['price'].value * params.discount + params.shipping",
			doc:    map[string]interface{}{"price": 100.0},
			params: map[string]interface{}{"discount": 0.8, "shipping": 10.0},
			want:   90.0,
		},
		{
			name:   "nested ternary",
			source: "doc['score'].value >= 90 ? 'A' : doc['score'].value >= 80 ? 'B' : 'C'",
			doc:    map[string]interface{}{"score": 85.0},
			want:   "B",
		},
		{
			name:   "math with field",
			source: "Math.sqrt(doc['area'].value)",
			doc:    map[string]interface{}{"area": 100.0},
			want:   10.0,
		},
		{
			name:   "combined comparison and arithmetic",
			source: "(doc['price'].value > 50 && doc['quantity'].value > 0) ? doc['price'].value * doc['quantity'].value : 0",
			doc:    map[string]interface{}{"price": 100.0, "quantity": 5.0},
			want:   500.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, tt.params)
			ctx := NewContext(tt.doc, tt.doc, tt.params)
			got, err := engine.Execute(script, ctx)
			if err != nil {
				t.Errorf("Execute() error = %v", err)
				return
			}
			// 处理浮点数比较
			if wantFloat, ok := tt.want.(float64); ok {
				gotFloat := toFloat64(got)
				if math.Abs(gotFloat-wantFloat) > 0.0001 {
					t.Errorf("Execute() = %v, want %v", got, tt.want)
				}
			} else if got != tt.want {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestNullHandling 测试 null 处理
func TestNullHandling(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name   string
		source string
		doc    map[string]interface{}
		want   interface{}
	}{
		{
			name:   "null literal",
			source: "null",
			doc:    map[string]interface{}{},
			want:   nil,
		},
		{
			name:   "null comparison",
			source: "doc['missing'].value == null",
			doc:    map[string]interface{}{},
			want:   true,
		},
		{
			name:   "null ternary",
			source: "doc['field'].value != null ? doc['field'].value : 'default'",
			doc:    map[string]interface{}{"field": "value"},
			want:   "value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, nil)
			ctx := NewContext(tt.doc, tt.doc, nil)
			got, err := engine.Execute(script, ctx)
			if err != nil {
				t.Errorf("Execute() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestArrayOperations 测试数组操作
func TestArrayOperations(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name      string
		source    string
		initial   map[string]interface{}
		wantField string
		wantValue interface{}
	}{
		{
			name:      "array add",
			source:    "ctx._source.tags.add('new_tag')",
			initial:   map[string]interface{}{"tags": []interface{}{"tag1", "tag2"}},
			wantField: "tags",
			wantValue: []interface{}{"tag1", "tag2", "new_tag"},
		},
		{
			name:      "array add to empty",
			source:    "ctx._source.tags.add('first')",
			initial:   map[string]interface{}{},
			wantField: "tags",
			wantValue: []interface{}{"first"},
		},
		{
			name:      "array remove",
			source:    "ctx._source.tags.remove('tag1')",
			initial:   map[string]interface{}{"tags": []interface{}{"tag1", "tag2", "tag3"}},
			wantField: "tags",
			wantValue: []interface{}{"tag2", "tag3"},
		},
		{
			name:      "array clear",
			source:    "ctx._source.tags.clear()",
			initial:   map[string]interface{}{"tags": []interface{}{"tag1", "tag2"}},
			wantField: "tags",
			wantValue: []interface{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, nil)
			ctx := NewContext(nil, tt.initial, nil)
			_, err := engine.Execute(script, ctx)
			if err != nil {
				t.Errorf("Execute() error = %v", err)
				return
			}
			got := ctx.Source[tt.wantField]
			gotArr, ok1 := got.([]interface{})
			wantArr, ok2 := tt.wantValue.([]interface{})
			if ok1 && ok2 {
				if len(gotArr) != len(wantArr) {
					t.Errorf("Execute() %s len = %d, want %d", tt.wantField, len(gotArr), len(wantArr))
				}
			}
		})
	}
}

// TestNestedFieldOperations 测试嵌套字段操作
func TestNestedFieldOperations(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name    string
		source  string
		initial map[string]interface{}
		check   func(source map[string]interface{}) bool
	}{
		{
			name:    "nested field assignment",
			source:  "ctx._source.user.name = 'John'",
			initial: map[string]interface{}{},
			check: func(source map[string]interface{}) bool {
				if user, ok := source["user"].(map[string]interface{}); ok {
					return user["name"] == "John"
				}
				return false
			},
		},
		{
			name:    "deep nested field",
			source:  "ctx._source.a.b.c = 'deep'",
			initial: map[string]interface{}{},
			check: func(source map[string]interface{}) bool {
				if a, ok := source["a"].(map[string]interface{}); ok {
					if b, ok := a["b"].(map[string]interface{}); ok {
						return b["c"] == "deep"
					}
				}
				return false
			},
		},
		{
			name:   "update existing nested",
			source: "ctx._source.user.age = 30",
			initial: map[string]interface{}{
				"user": map[string]interface{}{
					"name": "John",
				},
			},
			check: func(source map[string]interface{}) bool {
				if user, ok := source["user"].(map[string]interface{}); ok {
					return user["name"] == "John" && user["age"] == 30.0
				}
				return false
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, nil)
			ctx := NewContext(nil, tt.initial, nil)
			_, err := engine.Execute(script, ctx)
			if err != nil {
				t.Errorf("Execute() error = %v", err)
				return
			}
			if !tt.check(ctx.Source) {
				t.Errorf("Execute() check failed for %s, source = %v", tt.name, ctx.Source)
			}
		})
	}
}

// TestESCompatibility 测试 ES 兼容性常见用例
func TestESCompatibility(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name   string
		source string
		doc    map[string]interface{}
		params map[string]interface{}
		want   interface{}
	}{
		{
			name:   "ES script filter - price range",
			source: "doc['price'].value >= params.min && doc['price'].value <= params.max",
			doc:    map[string]interface{}{"price": 150.0},
			params: map[string]interface{}{"min": 100.0, "max": 200.0},
			want:   true,
		},
		{
			name:   "ES script score - decay function",
			source: "1 / (1 + Math.abs(doc['price'].value - params.origin) / params.scale)",
			doc:    map[string]interface{}{"price": 100.0},
			params: map[string]interface{}{"origin": 100.0, "scale": 10.0},
			want:   1.0,
		},
		{
			name:   "ES script update - increment counter",
			source: "ctx._source.views += 1",
			doc:    map[string]interface{}{"views": 10.0},
			want:   11.0,
		},
		{
			name:   "ES script sort - custom score",
			source: "_score * doc['popularity'].value",
			doc:    map[string]interface{}{"popularity": 2.0},
			want:   3.0, // _score = 1.5, 1.5 * 2 = 3.0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, tt.params)
			ctx := NewContext(tt.doc, tt.doc, tt.params)
			ctx.Score = 1.5

			var got interface{}
			var err error

			if tt.name == "ES script update - increment counter" {
				_, err = engine.Execute(script, ctx)
				got = ctx.Source["views"]
			} else {
				got, err = engine.Execute(script, ctx)
			}

			if err != nil {
				t.Errorf("Execute() error = %v", err)
				return
			}

			// 处理浮点数比较
			if wantFloat, ok := tt.want.(float64); ok {
				gotFloat := toFloat64(got)
				if math.Abs(gotFloat-wantFloat) > 0.0001 {
					t.Errorf("Execute() = %v, want %v", got, tt.want)
				}
			} else if got != tt.want {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSwitchStatement 测试switch语句
func TestSwitchStatement(t *testing.T) {
	engine := NewEngine()
	ctx := NewContext(nil, nil, nil)

	tests := []struct {
		name   string
		source string
		want   interface{}
	}{
		{
			name:   "switch with case match",
			source: "switch (1) { case 1: return 'one'; case 2: return 'two'; default: return 'other'; }",
			want:   "one",
		},
		{
			name:   "switch with default",
			source: "switch (3) { case 1: return 'one'; case 2: return 'two'; default: return 'other'; }",
			want:   "other",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, nil)
			got, err := engine.Execute(script, ctx)
			if err != nil {
				t.Errorf("Execute() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestDoWhileLoop 测试do-while循环
func TestDoWhileLoop(t *testing.T) {
	engine := NewEngine()
	ctx := NewContext(nil, nil, nil)

	tests := []struct {
		name   string
		source string
		want   interface{}
	}{
		{
			name:   "do-while executes at least once",
			source: "def x = 0;\ndo {\nx = x + 1;\n} while (x < 1);\nreturn x;",
			want:   float64(1),
		},
		{
			name:   "do-while with condition",
			source: "def x = 0;\ndo {\nx = x + 1;\n} while (x < 3);\nreturn x;",
			want:   float64(3),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, nil)
			got, err := engine.Execute(script, ctx)
			if err != nil {
				t.Errorf("Execute() error = %v", err)
				return
			}
			if wantFloat, ok := tt.want.(float64); ok {
				gotFloat := toFloat64(got)
				if math.Abs(gotFloat-wantFloat) > 0.0001 {
					t.Errorf("Execute() = %v, want %v", got, tt.want)
				}
			} else if got != tt.want {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestBreakContinue 测试break和continue语句
func TestBreakContinue(t *testing.T) {
	engine := NewEngine()
	ctx := NewContext(nil, nil, nil)

	tests := []struct {
		name   string
		source string
		want   interface{}
	}{
		{
			name:   "break in for loop",
			source: "def x = 0; for (def i = 0; i < 10; i = i + 1) { if (i == 5) { break; } x = x + 1; } return x;",
			want:   float64(5),
		},
		{
			name:   "continue in for loop",
			source: "def x = 0; for (def i = 0; i < 5; i = i + 1) { if (i == 2) { continue; } x = x + 1; } return x;",
			want:   float64(4),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, nil)
			got, err := engine.Execute(script, ctx)
			if err != nil {
				t.Errorf("Execute() error = %v", err)
				return
			}
			if wantFloat, ok := tt.want.(float64); ok {
				gotFloat := toFloat64(got)
				if math.Abs(gotFloat-wantFloat) > 0.0001 {
					t.Errorf("Execute() = %v, want %v", got, tt.want)
				}
			} else if got != tt.want {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestDateOperations 测试日期操作
func TestDateOperations(t *testing.T) {
	engine := NewEngine()
	ctx := NewContext(nil, nil, nil)

	tests := []struct {
		name   string
		source string
		want   interface{}
	}{
		{
			name:   "Date.parse ISO format",
			source: "Date.parse('2024-01-15')",
			want:   float64(1705276800000), // 2024-01-15 00:00:00 UTC 的时间戳（毫秒）
		},
		{
			name:   "Date.add days",
			source: "Date.add(1705276800000, 'days', 7)",
			want:   float64(1705881600000), // 2024-01-22 00:00:00 UTC
		},
		{
			name:   "Date.subtract days",
			source: "Date.subtract(1705276800000, 'days', 7)",
			want:   float64(1704672000000), // 2024-01-08 00:00:00 UTC
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := NewScript(tt.source, nil)
			got, err := engine.Execute(script, ctx)
			if err != nil {
				t.Errorf("Execute() error = %v", err)
				return
			}
			if wantFloat, ok := tt.want.(float64); ok {
				gotFloat := toFloat64(got)
				// 允许1小时的误差（由于时区差异）
				if math.Abs(gotFloat-wantFloat) > 3600000 {
					t.Errorf("Execute() = %v, want %v (diff: %v)", got, tt.want, math.Abs(gotFloat-wantFloat))
				}
			} else if got != tt.want {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}
