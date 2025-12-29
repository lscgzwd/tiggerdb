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

// Package script 实现 ES 兼容的脚本引擎
// 支持 Painless 表达式语法，用于过滤、排序、计算字段和更新文档
package script

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Script 表示一个脚本
type Script struct {
	Source string                 // 脚本源代码
	Lang   string                 // 脚本语言 (painless, expression)
	Params map[string]interface{} // 脚本参数
}

// NewScript 创建新脚本
func NewScript(source string, params map[string]interface{}) *Script {
	return &Script{
		Source: source,
		Lang:   "painless",
		Params: params,
	}
}

// ParseScript 从 ES 格式解析脚本
func ParseScript(data interface{}) (*Script, error) {
	switch v := data.(type) {
	case string:
		return NewScript(v, nil), nil
	case map[string]interface{}:
		script := &Script{Lang: "painless"}

		if source, ok := v["source"].(string); ok {
			script.Source = source
		} else if inline, ok := v["inline"].(string); ok {
			script.Source = inline
		}

		if lang, ok := v["lang"].(string); ok {
			script.Lang = lang
		}

		if params, ok := v["params"].(map[string]interface{}); ok {
			script.Params = params
		}

		if script.Source == "" {
			return nil, fmt.Errorf("script must have 'source' or 'inline' field")
		}

		return script, nil
	default:
		return nil, fmt.Errorf("invalid script format: %T", data)
	}
}

// Context 脚本执行上下文
type Context struct {
	Doc    map[string]interface{} // 文档字段 (doc['field'].value)
	Source map[string]interface{} // _source 字段
	Params map[string]interface{} // 脚本参数
	Score  float64                // 文档评分
	Now    int64                  // 当前时间戳（毫秒）
	Ctx    map[string]interface{} // ctx 上下文（用于更新脚本）
}

// NewContext 创建执行上下文
func NewContext(doc, source, params map[string]interface{}) *Context {
	ctx := &Context{
		Doc:    doc,
		Source: source,
		Params: params,
		Now:    time.Now().UnixMilli(),
	}
	// 初始化 ctx 对象，用于更新脚本
	ctx.Ctx = map[string]interface{}{
		"_source": source,
	}
	return ctx
}

// Engine 脚本引擎
type Engine struct {
	cache *ScriptCache // 脚本编译缓存
}

// NewEngine 创建脚本引擎
func NewEngine() *Engine {
	return &Engine{
		cache: globalCache,
	}
}

// NewEngineWithCache 创建带自定义缓存的脚本引擎
func NewEngineWithCache(cache *ScriptCache) *Engine {
	return &Engine{
		cache: cache,
	}
}

// Execute 执行脚本并返回结果
func (e *Engine) Execute(script *Script, ctx *Context) (interface{}, error) {
	if script == nil || script.Source == "" {
		return nil, fmt.Errorf("empty script")
	}

	// 合并参数到上下文
	if script.Params != nil {
		if ctx.Params == nil {
			ctx.Params = make(map[string]interface{})
		}
		for k, v := range script.Params {
			ctx.Params[k] = v
		}
	}

	return e.evaluate(script.Source, ctx)
}

// ExecuteFilter 执行脚本作为过滤器（返回布尔值）
func (e *Engine) ExecuteFilter(script *Script, ctx *Context) (bool, error) {
	result, err := e.Execute(script, ctx)
	if err != nil {
		return false, err
	}
	return toBool(result), nil
}

// ExecuteScore 执行脚本计算评分（返回数值）
func (e *Engine) ExecuteScore(script *Script, ctx *Context) (float64, error) {
	result, err := e.Execute(script, ctx)
	if err != nil {
		return 0, err
	}
	return toFloat64(result), nil
}

// evaluate 解析并执行表达式
func (e *Engine) evaluate(source string, ctx *Context) (interface{}, error) {
	source = strings.TrimSpace(source)

	// 空表达式返回 nil
	if source == "" {
		return nil, nil
	}

	// 处理括号表达式
	if result, ok, err := e.evaluateParentheses(source, ctx); ok {
		return result, err
	}

	// 处理三元运算符
	if result, ok, err := e.evaluateTernary(source, ctx); ok {
		return result, err
	}

	// 处理比较表达式
	if result, ok, err := e.evaluateComparison(source, ctx); ok {
		return result, err
	}

	// 处理算术表达式
	if result, ok, err := e.evaluateArithmetic(source, ctx); ok {
		return result, err
	}

	// 处理方法调用
	if result, ok, err := e.evaluateMethodCall(source, ctx); ok {
		return result, err
	}

	// 处理内置函数
	if result, ok, err := e.evaluateBuiltinFunction(source, ctx); ok {
		return result, err
	}

	// 处理字段访问
	if result, ok := e.evaluateFieldAccess(source, ctx); ok {
		return result, nil
	}

	// 处理字面量
	if result, ok := e.evaluateLiteral(source); ok {
		return result, nil
	}

	// 处理简单赋值语句（用于更新）
	if result, ok, err := e.evaluateAssignment(source, ctx); ok {
		return result, err
	}

	return nil, fmt.Errorf("unsupported expression: %s", source)
}

// evaluateParentheses 处理括号表达式
func (e *Engine) evaluateParentheses(source string, ctx *Context) (interface{}, bool, error) {
	if !strings.HasPrefix(source, "(") {
		return nil, false, nil
	}

	// 找到匹配的右括号
	depth := 0
	for i, c := range source {
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
			if depth == 0 {
				// 如果括号后面还有内容，不是纯括号表达式
				if i < len(source)-1 {
					return nil, false, nil
				}
				// 递归计算括号内的表达式
				inner := source[1:i]
				result, err := e.evaluate(inner, ctx)
				return result, true, err
			}
		}
	}
	return nil, false, nil
}

// evaluateTernary 处理三元运算符 condition ? trueValue : falseValue
func (e *Engine) evaluateTernary(source string, ctx *Context) (interface{}, bool, error) {
	// 找到问号位置（需要跳过字符串内的问号）
	qIdx := findOperatorOutsideStrings(source, "?")
	if qIdx < 0 {
		return nil, false, nil
	}

	// 找到冒号位置
	colonIdx := findOperatorOutsideStrings(source[qIdx+1:], ":")
	if colonIdx < 0 {
		return nil, false, nil
	}
	colonIdx += qIdx + 1

	condition := strings.TrimSpace(source[:qIdx])
	trueVal := strings.TrimSpace(source[qIdx+1 : colonIdx])
	falseVal := strings.TrimSpace(source[colonIdx+1:])

	condResult, err := e.evaluate(condition, ctx)
	if err != nil {
		return nil, true, err
	}

	if toBool(condResult) {
		result, err := e.evaluate(trueVal, ctx)
		return result, true, err
	}
	result, err := e.evaluate(falseVal, ctx)
	return result, true, err
}

// findOperatorOutsideStrings 在字符串外找运算符
func findOperatorOutsideStrings(source, op string) int {
	inString := false
	stringChar := byte(0)
	for i := 0; i < len(source); i++ {
		c := source[i]
		if !inString && (c == '"' || c == '\'') {
			inString = true
			stringChar = c
		} else if inString && c == stringChar {
			inString = false
		} else if !inString && strings.HasPrefix(source[i:], op) {
			return i
		}
	}
	return -1
}

// evaluateMethodCall 处理方法调用
func (e *Engine) evaluateMethodCall(source string, ctx *Context) (interface{}, bool, error) {
	// 字符串方法：str.length(), str.contains(), str.startsWith(), str.endsWith()
	// str.toLowerCase(), str.toUpperCase(), str.trim(), str.substring()

	// 查找方法调用模式
	dotIdx := strings.LastIndex(source, ".")
	if dotIdx < 0 {
		return nil, false, nil
	}

	// 检查是否是方法调用（后面跟括号）
	rest := source[dotIdx+1:]
	parenIdx := strings.Index(rest, "(")
	if parenIdx < 0 {
		return nil, false, nil
	}

	methodName := rest[:parenIdx]
	if !strings.HasSuffix(rest, ")") {
		return nil, false, nil
	}
	args := rest[parenIdx+1 : len(rest)-1]

	// 获取对象值
	objExpr := source[:dotIdx]
	objVal, err := e.evaluate(objExpr, ctx)
	if err != nil {
		return nil, true, err
	}

	// 执行方法
	result, err := e.executeMethod(objVal, methodName, args, ctx)
	return result, true, err
}

// executeMethod 执行对象方法
func (e *Engine) executeMethod(obj interface{}, method, args string, ctx *Context) (interface{}, error) {
	switch method {
	// 字符串方法
	case "length":
		if str, ok := obj.(string); ok {
			return float64(len(str)), nil
		}
		if arr, ok := obj.([]interface{}); ok {
			return float64(len(arr)), nil
		}
		return 0.0, nil

	case "contains":
		if str, ok := obj.(string); ok {
			argVal, err := e.evaluate(args, ctx)
			if err != nil {
				return false, err
			}
			return strings.Contains(str, toString(argVal)), nil
		}
		return false, nil

	case "startsWith":
		if str, ok := obj.(string); ok {
			argVal, err := e.evaluate(args, ctx)
			if err != nil {
				return false, err
			}
			return strings.HasPrefix(str, toString(argVal)), nil
		}
		return false, nil

	case "endsWith":
		if str, ok := obj.(string); ok {
			argVal, err := e.evaluate(args, ctx)
			if err != nil {
				return false, err
			}
			return strings.HasSuffix(str, toString(argVal)), nil
		}
		return false, nil

	case "toLowerCase":
		if str, ok := obj.(string); ok {
			return strings.ToLower(str), nil
		}
		return "", nil

	case "toUpperCase":
		if str, ok := obj.(string); ok {
			return strings.ToUpper(str), nil
		}
		return "", nil

	case "trim":
		if str, ok := obj.(string); ok {
			return strings.TrimSpace(str), nil
		}
		return "", nil

	case "substring":
		if str, ok := obj.(string); ok {
			argParts := strings.Split(args, ",")
			if len(argParts) >= 1 {
				start, _ := e.evaluate(strings.TrimSpace(argParts[0]), ctx)
				startIdx := int(toFloat64(start))
				if startIdx < 0 {
					startIdx = 0
				}
				if startIdx > len(str) {
					startIdx = len(str)
				}
				if len(argParts) >= 2 {
					end, _ := e.evaluate(strings.TrimSpace(argParts[1]), ctx)
					endIdx := int(toFloat64(end))
					if endIdx > len(str) {
						endIdx = len(str)
					}
					if endIdx < startIdx {
						endIdx = startIdx
					}
					return str[startIdx:endIdx], nil
				}
				return str[startIdx:], nil
			}
		}
		return "", nil

	case "indexOf":
		if str, ok := obj.(string); ok {
			argVal, err := e.evaluate(args, ctx)
			if err != nil {
				return -1.0, err
			}
			return float64(strings.Index(str, toString(argVal))), nil
		}
		return -1.0, nil

	case "replace":
		if str, ok := obj.(string); ok {
			argParts := strings.Split(args, ",")
			if len(argParts) >= 2 {
				old, _ := e.evaluate(strings.TrimSpace(argParts[0]), ctx)
				new, _ := e.evaluate(strings.TrimSpace(argParts[1]), ctx)
				return strings.Replace(str, toString(old), toString(new), -1), nil
			}
		}
		return "", nil

	case "split":
		if str, ok := obj.(string); ok {
			argVal, err := e.evaluate(args, ctx)
			if err != nil {
				return nil, err
			}
			parts := strings.Split(str, toString(argVal))
			result := make([]interface{}, len(parts))
			for i, p := range parts {
				result[i] = p
			}
			return result, nil
		}
		return nil, nil

	// 数组方法
	case "size":
		if arr, ok := obj.([]interface{}); ok {
			return float64(len(arr)), nil
		}
		if m, ok := obj.(map[string]interface{}); ok {
			return float64(len(m)), nil
		}
		return 0.0, nil

	case "isEmpty":
		if arr, ok := obj.([]interface{}); ok {
			return len(arr) == 0, nil
		}
		if str, ok := obj.(string); ok {
			return str == "", nil
		}
		return true, nil

	case "get":
		argVal, err := e.evaluate(args, ctx)
		if err != nil {
			return nil, err
		}
		if arr, ok := obj.([]interface{}); ok {
			idx := int(toFloat64(argVal))
			if idx >= 0 && idx < len(arr) {
				return arr[idx], nil
			}
		}
		if m, ok := obj.(map[string]interface{}); ok {
			key := toString(argVal)
			return m[key], nil
		}
		return nil, nil

	default:
		return nil, fmt.Errorf("unsupported method: %s", method)
	}
}

// evaluateBuiltinFunction 处理内置函数
func (e *Engine) evaluateBuiltinFunction(source string, ctx *Context) (interface{}, bool, error) {
	// Math 函数
	mathFuncs := map[string]func(float64) float64{
		"Math.abs":   math.Abs,
		"Math.ceil":  math.Ceil,
		"Math.floor": math.Floor,
		"Math.round": math.Round,
		"Math.sqrt":  math.Sqrt,
		"Math.log":   math.Log,
		"Math.log10": math.Log10,
		"Math.exp":   math.Exp,
		"Math.sin":   math.Sin,
		"Math.cos":   math.Cos,
		"Math.tan":   math.Tan,
	}

	for name, fn := range mathFuncs {
		if strings.HasPrefix(source, name+"(") && strings.HasSuffix(source, ")") {
			arg := source[len(name)+1 : len(source)-1]
			val, err := e.evaluate(arg, ctx)
			if err != nil {
				return nil, true, err
			}
			return fn(toFloat64(val)), true, nil
		}
	}

	// Math.min/max（多参数）
	if strings.HasPrefix(source, "Math.min(") && strings.HasSuffix(source, ")") {
		args := source[9 : len(source)-1]
		return e.evalMinMax(args, ctx, true)
	}
	if strings.HasPrefix(source, "Math.max(") && strings.HasSuffix(source, ")") {
		args := source[9 : len(source)-1]
		return e.evalMinMax(args, ctx, false)
	}

	// Math.pow
	if strings.HasPrefix(source, "Math.pow(") && strings.HasSuffix(source, ")") {
		args := source[9 : len(source)-1]
		parts := strings.Split(args, ",")
		if len(parts) == 2 {
			base, err := e.evaluate(strings.TrimSpace(parts[0]), ctx)
			if err != nil {
				return nil, true, err
			}
			exp, err := e.evaluate(strings.TrimSpace(parts[1]), ctx)
			if err != nil {
				return nil, true, err
			}
			return math.Pow(toFloat64(base), toFloat64(exp)), true, nil
		}
	}

	// Math.random
	if source == "Math.random()" {
		return float64(time.Now().UnixNano()%1000) / 1000.0, true, nil
	}

	return nil, false, nil
}

// evalMinMax 计算最小/最大值
func (e *Engine) evalMinMax(args string, ctx *Context, isMin bool) (interface{}, bool, error) {
	parts := strings.Split(args, ",")
	if len(parts) == 0 {
		return 0.0, true, nil
	}

	first, err := e.evaluate(strings.TrimSpace(parts[0]), ctx)
	if err != nil {
		return nil, true, err
	}
	result := toFloat64(first)

	for i := 1; i < len(parts); i++ {
		val, err := e.evaluate(strings.TrimSpace(parts[i]), ctx)
		if err != nil {
			return nil, true, err
		}
		num := toFloat64(val)
		if isMin {
			if num < result {
				result = num
			}
		} else {
			if num > result {
				result = num
			}
		}
	}
	return result, true, nil
}

// evaluateComparison 处理比较表达式
func (e *Engine) evaluateComparison(source string, ctx *Context) (interface{}, bool, error) {
	operators := []string{">=", "<=", "!=", "==", ">", "<"}

	for _, op := range operators {
		if parts := strings.SplitN(source, op, 2); len(parts) == 2 {
			left, err := e.evaluate(strings.TrimSpace(parts[0]), ctx)
			if err != nil {
				return nil, true, err
			}
			right, err := e.evaluate(strings.TrimSpace(parts[1]), ctx)
			if err != nil {
				return nil, true, err
			}

			result := compare(left, right, op)
			return result, true, nil
		}
	}

	// 处理逻辑运算符
	if strings.Contains(source, "&&") {
		parts := strings.SplitN(source, "&&", 2)
		left, err := e.evaluate(strings.TrimSpace(parts[0]), ctx)
		if err != nil {
			return nil, true, err
		}
		if !toBool(left) {
			return false, true, nil
		}
		right, err := e.evaluate(strings.TrimSpace(parts[1]), ctx)
		if err != nil {
			return nil, true, err
		}
		return toBool(right), true, nil
	}

	if strings.Contains(source, "||") {
		parts := strings.SplitN(source, "||", 2)
		left, err := e.evaluate(strings.TrimSpace(parts[0]), ctx)
		if err != nil {
			return nil, true, err
		}
		if toBool(left) {
			return true, true, nil
		}
		right, err := e.evaluate(strings.TrimSpace(parts[1]), ctx)
		if err != nil {
			return nil, true, err
		}
		return toBool(right), true, nil
	}

	return nil, false, nil
}

// evaluateArithmetic 处理算术表达式
func (e *Engine) evaluateArithmetic(source string, ctx *Context) (interface{}, bool, error) {
	// 按优先级从低到高处理
	for _, op := range []string{"+", "-"} {
		// 找最后一个操作符（左结合）
		idx := strings.LastIndex(source, op)
		if idx > 0 {
			left, err := e.evaluate(source[:idx], ctx)
			if err != nil {
				return nil, true, err
			}
			right, err := e.evaluate(source[idx+1:], ctx)
			if err != nil {
				return nil, true, err
			}

			leftNum := toFloat64(left)
			rightNum := toFloat64(right)

			switch op {
			case "+":
				return leftNum + rightNum, true, nil
			case "-":
				return leftNum - rightNum, true, nil
			}
		}
	}

	for _, op := range []string{"*", "/", "%"} {
		idx := strings.LastIndex(source, op)
		if idx > 0 {
			left, err := e.evaluate(source[:idx], ctx)
			if err != nil {
				return nil, true, err
			}
			right, err := e.evaluate(source[idx+1:], ctx)
			if err != nil {
				return nil, true, err
			}

			leftNum := toFloat64(left)
			rightNum := toFloat64(right)

			switch op {
			case "*":
				return leftNum * rightNum, true, nil
			case "/":
				if rightNum == 0 {
					return 0.0, true, nil
				}
				return leftNum / rightNum, true, nil
			case "%":
				if rightNum == 0 {
					return 0.0, true, nil
				}
				return float64(int64(leftNum) % int64(rightNum)), true, nil
			}
		}
	}

	return nil, false, nil
}

// docFieldPattern 匹配 doc['field'].value 或 doc.field 格式
var docFieldPattern = regexp.MustCompile(`doc\[['"]([^'"]+)['"]\]\.value|doc\.(\w+)`)
var paramsPattern = regexp.MustCompile(`params\[['"]([^'"]+)['"]\]|params\.(\w+)`)
var sourcePattern = regexp.MustCompile(`_source\[['"]([^'"]+)['"]\]|_source\.(\w+)`)

// evaluateFieldAccess 处理字段访问
func (e *Engine) evaluateFieldAccess(source string, ctx *Context) (interface{}, bool) {
	// 处理 doc['field'].value 或 doc.field
	if matches := docFieldPattern.FindStringSubmatch(source); len(matches) > 0 {
		field := matches[1]
		if field == "" {
			field = matches[2]
		}
		if ctx.Doc != nil {
			if val, ok := ctx.Doc[field]; ok {
				return val, true
			}
		}
		if ctx.Source != nil {
			if val, ok := ctx.Source[field]; ok {
				return val, true
			}
		}
		return nil, true
	}

	// 处理 params['key'] 或 params.key
	if matches := paramsPattern.FindStringSubmatch(source); len(matches) > 0 {
		key := matches[1]
		if key == "" {
			key = matches[2]
		}
		if ctx.Params != nil {
			if val, ok := ctx.Params[key]; ok {
				return val, true
			}
		}
		return nil, true
	}

	// 处理 _source['field'] 或 _source.field
	if matches := sourcePattern.FindStringSubmatch(source); len(matches) > 0 {
		field := matches[1]
		if field == "" {
			field = matches[2]
		}
		if ctx.Source != nil {
			if val, ok := ctx.Source[field]; ok {
				return val, true
			}
		}
		return nil, true
	}

	// 处理 _score
	if source == "_score" {
		return ctx.Score, true
	}

	return nil, false
}

// evaluateLiteral 处理字面量
func (e *Engine) evaluateLiteral(source string) (interface{}, bool) {
	// 布尔值
	if source == "true" {
		return true, true
	}
	if source == "false" {
		return false, true
	}

	// null
	if source == "null" {
		return nil, true
	}

	// 字符串（单引号或双引号）
	if (strings.HasPrefix(source, "'") && strings.HasSuffix(source, "'")) ||
		(strings.HasPrefix(source, "\"") && strings.HasSuffix(source, "\"")) {
		return source[1 : len(source)-1], true
	}

	// 数字
	if num, err := strconv.ParseFloat(source, 64); err == nil {
		return num, true
	}

	return nil, false
}

// evaluateAssignment 处理赋值语句（用于更新脚本）
func (e *Engine) evaluateAssignment(source string, ctx *Context) (interface{}, bool, error) {
	// 处理数组操作: ctx._source.tags.add('new_tag') 或 ctx._source.tags.remove('old_tag')
	if arrayOpResult, ok, err := e.evaluateArrayOperation(source, ctx); ok {
		return arrayOpResult, true, err
	}

	// 处理嵌套字段: ctx._source.nested.field = value
	if nestedResult, ok, err := e.evaluateNestedFieldAssignment(source, ctx); ok {
		return nestedResult, true, err
	}

	// 处理 ctx._source.field = value 格式
	if strings.HasPrefix(source, "ctx._source.") {
		parts := strings.SplitN(source, "=", 2)
		if len(parts) == 2 {
			field := strings.TrimPrefix(strings.TrimSpace(parts[0]), "ctx._source.")
			value, err := e.evaluate(strings.TrimSpace(parts[1]), ctx)
			if err != nil {
				return nil, true, err
			}
			if ctx.Source == nil {
				ctx.Source = make(map[string]interface{})
			}
			ctx.Source[field] = value
			return ctx.Source, true, nil
		}
	}

	// 处理复合赋值 ctx._source.field += value
	compoundOps := []string{"+=", "-=", "*=", "/="}
	for _, op := range compoundOps {
		if strings.Contains(source, "ctx._source.") && strings.Contains(source, op) {
			parts := strings.SplitN(source, op, 2)
			if len(parts) == 2 {
				field := strings.TrimPrefix(strings.TrimSpace(parts[0]), "ctx._source.")
				delta, err := e.evaluate(strings.TrimSpace(parts[1]), ctx)
				if err != nil {
					return nil, true, err
				}

				current := 0.0
				if ctx.Source != nil {
					if val, ok := ctx.Source[field]; ok {
						current = toFloat64(val)
					}
				} else {
					ctx.Source = make(map[string]interface{})
				}

				deltaNum := toFloat64(delta)
				var newVal float64
				switch op {
				case "+=":
					newVal = current + deltaNum
				case "-=":
					newVal = current - deltaNum
				case "*=":
					newVal = current * deltaNum
				case "/=":
					if deltaNum != 0 {
						newVal = current / deltaNum
					}
				}
				ctx.Source[field] = newVal
				return ctx.Source, true, nil
			}
		}
	}

	return nil, false, nil
}

// evaluateArrayOperation 处理数组操作
// 支持: ctx._source.tags.add('value'), ctx._source.tags.remove('value'), ctx._source.tags.contains('value')
func (e *Engine) evaluateArrayOperation(source string, ctx *Context) (interface{}, bool, error) {
	// 匹配 ctx._source.field.method(arg)
	arrayOpPattern := regexp.MustCompile(`ctx\._source\.(\w+)\.(add|remove|contains|clear|addAll|removeAll)\((.*)?\)`)
	matches := arrayOpPattern.FindStringSubmatch(source)
	if len(matches) < 3 {
		return nil, false, nil
	}

	field := matches[1]
	method := matches[2]
	argStr := ""
	if len(matches) > 3 {
		argStr = strings.TrimSpace(matches[3])
	}

	if ctx.Source == nil {
		ctx.Source = make(map[string]interface{})
	}

	// 获取当前数组
	var arr []interface{}
	if existing, ok := ctx.Source[field]; ok {
		if existingArr, ok := existing.([]interface{}); ok {
			arr = existingArr
		}
	}
	if arr == nil {
		arr = make([]interface{}, 0)
	}

	switch method {
	case "add":
		// 解析参数值
		arg, err := e.evaluate(argStr, ctx)
		if err != nil {
			return nil, true, err
		}
		arr = append(arr, arg)
		ctx.Source[field] = arr
		return true, true, nil

	case "remove":
		// 解析参数值
		arg, err := e.evaluate(argStr, ctx)
		if err != nil {
			return nil, true, err
		}
		newArr := make([]interface{}, 0, len(arr))
		for _, item := range arr {
			if item != arg {
				newArr = append(newArr, item)
			}
		}
		ctx.Source[field] = newArr
		return true, true, nil

	case "contains":
		arg, err := e.evaluate(argStr, ctx)
		if err != nil {
			return nil, true, err
		}
		for _, item := range arr {
			if item == arg {
				return true, true, nil
			}
		}
		return false, true, nil

	case "clear":
		ctx.Source[field] = make([]interface{}, 0)
		return true, true, nil

	case "addAll":
		// 解析数组参数
		arg, err := e.evaluate(argStr, ctx)
		if err != nil {
			return nil, true, err
		}
		if argArr, ok := arg.([]interface{}); ok {
			arr = append(arr, argArr...)
		}
		ctx.Source[field] = arr
		return true, true, nil

	case "removeAll":
		// 解析数组参数
		arg, err := e.evaluate(argStr, ctx)
		if err != nil {
			return nil, true, err
		}
		if argArr, ok := arg.([]interface{}); ok {
			toRemove := make(map[interface{}]bool)
			for _, item := range argArr {
				toRemove[item] = true
			}
			newArr := make([]interface{}, 0, len(arr))
			for _, item := range arr {
				if !toRemove[item] {
					newArr = append(newArr, item)
				}
			}
			ctx.Source[field] = newArr
		}
		return true, true, nil
	}

	return nil, false, nil
}

// evaluateNestedFieldAssignment 处理嵌套字段赋值
// 支持: ctx._source.nested.field = value, ctx._source.a.b.c = value
func (e *Engine) evaluateNestedFieldAssignment(source string, ctx *Context) (interface{}, bool, error) {
	if !strings.HasPrefix(source, "ctx._source.") {
		return nil, false, nil
	}

	// 检查是否有嵌套字段（包含多个点）
	parts := strings.SplitN(source, "=", 2)
	if len(parts) != 2 {
		return nil, false, nil
	}

	fieldPath := strings.TrimPrefix(strings.TrimSpace(parts[0]), "ctx._source.")
	fields := strings.Split(fieldPath, ".")

	// 如果只有一个字段，不是嵌套字段
	if len(fields) <= 1 {
		return nil, false, nil
	}

	// 解析值
	value, err := e.evaluate(strings.TrimSpace(parts[1]), ctx)
	if err != nil {
		return nil, true, err
	}

	if ctx.Source == nil {
		ctx.Source = make(map[string]interface{})
	}

	// 设置嵌套字段值
	setNestedField(ctx.Source, fields, value)
	return ctx.Source, true, nil
}

// setNestedField 设置嵌套字段值
func setNestedField(obj map[string]interface{}, fields []string, value interface{}) {
	if len(fields) == 0 {
		return
	}

	if len(fields) == 1 {
		obj[fields[0]] = value
		return
	}

	field := fields[0]
	if _, ok := obj[field]; !ok {
		obj[field] = make(map[string]interface{})
	}

	if nested, ok := obj[field].(map[string]interface{}); ok {
		setNestedField(nested, fields[1:], value)
	}
}

// getNestedField 获取嵌套字段值
func getNestedField(obj map[string]interface{}, fields []string) (interface{}, bool) {
	if len(fields) == 0 || obj == nil {
		return nil, false
	}

	if len(fields) == 1 {
		val, ok := obj[fields[0]]
		return val, ok
	}

	if nested, ok := obj[fields[0]].(map[string]interface{}); ok {
		return getNestedField(nested, fields[1:])
	}

	return nil, false
}

// compare 比较两个值
func compare(left, right interface{}, op string) bool {
	leftNum := toFloat64(left)
	rightNum := toFloat64(right)

	switch op {
	case "==":
		// 尝试字符串比较
		if leftStr, lok := left.(string); lok {
			if rightStr, rok := right.(string); rok {
				return leftStr == rightStr
			}
		}
		return leftNum == rightNum
	case "!=":
		if leftStr, lok := left.(string); lok {
			if rightStr, rok := right.(string); rok {
				return leftStr != rightStr
			}
		}
		return leftNum != rightNum
	case ">":
		return leftNum > rightNum
	case "<":
		return leftNum < rightNum
	case ">=":
		return leftNum >= rightNum
	case "<=":
		return leftNum <= rightNum
	}
	return false
}

// toBool 转换为布尔值
func toBool(v interface{}) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case float64:
		return val != 0
	case int:
		return val != 0
	case int64:
		return val != 0
	case string:
		return val != "" && val != "false"
	default:
		return true
	}
}

// toFloat64 转换为浮点数
func toFloat64(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case int32:
		return float64(val)
	case string:
		if num, err := strconv.ParseFloat(val, 64); err == nil {
			return num
		}
		return 0
	case bool:
		if val {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// toString 转换为字符串
func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case bool:
		return strconv.FormatBool(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}
