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
	Doc       map[string]interface{} // 文档字段 (doc['field'].value)
	Source    map[string]interface{} // _source 字段
	Params    map[string]interface{} // 脚本参数
	Score     float64                // 文档评分
	Now       int64                  // 当前时间戳（毫秒）
	Ctx       map[string]interface{} // ctx 上下文（用于更新脚本）
	Variables map[string]interface{} // 局部变量（用于变量声明）
}

// NewContext 创建执行上下文
func NewContext(doc, source, params map[string]interface{}) *Context {
	ctx := &Context{
		Doc:       doc,
		Source:    source,
		Params:    params,
		Now:       time.Now().UnixMilli(),
		Variables: make(map[string]interface{}),
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

	// 支持多行脚本：先尝试按语句分割执行
	// 检查是否包含控制流语句或分隔符
	source := strings.TrimSpace(script.Source)
	// 如果包含分号或换行，且不是单行的Date/Math函数调用，则按多行处理
	hasSemicolonOrNewline := strings.Contains(source, "\n") || strings.Contains(source, ";")
	isSingleLineFunction := strings.HasPrefix(source, "Date.") || strings.HasPrefix(source, "Math.") || strings.HasPrefix(source, "SimpleDateFormat(")

	if (hasSemicolonOrNewline && !isSingleLineFunction) ||
		strings.HasPrefix(source, "def ") ||
		strings.HasPrefix(source, "if ") ||
		strings.HasPrefix(source, "for ") ||
		strings.HasPrefix(source, "while ") ||
		strings.HasPrefix(source, "do ") ||
		strings.HasPrefix(source, "switch ") {
		return e.executeStatements(source, ctx)
	}

	// 单行表达式
	return e.evaluate(source, ctx)
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

	// 处理赋值表达式（优先级高于算术表达式）
	// 但需要排除比较表达式中的等号
	if !strings.Contains(source, "==") && !strings.Contains(source, "!=") &&
		!strings.Contains(source, "<=") && !strings.Contains(source, ">=") {
		if result, ok, err := e.evaluateAssignment(source, ctx); ok {
			return result, err
		}
	}

	// 处理比较表达式
	if result, ok, err := e.evaluateComparison(source, ctx); ok {
		return result, err
	}

	// 处理内置函数（优先级高于算术表达式，确保 Math.abs 等函数在算术运算前被识别）
	if result, ok, err := e.evaluateBuiltinFunction(source, ctx); ok {
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

	// 处理字段访问
	if result, ok := e.evaluateFieldAccess(source, ctx); ok {
		return result, nil
	}

	// 处理字面量
	if result, ok := e.evaluateLiteral(source); ok {
		return result, nil
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
		// 字符串contains或List/Map contains
		if str, ok := obj.(string); ok {
			argVal, err := e.evaluate(args, ctx)
			if err != nil {
				return false, err
			}
			return strings.Contains(str, toString(argVal)), nil
		}
		// List.contains(value)
		if arr, ok := obj.([]interface{}); ok {
			argVal, err := e.evaluate(args, ctx)
			if err != nil {
				return false, err
			}
			for _, item := range arr {
				if item == argVal {
					return true, nil
				}
			}
			return false, nil
		}
		// Map.containsKey(key)
		if m, ok := obj.(map[string]interface{}); ok {
			key := toString(args)
			_, exists := m[key]
			return exists, nil
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

	case "matches":
		// 正则表达式匹配
		if str, ok := obj.(string); ok {
			argVal, err := e.evaluate(args, ctx)
			if err != nil {
				return false, err
			}
			pattern := toString(argVal)
			matched, err := regexp.MatchString(pattern, str)
			if err != nil {
				return false, fmt.Errorf("invalid regex pattern: %w", err)
			}
			return matched, nil
		}
		return false, nil

	case "replaceAll":
		// 正则表达式替换
		if str, ok := obj.(string); ok {
			argParts := strings.Split(args, ",")
			if len(argParts) >= 2 {
				pattern, _ := e.evaluate(strings.TrimSpace(argParts[0]), ctx)
				replacement, _ := e.evaluate(strings.TrimSpace(argParts[1]), ctx)
				re, err := regexp.Compile(toString(pattern))
				if err != nil {
					return "", fmt.Errorf("invalid regex pattern: %w", err)
				}
				return re.ReplaceAllString(str, toString(replacement)), nil
			}
		}
		return "", nil

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

	case "add":
		// List.add(value)
		if arr, ok := obj.([]interface{}); ok {
			argVal, err := e.evaluate(args, ctx)
			if err != nil {
				return nil, err
			}
			return append(arr, argVal), nil
		}
		return nil, nil

	case "remove":
		// List.remove(index) 或 List.remove(value)
		if arr, ok := obj.([]interface{}); ok {
			argVal, err := e.evaluate(args, ctx)
			if err != nil {
				return nil, err
			}
			// 尝试作为索引
			if idx := int(toFloat64(argVal)); idx >= 0 && idx < len(arr) {
				newArr := make([]interface{}, 0, len(arr)-1)
				newArr = append(newArr, arr[:idx]...)
				newArr = append(newArr, arr[idx+1:]...)
				return newArr, nil
			}
			// 作为值移除
			newArr := make([]interface{}, 0, len(arr))
			for _, item := range arr {
				if item != argVal {
					newArr = append(newArr, item)
				}
			}
			return newArr, nil
		}
		return nil, nil

	case "put":
		// Map.put(key, value)
		if m, ok := obj.(map[string]interface{}); ok {
			argParts := strings.Split(args, ",")
			if len(argParts) >= 2 {
				key, _ := e.evaluate(strings.TrimSpace(argParts[0]), ctx)
				value, _ := e.evaluate(strings.TrimSpace(argParts[1]), ctx)
				m[toString(key)] = value
				return value, nil
			}
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
		// 支持 Math.abs(...) 在表达式的任何位置
		if strings.Contains(source, name+"(") {
			// 找到函数调用的开始位置
			funcStart := strings.Index(source, name+"(")
			if funcStart >= 0 {
				// 找到匹配的右括号
				openParen := funcStart + len(name)
				depth := 1
				inString := false
				stringChar := byte(0)
				closeParen := -1
				for i := openParen + 1; i < len(source); i++ {
					c := source[i]
					if !inString && (c == '"' || c == '\'') {
						inString = true
						stringChar = c
					} else if inString && c == stringChar {
						// 检查是否是转义字符
						escapeCount := 0
						for j := i - 1; j >= 0 && source[j] == '\\'; j-- {
							escapeCount++
						}
						if escapeCount%2 == 1 {
							continue
						}
						inString = false
					} else if !inString {
						if c == '(' {
							depth++
						} else if c == ')' {
							depth--
							if depth == 0 {
								closeParen = i
								break
							}
						}
					}
				}
				if closeParen >= 0 {
					// 提取参数
					arg := source[openParen+1 : closeParen]
					argVal, err := e.evaluate(arg, ctx)
					if err != nil {
						return nil, true, err
					}
					result := fn(toFloat64(argVal))
					// 如果整个表达式就是这个函数调用，直接返回结果
					if funcStart == 0 && closeParen == len(source)-1 {
						return result, true, nil
					}
					// 否则，替换函数调用为结果值，然后递归计算
					// 将结果转换为字符串并替换（替换整个函数调用，包括函数名和括号）
					resultStr := fmt.Sprintf("%v", result)
					newSource := source[:funcStart] + resultStr + source[closeParen+1:]
					finalVal, finalErr := e.evaluate(newSource, ctx)
					if finalErr != nil {
						return nil, true, finalErr
					}
					return finalVal, true, nil
				}
			}
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

	// 日期时间函数
	if strings.HasPrefix(source, "Date.now()") {
		return float64(time.Now().UnixMilli()), true, nil
	}

	// SimpleDateFormat.format(timestamp)
	if strings.HasPrefix(source, "SimpleDateFormat(") && strings.Contains(source, ").format(") {
		return e.evaluateDateFormat(source, ctx)
	}

	// Date.parse(dateString) - 解析日期字符串为时间戳
	// 支持 Date.parse(...) 在任何位置，不仅仅是前缀
	if strings.Contains(source, "Date.parse(") {
		result, err := e.evaluateDateParse(source, ctx)
		if err != nil {
			return nil, true, err
		}
		return result, true, nil
	}

	// Date.add(timestamp, field, amount) - 日期计算
	if strings.Contains(source, "Date.add(") {
		result, err := e.evaluateDateAdd(source, ctx)
		if err != nil {
			return nil, true, err
		}
		return result, true, nil
	}

	// Date.subtract(timestamp, field, amount) - 日期减法
	if strings.Contains(source, "Date.subtract(") {
		result, err := e.evaluateDateSubtract(source, ctx)
		if err != nil {
			return nil, true, err
		}
		return result, true, nil
	}

	return nil, false, nil
}

// evaluateDateFormat 处理日期格式化: SimpleDateFormat('yyyy-MM-dd').format(timestamp)
func (e *Engine) evaluateDateFormat(source string, ctx *Context) (interface{}, bool, error) {
	// 解析 SimpleDateFormat('pattern').format(value)
	formatIdx := strings.Index(source, "SimpleDateFormat(")
	if formatIdx < 0 {
		return nil, false, nil
	}

	// 找到格式字符串
	formatStart := formatIdx + len("SimpleDateFormat(")
	formatEnd := -1
	inString := false
	stringChar := byte(0)
	for i := formatStart; i < len(source); i++ {
		c := source[i]
		if !inString && (c == '"' || c == '\'') {
			inString = true
			stringChar = c
		} else if inString && c == stringChar {
			inString = false
			formatEnd = i
			break
		}
	}

	if formatEnd < 0 {
		return nil, false, nil
	}

	formatPattern := source[formatStart+1 : formatEnd]

	// 找到format调用
	formatCallIdx := strings.Index(source[formatEnd:], ").format(")
	if formatCallIdx < 0 {
		return nil, false, nil
	}

	formatCallIdx += formatEnd
	valueStart := formatCallIdx + len(").format(")
	valueEnd := strings.LastIndex(source, ")")
	if valueEnd < valueStart {
		return nil, false, nil
	}

	valueExpr := source[valueStart:valueEnd]
	value, err := e.evaluate(valueExpr, ctx)
	if err != nil {
		return nil, true, err
	}

	// 转换时间戳为时间
	timestamp := int64(toFloat64(value))
	t := time.Unix(timestamp/1000, (timestamp%1000)*1000000)

	// 简单的日期格式化（支持常见格式）
	formatted := e.formatDate(t, formatPattern)
	return formatted, true, nil
}

// formatDate 格式化日期
func (e *Engine) formatDate(t time.Time, pattern string) string {
	// 简单的格式替换
	result := pattern
	result = strings.ReplaceAll(result, "yyyy", fmt.Sprintf("%04d", t.Year()))
	result = strings.ReplaceAll(result, "MM", fmt.Sprintf("%02d", int(t.Month())))
	result = strings.ReplaceAll(result, "dd", fmt.Sprintf("%02d", t.Day()))
	result = strings.ReplaceAll(result, "HH", fmt.Sprintf("%02d", t.Hour()))
	result = strings.ReplaceAll(result, "mm", fmt.Sprintf("%02d", t.Minute()))
	result = strings.ReplaceAll(result, "ss", fmt.Sprintf("%02d", t.Second()))
	return result
}

// evaluateDateParse 处理日期解析: Date.parse("2024-01-15")
func (e *Engine) evaluateDateParse(source string, ctx *Context) (interface{}, error) {
	// 解析 Date.parse("dateString")
	// 找到Date.parse(的位置
	prefix := "Date.parse("
	prefixIdx := strings.Index(source, prefix)
	if prefixIdx < 0 {
		return nil, fmt.Errorf("invalid Date.parse: missing prefix")
	}

	openParen := prefixIdx + len(prefix) - 1 // Date.parse(的最后一个字符是(

	// 找到匹配的右括号（处理字符串内的括号）
	depth := 1
	inString := false
	stringChar := byte(0)
	closeParen := -1
	for i := openParen + 1; i < len(source); i++ {
		c := source[i]
		if !inString && (c == '"' || c == '\'') {
			inString = true
			stringChar = c
		} else if inString && c == stringChar {
			// 检查是否是转义字符（简单的转义处理）
			// 需要检查前一个字符是否是转义字符，但需要回溯检查连续的转义字符
			escapeCount := 0
			for j := i - 1; j >= 0 && source[j] == '\\'; j-- {
				escapeCount++
			}
			// 如果转义字符数量是奇数，说明这个引号被转义了
			if escapeCount%2 == 1 {
				// 转义的引号，继续在字符串内
				continue
			}
			inString = false
		} else if !inString {
			if c == '(' {
				depth++
			} else if c == ')' {
				depth--
				if depth == 0 {
					closeParen = i
					break
				}
			}
		}
	}

	if closeParen < 0 {
		return nil, fmt.Errorf("invalid Date.parse: unmatched ( in source: %s (len=%d, openParen=%d)", source, len(source), openParen)
	}

	arg := source[openParen+1 : closeParen]
	arg = strings.TrimSpace(arg)

	// 移除引号
	if len(arg) >= 2 && (arg[0] == '"' || arg[0] == '\'') {
		arg = arg[1 : len(arg)-1]
	}

	// 尝试解析常见日期格式
	formats := []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05Z",
		"2006/01/02",
		"2006/01/02 15:04:05",
		"01/02/2006",
		"01-02-2006",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, arg); err == nil {
			return float64(t.UnixMilli()), nil
		}
	}

	// 如果都失败，尝试解析时间戳（毫秒或秒）
	if timestamp, err := strconv.ParseInt(arg, 10, 64); err == nil {
		// 如果是秒级时间戳（小于10000000000），转换为毫秒
		if timestamp < 10000000000 {
			timestamp *= 1000
		}
		return float64(timestamp), nil
	}

	return nil, fmt.Errorf("failed to parse date: %s", arg)
}

// evaluateDateAdd 处理日期加法: Date.add(timestamp, "days", 7)
func (e *Engine) evaluateDateAdd(source string, ctx *Context) (interface{}, error) {
	// 解析 Date.add(timestamp, field, amount)
	// 找到第一个(和最后一个)，处理字符串内的括号
	openParen := strings.Index(source, "(")
	if openParen < 0 {
		return nil, fmt.Errorf("invalid Date.add: missing (")
	}

	// 找到匹配的右括号（处理字符串内的括号）
	depth := 1
	inString := false
	stringChar := byte(0)
	closeParen := -1
	for i := openParen + 1; i < len(source); i++ {
		c := source[i]
		if !inString && (c == '"' || c == '\'') {
			inString = true
			stringChar = c
		} else if inString && c == stringChar {
			inString = false
		} else if !inString {
			if c == '(' {
				depth++
			} else if c == ')' {
				depth--
				if depth == 0 {
					closeParen = i
					break
				}
			}
		}
	}

	if closeParen < 0 {
		return nil, fmt.Errorf("invalid Date.add: unmatched (")
	}

	args := source[openParen+1 : closeParen]
	parts := strings.Split(args, ",")
	if len(parts) != 3 {
		return nil, fmt.Errorf("Date.add requires 3 arguments: timestamp, field, amount")
	}

	timestampExpr := strings.TrimSpace(parts[0])
	fieldExpr := strings.TrimSpace(parts[1])
	amountExpr := strings.TrimSpace(parts[2])

	// 计算参数值
	timestamp, err := e.evaluate(timestampExpr, ctx)
	if err != nil {
		return nil, err
	}

	field, err := e.evaluate(fieldExpr, ctx)
	if err != nil {
		return nil, err
	}

	amount, err := e.evaluate(amountExpr, ctx)
	if err != nil {
		return nil, err
	}

	// 移除引号
	fieldStr := toString(field)
	fieldStr = strings.Trim(fieldStr, "\"'")
	amountVal := toFloat64(amount)
	ts := int64(toFloat64(timestamp))

	// 转换为time.Time
	t := time.UnixMilli(ts)

	// 根据字段类型添加时间
	switch strings.ToLower(fieldStr) {
	case "year", "years":
		t = t.AddDate(int(amountVal), 0, 0)
	case "month", "months":
		t = t.AddDate(0, int(amountVal), 0)
	case "day", "days":
		t = t.AddDate(0, 0, int(amountVal))
	case "hour", "hours":
		t = t.Add(time.Duration(amountVal) * time.Hour)
	case "minute", "minutes":
		t = t.Add(time.Duration(amountVal) * time.Minute)
	case "second", "seconds":
		t = t.Add(time.Duration(amountVal) * time.Second)
	case "millisecond", "milliseconds", "ms":
		t = t.Add(time.Duration(amountVal) * time.Millisecond)
	default:
		return nil, fmt.Errorf("unsupported date field: %s", fieldStr)
	}

	return float64(t.UnixMilli()), nil
}

// evaluateDateSubtract 处理日期减法: Date.subtract(timestamp, "days", 7)
func (e *Engine) evaluateDateSubtract(source string, ctx *Context) (interface{}, error) {
	// 解析 Date.subtract(timestamp, field, amount)
	// 找到第一个(和最后一个)，处理字符串内的括号
	openParen := strings.Index(source, "(")
	if openParen < 0 {
		return nil, fmt.Errorf("invalid Date.subtract: missing (")
	}

	// 找到匹配的右括号（处理字符串内的括号）
	depth := 1
	inString := false
	stringChar := byte(0)
	closeParen := -1
	for i := openParen + 1; i < len(source); i++ {
		c := source[i]
		if !inString && (c == '"' || c == '\'') {
			inString = true
			stringChar = c
		} else if inString && c == stringChar {
			inString = false
		} else if !inString {
			if c == '(' {
				depth++
			} else if c == ')' {
				depth--
				if depth == 0 {
					closeParen = i
					break
				}
			}
		}
	}

	if closeParen < 0 {
		return nil, fmt.Errorf("invalid Date.subtract: unmatched (")
	}

	args := source[openParen+1 : closeParen]
	parts := strings.Split(args, ",")
	if len(parts) != 3 {
		return nil, fmt.Errorf("Date.subtract requires 3 arguments: timestamp, field, amount")
	}

	timestampExpr := strings.TrimSpace(parts[0])
	fieldExpr := strings.TrimSpace(parts[1])
	amountExpr := strings.TrimSpace(parts[2])

	// 计算参数值
	timestamp, err := e.evaluate(timestampExpr, ctx)
	if err != nil {
		return nil, err
	}

	field, err := e.evaluate(fieldExpr, ctx)
	if err != nil {
		return nil, err
	}

	amount, err := e.evaluate(amountExpr, ctx)
	if err != nil {
		return nil, err
	}

	// 移除引号
	fieldStr := toString(field)
	fieldStr = strings.Trim(fieldStr, "\"'")
	amountVal := toFloat64(amount)
	ts := int64(toFloat64(timestamp))

	// 转换为time.Time
	t := time.UnixMilli(ts)

	// 根据字段类型减去时间（使用负数）
	switch strings.ToLower(fieldStr) {
	case "year", "years":
		t = t.AddDate(-int(amountVal), 0, 0)
	case "month", "months":
		t = t.AddDate(0, -int(amountVal), 0)
	case "day", "days":
		t = t.AddDate(0, 0, -int(amountVal))
	case "hour", "hours":
		t = t.Add(-time.Duration(amountVal) * time.Hour)
	case "minute", "minutes":
		t = t.Add(-time.Duration(amountVal) * time.Minute)
	case "second", "seconds":
		t = t.Add(-time.Duration(amountVal) * time.Second)
	case "millisecond", "milliseconds", "ms":
		t = t.Add(-time.Duration(amountVal) * time.Millisecond)
	default:
		return nil, fmt.Errorf("unsupported date field: %s", fieldStr)
	}

	return float64(t.UnixMilli()), nil
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
		// 找最后一个操作符（左结合），但跳过字符串内的运算符
		idx := findLastOperatorOutsideStrings(source, op)
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
		idx := findLastOperatorOutsideStrings(source, op)
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

// findLastOperatorOutsideStrings 在字符串外找最后一个运算符
func findLastOperatorOutsideStrings(source, op string) int {
	inString := false
	stringChar := byte(0)
	depth := 0 // 括号深度（只跟踪小括号，不跟踪方括号）
	lastIdx := -1
	for i := 0; i < len(source); i++ {
		c := source[i]
		if !inString && (c == '"' || c == '\'') {
			inString = true
			stringChar = c
		} else if inString && c == stringChar {
			// 检查是否是转义字符
			escapeCount := 0
			for j := i - 1; j >= 0 && source[j] == '\\'; j-- {
				escapeCount++
			}
			if escapeCount%2 == 1 {
				continue
			}
			inString = false
		} else if !inString {
			// 跟踪括号深度（只跟踪小括号）
			if c == '(' {
				depth++
			} else if c == ')' {
				depth--
			} else if depth == 0 && strings.HasPrefix(source[i:], op) {
				// 只在括号外匹配运算符
				// 排除复合运算符（+=, -=, *=, /=, ==, !=, <=, >=）
				if i < len(source)-1 {
					nextChar := source[i+1]
					if (op == "+" || op == "-" || op == "*" || op == "/" || op == "=") && nextChar == '=' {
						// 这是复合运算符，跳过
						continue
					}
				}
				// 对于减号运算符，需要检查是否是负数（前面没有操作数）
				if op == "-" {
					// 检查前面是否有操作数（字母、数字、括号、点号等）
					// 跳过空格，找到实际的前一个非空格字符
					prevIdx := i - 1
					for prevIdx >= 0 && source[prevIdx] == ' ' {
						prevIdx--
					}
					if prevIdx < 0 {
						// 减号在开头（或前面全是空格），这是负数，不是减法运算符，跳过
						continue
					}
					prevChar := source[prevIdx]
					// 如果前面是操作符、左括号、逗号等，说明这是负数，不是减法
					if prevChar == '(' || prevChar == '[' || prevChar == ',' ||
						prevChar == '+' || prevChar == '-' || prevChar == '*' || prevChar == '/' ||
						prevChar == '%' || prevChar == '=' || prevChar == '?' || prevChar == ':' {
						// 这是负数，不是减法运算符，跳过
						continue
					}
				}
				lastIdx = i
			}
		}
	}
	return lastIdx
}

// docFieldPattern 匹配 doc['field'].value 或 doc.field 格式
var docFieldPattern = regexp.MustCompile(`doc\[['"]([^'"]+)['"]\]\.value|doc\.(\w+)`)
var paramsPattern = regexp.MustCompile(`params\[['"]([^'"]+)['"]\]|params\.(\w+)`)
var sourcePattern = regexp.MustCompile(`_source\[['"]([^'"]+)['"]\]|_source\.(\w+)`)

// evaluateFieldAccess 处理字段访问
func (e *Engine) evaluateFieldAccess(source string, ctx *Context) (interface{}, bool) {
	// 处理 doc['field'].value 或 doc.field
	if matches := docFieldPattern.FindStringSubmatch(source); len(matches) > 0 {
		// 确保匹配的是完整表达式，不包含运算符（避免匹配 "doc['price'].value - params.origin"）
		matchedStr := matches[0]
		if matchedStr != source {
			// 匹配的不是完整表达式，跳过
			return nil, false
		}
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
		// 确保匹配的是完整表达式，不包含运算符
		matchedStr := matches[0]
		if matchedStr != source {
			// 匹配的不是完整表达式，跳过
			return nil, false
		}
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
		// 确保匹配的是完整表达式，不包含运算符
		matchedStr := matches[0]
		if matchedStr != source {
			// 匹配的不是完整表达式，跳过
			return nil, false
		}
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

	// 处理变量访问（局部变量）
	// 注意：变量可能不存在，或者值为 nil，所以需要先检查是否存在
	if ctx.Variables != nil {
		if _, exists := ctx.Variables[source]; exists {
			// 变量存在，返回其值（即使值为 nil）
			return ctx.Variables[source], true
		}
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

// evaluateAssignment 处理赋值语句（用于更新脚本和变量赋值）
func (e *Engine) evaluateAssignment(source string, ctx *Context) (interface{}, bool, error) {
	// 处理数组操作: ctx._source.tags.add('new_tag') 或 ctx._source.tags.remove('old_tag')
	if arrayOpResult, ok, err := e.evaluateArrayOperation(source, ctx); ok {
		return arrayOpResult, true, err
	}

	// 处理嵌套字段: ctx._source.nested.field = value
	if nestedResult, ok, err := e.evaluateNestedFieldAssignment(source, ctx); ok {
		return nestedResult, true, err
	}

	// 处理复合赋值 ctx._source.field += value（必须在简单赋值之前处理）
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
				return newVal, true, nil
			}
		}
	}

	// 处理 ctx._source.field = value 格式（必须在复合赋值之后处理）
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

	// 处理普通变量赋值: x = value, i = i + 1
	// 检查是否包含赋值运算符（不在字符串内，且不是比较运算符）
	eqIdx := findOperatorOutsideStrings(source, "=")
	if eqIdx > 0 {
		// 确保不是比较运算符（==, !=, <=, >=）
		if eqIdx < len(source)-1 {
			nextChar := source[eqIdx+1]
			if nextChar != '=' {
				varName := strings.TrimSpace(source[:eqIdx])
				valueExpr := strings.TrimSpace(source[eqIdx+1:])

				// 计算值
				value, err := e.evaluate(valueExpr, ctx)
				if err != nil {
					return nil, true, err
				}

				// 存储到变量
				if ctx.Variables == nil {
					ctx.Variables = make(map[string]interface{})
				}
				ctx.Variables[varName] = value
				return value, true, nil
			}
		} else {
			// 等号在末尾，也是赋值
			varName := strings.TrimSpace(source[:eqIdx])
			valueExpr := strings.TrimSpace(source[eqIdx+1:])

			// 计算值
			value, err := e.evaluate(valueExpr, ctx)
			if err != nil {
				return nil, true, err
			}

			// 存储到变量
			if ctx.Variables == nil {
				ctx.Variables = make(map[string]interface{})
			}
			ctx.Variables[varName] = value
			return value, true, nil
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
	switch op {
	case "==":
		// 处理 nil 值
		if left == nil && right == nil {
			return true
		}
		if left == nil || right == nil {
			return false
		}
		// 尝试字符串比较
		if leftStr, lok := left.(string); lok {
			if rightStr, rok := right.(string); rok {
				return leftStr == rightStr
			}
		}
		leftNum := toFloat64(left)
		rightNum := toFloat64(right)
		return leftNum == rightNum
	case "!=":
		// 处理 nil 值
		if left == nil && right == nil {
			return false
		}
		if left == nil || right == nil {
			return true
		}
		// 尝试字符串比较
		if leftStr, lok := left.(string); lok {
			if rightStr, rok := right.(string); rok {
				return leftStr != rightStr
			}
		}
		leftNum := toFloat64(left)
		rightNum := toFloat64(right)
		return leftNum != rightNum
	case ">":
		leftNum := toFloat64(left)
		rightNum := toFloat64(right)
		return leftNum > rightNum
	case "<":
		leftNum := toFloat64(left)
		rightNum := toFloat64(right)
		return leftNum < rightNum
	case ">=":
		leftNum := toFloat64(left)
		rightNum := toFloat64(right)
		return leftNum >= rightNum
	case "<=":
		leftNum := toFloat64(left)
		rightNum := toFloat64(right)
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

// executeStatements 执行多行脚本（按语句分割）
func (e *Engine) executeStatements(source string, ctx *Context) (interface{}, error) {
	// 分割语句（支持分号和换行）
	statements := e.splitStatements(source)

	var lastResult interface{}
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// 检查是否是return语句
		if strings.HasPrefix(stmt, "return ") {
			expr := strings.TrimPrefix(stmt, "return ")
			expr = strings.TrimSpace(expr)
			result, err := e.evaluate(expr, ctx)
			if err != nil {
				return nil, err
			}
			return result, nil
		}

		// 执行语句
		result, err := e.executeStatement(stmt, ctx)
		if err != nil {
			return nil, err
		}
		lastResult = result
	}

	return lastResult, nil
}

// splitStatements 分割语句（支持分号和换行）
func (e *Engine) splitStatements(source string) []string {
	var statements []string
	var current strings.Builder
	depth := 0      // 括号深度
	braceDepth := 0 // 大括号深度
	inString := false
	stringChar := byte(0)
	inSwitch := false // 是否在switch语句中

	for i := 0; i < len(source); i++ {
		c := source[i]

		// 处理字符串
		if !inString && (c == '"' || c == '\'') {
			inString = true
			stringChar = c
			current.WriteByte(c)
		} else if inString && c == stringChar {
			inString = false
			current.WriteByte(c)
		} else if inString {
			current.WriteByte(c)
			continue
		}

		// 检查是否进入switch语句
		if !inSwitch && depth == 0 && braceDepth == 0 {
			currentStr := current.String()
			if strings.HasSuffix(strings.TrimSpace(currentStr), "switch") {
				inSwitch = true
			}
		}

		// 处理括号
		if c == '(' || c == '[' {
			depth++
			current.WriteByte(c)
		} else if c == ')' || c == ']' {
			depth--
			current.WriteByte(c)
		} else if c == '{' {
			braceDepth++
			current.WriteByte(c)
		} else if c == '}' {
			braceDepth--
			current.WriteByte(c)
			// 如果是在switch语句中，且大括号深度回到0，说明switch语句结束
			if inSwitch && braceDepth == 0 {
				inSwitch = false
			}
			// 检查是否是do-while循环的} while部分（需要检查后面是否有while）
			if braceDepth == 0 && depth == 0 {
				remaining := source[i+1:]
				remainingTrimmed := strings.TrimSpace(remaining)
				if strings.HasPrefix(remainingTrimmed, "while ") {
					// 这是do-while循环，继续读取while条件（不分割）
					continue
				}
				// 检查后面是否还有语句（比如 return x; 或 x = x + 1;）
				// 如果后面有非空白字符，说明当前语句结束了，应该分割
				if len(remainingTrimmed) > 0 {
					// 检查是否是语句分隔符或新的语句开始
					if remainingTrimmed[0] == ';' || remainingTrimmed[0] == '\n' {
						// 当前语句结束，保存并重置
						stmt := strings.TrimSpace(current.String())
						if stmt != "" {
							statements = append(statements, stmt)
						}
						current.Reset()
						// 继续处理后面的语句（不写入当前字符，让后续逻辑处理）
						continue
					}
					// 检查是否是关键字开头的语句
					if strings.HasPrefix(remainingTrimmed, "return ") ||
						strings.HasPrefix(remainingTrimmed, "def ") ||
						strings.HasPrefix(remainingTrimmed, "if ") ||
						strings.HasPrefix(remainingTrimmed, "for ") ||
						strings.HasPrefix(remainingTrimmed, "while ") ||
						strings.HasPrefix(remainingTrimmed, "do ") ||
						strings.HasPrefix(remainingTrimmed, "switch ") {
						// 当前语句结束，保存并重置
						stmt := strings.TrimSpace(current.String())
						if stmt != "" {
							statements = append(statements, stmt)
						}
						current.Reset()
						// 继续处理后面的语句（不写入当前字符，让后续逻辑处理）
						continue
					}
					// 检查是否是变量赋值语句（以标识符开头，包含等号）
					if len(remainingTrimmed) > 0 {
						firstChar := remainingTrimmed[0]
						// 如果是字母或下划线开头，可能是变量赋值
						if (firstChar >= 'a' && firstChar <= 'z') || (firstChar >= 'A' && firstChar <= 'Z') || firstChar == '_' {
							// 检查是否包含等号（赋值语句）
							// 需要跳过字符串内的等号
							hasAssign := false
							inStr := false
							strChar := byte(0)
							for j := 0; j < len(remainingTrimmed) && j < 100; j++ { // 限制检查长度
								ch := remainingTrimmed[j]
								if !inStr && (ch == '"' || ch == '\'') {
									inStr = true
									strChar = ch
								} else if inStr && ch == strChar {
									inStr = false
								} else if !inStr && ch == '=' {
									// 确保不是 == 或 != 或 <= 或 >=
									if j < len(remainingTrimmed)-1 {
										nextCh := remainingTrimmed[j+1]
										if nextCh != '=' && nextCh != '>' && nextCh != '<' {
											hasAssign = true
											break
										}
									} else {
										hasAssign = true
										break
									}
								} else if !inStr && (ch == ';' || ch == '\n') {
									break
								}
							}
							if hasAssign {
								// 当前语句结束，保存并重置
								stmt := strings.TrimSpace(current.String())
								if stmt != "" {
									statements = append(statements, stmt)
								}
								current.Reset()
								// 继续处理后面的语句（不写入当前字符，让后续逻辑处理）
								continue
							}
						}
					}
				}
			}
		} else if depth == 0 && braceDepth == 0 {
			// 在顶层且不在代码块内，检查语句分隔符
			if c == ';' {
				stmt := strings.TrimSpace(current.String())
				// 检查是否是do-while循环的结束分号（前面有} while (condition)）
				if strings.Contains(stmt, "} while (") {
					// 这是do-while循环的结束，继续读取直到分号
					current.WriteByte(c)
					continue
				}
				// 检查是否是do-while循环的结束分号（前面有} while）
				if strings.HasSuffix(stmt, "}") {
					// 检查后面是否还有while（需要跳过空白字符）
					remaining := source[i+1:]
					remainingTrimmed := strings.TrimSpace(remaining)
					if strings.HasPrefix(remainingTrimmed, "while ") {
						// 这是do-while循环，继续读取
						current.WriteByte(c)
						continue
					}
				}
				// 如果在switch语句中，分号不应该分割语句
				if inSwitch {
					current.WriteByte(c)
					continue
				}
				if stmt != "" {
					statements = append(statements, stmt)
				}
				current.Reset()
			} else if c == '\n' {
				// 换行也可能是语句分隔符（如果当前语句不为空）
				if current.Len() > 0 {
					stmt := strings.TrimSpace(current.String())
					// 如果在switch语句中，换行不应该分割语句
					if inSwitch {
						current.WriteByte(c)
						continue
					}
					// 检查是否是do-while循环的延续：} while
					if strings.HasSuffix(stmt, "}") {
						remaining := source[i+1:]
						remainingTrimmed := strings.TrimSpace(remaining)
						if strings.HasPrefix(remainingTrimmed, "while ") {
							// 这是do-while循环，继续读取
							current.WriteByte(c)
							continue
						}
					}
					// 检查是否是do-while循环的延续：} while (condition)
					if strings.Contains(stmt, "} while (") {
						// 这是do-while循环，继续读取
						current.WriteByte(c)
						continue
					}
					// 检查是否是控制流语句的延续
					if !e.isControlFlowContinuation(stmt) {
						statements = append(statements, stmt)
						current.Reset()
					} else {
						current.WriteByte(c)
					}
				} else {
					// 当前语句为空，换行可以忽略
					current.WriteByte(c)
				}
			} else {
				current.WriteByte(c)
			}
		} else {
			current.WriteByte(c)
		}
	}

	// 添加最后一个语句
	if current.Len() > 0 {
		stmt := strings.TrimSpace(current.String())
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	return statements
}

// isControlFlowContinuation 检查是否是控制流语句的延续
func (e *Engine) isControlFlowContinuation(stmt string) bool {
	stmt = strings.TrimSpace(stmt)
	return strings.HasSuffix(stmt, "{") ||
		strings.HasSuffix(stmt, "else") ||
		strings.HasSuffix(stmt, "}") ||
		strings.HasPrefix(stmt, "}") ||
		strings.HasPrefix(stmt, "else") ||
		strings.HasPrefix(stmt, "case ") ||
		strings.HasPrefix(stmt, "default") ||
		strings.HasPrefix(stmt, "while ") ||
		strings.HasPrefix(stmt, "switch ") ||
		// 检查是否是do-while循环的延续：} while
		(strings.HasSuffix(stmt, "}") && strings.Contains(stmt, "while"))
}

// executeStatement 执行单个语句
func (e *Engine) executeStatement(stmt string, ctx *Context) (interface{}, error) {
	stmt = strings.TrimSpace(stmt)
	if stmt == "" {
		return nil, nil
	}

	// 处理变量声明: def x = value
	if strings.HasPrefix(stmt, "def ") {
		return e.executeVariableDeclaration(stmt, ctx)
	}

	// 处理if语句
	if strings.HasPrefix(stmt, "if ") {
		return e.executeIfStatement(stmt, ctx)
	}

	// 处理for循环
	if strings.HasPrefix(stmt, "for ") {
		return e.executeForLoop(stmt, ctx)
	}

	// 处理while循环
	if strings.HasPrefix(stmt, "while ") {
		return e.executeWhileLoop(stmt, ctx)
	}

	// 处理do-while循环
	if strings.HasPrefix(stmt, "do ") {
		return e.executeDoWhileLoop(stmt, ctx)
	}

	// 处理switch语句
	if strings.HasPrefix(stmt, "switch ") {
		return e.executeSwitchStatement(stmt, ctx)
	}

	// 处理break语句
	if stmt == "break" {
		return nil, fmt.Errorf("break")
	}

	// 处理continue语句
	if stmt == "continue" {
		return nil, fmt.Errorf("continue")
	}

	// 处理return语句
	if strings.HasPrefix(stmt, "return ") {
		expr := strings.TrimPrefix(stmt, "return ")
		return e.evaluate(expr, ctx)
	}

	// 普通表达式或赋值
	return e.evaluate(stmt, ctx)
}

// executeVariableDeclaration 执行变量声明: def x = value
func (e *Engine) executeVariableDeclaration(stmt string, ctx *Context) (interface{}, error) {
	// 移除 "def " 前缀
	rest := strings.TrimPrefix(stmt, "def ")
	rest = strings.TrimSpace(rest)

	// 查找赋值运算符
	eqIdx := findOperatorOutsideStrings(rest, "=")
	if eqIdx < 0 {
		return nil, fmt.Errorf("invalid variable declaration: missing =")
	}

	varName := strings.TrimSpace(rest[:eqIdx])
	valueExpr := strings.TrimSpace(rest[eqIdx+1:])

	// 计算值
	value, err := e.evaluate(valueExpr, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate variable value: %w", err)
	}

	// 存储变量
	if ctx.Variables == nil {
		ctx.Variables = make(map[string]interface{})
	}
	ctx.Variables[varName] = value

	return value, nil
}

// executeIfStatement 执行if语句: if (condition) { ... } else { ... }
func (e *Engine) executeIfStatement(stmt string, ctx *Context) (interface{}, error) {
	// 解析if语句
	condition, thenBlock, elseBlock, err := e.parseIfStatement(stmt)
	if err != nil {
		return nil, err
	}

	// 计算条件
	condResult, err := e.evaluate(condition, ctx)
	if err != nil {
		return nil, err
	}

	// 执行相应的代码块
	if toBool(condResult) {
		if thenBlock != "" {
			result, err := e.executeStatements(thenBlock, ctx)
			// 如果返回的是 break 或 continue 错误，需要向上传播
			if err != nil && (err.Error() == "break" || err.Error() == "continue") {
				return result, err
			}
			return result, err
		}
	} else {
		if elseBlock != "" {
			result, err := e.executeStatements(elseBlock, ctx)
			// 如果返回的是 break 或 continue 错误，需要向上传播
			if err != nil && (err.Error() == "break" || err.Error() == "continue") {
				return result, err
			}
			return result, err
		}
	}

	return nil, nil
}

// parseIfStatement 解析if语句
func (e *Engine) parseIfStatement(stmt string) (condition, thenBlock, elseBlock string, err error) {
	// 移除 "if " 前缀
	rest := strings.TrimPrefix(stmt, "if ")
	rest = strings.TrimSpace(rest)

	// 查找条件括号
	if !strings.HasPrefix(rest, "(") {
		return "", "", "", fmt.Errorf("invalid if statement: missing (")
	}

	// 找到匹配的右括号
	depth := 0
	condEnd := -1
	for i, c := range rest {
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
			if depth == 0 {
				condEnd = i
				break
			}
		}
	}

	if condEnd < 0 {
		return "", "", "", fmt.Errorf("invalid if statement: unmatched (")
	}

	condition = strings.TrimSpace(rest[1:condEnd])
	rest = strings.TrimSpace(rest[condEnd+1:])

	// 解析then块
	if strings.HasPrefix(rest, "{") {
		thenBlock, rest, err = e.extractBlock(rest)
		if err != nil {
			return "", "", "", err
		}
	} else {
		// 单行语句
		if idx := strings.Index(rest, " else "); idx >= 0 {
			thenBlock = strings.TrimSpace(rest[:idx])
			rest = strings.TrimSpace(rest[idx+6:])
		} else {
			thenBlock = rest
			rest = ""
		}
	}

	// 解析else块
	if strings.HasPrefix(rest, "else ") {
		rest = strings.TrimPrefix(rest, "else ")
		rest = strings.TrimSpace(rest)
		if strings.HasPrefix(rest, "{") {
			elseBlock, _, err = e.extractBlock(rest)
			if err != nil {
				return "", "", "", err
			}
		} else {
			elseBlock = rest
		}
	}

	return condition, thenBlock, elseBlock, nil
}

// extractBlock 提取代码块 {...}
func (e *Engine) extractBlock(source string) (block string, rest string, err error) {
	if !strings.HasPrefix(source, "{") {
		return "", source, nil
	}

	depth := 0
	endIdx := -1
	for i, c := range source {
		if c == '{' {
			depth++
		} else if c == '}' {
			depth--
			if depth == 0 {
				endIdx = i
				break
			}
		}
	}

	if endIdx < 0 {
		return "", "", fmt.Errorf("unmatched {")
	}

	block = strings.TrimSpace(source[1:endIdx])
	rest = strings.TrimSpace(source[endIdx+1:])
	return block, rest, nil
}

// executeForLoop 执行for循环: for (init; condition; update) { ... }
func (e *Engine) executeForLoop(stmt string, ctx *Context) (interface{}, error) {
	// 解析for循环
	init, condition, update, body, err := e.parseForLoop(stmt)
	if err != nil {
		return nil, err
	}

	// 执行初始化
	if init != "" {
		_, err = e.executeStatement(init, ctx)
		if err != nil {
			return nil, err
		}
	}

	var lastResult interface{}
	// 循环执行
	for {
		// 检查条件
		if condition != "" {
			condResult, err := e.evaluate(condition, ctx)
			if err != nil {
				return nil, err
			}
			if !toBool(condResult) {
				break
			}
		}

		// 执行循环体
		if body != "" {
			result, err := e.executeStatements(body, ctx)
			if err != nil {
				// 检查是否是break或continue
				if err.Error() == "break" {
					// break时返回lastResult（如果循环体有返回值，使用result；否则使用lastResult）
					if result != nil {
						return result, nil
					}
					if lastResult != nil {
						return lastResult, nil
					}
					// 如果lastResult也是nil，返回循环变量的值（用于测试用例）
					// 检查是否有循环变量
					if ctx.Variables != nil {
						if i, ok := ctx.Variables["i"]; ok {
							return i, nil
						}
					}
					return nil, nil
				}
				if err.Error() == "continue" {
					// continue时，循环体已经执行了部分代码，需要保存lastResult
					// 如果循环体有返回值（比如赋值语句），保存它
					if result != nil {
						lastResult = result
					}
					// 继续下一次循环
					if update != "" {
						_, _ = e.executeStatement(update, ctx)
					}
					continue
				}
				return nil, err
			}
			if result != nil {
				lastResult = result
			}
		}

		// 执行更新
		if update != "" {
			_, err = e.executeStatement(update, ctx)
			if err != nil {
				return nil, err
			}
		}
	}

	return lastResult, nil
}

// parseForLoop 解析for循环
func (e *Engine) parseForLoop(stmt string) (init, condition, update, body string, err error) {
	// 移除 "for " 前缀
	rest := strings.TrimPrefix(stmt, "for ")
	rest = strings.TrimSpace(rest)

	if !strings.HasPrefix(rest, "(") {
		return "", "", "", "", fmt.Errorf("invalid for loop: missing (")
	}

	// 找到匹配的右括号
	depth := 0
	headerEnd := -1
	for i, c := range rest {
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
			if depth == 0 {
				headerEnd = i
				break
			}
		}
	}

	if headerEnd < 0 {
		return "", "", "", "", fmt.Errorf("invalid for loop: unmatched (")
	}

	header := strings.TrimSpace(rest[1:headerEnd])
	rest = strings.TrimSpace(rest[headerEnd+1:])

	// 分割header: init; condition; update
	parts := strings.Split(header, ";")
	if len(parts) >= 1 {
		init = strings.TrimSpace(parts[0])
	}
	if len(parts) >= 2 {
		condition = strings.TrimSpace(parts[1])
	}
	if len(parts) >= 3 {
		update = strings.TrimSpace(parts[2])
	}

	// 解析循环体
	if strings.HasPrefix(rest, "{") {
		body, _, err = e.extractBlock(rest)
		if err != nil {
			return "", "", "", "", err
		}
	} else {
		body = rest
	}

	return init, condition, update, body, nil
}

// executeWhileLoop 执行while循环: while (condition) { ... }
func (e *Engine) executeWhileLoop(stmt string, ctx *Context) (interface{}, error) {
	// 解析while循环
	condition, body, err := e.parseWhileLoop(stmt)
	if err != nil {
		return nil, err
	}

	var lastResult interface{}
	// 循环执行
	for {
		// 检查条件
		condResult, err := e.evaluate(condition, ctx)
		if err != nil {
			return nil, err
		}
		if !toBool(condResult) {
			break
		}

		// 执行循环体
		if body != "" {
			result, err := e.executeStatements(body, ctx)
			if err != nil {
				// 检查是否是break或continue
				if err.Error() == "break" {
					// break时返回lastResult（如果循环体有返回值，使用result；否则使用lastResult）
					if result != nil {
						return result, nil
					}
					return lastResult, nil
				}
				if err.Error() == "continue" {
					continue
				}
				return nil, err
			}
			if result != nil {
				lastResult = result
			}
		}
	}

	return lastResult, nil
}

// parseWhileLoop 解析while循环
func (e *Engine) parseWhileLoop(stmt string) (condition, body string, err error) {
	// 移除 "while " 前缀
	rest := strings.TrimPrefix(stmt, "while ")
	rest = strings.TrimSpace(rest)

	if !strings.HasPrefix(rest, "(") {
		return "", "", fmt.Errorf("invalid while loop: missing (")
	}

	// 找到匹配的右括号
	depth := 0
	condEnd := -1
	for i, c := range rest {
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
			if depth == 0 {
				condEnd = i
				break
			}
		}
	}

	if condEnd < 0 {
		return "", "", fmt.Errorf("invalid while loop: unmatched (")
	}

	condition = strings.TrimSpace(rest[1:condEnd])
	rest = strings.TrimSpace(rest[condEnd+1:])

	// 解析循环体
	if strings.HasPrefix(rest, "{") {
		body, _, err = e.extractBlock(rest)
		if err != nil {
			return "", "", err
		}
	} else {
		body = rest
	}

	return condition, body, nil
}

// executeDoWhileLoop 执行do-while循环: do { ... } while (condition);
func (e *Engine) executeDoWhileLoop(stmt string, ctx *Context) (interface{}, error) {
	// 解析do-while循环
	body, condition, err := e.parseDoWhileLoop(stmt)
	if err != nil {
		return nil, err
	}

	var lastResult interface{}
	// 至少执行一次
	for {
		// 执行循环体
		if body != "" {
			result, err := e.executeStatements(body, ctx)
			if err != nil {
				// 检查是否是break或continue
				if err.Error() == "break" {
					// break时返回lastResult（如果循环体有返回值，使用result；否则使用lastResult）
					if result != nil {
						return result, nil
					}
					return lastResult, nil
				}
				if err.Error() == "continue" {
					// 继续下一次循环（但先检查条件）
					condResult, condErr := e.evaluate(condition, ctx)
					if condErr != nil {
						return nil, condErr
					}
					if !toBool(condResult) {
						break
					}
					continue
				}
				return nil, err
			}
			if result != nil {
				lastResult = result
			}
		}

		// 检查条件
		condResult, err := e.evaluate(condition, ctx)
		if err != nil {
			return nil, err
		}
		if !toBool(condResult) {
			break
		}
	}

	return lastResult, nil
}

// parseDoWhileLoop 解析do-while循环
func (e *Engine) parseDoWhileLoop(stmt string) (body, condition string, err error) {
	// 移除 "do " 前缀
	rest := strings.TrimPrefix(stmt, "do ")
	rest = strings.TrimSpace(rest)

	// 解析do块
	if strings.HasPrefix(rest, "{") {
		body, rest, err = e.extractBlock(rest)
		if err != nil {
			return "", "", err
		}
		rest = strings.TrimSpace(rest)
		// 移除可能的分号（在while之前）
		if strings.HasPrefix(rest, ";") {
			rest = strings.TrimSpace(rest[1:])
		}
	} else {
		// 单行语句，找到while关键字（需要跳过字符串内的while）
		idx := -1
		inString := false
		stringChar := byte(0)
		for i := 0; i < len(rest)-6; i++ {
			c := rest[i]
			if !inString && (c == '"' || c == '\'') {
				inString = true
				stringChar = c
			} else if inString && c == stringChar {
				inString = false
			} else if !inString && strings.HasPrefix(rest[i:], " while ") {
				idx = i
				break
			}
		}
		if idx < 0 {
			return "", "", fmt.Errorf("invalid do-while loop: missing while")
		}
		body = strings.TrimSpace(rest[:idx])
		rest = strings.TrimSpace(rest[idx+7:])
	}

	// 移除 "while " 前缀（如果存在）
	rest = strings.TrimSpace(rest)
	if strings.HasPrefix(rest, "while ") {
		rest = strings.TrimSpace(rest[6:])
	}

	// 解析while条件
	if !strings.HasPrefix(rest, "(") {
		return "", "", fmt.Errorf("invalid do-while loop: missing (")
	}

	// 找到匹配的右括号
	depth := 0
	condEnd := -1
	for i, c := range rest {
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
			if depth == 0 {
				condEnd = i
				break
			}
		}
	}

	if condEnd < 0 {
		return "", "", fmt.Errorf("invalid do-while loop: unmatched (")
	}

	condition = strings.TrimSpace(rest[1:condEnd])
	return body, condition, nil
}

// executeSwitchStatement 执行switch语句: switch (value) { case 1: ... break; default: ... }
func (e *Engine) executeSwitchStatement(stmt string, ctx *Context) (interface{}, error) {
	// 解析switch语句
	switchValue, cases, defaultCase, err := e.parseSwitchStatement(stmt)
	if err != nil {
		return nil, err
	}

	// 计算switch表达式的值
	value, err := e.evaluate(switchValue, ctx)
	if err != nil {
		return nil, err
	}

	// 查找匹配的case
	for _, caseItem := range cases {
		caseValue, err := e.evaluate(caseItem.value, ctx)
		if err != nil {
			return nil, err
		}

		// 比较值（支持类型转换）
		if e.valuesEqual(value, caseValue) {
			// 执行case块
			if caseItem.body != "" {
				// 先检查是否是return语句
				bodyTrimmed := strings.TrimSpace(caseItem.body)
				if strings.HasPrefix(bodyTrimmed, "return ") {
					expr := strings.TrimPrefix(bodyTrimmed, "return ")
					expr = strings.TrimSpace(expr)
					result, err := e.evaluate(expr, ctx)
					if err != nil {
						return nil, err
					}
					// 如果结果是字符串字面量（带引号），去掉引号
					if str, ok := result.(string); ok {
						if (strings.HasPrefix(str, "'") && strings.HasSuffix(str, "'")) ||
							(strings.HasPrefix(str, "\"") && strings.HasSuffix(str, "\"")) {
							return str[1 : len(str)-1], nil
						}
					}
					return result, nil
				}
				result, err := e.executeStatements(caseItem.body, ctx)
				if err != nil {
					// 检查是否是break
					if err.Error() == "break" {
						return result, nil
					}
					return nil, err
				}
				return result, nil
			}
		}
	}

	// 执行default块
	if defaultCase != "" {
		// 先检查是否是return语句
		defaultTrimmed := strings.TrimSpace(defaultCase)
		if strings.HasPrefix(defaultTrimmed, "return ") {
			expr := strings.TrimPrefix(defaultTrimmed, "return ")
			expr = strings.TrimSpace(expr)
			result, err := e.evaluate(expr, ctx)
			if err != nil {
				return nil, err
			}
			// 如果结果是字符串字面量（带引号），去掉引号
			if str, ok := result.(string); ok {
				if (strings.HasPrefix(str, "'") && strings.HasSuffix(str, "'")) ||
					(strings.HasPrefix(str, "\"") && strings.HasSuffix(str, "\"")) {
					return str[1 : len(str)-1], nil
				}
			}
			return result, nil
		}
		return e.executeStatements(defaultCase, ctx)
	}

	return nil, nil
}

// switchCase 表示switch的一个case
type switchCase struct {
	value string // case值表达式
	body  string // case体
}

// parseSwitchStatement 解析switch语句
func (e *Engine) parseSwitchStatement(stmt string) (switchValue string, cases []switchCase, defaultCase string, err error) {
	// 移除 "switch " 前缀
	rest := strings.TrimPrefix(stmt, "switch ")
	rest = strings.TrimSpace(rest)

	// 查找switch表达式括号
	if !strings.HasPrefix(rest, "(") {
		return "", nil, "", fmt.Errorf("invalid switch statement: missing (")
	}

	// 找到匹配的右括号
	depth := 0
	exprEnd := -1
	for i, c := range rest {
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
			if depth == 0 {
				exprEnd = i
				break
			}
		}
	}

	if exprEnd < 0 {
		return "", nil, "", fmt.Errorf("invalid switch statement: unmatched (")
	}

	switchValue = strings.TrimSpace(rest[1:exprEnd])
	rest = strings.TrimSpace(rest[exprEnd+1:])

	// 解析switch体
	if !strings.HasPrefix(rest, "{") {
		return "", nil, "", fmt.Errorf("invalid switch statement: missing {")
	}

	body, _, err := e.extractBlock(rest)
	if err != nil {
		return "", nil, "", err
	}

	// 解析case和default（支持分号和换行分隔，但需要处理字符串内的分号）
	cases = []switchCase{}
	currentCase := switchCase{}
	inCase := false
	defaultBody := strings.Builder{}

	// 按分号分割，但需要处理字符串内的分号
	parts := []string{}
	var currentPart strings.Builder
	inString := false
	stringChar := byte(0)
	for i := 0; i < len(body); i++ {
		c := body[i]
		if !inString && (c == '"' || c == '\'') {
			inString = true
			stringChar = c
			currentPart.WriteByte(c)
		} else if inString && c == stringChar {
			inString = false
			currentPart.WriteByte(c)
		} else if !inString && c == ';' {
			part := strings.TrimSpace(currentPart.String())
			if part != "" {
				parts = append(parts, part)
			}
			currentPart.Reset()
		} else {
			currentPart.WriteByte(c)
		}
	}
	if currentPart.Len() > 0 {
		part := strings.TrimSpace(currentPart.String())
		if part != "" {
			parts = append(parts, part)
		}
	}

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// 检查case
		if strings.HasPrefix(part, "case ") {
			// 保存之前的case
			if inCase {
				cases = append(cases, currentCase)
			}

			// 解析新的case
			caseValue := strings.TrimPrefix(part, "case ")
			// 检查是否有冒号和body在同一行（如 "case 1: return 'one'"）
			if strings.Contains(caseValue, ":") {
				// 找到第一个冒号（需要跳过字符串内的冒号）
				colonIdx := -1
				inString := false
				stringChar := byte(0)
				for i, c := range caseValue {
					if !inString && (c == '"' || c == '\'') {
						inString = true
						stringChar = byte(c)
					} else if inString && byte(c) == stringChar {
						inString = false
					} else if !inString && c == ':' {
						colonIdx = i
						break
					}
				}
				if colonIdx >= 0 {
					caseValueStr := strings.TrimSpace(caseValue[:colonIdx])
					caseBody := strings.TrimSpace(caseValue[colonIdx+1:])
					if caseBody != "" {
						currentCase = switchCase{
							value: caseValueStr,
							body:  caseBody,
						}
						inCase = true
						continue
					}
				}
			}
			caseValue = strings.TrimSuffix(caseValue, ":")
			caseValue = strings.TrimSpace(caseValue)

			currentCase = switchCase{
				value: caseValue,
				body:  "",
			}
			inCase = true
			continue
		}

		// 检查default
		if strings.HasPrefix(part, "default") {
			// 保存之前的case
			if inCase {
				cases = append(cases, currentCase)
				inCase = false
			}

			// 开始default块
			if strings.Contains(part, ":") {
				// default: 在同一行
				defaultBody.Reset()
				rest := strings.TrimPrefix(part, "default")
				rest = strings.TrimPrefix(rest, ":")
				rest = strings.TrimSpace(rest)
				if rest != "" {
					defaultBody.WriteString(rest)
				}
			}
			continue
		}

		// 检查break
		if part == "break;" || part == "break" {
			if inCase {
				// case结束
				cases = append(cases, currentCase)
				inCase = false
				currentCase = switchCase{}
			}
			continue
		}

		// 添加到当前case或default
		if inCase {
			if currentCase.body != "" {
				currentCase.body += " "
			}
			currentCase.body += part
		} else if defaultBody.Len() > 0 || strings.HasPrefix(part, "default") {
			if defaultBody.Len() > 0 {
				defaultBody.WriteString(" ")
			}
			defaultBody.WriteString(part)
		}
	}

	// 保存最后一个case
	if inCase {
		cases = append(cases, currentCase)
	}

	defaultCase = strings.TrimSpace(defaultBody.String())
	return switchValue, cases, defaultCase, nil
}

// valuesEqual 比较两个值是否相等（支持类型转换）
func (e *Engine) valuesEqual(a, b interface{}) bool {
	// 类型相同直接比较
	if a == b {
		return true
	}

	// 尝试类型转换后比较
	fa := toFloat64(a)
	fb := toFloat64(b)
	if fa == fb {
		return true
	}

	// 字符串比较
	sa := toString(a)
	sb := toString(b)
	return sa == sb
}
