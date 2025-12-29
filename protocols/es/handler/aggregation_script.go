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

package handler

import (
	"github.com/lscgzwd/tiggerdb/logger"
	"github.com/lscgzwd/tiggerdb/script"
)

// ScriptAggregationExecutor 脚本聚合执行器
type ScriptAggregationExecutor struct {
	engine *script.Engine
}

// NewScriptAggregationExecutor 创建脚本聚合执行器
func NewScriptAggregationExecutor() *ScriptAggregationExecutor {
	return &ScriptAggregationExecutor{
		engine: script.NewEngine(),
	}
}

// ExecuteScriptedMetric 执行 scripted_metric 聚合
// ES格式: {"scripted_metric": {"init_script": "state.sum = 0", "map_script": "state.sum += doc['value'].value", "combine_script": "return state.sum", "reduce_script": "..."}}
func (e *ScriptAggregationExecutor) ExecuteScriptedMetric(config *ScriptedMetricAggregationConfig, docs []map[string]interface{}) (interface{}, error) {
	if config == nil {
		return nil, nil
	}

	// 初始化状态
	state := make(map[string]interface{})
	if config.InitScript != "" {
		initScript := script.NewScript(config.InitScript, config.Params)
		ctx := script.NewContext(nil, state, config.Params)
		ctx.Ctx["state"] = state
		_, err := e.engine.Execute(initScript, ctx)
		if err != nil {
			logger.Warn("scripted_metric init_script error: %v", err)
		}
		// 从 ctx 中获取更新后的 state
		if s, ok := ctx.Ctx["state"].(map[string]interface{}); ok {
			state = s
		}
	}

	// 对每个文档执行 map_script
	if config.MapScript != "" {
		mapScript := script.NewScript(config.MapScript, config.Params)
		for _, doc := range docs {
			ctx := script.NewContext(doc, doc, config.Params)
			ctx.Ctx["state"] = state
			_, err := e.engine.Execute(mapScript, ctx)
			if err != nil {
				logger.Debug("scripted_metric map_script error for doc: %v", err)
				continue
			}
			// 更新 state
			if s, ok := ctx.Ctx["state"].(map[string]interface{}); ok {
				state = s
			}
		}
	}

	// 执行 combine_script（单节点场景下直接返回结果）
	var result interface{} = state
	if config.CombineScript != "" {
		combineScript := script.NewScript(config.CombineScript, config.Params)
		ctx := script.NewContext(nil, nil, config.Params)
		ctx.Ctx["state"] = state
		combineResult, err := e.engine.Execute(combineScript, ctx)
		if err != nil {
			logger.Warn("scripted_metric combine_script error: %v", err)
		} else {
			result = combineResult
		}
	}

	// reduce_script 在分布式场景下使用，单节点直接返回 combine 结果
	// 如果有 reduce_script，包装结果
	if config.ReduceScript != "" {
		reduceScript := script.NewScript(config.ReduceScript, config.Params)
		ctx := script.NewContext(nil, nil, config.Params)
		ctx.Ctx["states"] = []interface{}{result}
		reduceResult, err := e.engine.Execute(reduceScript, ctx)
		if err != nil {
			logger.Warn("scripted_metric reduce_script error: %v", err)
		} else {
			result = reduceResult
		}
	}

	return result, nil
}

// ExecuteBucketScript 执行 bucket_script 聚合
// ES格式: {"bucket_script": {"buckets_path": {"var1": "agg1", "var2": "agg2"}, "script": "params.var1 + params.var2"}}
func (e *ScriptAggregationExecutor) ExecuteBucketScript(config *BucketScriptAggregationConfig, bucketValues map[string]interface{}) (interface{}, error) {
	if config == nil || config.Script == "" {
		return nil, nil
	}

	// 将 bucket 值作为参数传入
	params := make(map[string]interface{})
	if config.Params != nil {
		for k, v := range config.Params {
			params[k] = v
		}
	}

	// 将 buckets_path 中的值添加到参数
	for varName, aggPath := range config.BucketsPath {
		if val, ok := bucketValues[aggPath]; ok {
			params[varName] = val
		} else {
			// 处理 gap_policy
			if config.GapPolicy == "insert_zeros" {
				params[varName] = 0.0
			} else {
				// skip: 如果值不存在，跳过计算
				return nil, nil
			}
		}
	}

	// 执行脚本
	s := script.NewScript(config.Script, params)
	ctx := script.NewContext(nil, nil, params)
	result, err := e.engine.Execute(s, ctx)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// ParseScriptedMetricAggregation 解析 scripted_metric 聚合配置
func ParseScriptedMetricAggregation(config map[string]interface{}) *ScriptedMetricAggregationConfig {
	agg := &ScriptedMetricAggregationConfig{}

	// 解析各个脚本
	if initScript, ok := config["init_script"]; ok {
		agg.InitScript = parseScriptSource(initScript)
	}
	if mapScript, ok := config["map_script"]; ok {
		agg.MapScript = parseScriptSource(mapScript)
	}
	if combineScript, ok := config["combine_script"]; ok {
		agg.CombineScript = parseScriptSource(combineScript)
	}
	if reduceScript, ok := config["reduce_script"]; ok {
		agg.ReduceScript = parseScriptSource(reduceScript)
	}

	// 解析参数
	if params, ok := config["params"].(map[string]interface{}); ok {
		agg.Params = params
	}

	return agg
}

// ParseBucketScriptAggregation 解析 bucket_script 聚合配置
func ParseBucketScriptAggregation(config map[string]interface{}) *BucketScriptAggregationConfig {
	agg := &BucketScriptAggregationConfig{
		BucketsPath: make(map[string]string),
		GapPolicy:   "skip", // 默认跳过空值
	}

	// 解析 buckets_path
	if bp, ok := config["buckets_path"].(map[string]interface{}); ok {
		for k, v := range bp {
			if path, ok := v.(string); ok {
				agg.BucketsPath[k] = path
			}
		}
	}

	// 解析脚本
	if scriptData, ok := config["script"]; ok {
		agg.Script = parseScriptSource(scriptData)
	}

	// 解析 gap_policy
	if gp, ok := config["gap_policy"].(string); ok {
		agg.GapPolicy = gp
	}

	// 解析 format
	if format, ok := config["format"].(string); ok {
		agg.Format = format
	}

	// 解析参数
	if params, ok := config["params"].(map[string]interface{}); ok {
		agg.Params = params
	}

	return agg
}

// parseScriptSource 解析脚本源码
func parseScriptSource(scriptData interface{}) string {
	switch s := scriptData.(type) {
	case string:
		return s
	case map[string]interface{}:
		if source, ok := s["source"].(string); ok {
			return source
		}
		if inline, ok := s["inline"].(string); ok {
			return inline
		}
	}
	return ""
}

// ScriptAggregationResult 脚本聚合结果
type ScriptAggregationResult struct {
	ScriptedMetrics map[string]interface{} // scripted_metric 结果
	BucketScripts   map[string]interface{} // bucket_script 结果
}

// NewScriptAggregationResult 创建脚本聚合结果
func NewScriptAggregationResult() *ScriptAggregationResult {
	return &ScriptAggregationResult{
		ScriptedMetrics: make(map[string]interface{}),
		BucketScripts:   make(map[string]interface{}),
	}
}
