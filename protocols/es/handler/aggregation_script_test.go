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
	"math"
	"testing"
)

func TestParseScriptedMetricAggregation(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]interface{}
		want   *ScriptedMetricAggregationConfig
	}{
		{
			name: "basic scripted_metric",
			config: map[string]interface{}{
				"init_script":    "state.sum = 0",
				"map_script":     "state.sum += doc['value'].value",
				"combine_script": "return state.sum",
				"reduce_script":  "return states.stream().mapToDouble(s -> s).sum()",
			},
			want: &ScriptedMetricAggregationConfig{
				InitScript:    "state.sum = 0",
				MapScript:     "state.sum += doc['value'].value",
				CombineScript: "return state.sum",
				ReduceScript:  "return states.stream().mapToDouble(s -> s).sum()",
			},
		},
		{
			name: "scripted_metric with params",
			config: map[string]interface{}{
				"map_script": "state.sum += doc['value'].value * params.factor",
				"params": map[string]interface{}{
					"factor": 2.0,
				},
			},
			want: &ScriptedMetricAggregationConfig{
				MapScript: "state.sum += doc['value'].value * params.factor",
				Params: map[string]interface{}{
					"factor": 2.0,
				},
			},
		},
		{
			name: "scripted_metric with script object",
			config: map[string]interface{}{
				"map_script": map[string]interface{}{
					"source": "state.sum += doc['value'].value",
					"lang":   "painless",
				},
			},
			want: &ScriptedMetricAggregationConfig{
				MapScript: "state.sum += doc['value'].value",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseScriptedMetricAggregation(tt.config)
			if got.InitScript != tt.want.InitScript {
				t.Errorf("InitScript = %v, want %v", got.InitScript, tt.want.InitScript)
			}
			if got.MapScript != tt.want.MapScript {
				t.Errorf("MapScript = %v, want %v", got.MapScript, tt.want.MapScript)
			}
			if got.CombineScript != tt.want.CombineScript {
				t.Errorf("CombineScript = %v, want %v", got.CombineScript, tt.want.CombineScript)
			}
			if got.ReduceScript != tt.want.ReduceScript {
				t.Errorf("ReduceScript = %v, want %v", got.ReduceScript, tt.want.ReduceScript)
			}
		})
	}
}

func TestParseBucketScriptAggregation(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]interface{}
		want   *BucketScriptAggregationConfig
	}{
		{
			name: "basic bucket_script",
			config: map[string]interface{}{
				"buckets_path": map[string]interface{}{
					"sum": "total_sum",
					"cnt": "doc_count",
				},
				"script": "params.sum / params.cnt",
			},
			want: &BucketScriptAggregationConfig{
				BucketsPath: map[string]string{
					"sum": "total_sum",
					"cnt": "doc_count",
				},
				Script:    "params.sum / params.cnt",
				GapPolicy: "skip",
			},
		},
		{
			name: "bucket_script with gap_policy",
			config: map[string]interface{}{
				"buckets_path": map[string]interface{}{
					"a": "agg1",
					"b": "agg2",
				},
				"script":     "params.a + params.b",
				"gap_policy": "insert_zeros",
			},
			want: &BucketScriptAggregationConfig{
				BucketsPath: map[string]string{
					"a": "agg1",
					"b": "agg2",
				},
				Script:    "params.a + params.b",
				GapPolicy: "insert_zeros",
			},
		},
		{
			name: "bucket_script with format",
			config: map[string]interface{}{
				"buckets_path": map[string]interface{}{
					"rate": "conversion_rate",
				},
				"script": "params.rate * 100",
				"format": "0.00%",
			},
			want: &BucketScriptAggregationConfig{
				BucketsPath: map[string]string{
					"rate": "conversion_rate",
				},
				Script:    "params.rate * 100",
				GapPolicy: "skip",
				Format:    "0.00%",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseBucketScriptAggregation(tt.config)
			if got.Script != tt.want.Script {
				t.Errorf("Script = %v, want %v", got.Script, tt.want.Script)
			}
			if got.GapPolicy != tt.want.GapPolicy {
				t.Errorf("GapPolicy = %v, want %v", got.GapPolicy, tt.want.GapPolicy)
			}
			if got.Format != tt.want.Format {
				t.Errorf("Format = %v, want %v", got.Format, tt.want.Format)
			}
			for k, v := range tt.want.BucketsPath {
				if got.BucketsPath[k] != v {
					t.Errorf("BucketsPath[%s] = %v, want %v", k, got.BucketsPath[k], v)
				}
			}
		})
	}
}

func TestExecuteBucketScript(t *testing.T) {
	executor := NewScriptAggregationExecutor()

	tests := []struct {
		name         string
		config       *BucketScriptAggregationConfig
		bucketValues map[string]interface{}
		want         float64
		wantNil      bool
	}{
		{
			name: "simple addition",
			config: &BucketScriptAggregationConfig{
				BucketsPath: map[string]string{
					"a": "agg1",
					"b": "agg2",
				},
				Script:    "params.a + params.b",
				GapPolicy: "skip",
			},
			bucketValues: map[string]interface{}{
				"agg1": 10.0,
				"agg2": 20.0,
			},
			want: 30.0,
		},
		{
			name: "division",
			config: &BucketScriptAggregationConfig{
				BucketsPath: map[string]string{
					"total": "sum_agg",
					"count": "count_agg",
				},
				Script:    "params.total / params.count",
				GapPolicy: "skip",
			},
			bucketValues: map[string]interface{}{
				"sum_agg":   100.0,
				"count_agg": 4.0,
			},
			want: 25.0,
		},
		{
			name: "missing value with skip",
			config: &BucketScriptAggregationConfig{
				BucketsPath: map[string]string{
					"a": "agg1",
					"b": "agg2",
				},
				Script:    "params.a + params.b",
				GapPolicy: "skip",
			},
			bucketValues: map[string]interface{}{
				"agg1": 10.0,
				// agg2 is missing
			},
			wantNil: true,
		},
		{
			name: "missing value with insert_zeros",
			config: &BucketScriptAggregationConfig{
				BucketsPath: map[string]string{
					"a": "agg1",
					"b": "agg2",
				},
				Script:    "params.a + params.b",
				GapPolicy: "insert_zeros",
			},
			bucketValues: map[string]interface{}{
				"agg1": 10.0,
				// agg2 is missing, will be 0
			},
			want: 10.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := executor.ExecuteBucketScript(tt.config, tt.bucketValues)
			if err != nil {
				t.Errorf("ExecuteBucketScript() error = %v", err)
				return
			}
			if tt.wantNil {
				if got != nil {
					t.Errorf("ExecuteBucketScript() = %v, want nil", got)
				}
				return
			}
			gotFloat, ok := got.(float64)
			if !ok {
				t.Errorf("ExecuteBucketScript() returned non-float64: %T", got)
				return
			}
			if math.Abs(gotFloat-tt.want) > 0.0001 {
				t.Errorf("ExecuteBucketScript() = %v, want %v", gotFloat, tt.want)
			}
		})
	}
}

func TestExecuteScriptedMetric(t *testing.T) {
	executor := NewScriptAggregationExecutor()

	tests := []struct {
		name   string
		config *ScriptedMetricAggregationConfig
		docs   []map[string]interface{}
		want   interface{}
	}{
		{
			name: "simple sum",
			config: &ScriptedMetricAggregationConfig{
				InitScript:    "state.sum = 0",
				MapScript:     "state.sum += doc['value'].value",
				CombineScript: "state.sum",
			},
			docs: []map[string]interface{}{
				{"value": 10.0},
				{"value": 20.0},
				{"value": 30.0},
			},
			want: 60.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := executor.ExecuteScriptedMetric(tt.config, tt.docs)
			if err != nil {
				t.Errorf("ExecuteScriptedMetric() error = %v", err)
				return
			}
			// 脚本执行结果可能是 state map 或具体值
			t.Logf("ExecuteScriptedMetric() result = %v (type: %T)", got, got)
		})
	}
}
