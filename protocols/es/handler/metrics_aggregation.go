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
	"strconv"

	bleve "github.com/lscgzwd/tiggerdb"
)

// 保留 bleve import 用于类型引用
var _ bleve.Index

// MetricsAggregationSpec Metrics聚合规格
type MetricsAggregationSpec struct {
	Type               string // avg, sum, min, max, stats, cardinality
	Field              string // 字段名
	PrecisionThreshold int    // cardinality聚合的精度阈值（可选）
}

// calculateMetricsAggregationsWithCache 从已缓存的文档中计算Metrics聚合
// 性能优化：复用已获取的文档数据，避免重复获取
func (h *DocumentHandler) calculateMetricsAggregationsWithCache(
	searchResult *bleve.SearchResult,
	metricsAggs map[string]MetricsAggregationSpec,
	docCache map[string]map[string]interface{},
) (map[string]interface{}, error) {
	if len(metricsAggs) == 0 {
		return nil, nil
	}

	// 收集所有需要计算的字段值
	fieldValues := make(map[string][]float64)                  // 字段名 -> 值列表（用于数值聚合）
	fieldUniqueValues := make(map[string]map[interface{}]bool) // 字段名 -> 唯一值集合（用于cardinality聚合）

	// 遍历所有匹配的文档
	// 性能优化：使用已缓存的文档数据
	for _, hit := range searchResult.Hits {
		if doc, ok := docCache[hit.ID]; ok {
			for fieldName, fieldValue := range doc {
				// 检查是否需要这个字段
				for _, spec := range metricsAggs {
					if spec.Field == fieldName {
						if spec.Type == "cardinality" {
							// Cardinality聚合：收集唯一值
							if fieldUniqueValues[fieldName] == nil {
								fieldUniqueValues[fieldName] = make(map[interface{}]bool)
							}
							// 处理数组字段
							if arr, ok := fieldValue.([]interface{}); ok {
								for _, v := range arr {
									fieldUniqueValues[fieldName][v] = true
								}
							} else {
								fieldUniqueValues[fieldName][fieldValue] = true
							}
						} else {
							// 其他metrics聚合：尝试从字段值中提取数值
							if value := h.extractNumericValueFromInterface(fieldValue); value != nil {
								fieldValues[fieldName] = append(fieldValues[fieldName], *value)
							}
						}
						break
					}
				}
			}
		}
	}

	// 计算每个聚合的结果
	results := make(map[string]interface{})
	for aggName, spec := range metricsAggs {
		if spec.Type == "cardinality" {
			// Cardinality聚合：计算唯一值数量
			uniqueValues, ok := fieldUniqueValues[spec.Field]
			if !ok || len(uniqueValues) == 0 {
				results[aggName] = map[string]interface{}{
					"value": 0,
				}
				continue
			}
			cardinality := h.calculateCardinality(uniqueValues, spec.PrecisionThreshold)
			results[aggName] = map[string]interface{}{
				"value": cardinality,
			}
		} else {
			// 其他metrics聚合
			values, ok := fieldValues[spec.Field]
			if !ok || len(values) == 0 {
				// 没有找到值，返回null或0
				results[aggName] = h.buildMetricsResult(spec.Type, nil)
				continue
			}

			results[aggName] = h.buildMetricsResult(spec.Type, values)
		}
	}

	return results, nil
}

// extractNumericValueFromInterface 从interface{}中提取数值（用于hit.Fields）
func (h *DocumentHandler) extractNumericValueFromInterface(value interface{}) *float64 {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case float64:
		return &v
	case float32:
		f := float64(v)
		return &f
	case int:
		f := float64(v)
		return &f
	case int64:
		f := float64(v)
		return &f
	case int32:
		f := float64(v)
		return &f
	case uint:
		f := float64(v)
		return &f
	case uint64:
		f := float64(v)
		return &f
	case uint32:
		f := float64(v)
		return &f
	case string:
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return &parsed
		}
	case []byte:
		if parsed, err := strconv.ParseFloat(string(v), 64); err == nil {
			return &parsed
		}
	}

	return nil
}

// buildMetricsResult 构建Metrics聚合结果
func (h *DocumentHandler) buildMetricsResult(aggType string, values []float64) map[string]interface{} {
	if len(values) == 0 {
		return map[string]interface{}{
			"value": nil,
		}
	}

	switch aggType {
	case "avg":
		return map[string]interface{}{
			"value": h.calculateAvg(values),
		}
	case "sum":
		return map[string]interface{}{
			"value": h.calculateSum(values),
		}
	case "min":
		return map[string]interface{}{
			"value": h.calculateMin(values),
		}
	case "max":
		return map[string]interface{}{
			"value": h.calculateMax(values),
		}
	case "stats":
		return h.calculateStats(values)
	default:
		return map[string]interface{}{
			"value": nil,
		}
	}
}

// calculateAvg 计算平均值
func (h *DocumentHandler) calculateAvg(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return h.calculateSum(values) / float64(len(values))
}

// calculateSum 计算总和
func (h *DocumentHandler) calculateSum(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum
}

// calculateMin 计算最小值
func (h *DocumentHandler) calculateMin(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

// calculateMax 计算最大值
func (h *DocumentHandler) calculateMax(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}

// calculateStats 计算统计信息（包含count, min, max, avg, sum）
func (h *DocumentHandler) calculateStats(values []float64) map[string]interface{} {
	if len(values) == 0 {
		return map[string]interface{}{
			"count": 0,
			"min":   nil,
			"max":   nil,
			"avg":   nil,
			"sum":   0.0,
		}
	}

	count := len(values)
	min := h.calculateMin(values)
	max := h.calculateMax(values)
	avg := h.calculateAvg(values)
	sum := h.calculateSum(values)

	// 计算方差和标准差（ES stats聚合包含这些）
	variance := 0.0
	for _, v := range values {
		diff := v - avg
		variance += diff * diff
	}
	variance /= float64(count)
	stdDeviation := math.Sqrt(variance)

	return map[string]interface{}{
		"count":          count,
		"min":            min,
		"max":            max,
		"avg":            avg,
		"sum":            sum,
		"sum_of_squares": h.calculateSumOfSquares(values),
		"variance":       variance,
		"std_deviation":  stdDeviation,
		"std_deviation_bounds": map[string]interface{}{
			"upper": avg + (2 * stdDeviation),
			"lower": avg - (2 * stdDeviation),
		},
	}
}

// calculateSumOfSquares 计算平方和
func (h *DocumentHandler) calculateSumOfSquares(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v * v
	}
	return sum
}

// calculateCardinality 计算唯一值数量（cardinality聚合）
// precisionThreshold: 精度阈值，用于控制HyperLogLog算法的精度（可选，默认3000）
func (h *DocumentHandler) calculateCardinality(uniqueValues map[interface{}]bool, precisionThreshold int) int64 {
	if len(uniqueValues) == 0 {
		return 0
	}

	// 如果precision_threshold未设置或为0，使用默认值3000
	if precisionThreshold <= 0 {
		precisionThreshold = 3000
	}

	// 如果唯一值数量小于precision_threshold，直接返回精确值
	// 否则使用HyperLogLog算法估算（这里简化实现，直接返回精确值）
	// 注意：ES的cardinality聚合使用HyperLogLog++算法，这里为了简化，直接计算精确值
	// 对于大数据集，可以考虑实现HyperLogLog算法以提高性能
	cardinality := int64(len(uniqueValues))

	// 如果值数量超过precision_threshold，记录警告（实际应该使用HyperLogLog）
	if cardinality > int64(precisionThreshold) {
		// 这里可以添加日志，但为了性能考虑，暂时不记录
		// logger.Debug("Cardinality [%d] exceeds precision_threshold [%d], consider using HyperLogLog for better performance", cardinality, precisionThreshold)
	}

	return cardinality
}
