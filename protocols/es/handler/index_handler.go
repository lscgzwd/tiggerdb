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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/lscgzwd/tiggerdb/logger"

	"github.com/gorilla/mux"
	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/directory"
	"github.com/lscgzwd/tiggerdb/mapping"
	"github.com/lscgzwd/tiggerdb/metadata"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/common"
)

// IndexManagerInterface 索引管理器接口（用于索引操作）
type IndexManagerInterface interface {
	InvalidateIndexStatus(string)
	CloseIndex(string) error
}

// IndexHandler ES索引处理器实现
type IndexHandler struct {
	dirMgr    directory.DirectoryManager
	metaStore metadata.MetadataStore
	indexMgr  IndexManagerInterface // 索引管理器（用于缓存失效和关闭索引）
}

// NewIndexHandler 创建新的索引处理器
func NewIndexHandler(dirMgr directory.DirectoryManager, metaStore metadata.MetadataStore) *IndexHandler {
	return &IndexHandler{
		dirMgr:    dirMgr,
		metaStore: metaStore,
		indexMgr:  nil, // 将在server.go中设置
	}
}

// SetIndexManager 设置索引管理器（用于缓存失效和关闭索引）
func (h *IndexHandler) SetIndexManager(indexMgr IndexManagerInterface) {
	h.indexMgr = indexMgr
}

// ListIndices 列出所有索引
// GET /_cat/indices
// ES的cat API默认返回纯文本表格格式，但可以通过Accept: application/json返回JSON格式
func (h *IndexHandler) ListIndices(w http.ResponseWriter, r *http.Request) {
	indices, err := h.dirMgr.ListIndices()
	if err != nil {
		common.HandleError(w, common.NewInternalServerError(err.Error()))
		return
	}

	// 检查Accept头，支持JSON格式
	acceptHeader := r.Header.Get("Accept")
	if acceptHeader == "application/json" || acceptHeader == "application/json; charset=utf-8" {
		// JSON格式响应（ES兼容格式）
		indexList := make([]map[string]interface{}, 0, len(indices))
		for _, indexName := range indices {
			indexList = append(indexList, map[string]interface{}{
				"index":  indexName,
				"status": "open",
			})
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		if err := encoder.Encode(indexList); err != nil {
			logger.Error("Failed to encode indices list: %v", err)
		}
		return
	}

	// 默认返回纯文本表格格式（ES cat API标准格式）
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	// 写入表头
	w.Write([]byte("index\tstatus\n"))

	// 写入数据行
	for _, indexName := range indices {
		w.Write([]byte(fmt.Sprintf("%s\topen\n", indexName)))
	}
}

// CreateIndex 创建索引
func (h *IndexHandler) CreateIndex(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 验证索引名称
	if err := common.ValidateIndexName(indexName); err != nil {
		common.HandleError(w, common.NewBadRequestError(err.Error()))
		return
	}

	// 检查索引是否已存在（原子性检查）
	if h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewConflictError("index already exists: "+indexName))
		return
	}

	// 解析请求体（mapping和settings）
	// 注意：无论 ContentLength 是多少，都应该尝试读取请求体
	// 因为 chunked encoding 时 ContentLength 为 -1，但请求体仍然存在
	var requestBody map[string]interface{}

	// 防御性检查：如果 ContentLength > 0，提前检查大小
	if r.ContentLength > 0 {
		if r.ContentLength > common.MaxIndexBodySize {
			common.HandleError(w, common.NewBadRequestError("request body too large"))
			return
		}
	}

	// 尝试读取请求体（兼容 chunked encoding 和 ContentLength = 0 的情况）
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&requestBody); err != nil {
		if err == io.EOF {
			// 请求体为空，使用空的 requestBody
			requestBody = make(map[string]interface{})
		} else {
			common.HandleError(w, common.NewBadRequestError("invalid JSON body: "+err.Error()))
			return
		}
	}

	// 调试：记录请求体的结构
	if len(requestBody) > 0 {
		logger.Debug("CreateIndex [%s] - Request body keys: %v", indexName, getMapKeys(requestBody))
		if m, ok := requestBody["mappings"].(map[string]interface{}); ok {
			logger.Debug("CreateIndex [%s] - Request body has 'mappings' with keys: %v", indexName, getMapKeys(m))
			if props, ok := m["properties"].(map[string]interface{}); ok {
				logger.Debug("CreateIndex [%s] - Request body 'mappings.properties' has %d fields", indexName, len(props))
			} else {
				logger.Debug("CreateIndex [%s] - Request body 'mappings.properties' is missing or not a map", indexName)
			}
		} else {
			logger.Debug("CreateIndex [%s] - Request body has no 'mappings' key", indexName)
		}
	} else {
		logger.Debug("CreateIndex [%s] - Request body is empty", indexName)
	}

	// 提取mapping和settings
	mapping, settings := h.extractMappingAndSettings(requestBody)

	// 调试：记录提取的 mapping 字段数量
	logger.Debug("CreateIndex [%s] - Extracted mapping keys: %v", indexName, getMapKeys(mapping))
	if props, ok := mapping["properties"].(map[string]interface{}); ok {
		logger.Debug("CreateIndex [%s] - Extracted mapping has %d properties", indexName, len(props))
	} else {
		logger.Debug("CreateIndex [%s] - Extracted mapping has no properties", indexName)
	}

	// 创建目录（原子操作）
	if err := h.dirMgr.CreateIndex(indexName); err != nil {
		common.HandleError(w, common.NewInternalServerError("failed to create index directory: "+err.Error()))
		return
	}

	// 提取 join 字段的关系定义
	joinRelations := h.extractJoinRelations(mapping)

	// 创建元数据
	now := time.Now()
	indexMeta := &metadata.IndexMetadata{
		Name:          indexName,
		Mapping:       mapping,
		Settings:      settings,
		Aliases:       []string{},
		JoinRelations: joinRelations,
		Version:       1,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	// 调试：记录保存前的 mapping 字段数量
	if props, ok := indexMeta.Mapping["properties"].(map[string]interface{}); ok {
		logger.Debug("CreateIndex [%s] - Before save, mapping has %d properties", indexName, len(props))
	}

	// 保存元数据，如果失败则回滚目录创建
	if err := h.metaStore.SaveIndexMetadata(indexName, indexMeta); err != nil {
		// 回滚：清理已创建的目录
		if delErr := h.dirMgr.DeleteIndex(indexName); delErr != nil {
			// 记录回滚失败，但不影响错误响应
			logger.Error("Failed to rollback index directory deletion for index [%s] after metadata save failure: %v", indexName, delErr)
		}
		logger.Error("Failed to save index metadata for index [%s]: %v", indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to save index metadata: "+err.Error()))
		return
	}

	// 创建bleve索引（如果有索引管理器）
	if h.indexMgr != nil {
		// 获取索引路径
		indexPath := h.dirMgr.GetIndexPath(indexName)
		if indexPath == "" {
			// 回滚
			h.metaStore.DeleteIndexMetadata(indexName)
			h.dirMgr.DeleteIndex(indexName)
			common.HandleError(w, common.NewInternalServerError("failed to get index path"))
			return
		}

		// 构建存储路径
		storePath := filepath.Join(indexPath, "store")

		// 将 ES mapping 转换为 Bleve IndexMapping
		bleveMapping, err := h.convertESMappingToBleve(mapping)
		if err != nil {
			// 回滚
			h.metaStore.DeleteIndexMetadata(indexName)
			h.dirMgr.DeleteIndex(indexName)
			logger.Error("Failed to convert ES mapping to Bleve mapping for [%s]: %v", indexName, err)
			common.HandleError(w, common.NewBadRequestError("invalid mapping: "+err.Error()))
			return
		}

		// 验证 mapping
		if err := bleveMapping.Validate(); err != nil {
			// 回滚
			h.metaStore.DeleteIndexMetadata(indexName)
			h.dirMgr.DeleteIndex(indexName)
			logger.Error("Invalid Bleve mapping for [%s]: %v", indexName, err)
			common.HandleError(w, common.NewBadRequestError("invalid mapping: "+err.Error()))
			return
		}

		idx, err := bleve.New(storePath, bleveMapping)
		if err != nil {
			// 回滚
			h.metaStore.DeleteIndexMetadata(indexName)
			h.dirMgr.DeleteIndex(indexName)
			logger.Error("Failed to create bleve index for [%s]: %v", indexName, err)
			common.HandleError(w, common.NewInternalServerError("failed to create index: "+err.Error()))
			return
		}
		idx.Close() // 创建后立即关闭，由IndexManager管理生命周期
	}

	// 使索引状态缓存失效
	if h.indexMgr != nil {
		h.indexMgr.InvalidateIndexStatus(indexName)
	}

	// 返回成功响应
	resp := common.SuccessResponse().
		WithAcknowledged(true).
		WithIndex(indexName)
	common.HandleSuccess(w, resp, http.StatusOK)
}

// extractMappingAndSettings 从请求体中提取mapping和settings
// 使用深拷贝确保数据完整性，避免引用问题导致的数据丢失
func (h *IndexHandler) extractMappingAndSettings(requestBody map[string]interface{}) (map[string]interface{}, map[string]interface{}) {
	mapping := make(map[string]interface{})
	settings := make(map[string]interface{})

	// 深拷贝 mapping，确保不会因为引用问题导致数据丢失
	if m, ok := requestBody["mappings"].(map[string]interface{}); ok {
		// 使用 JSON 序列化和反序列化实现深拷贝
		mappingBytes, err := json.Marshal(m)
		if err != nil {
			logger.Warn("Failed to marshal mapping for deep copy: %v", err)
			// 如果序列化失败，使用浅拷贝作为后备方案
			mapping = m
		} else {
			if err := json.Unmarshal(mappingBytes, &mapping); err != nil {
				logger.Warn("Failed to unmarshal mapping for deep copy: %v", err)
				// 如果反序列化失败，使用浅拷贝作为后备方案
				mapping = m
			}
		}
	}

	// 深拷贝 settings，确保不会因为引用问题导致数据丢失
	if s, ok := requestBody["settings"].(map[string]interface{}); ok {
		// 使用 JSON 序列化和反序列化实现深拷贝
		settingsBytes, err := json.Marshal(s)
		if err != nil {
			logger.Warn("Failed to marshal settings for deep copy: %v", err)
			// 如果序列化失败，使用浅拷贝作为后备方案
			settings = s
		} else {
			if err := json.Unmarshal(settingsBytes, &settings); err != nil {
				logger.Warn("Failed to unmarshal settings for deep copy: %v", err)
				// 如果反序列化失败，使用浅拷贝作为后备方案
				settings = s
			}
		}
	}

	return mapping, settings
}

// extractJoinRelations 从 mapping 中提取 join 字段的关系定义
func (h *IndexHandler) extractJoinRelations(esMapping map[string]interface{}) *metadata.JoinRelations {
	properties, ok := esMapping["properties"].(map[string]interface{})
	if !ok {
		return nil
	}

	for fieldName, fieldDef := range properties {
		fieldMap, ok := fieldDef.(map[string]interface{})
		if !ok {
			continue
		}

		fieldType, _ := fieldMap["type"].(string)
		if fieldType != "join" {
			continue
		}

		// 找到 join 字段，提取 relations
		relations, ok := fieldMap["relations"].(map[string]interface{})
		if !ok {
			continue
		}

		// 转换 relations 为 map[string][]string
		joinRelations := &metadata.JoinRelations{
			FieldName: fieldName,
			Relations: make(map[string][]string),
		}

		for parentType, childTypes := range relations {
			switch v := childTypes.(type) {
			case string:
				// 单个子类型
				joinRelations.Relations[parentType] = []string{v}
			case []interface{}:
				// 多个子类型
				children := make([]string, 0, len(v))
				for _, child := range v {
					if childStr, ok := child.(string); ok {
						children = append(children, childStr)
					}
				}
				joinRelations.Relations[parentType] = children
			}
		}

		logger.Debug("Extracted join relations for field '%s': %v", fieldName, joinRelations.Relations)
		return joinRelations
	}

	return nil
}

// getMapKeys 获取 map 的所有键（用于调试）
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// convertESMappingToBleve 将 ES 格式的 mapping 转换为 Bleve IndexMapping
func (h *IndexHandler) convertESMappingToBleve(esMapping map[string]interface{}) (*mapping.IndexMappingImpl, error) {
	// 如果没有提供 mapping，使用默认 mapping
	if len(esMapping) == 0 {
		return mapping.NewIndexMapping(), nil
	}

	// 创建默认的 Bleve IndexMapping
	bleveMapping := mapping.NewIndexMapping()

	// 第一步：收集所有日期格式
	dateFormats := make(map[string]bool)
	h.collectDateFormats(esMapping, dateFormats)

	// 第二步：为每个唯一的日期格式注册自定义日期时间解析器
	for esFormat := range dateFormats {
		if err := h.registerDateTimeParser(bleveMapping, esFormat); err != nil {
			return nil, fmt.Errorf("failed to register date time parser for format '%s': %w", esFormat, err)
		}
	}

	// 第三步：处理 properties（字段映射）
	if properties, ok := esMapping["properties"].(map[string]interface{}); ok {
		// 将 properties 转换为 Bleve 的 DefaultMapping.Properties
		defaultMapping := bleveMapping.DefaultMapping
		if defaultMapping.Properties == nil {
			defaultMapping.Properties = make(map[string]*mapping.DocumentMapping)
		}

		for fieldName, fieldDef := range properties {
			fieldMap, ok := fieldDef.(map[string]interface{})
			if !ok {
				continue
			}

			// 创建字段的 DocumentMapping
			fieldDocMapping := mapping.NewDocumentMapping()

			// 转换字段类型和属性
			if err := h.convertESFieldToBleve(fieldMap, fieldDocMapping); err != nil {
				return nil, fmt.Errorf("failed to convert field %s: %w", fieldName, err)
			}

			defaultMapping.Properties[fieldName] = fieldDocMapping
		}
	}

	// 处理其他 mapping 配置（如 dynamic_templates, _source 等）
	// 这些在 Bleve 中可能没有直接对应，但我们可以记录它们

	return bleveMapping, nil
}

// collectDateFormats 递归收集所有日期格式
func (h *IndexHandler) collectDateFormats(mappingData map[string]interface{}, dateFormats map[string]bool) {
	// 处理 properties
	if properties, ok := mappingData["properties"].(map[string]interface{}); ok {
		for _, fieldDef := range properties {
			fieldMap, ok := fieldDef.(map[string]interface{})
			if !ok {
				continue
			}

			fieldType, _ := fieldMap["type"].(string)
			if fieldType == "date" {
				if format, ok := fieldMap["format"].(string); ok && format != "" {
					dateFormats[format] = true
				}
			} else if fieldType == "object" || fieldType == "nested" {
				// 递归处理嵌套字段
				if nestedProps, ok := fieldMap["properties"].(map[string]interface{}); ok {
					h.collectDateFormats(map[string]interface{}{"properties": nestedProps}, dateFormats)
				}
			}
		}
	}
}

// convertESDateFormatToGo 将 ES 日期格式转换为 Go 的 time.Parse 格式
// ES 格式示例: "yyyy-MM-dd HH:mm:ss" -> Go 格式: "2006-01-02 15:04:05"
func convertESDateFormatToGo(esFormat string) string {
	// ES 日期格式到 Go 格式的映射
	replacements := map[string]string{
		"yyyy": "2006",
		"YYYY": "2006", // ES 中 YYYY 和 yyyy 在某些情况下相同
		"MM":   "01",
		"dd":   "02",
		"DD":   "02", // ES 中 DD 和 dd 在某些情况下相同
		"HH":   "15",
		"hh":   "03", // 12小时制
		"mm":   "04",
		"ss":   "05",
		"SSS":  "000",    // 毫秒
		"Z":    "Z07:00", // 时区
		"z":    "MST",    // 时区缩写
	}

	result := esFormat
	// 按长度从长到短排序，避免部分匹配问题
	order := []string{"yyyy", "YYYY", "SSS", "HH", "hh", "MM", "dd", "DD", "mm", "ss", "Z", "z"}
	for _, pattern := range order {
		if replacement, ok := replacements[pattern]; ok {
			result = strings.ReplaceAll(result, pattern, replacement)
		}
	}

	return result
}

// registerDateTimeParser 为 ES 日期格式注册 Bleve 日期时间解析器
func (h *IndexHandler) registerDateTimeParser(bleveMapping *mapping.IndexMappingImpl, esFormat string) error {
	// 如果格式为空，使用默认解析器
	if esFormat == "" {
		return nil
	}

	// 检查是否已经注册过
	if _, exists := bleveMapping.CustomAnalysis.DateTimeParsers[esFormat]; exists {
		return nil
	}

	// 将 ES 格式转换为 Go 格式
	goFormat := convertESDateFormatToGo(esFormat)

	// 使用 flexiblego 解析器，它接受 Go 的 time.Parse 格式
	config := map[string]interface{}{
		"type":    "flexiblego",
		"layouts": []interface{}{goFormat}, // 注意：必须是 []interface{} 而不是 []string
	}

	// 注册自定义日期时间解析器
	if err := bleveMapping.AddCustomDateTimeParser(esFormat, config); err != nil {
		return fmt.Errorf("failed to add custom date time parser: %w", err)
	}

	return nil
}

// convertESFieldToBleve 将 ES 字段定义转换为 Bleve FieldMapping
func (h *IndexHandler) convertESFieldToBleve(fieldMap map[string]interface{}, docMapping *mapping.DocumentMapping) error {
	fieldType, ok := fieldMap["type"].(string)
	if !ok {
		// 如果没有 type，默认为 text
		fieldType = "text"
	}

	var fieldMapping *mapping.FieldMapping

	switch fieldType {
	case "text":
		fieldMapping = mapping.NewTextFieldMapping()
		// 处理 analyzer
		if analyzer, ok := fieldMap["analyzer"].(string); ok {
			fieldMapping.Analyzer = analyzer
		}
		// 处理 search_analyzer
		if searchAnalyzer, ok := fieldMap["search_analyzer"].(string); ok {
			// Bleve 不支持单独的 search_analyzer，使用 analyzer
			if fieldMapping.Analyzer == "" {
				fieldMapping.Analyzer = searchAnalyzer
			}
		}

	case "keyword":
		fieldMapping = mapping.NewKeywordFieldMapping()

	case "long", "integer", "short", "byte":
		fieldMapping = mapping.NewNumericFieldMapping()

	case "double", "float":
		fieldMapping = mapping.NewNumericFieldMapping()

	case "boolean":
		fieldMapping = mapping.NewBooleanFieldMapping()

	case "date":
		fieldMapping = mapping.NewDateTimeFieldMapping()
		// 处理 format
		if format, ok := fieldMap["format"].(string); ok {
			// ES 日期格式需要转换为 Bleve 解析器名称
			// 解析器名称就是格式字符串本身（已注册）
			fieldMapping.DateFormat = format
		}

	case "object", "nested":
		// 对于 object 和 nested，需要递归处理 properties
		if properties, ok := fieldMap["properties"].(map[string]interface{}); ok {
			if docMapping.Properties == nil {
				docMapping.Properties = make(map[string]*mapping.DocumentMapping)
			}
			for propName, propDef := range properties {
				propMap, ok := propDef.(map[string]interface{})
				if !ok {
					continue
				}
				propDocMapping := mapping.NewDocumentMapping()
				if err := h.convertESFieldToBleve(propMap, propDocMapping); err != nil {
					return fmt.Errorf("failed to convert nested field %s: %w", propName, err)
				}
				docMapping.Properties[propName] = propDocMapping
			}
		}
		// object/nested 类型不需要 FieldMapping，直接返回
		return nil

	case "join":
		// join 字段用于父子文档关系
		// 在 Bleve 中，我们将 join 字段作为 keyword 存储
		// 同时存储 _join_name（文档类型）和 _join_parent（父文档ID）
		fieldMapping = mapping.NewKeywordFieldMapping()
		// join 字段的 relations 配置会在索引创建时保存到元数据中

	case "percolator":
		// percolator 字段用于存储查询
		// 在 Bleve 中，我们将查询序列化为 JSON 字符串存储
		fieldMapping = mapping.NewTextFieldMapping()
		fieldMapping.Index = false // 不索引，只存储
		fieldMapping.Store = true

	default:
		// 未知类型，默认为 text
		fieldMapping = mapping.NewTextFieldMapping()
	}

	// 处理通用属性
	if index, ok := fieldMap["index"].(bool); ok {
		fieldMapping.Index = index
	}
	if store, ok := fieldMap["store"].(bool); ok {
		fieldMapping.Store = store
	}
	if docValues, ok := fieldMap["doc_values"].(bool); ok {
		fieldMapping.DocValues = docValues
	}

	// 添加到 DocumentMapping
	docMapping.AddFieldMapping(fieldMapping)

	return nil
}

// GetIndex 获取索引信息
func (h *IndexHandler) GetIndex(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 获取元数据
	indexMeta, err := h.metaStore.GetIndexMetadata(indexName)
	if err != nil {
		// 如果元数据不存在，记录警告并返回基本结构
		logger.Warn("Index metadata not found for index [%s], using default structure: %v", indexName, err)
		indexMeta = &metadata.IndexMetadata{
			Name:     indexName,
			Mapping:  make(map[string]interface{}),
			Settings: make(map[string]interface{}),
			Aliases:  []string{},
		}
	}

	// 构建ES格式响应
	// 构建ES格式响应，直接使用保存的完整 settings 和 mappings
	var settings map[string]interface{}
	if len(indexMeta.Settings) == 0 {
		settings = map[string]interface{}{
			"index": map[string]interface{}{
				"number_of_shards":   "1",
				"number_of_replicas": "0",
			},
		}
	} else {
		settings = indexMeta.Settings
	}

	// ES 7.x+ 格式：mappings 需要包装在一个默认类型名称下
	// ES 官方行为：
	// 1. 如果 mapping 中已经有类型名称（如从 ES 6.x 迁移），使用那个类型名称
	// 2. 如果没有类型名称，检查是否有 properties 或 dynamic_templates（说明是 ES 7.x+ 格式）
	// 3. 如果是 ES 7.x+ 格式，使用 "_doc" 作为默认类型名称
	mappingsWithType := indexMeta.Mapping
	if mappingsWithType != nil {
		// 检查是否已经有类型包装
		// 如果顶级 key 中有 "properties" 或 "dynamic_templates"，说明没有类型包装
		_, hasProperties := mappingsWithType["properties"]
		_, hasDynamicTemplates := mappingsWithType["dynamic_templates"]

		if hasProperties || hasDynamicTemplates {
			// 没有类型包装，使用 "_doc" 作为默认类型名称
			mappingsWithType = map[string]interface{}{
				"_doc": indexMeta.Mapping,
			}
		}
		// 否则，说明已经有类型包装了（如 {"ips": {...}}），直接使用
	} else {
		mappingsWithType = map[string]interface{}{
			"_doc": make(map[string]interface{}),
		}
	}

	indexInfo := map[string]interface{}{
		"aliases":  h.buildAliasesMap(indexMeta.Aliases),
		"mappings": mappingsWithType,
		"settings": settings,
	}

	// ES 官方格式：GET /{index} 直接返回索引信息，不包含 acknowledged 字段
	// 格式：{"index_name": {"aliases": {}, "mappings": {...}, "settings": {...}}}
	response := map[string]interface{}{
		indexName: indexInfo,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(response); err != nil {
		logger.Error("Failed to encode index response: %v", err)
	}
}

// HeadIndex 检查索引是否存在 (HEAD /{index})
func (h *IndexHandler) HeadIndex(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// 索引存在，返回200
	w.WriteHeader(http.StatusOK)
}

// DeleteIndex 删除索引
// DELETE /{index}
// 支持多索引：DELETE /index1,index2,index3
func (h *IndexHandler) DeleteIndex(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 支持多索引（逗号分隔）
	indexNames := strings.Split(indexName, ",")
	validIndices := make([]string, 0, len(indexNames))
	invalidIndices := make([]string, 0)
	errors := make(map[string]string) // 记录每个索引的删除错误

	for _, idx := range indexNames {
		idx = strings.TrimSpace(idx)
		if idx == "" {
			continue
		}

		// 验证索引名称
		if err := common.ValidateIndexName(idx); err != nil {
			invalidIndices = append(invalidIndices, idx)
			errors[idx] = err.Error()
			continue
		}

		// 检查索引是否存在
		if !h.dirMgr.IndexExists(idx) {
			invalidIndices = append(invalidIndices, idx)
			errors[idx] = "index not found"
			continue
		}

		// 先关闭索引（从 IndexManager 中移除），释放文件句柄
		// 这很重要，特别是在 Windows 上，文件被占用时无法删除
		if h.indexMgr != nil {
			// 尝试关闭索引，如果失败记录警告但不中断删除流程
			if closeErr := h.indexMgr.CloseIndex(idx); closeErr != nil {
				logger.Warn("Failed to close index [%s] before deletion: %v", idx, closeErr)
			}
		}

		// 删除元数据（即使失败也继续删除目录，保证数据一致性）
		if err := h.metaStore.DeleteIndexMetadata(idx); err != nil {
			// 记录错误但不中断删除流程，确保即使元数据删除失败，目录也能被删除，避免数据不一致
			logger.Warn("Failed to delete index metadata for index [%s], continuing with directory deletion: %v", idx, err)
		}

		// 删除目录
		if err := h.dirMgr.DeleteIndex(idx); err != nil {
			errors[idx] = err.Error()
			logger.Error("Failed to delete index [%s]: %v", idx, err)
			invalidIndices = append(invalidIndices, idx)
			continue
		}

		// 使索引状态缓存失效（虽然已经关闭了，但为了确保一致性）
		if h.indexMgr != nil {
			h.indexMgr.InvalidateIndexStatus(idx)
		}

		validIndices = append(validIndices, idx)
	}

	// ES 规范：删除索引 API 的返回格式
	// 1. 如果所有索引都无效，返回错误
	if len(validIndices) == 0 {
		if len(invalidIndices) == 1 {
			// 单个索引不存在，返回 404
			common.HandleError(w, common.NewIndexNotFoundError(invalidIndices[0]))
		} else {
			// 多个索引都无效，返回 400
			errorMsg := fmt.Sprintf("failed to delete indices: %v", invalidIndices)
			if len(errors) > 0 {
				errorDetails := make([]string, 0, len(errors))
				for idx, errMsg := range errors {
					errorDetails = append(errorDetails, fmt.Sprintf("%s: %s", idx, errMsg))
				}
				errorMsg += " (" + strings.Join(errorDetails, "; ") + ")"
			}
			common.HandleError(w, common.NewBadRequestError(errorMsg))
		}
		return
	}

	// 2. ES 规范：删除索引 API 返回格式为 {"acknowledged": true}
	// 不包含索引列表，即使删除多个索引也是如此
	// 如果有部分索引删除失败，记录警告日志，但仍返回成功响应
	if len(invalidIndices) > 0 {
		logger.Warn("Some indices failed to delete: %v (errors: %v)", invalidIndices, errors)
	}

	// ES 官方规范：删除索引 API 返回格式为 {"acknowledged": true}
	// 无论删除单个还是多个索引，都只返回 acknowledged，不包含其他字段
	resp := common.SuccessResponse().
		WithAcknowledged(true)
	common.HandleSuccess(w, resp, http.StatusOK)
}

// GetMapping 获取索引映射
func (h *IndexHandler) GetMapping(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 获取元数据
	indexMeta, err := h.metaStore.GetIndexMetadata(indexName)
	if err != nil {
		// 如果元数据不存在，记录警告并返回空映射
		logger.Warn("Index metadata not found for index [%s] when getting mapping, using empty mapping: %v", indexName, err)
		indexMeta = &metadata.IndexMetadata{
			Name:    indexName,
			Mapping: make(map[string]interface{}),
		}
	}

	// 构建ES格式响应
	resp := common.SuccessResponse().WithData(map[string]interface{}{
		indexName: map[string]interface{}{
			"mappings": indexMeta.Mapping,
		},
	})
	common.HandleSuccess(w, resp, http.StatusOK)
}

// GetSettings 获取索引设置
// GET /{index}/_settings, GET /_all/_settings
func (h *IndexHandler) GetSettings(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 如果从路由变量中获取不到index，尝试从URL路径中提取
	if indexName == "" {
		// 检查是否是 /_all/_settings 路径
		if r.URL.Path == "/_all/_settings" {
			indexName = "_all"
		} else {
			// 尝试从路径中提取索引名
			// 路径格式应该是 /{index}/_settings
			path := r.URL.Path
			if strings.HasSuffix(path, "/_settings") {
				parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
				if len(parts) >= 2 && parts[1] == "_settings" {
					indexName = parts[0]
				}
			}
		}
	}

	// 处理 _all 特殊索引名
	if indexName == "_all" {
		// 获取所有索引的设置
		indices, err := h.dirMgr.ListIndices()
		if err != nil {
			logger.Error("Failed to list indices for get all settings: %v", err)
			common.HandleError(w, common.NewInternalServerError("failed to list indices: "+err.Error()))
			return
		}

		allSettings := make(map[string]interface{})
		for _, idxName := range indices {
			indexMeta, err := h.metaStore.GetIndexMetadata(idxName)
			if err != nil {
				logger.Warn("Failed to get metadata for index [%s]: %v", idxName, err)
				// 使用默认设置
				allSettings[idxName] = map[string]interface{}{
					"settings": map[string]interface{}{
						"index": map[string]interface{}{
							"number_of_shards":   "1",
							"number_of_replicas": "0",
						},
					},
				}
				continue
			}

			// 构建ES格式的settings响应
			// 将值字符串化，保持与ES一致
			indexSettings := make(map[string]interface{})
			for k, v := range indexMeta.Settings {
				switch tv := v.(type) {
				case string:
					indexSettings[k] = tv
				case []byte:
					indexSettings[k] = string(tv)
				case fmt.Stringer:
					indexSettings[k] = tv.String()
				default:
					indexSettings[k] = fmt.Sprintf("%v", v)
				}
			}
			settings := map[string]interface{}{"index": indexSettings}
			// 如果没有settings，使用默认值（字符串）
			if len(indexMeta.Settings) == 0 {
				settings["index"] = map[string]interface{}{
					"number_of_shards":   "1",
					"number_of_replicas": "0",
				}
			}

			allSettings[idxName] = map[string]interface{}{
				"settings": settings,
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		if err := encoder.Encode(allSettings); err != nil {
			logger.Error("Failed to encode all settings response: %v", err)
		}
		return
	}

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 获取元数据
	indexMeta, err := h.metaStore.GetIndexMetadata(indexName)
	if err != nil {
		logger.Warn("Index metadata not found for index [%s], using default settings: %v", indexName, err)
		// 返回默认设置
		settings := map[string]interface{}{
			"settings": map[string]interface{}{
				"index": map[string]interface{}{
					"number_of_shards":   "1",
					"number_of_replicas": "0",
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		if err := encoder.Encode(map[string]interface{}{indexName: settings}); err != nil {
			logger.Error("Failed to encode settings response: %v", err)
		}
		return
	}

	// 构建ES格式的settings响应
	// ES settings 格式：{"settings": {"index": {...}, "analysis": {...}, ...}}
	// 直接返回保存的完整 settings，保持原始结构
	var settings map[string]interface{}
	if len(indexMeta.Settings) == 0 {
		// 如果没有settings，使用默认值
		settings = map[string]interface{}{
			"index": map[string]interface{}{
				"number_of_shards":   "1",
				"number_of_replicas": "0",
			},
		}
	} else {
		// 直接使用保存的完整 settings（保持原始结构）
		settings = indexMeta.Settings
	}

	// 构建ES格式响应
	response := map[string]interface{}{
		indexName: map[string]interface{}{
			"settings": settings,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(response); err != nil {
		logger.Error("Failed to encode settings response: %v", err)
	}
}

// UpdateSettings 更新索引设置
// PUT /{index}/_settings
func (h *IndexHandler) UpdateSettings(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 验证索引名称
	if err := common.ValidateIndexName(indexName); err != nil {
		common.HandleError(w, common.NewBadRequestError(err.Error()))
		return
	}

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 解析请求体
	var requestBody map[string]interface{}
	// 保持最大体积限制（如配置所需，可在解码前做 io.LimitReader 包装）
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&requestBody); err != nil {
		if err == io.EOF {
			common.HandleError(w, common.NewBadRequestError("request body is required"))
			return
		}
		common.HandleError(w, common.NewBadRequestError("invalid JSON body: "+err.Error()))
		return
	}

	// 获取现有元数据
	indexMeta, err := h.metaStore.GetIndexMetadata(indexName)
	if err != nil {
		logger.Error("Failed to get index metadata for [%s]: %v", indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to get index metadata: "+err.Error()))
		return
	}

	// 保存原始 settings 用于比较
	originalSettings := make(map[string]interface{})
	if indexMeta.Settings != nil {
		// 深拷贝原始 settings
		originalSettingsBytes, _ := json.Marshal(indexMeta.Settings)
		json.Unmarshal(originalSettingsBytes, &originalSettings)
	}

	// 更新settings（合并到现有settings中）
	if settings, ok := requestBody["settings"].(map[string]interface{}); ok {
		// 合并settings
		if indexMeta.Settings == nil {
			indexMeta.Settings = make(map[string]interface{})
		}
		for k, v := range settings {
			indexMeta.Settings[k] = v
		}
	} else if indexSettings, ok := requestBody["index"].(map[string]interface{}); ok {
		// ES格式：{"index": {"setting": "value"}}
		if indexMeta.Settings == nil {
			indexMeta.Settings = make(map[string]interface{})
		}
		for k, v := range indexSettings {
			indexMeta.Settings[k] = v
		}
	} else {
		// 直接是settings对象
		if indexMeta.Settings == nil {
			indexMeta.Settings = make(map[string]interface{})
		}
		for k, v := range requestBody {
			indexMeta.Settings[k] = v
		}
	}

	// 检查 settings 是否真的发生了变化（深度比较）
	settingsChanged := !equalSettingsMaps(originalSettings, indexMeta.Settings)

	// 只有在 settings 真的发生变化时才保存元数据
	if settingsChanged {
		// 更新元数据
		indexMeta.UpdatedAt = time.Now()
		if err := h.metaStore.SaveIndexMetadata(indexName, indexMeta); err != nil {
			logger.Error("Failed to save index metadata for [%s]: %v", indexName, err)
			common.HandleError(w, common.NewInternalServerError("failed to update index settings: "+err.Error()))
			return
		}
	} else {
		logger.Debug("UpdateSettings [%s] - No settings changes detected, skipping metadata save", indexName)
	}

	// 返回成功响应
	resp := common.SuccessResponse().
		WithAcknowledged(true).
		WithIndex(indexName)
	common.HandleSuccess(w, resp, http.StatusOK)
}

// CloseIndex 关闭索引
// POST /{index}/_close
func (h *IndexHandler) CloseIndex(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 验证索引名称
	if err := common.ValidateIndexName(indexName); err != nil {
		common.HandleError(w, common.NewBadRequestError(err.Error()))
		return
	}

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 注意：在单节点模式下，关闭索引主要是从IndexManager缓存中移除
	// 实际的索引文件仍然存在，可以重新打开
	// 这里我们只返回成功响应，实际的关闭操作由IndexManager处理

	// 返回成功响应
	resp := common.SuccessResponse().
		WithAcknowledged(true).
		WithIndex(indexName)
	common.HandleSuccess(w, resp, http.StatusOK)
}

// OpenIndex 打开索引
// POST /{index}/_open
func (h *IndexHandler) OpenIndex(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 验证索引名称
	if err := common.ValidateIndexName(indexName); err != nil {
		common.HandleError(w, common.NewBadRequestError(err.Error()))
		return
	}

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 注意：在单节点模式下，打开索引主要是确保索引在IndexManager缓存中
	// 实际的打开操作会在GetIndex时自动完成
	// 这里我们只返回成功响应

	// 返回成功响应
	resp := common.SuccessResponse().
		WithAcknowledged(true).
		WithIndex(indexName)
	common.HandleSuccess(w, resp, http.StatusOK)
}

// RefreshIndex 刷新索引
// POST /{index}/_refresh
// 支持多索引：POST /index1,index2,index3/_refresh
func (h *IndexHandler) RefreshIndex(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 支持多索引（逗号分隔）
	indexNames := strings.Split(indexName, ",")
	validIndices := make([]string, 0, len(indexNames))
	invalidIndices := make([]string, 0)

	for _, idx := range indexNames {
		idx = strings.TrimSpace(idx)
		if idx == "" {
			continue
		}

		// 验证索引名称
		if err := common.ValidateIndexName(idx); err != nil {
			invalidIndices = append(invalidIndices, idx)
			continue
		}

		// 检查索引是否存在
		if !h.dirMgr.IndexExists(idx) {
			invalidIndices = append(invalidIndices, idx)
			continue
		}

		validIndices = append(validIndices, idx)
	}

	// 如果有无效索引，返回错误
	if len(invalidIndices) > 0 {
		common.HandleError(w, common.NewBadRequestError(fmt.Sprintf("invalid or non-existent indices: %v", invalidIndices)))
		return
	}

	// 如果没有有效索引，返回错误
	if len(validIndices) == 0 {
		common.HandleError(w, common.NewBadRequestError("no valid indices specified"))
		return
	}

	// 注意：bleve索引是实时更新的，不需要显式刷新
	// 但为了符合ES API规范，我们返回成功响应
	// 如果需要，可以在这里触发索引的强制刷新操作

	// 返回成功响应（多索引时返回第一个索引名称，符合ES规范）
	resp := common.SuccessResponse().
		WithAcknowledged(true).
		WithIndex(validIndices[0])
	common.HandleSuccess(w, resp, http.StatusOK)
}

// FlushIndex 刷新索引到磁盘
// POST /{index}/_flush
func (h *IndexHandler) FlushIndex(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 验证索引名称
	if err := common.ValidateIndexName(indexName); err != nil {
		common.HandleError(w, common.NewBadRequestError(err.Error()))
		return
	}

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 注意：bleve索引会自动刷新到磁盘
	// 但为了符合ES API规范，我们返回成功响应
	// 如果需要，可以在这里触发索引的强制刷新操作

	// 返回成功响应
	resp := common.SuccessResponse().
		WithAcknowledged(true).
		WithIndex(indexName)
	common.HandleSuccess(w, resp, http.StatusOK)
}

// buildAliasesMap 构建别名映射（ES格式）
func (h *IndexHandler) buildAliasesMap(aliases []string) map[string]interface{} {
	if len(aliases) == 0 {
		return make(map[string]interface{})
	}

	// 预分配容量以提高性能
	result := make(map[string]interface{}, len(aliases))
	for _, alias := range aliases {
		if alias != "" { // 忽略空别名
			result[alias] = map[string]interface{}{}
		}
	}
	return result
}

// GetAlias 获取索引的别名
// GET /{index}/_alias
func (h *IndexHandler) GetAlias(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 验证索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 获取元数据
	indexMeta, err := h.metaStore.GetIndexMetadata(indexName)
	if err != nil {
		// 如果元数据不存在，返回空别名
		logger.Warn("Index metadata not found for index [%s], returning empty aliases: %v", indexName, err)
		aliases := make(map[string]interface{})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		if err := encoder.Encode(aliases); err != nil {
			logger.Error("Failed to encode aliases response: %v", err)
		}
		return
	}

	// 构建ES格式响应
	aliases := h.buildAliasesMap(indexMeta.Aliases)

	// ES格式：{ "index_name": { "aliases": { "alias1": {}, "alias2": {} } } }
	response := map[string]interface{}{
		indexName: map[string]interface{}{
			"aliases": aliases,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(response); err != nil {
		logger.Error("Failed to encode aliases response: %v", err)
	}
}

// PutAlias 创建或更新别名
// PUT /{index}/_alias/{name}
func (h *IndexHandler) PutAlias(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	aliasName := mux.Vars(r)["name"]

	// 验证索引名称和别名
	if err := common.ValidateIndexName(indexName); err != nil {
		common.HandleError(w, common.NewBadRequestError(err.Error()))
		return
	}
	if aliasName == "" {
		common.HandleError(w, common.NewBadRequestError("alias name cannot be empty"))
		return
	}

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 获取元数据
	indexMeta, err := h.metaStore.GetIndexMetadata(indexName)
	if err != nil {
		// 如果元数据不存在，创建新的
		indexMeta = &metadata.IndexMetadata{
			Name:      indexName,
			Aliases:   []string{},
			Mapping:   make(map[string]interface{}),
			Settings:  make(map[string]interface{}),
			Version:   1,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}

	// 检查别名是否已存在
	aliasExists := false
	for _, alias := range indexMeta.Aliases {
		if alias == aliasName {
			aliasExists = true
			break
		}
	}

	// 如果不存在，添加别名
	if !aliasExists {
		indexMeta.Aliases = append(indexMeta.Aliases, aliasName)
		indexMeta.UpdatedAt = time.Now()
	}

	// 保存元数据
	if err := h.metaStore.SaveIndexMetadata(indexName, indexMeta); err != nil {
		logger.Error("Failed to save index metadata for [%s]: %v", indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to create alias: "+err.Error()))
		return
	}

	// 返回成功响应
	resp := common.SuccessResponse().
		WithAcknowledged(true).
		WithIndex(indexName)
	common.HandleSuccess(w, resp, http.StatusOK)
}

// DeleteAlias 删除别名
// DELETE /{index}/_alias/{name}
func (h *IndexHandler) DeleteAlias(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	aliasName := mux.Vars(r)["name"]

	// 验证索引名称和别名
	if err := common.ValidateIndexName(indexName); err != nil {
		common.HandleError(w, common.NewBadRequestError(err.Error()))
		return
	}
	if aliasName == "" {
		common.HandleError(w, common.NewBadRequestError("alias name cannot be empty"))
		return
	}

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 获取元数据
	indexMeta, err := h.metaStore.GetIndexMetadata(indexName)
	if err != nil {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 查找并删除别名
	found := false
	newAliases := make([]string, 0, len(indexMeta.Aliases))
	for _, alias := range indexMeta.Aliases {
		if alias != aliasName {
			newAliases = append(newAliases, alias)
		} else {
			found = true
		}
	}

	if !found {
		common.HandleError(w, common.NewBadRequestError("alias ["+aliasName+"] missing"))
		return
	}

	// 更新别名列表
	indexMeta.Aliases = newAliases
	indexMeta.UpdatedAt = time.Now()

	// 保存元数据
	if err := h.metaStore.SaveIndexMetadata(indexName, indexMeta); err != nil {
		logger.Error("Failed to save index metadata for [%s]: %v", indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to delete alias: "+err.Error()))
		return
	}

	// 返回成功响应
	resp := common.SuccessResponse().
		WithAcknowledged(true).
		WithIndex(indexName)
	common.HandleSuccess(w, resp, http.StatusOK)
}

// GetAllAliases 获取所有别名
// GET /_alias
func (h *IndexHandler) GetAllAliases(w http.ResponseWriter, r *http.Request) {
	// 获取所有索引
	indices, err := h.dirMgr.ListIndices()
	if err != nil {
		logger.Error("Failed to list indices: %v", err)
		common.HandleError(w, common.NewInternalServerError("failed to list indices: "+err.Error()))
		return
	}

	// 构建响应：{ "index_name": { "aliases": { "alias1": {}, "alias2": {} } } }
	response := make(map[string]interface{})

	for _, indexName := range indices {
		indexMeta, err := h.metaStore.GetIndexMetadata(indexName)
		if err != nil {
			// 如果元数据不存在，跳过
			continue
		}

		if len(indexMeta.Aliases) > 0 {
			aliases := h.buildAliasesMap(indexMeta.Aliases)
			response[indexName] = map[string]interface{}{
				"aliases": aliases,
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(response); err != nil {
		logger.Error("Failed to encode aliases response: %v", err)
	}
}

// GetAliasByName 根据别名名称获取索引
// GET /_alias/{name}
func (h *IndexHandler) GetAliasByName(w http.ResponseWriter, r *http.Request) {
	aliasName := mux.Vars(r)["name"]

	if aliasName == "" {
		common.HandleError(w, common.NewBadRequestError("alias name cannot be empty"))
		return
	}

	// 获取所有索引
	indices, err := h.dirMgr.ListIndices()
	if err != nil {
		logger.Error("Failed to list indices: %v", err)
		common.HandleError(w, common.NewInternalServerError("failed to list indices: "+err.Error()))
		return
	}

	// 查找包含该别名的索引
	response := make(map[string]interface{})
	found := false

	for _, indexName := range indices {
		indexMeta, err := h.metaStore.GetIndexMetadata(indexName)
		if err != nil {
			continue
		}

		// 检查索引是否包含该别名
		for _, alias := range indexMeta.Aliases {
			if alias == aliasName {
				aliases := h.buildAliasesMap([]string{aliasName})
				response[indexName] = map[string]interface{}{
					"aliases": aliases,
				}
				found = true
				break
			}
		}
	}

	if !found {
		common.HandleError(w, common.NewBadRequestError("alias ["+aliasName+"] missing"))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(response); err != nil {
		logger.Error("Failed to encode aliases response: %v", err)
	}
}

// UpdateAliases 批量更新别名（原子操作）
// POST /_aliases
func (h *IndexHandler) UpdateAliases(w http.ResponseWriter, r *http.Request) {
	// 解析请求体（兼容 chunked）
	var requestBody map[string]interface{}
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&requestBody); err != nil && err != io.EOF {
		common.HandleError(w, common.NewBadRequestError("invalid JSON body: "+err.Error()))
		return
	}

	// 解析 actions 数组
	actions, ok := requestBody["actions"].([]interface{})
	if !ok {
		common.HandleError(w, common.NewBadRequestError("request body must contain 'actions' array"))
		return
	}

	// 执行所有操作
	for _, actionItem := range actions {
		action, ok := actionItem.(map[string]interface{})
		if !ok {
			logger.Warn("Invalid action item: %v", actionItem)
			continue
		}

		// 处理 add 操作
		if addAction, ok := action["add"].(map[string]interface{}); ok {
			indexName, _ := addAction["index"].(string)
			aliasName, _ := addAction["alias"].(string)

			if indexName == "" || aliasName == "" {
				logger.Warn("Invalid add action: %v", addAction)
				continue
			}

			// 检查索引是否存在
			if !h.dirMgr.IndexExists(indexName) {
				logger.Warn("Index [%s] not found for alias [%s]", indexName, aliasName)
				continue
			}

			// 获取元数据
			indexMeta, err := h.metaStore.GetIndexMetadata(indexName)
			if err != nil {
				// 如果元数据不存在，创建新的
				indexMeta = &metadata.IndexMetadata{
					Name:      indexName,
					Aliases:   []string{},
					Mapping:   make(map[string]interface{}),
					Settings:  make(map[string]interface{}),
					Version:   1,
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				}
			}

			// 检查别名是否已存在
			aliasExists := false
			for _, alias := range indexMeta.Aliases {
				if alias == aliasName {
					aliasExists = true
					break
				}
			}

			// 如果不存在，添加别名
			if !aliasExists {
				indexMeta.Aliases = append(indexMeta.Aliases, aliasName)
				indexMeta.UpdatedAt = time.Now()

				// 保存元数据
				if err := h.metaStore.SaveIndexMetadata(indexName, indexMeta); err != nil {
					logger.Error("Failed to save index metadata for [%s]: %v", indexName, err)
				}
			}
		}

		// 处理 remove 操作
		if removeAction, ok := action["remove"].(map[string]interface{}); ok {
			indexName, _ := removeAction["index"].(string)
			aliasName, _ := removeAction["alias"].(string)

			if indexName == "" || aliasName == "" {
				logger.Warn("Invalid remove action: %v", removeAction)
				continue
			}

			// 获取元数据
			indexMeta, err := h.metaStore.GetIndexMetadata(indexName)
			if err != nil {
				logger.Warn("Index metadata not found for [%s]", indexName)
				continue
			}

			// 查找并删除别名
			newAliases := make([]string, 0, len(indexMeta.Aliases))
			for _, alias := range indexMeta.Aliases {
				if alias != aliasName {
					newAliases = append(newAliases, alias)
				}
			}

			// 更新别名列表
			indexMeta.Aliases = newAliases
			indexMeta.UpdatedAt = time.Now()

			// 保存元数据
			if err := h.metaStore.SaveIndexMetadata(indexName, indexMeta); err != nil {
				logger.Error("Failed to save index metadata for [%s]: %v", indexName, err)
			}
		}

		// 处理 remove_index 操作（删除索引的所有别名）
		if removeIndexAction, ok := action["remove_index"].(map[string]interface{}); ok {
			indexName, _ := removeIndexAction["index"].(string)

			if indexName == "" {
				logger.Warn("Invalid remove_index action: %v", removeIndexAction)
				continue
			}

			// 获取元数据
			indexMeta, err := h.metaStore.GetIndexMetadata(indexName)
			if err != nil {
				logger.Warn("Index metadata not found for [%s]", indexName)
				continue
			}

			// 清空别名列表
			indexMeta.Aliases = []string{}
			indexMeta.UpdatedAt = time.Now()

			// 保存元数据
			if err := h.metaStore.SaveIndexMetadata(indexName, indexMeta); err != nil {
				logger.Error("Failed to save index metadata for [%s]: %v", indexName, err)
			}
		}
	}

	// 返回成功响应
	resp := common.SuccessResponse().
		WithAcknowledged(true)
	common.HandleSuccess(w, resp, http.StatusOK)
}

// UpdateMapping 更新索引的 mapping
// PUT /{index}/_mapping
func (h *IndexHandler) UpdateMapping(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 验证索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 解析请求体（兼容 chunked 传输，不依赖 Content-Length）
	var reqBody map[string]interface{}
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&reqBody); err != nil {
		if err == io.EOF {
			common.HandleError(w, common.NewBadRequestError("request body is required"))
			return
		}
		common.HandleError(w, common.NewBadRequestError("invalid JSON body: "+err.Error()))
		return
	}

	// 支持 {"mappings": {...}} 或 直接 {...}
	var newMapping map[string]interface{}
	if m, ok := reqBody["mappings"].(map[string]interface{}); ok {
		newMapping = m
	} else {
		newMapping = reqBody
	}

	// 获取现有元数据
	indexMeta, err := h.metaStore.GetIndexMetadata(indexName)
	if err != nil {
		logger.Error("Failed to get index metadata for [%s]: %v", indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to get index metadata: "+err.Error()))
		return
	}

	// 调试：记录更新前的 mapping 字段数量
	if props, ok := indexMeta.Mapping["properties"].(map[string]interface{}); ok {
		logger.Debug("UpdateMapping [%s] - Before update, existing mapping has %d properties", indexName, len(props))
	} else {
		logger.Debug("UpdateMapping [%s] - Before update, existing mapping has no properties", indexName)
	}

	// 检查是否是添加新字段还是修改已有字段
	existingProps, _ := indexMeta.Mapping["properties"].(map[string]interface{})
	newProps, hasNewProps := newMapping["properties"].(map[string]interface{})

	// 调试：记录新传入的 mapping 字段数量
	if hasNewProps {
		logger.Debug("UpdateMapping [%s] - New mapping has %d properties", indexName, len(newProps))
	}

	var newFields []string
	var modifiedFields []string

	if hasNewProps && existingProps != nil {
		// 检查哪些是新字段，哪些是修改的字段
		for fieldName := range newProps {
			if _, exists := existingProps[fieldName]; !exists {
				newFields = append(newFields, fieldName)
			} else {
				modifiedFields = append(modifiedFields, fieldName)
			}
		}
	} else if hasNewProps {
		// 如果原来没有 properties，所有字段都是新的
		for fieldName := range newProps {
			newFields = append(newFields, fieldName)
		}
	}

	// ES 行为：添加新字段是允许的，修改已有字段的类型是不允许的
	// 但 Bleve 的动态 mapping 默认启用，新字段会自动处理
	// 如果用户需要为新字段指定特定配置（如 analyzer），这些配置会保存在元数据中
	// 但实际索引时，如果 Bleve 索引的 mapping 中没有该字段，会使用动态 mapping 的默认配置

	// 合并映射（浅合并：properties 等键合并；简单键覆盖）
	// 注意：必须保留现有 mapping 的所有字段，只更新传入的字段
	if indexMeta.Mapping == nil {
		indexMeta.Mapping = make(map[string]interface{})
	}

	// 关键：必须先深拷贝 existingProps，避免在合并时丢失现有字段
	// 如果 existingProps 是 nil，创建一个新的 map
	var mergedProps map[string]interface{}
	if existingProps != nil {
		// 深拷贝现有字段，确保不会丢失
		mergedProps = make(map[string]interface{})
		for k, v := range existingProps {
			mergedProps[k] = v
		}
	} else {
		mergedProps = make(map[string]interface{})
	}

	// 先保留所有现有的映射字段（包括 dynamic_templates 等）
	// 然后只更新传入的字段
	for k, v := range newMapping {
		// 如果是 properties，做键级合并
		if k == "properties" {
			if nv, ok := v.(map[string]interface{}); ok {
				for pk, pv := range nv {
					// 检查是否是修改已有字段的类型
					if existingFieldDef, exists := mergedProps[pk].(map[string]interface{}); exists {
						existingType, _ := existingFieldDef["type"].(string)
						newFieldDef, _ := pv.(map[string]interface{})
						newType, _ := newFieldDef["type"].(string)

						// ES 不允许修改已有字段的类型
						if existingType != "" && newType != "" && existingType != newType {
							common.HandleError(w, common.NewBadRequestError(
								fmt.Sprintf("cannot change field [%s] type from [%s] to [%s]. Use reindex API to update field types", pk, existingType, newType)))
							return
						}
					}
					// 更新或添加字段（保留现有字段，只更新传入的字段）
					mergedProps[pk] = pv
				}
			}
			// 使用合并后的 properties（包含所有现有字段和新字段）
			indexMeta.Mapping["properties"] = mergedProps
			continue
		}
		// 对于其他键（如 dynamic_templates、_source 等），直接覆盖
		indexMeta.Mapping[k] = v
	}

	// 确保 properties 字段始终存在（即使更新时没有传入 properties）
	if indexMeta.Mapping["properties"] == nil {
		if len(mergedProps) > 0 {
			indexMeta.Mapping["properties"] = mergedProps
		} else if existingProps != nil {
			indexMeta.Mapping["properties"] = existingProps
		}
	}

	// 调试：记录保存前的 mapping 字段数量
	if props, ok := indexMeta.Mapping["properties"].(map[string]interface{}); ok {
		logger.Debug("UpdateMapping [%s] - Before save, merged mapping has %d properties", indexName, len(props))
	}

	// 保存元数据
	indexMeta.UpdatedAt = time.Now()
	if err := h.metaStore.SaveIndexMetadata(indexName, indexMeta); err != nil {
		logger.Error("Failed to save index metadata for [%s]: %v", indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to update index mapping: "+err.Error()))
		return
	}

	// 尝试更新 Bleve 索引的 mapping（如果可能）
	// 注意：Bleve 的 updated_mapping 不支持添加新字段，只支持删除或修改字段属性
	// 但由于 Bleve 默认启用了动态 mapping，新字段会自动处理
	// 这里我们只记录日志，不强制更新 Bleve mapping
	if len(newFields) > 0 {
		logger.Info("Added new fields to index [%s]: %v. These fields will use dynamic mapping defaults.", indexName, newFields)
	}
	if len(modifiedFields) > 0 {
		logger.Info("Modified fields in index [%s]: %v. Field type changes are not allowed.", indexName, modifiedFields)
	}

	// 返回 ES 风格确认响应
	resp := map[string]interface{}{
		"acknowledged": true,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(resp); err != nil {
		logger.Error("Failed to encode update mapping response: %v", err)
	}
}

// equalSettingsMaps 深度比较两个 settings map 是否相等
func equalSettingsMaps(m1, m2 map[string]interface{}) bool {
	if len(m1) != len(m2) {
		return false
	}
	for k, v1 := range m1 {
		v2, ok := m2[k]
		if !ok {
			return false
		}
		if !equalSettingsValues(v1, v2) {
			return false
		}
	}
	return true
}

// equalSettingsValues 比较两个 settings 值是否相等（支持嵌套 map、slice 和基本类型）
func equalSettingsValues(v1, v2 interface{}) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}

	// 类型断言：map
	if m1, ok1 := v1.(map[string]interface{}); ok1 {
		if m2, ok2 := v2.(map[string]interface{}); ok2 {
			return equalSettingsMaps(m1, m2)
		}
		return false
	}

	// 类型断言：slice
	if s1, ok1 := v1.([]interface{}); ok1 {
		if s2, ok2 := v2.([]interface{}); ok2 {
			if len(s1) != len(s2) {
				return false
			}
			for i := range s1 {
				if !equalSettingsValues(s1[i], s2[i]) {
					return false
				}
			}
			return true
		}
		return false
	}

	// 基本类型比较（使用 JSON 序列化比较，确保类型一致性）
	v1Bytes, _ := json.Marshal(v1)
	v2Bytes, _ := json.Marshal(v2)
	return string(v1Bytes) == string(v2Bytes)
}

// ForceMerge 强制合并索引段
// POST /{index}/_forcemerge
// POST /_forcemerge
func (h *IndexHandler) ForceMerge(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// 如果没有指定索引，返回错误
	if indexName == "" {
		common.HandleError(w, common.NewBadRequestError("index name is required"))
		return
	}

	// 检查索引是否存在
	if !h.dirMgr.IndexExists(indexName) {
		common.HandleError(w, common.NewIndexNotFoundError(indexName))
		return
	}

	// 获取索引路径
	indexPath := h.dirMgr.GetIndexPath(indexName)

	// 打开索引
	idx, err := bleve.Open(indexPath)
	if err != nil {
		logger.Error("Failed to open index [%s] for force merge: %v", indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to open index: "+err.Error()))
		return
	}
	defer idx.Close()

	// 获取底层索引并触发合并
	advancedIdx, err := idx.Advanced()
	if err != nil {
		logger.Error("Failed to get advanced index [%s]: %v", indexName, err)
		common.HandleError(w, common.NewInternalServerError("failed to get advanced index: "+err.Error()))
		return
	}

	// 尝试触发合并（如果底层索引支持）
	// Scorch 索引的 ForceMerge 方法签名: ForceMerge(ctx context.Context, mo *mergeplan.MergePlanOptions) error
	if merger, ok := advancedIdx.(interface {
		ForceMerge(ctx interface{}, mo interface{}) error
	}); ok {
		if err := merger.ForceMerge(r.Context(), nil); err != nil {
			logger.Error("Failed to force merge index [%s]: %v", indexName, err)
			common.HandleError(w, common.NewInternalServerError("failed to force merge: "+err.Error()))
			return
		}
		logger.Info("Force merge triggered for index [%s]", indexName)
	} else {
		logger.Warn("Index [%s] does not support force merge", indexName)
	}

	// 返回成功响应
	resp := map[string]interface{}{
		"_shards": map[string]interface{}{
			"total":      1,
			"successful": 1,
			"failed":     0,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}
