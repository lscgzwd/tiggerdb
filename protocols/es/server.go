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

package es

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/lscgzwd/tiggerdb/directory"
	"github.com/lscgzwd/tiggerdb/metadata"
	"github.com/lscgzwd/tiggerdb/protocols"
	"github.com/lscgzwd/tiggerdb/protocols/es/handler"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/common"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/server"
	esIndex "github.com/lscgzwd/tiggerdb/protocols/es/index"
	"github.com/lscgzwd/tiggerdb/protocols/es/middleware"
)

// ESServer Elasticsearch协议服务器
type ESServer struct {
	config          *Config
	httpServer      *server.Server
	indexHandler    *handler.IndexHandler
	documentHandler *handler.DocumentHandler
	clusterHandler  *handler.ClusterHandler
	statsHandler    *handler.StatsHandler
	indexMgr        *esIndex.IndexManager
	dirMgr          directory.DirectoryManager
	metaStore       metadata.MetadataStore
	started         bool
	mu              sync.RWMutex
}

// NewServer 创建新的ES协议服务器
func NewServer(dirMgr directory.DirectoryManager, metaStore metadata.MetadataStore, config *Config) (*ESServer, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid ES config: %w", err)
	}

	// 创建HTTP服务器
	httpSrv, err := server.NewServer(config.ServerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP server: %w", err)
	}

	// 创建索引管理器
	indexMgr := esIndex.NewIndexManager(dirMgr, metaStore)

	// 创建索引处理器
	indexHandler := handler.NewIndexHandler(dirMgr, metaStore)
	indexHandler.SetIndexManager(indexMgr)

	// 创建文档处理器
	documentHandler := handler.NewDocumentHandler(indexMgr, dirMgr, metaStore)

	// 创建集群处理器
	clusterHandler := handler.NewClusterHandler(indexMgr, dirMgr, metaStore)

	// 创建统计信息处理器
	statsHandler := handler.NewStatsHandler(indexMgr, dirMgr, metaStore)

	// 创建认证中间件（处理 config.Auth 为 nil 的情况）
	var authMiddleware func(http.Handler) http.Handler
	if config.Auth != nil {
		authMiddleware = middleware.AuthMiddleware(config.Auth)
	} else {
		// 如果未配置认证，使用空中间件（直接放行）
		authMiddleware = func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				next.ServeHTTP(w, r)
			})
		}
	}

	esSrv := &ESServer{
		config:          config,
		httpServer:      httpSrv,
		indexHandler:    indexHandler,
		documentHandler: documentHandler,
		clusterHandler:  clusterHandler,
		statsHandler:    statsHandler,
		indexMgr:        indexMgr,
		dirMgr:          dirMgr,
		metaStore:       metaStore,
		started:         false,
	}

	// P2-6: 设置开发模式（根据日志级别判断）
	if config.ServerConfig != nil && config.ServerConfig.LogLevel == "debug" {
		common.SetDevMode(true)
	}

	// 注册ES路由（传入认证中间件）
	esSrv.registerRoutes(authMiddleware)

	// 预加载所有索引（启动后台合并任务）
	go indexMgr.PreloadAllIndices()

	return esSrv, nil
}

// registerRoutes 注册ES相关路由
func (s *ESServer) registerRoutes(authMiddleware func(http.Handler) http.Handler) {
	router := s.httpServer.GetRouter()

	// 注册索引相关路由（带认证保护）
	s.registerIndexRoutes(router, s.indexHandler, authMiddleware)

	// 注册文档相关路由（带认证保护）
	s.registerDocumentRoutes(router, s.documentHandler, authMiddleware)

	// 注册统计信息路由（带认证保护）
	s.registerStatsRoutes(router, s.statsHandler, authMiddleware)

	// 根路径处理函数（支持 GET 和 HEAD）
	rootHandler := func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"name":         handler.NodeName,
			"cluster_name": handler.ClusterName,
			"cluster_uuid": handler.ClusterUUID,
			"version": map[string]interface{}{
				"number":                              handler.ESVersionNumber,
				"build_flavor":                        "default",
				"build_type":                          "release",
				"build_hash":                          handler.ESBuildHash,
				"build_date":                          handler.ESBuildDate,
				"build_snapshot":                      false,
				"lucene_version":                      handler.ESLuceneVersion,
				"minimum_wire_compatibility_version":  handler.ESMinimumWireCompatibilityVersion,
				"minimum_index_compatibility_version": handler.ESMinimumIndexCompatibilityVersion,
			},
			"tagline": "You Know, for Search",
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		// HEAD 请求不返回响应体
		if r.Method != http.MethodHead {
			encoder := json.NewEncoder(w)
			if err := encoder.Encode(response); err != nil {
				log.Printf("ERROR: Failed to encode root response: %v", err)
			}
		}
	}

	// 添加全局路由（放在最后，确保最高优先级）
	globalRoutes := []server.Route{
		{Method: http.MethodGet, Path: "/", Handler: rootHandler},
		{Method: http.MethodHead, Path: "/", Handler: rootHandler},
		{Method: http.MethodGet, Path: "/_ping", Handler: s.clusterHandler.Ping},
		{Method: http.MethodHead, Path: "/_ping", Handler: s.clusterHandler.Ping},
		{Method: http.MethodGet, Path: "/_search", Handler: s.documentHandler.GlobalSearch},
		{Method: http.MethodPost, Path: "/_search", Handler: s.documentHandler.GlobalSearch},
		{Method: http.MethodGet, Path: "/_cluster/health", Handler: s.clusterHandler.ClusterHealth},
		{Method: http.MethodGet, Path: "/_cluster/state", Handler: s.clusterHandler.ClusterState},
		{Method: http.MethodGet, Path: "/_cluster/stats", Handler: s.clusterHandler.ClusterStats},
		{Method: http.MethodGet, Path: "/_nodes", Handler: s.clusterHandler.NodesInfo},
		{Method: http.MethodGet, Path: "/_cat/nodes", Handler: s.clusterHandler.CatNodes},
		{Method: http.MethodGet, Path: "/_cat/nodes/", Handler: s.clusterHandler.CatNodes},
		{Method: http.MethodGet, Path: "/_cat/indices", Handler: s.clusterHandler.CatIndices},
		{Method: http.MethodGet, Path: "/_cat/indices/", Handler: s.clusterHandler.CatIndices},
		{Method: http.MethodGet, Path: "/_cat/shards", Handler: s.clusterHandler.CatShards},
		{Method: http.MethodGet, Path: "/_cat/shards/", Handler: s.clusterHandler.CatShards},
		{Method: http.MethodGet, Path: "/_all/_settings", Handler: (*s.indexHandler).GetSettings},
		{Method: http.MethodGet, Path: "/_alias", Handler: (*s.indexHandler).GetAllAliases},
		{Method: http.MethodGet, Path: "/_alias/{name}", Handler: (*s.indexHandler).GetAliasByName},
		{Method: http.MethodPost, Path: "/_aliases", Handler: (*s.indexHandler).UpdateAliases},
	}
	router.AddRoutes(globalRoutes)

	// 添加默认路由（健康检查、指标等）
	s.httpServer.AddDefaultRoutes()
}

// registerIndexRoutes 注册ES索引相关路由
func (s *ESServer) registerIndexRoutes(router *server.Router, indexHandler *handler.IndexHandler, authMiddleware func(http.Handler) http.Handler) {
	routes := []server.Route{
		{Method: http.MethodPut, Path: "/{index:[^_][^/]*}", Handler: (*indexHandler).CreateIndex},
		{Method: http.MethodGet, Path: "/{index:[^_][^/]*}", Handler: (*indexHandler).GetIndex},
		{Method: http.MethodHead, Path: "/{index:[^_][^/]*}", Handler: (*indexHandler).HeadIndex},
		{Method: http.MethodDelete, Path: "/{index:[^_][^/]*}", Handler: (*indexHandler).DeleteIndex},
		{Method: http.MethodGet, Path: "/{index:[^_][^/]*}/_mapping", Handler: (*indexHandler).GetMapping},
		{Method: http.MethodPut, Path: "/{index:[^_][^/]*}/_mapping", Handler: (*indexHandler).UpdateMapping},
		{Method: http.MethodGet, Path: "/{index:[^_][^/]*}/_mapping/_all", Handler: (*indexHandler).GetMapping},
		{Method: http.MethodGet, Path: "/{index:[^_][^/]*}/_alias", Handler: (*indexHandler).GetAlias},
		{Method: http.MethodPut, Path: "/{index:[^_][^/]*}/_alias/{name}", Handler: (*indexHandler).PutAlias},
		{Method: http.MethodDelete, Path: "/{index:[^_][^/]*}/_alias/{name}", Handler: (*indexHandler).DeleteAlias},
		{Method: http.MethodGet, Path: "/{index:[^_][^/]*}/_settings", Handler: (*indexHandler).GetSettings},
		{Method: http.MethodPut, Path: "/{index:[^_][^/]*}/_settings", Handler: (*indexHandler).UpdateSettings},
		{Method: http.MethodPost, Path: "/{index:[^_][^/]*}/_close", Handler: (*indexHandler).CloseIndex},
		{Method: http.MethodPost, Path: "/{index:[^_][^/]*}/_open", Handler: (*indexHandler).OpenIndex},
		{Method: http.MethodPost, Path: "/{index:[^_][^/]*}/_refresh", Handler: (*indexHandler).RefreshIndex},
		{Method: http.MethodPost, Path: "/{index:[^_][^/]*}/_flush", Handler: (*indexHandler).FlushIndex},
		{Method: http.MethodPost, Path: "/{index:[^_][^/]*}/_forcemerge", Handler: (*indexHandler).ForceMerge},
	}
	// 应用认证中间件保护
	routes = s.applyAuthMiddleware(routes, authMiddleware)
	router.AddRoutes(routes)
}

// registerDocumentRoutes 注册ES文档相关路由
func (s *ESServer) registerDocumentRoutes(router *server.Router, documentHandler *handler.DocumentHandler, authMiddleware func(http.Handler) http.Handler) {
	routes := []server.Route{
		{Method: http.MethodPost, Path: "/_bulk", Handler: (*documentHandler).Bulk},
		{Method: http.MethodPost, Path: "/_msearch", Handler: (*documentHandler).MultiSearch},
		{Method: http.MethodGet, Path: "/_mget", Handler: (*documentHandler).MultiGet},
		{Method: http.MethodPost, Path: "/_mget", Handler: (*documentHandler).MultiGet},

		// 索引相关路由
		{Method: http.MethodPost, Path: "/{index:[^_][^/]*}/_doc", Handler: (*documentHandler).CreateDocument},
		{Method: http.MethodPut, Path: "/{index:[^_][^/]*}/_doc/{id}", Handler: (*documentHandler).IndexDocument},
		{Method: http.MethodGet, Path: "/{index:[^_][^/]*}/_doc/{id}", Handler: (*documentHandler).GetDocument},
		{Method: http.MethodDelete, Path: "/{index:[^_][^/]*}/_doc/{id}", Handler: (*documentHandler).DeleteDocument},
		{Method: http.MethodHead, Path: "/{index:[^_][^/]*}/_doc/{id}", Handler: (*documentHandler).HeadDocument},
		{Method: http.MethodPost, Path: "/{index:[^_][^/]*}/_update/{id}", Handler: (*documentHandler).UpdateDocument},
		{Method: http.MethodGet, Path: "/{index:[^_][^/]*}/_count", Handler: (*documentHandler).CountDocuments},
		{Method: http.MethodPost, Path: "/{index:[^_][^/]*}/_count", Handler: (*documentHandler).CountDocuments},
		{Method: http.MethodGet, Path: "/{index:[^_][^/]*}/_search", Handler: (*documentHandler).Search},
		{Method: http.MethodPost, Path: "/{index:[^_][^/]*}/_search", Handler: (*documentHandler).Search},
		{Method: http.MethodPost, Path: "/{index:[^_][^/]*}/_bulk", Handler: (*documentHandler).Bulk},
		{Method: http.MethodPost, Path: "/{index:[^_][^/]*}/_delete_by_query", Handler: (*documentHandler).DeleteByQuery},
		// Scroll API
		{Method: http.MethodGet, Path: "/_search/scroll", Handler: (*documentHandler).Scroll},
		{Method: http.MethodPost, Path: "/_search/scroll", Handler: (*documentHandler).Scroll},
		{Method: http.MethodDelete, Path: "/_search/scroll", Handler: (*documentHandler).ClearScroll},
		{Method: http.MethodGet, Path: "/{index:[^_][^/]*}/_mget", Handler: (*documentHandler).MultiGet},
		{Method: http.MethodPost, Path: "/{index:[^_][^/]*}/_mget", Handler: (*documentHandler).MultiGet},
		// Tasks API (P1-3: DeleteByQuery异步任务管理)
		{Method: http.MethodGet, Path: "/_tasks/{task_id}", Handler: (*documentHandler).GetTask},
		{Method: http.MethodPost, Path: "/_tasks/{task_id}/_cancel", Handler: (*documentHandler).CancelTask},
	}
	// 应用认证中间件保护
	routes = s.applyAuthMiddleware(routes, authMiddleware)
	router.AddRoutes(routes)
}

// registerStatsRoutes 注册ES统计信息相关路由
func (s *ESServer) registerStatsRoutes(router *server.Router, statsHandler *handler.StatsHandler, authMiddleware func(http.Handler) http.Handler) {
	routes := []server.Route{
		{Method: http.MethodGet, Path: "/_stats", Handler: (*statsHandler).GetStats},
		{Method: http.MethodGet, Path: "/_info", Handler: (*statsHandler).GetInfo},
		{Method: http.MethodGet, Path: "/{index:[^_][^/]*}/_stats", Handler: (*statsHandler).GetIndexStats},
	}
	// 应用认证中间件保护
	routes = s.applyAuthMiddleware(routes, authMiddleware)
	router.AddRoutes(routes)
}

// Start 启动ES服务器
func (s *ESServer) Start() error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("ES server already started")
	}
	s.started = true
	s.mu.Unlock()

	return s.httpServer.Start()
}

// Stop 停止ES服务器
func (s *ESServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}

	// 使用带超时的context，避免无限等待
	shutdownTimeout := s.config.ServerConfig.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = 30 * time.Second // 默认30秒超时
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := s.httpServer.Stop(ctx); err != nil {
		log.Printf("ERROR: Failed to stop ES HTTP server: %v", err)
		return err
	}

	// 关闭所有索引
	if err := s.indexMgr.CloseAll(); err != nil {
		log.Printf("WARN: Failed to close all indices: %v", err)
	}

	s.started = false
	return nil
}

// Name 返回协议名称
func (s *ESServer) Name() string {
	return "es"
}

// Address 返回监听地址
func (s *ESServer) Address() string {
	return s.config.ServerConfig.Address()
}

// IsRunning 返回服务器是否正在运行
func (s *ESServer) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.started && s.httpServer.IsRunning()
}

// applyAuthMiddleware 应用认证中间件到路由（跳过公开路由）
func (s *ESServer) applyAuthMiddleware(routes []server.Route, authMiddleware func(http.Handler) http.Handler) []server.Route {
	if authMiddleware == nil {
		return routes
	}

	// 不需要认证的公开路径
	publicPaths := map[string]bool{
		"/":                true,
		"/_ping":           true,
		"/_cluster/health": true,
		"/_search":         true,
	}

	protectedRoutes := make([]server.Route, 0, len(routes))
	for _, route := range routes {
		// 路过公开路径
		if isPublic, ok := publicPaths[route.Path]; ok && isPublic {
			protectedRoutes = append(protectedRoutes, route)
		} else {
			// 应用认证中间件
			// 将 http.Handler 转换为 http.HandlerFunc
			wrappedHandler := authMiddleware(route.Handler)
			protectedRoutes = append(protectedRoutes, server.Route{
				Method: route.Method,
				Path:   route.Path,
				Handler: func(w http.ResponseWriter, r *http.Request) {
					wrappedHandler.ServeHTTP(w, r)
				},
			})
		}
	}

	return protectedRoutes
}

// 确保ESServer实现了ProtocolServer接口
var _ protocols.ProtocolServer = (*ESServer)(nil)
