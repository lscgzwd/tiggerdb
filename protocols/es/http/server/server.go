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

// Package server 提供ES协议专用的HTTP服务器基础设施
// 注意：此包仅用于ES协议，Redis和MySQL协议使用TCP，不需要HTTP基础设施
package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/lscgzwd/tiggerdb/protocols/es/http/common"
)

// Server HTTP服务器
type Server struct {
	config     *ServerConfig
	router     *Router
	httpServer *http.Server
	middleware Middleware
	started    bool
	mu         sync.RWMutex
	startTime  time.Time
}

// NewServer 创建新的HTTP服务器
func NewServer(config *ServerConfig) (*Server, error) {
	if config == nil {
		config = DefaultServerConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid server config: %w", err)
	}

	router := NewRouter()
	middleware := DefaultMiddlewareStack(config)

	server := &Server{
		config:     config,
		router:     router,
		middleware: middleware,
		started:    false,
	}

	return server, nil
}

// GetRouter 获取路由管理器
func (s *Server) GetRouter() *Router {
	return s.router
}

// AddRoute 添加路由
func (s *Server) AddRoute(method, path string, handler http.HandlerFunc, middlewares ...Middleware) {
	s.router.AddRoute(method, path, handler, middlewares...)
}

// AddRoutes 批量添加路由
func (s *Server) AddRoutes(routes []Route) {
	s.router.AddRoutes(routes)
}

// Start 启动服务器
func (s *Server) Start() error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("server already started")
	}
	s.started = true
	s.startTime = time.Now()
	s.mu.Unlock()

	// 构建路由器
	muxRouter := s.router.Build()

	// 在构建的路由器上直接添加全局路由，确保它们有最高优先级
	// 注意：这里假设 ESServer 有访问处理器的方法
	// 由于这是 HTTP 服务器层，我们需要通过其他方式访问处理器
	// 暂时注释掉，稍后在 ES 层处理

	// 创建HTTP服务器
	// 注意：Go 的 http.Server 没有 MaxRequestBodySize 字段
	// 请求体大小限制通过中间件中的 http.MaxBytesReader 实现
	s.httpServer = &http.Server{
		Addr:              s.config.Address(),
		Handler:           s.middleware(muxRouter.ServeHTTP),
		ReadTimeout:       s.config.ReadTimeout,
		WriteTimeout:      s.config.WriteTimeout,
		IdleTimeout:       s.config.IdleTimeout,
		ReadHeaderTimeout: s.config.ReadHeaderTimeout,
		MaxHeaderBytes:    s.config.MaxHeaderBytes,
	}
	// 禁用 Keep-Alive，每次请求后关闭连接，避免空闲连接被意外关闭导致客户端报错
	s.httpServer.SetKeepAlivesEnabled(false)

	// 配置TLS
	if s.config.TLSEnable {
		if s.config.TLSCertFile == "" || s.config.TLSKeyFile == "" {
			return fmt.Errorf("TLS cert file and key file must be specified when TLS is enabled")
		}

		cert, err := tls.LoadX509KeyPair(s.config.TLSCertFile, s.config.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("failed to load TLS cert: %w", err)
		}

		s.httpServer.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	}

	log.Printf("Starting TigerDB HTTP server on %s", s.config.Address())

	// 启动服务器
	if s.config.TLSEnable {
		return s.httpServer.ListenAndServeTLS("", "")
	}

	return s.httpServer.ListenAndServe()
}

// StartWithGracefulShutdown 启动服务器并支持优雅关闭
func (s *Server) StartWithGracefulShutdown() error {
	// 启动服务器
	go func() {
		if err := s.Start(); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v", err)
		}
	}()

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// 优雅关闭
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
	defer cancel()

	if err := s.Stop(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
		return err
	}

	log.Println("Server exited")
	return nil
}

// Stop 停止服务器
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started || s.httpServer == nil {
		return nil
	}

	return s.httpServer.Shutdown(ctx)
}

// IsRunning 检查服务器是否正在运行
func (s *Server) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.started
}

// AddHealthCheck 添加健康检查端点
func (s *Server) AddHealthCheck() {
	s.AddRoute(http.MethodGet, s.config.HealthPath, s.healthCheckHandler)
}

// AddMetrics 添加指标收集端点
func (s *Server) AddMetrics() {
	if s.config.EnableMetrics {
		s.AddRoute(http.MethodGet, s.config.MetricsPath, s.metricsHandler)
	}
}

// healthCheckHandler 健康检查处理器
func (s *Server) healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	response := common.SuccessResponse().
		WithData(map[string]interface{}{
			"status":    "ok",
			"timestamp": time.Now().Unix(),
			"version":   "1.0.0",
		})

	common.HandleSuccess(w, response, http.StatusOK)
}

// metricsHandler 指标收集处理器
func (s *Server) metricsHandler(w http.ResponseWriter, r *http.Request) {
	// 这里应该收集实际的指标数据
	// 暂时返回模拟数据
	uptime := 0.0
	if !s.startTime.IsZero() {
		uptime = time.Since(s.startTime).Seconds()
	}
	metrics := map[string]interface{}{
		"uptime":             uptime,
		"requests_total":     0, // 总请求数
		"active_connections": 0, // 活跃连接数
		"memory_usage":       0, // 内存使用
		"cpu_usage":          0, // CPU使用
	}

	response := common.SuccessResponse().WithData(metrics)
	common.HandleSuccess(w, response, http.StatusOK)
}

// AddDefaultRoutes 添加默认路由
func (s *Server) AddDefaultRoutes() {
	// 添加健康检查
	s.AddHealthCheck()

	// 添加指标收集
	s.AddMetrics()
}
