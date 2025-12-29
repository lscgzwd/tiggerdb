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

package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/lscgzwd/tiggerdb/directory"
	"github.com/lscgzwd/tiggerdb/logger"
	"github.com/lscgzwd/tiggerdb/metadata"
	"github.com/lscgzwd/tiggerdb/protocols/es"
)

const (
	// Version TigerDB版本
	Version = "1.0.0"
	// Name 应用名称
	Name = "TigerDB"
)

func main() {
	// 解析命令行参数
	globalConfig, err := ParseFlags()
	if err != nil {
		log.Fatalf("Failed to parse flags: %v", err)
	}

	// 初始化日志系统
	if err := initLogger(globalConfig); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	logger.Info("Starting %s v%s", Name, Version)
	logger.Debug("Configuration loaded: datadir=%s", globalConfig.GetDataDir())

	// 1. 创建核心组件（所有协议共享）
	// 使用统一配置系统获取数据目录（优先级：命令行 > 环境变量 > 配置文件 > 默认值）
	dataDir := globalConfig.GetDataDir()

	// 创建目录管理器
	dirConfig := directory.DefaultDirectoryConfig(dataDir)
	dirMgr, err := directory.NewDirectoryManager(dirConfig)
	if err != nil {
		log.Fatalf("Failed to create directory manager: %v", err)
	}
	defer dirMgr.Cleanup()

	// 创建元数据存储
	metaConfig := &metadata.MetadataStoreConfig{
		StorageType:      "file",
		FilePath:         filepath.Join(dataDir, "metadata"),
		EnableCache:      true,
		EnableVersioning: true,
	}
	metaStore, err := metadata.NewMetadataStore(metaConfig)
	if err != nil {
		log.Fatalf("Failed to create metadata store: %v", err)
	}
	defer metaStore.Close()

	// 2. 创建ES协议服务器（如果启用）
	if globalConfig.ES == nil || !globalConfig.ES.Enabled {
		log.Fatalf("ES protocol is not enabled or not configured")
	}

	esServer, err := es.NewServer(dirMgr, metaStore, globalConfig.ES)
	if err != nil {
		log.Fatalf("Failed to create ES server: %v", err)
	}

	// 3. 显示启动信息
	fmt.Printf("%s %s starting\n", Name, Version)
	fmt.Printf("Data Directory: %s\n", dataDir)
	if globalConfig.ES.Enabled {
		fmt.Printf("ES Protocol: %s\n", esServer.Address())
		fmt.Printf("Health check: http://%s/_health\n", esServer.Address())
		if globalConfig.ES.ServerConfig != nil && globalConfig.ES.ServerConfig.EnableMetrics {
			fmt.Printf("Metrics: http://%s/_metrics\n", esServer.Address())
		}
	}

	// 4. 启动ES服务器（支持优雅关闭）
	if err := startESServerWithGracefulShutdown(esServer); err != nil {
		log.Fatalf("ES server failed: %v", err)
	}
}

// startESServerWithGracefulShutdown 启动ES服务器并支持优雅关闭
func startESServerWithGracefulShutdown(esServer *es.ESServer) error {
	// 启动错误通道
	errChan := make(chan error, 1)

	// 在goroutine中启动服务器
	go func() {
		if err := esServer.Start(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	// 等待服务器启动完成（检查服务器是否正在运行）
	// 最多等待5秒，避免无限等待
	startTimeout := time.NewTimer(5 * time.Second)
	defer startTimeout.Stop()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-startTimeout.C:
			// 超时，但继续运行（可能服务器正在启动中）
			log.Printf("WARN: Server startup check timeout, continuing...")
			goto waitForSignal
		case err := <-errChan:
			// 启动失败
			return fmt.Errorf("server startup failed: %w", err)
		case <-ticker.C:
			// 检查服务器是否正在运行
			if esServer.IsRunning() {
				goto waitForSignal
			}
		}
	}

waitForSignal:
	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down ES server...")
	if err := esServer.Stop(); err != nil {
		log.Printf("ES server forced to shutdown: %v", err)
		return err
	}

	log.Println("ES server exited")
	return nil
}

// ShowVersion 显示版本信息
func ShowVersion() {
	fmt.Printf("%s version %s\n", Name, Version)
	os.Exit(0)
}

// ShowUsage 显示使用帮助
func ShowUsage() {
	fmt.Printf(`%s - Elasticsearch compatible database

Usage:
  tigerdb [flags]

Flags:
  -h, --help                    Show help message
  -v, --version                 Show version information
  -c, --config string           Configuration file path (default "config.yaml")
      --data-dir string         Data directory path (all protocols share)
      --es-host string          ES protocol server host
      --es-port int             ES protocol server port
      --es-enabled              Enable ES protocol (default true)
      --es-log-level string     ES protocol log level

Examples:
  tigerdb                                    # Start with default configuration
  tigerdb -c myconfig.yaml                   # Start with custom configuration
  tigerdb --data-dir /var/lib/tigerdb        # Specify data directory
  tigerdb --es-host 127.0.0.1 --es-port 8080 # Start ES on specific host and port
  tigerdb --es-log-level debug               # Start with debug logging

Configuration file example (config.yaml):
  data_dir: "./data"
  es:
    enabled: true
    server_config:
      host: "0.0.0.0"
      port: 9200
      log_level: "info"
      enable_cors: true
      enable_metrics: true
  redis:
    enabled: false
    host: "0.0.0.0"
    port: 6379
  mysql:
    enabled: false
    host: "0.0.0.0"
    port: 3306

Configuration Priority:
  1. Command line arguments (highest priority)
  2. Environment variables
  3. Configuration file
  4. Default values (lowest priority)

For more information, visit: https://github.com/lscgzwd/tiggerdb
`, Name)
	os.Exit(0)
}
