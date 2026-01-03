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

// Package testutil 提供ES协议测试工具
package testutil

import (
	"os"
	"time"

	"github.com/lscgzwd/tiggerdb/directory"
	"github.com/lscgzwd/tiggerdb/metadata"
	"github.com/lscgzwd/tiggerdb/protocols/es"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/server"
)

// TestServer 测试服务器
type TestServer struct {
	Server  *es.ESServer
	TempDir string
	BaseURL string
	Cleanup func()
}

// NewTestServer 创建测试服务器
func NewTestServer() (*TestServer, error) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "tigerdb_test_*")
	if err != nil {
		return nil, err
	}

	// 创建目录管理器
	dirConfig := directory.DefaultDirectoryConfig(tempDir)
	dirMgr, err := directory.NewDirectoryManager(dirConfig)
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, err
	}

	// 创建元数据存储
	metaConfig := &metadata.MetadataStoreConfig{
		StorageType:      "file",
		FilePath:         tempDir,
		EnableCache:      true,
		EnableVersioning: true,
	}
	metaStore, err := metadata.NewFileMetadataStore(metaConfig)
	if err != nil {
		dirMgr.Cleanup()
		os.RemoveAll(tempDir)
		return nil, err
	}

	// 创建ES服务器配置
	config := es.DefaultConfig()
	config.ServerConfig = server.DefaultServerConfig()
	config.ServerConfig.Port = 0           // 使用随机端口
	config.ServerConfig.LogLevel = "error" // 测试时减少日志输出

	// 创建ES服务器
	esSrv, err := es.NewServer(dirMgr, metaStore, config)
	if err != nil {
		metaStore.Close()
		dirMgr.Cleanup()
		os.RemoveAll(tempDir)
		return nil, err
	}

	// 启动服务器（在后台）
	go func() {
		if err := esSrv.Start(); err != nil {
			// 测试服务器启动失败，忽略
		}
	}()

	// 等待服务器启动
	time.Sleep(100 * time.Millisecond)

	cleanup := func() {
		esSrv.Stop()
		metaStore.Close()
		dirMgr.Cleanup()
		os.RemoveAll(tempDir)
	}

	// 获取实际监听地址
	addr := esSrv.Address()
	baseURL := "http://" + addr

	return &TestServer{
		Server:  esSrv,
		TempDir: tempDir,
		BaseURL: baseURL,
		Cleanup: cleanup,
	}, nil
}

// Close 关闭测试服务器
func (ts *TestServer) Close() {
	if ts.Cleanup != nil {
		ts.Cleanup()
	}
}
