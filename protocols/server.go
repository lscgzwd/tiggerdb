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

package protocols

// ProtocolServer 协议服务器接口
// 所有协议服务器（ES、Redis、MySQL）都需要实现此接口
type ProtocolServer interface {
	// Start 启动服务器
	Start() error

	// Stop 停止服务器
	Stop() error

	// Name 返回协议名称（如 "es", "redis", "mysql"）
	Name() string

	// Address 返回监听地址（如 "0.0.0.0:9200"）
	Address() string

	// IsRunning 返回服务器是否正在运行
	IsRunning() bool
}
