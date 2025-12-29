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
	"github.com/lscgzwd/tiggerdb/protocols/es/http/server"
)

// Config ES协议服务器配置
type Config struct {
	// 是否启用ES协议
	Enabled bool `json:"enabled" yaml:"enabled"`

	// HTTP服务器配置
	ServerConfig *server.ServerConfig `json:"server_config" yaml:"server_config"`
}

// DefaultConfig 返回默认ES配置
func DefaultConfig() *Config {
	return &Config{
		Enabled:      true,
		ServerConfig: server.DefaultServerConfig(),
	}
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.ServerConfig == nil {
		c.ServerConfig = server.DefaultServerConfig()
	}
	return c.ServerConfig.Validate()
}
