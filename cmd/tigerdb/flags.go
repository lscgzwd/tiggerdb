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
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lscgzwd/tiggerdb/config"
	"gopkg.in/yaml.v3"
)

// ParseFlags 解析命令行参数
// 配置优先级：命令行参数 > 环境变量 > 配置文件 > 默认值
func ParseFlags() (*config.GlobalConfig, error) {
	// 定义命令行参数
	var (
		configFile  = flag.String("config", "", "Configuration file path")
		c           = flag.String("c", "", "Configuration file path (short)")
		showHelp    = flag.Bool("help", false, "Show help message")
		h           = flag.Bool("h", false, "Show help message (short)")
		showVersion = flag.Bool("version", false, "Show version information")
		v           = flag.Bool("v", false, "Show version information (short)")

		// 核心参数
		dataDir = flag.String("data-dir", "", "Data directory path (all protocols share)")

		// ES 协议参数
		esHost     = flag.String("es-host", "", "ES protocol server host")
		esPort     = flag.Int("es-port", 0, "ES protocol server port")
		esEnabled  = flag.Bool("es-enabled", true, "Enable ES protocol")
		esLogLevel = flag.String("es-log-level", "", "ES protocol log level")
	)

	// 自定义Usage函数
	flag.Usage = ShowUsage

	// 解析参数
	flag.Parse()

	// 处理帮助和版本
	if *showHelp || *h {
		ShowUsage()
	}
	if *showVersion || *v {
		ShowVersion()
	}

	// 确定配置文件路径
	configPath := *configFile
	if *c != "" {
		configPath = *c
	}
	// 如果未指定配置文件，尝试自动检测
	if configPath == "" {
		configPath = "config.yaml"
	}

	// 1. 加载配置文件（如果存在）
	globalConfig, err := LoadGlobalConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// 2. 应用环境变量覆盖（优先级高于配置文件）
	globalConfig.ApplyEnvOverrides()

	// 3. 应用命令行参数覆盖（最高优先级）
	applyCommandLineOverrides(globalConfig, dataDir, esHost, esPort, esEnabled, esLogLevel)

	// 4. 验证配置
	if err := globalConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return globalConfig, nil
}

// applyCommandLineOverrides 应用命令行参数覆盖（最高优先级）
func applyCommandLineOverrides(globalConfig *config.GlobalConfig,
	dataDir *string, esHost *string, esPort *int, esEnabled *bool, esLogLevel *string) {
	// 核心配置
	if *dataDir != "" {
		globalConfig.DataDir = *dataDir
	}

	// ES 协议配置
	if globalConfig.ES != nil && globalConfig.ES.ServerConfig != nil {
		if *esHost != "" {
			globalConfig.ES.ServerConfig.Host = *esHost
		}
		if *esPort > 0 {
			globalConfig.ES.ServerConfig.Port = *esPort
		}
		if *esLogLevel != "" {
			if isValidLogLevel(*esLogLevel) {
				globalConfig.ES.ServerConfig.LogLevel = *esLogLevel
			}
		}
		globalConfig.ES.Enabled = *esEnabled
	}
}

// LoadGlobalConfig 加载全局配置文件
// 支持自动检测配置文件，如果configPath为空或文件不存在，会尝试从多个位置查找
func LoadGlobalConfig(configPath string) (*config.GlobalConfig, error) {
	// 从默认配置开始
	globalConfig := config.DefaultGlobalConfig()

	// 如果指定了配置文件路径，尝试加载
	if configPath != "" && configPath != "config.yaml" {
		_, err := os.Stat(configPath)
		if err == nil {
			return loadGlobalConfigFromFile(configPath, globalConfig)
		}
		// 如果指定的文件不存在，继续尝试自动检测
		if !os.IsNotExist(err) {
			// 其他错误（如权限问题）直接返回
			return nil, fmt.Errorf("failed to stat config file %s: %w", configPath, err)
		}
		// 文件不存在，继续执行自动检测逻辑
	}

	// 自动检测配置文件
	detectedPath := autoDetectConfig()
	if detectedPath != "" {
		return loadGlobalConfigFromFile(detectedPath, globalConfig)
	}

	// 如果指定了configPath但文件不存在，且没有自动检测到
	if configPath != "" && configPath != "config.yaml" {
		// 用户明确指定了配置文件但不存在，返回错误
		return nil, fmt.Errorf("config file not found: %s", configPath)
	}

	// 没有找到配置文件，使用默认配置
	return globalConfig, nil
}

// loadGlobalConfigFromFile 从文件加载全局配置
func loadGlobalConfigFromFile(configPath string, baseConfig *config.GlobalConfig) (*config.GlobalConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}

	// 解析YAML
	if err := yaml.Unmarshal(data, baseConfig); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
	}

	return baseConfig, nil
}

// autoDetectConfig 自动检测配置文件
func autoDetectConfig() string {
	homeDir, _ := os.UserHomeDir()
	paths := []string{
		"config.yaml",
		"config.yml",
		"tigerdb.yaml",
		"tigerdb.yml",
		filepath.Join(".", "config", "tigerdb.yaml"),
		filepath.Join(homeDir, ".tigerdb", "config.yaml"),
		filepath.Join("/etc", "tigerdb", "config.yaml"),
	}

	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return ""
}

// isValidLogLevel 验证日志级别
func isValidLogLevel(level string) bool {
	validLevels := []string{"debug", "info", "warn", "error", "fatal"}
	for _, validLevel := range validLevels {
		if level == validLevel {
			return true
		}
	}
	return false
}
