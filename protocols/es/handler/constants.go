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

// Elasticsearch兼容性配置常量
const (
	// ESVersionNumber Elasticsearch版本号（用于兼容性）
	ESVersionNumber = "7.10.2"
	// ESBuildHash Elasticsearch构建哈希
	ESBuildHash = "747e1cc71def077253878a59143c1f785afa92b9"
	// ESBuildDate Elasticsearch构建日期
	ESBuildDate = "2021-01-13T00:42:12.435326Z"
	// ESLuceneVersion Lucene版本
	ESLuceneVersion = "8.7.0"
	// ESMinimumWireCompatibilityVersion 最小线兼容版本
	ESMinimumWireCompatibilityVersion = "6.8.0"
	// ESMinimumIndexCompatibilityVersion 最小索引兼容版本
	ESMinimumIndexCompatibilityVersion = "6.0.0"

	// ClusterName 集群名称
	ClusterName = "tigerdb-cluster"
	// ClusterUUID 集群UUID
	ClusterUUID = "tigerdb-cluster-uuid"
	// NodeName 节点名称
	NodeName = "tigerdb-node-1"
	// NodeTransportAddress 节点传输地址
	NodeTransportAddress = "127.0.0.1:9300"

	// ClusterStatusGreen 集群状态：绿色（健康）
	ClusterStatusGreen = "green"
	// ClusterStatusYellow 集群状态：黄色（警告）
	ClusterStatusYellow = "yellow"
	// ClusterStatusRed 集群状态：红色（错误）
	ClusterStatusRed = "red"

	// ActiveShardsPercent 活动分片百分比（单节点模式）
	ActiveShardsPercent = 100.0
)
