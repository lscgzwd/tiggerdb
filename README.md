# TigerDB

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.20+-blue.svg)](https://golang.org/)

TigerDB 是一个兼容 Elasticsearch API 的全文搜索引擎，基于 Bleve 构建。支持多协议访问，提供高性能的全文搜索和数据分析能力。

## ✨ 特性

- 🔍 **Elasticsearch API 兼容**：支持大部分 ES 7.x Query DSL
- 🚀 **高性能**：基于 Bleve，纯 Go 实现，无外部依赖
- 🔌 **多协议支持**：ES、Redis、MySQL、PostgreSQL（部分预留）
- 📊 **查询优化**：保守的查询优化器，确保不产生负优化
- 📝 **完善的日志系统**：多级别、多输出、自动轮转
- ⚙️ **灵活配置**：配置文件、环境变量、命令行参数
- 🛡️ **生产就绪**：完善的错误处理、监控指标、健康检查

## 🚀 快速开始

### 安装

```bash
# 克隆代码
git clone https://github.com/lscgzwd/tigerdb.git
cd tigerdb

# 构建
go build -o tigerdb ./cmd/tigerdb

# 运行
./tigerdb
```

### 基本使用

```bash
# 创建索引
curl -X PUT "localhost:19200/my_index" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "title": { "type": "text" },
      "age": { "type": "integer" }
    }
  }
}'

# 索引文档
curl -X POST "localhost:19200/my_index/_doc/1" -H 'Content-Type: application/json' -d'
{
  "title": "Hello TigerDB",
  "age": 25
}'

# 搜索文档
curl -X GET "localhost:19200/my_index/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "match": {
      "title": "TigerDB"
    }
  }
}'
```

## 📖 文档

### 核心文档

- [架构文档](ARCHITECTURE.md) - 系统架构、模块设计、技术决策
- [配置系统](config/README.md) - 配置说明、优先级、示例
- [日志系统](docs/LOGGING.md) - 日志级别、输出配置、使用指南

### 功能文档

- [查询优化器](docs/QUERY_OPTIMIZER_ARCHITECTURE.md) - 优化策略、性能保证
- [嵌套文档](docs/nested_documents_examples.md) - 嵌套查询示例
- [地理查询](docs/geo.md) - 地理位置查询
- [向量搜索](docs/vectors.md) - 向量相似度搜索
- [同义词](docs/synonyms.md) - 同义词配置
- [评分融合](docs/score_fusion.md) - 多查询评分融合

### 配置文件

- [配置示例](config/config.example.yaml) - 完整的配置示例

## 🔧 配置

### 配置文件

创建 `config.yaml`：

```yaml
# 数据目录
data_dir: "./data"

# Elasticsearch 协议
es:
  enabled: true
  host: "0.0.0.0"
  port: 19200

# 日志配置
logging:
  level: "info"
  output: "stdout"
  format: "text"
```

### 环境变量

```bash
export TIGERDB_DATA_DIR=./data
export LOG_LEVEL=debug
export LOG_OUTPUT=./logs/tigerdb.log
```

### 命令行参数

```bash
./tigerdb --config config.yaml --data-dir ./data --es-port 19200
```

**配置优先级**：命令行 > 环境变量 > 配置文件 > 默认值

## 📊 性能

| 操作     | 性能              |
| -------- | ----------------- |
| 简单查询 | < 10ms            |
| 复合查询 | < 50ms            |
| 批量索引 | > 1000 docs/s     |
| 内存占用 | < 500MB（小索引） |

## 🔍 支持的查询类型

### 全文搜索

- `match`, `match_all`, `match_phrase`
- `multi_match`, `query_string`

### 词条查询

- `term`, `terms`, `exists`, `ids`
- `prefix`, `wildcard`, `regexp`, `fuzzy`

### 复合查询

- `bool` (must/should/must_not/filter)
- `dis_max`, `boosting`

### 范围查询

- `range` (支持数字、日期、字符串)

### 地理查询

- `geo_bounding_box`, `geo_distance`

### 特殊查询

- `nested` (嵌套文档查询)

## 🛠️ 开发

### 构建

```bash
go build ./cmd/tigerdb
```

### 运行测试

```bash
go test ./...
```

### 开发模式

```bash
LOG_LEVEL=debug ./tigerdb
```

## 📦 依赖

- Go 1.20+
- github.com/blevesearch/bleve_index_api
- github.com/gorilla/mux
- gopkg.in/natefinch/lumberjack.v2

## 🤝 贡献

欢迎贡献代码！请参阅 [CONTRIBUTING.md](CONTRIBUTING.md)

## 📄 许可证

Apache License 2.0 - 详见 [LICENSE](LICENSE)

## 🔗 相关链接

- [Bleve](https://github.com/blevesearch/bleve) - 底层搜索引擎
- [Elasticsearch](https://www.elastic.co/) - API 兼容目标

## 📮 联系方式

- Issue: https://github.com/lscgzwd/tigerdb/issues
- Email: lscgzwd@gmail.com

---

**TigerDB** - 高性能、易部署的全文搜索引擎
