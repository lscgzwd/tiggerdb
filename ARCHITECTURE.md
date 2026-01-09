# TigerDB 架构文档

## 概述

TigerDB 是一个兼容 Elasticsearch API 的全文搜索引擎，基于 Bleve 构建。支持多协议访问（ES、Redis、MySQL、PostgreSQL），提供统一的数据存储和查询能力。

---

## 一、系统架构

### 1.1 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                     TigerDB 主服务                        │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  ES 协议层   │  │ Redis 协议层 │  │ MySQL 协议层 │  │
│  │   (19200)    │  │   (6379)     │  │   (3306)     │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
│         │                  │                  │          │
│         └──────────────────┼──────────────────┘          │
│                            │                             │
│  ┌─────────────────────────▼──────────────────────────┐ │
│  │              统一查询层                             │ │
│  │  - 查询解析  - 查询优化  - 查询执行                │ │
│  └─────────────────────────┬──────────────────────────┘ │
│                            │                             │
│  ┌─────────────────────────▼──────────────────────────┐ │
│  │              Bleve 索引引擎                         │ │
│  │  - 倒排索引  - 文档存储  - 字段映射                │ │
│  └─────────────────────────┬──────────────────────────┘ │
│                            │                             │
│  ┌─────────────────────────▼──────────────────────────┐ │
│  │              持久化层                               │ │
│  │  - 元数据存储  - 索引文件  - 日志文件              │ │
│  └──────────────────────────────────────────────────────┘ │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

### 1.2 核心模块

| 模块         | 目录                  | 说明                                    |
| ------------ | --------------------- | --------------------------------------- |
| **协议层**   | `protocols/`          | 多协议支持（ES/Redis/MySQL/PostgreSQL） |
| **查询引擎** | `search/`             | 查询解析、优化、执行                    |
| **索引引擎** | `index/`, `mapping/`  | 索引管理、字段映射                      |
| **存储层**   | `store/`, `metadata/` | 数据持久化、元数据管理                  |
| **配置系统** | `config/`             | 统一配置管理                            |
| **日志系统** | `logger/`             | 统一日志管理                            |

---

## 二、目录结构

```
tigerdb/
├── cmd/tigerdb/              # 主程序入口
│   ├── main.go               # 主函数
│   ├── flags.go              # 命令行参数
│   └── logger_init.go        # 日志初始化
├── config/                   # 配置系统
│   ├── global.go             # 全局配置
│   ├── README.md             # 配置文档
│   └── config.example.yaml   # 配置示例
├── logger/                   # 日志系统
│   └── logger.go             # 日志管理器
├── protocols/                # 协议层
│   └── es/                   # Elasticsearch 协议
│       ├── handler/          # HTTP 处理器
│       ├── search/dsl/       # 查询 DSL 解析
│       ├── index/            # 索引管理
│       └── http/server/      # HTTP 服务器
├── search/                   # 查询引擎
│   ├── query/                # 查询类型
│   └── searcher/             # 搜索器
├── mapping/                  # 字段映射
├── metadata/                 # 元数据管理
├── directory/                # 目录管理
├── store/                    # 存储引擎
├── docs/                     # 文档
└── README.md                 # 项目主文档
```

---

## 三、Elasticsearch 协议实现

### 3.1 支持的 API

#### 索引管理 API

- `PUT /{index}` - 创建索引
- `DELETE /{index}` - 删除索引（支持逗号分隔的多索引）
- `GET /{index}` - 获取索引信息
- `GET /{index}/_settings` - 获取索引设置
- `PUT /{index}/_settings` - 更新索引设置
- `GET /{index}/_mapping` - 获取索引映射
- `PUT /{index}/_mapping` - 更新索引映射

#### 文档 API

- `POST /{index}/_doc` - 创建文档
- `PUT /{index}/_doc/{id}` - 创建/更新文档
- `GET /{index}/_doc/{id}` - 获取文档
- `DELETE /{index}/_doc/{id}` - 删除文档
- `POST /{index}/_update/{id}` - 更新文档
- `POST /_bulk` - 批量操作
- `POST /{index}/_bulk` - 索引级批量操作

#### 搜索 API

- `GET /{index}/_search` - 搜索文档
- `POST /{index}/_search` - 搜索文档（POST）
- `POST /_msearch` - 多索引搜索
- `POST /{index}/_delete_by_query` - 按查询删除

#### 集群 API

- `GET /_cluster/health` - 集群健康状态
- `GET /_cluster/stats` - 集群统计信息
- `GET /_cat/indices` - 索引列表
- `GET /_cat/shards` - 分片列表

#### 其他 API

- `GET /_health` - 健康检查
- `GET /_metrics` - 监控指标

### 3.2 支持的查询类型

#### 全文搜索

- `match` - 全文匹配
- `match_all` - 匹配所有文档
- `match_none` - 不匹配任何文档
- `match_phrase` - 短语匹配
- `multi_match` - 多字段匹配
- `query_string` - 查询字符串

#### 词条查询

- `term` - 精确匹配（支持数字/字符串自动转换）
- `terms` - 多值匹配
- `exists` - 字段存在查询
- `ids` - ID 查询
- `prefix` - 前缀查询
- `wildcard` - 通配符查询
- `regexp` - 正则表达式查询
- `fuzzy` - 模糊查询

#### 复合查询

- `bool` - 布尔查询（must/should/must_not/filter）
- `dis_max` - 最佳匹配查询
- `boosting` - 提升查询

#### 范围查询

- `range` - 范围查询（支持数字、日期、字符串）

#### 地理查询

- `geo_bounding_box` - 地理边界框查询
- `geo_distance` - 地理距离查询
- `geo_shape` - 地理形状查询（部分支持）

#### 特殊查询

- `nested` - 嵌套查询
- `script` - 脚本查询（不支持，返回 match_all）
- `has_child`/`has_parent` - 父子查询（不支持，返回 match_all）

### 3.3 查询优化器

查询优化器采用保守优化策略，确保不产生负优化。

**优化策略**：

- Boolean 查询优化（重排序 must/should 子句）
- Conjunction 查询优化（按选择性排序）
- Disjunction 查询优化（should 子句排序）
- 选择性估算（基于查询类型）

详见：[查询优化器架构文档](docs/QUERY_OPTIMIZER_ARCHITECTURE.md)

---

## 四、核心功能

### 4.1 查询解析

**文件**：`protocols/es/search/dsl/parser.go`

**功能**：

- 完整的 ES Query DSL 解析
- 类型自动转换（数字 ↔ 字符串）
- 数组字段支持
- nested 字段支持
- `minimum_should_match` 支持（字符串、百分比、ES 默认行为）

### 4.2 索引映射

**文件**：`mapping/field.go`, `mapping/document.go`

**功能**：

- ES mapping 到 Bleve mapping 转换
- 动态映射支持
- nested/object 类型支持
- 日期格式自动转换
- 分词器配置

### 4.3 批量操作

**文件**：`protocols/es/handler/document_handler_bulk.go`

**功能**：

- 流式响应（Chunked Transfer Encoding）
- 批量索引优化（按索引分组）
- 高性能（100 项 < 1 秒）
- 大请求支持（500MB）

### 4.4 日志系统

**文件**：`logger/logger.go`

**功能**：

- 统一的日志接口
- 多种日志级别（debug/info/warn/error/silent）
- 灵活的输出目标（stdout/stderr/文件）
- 日志轮转（基于大小、时间、数量）
- 多种格式（text/json）
- 结构化日志（WithFields）

详见：[日志系统文档](docs/LOGGING.md)

---

## 五、配置系统

### 5.1 配置结构

```go
type GlobalConfig struct {
    DataDir    string          // 数据目录（所有协议共享）
    ES         *es.Config      // ES 协议配置
    Redis      *RedisConfig    // Redis 协议配置
    MySQL      *MySQLConfig    // MySQL 协议配置
    PostgreSQL *PostgreSQLConfig // PostgreSQL 协议配置
    Log        *LogConfig      // 日志配置
    Metrics    *MetricsConfig  // 监控配置
}
```

### 5.2 配置优先级

**命令行参数 > 环境变量 > 配置文件 > 默认值**

### 5.3 配置示例

详见：

- [配置系统文档](config/README.md)
- [配置示例文件](config/config.example.yaml)

---

## 六、性能特性

### 6.1 查询性能

| 操作     | 性能          | 说明               |
| -------- | ------------- | ------------------ |
| 简单查询 | < 10ms        | term, match_all 等 |
| 复合查询 | < 50ms        | bool, nested 等    |
| 聚合查询 | < 200ms       | 小数据集           |
| 批量索引 | > 1000 docs/s | 使用批量 API       |

### 6.2 内存使用

| 场景     | 内存占用   | 说明             |
| -------- | ---------- | ---------------- |
| 空载     | < 100MB    | 无索引           |
| 小索引   | < 500MB    | < 1 万文档       |
| 中等索引 | < 2GB      | < 100 万文档     |
| 大索引   | 视数据而定 | 建议监控内存使用 |

### 6.3 日志性能影响

| 日志级别 | 性能影响 |
| -------- | -------- |
| silent   | 0%       |
| error    | < 0.1%   |
| info     | < 1%     |
| debug    | 5-10%    |

---

## 七、关键技术决策

### 7.1 为什么选择 Bleve？

**优点**：

- 纯 Go 实现，无外部依赖
- 完整的全文搜索功能
- 支持多种分词器和分析器
- 灵活的字段映射
- 高性能

**限制**：

- 分布式支持有限
- 某些高级 ES 特性不支持

### 7.2 查询系统设计

**DisjunctionQuery 的 Min 值**：

- 所有 DisjunctionQuery 显式设置 Min=1
- 确保查询逻辑正确性

**类型转换**：

- keyword 字段支持数字/布尔数组索引
- term 查询同时支持数字和字符串类型匹配

### 7.3 日志系统设计

**统一的日志接口**：

- 实现统一的 logger 包
- 支持多种日志级别和输出目标
- 自动日志轮转

---

## 八、部署指南

### 8.1 快速开始

```bash
# 1. 克隆代码
git clone https://github.com/your/tigerdb.git
cd tigerdb

# 2. 构建
go build -o tigerdb ./cmd/tigerdb

# 3. 运行（默认配置）
./tigerdb

# 4. 使用自定义配置
./tigerdb --config config.yaml
```

### 8.2 生产环境部署

```yaml
# config.yaml
data_dir: "/var/lib/tigerdb"

es:
  enabled: true
  host: "0.0.0.0"
  port: 19200

logging:
  level: "info"
  output: "/var/log/tigerdb/tigerdb.log"
  format: "json"
  max_size: 100
  max_backups: 7
  max_age: 30
  compress: true

metrics:
  enabled: true
  path: "/metrics"
```

### 8.3 监控和运维

**健康检查**：

```bash
curl http://localhost:19200/_health
```

**查看日志**：

```bash
tail -f /var/log/tigerdb/tigerdb.log
```

**监控指标**：

```bash
curl http://localhost:19200/_metrics
```

---

## 九、开发指南

### 9.1 添加新的查询类型

1. 在 `protocols/es/search/dsl/parser.go` 添加 `parse*` 函数
2. 在 `ParseQuery` 的 switch 中添加路由
3. 实现查询逻辑
4. 添加单元测试

示例：

```go
func (p *QueryParser) parseMyQuery(body interface{}) (query.Query, error) {
    // 解析逻辑
    return query.NewMyQuery(...), nil
}
```

### 9.2 添加新的字段类型

1. 在 `protocols/es/handler/index_handler.go` 的 `convertESFieldToBleve` 添加 case
2. 在 `mapping/field.go` 添加 `process*` 函数
3. 测试索引和查询

### 9.3 添加新的协议

1. 在 `protocols/` 创建新目录（如 `protocols/redis/`）
2. 实现协议解析器和处理器
3. 在 `config/global.go` 添加配置
4. 在 `cmd/tigerdb/main.go` 添加启动逻辑

---

## 十、相关文档

### 核心文档

- [README.md](README.md) - 项目介绍
- [ARCHITECTURE.md](ARCHITECTURE.md) - 本文档

### 功能文档

- [查询优化器架构](docs/QUERY_OPTIMIZER_ARCHITECTURE.md)
- [日志系统文档](docs/LOGGING.md)
- [配置系统文档](config/README.md)

### 配置文件

- [配置示例](config/config.example.yaml)

---

## 十一、FAQ

### Q1: 与 Elasticsearch 的兼容性如何？

**A**:

- 核心查询 API 完全兼容
- 大部分 Query DSL 支持
- 高级特性部分支持（script、聚合等）
- 不支持分布式特性

### Q2: 性能如何？

**A**:

- 单机性能优秀（基于 Bleve）
- 内存占用合理
- 适合中小规模数据（< 千万级文档）

### Q3: 生产环境可用吗？

**A**:

- 核心功能稳定
- 日志系统完善
- 建议先在非关键业务试用

### Q4: 如何迁移自 Elasticsearch？

**A**:

1. 修改客户端连接地址（指向 TigerDB）
2. 测试查询兼容性
3. 根据需要调整映射
4. 监控性能和日志

---

**维护者**：TigerDB Team  
**许可证**：Apache License 2.0
