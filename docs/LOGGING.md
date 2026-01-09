# TigerDB 日志系统文档

## 概述

TigerDB 使用统一的日志系统，支持多种输出目标、日志级别和格式。日志系统基于 Go 标准库 `log` 包和 `lumberjack` 日志轮转库实现。

## 日志级别

TigerDB 支持以下日志级别（从低到高）：

| 级别     | 说明     | 用途                                     |
| -------- | -------- | ---------------------------------------- |
| `debug`  | 调试信息 | 详细的执行日志，包括查询解析、索引操作等 |
| `info`   | 一般信息 | API 请求/响应、服务启动/停止等           |
| `warn`   | 警告信息 | 非致命错误、降级操作等                   |
| `error`  | 错误信息 | 致命错误、操作失败等                     |
| `silent` | 静默模式 | 禁用所有日志输出                         |

**默认级别**：`info`

## 配置方式

### 1. 配置文件（推荐）

在 `config.yaml` 中配置：

```yaml
logging:
  # 日志级别：debug, info, warn, error, silent
  level: "info"

  # 输出目标：stdout, stderr, 或文件路径
  output: "stdout"
  # 示例：输出到文件
  # output: "./logs/tigerdb.log"

  # 日志格式：text 或 json
  format: "text"

  # 是否显示调用位置（文件:行号）
  enable_caller: false

  # 是否显示时间戳
  enable_timestamp: true

  # 文件轮转配置（仅当 output 为文件路径时生效）
  # 单个日志文件的最大大小（MB）
  max_size: 100

  # 保留的旧日志文件数量
  max_backups: 3

  # 保留旧日志文件的最大天数
  max_age: 7

  # 是否压缩旧日志文件
  compress: true
```

### 2. 环境变量

环境变量会覆盖配置文件中的设置：

```bash
# 日志级别
export LOG_LEVEL=debug

# 输出目标
export LOG_OUTPUT=./logs/tigerdb.log

# 日志格式
export LOG_FORMAT=json

# 是否显示调用位置
export LOG_ENABLE_CALLER=true
```

### 3. 命令行参数

```bash
# 通过配置文件指定
./tigerdb --config config.yaml

# 环境变量优先级最高
LOG_LEVEL=debug ./tigerdb
```

## 配置优先级

**命令行参数 > 环境变量 > 配置文件 > 默认值**

## 使用示例

### 示例 1：开发环境（详细日志）

```yaml
logging:
  level: "debug"
  output: "stdout"
  format: "text"
  enable_caller: true
  enable_timestamp: true
```

### 示例 2：生产环境（文件日志 + 轮转）

```yaml
logging:
  level: "info"
  output: "./logs/tigerdb.log"
  format: "json"
  enable_caller: false
  enable_timestamp: true
  max_size: 100
  max_backups: 7
  max_age: 30
  compress: true
```

### 示例 3：调试特定问题

```bash
# 临时启用 debug 级别
LOG_LEVEL=debug ./tigerdb

# 输出到文件以便分析
LOG_LEVEL=debug LOG_OUTPUT=./debug.log ./tigerdb
```

## 日志输出示例

### text 格式（默认）

```
[INFO] 2025/12/11 12:00:00.123456 Starting TigerDB v1.0.0
[DEBUG] 2025/12/11 12:00:00.234567 ParseQuery - Input query map:
{
  "term": {
    "network_type": 1
  }
}
[ERROR] 2025/12/11 12:00:01.345678 Failed to create index: invalid mapping
```

### json 格式

```json
{"level":"INFO","time":"2025-12-11T12:00:00.123456Z","msg":"Starting TigerDB v1.0.0"}
{"level":"DEBUG","time":"2025-12-11T12:00:00.234567Z","msg":"ParseQuery - Input query map:\n{\n  \"term\": {\n    \"network_type\": 1\n  }\n}"}
{"level":"ERROR","time":"2025-12-11T12:00:01.345678Z","msg":"Failed to create index: invalid mapping"}
```

## 日志级别说明

### DEBUG 级别

**用途**：开发和调试

**包含内容**：

- 查询解析详情（输入查询 JSON、解析后的 Bleve 查询）
- 索引操作详情（字段映射、文档结构）
- 元数据变更详情（mapping 更新、settings 变更）
- 搜索执行详情（查询参数、结果统计）

**示例**：

```
[DEBUG] ParseQuery - Input query map: {...}
[DEBUG] parseTerm [network_type] - Created DisjunctionQuery with NumericRangeQuery (1) and TermQuery ("1")
[DEBUG] executeSearchInternal [asset] - Search result: Total=311, Hits=30
```

### INFO 级别

**用途**：生产环境

**包含内容**：

- 服务启动/停止
- API 请求摘要（方法、路径、状态码、耗时）
- 索引创建/删除
- 重要操作成功消息

**示例**：

```
[INFO] Starting TigerDB v1.0.0
[INFO] ES server listening on 0.0.0.0:19200
[INFO] [POST] /_bulk [::1]:12345 200 45ms
```

### WARN 级别

**用途**：生产环境

**包含内容**：

- 非致命错误（如索引不存在但可继续）
- 降级操作（如使用默认值）
- 兼容性警告

**示例**：

```
[WARN] Index metadata not found for index [test], returning empty aliases
[WARN] Some indices failed to delete: [index1, index2]
```

### ERROR 级别

**用途**：所有环境

**包含内容**：

- 操作失败
- 系统错误
- 致命错误

**示例**：

```
[ERROR] Failed to create index [test]: invalid mapping
[ERROR] Failed to search index [asset]: query parse error
```

## 日志轮转

当 `output` 设置为文件路径时，日志会自动轮转：

- **按大小轮转**：单个文件达到 `max_size` MB 时创建新文件
- **保留数量**：最多保留 `max_backups` 个旧文件
- **保留时间**：旧文件超过 `max_age` 天后自动删除
- **压缩**：如果 `compress: true`，旧文件会被 gzip 压缩

**文件命名规则**：

```
tigerdb.log           # 当前日志
tigerdb-2025-12-11.log  # 轮转后的日志
tigerdb-2025-12-10.log.gz  # 压缩的旧日志
```

## 代码中使用日志

### 基本用法

```go
import "github.com/lscgzwd/tiggerdb/logger"

// Debug 级别
logger.Debug("ParseQuery - Input: %v", query)

// Info 级别
logger.Info("Index created successfully: %s", indexName)

// Warn 级别
logger.Warn("Index not found: %s", indexName)

// Error 级别
logger.Error("Failed to create index: %v", err)
```

### 条件日志（避免不必要的字符串格式化）

```go
if logger.IsDebugEnabled() {
    // 只在 debug 级别启用时才执行昂贵的操作
    queryJSON, _ := json.MarshalIndent(query, "", "  ")
    logger.Debug("Query JSON:\n%s", string(queryJSON))
}
```

### 结构化日志（带字段）

```go
logger.WithFields(map[string]interface{}{
    "index": "asset",
    "took": 123,
    "hits": 100,
}).Info("Search completed")

// 输出：[INFO] Search completed index=asset took=123 hits=100
```

## 迁移指南

### 从旧的环境变量日志开关迁移

**旧方式**（已废弃）：

```go
if os.Getenv("TIGERDB_DEBUG_QUERY") == "true" {
    log.Printf("DEBUG: Query: %v", query)
}
```

**新方式**：

```go
logger.Debug("Query: %v", query)
// 或
if logger.IsDebugEnabled() {
    logger.Debug("Query: %v", query)
}
```

**配置迁移**：

```bash
# 旧方式
export TIGERDB_DEBUG_QUERY=true
export TIGERDB_DEBUG_METADATA=true

# 新方式
export LOG_LEVEL=debug
# 或在 config.yaml 中设置 logging.level: debug
```

## 常见问题

### Q1: 如何只查看错误日志？

**A**: 设置日志级别为 `error`：

```yaml
logging:
  level: "error"
```

### Q2: 如何同时输出到控制台和文件？

**A**: 当前版本不支持多个输出目标。建议：

- 使用文件输出 + `tail -f` 查看实时日志
- 或使用系统日志工具（如 `tee`）：
  ```bash
  ./tigerdb 2>&1 | tee tigerdb.log
  ```

### Q3: 日志文件太大怎么办？

**A**: 调整轮转配置：

```yaml
logging:
  max_size: 50 # 减小单个文件大小
  max_backups: 5 # 增加保留文件数
  max_age: 3 # 减少保留天数
  compress: true # 启用压缩
```

### Q4: 如何查看历史日志？

**A**:

```bash
# 查看当前日志
cat logs/tigerdb.log

# 查看压缩的旧日志
zcat logs/tigerdb-2025-12-10.log.gz

# 搜索特定错误
grep "ERROR" logs/tigerdb*.log
```

### Q5: 性能影响？

**A**:

- `debug` 级别会有一定性能影响（约 5-10%）
- `info` 级别影响很小（< 1%）
- 使用 `logger.IsDebugEnabled()` 可避免不必要的字符串格式化
- 文件输出比控制台输出略快

## 最佳实践

1. **生产环境**：使用 `info` 或 `warn` 级别，输出到文件
2. **开发环境**：使用 `debug` 级别，输出到控制台
3. **调试问题**：临时启用 `debug` 级别，输出到文件
4. **昂贵操作**：使用 `logger.IsDebugEnabled()` 条件判断
5. **错误处理**：始终记录 `error` 级别日志
6. **结构化日志**：使用 `WithFields` 添加上下文信息

## 相关文件

- `logger/logger.go` - 日志系统实现
- `config/global.go` - 日志配置结构
- `cmd/tigerdb/logger_init.go` - 日志初始化逻辑
- `docs/LOGGING.md` - 本文档

## 更新历史

- 2025-12-11: 初始版本，统一日志系统
