# TigerDB 配置系统

## 概述

TigerDB 使用统一的配置系统，支持多协议（ES、Redis、MySQL、PostgreSQL 等），所有协议共享核心配置（如 `data_dir`）。

## 配置优先级

配置按以下优先级顺序应用（高优先级覆盖低优先级）：

1. **命令行参数**（最高优先级）
2. **环境变量**
3. **配置文件**
4. **默认值**（最低优先级）

## 配置文件格式

配置文件支持 YAML 格式，示例：

```yaml
# 核心配置（所有协议共享）
data_dir: "./data"

# ES 协议配置
es:
  enabled: true
  server_config:
    host: "0.0.0.0"
    port: 9200
    log_level: "info"
    enable_cors: true
    enable_metrics: true
    max_request_size: 524288000  # 500MB

# Redis 协议配置（预留）
redis:
  enabled: false
  host: "0.0.0.0"
  port: 6379

# MySQL 协议配置（预留）
mysql:
  enabled: false
  host: "0.0.0.0"
  port: 3306

# PostgreSQL 协议配置（预留）
postgresql:
  enabled: false
  host: "0.0.0.0"
  port: 5432

# 全局日志配置
log:
  level: "info"
  format: "json"
  file: ""
  max_size: 100
  max_files: 10

# 全局监控配置
metrics:
  enabled: true
  path: "/_metrics"
```

## 命令行参数

### 核心参数

- `--data-dir string`: 数据目录路径（所有协议共享）

### ES 协议参数

- `--es-host string`: ES 协议服务器地址
- `--es-port int`: ES 协议服务器端口
- `--es-enabled`: 是否启用 ES 协议（默认 true）
- `--es-log-level string`: ES 协议日志级别

### 通用参数

- `-c, --config string`: 配置文件路径
- `-h, --help`: 显示帮助信息
- `-v, --version`: 显示版本信息

## 环境变量

### 核心配置

- `TIGERDB_DATA_DIR`: 数据目录路径

### ES 协议配置

- `TIGERDB_ES_ENABLED`: 是否启用 ES 协议（true/false）
- `TIGERDB_ES_HOST`: ES 协议服务器地址
- `TIGERDB_ES_PORT`: ES 协议服务器端口

### Redis 协议配置（预留）

- `TIGERDB_REDIS_ENABLED`: 是否启用 Redis 协议
- `TIGERDB_REDIS_HOST`: Redis 协议服务器地址
- `TIGERDB_REDIS_PORT`: Redis 协议服务器端口

### MySQL 协议配置（预留）

- `TIGERDB_MYSQL_ENABLED`: 是否启用 MySQL 协议
- `TIGERDB_MYSQL_HOST`: MySQL 协议服务器地址
- `TIGERDB_MYSQL_PORT`: MySQL 协议服务器端口

## 使用示例

### 1. 使用默认配置

```bash
tigerdb
```

### 2. 通过命令行参数指定数据目录

```bash
tigerdb --data-dir /var/lib/tigerdb
```

### 3. 通过环境变量指定数据目录

```bash
export TIGERDB_DATA_DIR=/var/lib/tigerdb
tigerdb
```

### 4. 使用配置文件

```bash
tigerdb -c /etc/tigerdb/config.yaml
```

### 5. 命令行参数覆盖配置文件

```bash
# 配置文件中的 data_dir 为 ./data
# 命令行参数覆盖为 /var/lib/tigerdb
tigerdb -c config.yaml --data-dir /var/lib/tigerdb
```

## 配置文件自动检测

如果未指定配置文件路径，系统会自动从以下位置查找：

1. `./config.yaml`
2. `./config.yml`
3. `./tigerdb.yaml`
4. `./tigerdb.yml`
5. `./config/tigerdb.yaml`
6. `~/.tigerdb/config.yaml`
7. `/etc/tigerdb/config.yaml`

## 配置验证

配置系统会在启动时验证配置的有效性：

- `data_dir` 不能为空
- 端口范围必须在 1-65535 之间
- 日志级别必须是有效值（debug, info, warn, error, fatal）

## 多协议支持

配置系统设计支持多协议，当前已实现：

- ✅ ES 协议（Elasticsearch 兼容协议）

预留协议（配置结构已定义，待实现）：

- ⏳ Redis 协议
- ⏳ MySQL 协议
- ⏳ PostgreSQL 协议

所有协议共享 `data_dir` 配置，但可以独立配置各自的端口、主机等参数。
