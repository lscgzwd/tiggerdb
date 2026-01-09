# TigerDB HTTP 认证使用指南

**版本**: v1.0.0  
**更新时间**: 2025-12-30

---

## 一、概述

TigerDB ES 协议层支持三种 HTTP 认证方式：

1. **Basic Auth**: 用户名/密码认证（最常用）
2. **Bearer Token**: 持有 token 认证
3. **API Key**: API Key 认证（通过请求头）

---

## 二、配置认证

### 2.1 启用 Basic Auth

在配置文件中添加：

```yaml
es:
  enabled: true
  server_config:
    host: "0.0.0.0"
    port: 19200
  auth:
    enabled: true
    type: "basic"
    username: "admin"
    password: "tigerdb2024"
    realm: "TigerDB"
```

### 2.2 启用 Bearer Token

```yaml
es:
  enabled: true
  server_config:
    host: "0.0.0.0"
    port: 19200
  auth:
    enabled: true
    type: "bearer"
    apikeys:
      "token1": true
      "token2": true
      "token3": true
    realm: "TigerDB"
```

### 2.3 启用 API Key

```yaml
es:
  enabled: true
  server_config:
    host: "0.0.0.0"
    port: 19200
  auth:
    enabled: true
    type: "apikey"
    apikeys:
      "key1": true
      "key2": true
      "key3": true
    realm: "TigerDB"
```

### 2.4 禁用认证

```yaml
es:
  enabled: true
  server_config:
    host: "0.0.0.0"
    port: 19200
  auth:
    enabled: false
```

---

## 三、客户端使用

### 3.1 使用 Basic Auth

**curl 示例**：

```bash
# 方式1：使用 -u 参数
curl -u admin:tigerdb2024 http://localhost:19200/myindex/_search \
  -H "Content-Type: application/json" \
  -d '{"query": {"match_all": {}}}'

# 方式2：使用 Authorization 头
curl http://localhost:19200/myindex/_search \
  -H "Authorization: Basic YWRtaW46dGlnZXJkYjIwMjQ=" \
  -H "Content-Type: application/json" \
  -d '{"query": {"match_all": {}}}'
```

**生成 Base64 编码**：

```bash
# Linux/Mac
echo -n "admin:tigerdb2024" | base64

# Windows PowerShell
[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("admin:tigerdb2024"))
```

**Python 示例**：

```python
import requests
from requests.auth import HTTPBasicAuth

# 方式1：使用 HTTPBasicAuth
response = requests.get(
    'http://localhost:19200/myindex/_search',
    auth=HTTPBasicAuth('admin', 'tigerdb2024'),
    json={'query': {'match_all': {}}}
)

# 方式2：使用 headers
response = requests.get(
    'http://localhost:19200/myindex/_search',
    headers={'Authorization': 'Basic YWRtaW46dGlnZXJkYjIwMjQ='},
    json={'query': {'match_all': {}}}
)
```

**JavaScript/Node.js 示例**：

```javascript
const axios = require("axios");

// 方式1：使用 auth 配置
const response = await axios.get("http://localhost:19200/myindex/_search", {
  auth: {
    username: "admin",
    password: "tigerdb2024",
  },
  data: { query: { match_all: {} } },
});

// 方式2：使用 headers
const response = await axios.get("http://localhost:19200/myindex/_search", {
  headers: {
    Authorization:
      "Basic " + Buffer.from("admin:tigerdb2024").toString("base64"),
  },
  data: { query: { match_all: {} } },
});
```

**Elasticsearch 客户端示例**：

```javascript
const { Client } = require("@elastic/elasticsearch");

const client = new Client({
  node: "http://localhost:19200",
  auth: {
    username: "admin",
    password: "tigerdb2024",
  },
});

// 使用客户端
const result = await client.search({
  index: "myindex",
  body: {
    query: {
      match_all: {},
    },
  },
});
```

### 3.2 使用 Bearer Token

**curl 示例**：

```bash
curl http://localhost:19200/myindex/_search \
  -H "Authorization: Bearer token1" \
  -H "Content-Type: application/json" \
  -d '{"query": {"match_all": {}}}'
```

**Python 示例**：

```python
import requests

response = requests.get(
    'http://localhost:19200/myindex/_search',
    headers={'Authorization': 'Bearer token1'},
    json={'query': {'match_all': {}}}
)
```

**JavaScript 示例**：

```javascript
const axios = require("axios");

const response = await axios.get("http://localhost:19200/myindex/_search", {
  headers: {
    Authorization: "Bearer token1",
  },
  data: { query: { match_all: {} } },
});
```

### 3.3 使用 API Key

**curl 示例**：

```bash
curl http://localhost:19200/myindex/_search \
  -H "X-API-Key: key1" \
  -H "Content-Type: application/json" \
  -d '{"query": {"match_all": {}}}'
```

**Python 示例**：

```python
import requests

response = requests.get(
    'http://localhost:19200/myindex/_search',
    headers={'X-API-Key': 'key1'},
    json={'query': {'match_all': {}}}
)
```

**JavaScript 示例**：

```javascript
const axios = require("axios");

const response = await axios.get("http://localhost:19200/myindex/_search", {
  headers: {
    "X-API-Key": "key1",
  },
  data: { query: { match_all: {} } },
});
```

---

## 四、公开路径

以下路径无需认证（即使启用了认证）：

| 路径               | 说明                        |
| ------------------ | --------------------------- |
| `/`                | 根路径（健康检查）          |
| `/_ping`           | Ping API                    |
| `/_cluster/health` | 集群健康检查                |
| `/_cat/*`          | Cat API（某些环境可能公开） |

---

## 五、认证失败响应

当认证失败时，服务器返回：

**HTTP 状态码**: `401 Unauthorized`

**响应头**：

```
WWW-Authenticate: Basic realm="TigerDB"
```

**响应体**：

```
Unauthorized
```

---

## 六、安全建议

### 6.1 生产环境

1. **使用 HTTPS**：在生产环境中，务必启用 TLS/HTTPS 加密传输
2. **强密码**：使用强密码（至少 16 位，包含大小写字母、数字、特殊字符）
3. **定期轮换**：定期更换密码和 API Key
4. **最小权限**：为不同用户分配不同的 API Key，实现最小权限原则

### 6.2 开发环境

1. **禁用认证**：开发环境可以禁用认证以简化测试
2. **使用简单密码**：开发环境可以使用简单密码，但不要提交到代码仓库

### 6.3 配置管理

1. **环境变量**：敏感信息（密码、API Key）应通过环境变量传递，不要硬编码在配置文件中
2. **配置文件权限**：确保配置文件权限设置正确（仅所有者可读）
3. **密钥管理**：考虑使用密钥管理服务（如 HashiCorp Vault、AWS Secrets Manager）

---

## 七、故障排查

### 7.1 认证失败

**问题**：返回 401 Unauthorized

**可能原因**：

1. 用户名或密码错误
2. API Key 不存在或已禁用
3. 认证类型配置错误
4. Authorization 头格式错误

**解决方案**：

1. 检查配置文件中的认证信息
2. 验证 Authorization 头的格式
3. 查看服务器日志获取详细错误信息

### 7.2 认证未生效

**问题**：即使配置了认证，请求仍然可以访问

**可能原因**：

1. `auth.enabled` 设置为 `false`
2. 访问的是公开路径（/, /\_ping, /\_cluster/health）
3. 配置未正确加载

**解决方案**：

1. 检查配置文件中的 `auth.enabled` 设置
2. 确认访问的路径不是公开路径
3. 重启服务以加载新配置

---

## 八、示例配置

### 8.1 开发环境配置

```yaml
es:
  enabled: true
  server_config:
    host: "0.0.0.0"
    port: 19200
  auth:
    enabled: false # 开发环境禁用认证
```

### 8.2 生产环境配置

```yaml
es:
  enabled: true
  server_config:
    host: "0.0.0.0"
    port: 19200
  auth:
    enabled: true
    type: "basic"
    username: "${ES_USERNAME}" # 从环境变量读取
    password: "${ES_PASSWORD}" # 从环境变量读取
    realm: "TigerDB Production"
```

### 8.3 多用户 API Key 配置

```yaml
es:
  enabled: true
  server_config:
    host: "0.0.0.0"
    port: 19200
  auth:
    enabled: true
    type: "apikey"
    apikeys:
      "readonly-key": true # 只读用户
      "write-key": true # 写入用户
      "admin-key": true # 管理员
    realm: "TigerDB"
```

---

## 九、API 参考

### 9.1 AuthConfig 结构

```go
type AuthConfig struct {
    Enabled  bool              // 是否启用认证
    Type     string            // 认证类型: "basic", "bearer", "apikey"
    Username  string            // Basic Auth 用户名
    Password  string            // Basic Auth 密码
    ApiKeys   map[string]bool  // API Key 列表
    Realm     string            // 认证域
}
```

### 9.2 配置字段说明

| 字段       | 类型            | 必填 | 说明                                                  |
| ---------- | --------------- | ---- | ----------------------------------------------------- |
| `enabled`  | bool            | 是   | 是否启用认证                                          |
| `type`     | string          | 是   | 认证类型：`basic`、`bearer`、`apikey`                 |
| `username` | string          | 条件 | Basic Auth 用户名（type="basic" 时必填）              |
| `password` | string          | 条件 | Basic Auth 密码（type="basic" 时必填）                |
| `apikeys`  | map[string]bool | 条件 | API Key 列表（type="bearer" 或 type="apikey" 时必填） |
| `realm`    | string          | 否   | 认证域（默认：`TigerDB`）                             |

---

## 十、更新日志

### v1.0.0 (2025-12-30)

- ✅ 实现 Basic Auth 认证
- ✅ 实现 Bearer Token 认证
- ✅ 实现 API Key 认证
- ✅ 支持公开路径配置
- ✅ 完整的错误处理和日志记录

---

**文档维护**: TigerDB 开发团队  
**最后更新**: 2025-12-30
