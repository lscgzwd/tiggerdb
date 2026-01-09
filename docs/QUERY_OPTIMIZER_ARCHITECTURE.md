# 查询优化器架构设计

## 1. 设计原则

### 1.1 保守优化原则

- **不改变查询语义**: 所有优化必须保证查询结果完全一致
- **可开关控制**: 优化器可以随时启用/禁用，默认启用
- **失败安全**: 优化失败不影响查询执行，返回原查询
- **低开销**: 优化本身的开销要远小于优化带来的收益

### 1.2 避免负优化

- **不进行复杂分析**: 避免过度分析导致性能下降
- **只做明显优化**: 只优化那些明显有益的查询模式
- **避免过度优化**: 不进行可能带来副作用的优化

## 2. 优化策略

### 2.1 Boolean 查询优化

#### 优化 1: 移除 match_all

```go
// 优化前
{
  "bool": {
    "must": [
      {"match_all": {}},
      {"term": {"status": "active"}}
    ]
  }
}

// 优化后
{
  "bool": {
    "must": [
      {"term": {"status": "active"}}
    ]
  }
}
```

#### 优化 2: Should 子句重排序

```go
// 优化前
{
  "bool": {
    "should": [
      {"match": {"title": "elasticsearch"}},      // 低选择性
      {"term": {"category": "tech"}},             // 高选择性
      {"match_phrase": {"content": "search"}}     // 中低选择性
    ]
  }
}

// 优化后
{
  "bool": {
    "should": [
      {"term": {"category": "tech"}},             // 高选择性在前
      {"match_phrase": {"content": "search"}},     // 中低选择性
      {"match": {"title": "elasticsearch"}}       // 低选择性在后
    ]
  }
}
```

#### 优化 3: 合并相同字段的 term 查询

```go
// 优化前
{
  "bool": {
    "should": [
      {"term": {"status": "active"}},
      {"term": {"status": "pending"}},
      {"term": {"status": "closed"}}
    ]
  }
}

// 优化后
{
  "bool": {
    "should": [
      {"terms": {"status": ["active", "pending", "closed"]}}
    ]
  }
}
```

### 2.2 Conjunction 查询优化

#### 优化: 移除 match_all

```go
// 优化前
{
  "must": [
    {"match_all": {}},
    {"term": {"status": "active"}}
  ]
}

// 优化后
{
  "must": [
    {"term": {"status": "active"}}
  ]
}
```

#### 优化: 单查询简化

```go
// 优化前
{
  "must": [
    {"term": {"status": "active"}}
  ]
}

// 优化后
{
  "term": {"status": "active"}
}
```

### 2.3 Disjunction 查询优化

#### 优化: Should 子句重排序

- 与 Boolean 查询的 should 优化相同
- 将高选择性查询放在前面

## 3. 选择性估算

### 3.1 选择性分数表

| 查询类型                                   | 选择性分数 | 说明                     |
| ------------------------------------------ | ---------- | ------------------------ |
| TermQuery                                  | 100        | 精确匹配，选择性最高     |
| TermsQuery (DisjunctionQuery of TermQuery) | 90         | 多个精确匹配，选择性较高 |
| NumericRangeQuery                          | 80         | 范围查询，选择性较高     |
| BooleanQuery (with must)                   | 40-100     | 取决于 must 的选择性     |
| MatchQuery                                 | 30         | 全文匹配，选择性较低     |
| MatchPhraseQuery                           | 20         | 短语匹配，选择性更低     |
| MatchAllQuery                              | 0          | 匹配所有，选择性最低     |

### 3.2 估算方法

- **简单启发式**: 基于查询类型，不进行实际统计
- **低开销**: 只做类型判断，不访问索引
- **保守估计**: 对于未知类型，给中等分数（50）

## 4. 实现细节

### 4.1 优化器接口

```go
type QueryOptimizer struct {
    enabled bool
}

func (o *QueryOptimizer) Optimize(q query.Query) (query.Query, error)
```

### 4.2 集成点

- **位置**: `protocols/es/search/dsl/parser.go`
- **时机**: 查询解析完成后，返回前
- **方式**: 自动应用，可配置开关

### 4.3 错误处理

- 优化失败不影响查询执行
- 记录警告日志，返回原查询
- 不抛出异常，保证系统稳定性

## 5. 使用示例

### 5.1 默认使用（启用优化）

```go
parser := dsl.NewQueryParser()
query, err := parser.ParseQuery(queryMap)
// 自动应用优化器
```

### 5.2 禁用优化

```go
parser := dsl.NewQueryParser()
parser.SetOptimizerEnabled(false)
query, err := parser.ParseQuery(queryMap)
// 不应用优化器
```

### 5.3 手动优化

```go
optimizer := dsl.NewQueryOptimizer()
optimizedQuery, err := optimizer.Optimize(query)
```

## 6. 注意事项

### 6.1 不进行以下优化

- 改变查询语义的优化
- 需要统计信息的优化（避免额外开销）
- 可能带来副作用的优化
- 过度复杂的优化

### 6.2 优化限制

- 只优化 Boolean、Conjunction、Disjunction 查询
- 不优化其他查询类型（避免意外）
- 优化失败自动回退

### 6.3 性能考虑

- 优化器开销 < 1ms
- 优化收益 > 5ms（对于复杂查询）
- 简单查询可能没有明显收益，但也不会负优化
