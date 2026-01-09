# TigerDB ES 协议层测试指南

**最后更新**: 2025-12-29  
**版本**: v1.0

---

## 一、测试概述

TigerDB ES 协议层现在包含完整的测试套件：

1. **单元测试** - 测试单个函数和组件
2. **集成测试** - 测试完整的 API 工作流程
3. **性能测试** - 基准测试（Benchmark）
4. **压力测试** - 并发和大数据量测试

---

## 二、运行测试

### 2.1 运行所有测试

```bash
# 运行所有测试
go test ./protocols/es/...

# 运行测试并显示覆盖率
go test -cover ./protocols/es/...

# 运行测试并生成覆盖率报告
go test -coverprofile=coverage.out ./protocols/es/...
go tool cover -html=coverage.out
```

### 2.2 运行集成测试

```bash
# 运行集成测试
go test -v ./protocols/es -run TestESIntegration

# 运行特定集成测试
go test -v ./protocols/es -run TestESIntegration_CompleteWorkflow
go test -v ./protocols/es -run TestESIntegration_BulkOperations
go test -v ./protocols/es -run TestESIntegration_Aggregations
```

### 2.3 运行性能测试（Benchmark）

```bash
# 运行所有性能测试
go test -bench=. ./protocols/es/...

# 运行特定性能测试
go test -bench=BenchmarkSearch_SimpleQuery ./protocols/es
go test -bench=BenchmarkSearch_ComplexQuery ./protocols/es
go test -bench=BenchmarkSearch_WithAggregations ./protocols/es
go test -bench=BenchmarkBulk_IndexDocuments ./protocols/es
go test -bench=BenchmarkCount_SimpleQuery ./protocols/es

# 运行性能测试并显示内存分配
go test -bench=. -benchmem ./protocols/es/...

# 运行性能测试并生成CPU profile
go test -bench=. -cpuprofile=cpu.prof ./protocols/es/...
go tool pprof cpu.prof
```

### 2.4 运行压力测试

```bash
# 运行压力测试（需要构建标签）
go test -tags=stress -v ./protocols/es -run TestStress

# 运行特定压力测试
go test -tags=stress -v ./protocols/es -run TestStress_ConcurrentSearches
go test -tags=stress -v ./protocols/es -run TestStress_ConcurrentBulk
go test -tags=stress -v ./protocols/es -run TestStress_MixedOperations
go test -tags=stress -v ./protocols/es -run TestStress_LargeDataset

# 跳过短测试模式（压力测试会自动跳过短模式）
go test -short=false -tags=stress -v ./protocols/es -run TestStress
```

---

## 三、测试类型详解

### 3.1 集成测试

**文件**: `protocols/es/integration_test.go`

**测试内容**:

- ✅ 完整的 ES 工作流程（创建索引 → 索引文档 → 搜索 → 更新 → 删除）
- ✅ 批量操作（Bulk API）
- ✅ 聚合功能（Terms、Avg 等）

**示例**:

```go
func TestESIntegration_CompleteWorkflow(t *testing.T) {
    // 测试完整的CRUD工作流程
    // 1. 创建索引
    // 2. 索引文档
    // 3. 获取文档
    // 4. 搜索文档
    // 5. 更新文档
    // 6. 删除文档
    // 7. 删除索引
}
```

### 3.2 性能测试（Benchmark）

**文件**: `protocols/es/benchmark_test.go`

**测试内容**:

- ✅ 简单查询性能（`BenchmarkSearch_SimpleQuery`）
- ✅ 复杂查询性能（`BenchmarkSearch_ComplexQuery`）
- ✅ 聚合查询性能（`BenchmarkSearch_WithAggregations`）
- ✅ 批量索引性能（`BenchmarkBulk_IndexDocuments`）
- ✅ Count 查询性能（`BenchmarkCount_SimpleQuery`）

**性能目标**:

- 简单查询: < 10ms
- 复杂查询: < 50ms
- 聚合查询: < 200ms
- 批量索引: > 1000 docs/s
- 30 万数据查询: < 100ms

**示例输出**:

```
BenchmarkSearch_SimpleQuery-8    1000    12345678 ns/op    123456 B/op    1234 allocs/op
```

### 3.3 压力测试

**文件**: `protocols/es/stress_test.go`

**测试内容**:

- ✅ 并发搜索（`TestStress_ConcurrentSearches`）
  - 50 个并发 goroutine
  - 持续 30 秒
  - 10 万文档数据集
- ✅ 并发批量操作（`TestStress_ConcurrentBulk`）

  - 20 个并发 goroutine
  - 每个 goroutine 100 次操作
  - 每批 100 条文档

- ✅ 混合操作（`TestStress_MixedOperations`）

  - 搜索、索引、更新混合
  - 30 个并发 goroutine
  - 持续 20 秒

- ✅ 大数据集查询（`TestStress_LargeDataset`）
  - 30 万文档
  - 多种查询类型
  - 验证性能目标

**压力测试指标**:

- QPS（每秒查询数）
- 错误率（应 < 10%）
- 响应时间分布

---

## 四、测试工具

### 4.1 测试服务器工具

**文件**: `protocols/es/testutil/test_server.go`

提供测试服务器创建和管理功能：

```go
ts, err := testutil.NewTestServer()
if err != nil {
    t.Fatalf("Failed to create test server: %v", err)
}
defer ts.Close()

// 使用 ts.BaseURL 进行HTTP请求
resp, err := http.Get(ts.BaseURL + "/test_index/_search")
```

**特性**:

- 自动创建临时目录
- 自动启动 HTTP 服务器
- 自动清理资源
- 随机端口（避免冲突）

### 4.2 测试数据生成

集成测试和性能测试包含测试数据生成功能：

```go
// 设置基准测试索引（1万文档）
setupBenchmarkIndex(baseURL, indexName, 10000, b)

// 设置压力测试索引（10万文档）
setupStressIndex(baseURL, indexName, 100000, t)
```

---

## 五、CI/CD 集成

### 5.1 GitHub Actions 示例

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-go@v2
        with:
          go-version: "1.23"

      # 运行单元测试
      - name: Run unit tests
        run: go test -v ./protocols/es/handler/...

      # 运行集成测试
      - name: Run integration tests
        run: go test -v ./protocols/es -run TestESIntegration

      # 运行性能测试（限制时间）
      - name: Run benchmarks
        run: go test -bench=. -benchtime=1s ./protocols/es/...

      # 生成覆盖率报告
      - name: Generate coverage
        run: |
          go test -coverprofile=coverage.out ./protocols/es/...
          go tool cover -html=coverage.out -o coverage.html
```

### 5.2 本地开发测试

```bash
# 快速测试（跳过压力测试）
go test -short ./protocols/es/...

# 完整测试（包括压力测试）
go test -tags=stress ./protocols/es/...

# 持续监控性能
watch -n 5 'go test -bench=BenchmarkSearch_SimpleQuery ./protocols/es'
```

---

## 六、测试最佳实践

### 6.1 测试组织

- **单元测试**: 放在与被测试文件同目录，文件名 `*_test.go`
- **集成测试**: 放在 `protocols/es/integration_test.go`
- **性能测试**: 放在 `protocols/es/benchmark_test.go`
- **压力测试**: 放在 `protocols/es/stress_test.go`，使用 `+build stress` 标签

### 6.2 测试数据

- **小数据集**: 1-1000 文档（单元测试）
- **中数据集**: 1 万-10 万文档（集成测试、性能测试）
- **大数据集**: 30 万-100 万文档（压力测试）

### 6.3 测试隔离

- 每个测试使用独立的临时目录
- 每个测试使用独立的索引名称
- 测试完成后自动清理资源

### 6.4 性能基准

定期运行性能测试，记录基准数据：

```bash
# 保存基准数据
go test -bench=. -benchmem ./protocols/es/... > benchmark.txt

# 对比基准数据
go test -bench=. -benchmem -benchcmp=benchmark.txt ./protocols/es/...
```

---

## 七、测试覆盖率

### 7.1 查看覆盖率

```bash
# 生成覆盖率报告
go test -coverprofile=coverage.out ./protocols/es/...
go tool cover -func=coverage.out

# 生成HTML覆盖率报告
go tool cover -html=coverage.out -o coverage.html
```

### 7.2 覆盖率目标

- **单元测试覆盖率**: > 80%
- **集成测试覆盖率**: > 70%
- **总体覆盖率**: > 75%

---

## 八、故障排查

### 8.1 常见问题

**问题**: 测试服务器启动失败

```bash
# 检查端口占用
netstat -an | grep 9200

# 使用随机端口（测试工具已自动处理）
```

**问题**: 压力测试超时

```bash
# 增加超时时间
go test -timeout=10m -tags=stress ./protocols/es
```

**问题**: 内存不足

```bash
# 减少测试数据量
# 修改 setupStressIndex 中的 docCount 参数
```

### 8.2 调试技巧

```bash
# 详细输出
go test -v ./protocols/es

# 只运行失败的测试
go test -v ./protocols/es -run TestESIntegration_CompleteWorkflow

# 使用调试器
dlv test ./protocols/es -run TestESIntegration_CompleteWorkflow
```

---

## 九、测试报告

### 9.1 测试结果示例

**集成测试**:

```
=== RUN   TestESIntegration_CompleteWorkflow
=== RUN   TestESIntegration_CompleteWorkflow/CreateIndex
=== RUN   TestESIntegration_CompleteWorkflow/IndexDocument
=== RUN   TestESIntegration_CompleteWorkflow/GetDocument
=== RUN   TestESIntegration_CompleteWorkflow/SearchDocument
=== RUN   TestESIntegration_CompleteWorkflow/UpdateDocument
=== RUN   TestESIntegration_CompleteWorkflow/DeleteDocument
=== RUN   TestESIntegration_CompleteWorkflow/DeleteIndex
--- PASS: TestESIntegration_CompleteWorkflow (0.15s)
```

**性能测试**:

```
BenchmarkSearch_SimpleQuery-8         1000    12345678 ns/op    123456 B/op    1234 allocs/op
BenchmarkSearch_ComplexQuery-8          500    23456789 ns/op    234567 B/op    2345 allocs/op
BenchmarkSearch_WithAggregations-8     200    45678901 ns/op    456789 B/op    4567 allocs/op
```

**压力测试**:

```
=== RUN   TestStress_ConcurrentSearches
Stress test results:
  Duration: 30s
  Concurrency: 50
  Success: 15000
  Errors: 5
  QPS: 500.00
--- PASS: TestStress_ConcurrentSearches (30.15s)
```

---

## 十、总结

### 10.1 测试覆盖

| 测试类型 | 文件                  | 测试数量 | 状态    |
| -------- | --------------------- | -------- | ------- |
| 单元测试 | `handler/*_test.go`   | 50+      | ✅ 完成 |
| 集成测试 | `integration_test.go` | 3        | ✅ 完成 |
| 性能测试 | `benchmark_test.go`   | 5        | ✅ 完成 |
| 压力测试 | `stress_test.go`      | 4        | ✅ 完成 |

### 10.2 测试质量

- ✅ **完整性**: 覆盖主要 API 和工作流程
- ✅ **可靠性**: 测试隔离，自动清理
- ✅ **性能**: 验证性能目标
- ✅ **压力**: 验证并发和大数据量场景

---

**最后更新**: 2025-12-29
