# 测试指南

> PeerDiscoveryCenter 项目测试指南

## 测试策略

### 测试金字塔

```
        /\
       /  \        E2E 测试（少量）
      /----\
     /      \      集成测试（适量）
    /--------\
   /          \     单元测试（大量）
  /------------\
```

- **单元测试**：测试单个函数/结构体，占 70%
- **集成测试**：测试模块间交互，占 20%
- **E2E 测试**：测试完整流程，占 10%

### 测试原则

1. **快速**：单元测试应该在几秒内完成
2. **可靠**：测试不应该有随机性（除非明确测试随机行为）
3. **独立**：每个测试应该独立运行，不依赖其他测试
4. **可读**：测试代码应该清晰易读，像文档一样
5. **可维护**：测试代码应该和生产代码一样维护

## 单元测试

### 运行单元测试

```bash
# 运行所有单元测试
cargo test --lib

# 运行特定模块的测试
cargo test --lib intelligence

# 运行特定测试
cargo test --lib test_calculate_node_score

# 显示测试输出
cargo test --lib -- --nocapture

# 并行运行测试（默认）
cargo test --lib -- --test-threads=8

# 串行运行测试（调试用）
cargo test --lib -- --test-threads=1
```

### 编写单元测试

#### 测试文件位置

单元测试写在源文件末尾的 `#[cfg(test)] mod tests` 模块中：

```rust
// src/intelligence/node_score.rs

pub fn calculate_node_score(entry: &KBucketEntry) -> f64 {
    // ...
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_perfect_node_score() {
        let mut entry = KBucketEntry::new([0u8; 20], "127.0.0.1:6881".parse().unwrap());
        entry.query_count = 100;
        entry.success_count = 100;
        entry.total_latency_ms = 10000;
        entry.consecutive_failures = 0;

        let score = calculate_node_score(&entry);
        assert!(score > 90.0);
    }
}
```

#### 测试命名规范

- `test_<功能>_<场景>_<预期结果>`
- 示例：
  - `test_calculate_node_score_perfect_node_returns_high_score`
  - `test_add_node_duplicate_returns_false`
  - `test_save_all_empty_does_nothing`

#### 断言规范

- 使用 `assert!`, `assert_eq!`, `assert_ne!`
- 浮点数比较使用范围断言：`assert!((score - 100.0).abs() < 1.0)`
- 错误测试使用 `#[should_panic]` 或 `assert!(result.is_err())`

#### Mock 与 Trait

使用 trait 对象进行 mock：

```rust
#[async_trait]
trait NodeRepository: Send + Sync {
    async fn all_nodes(&self) -> Vec<KBucketEntry>;
    // ...
}

struct MockNodeRepo {
    nodes: Vec<KBucketEntry>,
}

#[async_trait]
impl NodeRepository for MockNodeRepo {
    async fn all_nodes(&self) -> Vec<KBucketEntry> {
        self.nodes.clone()
    }
    // ...
}
```

### 测试覆盖的模块

| 模块 | 测试重点 |
|---|---|
| `intelligence/node_score` | 评分计算、边界条件、特殊规则 |
| `intelligence/peer_score` | Peer 评分、多 infohash 共享 |
| `intelligence/tracker_score` | Tracker 评分、统计计算 |
| `intelligence/tier_system` | 冷热分层、多维度判定、特殊规则 |
| `intelligence/select_system` | 节点选择、多样性、去重 |
| `storage/node_repo` | 节点增删改查、脏标记、批量更新 |
| `storage/peer_repo` | Peer 增删改查、按 infohash 分组 |
| `storage/tracker_repo` | Tracker 增删改查、统计更新 |
| `storage/infohash_repo` | Infohash 注册、引用计数 |
| `storage/db` | SQLite 持久化、批量写入、表结构 |
| `dht/kbucket` | 路由表、节点状态、KBucket 操作 |
| `types` | 数据类型、序列化、反序列化 |

## 集成测试

### 运行集成测试

```bash
# 运行所有集成测试
cargo test --test '*'

# 运行特定集成测试
cargo test --test integration_test_name
```

### 集成测试文件位置

集成测试放在 `tests/` 目录下：

```
tests/
├── integration_test.rs
├── crawler_test.rs
├── tracker_test.rs
└── storage_test.rs
```

### 集成测试示例

```rust
// tests/integration_test.rs

use PeerDiscoveryCenter::storage::db::Storage;
use PeerDiscoveryCenter::storage::node_repo::NodeRepoImpl;
use PeerDiscoveryCenter::storage::repo_traits::NodeRepository;

#[tokio::test]
async fn test_node_repo_persistence() {
    let storage = Storage::memory().unwrap();
    let repo = NodeRepoImpl::new(storage.into());

    // 添加节点
    repo.add_node([0u8; 20], "127.0.0.1:6881".parse().unwrap()).await;

    // 保存到 SQLite
    repo.save_all().await.unwrap();

    // 创建新的 Repo，从 SQLite 加载
    let storage2 = Storage::memory().unwrap(); // 注意：内存数据库不共享，实际测试需要用临时文件
    // ...
}
```

## E2E 测试

### E2E 测试范围

- PDC 启动与关闭
- HTTP API 接口
- WebSocket 监控
- DHT 爬虫基本功能
- Tracker 基本功能

### E2E 测试示例

```rust
// tests/e2e_test.rs

#[tokio::test]
async fn test_pdc_startup() {
    // 启动 PDC
    // 等待端口监听
    // 发送 HTTP 请求
    // 验证响应
    // 关闭 PDC
}
```

## 性能测试

### 运行性能测试

```bash
# 使用 criterion 进行性能测试
cargo bench

# 运行特定基准测试
cargo bench --bench benchmark_name
```

### 性能基准测试

```rust
// benches/node_score_bench.rs

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use PeerDiscoveryCenter::intelligence::calculate_node_score;
use PeerDiscoveryCenter::dht::kbucket::KBucketEntry;

fn bench_calculate_node_score(c: &mut Criterion) {
    let entry = KBucketEntry::new([0u8; 20], "127.0.0.1:6881".parse().unwrap());
    c.bench_function("calculate_node_score", |b| {
        b.iter(|| calculate_node_score(black_box(&entry)))
    });
}

criterion_group!(benches, bench_calculate_node_score);
criterion_main!(benches);
```

### 性能指标

| 指标 | 目标 | 测量方法 |
|---|---|---|
| 评分增量重算延迟 | < 100ms | 日志统计 |
| 评分全量重算延迟 | < 5s（60000节点） | 日志统计 |
| 节点选择延迟 | < 50ms | 日志统计 |
| 数据全量保存延迟 | < 10s | 日志统计 |
| 磁盘 IO | < 1 MB/s | 资源监视器 |
| 内存占用 | < 500 MB（60000节点） | 任务管理器 |
| CPU 使用率 | < 20%（空闲时） | 任务管理器 |

## 代码质量检查

### 格式化检查

```bash
# 检查格式
cargo fmt --all -- --check

# 自动格式化
cargo fmt --all
```

### Clippy 检查

```bash
# 运行 clippy
cargo clippy --all-targets

# 自动修复可修复的警告
cargo clippy --all-targets --fix
```

### 合规检查

```bash
# 运行生态合规检查
bash ../PandaNetOS/scripts/check_compliance.sh .
```

## CI/CD

### CI 流水线

项目使用 GitHub Actions 进行 CI，包含以下检查：

1. **cargo fmt** — 代码格式检查
2. **cargo clippy** — 代码质量检查
3. **cargo test** — 单元测试
4. **cargo build** — 构建验证
5. **compliance** — 生态合规检查

### CI 配置文件

- `.github/workflows/cargo-format.yml`
- `.github/workflows/cargo-clippy.yml`
- `.github/workflows/cargo-test.yml`
- `.github/workflows/compliance.yml`

## 测试覆盖率

### 生成覆盖率报告

```bash
# 使用 cargo-tarpaulin
cargo install cargo-tarpaulin
cargo tarpaulin --out Html
```

### 覆盖率目标

| 模块 | 目标覆盖率 |
|---|---|
| intelligence（智能层） | > 80% |
| storage（数据层） | > 70% |
| dht（DHT 协议） | > 60% |
| services（业务层） | > 50% |
| 整体 | > 60% |

## 调试技巧

### 打印调试

```rust
// 使用 dbg! 宏
dbg!(&node);

// 使用 println!
println!("node score: {}", node.score);

// 使用 tracing
tracing::info!("node score: {}", node.score);
tracing::debug!("node details: {:?}", node);
```

### 日志级别

```bash
# 设置日志级别
RUST_LOG=debug ./target/release/pdc.exe

# 只显示特定模块的日志
RUST_LOG=pdс::intelligence=debug ./target/release/pdc.exe
```

### 测试失败调试

```bash
# 显示测试输出
cargo test --lib -- --nocapture

# 只运行失败的测试
cargo test --lib -- --failed

# 串行运行测试（避免并行干扰）
cargo test --lib -- --test-threads=1
```

## 常见问题

### Q: 测试中如何使用临时数据库文件？

A: 使用 `tempfile` crate 创建临时文件：
```rust
let temp_dir = tempfile::tempdir().unwrap();
let db_path = temp_dir.path().join("test.db");
let storage = Storage::open(&db_path).unwrap();
```

### Q: 异步测试如何编写？

A: 使用 `#[tokio::test]` 宏：
```rust
#[tokio::test]
async fn test_async_function() {
    let result = async_function().await;
    assert!(result.is_ok());
}
```

### Q: 测试中如何模拟时间？

A: 目前项目使用真实时间，测试中需要等待真实时间。未来可以引入 `tokio::time::pause()` 进行时间模拟。

### Q: 如何运行特定模块的测试？

A: 使用模块路径过滤：
```bash
cargo test --lib intelligence::node_score
```

## 参考资料

- [Rust 测试指南](https://doc.rust-lang.org/book/ch11-00-testing.html)
- [Rust 异步测试](https://tokio.rs/tokio/tutorial/testing)
- [Criterion 性能测试](https://bheisler.github.io/criterion.rs/book/)
- [cargo-tarpaulin 覆盖率](https://github.com/xd009642/tarpaulin)
