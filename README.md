# PeerDiscoveryCenter

统一的 BitTorrent Peer 发现中心：Tracker + DHT + PEX 三合一。

PeerDiscoveryCenter 是 PandaNetOS 生态中的核心组件，提供统一的 peer 发现接口，内部整合了三种发现机制，支持高并发、智能调度、自动健康检查。

## 功能特性

- **三合一发现机制**：Tracker（HTTP/UDP）+ DHT（Kademlia）+ PEX（Peer Exchange）统一封装
- **并发调度**：所有发现器并发运行，自动合并、去重、按优先级排序
- **智能缓存**：按 infohash 分组缓存，支持过期清理、连接反馈、LRU 淘汰
- **健康检查**：后台任务定期检查所有发现器健康状态，自动故障转移
- **统一接口**：`PeerDiscoverer` trait 统一所有发现机制，易于扩展新协议
- **统计监控**：完整的请求统计、成功率、响应时间、peer 发现数监控
- **协议无关**：上层架构与具体协议解耦，未来可扩展其他 peer 发现协议

## 标准库路径约定

本项目依赖 PandaNetOS 标准库，必须使用 path 依赖（本地开发）：

```toml
[dependencies]
pandanetos = { path = "../PandaNetOS/crates/pandanetos" }
```

**禁止使用 git 依赖**，所有项目必须与 PandaNetOS 标准库同级目录放置。

PandaNetOS 标准库提供：
- 统一的协议定义（`pandanetos::protocol`）
- 统一的错误处理（`pandanetos::error`）
- 统一的配置管理（`pandanetos::config`）
- 统一的日志规范（`pandanetos::logging`）

## 快速开始

### 环境要求

- Rust 1.75+（建议使用最新稳定版）
- Cargo 包管理器
- Git
- 网络连接（用于下载依赖和 peer 发现）

### 安装/构建

```bash
# 克隆仓库（与 PandaNetOS 同级目录）
git clone https://github.com/pandamelive/PeerDiscoveryCenter.git
cd PeerDiscoveryCenter

# 构建
cargo build --release

# 运行测试
cargo test
```

### 使用示例

```rust
use PeerDiscoveryCenter::aggregator::{PeerDiscoveryAggregator, PeerDiscoveryConfig};
use PeerDiscoveryCenter::tracker::TrackerDiscoverer;
use PeerDiscoveryCenter::dht::DhtDiscoverer;
use PeerDiscoveryCenter::pex::PexDiscoverer;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 创建聚合器
    let config = PeerDiscoveryConfig::default();
    let aggregator = Arc::new(PeerDiscoveryAggregator::new(config));

    // 2. 添加发现器
    aggregator.add_discoverer(Box::new(TrackerDiscoverer::with_default_config()));
    aggregator.add_discoverer(Box::new(DhtDiscoverer::with_default_config()));
    aggregator.add_discoverer(Box::new(PexDiscoverer::with_default_config()));

    // 3. 发现 peer
    let infohash = [0u8; 20]; // 替换为实际的 infohash
    let result = aggregator.discover_peers(&infohash, 100).await?;

    // 4. 处理结果
    for peer in &result.peers {
        // 连接 peer 并下载数据
    }

    Ok(())
}
```

## 配置说明

### PeerDiscoveryConfig

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `max_cached_peers` | `usize` | 10000 | 最大缓存 peer 数 |
| `peer_ttl` | `Duration` | 24小时 | Peer 过期时间 |
| `discovery_timeout` | `Duration` | 30秒 | 单次发现超时 |
| `max_concurrent_discoverers` | `usize` | 10 | 并发发现器数量限制 |
| `enable_tracker` | `bool` | true | 是否启用 Tracker |
| `enable_dht` | `bool` | true | 是否启用 DHT |
| `enable_pex` | `bool` | true | 是否启用 PEX |
| `max_peers_per_discovery` | `usize` | 200 | 每次发现的最大 peer 数 |

### TrackerConfig

- `trackers`: Tracker URL 列表（默认包含 100+ 公共 Tracker）
- `timeout`: 请求超时（默认 15 秒）
- `max_concurrent_requests`: 最大并发请求数（默认 10）
- `max_consecutive_failures`: 连续失败阈值（默认 3 次后临时禁用）
- `cooldown_duration`: 禁用恢复时间（默认 5 分钟）

### DhtConfig

- `bootstrap_nodes`: Bootstrap 节点列表
- `listen_port`: DHT 监听端口（默认 6881）
- `refresh_interval`: 路由表刷新间隔（默认 5 分钟）
- `node_ttl`: 节点过期时间（默认 1 小时）

### PexConfig

- `max_connected_peers`: 最大已连接 peer 数（默认 50）
- `pex_request_interval`: PEX 请求间隔（默认 60 秒）
- `max_peers_per_request`: 每个 peer 每次返回的最大 peer 数（默认 50）

## 项目结构

```
PeerDiscoveryCenter/
├── src/
│   ├── lib.rs              # 库入口，模块声明和重新导出
│   ├── types.rs            # 公共数据结构（PeerInfo、PeerSource 等）
│   ├── traits.rs           # 统一的 PeerDiscoverer trait 定义
│   ├── cache.rs            # Peer 缓存（去重、优先级、过期、LRU）
│   ├── aggregator.rs       # 核心聚合器（并发调度、合并排序）
│   ├── health_check.rs     # 健康检查后台任务
│   ├── tracker/            # Tracker 发现机制
│   │   ├── mod.rs
│   │   └── client.rs       # Tracker 客户端（HTTP/UDP）
│   ├── dht/                # DHT 发现机制
│   │   ├── mod.rs
│   │   └── client.rs       # DHT 客户端（Kademlia）
│   └── pex/                # PEX 发现机制
│       ├── mod.rs
│       └── client.rs       # PEX 客户端
├── Cargo.toml              # 项目配置
├── README.md               # 项目说明
└── .github/
    └── workflows/          # CI/CD 工作流
        ├── cargo-test.yml
        ├── cargo-format.yml
        ├── cargo-clippy.yml
        ├── compliance.yml
        └── tag-guard.yml
```

## 开发指南

### 构建

```bash
# Debug 构建
cargo build

# Release 构建
cargo build --release

# 检查编译（不生成二进制）
cargo check
```

### 测试

```bash
# 运行所有测试
cargo test

# 运行特定模块的测试
cargo test cache

# 运行测试并显示输出
cargo test -- --nocapture
```

### 代码格式

```bash
# 检查格式
cargo fmt --all -- --check

# 自动格式化
cargo fmt --all
```

### Clippy 检查

```bash
# 运行 Clippy
cargo clippy --all-targets

# 严格模式（警告视为错误）
cargo clippy --all-targets -- -D warnings
```

### 合规检查

所有提交必须通过 PandaNetOS 生态合规检查：

```bash
# 运行合规检查（在项目根目录）
bash ../PandaNetOS/scripts/check_compliance.sh .
```

合规检查包含 10 项：
1. 标准库依赖检查
2. 目录布局检查
3. README 规范检查
4. 代码格式检查
5. Clippy 检查
6. 单元测试
7. 敏感信息检查
8. 代码规范检查
9. Tag Guard 工作流检查
10. CI/CD 工作流完整性检查

## 贡献指南

欢迎提交 Issue 和 Pull Request！

### 提交规范

- 所有代码必须通过 `cargo fmt` 和 `cargo clippy` 检查
- 所有公共 API 必须有文档注释
- 新增功能必须包含单元测试
- 提交信息遵循 Conventional Commits 规范
- PR 必须通过所有 CI 检查才能合并

### 开发流程

1. Fork 本仓库
2. 创建特性分支（`git checkout -b feature/amazing-feature`）
3. 提交更改（`git commit -m 'feat: add amazing feature'`）
4. 推送到分支（`git push origin feature/amazing-feature`）
5. 开启 Pull Request

## 变更日志

### v0.1.0 (2026-09-02)

- 初始版本发布
- 实现 Tracker 发现机制（HTTP/HTTPS）
- 实现 DHT 发现机制骨架（Kademlia）
- 实现 PEX 发现机制骨架
- 实现统一的 PeerDiscoverer trait
- 实现核心聚合器（并发调度、合并去重、优先级排序）
- 实现 Peer 缓存（过期清理、连接反馈、LRU 淘汰）
- 实现健康检查后台任务
- 完整的单元测试覆盖
- PandaNetOS 生态合规

## 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件。

Copyright (c) 2026 PandaNetOS
