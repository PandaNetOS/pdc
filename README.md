# pdc（Peer Discovery Center）

PandaNetOS 生态的**节点发现 Agent**：超级 Tracker + DHT 爬虫 + 多协议发现器 + 联邦同步。

pdc（原 PeerDiscoveryCenter，已重命名）是 Agent 级独立进程，注册到 pnos-runtime，负责 P2P 网络中的节点发现、peer 查询与多实例数据同步。当前版本 **v0.2.0**。

## 核心能力

- **超级 Tracker**：TCP + UDP 双协议，标准 `/announce`、`/scrape` 接口（兼容 qBittorrent 等），按活跃时间排序 + 缓存加速响应
- **DHT 爬虫**：多 socket 架构（`socket_count` 可配，上限 10），tid 高位编码 socket 索引；自适应限速（按响应率动态启停）+ 预测式控制器（SGD 模型预测发送倍率）
- **多协议发现器**：Tracker（HTTP/UDP）、DHT（Kademlia）、PEX、LPD 多播，统一 `PeerDiscoverer` trait 插件化接入
- **联邦同步**：多实例互联，Gossip 实时推送 + delta（oplog 增量）+ Range 反熵（唯一兜底通道）+ bootstrap 全量引导（协议 v8 去 Merkle 化，v9 收敛修复）
- **智能层**：TaskScheduler 统一提调全部周期任务（按分类分级并发）、IOScheduler 平滑 IO、WAL checkpoint 两层机制、冷热分层（TierSystem）框架
- **控制层**：HTTP API + WebSocket 实时监控、配置热重载（文件监听 + `POST /api/v1/config/reload`）
- **存储**：SQLite（WAL 模式）+ WriteQueue 异步写入 + 增量持久化，NodeRepo / PeerRepo / InfohashRepo / TrackerRepo 四大仓库

## 生态定位

```
用户 / Web 前端
        │
   pnos-runtime（系统级运行时：注册中心 / 服务发现 / 事件总线）
        │
  ┌─────┼──────────────┐
  ▼     ▼              ▼
 pk    pdc           spde
（主控）（节点发现）  （下载执行）
```

pdc 与 pk、spde 并列注册到 pnos-runtime；pk↔pdc 智能层（ICC）统一走 pnos-sdk（决策见 `docs/adr/005-intelligent-control-center.md`）。

## 快速开始

### 环境要求

- Rust 1.75+（建议最新稳定版）
- 本地工作区需与 `pnos-spec/`、`pnos-sdk/` 同级（path 依赖）

### 构建

```bash
cargo build --release
```

### 运行

```bash
./target/release/pdc
# 首次运行自动创建工作目录（config/data/logs）、生成 node_id 与数据库
```

- 监控页：`http://127.0.0.1:6880`（HTTP API + WebSocket 实时监控）
- 更换工作目录：`--work-dir <dir>`；指定配置：`--config <path>`

### 默认端口

| 端口 | 用途 | 配置字段 |
|---|---|---|
| 6880 | HTTP 监控；UDP Tracker 未配置时回退此端口 | `server.port` |
| 6886 | TCP API | `server.api_port` |
| 6881 | DHT 发现器 / 中继（relay） | `discoverers.dht_listen_port` / `super_tracker.relay_port` |
| 6882 | DHT 爬虫监听 | `crawler.listen_port` |
| 6883 | uTP | `crawler.utp_port` |
| 6884 | TCP-PEX | `crawler.tcp_pex_port` |
| 6885 | 联邦同步 | `federation.listen_port` |
| 6771 | LPD 多播 | `discoverers.lpd_multicast_port` |

多 socket 爬虫的端口由 PortAllocator 按百位段整组偏移自动分配（`port_auto_alloc`）。

## 配置

配置文件：`config/config.yaml`，不存在或解析失败时使用代码内默认值（全部字段带 `#[serde(default)]`）。

```rust
PdcConfig::from_file(path)      // 从指定路径加载
PdcConfig::load_or_default()    // 不存在或失败时用默认值
```

**配置热重载**：`config_reload_interval_secs`（默认 30，0=禁用）周期轮询配置文件 mtime，防抖后对比差异分类应用——纯策略/调度类参数即时生效，端口等结构性参数提示重启。也可通过 `POST /api/v1/config/reload` 立即重载。

关键配置段：`server`（端口）、`super_tracker`（UDP Tracker/缓存）、`crawler`（socket 数量、并发、限速阈值、预热）、`task_scheduler`（分类并发、checkpoint 间隔）、`federation`（同步通道开关、并发）、`io_scheduler`（背压采样）。

## 工作目录

遵循生态统一 WorkDir 规范：

```
<root>/
├── config/config.yaml    # 配置文件（缺失自动生成默认配置）
├── data/pdc.db           # SQLite 数据库
├── data/node_id          # 节点身份（十六进制 40 字符，自动生成）
└── logs/                 # stdout.log / stderr.log / crash.log
```

Standalone 模式根目录为 `<work_dir>/pdc-agent/`；设置 `PNOS_APP_ID` 环境变量进入应用商店 Managed 模式。

## 项目结构

```
pdc/
├── src/
│   ├── main.rs            # 入口，TaskScheduler 任务注册中心
│   ├── config.rs          # 全量配置定义（端口/间隔均可配置）
│   ├── lib.rs             # 库入口（crate 名 PeerDiscoveryCenter 为历史遗留）
│   ├── aggregator.rs      # 发现器聚合（并发调度、合并去重）
│   ├── crawler/           # DHT 爬虫（engine、rate_limiter）
│   ├── intelligence/      # 智能层（task_scheduler、adaptive_controller 等）
│   ├── control_plane/     # HTTP API、WebSocket、配置热重载
│   ├── data_plane/        # UDP Tracker、中继
│   ├── discoverers/       # dht / tracker / pex / lpd 发现器
│   ├── federation/        # 多实例同步（Gossip / delta / Range 反熵 / bootstrap）
│   ├── storage/           # NodeRepo / PeerRepo / WriteQueue 等
│   ├── net/、nat/         # socket 选项、NAT、连接管理
│   └── event_bus.rs       # 事件驱动总线
├── config/config.yaml     # 配置文件
├── docs/architecture/     # 架构文档（00-overview ~ 08-intelligent-control-center）
└── docs/adr/              # 架构决策记录（ADR）
```

## 库模式

除 Agent 独立进程外，也可作为库嵌入：

```rust
use PeerDiscoveryCenter::aggregator::{PeerDiscoveryAggregator, PeerDiscoveryConfig};
use PeerDiscoveryCenter::discoverers::DiscovererRegistry;
use PeerDiscoveryCenter::event_bus::EventBus;

let aggregator = PeerDiscoveryAggregator::new(PeerDiscoveryConfig::default());
// 注册发现器并调用 discover_peers(&infohash, max_peers) 查询
```

## 依赖

- `pnos`（path：`../pnos-spec`）— 生态系统级标准库
- `pnos-net`（path：`../pnos-sdk/crates/pnos-net`）— 网络传输层

## 开发指南

```bash
cargo build --release        # Release 构建
cargo test --all             # 全部测试
cargo fmt --all -- --check   # 格式检查
cargo clippy --all-targets -- -D warnings   # 静态分析
```

测试须在 `D:\test\pdc\` 目录下运行，禁止在仓库目录执行（工作目录约束）。

### 合规检查

提交/推送前必须通过生态合规检查（`pandanetos-meta/check-compliance.ps1`，26 项静态检查 + 行为冒烟）：

```powershell
.\check-compliance.ps1 -ProjectPath D:\PNOS\pdc
```

提交信息遵循 Conventional Commits 规范（`feat:` / `fix:` / `docs:` 等）。

## 变更日志

### v0.2.0（当前开发版本）

- 重命名为 pdc，作为节点发现 Agent 注册到 pnos-runtime
- 超级 Tracker（TCP+UDP）、多 socket DHT 爬虫、PEX/LPD 发现器
- 联邦同步：v8 去 Merkle 化（Range 反熵为唯一兜底）→ v9 收敛修复
- 智能层：TaskScheduler 统一提调、IOScheduler、预测式自适应控制器、WAL checkpoint 两层机制
- 性能五轮改造（增量持久化、锁分片、对象池、冷热分层框架）
- 配置热重载完整落地

### v0.1.0（2026-09-02）

- 初始版本：Tracker/DHT/PEX 三合一发现、聚合器、缓存、健康检查

### 规划中

- ICC（智能控制中心）实施（ADR-005）：统一提调资源 / 统一安排任务 / 统一协调模块

## 许可证

MIT - 详见 [LICENSE](LICENSE)。
