# pdc AGENTS.md

> 本文件是 AI 代理进入 pdc 仓库时的首读指南。
> 生态级全局约束请参考 [根目录 AGENTS.md](../AGENTS.md)。

## 仓库定位

pdc（Peer Discovery Center）是 PandaNetOS 生态的**节点发现 Agent**，Agent 级独立进程，注册到 pnos-runtime。负责 DHT 爬虫、超级 Tracker、PEX、联邦同步等节点发现能力。

## 架构概览

```
pdc/
├── 控制层 (control_plane)   # HTTP API、WebSocket 监控、策略引擎
├── 数据层 (data_plane)      # UDP Tracker、中继、超级 Tracker
├── 智能层 (intelligence)    # TaskScheduler、自适应控制器、评分引擎、ICC
├── 发现层 (discoverers)     # DHT、Tracker、PEX、LPD 发现器
├── 爬虫层 (crawler)         # DHT 爬虫（8 socket 多并发）、TrackerFetcher
├── 存储层 (storage)         # NodeRepo、PeerRepo、InfohashRepo、WriteQueue
├── 联邦层 (federation)      # 多实例同步、Gossip、Merkle 同步
└── 网络层 (net)             # socket_opts、连接管理
```

核心数据流：爬虫发现节点 → NodeRepo → 评分引擎 → 选择高质量节点 → 继续爬行

## 目录结构

```
pdc/
├── src/
│   ├── main.rs                  # 入口，TaskScheduler 注册中心
│   ├── config.rs                # 配置（所有端口/间隔可配置）
│   ├── crawler/                 # DHT 爬虫（engine、rate_limiter）
│   ├── intelligence/            # 智能层（task_scheduler、adaptive_controller、crawler_history）
│   ├── storage/                 # 存储（node_repo、peer_repo、write_queue）
│   ├── data_plane/              # 数据层（udp_tracker、relay）
│   ├── control_plane/           # 控制层（HTTP API、WebSocket）
│   ├── discoverers/             # 发现器（dht、tracker、pex、lpd）
│   ├── federation/              # 联邦同步
│   └── net/                     # 网络工具（socket_opts）
├── config/                      # 配置文件
├── docs/architecture/           # 架构文档（00-08）
├── docs/adr/                    # 架构决策记录
└── Cargo.toml
```

## 构建与测试

| 命令 | 说明 |
|---|---|
| `cargo build --release` | Release 构建（约 2-3 分钟） |
| `cargo test --all` | 运行所有测试（300+ 用例） |
| `cargo fmt --all -- --check` | 格式检查 |
| `cargo clippy --all-targets -- -D warnings` | 静态分析 |

## 关键配置

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `server.port` | 6886 | HTTP API/监控端口 |
| `super_tracker.udp_port` | 6880 | UDP 超级 Tracker |
| `discoverers.dht_listen_port` | 6881 | DHT 发现器 |
| `crawler.socket_count` | 1（上限10） | 爬虫 socket 数量 |
| `crawler.concurrent_sockets` | 4 | 每轮并发 socket 数 |
| `crawler.listen_port` | 6802-6892 | 爬虫 socket 端口范围（PortAllocator 分配） |
| `crawler.utp_port` | 6883 | uTP |
| `crawler.tcp_pex_port` | 6884 | TCP-PEX |
| `federation.listen_port` | 6885 | 联邦 |
| `discoverers.lpd_multicast_port` | 6771 | LPD 多播 |

## 依赖关系

- **依赖**：`pnos`（path，pnos-spec）、`pnos-net`（path，pnos-sdk）
- **注意**：Cargo.toml 中 crate 名仍为 `PeerDiscoveryCenter`（历史遗留）
- **被依赖**：pk（通过 pnos-runtime 间接调用）

## 注意事项

1. **TaskScheduler 提调一切**：所有周期性任务必须注册到 TaskScheduler，禁止模块内部自跑定时
2. **8 Socket 架构**：爬虫支持多 socket，tid 高 4 位编码 socket 索引
3. **预测式自适应**：SGD 模型预测响应率，特征归一化+梯度裁剪
4. **测试目录**：测试必须在 `D:\test\pdc` 运行，禁止在仓库目录执行
5. **编译环境**：飞牛服务器 192.168.30.35:2222 Docker 容器
6. **远程节点**：192.168.30.51 的数据不允许删除

## 变更历史

| 日期 | 版本 | 变更内容 |
|---|---|---|
| 2026-09-16 | v1.0 | 初始版本，记录 8 socket、预测式自适应、TaskScheduler 纳管 |
