# 爬虫发送链路重构方案：流式发送 + 信号去噪 + tid 扩宽 + 可观测性

> 状态：**已实施（2026-10-05）**——批次A `d254c54`（信号去噪收尾：丢包窗口化+keepalive分母）、批次B `269b3e2`（tid 4字节）、批次C `86a5344`（paced 流式发送默认启用）、批次D `5e5a970`（/metrics 接线+新节点计数+调度器指标），全部过合规门禁。
> 实施差异说明：D1 主体（claim_pending 收敛计数、真实轮次反馈）已由 4714129 与 G2 先行落地；本批补齐其确认与收尾。D2/D3/D4 按本文档实施。
> 关联：`06-performance.md`、`pnos-test-suite/docs/09-observability.md`、根 AGENTS.md 性能目标

## 背景

运行实测（2026-10-01，三节点联邦环境）发现爬虫有效速率被自适应限速长期压在正常量的 20%–76%，/metrics 导出器全零，uptime 恒为 0，且不存在「新节点发现速率」这一核心业务指标。本方案一次性修复发送架构、限速信号质量和可观测性三类问题。

## 根因清单（含代码证据）

### R1 响应率信号被 4 处污染

自适应限速的输入信号（响应率）失真，导致倍率长期趴在 0.2–0.76 区间、无法安全上浮：

| # | 问题 | 位置 |
|---|---|---|
| 1 | 对**每个入站数据报**（含其他节点的未请求 ping/find_node/get_peers）都调 `record_response`，响应率实为「入包率」 | `engine.rs` `handle_response_sync` 顶部 |
| 2 | `estimate_responded(sent) = sent × global_response_rate()` —— 控制器的反馈输入是限速器自身的窗口，**循环自馈**，从未统计真实应答 | `engine.rs` `estimate_responded` |
| 3 | `send_sample_to_socket` / `send_sample_infohashes_concurrent` / `active_keepalive` 从不 `record_request`，但其响应照计 → 分母缺失，进一步虚高 | `engine.rs` 各发送函数 |
| 4 | 多 socket 并发路径直接按 `send_socket_idx` 轮转选 socket，**完全绕过 `RateLimiter::should_skip`**；限速仅在单 socket 路径的 `next_send_socket()` 生效 | `engine.rs` 三个 `send_*_concurrent` |

### R2 轮次突发无 pacing

4 个规划任务（active_crawl 30s / get_peers 10s / sample_infohashes 15s / scrape 60s）在轮内以紧循环 `send_to().await` 打包：get_peers 满载一轮 128 节点 × 5 查询 = 640 数据报瞬间发出。响应率 60s 窗口随之锯齿震荡（实测单 socket 在 1.0 与 0.2 之间摆动），控制器误判「响应恶化」降倍率，随后又恢复——持续 hunting。

### R3 事务 ID 只有 2 字节

tid 高 4 位编码 socket 索引，剩余 12 位随机 → **单 socket 在飞请求上限 4,096**（15s 超时窗口内理论上限 ~273 pps，且碰撞概率随并发上升）。tid 分片按 `tid[0] % 16` 路由。

### R4 /metrics 全零

`data_plane/metrics.rs` 注册了 10 个 Prometheus 指标（`pdc_tracker_*`、`pdc_dht_*`、`pdc_cache_*`、`pdc_health_score` 等），但**全仓库没有任何一处 `.inc()` / `.set()` 调用**——注册与更新之间完全断线。而 1 秒粒度的 `StatsSnapshot`（`spawn_snapshot_updater`）里所有数据都是活的。

### R5 uptime 恒为 0

`rest_api.rs` `health_handler`（:438）与 `crawler_handler` 降级分支（:744）硬编码 `uptime_seconds: 0`；`AppState` 无进程启动时间字段。

### R6 新节点发现速率无指标

`NodeRepoImpl::add_nodes_batch_internal` 已区分「真正新增」的 `new_pairs`，但返回值被所有调用方丢弃（engine.rs、dht/probe.rs、federation/sync）。另：`task_scheduler_metrics` 是 `Option<()>` 死占位恒 None；`udp_packet_loss_estimate` 用生命周期累计值而非 60s 窗口（与字段注释相反）；`pause_gate` 注入 engine 后从未被读取。

## 设计

### D1 响应率信号去噪（修复 R1 + R6 的丢失面）

1. **tid 匹配才计响应**：`handle_response_sync` 重排为「先解析 → 分派」：
   - 解析为响应且 tid 命中 pending 表 → `record_response` + 从 pending 移除 + 轮次反馈计数
   - 解析为响应但 tid 未命中（超时后迟到/伪造）→ 新计数器 `late_responses_total`
   - 解析为查询（其他节点请求我们）→ 新计数器 `inbound_queries_total`，**不进入响应率**
2. **补齐请求分母**：sample 全部路径与 keepalive 成功发送后 `record_request(socket_idx)`
3. **多 socket 路径接限速**：三个 `send_*_concurrent` 每包发送前检查 `should_skip(socket_idx)`，跳过则不发送不计数
4. **真实轮次反馈**：`PendingRequest` 增加 `round_seq: u64`；engine 持有 `round_seq: AtomicU64` 与 `round_feedback: Mutex<BTreeMap<u64, RoundFeedback>>`（`RoundFeedback { sent, responded, timed_out }`，报告后即删，>60s 兜底清理）。tid 命中时累加 responded，`cleanup_pending` 超时时累加 timed_out。`report_round_result` 改读真实计数，删除 `estimate_responded`；`AdaptiveController` 对外签名不变，但输入从自馈变为真实值
5. **丢包估计窗口化**：`udp_packet_loss_estimate` 改为限速器 60s 窗口未应答率（1 − 全局响应率），文档同步修正

### D2 tid 扩宽至 4 字节（修复 R3）

- `discoverers/dht/message.rs` 全部 builder/parse 的事务 ID 签名 `&[u8; 2]` → `&[u8; 4]`（手写 bencode 的 `1:t<len>` 长度前缀同步），`parse_find_node_response` 等返回类型跟随
- tid 生成：`[socket_idx, rand, rand, rand]`（字节 0 = socket 索引，支持 256 socket；24 位随机 → 单 socket 在飞空间 ~16.7M）
- 分片路由：`pending_shard` 改为 `tid[3] % 16`（末字节随机，均匀分布；原 `tid[0]` 现在是 socket 索引，会造成分片偏斜）
- pending 表键本就是 `Vec<u8>`，无结构改动
- 兼容性：BEP-5 规定 `t` 为任意长度字节串，主流客户端（libtorrent/qBittorrent/Transmission）原样回显，4 字节合法

### D3 流式平滑发送（修复 R2），默认启用

**架构选型**：引擎自有 tokio 任务（与 `recv_loop` 同级在 `CrawlerEngine::start()` spawn），**不经 TaskScheduler**——调度器对每次执行有 300s 硬超时且占用 Crawl 并发槽，常驻任务会被周期性杀死。这与 `crawl_loop`/`recv_loop` 的既有先例一致。

**生产者**：4 个规划任务保留现有节奏与选址逻辑（`select_diverse_nodes`、infohash 池、`64 × 倍率` 目标数），但发送尾部改为**入队**：

```rust
enum SendWorkItem {
    FindNode { addr, target },
    GetPeers { addr, infohash },
    Sample { addr },
    Scrape { addr, infohash },
}
// tokio::sync::mpsc::channel(8192)，满了 try_send 失败 → enqueue_dropped_total 自增
```

**消费者**（`paced_send_loop`）：每 tick（默认 250ms）：

```
预算 = paced_max_per_socket_per_tick × 活跃socket数 × 自适应倍率   （min 1）
循环出队：next_send_socket() 轮询选 socket → should_skip 检查 →
  构建 tid(4B) → pending 注册(带 round_seq) → send_to → record_request → 计数器
pause_gate 置位时跳过出队（堆积由 8192 队列容量兜底）
```

**保持直发**（不入队）：`bootstrap()`、`chain_crawl()`（响应驱动、每链 8 个、延迟敏感、量小）。

**配置**（全部 `#[serde(default)]`，旧 config.yaml 免改加载）：

| 字段 | 默认 | 说明 |
|---|---|---|
| `crawler.send_mode` | `paced` | `paced` / `round`，round 为行为逃生通道（启动时生效） |
| `crawler.paced_tick_ms` | 250 | 消费 tick 间隔 |
| `crawler.paced_max_per_socket_per_tick` | 8 | 每 socket 每 tick 配额（全局天花板 ≈ 8×8×4/s = 256 pps） |

新增运行指标：`send_queue_len`、`enqueue_dropped_total`、`paced_mode`（进 CrawlerState → /api/v1/stats）。

### D4 可观测性补课（修复 R4/R5/R6）

1. **/metrics 接线**：`spawn_snapshot_updater` 每个 tick 末尾调用 `metrics::update_from_snapshot(&snap, &scheduler_metrics)`。gauge 直接 `.set()`；counter 维护「上次原始值」表做增量 `inc_by()`。接线既有 10 个指标，并按生态规范 09 新增规范命名族：
   - `pdc_crawler_socket_send_pps{socket_idx}` / `pdc_crawler_socket_recv_pps{socket_idx}` / `pdc_crawler_socket_response_rate{socket_idx}`
   - `pdc_crawler_adaptive_multiplier` / `pdc_crawler_predicted_response_rate` / `pdc_crawler_pending_total` / `pdc_crawler_udp_packet_loss` / `pdc_crawler_concurrent_sockets_in_use`
   - `pdc_node_discovered_total`（新节点计数器）
   - `repo_total_count{repo}` / `repo_dirty_count{repo}` / `repo_subnet_count{repo}`
   - `pdc_scheduler_queue_len` / `pdc_scheduler_running_tasks{category}` / `pdc_scheduler_task_executions_total{task}`
2. **uptime**：`AppState` 增加 `started_at: Instant`（构造时赋值），`health_handler` 与 `crawler_handler` 降级分支改用真实值
3. **新节点计数**：`NodeRepoImpl` 增加 `new_nodes_total: AtomicU64`，在 `add_nodes_batch_internal` 的 `new_pairs` 分支与 `add_node_sync` 的 `is_new` 分支自增（单一咽喉点，天然覆盖爬虫/探测/联邦三条路径；启动加载 `load_initial` 不经过此路径，不会灌水）；经 `NodeRepoMetrics` 进快照，Prometheus 端 `rate(pdc_node_discovered_total[1m])` 即每分钟发现速率
4. **调度器指标**：`TaskScheduler::metrics_snapshot()` 返回队列长度、各分类运行数、累计执行次数、被延迟计数；替换 `StatsSnapshotData.task_scheduler_metrics` 的 `Option<()>` 死占位

## 测试计划

新增约 10 个单测（遵循既有 in-module `#[cfg(test)]` 惯例）：

| 测试 | 验证点 |
|---|---|
| tid 4 字节往返 | build → parse → tid 一致；socket_idx 从字节 0 还原 |
| 分片均匀性 | `pending_shard(tid[3]%16)` 随机 tid 均匀落 16 片 |
| 轮次反馈计数 | 构造真实 bencode 响应喂 `handle_response_sync` → 对应 round_seq 的 responded +1 |
| 未命中不计响应 | 无 pending 的响应/查询报文 → late_responses/inbound_queries 计数、响应率不变 |
| sample 计请求 | 127.0.0.1 临时 UDP 对跑 `send_sample_to_socket` → 限速器请求数 +1 |
| 预算数学 | 倍率 0.2 × max 8 → 每 tick 全局配额正确取整 |
| 队列溢出 | 塞满 8192 后 enqueue 计 dropped |
| pause_gate | 置位后消费者不出队 |
| metrics 输出 | update_from_snapshot 后 render 包含非零值 |
| 调度器快照 | metrics_snapshot 字段正确 |

既有 589 测试必须全绿；`rate_limiter.rs` 本体语义不变（13 个既有测试原样通过）。

## 提交序列（每个独立过合规门禁）

1. `docs: 流式发送与信号修复设计方案（19号文档）` ← 本文档
2. `fix(crawler): 响应率信号去噪`
3. `feat(crawler): tid扩宽至4字节`
4. `feat(crawler): 流式平滑发送（默认启用，round逃生）`
5. `feat(observability): /metrics接线与指标补全`
6. `docs: 配置表与实施记录同步`

## 验证步骤

1. 合规全绿：fmt / clippy / test（589+ 新增）/ release build
2. 部署 `D:\test\pdc` 重启本地实例，观察 30 分钟：
   - pps 平滑无锯齿（socket_send_pps 方差显著下降）
   - socket_response_rates 稳定（不再 1.0↔0.2 锯齿）
   - 自适应倍率从 0.2–0.76 自然上浮（**这是修复生效的信号，不是回归**）
   - /metrics 非零、uptime_seconds 正确、`rate(pdc_node_discovered_total)` 滚动
3. 达标后推送到远端（走门禁钩子）

## 回滚方案

- 行为级：`crawler.send_mode = "round"` 一键回到轮次模式（信号去噪与 tid 扩宽仍生效，两者对 round 模式同样有益且向后兼容）
- 版本级：git revert 对应提交；tid 扩宽提交回滚后，旧 2 字节 tid 与新 pending 表结构兼容（键均为字节串）
