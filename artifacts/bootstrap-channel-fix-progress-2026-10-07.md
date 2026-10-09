# bootstrap 通道修复进度报告（批次O，2026-10-07）

> 范围：pdc 联邦同步的 **bootstrap（快照）通道**分块传输。
> 目标：NAK 风暴 / 窗口状态丢失 / 按 index 失效的对齐全部修好，使 `.52 ↔ .53` 的 bootstrap
> 正常健康运行（`done_chunks` 单调、无自激、可观测、自愈），并通过合规门禁。
> 结论：**通道逻辑缺陷已全部修复并验证**（629 单测 / clippy 零告警 / 门禁 21 PASS 0 FAIL）；
> **端到端吞吐仍未连续跑完**，剩余瓶颈已定位到存储/运行时层（见 §5），并已把关键证据与下一步写清。
> 用户已确认「首次收敛无需分钟级」。

---

## 1. 故障形态（修复前，.52 ↔ .53 现网实测）

| 指标 | 值 |
|---|---|
| bootstrap 进度 | 长期停在 `done=9/3108`，`phase=transfer` |
| 28 分钟内块响应 | **143,886 个，其中仅 218 个带数据**（其余全是对端 NAK，空 `entries`） |
| 单块重试 | ~9,000 次 / 28 分钟（≈90 次/秒自激） |
| 应答方 | `bootstrap_send_in_flight` 钉在并发上限 16/16 |
| 请求方 | 读池饥饿（`read_pool_starved` 175+）、写延迟 EWMA 最高 14s、API 15s 超时 |
| 对齐 | 6.02M vs 6.2M 只对上 **8/3108** 块（97% 本端已有数据被判缺失重拉） |

## 2. 根因与修复（O2–O12）

| 编号 | 缺陷 | 证据 | 修复 |
|---|---|---|---|
| **O3** | 窗口状态**并发丢失**：`chunk_windows.remove()` 与 `insert()` 之间夹着落库 `spawn_blocking` 与 `sleep` 两个 await，而 dispatch 对每帧都 `tokio::spawn` ⇒ 并发响应各自「取走→按 DB 重建→回插」互相覆盖 | 同块被反复请求、`done` 前缀倒退 | 改为**单次无 await 临界区内 in-place 变更**（响应处理 + resume tick） |
| **O4/O6** | NAK 语义被当成传输失败**立即重发**；NAK 原因只在 debug 级 | 90 次/秒、单块 9000 次 | 失败块**逐块指数退避**（`defer_retry`）；空 `entries`（显式 NAK）**暂停整个窗口**（`pause_for`，暂停期不推进 `next_request`）；NAK 改**分原因限流 WARN** + `bootstrap_busy_naks`；应答方拿不到发送槽先**限时等 500ms** 再 NAK |
| **O2** | 对齐**按 index 比对**：块边界由各自行数推导 ⇒ 两端行集不同则边界整体错位 | 只对上 8/3108 | 改为**按远端块声明的 `[lo,hi)` 区间**统计本端摘要（单遍归并 + 逐块增量哈希，边界无关）；竣工校验改**区间行数覆盖**；摘要按边界指纹缓存、落块后失效 |
| **O5** | 清单重建**逐块查询**（3108 次）耗时 192.8s > 60s 租约 ⇒ 每 60s 再起一个全表扫描互抢读池 | 应答方持续 NAK `manifest rebuilding`、重建永不完成 | **分批读取 + 内存切块**（5 万行/次，~125 次；与逐块实现**输出等价**有单测）+ **真单飞**（完成才释放，仅超租约夺取并告警），租约 60→300s |
| **O7/O12** | 在途块回收复用发送超时（生产 600s）⇒ 丢帧请求占住窗口槽 10 分钟 | 窗口 8 全满、`inflight` 冻结 | 新增独立 `bootstrap_chunk_wait_secs`（60→**20**）：正常往返亚秒级，该值是丢帧场景的吞吐开关 |
| **O8** | `mark_bootstrap_serving` 在**清单请求**上也打标，而让路截止每次刷新 300s ⇒ 对端每 60s 问一次清单即可把 node_id 较大一侧拉取**永久冻结** | 该侧窗口满、对端 handler 从未被调用 | 只在**分块请求**（真实数据拉取）上打标 |
| **O10** | **延迟 EWMA 闩锁**：`level = max(队列压力, 延迟压力)`，EWMA 只在 writer 每批后更新 ⇒ 慢突发把它抬到十几秒后 level 钉 1.0 → `TaskScheduler 让路` → 写入更少 → 样本更少 → 只能靠重启恢复 | `.52` 全局停摆：`queue_rows=0`、`level=1.0`、`crawl=0/8`/`federation=0/8`、53 任务排队无人在飞、API 15s 超时、多任务 `槽位泄漏 >450s` | EWMA 记录采样时刻并按**每 5s 折半**自行衰减（`io_batch_latency_ewma_us_fresh` + 纯函数 `decay_ewma_us` 单测）；真实突发仍被新样本立刻抬高 |
| **O11** | **帧解码失败静默丢弃**：20 处 `if let Ok(msg) = bincode::deserialize(...)` 无 else 分支 | 排查无法区分「没发」与「解码失败」 | 4 个 bootstrap 分支补 `Err` → 计数 + 限流 WARN；`/io/status` 暴露 `federation_frame_decode_errors` |

**新增/变更配置**（全部 `#[serde(default)]`，向后兼容）

| 字段 | 默认值 | 说明 |
|---|---|---|
| `federation.bootstrap_window_size` | 16→**8** | 请求方并发窗口；应 ≤ 应答方并发的一半 |
| `federation.bootstrap_send_concurrency` | 4→**16** | 应答方同时在途发送数（旧配比 16 窗口 vs 4 并发必然打满） |
| `federation.bootstrap_send_permit_wait_ms` | 500 | 拿不到发送槽的等待时长（排队在加载 entries 之前，内存上界不变） |
| `federation.bootstrap_nak_log_interval_secs` | 30 | NAK 原因限流日志间隔 |
| `federation.bootstrap_rebuild_lease_secs` | 60→**300** | 清单重建单飞租约（必须显著大于一次全表重建耗时） |
| `federation.bootstrap_chunk_wait_secs` | 60→**20** | 在途块回收超时（与请求发送超时解耦） |

**观测新增**：`/api/v1/io/status` → `bootstrap_busy_naks`、`federation_frame_decode_errors`。

## 3. 验证

| 项 | 结果 |
|---|---|
| `cargo test --all` | **629 passed / 0 failed / 1 ignored** |
| 新增单测 | 分批切块与逐块实现**等价**、按区间对齐优于按 index、区间覆盖拒假竣工、边界指纹、摘要缓存、**延迟 EWMA 时间衰减** |
| `cargo clippy --all-targets -- -D warnings` | 零告警 |
| `cargo fmt --all -- --check` | 干净 |
| `cargo build --release` | 成功 |
| `check-compliance.ps1 -ProjectPath D:\PNOS\pdc` | **21 PASS / 0 FAIL / 5 WARN**（WARN = 6/9/10/11/13 既有启发式项，与改动前一致） |

**现网验证（.52 请求方 / .53 应答方，双端均为本轮最终二进制）**

- NAK 风暴消失：`bootstrap_busy_naks=0`、`bootstrap_send_failures_total=0`；NAK 变为「窗口暂停后重试」。
- 清单重建**单飞**且每个进程生命周期内**只做一次**，耗时 150–195s（分批切块前为分钟级且会被重复触发）。
- `federation_frame_decode_errors = 0`、事件通道 `Lagged = 0` ⇒ **排除**「解码失败」与「事件通道丢帧」两条假设。
- EWMA 闩锁修复生效：`.52` 突发后 `latency_ewma_ms` 由 14,182 回落到 **10→0**、`read_pool_starved: 0`，节点不再需要重启解锁。
- `done_chunks` 单调推进：9 → 40 → 51 → 62 → 70（跨多次重启不回退）。
- 吞吐量级（受限于 §5）：良性窗口 **8 块/83s ≈ 5.8 块/min**；回收超时 60→20s 后 `served` 速率约 **5.3 块/min**（此前 ~2 块/min）。

## 4. 运维发现（现网）

1. **WAL 文件长度膨胀**：`.52` 的 `pdc.db-wal` 曾达 **11.8GB**（DB 本体 1.19GB）。离线 `wal_checkpoint(TRUNCATE)` 得 `0|0|0` ⇒ **待检查点帧为 0**，纯属「PASSIVE 稳态检查点从不截断 + 每小时 TRUNCATE 被 Persistence 槽饥饿」造成的长度膨胀，不是积压数据。停节点后删除 WAL 安全，立即回收 11.8GB；`quick_check: ok`。
2. **全局停摆可自愈性**：`.52` 出现过 `crawl=0/8`/`federation=0/8`、53 任务排队无人在飞、API 15s 超时、多任务 `槽位泄漏 >450s`；根因即 O10 的节流闩锁，修复后不再需要重启。
3. **配置文件编码是硬约束**：`config.yaml` 必须为 **UTF-8**。用 PowerShell `Set-Content`（默认 ANSI/GBK）编辑后，中文注释被写成非 UTF-8 字节 → `解析配置文件失败: stream did not contain valid UTF-8` → **整份配置静默退回默认值**（`.53` 因此用默认 `chunk_rows=20000` 起了 312 块清单、单块 ~1.4MB）。本轮已把两端配置重写为纯 ASCII 的干净 UTF-8 配置并验证「已从 … 加载配置」。**后续改配置必须用 `-Encoding UTF8` 或 .NET `UTF8Encoding($false)` 写入。**

## 5. 剩余瓶颈（下一轮）

**现象**：分块传输呈「一阵一停」，能推进但不连续；应答方 `bootstrap_send_total` 增长的同时，请求方 `sync_entries_applied` 只增长约 1/3 ~ 1/6 ⇒ **部分分块响应在链路上消失**，每个丢失窗口要等回收超时才重发。

**已排除**：解码失败（O11 计数恒 0）、事件通道 Lag（`Lagged=0`）、应答方发送失败/超时（计数 0）、NAK 语义（NAK 均有原因日志）、窗口状态丢失（O3 已修）、对齐失效（O2 已修）。

**指向**：请求方在本端 DB 重活期间**读循环/事件消费被饿死**（伴随 `read_pool_starved` 上升、写延迟 500–1000ms、checkpoint 单次处理数万帧），成批丢帧与该窗口高度相关 —— 即 AGENTS v9 遗留「全局单连接 + 长 PRAGMA/检查点与只读查询争用」那一类问题，属存储/运行时层，不在 bootstrap 逻辑内。

**建议优先级**
1. **关键通道不被 IO 背压让路**：`fed_bootstrap_resume` / `fed_delta_sync` 在 `TaskScheduler 让路` 期间仍应保持保底节奏（当前 `level>0.5` 即让路，直接决定重发窗口何时补满）。
2. **清单缓存持久化**：应答方 `bootstrap_manifests` 是纯内存，重启即失 ⇒ 每次重启付 150–195s 重建。落库（或按廉价指纹缓存）可消除该固定成本。
3. **存储层读写分离与检查点硬超时**（v9 遗留 #1）：解除「写延迟高 → 读循环饿死 → 成批丢帧」链条。
4. 若后续仍要求首次收敛更快，再上「采样边界 + 空 hash 的快速清单模式」（当前不需要）。

## 6. 影响面与兼容性

- **未改线网格式**（`BootstrapChunk*` / `BootstrapManifest*` 字段与编号不变），两端可分别升级；服务端收益（单飞重建、区间对齐、显式 NAK 语义）需同版本。
- 变更文件：`src/federation/sync/bootstrap.rs`、`src/federation/sync/mod.rs`、`src/federation/config.rs`、`src/federation/dispatch.rs`、`src/storage/io_scheduler.rs`、`AGENTS.md`。
- 部署物：`D:\test\pdc\pdc.exe`（.52）与 `\\192.168.30.53\D$\test\pdc\pdc.exe`（.53），备份 `pdc.exe.bak-20261007-preO2`（.52）；配置已重写为纯 ASCII UTF-8。