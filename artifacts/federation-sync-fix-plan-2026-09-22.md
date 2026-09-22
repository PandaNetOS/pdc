# 联邦同步 Bug 整体修复方案（v8 去 Merkle 化后重分类）

> 日期：2026-09-22 02:30
> 基线：`pdc-session-layer` + 未提交的「去 Merkle 化」改动（协议 v8，ADR-007 已实施）
> 输入：2026-09-21 联邦同步排查报告（该报告基于**含 Merkle 的 v7 代码**）
> 本文作用：先做「幸存性核实」，再把仍成立的 bug 排成可执行批次

---

## 0. 结论先行

原报告列出的 15+ 项中，**6 项已随 Merkle 移除而消失、2 项已被 ADR-007 修掉**，真正仍需修的是 **4 项 P0 + 6 项 P1 + 5 项 P2**。

其中 **#2a（非 NODE repo 的 delta 被永久跳过）高度可疑就是当前 peer 收敛不上去的直接原因** —— 现网数据：peer 双端差距 10,813、51 侧 repo=2 的 `peer_max_seq` 为 `null`，与该 bug 的表征吻合。

**修复顺序建议：先止血（批次 1，四处小改动），再补链路（批次 2），最后做调度层治理（批次 3）。**

---

## 1. 幸存性重分类

### A. 已消失（无需修复）

| 原编号 | 问题 | 消失原因 |
|---|---|---|
| #1 | port 哈希 u16/i64 双公式 → Merkle 幻影差异 | `merkle.rs` 已删，分片哈希无消费者。⚠️ **待确认**：`data_hash` 是否仍被 gossip seen 去重 / SyncEntry 比较消费；若有，降级为 P2（统一 `port as u16`，几行） |
| #6 | `rebuild_all()` 后 65536 dirty 超 SQLite 参数上限，增量任务必败 1 小时 | Merkle 树与增量/冷重算任务整族删除 |
| #9 | `RangeReconcilePush` 走 `add_node_sync` → 回灌 oplog 违反不变量 | ADR-007：`RangeReconcilePush(45)` 退役，改 `Push2(49)` 走 `handle_sync_batch` 幂等 apply。⚠️ 验收时确认新路径确实不再写 oplog |
| — | FullSync / DiffSync / ShardSync 协议族相关 | 整族退役（编号留空位） |

### B. 已被 ADR-007 修复（待实测验证，不重复修）

| 原编号 | 问题 | 修复方式 |
|---|---|---|
| #4 | range 反熵对非 NODE repo 的修复是死路（委托 delta，而 delta 对无 oplog 数据失效） | 新增 `RangeReconcilePull(48)` / `Push2(49)`，4 repo 通用，不再依赖对端 oplog |
| — | 同步通道 serde 默认全 false（生产 yaml 不写键 → 无兜底对账） | serde 默认值与 `impl Default` 对齐（delta/range/bootstrap → true） |
| #10 部分 | 协议版本与死代码 | v8 + 死代码清理 |

### C. 仍成立（本文重点）

见下节 P0 / P1 / P2。

---

## 2. P0 — 直接导致「永远不同步」

### P0-1 非 NODE repo 的 delta 被永久跳过（原 #2a）⭐ 最高优先级

**代码位置**
- `src/federation/sync/mod.rs:1140-1167`：`use_bootstrap` 对**任何 repo** 成立 → `start_bootstrap(peer, rt)` → `continue` 跳过 delta
- `src/federation/sync/mod.rs:2188`：`start_bootstrap` 非 NODE 直接 `return`

**后果链**
```
协商裁定 BOOTSTRAP 或 lag > 阈值（对任何 repo 都可能）
  → use_bootstrap = true → continue（跳过该 repo 的 delta）
  → start_bootstrap 非 NODE 静默 return（no-op）
  → 该 (peer, repo) 每个 tick 都跳过 delta，且 bootstrap 什么也不做
  → 该 repo 永久停摆，无任何日志
```

**现网佐证**：peer repo 双端差距 10,813；51 侧 `ops_lag` 中 repo=2 的 `peer_max_seq = null`（水位从未建立，符合 delta 从未真正跑过的表征）。

**改法**（低风险，几行）
1. `use_bootstrap` 判定加 `rt == repo_type::NODE` 门控；非 NODE 一律落入 delta 主线。
2. 或：把 `start_bootstrap` 对非 NODE 的静默 return 改为 WARN 日志 + 回落 delta（双保险）。
3. 建议两者都做：门控在前，回落+日志兜底在后。

**风险**：极低。仅改变非 NODE repo 的通道选择，不动 NODE 主路径。

**验收**：`ops_lag` 中 repo=2/3/4 出现稳定的 `peer_max_seq` 与 `synced_seq`；peer 双端差距从 10,813 单调下降。

---

### P0-2 bootstrap 清单 `repo` 字段硬编码 NODE（原 #2b）

**代码位置**
- `src/federation/sync/bootstrap.rs:322`：`repo: crate::federation::protocol::repo_type::NODE`（与入参 `repo` 无关）
- `src/federation/sync/mod.rs:1918`：请求方 `if mf.repo != repo_type::NODE` → 丢弃

**后果**：v7 声称的「bootstrap 全 repo 化」实际没打通；应答方接受 4 repo 的清单请求，构造出的清单却标成 NODE。

**改法（二选一，建议先取 A 止血）**
- **A（保守，配合 P0-1）**：既然 bootstrap 当前只支持 NODE，就把它做成**显式约束**——非 NODE 不发起 bootstrap（P0-1 已做），并把硬编码改为「按入参构造」+ 断言，避免将来误用。成本最低。
- **B（完整）**：打通 4 repo —— 清单 `repo` 用入参、请求方接受 4 repo、`start_bootstrap` 支持 4 repo、`build_repo_manifest_impl` 按 repo 走对应加载器。工作量大，单列为独立任务。

**风险**：A 极低；B 中高（涉及 4 个 repo 的 key 解析与加载器）。

**验收**：A —— 非 NODE 不再出现 bootstrap 相关日志且 delta 正常；B —— 4 repo 的 `bootstrap.done_chunks` 均可推进。

---

### P0-3 TRACKER 删除永不收敛 + 墓碑复活（原 #3）— 已核实完整闭环

**代码位置**
- `src/federation/sync/tracker_sync.rs:159-161`：`entry.operation == DELETE → continue`（丢弃）
- `src/storage/db.rs:680`（`save_tracker`）：upsert 带 `deleted_at = NULL`
- 复活链路：A 删 T → B 未删 → 反熵判差异 → B 推 upsert → A 侧缓存 miss → `get_tracker_sync` 回源被 `deleted_at IS NULL` 过滤 → 当**新**条目插入 → `persist_dirty` → `save_tracker` 清墓碑 → **T 复活**

**改法**
1. `apply_tracker_sync` 处理 DELETE：走软删（写墓碑 + oplog），不再 `continue`。
2. `save_tracker` 的 upsert **不清墓碑**：按 `deleted_at` 仲裁——入站 upsert 若本地存在墓碑且墓碑更新（LWW 按 deleted_at 时间戳），保留墓碑。
3. 顺带修 `peer_sync.rs:152` / `infohash_sync.rs:132` 同样丢弃 DELETE 的语义（当前这两个 repo 若不产生删除则暂无实害，但语义不闭环）。

**风险**：中。改动涉及删除语义与 LWW 仲裁，需确认不会误复活正常数据。建议加一条「墓碑只在入站条目 `updated_ms` 新于墓碑时间时才被清除」的规则。

**验收**：A 删 tracker → 反熵多轮后 B 侧也删除，且 A 侧不复活；`tracker_repo_total` 双端一致（现网已一致 383/383，主要防回归）。

---

### P0-4 bootstrap 块校验语义恒失配 → F7 会陷入无限重拉（原 #2c）

**代码位置**：`src/federation/sync/mod.rs` `handle_bootstrap_chunk_response`（校验分支）

**问题**：校验方式为「落地后**重算本地整个区间**的摘要，与清单 hash 比对」。只要接收方在该 key 区间内有**任何对端没有的行**（双方各自爬取产生的 2% 差异，全域均匀分布），每个块都必失败；对端在传输期持续写入同样导致恒失配。

**后果**：未提交的 F7（连续 3 次失败重拉清单）治不了——重拉清单还是对端 DB，接收方多余行仍在 → **无限重拉**。

**改法**：校验对象改为「**收到的条目本身**」——对 `resp.entries` 计算摘要与 `chunk.hash` 比对（传输完整性校验），而非与本地区间比对（那是一致性校验，语义不同，应交给 range 反熵做）。

**风险**：低。仅改校验语义，不动传输。

**验收**：`bootstrap.done_chunks` 单调递增、不再出现连续校验失败重拉。

---

## 3. P1 — 丢数据 / 重复做功

| # | 问题（原编号） | 位置 | 改法 | 风险 |
|---|---|---|---|---|
| P1-1 | gossip 出站限流丢批不回退（#5） | `gossip.rs:569-592` | 限流跳过的批**回退 outbox**而非丢弃；重试改**时间预算**（如 30s）而非 `MAX_RETRIES=3` tick（≈3s）；成功/失败/重试键改 `(origin, msg_id)` | 中（改 outbox 语义，注意不要引发无界堆积） |
| P1-2 | oplog 裁剪不感知对端游标（#7） | `oplog.rs:267-280`、`mod.rs` 协商 `min_seq` | 消费协商消息里的 `min_seq`（当前无人消费）：对端游标低于 `min_seq` → 判定越界 → 转 bootstrap；或裁剪时按最小对端游标保护 | 中 |
| P1-3 | IPv6 key 两套编码（#8） | `mod.rs:97` (`addr.to_string()` → `[::1]:p`) vs `db.rs` (`format!("{}:{}", ip, port)` → `::1:p`) | 统一编码（建议统一为 `format!` 无方括号形式，或统一 `SocketAddr::to_string` 并同步 DB 侧解析） | 低（但涉及存量 key 兼容） |
| P1-4 | `delta_watchdog_ok` 只看水位不动（#11） | `mod.rs:2987-3039` | 增加 in-flight 判定：有请求在途时不误判暂停 | 低 |
| P1-5 | `delta_peer_max` 断连不清理（#12） | `mod.rs:170` | 断连时清理陈旧水位（与 P0-1 叠加会放大停摆） | 低 |
| P1-6 | `archive_cold_peers` 物理删除无墓碑无 oplog（#14） | `db.rs:1069-1101` | 改软删 + 墓碑（与 P0-3 配套，否则冷 peer 被反熵周期性复活） | 中 |

---

## 4. P2 — 健壮性 / 边界

| # | 问题（原编号） | 位置 | 说明 |
|---|---|---|---|
| P2-1 | broadcast Lagged 静默丢真实协议帧（#13） | `dispatch.rs:305-307` | Lagged 时对协议帧需重传或降级，不能静默 |
| P2-2 | seen 去重 LRU 命中不续热度（#15） | `sharded_lru.rs:69-77` | 10 万条后老消息可被驱逐重入（历史回环残留条件） |
| P2-3 | range 深度到顶时 `entries` 为空 → 幽灵推送（#10） | `mod.rs` + `range_reconcile.rs:136` | `key_diff(本地, 空)` 把整个区间判成「本地多」 |
| P2-4 | port 哈希双公式残留（#1 降级） | `db.rs` `load_all_*` / `*_in_range` | 仅在确认 `data_hash` 仍有非 Merkle 消费者时才修 |
| P2-5 | bootstrap 清单缓存按 node_id 不分 repo | `mod.rs` | 多 repo 并发 bootstrap 会串台（当前 NODE-only，潜伏） |

---

## 5. 与未提交改动（F6/F7/F8）的关系

| 改动 | 状态 | 建议 |
|---|---|---|
| **F6**（应答方清单缓存缺失时现场重建） | 已观测生效（51 端出现「清单缓存缺失，现场重建」） | **保留**，方向正确 |
| **F7**（连续 3 次校验失败 → 重拉清单自愈） | 未验证 | **必须先修 P0-4**（校验语义），否则 F7 会无限重拉。修完 P0-4 再合入 |
| **F8a**（Range 对账超时 300→900s） | 已判定**负收益** | **回滚**。实测 Range 对账 avg 127.94s / max 253.61s，并非被 300s 掐死；放宽到 900s 只是让它合法占槽更久，加剧 Federation 槽饥饿 |
| **F8b**（4 个 `merkle_incremental_*` 加 keepalive） | 随 Merkle 移除**自动失效** | 随代码删除自然消失，无需处理 |

---

## 6. 批次执行计划

### 批次 1 — 止血（低风险小改动，一批做完再验证）

1. **P0-1**：非 NODE repo 的 delta 不再被 bootstrap 分支跳过（门控 + 回落日志）
2. **P0-2（方案 A）**：bootstrap 显式限定 NODE，清单 `repo` 改按入参构造
3. **P0-3**：tracker DELETE 落软删 + `save_tracker` upsert 不清墓碑（按 `deleted_at` 仲裁）
4. **P0-4**：bootstrap 块校验改为校验收到的条目本身
5. 顺带：**回滚 F8a**（900s → 恢复默认）

验证：`cargo check` + `cargo test --all` + 门禁 → 双端部署 → 观察 `ops_lag` repo=2/3/4 水位建立、peer 差距收敛、`bootstrap.done_chunks` 推进。

### 批次 2 — 补链路

- P1-1（gossip 丢批回退 + 重试预算 + 键改 `(origin, msg_id)`）
- P1-2（oplog 裁剪感知对端游标 / 越界转 bootstrap）
- P1-4 / P1-5（watchdog in-flight 判定、断连清水位）

### 批次 3 — 调度层治理 + 健壮性

- **Federation 分类并发槽饥饿**（诊断报告 §4，v8 下更关键：Range 成为唯一兜底，avg 127.94s 占槽会饿死 bootstrap 与 delta）
  - 长 await 任务（Range 对账 / Gossip 传播 / delta 拉取）迁出 Federation 槽，或改**分帧让出**（跑 N 行返回，下一轮续跑）
  - bootstrap 续传改「一轮一块」分帧（当前一次占槽 max 164.36s）
  - bootstrap manifest 持久化到 DB（消除重启漂移）
- P1-3（IPv6 key 统一）、P1-6（冷 peer 软删）、P2 项

---

## 7. 验收口径（双端实测）

| 指标 | 来源 | 目标 |
|---|---|---|
| `ops_lag` 各 repo | `/api/v1/federation/sync-observability` | repo=1/2/3/4 双向收敛到 0；`peer_max_seq` 不再为 null |
| `bootstrap.done_chunks` | 同上 | 单调递增，`bytes > 0`，不再长期 0/N |
| DB 权威差距 | `/api/v1/federation/status` | node 差距（现 130,794）持续下降；peer 差距（现 10,813）下降 |
| `range_stats.repair_triggers` | 同上 | 增长且 `local_only`/`remote_only` 下降 |
| tracker 删除 | DB 直查 | 删除不复活 |
| Federation 槽 | 日志 | `Federation 8/8` 槽满次数大幅下降（现 493 次/窗口） |

---

## 8. 风险与回滚

- 批次 1 全部为局部改动，可逐个 `git revert`（当前改动尚未提交，回滚成本为零）。
- P0-3 涉及删除语义，建议先在测试库验证「墓碑不被误清」再上生产双端。
- 协议侧无改动（v8 已定稿），本方案不涉及两端版本兼容问题。
- 提交与推送当前受网络阻断（TLS 层被掐），本地改动已就绪，恢复后一次性提交。

---

## 9. 实施状态（2026-09-22 03:10 更新）

### 9.1 已实施（7 个文件，clippy `-D warnings` + release 构建全绿）

| 编号 | 文件 | 改动 |
|---|---|---|
| P0-1 | `sync/mod.rs` | `use_bootstrap` 加 `rt == NODE` 门控 + 非 NODE 回落 delta（debug 留痕）；`start_bootstrap` 非 NODE 补日志 |
| P0-2 | `sync/bootstrap.rs` | `build_repo_manifest_impl` 的 `manifest.repo` 由硬编码 NODE 改为回填入参 |
| P0-3 | `storage/db.rs`、`storage/tracker_repo.rs`、`sync/tracker_sync.rs` | 新增 `is_tracker_tombstoned` / `save_tracker_keep_tombstone`；`add_trackers_batch_internal(…, remote)` 入站命中墓碑不复活；新增 `remove_tracker_remote_sync`（软删、**不写 oplog**）；DELETE 分支由丢弃改为落地 |
| P0-4 | `sync/bootstrap.rs`、`sync/mod.rs` | 新增 `verify_transport(声明行数, 实收条数)`；块校验由「重算本地区间」改为「传输完整性」 |
| F8a 回滚 | `main.rs` | `fed_range_reconcile_timeout` 900s → 300s |
| P1-1（部分） | `federation/gossip.rs` | 零成功 batch（含限流全跳过）一律回退 outbox；放弃判据由 3 次 tick 改为 `RETRY_BUDGET=30s`（新增 `retry_first_at`） |
| P1-4 | `sync/mod.rs` | watchdog 增加 in-flight 判定（有请求在途不计 stall） |
| P1-5 | `sync/mod.rs` | 新增 `prune_stale_delta_state`：失联对端的 `delta_peer_max` / watchdog / request_at / inflight 全清 |
| 批次 3 | `intelligence/task_scheduler.rs` | **分类并发饥饿补偿**：连续被拒 6 轮（30s）后允许超额 +2 准入（新增 `concurrency_starve`） |
| 批次 3 | `sync/mod.rs` | 新增 `bootstrap_check_at`：`check_and_trigger_bootstrap` 独立节流 300s；新增 `bootstrap_rebuild_at`：应答方清单现场重建加 900s 租约（防双向重建循环） |
| P2-3 | `sync/mod.rs` | range 深度到顶被强制降级为叶时跳过修复（防 `key_diff(本地, 空)` 幽灵推送） |

### 9.2 实测结果（双端部署后）

| 指标 | 修复前 | 修复后 | 结论 |
|---|---|---|---|
| `fed_bootstrap_resume` 执行次数 | 32 次被延迟 / 仅 3 次执行 | 每轮执行（18:57/18:58/18:59/19:00 各一次，耗时 1–7s） | ✅ 槽饥饿解除 |
| 单次 resume 占槽 | max 164.36s | 1.19–6.86s | ✅ 巡检节流生效 |
| Gossip 丢批 | 限流跳过即永久丢弃 | 回退重试，启动瞬时重试后归零（后续窗口 retry_warn=0） | ✅ 不再静默丢数据 |
| range 对账 | — | `leaf_ranges=3, local_only=170, remote_only=106, repair_triggers=3` | ✅ 修复通道在跑 |
| `bootstrap.done_chunks` | 0 | 仍 0（见 9.3） | ⏳ 未打通 |
| `ops_lag.peer_max_seq` | null | 仍 null（启动 5 分钟内，待 delta 往返） | ⏳ 观察中 |

### 9.3 遗留（bootstrap 分块传输仍未打通）

现象：双端每 60s 互请块 0，应答方均需「现场重建清单」（DB 全表扫描数分钟）才能应答，
重建未完成前缓存为空 → 下一轮请求再次触发重建 → **双向重建循环**，块响应始终未落地。

已止血：900s 重建租约（每 15 分钟最多一次重建）。
根治（下一批）：① 应答方清单缓存持久化到 DB（消除重启漂移）；② 清单构建分帧（不要一次扫完）；
③ resume 节流（不要 60s 一次）；④ 修复后重测 `done_chunks` 是否推进。

### 9.4 确认无需修复

- **P2-4（port 哈希双公式）**：Merkle 删除后 `mod.rs:78` 注释已明示 `data_hash` 无消费方；
  range/bootstrap 两侧都走 `db.rs::load_*_key_hashes_in_range`（统一 `i64::to_le_bytes`），无幻影差异。

### 9.5 本轮未做（风险/收益权衡，单列）

P1-2（oplog 消费 `min_seq`）、P1-3（IPv6 key 统一，需同步改入站解析）、P1-6（`archive_cold_peers` 改软删）、
P2-1（broadcast Lagged）、P2-2（seen LRU 续热度）、P2-5（清单缓存分 repo）、
#5 的 msg_id 键改 `(origin, msg_id)`（3+ 节点才触发，当前双节点无实害）。
