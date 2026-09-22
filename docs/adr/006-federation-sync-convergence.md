# ADR-006: 联邦同步收敛架构（稳态 delta + 兜底区间反熵 + 独立 bootstrap）

> ⚠️ **本文已过时（2026-09-22）：Merkle 树已从 pdc 全面移除。** **本 ADR 中「Merkle 兜底」相关决策已被 ADR-007 取代**；稳态 delta + bootstrap 引导部分仍然有效。
>
> 现行同步架构以 [ADR-007 Range 反熵唯一兜底](007-range-only-anti-entropy.md) 与 [AGENTS.md §联邦同步架构 v8](../../AGENTS.md) 为准；
> 下文涉及 Merkle 对账 / 分片同步引擎 / DiffSync / FullSync 的描述仅作历史参考，不代表当前代码。

> 状态：📋 提议中
> 日期：2026-09-19
> 决策者：项目维护者
> 相关：联邦同步、Merkle 反熵、分片同步、存储 schema、亿级规模演进

## 背景 (Context)

### 现状

联邦同步当前由六条链路组成（详见 [12 文档 §2.1](../architecture/12-federation-sync-reconciliation.md)），其中 **Merkle 反熵对账每 60 s 一轮**，按 256 个 L1 分片比较摘要：差异 < 20% 走轻量 `MerkleRepair`，≥ 20% 走「分层 Merkle + 分片同步」重路径。

### 问题

2026-09-19 在两个生产节点（192.168.30.57 ↔ 192.168.30.51）上观测到**同步量严重超出正常范围**：

- 45 分钟双向搬运约 **55 GB**（≈ 90 Mbit/s 持续双向）；
- `node_sync_count` 达 **280 万 / 410 万**，而 `dht_nodes` 表仅约 **40 万行** → 同一张表被反复应用 **7–10 遍**；
- 远端日志**每 60 秒一次、连续 50+ 次**记录 `Merkle 对账发现 256 个差异分片（100.0%）`，随后固定进入 `差异L2总数=43172` / `entries=168336` / `elapsed=187.4s` 的重路径。

而经 SMB 抓取两端 DB 直接比对，**真实差异只有 8,608 行（2.1%）**。

### 根因（详见 artifacts 分析报告）

| # | 根因 | 位置 | 放大 |
|---|---|---|---|
| **R1 主因** | 升级阈值用 **L1 粒度**（`diffs.len()*5 >= 256`）。256 桶 × 1,600 行/桶，2% 均匀 churn 下**单桶零差异概率 ≈ 1.7×10⁻¹⁵** → 每轮恒判 100% → 永远走最重路径，且**天然不可能收敛到 0** | `sync/mod.rs:1885` | 触发频率 |
| **R2** | 入站 apply / `handle_shard_sync_complete` **不折 Merkle、不标 dirty**，只靠 300 s 冷重算收敛 → 差异被高估 **5.4×**（实测 66% vs 真实约 12%） | `sync/mod.rs:589-593`、`:2372-2386` | 差异高估 5.4× |
| **R3** | `load_fn` 按 L2 取数却 `l1_for_l2(l2)` **加载整条 L1**，且发送前**不按 L2 过滤** → **约 6,900 万次行加载/轮** | `sync/mod.rs:2493-2535`、`shard_sync_engine.rs:357-401` | 行加载最多 256× |
| **R4** | 三种「删」（`remove_node` / `remove_cold_nodes` / 收 DELETE 墓碑）**都不落 DB**，而 Merkle 是 DB 驱动 → 「删了又活」 | `node_repo.rs:631/869`、`sync/mod.rs:546` | 永久差异 |
| **R5** | `shard_backfill` 每 300 s 全表扫 `l2_shard=0` 回填，shard 0 积压 6,752 / 2,105 行 | `main.rs:1045` / `db.rs:1562` | 次要 |

### 约束条件

1. **对称爬虫、无主从**：两端互为备份，差异语义天然是「集合并集 + 逐字段取新（LWW by `updated_at`）」，适合集合调和假设，但没有权威版本可仲裁。
2. **高 churn 不可消除**：两个独立爬虫的 NODE 表天然存在 2% 互不重叠的新节点，这部分差异**消不掉**，只能让它「便宜地传」。
3. **亿级演进目标**（[11-billion-scale-storage.md](../architecture/11-billion-scale-storage.md)）：Merkle 每 300 s 全表重算、固定 2 层扇出、入站不折 Merkle 三项在 10⁹ 行下均不成立。
4. **协议兼容是硬约束**：`HelloMessage` 曾因新增 `timestamp_ms` / `nonce`（`4de716a`）导致 bincode 反序列化硬失败、握手全拒。任何协议字段变更必须全网同步升级。
5. **`d`（差异量）与 `N`（总量）必须分开衡量**：集合调和的成本只与 `d` 有关；`d` 到亿级时任何算法输出都是 Ω(d)（≥1.6 GB），**没有算法能救**，只能从「不让 d 涨到亿级」入手。

## 决策 (Decision)

采纳**三层分工**的联邦同步架构，并把「同步的内容」从**状态差**改为**变更流**：

### 1. 稳态层（主线）：oplog delta —— 成本 O(Δ)

| 变更 | 内容 |
|---|---|
| schema | 4 个 repo 加列 `version INTEGER` / `origin_node BLOB` / `updated_at INTEGER` / `deleted_at INTEGER` + 索引（`ADD COLUMN ... DEFAULT` 为 O(1) 元数据操作） |
| oplog | 新增 `feed_oplog(seq, op, repo, key, value, ts_ms, origin)`，**与业务写入同事务**；保留窗口默认 24 h、按最小对端进度裁剪 |
| 协议 | `OpsRequest { repo, since_seq, limit }` / `OpsBatch { ops, next_seq, has_more }`；幂等 upsert；`origin == 本节点` 的回流直接丢弃 |
| 版本向量 | 每个对端持久化 `peer_seq[A]`，重启不重传历史 |

**稳态下 `Δ` 只等于「对端还没收到的新增」**，与 `N`、`d` 无关。

### 2. 兜底层：Range-based 反熵 —— 自适应深度下钻

反熵从「哈希分桶比摘要」改为「**有序区间 + 分界点下钻**」（Meyer 2022）：

```
ReconcileRange(lo, hi): 若摘要相同 → 剪枝；若已到叶级 → 交换行指纹求集合差；否则按 split_points 递归
```

- **不需要预先知道 d**，最坏访问 O(d·log(N/d)) 个节点。
- 天然解决「差异撒满全部分片」—— 差异跟着 key 位置走，不再跟着 `blake3(key)[0]` 走。
- 兜底只做**抽样校验**（随机 M 个区间），成本可控。

### 3. 引导层：bootstrap 独立通道 —— 快照 + manifest + 追尾

**`trigger_initial_sync` 与在线反熵彻底解耦**，改为一次性数据迁移：

| 阶段 | 内容 |
|---|---|
| ① 协商与冻结快照 | `VACUUM INTO` 生成一致性快照，记录水位 **W0** |
| ② manifest | 按 key 有序区间切块，每块附 `sha256` |
| ③ 并行限流传输 | N 路并发，A 侧限流可抢占，B 侧流式落盘 |
| ④ 落地 | 物理快照直接当主库；逻辑块批量 upsert，**严禁逐条 INSERT** |
| ⑤ 增量追尾 | 拉 `seq > W0` 的 oplog，追到 Δ < 阈值 |
| ⑥ 校验与切稳态 | 抽查区间摘要 → 切 delta + 周期反熵兜底 |

**七条铁律**：A 侧限流可抢占 / 幂等 / 快照可复用 / **oplog 保留窗口 > 预估 bootstrap 时长** / B 侧流式落盘 / 进度可观测 / 按 repo 分别 bootstrap。

### 4. 落地顺序不可颠倒

```
S1  稳态止血   P0-A(反熵判定改 L2 粒度) + P0-B(补 L2 过滤) + P0-C(入站折 Merkle)   ← 不改 schema，低风险
S2  删除闭环 + schema 迁移
S3  oplog delta + Merkle 增量维护
S4  Range-based 反熵
S5  bootstrap 专用通道 + 可观测性
```

> **S1 不做，后面全白搭**：只要稳态还在跑「每 60 秒重传全表」，无论 bootstrap 多完美，跑几天照样退回亿级差异。

## 后果 (Consequences)

### 正面影响

- **同步量回到真实差异量**：稳态成本从 O(N)/Ω(d) 降到 **O(Δ)**。
- **停止永久闭环**：R1/R2/R3 修复后，差异不再被高估、不再每次走最重路径。
- **亿级可行**：Merkle 改增量维护（O(log N)）后，10⁹ 行不再需要 300 s 全表重算。
- **新节点引导工具化**：快照 + manifest + 断点续传 + 限流，可复用一份 `snap.db` 服务多个新节点。
- **可观测性补齐**：`diff_l2_count` / `ops_lag_seq` / `bootstrap_progress` 使「同步是否健康」可度量，不再靠日志考古。
- **S1 单独可交付**：三个文件的阈值/过滤改动即可拿到 -94% 数据量、-99.6% DB 行加载。

### 负面影响 / 代价

- **需要 schema 迁移**（S2 起）：4 个 repo 加 4 列 + 索引；亿级表虽有 O(1) 元数据优势，仍需备份与回滚脚本。
- **新增组件**：`storage/oplog.rs`、`federation/sync/delta.rs`，以及版本向量持久化。
- **协议扩展**：新增 `OpsRequest` / `OpsBatch` 消息类型，需能力位协商以兼容旧节点。
- **稳态与兜底双轨**：两套机制并存期需要一致性对账（用只读诊断模式比对）。

### 风险

- **协议兼容**：新消息类型若不带能力位协商，旧节点会遇到未知类型（历史上 `4de716a` 的字段变更已造成过一次全网握手失败）。
- **软删除误删**：`deleted_at` 一旦接上查询过滤，历史墓碑数据可能误伤。
- **oplog 膨胀**：保留窗口设得过大则表膨胀，过小则 bootstrap 追尾断档。
- **Range-based 下钻引入往返放大**：区间切分不当可能反而比哈希分桶更贵。

### 缓解措施

- **S1 先行且零 schema 改动**：先用最低风险的阈值/过滤改动止血，验证收益后再推进 schema。
- **协议变更走能力位**：`HelloMessage` 携带 capability，协商不通过则回退旧路径（保留 4c 旧版 DiffSync）。
- **软删除先只写不读**：`deleted_at` 先写入观察一个版本，确认无异常后再接 `WHERE deleted_at IS NULL`。
- **Range-based 先只读灰度**：以诊断模式运行（只求差集、不改数据），与现有机制比对一致性后再切主链。
- **oplog 窗口可配置 + 告警**：默认 24 h，监控表大小与最小对端进度。
- **回滚策略**：S1 纯阈值/过滤改动可 `git revert`；S2 起迁移脚本配套回滚（保留新列、旧代码忽略）。

## 替代方案 (Alternatives)

### 方案 A：保持现状，仅调大反熵周期

- 优点：零改动。
- 缺点：闭环依然存在，只是频率降低；`node_sync_count` 仍会超线性增长，亿级下彻底失效。
- 不选择的原因：治标不治本，且掩盖了 R1–R4 四个真实缺陷。

### 方案 B：只做 S1（阈值 + 过滤止血），不动架构

- 优点：风险最低，收益立竿见影（-94% 数据量）。
- 缺点：稳态仍是「状态差同步」，`d` 仍随两端爬虫速率差增长；亿级 `N` 下 Merkle 全表重算与固定扇出仍不成立。
- 不选择的原因：**作为第一阶段采纳（S1），但不作为终点** —— 需 S3/S4 收敛到 delta + 区间反熵。

### 方案 C：直接上 IBLT（单轮集合调和）

- 优点：单轮、传输量只与 `d` 有关（260–680 KB），与 `N` 无关。
- 缺点：**需预估 `d` 上界**（d 未知时要表翻倍重试）；不解删除语义，必须先把墓碑补成一等条目；`d` 亿级时表达约 4 GB，反而失效。
- 不选择的原因：适用区间是「大 N、小 d」，而本项目当前的痛点是「机制坏了导致 d 不收敛」，先修机制比换算法更根本。保留为 P2-4 可选项（小 d 快速路径）。

### 方案 D：优先做 bootstrap 专用通道

- 优点：直接服务「亿级节点接新节点」场景。
- 缺点：**顺序错误**。若稳态仍在每 60 秒重传全表，bootstrap 完成后系统会再次退化回亿级差异。
- 不选择的原因：bootstrap 是 S5，必须等 S1–S3 站住。

### 方案 E：把联邦多实例纳入 ICC 提调

- 优点：跨实例统一调配。
- 缺点：与 [ADR-005](005-intelligent-control-center.md) 已定边界冲突（ICC 明确不纳入联邦），且引入分布式一致性复杂度。
- 不选择的原因：违反已有架构边界，且与联邦去中心化方向相悖。

## 实施计划 (Implementation Plan)

- [ ] **S1 稳态止血（P0-A/B/C）**：`handle_merkle_digest` 升级判定改 L2 粒度；`load_fn` / `hash_list_fn` / `sync_single_shard` / `handle_shard_sync_hash_list` 补 L2 精确过滤；`handle_shard_sync_complete` 按批调 `recompute_l2_from_db`。**不改 schema**。
- [ ] **S2 删除闭环 + schema 迁移**：4 repo 加 `version/origin_node/updated_at/deleted_at`；三种「删」落 DB + 写 oplog；`load_*` 加 `deleted_at IS NULL`；`shard_backfill` 改为写入即填。
- [ ] **S3 oplog delta**：`storage/oplog.rs` + `federation/sync/delta.rs` + 版本向量；Merkle 改增量维护（O(log N)），取消 300 s 常态全表重算。
- [ ] **S4 Range-based 反熵**：`ReconcileRange` 下钻；旧分层 Merkle 降级为兼容路径；先诊断模式灰度。
- [ ] **S5 bootstrap 通道 + 可观测性**：六阶段 + 七铁律；`diff_l2_count` / `ops_lag_seq` / `bootstrap_progress` 指标与 API。
- [ ] **清理技术债**：删除死代码 `incremental_sync_tick`（`sync/mod.rs:2718`）；`recent_changes`（`:106`）补写入点或删除路径；`[DIAG]` / `[perf]` 日志降级。

## 验证标准 (Verification Criteria)

- [ ] S1：单轮 `entries` 从 168,336 降至 < 15,000；`node_sync_count` 增速下降 ≥ 90%；15 分钟双向流量从约 18 GB 降至 < 1 GB；真实差异（DB 比对）不再扩大。
- [ ] S1：L2 差异比例从 66% 回到真实约 12%；`merkle_repairs` 与分层同步触发次数显著下降。
- [ ] S2：删除的行在两端 DB 均可见 `deleted_at`；「删了又活」消失；shard 0 积压不再增长。
- [ ] S3：`ops_lag_seq` 稳态 < 10,000；稳态带宽 < 5 Mbit/s；重启后不重传历史。
- [ ] S4：反熵单轮访问节点数 < 50,000；差异稳定收敛，不再长期停在 100%。
- [ ] S5：新节点 10⁹ 行 bootstrap < 1 小时；中断可从断点恢复；A 侧 CPU / 带宽无显著抬升。
- [ ] 全流程：`cargo build --release` / `cargo test --all` / `cargo fmt --check` / `cargo clippy -D warnings` 全绿。
- [ ] 兼容性：新旧节点混跑时，未升级方回退到兼容路径且不影响数据正确性。

## 参考资料 (References)

- [12-federation-sync-reconciliation.md — 联邦同步架构重构方案](../architecture/12-federation-sync-reconciliation.md) — 本决策的完整设计与改动清单
- [artifacts/federation-sync-loop-analysis.md — 联邦同步回环排查分析报告](../../artifacts/federation-sync-loop-analysis.md) — R1–R5 根因与实测证据
- [artifacts/merkle-diff-analysis.md — Merkle 树分片幻影差异分析报告](../../artifacts/merkle-diff-analysis.md) — 同族分析（tracker `disabled` 编码宽度不一致）
- [11-billion-scale-storage.md — 亿级数据存储架构方案](../architecture/11-billion-scale-storage.md) — 存储面的亿级配套
- [07-federation.md — 联邦网络架构文档](../architecture/07-federation.md) — 现存但已过时（阶段 3 架构），待单独修订
- [09-merkle-async-update.md — Merkle 异步更新优化方案](../architecture/09-merkle-async-update.md)
- [ADR-004: 千万级数据性能目标与优化架构](004-performance-targets.md)
- [ADR-005: 智能控制中心（ICC）设立与统一控制收口](005-intelligent-control-center.md)
- Meyer, D. (2022). *Range-Based Set Reconciliation*. — 区间下钻方案出处
- Eppstein, D. et al. (2011). *What's the Difference? Efficient Set Reconciliation without Prior Context*. — IBLT 出处

## 变更记录 (Changelog)

| 日期 | 版本 | 变更内容 | 作者 |
|---|---|---|---|
| 2026-09-19 | 1.0 | 初始版本，记录联邦同步收敛架构决策（稳态 delta + 兜底区间反熵 + 独立 bootstrap）与 S1 优先止血顺序 | 项目维护者 |
