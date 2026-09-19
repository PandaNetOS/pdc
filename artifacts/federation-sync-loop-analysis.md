# PDC 联邦同步回环排查分析报告

> 现象：两个联邦节点（192.168.30.57 `22335aa9…` ↔ 192.168.30.51 `3b9a0512…`）45 分钟内双向搬运约 **55 GB**，`node_sync_count` 达 **280 万 / 410 万**，而 `dht_nodes` 表仅约 **40 万行**。
> 结论：**存在回环**。不是「A→B→A 消息回灌」，而是「**反熵对账粒度太粗 → 每轮都判 100% 差异 → 每 60 秒把整张 node 表重传一遍**」的永久闭环。
> 采样时间：2026-09-19 16:12 – 17:05 ｜ 基线版本：`ce2354e`（含身份迁移修复）｜ 两端运行同一二进制

---

## 一、现象（实测）

两端 `GET /api/v1/federation/status`（16:55 采样，uptime ≈ 45 min）：

| 指标 | 本地 .57 | 远程 .51 |
|---|---|---|
| bytes_sent | **24.66 GB** | **30.65 GB** |
| bytes_recv | **30.65 GB** | **24.66 GB** |
| total_messages_sent / recv | 447,500 / 933,605 | 486,216 / 943,149 |
| sync_entries_applied | 2,818,142 | 4,111,049 |
| **node_sync_count** | **2,805,135** | **4,097,953** |
| peer_sync_count | 175 | 56 |
| infohash_sync_count | 12,442 | 12,672 |
| tracker_sync_count | 390 | 368 |
| merkle_repairs | 77 | 73 |
| node_repo_total | 402,120 | 401,493 |

- 45 分钟单向约 25–30 GB ≈ **90 Mbit/s 持续双向**，合计约 55 GB。
- NODE 仓库只有约 40 万行，`node_sync_count` 却到 **280 万 / 410 万** → 同一张表在 45 分钟内被**反复应用 7–10 遍**。

远端 `stdout.log` 的决定性证据（**每 60 秒一次、连续 50+ 次**）：

```
16:12:43  Merkle 对账发现 256 个差异分片（100.0%）: repo_type=1, from=22335aa9
16:13:50  Merkle 对账发现 256 个差异分片（100.0%）: repo_type=1, from=22335aa9
...
17:03:18  Merkle 对账发现 256 个差异分片（100.0%）: repo_type=1, from=22335aa9
```

每次 100% 之后紧接着走**最重路径**：

```
差异≥20%，触发分层Merkle+分片同步（repo_type=1, from=22335aa9）
启动分层 Merkle 对比: repo=1, peer=22335aa9
分层对比完成: repo=1, peer=22335aa9, 差异L2总数=43172        ← 65,536 个 L2 中的 66%
创建分片同步引擎: peer=22335aa9, repo=1, 差异L2数=43172
引擎启动: repo=1, L2分片数=43172, 并发=4
引擎完成: repo=1, acked=27688, failed=0, entries=168336, elapsed=187.4s
```

**每一轮：43k–53k 个 L2 分片、16.8 万条目（约整张表的 41%）、耗时 187 秒，然后 60 秒后再来一次。**

---

## 二、关键反证：真实数据差异只有 2%

通过 SMB 抓取两端 `pdc.db`，用 sqlite 直接比对（本报告涉及的 DB 侧判定均以此为事实依据）：

| 项 | 值 |
|---|---|
| 本地 dht_nodes | 409,275 |
| 远程 dht_nodes | 406,419 |
| 交集 | 404,237（98.8%） |
| 仅本地有 | 5,038 |
| 仅远程有 | 2,182 |
| 交集内 node_id 不一致 | 1,388 |
| **需要同步的真实差异** | **8,608 行 ≈ 2.1%** |

而 256 个 L1 分片每片约 `409275 / 256 ≈ 1,600` 行 → 8,608 行差异平摊到 256 片约 **每片 34 行**。

→ 只要分片里有 1 行不同，分片哈希就不同 → **256/256 全不同**。

**所以「100% 差异」不是误报，而是「L1 粒度（每桶 1,600 行）+ 2% 散列化 churn」的数学必然结果。**

### 2.1 为什么差异必然撒满全部分片

分片归属公式（`db.rs` / `merkle.rs`）：

```
key        = "ip:port"
L1 分片号   = blake3(key)[0]              ← 第一个字节，0..255
L2 分片号   = blake3(key)[0] * 256 + blake3(key)[1]
```

- 分片号是**内容哈希**，不是地址就近 —— 无任何空间 / 时间局部性。
- 差异只来自「一边有一边没有」的陌生新节点（两侧爬虫各自独立发现），其 `ip:port` 全域均匀分布，经 blake3 更均匀。
- 单桶零差异概率：`(1 − 2.1%)^1600 ≈ 1.7 × 10⁻¹⁵`
- 256 个桶中期望「零差异」桶数：`256 × 1.7e-15 ≈ 4e-13 ≈ 0`

**结论：只要存在 2% 的均匀 churn，就物理上不可能存在任何一个哈希不变的分片。100% 是必然。**

---

## 三、整体同步逻辑梳理

### 3.1 六条链路

| # | 链路 | 触发 | 周期 / 条件 | 代码位置 |
|---|---|---|---|---|
| 1 | 本地变更 Gossip Push | 本地新节点 / 删除 | TaskScheduler 驱动 | `gossip.rs:178/215` `submit_gossip*`；`gossip.rs:264` `gossip_propagation_tick` |
| 2 | 心跳 / 连接维护 | 连接建立 | 30 s | `connection.rs` |
| 3 | **Merkle 反熵对账** | 定时 | **60 s** | `main.rs:1882` `fed_merkle_anti_entropy` → `gossip.rs:920` `anti_entropy_tick` → `sync/mod.rs:1847` `handle_merkle_digest` |
| 4a | 小差异修复 | 差异 < 20%（< 52 个 L1） | 事件 | `MerkleRequest` → `MerkleRepair`（`sync/mod.rs:1904/1923`） |
| 4b | **大差异：分层 Merkle + 分片同步** | 差异 ≥ 20% | 事件 | `trigger_layered_sync`（`:2396`）→ L1/L2 请求（`:2103`）→ `start_shard_sync`（`:2459`）→ `ShardSyncEngine` |
| 4c | 旧版 DiffSync | 对端不支持分层 | 事件 | `handle_diff_sync_request`（`:1139`） |
| 5 | 全量兜底（Bootstrap） | 首次连接 | 每对端一次 | `trigger_initial_sync`（`:671`） |
| 6 | Merkle 收敛 | 定时 | 全量重算 **300 s** / 增量 **10 s** / 分片列回填 300 s | `main.rs:2338/1045` |

### 3.2 Merkle 收敛链（关键）

```
dht_nodes 表(DB)  ──load_all_node_keys_hashes──►  L2(65536) ──► L1(256) ──► L0
      ▲                                                  ▲
      │ 只标 dirty（本地写入）                             │ 每 300 s 从 DB 全量重算
      │                                                  │ (merkle_cold_rebuild_*)
   本地写入 ─────────────────────────────────────────────┘
   ★ 联邦入站同步（apply_node_sync / handle_shard_sync_batch）故意【不】更新 Merkle、不标 dirty
     （为切断 A→B→A 回灌，见 sync/mod.rs:589-593、2372-2386）
```

### 3.3 已排除的可疑回路（逐个验证过，**不是**本次主因）

| 候选回路 | 结论 | 证据 |
|---|---|---|
| Gossip 转发 A→B→A | **不存在** | `gossip.rs:845-859`：单连接且 `origin == 唯一对端` 时不入 outbox |
| `incremental_sync_tick` 把 dirty L2 整批推回对端 | **不会发生** | `sync/mod.rs:2718` 定义后**全工程无调用点**（无 TaskScheduler 注册），属死代码 |
| Push-Pull `GossipDigest` 广告自身收来的变更 | **不会发生** | `recent_changes`（`sync/mod.rs:106`）只有读（`:1030` / `:2779`），**全工程无写入点** → 恒空，该路径 no-op |
| `apply_node_sync` 回灌 | **已修复** | `sync/mod.rs:468-473 / 589-593` 明确不回写 `recent_changes`、不标 dirty |

**→ 真正的问题不在「消息层回环」，而在「对账策略 + 分片加载」。**

---

## 四、根因（按影响排序）

### R1（主因）反熵升级阈值用 L1 粒度 → 2% 散列差异 = 永远 100%，永远走最重路径

`sync/mod.rs:1885`：

```rust
if diffs.len() * 5 >= 256 {          // ≥ 20% 的 L1 分片差异 → 走"分层分片同步"
    ... trigger_layered_sync / trigger_diff_sync
}
```

DHT 节点表是**两节点各自爬虫独立发现**的高 churn 数据（实测 10 分钟内两库都从约 402k 涨到约 409k）。2% 的差异散列在 256 个桶里 → 每桶都不同 → 100% → 每次都触发「全量级」路径，永远不会走「只修差异分片」。

**天然不可能收敛到 0 差异**（两边都在不停产生新节点），所以这个循环是永久的。

> **对照**：PEER / INFOHASH 两个 repo 的差异只有 2–8%（< 20%）→ 走轻量 `MerkleRequest/Repair`，所以 `merkle_repairs` 只有 77/73。**这反证了阈值逻辑本身在别的 repo 上是正常工作的。**

### R2（差异高估 5.4×）入站数据不折进 Merkle

- `sync/mod.rs:589-593`：入站 apply **不**更新内存 Merkle；`handle_shard_sync_complete`（`:2372-2386`）也明确**不 rebuild**。
- 唯一收敛靠 `merkle_cold_rebuild_*`（`main.rs:2551`，间隔 = `merkle_full_rebuild_interval_secs` = **300 s**）。
- 而反熵是 **60 s** 一次、单次分片同步要 **187 s**。
- → 下一轮对账时，本地树里还没包含「刚拉回来的 16.8 万条」+「本地爬虫这段时间新发现的」 → 实测 L2 差异 **43,172 / 65,536 = 66%**，而按 DB 真实差异推算应只有 **约 12%**（8,608 行落在约 8,050 个 L2）。

### R3（放大最多 256×）按 L2 取数，实际加载整条 L1，且没有 L2 过滤

`sync/mod.rs:2493-2535`（`start_shard_sync` 的 `load_fn`）：

```rust
Arc::new(move |l2: u32| -> Vec<SyncEntry> {
    let shard = MerkleTree::l1_for_l2(l2);   // ★ L2 → L1
    let shards = [shard];
    ... load_node_rows_by_shards(&shards)    // ★ 取回整条 L1（256 个 L2 的全部行）
```

- `db.rs:1117-1142` 的 `load_node_rows_by_shards` 是**按 L1 过滤**（列名 `l2_shard` 存的其实是 L1）。
- `shard_sync_engine.rs:357-401` 的 `sync_single_shard` 拿到 entries 后**直接发送，没有再按 L2 过滤**。
- 结果：43,172 个 L2 请求 × 每个加载约 1,600 行 = **约 6,900 万次行加载 / 轮**。
- `hash_list_fn`（`sync/mod.rs:2636-2666`）同样问题；接收侧 `handle_shard_sync_hash_list`（`:2280-2354`）也整条 L1 加载 → 双方各烧一遍 CPU / IO。
- `shard_sync_engine.rs:15-18` 的文档写的是「DB 按 L1 分片加载，**内存中按 L2 过滤分批**」—— **这个 L2 过滤没有实现**。

### R4 删除语义不闭环：三种「删」都不影响 DB，而 Merkle 是 DB 驱动的

| 删除入口 | 影响内存 | 影响 DB | 发墓碑 |
|---|---|---|---|
| `NodeRepository::remove_node`（`node_repo.rs:631-665`） | ✅ | ❌ | ✅ |
| **`remove_cold_nodes`（`node_repo.rs:869-926`，tier_check 每 300 s 调）** | ✅ | ❌ | **❌** |
| **收到 DELETE 墓碑（`sync/mod.rs:546-586` → `remove_nodes_batch_internal`）** | ✅ | ❌ | — |

- 对端删掉的行，DB 里永远还在 → 下一次 `load_all_node_keys_hashes` 又把它算进 Merkle → 又出现差异 → 又被拉回来。
- 配合 `contains_sync` 只查内存，冷驱逐后会重复「当作新节点再加一遍」，直接推高 `node_sync_count`。
- **内存 / DB / Merkle 三方真相不一致，是「删了又活」的根。**

### R5（次要）`shard_backfill` 每 300 s 全表扫 `l2_shard = 0`

`main.rs:1045` + `db.rs:1562`：运行时 INSERT 不写 `l2_shard`（默认 0），靠 300 s 回填修正。

实测 shard 0 已积压 **6,752 / 2,105 行**（正常分片约 1,600），说明回填正在追，但**不是本次放量的主因**（256 个 shard 都有数据，无空桶）。

---

## 五、修复建议（分档）

| 档 | 措施 | 预期效果 | 风险 |
|---|---|---|---|
| **P0-A** | **升级判定改为 L2 粒度**：`handle_merkle_digest` 不只看 L1 差异比例决定升级，而是直接用已有的 L2 对比结果（`diff_level2`，`merkle.rs:682`）驱动分片同步；把「≥20%」阈值改成「差异 L2 数 / 65,536」或绝对数判定 | 差异 L2 从 43k 降到约 8k，单轮数据量从 16.8 万降到约 1 万（**-94%**） | 低，只改阈值与判定入口 |
| **P0-B** | **补上 L2 过滤**：`load_fn` / `hash_list_fn` 按 L1 取回后用 `merkle.l2_shard_for_key(key) == l2` 过滤；DB 侧最好加 `WHERE l2_shard = ?` 的 L2 索引（或把列改存真 L2） | DB 行加载量 6,900 万 → 约 27 万 / 轮（**-99.6%**） | 中，DB schema / 索引改动需迁移 |
| **P0-C** | **入站同步后折进 Merkle**：`handle_shard_sync_complete` 时不 rebuild 全树，而是按本批 `l2_shards` 调 `recompute_l2_from_db`（**已有 API，`merkle.rs:327`**），只重算落地的 L2 | L2 差异回到约 12%，不再把刚拉的数据当差异 | 中，需保证不被 300 s 全量重算覆盖（幂等） |
| **P1** | **删除语义闭环**：`dht_nodes` 加软删除列 `deleted_at`，`load_all_node_keys_hashes` / `load_*_rows_by_shards` 过滤；DELETE 墓碑落库；`remove_cold_nodes` 明确「只动内存」语义不再污染 Merkle | 消除「删了又活」的永久差异 | 中，需 schema 迁移 |
| **P2** | 反熵按 repo 差异化：高 churn 的 NODE 用更细粒度（L2），低 churn 的 TRACKER 保持 L1；全量重算 300 s × 4 repo 的全表扫描评估降频 | 降 CPU / IO | 低 |
| **P2** | 收敛诊断：`connection.rs:327` 的握手失败 `debug!` 提级 + 给「分层对比得出的 diff_l2 数」加计数器 / 端点，避免下次只能靠日志考古 | 可观测性 | 低 |

> 完整的架构级整改（稳态改 oplog delta、反熵改有序区间下钻、bootstrap 独立通道）见
> [12-federation-sync-reconciliation.md](../docs/architecture/12-federation-sync-reconciliation.md)
> 与 [ADR-006](../docs/adr/006-federation-sync-convergence.md)。

---

## 六、一句话总结

不是网络问题，也不是消息层回环。是**反熵对账用 256 个 L1 桶（每桶 1,600 行）去衡量一个天然有 2% 散列 churn 的表** → 每轮都判「100% 不同」→ 每 60 秒触发一次 4.3 万 L2 / 16.8 万条的「全量级」分片同步；再叠加**按 L2 取数却加载整条 L1 且不过滤**（最多 256× 重复加载）和**入站数据不折进 Merkle**（差异被高估 5.4×）。
