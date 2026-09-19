# PDC 联邦同步架构重构方案

> 文档编号：PDC-ARCH-012
> 版本：v1.0
> 日期：2026-09-19
> 状态：方案评审中（未改动任何代码）
> 事实依据：[artifacts/federation-sync-loop-analysis.md](../../artifacts/federation-sync-loop-analysis.md)

---

## 0. 摘要（TL;DR）

**现象**：两节点 45 分钟双向搬运约 55 GB，`node_sync_count` 达 280 万 / 410 万，而 node 表仅约 40 万行 —— 同一张表被反复应用 7–10 遍。

**结论**：**存在回环，但不是消息层 A→B→A 回灌**，而是「对账粒度太粗 → 永远判定为大差异 → 每分钟重传整张表」的永久闭环。

**三条主线的优先级不可颠倒**：

```
第 1 步  稳态止血   P0-A 反熵判定改 L2 粒度 + P0-B 补 L2 过滤 + P0-C 入站折 Merkle   ← 先做，否则一切白搭
第 2 步  架构收敛   稳态主线改 oplog delta（O(Δ)）+ Merkle 增量维护
第 3 步  引导通道   bootstrap 独立成「快照 + manifest + 追尾」专用路径
```

> **为什么顺序不能换**：即使 bootstrap 设计得再好，只要稳态还在跑「每 60 秒重传全表」，系统跑几天后照样退化回亿级差异。

---

## 1. 问题陈述

### 1.1 实测现象

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
- NODE 仓库仅约 40 万行，`node_sync_count` 却达 280 万 / 410 万 → 同一张表 45 分钟内被**反复应用 7–10 遍**。

远端 `stdout.log` 的决定性证据（**每 60 秒一次、连续 50+ 次**）：

```
16:12:43  Merkle 对账发现 256 个差异分片（100.0%）: repo_type=1, from=22335aa9
16:13:50  Merkle 对账发现 256 个差异分片（100.0%）: repo_type=1, from=22335aa9
...
17:03:18  Merkle 对账发现 256 个差异分片（100.0%）: repo_type=1, from=22335aa9
```

每次 100% 之后紧接着走最重路径：

```
差异≥20%，触发分层Merkle+分片同步（repo_type=1, from=22335aa9）
分层对比完成: repo=1, peer=22335aa9, 差异L2总数=43172        ← 65,536 个 L2 中的 66%
创建分片同步引擎: peer=22335aa9, repo=1, 差异L2数=43172
引擎完成: repo=1, acked=27688, failed=0, entries=168336, elapsed=187.4s
```

**每一轮：43k–53k 个 L2 分片、16.8 万条目（约整表 41%）、耗时 187 秒，然后 60 秒后再来一次。**

### 1.2 关键反证：真实差异只有 2%

经 SMB 抓取两端 `pdc.db`，用 sqlite 直接比对：

| 项 | 值 |
|---|---|
| 本地 dht_nodes | 409,275 |
| 远程 dht_nodes | 406,419 |
| 交集 | 404,237（98.8%） |
| 仅本地有 | 5,038 |
| 仅远程有 | 2,182 |
| 交集内 node_id 不一致 | 1,388 |
| **需要同步的真实差异** | **8,608 行 ≈ 2.1%** |

而 256 个 L1 分片每片约 `409275 / 256 ≈ 1,600` 行 → 8,608 行差异平摊到 256 片约**每片 34 行**。

**只要分片里有 1 行不同，分片哈希就不同 → 256/256 全不同。**

**「100% 差异」不是误报，而是「L1 粒度（每桶 1,600 行）+ 2% 散列化 churn」的数学必然结果。**

### 1.3 为什么差异必然撒满全部分片

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

## 2. 现状同步逻辑梳理

### 2.1 六条链路

| # | 链路 | 触发 | 周期 / 条件 | 代码位置 |
|---|---|---|---|---|
| 1 | 本地变更 Gossip Push | 本地新节点 / 删除 | TaskScheduler 驱动 | `gossip.rs:178/215`；`gossip.rs:264` |
| 2 | 心跳 / 连接维护 | 连接建立 | 30 s | `connection.rs` |
| 3 | **Merkle 反熵对账** | 定时 | **60 s** | `main.rs:1882` → `gossip.rs:920` → `sync/mod.rs:1847` |
| 4a | 小差异修复 | 差异 < 20%（< 52 个 L1） | 事件 | `MerkleRequest` → `MerkleRepair`（`sync/mod.rs:1904/1923`） |
| 4b | **大差异：分层 Merkle + 分片同步** | 差异 ≥ 20% | 事件 | `trigger_layered_sync`（`:2396`）→ `start_shard_sync`（`:2459`）→ `ShardSyncEngine` |
| 4c | 旧版 DiffSync | 对端不支持分层 | 事件 | `handle_diff_sync_request`（`:1139`） |
| 5 | 全量兜底（Bootstrap） | 首次连接 | 每对端一次 | `trigger_initial_sync`（`:671`） |
| 6 | Merkle 收敛 | 定时 | 全量重算 **300 s** / 增量 **10 s** / 分片列回填 300 s | `main.rs:2338/1045` |

### 2.2 Merkle 收敛链

```
dht_nodes 表(DB)  ──load_all_node_keys_hashes──►  L2(65536) ──► L1(256) ──► L0
      ▲                                                  ▲
      │ 只标 dirty（本地写入）                             │ 每 300 s 从 DB 全量重算
      │                                                  │ (merkle_cold_rebuild_*)
   本地写入 ─────────────────────────────────────────────┘
   ★ 联邦入站同步（apply_node_sync / handle_shard_sync_batch）故意【不】更新 Merkle、不标 dirty
     （为切断 A→B→A 回灌，见 sync/mod.rs:589-593、2372-2386）
```

### 2.3 已排除的可疑回路

| 候选回路 | 结论 | 证据 |
|---|---|---|
| Gossip 转发 A→B→A | **不存在** | `gossip.rs:845-859`：单连接且 `origin == 唯一对端` 时不入 outbox |
| `incremental_sync_tick` 把 dirty L2 整批推回对端 | **不会发生** | `sync/mod.rs:2718` 定义后**全工程无调用点**（无 TaskScheduler 注册），属死代码 |
| Push-Pull `GossipDigest` 广告自身收来的变更 | **不会发生** | `recent_changes`（`sync/mod.rs:106`）只有读（`:1030` / `:2779`），**全工程无写入点** → 恒空，该路径 no-op |
| `apply_node_sync` 回灌 | **已修复** | `sync/mod.rs:468-473 / 589-593` 明确不回写 `recent_changes`、不标 dirty |

**→ 真正的问题不在「消息层回环」，而在「对账策略 + 分片加载」。**

---

## 3. 根因分析

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
- 唯一收敛靠 `merkle_cold_rebuild_*`（`main.rs:2450`，间隔 = `merkle_cold_rebuild_interval_secs` = **3600 s**）。
- 而反熵是 **60 s** 一次、单次分片同步要 **187 s**。
- → 下一轮对账时，本地树里还没包含「刚拉回来的 16.8 万条」+「本地爬虫这段时间新发现的」 → 实测 L2 差异 **43,172 / 65,536 = 66%**，而按 DB 真实差异推算应只有 **约 12%**。

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
- `shard_sync_engine.rs:15-18` 文档写的是「DB 按 L1 分片加载，**内存中按 L2 过滤分批**」—— **这个 L2 过滤没有实现**。

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

实测 shard 0 已积压 **6,752 / 2,105 行**（正常分片约 1,600），说明回填正在追，但不是本次放量的主因（256 个 shard 都有数据，无空桶）。

---

## 4. 规模约束分析

### 4.1 必须区分 N（总量）与 d（差异量）

| 场景 | 集合调和是否适用 | 原因 |
|---|---|---|
| **N 亿级、d 小** | ✅ **正好适用** | 成本只与 d 有关，**与 N 无关**；d = 10⁴ 时 IBLT 表仅几百 KB，N 再大也不变 |
| **d 亿级** | ❌ **必然撞墙** | 任何求差集算法的输出规模天然是 **Ω(d)** |

### 4.2 d 亿级时所有求差集方案的成本

| 方案 | d = 10⁸ 时的单轮成本 |
|---|---|
| IBLT（m = c·d，每桶约 20 B） | **约 4 GB 表**，且需同等内存 |
| 直接传差异清单（key + value） | **约 1.6 GB+**（理论下限） |
| Range-based 下钻 | 摘要开销 O(d·log(N/d)) ≈ **2.6 GB 摘要 + 4 GB 差异**，比直接传更差 |
| Bloom filter | O(N)，N = 10⁹ 时约 1.2 GB |

**结论：d 亿级时没有算法能救 —— 要同步 1 亿条差异，物理上就得搬 GB 级数据。**

> **推论**：稳态下 d 本来就该是小的（差异只是「对端还没收到的新增」）。**d 涨到亿级，说明增量机制坏了** —— 正是 R1/R2/R3 在更大规模上的放大：每轮重传全表 → 传完又判 100% → 差异永远不收敛，越滚越大。

### 4.3 亿级 N 对现有架构的硬约束

| 现有实现 | 亿级下的后果 | 必须改成 |
|---|---|---|
| Merkle 每 300 s **全表重算**（`rebuild_all_from_db`，`merkle.rs:375`） | 10⁹ 行全扫，5 分钟跑不完，永远在重算 | **增量维护**：写入时只更新 O(log N) 个节点 |
| 入站数据**不折进 Merkle**（R2） | 差异被指数级高估，永不收敛 | 按批增量折入 |
| 固定 2 层扇出（L1 = 256 / L2 = 65,536） | 10⁹ 行 → 每 L2 桶约 15,000 行，退回粗粒度 | **自适应深度的有序区间下钻**，深度随 N 增长 |

**即：亿级规模下「固定层数的哈希分桶树」这个数据结构本身就不成立。**

---

## 5. 目标架构

### 5.1 总体原则：三层分工

| 层 | 职责 | 成本 | 周期 |
|---|---|---|---|
| **稳态层（主线）** | oplog delta 增量广播 / 拉取 | **O(Δ)** | 实时 / 秒级 |
| **兜底层** | Range-based 反熵抽样校验 | O(差异子树) | 分钟级 |
| **引导层** | bootstrap：快照 + manifest + 追尾 | O(N) 一次性 | 仅首次 |

**核心原则：同步的内容从「状态差」改为「变更流」。**

| | 状态同步（现在） | 变更同步（目标） |
|---|---|---|
| 同步什么 | 双方状态的差集 | 自上轮以来的新增 / 删除 |
| 成本 | O(N) 或 Ω(d) | **O(Δ)**，Δ = 单轮变更量 |
| 亿级时表现 | 每轮 GB 级 | 每轮只传新的那几万条 |

### 5.2 稳态层：oplog delta

```
1. 每次写入追加一条 op: (op, repo, key, value, version, origin_node)
2. 每个对端记住「我对你同步到 version V」（版本向量）
3. 同步时只说一句：give me ops since V  → 对端流式返回增量
4. 幂等应用：upsert by (repo, key)，重复 op 无害
```

这是 CRDT / anti-entropy 的标准分工：**稳态走 delta（便宜），反熵做正确性保证（防丢包、防断线漏同步）**。

集合调和从「主线」降级为「补丁」。

### 5.3 兜底层：Range-based 反熵

从「哈希分桶」改为「**有序区间 + 分界点下钻**」：

- 请求方发一个 key 区间，应答方回「该区间的摘要 + 分界点」；
- 摘要相同 → 剪枝；不同 → 按分界点继续下钻，直到叶级拿到差异条目。
- 最坏只访问 O(d · log(N/d)) 个节点；**不需要预先知道 d**，自适应收敛。
- 天然解决「差异撒满全部分片」—— 因为差异跟着 key 位置走，不再跟着哈希走。

### 5.4 引导层：bootstrap 通道

**bootstrap 绝不能用在线反熵做。** 一亿行 vs 新节点，「差异」就是全量 —— 用 diff 算法找「哪一亿行不同」纯属浪费，且必然失败：

| 在线 diff 做 bootstrap 的失败点 | 后果 |
|---|---|
| 没有快照点，边传边变 | **永远追不上** |
| 没有 manifest | 不知传到哪、中断只能从头 |
| 没有限流 | **打垮生产节点 A** |
| 沿用 L1 粒度 + 整片加载 | R1/R3 放大，实际搬运量是真实需求的数百倍 |

正确做法是把它当成**一次数据迁移**，独立成专用通道。

---

## 6. 详细设计

### 6.1 数据模型变更

```sql
-- 为 4 个 repo 的统一抽象（以 dht_nodes 为例）
ALTER TABLE dht_nodes ADD COLUMN version     INTEGER NOT NULL DEFAULT 0;  -- 单调递增，全局或按 origin
ALTER TABLE dht_nodes ADD COLUMN origin_node BLOB;                        -- 20B node_id，冲突仲裁用
ALTER TABLE dht_nodes ADD COLUMN updated_at  INTEGER NOT NULL DEFAULT 0;  -- 毫秒时间戳，LWW 用
ALTER TABLE dht_nodes ADD COLUMN deleted_at  INTEGER;                     -- 软删除（NULL = 存活）

CREATE INDEX idx_nodes_version ON dht_nodes(version);
CREATE INDEX idx_nodes_key     ON dht_nodes(ip, port);                    -- 区间下钻 / 增量查询
CREATE INDEX idx_nodes_l2      ON dht_nodes(l2_shard);                    -- 保留，供分片加载
```

**迁移策略**：`ADD COLUMN ... DEFAULT` 是 SQLite 的 O(1) 元数据操作，亿级表可秒级完成；存量行 `version = 0`、`deleted_at = NULL`，由首次 bootstrap 或区间回填补齐。

冲突合并规则（对称爬虫、无主从）：

```
同一 key 多个版本 → 取 updated_at 最大者；时间相同则比 origin_node 字典序（确定性仲裁）
```

### 6.2 oplog 设计与保留策略

```sql
CREATE TABLE feed_oplog (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,  -- 全局单调
    op         TEXT NOT NULL,                      -- 'upsert' | 'delete'
    repo       INTEGER NOT NULL,                   -- 0..3
    key        BLOB NOT NULL,                      -- ip:port 编码
    value      BLOB,                               -- upsert 时携带
    ts_ms      INTEGER NOT NULL,
    origin     BLOB NOT NULL
);
```

- 写入路径：**与业务写入同事务**（保证不丢；也保证「有数据必有 op」）。
- 保留窗口：**必须 > 预估 bootstrap 时长**（见 6.8 铁律 4）。建议默认 24 h，可配置。
- 裁剪：`DELETE FROM feed_oplog WHERE seq < (min_peer_synced_seq - safety_margin)`。

### 6.3 增量拉取协议

```
B → A:  OpsRequest { repo, since_seq: u64, limit: u32 }
A → B:  OpsBatch   { ops: [...], next_seq: u64, has_more: bool }

B 侧：apply 幂等 upsert → 更新本地版本向量 peer_seq[A] = next_seq → 循环直到 has_more = false
```

- 单批上限（如 10,000 条 / 4 MB），流式多批，可断点（`since_seq` 即断点）。
- **应用后不写回 oplog**（`origin` 判定：若 `origin == 本节点` 说明是自己发出去的回流，直接丢弃）。

### 6.4 反熵：自适应深度的有序区间下钻

```
ReconcileRange(lo, hi):
  local_digest = digest(key ∈ [lo, hi))
  → 发送 (lo, hi, local_digest)
  对端回 (its_digest, split_points[])

  if local_digest == its_digest: 剪枝，返回
  if (hi - lo) 已到叶级（行数 <= K）: 交换行指纹清单，求集合差
  else: 按 split_points 切分，对各子区间递归
```

- 无需预先知道 d，深度自适应。
- 区间摘要 = `blake3(排序后各行 (key, data_hash) 的拼接)`，可用 B 树节点直接缓存。
- 抽样模式：兜底扫描时只对随机抽取的 M 个区间做对账（成本可控）。

### 6.5 分片同步：补上 L2 过滤（P0-B）

若短期仍在用现有分片同步（分层 Merkle），则必须：

```rust
// sync/mod.rs:2493 load_fn 修正
Arc::new(move |l2: u32| -> Vec<SyncEntry> {
    let l1 = MerkleTree::l1_for_l2(l2);
    let rows = load_node_rows_by_shards(&[l1]);            // DB 仍按 L1 取（避免 schema 改动）
    rows.into_iter()
        .filter(|e| MerkleTree::l2_shard_for_key(&e.key) == l2)   // ★ 新增：按 L2 精确过滤
        .collect()
})
```

`shard_sync_engine.rs:357` 的 `sync_single_shard` 与 `hash_list_fn`（`sync/mod.rs:2636`）同样补过滤。

**预期效果**：DB 行加载量 6,900 万 → 约 27 万 / 轮（**-99.6%**）。

> 根治方案是把 `l2_shard` 列改存**真 L2 值**并加索引，取数即 `WHERE l2_shard = ?`，无需内存过滤。但这属 schema 迁移，排在 P1。

### 6.6 Merkle 增量维护（P1）

- 写入时：只重算受影响的那**一个 L2** → 上溯更新其 L1、L0（O(log) 个哈希）。
- 入站 apply 后：按本批涉及的 `l2_shards` 调 `recompute_l2_from_db`（**已有 API，`merkle.rs:327`**），只重算落地的 L2，并**标记 dirty 但不触发回灌**。
- 取消「每 300 s 全表重算」作为常态（亿级下不可行），保留为**低频兜底 + 冷启动**。
- `shard_backfill`（`main.rs:1045` / `db.rs:1562`）：改为写入时即填 `l2_shard`，取消周期全表回填。

### 6.7 删除语义闭环（P1）

| 动作 | 修正 |
|---|---|
| `remove_node`（`node_repo.rs:631`） | 内存删 + DB 置 `deleted_at` + 写 oplog('delete') + 发墓碑 |
| `remove_cold_nodes`（`node_repo.rs:869`） | **明确只动内存**，不碰 DB / oplog / Merkle；并在注释与文档中固化该语义 |
| 收 DELETE 墓碑（`sync/mod.rs:546`） | 落 DB `deleted_at` + 写本地 oplog + 折 Merkle |
| `load_all_node_keys_hashes`（`db.rs:1228`） | 增加 `WHERE deleted_at IS NULL` |
| `load_node_rows_by_shards`（`db.rs:1119`） | 同上 |

**不补墓碑，任何集合调和方案都收敛不到真 0** —— 因为「两侧都没有」与「两侧都删了」在集合语义下无法区分。

### 6.8 Bootstrap 六阶段

| 阶段 | 动作 | 产物 / 约束 |
|---|---|---|
| **① 协商与冻结快照** | 握手对齐 schema 与协议版本；一致性快照（`VACUUM INTO`，不阻塞写） | 快照水位 **W0** + 冻结快照文件 |
| **② 分块清单 manifest** | 按 key 有序区间切块，每块附 `sha256`；先发清单 | manifest（几 MB）→ B 知道「要拉什么、拉了没、缺哪块」 |
| **③ 并行限流传输** | N 路并发拉块；A 侧带宽预算 + 可抢占；B 侧流式落盘 | LZ4 / zstd，压缩比 3–5× |
| **④ 落地** | 新节点直接接收**物理快照**当主库（近零 apply）；逻辑块则批量 upsert | **严禁逐条 INSERT** |
| **⑤ 增量追尾** | 拉 `seq > W0` 的 oplog，多轮追到 Δ < 阈值 | 成本 O(Δ)，与总量无关 |
| **⑥ 校验与切稳态** | 抽查 key 区间摘要校验；切 delta 同步 + 周期反熵兜底 | 持久化 bootstrap 版本，重启不重来 |

**七条工程铁律**：

| # | 约束 | 为什么 |
|---|---|---|
| 1 | A 侧限流 + 可抢占 | bootstrap 是低优先级后台任务，爬虫 / API / 其他对端优先 |
| 2 | 幂等 | 任何 chunk / op 重复应用必须无害（upsert by key） |
| 3 | 快照复用 | 一份 `snap.db` 可同时服务多个新节点 |
| 4 | **oplog 保留窗口 > 预估 bootstrap 时长** | 否则追尾时 W0 之后的 oplog 已被裁掉，只能重打快照 |
| 5 | B 侧流式落盘 | 内存开销 O(chunk) 而非 O(N) |
| 6 | 进度可观测 | `{phase, chunks_done/total, bytes, eta}` 要有 API |
| 7 | 按 repo 分别 bootstrap | 新节点可能只要 infohash/peer，不必拉全量 node 表 |

**容量估算**（每行原始约 60 B：ip 4 + port 2 + node_id 20 + 元数据约 30）：

| 总量 N | 原始体积 | 压缩后 | @20 MB/s 传输 | 全流程（含落地 + 追尾） |
|---|---|---|---|---|
| 10⁸ 行 | 约 6 GB | 约 2–3 GB | 约 2 分钟 | **约 10 分钟** |
| 10⁹ 行 | 约 60 GB | 约 25–30 GB | 约 25 分钟 | **约 1 小时** |

### 6.9 可观测性

| 指标 | 用途 |
|---|---|
| `diff_l2_count` | 分层对比得出的差异 L2 数（本次排查只能靠日志考古） |
| `ops_lag_seq` | 每个对端的增量落后量（稳态健康度核心指标） |
| `bootstrap_progress{phase, done, total, bytes, eta}` | bootstrap 进度 |
| `handshake_reject_reason{reason}` | 握手失败原因分类（`connection.rs:327` 的 `debug!` 提级为限流 `warn!`） |
| `reconcile_nodes_visited` | 反熵访问的节点数（衡量下钻效率） |

另：生产日志中 `WARN [federation][DIAG]` / `[perf]` 每条 gossip batch 刷一行 → 降为 `debug!` 或限流。

---

## 7. 改动清单

### 7.1 P0 —— 稳态止血（不改 schema，可立即上）

| # | 文件 | 位置 | 改动 |
|---|---|---|---|
| P0-A | `src/federation/sync/mod.rs` | `handle_merkle_digest` `:1847/1885` | 升级判定改用 **L2 粒度**（复用已有 `diff_level2`，`merkle.rs:682`），阈值改为「差异 L2 数 / 65,536」或绝对数 |
| P0-B | `src/federation/sync/mod.rs` | `load_fn` `:2493-2535`、`hash_list_fn` `:2636-2666`、`handle_shard_sync_hash_list` `:2280-2354` | 补 **L2 精确过滤**（`l2_shard_for_key(key) == l2`） |
| P0-B | `src/federation/sync/shard_sync_engine.rs` | `sync_single_shard` `:357-401` | 发送前按 L2 过滤（或信任上游已过滤并加断言） |
| P0-C | `src/federation/sync/mod.rs` | `handle_shard_sync_complete` `:2372-2386` | 入站落库后**按批折 Merkle**（调 `recompute_l2_from_db`），不 rebuild 全树 |

**预期效果**：单轮数据量 16.8 万 → 约 1 万条（**-94%**）；DB 行加载 **-99.6%**；L2 差异从 66% 回到真实约 12%。

### 7.2 P1 —— 架构收敛

| # | 文件 | 改动 |
|---|---|---|
| P1-1 | `src/storage/db.rs`、`node_repo.rs`、`peer_repo.rs`、`tracker_repo.rs`、`infohash_repo.rs` | 加列 `version / origin_node / updated_at / deleted_at` + 索引 + 迁移 |
| P1-2 | 新增 `src/storage/oplog.rs` | oplog 表 + 写入路径挂到业务事务 + 保留窗口裁剪 |
| P1-3 | 新增 `src/federation/sync/delta.rs` | `OpsRequest` / `OpsBatch` 协议 + 幂等应用 + 版本向量持久化 |
| P1-4 | `src/main.rs:1882`、`gossip.rs:920` | 反熵主链切到 Range-based 下钻；旧分层 Merkle 降级为兼容路径 |
| P1-5 | `src/federation/merkle.rs` | 增量维护（写入更新 O(log) 节点）；`l2_shard` 列改存真 L2 + 索引；取消 300 s 常态全表重算 |
| P1-6 | `node_repo.rs:631/869`、`sync/mod.rs:546`、`db.rs:1228/1119` | 删除语义闭环（软删除 + 墓碑落库 + 查询过滤） |
| P1-7 | `src/main.rs:1045`、`db.rs:1562` | `shard_backfill` 改为写入即填，取消周期全表回填 |
| P1-8 | `src/federation/sync/mod.rs:2718` | 删除死代码 `incremental_sync_tick`（或正确接线） |
| P1-9 | `src/federation/sync/mod.rs:106` | `recent_changes` 补写入点，或删除该路径 |

### 7.3 P2 —— 引导通道与增强

| # | 内容 |
|---|---|
| P2-1 | bootstrap 专用通道：`trigger_initial_sync`（`sync/mod.rs:671`）与在线反熵彻底解耦 |
| P2-2 | 快照（`VACUUM INTO`）+ manifest + 分块并行 + 断点续传 + 限流可抢占 |
| P2-3 | 可观测性：6.9 节全部指标 + bootstrap 进度 API |
| P2-4 | （可选）IBLT 集合调和：仅在小 d 场景作为单轮快速路径 |
| P2-5 | 反熵按 repo 差异化（高 churn 的 NODE 更细粒度 / 更短周期） |

---

## 8. 落地路线图与验收指标

### 8.1 阶段划分

| 阶段 | 内容 | 依赖 | 风险 |
|---|---|---|---|
| **S1** | P0-A + P0-B + P0-C | 无（不改 schema） | **低** |
| **S2** | P1-6 删除闭环 + P1-7 回填 + P1-1 schema 迁移 | S1 | 中（需 DB 迁移） |
| **S3** | P1-2 oplog + P1-3 delta 协议 + P1-5 Merkle 增量 | S2 | 中 |
| **S4** | P1-4 Range-based 反熵 | S3 | 中高 |
| **S5** | P2 bootstrap 通道 + 可观测性 | S3 | 中 |

### 8.2 每阶段验收指标

| 阶段 | 验收条件 |
|---|---|
| S1 | ① 单轮 `entries` 从 16.8 万降至 < 1.5 万；② `node_sync_count` 增速下降 ≥ 90%；③ 两端 15 分钟双向流量从约 18 GB 降至 < 1 GB；④ 真实差异（DB 比对）不再扩大 |
| S2 | ① 删除的行在两端 DB 中均可见 `deleted_at`；② 「删了又活」现象消失；③ `shard 0` 积压不再增长 |
| S3 | ① `ops_lag_seq` 稳态 < 10,000；② 稳态带宽 < 5 Mbit/s；③ 重启后不重传历史 |
| S4 | ① 反熵单轮访问节点数 < 50,000；② 差异稳定收敛（不长期停在 100%） |
| S5 | ① 新节点 10⁹ 行 bootstrap < 1 小时；② 中断后可从断点恢复；③ A 侧 CPU / 带宽占用无显著抬升 |

### 8.3 回归基线（当前值，用作对照）

```
node_sync_count      : 2,805,135 / 4,097,953   (45 min)
bytes_sent/recv      : 24.66 / 30.65 GB        (45 min)
单轮 entries         : 168,336                 (elapsed 187.4 s)
单轮差异 L2          : 43,172 / 65,536  (66%)
真实差异             : 8,608 行 / 409,275  (2.1%)
```

---

## 9. 风险与回滚

| 风险 | 缓解 |
|---|---|
| schema 迁移在亿级表上耗时 / 锁表 | 只用 `ADD COLUMN ... DEFAULT`（元数据操作，O(1)）；索引用 `CREATE INDEX` 后台建 |
| Range-based 下钻引入新的往返放大 | 先在**只读诊断模式**灰度（只求差集、不改数据），对比与现有机制的一致性 |
| 删除语义改动引发误删 | 软删除先只写不生效（观察一个版本），确认后再接查询过滤 |
| bootstrap 打垮生产节点 | 限流 + 可抢占 + 仅在显式触发时启动；默认关闭自动 bootstrap |
| oplog 膨胀 | 保留窗口按最小对端进度裁剪；监控表大小告警 |
| 版本兼容（新旧节点混跑） | 协议带能力位协商；`HelloMessage` 已有字段变更史（`4de716a` 加 `timestamp_ms` / `nonce` 曾导致 bincode 硬失败）→ **任何协议字段变更必须全网同步升级** |

**回滚**：S1 全部为阈值与过滤改动，`git revert` 即恢复；S2 起涉及 schema，迁移脚本须配套回滚（保留 `deleted_at` 列不删即可，旧代码忽略新列）。

---

## 10. 与既有文档的关系

| 文档 | 关系 |
|---|---|
| [ADR-004](04-data-model.md) / [06-performance.md](06-performance.md) | 千万级目标；本方案是其向亿级演进时的同步面配套 |
| [07-federation.md](07-federation.md) | **描述的仍是阶段 3 旧架构**（固定 256 分片 + NodeRepo 300 s SyncBatch 广播），需后续单独修订 |
| [09-merkle-async-update.md](09-merkle-async-update.md) | Merkle 异步重算（P1-1）已在本方案 6.6 中收编为「增量维护」的子项 |
| [11-billion-scale-storage.md](11-billion-scale-storage.md) | 解决**存储/内存**的亿级（分表 + LRU + 布隆）；本方案解决**同步**的亿级，两者互补 |
| [artifacts/merkle-diff-analysis.md](../../artifacts/merkle-diff-analysis.md) | 同族分析报告：tracker `disabled` 编码宽度不一致导致的**幻影差异**（与本次回环是两个独立问题） |
| [ADR-006](../../docs/adr/006-federation-sync-convergence.md) | 本方案的决策记录 |

---

## 附录 A：集合调和方案族对比

| 族 | 代表 | 正式名 / 出处 | 传输成本 | 轮数 | 前提 |
|---|---|---|---|---|---|
| 摘要下钻 | Merkle anti-entropy | — | O(差异子树) | 多轮 | 无 |
| 摘要下钻 | **Range-based Set Reconciliation** | Meyer 2022 | O(d·log(N/d)) | 多轮 | key 可排序 |
| 概率结构 | **IBLT** | Invertible Bloom Lookup Table，Eppstein et al. 2011 | O(d) | **单轮** | 需估 d 上界 |
| 概率结构 | Bloom filter | — | O(N) | 两轮 | 有假阳性 |
| 多项式插值 | CPISync | Characteristic Polynomial Interpolation，Minsky et al. 2003 | O(d) | 单轮 | 需估 d 上界 |
| 版本日志 | Version vector / oplog | CRDT delta | O(Δ) | 单轮 | 需 schema + 墓碑 |

**IBLT 在 d = 10⁸ 时的成本**：`m = c·d ≈ 2×10⁸` 桶 × 20 B = **约 4 GB**，需同等内存 —— **因此 IBLT 的适用区间是「大 N、小 d」，d 亿级时反而不用它**。

## 附录 B：容量估算

| 项 | 单条 / 单行 | 10⁸ 行 | 10⁹ 行 |
|---|---|---|---|
| 原始数据 | 约 60 B | 约 6 GB | 约 60 GB |
| 压缩后（LZ4 3–5×） | 约 15–20 B | 约 2–3 GB | 约 25–30 GB |
| 指纹清单（key_fp + data_fp） | 16 B | 1.6 GB | 16 GB |
| IBLT 表（c = 2） | 20 B / 桶 | 4 GB（d = 10⁸） | — |
| L2 摘要（65,536 × u32） | — | 256 KB（与 N 无关） | 256 KB（与 N 无关） |

## 附录 C：术语表

| 术语 | 含义 |
|---|---|
| **N** | 数据集总行数 |
| **d** | 两端对称差的大小（\|A △ B\|） |
| **Δ** | 单轮新增变更量 |
| **W0** | bootstrap 快照对应的 oplog 水位 |
| **L0 / L1 / L2** | Merkle 树层级：1 / 256 / 65,536 |
| **manifest** | bootstrap 分块清单（每块 key 区间 + sha256） |
| **oplog** | 变更日志，记录每次写入操作 |
| **LWW** | Last-Write-Wins，按 `updated_at` 取新 |
| **peeling** | IBLT 解码中「剥离纯桶」的迭代过程 |
