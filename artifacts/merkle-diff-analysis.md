# PDC Merkle 树分片幻影差异分析报告

> 现象：TrackerRepo 两边数据完全一致，但分层 Merkle 对比仍报告 **375 个差异 L2 分片**。
> 结论：这不是数据不一致，而是 **merkle 哈希计算公式在两条重算路径上不一致**——同一个 tracker 行，被增量重算和全量冷重算算出两种不同的 data_hash，导致同一行数据在两边处于"不同编码版本"的重算相位时产生幻影差异。

---

## 一、根因定位（具体到函数 + 行号）

### 根因（主因）：tracker `disabled` 字段在两条 DB 加载路径上编码宽度不一致

Tracker 的 data_hash 公式为 `blake3(url || disabled_le || last_used_le)`。
系统中存在**两个**从 DB 读 tracker 并计算 data_hash 的函数，但 `disabled` 的字节宽度不同：

| 函数 | 文件:行 | disabled 编码 | 字节数 |
|---|---|---|---|
| `Storage::load_all_tracker_keys_hashes` | `src/storage/db.rs:951` | `(disabled as u16).to_le_bytes()` | **2 字节** |
| `Storage::load_tracker_keys_hashes_by_shards` | `src/storage/db.rs:1818` | `disabled.to_le_bytes()`（disabled 是 `i64`） | **8 字节** |
| 线上线格式 `build_tracker_sync_entry` | `src/federation/sync/tracker_sync.rs:46-49` | `disabled_u16.to_le_bytes()` | **2 字节（规范值）** |

`disabled` 在 SQLite 中是 `INTEGER`（0/1），两个加载函数都把它读成 `i64`：

- `db.rs:951`（全量冷重算用）：`(disabled as u16).to_le_bytes()` → 对 `disabled=1` 产出 `[01 00]`（2 字节）。
- `db.rs:1818`（增量重算用）：`disabled.to_le_bytes()` → 对 `disabled=1` 产出 `[01 00 00 00 00 00 00 00]`（8 字节）。

两者之间相差 **6 个 `0x00` 字节**，blake3 输入完全不同 → data_hash 必然不同 → L2 哈希不同 → L1 哈希不同 → L0 根不同。

### 两条重算路径分别调用哪个加载函数

| 路径 | 任务名 | 周期 | 调用点 | 加载函数 | disabled 宽度 |
|---|---|---|---|---|---|
| 增量重算 | `merkle_incremental_tracker` | **10s** | `src/main.rs:2482` | `load_tracker_keys_hashes_by_shards` | **8 字节（错）** |
| 全量冷重算 | `merkle_cold_rebuild_tracker` | **300s** | `src/main.rs:2664` | `load_all_tracker_keys_hashes` | **2 字节（对，与线上一致）** |

- 增量路径（`main.rs:2482-2491`）：`take_dirty_shards()` → `load_tracker_keys_hashes_by_shards(&shards)` → 逐 L1 `recompute_shard_from_db`。
- 冷重算路径（`main.rs:2664-2667`）：`load_all_tracker_keys_hashes()` → `rebuild_cold_from_db`。

### 为什么"数据没变"却有 375 个差异

两棵树跑同一份代码，但每个 L2 分片的当前哈希值取决于"**最近一次是哪条路径重算它的**"：

1. 冷重算（300s）把所有 tracker L2 用 **2 字节**编码算一遍 → 哈希正确。
2. 任意一次写入（gossip apply / 分片同步接收 / tracker 发现）都会把对应 L2 标 dirty。
3. 10s 后增量任务把这些 dirty L2 用 **8 字节**编码重算 → 这些 L2 哈希被"翻转"成错误版本。
4. 错误版本会一直保持到下一次 300s 冷重算才被纠正。

两个节点的 10s/300s 任务相位、初始延迟（增量 30s / 冷重算 105s，见 `main.rs:2471`、`main.rs:2654-2657`）互不相同，因此在任意一次对比时刻：
- 节点 A 的某 L2 最近被"冷重算"（2 字节），
- 节点 B 的同一 L2 最近被"增量重算"（8 字节），
- → 两边数据行完全相同，哈希却不同 → **幻影差异**。

375 = 两个节点处于"不同重算相位"的 tracker 承载 L2 分片数（tracker 数量 × L2 分布）。这与 tracker 行数无关，只与"哪些分片最近被增量路径碰过"有关。

---

## 二、五个排查方向的结论

### 方向 1：dirty 分片清理逻辑 —— **是（存在次要 bug，非主因）**

**结论：take 语义本身正确，但存在两个并发消费者抢同一个 dirty 集合。**

- `take_dirty_l2_shards()`（`merkle.rs:282-284`）和 `take_dirty_shards()`（`merkle.rs:287-293`）都是 `std::mem::take(&mut *dirty_l2.write())`，即**取出并清空**，不是 peek。重算失败时 dirty 集合已被取空，本次不会重试——但因为下一轮冷重算（300s）会兜底全量覆盖，所以不会永久丢失。
- **真正的问题**：有**两个不同任务**对同一个 `dirty_l2` 集合做 take：
  1. `merkle_incremental_tracker` 任务（`main.rs:2477`）：`merkle.take_dirty_shards()`，目的是**重算本地哈希**。
  2. `incremental_sync_tick`（`sync/mod.rs:2603`）：`merkle.take_dirty_l2_shards()`，目的是**把 dirty L2 推送给对端**。
- 两者谁先跑谁拿走 dirty 集合并清空；后跑者拿到空集。如果 `incremental_sync_tick` 先跑，它把 dirty 集合拿走用于网络推送，本地哈希**本轮就不会被重算**（增量任务拿到空集直接 return，`main.rs:2478-2480`），本地哈希变 stale，直到 300s 冷重算。
- 这会**放大**主因：让本就错误的哈希更长时间保持错误相位。但它不改变"同一行算出两种哈希"这一事实，故为次要问题。

### 方向 2：时间差 / 更新不同步 —— **是（放大器，非根因）**

- 增量重算 10s，冷重算 300s（`config.rs:392-397`）。
- 两任务初始延迟不同（增量 30s、冷重算 105s，`main.rs:2471` / `main.rs:2654-2657`），两节点启动时刻也不同，相位必然错开。
- 正是这种相位差，让"同一数据、两种编码"在对比时刻同时暴露。若两节点严格同步（不可能），反而可能不暴露。
- 结论：时间差不是根因，而是让编码 bug 稳定可见的触发条件。

### 方向 3：数据加载一致性（范围/分页/过滤/排序） —— **否（范围一致，问题在逐行 hash）**

- SQL 过滤：`load_tracker_keys_hashes_by_shards` 用 `WHERE shard IN (...)`（`db.rs:1805`），`shard` 列在写入时由 `compute_shard(url)` 落库（`db.rs:829`、`db.rs:875`）。
- Rust 二次过滤：`main.rs:2487` `merkle.shard_for_key(k) == shard`，`shard_for_key`（`merkle.rs:170-174`）与 `compute_shard`（`db.rs:16-20`）公式完全相同（均为 `blake3(key) 前两字节 LE % 256`）。
- 两路径选出的行集合完全一致；`last_used` 为 NULL 时两条路径都用 `unwrap_or(0)`（`db.rs:943` vs `db.rs:1819`），一致。
- **问题不在选行，而在逐行算 data_hash 时 disabled 宽度不同**（见根因）。
- 排序：`compute_shard_root`（`merkle.rs:199-201`）统一按 key 字节序排序，两条重算路径共用，一致。

### 方向 4：hash 计算一致性 —— **是（根因就在这里）**

- key 排序：一致（`merkle.rs:201`）。
- blake3 输入结构：`blake3(key) ++ data_hash` 累加，一致（`merkle.rs:203-208`）。
- **不一致点**：`data_hash` 内部的 `disabled` 字段宽度——
  - 规范（线上 `tracker_sync.rs:46-49`、冷重算 `db.rs:951`）：u16 = **2 字节**。
  - 增量 `db.rs:1818`：i64 = **8 字节**。
- 空分片处理：一致。`recompute_l1_from_db`（`merkle.rs:316-318`）、`rebuild_all_from_db`（`merkle.rs:388-390`）对无数据 L2 都写 `[0u8;32]`，与 `recompute_l1`（`merkle.rs:92-96`）空 L1 规范化为全零一致。
- `last_used`/`last_seen`：非负时间戳，i64 LE 与 u64 LE 字节相同，无差异。
- 结论：**key 排序、累加结构、空分片、last_used 全部一致；唯一不一致就是 disabled 的 2 字节 vs 8 字节。**

### 方向 5：增量更新 vs 全量重建一致性 —— **是（根因的直接表现）**

- 这正是现象本身：同一批 tracker 行，
  - 走全量冷重建（`load_all_tracker_keys_hashes`，2 字节）得一套哈希；
  - 走增量重算（`load_tracker_keys_hashes_by_shards`，8 字节）得另一套哈希；
  - 两者**不相等**，且与线上格式（2 字节）对比，增量路径是错的。
- 单元测试 `test_incremental_vs_full_rebuild_consistency`（`merkle.rs:1110-1147`）**没有覆盖这个 bug**：它的"增量"侧最终也调用 `rebuild_all_from_db`（`merkle.rs:1125`），根本没走 `load_tracker_keys_hashes_by_shards` 这条 DB 加载路径，所以测不出两个 loader 的分歧。
- 结论：增量与全量对同一数据算出不同 hash —— 确认成立，即根因。

---

## 三、问题点清单

| # | 严重度 | 位置 | 问题 |
|---|---|---|---|
| P0 | **根因** | `src/storage/db.rs:1818` | `load_tracker_keys_hashes_by_shards` 把 `disabled`（i64）直接 `.to_le_bytes()`（8 字节），与 `load_all_tracker_keys_hashes`（`db.rs:951`，u16 2 字节）及线上格式（`tracker_sync.rs:46`，u16 2 字节）不一致。同一行算出两种 data_hash。 |
| P1 | 放大器 | `src/main.rs:2477` 与 `src/federation/sync/mod.rs:2603` | `take_dirty_shards()` 与 `take_dirty_l2_shards()` 两个任务并发竞争同一个 `dirty_l2` 集合，互相偷 dirty 标记；被 sync_tick 拿走后本地哈希本轮不重算，stale 最长 300s。 |
| P2 | 一致性隐患 | `src/federation/sync/tracker_sync.rs:273`、`:316`、`do_full_sync:121` | 线上构造 payload 时 `last_seen = last_used.unwrap_or(now)`（NULL 取当前时间），而 DB 重算两条路径都是 `unwrap_or(0)`。若某 tracker `last_used` 为 NULL，发出的 data_hash 用 `now`，对端落库后重算用 0，仍会不一致（仅影响 NULL 行，不是本次 375 的主因，但需一并对齐）。 |
| P3 | 测试盲区 | `src/federation/merkle.rs:1110` | 增量 vs 全量一致性测试未走 DB loader，无法发现 `load_all_tracker_keys_hashes` 与 `load_tracker_keys_hashes_by_shards` 的分歧。 |
| P4 | 架构一致性 | `src/main.rs:2484-2490` | 增量重算用 `recompute_shard_from_db`（整 L1 256 个 L2 全量重算），而非新设计的 `recompute_l2_subset_from_db`（仅重算 dirty L2）。非正确性 bug，但与"精准 L2"设计不符，且整 L1 重算会把同一错误编码扩散到该 L1 下所有 L2。 |

---

## 四、修复方案建议（不改动代码，仅建议）

### 必做（P0，直接消除根因）

统一 `disabled` 的编码宽度为 **u16（2 字节）**，与线上 `build_tracker_sync_entry` 及 `load_all_tracker_keys_hashes` 对齐：

```rust
// src/storage/db.rs:1818，将
data_hash.extend_from_slice(disabled.to_le_bytes().as_slice());
// 改为
data_hash.extend_from_slice(&(disabled as u16).to_le_bytes());
```

修完后，增量路径与冷重算路径对同一行算出相同 data_hash，幻影差异消失。建议顺手把 `disabled` 在 DB 层就定义为 `u16`/`bool`，杜绝再出现 i64/u16 混用。

### 强烈建议（P1，消除 dirty 偷取）

让"重算本地哈希"和"推送对端"两个消费者**不竞争同一 dirty 集合**，二选一：

- 方案 A：`incremental_sync_tick` 不再 `take_dirty_l2_shards()`，改为只读快照（peek 或 `dirty_l2_count` + 单独的"已推送"游标），重算任务负责清空；
- 方案 B：由重算任务在重算完成后，把"刚重算的 dirty L2"交给 sync 层去推送（生产者-消费者解耦），而不是各自 take。

### 建议（P2，对齐 NULL last_used 语义）

统一 NULL `last_used` 的取值：DB 重算与线上 payload 构造应使用同一默认值（要么都 0，要么都 now）。推荐 DB 落库时就把 NULL 规范化为 0/某固定值，避免运行时依赖 `now`。

### 建议（P3，补测试）

新增一个**走真实 DB loader** 的一致性测试：用内存 SQLite 写入若干 tracker 行，分别调用 `load_all_tracker_keys_hashes` 与 `load_tracker_keys_hashes_by_shards`，断言两者产出的 `(key, data_hash)` 完全一致；并断言与 `build_tracker_sync_entry` 的 data_hash 一致。该测试在 P0 修复前必然失败，修复后转绿，可防回归。

### 可选（P4）

增量重算迁移到 `recompute_l2_subset_from_db`，只重算真正 dirty 的 L2，减少错误编码（修复前）/无效计算（修复后）的扩散面。

---

## 附：关键调用链速查

```
写入/接收 sync
  └─ merkle.update / update_incremental_batch / mark_dirty_l2   (merkle.rs:457/427/274)
       └─ dirty_l2.insert(l2)                                    (只标脏，不算 hash)

后台增量任务 merkle_incremental_tracker  (main.rs:2456, 10s)
  └─ merkle.take_dirty_shards()                               (merkle.rs:287, 清空 dirty)
  └─ storage.load_tracker_keys_hashes_by_shards(&shards)      (db.rs:1796) ← 【P0: disabled=i64 8字节】
  └─ merkle.recompute_shard_from_db → recompute_l1_from_db     (merkle.rs:303)

后台冷重算 merkle_cold_rebuild_tracker  (main.rs:2639, 300s)
  └─ storage.load_all_tracker_keys_hashes()                    (db.rs:936) ← 【disabled=u16 2字节，正确】
  └─ merkle.rebuild_cold_from_db → rebuild_all_from_db         (merkle.rs:376)

增量网络推送 incremental_sync_tick  (sync/mod.rs:2576)
  └─ merkle.take_dirty_l2_shards()                            (merkle.rs:282, 与上面竞争同一集合)
  └─ start_shard_sync → 把 dirty L2 推给对端
```
