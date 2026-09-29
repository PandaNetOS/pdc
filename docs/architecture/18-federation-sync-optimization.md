# 联邦同步优化方案（D 批）

## 背景

空库冷启动 bootstrap 实测 90 分钟拉完 138 万行，运行中连上数据量更大的对端（333万→396万）后不触发快速补拉，63 万差异靠 range 对账跑 9 小时还没补上。

## 现状分析

### 已有机制
- **ChunkWindow 流水线**：`bootstrap_window_size=16`，同时 16 个块在途
- **四通道**：Gossip（实时）/ Delta（oplog 增量）/ Bootstrap（全量）/ Range Reconcile（对账）
- **策略决策**：`decide_strategies()` 根据 row_count 裁定 NONE/DELTA/BOOTSTRAP

### 问题
1. **Bootstrap 盲拉所有块**：不比较本地 hash，不管本地有没有，69 块全拉
2. **策略阈值方向单一**：`local/peer > 1.2` 只在"我比对端多"时触发 bootstrap。本地 333 万、对端 396 万时（ratio=0.84），对端视角 ratio=1.19 < 1.2，刚好不触发
3. **单源 bootstrap**：只从一个 peer 拉，不利用多对端并行
4. **对端重复查询**：每块做两次 SELECT（key+hash 校验 + 完整行数据），客户端其实从 manifest 已知 hash

## 改造项

### D1: 块级 hash 比较（只拉差异块）

**目标**：客户端收到对端 manifest 后，本地也按相同 chunk_rows 构建 manifest，逐块比较 hash，一致的跳过，只请求不一致的块。

**改动**：
- `sync/mod.rs` `handle_bootstrap_manifest_response()`：
  - 收到对端 manifest 后，调用 `build_repo_manifest_impl()` 构建本地 manifest（spawn_blocking）
  - 逐块比较 `chunk.hash`：
    - hash 一致 → 标记 `done`，不请求
    - hash 不一致 → 加入请求队列
    - 对端有但本地没有的块 → 请求
- `bootstrap.rs`：ChunkWindow 需要支持"跳过已完成块"的初始化（done_chunks 从 hash 比较结果来，而不是全 0）

**收益**：运行中补差异时，已有的块不拉，只补缺的 63 万。如果已有 80% 的块 hash 一致，块数从 69 降到 ~14。

**约束**：
- 本地 manifest 构建是全表扫描（138 万行 ~20s），必须 spawn_blocking
- 本地 manifest 构建期间 bootstrap 暂停，不影响其他连接
- 新增配置 `bootstrap_skip_identical_chunks: bool = true`（可关）

### D2: 策略阈值调整

**目标**：差异大时双向触发 bootstrap，不限方向。

**改动**：`sync/mod.rs` `decide_strategies()`：
```rust
// 当前：
// local/peer > 1.2 && local - peer > threshold → BOOTSTRAP（我推给你）

// 改为：
let ratio = local.max(peer) as f64 / local.min(peer).max(1) as f64;
let diff = local.abs_diff(peer);
if local == 0 && peer > SNAPSHOT_MIN_ROWS {
    // 我空库，你有数据 → 我向你拉
    STRATEGY_BOOTSTRAP
} else if ratio > 1.5 && diff > range_bulk_threshold_rows {
    // 差异 >50% 且差 >1万行 → 多的一方向少的一方推
    STRATEGY_BOOTSTRAP
} else {
    STRATEGY_DELTA
}
```

**收益**：本地 333 万 vs 对端 396 万（ratio=1.19，差 63 万）——ratio<1.5 仍不触发。需要调阈值。

**实际阈值**：考虑到实测 333/396=1.19 差 63 万但补了 9 小时没补上，阈值应降到 **1.3**（差 30% 就触发）。即 ratio > 1.3 且 diff > 50000 时触发块级补拉。

### D3: 对端查询合并

**目标**：对端处理块请求时只做一次 SELECT，不重复算 hash。

**现状**：
1. `load_repo_key_hashes_in_range()`：SELECT id,ip,port + blake3
2. `load_repo_sync_entries_in_range()`：SELECT 完整行

**改动**：
- 客户端请求块时，manifest 里已经有块的预期 hash
- 对端不需要重新算 hash 来校验——直接查完整行返回
- hash 校验在客户端做（客户端收到数据后自己算，和 manifest 里的 hash 对比）
- 删除 `handle_bootstrap_chunk_request` 里的第 ① 步（hash_rows 查询）

**收益**：对端每块少一次 SELECT + 2 万次 blake3，CPU 和 IO 减半。

### D4: 多源并行（后续，本批不做）

当前单源。改为每个连接独立跑 ChunkWindow，块请求轮询分配。本批先做 D1-D3，D4 观察效果后再做。

## 不改动
- ChunkWindow 流水线（已有 window=16）
- Delta/Oplog 通道
- Gossip
- Range Reconcile（保留为兜底）

## 验收
1. `cargo fmt --check` exit 0
2. `cargo clippy --all-targets -- -D warnings` exit 0
3. `cargo test --all` exit 0
4. 新增单测：
   - D1: 本地 manifest 和对端 manifest 比较，一致块跳过
   - D2: ratio=1.3 时触发 bootstrap
   - D3: 对端不重复算 hash
