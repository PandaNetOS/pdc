# Merkle 异步更新优化方案（P1-1）

> 状态：方案设计，待实施（因 `src/federation/sync/` 目录与其他任务冲突，暂不实施代码）
> 日期：2026-09-10
> 预期收益：+30% 联邦同步吞吐量

## 1. 问题分析

### 1.1 当前实现瓶颈

`src/federation/merkle.rs` 中的 `MerkleTree::recompute_shard` 方法是同步阻塞调用：

```rust
fn recompute_shard(&self, shard: u16) {
    let entries = self.entries.read();
    // O(N) 全表扫描，过滤出本分片条目
    let mut shard_entries: Vec<(&Vec<u8>, &Vec<u8>)> = entries
        .iter()
        .filter(|(k, _)| self.shard_for_key(k) == shard)
        .collect();
    shard_entries.sort_by(|a, b| a.0.cmp(b.0));
    // blake3 哈希计算
    let mut hasher = blake3::Hasher::new();
    for (key, data) in &shard_entries { ... }
    // 写回 roots + entry_counts
}
```

每次 `update()` 或 `update_batch()` 都会同步调用 `recompute_shard()`，其开销为：
- **O(N) 全表扫描**：N = 该 repo 总条目数（千万级时，单次扫描遍历百万级 entry）
- **排序**：本分片条目排序（平均分片大小 = N / shard_count）
- **blake3 哈希**：逐条 key+data 哈希
- **持读锁**：整个扫描+哈希期间持有 `entries.read()`，阻塞其他写操作

### 1.2 对联邦同步的影响

在联邦 Gossip 同步流程中：
1. 接收端 `handle_gossip_batch` → `apply_*_sync` → repo 写入 → `merkle.update_batch()`
2. `update_batch()` 同步调用 `recompute_shard()`，阻塞当前 task
3. 千万级数据下，单次 `recompute_shard` 可能耗时数毫秒至数十毫秒
4. 4 个 repo 并行接收时，Merkle 重算成为 CPU 瓶颈之一

### 1.3 全量同步期间的现有优化

当前已有 `full_sync_in_progress` 标志：全量期间 `update/update_batch` 跳过 `recompute_shard`，结束后 `rebuild_all()` 一次性重算。但**增量 Gossip 同步期间**（非全量），每次更新仍同步重算。

## 2. 优化方案：异步脏分片重算

### 2.1 核心思路

将 `recompute_shard` 从同步调用改为**异步延迟重算**：
1. `update/update_batch` 只写入 `entries`，并将受影响分片标记为 "dirty"
2. 后台独立 task 定期扫描 dirty 分片，批量重算
3. 同一分片在一个时间窗口内的多次更新只触发一次重算（合并效应）
4. `digest()` 调用时如遇 dirty 分片，可选同步重算或返回略旧数据（对账场景可接受最终一致）

### 2.2 数据结构变更

```rust
pub struct MerkleTree {
    shard_count: u16,
    roots: RwLock<Vec<[u8; 32]>>,
    entry_counts: RwLock<Vec<u32>>,
    entries: RwLock<FxHashMap<Vec<u8>, Vec<u8>>>,
    full_sync_in_progress: AtomicBool,

    // === 新增 ===
    /// 脏分片位图：bit=1 表示该分片需要重算
    dirty_shards: RwLock<Vec<bool>>,  // 或 AtomicBool 数组
    /// 异步重算通知器：有新 dirty 分片时通知后台 task
    dirty_notify: tokio::sync::Notify,
    /// 后台 task 关闭信号
    shutdown: tokio::sync::broadcast::Sender<()>,
}
```

### 2.3 核心方法变更

#### update / update_batch（非全量期间）

```rust
pub fn update_batch(&self, entries: &[(&[u8], &[u8])]) {
    if entries.is_empty() { return; }
    // 1. 一次写锁批量插入（不变）
    let mut map = self.entries.write();
    let mut affected_shards: FxHashSet<u16> = FxHashSet::default();
    for (key, data) in entries {
        let shard = self.shard_for_key(key);
        affected_shards.insert(shard);
        map.insert(key.to_vec(), data.to_vec());
    }
    drop(map);

    // 全量期间跳过重算（不变）
    if self.full_sync_in_progress.load(Ordering::SeqCst) {
        return;
    }

    // 2. 标记 dirty 分片（替代同步 recompute_shard）
    let mut dirty = self.dirty_shards.write();
    for shard in &affected_shards {
        dirty[*shard as usize] = true;
    }
    drop(dirty);
    // 通知后台 task 有新脏分片
    self.dirty_notify.notify_waiters();
}
```

#### 后台重算 task

```rust
async fn recompute_worker(self: Arc<Self>, mut shutdown_rx: broadcast::Receiver<()>) {
    loop {
        tokio::select! {
            // 等待 dirty 通知或定期检查（最长 100ms）
            _ = tokio::time::timeout(
                Duration::from_millis(100),
                self.dirty_notify.notified()
            ) => {}
            _ = shutdown_rx.recv() => break,
        }

        // 收集并清空所有 dirty 分片
        let dirty_shards: Vec<u16> = {
            let mut dirty = self.dirty_shards.write();
            let shards: Vec<u16> = dirty.iter().enumerate()
                .filter(|(_, &d)| d)
                .map(|(i, _)| i as u16)
                .collect();
            for d in dirty.iter_mut() { *d = false; }
            shards
        };

        // 逐个重算（recompute_shard 内部用读锁，不阻塞写）
        for shard in dirty_shards {
            self.recompute_shard(shard);
        }
    }
}
```

#### digest（对账查询）

```rust
pub fn digest(&self, repo_type: u8) -> MerkleDigestMessage {
    // 方案 A（强一致）：如有 dirty 分片，同步重算后再返回
    // let dirty = self.dirty_shards.read();
    // if dirty.iter().any(|&d| d) { /* 同步重算所有 dirty */ }

    // 方案 B（最终一致，推荐）：直接返回当前 roots，对账差异由 MerkleRepair 兜底
    // 反熵周期（默认 300s）远大于重算延迟（<100ms），不会产生持续误报
    MerkleDigestMessage {
        repo_type,
        shard_count: self.shard_count,
        roots: self.roots.read().clone(),
        entry_counts: self.entry_counts.read().clone(),
    }
}
```

**推荐方案 B**：反熵对账周期为 300 秒，而异步重算延迟 < 100ms，对账时数据几乎必然已收敛。即使极端情况下 digest 读到略旧数据，MerkleRepair 机制也会通过分片级修复兜底，不会导致数据丢失。

### 2.4 初始化与生命周期

```rust
impl MerkleTree {
    pub fn new(shard_count: u16) -> Self {
        let count = shard_count.max(1) as usize;
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            shard_count: shard_count.max(1),
            roots: RwLock::new(vec![[0u8; 32]; count]),
            entry_counts: RwLock::new(vec![0u32; count]),
            entries: RwLock::new(FxHashMap::default()),
            full_sync_in_progress: AtomicBool::new(false),
            dirty_shards: RwLock::new(vec![false; count]),
            dirty_notify: tokio::sync::Notify::new(),
            shutdown: shutdown_tx,
        }
    }

    /// 启动异步重算后台 task（需在 tokio runtime 中调用）
    pub fn spawn_recompute_worker(self: Arc<Self>) {
        let shutdown_rx = self.shutdown.subscribe();
        tokio::spawn(async move {
            self.recompute_worker(shutdown_rx).await;
        });
    }
}
```

**集成点**：在 `SyncManager::new()` 或各 `*_sync::new()` 中创建 MerkleTree 后，调用 `Arc::new(merkle).spawn_recompute_worker()`。需确保 MerkleTree 始终通过 `Arc` 共享。

### 2.5 全量同步期间的行为

全量同步期间（`full_sync_in_progress = true`）：
- `update/update_batch` 仍跳过重算（不变），也不标记 dirty
- `rebuild_all()` 在全量结束时同步重算所有分片（不变）
- 后台 worker 运行但无 dirty 分片可处理，空转等待

全量结束后，`rebuild_all()` 已重算所有分片，dirty 位图为空，后台 worker 继续处理增量更新。

## 3. 性能分析

### 3.1 合并效应

假设某分片在 100ms 窗口内收到 50 次更新：
- **同步模式**：50 次 `recompute_shard`，每次 O(N) 扫描 → 50 × O(N)
- **异步模式**：1 次 `recompute_shard`（窗口结束时）→ 1 × O(N)

在 Gossip 批量同步场景下，同一分片在短时间内会收到大量 batch，合并效应显著。

### 3.2 锁竞争减少

- **同步模式**：`recompute_shard` 持有 `entries.read()` 期间，`update` 的 `entries.write()` 被阻塞
- **异步模式**：`update` 只在插入时短暂持有写锁（微秒级），重算在后台独立进行

### 3.3 预期收益

| 场景 | 同步模式耗时 | 异步模式耗时 | 提升 |
|---|---|---|---|
| 单分片 100 次连续更新 | 100 × O(N) | 1 × O(N) + 标记开销 | ~99% 重算次数减少 |
| 4 repo 并行 Gossip 接收 | 重算阻塞接收 task | 接收 task 无阻塞 | +30% 吞吐量 |
| digest 对账查询 | 无影响（读 roots） | 无影响（读 roots） | 持平 |

## 4. 风险与缓解

### 4.1 对账数据短暂不一致

**风险**：digest() 可能返回尚未重算的旧 roots，导致对账误报差异。
**缓解**：
- 反熵周期 300s ≫ 重算延迟 < 100ms，实际几乎不会遇到
- MerkleRepair 分片级修复兜底，误报差异会被修复请求验证（请求方返回分片条目，接收方比对后无差异则忽略）
- 如强一致需求，可在 digest 中加 `if dirty { sync_recompute() }` 开关

### 4.2 后台 task 生命周期管理

**风险**：MerkleTree 被 drop 时后台 task 未正确关闭，导致内存泄漏。
**缓解**：
- 使用 `broadcast::Sender` 作为关闭信号，MerkleTree drop 时 sender 自动 drop，receiver 收到错误退出
- 或实现 `Drop` trait 显式发送关闭信号

### 4.3 dirty 位图并发安全

**风险**：update 标记 dirty 与 worker 清空 dirty 之间的竞态。
**缓解**：
- worker 在清空 dirty 位图后、重算期间，新的 update 会重新标记 dirty 并 notify
- 最坏情况：某分片在 worker 清空后、重算完成前被更新 → 该分片被重新标记为 dirty → 下一轮 worker 重算 → 最终一致
- 不会丢失更新（每次 update 都设置 dirty=true，幂等）

### 4.4 与现有 full_sync_in_progress 的交互

**风险**：全量期间不标记 dirty，全量结束后 rebuild_all 重算所有分片，但 rebuild_all 期间的增量更新可能被遗漏。
**缓解**：
- `handle_full_sync_complete` 中先 `set_full_sync_in_progress(false)`，再 `rebuild_all()`
- rebuild_all 之后的增量更新正常走异步路径
- rebuild_all 期间（同步重算所有分片，耗时较长）到达的 Gossip 消息会被 `full_sync_in_progress=false` 路径处理，标记 dirty 后由后台 worker 重算
- 实际上 rebuild_all 已经重算了全量数据，后续增量更新的 dirty 标记是正确的

## 5. 实施步骤

1. **修改 `merkle.rs`**：新增 `dirty_shards`、`dirty_notify`、`shutdown` 字段；修改 `update/update_batch` 为标记 dirty；新增 `recompute_worker` 和 `spawn_recompute_worker`
2. **修改 `sync/mod.rs` 和各 `*_sync.rs`**：MerkleTree 创建后调用 `spawn_recompute_worker()`
3. **配置化**：新增 `merkle_recompute_interval_ms` 配置字段（默认 100ms），带 `#[serde(default)]`
4. **单元测试**：验证异步重算最终一致性、dirty 合并效应、全量期间行为
5. **编译验证**：`cargo build --release` 通过
6. **性能测试**：对比优化前后联邦同步速率

## 6. 与其他优化的关系

| 优化项 | 关系 |
|---|---|
| P0-1 接收端多线程并行 | 互补：P0-1 并行处理 batch，P1-1 减少每个 batch 的 Merkle 重算阻塞 |
| P0-2 batch_size 提升 | 互补：更大 batch → 单次 update_batch 覆盖更多分片 → 异步合并效应更显著 |
| P1-2 Gossip 动态限流 | 独立：发送端优化，不影响接收端 Merkle 重算 |
| P2 发送端零间隔 | 独立：发送端优化 |
| 全量同步惰性重算（已有） | 共存：全量期间走惰性路径，增量期间走异步路径 |

## 7. 后续可选优化

- **分片级 entries 索引**：当前 `recompute_shard` 做 O(N) 全表扫描。可维护 `shard → Vec<key>` 的倒排索引，将重算降为 O(shard_size)。但这会增加内存开销和更新时的索引维护成本，需评估。
- **增量哈希**：维护分片内有序条目列表，更新时增量调整哈希（类似 balanced hash tree）。复杂度高，收益有限，暂不考虑。
- **多 worker 并行重算**：当前单 worker 顺序重算 dirty 分片。可改为多 worker 并行（每个 worker 处理不同分片），进一步利用多核。但需注意 `recompute_shard` 内部都获取 `entries.read()`，读锁可共享，并行是安全的。
