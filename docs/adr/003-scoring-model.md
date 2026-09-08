# ADR-003: 增量评分模型与脏标记机制

> 状态：✅ 已采纳
> 日期：2026-09-08
> 决策者：项目维护者
> 相关：评分系统、性能优化、千万级节点

## 背景 (Context)

PDC 的 DHT 节点数量持续增长（当前 60,000+，目标 1,000,000+），评分系统面临以下挑战：

1. **全量重算性能问题**：
   - ScoreMaintainer 每 60 秒全量重算所有节点评分
   - 每次重算需要：
     - 全量克隆 60000+ 节点（`repo.all_nodes()`）
     - 逐个计算评分（60000 次计算）
     - 逐个更新评分（60000 次 `repo.update_score()`）
   - 60000 次锁竞争 + 60000 次 SQLite UPDATE
   - 千万级节点时，全量重算可能需要几分钟，不可接受

2. **评分更新延迟**：
   - 新发现的节点要等 60 秒才能有评分
   - 节点统计数据更新后，评分不能及时反映
   - 爬虫在选择节点时，新节点没有评分，可能被低估

3. **无效计算**：
   - 大部分节点的统计数据在 60 秒内没有变化
   - 但全量重算仍然重新计算所有节点的评分
   - 99% 的计算是无效的

4. **千万级节点应对**：
   - 当前 60,000 节点时，全量重算已经有明显的性能开销
   - 目标 1,000,000+ 节点时，全量重算完全不可用
   - 需要一种可扩展的评分模型

## 决策 (Decision)

采用**增量评分模型 + 脏标记机制 + 批量更新**：

### 1. 脏标记 (Dirty Flag)

节点统计数据更新时，自动标记该节点为"脏"（需要重算评分）：

```rust
// NodeRepository trait 新增方法
async fn mark_dirty(&self, addr: &SocketAddr);
async fn dirty_nodes(&self) -> Vec<SocketAddr>;
async fn clear_dirty(&self, addr: &SocketAddr);
async fn clear_all_dirty(&self);
```

**触发脏标记的时机**：
- `record_query(addr, success, latency)` — 节点查询统计更新
- `record_query_with_nodes(addr, latency, nodes)` — 节点查询+产出统计更新
- `set_node_state(addr, state)` — 节点状态变化

**实现**：
```rust
pub struct NodeRepoImpl {
    nodes: RwLock<HashMap<SocketAddr, KBucketEntry>>,
    dirty: RwLock<HashSet<SocketAddr>>,  // 脏标记集合
    storage: Arc<Storage>,
}
```

### 2. 增量重算 (Incremental Rescore)

每 10 秒只重算脏节点，不重算全部：

```rust
impl NodeScorer {
    async fn rescore_dirty(&self, repo: &dyn NodeRepository) -> usize {
        let dirty_addrs = repo.dirty_nodes().await;
        if dirty_addrs.is_empty() {
            return 0;
        }
        let mut scores = Vec::with_capacity(dirty_addrs.len());
        for addr in &dirty_addrs {
            if let Some(node) = repo.get_node(addr).await {
                let score = calculate_node_score(&node);
                scores.push((*addr, score));
            }
        }
        // 批量更新评分
        repo.update_scores_batch(&scores).await;
        // 清除脏标记
        repo.clear_all_dirty().await;
        scores.len()
    }
}
```

### 3. 批量更新 (Batch Update)

一次事务更新所有评分，避免 60000 次锁竞争：

```rust
// NodeRepository trait 新增方法
async fn update_scores_batch(&self, scores: &[(SocketAddr, f64)]);

// 实现
pub fn update_scores_batch_sync(&self, scores: &[(SocketAddr, f64)]) {
    let mut nodes = self.nodes.write();  // 一次写锁
    for (addr, score) in scores {
        if let Some(entry) = nodes.get_mut(addr) {
            entry.score = *score;
        }
    }
}  // 锁自动释放
```

### 4. 全量兜底 (Full Rescore as Fallback)

每 300 秒全量重算一次，确保评分一致性：

- 即使有脏标记遗漏，全量重算也能修正
- 全量重算也使用批量更新，性能可接受
- 60000 节点全量重算 < 5 秒

### 5. 双模式调度

```
ScoreMaintainer.start():
  - 增量任务：每 10 秒 rescore_incremental()
  - 全量任务：每 300 秒 rescore_all()（延迟 300 秒启动，避免与增量竞争）
```

## 后果 (Consequences)

### 正面影响
- **评分更新延迟**：从 60 秒降到 10 秒
- **增量重算性能**：从 O(n) → O(d)，d 为脏节点数（通常 < 1000）
- **锁竞争**：从 60000 次 → 1 次（批量更新）
- **SQLite UPDATE**：从 60000 次 → 1 次事务（批量更新）
- **可扩展性**：千万级节点时，增量重算仍然可用（只重算脏节点）
- **CPU 利用率**：只计算变化的节点，避免 99% 的无效计算

### 负面影响 / 代价
- **实现复杂度**：需要脏标记机制，比全量重算复杂
- **脏标记开销**：每次统计更新都要标记脏，有微小的性能开销（HashSet insert，可忽略）
- **一致性窗口**：评分更新有最多 10 秒的延迟（增量重算间隔）
- **脏标记遗漏风险**：如果某个统计更新路径忘记标记脏，会导致评分不更新（由全量兜底修正）

### 风险
- **脏标记爆炸**：如果短时间内大量节点统计更新，脏节点集合可能很大
- **内存占用**：脏标记集合需要额外的内存（HashSet，每个 SocketAddr 16 字节，60000 节点约 1 MB，可接受）
- **并发安全**：脏标记的并发读写需要正确同步（使用 parking_lot::RwLock）

### 缓解措施
- **脏标记上限**：如果脏节点数超过阈值（如 10000），可以降级为全量重算
- **全量兜底**：每 300 秒全量重算一次，即使有遗漏也能修正
- **单元测试**：为脏标记和批量更新编写单元测试
- **可观测性**：记录每轮增量重算的脏节点数量和重算耗时，监控异常

## 替代方案 (Alternatives)

### 方案 A：保持全量重算
- 优点：简单，无需脏标记机制
- 缺点：性能差，60000 节点逐个更新，千万级时不可用
- 不选择的原因：性能问题严重，不可扩展

### 方案 B：实时评分（统计更新时立即重算）
- 优点：评分更新延迟最低（实时）
- 缺点：频繁的评分计算，CPU 开销大，DHT 爬虫每秒收到很多响应
- 不选择的原因：CPU 开销过大，批量计算更高效

### 方案 C：事件驱动评分（统计更新时发布事件，异步重算）
- 优点：解耦，评分计算异步执行
- 缺点：实现复杂，需要事件总线，可能有事件丢失风险
- 不选择的原因：增量重算已经足够简单高效，事件驱动过于复杂

### 方案 D：分片并行评分（将节点分成多个分片，并行计算）
- 优点：可利用多核 CPU，千万级时性能好
- 缺点：实现复杂，需要分片管理和结果合并
- 不选择的原因：当前阶段增量重算已经足够，分片并行作为未来优化方向

## 千万级节点应对策略

### 当前阶段（60,000 节点）
- 增量重算：每 10 秒，通常 < 1000 个脏节点，耗时 < 100ms
- 全量兜底：每 300 秒，60000 节点，耗时 < 5 秒

### 中期阶段（500,000 节点）
- 增量重算：每 10 秒，通常 < 5000 个脏节点，耗时 < 500ms
- 全量兜底：每 600 秒（延长间隔），500000 节点，耗时 < 30 秒
- 分片并行：可考虑将脏节点分成多个分片，并行计算

### 长期阶段（1,000,000+ 节点）
- 增量重算：每 10 秒，通常 < 10000 个脏节点，耗时 < 1 秒
- 全量兜底：每 1800 秒（30 分钟），1000000 节点，耗时 < 60 秒
- 分片并行：必须采用，将脏节点分成 N 个分片，tokio::spawn 并行计算
- 限流：每轮最多重算 X 个节点，避免 CPU 占满
- 优先级：优先重算最近活跃的节点，冷节点降低重算频率

## 实施计划 (Implementation Plan)

- [x] NodeRepository trait 添加脏标记方法
- [x] NodeRepository trait 添加批量更新方法
- [x] NodeRepoImpl 实现脏标记（dirty: RwLock<HashSet<SocketAddr>>）
- [x] NodeRepoImpl 实现批量更新（update_scores_batch_sync）
- [x] 统计更新时自动标记脏（record_query / record_query_with_nodes / set_node_state）
- [x] NodeScorer 添加 rescore_dirty 方法
- [x] ScoreMaintainer 改为增量+全量双模式
- [x] 单元测试（127 个全部通过）
- [x] 本地运行验证
- [ ] 分片并行计算（未来优化）
- [ ] 限流和优先级（未来优化）

## 验证标准 (Verification Criteria)

- [x] 评分更新延迟 < 10 秒
- [x] 增量重算耗时 < 100ms（60000 节点，通常 < 1000 个脏节点）
- [x] 批量更新一次写锁，无 60000 次锁竞争
- [x] 统计更新后节点被标记为脏
- [x] 增量重算后脏标记被清除
- [x] 全量兜底每 300 秒执行一次
- [x] 127 个单元测试全部通过
- [x] PDC 启动后正常运行，评分系统正常工作

## 参考资料 (References)

- [ADR-002: 智能层统一收口](002-intelligence-layer.md)
- [Dirty Flag Pattern](https://gameprogrammingpatterns.com/dirty-flag.html) — 脏标记模式
- [Batch Processing](https://en.wikipedia.org/wiki/Batch_processing) — 批量处理
- [Incremental Computing](https://en.wikipedia.org/wiki/Incremental_computing) — 增量计算

## 变更记录 (Changelog)

| 日期 | 版本 | 变更内容 | 作者 |
|---|---|---|---|
| 2026-09-08 | 1.0 | 初始版本，记录增量评分模型与脏标记机制决策 | 项目维护者 |
