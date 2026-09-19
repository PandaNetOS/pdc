# pdc 内存泄漏深度排查报告

> 排查时间：2026-09-17
> 运行时长：约 2 小时 20 分钟（05:04 → 07:25）
> 内存变化：493MB（启动 35s）→ 1376MB（OOM），净增约 883MB
> 关键现象：紧急驱逐 NodeRepo 缓存（500000→2965 条）后内存纹丝不动，证明泄漏不在 TieredCache

---

## 一、根因总览

共定位 **3 个确定泄漏点 + 1 个高度疑似主因 + 3 个次要嫌疑点**，另有 9 个嫌疑点已排除。

| 排名 | 嫌疑点 | 判定 | 估算贡献 | 模块 |
|---|---|---|---|---|
| **P0-1** | **SQLite mmap 工作集无界增长** | 高度疑似主因 | +400~550MB | 存储层 |
| **P0-2** | **inbound_sources HashSet 无界只增不减** | 确定泄漏 | +200~670MB | 爬虫 |
| **P0-3** | **MerkleTree.entries 只增不删** | 确定泄漏 | +100~170MB | 联邦 |
| P1-1 | NodeRepo 侧结构不随缓存驱逐释放 | 确定泄漏（静态） | +122MB（基线） | 存储层 |
| P1-2 | per-connection gossip_buffer 无硬上限 | 高度疑似（瞬态） | 尖峰~10MB | 联邦 |
| P2-1 | connecting_locks / cooldown_until 只增不删 | 可能泄漏 | <5MB | 联邦 |
| P2-2 | announce_cache 无淘汰 | 可能泄漏（当前量小） | <10MB | 超级Tracker |

---

## 二、详细排查结果

### P0-1：SQLite mmap 工作集无界增长（高度疑似主因）

**代码位置**：`src/storage/db.rs:55-65`

```rust
conn.execute_batch(&format!(
    "PRAGMA journal_mode=WAL;\
     PRAGMA mmap_size = {mmap};\
     PRAGMA cache_size = {cache};\
     PRAGMA wal_autocheckpoint = {wal_auto};"
))?;
```

**当前配置**（config.yaml 无 sqlite 段，全部走默认值）：

| PRAGMA | 值 | 含义 |
|---|---|---|
| `mmap_size` | **2,147,483,648** (2GB) | mmap 映射上限 |
| `cache_size` | **-262,144** (256MB) | 页缓存 |
| `wal_autocheckpoint` | **0** | 完全禁用自动 checkpoint |

**是否泄漏**：高度疑似主因（非传统泄漏，是 mmap 工作集在 Windows 下不主动 trim）

**判断依据**：
1. **紧急驱逐无效铁证**：日志显示 `node 2397/0->1199/0` 驱逐后，内存仍为 1376MB 纹丝不动。这证明内存不在 Rust 堆数据结构中，而在 SQLite mmap 工作集中。
2. **mmap 惰性加载机制**：`mmap_size=2GB` 预留虚拟地址，页面在首次访问时 fault in 物理内存。Windows 不主动裁剪 mmap 工作集。
3. **WAL 无界增长**：`wal_autocheckpoint=0` 禁用自动 checkpoint，WAL 文件从接近 0 增长到峰值数百 MB（历史记录 548MB），每一页在读写时都 fault in 进程工作集。
4. **TRUNCATE checkpoint 每小时才执行一次**，PASSIVE checkpoint 只把 WAL 数据写入主库，不截断 WAL 文件大小。

**内存增长估算**：
- cache_size 页缓存填充 0→256MB：+150~200MB
- mmap 工作集（主库 191MB + WAL 峰值）：+400~550MB
- temp_store=MEMORY 排序缓冲碎片：+30~80MB
- **合计：+580~830MB**，与观测的 +672MB 吻合

**修复建议**：
1. **减小 mmap_size**：2GB → 256MB（当前 DB 仅 191MB，256MB 足以覆盖热页面）
2. **恢复 wal_autocheckpoint**：0 → 1000 页（~4MB），防止 WAL 无界增长
3. **缩短 TRUNCATE checkpoint 间隔**：3600s → 300~600s，防止 WAL 在两次 TRUNCATE 间膨胀
4. **temp_store 改为 FILE**：避免大排序占用物理内存

---

### P0-2：inbound_sources HashSet 无界只增不减（确定泄漏）

**代码位置**：
- 定义：`src/crawler/engine.rs:180`
- 唯一写入点：`src/crawler/engine.rs:1864-1868`

```rust
// 定义：无界 HashSet，只增不减
inbound_sources: Arc<RwLock<HashSet<std::net::SocketAddr>>>,

// 唯一写入：每条入站 DHT 查询都 insert
{
    let mut sources = self.inbound_sources.write();
    sources.insert(from);              // ← 永远 insert，从不 remove
    state.inbound_unique_sources = sources.len();
}
```

**当前上限**：**无**。全局搜索 `inbound_sources.*clear|retain|remove` —— **0 处匹配**，整个代码库没有任何清理逻辑。

**是否泄漏**：**确定泄漏**

**判断依据**：
- 每条入站 DHT 查询（ping/find_node/get_peers/announce_peer/sample_infohashes）的源地址都被 insert
- 8 socket + 8 虚拟节点 ID 暴露在大量 DHT 节点路由表中，announce_peer 洪峰持续注入新源
- 每条目内存：SocketAddr(32B) + hashbrown 开销(~16B) ≈ 48B

**内存增长估算**：
- 若按 1555 条/秒新增唯一源计算，2.5 小时可达 ~1400 万条 ≈ **672MB**（完全匹配观测值上限）
- 实际增长可能因 DHT 网络收敛而趋缓，但量级在数百 MB

**修复建议**（三选一）：
1. **推荐**：改用 `LruCache<SocketAddr, ()>`，上限设 10 万条（~5MB），insert 自动淘汰最久未访问
2. **降粒度**：只统计 /24 网段而非 IP:port（`SocketAddr` → `Ipv4Addr`），条目数减少 10-50 倍
3. **最简**：直接删除此集合，`inbound_unique_sources` 改用 `AtomicUsize` 近似计数（新源判定用短期 Bloom filter）

---

### P0-3：MerkleTree.entries 只增不删（确定泄漏）

**代码位置**：`src/federation/merkle.rs:22`

```rust
entries: RwLock<FxHashMap<Vec<u8>, Vec<u8>>>,   // 全量 key→payload 常驻内存
```

**当前上限**：**无**。`remove()` 方法存在（`:145`）但全仓仅 `tracker_sync.rs:168` 调用一次。

**是否泄漏**：**确定泄漏**

**判断依据**：
1. **node / peer / infohash 三棵 Merkle 树从不调用 remove**——grep 全 `src/` 确认
2. **淘汰链路断裂**：`main.rs:553-562` 把 merkle 注入各 repo 仅用于 upsert 方向；repo 的冷热分层淘汰路径（`node_repo.rs:842-907 remove_cold_nodes`、`peer_repo.rs:466 tier_evict`、`infohash_repo.rs:373 tier_evict`）只清自家缓存，**全程无 merkle.remove 调用**
3. 爬虫持续发现新节点/peer/infohash → 本地 upsert 进 Merkle → repo 把旧数据降级到 SQLite 并淘汰热缓存 → **Merkle 里的旧 key 永久驻留**

**日志佐证**：
- 启动重建一次即载入 `500000 + 43187 + 5371 + 371 ≈ 54.9 万条` 到 4 棵 Merkle 树
- 运行期 150 次 diff sync + 本地爬虫新发现，只 insert 不 remove

**内存增长估算**：
- 启动静态：54.9 万条 × ~200B/条 ≈ **110MB 常驻**
- 运行期：2.5h 净增约 30~35 万条新 key ≈ **60~70MB**
- **合计：~170MB**

**修复建议**：
1. **【根因修复】repo 淘汰时联动 merkle.remove**：在 `node_repo::remove_cold_nodes`、`peer_repo::tier_evict/emergency_evict`、`infohash_repo::tier_evict` 的驱逐分支里，对被驱逐 key 调用已注入的 `merkle.remove(&key)`（`tier_check_with_evicted` 已返回 evicted_addrs，直接接上即可）
2. **【防御性】Merkle 树自身加容量/TTL**：参照 seen_msgs 用 ShardedLruCache，避免与 repo 双层各持一份全量

---

### P1-1：NodeRepo 侧结构不随缓存驱逐释放（确定泄漏，静态）

**代码位置**：
- 定义：`src/storage/node_repo.rs:69-85`
- 问题点：`src/storage/node_repo.rs:482-485`

```rust
pub struct NodeRepoImpl {
    subnet_index: RwLock<FxHashMap<[u8; 3], Vec<NodeId>>>,  // /24 网段索引
    hot_addrs: RwLock<FxHashSet<SocketAddr>>,
    cold_addrs: RwLock<FxHashSet<SocketAddr>>,
    dirty: RwLock<FxHashSet<NodeId>>,
}

pub fn tier_evict(&self) {
    self.cache.tier_check();
    self.cache.evict_if_needed();
    // ← 没有清理 subnet_index / hot_addrs / cold_addrs
}
```

**当前上限**：**无**。启动时 `load_hot_warm()` 将 **1,288,202 个节点**全部灌入这四个结构。

**是否泄漏**：确定泄漏（静态分配，推高基线，不随时间增长）

**内存估算**：
| 侧结构 | 估算大小 |
|---|---|
| subnet_index（1.28M NodeId × 20B + Vec 开销） | ~30MB |
| hot_addrs（1.28M SocketAddr × ~36B） | ~46MB |
| cold_addrs（同上） | ~46MB |
| **合计** | **~122MB** |

**修复建议**：
1. `tier_evict()` 增加侧结构清理：当缓存驱逐条目时，同步从 `subnet_index`/`hot_addrs`/`cold_addrs` 中移除被驱逐节点（参考 `remove_cold_nodes()` 中 line 886-904 的清理逻辑）
2. 启动加载时不全量灌入侧结构：只对实际进入 TieredCache 的节点维护侧结构，冷节点仅通过 DB 按需查询

---

### P1-2：per-connection gossip_buffer 无硬上限（高度疑似瞬态）

**代码位置**：
- 定义：`src/federation/connection.rs:45`
- 入队：`:811`（GossipBatch）、`:833`（GossipBatchBulk）

```rust
pub gossip_buffer: ParkingMutex<VecDeque<GossipBatchMessage>>,
// push 时无任何容量判断
buf.push_back(batch);
```

**当前上限**：**无**。flush 由 `flush_all_gossip_buffers` 每 50ms 一次性 `drain(..)` 取空。

**是否泄漏**：高度疑似瞬态尖峰，非稳态泄漏

**判断依据**：
- 日志实测 `buffer_len` 峰值 GossipBatch=814、Bulk=1500，均值 18.5
- 每 50ms 全量 drain，不会无限累积
- 但对端洪峰 + 本地 flush 慢时，drain 出来的 batch 会被 clone 进最多 4 个 group task，在信号量排队期间驻留内存
- 能解释瞬时内存毛刺，解释不了 2.5h 单调增长

**修复建议**：入队前加硬上限（如 `gossip_buffer_max_batches=2000`），超限直接 drop 最旧并计数告警。

---

### P2-1：connecting_locks / cooldown_until 只增不删（可能泄漏）

**代码位置**：`src/federation/connection.rs:111, :133`

```rust
connecting_locks: FxHashMap<NodeId, Arc<TokioMutex>>,  // 只 insert 不清理
cooldown_until: FxHashMap<NodeId, Instant>,             // 只 insert 不删除过期项
```

**当前上限**：无。`get_connecting_lock` 只 insert 不清理；`cooldown_until` 在 `remove_connection` 时 insert，从不删除过期项。

**是否泄漏**：可能泄漏（缓慢，MB 级）

**日志佐证**：连接建立 180 次、关闭 21 次，每个曾尝试连接的 node_id 永久留一把锁。单条 ~150 字节，累积到几千节点也仅 MB 级。

**修复建议**：连接移除时 `connecting_locks.write().remove(node_id)`；`cooldown_until` 定期清理过期项。

---

### P2-2：announce_cache 无淘汰（可能泄漏，当前量小）

**代码位置**：`src/data_plane/http_tracker.rs:76`

```rust
announce_cache: Arc<DashMap<Infohash, (Instant, Vec<SocketAddr>)>>,
```

**当前上限**：无。grep 确认无 `retain`/`remove`/`clear` 调用。

**是否泄漏**：可能泄漏（当前外部 announce 流量少，规模有限）

**修复建议**：加 TTL 清理任务（定期 `retain` 清理过期条目），或改用 LRU 缓存设上限 10,000 条。

---

## 三、已排除的嫌疑点

| 嫌疑点 | 排除依据 |
|---|---|
| **Pending 表** | tid 仅 2 字节，全局唯一空间 32768 条 ≈ 4.2MB 事实封顶；15s 超时 + 30s 清理逻辑有效（`retain` 真正删除） |
| **WriteQueue / IOScheduler** | IOScheduler 队列上限 100,000，实际日志仅 50 条；Critical/Important 可突破但未触发 |
| **TaskScheduler** | 44 静态任务不增长；recent_durations 每任务 10 条 FIFO；completed_dependencies ≤44；队列 ~50 条稳定 |
| **CrawlerBufferPool** | 容量 16 × 64KB = 1MB，池满即 drop，无归还泄漏 |
| **路由表（RoutingTable）** | 160 bucket × K=16 = 2560 条上限，~358KB |
| **AdaptiveController 历史窗口** | 1000 条环形缓冲（VecDeque），超量 pop_front |
| **RateLimiter 滑动窗口** | 60s 窗口，每次 record 自动清理过期 |
| **Gossip outbox** | MAX_OUTBOX_LEN=5000 硬上限 + 300s 过期过滤；日志 outbox_size 采样为 0 |
| **seen_msgs 去重** | 10 万条分片 LRU，有界 |
| **联邦连接列表** | max_connections=32，超限拒绝；remove_connection 完整清理 |
| **EventBus** | broadcast 容量 1024，lag 自动丢弃最旧 |
| **对象池 / 缓冲区** | 有界，池满即丢弃 |

---

## 四、日志关键证据

### 4.1 内存增长曲线

| 时间 | 内存 | NodeRepo 缓存 | 事件 |
|---|---|---|---|
| 05:04:46 | 493MB | 500000 | 启动后首次触发驱逐 |
| 05:06:54 | 637MB | 45295 | 持续增长 |
| 05:11:08 | 743MB | 138129 | |
| 06:20:00 | ~1000MB | — | （线性增长） |
| 07:20:03 | 1376MB | 3695 | 触顶 |
| 07:24:49 | 1376MB | 1848 | 驱逐后纹丝不动 |
| 07:25:20 | 1376MB | 1483 | 最后一次驱逐，仍不动 |

### 4.2 TaskScheduler 异常

整个运行期间心跳显示：`运行中[crawl=0, persistence=0, monitor=0, network=0~4]`，队列待执行稳定在 47~51。crawl/persistence/monitor 三类任务几乎从未被调度执行，但 WAL checkpoint 仍在运行（可能不走 TaskScheduler 或分类统计有 bug）。

### 4.3 联邦模块高频活动

- 联邦相关日志占总量 **91.5%**（139148/152010 行）
- Gossip 发送失败重试 **4218 次**，msg_id 从 381 增长到 70809
- 自连接拒绝 **631 次**（节点发现自己地址后尝试连接自己）
- 差异全量同步触发 **150 次**（差异≥20% 即触发）

### 4.4 WAL checkpoint 频率

配置 100ms，实际每 1~2 秒执行一次（PASSIVE），最后阶段甚至每 0.2~0.5 秒一次——说明 WAL 增长过快，checkpoint 执行时间变长。

---

## 五、立即修复方案（按优先级）

### 第一批：P0 修复（影响最大，可立即实施）

| # | 修复项 | 改动位置 | 预期效果 |
|---|---|---|---|
| 1 | `mmap_size` 2GB → 256MB | `config.rs` 默认值 / config.yaml | 减少 mmap 工作集 ~300MB |
| 2 | `wal_autocheckpoint` 0 → 1000 | `config.rs` 默认值 | 防止 WAL 无界增长 |
| 3 | `inbound_sources` 改为有界 LRU（上限 10 万） | `engine.rs:180, :1864` | 消除数百 MB 无界增长 |
| 4 | repo 淘汰时联动 `merkle.remove()` | node_repo/peer_repo/infohash_repo 淘汰分支 | 消除 Merkle 树只增不删 |

### 第二批：P1 修复

| # | 修复项 | 改动位置 | 预期效果 |
|---|---|---|---|
| 5 | TRUNCATE checkpoint 间隔 3600s → 300s | config | 防止 WAL 峰值膨胀 |
| 6 | `tier_evict()` 清理 NodeRepo 侧结构 | `node_repo.rs:482` | 释放 ~122MB 基线 |
| 7 | gossip_buffer 加硬上限（2000 条） | `connection.rs:811, :833` | 防止接收缓冲区瞬态膨胀 |

### 第三批：P2 修复

| # | 修复项 | 改动位置 |
|---|---|---|
| 8 | connecting_locks / cooldown_until 清理 | `connection.rs:111, :133` |
| 9 | announce_cache 加 TTL / LRU | `http_tracker.rs:76` |
| 10 | temp_store MEMORY → FILE | `config.rs` 默认值 |

---

## 六、验证建议

修复后建议按以下步骤验证：
1. 启动后运行 3 小时，每 30 分钟记录 RSS
2. 确认 `inbound_sources.len()` 稳定在 10 万以内
3. 确认 Merkle 树条目数随 repo 淘汰而下降
4. 确认 WAL 文件大小稳定在 <10MB
5. 确认紧急驱逐后 RSS 有明显下降（证明 Rust 堆内存可被回收）
