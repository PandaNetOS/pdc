# PDC 亿级数据存储架构方案

> ⚠️ **本文已过时（2026-09-22）：Merkle 树已从 pdc 全面移除。** 涉及 Merkle 的描述已失效（其余存储设计不受影响）。
>
> 现行同步架构以 [ADR-007 Range 反熵唯一兜底](../adr/007-range-only-anti-entropy.md) 与 [AGENTS.md §联邦同步架构 v8](../../AGENTS.md) 为准；
> 下文涉及 Merkle 对账 / 分片同步引擎 / DiffSync / FullSync 的描述仅作历史参考，不代表当前代码。

> 文档编号：PDC-ARCH-011
> 版本：v1.0
> 日期：2026-09-14
> 状态：设计阶段

## 一、背景与目标

### 1.1 当前架构

当前 PDC 采用「内存 FxHashMap + SQLite 增量持久化」架构：
- 四个核心 Repo（Node/Peer/Infohash/Tracker）均为全量内存存储
- 内存无容量上限，长期运行持续增长
- 冷热分层只做判定，未执行实际驱逐
- SQLite 单表存储，千万级数据查询可接受

### 1.2 亿级挑战

| 维度 | 千万级（当前目标） | 亿级（新目标） | 差距 |
|---|---|---|---|
| 数据量 | 1000万 | 1亿 | 10x |
| 单表查询 | SQLite 可接受 | 单表索引膨胀，查询慢 | 需分表/分区 |
| 内存占用 | 热+温 ~2GB | 不可能全放内存 | 严格冷热分离 |
| 写入吞吐 | 写入队列可支撑 | LSM-Tree 更优 | 评估存储引擎 |
| 查询延迟 | <100μs（内存） | 缓存 miss 时需 <1ms | 多层缓存 |

### 1.3 设计目标

- **数据永久保存**：SQLite 是唯一真实来源，内存只是缓存
- **内存可控**：L1 缓存 + L2 布隆过滤器，总内存 < 2GB
- **查询高效**：缓存命中 <50μs，缓存 miss <1ms
- **写入高吞吐**：分表并行写入，目标 10万条/秒
- **可扩展**：分表数、缓存容量、驱逐策略均可配置

---

## 二、总体架构

### 2.1 三层缓存架构

```
查询请求
    ↓
┌─────────────────────────┐
│ L1: 分片 LRU 缓存        │  ← 热数据，容量可配（如 500万）
│ ShardedLruCache          │     32 分片，减少锁竞争
└─────────┬───────────────┘
          │ miss
          ↓
┌─────────────────────────┐
│ L2: 布隆过滤器           │  ← 快速判断"不存在"，避免磁盘查询
│ Bloom Filter             │     1亿条 ~ 120MB，误判率 1%
└─────────┬───────────────┘
          │ 可能存在
          ↓
┌─────────────────────────┐
│ L3: SQLite 分表          │  ← 永久存储，按需加载
│ 256 张表 + 冷热分离       │
└─────────────────────────┘
```

### 2.2 核心原则

1. **写穿透（Write-through）**：写入时同时写内存缓存 + SQLite，保证一致性
2. **读穿透（Read-through）**：查询时先查内存，miss 时从 SQLite 加载到内存
3. **LRU 驱逐**：内存超过上限时，驱逐最久未访问的数据
4. **数据库永不删除**：驱逐只清内存，SQLite 数据永久保留
5. **分表并行**：256 张表分散写入和查询压力

---

## 三、存储引擎选型

### 3.1 方案对比

| 方案 | 写入 | 查询 | 事务 | 生态 | Rust 支持 |
|---|---|---|---|---|---|
| **SQLite + 分表** | 中（WAL） | 中（需索引） | 强 | 成熟 | rusqlite |
| **RocksDB** | 高（LSM） | 高（范围查询） | 弱（单key） | 成熟 | rust-rocksdb |
| **Sled** | 高（LSM） | 中 | 弱 | 新兴 | sled |

### 3.2 推荐方案

**SQLite 分表 + 可选 RocksDB 过渡**

理由：
1. 现有代码深度依赖 SQLite（Storage 层、事务、SQL 查询）
2. 分表方案改动可控，风险低
3. RocksDB 没有 SQL，需要重写所有查询逻辑，风险高
4. 后期如果写入成为瓶颈，可将写入路径迁移到 RocksDB，SQLite 保留查询

---

## 四、分表策略

### 4.1 按 Key 哈希分表（256 张表）

```
infohash_00, infohash_01, ..., infohash_ff  (256张表)
peer_00, peer_01, ..., peer_ff
node_00, node_01, ..., node_ff
tracker_00, tracker_01, ..., tracker_ff
```

### 4.2 分表规则

| 数据类型 | 分表键 | 分表规则 |
|---|---|---|
| Infohash | infohash | 取 hash 第一个字节 mod 256 |
| Peer | infohash + addr | 哈希后 mod 256 |
| Node | addr | 哈希后 mod 256 |
| Tracker | url | 哈希后 mod 256 |

### 4.3 优势与劣势

**优势**：
- 单表数据量：1亿 / 256 ≈ 39万，查询极快
- 写入分散到 256 张表，锁竞争小
- 可并行查询多张表

**劣势**：
- 全量查询需要遍历 256 张表（可并行）
- 分表数固定，后期扩容需数据迁移

### 4.4 冷热分离表

```
nodes_hot    (最近7天活跃)
nodes_cold   (7天前活跃，归档)
```

- 写入只写 hot 表
- 定期将冷数据迁移到 cold 表
- 查询优先查 hot，miss 再查 cold
- cold 表可压缩存储（SQLite VACUUM INTO 压缩库）

---

## 五、内存缓存架构

### 5.1 分片 LRU 缓存

```rust
/// 分片 LRU 缓存（减少锁竞争）
pub struct ShardedLruCache<K: Hash + Eq, V> {
    shards: Vec<RwLock<LruCache<K, V>>>,  // 32 分片
    shard_mask: usize,
}

impl ShardedLruCache {
    /// 根据 key 哈希选择分片
    fn shard_index(&self, key: &K) -> usize {
        hash(key) & self.shard_mask
    }

    /// 批量驱逐（每个分片独立驱逐）
    pub fn evict(&self) -> EvictStats;
}
```

- **分片数**：32（与 CPU 核心数匹配）
- **单分片锁粒度**：只锁一个分片，并发度 32x

### 5.2 布隆过滤器

```rust
/// 布隆过滤器（快速判断 key 是否存在）
pub struct BloomFilter {
    bits: Vec<u8>,
    hash_count: usize,
}

impl BloomFilter {
    /// 插入 key
    pub fn insert(&self, key: &[u8]);

    /// 可能存在（true=可能存在，false=一定不存在）
    pub fn may_contain(&self, key: &[u8]) -> bool;

    /// 从 SQLite 全量构建（启动时）
    pub fn from_db(storage: &Storage) -> Self;
}
```

**作用**：
- 查询时先查布隆过滤器
- 如果返回 false，直接返回"不存在"，**避免一次磁盘查询**
- 如果返回 true，再查缓存/SQLite
- 1亿条数据，1% 误判率，约 120MB 内存

### 5.3 缓存容量配置

| 缓存层 | 容量 | 内存占用 | 命中率目标 |
|---|---|---|---|
| L1 分片 LRU | 500万条 | ~1GB | >90% |
| L2 布隆过滤器 | 1亿条 | ~120MB | 过滤 99% 不存在查询 |
| L3 SQLite | 1亿条 | 磁盘 | 永久存储 |

---

## 六、四个 Repo 改造

### 6.1 NodeRepo

```rust
pub struct NodeRepoImpl {
    /// 内存缓存（热数据，分片 LRU 驱逐）
    cache: RwLock<ShardedLruCache<SocketAddr, KBucketEntry>>,
    /// 脏节点集合（增量持久化）
    dirty: RwLock<FxHashSet<SocketAddr>>,
    storage: Arc<Storage>,
}
```

- 查询：先查缓存，miss 时从 SQLite 分表加载
- 写入：写穿透（缓存 + SQLite）
- 驱逐：按 `last_active` 排序，驱逐最久未活跃的
- 配置：`max_nodes_in_memory = 2,000,000`

### 6.2 PeerRepo

```rust
pub struct PeerRepoImpl {
    /// 全局 peer 缓存（分片 LRU）
    global: RwLock<ShardedLruCache<SocketAddr, PeerInfo>>,
    /// infohash -> peer addr 集合（索引，全量保留，内存小）
    by_infohash: RwLock<FxHashMap<Infohash, FxHashSet<SocketAddr>>>,
    /// peer addr -> infohash 引用（索引，全量保留）
    infohash_refs: RwLock<FxHashMap<SocketAddr, FxHashSet<Infohash>>>,
    storage: Arc<Storage>,
}
```

- 废弃"永久资产模式"
- `global` 纳入 LRU 缓存，冷 peer 从内存驱逐
- `by_infohash` / `infohash_refs` 只是 addr 集合，内存占用小，全量保留
- 查询 infohash 的 peer 列表时，按需从 SQLite 加载 peer 详情
- 配置：`max_peers_in_memory = 2,000,000`

### 6.3 InfohashRepo

```rust
pub struct InfohashRepoImpl {
    /// 内存缓存（分片 LRU）
    cache: RwLock<ShardedLruCache<Infohash, (u32, String, f64, u64)>>,
    /// 待持久化的新 infohash 缓冲区
    pending: RwLock<Vec<(Infohash, String)>>,
    storage: Arc<Storage>,
}
```

- 引用计数仍保留（用于判断是否热门）
- 冷 infohash 从内存驱逐，保留在 SQLite
- 查询时按需加载
- 配置：`max_infohashes_in_memory = 5,000,000`

### 6.4 TrackerRepo

```rust
pub struct TrackerRepoImpl {
    /// 内存缓存（分片 LRU）
    cache: RwLock<ShardedLruCache<String, TrackerEntry>>,
    storage: Arc<Storage>,
}
```

- 驱逐排序：按 `last_checked` + `success_rate`
- 长期不检查的 tracker 优先驱逐
- 高成功率 tracker 保留更久
- 配置：`max_trackers_in_memory = 500,000`

### 6.5 内存估算

| Repo | 建议上限 | 单条大小 | 预估内存 |
|---|---|---|---|
| NodeRepo | 2,000,000 | ~200 bytes | ~400 MB |
| PeerRepo | 2,000,000 | ~150 bytes | ~300 MB |
| InfohashRepo | 5,000,000 | ~60 bytes | ~300 MB |
| TrackerRepo | 500,000 | ~100 bytes | ~50 MB |
| 布隆过滤器 | 1亿条 | - | ~120 MB |
| **合计** | | | **~1.17 GB** |

---

## 七、驱逐策略

### 7.1 多级驱逐

```
L1 缓存超过 90% → 驱逐最久未访问的 20%
    ↓
驱逐前检查：是否为热数据（TierSystem 判定）
    ↓
热数据跳过，冷数据驱逐
    ↓
脏数据先持久化到 SQLite，再清内存
```

### 7.2 驱逐保护级别

| 保护级别 | 条件 | 行为 |
|---|---|---|
| **P0 不驱逐** | Tier=Hot 且 评分>70 | 永不驱逐 |
| **P1 延迟驱逐** | Tier=Warm 或 评分>50 | 驱逐队列后排 |
| **P2 正常驱逐** | Tier=Cold | 正常驱逐 |
| **P3 优先驱逐** | 评分<30 且 长期不活跃 | 优先驱逐 |

### 7.3 内存压力动态调整

```rust
/// 内存监控（定期采集进程内存）
pub struct MemoryMonitor {
    current_memory: AtomicUsize,
    high_watermark: usize,  // 如 4GB
    low_watermark: usize,   // 如 3GB
}

/// 内存 > high_watermark → 加速驱逐（每次驱逐 30%）
/// 内存 < low_watermark → 正常驱逐（每次驱逐 10%）
```

---

## 八、查询优化

### 8.1 批量预加载

```rust
/// 批量查询（自动分批，避免单次查询过大）
pub async fn get_peers_batch(&self, infohashes: &[Infohash]) -> Vec<Peer> {
    // 1. 先查 L1 缓存
    // 2. 缓存 miss 的，按分表分组
    // 3. 每张表一次 SQL 查询（IN 语句）
    // 4. 结果回填 L1 缓存
}
```

### 8.2 预取（Prefetch）

```rust
/// 查询一个 infohash 的 peers 时，预取相关 infohash
/// （如同一个 tracker 返回的其他 infohash）
pub async fn get_peers_with_prefetch(&self, infohash: &Infohash) -> Vec<Peer>;
```

### 8.3 异步加载

```rust
/// 缓存 miss 时不阻塞，返回缓存数据 + 后台加载
pub fn get_peers_async(&self, infohash: &Infohash) -> PeersFuture;
```

---

## 九、写入优化

### 9.1 写入队列（已有，增强）

```
写入请求 → mpsc 队列 → 批量攒批 → 分表并行写入
```

- 每批 1000 条，一次事务写入
- 256 张表可并行写入（多线程）
- 写入吞吐目标：**10万条/秒**

### 9.2 WAL 模式 + 同步优化

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;  -- 而非 FULL，性能提升 3x
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -65536;   -- 64MB 页缓存
```

### 9.3 索引策略

| 表 | 索引 | 说明 |
|---|---|---|
| nodes | (addr) UNIQUE | 主键查询 |
| nodes | (score DESC, last_active DESC) | 爬虫候选选择 |
| peers | (infohash, addr) UNIQUE | 复合主键 |
| peers | (infohash) | 按 infohash 查询 |
| infohashes | (infohash) UNIQUE | 主键查询 |
| infohashes | (ref_count DESC) | 热门排序 |
| trackers | (url) UNIQUE | 主键查询 |

---

## 十、TaskScheduler 集成

新增 4 个定期驱逐任务：

| 任务名 | 间隔 | 职责 |
|---|---|---|
| `node_cache_evict` | 300s | NodeRepo 冷数据驱逐 |
| `peer_cache_evict` | 300s | PeerRepo 冷数据驱逐 |
| `infohash_cache_evict` | 300s | InfohashRepo 冷数据驱逐 |
| `tracker_cache_evict` | 300s | TrackerRepo 冷数据驱逐 |

每个任务：
1. 检查缓存是否超过上限
2. 超过则驱逐一批冷数据
3. 返回统计信息（驱逐数量、当前缓存大小）

---

## 十一、启动与恢复

### 11.1 启动流程

```
启动
  ↓
1. 打开 SQLite（256张表，自动创建不存在的表）
  ↓
2. 构建布隆过滤器（从 SQLite 全量扫描，约 30秒/亿条）
  ↓
3. 预热 L1 缓存（加载最近活跃的 500万条）
  ↓
4. 启动 TaskScheduler（驱逐任务、持久化任务）
  ↓
5. 对外服务
```

### 11.2 崩溃恢复

- WAL 模式保证已提交数据不丢
- 启动时自动 replay WAL
- 脏数据（未持久化的内存数据）可能丢失，可接受（爬虫数据可重新发现）

---

## 十二、性能目标

| 指标 | 千万级（当前） | 亿级（目标） |
|---|---|---|
| 数据规模 | 1000万 | 1亿 |
| 内存占用 | <8GB（热+温） | <2GB（L1+L2） |
| 查询延迟（缓存命中） | <100μs | <50μs（分片锁） |
| 查询延迟（缓存 miss） | ~1ms（SQLite） | <1ms（分表+布隆） |
| 写入吞吐 | ~1万/秒 | 10万/秒（分表并行） |
| 启动时间 | ~10秒 | ~60秒（构建布隆+预热） |
| 驱逐耗时 | 全量扫描 | 增量分片驱逐 <100ms |

---

## 十三、实施路线

| 阶段 | 内容 | 预估工作量 | 风险 |
|---|---|---|---|
| **P0** | ShardedLruCache + BloomFilter 模块 | 2天 | 低，纯新增 |
| **P1** | SQLite 分表改造（256张表） | 3天 | 中，Storage 层重写 |
| **P2** | 四个 Repo 接入分片缓存 + 按需加载 | 5天 | 中，核心逻辑 |
| **P3** | 驱逐策略 + 内存监控 | 2天 | 低 |
| **P4** | 启动优化（布隆构建 + 预热） | 2天 | 中 |
| **P5** | 性能测试 + 调优 | 3天 | 中 |
| **合计** | | **~17天** | |

---

## 十四、关键决策点

### 14.1 分表数 256 是否合适？

- 256 = 单表 39万条，查询极快
- 但全量查询需遍历 256 张表
- 可选 64（单表 156万）或 1024（单表 10万）

### 14.2 是否引入 RocksDB？

- 短期：SQLite 分表足够
- 长期：如果写入成为瓶颈，可将写入路径迁移到 RocksDB
- 建议先 SQLite 分表，预留 RocksDB 接口

### 14.3 布隆过滤器内存 120MB 是否可接受？

- 1亿条，1% 误判率，约 120MB
- 可接受，能避免大量磁盘查询
- 可选 0.1% 误判率（~180MB）或 5% 误判率（~60MB）

### 14.4 L1 缓存 500万条是否足够？

- 500万 / 1亿 = 5% 热数据
- 命中率目标 >90%（典型 2/8 原则）
- 可配置，根据实际命中率调整

---

## 十五、与现有架构的关系

### 15.1 复用的模块

- **WriteQueue**：写入队列，增强为分表并行写入
- **TierSystem**：冷热分层判定，用于驱逐保护
- **TaskScheduler**：统一调度驱逐任务
- **Storage**：SQLite 封装，扩展为分表操作

### 15.2 废弃的模式

- ❌ "永久资产模式"（PeerRepo 不删除任何 peer）
- ❌ 全量内存存储（无容量上限）
- ❌ 冷热分层只判定不驱逐

### 15.3 兼容策略

- 配置项新增缓存上限，默认值与当前行为一致（不限制）
- 分表改造提供数据迁移脚本，从旧单表迁移到 256 张分表
- 联邦同步不受影响（Merkle 树从 SQLite 全量计算）

---

## 十六、风险与应对

| 风险 | 应对 |
|---|---|
| 分表后全量查询变慢 | 并行查询 256 张表，结果合并 |
| 布隆过滤器误判导致查询空转 | 误判率 1%，可接受；可调整参数 |
| 缓存命中率低导致磁盘 IO 高 | 监控命中率，动态调整缓存容量 |
| 启动时间过长（构建布隆） | 异步构建，先启动服务，后台构建 |
| 数据迁移风险 | 提供迁移脚本，先备份再迁移 |
| 联邦 Merkle 计算变慢 | 从 SQLite 全量加载，计算完不放入缓存 |

---

## 十七、一句话总结

**从「全量内存 + 单表 SQLite」升级为「三层缓存（分片 LRU + 布隆过滤器 + 分表 SQLite）」，数据库是唯一真实来源，内存只保留热数据，冷数据自动驱逐但永久保存在数据库。核心不是「减少数据」，而是「把数据从必须放内存变为按需加载」——热数据在内存保证速度，冷数据在磁盘保证容量，亿级数据也能稳定运行。**
