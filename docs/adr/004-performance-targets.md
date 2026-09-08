# ADR-004: 千万级数据性能目标与优化架构

> 状态：✅ 已采纳
> 日期：2026-09-08
> 决策者：项目负责人
> 相关：性能优化阶段，PDC v0.2.0

## 背景 (Context)

PDC（PeerDiscoveryCenter）作为 PandaNetOS 生态的节点发现 Agent，当前数据规模仅为 6 万 DHT 节点、800 Peer、90 Infohash，远不能满足生态长期发展需求。

### 当前现状

| 指标 | 当前值 | 瓶颈 |
|---|---|---|
| DHT 节点 | 60,000+ | 全量保存导致 IO 高 |
| Peer | 800+ | 数据量小，未验证大规模 |
| Infohash | 90+ | 实时写入 SQLite |
| 磁盘 IO | ~10 MB/s（峰值） | 全量保存+频繁小事务 |
| 超级 Tracker QPS | 未压测 | 未知，架构未针对高并发优化 |
| 爬虫效率 | 未统计 | 并发度和节点选择策略未优化 |

### 存在的问题

1. **全量持久化**：每 5 分钟全量保存所有节点到 SQLite，数据量增长后 IO 将不可接受
2. **全局锁竞争**：所有 Repo 使用全局 `parking_lot::RwLock<HashMap>`，高并发下锁竞争严重
3. **SQLite 单连接**：`std::sync::Mutex<Connection>`，每次写入获取锁，无法批量攒写
4. **infohash 实时写入**：每次 register 都 tokio::spawn 写 SQLite，频繁小事务
5. **超级 Tracker 未优化**：announce/scrape 直接读写内存+SQLite，未针对 10 万 QPS 设计
6. **节点选择全量遍历**：SelectSystem 每次调用 all_nodes_sync() 全量克隆，千万级不可行
7. **评分全量重算风险**：虽然已实现增量评分，但千万级节点的分批策略未验证

### 约束条件

- 单机部署（不依赖分布式数据库）
- SQLite 作为持久化存储（不引入 Redis/MySQL）
- Rust + tokio 异步运行时
- 内存上限 8GB（冷热分层后）
- 磁盘 IO 目标 < 2 MB/s

## 决策 (Decision)

### 1. 数据规模目标

所有关键数据支撑**千万级**（10,000,000+）：

| 数据类型 | 目标规模 |
|---|---|
| DHT 节点（NodeRepo） | 10,000,000+ |
| Peer（PeerRepo） | 10,000,000+ |
| Infohash（InfohashRepo） | 10,000,000+ |
| Tracker（TrackerRepo） | 10,000,000+ |

### 2. 性能指标目标

| 指标 | 目标 |
|---|---|
| 爬虫效率 | 10,000 新节点/小时 |
| 超级 Tracker QPS | 100,000/秒（announce + scrape） |
| 平均响应延迟 | < 100 微秒 |
| 磁盘 IO | < 2 MB/s |
| 内存使用 | < 8 GB（冷热分层后） |
| 评分重算 | 增量+分批，全量兜底 24h |

### 3. 核心架构决策

#### 3.1 冷热分层 + 内存上限

- 热数据（最近 2 小时活跃）：在内存，上限 100 万
- 温数据（最近 24 小时活跃）：在内存，上限 500 万
- 冷数据：归档到 SQLite，不占内存
- 自动降级：热数据超上限时清理最老的

#### 3.2 增量持久化（替代全量保存）

- 所有 Repo 维护 dirty 标记（HashSet<id>）
- 节点/Peer/Infohash/Tracker 变更时标记 dirty
- save_all 只遍历 dirty 集合，只保存变更数据
- 保存后清除 dirty 标记
- 预期写入量减少 95%+

#### 3.3 SQLite 写入队列 + 批量事务

- 专用 writer task（单线程，保证 SQLite 单 writer）
- mpsc 队列接收所有写入请求
- 攒批策略：1000 条或 1 秒 flush（取先到者）
- 一次性事务写入（BEGIN...COMMIT）
- 写入不阻塞查询路径

#### 3.4 超级 Tracker 纯内存 + 异步持久化

- Tracker 数据（by_infohash、peer_info）**纯内存为主**
- announce/scrape 查询**不写 SQLite**，直接读写内存
- 持久化异步批量进行（每 5 分钟），不阻塞查询
- 可配置为纯内存模式（不持久化，重启后重新收集）
- 锁分片：by_infohash 按 infohash 哈希分片 64 把锁

#### 3.5 锁分片（Sharded Lock）

- 所有 Repo 的 HashMap 按 key 哈希分片
- 64 把独立 RwLock，读写只锁对应分片
- 全局操作（如 save_all）遍历所有分片，但不持有全局锁
- 预期高并发下锁竞争减少 90%+

#### 3.6 FxHashMap 替代 std::HashMap

- 所有 Repo 的 HashMap 改用 `rustc_hash::FxHashMap`
- FNV 哈希函数，比 std::HashMap 的 SipHash 快 20-30%
- 适用于非安全敏感场景（内部数据，不面临哈希碰撞攻击）

#### 3.7 增量评分 + 分批处理

- 节点变更时标记 dirty
- ScoreMaintainer 只重算 dirty 节点
- 每批处理 1000 个，避免阻塞
- 全量重算兜底：每 24 小时一次（低峰期）
- 千万级节点可支撑

#### 3.8 节点选择索引（SelectSystem）

- 建立评分索引：`BTreeMap<score, Vec<node_id>>`
- 建立网段索引：`HashMap<subnet, Vec<node_id>>`
- 分层采样：先从热数据高分节点采样，不够再从温数据采样
- 蓄水池算法：从大规模数据中高效均匀采样
- 避免全量遍历，选择效率 O(log n)

### 4. 实施范围

- 覆盖 PDC 所有数据层（NodeRepo/PeerRepo/TrackerRepo/InfohashRepo）
- 覆盖超级 Tracker（HTTP/UDP）
- 覆盖 DHT 爬虫引擎
- 覆盖评分系统（ScoreMaintainer）
- 覆盖节点选择（SelectSystem）
- 不涉及 pnos-sdk 接入（放后续阶段）

## 后果 (Consequences)

### 正面影响

- 数据规模从 6 万级提升到千万级，支撑生态长期发展
- 磁盘 IO 从 ~10 MB/s 降到 < 2 MB/s，适合长时间运行
- 超级 Tracker 支撑 10 万 QPS，可作为公共 Tracker 服务
- 爬虫效率提升到 1 万/小时，快速积累节点资源
- 内存使用可控（< 8GB），冷热分层避免 OOM
- 锁竞争减少，高并发下性能稳定

### 负面影响 / 代价

- 架构复杂度增加（锁分片、写入队列、增量持久化）
- 开发工作量大（P0-P3 共 24 项优化）
- 冷数据查询需要读 SQLite，延迟比内存高
- 增量持久化需要维护 dirty 标记，增加代码复杂度
- 纯内存 Tracker 模式下重启后数据丢失（可接受，Tracker 数据是临时的）

### 风险

- 锁分片实现不当可能导致数据不一致
- 写入队列积压可能导致内存增长（需要队列上限+背压）
- 冷热分层边界判断不准确可能导致热数据被清理
- 千万级数据下 SQLite 查询可能变慢（需要索引+分区）
- FxHashMap 存在哈希碰撞攻击风险（内部数据，风险低）

### 缓解措施

- 锁分片：充分测试，添加一致性校验
- 写入队列：设置队列上限（10 万条），超限时降级为同步写入
- 冷热分层：可配置阈值，添加监控指标
- SQLite：确保所有查询字段有索引，考虑分区表
- FxHashMap：仅用于内部数据，不用于外部输入的 key

## 替代方案 (Alternatives)

### 方案 A：引入 Redis 作为缓存层

- 优点：Redis 原生支持高并发、持久化、集群
- 缺点：引入外部依赖，增加部署复杂度，单机部署不希望依赖 Redis
- 不选择的原因：约束条件要求单机部署，不引入外部数据库

### 方案 B：使用 RocksDB 替代 SQLite

- 优点：RocksDB 写入性能更好，支持 LSM 树，适合高写入场景
- 缺点：Rust 绑定不够成熟，编译复杂，查询不如 SQLite 灵活
- 不选择的原因：SQLite 已足够，写入队列+批量事务可解决写入性能问题

### 方案 C：全内存模式，不持久化

- 优点：性能最高，无 IO 瓶颈
- 缺点：重启后数据丢失，千万级数据内存占用过大（>10GB）
- 不选择的原因：需要持久化保证数据不丢失，内存上限 8GB

### 方案 D：分布式部署（多实例分片）

- 优点：水平扩展，可支撑亿级数据
- 缺点：架构复杂度高，需要一致性哈希、节点同步、负载均衡
- 不选择的原因：当前阶段单机部署，分布式放 P3 极致性能阶段

## 实施计划 (Implementation Plan)

### P0 — 核心优化（8 项）

- [~] 1. 冷热分层完善 + 内存上限（tier_manager 框架已建，NodeRepo 已实现冷热过滤）
- [x] 2. 增量持久化（NodeRepo 已实现，take_dirty_sync 原子取出 dirty 集合）
- [x] 3. SQLite 写入队列（write_queue.rs 已创建，mpsc + 批量事务基础设施）
- [x] 4. infohash_repo 批量写入（pending 缓冲区 + flush_pending，30s 定时 flush）
- [~] 5. 超级 Tracker UDP 高性能架构（接收缓冲区 2048→4096，批量收发/零拷贝待做）
- [ ] 6. 超级 Tracker 锁分片（by_infohash 64 分片，sharded_map 基础设施已就绪）
- [~] 7. 超级 Tracker 纯内存 + 异步持久化（announce 双写 PeerRepo，纯内存模式待优化）
- [x] 8. FxHashMap 替代 std::HashMap（NodeRepo/PeerRepo/TrackerRepo/InfohashRepo 全部替换）

### P1 — 效率优化（7 项）

- [ ] 9. 增量评分 + 分批处理（每批 1000，全量兜底 24h）
- [~] 10. 节点选择索引（SelectSystem 已实现 ID分桶+网段去重+评分排序，BTreeMap索引待做）
- [ ] 11. 全局锁分片（所有 Repo HashMap 64 分片）
- [x] 12. FxHashMap 全面替换（8个核心文件：udp_tracker/http_tracker/aggregator/select_system/types/discover_service/tracker_service 等）
- [ ] 13. announce 响应优化（peer 列表预采样缓存 + bencode 序列化优化）
- [ ] 14. scrape 批量优化（多 infohash 批量查询 + 统计缓存）
- [ ] 15. HTTP Tracker 连接池（keep-alive + gzip 压缩）

### P2 — 爬虫与深度优化（5 项）

- [x] 16. 爬虫并发调优（active_crawl 32→64 节点，超时 30s→15s，target 8→16）
- [~] 17. 爬虫节点选择策略（SelectSystem 已实现高评分+ID分桶+网段多样性，高活跃优先待优化）
- [x] 18. UDP 收发优化（爬虫 4096→8192，超级Tracker 2048→4096，批量收发/零拷贝待做）
- [x] 19. 限流与监控（rate_limiter.rs：令牌桶100QPS+突发200+异常封禁60s+QPS滑动窗口+WebSocket实时显示）
- [x] 20. 对象池（object_pool.rs：crossbeam ArrayQueue 无锁+RAII自动归还+create_buffer_pool工厂+单元测试）

### P3 — 极致性能（4 项）

- [ ] 21. 多实例分片（按 infohash 一致性哈希 + 负载均衡）
- [ ] 22. 内核 bypass（DPDK/AF_XDP，可选）
- [ ] 23. CPU 亲和性（关键任务绑定 CPU 核心）
- [ ] 24. SQLite 分区表（千万级数据按时间/评分分区）

## 验证标准 (Verification Criteria)

### 功能验证

- [ ] 所有现有功能正常（爬虫、Tracker、PEX、Probe、REST API、监控）
- [ ] 数据持久化正常（重启后数据不丢失）
- [ ] 冷热分层正常（冷数据归档到 SQLite，热数据在内存）
- [ ] 增量持久化正常（只保存变更数据，dirty 标记正确维护）

### 性能验证

- [ ] DHT 节点数达到 100 万时，磁盘 IO < 2 MB/s
- [ ] DHT 节点数达到 100 万时，内存 < 2 GB
- [ ] 超级 Tracker announce QPS 达到 1 万时，平均延迟 < 100 微秒
- [ ] 超级 Tracker scrape QPS 达到 5000 时，平均延迟 < 50 微秒
- [ ] 爬虫效率达到 1000 新节点/小时（小规模验证）
- [ ] 评分重算不阻塞主流程（增量+分批）

### 稳定性验证

- [ ] 连续运行 24 小时无内存泄漏
- [ ] 连续运行 24 小时无 panic/crash
- [ ] 写入队列不积压（队列长度 < 1000）
- [ ] 锁竞争不导致性能下降（高并发下 QPS 稳定）

## 参考资料 (References)

- [SQLite WAL 模式](https://www.sqlite.org/wal.html)
- [rustc_hash::FxHashMap](https://docs.rs/rustc-hash/)
- [parking_lot](https://docs.rs/parking_lot/)
- [BT Tracker 协议](http://www.bittorrent.org/beps/bep_0003.html)
- [UDP Tracker 协议](http://www.bittorrent.org/beps/bep_0015.html)

## 变更记录 (Changelog)

### 2026-09-08 P0+P1+P2 优化实施完成

**实施范围**：P0 核心优化（4项完成+3项部分完成）、P1 效率优化（1项完成+1项部分完成）、P2 爬虫与深度优化（4项完成+1项部分完成）

**已完成模块**：
- `src/storage/sharded_map.rs` — 锁分片 HashMap（ShardedHashMap + DirtyShardedHashMap）
- `src/storage/write_queue.rs` — SQLite 写入队列（mpsc + 批量事务）
- `src/storage/node_repo.rs` — FxHashMap + 增量持久化（take_dirty_sync）
- `src/storage/infohash_repo.rs` — FxHashMap + pending 批量写入（flush_pending）
- `src/storage/tracker_repo.rs` — FxHashMap 替换
- `src/data_plane/rate_limiter.rs` — 限流与 QPS 监控（令牌桶+异常封禁）
- `src/utils/object_pool.rs` — 通用对象池（crossbeam ArrayQueue + RAII）
- `src/crawler/engine.rs` — 并发调优（32→64）、超时缩短（30s→15s）、target 多样化（8→16）、UDP 缓冲区增大（4096→8192）
- `src/data_plane/udp_tracker.rs` — UDP 缓冲区增大（2048→4096）、限流集成
- `src/data_plane/ws.rs` — QPS 统计实时显示
- `src/main.rs` — 限流器创建与集成
- `src/lib.rs` — utils 模块导出

**验证结果**：
- release 编译通过（43 秒，12.69 MB）
- PDC 启动正常，加载 77,079 个 DHT 节点（持续增长）
- 稳定运行后磁盘 IO 0 MB/s（增量持久化生效）
- 限流器正常工作（单 IP 100 QPS，突发 200）
- 对象池单元测试通过
- WebSocket 监控 QPS/封禁数实时显示

**剩余优化**：P0 第6项（超级Tracker锁分片）、P1 第9/11/13/14/15项、P3 全部4项

| 日期 | 版本 | 变更内容 | 作者 |
|---|---|---|---|
| 2026-09-08 | 1.0 | 初始版本，制定千万级性能目标与优化架构 | 项目负责人 |
