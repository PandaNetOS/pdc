# PDC 多实例外网数据同步方案研究报告

> 版本：v1.0 | 日期：2026-09-09 | 状态：初步研究
> 范围：PDC（节点发现 Agent）多实例间通过外网连接同步数据，聚焦数据面 P2P 导向的两类候选方案
> 约束：基于 PDC 实际代码架构（`D:\PNOS\pdc`），不凭空假设；信息不足标注"待确认"

---

## 0. 现状基线（基于代码实证）

在展开方案分析前，先固化 PDC 当前的数据层与网络层事实，所有后续分析以此为锚点。

### 0.1 四大 Repo 实际状态

| Repo | 内存结构 | 锁机制 | dirty 标记 | 持久化方式 | 实际规模 |
|---|---|---|---|---|---|
| **NodeRepo** | `RwLock<FxHashMap<SocketAddr, KBucketEntry>>` | 单 RwLock | ✅ `FxHashSet<SocketAddr>` | 增量（仅 dirty） | 目标 1000 万+，当前加载 77,079 |
| **PeerRepo** | `RwLock<PeerMemoryStore>`（by_infohash + global + infohash_refs 三表） | 单 RwLock | ❌ 无 | 全量 save_all + history 攒批（100 条 flush） | 目标 1000 万+ |
| **InfohashRepo** | `RwLock<FxHashMap<Infohash, (u32, String, f64)>>` | 单 RwLock | ❌ 无（有 pending 缓冲区） | 新 ih 批量写入 + 全量 ref_count 更新 | 实际 1K+ |
| **TrackerRepo** | `RwLock<FxHashMap<String, TrackerEntry>>` | 单 RwLock | ❌ 无 | 全量 | 实际 200+ |

> **重要修正**：agent-hint 称"每个 Repo 已有 dirty HashSet"，实际代码中**仅 NodeRepo 有 dirty 机制**。PeerRepo/InfohashRepo/TrackerRepo 均无 dirty 标记。Gossip 方案若要复用 dirty，需先为 PeerRepo 补充 dirty 机制。
>
> **另一个修正**：AGENTS.md 提及"ShardedHashMap（64 分片）"用于超级 Tracker，但代码中 `PeerRepoImpl` 实际是**单 RwLock**，`ShardedHashMap`/`DirtyShardedHashMap` 已定义但未在 PeerRepo 中使用。超级 Tracker 的 `SuperTrackerState.peers` 使用 `DashMap<Infohash, FxHashMap<...>>`（DashMap 内部分片）。10 万 QPS 下 PeerRepo 的单 RwLock 是潜在瓶颈——这是同步方案设计时需考虑的前置问题。

### 0.2 超级 Tracker 热路径（announce）

`handle_udp_announce`（BEP 15）的执行序列：

1. `connections.read()` 验证 connection_id（`FxHashMap<u64, ConnectionEntry>`，RwLock）
2. `super_tracker.peers.entry(infohash).or_default()` — DashMap 分片锁，插入/更新 `AnnouncedPeer`
3. `cache.add_peers_sync()` — PeerRepo 单 RwLock 写锁，更新三表
4. `super_tracker.get_peers_for_infohash()` — DashMap 读
5. `cache.get_peers_sync()` — PeerRepo 读锁，按 score 排序截断
6. 组装 compact peers 响应

**全部为本地内存操作，无网络 IO、无磁盘 IO**。这是 <100μs 目标的根基。任何将跨实例网络调用引入此路径的设计都会直接破坏该目标。

HTTP `/announce` 路径类似，额外有 axum 路由开销和可选的异步后端发现触发（不阻塞响应）。

**Scrape 现状**：
- UDP scrape：**未实现**（返回 "scrape not implemented" 错误）
- HTTP `/scrape`：已实现，从 `SuperTrackerState.peers` 本地统计 complete/incomplete，`downloaded` 恒为 0

### 0.3 已有可复用组件

| 组件 | 位置 | 复用价值 |
|---|---|---|
| Kademlia 路由表（160 bucket，K=8） | `dht/routing_table.rs` | 分片方案的实例间路由可参考，但需独立实例 DHT |
| DHT KRPC 协议（find_node/get_peers/announce_peer） | `discoverers/dht/`、`crawler/engine.rs` | 协议编解码可复用，消息类型需扩展 |
| NatService（UPnP/NAT-PMP/PCP/STUN/hole punching） | `nat/`、`services/nat_service.rs` | 两方案均需要外网可达性，直接复用 |
| `ShardedHashMap` / `DirtyShardedHashMap` | `storage/sharded_map.rs` | Gossip 变更源、分片存储均可复用 |
| `RateLimiter`（QPS 监控 + 单 IP 限流 + 封禁） | `data_plane/rate_limiter.rs` | P2P 连接的限流与恶意节点防护 |
| `EventBus` | `event_bus.rs` | 本地变更事件可作为 gossip 触发源 |
| `ObjectPool` | `utils/object_pool.rs` | gossip 消息对象复用 |

### 0.4 网络与端口

- 6880：TCP（HTTP tracker + REST API + WS）+ UDP（UDP tracker BEP 15）
- 6882：UDP（DHT 爬虫）
- 新增 P2P 同步端口：建议 6885（TCP 长连接 + UDP hole punching），待确认

---

## 1. 方案一：Gossip 协议 P2P 同步

### 核心思路

所有 PDC 实例保持**全量数据副本**（每个实例都有完整的四大 Repo），实例间通过 P2P 网状连接，以流行病协议（epidemic protocol）随机传播数据变更，达到最终一致性。无中心节点，无分片归属，任何实例可独立响应所有查询。

---

### 1.1 网络拓扑

| 维度 | 设计 |
|---|---|
| 拓扑类型 | **部分网状（partial mesh）**，非全互联 |
| 每个实例连接数 | 固定 fanout `f = 4~8` 个随机邻居 + 2~3 个"种子"稳定邻居 |
| 连接方向 | 双向 TCP 长连接（多路复用），UDP 用于 NAT 穿透信令 |
| 最大实例数 | 理论无上限，实际受单实例带宽约束（见 1.5），建议 ≤ 50 |
| 新实例加入 | 通过种子节点列表（bootstrap）获取邻居，逐步融入 |

**不选全互联的原因**：N 个实例全互联需 N(N-1)/2 条连接，N=20 即 190 条，NAT 穿透成本和连接维护开销过高。部分网状在 f=4 时传播延迟仅比全互联高 O(log N) 轮，带宽却大幅降低。

### 1.2 同步协议

**推荐混合模式：Push Rumor Mongering（增量） + Pull Anti-Entropy（对账）**

#### 1.2.1 增量传播：Push Rumor Mongering

- **触发源**：本地数据变更时，将变更条目写入 `outbox`（可复用 `DirtyShardedHashMap` 或新增 `GossipOutbox`）
- **传播周期**：每 `T = 500ms~1s`，从 outbox 取出未传播的变更，随机选择 f 个邻居发送
- **消息格式**（建议二进制，参考 KRPC bencode 但更紧凑）：

```
GossipMessage {
    msg_id: u64,              // 去重用
    origin_instance: [u8; 20],// 发起实例 ID（可用 DHT node id 格式）
    repo_type: u8,            // 0=Node, 1=Peer, 2=Infohash, 3=Tracker
    op: u8,                   // 0=upsert, 1=delete
    version: u64,             // 逻辑时钟/时间戳，用于冲突解决
    payload: bytes,           // 序列化的变更记录
}
```

- **抑制机制**：每个实例维护 `seen_msgs`（LRU，容量 100 万），收到已处理的 msg_id 直接丢弃，不再转发
- **PeerRepo 补充 dirty**：需为 PeerRepo 增加 `dirty: RwLock<FxHashSet<(Infohash, SocketAddr)>>`，在 `add_peers_sync`/`remove_peer`/`update_score` 时标记

#### 1.2.2 对账修复：Pull Anti-Entropy

- **周期**：每 `60s`，随机选择 1 个邻居，交换**版本向量摘要**（非全量数据）
- **摘要结构**：按 Repo +  key 前缀分片的 Merkle 树根哈希，或简化为 `(repo_type, key_hash_prefix, count, max_version)` 向量
- **差异修复**：发现版本落后的分片后，pull 该分片的全量/增量数据
- **作用**：修复 rumor mongering 的丢包/分区导致的永久不一致，是最终一致性的兜底

#### 1.2.3 反熵 vs 谣言传播的选择结论

| 维度 | 反熵（Anti-Entropy） | 谣言传播（Rumor Mongering） |
|---|---|---|
| 带宽 | 高（周期交换全量/摘要） | 低（仅传播变更） |
| 传播延迟 | 高（分钟级，取决于周期） | 低（秒级，指数扩散） |
| 一致性保证 | 强（最终一定收敛） | 弱（有概率遗漏，需兜底） |
| 适合 PDC | ❌ 千万级数据全量交换不可行 | ✅ 增量变更传播，配合反熵兜底 |

**结论**：以谣言传播为主（低延迟、低带宽），反熵仅作为周期性对账兜底（低频、摘要化）。

### 1.3 一致性模型

| 维度 | 说明 |
|---|---|
| 模型 | **最终一致性（Eventual Consistency）**，单 key 维度上为**最后写赢（LWW）** |
| 因果一致 | 不保证。同一 peer 的连续两次 score 更新可能乱序到达，但 LWW + version 可收敛 |
| 对 PDC 适用性 | ✅ 高度适配。PDC 数据天然容忍短暂不一致：peer 少几个不影响下载，node 评分晚几秒无关紧要，infohash ref_count 允许短暂偏差 |
| 收敛时间 | 99% 变更在 `O(log N / log f) × T` 内传播：N=10, f=4, T=1s → ~2s；N=50, f=4, T=1s → ~3s |

### 1.4 数据分类同步策略

| Repo | 同步策略 | 理由 |
|---|---|---|
| **NodeRepo** | ✅ 实时 gossip（增量） | 节点持续新增，评分异步重算。dirty 机制已存在，直接作为变更源。节点 state（Good/Bad）变更也传播 |
| **PeerRepo** | ⚠️ **选择性同步**：仅同步**新 peer 发现**（infohash+addr+source）和 **score 更新**；**不同步 last_active** | last_active 时效性极强（秒级变化），同步它会产生 10 万 QPS 的变更风暴。每个实例从自己收到的 announce 独立维护 last_active 和 TTL 过期。新 peer 到达率远低于 announce QPS（见 1.5 估算） |
| **InfohashRepo** | ⚠️ 同步 ih 存在性 + score；**ref_count 不同步**（各实例独立计数） | ref_count 是冲突热点（10 万 QPS announce 都在 +1），gossip 下无法精确合并。改为各实例独立维护 ref_count，仅在 ih 首次出现时 gossip 传播"ih 存在"事件。全局 ref_count 可通过反熵对账时粗略合并 |
| **TrackerRepo** | ✅ 全量 gossip（低频） | 仅 200+ 条，变更极少。任何实例发现新 tracker 或禁用 tracker，全量传播。可每次变更后 push 完整条目 |

**关键设计原则**：**时效性字段（last_active、ref_count、connection_attempts）本地化，存在性与评分字段 gossip 化**。这将 gossip 变更率从 10 万/s 降至数千/s（见 1.5）。

### 1.5 性能影响与带宽估算

#### 1.5.1 对 10 万 QPS / <100μs 的影响

- **announce 热路径零侵入**：gossip 是异步后台任务，不进入 `handle_udp_announce` 的同步调用链。announce 仍然纯本地内存操作，<100μs 目标不受影响。
- **间接开销**：PeerRepo 增加 dirty 标记（每次 add_peer 多一次 HashSet insert，~100ns），可忽略。
- **CPU 开销**：gossip 引擎占用 1~2 个核（序列化、网络 IO），与 announce 处理核隔离（tokio worker 池）。
- **内存开销**：outbox + seen_msgs 约 200~500 MB（千万级 key 的 dirty 集合），在 8GB 预算内。

#### 1.5.2 千万级 Peer 带宽开销量化估算

**变更率建模**：

| 变更类型 | 速率 | 单条大小（紧凑二进制） |
|---|---|---|
| 新 peer 发现（稳态） | 10M ÷ 3600s TTL ≈ **2,778 条/s** | infohash(20) + addr(6) + source(1) + score(8) + version(8) ≈ **43 B** |
| Peer score 更新（ScoreMaintainer 分批） | 约 **1,000 条/s**（10s 增量重算，每次 ~1 万条） | addr(6) + score(8) + version(8) ≈ **22 B** |
| Node 新增 | 目标 10,000/h ≈ **3 条/s** | id(20) + addr(6) + score(8) + state(1) ≈ **35 B** |
| Node score/state 更新 | 约 **500 条/s** | addr(6) + score(8) + state(1) ≈ **15 B** |
| Infohash 新增 | 实际 1K+ 总量，约 **0.1 条/s** | 20 B |
| Tracker 变更 | < **0.01 条/s** | ~100 B |
| **合计有效变更率 R** | **≈ 4,300 条/s** | 加权平均 ≈ **35 B** |

> 注：新 peer 到达率的推导——稳态下 peer 总数 1000 万，TTL 1 小时（PeerRepo `cleanup_expired` 默认 3600s），则每秒过期≈新增≈ 10M/3600 ≈ 2778。announce 的 10 万 QPS 中绝大多数是**已有 peer 的 re-announce**（仅更新 last_active，不 gossip）。

**单实例出口带宽**（push rumor mongering，fanout f=4，每变更每实例平均转发 1 次）：

```
带宽 = R × avg_size × f(转发系数)
     = 4,300 × 35 × 1
     ≈ 150,500 B/s ≈ 0.15 MB/s ≈ 1.2 Mbps
```

> 推导：rumor mongering 中每条变更在系统中被转发约 N 次（每个收到的实例转发一次），摊到每实例 ≈ 1 次/变更。加上 origin 发送 f 次，每实例平均发送 ≈ (1 + f/N) ≈ 1 次。

**考虑冗余系数**（实际中部分变更被重复转发，seen 命中率非 100%）：× 1.5 → **~1.8 Mbps**

**加上反熵对账**（每 60s 交换 Merkle 摘要，摘要大小 ≈ 分片数 × 32B，1024 分片 → 32KB，pull 差异数据平均 100KB/次）：

```
反熵带宽 = (32KB + 100KB) / 60s ≈ 2.2 KB/s ≈ 0.02 Mbps（可忽略）
```

**N 实例总带宽**：

| 实例数 N | 每实例出口 | 集群总出口 | 每实例入口 |
|---|---|---|---|
| 3 | ~1.8 Mbps | ~5.4 Mbps | ~1.8 Mbps |
| 10 | ~1.8 Mbps | ~18 Mbps | ~1.8 Mbps |
| 50 | ~2.0 Mbps* | ~100 Mbps | ~2.0 Mbps |

> *N=50 时传播轮数增加，冗余系数略升。

**结论**：在"选择性同步"策略下，gossip 带宽**与实例数近似无关**（每实例恒定 ~2 Mbps），千万级 peer 完全可控。这是 gossip 方案的核心优势。

> **反例警告**：若错误地将 last_active 也纳入 gossip，则 R = 100,000/s，带宽 = 100K × 20B × 1.5 = 3 MB/s = 24 Mbps/实例，N=10 时 240 Mbps，仍可接受但无意义（last_active 跨实例无价值）。

### 1.6 NAT 穿透

| 维度 | 方案 |
|---|---|
| 外网可达性 | 复用现有 `NatService`：UPnP/NAT-PMP 自动映射 6885 端口；失败则降级为 PCP/STUN |
| 直连建立 | TCP 直连优先；双侧均 NAT 时用 UDP hole punching（已有 `udp_hole_punch.rs`），穿透成功后升级为 TCP（或直接用 UDP + KCP 可靠传输） |
| 中继兜底 | 无法穿透时通过已可达的实例中继（TURN-like），增加延迟但保证连通。中继带宽需限流 |
| 待确认 | 是否所有部署环境都支持 UPnP；家用 NAT 后的实例是否需要公网中继节点 |

### 1.7 安全与认证

| 威胁 | 防护 |
|---|---|
| 恶意实例注入虚假 peer/node | **实例身份认证**：每个实例持有 Ed25519 密钥对，gossip 消息签名（origin_instance + signature），接收方验证。bootstrap 种子节点内置公钥列表 |
| Sybil 攻击（大量伪造实例） | 准入制：新实例需由已有实例签名推荐（信任链），或通过 pnos-runtime 统一注册认证。PDC 实例数少（≤50），Sybil 风险低 |
| 消息篡改/重放 | msg_id + version + 签名，重放窗口 30s |
| 数据污染（虚假高分 peer） | 评分由本地 ScoreMaintainer 独立计算，gossip 只传播原始数据，不传播最终评分；或传播评分但本地加权融合 |
| 带宽滥用 | 复用 `RateLimiter` 对单邻居连接限流 |

### 1.8 冲突解决

| 冲突场景 | 解决策略 |
|---|---|
| 同一 peer 不同 score | LWW（version 大者赢），version 为混合逻辑时钟（HLC） |
| 同一 node 不同 state | LWW；Bad 状态优先（保守原则：若任一实例标记 Bad，则全网 Bad） |
| Infohash ref_count | **不合并**，各实例独立计数。需要全局精确值时通过反熵 pull 全量后本地求和（低频操作） |
| Peer 同时被添加和删除 | 删除优先（tombstone），删除操作 version 必须高于添加；tombstone 保留 24h 后清理 |
| Tracker disabled 冲突 | 任一实例 disabled → 全网 disabled（保守） |

### 1.9 实现复杂度

| 模块 | 新增/改造 | 工作量估算 |
|---|---|---|
| `GossipEngine` | 新增：outbox、seen_msgs、邻居管理、周期推送 | 中（~800 行 Rust） |
| `AntiEntropy` | 新增：Merkle 摘要、差异 pull | 中（~500 行） |
| PeerRepo dirty 机制 | 改造：增加 dirty HashSet + take_dirty | 小（~100 行） |
| 消息序列化 | 新增：二进制编解码（可复用 bencode 或自定义） | 小（~200 行） |
| P2P 传输层 | 新增：TCP 多路复用 + UDP 信令（可复用 quinn/quic 或自研） | 中（~600 行） |
| 实例身份与签名 | 新增：Ed25519 密钥管理 + 消息签名验证 | 小（~200 行） |
| 配置与集成 | 改造：main.rs 启动 gossip 引擎，config 增加参数 | 小（~100 行） |
| **合计** | | **中大型改造，~2500 行，核心逻辑不触碰 announce 热路径** |

**优势**：对现有数据层侵入极小，announce 热路径零修改，可灰度上线（先开 gossip 只读接收，验证后再开启发送）。

### 1.10 可扩展性

| 维度 | 上限 |
|---|---|
| 实例数 | 带宽恒定 ~2 Mbps/实例，CPU 随 N 线性增长（连接维护）。实际上限 ~50~100 实例，受限于单实例 P2P 连接数和 seen_msgs 内存 |
| 数据规模 | 与 N 无关，每实例全量存储 1000 万 peer（内存 ~3~5GB，在 8GB 预算内）。数据规模不影响 gossip 带宽（仅变更率影响） |
| 传播延迟 | O(log N)，N=100 时 ~5s，可接受 |
| 瓶颈 | 单实例内存（全量副本）和磁盘（全量持久化），而非网络 |

---

## 2. 方案二：分层/分片同步（类 DHT 架构）

### 核心思路

按 key 空间分片，每个 PDC 实例**仅负责一部分数据**（而非全量）。实例间通过类 Kademlia 路由找到负责特定 key 的实例，查询时跨实例转发。数据归属明确，水平扩展通过增加实例分摊数据量。

---

### 2.1 网络拓扑

| 维度 | 设计 |
|---|---|
| 拓扑类型 | **结构化 P2P（DHT 覆盖网）**，实例间维护 Kademlia 风格路由表 |
| 每个实例连接数 | 路由表中 ~O(log N) 个活跃连接（N=10 → ~10，N=100 → ~20），比 gossip 的固定 fanout 略多但有结构 |
| 连接方向 | 按需建立（查询/转发时），长连接缓存 |
| 最大实例数 | 理论无限（DHT 可扩展到百万级节点），实际受单分片数据量和查询转发延迟约束 |
| 与主网 DHT 的关系 | **必须独立**：PDC 实例间的 DHT（"实例 DHT"）与 BitTorrent 主网 DHT 是两个独立覆盖网，共享 Kademlia 算法但 node ID 空间、路由表、端口完全隔离。复用 `routing_table.rs` 的数据结构，但实例 ID 由部署时分配（非随机 DHT node id） |

### 2.2 同步协议

#### 2.2.1 分片键选择

| 候选分片键 | 适用 Repo | 优劣 |
|---|---|---|
| **infohash 前缀 / 一致性哈希** | PeerRepo, InfohashRepo | ✅ Peer 天然按 infohash 分组（`by_infohash`），分片边界清晰。一致性哈希支持虚拟节点，迁移平滑 |
| **node id XOR 距离** | NodeRepo | ✅ 与 Kademlia 路由天然对齐，每个实例负责一个 ID 空间区间。但 NodeRepo 当前按 addr 去重而非 id |
| **url 哈希** | TrackerRepo | 数据量太小（200+），无需分片，全局复制即可 |

**推荐**：
- **PeerRepo / InfohashRepo**：按 `infohash` 做一致性哈希（160 位 ID 空间，虚拟节点数 = 100 × 实例数）
- **NodeRepo**：按 `node_id` 的 XOR 距离分片（每个实例负责连续的 bucket 区间）
- **TrackerRepo**：**全局复制**（200 条，全量同步成本可忽略）

#### 2.2.2 路由协议

扩展 KRPC 协议，新增实例间消息类型：

```
InstanceMessage {
    // 路由类（复用 Kademlia）
    find_instance(target_id) → closest instances
    ping(instance_id) → pong
    
    // 数据转发类
    forward_announce(infohash, peer_info) → ack        // 转发 announce 到负责实例
    forward_query(infohash, limit) → peers              // 转发 peer 查询
    forward_scrape(infohash[]) → scrape_entries         // 转发 scrape
    forward_node_op(node_id, op) → ack                  // 转发 node 操作
    
    // 分片迁移类
    transfer_shard(range_start, range_end, data_stream) // 实例上下线时数据迁移
}
```

- 查询路径：`本地路由表查找 → 若不负责则转发到更近的实例 → 递归直到负责实例 → 结果沿路径返回`
- 最大跳数：O(log N)，N=10 → 2~3 跳，N=100 → 3~4 跳

### 2.3 一致性模型

| 维度 | 说明 |
|---|---|
| 模型 | **分片内强一致（单 writer）**，分片间无事务。每个 key 只有一个负责实例（主），可配 1~2 个副本 |
| 副本一致性 | 主副本同步写（或异步复制）。异步复制下读副本可能短暂落后 |
| 对 PDC 适用性 | ⚠️ 分片归属消除了多副本写冲突，但**查询转发引入网络延迟**，与 <100μs 目标冲突（见 2.5 和第 4 节） |
| 分片迁移期间 | 短暂双写（旧实例 + 新实例），迁移完成后切换路由，窗口秒级 |

### 2.4 数据分类同步策略

| Repo | 分片/复制策略 | 理由 |
|---|---|---|
| **PeerRepo** | ✅ **按 infohash 分片**，每分片 1 主 + 1 副本 | 天然按 infohash 分组，分片后单实例数据量 = 总量/N。announce 必须路由到负责实例（见第 4 节） |
| **InfohashRepo** | ✅ 按 infohash 分片（与 PeerRepo 同分片键） | ref_count 仅在负责实例上维护，**消除冲突热点**。这是分片方案相对 gossip 的核心优势 |
| **NodeRepo** | ✅ 按 node_id 分片 | 爬虫可按分片分配 crawl 范围，各实例只爬自己负责的 ID 区间，减少重复爬取 |
| **TrackerRepo** | ✅ **全局全量复制**（非分片） | 仅 200+ 条，全量复制成本可忽略，且所有实例都需要 tracker 池做发现 |

**多副本策略**：
- 每个分片主副本 1 个 + 异步副本 1 个（共 2 副本）
- 副本用于读负载均衡和故障转移
- 副本同步：主副本异步推送变更（类 gossip 但定向，非随机）
- 副本延迟：100ms~1s

### 2.5 性能影响

#### 2.5.1 对 10 万 QPS / <100μs 的影响——核心矛盾

**这是分片方案的致命问题。**

当前 announce 热路径是纯本地内存（<100μs）。分片后：

- 若 announce 到达**非负责实例**，必须转发到负责实例：
  - 互联网 RTT：10~100ms（跨地域），LAN：0.1~1ms
  - 即使 LAN 部署，1ms = 1000μs，是目标的 **10 倍**
  - 转发 + 等待响应 + 返回，announce 延迟从 <100μs 飙升到 **1~50ms**
- **10 万 QPS 下，每实例需处理 10 万/N 的 announce，其中大部分需要转发**，转发流量 = 10 万 × (98B 请求 + ~200B 响应) ≈ 30 MB/s = 240 Mbps，且全部是同步阻塞调用

**缓解方案**（详见第 4 节）：
1. **写本地 + 异步复制**：announce 写入本地（保持 <100μs），后台异步转发到负责实例。但查询时该 infohash 的 peer 不在负责实例上 → 查询也需要 fanout。
2. **客户端层路由**：在 PDC 前部署 L7 代理，按 infohash 一致性哈希将 announce 路由到正确实例。但 BT 客户端不支持按 infohash 选择 tracker URL。
3. **接受延迟**：将 announce 响应延迟目标放宽到 1~5ms（仍远优于公网 tracker），牺牲 <100μs 指标。

#### 2.5.2 查询转发延迟

spde 请求某 infohash 的 peer 时：
- 若本地负责：<100μs（概率 = 1/N）
- 若需转发：1~2 跳 × RTT = **2~100ms**
- 平均查询延迟 = (1/N) × 100μs + (1-1/N) × 5ms ≈ **5ms**（N=10，LAN）

#### 2.5.3 内存与 CPU

- 内存：每实例仅存 1/N 数据 + 副本，1000 万/N + 副本，N=10 时 ~200 万 + 200 万 = 400 万 peer，内存 ~1.5GB，远低于 8GB
- CPU：分片路由 + 转发占用额外 1~2 核；announce 处理因数据量减小而更快
- 磁盘 IO：不变（异步持久化）

### 2.6 NAT 穿透

与方案一相同，复用 NatService。但分片方案对连通性要求更高：
- 查询转发必须能到达负责实例，无法穿透时查询失败（无全量本地兜底）
- 建议至少 1~2 个公网可达实例作为"中继根节点"，保证路由可达
- 待确认：NAT 后的实例是否适合作为分片主（可能导致该分片不可达）

### 2.7 安全与认证

| 威胁 | 防护 |
|---|---|
| 恶意实例冒充负责分片 | 实例 ID 与分片归属由**一致性哈希 + 签名**绑定，路由表中的实例条目带签名 |
| 转发路径篡改 | 逐跳签名，或端到端签名（origin → target） |
| Sybil 攻击 | 同方案一，准入制。分片方案下 Sybil 可通过大量实例"抢占"分片，危害更大，需严格准入 |
| 分片数据泄露 | 实例间传输加密（TLS 或 Noise 协议） |

### 2.8 冲突解决

| 冲突场景 | 解决策略 |
|---|---|
| Infohash ref_count | ✅ **无冲突**：仅负责实例维护，单 writer |
| Peer 重复添加 | 负责实例去重（by_infohash HashSet），天然解决 |
| 分片迁移期间双写 | 迁移窗口内旧实例和新实例都接受写，迁移完成后旧实例停止服务该分片，路由更新。短暂不一致（秒级） |
| 副本与主不一致 | 异步复制延迟，读主保证一致；读副本可能落后，副本数据带 version，过期读可检测 |
| Node 评分 | 各分片独立计算，无跨分片冲突 |

### 2.9 实现复杂度

| 模块 | 新增/改造 | 工作量估算 |
|---|---|---|
| 实例 DHT 路由层 | 新增：独立路由表、find_instance 协议、实例 ID 管理 | 大（~1000 行） |
| 分片管理层 | 新增：一致性哈希环、虚拟节点、分片归属计算 | 中（~400 行） |
| 查询转发代理 | 新增：announce/query/scrape 的转发逻辑，**侵入 announce 热路径** | 大（~800 行） |
| 分片迁移引擎 | 新增：实例上下线时的数据传输、双写、切换 | 大（~700 行） |
| 副本同步 | 新增：主→副本异步复制 | 中（~400 行） |
| PeerRepo 分片化改造 | 改造：仅加载/存储本分片数据，查询需判断归属 | 中（~300 行） |
| NodeRepo 分片化改造 | 改造：按 node_id 分片，爬虫按分片分配 | 中（~300 行） |
| P2P 传输层 + 安全 | 同方案一 | 中（~800 行） |
| **合计** | | **大型改造，~4700 行，announce 热路径需重构** |

**关键风险**：announce 热路径从纯本地变为可能涉及网络转发，重构风险高，且可能无法达到 <100μs。

### 2.10 可扩展性

| 维度 | 上限 |
|---|---|
| 实例数 | 理论无限（DHT），实际受查询转发延迟约束。N > 20 时平均查询跳数 ≥ 3，延迟不可接受 |
| 数据规模 | 水平扩展：N 实例支撑 N × 单实例容量。1000 万 peer 在 N=3 时即可轻松承载，无需分片 |
| 传播/查询延迟 | O(log N) 跳 × RTT，N=10 → 2 跳 × 1ms(LAN) = 2ms；跨地域 → 2 跳 × 30ms = 60ms |
| 瓶颈 | **网络延迟**（非带宽、非内存）。分片方案的扩展性受限于光速，而非资源 |

---

## 3. 两方案横向对比

| 维度 | 方案一：Gossip | 方案二：分片/DHT | 胜者 |
|---|---|---|---|
| **网络拓扑** | 部分网状，f=4~8 邻居 | 结构化 DHT，O(log N) 连接 | 平 |
| **announce 热路径** | ✅ 零侵入，保持 <100μs | ❌ 需转发，延迟 1~50ms | **Gossip** |
| **一致性模型** | 最终一致，LWW | 分片内强一致，单 writer | 分片（理论上） |
| **Infohash ref_count** | ⚠️ 各实例独立，不精确 | ✅ 单 writer，精确 | **分片** |
| **数据冗余** | 全量副本（N× 存储） | 1 主 + 1 副本（2× 存储） | **分片** |
| **带宽** | ~2 Mbps/实例，与 N 无关 | 转发流量 ~240 Mbps（10 万 QPS） | **Gossip** |
| **内存** | 每实例全量 ~5GB | 每实例 ~总量/N × 2 | **分片** |
| **查询延迟** | ✅ 本地查询 <100μs（全量数据） | ❌ 跨实例转发 2~100ms | **Gossip** |
| **NAT 容错** | ✅ 全量本地副本，断网仍可服务 | ❌ 分片不可达则查询失败 | **Gossip** |
| **实现复杂度** | 中（~2500 行，热路径零修改） | 大（~4700 行，热路径重构） | **Gossip** |
| **水平扩展上限** | ~50 实例（内存/连接约束） | ~20 实例（延迟约束） | 平 |
| **故障恢复** | ✅ 任意实例宕机不影响数据（全量副本） | ⚠️ 主副本宕机需切换到副本（秒级） | **Gossip** |
| **新实例加入** | ✅ 从任一节点 gossip 追赶数据 | ❌ 需等待分片迁移（分钟级） | **Gossip** |
| **爬虫协同** | 各实例独立爬全量（重复爬取） | ✅ 按分片分配 crawl 范围（无重复） | **分片** |
| **适合部署规模** | 小中型（3~20 实例） | 中大型（需精确分片的场景） | 取决于规模 |

---

## 4. 特别分析：分片方案下超级 Tracker 的 announce / scrape 工作机制

这是分片方案最核心的工程难题，单独展开。

### 4.1 问题本质

BT 客户端（qBittorrent 等）配置一个 tracker URL（如 `http://pdc.example.com:6880/announce`），向其发送 announce。客户端**不知道也不关心** PDC 内部有多少实例、哪个实例负责它的 infohash。DNS 解析或负载均衡将请求分配到任意 PDC 实例。

分片后，该 infohash 的 peer 数据**只存在于负责实例**上。接收到 announce 的实例（"入口实例"）不是负责实例时，必须解决：**如何在保持性能的同时，让 announce 写入正确位置，并让响应包含正确的 peer 列表？**

### 4.2 五种候选路由模式

#### 模式 A：同步转发（Synchronous Forward）

```
客户端 → 入口实例 → [RTT] → 负责实例 → 处理 → [RTT] → 入口实例 → 客户端
```

- 入口实例解析 announce 后，同步转发到负责实例，等待响应后返回客户端
- **延迟**：2 × RTT（入口→负责→入口）+ 本地处理。LAN 下 ~2ms，跨地域 ~60ms
- **10 万 QPS 下**：入口实例成为转发代理，需维护大量在途请求，内存和线程压力大
- **结论**：❌ 直接破坏 <100μs，且扩展性差

#### 模式 B：写本地 + 异步复制（Write-Local, Async-Replicate）

```
客户端 → 入口实例 → 本地写入（<100μs）→ 响应客户端
                         └→ 异步转发到负责实例
```

- announce 写入入口实例的本地存储（保持 <100μs），后台异步复制到负责实例
- **问题**：查询该 infohash 时，负责实例上没有这条最新 announce → 负责实例需反向查询所有入口实例，或查询 fanout 到所有实例
- **查询 fanout**：spde 查询 infohash 的 peer 时，负责实例需向所有 N 个实例请求本地数据，合并返回。延迟 = max(RTT) ≈ RTT，带宽 = N × 查询响应
- **结论**：⚠️ announce 快了，但查询退化为全互联 fanout，且数据分散在所有实例（失去分片意义）。本质上退化为"写任意、读全量"的弱模型

#### 模式 C：客户端重定向（Client Redirect）

```
客户端 → 入口实例 → 302 Redirect: http://负责实例:6880/announce
客户端 → 负责实例 → 正常处理（<100μs）
```

- 入口实例检查 infohash 分片归属，若不负责则返回 HTTP 302 重定向到负责实例
- **问题**：
  - BEP 3（HTTP Tracker 协议）**不标准支持重定向**，qBittorrent 等客户端可能不跟随 302
  - UDP Tracker（BEP 15）**无重定向机制**，无法实现
  - 每次 announce 多一次 RTT（重定向），且客户端可能缓存错误的目标
- **结论**：❌ 协议不兼容，UDP 完全不可行

#### 模式 D：入口层一致性哈希代理（Ingress Proxy with Consistent Hashing）

```
客户端 → L4/L7 代理（按 infohash 哈希路由）→ 负责实例 → <100μs 处理
```

- 在 PDC 实例前部署代理层（如 HAProxy + 自定义 Lua，或自研 Rust 代理），解析 announce 包提取 infohash，按一致性哈希转发到负责实例
- **announce 延迟**：代理 → 负责实例 RTT（LAN 0.1~1ms）+ 本地处理 <100μs ≈ **0.2~1ms**
- **UDP 支持**：需代理理解 BEP 15 协议（connect + announce 关联），复杂度高
- **代理本身成为瓶颈和单点**：需代理集群，且代理需感知 PDC 实例拓扑变化
- **结论**：⚠️ 可行但引入新的基础设施层，代理需解析 BT 协议（HTTP 解析简单，UDP 复杂），且增加了一跳延迟。适合有专业运维的大规模部署

#### 模式 E：协作式 announce + 本地部分响应（Cooperative Announce with Partial Local Response）

```
客户端 → 入口实例 → 本地写入 + 本地查询（<100μs，返回部分 peer）
                         └→ 异步转发到负责实例（后台）
```

- 入口实例**立即**用本地已有数据响应客户端（可能不完整，但快速），同时异步将 announce 转发到负责实例
- 客户端拿到部分 peer，足够启动下载；后续 announce 会逐渐拿到更多 peer（因为数据已复制到负责实例，且 gossip/复制会回传）
- **本质**：牺牲响应完整性换取低延迟，接受最终一致性
- **与 Gossip 方案的区别**：Gossip 方案下每个实例有全量数据，响应是完整的；本模式下入口实例只有部分数据，响应不完整
- **结论**：✅ 最务实的分片方案妥协。announce 保持 <100μs，数据最终收敛到负责实例，查询可优先查负责实例 + 本地兜底

### 4.3 Scrape 在分片方案下

Scrape 请求包含 1~N 个 infohash，要求返回每个 infohash 的 complete/incomplete 计数。

| 模式 | 实现 | 延迟 |
|---|---|---|
| 路由到负责实例 | 每个 infohash 转发到其负责实例，并行查询，合并结果 | max(RTT) ≈ 1~30ms |
| 本地近似计数 | 各实例维护本分片的精确计数 + 通过 gossip 同步其他分片的近似计数 | <1ms（本地），但计数有偏差 |
| 推荐 | **路由到负责实例**。scrape 频率远低于 announce（客户端通常每 15~30 分钟一次），延迟容忍度高，精确计数更重要 | 1~30ms |

UDP scrape 当前未实现，分片后若实现 UDP scrape，同样需路由。

### 4.4 分片方案 announce 总结

| 模式 | announce 延迟 | 协议兼容 | 实现复杂度 | 推荐度 |
|---|---|---|---|---|
| A 同步转发 | 1~50ms ❌ | ✅ | 中 | ❌ |
| B 写本地+异步复制 | <100μs ✅ | ✅ | 中 | ⚠️ 查询退化 |
| C 客户端重定向 | 2×RTT | ❌ UDP | 低 | ❌ |
| D 入口代理 | 0.2~1ms | ✅(需代理) | 高 | ⚠️ 大规模 |
| **E 协作式+部分响应** | **<100μs ✅** | **✅** | **中** | **✅ 推荐** |

**核心结论**：分片方案若要保持 <100μs，必须采用模式 E（写本地 + 部分响应 + 异步复制），这实际上**弱化了分片的归属意义**——数据仍然分散在各实例，查询仍需跨实例合并。此时分片方案与 Gossip 方案的界限变得模糊，而 Gossip 方案的全量副本模型在查询完整性和 NAT 容错上更优。

---

## 5. 特别分析：Gossip 方案千万级 Peer 带宽开销量化估算

（已在 1.5.2 节给出核心模型，本节补充敏感性分析和极端场景。）

### 5.1 基准模型回顾

- 有效变更率 R ≈ 4,300 条/s（新 peer 2778 + score 更新 1000 + node 变更 500 + 其他）
- 平均单条大小 ≈ 35 B
- fanout f=4，rumor mongering 每变更每实例平均转发 1 次
- **每实例出口带宽 ≈ 1.8 Mbps**（含 1.5× 冗余）

### 5.2 敏感性分析

| 变量 | 基准值 | 极端值 | 带宽影响 |
|---|---|---|---|
| Peer TTL | 3600s | 600s（更短过期） | 新 peer 率 ×6 → 16,667/s → 带宽 ~10 Mbps |
| announce QPS | 100,000 | 1,000,000（10× 目标） | 若 last_active 不同步，无影响；若同步则 ×10 → 240 Mbps ❌ |
| fanout f | 4 | 8 | 冗余系数 ×1.5 → ~2.7 Mbps |
| 实例数 N | 10 | 100 | 每实例带宽基本不变（~2 Mbps），总带宽 ×10 |
| 压缩率 | 无 | gzip 3:1 | 带宽降至 ~0.6 Mbps |
| score 更新频率 | 10s 增量 | 1s 增量 | score 变更 ×10 → ~5 Mbps |

### 5.3 极端场景：新实例冷启动

新实例加入时需从已有实例获取全量 1000 万 peer：

- 全量数据大小：10M × 35B = 350 MB（紧凑二进制），gzip 后 ~100 MB
- 从单实例拉取：100 MB ÷ 100 Mbps = 8s；÷ 1 Gbps = 0.8s
- 从多实例并行拉取（按 infohash 分片范围）：可加速到 2~3s
- **冷启动期间对提供数据的实例造成带宽冲击**：需限流（如 50 MB/s），避免影响 announce
- 冷启动期间新实例数据不完整，可标记为"warmup"状态，不参与查询响应（或仅响应已有数据）

### 5.4 带宽结论

Gossip 方案在"选择性同步"策略下，千万级 peer 的带宽开销**每实例恒定 ~2 Mbps**，与实例数无关，完全在普通家用宽带（上行 30~100 Mbps）和云服务器（≥100 Mbps）的能力范围内。**这是 Gossip 方案最被低估的优势**——直觉上 gossip 带宽放大严重，但通过只同步"存在性+评分"而不同步"时效性字段"，变更率降低了两个数量级。

---

## 6. 初步结论

### 6.1 推荐方案：Gossip 协议 P2P 同步（方案一）

**核心理由**：

1. **announce 热路径零侵入**：PDC 的核心性能目标（10 万 QPS、<100μs）完全依赖纯本地内存架构。Gossip 是异步后台任务，不触碰热路径。分片方案无论采用哪种路由模式，要么破坏延迟目标，要么退化为弱模型。

2. **带宽可控**：通过"时效性字段本地化、存在性/评分字段 gossip 化"的策略，千万级 peer 下每实例仅 ~2 Mbps，与实例数无关。

3. **NAT 容错与可用性**：全量副本意味着任何实例断网/分区后仍可独立服务所有查询，数据不丢失。分片方案下分片不可达则该 infohash 的查询失败。

4. **实现复杂度低**：~2500 行新增，核心是 gossip 引擎和 PeerRepo dirty 补充，可灰度上线。分片方案需重构 announce 热路径，风险高。

5. **PDC 数据特征适配**：peer 数据天然容忍最终一致性（少几个 peer 不影响下载），node 评分异步重算本身就是最终一致，infohash ref_count 的不精确可以接受（它只用于清理零引用，不用于查询路径）。

### 6.2 分片方案的适用场景

分片方案并非一无是处，在以下场景可考虑：

- **单实例内存不足以承载全量数据**：若 peer 规模从 1000 万增长到 1 亿+，单实例 8GB 内存不够，必须分片。
- **需要精确 ref_count**：若业务上依赖全局精确的 infohash 引用计数（当前不依赖）。
- **爬虫去重**：分片后各实例按 node_id 区间爬取，避免重复爬取，可降低 DHT 网络压力。
- **有专业 L7 代理基础设施**：模式 D（入口代理）在大规模部署下可工作。

**建议**：当前阶段（1000 万目标、3~10 实例）采用 Gossip。未来若数据量突破单实例内存上限，可在 Gossip 基础上**叠加分片**（混合架构：分片内全量 + 分片间 gossip），作为演进方向。

### 6.3 Gossip 方案的实施优先级建议

| 阶段 | 内容 | 目标 |
|---|---|---|
| P0 | PeerRepo 增加 dirty 机制 + GossipEngine 骨架（仅 NodeRepo 同步） | 验证 gossip 传输和成员管理 |
| P1 | 接入 PeerRepo 新 peer 发现 + score 同步 | 核心数据同步生效 |
| P2 | Infohash 存在性同步 + Tracker 全量同步 | 四大 Repo 全覆盖 |
| P3 | Anti-Entropy Merkle 对账 + 实例签名认证 | 生产级可靠性 |
| P4 | 新实例冷启动优化（并行拉取 + 限流） | 支持动态扩缩 |

### 6.4 待确认事项

1. **部署规模**：预期 PDC 实例数是多少？（3~10 / 10~50 / 50+）直接影响方案选择
2. **部署环境**：实例是否都在公网？还是部分在 NAT 后？是否有公网中继节点？
3. **跨地域需求**：实例是否跨地域部署？（跨地域 RTT 高，对分片方案更不利，对 gossip 无影响）
4. **infohash ref_count 精确性**：业务上是否需要全局精确 ref_count？（当前代码中 ref_count 仅用于 `cleanup_zero_ref`，不精确可接受）
5. **spde 查询模式**：spde 查询 peer 时是否容忍秒级数据延迟？还是需要实时精确？
6. **P2P 端口**：新增 6885 端口是否可接受？是否需要复用 6880？

---

> **报告结束**。本报告基于 2026-09-09 的 PDC 代码状态，后续代码变更可能影响分析结论。
