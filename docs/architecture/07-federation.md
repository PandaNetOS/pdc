# PDC 联邦网络架构文档

> 版本：阶段 3（中继 + TrackerRepo 同步 + REST API + WebSocket + 生产增强）
> 日期：2026-09-09

## 1. 架构概述

PDC 联邦网络是一个去中心化的 P2P 节点发现与数据同步网络，允许多个 PDC 实例组成联邦，共享 DHT 节点、Peer、Infohash 和 Tracker 数据。

### 三层架构



```
┌─────────────────────────────────────────────────────┐

│                   应用场景层                          │

│  REST API / WebSocket / 监控指标 / 状态快照           │

├─────────────────────────────────────────────────────┤

│                   同步与业务层                        │

│  Gossip引擎 │ Merkle对账 │ 4种Repo同步 │ 数据中继     │

├─────────────────────────────────────────────────────┤

│                   连接与传输层                        │

│  连接管理 │ 节点发现 │ 节点表 │ 打洞信令 │ NAT集成    │

├─────────────────────────────────────────────────────┤

│                   传输层                              │

│  TCP帧协议 │ UDP打洞 │ Ed25519认证 │ 二进制序列化    │

└─────────────────────────────────────────────────────┘
```

### 模块清单



| 模块          | 文件                      | 职责                              |
| ----------- | ----------------------- | ------------------------------- |
| 配置          | `config.rs`             | FederationConfig 定义与默认值         |
| 节点身份        | `node_id.rs`            | NodeId、NodeIdentity、Ed25519 密钥对 |
| 协议          | `protocol.rs`           | 二进制帧协议、消息类型、消息结构体               |
| 传输          | `transport.rs`          | TCP 传输层（粘包处理）、UDP 打洞传输          |
| 节点表         | `node_table.rs`         | 节点路由表、状态管理、邻居选择                 |
| 连接管理        | `connection.rs`         | TCP 连接池、握手、心跳、消息分发              |
| 节点发现        | `discovery.rs`          | 种子引导、PEX 交换、GetNodes            |
| NAT 集成      | `nat_integration.rs`    | UPnP 映射、STUN 探测、公网地址刷新          |
| Gossip 引擎   | `gossip.rs`             | 流行病传播、消息去重、反熵触发                 |
| Merkle 树    | `merkle.rs`             | 分片 Merkle 树、blake3 哈希、差异检测      |
| 同步管理        | `sync/mod.rs`           | 统一同步入口、Repo 分发                  |
| Peer 同步     | `sync/peer_sync.rs`     | PeerRepo Gossip 同步              |
| Infohash 同步 | `sync/infohash_sync.rs` | InfohashRepo Gossip 同步          |
| Tracker 同步  | `sync/tracker_sync.rs`  | TrackerRepo 全量对账 + Gossip       |
| 打洞信令        | `signaling.rs`          | NAT 打洞信令中继、UDP 打洞               |
| 数据中继        | `relay.rs`              | 中继通道、令牌桶限流、数据转发                 |
| 监控指标        | `metrics.rs`            | 原子计数器、指标快照                      |
| 主入口         | `mod.rs`                | FederationService、生命周期、状态快照     |

## 2. 节点身份与认证

### NodeId



* 20 字节随机数，与 BitTorrent DHT node\_id 格式一致

* 实现 Hash/Eq/Clone/Copy/Debug/Display

* Display 输出 40 字符十六进制

### Ed25519 密钥对



* `NodeIdentity.signing_key: SigningKey` — 32 字节种子

* `public_key() -> VerifyingKey` — 派生公钥

* `sign(data) -> Signature` — 签名

* `verify(public_key, data, signature) -> bool` — 静态验证

* 持久化：`data/federation/identity.bin`（20 字节 node\_id + 32 字节 signing\_key 种子）

* 向后兼容：阶段 1 的 52 字节文件直接当作 signing\_key 种子加载

### 握手认证



1. 发起方发送 `HelloMessage`（含 public\_key + signature）

2. 接收方调用 `verify_signature()` 验证

3. 验证失败：断开连接 + warn 日志

4. 验证通过：回复 `HelloAck`

签名内容：node\_id + addresses + version + is\_relay + public\_key 的 bincode 序列化

## 3. 二进制帧协议规范

### 帧格式



```
┌──────────┬──────────┬─────────────────┐

│ 4字节长度 │ 1字节类型 │    payload      │

│ (大端)   │          │ (bincode序列化)  │

└──────────┴──────────┴─────────────────┘
```



* 长度字段包含类型字节 + payload（不含长度字段本身）

* 最大帧大小：64MB（防攻击）

* TCP 使用 `set_nodelay(true)`

### 消息类型



| 值  | 类型            | 说明          | 阶段 |
| -- | ------------- | ----------- | -- |
| 0  | Hello         | 握手请求        | 1  |
| 1  | HelloAck      | 握手响应        | 1  |
| 2  | Ping          | 心跳请求        | 1  |
| 3  | Pong          | 心跳响应        | 1  |
| 4  | GetNodes      | 请求节点列表      | 1  |
| 5  | Nodes         | 节点列表响应      | 1  |
| 6  | ExchangeNodes | PEX 节点交换    | 1  |
| 7  | Signaling     | 打洞信令        | 2  |
| 8  | RelaySetup    | 中继建立        | 3  |
| 9  | RelayData     | 中继数据        | 3  |
| 10 | GossipBatch   | Gossip 批量传播 | 2  |
| 11 | MerkleDigest  | Merkle 摘要对账 | 2  |
| 12 | MerkleRequest | Merkle 差异请求 | 2  |
| 13 | SyncBatch     | 全量同步批量      | 1  |
| 14 | Goodbye       | 优雅关闭通知      | 1  |

### 关键消息结构



```
// 握手

HelloMessage { node\_id, public\_key, signature, addresses, version, is\_relay }

// Gossip

GossipBatch { msg\_id, origin, repo\_type, entries, timestamp }

SyncEntry { key, operation, version, payload }

// 中继

RelaySetupMessage { channel\_id, target\_node, action }  // action: 0=Request,1=Accept,2=Reject,3=Close

RelayDataMessage { channel\_id, data }

// Merkle

MerkleDigest { repo\_type, shard\_count, roots, entry\_counts }
```

### repo\_type 常量



| 值 | 仓库           |
| - | ------------ |
| 1 | NodeRepo     |
| 2 | PeerRepo     |
| 3 | InfohashRepo |
| 4 | TrackerRepo  |

### operation 常量



| 值 | 操作     |
| - | ------ |
| 0 | Upsert |
| 1 | Delete |

## 4. 节点发现机制

### 种子引导（bootstrap）



1. 读取 `config.seed_nodes`（域名或 IP: 端口列表）

2. 使用 `tokio::net::lookup_host` 解析 DNS

3. 逐个建立 TCP 连接 + 握手

4. 连接成功后发送 `GetNodes` 请求邻居节点

### PEX 交换



* 后台任务每 5 分钟向所有已连接节点发送 `ExchangeNodes`

* 收到节点后加入 node\_table，尝试连接未连接节点（不超过 target\_neighbors=8）

### GetNodes



* 请求方指定 count（默认 16）

* 响应方返回 node\_table 中最活跃的 count 个节点

### 节点表



* `FxHashMap<NodeId, NodeEntry>`

* NodeEntry 含：NodeAddress、status、rtt\_ms、connection\_count、consecutive\_failures、is\_relay

* 状态：Disconnected / Connecting / Connected / Failed

* `random_neighbors(n)` — 随机选择 n 个已连接邻居用于 Gossip

## 5. Gossip 传播协议

### 传播流程



```
本地变更 → submit\_gossip() → outbox队列

&#x20;   ↓ (每1000ms)

取最多10条 → 随机选3个邻居 → 发送 GossipBatch

&#x20;   ↓

接收方 → 检查 msg\_id 是否在 seen\_msgs(LRU)

&#x20;   ├─ 已处理：丢弃

&#x20;   └─ 未处理：加入seen\_msgs → 写入本地Repo → 加入outbox继续转发（排除来源）
```

### 关键参数



* `gossip_interval_ms`: 1000ms（传播间隔）

* `gossip_fanout`: 3（每批传播邻居数）

* `seen_msgs`: LRU 缓存（默认 10000 条）

* 每批最多取 10 条 outbox 消息

### 反熵（Anti-Entropy）



* 每 60 秒随机选 1 个邻居

* 发送 MerkleDigest 进行对账

* 差异分片通过 MerkleRequest 请求具体条目

## 6. Merkle 反熵机制

### 分片 Merkle 树



* 固定 256 个分片

* 分片选择：`blake3(key).first_byte % 256`

* 分片根：分片内所有条目按 key 排序后，依次 `hash(key) + hash(data)` 拼接，最终 blake3

### 对账流程



1. A 发送 `MerkleDigest { repo_type, shard_count=256, roots, entry_counts }` 给 B

2. B 对比本地 roots，找出不同的分片索引

3. B 请求差异分片的所有条目（`get_shard_entries(shard)`）

4. B 应用差异条目到本地 Repo

### 用途



* NodeRepo：阶段 1 全量同步（SyncBatch）

* PeerRepo/InfohashRepo：阶段 2 Gossip + Merkle 对账

* TrackerRepo：阶段 3 每小时全量对账 + 变更即时 Gossip

## 7. 四种 Repo 同步策略

### NodeRepo（repo\_type=1）



* **触发**：每 300 秒（sync\_node\_interval\_secs）

* **方式**：`node_repo.take_dirty_sync()` 取出脏节点 → SyncBatch 广播

* **冲突解决**：LWW（Last Write Wins），version 大的覆盖

* **数据**：SocketAddr 序列化为 SyncEntry（key=ip:port 字节，payload = 节点数据）

### PeerRepo（repo\_type=2）



* **触发**：EventBus `Event::PeerDiscovered` 事件驱动

* **方式**：Gossip 即时传播

* **同步字段**：infohash + addr + first\_seen + source（不同步评分 / 连接次数等本地计算字段）

* **写入**：`peer_repo.add_peers_sync()`，source 标记为 "Federation"

### InfohashRepo（repo\_type=3）



* **触发**：EventBus `Event::InfohashSeen` 事件驱动

* **方式**：Gossip 即时传播

* **同步字段**：infohash (20 字节) + seen\_at 时间戳

* **只同步存在性**，不同步元数据

### TrackerRepo（repo\_type=4）



* **触发**：每小时全量对账 + 变更即时 Gossip

* **方式**：Merkle 对账 + Gossip 传播

* **同步字段**：url + disabled 状态

* **限制**：TrackerRepoImpl 无 remove 方法，删除操作忽略

* **全量同步**：`all_trackers_sync()` → 更新 MerkleTree → 与随机邻居交换摘要

## 8. NAT 穿透与打洞信令

### NAT 类型检测



* UPnP 映射：`NatManager.init()` 映射联邦端口（TCP+UDP）

* STUN 探测：`crate::nat::stun` 模块获取公网地址和 NAT 类型

* IPv6 地址：直接判定为 PublicIpv6

### Reachability 等级



| 等级            | 条件             |
| ------------- | -------------- |
| PublicIpv6    | 有公网 IPv6 地址    |
| Mapped        | UPnP 映射成功      |
| HolePunchable | STUN 检测为锥形 NAT |
| OutboundOnly  | 对称 NAT，只能主动出站  |
| Unknown       | 无法判定           |

### 打洞信令流程



```
A 想连接 B（都在NAT后）

&#x20;   ↓

A → 中继节点R → Signaling(Request, A的公网地址, NAT类型)

&#x20;   ↓

R → B → 转发 Signaling(Request)

&#x20;   ↓

B → R → Signaling(Response, B的公网地址)

&#x20;   ↓

R → A → 转发 Signaling(Response)

&#x20;   ↓

A 和 B 同时调用 UdpTransport.hole\_punch() 向对方公网地址发包（持续5秒）

&#x20;   ↓

打洞成功 → UDP直连
```

### UDP 打洞包格式



```
┌──────────────┬──────────────┐

│ 4字节魔数     │ 20字节node\_id │

│ 0x50444346   │              │

│ ("PDCF")     │              │

└──────────────┴──────────────┘
```

### 地址刷新



* 后台任务每 5 分钟重新探测公网地址

* 地址变化时更新 identity.addresses

* 向所有连接发送新的 ExchangeNodes（含新地址）

## 9. 中继协议与限流

### 中继通道



```
A → R（中继）→ B
```



* A 通过已连接节点 R 建立到 B 的中继通道

* R 检查过载状态后接受或拒绝

* 数据通过 `RelayData` 消息在通道中转发

### 中继建立流程



1. A 生成 channel\_id，发送 `RelaySetup(Request, channel_id, target=B)` 给 R

2. R 检查：当前通道数 \<relay\_max\_connections (5) 且带宽未超限

3. R 转发 Request 给 B（如果 B 已连接）

4. B 回复 `RelaySetup(Accept)` 给 R

5. R 转发 Accept 给 A

6. 通道建立，A 和 B 通过 R 转发 `RelayData`

### 令牌桶限流



* `BandwidthTracker` 滑动窗口计数

* 总带宽限制：`relay_bandwidth_limit_mbps`（默认 10Mbps）

* 单连接限制：默认 2Mbps

* `try_consume(bytes, channel_id) -> bool` — 超限返回 false，数据丢弃

* 窗口：1 秒滑动窗口，AtomicU64 + Instant

### 通道管理



* 超时清理：每 30 秒检查，>120 秒无活动的通道自动关闭

* 关闭时发送 `RelaySetup(Close)` 通知对端

* `close_all()` — 优雅关闭时关闭所有通道

### 过载保护



* 当前中继连接数 >= relay\_max\_connections（默认 5）→ 拒绝新请求

* 总带宽超限 → 拒绝新请求

* 拒绝时发送 `RelaySetup(Reject)`

## 10. 配置说明

### FederationConfig 字段



| 字段                            | 类型     | 默认值   | 说明                      |
| ----------------------------- | ------ | ----- | ----------------------- |
| enabled                       | bool   | false | 联邦网络总开关                 |
| node\_id                      | Option | None  | 手动指定 node\_id（40 位 hex） |
| seed\_nodes                   | Vec    | \[]   | 种子节点列表（域名或 IP: 端口）      |
| listen\_port                  | u16    | 6885  | 联邦监听端口（TCP+UDP 共用）      |
| max\_connections              | usize  | 32    | 最大入站连接数                 |
| target\_neighbors             | usize  | 8     | 目标邻居数                   |
| heartbeat\_interval\_secs     | u64    | 30    | 心跳间隔                    |
| heartbeat\_timeout\_secs      | u64    | 90    | 心跳超时（无响应断开）             |
| nat\_mapping\_enabled         | bool   | true  | UPnP 端口映射开关             |
| stun\_servers                 | Vec    | \[]   | STUN 服务器列表              |
| enable\_relay                 | bool   | true  | 中继功能开关                  |
| relay\_bandwidth\_limit\_mbps | u32    | 10    | 中继总带宽限制                 |
| relay\_max\_connections       | usize  | 5     | 最大中继通道数                 |
| gossip\_interval\_ms          | u64    | 1000  | Gossip 传播间隔             |
| gossip\_fanout                | usize  | 3     | Gossip 每批邻居数            |
| sync\_peer\_enabled           | bool   | true  | Peer 同步开关               |
| sync\_node\_enabled           | bool   | true  | Node 同步开关               |
| sync\_node\_interval\_secs    | u64    | 300   | Node 全量同步间隔             |
| sync\_infohash\_enabled       | bool   | true  | Infohash 同步开关           |
| sync\_tracker\_enabled        | bool   | true  | Tracker 同步开关            |

### YAML 配置示例



```
federation:

&#x20; enabled: true

&#x20; seed\_nodes:

&#x20;   - "pdc-node1.example.com:6885"

&#x20;   - "192.168.1.100:6885"

&#x20; listen\_port: 6885

&#x20; target\_neighbors: 8

&#x20; enable\_relay: true

&#x20; relay\_bandwidth\_limit\_mbps: 10

&#x20; sync\_node\_enabled: true

&#x20; sync\_peer\_enabled: true

&#x20; sync\_infohash\_enabled: true

&#x20; sync\_tracker\_enabled: true
```

## 11. 部署指南

### 单节点测试



```
\# 节点A（监听6885）

federation:

&#x20; enabled: true

&#x20; listen\_port: 6885

\# 节点B（监听6886，连接A）

federation:

&#x20; enabled: true

&#x20; listen\_port: 6886

&#x20; seed\_nodes:

&#x20;   \- "127.0.0.1:6885"
```

### 多节点组网



```
节点A (公网, 6885) ← 种子节点

&#x20;   ↑

节点B (NAT, 6885) ── 连接A，通过A发现C

&#x20;   ↑

节点C (NAT, 6885) ── 连接A，PEX交换后直连B
```



1. 部署 1 个公网种子节点（配置 port forwarding 6885）

2. 其他节点配置 seed\_nodes 指向种子节点

3. 节点通过 PEX 自动发现更多邻居

4. NAT 后的节点通过打洞或中继建立连接

### 系统要求



* 端口：6885/TCP（联邦协议）、6885/UDP（打洞）

* 内存：< 512MB（联邦模块本身）

* 带宽：中继模式下额外消耗（默认上限 10Mbps）

## 12. REST API 文档

所有接口前缀：`/api/v1/federation`

### GET /status

返回联邦服务状态。

**响应示例：**



```
{

&#x20; "enabled": true,

&#x20; "node\_id": "a1b2c3d4e5f6...",

&#x20; "connections": 5,

&#x20; "known\_nodes": 128,

&#x20; "reachability": "Mapped",

&#x20; "uptime\_secs": 3600,

&#x20; "gossip\_queue\_size": 2,

&#x20; "relay\_channels": 1,

&#x20; "tracker\_sync\_enabled": true,

&#x20; "metrics": { ... }

}
```

### GET /nodes?limit=50

返回已知节点列表。

**查询参数：**



* `limit`（可选，默认 50）：返回节点数上限

**响应示例：**



```
{

&#x20; "total": 128,

&#x20; "returned": 50,

&#x20; "nodes": \[

&#x20;   {

&#x20;     "node\_id": "...",

&#x20;     "addr": "1.2.3.4:6885",

&#x20;     "status": "Connected",

&#x20;     "rtt\_ms": 45,

&#x20;     "last\_seen\_secs\_ago": 12

&#x20;   }

&#x20; ]

}
```

### GET /connections

返回当前活跃连接列表。

**响应示例：**



```
{

&#x20; "count": 5,

&#x20; "connections": \[

&#x20;   {

&#x20;     "node\_id": "...",

&#x20;     "addr": "1.2.3.4:6885",

&#x20;     "connected\_at\_secs": 1800,

&#x20;     "rtt\_ms": 45

&#x20;   }

&#x20; ]

}
```

### GET /sync-stats

返回同步统计、中继统计和指标。

**响应示例：**



```
{

&#x20; "sync\_stats": {

&#x20;   "node\_sync\_count": 0,

&#x20;   "peer\_sync\_count": 0,

&#x20;   "infohash\_sync\_count": 0,

&#x20;   "tracker\_sync\_count": 0,

&#x20;   "gossip\_propagations": 1523

&#x20; },

&#x20; "relay\_stats": {

&#x20;   "active\_channels": 1,

&#x20;   "total\_bytes\_forwarded": 1048576,

&#x20;   "current\_bandwidth\_mbps": 0.5

&#x20; },

&#x20; "metrics": { ... }

}
```

### 错误响应

联邦未启用时所有接口返回：



```
{ "error": "federation not enabled" }
```

HTTP 状态码：404

## 13. WebSocket 推送

联邦状态通过现有 WebSocket 接口每 5 秒自动推送，在 `collect_status()` 的 `federation` 字段中：



```
{

&#x20; "type": "status",

&#x20; "federation": {

&#x20;   "enabled": true,

&#x20;   "node\_id": "...",

&#x20;   "connections": 5,

&#x20;   "known\_nodes": 128,

&#x20;   "reachability": "Mapped",

&#x20;   "uptime\_secs": 3600,

&#x20;   "gossip\_queue\_size": 2,

&#x20;   "relay\_channels": 1

&#x20; }

}
```

## 14. 性能指标说明

### FederationMetrics 字段



| 指标                      | 说明            |
| ----------------------- | ------------- |
| total\_messages\_sent   | 发送消息总数        |
| total\_messages\_recv   | 接收消息总数        |
| gossip\_propagations    | Gossip 传播次数   |
| gossip\_received        | 收到 Gossip 消息数 |
| sync\_entries\_applied  | 应用同步条目数       |
| hole\_punch\_attempts   | 打洞尝试次数        |
| hole\_punch\_successes  | 打洞成功次数        |
| relay\_bytes\_forwarded | 中继转发总字节数      |
| merkle\_repairs         | Merkle 对账修复次数 |

### 获取方式



* REST API：`GET /api/v1/federation/sync-stats` 返回 `metrics` 字段

* WebSocket：每 5 秒推送的 status 中包含 `metrics`

* `FederationService::status()` 返回 `FederationStatus.metrics`

## 15. 优雅关闭流程



1. 发送 shutdown 广播信号给所有后台任务

2. 关闭所有中继通道（发送 Close 消息）

3. 等待 500ms 让消息发出

4. 向所有 TCP 连接发送 Goodbye 消息

5. 关闭所有 TCP 连接

6. identity 已在 load\_or\_create 时持久化，无需重复

## 16. 线程安全与并发



* 所有共享状态使用 `parking_lot::RwLock` 或 `std::sync::atomic`

* HashMap 使用 `rustc_hash::FxHashMap`（高性能）

* 连接的 transport 使用 `tokio::sync::Mutex`（跨 await 持锁）

* 循环依赖通过 `tokio::sync::OnceCell` 延迟注入

* 后台任务通过 `tokio::spawn` 启动，`broadcast::Sender<()>` 通知关闭

## 17. 测试覆盖

阶段 3 完成后联邦模块共 **98 个单元测试**，覆盖：



* config：默认值、序列化

* node\_id：生成、加载、签名验证

* protocol：编解码、签名构建验证

* transport：帧读写、粘包处理

* node\_table：增删改查、邻居选择、过期清理

* connection：握手、心跳、消息分发

* discovery：引导、PEX、节点处理

* gossip：提交、传播、去重

* merkle：更新、删除、摘要、差异

* metrics：计数、快照

* signaling：信令处理

* relay：建立、拒绝、数据转发、限流、清理（11 个测试）

* sync/mod：Node 同步、Merkle 摘要、未知 repo\_type

* sync/peer\_sync：事件消费、同步应用

* sync/infohash\_sync：事件消费、同步应用

* sync/tracker\_sync：全量同步、应用、Merkle 对账（4 个测试）

* mod：服务创建、启动关闭、身份持久化、状态序列化