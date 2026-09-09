# PDC 多实例外网互联专项分析报告

> **文档版本**: v1.0  
> **日期**: 2026-09-09  
> **范围**: NAT 穿透、安全认证、一致性与冲突解决三个横切关注点  
> **基准代码**: PDC commit at 2026-09-09（`D:\PNOS\pdc`）  
> **约束**: 不修改代码，仅输出分析与设计建议

---

## 目录

- [执行摘要](#执行摘要)
- [专项一：NAT 穿透与外网连接](#专项一nat-穿透与外网连接)
- [专项二：安全与认证](#专项二安全与认证)
- [专项三：一致性模型与冲突解决](#专项三一致性模型与冲突解决)
- [总体实施路线图](#总体实施路线图)
- [附录：关键术语表](#附录关键术语表)

---

## 执行摘要

PDC 已具备较完整的 NAT 基础设施（UPnP/NAT-PMP/PCP/STUN/UDP 打洞/自定义中继/IPv6），但缺少 **TURN 标准中继协议**、**ICE 交互式连接建立**、**传输层加密**和**实例级加密身份**。数据层四大 Repo 的结构天然适配 CRDT（引用计数→PN-Counter，Peer 集合→OR-Set，节点属性→LWW-Register），无需强一致协议。

**核心结论**：

| 维度 | 推荐方案 | 核心理由 |
|---|---|---|
| NAT 穿透 | UPnP→STUN 打洞→TURN 中继三级降级，复用现有 6881 中继端口升级为 TURN 兼容 | 覆盖 95%+ 连接场景，中继仅兜底 |
| 安全认证 | Ed25519 实例身份 + Noise XX 握手 + 消息签名 + TOFU 信任锚 | 轻量、1-RTT、与 pnos-runtime token 互补 |
| 一致性 | 最终一致 + 自定义 CRDT（PN-Counter/OR-Set/LWW-Register）+ HLC 逻辑时钟 | 数据特征决定强一致无必要，CRDT 天然冲突无关 |
| 同步通道 | 独立端口（建议 6883），与 6880 Tracker 服务通道物理隔离 | 加密握手不污染 <100μs Tracker 响应路径 |

---

## 专项一：NAT 穿透与外网连接

### 1.1 PDC 现有网络能力盘点

| 能力 | 实现位置 | 状态 | 说明 |
|---|---|---|---|
| UPnP IGD | `nat/mod.rs` + `igd` crate | ✅ 已实现 | 支持 TCP/UDP 映射、自动续租、冲突重试、映射验证 |
| NAT-PMP | `nat/nat_pmp.rs` | ✅ 已实现 | Apple 路由器/部分 OpenWrt 支持 |
| PCP | `nat/pcp.rs` | ✅ 已实现 | NAT-PMP 后继协议，企业级网关支持 |
| STUN Binding | `nat/stun.rs` | ✅ 已实现 | RFC 5389，支持 XOR-MAPPED-ADDRESS，双服务器 NAT 类型检测 |
| UDP 打洞 | `nat/udp_hole_punch.rs` | ✅ 已实现 | 支持 Symmetric NAT 端口预测，按 NAT 类型选策略 |
| 自定义中继 | `data_plane/relay.rs` | ✅ 已实现 | 端口 6881，UDP+TCP，magic byte `0x5044`，广播式转发 |
| 打洞信令 | `data_plane/hole_punch_signaling.rs` | ✅ 已实现 | HTTP REST，会话管理，长轮询等待对端 |
| IPv6 | `nat/ipv6.rs` | ✅ 已实现 | 双栈支持 |
| TURN (RFC 5766) | — | ❌ 未实现 | 标准中继协议，当前自定义中继不兼容 |
| ICE (RFC 8445) | — | ❌ 未实现 | STUN+TURN 组合的标准化连接建立流程 |
| TCP 打洞 | — | ❌ 未实现 | 仅 UDP 打洞 |

**关键发现**：PDC 的 NAT 模块已覆盖 WebRTC ICE 所需的全部基础原语（STUN 客户端、UDP 打洞、中继服务器、信令通道），但缺少将它们编排为标准 ICE 流程的逻辑。当前自定义中继协议（magic byte + peer_id + 广播）无法与标准 TURN 客户端互操作，且缺少带宽限流和认证。

### 1.2 穿透方案对比

| 方案 | 适用 NAT 类型 | 成功率 | 延迟开销 | 带宽成本 | 实现复杂度 | PDC 现状 |
|---|---|---|---|---|---|---|
| **UPnP / NAT-PMP / PCP** | 家用路由器（非 CGNAT） | 65-80%¹ | 映射建立 ~1-3s | 0（直连） | 低 | ✅ 已有 |
| **STUN 地址发现** | 所有（仅发现，不穿透） | 95%+ | ~50-200ms RTT | 0 | 低 | ✅ 已有 |
| **UDP 打洞（STUN 辅助）** | Cone-Cone, Cone-PortRestricted | 70-85%² | 打洞 ~100-500ms | 0（直连） | 中 | ✅ 已有 |
| **TCP 打洞** | Cone-Cone | 50-70%³ | ~200-800ms | 0（直连） | 中高 | ❌ 未实现 |
| **TURN 中继** | 所有（含 Symmetric+Symmetric） | 100% | +中继 RTT（通常 30-100ms） | **2x 流量**（进+出） | 高 | ⚠️ 自定义中继，非标准 |
| **ICE（STUN+TURN 编排）** | 所有 | 85-92% 直连⁴，余走中继 | 连接建立 ~500ms-2s | 直连 0，中继 2x | 高 | ❌ 未实现 |
| **公网中继节点（中心化）** | 所有 | 100% | +中继 RTT | 2x 流量，且**单点瓶颈** | 低 | ⚠️ 6881 中继可承担 |
| **反向连接（对端有公网）** | 至少一方有公网/Full Cone | 取决于对端 | 0 | 0 | 低 | 隐式支持 |

> ¹ 家用路由器 UPnP 启用率约 70-80%，但 CGNAT（运营商级 NAT）下完全无效；企业网络通常禁用 UPnP。  
> ² WebRTC 统计：约 8-15% 的通话最终需要 TURN 中继，其余通过 STUN 直连成功。  
> ³ TCP 打洞受 TCP 序列号预测和 NAT 状态机影响，成功率显著低于 UDP。  
> ⁴ Google WebRTC 遥测数据：ICE 直连成功率约 86%，TURN 兜底约 14%。

### 1.3 NAT 类型组合穿透成功率矩阵

下表为两个 PDC 实例在不同 NAT 类型组合下，**仅使用 UDP 打洞（无中继）** 的预期成功率：

| 发起方 \ 目标方 | Open Internet | Full Cone | Restricted Cone | Port Restricted | Symmetric | CGNAT |
|---|---|---|---|---|---|---|
| **Open Internet** | 100% | 100% | 100% | 100% | 100%⁵ | 100%⁵ |
| **Full Cone** | 100% | 95%+ | 90%+ | 85%+ | 60%⁶ | 85%+ |
| **Restricted Cone** | 100% | 90%+ | 85%+ | 80%+ | 40%⁶ | 80%+ |
| **Port Restricted** | 100% | 85%+ | 80%+ | 75%+ | 30%⁶ | 75%+ |
| **Symmetric** | 100%⁵ | 60%⁶ | 40%⁶ | 30%⁶ | **<5%**⁷ | 30%⁶ |
| **CGNAT** | 100%⁵ | 85%+ | 80%+ | 75%+ | 30%⁶ | 70%+⁸ |

> ⁵ 一方有公网 IP，另一方直接连接即可，无需打洞。  
> ⁶ Symmetric NAT 出站端口随目标变化，需端口预测；PDC 已实现 `enable_port_prediction`，预测范围 ±100 端口，成功率取决于 NAT 端口分配策略（递增式 NAT 预测成功率高，随机式几乎不可预测）。  
> ⁷ Symmetric-Symmetric 是最坏组合：双方端口都不可预测，UDP 打洞几乎必然失败，**必须走中继**。  
> ⁸ CGNAT-CGNAT：如果双方在同一运营商且运营商支持 hairpin，可能直连；否则需中继。

**关键洞察**：Symmetric NAT（企业/学校网络常见）与任何对端组合的打洞成功率都显著降低，Symmetric-Symmetric 几乎必须中继。这正是 TURN 存在的意义。

### 1.4 推荐穿透策略：三级降级

```
┌─────────────────────────────────────────────────────────────┐
│                    PDC 实例连接建立流程                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  阶段 0: 预检                                                │
│  ├─ STUN 检测自身 NAT 类型 + 获取公网映射地址                  │
│  ├─ 通过 pnos-runtime 或信令服务器获取对端地址信息             │
│  └─ 判断对端是否有公网可达地址（serve_host/serve_port）        │
│                          │                                  │
│         ┌────────────────┴────────────────┐                 │
│         ▼ 对端有公网/Full Cone             ▼ 对端在 NAT 后     │
│    【直连】                          阶段 1: UPnP            │
│    直接 TCP/UDP 连接                  ├─ 尝试 UPnP 映射同步端口 │
│                                      ├─ 成功 → 直连          │
│                                      └─ 失败 → 阶段 2        │
│                                               │             │
│                                               ▼             │
│                                    阶段 2: STUN + UDP 打洞   │
│                                    ├─ 交换公网映射地址        │
│                                    ├─ 同时发送打洞包          │
│                                    ├─ Symmetric 时端口预测    │
│                                    ├─ 成功 → 直连            │
│                                    └─ 失败（超时 5s）→ 阶段 3 │
│                                               │             │
│                                               ▼             │
│                                    阶段 3: TURN 中继（兜底）  │
│                                    ├─ 分配中继端口            │
│                                    ├─ 双方通过中继转发        │
│                                    └─ 带宽限流 + 计费统计     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**策略选择逻辑**：

1. **优先直连**：如果对端在 pnos-runtime 注册了 `serve_host`（公网 IP 或域名），直接连接，零穿透开销。
2. **UPnP 映射**：家用环境首选，成功后获得稳定公网端口，后续所有连接复用。PDC 已有完整实现。
3. **STUN 打洞**：UPnP 失败时（CGNAT/企业网），用现有 `HolePuncher` 执行 UDP 打洞。PDC 已实现按 NAT 类型选策略 + Symmetric 端口预测。
4. **TURN 中继兜底**：打洞失败时（主要是 Symmetric-Symmetric），走中继。**建议将现有 6881 自定义中继升级为 TURN 兼容协议**，或在其旁新增标准 TURN 端点。

### 1.5 中继带宽需求估算

**假设条件**：
- N 个 PDC 实例组成同步集群
- 每个实例每秒产生 X 条数据变更（node/peer/infohash/tracker 的增删改）
- 每条变更消息平均大小 S 字节（含签名、时间戳、CRDT 元数据，估算 128-256B）
- 中继流量放大系数：每条消息进入中继后转发给其他 N-1 个实例，即 **(N-1) 倍出向流量**
- 走中继的实例比例：根据成功率矩阵，约 10-15% 的实例对需要中继（主要是 Symmetric NAT 用户）

**单实例出站带宽**（直连场景）：
```
B_direct = X × S × (N-1)  bps
```

**中继场景带宽**（假设比例 r 的实例对走中继）：
```
B_relay_in  = X × S × N × r          (入向：所有中继实例上报)
B_relay_out = X × S × N × (N-1) × r  (出向：中继转发给所有其他实例)
B_relay_total = B_relay_in + B_relay_out
```

**数值示例**（N=10, X=100 条/秒, S=200B, r=15%）：

| 指标 | 计算 | 值 |
|---|---|---|
| 单实例直连出站 | 100 × 200 × 9 | 180 Kbps |
| 中继入向 | 100 × 200 × 10 × 0.15 | 30 Kbps |
| 中继出向 | 100 × 200 × 10 × 9 × 0.15 | 270 Kbps |
| 中继总带宽 | 30 + 270 | **300 Kbps** |

**大规模场景**（N=100, X=1000 条/秒, S=200B, r=15%）：

| 指标 | 值 |
|---|---|
| 单实例直连出站 | ~20 Mbps |
| 中继总带宽 | ~30 Mbps |

**结论**：
- 中小规模（N<50）：中继带宽需求在 Mbps 级，普通云服务器（5-10 Mbps 带宽）可承受。
- 大规模（N>100）：中继出向带宽随 O(N²) 增长，**必须优化**：
  - ** gossip 协议**：每个实例只向随机 k 个邻居转发（k=log₂N），而非全互联，将中继出向降为 O(N log N)。
  - **增量压缩**：变更消息批量打包 + snappy/zstd 压缩，可降 60-80% 体积。
  - **分层中继**：部署多个中继节点，按区域/网段分流，避免单点瓶颈。
- **当前 PDC 自定义中继的广播式转发**（`handle_udp_packet` 中转发给所有其他客户端）在 N>20 时会产生 O(N²) 流量，**必须改为定向转发或 gossip**。

### 1.6 端口复用分析

| 端口 | 当前用途 | 是否可复用于实例间同步 | 理由 |
|---|---|---|---|
| 6880 (TCP+UDP) | 超级 Tracker（BEP 15 UDP + HTTP TCP） | ❌ 不建议 | 10万 QPS、<100μs 响应路径，加密握手和同步流量会干扰性能 |
| 6882 (UDP) | DHT 爬虫 | ⚠️ 理论可复用 | DHT 本身就是 UDP KRPC，可扩展消息类型做实例通信，但协议混杂增加复杂度 |
| 6881 (UDP+TCP) | 自定义中继 | ✅ 推荐升级 | 已是中继/打洞用途，升级为 TURN 兼容后天然承载同步流量 |
| **6883 (新增)** | **实例间同步专用** | ✅ **推荐** | 物理隔离，独立加密，不影响 Tracker 和爬虫 |

**推荐方案**：新增 **6883 端口**作为实例间同步专用端口（TCP 为主，UDP 可选），与 6880 Tracker 服务通道完全隔离。6881 继续作为中继/打洞端口，升级为 TURN 兼容。

**关于复用 DHT UDP 连接**：
- DHT 爬虫使用 KRPC 协议（bencode 编码，`t`/`y`/`q`/`a` 字段），理论上可以新增 `q=pdc_sync` 消息类型。
- **但不推荐**：DHT 端口面向公网所有 DHT 节点，混入同步消息会：① 增加 DHT 节点处理复杂度；② 无法做实例间认证（DHT 是匿名协议）；③ 同步消息可能被 DHT 路由机制误转发。
- 正确做法是**独立同步通道**，DHT 仅用于发现其他 PDC 实例的存在（通过 infohash 公告机制），实际数据同步走 6883。

### 1.7 与 PDC 现有架构的结合点

| 现有模块 | 改造方向 |
|---|---|
| `nat/stun.rs` | 已有 STUN Binding，无需改动；可补充 `CHANGE-REQUEST` 支持以精确区分 Restricted/Port Restricted（当前双服务器方案只能区分 Full Cone vs Symmetric） |
| `nat/udp_hole_punch.rs` | 已有完整打洞逻辑，直接复用；`HolePuncher` 需暴露 `mapped_address()` 和 `nat_type()` 给同步模块 |
| `data_plane/relay.rs` | **升级重点**：当前广播式转发改为定向转发；增加 TURN `Allocate`/`CreatePermission`/`Send` 消息支持；增加认证（集成专项二的实例身份）；增加带宽限流 |
| `data_plane/hole_punch_signaling.rs` | 已有信令通道，可复用为 ICE 信令；需增加对端 NAT 类型和候选地址（candidate）的交换 |
| `pnos-sdk` 注册 | `ComponentRegisterRequest` 已有 `serve_host`/`serve_port` 字段，可用于公告公网可达地址；需增加 `nat_type` 和 `mapped_addr` 字段 |
| `nat/ipv6.rs` | IPv6 场景下 NAT 通常不存在，优先尝试 IPv6 直连，可绕过大部分 NAT 问题 |

### 1.8 实施建议（分阶段）

| 阶段 | 内容 | 预计工作量 | 依赖 |
|---|---|---|---|
| **P0** | 新增 6883 同步端口（TCP），通过 pnos-runtime 发现对端，有公网地址的实例直连 | 2-3 天 | pnos-sdk 服务发现 |
| **P1** | 集成现有 `HolePuncher`，NAT 后实例通过 STUN 打洞建立 6883 UDP 连接 | 3-5 天 | 专项二安全握手 |
| **P2** | 升级 6881 中继为定向转发 + 认证 + 限流，作为打洞失败兜底 | 3-5 天 | 专项二身份认证 |
| **P3** | 实现标准 TURN 协议（RFC 5766）兼容层，支持外部 TURN 客户端 | 1-2 周 | P2 完成 |
| **P4** | 实现 ICE 候选收集 + 连通性检查（Trickle ICE），自动化三级降级 | 1-2 周 | P1+P3 完成 |
| **P5** | 多中继节点部署 + gossip 协议优化大规模带宽 | 2-3 周 | P4 完成 + N>50 场景 |

---

## 专项二：安全与认证

### 2.1 安全威胁模型

多实例外网互联面临的威胁按优先级排序：

| 威胁 | 描述 | 影响 | 严重度 |
|---|---|---|---|
| **未授权接入** | 任意节点连接同步端口，读取/写入数据 | 数据泄露、污染 | 🔴 高 |
| **数据篡改** | 中间人修改同步消息内容 | 数据一致性破坏 | 🔴 高 |
| **伪造实例** | 攻击者冒充合法 PDC 实例注入数据 | 数据污染、Sybil | 🔴 高 |
| **重放攻击** | 截获合法消息后重放 | 状态回滚、计数错误 | 🟡 中 |
| **窃听** | 中间人读取同步流量 | 隐私泄露（peer 列表等） | 🟡 中 |
| **DoS** | 恶意实例大量发送同步消息耗尽资源 | 服务不可用 | 🟡 中 |
| **Sybil 攻击** | 攻击者创建大量恶意实例控制集群 | 数据多数被污染 | 🟠 中高 |
| **内部恶意** | 已授权实例故意发送虚假数据 | 数据质量下降 | 🟠 中高 |

### 2.2 实例身份认证方案对比

| 方案 | 身份载体 | 信任模型 | 密钥管理 | 实现复杂度 | 适用场景 |
|---|---|---|---|---|---|
| **共享密钥（PSK）** | 预共享密钥 | 对称信任 | 所有实例同一密钥 | 低 | 小规模私有集群（<10 实例） |
| **Ed25519 公钥身份** | 实例 ID = 公钥哈希 | TOFU / 白名单 | 每实例独立密钥对 | 中 | 推荐方案 |
| **X.509 证书 + PKI** | CA 签发证书 | 层级信任 | CA 管理 + 证书轮换 | 高 | 企业级、多组织 |
| **pnos-runtime 颁发 token** | runtime 签发的 JWT/token | 中心化信任 | runtime 管理 | 低 | 已有机制，但仅 HTTP 层 |
| **OAuth2 / OIDC** | 第三方身份提供商 | 联邦信任 | IDP 管理 | 高 | 不必要的复杂度 |

#### 推荐：Ed25519 公钥作为实例身份

**设计**：
1. 每个 PDC 实例首次启动时生成 Ed25519 密钥对，私钥存储在本地（`~/.pdc/identity.key`，权限 0600）。
2. 实例 ID = 公钥的 Base32 编码（或前 16 字节 hex），全局唯一且自验证。
3. 公钥随注册信息提交到 pnos-runtime，runtime 作为**公钥目录**（不签发证书，只存储和查询）。
4. 实例间连接时，通过 Noise 协议握手互相验证公钥指纹。

**信任模型选择**：

| 信任模型 | 描述 | 优点 | 缺点 | 推荐阶段 |
|---|---|---|---|---|
| **TOFU（Trust On First Use）** | 首次连接时记录对端公钥指纹，后续连接验证指纹一致 | 零配置，类似 SSH | 首次连接可能被中间人攻击 | MVP |
| **白名单** | 管理员手动批准实例公钥 | 安全性高 | 运维成本高 | 小规模生产 |
| **pnos-runtime 公证** | runtime 对实例公钥签名（类似 CA），实例间验证 runtime 签名 | 中心化信任，可审计 | runtime 成为信任根 | 完整方案 |
| **Web of Trust** | 实例间互相签名推荐 | 去中心化 | 复杂度高，用户体验差 | 不推荐 |

**推荐路径**：MVP 用 TOFU + 本地 `known_instances` 文件（类似 SSH `known_hosts`）；生产环境升级为 pnos-runtime 公证模式（runtime 用自己的私钥对已注册实例的公钥签名，实例间验证该签名）。

### 2.3 数据加密方案对比

| 方案 | 传输层 | 握手 RTT | 性能开销 | UDP 支持 | 实现复杂度 | Rust 生态 |
|---|---|---|---|---|---|---|
| **TLS 1.3** | TCP | 1-RTT | 中（AES-GCM 硬件加速） | ❌ | 中 | `rustls`（成熟） |
| **DTLS 1.3** | UDP | 1-RTT | 中高（重传开销） | ✅ | 高 | `webrtc-utils`/`openssl` |
| **Noise Protocol (XX)** | TCP/UDP | 1-RTT | **低**（轻量） | ✅ | 中 | `snow`（libp2p 使用） |
| **Noise Protocol (IK)** | TCP/UDP | **0-RTT**（已知对端公钥） | 低 | ✅ | 中 | `snow` |
| **QUIC + TLS 1.3** | UDP | 1-RTT | 中（内置拥塞控制） | ✅ | 高 | `quinn`（成熟） |
| **应用层加密（libsodium）** | 任意 | N/A | 低 | ✅ | 低 | `sodiumoxide`/`crypto_box` |

#### 推荐：Noise Protocol XX 模式（TCP 同步通道）

**理由**：
1. **1-RTT 握手**：比 TLS 1.3 更轻量，握手消息更小（~200B vs TLS ~1KB）。
2. **内置身份认证**：XX 模式在握手过程中交换并验证公钥，无需额外证书层。
3. **UDP/TCP 通用**：`snow` crate 同时支持流式（TCP）和数据报（UDP）。
4. **libp2p 验证**：libp2p 的 `noise` 协议已在大规模 P2P 网络中验证。
5. **0-RTT 升级**：已知对端公钥后可切换 IK 模式，实现 0-RTT 重连。

**性能数据**（参考值，需 PDC 实测确认）：

| 操作 | Ed25519 签名 | Ed25519 验证 | ChaCha20-Poly1305 加密 | AES-256-GCM 加密 |
|---|---|---|---|---|
| 单次耗时 | ~20-50 μs | ~50-100 μs | ~1-2 μs/KB | ~0.5-1 μs/KB（硬件 AES） |
| 每秒操作数 | ~20K-50K | ~10K-20K | ~500 MB/s | ~1-2 GB/s |

**对 10万 QPS Tracker 路径的影响**：
- 同步通道（6883）与 Tracker 通道（6880）物理隔离，加密开销**完全不影响** Tracker 响应延迟。
- 同步通道自身的加密开销：假设 1000 条/秒同步消息，每条 200B，加密总开销 < 1% CPU。
- 握手阶段（每连接一次）的 Ed25519 验证开销可忽略（长连接复用）。

### 2.4 防伪造与防恶意节点

#### 消息级签名

每条同步消息携带发送实例的 Ed25519 签名，结构如下：

```
┌──────────────────────────────────────────────────┐
│  消息信封 (Envelope)                              │
├──────────────────────────────────────────────────┤
│  version: u8 (1)                                 │
│  msg_type: u8 (SyncOp/Heartbeat/Ack)             │
│  instance_id: [u8; 32] (发送方公钥)               │
│  timestamp: u64 (毫秒级 Unix 时间)                │
│  nonce: [u8; 16] (随机数)                         │
│  payload_len: u32                                │
│  payload: [u8] (CRDT 操作 + 数据)                 │
│  signature: [u8; 64] (Ed25519 over header+payload)│
└──────────────────────────────────────────────────┘
```

**验证流程**：
1. 接收方用 `instance_id`（公钥）验证 `signature`。
2. 检查 `timestamp` 在 ±5 分钟窗口内（防重放）。
3. 检查 `nonce` 未在最近 10 分钟内出现过（防重放，用 Bloom filter 或 LRU cache）。
4. 验证通过后应用 payload 到本地 CRDT。

#### 防重放攻击

| 机制 | 描述 | 内存开销 | 安全性 |
|---|---|---|---|
| 时间戳窗口 | 拒绝 ±5 分钟外的消息 | 0 | 防长时间重放，不防窗口内重放 |
| Nonce + LRU | 记录最近 N 个 nonce，拒绝重复 | ~N × 16B | 防窗口内重放 |
| Nonce + Bloom Filter | 概率性去重，固定内存 | ~1MB（1% 误判率，100万 nonce） | 概率性防重放 |
| 序列号（per-instance） | 每实例维护单调递增序列号，拒绝回退 | N × 8B | 防重放最强，但需持久化 |

**推荐**：时间戳窗口 + per-instance 单调序列号（序列号包含在签名 payload 中，接收方维护 `last_seq[instance_id]`，拒绝 `seq <= last_seq` 的消息）。序列号无需持久化，重启后从对端同步最新值即可。

#### 恶意数据注入防护

仅靠身份认证无法防止**已授权实例**发送虚假数据。需多层防护：

| 层级 | 机制 | 描述 |
|---|---|---|
| **L1 身份层** | 实例认证 + 消息签名 | 确保消息来自已知实例，不可伪造 |
| **L2 信誉层** | 实例信誉评分 | 根据数据准确率（后续验证 peer 是否真实存在）动态调整信誉，低信誉实例数据降权或拒绝 |
| **L3 数据验证层** | 主动探测验证 | 对收到的 peer/node 数据，按比例主动探测验证（连接 peer、ping node），虚假数据回溯扣分 |
| **L4 冲突仲裁层** | CRDT + 多源合并 | 同一数据多实例上报时取并集/加权平均，单实例虚假数据被多数稀释 |
| **L5 隔离层** | 实例封禁 | 信誉低于阈值的实例自动断开连接并加入黑名单 |

**信誉评分算法**（简化版）：
```
instance_reputation = initial_score (50)
                    + Σ(verified_correct) × weight_positive
                    - Σ(verified_false) × weight_negative
                    - Σ(message_drop) × weight_drop
范围: 0-100，低于 30 触发降权，低于 10 触发封禁
```

#### Sybil 攻击防护

Sybil 攻击（攻击者创建大量恶意实例）在无许可 P2P 网络中是根本性难题。PDC 作为**半许可集群**（实例需注册到 pnos-runtime），有以下缓解手段：

| 手段 | 描述 | 有效性 | 成本 |
|---|---|---|---|
| **注册审批** | pnos-runtime 管理员手动批准新实例 | 高（完全阻止未授权 Sybil） | 运维成本 |
| **邀请制** | 新实例需已有实例签名邀请 | 中高 | 社交成本 |
| **工作量证明（PoW）** | 注册时需计算 Hashcash 难题 | 中（提高攻击成本） | 注册延迟 |
| **存储押金** | 注册需质押 token/存储空间 | 中高 | 经济成本 |
| **IP 限速** | 同一 /24 网段最多 N 个实例 | 低（IPv6 可绕过） | 低 |
| **信誉加权** | 新实例初始信誉低，数据降权 | 中（稀释而非阻止） | 低 |

**推荐**：MVP 阶段用**注册审批 + IP 限速**（同一 /24 最多 3 个实例）；完整方案增加**邀请制**（每个实例最多邀请 5 个新实例，邀请者对被邀请者负连带责任，被邀请者作恶扣邀请者信誉）。

### 2.5 访问控制与权限分级

| 权限级别 | 能力 | 典型场景 |
|---|---|---|
| **full** | 全量同步（收发所有数据变更） | 可信集群节点 |
| **receive_only** | 只接收同步数据，不上报 | 只读镜像节点、监控节点 |
| **report_only** | 只上报本地发现，不接收他人数据 | 边缘轻量节点、数据贡献者 |
| **limited** | 仅同步指定 infohash 范围/指定 Repo | 租户隔离、多租户场景 |

**实现方式**：在 pnos-runtime 的 `ComponentInfo` 中增加 `sync_permission` 字段，实例连接时由对端查询 runtime 确认权限。本地维护权限缓存（TTL 5 分钟）。

### 2.6 推荐安全架构

```
┌─────────────────────────────────────────────────────────────────┐
│                    PDC 实例安全架构                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │  身份层       │    │  传输层       │    │  消息层       │      │
│  │              │    │              │    │              │      │
│  │ Ed25519 密钥 │    │ Noise XX     │    │ 消息签名      │      │
│  │ 对生成       │    │ 1-RTT 握手   │    │ Ed25519      │      │
│  │              │    │              │    │              │      │
│  │ 实例ID=公钥  │    │ ChaCha20-    │    │ 时间戳+序列号 │      │
│  │ 全局唯一     │    │ Poly1305 加密│    │ 防重放        │      │
│  │              │    │              │    │              │      │
│  │ TOFU/白名单  │    │ 0-RTT 重连   │    │ 权限校验      │      │
│  │ 信任锚       │    │ (IK 模式)    │    │ (runtime 查询)│      │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘      │
│         │                   │                   │              │
│         └───────────────────┼───────────────────┘              │
│                             ▼                                  │
│                  ┌─────────────────────┐                       │
│                  │  数据层              │                       │
│                  │                     │                       │
│                  │ 实例信誉评分         │                       │
│                  │ 主动探测验证         │                       │
│                  │ CRDT 多源合并        │                       │
│                  │ 低信誉降权/封禁      │                       │
│                  └─────────────────────┘                       │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  pnos-runtime（信任根）                                  │   │
│  │  - 实例公钥目录（存储已注册实例的公钥）                    │   │
│  │  - 公钥签名（公证模式：runtime 私钥签名实例公钥）          │   │
│  │  - 权限管理（sync_permission 字段）                      │   │
│  │  - 注册审批/邀请制                                       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.7 性能影响量化分析

| 安全机制 | 计算开销 | 对同步通道影响 | 对 Tracker 通道(6880)影响 |
|---|---|---|---|
| Noise XX 握手 | ~200 μs/连接 | 每连接一次，长连接可忽略 | **零**（物理隔离） |
| ChaCha20-Poly1305 | ~1-2 μs/KB | 1000 msg/s × 200B = 0.2-0.4% CPU | **零** |
| Ed25519 消息签名 | ~20-50 μs/条 | 1000 msg/s = 2-5% CPU | **零** |
| Ed25519 消息验证 | ~50-100 μs/条 | 1000 msg/s × (N-1) = 接收侧 5-10% CPU | **零** |
| 时间戳+序列号检查 | <1 μs/条 | 可忽略 | **零** |
| 信誉评分计算 | 异步批量 | 可忽略 | **零** |

**结论**：安全机制对同步通道的 CPU 开销在 5-15% 范围内（主要是签名验证），对 10万 QPS 的 Tracker 服务通道**零影响**（因为物理隔离在 6883 端口）。如果签名验证成为瓶颈，可批量签名（每 10 条消息签一次 batch signature）将验证开销降为 1/10。

### 2.8 实施建议（分阶段）

| 阶段 | 内容 | 预计工作量 |
|---|---|---|
| **MVP（最小可行安全）** | ① Ed25519 密钥对生成与持久化 ② Noise XX 握手（snow crate）③ 消息签名+验证 ④ 时间戳+序列号防重放 ⑤ TOFU 信任（known_instances 文件） | 1-2 周 |
| **V1（生产安全）** | ① pnos-runtime 公钥目录 + 公证签名 ② 实例信誉评分系统 ③ 主动探测验证 ④ 权限分级（full/receive_only/report_only）⑤ 注册审批 + IP 限速 | 2-3 周 |
| **V2（完整安全）** | ① 邀请制 + 连带责任 ② 批量签名优化 ③ 0-RTT 重连（Noise IK）④ 多因素信誉（数据质量+在线率+带宽贡献）⑤ 审计日志（所有同步操作可追溯） | 3-4 周 |

**MVP 依赖的 Rust crate**：
- `snow` — Noise Protocol 实现（libp2p 同款）
- `ed25519-dalek` — Ed25519 签名（或 `ring`）
- `rand` — 已在依赖中，用于 nonce 生成
- `serde`/`bincode` — 消息序列化（bincode 比 JSON 紧凑 3-5 倍）

---

## 专项三：一致性模型与冲突解决

### 3.1 四大 Repo 一致性需求分级

| Repo | 数据特征 | 写入频率 | 冲突概率 | 不一致后果 | 一致性需求 |
|---|---|---|---|---|---|
| **PeerRepo** | infohash→peer 集合，时效性强（30min TTL） | 极高（announce 10万 QPS） | 高（多实例同时发现同一 peer） |  peer 列表多一条少一条不影响下载，过期自动清理 | **最终一致**，时效优先 |
| **NodeRepo** | DHT 节点池，评分异步重算，状态机 | 中（爬虫 1万/小时新增） | 中（多实例同时发现同一节点） | 节点去重即可，评分各算各的 | **最终一致**，去重优先 |
| **InfohashRepo** | 引用计数（ref_count），register/unregister | 中高（随 peer 增减） | **高**（多实例同时 register/unregister 同一 infohash） | 计数不准可能导致 infohash 被错误清理 | **计数精确**，CRDT 可解 |
| **TrackerRepo** | Tracker 池，低频变更，有禁用状态 | 极低（人工/自动添加） | 低 | 数据量小，短暂不一致无影响 | **最终一致**，甚至强一致也可 |

**核心判断**：PDC 没有任何数据需要强一致性（Raft/Paxos）。所有数据都可以用最终一致 + CRDT 解决，InfohashRepo 的引用计数用 PN-Counter 即可精确合并。

### 3.2 一致性模型对比

| 模型 | 延迟 | 吞吐量 | 冲突处理 | 实现复杂度 | PDC 适用性 |
|---|---|---|---|---|---|
| **强一致（Raft）** | 高（多数派确认，~1-10ms RTT） | 低（串行写入） | 无冲突（串行化） | 高（leader 选举、日志复制） | ❌ 不适用：10万 QPS 下 Raft 吞吐瓶颈，且跨公网 RTT 高 |
| **强一致（Paxos）** | 高 | 低 | 无冲突 | 极高 | ❌ 同上 |
| **最终一致（gossip）** | 低（本地立即写入，异步传播） | 高（并行写入） | 需冲突解决策略 | 中 | ✅ 适用：所有 Repo |
| **因果一致** | 低 | 高 | 保留因果顺序 | 中高 | ⚠️ 部分需要：register infohash → add peer 有因果，但 peer TTL 天然缓解 |
| **读己之写** | 低 | 高 | 本地读本地写 | 低 | ✅ 天然满足：本地写入立即可读，同步是增量传播 |

**为什么不需要强一致**：
1. **PeerRepo**：BT 协议本身就是最终一致的——peer 列表来自 DHT/Tracker/PEX 多个来源，本就不保证精确。多一条过期 peer 不影响下载（连接失败会跳过），少一条 peer 只是少一个候选。
2. **NodeRepo**：DHT 节点池是候选池，多一个坏节点只是浪费一次探测，少一个好节点只是少一个候选。评分各实例独立计算，不需要统一。
3. **InfohashRepo**：引用计数的目的是防止仍有 peer 的 infohash 被清理。PN-Counter 可以精确合并多实例的增减操作，不需要强一致。
4. **TrackerRepo**：数据量小（<1000 条），变更频率极低，最终一致足够。

### 3.3 CRDT 在 PDC 的适用性详细分析

#### 3.3.1 PN-Counter → InfohashRepo 引用计数

**PN-Counter（Positive-Negative Counter）** 是最适合引用计数的 CRDT：
- 每个实例维护一个 `(p, n)` 对：p = 本实例 register 次数，n = 本实例 unregister 次数。
- 全局值 = Σ(all instances p) - Σ(all instances n)。
- **合并操作 = 逐元素取 max**：`merge(p1,n1, p2,n2) = (max(p1,p2), max(n1,n2))`。
- 天然满足交换律、结合律、幂等性，**无冲突**。

**PDC 适配**：

当前 `InfohashRepoImpl` 的缓存结构：
```rust
entries: FxHashMap<Infohash, (ref_count: u32, source: String, score: f64)>
```

改造为 CRDT 后：
```rust
entries: FxHashMap<Infohash, PnCounterEntry>

struct PnCounterEntry {
    // per-instance counters: instance_id -> (positive, negative)
    counters: FxHashMap<InstanceId, (u32, u32)>,
    source: LwwRegister<String>,   // 首次发现来源，LWW
    score: LwwRegister<f64>,       // 热门度评分，LWW
}

impl PnCounterEntry {
    fn value(&self) -> u32 {
        self.counters.values()
            .map(|(p, n)| p.saturating_sub(*n))
            .sum()
    }
    
    fn register(&mut self, instance: InstanceId) {
        self.counters.entry(instance).or_default().0 += 1;
    }
    
    fn unregister(&mut self, instance: InstanceId) {
        self.counters.entry(instance).or_default().1 += 1;
    }
    
    fn merge(&mut self, other: &Self) {
        for (id, (p, n)) in &other.counters {
            let local = self.counters.entry(*id).or_default();
            local.0 = local.0.max(*p);
            local.1 = local.1.max(*n);
        }
    }
}
```

**优点**：
- 多实例同时 register/unregister 同一 infohash，计数精确，不会丢失增减。
- 实例崩溃重启后，从其他实例合并 counters 即可恢复精确计数。
- 内存开销：每个 infohash 多存 N 个 (u32,u32) 对，N=10 时每 infohash 多 80B，千万级 infohash 多 800MB（可接受，或用 gossip 只同步增量而非全量 counters）。

**优化**：实际同步时不需要传输完整的 per-instance counters，只传输增量操作（`{instance_id, infohash, +1}` 或 `{instance_id, infohash, -1}`），接收方应用到本地 PN-Counter。这将同步消息降为每条 ~40B。

#### 3.3.2 OR-Set → PeerRepo 集合

**OR-Set（Observed-Remove Set）** 是处理并发 add/remove 的最优集合 CRDT：
- 每个元素附带唯一 tag（UUID 或 (instance_id, seq)）。
- add 操作添加新 tag；remove 操作删除**已观察到的**所有 tag。
- 合并时：元素存在当且仅当存在至少一个未被删除的 tag。
- 并发 add+remove：add 胜出（因为 remove 只能删除已观察到的 tag，并发 add 的新 tag 不在 remove 的观察集中）。

**PDC 适配**：

当前 `PeerRepoImpl` 结构：
```rust
by_infohash: HashMap<Infohash, HashSet<SocketAddr>>
global: HashMap<SocketAddr, PeerInfo>
```

改造为 CRDT 后，每个 (infohash, peer_addr) 关联一个 OR-Set：

```rust
// 每个 peer 在每个 infohash 下的 OR-Set 条目
struct OrSetEntry {
    // tag -> (added_at HLC, instance_id)
    tags: FxHashMap<Tag, (Hlc, InstanceId)>,
    // 已删除的 tag 集合（tombstone）
    removed: FxHashSet<Tag>,
}

impl OrSetEntry {
    fn exists(&self) -> bool {
        self.tags.keys().any(|t| !self.removed.contains(t))
    }
    
    fn add(&mut self, tag: Tag, hlc: Hlc, instance: InstanceId) {
        self.tags.insert(tag, (hlc, instance));
    }
    
    fn remove(&mut self) {
        // 删除所有当前观察到的 tag
        self.removed.extend(self.tags.keys().copied());
    }
    
    fn merge(&mut self, other: &Self) {
        // tag 取并集
        for (tag, meta) in &other.tags {
            self.tags.entry(*tag).or_insert(*meta);
        }
        // removed 取并集
        self.removed.extend(other.removed.iter().copied());
    }
}
```

**PeerInfo 属性（last_active、source、priority_score 等）用 LWW-Register**：
```rust
struct PeerInfoCrdt {
    last_active: LwwRegister<SystemTime>,
    source: LwwRegister<PeerSource>,
    priority_score: LwwRegister<f64>,  // 或取 max
    connection_attempts: GCounter,     // 只增计数
    connection_successes: GCounter,    // 只增计数
}
```

**Tombstone 清理**：OR-Set 的 removed 集合会无限增长。PDC 的 peer 有 30 分钟 TTL，可定期清理：
- 当 `tags` 中所有 tag 都被 removed，且最早 tag 时间超过 TTL，删除整个 entry。
- 或者用 **2P-Set（Two-Phase Set）** 简化：元素只能 add 一次，remove 后不可再 add。但 peer 可能被 remove 后重新发现（add），所以 OR-Set 更合适。

**替代方案：基于 TTL 的简化 OR-Set**
由于 peer 数据有 30 分钟 TTL，可以用更简单的策略：
- 不做显式 remove，只做 add（携带 last_active 时间戳）。
- 过期 peer 自动从集合中剔除（last_active > 30min）。
- 这实际上是 **LWW-Element-Set** 的变体：每个元素带时间戳，合并取 max(last_active)，过期自动删除。
- 优点：无需 tombstone，内存可控；缺点：无法显式删除（但 TTL 天然解决）。

**推荐**：PeerRepo 使用 **LWW-Element-Set（基于 last_active）**，而非完整 OR-Set。理由：
1. peer 数据时效性强，30 分钟 TTL 天然解决删除问题。
2. 不需要显式 remove 操作（peer 不活跃自然过期）。
3. 实现简单，内存可控，无 tombstone 膨胀。
4. 并发 add 同一 peer：取 max(last_active)，天然合并。

#### 3.3.3 LWW-Register → NodeRepo / TrackerRepo 属性

**LWW-Register（Last-Write-Wins Register）** 是最简单的 CRDT：
- 每个值附带时间戳，合并时取时间戳较大的值。
- 适用于单值属性的更新。

**NodeRepo 适配**：
```rust
struct NodeEntryCrdt {
    id: LwwRegister<NodeId>,
    last_active: LwwRegister<Instant>,
    state: LwwRegister<NodeState>,       // Good/Questionable/Bad
    consecutive_failures: LwwRegister<u32>,
    score: LwwRegister<f64>,             // 评分各实例独立算，同步取 max 或 LWW
    query_count: GCounter,               // 只增计数器
}
```

**注意**：节点评分（score）各实例独立计算（基于本地观察的响应速度/稳定性等），同步时取 **max** 而非 LWW，因为评分是评估值而非事实值。或者不同步评分，各实例独立维护评分（评分维度包含本地观察，无法统一）。

**TrackerRepo 适配**：
```rust
struct TrackerEntryCrdt {
    score: LwwRegister<f64>,
    disabled: LwwRegister<bool>,
    total_requests: GCounter,
    success_requests: GCounter,
    failed_requests: GCounter,
    consecutive_failures: LwwRegister<u32>,
    avg_response_time_ms: LwwRegister<f64>,  // 或加权平均
    last_used: LwwRegister<Option<Instant>>,
}
```

TrackerRepo 数据量小（<1000 条），甚至可以用**全量状态同步 + LWW 合并**，不需要增量操作日志。

#### 3.3.4 CRDT 类型选择总结

| 数据字段 | CRDT 类型 | 合并操作 | 理由 |
|---|---|---|---|
| Infohash ref_count | **PN-Counter** | per-instance (p,n) 取 max | 精确计数，并发增减无冲突 |
| Infohash source | LWW-Register | 取 max(HLC) | 单值属性 |
| Infohash score | LWW-Register / max | 取 max(HLC) 或 max(value) | 热门度评分 |
| Peer (infohash→addr) | **LWW-Element-Set** | 取 max(last_active)，TTL 过期 | 时效性强，无需显式删除 |
| Peer.last_active | LWW-Register | 取 max(timestamp) | 时间戳天然有序 |
| Peer.source | LWW-Register | 取 max(HLC) | 单值属性 |
| Peer.priority_score | LWW-Register / max | 取 max(value) | 评分取高 |
| Peer.connection_attempts | G-Counter | per-instance 取 max 后求和 | 只增计数 |
| Node.state | LWW-Register | 取 max(HLC) | 状态机属性 |
| Node.last_active | LWW-Register | 取 max(timestamp) | 时间戳 |
| Node.score | **不同步**（各实例独立计算） | N/A | 评分依赖本地观察 |
| Node.query_count | G-Counter | per-instance 取 max 后求和 | 只增计数 |
| Tracker.disabled | LWW-Register | 取 max(HLC) | 布尔状态 |
| Tracker.total_requests | G-Counter | per-instance 取 max 后求和 | 只增计数 |
| Tracker.avg_response_time | LWW-Register / 加权平均 | 取 max(HLC) 或加权 | 统计值 |

### 3.4 冲突解决具体算法

#### 3.4.1 同步消息处理流程

```
function handle_sync_message(envelope):
    # 1. 安全验证（专项二）
    verify_signature(envelope)        # Ed25519 签名验证
    check_timestamp(envelope)         # ±5 分钟窗口
    check_sequence(envelope)          # 单调递增序列号
    check_permission(envelope)        # 实例权限校验
    
    # 2. 分发到对应 Repo 的 CRDT 合并器
    match envelope.msg_type:
        SyncOp::InfohashRegister   -> infohash_repo.apply_register(op)
        SyncOp::InfohashUnregister -> infohash_repo.apply_unregister(op)
        SyncOp::PeerAdd            -> peer_repo.apply_peer_add(op)
        SyncOp::NodeUpdate         -> node_repo.apply_node_update(op)
        SyncOp::TrackerUpdate      -> tracker_repo.apply_tracker_update(op)
        SyncOp::FullSnapshot       -> apply_full_snapshot(op)
    
    # 3. 传播（gossip）
    if envelope.ttl > 0:
        forward_to_random_neighbors(envelope.with_ttl(-1))

function apply_register(op):
    entry = infohash_cache.get_or_default(op.infohash)
    entry.pn_counter.register(op.instance_id)    # PN-Counter +1
    entry.source.merge(op.source)                # LWW
    if entry.pn_counter.value() == 1:
        mark_new_infohash(op.infohash)           # 触发持久化

function apply_peer_add(op):
    # LWW-Element-Set: 取 max(last_active)
    existing = peer_cache.global.get(op.addr)
    if existing is None or op.last_active > existing.last_active:
        peer_cache.global.insert(op.addr, op.peer_info)
    peer_cache.by_infohash[op.infohash].insert(op.addr)
    peer_cache.infohash_refs[op.addr].insert(op.infohash)
    # 触发 infohash register（引用计数 +1）
    infohash_repo.register_sync(op.infohash, op.instance_id)
```

#### 3.4.2 PN-Counter 合并伪代码

```
# 每个实例维护本地的 (positive, negative)
# 同步时只发送增量 delta，不发送全量

struct PnCounterDelta:
    instance_id: [u8; 32]
    infohash: [u8; 20]
    positive_delta: u32    # 本次新增的 register 次数
    negative_delta: u32    # 本次新增的 unregister 次数
    hlc: Hlc               # 混合逻辑时钟

function apply_delta(local_counters, delta):
    local = local_counters[delta.instance_id]
    local.positive += delta.positive_delta
    local.negative += delta.negative_delta
    # 注意：PN-Counter 的合并是取 max，但增量同步是加法
    # 因为每个 instance 只递增自己的 counter，不会回退
    # 如果收到乱序/重复 delta，用 HLC 去重

function merge_full(local, remote):
    # 全量合并时取 max（用于实例重启后全量同步）
    for instance_id in union(local.keys, remote.keys):
        local[instance_id].positive = max(
            local[instance_id].positive,
            remote[instance_id].positive
        )
        local[instance_id].negative = max(
            local[instance_id].negative,
            remote[instance_id].negative
        )
```

#### 3.4.3 LWW-Element-Set（Peer）合并伪代码

```
struct PeerAddOp:
    instance_id: [u8; 32]
    infohash: [u8; 20]
    peer_addr: SocketAddr
    peer_info: PeerInfo
    hlc: Hlc

function apply_peer_add(local_store, op):
    # 1. 更新全局 peer 信息（LWW: 取 max(last_active)）
    existing = local_store.global.get(op.peer_addr)
    if existing is None:
        local_store.global.insert(op.peer_addr, op.peer_info)
    else if op.peer_info.last_active > existing.last_active:
        # 合并：新数据更新 last_active/source，保留历史统计
        existing.last_active = op.peer_info.last_active
        existing.source = op.peer_info.source
        if op.peer_info.peer_id is not None:
            existing.peer_id = op.peer_info.peer_id
        # priority_score 取 max
        existing.priority_score = max(existing.priority_score, op.peer_info.priority_score)
    
    # 2. 添加到 infohash 集合
    local_store.by_infohash[op.infohash].insert(op.peer_addr)
    local_store.infohash_refs[op.peer_addr].insert(op.infohash)
    
    # 3. 触发 infohash 引用计数 +1
    infohash_repo.register(op.infohash, op.instance_id)

function expire_peers(local_store, now, ttl=30min):
    # 定期清理过期 peer
    expired = local_store.global.values()
        .filter(|p| now - p.last_active > ttl)
        .map(|p| p.addr)
        .collect()
    
    for addr in expired:
        infohashes = local_store.infohash_refs.remove(addr)
        for ih in infohashes:
            local_store.by_infohash[ih].remove(addr)
            if local_store.by_infohash[ih].is_empty():
                local_store.by_infohash.remove(ih)
            # 触发 infohash 引用计数 -1
            infohash_repo.unregister(ih, local_instance_id)
        local_store.global.remove(addr)
```

### 3.5 时钟同步问题

#### 3.5.1 NTP 偏移对 LWW 的影响

LWW（Last-Write-Wins）依赖时间戳决定胜负。如果两个实例的系统时钟有偏移：

| 偏移量 | 影响 | 严重度 |
|---|---|---|
| < 100ms | 几乎无影响（数据变更间隔通常 > 1s） | 低 |
| 100ms - 1s | 可能导致旧值覆盖新值（慢时钟实例的"新"写入被快时钟实例的"旧"写入覆盖） | 中 |
| > 1s | 频繁错误覆盖，一致性受损 | 高 |
| > 10s | LWW 基本失效 | 严重 |

**家用设备/云服务器的 NTP 同步质量**：
- 云服务器：NTP 偏移通常 < 10ms。
- 家用 Windows：Windows Time 服务默认同步间隔 7 天，偏移可能达数秒。**PDC 应在启动时强制 NTP 同步，并每小时校准**。
- 容器环境：依赖宿主机时钟，通常较好。

#### 3.5.2 混合逻辑时钟（HLC）

**HLC（Hybrid Logical Clock）** 结合了物理时钟和逻辑时钟的优点：
- 每个事件的 HLC = `(physical, counter)`，physical 取 max(本地物理时钟, 收到的最大 HLC.physical)。
- 如果物理时钟回退或等于收到的 HLC，counter 递增。
- HLC 保证因果有序（happens-before → HLC 递增），且物理时钟分量接近真实时间。

**PDC 中的 HLC 应用**：

```rust
struct Hlc {
    physical: u64,  // 毫秒级物理时钟
    counter: u32,   // 逻辑计数器
}

impl Hlc {
    // 本地事件：递增
    fn increment(&mut self, now_ms: u64) {
        if now_ms > self.physical {
            self.physical = now_ms;
            self.counter = 0;
        } else {
            self.counter += 1;
        }
    }
    
    // 收到远程消息：合并
    fn merge(&mut self, remote: &Hlc, now_ms: u64) {
        if remote.physical > self.physical {
            self.physical = remote.physical;
            self.counter = remote.counter + 1;
        } else if remote.physical == self.physical {
            self.counter = max(self.counter, remote.counter) + 1;
        } else {
            // remote.physical < self.physical，用本地时钟
            self.increment(now_ms);
        }
    }
    
    // 比较：先比 physical，再比 counter
    fn cmp(&self, other: &Hlc) -> Ordering {
        self.physical.cmp(&other.physical)
            .then(self.counter.cmp(&other.counter))
    }
}
```

**HLC 对 LWW 的改进**：
- LWW 比较从 `物理时间戳` 改为 `HLC`，保证因果有序的事件不会被错误覆盖。
- 即使物理时钟有偏移，HLC 的逻辑计数器分量保证因果顺序。
- HLC 的 physical 分量仍接近真实时间，可用于 TTL 过期判断。

**推荐**：所有 CRDT 的 LWW 时间戳使用 HLC 而非裸物理时间戳。HLC 状态（physical, counter）每实例维护一个，持久化到 SQLite，重启后恢复。

#### 3.5.3 向量时钟 vs HLC

| 特性 | 向量时钟（Vector Clock） | HLC |
|---|---|---|
| 大小 | O(N)（N=实例数） | O(1)（固定 12 字节） |
| 因果检测 | 精确检测并发冲突 | 保证因果有序，但不标记并发 |
| 冲突解决 | 需应用层处理并发 | LWW 直接用 HLC 比较 |
| PDC 适用性 | ❌ N=100 时每条消息多 800B | ✅ 固定开销，LWW 友好 |

**结论**：PDC 用 HLC，不用向量时钟。向量时钟的精确并发检测能力在 PDC 场景下不需要（CRDT 已保证无冲突合并），而 O(N) 的空间开销在大规模下不可接受。

### 3.6 TTL 过期机制如何简化冲突解决

PDC 数据的时效性特征大幅简化了冲突解决：

| 数据 | TTL | 过期效果 | 对冲突解决的简化 |
|---|---|---|---|
| Peer | 30 分钟（建议） | 不活跃 peer 自动删除 | 无需显式 remove 操作，LWW-Element-Set 无需 tombstone |
| Node | 24 小时（Bad 状态更快） | 不活跃节点降级/删除 | 节点状态冲突可被时间消解，坏节点自然淘汰 |
| Infohash | ref_count=0 时延迟删除 | 无 peer 的 infohash 清理 | PN-Counter 精确计数，TTL 作为兜底 |
| Tracker | 无 TTL（人工管理） | 不过期 | 数据量小，LWW 足够 |

**核心洞察**：PeerRepo 的 30 分钟 TTL 是最关键的设计——它意味着**任何 peer 数据的冲突最多存在 30 分钟**，之后过期自动消解。这使得不需要复杂的 OR-Set tombstone 管理，用简单的 LWW-Element-Set（基于 last_active）即可。

### 3.7 四大 Repo 一致性策略总表

| Repo | 一致性模型 | CRDT 类型 | 冲突解决 | 同步频率 | 同步方式 |
|---|---|---|---|---|---|
| **PeerRepo** | 最终一致 | LWW-Element-Set（基于 last_active） | 取 max(last_active)，TTL 30min 过期 | 实时（announce 触发） | 增量操作（PeerAdd），批量打包 |
| **NodeRepo** | 最终一致 | LWW-Register（属性）+ G-Counter（统计） | 属性取 max(HLC)，统计求和，评分不同步 | 批量（10s 间隔） | 增量操作（NodeUpdate），dirty 节点批量 |
| **InfohashRepo** | 最终一致（计数精确） | PN-Counter（ref_count）+ LWW-Register（source/score） | PN-Counter 无冲突合并，属性 LWW | 实时（随 peer 增减） | 增量 delta（+1/-1） |
| **TrackerRepo** | 最终一致 | LWW-Register + G-Counter | 全量状态 LWW 合并 | 低频（变更触发 + 5min 心跳） | 全量快照（数据量小） |

### 3.8 与 PDC 现有架构的结合点

| 现有模块 | 改造方向 |
|---|---|
| `storage/infohash_repo.rs` | `entries: FxHashMap<Infohash, (u32, String, f64)>` → `FxHashMap<Infohash, PnCounterEntry>`，新增 `apply_register_delta`/`apply_unregister_delta` 方法 |
| `storage/peer_repo.rs` | `add_peers_sync` 增加 `instance_id` 参数，`last_active` 比较逻辑已存在（`if let Some(existing)` 分支），天然适配 LWW；新增 `expire_peers` 定期任务 |
| `storage/node_repo.rs` | `add_node_sync` 增加 HLC 时间戳，`dirty` 机制可复用为增量同步缓冲区；评分（`score`）不同步，各实例独立维护 |
| `storage/tracker_repo.rs` | 新增 `merge_snapshot` 方法，全量 LWW 合并；数据量小，无需增量 |
| `storage/write_queue.rs` | 已有 SQLite 写入队列，可复用于同步消息的持久化（先写 WAL 再转发，保证崩溃恢复） |
| `storage/sharded_map.rs` | 已有锁分片 HashMap，CRDT 操作可按 infohash/addr 分片并行 |
| `event_bus.rs` | 已有事件总线，可作为本地变更→同步模块的通知通道（Repo 变更时发事件，同步模块订阅并打包发送） |
| `intelligence/score_maintainer.rs` | 评分维护器独立运行，不参与同步；同步模块只同步原始数据，评分各算各的 |

### 3.9 实施建议（分阶段）

| 阶段 | 内容 | 预计工作量 |
|---|---|---|
| **P0** | ① HLC 实现与持久化 ② InfohashRepo PN-Counter 改造 ③ PeerRepo LWW-Element-Set 确认（现有逻辑已接近） | 3-5 天 |
| **P1** | ① 同步消息格式定义（bincode 序列化 + 信封）② 本地变更捕获（event_bus 订阅）③ 增量打包发送 ④ 接收端 CRDT 合并 | 1-2 周 |
| **P2** | ① NodeRepo LWW-Register 改造 ② TrackerRepo 全量快照同步 ③ 过期清理任务（peer 30min / node 24h）④ 反熵（anti-entropy）定期全量对账 | 1-2 周 |
| **P3** | ① gossip 协议（随机 k 邻居转发）②  Merkle 树/哈希对账（快速检测不一致）③ 增量压缩（snappy/zstd）④ 同步限流（避免影响 Tracker） | 2-3 周 |

---

## 总体实施路线图

### 三专项依赖关系

```
专项二（安全认证）
    │
    ├──→ 专项一 P1（打洞建立连接后需安全握手）
    ├──→ 专项一 P2（中继需认证）
    └──→ 专项三 P1（同步消息需签名验证）

专项一 P0（直连）
    │
    └──→ 专项三 P1（有连接才能同步数据）

专项三 P0（CRDT 改造）
    │
    └──→ 专项三 P1（CRDT 合并器就绪后才能处理同步消息）
```

### 推荐实施顺序

| 里程碑 | 内容 | 涉及专项 | 预计周期 |
|---|---|---|---|
| **M1: 基础安全** | Ed25519 身份 + Noise 握手 + 消息签名 + TOFU | 专项二 MVP | 1-2 周 |
| **M2: CRDT 数据层** | HLC + PN-Counter + LWW-Element-Set 改造 | 专项三 P0 | 3-5 天 |
| **M3: 直连同步** | 6883 端口 + pnos-runtime 发现 + 有公网实例直连同步 | 专项一 P0 + 专项三 P1 | 1-2 周 |
| **M4: NAT 打洞同步** | 集成 HolePuncher，NAT 后实例打洞建立同步连接 | 专项一 P1 | 3-5 天 |
| **M5: 中继兜底** | 6881 中继升级（定向转发+认证+限流），打洞失败走中继 | 专项一 P2 + 专项二 V1 | 1-2 周 |
| **M6: 生产安全** | runtime 公证 + 信誉系统 + 权限分级 + 反熵对账 | 专项二 V1 + 专项三 P2 | 2-3 周 |
| **M7: 大规模优化** | gossip + 压缩 + 多中继 + ICE 标准化 | 专项一 P3-P5 + 专项三 P3 | 3-4 周 |

### 关键风险与待确认项

| 风险 | 描述 | 缓解措施 | 状态 |
|---|---|---|---|
| **pnos-runtime 公网可达性** | runtime 作为信令/公钥目录，需所有实例可访问。如果 runtime 也在 NAT 后，需先解决 runtime 自身的外网可达 | runtime 部署在有公网 IP 的云服务器；或 runtime 自身启用 UPnP | 待确认 |
| **家用设备 NTP 偏移** | Windows 默认 NTP 同步间隔长，时钟偏移可能 > 1s | PDC 启动时强制 `w32tm /resync`，HLC 兜底 | 需实现 |
| **Symmetric NAT 比例** | 企业/学校网络 Symmetric NAT 比例未知，影响中继带宽规划 | 上线后收集 `nat_type` 统计数据，按实际比例调整中继容量 | 待数据 |
| **CRDT 内存增长** | PN-Counter per-instance counters 在 N 大时内存增长 | 增量同步只传 delta，不全量传 counters；定期压缩（合并已离线实例的 counter） | 需监控 |
| **pnos-sdk 未接入** | PDC 当前 Cargo.toml 无 pnos-sdk 依赖，注册机制未接入 | 按 AGENTS.md 约束接入 pnos + pnos-sdk，这是多实例互联的前提 | 待实施 |
| **6881 中继广播问题** | 当前自定义中继广播式转发，N>20 时 O(N²) 流量 | M5 升级为定向转发 | 已知 |

---

## 附录：关键术语表

| 术语 | 全称 | 说明 |
|---|---|---|
| NAT | Network Address Translation | 网络地址转换，将私网地址映射为公网地址 |
| CGNAT | Carrier-Grade NAT | 运营商级 NAT，多家用户共享一个公网 IP，UPnP 无效 |
| STUN | Session Traversal Utilities for NAT | RFC 5389，用于发现 NAT 类型和公网映射地址 |
| TURN | Traversal Using Relays around NAT | RFC 5766，通过中继服务器转发流量，穿透所有 NAT |
| ICE | Interactive Connectivity Establishment | RFC 8445，STUN+TURN 组合的标准化连接建立流程 |
| CRDT | Conflict-free Replicated Data Type | 无冲突复制数据类型，保证多副本合并无冲突 |
| PN-Counter | Positive-Negative Counter | 支持增减的计数器 CRDT，per-replica (p,n) 取 max 合并 |
| OR-Set | Observed-Remove Set | 支持并发 add/remove 的集合 CRDT |
| LWW | Last-Write-Wins | 最后写入胜出，基于时间戳的简单冲突解决策略 |
| HLC | Hybrid Logical Clock | 混合逻辑时钟，结合物理时钟和逻辑时钟 |
| TOFU | Trust On First Use | 首次使用信任，类似 SSH 的信任模型 |
| Sybil | Sybil Attack | 女巫攻击，攻击者创建大量虚假节点控制网络 |
| Gossip | Gossip Protocol | 谣言协议，节点随机向邻居传播信息，实现最终一致 |

---

> **文档结束**  
> 本报告基于 PDC 2026-09-09 代码状态分析，后续代码变更可能影响部分结论。  
> 所有"待确认"项需在实施前通过实际部署数据验证。
