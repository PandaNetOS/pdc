//! 连接域适配器：pdc 联邦 ↔ `pnos-net` 通用会话层
//!
//! # 定位
//!
//! 本文件是 pdc **唯一**的连接域适配点（验收判据：pdc 对连接域的跨模块调用集中在此）。
//! 它做三件事，且**只做三件事**：
//!
//! | 组件 | 角色 | 归属 |
//! |---|---|---|
//! | [`FederationAuthenticator`] | 把 pdc 的 Ed25519 + blake3 握手字节注入 SDK 的 `PeerAuthenticator` | pdc 语义 |
//! | [`FederationPolicy`] | 把 NodeTable 的候选池与打分注入 SDK 的 `PeerPolicy` | pdc 语义 |
//! | [`FederationSessions`] | 无状态薄转发（SDK `SessionManager` + `PeerCapsTable`） | 纯转发 |
//!
//! **线缆格式逐字节不变**：`[4B 大端长度][1B kind][bincode payload]`，
//! `Hello`/`HelloAck` 的签名负载构造（`HelloMessage::signature_payload`）原样复用。
//!
//! # 协议版本的传递
//!
//! 握手读到的 `HelloMessage.version` 通过 `PeerIdentity.metadata`（4 字节 LE）
//! 交给 pdc，由事件循环写入 [`PeerCapsTable`]。SDK 只透传，不解释——
//! 这正是 trait 注入设计自洽的证明（迁移计划 §2.2 / K4）。
//!
//! # 心跳
//!
//! SDK 的 `tick` 负责保活探测与空闲回收；`heartbeat_kind` 设为 `Ping`(2)、
//! `heartbeat_reply_kind` 设为 `Pong`(3)。保活帧为**零载荷**——
//! 旧实现里 `PingMessage.timestamp` 仅被原样回显、`PongMessage.rtt_estimate_ms`
//! 恒为 0，两字段均无信息量，故载荷变化不改变语义（详见迁移计划 §5.1：
//! 「握手与心跳在 SDK 内重写」）。

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use pnos_net::session::{
    DisconnectReason, Frame, FrameTransport, PeerAuthenticator, PeerCandidate, PeerIdentity,
    PeerPolicy, SessionConfig, SessionEvent, SessionId, SessionInfo, SessionManager,
};
use pnos_net::types::{NodeId as SdkNodeId, Reachability as SdkReachability};
use rustc_hash::FxHashMap;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::federation::config::FederationConfig;
use crate::federation::metrics::FederationMetrics;
use crate::federation::node_id::{NodeId, NodeIdentity, Reachability};
use crate::federation::node_table::{NodeStatus, NodeTable};
use crate::federation::peer_caps::{PeerCaps, PeerCapsTable};
use crate::federation::peer_conn::PeerConn;
use crate::federation::protocol::{HelloMessage, MessageType, HELLO_PROTOCOL_VERSION};

/// 握手超时（与旧 `ConnectionManager::HANDSHAKE_TIMEOUT` 等值迁移）
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// 保活探测间隔下限（`heartbeat_timeout_secs / 3` 过小时兜底，避免探测过密）
const MIN_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// 单次 Hello 最多采纳的地址数（与旧实现一致，防节点表投毒）
const MAX_HELLO_ADDRESSES: usize = 64;

/// Hello nonce 去重表的有界上限（超出时按窗口回收，防无界增长）
const NONCE_CACHE_CAPACITY: usize = 4096;

// ---------------------------------------------------------------------------
// 类型转换（pdc ↔ SDK 各有一套同名但独立的 NodeId / Reachability）
// ---------------------------------------------------------------------------

/// pdc `NodeId` → SDK `NodeId`
#[inline]
pub fn to_sdk_node_id(id: NodeId) -> SdkNodeId {
    SdkNodeId(id.0)
}

/// SDK `NodeId` → pdc `NodeId`
#[inline]
pub fn from_sdk_node_id(id: SdkNodeId) -> NodeId {
    NodeId(id.0)
}

/// pdc `Reachability` → SDK `Reachability`（变体一一对应）
#[inline]
pub fn to_sdk_reachability(r: Reachability) -> SdkReachability {
    match r {
        Reachability::PublicIpv6 => SdkReachability::PublicIpv6,
        Reachability::Mapped => SdkReachability::Mapped,
        Reachability::HolePunchable => SdkReachability::HolePunchable,
        Reachability::OutboundOnly => SdkReachability::OutboundOnly,
        Reachability::Unknown => SdkReachability::Unknown,
    }
}

// ---------------------------------------------------------------------------
// 握手：PeerAuthenticator 注入
// ---------------------------------------------------------------------------

/// pdc 联邦握手（Ed25519 签名 + blake3 身份绑定 + 时间戳/nonce 防重放）
///
/// 字节格式与旧 `ConnectionManager::handshake_outbound/inbound` 完全一致：
/// `Hello`(kind=0) → `HelloAck`(kind=1)，payload 为 `bincode(HelloMessage)`。
pub struct FederationAuthenticator {
    identity: Arc<NodeIdentity>,
    node_table: Arc<NodeTable>,
    metrics: Arc<FederationMetrics>,
    /// 已见过的 Hello nonce（防重放）：nonce → 首次见到时刻
    seen_nonces: RwLock<FxHashMap<[u8; 16], Instant>>,
}

impl FederationAuthenticator {
    pub fn new(
        identity: Arc<NodeIdentity>,
        node_table: Arc<NodeTable>,
        metrics: Arc<FederationMetrics>,
    ) -> Self {
        Self {
            identity,
            node_table,
            metrics,
            seen_nonces: RwLock::new(FxHashMap::default()),
        }
    }

    /// 构造并序列化本节点 Hello（版本恒为 [`HELLO_PROTOCOL_VERSION`]）
    fn build_hello(&self) -> anyhow::Result<Vec<u8>> {
        let hello = HelloMessage::sign_and_build(
            &self.identity,
            self.identity.addresses_snapshot(),
            HELLO_PROTOCOL_VERSION,
            false,
        );
        bincode::serialize(&hello).map_err(|e| anyhow::anyhow!("Hello 序列化失败: {}", e))
    }

    /// 校验签名 + 身份绑定 + 时间戳新鲜度（不含 nonce 去重）
    fn verify_hello(&self, hello: &HelloMessage, expect: &str) -> anyhow::Result<()> {
        if !hello.verify_signature() {
            self.metrics.record_signature_failure();
            anyhow::bail!("{} 签名验证失败，节点 {}", expect, NodeId(hello.node_id));
        }
        if !hello.verify_identity_binding() {
            self.metrics.record_signature_failure();
            anyhow::bail!("{} 身份绑定校验失败：node_id 与公钥不匹配", expect);
        }
        if !hello.is_fresh() {
            anyhow::bail!("{} 时间戳超出重放窗口（{} ms）", expect, hello.timestamp_ms);
        }
        Ok(())
    }

    /// 记录并校验 nonce 是否为重放；重复返回 `true`
    fn is_replayed_nonce(&self, nonce: [u8; 16]) -> bool {
        let now = Instant::now();
        let ttl = Duration::from_millis(HelloMessage::REPLAY_WINDOW_MS * 2);
        let mut map = self.seen_nonces.write();
        // 有界清理：超过阈值时回收已过窗口的 nonce
        if map.len() > NONCE_CACHE_CAPACITY {
            map.retain(|_, t| now.duration_since(*t) < ttl);
        }
        if map.contains_key(&nonce) {
            return true;
        }
        map.insert(nonce, now);
        false
    }

    /// 把 Hello 携带的地址并入节点表（限 64 条，过滤无效地址）
    fn adopt_addresses(&self, hello: &HelloMessage) {
        let total_addrs = hello.addresses.len();
        let mut adopted = 0usize;
        for addr_info in hello.addresses.iter().take(MAX_HELLO_ADDRESSES) {
            let valid = addr_info
                .preferred_addr()
                .map(|a| a.port() != 0)
                .unwrap_or(false);
            if valid {
                self.node_table.add_or_update(addr_info.clone());
                adopted += 1;
            }
        }
        if total_addrs > MAX_HELLO_ADDRESSES {
            warn!(
                "[federation] Hello 携带地址过多（{}），仅采纳前 {} 条",
                total_addrs, adopted
            );
        }
    }
}

/// 把协议版本编成 `PeerIdentity.metadata`（4 字节 LE）
#[inline]
fn version_metadata(version: u32) -> Vec<u8> {
    version.to_le_bytes().to_vec()
}

/// 从 `PeerIdentity.metadata` 解出协议版本；缺失或长度不符时回退版本 1（保守）
pub fn version_from_metadata(metadata: Option<&[u8]>) -> u32 {
    match metadata {
        Some(bytes) if bytes.len() == 4 => {
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        }
        _ => 1,
    }
}

#[async_trait::async_trait]
impl PeerAuthenticator for FederationAuthenticator {
    /// 主动方向：发 Hello → 收 HelloAck（与旧 `handshake_outbound` 等价）
    async fn authenticate_outbound(&self, io: &FrameTransport) -> anyhow::Result<PeerIdentity> {
        let payload = self.build_hello()?;
        io.send_frame(MessageType::Hello.as_u8() as u16, &payload)
            .await?;

        let frame = io.recv_frame().await?;
        if frame.kind != MessageType::HelloAck.as_u8() as u16 {
            anyhow::bail!("期望 HelloAck(kind=1)，收到 kind={}", frame.kind);
        }
        let ack: HelloMessage = bincode::deserialize(&frame.payload)
            .map_err(|e| anyhow::anyhow!("HelloAck 反序列化失败: {}", e))?;

        self.verify_hello(&ack, "HelloAck")?;

        let peer_id = NodeId(ack.node_id);
        if peer_id == self.identity.node_id {
            warn!(
                "[federation] 检测到自连接（出站），已拒绝: node_id={}",
                peer_id
            );
            anyhow::bail!("拒绝自连接: {}", peer_id);
        }
        debug!(
            "[federation] 握手成功: 本地 {} <-> 远端 {} (proto={})",
            self.identity.node_id, peer_id, ack.version
        );
        Ok(PeerIdentity::new(to_sdk_node_id(peer_id), true)
            .with_metadata(version_metadata(ack.version)))
    }

    /// 被动方向：收 Hello → 发 HelloAck（与旧 `handshake_inbound` 等价）
    async fn authenticate_inbound(&self, io: &FrameTransport) -> anyhow::Result<PeerIdentity> {
        let frame = io.recv_frame().await?;
        if frame.kind != MessageType::Hello.as_u8() as u16 {
            anyhow::bail!("期望 Hello(kind=0)，收到 kind={}", frame.kind);
        }
        let hello: HelloMessage = bincode::deserialize(&frame.payload)
            .map_err(|e| anyhow::anyhow!("Hello 反序列化失败: {}", e))?;

        self.verify_hello(&hello, "Hello")?;

        if self.is_replayed_nonce(hello.nonce) {
            anyhow::bail!("Hello nonce 重放，已拒绝");
        }

        let peer_id = NodeId(hello.node_id);
        if peer_id == self.identity.node_id {
            warn!(
                "[federation] 检测到自连接（入站），已拒绝: node_id={}",
                peer_id
            );
            anyhow::bail!("拒绝自连接: {}", peer_id);
        }

        // 回 HelloAck（签名），携带本节点协议版本
        let ack_payload = self.build_hello()?;
        io.send_frame(MessageType::HelloAck.as_u8() as u16, &ack_payload)
            .await?;

        self.adopt_addresses(&hello);

        Ok(PeerIdentity::new(to_sdk_node_id(peer_id), true)
            .with_metadata(version_metadata(hello.version)))
    }

    fn timeout(&self) -> Duration {
        HANDSHAKE_TIMEOUT
    }
}

// ---------------------------------------------------------------------------
// 候选与打分：PeerPolicy 注入
// ---------------------------------------------------------------------------

/// 由 NodeTable 提供候选池与打分的策略
///
/// SDK 每轮 `tick` 调用 [`PeerPolicy::candidates`]，过滤已连接者后按
/// [`PeerPolicy::score`] 降序补齐到 [`PeerPolicy::target_sessions`]。
/// **连接状态由 SDK 持有**，本策略是纯函数。
pub struct FederationPolicy {
    node_table: Arc<NodeTable>,
    target_sessions: usize,
}

impl FederationPolicy {
    pub fn new(node_table: Arc<NodeTable>, target_sessions: usize) -> Self {
        Self {
            node_table,
            target_sessions,
        }
    }
}

impl PeerPolicy for FederationPolicy {
    fn candidates(&self) -> Vec<PeerCandidate> {
        self.node_table
            .all_nodes()
            .into_iter()
            .filter(|e| !matches!(e.status, NodeStatus::Failed))
            .filter_map(|e| {
                let addr = e.info.preferred_addr()?;
                let peer = to_sdk_node_id(NodeId(e.info.node_id));
                let weight = e.activity_score() as i64;
                Some(
                    PeerCandidate::new(peer, vec![addr])
                        .with_reachability(to_sdk_reachability(e.info.reachability))
                        .with_nat_type(e.info.nat_type.clone())
                        .with_weight(weight),
                )
            })
            .collect()
    }

    fn score(&self, candidate: &PeerCandidate, live: Option<&SessionInfo>) -> i64 {
        // 已连接者由 SDK 在补齐阶段过滤，这里只兜底：已连接且已知 RTT 时，
        // RTT 越小分越高（与 `NodeEntry::activity_score` 的 RTT 项同向）。
        if let Some(info) = live {
            if let Some(rtt) = info.rtt_ms {
                return 1_000 - rtt.min(1_000) as i64;
            }
            return 500;
        }
        candidate.weight
    }

    fn target_sessions(&self) -> usize {
        self.target_sessions
    }
}

// ---------------------------------------------------------------------------
// 会话配置映射
// ---------------------------------------------------------------------------

/// 由联邦配置构造 SDK 会话配置
///
/// 字段映射（旧语义 → SDK 字段）：
/// - `heartbeat_timeout_secs` → `idle_timeout`（空闲超时断连）
/// - `reconnect_cooldown_secs` → `addr_cooldown`（地址级冷却，与旧 `cooldown_addr` 等值）
/// - `transport_write_*` → 写超时与退避重试
/// - `max_connections` → `max_sessions`
///
/// `listen` 为 `false` 时不监听入站（供 G4 适配期使用；切换期由 SDK 独占端口）。
pub fn session_config_from(config: &FederationConfig, listen: bool) -> SessionConfig {
    let idle_timeout = Duration::from_secs(config.heartbeat_timeout_secs);
    // 保活探测间隔：取空闲超时的 1/3。
    // - 下限 `MIN_HEARTBEAT_INTERVAL` 避免探测过密；
    // - 上限 `idle_timeout / 2` 是**硬不变式**：SDK 的 `idle_ms` 以「上次收到数据」计时，
    //   主动发探测帧并不刷新 idle，只有对端应答（Pong）被收到才刷新。
    //   故探测间隔必须显著小于空闲超时，否则连接会被自己的空闲回收误杀。
    //   （小 `heartbeat_timeout_secs` 场景下上限优先于下限。）
    let heartbeat_interval = (idle_timeout / 3)
        .max(MIN_HEARTBEAT_INTERVAL)
        .min(idle_timeout / 2);
    let listen_addr = if listen {
        Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            config.listen_port,
        ))
    } else {
        None
    };
    SessionConfig {
        listen_addr,
        max_sessions: config.max_connections,
        heartbeat_interval,
        idle_timeout,
        addr_cooldown: Duration::from_secs(config.reconnect_cooldown_secs),
        // 保活帧：Ping(2) 探测 / Pong(3) 应答（零载荷，SDK 内部消化，不上抛业务）
        heartbeat_kind: Some(MessageType::Ping.as_u8() as u16),
        heartbeat_reply_kind: Some(MessageType::Pong.as_u8() as u16),
        write_timeout: Duration::from_secs(config.transport_write_timeout_secs),
        write_max_retries: config.transport_write_max_retries,
        write_retry_base_ms: config.transport_write_retry_base_ms,
    }
}

// ---------------------------------------------------------------------------
// 薄转发门面
// ---------------------------------------------------------------------------

/// 联邦会话门面（**无状态**，只转发到 SDK + 能力表）
///
/// 设计决定 K1：保留一个薄适配器而非让 80+ 调用点直连 SDK——
/// `get_connection() + send_message()` → `send_to()` 是语义变化，逐点直改风险大。
/// 本类型不持有任何连接状态（状态全在 SDK `SessionManager` 内），故不违反归口。
pub struct FederationSessions {
    mgr: Arc<SessionManager>,
    caps: Arc<PeerCapsTable>,
}

impl FederationSessions {
    pub fn new(mgr: Arc<SessionManager>, caps: Arc<PeerCapsTable>) -> Self {
        Self { mgr, caps }
    }

    /// 底层 `SessionManager`（调度注册、事件订阅用）
    pub fn manager(&self) -> &Arc<SessionManager> {
        &self.mgr
    }

    /// 对端能力表
    pub fn caps(&self) -> &Arc<PeerCapsTable> {
        &self.caps
    }

    /// 订阅会话事件（业务分派循环消费）
    pub fn subscribe(&self) -> broadcast::Receiver<SessionEvent> {
        self.mgr.subscribe()
    }

    // ---------------------------------------------------------------
    // 连接
    // ---------------------------------------------------------------

    pub async fn connect(
        &self,
        peer: NodeId,
        addrs: &[SocketAddr],
        reach: Reachability,
    ) -> anyhow::Result<SessionId> {
        self.mgr
            .connect(to_sdk_node_id(peer), addrs, to_sdk_reachability(reach))
            .await
    }

    /// 主动补齐（由调度器周期调用；SDK 内部幂等、无 sleep）
    pub async fn tick(&self) {
        self.mgr.tick().await;
    }

    pub async fn disconnect(
        &self,
        session: SessionId,
        reason: pnos_net::session::DisconnectReason,
    ) {
        self.mgr.disconnect(session, reason).await;
    }

    pub async fn shutdown(&self) {
        self.mgr.shutdown().await;
    }

    // ---------------------------------------------------------------
    // 收发
    // ---------------------------------------------------------------

    /// 按类型发送到指定对端（内部 bincode 序列化，kind 由 `MessageType` 决定）
    pub async fn send_to<T: serde::Serialize>(
        &self,
        peer: &NodeId,
        msg_type: MessageType,
        msg: &T,
    ) -> anyhow::Result<()> {
        let payload = bincode::serialize(msg).map_err(|e| anyhow::anyhow!("序列化失败: {}", e))?;
        self.mgr
            .send_to(
                &to_sdk_node_id(*peer),
                Frame::new(msg_type.as_u8() as u16, payload),
            )
            .await
    }

    /// 发送原始 payload（调用方已自行序列化，避免二次编码）
    pub async fn send_to_raw(
        &self,
        peer: &NodeId,
        msg_type: MessageType,
        payload: Vec<u8>,
    ) -> anyhow::Result<()> {
        self.mgr
            .send_to(
                &to_sdk_node_id(*peer),
                Frame::new(msg_type.as_u8() as u16, payload),
            )
            .await
    }

    /// 广播，返回成功条数
    pub async fn broadcast<T: serde::Serialize>(
        &self,
        msg_type: MessageType,
        msg: &T,
        exclude: Option<SessionId>,
    ) -> usize {
        match bincode::serialize(msg) {
            Ok(payload) => {
                self.mgr
                    .broadcast(Frame::new(msg_type.as_u8() as u16, payload), exclude)
                    .await
            }
            Err(e) => {
                warn!("[federation] 广播序列化失败: {}", e);
                0
            }
        }
    }

    // ---------------------------------------------------------------
    // 查询（全部由 SDK 提供，本层不做缓存）
    // ---------------------------------------------------------------

    /// 当前活跃会话数（对应旧 `connection_count()`）
    pub fn connection_count(&self) -> usize {
        self.mgr.stats().active as usize
    }

    /// 事件驱动的生命周期计数快照（established / closed / active 满足恒等）
    pub fn stats(&self) -> pnos_net::session::SessionStatsSnapshot {
        self.mgr.stats()
    }

    /// 全部会话快照
    pub fn sessions(&self) -> Vec<SessionInfo> {
        self.mgr.sessions()
    }

    /// 活跃对端（pdc NodeId）
    pub fn active_peers(&self) -> Vec<NodeId> {
        self.mgr
            .sessions()
            .into_iter()
            .map(|s| from_sdk_node_id(s.peer_id))
            .collect()
    }

    pub fn is_connected(&self, peer: &NodeId) -> bool {
        self.mgr.has_session(&to_sdk_node_id(*peer))
    }

    /// 地址是否已被任一会话占用（替代旧 `is_seed_connected` 的 IP 匹配兜底）
    pub fn is_addr_connected(&self, addr: &SocketAddr) -> bool {
        self.mgr.is_addr_connected(addr)
    }

    /// 对端协议能力（从能力表读取，不再依赖连接对象）
    pub fn peer_caps(&self, peer: &NodeId) -> PeerCaps {
        self.caps.of(peer)
    }

    /// 对端 RTT（毫秒），无会话或未知时返回 `None`
    pub fn peer_rtt_ms(&self, peer: &NodeId) -> Option<u32> {
        self.mgr
            .sessions()
            .into_iter()
            .find(|s| s.peer_id.0 == peer.0)
            .and_then(|s| s.rtt_ms)
    }

    /// 指定会话的完整快照
    pub fn session_of(&self, peer: &NodeId) -> Option<SessionInfo> {
        self.mgr
            .sessions()
            .into_iter()
            .find(|s| s.peer_id.0 == peer.0)
    }

    /// 指定对端的对端地址（无会话时 `None`）
    pub fn peer_addr(&self, peer: &NodeId) -> Option<SocketAddr> {
        self.session_of(peer).map(|s| s.addr)
    }

    /// 全部活跃会话的连接视图（替代旧 `ConnectionManager::all_connections`）
    ///
    /// 返回 [`PeerConn`] 而非 `SessionInfo`：消费侧（`sync/` / `gossip.rs` /
    /// `discovery.rs`）共用同一视图类型，故切换是签名替换而非逻辑改写。
    pub fn connections(self: &Arc<Self>) -> Vec<Arc<PeerConn>> {
        self.mgr
            .sessions()
            .into_iter()
            .map(|i| Arc::new(PeerConn::sdk(from_sdk_node_id(i.peer_id), self.clone())))
            .collect()
    }

    /// 指定对端的连接视图（替代旧 `ConnectionManager::get_connection`）
    pub fn connection_of(self: &Arc<Self>, peer: &NodeId) -> Option<Arc<PeerConn>> {
        self.mgr
            .has_session(&to_sdk_node_id(*peer))
            .then(|| Arc::new(PeerConn::sdk(*peer, self.clone())))
    }

    // ---------------------------------------------------------------
    // 断连（替代旧 `ConnectionManager::remove_connection`）
    // ---------------------------------------------------------------

    /// 按对端 NodeId 主动断开（无会话时为无操作），返回是否断开了一条会话
    ///
    /// 旧 `remove_connection(&NodeId)` 是同步的（直接从 HashMap 摘除）。
    /// SDK 的断连需要走发送关闭帧 + 反注册，故为 `async`；
    /// 调用点若在同步上下文，可用 [`Self::disconnect_peer_detached`]。
    pub async fn disconnect_peer(&self, peer: &NodeId, reason: DisconnectReason) -> bool {
        match self.session_of(peer) {
            Some(info) => {
                self.mgr.disconnect(SessionId(info.id), reason).await;
                true
            }
            None => false,
        }
    }

    /// 同步上下文用的断连：内部 `spawn`，立即返回（无会话时为无操作）
    ///
    /// 仅用于无法 `.await` 的调用点（如持有 `parking_lot` 锁时的收尾路径）。
    /// 优先使用 [`Self::disconnect_peer`]。
    pub fn disconnect_peer_detached(&self, peer: NodeId, reason: DisconnectReason) -> bool {
        match self.session_of(&peer) {
            Some(info) => {
                let mgr = self.mgr.clone();
                tokio::spawn(async move {
                    mgr.disconnect(SessionId(info.id), reason).await;
                });
                true
            }
            None => false,
        }
    }

    /// 全部活跃对端（pdc `NodeId`）
    pub fn peer_ids(&self) -> Vec<NodeId> {
        self.active_peers()
    }
}

// ---------------------------------------------------------------------------
// 延迟绑定句柄
// ---------------------------------------------------------------------------

/// 会话门面的**延迟绑定**句柄
///
/// # 为什么需要它
///
/// `FederationSessions` 必须在 `NetAgent` 就绪后才能 `bind`（`bind` 是 `async`，
/// 而它要吞掉 `Arc<NetAgent>` 作为 `Dialer`）。但 `FederationService::new` 是同步的，
/// 且 `GossipEngine` / `SyncManager` / `DiscoveryService` 都在 `new` 里就被构造出来——
/// 它们拿不到尚未存在的门面。
///
/// 旧 `ConnectionManager` 用 `OnceCell<Arc<NetAgent>>` 解决同一问题（`set_net_agent`）。
/// 本类型是这个模式的显式化：**先共享句柄，后填充内容**。
///
/// # 语义
///
/// - `current()` 返回 `None` 表示会话层尚未就绪（进程启动早期）。
///   此时任何连接操作都应视为「暂时无连接」而非错误——这正是旧代码
///   `self.net_agent.get()` 返回 `None` 时的处理方式。
/// - `bind_once()` 幂等：重复绑定返回 `Err`，不覆盖已绑定的门面。
pub struct SessionsHandle {
    inner: std::sync::OnceLock<Arc<FederationSessions>>,
}

/// 「只允许绑定一次」的公共语义（可单测，与具体类型解耦）
fn set_once<T>(cell: &std::sync::OnceLock<T>, value: T) -> anyhow::Result<()> {
    cell.set(value)
        .map_err(|_| anyhow::anyhow!("会话门面已绑定，不可重复绑定"))
}

impl SessionsHandle {
    pub fn new() -> Self {
        Self {
            inner: std::sync::OnceLock::new(),
        }
    }

    /// 绑定会话门面（只能成功一次）
    pub fn bind_once(&self, sessions: Arc<FederationSessions>) -> anyhow::Result<()> {
        set_once(&self.inner, sessions)
    }

    /// 已绑定的门面；尚未绑定时返回 `None`
    pub fn current(&self) -> Option<&Arc<FederationSessions>> {
        self.inner.get()
    }

    pub fn is_ready(&self) -> bool {
        self.inner.get().is_some()
    }

    // ---------------------------------------------------------------
    // 迁移期兼容层
    //
    // 方法名**沿用旧 `ConnectionManager`**，使 6 个业务模块
    // （`gossip` / `sync` / `discovery` / `dht_discovery` / `relay` / `signaling`）
    // 的调用点在「承载从 pdc 自有连接切到 SDK 会话」时保持**零改动**——
    // 这正是设计决定 K1（保留一个无状态薄适配器，不逐点直改 80+ 调用点）的落地。
    //
    // 未绑定（`NetAgent` 就绪前）时一律返回「无连接」语义而非报错，
    // 与旧代码 `self.net_agent.get()` 返回 `None` 时的处理一致。
    // ---------------------------------------------------------------

    /// 全部活跃会话的连接视图（旧 `ConnectionManager::all_connections`）
    pub fn all_connections(&self) -> Vec<Arc<PeerConn>> {
        self.current().map(|s| s.connections()).unwrap_or_default()
    }

    /// 指定对端的连接视图（旧 `ConnectionManager::get_connection`）
    pub fn get_connection(&self, peer: &NodeId) -> Option<Arc<PeerConn>> {
        self.current().and_then(|s| s.connection_of(peer))
    }

    /// 活跃会话数（旧 `ConnectionManager::connection_count`）
    pub fn connection_count(&self) -> usize {
        self.current().map(|s| s.connection_count()).unwrap_or(0)
    }

    /// 地址是否已被任一会话占用（旧 `ConnectionManager::is_seed_connected`）
    pub fn is_seed_connected(&self, addr: SocketAddr) -> bool {
        self.current()
            .map(|s| s.is_addr_connected(&addr))
            .unwrap_or(false)
    }

    /// 主动拨号（旧 `ConnectionManager::connect_to`）
    ///
    /// 返回对端连接视图。**关键细节**：握手前 `peer` 可能只是随机占位 `temp_id`
    /// （`connect_seed` / `connect_cached_node` 场景），SDK 会以握手得到的
    /// **真实 node_id** 注册会话，故这里按 `SessionId` 反查真实对端后再构造视图 ——
    /// 否则后续 `send_message` 会因占位 ID 查不到会话而失败。
    ///
    /// 对端可达性由 SDK 的 `PeerPolicy` 侧维护，这里沿用 `Unknown`。
    pub async fn connect_to(
        &self,
        peer: NodeId,
        addr: SocketAddr,
    ) -> anyhow::Result<Arc<PeerConn>> {
        let s = match self.current() {
            Some(s) => s.clone(),
            None => anyhow::bail!("会话层尚未就绪，无法连接 {}", peer),
        };
        let sid = s.connect(peer, &[addr], Reachability::Unknown).await?;
        let real_peer = s
            .sessions()
            .into_iter()
            .find(|i| i.id == sid.0)
            .map(|i| from_sdk_node_id(i.peer_id))
            .unwrap_or(peer);
        Ok(Arc::new(PeerConn::sdk(real_peer, s)))
    }

    /// 主动断开某对端（旧 `ConnectionManager::remove_connection`）
    pub fn remove_connection(&self, peer: &NodeId) {
        if let Some(s) = self.current() {
            s.disconnect_peer_detached(*peer, DisconnectReason::Local);
        }
    }

    /// 重新拨打已知节点（旧 `ConnectionManager::reconnect_discovered`）
    ///
    /// 翻转后候选挑选归 SDK：本方法等价于一次 `tick()`（幂等、无内部 sleep）。
    pub async fn reconnect_discovered(&self) {
        self.heartbeat_tick().await;
    }

    /// 保活探测 + 补链单次执行（旧 `ConnectionManager::heartbeat_tick`）
    pub async fn heartbeat_tick(&self) {
        if let Some(s) = self.current() {
            s.tick().await;
        }
    }

    /// 关闭全部会话（旧 `ConnectionManager::shutdown_all`）
    pub async fn shutdown_all(&self) {
        if let Some(s) = self.current() {
            s.shutdown().await;
        }
    }

    /// 测试用：**未绑定**句柄（`current() == None` → 全部查询返回「无连接」语义）
    ///
    /// 各模块单测原先构造 `ConnectionManager::new_for_test(..)`，其断言形如
    /// `connection_count() == 0` / `all_connections().is_empty()`——未绑定句柄天然满足。
    #[cfg(test)]
    pub fn new_for_test() -> Arc<Self> {
        Arc::new(Self::new())
    }
}

impl Default for SessionsHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// 构造联邦会话门面（供 `FederationService` 在 NetAgent 就绪后调用）
///
/// `listen` 为 `true` 时 SDK 立即开始受理入站（切换期由 SDK 独占监听端口）。
pub async fn bind_federation_sessions(
    config: &FederationConfig,
    net_agent: Arc<pnos_net::NetAgent>,
    identity: Arc<NodeIdentity>,
    node_table: Arc<NodeTable>,
    metrics: Arc<FederationMetrics>,
    caps: Arc<PeerCapsTable>,
    listen: bool,
) -> anyhow::Result<FederationSessions> {
    let local_id = identity.node_id;
    let auth: Arc<dyn PeerAuthenticator> = Arc::new(FederationAuthenticator::new(
        identity,
        node_table.clone(),
        metrics,
    ));
    let policy: Arc<dyn PeerPolicy> =
        Arc::new(FederationPolicy::new(node_table, config.max_connections));
    let mgr = SessionManager::bind(
        to_sdk_node_id(local_id),
        session_config_from(config, listen),
        net_agent,
        auth,
        policy,
    )
    .await?;
    Ok(FederationSessions::new(mgr, caps))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_metadata_roundtrip() {
        let md = version_metadata(6);
        assert_eq!(md.len(), 4);
        assert_eq!(version_from_metadata(Some(&md)), 6);
    }

    #[test]
    fn test_version_metadata_missing_falls_back_to_1() {
        assert_eq!(version_from_metadata(None), 1);
        assert_eq!(version_from_metadata(Some(&[1, 2, 3])), 1);
    }

    #[test]
    fn test_node_id_roundtrip() {
        let pdc = NodeId([9u8; 20]);
        let sdk = to_sdk_node_id(pdc);
        assert_eq!(sdk.0, pdc.0);
        assert_eq!(from_sdk_node_id(sdk).0, pdc.0);
    }

    #[test]
    fn test_reachability_mapping_is_total() {
        for r in [
            Reachability::PublicIpv6,
            Reachability::Mapped,
            Reachability::HolePunchable,
            Reachability::OutboundOnly,
            Reachability::Unknown,
        ] {
            // 映射必须是全函数且不 panic；同一输入两次结果一致
            let a = to_sdk_reachability(r);
            let b = to_sdk_reachability(r);
            assert_eq!(a, b);
        }
    }

    #[test]
    fn test_session_config_maps_ping_pong_kinds() {
        let cfg = FederationConfig::default();
        let sc = session_config_from(&cfg, false);
        assert!(sc.listen_addr.is_none(), "listen=false 不应监听");
        assert_eq!(sc.heartbeat_kind, Some(MessageType::Ping.as_u8() as u16));
        assert_eq!(
            sc.heartbeat_reply_kind,
            Some(MessageType::Pong.as_u8() as u16)
        );
        assert_eq!(sc.max_sessions, cfg.max_connections);
        assert_eq!(
            sc.idle_timeout,
            Duration::from_secs(cfg.heartbeat_timeout_secs)
        );
        assert_eq!(
            sc.addr_cooldown,
            Duration::from_secs(cfg.reconnect_cooldown_secs)
        );
    }

    #[test]
    fn test_session_config_listen_true_binds_port() {
        let cfg = FederationConfig::default();
        let sc = session_config_from(&cfg, true);
        let addr = sc.listen_addr.expect("listen=true 应有监听地址");
        assert_eq!(addr.port(), cfg.listen_port);
        assert!(addr.ip().is_unspecified());
    }

    #[test]
    fn test_heartbeat_interval_lower_bound_for_normal_timeout() {
        let cfg = FederationConfig {
            heartbeat_timeout_secs: 60, // 60/3 = 20s，高于下限 5s
            ..Default::default()
        };
        let sc = session_config_from(&cfg, false);
        assert_eq!(sc.heartbeat_interval, Duration::from_secs(20));
        assert!(sc.heartbeat_interval >= MIN_HEARTBEAT_INTERVAL);
    }

    #[test]
    fn test_heartbeat_interval_always_below_idle_timeout() {
        // 硬不变式：探测间隔必须严格小于空闲超时。
        // SDK 的 `idle_ms` 以「上次收到数据」计时，主动发探测帧不刷新 idle，
        // 只有对端 Pong 被收到才刷新 —— 间隔过大则连接会被自己的空闲回收误杀。
        for secs in [1u64, 3, 12, 30, 60, 300] {
            let cfg = FederationConfig {
                heartbeat_timeout_secs: secs,
                ..Default::default()
            };
            let sc = session_config_from(&cfg, false);
            assert!(
                sc.heartbeat_interval < sc.idle_timeout,
                "timeout={}s 时 interval={:?} 不应 >= idle={:?}",
                secs,
                sc.heartbeat_interval,
                sc.idle_timeout
            );
        }
    }

    #[test]
    fn test_sessions_handle_starts_unbound() {
        let h = SessionsHandle::new();
        assert!(!h.is_ready());
        assert!(h.current().is_none());
    }

    #[test]
    fn test_sessions_handle_bind_once_is_idempotent() {
        // 验证 `set_once` 语义（句柄的绑定契约），与具体门面类型解耦。
        let cell: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
        assert!(set_once(&cell, 1).is_ok());
        assert!(set_once(&cell, 2).is_err(), "重复绑定必须失败");
        assert_eq!(cell.get(), Some(&1), "首次绑定值不可被覆盖");
        // 句柄在未绑定时不可用
        let h = SessionsHandle::new();
        assert!(h.current().is_none());
        assert!(!h.is_ready());
    }
}
