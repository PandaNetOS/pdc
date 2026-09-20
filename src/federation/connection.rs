//! 连接管理
//!
//! 管理联邦网络中的所有 TCP 连接，包括监听、主动连接、握手、心跳和消息分发。

use std::collections::VecDeque;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use parking_lot::{Mutex as ParkingMutex, RwLock};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::{broadcast, Mutex as TokioMutex, Notify, OnceCell, Semaphore};
use tracing::{debug, info, warn};

use crate::federation::config::FederationConfig;
use crate::federation::metrics::FederationMetrics;
use crate::federation::node_id::NodeId;
use crate::federation::node_table::{NodeStatus, NodeTable};
use crate::federation::protocol::*;
use crate::federation::signaling::SignalingService;
use crate::federation::transport::TcpTransport;
use pnos_net::transport::{TcpTransportStream, TransportKind, TransportStream};

/// 接受连接失败后的退避等待
const ACCEPT_FAILURE_BACKOFF: Duration = Duration::from_millis(100);

/// 握手超时（入站/出站共用）：未完成签名的连接不得长期占用资源，防慢速连接 DoS
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// 本节点联邦协议版本（Hello/HelloAck 的 version 字段）。
/// 1：旧版本（仅全量推送差异分片）。
/// 2：支持 DiffSync key 列表交换（先交换 key 列表，只推送对方缺失条目，重复率 ~90%→<5%）。
/// 3：支持分层 Merkle 对比 + 分片并行同步（L0→L1→L2 三层定位差异，只同步差异 L2 分片，支持并行+断点续传+流式加载）。
/// 4：支持增量（delta）同步通道（OpsRequest/OpsBatch，基于本地 oplog 的 O(Δ) 稳态同步）。
/// 5：支持 Range-based（有序区间 + 分界点下钻）反熵（RangeReconcileRequest/Response）。
/// 6：支持 bootstrap 专用通道（BootstrapManifest*/BootstrapChunk*，与在线反熵解耦的全量引导）。
/// 对端 version < 2 时回退到原始全量推送；version == 2 时使用 DiffSync key 交换；version >= 3 时使用分层 Merkle；
/// version >= 4 且 `federation.delta_sync_enabled=true` 时启用 delta 通道；
/// version >= 5 且 `federation.range_reconcile_enabled=true` 时启用 range 反熵；
/// version >= 6 且 `federation.bootstrap_enabled=true` 时启用 bootstrap 通道。
pub const HELLO_PROTOCOL_VERSION: u32 = 6;

/// 单条连接
pub struct Connection {
    /// TCP 传输层（内部读写分离，独立锁，可并发收发）
    pub transport: TcpTransport,
    /// 对端节点 ID
    pub node_id: NodeId,
    /// 对端地址
    pub addr: SocketAddr,
    /// 连接建立时间
    pub connected_at: Instant,
    /// 最后活跃时间
    pub last_active: RwLock<Instant>,
    /// 待处理消息计数（接收端背压监控）：dispatch 前 +1，处理完成 -1。
    /// 超过配置阈值时输出背压告警日志。
    pub pending: AtomicU32,
    /// P1: 接收端 GossipBatch 攒批缓冲区
    pub gossip_buffer: ParkingMutex<VecDeque<GossipBatchMessage>>,
    /// P1: flush 任务唤醒通知
    pub gossip_flush_notify: Arc<Notify>,
    /// 对端协议版本（握手 Hello/HelloAck 中的 version 字段）。
    /// 默认 1（旧版本）；>= HELLO_PROTOCOL_VERSION 表示对端支持 DiffSync key 列表交换。
    pub peer_protocol_version: AtomicU32,
}

impl Connection {
    /// 创建新连接
    pub fn new(transport: TcpTransport, node_id: NodeId, addr: SocketAddr) -> Self {
        Self {
            transport,
            node_id,
            addr,
            connected_at: Instant::now(),
            last_active: RwLock::new(Instant::now()),
            pending: AtomicU32::new(0),
            gossip_buffer: ParkingMutex::new(VecDeque::new()),
            gossip_flush_notify: Arc::new(Notify::new()),
            peer_protocol_version: AtomicU32::new(1),
        }
    }

    /// 设置对端协议版本（握手后由 ConnectionManager 调用）
    pub fn set_peer_protocol_version(&self, v: u32) {
        self.peer_protocol_version.store(v, Ordering::Relaxed);
    }

    /// 对端是否支持 DiffSync key 列表交换（协议版本 >= 2）
    pub fn supports_diff_keys(&self) -> bool {
        self.peer_protocol_version.load(Ordering::Relaxed) >= 2
    }

    /// 对端是否支持分层 Merkle 对比 + 分片并行同步（协议版本 >= 3）
    pub fn supports_layered_merkle(&self) -> bool {
        self.peer_protocol_version.load(Ordering::Relaxed) >= 3
    }

    /// 对端是否支持增量（delta）同步通道（协议版本 >= 4）
    pub fn supports_delta_sync(&self) -> bool {
        crate::federation::sync::delta::supports_delta_sync(
            self.peer_protocol_version.load(Ordering::Relaxed),
        )
    }

    /// 对端是否支持 Range-based（有序区间下钻）反熵（协议版本 >= 5）
    pub fn supports_range_reconcile(&self) -> bool {
        crate::federation::sync::range_reconcile::supports_range_reconcile(
            self.peer_protocol_version.load(Ordering::Relaxed),
        )
    }

    /// 对端是否支持 bootstrap 专用通道（协议版本 >= 6）
    pub fn supports_bootstrap(&self) -> bool {
        self.peer_protocol_version.load(Ordering::Relaxed)
            >= crate::federation::sync::bootstrap::BOOTSTRAP_PROTOCOL_VERSION
    }

    /// 发送消息
    pub async fn send_message<T: serde::Serialize>(
        &self,
        msg_type: MessageType,
        msg: &T,
    ) -> anyhow::Result<()> {
        // Ping/Pong 走高优先级通道，避免排在大同步消息后面导致心跳超时
        match msg_type {
            MessageType::Ping | MessageType::Pong => {
                self.transport
                    .send_message_high_priority(msg_type, msg)
                    .await?;
            }
            _ => {
                self.transport.send_message(msg_type, msg).await?;
            }
        }
        *self.last_active.write() = Instant::now();
        Ok(())
    }

    /// 接收消息
    pub async fn recv_message(&self) -> anyhow::Result<(MessageType, Vec<u8>)> {
        let result = self.transport.recv_message().await?;
        *self.last_active.write() = Instant::now();
        Ok(result)
    }

    /// 更新最后活跃时间
    pub fn touch(&self) {
        *self.last_active.write() = Instant::now();
    }

    /// 距上次活跃的时间
    pub fn idle_duration(&self) -> Duration {
        self.last_active.read().elapsed()
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("node_id", &self.node_id)
            .field("addr", &self.addr)
            .field("connected_at", &self.connected_at)
            .finish()
    }
}

/// 连接管理器
pub struct ConnectionManager {
    /// 活跃连接池
    connections: RwLock<FxHashMap<NodeId, Arc<Connection>>>,
    /// 正在连接中的地址（防止并发重复连接）
    connecting: RwLock<FxHashSet<SocketAddr>>,
    /// 每个 node_id 一把连接锁，确保同一时刻只有一个方向在创建连接
    connecting_locks: RwLock<FxHashMap<NodeId, Arc<TokioMutex<()>>>>,
    /// 节点表
    node_table: Arc<NodeTable>,
    /// 节点身份
    identity: Arc<crate::federation::node_id::NodeIdentity>,
    /// 配置
    config: FederationConfig,
    /// 关闭信号发送端
    shutdown: broadcast::Sender<()>,
    /// 发现服务（延迟注入，解决循环依赖）
    discovery: OnceCell<Arc<crate::federation::discovery::DiscoveryService>>,
    /// 同步管理器（延迟注入）
    sync_manager: OnceCell<Arc<crate::federation::sync::SyncManager>>,
    /// 打洞信令服务（延迟注入）
    signaling_service: OnceCell<Arc<SignalingService>>,
    /// 中继管理器（延迟注入）
    relay_manager: OnceCell<Arc<crate::federation::relay::RelayManager>>,
    /// pnos-net 外部互联 Agent（延迟注入，用于替代原始 TCP 连接）
    net_agent: OnceCell<Arc<pnos_net::NetAgent>>,
    /// 监控指标
    metrics: Arc<FederationMetrics>,
    /// 重连冷却到期时间（NodeId -> 冷却截止时刻），断开后在此时间内不主动重连
    cooldown_until: RwLock<FxHashMap<NodeId, Instant>>,
    /// 地址级重连冷却（SocketAddr -> 冷却截止时刻）
    /// 重连时用 temp_id 还不知道 node_id，所以按地址冷却，防止连续建连死循环
    cooldown_addr: RwLock<FxHashMap<SocketAddr, Instant>>,
    /// 禁止出站的 node_id 集合（NodeId 仲裁后，大 node_id 不主动出站）
    no_outbound: RwLock<FxHashSet<NodeId>>,
    /// 重量级消息处理的有界并发信号量（GossipBatch/MerkleRepair 经 spawn_blocking 执行，
    /// 先 acquire permit 再 spawn，避免无界生成阻塞任务导致内存暴涨）
    heavy_task_semaphore: Arc<Semaphore>,
    /// 种子节点配置地址 → 握手后获知的真实 node_id 映射
    /// 用于连接维护任务：入站连接对端端口不固定，地址精确匹配永远失败，
    /// 需通过已记录的真实 node_id 判断种子节点是否已连接。
    seed_node_ids: RwLock<FxHashMap<SocketAddr, NodeId>>,
    /// 上次触发自动重连的 unix 秒（冷却限流，由 GossipEngine 在无连接时调用）
    last_reconnect_attempt: AtomicU64,
    /// 联邦实时 peer 查询响应收集器：key=infohash，value=各联邦节点返回的 peer 条目。
    /// query_peers 发起时清空对应 key，dispatch 收到 PeerQueryResponse 时 push，
    /// 超时后由 query_peers 取走结果并删除 key。
    peer_query_responses: Arc<DashMap<[u8; 20], Vec<PeerQueryEntry>>>,
    /// 已见过的 Hello nonce（防重放）：nonce -> 首次见到时刻
    seen_hello_nonces: RwLock<FxHashMap<[u8; 16], Instant>>,
    /// 已告警过的「自连接目标地址」集合：这类地址会被周期性重试，
    /// 只首次打 warn、之后降级 debug，避免用新的一类刷屏取代旧的刷屏。
    self_addr_warned: RwLock<FxHashSet<SocketAddr>>,
}

impl ConnectionManager {
    /// 创建连接管理器
    pub fn new(
        node_table: Arc<NodeTable>,
        identity: Arc<crate::federation::node_id::NodeIdentity>,
        config: FederationConfig,
        shutdown: broadcast::Sender<()>,
        metrics: Arc<FederationMetrics>,
    ) -> Self {
        Self {
            connections: RwLock::new(FxHashMap::default()),
            connecting: RwLock::new(FxHashSet::default()),
            connecting_locks: RwLock::new(FxHashMap::default()),
            node_table,
            identity,
            config: config.clone(),
            shutdown,
            discovery: OnceCell::new(),
            sync_manager: OnceCell::new(),
            signaling_service: OnceCell::new(),
            relay_manager: OnceCell::new(),
            net_agent: OnceCell::new(),
            metrics,
            cooldown_until: RwLock::new(FxHashMap::default()),
            cooldown_addr: RwLock::new(FxHashMap::default()),
            no_outbound: RwLock::new(FxHashSet::default()),
            heavy_task_semaphore: Arc::new(Semaphore::new(
                config.heavy_task_max_concurrency.max(1),
            )),
            seed_node_ids: RwLock::new(FxHashMap::default()),
            last_reconnect_attempt: AtomicU64::new(0),
            peer_query_responses: Arc::new(DashMap::new()),
            seen_hello_nonces: RwLock::new(FxHashMap::default()),
            self_addr_warned: RwLock::new(FxHashSet::default()),
        }
    }

    /// 记录并校验 Hello nonce 是否为重放；重复返回 true。
    fn is_replayed_nonce(&self, nonce: [u8; 16]) -> bool {
        let now = Instant::now();
        let ttl = Duration::from_millis(HelloMessage::REPLAY_WINDOW_MS * 2);
        let mut map = self.seen_hello_nonces.write();
        // 有界清理：超过阈值时回收已过窗口的 nonce，防止无界增长
        if map.len() > 4096 {
            map.retain(|_, t| now.duration_since(*t) < ttl);
        }
        if map.contains_key(&nonce) {
            return true;
        }
        map.insert(nonce, now);
        false
    }

    /// 获取指定 node_id 的连接锁（不存在则创建），用于串行化同节点的双向握手
    fn get_connecting_lock(&self, node_id: NodeId) -> Arc<TokioMutex<()>> {
        if let Some(lock) = self.connecting_locks.read().get(&node_id) {
            return lock.clone();
        }
        // 兜底清理：临时 NodeId（发现阶段随机生成）不会走 remove_connection
        self.prune_connecting_locks();
        self.connecting_locks
            .write()
            .entry(node_id)
            .or_insert_with(|| Arc::new(TokioMutex::new(())))
            .clone()
    }

    /// 注入发现服务（由 FederationService 调用）
    pub fn set_discovery(&self, discovery: Arc<crate::federation::discovery::DiscoveryService>) {
        let _ = self.discovery.set(discovery);
    }

    /// 注入同步管理器（由 FederationService 调用）
    pub fn set_sync_manager(&self, sync_manager: Arc<crate::federation::sync::SyncManager>) {
        let _ = self.sync_manager.set(sync_manager);
    }

    /// 注入打洞信令服务（由 FederationService 调用）
    pub fn set_signaling_service(&self, signaling: Arc<SignalingService>) {
        let _ = self.signaling_service.set(signaling);
    }

    /// 注入中继管理器（由 FederationService 调用）
    pub fn set_relay_manager(&self, relay: Arc<crate::federation::relay::RelayManager>) {
        let _ = self.relay_manager.set(relay);
    }

    /// 注入 pnos-net 外部互联 Agent（由 FederationService 调用）
    ///
    /// 注入后，connect_to 会优先使用 NetAgent 的连接策略引擎
    /// （TCP直连 → UDP打洞 → 中继），而非原始 TcpTransport::connect。
    pub fn set_net_agent(&self, agent: Arc<pnos_net::NetAgent>) {
        let _ = self.net_agent.set(agent);
    }

    /// 握手后向对端发送 PeerInfo（本地各 repo 条目数），供对端在全量同步数据源选择时
    /// 判断哪个节点数据最完整。轻量消息，不等待响应，发送失败仅记录 debug 日志。
    async fn send_peer_info(&self, connection: &Arc<Connection>) {
        if let Some(sync_mgr) = self.sync_manager.get() {
            let counts = sync_mgr.local_entry_counts();
            let msg = PeerInfoMessage {
                local_entry_counts: counts,
            };
            if let Err(e) = connection.send_message(MessageType::PeerInfo, &msg).await {
                debug!(
                    "[federation] PeerInfo 发送到 {} 失败: {}",
                    connection.node_id, e
                );
            }
        }
    }

    /// 启动 TCP 监听
    pub async fn start_listen(self: Arc<Self>) -> anyhow::Result<()> {
        let listen_addr = SocketAddr::new("0.0.0.0".parse().unwrap(), self.config.listen_port);
        let listener = TcpTransport::bind(listen_addr).await?;
        info!("[federation] 联邦监听已启动: {}", listen_addr);

        let shutdown_rx = self.shutdown.subscribe();
        info!("[federation] 准备 spawn accept_loop task");
        tokio::spawn(async move {
            info!("[federation] accept_loop task 已启动");
            tokio::select! {
                _ = self.accept_loop(listener) => {
                    warn!("[federation] accept_loop 意外退出");
                }
                _ = Self::wait_shutdown(shutdown_rx) => {
                    info!("[federation] 监听任务收到关闭信号");
                }
            }
            info!("[federation] accept_loop task 已结束");
        });
        info!("[federation] start_listen 返回");
        Ok(())
    }

    /// 接受连接循环
    async fn accept_loop(self: Arc<Self>, listener: tokio::net::TcpListener) {
        info!("[federation] accept_loop 开始运行，等待连接...");
        loop {
            debug!("[federation] 调用 listener.accept()");
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("[federation] 收到入站连接: {}", addr);
                    let _ = stream.set_nodelay(true);
                    let transport_stream: Box<dyn TransportStream> =
                        Box::new(TcpTransportStream::new(stream, TransportKind::Tcp));
                    let self_clone = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = self_clone.handle_inbound(transport_stream, addr).await {
                            debug!("[federation] 入站连接处理失败 {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    warn!("[federation] 接受连接失败: {}", e);
                    // [ALLOWED-SLEEP] 接受连接失败后一次性退避等待，非周期性
                    tokio::time::sleep(ACCEPT_FAILURE_BACKOFF).await;
                }
            }
        }
    }

    /// 处理入站连接
    async fn handle_inbound(
        self: Arc<Self>,
        stream: Box<dyn TransportStream>,
        addr: SocketAddr,
    ) -> anyhow::Result<()> {
        let transport_write_timeout = Duration::from_secs(self.config.transport_write_timeout_secs);
        let transport = TcpTransport::new(stream)
            .with_metrics(self.metrics.clone())
            .with_write_timeout(transport_write_timeout)
            .with_retry_config(
                self.config.transport_write_max_retries,
                self.config.transport_write_retry_base_ms,
            );

        // 入站握手（限时：未通过签名握手的连接不得长期占用，防慢速连接 DoS）
        let (node_id, hello) =
            tokio::time::timeout(HANDSHAKE_TIMEOUT, self.handshake_inbound(&transport))
                .await
                .map_err(|_| anyhow::anyhow!("入站握手超时（peer={}）", addr))??;

        // per-node 连接锁：与出站方向串行化，消除双向同时握手的重复连接竞态
        let connection = {
            let lock = self.get_connecting_lock(node_id);
            let _guard = lock.lock().await;

            // 获取锁后再次检查，避免竞态窗口内重复创建
            // node_id 仲裁：本节点 node_id 小的保留出站连接，大的保留入站连接
            // 这样两边用同一规则，不会同时 drop 导致重连循环
            if self.connections.read().contains_key(&node_id) {
                let local_id = self.identity.node_id;
                if local_id.0 < node_id.0 {
                    // 本节点 node_id 更小：保留已有的出站连接，关闭入站
                    debug!(
                        "[federation] 双向连接仲裁：本节点 {} < 对端 {}，保留出站，关闭入站",
                        local_id, node_id
                    );
                    return Ok(());
                } else {
                    // 本节点 node_id 更大：保留入站，关闭已有的出站连接
                    info!(
                        "[federation] 双向连接仲裁：本节点 {} > 对端 {}，关闭出站，保留入站，加入禁止出站",
                        local_id, node_id
                    );
                    self.connections.write().remove(&node_id);
                    // 记录禁止出站：以后不主动连这个节点（我是大的，应该等入站）
                    self.no_outbound.write().insert(node_id);
                }
            }

            // 检查连接数上限
            if self.connection_count() >= self.config.max_connections {
                debug!("[federation] 连接数已达上限，拒绝来自 {} 的连接", addr);
                return Ok(());
            }

            let connection = Arc::new(Connection::new(transport, node_id, addr));
            // 记录对端协议版本（决定是否启用 DiffSync key 交换新协议）
            connection.set_peer_protocol_version(hello.version);
            self.register_connection(connection.clone());
            connection
        };

        info!("[federation] 入站连接建立: {} ({})", node_id, addr);

        // 入站连接也需要更新 node_table 状态，否则连接维护任务会认为该节点未连接而反复重连
        self.node_table.mark_connected(&node_id, None);

        // 连接建立后不再自动触发全量同步。
        // 全量同步由 Merkle 反熵驱动：差异分片≥20%时即时触发差异全量。
        info!("[federation] 入站连接就绪: {}", node_id);

        // 启动消息处理循环
        self.clone().spawn_message_handler(connection.clone()).await;

        // 握手后立即发送 PeerInfo（携带本地条目数），供对端在数据源选择时判断数据完整度
        self.send_peer_info(&connection).await;

        Ok(())
    }

    /// 判断目标地址是否就是本节点自己（出站前置过滤）
    ///
    /// 三条判定路径：
    /// 1. `identity.addresses` 中登记的地址 —— 含 NAT/UPnP 映射后的公网地址
    ///    （端口为外部端口，可能与 `listen_port` 不同）。这是「自身公网地址被回传
    ///    后又连自己」这类场景的主判定依据；
    /// 2. 回环地址 + 联邦监听端口；
    /// 3. 本机网卡地址 + 联邦监听端口（NAT 探测尚未完成时的兜底）。
    fn is_self_address(&self, addr: SocketAddr) -> bool {
        // 1) 身份地址集合：ip + port 整体比对
        if self
            .identity
            .addresses_snapshot()
            .iter()
            .any(|a| a.ipv4_addr == Some(addr) || a.ipv6_addr == Some(addr))
        {
            return true;
        }
        // 2) 回环地址 + 联邦监听端口
        if addr.ip().is_loopback() && addr.port() == self.config.listen_port {
            return true;
        }
        // 3) 本机网卡地址 + 联邦监听端口（兜底）
        if addr.port() == self.config.listen_port && Self::is_local_interface_ip(addr.ip()) {
            return true;
        }
        false
    }

    /// 判断 IP 是否属于本机网卡（不含 NAT 公网映射）
    ///
    /// 用「bind 后 connect 再读 local_addr」的路由表探测：不发出任何报文，
    /// 若目标 IP 属于本机，内核选中的源地址就等于目标地址。
    fn is_local_interface_ip(ip: IpAddr) -> bool {
        let bind_addr: SocketAddr = if ip.is_ipv4() {
            match "0.0.0.0:0".parse() {
                Ok(a) => a,
                Err(_) => return false,
            }
        } else {
            match "[::]:0".parse() {
                Ok(a) => a,
                Err(_) => return false,
            }
        };
        let sock = match std::net::UdpSocket::bind(bind_addr) {
            Ok(s) => s,
            Err(_) => return false,
        };
        // 端口取 1（discard），仅用于触发内核路由选择
        match sock.connect(SocketAddr::new(ip, 1)) {
            Ok(()) => sock.local_addr().map(|a| a.ip() == ip).unwrap_or(false),
            Err(_) => false,
        }
    }

    /// 主动连接到节点
    pub async fn connect_to(
        self: Arc<Self>,
        node_id: NodeId,
        addr: SocketAddr,
    ) -> anyhow::Result<Arc<Connection>> {
        // 出站自连接前置过滤：目标是本机地址时直接放弃，不建 TCP、不握手。
        //
        // 背景：seed / peer_cache / 节点发现里可能存有本节点经 NAT 映射后的公网地址。
        // 这类入口（connect_seed / connect_cached_node）因为 node_id 未知，用随机
        // temp_id 发起，握手前的 node_id 自连接判定对它们无效 —— 连出去经 NAT hairpin
        // 回流到自己，要等握手才发现并拒绝；而随机 temp_id 每次都不同，不会进入按
        // node_id 记录的重连冷却，于是形成「连接→拒绝→重连」的无限循环。
        if self.is_self_address(addr) {
            let first_time = self.self_addr_warned.write().insert(addr);
            if first_time {
                warn!(
                    "[federation] 拒绝自连接（出站前置过滤）: {} 是本机地址，后续对该地址的连接将静默跳过",
                    addr
                );
            } else {
                debug!("[federation] 跳过自连接地址 {}", addr);
            }
            anyhow::bail!("拒绝自连接（目标为本机地址）: {}", addr);
        }

        // 检查是否已连接（用实际节点ID）
        if let Some(conn) = self.get_connection(&node_id) {
            return Ok(conn);
        }

        // 检查是否在禁止出站列表（NodeId 仲裁后，大 node_id 不主动出站）
        if self.no_outbound.read().contains(&node_id) {
            debug!(
                "[federation] 节点 {} 在禁止出站列表（仲裁后本节点更大），跳过出站",
                node_id
            );
            anyhow::bail!("节点 {} 在禁止出站列表，不主动出站", node_id);
        }

        // per-node 连接锁：持有至连接建立/失败，串行化出站与入站方向的握手
        let lock = self.get_connecting_lock(node_id);
        let _guard = lock.lock().await;

        // 获取锁后再次检查，消除检查与插入之间的竞态窗口
        if let Some(conn) = self.get_connection(&node_id) {
            return Ok(conn);
        }

        // 检查地址级重连冷却（比 node_id 冷却更早，因为重连用 temp_id 还不知道 node_id）
        let now = Instant::now();
        if let Some(until) = self.cooldown_addr.read().get(&addr) {
            if now < *until {
                let remaining = *until - now;
                debug!(
                    "[federation] 地址 {} 在重连冷却期内（剩余 {:?}），跳过连接",
                    addr, remaining
                );
                anyhow::bail!("地址 {} 在重连冷却期内", addr);
            }
        }

        // 检查 node_id 级重连冷却期：断开后 N 秒内不主动重连同一节点
        if let Some(until) = self.cooldown_until.read().get(&node_id) {
            if now < *until {
                let remaining = *until - now;
                debug!(
                    "[federation] 节点 {} 在重连冷却期内（剩余 {:?}），跳过连接",
                    node_id, remaining
                );
                anyhow::bail!("节点 {} 在重连冷却期内（剩余 {:?}）", node_id, remaining);
            }
        }

        // 检查是否正在连接中（用地址，防止并发重复连接）
        if !self.connecting.write().insert(addr) {
            anyhow::bail!("地址 {} 正在连接中，跳过重复连接", addr);
        }

        // RAII：无论成功/失败/任务被取消，退出时都移除 connecting 标记，
        // 避免 future 被 drop 后地址永久停留在"正在连接中"而无法重连。
        struct ConnectingGuard {
            mgr: Arc<ConnectionManager>,
            addr: SocketAddr,
        }
        impl Drop for ConnectingGuard {
            fn drop(&mut self) {
                self.mgr.connecting.write().remove(&self.addr);
            }
        }
        let _connecting_guard = ConnectingGuard {
            mgr: self.clone(),
            addr,
        };

        // 检查连接数上限
        if self.connection_count() >= self.config.max_connections {
            self.connecting.write().remove(&addr);
            anyhow::bail!("连接数已达上限");
        }

        self.node_table.mark_connecting(&node_id);

        // 建立 TCP 连接：优先使用 NetAgent（直连→打洞→中继），回退到原始连接
        let transport = if let Some(agent) = self.net_agent.get() {
            let net_node_id = pnos_net::types::NodeId(node_id.0);
            let (reachability, nat_type) = self
                .node_table
                .get(&node_id)
                .map(|e| {
                    let r = match e.info.reachability {
                        crate::federation::node_id::Reachability::PublicIpv6 => {
                            pnos_net::types::Reachability::PublicIpv6
                        }
                        crate::federation::node_id::Reachability::Mapped => {
                            pnos_net::types::Reachability::Mapped
                        }
                        crate::federation::node_id::Reachability::HolePunchable => {
                            pnos_net::types::Reachability::HolePunchable
                        }
                        crate::federation::node_id::Reachability::OutboundOnly => {
                            pnos_net::types::Reachability::OutboundOnly
                        }
                        crate::federation::node_id::Reachability::Unknown => {
                            pnos_net::types::Reachability::Unknown
                        }
                    };
                    (r, e.info.nat_type.clone())
                })
                .unwrap_or((pnos_net::types::Reachability::Unknown, None));

            match agent
                .connect_to(net_node_id, &[addr], reachability, nat_type)
                .await
            {
                Ok(result) => TcpTransport::new(result.connection)
                    .with_metrics(self.metrics.clone())
                    .with_write_timeout(Duration::from_secs(
                        self.config.transport_write_timeout_secs,
                    ))
                    .with_retry_config(
                        self.config.transport_write_max_retries,
                        self.config.transport_write_retry_base_ms,
                    ),
                Err(e) => {
                    self.connecting.write().remove(&addr);
                    self.node_table.mark_failed(&node_id);
                    return Err(e);
                }
            }
        } else {
            match TcpTransport::connect(addr).await {
                Ok(t) => t
                    .with_metrics(self.metrics.clone())
                    .with_write_timeout(Duration::from_secs(
                        self.config.transport_write_timeout_secs,
                    ))
                    .with_retry_config(
                        self.config.transport_write_max_retries,
                        self.config.transport_write_retry_base_ms,
                    ),
                Err(e) => {
                    self.connecting.write().remove(&addr);
                    self.node_table.mark_failed(&node_id);
                    return Err(e);
                }
            }
        };

        // 出站握手
        let transport = transport;
        // 出站握手（限时，避免对端不回 HelloAck 时任务长期挂起）
        let handshake_result = tokio::time::timeout(
            HANDSHAKE_TIMEOUT,
            self.handshake_outbound(&transport, node_id),
        )
        .await;
        let (peer_id, peer_version) = match handshake_result {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                self.connecting.write().remove(&addr);
                return Err(e);
            }
            Err(_) => {
                self.connecting.write().remove(&addr);
                anyhow::bail!("出站握手超时（peer={}）", addr);
            }
        };

        // 握手成功后再次检查是否已连接（可能并发建立了连接）
        if let Some(conn) = self.get_connection(&peer_id) {
            self.connecting.write().remove(&addr);
            info!("[federation] 连接 {} 已存在，复用已有连接", peer_id);
            return Ok(conn);
        }

        let connection = Arc::new(Connection::new(transport, peer_id, addr));
        connection.set_peer_protocol_version(peer_version);
        self.register_connection(connection.clone());

        self.node_table.mark_connected(&peer_id, None);
        info!("[federation] 出站连接建立: {} ({})", peer_id, addr);

        // 启动消息处理循环
        self.clone().spawn_message_handler(connection.clone()).await;

        // 握手后立即发送 PeerInfo（携带本地条目数），供对端在数据源选择时判断数据完整度
        self.send_peer_info(&connection).await;

        // 连接建立后不再自动触发全量同步。
        // 全量同步由 Merkle 反熵驱动：差异分片≥20%时即时触发差异全量。

        // 连接建立成功，移除 connecting 标记
        self.connecting.write().remove(&addr);

        // 出站直连建立后，若启用中继且开启自动协商，则在该控制连接上发起中继通道。
        // 注意：联邦中继是建立在“已有直连”之上的数据隧道，不能在打洞失败（无直连）时
        // 作为 NAT 回退；此处仅在出站连接成功后触发，此时 get_connection 必然命中。
        if self.config.enable_relay && self.config.relay_auto_setup_on_connect {
            if let Some(relay) = self.relay_manager.get() {
                match relay.setup_relay(peer_id) {
                    Ok(channel_id) => {
                        info!(
                            "[federation] 出站连接后自动建立中继 channel={}, peer={}",
                            channel_id, peer_id
                        );
                    }
                    Err(e) => {
                        debug!(
                            "[federation] 出站连接后自动建立中继失败 peer={}: {}",
                            peer_id, e
                        );
                    }
                }
            }
        }

        Ok(connection)
    }

    /// 出站握手：发 Hello -> 收 HelloAck（阶段2：Ed25519 签名验证）
    ///
    /// 返回 `(对端 NodeId, 对端协议版本)`。
    async fn handshake_outbound(
        &self,
        transport: &TcpTransport,
        _expected_node_id: NodeId,
    ) -> anyhow::Result<(NodeId, u32)> {
        let hello = HelloMessage::sign_and_build(
            &self.identity,
            self.identity.addresses_snapshot(),
            HELLO_PROTOCOL_VERSION,
            false,
        );
        transport.send_message(MessageType::Hello, &hello).await?;

        let (msg_type, payload) = transport.recv_message().await?;
        if msg_type != MessageType::HelloAck {
            anyhow::bail!("期望 HelloAck，收到 {:?}", msg_type);
        }
        let ack: HelloMessage = bincode::deserialize(&payload)
            .map_err(|e| anyhow::anyhow!("HelloAck 反序列化失败: {}", e))?;

        // 验证签名
        if !ack.verify_signature() {
            self.metrics.record_signature_failure();
            anyhow::bail!("HelloAck 签名验证失败，节点 {}", NodeId(ack.node_id));
        }

        // 身份绑定 + 时间戳新鲜度
        if !ack.verify_identity_binding() {
            self.metrics.record_signature_failure();
            anyhow::bail!("HelloAck 身份绑定校验失败：node_id 与公钥不匹配");
        }
        if !ack.is_fresh() {
            anyhow::bail!("HelloAck 时间戳超出重放窗口（{} ms）", ack.timestamp_ms);
        }

        let peer_id = NodeId(ack.node_id);
        let peer_version = ack.version;
        // 自连接过滤：不允许连接自己（通过 PEX/DHT 发现到自身地址后误连）
        if peer_id == self.identity.node_id {
            warn!(
                "[federation] 检测到自连接（出站），已拒绝: node_id={}",
                peer_id
            );
            anyhow::bail!("拒绝自连接: {}", peer_id);
        }
        debug!(
            "[federation] 握手成功: 本地 {} <-> 远端 {} (proto={})",
            self.identity.node_id, peer_id, peer_version
        );
        Ok((peer_id, peer_version))
    }

    /// 入站握手：收 Hello -> 发 HelloAck（阶段2：Ed25519 签名验证）
    async fn handshake_inbound(
        &self,
        transport: &TcpTransport,
    ) -> anyhow::Result<(NodeId, HelloMessage)> {
        let (msg_type, payload) = transport.recv_message().await?;
        if msg_type != MessageType::Hello {
            anyhow::bail!("期望 Hello，收到 {:?}", msg_type);
        }
        let hello: HelloMessage = bincode::deserialize(&payload)
            .map_err(|e| anyhow::anyhow!("Hello 反序列化失败: {}", e))?;

        // 验证签名
        if !hello.verify_signature() {
            self.metrics.record_signature_failure();
            anyhow::bail!("Hello 签名验证失败，节点 {}", NodeId(hello.node_id));
        }

        // 身份绑定：node_id 必须由 public_key 派生，防止用自建密钥冒充任意 node_id
        if !hello.verify_identity_binding() {
            self.metrics.record_signature_failure();
            anyhow::bail!("Hello 身份绑定校验失败：node_id 与公钥不匹配");
        }

        // 时间戳新鲜度：超出重放窗口的 Hello 一律拒绝
        if !hello.is_fresh() {
            anyhow::bail!("Hello 时间戳超出重放窗口（{} ms）", hello.timestamp_ms);
        }

        // nonce 去重：窗口内重复的 nonce 视为重放
        if self.is_replayed_nonce(hello.nonce) {
            anyhow::bail!("Hello nonce 重放，已拒绝");
        }

        let peer_id = NodeId(hello.node_id);

        // 自连接过滤：不允许连接自己（入站方向）
        if peer_id == self.identity.node_id {
            warn!(
                "[federation] 检测到自连接（入站），已拒绝: node_id={}",
                peer_id
            );
            anyhow::bail!("拒绝自连接: {}", peer_id);
        }

        // 回复 HelloAck（签名），携带本节点协议版本
        let ack = HelloMessage::sign_and_build(
            &self.identity,
            self.identity.addresses_snapshot(),
            HELLO_PROTOCOL_VERSION,
            false,
        );
        transport.send_message(MessageType::HelloAck, &ack).await?;

        // 将对端地址信息加入节点表（限制单次 Hello 携带的地址数量，防止节点表投毒）
        const MAX_HELLO_ADDRESSES: usize = 64;
        let total_addrs = hello.addresses.len();
        let mut adopted = 0usize;
        for addr_info in hello.addresses.iter().take(MAX_HELLO_ADDRESSES) {
            // 过滤无效地址（无可用地址或端口为 0）
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

        Ok((peer_id, hello))
    }

    /// 注册连接到连接池
    fn register_connection(&self, connection: Arc<Connection>) {
        self.metrics.record_connection_established();
        self.connections
            .write()
            .insert(connection.node_id, connection.clone());

        // 记录种子节点映射：若连接对端 IP 与某个配置种子节点 IP 一致，
        // 则记录 seed_addr -> real_node_id，供连接维护任务用 node_id 判断是否已连接
        // （入站连接对端端口为临时端口，地址精确匹配永远失败）
        let conn_ip = connection.addr.ip();
        for seed in &self.config.seed_nodes {
            if let Ok(seed_addr) = seed.parse::<SocketAddr>() {
                if seed_addr.ip() == conn_ip {
                    self.seed_node_ids
                        .write()
                        .insert(seed_addr, connection.node_id);
                    debug!(
                        "[federation] 记录种子节点映射: {} -> {}",
                        seed_addr, connection.node_id
                    );
                }
            }
        }
    }

    /// 移除连接（主动关闭 TCP socket，并记录重连冷却时间）
    pub fn remove_connection(&self, node_id: &NodeId) {
        // 记录重连冷却到期时间，防止立即重连形成循环
        let cooldown = Duration::from_secs(self.config.reconnect_cooldown_secs);
        let now = Instant::now();
        self.cooldown_until.write().insert(*node_id, now + cooldown);

        // 同时记录地址级冷却（从连接里拿 addr）
        if let Some(conn) = self.connections.read().get(node_id) {
            if let Ok(addr) = conn.transport.peer_addr() {
                self.cooldown_addr.write().insert(addr, now + cooldown);
            }
        }

        // 清理该节点的连接锁，防止 connecting_locks 无界增长
        self.connecting_locks.write().remove(node_id);
        // 顺带清理已过期的冷却项
        self.prune_cooldowns();

        let conn = self.connections.write().remove(node_id);
        if let Some(conn) = conn {
            self.metrics.record_connection_closed();
            // 主动关闭 TCP 传输，解除消息处理任务在 recv_message() 上的阻塞
            // 避免半开连接导致 task 泄漏
            let conn_clone = conn.clone();
            tokio::spawn(async move {
                let _ = conn_clone.transport.close().await;
            });
        }
        self.node_table.mark_disconnected(node_id);
    }

    /// 清理已过期的重连冷却项
    fn prune_cooldowns(&self) {
        let now = Instant::now();
        self.cooldown_until.write().retain(|_, until| now < *until);
        self.cooldown_addr.write().retain(|_, until| now < *until);
    }

    /// 清理未被持有的连接锁（仅 map 持有强引用者），防止 connecting_locks 无界增长。
    /// 临时 NodeId（发现阶段随机生成）不会走 remove_connection，故需兜底清理。
    fn prune_connecting_locks(&self) {
        let mut locks = self.connecting_locks.write();
        if locks.len() > 4096 {
            locks.retain(|_, lock| Arc::strong_count(lock) > 1);
        }
    }

    /// 启动心跳后台任务（已迁移到 TaskScheduler，此方法保留兼容但不再被调用）
    pub fn spawn_heartbeat(self: Arc<Self>) {
        // 已迁移到 TaskScheduler
    }

    /// 心跳单次执行（由 TaskScheduler 调度）
    pub async fn heartbeat_tick(&self) {
        let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let conns: Vec<Arc<Connection>> = self.all_connections();
        let now = Instant::now();

        for conn in conns {
            // 检查超时
            if conn.idle_duration() > timeout {
                warn!("[federation] 节点 {} 心跳超时，断开连接", conn.node_id);
                self.remove_connection(&conn.node_id);
                continue;
            }

            // 发送 Ping
            let ping = PingMessage {
                timestamp: now.elapsed().as_millis() as u64,
            };
            if let Err(e) = conn.send_message(MessageType::Ping, &ping).await {
                debug!("[federation] 发送 Ping 到 {} 失败: {}", conn.node_id, e);
                self.remove_connection(&conn.node_id);
            }
        }
    }

    /// 遍历所有连接并 flush 各自 gossip buffer（由 TaskScheduler 定期调度）
    pub async fn flush_all_gossip_buffers(self: Arc<Self>) {
        let max_batches = self.config.gossip_flush_max_batches;
        for conn in self.all_connections() {
            self.clone().flush_gossip_buffer(conn, max_batches).await;
        }
    }

    /// 启动消息处理循环（每条连接一个任务）
    async fn spawn_message_handler(self: Arc<Self>, connection: Arc<Connection>) {
        let mut shutdown_rx = self.shutdown.subscribe();
        let conn_id = connection.node_id;
        let pending_threshold = self.config.receive_pending_threshold;

        // 接收和处理分离：接收循环只管收，丢到队列；后台 worker 线程慢慢处理
        // 这样收消息永远不阻塞，Ping 心跳能及时收到
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(MessageType, Vec<u8>)>(256);

        // 接收循环：只收消息，丢到队列
        let recv_self = self.clone();
        let recv_conn = connection.clone();
        let recv_shutdown = self.shutdown.subscribe();
        tokio::spawn(async move {
            let mut shutdown_rx = recv_shutdown;
            loop {
                tokio::select! {
                    result = recv_conn.recv_message() => {
                        match result {
                            Ok((msg_type, payload)) => {
                                recv_conn.pending.fetch_add(1, Ordering::Relaxed);
                                // 丢到处理队列，立即继续收
                                if tx.send((msg_type, payload)).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                debug!("[federation] 连接 {} 读取失败: {}", conn_id, e);
                                recv_self.remove_connection(&conn_id);
                                break;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });

        // 处理循环：从队列取消息，后台慢慢处理
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    maybe_msg = rx.recv() => {
                        match maybe_msg {
                            Some((msg_type, payload)) => {
                                let offloaded = self.clone().dispatch_message(connection.clone(), msg_type, payload).await;
                                if !offloaded {
                                    connection.pending.fetch_sub(1, Ordering::Relaxed);
                                }
                                // 背压告警
                                let pending = connection.pending.load(Ordering::Relaxed);
                                if pending > pending_threshold {
                                    warn!(
                                        "[federation] 连接 {} 待处理消息积压 {} 超过阈值 {}，接收端处理慢（背压）",
                                        conn_id, pending, pending_threshold
                                    );
                                }
                            }
                            None => break,
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] 消息处理任务 {} 收到关闭信号", conn_id);
                        break;
                    }
                }
            }
        });
    }

    /// 分发消息。
    /// 返回值：true 表示本次消息已被异步卸载（重量级处理在后台执行，pending 计数由
    /// 卸载任务负责递减）；false 表示同步处理已完成（调用方负责递减 pending 计数）。
    async fn dispatch_message(
        self: Arc<Self>,
        connection: Arc<Connection>,
        msg_type: MessageType,
        payload: Vec<u8>,
    ) -> bool {
        connection.touch();

        let offloaded = match msg_type {
            MessageType::Ping => {
                if let Ok(ping) = bincode::deserialize::<PingMessage>(&payload) {
                    let pong = PongMessage {
                        timestamp: ping.timestamp,
                        rtt_estimate_ms: 0,
                    };
                    let _ = connection.send_message(MessageType::Pong, &pong).await;
                }
                false
            }
            MessageType::Pong => {
                if let Ok(pong) = bincode::deserialize::<PongMessage>(&payload) {
                    self.node_table
                        .update_rtt(&connection.node_id, pong.rtt_estimate_ms);
                    debug!(
                        "[federation] 收到 Pong from {} (rtt={}ms)",
                        connection.node_id, pong.rtt_estimate_ms
                    );
                }
                false
            }
            MessageType::GetNodes => {
                if let Ok(req) = bincode::deserialize::<GetNodesMessage>(&payload) {
                    if let Some(discovery) = self.discovery.get() {
                        discovery.handle_get_nodes(&connection, req.count).await;
                    }
                }
                false
            }
            MessageType::Nodes => {
                if let Ok(msg) = bincode::deserialize::<NodesMessage>(&payload) {
                    if let Some(discovery) = self.discovery.get() {
                        discovery.handle_nodes_received(msg.nodes);
                    }
                }
                false
            }
            MessageType::ExchangeNodes => {
                if let Ok(msg) = bincode::deserialize::<ExchangeNodesMessage>(&payload) {
                    if let Some(discovery) = self.discovery.get() {
                        discovery.handle_exchange_nodes(msg.nodes);
                    }
                }
                false
            }
            MessageType::SyncBatch => {
                // TODO(P1): handle_sync_batch 涉及大量 DB 写入，后续批量写入优化时一并异步化。
                // 当前为旧 stage-1 协议，主路径已走 GossipBatch（已异步卸载）。
                if let Ok(msg) = bincode::deserialize::<SyncBatchMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        sync_mgr.handle_sync_batch(msg.repo_type, &msg.entries);
                    }
                }
                false
            }
            MessageType::GossipBatch => {
                self.metrics.record_message_recv();
                // P1: 接收端攒批缓冲 —— 不立即 spawn，而是 push 到 per-connection buffer，
                // 由后台 flush 任务统一处理。N 个 batch 只需要 1 次 permit + 1 次 spawn。
                // P0-2: flush 任务内不使用 spawn_blocking（纯内存操作）。
                if let Ok(batch) = bincode::deserialize::<GossipBatchMessage>(&payload) {
                    debug!(
                        "[federation][perf] 收到 GossipBatch: entries={}",
                        batch.entries.len()
                    );
                    {
                        let mut buf = connection.gossip_buffer.lock();
                        let diag_repo_type = batch.repo_type;
                        let diag_entries = batch.entries.len();
                        buf.push_back(batch);
                        debug!("[federation][DIAG] GossipBatch received: repo_type={}, entries={}, buffer_len={}", diag_repo_type, diag_entries, buf.len());
                    }
                    // 通知 flush 任务（达到 max_batches 时立即刷新，否则等定时 tick）
                    connection.gossip_flush_notify.notify_one();
                }
                true
            }
            MessageType::GossipBatchBulk => {
                // 批量合并帧：将多个 batch 拆出后逐个加入 per-connection 缓冲，
                // 复用现有攒批 flush 机制，无需额外处理逻辑。
                self.metrics.record_message_recv();
                if let Ok(bulk) = bincode::deserialize::<GossipBatchBulkMessage>(&payload) {
                    let count = bulk.batches.len();
                    // 计算所有 batch 的 entries 总数（在 move 进 buffer 之前）
                    let total_entries: usize = bulk.batches.iter().map(|b| b.entries.len()).sum();
                    debug!(
                        "[federation][perf] 收到 GossipBatchBulk: {} 个 batch, 总条目 {}",
                        count, total_entries
                    );
                    {
                        let mut buf = connection.gossip_buffer.lock();
                        for batch in bulk.batches {
                            buf.push_back(batch);
                        }
                        debug!("[federation][DIAG] GossipBatchBulk received: batches={}, buffer_len={}", count, buf.len());
                    }
                    // 主循环已对每条消息 +1，Bulk 包含 N 个 batch，需补 +(N-1) 使计数与
                    // flush_gossip_buffer 按 batch 数 -group_len 递减匹配，避免 pending 下溢为巨大值触发虚假背压。
                    if count > 1 {
                        connection
                            .pending
                            .fetch_add((count - 1) as u32, Ordering::Relaxed);
                    }
                    connection.gossip_flush_notify.notify_one();
                }
                true
            }
            MessageType::MerkleDigest => {
                self.metrics.record_message_recv();
                if let Ok(digest) = bincode::deserialize::<MerkleDigestMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        let sync_mgr = sync_mgr.clone();
                        let conn = connection.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_merkle_digest(&conn, digest).await;
                        });
                    }
                }
                false
            }
            MessageType::MerkleRequest => {
                self.metrics.record_message_recv();
                if let Ok(request) = bincode::deserialize::<MerkleRequestMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        let sync_mgr = sync_mgr.clone();
                        let conn = connection.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_merkle_request(&conn, request).await;
                        });
                    }
                }
                false
            }
            MessageType::MerkleRepair => {
                self.metrics.record_message_recv();
                // P0-2: MerkleRepair 也是纯内存 + 批量 SQLite 写入，走 async handler（去掉 spawn_blocking 开销）
                if let Some(sync_mgr) = self.sync_manager.get() {
                    let sync_mgr = sync_mgr.clone();
                    let conn = connection.clone();
                    self.spawn_async_handler(conn, move || async move {
                        if let Ok(repair) = bincode::deserialize::<MerkleRepairMessage>(&payload) {
                            sync_mgr.handle_merkle_repair(repair);
                        }
                    });
                    true
                } else {
                    false
                }
            }
            MessageType::FullSyncStart => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<FullSyncStartMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        info!(
                            "[federation] 收到 FullSyncStart: repo_type={}, total={}, from={}",
                            msg.repo_type, msg.total_entries, connection.node_id
                        );
                        sync_mgr.handle_full_sync_start(msg.repo_type);
                    }
                }
                false
            }
            MessageType::FullSyncBatch => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<FullSyncBatchMessage>(&payload) {
                    let repo_type = msg.repo_type;
                    let seq = msg.seq;
                    let count = msg.entries.len();
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        // 直接应用到 repo（不经过 Gossip 去重）
                        sync_mgr.handle_full_sync_batch(repo_type, &msg.entries);
                        // 更新差量同步进度（防止无进度超时）
                        sync_mgr.update_diff_progress();
                        // 回复 Ack
                        let ack = FullSyncAckMessage { repo_type, seq };
                        let _ = connection
                            .send_message(MessageType::FullSyncAck, &ack)
                            .await;
                        debug!(
                            "[federation] FullSyncBatch 应用: repo_type={}, seq={}, entries={}",
                            repo_type, seq, count
                        );
                    }
                }
                false
            }
            MessageType::FullSyncAck => {
                // 发送端接收 Ack，简化版不做流控等待
                false
            }
            MessageType::DiffSyncRequest => {
                // 差量同步：对端携带 Merkle 摘要，本节点对比后只推送差异分片数据。
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    let sync_mgr = sync_mgr.clone();
                    let conn = connection.clone();
                    if let Ok(req) = bincode::deserialize::<DiffSyncRequestMessage>(&payload) {
                        tokio::spawn(async move {
                            sync_mgr.handle_diff_sync_request(conn.node_id, req).await;
                        });
                    }
                }
                false
            }
            MessageType::DiffSyncKeyRequest => {
                // P0-2: 数据服务器发来差异分片 key 列表分片，对比本地后回传缺失 key。
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    let sync_mgr = sync_mgr.clone();
                    let conn = connection.clone();
                    if let Ok(msg) = bincode::deserialize::<DiffSyncKeyRequestMessage>(&payload) {
                        tokio::spawn(async move {
                            sync_mgr.handle_diff_sync_key_request(&conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::DiffSyncKeyResponse => {
                // P0-2: 请求方回传缺失 key 列表分片，唤醒等待中的数据服务器推送任务。
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<DiffSyncKeyResponseMessage>(&payload) {
                        sync_mgr.handle_diff_sync_key_response(&connection, msg);
                    }
                }
                false
            }
            MessageType::PeerInfo => {
                // 握手后对端发来的节点信息（本地各 repo 条目数），记录到 peer_digests
                // 供全量同步数据源选择时判断数据完整度。
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<PeerInfoMessage>(&payload) {
                        sync_mgr.handle_peer_info(connection.node_id, msg.local_entry_counts);
                    }
                }
                false
            }
            MessageType::FullSyncComplete => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<FullSyncCompleteMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        info!(
                            "[federation] 收到 FullSyncComplete: repo_type={}, from={}",
                            msg.repo_type, connection.node_id
                        );
                        sync_mgr.handle_full_sync_complete(msg.repo_type);
                    }
                }
                false
            }
            MessageType::Signaling => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<SignalingMessage>(&payload) {
                    if let Some(signaling) = self.signaling_service.get() {
                        signaling.handle_signaling(connection.node_id, msg);
                    }
                }
                false
            }
            MessageType::RelaySetup => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<RelaySetupMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        sync_mgr.handle_relay_setup(connection.node_id, msg);
                    }
                }
                false
            }
            MessageType::RelayData => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<RelayDataMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        sync_mgr.handle_relay_data(connection.node_id, msg);
                    }
                }
                false
            }
            MessageType::Goodbye => {
                debug!("[federation] 收到 Goodbye from {}", connection.node_id);
                self.remove_connection(&connection.node_id);
                false
            }
            MessageType::PeerQueryRequest => {
                // 实时 peer 查询请求：从本地 PeerRepo 查询该 infohash 的 peer 并回复
                self.metrics.record_message_recv();
                if let Ok(req) = bincode::deserialize::<PeerQueryRequestMessage>(&payload) {
                    let peers: Vec<PeerQueryEntry> = if let Some(sync_mgr) = self.sync_manager.get()
                    {
                        sync_mgr.query_peers_for_infohash(&req.infohash, req.limit as usize)
                    } else {
                        Vec::new()
                    };
                    let resp = PeerQueryResponseMessage {
                        infohash: req.infohash,
                        peers,
                    };
                    if let Err(e) = connection
                        .send_message(MessageType::PeerQueryResponse, &resp)
                        .await
                    {
                        debug!(
                            "[federation] PeerQueryResponse 发送到 {} 失败: {}",
                            connection.node_id, e
                        );
                    }
                }
                false
            }
            MessageType::PeerQueryResponse => {
                // 实时 peer 查询响应：写入本地 PeerRepo，并推入响应收集器供 query_peers 取走
                self.metrics.record_message_recv();
                if let Ok(resp) = bincode::deserialize::<PeerQueryResponseMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        sync_mgr.add_remote_peers(&resp.infohash, &resp.peers);
                    }
                    if !resp.peers.is_empty() {
                        self.peer_query_responses
                            .entry(resp.infohash)
                            .or_default()
                            .extend(resp.peers);
                    }
                }
                false
            }
            MessageType::MerkleLevelRequest => {
                // 分层 Merkle 层级请求：返回指定层级的子哈希列表
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    let sync_mgr = sync_mgr.clone();
                    let conn = connection.clone();
                    if let Ok(req) = bincode::deserialize::<MerkleLevelRequestMessage>(&payload) {
                        tokio::spawn(async move {
                            sync_mgr.handle_merkle_level_request(conn, req).await;
                        });
                    }
                }
                false
            }
            MessageType::MerkleLevelResponse => {
                // 分层 Merkle 层级响应：继续逐层对比流程
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    let sync_mgr = sync_mgr.clone();
                    let conn = connection.clone();
                    if let Ok(resp) = bincode::deserialize::<MerkleLevelResponseMessage>(&payload) {
                        tokio::spawn(async move {
                            sync_mgr.handle_merkle_level_response(conn, resp).await;
                        });
                    }
                }
                false
            }
            MessageType::ShardSyncBatch => {
                // 分片同步批次：接收并应用条目，回复 Ack
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<ShardSyncBatchMessage>(&payload) {
                        let sync_mgr = sync_mgr.clone();
                        let conn = connection.clone();
                        sync_mgr.handle_shard_sync_batch(conn, msg);
                    }
                }
                false
            }
            MessageType::ShardSyncAck => {
                // 分片同步确认：唤醒发送端等待的 oneshot
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<ShardSyncAckMessage>(&payload) {
                        sync_mgr.handle_shard_sync_ack(msg);
                    }
                }
                false
            }
            MessageType::ShardSyncComplete => {
                // 分片同步完成：标记同步结束，触发 Merkle 重建
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<ShardSyncCompleteMessage>(&payload) {
                        info!(
                            "[federation] 收到 ShardSyncComplete: repo_type={}, from={}",
                            msg.repo_type, connection.node_id
                        );
                        sync_mgr.handle_shard_sync_complete(msg);
                    }
                }
                false
            }
            MessageType::ShardSyncHashList => {
                // 分片同步 hash 列表：对比本地 DB，回复缺失 key 列表（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<ShardSyncHashListMessage>(&payload) {
                        let sync_mgr = sync_mgr.clone();
                        let conn = connection.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_shard_sync_hash_list(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::ShardSyncMissing => {
                // 分片同步缺失 key 列表：唤醒发送端等待的 oneshot
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<ShardSyncMissingMessage>(&payload) {
                        sync_mgr.handle_shard_sync_missing(msg);
                    }
                }
                false
            }
            MessageType::OpsRequest => {
                // P1-3：增量拉取请求（数据服务器侧）—— 从本地 oplog 取 seq>since_seq 回 OpsBatch（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<OpsRequestMessage>(&payload) {
                        let sync_mgr = sync_mgr.clone();
                        let conn = connection.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_ops_request(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::OpsBatch => {
                // P1-3：增量拉取响应（请求方侧）—— 幂等应用 ops、推进版本向量、续拉下一批（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<OpsBatchMessage>(&payload) {
                        let sync_mgr = sync_mgr.clone();
                        let conn = connection.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_ops_batch(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::RangeReconcileRequest => {
                // P1-4：Range-based 反熵请求（应答方）—— 回该区间摘要 + 分界点/行指纹（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<RangeReconcileRequestMessage>(&payload)
                    {
                        let sync_mgr = sync_mgr.clone();
                        let conn = connection.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_range_reconcile_request(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::RangeReconcileResponse => {
                // P1-4：Range-based 反熵响应（请求方）—— 剪枝 / 求差 / 继续下钻（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<RangeReconcileResponseMessage>(&payload)
                    {
                        let sync_mgr = sync_mgr.clone();
                        let conn = connection.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_range_reconcile_response(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::RangeReconcilePush => {
                // P1-4：Range 反熵推送（接收方）—— 写入本地数据库
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<RangeReconcilePushMessage>(&payload) {
                        let sync_mgr = sync_mgr.clone();
                        let conn = connection.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_range_reconcile_push(conn, msg).await;
                        });
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            MessageType::BootstrapManifestRequest => {
                // P2-1：bootstrap 清单请求（应答方）—— 建 w0 水位 + 有序逻辑分块清单（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) =
                        bincode::deserialize::<BootstrapManifestRequestMessage>(&payload)
                    {
                        let sync_mgr = sync_mgr.clone();
                        let conn = connection.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_bootstrap_manifest_request(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::BootstrapManifestResponse => {
                // P2-1：bootstrap 清单响应（请求方）—— 保存进度并开始拉第一块（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) =
                        bincode::deserialize::<BootstrapManifestResponseMessage>(&payload)
                    {
                        let sync_mgr = sync_mgr.clone();
                        let conn = connection.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_bootstrap_manifest_response(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::BootstrapChunkRequest => {
                // P2-1：bootstrap 分块请求（应答方）—— 按区间取条目回发（受令牌桶限流，异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<BootstrapChunkRequestMessage>(&payload)
                    {
                        let sync_mgr = sync_mgr.clone();
                        let conn = connection.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_bootstrap_chunk_request(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::BootstrapChunkResponse => {
                // P2-1：bootstrap 分块响应（请求方）—— 批量 upsert 落块、校验、续拉/切追尾（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<BootstrapChunkResponseMessage>(&payload)
                    {
                        let sync_mgr = sync_mgr.clone();
                        let conn = connection.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_bootstrap_chunk_response(conn, msg).await;
                        });
                    }
                }
                false
            }
            _ => {
                debug!(
                    "[federation] 收到未处理消息类型 {:?} from {}",
                    msg_type, connection.node_id
                );
                false
            }
        };

        offloaded
    }

    /// P0-2: 异步执行纯内存消息处理（不经过 spawn_blocking）。
    ///
    /// GossipBatch 处理是纯内存操作（HashMap + Merkle），不需要阻塞线程池。
    /// 仍然通过 semaphore 限制并发，避免无界生成异步任务导致 CPU 飙升。
    fn spawn_async_handler<F, Fut>(&self, conn: Arc<Connection>, f: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let sem = self.heavy_task_semaphore.clone();
        tokio::spawn(async move {
            let _permit = match sem.acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    conn.pending.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
            };
            f().await;
            drop(_permit);
            conn.pending.fetch_sub(1, Ordering::Relaxed);
        });
    }

    /// P0-1: 刷新 per-connection GossipBatch 缓冲区（按 repo_type 并行分流）。
    ///
    /// 一次性 drain 所有缓冲的 GossipBatch，按 `repo_type` 分成最多 4 组
    /// （NODE/PEER/INFOHASH/TRACKER）。每个非空组独立 spawn 一个 tokio task，
    /// 各自获取 1 个 `heavy_task_semaphore` permit 后顺序处理本组 batch。
    /// 组间并行，组内顺序；每个 task 完成后按本组 batch 数递减 `conn.pending`。
    async fn flush_gossip_buffer(self: Arc<Self>, conn: Arc<Connection>, _max_batches: usize) {
        debug!(
            "[federation][DIAG] flush_gossip_buffer ENTER, buffer_len={}",
            conn.gossip_buffer.lock().len()
        );
        // 限频：每10次 flush tick 输出1次，用于确认 flush task 存活并观察 buffer 积压。
        // 放在 drain/early-return 之前，即使 buffer 为空也能看到 tick。
        static FLUSH_TICK_COUNT: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let tick = FLUSH_TICK_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if tick.is_multiple_of(10) {
            let buffer_len = conn.gossip_buffer.lock().len();
            debug!("[federation][perf] flush tick: buffer_len={}", buffer_len);
        }

        let total_start = Instant::now();

        // 一次性 drain 所有缓冲的 batch
        let drain_start = Instant::now();
        let batches: Vec<GossipBatchMessage> = {
            let mut buf = conn.gossip_buffer.lock();
            if buf.is_empty() {
                return;
            }
            buf.drain(..).collect()
        };
        let drain_elapsed = drain_start.elapsed();
        if batches.is_empty() {
            return;
        }

        // 优化2：接收端提前去重 —— 在按 repo_type 分组 / spawn task 前，
        // 一次性过滤掉已处理的重复 batch。重复 batch 此前仍消耗反序列化、缓冲、
        // 分组、task spawn、clone 的 CPU；提前丢弃可降低 90%+ 重复消息开销。
        // 只读检查（seen_msgs.contains），不插入；handle_gossip_batch 中的
        // check_and_put 仍保留作为兜底（防止本检查与实际处理间的竞态）。
        let batches: Vec<GossipBatchMessage> = if let Some(sync_mgr) = self.sync_manager.get() {
            let before = batches.len();
            let filtered: Vec<GossipBatchMessage> = batches
                .into_iter()
                .filter(|b| !sync_mgr.is_batch_seen(b))
                .collect();
            let dropped = before - filtered.len();
            if dropped > 0 {
                debug!(
                    "[federation][perf] flush 提前去重: 丢弃 {} 条重复 batch（剩余 {}）",
                    dropped,
                    filtered.len()
                );
            }
            filtered
        } else {
            batches
        };
        if batches.is_empty() {
            return;
        }

        let batch_count = batches.len();
        debug!("[federation][perf] flush drain: batches={}", batches.len());

        // 按 repo_type 分流到最多 4 组（使用 protocol::repo_type 常量，不硬编码数值）
        let mut node_group: Vec<GossipBatchMessage> = Vec::new();
        let mut peer_group: Vec<GossipBatchMessage> = Vec::new();
        let mut infohash_group: Vec<GossipBatchMessage> = Vec::new();
        let mut tracker_group: Vec<GossipBatchMessage> = Vec::new();
        for batch in batches {
            match batch.repo_type {
                repo_type::NODE => node_group.push(batch),
                repo_type::PEER => peer_group.push(batch),
                repo_type::INFOHASH => infohash_group.push(batch),
                repo_type::TRACKER => tracker_group.push(batch),
                _ => {
                    debug!(
                        "[federation] flush GossipBatch 遇到未知 repo_type={}, 丢弃",
                        batch.repo_type
                    );
                }
            }
        }
        let groups = [node_group, peer_group, infohash_group, tracker_group];
        let group_count = groups.iter().filter(|g| !g.is_empty()).count();

        // 每个非空组 spawn 独立 task，各自获取 1 个 permit（与 GossipBatch/MerkleRepair 共用）
        for group in groups {
            let group_len = group.len();
            if group_len == 0 {
                continue;
            }
            let sem = self.heavy_task_semaphore.clone();
            let conn_task = conn.clone();
            let self_task = self.clone();
            tokio::spawn(async move {
                // 等待 permit：信号量饱和时在此排队，对应 batch 计入 pending（背压可见）
                let _permit = match sem.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        conn_task
                            .pending
                            .fetch_sub(group_len as u32, Ordering::Relaxed);
                        return;
                    }
                };
                // 顺序处理本组 batch（纯内存操作，不经过 spawn_blocking）
                if let Some(sync_mgr) = self_task.sync_manager.get().cloned() {
                    for batch in group {
                        sync_mgr.handle_gossip_batch(batch);
                    }
                }
                drop(_permit);
                conn_task
                    .pending
                    .fetch_sub(group_len as u32, Ordering::Relaxed);
            });
        }

        let total_elapsed = total_start.elapsed();
        debug!(
            "[federation][perf] flush_gossip_buffer: batches={} groups={} drain={}ms total={}ms",
            batch_count,
            group_count,
            drain_elapsed.as_millis(),
            total_elapsed.as_millis()
        );
    }

    /// 获取指定节点的连接
    pub fn get_connection(&self, node_id: &NodeId) -> Option<Arc<Connection>> {
        self.connections.read().get(node_id).cloned()
    }

    /// 查询指定对端的 RTT（毫秒），未知则返回 u32::MAX（供数据源选择延迟打分）。
    pub fn peer_rtt_ms(&self, node_id: &NodeId) -> u32 {
        self.node_table
            .get(node_id)
            .and_then(|e| e.rtt_ms)
            .unwrap_or(u32::MAX)
    }

    /// 获取所有连接
    pub fn all_connections(&self) -> Vec<Arc<Connection>> {
        self.connections.read().values().cloned().collect()
    }

    /// 当前连接数
    pub fn connection_count(&self) -> usize {
        self.connections.read().len()
    }

    /// 联邦实时查询：清空指定 infohash 的旧响应（发起查询前调用）
    pub fn clear_peer_query_responses(&self, infohash: &[u8; 20]) {
        self.peer_query_responses.remove(infohash);
    }

    /// 联邦实时查询：取走并删除指定 infohash 的所有响应条目（超时后调用）
    pub fn take_peer_query_responses(&self, infohash: &[u8; 20]) -> Vec<PeerQueryEntry> {
        self.peer_query_responses
            .remove(infohash)
            .map(|(_, v)| v)
            .unwrap_or_default()
    }

    /// 无连接时自动重连：遍历种子节点与节点表中 Disconnected/Failed 且有地址的节点，逐个尝试 dial。
    /// 带冷却（reconnect_cooldown_secs），且仅在当前确实无连接时触发，避免与正常传播并发抢连接。
    pub async fn reconnect_discovered(self: Arc<Self>) {
        // 有连接时由正常传播兜底，不主动重连
        if self.connection_count() > 0 {
            return;
        }
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let last = self.last_reconnect_attempt.load(Ordering::Relaxed);
        let cooldown = self.config.reconnect_cooldown_secs;
        if now_secs.saturating_sub(last) < cooldown {
            return;
        }
        self.last_reconnect_attempt
            .store(now_secs, Ordering::Relaxed);

        let mut targets: Vec<(NodeId, SocketAddr)> = Vec::new();
        // 1) 种子节点已记录的真实 node_id + 地址（握手后获知，可信）
        for (addr, node_id) in self.seed_node_ids.read().iter() {
            if self.get_connection(node_id).is_none() {
                targets.push((*node_id, *addr));
            }
        }
        // 2) 节点表中 Disconnected/Failed 且有 preferred 地址的已知节点（最多再补到 8 个）
        for entry in self.node_table.all_nodes() {
            if targets.len() >= 8 {
                break;
            }
            if !matches!(entry.status, NodeStatus::Disconnected | NodeStatus::Failed) {
                continue;
            }
            if let Some(addr) = entry.info.preferred_addr() {
                let node_id = NodeId(entry.info.node_id);
                if self.get_connection(&node_id).is_none() {
                    targets.push((node_id, addr));
                }
            }
        }

        if targets.is_empty() {
            debug!("[federation] reconnect_discovered: 无候选节点可重连");
            return;
        }
        warn!("[federation] 无连接，尝试重连 {} 个候选节点", targets.len());

        for (node_id, addr) in targets {
            if self.connection_count() > 0 {
                break; // 已连上任意节点即停
            }
            if let Err(e) = self.clone().connect_to(node_id, addr).await {
                debug!("[federation] 重连节点 {}@{} 失败: {}", node_id, addr, e);
            }
        }
    }

    /// 检查指定种子节点是否已连接
    ///
    /// 优先使用握手后记录的真实 node_id 匹配（入站连接对端端口为临时端口，
    /// 地址精确匹配永远失败）；若映射尚未建立（首次连接前），则退回 IP 匹配。
    pub fn is_seed_connected(&self, seed_addr: SocketAddr) -> bool {
        // 1. 优先用已记录的真实 node_id 检查
        if let Some(&real_node_id) = self.seed_node_ids.read().get(&seed_addr) {
            if self.connections.read().contains_key(&real_node_id) {
                return true;
            }
        }

        // 2. 退回 IP 匹配（首次连接前或映射未建立时）
        let seed_ip = seed_addr.ip();
        self.connections
            .read()
            .values()
            .any(|c| c.addr.ip() == seed_ip)
    }

    /// 关闭所有连接
    pub async fn shutdown_all(&self) {
        let conns: Vec<Arc<Connection>> = self.all_connections();
        for conn in conns {
            let goodbye = PingMessage { timestamp: 0 };
            let _ = conn.send_message(MessageType::Goodbye, &goodbye).await;
        }
        self.connections.write().clear();
        info!("[federation] 所有连接已关闭");
    }

    /// 等待关闭信号
    async fn wait_shutdown(mut rx: broadcast::Receiver<()>) {
        let _ = rx.recv().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::node_id::NodeIdentity;

    fn make_test_config() -> FederationConfig {
        FederationConfig {
            enabled: true,
            listen_port: 0,
            max_connections: 10,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_connection_manager_creation() {
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let mgr = ConnectionManager::new(
            node_table,
            identity,
            make_test_config(),
            shutdown_tx,
            Arc::new(FederationMetrics::new()),
        );
        assert_eq!(mgr.connection_count(), 0);
        assert!(mgr.all_connections().is_empty());
    }

    #[test]
    fn test_is_self_address_filters_own_addresses() {
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut cfg = make_test_config();
        cfg.listen_port = 6885;
        let mgr = ConnectionManager::new(
            node_table,
            identity.clone(),
            cfg,
            shutdown_tx,
            Arc::new(FederationMetrics::new()),
        );

        // 1) 回环地址 + 联邦监听端口 → 自身
        assert!(mgr.is_self_address("127.0.0.1:6885".parse().unwrap()));
        // 2) 端口不同 → 不是自身（不误伤同机其它端口的服务）
        assert!(!mgr.is_self_address("127.0.0.1:9999".parse().unwrap()));
        // 3) 外部地址 → 不是自身
        assert!(!mgr.is_self_address("8.8.8.8:6885".parse().unwrap()));

        // 4) 模拟 NAT 映射后的自身公网地址（外部端口 == listen_port）
        let mapped: SocketAddr = "183.158.254.70:6885".parse().unwrap();
        identity.update_addresses(vec![crate::federation::node_id::NodeAddress {
            node_id: identity.node_id.0,
            ipv4_addr: Some(mapped),
            ipv6_addr: None,
            reachability: crate::federation::node_id::Reachability::Mapped,
            last_seen: 0,
            nat_type: None,
        }]);
        assert!(mgr.is_self_address(mapped));

        // 5) 外部端口与 listen_port 不同时，仍应通过身份地址集合命中
        let mapped2: SocketAddr = "183.158.254.70:46885".parse().unwrap();
        identity.update_addresses(vec![crate::federation::node_id::NodeAddress {
            node_id: identity.node_id.0,
            ipv4_addr: Some(mapped2),
            ipv6_addr: None,
            reachability: crate::federation::node_id::Reachability::Mapped,
            last_seen: 0,
            nat_type: None,
        }]);
        assert!(mgr.is_self_address(mapped2));
        // 已被覆盖掉的上一个地址不再算自身（地址集合是整体替换语义）
        assert!(!mgr.is_self_address(mapped));
    }

    #[tokio::test]
    async fn test_connection_send_recv() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let transport = TcpTransport::new(Box::new(TcpTransportStream::new(
                stream,
                TransportKind::Tcp,
            )));
            let conn = Connection::new(transport, NodeId([1; 20]), addr);
            let (msg_type, payload) = conn.recv_message().await.unwrap();
            assert_eq!(msg_type, MessageType::Ping);
            let ping: PingMessage = bincode::deserialize(&payload).unwrap();
            assert_eq!(ping.timestamp, 42);
            let pong = PongMessage {
                timestamp: 42,
                rtt_estimate_ms: 5,
            };
            conn.send_message(MessageType::Pong, &pong).await.unwrap();
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let transport = TcpTransport::new(Box::new(TcpTransportStream::new(
            stream,
            TransportKind::Tcp,
        )));
        let conn = Connection::new(transport, NodeId([2; 20]), addr);

        let ping = PingMessage { timestamp: 42 };
        conn.send_message(MessageType::Ping, &ping).await.unwrap();

        let (msg_type, payload) = conn.recv_message().await.unwrap();
        assert_eq!(msg_type, MessageType::Pong);
        let pong: PongMessage = bincode::deserialize(&payload).unwrap();
        assert_eq!(pong.timestamp, 42);
        assert_eq!(pong.rtt_estimate_ms, 5);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_connection_idle_tracking() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let _server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _transport = TcpTransport::new(Box::new(TcpTransportStream::new(
                stream,
                TransportKind::Tcp,
            )));
            // [ALLOWED-SLEEP] 测试代码中的一次性等待
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let transport = TcpTransport::new(Box::new(TcpTransportStream::new(
            stream,
            TransportKind::Tcp,
        )));
        let conn = Connection::new(transport, NodeId([1; 20]), addr);

        assert!(conn.idle_duration().as_millis() < 100);
        conn.touch();
        assert!(conn.idle_duration().as_millis() < 100);
    }

    #[tokio::test]
    async fn test_handshake_inbound_outbound() {
        let identity1 = Arc::new(NodeIdentity::generate());
        let identity2 = Arc::new(NodeIdentity::generate());
        let node_table1 = Arc::new(NodeTable::new(100));
        let node_table2 = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);

        let mgr1 = Arc::new(ConnectionManager::new(
            node_table1.clone(),
            identity1.clone(),
            make_test_config(),
            shutdown_tx.clone(),
            Arc::new(FederationMetrics::new()),
        ));
        let mgr2 = Arc::new(ConnectionManager::new(
            node_table2.clone(),
            identity2.clone(),
            make_test_config(),
            shutdown_tx,
            Arc::new(FederationMetrics::new()),
        ));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mgr2_clone = mgr2.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let transport = TcpTransport::new(Box::new(TcpTransportStream::new(
                stream,
                TransportKind::Tcp,
            )));
            let (peer_id, _) = mgr2_clone.handshake_inbound(&transport).await.unwrap();
            assert_eq!(peer_id, identity1.node_id);
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let transport = TcpTransport::new(Box::new(TcpTransportStream::new(
            stream,
            TransportKind::Tcp,
        )));
        let peer_id = mgr1
            .handshake_outbound(&transport, identity2.node_id)
            .await
            .unwrap();
        assert_eq!(peer_id.0, identity2.node_id);
        assert!(peer_id.1 >= 2);

        server.await.unwrap();
    }
}
