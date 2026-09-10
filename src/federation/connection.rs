//! 连接管理
//!
//! 管理联邦网络中的所有 TCP 连接，包括监听、主动连接、握手、心跳和消息分发。

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex as ParkingMutex, RwLock};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::{broadcast, Mutex as TokioMutex, Notify, OnceCell, Semaphore};
use tracing::{debug, info, warn};

use crate::federation::config::FederationConfig;
use crate::federation::node_id::{NodeAddress, NodeId};
use crate::federation::node_table::{NodeStatus, NodeTable};
use crate::federation::protocol::*;
use crate::federation::metrics::FederationMetrics;
use crate::federation::signaling::SignalingService;
use crate::federation::transport::TcpTransport;

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
        }
    }

    /// 发送消息
    pub async fn send_message<T: serde::Serialize>(
        &self,
        msg_type: MessageType,
        msg: &T,
    ) -> anyhow::Result<()> {
        self.transport.send_message(msg_type, msg).await?;
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
    /// 监控指标
    metrics: Arc<FederationMetrics>,
    /// 重连冷却到期时间（NodeId -> 冷却截止时刻），断开后在此时间内不主动重连
    cooldown_until: RwLock<FxHashMap<NodeId, Instant>>,
    /// 重量级消息处理的有界并发信号量（GossipBatch/MerkleRepair 经 spawn_blocking 执行，
    /// 先 acquire permit 再 spawn，避免无界生成阻塞任务导致内存暴涨）
    heavy_task_semaphore: Arc<Semaphore>,
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
            metrics,
            cooldown_until: RwLock::new(FxHashMap::default()),
            heavy_task_semaphore: Arc::new(Semaphore::new(
                config.heavy_task_max_concurrency.max(1),
            )),
        }
    }

    /// 获取指定 node_id 的连接锁（不存在则创建），用于串行化同节点的双向握手
    fn get_connecting_lock(&self, node_id: NodeId) -> Arc<TokioMutex<()>> {
        if let Some(lock) = self.connecting_locks.read().get(&node_id) {
            return lock.clone();
        }
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
                    let self_clone = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = self_clone.handle_inbound(stream, addr).await {
                            debug!("[federation] 入站连接处理失败 {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    warn!("[federation] 接受连接失败: {}", e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// 处理入站连接
    async fn handle_inbound(
        self: Arc<Self>,
        stream: tokio::net::TcpStream,
        addr: SocketAddr,
    ) -> anyhow::Result<()> {
        let _ = stream.set_nodelay(true);
        let transport_write_timeout = Duration::from_secs(self.config.transport_write_timeout_secs);
        let transport = TcpTransport::new(stream)
            .with_metrics(self.metrics.clone())
            .with_write_timeout(transport_write_timeout);

        // 入站握手
        let (node_id, _hello) = self.handshake_inbound(&transport).await?;

        // per-node 连接锁：与出站方向串行化，消除双向同时握手的重复连接竞态
        let connection = {
            let lock = self.get_connecting_lock(node_id);
            let _guard = lock.lock().await;

            // 获取锁后再次检查，避免竞态窗口内重复创建
            if self.connections.read().contains_key(&node_id) {
                debug!("[federation] 节点 {} 已存在连接，拒绝重复连接", node_id);
                return Ok(());
            }

            // 检查连接数上限
            if self.connection_count() >= self.config.max_connections {
                debug!("[federation] 连接数已达上限，拒绝来自 {} 的连接", addr);
                return Ok(());
            }

            let connection = Arc::new(Connection::new(transport, node_id, addr));
            self.register_connection(connection.clone());
            connection
        };

        info!("[federation] 入站连接建立: {} ({})", node_id, addr);

        // 入站连接也需要更新 node_table 状态，否则连接维护任务会认为该节点未连接而反复重连
        self.node_table.mark_connected(&node_id, None);

        // 入站连接建立后也触发初始全量同步（双向同步，确保双方历史数据都能同步）
        // 注意：必须在 spawn_message_handler 消费 self 之前获取 sync_manager
        let sync_mgr = self.sync_manager.get().cloned();
        info!(
            "[federation] 入站连接初始同步检查: sync_manager={}",
            if sync_mgr.is_some() { "Some" } else { "None" }
        );

        // 启动消息处理循环
        self.spawn_message_handler(connection).await;

        if let Some(mgr) = sync_mgr {
            info!("[federation] 入站连接触发初始全量同步");
            mgr.trigger_initial_sync(node_id);
        } else {
            warn!("[federation] 入站连接 sync_manager 为 None，无法触发初始全量同步");
        }

        Ok(())
    }

    /// 主动连接到节点
    pub async fn connect_to(
        self: Arc<Self>,
        node_id: NodeId,
        addr: SocketAddr,
    ) -> anyhow::Result<Arc<Connection>> {
        // 检查是否已连接（用实际节点ID）
        if let Some(conn) = self.get_connection(&node_id) {
            return Ok(conn);
        }

        // per-node 连接锁：持有至连接建立/失败，串行化出站与入站方向的握手
        let lock = self.get_connecting_lock(node_id);
        let _guard = lock.lock().await;

        // 获取锁后再次检查，消除检查与插入之间的竞态窗口
        if let Some(conn) = self.get_connection(&node_id) {
            return Ok(conn);
        }

        // 检查重连冷却期：断开后 N 秒内不主动重连同一节点
        let now = Instant::now();
        if let Some(until) = self.cooldown_until.read().get(&node_id) {
            if now < *until {
                let remaining = *until - now;
                debug!(
                    "[federation] 节点 {} 在重连冷却期内（剩余 {:?}），跳过连接",
                    node_id, remaining
                );
                anyhow::bail!(
                    "节点 {} 在重连冷却期内（剩余 {:?}）",
                    node_id,
                    remaining
                );
            }
        }

        // 检查是否正在连接中（用地址，防止并发重复连接）
        if !self.connecting.write().insert(addr) {
            anyhow::bail!("地址 {} 正在连接中，跳过重复连接", addr);
        }

        // 检查连接数上限
        if self.connection_count() >= self.config.max_connections {
            self.connecting.write().remove(&addr);
            anyhow::bail!("连接数已达上限");
        }

        self.node_table.mark_connecting(&node_id);

        let transport = match TcpTransport::connect(addr).await {
            Ok(t) => {
                t.with_metrics(self.metrics.clone())
                    .with_write_timeout(Duration::from_secs(self.config.transport_write_timeout_secs))
            }
            Err(e) => {
                self.connecting.write().remove(&addr);
                self.node_table.mark_failed(&node_id);
                return Err(e);
            }
        };

        // 出站握手
        let transport = transport;
        let peer_id = match self.handshake_outbound(&transport, node_id).await {
            Ok(id) => id,
            Err(e) => {
                self.connecting.write().remove(&addr);
                return Err(e);
            }
        };

        // 握手成功后再次检查是否已连接（可能并发建立了连接）
        if let Some(conn) = self.get_connection(&peer_id) {
            self.connecting.write().remove(&addr);
            info!("[federation] 连接 {} 已存在，复用已有连接", peer_id);
            return Ok(conn);
        }

        let connection = Arc::new(Connection::new(transport, peer_id, addr));
        self.register_connection(connection.clone());

        self.node_table.mark_connected(&peer_id, None);
        info!("[federation] 出站连接建立: {} ({})", peer_id, addr);

        // 启动消息处理循环
        self.clone().spawn_message_handler(connection.clone()).await;

        // 出站连接建立成功后触发初始全量同步（只触发一次，入站连接不触发）
        if let Some(sync_mgr) = self.sync_manager.get() {
            sync_mgr.clone().trigger_initial_sync(peer_id);
        }

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
                        debug!("[federation] 出站连接后自动建立中继失败 peer={}: {}", peer_id, e);
                    }
                }
            }
        }

        Ok(connection)
    }

    /// 出站握手：发 Hello -> 收 HelloAck（阶段2：Ed25519 签名验证）
    async fn handshake_outbound(
        &self,
        transport: &TcpTransport,
        expected_node_id: NodeId,
    ) -> anyhow::Result<NodeId> {
        let hello = HelloMessage::sign_and_build(
            &self.identity,
            self.identity.addresses_snapshot(),
            1,
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

        let peer_id = NodeId(ack.node_id);
        debug!(
            "[federation] 握手成功: 本地 {} <-> 远端 {}",
            self.identity.node_id, peer_id
        );
        Ok(peer_id)
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

        let peer_id = NodeId(hello.node_id);

        // 回复 HelloAck（签名）
        let ack = HelloMessage::sign_and_build(
            &self.identity,
            self.identity.addresses_snapshot(),
            1,
            false,
        );
        transport.send_message(MessageType::HelloAck, &ack).await?;

        // 将对端地址信息加入节点表
        for addr_info in &hello.addresses {
            self.node_table.add_or_update(addr_info.clone());
        }

        Ok((peer_id, hello))
    }

    /// 注册连接到连接池
    fn register_connection(&self, connection: Arc<Connection>) {
        self.metrics.record_connection_established();
        self.connections
            .write()
            .insert(connection.node_id, connection);
    }

    /// 移除连接（主动关闭 TCP socket，并记录重连冷却时间）
    pub fn remove_connection(&self, node_id: &NodeId) {
        // 记录重连冷却到期时间，防止立即重连形成循环
        let cooldown = Duration::from_secs(self.config.reconnect_cooldown_secs);
        self.cooldown_until.write().insert(*node_id, Instant::now() + cooldown);

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

    /// 启动心跳后台任务
    pub fn spawn_heartbeat(self: Arc<Self>) {
        let interval = Duration::from_secs(self.config.heartbeat_interval_secs);
        let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self.heartbeat_tick(timeout).await;
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] 心跳任务收到关闭信号");
                        break;
                    }
                }
            }
        });
        info!("[federation] 心跳任务已启动（间隔 {}s，超时 {}s）", interval.as_secs(), timeout.as_secs());
    }

    /// 心跳单次执行
    async fn heartbeat_tick(&self, timeout: Duration) {
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

    /// 启动消息处理循环（每条连接一个任务）
    async fn spawn_message_handler(self: Arc<Self>, connection: Arc<Connection>) {
        let mut shutdown_rx = self.shutdown.subscribe();
        let conn_id = connection.node_id;

        // P1: 启动 per-connection GossipBatch 攒批 flush 任务
        let flush_interval_ms = self.config.gossip_flush_interval_ms;
        let max_batches = self.config.gossip_flush_max_batches;
        let conn_flush = connection.clone();
        let cm_flush = self.clone();
        let mut flush_shutdown_rx = self.shutdown.subscribe();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(flush_interval_ms));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        cm_flush.clone().flush_gossip_buffer(conn_flush.clone(), max_batches).await;
                    }
                    _ = conn_flush.gossip_flush_notify.notified() => {
                        cm_flush.clone().flush_gossip_buffer(conn_flush.clone(), max_batches).await;
                    }
                    _ = flush_shutdown_rx.recv() => {
                        debug!("[federation] Gossip flush 任务 {} 收到关闭信号", conn_id);
                        break;
                    }
                }
            }
        });

        tokio::spawn(async move {
            let pending_threshold = self.config.receive_pending_threshold;
            loop {
                tokio::select! {
                    result = connection.recv_message() => {
                        match result {
                            Ok((msg_type, payload)) => {
                                // 接收端背压监控：计入待处理消息
                                connection.pending.fetch_add(1, Ordering::Relaxed);
                                // dispatch 返回 true 表示已异步卸载（重量级任务），
                                // pending 计数由卸载任务完成后自行递减；
                                // 返回 false 表示同步处理已完成，此处立即递减。
                                let offloaded = self.clone().dispatch_message(connection.clone(), msg_type, payload).await;
                                if !offloaded {
                                    connection.pending.fetch_sub(1, Ordering::Relaxed);
                                }
                                // 背压告警：单连接待处理积压超过阈值
                                let pending = connection.pending.load(Ordering::Relaxed);
                                if pending > pending_threshold {
                                    warn!(
                                        "[federation] 连接 {} 待处理消息积压 {} 超过阈值 {}，接收端处理慢（背压）",
                                        conn_id, pending, pending_threshold
                                    );
                                }
                            }
                            Err(e) => {
                                debug!("[federation] 连接 {} 读取失败: {}", conn_id, e);
                                self.remove_connection(&conn_id);
                                break;
                            }
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
                    self.node_table.update_rtt(&connection.node_id, pong.rtt_estimate_ms);
                    debug!("[federation] 收到 Pong from {} (rtt={}ms)", connection.node_id, pong.rtt_estimate_ms);
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
                    {
                        let mut buf = connection.gossip_buffer.lock();
                        buf.push_back(batch);
                    }
                    // 通知 flush 任务（达到 max_batches 时立即刷新，否则等定时 tick）
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
                        // 回复 Ack
                        let ack = FullSyncAckMessage { repo_type, seq };
                        let _ = connection.send_message(MessageType::FullSyncAck, &ack).await;
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
            _ => {
                debug!("[federation] 收到未处理消息类型 {:?} from {}", msg_type, connection.node_id);
                false
            }
        };

        offloaded
    }

    /// 异步执行重量级消息处理（GossipBatch / MerkleRepair）。
    ///
    /// 先从 `heavy_task_semaphore` 获取 permit（有界并发），再用
    /// `spawn_blocking` 把阻塞型 DB 写入移出异步运行时线程。
    /// 任务完成后递减该连接的 pending 待处理计数。
    fn spawn_heavy_handler<F>(&self, conn: Arc<Connection>, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let sem = self.heavy_task_semaphore.clone();
        tokio::spawn(async move {
            // 等待 permit：信号量饱和时在此排队，对应消息计入 pending（背压可见）
            let _permit = match sem.acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    conn.pending.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
            };
            // 阻塞型处理移入 blocking 线程池
            let _ = tokio::task::spawn_blocking(f).await;
            drop(_permit);
            conn.pending.fetch_sub(1, Ordering::Relaxed);
        });
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
        // 一次性 drain 所有缓冲的 batch
        let batches: Vec<GossipBatchMessage> = {
            let mut buf = conn.gossip_buffer.lock();
            if buf.is_empty() {
                return;
            }
            buf.drain(..).collect()
        };
        if batches.is_empty() {
            return;
        }

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
                    debug!("[federation] flush GossipBatch 遇到未知 repo_type={}, 丢弃", batch.repo_type);
                }
            }
        }
        let groups = [node_group, peer_group, infohash_group, tracker_group];

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
                        conn_task.pending.fetch_sub(group_len as u32, Ordering::Relaxed);
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
                conn_task.pending.fetch_sub(group_len as u32, Ordering::Relaxed);
            });
        }
    }

    /// 获取指定节点的连接
    pub fn get_connection(&self, node_id: &NodeId) -> Option<Arc<Connection>> {
        self.connections.read().get(node_id).cloned()
    }

    /// 获取所有连接
    pub fn all_connections(&self) -> Vec<Arc<Connection>> {
        self.connections.read().values().cloned().collect()
    }

    /// 当前连接数
    pub fn connection_count(&self) -> usize {
        self.connections.read().len()
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
    use crate::federation::node_id::{NodeIdentity, Reachability};

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
        let mgr = ConnectionManager::new(node_table, identity, make_test_config(), shutdown_tx, Arc::new(FederationMetrics::new()));
        assert_eq!(mgr.connection_count(), 0);
        assert!(mgr.all_connections().is_empty());
    }

    #[tokio::test]
    async fn test_connection_send_recv() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let transport = TcpTransport::new(stream);
            let conn = Connection::new(transport, NodeId([1; 20]), addr);
            let (msg_type, payload) = conn.recv_message().await.unwrap();
            assert_eq!(msg_type, MessageType::Ping);
            let ping: PingMessage = bincode::deserialize(&payload).unwrap();
            assert_eq!(ping.timestamp, 42);
            let pong = PongMessage { timestamp: 42, rtt_estimate_ms: 5 };
            conn.send_message(MessageType::Pong, &pong).await.unwrap();
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let transport = TcpTransport::new(stream);
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
            let _transport = TcpTransport::new(stream);
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let transport = TcpTransport::new(stream);
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
            let mut transport = TcpTransport::new(stream);
            let (peer_id, _) = mgr2_clone.handshake_inbound(&mut transport).await.unwrap();
            assert_eq!(peer_id, identity1.node_id);
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut transport = TcpTransport::new(stream);
        let peer_id = mgr1.handshake_outbound(&mut transport, identity2.node_id).await.unwrap();
        assert_eq!(peer_id, identity2.node_id);

        server.await.unwrap();
    }
}
