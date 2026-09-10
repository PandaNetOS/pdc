//! 连接管理
//!
//! 管理联邦网络中的所有 TCP 连接，包括监听、主动连接、握手、心跳和消息分发。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::{broadcast, Mutex as TokioMutex, OnceCell};
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
    /// 监控指标
    metrics: Arc<FederationMetrics>,
}

impl ConnectionManager {
    /// 创建连接管理器
    pub fn new(
        node_table: Arc<NodeTable>,
        identity: Arc<crate::federation::node_id::NodeIdentity>,
        config: FederationConfig,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            connections: RwLock::new(FxHashMap::default()),
            connecting: RwLock::new(FxHashSet::default()),
            node_table,
            identity,
            config,
            shutdown,
            discovery: OnceCell::new(),
            sync_manager: OnceCell::new(),
            signaling_service: OnceCell::new(),
            metrics: Arc::new(FederationMetrics::new()),
        }
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
        let transport = TcpTransport::new(stream);

        // 入站握手
        let (node_id, _hello) = self.handshake_inbound(&transport).await?;

        // 检查是否已存在连接
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

        info!("[federation] 入站连接建立: {} ({})", node_id, addr);

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
            mgr.trigger_initial_sync();
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
            Ok(t) => t,
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
            sync_mgr.clone().trigger_initial_sync();
        }

        // 连接建立成功，移除 connecting 标记
        self.connecting.write().remove(&addr);

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

    /// 移除连接
    fn remove_connection(&self, node_id: &NodeId) {
        if self.connections.write().remove(node_id).is_some() {
            self.metrics.record_connection_closed();
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

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = connection.recv_message() => {
                        match result {
                            Ok((msg_type, payload)) => {
                                self.clone().dispatch_message(connection.clone(), msg_type, payload).await;
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

    /// 分发消息
    async fn dispatch_message(
        self: Arc<Self>,
        connection: Arc<Connection>,
        msg_type: MessageType,
        payload: Vec<u8>,
    ) {
        connection.touch();

        match msg_type {
            MessageType::Ping => {
                if let Ok(ping) = bincode::deserialize::<PingMessage>(&payload) {
                    let pong = PongMessage {
                        timestamp: ping.timestamp,
                        rtt_estimate_ms: 0,
                    };
                    let _ = connection.send_message(MessageType::Pong, &pong).await;
                }
            }
            MessageType::Pong => {
                if let Ok(pong) = bincode::deserialize::<PongMessage>(&payload) {
                    self.node_table.update_rtt(&connection.node_id, pong.rtt_estimate_ms);
                    debug!("[federation] 收到 Pong from {} (rtt={}ms)", connection.node_id, pong.rtt_estimate_ms);
                }
            }
            MessageType::GetNodes => {
                if let Ok(req) = bincode::deserialize::<GetNodesMessage>(&payload) {
                    if let Some(discovery) = self.discovery.get() {
                        discovery.handle_get_nodes(&connection, req.count).await;
                    }
                }
            }
            MessageType::Nodes => {
                if let Ok(msg) = bincode::deserialize::<NodesMessage>(&payload) {
                    if let Some(discovery) = self.discovery.get() {
                        discovery.handle_nodes_received(msg.nodes);
                    }
                }
            }
            MessageType::ExchangeNodes => {
                if let Ok(msg) = bincode::deserialize::<ExchangeNodesMessage>(&payload) {
                    if let Some(discovery) = self.discovery.get() {
                        discovery.handle_exchange_nodes(msg.nodes);
                    }
                }
            }
            MessageType::SyncBatch => {
                if let Ok(msg) = bincode::deserialize::<SyncBatchMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        sync_mgr.handle_sync_batch(msg.repo_type, &msg.entries);
                    }
                }
            }
            MessageType::GossipBatch => {
                self.metrics.record_message_recv();
                if let Ok(batch) = bincode::deserialize::<GossipBatchMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        sync_mgr.handle_gossip_batch(batch);
                    }
                }
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
            }
            MessageType::MerkleRepair => {
                self.metrics.record_message_recv();
                if let Ok(repair) = bincode::deserialize::<MerkleRepairMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        sync_mgr.handle_merkle_repair(repair);
                    }
                }
            }
            MessageType::Signaling => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<SignalingMessage>(&payload) {
                    if let Some(signaling) = self.signaling_service.get() {
                        signaling.handle_signaling(connection.node_id, msg);
                    }
                }
            }
            MessageType::RelaySetup => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<RelaySetupMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        sync_mgr.handle_relay_setup(connection.node_id, msg);
                    }
                }
            }
            MessageType::RelayData => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<RelayDataMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        sync_mgr.handle_relay_data(connection.node_id, msg);
                    }
                }
            }
            MessageType::Goodbye => {
                debug!("[federation] 收到 Goodbye from {}", connection.node_id);
                self.remove_connection(&connection.node_id);
            }
            _ => {
                debug!("[federation] 收到未处理消息类型 {:?} from {}", msg_type, connection.node_id);
            }
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
        let mgr = ConnectionManager::new(node_table, identity, make_test_config(), shutdown_tx);
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
        ));
        let mgr2 = Arc::new(ConnectionManager::new(
            node_table2.clone(),
            identity2.clone(),
            make_test_config(),
            shutdown_tx,
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
