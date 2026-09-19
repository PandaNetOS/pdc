//! PDC 联邦网络
//!
//! 阶段2：Gossip 引擎 + Merkle 对账 + PeerRepo/InfohashRepo 同步 + 打洞信令 + Ed25519 认证
//!
//! 提供联邦网络的统一入口，管理节点身份、节点表、连接、发现、NAT 集成、数据同步、
//! Gossip 传播、Merkle 对账和打洞信令。

pub mod config;
pub mod connection;
pub mod dht_discovery;
pub mod discovery;
pub mod gossip;
pub mod merkle;
pub mod metrics;
pub mod nat_integration;
pub mod node_id;
pub mod node_table;
pub mod peer_cache;
pub mod protocol;
pub mod relay;
pub mod sharded_lru;
pub mod signaling;
pub mod sync;
pub mod transport;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::broadcast;
use tracing::{debug, info};

use crate::event_bus::EventBus;
use crate::federation::config::FederationConfig;
use crate::federation::connection::ConnectionManager;
use crate::federation::dht_discovery::DhtDiscoveryService;
use crate::federation::discovery::DiscoveryService;
use crate::federation::gossip::GossipEngine;
use crate::federation::metrics::{FederationMetrics, FederationMetricsSnapshot};
use crate::federation::nat_integration::NatIntegration;
use crate::federation::node_id::{NodeId, NodeIdentity};
use crate::federation::node_table::NodeTable;
use crate::federation::relay::RelayManager;
use crate::federation::signaling::SignalingService;
use crate::federation::sync::SyncManager;
use crate::federation::transport::UdpTransport;
use crate::nat::NatManager;
use crate::storage::{InfohashRepoImpl, NodeRepoImpl, PeerRepoImpl, TrackerRepoImpl};

/// iroh/QUIC 连接超时
const IROH_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// 启动时 STUN 探测等待超时
const STUN_PROBE_STARTUP_TIMEOUT: Duration = Duration::from_secs(3);
/// 关闭流程中等待消息发出的宽限时间
const SHUTDOWN_MESSAGE_FLUSH_GRACE: Duration = Duration::from_millis(500);

/// 联邦服务状态
#[derive(Debug, Clone, Serialize)]
pub struct FederationStatus {
    /// 是否启用
    pub enabled: bool,
    /// 节点 ID（十六进制）
    pub node_id: String,
    /// 当前连接数
    pub connections: usize,
    /// 已知节点数
    pub known_nodes: usize,
    /// 可达性等级
    pub reachability: String,
    /// 运行时长（秒）
    pub uptime_secs: u64,
    /// Gossip 待传播队列大小
    pub gossip_queue_size: usize,
    /// 中继通道数
    pub relay_channels: usize,
    /// Tracker 同步是否启用
    pub tracker_sync_enabled: bool,
    /// 指标快照
    pub metrics: FederationMetricsSnapshot,
    /// NodeRepo 实际总条目数（非联邦同步累计）
    pub node_repo_total: u64,
    /// PeerRepo 实际总条目数（非联邦同步累计）
    pub peer_repo_total: u64,
    /// PeerRepo 活跃 peer 数（最近1小时内有活跃）
    pub peer_repo_active: u64,
    /// InfohashRepo 实际总条目数（非联邦同步累计）
    pub infohash_repo_total: u64,
    /// TrackerRepo 实际总条目数（非联邦同步累计）
    pub tracker_repo_total: u64,
}

/// 连接信息（用于快照）
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionInfo {
    pub node_id: String,
    pub addr: String,
    pub connected_at_secs: u64,
    pub rtt_ms: Option<u32>,
}

/// 节点信息（用于快照）
#[derive(Debug, Clone, Serialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub addr: Option<String>,
    pub status: String,
    pub rtt_ms: Option<u32>,
    pub last_seen_secs_ago: u64,
}

/// 同步统计
#[derive(Debug, Clone, Serialize, Default)]
pub struct SyncStats {
    pub node_sync_count: u64,
    pub peer_sync_count: u64,
    pub infohash_sync_count: u64,
    pub tracker_sync_count: u64,
    pub gossip_propagations: u64,
}

/// 中继统计
#[derive(Debug, Clone, Serialize)]
pub struct RelayStats {
    pub active_channels: usize,
    pub total_bytes_forwarded: u64,
    pub current_bandwidth_mbps: f64,
}

/// 联邦完整状态快照
#[derive(Debug, Clone, Serialize)]
pub struct FederationSnapshot {
    pub status: FederationStatus,
    pub connections: Vec<ConnectionInfo>,
    pub nodes: Vec<NodeInfo>,
    pub sync_stats: SyncStats,
    pub relay_stats: RelayStats,
}

/// 联邦服务主入口
pub struct FederationService {
    /// 节点身份
    pub identity: Arc<NodeIdentity>,
    /// 节点表
    pub node_table: Arc<NodeTable>,
    /// 连接管理器
    pub connection_manager: Arc<ConnectionManager>,
    /// 节点发现服务
    pub discovery: Arc<DiscoveryService>,
    /// NAT 集成
    pub nat_integration: Arc<NatIntegration>,
    /// 同步管理器
    pub sync_manager: Arc<SyncManager>,
    /// Gossip 引擎
    pub gossip_engine: Arc<GossipEngine>,
    /// 打洞信令服务
    pub signaling_service: Arc<SignalingService>,
    /// 监控指标
    pub metrics: Arc<FederationMetrics>,
    /// UDP 传输层（打洞用）
    pub udp_transport: Arc<UdpTransport>,
    /// 中继管理器
    pub relay_manager: Arc<RelayManager>,
    /// DHT 魔法 infohash 发现服务（由 TaskScheduler 调度）
    pub dht_discovery: Option<Arc<DhtDiscoveryService>>,
    /// 配置
    pub config: FederationConfig,
    /// 数据目录（用于 NetAgent/Iroh 持久化）
    data_dir: std::path::PathBuf,
    /// 关闭信号发送端
    shutdown: broadcast::Sender<()>,
    /// 启动时间
    started_at: Instant,
}

impl FederationService {
    /// 创建联邦服务（初始化所有子模块，但不启动后台任务）
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: FederationConfig,
        nat_manager: Arc<NatManager>,
        node_repo: Arc<NodeRepoImpl>,
        data_dir: &Path,
        dht_discoverer: Option<Arc<crate::discoverers::dht::DhtDiscoverer>>,
        event_bus: Option<EventBus>,
        peer_repo: Option<Arc<PeerRepoImpl>>,
        infohash_repo: Option<Arc<InfohashRepoImpl>>,
        tracker_repo: Option<Arc<TrackerRepoImpl>>,
    ) -> anyhow::Result<Self> {
        // 1. 加载或创建节点身份
        let identity = Arc::new(NodeIdentity::load_or_create(data_dir)?);

        if let Some(ref id_str) = config.node_id {
            if let Ok(_custom_id) = NodeId::from_hex(id_str) {
                info!(
                    "[federation] 配置指定 node_id: {}（实际使用: {}）",
                    id_str,
                    identity.node_id.to_hex()
                );
            }
        }

        // 2. 创建关闭信号
        let (shutdown_tx, _) = broadcast::channel(1);

        // 3. 创建监控指标
        let metrics = Arc::new(FederationMetrics::new());

        // 4. 创建节点表
        let node_table = Arc::new(NodeTable::new(config.max_connections * 4));

        // 5. 创建连接管理器（传入共享 metrics，确保 transport 层字节统计与 REST API 返回同一实例）
        let connection_manager = Arc::new(ConnectionManager::new(
            node_table.clone(),
            identity.clone(),
            config.clone(),
            shutdown_tx.clone(),
            metrics.clone(),
        ));

        // 6. 创建 Gossip 引擎
        let gossip_engine = Arc::new(GossipEngine::new(
            connection_manager.clone(),
            config.clone(),
            identity.node_id,
            metrics.clone(),
            shutdown_tx.clone(),
        ));

        // 7. 创建发现服务
        let discovery = Arc::new(DiscoveryService::new(
            connection_manager.clone(),
            node_table.clone(),
            identity.clone(),
            config.clone(),
            config.api_port,
            shutdown_tx.clone(),
            data_dir,
            Some(node_repo.clone()),
            dht_discoverer.clone(),
        ));

        // 8. 创建中继管理器
        let relay_manager = Arc::new(RelayManager::new(
            connection_manager.clone(),
            identity.clone(),
            config.clone(),
            metrics.clone(),
        ));

        // 9. 创建同步管理器（含 PeerSync / InfohashSync / TrackerSync）
        let sync_manager = Arc::new(SyncManager::new(
            connection_manager.clone(),
            node_repo.clone(),
            config.clone(),
            shutdown_tx.clone(),
            gossip_engine.clone(),
            metrics.clone(),
            identity.node_id,
            event_bus,
            peer_repo,
            infohash_repo,
            tracker_repo,
            Some(relay_manager.clone()),
        ));

        // 9. 创建 NAT 集成
        let nat_integration = Arc::new(NatIntegration::new(
            nat_manager,
            identity.clone(),
            config.clone(),
            shutdown_tx.clone(),
        ));

        // 10. 创建 UDP 传输层（与 TCP 同端口）
        let udp_addr: SocketAddr = SocketAddr::new("0.0.0.0".parse().unwrap(), config.listen_port);
        let udp_transport = UdpTransport::try_bind(udp_addr)?;

        // 11. 创建打洞信令服务
        let signaling_service = Arc::new(SignalingService::new(
            connection_manager.clone(),
            node_table.clone(),
            identity.clone(),
            udp_transport.clone(),
            metrics.clone(),
            shutdown_tx.clone(),
        ));

        // 12. 注入循环依赖
        connection_manager.set_discovery(discovery.clone());
        connection_manager.set_sync_manager(sync_manager.clone());
        connection_manager.set_signaling_service(signaling_service.clone());
        connection_manager.set_relay_manager(relay_manager.clone());

        // 13. 创建 DHT 魔法 infohash 发现服务（从 DiscoveryService 迁移至此，由 TaskScheduler 统一调度）
        let dht_discovery = if config.dht_discovery_enabled {
            Some(Arc::new(DhtDiscoveryService::new(
                node_table.clone(),
                config.listen_port,
                config.dht_discovery_interval_secs,
                shutdown_tx.clone(),
                Some(node_repo.clone()),
                dht_discoverer.clone(),
                Some(connection_manager.clone()),
            )))
        } else {
            None
        };

        info!(
            "[federation] 联邦服务已创建: node_id={}, listen_port={}, ed25519_pubkey={}",
            identity.node_id,
            config.listen_port,
            &hex::encode(identity.public_key_bytes())[..16]
        );

        Ok(Self {
            identity,
            node_table,
            connection_manager,
            discovery,
            nat_integration,
            sync_manager,
            gossip_engine,
            signaling_service,
            metrics,
            udp_transport,
            relay_manager,
            dht_discovery,
            config,
            data_dir: data_dir.to_path_buf(),
            shutdown: shutdown_tx,
            started_at: Instant::now(),
        })
    }

    /// 初始化 NetAgent（Iroh+TCP 传输层）并注入 ConnectionManager
    ///
    /// 根据 config.transport_mode 创建 TransportRouter：
    /// - tcp_only: 仅 TCP
    /// - iroh_only: 仅 Iroh（QUIC）
    /// - auto: Iroh + TCP 并行 race，先连成功的用
    async fn init_net_agent(self: &Arc<Self>) -> anyhow::Result<()> {
        use pnos_net::transport::{IrohTransportConfig, TransportMode};

        let mode = match self.config.transport_mode.as_str() {
            "iroh_only" => TransportMode::IrohOnly,
            "auto" => TransportMode::Auto,
            _ => TransportMode::TcpOnly,
        };

        info!(
            "[federation] 初始化 NetAgent: transport_mode={:?}, listen_port={}",
            mode, self.config.listen_port
        );

        // Iroh 配置（非 TcpOnly 时启用）
        let iroh_config = if mode != TransportMode::TcpOnly {
            Some(IrohTransportConfig {
                node_id: self.identity.node_id.0,
                listen_port: self.config.listen_port, // 与 TCP 复用端口（QUIC 基于 UDP）
                data_dir: self.data_dir.join("iroh"),
                derp_enabled: true,
                derp_urls: Vec::new(),
                connect_timeout: IROH_CONNECT_TIMEOUT,
                alpn: b"pdc-federation/1.0".to_vec(),
            })
        } else {
            None
        };

        let net_config = pnos_net::NetAgentConfig {
            node_id: self.identity.node_id.0,
            listen_port: self.config.listen_port,
            api_port: self.config.api_port,
            data_dir: self.data_dir.clone(),
            lpd_multicast_port: 6771,
            lpd_enabled: false,        // PDC 已有独立 LPD
            peer_cache_enabled: false, // PDC 已有独立 peer_cache
            nat_enabled: false,        // PDC 已有独立 NAT
            hole_punch_enabled: false, // PDC 已有独立打洞
            connect_config: Default::default(),
            transport_mode: mode,
            iroh_config,
        };

        let net_agent = pnos_net::NetAgent::new(net_config).await?;
        net_agent.start_transport_only().await?;
        self.connection_manager.set_net_agent(net_agent);
        info!("[federation] NetAgent 已注入 ConnectionManager");
        Ok(())
    }

    /// 启动所有后台任务
    pub async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        info!("[federation] 启动联邦服务（阶段2）...");

        // 0. 创建并注入 NetAgent（Iroh+TCP 传输层）
        self.init_net_agent().await?;

        // 1. 启动 TCP 监听
        self.connection_manager.clone().start_listen().await?;

        // 2. 心跳任务已迁移到 TaskScheduler（fed_heartbeat）

        // 3. PEX 交换任务已迁移到 TaskScheduler（fed_pex_exchange）

        // 3.1 连接维护任务已迁移到 TaskScheduler（fed_connection_maintain）

        // 4. NAT 地址刷新任务已迁移到 TaskScheduler（fed_nat_refresh）

        // 4.1 启动时先执行一次 STUN 探测，确保 setup_mapping 能拿到 STUN 结果
        //     否则首次 setup_mapping 时 last_stun 为 None，reachability 会误判为 Unknown
        //     使用 spawn_blocking + 3秒超时，避免 STUN 无响应时阻塞 tokio 运行时
        let nat_clone = self.nat_integration.clone();
        let stun_handle = tokio::task::spawn_blocking(move || {
            nat_clone.stun_probe();
        });
        let _ = tokio::time::timeout(STUN_PROBE_STARTUP_TIMEOUT, stun_handle).await;

        // 5. 设置 NAT 映射
        self.nat_integration.setup_mapping();

        // 5.1 同步公网地址到 DiscoveryService（MQTT Rendezvous 上报时优先使用公网地址）
        let public_addr = self.nat_integration.get_public_address();
        self.discovery.set_public_addr(public_addr);

        // 5.2 公网地址同步已迁移到 TaskScheduler（fed_public_addr_sync）

        // 6. Node 同步任务已迁移到 TaskScheduler（fed_node_sync）

        // 6.1 Merkle 异步批量 flush 已迁移到 TaskScheduler（fed_merkle_flush）

        // 6.2 启动时从 repo 全量重建 Merkle 树（修复启动时 Merkle 为空导致差量同步推不全）
        self.sync_manager.clone().spawn_merkle_rebuilder();

        // 7. Gossip 传播任务已迁移到 TaskScheduler（fed_gossip_propagation）

        // 7.1 Merkle 反熵任务已迁移到 TaskScheduler（fed_merkle_anti_entropy）

        // 7.2 Push-Pull Gossip 已迁移到 TaskScheduler（fed_push_pull_gossip）
        // 8. 中继通道清理已迁移到 TaskScheduler（fed_relay_channel_cleanup）

        // 9. 引导连接种子节点
        self.discovery.clone().bootstrap().await;

        // 10. DHT 魔法 infohash 发现：异步初始化（不阻塞 start），后续周期由 TaskScheduler 调度
        if let Some(ref dht) = self.dht_discovery {
            let dht_clone = dht.clone();
            tokio::spawn(async move {
                dht_clone.init_and_first_tick().await;
            });
        }

        info!(
            "[federation] 联邦服务启动完成: node_id={}, port={}, gossip_interval={}ms",
            self.identity.node_id, self.config.listen_port, self.config.gossip_interval_ms
        );

        Ok(())
    }

    /// 同步公网地址到 DiscoveryService（由 TaskScheduler 按间隔调度）
    pub fn sync_public_addr(&self) {
        let addr = self.nat_integration.get_public_address();
        self.discovery.set_public_addr(addr);
    }

    /// 优雅关闭（阶段3增强）
    pub async fn shutdown(&self) {
        info!("[federation] 开始优雅关闭...");

        // 1. 发送关闭信号
        let _ = self.shutdown.send(());

        // 2. 关闭所有中继通道
        self.relay_manager.close_all();

        // [ALLOWED-SLEEP] 关闭流程中一次性等待消息发出，非周期性
        // 3. 等待500ms让消息发出
        tokio::time::sleep(SHUTDOWN_MESSAGE_FLUSH_GRACE).await;

        // 4. 关闭所有 TCP 连接
        self.connection_manager.shutdown_all().await;

        info!("[federation] 联邦服务已关闭");
    }

    /// 生成完整状态快照（用于 REST API）
    pub fn snapshot(&self) -> FederationSnapshot {
        let status = self.status();

        // 连接列表
        let conns = self.connection_manager.all_connections();
        let _now = std::time::Instant::now();
        let connections: Vec<ConnectionInfo> = conns
            .iter()
            .map(|c| ConnectionInfo {
                node_id: c.node_id.to_hex(),
                addr: c.addr.to_string(),
                connected_at_secs: c.connected_at.elapsed().as_secs(),
                rtt_ms: self.node_table.get(&c.node_id).and_then(|e| e.rtt_ms),
            })
            .collect();

        // 节点列表（最多100个）
        let nodes_all = self.node_table.all_nodes();
        let _total_nodes = nodes_all.len();
        let nodes: Vec<NodeInfo> = nodes_all
            .iter()
            .take(100)
            .map(|e| NodeInfo {
                node_id: NodeId(e.info.node_id).to_hex(),
                addr: e.info.preferred_addr().map(|a| a.to_string()),
                status: format!("{:?}", e.status),
                rtt_ms: e.rtt_ms,
                last_seen_secs_ago: e.info.last_seen,
            })
            .collect();

        // 同步统计
        let sync_stats = self.sync_manager.sync_stats();

        // 中继统计
        let relay_stats = RelayStats {
            active_channels: self.relay_manager.active_channel_count(),
            total_bytes_forwarded: self.relay_manager.total_bytes_forwarded(),
            current_bandwidth_mbps: self.relay_manager.current_bandwidth_mbps(),
        };

        FederationSnapshot {
            status,
            connections,
            nodes,
            sync_stats,
            relay_stats,
        }
    }

    /// 已知节点总数
    pub fn known_node_count(&self) -> usize {
        self.node_table.len()
    }

    /// 获取全量同步进行中标记的共享句柄。
    /// 非核心模块（DHT 爬虫 / Active-PEX / 健康检查 / dht_probe）在全量同步期间
    /// 通过此 flag 暂停主动工作，把带宽与 CPU 让给联邦增量/全量同步。
    pub fn full_sync_gate(&self) -> Arc<AtomicBool> {
        self.gossip_engine.pause_gate()
    }

    /// 获取联邦状态
    pub fn status(&self) -> FederationStatus {
        let addresses = self.identity.addresses_snapshot();
        let reachability = addresses
            .first()
            .map(|a| a.reachability.to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        FederationStatus {
            enabled: self.config.enabled,
            node_id: self.identity.node_id.to_hex(),
            connections: self.connection_manager.connection_count(),
            known_nodes: self.node_table.len(),
            reachability,
            uptime_secs: self.started_at.elapsed().as_secs(),
            gossip_queue_size: self.gossip_engine.outbox_size(),
            relay_channels: self.relay_manager.active_channel_count(),
            tracker_sync_enabled: self.config.sync_tracker_enabled,
            metrics: self.metrics.snapshot(),
            // Repo 实际总数由 handler 从 AppState 填充，此处先置 0
            node_repo_total: 0,
            peer_repo_total: 0,
            peer_repo_active: 0,
            infohash_repo_total: 0,
            tracker_repo_total: 0,
        }
    }

    /// P2-3：同步面可观测性（oplog 水位 / 每对端增量落后 / bootstrap 进度 / range 对账统计）。
    pub fn sync_observability(&self) -> serde_json::Value {
        self.sync_manager.sync_observability()
    }

    /// 向所有已连接的联邦节点并行发送 PeerQueryRequest，等待 `timeout` 后收集响应中的 peer 地址，去重返回。
    ///
    /// 用于 SuperTracker announce 时本地 peer 不足，实时向联邦节点拉取同 infohash 的 peer。
    /// 响应会同时异步写入本地 PeerRepo（由 ConnectionManager dispatch 的 PeerQueryResponse 分支完成）。
    pub async fn query_peers(
        &self,
        infohash: &[u8; 20],
        limit: usize,
        timeout: Duration,
    ) -> Vec<SocketAddr> {
        use crate::federation::protocol::{MessageType, PeerQueryRequestMessage};
        use std::collections::HashSet;

        let conns = self.connection_manager.all_connections();
        if conns.is_empty() {
            return Vec::new();
        }

        // 清空该 infohash 的旧响应，避免上一次查询残留
        self.connection_manager.clear_peer_query_responses(infohash);

        let req = PeerQueryRequestMessage {
            infohash: *infohash,
            limit: limit as u32,
        };

        // 并行向所有已连接节点发送查询请求（fire-and-forget，响应异步到达）
        for conn in &conns {
            if let Err(e) = conn.send_message(MessageType::PeerQueryRequest, &req).await {
                debug!(
                    "[federation] PeerQueryRequest 发送到 {} 失败: {}",
                    conn.node_id, e
                );
            }
        }

        // [ALLOWED-SLEEP] 等待响应到达的一次性超时，非周期性
        // 等待响应到达（统一超时，不阻塞调用方超过 timeout）
        tokio::time::sleep(timeout).await;

        // 收集所有节点返回的 peer，按 SocketAddr 去重
        let entries = self.connection_manager.take_peer_query_responses(infohash);
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for e in entries {
            if let Ok(ip) = e.ip.parse::<std::net::IpAddr>() {
                let addr = SocketAddr::new(ip, e.port);
                if seen.insert(addr) {
                    result.push(addr);
                }
            }
        }
        debug!(
            "[federation] query_peers: infohash={:?}, peers_returned={}",
            &hex::encode(infohash),
            result.len()
        );
        result
    }
}

impl std::fmt::Debug for FederationService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FederationService")
            .field("node_id", &self.identity.node_id)
            .field("config", &self.config)
            .field("connections", &self.connection_manager.connection_count())
            .field("known_nodes", &self.node_table.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nat::NatConfig;
    use crate::storage::Storage;

    fn make_test_config() -> FederationConfig {
        FederationConfig {
            enabled: true,
            listen_port: 0,
            max_connections: 10,
            target_neighbors: 4,
            seed_nodes: vec![],
            nat_mapping_enabled: false,
            sync_node_enabled: true,
            sync_node_interval_secs: 300,
            sync_peer_enabled: false,
            sync_infohash_enabled: false,
            dht_discovery_enabled: false,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_federation_service_creation() {
        let dir = std::env::temp_dir().join(format!("pdc_fed_svc_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let nat_config = NatConfig {
            enabled: false,
            ..Default::default()
        };
        let nat = Arc::new(NatManager::new(nat_config));
        let storage = Arc::new(Storage::memory().unwrap());
        let node_repo = Arc::new(NodeRepoImpl::new(storage));

        let service = FederationService::new(
            make_test_config(),
            nat,
            node_repo,
            &dir,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert!(service.config.enabled);
        assert_eq!(service.connection_manager.connection_count(), 0);
        assert_eq!(service.node_table.len(), 0);

        let status = service.status();
        assert!(status.enabled);
        assert_eq!(status.node_id.len(), 40);
        assert_eq!(status.connections, 0);
        assert_eq!(status.known_nodes, 0);
        assert!(!status.reachability.is_empty());
        assert_eq!(status.gossip_queue_size, 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[ignore = "network-dependent, can hang on Windows; run manually with --ignored"]
    #[tokio::test]
    async fn test_federation_service_start_stop() {
        let dir = std::env::temp_dir().join(format!("pdc_fed_start_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let nat_config = NatConfig {
            enabled: false,
            ..Default::default()
        };
        let nat = Arc::new(NatManager::new(nat_config));
        let storage = Arc::new(Storage::memory().unwrap());
        let node_repo = Arc::new(NodeRepoImpl::new(storage));

        let service = Arc::new(
            FederationService::new(
                make_test_config(),
                nat,
                node_repo.clone(),
                &dir,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap(),
        );

        service.clone().start().await.unwrap();
        // [ALLOWED-SLEEP] 测试代码中的一次性等待
        tokio::time::sleep(Duration::from_millis(200)).await;

        let status = service.status();
        assert!(status.enabled);
        assert!(status.uptime_secs < 10);

        service.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_federation_service_identity_persistence() {
        let dir = std::env::temp_dir().join(format!("pdc_fed_persist_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let nat_config = NatConfig {
            enabled: false,
            ..Default::default()
        };
        let storage = Arc::new(Storage::memory().unwrap());
        let node_repo = Arc::new(NodeRepoImpl::new(storage));

        let service1 = FederationService::new(
            make_test_config(),
            Arc::new(NatManager::new(nat_config.clone())),
            node_repo.clone(),
            &dir,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let id1 = service1.identity.node_id.to_hex();
        let pk1 = service1.identity.public_key_bytes();

        let service2 = FederationService::new(
            make_test_config(),
            Arc::new(NatManager::new(nat_config)),
            node_repo,
            &dir,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let id2 = service2.identity.node_id.to_hex();
        let pk2 = service2.identity.public_key_bytes();

        assert_eq!(id1, id2);
        assert_eq!(pk1, pk2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_federation_status_serialize() {
        let status = FederationStatus {
            enabled: true,
            node_id: "a".repeat(40),
            connections: 5,
            known_nodes: 100,
            reachability: "Mapped".to_string(),
            uptime_secs: 3600,
            gossip_queue_size: 3,
            relay_channels: 0,
            tracker_sync_enabled: false,
            metrics: FederationMetricsSnapshot::default(),
            node_repo_total: 0,
            peer_repo_total: 0,
            peer_repo_active: 0,
            infohash_repo_total: 0,
            tracker_repo_total: 0,
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"enabled\":true"));
        assert!(json.contains("\"connections\":5"));
        assert!(json.contains("\"gossip_queue_size\":3"));
    }
}
