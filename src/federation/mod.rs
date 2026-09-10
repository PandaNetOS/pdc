//! PDC 联邦网络
//!
//! 阶段2：Gossip 引擎 + Merkle 对账 + PeerRepo/InfohashRepo 同步 + 打洞信令 + Ed25519 认证
//!
//! 提供联邦网络的统一入口，管理节点身份、节点表、连接、发现、NAT 集成、数据同步、
//! Gossip 传播、Merkle 对账和打洞信令。

pub mod config;
pub mod connection;
pub mod discovery;
pub mod dht_discovery;
pub mod gossip;
pub mod merkle;
pub mod metrics;
pub mod nat_integration;
pub mod relay;
pub mod node_id;
pub mod node_table;
pub mod peer_cache;
pub mod protocol;
pub mod signaling;
pub mod sync;
pub mod transport;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::event_bus::EventBus;
use crate::federation::config::FederationConfig;
use crate::federation::connection::ConnectionManager;
use crate::federation::discovery::DiscoveryService;
use crate::federation::gossip::GossipEngine;
use crate::federation::metrics::{FederationMetrics, FederationMetricsSnapshot};
use crate::federation::nat_integration::NatIntegration;
use crate::federation::node_id::{NodeIdentity, NodeId};
use crate::federation::node_table::NodeTable;
use crate::federation::relay::RelayManager;
use crate::federation::signaling::SignalingService;
use crate::federation::sync::SyncManager;
use crate::federation::transport::UdpTransport;
use crate::nat::NatManager;
use crate::storage::{InfohashRepoImpl, NodeRepoImpl, PeerRepoImpl, TrackerRepoImpl};

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
    /// 配置
    pub config: FederationConfig,
    /// 关闭信号发送端
    shutdown: broadcast::Sender<()>,
    /// 启动时间
    started_at: Instant,
}

impl FederationService {
    /// 创建联邦服务（初始化所有子模块，但不启动后台任务）
    pub fn new(
        config: FederationConfig,
        nat_manager: Arc<NatManager>,
        node_repo: Arc<NodeRepoImpl>,
        data_dir: &Path,
        event_bus: Option<EventBus>,
        peer_repo: Option<Arc<PeerRepoImpl>>,
        infohash_repo: Option<Arc<InfohashRepoImpl>>,
        tracker_repo: Option<Arc<TrackerRepoImpl>>,
    ) -> anyhow::Result<Self> {
        // 1. 加载或创建节点身份
        let identity = Arc::new(NodeIdentity::load_or_create(data_dir)?);

        if let Some(ref id_str) = config.node_id {
            if let Ok(custom_id) = NodeId::from_hex(id_str) {
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

        // 5. 创建连接管理器
        let connection_manager = Arc::new(ConnectionManager::new(
            node_table.clone(),
            identity.clone(),
            config.clone(),
            shutdown_tx.clone(),
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
            shutdown_tx.clone(),
            data_dir,
        ));

        // 8. 创建中继管理器
        let relay_manager = Arc::new(RelayManager::new(
            connection_manager.clone(),
            identity.clone(),
            config.clone(),
            metrics.clone(),
            shutdown_tx.clone(),
        ));

        // 9. 创建同步管理器（含 PeerSync / InfohashSync / TrackerSync）
        let sync_manager = Arc::new(SyncManager::new(
            connection_manager.clone(),
            node_repo,
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
        let udp_addr: SocketAddr = SocketAddr::new(
            "0.0.0.0".parse().unwrap(),
            config.listen_port,
        );
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
            config,
            shutdown: shutdown_tx,
            started_at: Instant::now(),
        })
    }

    /// 启动所有后台任务
    pub async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        info!("[federation] 启动联邦服务（阶段2）...");

        // 1. 启动 TCP 监听
        self.connection_manager
            .clone()
            .start_listen()
            .await?;

        // 2. 启动心跳任务
        self.connection_manager.clone().spawn_heartbeat();

        // 3. 启动 PEX 交换任务
        self.discovery.clone().spawn_pex_exchange();

        // 3.1 启动连接维护任务（定期重置卡住的连接状态并重连）
        self.discovery.clone().spawn_connection_maintainer();

        // 4. 启动 NAT 地址刷新任务（含 STUN 探测）
        self.nat_integration.clone().spawn_address_refresh();

        // 5. 设置 NAT 映射
        self.nat_integration.setup_mapping();

        // 6. 启动 Node 同步任务
        self.sync_manager.clone().spawn_node_sync();

        // 7. 启动 Gossip 传播任务
        self.gossip_engine.clone().spawn_gossip_propagation();

        // 7.1 启动 Merkle 反熵任务（定期对账，发现差异自动修复）
        self.gossip_engine
            .clone()
            .spawn_anti_entropy(self.sync_manager.clone());

        // 8. 启动中继通道清理
        self.relay_manager.clone().spawn_channel_cleanup();

        // 9. 引导连接种子节点
        self.discovery.clone().bootstrap().await;

        info!(
            "[federation] 联邦服务启动完成: node_id={}, port={}, gossip_interval={}ms",
            self.identity.node_id, self.config.listen_port, self.config.gossip_interval_ms
        );

        Ok(())
    }

    /// 优雅关闭（阶段3增强）
    pub async fn shutdown(&self) {
        info!("[federation] 开始优雅关闭...");

        // 1. 发送关闭信号
        let _ = self.shutdown.send(());

        // 2. 关闭所有中继通道
        self.relay_manager.close_all();

        // 3. 等待500ms让消息发出
        tokio::time::sleep(Duration::from_millis(500)).await;

        // 4. 关闭所有 TCP 连接
        self.connection_manager.shutdown_all().await;

        info!("[federation] 联邦服务已关闭");
    }

    /// 生成完整状态快照（用于 REST API）
    pub fn snapshot(&self) -> FederationSnapshot {
        let status = self.status();

        // 连接列表
        let conns = self.connection_manager.all_connections();
        let now = std::time::Instant::now();
        let connections: Vec<ConnectionInfo> = conns
            .iter()
            .map(|c| ConnectionInfo {
                node_id: c.node_id.to_hex(),
                addr: c.addr.to_string(),
                connected_at_secs: c.connected_at.elapsed().as_secs(),
                rtt_ms: self
                    .node_table
                    .get(&c.node_id)
                    .and_then(|e| e.rtt_ms),
            })
            .collect();

        // 节点列表（最多100个）
        let nodes_all = self.node_table.all_nodes();
        let total_nodes = nodes_all.len();
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
        }
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
            FederationService::new(make_test_config(), nat, node_repo, &dir, None, None, None, None)
                .unwrap(),
        );

        service.clone().start().await.unwrap();
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
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"enabled\":true"));
        assert!(json.contains("\"connections\":5"));
        assert!(json.contains("\"gossip_queue_size\":3"));
    }
}
