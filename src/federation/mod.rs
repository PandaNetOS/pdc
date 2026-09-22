//! PDC 联邦网络
//!
//! 阶段2：Gossip 引擎 + Merkle 对账 + PeerRepo/InfohashRepo 同步 + 打洞信令 + Ed25519 认证
//!
//! 提供联邦网络的统一入口，管理节点身份、节点表、连接、发现、NAT 集成、数据同步、
//! Gossip 传播、Merkle 对账和打洞信令。

pub mod config;
pub mod dht_discovery;
pub mod discovery;
pub mod dispatch;
pub mod gossip;
pub mod metrics;
pub mod nat_integration;
pub mod node_id;
pub mod node_table;
pub mod peer_caps;
pub mod peer_conn;
pub mod peer_query_store;
pub mod protocol;
pub mod relay;
pub mod session;
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
use crate::federation::dht_discovery::DhtDiscoveryService;
use crate::federation::discovery::DiscoveryService;
use crate::federation::dispatch::FederationDispatcher;
use crate::federation::gossip::GossipEngine;
use crate::federation::metrics::{FederationMetrics, FederationMetricsSnapshot};
use crate::federation::nat_integration::NatIntegration;
use crate::federation::node_id::{NodeId, NodeIdentity};
use crate::federation::node_table::NodeTable;
use crate::federation::peer_caps::PeerCapsTable;
use crate::federation::peer_query_store::PeerQueryStore;
use crate::federation::relay::RelayManager;
use crate::federation::session::{bind_federation_sessions, SessionsHandle};
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
    /// oplog 当前行数（内存缓存，O(1)；与 `/sync-observability` 的 `oplog.len` 同源）
    pub oplog_len: u64,
    /// 指标快照
    pub metrics: FederationMetricsSnapshot,
    /// NodeRepo 实际总条目数（F9: = DB 冷数据有效行数 + 内存未落库写队列，唯一权威口径）
    pub node_repo_total: u64,
    /// NodeRepo 内存热/温条目数（DB 权威总数的子集，仅观测用）
    #[serde(default)]
    pub node_repo_hot_total: u64,
    /// PeerRepo 实际总条目数（F9: = DB peers 有效行数 + peers_archive 冷归档）
    pub peer_repo_total: u64,
    /// PeerRepo 内存热/温条目数（仅观测用）
    #[serde(default)]
    pub peer_repo_hot_total: u64,
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

/// 轻量同步摘要（`/federation/status` 快接口用，毫秒级；不触碰重查询）。
#[derive(Debug, Clone, Serialize, Default)]
pub struct SyncBrief {
    /// oplog 当前行数（内存缓存）
    pub oplog_len: u64,
}

/// 联邦服务主入口
pub struct FederationService {
    /// 节点身份
    pub identity: Arc<NodeIdentity>,
    /// 节点表
    pub node_table: Arc<NodeTable>,
    /// 业务分派器（G6 起所有 `MessageType` 分派与承载态归口于此）
    pub dispatcher: Arc<FederationDispatcher>,
    /// SDK 会话门面句柄（**延迟绑定**：`NetAgent` 就绪后由 `init_net_agent` 填充）
    ///
    /// 连接域 100% 归 `pnos-net`：入站 accept / 握手 / 保活 / 候选拨号全部由 SDK 承担。
    /// pdc 侧**不持有任何连接状态**，只经此句柄查询与下发指令；
    /// 未绑定（`NetAgent` 就绪前）时查询一律返回「无连接」语义。
    pub sessions: Arc<SessionsHandle>,
    /// 对端协议能力表（`peer_id → protocol_version`，从连接对象剥离）
    pub peer_caps: Arc<PeerCapsTable>,
    /// 联邦实时 peer 查询响应收集器（PT 业务，从连接对象剥离）
    pub peer_query_store: Arc<PeerQueryStore>,
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
    /// DNS 解析配置（默认走 pnos-net 内置公共 DNS，不读系统 DNS）
    dns: pnos_net::dns::DnsConfig,
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

        // 4.1 连接域剥离出的两张业务表（协议能力 / peer 查询结果）
        //     二者原本混在 ConnectionManager 内，属 pdc 业务语义，不下沉通用 SDK。
        let peer_caps = Arc::new(PeerCapsTable::new());
        let peer_query_store = Arc::new(PeerQueryStore::new());

        // 4.2 业务分派器：连接层与业务层由**上层装配**，连接层不反向持有业务对象（迁移计划 K3）
        let dispatcher = Arc::new(FederationDispatcher::new(
            config.clone(),
            node_table.clone(),
            metrics.clone(),
            peer_caps.clone(),
            peer_query_store.clone(),
        ));

        // 5. SDK 会话门面句柄（延迟绑定：`init_net_agent` 里 NetAgent 就绪后填充）
        //
        // 连接域 100% 归 `pnos-net`：门面是 pdc 侧唯一的连接适配点，
        // 6 个业务模块只持句柄 —— 未绑定时查询返回「无连接」语义。
        let sessions_handle = Arc::new(SessionsHandle::new());

        // 5.1 「主动断连」的执行权在**承载侧**＝ SDK 会话层：
        //     分派器只发指令，不自持连接、不关 socket。
        {
            let s = sessions_handle.clone();
            dispatcher.set_disconnector(Arc::new(move |peer, _reason| {
                s.remove_connection(&peer);
            }));
        }

        // 6. 创建 Gossip 引擎
        let gossip_engine = Arc::new(GossipEngine::new(
            sessions_handle.clone(),
            config.clone(),
            identity.node_id,
            metrics.clone(),
            shutdown_tx.clone(),
        ));

        // 7. 创建发现服务
        let discovery = Arc::new(DiscoveryService::new(
            sessions_handle.clone(),
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
            sessions_handle.clone(),
            identity.clone(),
            config.clone(),
            metrics.clone(),
        ));

        // 9. 创建同步管理器（含 PeerSync / InfohashSync / TrackerSync）
        let sync_manager = Arc::new(SyncManager::new(
            sessions_handle.clone(),
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
            sessions_handle.clone(),
            node_table.clone(),
            identity.clone(),
            udp_transport.clone(),
            metrics.clone(),
            shutdown_tx.clone(),
        ));

        // 12. 注入循环依赖
        //     三个业务对象注入**分派器**：翻转后分派由 `SessionEvent` 驱动，
        //     连接层（SDK）不持有任何业务对象 —— 这正是 K3 要的结果。
        dispatcher.set_discovery(discovery.clone());
        dispatcher.set_sync_manager(sync_manager.clone());
        dispatcher.set_signaling_service(signaling_service.clone());

        // 13. 创建 DHT 魔法 infohash 发现服务（从 DiscoveryService 迁移至此，由 TaskScheduler 统一调度）
        let dht_discovery = if config.dht_discovery_enabled {
            Some(Arc::new(DhtDiscoveryService::new(
                node_table.clone(),
                config.listen_port,
                config.dht_discovery_interval_secs,
                shutdown_tx.clone(),
                Some(node_repo.clone()),
                dht_discoverer.clone(),
                Some(sessions_handle.clone()),
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
            dispatcher,
            sessions: sessions_handle,
            peer_caps,
            peer_query_store,
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
            dns: pnos_net::dns::DnsConfig::default(),
        })
    }

    /// 注入 DNS 解析配置
    ///
    /// 决定 iroh 端点用哪个解析器解析 pkarr / DnsAddressLookup / DERP 主机名。
    /// 不调用时使用内置公共 DNS（不读宿主系统 DNS 配置）。
    pub fn with_dns(mut self, dns: pnos_net::dns::DnsConfig) -> Self {
        self.dns = dns;
        self
    }

    /// 初始化 NetAgent（Iroh+TCP 传输层）并绑定 SDK 会话层
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
                alpn: b"pnos/federation/1".to_vec(),
                // 注入内置 DNS 配置：iroh 默认解析器会读宿主系统 DNS 配置，
                // 宿主解析器不可用时 pkarr / DnsAddressLookup / DERP 全部握不上。
                dns: self.dns.clone(),
            })
        } else {
            None
        };

        let net_config = pnos_net::NetAgentConfig {
            node_id: self.identity.node_id.0,
            listen_port: self.config.listen_port,
            api_port: self.config.api_port,
            data_dir: self.data_dir.clone(),
            // 取联邦层 LPD 端口（与 `discovery.rs` 实际启动的 LPD 服务保持一致）。
            // 注意：`lpd_enabled=false` 时该字段当前未被 NetAgent 使用，
            // 但保持值一致可避免后续启用时踩到「两个同名 lpd_multicast_port」的坑。
            lpd_multicast_port: self.config.federation_lpd_multicast_port,
            lpd_enabled: false, // 联邦层 LPD 由 discovery.rs 自行启动，不用 NetAgent 内建
            peer_cache_enabled: false, // peer cache 由 discovery.rs 自行持有
            nat_enabled: false, // UPnP/NAT 由业务层 nat_service 自行驱动
            hole_punch_enabled: false, // 打洞当前由业务层信令 + 外部执行，不启用 SDK 内建
            connect_config: Default::default(),
            transport_mode: mode,
            iroh_config,
        };

        let net_agent = pnos_net::NetAgent::new(net_config).await?;
        net_agent.start_transport_only().await?;

        // 把 SDK 会话层绑到 NetAgent：**SDK 独占监听端口**（`listen = true`）
        //
        // 翻转后入站 accept / 握手 / 保活 / 候选拨号全部由 SDK 的 `SessionManager`
        // 承担，pdc 侧不再有连接状态；业务分派由下方 `spawn_event_loop` 驱动。
        let sessions = bind_federation_sessions(
            &self.config,
            net_agent,
            self.identity.clone(),
            self.node_table.clone(),
            self.metrics.clone(),
            self.peer_caps.clone(),
            true,
        )
        .await?;
        let sessions = Arc::new(sessions);
        self.sessions.bind_once(sessions.clone())?;
        info!(
            "[federation] SDK 会话层已绑定并独占监听端口 {}（listen=true）",
            self.config.listen_port
        );

        // 启动业务分派事件循环：订阅 `SessionEvent`，替代原
        // `ConnectionManager::spawn_message_handler` 的字节回调路径。
        self.dispatcher.clone().spawn_event_loop(sessions);

        Ok(())
    }

    /// 启动所有后台任务
    pub async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        info!("[federation] 启动联邦服务（阶段2）...");

        // 0. 创建并注入 NetAgent（Iroh+TCP 传输层）
        self.init_net_agent().await?;

        // 1. TCP 监听由 SDK `SessionManager` 在 `init_net_agent` 内独占（pdc 侧无监听）

        // 2. 心跳任务已迁移到 TaskScheduler（fed_heartbeat）

        // 3. PEX 交换任务已迁移到 TaskScheduler（fed_pex_exchange）

        // 3.1 连接维护任务已迁移到 TaskScheduler（fed_connection_maintain）

        // 4. NAT 地址刷新任务已迁移到 TaskScheduler（fed_nat_refresh）

        // 4.1 启动时先执行一次 STUN 探测，确保 setup_mapping 能拿到 STUN 结果
        //     否则首次 setup_mapping 时 last_stun 为 None，reachability 会误判为 Unknown
        //     使用 spawn_blocking + 3秒超时，避免 STUN 无响应时阻塞 tokio 运行时
        //     先在异步侧用内置 DNS 池把服务器域名解析成 ip:port（不读系统 DNS），
        //     再把字面量交给阻塞线程，阻塞线程内部不再做任何解析。
        let stun_servers = self.nat_integration.resolved_stun_servers().await;
        let nat_clone = self.nat_integration.clone();
        let stun_handle = tokio::task::spawn_blocking(move || {
            nat_clone.stun_probe_with(&stun_servers);
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

        // 7. Gossip 传播任务已迁移到 TaskScheduler（fed_gossip_propagation）

        // 7.1 Merkle 反熵任务已迁移到 TaskScheduler（fed_merkle_anti_entropy）

        // 7.2 Push-Pull Gossip 任务已随 P1-9 整体移除（稳态改由 oplog delta 通道驱动）
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
        self.sessions.shutdown_all().await;

        info!("[federation] 联邦服务已关闭");
    }

    /// 生成完整状态快照（用于 REST API）
    pub fn snapshot(&self) -> FederationSnapshot {
        let status = self.status();

        // 连接列表
        let conns = self.sessions.all_connections();
        let _now = std::time::Instant::now();
        let connections: Vec<ConnectionInfo> = conns
            .iter()
            .map(|c| ConnectionInfo {
                node_id: c.node_id.to_hex(),
                addr: c.addr().map(|a| a.to_string()).unwrap_or_default(),
                connected_at_secs: c.connected_secs(),
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

        let brief = self.sync_manager.sync_brief();
        FederationStatus {
            enabled: self.config.enabled,
            node_id: self.identity.node_id.to_hex(),
            connections: self.sessions.connection_count(),
            known_nodes: self.node_table.len(),
            reachability,
            uptime_secs: self.started_at.elapsed().as_secs(),
            gossip_queue_size: self.gossip_engine.outbox_size(),
            relay_channels: self.relay_manager.active_channel_count(),
            tracker_sync_enabled: self.config.sync_tracker_enabled,
            oplog_len: brief.oplog_len,
            metrics: self.metrics.snapshot(),
            // Repo 实际总数由 handler 从 AppState 填充，此处先置 0
            node_repo_total: 0,
            node_repo_hot_total: 0,
            peer_repo_total: 0,
            peer_repo_hot_total: 0,
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

        let conns = self.sessions.all_connections();
        if conns.is_empty() {
            return Vec::new();
        }

        // 清空该 infohash 的旧响应，避免上一次查询残留
        self.peer_query_store.clear(infohash);

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
        let entries = self.peer_query_store.take(infohash);
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
            .field("connections", &self.sessions.connection_count())
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
        assert_eq!(service.sessions.connection_count(), 0);
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
            oplog_len: 0,
            metrics: FederationMetricsSnapshot::default(),
            node_repo_total: 0,
            node_repo_hot_total: 0,
            peer_repo_total: 0,
            peer_repo_hot_total: 0,
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
