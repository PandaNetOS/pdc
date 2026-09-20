//! 节点发现服务
//!
//! 负责种子节点引导、节点列表交换（PEX）、DHT 魔法 infohash 自动发现和新节点发现。
//!
//! 零配置自动连接流程：
//! 1. 启动时从磁盘缓存加载历史节点并尝试连接
//! 2. 连接配置的 seed_nodes（如有）
//! 3. 启动 DHT 魔法 infohash 发现，自动发现其他 PDC 节点
//! 4. 定期将已连接节点同步到磁盘缓存

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::federation::config::FederationConfig;
use crate::federation::node_id::{NodeAddress, NodeId, NodeIdentity, Reachability};
use crate::federation::node_table::NodeTable;
use crate::federation::peer_conn::PeerConn;
use crate::federation::protocol::*;
use crate::federation::session::SessionsHandle;
use crate::storage::node_repo::NodeRepoImpl;
use pnos_net::discovery::lpd::LpdDiscoveryService;
use pnos_net::discovery::mqtt::MqttDiscoveryService;
use pnos_net::discovery::peer_cache::PeerCache;
use pnos_net::types::DiscoveredNode;

/// 节点发现服务
pub struct DiscoveryService {
    /// 连接管理器
    sessions: Arc<SessionsHandle>,
    /// 节点表
    node_table: Arc<NodeTable>,
    /// 主爬虫节点库
    _node_repo: Option<Arc<NodeRepoImpl>>,
    /// 主爬虫 DHT 发现器
    _dht_discoverer: Option<Arc<crate::discoverers::dht::DhtDiscoverer>>,
    /// 节点身份
    identity: Arc<NodeIdentity>,
    /// 配置
    config: FederationConfig,
    /// API/HTTP 监控端口（实际分配值，LPD 广播时携带）
    api_port: u16,
    /// 关闭信号
    shutdown: broadcast::Sender<()>,
    /// 数据目录（用于缓存文件）
    data_dir: PathBuf,
    /// 节点缓存（持久化到磁盘）
    peer_cache: RwLock<PeerCache>,
    /// NAT 映射后的公网地址（MQTT Rendezvous 上报时优先使用）
    public_addr: RwLock<Option<SocketAddr>>,
}

impl DiscoveryService {
    /// 创建发现服务
    ///
    /// 如果 `config.peer_cache_enabled` 为 true，会从 `data_dir/federation_peers.json` 加载历史节点缓存。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sessions: Arc<SessionsHandle>,
        node_table: Arc<NodeTable>,
        identity: Arc<NodeIdentity>,
        config: FederationConfig,
        api_port: u16,
        shutdown: broadcast::Sender<()>,
        data_dir: &Path,
        _node_repo: Option<Arc<NodeRepoImpl>>,
        _dht_discoverer: Option<Arc<crate::discoverers::dht::DhtDiscoverer>>,
    ) -> Self {
        let peer_cache = if config.peer_cache_enabled {
            PeerCache::load(data_dir)
        } else {
            PeerCache::default()
        };

        Self {
            sessions,
            node_table,
            identity,
            config,
            api_port,
            shutdown,
            data_dir: data_dir.to_path_buf(),
            peer_cache: RwLock::new(peer_cache),
            _node_repo,
            _dht_discoverer,
            public_addr: RwLock::new(None),
        }
    }

    /// 设置 NAT 映射后的公网地址（MQTT Rendezvous 上报时优先使用）
    pub fn set_public_addr(&self, addr: Option<SocketAddr>) {
        *self.public_addr.write() = addr;
        if let Some(a) = addr {
            info!("[federation] Discovery 公网地址已更新: {}", a);
        }
    }

    /// 引导连接：缓存节点 → 种子节点 → DHT 自动发现
    ///
    /// 新流程：
    /// 1. 从缓存加载历史节点到 NodeTable
    /// 2. 并发连接缓存中成功率最高的前 8 个节点
    /// 3. 连接 config.seed_nodes（原有逻辑）
    /// 4. 启动 DHT 魔法 infohash 发现后台任务
    /// 5. 启动 peer_cache 定期保存任务（每 5 分钟）
    pub async fn bootstrap(self: Arc<Self>) {
        // 1. 从缓存加载历史节点到 NodeTable
        if self.config.peer_cache_enabled {
            let cached_addrs = {
                let cache = self.peer_cache.read();
                cache.top_addrs(8)
            };
            if !cached_addrs.is_empty() {
                info!("[federation] 从缓存加载 {} 个历史节点", cached_addrs.len());
                for addr_str in &cached_addrs {
                    if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                        let temp_id = NodeId::random();
                        self.node_table.add_or_update(NodeAddress {
                            node_id: temp_id.0,
                            ipv4_addr: if addr.is_ipv4() { Some(addr) } else { None },
                            ipv6_addr: if addr.is_ipv6() { Some(addr) } else { None },
                            reachability: Reachability::Unknown,
                            last_seen: 0,
                            nat_type: None,
                        });
                    }
                }

                // 2. 并发连接缓存中的历史节点
                for addr_str in &cached_addrs {
                    if addr_str.parse::<SocketAddr>().is_ok() {
                        let self_clone = self.clone();
                        let addr = addr_str.clone();
                        tokio::spawn(async move {
                            if let Err(e) = self_clone.connect_cached_node(&addr).await {
                                debug!("[federation] 缓存节点 {} 连接失败: {}", addr, e);
                            }
                        });
                    }
                }
            }
        }

        // 3. 连接配置的种子节点
        if !self.config.seed_nodes.is_empty() {
            info!(
                "[federation] 开始引导，共 {} 个种子节点",
                self.config.seed_nodes.len()
            );

            for seed in &self.config.seed_nodes {
                let self_clone = self.clone();
                let seed = seed.clone();
                tokio::spawn(async move {
                    if let Err(e) = self_clone.connect_seed(&seed).await {
                        warn!("[federation] 种子节点 {} 连接失败: {}", seed, e);
                    }
                });
            }
        }

        // 4. DHT 魔法 infohash 发现已迁移到 FederationService 统一管理（由 TaskScheduler 调度）

        // 4.1 启动 LPD 局域网多播发现（零配置核心：同网段节点自动发现）
        // pnos-net 的 LPD 通过事件输出发现结果，此处订阅后加入 NodeTable
        let (discovered_tx, mut discovered_rx) = broadcast::channel::<DiscoveredNode>(64);
        let lpd_service = Arc::new(LpdDiscoveryService::new(
            self.identity.node_id.0,
            self.config.listen_port,
            self.api_port,
            self.config.federation_lpd_multicast_port,
            discovered_tx.clone(),
            self.shutdown.clone(),
        ));
        lpd_service.spawn();

        // 4.1.1 启动 MQTT Rendezvous 发现（公网零配置主通道）
        // 通过 UDP connect 公共地址获取出站 IP
        let mut my_addresses: Vec<SocketAddr> = Vec::new();
        // 优先上报公网地址（NAT 映射后），外网节点可直连
        if let Some(public) = *self.public_addr.read() {
            my_addresses.push(public);
        }
        // 其次上报局域网地址，同局域网节点可低延迟直连
        if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0") {
            if let Ok(()) = socket.connect("8.8.8.8:80") {
                if let Ok(local_addr) = socket.local_addr() {
                    let lan_addr = SocketAddr::new(local_addr.ip(), self.config.listen_port);
                    if !my_addresses.contains(&lan_addr) {
                        my_addresses.push(lan_addr);
                    }
                }
            }
        }
        let mqtt_service = Arc::new(MqttDiscoveryService::new(
            self.identity.node_id.0,
            self.config.listen_port,
            self.api_port,
            my_addresses.clone(),
            discovered_tx.clone(),
            self.shutdown.clone(),
        ));
        mqtt_service.spawn();
        info!(
            "[federation] MQTT Rendezvous 发现已启动，本地地址: {:?}",
            my_addresses
        );

        // 4.2 订阅 LPD/MQTT 发现事件，加入节点表并立即触发连接
        let self_clone = self.clone();
        tokio::spawn(async move {
            loop {
                match discovered_rx.recv().await {
                    Ok(node) => {
                        info!(
                            "[federation] 收到发现事件: source={}, node_id={}, addrs={:?}",
                            node.source,
                            hex::encode(node.node_id.0),
                            node.addresses
                        );
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        let new_nodes: Vec<crate::federation::node_id::NodeAddress> = node
                            .addresses
                            .iter()
                            .map(|addr| crate::federation::node_id::NodeAddress {
                                node_id: node.node_id.0,
                                ipv4_addr: if addr.is_ipv4() { Some(*addr) } else { None },
                                ipv6_addr: if addr.is_ipv6() { Some(*addr) } else { None },
                                reachability: crate::federation::node_id::Reachability::Unknown,
                                last_seen: now,
                                nat_type: None,
                            })
                            .collect();
                        self_clone.process_new_nodes(new_nodes);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        });

        // 5. 启动 peer_cache 定期保存任务
        if self.config.peer_cache_enabled {
            self.clone().spawn_peer_cache_saver();
        }

        // 5.1 启动定期连接维护任务（主动连接 node_table 中未连接的节点）
        self.clone().spawn_connection_maintainer();

        if self.config.seed_nodes.is_empty() {
            info!("[federation] 零配置模式：依赖 LPD 局域网多播 + DHT 魔法 infohash 自动发现（seed_nodes 为空）");
        }
    }

    /// 连接单个缓存节点（地址已是 ip:port，无需 DNS 解析）
    async fn connect_cached_node(self: Arc<Self>, addr: &str) -> anyhow::Result<()> {
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| anyhow::anyhow!("缓存节点地址解析失败 {}: {}", addr, e))?;

        let temp_id = NodeId::random();
        match self.sessions.clone().connect_to(temp_id, socket_addr).await {
            Ok(conn) => {
                info!(
                    "[federation] 缓存节点连接成功: {} ({})",
                    conn.node_id, socket_addr
                );
                // 连接成功后发送 GetNodes 获取更多节点
                let req = GetNodesMessage { count: 32 };
                if let Err(e) = conn.send_message(MessageType::GetNodes, &req).await {
                    warn!("[federation] 发送 GetNodes 失败: {}", e);
                }
                Ok(())
            }
            Err(e) => {
                debug!("[federation] 缓存节点 {} 连接失败: {}", socket_addr, e);
                Err(e)
            }
        }
    }

    /// 连接单个种子节点
    async fn connect_seed(self: Arc<Self>, seed: &str) -> anyhow::Result<()> {
        // 解析 DNS
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host(seed)
            .await
            .map_err(|e| anyhow::anyhow!("DNS 解析失败 {}: {}", seed, e))?
            .collect();

        if addrs.is_empty() {
            anyhow::bail!("DNS 解析无结果: {}", seed);
        }

        // 尝试每个地址
        for addr in addrs {
            // 种子节点的 node_id 未知，先用随机 ID 占位，握手后会更新
            let temp_id = NodeId::random();
            self.node_table.add_or_update(NodeAddress {
                node_id: temp_id.0,
                ipv4_addr: if addr.is_ipv4() { Some(addr) } else { None },
                ipv6_addr: if addr.is_ipv6() { Some(addr) } else { None },
                reachability: Reachability::Unknown,
                last_seen: 0,
                nat_type: None,
            });

            match self.sessions.clone().connect_to(temp_id, addr).await {
                Ok(conn) => {
                    info!("[federation] 种子节点连接成功: {} ({})", conn.node_id, addr);
                    // 连接成功后发送 GetNodes
                    let req = GetNodesMessage { count: 32 };
                    if let Err(e) = conn.send_message(MessageType::GetNodes, &req).await {
                        warn!("[federation] 发送 GetNodes 失败: {}", e);
                    }
                    return Ok(());
                }
                Err(e) => {
                    debug!("[federation] 种子节点地址 {} 连接失败: {}", addr, e);
                }
            }
        }

        anyhow::bail!("所有地址均连接失败: {}", seed)
    }

    /// 处理 GetNodes 请求：返回本地最活跃的节点
    pub async fn handle_get_nodes(&self, conn: &PeerConn, count: u16) {
        let nodes = self.node_table.top_active_nodes(count as usize);
        let addresses: Vec<NodeAddress> = nodes
            .into_iter()
            .filter(|n| NodeId(n.info.node_id) != conn.node_id) // 不返回请求方自己
            .map(|n| n.info)
            .collect();

        let msg = NodesMessage { nodes: addresses };
        if let Err(e) = conn.send_message(MessageType::Nodes, &msg).await {
            debug!("[federation] 发送 Nodes 失败: {}", e);
        }
    }

    /// 处理收到的节点列表
    pub fn handle_nodes_received(&self, nodes: Vec<NodeAddress>) {
        self.process_new_nodes(nodes);
    }

    /// 处理 ExchangeNodes 消息
    pub fn handle_exchange_nodes(&self, nodes: Vec<NodeAddress>) {
        self.process_new_nodes(nodes);
    }

    /// 处理新发现的节点：加入节点表，尝试连接未连接的
    fn process_new_nodes(&self, nodes: Vec<NodeAddress>) {
        let mut new_count = 0;
        for node_info in &nodes {
            // 跳过自己
            if NodeId(node_info.node_id) == self.identity.node_id {
                continue;
            }
            if self.node_table.add_or_update(node_info.clone()) {
                new_count += 1;
            }
        }

        if new_count > 0 {
            debug!(
                "[federation] 发现 {} 个新节点，节点表总数: {}",
                new_count,
                self.node_table.len()
            );
        }

        // 尝试连接未连接的节点（不超过 target_neighbors）
        let connected = self.node_table.connected_count();
        if connected < self.config.target_neighbors {
            let need = self.config.target_neighbors - connected;
            // 按活跃度降序排序，优先连接活跃节点；尝试 need*3 个候选，避免只选到死节点
            let mut candidates = self
                .node_table
                .all_nodes()
                .into_iter()
                .filter(|e| {
                    e.status != crate::federation::node_table::NodeStatus::Connected
                        && e.info.preferred_addr().is_some()
                })
                .collect::<Vec<_>>();
            candidates.sort_by(|a, b| {
                b.activity_score()
                    .partial_cmp(&a.activity_score())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let candidates = candidates.into_iter().take(need * 3).collect::<Vec<_>>();

            for entry in candidates {
                if let Some(addr) = entry.info.preferred_addr() {
                    let cm = self.sessions.clone();
                    let node_id = NodeId(entry.info.node_id);
                    tokio::spawn(async move {
                        if let Err(e) = cm.connect_to(node_id, addr).await {
                            debug!("[federation] 自动连接 {} 失败: {}", node_id, e);
                        }
                    });
                }
            }
        }
    }

    /// 启动 PEX 交换后台任务（已迁移到 TaskScheduler，此方法保留兼容但不再被调用）
    pub fn spawn_pex_exchange(self: Arc<Self>) {
        // 已迁移到 TaskScheduler
    }

    /// 启动连接维护后台任务（已迁移到 TaskScheduler，此方法保留兼容但不再被调用）
    pub fn spawn_connection_maintainer(self: Arc<Self>) {
        // 已迁移到 TaskScheduler
    }

    /// 启动节点缓存定期保存任务（已迁移到 TaskScheduler，此方法保留兼容但不再被调用）
    pub fn spawn_peer_cache_saver(self: Arc<Self>) {
        // 已迁移到 TaskScheduler
    }

    /// 节点缓存保存单次执行：同步已连接节点状态 → 清理过期 → 写入磁盘
    pub async fn peer_cache_save_tick(self: Arc<Self>) {
        self.sync_from_node_table();

        let cache = self.peer_cache.read().clone();
        if let Err(e) = cache.save(&self.data_dir) {
            warn!("[federation] 节点缓存保存失败: {}", e);
        }
    }

    /// 将 NodeTable 中已连接节点的状态同步到缓存
    ///
    /// 遍历所有 Connected 状态的节点，更新其 success_count 和 last_seen，
    /// 然后按配置的 max_nodes 清理过期节点。
    fn sync_from_node_table(&self) {
        let connected = self.node_table.connected_nodes();
        let mut cache = self.peer_cache.write();
        for entry in &connected {
            if let Some(addr) = entry.info.preferred_addr() {
                cache.upsert(&entry.info.node_id, &addr.to_string(), true);
            }
        }
        cache.prune(self.config.peer_cache_max_nodes);
    }

    /// 连接维护单次执行
    pub async fn connection_maintainer_tick(self: Arc<Self>) {
        // 1. 重置卡住的 Connecting 状态
        let stale = self.node_table.reset_stale_connecting();
        if stale > 0 {
            debug!("[federation] 重置了 {} 个卡住的 Connecting 状态", stale);
        }

        // 2. 如果连接数少于 target_neighbors，尝试连接未连接的节点
        let connected = self.node_table.connected_count();
        if connected >= self.config.target_neighbors {
            return;
        }

        let need = self.config.target_neighbors - connected;

        // 优先连接种子节点（如果种子节点未连接）
        for seed in &self.config.seed_nodes {
            if need == 0 {
                break;
            }
            if let Ok(addr) = seed.parse::<SocketAddr>() {
                // 检查是否已连接（优先 node_id 匹配，兼容入站连接临时端口场景）
                let already_connected = self.sessions.is_seed_connected(addr);
                if !already_connected {
                    let cm = self.sessions.clone();
                    let temp_id = NodeId::random();
                    tokio::spawn(async move {
                        if let Err(e) = cm.connect_to(temp_id, addr).await {
                            debug!("[federation] 维护重连种子节点 {} 失败: {}", addr, e);
                        }
                    });
                }
            }
        }

        // 3. 连接节点表中其他未连接的节点
        // 双重检查：node_table 状态 + connections map，避免入站连接未更新状态时被误重连
        let connected_ids: std::collections::HashSet<NodeId> = self
            .sessions
            .all_connections()
            .iter()
            .map(|c| c.node_id)
            .collect();

        // 按活跃度降序排序，优先连接活跃节点；尝试 need*3 个候选，避免只选到死节点
        let mut candidates = self
            .node_table
            .all_nodes()
            .into_iter()
            .filter(|e| {
                e.status != crate::federation::node_table::NodeStatus::Connected
                    && e.info.preferred_addr().is_some()
                    && !connected_ids.contains(&NodeId(e.info.node_id))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|a, b| {
            b.activity_score()
                .partial_cmp(&a.activity_score())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let candidates = candidates.into_iter().take(need * 3).collect::<Vec<_>>();

        for entry in candidates {
            if let Some(addr) = entry.info.preferred_addr() {
                let cm = self.sessions.clone();
                let node_id = NodeId(entry.info.node_id);
                tokio::spawn(async move {
                    if let Err(e) = cm.connect_to(node_id, addr).await {
                        debug!("[federation] 维护重连 {} 失败: {}", node_id, e);
                    }
                });
            }
        }
    }

    /// PEX 交换单次执行
    pub async fn pex_exchange_tick(self: Arc<Self>) {
        let conns = self.sessions.all_connections();
        if conns.is_empty() {
            return;
        }

        // 获取本地已知节点（排除自己和已连接的对端）
        let known_nodes: Vec<NodeAddress> = self
            .node_table
            .top_active_nodes(32)
            .into_iter()
            .map(|e| e.info)
            .collect();

        if known_nodes.is_empty() {
            return;
        }

        let conn_count = conns.len();
        for conn in &conns {
            // 过滤掉对端自己
            let nodes: Vec<NodeAddress> = known_nodes
                .iter()
                .filter(|n| NodeId(n.node_id) != conn.node_id)
                .cloned()
                .collect();

            if nodes.is_empty() {
                continue;
            }

            let msg = ExchangeNodesMessage { nodes };
            if let Err(e) = conn.send_message(MessageType::ExchangeNodes, &msg).await {
                debug!("[federation] PEX 交换发送失败 to {}: {}", conn.node_id, e);
            }
        }

        debug!(
            "[federation] PEX 交换完成，向 {} 个连接发送了节点信息",
            conn_count
        );
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::node_table::NodeTable;

    fn make_test_config() -> FederationConfig {
        FederationConfig {
            enabled: true,
            listen_port: 0,
            max_connections: 10,
            target_neighbors: 4,
            seed_nodes: vec![],
            ..Default::default()
        }
    }

    fn make_node_address(id: u8, port: u16) -> NodeAddress {
        NodeAddress {
            node_id: [id; 20],
            ipv4_addr: Some(format!("127.0.0.1:{}", port).parse().unwrap()),
            ipv6_addr: None,
            reachability: Reachability::Mapped,
            last_seen: 100,
            nat_type: None,
        }
    }

    fn test_data_dir() -> PathBuf {
        // 每个测试用**唯一**子目录：本函数被 5 个测试共用，若都落在同一路径，并发执行时
        // 彼此的 remove_dir_all / create_dir_all 会互相竞争，在 Windows 上表现为
        // create_dir_all 偶发失败（flaky）。加进程内自增序号即消除竞争。
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pdc_disc_test_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn test_handle_nodes_received() {
        let dir = test_data_dir();
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let cm = SessionsHandle::new_for_test();
        let discovery = DiscoveryService::new(
            cm,
            node_table.clone(),
            identity,
            make_test_config(),
            0,
            shutdown_tx,
            &dir,
            None,
            None,
        );

        let nodes = vec![
            make_node_address(1, 6885),
            make_node_address(2, 6886),
            make_node_address(3, 6887),
        ];
        discovery.handle_nodes_received(nodes);
        assert_eq!(node_table.len(), 3);

        // 重复添加不应增加
        discovery.handle_nodes_received(vec![make_node_address(1, 6885)]);
        assert_eq!(node_table.len(), 3);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_handle_exchange_nodes() {
        let dir = test_data_dir();
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let cm = SessionsHandle::new_for_test();
        let discovery = DiscoveryService::new(
            cm,
            node_table.clone(),
            identity,
            make_test_config(),
            0,
            shutdown_tx,
            &dir,
            None,
            None,
        );

        let nodes = vec![make_node_address(5, 6890), make_node_address(6, 6891)];
        discovery.handle_exchange_nodes(nodes);
        assert_eq!(node_table.len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_skip_self_in_nodes() {
        let dir = test_data_dir();
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let cm = SessionsHandle::new_for_test();
        let discovery = DiscoveryService::new(
            cm,
            node_table.clone(),
            identity.clone(),
            make_test_config(),
            0,
            shutdown_tx,
            &dir,
            None,
            None,
        );

        // 包含自己的节点
        let mut self_addr = make_node_address(0, 6885);
        self_addr.node_id = identity.node_id.0;
        let nodes = vec![self_addr, make_node_address(1, 6886)];
        discovery.handle_nodes_received(nodes);
        // 自己不应被加入
        assert_eq!(node_table.len(), 1);
        assert!(node_table.get(&identity.node_id).is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_bootstrap_no_seeds() {
        let dir = test_data_dir();
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let cm = SessionsHandle::new_for_test();
        let discovery = Arc::new(DiscoveryService::new(
            cm,
            node_table,
            identity,
            make_test_config(),
            0,
            shutdown_tx,
            &dir,
            None,
            None,
        ));

        // 没有种子节点，应该立即返回（但会启动 DHT 发现和缓存保存）
        discovery.bootstrap().await;

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_sync_from_node_table() {
        let dir = test_data_dir();
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let cm = SessionsHandle::new_for_test();
        let discovery = DiscoveryService::new(
            cm,
            node_table.clone(),
            identity,
            make_test_config(),
            0,
            shutdown_tx,
            &dir,
            None,
            None,
        );

        // 添加节点并标记为已连接
        node_table.add_or_update(make_node_address(1, 6885));
        node_table.mark_connected(&NodeId([1; 20]), Some(50));

        // 同步到缓存
        discovery.sync_from_node_table();

        let cache = discovery.peer_cache.read();
        assert_eq!(cache.nodes.len(), 1);
        assert_eq!(cache.nodes[0].success_count, 1);
        assert_eq!(cache.nodes[0].addr, "127.0.0.1:6885");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
