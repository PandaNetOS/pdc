//! 节点发现服务
//!
//! 负责种子节点引导、节点列表交换（PEX）、DHT 魔法 infohash 自动发现和新节点发现。
//!
//! 零配置自动连接流程：
//! 1. 启动时从磁盘缓存加载历史节点并尝试连接
//! 2. 连接配置的 seed_nodes（如有）
//! 3. 启动 DHT 魔法 infohash 发现，自动发现其他 PDC 节点
//! 4. 定期将已连接节点同步到磁盘缓存

use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::federation::config::FederationConfig;
use crate::federation::node_id::{NodeAddress, NodeId, NodeIdentity};
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
    /// DNS 解析池（进程级共享，内置公共 DNS，不读宿主系统 DNS）
    dns_pool: Arc<crate::dns_pool::DnsPool>,
    /// P1-5：地址级拨号在途表（addr → 发起时刻）。TTL 内同一地址只拨一次，
    /// 消除缓存引导 / 发现 / 维护等多条路径对同一地址的并发重复拨号
    /// （双拨 → 仲裁 → 断连震荡的来源）。
    dialing: Arc<RwLock<std::collections::HashMap<SocketAddr, std::time::Instant>>>,
}

/// 拆分 `host:port` 形式的种子地址
///
/// 支持 `host:port`、`ip:port`、`[v6]:port` 与裸 `ip`；缺端口时回退
/// `default_port`。用于把种子地址喂给 DnsPool 解析。
fn split_host_port(seed: &str, default_port: u16) -> (String, u16) {
    let seed = seed.trim();

    // 裸 IP（含 IPv6 无括号形式）：无需解析域名
    if let Ok(ip) = seed.parse::<IpAddr>() {
        return (ip.to_string(), default_port);
    }

    // [v6]:port
    if let Some(rest) = seed.strip_prefix('[') {
        if let Some((host, tail)) = rest.split_once(']') {
            let port = tail
                .strip_prefix(':')
                .and_then(|p| p.parse::<u16>().ok())
                .unwrap_or(default_port);
            return (host.to_string(), port);
        }
    }

    match seed.rsplit_once(':') {
        Some((host, p)) => match p.parse::<u16>() {
            Ok(port) => (host.to_string(), port),
            Err(_) => (seed.to_string(), default_port),
        },
        None => (seed.to_string(), default_port),
    }
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
            dns_pool: crate::dns_pool::global(),
            dialing: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// 覆盖 DNS 解析池（默认用进程级共享实例）
    pub fn with_dns_pool(mut self, pool: Arc<crate::dns_pool::DnsPool>) -> Self {
        self.dns_pool = pool;
        self
    }

    /// 设置 NAT 映射后的公网地址（MQTT Rendezvous 上报时优先使用）
    pub fn set_public_addr(&self, addr: Option<SocketAddr>) {
        *self.public_addr.write() = addr;
        if let Some(a) = addr {
            info!("[federation] Discovery 公网地址已更新: {}", a);
        }
    }

    /// P1-5：拨号在途 TTL（同一地址在该时间内只允许一次主动拨号）。
    const DIAL_INFLIGHT_TTL: std::time::Duration = std::time::Duration::from_secs(10);

    /// 尝试占据某地址的拨号名额：TTL 内已在拨号返回 false（调用方应跳过）。
    /// 顺带清理过期的在途记录。
    fn begin_dial(&self, addr: SocketAddr) -> bool {
        let mut d = self.dialing.write();
        let now = std::time::Instant::now();
        d.retain(|_, t| now.duration_since(*t) < Self::DIAL_INFLIGHT_TTL);
        if d.contains_key(&addr) {
            return false;
        }
        d.insert(addr, now);
        true
    }

    /// 拨号结束（成功 / 失败均调用），释放该地址的在途名额。
    fn end_dial(&self, addr: SocketAddr) {
        self.dialing.write().remove(&addr);
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
                // 不再预登记随机 temp_id：握手成功后由认证回调（adopt_addresses）
                // 统一登记真实 node_id，避免 temp_id 占位记录残留为垃圾节点
                // （反复连自己/连旧节点、重复拨号的根因）。

                // 1. 并发连接缓存中的历史节点
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
                        // v10(F7)：过滤本端地址 —— 同 NAT 后多节点经 STUN 会得到**相同的
                        // 公网映射**（同一公网 IP:port 不可能同时映射两台内网机），互把对方
                        // 广播里的该地址当可达目标，连接经网关 hairpin 后要么连回自己
                        // （自连接拒绝）、要么以次优路径连到对方（被拒），形成每 30s 一次的
                        // 回环连接风暴（2026-09-27 实测 52/58）。本端监听地址与公网映射
                        // 地址一律不入节点表、不触发连接。
                        // v11：本端地址集合构建提取为 `local_addr_set()`，与节点入表路径
                        // （process_new_nodes）共用同一口径，避免两处各写一份漂移。
                        let my_addrs = self_clone.local_addr_set();
                        if NodeId(node.node_id.0) == self_clone.identity.node_id {
                            debug!("[federation] 忽略本节点自广播");
                            continue;
                        }
                        let owned_addrs: Vec<SocketAddr> = node
                            .addresses
                            .iter()
                            .copied()
                            .filter(|a| !my_addrs.contains(a))
                            .collect();
                        if owned_addrs.is_empty() {
                            debug!(
                                "[federation] 发现事件地址均为本端地址，跳过: node_id={}",
                                hex::encode(node.node_id.0)
                            );
                            continue;
                        }
                        let new_nodes: Vec<crate::federation::node_id::NodeAddress> = owned_addrs
                            .iter()
                            .map(|addr| crate::federation::node_id::NodeAddress {
                                node_id: node.node_id.0,
                                ipv4_addr: if addr.is_ipv4() { Some(*addr) } else { None },
                                ipv6_addr: if addr.is_ipv6() { Some(*addr) } else { None },
                                reachability: crate::federation::node_id::Reachability::Unknown,
                                last_seen: now,
                                nat_type: None,
                                // 携带分类地址和发现来源，供地址聚合和三级选路使用
                                endpoints: vec![pnos_net::types::NodeEndpoint {
                                    addr: *addr,
                                    kind: pnos_net::types::NodeEndpoint::classify(addr),
                                    source: node.source,
                                    last_success: now,
                                    success_count: 0,
                                    fail_count: 0,
                                    latency_ms: None,
                                }],
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

        if !self.begin_dial(socket_addr) {
            debug!("[federation] 缓存节点 {} 拨号在途，跳过", socket_addr);
            return Ok(());
        }
        let temp_id = NodeId::random();
        let result = self.sessions.clone().connect_to(temp_id, socket_addr).await;
        self.end_dial(socket_addr);
        match result {
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
        // 解析 DNS：走内置 DnsPool（默认公共 DNS），不读宿主系统 DNS 配置。
        // 此前用 tokio::net::lookup_host（即 getaddrinfo → 系统 DNS），
        // 宿主解析器整台不可用时会直接把引导流程卡死。
        let (host, port) = split_host_port(seed, self.config.listen_port);
        let addrs: Vec<SocketAddr> = self
            .dns_pool
            .resolve(&host, port)
            .await
            .map_err(|e| anyhow::anyhow!("DNS 解析失败 {}: {}", seed, e))?;

        if addrs.is_empty() {
            anyhow::bail!("DNS 解析无结果: {}", seed);
        }

        // 尝试每个地址
        for addr in addrs {
            // node_id 未知：随机 ID 仅占位供 SDK 拨号；握手后 SDK 以真实 ID 注册，
            // 认证回调 adopt_addresses 把真实 ID 登记进 node_table，故不预登记 temp_id。
            if !self.begin_dial(addr) {
                continue;
            }
            let temp_id = NodeId::random();
            let result = self.sessions.clone().connect_to(temp_id, addr).await;
            self.end_dial(addr);
            match result {
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

    /// v11：构建本端地址集合 —— 身份地址快照的 preferred_addr + 公网映射地址。
    ///
    /// 同 NAT 后多节点经 STUN 会得到**相同的公网映射**（同一公网 IP:port 不可能同时
    /// 映射两台内网机），凡是命中本端集合的地址都不能作为拨号目标（hairpin 回环）。
    /// 发现事件路径（v10(F7)）与节点入表路径（process_new_nodes）共用此口径，
    /// 避免两处各写一份漂移。
    fn local_addr_set(&self) -> std::collections::HashSet<SocketAddr> {
        let mut s: std::collections::HashSet<SocketAddr> = self
            .identity
            .addresses_snapshot()
            .iter()
            .filter_map(|a| a.preferred_addr())
            .collect();
        if let Some(p) = *self.public_addr.read() {
            s.insert(p);
        }
        s
    }

    /// v11(F7 残余路径)：剥离节点条目中命中本端地址集合的地址；全部命中返回 None（整条丢弃）。
    ///
    /// 背景：PEX 交换 / GetNodes 响应交换回来的同 NAT 邻居会携带与本端一致的公网映射
    /// 地址，直连该地址经网关 hairpin：要么连回自己（自连接拒绝）、要么以次优路径连到
    /// 对方（被拒）——2026-09-27 18:20 实测一条「入站连接走了次优路径（NAT 回环）」。
    /// 口径与发现事件路径（v10(F7)）一致：只剥离命中地址、保留其余可达地址（如同网段
    /// 邻居的局域网地址仍可低延迟直连）；条目本无地址时原样返回，不改变既有行为。
    /// 纯函数（不依赖 self），便于单测。
    fn strip_local_addrs(
        n: &NodeAddress,
        my_addrs: &std::collections::HashSet<SocketAddr>,
    ) -> Option<NodeAddress> {
        let had_any = n.ipv4_addr.is_some() || n.ipv6_addr.is_some() || !n.endpoints.is_empty();
        if !had_any {
            return Some(n.clone());
        }
        let ipv4 = n.ipv4_addr.filter(|a| !my_addrs.contains(a));
        let ipv6 = n.ipv6_addr.filter(|a| !my_addrs.contains(a));
        let endpoints: Vec<pnos_net::types::NodeEndpoint> = n
            .endpoints
            .iter()
            .filter(|e| !my_addrs.contains(&e.addr))
            .cloned()
            .collect();
        if ipv4.is_none() && ipv6.is_none() && endpoints.is_empty() {
            return None; // 全部地址均为本端地址：整条跳过
        }
        let mut out = n.clone();
        out.ipv4_addr = ipv4;
        out.ipv6_addr = ipv6;
        out.endpoints = endpoints;
        // 主地址字段被剥离而 endpoints 仍有可用地址时，提升首个非本端地址为主地址，
        // 保持 preferred_addr() 可用（自动拨号候选筛选依赖它）。
        if out.ipv4_addr.is_none() && out.ipv6_addr.is_none() {
            let promoted = out
                .endpoints
                .iter()
                .map(|e| e.addr)
                .find(|a| a.is_ipv4())
                .or_else(|| out.endpoints.iter().map(|e| e.addr).find(|a| a.is_ipv6()));
            if let Some(a) = promoted {
                if a.is_ipv4() {
                    out.ipv4_addr = Some(a);
                } else {
                    out.ipv6_addr = Some(a);
                }
            }
        }
        Some(out)
    }

    /// 处理新发现的节点：加入节点表，尝试连接未连接的
    fn process_new_nodes(&self, nodes: Vec<NodeAddress>) {
        // v11(F7 残余路径)：入表前过滤本端地址 —— 节点入表/连接发起的其他入口
        // （PEX 交换回来的节点、GetNodes 响应等）携带的地址可能含与本端一致的公网映射
        // 地址（同 NAT 多节点共享同一映射），不剥离会在连通性维护时发起 hairpin
        // 自连接/次优路径连接。口径与发现事件路径（v10(F7)）一致：剥离命中地址、
        // 全部命中整条跳过。
        let my_addrs = self.local_addr_set();
        let mut new_count = 0;
        let mut local_skipped = 0;
        for node_info in &nodes {
            // 跳过自己
            if NodeId(node_info.node_id) == self.identity.node_id {
                continue;
            }
            // v11：剥离命中本端集合的地址；全部命中则整条不入表、不触发连接
            let filtered = match Self::strip_local_addrs(node_info, &my_addrs) {
                Some(f) => f,
                None => {
                    local_skipped += 1;
                    debug!(
                        "[federation] 节点地址均为本端地址，入表跳过: node_id={}",
                        hex::encode(node_info.node_id)
                    );
                    continue;
                }
            };
            if self.node_table.add_or_update(filtered) {
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
        if local_skipped > 0 {
            debug!(
                "[federation] 跳过 {} 个仅含本端地址的节点条目",
                local_skipped
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
                    !matches!(
                        e.status,
                        crate::federation::node_table::NodeStatus::Connected
                            | crate::federation::node_table::NodeStatus::Connecting
                    ) && e.info.preferred_addr().is_some()
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
                    if !self.begin_dial(addr) {
                        continue;
                    }
                    let cm = self.sessions.clone();
                    let node_id = NodeId(entry.info.node_id);
                    let dialing = self.dialing.clone();
                    tokio::spawn(async move {
                        let r = cm.connect_to(node_id, addr).await;
                        dialing.write().remove(&addr);
                        if let Err(e) = r {
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

    /// 把本节点的公网 + 局域网地址写入 PeerCache（同LAN 兜底通道）。
    ///
    /// 2026-10-09：生产观测 `connections=0` 持续 40+ 分钟，根因链是
    /// `seed_nodes` 为空 → MQTT 全部 broker 超时 + DHT 魔法 infohash 在公网
    /// 找不到其他 PDC + LPD 组播常被路由器屏蔽 → node_table 无候选 → 无连接。
    /// 这属于网络现实，但原实现缺兜底：PeerCache 只同步**已连接**节点，
    /// 连接数为 0 时缓存恒空，即便同LAN 内另有节点在跑也无法互认。
    ///
    /// 本方法让本机地址进入同一个 `data/federation_peers.json`，同网节点
    /// 启动时 `PeerCache::load` 即可读到，零外部依赖、不依赖组播可达。
    pub fn publish_local_addresses_to_cache(&self) {
        let mut addrs: Vec<SocketAddr> = Vec::new();
        if let Some(public) = *self.public_addr.read() {
            addrs.push(public);
        }
        // 用 UDP connect 探测出口 LAN 地址（不发送数据）
        if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0") {
            if socket.connect("8.8.8.8:80").is_ok() {
                if let Ok(local) = socket.local_addr() {
                    let lan = SocketAddr::new(local.ip(), self.config.listen_port);
                    if !addrs.contains(&lan) {
                        addrs.push(lan);
                    }
                }
            }
        }
        if addrs.is_empty() {
            return;
        }
        let mut cache = self.peer_cache.write();
        for a in &addrs {
            // success=false：这是「我宣布的地址」而非「验证过的对端」，
            // 不应抬高 best_addr 的优先级。
            cache.upsert(&self.identity.node_id.0, &a.to_string(), false);
        }
        drop(cache);
        if let Err(e) = self.peer_cache.read().save(&self.data_dir) {
            warn!("[federation] 本地地址发布到节点缓存失败: {}", e);
        } else {
            debug!(
                "[federation] 本地地址已发布到节点缓存: {:?}（供同 LAN 节点互认）",
                addrs
            );
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

        // 1.5 清理从未成功握手的占位（temp_id）垃圾；连接已满时也要清理，
        // 故放在 target_neighbors 的 early return 之前。
        let pruned = self
            .node_table
            .prune_never_connected(std::time::Duration::from_secs(
                self.config.never_connected_prune_secs.max(1),
            ));
        if pruned > 0 {
            debug!("[federation] 清理了 {} 个从未握手成功的占位节点", pruned);
        }

        // 2. 如果连接数少于 target_neighbors，尝试连接未连接的节点
        let connected = self.node_table.connected_count();
        if connected >= self.config.target_neighbors {
            return;
        }

        // 2026-10-09：补链失败此前全部沉在 debug 级，生产日志一条都看不到
        // （`target_neighbors` 只在启动日志里打印过一次）。联邦层因此长期是
        // 黑盒：运维只能从 /federation/status 的 connections=0 反推，
        // 无法区分「没有候选节点」与「候选连不上」。这里提到 warn 并区分成因。
        if self.config.seed_nodes.is_empty() && self.node_table.is_empty() {
            warn!(
                "[federation] 补链未达标 {}/{}：seed_nodes 为空且 node_table 无候选。\
                 零配置模式依赖 LPD 多播 + DHT 魔法 infohash + MQTT Rendezvous，\
                 三者皆不可用时联邦必然 0连接（非代码缺陷）",
                connected, self.config.target_neighbors
            );
        } else if connected < self.config.target_neighbors {
            warn!(
                "[federation] 补链未达标 {}/{}：seed_nodes={} 个，node_table 候选 {} 个",
                connected,
                self.config.target_neighbors,
                self.config.seed_nodes.len(),
                self.node_table.len()
            );
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
                if !already_connected && self.begin_dial(addr) {
                    let cm = self.sessions.clone();
                    let temp_id = NodeId::random();
                    let dialing = self.dialing.clone();
                    tokio::spawn(async move {
                        let r = cm.connect_to(temp_id, addr).await;
                        dialing.write().remove(&addr);
                        if let Err(e) = r {
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
                !matches!(
                    e.status,
                    crate::federation::node_table::NodeStatus::Connected
                        | crate::federation::node_table::NodeStatus::Connecting
                ) && e.info.preferred_addr().is_some()
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
                if !self.begin_dial(addr) {
                    continue;
                }
                let cm = self.sessions.clone();
                let node_id = NodeId(entry.info.node_id);
                let dialing = self.dialing.clone();
                tokio::spawn(async move {
                    let r = cm.connect_to(node_id, addr).await;
                    dialing.write().remove(&addr);
                    if let Err(e) = r {
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
            // 对端 TCP 不消费时 send_message 写阻塞会挂起（实测 355s+ 占联邦槽位），
            // 加 10s 超时释放槽位与 worker；超时记 warn 后自然 continue 下一个连接。
            // [ALLOWED-HARDCODED: 对端不消费时的发送兜底超时，短超时是设计意图，非业务可调参数]
            let send = tokio::time::timeout(
                tokio::time::Duration::from_secs(10),
                conn.send_message(MessageType::ExchangeNodes, &msg),
            );
            match send.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    debug!("[federation] PEX 交换发送失败 to {}: {}", conn.node_id, e);
                }
                Err(_elapsed) => {
                    warn!(
                        "[federation] PEX 交换发送超时（10s），跳过 to {} (addr={:?})",
                        conn.node_id,
                        conn.addr()
                    );
                }
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
            reachability: crate::federation::node_id::Reachability::Mapped,
            last_seen: 100,
            nat_type: None,
            endpoints: Vec::new(),
        }
    }

    fn test_data_dir() -> PathBuf {
        // 每个测试用**唯一**子目录：本函数被 5 个测试共用，若都落在同一路径，并发执行时
        // 彼此的 remove_dir_all / create_dir_all 会互相竞争，在 Windows 上表现为
        // create_dir_all 偶发失败（flaky）。加进程内自增序号即消除竞争。
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        // 测试临时根用 crate::test_tmp_dir()（构建目录 target/test-tmp）：本机安全策略
        // 拒绝 target 构建目录进程写 %TEMP% 根与数据盘（PermissionDenied code 5）。
        let dir = crate::test_tmp_dir().join(format!(
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

    // ---- v11(F7 残余路径)：strip_local_addrs 纯函数单测 ----

    /// 构造一个携带公网映射地址端点的节点条目
    fn make_endpoint(addr: SocketAddr) -> pnos_net::types::NodeEndpoint {
        pnos_net::types::NodeEndpoint {
            addr,
            kind: pnos_net::types::NodeEndpoint::classify(&addr),
            source: pnos_net::types::DiscoverySource::Pex,
            last_success: 0,
            success_count: 0,
            fail_count: 0,
            latency_ms: None,
        }
    }

    #[test]
    fn test_strip_local_addrs_all_local_skipped() {
        // 同 NAT 邻居的典型形态：仅携带与本端一致的公网映射地址 → 整条丢弃
        let pub_addr: SocketAddr = "203.0.113.7:40000".parse().unwrap();
        let mut my = std::collections::HashSet::new();
        my.insert(pub_addr);
        let mut n = make_node_address(1, 40000);
        n.ipv4_addr = Some(pub_addr);
        n.ipv6_addr = None;
        assert!(DiscoveryService::strip_local_addrs(&n, &my).is_none());
    }

    #[test]
    fn test_strip_local_addrs_partial_keeps_lan() {
        // 部分命中：剥离公网映射地址，保留同网段邻居的局域网地址并提升为主地址
        let pub_addr: SocketAddr = "203.0.113.7:40000".parse().unwrap();
        let lan_addr: SocketAddr = "192.168.1.20:6885".parse().unwrap();
        let mut my = std::collections::HashSet::new();
        my.insert(pub_addr);
        let mut n = make_node_address(1, 40000);
        n.ipv4_addr = Some(pub_addr); // 命中本端公网映射
        n.endpoints.push(make_endpoint(lan_addr)); // 局域网地址应保留
        let filtered =
            DiscoveryService::strip_local_addrs(&n, &my).expect("局域网地址应保留整条条目");
        assert_eq!(filtered.ipv4_addr, Some(lan_addr)); // 主地址由 endpoints 提升
        assert!(filtered.endpoints.iter().all(|e| e.addr != pub_addr));
        assert_eq!(filtered.endpoints.len(), 1);
    }

    #[test]
    fn test_strip_local_addrs_no_overlap_unchanged() {
        // 无命中：原样保留
        let my = std::collections::HashSet::new();
        let n = make_node_address(2, 6885);
        let filtered = DiscoveryService::strip_local_addrs(&n, &my).expect("无命中应原样保留");
        assert_eq!(filtered.ipv4_addr, n.ipv4_addr);
        assert!(filtered.endpoints.is_empty());
    }

    #[test]
    fn test_strip_local_addrs_no_address_entry_kept() {
        // 无地址条目：与本轮过滤无关，保持原样（不改变既有行为）
        let mut my = std::collections::HashSet::new();
        my.insert("203.0.113.7:40000".parse::<SocketAddr>().unwrap());
        let mut n = make_node_address(3, 6885);
        n.ipv4_addr = None;
        assert!(DiscoveryService::strip_local_addrs(&n, &my).is_some());
    }

    // ---- PEX 发送超时不 panic：语义回归 ----
    //
    // 生产代码 pex_exchange_tick 用 tokio::time::timeout(10s) 包裹 conn.send_message。
    // 现有测试基建（SessionsHandle::new_for_test）构造不出 send_message 真正挂起的
    // PeerConn（需对端 TCP 不消费的活会话），故这里用与生产完全一致的 timeout+match
    // 结构，对一个永不完结的 future 施加 50ms 超时，断言命中超时分支且不 panic。
    // 覆盖边界：验证的是"timeout 包裹 + match 三分支"的语义，而非真实网络发送路径。
    #[tokio::test]
    async fn test_pex_send_timeout_does_not_panic() {
        use std::future::pending;
        use std::time::Duration;

        // 模拟对端 TCP 不消费时 send_message 写阻塞挂起（永不完结）。
        let pending_send = async { pending::<anyhow::Result<()>>().await };

        let out = tokio::time::timeout(Duration::from_millis(50), pending_send).await;
        match out {
            Ok(Ok(())) => panic!("超时分支应被命中，此处不应完成"),
            Ok(Err(e)) => panic!("超时分支应被命中，此处不应返回发送错误: {e}"),
            Err(_elapsed) => {
                // 命中超时分支：这正是生产代码 warn! + continue 的路径。
            }
        }
    }
}
