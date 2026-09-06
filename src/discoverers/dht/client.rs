//! DHT 客户端
//!
//! 实现 BitTorrent DHT 协议（BEP 5），通过 Kademlia 分布式哈希表发现 peer。
//!
//! 实现了：
//! - Bootstrap 节点启动和路由表维护
//! - get_peers 递归查询（迭代式 Kademlia 查找）
//! - compact node info / compact peer 解析
//! - 节点健康检查和过期清理

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use parking_lot::RwLock;
use rand::Rng;
use tokio::net::UdpSocket;
use tracing::{debug, info};

use crate::traits::{AnnounceEvent, DiscovererStats, DiscovererType, PeerDiscoverer};
use crate::types::{Infohash, PeerInfo, PeerSource};

use super::message::{DhtMessage, DhtNode};

/// DHT 配置
#[derive(Debug, Clone)]
pub struct DhtConfig {
    /// Bootstrap 节点列表
    pub bootstrap_nodes: Vec<(String, u16)>,
    /// 监听端口
    pub listen_port: u16,
    /// 节点 ID（20 字节）
    pub node_id: [u8; 20],
    /// 路由表持久化路径
    pub persistence_path: Option<String>,
    /// 路由表刷新间隔
    pub refresh_interval: Duration,
    /// 节点过期时间
    pub node_ttl: Duration,
    /// 请求超时
    pub request_timeout: Duration,
    /// 最大并发请求数（alpha）
    pub max_concurrent_requests: usize,
    /// 最大查询轮次
    pub max_query_rounds: usize,
    /// 是否启用
    pub enabled: bool,
}

impl Default for DhtConfig {
    fn default() -> Self {
        let mut node_id = [0u8; 20];
        let mut rng = rand::thread_rng();
        for byte in node_id.iter_mut() {
            *byte = rng.gen();
        }

        Self {
            bootstrap_nodes: crate::discoverers::dht::DHT_BOOTSTRAP_NODES
                .iter()
                .map(|(host, port)| (host.to_string(), *port))
                .collect(),
            listen_port: 6881,
            node_id,
            persistence_path: None,
            refresh_interval: Duration::from_secs(300),
            node_ttl: Duration::from_secs(3600),
            request_timeout: Duration::from_secs(8),
            max_concurrent_requests: 3,
            max_query_rounds: 5,
            enabled: true,
        }
    }
}

/// 路由表中的节点
#[derive(Debug, Clone)]
struct RoutingNode {
    id: [u8; 20],
    addr: SocketAddr,
    last_active: Instant,
    consecutive_failures: u32,
    #[allow(dead_code)]
    is_bootstrap: bool,
}

impl RoutingNode {
    fn is_healthy(&self) -> bool {
        self.consecutive_failures < 3
    }
}

/// get_peers 查询结果
struct QueryResult {
    peers: Vec<SocketAddr>,
    nodes: Vec<DhtNode>,
    responder_id: [u8; 20],
}

/// DHT 发现器
pub struct DhtDiscoverer {
    config: DhtConfig,
    /// 路由表（addr -> RoutingNode）
    routing_table: Arc<RwLock<HashMap<SocketAddr, RoutingNode>>>,
    /// 统计
    stats: Arc<RwLock<DiscovererStats>>,
    /// 是否已初始化
    initialized: Arc<RwLock<bool>>,
}

impl DhtDiscoverer {
    /// 创建新的 DHT 发现器
    pub fn new(config: DhtConfig) -> Self {
        Self {
            config,
            routing_table: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(DiscovererStats::default())),
            initialized: Arc::new(RwLock::new(false)),
        }
    }

    /// 创建默认配置的 DHT 发现器
    pub fn with_default_config() -> Self {
        Self::new(DhtConfig::default())
    }

    /// 初始化 DHT 节点（解析 bootstrap 节点并加入路由表）
    pub async fn init(&self) -> Result<()> {
        if *self.initialized.read() {
            return Ok(());
        }

        info!(
            "[dht] 初始化 DHT 节点，连接 {} 个 bootstrap 节点",
            self.config.bootstrap_nodes.len()
        );

        let mut resolved = vec![];
        for (host, port) in &self.config.bootstrap_nodes {
            let addr_str = format!("{}:{}", host, port);
            match tokio::net::lookup_host(addr_str.clone()).await {
                Ok(addrs) => resolved.extend(addrs),
                Err(_) => debug!("[dht] bootstrap 节点 {} 解析失败", addr_str),
            }
        }

        {
            let mut table = self.routing_table.write();
            for addr in resolved {
                let mut node_id = [0u8; 20];
                rand::thread_rng().fill(&mut node_id);
                table.insert(
                    addr,
                    RoutingNode {
                        id: node_id,
                        addr,
                        last_active: Instant::now(),
                        consecutive_failures: 0,
                        is_bootstrap: true,
                    },
                );
            }
        }

        *self.initialized.write() = true;
        info!(
            "[dht] DHT 初始化完成，路由表 {} 个节点",
            self.routing_table.read().len()
        );

        Ok(())
    }

    /// 获取健康的节点列表
    fn healthy_nodes(&self) -> Vec<RoutingNode> {
        self.routing_table
            .read()
            .values()
            .filter(|n| n.is_healthy())
            .cloned()
            .collect()
    }

    /// 获取距离目标最近的 N 个节点
    fn nearest_nodes(&self, target: &[u8; 20], count: usize) -> Vec<RoutingNode> {
        let mut nodes: Vec<RoutingNode> = self.healthy_nodes();
        nodes.sort_by(|a, b| {
            let da = DhtMessage::xor_distance(&a.id, target);
            let db = DhtMessage::xor_distance(&b.id, target);
            if DhtMessage::distance_less(&da, &db) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        });
        nodes.truncate(count);
        nodes
    }

    /// 添加节点到路由表
    fn add_node(&self, node: &DhtNode) {
        let mut table = self.routing_table.write();
        if table.len() < 1000 {
            table.entry(node.addr).or_insert(RoutingNode {
                id: node.id,
                addr: node.addr,
                last_active: Instant::now(),
                consecutive_failures: 0,
                is_bootstrap: false,
            });
        }
    }

    /// 标记节点失败
    fn mark_node_failure(&self, addr: &SocketAddr) {
        if let Some(node) = self.routing_table.write().get_mut(addr) {
            node.consecutive_failures += 1;
        }
    }

    /// 标记节点成功
    fn mark_node_success(&self, addr: &SocketAddr, id: [u8; 20]) {
        if let Some(node) = self.routing_table.write().get_mut(addr) {
            node.id = id;
            node.last_active = Instant::now();
            node.consecutive_failures = 0;
        }
    }

    /// 向单个节点发送 get_peers 查询，返回 peers 和更近的 nodes
    async fn query_node(
        socket: &Arc<UdpSocket>,
        node_addr: SocketAddr,
        our_id: &[u8; 20],
        info_hash: &Infohash,
        timeout: Duration,
    ) -> Result<QueryResult> {
        let tid = rand::thread_rng().gen::<[u8; 2]>();
        let request = DhtMessage::build_get_peers(&tid, our_id, info_hash);

        socket.send_to(&request, node_addr).await?;

        let mut buf = vec![0u8; 4096];
        let (n, _from) = tokio::time::timeout(timeout, socket.recv_from(&mut buf)).await??;
        buf.truncate(n);

        if let Some((_resp_tid, response)) = DhtMessage::parse_get_peers_response(&buf) {
            return Ok(QueryResult {
                peers: response.values,
                nodes: response.nodes,
                responder_id: response.node_id,
            });
        }

        Ok(QueryResult {
            peers: vec![],
            nodes: vec![],
            responder_id: [0u8; 20],
        })
    }

    /// 记录请求结果
    fn record_result(&self, success: bool, peers_count: usize, duration: Duration) {
        let mut stats = self.stats.write();
        if success {
            stats.record_success(peers_count, duration.as_millis() as f64);
        } else {
            stats.record_failure();
        }
    }
}

#[async_trait]
impl PeerDiscoverer for DhtDiscoverer {
    fn name(&self) -> &str {
        "dht"
    }

    fn discoverer_type(&self) -> DiscovererType {
        DiscovererType::Dht
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn discover_peers(
        &self,
        infohash: &Infohash,
        limit: usize,
    ) -> anyhow::Result<Vec<PeerInfo>> {
        self.init().await?;

        let start = Instant::now();
        let socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);

        let mut all_peers: HashSet<SocketAddr> = HashSet::new();
        let mut queried: HashSet<SocketAddr> = HashSet::new();
        let mut closest_distance = [0xFFu8; 20];
        let mut rounds_without_improvement = 0;

        // 迭代式 Kademlia 查找
        for round in 0..self.config.max_query_rounds {
            // 获取最近的未查询节点
            let candidates = self.nearest_nodes(infohash, 20);
            let to_query: Vec<RoutingNode> = candidates
                .into_iter()
                .filter(|n| !queried.contains(&n.addr))
                .take(self.config.max_concurrent_requests)
                .collect();

            if to_query.is_empty() {
                debug!("[dht] 第 {} 轮：没有未查询的节点，结束", round);
                break;
            }

            // 检查是否有更近的节点
            let nearest = &to_query[0];
            let dist = DhtMessage::xor_distance(&nearest.id, infohash);
            if DhtMessage::distance_less(&dist, &closest_distance) {
                closest_distance = dist;
                rounds_without_improvement = 0;
            } else {
                rounds_without_improvement += 1;
                if rounds_without_improvement >= 2 {
                    debug!(
                        "[dht] 第 {} 轮：连续 {} 轮无更近节点，结束",
                        round, rounds_without_improvement
                    );
                    break;
                }
            }

            debug!(
                "[dht] 第 {} 轮：查询 {} 个节点（最近: {}）",
                round,
                to_query.len(),
                to_query[0].addr
            );

            // 并发查询
            let mut tasks = vec![];
            for node in &to_query {
                queried.insert(node.addr);
                let socket = socket.clone();
                let node_addr = node.addr;
                let our_id = self.config.node_id;
                let infohash = *infohash;
                let timeout = self.config.request_timeout;

                tasks.push(tokio::spawn(async move {
                    let result =
                        DhtDiscoverer::query_node(&socket, node_addr, &our_id, &infohash, timeout)
                            .await;
                    (node_addr, result)
                }));
            }

            // 收集结果
            let mut new_nodes_count = 0;
            for task in tasks {
                if let Ok((node_addr, result)) = task.await {
                    match result {
                        Ok(qr) => {
                            if !qr.peers.is_empty() {
                                debug!(
                                    "[dht] 节点 {} 返回 {} 个 peer, {} 个新节点",
                                    node_addr,
                                    qr.peers.len(),
                                    qr.nodes.len()
                                );
                            }
                            for peer in qr.peers {
                                all_peers.insert(peer);
                            }
                            // 将新节点加入路由表，供下一轮查询
                            for node in &qr.nodes {
                                if !queried.contains(&node.addr) {
                                    self.add_node(node);
                                    new_nodes_count += 1;
                                }
                            }
                            self.mark_node_success(&node_addr, qr.responder_id);
                        }
                        Err(e) => {
                            debug!("[dht] 节点 {} 查询失败: {}", node_addr, e);
                            self.mark_node_failure(&node_addr);
                        }
                    }
                }
            }

            debug!(
                "[dht] 第 {} 轮结束：新增 {} 个节点，累计 {} 个 peer",
                round,
                new_nodes_count,
                all_peers.len()
            );

            if all_peers.len() >= limit {
                debug!("[dht] 已收集足够 peer（{}），结束", all_peers.len());
                break;
            }
        }

        let peers: Vec<PeerInfo> = all_peers
            .into_iter()
            .take(limit)
            .map(|addr| PeerInfo::new(addr, PeerSource::Dht))
            .collect();

        self.record_result(true, peers.len(), start.elapsed());

        info!(
            "[dht] 发现完成: {} 个 peer (耗时 {:?})",
            peers.len(),
            start.elapsed()
        );

        Ok(peers)
    }

    async fn announce(
        &self,
        infohash: &Infohash,
        port: u16,
        event: AnnounceEvent,
    ) -> anyhow::Result<()> {
        debug!(
            "[dht] announce: infohash={}, port={}, event={}",
            hex::encode(infohash),
            port,
            event.as_str()
        );

        // DHT 没有 stopped 概念，直接忽略
        if event == AnnounceEvent::Stopped {
            return Ok(());
        }

        self.init().await?;

        let socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);

        // 获取最近的 5 个节点
        let nodes = self.nearest_nodes(infohash, 5);
        if nodes.is_empty() {
            debug!("[dht] announce: 没有可用节点");
            return Ok(());
        }

        // 并发发送 get_peers 获取 token
        let mut tasks = vec![];
        for node in &nodes {
            let socket = socket.clone();
            let node_addr = node.addr;
            let our_id = self.config.node_id;
            let infohash = *infohash;
            let timeout = self.config.request_timeout;

            tasks.push(tokio::spawn(async move {
                let tid = rand::thread_rng().gen::<[u8; 2]>();
                let request = DhtMessage::build_get_peers(&tid, &our_id, &infohash);
                if socket.send_to(&request, node_addr).await.is_err() {
                    return None;
                }

                let mut buf = vec![0u8; 4096];
                match tokio::time::timeout(timeout, socket.recv_from(&mut buf)).await {
                    Ok(Ok((n, _))) => {
                        buf.truncate(n);
                        DhtMessage::parse_get_peers_response(&buf)
                            .and_then(|(_, resp)| resp.token.map(|t| (node_addr, t)))
                    }
                    _ => None,
                }
            }));
        }

        // 收集 token 并发送 announce_peer
        let mut announced_count = 0;
        for task in tasks {
            if let Ok(Some((node_addr, token))) = task.await {
                // 发送 announce_peer（fire and forget）
                let tid = rand::thread_rng().gen::<[u8; 2]>();
                let announce_msg = DhtMessage::build_announce_peer(
                    &tid,
                    &self.config.node_id,
                    infohash,
                    port,
                    &token,
                );
                if socket.send_to(&announce_msg, node_addr).await.is_ok() {
                    announced_count += 1;
                    debug!("[dht] 向 {} announce_peer 成功", node_addr);
                }
            }
        }

        debug!(
            "[dht] announce 完成: 向 {}/{} 个节点发送了 announce_peer",
            announced_count,
            nodes.len()
        );

        Ok(())
    }

    async fn health_check(&self) -> bool {
        let healthy_count = self.healthy_nodes().len();
        debug!(
            "[dht] 健康检查: {}/{} 个节点健康",
            healthy_count,
            self.routing_table.read().len()
        );
        healthy_count > 0
    }

    fn stats(&self) -> DiscovererStats {
        self.stats.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dht_config_default() {
        let config = DhtConfig::default();
        assert!(!config.bootstrap_nodes.is_empty());
        assert_eq!(config.listen_port, 6881);
        assert_eq!(config.max_concurrent_requests, 3);
    }

    #[tokio::test]
    async fn test_dht_discoverer_creation() {
        let discoverer = DhtDiscoverer::with_default_config();
        assert_eq!(discoverer.name(), "dht");
        assert!(discoverer.is_enabled());
    }

    #[tokio::test]
    async fn test_dht_init() {
        let discoverer = DhtDiscoverer::with_default_config();
        let result = discoverer.init().await;
        assert!(result.is_ok());
        assert!(*discoverer.initialized.read());
        assert!(discoverer.routing_table.read().len() > 0);
    }

    #[test]
    fn test_nearest_nodes() {
        let discoverer = DhtDiscoverer::with_default_config();
        {
            let mut table = discoverer.routing_table.write();
            for i in 0..10u8 {
                let mut id = [0u8; 20];
                id[0] = i;
                let addr = SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                    6881 + i as u16,
                );
                table.insert(
                    addr,
                    RoutingNode {
                        id,
                        addr,
                        last_active: Instant::now(),
                        consecutive_failures: 0,
                        is_bootstrap: false,
                    },
                );
            }
        }

        let target = [0u8; 20];
        let nearest = discoverer.nearest_nodes(&target, 3);
        assert_eq!(nearest.len(), 3);
        assert_eq!(nearest[0].id[0], 0);
    }

    #[test]
    fn test_add_node() {
        let discoverer = DhtDiscoverer::with_default_config();
        let addr = SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            6881,
        );
        let node = DhtNode {
            id: [1u8; 20],
            addr,
        };
        discoverer.add_node(&node);
        assert_eq!(discoverer.routing_table.read().len(), 1);
    }

    #[test]
    fn test_mark_node_failure() {
        let discoverer = DhtDiscoverer::with_default_config();
        let addr = SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            6881,
        );
        {
            let mut table = discoverer.routing_table.write();
            table.insert(
                addr,
                RoutingNode {
                    id: [0u8; 20],
                    addr,
                    last_active: Instant::now(),
                    consecutive_failures: 0,
                    is_bootstrap: false,
                },
            );
        }
        for _ in 0..3 {
            discoverer.mark_node_failure(&addr);
        }
        let nodes = discoverer.healthy_nodes();
        assert!(nodes.is_empty()); // 连续失败 3 次后不健康
    }
}
