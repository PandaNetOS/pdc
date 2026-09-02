//! DHT 客户端
//!
//! 实现 BitTorrent DHT 协议，发现 peer。
//!
//! 注意：这是一个骨架实现，完整的 DHT 协议实现需要：
//! - Kademlia 路由表
//! - DHT 消息编码/解码（bencode）
//! - UDP 套接字管理
//! - 节点健康检查
//! - 路由表持久化
//!
//! 实际项目中建议集成 librqbit 或其他成熟的 DHT 库。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use parking_lot::RwLock;
use rand::Rng;
use tracing::{debug, info, warn};

use crate::traits::{AnnounceEvent, DiscovererStats, DiscovererType, PeerDiscoverer};
use crate::types::{Infohash, PeerInfo};

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
    /// 最大并发请求数
    pub max_concurrent_requests: usize,
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
            bootstrap_nodes: crate::dht::DHT_BOOTSTRAP_NODES
                .iter()
                .map(|(host, port)| (host.to_string(), *port))
                .collect(),
            listen_port: 6881,
            node_id,
            persistence_path: None,
            refresh_interval: Duration::from_secs(300),
            node_ttl: Duration::from_secs(3600),
            request_timeout: Duration::from_secs(10),
            max_concurrent_requests: 20,
            enabled: true,
        }
    }
}

/// DHT 节点信息
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct DhtNode {
    /// 节点 ID
    id: [u8; 20],
    /// 节点地址
    addr: SocketAddr,
    /// 最后一次活跃时间
    last_active: Instant,
    /// 连续失败次数
    consecutive_failures: u32,
    /// 是否为 bootstrap 节点
    is_bootstrap: bool,
}

impl DhtNode {
    /// 是否健康
    fn is_healthy(&self) -> bool {
        self.consecutive_failures < 3 && self.last_active.elapsed() < Duration::from_secs(3600)
    }
}

/// DHT 发现器
///
/// 注意：这是一个骨架实现，实际 DHT 协议需要完整的 Kademlia 实现。
/// 建议在实际项目中集成 librqbit 或其他成熟的 DHT 库。
pub struct DhtDiscoverer {
    config: DhtConfig,
    /// 路由表（node_id -> DhtNode）
    routing_table: Arc<RwLock<HashMap<[u8; 20], DhtNode>>>,
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

    /// 初始化 DHT 节点（连接 bootstrap 节点）
    pub async fn init(&self) -> Result<()> {
        if *self.initialized.read() {
            return Ok(());
        }

        info!(
            "[dht] 初始化 DHT 节点，连接 {} 个 bootstrap 节点",
            self.config.bootstrap_nodes.len()
        );

        // 先解析所有 bootstrap 节点的地址（避免写锁跨 await）
        let mut resolved_addrs = vec![];
        for (host, port) in &self.config.bootstrap_nodes {
            if let Ok(addrs) = tokio::net::lookup_host(format!("{}:{}", host, port)).await {
                resolved_addrs.extend(addrs);
            }
        }

        // 再添加到路由表
        {
            let mut routing_table = self.routing_table.write();
            for addr in resolved_addrs {
                let mut node_id = [0u8; 20];
                let mut rng = rand::thread_rng();
                for byte in node_id.iter_mut() {
                    *byte = rng.gen();
                }

                routing_table.insert(
                    node_id,
                    DhtNode {
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
            "[dht] DHT 节点初始化完成，路由表中有 {} 个节点",
            self.routing_table.read().len()
        );

        Ok(())
    }

    /// 获取健康的节点列表
    fn healthy_nodes(&self) -> Vec<DhtNode> {
        self.routing_table
            .read()
            .values()
            .filter(|n| n.is_healthy())
            .cloned()
            .collect()
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
        _limit: usize,
    ) -> anyhow::Result<Vec<PeerInfo>> {
        // 确保已初始化
        self.init().await?;

        let start = Instant::now();
        let nodes = self.healthy_nodes();

        if nodes.is_empty() {
            warn!("[dht] 路由表中没有健康的节点");
            self.record_result(false, 0, start.elapsed());
            return Ok(vec![]);
        }

        debug!(
            "[dht] 开始向 {} 个节点查询 peer (infohash={})",
            nodes.len(),
            hex::encode(infohash)
        );

        // 骨架实现：模拟 DHT 查询
        // 实际实现需要：
        // 1. 向最近的节点发送 get_peers 请求
        // 2. 递归查询更近的节点
        // 3. 收集所有返回的 peer
        // 4. 将新发现的节点加入路由表

        // 这里返回空列表，因为完整的 DHT 实现需要大量代码
        // 建议集成 librqbit 的 DHT 实现
        let peers: Vec<PeerInfo> = vec![];

        self.record_result(true, peers.len(), start.elapsed());

        info!("[dht] 发现完成: {} 个 peer", peers.len());

        Ok(peers)
    }

    async fn announce(
        &self,
        infohash: &Infohash,
        port: u16,
        event: AnnounceEvent,
    ) -> anyhow::Result<()> {
        // 骨架实现：向 DHT 网络宣告自己
        // 实际实现需要发送 announce_peer 请求
        debug!(
            "[dht] announce: infohash={}, port={}, event={}",
            hex::encode(infohash),
            port,
            event.as_str()
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
    }
}
