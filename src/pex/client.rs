//! PEX 客户端
//!
//! 实现 Peer Exchange（PEX）协议，从已连接的 peer 发现更多 peer。
//!
//! 注意：这是一个骨架实现，完整的 PEX 实现需要：
//! - BitTorrent 握手和扩展协议协商
//! - ut_pex 消息编码/解码
//! - peer 连接管理
//! - 持续的 peer 交换
//!
//! 实际项目中建议集成 librqbit 或其他成熟的 BT 库。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::RwLock;
use tracing::debug;

use crate::traits::{AnnounceEvent, DiscovererStats, DiscovererType, PeerDiscoverer};
use crate::types::{Infohash, PeerInfo, PeerSource};

/// PEX 配置
#[derive(Debug, Clone)]
pub struct PexConfig {
    /// 最大已连接 peer 数
    pub max_connected_peers: usize,
    /// PEX 请求间隔
    pub pex_request_interval: Duration,
    /// 每个 peer 每次返回的最大 peer 数
    pub max_peers_per_request: usize,
    /// peer 过期时间
    pub peer_ttl: Duration,
    /// 是否启用
    pub enabled: bool,
}

impl Default for PexConfig {
    fn default() -> Self {
        Self {
            max_connected_peers: 50,
            pex_request_interval: Duration::from_secs(60),
            max_peers_per_request: 50,
            peer_ttl: Duration::from_secs(1800), // 30 分钟
            enabled: true,
        }
    }
}

/// 已连接的 peer 信息
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ConnectedPeer {
    /// peer 地址
    addr: SocketAddr,
    /// peer ID
    peer_id: Option<[u8; 20]>,
    /// 连接时间
    connected_at: Instant,
    /// 最后一次 PEX 交换时间
    last_pex_exchange: Option<Instant>,
    /// 从这个 peer 获取的 peer 总数
    peers_received: u64,
    /// 是否支持 PEX
    supports_pex: bool,
}

/// PEX 发现器
///
/// 注意：这是一个骨架实现，实际 PEX 需要完整的 BitTorrent 连接管理。
/// 建议在实际项目中集成 librqbit 的 PEX 实现。
pub struct PexDiscoverer {
    config: PexConfig,
    /// 已连接的 peer（addr -> ConnectedPeer）
    connected_peers: Arc<RwLock<HashMap<SocketAddr, ConnectedPeer>>>,
    /// 已知的 peer（addr -> PeerInfo）
    known_peers: Arc<RwLock<HashMap<SocketAddr, PeerInfo>>>,
    /// 统计
    stats: Arc<RwLock<DiscovererStats>>,
}

impl PexDiscoverer {
    /// 创建新的 PEX 发现器
    pub fn new(config: PexConfig) -> Self {
        Self {
            config,
            connected_peers: Arc::new(RwLock::new(HashMap::new())),
            known_peers: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(DiscovererStats::default())),
        }
    }

    /// 创建默认配置的 PEX 发现器
    pub fn with_default_config() -> Self {
        Self::new(PexConfig::default())
    }

    /// 添加已连接的 peer
    pub fn add_connected_peer(&self, addr: SocketAddr, peer_id: Option<[u8; 20]>) {
        let mut peers = self.connected_peers.write();
        if peers.len() < self.config.max_connected_peers {
            peers.insert(
                addr,
                ConnectedPeer {
                    addr,
                    peer_id,
                    connected_at: Instant::now(),
                    last_pex_exchange: None,
                    peers_received: 0,
                    supports_pex: true, // 假设支持，实际需要协商
                },
            );
            debug!("[pex] 添加已连接 peer: {}", addr);
        }
    }

    /// 移除已断开的 peer
    pub fn remove_connected_peer(&self, addr: &SocketAddr) {
        self.connected_peers.write().remove(addr);
        debug!("[pex] 移除已断开 peer: {}", addr);
    }

    /// 添加从 PEX 获取的新 peer
    pub fn add_known_peers(&self, peers: &[SocketAddr]) {
        let mut known = self.known_peers.write();
        for addr in peers {
            known
                .entry(*addr)
                .or_insert_with(|| PeerInfo::new(*addr, PeerSource::Pex));
        }
    }

    /// 获取需要进行 PEX 交换的 peer（超过间隔时间的）
    fn peers_due_for_pex(&self) -> Vec<SocketAddr> {
        let peers = self.connected_peers.read();
        peers
            .values()
            .filter(|p| {
                p.supports_pex
                    && p.last_pex_exchange
                        .map(|t| t.elapsed() >= self.config.pex_request_interval)
                        .unwrap_or(true)
            })
            .map(|p| p.addr)
            .collect()
    }

    /// 清理过期的已知 peer
    fn cleanup_expired_peers(&self) {
        let mut known = self.known_peers.write();
        known.retain(|_, p| !p.is_expired());
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
impl PeerDiscoverer for PexDiscoverer {
    fn name(&self) -> &str {
        "pex"
    }

    fn discoverer_type(&self) -> DiscovererType {
        DiscovererType::Pex
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn discover_peers(
        &self,
        _infohash: &Infohash,
        limit: usize,
    ) -> anyhow::Result<Vec<PeerInfo>> {
        let start = Instant::now();

        // 清理过期的 peer
        self.cleanup_expired_peers();

        // 获取需要进行 PEX 交换的 peer
        let due_peers = self.peers_due_for_pex();
        if !due_peers.is_empty() {
            debug!("[pex] 有 {} 个 peer 需要进行 PEX 交换", due_peers.len());

            // 骨架实现：模拟 PEX 交换
            // 实际实现需要：
            // 1. 向每个 due peer 发送 ut_pex 请求
            // 2. 接收返回的 peer 列表
            // 3. 更新 last_pex_exchange 时间
            // 4. 将新 peer 加入 known_peers

            for addr in &due_peers {
                if let Some(peer) = self.connected_peers.write().get_mut(addr) {
                    peer.last_pex_exchange = Some(Instant::now());
                }
            }
        }

        // 返回已知的 peer
        let known = self.known_peers.read();
        let mut peers: Vec<PeerInfo> = known.values().cloned().collect();

        // 按优先级排序
        peers.sort_by_key(|a| std::cmp::Reverse(a.priority_score));

        if peers.len() > limit {
            peers.truncate(limit);
        }

        self.record_result(true, peers.len(), start.elapsed());

        debug!("[pex] 发现完成: {} 个 peer", peers.len());

        Ok(peers)
    }

    async fn announce(
        &self,
        _infohash: &Infohash,
        _port: u16,
        _event: AnnounceEvent,
    ) -> anyhow::Result<()> {
        // PEX 不需要 announce，这是 tracker/DHT 的功能
        Ok(())
    }

    async fn health_check(&self) -> bool {
        let connected = self.connected_peers.read().len();
        let known = self.known_peers.read().len();
        debug!(
            "[pex] 健康检查: 已连接 {} 个 peer, 已知 {} 个 peer",
            connected, known
        );
        connected > 0
    }

    fn stats(&self) -> DiscovererStats {
        self.stats.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_pex_config_default() {
        let config = PexConfig::default();
        assert_eq!(config.max_connected_peers, 50);
        assert!(config.enabled);
    }

    #[tokio::test]
    async fn test_pex_discoverer_creation() {
        let discoverer = PexDiscoverer::with_default_config();
        assert_eq!(discoverer.name(), "pex");
        assert!(discoverer.is_enabled());
    }

    #[tokio::test]
    async fn test_add_connected_peer() {
        let discoverer = PexDiscoverer::with_default_config();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6881);

        discoverer.add_connected_peer(addr, None);
        assert_eq!(discoverer.connected_peers.read().len(), 1);

        discoverer.remove_connected_peer(&addr);
        assert_eq!(discoverer.connected_peers.read().len(), 0);
    }

    #[tokio::test]
    async fn test_add_known_peers() {
        let discoverer = PexDiscoverer::with_default_config();
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6881);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 6882);

        discoverer.add_known_peers(&[addr1, addr2]);
        assert_eq!(discoverer.known_peers.read().len(), 2);
    }
}
