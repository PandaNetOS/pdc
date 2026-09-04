//! PEX 客户端
//!
//! 实现 Peer Exchange（PEX）协议，从已连接的 peer 发现更多 peer。
//!
//! 实现了：
//! - BitTorrent 握手
//! - 扩展协议协商（BEP 10）
//! - ut_pex 消息交换（BEP 11）

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use parking_lot::RwLock;
use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

use crate::traits::{AnnounceEvent, DiscovererStats, DiscovererType, PeerDiscoverer};
use crate::types::{Infohash, PeerInfo, PeerSource};

use super::message::{BtHandshake, ExtendedMessage, ExtensionHandshake, UtPexMessage};

/// PEX 配置
#[derive(Debug, Clone)]
pub struct PexConfig {
    /// 我们的 peer ID
    pub our_peer_id: [u8; 20],
    /// 我们的监听端口
    pub listen_port: u16,
    /// 最大已连接 peer 数
    pub max_connected_peers: usize,
    /// PEX 请求间隔
    pub pex_request_interval: Duration,
    /// 每个 peer 每次返回的最大 peer 数
    pub max_peers_per_request: usize,
    /// peer 过期时间
    pub peer_ttl: Duration,
    /// 连接超时
    pub connect_timeout: Duration,
    /// 是否启用
    pub enabled: bool,
}

impl Default for PexConfig {
    fn default() -> Self {
        let mut peer_id = [0u8; 20];
        let mut rng = rand::thread_rng();
        for byte in peer_id.iter_mut() {
            *byte = rng.gen();
        }
        // 设置客户端标识：-PD0002-
        peer_id[0..8].copy_from_slice(b"-PD0002-");

        Self {
            our_peer_id: peer_id,
            listen_port: 6881,
            max_connected_peers: 50,
            pex_request_interval: Duration::from_secs(60),
            max_peers_per_request: 50,
            peer_ttl: Duration::from_secs(1800),
            connect_timeout: Duration::from_secs(10),
            enabled: true,
        }
    }
}

/// 已连接的 peer 信息
#[derive(Debug, Clone)]
struct ConnectedPeer {
    /// peer 地址
    addr: SocketAddr,
    /// peer ID
    peer_id: Option<[u8; 20]>,
    /// 连接时间
    #[allow(dead_code)]
    connected_at: Instant,
    /// 最后一次 PEX 交换时间
    last_pex_exchange: Option<Instant>,
    /// 从这个 peer 获取的 peer 总数
    peers_received: u64,
    /// 是否支持 PEX
    supports_pex: bool,
    /// 对方分配的 ut_pex 扩展 ID
    ut_pex_id: Option<u8>,
}

/// PEX 发现器
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
                    supports_pex: true,
                    ut_pex_id: None,
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

    /// 获取需要进行 PEX 交换的 peer
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

    /// 与单个 peer 进行 PEX 交换
    ///
    /// 流程：
    /// 1. 建立 TCP 连接
    /// 2. BitTorrent 握手
    /// 3. 扩展协议握手
    /// 4. 发送 ut_pex 消息
    /// 5. 接收 ut_pex 消息
    async fn exchange_with_peer(
        &self,
        addr: SocketAddr,
        infohash: &Infohash,
    ) -> Result<Vec<SocketAddr>> {
        debug!("[pex] 与 {} 开始 PEX 交换", addr);

        // 1. 建立 TCP 连接
        let mut stream =
            tokio::time::timeout(self.config.connect_timeout, TcpStream::connect(addr)).await??;

        // 2. 发送 BitTorrent 握手
        let handshake = BtHandshake::build(infohash, &self.config.our_peer_id);
        stream.write_all(&handshake).await?;

        // 接收握手响应
        let mut resp_buf = [0u8; 68];
        tokio::time::timeout(
            self.config.connect_timeout,
            stream.read_exact(&mut resp_buf),
        )
        .await??;

        let resp_handshake = BtHandshake::parse(&resp_buf)
            .ok_or_else(|| anyhow::anyhow!("无效的 BitTorrent 握手响应"))?;

        // 检查 infohash 是否匹配
        if resp_handshake.infohash != *infohash {
            return Err(anyhow::anyhow!("infohash 不匹配"));
        }

        // 检查是否支持扩展协议
        if !resp_handshake.supports_extension() {
            debug!("[pex] peer {} 不支持扩展协议", addr);
            // 标记为不支持 PEX
            if let Some(peer) = self.connected_peers.write().get_mut(&addr) {
                peer.supports_pex = false;
            }
            return Ok(vec![]);
        }

        // 3. 发送扩展握手
        let ext_handshake =
            ExtensionHandshake::build_request(&self.config.our_peer_id, self.config.listen_port);
        let ext_msg = ExtendedMessage::build(0, &ext_handshake);
        stream.write_all(&ext_msg).await?;

        // 接收扩展握手响应（可能需要跳过一些消息）
        let mut ut_pex_id = None;
        for _ in 0..5 {
            let (msg_type, payload) = ExtendedMessage::read_message(&mut stream).await?;
            if msg_type == 20 {
                // 扩展协议消息
                if payload.is_empty() {
                    continue;
                }
                let ext_id = payload[0];
                if ext_id == 0 {
                    // 扩展握手响应
                    if let Some(handshake_resp) = ExtensionHandshake::parse(&payload[1..]) {
                        ut_pex_id = handshake_resp.ut_pex_id();
                        debug!(
                            "[pex] peer {} 支持 ut_pex, 扩展ID={:?}, 客户端={:?}",
                            addr, ut_pex_id, handshake_resp.version
                        );
                    }
                    break;
                }
            }
            // 其他消息（bitfield、have 等），跳过
        }

        let ut_pex_id = match ut_pex_id {
            Some(id) => id,
            None => {
                debug!("[pex] peer {} 不支持 ut_pex", addr);
                if let Some(peer) = self.connected_peers.write().get_mut(&addr) {
                    peer.supports_pex = false;
                }
                return Ok(vec![]);
            }
        };

        // 更新 peer 信息
        if let Some(peer) = self.connected_peers.write().get_mut(&addr) {
            peer.ut_pex_id = Some(ut_pex_id);
            peer.peer_id = Some(resp_handshake.peer_id);
        }

        // 4. 发送 ut_pex 消息（我们知道的 peer）
        let known: Vec<SocketAddr> = self
            .known_peers
            .read()
            .keys()
            .take(self.config.max_peers_per_request)
            .copied()
            .collect();
        let pex_msg = UtPexMessage::build(&known, &[]);
        let ext_pex_msg = ExtendedMessage::build(ut_pex_id, &pex_msg);
        stream.write_all(&ext_pex_msg).await?;

        // 5. 接收 ut_pex 响应（等待一小段时间）
        let mut new_peers = vec![];
        let receive_deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < receive_deadline {
            match tokio::time::timeout(
                Duration::from_secs(2),
                ExtendedMessage::read_message(&mut stream),
            )
            .await
            {
                Ok(Ok((msg_type, payload))) => {
                    if msg_type == 20 && !payload.is_empty() {
                        let ext_id = payload[0];
                        if ext_id == ut_pex_id {
                            // ut_pex 消息
                            if let Some(pex_resp) = UtPexMessage::parse(&payload[1..]) {
                                debug!(
                                    "[pex] 从 {} 收到 {} 个新增 peer, {} 个移除 peer",
                                    addr,
                                    pex_resp.added.len(),
                                    pex_resp.dropped.len()
                                );
                                new_peers.extend(pex_resp.added);
                            }
                            break;
                        }
                    }
                    // 其他消息，继续等待
                }
                _ => break,
            }
        }

        // 更新最后交换时间
        if let Some(peer) = self.connected_peers.write().get_mut(&addr) {
            peer.last_pex_exchange = Some(Instant::now());
            peer.peers_received += new_peers.len() as u64;
        }

        debug!(
            "[pex] 与 {} 交换完成，发现 {} 个新 peer",
            addr,
            new_peers.len()
        );
        Ok(new_peers)
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
        infohash: &Infohash,
        limit: usize,
    ) -> anyhow::Result<Vec<PeerInfo>> {
        let start = Instant::now();

        self.cleanup_expired_peers();

        let due_peers = self.peers_due_for_pex();
        if !due_peers.is_empty() {
            debug!("[pex] 有 {} 个 peer 需要进行 PEX 交换", due_peers.len());

            // 并发进行 PEX 交换
            let mut tasks = vec![];
            for addr in &due_peers {
                let addr = *addr;
                let infohash = *infohash;
                let discoverer = self.clone_shallow();
                tasks.push(tokio::spawn(async move {
                    let result = discoverer.exchange_with_peer(addr, &infohash).await;
                    (addr, result)
                }));
            }

            for task in tasks {
                if let Ok((addr, result)) = task.await {
                    match result {
                        Ok(peers) => {
                            if !peers.is_empty() {
                                self.add_known_peers(&peers);
                            }
                        }
                        Err(e) => {
                            debug!("[pex] 与 {} 交换失败: {}", addr, e);
                            // 失败不移除 peer，只是不更新最后交换时间
                        }
                    }
                }
            }
        }

        let known = self.known_peers.read();
        let mut peers: Vec<PeerInfo> = known.values().cloned().collect();

        peers.sort_by_key(|a| std::cmp::Reverse(a.priority_score));

        if peers.len() > limit {
            peers.truncate(limit);
        }

        self.record_result(true, peers.len(), start.elapsed());

        debug!(
            "[pex] 发现完成: {} 个 peer (耗时 {:?})",
            peers.len(),
            start.elapsed()
        );

        Ok(peers)
    }

    async fn announce(
        &self,
        _infohash: &Infohash,
        _port: u16,
        _event: AnnounceEvent,
    ) -> anyhow::Result<()> {
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

impl PexDiscoverer {
    /// 浅拷贝（只共享 Arc 内部状态）
    fn clone_shallow(&self) -> Self {
        Self {
            config: self.config.clone(),
            connected_peers: self.connected_peers.clone(),
            known_peers: self.known_peers.clone(),
            stats: self.stats.clone(),
        }
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
        assert_eq!(config.listen_port, 6881);
        assert!(config.enabled);
        // 检查 peer ID 前缀
        assert_eq!(&config.our_peer_id[0..8], b"-PD0002-");
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

    #[tokio::test]
    async fn test_peers_due_for_pex() {
        let discoverer = PexDiscoverer::with_default_config();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6881);

        discoverer.add_connected_peer(addr, None);
        let due = discoverer.peers_due_for_pex();
        assert_eq!(due.len(), 1); // 新 peer 应该需要 PEX

        // 标记为已交换
        if let Some(peer) = discoverer.connected_peers.write().get_mut(&addr) {
            peer.last_pex_exchange = Some(Instant::now());
        }
        let due = discoverer.peers_due_for_pex();
        assert_eq!(due.len(), 0); // 刚交换过，不需要
    }

    #[test]
    fn test_clone_shallow() {
        let discoverer = PexDiscoverer::with_default_config();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6881);
        discoverer.add_known_peers(&[addr]);

        let cloned = discoverer.clone_shallow();
        assert_eq!(cloned.known_peers.read().len(), 1); // 共享状态
    }
}
