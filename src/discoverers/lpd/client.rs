//! LPD 客户端
//!
//! 实现 BEP 14 Local Peer Discovery：
//! - 加入多播组 239.192.152.143:6771
//! - 监听其他 peer 的 BT-SEARCH 广播
//! - 主动发送多播查询触发响应
//! - 从消息中提取 infohash 和 peer 地址

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
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

use super::{LPD_MULTICAST_ADDR, LPD_MULTICAST_PORT};

/// LPD 配置
#[derive(Debug, Clone)]
pub struct LpdConfig {
    /// 我们的监听端口（在多播消息中通告）
    pub listen_port: u16,
    /// 多播地址
    pub multicast_addr: Ipv4Addr,
    /// 多播端口
    pub multicast_port: u16,
    /// 唯一 cookie（用于过滤自己发出的消息）
    pub cookie: String,
    /// 同一 infohash 的最小广播间隔
    pub min_broadcast_interval: Duration,
    /// discover_peers 等待响应的时间
    pub query_wait_time: Duration,
    /// peer 过期时间
    pub peer_ttl: Duration,
    /// 是否启用
    pub enabled: bool,
}

impl Default for LpdConfig {
    fn default() -> Self {
        let cookie: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();

        Self {
            listen_port: 6881,
            multicast_addr: LPD_MULTICAST_ADDR.parse().unwrap(),
            multicast_port: LPD_MULTICAST_PORT,
            cookie,
            min_broadcast_interval: Duration::from_secs(300),
            query_wait_time: Duration::from_secs(3),
            peer_ttl: Duration::from_secs(1800),
            enabled: true,
        }
    }
}

/// 发现的 LPD peer
#[derive(Debug, Clone)]
struct LpdPeer {
    addr: SocketAddr,
    #[allow(dead_code)]
    first_seen: Instant,
    last_seen: Instant,
}

/// LPD 发现器
pub struct LpdDiscoverer {
    config: LpdConfig,
    /// infohash -> peer 列表
    discovered_peers: Arc<RwLock<HashMap<Infohash, Vec<LpdPeer>>>>,
    /// 最近广播的 infohash（用于限流）
    last_broadcast: Arc<RwLock<HashMap<Infohash, Instant>>>,
    /// 统计
    stats: Arc<RwLock<DiscovererStats>>,
    /// 监听任务是否已启动
    listener_started: Arc<RwLock<bool>>,
}

impl LpdDiscoverer {
    /// 创建新的 LPD 发现器
    pub fn new(config: LpdConfig) -> Self {
        Self {
            config,
            discovered_peers: Arc::new(RwLock::new(HashMap::new())),
            last_broadcast: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(DiscovererStats::default())),
            listener_started: Arc::new(RwLock::new(false)),
        }
    }

    /// 创建默认配置的 LPD 发现器
    pub fn with_default_config() -> Self {
        Self::new(LpdConfig::default())
    }

    /// 启动后台监听任务
    pub async fn init(&self) -> Result<()> {
        if *self.listener_started.read() {
            return Ok(());
        }

        info!(
            "[lpd] 启动 LPD 监听，多播 {}:{}",
            self.config.multicast_addr, self.config.multicast_port
        );

        // 创建接收 socket（绑定到多播端口）
        let socket = UdpSocket::bind(format!("0.0.0.0:{}", self.config.multicast_port)).await?;
        socket.join_multicast_v4(self.config.multicast_addr, Ipv4Addr::UNSPECIFIED)?;
        socket.set_multicast_loop_v4(true)?;

        let discovered = self.discovered_peers.clone();
        let cookie = self.config.cookie.clone();
        let peer_ttl = self.config.peer_ttl;

        *self.listener_started.write() = true;

        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((n, from)) => {
                        let data = &buf[..n];
                        if let Some((infohash, port)) = Self::parse_lpd_message(data, &cookie) {
                            // 用消息中的 Port 头作为 peer 端口，回退到发送方端口
                            let peer_addr =
                                SocketAddr::new(from.ip(), port.unwrap_or_else(|| from.port()));
                            debug!(
                                "[lpd] 收到 {} 的广播，infohash={}, port={}",
                                from,
                                hex::encode(infohash),
                                peer_addr.port()
                            );

                            let mut peers = discovered.write();
                            let entry = peers.entry(infohash).or_default();
                            if let Some(existing) = entry.iter_mut().find(|p| p.addr == peer_addr) {
                                existing.last_seen = Instant::now();
                            } else {
                                entry.push(LpdPeer {
                                    addr: peer_addr,
                                    first_seen: Instant::now(),
                                    last_seen: Instant::now(),
                                });
                            }

                            // 清理过期 peer
                            entry.retain(|p| p.last_seen.elapsed() < peer_ttl);
                        }
                    }
                    Err(e) => {
                        debug!("[lpd] 接收多播消息失败: {}", e);
                    }
                }
            }
        });

        info!("[lpd] LPD 监听已启动");
        Ok(())
    }

    /// 解析 LPD 消息（BT-SEARCH HTTP-like 格式）
    ///
    /// 返回 (infohash, port)，如果是自己发出的消息则返回 None
    fn parse_lpd_message(data: &[u8], our_cookie: &str) -> Option<(Infohash, Option<u16>)> {
        let text = std::str::from_utf8(data).ok()?;

        // 检查是否是 BT-SEARCH
        if !text.starts_with("BT-SEARCH") {
            return None;
        }

        let mut infohash: Option<Infohash> = None;
        let mut port: Option<u16> = None;
        let mut cookie_match = false;

        for line in text.lines() {
            let line = line.trim();
            if let Some((key, value)) = line.split_once(':') {
                let key = key.trim().to_lowercase();
                let value = value.trim();
                match key.as_str() {
                    "infohash" => {
                        if value.len() == 40 {
                            let mut ih = [0u8; 20];
                            if hex::decode_to_slice(value, &mut ih).is_ok() {
                                infohash = Some(ih);
                            }
                        }
                    }
                    "port" => {
                        port = value.parse().ok();
                    }
                    "cookie" => {
                        cookie_match = value == our_cookie;
                    }
                    _ => {}
                }
            }
        }

        // 过滤自己发出的消息
        if cookie_match {
            return None;
        }

        infohash.map(|ih| (ih, port))
    }

    /// 构建 LPD 多播消息
    fn build_lpd_message(infohash: &Infohash, listen_port: u16, cookie: &str) -> Vec<u8> {
        let msg = format!(
            "BT-SEARCH * HTTP/1.1\r\n\
             Host: {}:{}\r\n\
             Port: {}\r\n\
             Infohash: {}\r\n\
             cookie: {}\r\n\
             \r\n",
            LPD_MULTICAST_ADDR,
            LPD_MULTICAST_PORT,
            listen_port,
            hex::encode(infohash),
            cookie
        );
        msg.into_bytes()
    }

    /// 发送多播查询
    async fn broadcast_query(&self, infohash: &Infohash) -> Result<()> {
        // 限流检查
        {
            let mut last = self.last_broadcast.write();
            if let Some(time) = last.get(infohash) {
                if time.elapsed() < self.config.min_broadcast_interval {
                    debug!(
                        "[lpd] infohash {} 最近已广播过，跳过",
                        hex::encode(infohash)
                    );
                    return Ok(());
                }
            }
            last.insert(*infohash, Instant::now());
        }

        // 创建发送 socket
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        socket.set_multicast_loop_v4(true)?;

        let msg = Self::build_lpd_message(infohash, self.config.listen_port, &self.config.cookie);
        let multicast = SocketAddrV4::new(self.config.multicast_addr, self.config.multicast_port);
        socket.send_to(&msg, multicast).await?;

        debug!("[lpd] 发送多播查询: infohash={}", hex::encode(infohash));
        Ok(())
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
impl PeerDiscoverer for LpdDiscoverer {
    fn name(&self) -> &str {
        "lpd"
    }

    fn discoverer_type(&self) -> DiscovererType {
        DiscovererType::Lpd
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

        // 确保监听已启动
        self.init().await?;

        // 发送多播查询
        let _ = self.broadcast_query(infohash).await;

        // 等待响应
        tokio::time::sleep(self.config.query_wait_time).await;

        // 获取已收集的 peer
        let peers: Vec<PeerInfo> = {
            let discovered = self.discovered_peers.read();
            discovered
                .get(infohash)
                .map(|list| {
                    list.iter()
                        .take(limit)
                        .map(|p| PeerInfo::new(p.addr, PeerSource::Lpd))
                        .collect()
                })
                .unwrap_or_default()
        };

        self.record_result(true, peers.len(), start.elapsed());

        debug!(
            "[lpd] 发现完成: {} 个 peer (耗时 {:?})",
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
        // LPD 不需要 announce，通过多播广播即可
        Ok(())
    }

    async fn health_check(&self) -> bool {
        *self.listener_started.read()
    }

    fn stats(&self) -> DiscovererStats {
        self.stats.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lpd_config_default() {
        let config = LpdConfig::default();
        assert_eq!(config.listen_port, 6881);
        assert_eq!(config.multicast_port, 6771);
        assert!(!config.cookie.is_empty());
        assert!(config.enabled);
    }

    #[test]
    fn test_build_lpd_message() {
        let infohash = [1u8; 20];
        let msg = LpdDiscoverer::build_lpd_message(&infohash, 6881, "testcookie");
        let text = String::from_utf8_lossy(&msg);
        assert!(text.contains("BT-SEARCH"));
        assert!(text.contains("Infohash: 0101010101010101010101010101010101010101"));
        assert!(text.contains("Port: 6881"));
        assert!(text.contains("cookie: testcookie"));
    }

    #[test]
    fn test_parse_lpd_message() {
        let infohash = [1u8; 20];
        let msg = LpdDiscoverer::build_lpd_message(&infohash, 6881, "othercookie");
        let (parsed_ih, port) = LpdDiscoverer::parse_lpd_message(&msg, "ourcookie").unwrap();
        assert_eq!(parsed_ih, infohash);
        assert_eq!(port, Some(6881));
    }

    #[test]
    fn test_parse_lpd_message_own_cookie() {
        let infohash = [1u8; 20];
        let msg = LpdDiscoverer::build_lpd_message(&infohash, 6881, "ourcookie");
        // 自己的消息应该被过滤
        assert!(LpdDiscoverer::parse_lpd_message(&msg, "ourcookie").is_none());
    }

    #[test]
    fn test_parse_lpd_message_invalid() {
        assert!(LpdDiscoverer::parse_lpd_message(b"GET / HTTP/1.1", "cookie").is_none());
        assert!(LpdDiscoverer::parse_lpd_message(b"", "cookie").is_none());
    }

    #[tokio::test]
    async fn test_lpd_discoverer_creation() {
        let discoverer = LpdDiscoverer::with_default_config();
        assert_eq!(discoverer.name(), "lpd");
        assert!(discoverer.is_enabled());
    }
}
