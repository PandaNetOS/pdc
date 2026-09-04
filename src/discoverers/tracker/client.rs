//! Tracker 客户端
//!
//! 实现 BitTorrent Tracker 协议（HTTP/UDP），发现 peer。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use parking_lot::RwLock;
use rand::Rng;
use reqwest::Client;
use tracing::{debug, info, warn};
use url::Url;

use crate::traits::{AnnounceEvent, DiscovererStats, DiscovererType, PeerDiscoverer};
use crate::types::{Infohash, PeerInfo, PeerSource};

use super::udp::UdpTrackerClient;

/// Tracker 配置
#[derive(Debug, Clone)]
pub struct TrackerConfig {
    /// Tracker URL 列表
    pub trackers: Vec<String>,
    /// 请求超时
    pub timeout: Duration,
    /// 最大并发请求数
    pub max_concurrent_requests: usize,
    /// 连续失败次数阈值（超过后暂时禁用）
    pub max_consecutive_failures: u32,
    /// 禁用恢复时间
    pub cooldown_duration: Duration,
    /// 本地监听端口（用于 announce）
    pub listen_port: u16,
    /// 上传速度（字节/秒）
    pub uploaded: u64,
    /// 下载速度（字节/秒）
    pub downloaded: u64,
    /// 剩余字节数
    pub left: u64,
    /// 客户端标识
    pub peer_id: [u8; 20],
    /// User-Agent
    pub user_agent: String,
}

impl Default for TrackerConfig {
    fn default() -> Self {
        let mut peer_id = [0u8; 20];
        let mut rng = rand::thread_rng();
        for byte in peer_id.iter_mut() {
            *byte = rng.gen();
        }
        // PD 前缀标识 PeerDiscoveryCenter
        peer_id[0] = b'P';
        peer_id[1] = b'D';
        peer_id[2] = b'-';
        peer_id[3] = b'0';
        peer_id[4] = b'2';
        peer_id[5] = b'0';
        peer_id[6] = b'0';
        peer_id[7] = b'-';

        Self {
            trackers: crate::discoverers::tracker::PUBLIC_TRACKERS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            timeout: Duration::from_secs(15),
            max_concurrent_requests: 10,
            max_consecutive_failures: 3,
            cooldown_duration: Duration::from_secs(300),
            listen_port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 0,
            peer_id,
            user_agent: "PeerDiscoveryCenter/0.2.0".to_string(),
        }
    }
}

/// Tracker 状态
#[derive(Debug, Clone, Default)]
struct TrackerState {
    /// 连续失败次数
    consecutive_failures: u32,
    /// 最后一次失败时间
    last_failure_at: Option<Instant>,
    /// 是否被临时禁用
    disabled: bool,
    /// 统计
    stats: DiscovererStats,
}

/// Tracker 发现器
pub struct TrackerDiscoverer {
    config: TrackerConfig,
    client: Client,
    states: Arc<RwLock<HashMap<String, TrackerState>>>,
}

impl TrackerDiscoverer {
    /// 创建新的 Tracker 发现器
    pub fn new(config: TrackerConfig) -> Self {
        let client = Client::builder()
            .timeout(config.timeout)
            .user_agent(&config.user_agent)
            .build()
            .expect("failed to build reqwest client");

        let mut states = HashMap::new();
        for tracker in &config.trackers {
            states.insert(tracker.clone(), TrackerState::default());
        }

        Self {
            config,
            client,
            states: Arc::new(RwLock::new(states)),
        }
    }

    /// 创建默认配置的 Tracker 发现器
    pub fn with_default_config() -> Self {
        Self::new(TrackerConfig::default())
    }

    /// 使用自定义 Tracker 列表
    pub fn with_trackers(trackers: Vec<String>) -> Self {
        let config = TrackerConfig {
            trackers,
            ..Default::default()
        };
        Self::new(config)
    }

    /// 获取活跃的 Tracker 列表（未被禁用的）
    fn active_trackers(&self) -> Vec<String> {
        let states = self.states.read();
        self.config
            .trackers
            .iter()
            .filter(|t| states.get(*t).map(|s| !s.disabled).unwrap_or(true))
            .cloned()
            .collect()
    }

    /// 从 HTTP Tracker 响应解析 peer 列表
    fn parse_http_peers(body: &[u8]) -> Result<Vec<SocketAddr>> {
        let body_str = String::from_utf8_lossy(body);

        // 尝试解析 compact peers（二进制格式）
        if let Some(peers_start) = body_str.find("5:peers") {
            let rest = &body_str[peers_start + 7..];
            if let Some(len_str) = rest.split(':').next() {
                if let Ok(len) = len_str.parse::<usize>() {
                    let data_start = len_str.len() + 1;
                    if data_start + len <= rest.len() {
                        let data = &rest.as_bytes()[data_start..data_start + len];
                        return Ok(Self::parse_compact_peers(data));
                    }
                }
            }
        }

        Ok(vec![])
    }

    /// 解析 compact peers 格式（每 6 字节一个 peer：4字节IP + 2字节端口）
    fn parse_compact_peers(data: &[u8]) -> Vec<SocketAddr> {
        let mut peers = vec![];
        let (chunks, _) = data.as_chunks::<6>();
        for chunk in chunks {
            let ip = std::net::Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            peers.push(SocketAddr::new(std::net::IpAddr::V4(ip), port));
        }
        peers
    }

    /// 记录请求结果
    fn record_result(
        &self,
        tracker_url: &str,
        success: bool,
        peers_count: usize,
        duration: Duration,
    ) {
        let mut states = self.states.write();
        if let Some(state) = states.get_mut(tracker_url) {
            if success {
                state.consecutive_failures = 0;
                state.disabled = false;
                state
                    .stats
                    .record_success(peers_count, duration.as_millis() as f64);
            } else {
                state.consecutive_failures += 1;
                state.last_failure_at = Some(Instant::now());
                state.stats.record_failure();

                if state.consecutive_failures >= self.config.max_consecutive_failures {
                    state.disabled = true;
                    warn!(
                        "[tracker] Tracker {} 连续失败 {} 次，已临时禁用",
                        tracker_url, state.consecutive_failures
                    );
                }
            }
        }
    }

    /// 恢复冷却期结束的 Tracker
    fn recover_cooldown_trackers(&self) {
        let mut states = self.states.write();
        for state in states.values_mut() {
            if state.disabled {
                if let Some(last_failure) = state.last_failure_at {
                    if last_failure.elapsed() >= self.config.cooldown_duration {
                        state.disabled = false;
                        state.consecutive_failures = 0;
                        debug!("[tracker] Tracker 冷却期结束，已恢复");
                    }
                }
            }
        }
    }
}

#[async_trait]
impl PeerDiscoverer for TrackerDiscoverer {
    fn name(&self) -> &str {
        "tracker"
    }

    fn discoverer_type(&self) -> DiscovererType {
        DiscovererType::Tracker
    }

    fn is_enabled(&self) -> bool {
        !self.config.trackers.is_empty()
    }

    async fn discover_peers(
        &self,
        infohash: &Infohash,
        limit: usize,
    ) -> anyhow::Result<Vec<PeerInfo>> {
        self.recover_cooldown_trackers();

        let active_trackers = self.active_trackers();
        if active_trackers.is_empty() {
            warn!("[tracker] 没有活跃的 Tracker");
            return Ok(vec![]);
        }

        debug!(
            "[tracker] 开始向 {} 个 Tracker 请求 peer",
            active_trackers.len()
        );

        let mut tasks = vec![];
        for tracker_url in active_trackers
            .iter()
            .take(self.config.max_concurrent_requests)
        {
            let tracker_url = tracker_url.clone();
            let infohash = *infohash;
            let self_clone = self.client.clone();
            let config = self.config.clone();

            tasks.push(tokio::spawn(async move {
                let start = Instant::now();

                if tracker_url.starts_with("http://") || tracker_url.starts_with("https://") {
                    let result = async {
                        let url = Url::parse(&tracker_url)?;
                        let infohash_hex = hex::encode(infohash);
                        let peer_id_hex = hex::encode(config.peer_id);

                        let mut request_url = url;
                        request_url
                            .query_pairs_mut()
                            .append_pair("info_hash", &infohash_hex)
                            .append_pair("peer_id", &peer_id_hex)
                            .append_pair("port", &config.listen_port.to_string())
                            .append_pair("uploaded", "0")
                            .append_pair("downloaded", "0")
                            .append_pair("left", "0")
                            .append_pair("event", "started")
                            .append_pair("compact", "1")
                            .append_pair("numwant", &limit.to_string());

                        let response = self_clone.get(request_url.as_str()).send().await?;

                        if !response.status().is_success() {
                            return Err(anyhow!("HTTP status: {}", response.status()));
                        }

                        let body = response.bytes().await?;
                        let peers = TrackerDiscoverer::parse_http_peers(&body)?;
                        Ok(peers)
                    }
                    .await;

                    (tracker_url, result, start.elapsed())
                } else if tracker_url.starts_with("udp://") {
                    // UDP Tracker（BEP 15）
                    let result = UdpTrackerClient::announce(
                        &tracker_url,
                        &infohash,
                        &config.peer_id,
                        config.listen_port,
                        config.timeout,
                    )
                    .await;
                    (tracker_url, result, start.elapsed())
                } else {
                    (
                        tracker_url,
                        Err(anyhow!("unsupported tracker protocol")),
                        start.elapsed(),
                    )
                }
            }));
        }

        let mut all_peers = vec![];
        for task in tasks {
            if let Ok((tracker_url, result, duration)) = task.await {
                match result {
                    Ok(peers) => {
                        self.record_result(&tracker_url, true, peers.len(), duration);
                        all_peers.extend(peers);
                    }
                    Err(e) => {
                        self.record_result(&tracker_url, false, 0, duration);
                        debug!("[tracker] Tracker {} 失败: {}", tracker_url, e);
                    }
                }
            }
        }

        all_peers.sort();
        all_peers.dedup();

        let peer_infos: Vec<PeerInfo> = all_peers
            .iter()
            .take(limit)
            .map(|addr| PeerInfo::new(*addr, PeerSource::Tracker))
            .collect();

        info!("[tracker] 发现完成: {} 个 peer (去重后)", peer_infos.len());

        Ok(peer_infos)
    }

    async fn announce(
        &self,
        infohash: &Infohash,
        port: u16,
        event: AnnounceEvent,
    ) -> anyhow::Result<()> {
        let active_trackers = self.active_trackers();
        let mut tasks = vec![];

        for tracker_url in active_trackers.iter().take(5) {
            let tracker_url = tracker_url.clone();
            let infohash = *infohash;
            let client = self.client.clone();
            let peer_id = self.config.peer_id;

            tasks.push(tokio::spawn(async move {
                if tracker_url.starts_with("http://") || tracker_url.starts_with("https://") {
                    if let Ok(url) = Url::parse(&tracker_url) {
                        let infohash_hex = hex::encode(infohash);
                        let peer_id_hex = hex::encode(peer_id);

                        let mut request_url = url;
                        request_url
                            .query_pairs_mut()
                            .append_pair("info_hash", &infohash_hex)
                            .append_pair("peer_id", &peer_id_hex)
                            .append_pair("port", &port.to_string())
                            .append_pair("uploaded", "0")
                            .append_pair("downloaded", "0")
                            .append_pair("left", "0")
                            .append_pair("event", event.as_str())
                            .append_pair("compact", "1");

                        let _ = client.get(request_url.as_str()).send().await;
                    }
                }
            }));
        }

        for task in tasks {
            let _ = task.await;
        }

        Ok(())
    }

    async fn health_check(&self) -> bool {
        let states = self.states.read();
        let total = states.len();
        let healthy = states.values().filter(|s| !s.disabled).count();
        debug!("[tracker] 健康检查: {}/{} 个 Tracker 健康", healthy, total);
        healthy > 0
    }

    fn stats(&self) -> DiscovererStats {
        let states = self.states.read();
        let mut aggregated = DiscovererStats::default();
        for state in states.values() {
            aggregated.total_requests += state.stats.total_requests;
            aggregated.success_requests += state.stats.success_requests;
            aggregated.failed_requests += state.stats.failed_requests;
            aggregated.total_peers_discovered += state.stats.total_peers_discovered;
        }
        aggregated
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_compact_peers() {
        let data = [127, 0, 0, 1, 0x1A, 0xE1];
        let peers = TrackerDiscoverer::parse_compact_peers(&data);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].to_string(), "127.0.0.1:6881");
    }

    #[test]
    fn test_tracker_config_default() {
        let config = TrackerConfig::default();
        assert!(!config.trackers.is_empty());
        assert_eq!(config.peer_id[0], b'P');
        assert_eq!(config.peer_id[1], b'D');
    }

    #[tokio::test]
    async fn test_tracker_discoverer_creation() {
        let discoverer = TrackerDiscoverer::with_default_config();
        assert_eq!(discoverer.name(), "tracker");
        assert!(discoverer.is_enabled());
    }
}
