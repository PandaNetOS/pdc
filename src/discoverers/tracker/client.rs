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
use crate::storage::repo_traits::TrackerRepository;

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
#[derive(Debug, Clone)]
struct TrackerState {
    /// 连续失败次数
    consecutive_failures: u32,
    /// 最后一次失败时间
    last_failure_at: Option<Instant>,
    /// 是否被临时禁用
    disabled: bool,
    /// 统计
    stats: DiscovererStats,
    /// 累计响应时间（毫秒）
    total_response_time_ms: f64,
    /// 综合评分（0-100）
    score: f64,
    /// 最后一次评分时间
    last_score_at: Option<Instant>,
}

impl Default for TrackerState {
    fn default() -> Self {
        Self {
            consecutive_failures: 0,
            last_failure_at: None,
            disabled: false,
            stats: DiscovererStats::default(),
            total_response_time_ms: 0.0,
            score: 0.0,
            last_score_at: None,
        }
    }
}

/// Tracker 发现器
pub struct TrackerDiscoverer {
    config: TrackerConfig,
    client: Client,
    states: Arc<RwLock<HashMap<String, TrackerState>>>,
    tracker_repo: Option<Arc<crate::storage::TrackerRepoImpl>>,
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

        let discoverer = Self {
            config,
            client,
            states: Arc::new(RwLock::new(states)),
            tracker_repo: None,
        };
        // 初始化评分（新 tracker 默认 15 分在线率）
        discoverer.recalculate_scores();
        discoverer
    }

    /// 注入 TrackerRepo（统一数据归口 + 持久化）
    pub fn with_tracker_repo(mut self, repo: Arc<crate::storage::TrackerRepoImpl>) -> Self {
        self.tracker_repo = Some(repo);
        // 将所有内置 tracker 同步到 repo
        let trackers: Vec<String> = self.config.trackers.clone();
        let repo_clone = self.tracker_repo.clone().unwrap();
        tokio::spawn(async move {
            for url in &trackers {
                repo_clone.add_tracker(url.clone()).await;
            }
        });
        self
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
                state.total_response_time_ms += duration.as_millis() as f64;
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

        // 同步到 TrackerRepo（统一数据归口）
        if let Some(repo) = &self.tracker_repo {
            let url = tracker_url.to_string();
            let repo = repo.clone();
            let peers = peers_count as u64;
            let latency = duration.as_millis() as u64;
            tokio::spawn(async move {
                repo.record_request(&url, success, peers, latency).await;
            });
        }
    }

    /// 重新计算所有 tracker 的评分
    pub fn recalculate_scores(&self) {
        let mut states = self.states.write();
        let mut scored: Vec<(String, f64)> = Vec::new();
        for (url, state) in states.iter_mut() {
            let tracker_stats = crate::intelligence::TrackerStats {
                total_requests: state.stats.total_requests,
                success_requests: state.stats.success_requests,
                failed_requests: state.stats.failed_requests,
                total_peers_discovered: state.stats.total_peers_discovered,
                total_response_time_ms: state.total_response_time_ms,
                consecutive_failures: state.consecutive_failures,
                disabled: state.disabled,
            };
            let score = tracker_stats.calculate_score();
            state.score = score.total;
            state.last_score_at = Some(Instant::now());
            scored.push((url.clone(), score.total));
            debug!("[tracker] {} 评分: {:.1}", url, score.total);
        }

        // 同步评分到 TrackerRepo
        if let Some(repo) = &self.tracker_repo {
            let repo = repo.clone();
            tokio::spawn(async move {
                for (url, score) in &scored {
                    repo.update_score(url, *score).await;
                }
            });
        }
    }

    /// 获取所有 tracker 的评分列表
    pub fn get_tracker_scores(&self) -> Vec<(String, f64, bool)> {
        let states = self.states.read();
        let mut scores: Vec<(String, f64, bool)> = states
            .iter()
            .map(|(url, s)| (url.clone(), s.score, s.disabled))
            .collect();
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores
    }

    /// 动态添加 tracker 到池
    pub fn add_tracker(&self, url: &str) -> bool {
        let mut states = self.states.write();
        if states.contains_key(url) {
            return false;
        }
        states.insert(url.to_string(), TrackerState::default());
        info!("[tracker] 动态添加 tracker: {}", url);

        // 同步到 TrackerRepo
        if let Some(repo) = &self.tracker_repo {
            let repo = repo.clone();
            let url = url.to_string();
            tokio::spawn(async move {
                repo.add_tracker(url).await;
            });
        }
        true
    }

    /// 动态移除 tracker
    pub fn remove_tracker(&self, url: &str) -> bool {
        let mut states = self.states.write();
        let removed = states.remove(url).is_some();
        if removed {
            info!("[tracker] 动态移除 tracker: {}", url);
        }

        // 同步到 TrackerRepo
        if removed {
            if let Some(repo) = &self.tracker_repo {
                let repo = repo.clone();
                let url = url.to_string();
                tokio::spawn(async move {
                    repo.remove_tracker(&url).await;
                });
            }
        }
        removed
    }

    /// 淘汰低评分 tracker（score < threshold 且有足够请求样本）
    pub fn prune_low_score(&self, threshold: f64, min_requests: u64) -> usize {
        let mut states = self.states.write();
        let before = states.len();
        states.retain(|url, s| {
            let keep = s.stats.total_requests < min_requests || s.score >= threshold;
            if !keep {
                info!("[tracker] 淘汰低评分 tracker: {} (score={:.1})", url, s.score);
            }
            keep
        });
        before - states.len()
    }

    /// 获取按评分排序的活跃 tracker 列表
    fn active_trackers_sorted(&self) -> Vec<String> {
        let scores = self.get_tracker_scores();
        scores
            .into_iter()
            .filter(|(_, _, disabled)| !disabled)
            .map(|(url, _, _)| url)
            .collect()
    }

    /// 恢复冷却期结束的 Tracker
    fn recover_cooldown_trackers(&self) {
        let mut recovered: Vec<String> = Vec::new();
        let mut states = self.states.write();
        for (url, state) in states.iter_mut() {
            if state.disabled {
                if let Some(last_failure) = state.last_failure_at {
                    if last_failure.elapsed() >= self.config.cooldown_duration {
                        state.disabled = false;
                        state.consecutive_failures = 0;
                        recovered.push(url.clone());
                        debug!("[tracker] Tracker 冷却期结束，已恢复");
                    }
                }
            }
        }

        // 同步恢复状态到 TrackerRepo
        if !recovered.is_empty() {
            if let Some(repo) = &self.tracker_repo {
                let repo = repo.clone();
                tokio::spawn(async move {
                    for url in &recovered {
                        repo.set_disabled(url, false).await;
                    }
                });
            }
        }
    }

    /// 向 tracker 发送 scrape 请求，获取 infohash 的种子/下载统计
    /// 返回 (infohash, complete, incomplete, downloaded) 列表
    pub async fn scrape(&self, infohashes: &[Infohash]) -> Vec<(Infohash, i64, i64, i64)> {
        if infohashes.is_empty() {
            return vec![];
        }

        let active_trackers = self.active_trackers_sorted();
        if active_trackers.is_empty() {
            return vec![];
        }

        let mut results = vec![];
        let mut tasks = vec![];

        for tracker_url in active_trackers.iter().take(self.config.max_concurrent_requests) {
            if !tracker_url.starts_with("http://") && !tracker_url.starts_with("https://") {
                continue;
            }

            let tracker_url = tracker_url.clone();
            let infohashes = infohashes.to_vec();
            let client = self.client.clone();

            tasks.push(tokio::spawn(async move {
                let mut url = match Url::parse(&tracker_url) {
                    Ok(u) => u,
                    Err(_) => return vec![],
                };

                url.path_segments_mut().unwrap().push("scrape");
                {
                    let mut query = url.query_pairs_mut();
                    for ih in &infohashes {
                        let ih_hex = hex::encode(ih);
                        query.append_pair("info_hash", &ih_hex);
                    }
                }

                let resp = match client.get(url.as_str()).send().await {
                    Ok(r) => r,
                    Err(_) => return vec![],
                };

                let body = match resp.bytes().await {
                    Ok(b) => b,
                    Err(_) => return vec![],
                };

                let value: serde_bencode::value::Value = match serde_bencode::from_bytes(&body) {
                    Ok(v) => v,
                    Err(_) => return vec![],
                };

                let dict = match value {
                    serde_bencode::value::Value::Dict(d) => d,
                    _ => return vec![],
                };

                let files = match dict.get(&b"files".to_vec()) {
                    Some(f) => f,
                    None => return vec![],
                };

                let files_dict = match files {
                    serde_bencode::value::Value::Dict(d) => d,
                    _ => return vec![],
                };

                let mut result = vec![];
                for (key, value) in files_dict {
                    let mut ih = [0u8; 20];
                    if key.len() == 20 {
                        ih.copy_from_slice(&key);
                    } else if key.len() == 40 {
                        if hex::decode_to_slice(&key, &mut ih).is_err() {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    let file_dict = match value {
                        serde_bencode::value::Value::Dict(d) => d,
                        _ => continue,
                    };

                    let complete = file_dict.get(&b"complete".to_vec()).and_then(|v| match v {
                        serde_bencode::value::Value::Int(i) => Some(*i),
                        _ => None,
                    }).unwrap_or(0);
                    let incomplete = file_dict.get(&b"incomplete".to_vec()).and_then(|v| match v {
                        serde_bencode::value::Value::Int(i) => Some(*i),
                        _ => None,
                    }).unwrap_or(0);
                    let downloaded = file_dict.get(&b"downloaded".to_vec()).and_then(|v| match v {
                        serde_bencode::value::Value::Int(i) => Some(*i),
                        _ => None,
                    }).unwrap_or(0);

                    result.push((ih, complete, incomplete, downloaded));
                }

                result
            }));
        }

        for task in tasks {
            if let Ok(result) = task.await {
                results.extend(result);
            }
        }

        results
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
        self.recalculate_scores();

        let active_trackers = self.active_trackers_sorted();
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

    fn tracker_scores(&self) -> Option<Vec<(String, f64, bool)>> {
        Some(self.get_tracker_scores())
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
