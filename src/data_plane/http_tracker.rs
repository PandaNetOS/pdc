//! HTTP Tracker 协议适配层（超级 Tracker）
//!
//! 实现 BEP 3（HTTP Tracker 协议）和 BEP 48（Tracker 扩展），
//! 对 qBittorrent 等 BT 客户端暴露标准 /announce 和 /scrape 接口。
//!
//! 背后自动多源聚合：announce 上来的 peer + 缓存 + 后端发现器（Tracker/DHT/PEX）。
//!
//! qBittorrent 只需配置一个 tracker 地址：`http://pdc-host:port/announce`

use rustc_hash::FxHashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use dashmap::DashMap;
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::storage::PeerRepoImpl;
use crate::storage::repo_traits::PeerRepository;
use crate::config::SuperTrackerConfig;
use crate::data_plane::AppState;
use crate::types::{
    AnnounceEvent, Infohash, PeerInfo, PeerSource, ScrapeEntry, TrackerAnnounceRequest,
    TrackerAnnounceResponse, TrackerScrapeResponse,
};

// ---------------------------------------------------------------------------
// 超级 Tracker 状态
// ---------------------------------------------------------------------------

/// 单个 announce peer 的存储记录
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct AnnouncedPeer {
    addr: SocketAddr,
    peer_id: Option<[u8; 20]>,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    last_seen: Instant,
    event: AnnounceEvent,
}

/// 超级 Tracker 状态
///
/// 存储所有 announce 上来的 peer，按 infohash 分组。
/// 带过期自动清理。
/// 同时双写到 PeerRepo（统一数据归口）。
pub struct SuperTrackerState {
    /// infohash -> (peer_addr -> AnnouncedPeer)
    peers: Arc<DashMap<Infohash, FxHashMap<SocketAddr, AnnouncedPeer>>>,
    /// 配置
    config: Arc<tokio::sync::RwLock<SuperTrackerConfig>>,
    /// 统一 PeerRepo（双写）
    peer_repo: Option<Arc<PeerRepoImpl>>,
}

impl SuperTrackerState {
    /// 创建新的超级 Tracker 状态
    pub fn new(config: SuperTrackerConfig) -> Self {
        let state = Self {
            peers: Arc::new(DashMap::new()),
            config: Arc::new(tokio::sync::RwLock::new(config)),
            peer_repo: None,
        };
        state.spawn_cleanup_task();
        state
    }

    /// 注入 PeerRepo（announce peer 双写到统一归口）
    pub fn with_peer_repo(mut self, repo: Arc<PeerRepoImpl>) -> Self {
        self.peer_repo = Some(repo);
        self
    }

    /// 更新配置
    pub async fn update_config(&self, config: SuperTrackerConfig) {
        *self.config.write().await = config;
    }

    /// 处理 announce 请求
    pub async fn handle_announce(
        &self,
        req: TrackerAnnounceRequest,
        cache: &PeerRepoImpl,
        trigger_backend: bool,
    ) -> TrackerAnnounceResponse {
        let config = self.config.read().await.clone();

        // 1. 存储/更新 announce 上来的 peer
        self.store_peer(&req).await;

        // 2. 收集 peer：announce 存储 + 缓存
        let mut peers = self.get_peers_for_infohash(&req.info_hash, &req.remote_addr);

        // 从缓存补充
        let cached = cache.get_peers_sync(&req.info_hash, config.max_numwant);
        for peer in cached {
            if !peers.contains(&peer.addr) && peer.addr != req.remote_addr {
                peers.push(peer.addr);
            }
        }

        // 3. 如果配置了触发后端发现且缓存不足，异步触发（不阻塞响应）
        if trigger_backend && config.trigger_backend_discovery && peers.len() < 10 {
            // 后端发现由调用方（聚合器）异步触发，这里只返回已有 peer
            debug!(
                "[super_tracker] peer 不足（{}），建议触发后端发现",
                peers.len()
            );
        }

        // 4. 限制数量
        if peers.len() > config.max_numwant {
            peers.truncate(config.max_numwant);
        }

        // 5. 统计 complete/incomplete
        let (complete, incomplete) = self.count_seeders_leechers(&req.info_hash);

        TrackerAnnounceResponse {
            interval: config.interval,
            min_interval: if config.min_interval > 0 { Some(config.min_interval) } else { None },
            tracker_id: Some("pdc".to_string()),
            complete,
            incomplete,
            peers,
            failure_reason: None,
            warning_message: None,
        }
    }

    /// 处理 scrape 请求
    pub fn handle_scrape(&self, info_hashes: &[Infohash]) -> TrackerScrapeResponse {
        let mut files = FxHashMap::default();
        for ih in info_hashes {
            let (complete, incomplete) = self.count_seeders_leechers(ih);
            files.insert(
                *ih,
                ScrapeEntry {
                    complete,
                    downloaded: 0, // 暂不统计总下载数
                    incomplete,
                    name: None,
                },
            );
        }
        TrackerScrapeResponse {
            files,
            failure_reason: None,
        }
    }

    /// 存储 announce 上来的 peer
    async fn store_peer(&self, req: &TrackerAnnounceRequest) {
        let mut entry = self.peers.entry(req.info_hash).or_default();

        // 如果是 stopped 事件，移除 peer
        if req.event == Some(AnnounceEvent::Stopped) {
            entry.remove(&req.remote_addr);
            debug!("[super_tracker] peer {} 已移除（stopped）", req.remote_addr);
            return;
        }

        let peer = AnnouncedPeer {
            addr: req.remote_addr,
            peer_id: Some(req.peer_id),
            uploaded: req.uploaded,
            downloaded: req.downloaded,
            left: req.left,
            last_seen: Instant::now(),
            event: req.event.unwrap_or(AnnounceEvent::None),
        };
        entry.insert(req.remote_addr, peer);
        debug!(
            "[super_tracker] peer {} 已存储（infohash={}）",
            req.remote_addr,
            hex::encode(&req.info_hash[..4])
        );

        // 双写到 PeerRepo（统一数据归口）
        if let Some(repo) = &self.peer_repo {
            let mut peer_info = PeerInfo::new(req.remote_addr, PeerSource::SuperTracker);
            peer_info.peer_id = Some(req.peer_id);
            repo.add_peer(req.info_hash, peer_info).await;
        }
    }

    /// 获取指定 infohash 的 peer 列表（排除请求方）
    fn get_peers_for_infohash(&self, infohash: &Infohash, exclude: &SocketAddr) -> Vec<SocketAddr> {
        self.peers
            .get(infohash)
            .map(|entry| {
                entry
                    .values()
                    .filter(|p| &p.addr != exclude)
                    .map(|p| p.addr)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// 统计做种者(complete)和下载者(incomplete)
    fn count_seeders_leechers(&self, infohash: &Infohash) -> (i64, i64) {
        self.peers
            .get(infohash)
            .map(|entry| {
                let mut complete = 0i64;
                let mut incomplete = 0i64;
                for peer in entry.values() {
                    if peer.left == 0 {
                        complete += 1;
                    } else {
                        incomplete += 1;
                    }
                }
                (complete, incomplete)
            })
            .unwrap_or((0, 0))
    }

    /// 启动过期清理任务
    fn spawn_cleanup_task(&self) {
        let peers = self.peers.clone();
        let config = self.config.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let ttl = config.read().await.peer_ttl_secs;
                let mut removed = 0;
                peers.retain(|_, entry| {
                    entry.retain(|_, peer| peer.last_seen.elapsed() < Duration::from_secs(ttl));
                    if entry.is_empty() {
                        removed += 1;
                        false
                    } else {
                        true
                    }
                });
                if removed > 0 {
                    debug!("[super_tracker] 清理了 {} 个空 infohash 条目", removed);
                }
            }
        });
    }

    /// 获取当前存储的 infohash 数量
    pub fn infohash_count(&self) -> usize {
        self.peers.len()
    }

    /// 获取当前存储的 peer 总数
    pub fn peer_count(&self) -> usize {
        self.peers.iter().map(|entry| entry.value().len()).sum()
    }

    /// 获取单个 infohash 的 swarm 统计（做种者数、下载者数、总peer数）
    pub fn get_swarm_stats(&self, infohash: &Infohash) -> Option<(u32, u32, u32)> {
        self.peers.get(infohash).map(|entry| {
            let mut seeders = 0u32;
            let mut leechers = 0u32;
            for peer in entry.values() {
                if peer.left == 0 {
                    seeders += 1;
                } else {
                    leechers += 1;
                }
            }
            (seeders, leechers, seeders + leechers)
        })
    }

    /// 获取单个 infohash 最近指定时间窗口内的 announce 次数（近似值）
    pub fn get_recent_announce_count(&self, infohash: &Infohash, window_secs: u64) -> u32 {
        self.peers
            .get(infohash)
            .map(|entry| {
                let cutoff = Instant::now() - Duration::from_secs(window_secs);
                entry.values().filter(|p| p.last_seen >= cutoff).count() as u32
            })
            .unwrap_or(0)
    }

    /// 处理 UDP announce（BEP 15 服务端）
    ///
    /// 与 handle_announce 类似，但不依赖 HTTP 请求结构。
    /// 返回 (peer列表, seeders, leechers)
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_udp_announce(
        &self,
        infohash: Infohash,
        peer_id: [u8; 20],
        port: u16,
        remote_addr: SocketAddr,
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: AnnounceEvent,
        cache: &PeerRepoImpl,
    ) -> (Vec<SocketAddr>, i64, i64) {
        let config = self.config.read().await.clone();

        // 构造 peer 地址（用源 IP + announce 中的 port）
        let peer_addr = SocketAddr::new(remote_addr.ip(), port);

        // 存储 peer
        {
            let mut entry = self.peers.entry(infohash).or_default();
            if event == AnnounceEvent::Stopped {
                entry.remove(&peer_addr);
            } else {
                let peer = AnnouncedPeer {
                    addr: peer_addr,
                    peer_id: Some(peer_id),
                    uploaded,
                    downloaded,
                    left,
                    last_seen: Instant::now(),
                    event,
                };
                entry.insert(peer_addr, peer);
            }
        }

        // 双写到 PeerRepo（统一数据归口）
        if event != AnnounceEvent::Stopped {
            let mut peer_info = crate::types::PeerInfo::new(peer_addr, crate::types::PeerSource::SuperTracker);
            peer_info.peer_id = Some(peer_id);
            cache.add_peers_sync(&infohash, &[peer_info]);
        }

        // 收集 peer
        let mut peers = self.get_peers_for_infohash(&infohash, &peer_addr);

        // 从缓存补充
        let cached = cache.get_peers_sync(&infohash, config.max_numwant);
        for peer in cached {
            if !peers.contains(&peer.addr) && peer.addr != peer_addr {
                peers.push(peer.addr);
            }
        }

        if peers.len() > config.max_numwant {
            peers.truncate(config.max_numwant);
        }

        let (complete, incomplete) = self.count_seeders_leechers(&infohash);
        (peers, complete, incomplete)
    }
}

// ---------------------------------------------------------------------------
// HTTP 路由
// ---------------------------------------------------------------------------

/// announce 请求查询参数（保留用于文档参考，实际使用 RawQuery 手动解析）
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct AnnounceQuery {
    info_hash: String,
    peer_id: String,
    port: u16,
    uploaded: Option<u64>,
    downloaded: Option<u64>,
    left: Option<u64>,
    event: Option<String>,
    compact: Option<u8>,
    numwant: Option<usize>,
    no_peer_id: Option<u8>,
    key: Option<String>,
    trackerid: Option<String>,
}

/// scrape 请求查询参数（保留用于文档参考，实际使用 RawQuery 手动解析）
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ScrapeQuery {
    info_hash: Option<Vec<String>>,
}

/// 构建超级 Tracker 路由
pub fn routes(state: AppState) -> Router {
    Router::new()
        .route("/announce", get(announce_handler))
        .route("/scrape", get(scrape_handler))
        .with_state(state)
}

/// announce 处理函数
async fn announce_handler(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
    remote_addr: Option<axum::extract::ConnectInfo<SocketAddr>>,
) -> Response {
    // 手动解析查询参数（支持非 UTF-8 的 info_hash / peer_id 原始字节）
    let query_str = raw_query.as_deref().unwrap_or("");
    let params = parse_query_params(query_str);

    // 解析 infohash（URL 编码的 20 字节）
    let info_hash_str = match params.get("info_hash").and_then(|v| v.first()) {
        Some(s) => s.clone(),
        None => {
            warn!("[super_tracker] 缺少 info_hash 参数");
            return error_response("missing info_hash");
        }
    };
    let info_hash = match parse_info_hash(&info_hash_str) {
        Ok(ih) => ih,
        Err(e) => {
            warn!("[super_tracker] 无效的 info_hash: {}", e);
            return error_response("invalid info_hash");
        }
    };

    // 解析 peer_id
    let peer_id_str = params.get("peer_id").and_then(|v| v.first()).cloned().unwrap_or_default();
    let peer_id = parse_peer_id(&peer_id_str).unwrap_or_default();

    // 解析 port
    let port: u16 = params
        .get("port")
        .and_then(|v| v.first())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // 获取远程地址
    let client_addr = remote_addr
        .map(|ca| ca.0)
        .unwrap_or_else(|| SocketAddr::new(IpAddr::from([127, 0, 0, 1]), 0));

    let req = TrackerAnnounceRequest {
        info_hash,
        peer_id,
        port,
        uploaded: params.get("uploaded").and_then(|v| v.first()).and_then(|s| s.parse().ok()).unwrap_or(0),
        downloaded: params.get("downloaded").and_then(|v| v.first()).and_then(|s| s.parse().ok()).unwrap_or(0),
        left: params.get("left").and_then(|v| v.first()).and_then(|s| s.parse().ok()).unwrap_or(0),
        event: params.get("event").and_then(|v| v.first()).and_then(|s| s.parse().ok()),
        compact: params.get("compact").and_then(|v| v.first()).and_then(|s| s.parse::<u8>().ok()).unwrap_or(1) == 1,
        numwant: params.get("numwant").and_then(|v| v.first()).and_then(|s| s.parse().ok()),
        no_peer_id: params.get("no_peer_id").and_then(|v| v.first()).and_then(|s| s.parse::<u8>().ok()).unwrap_or(0) == 1,
        key: params.get("key").and_then(|v| v.first()).cloned(),
        trackerid: params.get("trackerid").and_then(|v| v.first()).cloned(),
        remote_addr: client_addr,
    };

    debug!(
        "[super_tracker] announce: infohash={}, port={}, event={:?}, addr={}",
        hex::encode(&info_hash[..4]),
        port,
        req.event,
        client_addr
    );

    let config = state.config.read().clone();
    let response = state
        .super_tracker
        .handle_announce(
            req,
            &state.peer_repo,
            config.super_tracker.trigger_backend_discovery,
        )
        .await;

    // 统一数据归口：announce 的 infohash 注册到 InfohashRepo
    if let Some(ref repo) = state.infohash_repo {
        repo.register_sync(info_hash, "http_announce");
    }

    // 如果 peer 不足，异步触发后端发现（不阻塞响应）
    if response.peers.len() < 10 && config.super_tracker.trigger_backend_discovery {
        let cp = state.control_plane.clone();
        let cache = state.peer_repo.clone();
        let bus = state.event_bus.clone();
        tokio::spawn(async move {
            trigger_backend_discovery(cp, cache, bus, info_hash).await;
        });
    }

    // announce 的 peer 已存入 PeerRepo（见上方 cache.add_peers），DhtProbe 会统一从 PeerRepo 拉取探测

    let body = response.to_bencode_compact();
    (StatusCode::OK, [("content-type", "text/plain")], body).into_response()
}

/// scrape 处理函数
async fn scrape_handler(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    // 手动解析查询参数（scrape 可能有多个 info_hash）
    let query_str = raw_query.as_deref().unwrap_or("");
    let params = parse_query_params(query_str);

    let info_hashes: Vec<Infohash> = params
        .get("info_hash")
        .map(|values| {
            values
                .iter()
                .filter_map(|h| parse_info_hash(h).ok())
                .collect()
        })
        .unwrap_or_default();

    if info_hashes.is_empty() {
        return error_response("no info_hash provided");
    }

    // 统一数据归口：scrape 的 infohash 注册到 InfohashRepo
    if let Some(ref repo) = state.infohash_repo {
        for ih in &info_hashes {
            repo.register_sync(*ih, "http_scrape");
        }
    }

    let response = state.super_tracker.handle_scrape(&info_hashes);
    let body = response.to_bencode();

    (StatusCode::OK, [("content-type", "text/plain")], body).into_response()
}

// ---------------------------------------------------------------------------
// 辅助函数
// ---------------------------------------------------------------------------

/// 手动解析 URL 查询字符串
///
/// 返回 key -> Vec<value> 的映射（同一个 key 可能出现多次，如 scrape 的 info_hash）
/// value 保留原始 percent-encoded 字符串，由调用方自行解码，避免 from_utf8_lossy 破坏非 UTF-8 原始字节
fn parse_query_params(raw_query: &str) -> FxHashMap<String, Vec<String>> {
    let mut params: FxHashMap<String, Vec<String>> = FxHashMap::default();

    for pair in raw_query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.find('=') {
            Some(idx) => (&pair[..idx], &pair[idx + 1..]),
            None => (pair, ""),
        };

        // key 都是 ASCII，直接使用；value 保留原始 percent-encoded 字符串
        params.entry(key.to_string()).or_default().push(value.to_string());
    }

    params
}

/// 解析 URL 编码的 infohash
fn parse_info_hash(s: &str) -> Result<Infohash, String> {
    // 尝试 hex 解码
    if s.len() == 40 {
        if let Ok(bytes) = hex::decode(s) {
            if bytes.len() == 20 {
                let mut arr = [0u8; 20];
                arr.copy_from_slice(&bytes);
                return Ok(arr);
            }
        }
    }
    // 尝试 URL 解码（percent-encoded raw bytes）
    let decoded = percent_decode(s);
    if decoded.len() == 20 {
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&decoded);
        return Ok(arr);
    }
    Err(format!("invalid info_hash length: {}", decoded.len()))
}

/// 解析 peer_id
fn parse_peer_id(s: &str) -> Result<[u8; 20], String> {
    let decoded = percent_decode(s);
    if decoded.len() == 20 {
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&decoded);
        Ok(arr)
    } else {
        Err(format!("invalid peer_id length: {}", decoded.len()))
    }
}

/// 简单的 percent-decode
fn percent_decode(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let mut result = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex_str = &s[i + 1..i + 3];
            if let Ok(byte) = u8::from_str_radix(hex_str, 16) {
                result.push(byte);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }
    result
}

/// 生成错误响应（bencode 格式）
fn error_response(reason: &str) -> Response {
    let body = format!("d14:failure reason{}:{}e", reason.len(), reason);
    (
        StatusCode::OK,
        [("content-type", "text/plain")],
        body.into_bytes(),
    )
        .into_response()
}

/// 异步触发后端发现器
async fn trigger_backend_discovery(
    control_plane: crate::control_plane::ControlPlane,
    cache: Arc<PeerRepoImpl>,
    event_bus: crate::event_bus::EventBus,
    infohash: Infohash,
) {
    use crate::types::Event;

    let policy = control_plane.policy();
    let registry = control_plane.registry();
    let results = registry
        .discover_all(
            &infohash,
            policy.max_peers,
            policy.max_concurrent,
            policy.timeout,
        )
        .await;

    let mut all_peers: Vec<PeerInfo> = vec![];
    for (name, result, _duration) in results {
        match result {
            Ok(peers) => {
                debug!("[backend_discovery] {} 返回 {} 个 peer", name, peers.len());
                all_peers.extend(peers);
            }
            Err(e) => {
                warn!("[backend_discovery] {} 失败: {}", name, e);
            }
        }
    }

    if !all_peers.is_empty() {
        // 去重
        all_peers.sort_by_key(|p| p.addr);
        all_peers.dedup_by_key(|p| p.addr);

        cache.add_peers_sync(&infohash, &all_peers);
        event_bus.publish(Event::PeerDiscovered {
            infohash,
            peers: all_peers.clone(),
            source: "backend_triggered".to_string(),
        });
        info!(
            "[backend_discovery] 异步发现完成，新增 {} 个 peer（infohash={}）",
            all_peers.len(),
            hex::encode(&infohash[..4])
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percent_decode() {
        assert_eq!(percent_decode("hello"), b"hello");
        assert_eq!(percent_decode("%41%42%43"), b"ABC");
        assert_eq!(percent_decode("a%20b"), b"a b");
    }

    #[test]
    fn test_parse_info_hash_hex() {
        let hex_str = "a".repeat(40);
        let result = parse_info_hash(&hex_str);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), [0xaa; 20]);
    }

    #[test]
    fn test_parse_info_hash_percent() {
        // 20 个 0x01 字节的 percent 编码
        let mut s = String::new();
        for _ in 0..20 {
            s.push_str("%01");
        }
        let result = parse_info_hash(&s);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), [0x01; 20]);
    }

    #[test]
    fn test_error_response() {
        let resp = error_response("test error");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_parse_query_params_basic() {
        let params = parse_query_params("port=6881&uploaded=100&downloaded=0");
        assert_eq!(params.get("port").unwrap()[0], "6881");
        assert_eq!(params.get("uploaded").unwrap()[0], "100");
        assert_eq!(params.get("downloaded").unwrap()[0], "0");
    }

    #[test]
    fn test_parse_query_params_multiple_info_hash() {
        let params = parse_query_params("info_hash=aaa&info_hash=bbb");
        let info_hashes = params.get("info_hash").unwrap();
        assert_eq!(info_hashes.len(), 2);
        assert_eq!(info_hashes[0], "aaa");
        assert_eq!(info_hashes[1], "bbb");
    }

    #[test]
    fn test_parse_query_params_percent_encoded() {
        // 模拟 info_hash 包含非 ASCII 字节
        let params = parse_query_params("info_hash=%01%02%03&peer_id=abc");
        // value 保留原始 percent-encoded 字符串，由 parse_info_hash 自行解码
        assert_eq!(params.get("info_hash").unwrap()[0], "%01%02%03");
        assert_eq!(params.get("peer_id").unwrap()[0], "abc");
    }

    #[tokio::test]
    async fn test_super_tracker_store_and_get() {
        let config = SuperTrackerConfig::default();
        let state = SuperTrackerState::new(config);
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        let cache = PeerRepoImpl::new(storage);

        let infohash = [0u8; 20];
        let addr = SocketAddr::new(IpAddr::from([127, 0, 0, 1]), 6881);

        let req = TrackerAnnounceRequest {
            info_hash: infohash,
            peer_id: [0u8; 20],
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 1000,
            event: Some(AnnounceEvent::Started),
            compact: true,
            numwant: None,
            no_peer_id: false,
            key: None,
            trackerid: None,
            remote_addr: addr,
        };

        let resp = state.handle_announce(req, &cache, false).await;
        // 第一个 peer announce 时，排除自己后应该返回 0 个 peer
        assert_eq!(resp.peers.len(), 0);
        assert_eq!(resp.incomplete, 1);

        // 第二个 peer announce
        let addr2 = SocketAddr::new(IpAddr::from([127, 0, 0, 2]), 6882);
        let req2 = TrackerAnnounceRequest {
            info_hash: infohash,
            peer_id: [1u8; 20],
            port: 6882,
            uploaded: 0,
            downloaded: 0,
            left: 0, // 做种者
            event: Some(AnnounceEvent::Started),
            compact: true,
            numwant: None,
            no_peer_id: false,
            key: None,
            trackerid: None,
            remote_addr: addr2,
        };
        let resp2 = state.handle_announce(req2, &cache, false).await;
        // 第二个 peer 应该能看到第一个 peer
        assert_eq!(resp2.peers.len(), 1);
        assert_eq!(resp2.complete, 1);
        assert_eq!(resp2.incomplete, 1);
    }

    #[tokio::test]
    async fn test_scrape() {
        let config = SuperTrackerConfig::default();
        let state = SuperTrackerState::new(config);
        let infohash = [0u8; 20];
        let resp = state.handle_scrape(&[infohash]);
        assert!(resp.files.contains_key(&infohash));
    }
}
