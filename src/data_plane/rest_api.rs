//! REST API
//!
//! 提供管理和查询接口：
//! - GET /health - 健康检查
//! - GET /api/v1/stats - 统计信息
//! - POST /api/v1/discover - 主动发现 peer
//! - GET /api/v1/discoverers - 发现器列表
//! - GET /api/v1/cache/{infohash} - 查询缓存

use crate::intelligence::scorer_traits::HealthScorer;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tracing::debug;

use crate::data_plane::AppState;
use crate::discoverers::tracker::PUBLIC_TRACKERS;
use crate::types::{Infohash, PeerInfo};

// ---------------------------------------------------------------------------
// 响应类型
// ---------------------------------------------------------------------------

/// 健康检查响应
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub discoverers: usize,
    pub cached_infohashes: usize,
    pub cached_peers: usize,
    pub super_tracker_infohashes: usize,
    pub super_tracker_peers: usize,
    pub uptime_seconds: u64,
    pub system_health: crate::health_check::SystemHealth,
}

/// 统计响应
#[derive(Debug, Serialize)]
pub struct TrackerScoreInfo {
    pub url: String,
    pub score: f64,
    pub disabled: bool,
}

#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub discoverer_stats: Vec<DiscovererStat>,
    pub cache_stats: CacheStats,
    pub super_tracker_stats: SuperTrackerStats,
    #[serde(default)]
    pub tracker_scores: Vec<TrackerScoreInfo>,
    #[serde(default)]
    pub fetcher_stats: Option<FetcherStats>,
}

#[derive(Debug, Serialize)]
pub struct FetcherStats {
    pub total_rounds: u64,
    pub last_round_peers: u64,
    pub total_peers_fetched: u64,
    pub infohash_repo_count: usize,
}

#[derive(Debug, Serialize)]
pub struct DiscovererStat {
    pub name: String,
    pub discoverer_type: String,
    pub enabled: bool,
    pub total_requests: u64,
    pub success_requests: u64,
    pub failed_requests: u64,
    pub total_peers_discovered: u64,
    pub success_rate: f64,
    pub avg_response_time_ms: f64,
    #[serde(default)]
    pub tracker_count: usize,
}

#[derive(Debug, Serialize)]
pub struct CacheStats {
    pub total_infohashes: usize,
    pub total_peers: usize,
}

#[derive(Debug, Serialize)]
pub struct SuperTrackerStats {
    pub total_infohashes: usize,
    pub total_peers: usize,
}

/// 发现请求
#[derive(Debug, Deserialize)]
pub struct DiscoverRequest {
    pub infohash: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub force_refresh: bool,
}

fn default_limit() -> usize {
    100
}

/// 发现响应
#[derive(Debug, Serialize)]
pub struct DiscoverResponse {
    pub infohash: String,
    pub peers: Vec<PeerInfo>,
    pub total: usize,
    pub from_cache: bool,
    pub duration_ms: u64,
}

/// 发现器列表响应
#[derive(Debug, Serialize)]
pub struct DiscovererListResponse {
    pub discoverers: Vec<DiscovererInfo>,
}

#[derive(Debug, Serialize)]
pub struct DiscovererInfo {
    pub name: String,
    pub discoverer_type: String,
    pub enabled: bool,
}

/// 缓存查询响应
#[derive(Debug, Serialize)]
pub struct CacheQueryResponse {
    pub infohash: String,
    pub peers: Vec<PeerInfo>,
    pub count: usize,
}

/// 错误响应
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Peer 连接反馈请求
///
/// 下载引擎连接 peer 后调用此接口反馈结果，
/// PDC 根据反馈动态调整 peer 优先级评分。
#[derive(Debug, Deserialize)]
pub struct PeerFeedbackRequest {
    /// infohash（hex 或 raw）
    pub infohash: String,
    /// peer 地址（ip:port）
    pub addr: String,
    /// 是否连接成功
    pub success: bool,
    /// 连接延迟（毫秒，可选）
    #[serde(default)]
    pub latency_ms: Option<u64>,
    /// 下载速度（字节/秒，可选）
    #[serde(default)]
    pub download_speed: Option<u64>,
}

/// Peer 反馈响应
#[derive(Debug, Serialize)]
pub struct PeerFeedbackResponse {
    pub status: String,
    pub updated: bool,
}

/// 爬虫状态响应
#[derive(Debug, Serialize)]
pub struct NodeInfo {
    pub addr: String,
    pub score: f64,
    pub state: String,
    pub query_count: u64,
    pub success_rate: f64,
}

#[derive(Debug, Serialize)]
pub struct CrawlerStatsResponse {
    pub enabled: bool,
    pub running: bool,
    pub nodes_crawled: u64,
    pub infohashes_collected: u64,
    pub peers_collected: u64,
    pub known_nodes: usize,
    pub messages_received: u64,
    pub requests_sent: u64,
    pub errors: u64,
    pub uptime_seconds: u64,
    #[serde(default)]
    pub top_nodes: Vec<NodeInfo>,
}

// ---------------------------------------------------------------------------
// 路由
// ---------------------------------------------------------------------------

/// 构建 REST API 路由
pub fn routes(state: AppState) -> Router {
    let event_bus = state.event_bus.clone();
    Router::new()
        .route("/health", get(health_handler))
        .route("/api/v1/stats", get(stats_handler))
        .route("/api/v1/discover", post(discover_handler))
        .route("/api/v1/discoverers", get(discoverers_handler))
        .route("/api/v1/cache/{infohash}", get(cache_query_handler))
        .route("/api/v1/peer-feedback", post(peer_feedback_handler))
        .route("/api/v1/nat/status", get(nat_status_handler))
        .route("/api/v1/crawler", get(crawler_handler))
        .route("/api/v1/utp", get(utp_handler))
        .route("/api/v1/pex", get(pex_handler))
        .route("/api/v1/history/peers/{infohash}", get(peer_history_handler))
        .route("/api/v1/history/stats/{metric}", get(stats_history_handler))
        .route("/metrics", get(crate::data_plane::metrics::metrics_handler))
        .route("/ws", get(crate::data_plane::ws::ws_handler))
        .with_state(state)
        .layer(Extension(event_bus))
}

// ---------------------------------------------------------------------------
// 处理函数
// ---------------------------------------------------------------------------

/// 健康检查
async fn health_handler(State(state): State<AppState>) -> Response {
    let registry = state.control_plane.registry();
    let cache_stats = state.peer_repo.stats();
    // 从 crawler_state 获取入站连通性评分
    let inbound_score = state.crawler_state.as_ref().map(|cs| {
        let s = cs.read();
        let uptime = s.started_at.map(|t| t.elapsed().as_secs()).unwrap_or(0);
        let inbound_per_min = if uptime > 0 {
            (s.inbound_total as f64 / uptime as f64) * 60.0
        } else { 0.0 };
        if inbound_per_min >= 10.0 { 100.0 }
        else if inbound_per_min >= 5.0 { 80.0 }
        else if inbound_per_min >= 1.0 { 60.0 }
        else if inbound_per_min >= 0.1 { 40.0 }
        else if inbound_per_min > 0.0 { 20.0 }
        else { 0.0 }
    });
    // 使用 HealthScorerImpl 统一计算健康度（唯一计算路径，评分统一收口）
    let health_scorer = crate::intelligence::health_scorer::HealthScorerImpl::new();
    let system_health = if let (Some(node_repo), Some(tracker_repo), Some(infohash_repo)) =
        (state.node_repo.as_ref(), state.tracker_repo.as_ref(), state.infohash_repo.as_ref())
    {
        let report = health_scorer.calculate(
            tracker_repo.as_ref() as &dyn crate::storage::repo_traits::TrackerRepository,
            node_repo.as_ref() as &dyn crate::storage::repo_traits::NodeRepository,
            state.peer_repo.as_ref() as &dyn crate::storage::repo_traits::PeerRepository,
            infohash_repo.as_ref() as &dyn crate::storage::repo_traits::InfohashRepository,
        ).await;
        let status = crate::health_check::SystemHealth::from_score(report.overall);
        crate::health_check::SystemHealth {
            overall_score: report.overall,
            status,
            tracker_layer_score: report.tracker_layer,
            dht_layer_score: report.dht_layer,
            peer_layer_score: report.peer_layer,
            active_trackers: report.active_trackers,
            total_trackers: report.total_trackers,
            avg_tracker_score: report.avg_tracker_score,
        }
    } else {
        crate::health_check::SystemHealth {
            overall_score: 0.0,
            status: crate::health_check::HealthStatus::Unhealthy,
            tracker_layer_score: 0.0,
            dht_layer_score: 0.0,
            peer_layer_score: 0.0,
            active_trackers: 0,
            total_trackers: 0,
            avg_tracker_score: 0.0,
        }
    };

    let resp = HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        discoverers: registry.len(),
        cached_infohashes: cache_stats.0,
        cached_peers: cache_stats.1,
        super_tracker_infohashes: state.super_tracker.infohash_count(),
        super_tracker_peers: state.super_tracker.peer_count(),
        uptime_seconds: 0,
        system_health,
    };

    Json(resp).into_response()
}

/// 统计信息
async fn stats_handler(State(state): State<AppState>) -> Response {
    let registry = state.control_plane.registry();
    let config = state.config.read();
    let custom_trackers = &config.discoverers.custom_trackers;
    let total_tracker_count = if custom_trackers.is_empty() {
        PUBLIC_TRACKERS.len()
    } else {
        custom_trackers.len()
    };

    let discoverer_stats: Vec<DiscovererStat> = registry
        .all()
        .iter()
        .map(|d| {
            let stats = d.stats();
            let is_tracker = d.name() == "tracker";
            DiscovererStat {
                name: d.name().to_string(),
                discoverer_type: d.discoverer_type().as_str().to_string(),
                enabled: d.is_enabled(),
                total_requests: stats.total_requests,
                success_requests: stats.success_requests,
                failed_requests: stats.failed_requests,
                total_peers_discovered: stats.total_peers_discovered,
                success_rate: stats.success_rate(),
                avg_response_time_ms: stats.avg_response_time_ms,
                tracker_count: if is_tracker { total_tracker_count } else { 0 },
            }
        })
        .collect();

    let cache_stats_raw = state.peer_repo.stats();

    // 收集 tracker 评分
    let tracker_scores: Vec<TrackerScoreInfo> = registry
        .all()
        .iter()
        .find(|d| d.name() == "tracker")
        .and_then(|d| d.tracker_scores())
        .map(|scores| {
            scores
                .into_iter()
                .map(|(url, score, disabled)| TrackerScoreInfo { url, score, disabled })
                .collect()
        })
        .unwrap_or_default();

    let resp = StatsResponse {
        discoverer_stats,
        cache_stats: CacheStats {
            total_infohashes: cache_stats_raw.0,
            total_peers: cache_stats_raw.1,
        },
        super_tracker_stats: SuperTrackerStats {
            total_infohashes: state.super_tracker.infohash_count(),
            total_peers: state.super_tracker.peer_count(),
        },
        tracker_scores,
        fetcher_stats: state.fetcher.as_ref().map(|f| FetcherStats {
            total_rounds: f.total_rounds.load(std::sync::atomic::Ordering::Relaxed),
            last_round_peers: f.last_round_peers.load(std::sync::atomic::Ordering::Relaxed),
            total_peers_fetched: f.total_peers_fetched.load(std::sync::atomic::Ordering::Relaxed),
            infohash_repo_count: f.infohash_count(),
        }),
    };

    Json(resp).into_response()
}

/// 主动发现 peer
async fn discover_handler(
    State(state): State<AppState>,
    Json(req): Json<DiscoverRequest>,
) -> Response {
    let start = std::time::Instant::now();

    // 解析 infohash
    let infohash = match parse_infohash(&req.infohash) {
        Ok(ih) => ih,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    // 检查缓存
    if !req.force_refresh {
        let cached = state.peer_repo.get_peers_sync(&infohash, req.limit);
        if !cached.is_empty() {
            debug!("[rest_api] 缓存命中: {} 个 peer", cached.len());
            let resp = DiscoverResponse {
                infohash: req.infohash,
                peers: cached,
                total: state.peer_repo.peer_count_for_infohash(&infohash),
                from_cache: true,
                duration_ms: start.elapsed().as_millis() as u64,
            };
            return Json(resp).into_response();
        }
    }

    // 触发后端发现
    let policy = state.control_plane.policy();
    let registry = state.control_plane.registry();
    let results = registry
        .discover_all(&infohash, req.limit, policy.max_concurrent, policy.timeout)
        .await;

    // 统一数据归口：discover 的 infohash 注册到 InfohashRepo
    if let Some(ref repo) = state.infohash_repo {
        repo.register_sync(infohash, "rest_discover");
    }

    let mut all_peers: Vec<PeerInfo> = vec![];
    for (name, result, _duration) in results {
        match result {
            Ok(peers) => {
                debug!("[rest_api] {} 返回 {} 个 peer", name, peers.len());
                all_peers.extend(peers);
            }
            Err(e) => {
                debug!("[rest_api] {} 失败: {}", name, e);
            }
        }
    }

    // 去重
    all_peers.sort_by_key(|p| p.addr);
    all_peers.dedup_by_key(|p| p.addr);

    // 存入缓存
    if !all_peers.is_empty() {
        state.peer_repo.add_peers_sync(&infohash, &all_peers);
    }

    // peer 已存入 PeerRepo，DhtProbe 会统一从 PeerRepo 拉取探测

    // 把发现的 peer 加入 PEX 连接池（PEX 二次扩散获取更多 peer）
    if let Some(pex) = registry.all().iter().find(|d| d.name() == "pex") {
        for peer in &all_peers {
            pex.add_peer_for_pex(peer.addr);
        }
        debug!("[rest_api] 已提交 {} 个 peer 到 PEX 连接池", all_peers.len());
    }

    if all_peers.len() > req.limit {
        all_peers.truncate(req.limit);
    }

    let resp = DiscoverResponse {
        infohash: req.infohash,
        peers: all_peers,
        total: state.peer_repo.peer_count_for_infohash(&infohash),
        from_cache: false,
        duration_ms: start.elapsed().as_millis() as u64,
    };

    Json(resp).into_response()
}

/// 发现器列表
async fn discoverers_handler(State(state): State<AppState>) -> Response {
    let registry = state.control_plane.registry();
    let discoverers: Vec<DiscovererInfo> = registry
        .all()
        .iter()
        .map(|d| DiscovererInfo {
            name: d.name().to_string(),
            discoverer_type: d.discoverer_type().as_str().to_string(),
            enabled: d.is_enabled(),
        })
        .collect();

    Json(DiscovererListResponse { discoverers }).into_response()
}

/// 缓存查询
async fn cache_query_handler(
    State(state): State<AppState>,
    Path(infohash_str): Path<String>,
) -> Response {
    let infohash = match parse_infohash(&infohash_str) {
        Ok(ih) => ih,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    let peers = state.peer_repo.get_peers_sync(&infohash, usize::MAX);
    let count = peers.len();

    Json(CacheQueryResponse {
        infohash: infohash_str,
        peers,
        count,
    })
    .into_response()
}

/// Peer 连接反馈
///
/// 下载引擎连接 peer 后调用此接口，PDC 根据反馈更新 peer 优先级。
/// 连续失败 5 次的 peer 会被自动移除。
async fn peer_feedback_handler(
    State(state): State<AppState>,
    Json(req): Json<PeerFeedbackRequest>,
) -> Response {
    let infohash = match parse_infohash(&req.infohash) {
        Ok(ih) => ih,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    let addr = match req.addr.parse::<SocketAddr>() {
        Ok(a) => a,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("无效的 addr: {}", e),
                }),
            )
                .into_response();
        }
    };

    let updated = if req.success {
        state.peer_repo.mark_connection_success_sync(&infohash, &addr);
        true
    } else {
        state.peer_repo.mark_connection_failure_sync(&infohash, &addr);
        // 检查是否被移除
        state.peer_repo.len_for_infohash(&infohash) > 0
    };

    debug!(
        "[rest_api] peer 反馈: infohash={}, addr={}, success={}, updated={}",
        &req.infohash[..8],
        addr,
        req.success,
        updated
    );

    Json(PeerFeedbackResponse {
        status: "ok".to_string(),
        updated,
    })
    .into_response()
}

/// NAT 状态查询
async fn nat_status_handler(State(state): State<AppState>) -> Response {
    let status = state.nat.status();
    Json(status).into_response()
}

// ---------------------------------------------------------------------------
// 辅助函数
// ---------------------------------------------------------------------------

/// 解析 infohash（hex 格式）
fn parse_infohash(s: &str) -> Result<Infohash, String> {
    if s.len() != 40 {
        return Err(format!(
            "infohash 必须是 40 字符的 hex 字符串，当前长度: {}",
            s.len()
        ));
    }
    let bytes = hex::decode(s).map_err(|e| format!("无效的 hex 字符串: {}", e))?;
    let mut arr = [0u8; 20];
    arr.copy_from_slice(&bytes);
    Ok(arr)
}

/// 爬虫状态
async fn crawler_handler(State(state): State<AppState>) -> Response {
    let resp = match &state.crawler_state {
        Some(cs) => {
            let s = cs.read();
            let uptime = s
                .started_at
                .map(|t| t.elapsed().as_secs())
                .unwrap_or(0);

            // 获取 top 10 节点
            let top_nodes = state
                .crawler_routing_table
                .as_ref()
                .map(|rt| {
                    let table = rt.read();
                    table
                        .top_nodes_by_score(10)
                        .into_iter()
                        .map(|n| NodeInfo {
                            addr: n.addr.to_string(),
                            score: n.score,
                            state: format!("{:?}", n.state),
                            query_count: n.query_count,
                            success_rate: n.success_rate(),
                        })
                        .collect()
                })
                .unwrap_or_default();

            CrawlerStatsResponse {
                enabled: true,
                running: s.running,
                nodes_crawled: s.nodes_crawled,
                infohashes_collected: s.infohashes_collected,
                peers_collected: s.peers_collected,
                known_nodes: s.known_nodes,
                messages_received: s.messages_received,
                requests_sent: s.requests_sent,
                errors: s.errors,
                uptime_seconds: uptime,
                top_nodes,
            }
        }
        None => CrawlerStatsResponse {
            enabled: false,
            running: false,
            nodes_crawled: 0,
            infohashes_collected: 0,
            peers_collected: 0,
            known_nodes: 0,
            messages_received: 0,
            requests_sent: 0,
            errors: 0,
            uptime_seconds: 0,
            top_nodes: vec![],
        },
    };

    Json(resp).into_response()
}

/// Peer 历史查询
async fn peer_history_handler(
    State(state): State<AppState>,
    Path(infohash_hex): Path<String>,
) -> Response {
    let infohash = match parse_infohash(&infohash_hex) {
        Ok(ih) => ih,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    match state.storage.query_peer_history(&infohash, 100) {
        Ok(history) => {
            #[derive(Serialize)]
            struct PeerHistoryItem {
                ip: String,
                port: u16,
                source: String,
                score: f64,
                discovered_at: i64,
            }
            let items: Vec<PeerHistoryItem> = history
                .into_iter()
                .map(|h| PeerHistoryItem {
                    ip: h.ip,
                    port: h.port,
                    source: h.source,
                    score: h.score,
                    discovered_at: h.discovered_at,
                })
                .collect();
            Json(serde_json::json!({
                "infohash": infohash_hex,
                "count": items.len(),
                "peers": items,
            }))
            .into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// 统计历史查询
async fn stats_history_handler(
    State(state): State<AppState>,
    Path(metric): Path<String>,
) -> Response {
    match state.storage.query_stats_history(&metric, 24) {
        Ok(points) => {
            #[derive(Serialize)]
            struct StatsPoint {
                timestamp: i64,
                value: f64,
            }
            let items: Vec<StatsPoint> = points
                .into_iter()
                .map(|(ts, val)| StatsPoint {
                    timestamp: ts,
                    value: val,
                })
                .collect();
            Json(serde_json::json!({
                "metric": metric,
                "hours": 24,
                "count": items.len(),
                "points": items,
            }))
            .into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_infohash() {
        let hex_str = "a".repeat(40);
        let result = parse_infohash(&hex_str);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), [0xaa; 20]);
    }

    #[test]
    fn test_parse_infohash_invalid_length() {
        let result = parse_infohash("abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_infohash_invalid_hex() {
        let result = parse_infohash(&"g".repeat(40));
        assert!(result.is_err());
    }
}


/// uTP 服务端统计 handler
async fn utp_handler(State(state): State<AppState>) -> Response {
    match &state.utp_server {
        Some(server) => {
            let stats = server.stats();
            Json(serde_json::json!({
                "enabled": true,
                "port": 6883,
                "syn_received": stats.syn_received,
                "connections_established": stats.connections_established,
                "handshakes_received": stats.handshakes_received,
                "peers_extracted": stats.peers_extracted,
                "resets_received": stats.resets_received,
                "timeouts": stats.timeouts,
                "errors": stats.errors,
                "active_connections": stats.active_connections,
                "connections_rejected": stats.connections_rejected,
                "connections_evicted": stats.connections_evicted,
                "max_connections": 100,
            })).into_response()
        }
        None => Json(serde_json::json!({ "enabled": false, "port": 6883 })).into_response(),
    }
}

/// PEX 接收器统计 handler
async fn pex_handler(State(state): State<AppState>) -> Response {
    match &state.pex_receiver {
        Some(receiver) => {
            let stats = receiver.stats();
            Json(serde_json::json!({
                "enabled": true,
                "extension_handshakes": stats.extension_handshakes,
                "pex_supported": stats.pex_supported,
                "pex_messages": stats.pex_messages,
                "peers_extracted": stats.peers_extracted,
                "ipv4_peers": stats.ipv4_peers,
                "ipv6_peers": stats.ipv6_peers,
                "utp_peers": stats.utp_peers,
                "holepunch_peers": stats.holepunch_peers,
                "parse_errors": stats.parse_errors,
            })).into_response()
        }
        None => Json(serde_json::json!({ "enabled": false })).into_response(),
    }
}
