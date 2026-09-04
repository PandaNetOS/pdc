//! REST API
//!
//! 提供管理和查询接口：
//! - GET /health - 健康检查
//! - GET /api/v1/stats - 统计信息
//! - POST /api/v1/discover - 主动发现 peer
//! - GET /api/v1/discoverers - 发现器列表
//! - GET /api/v1/cache/{infohash} - 查询缓存

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tracing::debug;

use crate::data_plane::AppState;
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
}

/// 统计响应
#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub discoverer_stats: Vec<DiscovererStat>,
    pub cache_stats: CacheStats,
    pub super_tracker_stats: SuperTrackerStats,
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

// ---------------------------------------------------------------------------
// 路由
// ---------------------------------------------------------------------------

/// 构建 REST API 路由
pub fn routes(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/api/v1/stats", get(stats_handler))
        .route("/api/v1/discover", post(discover_handler))
        .route("/api/v1/discoverers", get(discoverers_handler))
        .route("/api/v1/cache/{infohash}", get(cache_query_handler))
        .route("/api/v1/peer-feedback", post(peer_feedback_handler))
        .route("/api/v1/nat/status", get(nat_status_handler))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// 处理函数
// ---------------------------------------------------------------------------

/// 健康检查
async fn health_handler(State(state): State<AppState>) -> Response {
    let registry = state.control_plane.registry();
    let cache_stats = state.cache.stats();

    let resp = HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        discoverers: registry.len(),
        cached_infohashes: cache_stats.0,
        cached_peers: cache_stats.1,
        super_tracker_infohashes: state.super_tracker.infohash_count(),
        super_tracker_peers: state.super_tracker.peer_count(),
        uptime_seconds: 0, // TODO: 记录启动时间
    };

    Json(resp).into_response()
}

/// 统计信息
async fn stats_handler(State(state): State<AppState>) -> Response {
    let registry = state.control_plane.registry();

    let discoverer_stats: Vec<DiscovererStat> = registry
        .all()
        .iter()
        .map(|d| {
            let stats = d.stats();
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
            }
        })
        .collect();

    let cache_stats_raw = state.cache.stats();
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
        let cached = state.cache.get_peers(&infohash, req.limit);
        if !cached.is_empty() {
            debug!("[rest_api] 缓存命中: {} 个 peer", cached.len());
            let resp = DiscoverResponse {
                infohash: req.infohash,
                peers: cached,
                total: state.cache.peer_count(&infohash),
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
        state.cache.add_peers(&infohash, &all_peers);
    }

    if all_peers.len() > req.limit {
        all_peers.truncate(req.limit);
    }

    let resp = DiscoverResponse {
        infohash: req.infohash,
        peers: all_peers,
        total: state.cache.peer_count(&infohash),
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

    let peers = state.cache.get_peers(&infohash, usize::MAX);
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
        state.cache.mark_connection_success(&infohash, &addr);
        true
    } else {
        state.cache.mark_connection_failure(&infohash, &addr);
        // 检查是否被移除
        state.cache.len_for_infohash(&infohash) > 0
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
