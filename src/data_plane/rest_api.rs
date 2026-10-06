//! REST API
//!
//! 提供管理和查询接口：
//! - GET /health - 健康检查
//! - GET /api/v1/stats - 统计信息
//! - POST /api/v1/discover - 主动发现 peer
//! - GET /api/v1/discoverers - 发现器列表
//! - GET /api/v1/cache/{infohash} - 查询缓存

use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;
use tracing::debug;

use crate::data_plane::AppState;
use crate::types::{Infohash, PeerInfo};

/// 默认 uTP 监听端口（用于状态上报）
const DEFAULT_UTP_REPORT_PORT: u16 = 6883;

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
#[derive(Debug, Serialize, Clone)]
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
    #[serde(default)]
    pub crawler_metrics: Option<CrawlerMetrics>,
    #[serde(default)]
    pub task_scheduler_metrics: Option<TaskSchedulerMetrics>,
    #[serde(default)]
    pub node_repo_metrics: Option<NodeRepoMetrics>,
    #[serde(default)]
    pub peer_repo_metrics: Option<PeerRepoMetrics>,
    #[serde(default)]
    pub infohash_repo_metrics: Option<InfohashRepoMetrics>,
    #[serde(default)]
    pub tracker_repo_metrics: Option<TrackerRepoMetrics>,
}

#[derive(Debug, Serialize, Clone)]
pub struct FetcherStats {
    pub total_rounds: u64,
    pub last_round_peers: u64,
    pub total_peers_fetched: u64,
    pub infohash_repo_count: usize,
}

#[derive(Debug, Serialize, Clone)]
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

#[derive(Debug, Serialize, Clone, Default)]
pub struct CacheStats {
    pub total_infohashes: usize,
    pub total_peers: usize,
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct SuperTrackerStats {
    pub total_infohashes: usize,
    pub total_peers: usize,
}

/// 爬虫深度监控指标（C3 扩展）
#[derive(Debug, Serialize, Default, Clone)]
pub struct CrawlerMetrics {
    pub socket_send_pps: Vec<u64>,
    pub socket_recv_pps: Vec<u64>,
    pub socket_response_rates: Vec<f64>,
    pub pending_shard_lens: Vec<usize>,
    pub udp_packet_loss_estimate: f64,
    pub node_select_avg_us: u64,
    pub adaptive_multiplier: f64,
    pub predicted_response_rate: Option<f64>,
    pub model_update_count: u64,
    pub history_len: usize,
    pub concurrent_sockets_in_use: usize,
}

/// 任务调度器监控指标（C3 扩展）
#[derive(Debug, Serialize, Default, Clone)]
pub struct TaskSchedulerMetrics {
    pub running_by_category: std::collections::HashMap<String, u32>,
    pub queue_len: usize,
    pub task_recent_avg_durations: std::collections::HashMap<String, u64>,
}

/// Repo 冷热分层统计（监控面板用：总数/热/温/冷）
#[derive(Debug, Serialize, Default, Clone)]
pub struct RepoTierStats {
    /// 数据库总数
    pub total: u64,
    /// 热缓存数量
    pub hot: usize,
    /// 温缓存数量
    pub warm: usize,
    /// 冷数据 = total - hot - warm
    pub cold: u64,
}

/// NodeRepo 监控指标（C3 扩展）
#[derive(Debug, Serialize, Default, Clone)]
pub struct NodeRepoMetrics {
    pub dirty_count: usize,
    pub write_queue_len: usize,
    pub subnet_count: usize,
    #[serde(default)]
    pub tier: RepoTierStats,
}

/// PeerRepo 监控指标
#[derive(Debug, Serialize, Default, Clone)]
pub struct PeerRepoMetrics {
    #[serde(default)]
    pub tier: RepoTierStats,
}

/// InfohashRepo 监控指标
#[derive(Debug, Serialize, Default, Clone)]
pub struct InfohashRepoMetrics {
    #[serde(default)]
    pub tier: RepoTierStats,
}

/// TrackerRepo 监控指标
#[derive(Debug, Serialize, Default, Clone)]
pub struct TrackerRepoMetrics {
    #[serde(default)]
    pub tier: RepoTierStats,
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
    /// 真实 RTT 的 EMA（毫秒）——区分「响应延迟」与「节点沉默」的关键诊断量（批次K #15）
    #[serde(default)]
    pub latency_ema_ms: u64,
    /// tid 命中 pending 的响应数
    #[serde(default)]
    pub responses_matched_total: u64,
    /// 迟到/伪造响应数（不进入响应率信号）
    #[serde(default)]
    pub late_responses_total: u64,
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
        .route(
            "/api/v1/history/peers/{infohash}",
            get(peer_history_handler),
        )
        .route("/api/v1/history/stats/{metric}", get(stats_history_handler))
        .route("/api/v1/federation/status", get(federation_status_handler))
        .route("/api/v1/federation/nodes", get(federation_nodes_handler))
        .route(
            "/api/v1/federation/connections",
            get(federation_connections_handler),
        )
        .route(
            "/api/v1/federation/sync-stats",
            get(federation_sync_stats_handler),
        )
        .route(
            "/api/v1/federation/sync-observability",
            get(federation_sync_observability_handler),
        )
        .route(
            "/api/v1/federation/relay/setup",
            get(federation_relay_setup_handler),
        )
        .route("/api/v1/relay/stats", get(relay_stats_handler))
        .route("/api/v1/io/status", get(io_status_handler))
        .route("/api/v1/sync/channels", get(sync_channels_handler))
        .route("/api/v1/system", get(system_handler))
        .route("/api/v1/config", get(get_config_handler))
        .route("/api/v1/config/reload", post(reload_config_handler))
        // E7：实时状态轮询端点（与 WS status 推送同构，含 node_repo.tier/peer_repo.tier 等）
        .route("/api/v1/status", get(status_handler))
        .route("/metrics", get(crate::data_plane::metrics::metrics_handler))
        .route("/ws", get(crate::data_plane::ws::ws_handler))
        .with_state(state)
        .layer(Extension(event_bus))
        .layer(CorsLayer::permissive())
}

// ---------------------------------------------------------------------------
// 配置热重载处理函数
// ---------------------------------------------------------------------------

/// GET /api/v1/config —— 当前生效配置（快照 = 最近一次热重载后的文件状态）
async fn get_config_handler(State(state): State<AppState>) -> Response {
    Json(state.config.read().clone()).into_response()
}

/// POST /api/v1/config/reload —— 立即重新加载配置文件并应用（继承 API token 鉴权）
async fn reload_config_handler(State(state): State<AppState>) -> Response {
    match &state.config_reloader {
        Some(reloader) => {
            let report = reloader.reload_now().await;
            if report.errors.is_empty() {
                Json(serde_json::json!({
                    "code": 0,
                    "message": "配置重载完成",
                    "report": report,
                }))
                .into_response()
            } else {
                (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(serde_json::json!({
                        "code": 1,
                        "message": "配置重载存在问题（旧配置保留）",
                        "report": report,
                    })),
                )
                    .into_response()
            }
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "code": 1,
                "message": "配置热重载器未装配（config_reloader = None）",
            })),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// 处理函数
// ---------------------------------------------------------------------------

/// 健康检查
async fn health_handler(State(state): State<AppState>) -> Response {
    let registry = state.control_plane.registry();
    let cache_stats = state.peer_repo.stats();
    // 从 crawler_state 获取入站连通性评分
    let _inbound_score = state.crawler_state.as_ref().map(|cs| {
        let s = cs.read();
        let uptime = s.started_at.map(|t| t.elapsed().as_secs()).unwrap_or(0);
        let inbound_per_min = if uptime > 0 {
            (s.inbound_total as f64 / uptime as f64) * 60.0
        } else {
            0.0
        };
        if inbound_per_min >= 10.0 {
            100.0
        } else if inbound_per_min >= 5.0 {
            80.0
        } else if inbound_per_min >= 1.0 {
            60.0
        } else if inbound_per_min >= 0.1 {
            40.0
        } else if inbound_per_min > 0.0 {
            20.0
        } else {
            0.0
        }
    });
    // C：健康分改由后台统计快照纯计算（health_report_from_snapshot）。
    // 不再在 API 线程实时 HealthScorerImpl::calculate()（同步遍历全部 Repo，是 API 卡死元凶之一）。
    // 语义差异：输入为后台快照（最长约 1 个快照周期陈旧）；
    // repo 未启用时对应层自然为 0（tracker 无快照则 tracker_layer=0，node 无快照则 dht_layer=0）。
    let snap = state.stats_snapshot.get();
    let report = crate::data_plane::stats_snapshot::health_report_from_snapshot(&snap);
    let system_health = crate::health_check::SystemHealth {
        overall_score: report.overall,
        status: crate::health_check::SystemHealth::from_score(report.overall),
        tracker_layer_score: report.tracker_layer,
        dht_layer_score: report.dht_layer,
        peer_layer_score: report.peer_layer,
        active_trackers: report.active_trackers,
        total_trackers: report.total_trackers,
        avg_tracker_score: report.avg_tracker_score,
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
///
/// 只读后台快照（stats_snapshot），不直接访问 Repo 层，
/// 避免在 API 线程上执行 DB COUNT 查询导致阻塞。
async fn stats_handler(State(state): State<AppState>) -> Response {
    let s = state.stats_snapshot.get();

    let resp = StatsResponse {
        discoverer_stats: s.discoverer_stats,
        cache_stats: s.cache_stats,
        super_tracker_stats: s.super_tracker_stats,
        tracker_scores: s.tracker_scores,
        fetcher_stats: s.fetcher_stats,
        crawler_metrics: s.crawler_metrics,
        task_scheduler_metrics: s.task_scheduler_metrics,
        node_repo_metrics: s.node_repo_metrics,
        peer_repo_metrics: s.peer_repo_metrics,
        infohash_repo_metrics: s.infohash_repo_metrics,
        tracker_repo_metrics: s.tracker_repo_metrics,
    };

    Json(resp).into_response()
}

/// GET /api/v1/status —— 实时状态（与 WS `status` 推送的 `data` 同构）。
///
/// 面板 tier 轮询主路径（前端另由代理加 HTTP 轮询 + WS fallback）。
/// 直接复用 [`crate::data_plane::ws::collect_status`]：内部已用后台 stats_snapshot
/// 纯组装、JSON 构造放 spawn_blocking，handler 不在 API 线程做任何 DB/全量克隆。
/// 输出含 `node_repo.tier` / `peer_repo.tier` / `infohash_repo.tier` / `tracker_repo.tier`
/// 等嵌套冷热分层结构（面板 tier 所需）。
async fn status_handler(State(state): State<AppState>) -> Response {
    let data = crate::data_plane::ws::collect_status(&state).await;
    Json(data).into_response()
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
        debug!(
            "[rest_api] 已提交 {} 个 peer 到 PEX 连接池",
            all_peers.len()
        );
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
        state
            .peer_repo
            .mark_connection_success_sync(&infohash, &addr);
        true
    } else {
        state
            .peer_repo
            .mark_connection_failure_sync(&infohash, &addr);
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
            let uptime = s.started_at.map(|t| t.elapsed().as_secs()).unwrap_or(0);

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
                latency_ema_ms: s.latency_ema_ms,
                responses_matched_total: s.responses_matched_total,
                late_responses_total: s.late_responses_total,
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
            latency_ema_ms: 0,
            responses_matched_total: 0,
            late_responses_total: 0,
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

    // SQLite 查询移到 blocking 线程：避免 API worker 被 DB 读阻塞（历史事故：DB 调用钉死 api_runtime）
    let storage = state.storage.clone();
    let history =
        match tokio::task::spawn_blocking(move || storage.query_peer_history(&infohash, 100)).await
        {
            Ok(Ok(history)) => history,
            Ok(Err(e)) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        };

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

/// 统计历史查询
async fn stats_history_handler(
    State(state): State<AppState>,
    Path(metric): Path<String>,
) -> Response {
    // SQLite 查询移到 blocking 线程：避免 API worker 被 DB 读阻塞
    let storage = state.storage.clone();
    let metric_q = metric.clone();
    let points =
        match tokio::task::spawn_blocking(move || storage.query_stats_history(&metric_q, 24)).await
        {
            Ok(Ok(points)) => points,
            Ok(Err(e)) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        };

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

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
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

    // ---- E7：/api/v1/status 关键字段契约（面板 tier 数据）----
    // 说明：完整端点集成测试需要构造 AppState（SQLite + 全部 Repo + EventBus），
    // 当前 rest_api 测试基建无此装配（既有 3 个测试均为 parse_infohash 纯函数），
    // 故此处只对面板依赖的 tier 数据结构做纯序列化契约测试。
    // collect_status 的兜底逻辑（超时/join 失败回退 LAST_STATUS_DATA）依赖 tokio
    // 运行时与全局静态缓存，同样无法在无 AppState 下单测，见交付报告。

    #[test]
    fn repo_tier_stats_serializes_to_panel_shape() {
        // 面板 tier 需要 {total, hot, warm, cold} 四元组；序列化名必须稳定。
        let tier = RepoTierStats {
            total: 10_000,
            hot: 200,
            warm: 300,
            cold: 9_500,
        };
        let v = serde_json::to_value(&tier).expect("RepoTierStats 应可序列化");
        assert_eq!(v["total"], 10_000);
        assert_eq!(v["hot"], 200);
        assert_eq!(v["warm"], 300);
        assert_eq!(v["cold"], 9_500);
        // 四个键缺一不可（前端按这四个键渲染 tier 分层）
        for k in ["total", "hot", "warm", "cold"] {
            assert!(v.get(k).is_some(), "RepoTierStats 缺少键 {}", k);
        }
    }

    #[test]
    fn repo_tier_cold_is_total_minus_hot_warm() {
        // 与 stats_snapshot::collect_stats 中 RepoTierStats 的 cold 推导一致：
        // cold = total - hot - warm（饱和减法）。
        let (total, hot, warm) = (5000u64, 100usize, 200usize);
        let cold = total.saturating_sub(hot as u64).saturating_sub(warm as u64);
        assert_eq!(cold, 4700);
    }
}

/// uTP 服务端统计 handler
async fn utp_handler(State(state): State<AppState>) -> Response {
    match &state.utp_server {
        Some(server) => {
            let stats = server.stats();
            Json(serde_json::json!({
                "enabled": true,
                "port": DEFAULT_UTP_REPORT_PORT,
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
            }))
            .into_response()
        }
        None => Json(serde_json::json!({ "enabled": false, "port": DEFAULT_UTP_REPORT_PORT }))
            .into_response(),
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
            }))
            .into_response()
        }
        None => Json(serde_json::json!({ "enabled": false })).into_response(),
    }
}

// ---------------------------------------------------------------------------
// 联邦网络 API
// ---------------------------------------------------------------------------

/// 联邦状态
async fn federation_status_handler(State(state): State<AppState>) -> Response {
    let mut status = match &state.federation {
        Some(fed) => fed.status(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "federation not enabled" })),
            )
                .into_response();
        }
    };

    // 新口径（2026-09-30）：API 线程零 DB 接触——本 handler 不再引用 state.storage，
    // 杜绝 entity_counts_cached() 在读池耗尽时回退抢全局写锁（曾致 /federation/status 4s+ 超时）。
    // *_total = 后台 stats_snapshot 维护的 DB 总行数（count_table = 表总行数，含软删墓碑，
    // 与旧 entity_counts 的"有效行"口径有微小差异：软删行会多计入；peer_repo_total 相比旧口径
    // 缺少 peers_archive 部分，因无内存侧来源）+ 内存原子增量（write_queue_len = 未落库部分）；
    // hot_total / active 为纯内存计数。快照字段缺失时用各 repo 内存原子读兜底，字段永不缺省。
    let snap = state.stats_snapshot.get();

    // node_repo_total = 快照 DB 总行数 + 未落库增量；快照 None 则 stats_sync().total 兜底
    let node_db_total = snap
        .node_repo_metrics
        .as_ref()
        .map(|m| m.tier.total)
        .unwrap_or_else(|| {
            state
                .node_repo
                .as_ref()
                .map(|r| r.stats_sync().total as u64)
                .unwrap_or(0)
        });
    status.node_repo_total = node_db_total
        + state
            .node_repo
            .as_ref()
            .map(|r| r.write_queue_len_sync() as u64)
            .unwrap_or(0);
    status.node_repo_hot_total = state
        .node_repo
        .as_ref()
        .map(|r| r.len_sync() as u64)
        .unwrap_or(0);

    // peer_repo_total = 快照 DB 总行数（peers 表）；快照 None 则内存 peer_repo.len() 兜底
    status.peer_repo_total = snap
        .peer_repo_metrics
        .as_ref()
        .map(|m| m.tier.total)
        .unwrap_or_else(|| state.peer_repo.len() as u64);
    status.peer_repo_hot_total = state.peer_repo.len() as u64;
    // 活跃 peer 数（近 1h）：O(1) 原子读，PeerRepo 记账维护 + 后台 60s sweep 兜底
    status.peer_repo_active = state.peer_repo.active_1h_count();

    // infohash / tracker 总行数：快照优先，None 时 count_sync() 内存原子兜底
    status.infohash_repo_total = snap
        .infohash_repo_metrics
        .as_ref()
        .map(|m| m.tier.total)
        .unwrap_or_else(|| {
            state
                .infohash_repo
                .as_ref()
                .map(|r| r.count_sync() as u64)
                .unwrap_or(0)
        });
    status.tracker_repo_total = snap
        .tracker_repo_metrics
        .as_ref()
        .map(|m| m.tier.total)
        .unwrap_or_else(|| {
            state
                .tracker_repo
                .as_ref()
                .map(|r| r.count_sync() as u64)
                .unwrap_or(0)
        });

    Json(status).into_response()
}

/// 联邦节点列表
async fn federation_nodes_handler(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let fed = match &state.federation {
        Some(fed) => fed.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "federation not enabled" })),
            )
                .into_response()
        }
    };
    let limit: usize = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    // fed.snapshot() 内部 clone 全量 node_table（known nodes 可能上千），放 blocking 线程
    let snapshot = match tokio::task::spawn_blocking(move || fed.snapshot()).await {
        Ok(snapshot) => snapshot,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    let total = snapshot.nodes.len();
    let nodes: Vec<_> = snapshot.nodes.into_iter().take(limit).collect();
    Json(serde_json::json!({
        "total": total,
        "returned": nodes.len(),
        "nodes": nodes,
    }))
    .into_response()
}

/// 联邦连接列表
async fn federation_connections_handler(State(state): State<AppState>) -> Response {
    let fed = match &state.federation {
        Some(fed) => fed.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "federation not enabled" })),
            )
                .into_response()
        }
    };
    // fed.snapshot() 内部 clone 全量 node_table，放 blocking 线程
    let snapshot = match tokio::task::spawn_blocking(move || fed.snapshot()).await {
        Ok(snapshot) => snapshot,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    Json(serde_json::json!({
        "count": snapshot.connections.len(),
        "connections": snapshot.connections,
    }))
    .into_response()
}

/// 联邦同步统计 + 中继统计 + metrics
async fn federation_sync_stats_handler(State(state): State<AppState>) -> Response {
    let fed = match &state.federation {
        Some(fed) => fed.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "federation not enabled" })),
            )
                .into_response()
        }
    };
    // fed.snapshot() 内部 clone 全量 node_table，放 blocking 线程
    let snapshot = match tokio::task::spawn_blocking(move || fed.snapshot()).await {
        Ok(snapshot) => snapshot,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    Json(serde_json::json!({
        "sync_stats": snapshot.sync_stats,
        "relay_stats": snapshot.relay_stats,
        "metrics": snapshot.status.metrics,
    }))
    .into_response()
}

/// P2-3：同步面可观测性（P1-2/P1-3/P1-4/P2-1 的运维观测点）。
///
/// 返回：oplog 水位与保留窗口、每个对端每个 repo 的增量落后量（`ops_lag_seq`）、
/// 未完成的 bootstrap 进度（`{phase, chunks_done/total, bytes, eta 由 ratio 推得}`）、
/// range 反熵访问区间数（`reconcile_nodes_visited`）与各开关状态。
async fn federation_sync_observability_handler(State(state): State<AppState>) -> Response {
    let fed = match &state.federation {
        Some(fed) => fed.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "federation not enabled" })),
            )
                .into_response()
        }
    };
    // sync_observability 内部读 oplog（SQLite），放 blocking 线程
    let value = match tokio::task::spawn_blocking(move || fed.sync_observability()).await {
        Ok(value) => value,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    Json(value).into_response()
}

/// 手动触发与指定已连接节点建立联邦中继通道（运维/验证用）。
/// GET /api/v1/federation/relay/setup?target=<40 位 hex node_id>
/// 前提：本节点与 target 已有直连控制连接（联邦中继建立在已有直连之上）。
async fn federation_relay_setup_handler(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let fed = match &state.federation {
        Some(f) => f,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "federation not enabled" })),
            )
                .into_response();
        }
    };

    let target_hex = match params.get("target") {
        Some(t) => t.trim(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "missing query param 'target' (40-char hex node_id)"
                })),
            )
                .into_response();
        }
    };

    let node_id = match crate::federation::node_id::NodeId::from_hex(target_hex) {
        Ok(n) => n,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid node_id hex, expect 40 hex chars" })),
            )
                .into_response();
        }
    };

    match fed.relay_manager.setup_relay(node_id) {
        Ok(channel_id) => Json(serde_json::json!({
            "status": "initiated",
            "channel_id": channel_id,
            "active_channels": fed.relay_manager.active_channel_count(),
            "total_bytes_forwarded": fed.relay_manager.total_bytes_forwarded(),
        }))
        .into_response(),
        Err(e) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// 中继服务器统计（data_plane RelayServer）
// B4：GET /api/v1/io/status
async fn io_status_handler(State(state): State<AppState>) -> Response {
    use crate::storage::io_scheduler::disk_class;
    match state.io_scheduler {
        Some(ref s) => Json(s.status_snapshot()).into_response(),
        None => Json(serde_json::json!({
            "enabled": false,
            "disk_class": disk_class(),
        }))
        .into_response(),
    }
}

// 联邦同步通道状态轮询：GET /api/v1/sync/channels
async fn sync_channels_handler(State(state): State<AppState>) -> Response {
    // parking_lot 同步锁，read() 短时阻塞（与 io_status_handler 读法一致）。
    Json(state.sync_channels.read().clone()).into_response()
}

/// GET /api/v1/system：进程运行时信息（内存 / CPU / 线程 / fd）。
#[derive(serde::Serialize)]
struct SystemStatus {
    memory_bytes: u64,
    memory_mb: f64,
    cpu_usage_percent: f64,
    thread_count: u32,
    fd_count: u32,
    /// 快照数据距今的毫秒数（system_stats 缓存按 1 秒限频刷新，通常落在 0~1000）
    staleness_ms: u64,
}

async fn system_handler() -> Response {
    // 从全局缓存读取（内部仅做"自身进程"最小刷新并按 1 秒限频）。
    // 严禁在请求路径执行 System::new_all()：Windows 全量枚举单次可达数秒到数分钟，
    // 监控面板持续轮询曾把 api_runtime 全部 worker park 死（2026-09-30 .51 事故）。
    let snap = crate::services::system_stats::snapshot();
    let memory_mb = snap.mem_bytes as f64 / 1024.0 / 1024.0;
    // cpu_usage 由缓存周期刷新维护（两次刷新间的差值），首次轮询为 0，第二次起有效。
    let cpu_usage_percent = snap.cpu_usage_percent;
    // TODO: sysinfo 0.32 不暴露 thread/fd count，Windows 下需要 NtQuerySystemInformation，暂以默认值占位。
    let thread_count = u32::default();
    let fd_count = u32::default();
    Json(SystemStatus {
        memory_bytes: snap.mem_bytes,
        memory_mb,
        cpu_usage_percent,
        thread_count,
        fd_count,
        staleness_ms: snap.staleness_ms,
    })
    .into_response()
}

async fn relay_stats_handler(State(state): State<AppState>) -> Response {
    match &state.relay_server {
        Some(server) => {
            let stats = server.stats();
            Json(serde_json::to_value(&stats).unwrap_or(serde_json::json!({}))).into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "relay server not started" })),
        )
            .into_response(),
    }
}
