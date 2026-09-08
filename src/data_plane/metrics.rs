//! Prometheus Metrics
//!
//! 提供 /metrics 端点，输出标准 Prometheus 格式指标。

use std::sync::OnceLock;

use axum::response::Response;
use prometheus::{
    Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts, Registry,
    TextEncoder,
};

/// 全局 Metrics 注册表
static REGISTRY: OnceLock<Registry> = OnceLock::new();

// ---- 指标定义 ----

/// Tracker 请求总数（按 tracker 和状态）
pub static TRACKER_REQUESTS_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();
/// Tracker 响应时间直方图
pub static TRACKER_RESPONSE_TIME: OnceLock<HistogramVec> = OnceLock::new();
/// Tracker 评分
pub static TRACKER_SCORE: OnceLock<GaugeVec> = OnceLock::new();

/// DHT 节点总数（按状态）
pub static DHT_NODES_TOTAL: OnceLock<GaugeVec> = OnceLock::new();
/// DHT 收到消息总数
pub static DHT_MESSAGES_RECEIVED: OnceLock<IntCounter> = OnceLock::new();
/// DHT 发送请求总数
pub static DHT_REQUESTS_SENT: OnceLock<IntCounter> = OnceLock::new();

/// 缓存 infohash 数
pub static CACHE_INFOHASHES: OnceLock<Gauge> = OnceLock::new();
/// 缓存 peer 数
pub static CACHE_PEERS: OnceLock<Gauge> = OnceLock::new();

/// 发现 peer 总数（按来源）
pub static PEER_DISCOVERED_TOTAL: OnceLock<IntCounterVec> = OnceLock::new();

/// 系统健康分
pub static HEALTH_SCORE: OnceLock<Gauge> = OnceLock::new();

/// 初始化所有指标
pub fn init_metrics() {
    let registry = Registry::new();

    // Tracker 指标
    let tracker_requests = IntCounterVec::new(
        Opts::new("pdc_tracker_requests_total", "Total tracker requests"),
        &["tracker", "status"],
    )
    .unwrap();
    let tracker_response = HistogramVec::new(
        HistogramOpts::new("pdc_tracker_response_time_seconds", "Tracker response time"),
        &["tracker"],
    )
    .unwrap();
    let tracker_score = GaugeVec::new(
        Opts::new("pdc_tracker_score", "Tracker quality score (0-100)"),
        &["tracker"],
    )
    .unwrap();

    // DHT 指标
    let dht_nodes = GaugeVec::new(
        Opts::new("pdc_dht_nodes_total", "Total DHT nodes in routing table"),
        &["state"],
    )
    .unwrap();
    let dht_messages = IntCounter::new(
        "pdc_dht_messages_received_total",
        "Total DHT messages received",
    )
    .unwrap();
    let dht_requests = IntCounter::new(
        "pdc_dht_requests_sent_total",
        "Total DHT requests sent",
    )
    .unwrap();

    // 缓存指标
    let cache_ih = Gauge::new("pdc_cache_infohashes", "Cached infohash count").unwrap();
    let cache_peers = Gauge::new("pdc_cache_peers", "Cached peer count").unwrap();

    // Peer 发现指标
    let peer_discovered = IntCounterVec::new(
        Opts::new("pdc_peer_discovered_total", "Total peers discovered"),
        &["source"],
    )
    .unwrap();

    // 健康指标
    let health = Gauge::new("pdc_health_score", "System health score (0-100)").unwrap();

    // 注册
    registry.register(Box::new(tracker_requests.clone())).unwrap();
    registry.register(Box::new(tracker_response.clone())).unwrap();
    registry.register(Box::new(tracker_score.clone())).unwrap();
    registry.register(Box::new(dht_nodes.clone())).unwrap();
    registry.register(Box::new(dht_messages.clone())).unwrap();
    registry.register(Box::new(dht_requests.clone())).unwrap();
    registry.register(Box::new(cache_ih.clone())).unwrap();
    registry.register(Box::new(cache_peers.clone())).unwrap();
    registry.register(Box::new(peer_discovered.clone())).unwrap();
    registry.register(Box::new(health.clone())).unwrap();

    // 设置全局引用
    TRACKER_REQUESTS_TOTAL.set(tracker_requests).ok();
    TRACKER_RESPONSE_TIME.set(tracker_response).ok();
    TRACKER_SCORE.set(tracker_score).ok();
    DHT_NODES_TOTAL.set(dht_nodes).ok();
    DHT_MESSAGES_RECEIVED.set(dht_messages).ok();
    DHT_REQUESTS_SENT.set(dht_requests).ok();
    CACHE_INFOHASHES.set(cache_ih).ok();
    CACHE_PEERS.set(cache_peers).ok();
    PEER_DISCOVERED_TOTAL.set(peer_discovered).ok();
    HEALTH_SCORE.set(health).ok();

    REGISTRY.set(registry).ok();
}

/// 处理 /metrics 请求
pub async fn metrics_handler() -> Response {
    let registry = match REGISTRY.get() {
        Some(r) => r,
        None => return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };

    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    let mut buffer = vec![];
    match encoder.encode(&metric_families, &mut buffer) {
        Ok(_) => Response::builder()
            .header("Content-Type", encoder.format_type())
            .body(buffer.into())
            .unwrap_or_else(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()),
        Err(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

use axum::response::IntoResponse;
