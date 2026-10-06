//! Prometheus Metrics
//!
//! 提供 /metrics 端点，输出标准 Prometheus 格式指标。

use std::sync::OnceLock;

use axum::response::Response;
use prometheus::{
    Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts,
    Registry, TextEncoder,
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

// ---- 爬虫深度指标（19号 D4：注册与更新接线）----

/// 每 socket 发送 PPS
pub static CRAWLER_SOCKET_SEND_PPS: OnceLock<GaugeVec> = OnceLock::new();
/// 每 socket 接收 PPS
pub static CRAWLER_SOCKET_RECV_PPS: OnceLock<GaugeVec> = OnceLock::new();
/// 每 socket 响应率
pub static CRAWLER_SOCKET_RESPONSE_RATE: OnceLock<GaugeVec> = OnceLock::new();
/// 自适应倍率
pub static CRAWLER_ADAPTIVE_MULTIPLIER: OnceLock<Gauge> = OnceLock::new();
/// 预测响应率
pub static CRAWLER_PREDICTED_RESPONSE_RATE: OnceLock<Gauge> = OnceLock::new();
/// pending 总数
pub static CRAWLER_PENDING_TOTAL: OnceLock<Gauge> = OnceLock::new();
/// UDP 丢包估算（60s 窗口）
pub static CRAWLER_UDP_PACKET_LOSS: OnceLock<Gauge> = OnceLock::new();
/// 并发 socket 数
pub static CRAWLER_CONCURRENT_SOCKETS: OnceLock<Gauge> = OnceLock::new();
/// paced 队列长度
pub static CRAWLER_SEND_QUEUE_LEN: OnceLock<Gauge> = OnceLock::new();
/// paced 入队丢弃累计
pub static CRAWLER_ENQUEUE_DROPPED_TOTAL: OnceLock<IntCounter> = OnceLock::new();
/// paced 模式开关（0/1）
pub static CRAWLER_PACED_MODE: OnceLock<Gauge> = OnceLock::new();
/// 真新增节点累计（爬虫/探测/联邦单一咽喉点计数）
pub static NODE_DISCOVERED_TOTAL: OnceLock<IntCounter> = OnceLock::new();
/// 各 Repo 内存池总量
pub static REPO_TOTAL_COUNT: OnceLock<GaugeVec> = OnceLock::new();
/// 各 Repo dirty 数
pub static REPO_DIRTY_COUNT: OnceLock<GaugeVec> = OnceLock::new();
/// 调度器队列长度
pub static SCHEDULER_QUEUE_LEN: OnceLock<Gauge> = OnceLock::new();
/// 调度器各分类运行数
pub static SCHEDULER_RUNNING_TASKS: OnceLock<GaugeVec> = OnceLock::new();
/// 调度器累计准入执行数
pub static SCHEDULER_TASK_EXECUTIONS_TOTAL: OnceLock<IntCounter> = OnceLock::new();

/// 任务调度器引用（main 启动时注入，供调度器指标导出）
static SCHEDULER: OnceLock<std::sync::Arc<crate::intelligence::task_scheduler::TaskScheduler>> =
    OnceLock::new();

/// 注入任务调度器引用（19号 D4）
pub fn set_scheduler(s: std::sync::Arc<crate::intelligence::task_scheduler::TaskScheduler>) {
    SCHEDULER.set(s).ok();
}

/// 调度器指标快照（None = 未注入）
pub fn scheduler_metrics() -> Option<crate::data_plane::rest_api::TaskSchedulerMetrics> {
    let sc = SCHEDULER.get()?;
    Some(crate::data_plane::rest_api::TaskSchedulerMetrics {
        running_by_category: sc.running_by_category(),
        queue_len: sc.queue_len(),
        task_recent_avg_durations: sc.task_recent_avg_durations(),
    })
}

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
    let dht_requests =
        IntCounter::new("pdc_dht_requests_sent_total", "Total DHT requests sent").unwrap();

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
    registry
        .register(Box::new(tracker_requests.clone()))
        .unwrap();
    registry
        .register(Box::new(tracker_response.clone()))
        .unwrap();
    registry.register(Box::new(tracker_score.clone())).unwrap();
    registry.register(Box::new(dht_nodes.clone())).unwrap();
    registry.register(Box::new(dht_messages.clone())).unwrap();
    registry.register(Box::new(dht_requests.clone())).unwrap();
    registry.register(Box::new(cache_ih.clone())).unwrap();
    registry.register(Box::new(cache_peers.clone())).unwrap();
    registry
        .register(Box::new(peer_discovered.clone()))
        .unwrap();
    registry.register(Box::new(health.clone())).unwrap();

    // 爬虫深度指标（19号 D4）
    let crawler_send_pps = GaugeVec::new(
        Opts::new("pdc_crawler_socket_send_pps", "Crawler per-socket send pps"),
        &["socket_idx"],
    )
    .unwrap();
    let crawler_recv_pps = GaugeVec::new(
        Opts::new("pdc_crawler_socket_recv_pps", "Crawler per-socket recv pps"),
        &["socket_idx"],
    )
    .unwrap();
    let crawler_rr = GaugeVec::new(
        Opts::new(
            "pdc_crawler_socket_response_rate",
            "Crawler per-socket response rate",
        ),
        &["socket_idx"],
    )
    .unwrap();
    let crawler_multiplier = Gauge::new(
        "pdc_crawler_adaptive_multiplier",
        "Adaptive send multiplier",
    )
    .unwrap();
    let crawler_predicted = Gauge::new(
        "pdc_crawler_predicted_response_rate",
        "Predicted next-round response rate",
    )
    .unwrap();
    let crawler_pending =
        Gauge::new("pdc_crawler_pending_total", "In-flight DHT requests").unwrap();
    let crawler_loss = Gauge::new(
        "pdc_crawler_udp_packet_loss",
        "UDP packet loss estimate (60s window)",
    )
    .unwrap();
    let crawler_concurrent = Gauge::new(
        "pdc_crawler_concurrent_sockets_in_use",
        "Concurrent sockets in use",
    )
    .unwrap();
    let crawler_queue =
        Gauge::new("pdc_crawler_send_queue_len", "Paced send queue length").unwrap();
    let crawler_dropped =
        IntCounter::new("pdc_crawler_enqueue_dropped_total", "Paced enqueue drops").unwrap();
    let crawler_paced =
        Gauge::new("pdc_crawler_paced_mode", "Paced send mode enabled (0/1)").unwrap();
    let node_discovered =
        IntCounter::new("pdc_node_discovered_total", "Truly new nodes discovered").unwrap();
    let repo_total = GaugeVec::new(
        Opts::new("repo_total_count", "Repo in-memory entry count"),
        &["repo"],
    )
    .unwrap();
    let repo_dirty = GaugeVec::new(
        Opts::new("repo_dirty_count", "Repo dirty entry count"),
        &["repo"],
    )
    .unwrap();
    let sched_queue = Gauge::new("pdc_scheduler_queue_len", "Scheduler queue length").unwrap();
    let sched_running = GaugeVec::new(
        Opts::new(
            "pdc_scheduler_running_tasks",
            "Scheduler running tasks by category",
        ),
        &["category"],
    )
    .unwrap();
    let sched_execs = IntCounter::new(
        "pdc_scheduler_task_executions_total",
        "Scheduler admitted task executions",
    )
    .unwrap();

    registry
        .register(Box::new(crawler_send_pps.clone()))
        .unwrap();
    registry
        .register(Box::new(crawler_recv_pps.clone()))
        .unwrap();
    registry.register(Box::new(crawler_rr.clone())).unwrap();
    registry
        .register(Box::new(crawler_multiplier.clone()))
        .unwrap();
    registry
        .register(Box::new(crawler_predicted.clone()))
        .unwrap();
    registry
        .register(Box::new(crawler_pending.clone()))
        .unwrap();
    registry.register(Box::new(crawler_loss.clone())).unwrap();
    registry
        .register(Box::new(crawler_concurrent.clone()))
        .unwrap();
    registry.register(Box::new(crawler_queue.clone())).unwrap();
    registry
        .register(Box::new(crawler_dropped.clone()))
        .unwrap();
    registry.register(Box::new(crawler_paced.clone())).unwrap();
    registry
        .register(Box::new(node_discovered.clone()))
        .unwrap();
    registry.register(Box::new(repo_total.clone())).unwrap();
    registry.register(Box::new(repo_dirty.clone())).unwrap();
    registry.register(Box::new(sched_queue.clone())).unwrap();
    registry.register(Box::new(sched_running.clone())).unwrap();
    registry.register(Box::new(sched_execs.clone())).unwrap();

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
    CRAWLER_SOCKET_SEND_PPS.set(crawler_send_pps).ok();
    CRAWLER_SOCKET_RECV_PPS.set(crawler_recv_pps).ok();
    CRAWLER_SOCKET_RESPONSE_RATE.set(crawler_rr).ok();
    CRAWLER_ADAPTIVE_MULTIPLIER.set(crawler_multiplier).ok();
    CRAWLER_PREDICTED_RESPONSE_RATE.set(crawler_predicted).ok();
    CRAWLER_PENDING_TOTAL.set(crawler_pending).ok();
    CRAWLER_UDP_PACKET_LOSS.set(crawler_loss).ok();
    CRAWLER_CONCURRENT_SOCKETS.set(crawler_concurrent).ok();
    CRAWLER_SEND_QUEUE_LEN.set(crawler_queue).ok();
    CRAWLER_ENQUEUE_DROPPED_TOTAL.set(crawler_dropped).ok();
    CRAWLER_PACED_MODE.set(crawler_paced).ok();
    NODE_DISCOVERED_TOTAL.set(node_discovered).ok();
    REPO_TOTAL_COUNT.set(repo_total).ok();
    REPO_DIRTY_COUNT.set(repo_dirty).ok();
    SCHEDULER_QUEUE_LEN.set(sched_queue).ok();
    SCHEDULER_RUNNING_TASKS.set(sched_running).ok();
    SCHEDULER_TASK_EXECUTIONS_TOTAL.set(sched_execs).ok();

    REGISTRY.set(registry).ok();
}

/// 每 tick 从运行时快照更新全部指标（19号 D4：修复 R4 注册与更新断线）
pub fn update_from_snapshot(
    state: &super::AppState,
    data: &super::stats_snapshot::StatsSnapshotData,
) {
    // 爬虫深度指标
    if let Some(cs) = state.crawler_state.as_ref().map(|c| c.read().clone()) {
        if let Some(m) = DHT_MESSAGES_RECEIVED.get() {
            m.reset();
            m.inc_by(cs.messages_received);
        }
        if let Some(m) = DHT_REQUESTS_SENT.get() {
            m.reset();
            m.inc_by(cs.requests_sent);
        }
        for (i, v) in cs.socket_send_pps.iter().enumerate() {
            if let Some(g) = CRAWLER_SOCKET_SEND_PPS.get() {
                g.with_label_values(&[&i.to_string()]).set(*v as f64);
            }
        }
        for (i, v) in cs.socket_recv_pps.iter().enumerate() {
            if let Some(g) = CRAWLER_SOCKET_RECV_PPS.get() {
                g.with_label_values(&[&i.to_string()]).set(*v as f64);
            }
        }
        for (i, v) in cs.socket_response_rates.iter().enumerate() {
            if let Some(g) = CRAWLER_SOCKET_RESPONSE_RATE.get() {
                g.with_label_values(&[&i.to_string()]).set(*v);
            }
        }
        if let Some(g) = CRAWLER_ADAPTIVE_MULTIPLIER.get() {
            g.set(cs.adaptive_multiplier);
        }
        if let Some(v) = cs.predicted_response_rate {
            if let Some(g) = CRAWLER_PREDICTED_RESPONSE_RATE.get() {
                g.set(v);
            }
        }
        let pending: usize = cs.pending_shard_lens.iter().sum();
        if let Some(g) = CRAWLER_PENDING_TOTAL.get() {
            g.set(pending as f64);
        }
        if let Some(g) = CRAWLER_CONCURRENT_SOCKETS.get() {
            g.set(cs.concurrent_sockets_in_use as f64);
        }
        if let Some(g) = CRAWLER_UDP_PACKET_LOSS.get() {
            g.set(cs.udp_packet_loss_estimate);
        }
        if let Some(g) = CRAWLER_SEND_QUEUE_LEN.get() {
            g.set(cs.send_queue_len as f64);
        }
        if let Some(m) = CRAWLER_ENQUEUE_DROPPED_TOTAL.get() {
            m.reset();
            m.inc_by(cs.enqueue_dropped_total);
        }
        if let Some(g) = CRAWLER_PACED_MODE.get() {
            g.set(if cs.paced_mode { 1.0 } else { 0.0 });
        }
    }
    // 新节点发现总数（单一咽喉点）
    if let Some(nr) = &state.node_repo {
        if let Some(m) = NODE_DISCOVERED_TOTAL.get() {
            m.reset();
            m.inc_by(nr.new_nodes_total());
        }
        if let Some(g) = REPO_TOTAL_COUNT.get() {
            g.with_label_values(&["node"]).set(nr.len_sync() as f64);
        }
    }
    // 缓存与 repo 池
    if let Some(g) = CACHE_INFOHASHES.get() {
        g.set(data.cache_stats.total_infohashes as f64);
    }
    if let Some(g) = CACHE_PEERS.get() {
        g.set(data.cache_stats.total_peers as f64);
    }
    if let Some(pm) = &data.peer_repo_metrics {
        if let Some(g) = REPO_TOTAL_COUNT.get() {
            g.with_label_values(&["peer"]).set(pm.tier.total as f64);
        }
    }
    if let Some(im) = &data.infohash_repo_metrics {
        if let Some(g) = REPO_TOTAL_COUNT.get() {
            g.with_label_values(&["infohash"]).set(im.tier.total as f64);
        }
    }
    if let Some(tm) = &data.tracker_repo_metrics {
        if let Some(g) = REPO_TOTAL_COUNT.get() {
            g.with_label_values(&["tracker"]).set(tm.tier.total as f64);
        }
    }
    if let Some(nm) = &data.node_repo_metrics {
        if let Some(g) = REPO_DIRTY_COUNT.get() {
            g.with_label_values(&["node"]).set(nm.dirty_count as f64);
        }
    }
    // 调度器
    if let Some(sc) = SCHEDULER.get() {
        if let Some(g) = SCHEDULER_QUEUE_LEN.get() {
            g.set(sc.queue_len() as f64);
        }
        for (cat, n) in sc.running_by_category() {
            if let Some(g) = SCHEDULER_RUNNING_TASKS.get() {
                g.with_label_values(&[&cat]).set(n as f64);
            }
        }
        if let Some(m) = SCHEDULER_TASK_EXECUTIONS_TOTAL.get() {
            m.reset();
            m.inc_by(sc.admitted_total());
        }
    }
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
