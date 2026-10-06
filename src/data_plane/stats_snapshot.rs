//! Stats Snapshot
//!
//! 后台任务定期从各 Repo 收集统计数据并写入快照。
//! REST API 的 /api/v1/stats 接口只读快照，避免在 API 线程上
//! 直接执行 DB COUNT 查询或锁竞争导致 API 失联。

use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::data_plane::rest_api::{
    CacheStats, CrawlerMetrics, DiscovererStat, FetcherStats, InfohashRepoMetrics, NodeRepoMetrics,
    PeerRepoMetrics, RepoTierStats, SuperTrackerStats, TaskSchedulerMetrics, TrackerRepoMetrics,
    TrackerScoreInfo,
};
use crate::data_plane::AppState;
use crate::intelligence::scorer_traits::HealthReport;

// ---------------------------------------------------------------------------
// 快照数据
// ---------------------------------------------------------------------------

/// 统计快照数据（纯数据结构，Clone 开销极小）
#[derive(Default, Clone, Debug)]
pub struct StatsSnapshotData {
    /// 快照生成时间（Unix 秒）
    pub snapshot_time_secs: u64,
    /// 发现器统计
    pub discoverer_stats: Vec<DiscovererStat>,
    /// 缓存统计（peer_repo 内存索引）
    pub cache_stats: CacheStats,
    /// 超级 Tracker 统计
    pub super_tracker_stats: SuperTrackerStats,
    /// Tracker 评分列表
    pub tracker_scores: Vec<TrackerScoreInfo>,
    /// Fetcher 统计
    pub fetcher_stats: Option<FetcherStats>,
    /// 爬虫深度指标
    pub crawler_metrics: Option<CrawlerMetrics>,
    /// 任务调度器指标（19号 D4：由 metrics::scheduler_metrics() 注入）
    pub task_scheduler_metrics: Option<TaskSchedulerMetrics>,
    /// NodeRepo 深度指标
    pub node_repo_metrics: Option<NodeRepoMetrics>,
    /// PeerRepo 冷热分层指标
    pub peer_repo_metrics: Option<PeerRepoMetrics>,
    /// InfohashRepo 冷热分层指标
    pub infohash_repo_metrics: Option<InfohashRepoMetrics>,
    /// TrackerRepo 冷热分层指标
    pub tracker_repo_metrics: Option<TrackerRepoMetrics>,
    /// NodeRepo 平均节点评分（DHT 层 avg_node_score；None = node_repo 未启用）
    pub node_avg_score: Option<f64>,
}

// ---------------------------------------------------------------------------
// 线程安全快照
// ---------------------------------------------------------------------------

/// 线程安全的统计快照持有者
///
/// 读操作通过 `parking_lot::RwLock` 获取克隆，不持锁跨越 await 点。
/// 写操作由后台 updater 独占，频率每秒一次。
pub struct StatsSnapshot {
    inner: RwLock<StatsSnapshotData>,
}

impl StatsSnapshot {
    /// 创建空快照
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(StatsSnapshotData::default()),
        }
    }

    /// 读取快照（克隆数据，微秒级）
    pub fn get(&self) -> StatsSnapshotData {
        self.inner.read().clone()
    }

    /// 更新快照（后台 updater 调用）
    fn update(&self, data: StatsSnapshotData) {
        *self.inner.write() = data;
    }
}

impl Default for StatsSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// 后台更新任务
// ---------------------------------------------------------------------------

/// 启动统计快照后台更新任务
///
/// # 参数
/// - `state`: 数据面共享状态（持有所有 Repo 的 Arc 引用）
/// - `interval_secs`: 更新间隔（秒），最小 1 秒
///
/// # 返回
/// 返回 shutdown 信号发送端。调用方在优雅关闭时调用
/// `notify_waiters()` 即可让后台任务退出。
pub fn spawn_snapshot_updater(state: AppState, interval_secs: u64) -> Arc<Notify> {
    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = shutdown.clone();
    let snapshot = state.stats_snapshot.clone();
    let interval = Duration::from_secs(interval_secs.max(1));
    // AppState 不能 Copy：先克隆给主快照任务，原 state 留给 D 任务再 clone
    let state_main = state.clone();

    tokio::spawn(async move {
        // 立即执行一次首次收集，避免启动后第一秒快照为空
        let data = collect_stats(&state_main);
        snapshot.update(data.clone());
        // Prometheus /metrics 每 tick 接线（19号 D4：修复注册与更新断线）
        crate::data_plane::metrics::update_from_snapshot(&state_main, &data);

        // [ALLOWED-INTERVAL] 统计快照后台更新，间隔由 spawn_snapshot_updater 的 interval_secs 参数控制
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let data = collect_stats(&state_main);
                    snapshot.update(data.clone());
                    crate::data_plane::metrics::update_from_snapshot(&state_main, &data);
                }
                _ = shutdown_clone.notified() => {
                    tracing::debug!("[stats_snapshot] 后台更新任务收到关闭信号，退出");
                    break;
                }
            }
        }
    });

    // D：60s 周期后台任务——NodeRepo 统计一致性抽查 + PeerRepo 活跃记账过期清理。
    // 两个动作都在后台线程执行，绝不触碰 API handler 路径；
    // 与主快照任务共享同一个 shutdown Notify（notify_waiters 会唤醒全部 waiter）。
    let shutdown_d = shutdown.clone();
    let state_d = state.clone();
    tokio::spawn(async move {
        // [ALLOWED-INTERVAL] 一致性抽查 + 活跃记账清理，固定 60s 周期
        // [ALLOWED-HARDCODED: 一致性抽查+活跃记账清理的固定周期常量，非业务可调参数]
        let mut ticker = tokio::time::interval(Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Some(nr) = state_d.node_repo.as_ref() {
                        let drift = nr.verify_stats_consistency();
                        if stats_drift_exceeds_threshold(
                            drift.total,
                            drift.good,
                            drift.questionable,
                            drift.bad,
                            drift.active,
                            drift.avg_score_diff,
                        ) {
                            tracing::warn!(
                                "[stats_snapshot] NodeRepo 统计一致性漂移: total={} good={} questionable={} bad={} active={} avg_score_diff={:.4}",
                                drift.total,
                                drift.good,
                                drift.questionable,
                                drift.bad,
                                drift.active,
                                drift.avg_score_diff
                            );
                        }
                    }
                    // 活跃 peer 记账过期清理（last_active 距今 > 3600s 的条目移除并 count--）
                    state_d.peer_repo.sweep_active_1h();
                }
                _ = shutdown_d.notified() => {
                    tracing::debug!("[stats_snapshot] 一致性抽查任务收到关闭信号，退出");
                    break;
                }
            }
        }
    });

    tracing::info!(
        "[stats_snapshot] 后台更新任务已启动（间隔 {} 秒）；一致性抽查任务已启动（60s）",
        interval_secs.max(1)
    );

    shutdown
}

// ---------------------------------------------------------------------------
// 健康报告纯计算（由快照数据推导，公式与 health_scorer::calculate 逐项一致）
// ---------------------------------------------------------------------------

/// 节点统计漂移是否超过告警阈值（纯函数，便于单测）。
///
/// 容忍 |计数差| ≤ 5、|avg_score_diff| ≤ 0.01：扫描瞬间并发写入会有 1~2 个瞬差。
pub fn stats_drift_exceeds_threshold(
    total: i64,
    good: i64,
    questionable: i64,
    bad: i64,
    active: i64,
    avg_score_diff: f64,
) -> bool {
    total.abs() > 5
        || good.abs() > 5
        || questionable.abs() > 5
        || bad.abs() > 5
        || active.abs() > 5
        || avg_score_diff.abs() > 0.01
}

/// 由统计快照纯计算系统健康分。
///
/// 公式逐项对齐 `crate::intelligence::health_scorer::HealthScorerImpl::calculate`：
/// - Tracker 层(40%)：richness(25%, ≥50 tracker 满分线性插值) + active_ratio(25%)
///   + avg_score(30%) + protocol_diversity(20%, http(s):// 与 udp:// 都在=100，单一=50，都无=0)；
/// - DHT 层(30%)：节点丰富度(40%, ≥10000 满分线性插值) + avg_node_score(60%)；
/// - Peer 层(30%)：ih_score(40%, ≥50 满分否则 ih*2.0) + peer_score(40%, ≥500 满分否则 peer*0.2)
///   + base 20；
/// - overall = tracker_layer*0.4 + dht_layer*0.3 + peer_layer*0.3。
///
/// 语义差异：输入为后台快照（最长约 1 个快照周期陈旧），而非实时遍历各 Repo；
/// tracker 维度来自 discoverer 评分快照（tracker_scores），node 平均分/总数来自 NodeRepo 快照。
pub fn health_report_from_snapshot(snap: &StatsSnapshotData) -> HealthReport {
    // ---- Tracker 层（40%）----
    let total_trackers = snap.tracker_scores.len();
    let active_trackers = snap.tracker_scores.iter().filter(|t| !t.disabled).count();
    let avg_tracker_score = if total_trackers > 0 {
        snap.tracker_scores.iter().map(|t| t.score).sum::<f64>() / total_trackers as f64
    } else {
        0.0
    };

    // 数量丰富度（25%）：≥50 个 tracker 满分，线性插值
    let richness_score = if total_trackers >= 50 {
        100.0
    } else {
        total_trackers as f64 / 50.0 * 100.0
    };

    // 活跃比例（25%）
    let active_ratio = if total_trackers > 0 {
        active_trackers as f64 / total_trackers as f64 * 100.0
    } else {
        0.0
    };

    // 协议多样性（20%）
    let has_http = snap
        .tracker_scores
        .iter()
        .any(|t| t.url.starts_with("http://") || t.url.starts_with("https://"));
    let has_udp = snap
        .tracker_scores
        .iter()
        .any(|t| t.url.starts_with("udp://"));
    let protocol_diversity = match (has_http, has_udp) {
        (true, true) => 100.0,
        (true, false) | (false, true) => 50.0,
        (false, false) => 0.0,
    };

    let tracker_layer = richness_score * 0.25
        + active_ratio * 0.25
        + avg_tracker_score * 0.30
        + protocol_diversity * 0.20;

    // ---- DHT 层（30%）----
    let total_nodes = snap
        .node_repo_metrics
        .as_ref()
        .map(|m| m.tier.total)
        .unwrap_or(0);
    // 节点池丰富度（40%）：≥10000 节点满分，线性插值
    let richness_nodes = if total_nodes >= 10000 {
        100.0
    } else {
        total_nodes as f64 / 10000.0 * 100.0
    };
    // 节点平均质量（60%）
    let avg_node_score = snap.node_avg_score.unwrap_or(0.0);
    let dht_layer = richness_nodes * 0.40 + avg_node_score * 0.60;

    // ---- Peer 层（30%）----
    let ih_count = snap.cache_stats.total_infohashes;
    let peer_count = snap.cache_stats.total_peers;
    // infohash 覆盖度：≥50 满分
    let ih_score = if ih_count >= 50 {
        100.0
    } else {
        ih_count as f64 * 2.0
    };
    // peer 丰富度：≥500 满分
    let peer_score = if peer_count >= 500 {
        100.0
    } else {
        peer_count as f64 * 0.2
    };
    // 基础分：缓存系统正常运行给 20 分
    let peer_layer = ih_score * 0.4 + peer_score * 0.4 + 20.0;

    // ---- 综合健康度 ----
    let overall = tracker_layer * 0.4 + dht_layer * 0.3 + peer_layer * 0.3;

    HealthReport {
        overall,
        tracker_layer,
        dht_layer,
        peer_layer,
        active_trackers,
        total_trackers,
        avg_tracker_score,
    }
}

/// 从各 Repo 同步收集统计数据
///
/// 此函数在后台任务中执行，可以安全地调用 DB 查询和锁读取，
/// 不会阻塞 API 线程。
fn collect_stats(state: &AppState) -> StatsSnapshotData {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // ---- 发现器统计 ----
    let registry = state.control_plane.registry();
    let config = state.config.read();
    let custom_trackers = &config.discoverers.custom_trackers;
    let total_tracker_count = if custom_trackers.is_empty() {
        crate::discoverers::tracker::PUBLIC_TRACKERS.len()
    } else {
        custom_trackers.len()
    };
    drop(config);

    let discoverer_stats: Vec<DiscovererStat> = registry
        .all()
        .iter()
        .map(|d| {
            let s = d.stats();
            let is_tracker = d.name() == "tracker";
            DiscovererStat {
                name: d.name().to_string(),
                discoverer_type: d.discoverer_type().as_str().to_string(),
                enabled: d.is_enabled(),
                total_requests: s.total_requests,
                success_requests: s.success_requests,
                failed_requests: s.failed_requests,
                total_peers_discovered: s.total_peers_discovered,
                success_rate: s.success_rate(),
                avg_response_time_ms: s.avg_response_time_ms,
                tracker_count: if is_tracker { total_tracker_count } else { 0 },
            }
        })
        .collect();

    // ---- Tracker 评分 ----
    let tracker_scores: Vec<TrackerScoreInfo> = registry
        .all()
        .iter()
        .find(|d| d.name() == "tracker")
        .and_then(|d| d.tracker_scores())
        .map(|scores| {
            scores
                .into_iter()
                .map(|(url, score, disabled)| TrackerScoreInfo {
                    url,
                    score,
                    disabled,
                })
                .collect()
        })
        .unwrap_or_default();

    // ---- 缓存统计（peer_repo 内存索引）----
    let cache_raw = state.peer_repo.stats();
    let cache_stats = CacheStats {
        total_infohashes: cache_raw.0,
        total_peers: cache_raw.1,
    };

    // ---- 超级 Tracker 统计 ----
    let super_tracker_stats = SuperTrackerStats {
        total_infohashes: state.super_tracker.infohash_count(),
        total_peers: state.super_tracker.peer_count(),
    };

    // ---- Fetcher 统计 ----
    let fetcher_stats = state.fetcher.as_ref().map(|f| FetcherStats {
        total_rounds: f.total_rounds.load(std::sync::atomic::Ordering::Relaxed),
        last_round_peers: f
            .last_round_peers
            .load(std::sync::atomic::Ordering::Relaxed),
        total_peers_fetched: f
            .total_peers_fetched
            .load(std::sync::atomic::Ordering::Relaxed),
        infohash_repo_count: f.infohash_count(),
    });

    // ---- 爬虫深度指标 ----
    let crawler_metrics = state.crawler_state.as_ref().map(|cs| {
        let s = cs.read();
        CrawlerMetrics {
            socket_send_pps: s.socket_send_pps.clone(),
            socket_recv_pps: s.socket_recv_pps.clone(),
            socket_response_rates: s.socket_response_rates.clone(),
            pending_shard_lens: s.pending_shard_lens.clone(),
            udp_packet_loss_estimate: s.udp_packet_loss_estimate,
            node_select_avg_us: s.node_select_avg_us,
            adaptive_multiplier: s.adaptive_multiplier,
            predicted_response_rate: s.predicted_response_rate,
            model_update_count: s.model_update_count,
            history_len: s.history_len,
            concurrent_sockets_in_use: s.concurrent_sockets_in_use,
        }
    });

    // ---- NodeRepo 深度指标 + 平均节点评分（健康报告 DHT 层用）----
    let mut node_avg_score: Option<f64> = None;
    let node_repo_metrics = state.node_repo.as_ref().map(|nr| {
        // stats_sync() 已由 NodeRepo 代理改造为 O(1)（原子计数器读），后台调用安全
        node_avg_score = Some(nr.stats_sync().avg_score);
        let (hot, warm, _) = nr.cache_stats();
        let total = nr.total_count_sync();
        NodeRepoMetrics {
            dirty_count: nr.dirty_count_sync(),
            write_queue_len: nr.write_queue_len_sync(),
            subnet_count: nr.subnet_count_sync(),
            tier: RepoTierStats {
                total,
                hot,
                warm,
                cold: total.saturating_sub(hot as u64).saturating_sub(warm as u64),
            },
        }
    });

    // ---- PeerRepo 冷热分层指标 ----
    let peer_repo_metrics = {
        let (hot, warm, _) = state.peer_repo.cache_stats();
        let total = state.peer_repo.total_count_sync();
        Some(PeerRepoMetrics {
            tier: RepoTierStats {
                total,
                hot,
                warm,
                cold: total.saturating_sub(hot as u64).saturating_sub(warm as u64),
            },
        })
    };

    // ---- InfohashRepo 冷热分层指标 ----
    let infohash_repo_metrics = state.infohash_repo.as_ref().map(|ir| {
        let (hot, warm, _) = ir.cache_stats();
        let total = ir.total_count_sync();
        InfohashRepoMetrics {
            tier: RepoTierStats {
                total,
                hot,
                warm,
                cold: total.saturating_sub(hot as u64).saturating_sub(warm as u64),
            },
        }
    });

    // ---- TrackerRepo 冷热分层指标 ----
    let tracker_repo_metrics = state.tracker_repo.as_ref().map(|tr| {
        let (hot, warm, _) = tr.cache_stats();
        let total = tr.total_count_sync();
        TrackerRepoMetrics {
            tier: RepoTierStats {
                total,
                hot,
                warm,
                cold: total.saturating_sub(hot as u64).saturating_sub(warm as u64),
            },
        }
    });

    StatsSnapshotData {
        snapshot_time_secs: now_secs,
        discoverer_stats,
        cache_stats,
        super_tracker_stats,
        tracker_scores,
        fetcher_stats,
        crawler_metrics,
        task_scheduler_metrics: crate::data_plane::metrics::scheduler_metrics(),
        node_repo_metrics,
        peer_repo_metrics,
        infohash_repo_metrics,
        tracker_repo_metrics,
        node_avg_score,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_plane::rest_api::{
        CacheStats, NodeRepoMetrics, RepoTierStats, TrackerScoreInfo,
    };

    fn approx_eq(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    fn tracker(url: &str, score: f64, disabled: bool) -> TrackerScoreInfo {
        TrackerScoreInfo {
            url: url.to_string(),
            score,
            disabled,
        }
    }

    fn node_metrics(total: u64) -> NodeRepoMetrics {
        NodeRepoMetrics {
            dirty_count: 0,
            write_queue_len: 0,
            subnet_count: 0,
            tier: RepoTierStats {
                total,
                hot: 0,
                warm: 0,
                cold: 0,
            },
        }
    }

    #[test]
    fn empty_snapshot_gives_only_base_peer_score() {
        let snap = StatsSnapshotData::default();
        let r = health_report_from_snapshot(&snap);
        assert!(approx_eq(r.tracker_layer, 0.0));
        assert!(approx_eq(r.dht_layer, 0.0));
        // peer 层 base 20，其余为 0
        assert!(approx_eq(r.peer_layer, 20.0));
        assert!(approx_eq(r.overall, 20.0 * 0.3));
        assert_eq!(r.active_trackers, 0);
        assert_eq!(r.total_trackers, 0);
        assert!(approx_eq(r.avg_tracker_score, 0.0));
        // 新字段读写
        assert_eq!(snap.node_avg_score, None);
    }

    #[test]
    fn full_marks_snapshot_is_100() {
        let snap = StatsSnapshotData {
            tracker_scores: (0..50)
                .map(|i| {
                    if i % 2 == 0 {
                        tracker("http://t.example.com/announce", 100.0, false)
                    } else {
                        tracker("udp://t.example.com:6969/announce", 100.0, false)
                    }
                })
                .collect(),
            node_repo_metrics: Some(node_metrics(10000)),
            node_avg_score: Some(100.0),
            cache_stats: CacheStats {
                total_infohashes: 50,
                total_peers: 500,
            },
            ..Default::default()
        };
        let r = health_report_from_snapshot(&snap);
        assert!(
            approx_eq(r.tracker_layer, 100.0),
            "tracker={}",
            r.tracker_layer
        );
        assert!(approx_eq(r.dht_layer, 100.0), "dht={}", r.dht_layer);
        assert!(approx_eq(r.peer_layer, 100.0), "peer={}", r.peer_layer);
        assert!(approx_eq(r.overall, 100.0), "overall={}", r.overall);
        assert_eq!(r.active_trackers, 50);
        assert_eq!(r.total_trackers, 50);
    }

    #[test]
    fn mixed_snapshot_matches_hand_calc() {
        // tracker: 10 个、8 活跃、均分 50、仅 http（无 udp）
        //   richness=20, active_ratio=80, score=50, proto=50
        //   tracker_layer = 20*.25 + 80*.25 + 50*.30 + 50*.20 = 5+20+15+10 = 50
        // node: 5000 个、avg=60
        //   richness=50, dht = 50*.4 + 60*.6 = 20+36 = 56
        // peer: ih=25, peer=100
        //   ih_score=50, peer_score=20, peer_layer = 20+8+20 = 48
        // overall = 50*.4 + 56*.3 + 48*.3 = 20 + 16.8 + 14.4 = 51.2
        let snap = StatsSnapshotData {
            tracker_scores: (0..10)
                .map(|i| tracker("http://t/announce", 50.0, i >= 8))
                .collect(),
            node_repo_metrics: Some(node_metrics(5000)),
            node_avg_score: Some(60.0),
            cache_stats: CacheStats {
                total_infohashes: 25,
                total_peers: 100,
            },
            ..Default::default()
        };
        let r = health_report_from_snapshot(&snap);
        assert!(
            approx_eq(r.tracker_layer, 50.0),
            "tracker={}",
            r.tracker_layer
        );
        assert!(approx_eq(r.dht_layer, 56.0), "dht={}", r.dht_layer);
        assert!(approx_eq(r.peer_layer, 48.0), "peer={}", r.peer_layer);
        assert!(approx_eq(r.overall, 51.2), "overall={}", r.overall);
        assert_eq!(r.active_trackers, 8);
        assert!(approx_eq(r.avg_tracker_score, 50.0));
    }

    #[test]
    fn protocol_diversity_boundaries() {
        // 仅 udp（无 http）：protocol_diversity = 50
        let snap = StatsSnapshotData {
            tracker_scores: vec![tracker("udp://t:6969/announce", 0.0, false)],
            ..Default::default()
        };
        let r = health_report_from_snapshot(&snap);
        // richness=100/50=2, ratio=100, score=0, proto=50
        // tracker_layer = 2*.25 + 100*.25 + 0 + 50*.2 = 0.5+25+10 = 35.5
        assert!(
            approx_eq(r.tracker_layer, 35.5),
            "tracker={}",
            r.tracker_layer
        );

        // 仅 http：同样 50
        let snap2 = StatsSnapshotData {
            tracker_scores: vec![tracker("https://t/announce", 0.0, false)],
            ..Default::default()
        };
        let r2 = health_report_from_snapshot(&snap2);
        assert!(
            approx_eq(r2.tracker_layer, 35.5),
            "tracker={}",
            r2.tracker_layer
        );

        // 两者都无（url 既非 http 也非 udp）：proto=0
        let snap3 = StatsSnapshotData {
            tracker_scores: vec![tracker("wss://t/announce", 0.0, false)],
            ..Default::default()
        };
        let r3 = health_report_from_snapshot(&snap3);
        // tracker_layer = 0.5 + 25 + 0 + 0 = 25.5
        assert!(
            approx_eq(r3.tracker_layer, 25.5),
            "tracker={}",
            r3.tracker_layer
        );
    }

    #[test]
    fn node_missing_means_zero_dht_layer() {
        // node_repo 未启用：dht 层应为 0（丰富度 0 + avg 缺省 0）
        let snap = StatsSnapshotData {
            cache_stats: CacheStats {
                total_infohashes: 10,
                total_peers: 100,
            },
            ..Default::default()
        };
        let r = health_report_from_snapshot(&snap);
        assert!(approx_eq(r.dht_layer, 0.0));
    }

    #[test]
    fn drift_threshold_judgement() {
        // 全 0：不告警
        assert!(!stats_drift_exceeds_threshold(0, 0, 0, 0, 0, 0.0));
        // 边界：差值恰好 5 / avg 恰好 0.01 → 不告警（容忍线）
        assert!(!stats_drift_exceeds_threshold(5, -5, 0, 0, 0, 0.01));
        // 任一计数 >5 → 告警
        assert!(stats_drift_exceeds_threshold(6, 0, 0, 0, 0, 0.0));
        assert!(stats_drift_exceeds_threshold(0, 0, 0, 0, -6, 0.0));
        // avg 差 >0.01 → 告警
        assert!(stats_drift_exceeds_threshold(0, 0, 0, 0, 0, 0.02));
    }
}
