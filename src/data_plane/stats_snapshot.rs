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
    PeerRepoMetrics, RepoTierStats, SuperTrackerStats, TrackerRepoMetrics, TrackerScoreInfo,
};
use crate::data_plane::AppState;

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
    /// 任务调度器指标（暂未注入）
    pub task_scheduler_metrics: Option<()>,
    /// NodeRepo 深度指标
    pub node_repo_metrics: Option<NodeRepoMetrics>,
    /// PeerRepo 冷热分层指标
    pub peer_repo_metrics: Option<PeerRepoMetrics>,
    /// InfohashRepo 冷热分层指标
    pub infohash_repo_metrics: Option<InfohashRepoMetrics>,
    /// TrackerRepo 冷热分层指标
    pub tracker_repo_metrics: Option<TrackerRepoMetrics>,
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

    tokio::spawn(async move {
        // 立即执行一次首次收集，避免启动后第一秒快照为空
        let data = collect_stats(&state);
        snapshot.update(data);

        // [ALLOWED-INTERVAL] 统计快照后台更新，间隔由 spawn_snapshot_updater 的 interval_secs 参数控制
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let data = collect_stats(&state);
                    snapshot.update(data);
                }
                _ = shutdown_clone.notified() => {
                    tracing::debug!("[stats_snapshot] 后台更新任务收到关闭信号，退出");
                    break;
                }
            }
        }
    });

    tracing::info!(
        "[stats_snapshot] 后台更新任务已启动（间隔 {} 秒）",
        interval_secs.max(1)
    );

    shutdown
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

    // ---- NodeRepo 深度指标 ----
    let node_repo_metrics = state.node_repo.as_ref().map(|nr| {
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
        task_scheduler_metrics: None,
        node_repo_metrics,
        peer_repo_metrics,
        infohash_repo_metrics,
        tracker_repo_metrics,
    }
}
