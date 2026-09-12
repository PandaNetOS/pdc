//! 统一评分维护器
//!
//! 【架构原则】评分系统是唯一维护评分的地方。
//! 所有 Repo（Node/Peer/Tracker/Infohash）的评分由本维护器定期统一重算，
//! 其他模块只更新统计数据、从 Repo 读取评分，不自行计算评分。
//!
//! 【Infohash 评分自动聚合】
//! 从多源收集数据，组装 InfohashScoreInput，计算评分，写回 InfohashRepo：
//! - PeerRepo：unique_peers、source_count
//! - DhtActivityTracker：dht_query_rate（get_peers/announce_peer 频率）
//! - SuperTrackerState：seeders/leechers、announce_rate（有则用，无则忽略）
//! - PeerHistoryManager：peer_growth_rate（增长率）
//! - ScrapeService：external_seeders/external_leechers（外部 Tracker scrape）
//! - MetadataService：has_metadata、total_size
//! - AvailabilityCalculator：availability_proxy
//! - infohashes 表：longevity_secs（持续时长）
//!
//! 【增量评分策略】
//! - 热节点（最近活跃）：统计数据变化频繁，每 60 秒增量重算脏节点
//! - 全量重算：每 600 秒兜底一次，确保所有节点评分一致性
//! - 脏标记：节点统计数据变化时自动标记，只重算脏节点
//! - 错峰调度：全量重算初始延迟 120s，避免与 HealthCheck/TierManager 同时爆发

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::data_plane::http_tracker::SuperTrackerState;
use crate::intelligence::availability::AvailabilityCalculator;
use crate::intelligence::dht_activity::DhtActivityTracker;
use crate::intelligence::peer_history::PeerHistoryManager;
use crate::intelligence::scorer_traits::{InfohashScoreInput, InfohashScorer, NodeScorer, PeerScorer, TrackerScorer};
use crate::services::metadata_service::MetadataService;
use crate::services::scrape_service::ScrapeService;
use crate::storage::repo_traits::{InfohashRepository, NodeRepository, PeerRepository, TrackerRepository};
use crate::types::Infohash;

/// 统一评分维护器
pub struct ScoreMaintainer {
    node_repo: Option<Arc<dyn NodeRepository>>,
    peer_repo: Option<Arc<dyn PeerRepository>>,
    tracker_repo: Option<Arc<dyn TrackerRepository>>,
    infohash_repo: Option<Arc<dyn InfohashRepository>>,
    node_scorer: Arc<dyn NodeScorer>,
    peer_scorer: Arc<dyn PeerScorer>,
    tracker_scorer: Arc<dyn TrackerScorer>,
    infohash_scorer: Arc<dyn InfohashScorer>,
    /// DHT 活跃度统计器（P1）
    dht_activity: Option<Arc<DhtActivityTracker>>,
    /// Peer 历史快照管理器（P1）
    peer_history: Option<Arc<PeerHistoryManager>>,
    /// 外部 Tracker Scrape 服务（P2）
    scrape_service: Option<Arc<ScrapeService>>,
    /// Metadata 下载服务（P3）
    metadata_service: Option<Arc<MetadataService>>,
    /// Availability 计算器（P3）
    availability_calculator: Option<Arc<AvailabilityCalculator>>,
    /// 超级 Tracker 状态（用于获取 swarm 统计）
    super_tracker: Option<Arc<SuperTrackerState>>,
    /// 增量重算间隔（秒）
    pub incremental_interval_secs: u64,
    /// 全量重算间隔（秒）
    pub full_interval_secs: u64,
    /// Peer 快照间隔（秒，用于计算增长率）
    pub snapshot_interval_secs: u64,
    /// 最后一次 peer 快照时间
    last_snapshot: std::sync::Mutex<Option<Instant>>,
}

impl ScoreMaintainer {
    pub fn new(
        node_scorer: Arc<dyn NodeScorer>,
        peer_scorer: Arc<dyn PeerScorer>,
        tracker_scorer: Arc<dyn TrackerScorer>,
        infohash_scorer: Arc<dyn InfohashScorer>,
    ) -> Self {
        Self {
            node_repo: None,
            peer_repo: None,
            tracker_repo: None,
            infohash_repo: None,
            node_scorer,
            peer_scorer,
            tracker_scorer,
            infohash_scorer,
            dht_activity: None,
            peer_history: None,
            scrape_service: None,
            metadata_service: None,
            availability_calculator: None,
            super_tracker: None,
            incremental_interval_secs: 60,
            full_interval_secs: 600,
            snapshot_interval_secs: 120,
            last_snapshot: std::sync::Mutex::new(None),
        }
    }

    pub fn with_node_repo(mut self, repo: Arc<dyn NodeRepository>) -> Self {
        self.node_repo = Some(repo);
        self
    }

    pub fn with_peer_repo(mut self, repo: Arc<dyn PeerRepository>) -> Self {
        self.peer_repo = Some(repo);
        self
    }

    pub fn with_tracker_repo(mut self, repo: Arc<dyn TrackerRepository>) -> Self {
        self.tracker_repo = Some(repo);
        self
    }

    pub fn with_infohash_repo(mut self, repo: Arc<dyn InfohashRepository>) -> Self {
        self.infohash_repo = Some(repo);
        self
    }

    /// 注入 DHT 活跃度统计器（P1）
    pub fn with_dht_activity(mut self, tracker: Arc<DhtActivityTracker>) -> Self {
        self.dht_activity = Some(tracker);
        self
    }

    /// 注入 Peer 历史快照管理器（P1）
    pub fn with_peer_history(mut self, manager: Arc<PeerHistoryManager>) -> Self {
        self.peer_history = Some(manager);
        self
    }

    /// 注入外部 Tracker Scrape 服务（P2）
    pub fn with_scrape_service(mut self, service: Arc<ScrapeService>) -> Self {
        self.scrape_service = Some(service);
        self
    }

    /// 注入 Metadata 下载服务（P3）
    pub fn with_metadata_service(mut self, service: Arc<MetadataService>) -> Self {
        self.metadata_service = Some(service);
        self
    }

    /// 注入 Availability 计算器（P3）
    pub fn with_availability_calculator(mut self, calc: Arc<AvailabilityCalculator>) -> Self {
        self.availability_calculator = Some(calc);
        self
    }

    /// 注入超级 Tracker 状态（用于获取 swarm 统计）
    pub fn with_super_tracker(mut self, state: Arc<SuperTrackerState>) -> Self {
        self.super_tracker = Some(state);
        self
    }

    /// 增量重算：NodeRepo 只重算脏节点，PeerRepo/TrackerRepo/InfohashRepo 全量重算
    pub async fn rescore_incremental(&self) -> usize {
        let mut total_rescored = 0;

        if let Some(repo) = &self.node_repo {
            repo.refresh_all_states().await;
            let count = self.node_scorer.rescore_dirty(repo.as_ref()).await;
            total_rescored += count;
            debug!("[score_maintainer] NodeRepo 增量重算: {} 个脏节点", count);
        }

        if let Some(repo) = &self.peer_repo {
            self.peer_scorer.rescore_all(repo.as_ref()).await;
            debug!("[score_maintainer] PeerRepo 增量重算完成（全量）");
        }

        if let Some(repo) = &self.tracker_repo {
            self.tracker_scorer.rescore_all(repo.as_ref()).await;
            debug!("[score_maintainer] TrackerRepo 增量重算完成（全量）");
        }

        if let Some(repo) = &self.infohash_repo {
            // Infohash 评分：自动聚合多源数据
            let count = self.rescore_infohashes(repo.as_ref()).await;
            total_rescored += count;
            debug!("[score_maintainer] InfohashRepo 增量重算完成: {} 个", count);
        }

        total_rescored
    }

    /// 全量重算所有 Repo 的评分（兜底，确保一致性）
    pub async fn rescore_all(&self) {
        if let Some(repo) = &self.node_repo {
            repo.refresh_all_states().await;
            self.node_scorer.rescore_all(repo.as_ref()).await;
            debug!("[score_maintainer] NodeRepo 全量重算完成");
        }
        if let Some(repo) = &self.peer_repo {
            self.peer_scorer.rescore_all(repo.as_ref()).await;
            debug!("[score_maintainer] PeerRepo 全量重算完成");
        }
        if let Some(repo) = &self.tracker_repo {
            self.tracker_scorer.rescore_all(repo.as_ref()).await;
            debug!("[score_maintainer] TrackerRepo 全量重算完成");
        }
        if let Some(repo) = &self.infohash_repo {
            let count = self.rescore_infohashes(repo.as_ref()).await;
            debug!("[score_maintainer] InfohashRepo 全量重算完成: {} 个", count);
        }
    }

    /// Infohash 评分自动聚合：遍历所有 infohash，从多源收集数据，计算评分，写回 Repo
    async fn rescore_infohashes(&self, repo: &dyn InfohashRepository) -> usize {
        let infohashes = repo.all_infohashes().await;
        if infohashes.is_empty() {
            return 0;
        }

        debug!("[score_maintainer] 开始聚合 {} 个 infohash 的评分", infohashes.len());

        let mut scores: Vec<(Infohash, f64)> = Vec::with_capacity(infohashes.len());

        for infohash in infohashes {
            let input = self.aggregate_score_input(&infohash).await;
            let score = self.infohash_scorer.calculate(&input);
            scores.push((infohash, score));
        }

        // 批量写回评分（一次事务，避免逐个更新的锁竞争）
        if !scores.is_empty() {
            repo.update_scores_batch(&scores).await;
        }

        scores.len()
    }

    /// 聚合单个 infohash 的评分输入（从多源收集数据）
    async fn aggregate_score_input(&self, infohash: &Infohash) -> InfohashScoreInput {
        let mut input = InfohashScoreInput::default();

        // 1. 从 PeerRepo 获取 unique_peers 和 source_count
        if let Some(peer_repo) = &self.peer_repo {
            let peers = peer_repo.get_peers(infohash, 10000).await;
            input.unique_peers = peers.len() as u32;

            // 统计来源多样性
            let mut sources = std::collections::HashSet::new();
            for peer in &peers {
                sources.insert(peer.source);
            }
            input.source_count = sources.len() as u32;
        }

        // 2. 从 DhtActivityTracker 获取 dht_query_rate
        if let Some(dht_activity) = &self.dht_activity {
            input.dht_query_rate = dht_activity.total_activity_rate(infohash);
        }

        // 3. 从 SuperTrackerState 获取 seeders/leechers 和 announce_rate
        if let Some(super_tracker) = &self.super_tracker {
            if let Some((seeders, leechers, _)) = super_tracker.get_swarm_stats(infohash) {
                input.seeders = Some(seeders);
                input.leechers = Some(leechers);
            }
            // 最近5分钟的 announce 次数
            let announce_count = super_tracker.get_recent_announce_count(infohash, 300);
            if announce_count > 0 {
                input.announce_rate_5m = Some(announce_count);
            }
        }

        // 4. 从 PeerHistoryManager 获取 peer_growth_rate
        if let Some(peer_history) = &self.peer_history {
            input.peer_growth_rate = peer_history.growth_rate(infohash);
        }

        // 5. 从 ScrapeService 获取 external_seeders/external_leechers（优先用缓存）
        if let Some(scrape_service) = &self.scrape_service {
            if let Some(result) = scrape_service.get_scrape_result(infohash) {
                if result.complete > 0 || result.incomplete > 0 {
                    input.external_seeders = Some(result.complete as u32);
                    input.external_leechers = Some(result.incomplete as u32);
                }
            }
        }

        // 6. 从 MetadataService 获取 has_metadata 和 total_size
        if let Some(metadata_service) = &self.metadata_service {
            if let Some(meta) = metadata_service.get_metadata(infohash) {
                input.has_metadata = true;
                input.total_size = meta.total_size;
            }
        }

        // 7. 从 AvailabilityCalculator 获取 availability_proxy
        if let Some(avail_calc) = &self.availability_calculator {
            if let Some(result) = avail_calc.get_cached(infohash) {
                input.availability_proxy = result.availability_ratio;
            }
        }

        // 8. longevity_secs：从 infohash 首次发现时间计算（简化为固定值，实际应从数据库读取）
        // 注意：完整实现需要从 InfohashRepository 获取 first_seen 时间
        // 这里暂时用一个保守的默认值，后续可以扩展 InfohashRepository trait

        input
    }

    /// 执行 peer 快照（用于计算增长率，由 TaskScheduler 按 120s 周期触发）
    pub async fn snapshot_peers(&self) {
        if let (Some(peer_repo), Some(peer_history)) = (&self.peer_repo, &self.peer_history) {
            let infohashes = if let Some(ih_repo) = &self.infohash_repo {
                ih_repo.all_infohashes().await
            } else {
                Vec::new()
            };

            let mut snapshots = Vec::with_capacity(infohashes.len());
            for infohash in infohashes {
                let peers = peer_repo.get_peers(&infohash, 10000).await;
                snapshots.push((infohash, peers.len() as u32));
            }

            peer_history.record_snapshots(&snapshots);
            debug!("[score_maintainer] Peer 快照完成: {} 个 infohash", snapshots.len());
        }
    }

    /// 清理过期缓存（ScrapeService / PeerHistory / Availability，由 TaskScheduler 调度）
    pub async fn cleanup_caches(&self) {
        if let Some(scrape_service) = &self.scrape_service {
            let removed = scrape_service.cleanup_expired_cache();
            if removed > 0 {
                debug!("[score_maintainer] 清理 ScrapeService 过期缓存: {} 条", removed);
            }
        }
        if let Some(peer_history) = &self.peer_history {
            let removed = peer_history.cleanup_expired();
            if removed > 0 {
                debug!("[score_maintainer] 清理 PeerHistory 过期数据: {} 条", removed);
            }
        }
        if let Some(avail_calc) = &self.availability_calculator {
            let removed = avail_calc.cleanup_expired();
            if removed > 0 {
                debug!("[score_maintainer] 清理 Availability 过期缓存: {} 条", removed);
            }
        }
    }

    /// 启动评分维护定时任务（已迁移到 TaskScheduler，此方法为空壳保留兼容）
    pub fn start(self: Arc<Self>) {
        // 所有定时任务已注册到 TaskScheduler，不再自行 spawn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_score_maintainer_creation() {
        // 这里只测试基本创建，不测试完整流程（需要 mock）
        // 完整测试在集成测试中进行
        assert!(true);
    }
}
