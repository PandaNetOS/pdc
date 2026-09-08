//! 统一评分维护器
//!
//! 【架构原则】评分系统是唯一维护评分的地方。
//! 所有 Repo（Node/Peer/Tracker）的评分由本维护器定期统一重算，
//! 其他模块只更新统计数据、从 Repo 读取评分，不自行计算评分。
//!
//! 【增量评分策略】
//! - 热节点（最近活跃）：统计数据变化频繁，每 10 秒增量重算脏节点
//! - 全量重算：每 300 秒兜底一次，确保所有节点评分一致性
//! - 脏标记：节点统计数据变化时自动标记，只重算脏节点

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info};

use crate::intelligence::scorer_traits::{NodeScorer, PeerScorer, TrackerScorer};
use crate::storage::repo_traits::{NodeRepository, PeerRepository, TrackerRepository};

/// 统一评分维护器
pub struct ScoreMaintainer {
    node_repo: Option<Arc<dyn NodeRepository>>,
    peer_repo: Option<Arc<dyn PeerRepository>>,
    tracker_repo: Option<Arc<dyn TrackerRepository>>,
    node_scorer: Arc<dyn NodeScorer>,
    peer_scorer: Arc<dyn PeerScorer>,
    tracker_scorer: Arc<dyn TrackerScorer>,
    /// 增量重算间隔（秒）
    pub incremental_interval_secs: u64,
    /// 全量重算间隔（秒）
    pub full_interval_secs: u64,
}

impl ScoreMaintainer {
    pub fn new(
        node_scorer: Arc<dyn NodeScorer>,
        peer_scorer: Arc<dyn PeerScorer>,
        tracker_scorer: Arc<dyn TrackerScorer>,
    ) -> Self {
        Self {
            node_repo: None,
            peer_repo: None,
            tracker_repo: None,
            node_scorer,
            peer_scorer,
            tracker_scorer,
            incremental_interval_secs: 10,  // 每10秒增量重算脏节点
            full_interval_secs: 300,         // 每5分钟全量重算兜底
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

    /// 增量重算：只重算脏节点（统计数据有变化的节点）
    pub async fn rescore_incremental(&self) -> usize {
        let mut total_rescored = 0;
        if let Some(repo) = &self.node_repo {
            // 先刷新所有节点状态（基于最后活跃时间更新 Good/Questionable）
            repo.refresh_all_states().await;
            // 增量重算脏节点
            let count = self.node_scorer.rescore_dirty(repo.as_ref()).await;
            total_rescored += count;
            debug!("[score_maintainer] NodeRepo 增量重算: {} 个脏节点", count);
        }
        total_rescored
    }

    /// 全量重算所有 Repo 的评分（兜底，确保一致性）
    pub async fn rescore_all(&self) {
        if let Some(repo) = &self.node_repo {
            // 先刷新所有节点状态
            repo.refresh_all_states().await;
            // 全量重算评分
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
    }

    /// 启动评分维护定时任务
    /// - 每 incremental_interval_secs 秒：增量重算脏节点
    /// - 每 full_interval_secs 秒：全量重算兜底
    pub fn start(self: Arc<Self>) {
        let incremental_interval = Duration::from_secs(self.incremental_interval_secs);
        let full_interval = Duration::from_secs(self.full_interval_secs);

        info!(
            "[score_maintainer] 评分维护任务已启动（增量每 {}s，全量每 {}s）",
            self.incremental_interval_secs, self.full_interval_secs
        );

        // 增量重算任务
        {
            let maintainer = self.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(incremental_interval).await;
                    maintainer.rescore_incremental().await;
                }
            });
        }

        // 全量重算任务（兜底）
        {
            let maintainer = self.clone();
            tokio::spawn(async move {
                // 延迟一个全量周期再开始，避免启动时和增量任务竞争
                tokio::time::sleep(full_interval).await;
                loop {
                    maintainer.rescore_all().await;
                    tokio::time::sleep(full_interval).await;
                }
            });
        }
    }
}
