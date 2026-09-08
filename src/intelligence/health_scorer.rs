//! HealthScorerImpl — 系统健康度评分（三层加权）
//!
//! 三层加权：Tracker层(40%) + DHT层(30%) + Peer层(30%)
//! 输入：四个 Repo 的数据快照，输出：HealthReport

use async_trait::async_trait;

use crate::intelligence::scorer_traits::{HealthReport, HealthScorer};
use crate::storage::repo_traits::{
    InfohashRepository, NodeRepository, PeerRepository, TrackerRepository,
};

pub struct HealthScorerImpl;

impl HealthScorerImpl {
    pub fn new() -> Self {
        Self
    }
}

impl Default for HealthScorerImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HealthScorer for HealthScorerImpl {
    async fn calculate(
        &self,
        trackers: &dyn TrackerRepository,
        nodes: &dyn NodeRepository,
        peers: &dyn PeerRepository,
        _infohashes: &dyn InfohashRepository,
    ) -> HealthReport {
        // ---- Tracker 层健康度（40%）----
        let all_trackers = trackers.all_trackers().await;
        let total_trackers = all_trackers.len();
        let active_trackers = all_trackers.iter().filter(|t| !t.disabled).count();
        let avg_tracker_score = if total_trackers > 0 {
            all_trackers.iter().map(|t| t.score).sum::<f64>() / total_trackers as f64
        } else {
            0.0
        };
        let active_ratio = if total_trackers > 0 {
            active_trackers as f64 / total_trackers as f64
        } else {
            0.0
        };
        // 活跃比例 50% + 平均评分 50%
        let tracker_layer = active_ratio * 50.0 + (avg_tracker_score / 100.0) * 50.0;

        // ---- DHT 层健康度（30%）----
        // 使用 stats() 替代 all_nodes() 全量克隆，避免大量内存分配
        let node_stats = nodes.stats().await;
        let total_nodes = node_stats.total;

        // 1. 节点池丰富度（35%）：≥10000 节点满分，线性插值
        let richness_score = if total_nodes >= 10000 {
            100.0
        } else {
            total_nodes as f64 / 10000.0 * 100.0
        };

        // 2. 节点平均质量（35%）：所有节点平均评分（0-100）
        let avg_node_score = node_stats.avg_score;

        // 3. 活跃节点比例（30%）：有查询记录的节点比例
        let active_node_ratio = if total_nodes > 0 {
            node_stats.active as f64 / total_nodes as f64
        } else {
            0.0
        };
        let activity_score = active_node_ratio * 100.0;

        let dht_layer = richness_score * 0.35 + avg_node_score * 0.35 + activity_score * 0.30;

        // ---- Peer 层健康度（30%）----
        let ih_count = peers.infohash_count().await;
        let peer_count = peers.peer_count().await;

        // infohash 覆盖度：>50 个 infohash 满分
        let ih_score = if ih_count >= 50 { 100.0 } else { ih_count as f64 * 2.0 };
        // peer 丰富度：>500 个 peer 满分
        let peer_score = if peer_count >= 500 { 100.0 } else { peer_count as f64 * 0.2 };
        // 基础分：缓存系统正常运行给 20 分
        let base_score = 20.0;
        let peer_layer = ih_score * 0.4 + peer_score * 0.4 + base_score;

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_scorer_creation() {
        let scorer = HealthScorerImpl::new();
        assert!(true);
    }
}
