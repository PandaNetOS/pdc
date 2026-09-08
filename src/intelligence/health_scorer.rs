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
        // 评分统一收口原则：HealthChecker 只做聚合，不做重复评分
        // 所有 per-entity 评分由 ScoreMaintainer 统一维护，HealthChecker 直接取平均值

        // ---- Tracker 层健康度（40%）----
        // 四维度加权：数量丰富度(25%) + 活跃比例(25%) + 平均评分(30%) + 协议多样性(20%)
        // 评分统一收口：平均评分由 ScoreMaintainer 维护，其他维度为聚合统计
        let all_trackers = trackers.all_trackers().await;
        let total_trackers = all_trackers.len();
        let active_trackers = all_trackers.iter().filter(|t| !t.disabled).count();
        let avg_tracker_score = if total_trackers > 0 {
            all_trackers.iter().map(|t| t.score).sum::<f64>() / total_trackers as f64
        } else {
            0.0
        };

        // 维度1：数量丰富度（25%）— ≥50 个 tracker 满分，线性插值
        let richness_score = if total_trackers >= 50 {
            100.0
        } else {
            total_trackers as f64 / 50.0 * 100.0
        };

        // 维度2：活跃比例（25%）— 未禁用 tracker 占比
        let active_ratio = if total_trackers > 0 {
            active_trackers as f64 / total_trackers as f64 * 100.0
        } else {
            0.0
        };

        // 维度3：平均评分（30%）— ScoreMaintainer 统一维护
        let score_dimension = avg_tracker_score;

        // 维度4：协议多样性（20%）— HTTP + UDP 都有 = 满分，只有一种 = 50分
        let has_http = all_trackers.iter().any(|t| t.url.starts_with("http://") || t.url.starts_with("https://"));
        let has_udp = all_trackers.iter().any(|t| t.url.starts_with("udp://"));
        let protocol_diversity = match (has_http, has_udp) {
            (true, true) => 100.0,
            (true, false) | (false, true) => 50.0,
            (false, false) => 0.0,
        };

        // Tracker 层健康度 = 四维度加权
        let tracker_layer = richness_score * 0.25
            + active_ratio * 0.25
            + score_dimension * 0.30
            + protocol_diversity * 0.20;

        // ---- DHT 层健康度（30%）----
        // 丰富度（节点数量）+ 平均节点 score（ScoreMaintainer 统一维护）
        // 不再重复计算"活跃节点比例"，因为节点 score 中已包含活跃度维度
        let node_stats = nodes.stats().await;
        let total_nodes = node_stats.total;

        // 节点池丰富度（40%）：≥10000 节点满分，线性插值
        let richness_score = if total_nodes >= 10000 {
            100.0
        } else {
            total_nodes as f64 / 10000.0 * 100.0
        };

        // 节点平均质量（60%）：直接取 ScoreMaintainer 维护的平均 score
        let avg_node_score = node_stats.avg_score;

        let dht_layer = richness_score * 0.40 + avg_node_score * 0.60;

        // ---- Peer 层健康度（30%）----
        // PeerRepo 目前没有 per-peer score 聚合，保持原有的覆盖度+丰富度计算
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
