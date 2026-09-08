//! DHT 节点评分系统
//!
//! 综合评估 DHT 节点质量，指导爬虫优先爬高质量节点。
//! 评分维度：响应率(40%)、延迟(20%)、节点产出(25%)、在线率(15%)
//! 【可配置化】权重通过 NodeScoreConfig 配置，支持运行时调整。

use async_trait::async_trait;

use crate::dht::kbucket::KBucketEntry;
use crate::intelligence::scorer_config::NodeScoreConfig;
use crate::intelligence::scorer_traits::NodeScorer;
use crate::storage::repo_traits::NodeRepository;

/// Node 评分器实现
pub struct NodeScorerImpl {
    config: NodeScoreConfig,
}

impl NodeScorerImpl {
    pub fn new() -> Self {
        Self {
            config: NodeScoreConfig::default(),
        }
    }

    pub fn with_config(config: NodeScoreConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &NodeScoreConfig {
        &self.config
    }
}

impl Default for NodeScorerImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NodeScorer for NodeScorerImpl {
    async fn rescore_all(&self, repo: &dyn NodeRepository) {
        let nodes = repo.all_nodes().await;
        let mut scores = Vec::with_capacity(nodes.len());
        for node in &nodes {
            let score = calculate_node_score_with_config(node, &self.config);
            scores.push((node.addr, score));
        }
        // 批量更新评分（一次事务）
        repo.update_scores_batch(&scores).await;
    }

    async fn rescore_dirty(&self, repo: &dyn NodeRepository) -> usize {
        let dirty_addrs = repo.dirty_nodes().await;
        if dirty_addrs.is_empty() {
            return 0;
        }
        let mut scores = Vec::with_capacity(dirty_addrs.len());
        for addr in &dirty_addrs {
            if let Some(node) = repo.get_node(addr).await {
                let score = calculate_node_score_with_config(&node, &self.config);
                scores.push((*addr, score));
            }
        }
        // 批量更新评分（一次事务）
        repo.update_scores_batch(&scores).await;
        // 清除脏标记
        repo.clear_all_dirty().await;
        scores.len()
    }

    fn calculate(&self, node: &KBucketEntry) -> f64 {
        calculate_node_score_with_config(node, &self.config)
    }
}

/// 计算单个节点的综合评分（0-100）使用默认配置
pub fn calculate_node_score(entry: &KBucketEntry) -> f64 {
    calculate_node_score_with_config(entry, &NodeScoreConfig::default())
}

/// 计算单个节点的综合评分（0-100）使用指定配置
pub fn calculate_node_score_with_config(entry: &KBucketEntry, config: &NodeScoreConfig) -> f64 {
    // Bad 状态直接 0 分
    if entry.state == crate::dht::kbucket::NodeState::Bad {
        return 0.0;
    }

    // 响应率得分
    let response_rate = if entry.query_count > 0 {
        entry.success_count as f64 / entry.query_count as f64
    } else {
        0.5 // 无查询记录给中性分
    };
    let response_rate_score = response_rate * config.response_rate_weight;

    // 延迟得分：越快越高，基准 5000ms 为 0，100ms 为满分
    let avg_latency = entry.avg_latency_ms();
    let latency_score = if avg_latency <= 0.0 {
        config.latency_weight * 0.5 // 无数据给中性分
    } else if avg_latency >= 5000.0 {
        0.0
    } else {
        config.latency_weight * (1.0 - avg_latency / 5000.0)
    };

    // 节点产出得分：每次查询平均返回的节点数（实际统计）
    let avg_nodes = entry.avg_nodes_returned();
    let nodes_output_score = if avg_nodes >= 8.0 {
        config.nodes_output_weight
    } else if avg_nodes <= 0.0 {
        // 无产出数据时用响应率代理（假设成功查询平均返回8节点）
        response_rate * config.nodes_output_weight
    } else {
        avg_nodes / 8.0 * config.nodes_output_weight
    };

    // 在线率得分：基于连续失败次数
    let uptime_score = if entry.consecutive_failures == 0 {
        config.uptime_weight
    } else if entry.consecutive_failures >= 3 {
        0.0
    } else {
        config.uptime_weight * (1.0 - entry.consecutive_failures as f64 / 3.0)
    };

    let mut total = response_rate_score + latency_score + nodes_output_score + uptime_score;

    // Questionable 状态惩罚
    if entry.state == crate::dht::kbucket::NodeState::Questionable {
        total *= config.questionable_penalty;
    }

    // 时间衰减：最后一次查询超过 decay_start_hours，评分按时间衰减
    if let Some(last_query) = entry.last_query_time {
        let hours_elapsed = last_query.elapsed().as_secs_f64() / 3600.0;
        if hours_elapsed > config.decay_start_hours {
            let decay_range = config.decay_end_hours - config.decay_start_hours;
            let decay = (1.0 - (hours_elapsed - config.decay_start_hours) / decay_range)
                .max(config.decay_min_factor);
            total *= decay;
        }
    }

    total.max(0.0).min(100.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn test_perfect_node_score() {
        let mut entry = KBucketEntry::new([0u8; 20], SocketAddr::from(([127, 0, 0, 1], 6881)));
        entry.query_count = 100;
        entry.success_count = 100;
        entry.total_latency_ms = 10000; // 100ms avg
        entry.consecutive_failures = 0;
        let score = calculate_node_score(&entry);
        assert!(score > 90.0);
    }

    #[test]
    fn test_bad_node_score() {
        let mut entry = KBucketEntry::new([0u8; 20], SocketAddr::from(([127, 0, 0, 1], 6881)));
        entry.consecutive_failures = 5;
        entry.state = crate::dht::kbucket::NodeState::Bad;
        let score = calculate_node_score(&entry);
        assert_eq!(score, 0.0);
    }
}
