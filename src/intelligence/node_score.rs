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
        // 只清除本次处理的脏标记，避免误清并发新增的标记导致漏算
        repo.clear_dirty_batch(&dirty_addrs).await;
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

    // 响应率得分：近期窗口优先（样本足够时），回落终身累计，再回落未验证先验
    let response_rate =
        match entry.windowed_response_rate(config.recent_decay_alpha, config.recent_min_samples) {
            Some(r) => r,
            None => {
                if entry.query_count > 0 {
                    entry.success_count as f64 / entry.query_count as f64
                } else {
                    // 未验证先验（2026-10 D3/R4）：刚被验证响应 ≈ 0.8、仅被提及 ≈ 0.3、
                    // 陈旧未验证 = 0.2 中性——替代旧「新节点 45 分随后被压到池底」的倒挂
                    let half_life = config.fresh_prior_half_life_hours.max(0.01);
                    let verified_boost = entry
                        .last_verified
                        .map(|t| {
                            config.fresh_prior_verified_boost
                                * 0.5f64.powf(t.elapsed().as_secs_f64() / 3600.0 / half_life)
                        })
                        .unwrap_or(0.0);
                    let mention_boost = entry
                        .last_mentioned
                        .map(|t| {
                            config.fresh_prior_mention_boost
                                * 0.5f64.powf(t.elapsed().as_secs_f64() / 3600.0 / half_life)
                        })
                        .unwrap_or(0.0);
                    (0.2 + verified_boost + mention_boost).min(1.0)
                }
            }
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

    total.clamp(0.0, 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::time::Instant;

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

    /// 未验证先验阶梯（2026-10 D3）：刚验证 > 仅被提及 > 陈旧中性；
    /// 「刚被证实活着」的节点不得再被压到池底（旧 45→26.6 倒挂）
    #[test]
    fn test_fresh_prior_ladder() {
        let now = Instant::now();
        let mut verified = KBucketEntry::new([1u8; 20], SocketAddr::from(([10, 0, 0, 1], 1000)));
        verified.last_verified = Some(now);
        let mut mentioned = KBucketEntry::new([2u8; 20], SocketAddr::from(([10, 0, 0, 2], 1000)));
        mentioned.last_mentioned = Some(now);
        let stale = KBucketEntry::new([3u8; 20], SocketAddr::from(([10, 0, 0, 3], 1000)));
        let mut stale = stale;
        stale.last_active = crate::utils::cutoff_before(std::time::Duration::from_secs(3600));

        let sv = calculate_node_score(&verified);
        let sm = calculate_node_score(&mentioned);
        let ss = calculate_node_score(&stale);
        assert!(sv > 70.0, "刚验证先验分应约 77，实测 {sv}");
        assert!(sm > ss && sm < 55.0, "仅被提及应介于两者之间，实测 {sm}");
        assert!(ss < 45.0, "陈旧未验证 = 中性先验，实测 {ss}");
    }

    /// 近期窗口优先于终身累计：终身 0% 成功但近期 100% → 高分；反之近期全败 → 快速跌分
    #[test]
    fn test_windowed_rate_overrides_lifetime() {
        // 终身 100 查 0 成功（历史很差），本会话近期 5 查 5 成功
        let mut recovered = KBucketEntry::new([4u8; 20], SocketAddr::from(([10, 0, 0, 4], 1000)));
        recovered.query_count = 100;
        recovered.success_count = 0;
        recovered.recent_query = 5.0;
        recovered.recent_success = 5.0;
        recovered.recent_updated = Some(Instant::now());
        recovered.total_latency_ms = 500;
        let good = calculate_node_score(&recovered);
        assert!(good > 60.0, "近期窗口应覆盖终身糟糕记录，实测 {good}");

        // 终身 100 查 100 成功（历史很好），本会话近期 5 查 0 成功
        let mut degraded = KBucketEntry::new([5u8; 20], SocketAddr::from(([10, 0, 0, 5], 1000)));
        degraded.query_count = 100;
        degraded.success_count = 100;
        degraded.total_latency_ms = 500;
        degraded.recent_query = 5.0;
        degraded.recent_updated = Some(Instant::now());
        let bad = calculate_node_score(&degraded);
        assert!(bad < 45.0, "近期全败应压过终身好记录，实测 {bad}");
    }
}
