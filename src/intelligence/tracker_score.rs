//! Tracker 评分系统
//!
//! 综合评估 tracker 质量，指导优先连接高质量 tracker。
//! 评分维度：成功率(40%)、响应速度(20%)、peer产出(25%)、在线率(15%)

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::intelligence::scorer_traits::TrackerScorer;
use crate::storage::repo_traits::TrackerRepository;

/// Tracker 评分器实现
pub struct TrackerScorerImpl;

impl TrackerScorerImpl {
    pub fn new() -> Self {
        Self
    }
}

impl Default for TrackerScorerImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TrackerScorer for TrackerScorerImpl {
    async fn rescore_all(&self, repo: &dyn TrackerRepository) {
        let trackers = repo.all_trackers().await;
        for tracker in &trackers {
            let score = self.calculate(
                tracker.total_requests,
                tracker.success_requests,
                tracker.total_peers_discovered,
                tracker.avg_response_time_ms,
                tracker.consecutive_failures,
                tracker.disabled,
            );
            repo.update_score(&tracker.url, score).await;
        }
    }

    fn calculate(
        &self,
        total_requests: u64,
        success_requests: u64,
        total_peers: u64,
        avg_latency_ms: f64,
        consecutive_failures: u32,
        disabled: bool,
    ) -> f64 {
        let stats = TrackerStats {
            total_requests,
            success_requests,
            failed_requests: total_requests.saturating_sub(success_requests),
            total_peers_discovered: total_peers,
            total_response_time_ms: avg_latency_ms * success_requests as f64,
            consecutive_failures,
            disabled,
        };
        stats.calculate_score().total
    }
}

/// Tracker 评分结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerScore {
    /// 综合评分（0-100）
    pub total: f64,
    /// 成功率得分（0-40）
    pub success_rate_score: f64,
    /// 响应速度得分（0-20）
    pub latency_score: f64,
    /// peer 产出得分（0-25）
    pub peers_per_request_score: f64,
    /// 在线率得分（0-15）
    pub uptime_score: f64,
}

impl Default for TrackerScore {
    fn default() -> Self {
        Self {
            total: 0.0,
            success_rate_score: 0.0,
            latency_score: 0.0,
            peers_per_request_score: 0.0,
            uptime_score: 0.0,
        }
    }
}

/// Tracker 运行统计（用于计算评分）
#[derive(Debug, Clone, Default)]
pub struct TrackerStats {
    /// 总请求数
    pub total_requests: u64,
    /// 成功请求数
    pub success_requests: u64,
    /// 失败请求数
    pub failed_requests: u64,
    /// 累计发现 peer 数
    pub total_peers_discovered: u64,
    /// 累计响应时间（毫秒）
    pub total_response_time_ms: f64,
    /// 连续失败次数
    pub consecutive_failures: u32,
    /// 是否被临时禁用
    pub disabled: bool,
}

impl TrackerStats {
    /// 计算成功率
    pub fn success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.success_requests as f64 / self.total_requests as f64
        }
    }

    /// 平均响应时间（毫秒）
    pub fn avg_response_time_ms(&self) -> f64 {
        if self.success_requests == 0 {
            0.0
        } else {
            self.total_response_time_ms / self.success_requests as f64
        }
    }

    /// 每次请求平均 peer 产出
    pub fn peers_per_request(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.total_peers_discovered as f64 / self.total_requests as f64
        }
    }

    /// 计算综合评分
    pub fn calculate_score(&self) -> TrackerScore {
        // 成功率得分（0-40）：成功率 * 40
        let success_rate_score = self.success_rate() * 40.0;

        // 响应速度得分（0-20）：越快越高，基准 5000ms 为 0 分，100ms 为满分
        let avg_latency = self.avg_response_time_ms();
        let latency_score = if avg_latency <= 0.0 {
            0.0
        } else if avg_latency >= 5000.0 {
            0.0
        } else {
            20.0 * (1.0 - avg_latency / 5000.0)
        };

        // peer 产出得分（0-25）：每次请求平均 peer 数，基准 10 个为满分
        let ppr = self.peers_per_request();
        let peers_per_request_score = if ppr >= 10.0 {
            25.0
        } else {
            ppr / 10.0 * 25.0
        };

        // 在线率得分（0-15）：基于连续失败次数，0 次失败为满分
        let uptime_score = if self.consecutive_failures == 0 {
            15.0
        } else if self.consecutive_failures >= 5 {
            0.0
        } else {
            15.0 * (1.0 - self.consecutive_failures as f64 / 5.0)
        };

        let total = if self.disabled {
            0.0
        } else {
            success_rate_score + latency_score + peers_per_request_score + uptime_score
        };

        TrackerScore {
            total,
            success_rate_score,
            latency_score,
            peers_per_request_score,
            uptime_score,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_perfect_score() {
        let stats = TrackerStats {
            total_requests: 100,
            success_requests: 100,
            failed_requests: 0,
            total_peers_discovered: 1000,
            total_response_time_ms: 10000.0, // 100ms avg
            consecutive_failures: 0,
            disabled: false,
        };
        let score = stats.calculate_score();
        assert!((score.total - 100.0).abs() < 1.0);
    }

    #[test]
    fn test_zero_score() {
        let stats = TrackerStats {
            total_requests: 100,
            success_requests: 0,
            failed_requests: 100,
            total_peers_discovered: 0,
            total_response_time_ms: 0.0,
            consecutive_failures: 10,
            disabled: true,
        };
        let score = stats.calculate_score();
        assert_eq!(score.total, 0.0);
    }

    #[test]
    fn test_medium_score() {
        let stats = TrackerStats {
            total_requests: 100,
            success_requests: 50,
            failed_requests: 50,
            total_peers_discovered: 200,
            total_response_time_ms: 50000.0, // 1000ms avg
            consecutive_failures: 1,
            disabled: false,
        };
        let score = stats.calculate_score();
        // success: 20, latency: 16, peers: 5, uptime: 12 = 53
        assert!((score.total - 53.0).abs() < 2.0);
    }
}
