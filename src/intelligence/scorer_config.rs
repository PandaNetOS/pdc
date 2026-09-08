//! 评分配置（可配置化权重）
//!
//! 所有评分维度的权重统一在这里配置，支持运行时调整。
//! 默认值与之前的硬编码权重保持一致。

use serde::{Deserialize, Serialize};

/// Node 评分权重配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeScoreConfig {
    /// 响应率权重（默认 40%）
    pub response_rate_weight: f64,
    /// 延迟权重（默认 20%）
    pub latency_weight: f64,
    /// 节点产出权重（默认 25%）
    pub nodes_output_weight: f64,
    /// 在线率权重（默认 15%）
    pub uptime_weight: f64,
    /// Questionable 状态惩罚系数（默认 0.7）
    pub questionable_penalty: f64,
    /// 时间衰减起始时间（小时，默认 24）
    pub decay_start_hours: f64,
    /// 时间衰减结束时间（小时，默认 168=7天）
    pub decay_end_hours: f64,
    /// 时间衰减最低系数（默认 0.5）
    pub decay_min_factor: f64,
}

impl Default for NodeScoreConfig {
    fn default() -> Self {
        Self {
            response_rate_weight: 40.0,
            latency_weight: 20.0,
            nodes_output_weight: 25.0,
            uptime_weight: 15.0,
            questionable_penalty: 0.7,
            decay_start_hours: 24.0,
            decay_end_hours: 168.0,
            decay_min_factor: 0.5,
        }
    }
}

/// Peer 评分权重配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerScoreConfig {
    /// 来源可信度权重（默认 30%）
    pub source_weight: f64,
    /// TCP 可达性权重（默认 30%）
    pub reachability_weight: f64,
    /// DHT 支持权重（默认 20%）
    pub dht_support_weight: f64,
    /// 存活时间权重（默认 10%）
    pub uptime_weight: f64,
    /// 多 infohash 共享权重（默认 10%）
    pub shared_weight: f64,
}

impl Default for PeerScoreConfig {
    fn default() -> Self {
        Self {
            source_weight: 30.0,
            reachability_weight: 30.0,
            dht_support_weight: 20.0,
            uptime_weight: 10.0,
            shared_weight: 10.0,
        }
    }
}

/// Tracker 评分权重配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerScoreConfig {
    /// 成功率权重（默认 40%）
    pub success_rate_weight: f64,
    /// 响应速度权重（默认 20%）
    pub latency_weight: f64,
    /// peer 产出权重（默认 25%）
    pub peers_output_weight: f64,
    /// 在线率权重（默认 15%）
    pub uptime_weight: f64,
}

impl Default for TrackerScoreConfig {
    fn default() -> Self {
        Self {
            success_rate_weight: 40.0,
            latency_weight: 20.0,
            peers_output_weight: 25.0,
            uptime_weight: 15.0,
        }
    }
}

/// 统一评分配置
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScorerConfig {
    pub node: NodeScoreConfig,
    pub peer: PeerScoreConfig,
    pub tracker: TrackerScoreConfig,
}

impl ScorerConfig {
    pub fn new() -> Self {
        Self::default()
    }
}
