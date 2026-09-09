//! 评分配置（可配置化权重）
//!
//! 所有评分维度的权重统一在这里配置，支持运行时调整。
//! InfohashScorer 采用分层评估：流行度（Popularity）+ 健康度（Health）

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

// ---------------------------------------------------------------------------
// Infohash 评分配置（分层评估：流行度 + 健康度）
// ---------------------------------------------------------------------------

/// 流行度（Popularity）评分权重配置
///
/// 反映有多少人在关注/下载这个资源
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PopularityConfig {
    /// 唯一 peer 数量权重（默认 35%）— 高覆盖率数据源
    pub unique_peers_weight: f64,
    /// DHT 查询频率权重（默认 25%）— 高覆盖率数据源
    pub dht_query_rate_weight: f64,
    /// announce 频率权重（默认 15%）— 低覆盖率数据源，有则用
    pub announce_rate_weight: f64,
    /// 来源多样性权重（默认 15%）— 高覆盖率数据源
    pub source_diversity_weight: f64,
    /// 增长率权重（默认 10%）— 反映上升趋势
    pub growth_rate_weight: f64,
}

impl Default for PopularityConfig {
    fn default() -> Self {
        Self {
            unique_peers_weight: 35.0,
            dht_query_rate_weight: 25.0,
            announce_rate_weight: 15.0,
            source_diversity_weight: 15.0,
            growth_rate_weight: 10.0,
        }
    }
}

/// 健康度（Health）评分权重配置
///
/// 反映这个资源能不能下完、速度快不快
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    /// 做种者比例权重（默认 40%）— seeders/total
    pub seeder_ratio_weight: f64,
    /// 做种者绝对数量权重（默认 30%）— 对数缩放
    pub seeders_count_weight: f64,
    /// 持续时长权重（默认 20%）— 经典资源加分
    pub longevity_weight: f64,
    /// 可用性代理权重（默认 10%）— peer>10且有做种者则高
    pub availability_weight: f64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            seeder_ratio_weight: 40.0,
            seeders_count_weight: 30.0,
            longevity_weight: 20.0,
            availability_weight: 10.0,
        }
    }
}

/// Infohash 热门度评分配置（分层评估）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfohashScoreConfig {
    /// 流行度配置
    pub popularity: PopularityConfig,
    /// 健康度配置
    pub health: HealthConfig,
    /// 流行度在最终评分中的权重（默认 60%）
    pub popularity_total_weight: f64,
    /// 健康度在最终评分中的权重（默认 40%）
    pub health_total_weight: f64,
    /// 数据完整性阈值：低于此值标记为"数据不足"，评分上限=30（默认 0.3）
    pub low_data_threshold: f64,
    /// 数据完整性阈值：低于此值标记为"数据有限"，评分上限=60（默认 0.6）
    pub medium_data_threshold: f64,
    /// 数据不足时的评分上限（默认 30）
    pub low_data_score_cap: f64,
    /// 数据有限时的评分上限（默认 60）
    pub medium_data_score_cap: f64,
    /// 对数缩放参考值：unique_peers 达到此值时满分（默认 1000）
    pub log_scale_unique_peers: f64,
    /// 对数缩放参考值：dht_query_rate 达到此值时满分（默认 100/小时）
    pub log_scale_dht_query_rate: f64,
    /// 对数缩放参考值：announce_rate 达到此值时满分（默认 60/5分钟）
    pub log_scale_announce_rate: f64,
    /// 对数缩放参考值：seeders 达到此值时满分（默认 50）
    pub log_scale_seeders: f64,
    /// 持续时长满分阈值（秒，默认 7天=604800）
    pub longevity_full_score_secs: u64,
}

impl Default for InfohashScoreConfig {
    fn default() -> Self {
        Self {
            popularity: PopularityConfig::default(),
            health: HealthConfig::default(),
            popularity_total_weight: 60.0,
            health_total_weight: 40.0,
            low_data_threshold: 0.3,
            medium_data_threshold: 0.6,
            low_data_score_cap: 30.0,
            medium_data_score_cap: 60.0,
            log_scale_unique_peers: 1000.0,
            log_scale_dht_query_rate: 100.0,
            log_scale_announce_rate: 60.0,
            log_scale_seeders: 50.0,
            longevity_full_score_secs: 604800, // 7天
        }
    }
}

/// 统一评分配置
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScorerConfig {
    pub node: NodeScoreConfig,
    pub peer: PeerScoreConfig,
    pub tracker: TrackerScoreConfig,
    pub infohash: InfohashScoreConfig,
}

impl ScorerConfig {
    pub fn new() -> Self {
        Self::default()
    }
}
