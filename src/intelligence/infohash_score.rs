//! InfohashScorer — Infohash 热门度评分（分层评估：流行度+健康度）
//!
//! 【核心设计】
//! 1. 多源融合：不依赖单一数据源，PeerRepo+DHT+超级Tracker+外部Scrape多源互补
//! 2. 分层评估：流行度（Popularity）和健康度（Health）分开评估再综合
//! 3. 数据完整性感知：数据不足的infohash标记并限制评分上限，避免冷启动误判
//! 4. 对数缩放：所有数量指标用对数缩放，避免大值压制小值
//! 5. 时间衰减：越近的活动权重越高，旧数据自然衰减
//!
//! 【流行度维度】unique_peers(35%) + dht_query_rate(25%) + announce_rate(15%) + source_count(15%) + growth_rate(10%)
//! 【健康度维度】seeder_ratio(40%) + seeders_count(30%) + longevity(20%) + availability(10%)
//! 【最终评分】流行度×60% + 健康度×40%

use async_trait::async_trait;

use crate::intelligence::scorer_config::InfohashScoreConfig;
use crate::intelligence::scorer_traits::{InfohashScoreInput, InfohashScorer};
use crate::storage::repo_traits::InfohashRepository;
use crate::types::Infohash;

/// Infohash 评分器实现
pub struct InfohashScorerImpl {
    config: InfohashScoreConfig,
}

impl InfohashScorerImpl {
    pub fn new() -> Self {
        Self {
            config: InfohashScoreConfig::default(),
        }
    }

    pub fn with_config(config: InfohashScoreConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &InfohashScoreConfig {
        &self.config
    }
}

impl Default for InfohashScorerImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl InfohashScorer for InfohashScorerImpl {
    async fn rescore_all(&self, _repo: &dyn InfohashRepository) {
        // 全量重算由 ScoreMaintainer 协调：从多源聚合数据 → calculate → update_score
        tracing::debug!("[infohash_scorer] rescore_all called (aggregation handled by ScoreMaintainer)");
    }

    fn calculate(&self, input: &InfohashScoreInput) -> f64 {
        calculate_infohash_score_with_config(input, &self.config)
    }
}

/// 计算单个 infohash 的热门度评分（0-100）使用默认配置
pub fn calculate_infohash_score(input: &InfohashScoreInput) -> f64 {
    calculate_infohash_score_with_config(input, &InfohashScoreConfig::default())
}

/// 对数缩放归一化：将值映射到 0.0~1.0
///
/// 使用自然对数，达到 reference 值时得 1.0（满分）
/// 例如：log_scale(100, 1000) = ln(101)/ln(1001) ≈ 0.67
fn log_scale(value: f64, reference: f64) -> f64 {
    if value <= 0.0 {
        return 0.0;
    }
    let normalized = (value + 1.0).ln() / (reference + 1.0).ln();
    normalized.min(1.0).max(0.0)
}

/// 线性归一化：将值映射到 0.0~1.0
fn linear_scale(value: f64, max: f64) -> f64 {
    if max <= 0.0 {
        return 0.0;
    }
    (value / max).min(1.0).max(0.0)
}

/// 计算单个 infohash 的热门度评分（0-100）使用指定配置
pub fn calculate_infohash_score_with_config(
    input: &InfohashScoreInput,
    config: &InfohashScoreConfig,
) -> f64 {
    // ===== 1. 计算流行度（Popularity）0-100 =====
    let popularity = calculate_popularity(input, config);

    // ===== 2. 计算健康度（Health）0-100 =====
    let health = calculate_health(input, config);

    // ===== 3. 综合评分 =====
    let total_weight = config.popularity_total_weight + config.health_total_weight;
    let raw_score = if total_weight > 0.0 {
        (popularity * config.popularity_total_weight + health * config.health_total_weight) / total_weight
    } else {
        (popularity + health) / 2.0
    };

    // ===== 4. 数据完整性感知调整 =====
    let completeness = input.data_completeness();
    let final_score = if completeness < config.low_data_threshold {
        // 数据不足：评分上限
        raw_score.min(config.low_data_score_cap)
    } else if completeness < config.medium_data_threshold {
        // 数据有限：评分上限
        raw_score.min(config.medium_data_score_cap)
    } else {
        // 数据充足：正常评分
        raw_score
    };

    final_score.max(0.0).min(100.0)
}

/// 计算流行度（Popularity）0-100
fn calculate_popularity(input: &InfohashScoreInput, config: &InfohashScoreConfig) -> f64 {
    let p = &config.popularity;

    // 1. unique_peers（35%）：对数缩放，高覆盖率数据源
    let unique_peers_score = log_scale(input.unique_peers as f64, config.log_scale_unique_peers)
        * p.unique_peers_weight;

    // 2. dht_query_rate（25%）：对数缩放，高覆盖率数据源
    let dht_query_score = log_scale(input.dht_query_rate as f64, config.log_scale_dht_query_rate)
        * p.dht_query_rate_weight;

    // 3. announce_rate（15%）：低覆盖率数据源，有则用，无则给中性分（按比例分配）
    let announce_score = if let Some(rate) = input.announce_rate_5m {
        log_scale(rate as f64, config.log_scale_announce_rate) * p.announce_rate_weight
    } else {
        // 无数据时给中性分（50%），避免拉低总分
        0.5 * p.announce_rate_weight
    };

    // 4. source_count（15%）：线性缩放，高覆盖率数据源，≥5个来源满分
    let source_score = linear_scale(input.source_count as f64, 5.0)
        * p.source_diversity_weight;

    // 5. growth_rate（10%）：-1.0~1.0 映射到 0~100
    let growth_score = ((input.peer_growth_rate + 1.0) / 2.0).max(0.0).min(1.0)
        * p.growth_rate_weight;

    unique_peers_score + dht_query_score + announce_score + source_score + growth_score
}

/// 计算健康度（Health）0-100
fn calculate_health(input: &InfohashScoreInput, config: &InfohashScoreConfig) -> f64 {
    let h = &config.health;

    // 1. seeder_ratio（40%）：做种者比例，无数据时给中性分 0.5
    let ratio_score = input.seeder_ratio() * h.seeder_ratio_weight;

    // 2. seeders_count（30%）：做种者绝对数量，对数缩放
    //    无数据时用 unique_peers 作为代理（假设其中一部分是做种者）
    let seeders_for_score = if input.fused_seeders() > 0 {
        input.fused_seeders() as f64
    } else if input.unique_peers > 0 {
        // 无做种者数据时，用 unique_peers 的 30% 作为估算（保守估计）
        (input.unique_peers as f64) * 0.3
    } else {
        0.0
    };
    let seeders_score = log_scale(seeders_for_score, config.log_scale_seeders)
        * h.seeders_count_weight;

    // 3. longevity（20%）：持续时长，线性缩放，≥7天满分
    let longevity_score = linear_scale(
        input.longevity_secs as f64,
        config.longevity_full_score_secs as f64,
    ) * h.longevity_weight;

    // 4. availability_proxy（10%）：可用性代理
    //    如果没有显式设置，自动计算：peer>10且有做种者则高
    let availability = if input.availability_proxy > 0.0 {
        input.availability_proxy
    } else {
        // 自动计算可用性代理
        let has_peers = input.unique_peers >= 10;
        let has_seeders = input.fused_seeders() > 0;
        if has_peers && has_seeders {
            1.0
        } else if has_peers || has_seeders {
            0.5
        } else {
            0.0
        }
    };
    let availability_score = availability * h.availability_weight;

    ratio_score + seeders_score + longevity_score + availability_score
}

/// Infohash 热门度等级
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InfohashHotnessLevel {
    /// 冷门（0-30）
    Cold,
    /// 一般（30-50）
    Normal,
    /// 较热（50-70）
    Warm,
    /// 热门（70-90）
    Hot,
    /// 爆热（90-100）
    Trending,
}

impl InfohashHotnessLevel {
    pub fn from_score(score: f64) -> Self {
        match score {
            s if s >= 90.0 => InfohashHotnessLevel::Trending,
            s if s >= 70.0 => InfohashHotnessLevel::Hot,
            s if s >= 50.0 => InfohashHotnessLevel::Warm,
            s if s >= 30.0 => InfohashHotnessLevel::Normal,
            _ => InfohashHotnessLevel::Cold,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            InfohashHotnessLevel::Cold => "冷门",
            InfohashHotnessLevel::Normal => "一般",
            InfohashHotnessLevel::Warm => "较热",
            InfohashHotnessLevel::Hot => "热门",
            InfohashHotnessLevel::Trending => "爆热",
        }
    }
}

/// 数据完整性等级
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataCompletenessLevel {
    /// 数据不足（<30%维度有数据）
    Insufficient,
    /// 数据有限（30%-60%维度有数据）
    Limited,
    /// 数据充足（>60%维度有数据）
    Sufficient,
}

impl DataCompletenessLevel {
    pub fn from_completeness(completeness: f64, config: &InfohashScoreConfig) -> Self {
        if completeness < config.low_data_threshold {
            DataCompletenessLevel::Insufficient
        } else if completeness < config.medium_data_threshold {
            DataCompletenessLevel::Limited
        } else {
            DataCompletenessLevel::Sufficient
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            DataCompletenessLevel::Insufficient => "数据不足",
            DataCompletenessLevel::Limited => "数据有限",
            DataCompletenessLevel::Sufficient => "数据充足",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_scale() {
        assert_eq!(log_scale(0.0, 100.0), 0.0);
        assert!((log_scale(100.0, 100.0) - 1.0).abs() < 0.01);
        assert!(log_scale(50.0, 100.0) > 0.5);
        assert!(log_scale(1000.0, 100.0) <= 1.0);
    }

    #[test]
    fn test_perfect_infohash_score() {
        let input = InfohashScoreInput {
            unique_peers: 2000,
            dht_query_rate: 200,
            announce_rate_5m: Some(120),
            source_count: 5,
            peer_growth_rate: 0.5,
            seeders: Some(100),
            leechers: Some(50),
            external_seeders: Some(80),
            external_leechers: Some(40),
            longevity_secs: 604800 * 2, // 14天
            availability_proxy: 1.0,
            has_metadata: true,
            total_size: 1024 * 1024 * 1024,
        };
        let score = calculate_infohash_score(&input);
        assert!(score > 80.0, "perfect infohash should score high, got {}", score);
        assert!(score <= 100.0);
    }

    #[test]
    fn test_zero_infohash_score() {
        let input = InfohashScoreInput::default();
        let score = calculate_infohash_score(&input);
        // 全0数据：数据完整性低，评分上限30，且announce给中性分
        assert!(score <= 30.0, "zero data should be capped, got {}", score);
    }

    #[test]
    fn test_medium_infohash_score() {
        let input = InfohashScoreInput {
            unique_peers: 50,
            dht_query_rate: 20,
            announce_rate_5m: None, // 无announce数据
            source_count: 3,
            peer_growth_rate: 0.1,
            seeders: None,
            leechers: None,
            external_seeders: None,
            external_leechers: None,
            longevity_secs: 86400, // 1天
            availability_proxy: 0.5,
            has_metadata: false,
            total_size: 0,
        };
        let score = calculate_infohash_score(&input);
        assert!(score > 10.0 && score < 70.0, "medium infohash should score moderate, got {}", score);
    }

    #[test]
    fn test_hot_but_no_announce() {
        // 关键测试：热门infohash但没有向超级Tracker发announce
        // 应该通过 unique_peers + dht_query_rate + source_count 得到高分，不被误判
        let input = InfohashScoreInput {
            unique_peers: 500,           // 很多peer
            dht_query_rate: 150,         // DHT上很活跃
            announce_rate_5m: None,      // 没有向PDC发announce
            source_count: 4,             // 多来源发现
            peer_growth_rate: 0.3,       // 正增长
            seeders: None,
            leechers: None,
            external_seeders: Some(100), // 外部scrape有做种者
            external_leechers: Some(200),
            longevity_secs: 172800,      // 2天
            availability_proxy: 1.0,
            has_metadata: true,
            total_size: 2 * 1024 * 1024 * 1024,
        };
        let score = calculate_infohash_score(&input);
        assert!(score > 60.0, "hot infohash without announce should still score high, got {}", score);
    }

    #[test]
    fn test_data_completeness_calculation() {
        let input = InfohashScoreInput::default();
        assert!(input.data_completeness() < 0.3);

        let input = InfohashScoreInput {
            unique_peers: 10,
            dht_query_rate: 5,
            source_count: 2,
            longevity_secs: 3600,
            ..Default::default()
        };
        assert!(input.data_completeness() >= 0.3);
        assert!(input.data_completeness() < 0.6);
    }

    #[test]
    fn test_fused_seeders() {
        let input = InfohashScoreInput {
            seeders: Some(10),
            external_seeders: Some(50),
            ..Default::default()
        };
        assert_eq!(input.fused_seeders(), 50);

        let input = InfohashScoreInput {
            seeders: None,
            external_seeders: None,
            ..Default::default()
        };
        assert_eq!(input.fused_seeders(), 0);
    }

    #[test]
    fn test_seeder_ratio_neutral() {
        let input = InfohashScoreInput::default();
        // 无数据时给中性分 0.5
        assert_eq!(input.seeder_ratio(), 0.5);

        let input = InfohashScoreInput {
            seeders: Some(30),
            leechers: Some(70),
            ..Default::default()
        };
        assert!((input.seeder_ratio() - 0.3).abs() < 0.01);
    }

    #[test]
    fn test_hotness_level() {
        assert_eq!(InfohashHotnessLevel::from_score(95.0), InfohashHotnessLevel::Trending);
        assert_eq!(InfohashHotnessLevel::from_score(80.0), InfohashHotnessLevel::Hot);
        assert_eq!(InfohashHotnessLevel::from_score(60.0), InfohashHotnessLevel::Warm);
        assert_eq!(InfohashHotnessLevel::from_score(40.0), InfohashHotnessLevel::Normal);
        assert_eq!(InfohashHotnessLevel::from_score(20.0), InfohashHotnessLevel::Cold);
    }

    #[test]
    fn test_score_range() {
        // 确保评分始终在 0-100 范围内
        let input = InfohashScoreInput {
            unique_peers: u32::MAX,
            dht_query_rate: u32::MAX,
            announce_rate_5m: Some(u32::MAX),
            source_count: u32::MAX,
            peer_growth_rate: 1.0,
            seeders: Some(u32::MAX),
            leechers: Some(u32::MAX),
            external_seeders: Some(u32::MAX),
            external_leechers: Some(u32::MAX),
            longevity_secs: u64::MAX,
            availability_proxy: 1.0,
            has_metadata: true,
            total_size: u64::MAX,
        };
        let score = calculate_infohash_score(&input);
        assert!(score <= 100.0, "score should not exceed 100, got {}", score);
        assert!(score >= 0.0, "score should not be negative, got {}", score);
    }

    #[test]
    fn test_growth_rate_negative() {
        let input = InfohashScoreInput {
            unique_peers: 100,
            peer_growth_rate: -0.8, // 快速衰退
            ..Default::default()
        };
        let score = calculate_infohash_score(&input);
        // 负增长应该拉低流行度
        assert!(score < 50.0, "declining infohash should score lower, got {}", score);
    }
}
