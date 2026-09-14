//! 智能评分模块
//!
//! 提供 Node Score、Peer Score、Tracker Score、Health Score 等综合评分能力。
//! 【架构原则】评分系统是唯一维护评分的地方，所有 Repo 评分由 ScoreMaintainer 统一重算。

pub mod availability;
pub mod dht_activity;
pub mod health_scorer;
pub mod infohash_score;
pub mod node_score;
pub mod peer_history;
pub mod peer_score;
pub mod score_maintainer;
pub mod scorer_config;
pub mod scorer_traits;
pub mod select_system;
pub mod task_scheduler;
pub mod tier_manager;
pub mod tier_system;
pub mod tracker_score;

pub use availability::{AvailabilityCalculator, AvailabilityMethod, AvailabilityResult};
pub use dht_activity::{ActivityEventType, DhtActivityTracker};
pub use health_scorer::HealthScorerImpl;
pub use infohash_score::{
    calculate_infohash_score, DataCompletenessLevel, InfohashHotnessLevel, InfohashScorerImpl,
};
pub use node_score::{calculate_node_score, NodeScorerImpl};
pub use peer_history::PeerHistoryManager;
pub use peer_score::PeerScorerImpl;
pub use score_maintainer::ScoreMaintainer;
pub use scorer_config::{
    HealthConfig, InfohashScoreConfig, NodeScoreConfig, PeerScoreConfig, PopularityConfig,
    ScorerConfig, TrackerScoreConfig,
};
pub use scorer_traits::*;
pub use select_system::SelectSystem;
pub use task_scheduler::{
    CategoryConcurrency, ResourceLevel, ResourceMonitor, ResourceProfile, ResourceState,
    TaskCategory, TaskMetadata, TaskPriority, TaskScheduler, TaskSchedulerSummary, TaskStats,
};
pub use tier_manager::{DataTier, TierConfig, TierManager, TierStats};
pub use tier_system::{NodeTierManagerAdapter, TierManageable as TierSystemManageable, TierSystem};
pub use tracker_score::{TrackerScore, TrackerScorerImpl, TrackerStats};
