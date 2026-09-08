//! 智能评分模块
//!
//! 提供 Node Score、Peer Score、Tracker Score、Health Score 等综合评分能力。
//! 【架构原则】评分系统是唯一维护评分的地方，所有 Repo 评分由 ScoreMaintainer 统一重算。

pub mod node_score;
pub mod tracker_score;
pub mod peer_score;
pub mod health_scorer;
pub mod tier_manager;
pub mod tier_system;
pub mod select_system;
pub mod scorer_config;
pub mod scorer_traits;
pub mod score_maintainer;

pub use node_score::{calculate_node_score, NodeScorerImpl};
pub use tracker_score::{TrackerScore, TrackerStats, TrackerScorerImpl};
pub use peer_score::PeerScorerImpl;
pub use health_scorer::HealthScorerImpl;
pub use tier_manager::{TierManager, TierConfig, TierStats, DataTier};
pub use tier_system::{TierSystem, NodeTierManagerAdapter, TierManageable as TierSystemManageable};
pub use select_system::SelectSystem;
pub use scorer_config::{ScorerConfig, NodeScoreConfig, PeerScoreConfig, TrackerScoreConfig};
pub use score_maintainer::ScoreMaintainer;
pub use scorer_traits::*;
