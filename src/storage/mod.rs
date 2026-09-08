//! 持久化存储模块
//!
//! 基于 SQLite 的持久化层，存储路由表、tracker 池、历史数据和统计数据。

pub mod db;
pub mod repo_traits;
pub mod node_repo;
pub mod peer_repo;
pub mod infohash_repo;
pub mod tracker_repo;

pub use db::{DhtNodeRow, InfohashRow, PeerHistoryRow, PeerRow, Storage, TrackerRow};
pub use repo_traits::*;
pub use node_repo::NodeRepoImpl;
pub use peer_repo::PeerRepoImpl;
pub use infohash_repo::InfohashRepoImpl;
pub use tracker_repo::TrackerRepoImpl;
