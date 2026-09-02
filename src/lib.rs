//! PeerDiscoveryCenter
//!
//! 统一的 BitTorrent Peer 发现中心：Tracker + DHT + PEX 三合一。
//!
//! ## 概述
//!
//! PeerDiscoveryCenter 提供统一的 peer 发现接口，内部整合了三种发现机制：
//! - **Tracker**：通过 BitTorrent Tracker 协议（HTTP/UDP）发现 peer
//! - **DHT**：通过分布式哈希表（Kademlia）发现 peer
//! - **PEX**：通过 Peer Exchange 从已连接的 peer 发现更多 peer
//!
//! 三种机制并发运行，结果自动合并、去重、按优先级排序。
//!
//! ## 快速开始
//!
//! ```text
//! use PeerDiscoveryCenter::aggregator::{PeerDiscoveryAggregator, PeerDiscoveryConfig};
//! use PeerDiscoveryCenter::tracker::TrackerDiscoverer;
//! use PeerDiscoveryCenter::dht::DhtDiscoverer;
//! use PeerDiscoveryCenter::pex::PexDiscoverer;
//! use std::sync::Arc;
//!
//! // 1. 创建聚合器
//! let config = PeerDiscoveryConfig::default();
//! let aggregator = Arc::new(PeerDiscoveryAggregator::new(config));
//!
//! // 2. 添加发现器
//! aggregator.add_discoverer(Box::new(TrackerDiscoverer::with_default_config()));
//! aggregator.add_discoverer(Box::new(DhtDiscoverer::with_default_config()));
//! aggregator.add_discoverer(Box::new(PexDiscoverer::with_default_config()));
//!
//! // 3. 发现 peer
//! let infohash = [0u8; 20];
//! let result = aggregator.discover_peers(&infohash, 100).await?;
//!
//! // 输出发现结果
//! for (source, count) in &result.source_stats {
//!     // 处理每个来源的 peer 数量
//! }
//! ```
//!
//! ## 模块结构
//!
//! - [`aggregator`] - 核心聚合器，统一调度所有发现器
//! - [`cache`] - Peer 缓存，支持去重、优先级排序、过期清理
//! - [`traits`] - 统一的 PeerDiscoverer trait 定义
//! - [`types`] - 公共数据结构（PeerInfo、PeerSource 等）
//! - [`tracker`] - Tracker 发现机制实现
//! - [`dht`] - DHT 发现机制实现
//! - [`pex`] - PEX 发现机制实现
//! - [`health_check`] - 健康检查后台任务

#![allow(non_snake_case)]

pub mod aggregator;
pub mod cache;
pub mod dht;
pub mod health_check;
pub mod pex;
pub mod tracker;
pub mod traits;
pub mod types;

// 重新导出常用类型
pub use aggregator::{PeerDiscoveryAggregator, PeerDiscoveryConfig};
pub use cache::PeerCache;
pub use traits::{AnnounceEvent, DiscovererStats, DiscovererType, PeerDiscoverer};
pub use types::{DiscoveryResult, Infohash, PeerInfo, PeerSource};

/// 库版本
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// 库名称
pub const NAME: &str = "PeerDiscoveryCenter";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
        assert_eq!(NAME, "PeerDiscoveryCenter");
    }
}
