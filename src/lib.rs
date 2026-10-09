//! PeerDiscoveryCenter
//!
//! 统一的 BitTorrent Peer 发现中心：Tracker + DHT + PEX 三合一，
//! 终极形态：事件驱动 + 控制面/数据面分离 + 插件化发现器 + 双引擎 + 超级 Tracker。
//!
//! ## 概述
//!
//! PeerDiscoveryCenter 提供统一的 peer 发现接口，内部整合了多种发现机制：
//! - **Tracker**：通过 BitTorrent Tracker 协议（HTTP/UDP）发现 peer
//! - **DHT**：通过分布式哈希表（Kademlia）发现 peer
//! - **PEX**：通过 Peer Exchange 从已连接的 peer 发现更多 peer
//! - **LPD**：局域网多播发现（可选）
//! - **WebSeed**：HTTP/Web Seed（可选）
//!
//! 架构特性：
//! - **事件驱动**：所有模块通过 EventBus 解耦通信
//! - **控制面/数据面分离**：控制面做策略决策，数据面无状态处理请求
//! - **插件化发现器**：新增发现器只需实现 PeerDiscoverer trait 并注册
//! - **双引擎**：数据面被动查询 + 爬虫引擎主动爬行
//! - **超级 Tracker**：对 qBittorrent 暴露标准 /announce 和 /scrape 接口
//!
//! ## 快速开始（库模式）
//!
//! ```text
//! use PeerDiscoveryCenter::aggregator::{PeerDiscoveryAggregator, PeerDiscoveryConfig};
//! use PeerDiscoveryCenter::discoverers::DiscovererRegistry;
//! use PeerDiscoveryCenter::event_bus::EventBus;
//! use std::sync::Arc;
//!
//! // 1. 创建事件总线和发现器注册表
//! let bus = EventBus::default();
//! let registry = Arc::new(DiscovererRegistry::new());
//!
//! // 2. 注册发现器
//! registry.register(Box::new(
//!     PeerDiscoveryCenter::discoverers::tracker::TrackerDiscoverer::with_default_config()
//! ));
//!
//! // 3. 创建聚合器
//! let config = PeerDiscoveryConfig::default();
//! let aggregator = PeerDiscoveryAggregator::new(config, registry, bus);
//!
//! // 4. 发现 peer
//! let infohash = [0u8; 20];
//! let result = aggregator.discover_peers(&infohash, 100).await?;
//! ```
//!
//! ## 模块结构
//!
//! - [`aggregator`] - 核心聚合器，统一调度所有发现器
//! - [`config`] - 配置管理（YAML 文件 + 环境变量）
//! - [`control_plane`] - 控制面（策略管理、配置热更新、发现器生命周期）
//! - [`crawler`] - 爬虫引擎（主动爬行 DHT 网络）
//! - [`data_plane`] - 数据面（超级 Tracker HTTP 协议层 + REST API）
//! - [`discoverers`] - 发现器插件层（tracker/dht/pex/lpd/webseed）
//! - [`event_bus`] - 事件总线（发布/订阅解耦）
//! - [`health_check`] - 健康检查后台任务
//! - [`traits`] - 统一的 PeerDiscoverer trait 定义
//! - [`types`] - 公共数据结构（PeerInfo、PeerSource、Event、Tracker 协议类型等）

#![allow(non_snake_case)]

/// 进程启动时刻锚点（跨模块共享）。
///
/// 2026-10-09：`/health` 的 `uptime_seconds` 恒为 0 —— `rest_api.rs` 里是
/// 字面量 `uptime_seconds: 0`，全仓无任何赋值来源；而federation 的
/// `uptime_secs` 正常递增，因为联邦服务有 `self.started_at`，全局缺这个锚点。
/// rustc ≥ 1.80 稳定；本仓 edition 2021 + rustc 1.98 可用。
pub static STARTED_AT: std::sync::LazyLock<std::time::Instant> =
    std::sync::LazyLock::new(std::time::Instant::now);

/// 当前进程已运行秒数（供 health 等对外可观测字段使用）。
pub fn uptime_secs() -> u64 {
    STARTED_AT.elapsed().as_secs()
}

pub mod aggregator;
pub mod config;
pub mod control_plane;
pub mod crawler;
pub mod data_plane;
pub mod dht;
pub mod discoverers;
pub mod dns_pool;
pub mod dns_resolve;
pub mod event_bus;
pub mod federation;
pub mod firewall;
pub mod health_check;
pub mod intelligence;
pub mod nat;
pub mod net;
pub mod port_allocator;
pub mod services;
pub mod storage;
pub mod traits;
pub mod types;
pub mod utils;

// 重新导出常用类型
pub use aggregator::{AggregateStats, PeerDiscoveryAggregator, PeerDiscoveryConfig};
pub use config::PdcConfig;
pub use control_plane::ControlPlane;
pub use crawler::{Crawler, CrawlerEngine, CrawlerState};
pub use data_plane::{AppState, DataPlane};
pub use discoverers::DiscovererRegistry;
pub use event_bus::EventBus;
pub use traits::{AnnounceEvent, DiscovererStats, DiscovererType, PeerDiscoverer};
pub use types::{
    DiscoveryResult, Event, Infohash, PeerInfo, PeerSource, TrackerAnnounceRequest,
    TrackerAnnounceResponse, TrackerScrapeResponse,
};

/// 库版本（包含 git commit hash）
pub const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")");

/// 纯版本号（不含 git hash）
pub const VERSION_PURE: &str = env!("CARGO_PKG_VERSION");

/// Git commit hash
pub const GIT_HASH: &str = env!("GIT_HASH");

/// Git branch
pub const GIT_BRANCH: &str = env!("GIT_BRANCH");

/// 库名称
pub const NAME: &str = "PeerDiscoveryCenter";

/// 测试临时目录根（仅 `cfg(test)` 可用）。
///
/// 2026-10-08 迁移：本机安全策略（EDR/防勒索类）会拒绝「target 构建目录镜像进程」
/// 对 %TEMP% 根目录与 D:\test\ 等数据盘的直写（PermissionDenied code 5，项目
/// config.rs 已有先例注释），但对**构建目录内**（`target/`）的写入放行。故测试
/// 临时文件统一落在 `target/test-tmp/`（构建产物目录，与 config.rs 先例一致），
/// 各测试模块的临时目录都从它下面派生，不再使用 `std::env::temp_dir()`。
#[cfg(test)]
pub fn test_tmp_dir() -> std::path::PathBuf {
    let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/test-tmp");
    std::fs::create_dir_all(&base)
        .unwrap_or_else(|e| panic!("test_tmp_dir 创建失败 {}: {}", base.display(), e));
    base
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
        assert_eq!(NAME, "PeerDiscoveryCenter");
    }

    #[test]
    fn test_test_tmp_dir_writable() {
        let base = test_tmp_dir();
        let probe = base.join(format!("wprobe_{}", std::process::id()));
        std::fs::write(&probe, b"x").expect("测试临时目录应可写");
        let _ = std::fs::remove_file(&probe);
    }
}
