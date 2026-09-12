//! 冷热分层管理器
//!
//! 三层定义：
//! - Hot（热）：最近有活跃/交互，访问频率高，保留在内存
//! - Warm（温）：有活跃但频率不高，可从内存降级
//! - Cold（冷）：长时间无活跃，访问频率低，可从内存清理（SQLite 中保留）
//!
//! 迁移机制：
//! - 升温：冷/温数据有新交互时立即升温到热
//! - 降温：热数据超阈值无交互→温，温数据超阈值→冷
//! - 定期检查：每 5 分钟检查一次温度

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::storage::db::Storage;
use crate::storage::repo_traits::{NodeRepository, PeerRepository};
use crate::types::Infohash;

/// 数据温度层级
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DataTier {
    /// 热数据：最近有活跃，保留在内存
    Hot,
    /// 温数据：有活跃但频率不高
    Warm,
    /// 冷数据：长时间无活跃，可从内存清理
    Cold,
}

impl DataTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            DataTier::Hot => "hot",
            DataTier::Warm => "warm",
            DataTier::Cold => "cold",
        }
    }
}

/// 冷热分层配置
#[derive(Debug, Clone)]
pub struct TierConfig {
    /// 热数据阈值：最近 N 秒内有活跃视为热
    pub hot_threshold_secs: u64,
    /// 温数据阈值：最近 N 秒内有活跃视为温，超过则为冷
    pub warm_threshold_secs: u64,
    /// 内存热数据上限（超过时评分最低的降级到温）
    pub max_hot_in_memory: usize,
    /// 检查间隔
    pub check_interval_secs: u64,
}

impl Default for TierConfig {
    fn default() -> Self {
        Self {
            hot_threshold_secs: 1800,    // 30 分钟内有活跃视为热
            warm_threshold_secs: 7200,   // 2 小时内有活跃视为温
            max_hot_in_memory: 5000,      // 内存最多保留 5000 热数据
            check_interval_secs: 300,      // 每 5 分钟检查一次
        }
    }
}

/// 分层统计
#[derive(Debug, Clone, Default)]
pub struct TierStats {
    pub hot_count: usize,
    pub warm_count: usize,
    pub cold_count: usize,
}

/// 冷热分层管理器 trait
#[async_trait]
pub trait TierManageable: Send + Sync {
    /// 检查并执行温度迁移，返回迁移统计
    async fn check_and_migrate(&self) -> TierStats;
    /// 获取当前分层统计
    async fn tier_stats(&self) -> TierStats;
}

/// PeerRepo 冷热分层管理器
pub struct PeerTierManager {
    peer_repo: Arc<dyn PeerRepository>,
    storage: Option<Arc<Storage>>,
    config: TierConfig,
    stats: RwLock<TierStats>,
}

impl PeerTierManager {
    pub fn new(peer_repo: Arc<dyn PeerRepository>, config: TierConfig) -> Self {
        Self {
            peer_repo,
            storage: None,
            config,
            stats: RwLock::new(TierStats::default()),
        }
    }

    pub fn with_storage(mut self, storage: Arc<Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// 判断 peer 的温度层级
    fn classify_peer(last_active: SystemTime, config: &TierConfig) -> DataTier {
        let elapsed = last_active.elapsed().unwrap_or(Duration::from_secs(u64::MAX));
        if elapsed.as_secs() < config.hot_threshold_secs {
            DataTier::Hot
        } else if elapsed.as_secs() < config.warm_threshold_secs {
            DataTier::Warm
        } else {
            DataTier::Cold
        }
    }
}

#[async_trait]
impl TierManageable for PeerTierManager {
    async fn check_and_migrate(&self) -> TierStats {
        let peers = self.peer_repo.all_peers().await;
        let now = SystemTime::now();

        let mut hot_count = 0;
        let mut warm_count = 0;
        let mut cold_count = 0;

        for peer in &peers {
            let elapsed = now.duration_since(peer.last_active).unwrap_or(Duration::from_secs(u64::MAX));
            if elapsed.as_secs() < self.config.hot_threshold_secs {
                hot_count += 1;
            } else if elapsed.as_secs() < self.config.warm_threshold_secs {
                warm_count += 1;
            } else {
                cold_count += 1;
            }
        }

        // 冷数据归档：超过 warm_threshold 的 peer 从主表迁移到归档表（减少主表体积）
        if cold_count > 0 {
            if let Some(ref storage) = self.storage {
                let storage = storage.clone();
                let threshold = self.config.warm_threshold_secs as i64;
                match tokio::task::spawn_blocking(move || storage.archive_cold_peers(threshold)).await {
                    Ok(Ok(count)) => debug!("[tier_manager] PeerRepo 冷数据归档: {} 个", count),
                    Ok(Err(e)) => warn!("[tier_manager] PeerRepo 冷数据归档失败: {}", e),
                    Err(e) => warn!("[tier_manager] PeerRepo 冷数据归档任务失败: {}", e),
                }
            }
        }


        let stats = TierStats { hot_count, warm_count, cold_count };
        *self.stats.write() = stats.clone();
        stats
    }

    async fn tier_stats(&self) -> TierStats {
        self.stats.read().clone()
    }
}

/// NodeRepo 冷热分层管理器
pub struct NodeTierManager {
    node_repo: Arc<dyn NodeRepository>,
    config: TierConfig,
    stats: RwLock<TierStats>,
}

impl NodeTierManager {
    pub fn new(node_repo: Arc<dyn NodeRepository>, config: TierConfig) -> Self {
        Self {
            node_repo,
            config,
            stats: RwLock::new(TierStats::default()),
        }
    }
}

#[async_trait]
impl TierManageable for NodeTierManager {
    async fn check_and_migrate(&self) -> TierStats {
        let nodes = self.node_repo.all_nodes().await;

        let mut hot_count = 0;
        let mut warm_count = 0;
        let mut cold_count = 0;

        for node in &nodes {
            let elapsed = node.last_active.elapsed();
            if elapsed.as_secs() < self.config.hot_threshold_secs {
                hot_count += 1;
            } else if elapsed.as_secs() < self.config.warm_threshold_secs {
                warm_count += 1;
            } else {
                cold_count += 1;
            }
        }

        let stats = TierStats { hot_count, warm_count, cold_count };
        *self.stats.write() = stats.clone();
        stats
    }

    async fn tier_stats(&self) -> TierStats {
        self.stats.read().clone()
    }
}

/// 冷热分层管理器（统一调度）
pub struct TierManager {
    peer_manager: Option<Arc<PeerTierManager>>,
    node_manager: Option<Arc<NodeTierManager>>,
    storage: Option<Arc<Storage>>,
    config: TierConfig,
}

impl TierManager {
    pub fn new(config: TierConfig) -> Self {
        Self {
            peer_manager: None,
            node_manager: None,
            storage: None,
            config,
        }
    }

    pub fn with_storage(mut self, storage: Arc<Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    pub fn with_peer_repo(mut self, peer_repo: Arc<dyn PeerRepository>) -> Self {
        let mut mgr = PeerTierManager::new(peer_repo, self.config.clone());
        if let Some(ref storage) = self.storage {
            mgr = mgr.with_storage(storage.clone());
        }
        self.peer_manager = Some(Arc::new(mgr));
        self
    }

    pub fn with_node_repo(mut self, node_repo: Arc<dyn NodeRepository>) -> Self {
        self.node_manager = Some(Arc::new(NodeTierManager::new(node_repo, self.config.clone())));
        self
    }

    /// 执行一次全量温度检查和迁移
    pub async fn check_all(&self) {
        if let Some(ref mgr) = self.peer_manager {
            let stats = mgr.check_and_migrate().await;
            debug!("[tier_manager] PeerRepo 分层: hot={}, warm={}, cold={}", stats.hot_count, stats.warm_count, stats.cold_count);
        }
        if let Some(ref mgr) = self.node_manager {
            let stats = mgr.check_and_migrate().await;
            debug!("[tier_manager] NodeRepo 分层: hot={}, warm={}, cold={}", stats.hot_count, stats.warm_count, stats.cold_count);
        }
    }

    /// 启动定期检查任务（已迁移到 TaskScheduler，此方法为空壳保留兼容）
    pub async fn run(self: Arc<Self>) {
        // 所有定时检查已注册到 TaskScheduler，不再自行 loop
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_tier_ordering() {
        assert!(DataTier::Hot < DataTier::Warm);
        assert!(DataTier::Warm < DataTier::Cold);
    }

    #[test]
    fn test_tier_config_default() {
        let config = TierConfig::default();
        assert_eq!(config.hot_threshold_secs, 1800);
        assert_eq!(config.warm_threshold_secs, 7200);
        assert_eq!(config.max_hot_in_memory, 5000);
        assert_eq!(config.check_interval_secs, 300);
    }
}
