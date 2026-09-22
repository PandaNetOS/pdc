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
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use parking_lot::RwLock;
use tracing::{debug, warn};

use crate::storage::db::Storage;
use crate::storage::repo_traits::{NodeRepository, PeerRepository};
use crate::storage::write_queue::WriteQueue;

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
            hot_threshold_secs: 1800,  // 30 分钟内有活跃视为热
            warm_threshold_secs: 7200, // 2 小时内有活跃视为温
            max_hot_in_memory: 5000,   // 内存最多保留 5000 热数据
            check_interval_secs: 300,  // 每 5 分钟检查一次
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
    write_queue: Option<Arc<WriteQueue>>,
}

impl PeerTierManager {
    pub fn new(peer_repo: Arc<dyn PeerRepository>, config: TierConfig) -> Self {
        Self {
            peer_repo,
            storage: None,
            config,
            stats: RwLock::new(TierStats::default()),
            write_queue: None,
        }
    }

    pub fn with_storage(mut self, storage: Arc<Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// 注入写入队列（builder 模式）
    pub fn with_write_queue(mut self, wq: Arc<WriteQueue>) -> Self {
        self.write_queue = Some(wq);
        self
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
            let elapsed = now
                .duration_since(peer.last_active)
                .unwrap_or(Duration::from_secs(u64::MAX));
            if elapsed.as_secs() < self.config.hot_threshold_secs {
                hot_count += 1;
            } else if elapsed.as_secs() < self.config.warm_threshold_secs {
                warm_count += 1;
            } else {
                cold_count += 1;
            }
        }

        // 冷数据归档（peers → peers_archive）：已按「永久资产模式」移除，不再执行。
        //
        // 移除理由：
        // ① 归档是「物理删除 peers 行 + INSERT 到归档表」，既无软删墓碑也不写 oplog。
        //    对端 range/delta 反熵发现该 peer 缺失后会重新推送，peer 被插回主表 →
        //    下一轮又被归档，形成冷 peer 的周期性搬移（联邦内无意义的来回做功）。
        // ② `peers_archive` 本身计入 `peer_repo_total`（rest_api.rs 口径），归档对
        //    「总量」没有任何收益，只会让 total 出现搬移抖动，不利于以 total 验收收敛。
        // ③ health_check::cleanup_expired_peers 同样是「不删除 peer」的语义，保持一致。
        //
        // cold_count 仍照常统计，供分层观测使用。

        let stats = TierStats {
            hot_count,
            warm_count,
            cold_count,
        };
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
        let now = Instant::now();

        let mut hot_count = 0;
        let mut warm_count = 0;
        let mut cold_count = 0;

        for node in &nodes {
            // 用 now 与 last_active 比较后相减，而非 last_active.elapsed()：
            // last_active 为未来时间时 elapsed() 会 panic；此处显式比较可安全兜底为最大值。
            let elapsed = if now >= node.last_active {
                now - node.last_active
            } else {
                Duration::from_secs(u64::MAX)
            };
            if elapsed.as_secs() < self.config.hot_threshold_secs {
                hot_count += 1;
            } else if elapsed.as_secs() < self.config.warm_threshold_secs {
                warm_count += 1;
            } else {
                cold_count += 1;
            }
        }

        // 冷数据驱逐：超过 warm_threshold 的节点从内存移除（DB 中永久保留）
        if cold_count > 0 {
            let threshold = self.config.warm_threshold_secs;
            match self.node_repo.remove_cold_nodes(threshold).await {
                Ok(removed) => {
                    debug!(
                        "[tier_manager] NodeRepo 冷节点驱逐: 从内存移除 {} 个",
                        removed
                    );
                }
                Err(e) => warn!("[tier_manager] NodeRepo 冷节点驱逐失败: {}", e),
            }
        }

        // 按数量驱逐：超过 hot_max_count 时驱逐最久未活跃的节点
        let current = self.node_repo.len_sync();
        if current > self.config.max_hot_in_memory {
            let removed = self.node_repo.evict_by_count(self.config.max_hot_in_memory);
            if removed > 0 {
                tracing::info!(
                    "[tier_manager] NodeRepo 按数量驱逐: {} 个（{}→{}，上限 {}）",
                    removed,
                    current,
                    current - removed,
                    self.config.max_hot_in_memory
                );
            }
        }

        let stats = TierStats {
            hot_count,
            warm_count,
            cold_count,
        };
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
    write_queue: Option<Arc<WriteQueue>>,
}

impl TierManager {
    pub fn new(config: TierConfig) -> Self {
        Self {
            peer_manager: None,
            node_manager: None,
            storage: None,
            config,
            write_queue: None,
        }
    }

    pub fn with_storage(mut self, storage: Arc<Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// 注入写入队列（builder 模式）
    pub fn with_write_queue(mut self, wq: Arc<WriteQueue>) -> Self {
        self.write_queue = Some(wq);
        self
    }

    pub fn with_peer_repo(mut self, peer_repo: Arc<dyn PeerRepository>) -> Self {
        let mut mgr = PeerTierManager::new(peer_repo, self.config.clone());
        if let Some(ref storage) = self.storage {
            mgr = mgr.with_storage(storage.clone());
        }
        if let Some(ref wq) = self.write_queue {
            mgr = mgr.with_write_queue(wq.clone());
        }
        self.peer_manager = Some(Arc::new(mgr));
        self
    }

    pub fn with_node_repo(mut self, node_repo: Arc<dyn NodeRepository>) -> Self {
        self.node_manager = Some(Arc::new(NodeTierManager::new(
            node_repo,
            self.config.clone(),
        )));
        self
    }

    /// 执行一次全量温度检查和迁移
    pub async fn check_all(&self) {
        if let Some(ref mgr) = self.peer_manager {
            let stats = mgr.check_and_migrate().await;
            debug!(
                "[tier_manager] PeerRepo 分层: hot={}, warm={}, cold={}",
                stats.hot_count, stats.warm_count, stats.cold_count
            );
        }
        if let Some(ref mgr) = self.node_manager {
            let stats = mgr.check_and_migrate().await;
            debug!(
                "[tier_manager] NodeRepo 分层: hot={}, warm={}, cold={}",
                stats.hot_count, stats.warm_count, stats.cold_count
            );
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
