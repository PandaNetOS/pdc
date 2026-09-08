//! 统一冷热分层系统（TierSystem）
//!
//! 【架构原则】所有冷热判定统一收口到 intelligence 层。
//! 数据层（Repo）只负责存储，不做冷热判断；业务层通过 TierSystem 获取冷热信息。
//!
//! 【多维度判定】
//! - 最后活跃时间（40%）：最近活跃的更可能被再次访问
//! - 节点评分（30%）：高评分节点更可能被爬虫选择，应保留
//! - 访问频率（20%）：被频繁访问的应保留
//! - 数据重要性（10%）：预留扩展（如热门 infohash 的 peer）
//!
//! 【特殊规则】
//! - 高评分保底：评分 > 70 的节点，即使暂时不活跃也至少保留为温数据
//! - 低评分加速降级：评分 < 30 的节点，热阈值缩短（加速降级）
//! - 访问频率调整：高频访问节点热阈值延长，低频访问加速降级

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::RwLock;
use tracing::{debug, info};

use crate::intelligence::tier_manager::{DataTier, TierConfig, TierStats};
use crate::storage::repo_traits::NodeRepository;

/// 冷热分层系统（统一收口所有冷热判定）
pub struct TierSystem {
    config: TierConfig,
    /// 缓存的分层统计（避免每次全量计算）
    stats: RwLock<TierStats>,
    /// 最后一次全量检查时间
    last_full_check: RwLock<Option<Instant>>,
}

impl TierSystem {
    pub fn new(config: TierConfig) -> Self {
        Self {
            config,
            stats: RwLock::new(TierStats::default()),
            last_full_check: RwLock::new(None),
        }
    }

    /// 判断单个节点的冷热层级（多维度综合判定）
    pub fn classify_node(
        &self,
        last_active: Instant,
        score: f64,
        query_count: u64,
    ) -> DataTier {
        let elapsed = last_active.elapsed();

        // 1. 基础分层：根据最后活跃时间
        let base_tier = if elapsed.as_secs() < self.config.hot_threshold_secs {
            DataTier::Hot
        } else if elapsed.as_secs() < self.config.warm_threshold_secs {
            DataTier::Warm
        } else {
            DataTier::Cold
        };

        // 2. 评分调整
        // 高评分保底：评分 > 70 的节点，至少保留为温数据
        if score > 70.0 && base_tier == DataTier::Cold {
            return DataTier::Warm;
        }
        // 低评分加速降级：评分 < 30 的节点，热阈值缩短为 1/3
        if score < 30.0 && base_tier == DataTier::Hot {
            let shortened_threshold = self.config.hot_threshold_secs / 3;
            if elapsed.as_secs() >= shortened_threshold {
                return DataTier::Warm;
            }
        }

        // 3. 访问频率调整
        // 高频访问（>100次）：热阈值延长 1.5 倍
        if query_count > 100 && base_tier == DataTier::Warm {
            let extended_threshold = (self.config.hot_threshold_secs as f64 * 1.5) as u64;
            if elapsed.as_secs() < extended_threshold {
                return DataTier::Hot;
            }
        }
        // 低频访问（<5次）：加速降级
        if query_count < 5 && base_tier == DataTier::Hot {
            let shortened_threshold = self.config.hot_threshold_secs / 2;
            if elapsed.as_secs() >= shortened_threshold {
                return DataTier::Warm;
            }
        }

        base_tier
    }

    /// 判断节点是否需要持久化（热/温数据需要持久化，冷数据跳过）
    pub fn should_persist(&self, last_active: Instant, score: f64, query_count: u64) -> bool {
        match self.classify_node(last_active, score, query_count) {
            DataTier::Hot | DataTier::Warm => true,
            DataTier::Cold => false,
        }
    }

    /// 判断节点是否在内存中保留（热数据保留，温数据部分保留，冷数据可清理）
    pub fn should_keep_in_memory(&self, last_active: Instant, score: f64, query_count: u64) -> bool {
        match self.classify_node(last_active, score, query_count) {
            DataTier::Hot => true,
            DataTier::Warm => score > 50.0, // 温数据中评分 > 50 的保留
            DataTier::Cold => false,
        }
    }

    /// 全量检查 NodeRepo 中所有节点的冷热层级，返回统计
    pub async fn check_node_repo(&self, repo: &dyn NodeRepository) -> TierStats {
        let nodes = repo.all_nodes().await;
        let mut hot_count = 0;
        let mut warm_count = 0;
        let mut cold_count = 0;

        for node in &nodes {
            match self.classify_node(node.last_active, node.score, node.query_count) {
                DataTier::Hot => hot_count += 1,
                DataTier::Warm => warm_count += 1,
                DataTier::Cold => cold_count += 1,
            }
        }

        let stats = TierStats { hot_count, warm_count, cold_count };
        *self.stats.write() = stats.clone();
        *self.last_full_check.write() = Some(Instant::now());
        debug!("[tier_system] NodeRepo 分层: hot={}, warm={}, cold={}", hot_count, warm_count, cold_count);
        stats
    }

    /// 获取缓存的分层统计
    pub fn cached_stats(&self) -> TierStats {
        self.stats.read().clone()
    }

    /// 获取热节点地址列表（用于爬虫候选选择等）
    pub async fn get_hot_nodes(&self, repo: &dyn NodeRepository) -> Vec<SocketAddr> {
        let nodes = repo.all_nodes().await;
        nodes
            .into_iter()
            .filter(|n| {
                self.classify_node(n.last_active, n.score, n.query_count) == DataTier::Hot
            })
            .map(|n| n.addr)
            .collect()
    }

    /// 获取需要持久化的节点（热/温数据）
    pub async fn get_persistable_nodes(&self, repo: &dyn NodeRepository) -> Vec<SocketAddr> {
        let nodes = repo.all_nodes().await;
        nodes
            .into_iter()
            .filter(|n| self.should_persist(n.last_active, n.score, n.query_count))
            .map(|n| n.addr)
            .collect()
    }
}

/// 冷热分层系统 trait（供业务层依赖）
#[async_trait]
pub trait TierManageable: Send + Sync {
    async fn check_and_migrate(&self) -> TierStats;
    async fn tier_stats(&self) -> TierStats;
}

/// NodeRepo 冷热分层管理器（适配旧接口，内部委托给 TierSystem）
pub struct NodeTierManagerAdapter {
    system: Arc<TierSystem>,
    node_repo: Arc<dyn NodeRepository>,
}

impl NodeTierManagerAdapter {
    pub fn new(system: Arc<TierSystem>, node_repo: Arc<dyn NodeRepository>) -> Self {
        Self { system, node_repo }
    }
}

#[async_trait]
impl TierManageable for NodeTierManagerAdapter {
    async fn check_and_migrate(&self) -> TierStats {
        self.system.check_node_repo(self.node_repo.as_ref()).await
    }

    async fn tier_stats(&self) -> TierStats {
        self.system.cached_stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> TierConfig {
        TierConfig {
            hot_threshold_secs: 1800,
            warm_threshold_secs: 7200,
            max_hot_in_memory: 5000,
            check_interval_secs: 300,
        }
    }

    #[test]
    fn test_classify_hot() {
        let system = TierSystem::new(test_config());
        let last_active = Instant::now() - Duration::from_secs(100);
        let tier = system.classify_node(last_active, 50.0, 10);
        assert_eq!(tier, DataTier::Hot);
    }

    #[test]
    fn test_classify_warm() {
        let system = TierSystem::new(test_config());
        let last_active = Instant::now() - Duration::from_secs(3600);
        let tier = system.classify_node(last_active, 50.0, 10);
        assert_eq!(tier, DataTier::Warm);
    }

    #[test]
    fn test_classify_cold() {
        let system = TierSystem::new(test_config());
        let last_active = Instant::now() - Duration::from_secs(10000);
        let tier = system.classify_node(last_active, 50.0, 10);
        assert_eq!(tier, DataTier::Cold);
    }

    #[test]
    fn test_high_score_floor() {
        // 高评分节点即使冷也至少温
        let system = TierSystem::new(test_config());
        let last_active = Instant::now() - Duration::from_secs(10000);
        let tier = system.classify_node(last_active, 85.0, 10);
        assert_eq!(tier, DataTier::Warm);
    }

    #[test]
    fn test_low_score_accelerated_demotion() {
        // 低评分节点热阈值缩短
        let system = TierSystem::new(test_config());
        let last_active = Instant::now() - Duration::from_secs(800); // 正常是热(1800)，低评分缩短为600
        let tier = system.classify_node(last_active, 20.0, 10);
        assert_eq!(tier, DataTier::Warm);
    }

    #[test]
    fn test_should_persist() {
        let system = TierSystem::new(test_config());
        // 热数据需要持久化
        assert!(system.should_persist(Instant::now(), 50.0, 10));
        // 冷数据不需要持久化
        assert!(!system.should_persist(Instant::now() - Duration::from_secs(10000), 50.0, 10));
        // 高评分冷数据（温）需要持久化
        assert!(system.should_persist(Instant::now() - Duration::from_secs(10000), 85.0, 10));
    }
}
