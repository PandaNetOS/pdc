//! 健康检查后台任务
//!
//! 定期：
//! 1. 检查所有发现器健康状态
//! 2. 清理过期的 peer 缓存
//! 3. 输出统计信息
//!
//! 终极形态改造：从 DiscovererRegistry 获取发现器，不再依赖 aggregator。

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::discoverers::DiscovererRegistry;
use crate::intelligence::{HealthScorerImpl, HealthScorer};
use crate::storage::{InfohashRepoImpl, NodeRepoImpl, PeerRepoImpl, TrackerRepoImpl};
use crate::storage::repo_traits::{InfohashRepository, NodeRepository, PeerRepository, TrackerRepository};

/// 系统健康状态
#[derive(Debug, Clone, Serialize)]
pub struct SystemHealth {
    /// 综合健康分（0-100）
    pub overall_score: f64,
    /// 健康状态
    pub status: HealthStatus,
    /// Tracker 层健康分
    pub tracker_layer_score: f64,
    /// DHT 层健康分
    pub dht_layer_score: f64,
    /// Peer 层健康分
    pub peer_layer_score: f64,
    /// 活跃 tracker 数
    pub active_trackers: usize,
    /// 总 tracker 数
    pub total_trackers: usize,
    /// 平均 tracker 评分
    pub avg_tracker_score: f64,
}

/// 健康状态枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

impl HealthStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            HealthStatus::Healthy => "healthy",
            HealthStatus::Degraded => "degraded",
            HealthStatus::Unhealthy => "unhealthy",
        }
    }
}

impl SystemHealth {
    pub fn from_score(score: f64) -> HealthStatus {
        if score >= 80.0 {
            HealthStatus::Healthy
        } else if score >= 50.0 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        }
    }
}

/// 计算系统综合健康度（独立函数，供 API 和健康检查任务共用）
pub fn calculate_system_health(
    registry: &DiscovererRegistry,
    cache: &PeerRepoImpl,
    node_repo: Option<&crate::storage::NodeRepoImpl>,
) -> SystemHealth {
    let discoverers = registry.all();

    // Tracker 层健康度
    let (tracker_score, active_trackers, total_trackers, avg_tracker_score) = discoverers
        .iter()
        .find(|d| d.name() == "tracker")
        .and_then(|d| d.tracker_scores())
        .map(|scores| {
            let total = scores.len();
            let active = scores.iter().filter(|(_, _, disabled)| !disabled).count();
            let avg = if total > 0 {
                scores.iter().map(|(_, s, _)| s).sum::<f64>() / total as f64
            } else {
                0.0
            };
            let active_ratio = if total > 0 { active as f64 / total as f64 } else { 0.0 };
            let score = active_ratio * 50.0 + (avg / 100.0) * 50.0;
            (score, active, total, avg)
        })
        .unwrap_or((0.0, 0, 0, 0.0));

    // DHT 层健康度（基于 NodeRepo 实际数据，不再硬编码）
    let dht_healthy = if let Some(repo) = node_repo {
        let stats = repo.stats_sync();
        let total_nodes = stats.total;

        // 1. 节点池丰富度（35%）：≥10000 节点满分，线性插值
        let richness_score = if total_nodes >= 10000 {
            100.0
        } else {
            total_nodes as f64 / 10000.0 * 100.0
        };

        // 2. 节点平均质量（35%）：所有节点平均评分（0-100）
        let avg_node_score = stats.avg_score;

        // 3. 活跃节点比例（30%）：有查询记录的节点比例
        let active_ratio = if total_nodes > 0 {
            stats.active as f64 / total_nodes as f64
        } else {
            0.0
        };
        let activity_score = active_ratio * 100.0;

        richness_score * 0.35 + avg_node_score * 0.35 + activity_score * 0.30
    } else {
        // 没有 NodeRepo 时回退到旧逻辑：DHT 发现器存在给 50 分
        discoverers
            .iter()
            .find(|d| d.name() == "dht")
            .map(|_| 50.0)
            .unwrap_or(0.0)
    };

    // 缓存层健康度（infohash覆盖度40% + peer丰富度40% + 基础分20%）
    let cache_stats = cache.stats();
    let (cache_ih, cache_peers) = cache_stats;
    // infohash 覆盖度：>50 个 infohash 满分
    let ih_score = if cache_ih >= 50 { 100.0 } else { cache_ih as f64 * 2.0 };
    // peer 丰富度：>500 个 peer 满分
    let peer_score = if cache_peers >= 500 { 100.0 } else { cache_peers as f64 * 0.2 };
    // 基础分：缓存系统正常运行给 20 分
    let base_score = 20.0;
    let cache_score = ih_score * 0.4 + peer_score * 0.4 + base_score;

    let overall = tracker_score * 0.4 + dht_healthy * 0.3 + cache_score * 0.3;
    let status = SystemHealth::from_score(overall);

    SystemHealth {
        overall_score: overall,
        status,
        tracker_layer_score: tracker_score,
        dht_layer_score: dht_healthy,
        peer_layer_score: cache_score,
        active_trackers,
        total_trackers,
        avg_tracker_score,
    }
}

/// 健康检查配置
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// 检查间隔
    pub interval: Duration,
    /// 缓存清理间隔
    pub cache_cleanup_interval: Duration,
    /// 统计输出间隔
    pub stats_output_interval: Duration,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(300),
            cache_cleanup_interval: Duration::from_secs(600),
            stats_output_interval: Duration::from_secs(300),
        }
    }
}

/// 健康检查后台任务
pub struct HealthCheckTask {
    registry: Arc<DiscovererRegistry>,
    cache: Arc<PeerRepoImpl>,
    node_repo: Option<Arc<NodeRepoImpl>>,
    tracker_repo: Option<Arc<TrackerRepoImpl>>,
    infohash_repo: Option<Arc<InfohashRepoImpl>>,
    config: HealthCheckConfig,
    storage: Option<Arc<crate::storage::Storage>>,
    health_scorer: HealthScorerImpl,
}

impl HealthCheckTask {
    /// 创建新的健康检查任务
    pub fn new(
        registry: Arc<DiscovererRegistry>,
        cache: Arc<PeerRepoImpl>,
        node_repo: Option<Arc<NodeRepoImpl>>,
        tracker_repo: Option<Arc<TrackerRepoImpl>>,
        infohash_repo: Option<Arc<InfohashRepoImpl>>,
        config: HealthCheckConfig,
        storage: Option<Arc<crate::storage::Storage>>,
    ) -> Self {
        Self {
            registry,
            cache,
            node_repo,
            tracker_repo,
            infohash_repo,
            config,
            storage,
            health_scorer: HealthScorerImpl::new(),
        }
    }

    /// 创建默认配置的健康检查任务
    pub fn with_default_config(
        registry: Arc<DiscovererRegistry>,
        cache: Arc<PeerRepoImpl>,
        node_repo: Option<Arc<NodeRepoImpl>>,
        tracker_repo: Option<Arc<TrackerRepoImpl>>,
        infohash_repo: Option<Arc<InfohashRepoImpl>>,
    ) -> Self {
        Self::new(registry, cache, node_repo, tracker_repo, infohash_repo, HealthCheckConfig::default(), None)
    }

    /// 启动健康检查任务
    pub async fn run(self: Arc<Self>) {
        let mut main_ticker = interval(self.config.interval);
        let mut cache_ticker = interval(self.config.cache_cleanup_interval);
        let mut stats_ticker = interval(self.config.stats_output_interval);

        info!(
            "[health_check] 健康检查任务已启动，主间隔 {:?}, 缓存清理间隔 {:?}, 统计输出间隔 {:?}",
            self.config.interval,
            self.config.cache_cleanup_interval,
            self.config.stats_output_interval
        );

        loop {
            tokio::select! {
                _ = main_ticker.tick() => {
                    self.check_discoverers_health().await;
                }
                _ = cache_ticker.tick() => {
                    self.cleanup_expired_peers().await;
                }
                _ = stats_ticker.tick() => {
                    self.output_stats().await;
                }
            }
        }
    }

    /// 检查所有发现器健康状态
    async fn check_discoverers_health(&self) {
        let discoverers = self.registry.all();
        debug!(
            "[health_check] 开始健康检查，共 {} 个发现器",
            discoverers.len()
        );

        for discoverer in discoverers {
            let name = discoverer.name().to_string();
            match discoverer.health_check().await {
                true => {
                    debug!("[health_check] 发现器 {} 健康", name);
                }
                false => {
                    warn!("[health_check] 发现器 {} 健康检查失败", name);
                }
            }
        }
    }

    /// 清理过期的 peer 缓存
    async fn cleanup_expired_peers(&self) {
        let before = self.cache.len();
        self.cache.cleanup_expired_sync();
        let after = self.cache.len();

        if before != after {
            info!(
                "[health_check] 已清理过期 peer: {} -> {} (清理 {} 个)",
                before,
                after,
                before - after
            );
        } else {
            debug!(
                "[health_check] 缓存清理完成，无过期 peer，当前 {} 个",
                after
            );
        }
    }

    /// 计算系统综合健康度（使用 HealthScorer 统一入口）
    pub async fn calculate_health(&self) -> SystemHealth {
        // 如果四个 Repo 都可用，使用 HealthScorer 统一计算
        if let (Some(node_repo), Some(tracker_repo), Some(infohash_repo)) =
            (&self.node_repo, &self.tracker_repo, &self.infohash_repo)
        {
            let report = self.health_scorer.calculate(
                tracker_repo.as_ref() as &dyn TrackerRepository,
                node_repo.as_ref() as &dyn NodeRepository,
                self.cache.as_ref() as &dyn PeerRepository,
                infohash_repo.as_ref() as &dyn InfohashRepository,
            ).await;

            let status = SystemHealth::from_score(report.overall);
            return SystemHealth {
                overall_score: report.overall,
                status,
                tracker_layer_score: report.tracker_layer,
                dht_layer_score: report.dht_layer,
                peer_layer_score: report.peer_layer,
                active_trackers: report.active_trackers,
                total_trackers: report.total_trackers,
                avg_tracker_score: report.avg_tracker_score,
            };
        }

        // 回退：使用旧的 calculate_system_health（仅当 Repo 不可用时）
        calculate_system_health(&self.registry, &self.cache, self.node_repo.as_deref())
    }

    /// 输出统计信息
    async fn output_stats(&self) {
        let raw_stats = self.registry.aggregate_stats();
        let mut total_requests = 0u64;
        let mut success_requests = 0u64;
        let mut failed_requests = 0u64;
        let mut total_peers = 0u64;

        for (name, s) in &raw_stats {
            total_requests += s.total_requests;
            success_requests += s.success_requests;
            failed_requests += s.failed_requests;
            total_peers += s.total_peers_discovered;

            debug!(
                "[health_check]   {}: 请求={}, 成功={}, 失败={}, 成功率={:.2}%, 发现peer={}, 平均响应={:.1}ms",
                name,
                s.total_requests,
                s.success_requests,
                s.failed_requests,
                s.success_rate() * 100.0,
                s.total_peers_discovered,
                s.avg_response_time_ms
            );
        }

        let success_rate = if total_requests > 0 {
            success_requests as f64 / total_requests as f64 * 100.0
        } else {
            0.0
        };

        info!(
            "[health_check] 统计: 总请求={}, 成功={}, 失败={}, 成功率={:.2}%, 发现peer={}, 缓存={}",
            total_requests,
            success_requests,
            failed_requests,
            success_rate,
            total_peers,
            self.cache.len()
        );

        // 输出综合健康度
        let health = self.calculate_health().await;
        info!(
            "[health_check] 健康度: {:.1} ({}) | Tracker={:.1} ({}/{} active, avg={:.1}) | DHT={:.1} | Cache={:.1}",
            health.overall_score,
            health.status.as_str(),
            health.tracker_layer_score,
            health.active_trackers,
            health.total_trackers,
            health.avg_tracker_score,
            health.dht_layer_score,
            health.peer_layer_score
        );

        // 记录统计快照到 SQLite
        if let Some(storage) = &self.storage {
            let _ = storage.record_stats("total_requests", total_requests as f64);
            let _ = storage.record_stats("success_requests", success_requests as f64);
            let _ = storage.record_stats("failed_requests", failed_requests as f64);
            let _ = storage.record_stats("success_rate", success_rate);
            let _ = storage.record_stats("total_peers_discovered", total_peers as f64);
            let _ = storage.record_stats("cached_peers", self.cache.len() as f64);
            let _ = storage.record_stats("health_score", health.overall_score);
            let _ = storage.update_aggregate("total_requests_ever", total_requests as f64);
            debug!("[health_check] 统计快照已写入 SQLite");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_check_creation() {
        let registry = Arc::new(DiscovererRegistry::new());
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        let cache = Arc::new(PeerRepoImpl::new(storage));
        let task = HealthCheckTask::with_default_config(registry, cache, None, None, None);
        assert_eq!(task.config.interval, Duration::from_secs(300));
    }
}
