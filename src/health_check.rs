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

use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::cache::PeerCache;
use crate::discoverers::DiscovererRegistry;

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
    cache: Arc<PeerCache>,
    config: HealthCheckConfig,
}

impl HealthCheckTask {
    /// 创建新的健康检查任务
    pub fn new(
        registry: Arc<DiscovererRegistry>,
        cache: Arc<PeerCache>,
        config: HealthCheckConfig,
    ) -> Self {
        Self {
            registry,
            cache,
            config,
        }
    }

    /// 创建默认配置的健康检查任务
    pub fn with_default_config(registry: Arc<DiscovererRegistry>, cache: Arc<PeerCache>) -> Self {
        Self::new(registry, cache, HealthCheckConfig::default())
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
        self.cache.cleanup_expired();
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_check_creation() {
        let registry = Arc::new(DiscovererRegistry::new());
        let cache = Arc::new(PeerCache::default());
        let task = HealthCheckTask::with_default_config(registry, cache);
        assert_eq!(task.config.interval, Duration::from_secs(300));
    }
}
