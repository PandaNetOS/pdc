//! 调度策略
//!
//! 控制面的调度策略，决定每次发现请求使用哪些发现器、并发数、超时等。
//!
//! 策略可以根据 infohash、历史成功率、网络状况动态调整。

use std::time::Duration;

use crate::config::PdcConfig;
use crate::traits::DiscovererType;

/// 发现调度策略
#[derive(Debug, Clone)]
pub struct DiscoveryPolicy {
    /// 最大并发发现器数
    pub max_concurrent: usize,
    /// 单次发现超时
    pub timeout: Duration,
    /// 每次发现返回的最大 peer 数
    pub max_peers: usize,
    /// 启用的发现器类型
    pub enabled_types: Vec<DiscovererType>,
    /// 是否优先使用缓存
    pub prefer_cache: bool,
    /// 缓存命中阈值（缓存 peer 数超过此值时不触发后端发现）
    pub cache_hit_threshold: usize,
}

impl DiscoveryPolicy {
    /// 从配置创建策略
    pub fn from_config(config: &PdcConfig) -> Self {
        let mut enabled_types = vec![];
        if config.discoverers.enable_tracker {
            enabled_types.push(DiscovererType::Tracker);
        }
        if config.discoverers.enable_dht {
            enabled_types.push(DiscovererType::Dht);
        }
        if config.discoverers.enable_pex {
            enabled_types.push(DiscovererType::Pex);
        }
        if config.discoverers.enable_lpd {
            enabled_types.push(DiscovererType::Lpd);
        }
        if config.discoverers.enable_webseed {
            enabled_types.push(DiscovererType::WebSeed);
        }

        Self {
            max_concurrent: config.discoverers.max_concurrent,
            timeout: Duration::from_secs(config.discoverers.discovery_timeout_secs),
            max_peers: config.cache.max_peers_per_discovery,
            enabled_types,
            prefer_cache: true,
            cache_hit_threshold: 20,
        }
    }

    /// 默认策略
    pub fn default_policy() -> Self {
        Self {
            max_concurrent: 10,
            timeout: Duration::from_secs(30),
            max_peers: 200,
            enabled_types: vec![
                DiscovererType::Tracker,
                DiscovererType::Dht,
                DiscovererType::Pex,
            ],
            prefer_cache: true,
            cache_hit_threshold: 20,
        }
    }

    /// 判断某个发现器类型是否启用
    pub fn is_type_enabled(&self, dtype: DiscovererType) -> bool {
        self.enabled_types.contains(&dtype)
    }

    /// 根据缓存数量决定是否需要后端发现
    ///
    /// 返回 true 表示需要触发后端发现器，false 表示缓存足够直接返回。
    pub fn should_trigger_discovery(&self, cached_count: usize) -> bool {
        if !self.prefer_cache {
            return true;
        }
        cached_count < self.cache_hit_threshold
    }
}

impl Default for DiscoveryPolicy {
    fn default() -> Self {
        Self::default_policy()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_policy() {
        let policy = DiscoveryPolicy::default();
        assert_eq!(policy.max_concurrent, 10);
        assert!(policy.is_type_enabled(DiscovererType::Tracker));
        assert!(policy.is_type_enabled(DiscovererType::Dht));
        assert!(policy.is_type_enabled(DiscovererType::Pex));
        assert!(!policy.is_type_enabled(DiscovererType::Lpd));
    }

    #[test]
    fn test_should_trigger_discovery() {
        let policy = DiscoveryPolicy::default();
        // 缓存不足，需要触发
        assert!(policy.should_trigger_discovery(10));
        // 缓存足够，不触发
        assert!(!policy.should_trigger_discovery(30));
    }

    #[test]
    fn test_from_config() {
        let mut config = PdcConfig::default();
        config.discoverers.enable_dht = false;
        config.discoverers.enable_lpd = true;
        let policy = DiscoveryPolicy::from_config(&config);
        assert!(policy.is_type_enabled(DiscovererType::Tracker));
        assert!(!policy.is_type_enabled(DiscovererType::Dht));
        assert!(policy.is_type_enabled(DiscovererType::Lpd));
    }
}
