//! 发现器插件层
//!
//! 插件化发现器注册表，所有发现器（Tracker、DHT、PEX、LPD、WebSeed、自定义）
//! 都通过 DiscovererRegistry 注册和管理，聚合器从注册表获取发现器列表。
//!
//! 新增发现器只需实现 PeerDiscoverer trait 并注册到注册表，无需修改聚合器代码。

pub mod dht;
pub mod lpd;
pub mod pex;
pub mod tracker;
pub mod webseed;

use std::sync::Arc;

use parking_lot::RwLock;
use tracing::info;

use crate::traits::{DiscovererStats, PeerDiscoverer};
use crate::types::{Infohash, PeerInfo};

/// 发现器插件注册表
///
/// 线程安全，支持运行时动态添加/移除发现器。
pub struct DiscovererRegistry {
    discoverers: RwLock<Vec<Arc<dyn PeerDiscoverer>>>,
}

impl DiscovererRegistry {
    /// 创建空的注册表
    pub fn new() -> Self {
        Self {
            discoverers: RwLock::new(vec![]),
        }
    }

    /// 注册发现器
    pub fn register(&self, discoverer: Box<dyn PeerDiscoverer>) {
        let name = discoverer.name().to_string();
        let dtype = discoverer.discoverer_type();
        self.discoverers.write().push(Arc::from(discoverer));
        info!("[discoverers] 注册发现器: {} (类型: {:?})", name, dtype);
    }

    /// 移除发现器（按名称）
    pub fn unregister(&self, name: &str) {
        let mut discoverers = self.discoverers.write();
        discoverers.retain(|d| d.name() != name);
        info!("[discoverers] 移除发现器: {}", name);
    }

    /// 获取所有发现器
    pub fn all(&self) -> Vec<Arc<dyn PeerDiscoverer>> {
        self.discoverers.read().clone()
    }

    /// 获取启用的发现器
    pub fn enabled(&self) -> Vec<Arc<dyn PeerDiscoverer>> {
        self.discoverers
            .read()
            .iter()
            .filter(|d| d.is_enabled())
            .cloned()
            .collect()
    }

    /// 按名称查找发现器
    pub fn get(&self, name: &str) -> Option<Arc<dyn PeerDiscoverer>> {
        self.discoverers
            .read()
            .iter()
            .find(|d| d.name() == name)
            .cloned()
    }

    /// 发现器数量
    pub fn len(&self) -> usize {
        self.discoverers.read().len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.discoverers.read().is_empty()
    }

    /// 聚合所有发现器的统计
    pub fn aggregate_stats(&self) -> Vec<(String, DiscovererStats)> {
        self.discoverers
            .read()
            .iter()
            .map(|d| (d.name().to_string(), d.stats()))
            .collect()
    }

    /// 并发调用所有启用的发现器
    ///
    /// # 参数
    /// - `infohash`: 要查询的 infohash
    /// - `limit`: 每个发现器返回的最大 peer 数
    /// - `max_concurrent`: 最大并发数
    /// - `timeout`: 单个发现器超时
    ///
    /// # 返回
    /// (发现器名称, 结果(peer列表或错误), 耗时) 的列表
    pub async fn discover_all(
        &self,
        infohash: &Infohash,
        limit: usize,
        max_concurrent: usize,
        timeout: std::time::Duration,
    ) -> Vec<(String, anyhow::Result<Vec<PeerInfo>>, std::time::Duration)> {
        let discoverers = self.enabled();
        let mut tasks = vec![];

        for discoverer in discoverers.into_iter().take(max_concurrent) {
            let infohash = *infohash;
            tasks.push(tokio::spawn(async move {
                let name = discoverer.name().to_string();
                let start = std::time::Instant::now();
                let result =
                    tokio::time::timeout(timeout, discoverer.discover_peers(&infohash, limit))
                        .await;
                let duration = start.elapsed();
                match result {
                    Ok(Ok(peers)) => (name, Ok(peers), duration),
                    Ok(Err(e)) => (name, Err(e), duration),
                    Err(_) => (name, Err(anyhow::anyhow!("timeout")), duration),
                }
            }));
        }

        let mut results = vec![];
        for task in tasks {
            if let Ok(r) = task.await {
                results.push(r);
            }
        }
        results
    }
}

impl Default for DiscovererRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::AnnounceEvent;
    use crate::types::PeerSource;
    use async_trait::async_trait;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    struct MockDiscoverer {
        name: String,
        enabled: bool,
        peers: Vec<PeerInfo>,
    }

    #[async_trait]
    impl PeerDiscoverer for MockDiscoverer {
        fn name(&self) -> &str {
            &self.name
        }
        fn discoverer_type(&self) -> crate::traits::DiscovererType {
            crate::traits::DiscovererType::Custom
        }
        fn is_enabled(&self) -> bool {
            self.enabled
        }
        async fn discover_peers(
            &self,
            _infohash: &Infohash,
            _limit: usize,
        ) -> anyhow::Result<Vec<PeerInfo>> {
            Ok(self.peers.clone())
        }
        async fn announce(
            &self,
            _infohash: &Infohash,
            _port: u16,
            _event: AnnounceEvent,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn health_check(&self) -> bool {
            true
        }
        fn stats(&self) -> DiscovererStats {
            DiscovererStats::default()
        }
    }

    fn make_peer(port: u16) -> PeerInfo {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        PeerInfo::new(addr, PeerSource::Tracker)
    }

    #[test]
    fn test_register_and_get() {
        let registry = DiscovererRegistry::new();
        let d = MockDiscoverer {
            name: "mock".to_string(),
            enabled: true,
            peers: vec![make_peer(6881)],
        };
        registry.register(Box::new(d));
        assert_eq!(registry.len(), 1);
        assert!(registry.get("mock").is_some());
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn test_enabled_filter() {
        let registry = DiscovererRegistry::new();
        registry.register(Box::new(MockDiscoverer {
            name: "enabled".to_string(),
            enabled: true,
            peers: vec![],
        }));
        registry.register(Box::new(MockDiscoverer {
            name: "disabled".to_string(),
            enabled: false,
            peers: vec![],
        }));
        assert_eq!(registry.enabled().len(), 1);
    }

    #[test]
    fn test_unregister() {
        let registry = DiscovererRegistry::new();
        registry.register(Box::new(MockDiscoverer {
            name: "mock".to_string(),
            enabled: true,
            peers: vec![],
        }));
        registry.unregister("mock");
        assert_eq!(registry.len(), 0);
    }

    #[tokio::test]
    async fn test_discover_all() {
        let registry = DiscovererRegistry::new();
        registry.register(Box::new(MockDiscoverer {
            name: "d1".to_string(),
            enabled: true,
            peers: vec![make_peer(6881)],
        }));
        registry.register(Box::new(MockDiscoverer {
            name: "d2".to_string(),
            enabled: true,
            peers: vec![make_peer(6882)],
        }));

        let infohash = [0u8; 20];
        let results = registry
            .discover_all(&infohash, 10, 10, std::time::Duration::from_secs(5))
            .await;
        assert_eq!(results.len(), 2);
    }
}
