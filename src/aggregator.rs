//! Peer 发现聚合器
//!
//! 统一管理所有 peer 发现机制，并发调用，合并结果，去重，排序，缓存。
//!
//! 终极形态改造：
//! - 发现器从 DiscovererRegistry 获取（插件化）
//! - 发现完成后发布 PeerDiscovered 事件到事件总线
//! - 保留原有的并发调度、缓存、去重、排序逻辑

use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::discoverers::DiscovererRegistry;
use crate::storage::PeerRepoImpl;
use crate::event_bus::EventBus;
use crate::traits::{AnnounceEvent, DiscovererStats, PeerDiscoverer};
use crate::types::{DiscoveryResult, Event, Infohash, PeerInfo, PeerSource};

/// Peer 发现配置
#[derive(Debug, Clone)]
pub struct PeerDiscoveryConfig {
    /// 最大缓存 peer 数
    pub max_cached_peers: usize,
    /// Peer 过期时间
    pub peer_ttl: Duration,
    /// 发现超时
    pub discovery_timeout: Duration,
    /// 并发发现器数量限制
    pub max_concurrent_discoverers: usize,
    /// 每次发现的最大 peer 数
    pub max_peers_per_discovery: usize,
}

impl Default for PeerDiscoveryConfig {
    fn default() -> Self {
        Self {
            max_cached_peers: 10000,
            peer_ttl: Duration::from_secs(86400),
            discovery_timeout: Duration::from_secs(30),
            max_concurrent_discoverers: 10,
            max_peers_per_discovery: 200,
        }
    }
}

/// Peer 发现聚合器
///
/// 统一管理所有 peer 发现机制，对外提供统一接口。
/// 发现器从 DiscovererRegistry 获取，发现结果发布到 EventBus。
pub struct PeerDiscoveryAggregator {
    /// 发现器注册表
    registry: Arc<DiscovererRegistry>,
    /// Peer 仓库（统一归口 PeerRepoImpl）
    peer_repo: Arc<PeerRepoImpl>,
    /// 事件总线
    event_bus: EventBus,
    /// 全局配置
    config: PeerDiscoveryConfig,
}

impl PeerDiscoveryAggregator {
    /// 创建新的聚合器
    pub fn new(
        config: PeerDiscoveryConfig,
        registry: Arc<DiscovererRegistry>,
        event_bus: EventBus,
    ) -> Self {
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        let peer_repo = Arc::new(PeerRepoImpl::new(storage));
        Self {
            registry,
            peer_repo,
            event_bus,
            config,
        }
    }

    /// 使用已有 Peer 仓库创建聚合器
    pub fn with_peer_repo(
        config: PeerDiscoveryConfig,
        registry: Arc<DiscovererRegistry>,
        event_bus: EventBus,
        peer_repo: Arc<PeerRepoImpl>,
    ) -> Self {
        Self {
            registry,
            peer_repo,
            event_bus,
            config,
        }
    }

    /// 获取所有发现器
    pub fn discoverers(&self) -> Vec<Arc<dyn PeerDiscoverer>> {
        self.registry.all()
    }

    /// 获取发现器数量
    pub fn discoverer_count(&self) -> usize {
        self.registry.len()
    }

    /// 发现 peer（核心方法）
    ///
    /// 并发调用所有启用的发现器，合并结果，去重，排序，缓存，发布事件。
    pub async fn discover_peers(
        &self,
        infohash: &Infohash,
        limit: usize,
    ) -> anyhow::Result<DiscoveryResult> {
        let start = Instant::now();

        // 1. 先从缓存获取
        let cached_peers = self.peer_repo.get_peers_sync(infohash, limit);
        if !cached_peers.is_empty() {
            debug!("[aggregator] 缓存命中 {} 个 peer", cached_peers.len());
        }

        // 2. 从注册表获取启用的发现器，并发调用
        let results = self
            .registry
            .discover_all(
                infohash,
                limit,
                self.config.max_concurrent_discoverers,
                self.config.discovery_timeout,
            )
            .await;

        if results.is_empty() && cached_peers.is_empty() {
            warn!("[aggregator] 没有启用的发现器，且缓存为空");
            return Ok(DiscoveryResult {
                peers: vec![],
                source_stats: FxHashMap::default(),
                total_duration: start.elapsed(),
                discoverer_durations: FxHashMap::default(),
            });
        }

        // 3. 合并结果
        let mut all_peers: Vec<PeerInfo> = cached_peers;
        let mut source_stats: FxHashMap<PeerSource, usize> = FxHashMap::default();
        let mut discoverer_durations: FxHashMap<String, Duration> = FxHashMap::default();

        for (name, result, duration) in results {
            discoverer_durations.insert(name.clone(), duration);
            match result {
                Ok(peers) => {
                    debug!(
                        "[aggregator] {} 返回 {} 个 peer ({:?})",
                        name,
                        peers.len(),
                        duration
                    );
                    for peer in peers {
                        *source_stats.entry(peer.source).or_insert(0) += 1;
                        all_peers.push(peer);
                    }
                }
                Err(e) => {
                    warn!("[aggregator] {} 失败: {} ({:?})", name, e, duration);
                }
            }
        }

        // 4. 去重（按 IP:端口）
        all_peers.sort_by_key(|a| a.addr);
        all_peers.dedup_by(|a, b| a.addr == b.addr);

        // 5. 计算优先级并排序
        for peer in all_peers.iter_mut() {
            peer.calculate_priority();
        }
        all_peers.sort_by(|a, b| b.priority_score.partial_cmp(&a.priority_score).unwrap_or(std::cmp::Ordering::Equal));

        // 6. 限制数量
        if all_peers.len() > limit {
            all_peers.truncate(limit);
        }

        // 7. 更新缓存
        self.peer_repo.add_peers_sync(infohash, &all_peers);

        let total_duration = start.elapsed();

        // 8. 发布发现事件
        if !all_peers.is_empty() {
            self.event_bus.publish(Event::PeerDiscovered {
                infohash: *infohash,
                peers: all_peers.clone(),
                source: "aggregator".to_string(),
            });
        }

        info!(
            "[aggregator] 发现完成: {} 个 peer (tracker={}, dht={}, pex={}), 耗时 {:?}",
            all_peers.len(),
            source_stats.get(&PeerSource::Tracker).unwrap_or(&0),
            source_stats.get(&PeerSource::Dht).unwrap_or(&0),
            source_stats.get(&PeerSource::Pex).unwrap_or(&0),
            total_duration
        );

        Ok(DiscoveryResult {
            peers: all_peers,
            source_stats,
            total_duration,
            discoverer_durations,
        })
    }

    /// 宣告自己正在下载/做种
    pub async fn announce(&self, infohash: &Infohash, port: u16, event: AnnounceEvent) {
        let discoverers = self.registry.enabled();
        let mut tasks = vec![];

        for discoverer in discoverers.iter() {
            let discoverer = discoverer.clone();
            let infohash = *infohash;
            tasks.push(tokio::spawn(async move {
                if let Err(e) = discoverer.announce(&infohash, port, event).await {
                    warn!("[aggregator] {} announce 失败: {}", discoverer.name(), e);
                }
            }));
        }

        for task in tasks {
            let _ = task.await;
        }
    }

    /// 获取 Peer 仓库引用
    pub fn peer_repo(&self) -> Arc<PeerRepoImpl> {
        self.peer_repo.clone()
    }

    /// 获取配置引用
    pub fn config(&self) -> &PeerDiscoveryConfig {
        &self.config
    }

    /// 获取发现器注册表引用
    pub fn registry(&self) -> Arc<DiscovererRegistry> {
        self.registry.clone()
    }

    /// 获取事件总线引用
    pub fn event_bus(&self) -> EventBus {
        self.event_bus.clone()
    }

    /// 获取聚合统计
    pub fn aggregate_stats(&self) -> AggregateStats {
        let raw_stats = self.registry.aggregate_stats();
        let mut stats = AggregateStats::default();

        for (name, s) in raw_stats {
            stats.total_requests += s.total_requests;
            stats.success_requests += s.success_requests;
            stats.failed_requests += s.failed_requests;
            stats.total_peers_discovered += s.total_peers_discovered;
            stats.discoverer_stats.insert(name, s);
        }

        stats.cached_peers = self.peer_repo.len();
        stats
    }
}

/// 聚合统计
#[derive(Debug, Clone, Default)]
pub struct AggregateStats {
    pub total_requests: u64,
    pub success_requests: u64,
    pub failed_requests: u64,
    pub total_peers_discovered: u64,
    pub cached_peers: usize,
    pub discoverer_stats: FxHashMap<String, DiscovererStats>,
}

impl AggregateStats {
    /// 成功率
    pub fn success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            return 0.0;
        }
        self.success_requests as f64 / self.total_requests as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::DiscovererType;
    use async_trait::async_trait;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    struct MockDiscoverer {
        name: String,
        peers: Vec<PeerInfo>,
    }

    #[async_trait]
    impl PeerDiscoverer for MockDiscoverer {
        fn name(&self) -> &str {
            &self.name
        }
        fn discoverer_type(&self) -> DiscovererType {
            DiscovererType::Tracker
        }
        fn is_enabled(&self) -> bool {
            true
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

    #[tokio::test]
    async fn test_discover_peers() {
        let registry = Arc::new(DiscovererRegistry::new());
        let bus = EventBus::default();
        let config = PeerDiscoveryConfig::default();
        let aggregator = PeerDiscoveryAggregator::new(config, registry.clone(), bus);

        let discoverer = MockDiscoverer {
            name: "mock".to_string(),
            peers: vec![make_peer(6881), make_peer(6882)],
        };
        registry.register(Box::new(discoverer));

        let infohash = [0u8; 20];
        let result = aggregator.discover_peers(&infohash, 100).await.unwrap();

        assert_eq!(result.peers.len(), 2);
        assert_eq!(aggregator.peer_repo().len_for_infohash(&infohash), 2);
    }

    #[tokio::test]
    async fn test_dedup_across_discoverers() {
        let registry = Arc::new(DiscovererRegistry::new());
        let bus = EventBus::default();
        let config = PeerDiscoveryConfig::default();
        let aggregator = PeerDiscoveryAggregator::new(config, registry.clone(), bus);

        let d1 = MockDiscoverer {
            name: "d1".to_string(),
            peers: vec![make_peer(6881)],
        };
        let d2 = MockDiscoverer {
            name: "d2".to_string(),
            peers: vec![make_peer(6881)], // 相同地址
        };

        registry.register(Box::new(d1));
        registry.register(Box::new(d2));

        let infohash = [0u8; 20];
        let result = aggregator.discover_peers(&infohash, 100).await.unwrap();

        assert_eq!(result.peers.len(), 1); // 去重后只有 1 个
    }

    #[tokio::test]
    async fn test_event_published() {
        let registry = Arc::new(DiscovererRegistry::new());
        let bus = EventBus::default();
        let mut rx = bus.subscribe();
        let config = PeerDiscoveryConfig::default();
        let aggregator = PeerDiscoveryAggregator::new(config, registry.clone(), bus);

        let discoverer = MockDiscoverer {
            name: "mock".to_string(),
            peers: vec![make_peer(6881)],
        };
        registry.register(Box::new(discoverer));

        let infohash = [0u8; 20];
        let _ = aggregator.discover_peers(&infohash, 100).await.unwrap();

        // 应该收到 PeerDiscovered 事件
        let event = rx.recv().await.unwrap();
        assert!(matches!(event, Event::PeerDiscovered { .. }));
    }
}
