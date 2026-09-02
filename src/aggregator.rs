//! Peer 发现聚合器
//!
//! 统一管理所有 peer 发现机制（Tracker、DHT、PEX），
//! 并发调用，合并结果，去重，排序。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::cache::PeerCache;
use crate::traits::{AnnounceEvent, DiscovererStats, PeerDiscoverer};
use crate::types::{DiscoveryResult, Infohash, PeerInfo, PeerSource};

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
    /// 是否启用 Tracker
    pub enable_tracker: bool,
    /// 是否启用 DHT
    pub enable_dht: bool,
    /// 是否启用 PEX
    pub enable_pex: bool,
    /// 每次发现的最大 peer 数
    pub max_peers_per_discovery: usize,
}

impl Default for PeerDiscoveryConfig {
    fn default() -> Self {
        Self {
            max_cached_peers: 10000,
            peer_ttl: Duration::from_secs(86400), // 24 小时
            discovery_timeout: Duration::from_secs(30),
            max_concurrent_discoverers: 10,
            enable_tracker: true,
            enable_dht: true,
            enable_pex: true,
            max_peers_per_discovery: 200,
        }
    }
}

/// Peer 发现聚合器
///
/// 统一管理所有 peer 发现机制，对外提供统一接口。
///
/// # 示例
/// ```text
/// use PeerDiscoveryCenter::aggregator::{PeerDiscoveryAggregator, PeerDiscoveryConfig};
///
/// // 1. 创建聚合器
/// let config = PeerDiscoveryConfig::default();
/// let aggregator = PeerDiscoveryAggregator::new(config);
///
/// // 2. 添加发现器（Tracker、DHT、PEX）
/// // aggregator.add_discoverer(Box::new(tracker_discoverer));
///
/// // 3. 发现 peer
/// let infohash = [0u8; 20];
/// let result = aggregator.discover_peers(&infohash, 100).await?;
///
/// // 4. 处理结果（result.peers 包含发现的 peer 列表）
/// ```
pub struct PeerDiscoveryAggregator {
    /// 所有发现器
    discoverers: RwLock<Vec<Arc<dyn PeerDiscoverer>>>,
    /// Peer 缓存
    cache: Arc<PeerCache>,
    /// 全局配置
    config: PeerDiscoveryConfig,
}

impl PeerDiscoveryAggregator {
    /// 创建新的聚合器
    pub fn new(config: PeerDiscoveryConfig) -> Self {
        let cache = Arc::new(PeerCache::new(config.max_cached_peers, config.peer_ttl));
        Self {
            discoverers: RwLock::new(vec![]),
            cache,
            config,
        }
    }

    /// 添加发现器
    pub fn add_discoverer(&self, discoverer: Box<dyn PeerDiscoverer>) {
        let mut discoverers = self.discoverers.write();
        info!(
            "[peer_discovery] 添加发现器: {} (类型: {:?})",
            discoverer.name(),
            discoverer.discoverer_type()
        );
        discoverers.push(Arc::from(discoverer));
    }

    /// 移除发现器
    pub fn remove_discoverer(&self, name: &str) {
        let mut discoverers = self.discoverers.write();
        discoverers.retain(|d| d.name() != name);
        info!("[peer_discovery] 移除发现器: {}", name);
    }

    /// 获取所有发现器
    pub fn discoverers(&self) -> Vec<Arc<dyn PeerDiscoverer>> {
        self.discoverers.read().clone()
    }

    /// 获取发现器数量
    pub fn discoverer_count(&self) -> usize {
        self.discoverers.read().len()
    }

    /// 发现 peer（核心方法）
    ///
    /// 并发调用所有启用的发现器，合并结果，去重，排序，缓存。
    pub async fn discover_peers(
        &self,
        infohash: &Infohash,
        limit: usize,
    ) -> anyhow::Result<DiscoveryResult> {
        let start = Instant::now();
        let discoverers = self.discoverers.read().clone();

        // 1. 先从缓存获取
        let cached_peers = self.cache.get_peers(infohash, limit);
        if !cached_peers.is_empty() {
            debug!("[peer_discovery] 缓存命中 {} 个 peer", cached_peers.len());
        }

        // 2. 过滤启用的发现器
        let active_discoverers: Vec<Arc<dyn PeerDiscoverer>> = discoverers
            .into_iter()
            .filter(|d| {
                if !d.is_enabled() {
                    return false;
                }
                match d.discoverer_type() {
                    crate::traits::DiscovererType::Tracker => self.config.enable_tracker,
                    crate::traits::DiscovererType::Dht => self.config.enable_dht,
                    crate::traits::DiscovererType::Pex => self.config.enable_pex,
                }
            })
            .collect();

        if active_discoverers.is_empty() {
            warn!("[peer_discovery] 没有启用的发现器，仅返回缓存结果");
            return Ok(DiscoveryResult {
                peers: cached_peers,
                source_stats: HashMap::new(),
                total_duration: start.elapsed(),
                discoverer_durations: HashMap::new(),
            });
        }

        // 3. 并发调用所有发现器
        let mut tasks = vec![];
        for discoverer in active_discoverers
            .iter()
            .take(self.config.max_concurrent_discoverers)
        {
            let discoverer = discoverer.clone();
            let infohash = *infohash;
            let timeout = self.config.discovery_timeout;

            tasks.push(tokio::spawn(async move {
                let name = discoverer.name().to_string();
                let start = Instant::now();

                let result =
                    tokio::time::timeout(timeout, discoverer.discover_peers(&infohash, limit))
                        .await;

                let duration = start.elapsed();

                match result {
                    Ok(Ok(peers)) => {
                        debug!(
                            "[peer_discovery] {} 返回 {} 个 peer ({:?})",
                            name,
                            peers.len(),
                            duration
                        );
                        (name, Ok(peers), duration)
                    }
                    Ok(Err(e)) => {
                        warn!("[peer_discovery] {} 失败: {} ({:?})", name, e, duration);
                        (name, Err(e), duration)
                    }
                    Err(_) => {
                        warn!("[peer_discovery] {} 超时 ({:?})", name, duration);
                        (name, Err(anyhow::anyhow!("timeout")), duration)
                    }
                }
            }));
        }

        // 4. 等待所有任务完成，合并结果
        let mut all_peers: Vec<PeerInfo> = cached_peers;
        let mut source_stats: HashMap<PeerSource, usize> = HashMap::new();
        let mut discoverer_durations: HashMap<String, Duration> = HashMap::new();

        for task in tasks {
            if let Ok((name, result, duration)) = task.await {
                discoverer_durations.insert(name, duration);
                if let Ok(peers) = result {
                    for peer in peers {
                        *source_stats.entry(peer.source).or_insert(0) += 1;
                        all_peers.push(peer);
                    }
                }
            }
        }

        // 5. 去重（按 IP:端口）
        all_peers.sort_by_key(|a| a.addr);
        all_peers.dedup_by(|a, b| a.addr == b.addr);

        // 6. 计算优先级并排序
        for peer in all_peers.iter_mut() {
            peer.calculate_priority();
        }
        all_peers.sort_by_key(|a| std::cmp::Reverse(a.priority_score));

        // 7. 限制数量
        if all_peers.len() > limit {
            all_peers.truncate(limit);
        }

        // 8. 更新缓存
        self.cache.add_peers(infohash, &all_peers);

        let total_duration = start.elapsed();

        info!(
            "[peer_discovery] 发现完成: {} 个 peer (tracker={}, dht={}, pex={}), 耗时 {:?}",
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
        let discoverers = self.discoverers.read().clone();
        let mut tasks = vec![];

        for discoverer in discoverers.iter() {
            if !discoverer.is_enabled() {
                continue;
            }
            let discoverer = discoverer.clone();
            let infohash = *infohash;
            tasks.push(tokio::spawn(async move {
                if let Err(e) = discoverer.announce(&infohash, port, event).await {
                    warn!(
                        "[peer_discovery] {} announce 失败: {}",
                        discoverer.name(),
                        e
                    );
                }
            }));
        }

        for task in tasks {
            let _ = task.await;
        }
    }

    /// 获取缓存引用
    pub fn cache(&self) -> Arc<PeerCache> {
        self.cache.clone()
    }

    /// 获取配置引用
    pub fn config(&self) -> &PeerDiscoveryConfig {
        &self.config
    }

    /// 获取聚合统计
    pub fn aggregate_stats(&self) -> AggregateStats {
        let discoverers = self.discoverers.read();
        let mut stats = AggregateStats::default();

        for d in discoverers.iter() {
            let s = d.stats();
            stats.total_requests += s.total_requests;
            stats.success_requests += s.success_requests;
            stats.failed_requests += s.failed_requests;
            stats.total_peers_discovered += s.total_peers_discovered;
            stats.discoverer_stats.insert(d.name().to_string(), s);
        }

        stats.cached_peers = self.cache.len();
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
    pub discoverer_stats: HashMap<String, DiscovererStats>,
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
        fn discoverer_type(&self) -> crate::traits::DiscovererType {
            crate::traits::DiscovererType::Tracker
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
        let config = PeerDiscoveryConfig::default();
        let aggregator = PeerDiscoveryAggregator::new(config);

        let discoverer = MockDiscoverer {
            name: "mock".to_string(),
            peers: vec![make_peer(6881), make_peer(6882)],
        };
        aggregator.add_discoverer(Box::new(discoverer));

        let infohash = [0u8; 20];
        let result = aggregator.discover_peers(&infohash, 100).await.unwrap();

        assert_eq!(result.peers.len(), 2);
        assert_eq!(aggregator.cache().len_for_infohash(&infohash), 2);
    }

    #[tokio::test]
    async fn test_dedup_across_discoverers() {
        let config = PeerDiscoveryConfig::default();
        let aggregator = PeerDiscoveryAggregator::new(config);

        let d1 = MockDiscoverer {
            name: "d1".to_string(),
            peers: vec![make_peer(6881)],
        };
        let d2 = MockDiscoverer {
            name: "d2".to_string(),
            peers: vec![make_peer(6881)], // 相同地址
        };

        aggregator.add_discoverer(Box::new(d1));
        aggregator.add_discoverer(Box::new(d2));

        let infohash = [0u8; 20];
        let result = aggregator.discover_peers(&infohash, 100).await.unwrap();

        assert_eq!(result.peers.len(), 1); // 去重后只有 1 个
    }
}
