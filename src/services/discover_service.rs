//! DiscoverService — Peer 发现业务层

use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::discoverers::DiscovererRegistry;
use crate::storage::repo_traits::{InfohashRepository, PeerRepository};
use crate::storage::{InfohashRepoImpl, PeerRepoImpl};
use crate::types::{DiscoveryResult, Infohash, PeerInfo, PeerSource};

/// 单次主动发现：每源最多收集 peer 数
const DISCOVER_MAX_RESULTS_PER_SOURCE: usize = 50;
/// 单次主动发现：最大并发发现器数
const DISCOVER_MAX_CONCURRENCY: usize = 8;
/// 单次主动发现：超时
const DISCOVER_TIMEOUT: Duration = Duration::from_secs(10);

pub struct DiscoverService {
    registry: Arc<DiscovererRegistry>,
    peer_repo: Arc<PeerRepoImpl>,
    infohash_repo: Arc<InfohashRepoImpl>,
}

impl DiscoverService {
    pub fn new(
        registry: Arc<DiscovererRegistry>,
        peer_repo: Arc<PeerRepoImpl>,
        infohash_repo: Arc<InfohashRepoImpl>,
    ) -> Self {
        Self {
            registry,
            peer_repo,
            infohash_repo,
        }
    }

    pub async fn discover(&self, infohash: Infohash) -> anyhow::Result<DiscoveryResult> {
        let raw = self
            .registry
            .discover_all(
                &infohash,
                DISCOVER_MAX_RESULTS_PER_SOURCE,
                DISCOVER_MAX_CONCURRENCY,
                DISCOVER_TIMEOUT,
            )
            .await;

        let mut all_peers: Vec<PeerInfo> = Vec::new();
        let mut source_stats: FxHashMap<PeerSource, usize> = FxHashMap::default();
        let mut discoverer_durations: FxHashMap<String, Duration> = FxHashMap::default();
        let start = std::time::Instant::now();

        for (name, result, duration) in raw {
            discoverer_durations.insert(name.clone(), duration);
            if let Ok(peers) = result {
                for peer in &peers {
                    *source_stats.entry(peer.source).or_insert(0) += 1;
                }
                all_peers.extend(peers);
            }
        }

        // 去重
        all_peers.sort_by_key(|a| a.addr);
        all_peers.dedup_by(|a, b| a.addr == b.addr);

        let result = DiscoveryResult {
            peers: all_peers.clone(),
            source_stats,
            total_duration: start.elapsed(),
            discoverer_durations,
        };

        // 写入 PeerRepo（用完全限定语法调用 async trait 方法，避免与同步 inherent 方法冲突）
        PeerRepository::add_peers(&*self.peer_repo, infohash, all_peers).await;

        // 注册 Infohash
        self.infohash_repo.register(infohash, "discover").await;

        Ok(result)
    }

    pub fn registry(&self) -> &Arc<DiscovererRegistry> {
        &self.registry
    }

    pub fn peer_repo(&self) -> &Arc<PeerRepoImpl> {
        &self.peer_repo
    }
}
