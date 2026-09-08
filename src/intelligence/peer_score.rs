//! PeerScorer — BT Peer 评分（5维度加权）
//!
//! 评分维度：来源可信度30% + TCP可达性30% + DHT支持20% + 存活时间10% + 多infohash共享10%

use async_trait::async_trait;

use crate::intelligence::scorer_traits::PeerScorer;
use crate::storage::repo_traits::PeerRepository;
use crate::types::{PeerInfo, PeerSource};

pub struct PeerScorerImpl;

impl PeerScorerImpl {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PeerScorerImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PeerScorer for PeerScorerImpl {
    async fn rescore_all(&self, repo: &dyn PeerRepository) {
        let peers = repo.all_peers().await;
        for peer in &peers {
            // 计算该 peer 出现在多少个 infohash 下
            let infohash_count = self.count_infohashes_for_peer(repo, &peer.addr).await;
            let score = self.calculate(peer, infohash_count);
            repo.update_score(&peer.addr, score).await;
        }
    }

    fn calculate(&self, peer: &PeerInfo, infohash_count: u32) -> f64 {
        // 1. 来源可信度 30%
        let source_score = match peer.source {
            PeerSource::Tracker => 100.0,
            PeerSource::SuperTracker => 90.0,
            PeerSource::Lpd => 85.0,
            PeerSource::WebSeed => 80.0,
            PeerSource::Dht => 70.0,
            PeerSource::Pex => 50.0,
            PeerSource::Manual => 40.0,
            PeerSource::Utp => 65.0,
        };

        // 2. TCP 可达性 30%
        let reachability = if peer.connection_attempts > 0 {
            peer.connection_successes as f64 / peer.connection_attempts as f64 * 100.0
        } else {
            50.0 // 未探测过给中性分
        };

        // 3. DHT 支持 20%（从 metadata 读取，默认不支持给30分）
        let supports_dht = peer
            .metadata
            .get("supports_dht")
            .map(|v| v == "true")
            .unwrap_or(false);
        let dht_score = if supports_dht { 100.0 } else { 30.0 };

        // 4. 存活时间 10%（≥24h满分）
        let uptime_secs = peer
            .first_seen
            .elapsed()
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let uptime_score = (uptime_secs.min(86400) as f64 / 86400.0) * 100.0;

        // 5. 多 infohash 共享 10%（≥3个满分）
        let shared_score = if infohash_count >= 3 {
            100.0
        } else {
            infohash_count as f64 / 3.0 * 100.0
        };

        source_score * 0.30 + reachability * 0.30 + dht_score * 0.20 + uptime_score * 0.10
            + shared_score * 0.10
    }
}

impl PeerScorerImpl {
    async fn count_infohashes_for_peer(
        &self,
        repo: &dyn PeerRepository,
        addr: &std::net::SocketAddr,
    ) -> u32 {
        repo.get_peer_infohash_count(addr).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn test_peer_score_tracker_high() {
        let scorer = PeerScorerImpl::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6881);
        let mut peer = PeerInfo::new(addr, PeerSource::Tracker);
        peer.connection_attempts = 10;
        peer.connection_successes = 9;
        peer.metadata.insert("supports_dht".to_string(), "true".to_string());

        let score = scorer.calculate(&peer, 3);
        // 来源30 + 可达27 + DHT20 + 存活~0 + 共享10 = ~87
        assert!(score > 70.0, "score should be high, got {}", score);
    }

    #[test]
    fn test_peer_score_pex_low() {
        let scorer = PeerScorerImpl::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6881);
        let peer = PeerInfo::new(addr, PeerSource::Pex);

        let score = scorer.calculate(&peer, 1);
        // 来源15 + 可达15 + DHT6 + 存活0 + 共享3.3 = ~39
        assert!(score < 60.0, "score should be low, got {}", score);
    }
}
