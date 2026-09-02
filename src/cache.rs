//! Peer 缓存
//!
//! 按 infohash 分组缓存 peer，支持：
//! - 去重
//! - 优先级排序
//! - 过期自动清理
//! - LRU 淘汰

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use dashmap::DashMap;

use crate::types::{Infohash, PeerInfo};

/// Peer 缓存
///
/// 按 infohash 分组缓存 peer，线程安全。
#[allow(dead_code)]
pub struct PeerCache {
    /// infohash -> (addr -> PeerInfo)
    peers: Arc<DashMap<Infohash, HashMap<SocketAddr, PeerInfo>>>,
    /// 最大缓存 peer 数（全局）
    max_peers: usize,
    /// Peer 过期时间
    ttl: Duration,
    /// 每个 infohash 最大缓存数
    max_per_infohash: usize,
}

impl PeerCache {
    /// 创建新的 Peer 缓存
    pub fn new(max_peers: usize, ttl: Duration) -> Self {
        let max_per_infohash = (max_peers / 10).max(100);
        Self {
            peers: Arc::new(DashMap::new()),
            max_peers,
            ttl,
            max_per_infohash,
        }
    }

    /// 获取指定 infohash 的 peer（按优先级排序）
    pub fn get_peers(&self, infohash: &Infohash, limit: usize) -> Vec<PeerInfo> {
        let mut result: Vec<PeerInfo> = self
            .peers
            .get(infohash)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();

        // 过滤过期的
        result.retain(|p| !p.is_expired());

        // 按优先级排序
        result.sort_by_key(|a| std::cmp::Reverse(a.priority_score));

        if result.len() > limit {
            result.truncate(limit);
        }

        result
    }

    /// 添加 peer 到缓存
    pub fn add_peers(&self, infohash: &Infohash, new_peers: &[PeerInfo]) {
        let mut entry = self.peers.entry(*infohash).or_default();

        for peer in new_peers {
            // 如果已存在，合并统计信息
            if let Some(existing) = entry.get_mut(&peer.addr) {
                existing.last_active = SystemTime::now();
                existing.connection_attempts =
                    existing.connection_attempts.max(peer.connection_attempts);
                existing.connection_successes =
                    existing.connection_successes.max(peer.connection_successes);
                existing.calculate_priority();
            } else {
                entry.insert(peer.addr, peer.clone());
            }
        }

        // 清理过期的
        entry.retain(|_, p| !p.is_expired());

        // 如果超过限制，按优先级淘汰最低的
        if entry.len() > self.max_per_infohash {
            let mut sorted: Vec<_> = entry.values().cloned().collect();
            sorted.sort_by_key(|a| a.priority_score);
            let to_remove: Vec<SocketAddr> = sorted
                .iter()
                .take(sorted.len() - self.max_per_infohash)
                .map(|p| p.addr)
                .collect();
            for addr in to_remove {
                entry.remove(&addr);
            }
        }
    }

    /// 标记 peer 连接成功
    pub fn mark_connection_success(&self, infohash: &Infohash, addr: &SocketAddr) {
        if let Some(mut entry) = self.peers.get_mut(infohash) {
            if let Some(peer) = entry.get_mut(addr) {
                peer.connection_attempts += 1;
                peer.connection_successes += 1;
                peer.last_active = SystemTime::now();
                peer.calculate_priority();
            }
        }
    }

    /// 标记 peer 连接失败
    pub fn mark_connection_failure(&self, infohash: &Infohash, addr: &SocketAddr) {
        if let Some(mut entry) = self.peers.get_mut(infohash) {
            if let Some(peer) = entry.get_mut(addr) {
                peer.connection_attempts += 1;
                peer.calculate_priority();

                // 连续失败 5 次，移除
                if peer
                    .connection_attempts
                    .saturating_sub(peer.connection_successes)
                    >= 5
                {
                    entry.remove(addr);
                }
            }
        }
    }

    /// 清理所有过期的 peer
    pub fn cleanup_expired(&self) {
        self.peers.retain(|_, entry| {
            entry.retain(|_, p| !p.is_expired());
            !entry.is_empty()
        });
    }

    /// 获取缓存总数
    pub fn len(&self) -> usize {
        self.peers.iter().map(|m| m.len()).sum()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 获取指定 infohash 的缓存数
    pub fn len_for_infohash(&self, infohash: &Infohash) -> usize {
        self.peers.get(infohash).map(|m| m.len()).unwrap_or(0)
    }

    /// 获取所有 infohash
    pub fn infohashes(&self) -> Vec<Infohash> {
        self.peers.iter().map(|r| *r.key()).collect()
    }

    /// 清空缓存
    pub fn clear(&self) {
        self.peers.clear();
    }
}

impl Default for PeerCache {
    fn default() -> Self {
        Self::new(5000, Duration::from_secs(86400))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PeerSource;
    use std::net::{IpAddr, Ipv4Addr};

    fn make_peer(port: u16, source: PeerSource) -> PeerInfo {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        PeerInfo::new(addr, source)
    }

    #[test]
    fn test_add_and_get() {
        let cache = PeerCache::default();
        let infohash = [0u8; 20];

        let peers = vec![
            make_peer(6881, PeerSource::Tracker),
            make_peer(6882, PeerSource::Dht),
            make_peer(6883, PeerSource::Pex),
        ];

        cache.add_peers(&infohash, &peers);
        assert_eq!(cache.len_for_infohash(&infohash), 3);

        let result = cache.get_peers(&infohash, 10);
        assert_eq!(result.len(), 3);
        // Tracker 优先级最高，应该排第一
        assert_eq!(result[0].source, PeerSource::Tracker);
    }

    #[test]
    fn test_dedup() {
        let cache = PeerCache::default();
        let infohash = [0u8; 20];

        let peer1 = make_peer(6881, PeerSource::Tracker);
        let peer2 = make_peer(6881, PeerSource::Dht); // 相同地址

        cache.add_peers(&infohash, &[peer1, peer2]);
        assert_eq!(cache.len_for_infohash(&infohash), 1);
    }

    #[test]
    fn test_connection_feedback() {
        let cache = PeerCache::default();
        let infohash = [0u8; 20];
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6881);

        let peer = make_peer(6881, PeerSource::Tracker);
        cache.add_peers(&infohash, &[peer]);

        // 标记成功
        cache.mark_connection_success(&infohash, &addr);
        let result = cache.get_peers(&infohash, 10);
        assert_eq!(result[0].connection_successes, 1);
        assert_eq!(result[0].connection_attempts, 1);

        // 标记失败 5 次，应该被移除
        for _ in 0..5 {
            cache.mark_connection_failure(&infohash, &addr);
        }
        assert_eq!(cache.len_for_infohash(&infohash), 0);
    }
}
