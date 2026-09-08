//! PeerRepository 实现
//!
//! 合并 PeerCache + PEX池 + Probe队列 + SuperTracker peers，作为 BT Peer 的唯一归口。
//! 内存缓存 + SQLite 历史双写。
//! 使用 parking_lot::RwLock（同步），与 NodeRepo 一致，便于替换原 PeerCache。

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::storage::db::Storage;
use crate::storage::repo_traits::PeerRepository;
use crate::types::{Infohash, PeerInfo, PeerSource};

/// 内存中的 peer 缓存（按 infohash 分组）
struct PeerMemoryStore {
    /// infohash -> peer addr 集合
    by_infohash: HashMap<Infohash, HashSet<SocketAddr>>,
    /// 全局 peer addr -> PeerInfo（跨 infohash 去重）
    global: HashMap<SocketAddr, PeerInfo>,
    /// peer addr -> 出现在哪些 infohash 下
    infohash_refs: HashMap<SocketAddr, HashSet<Infohash>>,
}

impl PeerMemoryStore {
    fn new() -> Self {
        Self {
            by_infohash: HashMap::new(),
            global: HashMap::new(),
            infohash_refs: HashMap::new(),
        }
    }
}

pub struct PeerRepoImpl {
    cache: RwLock<PeerMemoryStore>,
    storage: Arc<Storage>,
    /// peer_history 写入缓冲区（攒批写入，减少 fsync）
    history_buffer: RwLock<Vec<crate::storage::db::PeerHistoryEntry>>,
}

impl PeerRepoImpl {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            cache: RwLock::new(PeerMemoryStore::new()),
            storage,
            history_buffer: RwLock::new(Vec::new()),
        }
    }

    // ── 同步便捷方法（_sync 后缀，与 async trait 方法区分）──

    pub fn add_peers_sync(&self, infohash: &Infohash, new_peers: &[PeerInfo]) {
        let mut cache = self.cache.write();
        for peer in new_peers {
            let addr = peer.addr;
            if let Some(existing) = cache.global.get_mut(&addr) {
                existing.last_active = peer.last_active;
                if peer.source.base_score() > existing.source.base_score() {
                    existing.source = peer.source;
                }
                if peer.peer_id.is_some() {
                    existing.peer_id = peer.peer_id;
                }
            } else {
                cache.global.insert(addr, peer.clone());
            }
            cache.by_infohash.entry(*infohash).or_default().insert(addr);
            cache.infohash_refs.entry(addr).or_default().insert(*infohash);
        }
        // 写入 peer_history 缓冲区（攒批写入，减少 fsync）
        let now = chrono::Utc::now().timestamp();
        let mut buffer = self.history_buffer.write();
        for p in new_peers {
            buffer.push(crate::storage::db::PeerHistoryEntry {
                infohash: *infohash,
                ip: p.addr.ip().to_string(),
                port: p.addr.port(),
                source: p.source.as_str().to_string(),
                score: p.priority_score,
                discovered_at: now,
            });
        }
        // 缓冲区满 100 条时自动 flush
        if buffer.len() >= 100 {
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);
            let storage = self.storage.clone();
            tokio::spawn(async move {
                let _ = storage.save_peer_history_batch(&batch);
            });
        }
    }

    /// 手动 flush peer_history 缓冲区
    pub async fn flush_history(&self) -> anyhow::Result<usize> {
        let batch: Vec<_> = self.history_buffer.write().drain(..).collect();
        if batch.is_empty() {
            return Ok(0);
        }
        let count = batch.len();
        let storage = self.storage.clone();
        tokio::task::spawn_blocking(move || storage.save_peer_history_batch(&batch)).await??;
        Ok(count)
    }

    pub fn get_peers_sync(&self, infohash: &Infohash, limit: usize) -> Vec<PeerInfo> {
        let cache = self.cache.read();
        let mut result: Vec<PeerInfo> = cache
            .by_infohash
            .get(infohash)
            .map(|addrs| addrs.iter().filter_map(|a| cache.global.get(a).cloned()).collect())
            .unwrap_or_default();
        result.sort_by(|a, b| b.priority_score.partial_cmp(&a.priority_score).unwrap_or(std::cmp::Ordering::Equal));
        result.truncate(limit);
        result
    }

    pub fn mark_connection_success_sync(&self, infohash: &Infohash, addr: &SocketAddr) {
        let mut cache = self.cache.write();
        if let Some(peer) = cache.global.get_mut(addr) {
            peer.connection_attempts += 1;
            peer.connection_successes += 1;
            peer.last_active = SystemTime::now();
            peer.calculate_priority();
        }
        let _ = infohash; // 兼容接口
    }

    pub fn mark_connection_failure_sync(&self, infohash: &Infohash, addr: &SocketAddr) {
        let mut cache = self.cache.write();
        if let Some(peer) = cache.global.get_mut(addr) {
            peer.connection_attempts += 1;
            peer.calculate_priority();
        }
        let _ = infohash;
    }

    pub fn cleanup_expired_sync(&self) {
        let ttl = Duration::from_secs(3600); // 默认1小时
        let now = SystemTime::now();
        let mut cache = self.cache.write();
        let expired: Vec<SocketAddr> = cache
            .global.iter()
            .filter(|(_, p)| now.duration_since(p.last_active).unwrap_or_default() > ttl)
            .map(|(a, _)| *a)
            .collect();
        for addr in &expired {
            if let Some(refs) = cache.infohash_refs.remove(addr) {
                for ih in refs {
                    if let Some(set) = cache.by_infohash.get_mut(&ih) {
                        set.remove(addr);
                        if set.is_empty() { cache.by_infohash.remove(&ih); }
                    }
                }
            }
            cache.global.remove(addr);
        }
    }

    pub fn len(&self) -> usize {
        self.cache.read().global.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.read().global.is_empty()
    }

    pub fn stats(&self) -> (usize, usize) {
        let cache = self.cache.read();
        (cache.by_infohash.len(), cache.global.len())
    }

    pub fn len_for_infohash(&self, infohash: &Infohash) -> usize {
        self.cache.read().by_infohash.get(infohash).map(|s| s.len()).unwrap_or(0)
    }

    pub fn peer_count_for_infohash(&self, infohash: &Infohash) -> usize {
        self.len_for_infohash(infohash)
    }

    pub fn infohashes(&self) -> Vec<Infohash> {
        self.cache.read().by_infohash.keys().cloned().collect()
    }

    pub fn clear(&self) {
        let mut cache = self.cache.write();
        cache.by_infohash.clear();
        cache.global.clear();
        cache.infohash_refs.clear();
    }

    pub fn all_peers_sync(&self) -> Vec<PeerInfo> {
        self.cache.read().global.values().cloned().collect()
    }

    /// 全量保存到 SQLite
    /// 注意：冷热判定统一由 intelligence 层的 TierSystem 负责，这里全量保存所有 peer
    pub async fn save_all(&self) -> anyhow::Result<()> {
        // 用内部作用域确保 cache 锁在 spawn_blocking 之前释放
        let batch = {
            let cache = self.cache.read();
            let mut batch = Vec::with_capacity(cache.global.len());
            for (infohash, addrs) in &cache.by_infohash {
                for addr in addrs {
                    if let Some(peer) = cache.global.get(addr) {
                        let last_active = peer
                            .last_active
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0);
                        batch.push(crate::storage::db::PeerRow {
                            infohash: *infohash,
                            ip: addr.ip().to_string(),
                            port: addr.port(),
                            source: peer.source.as_str().to_string(),
                            score: peer.priority_score,
                            connection_attempts: peer.connection_attempts,
                            connection_successes: peer.connection_successes,
                            last_active,
                        });
                    }
                }
            }
            batch
        };
        let storage = self.storage.clone();
        tokio::task::spawn_blocking(move || storage.save_peers_batch(&batch)).await??;
        Ok(())
    }

    /// 从 SQLite 加载全部 peer（运行时活跃 peer）
    pub async fn load_all(&self) -> anyhow::Result<usize> {
        let rows = self.storage.load_peers()?;
        let mut cache = self.cache.write();
        let mut count = 0;
        for row in &rows {
            let addr = SocketAddr::new(
                row.ip.parse().unwrap_or([127, 0, 0, 1].into()),
                row.port,
            );
            let source = match row.source.as_str() {
                "tracker" => PeerSource::Tracker,
                "dht" => PeerSource::Dht,
                "pex" => PeerSource::Pex,
                "super_tracker" => PeerSource::SuperTracker,
                "lpd" => PeerSource::Lpd,
                "webseed" => PeerSource::WebSeed,
                _ => PeerSource::Manual,
            };
            let mut peer = PeerInfo::new(addr, source);
            peer.priority_score = row.score;
            peer.connection_attempts = row.connection_attempts;
            peer.connection_successes = row.connection_successes;
            if row.last_active > 0 {
                peer.last_active = std::time::UNIX_EPOCH + std::time::Duration::from_secs(row.last_active as u64);
            }
            cache.global.insert(addr, peer);
            cache.by_infohash.entry(row.infohash).or_default().insert(addr);
            cache.infohash_refs.entry(addr).or_default().insert(row.infohash);
            count += 1;
        }
        Ok(count)
    }
}

#[async_trait]
impl PeerRepository for PeerRepoImpl {
    async fn add_peer(&self, infohash: Infohash, peer: PeerInfo) {
        self.add_peers_sync(&infohash, &[peer]);
    }

    async fn add_peers(&self, infohash: Infohash, peers: Vec<PeerInfo>) {
        self.add_peers_sync(&infohash, &peers);
    }

    async fn get_peers(&self, infohash: &Infohash, limit: usize) -> Vec<PeerInfo> {
        self.get_peers_sync(infohash, limit)
    }

    async fn remove_peer(&self, infohash: &Infohash, addr: &SocketAddr) {
        let mut cache = self.cache.write();
        if let Some(set) = cache.by_infohash.get_mut(infohash) { set.remove(addr); }
        if let Some(refs) = cache.infohash_refs.get_mut(addr) {
            refs.remove(infohash);
            if refs.is_empty() {
                cache.global.remove(addr);
                cache.infohash_refs.remove(addr);
            }
        }
    }

    async fn all_peers(&self) -> Vec<PeerInfo> {
        self.all_peers_sync()
    }

    async fn peer_count(&self) -> usize {
        self.len()
    }

    async fn infohash_count(&self) -> usize {
        self.cache.read().by_infohash.len()
    }

    async fn top_peers(&self, infohash: &Infohash, n: usize) -> Vec<PeerInfo> {
        self.get_peers_sync(infohash, n)
    }

    async fn update_score(&self, addr: &SocketAddr, score: f64) {
        if let Some(peer) = self.cache.write().global.get_mut(addr) {
            peer.priority_score = score;
        }
    }

    async fn update_probe_stats(&self, addr: &SocketAddr, tcp_ok: bool, _supports_dht: bool) {
        let mut cache = self.cache.write();
        if let Some(peer) = cache.global.get_mut(addr) {
            peer.connection_attempts += 1;
            if tcp_ok {
                peer.connection_successes += 1;
                peer.last_active = SystemTime::now();
            }
            peer.calculate_priority();
        }
    }

    async fn get_peer_global(&self, addr: &SocketAddr) -> Option<PeerInfo> {
        self.cache.read().global.get(addr).cloned()
    }

    async fn get_peer_infohash_count(&self, addr: &SocketAddr) -> u32 {
        self.cache.read().infohash_refs.get(addr).map(|s| s.len() as u32).unwrap_or(0)
    }

    async fn cleanup_expired(&self, ttl_secs: u64) {
        let ttl = Duration::from_secs(ttl_secs);
        let now = SystemTime::now();
        let mut cache = self.cache.write();
        let expired: Vec<SocketAddr> = cache
            .global.iter()
            .filter(|(_, p)| now.duration_since(p.last_active).unwrap_or_default() > ttl)
            .map(|(a, _)| *a)
            .collect();
        for addr in &expired {
            if let Some(refs) = cache.infohash_refs.remove(addr) {
                for ih in refs {
                    if let Some(set) = cache.by_infohash.get_mut(&ih) {
                        set.remove(addr);
                        if set.is_empty() { cache.by_infohash.remove(&ih); }
                    }
                }
            }
            cache.global.remove(addr);
        }
    }

    async fn save_all(&self) -> anyhow::Result<()> {
        PeerRepoImpl::save_all(self).await
    }

    async fn load_all(&self) -> anyhow::Result<usize> {
        PeerRepoImpl::load_all(self).await
    }

    async fn save_history(&self, infohash: Infohash, peer: &PeerInfo) {
        let now = chrono::Utc::now().timestamp();
        let mut buffer = self.history_buffer.write();
        buffer.push(crate::storage::db::PeerHistoryEntry {
            infohash,
            ip: peer.addr.ip().to_string(),
            port: peer.addr.port(),
            source: peer.source.as_str().to_string(),
            score: peer.priority_score,
            discovered_at: now,
        });
        // 缓冲区满 100 条时自动 flush
        if buffer.len() >= 100 {
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);
            let storage = self.storage.clone();
            tokio::spawn(async move {
                let _ = storage.save_peer_history_batch(&batch);
            });
        }
    }

    async fn query_history(&self, infohash: &Infohash, limit: usize) -> Vec<PeerInfo> {
        match self.storage.query_peer_history(infohash, limit) {
            Ok(rows) => rows.into_iter().map(|row| {
                let addr = SocketAddr::new(
                    row.ip.parse().unwrap_or([127, 0, 0, 1].into()), row.port);
                let source = match row.source.as_str() {
                    "tracker" => PeerSource::Tracker,
                    "dht" => PeerSource::Dht,
                    "pex" => PeerSource::Pex,
                    "super_tracker" => PeerSource::SuperTracker,
                    "lpd" => PeerSource::Lpd,
                    "webseed" => PeerSource::WebSeed,
                    _ => PeerSource::Manual,
                };
                let mut peer = PeerInfo::new(addr, source);
                peer.priority_score = row.score;
                peer
            }).collect(),
            Err(_) => Vec::new(),
        }
    }
}
