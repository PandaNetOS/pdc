//! PeerRepository 瀹炵幇
//!
//! 鍚堝苟 PeerCache + PEX姹?+ Probe闃熷垪 + SuperTracker peers锛屼綔涓?BT Peer 鐨勫敮涓€褰掑彛銆?
//! 鍐呭瓨缂撳瓨 + SQLite 鍘嗗彶鍙屽啓銆?
//! 浣跨敤 parking_lot::RwLock锛堝悓姝ワ級锛屼笌 NodeRepo 涓€鑷达紝渚夸簬鏇挎崲鍘?PeerCache銆?

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use parking_lot::RwLock;
use tracing::{info, warn};

use crate::storage::db::Storage;
use crate::storage::repo_traits::PeerRepository;
use crate::types::{Infohash, PeerInfo, PeerSource};

/// 鍐呭瓨涓殑 peer 缂撳瓨锛堟寜 infohash 鍒嗙粍锛?
struct PeerMemoryStore {
    /// infohash -> peer addr 闆嗗悎
    by_infohash: HashMap<Infohash, HashSet<SocketAddr>>,
    /// 鍏ㄥ眬 peer addr -> PeerInfo锛堣法 infohash 鍘婚噸锛?
    global: HashMap<SocketAddr, PeerInfo>,
    /// peer addr -> 鍑虹幇鍦ㄥ摢浜?infohash 涓?
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
    /// peer_history 鍐欏叆缂撳啿鍖猴紙鏀掓壒鍐欏叆锛屽噺灏?fsync锛?
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

    // 鈹€鈹€ 鍚屾渚挎嵎鏂规硶锛坃sync 鍚庣紑锛屼笌 async trait 鏂规硶鍖哄垎锛夆攢鈹€

    /// 确保数据已加载（如果 cache 为空，从 SQLite 同步加载）
    pub fn ensure_loaded(&self) {
        if self.cache.read().global.is_empty() {
            info!("[federation] PeerRepo cache 为空，重新从 SQLite 加载...");
            match self.storage.load_peers() {
                Ok(rows) => {
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
                        } else {
                            peer.last_active = SystemTime::now();
                        }
                        cache.global.insert(addr, peer);
                        cache.by_infohash.entry(row.infohash).or_default().insert(addr);
                        cache.infohash_refs.entry(addr).or_default().insert(row.infohash);
                        count += 1;
                    }
                    info!("[federation] PeerRepo 重新加载完成: {} 个 peer", count);
                }
                Err(e) => {
                    warn!("[federation] PeerRepo 重新加载失败: {}", e);
                }
            }
        }
    }

    pub fn add_peers_sync(&self, infohash: &Infohash, new_peers: &[PeerInfo]) {
        let mut cache = self.cache.write();
        for peer in new_peers {
            let addr = peer.addr;
            if let Some(existing) = cache.global.get_mut(&addr) {
                existing.last_active = peer.last_active;
                existing.source = peer.source;  // 鐩存帴鏇存柊鏉ユ簮锛堟暟鎹眰涓嶅仛璇勫垎鍐崇瓥锛?
                if peer.peer_id.is_some() {
                    existing.peer_id = peer.peer_id;
                }
            } else {
                cache.global.insert(addr, peer.clone());
            }
            cache.by_infohash.entry(*infohash).or_default().insert(addr);
            cache.infohash_refs.entry(addr).or_default().insert(*infohash);
        }
        // 鍐欏叆 peer_history 缂撳啿鍖猴紙鏀掓壒鍐欏叆锛屽噺灏?fsync锛?
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
        // 缂撳啿鍖烘弧 100 鏉℃椂鑷姩 flush
        if buffer.len() >= 100 {
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);
            let storage = self.storage.clone();
            tokio::spawn(async move {
                let _ = storage.save_peer_history_batch(&batch);
            });
        }
    }

    /// 鎵嬪姩 flush peer_history 缂撳啿鍖?
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
        }
        let _ = infohash; // 鍏煎鎺ュ彛
    }

    pub fn mark_connection_failure_sync(&self, infohash: &Infohash, addr: &SocketAddr) {
        let mut cache = self.cache.write();
        if let Some(peer) = cache.global.get_mut(addr) {
            peer.connection_attempts += 1;
        }
        let _ = infohash;
    }

    pub fn cleanup_expired_sync(&self) {
        let ttl = Duration::from_secs(3600); // 榛樿1灏忔椂
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

    /// 高效获取所有 peer 及其关联的 infohashes（一次读锁）
    pub fn all_peers_with_infohashes_sync(&self) -> Vec<(PeerInfo, Vec<Infohash>)> {
        let cache = self.cache.read();
        let mut result = Vec::with_capacity(cache.global.len());
        for (addr, peer) in &cache.global {
            let infohashes = cache
                .infohash_refs
                .get(addr)
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default();
            result.push((peer.clone(), infohashes));
        }
        result
    }

    /// 鍏ㄩ噺淇濆瓨鍒?SQLite
    /// 娉ㄦ剰锛氬喎鐑垽瀹氱粺涓€鐢?intelligence 灞傜殑 TierSystem 璐熻矗锛岃繖閲屽叏閲忎繚瀛樻墍鏈?peer
    pub async fn save_all(&self) -> anyhow::Result<()> {
        // 鐢ㄥ唴閮ㄤ綔鐢ㄥ煙纭繚 cache 閿佸湪 spawn_blocking 涔嬪墠閲婃斁
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

    /// 浠?SQLite 鍔犺浇鍏ㄩ儴 peer锛堣繍琛屾椂娲昏穬 peer锛?
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
            } else {
                peer.last_active = SystemTime::now();
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
        // 缂撳啿鍖烘弧 100 鏉℃椂鑷姩 flush
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
