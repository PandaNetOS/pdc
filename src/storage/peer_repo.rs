//! PeerRepository 鐎圭偟骞?
//!
//! 閸氬牆鑻?PeerCache + PEX濮?+ Probe闂冪喎鍨?+ SuperTracker peers閿涘奔缍旀稉?BT Peer 閻ㄥ嫬鏁稉鈧ぐ鎺戝經閵?
//! 閸愬懎鐡ㄧ紓鎾崇摠 + SQLite 閸樺棗褰堕崣灞藉晸閵?
//! 娴ｈ法鏁?parking_lot::RwLock閿涘牆鎮撳銉礆閿涘奔绗?NodeRepo 娑撯偓閼疯揪绱濇笟澶哥艾閺囨寧宕查崢?PeerCache閵?

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use parking_lot::RwLock;
use rustc_hash::FxHashSet;
use tracing::{info, warn};

use crate::federation::gossip::GossipEngine;
use crate::federation::merkle::MerkleTree;
use crate::federation::protocol::{operation, repo_type, SyncEntry};

use crate::storage::db::Storage;
use crate::storage::repo_traits::PeerRepository;
use crate::storage::tiered_cache::TieredCacheConfig;
use crate::storage::write_queue::WriteQueue;
use crate::types::{Infohash, PeerInfo, PeerSource};

/// 閸愬懎鐡ㄦ稉顓犳畱 peer 缂傛挸鐡ㄩ敍鍫熷瘻 infohash 閸掑棛绮嶉敍?
struct PeerMemoryStore {
    /// infohash -> peer addr 闂嗗棗鎮?
    by_infohash: HashMap<Infohash, HashSet<SocketAddr>>,
    /// 閸忋劌鐪?peer addr -> PeerInfo閿涘牐娉?infohash 閸樺鍣搁敍?
    global: HashMap<SocketAddr, PeerInfo>,
    /// peer addr -> 閸戣櫣骞囬崷銊ユ憿娴?infohash 娑?
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
    /// peer_history 閸愭瑥鍙嗙紓鎾冲暱閸栫尨绱欓弨鎺撳閸愭瑥鍙嗛敍灞藉櫤鐏?fsync閿?
    history_buffer: RwLock<Vec<crate::storage::db::PeerHistoryEntry>>,
    /// 鑴?peer 闆嗗悎锛堢粺璁℃暟鎹凡鍙樺寲锛岄渶瑕侀噸绠楄瘎鍒?+ 澧為噺鎸佷箙鍖栵級
    dirty: RwLock<FxHashSet<SocketAddr>>,
    /// 鑱旈偊寮曠敤锛圤nceLock 娉ㄥ叆锛涙湭璁剧疆鏃舵湰鍦板啓鍏ヤ笉瑙﹀彂 Merkle/Gossip锛宺epo 姝ｅ父宸ヤ綔锛?
    merkle: OnceLock<Arc<MerkleTree>>,
    gossip: OnceLock<Arc<GossipEngine>>,
    /// 鍐欏叆闃熷垪锛堝彲閫夛紝Some 鏃?history flush 閫氳繃 WriteQueue/IOScheduler 鎻愪氦锛?
    write_queue: Option<Arc<WriteQueue>>,
}

impl PeerRepoImpl {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self::with_tier_config(storage, Default::default(), true)
    }

    pub fn with_tier_config(
        storage: Arc<Storage>,
        _cache_config: TieredCacheConfig,
        _tier_enabled: bool,
    ) -> Self {
        Self {
            cache: RwLock::new(PeerMemoryStore::new()),
            storage,
            history_buffer: RwLock::new(Vec::new()),
            dirty: RwLock::new(FxHashSet::default()),
            merkle: OnceLock::new(),
            gossip: OnceLock::new(),
            write_queue: None,
        }
    }

    // 閳光偓閳光偓 閸氬本顒炴笟鎸庡祹閺傝纭堕敍鍧僺ync 閸氬海绱戦敍灞肩瑢 async trait 閺傝纭堕崠鍝勫瀻閿涘鏀㈤埞鈧?

    /// 娉ㄥ叆鑱旈偊 Merkle 鏍戜笌 Gossip 寮曟搸寮曠敤锛坢ain.rs 鍦?FederationService 鍒涘缓鍚庤皟鐢級銆?
    /// 鏈皟鐢ㄦ椂锛堝鍗曞厓娴嬭瘯锛夛紝鏈湴鍐欏叆涓嶈Е鍙戜紶鎾紝repo 琛屼负瀹屽叏涓嶅彉銆?
    pub fn set_federation_refs(&self, merkle: Arc<MerkleTree>, gossip: Arc<GossipEngine>) {
        let _ = self.merkle.set(merkle);
        let _ = self.gossip.set(gossip);
    }

    /// 娉ㄥ叆鍐欏叆闃熷垪锛坆uilder 妯″紡锛?
    pub fn with_write_queue(mut self, wq: Arc<WriteQueue>) -> Self {
        self.write_queue = Some(wq);
        self
    }

    /// 鑾峰彇搴曞眰 Storage 寮曠敤锛堢敤浜庤仈閭﹀悓姝ユ寜鍒嗙墖鍔犺浇鏁版嵁锛夈€?
    pub fn storage(&self) -> Arc<Storage> {
        self.storage.clone()
    }

    /// 鍒嗗眰缂撳瓨缁熻锛堜笌 TrackerRepo 鎺ュ彛涓€鑷达細(hot, warm, cold_loaded)锛夈€?
    /// PeerRepo 鍏ㄩ噺椹诲唴瀛橈紝鍏ㄩ儴璁″叆 hot銆?
    pub fn cache_stats(&self) -> (usize, usize, u64) {
        (self.len(), 0, 0)
    }

    /// 鏁版嵁搴撲腑 peers 琛ㄧ殑鎬昏鏁帮紙鍚屾锛岀敤浜庣洃鎺ч潰鏉匡級
    pub fn total_count_sync(&self) -> u64 {
        self.storage.count_table("peers").unwrap_or(0)
    }

    /// 执行分层检查 + 驱逐（由 TaskScheduler 定时调用）。
    /// 全量驻内存不使用 TieredCache，为空操作。
    pub fn tier_evict(&self) {
        // no-op: data is fully in memory, no tiered cache to evict from
    }

    /// 紧急驱逐（内存超限时调用）。空操作。
    pub fn emergency_evict(&self, _count: usize) {
        // no-op: data is fully in memory
    }

    /// 妫€鏌?(infohash, addr) 鍏宠仈鏄惁宸插瓨鍦紙鑱旈偊鍚屾鍘婚噸鐢級
    pub fn has_peer_assoc(&self, infohash: &Infohash, addr: &SocketAddr) -> bool {
        self.cache
            .read()
            .by_infohash
            .get(infohash)
            .map(|set| set.contains(addr))
            .unwrap_or(false)
    }

    // 鈹€鈹€ 鑴忔爣璁板悓姝ユ柟娉曪紙鐢ㄤ簬澧為噺璇勫垎 + 澧為噺鎸佷箙鍖栵級鈹€鈹€

    pub fn mark_dirty_sync(&self, addr: &SocketAddr) {
        self.dirty.write().insert(*addr);
    }

    pub fn dirty_peers_sync(&self) -> Vec<SocketAddr> {
        self.dirty.read().iter().cloned().collect()
    }

    pub fn clear_all_dirty_sync(&self) {
        self.dirty.write().clear();
    }

    /// 灏嗘湰鍦版柊鍐欏叆鐨勬潯鐩壒閲忔洿鏂?Merkle 骞舵彁浜?Gossip锛堝啓閿佸鎵ц锛岀函鍐呭瓨鎿嶄綔锛夈€?
    /// merkle/gossip 鏈敞鍏ユ椂鐩存帴璺宠繃锛屼笉 panic銆?
    #[inline]
    fn propagate(&self, rt: u8, built: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>) {
        if built.is_empty() {
            return;
        }
        let Some(merkle) = self.merkle.get() else {
            return;
        };
        let Some(gossip) = self.gossip.get() else {
            return;
        };
        let refs: Vec<(&[u8], &[u8], &[u8])> = built
            .iter()
            .map(|(k, p, h)| (k.as_slice(), p.as_slice(), h.as_slice()))
            .collect();
        merkle.update_batch(&refs);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entries: Vec<SyncEntry> = built
            .into_iter()
            .map(|(key, payload, _)| SyncEntry {
                key,
                operation: operation::UPSERT,
                version: now,
                payload,
            })
            .collect();
        gossip.submit_gossip(rt, entries);
    }

    /// 纭繚鏁版嵁宸插姞杞斤紙濡傛灉 cache 涓虹┖锛屼粠 SQLite 鍚屾鍔犺浇锛?
    pub fn ensure_loaded(&self) {
        if self.cache.read().global.is_empty() {
            info!("[federation] PeerRepo cache 涓虹┖锛岄噸鏂颁粠 SQLite 鍔犺浇...");
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
                            peer.last_active = std::time::UNIX_EPOCH
                                + std::time::Duration::from_secs(row.last_active as u64);
                        } else {
                            peer.last_active = SystemTime::now();
                        }
                        cache.global.insert(addr, peer);
                        cache
                            .by_infohash
                            .entry(row.infohash)
                            .or_default()
                            .insert(addr);
                        cache
                            .infohash_refs
                            .entry(addr)
                            .or_default()
                            .insert(row.infohash);
                        count += 1;
                    }
                    info!(
                        "[federation] PeerRepo 閲嶆柊鍔犺浇瀹屾垚: {} 涓?peer",
                        count
                    );
                }
                Err(e) => {
                    warn!("[federation] PeerRepo 閲嶆柊鍔犺浇澶辫触: {}", e);
                }
            }
        }
    }

    /// 鍐呴儴鍐欏叆锛氭壒閲忔洿鏂板唴瀛樼紦瀛?+ history 缂撳啿鍖猴紝涓嶈Е鍙?Merkle/Gossip銆?
    /// 鍐呴儴鍐欏叆锛氭壒閲忔柊澧?peer + 鍏宠仈锛屼笉瑙﹀彂 Merkle/Gossip銆?
    /// 杩斿洖鐪熸鏂板鐨?peer (addr, first_seen_secs, source)锛堟寜 addr 鍘婚噸锛夈€?
    /// 鑱旈偊鍚屾鍏ョ珯锛坅pply_peer_sync锛夎鏀圭敤 add_peers_only_internal锛岄伩鍏嶅洖鐜€?
    pub(crate) fn add_peers_sync_internal(
        &self,
        items: &[(Infohash, PeerInfo)],
    ) -> Vec<(Infohash, SocketAddr, u64, String)> {
        if items.is_empty() {
            return Vec::new();
        }
        let mut cache = self.cache.write();
        let mut new_entries: Vec<(Infohash, SocketAddr, u64, String)> = Vec::new();
        let mut seen_keys: FxHashSet<(Infohash, SocketAddr)> = FxHashSet::default();
        for (infohash, peer) in items {
            let addr = peer.addr;
            let _is_new_peer = !cache.global.contains_key(&addr);
            if let Some(existing) = cache.global.get_mut(&addr) {
                existing.last_active = peer.last_active;
                existing.source = peer.source;
                if peer.peer_id.is_some() {
                    existing.peer_id = peer.peer_id;
                }
            } else {
                cache.global.insert(addr, peer.clone());
            }
            // 缁存姢 (infohash, addr) 鍏宠仈
            let is_new_assoc = cache.by_infohash.entry(*infohash).or_default().insert(addr);
            cache
                .infohash_refs
                .entry(addr)
                .or_default()
                .insert(*infohash);
            if is_new_assoc && seen_keys.insert((*infohash, addr)) {
                let first_seen_secs = peer
                    .first_seen
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                new_entries.push((
                    *infohash,
                    addr,
                    first_seen_secs,
                    peer.source.as_str().to_string(),
                ));
            }
        }
        drop(cache);

        // 鍐欏叆 peer_history 缂撳啿鍖猴紙鎵归噺鑺傛祦锛屽噺灏?fsync锛?
        let now = chrono::Utc::now().timestamp();
        let mut buffer = self.history_buffer.write();
        for (infohash, p) in items {
            buffer.push(crate::storage::db::PeerHistoryEntry {
                infohash: *infohash,
                ip: p.addr.ip().to_string(),
                port: p.addr.port(),
                source: p.source.as_str().to_string(),
                score: p.priority_score,
                discovered_at: now,
            });
        }
        // 缂撳啿鍖烘弧 100 鏉℃椂鑷姩 flush锛堜笌鍘熻涓轰竴鑷达級
        if buffer.len() >= 100 {
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);
            if let Some(ref wq) = self.write_queue {
                // 閫氳繃 WriteQueue/IOScheduler 鎻愪氦锛圢ormal 浼樺厛绾э級
                wq.send(move |conn| Storage::save_peer_history_batch_in_tx(conn, &batch));
            } else {
                let storage = self.storage.clone();
                tokio::spawn(async move {
                    let _ = storage.save_peer_history_batch(&batch);
                });
            }
        }
        new_entries
    }

    /// 鍐呴儴鍐欏叆锛氫粎鎵归噺鏂板 peer锛堜笉缁存姢 infohash 鍏宠仈锛夛紝涓嶈Е鍙?Merkle/Gossip銆?
    /// 鑱旈偊鍚屾鍏ョ珯锛坅pply_peer_sync锛夎皟鐢ㄦ湰鏂规硶锛岄伩鍏?Merkle 閲嶅鏇存柊涓?Gossip 鍥炵幆銆?
    #[allow(dead_code)]
    pub(crate) fn add_peers_only_internal(&self, peers: &[PeerInfo]) {
        if peers.is_empty() {
            return;
        }
        let mut cache = self.cache.write();
        for peer in peers {
            let addr = peer.addr;
            if let Some(existing) = cache.global.get_mut(&addr) {
                existing.last_active = peer.last_active;
                existing.source = peer.source;
                if peer.peer_id.is_some() {
                    existing.peer_id = peer.peer_id;
                }
            } else {
                cache.global.insert(addr, peer.clone());
            }
        }
    }

    /// 鎶婃柊 peer 鍒楄〃鏋勫缓鎴?merkle/gossip 鏉＄洰骞朵紶鎾紙涓?Node/Infohash/Tracker 涓€鑷达級銆?
    fn propagate_peers(&self, new_entries: Vec<(Infohash, SocketAddr, u64, String)>) {
        if new_entries.is_empty() {
            return;
        }
        let mut built: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::with_capacity(new_entries.len());
        for (infohash, addr, _first_seen_secs, _source) in &new_entries {
            if let Some((k, p, h)) =
                crate::federation::sync::peer_sync::build_peer_sync_entry(*infohash, *addr)
            {
                built.push((k, p, h));
            }
        }
        self.propagate(repo_type::PEER, built);
    }

    /// 鍚屾鎵归噺鍔犲叆 peer锛堟湰鍦板彂鐜拌矾寰勶級锛氬啓鍏ュ悗缁熶竴鏇存柊 Merkle + 鎻愪氦 Gossip銆?
    pub fn add_peers_sync(&self, infohash: &Infohash, new_peers: &[PeerInfo]) {
        let items: Vec<(Infohash, PeerInfo)> =
            new_peers.iter().map(|p| (*infohash, p.clone())).collect();
        let new_peers = self.add_peers_sync_internal(&items);
        self.propagate_peers(new_peers);
    }

    /// 鎵归噺鍐欏叆 peer锛堜竴娆?cache 鍐欓攣 + 涓€娆?history_buffer 鍐欓攣锛夛紝杩斿洖澶勭悊鏉℃暟銆?
    /// 鏈湴鍐欏叆璺緞锛氭柊 peer 鏇存柊 Merkle + 鎻愪氦 Gossip銆?
    /// 鑱旈偊鍚屾鍏ョ珯锛坅pply_peer_sync锛夎鏀圭敤 add_peers_only_internal锛岄伩鍏嶅洖鐜€?
    pub fn add_peers_sync_batch(&self, items: &[(Infohash, PeerInfo)]) -> usize {
        if items.is_empty() {
            return 0;
        }
        let new_peers = self.add_peers_sync_internal(items);
        self.propagate_peers(new_peers);
        items.len()
    }

    pub async fn flush_history(&self) -> anyhow::Result<usize> {
        let batch: Vec<_> = self.history_buffer.write().drain(..).collect();
        if batch.is_empty() {
            return Ok(0);
        }
        let count = batch.len();
        if let Some(ref wq) = self.write_queue {
            wq.send(move |conn| Storage::save_peer_history_batch_in_tx(conn, &batch));
        } else {
            let storage = self.storage.clone();
            tokio::task::spawn_blocking(move || storage.save_peer_history_batch(&batch)).await??;
        }
        Ok(count)
    }

    pub fn get_peers_sync(&self, infohash: &Infohash, limit: usize) -> Vec<PeerInfo> {
        let cache = self.cache.read();
        let mut result: Vec<PeerInfo> = cache
            .by_infohash
            .get(infohash)
            .map(|addrs| {
                addrs
                    .iter()
                    .filter_map(|a| cache.global.get(a).cloned())
                    .collect()
            })
            .unwrap_or_default();
        result.sort_by(|a, b| {
            b.priority_score
                .partial_cmp(&a.priority_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        result.truncate(limit);
        result
    }

    /// 浠?peer_history 琛ㄥ悓姝ユ煡璇㈠巻鍙?peer锛堝喎鏁版嵁琛ュ厖锛夈€?
    ///
    /// 鐢ㄤ簬瓒呯骇 Tracker 鍦ㄥ唴瀛?peer 涓嶈冻鏃惰ˉ鍏呰惤鐩樼殑鍐锋暟鎹€?
    /// 杩斿洖瑙ｆ瀽鍑虹殑 SocketAddr 鍒楄〃锛堟寜 discovered_at 鍊掑簭锛岀敱 SQL 鍐冲畾锛夈€?
    pub fn get_peers_from_history(&self, infohash: &Infohash, limit: usize) -> Vec<SocketAddr> {
        match self.storage.query_peer_history(infohash, limit) {
            Ok(rows) => rows
                .into_iter()
                .filter_map(|row| row.ip.parse().ok().map(|ip| SocketAddr::new(ip, row.port)))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    pub fn mark_connection_success_sync(&self, infohash: &Infohash, addr: &SocketAddr) {
        let mut cache = self.cache.write();
        if let Some(peer) = cache.global.get_mut(addr) {
            peer.connection_attempts += 1;
            peer.connection_successes += 1;
            peer.last_active = SystemTime::now();
        }
        let _ = infohash; // 閸忕厧顔愰幒銉ュ經
    }

    pub fn mark_connection_failure_sync(&self, infohash: &Infohash, addr: &SocketAddr) {
        let mut cache = self.cache.write();
        if let Some(peer) = cache.global.get_mut(addr) {
            peer.connection_attempts += 1;
        }
        let _ = infohash;
    }

    pub fn cleanup_expired_sync(&self) {
        // 姘镐箙璧勪骇妯″紡锛氫笉鍒犻櫎浠讳綍 peer
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
        self.cache
            .read()
            .by_infohash
            .get(infohash)
            .map(|s| s.len())
            .unwrap_or(0)
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

    /// 楂樻晥鑾峰彇鎵€鏈?peer 鍙婂叾鍏宠仈鐨?infohashes锛堜竴娆¤閿侊級
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

    /// 閸忋劑鍣烘穱婵嗙摠閸?SQLite
    /// 濞夈劍鍓伴敍姘枎閻戭厼鍨界€规氨绮烘稉鈧悽?intelligence 鐏炲倻娈?TierSystem 鐠愮喕鐭楅敍宀冪箹闁插苯鍙忛柌蹇庣箽鐎涙ɑ澧嶉張?peer
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
        if batch.is_empty() {
            return Ok(());
        }
        if let Some(wq) = &self.write_queue {
            // 寮傛妯″紡锛氶潪闃诲鍏ラ槦 WriteQueue/IOScheduler
            let count = batch.len();
            wq.send(move |conn| Storage::save_peers_batch_in_tx(conn, &batch));
            tracing::debug!("[peer_repo] 寮傛鍏ラ槦淇濆瓨 {} 涓?peer", count);
            Ok(())
        } else {
            // 鍚屾妯″紡锛氫繚鐣欏師 spawn_blocking 閫昏緫
            let storage = self.storage.clone();
            tokio::task::spawn_blocking(move || storage.save_peers_batch(&batch)).await??;
            Ok(())
        }
    }

    /// 娴?SQLite 閸旂姾娴囬崗銊╁劥 peer閿涘牐绻嶇悰灞炬濞叉槒绌?peer閿?
    /// 澧為噺鎸佷箙鍖栵紙鍏ㄩ噺淇濆瓨妯″紡锛氭墍鏈夋暟鎹潎瑙嗕负 dirty锛岀洿鎺ュ叏閲忎繚瀛橈級
    pub async fn save_dirty(&self) -> anyhow::Result<()> {
        self.save_all().await
    }

    pub async fn load_all(&self) -> anyhow::Result<usize> {
        let rows = self.storage.load_peers()?;
        let mut cache = self.cache.write();
        let mut count = 0;
        for row in &rows {
            let addr = SocketAddr::new(row.ip.parse().unwrap_or([127, 0, 0, 1].into()), row.port);
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
                peer.last_active =
                    std::time::UNIX_EPOCH + std::time::Duration::from_secs(row.last_active as u64);
            } else {
                peer.last_active = SystemTime::now();
            }
            cache.global.insert(addr, peer);
            cache
                .by_infohash
                .entry(row.infohash)
                .or_default()
                .insert(addr);
            cache
                .infohash_refs
                .entry(addr)
                .or_default()
                .insert(row.infohash);
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
        if let Some(set) = cache.by_infohash.get_mut(infohash) {
            set.remove(addr);
        }
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

    async fn update_scores_batch(&self, scores: &[(SocketAddr, f64)]) {
        let mut cache = self.cache.write();
        let mut dirty = self.dirty.write();
        for (addr, score) in scores {
            if let Some(peer) = cache.global.get_mut(addr) {
                peer.priority_score = *score;
                dirty.insert(*addr);
            }
        }
    }

    async fn mark_dirty(&self, addr: &SocketAddr) {
        self.mark_dirty_sync(addr);
    }

    async fn dirty_peers(&self) -> Vec<SocketAddr> {
        self.dirty_peers_sync()
    }

    async fn clear_all_dirty(&self) {
        self.clear_all_dirty_sync();
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
        self.cache
            .read()
            .infohash_refs
            .get(addr)
            .map(|s| s.len() as u32)
            .unwrap_or(0)
    }

    async fn cleanup_expired(&self, _ttl_secs: u64) {
        // 姘镐箙璧勪骇妯″紡锛氫笉鍒犻櫎浠讳綍 peer
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
            if let Some(ref wq) = self.write_queue {
                wq.send(move |conn| Storage::save_peer_history_batch_in_tx(conn, &batch));
            } else {
                let storage = self.storage.clone();
                tokio::spawn(async move {
                    let _ = storage.save_peer_history_batch(&batch);
                });
            }
        }
    }

    async fn query_history(&self, infohash: &Infohash, limit: usize) -> Vec<PeerInfo> {
        match self.storage.query_peer_history(infohash, limit) {
            Ok(rows) => rows
                .into_iter()
                .map(|row| {
                    let addr =
                        SocketAddr::new(row.ip.parse().unwrap_or([127, 0, 0, 1].into()), row.port);
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
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }
}
