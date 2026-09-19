//! InfohashRepository 瀹炵幇
//!
//! 鍚堝苟 seen_infohashes + 寮曠敤璁℃暟锛岃嚜鍔ㄦ竻鐞嗛浂寮曠敤銆?
//! 鍐呭瓨 FxHashMap + SQLite 澧為噺鎸佷箙鍖栥€?
//! 鍗冧竾绾ф€ц兘浼樺寲锛欶xHashMap 鏇夸唬 std::HashMap锛屾柊 infohash 鎵归噺鍐欏叆銆?

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::federation::gossip::GossipEngine;
use crate::federation::merkle::MerkleTree;
use crate::federation::protocol::{operation, repo_type, SyncEntry};

use crate::storage::db::{InfohashRow, Storage};
use crate::storage::repo_traits::InfohashRepository;
use crate::storage::tiered_cache::TieredCacheConfig;
use crate::storage::write_queue::WriteQueue;
use crate::types::Infohash;

struct InfohashCacheInner {
    /// infohash -> (寮曠敤璁℃暟, 棣栨鍙戠幇鏉ユ簮, 鐑棬搴﹁瘎鍒?
    entries: FxHashMap<Infohash, (u32, String, f64, u64)>,
}

impl InfohashCacheInner {
    fn new() -> Self {
        Self {
            entries: FxHashMap::default(),
        }
    }
}

pub struct InfohashRepoImpl {
    cache: RwLock<InfohashCacheInner>,
    storage: Arc<Storage>,
    /// 寰呮寔涔呭寲鐨勬柊 infohash 缂撳啿鍖猴紙鎵归噺鍐欏叆锛岄伩鍏嶉绻?SQLite IO锛?
    pending: RwLock<Vec<(Infohash, String)>>,
    /// 鑴?infohash 闆嗗悎锛堢粺璁℃暟鎹凡鍙樺寲锛岄渶瑕侀噸绠楄瘎鍒嗭級
    dirty: RwLock<FxHashSet<Infohash>>,
    /// 鑱旈偊寮曠敤锛圤nceLock 娉ㄥ叆锛涙湭璁剧疆鏃舵湰鍦板啓鍏ヤ笉瑙﹀彂 Merkle/Gossip锛宺epo 姝ｅ父宸ヤ綔锛?
    merkle: OnceLock<Arc<MerkleTree>>,
    gossip: OnceLock<Arc<GossipEngine>>,
    /// 鍐欏叆闃熷垪锛堝彲閫夛紝Some 鏃?flush_pending 閫氳繃 WriteQueue/IOScheduler 鎻愪氦锛?
    write_queue: Option<Arc<WriteQueue>>,
}

impl InfohashRepoImpl {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self::with_tier_config(storage, Default::default(), true)
    }

    pub fn with_tier_config(
        storage: Arc<Storage>,
        _cache_config: TieredCacheConfig,
        _tier_enabled: bool,
    ) -> Self {
        Self {
            cache: RwLock::new(InfohashCacheInner::new()),
            storage,
            pending: RwLock::new(Vec::new()),
            dirty: RwLock::new(FxHashSet::default()),
            merkle: OnceLock::new(),
            gossip: OnceLock::new(),
            write_queue: None,
        }
    }

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
    /// InfohashRepo 鍏ㄩ噺椹诲唴瀛橈紝鍏ㄩ儴璁″叆 hot銆?
    pub fn cache_stats(&self) -> (usize, usize, u64) {
        (self.count_sync(), 0, 0)
    }

    /// 鏁版嵁搴撲腑 infohashes 琛ㄧ殑鎬昏鏁帮紙鍚屾锛岀敤浜庣洃鎺ч潰鏉匡級
    pub fn total_count_sync(&self) -> u64 {
        self.storage.count_table("infohashes").unwrap_or(0)
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

    // 鈹€鈹€ 鑴忔爣璁板悓姝ユ柟娉曪紙鐢ㄤ簬澧為噺璇勫垎锛夆攢鈹€

    pub fn mark_dirty_sync(&self, infohash: &Infohash) {
        self.dirty.write().insert(*infohash);
    }

    pub fn dirty_infohashes_sync(&self) -> Vec<Infohash> {
        self.dirty.read().iter().copied().collect()
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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
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

    /// 鍚屾鑾峰彇 infohash 鏁伴噺
    pub fn count_sync(&self) -> usize {
        self.cache.read().entries.len()
    }

    /// 鍚屾鑾峰彇鎵€鏈?infohash
    pub fn all_sync(&self) -> Vec<(Infohash, u64)> {
        self.cache
            .read()
            .entries
            .iter()
            .map(|(k, v)| (*k, v.3))
            .collect()
    }

    /// 鍐呴儴鎵归噺娉ㄥ唽锛氭洿鏂板紩鐢ㄨ鏁?+ 鏂?infohash 鍏?pending 缂撳啿鍖猴紝涓嶈Е鍙?Merkle/Gossip銆?
    /// 杩斿洖鐪熸鏂板鐨?(infohash, source)锛堝紩鐢ㄨ鏁?0鈫?锛夈€?
    /// 鑱旈偊鍚屾鍏ョ珯锛坅pply_infohash_sync锛夎皟鐢ㄦ湰鏂规硶锛岄伩鍏?Merkle 閲嶅鏇存柊涓?Gossip 鍥炵幆銆?
    pub(crate) fn register_batch_internal(
        &self,
        items: &[(Infohash, String, u64)],
    ) -> Vec<(Infohash, String)> {
        if items.is_empty() {
            return Vec::new();
        }
        let mut cache = self.cache.write();
        let mut new_items: Vec<(Infohash, String)> = Vec::new();
        for (infohash, source, last_seen) in items {
            // LWW: 濡傛灉鏈湴宸叉湁涓斿绔?last_seen 杈冩棫锛屽垯璺宠繃
            if let Some(existing) = cache.entries.get(infohash) {
                if *last_seen < existing.3 {
                    continue;
                }
            }
            let entry =
                cache
                    .entries
                    .entry(*infohash)
                    .or_insert((0, source.clone(), 0.0, *last_seen));
            entry.0 += 1;
            // 鏇存柊 last_seen锛堝彇杈冨ぇ鍊硷級
            if *last_seen > entry.3 {
                entry.3 = *last_seen;
            }
            if entry.0 == 1 {
                new_items.push((*infohash, source.clone()));
            }
        }
        drop(cache);

        // 鏂?infohash 鎵归噺鍐欏叆 pending 缂撳啿鍖猴紝鐢?flush_pending 鎵归噺鍐欏叆 SQLite
        if !new_items.is_empty() {
            let mut pending = self.pending.write();
            for item in &new_items {
                pending.push(item.clone());
            }
        }
        new_items
    }

    /// 鍚屾娉ㄥ唽 infohash锛堝紩鐢ㄨ鏁?1锛夛紝鏂?infohash 鍐欏叆 pending 缂撳啿鍖烘壒閲忔寔涔呭寲銆?
    /// 鏈湴鍐欏叆璺緞锛氭柊 infohash 鏇存柊 Merkle + 鎻愪氦 Gossip銆?
    pub fn register_sync(&self, infohash: Infohash, source: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let items = [(infohash, source.to_string(), now)];
        let new_items = self.register_batch_internal(&items);
        self.propagate_infohash(new_items);
    }

    /// 鎵归噺娉ㄥ唽 infohash锛堜竴娆?cache 鍐欓攣 + 涓€娆?pending 鍐欓攣锛夛紝杩斿洖鏂版敞鍐屾暟銆?
    /// 鏈湴鍐欏叆璺緞锛氭柊 infohash 鏇存柊 Merkle + 鎻愪氦 Gossip銆?
    /// 鑱旈偊鍚屾鍏ョ珯锛坅pply_infohash_sync锛夎鏀圭敤 register_batch_internal锛岄伩鍏嶅洖鐜€?
    pub fn register_batch_sync(&self, items: &[(Infohash, String)]) -> usize {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let items_with_ts: Vec<(Infohash, String, u64)> = items
            .iter()
            .map(|(ih, src)| (*ih, src.clone(), now))
            .collect();
        let new_items = self.register_batch_internal(&items_with_ts);
        let count = new_items.len();
        self.propagate_infohash(new_items);
        count
    }

    /// 鎶婃柊 infohash 鍒楄〃鏋勫缓鎴?merkle/gossip 鏉＄洰骞朵紶鎾€?
    fn propagate_infohash(&self, new_items: Vec<(Infohash, String)>) {
        if new_items.is_empty() {
            return;
        }
        let mut built: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::with_capacity(new_items.len());
        for (infohash, _source) in &new_items {
            if let Some((k, p, h)) =
                crate::federation::sync::infohash_sync::build_infohash_sync_entry(*infohash)
            {
                built.push((k, p, h));
            }
        }
        self.propagate(repo_type::INFOHASH, built);
    }

    pub async fn flush_pending(&self) -> anyhow::Result<usize> {
        let pending = {
            let mut p = self.pending.write();
            if p.is_empty() {
                return Ok(0);
            }
            std::mem::take(&mut *p)
        };

        let count = pending.len();
        if let Some(ref wq) = self.write_queue {
            // 閫氳繃 WriteQueue/IOScheduler 鎻愪氦锛圢ormal 浼樺厛绾э級
            wq.send(move |conn| {
                for (infohash, source) in &pending {
                    Storage::save_infohash_in_tx(conn, infohash, 1, source, 0.0)?;
                }
                Ok(())
            });
        } else {
            let storage = self.storage.clone();
            tokio::task::spawn_blocking(move || {
                for (infohash, source) in &pending {
                    storage.save_infohash(infohash, 1, source, 0.0)?;
                }
                Ok::<(), anyhow::Error>(())
            })
            .await??;
        }

        tracing::debug!(
            "[infohash_repo] flush_pending 鎵归噺鍐欏叆 {} 涓柊 infohash",
            count
        );
        Ok(count)
    }

    /// pending 缂撳啿鍖哄ぇ灏?
    pub fn pending_count(&self) -> usize {
        self.pending.read().len()
    }

    /// 鍏ㄩ噺淇濆瓨鍒?SQLite锛堝厛 flush pending锛屽啀鍏ㄩ噺鏇存柊寮曠敤璁℃暟锛?
    pub async fn save_all(&self) -> anyhow::Result<()> {
        // 鍏?flush pending 鏂?infohash
        self.flush_pending().await?;

        let entries: Vec<InfohashRow> = self
            .cache
            .read()
            .entries
            .iter()
            .map(|(ih, (count, src, score, _ls))| InfohashRow {
                infohash: *ih,
                ref_count: *count,
                first_source: src.clone(),
                score: *score,
            })
            .collect();

        if entries.is_empty() {
            return Ok(());
        }

        if let Some(wq) = &self.write_queue {
            // 寮傛妯″紡锛氶潪闃诲鍏ラ槦 WriteQueue
            let count = entries.len();
            wq.send(move |conn| Storage::save_infohashes_batch_in_tx(conn, &entries));
            tracing::debug!(
                "[infohash_repo] 寮傛鍏ラ槦鍏ㄩ噺淇濆瓨 {} 涓?infohash",
                count
            );
            Ok(())
        } else {
            // 鍚屾妯″紡锛氫繚鐣欏師 spawn_blocking 閫愭潯鍐欏叆閫昏緫
            let storage = self.storage.clone();
            tokio::task::spawn_blocking(move || {
                for row in &entries {
                    storage.save_infohash(
                        &row.infohash,
                        row.ref_count,
                        &row.first_source,
                        row.score,
                    )?;
                }
                Ok::<(), anyhow::Error>(())
            })
            .await??;
            Ok(())
        }
    }

    /// 浠?SQLite 鍔犺浇鍏ㄩ儴 infohash
    /// 澧為噺鎸佷箙鍖栵紙鍏ㄩ噺淇濆瓨妯″紡锛氭墍鏈夋暟鎹潎瑙嗕负 dirty锛岀洿鎺ュ叏閲忎繚瀛橈級
    pub async fn save_dirty(&self) -> anyhow::Result<()> {
        self.save_all().await
    }

    pub async fn load_all(&self) -> anyhow::Result<usize> {
        let rows = self.storage.load_infohashes()?;
        let mut cache = self.cache.write();
        let mut count = 0;
        for row in rows {
            cache.entries.insert(
                row.infohash,
                (row.ref_count, row.first_source, row.score, 0),
            );
            count += 1;
        }
        Ok(count)
    }
}

#[async_trait]
impl InfohashRepository for InfohashRepoImpl {
    async fn register(&self, infohash: Infohash, source: &str) {
        self.register_sync(infohash, source);
    }

    async fn unregister(&self, infohash: &Infohash) {
        let mut cache = self.cache.write();
        if let Some((count, _, _, _)) = cache.entries.get_mut(infohash) {
            *count = count.saturating_sub(1);
        }
    }

    async fn ref_count(&self, infohash: &Infohash) -> u32 {
        self.cache
            .read()
            .entries
            .get(infohash)
            .map(|(c, _, _, _)| *c)
            .unwrap_or(0)
    }

    async fn all_infohashes(&self) -> Vec<Infohash> {
        self.cache.read().entries.keys().cloned().collect()
    }

    async fn count(&self) -> usize {
        self.cache.read().entries.len()
    }

    async fn cleanup_zero_ref(&self) -> usize {
        // 姘镐箙璧勪骇妯″紡锛歩nfohash 姘镐箙淇濈暀锛屼笉鍒犻櫎闆跺紩鐢ㄦ潯鐩?
        0
    }

    async fn update_score(&self, infohash: &Infohash, score: f64) {
        // 鏇存柊鍐呭瓨缂撳瓨
        {
            let mut cache = self.cache.write();
            if let Some((_, _, s, _)) = cache.entries.get_mut(infohash) {
                *s = score;
            }
        }
        // 寮傛鎸佷箙鍖栧埌 SQLite
        if let Some(wq) = &self.write_queue {
            let ih = *infohash;
            wq.send(move |conn| Storage::update_infohash_score_in_tx(conn, &ih, score));
        } else {
            let storage = self.storage.clone();
            let ih = *infohash;
            tokio::task::spawn_blocking(move || {
                let _ = storage.update_infohash_score(&ih, score);
            });
        }
    }

    async fn update_scores_batch(&self, scores: &[(Infohash, f64)]) {
        if scores.is_empty() {
            return;
        }
        // 鏇存柊鍐呭瓨缂撳瓨
        {
            let mut cache = self.cache.write();
            for (infohash, score) in scores {
                if let Some((_, _, s, _)) = cache.entries.get_mut(infohash) {
                    *s = *score;
                }
            }
        }
        // 寮傛鎵归噺鎸佷箙鍖栧埌 SQLite
        let scores_vec: Vec<([u8; 20], f64)> = scores.iter().map(|(ih, s)| (*ih, *s)).collect();
        if let Some(wq) = &self.write_queue {
            wq.send(move |conn| Storage::update_infohash_scores_batch_in_tx(conn, &scores_vec));
        } else {
            let storage = self.storage.clone();
            tokio::task::spawn_blocking(move || {
                let _ = storage.update_infohash_scores_batch(&scores_vec);
            });
        }
    }

    async fn get_score(&self, infohash: &Infohash) -> f64 {
        self.cache
            .read()
            .entries
            .get(infohash)
            .map(|(_, _, s, _)| *s)
            .unwrap_or(0.0)
    }

    async fn top_infohashes(&self, n: usize) -> Vec<(Infohash, f64)> {
        let mut entries: Vec<(Infohash, f64)> = self
            .cache
            .read()
            .entries
            .iter()
            .map(|(ih, (_, _, score, _))| (*ih, *score))
            .collect();
        entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        entries.truncate(n);
        entries
    }

    async fn mark_dirty(&self, infohash: &Infohash) {
        self.mark_dirty_sync(infohash);
    }

    async fn dirty_infohashes(&self) -> Vec<Infohash> {
        self.dirty_infohashes_sync()
    }

    async fn clear_all_dirty(&self) {
        self.clear_all_dirty_sync();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_count() {
        let storage = Arc::new(Storage::memory().unwrap());
        let repo = InfohashRepoImpl::new(storage);
        let ih = [1u8; 20];
        repo.register(ih, "test").await;
        assert_eq!(repo.count().await, 1);
        assert_eq!(repo.ref_count(&ih).await, 1);
    }

    #[tokio::test]
    async fn test_persistence() {
        let storage = Arc::new(Storage::memory().unwrap());
        let repo = InfohashRepoImpl::new(storage.clone());
        let ih = [2u8; 20];
        repo.register(ih, "dht").await;
        repo.save_all().await.unwrap();

        let repo2 = InfohashRepoImpl::new(storage);
        let loaded = repo2.load_all().await.unwrap();
        assert_eq!(loaded, 1);
        assert_eq!(repo2.ref_count(&ih).await, 1);
    }

    #[tokio::test]
    async fn test_pending_batch() {
        let storage = Arc::new(Storage::memory().unwrap());
        let repo = InfohashRepoImpl::new(storage);
        repo.register([1u8; 20], "dht").await;
        repo.register([2u8; 20], "tracker").await;
        assert_eq!(repo.pending_count(), 2);
        let flushed = repo.flush_pending().await.unwrap();
        assert_eq!(flushed, 2);
        assert_eq!(repo.pending_count(), 0);
    }
}
