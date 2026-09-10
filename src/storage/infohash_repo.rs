//! InfohashRepository 实现
//!
//! 合并 seen_infohashes + 引用计数，自动清理零引用。
//! 内存 FxHashMap + SQLite 增量持久化。
//! 千万级性能优化：FxHashMap 替代 std::HashMap，新 infohash 批量写入。

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;

use crate::federation::gossip::GossipEngine;
use crate::federation::merkle::MerkleTree;
use crate::federation::protocol::{operation, repo_type, SyncEntry};

use crate::storage::db::Storage;
use crate::storage::repo_traits::InfohashRepository;
use crate::types::Infohash;

struct InfohashCacheInner {
    /// infohash -> (引用计数, 首次发现来源, 热门度评分)
    entries: FxHashMap<Infohash, (u32, String, f64)>,
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
    /// 待持久化的新 infohash 缓冲区（批量写入，避免频繁 SQLite IO）
    pending: RwLock<Vec<(Infohash, String)>>,
    /// 联邦引用（OnceLock 注入；未设置时本地写入不触发 Merkle/Gossip，repo 正常工作）
    merkle: OnceLock<Arc<MerkleTree>>,
    gossip: OnceLock<Arc<GossipEngine>>,
}

impl InfohashRepoImpl {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            cache: RwLock::new(InfohashCacheInner::new()),
            storage,
            pending: RwLock::new(Vec::new()),
            merkle: OnceLock::new(),
            gossip: OnceLock::new(),
        }
    }

    /// 注入联邦 Merkle 树与 Gossip 引擎引用（main.rs 在 FederationService 创建后调用）。
    /// 未调用时（如单元测试），本地写入不触发传播，repo 行为完全不变。
    pub fn set_federation_refs(&self, merkle: Arc<MerkleTree>, gossip: Arc<GossipEngine>) {
        let _ = self.merkle.set(merkle);
        let _ = self.gossip.set(gossip);
    }

    /// 将本地新写入的条目批量更新 Merkle 并提交 Gossip（写锁外执行，纯内存操作）。
    /// merkle/gossip 未注入时直接跳过，不 panic。
    #[inline]
    fn propagate(&self, rt: u8, built: Vec<(Vec<u8>, Vec<u8>)>) {
        if built.is_empty() {
            return;
        }
        let Some(merkle) = self.merkle.get() else { return; };
        let Some(gossip) = self.gossip.get() else { return; };
        let refs: Vec<(&[u8], &[u8])> = built
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        merkle.update_batch(&refs);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entries: Vec<SyncEntry> = built
            .into_iter()
            .map(|(key, payload)| SyncEntry {
                key,
                operation: operation::UPSERT,
                version: now,
                payload,
            })
            .collect();
        gossip.submit_gossip(rt, entries);
    }

    /// 同步获取 infohash 数量
    pub fn count_sync(&self) -> usize {
        self.cache.read().entries.len()
    }

    /// 同步获取所有 infohash
    pub fn all_sync(&self) -> Vec<Infohash> {
        self.cache.read().entries.keys().cloned().collect()
    }

    /// 内部批量注册：更新引用计数 + 新 infohash 入 pending 缓冲区，不触发 Merkle/Gossip。
    /// 返回真正新增的 (infohash, source)（引用计数 0→1）。
    /// 联邦同步入站（apply_infohash_sync）调用本方法，避免 Merkle 重复更新与 Gossip 回环。
    pub(crate) fn register_batch_internal(&self, items: &[(Infohash, String)]) -> Vec<(Infohash, String)> {
        if items.is_empty() {
            return Vec::new();
        }
        let mut cache = self.cache.write();
        let mut new_items: Vec<(Infohash, String)> = Vec::new();
        for (infohash, source) in items {
            let entry = cache
                .entries
                .entry(*infohash)
                .or_insert((0, source.clone(), 0.0));
            entry.0 += 1;
            if entry.0 == 1 {
                new_items.push((*infohash, source.clone()));
            }
        }
        drop(cache);

        // 新 infohash 批量写入 pending 缓冲区，由 flush_pending 批量写入 SQLite
        if !new_items.is_empty() {
            let mut pending = self.pending.write();
            for item in &new_items {
                pending.push(item.clone());
            }
        }
        new_items
    }

    /// 同步注册 infohash（引用计数+1），新 infohash 写入 pending 缓冲区批量持久化。
    /// 本地写入路径：新 infohash 更新 Merkle + 提交 Gossip。
    pub fn register_sync(&self, infohash: Infohash, source: &str) {
        let items = [(infohash, source.to_string())];
        let new_items = self.register_batch_internal(&items);
        self.propagate_infohash(new_items);
    }

    /// 批量注册 infohash（一次 cache 写锁 + 一次 pending 写锁），返回新注册数。
    /// 本地写入路径：新 infohash 更新 Merkle + 提交 Gossip。
    /// 联邦同步入站（apply_infohash_sync）请改用 register_batch_internal，避免回环。
    pub fn register_batch_sync(&self, items: &[(Infohash, String)]) -> usize {
        let new_items = self.register_batch_internal(items);
        let count = new_items.len();
        self.propagate_infohash(new_items);
        count
    }

    /// 把新 infohash 列表构建成 merkle/gossip 条目并传播。
    fn propagate_infohash(&self, new_items: Vec<(Infohash, String)>) {
        if new_items.is_empty() {
            return;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut built: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(new_items.len());
        for (infohash, source) in &new_items {
            if let Some((k, p)) = crate::federation::sync::infohash_sync::build_infohash_sync_entry(
                *infohash, now, source,
            ) {
                built.push((k, p));
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
        let storage = self.storage.clone();
        tokio::task::spawn_blocking(move || {
            for (infohash, source) in &pending {
                storage.save_infohash(infohash, 1, source, 0.0)?;
            }
            Ok::<(), anyhow::Error>(())
        }).await??;

        tracing::debug!("[infohash_repo] flush_pending 批量写入 {} 个新 infohash", count);
        Ok(count)
    }

    /// pending 缓冲区大小
    pub fn pending_count(&self) -> usize {
        self.pending.read().len()
    }

    /// 全量保存到 SQLite（先 flush pending，再全量更新引用计数）
    pub async fn save_all(&self) -> anyhow::Result<()> {
        // 先 flush pending 新 infohash
        self.flush_pending().await?;

        let entries: Vec<(Infohash, u32, String, f64)> = self
            .cache
            .read()
            .entries
            .iter()
            .map(|(ih, (count, src, score))| (*ih, *count, src.clone(), *score))
            .collect();

        let storage = self.storage.clone();
        tokio::task::spawn_blocking(move || {
            for (infohash, ref_count, source, score) in &entries {
                storage.save_infohash(infohash, *ref_count, source, *score)?;
            }
            Ok::<(), anyhow::Error>(())
        }).await??;
        Ok(())
    }

    /// 从 SQLite 加载全部 infohash
    pub async fn load_all(&self) -> anyhow::Result<usize> {
        let rows = self.storage.load_infohashes()?;
        let mut cache = self.cache.write();
        let mut count = 0;
        for row in rows {
            cache
                .entries
                .insert(row.infohash, (row.ref_count, row.first_source, row.score));
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
        if let Some((count, _, _)) = cache.entries.get_mut(infohash) {
            *count = count.saturating_sub(1);
        }
    }

    async fn ref_count(&self, infohash: &Infohash) -> u32 {
        self.cache
            .read()
            .entries
            .get(infohash)
            .map(|(c, _, _)| *c)
            .unwrap_or(0)
    }

    async fn all_infohashes(&self) -> Vec<Infohash> {
        self.cache.read().entries.keys().cloned().collect()
    }

    async fn count(&self) -> usize {
        self.cache.read().entries.len()
    }

    async fn cleanup_zero_ref(&self) -> usize {
        let mut cache = self.cache.write();
        let before = cache.entries.len();
        cache.entries.retain(|_, (count, _, _)| *count > 0);
        before - cache.entries.len()
    }

    async fn update_score(&self, infohash: &Infohash, score: f64) {
        // 更新内存缓存
        {
            let mut cache = self.cache.write();
            if let Some((_, _, s)) = cache.entries.get_mut(infohash) {
                *s = score;
            }
        }
        // 异步持久化到 SQLite
        let storage = self.storage.clone();
        let ih = *infohash;
        tokio::task::spawn_blocking(move || {
            let _ = storage.update_infohash_score(&ih, score);
        });
    }

    async fn update_scores_batch(&self, scores: &[(Infohash, f64)]) {
        if scores.is_empty() {
            return;
        }
        // 更新内存缓存
        {
            let mut cache = self.cache.write();
            for (infohash, score) in scores {
                if let Some((_, _, s)) = cache.entries.get_mut(infohash) {
                    *s = *score;
                }
            }
        }
        // 异步批量持久化到 SQLite
        let storage = self.storage.clone();
        let scores_vec: Vec<([u8; 20], f64)> = scores.iter().map(|(ih, s)| (*ih, *s)).collect();
        tokio::task::spawn_blocking(move || {
            let _ = storage.update_infohash_scores_batch(&scores_vec);
        });
    }

    async fn get_score(&self, infohash: &Infohash) -> f64 {
        self.cache
            .read()
            .entries
            .get(infohash)
            .map(|(_, _, s)| *s)
            .unwrap_or(0.0)
    }

    async fn top_infohashes(&self, n: usize) -> Vec<(Infohash, f64)> {
        let mut entries: Vec<(Infohash, f64)> = self
            .cache
            .read()
            .entries
            .iter()
            .map(|(ih, (_, _, score))| (*ih, *score))
            .collect();
        entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        entries.truncate(n);
        entries
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
