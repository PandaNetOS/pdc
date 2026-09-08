//! InfohashRepository 实现
//!
//! 合并 seen_infohashes + 引用计数，自动清理零引用。
//! 内存 FxHashMap + SQLite 增量持久化。
//! 千万级性能优化：FxHashMap 替代 std::HashMap，新 infohash 批量写入。

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;

use crate::storage::db::Storage;
use crate::storage::repo_traits::InfohashRepository;
use crate::types::Infohash;

struct InfohashCacheInner {
    /// infohash -> (引用计数, 首次发现来源)
    entries: FxHashMap<Infohash, (u32, String)>,
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
}

impl InfohashRepoImpl {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            cache: RwLock::new(InfohashCacheInner::new()),
            storage,
            pending: RwLock::new(Vec::new()),
        }
    }

    /// 同步获取 infohash 数量
    pub fn count_sync(&self) -> usize {
        self.cache.read().entries.len()
    }

    /// 同步获取所有 infohash
    pub fn all_sync(&self) -> Vec<Infohash> {
        self.cache.read().entries.keys().cloned().collect()
    }

    /// 同步注册 infohash（引用计数+1），新 infohash 写入 pending 缓冲区批量持久化
    pub fn register_sync(&self, infohash: Infohash, source: &str) {
        let is_new = {
            let mut cache = self.cache.write();
            let entry = cache
                .entries
                .entry(infohash)
                .or_insert((0, source.to_string()));
            entry.0 += 1;
            entry.0 == 1
        };

        // 新 infohash 写入 pending 缓冲区，由 flush_pending 批量写入 SQLite
        if is_new {
            self.pending.write().push((infohash, source.to_string()));
        }
    }

    /// 批量 flush pending 缓冲区到 SQLite
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
                storage.save_infohash(infohash, 1, source)?;
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

        let entries: Vec<(Infohash, u32, String)> = self
            .cache
            .read()
            .entries
            .iter()
            .map(|(ih, (count, src))| (*ih, *count, src.clone()))
            .collect();

        let storage = self.storage.clone();
        tokio::task::spawn_blocking(move || {
            for (infohash, ref_count, source) in &entries {
                storage.save_infohash(infohash, *ref_count, source)?;
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
                .insert(row.infohash, (row.ref_count, row.first_source));
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
        if let Some((count, _)) = cache.entries.get_mut(infohash) {
            *count = count.saturating_sub(1);
        }
    }

    async fn ref_count(&self, infohash: &Infohash) -> u32 {
        self.cache
            .read()
            .entries
            .get(infohash)
            .map(|(c, _)| *c)
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
        cache.entries.retain(|_, (count, _)| *count > 0);
        before - cache.entries.len()
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
