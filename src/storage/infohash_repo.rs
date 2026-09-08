//! InfohashRepository 实现
//!
//! 合并 seen_infohashes + 引用计数，自动清理零引用。
//! 内存 HashMap + SQLite 持久化双写。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::storage::db::Storage;
use crate::storage::repo_traits::InfohashRepository;
use crate::types::Infohash;

struct InfohashCacheInner {
    /// infohash -> (引用计数, 首次发现来源)
    entries: HashMap<Infohash, (u32, String)>,
}

impl InfohashCacheInner {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
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

    /// 同步注册 infohash（引用计数+1），同时异步持久化
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

        // 新 infohash 立即持久化；已有 infohash 由定期 save_all 覆盖
        if is_new {
            let storage = self.storage.clone();
            let src = source.to_string();
            tokio::spawn(async move {
                let _ = storage.save_infohash(&infohash, 1, &src);
            });
        }
    }

    /// 全量保存到 SQLite
    pub async fn save_all(&self) -> anyhow::Result<()> {
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
}
