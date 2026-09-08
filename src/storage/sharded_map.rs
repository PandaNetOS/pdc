//! 锁分片 HashMap（Sharded HashMap）
//!
//! 用于高并发场景，将 HashMap 按 key 哈希分片到多个独立的 RwLock，
//! 读写只锁对应分片，避免全局锁竞争。
//!
//! 千万级数据下，64 分片可将锁竞争减少 90%+。

use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::Arc;

use parking_lot::RwLock;
use rustc_hash::FxHashMap;

/// 锁分片 HashMap
pub struct ShardedHashMap<K, V, S = rustc_hash::FxBuildHasher> {
    shards: Vec<RwLock<FxHashMap<K, V>>>,
    shard_count: usize,
    hasher: S,
}

impl<K, V> ShardedHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// 创建新的分片 HashMap
    pub fn new(shard_count: usize) -> Self {
        assert!(shard_count > 0 && shard_count.is_power_of_two(), "shard_count 必须是 2 的幂");
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(RwLock::new(FxHashMap::default()));
        }
        Self {
            shards,
            shard_count,
            hasher: rustc_hash::FxBuildHasher::default(),
        }
    }

    /// 计算分片索引
    #[inline]
    fn shard_index(&self, key: &K) -> usize {
        let mut h = self.hasher.build_hasher();
        key.hash(&mut h);
        (h.finish() as usize) & (self.shard_count - 1)
    }

    /// 获取值（克隆）
    pub fn get(&self, key: &K) -> Option<V> {
        let idx = self.shard_index(key);
        self.shards[idx].read().get(key).cloned()
    }

    /// 检查是否存在
    pub fn contains_key(&self, key: &K) -> bool {
        let idx = self.shard_index(key);
        self.shards[idx].read().contains_key(key)
    }

    /// 插入值，返回旧值
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let idx = self.shard_index(&key);
        self.shards[idx].write().insert(key, value)
    }

    /// 删除值，返回被删除的值
    pub fn remove(&self, key: &K) -> Option<V> {
        let idx = self.shard_index(key);
        self.shards[idx].write().remove(key)
    }

    /// 总元素数
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.read().is_empty())
    }

    /// 全量遍历（不持有全局锁，逐个分片读取）
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&K, &V),
    {
        for shard in &self.shards {
            let map = shard.read();
            for (k, v) in map.iter() {
                f(k, v);
            }
        }
    }

    /// 全量遍历并收集（不持有全局锁）
    pub fn collect<F, R>(&self, mut f: F) -> Vec<R>
    where
        F: FnMut(&K, &V) -> R,
    {
        let mut result = Vec::new();
        for shard in &self.shards {
            let map = shard.read();
            for (k, v) in map.iter() {
                result.push(f(k, v));
            }
        }
        result
    }

    /// 获取分片数量
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    /// 清空所有分片
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.write().clear();
        }
    }
}

/// 带 dirty 标记的分片 HashMap（用于增量持久化）
pub struct DirtyShardedHashMap<K, V> {
    map: Arc<ShardedHashMap<K, V>>,
    dirty: RwLock<FxHashMap<K, ()>>,
}

impl<K, V> DirtyShardedHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new(shard_count: usize) -> Self {
        Self {
            map: Arc::new(ShardedHashMap::new(shard_count)),
            dirty: RwLock::new(FxHashMap::default()),
        }
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.dirty.write().insert(key.clone(), ());
        self.map.insert(key, value)
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.map.get(key)
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.dirty.write().insert(key.clone(), ());
        self.map.remove(key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn for_each<F>(&self, f: F)
    where
        F: FnMut(&K, &V),
    {
        self.map.for_each(f)
    }

    /// 取出所有 dirty 的 key（原子替换为空集合）
    pub fn take_dirty(&self) -> Vec<K> {
        let mut dirty = self.dirty.write();
        let keys: Vec<K> = dirty.keys().cloned().collect();
        dirty.clear();
        keys
    }

    /// dirty 数量
    pub fn dirty_count(&self) -> usize {
        self.dirty.read().len()
    }

    pub fn inner(&self) -> &Arc<ShardedHashMap<K, V>> {
        &self.map
    }
}
