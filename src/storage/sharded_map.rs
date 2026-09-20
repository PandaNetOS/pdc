//! 锁分片 HashMap（Sharded HashMap）
//!
//! 用于高并发场景，将 HashMap 按 key 哈希分片到多个独立的 RwLock，
//! 读写只锁对应分片，避免全局锁竞争。
//!
//! 千万级数据下，16 分片可将锁竞争减少 90%+。

use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

use parking_lot::RwLock;
use rustc_hash::FxHashMap;

/// 锁分片 HashMap
pub struct ShardedHashMap<K, V, S = rustc_hash::FxBuildHasher> {
    shards: Vec<RwLock<FxHashMap<K, V>>>,
    shard_count: usize,
    hasher: S,
}

/// 读取所有分片的读守卫（持有全部读锁，提供统一 HashMap 视图）
///
/// 用于批量遍历/统计操作。热点单 key 操作应直接使用
/// `ShardedHashMap` 的分片方法，避免持有全部分片锁。
pub struct ShardedReadGuard<'a, K: Eq + Hash, V> {
    guards: Vec<parking_lot::RwLockReadGuard<'a, FxHashMap<K, V>>>,
    shard_count: usize,
}

/// 写入所有分片的写守卫（持有全部写锁，提供统一 HashMap 视图）
///
/// 用于批量插入/删除/遍历修改操作。热点单 key 写操作应使用
/// `ShardedHashMap::with_mut` 或 `insert`/`remove` 等分片方法。
pub struct ShardedWriteGuard<'a, K: Eq + Hash, V> {
    guards: Vec<parking_lot::RwLockWriteGuard<'a, FxHashMap<K, V>>>,
    shard_count: usize,
}

impl<K, V> ShardedHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// 创建新的分片 HashMap
    pub fn new(shard_count: usize) -> Self {
        assert!(
            shard_count > 0 && shard_count.is_power_of_two(),
            "shard_count 必须是 2 的幂"
        );
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(RwLock::new(FxHashMap::default()));
        }
        Self {
            shards,
            shard_count,
            hasher: rustc_hash::FxBuildHasher,
        }
    }

    /// 计算分片索引
    #[inline]
    fn shard_index(&self, key: &K) -> usize {
        (self.hasher.hash_one(key) as usize) & (self.shard_count - 1)
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

    /// 对单 key 执行可变操作（只锁对应分片，热点路径用）
    ///
    /// 闭包接收 `&mut V`，返回闭包返回值。
    /// 用于 update_score / set_node_state 等单 key 写操作，
    /// 避免 `write_all()` 持有全部 16 把锁。
    pub fn with_mut<R>(&self, key: &K, f: impl FnOnce(&mut V) -> R) -> Option<R> {
        let idx = self.shard_index(key);
        self.shards[idx].write().get_mut(key).map(f)
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

    /// 锁定全部分片读锁，返回统一读视图
    ///
    /// 用于批量遍历/统计操作。热点单 key 读取请直接用 `get()` / `contains_key()`。
    pub fn read_all(&self) -> ShardedReadGuard<'_, K, V> {
        let guards = self.shards.iter().map(|s| s.read()).collect();
        ShardedReadGuard {
            guards,
            shard_count: self.shard_count,
        }
    }

    /// 锁定全部分片写锁，返回统一写视图
    ///
    /// 用于批量插入/删除/遍历修改操作。热点单 key 写入请用
    /// `insert()` / `remove()` / `with_mut()` 等分片方法。
    pub fn write_all(&self) -> ShardedWriteGuard<'_, K, V> {
        let guards = self.shards.iter().map(|s| s.write()).collect();
        ShardedWriteGuard {
            guards,
            shard_count: self.shard_count,
        }
    }
}

impl<'a, K: Eq + Hash, V> ShardedReadGuard<'a, K, V> {
    #[inline]
    fn shard_idx(&self, key: &K) -> usize {
        let h = rustc_hash::FxBuildHasher;
        (h.hash_one(key) as usize) & (self.shard_count - 1)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let i = self.shard_idx(key);
        self.guards[i].get(key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        let i = self.shard_idx(key);
        self.guards[i].contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.guards.iter().map(|g| g.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.guards.iter().all(|g| g.is_empty())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        self.guards.iter().flat_map(|g| g.iter())
    }

    pub fn values(&self) -> impl Iterator<Item = &V> + '_ {
        self.guards.iter().flat_map(|g| g.values())
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> + '_ {
        self.guards.iter().flat_map(|g| g.keys())
    }
}

impl<'a, K: Eq + Hash, V> ShardedWriteGuard<'a, K, V> {
    #[inline]
    fn shard_idx(&self, key: &K) -> usize {
        let h = rustc_hash::FxBuildHasher;
        (h.hash_one(key) as usize) & (self.shard_count - 1)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let i = self.shard_idx(key);
        self.guards[i].get(key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let i = self.shard_idx(key);
        self.guards[i].get_mut(key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        let i = self.shard_idx(key);
        self.guards[i].contains_key(key)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let i = self.shard_idx(&key);
        self.guards[i].insert(key, value)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let i = self.shard_idx(key);
        self.guards[i].remove(key)
    }

    pub fn len(&self) -> usize {
        self.guards.iter().map(|g| g.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.guards.iter().all(|g| g.is_empty())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        self.guards.iter().flat_map(|g| g.iter())
    }

    pub fn values(&self) -> impl Iterator<Item = &V> + '_ {
        self.guards.iter().flat_map(|g| g.values())
    }

    /// 对所有值执行可变闭包（避免返回迭代器的生命周期问题）
    pub fn for_values_mut<F: FnMut(&mut V)>(&mut self, mut f: F) {
        for g in self.guards.iter_mut() {
            for v in g.values_mut() {
                f(v);
            }
        }
    }

    /// 对所有 (key, value) 对执行可变闭包
    pub fn for_each_mut<F: FnMut(&K, &mut V)>(&mut self, mut f: F) {
        for g in self.guards.iter_mut() {
            for (k, v) in g.iter_mut() {
                f(k, v);
            }
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

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
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
