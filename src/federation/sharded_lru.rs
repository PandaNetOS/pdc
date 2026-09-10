//! 分片 LRU 缓存
//!
//! 将单个全局 `RwLock<LruCache>` 拆成 N 片，每片独立一把读写锁。
//! key 按 `DefaultHasher` 哈希后取模落到对应分片，使原本串行化的全局写锁
//! 退化为仅同分片 key 之间的锁竞争，从而消除 PDC 联邦初始同步时
//! `flush_gossip_buffer` 4 路并行 task 在 `seen_msgs` 上的串行化瓶颈。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;

use lru::LruCache;
use parking_lot::RwLock;

/// 分片 LRU 缓存。
///
/// - 分片数 `shard_count`：默认 16，由配置 `gossip_seen_shards` 控制。
/// - 每片容量 `capacity_per_shard`：总容量 / 分片数，四舍五入后至少为 1。
///
/// 所有方法均为 `&self`，内部按分片加锁；`RwLock<LruCache<..>>` 满足 `Send + Sync`，
/// 因此本结构可安全地在多线程间共享。
pub struct ShardedLruCache<K: Hash + Eq + Clone, V: Clone> {
    shards: Vec<RwLock<LruCache<K, V>>>,
}

impl<K: Hash + Eq + Clone, V: Clone> ShardedLruCache<K, V> {
    /// 创建分片缓存。
    ///
    /// `shard_count` 与 `capacity_per_shard` 均会被钳制到至少 1，
    /// 避免传入 0 导致 panic。
    pub fn new(shard_count: usize, capacity_per_shard: usize) -> Self {
        let shard_count = shard_count.max(1);
        let capacity_per_shard = capacity_per_shard.max(1);
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(RwLock::new(LruCache::new(
                NonZeroUsize::new(capacity_per_shard).expect("capacity_per_shard >= 1"),
            )));
        }
        Self { shards }
    }

    #[inline]
    fn shard_index(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    /// 查询 key 是否存在（读锁，仅阻塞同分片写）。
    pub fn contains(&self, key: &K) -> bool {
        let idx = self.shard_index(key);
        self.shards[idx].read().contains(key)
    }

    /// 插入/更新 key（写锁，仅阻塞同分片）。
    pub fn put(&self, key: K, value: V) {
        let idx = self.shard_index(&key);
        self.shards[idx].write().put(key, value);
    }

    /// 原子地“检查并插入”：
    ///
    /// - 若 key 已存在，返回 `false`（不更新）；
    /// - 若 key 不存在，插入并返回 `true`。
    ///
    /// 与原单锁版本“持有写锁先 contains 再 put”等价：在同一分片的写锁内完成两步，
    /// 避免两个并发任务对同一 key 同时判空导致重复处理。
    pub fn check_and_put(&self, key: K, value: V) -> bool {
        let idx = self.shard_index(&key);
        let mut guard = self.shards[idx].write();
        if guard.contains(&key) {
            return false;
        }
        guard.put(key, value);
        true
    }
}
