//! TrackerRepository 实现
//!
//! 封装 tracker 状态 + SQLite 持久化。
//!
//! 【冷热分层架构】所有数据永久保存在 SQLite，内存只保留 Hot+Warm。
//! - cache: TieredCache<String, TrackerEntry>
//! - tracker 列表量小（~326个），可全量保留，但仍接入 TieredCache 保持一致
//! - 缓存未命中时自动从 SQLite 按需加载
//! - 对外接口不变，调用方无感知

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use parking_lot::RwLock;
use rustc_hash::FxHashSet;

use crate::federation::gossip::GossipEngine;
use crate::federation::merkle::MerkleTree;
use crate::federation::protocol::{operation, repo_type, SyncEntry};

use crate::storage::db::{Storage, TrackerRow};
use crate::storage::repo_traits::{TrackerEntry, TrackerRepository};
use crate::storage::tiered_cache::{TieredCache, TieredCacheConfig};
use crate::storage::write_queue::WriteQueue;

pub struct TrackerRepoImpl {
    /// 冷热分层缓存（替代原 FxHashMap 全量存储）
    cache: TieredCache<String, TrackerEntry>,
    /// 持久化脏 tracker 集合（统计数据变化，需要增量持久化到 SQLite）
    persist_dirty: RwLock<FxHashSet<String>>,
    /// 评分脏 tracker 集合（评分已更新，供评分系统查询增量评分）
    score_dirty: RwLock<FxHashSet<String>>,
    storage: Arc<Storage>,
    /// 联邦引用（OnceLock 注入）
    merkle: OnceLock<Arc<MerkleTree>>,
    gossip: OnceLock<Arc<GossipEngine>>,
    /// 写入队列（可选）
    write_queue: Option<Arc<WriteQueue>>,
    /// 冷热分层开关
    tier_enabled: bool,
}

impl TrackerRepoImpl {
    pub fn new(storage: Arc<Storage>) -> Self {
        // Tracker 列表量小，使用较大容量配置（实际不会超限）
        let config = TieredCacheConfig {
            hot_max_count: 10_000,
            warm_max_count: 10_000,
            hot_threshold_secs: 86400,   // 24 小时
            warm_threshold_secs: 604800, // 7 天
        };
        Self::with_tier_config(storage, config, true)
    }

    pub fn with_tier_config(
        storage: Arc<Storage>,
        cache_config: TieredCacheConfig,
        tier_enabled: bool,
    ) -> Self {
        Self {
            cache: TieredCache::new(cache_config),
            persist_dirty: RwLock::new(FxHashSet::default()),
            score_dirty: RwLock::new(FxHashSet::default()),
            storage,
            merkle: OnceLock::new(),
            gossip: OnceLock::new(),
            write_queue: None,
            tier_enabled,
        }
    }

    pub fn with_write_queue(mut self, wq: Arc<WriteQueue>) -> Self {
        self.write_queue = Some(wq);
        self
    }

    /// 获取底层 Storage 引用（用于联邦同步按分片加载数据）。
    pub fn storage(&self) -> Arc<Storage> {
        self.storage.clone()
    }

    pub fn set_federation_refs(&self, merkle: Arc<MerkleTree>, gossip: Arc<GossipEngine>) {
        let _ = self.merkle.set(merkle);
        let _ = self.gossip.set(gossip);
    }

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
        // P1-2：本地新增/更新记入 oplog（delta 同步来源）；失败只告警。
        crate::storage::oplog::record_local_ops(&self.storage, rt, &entries);
        gossip.submit_gossip(rt, entries);
    }

    // ── 同步便捷方法 ──

    /// 内部写入：批量新增 tracker，不触发 Merkle/Gossip。
    pub(crate) fn add_trackers_batch_internal(&self, items: &[(String, u64)]) -> Vec<String> {
        if items.is_empty() {
            return Vec::new();
        }
        let mut new_urls: Vec<String> = Vec::new();
        for (url, last_seen) in items {
            let updated = self.cache.update(url, |entry| {
                if entry.last_used.is_none_or(|t| *last_seen > t) {
                    entry.last_used = Some(*last_seen);
                }
            });
            if !updated {
                // 缓存未命中（可能在 Cold 层或 DB）：先回源，避免把已有 tracker 当新条目插入
                // 而把 score / 请求统计整体清零，也避免向联邦误报为新建。
                if let Some(mut entry) = self.get_tracker_sync(url) {
                    if entry.last_used.is_none_or(|t| *last_seen > t) {
                        entry.last_used = Some(*last_seen);
                    }
                    self.cache.put(url.clone(), entry);
                } else {
                    self.cache.put(
                        url.clone(),
                        TrackerEntry {
                            url: url.clone(),
                            score: 15.0,
                            disabled: false,
                            total_requests: 0,
                            success_requests: 0,
                            failed_requests: 0,
                            total_peers_discovered: 0,
                            avg_response_time_ms: 0.0,
                            consecutive_failures: 0,
                            last_used: Some(*last_seen),
                        },
                    );
                    new_urls.push(url.clone());
                }
            }
            // 标记该 tracker 为脏（数据变化，需要增量持久化）
            self.persist_dirty.write().insert(url.clone());
        }
        new_urls
    }

    fn propagate_trackers(&self, new_urls: Vec<String>) {
        if new_urls.is_empty() {
            return;
        }
        let mut built: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::with_capacity(new_urls.len());
        for url in &new_urls {
            if let Some((k, p, h)) =
                crate::federation::sync::tracker_sync::build_tracker_sync_entry(url)
            {
                built.push((k, p, h));
            }
        }
        self.propagate(repo_type::TRACKER, built);
    }

    pub fn add_tracker_sync(&self, url: String) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let new_urls = self.add_trackers_batch_internal(&[(url, now)]);
        self.propagate_trackers(new_urls);
    }

    pub fn add_trackers_sync_batch(&self, urls: &[String]) -> usize {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let items: Vec<(String, u64)> = urls.iter().map(|u| (u.clone(), now)).collect();
        let new_urls = self.add_trackers_batch_internal(&items);
        let count = new_urls.len();
        self.propagate_trackers(new_urls);
        count
    }

    pub fn get_tracker_sync(&self, url: &str) -> Option<TrackerEntry> {
        let key = url.to_string();
        if let Some(entry) = self.cache.get(&key) {
            return Some(entry);
        }
        // 缓存未命中，从 DB 加载
        if self.tier_enabled {
            if let Some(row) = self.storage.load_tracker_by_url(url).ok().flatten() {
                let entry = TrackerEntry {
                    url: row.url,
                    score: row.score,
                    disabled: row.disabled,
                    total_requests: row.total_requests,
                    success_requests: row.success_requests,
                    failed_requests: row.failed_requests,
                    total_peers_discovered: row.total_peers_discovered,
                    avg_response_time_ms: row.total_response_time_ms,
                    consecutive_failures: row.consecutive_failures,
                    last_used: None,
                };
                self.cache.put(url.to_string(), entry.clone());
                return Some(entry);
            }
        }
        None
    }

    pub fn all_trackers_sync(&self) -> Vec<TrackerEntry> {
        self.cache.values()
    }

    pub fn active_trackers_sync(&self) -> Vec<TrackerEntry> {
        self.cache
            .values()
            .into_iter()
            .filter(|t| !t.disabled)
            .collect()
    }

    pub fn top_trackers_sync(&self, n: usize) -> Vec<TrackerEntry> {
        let mut trackers: Vec<TrackerEntry> = self.active_trackers_sync();
        trackers.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        trackers.truncate(n);
        trackers
    }

    pub fn count_sync(&self) -> usize {
        self.cache.len()
    }

    pub fn update_score_sync(&self, url: &str, score: f64) {
        let key = url.to_string();
        let updated = self.cache.update_without_touch(&key, |entry| {
            entry.score = score;
        });
        if updated {
            // 评分更新标记 score_dirty，不标记 persist_dirty
            self.score_dirty.write().insert(key);
            return;
        }
        if self.tier_enabled {
            // 缓存未命中，从 DB 加载后更新
            if let Some(mut entry) = self.get_tracker_sync(url) {
                entry.score = score;
                self.cache.put(key.clone(), entry);
                self.score_dirty.write().insert(key);
            }
        }
    }

    /// 将一次 tracker 请求结果应用到条目。
    ///
    /// 命中缓存与回源 DB 两条路径共用，避免"冷 tracker 反复失败永不禁用 / 平均响应时间失真"。
    fn apply_request_stats(entry: &mut TrackerEntry, success: bool, peers: u64, latency_ms: u64) {
        entry.total_requests += 1;
        if success {
            entry.success_requests += 1;
            entry.total_peers_discovered += peers;
            entry.consecutive_failures = 0;
            if entry.avg_response_time_ms == 0.0 {
                entry.avg_response_time_ms = latency_ms as f64;
            } else {
                entry.avg_response_time_ms =
                    entry.avg_response_time_ms * 0.9 + latency_ms as f64 * 0.1;
            }
        } else {
            entry.failed_requests += 1;
            entry.consecutive_failures += 1;
            if entry.consecutive_failures >= 10 {
                entry.disabled = true;
            }
        }
        entry.last_used = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );
    }

    pub fn record_request_sync(&self, url: &str, success: bool, peers: u64, latency_ms: u64) {
        let key = url.to_string();
        let updated = self.cache.update(&key, |entry| {
            Self::apply_request_stats(entry, success, peers, latency_ms);
        });
        if !updated && self.tier_enabled {
            if let Some(mut entry) = self.get_tracker_sync(url) {
                // 与命中路径一致的统计更新
                Self::apply_request_stats(&mut entry, success, peers, latency_ms);
                self.cache.put(key, entry);
            }
        }
        self.mark_dirty_sync(url);
    }

    pub fn set_disabled_sync(&self, url: &str, disabled: bool) {
        let key = url.to_string();
        let updated = self.cache.update(&key, |entry| {
            entry.disabled = disabled;
            if !disabled {
                entry.consecutive_failures = 0;
            }
        });
        if !updated && self.tier_enabled {
            if let Some(mut entry) = self.get_tracker_sync(url) {
                entry.disabled = disabled;
                if !disabled {
                    entry.consecutive_failures = 0;
                }
                self.cache.put(key, entry);
            }
        }
        self.mark_dirty_sync(url);
    }

    /// 增量持久化：只保存 persist_dirty 的 tracker，不再全量 save_all
    pub async fn save_dirty(&self) -> anyhow::Result<()> {
        if let Some(wq) = &self.write_queue {
            // 异步模式：原子取出并清空 dirty，非阻塞入队 WriteQueue
            let dirty_urls = self.take_dirty_sync();
            if dirty_urls.is_empty() {
                return Ok(());
            }
            let batch = self.build_dirty_batch(&dirty_urls);
            if batch.is_empty() {
                return Ok(());
            }
            let wq = wq.clone();
            let count = batch.len();
            wq.send(move |conn| Storage::save_trackers_batch_in_tx(conn, &batch));
            tracing::debug!("[tracker_repo] 异步入队保存 {} 个 dirty tracker", count);
            Ok(())
        } else {
            // 同步模式：先查看 dirty（不清空），保存成功后再清空，失败则保留重试
            let dirty_urls = self.dirty_trackers_sync();
            if dirty_urls.is_empty() {
                return Ok(());
            }
            let batch = self.build_dirty_batch(&dirty_urls);
            if batch.is_empty() {
                // 内存中已不存在的 dirty tracker（可能已被删除），清理标记
                let mut persist_dirty = self.persist_dirty.write();
                for url in &dirty_urls {
                    persist_dirty.remove(url);
                }
                return Ok(());
            }
            let storage = self.storage.clone();
            let count = batch.len();
            tracing::debug!("[tracker_repo] 增量保存 {} 个 dirty tracker", count);
            tokio::task::spawn_blocking(move || {
                for t in &batch {
                    storage.save_tracker(
                        &t.url,
                        t.score,
                        t.total_requests,
                        t.success_requests,
                        t.failed_requests,
                        t.total_peers_discovered,
                        t.total_response_time_ms,
                        t.consecutive_failures,
                        t.disabled,
                    )?;
                }
                Ok::<(), anyhow::Error>(())
            })
            .await??;
            let mut persist_dirty = self.persist_dirty.write();
            for url in &dirty_urls {
                persist_dirty.remove(url);
            }
            Ok(())
        }
    }

    /// 固有 load_all 方法（兼容 main.rs 直接调用）
    pub async fn load_all(&self) -> anyhow::Result<usize> {
        // Tracker 列表量小，全量加载
        let storage = self.storage.clone();
        let rows = tokio::task::spawn_blocking(move || storage.load_trackers()).await??;
        let mut count = 0;
        for row in rows {
            self.cache.put(
                row.url.clone(),
                TrackerEntry {
                    url: row.url,
                    score: row.score,
                    disabled: row.disabled,
                    total_requests: row.total_requests,
                    success_requests: row.success_requests,
                    failed_requests: row.failed_requests,
                    total_peers_discovered: row.total_peers_discovered,
                    avg_response_time_ms: row.total_response_time_ms,
                    consecutive_failures: row.consecutive_failures,
                    last_used: None,
                },
            );
            count += 1;
        }
        Ok(count)
    }

    /// 执行分层检查 + 驱逐（由 TaskScheduler 定时调用）。
    /// 驱逐的 tracker 从 Merkle 热数据搬到冷数据（key = url.as_bytes()），
    /// 节点仍在 cold_entries/cold_roots 中，不从树删除。
    pub fn tier_evict(&self) {
        let before: std::collections::HashSet<String> =
            self.cache.iter().into_iter().map(|(k, _)| k).collect();
        self.cache.tier_check();
        self.cache.evict_if_needed();
        let after: std::collections::HashSet<String> =
            self.cache.iter().into_iter().map(|(k, _)| k).collect();
        let evicted: Vec<Vec<u8>> = before
            .difference(&after)
            .map(|url| url.as_bytes().to_vec())
            .collect();
        if !evicted.is_empty() {
            if let Some(merkle) = self.merkle.get() {
                merkle.evict_to_cold(&evicted);
            }
        }
    }

    /// 紧急驱逐（内存超限时调用）
    pub fn emergency_evict(&self, count: usize) {
        self.cache.emergency_evict(count);
    }

    /// 分层缓存统计
    pub fn cache_stats(&self) -> (usize, usize, u64) {
        self.cache.stats()
    }

    /// 数据库中 trackers 表的总行数（同步，用于监控面板）
    pub fn total_count_sync(&self) -> u64 {
        self.storage.count_table("trackers").unwrap_or(0)
    }

    // ── 持久化脏标记同步方法（用于增量持久化）──

    pub fn mark_dirty_sync(&self, url: &str) {
        self.persist_dirty.write().insert(url.to_string());
    }

    pub fn dirty_trackers_sync(&self) -> Vec<String> {
        self.persist_dirty.read().iter().cloned().collect()
    }

    /// 只清除指定 tracker 的脏标记（避免全量清除误伤并发新增的脏标记）
    pub fn clear_dirty_batch_sync(&self, urls: &[String]) {
        if urls.is_empty() {
            return;
        }
        let mut dirty = self.persist_dirty.write();
        for url in urls {
            dirty.remove(url);
        }
    }

    pub fn clear_all_dirty_sync(&self) {
        self.persist_dirty.write().clear();
    }

    /// 取出所有 persist_dirty tracker 并清空（原子操作，用于增量持久化）
    pub fn take_dirty_sync(&self) -> Vec<String> {
        let mut persist_dirty = self.persist_dirty.write();
        let urls: Vec<String> = persist_dirty.iter().cloned().collect();
        persist_dirty.clear();
        urls
    }

    /// persist_dirty tracker 数量
    pub fn dirty_count_sync(&self) -> usize {
        self.persist_dirty.read().len()
    }

    // ── 评分脏标记同步方法（用于增量评分查询）──

    /// 取出所有 score_dirty tracker 并清空（原子操作，供评分系统查询）
    pub fn take_score_dirty_sync(&self) -> Vec<String> {
        let mut score_dirty = self.score_dirty.write();
        let urls: Vec<String> = score_dirty.iter().cloned().collect();
        score_dirty.clear();
        urls
    }

    /// score_dirty tracker 数量
    pub fn score_dirty_count_sync(&self) -> usize {
        self.score_dirty.read().len()
    }

    /// 清空 score_dirty 集合
    pub fn clear_score_dirty_sync(&self) {
        self.score_dirty.write().clear();
    }

    /// 根据 dirty url 列表构建 TrackerRow 批量（从缓存读取，不修改任何状态）
    fn build_dirty_batch(&self, dirty_urls: &[String]) -> Vec<TrackerRow> {
        if dirty_urls.is_empty() {
            return Vec::new();
        }
        let url_set: FxHashSet<String> = dirty_urls.iter().cloned().collect();
        self.cache
            .iter()
            .into_iter()
            .filter(|(url, _)| url_set.contains(url))
            .map(|(_, t)| TrackerRow {
                url: t.url,
                score: t.score,
                total_requests: t.total_requests,
                success_requests: t.success_requests,
                failed_requests: t.failed_requests,
                total_peers_discovered: t.total_peers_discovered,
                total_response_time_ms: t.avg_response_time_ms,
                consecutive_failures: t.consecutive_failures,
                disabled: t.disabled,
            })
            .collect()
    }
}

#[async_trait]
impl TrackerRepository for TrackerRepoImpl {
    async fn add_tracker(&self, url: String) {
        self.add_tracker_sync(url);
    }

    async fn remove_tracker(&self, url: &str) {
        let key = url.to_string();
        self.cache.remove(&key);
        // 清除脏标记，避免后续 save_dirty 又把它写回
        self.persist_dirty.write().remove(&key);
        self.score_dirty.write().remove(&key);
        // P1-6：写 deleted_at 墓碑而非物理删除；load_trackers 已过滤墓碑，回源不会再"复活"。
        // 物理删除无法与"从来没有"区分，会破坏两端 Merkle 的删除闭环。
        if let Err(e) = self.storage.soft_delete_tracker(url) {
            tracing::warn!("[tracker_repo] 软删除 tracker 失败: {}", e);
        }
        // 联动 Merkle：标记分片 dirty，联邦重算时同步删除
        if let Some(merkle) = self.merkle.get() {
            merkle.mark_tombstone(url.as_bytes());
        }
        // P1-2：删除记入 oplog（delta 通道传播删除）；失败只告警。
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let items = vec![SyncEntry {
            key: url.as_bytes().to_vec(),
            operation: operation::DELETE,
            version: now,
            payload: Vec::new(),
        }];
        crate::storage::oplog::record_local_ops(
            &self.storage,
            crate::federation::protocol::repo_type::TRACKER,
            &items,
        );
    }

    async fn get_tracker(&self, url: &str) -> Option<TrackerEntry> {
        self.get_tracker_sync(url)
    }

    async fn all_trackers(&self) -> Vec<TrackerEntry> {
        self.all_trackers_sync()
    }

    async fn active_trackers(&self) -> Vec<TrackerEntry> {
        self.active_trackers_sync()
    }

    async fn top_trackers(&self, n: usize) -> Vec<TrackerEntry> {
        self.top_trackers_sync(n)
    }

    async fn count(&self) -> usize {
        self.count_sync()
    }

    async fn update_score(&self, url: &str, score: f64) {
        self.update_score_sync(url, score);
    }

    async fn update_scores_batch(&self, scores: &[(String, f64)]) {
        let mut updated: Vec<String> = Vec::with_capacity(scores.len());
        for (url, score) in scores {
            let ok = self.cache.update_without_touch(url, |entry| {
                entry.score = *score;
            });
            if ok {
                updated.push(url.clone());
            }
        }
        if !updated.is_empty() {
            let mut score_dirty = self.score_dirty.write();
            for url in updated {
                score_dirty.insert(url);
            }
        }
    }

    async fn mark_dirty(&self, url: &str) {
        self.mark_dirty_sync(url);
    }

    async fn dirty_trackers(&self) -> Vec<String> {
        self.dirty_trackers_sync()
    }

    async fn clear_dirty_batch(&self, urls: &[String]) {
        self.clear_dirty_batch_sync(urls);
    }

    async fn clear_all_dirty(&self) {
        self.clear_all_dirty_sync();
    }

    async fn record_request(&self, url: &str, success: bool, peers: u64, latency_ms: u64) {
        self.record_request_sync(url, success, peers, latency_ms);
    }

    async fn set_disabled(&self, url: &str, disabled: bool) {
        self.set_disabled_sync(url, disabled);
    }

    async fn save_all(&self) -> anyhow::Result<()> {
        let trackers = self.all_trackers_sync();
        if trackers.is_empty() {
            return Ok(());
        }
        let batch: Vec<TrackerRow> = trackers
            .into_iter()
            .map(|t| TrackerRow {
                url: t.url,
                score: t.score,
                total_requests: t.total_requests,
                success_requests: t.success_requests,
                failed_requests: t.failed_requests,
                total_peers_discovered: t.total_peers_discovered,
                total_response_time_ms: t.avg_response_time_ms,
                consecutive_failures: t.consecutive_failures,
                disabled: t.disabled,
            })
            .collect();

        if let Some(wq) = &self.write_queue {
            let count = batch.len();
            wq.send(move |conn| Storage::save_trackers_batch_in_tx(conn, &batch));
            tracing::debug!("[tracker_repo] 异步入队保存 {} 个 tracker", count);
            Ok(())
        } else {
            let storage = self.storage.clone();
            tokio::task::spawn_blocking(move || {
                for t in &batch {
                    storage.save_tracker(
                        &t.url,
                        t.score,
                        t.total_requests,
                        t.success_requests,
                        t.failed_requests,
                        t.total_peers_discovered,
                        t.total_response_time_ms,
                        t.consecutive_failures,
                        t.disabled,
                    )?;
                }
                Ok::<(), anyhow::Error>(())
            })
            .await??;
            Ok(())
        }
    }

    async fn load_all(&self) -> anyhow::Result<usize> {
        TrackerRepoImpl::load_all(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_repo() -> TrackerRepoImpl {
        let storage = Arc::new(Storage::memory().unwrap());
        TrackerRepoImpl::new(storage)
    }

    #[tokio::test]
    async fn test_add_and_get() {
        let repo = test_repo();
        let url = "http://example.com/announce".to_string();
        repo.add_tracker(url.clone()).await;
        assert_eq!(repo.count().await, 1);
        let tracker = repo.get_tracker(&url).await;
        assert!(tracker.is_some());
        assert_eq!(tracker.unwrap().url, url);
    }

    #[tokio::test]
    async fn test_persistence() {
        let repo = test_repo();
        let url = "http://test.com/announce".to_string();
        repo.add_tracker(url.clone()).await;
        repo.save_all().await.unwrap();

        let storage = repo.storage.clone();
        let repo2 = TrackerRepoImpl::new(storage);
        let loaded = repo2.load_all().await.unwrap();
        assert!(loaded >= 1);
        assert!(repo2.get_tracker(&url).await.is_some());
    }

    #[tokio::test]
    async fn test_record_request() {
        let repo = test_repo();
        let url = "http://req.com/announce".to_string();
        repo.add_tracker(url.clone()).await;
        repo.record_request(&url, true, 10, 50).await;

        let tracker = repo.get_tracker(&url).await.unwrap();
        assert_eq!(tracker.total_requests, 1);
        assert_eq!(tracker.success_requests, 1);
        assert_eq!(tracker.total_peers_discovered, 10);
    }

    #[tokio::test]
    async fn test_cache_miss_load_from_db() {
        let repo = test_repo();
        let url = "http://cold.com/announce".to_string();
        repo.add_tracker(url.clone()).await;
        repo.save_all().await.unwrap();

        // 从缓存移除
        repo.cache.remove(&url);
        assert!(repo.cache.get(&url).is_none());

        // get_tracker 应从 DB 按需加载
        let tracker = repo.get_tracker(&url).await;
        assert!(tracker.is_some());
        assert!(repo.cache.contains(&url));
    }
}
