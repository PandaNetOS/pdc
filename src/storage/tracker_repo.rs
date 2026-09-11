//! TrackerRepository 实现
//!
//! 封装 tracker 状态 FxHashMap + SQLite 持久化。
//! 使用 parking_lot::RwLock（同步），与 NodeRepo/PeerRepo 一致。
//! 千万级性能优化：FxHashMap 替代 std::HashMap。

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;

use crate::federation::gossip::GossipEngine;
use crate::federation::merkle::MerkleTree;
use crate::federation::protocol::{operation, repo_type, SyncEntry};

use crate::storage::db::Storage;
use crate::storage::repo_traits::{TrackerEntry, TrackerRepository};

struct TrackerCacheInner {
    /// url -> TrackerEntry
    entries: FxHashMap<String, TrackerEntry>,
}

impl TrackerCacheInner {
    fn new() -> Self {
        Self {
            entries: FxHashMap::default(),
        }
    }
}

pub struct TrackerRepoImpl {
    cache: RwLock<TrackerCacheInner>,
    storage: Arc<Storage>,
    /// 联邦引用（OnceLock 注入；未设置时本地写入不触发 Merkle/Gossip，repo 正常工作）
    merkle: OnceLock<Arc<MerkleTree>>,
    gossip: OnceLock<Arc<GossipEngine>>,
}

impl TrackerRepoImpl {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            cache: RwLock::new(TrackerCacheInner::new()),
            storage,
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

    // ── 同步便捷方法（高频调用，避免 async 开销）──

    /// 内部写入：批量新增 tracker，不触发 Merkle/Gossip。
    /// 返回真正新增的 url 列表。
    /// 联邦同步入站（apply_tracker_sync）调用本方法，避免 Merkle 重复更新与 Gossip 回环。
    pub(crate) fn add_trackers_batch_internal(&self, items: &[(String, u64)]) -> Vec<String> {
        if items.is_empty() {
            return Vec::new();
        }
        let mut cache = self.cache.write();
        let mut new_urls: Vec<String> = Vec::new();
        for (url, last_seen) in items {
            if let Some(existing) = cache.entries.get_mut(url) {
                // 已有条目：更新 last_used 取较大值
                if existing.last_used.map_or(true, |t| *last_seen > t) {
                    existing.last_used = Some(*last_seen);
                }
            } else {
                cache.entries.insert(
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
        new_urls
    }

    /// 把新 tracker 列表构建成 merkle/gossip 条目并传播（新 tracker disabled=false）。
    fn propagate_trackers(&self, new_urls: Vec<String>) {
        if new_urls.is_empty() {
            return;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut built: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(new_urls.len());
        for url in &new_urls {
            if let Some((k, p)) =
                crate::federation::sync::tracker_sync::build_tracker_sync_entry(url, false, now)
            {
                built.push((k, p));
            }
        }
        self.propagate(repo_type::TRACKER, built);
    }

    /// 同步加入单个 tracker（本地路径）：新 tracker 更新 Merkle + 提交 Gossip。
    pub fn add_tracker_sync(&self, url: String) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let new_urls = self.add_trackers_batch_internal(&[(url, now)]);
        self.propagate_trackers(new_urls);
    }

    /// 批量加入 tracker（一次 cache 写锁），返回新加入的 tracker 数。
    /// 本地写入路径：新 tracker 更新 Merkle + 提交 Gossip。
    /// 联邦同步入站（apply_tracker_sync）请改用 add_trackers_batch_internal，避免回环。
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
        self.cache.read().entries.get(url).cloned()
    }

    pub fn all_trackers_sync(&self) -> Vec<TrackerEntry> {
        self.cache.read().entries.values().cloned().collect()
    }

    pub fn active_trackers_sync(&self) -> Vec<TrackerEntry> {
        self.cache
            .read()
            .entries
            .values()
            .filter(|t| !t.disabled)
            .cloned()
            .collect()
    }

    pub fn top_trackers_sync(&self, n: usize) -> Vec<TrackerEntry> {
        let mut trackers: Vec<TrackerEntry> = self.active_trackers_sync();
        trackers.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        trackers.truncate(n);
        trackers
    }

    pub fn count_sync(&self) -> usize {
        self.cache.read().entries.len()
    }

    pub fn update_score_sync(&self, url: &str, score: f64) {
        if let Some(entry) = self.cache.write().entries.get_mut(url) {
            entry.score = score;
        }
    }

    pub fn record_request_sync(&self, url: &str, success: bool, peers: u64, latency_ms: u64) {
        let mut cache = self.cache.write();
        if let Some(entry) = cache.entries.get_mut(url) {
            entry.total_requests += 1;
            if success {
                entry.success_requests += 1;
                entry.total_peers_discovered += peers;
                entry.consecutive_failures = 0;
                // 指数移动平均延迟
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
    }

    pub fn set_disabled_sync(&self, url: &str, disabled: bool) {
        if let Some(entry) = self.cache.write().entries.get_mut(url) {
            entry.disabled = disabled;
            if !disabled {
                entry.consecutive_failures = 0;
            }
        }
    }
}

#[async_trait]
impl TrackerRepository for TrackerRepoImpl {
    async fn add_tracker(&self, url: String) {
        self.add_tracker_sync(url);
    }

    async fn remove_tracker(&self, url: &str) {
        self.cache.write().entries.remove(url);
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

    async fn record_request(&self, url: &str, success: bool, peers: u64, latency_ms: u64) {
        self.record_request_sync(url, success, peers, latency_ms);
    }

    async fn set_disabled(&self, url: &str, disabled: bool) {
        self.set_disabled_sync(url, disabled);
    }

    async fn save_all(&self) -> anyhow::Result<()> {
        let trackers = self.all_trackers_sync();
        let storage = self.storage.clone();
        tokio::task::spawn_blocking(move || {
            for t in &trackers {
                storage.save_tracker(
                    &t.url,
                    t.score,
                    t.total_requests,
                    t.success_requests,
                    t.failed_requests,
                    t.total_peers_discovered,
                    t.avg_response_time_ms,
                    t.consecutive_failures,
                    t.disabled,
                )?;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    async fn load_all(&self) -> anyhow::Result<usize> {
        let rows = self.storage.load_trackers()?;
        let mut cache = self.cache.write();
        let mut count = 0;
        for row in rows {
            cache.entries.insert(
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
}
