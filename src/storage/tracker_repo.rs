//! TrackerRepository 实现
//!
//! 封装 tracker 状态 FxHashMap + SQLite 持久化。
//! 使用 parking_lot::RwLock（同步），与 NodeRepo/PeerRepo 一致。
//! 千万级性能优化：FxHashMap 替代 std::HashMap。

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;

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
}

impl TrackerRepoImpl {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            cache: RwLock::new(TrackerCacheInner::new()),
            storage,
        }
    }

    // ── 同步便捷方法（高频调用，避免 async 开销）──

    pub fn add_tracker_sync(&self, url: String) {
        let mut cache = self.cache.write();
        if !cache.entries.contains_key(&url) {
            cache.entries.insert(
                url.clone(),
                TrackerEntry {
                    url,
                    score: 15.0,
                    disabled: false,
                    total_requests: 0,
                    success_requests: 0,
                    failed_requests: 0,
                    total_peers_discovered: 0,
                    avg_response_time_ms: 0.0,
                    consecutive_failures: 0,
                    last_used: None,
                },
            );
        }
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
