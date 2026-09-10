//! TrackerRepo 同步
//!
//! 通过 Gossip 协议传播 Tracker 列表变更，支持 Merkle 全量对账。

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, info};

use crate::federation::gossip::GossipEngine;
use crate::federation::merkle::MerkleTree;
use crate::federation::metrics::FederationMetrics;
use crate::federation::node_id::NodeId;
use crate::federation::protocol::*;
use crate::storage::TrackerRepoImpl;

/// Tracker 同步负载
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TrackerSyncPayload {
    url: String,
    disabled: bool,
}

/// TrackerRepo 同步服务
pub struct TrackerSync {
    tracker_repo: Arc<TrackerRepoImpl>,
    gossip_engine: Arc<GossipEngine>,
    merkle: Arc<MerkleTree>,
    metrics: Arc<FederationMetrics>,
    last_full_sync: parking_lot::RwLock<Instant>,
    enabled: bool,
    shutdown: broadcast::Sender<()>,
}

impl TrackerSync {
    pub fn new(
        tracker_repo: Arc<TrackerRepoImpl>,
        gossip_engine: Arc<GossipEngine>,
        merkle: Arc<MerkleTree>,
        metrics: Arc<FederationMetrics>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            tracker_repo,
            gossip_engine,
            merkle,
            metrics,
            last_full_sync: parking_lot::RwLock::new(Instant::now()),
            enabled: true,
            shutdown,
        }
    }

    /// 启动全量对账后台任务（每小时）
    pub fn spawn_full_sync(self: Arc<Self>) {
        if !self.enabled {
            return;
        }
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(3600));
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self.do_full_sync();
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] Tracker 全量同步任务收到关闭信号");
                        break;
                    }
                }
            }
        });
        info!("[federation] Tracker 全量同步任务已启动（间隔 3600s）");
    }

    /// 执行全量对账
    fn do_full_sync(&self) {
        let trackers = self.tracker_repo.all_trackers_sync();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut entries = Vec::with_capacity(trackers.len());
        let mut merkle_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(trackers.len());
        for tracker in &trackers {
            let payload = TrackerSyncPayload {
                url: tracker.url.clone(),
                disabled: tracker.disabled,
            };
            let payload_bytes = match bincode::serialize(&payload) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let key = tracker.url.as_bytes().to_vec();
            merkle_batch.push((key.clone(), payload_bytes.clone()));
            entries.push(SyncEntry {
                key,
                operation: operation::UPSERT,
                version: now,
                payload: payload_bytes,
            });
        }

        // 批量更新 Merkle
        if !merkle_batch.is_empty() {
            let refs: Vec<(&[u8], &[u8])> = merkle_batch
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice()))
                .collect();
            self.merkle.update_batch(&refs);
        }

        *self.last_full_sync.write() = Instant::now();

        if !entries.is_empty() {
            self.gossip_engine
                .submit_gossip(repo_type::TRACKER, entries);
            debug!(
                "[federation] Tracker 全量同步: {} 个 tracker 已提交 Gossip",
                trackers.len()
            );
        }
    }

    /// Tracker 变更时即时 Gossip 传播
    pub fn submit_tracker_change(&self, url: &str, operation: u8) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let key = url.as_bytes().to_vec();
        let payload = if operation == operation::UPSERT {
            let tracker = self.tracker_repo.get_tracker_sync(url);
            let p = TrackerSyncPayload {
                url: url.to_string(),
                disabled: tracker.map(|t| t.disabled).unwrap_or(false),
            };
            match bincode::serialize(&p) {
                Ok(b) => b,
                Err(_) => return,
            }
        } else {
            Vec::new()
        };

        if operation == operation::DELETE {
            self.merkle.remove(&key);
        } else {
            self.merkle.update(&key, &payload);
        }

        let entry = SyncEntry {
            key,
            operation,
            version: now,
            payload,
        };

        self.gossip_engine
            .submit_gossip(repo_type::TRACKER, vec![entry]);
        debug!("[federation] Tracker 变更传播: url={}, op={}", url, operation);
    }

    /// 应用收到的 Tracker 同步数据
    pub fn apply_tracker_sync(&self, entries: &[SyncEntry]) {
        // 第一遍：过滤 DELETE / 反序列化失败，收集 url 并批量更新 Merkle
        let mut urls: Vec<String> = Vec::new();
        let mut merkle_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut applied = 0;
        for entry in entries {
            if entry.operation == operation::DELETE {
                // TrackerRepoImpl 没有 remove 方法，删除操作忽略
                continue;
            }
            let payload: TrackerSyncPayload = match bincode::deserialize(&entry.payload) {
                Ok(p) => p,
                Err(_) => continue,
            };

            merkle_batch.push((entry.key.clone(), entry.payload.clone()));
            urls.push(payload.url);
            applied += 1;
        }

        // 批量更新 Merkle
        if !merkle_batch.is_empty() {
            let refs: Vec<(&[u8], &[u8])> = merkle_batch
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice()))
                .collect();
            self.merkle.update_batch(&refs);
        }

        // 第二遍：一次写锁批量写入，替代逐条 add_tracker_sync
        if !urls.is_empty() {
            self.tracker_repo.add_trackers_sync_batch(&urls);
        }

        if applied > 0 {
            self.metrics.record_sync_entries(applied as u64);
            self.metrics.record_tracker_sync(applied as u64);
            debug!("[federation] Tracker 同步应用 {} 条", applied);
        }
    }

    /// 获取 Merkle 树引用（用于反熵对账）
    pub fn merkle(&self) -> Arc<MerkleTree> {
        self.merkle.clone()
    }

    /// 收集全量 Tracker 同步条目（用于初始全量同步）
    ///
    /// 遍历所有 tracker，构建 SyncEntry 列表。
    /// 注意：此方法不更新 Merkle 树，避免在遍历大量数据时持有锁导致死锁。
    pub fn collect_all_entries(&self) -> Vec<SyncEntry> {
        let trackers = self.tracker_repo.all_trackers_sync();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut entries = Vec::with_capacity(trackers.len());
        for tracker in &trackers {
            let payload = TrackerSyncPayload {
                url: tracker.url.clone(),
                disabled: tracker.disabled,
            };
            let payload_bytes = match bincode::serialize(&payload) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let key = tracker.url.as_bytes().to_vec();
            entries.push(SyncEntry {
                key,
                operation: operation::UPSERT,
                version: now,
                payload: payload_bytes,
            });
        }

        entries
    }

    /// 处理 Merkle 摘要（对账）
    pub fn handle_merkle_digest(&self, _from_node: NodeId, digest: &MerkleDigestMessage) {
        let diffs = self.merkle.diff(digest);
        if diffs.is_empty() {
            debug!("[federation] Tracker Merkle 对账：无差异");
            return;
        }

        debug!(
            "[federation] Tracker Merkle 对账：{} 个分片有差异，触发全量同步",
            diffs.len()
        );
        // 简化处理：有差异就触发全量同步
        let trackers = self.tracker_repo.all_trackers_sync();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut entries = Vec::new();
        for tracker in &trackers {
            let payload = TrackerSyncPayload {
                url: tracker.url.clone(),
                disabled: tracker.disabled,
            };
            if let Ok(payload_bytes) = bincode::serialize(&payload) {
                entries.push(SyncEntry {
                    key: tracker.url.as_bytes().to_vec(),
                    operation: operation::UPSERT,
                    version: now,
                    payload: payload_bytes,
                });
            }
        }
        if !entries.is_empty() {
            self.gossip_engine
                .submit_gossip(repo_type::TRACKER, entries);
        }
    }

    /// 获取 Merkle 摘要
    pub fn merkle_digest(&self) -> MerkleDigestMessage {
        self.merkle.digest(repo_type::TRACKER)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
use crate::federation::config::FederationConfig;
use crate::federation::connection::ConnectionManager;
use crate::federation::node_id::NodeIdentity;
    use crate::federation::node_table::NodeTable;
    use crate::storage::Storage;

    fn make_tracker_repo() -> Arc<TrackerRepoImpl> {
        let storage = Arc::new(Storage::memory().unwrap());
        Arc::new(TrackerRepoImpl::new(storage))
    }

    fn make_gossip_engine(shutdown_tx: broadcast::Sender<()>) -> Arc<GossipEngine> {
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (cm_shutdown, _) = broadcast::channel(1);
        let cm = Arc::new(ConnectionManager::new(
            node_table,
            identity,
            FederationConfig::default(),
            cm_shutdown,
            Arc::new(FederationMetrics::new()),
        ));
        let metrics = Arc::new(FederationMetrics::new());
        Arc::new(GossipEngine::new(
            cm,
            FederationConfig::default(),
            NodeId([1; 20]),
            metrics,
            shutdown_tx,
        ))
    }

    #[test]
    fn test_tracker_sync_payload_serde() {
        let payload = TrackerSyncPayload {
            url: "http://tracker.example.com:6969/announce".to_string(),
            disabled: false,
        };
        let bytes = bincode::serialize(&payload).unwrap();
        let decoded: TrackerSyncPayload = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.url, "http://tracker.example.com:6969/announce");
        assert!(!decoded.disabled);
    }

    #[test]
    fn test_apply_tracker_sync() {
        let repo = make_tracker_repo();
        let (shutdown_tx, _) = broadcast::channel(1);
        let gossip = make_gossip_engine(shutdown_tx.clone());
        let merkle = Arc::new(MerkleTree::new(16));
        let metrics = Arc::new(FederationMetrics::new());
        let tracker_sync = TrackerSync::new(
            repo.clone(),
            gossip,
            merkle,
            metrics,
            shutdown_tx,
        );

        assert_eq!(repo.count_sync(), 0);

        let payload = TrackerSyncPayload {
            url: "http://test.tracker:6969/announce".to_string(),
            disabled: false,
        };
        let entries = vec![SyncEntry {
            key: b"http://test.tracker:6969/announce".to_vec(),
            operation: operation::UPSERT,
            version: 1,
            payload: bincode::serialize(&payload).unwrap(),
        }];

        tracker_sync.apply_tracker_sync(&entries);
        assert_eq!(repo.count_sync(), 1);
    }

    #[test]
    fn test_submit_tracker_change() {
        let repo = make_tracker_repo();
        repo.add_tracker_sync("http://change.tracker:6969".to_string());

        let (shutdown_tx, _) = broadcast::channel(1);
        let gossip = make_gossip_engine(shutdown_tx.clone());
        let merkle = Arc::new(MerkleTree::new(16));
        let metrics = Arc::new(FederationMetrics::new());
        let tracker_sync = TrackerSync::new(
            repo,
            gossip.clone(),
            merkle,
            metrics,
            shutdown_tx,
        );

        // 提交变更，应加入 gossip outbox
        tracker_sync.submit_tracker_change("http://change.tracker:6969", operation::UPSERT);
        assert_eq!(gossip.outbox_size(), 1);
    }

    #[test]
    fn test_merkle_digest() {
        let repo = make_tracker_repo();
        repo.add_tracker_sync("http://digest.tracker:6969".to_string());

        let (shutdown_tx, _) = broadcast::channel(1);
        let gossip = make_gossip_engine(shutdown_tx.clone());
        let merkle = Arc::new(MerkleTree::new(16));
        let metrics = Arc::new(FederationMetrics::new());
        let tracker_sync = TrackerSync::new(
            repo,
            gossip,
            merkle.clone(),
            metrics,
            shutdown_tx,
        );

        // 先做一次全量同步更新 merkle
        tracker_sync.do_full_sync();

        let digest = tracker_sync.merkle_digest();
        assert_eq!(digest.repo_type, repo_type::TRACKER);
        assert_eq!(digest.shard_count, 16);
    }
}
