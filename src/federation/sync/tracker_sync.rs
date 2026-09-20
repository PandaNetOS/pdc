//! TrackerRepo 同步
//!
//! 通过 Gossip 协议传播 Tracker 列表变更，支持 Merkle 全量对账。

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::debug;

use crate::federation::gossip::GossipEngine;
use crate::federation::merkle::MerkleTree;
use crate::federation::metrics::FederationMetrics;
use crate::federation::node_id::NodeId;
use crate::federation::protocol::*;
use crate::federation::sync::merkle_updater::MerkleUpdateQueue;
use crate::storage::TrackerRepoImpl;

/// Tracker 同步负载
///
/// 联邦同步只传播 url 字段；disabled / last_seen 属于各节点本地状态，
/// 不同步（避免时间戳差异导致 Merkle 哈希永远对不上）。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TrackerSyncPayload {
    url: String,
}

/// 构建 Tracker 同步条目的 (key, payload_bytes, data_hash)。
/// 格式与 collect_all_entries 一致，供 TrackerRepoImpl 本地写入后更新 Merkle / 提交 Gossip。
///
/// data_hash 公式与 db.rs `load_all_tracker_keys_hashes` 完全一致：
/// `blake3(url)`，保证冷热同构。
pub(crate) fn build_tracker_sync_entry(url: &str) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let payload = TrackerSyncPayload {
        url: url.to_string(),
    };
    let payload_bytes = bincode::serialize(&payload).ok()?;
    let key = url.as_bytes().to_vec();
    // data_hash = blake3(url)
    let data_hash = blake3::hash(url.as_bytes()).as_bytes().to_vec();
    Some((key, payload_bytes, data_hash))
}

/// 从已序列化的 TrackerSyncPayload 计算 data_hash（与 build_tracker_sync_entry 公式一致）。
pub(crate) fn data_hash_from_payload(payload: &[u8]) -> Option<Vec<u8>> {
    let p: TrackerSyncPayload = bincode::deserialize(payload).ok()?;
    Some(blake3::hash(p.url.as_bytes()).as_bytes().to_vec())
}

/// TrackerRepo 同步服务
pub struct TrackerSync {
    tracker_repo: Arc<TrackerRepoImpl>,
    gossip_engine: Arc<GossipEngine>,
    merkle: Arc<MerkleTree>,
    #[allow(dead_code)]
    merkle_queue: Arc<MerkleUpdateQueue>,
    metrics: Arc<FederationMetrics>,
    last_full_sync: parking_lot::RwLock<Instant>,
    _enabled: bool,
    _shutdown: broadcast::Sender<()>,
}

impl TrackerSync {
    pub fn new(
        tracker_repo: Arc<TrackerRepoImpl>,
        gossip_engine: Arc<GossipEngine>,
        merkle: Arc<MerkleTree>,
        #[allow(dead_code)] merkle_queue: Arc<MerkleUpdateQueue>,
        metrics: Arc<FederationMetrics>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            tracker_repo,
            gossip_engine,
            merkle,
            merkle_queue,
            metrics,
            last_full_sync: parking_lot::RwLock::new(Instant::now()),
            _enabled: true,
            _shutdown: shutdown,
        }
    }

    /// 启动全量对账后台任务（每小时）
    ///
    /// 已迁移：周期性 tick 由 TaskScheduler 统一调度，调用 [`TrackerSync::do_full_sync`]。
    /// 保留空壳以兼容 mod.rs 中的旧调用，待 start() 统一清理后移除。
    pub fn spawn_full_sync(self: Arc<Self>) {
        // [MIGRATED] 内部 spawn+interval 循环已迁移到 TaskScheduler，由 main.rs 统一注册。
        // 周期 tick 入口: pub(crate) fn do_full_sync(&self)
    }

    /// 执行全量对账
    ///
    /// 周期性 tick 入口，已迁移到 TaskScheduler 统一调度。
    pub fn do_full_sync(&self) {
        let trackers = self.tracker_repo.all_trackers_sync();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut entries = Vec::with_capacity(trackers.len());
        for tracker in &trackers {
            let (key, payload_bytes, _data_hash) = match build_tracker_sync_entry(&tracker.url) {
                Some(v) => v,
                None => continue,
            };
            entries.push(SyncEntry {
                key,
                operation: operation::UPSERT,
                version: now,
                payload: payload_bytes,
            });
        }

        // Merkle 哈希不再从内存喂入：由周期冷重算任务从 DB 重算（见 main.rs 冷重算）。

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
            match build_tracker_sync_entry(url) {
                Some((_key, payload_bytes, _hash)) => payload_bytes,
                None => return,
            }
        } else {
            Vec::new()
        };

        if operation == operation::DELETE {
            // 真正删除：标记墓碑（下次冷重算排除），不从热数据物理删除
            self.merkle.mark_tombstone(&key);
        } else {
            let data_hash = data_hash_from_payload(&payload).unwrap_or_default();
            self.merkle.update(&key, &payload, &data_hash);
        }

        let entry = SyncEntry {
            key,
            operation,
            version: now,
            payload,
        };

        self.gossip_engine
            .submit_gossip(repo_type::TRACKER, vec![entry]);
        debug!(
            "[federation] Tracker 变更传播: url={}, op={}",
            url, operation
        );
    }

    /// 应用收到的 Tracker 同步数据
    ///
    /// 联邦同步只传播 url：入站仅新增本地不存在的 tracker，
    /// 不覆盖本地的 disabled 状态与 last_seen 时间戳。
    pub fn apply_tracker_sync(&self, entries: &[SyncEntry]) {
        // 第一遍：过滤 DELETE / 反序列化失败，收集 url 并批量更新 Merkle
        // 时间戳传 0：仅用于"新增"，update 闭包里 0 不会大于本地真实时间，
        // 因此不会覆盖本地已存在 tracker 的 last_seen；disabled 字段本地自行维护。
        let mut items: Vec<(String, u64)> = Vec::new();
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

            items.push((payload.url, 0));
            applied += 1;
        }

        // 第二遍：一次写锁批量写入（调用内部方法，不触发 Merkle/Gossip，避免回环）
        if !items.is_empty() {
            self.tracker_repo.add_trackers_batch_internal(&items);
        }

        // 【回环修复】入站 apply 不再 update_incremental_batch 标记 dirty（避免整批推回对端）。

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
            let (key, payload_bytes, _hash) = match build_tracker_sync_entry(&tracker.url) {
                Some(v) => v,
                None => continue,
            };
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
            if let Some((key, payload_bytes, _hash)) = build_tracker_sync_entry(&tracker.url) {
                entries.push(SyncEntry {
                    key,
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
    use crate::federation::session::SessionsHandle;
    use crate::storage::Storage;

    fn make_tracker_repo() -> Arc<TrackerRepoImpl> {
        let storage = Arc::new(Storage::memory().unwrap());
        Arc::new(TrackerRepoImpl::new(storage))
    }

    fn make_gossip_engine(shutdown_tx: broadcast::Sender<()>) -> Arc<GossipEngine> {
        let cm = SessionsHandle::new_for_test();
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
        };
        let bytes = bincode::serialize(&payload).unwrap();
        let decoded: TrackerSyncPayload = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.url, "http://tracker.example.com:6969/announce");
    }

    #[test]
    fn test_apply_tracker_sync() {
        let repo = make_tracker_repo();
        let (shutdown_tx, _) = broadcast::channel(1);
        let gossip = make_gossip_engine(shutdown_tx.clone());
        let merkle = Arc::new(MerkleTree::new(16));
        let metrics = Arc::new(FederationMetrics::new());
        let queue = Arc::new(MerkleUpdateQueue::new());
        let tracker_sync =
            TrackerSync::new(repo.clone(), gossip, merkle, queue, metrics, shutdown_tx);

        assert_eq!(repo.count_sync(), 0);

        let payload = TrackerSyncPayload {
            url: "http://test.tracker:6969/announce".to_string(),
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
        let queue = Arc::new(MerkleUpdateQueue::new());
        let tracker_sync =
            TrackerSync::new(repo, gossip.clone(), merkle, queue, metrics, shutdown_tx);

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
        let queue = Arc::new(MerkleUpdateQueue::new());
        let tracker_sync =
            TrackerSync::new(repo, gossip, merkle.clone(), queue, metrics, shutdown_tx);

        // 先做一次全量同步更新 merkle
        tracker_sync.do_full_sync();

        let digest = tracker_sync.merkle_digest();
        assert_eq!(digest.repo_type, repo_type::TRACKER);
        assert_eq!(digest.shard_count, 16);
    }
}
