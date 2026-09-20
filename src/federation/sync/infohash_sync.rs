//! InfohashRepo 同步
//!
//! 订阅 EventBus 的 InfohashSeen 事件，通过 Gossip 协议传播 infohash 存在性。

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::debug;

use crate::event_bus::EventBus;
use crate::federation::gossip::GossipEngine;
use crate::federation::merkle::MerkleTree;
use crate::federation::metrics::FederationMetrics;
use crate::federation::protocol::*;
use crate::federation::sync::merkle_updater::MerkleUpdateQueue;
use crate::storage::InfohashRepoImpl;
use crate::types::{Event, Infohash};

/// Infohash 同步负载（只同步事实：infohash 存在性）
#[derive(Debug, Clone, Serialize, Deserialize)]
struct InfohashSyncPayload {
    infohash: Infohash,
}

/// 构建 Infohash 同步条目的 (key, payload_bytes, data_hash)。
/// 格式与 collect_all_entries 一致，供 InfohashRepoImpl 本地写入后更新 Merkle / 提交 Gossip。
///
/// data_hash 公式与 db.rs `load_all_infohash_keys_hashes` 完全一致：
/// `blake3(infohash)`，保证冷热同构。
pub(crate) fn build_infohash_sync_entry(infohash: Infohash) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let payload = InfohashSyncPayload { infohash };
    let payload_bytes = bincode::serialize(&payload).ok()?;
    let key = infohash.to_vec();
    // data_hash = blake3(infohash)
    let data_hash = blake3::hash(infohash.as_slice()).as_bytes().to_vec();
    Some((key, payload_bytes, data_hash))
}

/// 从已序列化的 InfohashSyncPayload 计算 data_hash（与 build_infohash_sync_entry 公式一致）。
#[allow(dead_code)]
pub(crate) fn data_hash_from_payload(payload: &[u8]) -> Option<Vec<u8>> {
    let p: InfohashSyncPayload = bincode::deserialize(payload).ok()?;
    Some(blake3::hash(p.infohash.as_slice()).as_bytes().to_vec())
}

/// InfohashRepo 同步服务
pub struct InfohashSync {
    infohash_repo: Arc<InfohashRepoImpl>,
    _gossip_engine: Arc<GossipEngine>,
    merkle: Arc<MerkleTree>,
    #[allow(dead_code)]
    merkle_queue: Arc<MerkleUpdateQueue>,
    metrics: Arc<FederationMetrics>,
    enabled: bool,
    shutdown: broadcast::Sender<()>,
}

impl InfohashSync {
    pub fn new(
        infohash_repo: Arc<InfohashRepoImpl>,
        _gossip_engine: Arc<GossipEngine>,
        merkle: Arc<MerkleTree>,
        #[allow(dead_code)] merkle_queue: Arc<MerkleUpdateQueue>,
        metrics: Arc<FederationMetrics>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            infohash_repo,
            _gossip_engine,
            merkle,
            merkle_queue,
            metrics,
            enabled: true,
            shutdown,
        }
    }

    /// 启动事件消费者
    pub fn spawn_event_consumer(self: Arc<Self>, event_bus: EventBus) {
        if !self.enabled {
            return;
        }
        let mut rx = event_bus.subscribe();
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = rx.recv() => {
                        match result {
                            Ok(event) => {
                                self.clone().handle_event(event);
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                debug!("[federation] InfohashSync 事件滞后，丢弃 {} 个", n);
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                break;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] InfohashSync 事件消费者收到关闭信号");
                        break;
                    }
                }
            }
        });
        debug!("[federation] InfohashSync 事件消费者已启动");
    }

    /// 处理事件（已退役）：本地 infohash 写入已由 InfohashRepoImpl.register_sync 统一更新
    /// Merkle + 提交 Gossip，此处不再重复传播，避免双发。
    fn handle_event(self: Arc<Self>, _event: Event) {}

    /// 应用收到的 Infohash 同步数据
    ///
    /// 联邦同步只传播事实（infohash 存在性）：入站用默认 source="federation"
    /// 和当前时间注册，不覆盖本地 infohash 的 source / last_seen 状态。
    pub fn apply_infohash_sync(&self, entries: &[SyncEntry]) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // 第一遍：过滤 DELETE / 反序列化失败，收集 (infohash, source, last_seen)
        let mut items: Vec<(Infohash, String, u64)> = Vec::new();
        let mut applied = 0;
        for entry in entries {
            if entry.operation == operation::DELETE {
                continue;
            }
            let payload: InfohashSyncPayload = match bincode::deserialize(&entry.payload) {
                Ok(p) => p,
                Err(_) => continue,
            };

            items.push((payload.infohash, "federation".to_string(), now));
            applied += 1;
        }

        // 第二遍：一次写锁批量注册（调用内部方法，不触发 Merkle/Gossip，避免回环）
        if !items.is_empty() {
            self.infohash_repo.register_batch_internal(&items);
        }

        // 【回环修复】入站 apply 不再 update_incremental_batch 标记 dirty（避免整批推回对端）。

        if applied > 0 {
            self.metrics.record_sync_entries(applied as u64);
            self.metrics.record_infohash_sync(applied as u64);
            debug!("[federation] InfohashSync 应用 {} 条", applied);
        }
    }

    /// 获取 Merkle 树引用（用于反熵对账）
    pub fn merkle(&self) -> Arc<MerkleTree> {
        self.merkle.clone()
    }

    /// 收集全量 Infohash 同步条目（用于初始全量同步）
    ///
    /// 遍历所有已注册的 infohash，构建 SyncEntry 列表。
    /// 注意：此方法不更新 Merkle 树，Merkle 树应在数据写入时更新，
    /// 避免在遍历大量数据时持有锁导致死锁。
    pub fn collect_all_entries(&self) -> Vec<SyncEntry> {
        let infohashes = self.infohash_repo.all_sync();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut entries = Vec::with_capacity(infohashes.len());

        for (infohash, _last_seen) in &infohashes {
            let payload = InfohashSyncPayload {
                infohash: *infohash,
            };
            let payload_bytes = match bincode::serialize(&payload) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let key = infohash.to_vec();
            entries.push(SyncEntry {
                key,
                operation: operation::UPSERT,
                version: now,
                payload: payload_bytes,
            });
        }

        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::node_id::NodeId;
    use crate::federation::session::SessionsHandle;
    use crate::storage::Storage;

    fn make_infohash_repo() -> Arc<InfohashRepoImpl> {
        let storage = Arc::new(Storage::memory().unwrap());
        Arc::new(InfohashRepoImpl::new(storage))
    }

    #[test]
    fn test_infohash_sync_payload_serde() {
        let payload = InfohashSyncPayload { infohash: [7; 20] };
        let bytes = bincode::serialize(&payload).unwrap();
        let decoded: InfohashSyncPayload = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.infohash, [7; 20]);
    }

    #[test]
    fn test_apply_infohash_sync() {
        let repo = make_infohash_repo();
        let (shutdown_tx, _) = broadcast::channel(1);
        let cm = SessionsHandle::new_for_test();
        let metrics = Arc::new(FederationMetrics::new());
        let gossip = Arc::new(GossipEngine::new(
            cm,
            crate::federation::config::FederationConfig::default(),
            NodeId([1; 20]),
            metrics.clone(),
            shutdown_tx.clone(),
        ));
        let merkle = Arc::new(MerkleTree::new(16));
        let queue = Arc::new(MerkleUpdateQueue::new());
        let ih_sync = InfohashSync::new(repo.clone(), gossip, merkle, queue, metrics, shutdown_tx);

        assert_eq!(repo.count_sync(), 0);

        let payload = InfohashSyncPayload { infohash: [9; 20] };
        let entries = vec![SyncEntry {
            key: vec![9; 20],
            operation: operation::UPSERT,
            version: 1,
            payload: bincode::serialize(&payload).unwrap(),
        }];

        ih_sync.apply_infohash_sync(&entries);
        assert_eq!(repo.count_sync(), 1);
    }
}
