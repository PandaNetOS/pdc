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
use crate::federation::node_id::NodeId;
use crate::federation::protocol::*;
use crate::federation::sync::merkle_updater::MerkleUpdateQueue;
use crate::storage::InfohashRepoImpl;
use crate::types::{Event, Infohash};

/// Infohash 同步负载
#[derive(Debug, Clone, Serialize, Deserialize)]
struct InfohashSyncPayload {
    infohash: Infohash,
    seen_at_secs: u64,
    source: String,
}

/// 构建 Infohash 同步条目的 (key, payload_bytes)。
/// 格式与 collect_all_entries 一致，供 InfohashRepoImpl 本地写入后更新 Merkle / 提交 Gossip。
pub(crate) fn build_infohash_sync_entry(
    infohash: Infohash,
    seen_at_secs: u64,
    source: &str,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let payload = InfohashSyncPayload {
        infohash,
        seen_at_secs,
        source: source.to_string(),
    };
    let payload_bytes = bincode::serialize(&payload).ok()?;
    let key = infohash.to_vec();
    Some((key, payload_bytes))
}

/// InfohashRepo 同步服务
pub struct InfohashSync {
    infohash_repo: Arc<InfohashRepoImpl>,
    gossip_engine: Arc<GossipEngine>,
    merkle: Arc<MerkleTree>,
    merkle_queue: Arc<MerkleUpdateQueue>,
    metrics: Arc<FederationMetrics>,
    enabled: bool,
    shutdown: broadcast::Sender<()>,
}

impl InfohashSync {
    pub fn new(
        infohash_repo: Arc<InfohashRepoImpl>,
        gossip_engine: Arc<GossipEngine>,
        merkle: Arc<MerkleTree>,
        merkle_queue: Arc<MerkleUpdateQueue>,
        metrics: Arc<FederationMetrics>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            infohash_repo,
            gossip_engine,
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
    pub fn apply_infohash_sync(&self, entries: &[SyncEntry]) {
        // 第一遍：过滤 DELETE / 反序列化失败，收集 (infohash, source, last_seen) 并批量更新 Merkle
        let mut items: Vec<(Infohash, String, u64)> = Vec::new();
        let mut merkle_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut applied = 0;
        for entry in entries {
            if entry.operation == operation::DELETE {
                continue;
            }
            let payload: InfohashSyncPayload = match bincode::deserialize(&entry.payload) {
                Ok(p) => p,
                Err(_) => continue,
            };

            merkle_batch.push((entry.key.clone(), entry.payload.clone()));
            items.push((payload.infohash, payload.source, payload.seen_at_secs));
            applied += 1;
        }

        // 异步批量入队 Merkle 更新（后台任务定期 flush），不再同步调用 update_batch
        if !merkle_batch.is_empty() {
            let items: Vec<_> = merkle_batch
                .into_iter()
                .map(|(k, v)| (repo_type::INFOHASH, k, v))
                .collect();
            self.merkle_queue.push_batch(items);
        }

        // 第二遍：一次写锁批量注册（调用内部方法，不触发 Merkle/Gossip，避免回环）
        if !items.is_empty() {
            self.infohash_repo.register_batch_internal(&items);
        }

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
        let mut entries = Vec::with_capacity(infohashes.len());

        for (infohash, last_seen) in &infohashes {
            let ts = if *last_seen > 0 { *last_seen } else {
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
            };
            let payload = InfohashSyncPayload {
                infohash: *infohash,
                seen_at_secs: ts,
                source: "federation_init".to_string(),
            };
            let payload_bytes = match bincode::serialize(&payload) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let key = infohash.to_vec();
            entries.push(SyncEntry {
                key,
                operation: operation::UPSERT,
                version: ts,
                payload: payload_bytes,
            });
        }

        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;

    fn make_infohash_repo() -> Arc<InfohashRepoImpl> {
        let storage = Arc::new(Storage::memory().unwrap());
        Arc::new(InfohashRepoImpl::new(storage))
    }

    #[test]
    fn test_infohash_sync_payload_serde() {
        let payload = InfohashSyncPayload {
            infohash: [7; 20],
            seen_at_secs: 2000,
            source: "dht".to_string(),
        };
        let bytes = bincode::serialize(&payload).unwrap();
        let decoded: InfohashSyncPayload = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.infohash, [7; 20]);
        assert_eq!(decoded.seen_at_secs, 2000);
    }

    #[test]
    fn test_apply_infohash_sync() {
        let repo = make_infohash_repo();
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cm_shutdown, _) = broadcast::channel(1);
        let identity = crate::federation::node_id::NodeIdentity::generate();
        let node_table = Arc::new(crate::federation::node_table::NodeTable::new(100));
        let cm = Arc::new(crate::federation::connection::ConnectionManager::new(
            node_table,
            Arc::new(identity),
            crate::federation::config::FederationConfig::default(),
            cm_shutdown,
            Arc::new(FederationMetrics::new()),
        ));
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
        let ih_sync = InfohashSync::new(
            repo.clone(),
            gossip,
            merkle,
            queue,
            metrics,
            shutdown_tx,
        );

        assert_eq!(repo.count_sync(), 0);

        let payload = InfohashSyncPayload {
            infohash: [9; 20],
            seen_at_secs: 100,
            source: "crawler".to_string(),
        };
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
