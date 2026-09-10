//! PeerRepo 同步
//!
//! 订阅 EventBus 的 PeerDiscovered 事件，通过 Gossip 协议传播给联邦节点。
//! 只同步事实数据：infohash + ip:port + first_seen + source。

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, info};

use crate::event_bus::EventBus;
use crate::federation::gossip::GossipEngine;
use crate::federation::merkle::MerkleTree;
use crate::federation::metrics::FederationMetrics;
use crate::federation::node_id::NodeId;
use crate::federation::protocol::*;
use crate::federation::sync::merkle_updater::MerkleUpdateQueue;
use crate::storage::PeerRepoImpl;
use crate::types::{Event, Infohash, PeerInfo, PeerSource};

/// Peer 同步负载（精简版，只同步事实数据）
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PeerSyncPayload {
    infohash: Infohash,
    addr: std::net::SocketAddr,
    first_seen_secs: u64,
    source: String,
}

/// 构建 Peer 同步条目的 (key, payload_bytes)。
/// 格式与 collect_all_entries 完全一致，供 PeerRepoImpl 本地写入后更新 Merkle / 提交 Gossip。
pub(crate) fn build_peer_sync_entry(
    infohash: Infohash,
    addr: std::net::SocketAddr,
    first_seen_secs: u64,
    source: &str,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let payload = PeerSyncPayload {
        infohash,
        addr,
        first_seen_secs,
        source: source.to_string(),
    };
    let payload_bytes = bincode::serialize(&payload).ok()?;
    let mut key = Vec::with_capacity(20 + 8);
    key.extend_from_slice(&infohash);
    key.extend_from_slice(&addr.to_string().into_bytes());
    Some((key, payload_bytes))
}

/// PeerRepo 同步服务
pub struct PeerSync {
    peer_repo: Arc<PeerRepoImpl>,
    gossip_engine: Arc<GossipEngine>,
    merkle: Arc<MerkleTree>,
    merkle_queue: Arc<MerkleUpdateQueue>,
    local_node_id: NodeId,
    metrics: Arc<FederationMetrics>,
    enabled: bool,
    shutdown: broadcast::Sender<()>,
}

impl PeerSync {
    pub fn new(
        peer_repo: Arc<PeerRepoImpl>,
        gossip_engine: Arc<GossipEngine>,
        merkle: Arc<MerkleTree>,
        merkle_queue: Arc<MerkleUpdateQueue>,
        local_node_id: NodeId,
        metrics: Arc<FederationMetrics>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            peer_repo,
            gossip_engine,
            merkle,
            merkle_queue,
            local_node_id,
            metrics,
            enabled: true,
            shutdown,
        }
    }

    /// 启动事件消费者：订阅 EventBus，筛选 PeerDiscovered 事件
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
                                debug!("[federation] PeerSync 事件滞后，丢弃 {} 个", n);
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                debug!("[federation] PeerSync 事件总线已关闭");
                                break;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] PeerSync 事件消费者收到关闭信号");
                        break;
                    }
                }
            }
        });
        debug!("[federation] PeerSync 事件消费者已启动");
    }

    /// 处理事件（已退役）：本地 peer 写入已由 PeerRepoImpl.add_peers_sync 统一更新
    /// Merkle + 提交 Gossip（payload 使用 peer.source，与 collect_all_entries 一致），
    /// 此处不再重复传播，避免双发与 payload source 不一致导致的 Merkle 抖动。
    fn handle_event(self: Arc<Self>, _event: Event) {}

    /// 应用收到的 Peer 同步数据
    pub fn apply_peer_sync(&self, entries: &[SyncEntry]) {
        // 第一遍：过滤 DELETE / 反序列化失败，构建 (infohash, peer)，收集 Merkle 批量更新
        let mut items: Vec<(Infohash, PeerInfo)> = Vec::new();
        let mut merkle_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut applied = 0;
        for entry in entries {
            if entry.operation == operation::DELETE {
                continue;
            }
            let payload: PeerSyncPayload = match bincode::deserialize(&entry.payload) {
                Ok(p) => p,
                Err(_) => continue,
            };

            let source = match payload.source.as_str() {
                "tracker" => PeerSource::Tracker,
                "dht" => PeerSource::Dht,
                "pex" => PeerSource::Pex,
                "lpd" => PeerSource::Lpd,
                "webseed" => PeerSource::WebSeed,
                "super_tracker" => PeerSource::SuperTracker,
                "utp" => PeerSource::Utp,
                _ => PeerSource::Manual,
            };

            let first_seen = UNIX_EPOCH + std::time::Duration::from_secs(payload.first_seen_secs);
            let peer = PeerInfo {
                addr: payload.addr,
                peer_id: None,
                source,
                first_seen,
                last_active: SystemTime::now(),
                priority_score: 45.0,
                connection_attempts: 0,
                connection_successes: 0,
                is_ipv6: payload.addr.is_ipv6(),
                metadata: Default::default(),
            };

            // key = infohash(20) + addr 序列化
            let mut key = Vec::with_capacity(20 + 8);
            key.extend_from_slice(&payload.infohash);
            key.extend_from_slice(&payload.addr.to_string().into_bytes());
            merkle_batch.push((key, entry.payload.clone()));

            items.push((payload.infohash, peer));
            applied += 1;
        }

        // 异步批量入队 Merkle 更新（后台任务定期 flush），不再同步调用 update_batch
        if !merkle_batch.is_empty() {
            let items: Vec<_> = merkle_batch
                .into_iter()
                .map(|(k, v)| (repo_type::PEER, k, v))
                .collect();
            self.merkle_queue.push_batch(items);
        }

        // 第二遍：一次写锁批量写入（调用内部方法，不触发 Merkle/Gossip，避免回环）
        if !items.is_empty() {
            self.peer_repo.add_peers_sync_internal(&items);
        }

        if applied > 0 {
            self.metrics.record_sync_entries(applied as u64);
            self.metrics.record_peer_sync(applied as u64);
            debug!("[federation] PeerSync 应用 {} 条", applied);
        }
    }

    /// 获取 Merkle 树引用（用于反熵对账）
    pub fn merkle(&self) -> Arc<MerkleTree> {
        self.merkle.clone()
    }

    /// 收集全量 Peer 同步条目（用于初始全量同步）
    ///
    /// 遍历所有 infohash 及其关联的 peer，构建 SyncEntry 列表。
    /// 注意：此方法不更新 Merkle 树，避免在遍历大量数据时持有锁导致死锁。
    pub fn collect_all_entries(&self) -> Vec<SyncEntry> {
        // 确保 PeerRepo 数据已加载（联邦模块可能拿到独立实例，cache 为空）
        self.peer_repo.ensure_loaded();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // 高效方式：一次读锁获取所有 peer 及其关联的 infohashes
        // 避免遍历 2741 个 infohash 时每次都加锁+排序导致的性能问题
        let all_peers = self.peer_repo.all_peers_with_infohashes_sync();
        info!(
            "[federation] Peer collect_all_entries: all_peers={}",
            all_peers.len()
        );

        let mut entries = Vec::with_capacity(all_peers.len());

        for (peer, infohashes) in &all_peers {
            for infohash in infohashes {
                let payload = PeerSyncPayload {
                    infohash: *infohash,
                    addr: peer.addr,
                    first_seen_secs: peer
                        .first_seen
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    source: peer.source.as_str().to_string(),
                };
                let payload_bytes = match bincode::serialize(&payload) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                let mut key = Vec::with_capacity(20 + 8);
                key.extend_from_slice(infohash);
                key.extend_from_slice(&peer.addr.to_string().into_bytes());

                entries.push(SyncEntry {
                    key,
                    operation: operation::UPSERT,
                    version: now,
                    payload: payload_bytes,
                });
            }
        }

        info!("[federation] Peer collect_all_entries 完成: {} 条 entries", entries.len());
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;

    fn make_peer_repo() -> Arc<PeerRepoImpl> {
        let storage = Arc::new(Storage::memory().unwrap());
        Arc::new(PeerRepoImpl::new(storage))
    }

    #[test]
    fn test_peer_sync_payload_serde() {
        let payload = PeerSyncPayload {
            infohash: [1; 20],
            addr: "127.0.0.1:6881".parse().unwrap(),
            first_seen_secs: 1000,
            source: "dht".to_string(),
        };
        let bytes = bincode::serialize(&payload).unwrap();
        let decoded: PeerSyncPayload = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.infohash, [1; 20]);
        assert_eq!(decoded.addr, "127.0.0.1:6881".parse::<std::net::SocketAddr>().unwrap());
        assert_eq!(decoded.source, "dht");
    }

    #[test]
    fn test_apply_peer_sync() {
        let peer_repo = make_peer_repo();
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
        let peer_sync = PeerSync::new(
            peer_repo.clone(),
            gossip,
            merkle,
            queue,
            NodeId([1; 20]),
            metrics,
            shutdown_tx,
        );

        let payload = PeerSyncPayload {
            infohash: [5; 20],
            addr: "10.0.0.1:6881".parse().unwrap(),
            first_seen_secs: 1000,
            source: "tracker".to_string(),
        };
        let entries = vec![SyncEntry {
            key: b"testkey".to_vec(),
            operation: operation::UPSERT,
            version: 1,
            payload: bincode::serialize(&payload).unwrap(),
        }];

        peer_sync.apply_peer_sync(&entries);
        // 验证 peer 已写入
        let peers = peer_repo.get_peers_sync(&[5; 20], 10);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].addr, "10.0.0.1:6881".parse::<std::net::SocketAddr>().unwrap());
    }
}
