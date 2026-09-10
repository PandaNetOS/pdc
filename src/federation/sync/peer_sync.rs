//! PeerRepo 同步
//!
//! 订阅 EventBus 的 PeerDiscovered 事件，通过 Gossip 协议传播给联邦节点。
//! 只同步事实数据：infohash + ip:port + first_seen + source。

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

/// PeerRepo 同步服务
pub struct PeerSync {
    peer_repo: Arc<PeerRepoImpl>,
    gossip_engine: Arc<GossipEngine>,
    merkle: Arc<MerkleTree>,
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
        local_node_id: NodeId,
        metrics: Arc<FederationMetrics>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            peer_repo,
            gossip_engine,
            merkle,
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

    /// 处理事件
    fn handle_event(self: Arc<Self>, event: Event) {
        if let Event::PeerDiscovered { infohash, peers, source } = event {
            if peers.is_empty() {
                return;
            }

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let mut entries = Vec::with_capacity(peers.len());
            for peer in &peers {
                let payload = PeerSyncPayload {
                    infohash,
                    addr: peer.addr,
                    first_seen_secs: peer
                        .first_seen
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    source: source.clone(),
                };
                let payload_bytes = match bincode::serialize(&payload) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                // key = infohash(20) + addr 序列化
                let mut key = Vec::with_capacity(20 + 8);
                key.extend_from_slice(&infohash);
                key.extend_from_slice(&peer.addr.to_string().into_bytes());

                // 更新 Merkle（在 move 之前）
                self.merkle.update(&key, &payload_bytes);

                entries.push(SyncEntry {
                    key,
                    operation: operation::UPSERT,
                    version: now,
                    payload: payload_bytes,
                });
            }

            if !entries.is_empty() {
                self.gossip_engine.submit_gossip(repo_type::PEER, entries);
            }
        }
    }

    /// 应用收到的 Peer 同步数据
    pub fn apply_peer_sync(&self, entries: &[SyncEntry]) {
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

            self.peer_repo.add_peers_sync(&payload.infohash, &[peer]);

            // 更新 Merkle
            let mut key = Vec::with_capacity(20 + 8);
            key.extend_from_slice(&payload.infohash);
            key.extend_from_slice(&payload.addr.to_string().into_bytes());
            self.merkle.update(&key, &entry.payload);

            applied += 1;
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
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let infohashes = self.peer_repo.infohashes();
        let mut entries = Vec::new();

        for infohash in &infohashes {
            let peers = self.peer_repo.get_peers_sync(infohash, usize::MAX);
            for peer in &peers {
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
        let peer_sync = PeerSync::new(
            peer_repo.clone(),
            gossip,
            merkle,
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
