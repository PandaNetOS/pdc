//! PeerRepo 同步
//!
//! 订阅 EventBus 的 PeerDiscovered 事件，通过 Gossip 协议传播给联邦节点。
//! 只同步事实数据：infohash + addr。时间戳、来源、是否禁用等节点本地状态不同步。

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

/// Peer 同步负载（只同步事实数据：infohash + addr）
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PeerSyncPayload {
    infohash: Infohash,
    addr: std::net::SocketAddr,
}

/// 构建 Peer 同步条目的 (key, payload_bytes, data_hash)。
/// 主键为 infohash:addr 组合，同步 (infohash, peer) 关联。
///
/// data_hash 公式与 db.rs `load_all_peer_keys_hashes` 完全一致：
/// `blake3(infohash || ip_string || port_le)`，保证冷热同构。
pub(crate) fn build_peer_sync_entry(
    infohash: Infohash,
    addr: std::net::SocketAddr,
) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let payload = PeerSyncPayload { infohash, addr };
    let payload_bytes = bincode::serialize(&payload).ok()?;
    let ih_hex = infohash
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    let key = format!("{}:{}", ih_hex, addr).into_bytes();
    // data_hash = blake3(infohash || ip_string || port_le)
    let mut buf = Vec::with_capacity(infohash.len() + 16 + 2);
    buf.extend_from_slice(infohash.as_slice());
    buf.extend_from_slice(addr.ip().to_string().as_bytes());
    buf.extend_from_slice(&addr.port().to_le_bytes());
    let data_hash = blake3::hash(&buf).as_bytes().to_vec();
    Some((key, payload_bytes, data_hash))
}

/// 从已序列化的 PeerSyncPayload 计算 data_hash（与 build_peer_sync_entry 公式一致）。
/// 用于 apply/重建路径，反序列化 payload 后按 db.rs 公式计算。
#[allow(dead_code)]
pub(crate) fn data_hash_from_payload(payload: &[u8]) -> Option<Vec<u8>> {
    let p: PeerSyncPayload = bincode::deserialize(payload).ok()?;
    let mut buf = Vec::with_capacity(p.infohash.len() + 16 + 2);
    buf.extend_from_slice(p.infohash.as_slice());
    buf.extend_from_slice(p.addr.ip().to_string().as_bytes());
    buf.extend_from_slice(&p.addr.port().to_le_bytes());
    Some(blake3::hash(&buf).as_bytes().to_vec())
}

/// PeerRepo 同步服务
pub struct PeerSync {
    peer_repo: Arc<PeerRepoImpl>,
    _gossip_engine: Arc<GossipEngine>,
    merkle: Arc<MerkleTree>,
    #[allow(dead_code)]
    merkle_queue: Arc<MerkleUpdateQueue>,
    _local_node_id: NodeId,
    metrics: Arc<FederationMetrics>,
    enabled: bool,
    shutdown: broadcast::Sender<()>,
}

impl PeerSync {
    pub fn new(
        peer_repo: Arc<PeerRepoImpl>,
        _gossip_engine: Arc<GossipEngine>,
        merkle: Arc<MerkleTree>,
        #[allow(dead_code)] merkle_queue: Arc<MerkleUpdateQueue>,
        _local_node_id: NodeId,
        metrics: Arc<FederationMetrics>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            peer_repo,
            _gossip_engine,
            merkle,
            merkle_queue,
            _local_node_id,
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
    ///
    /// 联邦同步只传播事实（infohash + addr）：入站仅添加本地不存在的
    /// (infohash, addr) 关联，不覆盖本地已有 peer 的 source / first_seen /
    /// last_active 等节点状态字段。
    pub fn apply_peer_sync(&self, entries: &[SyncEntry]) {
        // 第一遍：过滤 DELETE / 反序列化失败 / 本地已存在的关联，构建 (infohash, PeerInfo) 列表
        let mut items: Vec<(Infohash, PeerInfo)> = Vec::new();
        let mut applied = 0;
        for entry in entries {
            if entry.operation == operation::DELETE {
                continue;
            }
            let payload: PeerSyncPayload = match bincode::deserialize(&entry.payload) {
                Ok(p) => p,
                Err(_) => continue,
            };

            // 本地已存在该 (infohash, addr) 关联则跳过，绝不覆盖本地 peer 状态
            if self
                .peer_repo
                .has_peer_assoc(&payload.infohash, &payload.addr)
            {
                continue;
            }

            // 新关联：用默认状态构建 PeerInfo（source=Manual，时间取当前）
            let peer = PeerInfo {
                addr: payload.addr,
                peer_id: None,
                source: PeerSource::Manual,
                first_seen: SystemTime::now(),
                last_active: SystemTime::now(),
                priority_score: 45.0,
                connection_attempts: 0,
                connection_successes: 0,
                is_ipv6: payload.addr.is_ipv6(),
                metadata: Default::default(),
            };

            items.push((payload.infohash, peer));
            applied += 1;
        }

        // 第二遍：一次写锁批量写入（调用 add_peers_sync_internal，维护 global + by_infohash）
        // 注意：联邦入站不调用 propagate_peers，避免 Gossip 回环
        if !items.is_empty() {
            self.peer_repo.add_peers_sync_internal(&items);
        }

        // 【回环修复】入站 apply 不再 update_incremental_batch 标记 dirty（同 apply_node_sync），
        // 否则本批条目所属 L2 被标 dirty，incremental_sync_tick 又把它整批推回对端。

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

    /// 收集全量 Peer 同步条目（用于差量同步）
    ///
    /// 遍历 by_infohash 中的所有 (infohash, addr) 关联，构建 SyncEntry 列表。
    /// 主键为 infohash:addr 组合。
    pub fn collect_all_entries(&self) -> Vec<SyncEntry> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let all_with_ih = self.peer_repo.all_peers_with_infohashes_sync();
        info!(
            "[federation] Peer collect_all_entries: peers={}, infohash_refs_total={}",
            all_with_ih.len(),
            all_with_ih.iter().map(|(_, ihs)| ihs.len()).sum::<usize>()
        );

        let mut entries = Vec::new();

        for (peer, infohashes) in &all_with_ih {
            // 如果 peer 没有关联任何 infohash（理论上不应发生），跳过
            if infohashes.is_empty() {
                continue;
            }

            for infohash in infohashes {
                let payload = PeerSyncPayload {
                    infohash: *infohash,
                    addr: peer.addr,
                };
                let payload_bytes = match bincode::serialize(&payload) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                let ih_hex = infohash
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<String>();
                let key = format!("{}:{}", ih_hex, peer.addr).into_bytes();

                entries.push(SyncEntry {
                    key,
                    operation: operation::UPSERT,
                    version: now,
                    payload: payload_bytes,
                });
            }
        }

        info!(
            "[federation] Peer collect_all_entries 完成: {} 条 entries",
            entries.len()
        );
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
        };
        let bytes = bincode::serialize(&payload).unwrap();
        let decoded: PeerSyncPayload = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.infohash, [1; 20]);
        assert_eq!(
            decoded.addr,
            "127.0.0.1:6881".parse::<std::net::SocketAddr>().unwrap()
        );
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
        };
        let ih_hex = "05".repeat(20);
        let entries = vec![SyncEntry {
            key: format!("{}:10.0.0.1:6881", ih_hex).into_bytes(),
            operation: operation::UPSERT,
            version: 1,
            payload: bincode::serialize(&payload).unwrap(),
        }];

        peer_sync.apply_peer_sync(&entries);
        // 验证 peer 已写入 global 和 by_infohash
        assert_eq!(peer_repo.len(), 1);
        let peers = peer_repo.get_peers_sync(&[5; 20], 10);
        assert_eq!(peers.len(), 1);
        assert_eq!(
            peers[0].addr,
            "10.0.0.1:6881".parse::<std::net::SocketAddr>().unwrap()
        );
    }
}
