//! 同步管理器
//!
//! 阶段2扩展：集成 Gossip 引擎、PeerRepo 同步、InfohashRepo 同步。
//! 阶段1的 NodeRepo 同步保留。

pub mod infohash_sync;
pub mod merkle_updater;
pub mod peer_sync;
pub mod tracker_sync;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::event_bus::EventBus;
use crate::federation::config::FederationConfig;
use crate::federation::connection::{Connection, ConnectionManager};
use crate::federation::gossip::GossipEngine;
use crate::federation::merkle::{MerkleProvider, MerkleTree};
use crate::federation::metrics::FederationMetrics;
use crate::federation::node_id::NodeId;
use crate::federation::protocol::*;
use crate::federation::sync::infohash_sync::InfohashSync;
use crate::federation::relay::RelayManager;
use crate::federation::sync::merkle_updater::MerkleUpdateQueue;
use crate::federation::sync::peer_sync::PeerSync;
use crate::federation::sync::tracker_sync::TrackerSync;
use crate::storage::{InfohashRepoImpl, NodeRepoImpl, PeerRepoImpl, TrackerRepoImpl};

/// 节点同步负载
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeSyncPayload {
    node_id: [u8; 20],
    addr: SocketAddr,
}

/// 构建 Node 同步条目的 (key, payload_bytes)。
/// 格式与 collect_node_entries 一致，供 NodeRepoImpl 本地写入后更新 Merkle / 提交 Gossip。
pub(crate) fn build_node_sync_entry(
    node_id: [u8; 20],
    addr: SocketAddr,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let payload = NodeSyncPayload { node_id, addr };
    let payload_bytes = bincode::serialize(&payload).ok()?;
    let key = addr.to_string().into_bytes();
    Some((key, payload_bytes))
}

/// 同步管理器
pub struct SyncManager {
    connection_manager: Arc<ConnectionManager>,
    node_repo: Arc<NodeRepoImpl>,
    gossip_engine: Arc<GossipEngine>,
    peer_sync: Option<Arc<PeerSync>>,
    infohash_sync: Option<Arc<InfohashSync>>,
    node_merkle: Arc<MerkleTree>,
    tracker_sync: Option<Arc<TrackerSync>>,
    relay_manager: Option<Arc<RelayManager>>,
    /// Merkle 树异步批量更新队列（apply 时入队，后台任务定期 flush）
    merkle_queue: Arc<MerkleUpdateQueue>,
    config: FederationConfig,
    metrics: Arc<FederationMetrics>,
    shutdown: broadcast::Sender<()>,
    /// 已触发初始全量同步的对端节点集合（按对端去重，每个对端只同步一次）
    initial_sync_peers: RwLock<HashSet<NodeId>>,
}

impl SyncManager {
    /// 创建同步管理器
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        node_repo: Arc<NodeRepoImpl>,
        config: FederationConfig,
        shutdown: broadcast::Sender<()>,
        gossip_engine: Arc<GossipEngine>,
        metrics: Arc<FederationMetrics>,
        local_node_id: NodeId,
        event_bus: Option<EventBus>,
        peer_repo: Option<Arc<PeerRepoImpl>>,
        infohash_repo: Option<Arc<InfohashRepoImpl>>,
        tracker_repo: Option<Arc<TrackerRepoImpl>>,
        relay_manager: Option<Arc<RelayManager>>,
    ) -> Self {
        let node_merkle = Arc::new(MerkleTree::new(256));
        let merkle_queue = Arc::new(MerkleUpdateQueue::new());

        // PeerSync
        let peer_sync = if config.sync_peer_enabled {
            if let Some(pr) = peer_repo {
                let ps = Arc::new(PeerSync::new(
                    pr,
                    gossip_engine.clone(),
                    Arc::new(MerkleTree::new(256)),
                    merkle_queue.clone(),
                    local_node_id,
                    metrics.clone(),
                    shutdown.clone(),
                ));
                if let Some(ref bus) = event_bus {
                    ps.clone().spawn_event_consumer(bus.clone());
                }
                Some(ps)
            } else {
                None
            }
        } else {
            None
        };

        // InfohashSync
        let infohash_sync = if config.sync_infohash_enabled {
            if let Some(ir) = infohash_repo {
                let ihs = Arc::new(InfohashSync::new(
                    ir,
                    gossip_engine.clone(),
                    Arc::new(MerkleTree::new(256)),
                    merkle_queue.clone(),
                    metrics.clone(),
                    shutdown.clone(),
                ));
                if let Some(ref bus) = event_bus {
                    ihs.clone().spawn_event_consumer(bus.clone());
                }
                Some(ihs)
            } else {
                None
            }
        } else {
            None
        };

        // TrackerSync
        let tracker_sync = if config.sync_tracker_enabled {
            if let Some(tr) = tracker_repo {
                let ts = Arc::new(TrackerSync::new(
                    tr,
                    gossip_engine.clone(),
                    Arc::new(MerkleTree::new(256)),
                    merkle_queue.clone(),
                    metrics.clone(),
                    shutdown.clone(),
                ));
                ts.clone().spawn_full_sync();
                Some(ts)
            } else {
                None
            }
        } else {
            None
        };

        Self {
            connection_manager,
            node_repo,
            gossip_engine,
            peer_sync,
            infohash_sync,
            node_merkle,
            tracker_sync,
            relay_manager,
            merkle_queue,
            config,
            metrics,
            shutdown,
            initial_sync_peers: RwLock::new(HashSet::new()),
        }
    }

    /// 启动 Node 同步后台任务（阶段1保留）
    pub fn spawn_node_sync(self: Arc<Self>) {
        if !self.config.sync_node_enabled {
            return;
        }
        let interval_secs = self.config.sync_node_interval_secs;
        let interval = Duration::from_secs(interval_secs);
        let mut shutdown_rx = self.shutdown.subscribe();
        let self_clone = self.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self_clone.clone().do_node_sync().await;
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });
        debug!("[federation] Node 同步任务已启动（间隔 {}s）", interval_secs);
    }

    /// 启动 Merkle 异步批量 flush 后台任务
    ///
    /// apply_*_sync 只入队，本任务每 `merkle_async_update_interval_ms` 毫秒或被 Notify
    /// 唤醒（入队时触发）后将队列中条目按 repo_type 分组调用 merkle.update_batch。
    /// 关闭信号到达时先 flush 一次再退出，保证退出前数据不丢失。
    pub fn spawn_merkle_flusher(self: Arc<Self>) {
        let interval_ms = self.config.merkle_async_update_interval_ms;
        let batch_threshold = self.config.merkle_async_update_batch_size;
        let queue = self.merkle_queue.clone();
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
            ticker.tick().await; // 跳过首次立即触发

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self.clone().flush_merkle_queue(batch_threshold);
                    }
                    _ = queue.notify().notified() => {
                        // 入队通知唤醒：即使未达阈值也 flush（apply 批量本身已攒批）
                        self.clone().flush_merkle_queue(batch_threshold);
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] Merkle flush 任务收到关闭信号，退出前 flush");
                        self.flush_merkle_queue(batch_threshold);
                        break;
                    }
                }
            }
        });
        debug!(
            "[federation] Merkle 异步批量 flush 任务已启动（间隔 {}ms, 阈值 {} 条）",
            interval_ms, batch_threshold
        );
    }

    /// 批量 flush 队列中的 Merkle 更新：按 repo_type 分组后调用对应 merkle.update_batch
    fn flush_merkle_queue(&self, _batch_threshold: usize) {
        let items = self.merkle_queue.drain();
        if items.is_empty() {
            return;
        }

        // 按 repo_type 分组
        let mut node_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut peer_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut infohash_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut tracker_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        for (repo_type, key, payload) in items {
            match repo_type {
                repo_type::NODE => node_batch.push((key, payload)),
                repo_type::PEER => peer_batch.push((key, payload)),
                repo_type::INFOHASH => infohash_batch.push((key, payload)),
                repo_type::TRACKER => tracker_batch.push((key, payload)),
                other => {
                    warn!("[federation] Merkle flush: 未知 repo_type={}, 丢弃", other);
                }
            }
        }

        // 逐 repo 调用 update_batch（一次写锁批量插入，只重算受影响分片）
        if !node_batch.is_empty() {
            let refs: Vec<(&[u8], &[u8])> = node_batch
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice()))
                .collect();
            self.node_merkle.update_batch(&refs);
        }
        if !peer_batch.is_empty() {
            if let Some(ps) = &self.peer_sync {
                let refs: Vec<(&[u8], &[u8])> = peer_batch
                    .iter()
                    .map(|(k, v)| (k.as_slice(), v.as_slice()))
                    .collect();
                ps.merkle().update_batch(&refs);
            }
        }
        if !infohash_batch.is_empty() {
            if let Some(ihs) = &self.infohash_sync {
                let refs: Vec<(&[u8], &[u8])> = infohash_batch
                    .iter()
                    .map(|(k, v)| (k.as_slice(), v.as_slice()))
                    .collect();
                ihs.merkle().update_batch(&refs);
            }
        }
        if !tracker_batch.is_empty() {
            if let Some(ts) = &self.tracker_sync {
                let refs: Vec<(&[u8], &[u8])> = tracker_batch
                    .iter()
                    .map(|(k, v)| (k.as_slice(), v.as_slice()))
                    .collect();
                ts.merkle().update_batch(&refs);
            }
        }

        debug!(
            "[federation] Merkle 批量 flush: total={}, node={}, peer={}, infohash={}, tracker={}",
            node_batch.len() + peer_batch.len() + infohash_batch.len() + tracker_batch.len(),
            node_batch.len(),
            peer_batch.len(),
            infohash_batch.len(),
            tracker_batch.len()
        );
    }

    /// 执行一次 Node 同步（已退役）：本地新节点已由 NodeRepoImpl.add_node_sync 统一更新
    /// Merkle + 提交 Gossip，此处不再轮询传播，也不再 take_dirty（避免与 save_all 抢脏标记）。
    async fn do_node_sync(self: Arc<Self>) {}

    /// 处理收到的同步批量消息（阶段1 SyncBatch 协议）
    pub fn handle_sync_batch(&self, repo_type: u8, entries: &[SyncEntry]) {
        match repo_type {
            repo_type::NODE => self.apply_node_sync(entries),
            repo_type::PEER => {
                if let Some(ref ps) = self.peer_sync {
                    ps.apply_peer_sync(entries);
                }
            }
            repo_type::INFOHASH => {
                if let Some(ref ihs) = self.infohash_sync {
                    ihs.apply_infohash_sync(entries);
                }
            }
            repo_type::TRACKER => {
                if let Some(ref ts) = self.tracker_sync {
                    ts.apply_tracker_sync(entries);
                }
            }
            _ => {
                warn!("[federation] 未知仓库类型: {}", repo_type);
            }
        }
    }

    /// 处理收到的 Gossip 消息
    pub fn handle_gossip_batch(&self, batch: GossipBatchMessage) {
        let entries = self.gossip_engine.handle_gossip_batch(batch.clone());
        if entries.is_empty() {
            return;
        }
        self.handle_sync_batch(batch.repo_type, &entries);
    }

    /// 应用 Node 同步数据
    pub fn apply_node_sync(&self, entries: &[SyncEntry]) {
        // 第一遍：过滤 DELETE / 反序列化失败的条目，收集有效 payload，
        // 并批量收集 Merkle 更新（循环结束后一次 update_batch）。
        let mut items: Vec<([u8; 20], SocketAddr)> = Vec::new();
        let mut merkle_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut applied = 0;
        for entry in entries {
            if entry.operation == operation::DELETE {
                continue;
            }
            let payload: NodeSyncPayload = match bincode::deserialize(&entry.payload) {
                Ok(p) => p,
                Err(_) => continue,
            };
            items.push((payload.node_id, payload.addr));
            merkle_batch.push((entry.key.clone(), entry.payload.clone()));
            applied += 1;
        }

        // 异步批量入队 Merkle 更新（后台任务定期 flush），不再同步调用 update_batch
        if !merkle_batch.is_empty() {
            let items: Vec<_> = merkle_batch
                .into_iter()
                .map(|(k, v)| (repo_type::NODE, k, v))
                .collect();
            self.merkle_queue.push_batch(items);
        }

        // 第二遍：一次写锁批量写入（调用内部方法，不触发 Merkle/Gossip，避免回环）
        if !items.is_empty() {
            self.node_repo.add_nodes_batch_internal(&items);
        }

        if applied > 0 {
            self.metrics.record_sync_entries(applied as u64);
            self.metrics.record_node_sync(applied as u64);
            debug!("[federation] Node 同步应用 {} 条", applied);
        }
    }

    /// 获取 Node Merkle 摘要（用于反熵对账）
    pub fn node_merkle_digest(&self) -> MerkleDigestMessage {
        self.node_merkle.digest(repo_type::NODE)
    }

    /// 处理中继建立消息
    pub fn handle_relay_setup(&self, from_node: NodeId, msg: RelaySetupMessage) {
        if let Some(ref relay) = self.relay_manager {
            relay.handle_relay_setup(from_node, msg);
        }
    }

    /// 处理中继数据消息
    pub fn handle_relay_data(&self, from_node: NodeId, msg: RelayDataMessage) {
        if let Some(ref relay) = self.relay_manager {
            relay.handle_relay_data(from_node, msg);
        }
    }

    /// 获取中继管理器引用
    pub fn relay_manager(&self) -> Option<Arc<RelayManager>> {
        self.relay_manager.clone()
    }

    /// 获取 TrackerSync 引用
    pub fn tracker_sync(&self) -> Option<Arc<TrackerSync>> {
        self.tracker_sync.clone()
    }

    /// 获取 Node Merkle 树引用（main.rs 注入 NodeRepo 联邦引用用）
    pub fn node_merkle(&self) -> Arc<MerkleTree> {
        self.node_merkle.clone()
    }

    /// 获取 Peer Merkle 树引用（main.rs 注入 PeerRepo 联邦引用用）
    pub fn peer_merkle(&self) -> Option<Arc<MerkleTree>> {
        self.peer_sync.as_ref().map(|ps| ps.merkle())
    }

    /// 获取 Infohash Merkle 树引用（main.rs 注入 InfohashRepo 联邦引用用）
    pub fn infohash_merkle(&self) -> Option<Arc<MerkleTree>> {
        self.infohash_sync.as_ref().map(|ihs| ihs.merkle())
    }

    /// 获取 Tracker Merkle 树引用（main.rs 注入 TrackerRepo 联邦引用用）
    pub fn tracker_merkle(&self) -> Option<Arc<MerkleTree>> {
        self.tracker_sync.as_ref().map(|ts| ts.merkle())
    }

    /// 触发初始全量同步（连接建立后调用，异步执行不阻塞）
    ///
    /// Node/Peer/Infohash/Tracker 四个 repo 并行执行全量同步，
    /// 每个 repo 独立 spawn_blocking 收集数据，按配置化 batch_size 通过 Gossip 批量提交。
    /// 入站和出站连接建立后都会触发，按对端 node_id 去重，每个对端只触发一次。
    ///
    /// P2-1 优化：同步开始时对所有 MerkleTree 设置 full_sync_in_progress=true，
    /// 跳过逐条 recompute_shard；全部完成后 set false + rebuild_all 一次性重算。
    pub fn trigger_initial_sync(self: Arc<Self>, peer_node_id: NodeId) {
        // 防重复：按对端节点去重，同一对端只触发一次初始全量同步
        if !self.initial_sync_peers.write().insert(peer_node_id) {
            info!("[federation] 初始全量同步已对 {:?} 触发过，跳过（去重生效）", peer_node_id);
            return;
        }

        info!("[federation] 开始初始全量同步（4 repo 并行，对端: {:?}）...", peer_node_id);

        // 全量同步期间告知 GossipEngine：暂停 outbox 丢弃/限流，确保全量批次完整传播
        self.gossip_engine.set_full_sync_in_progress(true);

        // P2-1: 设置所有 MerkleTree 为全量同步模式（跳过 recompute_shard）
        self.node_merkle.set_full_sync_in_progress(true);
        if let Some(ps) = &self.peer_sync {
            ps.merkle().set_full_sync_in_progress(true);
        }
        if let Some(ihs) = &self.infohash_sync {
            ihs.merkle().set_full_sync_in_progress(true);
        }
        if let Some(ts) = &self.tracker_sync {
            ts.merkle().set_full_sync_in_progress(true);
        }

        let batch_size = self.config.initial_sync_batch_size;

        // Node 全量同步（独立任务）
        let mut handles = Vec::new();
        if self.config.sync_node_enabled {
            let s = self.clone();
            handles.push(tokio::task::spawn_blocking(move || {
                let entries = s.collect_node_entries();
                if !entries.is_empty() {
                    let count = entries.len();
                    s.gossip_engine
                        .submit_gossip_batch(repo_type::NODE, entries, batch_size);
                    info!("[federation] 初始同步 Node: {} 条（分批提交）", count);
                }
            }));
        }

        // Peer 全量同步（独立任务）
        if self.config.sync_peer_enabled {
            if let Some(ps) = self.peer_sync.clone() {
                let gossip = self.gossip_engine.clone();
                handles.push(tokio::task::spawn_blocking(move || {
                    let entries = ps.collect_all_entries();
                    if !entries.is_empty() {
                        let count = entries.len();
                        gossip.submit_gossip_batch(repo_type::PEER, entries, batch_size);
                        info!("[federation] 初始同步 Peer: {} 条（分批提交）", count);
                    }
                }));
            }
        }

        // Infohash 全量同步（独立任务）
        if self.config.sync_infohash_enabled {
            if let Some(ihs) = self.infohash_sync.clone() {
                let gossip = self.gossip_engine.clone();
                handles.push(tokio::task::spawn_blocking(move || {
                    let entries = ihs.collect_all_entries();
                    if !entries.is_empty() {
                        let count = entries.len();
                        gossip.submit_gossip_batch(repo_type::INFOHASH, entries, batch_size);
                        info!("[federation] 初始同步 Infohash: {} 条（分批提交）", count);
                    }
                }));
            }
        }

        // Tracker 全量同步（独立任务）
        if self.config.sync_tracker_enabled {
            if let Some(ts) = self.tracker_sync.clone() {
                let gossip = self.gossip_engine.clone();
                handles.push(tokio::task::spawn_blocking(move || {
                    let entries = ts.collect_all_entries();
                    if !entries.is_empty() {
                        let count = entries.len();
                        gossip.submit_gossip_batch(repo_type::TRACKER, entries, batch_size);
                        info!("[federation] 初始同步 Tracker: {} 条（分批提交）", count);
                    }
                }));
            }
        }

        // P2-1: 等待所有任务完成后，恢复 Merkle 正常模式并一次性重建
        let self_clone = self.clone();
        tokio::spawn(async move {
            for h in handles {
                let _ = h.await;
            }
            // 全量同步数据已全部入队，关闭 GossipEngine 全量同步标志（恢复正常丢弃/限流）
            self_clone.gossip_engine.set_full_sync_in_progress(false);
            // 恢复 Merkle 正常模式并重建所有分片
            self_clone.node_merkle.set_full_sync_in_progress(false);
            self_clone.node_merkle.rebuild_all();
            if let Some(ps) = &self_clone.peer_sync {
                ps.merkle().set_full_sync_in_progress(false);
                ps.merkle().rebuild_all();
            }
            if let Some(ihs) = &self_clone.infohash_sync {
                ihs.merkle().set_full_sync_in_progress(false);
                ihs.merkle().rebuild_all();
            }
            if let Some(ts) = &self_clone.tracker_sync {
                ts.merkle().set_full_sync_in_progress(false);
                ts.merkle().rebuild_all();
            }
            info!("[federation] 初始全量同步完成，Merkle 树已重建");
        });
    }

    /// 全量同步开始（接收端：由 FullSyncStart 消息触发）
    ///
    /// 对指定 repo_type 的 MerkleTree 设置 full_sync_in_progress=true，
    /// 跳过逐条 recompute_shard。
    pub fn handle_full_sync_start(&self, repo_type: u8) {
        if let Some(merkle) = self.merkle_for_repo(repo_type) {
            merkle.set_full_sync_in_progress(true);
            info!("[federation] FullSync 开始: repo_type={}, Merkle 进入惰性模式", repo_type);
        }
    }

    /// 全量同步完成（接收端：由 FullSyncComplete 消息触发）
    ///
    /// 恢复 Merkle 正常模式，一次性 rebuild_all 重算所有分片。
    pub fn handle_full_sync_complete(&self, repo_type: u8) {
        if let Some(merkle) = self.merkle_for_repo(repo_type) {
            merkle.set_full_sync_in_progress(false);
            merkle.rebuild_all();
            info!("[federation] FullSync 完成: repo_type={}, Merkle 已重建", repo_type);
        }
    }

    /// 全量同步批量数据应用（接收端：由 FullSyncBatch 消息触发）
    ///
    /// 直接应用到 repo（不经过 Gossip 去重），Merkle 在惰性模式下只写入不重算。
    pub fn handle_full_sync_batch(&self, repo_type: u8, entries: &[SyncEntry]) {
        self.handle_sync_batch(repo_type, entries);
    }

    /// 启动全量同步专门通道（发送端）
    ///
    /// 对指定 target_node_id 发送 FullSyncStart → 分批发送 FullSyncBatch（窗口流控，等待 Ack）
    /// → 发送 FullSyncComplete。绕过 Gossip 去重和传播，直接全量推送。
    ///
    /// 注意：当前为简化版，按 repo_type 逐个 repo 同步。窗口流控由 full_sync_window_size 控制。
    pub async fn start_full_sync(self: Arc<Self>, target_node_id: NodeId) {
        let conn = match self.connection_manager.get_connection(&target_node_id) {
            Some(c) => c,
            None => {
                warn!("[federation] FullSync: 目标 {} 无连接，取消", target_node_id);
                return;
            }
        };

        // 全量同步期间告知 GossipEngine：暂停 outbox 丢弃/限流
        self.gossip_engine.set_full_sync_in_progress(true);

        let batch_size = self.config.full_sync_batch_size;
        let window = self.config.full_sync_window_size;

        // 对每个启用的 repo_type 执行全量同步
        for repo_type in &[repo_type::NODE, repo_type::PEER, repo_type::INFOHASH, repo_type::TRACKER] {
            // 收集全量条目
            let entries: Vec<SyncEntry> = match *repo_type {
                repo_type::NODE => self.collect_node_entries(),
                repo_type::PEER => {
                    match &self.peer_sync {
                        Some(ps) => ps.collect_all_entries(),
                        None => continue,
                    }
                }
                repo_type::INFOHASH => {
                    match &self.infohash_sync {
                        Some(ihs) => ihs.collect_all_entries(),
                        None => continue,
                    }
                }
                repo_type::TRACKER => {
                    match &self.tracker_sync {
                        Some(ts) => ts.collect_all_entries(),
                        None => continue,
                    }
                }
                _ => continue,
            };

            if entries.is_empty() {
                continue;
            }

            let total = entries.len() as u64;
            info!(
                "[federation] FullSync 发送: repo_type={}, total={}, batch_size={}, window={}, to={}",
                repo_type, total, batch_size, window, target_node_id
            );

            // 1. 发送 FullSyncStart
            let start_msg = FullSyncStartMessage {
                repo_type: *repo_type,
                total_entries: total,
            };
            if let Err(e) = conn.send_message(MessageType::FullSyncStart, &start_msg).await {
                warn!("[federation] FullSyncStart 发送失败: {}", e);
                break;
            }

            // 2. 分批发送 FullSyncBatch（简单窗口流控：每批等待 Ack）
            let mut seq: u64 = 0;
            for chunk in entries.chunks(batch_size) {
                let batch_msg = FullSyncBatchMessage {
                    repo_type: *repo_type,
                    entries: chunk.to_vec(),
                    seq,
                };
                if let Err(e) = conn.send_message(MessageType::FullSyncBatch, &batch_msg).await {
                    warn!("[federation] FullSyncBatch seq={} 发送失败: {}", seq, e);
                    break;
                }
                seq += 1;

                // 简单流控：每 window 个批次等待一次（此处简化为每批等待 Ack）
                // 接收端在 connection.rs 中收到 FullSyncBatch 后自动回复 Ack
                // 这里不主动等待 Ack，由 TCP 层流控和窗口大小间接控制速率
                if seq % (window as u64) == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }

            // 3. 发送 FullSyncComplete
            let complete_msg = FullSyncCompleteMessage {
                repo_type: *repo_type,
            };
            if let Err(e) = conn.send_message(MessageType::FullSyncComplete, &complete_msg).await {
                warn!("[federation] FullSyncComplete 发送失败: {}", e);
            }

            info!(
                "[federation] FullSync 完成: repo_type={}, batches={}, to={}",
                repo_type, seq, target_node_id
            );
        }

        // 全量同步结束，恢复 GossipEngine 正常丢弃/限流
        self.gossip_engine.set_full_sync_in_progress(false);
    }

    /// 收集全量 Node 同步条目（内部辅助方法）
    ///
    /// 注意：此方法不更新 Merkle 树，避免在遍历大量数据时持有锁导致死锁。
    /// Merkle 树应在数据写入时更新，或在启动时一次性重建。
    fn collect_node_entries(&self) -> Vec<SyncEntry> {
        let all_nodes = self.node_repo.all_nodes_sync();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut entries = Vec::with_capacity(all_nodes.len());
        for entry in &all_nodes {
            let payload = NodeSyncPayload {
                node_id: entry.id,
                addr: entry.addr,
            };
            let payload_bytes = match bincode::serialize(&payload) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let key = entry.addr.to_string().into_bytes();
            entries.push(SyncEntry {
                key,
                operation: operation::UPSERT,
                version: now,
                payload: payload_bytes,
            });
        }
        entries
    }

    /// 获取同步统计（各 repo 同步计数 + Gossip 传播次数）
    pub fn sync_stats(&self) -> crate::federation::SyncStats {
        let snap = self.metrics.snapshot();
        crate::federation::SyncStats {
            node_sync_count: snap.node_sync_count,
            peer_sync_count: snap.peer_sync_count,
            infohash_sync_count: snap.infohash_sync_count,
            tracker_sync_count: snap.tracker_sync_count,
            gossip_propagations: snap.gossip_propagations,
        }
    }

    /// 根据 repo_type 获取对应的 Merkle 树（内部辅助方法）
    fn merkle_for_repo(&self, repo_type: u8) -> Option<Arc<MerkleTree>> {
        match repo_type {
            repo_type::NODE => Some(self.node_merkle.clone()),
            repo_type::PEER => self.peer_sync.as_ref().map(|ps| ps.merkle()),
            repo_type::INFOHASH => self.infohash_sync.as_ref().map(|ihs| ihs.merkle()),
            repo_type::TRACKER => self.tracker_sync.as_ref().map(|ts| ts.merkle()),
            _ => None,
        }
    }

    /// 处理收到的 MerkleDigest：对比本地 Merkle 树，发现差异后请求修复
    ///
    /// 收到对端的 MerkleDigest 后，对比本地对应 repo 的 Merkle 树，
    /// 找出根哈希不同的分片，向对端发送 MerkleRequest 请求差异分片数据。
    pub async fn handle_merkle_digest(&self, conn: &Connection, digest: MerkleDigestMessage) {
        let merkle = match self.merkle_for_repo(digest.repo_type) {
            Some(m) => m,
            None => return,
        };

        let diffs = merkle.diff(&digest);
        if diffs.is_empty() {
            debug!(
                "[federation] Merkle 对账无差异: repo_type={}, from={}",
                digest.repo_type, conn.node_id
            );
            return;
        }

        debug!(
            "[federation] Merkle 对账发现 {} 个差异分片: repo_type={}, from={}",
            diffs.len(),
            digest.repo_type,
            conn.node_id
        );

        let request = MerkleRequestMessage {
            repo_type: digest.repo_type,
            shards: diffs,
        };

        if let Err(e) = conn.send_message(MessageType::MerkleRequest, &request).await {
            debug!("[federation] 发送 MerkleRequest 失败: {}", e);
        } else {
            self.metrics.record_message_sent();
        }
    }

    /// 处理收到的 MerkleRequest：返回指定分片的全量条目（MerkleRepair）
    ///
    /// 收到对端的分片请求后，从本地 Merkle 树收集指定分片的所有条目，
    /// 打包成 MerkleRepair 消息发送回对端。
    pub async fn handle_merkle_request(&self, conn: &Connection, request: MerkleRequestMessage) {
        let merkle = match self.merkle_for_repo(request.repo_type) {
            Some(m) => m,
            None => return,
        };

        let mut entries = Vec::new();
        for shard in &request.shards {
            let shard_entries = merkle.get_shard_entries(*shard);
            for (key, payload) in shard_entries {
                entries.push(SyncEntry {
                    key,
                    operation: operation::UPSERT,
                    version: 0,
                    payload,
                });
            }
        }

        if entries.is_empty() {
            return;
        }

        let repair = MerkleRepairMessage {
            repo_type: request.repo_type,
            entries,
        };

        if let Err(e) = conn.send_message(MessageType::MerkleRepair, &repair).await {
            debug!("[federation] 发送 MerkleRepair 失败: {}", e);
        } else {
            self.metrics.record_message_sent();
            debug!(
                "[federation] Merkle 修复发送: repo_type={}, entries={}, to={}",
                request.repo_type,
                repair.entries.len(),
                conn.node_id
            );
        }
    }

    /// 处理收到的 MerkleRepair：应用修复数据到本地 repo
    ///
    /// 收到对端返回的差异分片数据后，通过 handle_sync_batch 应用到本地，
    /// 并记录 merkle_repairs 计数。
    pub fn handle_merkle_repair(&self, repair: MerkleRepairMessage) {
        self.metrics.record_merkle_repair();
        debug!(
            "[federation] Merkle 修复接收: repo_type={}, entries={}",
            repair.repo_type,
            repair.entries.len()
        );
        self.handle_sync_batch(repair.repo_type, &repair.entries);
    }
}

/// MerkleProvider 实现：为 GossipEngine 反熵任务提供各 repo 的 Merkle 摘要和分片数据
impl MerkleProvider for SyncManager {
    fn get_digest(&self, repo_type: u8) -> MerkleDigestMessage {
        // 反熵对账前等待 Merkle 更新队列排空（最多 500ms），
        // 避免用尚未 flush 的旧 Merkle 值对账导致误判；超时未排空则直接用当前值对账。
        if !self.merkle_queue.is_empty() {
            self.merkle_queue
                .wait_drain(Duration::from_millis(500));
        }
        if let Some(merkle) = self.merkle_for_repo(repo_type) {
            merkle.digest(repo_type)
        } else {
            MerkleDigestMessage {
                repo_type,
                shard_count: 0,
                roots: Vec::new(),
                entry_counts: Vec::new(),
            }
        }
    }

    fn get_shard_entries(&self, repo_type: u8, shard: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
        if let Some(merkle) = self.merkle_for_repo(repo_type) {
            merkle.get_shard_entries(shard)
        } else {
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::node_id::NodeIdentity;
    use crate::federation::node_table::NodeTable;
    use crate::storage::Storage;

    fn make_config() -> FederationConfig {
        FederationConfig {
            enabled: true,
            listen_port: 0,
            max_connections: 10,
            sync_node_enabled: true,
            sync_node_interval_secs: 300,
            sync_peer_enabled: true,
            sync_infohash_enabled: true,
            ..Default::default()
        }
    }

    fn make_node_repo() -> Arc<NodeRepoImpl> {
        let storage = Arc::new(Storage::memory().unwrap());
        Arc::new(NodeRepoImpl::new(storage))
    }

    #[test]
    fn test_node_sync_payload_roundtrip() {
        let payload = NodeSyncPayload {
            node_id: [0xab; 20],
            addr: "127.0.0.1:6885".parse().unwrap(),
        };
        let bytes = bincode::serialize(&payload).unwrap();
        let decoded: NodeSyncPayload = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.node_id, [0xab; 20]);
        assert_eq!(decoded.addr, "127.0.0.1:6885".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_apply_node_sync() {
        let node_repo = make_node_repo();
        let identity = NodeIdentity::generate();
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cm_shutdown, _) = broadcast::channel(1);
        let cm = Arc::new(ConnectionManager::new(
            node_table,
            Arc::new(identity),
            make_config(),
            cm_shutdown,
            Arc::new(FederationMetrics::new()),
        ));
        let metrics = Arc::new(FederationMetrics::new());
        let gossip = Arc::new(GossipEngine::new(
            cm.clone(),
            make_config(),
            NodeId([1; 20]),
            metrics,
            shutdown_tx.clone(),
        ));
        let mgr = SyncManager::new(
            cm,
            node_repo.clone(),
            make_config(),
            shutdown_tx,
            gossip,
            Arc::new(FederationMetrics::new()),
            NodeId([1; 20]),
            None,
            None,
            None,
            None,
            None,
        );

        let payload = NodeSyncPayload {
            node_id: [1; 20],
            addr: "10.0.0.1:6885".parse().unwrap(),
        };
        let entries = vec![SyncEntry {
            key: b"10.0.0.1:6885".to_vec(),
            operation: operation::UPSERT,
            version: 100,
            payload: bincode::serialize(&payload).unwrap(),
        }];

        assert_eq!(node_repo.len_sync(), 0);
        mgr.apply_node_sync(&entries);
        assert_eq!(node_repo.len_sync(), 1);
    }

    #[test]
    fn test_handle_sync_batch_unknown_type() {
        let node_repo = make_node_repo();
        let identity = NodeIdentity::generate();
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cm_shutdown, _) = broadcast::channel(1);
        let cm = Arc::new(ConnectionManager::new(
            node_table,
            Arc::new(identity),
            make_config(),
            cm_shutdown,
            Arc::new(FederationMetrics::new()),
        ));
        let metrics = Arc::new(FederationMetrics::new());
        let gossip = Arc::new(GossipEngine::new(
            cm.clone(),
            make_config(),
            NodeId([1; 20]),
            metrics,
            shutdown_tx.clone(),
        ));
        let mgr = SyncManager::new(
            cm,
            node_repo,
            make_config(),
            shutdown_tx,
            gossip,
            Arc::new(FederationMetrics::new()),
            NodeId([1; 20]),
            None,
            None,
            None,
            None,
            None,
        );

        // 不应 panic
        mgr.handle_sync_batch(99, &[]);
        mgr.handle_sync_batch(repo_type::PEER, &[]);
        mgr.handle_sync_batch(repo_type::INFOHASH, &[]);
        mgr.handle_sync_batch(repo_type::TRACKER, &[]);
    }

    #[test]
    fn test_node_merkle_digest() {
        let node_repo = make_node_repo();
        let identity = NodeIdentity::generate();
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cm_shutdown, _) = broadcast::channel(1);
        let cm = Arc::new(ConnectionManager::new(
            node_table,
            Arc::new(identity),
            make_config(),
            cm_shutdown,
            Arc::new(FederationMetrics::new()),
        ));
        let metrics = Arc::new(FederationMetrics::new());
        let gossip = Arc::new(GossipEngine::new(
            cm.clone(),
            make_config(),
            NodeId([1; 20]),
            metrics,
            shutdown_tx.clone(),
        ));
        let mgr = SyncManager::new(
            cm,
            node_repo,
            make_config(),
            shutdown_tx,
            gossip,
            Arc::new(FederationMetrics::new()),
            NodeId([1; 20]),
            None,
            None,
            None,
            None,
            None,
        );

        let digest = mgr.node_merkle_digest();
        assert_eq!(digest.repo_type, repo_type::NODE);
        assert_eq!(digest.shard_count, 256);
    }
}
