//! 同步管理器
//!
//! 阶段2扩展：集成 Gossip 引擎、PeerRepo 同步、InfohashRepo 同步。
//! 阶段1的 NodeRepo 同步保留。

#![allow(clippy::type_complexity)]

pub mod infohash_sync;
pub mod merkle_updater;
pub mod peer_sync;
pub mod shard_sync_engine;
pub mod tracker_sync;

use std::collections::{HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, oneshot};
use tracing::{debug, info, warn};

use crate::event_bus::EventBus;
use crate::federation::config::FederationConfig;
use crate::federation::connection::{Connection, ConnectionManager};
use crate::federation::gossip::GossipEngine;
use crate::federation::merkle::{MerkleProvider, MerkleTree};
use crate::federation::metrics::FederationMetrics;
use crate::federation::node_id::NodeId;
use crate::federation::protocol::*;
use crate::federation::relay::RelayManager;
use crate::federation::sync::infohash_sync::InfohashSync;
use crate::federation::sync::merkle_updater::MerkleUpdateQueue;
use crate::federation::sync::peer_sync::PeerSync;
use crate::federation::sync::shard_sync_engine::{LayeredCompareSession, ShardSyncEngine};
use crate::federation::sync::tracker_sync::TrackerSync;
use crate::storage::{InfohashRepoImpl, NodeRepoImpl, PeerRepoImpl, TrackerRepoImpl};

/// 启动时全量重建 Merkle 树前的一次性延迟
const MERKLE_REBUILD_STARTUP_DELAY: Duration = Duration::from_secs(5);
/// 全量同步完成后恢复 Gossip 的一次性延迟
const GOSSIP_RESUME_DELAY: Duration = Duration::from_secs(30);
/// 全量同步发送流控间隔
const FULL_SYNC_FLOW_CONTROL_INTERVAL: Duration = Duration::from_millis(10);
/// 反熵对账前等待 Merkle 更新队列排空的上限
const MERKLE_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);

/// 节点同步负载
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeSyncPayload {
    node_id: [u8; 20],
    addr: SocketAddr,
}

/// 构建 Node 同步条目的 (key, payload_bytes, data_hash)。
/// 格式与 collect_node_entries 一致，供 NodeRepoImpl 本地写入后更新 Merkle / 提交 Gossip。
///
/// data_hash 公式与 db.rs `load_all_node_keys_hashes` 完全一致：
/// `blake3(node_id || ip.as_bytes() || port_le)`，保证冷热同构。
pub(crate) fn build_node_sync_entry(
    node_id: [u8; 20],
    addr: SocketAddr,
) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let payload = NodeSyncPayload { node_id, addr };
    let payload_bytes = bincode::serialize(&payload).ok()?;
    let key = addr.to_string().into_bytes();
    // data_hash = blake3(node_id || ip || port_le)
    let mut buf = Vec::with_capacity(node_id.len() + 16 + 2);
    buf.extend_from_slice(node_id.as_slice());
    buf.extend_from_slice(addr.ip().to_string().as_bytes());
    buf.extend_from_slice(&addr.port().to_le_bytes());
    let data_hash = blake3::hash(&buf).as_bytes().to_vec();
    Some((key, payload_bytes, data_hash))
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
    /// 各 repo 直接引用（用于 local_entry_counts 统计真实数据量，而非 Merkle 树中的条目数）
    peer_repo: Option<Arc<PeerRepoImpl>>,
    infohash_repo: Option<Arc<InfohashRepoImpl>>,
    tracker_repo: Option<Arc<TrackerRepoImpl>>,
    /// Merkle 树异步批量更新队列（apply 时入队，后台任务定期 flush）
    merkle_queue: Arc<MerkleUpdateQueue>,
    config: FederationConfig,
    metrics: Arc<FederationMetrics>,
    shutdown: broadcast::Sender<()>,
    /// 已触发初始全量同步的对端节点集合（按对端去重，每个对端只同步一次）
    initial_sync_peers: RwLock<HashSet<NodeId>>,
    /// 差量同步状态：每 repo 独立并发，key=repo_type。
    /// value=(peer_id, start_time, last_progress_time)。
    /// 大差异（Merkle 差异分片≥20%）时触发，只拉差异部分。
    /// 全局最多4个并发（每repo1个）。
    diff_sync_states: RwLock<FxHashMap<u8, (NodeId, Instant, Instant)>>,
    /// 最近变更缓冲区（Push-Pull Gossip 用）：(repo_type, key, version)
    /// 固定容量 500，写入时追加，超过容量弹出最旧。
    recent_changes: RwLock<VecDeque<(u8, Vec<u8>, u64)>>,
    /// 已见过的对端 Merkle 摘要（对端 node_id -> 各 repo 条目数），
    /// 供数据源选择时按「数据最完整（与本地差异最大）」排序。
    peer_digests: RwLock<FxHashMap<NodeId, Vec<u32>>>,
    /// DiffSync key 列表请求累计缓冲（请求方侧）：key=(对端, repo_type)。
    /// 数据服务器分片发送 key 列表，请求方收齐到 is_last 后才对比本地并回传缺失 key。
    diff_key_req_buffer: RwLock<FxHashMap<(NodeId, u8), DiffKeyReqBuffer>>,
    /// DiffSync key 列表响应待收集（数据服务器侧）：key=(对端, repo_type)。
    /// 数据服务器发出 key 列表后在此挂起 oneshot，等待请求方回传的缺失 key 分片收齐。
    pending_diff_key_resp: RwLock<FxHashMap<(NodeId, u8), PendingKeyResp>>,
    /// 活跃的分片同步引擎：key=(peer, repo_type)。
    /// 每个 peer+repo 同时只允许一个分片同步引擎运行（互斥）。
    active_shard_engines: RwLock<FxHashMap<(NodeId, u8), Arc<ShardSyncEngine>>>,
    /// 分片同步 hash 列表累计缓冲（接收方侧）：key=(对端, repo_type, l2_shard)。
    /// 数据服务器分片发送 (key, data_hash) 列表，接收方收齐到 is_last 后才对比本地并回复缺失 key。
    shard_hash_list_buffer: RwLock<FxHashMap<(NodeId, u8, u32), Vec<(Vec<u8>, Vec<u8>)>>>,
    /// 分层 Merkle 对比会话：key=(peer, repo_type)。
    /// 追踪正在进行的 L0→L1→L2 分层对比流程。
    layered_compare_sessions: RwLock<FxHashMap<(NodeId, u8), LayeredCompareSession>>,
    /// 增量同步互斥标记：每个 repo 同时只进行一次增量同步。
    incremental_sync_active: RwLock<FxHashSet<u8>>,
}

/// DiffSync key 列表请求累计缓冲（请求方侧）
#[derive(Default)]
struct DiffKeyReqBuffer {
    shards: Vec<u16>,
    keys: Vec<Vec<u8>>,
}

/// DiffSync key 列表响应待收集（数据服务器侧）
struct PendingKeyResp {
    ///  oneshot 发送端（收到最后一片缺失 key 后触发，唤醒等待中的推送任务）
    tx: Option<oneshot::Sender<Vec<Vec<u8>>>>,
    /// 已累计的缺失 key 分片
    buf: Vec<Vec<u8>>,
}

impl SyncManager {
    /// 创建同步管理器
    #[allow(clippy::too_many_arguments)]
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

        // 保留 repo 引用供 local_entry_counts 使用（后续 if let 会 move 原始变量）
        let peer_repo_clone = peer_repo.clone();
        let infohash_repo_clone = infohash_repo.clone();
        let tracker_repo_clone = tracker_repo.clone();

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
            peer_repo: peer_repo_clone,
            infohash_repo: infohash_repo_clone,
            tracker_repo: tracker_repo_clone,
            merkle_queue,
            config,
            metrics,
            shutdown,
            initial_sync_peers: RwLock::new(HashSet::new()),
            diff_sync_states: RwLock::new(FxHashMap::default()),
            recent_changes: RwLock::new(VecDeque::with_capacity(500)),
            peer_digests: RwLock::new(FxHashMap::default()),
            diff_key_req_buffer: RwLock::new(FxHashMap::default()),
            pending_diff_key_resp: RwLock::new(FxHashMap::default()),
            active_shard_engines: RwLock::new(FxHashMap::default()),
            shard_hash_list_buffer: RwLock::new(FxHashMap::default()),
            layered_compare_sessions: RwLock::new(FxHashMap::default()),
            incremental_sync_active: RwLock::new(FxHashSet::default()),
        }
    }

    /// 启动 Node 同步后台任务（已迁移到 TaskScheduler）
    ///
    /// 周期性 Node 同步由 TaskScheduler 调用 `do_node_sync()` 驱动。
    /// do_node_sync 已退役为空实现（本地新节点由 NodeRepoImpl.add_node_sync 统一更新）。
    pub fn spawn_node_sync(self: Arc<Self>) {
        // 已迁移：Node 同步周期任务由 TaskScheduler 调度 do_node_sync()
    }

    /// 启动时从数据库全量重建 Merkle 树（延迟5秒）。
    ///
    /// 纯 DB 驱动：直接从存储层加载 (key, data_hash) 后调用
    /// `rebuild_cold_from_db`，不从内存喂 Merkle（内存只有热+温数据，不完整）。
    pub fn spawn_merkle_rebuilder(self: Arc<Self>) {
        tokio::spawn(async move {
            // [ALLOWED-SLEEP] 启动时一次性延迟重建，非周期性
            tokio::time::sleep(MERKLE_REBUILD_STARTUP_DELAY).await;
            info!("[federation] 开始从数据库全量重建 Merkle 树");

            // Node：从 DB 加载 (key, data_hash) 后冷重算
            {
                let storage = self.node_repo.storage();
                match storage.load_all_node_keys_hashes() {
                    Ok(keys_hashes) => {
                        self.node_merkle().rebuild_cold_from_db(&keys_hashes);
                        info!(
                            "[federation] Node Merkle 从 DB 重建: {} 条",
                            keys_hashes.len()
                        );
                    }
                    Err(e) => warn!("[federation] Node Merkle 从 DB 重建失败: {}", e),
                }
            }
            // Peer：从 DB 加载
            if let Some(pr) = &self.peer_repo {
                let storage = pr.storage();
                match storage.load_all_peer_keys_hashes() {
                    Ok(keys_hashes) => {
                        if let Some(m) = self.peer_merkle() {
                            m.rebuild_cold_from_db(&keys_hashes);
                            info!(
                                "[federation] Peer Merkle 从 DB 重建: {} 条",
                                keys_hashes.len()
                            );
                        }
                    }
                    Err(e) => warn!("[federation] Peer Merkle 从 DB 重建失败: {}", e),
                }
            }
            // Infohash：从 DB 加载
            if let Some(ir) = &self.infohash_repo {
                let storage = ir.storage();
                match storage.load_all_infohash_keys_hashes() {
                    Ok(keys_hashes) => {
                        if let Some(m) = self.infohash_merkle() {
                            m.rebuild_cold_from_db(&keys_hashes);
                            info!(
                                "[federation] Infohash Merkle 从 DB 重建: {} 条",
                                keys_hashes.len()
                            );
                        }
                    }
                    Err(e) => {
                        warn!("[federation] Infohash Merkle 从 DB 重建失败: {}", e)
                    }
                }
            }
            // Tracker：从 DB 加载
            if let Some(tr) = &self.tracker_repo {
                let storage = tr.storage();
                match storage.load_all_tracker_keys_hashes() {
                    Ok(keys_hashes) => {
                        if let Some(m) = self.tracker_merkle() {
                            m.rebuild_cold_from_db(&keys_hashes);
                            info!(
                                "[federation] Tracker Merkle 从 DB 重建: {} 条",
                                keys_hashes.len()
                            );
                        }
                    }
                    Err(e) => {
                        warn!("[federation] Tracker Merkle 从 DB 重建失败: {}", e)
                    }
                }
            }
            info!("[federation] 所有 Merkle 树从数据库重建完成");
        });
    }

    /// 启动 Merkle 异步批量 flush 后台任务（事件驱动部分）
    ///
    /// 定期批量 flush 已迁移到 TaskScheduler 调用 `merkle_flush_tick()`。
    /// 此处仅保留入队通知（Notify）的即时唤醒路径和关闭信号处理。
    /// 关闭信号到达时先 flush 一次再退出，保证退出前数据不丢失。
    pub fn spawn_merkle_flusher(self: Arc<Self>) {
        let batch_threshold = self.config.merkle_async_update_batch_size;
        let queue = self.merkle_queue.clone();
        let mut shutdown_rx = self.shutdown.subscribe();

        // [ALLOWED-SLEEP] Merkle flush 事件驱动消费者：监听 queue.notify() 入队通知即时 flush，
        // 非周期性轮询（定期批量 flush 已迁移到 TaskScheduler 调用 merkle_flush_tick()）。
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = queue.notify().notified() => {
                        // 入队通知唤醒：即使未达阈值也 flush（apply 批量本身已攒批）
                        self.clone().flush_merkle_queue(batch_threshold);
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] Merkle flush 事件任务收到关闭信号，退出前 flush");
                        self.flush_merkle_queue(batch_threshold);
                        break;
                    }
                }
            }
        });
        debug!(
            "[federation] Merkle 事件驱动 flush 任务已启动（定期 flush 由 TaskScheduler 调度 merkle_flush_tick）"
        );
    }

    /// 单次 Merkle flush tick（由 TaskScheduler 定期调用）。
    ///
    /// 将积压在队列中的 Merkle 更新按 repo_type 分组批量写入 Merkle 树。
    /// 事件驱动的即时 flush 由 `spawn_merkle_flusher` 中的 Notify 路径负责。
    pub fn merkle_flush_tick(&self) {
        let batch_threshold = self.config.merkle_async_update_batch_size;
        self.flush_merkle_queue(batch_threshold);
    }

    /// 批量 flush 队列中的 Merkle 更新：按 repo_type 分组后调用对应 merkle.update_batch
    fn flush_merkle_queue(&self, _batch_threshold: usize) {
        let items = self.merkle_queue.drain();
        if items.is_empty() {
            return;
        }

        // 按 repo_type 分组（item 为 (key, payload, data_hash) 三元组）
        let mut node_batch: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::new();
        let mut peer_batch: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::new();
        let mut infohash_batch: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::new();
        let mut tracker_batch: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::new();

        for (repo_type, key, payload, data_hash) in items {
            match repo_type {
                repo_type::NODE => node_batch.push((key, payload, data_hash)),
                repo_type::PEER => peer_batch.push((key, payload, data_hash)),
                repo_type::INFOHASH => infohash_batch.push((key, payload, data_hash)),
                repo_type::TRACKER => tracker_batch.push((key, payload, data_hash)),
                other => {
                    warn!("[federation] Merkle flush: 未知 repo_type={}, 丢弃", other);
                }
            }
        }

        // 逐 repo 调用 update_batch（一次写锁批量插入，只重算受影响分片）
        if !node_batch.is_empty() {
            let refs: Vec<(&[u8], &[u8], &[u8])> = node_batch
                .iter()
                .map(|(k, p, h)| (k.as_slice(), p.as_slice(), h.as_slice()))
                .collect();
            self.node_merkle.update_batch(&refs);
        }
        if !peer_batch.is_empty() {
            if let Some(ps) = &self.peer_sync {
                let refs: Vec<(&[u8], &[u8], &[u8])> = peer_batch
                    .iter()
                    .map(|(k, p, h)| (k.as_slice(), p.as_slice(), h.as_slice()))
                    .collect();
                ps.merkle().update_batch(&refs);
            }
        }
        if !infohash_batch.is_empty() {
            if let Some(ihs) = &self.infohash_sync {
                let refs: Vec<(&[u8], &[u8], &[u8])> = infohash_batch
                    .iter()
                    .map(|(k, p, h)| (k.as_slice(), p.as_slice(), h.as_slice()))
                    .collect();
                ihs.merkle().update_batch(&refs);
            }
        }
        if !tracker_batch.is_empty() {
            if let Some(ts) = &self.tracker_sync {
                let refs: Vec<(&[u8], &[u8], &[u8])> = tracker_batch
                    .iter()
                    .map(|(k, p, h)| (k.as_slice(), p.as_slice(), h.as_slice()))
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
    pub async fn do_node_sync(self: Arc<Self>) {}

    /// 处理收到的同步批量消息（阶段1 SyncBatch 协议）
    pub fn handle_sync_batch(&self, repo_type: u8, entries: &[SyncEntry]) {
        // 【回环修复】此处不再把入站条目登记进 recent_changes。
        // recent_changes 是「本地最近变更广告位」，只应装本地产生的变更（本地新节点走
        // propagate->submit_gossip 直接传播）。若把对端发来的条目登记进去，push_pull_tick /
        // incremental_sync_tick 会把它当作本地变更再用 GossipDigest 广告回对端，对端 pull
        // 回来又登记、再广告 -> A->B->A 无限拉取回环。
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
        warn!(
            "[federation][DIAG] SyncManager::handle_gossip_batch ENTER: repo_type={}, entries={}",
            batch.repo_type,
            batch.entries.len()
        );
        let entries = self.gossip_engine.handle_gossip_batch(batch.clone());
        warn!(
            "[federation][perf] handle_gossip_batch returned: entries_len={}, repo_type={}",
            entries.len(),
            batch.repo_type
        );
        if entries.is_empty() {
            return;
        }
        self.handle_sync_batch(batch.repo_type, &entries);
    }

    /// 供 ConnectionManager::flush_gossip_buffer 在分组/spawn task 前提前过滤重复 batch。
    /// 只读检查 GossipEngine.seen_msgs（不插入），重复 batch 直接丢弃，避免后续
    /// 分组、task spawn、clone 的 CPU 开销。handle_gossip_batch 中的 check_and_put
    /// 仍保留作为兜底（防止本检查与实际处理之间的竞态）。
    pub fn is_batch_seen(&self, batch: &GossipBatchMessage) -> bool {
        self.gossip_engine
            .is_batch_seen(NodeId(batch.origin), batch.msg_id)
    }

    /// 应用 Node 同步数据
    pub fn apply_node_sync(&self, entries: &[SyncEntry]) {
        warn!(
            "[federation][perf] apply_node_sync ENTER: entries_len={}",
            entries.len()
        );
        if entries.len() > 100 {
            warn!(
                "[federation][perf] apply_node_sync start: entries={}",
                entries.len()
            );
        }
        let total_start = Instant::now();

        // 第一遍：过滤 DELETE / 反序列化失败的条目，收集有效 payload。
        // （历史上这里还收集 (key, data_hash) 去标记 Merkle dirty，已移除——见下方说明。）
        let deserialize_start = Instant::now();
        let mut items: Vec<([u8; 20], SocketAddr)> = Vec::new();
        let mut deletes: Vec<SocketAddr> = Vec::new();
        let mut applied = 0;
        for entry in entries {
            if entry.operation == operation::DELETE {
                // 删除墓碑：key 为 addr 字符串，解析后从本地移除
                // （入站路径不回播，避免 A->B->A 回环）
                if let Ok(s) = std::str::from_utf8(&entry.key) {
                    if let Ok(addr) = s.parse::<SocketAddr>() {
                        deletes.push(addr);
                        applied += 1;
                    }
                }
                continue;
            }
            let payload: NodeSyncPayload = match bincode::deserialize(&entry.payload) {
                Ok(p) => p,
                Err(_) => continue,
            };
            // LWW 检查：本地已存在该节点且 entry 版本未知（==0，旧版/未知）时跳过，
            // 避免版本缺失的旧数据覆盖本地较新数据。version>0 的条目走 upsert 语义
            // （真正的时间戳对比需 DB 存 version 字段，当前不修改 schema）。
            if self.node_repo.contains_sync(payload.addr) && entry.version == 0 {
                continue;
            }
            items.push((payload.node_id, payload.addr));
            applied += 1;
        }
        let deserialize_elapsed = deserialize_start.elapsed();

        // 第二遍：一次写锁批量写入（调用内部方法，不触发 Merkle/Gossip，避免回环）
        let repo_start = Instant::now();
        if !items.is_empty() {
            self.node_repo.add_nodes_batch_internal(&items);
        }
        if !deletes.is_empty() {
            let removed = self.node_repo.remove_nodes_batch_internal(&deletes);
            if removed > 0 {
                debug!(
                    "[federation] Node 同步删除 {} 条（收到 {} 条墓碑）",
                    removed,
                    deletes.len()
                );
            }
        }
        let repo_elapsed = repo_start.elapsed();

        // 【回环修复】入站 apply 不再调用 node_merkle.update_incremental_batch。
        // 该调用只会把这批条目所属 L2 分片标 dirty；dirty_l2 的唯一消费者是
        // incremental_sync_tick（它会把 dirty 分片整批推回对端）。入站数据本就是对端发来的，
        // 再标 dirty 并推回 -> A->B->A 无限回环。本地新节点的 merkle dirty 由
        // NodeRepoImpl.propagate 的 update_batch 负责，与本入站路径无关。
        let total_elapsed = total_start.elapsed();

        warn!(
            "[federation][perf] apply_node_sync: count={} deserialize={}ms repo_write={}ms total={}ms",
            entries.len(),
            deserialize_elapsed.as_millis(),
            repo_elapsed.as_millis(),
            total_elapsed.as_millis()
        );

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

    /// 触发初始全量推送（本节点作为发送方，把本地数据通过 Gossip 推出去）。
    ///
    /// Node/Peer/Infohash/Tracker 四个 repo 并行执行，每个 repo 独立 spawn_blocking
    /// 收集数据，按配置化 batch_size 通过 Gossip 批量提交（fanout 到所有已连接邻居）。
    ///
    /// 纯对等架构下，本方法只在收到对端的 FullSyncRequest（handle_full_sync_request）时
    /// 被调用，或作为兼容入口由 on_connection_ready 在旧路径下触发；连接建立不再自动调用，
    /// 避免每个节点对每个入站连接都重复推送（旧行为导致每个节点触发 2 次全量同步）。
    ///
    /// 标志拆分（P0-1）：发送端只设置 sending_full_sync，暂停 Merkle 增量更新以保证一致性，
    /// 但**不影响**接收端的 Gossip 转发。
    pub fn trigger_initial_sync(self: Arc<Self>, peer_node_id: NodeId) {
        // 防重复：按对端节点去重，同一对端只推送一次
        if !self.initial_sync_peers.write().insert(peer_node_id) {
            info!(
                "[federation] 初始全量推送已对 {:?} 触发过，跳过（去重生效）",
                peer_node_id
            );
            return;
        }

        info!(
            "[federation] 开始初始全量推送（4 repo 并行，对端: {:?}）...",
            peer_node_id
        );

        self.mark_sending_full_sync(true);
        self.run_full_sync_push();
    }

    /// 标记发送端全量同步状态（GossipEngine 放开限流/outbox 防护 + 所有 Merkle 暂停增量更新）。
    fn mark_sending_full_sync(&self, v: bool) {
        self.gossip_engine.set_sending_full_sync(v);
        self.node_merkle.set_sending_full_sync(v);
        if let Some(ps) = &self.peer_sync {
            ps.merkle().set_sending_full_sync(v);
        }
        if let Some(ihs) = &self.infohash_sync {
            ihs.merkle().set_sending_full_sync(v);
        }
        if let Some(ts) = &self.tracker_sync {
            ts.merkle().set_sending_full_sync(v);
        }
    }

    /// 执行全量数据收集 + Gossip 批量推送（4 repo 并行），完成后等待 outbox 排空并恢复标志。
    /// 与具体对端解耦：数据通过 Gossip fanout 传播给所有已连接邻居。
    fn run_full_sync_push(self: Arc<Self>) {
        let batch_size = self.config.initial_sync_batch_size;

        let mut handles = Vec::new();

        // Node 全量同步（独立任务）
        if self.config.sync_node_enabled {
            let s = self.clone();
            handles.push(tokio::task::spawn_blocking(move || -> usize {
                let entries = s.collect_node_entries();
                if entries.is_empty() {
                    return 0;
                }
                let count = entries.len();
                s.gossip_engine
                    .submit_gossip_batch(repo_type::NODE, entries, batch_size);
                info!("[federation] 初始同步 Node: {} 条（分批提交）", count);
                count
            }));
        }

        // Peer 全量同步（独立任务）
        if self.config.sync_peer_enabled {
            if let Some(ps) = self.peer_sync.clone() {
                let gossip = self.gossip_engine.clone();
                handles.push(tokio::task::spawn_blocking(move || -> usize {
                    let entries = ps.collect_all_entries();
                    if entries.is_empty() {
                        return 0;
                    }
                    let count = entries.len();
                    gossip.submit_gossip_batch(repo_type::PEER, entries, batch_size);
                    info!("[federation] 初始同步 Peer: {} 条（分批提交）", count);
                    count
                }));
            }
        }

        // Infohash 全量同步（独立任务）
        if self.config.sync_infohash_enabled {
            if let Some(ihs) = self.infohash_sync.clone() {
                let gossip = self.gossip_engine.clone();
                handles.push(tokio::task::spawn_blocking(move || -> usize {
                    let entries = ihs.collect_all_entries();
                    if entries.is_empty() {
                        return 0;
                    }
                    let count = entries.len();
                    gossip.submit_gossip_batch(repo_type::INFOHASH, entries, batch_size);
                    info!("[federation] 初始同步 Infohash: {} 条（分批提交）", count);
                    count
                }));
            }
        }

        // Tracker 全量同步（独立任务）
        if self.config.sync_tracker_enabled {
            if let Some(ts) = self.tracker_sync.clone() {
                let gossip = self.gossip_engine.clone();
                handles.push(tokio::task::spawn_blocking(move || -> usize {
                    let entries = ts.collect_all_entries();
                    if entries.is_empty() {
                        return 0;
                    }
                    let count = entries.len();
                    gossip.submit_gossip_batch(repo_type::TRACKER, entries, batch_size);
                    info!("[federation] 初始同步 Tracker: {} 条（分批提交）", count);
                    count
                }));
            }
        }

        // P2-1: 等待所有任务完成后，恢复 Merkle 正常模式并一次性重建
        let self_clone = self.clone();
        tokio::spawn(async move {
            let mut total_entries: usize = 0;
            for h in handles {
                if let Ok(n) = h.await {
                    total_entries += n;
                }
            }
            // 数据收集已全部入队，但 outbox 中可能仍积压大量待传播 batch。
            // 先等待 outbox 排空，再关闭全量同步标志，避免洪峰未发完就回退到常规限流拖慢收尾。
            // 超时按总条目数动态估算：按 1万条/秒，下限 30s，上限 1800s（30分钟）。
            let timeout_secs = ((total_entries / 10000) as u64).clamp(30, 1800);
            info!(
                "[federation] 初始同步总条目 {}, wait_outbox_empty 超时 {}s",
                total_entries, timeout_secs
            );
            if !self_clone
                .gossip_engine
                .wait_outbox_empty(Duration::from_secs(timeout_secs))
                .await
            {
                warn!("[federation] initial push outbox not empty after {}s timeout, forcing sending_full_sync=false", timeout_secs);
            }
            // outbox 已排空，关闭发送端全量标志并重建所有 Merkle 分片
            self_clone.clear_sending_and_rebuild_all();
        });
    }

    /// 关闭发送端全量标志并重建所有 Merkle 分片（推送完成后调用一次）。
    fn clear_sending_and_rebuild_all(&self) {
        self.gossip_engine.set_sending_full_sync(false);
        self.node_merkle.set_sending_full_sync(false);
        self.node_merkle.rebuild_all();
        if let Some(ps) = &self.peer_sync {
            ps.merkle().set_sending_full_sync(false);
            ps.merkle().rebuild_all();
        }
        if let Some(ihs) = &self.infohash_sync {
            ihs.merkle().set_sending_full_sync(false);
            ihs.merkle().rebuild_all();
        }
        if let Some(ts) = &self.tracker_sync {
            ts.merkle().set_sending_full_sync(false);
            ts.merkle().rebuild_all();
        }
        info!("[federation] 初始全量推送完成，Merkle 树已重建");
    }

    /// 处理收到的 PeerInfo（握手后对端立即发送的本地条目数）。
    ///
    /// 将对端各 repo 条目数记录到 peer_digests，供全量同步数据源选择时
    /// 判断哪个节点数据最完整。PeerInfo 在握手后立即到达，远早于 60s 反熵
    /// 的 MerkleDigest，确保数据源选择时有真实数据可用。
    pub fn handle_peer_info(&self, from_node_id: NodeId, counts: Vec<u32>) {
        let mut digests = self.peer_digests.write();
        let entry = digests.entry(from_node_id).or_insert_with(|| vec![0u32; 4]);
        for (i, &c) in counts.iter().take(4).enumerate() {
            entry[i] = c;
        }
        let total: u64 = entry.iter().map(|c| *c as u64).sum();
        debug!(
            "[federation] 收到 PeerInfo from={}, 条目总数={}",
            from_node_id, total
        );
    }

    /// 触发差异全量同步（接收方），按 repo 独立触发、独立并发。
    ///
    /// 由 Merkle 反熵驱动：当检测到与某对端的某 repo 差异分片≥20%时调用。
    /// 每 repo 最多1个差量同步并发，全局最多4个并行。
    async fn trigger_diff_sync(self: Arc<Self>, peer: NodeId, repo_type: u8) {
        if !self.try_start_diff_sync(peer, repo_type) {
            info!(
                "[federation] 差异全量同步被跳过：repo={} 已有同步进行中（请求对端={}）",
                repo_type, peer
            );
            return;
        }

        info!(
            "[federation] 触发差异全量同步，repo={}, 对端={}",
            repo_type, peer
        );
        self.gossip_engine.set_receiving_full_sync(true);

        let digest = self.get_digest(repo_type);
        let req = DiffSyncRequestMessage { digest };
        let sent = match self.connection_manager.get_connection(&peer) {
            Some(conn) => conn
                .send_message(MessageType::DiffSyncRequest, &req)
                .await
                .is_ok(),
            None => false,
        };
        if !sent {
            warn!(
                "[federation] DiffSyncRequest 发送到 {} 失败（repo={}）",
                peer, repo_type
            );
            self.finish_diff_sync(repo_type);
            return;
        }
        self.metrics.record_message_sent();
        self.clone().spawn_diff_sync_watcher(repo_type);

        let settle_ms = self.config.full_sync_receiving_settle_ms;
        let self_clone = self.clone();
        tokio::spawn(async move {
            // [ALLOWED-SLEEP] 差异全量接收稳态一次性延迟，非周期性
            tokio::time::sleep(Duration::from_millis(settle_ms)).await;
            info!(
                "[federation] 差异全量接收稳态 {}ms 到期（repo={}）",
                settle_ms, repo_type
            );
            self_clone.finish_diff_sync(repo_type);
        });
    }

    /// 尝试开始差量同步：每 repo 最多1个并发。
    fn try_start_diff_sync(&self, peer: NodeId, repo_type: u8) -> bool {
        let mut states = self.diff_sync_states.write();
        if states.contains_key(&repo_type) {
            return false;
        }
        let now = Instant::now();
        states.insert(repo_type, (peer, now, now));
        true
    }

    /// 更新差量同步进度（每收到一批 FullSyncBatch 时调用）。
    pub fn update_diff_progress(&self) {
        let mut states = self.diff_sync_states.write();
        let now = Instant::now();
        for (_, _, last_progress) in states.values_mut() {
            *last_progress = now;
        }
    }

    /// 完成差量同步：清除该 repo 标志、重建该 repo Merkle。所有 repo 完成后才恢复 Gossip。
    fn finish_diff_sync(&self, repo_type: u8) {
        let had_sync = self.diff_sync_states.write().remove(&repo_type).is_some();
        if !had_sync {
            return;
        }
        self.flush_merkle_queue(usize::MAX);
        match repo_type {
            rt if rt == repo_type::NODE => self.node_merkle.rebuild_all(),
            rt if rt == repo_type::PEER => {
                if let Some(ps) = &self.peer_sync {
                    ps.merkle().rebuild_all();
                }
            }
            rt if rt == repo_type::INFOHASH => {
                if let Some(ihs) = &self.infohash_sync {
                    ihs.merkle().rebuild_all();
                }
            }
            rt if rt == repo_type::TRACKER => {
                if let Some(ts) = &self.tracker_sync {
                    ts.merkle().rebuild_all();
                }
            }
            _ => {}
        }
        let remaining = self.diff_sync_states.read().len();
        if remaining == 0 {
            let gossip = self.gossip_engine.clone();
            tokio::spawn(async move {
                // [ALLOWED-SLEEP] 全量完成后一次性延迟恢复 Gossip，非周期性
                tokio::time::sleep(GOSSIP_RESUME_DELAY).await;
                gossip.set_receiving_full_sync(false);
                info!("[federation] 所有差异全量完成，恢复 Gossip 转发");
            });
        }
        info!(
            "[federation] 差异全量完成（repo={}），剩余 {} 个 repo 同步中",
            repo_type, remaining
        );
    }

    /// 差量同步超时监控（已迁移到 TaskScheduler）
    ///
    /// 周期性超时检查由 TaskScheduler 调用 check_diff_sync_timeouts() 驱动。
    /// 此方法保留为空以兼容现有调用点，不再内部 spawn。
    fn spawn_diff_sync_watcher(self: Arc<Self>, _repo_type: u8) {
        // 已迁移：差量同步超时监控由 TaskScheduler 调度 check_diff_sync_timeouts()
    }

    /// 检查所有差量同步的超时状态（由 TaskScheduler 定期调用）
    ///
    /// - 5 分钟无进度 -> 取消
    /// - 1 小时硬超时 -> 取消
    pub fn check_diff_sync_timeouts(&self) {
        let expired: Vec<u8> = {
            let states = self.diff_sync_states.read();
            let now = Instant::now();
            states
                .iter()
                .filter_map(|(repo_type, (peer, start, last_progress))| {
                    if now.duration_since(*last_progress).as_secs() >= 300 {
                        warn!(
                            "[federation] 差异全量5分钟无进度，取消（repo={}, 对端={}）",
                            repo_type, peer
                        );
                        Some(*repo_type)
                    } else if now.duration_since(*start).as_secs() >= 3600 {
                        warn!(
                            "[federation] 差异全量1小时硬超时，取消（repo={}, 对端={}）",
                            repo_type, peer
                        );
                        Some(*repo_type)
                    } else {
                        None
                    }
                })
                .collect()
        };
        for repo_type in expired {
            self.finish_diff_sync(repo_type);
        }
    }

    // ========================================================================
    // Push-Pull Gossip（每30秒交换最近变更，兜底纯 Push 丢失的消息）
    // ========================================================================

    /// 启动 Push-Pull Gossip 定时任务（已迁移到 TaskScheduler）
    ///
    /// 周期性 Push-Pull 由 TaskScheduler 调用 `push_pull_tick()` 驱动。
    /// 此方法保留为空以兼容现有调用点，不再内部 spawn。
    pub fn spawn_push_pull_gossip(self: Arc<Self>) {
        // 已迁移：Push-Pull Gossip 由 TaskScheduler 调度 push_pull_tick()
    }

    /// 单次 Push-Pull：随机选1个邻居，发送最近变更摘要（由 TaskScheduler 调度）
    pub async fn push_pull_tick(&self) {
        let conns = self.connection_manager.all_connections();
        if conns.is_empty() {
            return;
        }
        // 随机选1个邻居
        use std::time::{SystemTime, UNIX_EPOCH};
        let idx = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as usize)
            % conns.len();
        let conn = &conns[idx];

        let changes = self
            .recent_changes
            .read()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        if changes.is_empty() {
            return;
        }

        let digest = GossipDigestMessage { changes };
        if let Err(e) = conn.send_message(MessageType::GossipDigest, &digest).await {
            debug!("[federation] Push-Pull GossipDigest 发送失败: {}", e);
        } else {
            self.metrics.record_message_sent();
            debug!(
                "[federation] Push-Pull: 发送 {} 条变更摘要 to={}",
                digest.changes.len(),
                conn.node_id
            );
        }
    }

    /// 处理收到的 GossipDigest：对比本地，找出对端有而本地没有的 key，发送 PullRequest
    pub async fn handle_gossip_digest(&self, conn: &Connection, digest: GossipDigestMessage) {
        // 收集本地缺失的 key（对端有而本地没有）。
        // 【回环修复】不能用 merkle.contains_key 判断本地是否持有：当前 Merkle 是 DB 驱动的，
        // contains_key() 恒返回 false（merkle.rs 无 per-key 内存索引），会把本地已持有的 key
        // 全部误判为「缺失」-> 对每个 key 都发 PullRequest -> 对端回传 -> 又登记 -> 再广告，
        // 形成 A<->B 无限拉取回环。NODE repo 直接查存储层按 addr 判断存在性。
        let mut missing: Vec<(u8, Vec<u8>)> = Vec::new();
        for (repo_type, key, _version) in &digest.changes {
            let already_here: bool = match *repo_type {
                repo_type::NODE => std::str::from_utf8(key)
                    .ok()
                    .and_then(|s| s.parse::<SocketAddr>().ok())
                    .map(|addr| self.node_repo.contains_sync(addr))
                    .unwrap_or(false),
                _ => self
                    .merkle_for_repo(*repo_type)
                    .map(|m| m.contains_key(key))
                    .unwrap_or(false),
            };
            if !already_here {
                missing.push((*repo_type, key.clone()));
            }
        }

        if missing.is_empty() {
            return;
        }

        debug!(
            "[federation] Push-Pull: 发现 {} 个缺失 key，请求拉取 from={}",
            missing.len(),
            conn.node_id
        );

        let req = GossipPullRequestMessage { keys: missing };
        if let Err(e) = conn
            .send_message(MessageType::GossipPullRequest, &req)
            .await
        {
            debug!("[federation] GossipPullRequest 发送失败: {}", e);
        } else {
            self.metrics.record_message_sent();
        }
    }

    /// 处理收到的 GossipPullRequest：返回指定 key 的完整数据
    pub async fn handle_gossip_pull_request(
        &self,
        conn: &Connection,
        req: GossipPullRequestMessage,
    ) {
        let mut entries: Vec<(u8, SyncEntry)> = Vec::new();
        for (repo_type, key) in &req.keys {
            if let Some(entry) = self.load_entry_by_key(*repo_type, key) {
                entries.push((*repo_type, entry));
            }
        }

        if entries.is_empty() {
            return;
        }

        let resp = GossipPullResponseMessage { entries };
        if let Err(e) = conn
            .send_message(MessageType::GossipPullResponse, &resp)
            .await
        {
            debug!("[federation] GossipPullResponse 发送失败: {}", e);
        } else {
            self.metrics.record_message_sent();
        }
    }

    /// 处理收到的 GossipPullResponse：应用到本地
    pub fn handle_gossip_pull_response(&self, resp: GossipPullResponseMessage) {
        let count = resp.entries.len();
        for (repo_type, entry) in resp.entries {
            self.handle_sync_batch(repo_type, std::slice::from_ref(&entry));
        }
        debug!("[federation] Push-Pull: 应用 {} 条拉取数据", count);
    }

    /// 处理收到的 DiffSyncRequest（本节点作为数据源 / 服务端）。
    ///
    /// 请求方携带本地各 repo 的 Merkle 摘要，本节点对比后找出差异分片，
    /// 只推送差异分片的条目（不是全量）。
    pub async fn handle_diff_sync_request(
        self: Arc<Self>,
        from_node_id: NodeId,
        req: DiffSyncRequestMessage,
    ) {
        let conn = match self.connection_manager.get_connection(&from_node_id) {
            Some(c) => c,
            None => {
                warn!("[federation] DiffSync: 目标 {} 无连接，取消", from_node_id);
                return;
            }
        };

        self.gossip_engine.set_sending_full_sync(true);
        let batch_size = self.config.full_sync_batch_size;
        let window = self.config.full_sync_window_size;
        let mut _total_pushed = 0usize;

        // 只处理请求中的单个 repo（按repo独立触发）
        let digest = &req.digest;
        let merkle = match self.merkle_for_repo(digest.repo_type) {
            Some(m) => m,
            None => {
                self.gossip_engine.set_sending_full_sync(false);
                return;
            }
        };

        let diff_shards = merkle.diff(digest);
        if diff_shards.is_empty() {
            self.gossip_engine.set_sending_full_sync(false);
            return;
        }

        // DB 驱动推送：从数据库按差异分片加载完整 SyncEntry
        let entries = self.load_entries_by_shards(digest.repo_type, &diff_shards);

        if entries.is_empty() {
            self.gossip_engine.set_sending_full_sync(false);
            return;
        }

        // P0-2: 若对端支持 key 列表交换，先交换 key 列表，只推送对方缺失的条目
        // （重复率 ~90% -> <5%）。对端为旧版本或交换失败时回退到原始全量推送。
        let final_entries: Vec<SyncEntry> = if conn.supports_diff_keys() {
            match self
                .clone()
                .run_diff_key_exchange(
                    &conn,
                    from_node_id,
                    digest.repo_type,
                    &diff_shards,
                    &entries,
                )
                .await
            {
                Ok(filtered) => {
                    info!(
                        "[federation] DiffSync key 交换完成: repo={}, 本地候选={}, 仅推送对方缺失={}",
                        digest.repo_type,
                        entries.len(),
                        filtered.len()
                    );
                    filtered
                }
                Err(e) => {
                    warn!(
                        "[federation] DiffSync key 交换失败，回退全量推送: repo={}, err={}",
                        digest.repo_type, e
                    );
                    entries
                }
            }
        } else {
            entries
        };

        let total = final_entries.len();
        info!(
            "[federation] DiffSync 推送: repo_type={}, 差异分片={}, 条目={}, to={}",
            digest.repo_type,
            diff_shards.len(),
            total,
            from_node_id
        );
        _total_pushed = total;

        let start_msg = FullSyncStartMessage {
            repo_type: digest.repo_type,
            total_entries: total as u64,
        };
        if let Err(e) = conn
            .send_message(MessageType::FullSyncStart, &start_msg)
            .await
        {
            warn!("[federation] FullSyncStart 发送失败: {}", e);
            self.gossip_engine.set_sending_full_sync(false);
            return;
        }

        let mut seq: u64 = 0;
        for chunk in final_entries.chunks(batch_size) {
            let batch_msg = FullSyncBatchMessage {
                repo_type: digest.repo_type,
                entries: chunk.to_vec(),
                seq,
            };
            if let Err(e) = conn
                .send_message(MessageType::FullSyncBatch, &batch_msg)
                .await
            {
                warn!("[federation] FullSyncBatch seq={} 发送失败: {}", seq, e);
                break;
            }
            seq += 1;
            if seq.is_multiple_of(window as u64) {
                // [ALLOWED-SLEEP] 全量同步发送流控，函数内限流等待，非周期性
                tokio::time::sleep(FULL_SYNC_FLOW_CONTROL_INTERVAL).await;
            }
        }

        let complete_msg = FullSyncCompleteMessage {
            repo_type: digest.repo_type,
        };
        let _ = conn
            .send_message(MessageType::FullSyncComplete, &complete_msg)
            .await;

        self.gossip_engine.set_sending_full_sync(false);
        info!(
            "[federation] DiffSync 完成: to={}, 总推送条目={}",
            from_node_id, _total_pushed
        );
    }

    /// P0-2: DiffSync key 列表交换（数据服务器侧）。
    ///
    /// 把本地差异分片的 key 列表分片发给请求方，等待请求方回传其缺失的 key 列表，
    /// 然后只从 `entries` 中筛出对方缺失的条目返回。
    /// 超时或发送失败时返回 Err，调用方回退到全量推送。
    async fn run_diff_key_exchange(
        self: Arc<Self>,
        conn: &Connection,
        peer: NodeId,
        repo_type: u8,
        diff_shards: &[u16],
        entries: &[SyncEntry],
    ) -> anyhow::Result<Vec<SyncEntry>> {
        // 1. 本地差异分片的全部 key
        let all_keys: Vec<Vec<u8>> = entries.iter().map(|e| e.key.clone()).collect();

        // 2. 注册 oneshot，等待请求方回传缺失 key
        let (tx, rx) = oneshot::channel::<Vec<Vec<u8>>>();
        let key = (peer, repo_type);
        self.pending_diff_key_resp.write().insert(
            key,
            PendingKeyResp {
                tx: Some(tx),
                buf: Vec::new(),
            },
        );

        // 确保无论成功/失败/超时都清理待收集项，避免泄漏
        struct Guard<'a> {
            mgr: &'a SyncManager,
            key: (NodeId, u8),
        }
        impl<'a> Drop for Guard<'a> {
            fn drop(&mut self) {
                self.mgr.pending_diff_key_resp.write().remove(&self.key);
            }
        }
        let _guard = Guard {
            mgr: self.as_ref(),
            key,
        };

        // 3. 分片发送 key 列表（每片 <= DIFF_SYNC_KEY_CHUNK_SIZE 个 key）
        let chunk_size = DIFF_SYNC_KEY_CHUNK_SIZE;
        let total_chunks = all_keys.len().div_ceil(chunk_size).max(1);
        for (i, chunk) in all_keys.chunks(chunk_size).enumerate() {
            let is_last = i + 1 == total_chunks;
            let msg = DiffSyncKeyRequestMessage {
                repo_type,
                shards: diff_shards.to_vec(),
                keys: chunk.to_vec(),
                is_last,
            };
            if let Err(e) = conn
                .send_message(MessageType::DiffSyncKeyRequest, &msg)
                .await
            {
                anyhow::bail!("发送 DiffSyncKeyRequest 失败: {}", e);
            }
        }
        self.metrics.record_message_sent();
        debug!(
            "[federation] DiffSyncKeyRequest 已发送: repo={}, keys={}, chunks={}, to={}",
            repo_type,
            all_keys.len(),
            total_chunks,
            peer
        );

        // 4. 等待请求方回传缺失 key（带超时，超时回退全量）
        let timeout = Duration::from_secs(self.config.diff_key_exchange_timeout_secs);
        let missing = match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(m)) => m,
            Ok(Err(_)) => {
                anyhow::bail!("对端未回传 DiffSyncKeyResponse（通道关闭）");
            }
            Err(_) => {
                anyhow::bail!("等待 DiffSyncKeyResponse 超时 {}s", timeout.as_secs());
            }
        };

        // 5. 只保留对方缺失的条目
        let missing_set: FxHashSet<&[u8]> = missing.iter().map(|k| k.as_slice()).collect();
        let filtered: Vec<SyncEntry> = entries
            .iter()
            .filter(|e| missing_set.contains(e.key.as_slice()))
            .cloned()
            .collect();
        Ok(filtered)
    }

    /// P0-2: 处理收到的 DiffSyncKeyRequest（请求方侧）。
    ///
    /// 累计数据服务器分片发来的 key 列表，收齐到 is_last 后，从本地 DB 加载同分片 key，
    /// 对比出对方有而本地缺失的 key，分片回传 DiffSyncKeyResponse。
    pub async fn handle_diff_sync_key_request(
        self: Arc<Self>,
        conn: &Connection,
        msg: DiffSyncKeyRequestMessage,
    ) {
        let repo_type = msg.repo_type;
        let peer = conn.node_id;
        let key = (peer, repo_type);

        // 累计本片 key
        {
            let mut buf_map = self.diff_key_req_buffer.write();
            let buf = buf_map.entry(key).or_default();
            if !msg.shards.is_empty() {
                buf.shards = msg.shards;
            }
            buf.keys.extend(msg.keys);
        }

        if !msg.is_last {
            // 还有更多分片，等待下一片
            return;
        }

        // 收齐：取出累计缓冲
        let (shards, server_keys) = {
            let mut buf_map = self.diff_key_req_buffer.write();
            buf_map
                .remove(&key)
                .map(|b| (b.shards, b.keys))
                .unwrap_or_default()
        };

        // 从本地 DB 加载同分片 key，构建本地 key 集合
        let local_entries = self.load_entries_by_shards(repo_type, &shards);
        let local_set: FxHashSet<&[u8]> = local_entries.iter().map(|e| e.key.as_slice()).collect();

        // 对方有而本地缺失的 key
        let missing: Vec<Vec<u8>> = server_keys
            .iter()
            .filter(|k| !local_set.contains(k.as_slice()))
            .cloned()
            .collect();

        debug!(
            "[federation] DiffSyncKeyRequest 对比: repo={}, 对方keys={}, 本地keys={}, 缺失={}, from={}",
            repo_type,
            server_keys.len(),
            local_set.len(),
            missing.len(),
            peer
        );

        // 分片回传缺失 key
        let chunk_size = DIFF_SYNC_KEY_CHUNK_SIZE;
        let total_chunks = missing.len().div_ceil(chunk_size).max(1);
        for (i, chunk) in missing.chunks(chunk_size).enumerate() {
            let is_last = i + 1 == total_chunks;
            let resp = DiffSyncKeyResponseMessage {
                repo_type,
                missing_keys: chunk.to_vec(),
                is_last,
            };
            if let Err(e) = conn
                .send_message(MessageType::DiffSyncKeyResponse, &resp)
                .await
            {
                debug!("[federation] DiffSyncKeyResponse 发送失败: {}", e);
                return;
            }
        }
        self.metrics.record_message_sent();
    }

    /// P0-2: 处理收到的 DiffSyncKeyResponse（数据服务器侧）。
    ///
    /// 累计请求方回传的缺失 key 分片，收齐到 is_last 后唤醒等待中的推送任务。
    pub fn handle_diff_sync_key_response(
        &self,
        conn: &Connection,
        msg: DiffSyncKeyResponseMessage,
    ) {
        let repo_type = msg.repo_type;
        let key = (conn.node_id, repo_type);

        let pending = {
            let mut map = self.pending_diff_key_resp.write();
            if let Some(p) = map.get_mut(&key) {
                p.buf.extend(msg.missing_keys);
                if msg.is_last {
                    map.remove(&key)
                } else {
                    None
                }
            } else {
                None
            }
        };

        // is_last 时 pending 已被移除，取出 tx 唤醒等待方
        if let Some(p) = pending {
            if let Some(tx) = p.tx {
                let _ = tx.send(p.buf);
            }
        }
    }

    /// 统计本节点各 repo 的当前条目数（顺序与 repo_type::NODE/PEER/INFOHASH/TRACKER 一致）。
    ///
    /// 直接从各 Repo 实现获取真实数据量，而非从 Merkle 树获取（Merkle 树启动时为空，
    /// 只有通过 Gossip 收到的少量条目会被计入，导致 super_node 报告的条目数远低于实际）。
    pub(crate) fn local_entry_counts(&self) -> Vec<u32> {
        vec![
            self.node_repo.len_sync() as u32,
            self.peer_repo.as_ref().map(|r| r.len() as u32).unwrap_or(0),
            self.infohash_repo
                .as_ref()
                .map(|r| r.count_sync() as u32)
                .unwrap_or(0),
            self.tracker_repo
                .as_ref()
                .map(|r| r.count_sync() as u32)
                .unwrap_or(0),
        ]
    }

    /// 全量同步开始（接收端：由 FullSyncStart 消息触发）
    ///
    /// P0-1 标志拆分：对指定 repo_type 的 MerkleTree 设置 receiving_full_sync=true
    /// （跳过逐条 recompute_shard），同时告知 GossipEngine 进入接收态——收到的
    /// Gossip 只本地写入、不再转发，防止多源重复洪峰。
    pub fn handle_full_sync_start(&self, repo_type: u8) {
        self.gossip_engine.set_receiving_full_sync(true);
        if let Some(merkle) = self.merkle_for_repo(repo_type) {
            merkle.set_receiving_full_sync(true);
            info!(
                "[federation] FullSync 开始: repo_type={}, Merkle 进入接收惰性模式",
                repo_type
            );
        }
    }

    /// 全量同步完成（接收端：由 FullSyncComplete 消息触发）
    ///
    /// 恢复 Merkle 正常模式并一次性 rebuild_all；关闭 Gossip 接收态，恢复正常转发。
    pub fn handle_full_sync_complete(&self, repo_type: u8) {
        self.gossip_engine.set_receiving_full_sync(false);
        if let Some(merkle) = self.merkle_for_repo(repo_type) {
            merkle.set_receiving_full_sync(false);
            merkle.rebuild_all();
            info!(
                "[federation] FullSync 完成: repo_type={}, Merkle 已重建",
                repo_type
            );
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
                warn!(
                    "[federation] FullSync: 目标 {} 无连接，取消",
                    target_node_id
                );
                return;
            }
        };

        // 发送端全量期间告知 GossipEngine：放开 outbox 丢弃/限流（不影响接收端转发）
        self.gossip_engine.set_sending_full_sync(true);

        let batch_size = self.config.full_sync_batch_size;
        let window = self.config.full_sync_window_size;

        // 对每个启用的 repo_type 执行全量同步
        for repo_type in &[
            repo_type::NODE,
            repo_type::PEER,
            repo_type::INFOHASH,
            repo_type::TRACKER,
        ] {
            // 收集全量条目
            let entries: Vec<SyncEntry> = match *repo_type {
                repo_type::NODE => self.collect_node_entries(),
                repo_type::PEER => match &self.peer_sync {
                    Some(ps) => ps.collect_all_entries(),
                    None => continue,
                },
                repo_type::INFOHASH => match &self.infohash_sync {
                    Some(ihs) => ihs.collect_all_entries(),
                    None => continue,
                },
                repo_type::TRACKER => match &self.tracker_sync {
                    Some(ts) => ts.collect_all_entries(),
                    None => continue,
                },
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
            if let Err(e) = conn
                .send_message(MessageType::FullSyncStart, &start_msg)
                .await
            {
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
                if let Err(e) = conn
                    .send_message(MessageType::FullSyncBatch, &batch_msg)
                    .await
                {
                    warn!("[federation] FullSyncBatch seq={} 发送失败: {}", seq, e);
                    break;
                }
                seq += 1;

                // 简单流控：每 window 个批次等待一次（此处简化为每批等待 Ack）
                // 接收端在 connection.rs 中收到 FullSyncBatch 后自动回复 Ack
                // 这里不主动等待 Ack，由 TCP 层流控和窗口大小间接控制速率
                if seq.is_multiple_of(window as u64) {
                    // [ALLOWED-SLEEP] 全量同步发送流控，函数内限流等待，非周期性
                    tokio::time::sleep(FULL_SYNC_FLOW_CONTROL_INTERVAL).await;
                }
            }

            // 3. 发送 FullSyncComplete
            let complete_msg = FullSyncCompleteMessage {
                repo_type: *repo_type,
            };
            if let Err(e) = conn
                .send_message(MessageType::FullSyncComplete, &complete_msg)
                .await
            {
                warn!("[federation] FullSyncComplete 发送失败: {}", e);
            }

            info!(
                "[federation] FullSync 完成: repo_type={}, batches={}, to={}",
                repo_type, seq, target_node_id
            );
        }

        // 发送端全量结束，恢复 GossipEngine 正常丢弃/限流
        self.gossip_engine.set_sending_full_sync(false);
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

    /// 从数据库按分片加载完整 SyncEntry 数据（DB 驱动推送路径）。
    ///
    /// Merkle 根和推送数据都基于数据库，不再从内存 Merkle entries 取热数据。
    /// 此方法调用 DB 的 load_*_rows_by_shards（走 shard 索引），返回完整条目。
    pub fn load_entries_by_shards(&self, repo_type: u8, shards: &[u16]) -> Vec<SyncEntry> {
        match repo_type {
            rt if rt == repo_type::NODE => {
                let storage = self.node_repo.storage();
                let rows = match storage.load_node_rows_by_shards(shards) {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("[federation] load_node_rows_by_shards 失败: {}", e);
                        return Vec::new();
                    }
                };
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                rows.into_iter()
                    .filter_map(|(id, ip, port)| {
                        let mut node_id = [0u8; 20];
                        if id.len() == 20 {
                            node_id.copy_from_slice(&id);
                        }
                        let addr: SocketAddr = format!("{}:{}", ip, port).parse().ok()?;
                        let (_key, payload, _hash) = build_node_sync_entry(node_id, addr)?;
                        Some(SyncEntry {
                            key: _key,
                            operation: operation::UPSERT,
                            version: now,
                            payload,
                        })
                    })
                    .collect()
            }
            rt if rt == repo_type::PEER => {
                let storage = match &self.peer_repo {
                    Some(r) => r.storage(),
                    None => return Vec::new(),
                };
                let rows = match storage.load_peer_rows_by_shards(shards) {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("[federation] load_peer_rows_by_shards 失败: {}", e);
                        return Vec::new();
                    }
                };
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                rows.into_iter()
                    .filter_map(|(infohash, ip, port, _source, _last_active)| {
                        let mut ih = [0u8; 20];
                        if infohash.len() == 20 {
                            ih.copy_from_slice(&infohash);
                        }
                        let addr: SocketAddr = format!("{}:{}", ip, port).parse().ok()?;
                        let (_key, payload, _hash) = peer_sync::build_peer_sync_entry(ih, addr)?;
                        Some(SyncEntry {
                            key: _key,
                            operation: operation::UPSERT,
                            version: now,
                            payload,
                        })
                    })
                    .collect()
            }
            rt if rt == repo_type::INFOHASH => {
                let storage = match &self.infohash_repo {
                    Some(r) => r.storage(),
                    None => return Vec::new(),
                };
                let rows = match storage.load_infohash_rows_by_shards(shards) {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("[federation] load_infohash_rows_by_shards 失败: {}", e);
                        return Vec::new();
                    }
                };
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                rows.into_iter()
                    .filter_map(|(infohash, _last_seen, _first_source)| {
                        let mut ih = [0u8; 20];
                        if infohash.len() == 20 {
                            ih.copy_from_slice(&infohash);
                        }
                        let (_key, payload, _hash) = infohash_sync::build_infohash_sync_entry(ih)?;
                        Some(SyncEntry {
                            key: _key,
                            operation: operation::UPSERT,
                            version: now,
                            payload,
                        })
                    })
                    .collect()
            }
            rt if rt == repo_type::TRACKER => {
                let storage = match &self.tracker_repo {
                    Some(r) => r.storage(),
                    None => return Vec::new(),
                };
                let rows = match storage.load_tracker_rows_by_shards(shards) {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("[federation] load_tracker_rows_by_shards 失败: {}", e);
                        return Vec::new();
                    }
                };
                rows.into_iter()
                    .filter_map(|(url, _disabled, last_used)| {
                        let ts = last_used.unwrap_or(0) as u64;
                        let (_key, payload, _hash) = tracker_sync::build_tracker_sync_entry(&url)?;
                        Some(SyncEntry {
                            key: _key,
                            operation: operation::UPSERT,
                            version: ts,
                            payload,
                        })
                    })
                    .collect()
            }
            _ => Vec::new(),
        }
    }

    /// 从数据库按 key 加载单个 SyncEntry（用于 Gossip Pull）。
    pub fn load_entry_by_key(&self, repo_type: u8, key: &[u8]) -> Option<SyncEntry> {
        let shard = self.merkle_for_repo(repo_type)?.shard_for_key(key);
        let entries = self.load_entries_by_shards(repo_type, &[shard]);
        entries.into_iter().find(|e| e.key == key)
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
    /// 小差异（<20%分片）：逐分片发送 MerkleRequest 修复。
    /// 大差异（≥20%分片）：触发差异全量同步，一次性拉取所有差异分片数据。
    pub async fn handle_merkle_digest(
        self: Arc<Self>,
        conn: &Connection,
        digest: MerkleDigestMessage,
    ) {
        let merkle = match self.merkle_for_repo(digest.repo_type) {
            Some(m) => m,
            None => return,
        };

        // 记录对端该 repo 的条目总数
        let idx = (digest.repo_type as usize).wrapping_sub(1);
        if idx < 4 {
            let total: u32 = digest.entry_counts.iter().sum();
            let mut digests = self.peer_digests.write();
            let entry = digests.entry(conn.node_id).or_insert_with(|| vec![0u32; 4]);
            entry[idx] = total;
        }

        let diffs = merkle.diff(&digest);
        if diffs.is_empty() {
            debug!(
                "[federation] Merkle 对账无差异: repo_type={}, from={}",
                digest.repo_type, conn.node_id
            );
            return;
        }

        let diff_ratio = diffs.len() as f64 / 256.0;
        info!(
            "[federation] Merkle 对账发现 {} 个差异分片（{:.1}%）: repo_type={}, from={}",
            diffs.len(),
            diff_ratio * 100.0,
            digest.repo_type,
            conn.node_id
        );

        // 大差异（≥20%分片）：优先走分层 Merkle + 分片并行同步（version>=3）
        if diffs.len() * 5 >= 256 {
            if conn.supports_layered_merkle() && self.config.layered_merkle_enabled {
                info!(
                    "[federation] 差异≥20%，触发分层Merkle+分片同步（repo_type={}, from={}）",
                    digest.repo_type, conn.node_id
                );
                self.trigger_layered_sync(conn.node_id, digest.repo_type)
                    .await;
            } else {
                info!(
                    "[federation] 差异≥20%，对端不支持分层Merkle，触发旧版DiffSync（repo_type={}, from={}）",
                    digest.repo_type, conn.node_id
                );
                self.trigger_diff_sync(conn.node_id, digest.repo_type).await;
            }
            return;
        }

        // 小差异：逐分片 MerkleRequest 修复
        let request = MerkleRequestMessage {
            repo_type: digest.repo_type,
            shards: diffs,
        };

        if let Err(e) = conn
            .send_message(MessageType::MerkleRequest, &request)
            .await
        {
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
        let _merkle = match self.merkle_for_repo(request.repo_type) {
            Some(m) => m,
            None => return,
        };

        // DB 驱动推送：从数据库按请求分片加载完整 SyncEntry
        let entries = self.load_entries_by_shards(request.repo_type, &request.shards);

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

    /// 联邦实时查询：从本地 PeerRepo 获取指定 infohash 的 peer
    pub fn query_peers_for_infohash(
        &self,
        infohash: &[u8; 20],
        limit: usize,
    ) -> Vec<crate::federation::protocol::PeerQueryEntry> {
        self.peer_repo
            .as_ref()
            .map(|repo| {
                let ih: crate::types::Infohash = *infohash;
                repo.get_peers_sync(&ih, limit)
                    .into_iter()
                    .map(|p| crate::federation::protocol::PeerQueryEntry {
                        ip: p.addr.ip().to_string(),
                        port: p.addr.port(),
                        source: p.source.as_str().to_string(),
                        score: p.priority_score,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// 联邦实时查询：将远程节点返回的 peer 写入本地 PeerRepo
    pub fn add_remote_peers(
        &self,
        infohash: &[u8; 20],
        peers: &[crate::federation::protocol::PeerQueryEntry],
    ) {
        let repo = match &self.peer_repo {
            Some(r) => r,
            None => return,
        };
        let ih: crate::types::Infohash = *infohash;
        let peer_infos: Vec<crate::types::PeerInfo> = peers
            .iter()
            .filter_map(|p| {
                let ip: std::net::IpAddr = p.ip.parse().ok()?;
                let addr = std::net::SocketAddr::new(ip, p.port);
                let source = match p.source.as_str() {
                    "tracker" => crate::types::PeerSource::Tracker,
                    "dht" => crate::types::PeerSource::Dht,
                    "pex" => crate::types::PeerSource::Pex,
                    "super_tracker" => crate::types::PeerSource::SuperTracker,
                    "lpd" => crate::types::PeerSource::Lpd,
                    "webseed" => crate::types::PeerSource::WebSeed,
                    "utp" => crate::types::PeerSource::Utp,
                    _ => crate::types::PeerSource::Manual,
                };
                let mut peer = crate::types::PeerInfo::new(addr, source);
                peer.priority_score = p.score;
                Some(peer)
            })
            .collect();
        repo.add_peers_sync(&ih, &peer_infos);
    }

    // ========================================================================
    // 分层 Merkle 对比 + 分片并行同步（协议版本 >=3，亿级数据架构升级）
    // ========================================================================

    /// 处理收到的 MerkleLevelRequest：返回指定层级的子哈希列表
    ///
    /// 由对端请求某层某分片的子哈希，用于逐层定位差异 L2。
    /// - level=0: 返回 L0 根哈希（1个）
    /// - level=1: 返回所有 L1 一级分片哈希（256个）
    /// - level=2: 返回指定 L1 下的 L2 二级分片哈希（256个）
    pub async fn handle_merkle_level_request(
        self: Arc<Self>,
        conn: Arc<Connection>,
        req: MerkleLevelRequestMessage,
    ) {
        let merkle = match self.merkle_for_repo(req.repo_type) {
            Some(m) => m,
            None => return,
        };

        let (hashes, entry_counts) = match req.level {
            0 => {
                // L0 根
                let root = merkle.root_hash();
                (vec![root], vec![merkle.total_entries() as u32])
            }
            1 => {
                // L1 一级分片
                let l1_hashes = merkle.level1_hashes();
                let counts: Vec<u32> = (0..merkle.shard_count())
                    .map(|i| merkle.l1_entry_count(i))
                    .collect();
                (l1_hashes, counts)
            }
            2 => {
                // L2 二级分片（指定 L1 下）
                let l2_hashes = merkle.level2_hashes(req.parent_shard);
                let l2_start =
                    req.parent_shard as u32 * crate::federation::merkle::L2_PER_L1 as u32;
                let counts: Vec<u32> = (0..l2_hashes.len() as u32)
                    .map(|i| merkle.l2_entry_count(l2_start + i))
                    .collect();
                (l2_hashes, counts)
            }
            _ => {
                debug!("[shard-sync] 未知 MerkleLevelRequest level={}", req.level);
                return;
            }
        };

        let resp = MerkleLevelResponseMessage {
            repo_type: req.repo_type,
            level: req.level,
            parent_shard: req.parent_shard,
            hashes,
            entry_counts,
        };

        if let Err(e) = conn
            .send_message(MessageType::MerkleLevelResponse, &resp)
            .await
        {
            debug!("[shard-sync] 发送 MerkleLevelResponse 失败: {}", e);
        } else {
            self.metrics.record_message_sent();
            debug!(
                "[shard-sync] MerkleLevelResponse: repo={}, level={}, parent={}, hashes={}",
                req.repo_type,
                req.level,
                req.parent_shard,
                resp.hashes.len()
            );
        }
    }

    /// 处理收到的 MerkleLevelResponse：继续分层对比流程
    ///
    /// 收到对端返回的层级哈希后，与本地对比：
    /// - level=1: 对比 L1，找出差异 L1，然后请求差异 L1 的 L2
    /// - level=2: 对比 L2，收集差异 L2，全部收齐后启动分片同步
    pub async fn handle_merkle_level_response(
        self: Arc<Self>,
        conn: Arc<Connection>,
        resp: MerkleLevelResponseMessage,
    ) {
        let merkle = match self.merkle_for_repo(resp.repo_type) {
            Some(m) => m,
            None => return,
        };

        let key = (conn.node_id, resp.repo_type);

        match resp.level {
            1 => {
                // 对比 L1，找出差异 L1 分片
                let diff_l1 = merkle.diff_level1(&resp.hashes);
                debug!(
                    "[shard-sync] L1 对比完成: repo={}, 差异L1数={}",
                    resp.repo_type,
                    diff_l1.len()
                );

                if diff_l1.is_empty() {
                    self.layered_compare_sessions.write().remove(&key);
                    return;
                }

                // 为每个差异 L1 请求 L2 哈希
                let pending_count = diff_l1.len();
                self.layered_compare_sessions.write().insert(
                    key,
                    LayeredCompareSession {
                        peer: conn.node_id,
                        repo_type: resp.repo_type,
                        started_at: std::time::Instant::now(),
                        phase: crate::federation::sync::shard_sync_engine::LayeredComparePhase::AwaitingL2 {
                            compared_l1: Vec::new(),
                            diff_l2: FxHashSet::default(),
                            pending_l1_count: pending_count,
                        },
                    },
                );

                for &l1 in &diff_l1 {
                    let req = MerkleLevelRequestMessage {
                        repo_type: resp.repo_type,
                        level: 2,
                        parent_shard: l1,
                    };
                    if let Err(e) = conn
                        .send_message(MessageType::MerkleLevelRequest, &req)
                        .await
                    {
                        warn!("[shard-sync] 发送 L2 请求失败: {}", e);
                        break;
                    }
                }
                self.metrics.record_message_sent();
            }
            2 => {
                // 对比指定 L1 下的 L2，收集差异 L2
                let diff_l2_offsets = merkle.diff_level2(resp.parent_shard, &resp.hashes);
                let l1_base =
                    resp.parent_shard as u32 * crate::federation::merkle::L2_PER_L1 as u32;
                let absolute_diff_l2: Vec<u32> = diff_l2_offsets
                    .iter()
                    .map(|&off| l1_base + off as u32)
                    .collect();

                debug!(
                    "[shard-sync] L2 对比: repo={}, L1={}, 差异L2数={}",
                    resp.repo_type,
                    resp.parent_shard,
                    absolute_diff_l2.len()
                );

                // 更新会话状态
                let mut sessions = self.layered_compare_sessions.write();
                if let Some(session) = sessions.get_mut(&key) {
                    if let crate::federation::sync::shard_sync_engine::LayeredComparePhase::AwaitingL2 {
                        compared_l1,
                        diff_l2,
                        pending_l1_count,
                    } = &mut session.phase
                    {
                        compared_l1.push(resp.parent_shard);
                        for &l2 in &absolute_diff_l2 {
                            diff_l2.insert(l2);
                        }
                        *pending_l1_count -= 1;

                        // 所有 L1 都收齐了，启动分片同步
                        if *pending_l1_count == 0 {
                            let diff_l2_list: Vec<u32> =
                                std::mem::take(diff_l2).into_iter().collect();
                            drop(sessions);

                            info!(
                                "[shard-sync] 分层对比完成: repo={}, peer={}, 差异L2总数={}",
                                resp.repo_type,
                                conn.node_id,
                                diff_l2_list.len()
                            );

                            // 启动分片同步引擎
                            self.start_shard_sync(conn.clone(), resp.repo_type, diff_l2_list);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// 处理收到的 ShardSyncBatch：应用条目并回复 Ack
    ///
    /// 接收方将批次中的条目应用到本地 repo，然后回复 ShardSyncAck。
    pub fn handle_shard_sync_batch(
        self: Arc<Self>,
        conn: Arc<Connection>,
        msg: ShardSyncBatchMessage,
    ) {
        debug!(
            "[shard-sync] 收到 ShardSyncBatch: repo={}, seq={}, l2_shards={:?}, entries={}, is_last={}",
            msg.repo_type,
            msg.seq,
            msg.l2_shards,
            msg.entries.len(),
            msg.is_last
        );

        // 应用条目到本地
        self.handle_sync_batch(msg.repo_type, &msg.entries);

        // 回复 Ack
        let ack = ShardSyncAckMessage {
            repo_type: msg.repo_type,
            seq: msg.seq,
            applied_count: msg.entries.len() as u32,
        };

        let conn_clone = conn.clone();
        let self_clone = self.clone();
        tokio::spawn(async move {
            if let Err(e) = conn_clone
                .send_message(MessageType::ShardSyncAck, &ack)
                .await
            {
                debug!("[shard-sync] 发送 ShardSyncAck 失败: {}", e);
            } else {
                self_clone.metrics.record_message_sent();
            }
        });
    }

    /// 处理收到的 ShardSyncAck：唤醒发送端等待的 oneshot
    ///
    /// 在活跃的分片同步引擎中查找匹配 seq 的等待者并唤醒。
    pub fn handle_shard_sync_ack(&self, ack: ShardSyncAckMessage) {
        let engines = self.active_shard_engines.read();

        // 遍历该 repo 的所有活跃引擎，找到匹配的
        for ((peer, rt), engine) in engines.iter() {
            if *rt == ack.repo_type {
                engine.handle_ack(&ack);
                debug!(
                    "[shard-sync] Ack 路由到引擎: peer={}, repo={}, seq={}, applied={}",
                    peer, ack.repo_type, ack.seq, ack.applied_count
                );
            }
        }
    }

    /// 处理收到的 ShardSyncHashList：对比本地 DB，回复缺失的 key（异步）
    ///
    /// 接收方把对端发来的 (key, data_hash) 列表分片累计，收齐后对比本地该 L1 分片的 key，
    /// 回传本地缺失的 key 列表，数据服务器只推送这些 key 的完整数据。
    pub async fn handle_shard_sync_hash_list(
        self: Arc<Self>,
        conn: Arc<Connection>,
        msg: ShardSyncHashListMessage,
    ) {
        let key = (conn.node_id, msg.repo_type, msg.l2_shard);
        // 累计分片
        {
            let mut buf = self.shard_hash_list_buffer.write();
            let entry = buf.entry(key).or_default();
            entry.extend(msg.entries);
        }
        if !msg.is_last {
            return; // 继续等待后续分片
        }
        // 收齐，取出累计列表
        let entries = self
            .shard_hash_list_buffer
            .write()
            .remove(&key)
            .unwrap_or_default();

        // NODE repo：查本地该 L1 分片的 key 集合（走 shard 索引），计算缺失
        let missing: Vec<Vec<u8>> = if msg.repo_type == repo_type::NODE {
            let l1 = MerkleTree::l1_for_l2(msg.l2_shard);
            let storage = self.node_repo.storage();
            match storage.load_node_rows_by_shards(&[l1]) {
                Ok(rows) => {
                    let local_keys: FxHashSet<Vec<u8>> = rows
                        .into_iter()
                        .map(|(_id, ip, port)| format!("{}:{}", ip, port).into_bytes())
                        .collect();
                    entries
                        .iter()
                        .filter(|(k, _h)| !local_keys.contains(k))
                        .map(|(k, _h)| k.clone())
                        .collect()
                }
                Err(e) => {
                    warn!(
                        "[shard-sync] 加载本地节点 key 失败: {}, 回传全部 key 作为缺失",
                        e
                    );
                    entries.iter().map(|(k, _)| k.clone()).collect()
                }
            }
        } else {
            // 其他 repo 暂不支持 hash 对比，回传全部 key 作为缺失（对端全量发送）
            entries.iter().map(|(k, _)| k.clone()).collect()
        };

        debug!(
            "[shard-sync] ShardSyncHashList: repo={}, l2={}, 对端条目={}, 本地缺失={}",
            msg.repo_type,
            msg.l2_shard,
            entries.len(),
            missing.len()
        );

        // 回复缺失 key 列表
        let reply = ShardSyncMissingMessage {
            repo_type: msg.repo_type,
            l2_shard: msg.l2_shard,
            missing_keys: missing,
            is_last: true,
        };
        if let Err(e) = conn
            .send_message(MessageType::ShardSyncMissing, &reply)
            .await
        {
            warn!("[shard-sync] 发送 ShardSyncMissing 失败: {}", e);
        } else {
            self.metrics.record_message_sent();
        }
    }

    /// 处理收到的 ShardSyncMissing：唤醒发送端等待的 oneshot
    ///
    /// 在活跃的分片同步引擎中查找匹配 repo_type 的引擎并唤醒其等待。
    pub fn handle_shard_sync_missing(&self, msg: ShardSyncMissingMessage) {
        let engines = self.active_shard_engines.read();
        for ((_peer, rt), engine) in engines.iter() {
            if *rt == msg.repo_type {
                engine.handle_shard_sync_missing(msg.l2_shard, msg.missing_keys.clone());
            }
        }
    }

    /// 处理收到的 ShardSyncComplete：分层分片同步完成。
    ///
    /// 入站 ShardSyncBatch 走 handle_sync_batch -> apply_*（add_nodes_batch_internal 等），
    /// 条目已写入本地 repo 并落盘；但**不再**实时维护内存 Merkle 的 l2_hashes，也**不**把所属
    /// L2 分片标 dirty_l2（这是切断 A->B->A 回环的关键：入站数据若标 dirty，
    /// incremental_sync_tick 会把整批再推回对端，形成无限回灌）。
    ///
    /// 内存 Merkle 根不靠这里维护，而是由周期任务 merkle_cold_rebuild_*（间隔
    /// merkle_full_rebuild_interval_secs）从 DB 全量 load_all_*_keys_hashes 后调用
    /// rebuild_cold_from_db / rebuild_all_from_db 重算 L2/L1/L0，并清空 dirty_l2，收敛到 DB 状态。
    ///
    /// 此处**不**调用 rebuild_all()：rebuild_all() 会把全部 65536 个 L2 标 dirty，
    /// 反而触发 incremental_sync_tick 把整库回灌给对端。收敛交给周期 cold rebuild 即可。
    pub fn handle_shard_sync_complete(&self, msg: ShardSyncCompleteMessage) {
        info!(
            "[shard-sync] 收到 ShardSyncComplete（入站已落库；不实时维护内存 Merkle、不标 dirty，根由周期 cold rebuild 收敛）: repo={}, total_l2={}, total_entries={}, max_seq={}",
            msg.repo_type, msg.total_l2_shards, msg.total_entries, msg.max_seq
        );
    }

    /// 启动分层 Merkle 对比 + 分片同步流程
    ///
    /// 由 handle_merkle_digest 在检测到大差异且对端支持分层 Merkle 时调用。
    /// 流程：
    /// 1. 对比 L1（已从 MerkleDigest 获得），找出差异 L1
    /// 2. 对每个差异 L1 请求 L2 哈希
    /// 3. 对比 L2，收集差异 L2
    /// 4. 启动 ShardSyncEngine 推送差异 L2
    pub async fn trigger_layered_sync(self: Arc<Self>, peer: NodeId, repo_type: u8) {
        let conn = match self.connection_manager.get_connection(&peer) {
            Some(c) => c,
            None => {
                warn!("[shard-sync] trigger_layered_sync: 无连接 peer={}", peer);
                return;
            }
        };

        if !conn.supports_layered_merkle() {
            warn!(
                "[shard-sync] 对端不支持分层 Merkle (version={}), 回退 DiffSync",
                conn.peer_protocol_version
                    .load(std::sync::atomic::Ordering::Relaxed)
            );
            self.trigger_diff_sync(peer, repo_type).await;
            return;
        }

        let _merkle = match self.merkle_for_repo(repo_type) {
            Some(m) => m,
            None => return,
        };

        info!(
            "[shard-sync] 启动分层 Merkle 对比: repo={}, peer={}",
            repo_type, peer
        );

        // 步骤1：请求对端的 L1 哈希（虽然 MerkleDigest 已有 L1，但为了流程统一，
        // 这里直接用已有的 digest 对比 L1，然后请求差异 L1 的 L2）
        // 实际上 handle_merkle_digest 已经做了 L1 对比，这里直接请求 L2

        // 发送 L2 请求给对端，获取差异 L1 的 L2 哈希
        // 注意：差异 L1 列表需要从 handle_merkle_digest 传入，这里简化为
        // 先请求所有 L1 的 L2（开销大但实现简单，后续优化为只请求差异 L1）
        // 为优化性能，我们直接用已有 digest 对比 L1，然后只请求差异 L1 的 L2

        // 这里需要从对端重新获取最新的 L1 哈希做对比
        let req = MerkleLevelRequestMessage {
            repo_type,
            level: 1,
            parent_shard: 0, // 忽略
        };

        if let Err(e) = conn
            .send_message(MessageType::MerkleLevelRequest, &req)
            .await
        {
            warn!("[shard-sync] 发送 MerkleLevelRequest(level=1) 失败: {}", e);
            // 回退到旧版 DiffSync
            self.trigger_diff_sync(peer, repo_type).await;
            return;
        }
        self.metrics.record_message_sent();
    }

    /// 启动分片同步引擎（数据服务器侧，推送差异 L2 分片数据到请求方）
    ///
    /// # 参数
    /// - conn: 到请求方的连接
    /// - repo_type: 仓库类型
    /// - diff_l2_shards: 差异 L2 分片列表
    pub fn start_shard_sync(
        self: &Arc<Self>,
        conn: Arc<Connection>,
        repo_type: u8,
        diff_l2_shards: Vec<u32>,
    ) {
        let peer_id = conn.node_id;
        let key = (peer_id, repo_type);

        // 互斥检查：同一 peer+repo 同时只允许一个引擎
        {
            let engines = self.active_shard_engines.read();
            if engines.contains_key(&key) {
                debug!(
                    "[shard-sync] 分片同步已在进行中: peer={}, repo={}",
                    peer_id, repo_type
                );
                return;
            }
        }

        let merkle = match self.merkle_for_repo(repo_type) {
            Some(m) => m,
            None => return,
        };

        info!(
            "[shard-sync] 创建分片同步引擎: peer={}, repo={}, 差异L2数={}",
            peer_id,
            repo_type,
            diff_l2_shards.len()
        );

        // 创建数据加载回调：按L2分片号加载数据（支持4种repo类型）
        let load_fn: Arc<dyn Fn(u32) -> Vec<SyncEntry> + Send + Sync> = {
            let node_repo = self.node_repo.clone();
            let peer_repo = self.peer_repo.clone();
            let infohash_repo = self.infohash_repo.clone();
            let tracker_repo = self.tracker_repo.clone();
            let repo_type_clone = repo_type;
            Arc::new(move |l2: u32| -> Vec<SyncEntry> {
                // l2 是 0..65535 的绝对 L2 索引，DB 的 shard 列存的是 L1 分片 0..255（= L2 / 256）
                let shard = MerkleTree::l1_for_l2(l2);
                let shards = [shard];
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                match repo_type_clone {
                    rt if rt == repo_type::NODE => {
                        let storage = node_repo.storage();
                        match storage.load_node_rows_by_shards(&shards) {
                            Ok(rows) => rows
                                .into_iter()
                                .filter_map(|(id, ip, port)| {
                                    let mut node_id = [0u8; 20];
                                    if id.len() == 20 {
                                        node_id.copy_from_slice(&id);
                                    }
                                    let addr: std::net::SocketAddr =
                                        format!("{}:{}", ip, port).parse().ok()?;
                                    let (key, payload, _hash) =
                                        build_node_sync_entry(node_id, addr)?;
                                    Some(SyncEntry {
                                        key,
                                        operation: operation::UPSERT,
                                        version: now,
                                        payload,
                                    })
                                })
                                .collect(),
                            Err(e) => {
                                warn!("[shard-sync] load_node_rows_by_shards 失败: {}", e);
                                Vec::new()
                            }
                        }
                    }
                    rt if rt == repo_type::PEER => {
                        let storage = match &peer_repo {
                            Some(r) => r.storage(),
                            None => return Vec::new(),
                        };
                        match storage.load_peer_rows_by_shards(&shards) {
                            Ok(rows) => rows
                                .into_iter()
                                .filter_map(|(infohash, ip, port, _source, _last_active)| {
                                    let mut ih = [0u8; 20];
                                    if infohash.len() == 20 {
                                        ih.copy_from_slice(&infohash);
                                    }
                                    let addr: std::net::SocketAddr =
                                        format!("{}:{}", ip, port).parse().ok()?;
                                    let (key, payload, _hash) =
                                        crate::federation::sync::peer_sync::build_peer_sync_entry(
                                            ih, addr,
                                        )?;
                                    Some(SyncEntry {
                                        key,
                                        operation: operation::UPSERT,
                                        version: now,
                                        payload,
                                    })
                                })
                                .collect(),
                            Err(e) => {
                                warn!("[shard-sync] load_peer_rows_by_shards 失败: {}", e);
                                Vec::new()
                            }
                        }
                    }
                    rt if rt == repo_type::INFOHASH => {
                        let storage = match &infohash_repo {
                            Some(r) => r.storage(),
                            None => return Vec::new(),
                        };
                        match storage.load_infohash_rows_by_shards(&shards) {
                            Ok(rows) => {
                                rows.into_iter()
                                    .filter_map(|(infohash, _last_seen, _first_source)| {
                                        let mut ih = [0u8; 20];
                                        if infohash.len() == 20 {
                                            ih.copy_from_slice(&infohash);
                                        }
                                        let (key, payload, _hash) =
                                            crate::federation::sync::infohash_sync::build_infohash_sync_entry(ih)?;
                                        Some(SyncEntry {
                                            key,
                                            operation: operation::UPSERT,
                                            version: now,
                                            payload,
                                        })
                                    })
                                    .collect()
                            }
                            Err(e) => {
                                warn!("[shard-sync] load_infohash_rows_by_shards 失败: {}", e);
                                Vec::new()
                            }
                        }
                    }
                    rt if rt == repo_type::TRACKER => {
                        let storage = match &tracker_repo {
                            Some(r) => r.storage(),
                            None => return Vec::new(),
                        };
                        match storage.load_tracker_rows_by_shards(&shards) {
                            Ok(rows) => {
                                rows.into_iter()
                                    .filter_map(|(url, _disabled, last_used)| {
                                        let ts = last_used.unwrap_or(0) as u64;
                                        let (key, payload, _hash) =
                                            crate::federation::sync::tracker_sync::build_tracker_sync_entry(&url)?;
                                        Some(SyncEntry {
                                            key,
                                            operation: operation::UPSERT,
                                            version: ts,
                                            payload,
                                        })
                                    })
                                    .collect()
                            }
                            Err(e) => {
                                warn!("[shard-sync] load_tracker_rows_by_shards 失败: {}", e);
                                Vec::new()
                            }
                        }
                    }
                    _ => {
                        warn!("[shard-sync] 未知repo_type={}", repo_type_clone);
                        Vec::new()
                    }
                }
            })
        };

        // 节点级 hash 去重：NODE repo 注入 (key, data_hash) 加载回调；其他 repo 暂为 None（回退全量）
        let hash_list_fn: Option<Arc<dyn Fn(u32) -> Vec<(Vec<u8>, Vec<u8>)> + Send + Sync>> =
            if repo_type == repo_type::NODE {
                let node_repo_hl = self.node_repo.clone();
                Some(Arc::new(move |l2: u32| -> Vec<(Vec<u8>, Vec<u8>)> {
                    let l1 = MerkleTree::l1_for_l2(l2);
                    let storage = node_repo_hl.storage();
                    match storage.load_node_rows_by_shards(&[l1]) {
                        Ok(rows) => rows
                            .into_iter()
                            .map(|(id, ip, port)| {
                                let key = format!("{}:{}", ip, port).into_bytes();
                                let mut buf = Vec::with_capacity(id.len() + ip.len() + 2);
                                buf.extend_from_slice(&id);
                                buf.extend_from_slice(ip.as_bytes());
                                buf.extend_from_slice(&port.to_le_bytes());
                                let data_hash = blake3::hash(&buf).as_bytes().to_vec();
                                (key, data_hash)
                            })
                            .collect(),
                        Err(e) => {
                            warn!(
                                "[shard-sync] hash_list: load_node_rows_by_shards 失败: {}",
                                e
                            );
                            Vec::new()
                        }
                    }
                }))
            } else {
                None
            };

        let engine = Arc::new(ShardSyncEngine::new(
            conn.clone(),
            peer_id,
            repo_type,
            merkle,
            self.config.clone(),
            Some(load_fn),
            hash_list_fn,
        ));

        // 注册引擎到活跃列表
        self.active_shard_engines
            .write()
            .insert(key, engine.clone());

        // 清理函数：引擎完成后从活跃列表移除
        let self_clone = self.clone();
        let key_clone = key;
        let engine_clone = engine.clone();

        // 启动引擎
        engine.start(diff_l2_shards);

        // 后台等待引擎完成并清理
        tokio::spawn(async move {
            // 简单等待：轮询检查引擎是否还在运行
            // [ALLOWED-SLEEP] 分片同步引擎完成后轮询清理，一次性后台任务（非周期性）
            loop {
                tokio::time::sleep(Duration::from_secs(
                    self_clone.config.shard_sync_engine_poll_interval_secs,
                ))
                .await;
                if !engine_clone.is_running() {
                    break;
                }
            }
            self_clone.active_shard_engines.write().remove(&key_clone);
            debug!(
                "[shard-sync] 引擎已移除: peer={}, repo={}",
                key_clone.0, key_clone.1
            );
        });
    }

    /// 增量同步 tick：基于 dirty L2 标记同步变更数据
    ///
    /// 由 TaskScheduler 周期性调用。对每个 repo：
    /// 1. 取出 dirty L2 分片
    /// 2. 如果有 dirty 分片且对端支持分层 Merkle，触发增量分片同步
    /// 3. 增量失败时降级到分层 Merkle 差异同步
    pub async fn incremental_sync_tick(self: Arc<Self>) {
        if !self.config.incremental_sync_enabled {
            return;
        }

        let repos = [
            repo_type::NODE,
            repo_type::PEER,
            repo_type::INFOHASH,
            repo_type::TRACKER,
        ];

        for &repo_type in &repos {
            let merkle = match self.merkle_for_repo(repo_type) {
                Some(m) => m,
                None => continue,
            };

            // 检查是否已有增量同步在进行
            {
                let active = self.incremental_sync_active.read();
                if active.contains(&repo_type) {
                    continue;
                }
            }

            // 取出 dirty L2 分片
            let dirty_l2 = merkle.take_dirty_l2_shards();
            if dirty_l2.is_empty() {
                continue;
            }

            debug!(
                "[shard-sync] 增量同步 tick: repo={}, dirty_l2数={}",
                repo_type,
                dirty_l2.len()
            );

            // 标记为活跃
            self.incremental_sync_active.write().insert(repo_type);

            // 找支持分层 Merkle 的活跃连接
            let connections = self.connection_manager.all_connections();
            let layered_peers: Vec<_> = connections
                .iter()
                .filter(|c| c.supports_layered_merkle())
                .cloned()
                .collect();

            if layered_peers.is_empty() {
                debug!(
                    "[shard-sync] 增量同步: 无支持分层Merkle的连接, repo={}",
                    repo_type
                );
                self.incremental_sync_active.write().remove(&repo_type);
                continue;
            }

            // 先发送最近变更的增量摘要（Push-Pull 优化）：把本 repo 的近期变更
            // 推给所有支持分层 Merkle 的邻居（不仅随机1个），对端对比后拉取缺失 key。
            let recent: Vec<(u8, Vec<u8>, u64)> = self
                .recent_changes
                .read()
                .iter()
                .filter(|(rt, _, _)| *rt == repo_type)
                .cloned()
                .collect();
            if !recent.is_empty() {
                for conn in &layered_peers {
                    let digest = GossipDigestMessage {
                        changes: recent.clone(),
                    };
                    if let Err(e) = conn.send_message(MessageType::GossipDigest, &digest).await {
                        debug!(
                            "[shard-sync] 增量 GossipDigest 发送失败 to={}: {}",
                            conn.node_id, e
                        );
                    } else {
                        self.metrics.record_message_sent();
                    }
                }
            }

            // 选择第一个分层 Merkle 节点作为同步目标
            let target = &layered_peers[0];
            let dirty_list: Vec<u32> = dirty_l2.into_iter().collect();

            info!(
                "[shard-sync] 触发增量分片同步: repo={}, peer={}, dirty_l2数={}",
                repo_type,
                target.node_id,
                dirty_list.len()
            );

            // 启动分片同步引擎推送 dirty L2
            self.start_shard_sync(target.clone(), repo_type, dirty_list);

            // 异步清理活跃标记
            let self_clone = self.clone();
            tokio::spawn(async move {
                // 等待一段时间后清理
                tokio::time::sleep(Duration::from_secs(
                    self_clone.config.incremental_sync_active_cleanup_secs,
                ))
                .await;
                self_clone
                    .incremental_sync_active
                    .write()
                    .remove(&repo_type);
            });
        }
    }
}

/// MerkleProvider 实现：为 GossipEngine 反熵任务提供各 repo 的 Merkle 摘要和分片数据
impl MerkleProvider for SyncManager {
    fn get_digest(&self, repo_type: u8) -> MerkleDigestMessage {
        // 反熵对账前等待 Merkle 更新队列排空（最多 500ms），
        // 避免用尚未 flush 的旧 Merkle 值对账导致误判；超时未排空则直接用当前值对账。
        if !self.merkle_queue.is_empty() {
            self.merkle_queue.wait_drain(MERKLE_DRAIN_TIMEOUT);
        }
        if let Some(merkle) = self.merkle_for_repo(repo_type) {
            merkle.digest(repo_type)
        } else {
            MerkleDigestMessage {
                repo_type,
                shard_count: 0,
                roots: Vec::new(),
                entry_counts: Vec::new(),
                full_root: None,
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
        assert_eq!(
            decoded.addr,
            "127.0.0.1:6885".parse::<SocketAddr>().unwrap()
        );
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
