//! 同步管理器
//!
//! 阶段2扩展：集成 Gossip 引擎、PeerRepo 同步、InfohashRepo 同步。
//! 阶段1的 NodeRepo 同步保留。

#![allow(clippy::type_complexity)]

pub mod bootstrap;
pub mod delta;
pub mod infohash_sync;
pub mod merkle_updater;
pub mod peer_sync;
pub mod range_reconcile;
pub mod shard_sync_engine;
pub mod tracker_sync;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex as ParkingMutex, RwLock};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, oneshot};
use tracing::{debug, info, warn};

use crate::event_bus::EventBus;
use crate::federation::config::FederationConfig;
use crate::federation::gossip::GossipEngine;
use crate::federation::merkle::{MerkleProvider, MerkleTree};
use crate::federation::metrics::FederationMetrics;
use crate::federation::node_id::NodeId;
use crate::federation::peer_conn::PeerConn;
use crate::federation::protocol;
use crate::federation::protocol::*;
use crate::federation::relay::RelayManager;
use crate::federation::session::SessionsHandle;
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

/// v7：协商重发节流（秒）。
const NEGOTIATE_RESEND_SECS: u64 = 60;
/// v7：协商发出后无 Ack 的降级放行等待（秒）—— 视为协商失败回落旧行为，防永久卡死。
const NEGOTIATE_FALLBACK_SECS: u64 = 120;
/// v7：看门狗触发后的暂停时长（× delta 拉取周期）。
const DELTA_WATCHDOG_PAUSE_MULT: u32 = 10;
/// v7：bootstrap 快照触发的最小行数（对端该 repo 为空且本端 ≥ 此量才走快照）。
const SNAPSHOT_MIN_ROWS: u64 = 1_000;
/// v7：快照分流比例阈值（本端比对端多出该比例且差值超阈值 → 快照）。
const SNAPSHOT_RATIO_THRESHOLD: f64 = 1.2;
/// v7：range 反熵 per-repo 周期（秒）：NODE churn 最高用短周期，TRACKER 小库最长。
const RANGE_INTERVAL_SECS: [(u8, u64); 4] = [
    (repo_type::NODE, 30),
    (repo_type::PEER, 120),
    (repo_type::INFOHASH, 300),
    (repo_type::TRACKER, 600),
];
/// v7：TRACKER（小库）叶级行数阈值。
const RANGE_LEAF_ROWS_TRACKER: u32 = 512;
/// v7：PEER / INFOHASH（中库）叶级行数阈值。
const RANGE_LEAF_ROWS_MID: u32 = 2048;

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
    sessions: Arc<SessionsHandle>,
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
    /// P2-1：服务端为每个对端缓存最近一次发出的 bootstrap 清单（分块边界由它决定）。
    bootstrap_manifests: RwLock<FxHashMap<NodeId, bootstrap::BootstrapManifest>>,
    /// P2-1：bootstrap 服务端带宽令牌桶（限流，铁律 1：低优先级、可抢占）。
    bootstrap_bucket: ParkingMutex<bootstrap::TokenBucket>,
    /// P1-4：range 反熵累计访问的区间数（可观测性）。
    range_ranges_visited: std::sync::atomic::AtomicU64,
    /// F3：range 叶级对账累计统计（区间数 / 本地多 / 对端多 / 触发修复次数）。
    /// 叶级明细降为 debug 后，由这些累加器在每轮 tick 收尾时汇总输出，避免刷屏。
    range_leaf_ranges: std::sync::atomic::AtomicU64,
    range_local_only_total: std::sync::atomic::AtomicU64,
    range_remote_only_total: std::sync::atomic::AtomicU64,
    range_repair_triggers: std::sync::atomic::AtomicU64,
    /// F1：每个 (对端, repo) 最近一次 delta 拉取发起时刻。
    /// 周期 tick 据此节流（间隔内不重发）并在无响应时超时重试。
    delta_request_at: RwLock<FxHashMap<(NodeId, u8), Instant>>,
    /// F2：对端最近一次回报的 oplog 水位（与本地 synced_seq 同序列空间，用于算真实 lag）。
    delta_peer_max: RwLock<FxHashMap<(NodeId, u8), u64>>,
    /// P2-5：反熵按 repo 差异化的上次执行时刻。
    anti_entropy_last: RwLock<FxHashMap<u8, Instant>>,
    /// v7：已向对端发起建连协商的时刻（重发节流）。
    negotiation_sent_at: RwLock<FxHashMap<NodeId, Instant>>,
    /// v7：协商结果（对端 → 各 repo 策略表，来自对端 Ack）。
    negotiated: RwLock<FxHashMap<NodeId, Vec<RepoStrategy>>>,
    /// v7：delta 看门狗：(peer, repo) → (上次 synced_seq, 连续零进展 tick 数, 暂停截止)。
    delta_watchdog: RwLock<FxHashMap<(NodeId, u8), (u64, u32, Option<Instant>)>>,
    /// v7：range 反熵按 repo 的上次执行时刻（per-repo interval 节流）。
    range_tick_last: RwLock<FxHashMap<u8, Instant>>,
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
        sessions: Arc<SessionsHandle>,
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

        let bootstrap_bucket = ParkingMutex::new(bootstrap::TokenBucket::new(
            config.bootstrap_rate_bytes_per_sec,
        ));

        Self {
            sessions,
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
            peer_digests: RwLock::new(FxHashMap::default()),
            diff_key_req_buffer: RwLock::new(FxHashMap::default()),
            pending_diff_key_resp: RwLock::new(FxHashMap::default()),
            active_shard_engines: RwLock::new(FxHashMap::default()),
            shard_hash_list_buffer: RwLock::new(FxHashMap::default()),
            layered_compare_sessions: RwLock::new(FxHashMap::default()),
            bootstrap_manifests: RwLock::new(FxHashMap::default()),
            bootstrap_bucket,
            range_ranges_visited: std::sync::atomic::AtomicU64::new(0),
            range_leaf_ranges: std::sync::atomic::AtomicU64::new(0),
            range_local_only_total: std::sync::atomic::AtomicU64::new(0),
            range_remote_only_total: std::sync::atomic::AtomicU64::new(0),
            range_repair_triggers: std::sync::atomic::AtomicU64::new(0),
            delta_request_at: RwLock::new(FxHashMap::default()),
            delta_peer_max: RwLock::new(FxHashMap::default()),
            anti_entropy_last: RwLock::new(FxHashMap::default()),
            negotiation_sent_at: RwLock::new(FxHashMap::default()),
            negotiated: RwLock::new(FxHashMap::default()),
            delta_watchdog: RwLock::new(FxHashMap::default()),
            range_tick_last: RwLock::new(FxHashMap::default()),
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

    /// 处理收到的同步批量消息（阶段1 SyncBatch 协议 / P1-3 delta 通道）
    pub fn handle_sync_batch(&self, repo_type: u8, entries: &[SyncEntry]) {
        // 【回环修复】入站条目一律**不写回 oplog**（oplog 只记本地 origin 的变更），
        // 也不登记进任何「本地最近变更」缓冲（Push-Pull Gossip 路径已随 P1-9 移除）。
        // 否则 A->B->A 会形成无限回灌：对端发来的数据被当成本地变更再广告/回推。
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

        // 【P0-C/P1-6】入站落库后把受影响的分片折进内存 Merkle。
        // 只重算受影响的 L2（按 L1 分组、每个 L1 从 DB 读一次），既不 rebuild 全树、
        // 也不标 dirty 触发回推，因此不会形成 A->B->A 回环；作用是把「本地 Merkle 相对 DB
        // 的滞后」从一整个冷重算周期（300s）压缩为本批立刻收敛，避免下一轮反熵继续判出假差异。
        // 注意：这里**包含 DELETE 键**——折算以 DB 为权威（查询已过滤 deleted_at），被删的 key
        // 在重算后自然从对应 L2 消失，从而让删除也立刻折进 Merkle；若排除 DELETE，则入站删除会
        // 在 Merkle 中残留一个幽灵叶子，直到下次冷重算才消失。
        let keys: Vec<Vec<u8>> = entries.iter().map(|e| e.key.clone()).collect();
        self.fold_merkle_from_keys(repo_type, &keys);
    }

    /// 【P0-C/P1-5】把一批入站 key 所影响的 L2 折进内存 Merkle。
    ///
    /// 只重算受影响的 L2：按 L2 去重后，一次性从 DB 精确加载这些 L2 的 (key, data_hash)
    /// （P1-5 起 `l2_shard` 列存真 L2，可按 L2 命中，不再有 256× 放大），
    /// 再对每个受影响 L2 调用 `recompute_l2_from_db`（其余 L2 保持不变）。
    /// - 不调用 `rebuild_all`（那会把全部 65536 个 L2 标记脏、开销 O(N)）；
    /// - 不标记 dirty（因此不会触发任何回推，无 A->B->A 风险）。
    ///
    /// 若本批触及的 L2 过多（≈全表），退化为标 dirty，交给周期增量任务/冷重算兜底，
    /// 避免在本路径里重载整表。
    fn fold_merkle_from_keys(&self, repo_type: u8, keys: &[Vec<u8>]) {
        if keys.is_empty() {
            return;
        }
        let merkle = match self.merkle_for_repo(repo_type) {
            Some(m) => m,
            None => return,
        };

        // 受影响 L2 集合（P1-5：DB 分片列存真 L2，可按 L2 精确加载，无需按 L1 放大）
        let affected: FxHashSet<u32> = keys.iter().map(|k| merkle.l2_shard_for_key(k)).collect();
        if affected.is_empty() {
            return;
        }

        // 触及 L2 过多（≈全表）→ 交给周期增量任务，避免本路径重载过多
        const MAX_FOLD_L2: usize = 4096;
        if affected.len() > MAX_FOLD_L2 {
            for &l2 in &affected {
                merkle.mark_dirty_l2(l2);
            }
            debug!(
                "[federation] 入站批次触及 {} 个 L2（>{}），转为 dirty 交周期增量任务兜底: repo={}",
                affected.len(),
                MAX_FOLD_L2,
                repo_type
            );
            return;
        }

        let l2s: Vec<u16> = affected.iter().map(|&l2| l2 as u16).collect();
        match self.load_keys_hashes_by_shards(repo_type, &l2s) {
            Ok(entries) => {
                // 按实际 L2 归并（用 merkle 的 key→L2 映射，保证与写入/加载口径一致）
                let mut grouped: FxHashMap<u32, Vec<(Vec<u8>, Vec<u8>)>> = FxHashMap::default();
                for (k, h) in entries {
                    grouped
                        .entry(merkle.l2_shard_for_key(&k))
                        .or_default()
                        .push((k, h));
                }
                // 受影响的 L2 即使加载不到条目（删除后变空）也必须重算写回「缺席」表示，
                // 否则删除不会折进 Merkle（幽灵叶子残留到下次冷重算）。
                for &l2 in &affected {
                    let v = grouped.remove(&l2).unwrap_or_default();
                    merkle.recompute_l2_from_db(l2, &v);
                }
            }
            Err(e) => {
                warn!(
                    "[federation] 折 Merkle 加载 {} 个 L2 失败: {}，转为 dirty",
                    affected.len(),
                    e
                );
                for &l2 in &affected {
                    merkle.mark_dirty_l2(l2);
                }
            }
        }
    }

    /// 按 repo 类型、按 L2 二级分片加载 (key, data_hash)（供 Merkle 增量重算使用）。
    fn load_keys_hashes_by_shards(
        &self,
        repo_type: u8,
        shards: &[u16],
    ) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match repo_type {
            repo_type::NODE => self
                .node_repo
                .storage()
                .load_node_keys_hashes_by_shards(shards),
            repo_type::PEER => match &self.peer_repo {
                Some(r) => r.storage().load_peer_keys_hashes_by_shards(shards),
                None => Ok(Vec::new()),
            },
            repo_type::INFOHASH => match &self.infohash_repo {
                Some(r) => r.storage().load_infohash_keys_hashes_by_shards(shards),
                None => Ok(Vec::new()),
            },
            repo_type::TRACKER => match &self.tracker_repo {
                Some(r) => r.storage().load_tracker_keys_hashes_by_shards(shards),
                None => Ok(Vec::new()),
            },
            _ => Ok(Vec::new()),
        }
    }

    /// 处理收到的 Gossip 消息
    pub fn handle_gossip_batch(&self, batch: GossipBatchMessage) {
        debug!(
            "[federation][DIAG] SyncManager::handle_gossip_batch ENTER: repo_type={}, entries={}",
            batch.repo_type,
            batch.entries.len()
        );
        let entries = self.gossip_engine.handle_gossip_batch(batch.clone());
        debug!(
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
        debug!(
            "[federation][perf] apply_node_sync ENTER: entries_len={}",
            entries.len()
        );
        if entries.len() > 100 {
            debug!(
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
        // 该调用只会把这批条目所属 L2 分片标 dirty；dirty_l2 的消费者是 Merkle 增量更新任务
        // （merkle_incremental_*），重算后可能被反熵重新推回对端。入站数据本就是对端发来的，
        // 再标 dirty 并推回 -> A->B->A 无限回环。本地新节点的 merkle dirty 由
        // NodeRepoImpl.propagate 的 update_batch 负责，与本入站路径无关。
        let total_elapsed = total_start.elapsed();

        debug!(
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
    pub fn handle_peer_info(self: &Arc<Self>, from_node_id: NodeId, counts: Vec<u32>) {
        let total = {
            let mut digests = self.peer_digests.write();
            let entry = digests.entry(from_node_id).or_insert_with(|| vec![0u32; 4]);
            for (i, &c) in counts.iter().take(4).enumerate() {
                entry[i] = c;
            }
            entry.iter().map(|c| *c as u64).sum::<u64>()
        };
        debug!(
            "[federation] 收到 PeerInfo from={}, 条目总数={}",
            from_node_id, total
        );

        // P1-3：握手完成（PeerInfo 到达）后，若 delta 通道开启且对端支持（version>=4），
        // 立即对四个 repo 各发起一次增量拉取（断点来自持久化的 delta_peer_seq，重启后续传）。
        // 关闭 delta 时完全不发 OpsRequest，行为与改造前一致。
        if self.config.delta_sync_enabled {
            let this = self.clone();
            tokio::spawn(async move {
                for &rt in &[
                    repo_type::NODE,
                    repo_type::PEER,
                    repo_type::INFOHASH,
                    repo_type::TRACKER,
                ] {
                    this.trigger_delta_sync(from_node_id, rt).await;
                }
            });
        }
    }

    /// 触发差异全量同步（接收方），按 repo 独立触发、独立并发。
    ///
    /// 由 Merkle 反熵驱动：当检测到与某对端的某 repo 差异分片≥20%时调用。
    /// 每 repo 最多1个差量同步并发，全局最多4个并行。
    async fn trigger_diff_sync(self: Arc<Self>, peer: NodeId, repo_type: u8) {
        // v7 退役开关：range 反熵已统一接管全部 repo，DiffSync（无续传/无限速，亿级下
        // 会陷入「60s settle → 整仓重发」循环）永久停用；差异迁移由 range 修复 +
        // delta 追平 + bootstrap 快照承担。`range_reconcile_enabled=false` 时保留旧行为。
        if self.config.range_reconcile_enabled {
            return;
        }
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
        let sent = match self.sessions.get_connection(&peer) {
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

    /// 处理收到的 DiffSyncRequest（本节点作为数据源 / 服务端）。
    ///
    /// 请求方携带本地各 repo 的 Merkle 摘要，本节点对比后找出差异分片，
    /// 只推送差异分片的条目（不是全量）。
    pub async fn handle_diff_sync_request(
        self: Arc<Self>,
        from_node_id: NodeId,
        req: DiffSyncRequestMessage,
    ) {
        let conn = match self.sessions.get_connection(&from_node_id) {
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
        conn: &PeerConn,
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
        conn: &PeerConn,
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
    pub fn handle_diff_sync_key_response(&self, conn: &PeerConn, msg: DiffSyncKeyResponseMessage) {
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
            self.node_repo.total_count_sync() as u32,
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
        let conn = match self.sessions.get_connection(&target_node_id) {
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
        conn: &PeerConn,
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

        let l1_total = merkle.shard_count().max(1) as f64;
        let diff_ratio = diffs.len() as f64 / l1_total;
        info!(
            "[federation] Merkle 对账发现 {} 个差异分片（{:.1}%）: repo_type={}, from={}",
            diffs.len(),
            diff_ratio * 100.0,
            digest.repo_type,
            conn.node_id
        );

        // ===== P0-A：升级判定改用 L2 粒度 =====
        // 背景：L1 每桶约 1600 行，高 churn 表（NODE）只要有 ~2% 的散列差异，256 个 L1 的哈希
        // 就会全部不同，于是「L1 差异 ≥20%」这个判据恒为真、永远走最重路径；而且 L1 比例
        // 无法区分「整表发散」与「个别行新增」。
        // 新逻辑：只要 L1 有差异且对端支持分层 Merkle，就先下钻到 L2（MerkleLevelRequest），
        // 拿到精确的差异 L2 集合后，在 handle_merkle_level_response 里按 L2 比例决定走
        // 「轻量 MerkleRequest（差异小且集中时）」还是「分片同步引擎（差异大/分散时）」。
        // 注意：本改动不改变任何线上消息格式（digest 仍携带 L1 哈希）。
        if conn.supports_layered_merkle() && self.config.layered_merkle_enabled {
            info!(
                "[federation] L1 差异 {} 个（{:.1}%），下钻 L2 精确对账: repo_type={}, from={}",
                diffs.len(),
                diff_ratio * 100.0,
                digest.repo_type,
                conn.node_id
            );
            self.trigger_layered_sync(conn.node_id, digest.repo_type)
                .await;
            return;
        }

        // 对端不支持分层 Merkle：退回旧判据（大差异走 DiffSync、小差异走逐分片 MerkleRequest）
        if diffs.len() * 5 >= 256 {
            info!(
                "[federation] 对端不支持分层Merkle，触发旧版DiffSync（repo_type={}, from={}）",
                digest.repo_type, conn.node_id
            );
            self.trigger_diff_sync(conn.node_id, digest.repo_type).await;
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
    pub async fn handle_merkle_request(&self, conn: &PeerConn, request: MerkleRequestMessage) {
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
        conn: Arc<PeerConn>,
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
        conn: Arc<PeerConn>,
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

                // 更新会话状态；把「收齐后的差异 L2 集合」带出锁作用域，锁随即释放。
                // 关键：后续的网络发送（.await）绝不能持有 parking_lot 写锁 ——
                // 否则该 future 非 Send，tokio::spawn 会编译失败（见 connection.rs 的调用点）。
                let ready_diff_l2: Option<Vec<u32>> = {
                    let mut sessions = self.layered_compare_sessions.write();
                    let mut ready: Option<Vec<u32>> = None;
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
                            if *pending_l1_count == 0 {
                                ready = Some(std::mem::take(diff_l2).into_iter().collect());
                            }
                        }
                    }
                    ready
                };

                // 所有 L1 都收齐了：按 L2 粒度决定修复路径（P0-A）
                let diff_l2_list = match ready_diff_l2 {
                    Some(list) => list,
                    None => return,
                };

                let total_l2 = merkle.l2_total_count().max(1) as f64;
                let l2_ratio = diff_l2_list.len() as f64 / total_l2;

                info!(
                    "[shard-sync] 分层对比完成: repo={}, peer={}, 差异L2总数={}/{}（{:.1}%）",
                    resp.repo_type,
                    conn.node_id,
                    diff_l2_list.len(),
                    total_l2 as u64,
                    l2_ratio * 100.0
                );

                if diff_l2_list.is_empty() {
                    return;
                }

                // 差异 L2 的父 L1 集合（判断差异是否「集中」）
                let mut parents: FxHashSet<u16> = FxHashSet::default();
                for &l2 in &diff_l2_list {
                    parents.insert(MerkleTree::l1_for_l2(l2));
                }

                // 阈值：差异 L2 ≥20%（整表级差异），或差异分散到 >32 个 L1
                // （按 L1 拉取会退化为接近全表）→ 走分片同步引擎（按 L2 定向推送）；
                // 否则差异小且集中 → 走轻量 MerkleRequest（只拉差异 L2 所在的少数 L1）。
                const REPAIR_MAX_L1: usize = 32;
                if l2_ratio >= 0.20 || parents.len() > REPAIR_MAX_L1 {
                    // 启动分片同步引擎
                    self.start_shard_sync(conn.clone(), resp.repo_type, diff_l2_list);
                } else {
                    let shards: Vec<u16> = parents.into_iter().collect();
                    info!(
                        "[shard-sync] L2 差异小且集中（{} 个 L2 / {} 个 L1），走轻量 MerkleRequest: repo={}",
                        diff_l2_list.len(),
                        shards.len(),
                        resp.repo_type
                    );
                    let req = MerkleRequestMessage {
                        repo_type: resp.repo_type,
                        shards,
                    };
                    if let Err(e) = conn.send_message(MessageType::MerkleRequest, &req).await {
                        warn!("[shard-sync] 发送轻量 MerkleRequest 失败: {}", e);
                    } else {
                        self.metrics.record_message_sent();
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
        conn: Arc<PeerConn>,
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
        conn: Arc<PeerConn>,
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

        // NODE repo：查本地该 L2 分片的 key 集合（走 l2_shard 索引，P1-5 起列为真 L2），计算缺失
        let missing: Vec<Vec<u8>> = if msg.repo_type == repo_type::NODE {
            let storage = self.node_repo.storage();
            match storage.load_node_rows_by_shards(&[msg.l2_shard as u16]) {
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
    /// 会被 Merkle 增量更新任务重算并可能经反熵再推回对端，形成无限回灌）。
    ///
    /// 内存 Merkle 根不靠这里维护，而是由周期任务 merkle_cold_rebuild_*（间隔
    /// merkle_cold_rebuild_interval_secs）从 DB 全量 load_all_*_keys_hashes 后调用
    /// rebuild_cold_from_db / rebuild_all_from_db 重算 L2/L1/L0，并清空 dirty_l2，收敛到 DB 状态。
    ///
    /// 此处**不**调用 rebuild_all()：rebuild_all() 会把全部 65536 个 L2 标 dirty，
    /// 反而把整库经反熵回灌给对端。收敛交给周期 cold rebuild 即可。
    pub fn handle_shard_sync_complete(&self, msg: ShardSyncCompleteMessage) {
        info!(
            "[shard-sync] 收到 ShardSyncComplete（入站已落库；不实时维护内存 Merkle、不标 dirty，根由周期 cold rebuild 收敛）: repo={}, total_l2={}, total_entries={}, max_seq={}",
            msg.repo_type, msg.total_l2_shards, msg.total_entries, msg.max_seq
        );
    }

    // ========================================================================
    // P1-3：增量（delta）同步通道
    //
    // 稳态主线 = delta：请求方只发 `OpsRequest { repo, since_seq }`，数据服务器从本地
    // `feed_oplog` 取 `seq > since_seq` 回 `OpsBatch`，成本 O(Δ)（Δ = 单轮新增变更数），
    // 与库总量 N、与差异量 d 都无关。丢包/裁剪越界/首次接触时由反熵（Merkle 对账）兜底。
    // 入站 apply 走既有幂等路径且**不写回 oplog**（否则 A→B→A 回环）。
    // ========================================================================

    /// delta 通道使用的 Storage（`feed_oplog` / `delta_peer_seq` 均为全局表，任取一个 repo 的 storage 即可）。
    fn delta_storage(&self) -> Arc<crate::storage::db::Storage> {
        self.node_repo.storage()
    }

    /// 轻量同步摘要（供 `/federation/status` 快接口；不触碰任何 SQLite 重查询）。
    ///
    /// 背景：`/sync-observability` 的 `oplog_len` 在大表慢盘节点上冷缓存可达数十秒
    /// （2026-09-21 实测 51 节点 28s），不适合作为巡检入口；oplog 行数已有内存缓存
    /// （`storage/oplog.rs`），加上 tick 计数即可让运维在毫秒级接口上看到反熵是否在跑。
    pub fn sync_brief(&self) -> crate::federation::SyncBrief {
        let s = self.metrics.snapshot();
        crate::federation::SyncBrief {
            oplog_len: self.delta_storage().oplog_len().unwrap_or(0),
            anti_entropy_ticks: s.anti_entropy_ticks,
            anti_entropy_digests_sent: s.anti_entropy_digests_sent,
            anti_entropy_no_conn_skips: s.anti_entropy_no_conn,
        }
    }

    /// 对某对端某 repo 发起增量拉取（发送首个 OpsRequest）。
    ///
    /// 断点来自本地持久化的版本向量 `delta_peer_seq`（重启后续传，不重来）。
    /// `delta_sync_enabled=false` 或对端协议 < 4 时直接返回（回退反熵）。
    pub async fn trigger_delta_sync(self: &Arc<Self>, peer: NodeId, repo: u8) {
        if !self.config.delta_sync_enabled {
            return;
        }
        let conn = match self.sessions.get_connection(&peer) {
            Some(c) => c,
            None => return,
        };
        if !conn.supports_delta_sync() {
            debug!(
                "[delta] 对端 {} 不支持 delta（version<4），跳过 repo={}（回退反熵）",
                peer, repo
            );
            return;
        }
        // v7：协商 + 稳定性门控 + 策略裁定（对端 < v7 时恒放行）
        if !self.delta_channel_allowed(&conn, repo) {
            debug!(
                "[delta] 协商未通过/被门控，跳过 repo={} peer={}（连接存活 {}s）",
                repo,
                peer,
                conn.connected_secs()
            );
            return;
        }
        let since = self
            .delta_storage()
            .get_peer_seq(&peer.0, repo)
            .unwrap_or(0)
            .max(0) as u64;
        // F1：记录发起时刻。周期 tick 据此节流；若对端无响应，超过间隔后会重试。
        self.delta_request_at
            .write()
            .insert((peer, repo), Instant::now());
        let req = OpsRequestMessage {
            repo,
            since_seq: since,
            limit: delta::DELTA_BATCH_LIMIT_DEFAULT,
        };
        match conn.send_message(MessageType::OpsRequest, &req).await {
            Ok(()) => {
                self.metrics.record_message_sent();
                debug!(
                    "[delta] 发起增量拉取: peer={}, repo={}, since_seq={}",
                    peer, repo, since
                );
            }
            Err(e) => warn!("[delta] 发送 OpsRequest 失败 peer={}: {}", peer, e),
        }
    }

    /// 处理对端的增量拉取请求（数据服务器侧）
    ///
    /// 从本地 oplog 取 `seq > since_seq` 的变更（升序，最多 limit 条），组装 OpsBatch 回发。
    /// 仅回发**本地 origin** 的变更（oplog 只记本地变更），成本 O(Δ)。
    pub async fn handle_ops_request(self: Arc<Self>, conn: Arc<PeerConn>, req: OpsRequestMessage) {
        if !self.config.delta_sync_enabled {
            debug!(
                "[delta] 收到 OpsRequest 但 delta_sync_enabled=false，忽略: peer={}",
                conn.node_id
            );
            return;
        }
        let limit = if req.limit == 0 {
            delta::DELTA_BATCH_LIMIT_DEFAULT as usize
        } else {
            req.limit as usize
        };
        let since = delta::seq_to_i64(req.since_seq);
        let records = match self.delta_storage().load_ops_since(req.repo, since, limit) {
            Ok(r) => r,
            Err(e) => {
                warn!("[delta] 加载 oplog 失败 repo={}: {}", req.repo, e);
                return;
            }
        };
        let ops = delta::records_to_entries(&records);
        // v7：字节上限 —— 大 value 场景防批帧失控（对齐 gossip_bulk_max_bytes 量级）。
        // 截断时 has_more 仍为 true，下一批从同一 seq 续拉（不丢数据，不空转：至少保留 1 条）。
        let mut truncated = false;
        let mut total = 0usize;
        let mut cut = ops.len();
        for (i, o) in ops.iter().enumerate() {
            total += o.key.len() + o.value.len() + 32;
            if total > delta::DELTA_BATCH_MAX_BYTES {
                cut = i;
                truncated = true;
                break;
            }
        }
        let mut ops = ops;
        if truncated {
            ops.truncate(cut.max(1));
        }
        let next_seq = ops.last().map(|o| o.seq).unwrap_or(req.since_seq);
        let has_more = !ops.is_empty() && (ops.len() >= limit || truncated);
        // F2：回带「本机在该 repo 上的」oplog 水位（= 该 repo 最后一条变更的 seq；无则 0）。
        // 必须**按 repo** 取水位：请求方的断点是按 repo 独立维护的，若用全局水位相减，
        // op 稀疏的 repo（如 TRACKER）会被算成「落后上千条」的虚高值。
        let server_max_seq = self
            .delta_storage()
            .oplog_max_seq_for_repo(req.repo)
            .unwrap_or(0)
            .max(0) as u64;
        debug!(
            "[delta] 响应 OpsRequest: peer={}, repo={}, since_seq={}, 返回={}, has_more={}, repo水位={}",
            conn.node_id,
            req.repo,
            req.since_seq,
            ops.len(),
            has_more,
            server_max_seq
        );
        let batch = OpsBatchMessage {
            repo: req.repo,
            ops,
            next_seq,
            has_more,
            server_max_seq,
        };
        if let Err(e) = conn.send_message(MessageType::OpsBatch, &batch).await {
            warn!("[delta] 发送 OpsBatch 失败 to={}: {}", conn.node_id, e);
        } else {
            self.metrics.record_message_sent();
        }
    }

    /// 处理对端的增量响应（请求方侧）
    ///
    /// 幂等应用 ops（走既有 `handle_sync_batch`，**不写回 oplog**），推进本地版本向量；
    /// `has_more=true` 时立即续拉下一批，直到对端返回空批。
    pub async fn handle_ops_batch(self: Arc<Self>, conn: Arc<PeerConn>, batch: OpsBatchMessage) {
        if !self.config.delta_sync_enabled {
            return;
        }
        let entries = delta::ops_to_sync_entries(&batch.ops);
        if !entries.is_empty() {
            self.handle_sync_batch(batch.repo, &entries);
        }
        // 推进版本向量（仅前进，不回退）
        if let Err(e) = self.delta_storage().set_peer_seq(
            &conn.node_id.0,
            batch.repo,
            delta::seq_to_i64(batch.next_seq),
        ) {
            warn!("[delta] 推进版本向量失败 peer={}: {}", conn.node_id, e);
        }
        // F2：记录对端在本 repo 的 oplog 水位。它与本机记录的 synced_seq 同属对端 seq
        // 空间，二者相减才是「真实落后量」；无此值时 lag 报 null（不跨空间相减）。
        if batch.server_max_seq > 0 {
            self.delta_peer_max
                .write()
                .insert((conn.node_id, batch.repo), batch.server_max_seq);
        }
        // 拉取往返成功：把节流计时推后，避免同一轮里 tick 立刻重发
        self.delta_request_at
            .write()
            .insert((conn.node_id, batch.repo), Instant::now());
        if !entries.is_empty() || batch.has_more {
            delta::log_applied(batch.repo, entries.len(), batch.next_seq);
        }

        // 还有更多：立即续拉下一批
        if batch.has_more {
            let req = OpsRequestMessage {
                repo: batch.repo,
                since_seq: batch.next_seq,
                limit: delta::DELTA_BATCH_LIMIT_DEFAULT,
            };
            if let Err(e) = conn.send_message(MessageType::OpsRequest, &req).await {
                warn!("[delta] 续拉 OpsRequest 失败 to={}: {}", conn.node_id, e);
            } else {
                self.metrics.record_message_sent();
            }
        }
    }

    // ========================================================================
    // v7：建连协商（SyncNegotiate / SyncNegotiateAck）
    //
    // 连接建立后双方互发一次 SyncNegotiate（互报各 repo 数据量 + 能力），
    // 收到对端 Negotiate 的一方按纯函数策略回 Ack（per-repo：DELTA / BOOTSTRAP / NONE）。
    // 稳定性门控：连接存活 ≥ strategy_min_conn_secs 才发协商；协商通过前 v7+ 对端的
    // delta/bootstrap 大通道不启动（range 只读对账不受限）。对端 < v7 回落旧行为。
    // ========================================================================

    /// v7：per-repo 叶级行数阈值（TRACKER 小库用小叶，PEER/INFOHASH 中库，NODE 走配置）。
    fn leaf_rows_for_repo(&self, repo: u8) -> u32 {
        match repo {
            repo_type::TRACKER => RANGE_LEAF_ROWS_TRACKER,
            repo_type::PEER | repo_type::INFOHASH => RANGE_LEAF_ROWS_MID,
            _ => self.config.range_reconcile_leaf_rows.max(1),
        }
    }

    /// v7：确保对端协商已发起/未过期（由 delta tick 周期调用；60s 重发节流）。
    async fn ensure_negotiation(self: &Arc<Self>, conn: &Arc<PeerConn>) {
        let needs = {
            let sent = self.negotiation_sent_at.read();
            match sent.get(&conn.node_id) {
                Some(t) => t.elapsed() >= Duration::from_secs(NEGOTIATE_RESEND_SECS),
                None => true,
            }
        };
        if !needs {
            return;
        }
        // 稳定性门控：连接存活不足则等下一轮
        if conn.connected_secs() < self.config.strategy_min_conn_secs {
            return;
        }
        self.negotiation_sent_at
            .write()
            .insert(conn.node_id, Instant::now());
        let msg = self.build_negotiate_message();
        match conn.send_message(MessageType::SyncNegotiate, &msg).await {
            Ok(()) => {
                self.metrics.record_message_sent();
                info!(
                    "[negotiate] 已发送协商请求 to={}（存活 {}s）",
                    conn.node_id,
                    conn.connected_secs()
                );
            }
            Err(e) => warn!("[negotiate] 发送协商请求失败 to={}: {}", conn.node_id, e),
        }
    }

    /// v7：构造本端协商载荷（各 repo 状态 + 能力）。
    fn build_negotiate_message(&self) -> SyncNegotiateMessage {
        let counts = self.local_entry_counts();
        let st = self.delta_storage();
        let max_seq = st.oplog_max_seq().unwrap_or(0).max(0) as u64;
        let min_seq = st.oplog_min_seq().unwrap_or(0).max(0) as u64;
        let retention = self.config.oplog_retention_secs;
        let repos = (repo_type::NODE..=repo_type::TRACKER)
            .map(|repo| {
                let idx = (repo - repo_type::NODE) as usize;
                RepoSyncState {
                    repo,
                    row_count: counts.get(idx).copied().unwrap_or(0) as u64,
                    max_seq,
                    min_seq,
                    retention_secs: retention,
                }
            })
            .collect();
        SyncNegotiateMessage {
            repos,
            caps: SyncCaps {
                send_rate_bytes_per_sec: self.config.bootstrap_rate_bytes_per_sec,
                recv_rate_bytes_per_sec: 0,
                disk_throughput_hint: 0,
                batch_limit: delta::DELTA_BATCH_LIMIT_DEFAULT,
            },
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    /// v7：协商策略决策（Ack 发送方视角：为「对端应如何从我这里取数」裁定）。
    ///
    /// 规则（对齐架构评审稿 §追平分流）：
    /// - 对端该 repo 为空且本端有量 → `BOOTSTRAP`（冷启动走快照）；
    /// - 本端比对端多 20% 以上且差 > `range_bulk_threshold_rows` → `BOOTSTRAP`（大差集走快照）；
    /// - 其余 → `DELTA`（稳态水位续拉）；双方皆空 → `NONE`。
    fn decide_strategies(&self, remote: &SyncNegotiateMessage) -> Vec<RepoStrategy> {
        let counts = self.local_entry_counts();
        let remote_of = |repo: u8| -> u64 {
            remote
                .repos
                .iter()
                .find(|r| r.repo == repo)
                .map(|r| r.row_count)
                .unwrap_or(0)
        };
        (repo_type::NODE..=repo_type::TRACKER)
            .map(|repo| {
                let idx = (repo - repo_type::NODE) as usize;
                let local = counts.get(idx).copied().unwrap_or(0) as u64;
                let peer = remote_of(repo);
                let strategy = if local == 0 && peer == 0 {
                    protocol::STRATEGY_NONE
                } else if (peer == 0 && local >= SNAPSHOT_MIN_ROWS)
                    || (local as f64 / peer.max(1) as f64 > SNAPSHOT_RATIO_THRESHOLD
                        && local.saturating_sub(peer) > self.config.range_bulk_threshold_rows)
                {
                    // 冷启动（对端为空且本端有量）或大差集 → 走 bootstrap 快照通道
                    protocol::STRATEGY_BOOTSTRAP
                } else {
                    protocol::STRATEGY_DELTA
                };
                RepoStrategy {
                    repo,
                    strategy,
                    rate_bytes_per_sec: if strategy == protocol::STRATEGY_BOOTSTRAP {
                        self.config.bootstrap_rate_bytes_per_sec
                    } else {
                        0
                    },
                    batch_limit: delta::DELTA_BATCH_LIMIT_DEFAULT,
                }
            })
            .collect()
    }

    /// v7：处理对端协商请求（应答方）—— 回 Ack，并记住对端状态摘要。
    pub async fn handle_sync_negotiate(
        self: Arc<Self>,
        conn: Arc<PeerConn>,
        msg: SyncNegotiateMessage,
    ) {
        let strategies = self.decide_strategies(&msg);
        for s in &strategies {
            info!(
                "[negotiate] 策略裁定 repo={} strategy={} rate={}（对端行数 {}）",
                s.repo,
                s.strategy,
                s.rate_bytes_per_sec,
                msg.repos
                    .iter()
                    .find(|r| r.repo == s.repo)
                    .map(|r| r.row_count)
                    .unwrap_or(0)
            );
        }
        let ack = SyncNegotiateAckMessage {
            repos: strategies,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };
        match conn.send_message(MessageType::SyncNegotiateAck, &ack).await {
            Ok(()) => self.metrics.record_message_sent(),
            Err(e) => warn!("[negotiate] 发送 Ack 失败 to={}: {}", conn.node_id, e),
        }
    }

    /// v7：处理对端协商确认（请求方）—— 存策略表，大通道随后放行。
    pub async fn handle_sync_negotiate_ack(
        self: Arc<Self>,
        conn: Arc<PeerConn>,
        msg: SyncNegotiateAckMessage,
    ) {
        let mut summary = String::new();
        for s in &msg.repos {
            summary.push_str(&format!("r{}={:?} ", s.repo, s.strategy));
        }
        info!(
            "[negotiate] 收到协商结果 from={}: {}",
            conn.node_id,
            summary.trim()
        );
        self.negotiated
            .write()
            .insert(conn.node_id, msg.repos.clone());
    }

    /// v7：查 (peer, repo) 的协商策略；未协商返回 `None`。
    pub fn strategy_for(&self, peer: &NodeId, repo: u8) -> Option<u8> {
        self.negotiated
            .read()
            .get(peer)?
            .iter()
            .find(|s| s.repo == repo)
            .map(|s| s.strategy)
    }

    /// v7：delta 大通道是否放行（协商 + 门控 + 策略）。
    ///
    /// v7+ 对端：需协商通过且策略为 DELTA（NONE/BOOTSTRAP 不走 delta）；
    /// 协商发出 120s 仍无 Ack → 视为协商失败降级放行（防永久卡死）。
    /// < v7 对端：回落旧行为（true）。
    fn delta_channel_allowed(&self, conn: &Arc<PeerConn>, repo: u8) -> bool {
        if !self.config.negotiation_enabled
            || conn.protocol_version() < delta::NEGOTIATION_PROTOCOL_VERSION
        {
            return true;
        }
        // 稳定性门控
        if conn.connected_secs() < self.config.strategy_min_conn_secs {
            return false;
        }
        match self.strategy_for(&conn.node_id, repo) {
            Some(protocol::STRATEGY_DELTA) => true,
            Some(_) => false,
            None => self
                .negotiation_sent_at
                .read()
                .get(&conn.node_id)
                .map(|t| t.elapsed() >= Duration::from_secs(NEGOTIATE_FALLBACK_SECS))
                .unwrap_or(false),
        }
    }

    /// v7：delta 看门狗。连续 N 个周期零进展且 lag>0 → 暂停 + 清除协商（触发重协商）。
    /// 返回 false 表示当前被暂停，跳过本轮拉取。
    fn delta_watchdog_ok(&self, peer: NodeId, repo: u8, interval: Duration, max_seq: u64) -> bool {
        let cur = self
            .delta_storage()
            .get_peer_seq(&peer.0, repo)
            .unwrap_or(0)
            .max(0) as u64;
        let stall_limit = self.config.delta_watchdog_stall_ticks.max(1);
        let mut pause = false;
        let mut ok = true;
        {
            let mut w = self.delta_watchdog.write();
            let e = w.entry((peer, repo)).or_insert((cur, 0, None));
            if let Some(until) = e.2 {
                if Instant::now() < until {
                    ok = false;
                } else {
                    e.2 = None;
                    e.0 = cur;
                    e.1 = 0;
                }
            }
            if ok {
                if cur > e.0 {
                    e.0 = cur;
                    e.1 = 0;
                } else if max_seq > cur {
                    // 有欠账但水位零进展
                    e.1 += 1;
                    if e.1 >= stall_limit {
                        warn!(
                            "[watchdog] delta 零进展 {} 周期 peer={} repo={} synced_seq={}（暂停并重协商）",
                            e.1, peer, repo, cur
                        );
                        pause = true;
                    }
                } else {
                    e.1 = 0;
                }
            }
        }
        if pause {
            let mut w = self.delta_watchdog.write();
            if let Some(e) = w.get_mut(&(peer, repo)) {
                e.1 = 0;
                e.2 = Some(Instant::now() + interval * DELTA_WATCHDOG_PAUSE_MULT);
            }
            // 清除协商结果 → 大通道全停，下一轮 ensure_negotiation 重发 → 重新裁定
            self.negotiated.write().remove(&peer);
            self.negotiation_sent_at.write().remove(&peer);
            return false;
        }
        ok
    }

    /// F1：delta 通道的周期驱动（由 TaskScheduler 周期调用）。
    ///
    /// 修复「delta 只在建连（PeerInfo）与 bootstrap 追尾时拉一次」的缺陷 —— 否则建连瞬间
    /// 本机 oplog 为空会导致空批返回、此后新写入永远不被拉取（实测 2185 条 op 从未被拉走）。
    ///
    /// 对每个已连接且支持 delta（v>=4）的对端 × 四个 repo，若距上次发起已超过
    /// `delta_sync_interval_secs`，则再发一次 OpsRequest。空批成本 = 一个极小请求 + 极小响应
    /// （O(Δ) 且 Δ=0），与库总量无关，可安全高频；节流表同时充当无响应时的重试计时器。
    /// `delta_sync_enabled=false` 时为 no-op（行为与改造前一致）。
    pub async fn delta_sync_tick(self: Arc<Self>) {
        if !self.config.delta_sync_enabled {
            return;
        }
        let interval = std::time::Duration::from_secs(self.config.delta_sync_interval_secs.max(1));
        let conns = self.sessions.all_connections();
        if conns.is_empty() {
            return;
        }
        let n_conns = conns.len();
        let mut triggered = 0u32;
        for conn in conns {
            if !conn.supports_delta_sync() {
                continue;
            }
            // v7：协商维护（节流重发；<v7 对端 no-op）
            if self.config.negotiation_enabled
                && conn.protocol_version() >= delta::NEGOTIATION_PROTOCOL_VERSION
            {
                self.ensure_negotiation(&conn).await;
            }
            for &rt in &[
                repo_type::NODE,
                repo_type::PEER,
                repo_type::INFOHASH,
                repo_type::TRACKER,
            ] {
                // v7：bootstrap 策略执行（大差集/冷启动走快照通道，不走 delta 硬拉）
                if self.config.bootstrap_enabled
                    && self.strategy_for(&conn.node_id, rt) == Some(protocol::STRATEGY_BOOTSTRAP)
                    && conn.connected_secs() >= self.config.strategy_min_conn_secs
                {
                    let running = self
                        .delta_storage()
                        .bootstrap_list()
                        .map(|list| {
                            list.iter()
                                .any(|p| p.repo == rt && p.phase != bootstrap::BootstrapPhase::Done)
                        })
                        .unwrap_or(false);
                    if !running {
                        info!(
                            "[negotiate] 执行快照策略：启动 bootstrap peer={} repo={}",
                            conn.node_id, rt
                        );
                        let sm = self.clone();
                        let peer = conn.node_id;
                        tokio::spawn(async move { sm.start_bootstrap(peer, rt).await });
                    }
                    continue;
                }
                // v7：看门狗（暂停期跳过；触发时已清协商）
                let peer_max = *self
                    .delta_peer_max
                    .read()
                    .get(&(conn.node_id, rt))
                    .unwrap_or(&0);
                if !self.delta_watchdog_ok(conn.node_id, rt, interval, peer_max) {
                    continue;
                }
                let due = {
                    let last = self.delta_request_at.read();
                    match last.get(&(conn.node_id, rt)) {
                        Some(t) => t.elapsed() >= interval,
                        None => true,
                    }
                };
                if due {
                    self.trigger_delta_sync(conn.node_id, rt).await;
                    triggered += 1;
                }
            }
        }
        if triggered > 0 {
            debug!(
                "[delta] 周期拉取触发 {} 次（连接数={}，间隔={}s）",
                triggered, n_conns, self.config.delta_sync_interval_secs
            );
        }
    }

    // ========================================================================
    // P1-4：Range-based（有序区间 + 分界点下钻）反熵
    //
    // 仅接管 NODE repo（churn 最高）；默认 range_reconcile_enabled=false，开启后先以
    // 只读诊断模式运行（只求差集 + 打印统计，不改数据），确认口径一致后再走实际修复。
    // 其余 repo 与旧版本对端仍走既有分层 Merkle（兼容路径）。
    // ========================================================================

    /// 区间边界转换：空 `&[u8]` 表示 ±∞（`None`）。
    fn range_bound(b: &[u8]) -> Option<&[u8]> {
        if b.is_empty() {
            None
        } else {
            Some(b)
        }
    }

    /// P1-4：处理对端的 RangeReconcile 请求（应答方）。
    pub async fn handle_range_reconcile_request(
        self: Arc<Self>,
        conn: Arc<PeerConn>,
        req: RangeReconcileRequestMessage,
    ) {
        if !self.config.range_reconcile_enabled {
            return;
        }
        // v7：统一 range 反熵 —— 4 个 repo 全部走 range 通道（key 编码与各 repo Merkle 一致）。
        if req.repo < repo_type::NODE || req.repo > repo_type::TRACKER {
            return;
        }
        let leaf_rows = if req.leaf_rows == 0 {
            range_reconcile::DEFAULT_LEAF_ROWS as usize
        } else {
            req.leaf_rows as usize
        };
        // 多取 1 条以判断是否超过叶级阈值
        let rows = match self.delta_storage().load_repo_key_hashes_in_range(
            req.repo,
            Self::range_bound(&req.lo),
            Self::range_bound(&req.hi),
            leaf_rows + 1,
        ) {
            Ok(r) => r,
            Err(e) => {
                warn!("[range] 加载区间失败 repo={}: {}", req.repo, e);
                return;
            }
        };
        let is_leaf = rows.len() <= leaf_rows;
        let resp = if is_leaf {
            RangeReconcileResponseMessage {
                repo: req.repo,
                lo: req.lo.clone(),
                hi: req.hi.clone(),
                digest: range_reconcile::range_digest(&rows),
                split_points: Vec::new(),
                entries: rows
                    .into_iter()
                    .take(range_reconcile::MAX_LEAF_ENTRIES)
                    .collect(),
                is_leaf: true,
                depth: req.depth,
            }
        } else {
            let sp = range_reconcile::split_points(
                &rows,
                self.config.range_reconcile_max_splits as usize,
            );
            RangeReconcileResponseMessage {
                repo: req.repo,
                lo: req.lo.clone(),
                hi: req.hi.clone(),
                digest: range_reconcile::range_digest(&rows),
                split_points: sp,
                entries: Vec::new(),
                is_leaf: false,
                depth: req.depth,
            }
        };
        if let Err(e) = conn
            .send_message(MessageType::RangeReconcileResponse, &resp)
            .await
        {
            warn!(
                "[range] 发送 RangeReconcileResponse 失败 to={}: {}",
                conn.node_id, e
            );
        } else {
            self.metrics.record_message_sent();
        }
    }

    /// P1-4：处理对端的 RangeReconcile 响应（请求方）。
    ///
    /// - 摘要相同 → 剪枝；
    /// - 对端为叶（或深度到顶）→ 对本地清单求集合差并**打印统计**；
    /// - 否则按对端分界点继续下钻（`depth+1`，受 `range_reconcile_max_depth` 约束）。
    pub async fn handle_range_reconcile_response(
        self: Arc<Self>,
        conn: Arc<PeerConn>,
        resp: RangeReconcileResponseMessage,
    ) {
        if !self.config.range_reconcile_enabled {
            return;
        }
        // v7：4 个 repo 全部走 range 通道
        if resp.repo < repo_type::NODE || resp.repo > repo_type::TRACKER {
            return;
        }
        let leaf_rows = self.leaf_rows_for_repo(resp.repo) as usize;
        let lo = Self::range_bound(&resp.lo);
        let hi = Self::range_bound(&resp.hi);
        let local = match self.delta_storage().load_repo_key_hashes_in_range(
            resp.repo,
            lo,
            hi,
            range_reconcile::MAX_LEAF_ENTRIES + 1,
        ) {
            Ok(r) => r,
            Err(e) => {
                warn!("[range] 请求方加载区间失败: {}", e);
                return;
            }
        };
        let local_digest = range_reconcile::range_digest(&local);
        // 可观测性：累计访问的区间数（衡量下钻效率）
        self.range_ranges_visited
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let decision = range_reconcile::decide(
            &local_digest,
            &resp.digest,
            resp.is_leaf,
            resp.depth,
            self.config.range_reconcile_max_depth,
        );

        match decision {
            range_reconcile::RangeDecision::Prune => {
                debug!(
                    "[range] 剪枝 repo={} [{}, {}) depth={}",
                    resp.repo,
                    String::from_utf8_lossy(&resp.lo),
                    String::from_utf8_lossy(&resp.hi),
                    resp.depth
                );
            }
            range_reconcile::RangeDecision::Leaf => {
                let (local_only, remote_only) = range_reconcile::key_diff(&local, &resp.entries);
                let n_local = local_only.len() as u64;
                let n_remote = remote_only.len() as u64;
                // F3：叶级明细由 info 降为 debug，并累加到计数器，由每轮 tick 收尾汇总输出。
                // 之前每轮下钻会打上千行 INFO（每区间一行），实测把 stdout.log 刷到 150MB。
                self.range_leaf_ranges
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.range_local_only_total
                    .fetch_add(n_local, std::sync::atomic::Ordering::Relaxed);
                self.range_remote_only_total
                    .fetch_add(n_remote, std::sync::atomic::Ordering::Relaxed);
                if n_local > 0 || n_remote > 0 {
                    debug!(
                        "[range] 叶级差异 repo={} [{}, {}) depth={}: 本地多={}, 对端多={}",
                        resp.repo,
                        String::from_utf8_lossy(&resp.lo),
                        String::from_utf8_lossy(&resp.hi),
                        resp.depth,
                        n_local,
                        n_remote
                    );
                }
                // F3：非诊断模式下真正执行修复
                if !self.config.range_reconcile_diagnostic_only && (n_local > 0 || n_remote > 0) {
                    self.range_repair_triggers
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // 对端多 → 触发 delta 拉取（本地从对端拉；delta 按repo通用）
                    if n_remote > 0 {
                        self.trigger_delta_sync(conn.node_id, resp.repo).await;
                    }
                    // 本地多 → 直接推送数据给对端（不等 gossip）。
                    // v7：NODE 走专用 Push 通道；其余 repo 由对端自己的 delta 从我方 oplog
                    // 追平（本地多的 key 必有对应 oplog 条目），无需专用推送。
                    if n_local > 0 && !local_only.is_empty() && resp.repo == repo_type::NODE {
                        match self.delta_storage().load_nodes_by_keys(&local_only) {
                            Ok(nodes) => {
                                if nodes.is_empty() {
                                    debug!(
                                        "[range] 本地多 {} 个 key，但加载到 0 条节点数据",
                                        local_only.len()
                                    );
                                } else {
                                    let push_msg =
                                        crate::federation::protocol::RangeReconcilePushMessage {
                                            repo: resp.repo,
                                            nodes: nodes
                                                .iter()
                                                .map(|n| {
                                                    crate::federation::protocol::PushNodeEntry {
                                                        id: n.id.to_vec(),
                                                        ip: n.ip.clone(),
                                                        port: n.port,
                                                        score: n.score,
                                                        state: n.state.as_bytes()[0],
                                                        query_count: n.query_count,
                                                        success_count: n.success_count,
                                                        total_latency_ms: n.total_latency_ms,
                                                        consecutive_failures: n
                                                            .consecutive_failures,
                                                        nodes_returned: n.nodes_returned,
                                                        last_active: 0,
                                                    }
                                                })
                                                .collect(),
                                        };
                                    if let Err(e) = conn.send_message(
                                        crate::federation::protocol::MessageType::RangeReconcilePush,
                                        &push_msg,
                                    ).await {
                                        warn!("[range] 推送本地多节点数据失败 to={}: {}", conn.node_id, e);
                                    } else {
                                        debug!("[range] 推送本地多节点数据 to={}: {} 条", conn.node_id, nodes.len());
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("[range] 加载本地多节点数据失败: {}", e);
                            }
                        }
                    }
                }
            }
            range_reconcile::RangeDecision::Descend => {
                if resp.depth >= self.config.range_reconcile_max_depth {
                    return;
                }
                let mut bounds: Vec<Vec<u8>> = Vec::with_capacity(resp.split_points.len() + 2);
                bounds.push(resp.lo.clone());
                bounds.extend(resp.split_points.iter().cloned());
                bounds.push(resp.hi.clone());
                for w in bounds.windows(2) {
                    let sub_lo = w[0].clone();
                    let sub_hi = w[1].clone();
                    if sub_lo == sub_hi {
                        continue;
                    }
                    let rows = match self.delta_storage().load_repo_key_hashes_in_range(
                        resp.repo,
                        Self::range_bound(&sub_lo),
                        Self::range_bound(&sub_hi),
                        leaf_rows + 1,
                    ) {
                        Ok(r) => r,
                        Err(_) => continue,
                    };
                    let digest = range_reconcile::range_digest(&rows);
                    let sub_req = RangeReconcileRequestMessage {
                        repo: resp.repo,
                        lo: sub_lo,
                        hi: sub_hi,
                        digest,
                        leaf_rows: leaf_rows as u32,
                        depth: resp.depth + 1,
                    };
                    if let Err(e) = conn
                        .send_message(MessageType::RangeReconcileRequest, &sub_req)
                        .await
                    {
                        warn!("[range] 下钻请求发送失败 to={}: {}", conn.node_id, e);
                        break;
                    }
                    self.metrics.record_message_sent();
                }
            }
        }
    }

    /// P1-4：处理 Range 反熵推送（接收方）—— 将推送的节点数据写入本地数据库。
    pub async fn handle_range_reconcile_push(
        self: Arc<Self>,
        conn: Arc<PeerConn>,
        msg: crate::federation::protocol::RangeReconcilePushMessage,
    ) {
        if msg.repo != repo_type::NODE {
            return;
        }
        let count = msg.nodes.len();
        if count == 0 {
            return;
        }
        // 批量写入本地数据库
        let repo = self.node_repo.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut inserted = 0;
            for n in &msg.nodes {
                let mut id_arr = [0u8; 20];
                if n.id.len() == 20 {
                    id_arr.copy_from_slice(&n.id);
                }
                let addr = std::net::SocketAddr::new(
                    n.ip.parse()
                        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0))),
                    n.port,
                );
                repo.add_node_sync(id_arr, addr);
                inserted += 1;
            }
            inserted
        })
        .await;
        match result {
            Ok(inserted) => {
                debug!(
                    "[range] 收到推送节点数据 from={}: {} 条，写入成功",
                    conn.node_id, inserted
                );
            }
            Err(e) => {
                warn!("[range] 处理推送节点数据失败 from={}: {}", conn.node_id, e);
            }
        }
    }

    /// P1-4：单轮 range-based 反熵（请求方驱动，由 TaskScheduler 周期调用）。
    ///
    /// v7：统一 range 通道 —— 4 个 repo 全部参与，per-repo 周期节流
    /// （NODE 30s / PEER 120s / INFOHASH 300s / TRACKER 600s）。
    /// 每个 repo 抽样 `range_reconcile_sample_ranges + 1` 个分界 key 形成若干区间
    /// （首尾接 ±∞），对每个区间发一个 depth=0 的 RangeReconcileRequest。
    /// 默认 `range_reconcile_enabled=false` 时为 no-op（行为与改造前一致）。
    pub async fn range_reconcile_tick(self: Arc<Self>) {
        if !self.config.range_reconcile_enabled {
            return;
        }
        for (repo, interval) in RANGE_INTERVAL_SECS {
            let due = {
                let last = self.range_tick_last.read();
                match last.get(&repo) {
                    Some(t) => t.elapsed().as_secs() >= interval,
                    None => true,
                }
            };
            if !due {
                continue;
            }
            self.range_tick_last.write().insert(repo, Instant::now());
            self.clone().range_reconcile_tick_repo(repo).await;
        }
    }

    /// v7：单 repo 的 range 反熵抽样对账（原 NODE 专属逻辑通用化）。
    async fn range_reconcile_tick_repo(self: Arc<Self>, repo: u8) {
        let conns = self.sessions.all_connections();
        if conns.is_empty() {
            return;
        }
        use std::time::{SystemTime, UNIX_EPOCH};
        let idx = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as usize)
            % conns.len();
        let conn = &conns[idx];
        if !conn.supports_range_reconcile() {
            debug!(
                "[range] 对端 {} 不支持 range 反熵（version<{}），跳过",
                conn.node_id,
                range_reconcile::RANGE_RECONCILE_PROTOCOL_VERSION
            );
            return;
        }
        let n = self.config.range_reconcile_sample_ranges.max(1) as usize;
        let keys = match self.delta_storage().sample_repo_range_keys(repo, n + 1) {
            Ok(k) => k,
            Err(e) => {
                warn!("[range] 抽样分界 key 失败 repo={}: {}", repo, e);
                return;
            }
        };
        if keys.len() < 2 {
            debug!("[range] repo={} 本地数据不足，跳过抽样对账", repo);
            return;
        }
        let mut bounds: Vec<Vec<u8>> = Vec::with_capacity(keys.len() + 2);
        bounds.push(Vec::new()); // -∞
        bounds.extend(keys);
        bounds.push(Vec::new()); // +∞
        let leaf_rows = self.leaf_rows_for_repo(repo);
        let storage = self.delta_storage();
        let mut sent = 0u32;
        for w in bounds.windows(2) {
            let lo = w[0].clone();
            let hi = w[1].clone();
            let rows = match storage.load_repo_key_hashes_in_range(
                repo,
                Self::range_bound(&lo),
                Self::range_bound(&hi),
                leaf_rows as usize + 1,
            ) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let digest = range_reconcile::range_digest(&rows);
            let req = RangeReconcileRequestMessage {
                repo,
                lo,
                hi,
                digest,
                leaf_rows,
                depth: 0,
            };
            if let Err(e) = conn
                .send_message(MessageType::RangeReconcileRequest, &req)
                .await
            {
                warn!(
                    "[range] 发送 RangeReconcileRequest 失败 to={}: {}",
                    conn.node_id, e
                );
                break;
            }
            self.metrics.record_message_sent();
            sent += 1;
        }
        // F3：叶级明细已降为 debug，这里给出每轮一行汇总（轮内发送量 + 累计对账统计 + 当前模式）
        let leaf_ranges = self
            .range_leaf_ranges
            .load(std::sync::atomic::Ordering::Relaxed);
        let local_only = self
            .range_local_only_total
            .load(std::sync::atomic::Ordering::Relaxed);
        let remote_only = self
            .range_remote_only_total
            .load(std::sync::atomic::Ordering::Relaxed);
        let repairs = self
            .range_repair_triggers
            .load(std::sync::atomic::Ordering::Relaxed);
        info!(
            "[range] repo={} 抽样对账发送到 {}（{} 个区间，连接数={}）| 累计 叶级对账={} 本地多={} 对端多={} 触发修复={} 模式={}",
            repo,
            conn.node_id,
            sent,
            conns.len(),
            leaf_ranges,
            local_only,
            remote_only,
            repairs,
            if self.config.range_reconcile_diagnostic_only {
                "诊断(只读)"
            } else {
                "修复"
            }
        );
    }

    // ========================================================================
    // P2-1/P2-2：bootstrap 专用通道（六阶段，与在线反熵解耦）
    //
    // 仅接管 NODE repo；默认 bootstrap_enabled=false（不注册、不发起、不响应）。
    // 一致性：用「显式区间边界的逻辑分块 + W0 水位 + 末段哈希校验」替代物理快照文件，
    // 漂移由阶段 ⑥ 校验发现并重拉该块（幂等）。
    // ========================================================================

    /// P2-1：处理对端的 bootstrap 清单请求（应答方）。
    ///
    /// 取水位 `w0 = oplog_max_seq()`，按有序 key 区间流式分块，缓存清单供后续分块请求使用。
    pub async fn handle_bootstrap_manifest_request(
        self: Arc<Self>,
        conn: Arc<PeerConn>,
        req: BootstrapManifestRequestMessage,
    ) {
        if !self.config.bootstrap_enabled {
            return;
        }
        if req.repo < repo_type::NODE || req.repo > repo_type::TRACKER {
            return;
        }
        let storage = self.delta_storage();
        let w0 = storage.oplog_max_seq().unwrap_or(0).max(0) as u64;
        let version = w0.wrapping_add(1) as u32; // 以 w0 派生：重新打清单即换版本
        let manifest = match bootstrap::build_repo_manifest_impl(
            &storage,
            req.repo,
            self.config.bootstrap_chunk_rows,
            w0,
            version,
        ) {
            Ok(m) => m,
            Err(e) => {
                warn!("[bootstrap] 建清单失败 repo={}: {}", req.repo, e);
                return;
            }
        };
        info!(
            "[bootstrap] 响应清单请求 from={}: 总行={}, 块数={}, w0={}",
            conn.node_id,
            manifest.total_rows,
            manifest.chunks.len(),
            w0
        );
        self.bootstrap_manifests
            .write()
            .insert(conn.node_id, manifest.clone());
        let resp = BootstrapManifestResponseMessage { manifest };
        if let Err(e) = conn
            .send_message(MessageType::BootstrapManifestResponse, &resp)
            .await
        {
            warn!("[bootstrap] 发送清单失败: {}", e);
        } else {
            self.metrics.record_message_sent();
        }
    }

    /// P2-1：处理对端的 bootstrap 分块请求（应答方）—— 按清单边界取条目回发（令牌桶限流）。
    pub async fn handle_bootstrap_chunk_request(
        self: Arc<Self>,
        conn: Arc<PeerConn>,
        req: BootstrapChunkRequestMessage,
    ) {
        if !self.config.bootstrap_enabled {
            return;
        }
        if req.repo < repo_type::NODE || req.repo > repo_type::TRACKER {
            return;
        }
        let manifest = match self.bootstrap_manifests.read().get(&conn.node_id).cloned() {
            Some(m) => m,
            None => {
                warn!(
                    "[bootstrap] 收到分块请求但无清单缓存 peer={}（需先发清单请求）",
                    conn.node_id
                );
                return;
            }
        };
        let chunk = match manifest.chunks.iter().find(|c| c.index == req.index) {
            Some(c) => c.clone(),
            None => {
                warn!(
                    "[bootstrap] 分块 index={} 越界（共 {} 块）",
                    req.index,
                    manifest.chunks.len()
                );
                return;
            }
        };
        let lo = Self::range_bound(&chunk.lo);
        let hi = Self::range_bound(&chunk.hi);
        let storage = self.delta_storage();
        // ① 内容哈希：与建清单同源（(key, data_hash) 有序流）—— v7 全 repo 通用
        let hash_rows = match storage.load_repo_key_hashes_in_range(
            req.repo,
            lo,
            hi,
            chunk.rows as usize + 1,
        ) {
            Ok(r) => r,
            Err(e) => {
                warn!("[bootstrap] 取块哈希失败 index={}: {}", req.index, e);
                return;
            }
        };
        let take = hash_rows.len().min(chunk.rows as usize);
        let hash = bootstrap::chunk_hash(&hash_rows[..take]);
        // ② 完整条目（含 payload，供接收方批量 upsert）—— v7 全 repo 通用
        let entries: Vec<SyncEntry> = match storage.load_repo_sync_entries_in_range(
            req.repo,
            lo,
            hi,
            chunk.rows as usize + 1,
        ) {
            Ok(rows) => rows.into_iter().take(chunk.rows as usize).collect(),
            Err(e) => {
                warn!("[bootstrap] 取块条目失败 index={}: {}", req.index, e);
                return;
            }
        };
        // ③ 令牌桶限流（不跨 await 持锁）
        let bytes: u64 = entries
            .iter()
            .map(|e| (e.key.len() + e.payload.len() + 16) as u64)
            .sum();
        let wait = {
            let mut bucket = self.bootstrap_bucket.lock();
            bucket.wait_duration(bytes)
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        let is_last = (req.index as usize + 1) >= manifest.chunks.len();
        let resp = BootstrapChunkResponseMessage {
            repo: req.repo,
            index: req.index,
            entries,
            hash,
            is_last,
        };
        if let Err(e) = conn
            .send_message(MessageType::BootstrapChunkResponse, &resp)
            .await
        {
            warn!("[bootstrap] 发送块 {} 失败: {}", req.index, e);
        } else {
            self.metrics.record_message_sent();
        }
    }

    /// P2-1：处理对端的 bootstrap 清单响应（请求方）—— 落进度并开始拉第一块。
    pub async fn handle_bootstrap_manifest_response(
        self: Arc<Self>,
        conn: Arc<PeerConn>,
        resp: BootstrapManifestResponseMessage,
    ) {
        if !self.config.bootstrap_enabled {
            return;
        }
        let mf = resp.manifest;
        if mf.repo != repo_type::NODE {
            return;
        }
        let now = chrono::Utc::now().timestamp_millis();
        let mut progress = bootstrap::BootstrapProgress::new(mf.repo, conn.node_id.0.to_vec(), now);
        progress.phase = bootstrap::BootstrapPhase::Transfer;
        progress.version = mf.version;
        progress.w0_seq = mf.w0_seq;
        progress.total_chunks = mf.chunks.len() as u64;
        progress.done_chunks = 0;
        // 把该对端该 repo 的版本向量对齐到 w0（追尾起点；MAX 语义下仅前进）
        let _ = self.delta_storage().set_peer_seq(
            &conn.node_id.0,
            mf.repo,
            delta::seq_to_i64(mf.w0_seq),
        );
        if let Err(e) = self.delta_storage().bootstrap_save(&progress, Some(&mf)) {
            warn!("[bootstrap] 保存进度失败: {}", e);
        }
        info!(
            "[bootstrap] 收到清单 from={}: 总行={}, 块数={}, w0={}",
            conn.node_id,
            mf.total_rows,
            mf.chunks.len(),
            mf.w0_seq
        );
        if mf.chunks.is_empty() {
            self.finish_bootstrap(&conn, mf.repo, mf.w0_seq).await;
            return;
        }
        self.request_bootstrap_chunk(&conn, mf.repo, 0, &mf).await;
    }

    /// P2-1：处理对端的 bootstrap 分块响应（请求方）—— 批量 upsert 落块、校验、续拉或切追尾。
    pub async fn handle_bootstrap_chunk_response(
        self: Arc<Self>,
        conn: Arc<PeerConn>,
        resp: BootstrapChunkResponseMessage,
    ) {
        if !self.config.bootstrap_enabled {
            return;
        }
        let (mut progress, mf) = match self.delta_storage().bootstrap_load(resp.repo) {
            Ok(Some((p, Some(m)))) => (p, m),
            Ok(_) => {
                warn!("[bootstrap] 收到块 {} 但无进度/清单，忽略", resp.index);
                return;
            }
            Err(e) => {
                warn!("[bootstrap] 读进度失败: {}", e);
                return;
            }
        };
        let chunk = match mf.chunks.iter().find(|c| c.index == resp.index) {
            Some(c) => c.clone(),
            None => return,
        };
        // ④ 批量 upsert（复用既有 apply_node_sync → add_nodes_batch_internal，严禁逐条 INSERT）
        if !resp.entries.is_empty() {
            self.handle_sync_batch(resp.repo, &resp.entries);
        }
        // ⑥ 校验：落地后重算该块 DB 摘要，与清单 hash 比对（幂等：不符只记 warn，重拉无害）
        let lo = Self::range_bound(&chunk.lo);
        let hi = Self::range_bound(&chunk.hi);
        let rows = self
            .delta_storage()
            .load_repo_key_hashes_in_range(resp.repo, lo, hi, chunk.rows as usize + 1)
            .unwrap_or_default();
        let take = rows.len().min(chunk.rows as usize);
        let ok = bootstrap::verify_chunk(&chunk.hash, &rows[..take]);
        if ok {
            progress.done_chunks = (resp.index as u64 + 1).max(progress.done_chunks);
            progress.bytes += resp
                .entries
                .iter()
                .map(|e| (e.key.len() + e.payload.len() + 16) as u64)
                .sum::<u64>();
            progress.phase = bootstrap::BootstrapPhase::Transfer;
        } else {
            // 传输期漂移或丢包：不改 done_chunks，等下一轮恢复任务重拉
            warn!(
                "[bootstrap] 块 {} 校验失败（可能传输期漂移），保持进度 done={}",
                resp.index, progress.done_chunks
            );
        }
        progress.updated_ms = chrono::Utc::now().timestamp_millis();
        let _ = self.delta_storage().bootstrap_save(&progress, None);

        let last = resp.is_last || (resp.index as usize + 1) >= mf.chunks.len();
        if last && ok {
            self.finish_bootstrap(&conn, resp.repo, mf.w0_seq).await;
        } else if !last {
            self.request_bootstrap_chunk(&conn, resp.repo, resp.index + 1, &mf)
                .await;
        }
    }

    /// 请求清单中第 `index` 块。
    async fn request_bootstrap_chunk(
        &self,
        conn: &PeerConn,
        repo: u8,
        index: u32,
        manifest: &bootstrap::BootstrapManifest,
    ) {
        if manifest.chunks.iter().all(|c| c.index != index) {
            return;
        }
        let req = BootstrapChunkRequestMessage { repo, index };
        match conn
            .send_message(MessageType::BootstrapChunkRequest, &req)
            .await
        {
            Ok(()) => self.metrics.record_message_sent(),
            Err(e) => warn!("[bootstrap] 请求块 {} 失败: {}", index, e),
        }
    }

    /// 完成 ③④ 后进入 ⑤ 追尾（复用 P1-3 delta 通道拉 `seq > w0`）。
    async fn finish_bootstrap(self: &Arc<Self>, conn: &PeerConn, repo: u8, w0_seq: u64) {
        let now = chrono::Utc::now().timestamp_millis();
        if let Ok(Some((mut p, mf))) = self.delta_storage().bootstrap_load(repo) {
            p.phase = bootstrap::BootstrapPhase::Done;
            p.updated_ms = now;
            let _ = self.delta_storage().bootstrap_save(&p, mf.as_ref());
        }
        info!(
            "[bootstrap] {} 块全部落地并校验通过，切 delta 追尾（since_seq={}）",
            conn.node_id, w0_seq
        );
        // ⑤ 追尾：从 w0 拉 oplog 增量（P1-3 通道）
        self.trigger_delta_sync(conn.node_id, repo).await;
    }

    /// P2-1：重启后恢复未完成的 bootstrap（按已落进度续传）。
    pub async fn bootstrap_resume_tick(self: Arc<Self>) {
        if !self.config.bootstrap_enabled {
            return;
        }
        let list = match self.delta_storage().bootstrap_list() {
            Ok(l) => l,
            Err(_) => return,
        };
        for p in list {
            if p.phase == bootstrap::BootstrapPhase::Done || p.peer.len() != 20 {
                continue;
            }
            let mut arr = [0u8; 20];
            arr.copy_from_slice(&p.peer);
            let peer = NodeId(arr);
            match self.delta_storage().bootstrap_load(p.repo) {
                Ok(Some((_, Some(mf)))) if (mf.chunks.len() as u64) > p.done_chunks => {
                    if let Some(conn) = self.sessions.get_connection(&peer) {
                        debug!(
                            "[bootstrap] 恢复续传: peer={}, repo={}, 从块 {} 继续",
                            peer, p.repo, p.done_chunks
                        );
                        self.request_bootstrap_chunk(&conn, p.repo, p.done_chunks as u32, &mf)
                            .await;
                    }
                }
                _ => {
                    // 无清单或已到末尾：重拉清单
                    self.clone().start_bootstrap(peer, p.repo).await;
                }
            }
        }

        // 定期检查：对比本地与对端各 repo 总数，差 20% 以上自动触发 bootstrap
        self.check_and_trigger_bootstrap().await;
    }

    /// 定期检查本地与对端各 repo 总数差异，差 20% 以上自动触发 bootstrap
    async fn check_and_trigger_bootstrap(self: &Arc<Self>) {
        if !self.config.bootstrap_enabled {
            return;
        }

        let local_counts = self.local_entry_counts();
        // 先 clone 出 digest 数据，立即释放读锁，避免读锁 guard 跨 await 导致 Send 不满足
        let digests: Vec<(NodeId, u32)> = {
            let d = self.peer_digests.read();
            d.iter()
                .filter_map(|(peer, counts)| {
                    if counts.is_empty() {
                        None
                    } else {
                        Some((*peer, counts[0]))
                    }
                })
                .collect()
        };

        // 只检查 NODE repo（bootstrap 目前只支持 NODE）
        let local_node_count = local_counts[0] as u64;
        if local_node_count < 1000 {
            // 本地数据太少（启动初期），不触发
            return;
        }

        for (peer, remote_node_count) in digests {
            let remote_node_count = remote_node_count as u64;
            if remote_node_count == 0 || local_node_count == 0 {
                continue;
            }

            // 对端比本地多 20% 以上才触发
            let ratio = remote_node_count as f64 / local_node_count as f64;
            if ratio < 1.2 {
                continue;
            }

            // 检查是否已有进行中的 bootstrap
            let already_running = if let Ok(list) = self.delta_storage().bootstrap_list() {
                list.iter().any(|p| {
                    p.peer.len() == 20 && {
                        let mut arr = [0u8; 20];
                        arr.copy_from_slice(&p.peer);
                        NodeId(arr) == peer
                            && p.repo == repo_type::NODE
                            && p.phase != bootstrap::BootstrapPhase::Done
                    }
                })
            } else {
                false
            };
            if already_running {
                continue;
            }

            info!(
                "[bootstrap] 检测到差异: local={}, remote={}, ratio={:.1}%, 触发 bootstrap: peer={}",
                local_node_count, remote_node_count, ratio * 100.0, peer
            );

            if let Some(conn) = self.sessions.get_connection(&peer) {
                self.clone()
                    .start_bootstrap(conn.node_id, repo_type::NODE)
                    .await;
                return; // 一次只触发一个
            }
        }
    }

    /// P2-1：向指定对端发起某 repo 的 bootstrap（拉清单 → 分块 → 追尾）。默认关闭。
    pub async fn start_bootstrap(self: Arc<Self>, peer: NodeId, repo: u8) {
        if !self.config.bootstrap_enabled {
            return;
        }
        if repo != repo_type::NODE {
            return;
        }
        let conn = match self.sessions.get_connection(&peer) {
            Some(c) => c,
            None => {
                warn!("[bootstrap] 目标 {} 无连接，取消", peer);
                return;
            }
        };
        if !conn.supports_bootstrap() {
            warn!(
                "[bootstrap] 对端 {} 不支持 bootstrap（version<{}）",
                peer,
                bootstrap::BOOTSTRAP_PROTOCOL_VERSION
            );
            return;
        }
        info!("[bootstrap] 向 {} 请求 repo={} 清单", peer, repo);
        let req = BootstrapManifestRequestMessage { repo };
        match conn
            .send_message(MessageType::BootstrapManifestRequest, &req)
            .await
        {
            Ok(()) => self.metrics.record_message_sent(),
            Err(e) => warn!("[bootstrap] 发送清单请求失败: {}", e),
        }
    }

    /// P2-3：同步面可观测性快照（oplog 水位 / 每对端增量落后 / bootstrap 进度 / range 对账）。
    pub fn sync_observability(&self) -> serde_json::Value {
        let st = self.delta_storage();
        let oplog_len = st.oplog_len().unwrap_or(0);
        let oplog_max = st.oplog_max_seq().unwrap_or(0).max(0) as u64;
        let oplog_min = st.oplog_min_seq().unwrap_or(0).max(0) as u64;
        // F2：lag 必须同序列空间相减。`synced_seq` 是本机已从该对端消费到的「对端 seq」，
        // 因此对照物只能是「对端回报的 oplog 水位」（delta_peer_max），不能用本机 oplog_max
        // （那是本机的 seq 空间，二者相减得到的是纯噪声）。未知时 lag 报 null。
        let peer_max = self.delta_peer_max.read();
        let lag: Vec<serde_json::Value> = st
            .all_peer_seqs()
            .unwrap_or_default()
            .into_iter()
            .map(|(peer, repo, seq)| {
                let seqv = seq.max(0) as u64;
                let mut arr = [0u8; 20];
                if peer.len() == 20 {
                    arr.copy_from_slice(&peer);
                }
                let pv = peer_max.get(&(NodeId(arr), repo)).copied();
                serde_json::json!({
                    "peer": peer.iter().map(|b| format!("{:02x}", b)).collect::<String>(),
                    "repo": repo,
                    "synced_seq": seqv,
                    "peer_max_seq": pv,
                    "lag_seq": pv.map(|m| m.saturating_sub(seqv)),
                })
            })
            .collect();
        drop(peer_max);
        let bootstraps: Vec<serde_json::Value> = st
            .bootstrap_list()
            .unwrap_or_default()
            .into_iter()
            .map(|p| {
                serde_json::json!({
                    "repo": p.repo,
                    "phase": p.phase.as_str(),
                    "total_chunks": p.total_chunks,
                    "done_chunks": p.done_chunks,
                    "ratio": p.ratio(),
                    "bytes": p.bytes,
                    "w0_seq": p.w0_seq,
                    "error": p.error,
                })
            })
            .collect();
        let range_stats = serde_json::json!({
            "leaf_ranges": self
                .range_leaf_ranges
                .load(std::sync::atomic::Ordering::Relaxed),
            "local_only": self
                .range_local_only_total
                .load(std::sync::atomic::Ordering::Relaxed),
            "remote_only": self
                .range_remote_only_total
                .load(std::sync::atomic::Ordering::Relaxed),
            "repair_triggers": self
                .range_repair_triggers
                .load(std::sync::atomic::Ordering::Relaxed),
            "diagnostic_only": self.config.range_reconcile_diagnostic_only,
        });
        // F1：当前被节流表跟踪的 (对端, repo) 对数 ≈ 活跃的 delta 拉取通道数
        let delta_tracked = self.delta_request_at.read().len();
        // P3-C：反熵周期任务可观测性 —— tick 执行次数 / 每个 repo 距上次对账多久。
        // 之前只靠 merkle_repairs 判断，两个都看不到时无法区分「没跑」和「跑了但无差异」。
        let m_snap = self.metrics.snapshot();
        let ae_last = self.anti_entropy_last.read();
        let ae_repos: Vec<serde_json::Value> = [
            (repo_type::NODE, "node"),
            (repo_type::PEER, "peer"),
            (repo_type::INFOHASH, "infohash"),
            (repo_type::TRACKER, "tracker"),
        ]
        .iter()
        .map(|(rt, name)| {
            serde_json::json!({
                "repo": *name,
                "repo_type": *rt,
                "last_tick_age_secs": ae_last.get(rt).map(|i| i.elapsed().as_secs()),
                "owned_by_range_reconcile": self.config.range_reconcile_enabled
                    && *rt == repo_type::NODE,
            })
        })
        .collect();
        drop(ae_last);
        serde_json::json!({
            "oplog": {
                "len": oplog_len,
                "min_seq": oplog_min,
                "max_seq": oplog_max,
                "retention_secs": self.config.oplog_retention_secs,
            },
            "ops_lag": lag,
            "delta_sync": {
                "enabled": self.config.delta_sync_enabled,
                "interval_secs": self.config.delta_sync_interval_secs,
                "tracked_pairs": delta_tracked,
            },
            "delta_sync_enabled": self.config.delta_sync_enabled,
            "range_reconcile_enabled": self.config.range_reconcile_enabled,
            "range_reconcile_diagnostic_only": self.config.range_reconcile_diagnostic_only,
            "bootstrap_enabled": self.config.bootstrap_enabled,
            "bootstrap": bootstraps,
            "reconcile_nodes_visited": self
                .range_ranges_visited
                .load(std::sync::atomic::Ordering::Relaxed),
            "range_stats": range_stats,
            "anti_entropy": {
                "node_interval_secs": self.config.anti_entropy_node_interval_secs,
                "other_interval_secs": self.config.anti_entropy_other_interval_secs,
                "ticks_total": m_snap.anti_entropy_ticks,
                "digests_sent_total": m_snap.anti_entropy_digests_sent,
                "no_conn_skips_total": m_snap.anti_entropy_no_conn,
                "repos": ae_repos,
            },
        })
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
        let conn = match self.sessions.get_connection(&peer) {
            Some(c) => c,
            None => {
                warn!("[shard-sync] trigger_layered_sync: 无连接 peer={}", peer);
                return;
            }
        };

        if !conn.supports_layered_merkle() {
            warn!(
                "[shard-sync] 对端不支持分层 Merkle (version={}), 回退 DiffSync",
                conn.protocol_version()
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
        conn: Arc<PeerConn>,
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
            // P0-B/P1-5：DB 分片列现已存真 L2，按 L2 取数即精确；下列内存过滤保留为**防御性兜底**
            // （防止个别旧行残留错误分片值被误纳入）。
            let merkle_l2 = merkle.clone();
            Arc::new(move |l2: u32| -> Vec<SyncEntry> {
                // P1-5：DB 的 l2_shard 列存的是真 L2（0..65535），直接按 l2 精确加载，
                // 不再需要「取整条 L1 再内存过滤」的 256× 放大。
                let shards = [l2 as u16];
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
                                    // P0-B：按 L2 精确过滤（DB 只按 L1 取数，这里剔除不属于本 L2 的行，避免 256× 放大）
                                    if merkle_l2.l2_shard_for_key(&key) != l2 {
                                        return None;
                                    }
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
                                    // P0-B：按 L2 精确过滤
                                    if merkle_l2.l2_shard_for_key(&key) != l2 {
                                        return None;
                                    }
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
                                        // P0-B：按 L2 精确过滤
                                        if merkle_l2.l2_shard_for_key(&key) != l2 {
                                            return None;
                                        }
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
                                        // P0-B：按 L2 精确过滤
                                        if merkle_l2.l2_shard_for_key(&key) != l2 {
                                            return None;
                                        }
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
                let merkle_hl = merkle.clone();
                Some(Arc::new(move |l2: u32| -> Vec<(Vec<u8>, Vec<u8>)> {
                    let storage = node_repo_hl.storage();
                    match storage.load_node_rows_by_shards(&[l2 as u16]) {
                        Ok(rows) => rows
                            .into_iter()
                            .filter_map(|(id, ip, port)| {
                                let key = format!("{}:{}", ip, port).into_bytes();
                                // P0-B：按 L2 精确过滤（hash 清单同样只需本 L2 的条目，避免整条 L1）
                                if merkle_hl.l2_shard_for_key(&key) != l2 {
                                    return None;
                                }
                                let mut buf = Vec::with_capacity(id.len() + ip.len() + 2);
                                buf.extend_from_slice(&id);
                                buf.extend_from_slice(ip.as_bytes());
                                buf.extend_from_slice(&port.to_le_bytes());
                                let data_hash = blake3::hash(&buf).as_bytes().to_vec();
                                Some((key, data_hash))
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

    /// P1-4：开启 range 反熵后，NODE repo 由 range 通道接管，反熵不再对它发 MerkleDigest。
    fn range_reconcile_owns(&self, repo_type: u8) -> bool {
        self.config.range_reconcile_enabled && repo_type == repo_type::NODE
    }

    /// P2-5：反熵按 repo 差异化 —— NODE（高 churn）用更短周期，其余用更长周期。
    fn anti_entropy_due(&self, repo_type: u8) -> bool {
        let interval = if repo_type == repo_type::NODE {
            self.config.anti_entropy_node_interval_secs
        } else {
            self.config.anti_entropy_other_interval_secs
        };
        let now = Instant::now();
        let mut last = self.anti_entropy_last.write();
        let due = match last.get(&repo_type) {
            Some(t) => now.duration_since(*t).as_secs() >= interval,
            None => true,
        };
        if due {
            last.insert(repo_type, now);
        }
        due
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;

    fn make_config() -> FederationConfig {
        FederationConfig {
            enabled: true,
            listen_port: 0,
            max_connections: 10,
            sync_node_enabled: true,
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
        let (shutdown_tx, _) = broadcast::channel(1);
        let cm = SessionsHandle::new_for_test();
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
        let (shutdown_tx, _) = broadcast::channel(1);
        let cm = SessionsHandle::new_for_test();
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
        let (shutdown_tx, _) = broadcast::channel(1);
        let cm = SessionsHandle::new_for_test();
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
