//! 同步管理器
//!
//! 阶段2扩展：集成 Gossip 引擎、PeerRepo 同步、InfohashRepo 同步。
//! 阶段1的 NodeRepo 同步保留。

#![allow(clippy::type_complexity)]

pub mod bootstrap;
pub mod delta;
pub mod infohash_sync;
pub mod peer_sync;
pub mod range_reconcile;
pub mod tracker_sync;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex as ParkingMutex, RwLock};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::event_bus::EventBus;
use crate::federation::config::FederationConfig;
use crate::federation::gossip::GossipEngine;
use crate::federation::metrics::FederationMetrics;
use crate::federation::node_id::NodeId;
use crate::federation::peer_conn::PeerConn;
use crate::federation::protocol;
use crate::federation::protocol::*;
use crate::federation::relay::RelayManager;
use crate::federation::session::SessionsHandle;
use crate::federation::sync::infohash_sync::InfohashSync;
use crate::federation::sync::peer_sync::PeerSync;
use crate::federation::sync::tracker_sync::TrackerSync;
use crate::storage::{InfohashRepoImpl, NodeRepoImpl, PeerRepoImpl, TrackerRepoImpl};

/// v7：协商重发节流（秒）。
const NEGOTIATE_RESEND_SECS: u64 = 60;
/// v7：协商发出后无 Ack 的降级放行等待（秒）—— 视为协商失败回落旧行为，防永久卡死。
const NEGOTIATE_FALLBACK_SECS: u64 = 120;
/// v8 F3：看门狗触发后的暂停时长（× delta 拉取周期）。原 10 周期停摆 10 分钟过久，缩到 2。
const DELTA_WATCHDOG_PAUSE_MULT: u32 = 2;
/// v8 F4：in-flight 请求视为存活的时长（秒）——超过后允许重发（覆盖写超时与短暂失联）。
const DELTA_INFLIGHT_TIMEOUT_SECS: u64 = 30;

/// 批次 3：全量差异巡检（本地 vs 对端各 repo 总数比对）的最小间隔（秒）。
/// 该巡检要做 DB 级全表 COUNT，不能挂在秒级返回的 bootstrap 续传任务里每轮都跑 ——
/// 实测会把续传任务的单次占槽拉到 164.36s，连带饿死同分类其它联邦任务。
const BOOTSTRAP_CHECK_INTERVAL_SECS: u64 = 300;
/// v7：bootstrap 快照触发的最小行数（对端该 repo 为空且本端 ≥ 此量才走快照）。
const SNAPSHOT_MIN_ROWS: u64 = 1_000;
/// v7：快照分流比例阈值（本端比对端多出该比例且差值超阈值 → 快照）。
const SNAPSHOT_RATIO_THRESHOLD: f64 = 1.2;
/// v10(F2)：Range 抽样进度的停滞阈值（秒）—— 超过未推进视为对端持续不可达/连接失效，
/// 弃置该进度、下一轮重新抽样。正常推进时每 tick 刷新，不会触发。
const RANGE_SAMPLE_STALL_SECS: u64 = 1800;
/// v10：bootstrap 发起互斥的 TTL（秒）。多触发器（协商 Ack / 巡检 / 续传欠账）并发点火
/// 的竞态窗口内同一 (peer,repo) 只允许一个在途发起；60s 后 bootstrap_state 必有进度，
/// 由 `bootstrap_running_fresh` 接管去重。
const BOOTSTRAP_INFLIGHT_TTL_SECS: u64 = 60;
/// v10(A)：双向引导冲突的判定窗口（秒）—— 该窗口内响应过对方的 bootstrap 请求即视为
/// 「对方正在从我拉快照」，node_id 字典序大的一方让路（响应方优先，确定性无震荡）。
const BOOTSTRAP_SERVING_WINDOW_SECS: u64 = 120;
/// v10(A+)：让路保持期（秒）—— 冲突成立后持续让路至少该时长。对端可能因停滞暂停
/// 请求超过 serving 窗口，仅靠 serving 判定会让路失效 → 双向重启震荡
/// （实测 15:30-15:34 三轮重启循环）。对端安静满该时长才恢复自己的拉取。
const BOOTSTRAP_YIELD_HOLD_SECS: u64 = 300;

/// v10(F2)：一轮 Range 抽样对账的进行中状态（断点续跑游标）。
///
/// 旧实现一个 tick 内同步发完全部 161 个区间，在百万行库上单轮 >300s，
/// 被 TaskScheduler 杀掉后进度（内存 for 循环）全部作废、下一轮从头再来 ——
/// 兜底对账永远完不成一轮。现在每 tick 只发送 `range_ranges_per_tick` 个区间，
/// 游标存放在 SyncManager（TaskScheduler 杀掉的是 future，不杀 SyncManager 状态），
/// 下一 tick 从断点继续。
#[derive(Clone)]
struct RangeSampleProgress {
    /// 分界 key 序列（含 ±∞ 首尾），区间数 = bounds.len() - 1；区间 i = [bounds[i], bounds[i+1])
    bounds: Vec<Vec<u8>>,
    leaf_rows: u32,
    /// 下一个待发送的区间下标
    next: usize,
    /// 最近一次推进时刻（停滞超 `RANGE_SAMPLE_STALL_SECS` 弃置重抽）
    updated: std::time::Instant,
}

/// P1-4：range 反熵单节点同时处理的最大 handler 数（请求/响应/拉/推共用一个闸）。
/// dispatch 对每条 range 消息都 `tokio::spawn`，突发上百请求会 spawn 上百并发 handler
/// 同时 load 区间 / 回帧，高负载节点 IO 饱和引发帧风暴与 os error 10053；闸削平突发。
const RANGE_MAX_CONCURRENT_HANDLERS: usize = 8;

/// 节点同步负载
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeSyncPayload {
    node_id: [u8; 20],
    addr: SocketAddr,
}

/// 构建 Node 同步条目的 (key, payload_bytes, data_hash)。
///
/// key 为 `addr.to_string()` 形态；data_hash 仅供原 Merkle 通道使用，现无消费方，
/// 保留三段返回签名以兼容既有调用点。
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
    tracker_sync: Option<Arc<TrackerSync>>,
    relay_manager: Option<Arc<RelayManager>>,
    /// peer repo 直接引用（供 peer infohash 关联查询使用）。
    /// 注：infohash/tracker repo 无需在本结构体留存句柄——它们仅用于构造
    /// InfohashSync / TrackerSync，条目数统计已改读 DB 权威值（local_entry_counts），
    /// 故不保留字段，避免死代码。
    peer_repo: Option<Arc<PeerRepoImpl>>,
    config: FederationConfig,
    metrics: Arc<FederationMetrics>,
    /// 已见过的对端各 repo 条目数（对端 node_id -> 各 repo 条目数），
    /// 由握手后的 PeerInfo 消息喂入，供 bootstrap 自动触发时判断对端数据量。
    peer_digests: RwLock<FxHashMap<NodeId, Vec<u32>>>,
    /// P2-1：服务端为每个对端缓存最近一次发出的 bootstrap 清单（分块边界由它决定）。
    /// 应答方侧缓存的清单。key 必须含 repo：多 repo 并行 bootstrap 时若只按 node_id
    /// 索引，后到的 repo 清单会覆盖先到的，导致分块请求按错误 repo 的边界取数据。
    bootstrap_manifests: RwLock<FxHashMap<(NodeId, u8), bootstrap::BootstrapManifest>>,
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
    /// v8 F4：每个 (对端, repo) 的 in-flight 请求发起时刻（DELTA_INFLIGHT_TIMEOUT_SECS 超时兜底）。
    /// 同一 (peer, repo) 未完成前不重发，消灭「tick + 续拉 + 重复应用再续拉」重试风暴。
    delta_inflight: RwLock<FxHashMap<(NodeId, u8), Instant>>,
    /// F2：对端最近一次回报的 oplog 水位（与本地 synced_seq 同序列空间，用于算真实 lag）。
    delta_peer_max: RwLock<FxHashMap<(NodeId, u8), u64>>,
    /// v7：已向对端发起建连协商的时刻（重发节流）。
    negotiation_sent_at: RwLock<FxHashMap<NodeId, Instant>>,
    /// v7：协商结果（对端 → 各 repo 策略表，来自对端 Ack）。
    negotiated: RwLock<FxHashMap<NodeId, Vec<RepoStrategy>>>,
    /// v7：delta 看门狗：(peer, repo) → (上次 synced_seq, 连续零进展 tick 数, 暂停截止)。
    delta_watchdog: RwLock<FxHashMap<(NodeId, u8), (u64, u32, Option<Instant>)>>,
    /// v7：range 反熵按 repo 的上次执行时刻（per-repo interval 节流）。
    range_tick_last: RwLock<FxHashMap<u8, Instant>>,
    /// F7：bootstrap 块校验连续失败计数（(对端, repo) → 次数）。
    /// 连续 ≥ `bootstrap_chunk_max_attempts` 次判定清单漂移（对端重启后清单已重建/数据已前进），
    /// 重拉清单自愈。v9 起按 (peer, repo) 计数 —— 旧实现只按 repo，多对端时会互相污染。
    bootstrap_verify_fails: RwLock<FxHashMap<(NodeId, u8), u32>>,
    /// 批次 3：应答方「清单现场重建」（F6）的进行中标记（node_id → 起始时刻）。
    /// 重建是 DB 全表扫描（百万行级，实测数分钟）；请求方每 60s resume 一次会反复触发，
    /// 双端互请时形成**双向重建循环**，把 DB IO 与 Federation 分类槽吃光。
    bootstrap_rebuild_at: RwLock<FxHashMap<(NodeId, u8), Instant>>,
    /// 批次 3：全量差异巡检（`check_and_trigger_bootstrap`）的上次执行时刻。
    /// 该巡检要做 DB 级全表计数比对，挂在 `bootstrap_resume_tick` 里会让本该秒级返回的
    /// 续传任务占住 Federation 槽上百秒（实测 max 164.36s），把其它联邦任务一起饿死。
    bootstrap_check_at: RwLock<Option<Instant>>,
    // ========================================================================
    // v9 收敛修复新增状态
    // ========================================================================
    /// v9：对端最近一次协商里自报的 per-repo 水位（连接早期即获得，不依赖 OpsBatch）。
    /// `delta_peer_max` 只在收到 OpsBatch 后才有值，重启后的一段时间里是空的 ——
    /// 而「对端静默 + 历史欠账巨大」恰恰是这段时间最需要判定的场景。
    peer_negotiate_state: RwLock<FxHashMap<NodeId, Vec<RepoSyncState>>>,
    /// v9：缓存清单的构建时刻（(peer, repo) → Instant）。
    /// 租约期内重复到达的清单请求**直接复用缓存**，不再全表重排 —— 旧实现每次请求都重建，
    /// 请求方每 60s 一次即形成「自持 DB 风暴」（实测单次重建 >131s，期间块请求被静默丢弃）。
    bootstrap_manifest_at: RwLock<FxHashMap<(NodeId, u8), Instant>>,
    /// v9：分块请求的在途/失败跟踪 —— (peer, repo) → (当前 index, 连续尝试次数, 首次尝试时刻)。
    ///
    /// 旧实现只会在 resume 周期里重发同一个 index，**没有次数与时间上限**，也不会升级；
    /// 应答方一旦静默（租约跳过 / index 越界 / DB 读失败），`done_chunks` 就永远停在原处
    /// （实测：`phase=transfer, done=0` 持续存在，两天内 `收到清单` 仅 1 次）。
    bootstrap_chunk_attempt: RwLock<FxHashMap<(NodeId, u8), (u32, u32, Instant)>>,
    /// v9：Range 修复去重 —— (peer, repo, lo, hi) → 上次修复时刻。
    /// 同一批差异在多个 tick 里被重复推拉（叠加 NODE 内容差异无法收敛时）会白白吃带宽。
    range_repair_recent: RwLock<FxHashMap<(NodeId, u8, Vec<u8>, Vec<u8>), Instant>>,
    /// v9：Range 每 repo 的下一个待对账连接下标（轮转），取代「按时间取模挑一条」。
    range_rr: RwLock<FxHashMap<u8, usize>>,
    /// v9：(peer, repo) → 本周期内最近一次对账时刻，用于多对端公平轮转。
    range_peer_tick_last: RwLock<FxHashMap<(NodeId, u8), Instant>>,
    /// v10(F2)：Range 抽样对账断点 —— (peer, repo) → 进行中的区间序列与游标。
    /// 每 tick 只发送 `range_ranges_per_tick` 个区间，被超时杀掉也不丢进度。
    range_progress: RwLock<FxHashMap<(NodeId, u8), RangeSampleProgress>>,
    /// v10(F4)：快照冷却期 —— (peer, repo) → 竣工时刻。冷却期内协商裁定与巡检
    /// 对该 repo 强制 DELTA，防「竣工 → 清协商重裁 → 行数未变 → 又裁 BOOTSTRAP」环。
    bootstrap_cooldown: RwLock<FxHashMap<(NodeId, u8), Instant>>,
    /// v10：bootstrap 发起互斥 —— (peer, repo) → 发起时刻（TTL 内重复触发直接拒绝）。
    bootstrap_inflight: RwLock<FxHashMap<(NodeId, u8), Instant>>,
    /// v10：本端节点身份（双向引导冲突时按 node_id 字典序确定性让路）。
    local_node_id: NodeId,
    /// v10(A)：响应方活动标记 —— (peer, repo) → 最近一次响应对方 bootstrap 请求的时刻。
    /// 「对方在从我拉快照」的信号，用于响应方优先串行化。
    bootstrap_serving_at: RwLock<FxHashMap<(NodeId, u8), Instant>>,
    /// v10(A+)：让路截止 —— (peer, repo) → 让路保持到的时刻（冲突后持续让路防震荡）。
    bootstrap_yield_until: RwLock<FxHashMap<(NodeId, u8), Instant>>,
    /// v9：delta 续拉未完成标记（(peer, repo)）。续拉发送失败时置位，下一 tick 不等间隔立即重试。
    delta_has_more: RwLock<FxHashSet<(NodeId, u8)>>,
    /// v9：检测到「对端 oplog 已被裁剪、中间段结构性缺失」的 (peer, repo)。
    /// 置位后该 repo 优先走 bootstrap/反熵，并在可观测性里暴露（旧实现是静默跳过 + lag 归零）。
    delta_gap: RwLock<FxHashSet<(NodeId, u8)>>,
    /// P1-4：range 反熵全局并发闸（请求/响应/拉/推 handler 共用），削平突发帧风暴。
    range_gate: Arc<tokio::sync::Semaphore>,
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
        // 保留 peer repo 引用供后续关联查询使用（后续 if let 会 move 原始变量）
        let peer_repo_clone = peer_repo.clone();

        // PeerSync
        let peer_sync = if config.sync_peer_enabled {
            if let Some(pr) = peer_repo {
                let ps = Arc::new(PeerSync::new(
                    pr,
                    gossip_engine.clone(),
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
            local_node_id,
            bootstrap_serving_at: RwLock::new(FxHashMap::default()),
            bootstrap_yield_until: RwLock::new(FxHashMap::default()),
            gossip_engine,
            peer_sync,
            infohash_sync,
            tracker_sync,
            relay_manager,
            peer_repo: peer_repo_clone,
            config,
            metrics,

            peer_digests: RwLock::new(FxHashMap::default()),
            bootstrap_manifests: RwLock::new(FxHashMap::default()),
            bootstrap_bucket,
            range_ranges_visited: std::sync::atomic::AtomicU64::new(0),
            range_leaf_ranges: std::sync::atomic::AtomicU64::new(0),
            range_local_only_total: std::sync::atomic::AtomicU64::new(0),
            range_remote_only_total: std::sync::atomic::AtomicU64::new(0),
            range_repair_triggers: std::sync::atomic::AtomicU64::new(0),
            delta_request_at: RwLock::new(FxHashMap::default()),
            delta_inflight: RwLock::new(FxHashMap::default()),
            delta_peer_max: RwLock::new(FxHashMap::default()),
            negotiation_sent_at: RwLock::new(FxHashMap::default()),
            negotiated: RwLock::new(FxHashMap::default()),
            delta_watchdog: RwLock::new(FxHashMap::default()),
            range_tick_last: RwLock::new(FxHashMap::default()),
            bootstrap_verify_fails: RwLock::new(FxHashMap::default()),
            bootstrap_rebuild_at: RwLock::new(FxHashMap::default()),
            bootstrap_check_at: RwLock::new(None),
            bootstrap_chunk_attempt: RwLock::new(FxHashMap::default()),
            bootstrap_manifest_at: RwLock::new(FxHashMap::default()),
            peer_negotiate_state: RwLock::new(FxHashMap::default()),
            range_repair_recent: RwLock::new(FxHashMap::default()),
            range_rr: RwLock::new(FxHashMap::default()),
            range_peer_tick_last: RwLock::new(FxHashMap::default()),
            range_progress: RwLock::new(FxHashMap::default()),
            bootstrap_cooldown: RwLock::new(FxHashMap::default()),
            bootstrap_inflight: RwLock::new(FxHashMap::default()),
            delta_has_more: RwLock::new(FxHashSet::default()),
            delta_gap: RwLock::new(FxHashSet::default()),
            range_gate: Arc::new(tokio::sync::Semaphore::new(RANGE_MAX_CONCURRENT_HANDLERS)),
        }
    }

    /// 启动 Node 同步后台任务（已迁移到 TaskScheduler）
    ///
    /// 周期性 Node 同步由 TaskScheduler 调用 `do_node_sync()` 驱动。
    /// do_node_sync 已退役为空实现（本地新节点由 NodeRepoImpl.add_node_sync 统一更新）。
    pub fn spawn_node_sync(self: Arc<Self>) {
        // 已迁移：Node 同步周期任务由 TaskScheduler 调度 do_node_sync()
    }

    /// 执行一次 Node 同步（已退役）：本地新节点由 NodeRepoImpl.add_node_sync 统一更新。
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
            // v9 收敛修复：NODE 的 data_hash = blake3(node_id ‖ ip ‖ port)，
            // 而 DHT 里同一 ip:port 被重新 announce 换成新 id 是常态 ⇒ 两端会对**同一个 key**
            // 长期判出「内容不同」。旧实现「本地已存在 且 version==0 ⇒ 跳过」让 delta /
            // bootstrap / Range-Push2 三条入站通道都无法修复它，于是反熵每轮重新发现、
            // 每轮推拉、每轮丢弃（实测 repair_triggers == leaf_ranges，差异量长期停在数百）。
            //
            // 现在改为**确定性裁决**：同一 key 两端 node_id 不一致时，统一取字典序较小者
            // 作为规范值。两端规则一致 ⇒ 一轮交换后即收敛，且不会来回翻覆（双向 LWW 会）。
            // v9 修正：裁决对**所有**入站条目生效（不再只对 version==0）—— gossip 的本地变更
            // 携带秒级时间戳（version>0）且会无条件覆盖内存 id，若绕过裁决，反熵刚把两端收敛到
            // 规范值就会被下一次 gossip 重新拉开，差异永不消失。
            if let Some(local_id) = self.node_repo.node_id_sync(payload.addr) {
                if local_id == payload.node_id {
                    continue;
                }
                if payload.node_id > local_id {
                    // 本地 id 更小 → 保留本地（对端下一轮会收敛到我们的值）
                    continue;
                }
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

    /// 处理收到的 PeerInfo（握手后对端立即发送的本地条目数）。
    ///
    /// 将对端各 repo 条目数记录到 peer_digests，供 bootstrap 自动触发时
    /// 判断对端数据量。
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
        crate::federation::SyncBrief {
            oplog_len: self.delta_storage().oplog_len().unwrap_or(0),
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
        // v8 F4：in-flight 去重 —— 同一 (peer, repo) 存在未完成请求（30s 内）时不重发。
        // 否则 tick + 续拉 + 重复应用再续拉叠加出重试风暴：每请求服务端都发 2MB 重复帧，
        // 单连接 writer 锁串行排队后乱序到达，实测 70s 内同一 since_seq 发起 15+ 次。
        {
            let mut w = self.delta_inflight.write();
            if let Some(t) = w.get(&(peer, repo)) {
                if t.elapsed() < Duration::from_secs(DELTA_INFLIGHT_TIMEOUT_SECS) {
                    debug!(
                        "[delta] in-flight 请求未完成，跳过本轮: peer={}, repo={}",
                        peer, repo
                    );
                    return;
                }
            }
            w.insert((peer, repo), Instant::now());
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
            Err(e) => {
                warn!("[delta] 发送 OpsRequest 失败 peer={}: {}", peer, e);
                // v8 F4：发送失败不会有响应回来，立即解除 in-flight 以便下轮重试
                self.delta_inflight.write().remove(&(peer, repo));
            }
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
        // v9：记录「该对端已消费本机该 repo 的 oplog 到 since」——这是本机 seq 空间里的位点，
        // 供 `trim_oplog_guarded` 按「最小对端进度」裁剪，避免裁出对端永远拉不到的空洞
        // （空洞会被请求方游标跨过并把 lag 抹平 = 假收敛）。失败不影响服务。
        if let Err(e) = self
            .delta_storage()
            .set_peer_ack(&conn.node_id.0, req.repo, since)
        {
            debug!("[delta] 记录对端 ack 失败（不影响服务）: {}", e);
        }
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
        // v8 F4：请求往返完成，清除 in-flight 标记（此后 tick 可发下一轮请求）。
        self.delta_inflight
            .write()
            .remove(&(conn.node_id, batch.repo));
        // F2：记录对端在本 repo 的 oplog 水位。它与本机记录的 synced_seq 同属对端 seq
        // 空间，二者相减才是「真实落后量」；无此值时 lag 报 null（不跨空间相减）。
        // v8：即使批已过期，水位也是最新信息，始终记录。
        if batch.server_max_seq > 0 {
            self.delta_peer_max
                .write()
                .insert((conn.node_id, batch.repo), batch.server_max_seq);
        }
        // v8 F5：游标单调保护 —— 过期/重复批（next_seq ≤ 当前游标）直接丢弃：
        // 不重复应用、不推进、不续拉。否则重试风暴下乱序到达的旧批会把游标
        // 打回去（实测 1016764 → 1015764），再触发同区间无限重拉。
        let cur = self
            .delta_storage()
            .get_peer_seq(&conn.node_id.0, batch.repo)
            .unwrap_or(0)
            .max(0) as u64;
        let next = delta::seq_to_i64(batch.next_seq).max(0) as u64;
        if next <= cur {
            debug!(
                "[delta] 丢弃过期/重复批: peer={}, repo={}, next_seq={} ≤ 当前 {}（单调保护）",
                conn.node_id, batch.repo, batch.next_seq, cur
            );
            // v9：单调保护分支同样要清「续拉未完成」标记 —— 否则竣工后的空批
            // （next_seq == cur）会让标记永久粘滞，使 tick 永久绕过配置的拉取间隔。
            self.delta_has_more
                .write()
                .remove(&(conn.node_id, batch.repo));
            return;
        }
        // v9：**oplog 空洞检测**（判据必须用对端的 per-repo `min_seq`，不能用 seq 间距）。
        //
        // 背景：`feed_oplog.seq` 是**全局** AUTOINCREMENT、4 个 repo 共用，因此
        // 「本批首条 seq 与本地游标的差值」衡量的是跨 repo 的全局间距，而不是该 repo
        // 被裁掉的 op 数 —— 稀疏 repo（TRACKER 仅数百条 / INFOHASH / PEER）连续两条同 repo
        // op 的全局间距动辄上万，用它判洞会把稀疏 repo **长期**误判为「已被裁剪」，
        // 于是每来一条稀疏 op 就触发一次整仓快照。
        //
        // 正确判据：对端在协商里自报的**该 repo** oplog 最小 seq（`RepoSyncState.min_seq`，
        // 与本地游标 `cur` 同属对端 seq 空间）。真实空洞当且仅当 `cur + 1 < peer_min_seq`。
        // 置位后 `delta_sync_tick` 让该 repo 一定走 bootstrap 补齐，并在观测接口暴露。
        let peer_min_seq = self
            .peer_negotiate_state
            .read()
            .get(&conn.node_id)
            .and_then(|v| v.iter().find(|s| s.repo == batch.repo).map(|s| s.min_seq))
            .unwrap_or(0);
        if peer_min_seq > 0 && cur.saturating_add(1) < peer_min_seq {
            let missing = peer_min_seq.saturating_sub(cur).saturating_sub(1);
            warn!(
                "[delta] 检测到 oplog 空洞: peer={}, repo={}, 缺口约 {} 条（本地游标 {} < 对端 min_seq {}）→ 转 bootstrap 补齐",
                conn.node_id, batch.repo, missing, cur, peer_min_seq
            );
            self.delta_gap.write().insert((conn.node_id, batch.repo));
        } else if batch.ops.is_empty() {
            // 空批 = 对端该 repo 已无更新可给 ⇒ 不存在待补空洞，清除标记
            // （旧写法只在「本批非空且间距小」时清除，空批会让标记永久粘滞）。
            self.delta_gap.write().remove(&(conn.node_id, batch.repo));
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
        // 拉取往返成功：把节流计时推后，避免同一轮里 tick 立刻重发；
        // v9：清掉「续拉未完成」标记（本轮已收到响应）。
        self.delta_request_at
            .write()
            .insert((conn.node_id, batch.repo), Instant::now());
        self.delta_has_more
            .write()
            .remove(&(conn.node_id, batch.repo));
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
                // v9：续拉失败必须留下待续标记 —— 旧实现只 warn，而唯一的补救路径
                // （tick 的节流重发）此前又被 `use_bootstrap → continue` 关闭，
                // 于是任何一次写超时都会把该 (peer,repo) 的游标永久冻结。
                if self.config.delta_retry_immediately {
                    self.delta_request_at
                        .write()
                        .remove(&(conn.node_id, batch.repo));
                    self.delta_has_more
                        .write()
                        .insert((conn.node_id, batch.repo));
                }
            } else {
                self.metrics.record_message_sent();
                // v8 F4：续拉同样标记 in-flight（防与下一轮 tick 叠加）
                self.delta_inflight
                    .write()
                    .insert((conn.node_id, batch.repo), Instant::now());
                self.delta_has_more
                    .write()
                    .insert((conn.node_id, batch.repo));
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

    /// B2：per-repo 叶级行数阈值。四个 repo 走**同一套**下钻逻辑，阈值**全部来自配置**
    /// `range_reconcile_leaf_rows_per_repo`（顺序 NODE/PEER/INFOHASH/TRACKER）。
    /// 旧实现 TRACKER/PEER/INFOHASH 用写死常量、NODE 却读配置，来源不一致，已统一。
    /// 索引越界（非法 repo）时回落 `range_reconcile_leaf_rows`。
    fn leaf_rows_for_repo(&self, repo: u8) -> u32 {
        let idx = repo.saturating_sub(repo_type::NODE) as usize;
        self.config
            .range_reconcile_leaf_rows_per_repo
            .get(idx)
            .copied()
            .unwrap_or(self.config.range_reconcile_leaf_rows)
            .max(1)
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

    /// 直接从各 Repo 实现获取真实数据量（协商 / bootstrap 触发判定用）。
    ///
    /// 统一以 **DB 为唯一数据源**，口径与 `rest_api::federation_status_handler` 的
    /// `*_repo_total` 完全一致（顺序 NODE/PEER/INFOHASH/TRACKER）：
    /// - NODE     = dht_nodes(deleted_at IS NULL) + 内存写队列未落库部分
    /// - PEER     = peers + peers_archive（冷归档仍计入总量，与 total 验收口径一致）
    /// - INFOHASH = infohashes(deleted_at IS NULL)
    /// - TRACKER  = trackers(deleted_at IS NULL)
    ///
    /// 旧实现读内存 repo 长度，那只是热/温子集（实测 peer 内存 12,731 而 DB total 25,564），
    /// 与本端对外展示的 total 口径不一致，会让 20% 差异的快照触发判定失真。
    /// 各 repo 条目数（协商/巡检/快照裁决共用）。
    ///
    /// 口径铁律：必须与 `load_repo_key_hashes_in_range`（bootstrap 清单扫描）一致。
    /// PEER 只取 `peers` 主表活行，**不含 `peers_archive`** —— 归档表不在清单扫描范围，
    /// 计入后「协商判定永远差一截、快照永远拉不到」→ BOOTSTRAP 死循环
    /// （2026-09-27 实测 52/58：58 报 40,042 vs 清单 28,009，每 5 分钟全量重拉一轮）。
    /// node 的 write_queue_len 是落库前瞬时差，自愈性偏差，保留。
    pub(crate) fn local_entry_counts(&self) -> Vec<u32> {
        let db = self.delta_storage().entity_counts_cached();
        let pick = |i: usize| -> u64 { db.get(i).copied().unwrap_or(-1).max(0) as u64 };
        let node = pick(0) + self.node_repo.write_queue_len_sync() as u64;
        vec![node as u32, pick(1) as u32, pick(3) as u32, pick(4) as u32]
    }

    /// v7：构造本端协商载荷（各 repo 状态 + 能力）。
    fn build_negotiate_message(&self) -> SyncNegotiateMessage {
        let counts = self.local_entry_counts();
        let st = self.delta_storage();
        let retention = self.config.oplog_retention_secs;
        // v8 F1：水位必须按 repo 取 —— 原来填全局 oplog 水位（同值 × 4 repo），
        // oplog 稀疏 repo 的欠账在协商里完全失真（假数据）。
        let repos = (repo_type::NODE..=repo_type::TRACKER)
            .map(|repo| {
                let idx = (repo - repo_type::NODE) as usize;
                RepoSyncState {
                    repo,
                    row_count: counts.get(idx).copied().unwrap_or(0) as u64,
                    max_seq: st.oplog_max_seq_for_repo(repo).unwrap_or(0).max(0) as u64,
                    min_seq: st.oplog_min_seq_for_repo(repo).unwrap_or(0).max(0) as u64,
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

    /// v10(A)：该 (peer, repo) 最近是否在响应对方的 bootstrap 请求（对方在从我拉快照）。
    fn serving_peer_bootstrap(&self, peer: &NodeId, repo: u8) -> bool {
        let m = self.bootstrap_serving_at.read();
        m.get(&(*peer, repo))
            .map(|t| t.elapsed().as_secs() < BOOTSTRAP_SERVING_WINDOW_SECS)
            .unwrap_or(false)
    }

    /// v10(A)：双向引导冲突让路判定。双方因 oplog seq 断档互有 gap 时会**同时**发起
    /// 全量快照互拉，双向 IO 互踩把块响应拖到超时 → 重拉 → w0 漂移断点归零，永不收敛
    /// （2026-09-27 实测 52/58：148 块 × 3 次「续传起点=0」）。
    /// 响应方优先：正在响应对方请求者继续传；node_id 字典序大的一方让路暂停自己的拉取
    /// （确定性打破对称，无震荡）。对端完成后让路方由 resume 自动恢复续传。
    ///
    /// v10(A+)：**持久让路** —— 让路方判定成立时刷新让路截止（`BOOTSTRAP_YIELD_HOLD_SECS`）；
    /// 对端请求停滞（serving 过期）后截止前仍让路，防「停滞期让路失效 → 双向重启」震荡
    /// （实测 15:30-15:34 三轮重启循环）。
    fn should_yield_bootstrap(&self, peer: &NodeId, repo: u8) -> bool {
        let serving_fresh = self.serving_peer_bootstrap(peer, repo);
        if serving_fresh && self.local_node_id.0 > peer.0 {
            // 冲突成立：持续让路并延长截止
            self.bootstrap_yield_until.write().insert(
                (*peer, repo),
                Instant::now() + Duration::from_secs(BOOTSTRAP_YIELD_HOLD_SECS),
            );
            return true;
        }
        if self.local_node_id.0 > peer.0 {
            // 对端安静：让路截止前仍保持让路（防震荡），截止后恢复拉取
            let m = self.bootstrap_yield_until.read();
            return m
                .get(&(*peer, repo))
                .map(|t| *t > Instant::now())
                .unwrap_or(false);
        }
        false
    }

    /// v10(A)：响应方活动标记入口（清单/块请求共用）。
    fn mark_bootstrap_serving(&self, peer: &NodeId, repo: u8) {
        self.bootstrap_serving_at
            .write()
            .insert((*peer, repo), Instant::now());
    }

    /// v10：bootstrap 发起互斥。返回 true = 获得发起槽位；false = TTL 内已有在途发起。
    /// 多触发器（协商 Ack / 巡检 / 续传欠账）并发点火的竞态窗口内，同一 (peer,repo)
    /// 只放行一个 start_bootstrap（实测启动期同 repo 15s 内两轮并行竣工）。
    fn try_acquire_bootstrap_slot(&self, peer: &NodeId, repo: u8) -> bool {
        let mut m = self.bootstrap_inflight.write();
        m.retain(|_, t| t.elapsed().as_secs() < BOOTSTRAP_INFLIGHT_TTL_SECS);
        m.insert((*peer, repo), Instant::now()).is_none()
    }

    /// v10(F4)：该 (peer, repo) 是否在快照冷却期内。
    fn snapshot_in_cooldown(&self, peer: &NodeId, repo: u8) -> bool {
        let mut m = self.bootstrap_cooldown.write();
        let ttl = self.config.bootstrap_cooldown_secs.max(1);
        m.retain(|_, t| t.elapsed().as_secs() < ttl);
        m.contains_key(&(*peer, repo))
    }

    /// v10(F5)：快照发起前的水位校验。
    ///
    /// 对端自报 oplog 保留窗口 `[min_seq, max_seq]`（`peer_negotiate_state`）；
    /// 我方游标（`delta_peer_seq`，已从对端应用到的 seq）若 ≥ 对端 `min_seq`，
    /// 说明欠账全部落在对端保留窗口内 —— **delta 即可追平，快照是浪费**；
    /// 仅当游标 < min_seq（对端 oplog 已裁剪掉我缺失的历史段 → 存在 delta 永远
    /// 补不上的空洞）才真正需要快照。对端状态缺失（协商早期）时保守放行（回退旧行为）。
    fn snapshot_really_needed(&self, peer: &NodeId, repo: u8) -> bool {
        let guard = self.peer_negotiate_state.read();
        let Some(states) = guard.get(peer) else {
            return true;
        };
        let Some(st) = states.iter().find(|s| s.repo == repo) else {
            return true;
        };
        if st.min_seq == 0 {
            return true;
        }
        let cursor = self
            .delta_storage()
            .get_peer_seq(&peer.0, repo)
            .unwrap_or(0)
            .max(0) as u64;
        cursor < st.min_seq
    }

    /// v7：协商策略决策（Ack 发送方视角：为「对端应如何从我这里取数」裁定）。
    ///
    /// 规则（对齐架构评审稿 §追平分流）：
    /// - 对端该 repo 为空且本端有量 → `BOOTSTRAP`（冷启动走快照）；
    /// - 本端比对端多 20% 以上且差 > `range_bulk_threshold_rows` → `BOOTSTRAP`（大差集走快照）；
    /// - 其余 → `DELTA`（稳态水位续拉）；双方皆空 → `NONE`。
    /// - v10(F4)：该 (peer,repo) 处于快照冷却期时一律 `DELTA` —— 竣工清协商（B5）后
    ///   裁定输入（行数）不会立刻变化，无冷却必然重裁 BOOTSTRAP。
    fn decide_strategies(&self, peer: &NodeId, remote: &SyncNegotiateMessage) -> Vec<RepoStrategy> {
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
                let peer_count = remote_of(repo);
                let strategy = if local == 0 && peer_count == 0 {
                    protocol::STRATEGY_NONE
                } else if self.snapshot_in_cooldown(peer, repo) {
                    // v10(F4)：冷却期内强制 DELTA（快照刚竣工，追尾是正确路径）
                    protocol::STRATEGY_DELTA
                } else if (peer_count == 0 && local >= SNAPSHOT_MIN_ROWS)
                    || (local as f64 / peer_count.max(1) as f64 > SNAPSHOT_RATIO_THRESHOLD
                        && local.saturating_sub(peer_count) > self.config.range_bulk_threshold_rows)
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
        let strategies = self.decide_strategies(&conn.node_id, &msg);
        // v9：记住对端自报的 per-repo 水位（连接早期即可用于判定欠账，不必等 OpsBatch）。
        self.peer_negotiate_state
            .write()
            .insert(conn.node_id, msg.repos.clone());
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
        // v10(F6)：Ack 即行动 —— 对端裁定 BOOTSTRAP 的 repo 立即发起快照，
        // 不再等最长 5 分钟的巡检周期。发起前过三重守卫：冷却（F4）、
        // 水位校验（F5，欠账在对端 oplog 窗口内时 delta 即可、无需快照）、
        // 单飞（bootstrap_running_fresh）。
        if self.config.bootstrap_enabled {
            let peer = conn.node_id;
            for s in &msg.repos {
                if s.strategy != protocol::STRATEGY_BOOTSTRAP {
                    continue;
                }
                if self.snapshot_in_cooldown(&peer, s.repo) {
                    debug!(
                        "[bootstrap] 收到 BOOTSTRAP 裁决 repo={} 但冷却中，跳过",
                        s.repo
                    );
                    continue;
                }
                if !self.snapshot_really_needed(&peer, s.repo) {
                    debug!(
                        "[bootstrap] 收到 BOOTSTRAP 裁决 repo={} 但欠账在对端 oplog 窗口内（游标≥min_seq），delta 追平即可",
                        s.repo
                    );
                    continue;
                }
                let running_rows = self.delta_storage().bootstrap_list().unwrap_or_default();
                if self.bootstrap_running_fresh(&running_rows, peer, s.repo) {
                    continue;
                }
                info!(
                    "[bootstrap] 协商裁定 BOOTSTRAP repo={}，立即发起（不等巡检）: peer={}",
                    s.repo, peer
                );
                let sm = self.clone();
                let repo = s.repo;
                tokio::spawn(async move {
                    sm.start_bootstrap(peer, repo).await;
                });
            }
        }
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
    /// v9 语义修正：**策略只决定「优先怎么追」，不再决定「能不能追」**。
    /// 旧实现只放行 `STRATEGY_DELTA`，于是「因为欠账大被裁定 BOOTSTRAP」直接导致 delta 也被
    /// 拒绝 —— 与「bootstrap 卡死」叠加就是永久停摆（实测 repo1/2/3 零进展 25 分钟）。
    /// 现在 `BOOTSTRAP` 与 `DELTA` 一样放行 delta（快照与增量并行，快照只负责补历史空洞）；
    /// 只有 `NONE`（双方皆空）不放行。
    ///
    /// v7+ 对端：需协商通过；协商发出 120s 仍无 Ack → 视为协商失败降级放行（防永久卡死）。
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
            Some(protocol::STRATEGY_DELTA) | Some(protocol::STRATEGY_BOOTSTRAP) => true,
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
        // P1-4：有请求在途时不计入 stall。旧逻辑只看「水位有没有动」，而对端响应慢于
        // 几个 tick 属正常（大批量 OpsBatch 拉取本身就要几十秒），结果被误判为零进展
        // → 暂停 + 清除协商 → 打断正在推进的同步并重走一遍协商，反而更慢。
        let inflight = self
            .delta_inflight
            .read()
            .get(&(peer, repo))
            .map(|t| t.elapsed() < Duration::from_secs(DELTA_INFLIGHT_TIMEOUT_SECS))
            .unwrap_or(false);
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
                } else if max_seq > cur && !inflight {
                    // 有欠账、无请求在途、水位零进展 → 才算真 stall
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

    /// P1-5：清理已失联对端的 delta 侧状态（水位 / 看门狗 / 节流 / in-flight）。
    ///
    /// `delta_peer_max` 在断连后不清：`陈旧水位 - 当前 synced_seq` 会是虚高欠账，
    /// 使 `lag_over` 持续为真 → 该 repo 恒被判「欠账巨大」→ 与 P0-1 叠加即永久停摆；
    /// in-flight 不清则重连后首个请求会被误判去重而直接跳过。
    fn prune_stale_delta_state(&self, conns: &[Arc<PeerConn>]) {
        let alive: std::collections::HashSet<NodeId> = conns.iter().map(|c| c.node_id).collect();
        let mut pruned = 0usize;
        {
            let mut m = self.delta_peer_max.write();
            let before = m.len();
            m.retain(|(p, _), _| alive.contains(p));
            pruned += before - m.len();
        }
        self.delta_watchdog
            .write()
            .retain(|(p, _), _| alive.contains(p));
        self.delta_request_at
            .write()
            .retain(|(p, _), _| alive.contains(p));
        self.delta_inflight
            .write()
            .retain(|(p, _), _| alive.contains(p));
        // v9：协商表/协商时刻表/续拉标记/gap 标记同样按存活对端清理 ——
        // 旧实现不清 `negotiated`，重连后会沿用陈旧策略（可能把 delta 误门控），
        // 且内存随历史对端数单调增长。
        self.negotiated.write().retain(|p, _| alive.contains(p));
        self.negotiation_sent_at
            .write()
            .retain(|p, _| alive.contains(p));
        self.delta_has_more
            .write()
            .retain(|(p, _)| alive.contains(p));
        self.delta_gap.write().retain(|(p, _)| alive.contains(p));
        self.bootstrap_chunk_attempt
            .write()
            .retain(|(p, _), _| alive.contains(p));
        if pruned > 0 {
            debug!("[delta] 清理失联对端陈旧水位 {} 项", pruned);
        }
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
        // P1-5：清理失联对端的陈旧水位 / 看门狗 / in-flight（见方法注释）
        self.prune_stale_delta_state(&conns);
        // v9：本轮只读一次 bootstrap 进度表，供所有 (peer, repo) 复用
        // （旧写法在 peer×repo 双层循环里逐个调用 `bootstrap_list()`，对端多时是
        // O(peers²) 次全表读 + JSON 反序列化，且每次都抢全局 SQLite 连接锁）。
        let bootstrap_rows = self.delta_storage().bootstrap_list().unwrap_or_default();
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
                // v7/v8：bootstrap 判定 —— 协商裁定 BOOTSTRAP，或 v8 拉取侧本地改判：
                // 对端 per-repo 水位（delta_peer_max，来自 OpsBatch server_max_seq）与本地
                // synced_seq 同序列空间，欠账 > range_bulk_threshold_rows 时 delta 硬拉
                // 已无意义（大批量慢 + 风暴），直接走快照通道（实测 38 万欠账被误判 DELTA）。
                let peer_max = {
                    let from_ops = *self
                        .delta_peer_max
                        .read()
                        .get(&(conn.node_id, rt))
                        .unwrap_or(&0);
                    // v9：OpsBatch 还没到过时回落到协商里对端自报的水位（重启后也能立即判欠账）。
                    let from_neg = self
                        .peer_negotiate_state
                        .read()
                        .get(&conn.node_id)
                        .and_then(|v| v.iter().find(|s| s.repo == rt).map(|s| s.max_seq))
                        .unwrap_or(0);
                    from_ops.max(from_neg)
                };
                let synced_seq = self
                    .delta_storage()
                    .get_peer_seq(&conn.node_id.0, rt)
                    .unwrap_or(0)
                    .max(0) as u64;
                let lag = peer_max.saturating_sub(synced_seq);
                // v9：检测到「对端 oplog 已被裁剪、中间段结构性缺失」时，该 repo 必须走快照补齐，
                // 无论 lag 大小（`handle_ops_batch` 在识别到 seq 跳变时置位 delta_gap）。
                let gap_flagged = self.delta_gap.read().contains(&(conn.node_id, rt));
                let lag_over = (peer_max > 0
                    && lag > self.config.range_bulk_threshold_rows.max(1)
                    && self.strategy_for(&conn.node_id, rt) != Some(protocol::STRATEGY_NONE))
                    || gap_flagged;
                // A1：全 repo 统一策略。bootstrap 通道已对四个 repo 打通（清单构建
                // build_repo_manifest_impl、应答取数 load_repo_sync_entries_in_range、
                // 落地 handle_sync_batch 均为 repo 通用），因此不再按 repo 特判：
                // 四个 repo 共用同一套「协商裁定 BOOTSTRAP 或 lag 超阈值 → 走快照」判定。
                let bootstrap_decided = self.strategy_for(&conn.node_id, rt)
                    == Some(protocol::STRATEGY_BOOTSTRAP)
                    || lag_over;
                let use_bootstrap = self.config.bootstrap_enabled
                    && bootstrap_decided
                    && conn.connected_secs() >= self.config.strategy_min_conn_secs;
                // v10(B2)：方向守卫 —— 对端行数不多于本端时，本端的缺口不来自对端的
                // 存量（快照拉来的都是已有行），欠账由 delta 追平即可。否则竣工后
                // 行数反超的部分会持续触发空转 bootstrap（每 60s 一次清单往返），
                // 且空转请求会不断刷新对端视角的 serving，令对端（真正的欠账方）
                // 被让路机制永久卡住（实测 58 竣工后 52 无法恢复拉取）。
                let remote_rows = self
                    .peer_negotiate_state
                    .read()
                    .get(&conn.node_id)
                    .and_then(|v| v.iter().find(|s| s.repo == rt).map(|s| s.row_count))
                    .unwrap_or(0);
                let local_rows = self
                    .local_entry_counts()
                    .get((rt - repo_type::NODE) as usize)
                    .copied()
                    .unwrap_or(0) as u64;
                let use_bootstrap =
                    use_bootstrap && !(remote_rows > 0 && remote_rows <= local_rows);
                if use_bootstrap {
                    // v9：running 判定改为「同 peer 同 repo 且**仍在推进**」（见
                    // bootstrap_running_fresh）：旧实现不含 peer、无超时，一行卡在 Transfer
                    // 就让该 repo 对所有对端的 delta 永久停摆。
                    let running = self.bootstrap_running_fresh(&bootstrap_rows, conn.node_id, rt);
                    if !running {
                        info!(
                            "[negotiate] 执行快照策略：启动 bootstrap peer={} repo={}（协商裁定={}，欠账={}，gap={}）",
                            conn.node_id,
                            rt,
                            self.strategy_for(&conn.node_id, rt)
                                == Some(protocol::STRATEGY_BOOTSTRAP),
                            lag,
                            gap_flagged
                        );
                        let sm = self.clone();
                        let peer = conn.node_id;
                        tokio::spawn(async move { sm.start_bootstrap(peer, rt).await });
                    }
                    // v9：**不再无条件 `continue` 跳过 delta**。旧实现与「bootstrap 卡死」
                    // 构成互锁闭环：lag>1万 → 关 delta → bootstrap 永不完成 → delta 永不恢复
                    // （实测 repo1/2/3 的 lag 25 分钟逐位不变）。现在两通道并行；
                    // 需要退回旧行为可置 federation.bootstrap_blocks_delta=true。
                    if self.config.bootstrap_blocks_delta {
                        continue;
                    }
                }
                // v7：看门狗（暂停期跳过；触发时已清协商）
                if !self.delta_watchdog_ok(conn.node_id, rt, interval, peer_max) {
                    continue;
                }
                // v9：续拉未完成（has_more 发送失败）时不等间隔立即补发，避免游标永久冻结。
                let has_more = self.delta_has_more.read().contains(&(conn.node_id, rt));
                let due = has_more || {
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
        // P1-4：全局并发闸，限制同时在跑的 range handler 数，削平突发帧/IO 风暴。
        let _range_permit = match self.range_gate.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
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
        // P1-4：全局并发闸，限制同时在跑的 range handler 数，削平突发帧/IO 风暴。
        let _range_permit = match self.range_gate.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
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
                // P2-3：深度到顶被**强制降级**为「叶」时，对端并未返回行指纹明细
                //（`resp.entries` 为空），此时 `key_diff(本地, 空)` 会把整个区间判成
                //「本地多」→ 把整片数据幽灵推送给对端（对端再按 upsert 全量落地，双向放大）。
                // 正常数据下钻到不了 max_depth；到达即说明配置过小或数据严重倾斜，
                // 这种区间只记统计、不修复。
                let forced_leaf =
                    !resp.is_leaf && resp.depth >= self.config.range_reconcile_max_depth;
                if forced_leaf && (n_local > 0 || n_remote > 0) {
                    debug!(
                        "[range] 深度到顶强制降级为叶，跳过修复（明细不可信）: repo={} [{}, {}) depth={} 本地多={}",
                        resp.repo,
                        String::from_utf8_lossy(&resp.lo),
                        String::from_utf8_lossy(&resp.hi),
                        resp.depth,
                        n_local
                    );
                }
                // F3：非诊断模式下真正执行修复
                if !self.config.range_reconcile_diagnostic_only
                    && (n_local > 0 || n_remote > 0)
                    && !forced_leaf
                {
                    // v9：同一 (peer, repo, lo, hi) 的修复在 `range_repair_min_interval_secs`
                    // 内只做一次。旧实现是 fire-and-forget、无去重：同一批差异会被每个 tick
                    // 重新推拉一遍（叠加「入站条目被 version==0 早退丢弃」时就是纯无效流量）。
                    let dedupe_key = (conn.node_id, resp.repo, resp.lo.clone(), resp.hi.clone());
                    let skip = {
                        let dedupe = self.range_repair_recent.read();
                        match dedupe.get(&dedupe_key) {
                            Some(t) => {
                                t.elapsed().as_secs()
                                    < self.config.range_repair_min_interval_secs.max(1)
                            }
                            None => false,
                        }
                    };
                    if skip {
                        debug!(
                            "[range] 同一叶区间修复冷却中，跳过本轮: repo={} [{}, {})",
                            resp.repo,
                            String::from_utf8_lossy(&resp.lo),
                            String::from_utf8_lossy(&resp.hi)
                        );
                    } else {
                        if self.range_repair_recent.read().len() > 4096 {
                            let cutoff = self.config.range_repair_min_interval_secs.max(1) * 2;
                            self.range_repair_recent
                                .write()
                                .retain(|_, t| t.elapsed().as_secs() < cutoff);
                        }
                        self.range_repair_recent
                            .write()
                            .insert(dedupe_key, Instant::now());
                        self.range_repair_triggers
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        // v8：4 repo 通用的 Pull/Push2 修复通道。不再依赖「对端 oplog 必有对应 op」
                        // 的假设——入站/bootstrap 来源的数据在 oplog 里没有 op，delta 拉不到，
                        // 那是此前 3 个非 NODE repo 修复死路的根因。
                        if conn.supports_range_v2() {
                            // 对端多 → 把缺失 key 列表发给对端，对端按 key 加载完整条目回推
                            if n_remote > 0 {
                                self.send_range_pull(&conn, resp.repo, &remote_only).await;
                            }
                            // 本地多 → 按 key 加载本地完整条目直接推给对端
                            if n_local > 0 {
                                self.send_range_push(&conn, resp.repo, &local_only).await;
                            }
                        } else if n_remote > 0 {
                            // < v8 对端：回落 delta 委托（仅对 oplog 窗口内、对端本地 origin 的数据有效）
                            self.trigger_delta_sync(conn.node_id, resp.repo).await;
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

    /// v8：发送 Range 反熵按键拉取请求（「对端多」修复第一步）。
    /// key 数与估算字节双重上限；超出部分留给下一轮 range 对账（对账幂等，不会丢）。
    async fn send_range_pull(&self, conn: &PeerConn, repo: u8, keys: &[Vec<u8>]) {
        let mut take: Vec<Vec<u8>> = Vec::with_capacity(keys.len().min(64));
        let mut bytes = 0usize;
        for k in keys {
            let kb = k.len() + 8;
            if take.len() >= range_reconcile::MAX_LEAF_ENTRIES
                || bytes + kb > delta::DELTA_BATCH_MAX_BYTES
            {
                debug!(
                    "[range] 拉取请求截断: repo={}, 取 {} / 共 {} 个 key（本轮上限）",
                    repo,
                    take.len(),
                    keys.len()
                );
                break;
            }
            bytes += kb;
            take.push(k.clone());
        }
        if take.is_empty() {
            return;
        }
        let msg = crate::federation::protocol::RangeReconcilePullMessage { repo, keys: take };
        match conn
            .send_message(
                crate::federation::protocol::MessageType::RangeReconcilePull,
                &msg,
            )
            .await
        {
            Ok(()) => self.metrics.record_message_sent(),
            Err(e) => warn!("[range] 发送按键拉取失败 to={}: {}", conn.node_id, e),
        }
    }

    /// v8：按 key 加载本地完整条目并推送给对端（「本地多」修复）。
    async fn send_range_push(&self, conn: &PeerConn, repo: u8, keys: &[Vec<u8>]) {
        let entries = match self.delta_storage().load_repo_entries_by_keys(repo, keys) {
            Ok(e) => e,
            Err(e) => {
                warn!("[range] 按 key 加载条目失败 repo={}: {}", repo, e);
                return;
            }
        };
        if entries.is_empty() {
            debug!(
                "[range] 本地多 {} 个 key，但按 key 加载到 0 条 repo={}",
                keys.len(),
                repo
            );
            return;
        }
        let count = entries.len();
        let msg = crate::federation::protocol::RangeReconcilePush2Message { repo, entries };
        match conn
            .send_message(
                crate::federation::protocol::MessageType::RangeReconcilePush2,
                &msg,
            )
            .await
        {
            Ok(()) => {
                self.metrics.record_message_sent();
                debug!(
                    "[range] 推送本地多数据 to={} repo={}: {} 条",
                    conn.node_id, repo, count
                );
            }
            Err(e) => warn!("[range] 推送本地多数据失败 to={}: {}", conn.node_id, e),
        }
    }

    /// v8：处理对端的按键拉取请求（应答方）——按 key 加载完整条目回 Push2。
    pub async fn handle_range_reconcile_pull(
        self: Arc<Self>,
        conn: Arc<PeerConn>,
        msg: crate::federation::protocol::RangeReconcilePullMessage,
    ) {
        if !self.config.range_reconcile_enabled {
            return;
        }
        // P1-4：全局并发闸，限制同时在跑的 range handler 数，削平突发帧/IO 风暴。
        let _range_permit = match self.range_gate.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
        if msg.repo < repo_type::NODE || msg.repo > repo_type::TRACKER || msg.keys.is_empty() {
            return;
        }
        let repo = msg.repo;
        let keys = msg.keys;
        let sm = self.clone();
        let peer = conn.node_id;
        let entries = tokio::task::spawn_blocking(move || {
            sm.delta_storage().load_repo_entries_by_keys(repo, &keys)
        })
        .await;
        let entries = match entries {
            Ok(Ok(e)) => e,
            Ok(Err(e)) => {
                warn!("[range] 应答按键拉取加载失败 repo={}: {}", repo, e);
                return;
            }
            Err(e) => {
                warn!("[range] 应答按键拉取任务失败: {}", e);
                return;
            }
        };
        if entries.is_empty() {
            debug!("[range] 按键拉取无命中: peer={}, repo={}", peer, repo);
            return;
        }
        let count = entries.len();
        let resp = crate::federation::protocol::RangeReconcilePush2Message { repo, entries };
        match conn
            .send_message(
                crate::federation::protocol::MessageType::RangeReconcilePush2,
                &resp,
            )
            .await
        {
            Ok(()) => {
                self.metrics.record_message_sent();
                debug!(
                    "[range] 应答按键拉取 to={} repo={}: {} 条",
                    peer, repo, count
                );
            }
            Err(e) => warn!("[range] 应答按键拉取回推失败 to={}: {}", peer, e),
        }
    }

    /// v8：处理 Range 反熵通用推送（接收方）——完整 SyncEntry 走 handle_sync_batch 幂等 apply。
    /// 入站路径：不写 oplog、不提交 gossip（「入站不写回」不变量）。
    pub async fn handle_range_reconcile_push2(
        self: Arc<Self>,
        conn: Arc<PeerConn>,
        msg: crate::federation::protocol::RangeReconcilePush2Message,
    ) {
        if !self.config.range_reconcile_enabled {
            return;
        }
        // P1-4：全局并发闸，限制同时在跑的 range handler 数，削平突发帧/IO 风暴。
        let _range_permit = match self.range_gate.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
        if msg.repo < repo_type::NODE || msg.repo > repo_type::TRACKER || msg.entries.is_empty() {
            return;
        }
        let repo = msg.repo;
        let entries = msg.entries;
        let count = entries.len();
        let sm = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            sm.handle_sync_batch(repo, &entries);
        })
        .await;
        match result {
            Ok(()) => {
                debug!(
                    "[range] 收到通用推送 from={} repo={}: {} 条，已 apply",
                    conn.node_id, repo, count
                );
                self.metrics.record_sync_entries(count as u64);
            }
            Err(e) => {
                warn!("[range] 处理通用推送失败 from={}: {}", conn.node_id, e);
            }
        }
    }

    /// P1-4：单轮 range-based 反熵（请求方驱动，由 TaskScheduler 周期调用）。
    ///
    /// v7：统一 range 通道 —— 4 个 repo 全部参与同一套对账逻辑，per-repo 周期节流。
    /// B1：周期**全部来自配置** `range_reconcile_interval_secs`
    /// （顺序 NODE/PEER/INFOHASH/TRACKER），不再使用写死常量表。
    /// 每个 repo 抽样 `range_reconcile_sample_ranges + 1` 个分界 key 形成若干区间
    /// （首尾接 ±∞），对每个区间发一个 depth=0 的 RangeReconcileRequest。
    /// 默认 `range_reconcile_enabled=false` 时为 no-op（行为与改造前一致）。
    pub async fn range_reconcile_tick(self: Arc<Self>) {
        if !self.config.range_reconcile_enabled {
            return;
        }
        // v10(F2b)：bootstrap 传输期让路 —— 快照是全量 IO（实测 NODE 128 块 × 2 万行），
        // 与反熵区间扫描争同一 SQLite 读池，会把 tick 从毫秒级拖到 300s 超时
        // （2026-09-27 实测：快照期连续两个 tick 飞满 300s 被杀）。铁律 1（低优先级
        // 可抢占）：快照在途时反熵让路，竣工/停滞判定失效后自动恢复。
        if self.bootstrap_transfer_active() {
            debug!("[range] bootstrap 传输进行中，本轮反熵让路");
            return;
        }
        // v9：单轮最多处理 `range_repos_per_tick` 个 repo（默认 1）。
        // 旧实现一轮把 4 个 repo 全部串行跑完（各 160+ 个区间、合计约 505 帧），
        // 实测单次占槽 166~226s，把同分类的 delta / bootstrap / 心跳一起饿死。
        //
        // ⚠️ 预算必须按「**最久未处理优先**」发放，不能按 repo 编号顺序：NODE 的周期只有 30s
        // 而本任务每 60s 触发一次，按编号顺序取 N 个会让 NODE 每轮都吃掉全部预算，
        // PEER/INFOHASH/TRACKER 永远轮不到（它们自 v8 起只有 Range 这一条兜底通道）。
        let budget = self.config.range_repos_per_tick.max(1) as usize;
        let mut due_repos: Vec<(u8, u64)> = Vec::with_capacity(4);
        for repo in repo_type::NODE..=repo_type::TRACKER {
            let idx = (repo - repo_type::NODE) as usize;
            let interval = self
                .config
                .range_reconcile_interval_secs
                .get(idx)
                .copied()
                .unwrap_or(60)
                .max(1);
            let last_age = {
                let last = self.range_tick_last.read();
                last.get(&repo).map(|t| t.elapsed().as_secs())
            };
            // 从未处理过的 repo 视为最紧急（u64::MAX）
            let age = last_age.unwrap_or(u64::MAX);
            if age >= interval {
                due_repos.push((repo, age));
            }
        }
        // 最久未处理优先（age 越大越紧急；从未处理过记 u64::MAX）
        due_repos.sort_by_key(|(_, age)| std::cmp::Reverse(*age));
        for (repo, _) in due_repos.into_iter().take(budget) {
            self.range_tick_last.write().insert(repo, Instant::now());
            self.clone().range_reconcile_tick_repo(repo).await;
        }
    }

    /// v9：为某 repo 轮转挑一条「尚未到本轮周期」的连接（多对端时逐个覆盖，而非随机撞一条）。
    ///
    /// 旧实现按 `now.as_nanos() % conns.len()` 随机挑一条，且 `range_tick_last` 只按 repo 记时间
    /// ⇒ 多对端（`target_neighbors` 默认 8）时每个 repo 每轮只对账一条连接，
    /// 其余对端要靠运气被抽中，覆盖率被摊薄 N 倍。
    fn pick_range_conn(
        &self,
        repo: u8,
        interval_secs: u64,
        conns: &[Arc<PeerConn>],
    ) -> Option<Arc<PeerConn>> {
        if conns.is_empty() {
            return None;
        }
        let fresh = |t: Option<&Instant>| -> bool {
            t.map(|x| x.elapsed().as_secs() < interval_secs)
                .unwrap_or(false)
        };
        let mut rr = self.range_rr.write();
        let start = *rr.get(&repo).unwrap_or(&0) % conns.len();
        for offset in 0..conns.len() {
            let i = (start + offset) % conns.len();
            let cand = &conns[i];
            if !cand.supports_range_reconcile() {
                continue;
            }
            let seen = fresh(self.range_peer_tick_last.read().get(&(cand.node_id, repo)));
            if !seen {
                rr.insert(repo, (i + 1) % conns.len());
                return Some(cand.clone());
            }
        }
        // 全部对端本轮都做过了：轮转到下一条（下个周期再对账）。
        // 兜底分支同样只返回**支持 range** 的连接，否则调用方直接 return、白耗本轮预算。
        let supported: Vec<&Arc<PeerConn>> = conns
            .iter()
            .filter(|c| c.supports_range_reconcile())
            .collect();
        if supported.is_empty() {
            return None;
        }
        let pick = supported[start % supported.len()].clone();
        rr.insert(repo, (start + 1) % conns.len());
        Some(pick)
    }

    /// v10(F2b)：是否有 bootstrap 传输仍在推进（反熵让路判定）。
    /// 任一 (peer,repo) 的进度非 Done 且 `bootstrap_stall_secs` 内仍在更新 → 传输活跃。
    fn bootstrap_transfer_active(&self) -> bool {
        let rows = self.delta_storage().bootstrap_list().unwrap_or_default();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let fresh_ms = self.config.bootstrap_stall_secs.max(1) as i64 * 1000;
        rows.iter().any(|p| {
            !matches!(p.phase, bootstrap::BootstrapPhase::Done) && now_ms - p.updated_ms <= fresh_ms
        })
    }

    /// v7：单 repo 的 range 反熵抽样对账（原 NODE 专属逻辑通用化）。
    async fn range_reconcile_tick_repo(self: Arc<Self>, repo: u8) {
        let conns = self.sessions.all_connections();
        if conns.is_empty() {
            return;
        }
        let idx = (repo - repo_type::NODE) as usize;
        let interval_secs = self
            .config
            .range_reconcile_interval_secs
            .get(idx)
            .copied()
            .unwrap_or(60)
            .max(1);
        // 定期清理过期的「本轮已对账」标记（防多对端 × 4 repo 的表无界增长）
        if self.range_peer_tick_last.read().len() > conns.len() * 4 + 8 {
            let cutoff = interval_secs.saturating_mul(2);
            self.range_peer_tick_last
                .write()
                .retain(|_, t| t.elapsed().as_secs() < cutoff);
        }
        // v10(F2)：断点优先 —— 该 repo 有未完成的抽样进度（对端连接仍存活）则直接续跑
        // 同一对端同一序列，不轮转挑新连接、不重新抽样；预算与游标推进见 `range_send_batch`。
        if let Some((resume_conn, mut prog)) = self.range_resume_candidate(repo, &conns) {
            self.range_send_batch(&resume_conn, repo, &mut prog).await;
            return;
        }
        let conn = match self.pick_range_conn(repo, interval_secs, &conns) {
            Some(c) => c,
            None => return,
        };
        if !conn.supports_range_reconcile() {
            debug!(
                "[range] 对端 {} 不支持 range 反熵（version<{}），跳过",
                conn.node_id,
                range_reconcile::RANGE_RECONCILE_PROTOCOL_VERSION
            );
            return;
        }
        // v9：本轮已对账的连接：本 repo 周期内不再重复抽到它（多对端公平轮转）。
        self.range_peer_tick_last
            .write()
            .insert((conn.node_id, repo), Instant::now());
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
        let mut prog = RangeSampleProgress {
            bounds,
            leaf_rows,
            next: 0,
            updated: Instant::now(),
        };
        // v10(F2)：本 tick 只发送预算内的区间；未完成时 `range_send_batch` 内部落断点，
        // 下一 tick 由 `range_resume_candidate` 续跑。
        self.range_send_batch(&conn, repo, &mut prog).await;
    }

    /// v10(F2)：取该 repo 仍需续跑的抽样断点（进度存在、对端连接仍存活且支持 range）。
    fn range_resume_candidate(
        &self,
        repo: u8,
        conns: &[Arc<PeerConn>],
    ) -> Option<(Arc<PeerConn>, RangeSampleProgress)> {
        let mut m = self.range_progress.write();
        // 停滞清理：长时间未推进（对端持续不可达/连接失效）弃置，下一轮重新抽样。
        m.retain(|_, p| p.updated.elapsed().as_secs() < RANGE_SAMPLE_STALL_SECS);
        for ((peer, r), prog) in m.iter() {
            if *r != repo {
                continue;
            }
            if let Some(c) = conns.iter().find(|c| c.node_id == *peer) {
                if !c.supports_range_reconcile() {
                    continue;
                }
                return Some((c.clone(), prog.clone()));
            }
        }
        None
    }

    /// v10(F2)：发送当前抽样进度下的一个预算批次（`range_ranges_per_tick` 个区间）并推进游标。
    ///
    /// 旧实现一个 tick 内同步发完全部 161 个区间（百万行库上单轮 >300s），被 TaskScheduler
    /// 超时杀掉后进度作废、下一轮从头再来，兜底对账永远完不成一轮（2026-09-27 实测 52/58）。
    /// 现在每 tick 只发预算内的区间：被杀不丢进度（游标在 SyncManager）、单 tick 秒级返回、
    /// Federation 分类槽快速让位。
    async fn range_send_batch(
        &self,
        conn: &Arc<PeerConn>,
        repo: u8,
        prog: &mut RangeSampleProgress,
    ) {
        let budget = self.config.range_ranges_per_tick.max(1) as usize;
        let total = prog.bounds.len().saturating_sub(1);
        let end = prog.next.saturating_add(budget).min(total);
        let storage = self.delta_storage();
        let mut sent = 0u32;
        let mut advanced = prog.next;
        for i in prog.next..end {
            let lo = prog.bounds[i].clone();
            let hi = prog.bounds[i + 1].clone();
            let rows = match storage.load_repo_key_hashes_in_range(
                repo,
                Self::range_bound(&lo),
                Self::range_bound(&hi),
                prog.leaf_rows as usize + 1,
            ) {
                Ok(r) => r,
                Err(_) => {
                    // 本地读失败：跳过该区间（与旧行为一致），游标照常推进。
                    advanced = i + 1;
                    continue;
                }
            };
            let digest = range_reconcile::range_digest(&rows);
            let req = RangeReconcileRequestMessage {
                repo,
                lo,
                hi,
                digest,
                leaf_rows: prog.leaf_rows,
                depth: 0,
            };
            if let Err(e) = conn
                .send_message(MessageType::RangeReconcileRequest, &req)
                .await
            {
                warn!(
                    "[range] 发送 RangeReconcileRequest 失败 to={}（区间 {}/{}，进度保留待续跑）: {}",
                    conn.node_id,
                    i + 1,
                    total,
                    e
                );
                break;
            }
            self.metrics.record_message_sent();
            advanced = i + 1;
            sent += 1;
        }
        prog.next = advanced;
        prog.updated = Instant::now();

        let done = prog.next >= total;
        if done {
            self.range_progress.write().remove(&(conn.node_id, repo));
        } else {
            self.range_progress
                .write()
                .insert((conn.node_id, repo), prog.clone());
        }
        // F3：叶级明细已降为 debug，按「批次 debug + 轮次完成 info」两级输出，
        // 既保留累计对账统计，又避免每 tick 刷一整轮的长日志。
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
        let stats = format!(
            "累计 叶级对账={} 本地多={} 对端多={} 触发修复={} 模式={}",
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
        if done {
            info!(
                "[range] repo={} 抽样对账一轮发送完成 to={}（{} 个区间）| {}",
                repo, conn.node_id, total, stats
            );
        } else {
            debug!(
                "[range] repo={} 本 tick 发送 {} 区间（断点 {}/{}，to={}）| {}",
                repo, sent, prog.next, total, conn.node_id, stats
            );
        }
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
        // v10(A)：标记「正在响应对方的 bootstrap 请求」—— 双向引导冲突让路的信号源。
        self.mark_bootstrap_serving(&conn.node_id, req.repo);
        let key = (conn.node_id, req.repo);
        // v9：① 缓存复用 —— 重复到达的清单请求直接回缓存清单，不再全表重排。
        // v10(B2)：**缓存保活** —— 旧实现 lease 过期即重建（全表扫 38s），而请求方拉完
        // 全量需几十分钟 ≫ lease，每次重拉都触发重建 → 缓存边界漂移 → 断点归零死循环。
        // 现在缓存**一旦建立就一直服务**：请求方持有的清单正是这份，回它永远自洽；
        // 表增长落在末块 [lo,+∞) 与竣工后的 delta/Range 兜底。仅缓存缺失（重启）才重建。
        // 注意：读锁守卫必须在语句内释放（不得跨 await），否则 handler future 不是 Send。
        let cached = self.bootstrap_manifests.read().get(&key).cloned();
        if let Some(m) = cached {
            let resp = BootstrapManifestResponseMessage { manifest: m };
            if conn
                .send_message(MessageType::BootstrapManifestResponse, &resp)
                .await
                .is_ok()
            {
                self.metrics.record_message_sent();
            }
            return;
        }
        // v9：② 单飞 —— 已有一次重建在途时不再并发铺开全表扫描；
        // 有旧缓存就回旧缓存（边界仍然自洽），没有就回空清单让请求方稍后重试。
        let rebuild_lease = Duration::from_secs(self.config.bootstrap_rebuild_lease_secs.max(1));
        let rebuilding_age = {
            let g = self.bootstrap_rebuild_at.read();
            g.get(&key).map(|t| t.elapsed())
        };
        if let Some(age) = rebuilding_age {
            if age < rebuild_lease {
                warn!(
                    "[bootstrap] 清单重建进行中（已 {}s），本轮回退旧缓存/空清单: peer={} repo={}",
                    age.as_secs(),
                    conn.node_id,
                    req.repo
                );
                let manifest = self
                    .bootstrap_manifests
                    .read()
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(|| bootstrap::BootstrapManifest {
                        repo: req.repo,
                        ..Default::default()
                    });
                let resp = BootstrapManifestResponseMessage { manifest };
                let _ = conn
                    .send_message(MessageType::BootstrapManifestResponse, &resp)
                    .await;
                return;
            }
        }
        self.bootstrap_rebuild_at
            .write()
            .insert(key, Instant::now());
        let storage = self.delta_storage();
        // v9：w0 必须取**该 repo** 的水位。旧实现用全局 `oplog_max_seq()`，而 delta 断点是
        // per-repo 空间 ⇒ 稀疏 repo（如 TRACKER）的游标会被一次性抬到全局水位，
        // 中间该 repo 的历史 op 被永久跳过。
        let w0 = storage.oplog_max_seq_for_repo(req.repo).unwrap_or(0).max(0) as u64;
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
                self.bootstrap_rebuild_at.write().remove(&key);
                return;
            }
        };
        info!(
            "[bootstrap] 响应清单请求 from={}: repo={} 总行={}, 块数={}, w0={}",
            conn.node_id,
            req.repo,
            manifest.total_rows,
            manifest.chunks.len(),
            w0
        );
        self.bootstrap_manifests
            .write()
            .insert(key, manifest.clone());
        self.bootstrap_manifest_at
            .write()
            .insert(key, Instant::now());
        self.bootstrap_rebuild_at.write().remove(&key);
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

    /// v9：回一个**显式空块**作为 NAK。
    ///
    /// 旧实现在「重建进行中 / index 越界 / DB 读失败」三条路径上直接 `return` 不回帧，
    /// 请求方因此永远收不到响应、`done_chunks` 恒为 0 且无任何失败计数可触发自愈
    /// （实测 `phase=transfer, done=0` 卡死两天）。空块的 `entries.len() == 0`
    /// 会命中接收方 `verify_transport(expected>0, 0) == false`，从而进入可计数、可升级的失败路径。
    async fn send_bootstrap_nak(&self, conn: &PeerConn, repo: u8, index: u32, reason: &str) {
        debug!(
            "[bootstrap] 回显式 NAK: peer={}, repo={}, index={}, reason={}",
            conn.node_id, repo, index, reason
        );
        let resp = BootstrapChunkResponseMessage {
            repo,
            index,
            entries: Vec::new(),
            hash: [0u8; 32],
            is_last: false,
        };
        if let Err(e) = conn
            .send_message(MessageType::BootstrapChunkResponse, &resp)
            .await
        {
            warn!("[bootstrap] 发送 NAK 失败 to={}: {}", conn.node_id, e);
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
        // v10(A)：标记「正在响应对方的 bootstrap 请求」—— 双向引导冲突让路的信号源。
        self.bootstrap_serving_at
            .write()
            .insert((conn.node_id, req.repo), Instant::now());
        if req.repo < repo_type::NODE || req.repo > repo_type::TRACKER {
            return;
        }
        let key = (conn.node_id, req.repo);
        // 读锁守卫不得跨 await：先 clone 出缓存清单再 match。
        let cached_manifest = self.bootstrap_manifests.read().get(&key).cloned();
        let manifest = match cached_manifest {
            Some(m) => m,
            None => {
                // F6/v9：应答方清单缓存是纯内存（重启即空），而请求方持本地持久化的旧清单
                // 直接要块 —— 若按 index 硬服务，两侧块边界不同会让请求方把**不同区间**的数据
                // 当成第 i 块落地，造成静默空洞（实测双端 74 块 vs 82 块、done_chunks 照常 +1）。
                // 因此这里**只回清单、令请求方以同一边界重新开始**，从根上消除 index↔区间错位。
                // 重建中（租约内）回显式空 NAK，让请求方走可计数、可升级的失败路径。
                let lease = Duration::from_secs(self.config.bootstrap_rebuild_lease_secs.max(1));
                let rebuilding_age = {
                    let g = self.bootstrap_rebuild_at.read();
                    g.get(&key).map(|t| t.elapsed())
                };
                if let Some(age) = rebuilding_age {
                    if age < lease {
                        self.send_bootstrap_nak(&conn, req.repo, req.index, "manifest rebuilding")
                            .await;
                        return;
                    }
                }
                self.bootstrap_rebuild_at
                    .write()
                    .insert(key, Instant::now());
                let storage = self.delta_storage();
                let w0 = storage.oplog_max_seq_for_repo(req.repo).unwrap_or(0).max(0) as u64;
                let version = w0.wrapping_add(1) as u32;
                match bootstrap::build_repo_manifest_impl(
                    &storage,
                    req.repo,
                    self.config.bootstrap_chunk_rows,
                    w0,
                    version,
                ) {
                    Ok(m) => {
                        info!(
                            "[bootstrap] 清单缓存缺失，现场重建并要求请求方重新对齐: peer={}, repo={}, 块数={}, w0={}, 丢弃其 index={}",
                            conn.node_id,
                            req.repo,
                            m.chunks.len(),
                            w0,
                            req.index
                        );
                        self.bootstrap_manifests.write().insert(key, m.clone());
                        self.bootstrap_manifest_at
                            .write()
                            .insert(key, Instant::now());
                        self.bootstrap_rebuild_at.write().remove(&key);
                        let resp = BootstrapManifestResponseMessage { manifest: m };
                        if conn
                            .send_message(MessageType::BootstrapManifestResponse, &resp)
                            .await
                            .is_ok()
                        {
                            self.metrics.record_message_sent();
                        }
                        return;
                    }
                    Err(e) => {
                        self.bootstrap_rebuild_at.write().remove(&key);
                        warn!(
                            "[bootstrap] 收到分块请求且清单重建失败 peer={}, repo={}: {}",
                            conn.node_id, req.repo, e
                        );
                        self.send_bootstrap_nak(
                            &conn,
                            req.repo,
                            req.index,
                            "manifest rebuild failed",
                        )
                        .await;
                        return;
                    }
                }
            }
        };
        let chunk = match manifest.chunks.iter().find(|c| c.index == req.index) {
            Some(c) => c.clone(),
            None => {
                warn!(
                    "[bootstrap] 分块 index={} 越界（共 {} 块）→ 回 NAK 并作废清单缓存迫使重新对齐",
                    req.index,
                    manifest.chunks.len()
                );
                // 缓存与请求方已失配：作废缓存，使下一次请求走「重建 + 重新对齐」路径。
                self.bootstrap_manifests.write().remove(&key);
                self.bootstrap_manifest_at.write().remove(&key);
                self.send_bootstrap_nak(&conn, req.repo, req.index, "index out of range")
                    .await;
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
                self.send_bootstrap_nak(&conn, req.repo, req.index, "load chunk hash failed")
                    .await;
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
                self.send_bootstrap_nak(&conn, req.repo, req.index, "load chunk entries failed")
                    .await;
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
        // A3：全 repo 统一 —— 只校验 repo 取值合法（四个 repo 等价），不再只放行 NODE。
        if mf.repo < repo_type::NODE || mf.repo > repo_type::TRACKER {
            debug!(
                "[bootstrap] 收到非法 repo={} 的清单，忽略: from={}",
                mf.repo, conn.node_id
            );
            return;
        }
        // v9：**空清单是「重建在途」的回退帧，不是真清单** —— 必须原样丢弃，
        // 绝不能落库（旧写法会把它当清单 `bootstrap_save(..., Some(&mf))` 覆盖本地有效清单，
        // 把断点续传状态直接摧毁）。因此空清单要在任何写入之前拦掉。
        if mf.chunks.is_empty() {
            warn!(
                "[bootstrap] 收到空清单（对端正在重建/无数据），忽略且不改本地状态: peer={} repo={}",
                conn.node_id, mf.repo
            );
            return;
        }
        // v10(B2)：断点继承 —— version 相同（同一快照）直接继承 done_chunks；
        // version 不同（w0 漂移，对端 oplog 持续写则必然）时用 **key 游标** 在新清单
        // 中定位续传起点：块边界随活表写入漂移，边界比对必然失配 → 归零循环
        // （实测 52/58 NODE 快照「续传起点=0」反复），key 游标不随边界失效。
        // 已传块与新 w0 之间的值漂移由竣工后的 delta 追尾 + Range 反熵兜底。
        let loaded = self
            .delta_storage()
            .bootstrap_load(&conn.node_id.0, mf.repo)
            .ok()
            .flatten();
        let (same_done, resume_src) = match loaded {
            Some((p, _)) if p.peer.as_slice() == conn.node_id.0 && p.version == mf.version => {
                (p.done_chunks.min(mf.chunks.len() as u64), "version")
            }
            Some((p, _)) if p.peer.as_slice() == conn.node_id.0 => {
                let idx = bootstrap::locate_resume_index(&mf.chunks, p.last_key.as_deref());
                (idx.min(mf.chunks.len() as u64), "last_key")
            }
            _ => (0, "none"),
        };
        let now = chrono::Utc::now().timestamp_millis();
        let mut progress = bootstrap::BootstrapProgress::new(mf.repo, conn.node_id.0.to_vec(), now);
        progress.phase = bootstrap::BootstrapPhase::Transfer;
        progress.version = mf.version;
        progress.w0_seq = mf.w0_seq;
        progress.total_chunks = mf.chunks.len() as u64;
        progress.done_chunks = same_done;
        if same_done > 0 {
            info!(
                "[bootstrap] 断点继承（来源 {}）done={}/{}: peer={} repo={}",
                resume_src,
                same_done,
                mf.chunks.len(),
                conn.node_id,
                mf.repo
            );
        }
        // v10(B2)：继承判定已覆盖全部块（对端全量此前已落地）→ 直接竣工，
        // 不再发越界块请求空转；竣工推进游标并切 delta 追尾。
        if !mf.chunks.is_empty() && same_done >= mf.chunks.len() as u64 {
            info!(
                "[bootstrap] 继承判定快照已全部落地，直接竣工: peer={} repo={}",
                conn.node_id, mf.repo
            );
            let _ = self.delta_storage().bootstrap_save(&progress, Some(&mf));
            self.finish_bootstrap(&conn, mf.repo, mf.w0_seq).await;
            return;
        }
        // v9：**不再在此处 `set_peer_seq`**。旧实现在一个字节都没落地时就把该 (peer,repo) 的
        // per-repo 游标抬到 w0（`set_peer_seq` 是 MAX 语义、只进不退），于是「数据未传、
        // 水位已声明」—— lag 归零、看板显示已同步，且无法回退。现在改为竣工时
        // （`finish_bootstrap`）才推进游标。
        if let Err(e) = self.delta_storage().bootstrap_save(&progress, Some(&mf)) {
            warn!("[bootstrap] 保存进度失败: {}", e);
        }
        // v9：换清单即清空该 (peer,repo) 的分块尝试计数（新一轮重新计数）
        self.bootstrap_chunk_attempt
            .write()
            .remove(&(conn.node_id, mf.repo));
        info!(
            "[bootstrap] 收到清单 from={}: 总行={}, 块数={}, w0={}, 续传起点={}",
            conn.node_id,
            mf.total_rows,
            mf.chunks.len(),
            mf.w0_seq,
            progress.done_chunks
        );
        let start_index = progress.done_chunks as u32;
        self.request_bootstrap_chunk(&conn, mf.repo, start_index, &mf)
            .await;
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
        // v9：按 (peer, repo) 读取进度 —— 旧实现只按 repo 读，多对端时会把 B 的块响应
        // 记到 A 的进度上（进度表主键已在 v9 改为 (peer,repo)）。
        let peer_key = conn.node_id.0;
        let (mut progress, mf) = match self.delta_storage().bootstrap_load(&peer_key, resp.repo) {
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
        if progress.peer.as_slice() != peer_key {
            warn!(
                "[bootstrap] 块响应来源与进度归属不一致，忽略: resp_from={} progress_peer={}",
                conn.node_id,
                progress.peer.len()
            );
            return;
        }
        let chunk = match mf.chunks.iter().find(|c| c.index == resp.index) {
            Some(c) => c.clone(),
            None => return,
        };
        // v9：收到响应（含 NAK）即清掉「无响应」计数 —— 该计数器只用于识别**完全无回帧**的
        // 传输/连接故障，不应把「对端明确回 NAK」也算进去（NAK 走 verify_fails 自愈路径）。
        {
            let mut m = self.bootstrap_chunk_attempt.write();
            if let Some(e) = m.get_mut(&(conn.node_id, resp.repo)) {
                if e.0 == resp.index {
                    m.remove(&(conn.node_id, resp.repo));
                }
            }
        }
        // ④ 批量 upsert（A4：走全 repo 通用 dispatch handle_sync_batch，按 resp.repo 分派到
        // apply_node/peer/infohash/tracker_sync，严禁逐条 INSERT，也严禁硬编码走 NODE 落地）
        if !resp.entries.is_empty() {
            self.handle_sync_batch(resp.repo, &resp.entries);
        }
        // ⑥ 校验（P0-4 语义修正）：只做「传输完整性」校验 —— 校验**对端发来的这一批条目**
        // 是否完整到达，不再重算本地 [lo,hi) 区间摘要与清单 hash 比对。
        //
        // 旧语义是「一致性」校验，必然恒失配：只要接收方在该 key 区间内有**任何对端没有的
        // 行**（双方各自独立爬取产生的 ~2% 差异，全域均匀分布），或对端在传输期有写入，
        // 每个块都判失败。后果是 F7 的「连续 3 次失败重拉清单」陷入死循环 —— 重拉回来的
        // 清单仍是对端 DB，接收方的多余行还在，永远对不上。
        // 一致性校验应交给 range 反熵（v8 下唯一兜底通道）负责，bootstrap 只负责搬数据。
        let ok = bootstrap::verify_transport(chunk.rows, resp.entries.len());
        // v9：失败（含对端显式 NAK/空块）时**重发同一 index**，绝不跳到下一块 ——
        // 旧实现失败后仍执行 `request(index+1)`，等于把该区间的数据静默跳过（永久空洞）。
        let mut retry_same = false;
        if ok {
            self.bootstrap_verify_fails
                .write()
                .remove(&(conn.node_id, resp.repo));
            progress.done_chunks = (resp.index as u64 + 1).max(progress.done_chunks);
            // v10(B2)：记录 key 游标 —— 重拉清单后按此定位续传起点（活表边界漂移免疫）。
            progress.last_key = Some(chunk.hi.clone());
            progress.bytes += resp
                .entries
                .iter()
                .map(|e| (e.key.len() + e.payload.len() + 16) as u64)
                .sum::<u64>();
            progress.phase = bootstrap::BootstrapPhase::Transfer;
        } else {
            // 传输期漂移或丢包：不改 done_chunks，重发同块或升级为重拉清单
            warn!(
                "[bootstrap] 块 {} 传输校验失败（声明 {} 行 / 实收 {} 行），保持进度 done={}",
                resp.index,
                chunk.rows,
                resp.entries.len(),
                progress.done_chunks
            );
            // F7/v9: 连续 N 次失败 → 判定清单漂移/边界失配，重拉清单重置进度重拉。
            // 块数据按 upsert 落库（幂等），重复拉取无害。
            let fails = self
                .bootstrap_verify_fails
                .read()
                .get(&(conn.node_id, resp.repo))
                .copied()
                .unwrap_or(0)
                + 1;
            if fails >= self.config.bootstrap_chunk_max_attempts.max(1) {
                self.bootstrap_verify_fails
                    .write()
                    .remove(&(conn.node_id, resp.repo));
                warn!(
                    "[bootstrap] 连续 {} 次分块失败，判定清单漂移/边界失配，重拉清单: repo={}, peer={}",
                    fails, resp.repo, conn.node_id
                );
                self.clone().start_bootstrap(conn.node_id, resp.repo).await;
            } else {
                self.bootstrap_verify_fails
                    .write()
                    .insert((conn.node_id, resp.repo), fails);
                retry_same = true;
            }
        }
        progress.updated_ms = chrono::Utc::now().timestamp_millis();
        let _ = self.delta_storage().bootstrap_save(&progress, None);

        let last = resp.is_last || (resp.index as usize + 1) >= mf.chunks.len();
        if ok {
            if last {
                self.finish_bootstrap(&conn, resp.repo, mf.w0_seq).await;
            } else {
                self.request_bootstrap_chunk(&conn, resp.repo, resp.index + 1, &mf)
                    .await;
            }
        } else if retry_same {
            self.request_bootstrap_chunk(&conn, resp.repo, resp.index, &mf)
                .await;
        }
    }

    /// v9：请求清单中第 `index` 块，并记录「无响应」尝试次数/首次时刻。
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
        // v10(A)：双向引导冲突 —— 对端正在从我拉同 repo 快照且本端为让路方时，
        // 暂停发块请求（进度保留，对端传完后 resume 自动恢复续传）。
        if self.should_yield_bootstrap(&conn.node_id, repo) {
            debug!(
                "[bootstrap] 双向引导冲突，暂停拉取让路对方: peer={} repo={} index={}",
                conn.node_id, repo, index
            );
            return;
        }
        // v9：记录 (index, 次数, 首次时刻)。resume tick 据此判定「对端一直不回帧」
        // 并升级为「重拉清单」——旧实现无计数、无超时，只会在 60s 周期里无限重发同一 index。
        {
            let mut m = self.bootstrap_chunk_attempt.write();
            let e = m
                .entry((conn.node_id, repo))
                .or_insert((index, 0, Instant::now()));
            if e.0 != index {
                *e = (index, 0, Instant::now());
            }
            e.1 = e.1.saturating_add(1);
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
        if let Ok(Some((mut p, mf))) = self.delta_storage().bootstrap_load(&conn.node_id.0, repo) {
            p.phase = bootstrap::BootstrapPhase::Done;
            p.updated_ms = now;
            let _ = self.delta_storage().bootstrap_save(&p, mf.as_ref());
        }
        // v9：追尾起点在这里落库。清单响应阶段不再提前推进游标（见
        // `handle_bootstrap_manifest_response`），保证「数据真正落地后才声明水位」。
        let _ = self
            .delta_storage()
            .set_peer_seq(&conn.node_id.0, repo, delta::seq_to_i64(w0_seq));
        // v9：清掉该 (peer,repo) 的分块尝试/失败计数与 gap 标记 —— 快照已补齐历史空洞。
        self.bootstrap_chunk_attempt
            .write()
            .remove(&(conn.node_id, repo));
        self.bootstrap_verify_fails
            .write()
            .remove(&(conn.node_id, repo));
        self.delta_gap.write().remove(&(conn.node_id, repo));
        // v9：追尾不受协商策略门控 —— 进入 bootstrap 的判定之一恰是
        // 「策略 = BOOTSTRAP」，而 `delta_channel_allowed` 要求策略 = DELTA，
        // 旧实现因此让竣工后的追尾被同一条策略静默拒绝（B5）。这里清掉协商结果，
        // 迫使下一轮重协商，同时把该 (peer,repo) 的节流清空以便立即追尾。
        self.negotiated.write().remove(&conn.node_id);
        self.negotiation_sent_at.write().remove(&conn.node_id);
        // v10(F4)：快照冷却 —— 竣工时刻起 `bootstrap_cooldown_secs` 内，协商裁定与
        // 巡检对本 (peer,repo) 强制 DELTA。清协商（B5 修复）迫使立即重裁，而裁定输入
        // （行数）不会立刻变化，无冷却则必然再次裁 BOOTSTRAP（实测 52/58 死循环通道）。
        self.bootstrap_cooldown
            .write()
            .insert((conn.node_id, repo), Instant::now());
        self.delta_request_at.write().remove(&(conn.node_id, repo));
        self.delta_has_more.write().insert((conn.node_id, repo));
        info!(
            "[bootstrap] {} repo={} 块全部落地，切 delta 追尾（since_seq={}，已清协商以解除追尾门控）",
            conn.node_id, repo, w0_seq
        );
        // ⑤ 追尾：从 w0 拉 oplog 增量（P1-3 通道）
        self.trigger_delta_sync(conn.node_id, repo).await;
    }

    /// v9：某 (peer, repo) 是否存在**仍在推进**的 bootstrap 进度。
    ///
    /// 旧判定是 `bootstrap_list().any(|p| p.repo == rt && p.phase != Done)` ——
    /// 不含 peer、无超时、无失败态、`bootstrap_clear` 在生产零调用，于是一行卡在 Transfer
    /// 就让**该 repo 对所有对端的 delta 永久停摆**（实测 repo1/2/3 零进展 25 分钟）。
    /// 现在要求 peer/repo 双匹配且 `updated_ms` 在 `bootstrap_stall_secs` 内；
    /// 超期即判定卡死 → 置 Idle（保留 w0/清单/进度，下一轮 resume 继续）并返回 false，
    /// 让 delta 立刻恢复。
    fn bootstrap_running_fresh(
        &self,
        list: &[bootstrap::BootstrapProgress],
        peer: NodeId,
        repo: u8,
    ) -> bool {
        let Some(p) = list
            .iter()
            .find(|p| p.repo == repo && p.peer.as_slice() == peer.0)
        else {
            return false;
        };
        if p.phase == bootstrap::BootstrapPhase::Done {
            return false;
        }
        let st = self.delta_storage();
        let stall_ms = self.config.bootstrap_stall_secs.saturating_mul(1000).max(1);
        let now_ms = chrono::Utc::now().timestamp_millis();
        let age_ms = now_ms.saturating_sub(p.updated_ms).max(0) as u64;
        if age_ms <= stall_ms {
            return true;
        }
        warn!(
            "[bootstrap] 进度停滞 {}s（阈值 {}s）→ 置 Idle 并放行 delta: peer={} repo={} done={}/{} phase={}",
            age_ms / 1000,
            self.config.bootstrap_stall_secs,
            peer,
            repo,
            p.done_chunks,
            p.total_chunks,
            p.phase.as_str()
        );
        let mut reset = p.clone();
        reset.phase = bootstrap::BootstrapPhase::Idle;
        reset.updated_ms = now_ms;
        reset.error = Some(format!("stalled {}s (v9 watchdog)", age_ms / 1000));
        let _ = st.bootstrap_save(&reset, None);
        self.bootstrap_chunk_attempt.write().remove(&(peer, repo));
        false
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
        let max_attempts = self.config.bootstrap_chunk_max_attempts.max(1);
        let chunk_timeout = Duration::from_secs(self.config.bootstrap_chunk_timeout_secs.max(1));
        for p in list {
            if p.phase == bootstrap::BootstrapPhase::Done || p.peer.len() != 20 {
                continue;
            }
            let mut arr = [0u8; 20];
            arr.copy_from_slice(&p.peer);
            let peer = NodeId(arr);
            // v9：无响应升级 —— 同一 index 连续请求超限或超时（对端完全没回帧）时重拉清单。
            // 旧实现没有这层判定，只会在 60s 周期里无限重发同一 index（实测 194 次全从块 0）。
            let stale_request = {
                let m = self.bootstrap_chunk_attempt.read();
                match m.get(&(peer, p.repo)) {
                    Some(&(idx, cnt, first)) => {
                        idx == p.done_chunks as u32
                            && (cnt >= max_attempts || first.elapsed() >= chunk_timeout)
                    }
                    None => false,
                }
            };
            if stale_request {
                let (idx, cnt, secs) = {
                    let m = self.bootstrap_chunk_attempt.read();
                    m.get(&(peer, p.repo))
                        .map(|&(i, c, t)| (i, c, t.elapsed().as_secs()))
                        .unwrap_or((0, 0, 0))
                };
                warn!(
                    "[bootstrap] 分块 {} 连续 {} 次 / {}s 无响应 → 重拉清单: peer={} repo={}",
                    idx, cnt, secs, peer, p.repo
                );
                self.bootstrap_chunk_attempt.write().remove(&(peer, p.repo));
                self.clone().start_bootstrap(peer, p.repo).await;
                continue;
            }
            if self.sessions.get_connection(&peer).is_none() {
                // 无连接时不计入失败，等连接恢复再续传（避免把「没连接」当成对端无响应）。
                continue;
            }
            match self.delta_storage().bootstrap_load(&p.peer, p.repo) {
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

        // 定期检查（批次 3：独立节流）—— 对比本地与对端各 repo 总数，差 20% 以上触发 bootstrap。
        // 该巡检是 DB 级全表计数比对：原先挂在 resume tick 里**每轮**都跑，把本该秒级返回的
        // 续传任务单次占槽拉到 max 164.36s，同分类（Federation 8/8）的 delta / gossip
        // 一起被饿死。这里独立节流到 5 分钟一次，续传任务即可快速让出分类槽。
        let due_check = {
            let mut last = self.bootstrap_check_at.write();
            let due = last
                .map(|t| t.elapsed() >= Duration::from_secs(BOOTSTRAP_CHECK_INTERVAL_SECS))
                .unwrap_or(true);
            if due {
                *last = Some(Instant::now());
            }
            due
        };
        if due_check {
            self.check_and_trigger_bootstrap().await;
        }
    }

    /// 定期检查本地与对端各 repo 总数差异，差 20% 以上自动触发 bootstrap
    async fn check_and_trigger_bootstrap(self: &Arc<Self>) {
        if !self.config.bootstrap_enabled {
            return;
        }

        let local_counts = self.local_entry_counts();
        // 先 clone 出 digest 数据，立即释放读锁，避免读锁 guard 跨 await 导致 Send 不满足
        let digests: Vec<(NodeId, Vec<u32>)> = {
            let d = self.peer_digests.read();
            d.iter()
                .filter_map(|(peer, counts)| {
                    if counts.is_empty() {
                        None
                    } else {
                        Some((*peer, counts.clone()))
                    }
                })
                .collect()
        };

        // A5：四个 repo 走**完全相同**的逻辑与阈值（此前硬编码只取 counts[0] 并写死
        // repo_type::NODE，导致 PEER 差 44%、INFOHASH 差数百也不触发快照）。
        // 唯一差异只是各 repo 自身的数据量。
        for &repo in &[
            repo_type::NODE,
            repo_type::PEER,
            repo_type::INFOHASH,
            repo_type::TRACKER,
        ] {
            let idx = (repo - repo_type::NODE) as usize;
            let local_count = match local_counts.get(idx) {
                Some(&c) => c as u64,
                None => continue,
            };
            if local_count < SNAPSHOT_MIN_ROWS {
                // 本地该 repo 数据太少（启动初期），不触发
                continue;
            }

            for (peer, counts) in &digests {
                let remote_count = match counts.get(idx) {
                    Some(&c) => c as u64,
                    None => continue,
                };
                if remote_count == 0 || local_count == 0 {
                    continue;
                }

                // v9：触发条件改为「**绝对差**超阈值 **或** 相对差超 20%」。
                // 旧实现只看相对比例（20%），于是 1.86M vs 1.73M（差 12 万行、7%）永不触发 ——
                // 实测本端领先 12 万行就是这么来的。方向仍保持「对端更多 → 本端拉取」：
                // 本端领先时由对端自己的巡检触发它来拉我们（两端同代码、同规则，天然对称）。
                if remote_count <= local_count {
                    continue;
                }
                let diff = remote_count - local_count;
                let ratio = remote_count as f64 / local_count as f64;
                if ratio < SNAPSHOT_RATIO_THRESHOLD
                    && diff <= self.config.range_bulk_threshold_rows.max(1)
                {
                    continue;
                }

                // 检查是否已有正在进行且仍在推进的 bootstrap（v9：带新鲜度判定）
                let running_rows = self.delta_storage().bootstrap_list().unwrap_or_default();
                if self.bootstrap_running_fresh(&running_rows, *peer, repo) {
                    continue;
                }
                // v10(F4)：快照冷却期内跳过 —— 竣工后行数差不会立刻消失，
                // 无冷却则每轮巡检必然重触发（死循环通道之二）。
                if self.snapshot_in_cooldown(peer, repo) {
                    continue;
                }
                // v10(F5)：水位校验 —— 我方游标已 ≥ 对端 oplog 保留窗口起点时，
                // 欠账全在窗口内，delta 即可追平，快照是纯浪费。
                if !self.snapshot_really_needed(peer, repo) {
                    debug!(
                        "[bootstrap] repo={} peer={} 欠账在对端 oplog 窗口内（游标≥min_seq），跳过快照走 delta",
                        repo, peer
                    );
                    continue;
                }

                info!(
                    "[bootstrap] 检测到差异: repo={} local={}, remote={}, diff={}, ratio={:.1}%, 触发 bootstrap: peer={}",
                    repo,
                    local_count,
                    remote_count,
                    diff,
                    ratio * 100.0,
                    peer
                );

                if let Some(conn) = self.sessions.get_connection(peer) {
                    let node_id = conn.node_id;
                    self.clone().start_bootstrap(node_id, repo).await;
                    return; // 一次只触发一个
                }
            }
        }
    }

    /// P2-1：向指定对端发起某 repo 的 bootstrap（拉清单 → 分块 → 追尾）。默认关闭。
    pub async fn start_bootstrap(self: Arc<Self>, peer: NodeId, repo: u8) {
        if !self.config.bootstrap_enabled {
            return;
        }
        // v10(A)：双向引导冲突让路 —— 对端正在从我拉同 repo 快照且本端为让路方时，
        // 不发起（等对端传完，resume 自动恢复）。
        if self.should_yield_bootstrap(&peer, repo) {
            debug!(
                "[bootstrap] 双向引导冲突，让路对方（响应方优先）: peer={} repo={}",
                peer, repo
            );
            return;
        }
        // v10：发起互斥 —— 竞态窗口内同一 (peer,repo) 只放行一个在途发起。
        if !self.try_acquire_bootstrap_slot(&peer, repo) {
            debug!(
                "[bootstrap] 已有在途发起（TTL {}s 内），跳过重复触发: peer={} repo={}",
                BOOTSTRAP_INFLIGHT_TTL_SECS, peer, repo
            );
            return;
        }
        // P0-1/P0-5：bootstrap 已打通全 repo（清单构建 build_repo_manifest_impl 与应答取数
        // load_repo_sync_entries_in_range 本就是 repo 通用的，落地改走 handle_sync_batch）。
        // 保留范围校验（非法 repo 值一律拒绝），不再限制只做 NODE。
        if !(repo_type::NODE..=repo_type::TRACKER).contains(&repo) {
            debug!("[bootstrap] repo={} 非法，忽略: peer={}", repo, peer);
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
                    // v9：暴露 peer —— 进度表已按 (peer, repo) 存储，旧接口不输出 peer
                    // 导致现场无法判断「卡住的是哪个对端」（本次故障正是卡在这里）。
                    "peer": p.peer.iter().map(|b| format!("{:02x}", b)).collect::<String>(),
                    "phase": p.phase.as_str(),
                    "total_chunks": p.total_chunks,
                    "done_chunks": p.done_chunks,
                    "ratio": p.ratio(),
                    "bytes": p.bytes,
                    "w0_seq": p.w0_seq,
                    "updated_ms": p.updated_ms,
                    "error": p.error,
                    "stalled": chrono::Utc::now().timestamp_millis()
                        .saturating_sub(p.updated_ms)
                        .max(0) as u64
                        > self.config.bootstrap_stall_secs.saturating_mul(1000),
                })
            })
            .collect();
        // v9：oplog 空洞标记（对端已裁剪、中间段结构性缺失）与 gossip 丢批计数
        let gaps: Vec<serde_json::Value> = self
            .delta_gap
            .read()
            .iter()
            .map(|(peer, repo)| {
                serde_json::json!({
                    "peer": peer.0.iter().map(|b| format!("{:02x}", b)).collect::<String>(),
                    "repo": repo,
                })
            })
            .collect();
        let (drop_overflow, drop_expired, drop_retry, requeued) = self.gossip_engine.drop_stats();
        let coalesce_depth = self.gossip_engine.coalesce_pending_depth();
        let gossip_drops = serde_json::json!({
            "outbox_overflow": drop_overflow,
            "expired": drop_expired,
            "retry_budget": drop_retry,
            "requeued_on_cancel": requeued,
            "coalesce_pending": coalesce_depth,
        });
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
            "delta_gap": gaps,
            "gossip_drops": gossip_drops,
            "reconcile_nodes_visited": self
                .range_ranges_visited
                .load(std::sync::atomic::Ordering::Relaxed),
            "range_stats": range_stats,
        })
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

    /// F1(2026-09-27 52/58 BOOTSTRAP 死循环回归测试)：
    /// 协商/巡检计数（`local_entry_counts`）必须与 bootstrap 清单扫描
    /// （`build_repo_manifest_impl` → `load_repo_key_hashes_in_range`）同口径。
    /// peers 主表 3 活行 + peers_archive 2 行 → peer 计数必须 = 3。
    /// 旧实现把 archive 计入（= 5），清单永远拉不到那 2 行归档 → 每轮巡检裁 BOOTSTRAP 全量重拉。
    #[test]
    fn test_local_entry_counts_matches_manifest_scope() {
        use crate::federation::protocol::repo_type;
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        {
            let conn = storage.connection();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            for i in 0..3i64 {
                conn.execute(
                    "INSERT INTO peers (infohash, ip, port, source) VALUES (?1, ?2, ?3, 'test')",
                    rusqlite::params![[0xaau8; 20], format!("10.0.0.{}", i), 6881i64],
                )
                .unwrap();
            }
            for i in 0..2i64 {
                conn.execute(
                    "INSERT OR IGNORE INTO peers_archive (infohash, ip, port, archived_at) \
                     VALUES (?1, ?2, ?3, 0)",
                    rusqlite::params![[0xbbu8; 20], format!("10.1.0.{}", i), 6881i64],
                )
                .unwrap();
            }
        }

        let node_repo = Arc::new(NodeRepoImpl::new(storage.clone()));
        let (shutdown_tx, _) = broadcast::channel(1);
        let cm = SessionsHandle::new_for_test();
        let metrics = Arc::new(FederationMetrics::new());
        let gossip = Arc::new(GossipEngine::new(
            cm.clone(),
            make_config(),
            NodeId([1; 20]),
            metrics.clone(),
            shutdown_tx.clone(),
        ));
        let mgr = SyncManager::new(
            cm,
            node_repo,
            make_config(),
            shutdown_tx,
            gossip,
            metrics,
            NodeId([1; 20]),
            None,
            None,
            None,
            None,
            None,
        );

        let counts = mgr.local_entry_counts();
        assert_eq!(
            counts[1], 3,
            "peer 计数必须等于 peers 主表活行数（不含 peers_archive）"
        );

        let manifest =
            bootstrap::build_repo_manifest_impl(storage.as_ref(), repo_type::PEER, 2, 0, 1)
                .unwrap();
        assert_eq!(
            counts[1] as u64, manifest.total_rows,
            "协商计数与 bootstrap 清单行数必须一致（口径铁律）"
        );
    }

    /// F4/F5 测试共用构造器（照抄 test_apply_node_sync 的装配方式）。
    fn make_sync_manager(
        storage: Arc<crate::storage::db::Storage>,
        cfg: FederationConfig,
    ) -> SyncManager {
        let node_repo = Arc::new(NodeRepoImpl::new(storage));
        let (shutdown_tx, _) = broadcast::channel(1);
        let cm = SessionsHandle::new_for_test();
        let metrics = Arc::new(FederationMetrics::new());
        let gossip = Arc::new(GossipEngine::new(
            cm.clone(),
            cfg.clone(),
            NodeId([1; 20]),
            metrics.clone(),
            shutdown_tx.clone(),
        ));
        SyncManager::new(
            cm,
            node_repo,
            cfg,
            shutdown_tx,
            gossip,
            metrics,
            NodeId([1; 20]),
            None,
            None,
            None,
            None,
            None,
        )
    }

    /// F4(死循环通道守卫回归)：快照冷却期内的 (peer,repo) 在协商裁定中强制 DELTA，
    /// 冷却期外恢复行数规则；且冷却按 repo 隔离，不波及其他 repo 裁定。
    #[test]
    fn test_snapshot_cooldown_forces_delta() {
        use crate::federation::protocol::repo_type;
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        {
            let conn = storage.connection();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            for i in 0..200i64 {
                conn.execute(
                    "INSERT INTO peers (infohash, ip, port, source) VALUES (?1, ?2, ?3, 'test')",
                    rusqlite::params![
                        [0xacu8; 20],
                        format!("10.2.{}.{}", i / 250, i % 250),
                        6881i64
                    ],
                )
                .unwrap();
            }
        }
        let mut cfg = make_config();
        cfg.range_bulk_threshold_rows = 10;
        let mgr = make_sync_manager(storage, cfg);

        let peer = NodeId([2; 20]);
        let remote = SyncNegotiateMessage {
            repos: vec![
                RepoSyncState {
                    repo: repo_type::PEER,
                    row_count: 100,
                    max_seq: 10,
                    min_seq: 1,
                    retention_secs: 3600,
                },
                RepoSyncState {
                    repo: repo_type::TRACKER,
                    row_count: 0,
                    max_seq: 0,
                    min_seq: 0,
                    retention_secs: 3600,
                },
            ],
            caps: SyncCaps {
                send_rate_bytes_per_sec: 0,
                recv_rate_bytes_per_sec: 0,
                disk_throughput_hint: 0,
                batch_limit: 1000,
            },
            timestamp_ms: 0,
        };

        // 无冷却：本地 200 / 对端 100 = 2.0 > 1.2 且差 100 > 10 → BOOTSTRAP
        let s = mgr.decide_strategies(&peer, &remote);
        assert_eq!(
            s.iter()
                .find(|r| r.repo == repo_type::PEER)
                .unwrap()
                .strategy,
            protocol::STRATEGY_BOOTSTRAP
        );

        // 落冷却：同输入必须裁 DELTA（无冷却则竣工清协商重裁必然再次 BOOTSTRAP）
        mgr.bootstrap_cooldown
            .write()
            .insert((peer, repo_type::PEER), Instant::now());
        let s2 = mgr.decide_strategies(&peer, &remote);
        assert_eq!(
            s2.iter()
                .find(|r| r.repo == repo_type::PEER)
                .unwrap()
                .strategy,
            protocol::STRATEGY_DELTA,
            "冷却期内必须强制 DELTA"
        );

        // 冷却按 repo 隔离：TRACKER 双方皆 0 → NONE，不受 PEER 冷却影响
        //（不用 NODE 验证：其计数含 write_queue 非零积压，前提不成立）
        assert_eq!(
            s2.iter()
                .find(|r| r.repo == repo_type::TRACKER)
                .unwrap()
                .strategy,
            protocol::STRATEGY_NONE
        );
    }

    /// F5(水位守卫)：我方游标 ≥ 对端 oplog 保留窗口起点(min_seq) → 欠账全在窗口内，
    /// delta 可追平、无需快照(false)；游标 < min_seq(retention 断档) → 需要快照(true)；
    /// 对端状态缺失或 min_seq=0 → 保守放行(true,回退旧行为)。
    #[test]
    fn test_snapshot_really_needed_watermark_gate() {
        use crate::federation::protocol::repo_type;
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        let mgr = make_sync_manager(storage.clone(), make_config());

        let peer = NodeId([3; 20]);
        mgr.peer_negotiate_state.write().insert(
            peer,
            vec![RepoSyncState {
                repo: repo_type::PEER,
                row_count: 5,
                max_seq: 500,
                min_seq: 100,
                retention_secs: 3600,
            }],
        );

        // 游标 0 < 100：存在 delta 补不上的空洞 → 需要快照
        assert!(mgr.snapshot_really_needed(&peer, repo_type::PEER));
        // 游标推进到 150 ≥ 100：欠账全在窗口内 → delta 即可，不需要快照
        storage.set_peer_seq(&peer.0, repo_type::PEER, 150).unwrap();
        assert!(
            !mgr.snapshot_really_needed(&peer, repo_type::PEER),
            "游标已进对端保留窗口，快照是纯浪费"
        );
        // 对端无自报状态（协商早期）→ 保守放行
        assert!(mgr.snapshot_really_needed(&NodeId([9; 20]), repo_type::PEER));
    }

    /// v10 收尾测试：bootstrap 发起互斥（竞态窗口单飞）与 Range 让路判定
    /// （快照传输活跃时反熵让路，避免 SQLite 读池争用把 tick 拖到 300s）。
    #[test]
    fn test_bootstrap_inflight_mutex_and_range_yield() {
        use crate::federation::protocol::repo_type;
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        let mgr = make_sync_manager(storage.clone(), make_config());
        let peer = NodeId([4; 20]);

        // 发起互斥：首次获得槽位，同 (peer,repo) 重复触发被拒；不同 repo 不互斥
        assert!(mgr.try_acquire_bootstrap_slot(&peer, repo_type::PEER));
        assert!(!mgr.try_acquire_bootstrap_slot(&peer, repo_type::PEER));
        assert!(mgr.try_acquire_bootstrap_slot(&peer, repo_type::INFOHASH));

        // Range 让路：无 bootstrap 进度 → 不让路；有非 Done 且新鲜的进度 → 让路
        assert!(!mgr.bootstrap_transfer_active());
        let mut p = bootstrap::BootstrapProgress::new(
            repo_type::PEER,
            peer.0.to_vec(),
            chrono::Utc::now().timestamp_millis(),
        );
        p.phase = bootstrap::BootstrapPhase::Transfer;
        storage.bootstrap_save(&p, None).unwrap();
        assert!(
            mgr.bootstrap_transfer_active(),
            "快照传输活跃期反熵必须让路"
        );
    }

    /// v10(B2)：key 游标续传定位 —— 块边界随活表写入漂移（块数 129→130、边界 key
    /// 后移）后，last_key 游标仍能正确定位续传起点；各退化场景（None/+∞/越尾）自洽。
    #[test]
    fn test_locate_resume_index_survives_boundary_drift() {
        use crate::federation::protocol::repo_type;
        let chunk = |index: u32, hi: Vec<u8>, rows: u64| bootstrap::ManifestChunk {
            index,
            lo: vec![],
            hi,
            rows,
            hash: [0u8; 32],
        };
        // 旧清单: 3 块, 边界 a/b/c, 已传 2 块 → last_key = "b"
        let old = bootstrap::BootstrapManifest {
            repo: repo_type::NODE,
            version: 829162,
            w0_seq: 829161,
            chunk_rows: 10,
            total_rows: 30,
            chunks: vec![
                chunk(0, vec![b'a'], 10),
                chunk(1, vec![b'b'], 10),
                chunk(2, vec![], 10),
            ],
        };
        // 新清单: 活表写入 → 块数 3→4, 边界整体漂移, version/w0 变化
        let new = bootstrap::BootstrapManifest {
            repo: repo_type::NODE,
            version: 4_091_331,
            w0_seq: 4_091_330,
            chunk_rows: 10,
            total_rows: 31,
            chunks: vec![
                chunk(0, vec![b'a', b'5'], 10),
                chunk(1, vec![b'b', b'5'], 10),
                chunk(2, vec![b'c', b'5'], 10),
                chunk(3, vec![], 1),
            ],
        };
        // last_key = "b"(旧第 2 块 hi): 新清单第一个 hi > "b" 的是块 1(b'5' > b'b'? 0x35 < 0x62 → 否!)
        // b"b\x35" 与 "b": 首字节相等, 次字节新清单为 0x35 —— last_key 是精确的 "b"(单字节)。
        // "b\x35" > "b"(前缀比较, 长者大) → 块 1 hi > last_key → 续传从块 1(部分重传, 幂等)
        assert_eq!(
            bootstrap::locate_resume_index(&new.chunks, Some(&old.chunks[1].hi)),
            1,
            "last_key 所在块之后的第一个块即续传点"
        );
        // None → 从 0
        assert_eq!(bootstrap::locate_resume_index(&new.chunks, None), 0);
        // +∞(空 hi, 传完全量) → 全跳过
        assert_eq!(
            bootstrap::locate_resume_index(&new.chunks, Some(&[])),
            new.chunks.len() as u64
        );
        // last_key 大于前面块的边界，但末块 hi=+∞ 永远兜底 → 续传点 = 末块下标
        assert_eq!(
            bootstrap::locate_resume_index(&new.chunks, Some(&[0xff, 0xff])),
            (new.chunks.len() - 1) as u64
        );
    }

    /// v10(A)：双向引导冲突按 node_id 字典序确定性让路（大者让），
    /// serving 窗口过期或对端更大时不让路。
    #[test]
    fn test_bidirectional_bootstrap_yield_by_node_id() {
        use crate::federation::protocol::repo_type;
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        let mgr = make_sync_manager(storage, make_config());
        // make_sync_manager 的本端 = NodeId([1;20])
        let smaller_peer = NodeId([0; 20]);
        let bigger_peer = NodeId([2; 20]);

        // serving 未刷新 → 不让路
        assert!(!mgr.should_yield_bootstrap(&smaller_peer, repo_type::NODE));

        // serving 新鲜：本端(0x01…) > 对端(0x00…) → 本端让路
        mgr.bootstrap_serving_at
            .write()
            .insert((smaller_peer, repo_type::NODE), Instant::now());
        mgr.bootstrap_serving_at
            .write()
            .insert((bigger_peer, repo_type::NODE), Instant::now());
        assert!(mgr.should_yield_bootstrap(&smaller_peer, repo_type::NODE));
        // 对端(0x02…) > 本端(0x01…) → 对端让路，本端继续
        assert!(!mgr.should_yield_bootstrap(&bigger_peer, repo_type::NODE));
        // v10(A+) 持久让路：serving 过期后、让路截止前仍保持让路（防双向重启震荡）
        mgr.bootstrap_serving_at
            .write()
            .remove(&(smaller_peer, repo_type::NODE));
        assert!(
            mgr.should_yield_bootstrap(&smaller_peer, repo_type::NODE),
            "serving 过期但让路截止未到，必须保持让路"
        );
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
}
