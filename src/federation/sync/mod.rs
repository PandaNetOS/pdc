//! 同步管理器
//!
//! 阶段2扩展：集成 Gossip 引擎、PeerRepo 同步、InfohashRepo 同步。
//! 阶段1的 NodeRepo 同步保留。

#![allow(clippy::type_complexity)]

pub mod bootstrap;
pub mod channels_status;
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
use tracing::{debug, error, info, warn};

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
/// D批(D2)：双向快照触发的「大小端比例」阈值（不区分方向，max/min 超过即候选）。
/// D批(D3)：1.3→1.1 调敏 —— 三端联邦实测 52/62 ratio≈1.11 在 1.3 下永远裁定 DELTA，
/// 不触发 bootstrap，NODE 表长期不对齐；1.1 配合 diff>50_000 即可覆盖该量级差。
const SNAPSHOT_BIDIR_RATIO: f64 = 1.1;
/// D批(D2)：双向快照触发的最小行数绝对差（行）。
const BOOTSTRAP_BIDIR_MIN_DIFF_ROWS: u64 = 50_000;
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

/// 修复（活表竣工死循环）：INFOHASH/PEER 这类 crawler 持续写入的活表，键随机分布、
/// 插入可落在任意块区间，传输期间本地表一直在变 → 按远端快照清单逐块 hash&rows 全等
/// 的严格竣工校验永不成立（不写 Done → resume/watchdog 反复重试 = 死循环）。
/// 对这类表改走宽松竣工判定：只要本地实际行数已达到远端快照总行数的该比例，即视为数据
/// 已基本落地、允许写 Done 进入 delta 追尾；不足比例则判「块返回极少行」的假竣工，
/// 仍不写 Done，交由 resume/watchdog 继续推进。NODE/TRACKER 静态表保持严格全等校验。
const BOOTSTRAP_LIVE_RELAXED_ROW_RATIO: f64 = 0.95;

// ========================================================================
// 核心运行时切片（2026-09-30）：bootstrap 发送端并发上限 / 发送超时 / per-peer 熔断 /
// 内存反压联动。根因：52 内存峰值 6.5GB = bootstrap 发送 handler 无并发上限 +
// send_message 无快速失败，对端 HDD 慢时已加载的 2 万行 SyncEntry 在任务中无限堆积。
// 全部为进程级全局状态（与 channels_status 同模式），面板/内存监控直接读，无需改
// io_scheduler 接口。
// ========================================================================
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// A1/A4：当前在途（已加载 entries、尚未发送完成）的 bootstrap 发送任务数。
static BOOTSTRAP_SEND_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
/// A2/面板：发送尝试累计（成功+失败），供面板算失败率。
static BOOTSTRAP_SEND_TOTAL: AtomicU64 = AtomicU64::new(0);
/// A2/面板：发送失败累计（send_message Err 或超时）。
static BOOTSTRAP_SEND_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// A4：内存反压联动 override —— >0 时在途上限取此值（内存超阈值时由 memory_monitor
/// 置 1 把发送压到串行）；0 = 恢复使用配置值 `bootstrap_send_concurrency`。
static BOOTSTRAP_SEND_CONCURRENCY_OVERRIDE: AtomicUsize = AtomicUsize::new(0);
/// D1：range handler 执行体超时累计次数（计数落点：模块静态，面板可经可观测性读取）。
static RANGE_HANDLER_TIMEOUTS: AtomicU64 = AtomicU64::new(0);

/// A1：非阻塞地把 `counter` 从 `cur` CAS 到 `cur+1`，仅当 `cur < limit` 时成功。
/// 这是「在途发送数」的有界获取：拿不到立即返回 false（调用方 NAK、不排队）。
/// 抽成纯函数便于单测（生产用全局 `BOOTSTRAP_SEND_IN_FLIGHT`，测试用本地 AtomicUsize）。
fn try_inc_bounded(counter: &AtomicUsize, limit: usize) -> bool {
    let limit = limit.max(1);
    let mut cur = counter.load(Ordering::Acquire);
    loop {
        if cur >= limit {
            return false;
        }
        match counter.compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(actual) => cur = actual,
        }
    }
}

/// A3：per-peer 发送熔断状态（纯数据，便于单测）。
///
/// 连续发送失败达阈值即熔断：新块请求直接 NAK("circuit open")；按
/// [`bootstrap::backoff_delay`]（30s 基数、600s 封顶）做指数退避，退避到期后半开探测；
/// 成功发送一次即整条清零。
#[derive(Debug, Clone, Copy)]
pub(crate) struct SendCircuitState {
    /// 连续发送失败次数（成功一次即归零）。
    pub fails: u32,
    /// 最近一次记录失败/熔断的时刻；据此按指数退避表决定半开探测时机。
    pub marked_at: Option<Instant>,
}

impl SendCircuitState {
    /// 记录一次发送失败：失败计数 +1，刷新标记时刻。
    pub fn record_failure(&self, now: Instant) -> Self {
        Self {
            fails: self.fails.saturating_add(1),
            marked_at: Some(now),
        }
    }
    /// 发送成功：整条清零（解除熔断）。
    ///
    /// 仅测试用 —— 生产路径的成功清零走 `send_circuits().write().remove(&peer)`
    /// （见 handle_bootstrap_chunk_request 发送成功分支），无需构造新状态。
    #[cfg(test)]
    pub fn record_success() -> Self {
        Self {
            fails: 0,
            marked_at: None,
        }
    }
    /// 当前是否仍处于熔断开放期：fails 达阈值且指数退避未到期。
    pub fn is_open(&self, threshold: u32, now: Instant, base: Duration, max: Duration) -> bool {
        if self.fails < threshold {
            return false;
        }
        let Some(marked) = self.marked_at else {
            return false;
        };
        let backoff = bootstrap::backoff_delay(base, max, self.fails);
        now.duration_since(marked) < backoff
    }
}

/// A3：per-peer 发送熔断表（全局单例，与 channels_status 同模式）。
static SEND_CIRCUITS: std::sync::OnceLock<
    parking_lot::RwLock<FxHashMap<NodeId, SendCircuitState>>,
> = std::sync::OnceLock::new();

fn send_circuits() -> &'static parking_lot::RwLock<FxHashMap<NodeId, SendCircuitState>> {
    SEND_CIRCUITS.get_or_init(|| parking_lot::RwLock::new(FxHashMap::default()))
}

/// A5：serving 标记是否已 idle 过期（纯判定）。`now - last >= idle` 即应移除。
fn serving_entry_expired(last: Instant, now: Instant, idle: Duration) -> bool {
    now.duration_since(last) >= idle
}

/// A1/A4：当前生效的在途发送上限 —— override>0 时取 override（内存反压压到 1），
/// 否则取配置默认值。
fn effective_send_limit(config_concurrency: usize) -> usize {
    let ov = BOOTSTRAP_SEND_CONCURRENCY_OVERRIDE.load(Ordering::Acquire);
    if ov > 0 {
        ov
    } else {
        config_concurrency
    }
}

// === 跨代理契约（供面板 / session.rs / memory_monitor 调用）===
// 注：main.rs 是独立 bin crate，与 lib 分离，故对外可见性用 pub（非 pub(crate)），
// 否则 bin 端 memory_monitor 无法调用 override set/get。

/// 当前在途 bootstrap 发送任务数（A1 计数器实时值）。
pub fn bootstrap_send_in_flight() -> usize {
    BOOTSTRAP_SEND_IN_FLIGHT.load(Ordering::Acquire)
}
/// 发送尝试累计（成功+失败），供面板算失败率。
pub fn bootstrap_send_total() -> u64 {
    BOOTSTRAP_SEND_TOTAL.load(Ordering::Acquire)
}
/// 发送失败累计。
pub fn bootstrap_send_failures_total() -> u64 {
    BOOTSTRAP_SEND_FAILURES_TOTAL.load(Ordering::Acquire)
}
/// 当前处于熔断开放期的对端数。
pub fn bootstrap_send_circuit_open_peers() -> usize {
    send_circuits()
        .read()
        .values()
        .filter(|s| s.fails > 0)
        .count()
}
/// A4：设置内存反压在途并发 override（0 = 恢复配置值；>0 = 收缩到此值）。
pub fn set_bootstrap_send_concurrency_override(n: usize) {
    BOOTSTRAP_SEND_CONCURRENCY_OVERRIDE.store(n, Ordering::Release);
}
/// A4：当前 override 值（0 = 未启用）。
pub fn bootstrap_send_concurrency_override() -> usize {
    BOOTSTRAP_SEND_CONCURRENCY_OVERRIDE.load(Ordering::Acquire)
}

/// 跨代理契约：该对端是否有未过期的 bootstrap 工作（发送或接收），供 session.rs
/// 空闲会话豁免 —— 有在途工作的连接不得被 idle_timeout 误杀。
///
/// 自由函数（session.rs 经 `crate::federation::sync::has_active_bootstrap_work(&peer)` 调用）。
/// 最小实现：读全局通道状态 —— 当前 bootstrap 槽位 active 且 peer_id 对端一致即视为
/// 有未过期工作（发送/接收方向均会写 `active=true + peer_id`，E3 覆盖两方向）。
pub(crate) fn has_active_bootstrap_work(node_id: &NodeId) -> bool {
    let s = crate::federation::sync::channels_status::global();
    let g = s.read();
    g.bootstrap.active && g.bootstrap.peer_id == node_id.to_hex()
}

/// A1：在途发送槽 RAII 守卫 —— drop 时递减全局在途计数（覆盖 early-return 路径）。
struct BootstrapSendPermit;

impl Drop for BootstrapSendPermit {
    fn drop(&mut self) {
        BOOTSTRAP_SEND_IN_FLIGHT.fetch_sub(1, Ordering::AcqRel);
    }
}

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
    /// v9：分块请求的在途/失败跟踪 —— (peer, repo) → 退避重试状态。
    ///
    /// 活锁治理(任务1)重定义：旧结构 (index, 次数, 首次时刻) 按「无响应次数」计数并
    /// 升级重拉清单，是三节点互拉活锁的第一环。现改为 [`bootstrap::ChunkRetryState`]：
    /// 只记**传输类失败**（send 失败/超时/无会话），resume tick 按指数退避表决定何时重发，
    /// 永不因此重拉清单；成功收到任意响应即整条清零。
    bootstrap_chunk_attempt: RwLock<FxHashMap<(NodeId, u8), bootstrap::ChunkRetryState>>,
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
    /// E1：运行时差异复检重触发冷却 —— (peer, repo) → 最近一次因运行时行数差超 D2 阈值
    /// 而重触发 bootstrap 的时刻。冷却期内不再重复点火（防抖），与 v10(F4) 的
    /// `bootstrap_cooldown`（竣工后防死循环）相互独立。
    bootstrap_rediff_cooldown: RwLock<FxHashMap<(NodeId, u8), Instant>>,
    /// v10：本端节点身份（双向引导冲突时按 node_id 字典序确定性让路）。
    local_node_id: NodeId,
    /// v10(A)：响应方活动标记 —— (peer, repo) → 最近一次响应对方 bootstrap 请求的时刻。
    /// 「对方在从我拉快照」的信号，用于响应方优先串行化。
    bootstrap_serving_at: RwLock<FxHashMap<(NodeId, u8), Instant>>,
    /// v10(A+)：让路截止 —— (peer, repo) → 让路保持到的时刻（冲突后持续让路防震荡）。
    bootstrap_yield_until: RwLock<FxHashMap<(NodeId, u8), Instant>>,
    /// v10(C)：块传输窗口状态机 —— (peer, repo) → 窗口化并发预取的在途/收齐状态。
    chunk_windows: RwLock<FxHashMap<(NodeId, u8), bootstrap::ChunkWindow>>,
    /// v9：delta 续拉未完成标记（(peer, repo)）。续拉发送失败时置位，下一 tick 不等间隔立即重试。
    delta_has_more: RwLock<FxHashSet<(NodeId, u8)>>,
    /// v9：检测到「对端 oplog 已被裁剪、中间段结构性缺失」的 (peer, repo)。
    /// 置位后该 repo 优先走 bootstrap/反熵，并在可观测性里暴露（旧实现是静默跳过 + lag 归零）。
    delta_gap: RwLock<FxHashSet<(NodeId, u8)>>,
    /// P1-4：range 反熵全局并发闸（请求/响应/拉/推 handler 共用），削平突发帧风暴。
    range_gate: Arc<tokio::sync::Semaphore>,
    /// 活锁治理(任务2)：重拉熔断 —— (peer, repo) → (连续重拉次数, 冷却到点)。
    /// 同一 (peer, repo) 连续 `bootstrap_repull_circuit_threshold` 次重拉清单且期间
    /// 无任何块成功落地 → 判定 bootstrap 停滞：置通道窗口 inactive、error! 告警、
    /// 冷却 `bootstrap_repull_circuit_cooldown_secs`（resume tick 见冷却直接跳过）。
    /// 任一块成功落地即清零计数并解除冷却。
    bootstrap_repull_circuit: RwLock<FxHashMap<(NodeId, u8), (u32, Option<Instant>)>>,
    /// 活锁治理(任务4)：range 让路的「bootstrap 停滞」WARN 已打标记（防刷屏）。
    /// 转入停滞打一次 WARN；恢复活跃时清除标记并打一次 INFO。
    bootstrap_stall_warned: std::sync::atomic::AtomicBool,
    /// 永动治理(任务5b)：各 repo 最近一次 bootstrap 竣工时刻（仅触发器用于竣工冷却）。
    /// 对端活表持续增长时，无冷却的差距触发会让 bootstrap 竣工后立即再开火，
    /// 永动挤占 range 让路窗口（2026-09-30 三节点实证）。
    #[allow(dead_code)] // H批：并行 WIP 尚未接线（构造器已初始化），待任务5b 落地后移除
    repo_done_at: RwLock<FxHashMap<u8, Instant>>,
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
            chunk_windows: RwLock::new(FxHashMap::default()),
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
            bootstrap_rediff_cooldown: RwLock::new(FxHashMap::default()),
            delta_has_more: RwLock::new(FxHashSet::default()),
            delta_gap: RwLock::new(FxHashSet::default()),
            range_gate: Arc::new(tokio::sync::Semaphore::new(RANGE_MAX_CONCURRENT_HANDLERS)),
            bootstrap_repull_circuit: RwLock::new(FxHashMap::default()),
            bootstrap_stall_warned: std::sync::atomic::AtomicBool::new(false),
            repo_done_at: RwLock::new(FxHashMap::default()),
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
            {
                let s = crate::federation::sync::channels_status::global();
                let mut g = s.write();
                g.delta.sync_entries_applied =
                    g.delta.sync_entries_applied.saturating_add(applied as u64);
            }
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
        // G2：发送统一加超时兜底（复用 transport_write_timeout_secs）—— 对端 writer 锁
        // 卡死时 send_message 无限 await 会把调度任务挂死，进而触发 watchdog/idle_timeout
        // 断连循环。超时与发送失败同路径：解除 in-flight，下一 tick 重发，绝不无限挂起。
        let send_res = tokio::time::timeout(
            Duration::from_secs(self.config.transport_write_timeout_secs.max(1)),
            conn.send_message(MessageType::OpsRequest, &req),
        )
        .await;
        match send_res {
            Ok(Ok(())) => {
                self.metrics.record_message_sent();
                debug!(
                    "[delta] 发起增量拉取: peer={}, repo={}, since_seq={}",
                    peer, repo, since
                );
            }
            Ok(Err(e)) => {
                warn!("[delta] 发送 OpsRequest 失败 peer={}: {}", peer, e);
                // v8 F4：发送失败不会有响应回来，立即解除 in-flight 以便下轮重试
                self.delta_inflight.write().remove(&(peer, repo));
            }
            Err(_) => {
                warn!(
                    "[delta] 发送 OpsRequest 超时 peer={}（{}s），解除 in-flight 待下轮重发",
                    peer, self.config.transport_write_timeout_secs
                );
                self.delta_inflight.write().remove(&(peer, repo));
            }
        }
    }

    /// 处理对端的增量拉取请求（数据服务器侧）
    ///
    /// 从本地 oplog 取 `seq > since_seq` 的变更（升序，最多 limit 条），组装批次回发。
    /// F8/v9：对端 ≥ 9 回 `OpsBatchV2`（ops 携带真实 LWW version）；v4-v8 回旧
    /// `OpsBatch`（wire 上无 version 字段，行为同改造前）。
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
        // F8/v9：响应方按对端协议版本选择批次消息形态。
        // - ≥9：OpsBatchV2 —— ops 每条携带真实 LWW version，接收端对「既有条目的版本提升」
        //   走正常 LWW（大者胜），不再被 version=0 语义静默丢弃；
        // - v4-v8：旧 OpsBatch —— wire 上无 version 字段，条目经 to_legacy_entries 降级，
        //   字节格式与行为同改造前完全一致。
        if conn.supports_delta_sync_v2() {
            let batch = OpsBatchV2Message {
                repo: req.repo,
                ops,
                next_seq,
                has_more,
                server_max_seq,
            };
            // G2：发送超时兜底（复用 transport_write_timeout_secs）——超时按发送失败计。
            let send_res = tokio::time::timeout(
                Duration::from_secs(self.config.transport_write_timeout_secs.max(1)),
                conn.send_message(MessageType::OpsBatchV2, &batch),
            )
            .await;
            match send_res {
                Ok(Ok(())) => {
                    self.metrics.record_message_sent();
                }
                Ok(Err(e)) => {
                    warn!("[delta] 发送 OpsBatchV2 失败 to={}: {}", conn.node_id, e);
                    crate::federation::sync::channels_status::global()
                        .write()
                        .delta
                        .batch_send_failures += 1;
                }
                Err(_) => {
                    warn!(
                        "[delta] 发送 OpsBatchV2 超时 to={}（{}s），按发送失败计",
                        conn.node_id, self.config.transport_write_timeout_secs
                    );
                    crate::federation::sync::channels_status::global()
                        .write()
                        .delta
                        .batch_send_failures += 1;
                }
            }
        } else {
            let batch = OpsBatchMessage {
                repo: req.repo,
                ops: delta::to_legacy_entries(&ops),
                next_seq,
                has_more,
                server_max_seq,
            };
            // G2：发送超时兜底（复用 transport_write_timeout_secs）——超时按发送失败计。
            let send_res = tokio::time::timeout(
                Duration::from_secs(self.config.transport_write_timeout_secs.max(1)),
                conn.send_message(MessageType::OpsBatch, &batch),
            )
            .await;
            match send_res {
                Ok(Ok(())) => {
                    self.metrics.record_message_sent();
                }
                Ok(Err(e)) => {
                    warn!("[delta] 发送 OpsBatch 失败 to={}: {}", conn.node_id, e);
                    crate::federation::sync::channels_status::global()
                        .write()
                        .delta
                        .batch_send_failures += 1;
                }
                Err(_) => {
                    warn!(
                        "[delta] 发送 OpsBatch 超时 to={}（{}s），按发送失败计",
                        conn.node_id, self.config.transport_write_timeout_secs
                    );
                    crate::federation::sync::channels_status::global()
                        .write()
                        .delta
                        .batch_send_failures += 1;
                }
            }
        }
    }

    /// delta 批次（V1/V2 共用）应用前段：in-flight 清理、水位记录、游标单调保护、空洞检测。
    ///
    /// 返回 `Some(next_seq)` 表示本批可应用；`None` 表示过期/重复批已被丢弃（单调保护）。
    /// V1（OpsBatch）与 V2（OpsBatchV2）在此之前的逻辑逐行同构，抽公共方法避免双份漂移。
    fn prepare_ops_batch(
        &self,
        conn: &PeerConn,
        repo: u8,
        server_max_seq: u64,
        next_seq: u64,
        batch_empty: bool,
    ) -> Option<u64> {
        // v8 F4：请求往返完成，清除 in-flight 标记（此后 tick 可发下一轮请求）。
        self.delta_inflight.write().remove(&(conn.node_id, repo));
        // F2：记录对端在本 repo 的 oplog 水位。它与本机记录的 synced_seq 同属对端 seq
        // 空间，二者相减才是「真实落后量」；无此值时 lag 报 null（不跨空间相减）。
        // v8：即使批已过期，水位也是最新信息，始终记录。
        if server_max_seq > 0 {
            self.delta_peer_max
                .write()
                .insert((conn.node_id, repo), server_max_seq);
        }
        // v8 F5：游标单调保护 —— 过期/重复批（next_seq ≤ 当前游标）直接丢弃：
        // 不重复应用、不推进、不续拉。否则重试风暴下乱序到达的旧批会把游标
        // 打回去（实测 1016764 → 1015764），再触发同区间无限重拉。
        let cur = self
            .delta_storage()
            .get_peer_seq(&conn.node_id.0, repo)
            .unwrap_or(0)
            .max(0) as u64;
        let next = delta::seq_to_i64(next_seq).max(0) as u64;
        if next <= cur {
            debug!(
                "[delta] 丢弃过期/重复批: peer={}, repo={}, next_seq={} ≤ 当前 {}（单调保护）",
                conn.node_id, repo, next_seq, cur
            );
            // v9：单调保护分支同样要清「续拉未完成」标记 —— 否则竣工后的空批
            // （next_seq == cur）会让标记永久粘滞，使 tick 永久绕过配置的拉取间隔。
            self.delta_has_more.write().remove(&(conn.node_id, repo));
            return None;
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
            .and_then(|v| v.iter().find(|s| s.repo == repo).map(|s| s.min_seq))
            .unwrap_or(0);
        if peer_min_seq > 0 && cur.saturating_add(1) < peer_min_seq {
            let missing = peer_min_seq.saturating_sub(cur).saturating_sub(1);
            warn!(
                "[delta] 检测到 oplog 空洞: peer={}, repo={}, 缺口约 {} 条（本地游标 {} < 对端 min_seq {}）→ 转 bootstrap 补齐",
                conn.node_id, repo, missing, cur, peer_min_seq
            );
            self.delta_gap.write().insert((conn.node_id, repo));
            crate::federation::sync::channels_status::global()
                .write()
                .delta
                .oplog_gap_detected = true;
        } else if batch_empty {
            // 空批 = 对端该 repo 已无更新可给 ⇒ 不存在待补空洞，清除标记
            // （旧写法只在「本批非空且间距小」时清除，空批会让标记永久粘滞）。
            self.delta_gap.write().remove(&(conn.node_id, repo));
        }
        Some(next)
    }

    /// delta 批次（V1/V2 共用）应用后段：幂等 apply → 推进游标 → 节流/续拉。
    ///
    /// V1 与 V2 唯一差异是 `entries` 的来源（旧 wire 无 version 置 0；V2 携带真实
    /// version 走 LWW），apply 及其后的推进/续拉逻辑完全同构。
    async fn finish_ops_batch(
        self: &Arc<Self>,
        conn: &Arc<PeerConn>,
        repo: u8,
        next_seq: u64,
        has_more: bool,
        entries: Vec<SyncEntry>,
    ) {
        if !entries.is_empty() {
            self.handle_sync_batch(repo, &entries);
        }
        // 联邦同步通道状态：oplog 水位 + 对端拉取水位（DB 查询在写锁外做）
        {
            let oplog_len = self.delta_storage().oplog_len().unwrap_or(0);
            let s = crate::federation::sync::channels_status::global();
            let mut g = s.write();
            g.delta.oplog_len = oplog_len;
            g.delta.since_seq = g.delta.since_seq.max(next_seq);
        }
        // 推进版本向量（仅前进，不回退）
        if let Err(e) =
            self.delta_storage()
                .set_peer_seq(&conn.node_id.0, repo, delta::seq_to_i64(next_seq))
        {
            warn!("[delta] 推进版本向量失败 peer={}: {}", conn.node_id, e);
        }
        // 拉取往返成功：把节流计时推后，避免同一轮里 tick 立刻重发；
        // v9：清掉「续拉未完成」标记（本轮已收到响应）。
        self.delta_request_at
            .write()
            .insert((conn.node_id, repo), Instant::now());
        self.delta_has_more.write().remove(&(conn.node_id, repo));
        if !entries.is_empty() || has_more {
            delta::log_applied(repo, entries.len(), next_seq);
        }

        // 还有更多：立即续拉下一批
        if has_more {
            let req = OpsRequestMessage {
                repo,
                since_seq: next_seq,
                limit: delta::DELTA_BATCH_LIMIT_DEFAULT,
            };
            // G2：发送超时兜底（复用 transport_write_timeout_secs）——超时与发送失败同路径，
            // 留下待续标记由 tick 重发，绝不无限挂起。
            let send_res = tokio::time::timeout(
                Duration::from_secs(self.config.transport_write_timeout_secs.max(1)),
                conn.send_message(MessageType::OpsRequest, &req),
            )
            .await;
            match send_res {
                Ok(Ok(())) => {
                    self.metrics.record_message_sent();
                    // v8 F4：续拉同样标记 in-flight（防与下一轮 tick 叠加）
                    self.delta_inflight
                        .write()
                        .insert((conn.node_id, repo), Instant::now());
                    self.delta_has_more.write().insert((conn.node_id, repo));
                }
                Ok(Err(e)) => {
                    warn!("[delta] 续拉 OpsRequest 失败 to={}: {}", conn.node_id, e);
                    // v9：续拉失败必须留下待续标记 —— 旧实现只 warn，而唯一的补救路径
                    // （tick 的节流重发）此前又被 `use_bootstrap → continue` 关闭，
                    // 于是任何一次写超时都会把该 (peer,repo) 的游标永久冻结。
                    if self.config.delta_retry_immediately {
                        self.delta_request_at.write().remove(&(conn.node_id, repo));
                        self.delta_has_more.write().insert((conn.node_id, repo));
                    }
                }
                Err(_) => {
                    warn!(
                        "[delta] 续拉 OpsRequest 超时 to={}（{}s），按发送失败留待续标记",
                        conn.node_id, self.config.transport_write_timeout_secs
                    );
                    if self.config.delta_retry_immediately {
                        self.delta_request_at.write().remove(&(conn.node_id, repo));
                        self.delta_has_more.write().insert((conn.node_id, repo));
                    }
                }
            }
        }
    }

    /// 处理对端的增量响应（请求方侧，旧通道 OpsBatch，对端 v4-v8）
    ///
    /// 幂等应用 ops（走既有 `handle_sync_batch`，**不写回 oplog**），推进本地版本向量；
    /// `has_more=true` 时立即续拉下一批，直到对端返回空批。
    pub async fn handle_ops_batch(self: Arc<Self>, conn: Arc<PeerConn>, batch: OpsBatchMessage) {
        if !self.config.delta_sync_enabled {
            return;
        }
        let Some(next) = self.prepare_ops_batch(
            &conn,
            batch.repo,
            batch.server_max_seq,
            batch.next_seq,
            batch.ops.is_empty(),
        ) else {
            return;
        };
        // 旧 wire 无 version 字段 → version 恒 0（对既有条目维持保守跳过语义）
        let entries = delta::ops_to_sync_entries(&batch.ops);
        self.finish_ops_batch(&conn, batch.repo, next, batch.has_more, entries)
            .await;
    }

    /// F8/v9：处理对端的增量响应 V2（请求方侧，OpsBatchV2，对端 ≥ v9）
    ///
    /// 与 [`Self::handle_ops_batch`] 唯一差异：ops 每条携带真实 LWW version，
    /// 转换时原样透传 → apply 走正常 LWW，「对既有条目的版本提升」不再被静默丢弃。
    /// 前段（水位/单调保护/空洞检测）与后段（推进/续拉）与 V1 完全共用。
    pub async fn handle_ops_batch_v2(
        self: Arc<Self>,
        conn: Arc<PeerConn>,
        batch: OpsBatchV2Message,
    ) {
        if !self.config.delta_sync_enabled {
            return;
        }
        let Some(next) = self.prepare_ops_batch(
            &conn,
            batch.repo,
            batch.server_max_seq,
            batch.next_seq,
            batch.ops.is_empty(),
        ) else {
            return;
        };
        let entries = delta::ops_to_sync_entries_v2(&batch.ops);
        self.finish_ops_batch(&conn, batch.repo, next, batch.has_more, entries)
            .await;
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
        // G2：发送超时兜底（复用 transport_write_timeout_secs）——绝不无限挂起协商 tick。
        let send_res = tokio::time::timeout(
            Duration::from_secs(self.config.transport_write_timeout_secs.max(1)),
            conn.send_message(MessageType::SyncNegotiate, &msg),
        )
        .await;
        match send_res {
            Ok(Ok(())) => {
                self.metrics.record_message_sent();
                info!(
                    "[negotiate] 已发送协商请求 to={}（存活 {}s）",
                    conn.node_id,
                    conn.connected_secs()
                );
            }
            Ok(Err(e)) => warn!("[negotiate] 发送协商请求失败 to={}: {}", conn.node_id, e),
            Err(_) => warn!(
                "[negotiate] 发送协商请求超时 to={}（{}s）",
                conn.node_id, self.config.transport_write_timeout_secs
            ),
        }
    }

    /// 直接从各 Repo 实现获取真实数据量（协商 / bootstrap 触发判定用）。
    ///
    /// 统一以 **DB 为唯一数据源**，口径与 `rest_api::federation_status_handler` 的
    /// `*_repo_total` 完全一致（顺序 NODE/PEER/INFOHASH/TRACKER）：
    /// - NODE     = dht_nodes(deleted_at IS NULL) + 内存写队列未落库部分
    /// - PEER     = peers(deleted_at IS NULL) + peers_archive（F9 方案 B：归档数据参与联邦同步）
    /// - INFOHASH = infohashes(deleted_at IS NULL)
    /// - TRACKER  = trackers(deleted_at IS NULL)
    ///
    /// 口径铁律：必须与 `load_repo_key_hashes_in_range`（bootstrap 清单扫描）一致。
    ///
    /// 口径沿革：
    /// - F1（2026-09-27 52/58 实测）：当时清单扫描只扫 peers 主表活行，本函数却把
    ///   archive 计入（58 报 40,042 vs 清单 28,009），差的部分快照永远拉不到 →
    ///   每 5 分钟全量重拉一轮的 BOOTSTRAP 死循环。当时修复 = 计数改为主表活行
    ///   （pick(1)），向清单口径看齐。
    /// - F9 方案 B（本次）：产品决策反转——归档是本地冷数据分层（存储优化），不是
    ///   数据边界，两节点都应拥有对方的归档行。清单扫描 PEER 分支改为
    ///   peers + peers_archive 双表合并（db.rs `query_peer_rows_both_tables`），块读取
    ///   同步扩到两表，本函数恢复 pick(1)+pick(2)。三口径（协商计数 / 清单行数 /
    ///   块数据）重新对齐为「两表并集」，同时消除了 F1 之前就存在的
    ///   「rest_api total 含归档 vs 清单不含」的隐性口径差。
    ///
    /// node 的 write_queue_len 是落库前瞬时差，自愈性偏差，保留。
    pub(crate) fn local_entry_counts(&self) -> Vec<u32> {
        let db = self.delta_storage().entity_counts_cached();
        let pick = |i: usize| -> u64 { db.get(i).copied().unwrap_or(-1).max(0) as u64 };
        let node = pick(0) + self.node_repo.write_queue_len_sync() as u64;
        // F9 方案 B：peer = peers 主表活行 + peers_archive 归档行（entity_counts 下标
        // [dht_nodes, peers, peers_archive, infohashes, trackers]，与清单口径严格一致）
        let peer = pick(1) + pick(2);
        vec![node as u32, peer as u32, pick(3) as u32, pick(4) as u32]
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

    /// E1：该 (peer, repo) 是否处于「运行时差异复检重触发」冷却期内。
    fn rediff_in_cooldown(&self, peer: &NodeId, repo: u8) -> bool {
        let mut m = self.bootstrap_rediff_cooldown.write();
        let ttl = self.config.bootstrap_rediff_cooldown_secs.max(1);
        m.retain(|_, t| t.elapsed().as_secs() < ttl);
        m.contains_key(&(*peer, repo))
    }

    /// E1：标记 (peer, repo) 刚因运行时差异超阈值重触发 bootstrap（写入冷却起点）。
    fn mark_rediff_triggered(&self, peer: &NodeId, repo: u8) {
        self.bootstrap_rediff_cooldown
            .write()
            .insert((*peer, repo), Instant::now());
    }

    // ========================================================================
    // 活锁治理（2026-09-30）：退避重试 / 重拉熔断 / 停滞判定
    // ========================================================================

    /// 活锁治理(任务1)：记录一次分块传输类失败（send 失败/超时/无会话）。
    /// 换块即重置连续失败计数；成功收到任意响应时由调用方整条清零。
    fn record_chunk_transport_fail(&self, peer: &NodeId, repo: u8, index: u32) {
        let mut m = self.bootstrap_chunk_attempt.write();
        let st = m
            .entry((*peer, repo))
            .or_insert(bootstrap::ChunkRetryState {
                index,
                fails: 0,
                last_fail_at: Instant::now(),
            });
        if st.index != index {
            st.index = index;
            st.fails = 0;
        }
        st.fails = st.fails.saturating_add(1);
        st.last_fail_at = Instant::now();
    }

    /// 活锁治理(任务1)：该 (peer, repo) 的退避是否已到期（可重发）。
    /// 无失败记录或失败已清零 → 立即放行；否则按 `backoff_delay` 指数退避表判断。
    fn chunk_backoff_ready(&self, peer: &NodeId, repo: u8) -> bool {
        let m = self.bootstrap_chunk_attempt.read();
        match m.get(&(*peer, repo)) {
            Some(st) if st.fails > 0 => {
                let delay = bootstrap::backoff_delay(
                    Duration::from_secs(self.config.bootstrap_backoff_base_secs.max(1)),
                    Duration::from_secs(self.config.bootstrap_backoff_max_secs.max(1)),
                    st.fails,
                );
                st.last_fail_at.elapsed() >= delay
            }
            _ => true,
        }
    }

    /// 活锁治理(任务2)：记录一次重拉（清单请求发起）。返回 true = 本次触发即熔断
    /// （连续次数达阈值且期间无任何块成功落地）。
    fn record_bootstrap_repull(&self, peer: &NodeId, repo: u8) -> bool {
        let threshold = self.config.bootstrap_repull_circuit_threshold.max(1);
        let mut tripped = false;
        {
            let mut m = self.bootstrap_repull_circuit.write();
            let e = m.entry((*peer, repo)).or_insert((0, None));
            // 仍在冷却中：不累计、不延长（到期后的一次重拉会立即重新熔断，
            // 形成「每冷却期至多一次试探」的慢速循环）
            if matches!(e.1, Some(until) if Instant::now() < until) {
                return false;
            }
            // 冷却已到期 → 解除冷却但**保留历史计数**：到期后的首次重拉即达阈值重新熔断
            if e.1.is_some() {
                e.1 = None;
            }
            e.0 = e.0.saturating_add(1);
            if e.0 >= threshold {
                e.1 = Some(
                    Instant::now()
                        + Duration::from_secs(
                            self.config.bootstrap_repull_circuit_cooldown_secs.max(1),
                        ),
                );
                tripped = true;
            }
        }
        if tripped {
            // 取证：读进度行与缓存清单，区分「清单都没拿到（重拉打空）」与
            // 「清单拿到了但块请求零落地（对端不回块/NAK/会话闪断）」两类停滞。
            let evidence = match self.delta_storage().bootstrap_load(&peer.0, repo) {
                Ok(Some((p, mf))) => {
                    let age_s = (chrono::Utc::now().timestamp_millis() - p.updated_ms) / 1000;
                    let mdesc = match mf {
                        Some(m) => format!("清单={}块/{}行", m.chunks.len(), m.total_rows),
                        None => "清单=无(未持久化)".to_string(),
                    };
                    format!(
                        "phase={} done={}/{} updated {}s 前 {}",
                        p.phase.as_str(),
                        p.done_chunks,
                        p.total_chunks,
                        age_s,
                        mdesc
                    )
                }
                _ => "无进度行".to_string(),
            };
            // 熔断动作：error! 告警 + 置通道窗口 inactive（防「bootstrap 永久 active」
            // 把 range 反熵永久让路）；进度行保留（冷却到期后可从断点续传）。
            error!(
                "[bootstrap] 重拉熔断：peer={} repo={} 连续 {} 次重拉且零块落地，判定停滞（{}）—— 置 inactive 并冷却 {}s（期间 resume tick 跳过，range 反熵接管）",
                peer,
                repo,
                threshold,
                evidence,
                self.config.bootstrap_repull_circuit_cooldown_secs
            );
            self.chunk_windows.write().remove(&(*peer, repo));
            let s = crate::federation::sync::channels_status::global();
            let mut g = s.write();
            g.bootstrap.active = false;
            g.bootstrap.inflight = 0;
            g.bootstrap.phase = "circuit_open".to_string();
            // E3：熔断终态复位方向。
            g.bootstrap.direction = String::new();
        }
        tripped
    }

    /// 活锁治理(任务2)：该 (peer, repo) 是否处于重拉熔断冷却期。
    fn bootstrap_repull_in_cooldown(&self, peer: &NodeId, repo: u8) -> bool {
        let m = self.bootstrap_repull_circuit.read();
        matches!(m.get(&(*peer, repo)), Some((_, Some(until))) if Instant::now() < *until)
    }

    /// 活锁治理(任务2)：任一块成功落地 → 清零该 (peer, repo) 的连续重拉计数并解除冷却。
    fn clear_bootstrap_repull(&self, peer: &NodeId, repo: u8) {
        self.bootstrap_repull_circuit.write().remove(&(*peer, repo));
    }

    /// 活锁治理(任务4)：转入停滞时打一次 WARN（返回 true = 本次调用打了 WARN）。
    fn mark_bootstrap_stall_warned(&self) -> bool {
        !self
            .bootstrap_stall_warned
            .swap(true, std::sync::atomic::Ordering::Relaxed)
    }

    /// 活锁治理(任务4)：恢复活跃时清除停滞标记（返回 true = 之前曾 WARN 过，
    /// 调用方补打一次 INFO）。
    fn clear_bootstrap_stall_warned(&self) -> bool {
        self.bootstrap_stall_warned
            .swap(false, std::sync::atomic::Ordering::Relaxed)
    }

    /// E1：巡检里挑出下一个「本地实时行数 vs 对端最近自报行数」已超 D2 双向阈值、
    /// 且无在途 bootstrap、不在 F4 竣工冷却 / E1 重触发冷却中的 (peer, repo) 候选。
    /// 纯判定（不含连接/发送 IO），便于单测。阈值直接复用 D2 的 `should_bootstrap_by_volume`
    /// （ratio>1.1 且 diff>50000，冷启动一端为 0 也算），不另写一套。
    fn next_rediff_candidate(
        &self,
        local: &[u32],
        states: &[(NodeId, Vec<RepoSyncState>)],
        running: &[bootstrap::BootstrapProgress],
    ) -> Option<(NodeId, u8)> {
        for (peer, repos) in states {
            for st in repos {
                let repo = st.repo;
                if !(repo_type::NODE..=repo_type::TRACKER).contains(&repo) {
                    continue;
                }
                let idx = (repo - repo_type::NODE) as usize;
                let local_n = local.get(idx).copied().unwrap_or(0) as u64;
                let remote_n = st.row_count;
                // 方向保护（修复 E1 双向反向拉数）：`should_bootstrap_by_volume` 是双向判定
                // （协商裁定 `decide_strategies` 仍按应答方视角保持双向，本体不动）；但巡检
                // 重触发若不辨方向，数据多的一方（如 422 万行）会因 ratio>1.1 向数据少的一方
                // （350 万行）反向发起 bootstrap —— 拉来的几乎全是对方已有行，纯浪费带宽还占槽。
                // 这里只在「本地少、对端多」（local_n < remote_n，本地确需向对端补数）才可能成候选；
                // local_n >= remote_n 直接跳过。冷启动 local=0 & remote>SNAPSHOT_MIN_ROWS 仍满足
                // local<remote 而触发；remote=0 & local>=SNAPSHOT_MIN_ROWS 因 local>=remote 被拦。
                if local_n >= remote_n {
                    continue;
                }
                if !Self::should_bootstrap_by_volume(local_n, remote_n) {
                    continue;
                }
                if self.bootstrap_running_fresh(running, *peer, repo) {
                    continue;
                }
                if self.snapshot_in_cooldown(peer, repo) {
                    continue;
                }
                if self.rediff_in_cooldown(peer, repo) {
                    continue;
                }
                return Some((*peer, repo));
            }
        }
        None
    }

    /// E1：bootstrap 中途断裂（OOM/消息超时）后的运行时差异复检 —— 用对端最近一次
    /// 协商自报行数（`peer_negotiate_state`，每次协商刷新）与本地实时行数再比一次，
    /// 超 D2 阈值即重触发一次 bootstrap。落点与 `check_and_trigger_bootstrap` 同一巡检
    /// 节流点（`BOOTSTRAP_CHECK_INTERVAL_SECS`），避免每次 resume tick 都做全表计数。
    ///
    /// 背景：旧 `decide_strategies` 只在连接建立/协商时裁定一次；bootstrap 因 OOM 中途
    /// 断掉后，后续协商永久落在 DELTA，差百万行只能靠 range 对账慢补。本方法把
    /// 「已竣工冷却 / 在途 bootstrap / 重触发冷却」三道闸都过了才点火。
    async fn rediff_bootstrap_recheck(self: &Arc<Self>) {
        if !self.config.bootstrap_enabled {
            return;
        }
        let local = self.local_entry_counts();
        let states: Vec<(NodeId, Vec<RepoSyncState>)> = {
            let g = self.peer_negotiate_state.read();
            g.iter().map(|(p, v)| (*p, v.clone())).collect()
        };
        if states.is_empty() {
            return;
        }
        let running = self.delta_storage().bootstrap_list().unwrap_or_default();
        let Some((peer, repo)) = self.next_rediff_candidate(&local, &states, &running) else {
            return;
        };
        if self.sessions.get_connection(&peer).is_none() {
            return;
        }
        self.mark_rediff_triggered(&peer, repo);
        let idx = (repo - repo_type::NODE) as usize;
        let local_n = local.get(idx).copied().unwrap_or(0) as u64;
        let remote_n = states
            .iter()
            .find(|(p, _)| *p == peer)
            .and_then(|(_, v)| v.iter().find(|s| s.repo == repo))
            .map(|s| s.row_count)
            .unwrap_or(0);
        info!(
            "[bootstrap] E1 运行时差异复检超 D2 阈值 local={} remote={} → 重触发 bootstrap: peer={} repo={}",
            local_n, remote_n, peer, repo
        );
        self.clone().start_bootstrap(peer, repo).await;
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

    /// D批(D2)：双向快照阈值判定 —— 不区分方向（我多或你多），差异够大即走 bootstrap。
    /// 冷启动（一端为 0、另一端有量）同样触发。双零与冷却期判定在调用处；本函数只负责
    /// 「是否按数据量走快照」，便于纯函数单测。
    fn should_bootstrap_by_volume(local: u64, peer_count: u64) -> bool {
        let ratio = local.max(peer_count) as f64 / local.min(peer_count).max(1) as f64;
        let diff = local.abs_diff(peer_count);
        (local == 0 && peer_count > SNAPSHOT_MIN_ROWS)
            || (peer_count == 0 && local >= SNAPSHOT_MIN_ROWS)
            || (ratio > SNAPSHOT_BIDIR_RATIO && diff > BOOTSTRAP_BIDIR_MIN_DIFF_ROWS)
    }

    /// v7：协商策略决策（Ack 发送方视角：为「对端应如何从我这里取数」裁定）。
    ///
    /// 规则（D批 D2 已改为双向）：
    /// - 双方皆空 → `NONE`；冷却期 → `DELTA`；
    /// - 一端为空另一端有量（冷启动），或 `max/min > 1.1` 且绝对差 > 50_000 行
    ///   （不区分方向，我多或你多都触发）→ `BOOTSTRAP`（大差集走快照）；
    /// - 其余 → `DELTA`（稳态水位续拉）。
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
                } else if Self::should_bootstrap_by_volume(local, peer_count) {
                    // D批(D2)：双向大差集（不区分方向）或冷启动 → 走 bootstrap 快照通道
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
        // G2：发送超时兜底（复用 transport_write_timeout_secs）——绝不无限挂起。
        let send_res = tokio::time::timeout(
            Duration::from_secs(self.config.transport_write_timeout_secs.max(1)),
            conn.send_message(MessageType::SyncNegotiateAck, &ack),
        )
        .await;
        match send_res {
            Ok(Ok(())) => self.metrics.record_message_sent(),
            Ok(Err(e)) => warn!("[negotiate] 发送 Ack 失败 to={}: {}", conn.node_id, e),
            Err(_) => warn!(
                "[negotiate] 发送 Ack 超时 to={}（{}s）",
                conn.node_id, self.config.transport_write_timeout_secs
            ),
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
        self.chunk_windows
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
        let permit = match self.range_gate.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
        // H3：执行体超时兜底 —— 到点只放闸、不中断对账：
        // 后台任务继续跑完，把存量差距补完；闸释放后别的 range 任务可立即进入。
        let range_handler_timeout = self.config.range_handler_timeout_secs;
        let __range_deadline = Duration::from_secs(range_handler_timeout.max(1));
        let __range_body = async move {
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
        };
        let join_handle = tokio::spawn(__range_body);
        // D1：执行体超时兜底收尾。H3：超时只放闸，不 abort 后台任务。
        match tokio::time::timeout(__range_deadline, join_handle).await {
            Ok(Ok(())) => {
                // 正常完成，permit 在函数返回时自然 drop
            }
            Ok(Err(join_err)) => {
                warn!("[range] reconcile_request 任务异常退出: {}", join_err);
            }
            Err(_) => {
                // H3：超时只放闸、不中断对账——permit 立即归还，后台任务继续跑完
                drop(permit);
                warn!(
                    "[range] reconcile_request 执行超过 {}s，释放并发闸（任务后台继续）",
                    range_handler_timeout
                );
                RANGE_HANDLER_TIMEOUTS.fetch_add(1, Ordering::Relaxed);
            }
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
        let permit = match self.range_gate.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
        // H3：执行体超时兜底 —— 到点只放闸、不中断对账（见 range_handler_timeout_secs）。
        let range_handler_timeout = self.config.range_handler_timeout_secs;
        let __range_deadline = Duration::from_secs(range_handler_timeout.max(1));
        let __range_body = async move {
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
                    let (local_only, remote_only) =
                        range_reconcile::key_diff(&local, &resp.entries);
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
                        let dedupe_key =
                            (conn.node_id, resp.repo, resp.lo.clone(), resp.hi.clone());
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
        };
        let join_handle = tokio::spawn(__range_body);
        // H3：超时只放闸，不 abort 后台任务——对账继续跑完，存量差距持续补齐。
        match tokio::time::timeout(__range_deadline, join_handle).await {
            Ok(Ok(())) => {
                // 正常完成，permit 在函数返回时自然 drop
            }
            Ok(Err(join_err)) => {
                warn!("[range] reconcile_response 任务异常退出: {}", join_err);
            }
            Err(_) => {
                // H3：超时只放闸、不中断对账——permit 立即归还，后台任务继续跑完
                drop(permit);
                warn!(
                    "[range] reconcile_response 执行超过 {}s，释放并发闸（任务后台继续）",
                    range_handler_timeout
                );
                RANGE_HANDLER_TIMEOUTS.fetch_add(1, Ordering::Relaxed);
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
        // H2：range 数据型消息发送用独立超时（range_send_timeout_secs），
        // 大块数据 5s 发不完会被当失败反复重试 → 收敛极慢。
        let send_res = tokio::time::timeout(
            Duration::from_secs(self.config.range_send_timeout_secs.max(1)),
            conn.send_message(
                crate::federation::protocol::MessageType::RangeReconcilePull,
                &msg,
            ),
        )
        .await;
        match send_res {
            Ok(Ok(())) => self.metrics.record_message_sent(),
            Ok(Err(e)) => warn!("[range] 发送按键拉取失败 to={}: {}", conn.node_id, e),
            Err(_) => warn!(
                "[range] 发送按键拉取超时 to={}（{}s）",
                conn.node_id, self.config.range_send_timeout_secs
            ),
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
        // H2：range 数据型消息发送用独立超时（range_send_timeout_secs）。
        let send_res = tokio::time::timeout(
            Duration::from_secs(self.config.range_send_timeout_secs.max(1)),
            conn.send_message(
                crate::federation::protocol::MessageType::RangeReconcilePush2,
                &msg,
            ),
        )
        .await;
        match send_res {
            Ok(Ok(())) => {
                self.metrics.record_message_sent();
                debug!(
                    "[range] 推送本地多数据 to={} repo={}: {} 条",
                    conn.node_id, repo, count
                );
            }
            Ok(Err(e)) => warn!("[range] 推送本地多数据失败 to={}: {}", conn.node_id, e),
            Err(_) => warn!(
                "[range] 推送本地多数据超时 to={}（{}s）",
                conn.node_id, self.config.range_send_timeout_secs
            ),
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
        let permit = match self.range_gate.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
        // H3：执行体超时兜底 —— 到点只放闸、不中断对账（见 range_handler_timeout_secs）。
        let range_handler_timeout = self.config.range_handler_timeout_secs;
        let __range_deadline = Duration::from_secs(range_handler_timeout.max(1));
        let __range_body = async move {
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
        };
        let join_handle = tokio::spawn(__range_body);
        // H3：超时只放闸，不 abort 后台任务——对账继续跑完。
        match tokio::time::timeout(__range_deadline, join_handle).await {
            Ok(Ok(())) => {
                // 正常完成，permit 在函数返回时自然 drop
            }
            Ok(Err(join_err)) => {
                warn!("[range] reconcile_pull 任务异常退出: {}", join_err);
            }
            Err(_) => {
                // H3：超时只放闸、不中断对账——permit 立即归还，后台任务继续跑完
                drop(permit);
                warn!(
                    "[range] reconcile_pull 执行超过 {}s，释放并发闸（任务后台继续）",
                    range_handler_timeout
                );
                RANGE_HANDLER_TIMEOUTS.fetch_add(1, Ordering::Relaxed);
            }
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
        let permit = match self.range_gate.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
        // H3：执行体超时兜底 —— 到点只放闸、不中断对账（见 range_handler_timeout_secs）。
        let range_handler_timeout = self.config.range_handler_timeout_secs;
        let __range_deadline = Duration::from_secs(range_handler_timeout.max(1));
        let __range_body = async move {
            if msg.repo < repo_type::NODE || msg.repo > repo_type::TRACKER || msg.entries.is_empty()
            {
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
                    {
                        let s = crate::federation::sync::channels_status::global();
                        let mut g = s.write();
                        g.delta.sync_entries_applied =
                            g.delta.sync_entries_applied.saturating_add(count as u64);
                    }
                }
                Err(e) => {
                    warn!("[range] 处理通用推送失败 from={}: {}", conn.node_id, e);
                }
            }
        };
        let join_handle = tokio::spawn(__range_body);
        // H3：超时只放闸，不 abort 后台任务——推送继续 apply 完，存量差距持续补齐。
        match tokio::time::timeout(__range_deadline, join_handle).await {
            Ok(Ok(())) => {
                // 正常完成，permit 在函数返回时自然 drop
            }
            Ok(Err(join_err)) => {
                warn!("[range] reconcile_push2 任务异常退出: {}", join_err);
            }
            Err(_) => {
                // H3：超时只放闸、不中断对账——permit 立即归还，后台任务继续跑完
                drop(permit);
                warn!(
                    "[range] reconcile_push2 执行超过 {}s，释放并发闸（任务后台继续）",
                    range_handler_timeout
                );
                RANGE_HANDLER_TIMEOUTS.fetch_add(1, Ordering::Relaxed);
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
        // 可抢占）：快照在途时反熵让路。
        // 活锁治理(任务4)：让路必须以「bootstrap 仍在推进」为前提 —— 现场实测三节点
        // 互拉全部卡死时 bootstrap 永久 active，让路逻辑使 range 反熵永不运行（唯一
        // 兜底通道失效）。现在距最近一次块成功落地超过 `bootstrap_stall_threshold_secs`
        // 即判定停滞：不再让路，转入停滞打一次 WARN（防刷屏），恢复活跃补一次 INFO。
        if self.bootstrap_transfer_active() {
            if self.bootstrap_transfer_stalled() {
                if self.mark_bootstrap_stall_warned() {
                    warn!(
                        "[range] bootstrap 距最近块落地超过 {}s 判定停滞，反熵不再让路（详见 /sync-observability bootstrap.stalled）",
                        self.config.bootstrap_stall_threshold_secs
                    );
                }
                // 不 return：继续执行本轮反熵
            } else {
                if self.clear_bootstrap_stall_warned() {
                    info!("[range] bootstrap 恢复推进，反熵重新让路");
                }
                debug!("[range] bootstrap 传输进行中，本轮反熵让路");
                return;
            }
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

    /// 活锁治理(任务4)：bootstrap 是否整体停滞 —— 存在非 Done 进度，但**没有任何**
    /// (peer, repo) 在 `bootstrap_stall_threshold_secs` 内成功落过块。
    ///
    /// 与 `bootstrap_transfer_active`（基于 `updated_ms`，会被清单往返刷新）不同，
    /// 本判定基于 `last_progress_ms`（仅块成功落地时推进）—— 活锁场景下重拉循环
    /// 会不断刷新 `updated_ms` 让 active 永真，但 `last_progress_ms` 暴露真实停滞。
    fn bootstrap_transfer_stalled(&self) -> bool {
        let rows = self.delta_storage().bootstrap_list().unwrap_or_default();
        let has_active = rows
            .iter()
            .any(|p| !matches!(p.phase, bootstrap::BootstrapPhase::Done));
        if !has_active {
            return false;
        }
        let now_ms = chrono::Utc::now().timestamp_millis();
        let threshold_ms = self.config.bootstrap_stall_threshold_secs.max(1) as i64 * 1000;
        !rows.iter().any(|p| {
            !matches!(p.phase, bootstrap::BootstrapPhase::Done)
                && !bootstrap::progress_stalled(
                    p.last_progress_ms,
                    p.updated_ms,
                    now_ms,
                    threshold_ms,
                )
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
            // H2：range 对账请求用独立超时（range_send_timeout_secs），
            // 避免大数据量 digest 计算+发送被 5s 超时反复打断。
            let send_res = tokio::time::timeout(
                Duration::from_secs(self.config.range_send_timeout_secs.max(1)),
                conn.send_message(MessageType::RangeReconcileRequest, &req),
            )
            .await;
            match send_res {
                Ok(Ok(())) => {
                    self.metrics.record_message_sent();
                }
                Ok(Err(e)) => {
                    warn!(
                        "[range] 发送 RangeReconcileRequest 失败 to={}（区间 {}/{}，进度保留待续跑）: {}",
                        conn.node_id,
                        i + 1,
                        total,
                        e
                    );
                    break;
                }
                Err(_) => {
                    warn!(
                        "[range] 发送 RangeReconcileRequest 超时 to={}（区间 {}/{}，{}s，进度保留待续跑）",
                        conn.node_id, i + 1, total, self.config.range_send_timeout_secs
                    );
                    break;
                }
            }
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
        // 联邦同步通道状态：镜像累计对账计数（原子量已在上方读出）
        {
            let s = crate::federation::sync::channels_status::global();
            let mut g = s.write();
            g.range_reconcile.leaf_compares = leaf_ranges;
            g.range_reconcile.local_extra = local_only;
            g.range_reconcile.remote_extra = remote_only;
            g.range_reconcile.repairs_triggered = repairs;
            g.range_reconcile.mode = if self.config.range_reconcile_diagnostic_only {
                "diagnostic".to_string()
            } else {
                "repair".to_string()
            };
        }
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
            crate::federation::sync::channels_status::global()
                .write()
                .range_reconcile
                .rounds_completed += 1;
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
            // G2：发送超时兜底（复用 transport_write_timeout_secs）——绝不无限挂起。
            let send_ok = tokio::time::timeout(
                Duration::from_secs(self.config.transport_write_timeout_secs.max(1)),
                conn.send_message(MessageType::BootstrapManifestResponse, &resp),
            )
            .await
            .map(|r| r.is_ok())
            .unwrap_or(false);
            if send_ok {
                self.metrics.record_message_sent();
            } else {
                warn!(
                    "[bootstrap] 发送缓存清单失败/超时 to={}（{}s）",
                    conn.node_id, self.config.transport_write_timeout_secs
                );
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
                // G2：发送超时兜底（复用 transport_write_timeout_secs）——绝不无限挂起。
                let _ = tokio::time::timeout(
                    Duration::from_secs(self.config.transport_write_timeout_secs.max(1)),
                    conn.send_message(MessageType::BootstrapManifestResponse, &resp),
                )
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
                                                 // v11：全表扫描移入 spawn_blocking —— `build_repo_manifest_impl` 是全表排序扫描，
                                                 // 259 万行表实测 ~38s，此前在异步 handler 里同步执行会把当前 tokio worker 线程
                                                 // 整段阻塞（其他连接的收发/心跳全被拖住）。移入 blocking 线程池后，重建期间
                                                 // 后续到达的请求仍走上方「rebuilding 租约 + 旧缓存/NAK 回退」逻辑（已先行写入
                                                 // rebuilding 标记），不受影响。
        let chunk_rows = self.config.bootstrap_chunk_rows;
        let repo = req.repo;
        let manifest = match tokio::task::spawn_blocking(move || {
            bootstrap::build_repo_manifest_impl(&storage, repo, chunk_rows, w0, version)
        })
        .await
        {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => {
                warn!("[bootstrap] 建清单失败 repo={}: {}", req.repo, e);
                self.bootstrap_rebuild_at.write().remove(&key);
                return;
            }
            Err(e) => {
                // blocking 任务 join 失败（panic/取消）按建清单失败同一路径处理，
                // 释放重建标记让下一轮请求可重试
                warn!("[bootstrap] 建清单任务 join 失败 repo={}: {}", req.repo, e);
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
        // G2：发送超时兜底（复用 transport_write_timeout_secs）——绝不无限挂起。
        let send_res = tokio::time::timeout(
            Duration::from_secs(self.config.transport_write_timeout_secs.max(1)),
            conn.send_message(MessageType::BootstrapManifestResponse, &resp),
        )
        .await;
        match send_res {
            Ok(Ok(())) => {
                self.metrics.record_message_sent();
            }
            Ok(Err(e)) => warn!("[bootstrap] 发送清单失败: {}", e),
            Err(_) => warn!(
                "[bootstrap] 发送清单超时（{}s）",
                self.config.transport_write_timeout_secs
            ),
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
        // G2：发送超时兜底（复用 transport_write_timeout_secs）——绝不无限挂起。
        let send_res = tokio::time::timeout(
            Duration::from_secs(self.config.transport_write_timeout_secs.max(1)),
            conn.send_message(MessageType::BootstrapChunkResponse, &resp),
        )
        .await;
        match send_res {
            Ok(Ok(())) => {
                self.metrics.record_message_sent();
            }
            Ok(Err(e)) => warn!("[bootstrap] 发送 NAK 失败 to={}: {}", conn.node_id, e),
            Err(_) => warn!(
                "[bootstrap] 发送 NAK 超时 to={}（{}s）",
                conn.node_id, self.config.transport_write_timeout_secs
            ),
        }
    }

    /// A2/A3：记录一次 bootstrap 块发送失败 —— 全局失败累计 +1，per-peer 连续失败 +1。
    /// 达阈值时该 peer 进入指数退避熔断（后续块请求在 handler 入口被 NAK("circuit open")）。
    fn record_bootstrap_send_failure(&self, peer: &NodeId) {
        BOOTSTRAP_SEND_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
        let threshold = self.config.bootstrap_send_circuit_break_threshold.max(1);
        let prev = send_circuits()
            .read()
            .get(peer)
            .copied()
            .unwrap_or(SendCircuitState {
                fails: 0,
                marked_at: None,
            });
        let next = prev.record_failure(Instant::now());
        if next.fails >= threshold {
            warn!(
                "[bootstrap] 对端 {} 连续发送失败 {} 次，进入熔断退避（新块请求 NAK circuit open）",
                peer, next.fails
            );
        }
        send_circuits().write().insert(*peer, next);
    }

    /// A5：发送方 idle 看门狗 —— 移除超过 `bootstrap_send_idle_timeout_secs` 无新块请求的
    /// (peer,repo)「正在服务」标记；若全局通道方向仍为 send 且对端已停止拉取，复位 direction。
    fn prune_idle_send_serving(&self) {
        let idle = Duration::from_secs(self.config.bootstrap_send_idle_timeout_secs.max(1));
        let now = Instant::now();
        let mut pruned: Vec<NodeId> = Vec::new();
        {
            let mut m = self.bootstrap_serving_at.write();
            m.retain(|(peer, _repo), last| {
                let keep = !serving_entry_expired(*last, now, idle);
                if !keep {
                    pruned.push(*peer);
                }
                keep
            });
        }
        if pruned.is_empty() {
            return;
        }
        // 仅当面板方向仍为 send、且被 pruning 的对端与全局 peer_id 一致时复位 direction。
        let s = crate::federation::sync::channels_status::global();
        let mut g = s.write();
        if g.bootstrap.direction == "send" {
            for p in &pruned {
                if p.to_hex() == g.bootstrap.peer_id {
                    g.bootstrap.direction = String::new();
                    debug!(
                        "[bootstrap] 发送 idle {}s 无新块请求，复位 direction: peer={}",
                        idle.as_secs(),
                        p
                    );
                    break;
                }
            }
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
        // A3：per-peer 发送熔断 —— 该对端连续发送失败达阈值且指数退避未到期时，
        // 直接 NAK("circuit open")，不加载 entries、不占内存（半开探测由成功/失败推进）。
        {
            let base = Duration::from_secs(self.config.bootstrap_backoff_base_secs.max(1));
            let max = Duration::from_secs(self.config.bootstrap_backoff_max_secs.max(1));
            let open = send_circuits()
                .read()
                .get(&conn.node_id)
                .map(|s| {
                    s.is_open(
                        self.config.bootstrap_send_circuit_break_threshold,
                        Instant::now(),
                        base,
                        max,
                    )
                })
                .unwrap_or(false);
            if open {
                self.send_bootstrap_nak(&conn, req.repo, req.index, "circuit open")
                    .await;
                return;
            }
        }
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
                // v11：同 manifest_request —— 现场重建是全表扫描（259 万行实测 ~38s），
                // 移入 spawn_blocking 避免阻塞 tokio worker 线程；重建期间后续块请求
                // 仍走上方租约 NAK 路径（rebuilding 标记已先行写入，不破坏）。
                let chunk_rows = self.config.bootstrap_chunk_rows;
                let repo = req.repo;
                let rebuilt = tokio::task::spawn_blocking(move || {
                    bootstrap::build_repo_manifest_impl(&storage, repo, chunk_rows, w0, version)
                })
                .await;
                match rebuilt {
                    Ok(Ok(m)) => {
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
                        // G2：发送超时兜底（复用 transport_write_timeout_secs）。
                        let send_ok = tokio::time::timeout(
                            Duration::from_secs(self.config.transport_write_timeout_secs.max(1)),
                            conn.send_message(MessageType::BootstrapManifestResponse, &resp),
                        )
                        .await
                        .map(|r| r.is_ok())
                        .unwrap_or(false);
                        if send_ok {
                            self.metrics.record_message_sent();
                        }
                        return;
                    }
                    Ok(Err(e)) => {
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
                    Err(e) => {
                        // blocking 任务 join 失败（panic/取消）按重建失败同一路径处理，
                        // 回 NAK 让请求方走可计数、可升级的失败路径
                        self.bootstrap_rebuild_at.write().remove(&key);
                        warn!(
                            "[bootstrap] 收到分块请求且清单重建任务 join 失败 peer={}, repo={}: {}",
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
        // A1：发送并发上限 —— 必须早于 entries 加载（这才是内存堆积窗口）。
        // 在途计数覆盖「加载 entries → 发送完成」整段；拿不到立即 NAK、不排队。
        // `_send_permit` 为 RAII：发送完成或中途任何 early-return（含取数失败的 NAK）都释放槽。
        BOOTSTRAP_SEND_TOTAL.fetch_add(1, Ordering::Relaxed);
        let send_limit = effective_send_limit(self.config.bootstrap_send_concurrency);
        if !try_inc_bounded(&BOOTSTRAP_SEND_IN_FLIGHT, send_limit) {
            self.send_bootstrap_nak(&conn, req.repo, req.index, "send concurrency limit")
                .await;
            return;
        }
        let _send_permit = BootstrapSendPermit;
        // E3：发送方主路径 —— 通道方向标记为 send（面板据此区分收发方向）。
        {
            let s = crate::federation::sync::channels_status::global();
            let mut g = s.write();
            g.bootstrap.active = true;
            g.bootstrap.direction = "send".to_string();
            g.bootstrap.peer_id = conn.node_id.to_hex();
            g.bootstrap.repo = req.repo as u64;
        }
        let lo = Self::range_bound(&chunk.lo).map(|b| b.to_vec());
        let hi = Self::range_bound(&chunk.hi).map(|b| b.to_vec());
        let storage = self.delta_storage();
        // D批(D3)：删除对端「取块哈希」第二次 SELECT —— 客户端从 manifest 已知块 hash，
        // 对端不必重算 blake3。传输完整性由 verify_transport + Range 反熵兜底（见 P0-4：
        // 落地后重算本地区间摘要必然因活表 ~2% 独立行差异失配 → 3 次失败重拉清单死循环）。
        // 响应线协议仍保留 hash 字段，但客户端从不读取，固定填零占位。
        let hash = [0u8; 32];
        // ② 完整条目（含 payload，供接收方批量 upsert）—— v7 全 repo 通用
        // v12：同步 SQLite 区间查询在 async handler 里会阻塞 tokio worker（叠加读池空回退写锁
        // 曾致 62 侧 30s write timeout 重试循环）—— 移入 spawn_blocking，worker 不再被阻塞。
        // lo/hi 提升为 owned Vec（原 &[u8] 借用 chunk 非 'static，无法进 'static 闭包）。
        let max_rows = chunk.rows as usize;
        let repo = req.repo;
        let entries: Vec<SyncEntry> = match tokio::task::spawn_blocking(move || {
            storage.load_repo_sync_entries_in_range(
                repo,
                lo.as_deref(),
                hi.as_deref(),
                max_rows + 1,
            )
        })
        .await
        {
            Ok(Ok(rows)) => rows.into_iter().take(max_rows).collect(),
            Ok(Err(e)) => {
                warn!("[bootstrap] 取块条目失败 index={}: {}", req.index, e);
                self.send_bootstrap_nak(&conn, req.repo, req.index, "load chunk entries failed")
                    .await;
                return;
            }
            Err(e) => {
                // blocking 任务 join 失败（panic/取消）按取数失败同一路径处理，
                // 回 NAK 让请求方走可计数、可升级的失败路径
                warn!(
                    "[bootstrap] 取块条目任务 join 失败 index={}: {}",
                    req.index, e
                );
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
        // A2：单块发送超时兜底 —— 超时按失败处理（warn + 失败计数 + per-peer 熔断累计），
        // 不重试、不堆积。_send_permit 在本函数返回时释放（无论成功/超时/发送 Err）。
        let send_deadline = Duration::from_secs(self.config.bootstrap_send_timeout_secs.max(1));
        let send_result = tokio::time::timeout(
            send_deadline,
            conn.send_message(MessageType::BootstrapChunkResponse, &resp),
        )
        .await;
        match send_result {
            Ok(Ok(())) => {
                self.metrics.record_message_sent();
                // A3：成功发送一次即清零该对端连续失败计数并解除熔断。
                send_circuits().write().remove(&conn.node_id);
            }
            Ok(Err(e)) => {
                warn!("[bootstrap] 发送块 {} 失败: {}", req.index, e);
                self.record_bootstrap_send_failure(&conn.node_id);
            }
            Err(_) => {
                warn!(
                    "[bootstrap] 发送块 {} 超过 {}s 超时，按失败处理（不重试、不堆积）",
                    req.index,
                    send_deadline.as_secs()
                );
                self.record_bootstrap_send_failure(&conn.node_id);
            }
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
        // 停摆修复（2026-09-30 第二轮）：退避期 / 熔断冷却期内丢弃清单响应。
        // 本 handler 后续的 D3 对齐验证是全表扫描（spawn_blocking 但全程持有全局
        // SQLite 连接锁）。「继承全覆盖 → 直接竣工 → 校验失败 → 重拉」循环在旧实现下
        // 按发起互斥 TTL（60s）固定节奏每轮跑 1~2 次扫描，把 api_runtime 的 DB 只读
        // handler 全部 park 在连接锁上直至 worker 耗尽（现场实证 /metrics、/health 全超时）。
        // 在任何重 IO 之前拦下，让重试节奏服从指数退避表（30s 起倍增，600s 封顶）。
        if !self.chunk_backoff_ready(&conn.node_id, mf.repo) {
            warn!(
                "[bootstrap] 传输退避期内丢弃清单响应（抑制全表扫描风暴）: peer={} repo={} 总行={} 块数={}",
                conn.node_id,
                mf.repo,
                mf.total_rows,
                mf.chunks.len()
            );
            return;
        }
        if self.bootstrap_repull_in_cooldown(&conn.node_id, mf.repo) {
            warn!(
                "[bootstrap] 重拉熔断冷却期内丢弃清单响应: peer={} repo={}",
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
        let (same_done, resume_src) = match &loaded {
            Some((p, _)) if p.peer.as_slice() == conn.node_id.0 && p.version == mf.version => {
                (p.done_chunks.min(mf.chunks.len() as u64), "version")
            }
            Some((p, _)) if p.peer.as_slice() == conn.node_id.0 => {
                let idx = bootstrap::locate_resume_index(&mf.chunks, p.last_key.as_deref());
                (idx.min(mf.chunks.len() as u64), "last_key")
            }
            _ => (0, "none"),
        };
        // D批(D1)/D批(D3)：块级 hash 比较 + 断点继承前缀逐块验证。
        // skip=true 时按相同 chunk_rows 重建本地清单（全表扫描，spawn_blocking），用纯函数
        // `align_bootstrap_seed` 算出「全部 hash&rows 一致块集合」与「验证通过的连续前缀」：
        //   - seed = 全部一致块（前缀块一旦与本地不一致即被剔除，落入待拉集合由窗口补发）；
        //   - done_chunks = 验证通过的连续前缀（不再无条件信任旧 done_chunks）；
        //   - 本地清单重建失败 → 完全不继承（seed 空、done=0，退回全量拉取）。
        // skip=false 时保留旧行为：无条件继承 0..same_done。
        // 活锁治理(任务1-b/任务3)：新旧清单漂移判定 —— total_rows 变化 > 配置百分比
        // 判**结构性漂移**（重拉的第二个合法触发证据：对端数据量级已变，旧断点无意义，
        // 允许从零重传并告警）；total_chunks 差 ≤ 容差则旧进度可按 index 继承。
        let (old_rows, old_chunks, old_done) = match &loaded {
            Some((p, Some(om))) if p.peer.as_slice() == conn.node_id.0 => {
                (om.total_rows, om.chunks.len() as u64, p.done_chunks)
            }
            _ => (0, 0, 0),
        };
        let total_chunks = mf.chunks.len() as u32;
        let (structural_drift, inheritable) = bootstrap::manifest_drift(
            old_rows,
            old_chunks,
            mf.total_rows,
            total_chunks as u64,
            self.config.bootstrap_structural_drift_rows_percent,
            self.config.bootstrap_inherit_tolerance_percent,
        );
        if structural_drift {
            warn!(
                "[bootstrap] 结构性漂移：total_rows {} → {}（变化 >{}%），旧进度作废从零重传（块内容幂等重传安全）: peer={} repo={}",
                old_rows,
                mf.total_rows,
                self.config.bootstrap_structural_drift_rows_percent,
                conn.node_id,
                mf.repo
            );
        }
        let do_verify = self.config.bootstrap_skip_identical_chunks;
        let (mut seed, done_chunks): (std::collections::HashSet<u32>, u64) = if do_verify {
            let storage = self.delta_storage();
            let w0 = storage.oplog_max_seq_for_repo(mf.repo).unwrap_or(0).max(0) as u64;
            let version = w0.wrapping_add(1) as u32;
            let chunk_rows = mf.chunk_rows;
            let repo = mf.repo;
            let built = tokio::task::spawn_blocking(move || {
                bootstrap::build_repo_manifest_impl(&storage, repo, chunk_rows, w0, version)
            })
            .await;
            match built {
                Ok(Ok(local_mf)) => {
                    let (prefix, matching) = bootstrap::align_bootstrap_seed(&mf.chunks, &local_mf);
                    (matching, prefix as u64)
                }
                Ok(Err(e)) => {
                    warn!(
                        "[bootstrap] 本地清单重建失败，不继承任何断点、退回全量拉取: repo={} peer={}: {}",
                        mf.repo, conn.node_id, e
                    );
                    (std::collections::HashSet::new(), 0)
                }
                Err(e) => {
                    warn!(
                        "[bootstrap] 本地清单重建任务 join 失败，不继承任何断点、退回全量拉取: repo={} peer={}: {}",
                        mf.repo, conn.node_id, e
                    );
                    (std::collections::HashSet::new(), 0)
                }
            }
        } else {
            // skip=false：无条件继承旧进度（旧行为，不做本地校验）
            ((0..same_done as u32).collect(), same_done)
        };
        // 活锁治理(任务3)：重拉进度继承 —— 新旧清单 total_chunks 差 ≤ 容差（非结构性漂移）
        // 时，旧进度（已完成块）按 index 继承不归零，与 D3 验证前缀取 max。
        // 继承块同时并入 skip 集合，保证窗口 received 与持久化 done_chunks 一致
        // （不再重发已继承块；未真正落地的残留差异由竣工对齐校验与 range 反熵兜底）。
        let inherited_done = bootstrap::inherit_done_chunks(
            old_done,
            total_chunks as u64,
            inheritable && !structural_drift,
            done_chunks,
        );
        if inherited_done > done_chunks {
            for i in 0..inherited_done as u32 {
                seed.insert(i);
            }
            info!(
                "[bootstrap] 重拉进度继承（任务3）：旧进度 done={} 按 index 继承（新清单 {} 块，差异 ≤{}%）: peer={} repo={}",
                old_done,
                total_chunks,
                self.config.bootstrap_inherit_tolerance_percent,
                conn.node_id,
                mf.repo
            );
        }
        let done_chunks = inherited_done;
        let now = chrono::Utc::now().timestamp_millis();
        let mut progress = bootstrap::BootstrapProgress::new(mf.repo, conn.node_id.0.to_vec(), now);
        progress.phase = bootstrap::BootstrapPhase::Transfer;
        progress.version = mf.version;
        progress.w0_seq = mf.w0_seq;
        progress.total_chunks = mf.chunks.len() as u64;
        progress.done_chunks = done_chunks;
        if done_chunks > 0 || !seed.is_empty() {
            info!(
                "[bootstrap] 断点继承/对齐验证（来源 {}）verified_done={}/{} 跳过/继承块={}: peer={} repo={}",
                resume_src,
                done_chunks,
                mf.chunks.len(),
                seed.len(),
                conn.node_id,
                mf.repo
            );
        }
        // v10(B2)/D批(D1)/D批(D3)：对齐验证覆盖全部块（seed 逐块 hash&rows 与本地一致 = 全量）
        // → 直接竣工，不再发越界块请求空转；竣工推进游标并切 delta 追尾。
        // 活锁治理(任务3)：seed 现在还可能含「按 index 继承」的旧进度块（未经本轮 hash
        // 验证）；全覆盖触发的直接竣工仍要过 finish_bootstrap 的竣工前对齐校验
        // （静态表严格逐块全等 / 活表行数 ≥95% 兜底），假进度不会写 Done。
        if !mf.chunks.is_empty() && seed.len() >= total_chunks as usize {
            info!(
                "[bootstrap] 对齐验证快照已全部落地（逐块 hash/rows 一致），直接竣工: peer={} repo={}",
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
            "[bootstrap] 收到清单 from={}: 总行={}, 块数={}, w0={}, 续传前缀={}, hash一致跳过块={}",
            conn.node_id,
            mf.total_rows,
            mf.chunks.len(),
            mf.w0_seq,
            progress.done_chunks,
            seed.len()
        );

        // 联邦同步通道状态：清单到达 → bootstrap 活跃
        {
            let s = crate::federation::sync::channels_status::global();
            let mut g = s.write();
            g.bootstrap.active = true;
            g.bootstrap.peer_id = conn
                .node_id
                .0
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect();
            g.bootstrap.repo = mf.repo as u64;
            g.bootstrap.total_chunks = mf.chunks.len() as u64;
            g.bootstrap.done_chunks = progress.done_chunks;
            g.bootstrap.phase = "transfer".to_string();
            g.bootstrap.skipped_identical = seed.len() as u64;
            // E3：接收方（我们在拉对端快照）标记方向为 recv。
            g.bootstrap.direction = "recv".to_string();
        }
        // v10(C)：窗口化并发预取 —— 建立窗口状态机（resume_from = 继承的连续前缀），
        // fill 出首批在途块并批量请求。旧实现链式传输（收一块才请求下一块）把吞吐
        // 钉死在单块「生成+传输+RTT」线性叠加（实测 0.33MB/s）；窗口化后吞吐随
        // 窗口扩大，直至撞上落库/磁盘上限。窗口大小配置化（bootstrap_window_size）。
        let window_size = self.config.bootstrap_window_size.max(1);
        // D批(D1)/D批(D3)：以 seed（逐块验证 hash&rows 一致块）初始化窗口，只请求剩余差异块。
        // done_chunks 为**验证通过**的连续前缀，非连续 skip 不持久化（重启后由清单响应重算）。
        let mut cw = bootstrap::ChunkWindow::with_skip(total_chunks, window_size, &seed);
        let first_batch = cw.fill(0);
        self.chunk_windows
            .write()
            .insert((conn.node_id, mf.repo), cw);
        info!(
            "[bootstrap] 窗口化传输启动: peer={} repo={} 块数={} 窗口={} 首批={}",
            conn.node_id,
            mf.repo,
            mf.chunks.len(),
            window_size,
            first_batch.len()
        );
        for idx in &first_batch {
            self.request_bootstrap_chunk(&conn, mf.repo, *idx, &mf)
                .await;
        }
    }

    /// P2-1：处理对端的 bootstrap 分块响应（请求方）—— 批量 upsert 落块、校验、窗口续拉或切追尾。
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
        // v10(C)：窗口状态机 —— (peer,repo) 的 ChunkWindow 缺失（重启/首次响应）
        // 时从进度重建（resume_from = 持久化的连续前缀 done_chunks）。
        // 全程只取一次、驱动完状态迁移后回插，不再二次取出（旧代码两次 remove 导致在途状态丢失）。
        let window_size = self.config.bootstrap_window_size.max(1);
        let mut cw = self
            .chunk_windows
            .write()
            .remove(&(conn.node_id, resp.repo))
            .unwrap_or_else(|| {
                bootstrap::ChunkWindow::new(
                    mf.chunks.len() as u32,
                    window_size,
                    progress.done_chunks.min(mf.chunks.len() as u64) as u32,
                )
            });
        // ④ 批量 upsert（A4：走全 repo 通用 dispatch handle_sync_batch，按 resp.repo 分派到
        // apply_node/peer/infohash/tracker_sync，严禁逐条 INSERT，也严禁硬编码走 NODE 落地）
        if !resp.entries.is_empty() {
            self.handle_sync_batch(resp.repo, &resp.entries);
            // v10(C)：刷新快照导入窗口 —— 落库预算在窗口内解除时间片限速
            // （writes_per_tick ×200），窗口静默 30s 自动回落稳态平滑语义。
            crate::storage::io_scheduler::refresh_bootstrap_import_window(Duration::from_secs(
                crate::storage::io_scheduler::BOOTSTRAP_IMPORT_WINDOW_TTL_SECS,
            ));
        }
        // ⑥ 校验（P0-4 语义修正）：只做「传输完整性」校验 —— 校验**对端发来的这一批条目**
        // 是否完整到达，不再重算本地 [lo,hi) 区间摘要与清单 hash 比对。
        let ok = bootstrap::verify_transport(chunk.rows, resp.entries.len());
        if ok {
            // D批(D3)：对端负载保护 —— 每收到一个完成块，按配置节流 sleep，避免 16 路窗口
            // 满速回包把对端正常业务挤爆（工程约束「同步不能影响对端正常运行」）。
            let d = self.config.bootstrap_peer_protect_delay_ms;
            if d > 0 {
                tokio::time::sleep(Duration::from_millis(d)).await;
            }
            // 成功路径：统计字节、记录 key 游标、推进阶段。
            // done_chunks 不在此处按链式语义 (index+1) 推进 —— 窗口化乱序到达，
            // done_chunks 必须等于 cw.done_prefix()（最大连续前缀），否则持久化进度会跳号。
            self.bootstrap_verify_fails
                .write()
                .remove(&(conn.node_id, resp.repo));
            // 停摆修复：成功响应同样清退避状态 —— 收到任何块响应都证明链路活着；
            // 否则窗口竣工进入 finish_bootstrap 时可能带着陈旧失败计数，被
            // 「退避期内跳过竣工校验」短路段误拦，快照永远差最后一块写不了 Done。
            self.bootstrap_chunk_attempt
                .write()
                .remove(&(conn.node_id, resp.repo));
            // 活锁治理(任务4)：块成功落地 → touch 停滞判定时钟；
            // 活锁治理(任务2)：任一块成功落地 → 清零重拉熔断计数并解除冷却。
            progress.last_progress_ms = chrono::Utc::now().timestamp_millis();
            self.clear_bootstrap_repull(&conn.node_id, resp.repo);
            progress.last_key = Some(chunk.hi.clone());
            progress.bytes += resp
                .entries
                .iter()
                .map(|e| (e.key.len() + e.payload.len() + 16) as u64)
                .sum::<u64>();
            progress.phase = bootstrap::BootstrapPhase::Transfer;
            // v10(C)：驱动窗口状态机。on_response 返回 true = 全部块收齐（竣工）。
            if cw.on_response(resp.index) {
                progress.done_chunks = cw.done_prefix() as u64;
                progress.updated_ms = chrono::Utc::now().timestamp_millis();
                let _ = self.delta_storage().bootstrap_save(&progress, None);
                self.finish_bootstrap(&conn, resp.repo, mf.w0_seq).await;
                return;
            }
        } else {
            // 失败路径（对端 NAK / 空块 / 传输漂移）：不做字节统计，记录告警后驱动窗口重试。
            warn!(
                "[bootstrap] 块 {} 传输校验失败（声明 {} 行 / 实收 {} 行），保持进度 done={}/{}",
                resp.index,
                chunk.rows,
                resp.entries.len(),
                cw.done_prefix(),
                mf.chunks.len()
            );
            // 活锁治理(任务1-a)：唯一保留的块级重拉触发 —— 对端回带**非零** hash 且与
            // 清单块 hash 不符 = 真实数据漂移证据（当前服务端恒填零占位，本分支为协议
            // 预留；真漂移证据主走竣工前 D3 对齐校验路径，见 finish_bootstrap）。
            if resp.hash != [0u8; 32] && resp.hash != chunk.hash {
                warn!(
                    "[bootstrap] 块 {} hash 校验失败（真实数据漂移证据），重拉清单: repo={} peer={}",
                    resp.index, resp.repo, conn.node_id
                );
                progress.done_chunks = cw.done_prefix() as u64;
                progress.updated_ms = chrono::Utc::now().timestamp_millis();
                let _ = self.delta_storage().bootstrap_save(&progress, None);
                self.chunk_windows
                    .write()
                    .insert((conn.node_id, resp.repo), cw);
                self.record_bootstrap_repull(&conn.node_id, resp.repo);
                self.clone().start_bootstrap(conn.node_id, resp.repo).await;
                return;
            }
            let attempts = cw.on_failure(resp.index);
            if attempts >= self.config.bootstrap_chunk_max_attempts.max(1) {
                // 活锁治理(任务1)：连续失败**不再升级重拉清单**（旧实现此处 start_bootstrap，
                // 与会话闪断叠加形成「重拉→清零→再失败」死循环，单节点一天 1846 次）。
                // 改为记入退避表：resume tick 按指数退避表（30s→…→600s 封顶）推迟重发；
                // 一致性由 range 反熵兜底（v8/v9 基准）。
                warn!(
                    "[bootstrap] 块 {} 连续 {} 次失败，转入指数退避重试（不重拉清单）: repo={} peer={}",
                    resp.index, attempts, resp.repo, conn.node_id
                );
                self.record_chunk_transport_fail(&conn.node_id, resp.repo, resp.index);
                progress.done_chunks = cw.done_prefix() as u64;
                progress.updated_ms = chrono::Utc::now().timestamp_millis();
                let _ = self.delta_storage().bootstrap_save(&progress, None);
                self.chunk_windows
                    .write()
                    .insert((conn.node_id, resp.repo), cw);
                return;
            }
        }
        // v10(C)：done_chunks = 最大连续前缀（乱序安全），保存进度后 fill 补发。
        progress.done_chunks = cw.done_prefix() as u64;
        progress.updated_ms = chrono::Utc::now().timestamp_millis();
        let _ = self.delta_storage().bootstrap_save(&progress, None);
        // 收到响应即清「无回帧」看门狗计数（该计数只用于识别完全无回帧的连接故障）。
        self.bootstrap_chunk_attempt
            .write()
            .remove(&(conn.node_id, resp.repo));
        // 联邦同步通道状态：刷新 done/inflight/phase
        {
            let s = crate::federation::sync::channels_status::global();
            let mut g = s.write();
            g.bootstrap.active = true;
            g.bootstrap.peer_id = conn
                .node_id
                .0
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect();
            g.bootstrap.repo = resp.repo as u64;
            g.bootstrap.done_chunks = progress.done_chunks;
            g.bootstrap.total_chunks = mf.chunks.len() as u64;
            g.bootstrap.inflight = cw.inflight_len() as u32;
            g.bootstrap.phase = "transfer".to_string();
            // E3：接收方块到达 → 方向 recv。
            g.bootstrap.direction = "recv".to_string();
        }
        let batch = cw.fill(0);
        self.chunk_windows
            .write()
            .insert((conn.node_id, resp.repo), cw);
        for idx in batch {
            self.request_bootstrap_chunk(&conn, resp.repo, idx, &mf)
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
        // 活锁治理(任务1)：旧实现此处对**每次发送**累加「无响应次数」并按
        // `bootstrap_chunk_max_attempts` 升级重拉清单（活锁第一环）。现在发送前不再计数；
        // 仅发送失败（传输类）时记入退避表，resume tick 按指数退避表决定何时重发，
        // 永不因此重拉清单。
        let req = BootstrapChunkRequestMessage { repo, index };
        // H2：bootstrap 块请求发送走 bootstrap_chunk_timeout_secs（与逐块等待超时一致量级），
        // 避免 5s 发送超时把大块数据请求反复打断导致收敛极慢。
        let send_res = tokio::time::timeout(
            Duration::from_secs(self.config.bootstrap_chunk_timeout_secs.max(1)),
            conn.send_message(MessageType::BootstrapChunkRequest, &req),
        )
        .await;
        match send_res {
            Ok(Ok(())) => self.metrics.record_message_sent(),
            Ok(Err(e)) => {
                warn!(
                    "[bootstrap] 请求块 {} 失败（传输类，计入退避重试）: peer={} repo={} err={}",
                    index, conn.node_id, repo, e
                );
                self.record_chunk_transport_fail(&conn.node_id, repo, index);
            }
            Err(_) => {
                warn!(
                    "[bootstrap] 请求块 {} 超时（{}s，计入退避重试）: peer={} repo={}",
                    index, self.config.bootstrap_chunk_timeout_secs, conn.node_id, repo
                );
                self.record_chunk_transport_fail(&conn.node_id, repo, index);
            }
        }
    }

    /// D批(D3)：竣工前/断点继承共用的「逐块对齐校验」—— 按与断点继承相同方式重建本地清单，
    /// 判断 `remote_mf.chunks` 全部块是否都能在本地找到 index/hash/rows 全等的块。
    /// 纯判定逻辑（不含水位抬升/清计数等副作用），便于单测；重建失败返回 false。
    /// 活表（crawler 持续写入、键随机分布）：竣工校验不能要求逐块 hash&rows 全等。
    /// INFOHASH(3)/PEER(2) 的插入可落在任意块区间，传输期间本地表一直在变，严格全等
    /// 永远不成立 → 死循环。NODE(1)/TRACKER(4) 是静态表，保持严格全等校验。
    fn repo_is_live_for_relaxed_finish(repo: u8) -> bool {
        repo == repo_type::INFOHASH || repo == repo_type::PEER
    }

    /// 活表宽松竣工兜底（纯判定）：本地重建清单总行数 ≥ 远端快照总行数 ×
    /// `BOOTSTRAP_LIVE_RELAXED_ROW_RATIO`。远端为空（total_rows==0）天然通过，与严格路径
    /// 对空清单的放行口径一致。不叠加「连续前缀块存在」要求：infohash 随机键插入会让
    /// 任意块区间（含前缀块 0）都可能在传输中变化，再加前缀全等会把死锁原样请回来。
    fn live_finish_rows_enough(local_total: u64, remote_total: u64) -> bool {
        if remote_total == 0 {
            return true;
        }
        local_total as f64 >= remote_total as f64 * BOOTSTRAP_LIVE_RELAXED_ROW_RATIO
    }

    async fn bootstrap_manifest_aligned(
        &self,
        repo: u8,
        remote_mf: &bootstrap::BootstrapManifest,
    ) -> bool {
        let storage = self.delta_storage();
        let w0 = storage.oplog_max_seq_for_repo(repo).unwrap_or(0).max(0) as u64;
        let version = w0.wrapping_add(1) as u32;
        let chunk_rows = remote_mf.chunk_rows;
        let remote_chunks = remote_mf.chunks.clone();
        let remote_total = remote_mf.total_rows;
        let built = tokio::task::spawn_blocking(move || {
            bootstrap::build_repo_manifest_impl(&storage, repo, chunk_rows, w0, version)
        })
        .await;
        match built {
            Ok(Ok(local_mf)) => {
                // 活表（INFOHASH/PEER）：键随机分布致块边界/hash 在传输期间漂移，逐块全等
                // 永不成立。改用行数兜底 —— 本地行数已达远端快照 95% 即视为基本落地，放行
                // 写 Done；不足则判假竣工（块返回极少行），不写 Done 交由 resume/watchdog。
                if Self::repo_is_live_for_relaxed_finish(repo) {
                    return Self::live_finish_rows_enough(local_mf.total_rows, remote_total);
                }
                // 静态表（NODE/TRACKER）：保持严格逐块 hash&rows 全等（空清单天然通过）。
                let matched = bootstrap::align_bootstrap_seed(&remote_chunks, &local_mf).1;
                matched.len() == remote_chunks.len()
            }
            _ => false,
        }
    }

    /// 完成 ③④ 后进入 ⑤ 追尾（复用 P1-3 delta 通道拉 `seq > w0`）。
    async fn finish_bootstrap(self: &Arc<Self>, conn: &PeerConn, repo: u8, w0_seq: u64) {
        let now = chrono::Utc::now().timestamp_millis();
        // 停摆修复（2026-09-30 第二轮）：退避期内跳过竣工前 D3 全表校验与重拉。
        // 正常块落地路径收到**任何**块响应都会清退避状态（见 handle_bootstrap_chunk_response
        // ok 分支），能带着活跃退避进入这里的只剩「继承全覆盖 → 直接竣工 → 校验失败 →
        // 重拉」循环本身 —— 该循环每轮要跑两次全表扫描（清单对齐验证 + 本处竣工校验，
        // spawn_blocking 但全程持有全局 SQLite 连接锁，259 万行实测 ~38s/次），把
        // api_runtime 的 DB 只读 handler（sync-observability/stats 轮询）全部 park 在
        // 连接锁上，4 个 worker 耗尽后连 /metrics、/health 都无 worker 可响应
        // （现场实证：api 整体超时、IOScheduler 背压升档后 9 分钟不恢复、Monitor 饿死）。
        if !self.chunk_backoff_ready(&conn.node_id, repo) {
            debug!(
                "[bootstrap] 传输退避期内跳过竣工校验与重拉（抑制全表扫描风暴）: peer={} repo={}",
                conn.node_id, repo
            );
            return;
        }
        // D批(D3)：竣工前强制对齐校验 —— 防「假竣工」（块返回极少行、断点未真落库却写 Done）。
        // skip=true 且本地存有清单时，按与断点继承相同方式重建本地清单，逐块 hash&rows 核对
        // mf.chunks 全部块。任一不一致或重建失败 → warn 后直接 return：不写 Done、不抬水位、
        // 不清尝试/失败计数、不插冷却、不 trigger_delta，交由现有 resume/watchdog 继续推进。
        if self.config.bootstrap_skip_identical_chunks {
            if let Ok(Some((p, Some(mf)))) =
                self.delta_storage().bootstrap_load(&conn.node_id.0, repo)
            {
                if !self.bootstrap_manifest_aligned(repo, &mf).await {
                    // 活锁治理(任务1-a)：D3 逐块对齐校验失败 = 真实数据漂移证据，
                    // 是重拉清单的两个合法触发条件之一（另一为 total_rows 结构性漂移）。
                    // 停摆修复：校验失败同时计入退避表（锚点 = 当前 done_chunks）——
                    // 退避在清单响应入口与本入口双重拦截，扫描频率随退避指数衰减，
                    // 而不是按发起互斥 TTL（60s）固定节奏狂扫直到熔断。
                    // 熔断计数与冷却在 start_bootstrap 内统一记账，连续无进展会被熔断。
                    warn!(
                        "[bootstrap] 竣工前对齐校验失败（块 hash/rows 与本地不一致或清单重建失败），不写 Done，重拉清单（计入退避）: peer={} repo={}",
                        conn.node_id, repo
                    );
                    self.record_chunk_transport_fail(
                        &conn.node_id,
                        repo,
                        p.done_chunks.min(u32::MAX as u64) as u32,
                    );
                    self.record_bootstrap_repull(&conn.node_id, repo);
                    self.clone().start_bootstrap(conn.node_id, repo).await;
                    return;
                }
            }
        }
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
        // v10(C)：清掉窗口状态机 —— 全部块已收齐，避免泄漏。
        self.chunk_windows.write().remove(&(conn.node_id, repo));
        // 联邦同步通道状态：切 delta 追尾 → bootstrap 进入「已完成」终态。
        // 修复：旧实现只置 active=false，done/total/inflight/phase 残留上一次
        // 块响应时的中间值，监控进度条永远停在半途（实测 202/207 卡死）。
        // 现在统一写成完整终态：done=total（进度条 100%）、phase=done。
        {
            let s = crate::federation::sync::channels_status::global();
            let mut g = s.write();
            g.bootstrap.active = false;
            g.bootstrap.done_chunks = g.bootstrap.total_chunks;
            g.bootstrap.inflight = 0;
            g.bootstrap.phase = "done".to_string();
            // E3：竣工终态复位方向。
            g.bootstrap.direction = String::new();
        }
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
        // 联邦同步通道状态：停滞 → bootstrap 进入「闲置」终态。
        // 修复：旧实现只改 DB 进度，不更新 channels 状态，监控里 active 残留
        // true、进度条停在停滞前的数值看起来像卡死。现在补完整终态写入。
        {
            let s = crate::federation::sync::channels_status::global();
            let mut g = s.write();
            g.bootstrap.active = false;
            g.bootstrap.done_chunks = p.done_chunks;
            g.bootstrap.total_chunks = p.total_chunks;
            g.bootstrap.inflight = 0;
            g.bootstrap.phase = "idle".to_string();
            // E3：停滞终态复位方向。
            g.bootstrap.direction = String::new();
        }
        false
    }

    /// P2-1：重启后恢复未完成的 bootstrap（按已落进度续传）。
    pub async fn bootstrap_resume_tick(self: Arc<Self>) {
        if !self.config.bootstrap_enabled {
            return;
        }
        // A5：发送方 idle 看门狗 —— 每轮续传 tick 顺带清理过期的「正在服务」标记并复位方向。
        self.prune_idle_send_serving();
        let list = match self.delta_storage().bootstrap_list() {
            Ok(l) => l,
            Err(_) => return,
        };
        let chunk_timeout = Duration::from_secs(self.config.bootstrap_chunk_timeout_secs.max(1));
        for p in list {
            if p.phase == bootstrap::BootstrapPhase::Done || p.peer.len() != 20 {
                continue;
            }
            let mut arr = [0u8; 20];
            arr.copy_from_slice(&p.peer);
            let peer = NodeId(arr);
            // I批：续传方向守卫 —— resume 恢复路径此前不查方向守卫：H 版重启后恢复
            // 持久化进度时直接重建窗口发块请求，实测 62(436.8万行) 向 51(350.9万行) 发起的
            // 无收益 bootstrap 空转 20+ 分钟（全部"对端无活跃会话"，进度永不 Done，调度器
            // 超龄回收抓不到秒级返回的 resume tick）。与发起路径共用 direction_guard_blocks：
            // 对端自报缺失(0)或行数不多于本端且本端已有存量 → **终止**该续传（置 Done、
            // 清窗口/尝试，下一轮不再恢复），而非 continue 等冷却/退避到期再试；仅空库
            // 冷启动（local_rows == 0）放行照常恢复。放在熔断/退避检查之前：终止与重发
            // 方向相反，无需等待冷却/退避到期。
            let remote_rows = self
                .peer_negotiate_state
                .read()
                .get(&peer)
                .and_then(|v| v.iter().find(|s| s.repo == p.repo).map(|s| s.row_count))
                .unwrap_or(0);
            let local_rows = self
                .local_entry_counts()
                .get((p.repo.saturating_sub(repo_type::NODE)) as usize)
                .copied()
                .unwrap_or(0) as u64;
            if Self::resume_guard_terminates(remote_rows, local_rows) {
                debug!(
                    "[bootstrap] 续传方向守卫：终止无收益续传（本端存量 {}，对端自报缺失(0)或行数 {} 不多于本端）: peer={} repo={}",
                    local_rows, remote_rows, peer, p.repo
                );
                let mut done = p.clone();
                done.phase = bootstrap::BootstrapPhase::Done;
                done.updated_ms = chrono::Utc::now().timestamp_millis();
                done.error = Some(format!(
                    "I批方向守卫终止：remote={} <= local={}",
                    remote_rows, local_rows
                ));
                let _ = self.delta_storage().bootstrap_save(&done, None);
                // 清掉该 (peer,repo) 的内存窗口与分块尝试，避免残留状态被后续逻辑拾起。
                self.chunk_windows.write().remove(&(peer, p.repo));
                self.bootstrap_chunk_attempt.write().remove(&(peer, p.repo));
                // 联邦同步通道状态：终止 → 不再显示在途传输（与停滞置 Idle 的终态写法一致）。
                {
                    let s = crate::federation::sync::channels_status::global();
                    let mut g = s.write();
                    g.bootstrap.active = false;
                    g.bootstrap.done_chunks = p.done_chunks;
                    g.bootstrap.total_chunks = p.total_chunks;
                    g.bootstrap.inflight = 0;
                    g.bootstrap.phase = "idle".to_string();
                }
                continue;
            }
            // 活锁治理(任务2)：重拉熔断冷却期内直接跳过 —— 不重发、不重拉、不重建窗口。
            if self.bootstrap_repull_in_cooldown(&peer, p.repo) {
                continue;
            }
            // 活锁治理(任务1)：退避未到期 → 本轮不重发（指数退避表决定重发时机，
            // 30s 起逐次翻倍至 `bootstrap_backoff_max_secs` 封顶）。
            if !self.chunk_backoff_ready(&peer, p.repo) {
                continue;
            }
            // v10(C)：窗口化续传 —— 不依赖链式「从 done_chunks 请求一块」，而是：
            //   ① 若内存中已有 ChunkWindow（传输进行中）：回收超时在途块，fill 补发。
            //   ② 若无 ChunkWindow（重启/异常掉出）：从持久化 done_chunks 重建窗口并填满。
            //   ③ 旧 bootstrap_chunk_attempt 的「idx == done_chunks」判定在窗口化下恒不成立
            //      （最后请求的 index 可能领先 done_chunks 一整个窗口），改由 ChunkWindow
            //      的 sent_at 超时回收负责识别「发了请求但对端完全不回帧」。
            let has_window = self.chunk_windows.read().contains_key(&(peer, p.repo));
            if has_window {
                // 传输进行中：回收超时在途块并重发
                let mf = match self.delta_storage().bootstrap_load(&p.peer, p.repo) {
                    Ok(Some((_, Some(m)))) => m,
                    _ => continue,
                };
                if let Some(conn) = self.sessions.get_connection(&peer) {
                    let mut cw = match self.chunk_windows.write().remove(&(peer, p.repo)) {
                        Some(w) => w,
                        None => continue,
                    };
                    let timed_out = cw.reap_timed_out(chunk_timeout);
                    if !timed_out.is_empty() {
                        warn!(
                            "[bootstrap] 回收 {} 个超时在途块: peer={} repo={} blocks={:?}",
                            timed_out.len(),
                            peer,
                            p.repo,
                            timed_out
                        );
                    }
                    // E2：窗口级空闲看门狗 —— 距最后一块落地超过阈值仍有在途块 → 回收重发。
                    let idle_timeout =
                        Duration::from_secs(self.config.bootstrap_window_idle_timeout_secs.max(1));
                    let idle = cw.idle_reap(idle_timeout);
                    if !idle.is_empty() {
                        warn!(
                            "[bootstrap] 窗口空闲看门狗：{}s 无新块落地，回收 {} 个在途块重发: peer={} repo={} blocks={:?} 恢复次数={}",
                            self.config.bootstrap_window_idle_timeout_secs,
                            idle.len(),
                            peer,
                            p.repo,
                            idle,
                            cw.idle_recovery_count()
                        );
                    }
                    // 联邦同步通道状态：窗口空闲看门狗回收
                    {
                        let s = crate::federation::sync::channels_status::global();
                        let mut g = s.write();
                        g.bootstrap.active = true;
                        g.bootstrap.peer_id = peer.0.iter().map(|b| format!("{:02x}", b)).collect();
                        g.bootstrap.repo = p.repo as u64;
                        g.bootstrap.idle_recoveries = cw.idle_recovery_count();
                        g.bootstrap.inflight = cw.inflight_len() as u32;
                        g.bootstrap.phase = "transfer".to_string();
                        // E3：续传（我们在拉对端快照）→ 方向 recv。
                        g.bootstrap.direction = "recv".to_string();
                    }
                    // E2 + 活锁治理(任务1)：连续空闲恢复达上限 → 记入退避表推迟重发，
                    // **不再重拉清单**。旧实现此处 start_bootstrap，对端不可达时形成
                    // 重拉风暴；且空闲计数已在块成功落地（on_response）时归零，
                    // 能达到上限说明对端真不可达，重拉同清单毫无收益。
                    if cw.idle_recovery_count()
                        >= self.config.bootstrap_window_idle_max_retries.max(1)
                    {
                        let anchor = cw.first_inflight().unwrap_or(p.done_chunks as u32);
                        warn!(
                            "[bootstrap] 窗口连续 {} 次空闲无进展，按退避表推迟重发（不重拉清单）: peer={} repo={}",
                            cw.idle_recovery_count(),
                            peer,
                            p.repo
                        );
                        self.record_chunk_transport_fail(&peer, p.repo, anchor);
                        self.chunk_windows.write().insert((peer, p.repo), cw);
                        continue;
                    }
                    let batch = cw.fill(0);
                    self.chunk_windows.write().insert((peer, p.repo), cw);
                    for idx in batch {
                        self.request_bootstrap_chunk(&conn, p.repo, idx, &mf).await;
                    }
                }
                continue;
            }
            // 无内存窗口（重启后）：从持久化进度重建窗口
            if self.sessions.get_connection(&peer).is_none() {
                continue;
            }
            match self.delta_storage().bootstrap_load(&p.peer, p.repo) {
                Ok(Some((_, Some(mf)))) if (mf.chunks.len() as u32) > p.done_chunks as u32 => {
                    if let Some(conn) = self.sessions.get_connection(&peer) {
                        let window_size = self.config.bootstrap_window_size.max(1);
                        let mut cw = bootstrap::ChunkWindow::new(
                            mf.chunks.len() as u32,
                            window_size,
                            p.done_chunks.min(mf.chunks.len() as u64) as u32,
                        );
                        let batch = cw.fill(0);
                        debug!(
                            "[bootstrap] 恢复续传: peer={}, repo={}, 重建窗口 size={}, 首批={:?}",
                            peer, p.repo, window_size, batch
                        );
                        self.chunk_windows.write().insert((peer, p.repo), cw);
                        for idx in batch {
                            self.request_bootstrap_chunk(&conn, p.repo, idx, &mf).await;
                        }
                    }
                }
                _ => {
                    // 无清单或已到末尾：重拉清单
                    self.clone().start_bootstrap(peer, p.repo).await;
                }
            }
        }

        // 活锁治理(任务5)：多 repo bootstrap 触发 —— 对 enable_repos 里每个 repo，
        // 若无活跃进度且不在熔断冷却期，从已连接对端选一个发起 bootstrap
        // （让 PEER(repo=2)/INFOHASH/TRACKER 也能通过快照通道追赶，不再只限 NODE）。
        self.trigger_enabled_repo_bootstraps().await;

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
            // E1：bootstrap 中途断裂后的运行时差异复检（同 5 分钟巡检节流，复用 D2 阈值）。
            self.rediff_bootstrap_recheck().await;
        }
    }

    /// 活锁治理(任务5)：多 repo bootstrap 触发 —— 对 `bootstrap_enable_repos` 里每个
    /// repo，若无任何活跃（非 Done）进度且无内存窗口，从已连接对端选一个发起
    /// bootstrap。方向守卫 / 双向让路 / 发起互斥 / 熔断冷却全部由 `start_bootstrap`
    /// 内部统一把关，这里只做「该 repo 是否需要发起」与对端初选。
    async fn trigger_enabled_repo_bootstraps(self: &Arc<Self>) {
        if self.config.bootstrap_enable_repos.is_empty() {
            return;
        }
        let rows = self.delta_storage().bootstrap_list().unwrap_or_default();
        for repo in self.config.bootstrap_enable_repos.clone() {
            // 非法 repo 值跳过（start_bootstrap 也有同款校验，这里提前省 IO）
            if !(repo_type::NODE..=repo_type::TRACKER).contains(&repo) {
                continue;
            }
            // 该 repo 已有活跃进度（任意对端）→ 快照已在途，不重复发起（每 repo 串行）
            if rows
                .iter()
                .any(|p| p.repo == repo && !matches!(p.phase, bootstrap::BootstrapPhase::Done))
            {
                continue;
            }
            // 该 repo 已有内存窗口（传输进行中）→ 同上
            if self.chunk_windows.read().keys().any(|(_, r)| *r == repo) {
                continue;
            }
            let conns = self.sessions.all_connections();
            if let Some(peer) = self.pick_bootstrap_peer(repo, &conns) {
                info!(
                    "[bootstrap] 任务5：repo={} 无活跃进度，从已连接对端发起 bootstrap: peer={}",
                    repo, peer
                );
                self.clone().start_bootstrap(peer, repo).await;
            }
        }
    }

    /// H批：方向优先门槛（纯函数）。对端存量需超过本端 ×SNAPSHOT_RATIO_THRESHOLD(1.2)
    /// 才视为「确有本端缺的存量」—— 与 check_and_trigger_bootstrap 的快照分流阈值对齐，
    /// 微差距（≤20%）不再触发无收益快照。边界：remote == local × 1.2 不触发。
    fn remote_exceeds_local_threshold(remote_rows: u64, local_rows: u64) -> bool {
        remote_rows as f64 > local_rows as f64 * SNAPSHOT_RATIO_THRESHOLD
    }

    /// H批：无方向优先候选时的回退闸（纯函数）。仅空库冷启动（local_rows == 0）允许
    /// 回退到任一已连接对端；本端已有存量时任何对端都不是值得拉的方向 → 返回 None，
    /// 由 delta/range 与 check_and_trigger_bootstrap 接管。
    fn allow_direction_fallback(local_rows: u64) -> bool {
        local_rows == 0
    }

    /// 活锁治理(任务5)：为某 repo 初选一个已连接对端。优先「对端自报行数 > 本端 ×1.2」
    /// 的方向（start_bootstrap 的方向守卫会再校验一次），无方向优先候选时仅空库冷启动
    /// 回退到第一条支持 bootstrap 的连接。全部候选都在熔断/竣工冷却期 → None。
    fn pick_bootstrap_peer(&self, repo: u8, conns: &[Arc<PeerConn>]) -> Option<NodeId> {
        let idx = (repo - repo_type::NODE) as usize;
        let local_rows = self.local_entry_counts().get(idx).copied().unwrap_or(0) as u64;
        let mut fallback: Option<NodeId> = None;
        for c in conns {
            if !c.supports_bootstrap() {
                continue;
            }
            if self.bootstrap_repull_in_cooldown(&c.node_id, repo) {
                continue;
            }
            // v10(F4)：竣工冷却期内不选为快照源（bootstrap 竣工后 bootstrap_cooldown_secs
            // 内不再被拉，避免刚对齐又立刻被反向拉）。
            if self.snapshot_in_cooldown(&c.node_id, repo) {
                continue;
            }
            let remote_rows = self
                .peer_negotiate_state
                .read()
                .get(&c.node_id)
                .and_then(|v| v.iter().find(|s| s.repo == repo).map(|s| s.row_count))
                .unwrap_or(0);
            // 方向优先：对端确有本端缺的存量（差 >20%）
            if Self::remote_exceeds_local_threshold(remote_rows, local_rows) {
                return Some(c.node_id);
            }
            if Self::allow_direction_fallback(local_rows) && fallback.is_none() {
                fallback = Some(c.node_id);
            }
        }
        fallback
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

    /// H批：方向守卫（纯函数）。本端已有存量（local_rows > 0）且对端自报缺失
    /// （remote_rows == 0，协商早期/缺失，方向证据不足）或行数不多于本端 → 拦截；
    /// 仅空库冷启动（local_rows == 0）放行。
    fn direction_guard_blocks(remote_rows: u64, local_rows: u64) -> bool {
        local_rows > 0 && (remote_rows == 0 || remote_rows <= local_rows)
    }

    /// I批：续传方向守卫判定（纯函数）。与发起路径共用 `direction_guard_blocks`：
    /// 对端自报缺失(0)或行数不多于本端且本端已有存量 → 该续传无收益，返回 true 表示
    /// 应**终止**（resume tick 置 Done、清窗口，下一轮不再恢复）；仅空库冷启动
    /// （local_rows == 0）返回 false，照常恢复续传。
    fn resume_guard_terminates(remote_rows: u64, local_rows: u64) -> bool {
        Self::direction_guard_blocks(remote_rows, local_rows)
    }

    /// P2-1：向指定对端发起某 repo 的 bootstrap（拉清单 → 分块 → 追尾）。默认关闭。
    pub async fn start_bootstrap(self: Arc<Self>, peer: NodeId, repo: u8) {
        if !self.config.bootstrap_enabled {
            return;
        }
        // 活锁治理(任务2)：重拉熔断冷却期内拒绝发起 —— 所有触发路径（协商 / 巡检 /
        // resume 续传欠账 / 竣工对齐失败）的单点闸，冷却期内不产生任何清单请求。
        if self.bootstrap_repull_in_cooldown(&peer, repo) {
            debug!(
                "[bootstrap] 熔断冷却期内，拒绝发起: peer={} repo={}",
                peer, repo
            );
            return;
        }
        // v10(B2)：方向守卫（所有发起路径的单点判定，含 resume 恢复/续传）——
        // 对端自报行数不多于本端时，快照拉来的几乎全是本端已有行（行集近似包含），
        // 数 GB 传输零收益，还会占住带宽与响应能力、阻塞对端真正需要的第一优先拉取。
        // 实测 52/58：58（305 万行）持续从 52（267 万行）拉无收益快照，而 52 真缺的
        // 38 万行被让路卡住。H批：协商状态缺失（对端自报缺失）不再放行 —— 本端已有
        // 存量时方向证据不足同样拦截；仅空库冷启动（local_rows == 0）放行。
        let remote_rows = self
            .peer_negotiate_state
            .read()
            .get(&peer)
            .and_then(|v| v.iter().find(|s| s.repo == repo).map(|s| s.row_count))
            .unwrap_or(0);
        let local_rows = self
            .local_entry_counts()
            .get((repo - repo_type::NODE) as usize)
            .copied()
            .unwrap_or(0) as u64;
        if Self::direction_guard_blocks(remote_rows, local_rows) {
            debug!(
                "[bootstrap] 方向守卫：本端已有存量({})，对端自报缺失(0)或行数({})不多于本端，跳过快照: peer={} repo={}",
                local_rows, remote_rows, peer, repo
            );
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
        // 活锁治理(任务2)：记账本次清单拉取。连续 `bootstrap_repull_circuit_threshold`
        // 次且期间无任何块成功落地 → 熔断（error! + 置 inactive + 冷却）；任一块成功
        // 落地即清零。首次发起同样记账 ——「清单永远拉不回来」与「拉回来传不动」同属停滞。
        self.record_bootstrap_repull(&peer, repo);
        let req = BootstrapManifestRequestMessage { repo };
        // G2：发送超时兜底（复用 transport_write_timeout_secs）——绝不无限挂起。
        let send_res = tokio::time::timeout(
            Duration::from_secs(self.config.transport_write_timeout_secs.max(1)),
            conn.send_message(MessageType::BootstrapManifestRequest, &req),
        )
        .await;
        match send_res {
            Ok(Ok(())) => self.metrics.record_message_sent(),
            Ok(Err(e)) => warn!("[bootstrap] 发送清单请求失败: {}", e),
            Err(_) => warn!(
                "[bootstrap] 发送清单请求超时（{}s）",
                self.config.transport_write_timeout_secs
            ),
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

    /// F8/v9 回归：delta 通道 version 透传后，「对既有条目的更新」能正常落地，
    /// 不再被 version=0 的保守跳过语义静默丢弃（修复前一致性全靠 Range 反熵兜底）。
    ///
    /// 链路：对端 oplog 批（OpEntryV2 携带真实 version）→ `ops_to_sync_entries_v2`
    /// （version 原样进入 SyncEntry，作为 LWW/裁决输入）→ `apply_node_sync` 更新
    /// 已存在的同 key 条目。NODE 的裁决规则是「同 key 取字典序较小的 node_id」，
    /// 更新侧 node_id 取更小值以命中「对端胜出」分支，并断言内存条目确实被改写。
    #[test]
    fn test_delta_v2_entries_update_existing_entry() {
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
        let addr: SocketAddr = "10.0.0.1:6885".parse().unwrap();

        // 1) 本地已有该地址的条目（node_id 字典序较大）
        let local = NodeSyncPayload {
            node_id: [0xbb; 20],
            addr,
        };
        mgr.apply_node_sync(&[SyncEntry {
            key: b"10.0.0.1:6885".to_vec(),
            operation: operation::UPSERT,
            version: 100,
            payload: bincode::serialize(&local).unwrap(),
        }]);
        assert_eq!(node_repo.node_id_sync(addr), Some([0xbb; 20]));

        // 2) 对端 delta V2 批：同 key、携带真实（更高）version，node_id 字典序更小 → 应胜出
        let update = NodeSyncPayload {
            node_id: [0x99; 20],
            addr,
        };
        let ops = vec![OpEntryV2 {
            seq: 2,
            is_delete: false,
            key: b"10.0.0.1:6885".to_vec(),
            value: bincode::serialize(&update).unwrap(),
            version: 200,
        }];
        let entries = delta::ops_to_sync_entries_v2(&ops);
        // 转换层闸门：真实 version 必须进入 SyncEntry（修复前此处恒为 0）
        assert_eq!(entries[0].version, 200);
        mgr.apply_node_sync(&entries);

        // 3) 既有条目已被更新（而非被静默丢弃），且没有产生第二条
        assert_eq!(node_repo.node_id_sync(addr), Some([0x99; 20]));
        assert_eq!(node_repo.len_sync(), 1);
    }

    /// F9 方案 B（2026-09-27，归档数据参与联邦同步）口径一致性回归测试：
    /// 协商/巡检计数（`local_entry_counts`）必须与 bootstrap 清单扫描
    /// （`build_repo_manifest_impl` → `load_repo_key_hashes_in_range`）同口径。
    /// F9 后清单扫描 = peers 主表活行 + peers_archive 归档行（db.rs 双路归并），
    /// 计数口径同步反转：peers 3 活行 + peers_archive 2 行 → peer 计数 = 清单行数 = 5。
    ///
    /// 口径沿革：F1 时期清单只扫主表，计数含 archive 会「差的部分永远拉不到」→
    /// BOOTSTRAP 死循环；F9 把清单扩到两表后两者必须**同时**含 archive——只改一边
    /// （计数含/清单不含，或计数不含/清单含）都会把死循环原样请回来。
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
            counts[1], 5,
            "peer 计数必须 = peers 主表活行 + peers_archive 归档行（F9 口径）"
        );

        let manifest =
            bootstrap::build_repo_manifest_impl(storage.as_ref(), repo_type::PEER, 2, 0, 1)
                .unwrap();
        assert_eq!(
            manifest.total_rows, 5,
            "bootstrap 清单行数必须含归档行（F9：清单 = peers + peers_archive）"
        );
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
            // D批(D2)：双向阈值要求绝对差 > 50_000 行才触发 BOOTSTRAP，故事务包裹批量灌
            // ~6 万行，使「无冷却 → BOOTSTRAP、冷却 → DELTA」的对照仍成立。
            conn.execute_batch("BEGIN").unwrap();
            for i in 0..60_000i64 {
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
            conn.execute_batch("COMMIT").unwrap();
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

        // 无冷却：本地 60000 / 对端 100，ratio 与绝对差(59900)均超 D2 双向阈值 → BOOTSTRAP
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

    /// D批(D2/D3)：双向快照阈值 —— 不区分方向。max/min > 1.1 且绝对差 > 50_000 才触发；
    /// 冷启动（一端为 0、另一端有量）触发；小比例差走 DELTA。
    #[test]
    fn test_bidir_bootstrap_by_volume_threshold() {
        // local 10 万 vs peer 150001：ratio≈1.5、diff=50001 → 触发（对端更多）
        assert!(SyncManager::should_bootstrap_by_volume(100_000, 150_001));
        // 对称方向（我更多）同结果
        assert!(SyncManager::should_bootstrap_by_volume(150_001, 100_000));
        // ratio=1.2、diff=2 万 → 不触发（绝对差门槛仍拦住，即使 ratio 已 > 1.1）
        assert!(!SyncManager::should_bootstrap_by_volume(100_000, 120_000));
        // 冷启动：本地 0、对端 5000 (>1000) → 触发
        assert!(SyncManager::should_bootstrap_by_volume(0, 5_000));
        // 冷启动：本地 5000、对端 0 → 触发
        assert!(SyncManager::should_bootstrap_by_volume(5_000, 0));
        // 边界：对端恰好 == SNAPSHOT_MIN_ROWS(1000) 而本地 0 → 不触发（需 >）
        assert!(!SyncManager::should_bootstrap_by_volume(0, 1_000));
        // 双方接近且都非 0 → 不触发
        assert!(!SyncManager::should_bootstrap_by_volume(100_000, 100_000));

        // D批(D3) 新阈值（ratio>1.1 且 diff>50_000，AND）边界：
        // 30 万 vs 36 万：ratio=1.2、diff=6 万 → 触发
        assert!(SyncManager::should_bootstrap_by_volume(300_000, 360_000));
        // 30 万 vs 35.1 万：ratio=1.17、diff=5.1 万 → 触发
        assert!(SyncManager::should_bootstrap_by_volume(300_000, 351_000));
        // 30 万 vs 33 万：ratio 恰好 1.1（要求 >1.1）→ 不触发
        assert!(!SyncManager::should_bootstrap_by_volume(300_000, 330_000));
        // 30 万 vs 35 万：diff 恰好 5 万（要求 >50_000）→ 不触发（ratio 虽 >1.1）
        assert!(!SyncManager::should_bootstrap_by_volume(300_000, 350_000));
        // 50 万 vs 55 万：ratio 恰好 1.1 → 不触发
        assert!(!SyncManager::should_bootstrap_by_volume(500_000, 550_000));
    }

    /// E1：运行时差异复检 —— 本地 vs 对端行数差超 D2 阈值时挑出 (peer,repo) 候选；
    /// 标记重触发后冷却期内不再重复挑出；未达阈值则不挑。
    #[test]
    fn test_rediff_candidate_threshold_and_cooldown() {
        use crate::federation::protocol::repo_type;
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        {
            let conn = storage.connection();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute_batch("BEGIN").unwrap();
            for i in 0..60_000i64 {
                conn.execute(
                    "INSERT INTO peers (infohash, ip, port, source) VALUES (?1, ?2, ?3, 'test')",
                    rusqlite::params![
                        [0xddu8; 20],
                        format!("10.3.{}.{}", i / 250, i % 250),
                        6881i64
                    ],
                )
                .unwrap();
            }
            conn.execute_batch("COMMIT").unwrap();
        }
        let mut cfg = make_config();
        cfg.bootstrap_rediff_cooldown_secs = 300;
        let mgr = make_sync_manager(storage, cfg);

        let peer = NodeId([7; 20]);
        // 对端 PEER 行数 130_000 vs 本地 ~60_000：ratio≈2.17、diff≈70_000 → 超 D2 阈值。
        let states = vec![(
            peer,
            vec![RepoSyncState {
                repo: repo_type::PEER,
                row_count: 130_000,
                max_seq: 10,
                min_seq: 1,
                retention_secs: 3600,
            }],
        )];
        let local = mgr.local_entry_counts();
        let running: Vec<bootstrap::BootstrapProgress> = Vec::new();

        // 1) 无冷却 → 挑出候选
        let cand = mgr.next_rediff_candidate(&local, &states, &running);
        assert_eq!(
            cand,
            Some((peer, repo_type::PEER)),
            "差异超 D2 阈值应挑出重触发候选"
        );

        // 2) 标记重触发后 → 冷却期内不再挑出
        mgr.mark_rediff_triggered(&peer, repo_type::PEER);
        let cand2 = mgr.next_rediff_candidate(&local, &states, &running);
        assert_eq!(cand2, None, "重触发冷却期内不得重复挑出");
        assert!(mgr.rediff_in_cooldown(&peer, repo_type::PEER));

        // 3) 未达阈值的对端（diff=2000 < 50000）→ 不挑
        let small = vec![(
            peer,
            vec![RepoSyncState {
                repo: repo_type::PEER,
                row_count: 62_000,
                max_seq: 10,
                min_seq: 1,
                retention_secs: 3600,
            }],
        )];
        mgr.bootstrap_rediff_cooldown.write().clear();
        let cand3 = mgr.next_rediff_candidate(&local, &small, &running);
        assert_eq!(cand3, None, "差异未达 D2 阈值不应挑出");
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

    /// 活锁治理(任务1)：传输失败退避 —— 记入 `bootstrap_chunk_attempt` 后，
    /// `chunk_backoff_ready` 在退避窗口内拒绝重发、到期放行；换块重置计数；
    /// 成功收到响应（此处模拟为直接清零入口）后立即放行。
    #[test]
    fn test_chunk_transport_backoff_gate() {
        use crate::federation::protocol::repo_type;
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        let mgr = make_sync_manager(storage, make_config());
        let peer = NodeId([5; 20]);
        let repo = repo_type::NODE;

        // 无失败记录 → 放行
        assert!(mgr.chunk_backoff_ready(&peer, repo));
        // 第 1 次失败 → 退避 30s 内不放行
        mgr.record_chunk_transport_fail(&peer, repo, 0);
        assert!(!mgr.chunk_backoff_ready(&peer, repo), "退避窗口内不得重发");
        // 把 last_fail_at 拨到 31s 前 → 第 1 档（30s）到期放行
        {
            let mut m = mgr.bootstrap_chunk_attempt.write();
            let st = m.get_mut(&(peer, repo)).unwrap();
            st.last_fail_at = Instant::now() - Duration::from_secs(31);
        }
        assert!(mgr.chunk_backoff_ready(&peer, repo), "退避到期应放行");
        // 连续失败累计：第 4 次失败 → 240s 档，31s 前的失败时刻不放行
        mgr.record_chunk_transport_fail(&peer, repo, 0);
        mgr.record_chunk_transport_fail(&peer, repo, 0);
        mgr.record_chunk_transport_fail(&peer, repo, 0);
        {
            let mut m = mgr.bootstrap_chunk_attempt.write();
            let st = m.get_mut(&(peer, repo)).unwrap();
            assert_eq!(st.fails, 4, "同块连续失败应累计");
            st.last_fail_at = Instant::now() - Duration::from_secs(31);
        }
        assert!(
            !mgr.chunk_backoff_ready(&peer, repo),
            "第 4 次失败的退避档（240s）未到不得重发"
        );
        // 换块 → 计数重置为 1（30s 档）
        mgr.record_chunk_transport_fail(&peer, repo, 7);
        {
            let m = mgr.bootstrap_chunk_attempt.read();
            let st = m.get(&(peer, repo)).unwrap();
            assert_eq!(st.fails, 1, "换块必须重置连续失败计数");
            assert_eq!(st.index, 7);
        }
        // 成功收到任意响应 → 整条清零（生产路径为 map.remove，此处等价验证放行语义）
        mgr.bootstrap_chunk_attempt.write().remove(&(peer, repo));
        assert!(mgr.chunk_backoff_ready(&peer, repo));
    }

    /// 停摆修复（2026-09-30 第二轮）回归：「继承全覆盖 → 直接竣工 → D3 校验失败 →
    /// 重拉」循环必须被退避表指数衰减 —— 每轮校验失败以相同锚点（done_chunks，继承
    /// 全覆盖后恒定）记一次退避失败，第 3 轮起退避（120s）超过发起互斥 TTL（60s），
    /// 清单响应入口 / 竣工入口（chunk_backoff_ready）在退避期内拒绝，全表扫描频率
    /// 随退避衰减，而不是按 60s 固定节奏狂扫直到熔断（现场 api_runtime 停摆根因：
    /// 每轮 1~2 次全表扫描持全局 SQLite 连接锁 ~40s，api 的 DB 只读 handler 全部
    /// park 直至 worker 耗尽，连 /metrics、/health 都无 worker 可响应）。
    #[test]
    fn test_gate_fail_backoff_damps_direct_finish_loop() {
        use crate::federation::protocol::repo_type;
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        let mgr = make_sync_manager(storage, make_config());
        let peer = NodeId([11; 20]);
        let repo = repo_type::NODE;
        let anchor = 148u32; // 继承全覆盖后 done_chunks 恒定，循环内锚点不变

        // 初始入口放行（首轮扫描允许 —— 需要它区分漂移是否真实）
        assert!(mgr.chunk_backoff_ready(&peer, repo));

        // 第 1 轮校验失败 → 退避 30s：60s TTL 内到达的清单响应被入口丢弃
        mgr.record_chunk_transport_fail(&peer, repo, anchor);
        assert!(
            !mgr.chunk_backoff_ready(&peer, repo),
            "首轮失败后入口必须关闭"
        );
        // 拨到 31s 后：放行（第 2 轮扫描）
        {
            let mut m = mgr.bootstrap_chunk_attempt.write();
            m.get_mut(&(peer, repo)).unwrap().last_fail_at =
                Instant::now() - Duration::from_secs(31);
        }
        assert!(mgr.chunk_backoff_ready(&peer, repo));

        // 第 2 轮失败 → 退避 60s；61s 后放行（第 3 轮扫描）
        mgr.record_chunk_transport_fail(&peer, repo, anchor);
        {
            let mut m = mgr.bootstrap_chunk_attempt.write();
            m.get_mut(&(peer, repo)).unwrap().last_fail_at =
                Instant::now() - Duration::from_secs(61);
        }
        assert!(mgr.chunk_backoff_ready(&peer, repo));

        // 第 3 轮失败 → 退避 120s > 60s TTL：按 60s 节奏到达的清单响应一律入口丢弃，
        // 不再触发任何全表扫描
        mgr.record_chunk_transport_fail(&peer, repo, anchor);
        {
            let mut m = mgr.bootstrap_chunk_attempt.write();
            m.get_mut(&(peer, repo)).unwrap().last_fail_at =
                Instant::now() - Duration::from_secs(61);
        }
        assert!(
            !mgr.chunk_backoff_ready(&peer, repo),
            "120s 退避档必须覆盖 60s 的重拉节奏（扫描频率衰减的关键断言）"
        );
        // 拨到 121s 后恢复放行；锚点恒定 → fails 连续累计未被换块重置
        {
            let mut m = mgr.bootstrap_chunk_attempt.write();
            m.get_mut(&(peer, repo)).unwrap().last_fail_at =
                Instant::now() - Duration::from_secs(121);
        }
        assert!(mgr.chunk_backoff_ready(&peer, repo));
        {
            let m = mgr.bootstrap_chunk_attempt.read();
            assert_eq!(
                m.get(&(peer, repo)).unwrap().fails,
                3,
                "同锚点循环失败必须连续累计"
            );
        }

        // 任一块成功落地（ok 路径清退避）→ 入口立即恢复放行（正常竣工不被误拦）
        mgr.bootstrap_chunk_attempt.write().remove(&(peer, repo));
        assert!(mgr.chunk_backoff_ready(&peer, repo));
    }

    /// 活锁治理(任务2)：重拉熔断 —— 连续重拉达到阈值且无块落地 → 进入冷却；
    /// 冷却期内 resume/start 全部跳过（`bootstrap_repull_in_cooldown`）；
    /// 任一块成功落地（`clear_bootstrap_repull`）即清零计数解除冷却；
    /// 冷却到期后下一次重拉立即重新熔断（每冷却期至多一次试探）。
    #[test]
    fn test_repull_circuit_breaker() {
        use crate::federation::protocol::repo_type;
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        let mut cfg = make_config();
        cfg.bootstrap_repull_circuit_threshold = 3;
        cfg.bootstrap_repull_circuit_cooldown_secs = 1_800;
        let mgr = make_sync_manager(storage, cfg);
        let peer = NodeId([6; 20]);
        let repo = repo_type::PEER;

        // 前两次重拉：未达阈值，不冷却
        assert!(!mgr.record_bootstrap_repull(&peer, repo));
        assert!(!mgr.record_bootstrap_repull(&peer, repo));
        assert!(!mgr.bootstrap_repull_in_cooldown(&peer, repo));
        // 第 3 次（达阈值）→ 熔断进入冷却
        assert!(
            mgr.record_bootstrap_repull(&peer, repo),
            "达到阈值本次应返回 tripped"
        );
        assert!(
            mgr.bootstrap_repull_in_cooldown(&peer, repo),
            "熔断后必须处于冷却期"
        );
        // 冷却期内继续记账不重复累计、不延长冷却
        assert!(!mgr.record_bootstrap_repull(&peer, repo));
        // 任一块成功落地 → 清零并解除冷却
        mgr.clear_bootstrap_repull(&peer, repo);
        assert!(!mgr.bootstrap_repull_in_cooldown(&peer, repo));
        // 冷却到期（把冷却到点拨到过去）→ 下一次重拉立即重新熔断
        mgr.record_bootstrap_repull(&peer, repo);
        mgr.record_bootstrap_repull(&peer, repo);
        {
            let mut m = mgr.bootstrap_repull_circuit.write();
            let e = m.get_mut(&(peer, repo)).unwrap();
            e.0 = 2;
            e.1 = Some(Instant::now() - Duration::from_secs(1));
        }
        assert!(
            !mgr.bootstrap_repull_in_cooldown(&peer, repo),
            "冷却到点已过应视为不在冷却"
        );
        assert!(
            mgr.record_bootstrap_repull(&peer, repo),
            "冷却到期后的首次重拉应立即重新熔断（保留历史计数）"
        );
        // 熔断按 (peer, repo) 隔离：其他 repo 不受影响
        assert!(!mgr.bootstrap_repull_in_cooldown(&peer, repo_type::NODE));
    }

    /// 活锁治理(任务4)：bootstrap 停滞判定解除 range 让路死锁 ——
    /// 非 Done 进度且最近块成功落地在阈值内 → active 且不停滞（让路）；
    /// 距最近块落地超过阈值 → 停滞（反熵不再让路）；旧进度行（无 last_progress_ms）
    /// 回落 updated_ms；Done 进度既不 active 也不停滞。
    #[test]
    fn test_bootstrap_transfer_stalled_breaks_range_yield() {
        use crate::federation::protocol::repo_type;
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        let mut cfg = make_config();
        cfg.bootstrap_stall_threshold_secs = 300;
        cfg.bootstrap_stall_secs = 3600; // 隔离变量：v9 fresh 窗口调大，只看任务4判定
        let mgr = make_sync_manager(storage.clone(), cfg);
        let peer = NodeId([8; 20]);

        // 无进度 → 不 active、不停滞
        assert!(!mgr.bootstrap_transfer_active());
        assert!(!mgr.bootstrap_transfer_stalled());

        let now = chrono::Utc::now().timestamp_millis();
        // 活跃传输：updated/last_progress 都新鲜 → active（让路）且不停滞
        let mut p = bootstrap::BootstrapProgress::new(repo_type::PEER, peer.0.to_vec(), now);
        p.phase = bootstrap::BootstrapPhase::Transfer;
        p.total_chunks = 100;
        p.done_chunks = 10;
        storage.bootstrap_save(&p, None).unwrap();
        assert!(mgr.bootstrap_transfer_active());
        assert!(
            !mgr.bootstrap_transfer_stalled(),
            "最近有块落地（last_progress 新鲜）不得判停滞"
        );

        // 活锁形态：清单往返持续刷新 updated_ms（保持 active），但 last_progress_ms
        // 停在 400s 前（>300s 阈值）→ 判停滞，反熵不再让路
        let mut stalled = p.clone();
        stalled.last_progress_ms = now - 400_000;
        stalled.updated_ms = now; // 重拉循环会刷新 updated_ms
        storage.bootstrap_save(&stalled, None).unwrap();
        assert!(
            mgr.bootstrap_transfer_active(),
            "updated_ms 新鲜时 active 判定与引入前一致"
        );
        assert!(
            mgr.bootstrap_transfer_stalled(),
            "块落地停摆超过阈值必须判停滞（打破 range 让路死锁）"
        );

        // 旧进度行（last_progress_ms=0，serde 回退）→ 回落 updated_ms 判定
        let mut legacy = p.clone();
        legacy.last_progress_ms = 0;
        storage.bootstrap_save(&legacy, None).unwrap();
        assert!(
            !mgr.bootstrap_transfer_stalled(),
            "旧行回落 updated_ms（新鲜）不得判停滞"
        );
        legacy.updated_ms = now - 400_000;
        storage.bootstrap_save(&legacy, None).unwrap();
        assert!(
            mgr.bootstrap_transfer_stalled(),
            "旧行 updated 过期同样判停滞"
        );

        // Done → 既不 active 也不停滞
        legacy.phase = bootstrap::BootstrapPhase::Done;
        storage.bootstrap_save(&legacy, None).unwrap();
        assert!(!mgr.bootstrap_transfer_active());
        assert!(!mgr.bootstrap_transfer_stalled());
    }

    /// 活锁治理(任务5)：多 repo bootstrap 触发 —— 默认 enable_repos 仅 [1]（NODE，
    /// 与引入前一致）；无连接时对端初选返回 None（不发起）；repo 已有活跃进度时不触发。
    #[test]
    fn test_bootstrap_enable_repos_default_and_peer_pick() {
        use crate::federation::protocol::repo_type;
        // 默认配置：仅 NODE(1)，现行为不变
        let cfg = make_config();
        assert_eq!(cfg.bootstrap_enable_repos, vec![repo_type::NODE]);

        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        let mgr = make_sync_manager(storage.clone(), make_config());
        // 测试 SessionsHandle 无连接 → 初选 None（tick 内不发起任何 bootstrap）
        let conns = mgr.sessions.all_connections();
        assert!(conns.is_empty());
        assert_eq!(
            mgr.pick_bootstrap_peer(repo_type::PEER, &conns),
            None,
            "无连接时不得选出发起对端"
        );

        // repo=2（PEER）已有非 Done 活跃进度 → 触发器的活跃检查应跳过
        // （直接验证判据：bootstrap_list 中存在该 repo 非 Done 行）
        let mut p = bootstrap::BootstrapProgress::new(
            repo_type::PEER,
            NodeId([9; 20]).0.to_vec(),
            chrono::Utc::now().timestamp_millis(),
        );
        p.phase = bootstrap::BootstrapPhase::Transfer;
        storage.bootstrap_save(&p, None).unwrap();
        let rows = mgr.delta_storage().bootstrap_list().unwrap_or_default();
        assert!(rows
            .iter()
            .any(|r| r.repo == repo_type::PEER
                && !matches!(r.phase, bootstrap::BootstrapPhase::Done)));
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

    /// D批(D3)：断点继承的逐块验证纯函数（`align_bootstrap_seed`，即 handle_bootstrap_manifest_response
    /// 实际走的代码路径）——全一致时前缀=全量、所有块进 skip（可直接竣工）；中间一块 hash
    /// 被改时前缀在该块截断、该块不得进 skip（需重拉），前缀外一致块仍跳过；有块不一致时
    /// skip 集合不足全量 → 不得直接竣工。
    #[test]
    fn test_align_bootstrap_seed_verifies_inherited_prefix() {
        use crate::federation::protocol::repo_type;
        let st = crate::storage::Storage::memory().unwrap();
        {
            let conn = st.connection();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            for i in 0..1000u32 {
                conn.execute(
                    "INSERT INTO dht_nodes (id, ip, port, l2_shard, deleted_at) VALUES (?1, ?2, ?3, 0, NULL)",
                    rusqlite::params![
                        vec![(i % 256) as u8; 20],
                        format!("10.0.{}.{}", i / 256, i % 256),
                        6881i64
                    ],
                )
                .unwrap();
            }
        }
        // chunk_rows=200 → 1000 行 = 5 块
        let local = bootstrap::build_repo_manifest_impl(&st, repo_type::NODE, 200, 1, 1).unwrap();
        assert_eq!(local.chunks.len(), 5);

        // 1) 全一致（远端==本地）→ 前缀=5，skip 集合含全部 5 块 → 可直接竣工
        let (prefix, skip) = bootstrap::align_bootstrap_seed(&local.chunks, &local);
        assert_eq!(prefix, 5, "全一致时连续前缀应覆盖全部块");
        assert_eq!(skip.len(), 5, "全一致时 5 块都进 skip");
        assert!(
            skip.len() >= local.chunks.len(),
            "全一致 → seed 覆盖全量 → 直接竣工"
        );

        // 2) 块 2 的 hash 被篡改（远端清单与本地不一致）→ 前缀在块 2 截断
        let mut remote = local.clone();
        remote.chunks[2].hash = [0xffu8; 32];
        let (prefix2, skip2) = bootstrap::align_bootstrap_seed(&remote.chunks, &local);
        assert_eq!(prefix2, 2, "块 2 不一致 → 验证通过的连续前缀应为 0,1");
        assert!(
            !skip2.contains(&2),
            "块 2 hash 不一致 → 不得进 skip（需重拉）"
        );
        assert!(
            skip2.contains(&0) && skip2.contains(&1),
            "前缀内一致块仍跳过"
        );
        assert!(
            skip2.contains(&3) && skip2.contains(&4),
            "前缀外一致块仍跳过"
        );
        assert!(
            skip2.len() < remote.chunks.len(),
            "块 2 不一致 → skip 不足全量 → 不得直接竣工"
        );
    }

    /// D批(D3)：竣工前对齐校验 `bootstrap_manifest_aligned`（finish_bootstrap 实际走的判定）——
    /// 本地与远端清单同源 → true；本地表内容变化致远端末块 hash/rows 失配 → false。
    /// false 时 finish_bootstrap 在写 phase=Done 之前 return（不抬水位/不清计数/不追尾）。
    #[tokio::test]
    async fn test_finish_bootstrap_alignment_gate() {
        use crate::federation::protocol::repo_type;
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        {
            let conn = storage.connection();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            for i in 0..550u32 {
                conn.execute(
                    "INSERT INTO dht_nodes (id, ip, port, l2_shard, deleted_at) VALUES (?1, ?2, ?3, 0, NULL)",
                    rusqlite::params![
                        vec![(i % 256) as u8; 20],
                        format!("10.0.{}.{}", i / 256, i % 256),
                        6881i64
                    ],
                )
                .unwrap();
            }
        }
        let mgr = make_sync_manager(storage.clone(), make_config());
        // chunk_rows=200：550 行 = 2 满块(400) + 1 残块(150)
        let remote =
            bootstrap::build_repo_manifest_impl(&storage, repo_type::NODE, 200, 1, 1).unwrap();
        assert_eq!(remote.chunks.len(), 3);
        assert!(
            mgr.bootstrap_manifest_aligned(repo_type::NODE, &remote)
                .await,
            "本地与远端清单同源时应判定对齐"
        );
        // 再追加 300 行 → 末块(残块)边界/行数漂移，远端第 3 块与本地重建不一致 → 失配
        {
            let conn = storage.connection();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            for i in 550..850u32 {
                conn.execute(
                    "INSERT INTO dht_nodes (id, ip, port, l2_shard, deleted_at) VALUES (?1, ?2, ?3, 0, NULL)",
                    rusqlite::params![
                        vec![(i % 256) as u8; 20],
                        format!("10.0.{}.{}", i / 256, i % 256),
                        6881i64
                    ],
                )
                .unwrap();
            }
        }
        assert!(
            !mgr.bootstrap_manifest_aligned(repo_type::NODE, &remote)
                .await,
            "本地表变化致远端末块 hash/rows 失配时应判定不对齐（finish_bootstrap 不写 Done）"
        );
    }

    /// 修复（活表竣工死循环）：INFOHASH 走宽松竣工判定。
    /// (i) 严格会失败、宽松应通过：先灌 250 行 infohash 建远端清单（chunk_rows=100 →
    ///     末块残 50 行），再追加 100 行使末块行数漂移 → 逐块全等必败，但本地行数已达
    ///     远端 95% → 宽松放行写 Done（不再死循环）。
    /// (ii) 假竣工兜底：远端清单 total_rows 远大于本地实际行数（本地仅 ~10%）→ 仍判 false。
    /// (iii) NODE 静态表未被豁免：本地追加行致块漂移 → 严格全等仍判 false。
    #[tokio::test]
    async fn test_bootstrap_infohash_relaxed_finish() {
        use crate::federation::protocol::repo_type;
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        // (i) 灌 250 行 infohash（u64 大端键，天然不撞、按数值升序）。
        {
            let conn = storage.connection();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            for i in 0..250u64 {
                conn.execute(
                    "INSERT INTO infohashes (infohash) VALUES (?1)",
                    rusqlite::params![i.to_be_bytes().to_vec()],
                )
                .unwrap();
            }
        }
        let mgr = make_sync_manager(storage.clone(), make_config());
        // chunk_rows=100：250 行 = 2 满块(200) + 1 残块(50)。
        let remote =
            bootstrap::build_repo_manifest_impl(&storage, repo_type::INFOHASH, 100, 0, 1).unwrap();
        assert_eq!(remote.total_rows, 250);
        assert_eq!(remote.chunks.len(), 3);

        // 再追加 100 行（键 250..349，排在尾部）→ 末块(残 50)被填满成 100 行并溢出出新块，
        // 远端第 3 块(rows=50)与本地重建(rows=100)失配 —— 严格全等在此必然失败。
        {
            let conn = storage.connection();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            for i in 250..350u64 {
                conn.execute(
                    "INSERT INTO infohashes (infohash) VALUES (?1)",
                    rusqlite::params![i.to_be_bytes().to_vec()],
                )
                .unwrap();
            }
        }
        // 旁证（锁定修复前提）：严格全等路径此刻必失败 —— 远端末块 rows=50 已对不上本地。
        {
            let w0 = storage
                .oplog_max_seq_for_repo(repo_type::INFOHASH)
                .unwrap_or(0)
                .max(0) as u64;
            let local_mf = bootstrap::build_repo_manifest_impl(
                &storage,
                repo_type::INFOHASH,
                remote.chunk_rows,
                w0,
                w0.wrapping_add(1) as u32,
            )
            .unwrap();
            let strict = bootstrap::align_bootstrap_seed(&remote.chunks, &local_mf)
                .1
                .len();
            assert!(
                strict < remote.chunks.len(),
                "前提：活表漂移后严格逐块全等必然不成立（否则宽松无意义）"
            );
        }
        // (i) 本地 350 行 >= 远端 250 行 ×0.95=237.5 → 宽松放行。
        assert!(
            mgr.bootstrap_manifest_aligned(repo_type::INFOHASH, &remote)
                .await,
            "活表行数已达远端 95% → 宽松竣工应放行（不再死循环）"
        );

        // (iii) NODE 静态表保持严格全等，未被宽松豁免。
        {
            let conn = storage.connection();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            for i in 0..550u32 {
                conn.execute(
                    "INSERT INTO dht_nodes (id, ip, port, l2_shard, deleted_at) \
                     VALUES (?1, ?2, ?3, 0, NULL)",
                    rusqlite::params![
                        vec![(i % 256) as u8; 20],
                        format!("10.6.{}.{}", i / 256, i % 256),
                        6881i64
                    ],
                )
                .unwrap();
            }
        }
        let remote_node =
            bootstrap::build_repo_manifest_impl(&storage, repo_type::NODE, 200, 0, 1).unwrap();
        {
            let conn = storage.connection();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            for i in 550..850u32 {
                conn.execute(
                    "INSERT INTO dht_nodes (id, ip, port, l2_shard, deleted_at) \
                     VALUES (?1, ?2, ?3, 0, NULL)",
                    rusqlite::params![
                        vec![(i % 256) as u8; 20],
                        format!("10.6.{}.{}", i / 256, i % 256),
                        6881i64
                    ],
                )
                .unwrap();
            }
        }
        assert!(
            !mgr.bootstrap_manifest_aligned(repo_type::NODE, &remote_node)
                .await,
            "NODE 静态表未被豁免：块漂移后严格全等必须 false"
        );

        // (ii) 假竣工兜底：独立 storage 仅 100 行，远端清单却报 1000 行（本地 ~10%）→ false。
        let s2 = Arc::new(crate::storage::Storage::memory().unwrap());
        {
            let conn = s2.connection();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            for i in 0..100u64 {
                conn.execute(
                    "INSERT INTO infohashes (infohash) VALUES (?1)",
                    rusqlite::params![(i + 5000).to_be_bytes().to_vec()],
                )
                .unwrap();
            }
        }
        let mgr2 = make_sync_manager(s2, make_config());
        let bogus_remote = bootstrap::BootstrapManifest {
            repo: repo_type::INFOHASH,
            version: 1,
            w0_seq: 0,
            chunk_rows: 100,
            total_rows: 1000,
            chunks: vec![],
        };
        assert!(
            !mgr2
                .bootstrap_manifest_aligned(repo_type::INFOHASH, &bogus_remote)
                .await,
            "本地仅约 10% 行 → 假竣工兜底必须 false（不写 Done）"
        );
    }

    /// 修复（E1 双向反向拉数）：`next_rediff_candidate` 方向保护 —— 只在「本地少、对端多」
    /// （local < remote）才挑候选。用空 running、无冷却的 mgr，结果只由阈值+方向决定。
    #[test]
    fn test_rediff_candidate_direction_guard() {
        use crate::federation::protocol::repo_type;
        let storage = Arc::new(crate::storage::Storage::memory().unwrap());
        let mgr = make_sync_manager(storage, make_config());
        let peer = NodeId([9; 20]);
        let running: Vec<bootstrap::BootstrapProgress> = Vec::new();
        // local[1] = PEER 计数（idx = repo - NODE = 2-1 = 1）。
        let case = |local_peer: u32, remote_peer: u64| {
            let local = vec![0u32, local_peer, 0, 0];
            let states = vec![(
                peer,
                vec![RepoSyncState {
                    repo: repo_type::PEER,
                    row_count: remote_peer,
                    max_seq: 10,
                    min_seq: 1,
                    retention_secs: 3600,
                }],
            )];
            mgr.next_rediff_candidate(&local, &states, &running)
        };
        // (i) 本地 10 万 < 对端 20 万（ratio=2、diff=10 万 > 5 万）→ 候选
        assert_eq!(case(100_000, 200_000), Some((peer, repo_type::PEER)));
        // (ii) 本地 20 万 > 对端 10 万（同阈值，但方向反）→ None，不得反向拉数
        assert_eq!(
            case(200_000, 100_000),
            None,
            "本地多→对端少时不得反向重触发 bootstrap"
        );
        // (iii) 冷启动 local=0 & remote=5000(>SNAPSHOT_MIN_ROWS=1000) → Some
        assert_eq!(case(0, 5_000), Some((peer, repo_type::PEER)));
        // (iv) remote=0 & local=5000(>=1000) → None（local>=remote 被方向保护拦）
        assert_eq!(
            case(5_000, 0),
            None,
            "对端空→本地多时不得反向重触发 bootstrap"
        );
    }

    // ==================== 核心运行时切片（2026-09-30）：A1/A3/A5 纯逻辑单测 ====================

    #[test]
    fn a1_try_inc_bounded_caps_inflight_and_releases() {
        // A1：有界在途计数 —— 打满后拒绝获取，释放一个后可再获取。
        let c = std::sync::atomic::AtomicUsize::new(0);
        let limit = 4;
        for _ in 0..limit {
            assert!(try_inc_bounded(&c, limit));
        }
        // 已满 → 非阻塞拒绝（handler 据此 NAK "send concurrency limit"）
        assert!(!try_inc_bounded(&c, limit));
        assert!(!try_inc_bounded(&c, limit));
        // 释放一个（模拟 permit drop）→ 可再获取
        c.fetch_sub(1, Ordering::AcqRel);
        assert!(try_inc_bounded(&c, limit));
        // limit<1 按 1 兜底（防配置 0 锁死发送）
        let c2 = std::sync::atomic::AtomicUsize::new(0);
        assert!(try_inc_bounded(&c2, 0));
        assert!(!try_inc_bounded(&c2, 0));
    }

    #[test]
    fn a3_send_circuit_threshold_open_then_half_open_and_reset() {
        let now = Instant::now();
        let base = Duration::from_secs(30);
        let max = Duration::from_secs(600);
        let threshold = 5;
        let mut s = SendCircuitState::record_success();
        // 连续 4 次失败：未达阈值，不熔断
        for _ in 0..4 {
            s = s.record_failure(now);
            assert!(!s.is_open(threshold, now, base, max));
        }
        // 第 5 次失败 → 熔断开放（backoff_delay(30,600,5)=480s）
        s = s.record_failure(now);
        assert_eq!(s.fails, 5);
        assert!(s.is_open(threshold, now, base, max));
        // backoff 到期（481s > 480s）→ 半开，允许探测
        let later = now + Duration::from_secs(481);
        assert!(!s.is_open(threshold, later, base, max));
        // 成功一次 → 整条清零
        let fresh = SendCircuitState::record_success();
        assert_eq!(fresh.fails, 0);
        assert!(!fresh.is_open(threshold, now, base, max));
    }

    #[test]
    fn a5_serving_entry_expired_boundary() {
        let now = Instant::now();
        let idle = Duration::from_secs(60);
        // 59s 前的服务标记 → 仍在窗口内，保留
        assert!(!serving_entry_expired(
            now - Duration::from_secs(59),
            now,
            idle
        ));
        // 恰好 60s / 120s 前 → 过期，应移除并复位 direction
        assert!(serving_entry_expired(
            now - Duration::from_secs(60),
            now,
            idle
        ));
        assert!(serving_entry_expired(
            now - Duration::from_secs(120),
            now,
            idle
        ));
    }

    // ==================== H批：方向判定纯函数单测 ====================

    /// H批：方向优先门槛 —— 对端存量需超过本端 ×SNAPSHOT_RATIO_THRESHOLD(1.2)。
    #[test]
    fn test_remote_exceeds_local_threshold() {
        // 正向：对端存量超过本端 1.2 倍 → 触发
        assert!(SyncManager::remote_exceeds_local_threshold(13, 10));
        assert!(SyncManager::remote_exceeds_local_threshold(121, 100));
        assert!(SyncManager::remote_exceeds_local_threshold(
            2_500_000, 2_000_000
        ));
        // 边界：恰好 1.2 倍 → 不触发
        assert!(!SyncManager::remote_exceeds_local_threshold(12, 10));
        assert!(!SyncManager::remote_exceeds_local_threshold(120, 100));
        assert!(!SyncManager::remote_exceeds_local_threshold(1_200, 1_000));
        // 对端不多于本端 → 不触发
        assert!(!SyncManager::remote_exceeds_local_threshold(10, 10));
        assert!(!SyncManager::remote_exceeds_local_threshold(9, 10));
        // 空库冷启动：本端 0 行、对端有任意存量 → 触发
        assert!(SyncManager::remote_exceeds_local_threshold(1, 0));
        // 双方皆空 → 不触发
        assert!(!SyncManager::remote_exceeds_local_threshold(0, 0));
    }

    /// H批：方向守卫 —— 本端有存量时，协商状态缺失（remote_rows == 0）或对端行数
    /// 不多于本端 → 拦截；仅冷启动（local_rows == 0）放行。
    #[test]
    fn test_direction_guard_blocks() {
        // 协商状态缺失（remote_rows == 0）且本端有存量 → 拦截（方向证据不足）
        assert!(SyncManager::direction_guard_blocks(0, 100));
        assert!(SyncManager::direction_guard_blocks(0, 3_000_000));
        // 对端行数不多于本端 → 拦截
        assert!(SyncManager::direction_guard_blocks(100, 100));
        assert!(SyncManager::direction_guard_blocks(50, 100));
        // 冷启动（local_rows == 0）：对端自报缺失或任意行数 → 放行
        assert!(!SyncManager::direction_guard_blocks(0, 0));
        assert!(!SyncManager::direction_guard_blocks(100, 0));
        // 对端确有多于本端的存量 → 放行
        assert!(!SyncManager::direction_guard_blocks(101, 100));
        assert!(!SyncManager::direction_guard_blocks(1_000, 100));
    }

    /// I批：续传方向守卫判定 —— 命中 direction_guard_blocks 的续传应被终止（置 Done、
    /// 不再恢复重建窗口）；未命中（含空库冷启动 local_rows == 0）照常恢复。
    #[test]
    fn test_resume_guard_terminates() {
        // 对端自报缺失(0)且本端有存量 → 拦截终止
        assert!(SyncManager::resume_guard_terminates(0, 100));
        assert!(SyncManager::resume_guard_terminates(0, 3_000_000));
        // 对端行数不多于本端 → 拦截终止（含实测场景：62 的 436.8 万 vs 51 的 350.9 万）
        assert!(SyncManager::resume_guard_terminates(100, 100));
        assert!(SyncManager::resume_guard_terminates(50, 100));
        assert!(SyncManager::resume_guard_terminates(3_509_000, 4_368_000));
        // 冷启动（local_rows == 0）：对端自报缺失或任意行数 → 放行恢复
        assert!(!SyncManager::resume_guard_terminates(0, 0));
        assert!(!SyncManager::resume_guard_terminates(100, 0));
        // 对端确有多于本端的存量 → 放行恢复
        assert!(!SyncManager::resume_guard_terminates(101, 100));
        assert!(!SyncManager::resume_guard_terminates(1_000, 100));
    }

    /// H批：pick_bootstrap_peer 回退闸 —— 仅空库冷启动允许回退，本端已有存量时回退被禁用。
    #[test]
    fn test_allow_direction_fallback() {
        // 空库冷启动（local_rows == 0）→ 允许回退
        assert!(SyncManager::allow_direction_fallback(0));
        // 本端已有存量 → 不允许回退（宁可不发起，交给 delta/range/巡检）
        assert!(!SyncManager::allow_direction_fallback(1));
        assert!(!SyncManager::allow_direction_fallback(1_000));
        assert!(!SyncManager::allow_direction_fallback(3_000_000));
    }

    /// 「块请求 spawn_blocking 正确性」回归：handle_bootstrap_chunk_request 的取数段已从同步
    /// SQLite 调用移入 tokio::task::spawn_blocking（避免在 async handler 里阻塞 tokio worker，
    /// 叠加读池空回退写锁曾致 62 侧 30s write timeout 重试循环）。
    /// 本测试以与生产代码完全一致的闭包形态（move 捕获 Arc<Storage>/owned lo/hi/max_rows）
    /// 验证：
    ///   (1) 阻塞线程取数路径返回的条目与直接同步调用逐 key 一致；
    ///   (2) 半开区间 [lo, hi) 边界正确（按 infohash 升序，hi 不包含）；
    ///   (3) 查询 limit=max_rows+1 后再 take(max_rows) 的边界语义（多取一行用于末块判定，
    ///       实际下发仍截断到 max_rows）。
    #[tokio::test]
    async fn test_bootstrap_chunk_spawn_blocking_entries_correctness() {
        use crate::federation::protocol::repo_type;
        // 20 字节 infohash：前 8 字节放大端下标，后 12 字节补 0 —— 唯一、按数值升序。
        fn ih_key(i: u64) -> Vec<u8> {
            let mut k = [0u8; 20];
            k[..8].copy_from_slice(&i.to_be_bytes());
            k.to_vec()
        }
        let storage = Arc::new(Storage::memory().unwrap());
        {
            let conn = storage.connection();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            for i in 0..100u64 {
                conn.execute(
                    "INSERT INTO infohashes (infohash) VALUES (?1)",
                    rusqlite::params![ih_key(i)],
                )
                .unwrap();
            }
        }
        let repo = repo_type::INFOHASH;

        // 与生产 handler 完全一致的 spawn_blocking 取数形态：move 闭包 + 三分支 match。
        // 此处 happy-path 仅走 Ok(Ok)；Ok(Err)/Err(join) 为防御分支，与清单重建同风格。
        async fn load_via_blocking(
            storage: Arc<Storage>,
            repo: u8,
            lo: Option<Vec<u8>>,
            hi: Option<Vec<u8>>,
            max_rows: usize,
        ) -> Vec<crate::federation::protocol::SyncEntry> {
            match tokio::task::spawn_blocking(move || {
                storage.load_repo_sync_entries_in_range(
                    repo,
                    lo.as_deref(),
                    hi.as_deref(),
                    max_rows + 1,
                )
            })
            .await
            {
                Ok(Ok(rows)) => rows.into_iter().take(max_rows).collect(),
                Ok(Err(e)) => panic!("阻塞取数返回 Err: {}", e),
                Err(e) => panic!("阻塞取数 join 失败: {}", e),
            }
        }

        // (1) 全区间：max_rows=100（库里恰好 100 行）。
        let direct_all: Vec<Vec<u8>> = storage
            .load_repo_sync_entries_in_range(repo, None, None, 101)
            .unwrap()
            .into_iter()
            .map(|e| e.key)
            .collect();
        let blocking_all: Vec<Vec<u8>> = load_via_blocking(storage.clone(), repo, None, None, 100)
            .await
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(blocking_all.len(), 100, "全区间应取满 100 行");
        assert_eq!(
            blocking_all, direct_all,
            "spawn_blocking 取数必须与直接同步调用逐 key 一致"
        );
        assert_eq!(blocking_all.first().unwrap(), &ih_key(0));
        assert_eq!(blocking_all.last().unwrap(), &ih_key(99));

        // (2) 半开区间 [30, 60)：应恰返回下标 30..=59 共 30 行。
        let lo = ih_key(30);
        let hi = ih_key(60);
        let direct_range: Vec<Vec<u8>> = storage
            .load_repo_sync_entries_in_range(repo, Some(&lo), Some(&hi), 101)
            .unwrap()
            .into_iter()
            .map(|e| e.key)
            .collect();
        let blocking_range: Vec<Vec<u8>> =
            load_via_blocking(storage.clone(), repo, Some(lo), Some(hi), 30)
                .await
                .into_iter()
                .map(|e| e.key)
                .collect();
        assert_eq!(blocking_range.len(), 30, "[30,60) 应返回 30 行");
        assert_eq!(
            blocking_range, direct_range,
            "区间取数必须与直接调用逐 key 一致"
        );
        assert_eq!(blocking_range.first().unwrap(), &ih_key(30));
        assert_eq!(
            blocking_range.last().unwrap(),
            &ih_key(59),
            "hi=60 为开界，不得包含"
        );

        // (3) +1/take 边界：max_rows=20、库里 100 行 —— 查询多取一行(limit=21)，
        //     但 take(20) 截断后必须恰为 20 行，末行为下标 19（多取的第 21 行被丢弃）。
        let chunk: Vec<Vec<u8>> = load_via_blocking(storage.clone(), repo, None, None, 20)
            .await
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(
            chunk.len(),
            20,
            "take(max_rows=20) 后必须恰为 20 行（多取的第 21 行被截断）"
        );
        assert_eq!(chunk.last().unwrap(), &ih_key(19));
    }
}
