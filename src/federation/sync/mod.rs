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
use rustc_hash::FxHashMap;
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

/// 批次 3：应答方「清单现场重建」的租约时长（秒）。
/// 重建 = DB 全表扫描（百万行级，实测数分钟）；请求方每 60s resume 一次会反复触发，
/// 并发/高频重复重建会把 DB IO 与 Federation 分类槽吃光（双端互请 → 双向重建循环）。
const BOOTSTRAP_REBUILD_LEASE_SECS: u64 = 900;

/// 批次 3：全量差异巡检（本地 vs 对端各 repo 总数比对）的最小间隔（秒）。
/// 该巡检要做 DB 级全表 COUNT，不能挂在秒级返回的 bootstrap 续传任务里每轮都跑 ——
/// 实测会把续传任务的单次占槽拉到 164.36s，连带饿死同分类其它联邦任务。
const BOOTSTRAP_CHECK_INTERVAL_SECS: u64 = 300;
/// v7：bootstrap 快照触发的最小行数（对端该 repo 为空且本端 ≥ 此量才走快照）。
const SNAPSHOT_MIN_ROWS: u64 = 1_000;
/// v7：快照分流比例阈值（本端比对端多出该比例且差值超阈值 → 快照）。
const SNAPSHOT_RATIO_THRESHOLD: f64 = 1.2;

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
    /// F7：bootstrap 块校验连续失败计数（repo → 次数）。
    /// 连续 ≥3 次判定清单漂移（对端重启后清单已重建/数据已前进），重拉清单自愈。
    bootstrap_verify_fails: RwLock<FxHashMap<u8, u32>>,
    /// 批次 3：应答方「清单现场重建」（F6）的进行中标记（node_id → 起始时刻）。
    /// 重建是 DB 全表扫描（百万行级，实测数分钟）；请求方每 60s resume 一次会反复触发，
    /// 双端互请时形成**双向重建循环**，把 DB IO 与 Federation 分类槽吃光。
    bootstrap_rebuild_at: RwLock<FxHashMap<(NodeId, u8), Instant>>,
    /// 批次 3：全量差异巡检（`check_and_trigger_bootstrap`）的上次执行时刻。
    /// 该巡检要做 DB 级全表计数比对，挂在 `bootstrap_resume_tick` 里会让本该秒级返回的
    /// 续传任务占住 Federation 槽上百秒（实测 max 164.36s），把其它联邦任务一起饿死。
    bootstrap_check_at: RwLock<Option<Instant>>,
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
                // v8 F4：续拉同样标记 in-flight（防与下一轮 tick 叠加）
                self.delta_inflight
                    .write()
                    .insert((conn.node_id, batch.repo), Instant::now());
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
    pub(crate) fn local_entry_counts(&self) -> Vec<u32> {
        let db = self.delta_storage().entity_counts_cached();
        let pick = |i: usize| -> u64 { db.get(i).copied().unwrap_or(-1).max(0) as u64 };
        let node = pick(0) + self.node_repo.write_queue_len_sync() as u64;
        vec![
            node as u32,
            (pick(1) + pick(2)) as u32,
            pick(3) as u32,
            pick(4) as u32,
        ]
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
                let peer_max = *self
                    .delta_peer_max
                    .read()
                    .get(&(conn.node_id, rt))
                    .unwrap_or(&0);
                let synced_seq = self
                    .delta_storage()
                    .get_peer_seq(&conn.node_id.0, rt)
                    .unwrap_or(0)
                    .max(0) as u64;
                let lag = peer_max.saturating_sub(synced_seq);
                let lag_over = peer_max > 0
                    && lag > self.config.range_bulk_threshold_rows.max(1)
                    && self.strategy_for(&conn.node_id, rt) != Some(protocol::STRATEGY_NONE);
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
                if use_bootstrap {
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
                            "[negotiate] 执行快照策略：启动 bootstrap peer={} repo={}（协商裁定={}，欠账={}）",
                            conn.node_id,
                            rt,
                            self.strategy_for(&conn.node_id, rt)
                                == Some(protocol::STRATEGY_BOOTSTRAP),
                            lag
                        );
                        let sm = self.clone();
                        let peer = conn.node_id;
                        tokio::spawn(async move { sm.start_bootstrap(peer, rt).await });
                    }
                    continue;
                }
                // v7：看门狗（暂停期跳过；触发时已清协商）
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
        for repo in repo_type::NODE..=repo_type::TRACKER {
            let idx = (repo - repo_type::NODE) as usize;
            let interval = self
                .config
                .range_reconcile_interval_secs
                .get(idx)
                .copied()
                .unwrap_or(60)
                .max(1);
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
            .insert((conn.node_id, req.repo), manifest.clone());
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
        let manifest = match self
            .bootstrap_manifests
            .read()
            .get(&(conn.node_id, req.repo))
            .cloned()
        {
            Some(m) => m,
            None => {
                // F6: 应答方清单缓存是纯内存（重启即空），而请求方 resume 持本地持久化的
                // 旧清单直接要块，双方互等对方先发清单请求 → 死锁（「无清单缓存」双端刷屏）。
                // 现场按当前 DB 重建清单并缓存，直接服务该块；若与请求方旧清单版本漂移，
                // 由请求方校验失败计数触发重拉清单自愈（F7）。
                //
                // 批次 3 租约：重建是**DB 全表扫描**（百万行级，实测数分钟）。请求方每 60s
                // resume 一次 → 重建未完成前缓存仍为空 → 下一轮请求又触发一次重建，
                // 双端互请时形成双向重建循环，DB IO 与 Federation 分类槽被吃光。
                // 这里给「重建中」加租约：租约内到达的块请求直接跳过（下一轮 resume 会再来）。
                {
                    let mut g = self.bootstrap_rebuild_at.write();
                    if let Some(t) = g.get(&(conn.node_id, req.repo)) {
                        if t.elapsed() < Duration::from_secs(BOOTSTRAP_REBUILD_LEASE_SECS) {
                            debug!(
                                "[bootstrap] 清单重建进行中（已 {}s），本轮跳过块请求: peer={} repo={} index={}",
                                t.elapsed().as_secs(),
                                conn.node_id,
                                req.repo,
                                req.index
                            );
                            return;
                        }
                    }
                    g.insert((conn.node_id, req.repo), Instant::now());
                }
                let storage = self.delta_storage();
                let w0 = storage.oplog_max_seq().unwrap_or(0).max(0) as u64;
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
                            "[bootstrap] 清单缓存缺失，现场重建: peer={}, repo={}, 块数={}, w0={}",
                            conn.node_id,
                            req.repo,
                            m.chunks.len(),
                            w0
                        );
                        self.bootstrap_manifests
                            .write()
                            .insert((conn.node_id, req.repo), m.clone());
                        self.bootstrap_rebuild_at
                            .write()
                            .remove(&(conn.node_id, req.repo));
                        m
                    }
                    Err(e) => {
                        self.bootstrap_rebuild_at
                            .write()
                            .remove(&(conn.node_id, req.repo));
                        warn!(
                            "[bootstrap] 收到分块请求且清单重建失败 peer={}, repo={}: {}",
                            conn.node_id, req.repo, e
                        );
                        return;
                    }
                }
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
        // A3：全 repo 统一 —— 只校验 repo 取值合法（四个 repo 等价），不再只放行 NODE。
        if mf.repo < repo_type::NODE || mf.repo > repo_type::TRACKER {
            debug!(
                "[bootstrap] 收到非法 repo={} 的清单，忽略: from={}",
                mf.repo, conn.node_id
            );
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
        if ok {
            self.bootstrap_verify_fails.write().remove(&resp.repo);
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
                "[bootstrap] 块 {} 传输校验失败（声明 {} 行 / 实收 {} 行，可能丢包），保持进度 done={}",
                resp.index, chunk.rows, resp.entries.len(), progress.done_chunks
            );
            // F7: 连续 3 次校验失败 → 判定清单漂移（对端重启后已重建清单/数据已前进，
            // 本地旧清单的块 hash 恒对不上），重发清单请求重置进度重拉。
            // 块数据按 upsert 落库（幂等），重复拉取无害。
            let fails = self
                .bootstrap_verify_fails
                .read()
                .get(&resp.repo)
                .copied()
                .unwrap_or(0)
                + 1;
            if fails >= 3 {
                self.bootstrap_verify_fails.write().remove(&resp.repo);
                warn!(
                    "[bootstrap] 连续 {} 次校验失败，判定清单漂移，重拉清单: repo={}, peer={}",
                    fails, resp.repo, conn.node_id
                );
                self.clone().start_bootstrap(conn.node_id, resp.repo).await;
            } else {
                self.bootstrap_verify_fails.write().insert(resp.repo, fails);
            }
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

                // 对端比本地多 20% 以上才触发
                let ratio = remote_count as f64 / local_count as f64;
                if ratio < SNAPSHOT_RATIO_THRESHOLD {
                    continue;
                }

                // 检查是否已有进行中的 bootstrap
                let already_running = if let Ok(list) = self.delta_storage().bootstrap_list() {
                    list.iter().any(|p| {
                        p.peer.len() == 20 && {
                            let mut arr = [0u8; 20];
                            arr.copy_from_slice(&p.peer);
                            NodeId(arr) == *peer
                                && p.repo == repo
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
                    "[bootstrap] 检测到差异: repo={} local={}, remote={}, ratio={:.1}%, 触发 bootstrap: peer={}",
                    repo, local_count, remote_count, ratio * 100.0, peer
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
