//! 联邦业务分派层（**从连接层剥离**）
//!
//! # 为什么要有这一层
//!
//! 迁移前，`ConnectionManager` 同时承担三件事：
//! ① 建连/握手/心跳（**通用**，已下沉 `pnos-net`）；
//! ② 43 种 `MessageType` 的业务分派（**纯 pdc 语义**，577 行）；
//! ③ 承载态（`pending` 背压计数、gossip 攒批缓冲）（**纯 pdc 语义**）。
//!
//! ②③ 若不剥离就上移，等于把 pdc 灌进通用库 —— 这正是 v1 方案被否决的原因
//! （迁移计划 §2.1 / §2.3）。本模块承接 ②③，使 `pnos-net` 只认识「帧 + 会话」。
//!
//! # 结构
//!
//! | 组件 | 角色 |
//! |---|---|
//! | [`PeerRuntime`] | 按 `peer_id` 索引的承载态（`pending` / gossip 攒批缓冲） |
//! | [`FederationDispatcher`] | 业务分派器：`dispatch()` 逐臂等价旧 `dispatch_message` |
//!
//! # 事件驱动（替代四个 `set_*` 反向注入）
//!
//! 旧实现由 `ConnectionManager` 在会话建立时反向注入 `discovery` / `sync_manager` /
//! `signaling_service` / `relay_manager` 四个业务对象（迁移计划 K3 要求全部移除）。
//! 新实现改为 pdc 侧订阅 `SessionEvent`：
//!
//! ```text
//! SessionEvent::Connected    → 写 PeerCapsTable（版本来自 PeerIdentity.metadata）
//! SessionEvent::Frame        → dispatch(PeerConn, kind, payload)
//! SessionEvent::Disconnected → 清 caps / 释放 PeerRuntime / 标记 node_table
//! SessionEvent::Rejected     → 仅日志
//! ```
//!
//! 本模块的 `set_*` 只用于**同一个进程内**把业务对象交给分派器
//! （`FederationService::new` 内建连对象晚于业务对象存在时的循环依赖解法），
//! 与「SDK 反向注入」是两回事：SDK 侧零注入。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use parking_lot::{Mutex as ParkingMutex, RwLock};
use tokio::sync::{broadcast, Notify, OnceCell, Semaphore};
use tracing::{debug, info, warn};

use crate::federation::config::FederationConfig;
use crate::federation::discovery::DiscoveryService;
use crate::federation::metrics::FederationMetrics;
use crate::federation::node_id::NodeId;
use crate::federation::node_table::NodeTable;
use crate::federation::peer_caps::PeerCapsTable;
use crate::federation::peer_conn::PeerConn;
use crate::federation::peer_query_store::PeerQueryStore;
use crate::federation::protocol::*;
use crate::federation::session::{from_sdk_node_id, version_from_metadata, FederationSessions};
use crate::federation::signaling::SignalingService;
use crate::federation::sync::SyncManager;

use pnos_net::session::{DisconnectReason, SessionEvent};

/// 主动断连钩子：把「断开某对端」与具体承载解耦
///
/// 迁移期指向 `ConnectionManager::remove_connection`（从连接表摘除 + 关 TCP）；
/// 翻转后指向 `FederationSessions::disconnect_peer_detached`（SDK 会话断连）。
/// 分派逻辑无需知道承载是谁。
type DisconnectFn = Arc<dyn Fn(NodeId, DisconnectReason) + Send + Sync + 'static>;

/// 单个对端的**承载态**（从 `Connection` 剥离）
///
/// 这三样东西此前挂在 `Connection` 上，但它们与「字节流怎么来的」完全无关：
/// - `pending`：接收端背压计数（每帧 +1，处理完 -1）
/// - `gossip_buffer`：`GossipBatch` 攒批缓冲（N 个 batch 只需 1 次 permit + 1 次 spawn）
/// - `gossip_flush_notify`：达到 `max_batches` 时立即唤醒 flush
///
/// 因此按 `peer_id` 索引独立存在，旧路径与 SDK 路径**共用同一张表**。
pub struct PeerRuntime {
    /// 对端节点 ID
    pub node_id: NodeId,
    /// 待处理消息计数（背压监控）
    pub pending: AtomicU32,
    /// `GossipBatch` 攒批缓冲区
    pub gossip_buffer: ParkingMutex<VecDeque<GossipBatchMessage>>,
    /// flush 唤醒通知
    pub gossip_flush_notify: Arc<Notify>,
}

impl PeerRuntime {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            pending: AtomicU32::new(0),
            gossip_buffer: ParkingMutex::new(VecDeque::new()),
            gossip_flush_notify: Arc::new(Notify::new()),
        }
    }

    /// 当前待处理消息数
    pub fn pending(&self) -> u32 {
        self.pending.load(Ordering::Relaxed)
    }
}

/// 联邦业务分派器
///
/// **零连接状态**：连接状态全在承载侧（`ConnectionManager` 或 SDK `SessionManager`）。
/// 本类型只持有业务对象与按 peer 索引的承载态。
pub struct FederationDispatcher {
    /// 配置
    config: FederationConfig,
    /// 节点表
    node_table: Arc<NodeTable>,
    /// 监控指标
    metrics: Arc<FederationMetrics>,
    /// 对端协议能力表
    caps: Arc<PeerCapsTable>,
    /// 实时 peer 查询结果收集器（PT 业务）
    peer_query_store: Arc<PeerQueryStore>,
    /// 重量级消息处理的有界并发信号量
    heavy_task_semaphore: Arc<Semaphore>,
    /// 节点发现服务（延迟注入，解决循环依赖）
    discovery: OnceCell<Arc<DiscoveryService>>,
    /// 同步管理器（延迟注入）
    sync_manager: OnceCell<Arc<SyncManager>>,
    /// 打洞信令服务（延迟注入）
    signaling_service: OnceCell<Arc<SignalingService>>,
    /// 按 peer 索引的承载态
    peers: DashMap<NodeId, Arc<PeerRuntime>>,
    /// 主动断连钩子（迁移期可整体替换）
    disconnect: RwLock<Option<DisconnectFn>>,
}

impl FederationDispatcher {
    /// 创建分派器
    pub fn new(
        config: FederationConfig,
        node_table: Arc<NodeTable>,
        metrics: Arc<FederationMetrics>,
        caps: Arc<PeerCapsTable>,
        peer_query_store: Arc<PeerQueryStore>,
    ) -> Self {
        let heavy_task_max_concurrency = config.heavy_task_max_concurrency.max(1);
        Self {
            config,
            node_table,
            metrics,
            caps,
            peer_query_store,
            heavy_task_semaphore: Arc::new(Semaphore::new(heavy_task_max_concurrency)),
            discovery: OnceCell::new(),
            sync_manager: OnceCell::new(),
            signaling_service: OnceCell::new(),
            peers: DashMap::new(),
            disconnect: RwLock::new(None),
        }
    }

    // ------------------------------------------------------------------
    // 延迟注入（进程内循环依赖解法；SDK 侧零注入）
    // ------------------------------------------------------------------

    /// 注入发现服务
    pub fn set_discovery(&self, discovery: Arc<DiscoveryService>) {
        let _ = self.discovery.set(discovery);
    }

    /// 注入同步管理器
    pub fn set_sync_manager(&self, sync_manager: Arc<SyncManager>) {
        let _ = self.sync_manager.set(sync_manager);
    }

    /// 注入打洞信令服务
    pub fn set_signaling_service(&self, signaling: Arc<SignalingService>) {
        let _ = self.signaling_service.set(signaling);
    }

    /// 注入主动断连钩子（可重复设置，后设覆盖）
    pub fn set_disconnector(&self, f: DisconnectFn) {
        *self.disconnect.write() = Some(f);
    }

    /// 对端协议能力表
    pub fn caps(&self) -> &Arc<PeerCapsTable> {
        &self.caps
    }

    // ------------------------------------------------------------------
    // 承载态管理
    // ------------------------------------------------------------------

    /// 取（必要时创建）某对端的承载态
    pub fn peer(&self, peer: NodeId) -> Arc<PeerRuntime> {
        if let Some(rt) = self.peers.get(&peer).map(|r| Arc::clone(r.value())) {
            return rt;
        }
        self.peers
            .entry(peer)
            .or_insert_with(|| Arc::new(PeerRuntime::new(peer)))
            .value()
            .clone()
    }

    /// 释放某对端的承载态（断连时调用，防表无界增长）
    pub fn forget(&self, peer: &NodeId) {
        self.peers.remove(peer);
    }

    /// 当前持有承载态的对端数
    pub fn tracked_peers(&self) -> usize {
        self.peers.len()
    }

    /// 遍历所有承载态（供 gossip flush 调度）
    pub fn runtimes(&self) -> Vec<Arc<PeerRuntime>> {
        self.peers.iter().map(|e| Arc::clone(e.value())).collect()
    }

    /// 主动断连（`Goodbye` 等业务触发）；未注入钩子时为无操作
    fn disconnect_peer(&self, peer: NodeId, reason: DisconnectReason) {
        // 先克隆再调用：不在持锁状态下执行外部代码
        let hook = self.disconnect.read().clone();
        match hook {
            Some(f) => f(peer, reason),
            None => debug!("[federation] 断连钩子未注入，忽略 disconnect({})", peer),
        }
    }

    // ------------------------------------------------------------------
    // gossip 攒批 flush（从 `ConnectionManager` 迁出）
    // ------------------------------------------------------------------

    /// 遍历所有对端并 flush 各自 gossip buffer（由 TaskScheduler 定期调度）
    pub async fn flush_all_gossip_buffers(self: &Arc<Self>) {
        let max_batches = self.config.gossip_flush_max_batches;
        for rt in self.runtimes() {
            self.clone().flush_gossip_buffer(rt, max_batches).await;
        }
    }

    // ------------------------------------------------------------------
    // 事件循环（迁移计划 §3.2：替代四个 `set_*` 反向注入）
    // ------------------------------------------------------------------

    /// 订阅 `SessionEvent` 并驱动业务分派
    ///
    /// 翻转后由 `FederationService` 调用；返回 `JoinHandle` 便于测试与关闭。
    pub fn spawn_event_loop(
        self: Arc<Self>,
        sessions: Arc<FederationSessions>,
    ) -> tokio::task::JoinHandle<()> {
        let mut rx = sessions.subscribe();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(SessionEvent::Connected { peer, addr, .. }) => {
                        let peer_id = from_sdk_node_id(peer);
                        // 协议版本经 `PeerIdentity.metadata` 透出（迁移计划 K4）：
                        // SDK 只透传，不解释 —— 由 pdc 解出并写入能力表。
                        let version = sessions
                            .session_of(&peer_id)
                            .map(|i| version_from_metadata(i.metadata.as_deref()))
                            .unwrap_or(1);
                        self.caps.set(peer_id, version);
                        let _ = self.peer(peer_id);
                        info!(
                            "[federation] 会话建立: {} @ {} (proto=v{})",
                            peer_id, addr, version
                        );
                    }
                    Ok(SessionEvent::Disconnected { peer, reason, .. }) => {
                        let peer_id = from_sdk_node_id(peer);
                        self.caps.remove(&peer_id);
                        self.forget(&peer_id);
                        self.node_table.mark_disconnected(&peer_id);
                        debug!("[federation] 会话断开: {}（{}）", peer_id, reason);
                    }
                    Ok(SessionEvent::Frame { peer, frame, .. }) => {
                        let peer_id = from_sdk_node_id(peer);
                        let kind = match u8::try_from(frame.kind) {
                            Ok(k) => k,
                            Err(_) => {
                                debug!("[federation] 帧 kind 超范围: {}", frame.kind);
                                continue;
                            }
                        };
                        let Some(mt) = MessageType::from_u8(kind) else {
                            debug!("[federation] 未知帧 kind={}", kind);
                            continue;
                        };
                        let pc = Arc::new(PeerConn::sdk(peer_id, sessions.clone()));
                        let rt = self.peer(peer_id);
                        rt.pending.fetch_add(1, Ordering::Relaxed);
                        let this = self.clone();
                        tokio::spawn(async move {
                            let payload = frame.payload.to_vec();
                            let offloaded = this.dispatch(pc, mt, payload).await;
                            if !offloaded {
                                rt.pending.fetch_sub(1, Ordering::Relaxed);
                            }
                        });
                    }
                    Ok(SessionEvent::Rejected { addr, reason }) => {
                        debug!("[federation] 入站被拒 {}: {}", addr, reason);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("[federation] 会话事件订阅落后 {} 条，已跳过", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!("[federation] 会话事件通道关闭，分派循环退出");
                        break;
                    }
                }
            }
        })
    }
}

impl std::fmt::Debug for FederationDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FederationDispatcher")
            .field("peers", &self.peers.len())
            .field("caps", &self.caps.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// 业务分派（逐臂等价迁移自 `ConnectionManager::dispatch_message`）
// ---------------------------------------------------------------------------

impl FederationDispatcher {
    /// 业务分派（**连接层不再认识业务**）
    ///
    /// 与旧 `ConnectionManager::dispatch_message` **逐臂等价**，差异仅在三处：
    /// - 入参由 `Arc<Connection>` 换成 [`PeerConn`]（承载无关的对端视图）；
    /// - 承载态（`pending` / gossip 攒批缓冲）改由 [`PeerRuntime`] 按 peer 索引持有；
    /// - 断连由注入的钩子表达，不再直接操作连接表。
    ///
    /// 返回值：`true` 表示本次消息已被异步卸载（重量级处理在后台执行，
    /// `pending` 计数由卸载任务负责递减）；`false` 表示同步处理已完成
    /// （调用方负责递减 `pending` 计数）。
    pub async fn dispatch(
        self: &Arc<Self>,
        pc: Arc<PeerConn>,
        msg_type: MessageType,
        payload: Vec<u8>,
    ) -> bool {
        // 承载态按 peer 索引：`pc` 指向旧 `Connection` 还是 SDK 会话都落在同一张表上，
        // 这正是翻转时本函数**一行都不用改**的原因。
        let rt = self.peer(pc.node_id);

        let offloaded = match msg_type {
            MessageType::Ping => {
                if let Ok(ping) = bincode::deserialize::<PingMessage>(&payload) {
                    let pong = PongMessage {
                        timestamp: ping.timestamp,
                        rtt_estimate_ms: 0,
                    };
                    let _ = pc.send_message(MessageType::Pong, &pong).await;
                }
                false
            }
            MessageType::Pong => {
                if let Ok(pong) = bincode::deserialize::<PongMessage>(&payload) {
                    self.node_table
                        .update_rtt(&pc.node_id, pong.rtt_estimate_ms);
                    debug!(
                        "[federation] 收到 Pong from {} (rtt={}ms)",
                        pc.node_id, pong.rtt_estimate_ms
                    );
                }
                false
            }
            MessageType::GetNodes => {
                if let Ok(req) = bincode::deserialize::<GetNodesMessage>(&payload) {
                    if let Some(discovery) = self.discovery.get() {
                        discovery.handle_get_nodes(&pc, req.count).await;
                    }
                }
                false
            }
            MessageType::Nodes => {
                if let Ok(msg) = bincode::deserialize::<NodesMessage>(&payload) {
                    if let Some(discovery) = self.discovery.get() {
                        discovery.handle_nodes_received(msg.nodes);
                    }
                }
                false
            }
            MessageType::ExchangeNodes => {
                if let Ok(msg) = bincode::deserialize::<ExchangeNodesMessage>(&payload) {
                    if let Some(discovery) = self.discovery.get() {
                        discovery.handle_exchange_nodes(msg.nodes);
                    }
                }
                false
            }
            MessageType::SyncBatch => {
                // TODO(P1): handle_sync_batch 涉及大量 DB 写入，后续批量写入优化时一并异步化。
                // 当前为旧 stage-1 协议，主路径已走 GossipBatch（已异步卸载）。
                if let Ok(msg) = bincode::deserialize::<SyncBatchMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        sync_mgr.handle_sync_batch(msg.repo_type, &msg.entries);
                    }
                }
                false
            }
            MessageType::GossipBatch => {
                self.metrics.record_message_recv();
                // P1: 接收端攒批缓冲 —— 不立即 spawn，而是 push 到 per-connection buffer，
                // 由后台 flush 任务统一处理。N 个 batch 只需要 1 次 permit + 1 次 spawn。
                // P0-2: flush 任务内不使用 spawn_blocking（纯内存操作）。
                if let Ok(batch) = bincode::deserialize::<GossipBatchMessage>(&payload) {
                    debug!(
                        "[federation][perf] 收到 GossipBatch: entries={}",
                        batch.entries.len()
                    );
                    {
                        let mut buf = rt.gossip_buffer.lock();
                        let diag_repo_type = batch.repo_type;
                        let diag_entries = batch.entries.len();
                        buf.push_back(batch);
                        debug!("[federation][DIAG] GossipBatch received: repo_type={}, entries={}, buffer_len={}", diag_repo_type, diag_entries, buf.len());
                    }
                    // 通知 flush 任务（达到 max_batches 时立即刷新，否则等定时 tick）
                    rt.gossip_flush_notify.notify_one();
                }
                true
            }
            MessageType::GossipBatchBulk => {
                // 批量合并帧：将多个 batch 拆出后逐个加入 per-connection 缓冲，
                // 复用现有攒批 flush 机制，无需额外处理逻辑。
                self.metrics.record_message_recv();
                if let Ok(bulk) = bincode::deserialize::<GossipBatchBulkMessage>(&payload) {
                    let count = bulk.batches.len();
                    // 计算所有 batch 的 entries 总数（在 move 进 buffer 之前）
                    let total_entries: usize = bulk.batches.iter().map(|b| b.entries.len()).sum();
                    debug!(
                        "[federation][perf] 收到 GossipBatchBulk: {} 个 batch, 总条目 {}",
                        count, total_entries
                    );
                    {
                        let mut buf = rt.gossip_buffer.lock();
                        for batch in bulk.batches {
                            buf.push_back(batch);
                        }
                        debug!("[federation][DIAG] GossipBatchBulk received: batches={}, buffer_len={}", count, buf.len());
                    }
                    // 主循环已对每条消息 +1，Bulk 包含 N 个 batch，需补 +(N-1) 使计数与
                    // flush_gossip_buffer 按 batch 数 -group_len 递减匹配，避免 pending 下溢为巨大值触发虚假背压。
                    if count > 1 {
                        rt.pending.fetch_add((count - 1) as u32, Ordering::Relaxed);
                    }
                    rt.gossip_flush_notify.notify_one();
                }
                true
            }
            MessageType::MerkleDigest => {
                self.metrics.record_message_recv();
                if let Ok(digest) = bincode::deserialize::<MerkleDigestMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_merkle_digest(&conn, digest).await;
                        });
                    }
                }
                false
            }
            MessageType::MerkleRequest => {
                self.metrics.record_message_recv();
                if let Ok(request) = bincode::deserialize::<MerkleRequestMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_merkle_request(&conn, request).await;
                        });
                    }
                }
                false
            }
            MessageType::MerkleRepair => {
                self.metrics.record_message_recv();
                // P0-2: MerkleRepair 也是纯内存 + 批量 SQLite 写入，走 async handler（去掉 spawn_blocking 开销）
                if let Some(sync_mgr) = self.sync_manager.get() {
                    let sync_mgr = sync_mgr.clone();
                    let conn = rt.clone();
                    self.spawn_async_handler(conn, move || async move {
                        if let Ok(repair) = bincode::deserialize::<MerkleRepairMessage>(&payload) {
                            sync_mgr.handle_merkle_repair(repair);
                        }
                    });
                    true
                } else {
                    false
                }
            }
            MessageType::FullSyncStart => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<FullSyncStartMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        info!(
                            "[federation] 收到 FullSyncStart: repo_type={}, total={}, from={}",
                            msg.repo_type, msg.total_entries, pc.node_id
                        );
                        sync_mgr.handle_full_sync_start(msg.repo_type);
                    }
                }
                false
            }
            MessageType::FullSyncBatch => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<FullSyncBatchMessage>(&payload) {
                    let repo_type = msg.repo_type;
                    let seq = msg.seq;
                    let count = msg.entries.len();
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        // 直接应用到 repo（不经过 Gossip 去重）
                        sync_mgr.handle_full_sync_batch(repo_type, &msg.entries);
                        // 更新差量同步进度（防止无进度超时）
                        sync_mgr.update_diff_progress();
                        // 回复 Ack
                        let ack = FullSyncAckMessage { repo_type, seq };
                        let _ = pc.send_message(MessageType::FullSyncAck, &ack).await;
                        debug!(
                            "[federation] FullSyncBatch 应用: repo_type={}, seq={}, entries={}",
                            repo_type, seq, count
                        );
                    }
                }
                false
            }
            MessageType::FullSyncAck => {
                // 发送端接收 Ack，简化版不做流控等待
                false
            }
            MessageType::DiffSyncRequest => {
                // 差量同步：对端携带 Merkle 摘要，本节点对比后只推送差异分片数据。
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    let sync_mgr = sync_mgr.clone();
                    let conn = pc.clone();
                    if let Ok(req) = bincode::deserialize::<DiffSyncRequestMessage>(&payload) {
                        tokio::spawn(async move {
                            sync_mgr.handle_diff_sync_request(conn.node_id, req).await;
                        });
                    }
                }
                false
            }
            MessageType::DiffSyncKeyRequest => {
                // P0-2: 数据服务器发来差异分片 key 列表分片，对比本地后回传缺失 key。
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    let sync_mgr = sync_mgr.clone();
                    let conn = pc.clone();
                    if let Ok(msg) = bincode::deserialize::<DiffSyncKeyRequestMessage>(&payload) {
                        tokio::spawn(async move {
                            sync_mgr.handle_diff_sync_key_request(&conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::DiffSyncKeyResponse => {
                // P0-2: 请求方回传缺失 key 列表分片，唤醒等待中的数据服务器推送任务。
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<DiffSyncKeyResponseMessage>(&payload) {
                        sync_mgr.handle_diff_sync_key_response(&pc, msg);
                    }
                }
                false
            }
            MessageType::PeerInfo => {
                // 握手后对端发来的节点信息（本地各 repo 条目数），记录到 peer_digests
                // 供全量同步数据源选择时判断数据完整度。
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<PeerInfoMessage>(&payload) {
                        sync_mgr.handle_peer_info(pc.node_id, msg.local_entry_counts);
                    }
                }
                false
            }
            MessageType::FullSyncComplete => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<FullSyncCompleteMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        info!(
                            "[federation] 收到 FullSyncComplete: repo_type={}, from={}",
                            msg.repo_type, pc.node_id
                        );
                        sync_mgr.handle_full_sync_complete(msg.repo_type);
                    }
                }
                false
            }
            MessageType::Signaling => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<SignalingMessage>(&payload) {
                    if let Some(signaling) = self.signaling_service.get() {
                        signaling.handle_signaling(pc.node_id, msg);
                    }
                }
                false
            }
            MessageType::RelaySetup => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<RelaySetupMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        sync_mgr.handle_relay_setup(pc.node_id, msg);
                    }
                }
                false
            }
            MessageType::RelayData => {
                self.metrics.record_message_recv();
                if let Ok(msg) = bincode::deserialize::<RelayDataMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        sync_mgr.handle_relay_data(pc.node_id, msg);
                    }
                }
                false
            }
            MessageType::Goodbye => {
                debug!("[federation] 收到 Goodbye from {}", pc.node_id);
                self.disconnect_peer(pc.node_id, DisconnectReason::Local);
                false
            }
            MessageType::PeerQueryRequest => {
                // 实时 peer 查询请求：从本地 PeerRepo 查询该 infohash 的 peer 并回复
                self.metrics.record_message_recv();
                if let Ok(req) = bincode::deserialize::<PeerQueryRequestMessage>(&payload) {
                    let peers: Vec<PeerQueryEntry> = if let Some(sync_mgr) = self.sync_manager.get()
                    {
                        sync_mgr.query_peers_for_infohash(&req.infohash, req.limit as usize)
                    } else {
                        Vec::new()
                    };
                    let resp = PeerQueryResponseMessage {
                        infohash: req.infohash,
                        peers,
                    };
                    if let Err(e) = pc.send_message(MessageType::PeerQueryResponse, &resp).await {
                        debug!(
                            "[federation] PeerQueryResponse 发送到 {} 失败: {}",
                            pc.node_id, e
                        );
                    }
                }
                false
            }
            MessageType::PeerQueryResponse => {
                // 实时 peer 查询响应：写入本地 PeerRepo，并推入响应收集器供 query_peers 取走
                self.metrics.record_message_recv();
                if let Ok(resp) = bincode::deserialize::<PeerQueryResponseMessage>(&payload) {
                    if let Some(sync_mgr) = self.sync_manager.get() {
                        sync_mgr.add_remote_peers(&resp.infohash, &resp.peers);
                    }
                    if !resp.peers.is_empty() {
                        self.peer_query_store.push_many(resp.infohash, resp.peers);
                    }
                }
                false
            }
            MessageType::MerkleLevelRequest => {
                // 分层 Merkle 层级请求：返回指定层级的子哈希列表
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    let sync_mgr = sync_mgr.clone();
                    let conn = pc.clone();
                    if let Ok(req) = bincode::deserialize::<MerkleLevelRequestMessage>(&payload) {
                        tokio::spawn(async move {
                            sync_mgr.handle_merkle_level_request(conn, req).await;
                        });
                    }
                }
                false
            }
            MessageType::MerkleLevelResponse => {
                // 分层 Merkle 层级响应：继续逐层对比流程
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    let sync_mgr = sync_mgr.clone();
                    let conn = pc.clone();
                    if let Ok(resp) = bincode::deserialize::<MerkleLevelResponseMessage>(&payload) {
                        tokio::spawn(async move {
                            sync_mgr.handle_merkle_level_response(conn, resp).await;
                        });
                    }
                }
                false
            }
            MessageType::ShardSyncBatch => {
                // 分片同步批次：接收并应用条目，回复 Ack
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<ShardSyncBatchMessage>(&payload) {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        sync_mgr.handle_shard_sync_batch(conn, msg);
                    }
                }
                false
            }
            MessageType::ShardSyncAck => {
                // 分片同步确认：唤醒发送端等待的 oneshot
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<ShardSyncAckMessage>(&payload) {
                        sync_mgr.handle_shard_sync_ack(msg);
                    }
                }
                false
            }
            MessageType::ShardSyncComplete => {
                // 分片同步完成：标记同步结束，触发 Merkle 重建
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<ShardSyncCompleteMessage>(&payload) {
                        info!(
                            "[federation] 收到 ShardSyncComplete: repo_type={}, from={}",
                            msg.repo_type, pc.node_id
                        );
                        sync_mgr.handle_shard_sync_complete(msg);
                    }
                }
                false
            }
            MessageType::ShardSyncHashList => {
                // 分片同步 hash 列表：对比本地 DB，回复缺失 key 列表（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<ShardSyncHashListMessage>(&payload) {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_shard_sync_hash_list(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::ShardSyncMissing => {
                // 分片同步缺失 key 列表：唤醒发送端等待的 oneshot
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<ShardSyncMissingMessage>(&payload) {
                        sync_mgr.handle_shard_sync_missing(msg);
                    }
                }
                false
            }
            MessageType::OpsRequest => {
                // P1-3：增量拉取请求（数据服务器侧）—— 从本地 oplog 取 seq>since_seq 回 OpsBatch（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<OpsRequestMessage>(&payload) {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_ops_request(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::OpsBatch => {
                // P1-3：增量拉取响应（请求方侧）—— 幂等应用 ops、推进版本向量、续拉下一批（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<OpsBatchMessage>(&payload) {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_ops_batch(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::RangeReconcileRequest => {
                // P1-4：Range-based 反熵请求（应答方）—— 回该区间摘要 + 分界点/行指纹（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<RangeReconcileRequestMessage>(&payload)
                    {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_range_reconcile_request(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::RangeReconcileResponse => {
                // P1-4：Range-based 反熵响应（请求方）—— 剪枝 / 求差 / 继续下钻（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<RangeReconcileResponseMessage>(&payload)
                    {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_range_reconcile_response(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::RangeReconcilePush => {
                // P1-4：Range 反熵推送（接收方）—— 写入本地数据库
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<RangeReconcilePushMessage>(&payload) {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_range_reconcile_push(conn, msg).await;
                        });
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            MessageType::SyncNegotiate => {
                // v7：建连协商请求（应答方）—— 按策略裁定回 Ack（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<SyncNegotiateMessage>(&payload) {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_sync_negotiate(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::SyncNegotiateAck => {
                // v7：建连协商确认（请求方）—— 存策略表，大通道放行（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<SyncNegotiateAckMessage>(&payload) {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_sync_negotiate_ack(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::BootstrapManifestRequest => {
                // P2-1：bootstrap 清单请求（应答方）—— 建 w0 水位 + 有序逻辑分块清单（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) =
                        bincode::deserialize::<BootstrapManifestRequestMessage>(&payload)
                    {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_bootstrap_manifest_request(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::BootstrapManifestResponse => {
                // P2-1：bootstrap 清单响应（请求方）—— 保存进度并开始拉第一块（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) =
                        bincode::deserialize::<BootstrapManifestResponseMessage>(&payload)
                    {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_bootstrap_manifest_response(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::BootstrapChunkRequest => {
                // P2-1：bootstrap 分块请求（应答方）—— 按区间取条目回发（受令牌桶限流，异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<BootstrapChunkRequestMessage>(&payload)
                    {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_bootstrap_chunk_request(conn, msg).await;
                        });
                    }
                }
                false
            }
            MessageType::BootstrapChunkResponse => {
                // P2-1：bootstrap 分块响应（请求方）—— 批量 upsert 落块、校验、续拉/切追尾（异步）
                self.metrics.record_message_recv();
                if let Some(sync_mgr) = self.sync_manager.get() {
                    if let Ok(msg) = bincode::deserialize::<BootstrapChunkResponseMessage>(&payload)
                    {
                        let sync_mgr = sync_mgr.clone();
                        let conn = pc.clone();
                        tokio::spawn(async move {
                            sync_mgr.handle_bootstrap_chunk_response(conn, msg).await;
                        });
                    }
                }
                false
            }
            _ => {
                debug!(
                    "[federation] 收到未处理消息类型 {:?} from {}",
                    msg_type, pc.node_id
                );
                false
            }
        };

        offloaded
    }

    /// P0-2: 异步执行纯内存消息处理（不经过 spawn_blocking）。
    ///
    /// GossipBatch 处理是纯内存操作（HashMap + Merkle），不需要阻塞线程池。
    /// 仍然通过 semaphore 限制并发，避免无界生成异步任务导致 CPU 飙升。
    fn spawn_async_handler<F, Fut>(&self, conn: Arc<PeerRuntime>, f: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let sem = self.heavy_task_semaphore.clone();
        tokio::spawn(async move {
            let _permit = match sem.acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    conn.pending.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
            };
            f().await;
            drop(_permit);
            conn.pending.fetch_sub(1, Ordering::Relaxed);
        });
    }

    /// P0-1: 刷新 per-connection GossipBatch 缓冲区（按 repo_type 并行分流）。
    ///
    /// 一次性 drain 所有缓冲的 GossipBatch，按 `repo_type` 分成最多 4 组
    /// （NODE/PEER/INFOHASH/TRACKER）。每个非空组独立 spawn 一个 tokio task，
    /// 各自获取 1 个 `heavy_task_semaphore` permit 后顺序处理本组 batch。
    /// 组间并行，组内顺序；每个 task 完成后按本组 batch 数递减 `conn.pending`。
    async fn flush_gossip_buffer(self: &Arc<Self>, conn: Arc<PeerRuntime>, _max_batches: usize) {
        debug!(
            "[federation][DIAG] flush_gossip_buffer ENTER, buffer_len={}",
            conn.gossip_buffer.lock().len()
        );
        // 限频：每10次 flush tick 输出1次，用于确认 flush task 存活并观察 buffer 积压。
        // 放在 drain/early-return 之前，即使 buffer 为空也能看到 tick。
        static FLUSH_TICK_COUNT: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let tick = FLUSH_TICK_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if tick.is_multiple_of(10) {
            let buffer_len = conn.gossip_buffer.lock().len();
            debug!("[federation][perf] flush tick: buffer_len={}", buffer_len);
        }

        let total_start = Instant::now();

        // 一次性 drain 所有缓冲的 batch
        let drain_start = Instant::now();
        let batches: Vec<GossipBatchMessage> = {
            let mut buf = conn.gossip_buffer.lock();
            if buf.is_empty() {
                return;
            }
            buf.drain(..).collect()
        };
        let drain_elapsed = drain_start.elapsed();
        if batches.is_empty() {
            return;
        }

        // 优化2：接收端提前去重 —— 在按 repo_type 分组 / spawn task 前，
        // 一次性过滤掉已处理的重复 batch。重复 batch 此前仍消耗反序列化、缓冲、
        // 分组、task spawn、clone 的 CPU；提前丢弃可降低 90%+ 重复消息开销。
        // 只读检查（seen_msgs.contains），不插入；handle_gossip_batch 中的
        // check_and_put 仍保留作为兜底（防止本检查与实际处理间的竞态）。
        let batches: Vec<GossipBatchMessage> = if let Some(sync_mgr) = self.sync_manager.get() {
            let before = batches.len();
            let filtered: Vec<GossipBatchMessage> = batches
                .into_iter()
                .filter(|b| !sync_mgr.is_batch_seen(b))
                .collect();
            let dropped = before - filtered.len();
            if dropped > 0 {
                debug!(
                    "[federation][perf] flush 提前去重: 丢弃 {} 条重复 batch（剩余 {}）",
                    dropped,
                    filtered.len()
                );
            }
            filtered
        } else {
            batches
        };
        if batches.is_empty() {
            return;
        }

        let batch_count = batches.len();
        debug!("[federation][perf] flush drain: batches={}", batches.len());

        // 按 repo_type 分流到最多 4 组（使用 protocol::repo_type 常量，不硬编码数值）
        let mut node_group: Vec<GossipBatchMessage> = Vec::new();
        let mut peer_group: Vec<GossipBatchMessage> = Vec::new();
        let mut infohash_group: Vec<GossipBatchMessage> = Vec::new();
        let mut tracker_group: Vec<GossipBatchMessage> = Vec::new();
        for batch in batches {
            match batch.repo_type {
                repo_type::NODE => node_group.push(batch),
                repo_type::PEER => peer_group.push(batch),
                repo_type::INFOHASH => infohash_group.push(batch),
                repo_type::TRACKER => tracker_group.push(batch),
                _ => {
                    debug!(
                        "[federation] flush GossipBatch 遇到未知 repo_type={}, 丢弃",
                        batch.repo_type
                    );
                }
            }
        }
        let groups = [node_group, peer_group, infohash_group, tracker_group];
        let group_count = groups.iter().filter(|g| !g.is_empty()).count();

        // 每个非空组 spawn 独立 task，各自获取 1 个 permit（与 GossipBatch/MerkleRepair 共用）
        for group in groups {
            let group_len = group.len();
            if group_len == 0 {
                continue;
            }
            let sem = self.heavy_task_semaphore.clone();
            let conn_task = conn.clone();
            let self_task = self.clone();
            tokio::spawn(async move {
                // 等待 permit：信号量饱和时在此排队，对应 batch 计入 pending（背压可见）
                let _permit = match sem.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        conn_task
                            .pending
                            .fetch_sub(group_len as u32, Ordering::Relaxed);
                        return;
                    }
                };
                // 顺序处理本组 batch（纯内存操作，不经过 spawn_blocking）
                if let Some(sync_mgr) = self_task.sync_manager.get().cloned() {
                    for batch in group {
                        sync_mgr.handle_gossip_batch(batch);
                    }
                }
                drop(_permit);
                conn_task
                    .pending
                    .fetch_sub(group_len as u32, Ordering::Relaxed);
            });
        }

        let total_elapsed = total_start.elapsed();
        debug!(
            "[federation][perf] flush_gossip_buffer: batches={} groups={} drain={}ms total={}ms",
            batch_count,
            group_count,
            drain_elapsed.as_millis(),
            total_elapsed.as_millis()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dispatcher() -> FederationDispatcher {
        let cfg = FederationConfig {
            listen_port: 0,
            max_connections: 10,
            ..Default::default()
        };
        FederationDispatcher::new(
            cfg,
            Arc::new(NodeTable::new(100)),
            Arc::new(FederationMetrics::new()),
            Arc::new(PeerCapsTable::new()),
            Arc::new(PeerQueryStore::new()),
        )
    }

    fn id(b: u8) -> NodeId {
        NodeId([b; 20])
    }

    fn batch(msg_id: u64) -> GossipBatchMessage {
        GossipBatchMessage {
            msg_id,
            origin: [0u8; 20],
            repo_type: repo_type::NODE,
            entries: Vec::new(),
            timestamp: 0,
            serialized_size: 0,
        }
    }

    #[test]
    fn runtime_starts_empty() {
        let rt = PeerRuntime::new(id(1));
        assert_eq!(rt.pending(), 0);
        assert_eq!(rt.node_id, id(1));
        assert!(rt.gossip_buffer.lock().is_empty());
    }

    #[test]
    fn peer_is_identity_stable() {
        let d = test_dispatcher();
        let a = d.peer(id(1));
        let b = d.peer(id(1));
        assert!(Arc::ptr_eq(&a, &b), "同一 peer 必须返回同一份承载态");
        assert_eq!(d.tracked_peers(), 1);

        let other = d.peer(id(2));
        assert!(!Arc::ptr_eq(&a, &other));
        assert_eq!(d.tracked_peers(), 2);
    }

    #[test]
    fn forget_releases_runtime() {
        let d = test_dispatcher();
        let _ = d.peer(id(3));
        assert_eq!(d.tracked_peers(), 1);
        d.forget(&id(3));
        assert_eq!(d.tracked_peers(), 0);
        // 释放后重新取到的是**新**实例（旧承载态不可复用）
        let fresh = d.peer(id(3));
        assert_eq!(fresh.pending(), 0);
    }

    #[test]
    fn runtimes_snapshot_covers_all_peers() {
        let d = test_dispatcher();
        let _ = d.peer(id(4));
        let _ = d.peer(id(5));
        let ids: Vec<u8> = d.runtimes().iter().map(|r| r.node_id.0[0]).collect();
        assert_eq!(d.runtimes().len(), 2);
        assert!(ids.contains(&4) && ids.contains(&5));
    }

    #[test]
    fn caps_table_is_shared_not_copied() {
        let d = test_dispatcher();
        d.caps().set(id(6), 6);
        assert_eq!(d.caps().of(&id(6)).version(), 6);
        // 未记录的对端回退到最保守版本
        assert_eq!(d.caps().of(&id(7)).version(), 1);
    }

    #[test]
    fn business_objects_start_uninjected() {
        // 启动早期（`FederationService::new` 之前）业务对象尚未注入：
        // 分派器必须处于「可安全调用」状态而非 panic 状态。
        let d = test_dispatcher();
        assert!(d.discovery.get().is_none());
        assert!(d.sync_manager.get().is_none());
        assert!(d.signaling_service.get().is_none());
    }

    #[test]
    fn disconnector_is_replaceable() {
        use std::sync::atomic::{AtomicUsize, Ordering as O};
        let d = test_dispatcher();
        let calls = Arc::new(AtomicUsize::new(0));
        let c1 = calls.clone();
        d.set_disconnector(Arc::new(move |_, _| {
            c1.fetch_add(1, O::Relaxed);
        }));
        let c2 = calls.clone();
        // 后设覆盖：迁移期"指向 ConnectionManager"→"指向 SDK"的替换点
        d.set_disconnector(Arc::new(move |_, _| {
            c2.fetch_add(10, O::Relaxed);
        }));
        d.disconnect_peer(id(8), DisconnectReason::Local);
        assert_eq!(calls.load(O::Relaxed), 10, "必须是后注入的钩子生效");
    }

    #[test]
    fn disconnect_without_hook_is_noop() {
        let d = test_dispatcher();
        // 未注入钩子时必须安静通过，不能 panic（启动早期路径）
        d.disconnect_peer(id(9), DisconnectReason::Local);
    }

    #[tokio::test]
    async fn flush_drains_same_peer_buffers_across_reallocation() {
        // 承载态按 peer 索引后，"旧连接 A 的缓冲"与"新连接 A 的缓冲"是同一条，
        // 这正是旧实现（缓冲挂在 Connection 实例上）无法保证的性质。
        let d = Arc::new(test_dispatcher());
        let rt = d.peer(id(1));
        {
            let mut buf = rt.gossip_buffer.lock();
            buf.push_back(batch(1));
            buf.push_back(batch(2));
        }
        rt.pending.fetch_add(2, Ordering::Relaxed);
        assert_eq!(rt.gossip_buffer.lock().len(), 2);

        d.flush_all_gossip_buffers().await;

        // drain 是同步发生的：无论是否注入 sync_manager，缓冲必须清空
        assert!(rt.gossip_buffer.lock().is_empty());
        // 未注入 sync_manager → 不处理业务，但 pending 由 group task 递减归零
        for _ in 0..200 {
            if rt.pending() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(rt.pending(), 0, "flush 完成后 pending 必须归零");
    }

    #[tokio::test]
    async fn flush_on_empty_table_is_noop() {
        let d = Arc::new(test_dispatcher());
        d.flush_all_gossip_buffers().await;
        assert_eq!(d.tracked_peers(), 0);
    }

    #[test]
    fn runtime_is_not_shared_between_peers() {
        let d = test_dispatcher();
        let a = d.peer(id(1));
        a.pending.fetch_add(5, Ordering::Relaxed);
        let b = d.peer(id(2));
        assert_eq!(b.pending(), 0, "承载态必须按 peer 隔离");
        assert_eq!(d.peer(id(1)).pending(), 5);
    }
}
