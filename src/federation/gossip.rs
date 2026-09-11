//! Gossip 引擎
//!
//! 基于流行病协议的消息传播。节点将同步数据提交到 outbox，
//! 后台任务定期随机选择 fanout 个邻居传播。已处理消息通过 LRU 去重。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use rand::SeedableRng;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::broadcast;
use tracing::{debug, error, warn};

use crate::federation::config::FederationConfig;
use crate::federation::connection::{Connection, ConnectionManager};
use crate::federation::merkle::MerkleProvider;
use crate::federation::metrics::FederationMetrics;
use crate::federation::node_id::NodeId;
use crate::federation::protocol::*;
use crate::federation::sharded_lru::ShardedLruCache;

/// Gossip 引擎
pub struct GossipEngine {
    /// 待传播队列
    outbox: RwLock<Vec<GossipBatchMessage>>,
    /// outbox 中当前存在的 (origin, msg_id) 集合，防止重复条目进入 outbox。
    /// 与 outbox Vec 同步维护：push 前检查插入，drain 后移除。
    /// 防止 seen_msgs LRU 驱逐后同一消息被 handle_gossip_batch 重复加入 outbox。
    outbox_msg_ids: RwLock<FxHashSet<(NodeId, u64)>>,
    /// 已处理消息 ID 去重（按 (origin_node, msg_id) 全局唯一去重）。
    /// 分片 LRU：按 key 哈希分片，消除多 repo 并行 flush 时的全局写锁竞争。
    seen_msgs: ShardedLruCache<(NodeId, u64), ()>,
    /// 连接管理器
    connection_manager: Arc<ConnectionManager>,
    /// 配置
    config: FederationConfig,
    /// 消息 ID 计数器
    next_msg_id: AtomicU64,
    /// 本地节点 ID
    local_node_id: NodeId,
    /// 指标
    metrics: Arc<FederationMetrics>,
    /// 关闭信号
    shutdown: broadcast::Sender<()>,
    /// 每个 batch 的重试次数计数（按 msg_id），超过 MAX_RETRIES 则丢弃
    retry_counts: RwLock<FxHashMap<u64, u32>>,
    /// 每个节点的 Gossip 连续发送失败计数，达到阈值则断开连接
    consecutive_failures: RwLock<FxHashMap<NodeId, u32>>,
    /// 发送端速率限制：当前统计窗口的起始 unix 秒（固定窗口计数器，全局维度）
    rate_window_secs: AtomicU64,
    /// 当前窗口已发送字节数（含帧头估算）
    bytes_in_window: AtomicU64,
    /// 当前窗口已发送消息条数
    msgs_in_window: AtomicU64,
    /// 正在发送中的 batch 数量（已从 outbox 取出但未完成发送）。
    /// wait_outbox_empty 需把它计入，避免 outbox 暂时为空时误判同步完成。
    in_flight_count: AtomicUsize,
    /// 本节点正在接收全量数据（接收方）：为 true 时收到的 Gossip 只本地写入，
    /// 不 push 到 outbox 转发（防多源重复洪峰）。不影响发送端限流/outbox 防护。
    receiving_full_sync: AtomicBool,
    /// 本节点正在向对端发送全量数据（发送方）：为 true 时临时放开 Gossip 发送限流、
    /// 缩短传播 tick、跳过 outbox 截断。不影响接收端是否转发。
    /// 用 Arc 包装以便 pause_gate() 向 crawler/dht/健康检查等外部模块共享同一信号。
    sending_full_sync: Arc<AtomicBool>,
}

/// 单个连接的发送结果集合（并行 spawn 后由主任务合并）
#[derive(Default)]
struct ConnSendResult {
    failed_msg_ids: std::collections::HashSet<u64>,
    rate_skipped_ids: std::collections::HashSet<u64>,
    /// 至少在一个连接上发送成功的 batch msg_id。
    /// 用于判定 batch 是否已传播到网络中（gossip 只需至少一个邻居收到即可继续传播）。
    successful_msg_ids: std::collections::HashSet<u64>,
    failed_nodes: FxHashSet<NodeId>,
    successful_nodes: FxHashSet<NodeId>,
}

/// 单个 batch 最大重试次数，超过则丢弃
const MAX_RETRIES: u32 = 3;

/// 本地序列化辅助结构：与线网格式 GossipBatchBulkMessage 字段布局完全一致，
/// 但持有引用而非所有权，避免发送 bulk 时深拷贝 batch 数据。
/// bincode 按字段顺序序列化，&T 与 T 产出相同字节。
#[derive(serde::Serialize)]
struct BulkRef<'a> {
    batches: Vec<&'a GossipBatchMessage>,
}

/// seen_msgs 去重缓存总容量。
/// 6 节点全互联场景下，全量同步 + 增量 gossip 可产生数万条唯一 (origin, msg_id)。
/// 原 10000 容量导致 LRU 频繁驱逐旧条目，被驱逐的消息经其他 gossip 路径再次到达时
/// 被误判为新消息并重新加入 outbox，形成消息循环与队列积压。提升至 100000 以覆盖
/// 长时间运行的去重需求（内存约 8MB，可接受）。
/// 每片容量 = 总容量 / 分片数（向上取整，至少为 1）。
const SEEN_MSGS_TOTAL_CAPACITY: usize = 100_000;

impl GossipEngine {
    /// 创建 Gossip 引擎
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        config: FederationConfig,
        local_node_id: NodeId,
        metrics: Arc<FederationMetrics>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        let shards = config.gossip_seen_shards.max(1);
        let cap_per_shard = (SEEN_MSGS_TOTAL_CAPACITY + shards - 1) / shards;
        Self {
            outbox: RwLock::new(Vec::new()),
            outbox_msg_ids: RwLock::new(FxHashSet::default()),
            seen_msgs: ShardedLruCache::new(shards, cap_per_shard),
            connection_manager,
            config,
            next_msg_id: AtomicU64::new(1),
            local_node_id,
            metrics,
            shutdown,
            retry_counts: RwLock::new(FxHashMap::default()),
            consecutive_failures: RwLock::new(FxHashMap::default()),
            rate_window_secs: AtomicU64::new(0),
            bytes_in_window: AtomicU64::new(0),
            msgs_in_window: AtomicU64::new(0),
            in_flight_count: AtomicUsize::new(0),
            receiving_full_sync: AtomicBool::new(false),
            sending_full_sync: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 设置「接收全量数据」标记（接收方：由 SyncManager 在发起拉取 / 收到 FullSyncStart 时调用）
    pub fn set_receiving_full_sync(&self, v: bool) {
        self.receiving_full_sync.store(v, Ordering::Relaxed);
    }

    /// 查询是否正在接收全量数据
    pub fn is_receiving_full_sync(&self) -> bool {
        self.receiving_full_sync.load(Ordering::Relaxed)
    }

    /// 设置「发送全量数据」标记（发送方：由 SyncManager 在 trigger_initial_sync / start_full_sync 期间调用）
    pub fn set_sending_full_sync(&self, v: bool) {
        self.sending_full_sync.store(v, Ordering::Relaxed);
    }

    /// 查询是否正在发送全量数据
    pub fn is_sending_full_sync(&self) -> bool {
        self.sending_full_sync.load(Ordering::Relaxed)
    }

    /// 查询全量同步是否进行中（接收或发送任一）
    pub fn is_full_sync_in_progress(&self) -> bool {
        self.is_receiving_full_sync() || self.is_sending_full_sync()
    }

    /// 仅检查 batch 是否已处理（只读，不插入 seen_msgs）。
    ///
    /// 供 ConnectionManager::flush_gossip_buffer 在分组/spawn task 前提前过滤重复 batch，
    /// 避免重复消息仍消耗反序列化、缓冲、分组、task spawn、clone 的 CPU。
    /// 返回 true 表示已处理（调用方应丢弃）；false 表示未处理（可继续处理）。
    /// handle_gossip_batch 中的 check_and_put 仍保留作为兜底（防止本检查与实际处理间的竞态）。
    pub fn is_batch_seen(&self, origin: NodeId, msg_id: u64) -> bool {
        self.seen_msgs.contains(&(origin, msg_id))
    }

    /// 获取全量同步标记的共享句柄（供非核心模块在全量同步期间暂停检查）。
    /// 反映发送端状态：对外（crawler/dht/健康检查）在本节点推送全量期间暂停主动工作，
    /// 把带宽/CPU 让给联邦同步；接收端是否暂停不通过此门控。
    pub fn pause_gate(&self) -> Arc<AtomicBool> {
        self.sending_full_sync.clone()
    }

    /// 提交同步数据到 outbox
    pub fn submit_gossip(&self, repo_type: u8, entries: Vec<SyncEntry>) {
        if entries.is_empty() {
            return;
        }
        let msg_id = self.next_msg_id.fetch_add(1, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry_count = entries.len();
        let batch = GossipBatchMessage {
            msg_id,
            origin: self.local_node_id.0,
            repo_type,
            serialized_size: 0, // placeholder, computed below
            entries,
            timestamp: now,
        };
        // 计算序列化大小（一次性，发送时直接读取避免重复计算）
        let serialized_size = bincode::serialized_size(&batch).unwrap_or(0);
        let mut batch = batch;
        batch.serialized_size = serialized_size;

        // 标记自己发出的消息为已处理（避免回环）
        self.seen_msgs.put((self.local_node_id, msg_id), ());
        self.push_outbox_unique(batch);
        debug!("[federation] Gossip 提交: msg_id={}, repo_type={}, entries={}", msg_id, repo_type, entry_count);
    }

    /// 批量提交同步数据到 outbox（将大量 entries 按 batch_size 拆分成多批提交）
    ///
    /// 适用于初始全量同步等场景，避免单条消息过大。每批生成独立的 msg_id，
    /// 分别加入 outbox 等待传播。
    pub fn submit_gossip_batch(&self, repo_type: u8, entries: Vec<SyncEntry>, batch_size: usize) {
        if entries.is_empty() || batch_size == 0 {
            return;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // 逐批标记 seen 并构建 batch，最后统一写入 outbox。
        // seen_msgs 已分片，此处不再持有全局写锁。
        let mut batches = Vec::new();
        for chunk in entries.chunks(batch_size) {
            let msg_id = self.next_msg_id.fetch_add(1, Ordering::Relaxed);
            self.seen_msgs.put((self.local_node_id, msg_id), ());
            let mut batch = GossipBatchMessage {
                msg_id,
                origin: self.local_node_id.0,
                repo_type,
                serialized_size: 0,
                entries: chunk.to_vec(),
                timestamp: now,
            };
            batch.serialized_size = bincode::serialized_size(&batch).unwrap_or(0);
            batches.push(batch);
            debug!("[federation] Gossip 批量提交: msg_id={}, repo_type={}, entries={}", msg_id, repo_type, chunk.len());
        }

        let accepted = self.extend_outbox_unique(batches);
        debug!("[federation] Gossip 批量提交完成: 接受 {} 条", accepted);
    }

    /// 启动 Gossip 传播后台任务
    pub fn spawn_gossip_propagation(self: Arc<Self>) {
        let interval = Duration::from_millis(self.config.gossip_interval_ms);
        let fanout = self.config.gossip_fanout;
        let mut shutdown_rx = self.shutdown.subscribe();

        // P2: 全量同步期间发送端零节流加速。
        // 全量同步进行中（outbox 洪峰）时使用 10ms 极短间隔连续发送；
        // 10ms 同时作为限流跳过（rate_skipped）时的最小退避，避免 rate_try_consume
        // 持续拒绝导致 CPU 忙等。非全量期保持原 gossip_interval_ms（默认 1000ms）。
        const FULL_SYNC_TICK_INTERVAL: Duration = Duration::from_millis(10);

        tokio::spawn(async move {
            loop {
                // 每轮循环开头根据发送端全量同步标记动态计算下一次 tick 间隔
                let tick_interval = if self.is_sending_full_sync() {
                    FULL_SYNC_TICK_INTERVAL
                } else {
                    interval
                };

                tokio::select! {
                    _ = tokio::time::sleep(tick_interval) => {
                        self.clone().propagation_tick(fanout).await;
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] Gossip 传播任务收到关闭信号");
                        break;
                    }
                }
            }
        });
        debug!("[federation] Gossip 传播任务已启动（常规间隔 {}ms, 全量期间隔 {}ms, fanout={}）", interval.as_millis(), FULL_SYNC_TICK_INTERVAL.as_millis(), fanout);
    }

    /// 发送端全局速率限制：尝试在当前秒级窗口内计入 `bytes` 字节 / `msgs` 条消息。
    ///
    /// 采用固定窗口计数器（AtomicU64，全局维度统计总出口带宽）：
    /// 窗口跨秒时自动滚动重置；若计入后会超过配置的字节数或消息数上限则返回 false。
    /// 对 2 节点测试场景，单连接流量即全局流量，天然受限。
    fn rate_try_consume(&self, bytes: u64, msgs: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let window = self.rate_window_secs.load(Ordering::Relaxed);
        if window != now {
            // 跨秒：滚动窗口（CAS 抢占复位，最坏情况多算一次，对限流可接受）
            let _ = self.rate_window_secs.compare_exchange(
                window,
                now,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            self.bytes_in_window.store(0, Ordering::Relaxed);
            self.msgs_in_window.store(0, Ordering::Relaxed);
        }
        // 根据发送端全量同步标记选择限流值：发送全量期间临时放开到更高上限
        let (max_bytes, max_msgs) = if self.is_sending_full_sync() {
            (
                self.config.full_sync_gossip_max_bytes_per_second,
                self.config.full_sync_gossip_max_messages_per_second as u64,
            )
        } else {
            (
                self.config.gossip_max_bytes_per_second,
                self.config.gossip_max_messages_per_second as u64,
            )
        };
        let cur_bytes = self.bytes_in_window.load(Ordering::Relaxed);
        let cur_msgs = self.msgs_in_window.load(Ordering::Relaxed);
        if cur_bytes + bytes > max_bytes || cur_msgs + msgs > max_msgs {
            return false;
        }
        self.bytes_in_window.fetch_add(bytes, Ordering::Relaxed);
        self.msgs_in_window.fetch_add(msgs, Ordering::Relaxed);
        true
    }

    /// 单次传播：从 outbox 取一批，随机选 fanout 个邻居发送
    async fn propagation_tick(self: Arc<Self>, fanout: usize) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // 取出待传播消息
        let batches: Vec<GossipBatchMessage> = {
            let mut outbox = self.outbox.write();
            if outbox.is_empty() {
                return;
            }
            // 全量同步期间跳过 outbox 长度截断和过期过滤：4 repo 并行洪峰（infohash/tracker
            // 百万级条目 → 数万 batch）会超过 5000 上限，drain 最老批次会导致先提交的 node
            // 数据丢失。全量同步由 sending_full_sync flag 控制，完成后恢复正常防护。
            if !self.is_sending_full_sync() {
                const MAX_OUTBOX_LEN: usize = 5000;
                if outbox.len() > MAX_OUTBOX_LEN {
                    let drop_count = outbox.len() - MAX_OUTBOX_LEN;
                    outbox.drain(..drop_count);
                    debug!("[federation] outbox 队列过长，丢弃 {} 条旧消息", drop_count);
                }
                // 过滤过期消息（超过 300 秒的消息不再传播，避免 2 节点场景下队列积压）
                outbox.retain(|b| now.saturating_sub(b.timestamp) < 300);
            }
            if outbox.is_empty() {
                return;
            }
            // 单连接场景：一次性取出所有消息，加快传播速度
            let conn_count = self.connection_manager.connection_count();
            let batch_size = if conn_count <= 1 { 500 } else { 100 };
            let count = outbox.len().min(batch_size);
            let drained: Vec<GossipBatchMessage> = outbox.drain(..count).collect();
            // 同步 outbox_msg_ids：截断/过期/drain 的消息已从 outbox 移除，
            // 将集合重建为当前 outbox 中剩余消息的 ID，保证一致性。
            let mut ids = self.outbox_msg_ids.write();
            ids.clear();
            for b in outbox.iter() {
                ids.insert((NodeId(b.origin), b.msg_id));
            }
            drop(ids);
            drained
        };

        if batches.is_empty() {
            return;
        }

        // 随机选择 fanout 个已连接邻居（先完成所有随机选择，避免 rng 跨 await）
        let conns = self.connection_manager.all_connections();
        if conns.is_empty() {
            // 无连接，把消息放回 outbox（下次重试），并尝试重连已知节点（带冷却）
            self.extend_outbox_unique(batches);
            self.connection_manager.clone().reconnect_discovered().await;
            return;
        }

        // 多个连接并行发送：把 batches 包进 Arc 供各 spawn task 共享只读
        let batches = Arc::new(batches);

        // 标记 in-flight：这些 batch 已从 outbox 取出但发送尚未完成（含阻塞中的 write_all）。
        // 此处之后无 early return，函数末尾统一 fetch_sub，保证配对。
        let in_flight_added = batches.len();
        self.in_flight_count.fetch_add(in_flight_added, Ordering::Relaxed);
        warn!(
            "[federation][DIAG] propagation_tick: took={} batches, outbox_remaining={}, in_flight_after_add={}",
            batches.len(),
            self.outbox_size(),
            self.in_flight_count.load(Ordering::Relaxed)
        );

        // 收集所有待发送的 (connection, batch_index) 对，按连接分组。
        // 使用索引而非引用，便于后续按连接分组和 bulk 合并。
        let mut by_conn: FxHashMap<NodeId, (Arc<Connection>, Vec<usize>)> = FxHashMap::default();
        {
            // 全量同步期间：接收端设置了 receiving_full_sync=true（不转发），
            // 流行病传播链断裂。此时必须广播到所有连接，否则未被随机 fanout 选中的
            // 节点永远收不到全量数据（实测 5 节点中仅 1 个成功同步）。
            let full_sync_broadcast = self.is_sending_full_sync();
            use rand::seq::SliceRandom;
            let mut rng = rand::rngs::StdRng::from_entropy();
            for (batch_idx, batch) in batches.iter().enumerate() {
                let filtered: Vec<_> = conns
                    .iter()
                    .filter(|c| NodeId(batch.origin) != c.node_id)
                    .cloned()
                    .collect();
                let selected: Vec<_> = if full_sync_broadcast {
                    // 全量同步：广播到所有连接，确保每个请求节点都收到完整数据
                    filtered
                } else {
                    // 常规 Gossip：随机 fanout
                    filtered
                        .choose_multiple(&mut rng, fanout.min(filtered.len()))
                        .cloned()
                        .collect()
                };
                for conn in selected {
                    by_conn.entry(conn.node_id).or_insert_with(|| (conn, Vec::new())).1.push(batch_idx);
                }
            }
        }

        // 主任务汇总用的结果集合（各连接 task 并行发送后合并）
        let mut failed_msg_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        // 记录因发送端限流跳过的 batch msg_id。
        // 修复：限流跳过的 batch 不再回退 outbox（gossip 只需至少一个邻居收到即可传播，
        // 限流是主动带宽控制，回退会导致队列永久积压）。
        let mut rate_skipped_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        // 至少在一个连接上发送成功的 batch msg_id（合并自所有连接结果）。
        // 有成功发送的 batch 视为已传播到网络中，不再回退 outbox。
        let mut successful_msg_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        // 按节点跟踪本次 tick 的发送结果
        let mut failed_nodes: FxHashSet<NodeId> = FxHashSet::default();
        let mut successful_nodes: FxHashSet<NodeId> = FxHashSet::default();

        // 记录所有被分配到连接发送的 batch msg_id（用于识别未分配到任何连接的孤儿 batch）
        let mut assigned_msg_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (_node_id, (_, batch_indices)) in &by_conn {
            for &idx in batch_indices {
                assigned_msg_ids.insert(batches[idx].msg_id);
            }
        }

        let bulk_max_batches = self.config.gossip_bulk_max_batches;
        let bulk_max_bytes = self.config.gossip_bulk_max_bytes;

        // 发送到各连接：多节点场景按 config.parallel_propagation 并行发送，
        // 避免串行 for 循环把超级节点带宽分摊到单个普通节点（测试 node2 仅 8,450 条/s）。
        // 限流计数器为 AtomicU64，多 task 并发安全；单连接或关闭并行时走串行 fallback。
        let parallel_enabled = self.config.parallel_propagation && by_conn.len() > 1;
        if parallel_enabled {
            // 并行：先全部 spawn（保证并发调度），再逐个 await 收集结果
            let mut handles = Vec::with_capacity(by_conn.len());
            for (_node_id, (conn, batch_indices)) in by_conn {
                let this = self.clone();
                let batches_arc = batches.clone();
                handles.push(tokio::spawn(async move {
                    this.send_to_conn(
                        conn,
                        batch_indices,
                        batches_arc,
                        bulk_max_batches,
                        bulk_max_bytes,
                    )
                    .await
                }));
            }
            for h in handles {
                match h.await {
                    Ok(res) => {
                        failed_msg_ids.extend(res.failed_msg_ids);
                        rate_skipped_ids.extend(res.rate_skipped_ids);
                        successful_msg_ids.extend(res.successful_msg_ids);
                        failed_nodes.extend(res.failed_nodes);
                        successful_nodes.extend(res.successful_nodes);
                    }
                    Err(e) => {
                        warn!("[federation] 并行传播 task 异常: {}", e);
                    }
                }
            }
        } else {
            // 串行发送（单连接 localhost 场景并行无收益，并行反而放大 write timeout）。
            for (_node_id, (conn, batch_indices)) in by_conn {
                let res = self
                    .send_to_conn(conn, batch_indices, batches.clone(), bulk_max_batches, bulk_max_bytes)
                    .await;
                failed_msg_ids.extend(res.failed_msg_ids);
                rate_skipped_ids.extend(res.rate_skipped_ids);
                successful_msg_ids.extend(res.successful_msg_ids);
                failed_nodes.extend(res.failed_nodes);
                successful_nodes.extend(res.successful_nodes);
            }
        }

        // 更新节点连续失败计数：成功清零，失败递增，达到阈值则断开连接
        let max_failures = self.config.gossip_max_consecutive_failures;
        let mut to_disconnect: Vec<NodeId> = Vec::new();
        {
            let mut failures = self.consecutive_failures.write();
            // 发送成功的节点清零计数
            for node_id in &successful_nodes {
                failures.remove(node_id);
            }
            // 发送失败的节点递增计数
            for node_id in &failed_nodes {
                let count = failures.entry(*node_id).or_insert(0);
                *count += 1;
                if *count >= max_failures {
                    to_disconnect.push(*node_id);
                    failures.remove(node_id);
                }
            }
        }
        // 在锁外执行断开连接
        for node_id in to_disconnect {
            warn!(
                "[federation] 节点 {} Gossip 连续失败 {} 次，主动断开连接",
                node_id, max_failures
            );
            self.connection_manager.remove_connection(&node_id);
        }

        // === batch 回退决策 ===
        // 修复前：任意一个 fanout 发送被限流跳过或失败，整个 batch 就回退 outbox。
        //   在 ~50% 单连接限流跳过率下，P(3个fanout中至少1个被跳过)≈87%，导致每 tick
        //   仅 ~13% 的 batch 真正出队，队列永久积压在 4900 上限。
        // 修复后：gossip 协议只需至少一个邻居收到即可继续传播。
        //   - 至少一个连接发送成功 → 已传播，DROP（不回退）
        //   - 零成功 + 至少一个连接实际失败（连接错误）→ 回退重试（带 MAX_RETRIES）
        //   - 零成功 + 全部因限流跳过（无连接错误）→ 系统过载，DROP（不回退，避免死循环）
        //   - 未分配到任何连接的孤儿 batch → 回退（从未尝试发送）

        let all_batch_ids: std::collections::HashSet<u64> = batches.iter().map(|b| b.msg_id).collect();

        // 零成功发送的 batch（所有连接都没成功）
        let no_success_ids: Vec<u64> = all_batch_ids.difference(&successful_msg_ids).copied().collect();

        // 需要重试的：零成功 + 至少一个连接实际失败（非限流跳过）
        let retry_ids: std::collections::HashSet<u64> = no_success_ids
            .iter()
            .filter(|id| failed_msg_ids.contains(id))
            .copied()
            .collect();

        // 因限流全部跳过且零成功的 batch（DROP，不回退）
        let rate_drop_count = no_success_ids.iter().filter(|id| !failed_msg_ids.contains(id)).count();

        // 将需要重试的 batch 放回 outbox，带重试次数限制
        if !retry_ids.is_empty() {
            let mut retry_guard = self.retry_counts.write();
            let mut returned: Vec<GossipBatchMessage> = Vec::new();

            for batch in batches.iter() {
                if !retry_ids.contains(&batch.msg_id) {
                    // 不需要重试（已成功传播或因限流丢弃），清理重试计数
                    retry_guard.remove(&batch.msg_id);
                    continue;
                }
                let count = retry_guard.entry(batch.msg_id).or_insert(0);
                *count += 1;
                if *count >= MAX_RETRIES {
                    error!(
                        "[federation] Gossip 批次 msg_id={} 重试 {} 次仍失败，丢弃",
                        batch.msg_id, count
                    );
                    retry_guard.remove(&batch.msg_id);
                } else {
                    warn!(
                        "[federation] Gossip 批次 msg_id={} 发送失败（第 {} 次），放回 outbox 重试",
                        batch.msg_id, count
                    );
                    returned.push(batch.clone());
                }
            }
            drop(retry_guard);

            if !returned.is_empty() {
                self.extend_outbox_unique(returned);
            }
        } else {
            // 没有需要重试的，清理所有批次的重试计数
            let mut retry_guard = self.retry_counts.write();
            for batch in batches.iter() {
                retry_guard.remove(&batch.msg_id);
            }
        }

        if rate_drop_count > 0 {
            debug!(
                "[federation] 限流全跳过且零成功的 {} 条 batch 直接丢弃（不回退 outbox）",
                rate_drop_count
            );
        }

        // 未被分配到任何连接的孤儿 batch（fanout 选不到邻居时产生），回退 outbox 避免丢失
        let orphan_count = batches.iter().filter(|b| !assigned_msg_ids.contains(&b.msg_id)).count();
        if orphan_count > 0 {
            warn!("[federation][DIAG] propagation_tick: {} 条 batch 未分配到连接，回退 outbox", orphan_count);
            let orphans: Vec<GossipBatchMessage> = batches.iter()
                .filter(|b| !assigned_msg_ids.contains(&b.msg_id))
                .cloned()
                .collect();
            self.extend_outbox_unique(orphans);
        }

        debug!("[federation] Gossip 传播: {} 条消息发送到 {} 个邻居", batches.len(), fanout);
        warn!(
            "[federation][DIAG] propagation_tick: sent={} batches, success={}, failed={}, rate_skipped={}, rate_dropped={}, retry={}, in_flight_before_sub={}",
            batches.len(),
            successful_msg_ids.len(),
            failed_msg_ids.len(),
            rate_skipped_ids.len(),
            rate_drop_count,
            retry_ids.len(),
            self.in_flight_count.load(Ordering::Relaxed)
        );
        self.in_flight_count.fetch_sub(in_flight_added, Ordering::Relaxed);
    }

    /// 向单个连接串行发送该连接的所有 batch（内部按 bulk 分组，保证单连接顺序）。
    /// 并行化后每个连接一个 task；限流计数器为原子，多 task 并发安全。
    async fn send_to_conn(
        self: &Arc<Self>,
        conn: Arc<Connection>,
        batch_indices: Vec<usize>,
        batches: Arc<Vec<GossipBatchMessage>>,
        bulk_max_batches: usize,
        bulk_max_bytes: usize,
    ) -> ConnSendResult {
        let mut result = ConnSendResult::default();
        let mut pending: Vec<usize> = Vec::new();
        let mut pending_bytes: u64 = 0;

        for &batch_idx in &batch_indices {
            let batch = &batches[batch_idx];
            // 若 serialized_size 未设置（如从对端接收后转发的 batch），现场计算
            let batch_size = if batch.serialized_size > 0 {
                batch.serialized_size
            } else {
                bincode::serialized_size(batch).unwrap_or(0)
            };

            let would_exceed_batches = pending.len() >= bulk_max_batches;
            let would_exceed_bytes = !pending.is_empty()
                && pending_bytes + batch_size > bulk_max_bytes as u64;

            if would_exceed_batches || would_exceed_bytes {
                Self::send_bulk_or_single(
                    self, &conn, batches.as_slice(), &pending, pending_bytes,
                    &mut result.failed_msg_ids, &mut result.rate_skipped_ids,
                    &mut result.successful_msg_ids,
                    &mut result.failed_nodes, &mut result.successful_nodes,
                )
                .await;
                pending.clear();
                pending_bytes = 0;
            }
            pending.push(batch_idx);
            pending_bytes += batch_size;
        }

        if !pending.is_empty() {
            Self::send_bulk_or_single(
                self, &conn, batches.as_slice(), &pending, pending_bytes,
                &mut result.failed_msg_ids, &mut result.rate_skipped_ids,
                &mut result.successful_msg_ids,
                &mut result.failed_nodes, &mut result.successful_nodes,
            )
            .await;
        }
        result
    }

    /// 发送一组（可能仅1个）batch 到指定连接。
    /// 单个 batch 直接发 GossipBatch；多个 batch 合并为 GossipBatchBulk 发送。
    /// 限流按 batch 数量扣减 msgs，按总字节数扣减 bytes。
    async fn send_bulk_or_single(
        self: &Arc<Self>,
        conn: &Connection,
        batches: &[GossipBatchMessage],
        indices: &[usize],
        total_batch_bytes: u64,
        failed_msg_ids: &mut std::collections::HashSet<u64>,
        rate_skipped_ids: &mut std::collections::HashSet<u64>,
        successful_msg_ids: &mut std::collections::HashSet<u64>,
        failed_nodes: &mut FxHashSet<NodeId>,
        successful_nodes: &mut FxHashSet<NodeId>,
    ) {
        let node_id = conn.node_id;
        let batch_count = indices.len();

        // 限流：按总字节数（含1个帧头）和 batch 条数扣减
        let frame_overhead = FRAME_HEADER_SIZE as u64;
        let total_bytes = total_batch_bytes + frame_overhead;
        if !self.rate_try_consume(total_bytes, batch_count as u64) {
            debug!(
                "[federation] Gossip 速率超限，跳过 {} 条 batch 到 {}（{} 字节）",
                batch_count, node_id, total_bytes
            );
            for &idx in indices {
                rate_skipped_ids.insert(batches[idx].msg_id);
            }
            return;
        }

        if batch_count == 1 {
            // 单个 batch：直接发 GossipBatch
            let batch = &batches[indices[0]];
            if let Err(e) = conn.send_message(MessageType::GossipBatch, batch).await {
                warn!("[federation] Gossip 发送到 {} 失败: {}", node_id, e);
                failed_msg_ids.insert(batch.msg_id);
                failed_nodes.insert(node_id);
                let err_str = e.to_string();
                if err_str.contains("10058") || err_str.contains("Connection reset") || err_str.contains("Broken pipe") || err_str.contains("closed") {
                    warn!("[federation] 检测到连接 {} 已关闭，立即移除", node_id);
                    self.connection_manager.remove_connection(&node_id);
                }
            } else {
                self.metrics.record_gossip_propagation();
                self.metrics.record_message_sent();
                successful_nodes.insert(node_id);
                successful_msg_ids.insert(batch.msg_id);
            }
        } else {
            // 多个 batch：合并为 GossipBatchBulk 发送
            let bulk_refs: Vec<&GossipBatchMessage> = indices.iter().map(|&i| &batches[i]).collect();
            let bulk = BulkRef { batches: bulk_refs };
            if let Err(e) = conn.send_message(MessageType::GossipBatchBulk, &bulk).await {
                warn!("[federation] GossipBulk 发送 {} 条到 {} 失败: {}", batch_count, node_id, e);
                for &idx in indices {
                    failed_msg_ids.insert(batches[idx].msg_id);
                }
                failed_nodes.insert(node_id);
                let err_str = e.to_string();
                if err_str.contains("10058") || err_str.contains("Connection reset") || err_str.contains("Broken pipe") || err_str.contains("closed") {
                    warn!("[federation] 检测到连接 {} 已关闭，立即移除", node_id);
                    self.connection_manager.remove_connection(&node_id);
                }
            } else {
                // 每个 batch 计一次传播指标
                for &idx in indices {
                    self.metrics.record_gossip_propagation();
                    successful_msg_ids.insert(batches[idx].msg_id);
                }
                self.metrics.record_message_sent();
                successful_nodes.insert(node_id);
            }
        }
    }

    /// 处理收到的 Gossip 消息
    ///
    /// 检查 msg_id 去重，未处理则加入 seen_msgs，返回 entries 供上层写入本地，
    /// 并将 batch 加入 outbox 继续转发。
    pub fn handle_gossip_batch(&self, batch: GossipBatchMessage) -> Vec<SyncEntry> {
        let total_start = Instant::now();
        self.metrics.record_gossip_received();
        self.metrics.record_message_recv();

        let entry_count = batch.entries.len();

        // 分片原子"检查并插入"：已处理过的消息直接丢弃，未处理则在同一分片锁内标记。
        let seen_start = Instant::now();
        let dedup_key = (NodeId(batch.origin), batch.msg_id);
        if !self.seen_msgs.check_and_put(dedup_key, ()) {
            debug!("[federation] Gossip 消息已处理，跳过: msg_id={}", batch.msg_id);
            return Vec::new();
        }
        let seen_elapsed = seen_start.elapsed();

        // 单连接场景优化：如果只有1个连接，且消息来自该连接，则不需要加入outbox
        // 因为传播不出去（只会发回给发送方，而发送方已经处理过这条消息了）
        // 优化1：接收全量期间禁用转发 —— 纯对等单源拉取后，本节点只从选中的数据源
        // 接收全量数据，只 apply 本地写入（走零 clone 路径），不 push outbox 继续转发，
        // 避免重复洪峰（测试中 node1 收到 408 batch 是实际数据的 2 倍）。
        // 关键：只看 receiving_full_sync；sending_full_sync（本节点在向外推）不影响接收端转发，
        // 否则发送方会被误伤、暂停 Gossip 转发。接收结束后自动恢复正常转发。
        // 超级节点不受影响：它通过 submit_gossip/submit_gossip_batch 直接写 outbox，不走本路径。
        let conn_count = self.connection_manager.connection_count();
        let should_propagate = if self.is_receiving_full_sync() {
            // 接收全量期间：只本地写入，不转发
            false
        } else if conn_count <= 1 {
            let conns = self.connection_manager.all_connections();
            if conns.len() == 1 {
                // 消息origin不是唯一连接的node_id时才需要传播（本地产生的消息）
                NodeId(batch.origin) != conns[0].node_id
            } else {
                true
            }
        } else {
            true
        };

        let clone_start = Instant::now();
        let result = if should_propagate {
            // 多连接场景：需要同时返回 entries 给本地写入 + 保留 batch 转发，
            // 必须 clone 一次（下游 DB 写入和网络传播各自需要所有权）。
            let entries = batch.entries.clone();
            debug!(
                "[federation] Gossip 收到新消息: msg_id={}, origin={}, repo_type={}, entries={}",
                batch.msg_id,
                NodeId(batch.origin),
                batch.repo_type,
                entries.len()
            );
            self.push_outbox_unique(batch);
            entries
        } else {
            // 单连接/无传播场景：直接 move entries，零 clone。
            // 2 节点同步场景下这是常见路径，节省 5000 条 entries 的深拷贝。
            let GossipBatchMessage { entries, msg_id, origin, repo_type, .. } = batch;
            debug!(
                "[federation] Gossip 收到新消息(不传播): msg_id={}, origin={}, repo_type={}, entries={}",
                msg_id, NodeId(origin), repo_type, entries.len()
            );
            entries
        };
        let clone_elapsed = clone_start.elapsed();
        let total_elapsed = total_start.elapsed();

        if entry_count > 100 {
            warn!(
                "[federation][perf] handle_gossip_batch: entries={} seen={}ms clone={}ms total={}ms",
                entry_count,
                seen_elapsed.as_millis(),
                clone_elapsed.as_millis(),
                total_elapsed.as_millis()
            );
        }

        result
    }

    /// 启动反熵（anti-entropy）后台任务
    ///
    /// 每60秒随机选1个邻居，发送 MerkleDigest 进行对账。
    pub fn spawn_anti_entropy<M: MerkleProvider + 'static>(self: Arc<Self>, merkle_provider: Arc<M>) {
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(60));
            ticker.tick().await; // 跳过第一次

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self.clone().anti_entropy_tick(merkle_provider.clone()).await;
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] 反熵任务收到关闭信号");
                        break;
                    }
                }
            }
        });
        debug!("[federation] 反熵任务已启动（间隔 60s）");
    }

    /// 单次反熵：随机选1个邻居，发送所有 repo_type 的 MerkleDigest
    async fn anti_entropy_tick<M: MerkleProvider>(self: Arc<Self>, merkle_provider: Arc<M>) {
        let conns = self.connection_manager.all_connections();
        if conns.is_empty() {
            return;
        }

        use rand::seq::SliceRandom;
        let mut rng = rand::rngs::StdRng::from_entropy();
        let conn = match conns.choose(&mut rng) {
            Some(c) => c.clone(),
            None => return,
        };

        // 发送所有 repo 的 MerkleDigest（Node/Peer/Infohash/Tracker）
        for repo_type in &[repo_type::NODE, repo_type::PEER, repo_type::INFOHASH, repo_type::TRACKER] {
            let digest = merkle_provider.get_digest(*repo_type);
            if let Err(e) = conn.send_message(MessageType::MerkleDigest, &digest).await {
                debug!("[federation] 反熵 MerkleDigest 发送失败: {}", e);
            } else {
                self.metrics.record_message_sent();
            }
        }

        debug!("[federation] 反熵对账发送到 {}", conn.node_id);
    }

    /// outbox 大小
    pub fn outbox_size(&self) -> usize {
        self.outbox.read().len()
    }

    /// 将 batch 加入 outbox（带去重）。
    /// 如果 (origin, msg_id) 已在 outbox_msg_ids 中，跳过不加入。
    /// 防止 seen_msgs LRU 驱逐后同一消息被重复加入 outbox 导致队列膨胀。
    fn push_outbox_unique(&self, batch: GossipBatchMessage) {
        let key = (NodeId(batch.origin), batch.msg_id);
        let mut ids = self.outbox_msg_ids.write();
        if ids.contains(&key) {
            debug!("[federation] outbox 去重跳过: origin={}, msg_id={}", NodeId(batch.origin), batch.msg_id);
            return;
        }
        ids.insert(key);
        drop(ids);
        self.outbox.write().push(batch);
    }

    /// 批量加入 outbox（带去重），返回实际加入的数量。
    fn extend_outbox_unique(&self, batches: Vec<GossipBatchMessage>) -> usize {
        if batches.is_empty() {
            return 0;
        }
        let mut ids = self.outbox_msg_ids.write();
        let mut accepted: Vec<GossipBatchMessage> = Vec::with_capacity(batches.len());
        for batch in batches {
            let key = (NodeId(batch.origin), batch.msg_id);
            if ids.contains(&key) {
                continue;
            }
            ids.insert(key);
            accepted.push(batch);
        }
        let count = accepted.len();
        drop(ids);
        if !accepted.is_empty() {
            self.outbox.write().extend(accepted);
        }
        count
    }

    /// 轮询等待 outbox 排空（剩余 <= 1 视为排空），最长等待 `timeout`。
    ///
    /// 在 async 上下文（SyncManager::trigger_initial_sync 的收尾任务）中调用：
    /// 数据收集完成后，outbox 中仍可能积压大量待传播 batch，若立即关闭
    /// `full_sync_in_progress` 会回退到常规限流（100 msg/s）拖慢收尾。
    /// 此处轮询等待洪峰发完，再由调用方关闭标记。
    /// 返回 true 表示已排空，false 表示超时。
    pub async fn wait_outbox_empty(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        // 连续检测到完全为空的次数（防抖），避免 propagation tick 间隙的瞬时空窗误判
        let mut empty_streak: u32 = 0;
        while start.elapsed() < timeout {
            let outbox = self.outbox_size();
            let in_flight = self.in_flight_count.load(Ordering::Relaxed);
            warn!(
                "[federation][DIAG] wait_outbox_empty: outbox_size={}, in_flight={}, elapsed={}ms",
                outbox,
                in_flight,
                start.elapsed().as_millis()
            );
            // 把已取出但仍在发送中的 batch 计入；必须完全为 0（不再留 1 条），
            // 且连续 3 次（~300ms）都为空才返回，避免 tick 间隙误判。
            if outbox + in_flight == 0 {
                empty_streak += 1;
                if empty_streak >= 3 {
                    return true;
                }
            } else {
                empty_streak = 0;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::node_id::NodeIdentity;
    use crate::federation::node_table::NodeTable;

    fn make_config() -> FederationConfig {
        FederationConfig {
            enabled: true,
            listen_port: 0,
            max_connections: 10,
            gossip_interval_ms: 100,
            gossip_fanout: 3,
            ..Default::default()
        }
    }

    #[test]
    fn test_submit_and_handle_gossip() {
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
        let engine = GossipEngine::new(
            cm,
            make_config(),
            NodeId([1; 20]),
            metrics,
            shutdown_tx,
        );

        let entries = vec![
            SyncEntry { key: b"k1".to_vec(), operation: 0, version: 1, payload: vec![1] },
        ];
        engine.submit_gossip(repo_type::NODE, entries.clone());
        assert_eq!(engine.outbox_size(), 1);

        // 构造一个收到的 batch（不同 msg_id）
        let batch = GossipBatchMessage {
            msg_id: 999,
            origin: [2; 20],
            repo_type: repo_type::NODE,
            entries: entries.clone(),
            timestamp: 100,
            serialized_size: 0,
        };
        let result = engine.handle_gossip_batch(batch);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, b"k1");

        // 重复处理应返回空
        let batch2 = GossipBatchMessage {
            msg_id: 999,
            origin: [2; 20],
            repo_type: repo_type::NODE,
            entries,
            timestamp: 100,
            serialized_size: 0,
        };
        let result2 = engine.handle_gossip_batch(batch2);
        assert!(result2.is_empty());
    }

    #[test]
    fn test_submit_empty_entries() {
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
        let engine = GossipEngine::new(
            cm,
            make_config(),
            NodeId([1; 20]),
            metrics,
            shutdown_tx,
        );

        engine.submit_gossip(repo_type::NODE, vec![]);
        assert_eq!(engine.outbox_size(), 0);
    }

    #[test]
    fn test_msg_id_increment() {
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
        let engine = GossipEngine::new(
            cm,
            make_config(),
            NodeId([1; 20]),
            metrics,
            shutdown_tx,
        );

        let entries = vec![SyncEntry { key: b"k".to_vec(), operation: 0, version: 1, payload: vec![] }];
        engine.submit_gossip(1, entries.clone());
        engine.submit_gossip(1, entries.clone());
        engine.submit_gossip(1, entries);
        assert_eq!(engine.outbox_size(), 3);
    }
}
