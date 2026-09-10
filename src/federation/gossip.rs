//! Gossip 引擎
//!
//! 基于流行病协议的消息传播。节点将同步数据提交到 outbox，
//! 后台任务定期随机选择 fanout 个邻居传播。已处理消息通过 LRU 去重。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lru::LruCache;
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

/// Gossip 引擎
pub struct GossipEngine {
    /// 待传播队列
    outbox: RwLock<Vec<GossipBatchMessage>>,
    /// 已处理消息 ID 去重（按 (origin_node, msg_id) 全局唯一去重）
    seen_msgs: RwLock<LruCache<(NodeId, u64), ()>>,
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
    /// 全量同步进行中标记，为 true 时临时放开 Gossip 发送限流
    full_sync_in_progress: Arc<AtomicBool>,
}

/// 单个 batch 最大重试次数，超过则丢弃
const MAX_RETRIES: u32 = 3;

impl GossipEngine {
    /// 创建 Gossip 引擎
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        config: FederationConfig,
        local_node_id: NodeId,
        metrics: Arc<FederationMetrics>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            outbox: RwLock::new(Vec::new()),
            seen_msgs: RwLock::new(LruCache::new(
                std::num::NonZeroUsize::new(10000).unwrap(),
            )),
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
            full_sync_in_progress: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 设置全量同步进行中标记（由 SyncManager 在 trigger_initial_sync 期间调用）
    pub fn set_full_sync_in_progress(&self, v: bool) {
        self.full_sync_in_progress.store(v, Ordering::Relaxed);
    }

    /// 查询全量同步是否进行中
    pub fn is_full_sync_in_progress(&self) -> bool {
        self.full_sync_in_progress.load(Ordering::Relaxed)
    }

    /// 获取全量同步标记的共享句柄（供非核心模块在全量同步期间暂停检查）
    pub fn pause_gate(&self) -> Arc<AtomicBool> {
        self.full_sync_in_progress.clone()
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
            entries,
            timestamp: now,
        };

        // 标记自己发出的消息为已处理（避免回环）
        self.seen_msgs.write().put((self.local_node_id, msg_id), ());
        self.outbox.write().push(batch);
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

        // 先构建所有 batch（获取 seen_msgs 锁），再统一写入 outbox，避免锁顺序反转
        let mut batches = Vec::new();
        for chunk in entries.chunks(batch_size) {
            let msg_id = self.next_msg_id.fetch_add(1, Ordering::Relaxed);
            self.seen_msgs.write().put((self.local_node_id, msg_id), ());
            batches.push(GossipBatchMessage {
                msg_id,
                origin: self.local_node_id.0,
                repo_type,
                entries: chunk.to_vec(),
                timestamp: now,
            });
            debug!("[federation] Gossip 批量提交: msg_id={}, repo_type={}, entries={}", msg_id, repo_type, chunk.len());
        }

        self.outbox.write().extend(batches);
    }

    /// 启动 Gossip 传播后台任务
    pub fn spawn_gossip_propagation(self: Arc<Self>) {
        let interval = Duration::from_millis(self.config.gossip_interval_ms);
        let fanout = self.config.gossip_fanout;
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // 跳过第一次

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self.clone().propagation_tick(fanout).await;
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] Gossip 传播任务收到关闭信号");
                        break;
                    }
                }
            }
        });
        debug!("[federation] Gossip 传播任务已启动（间隔 {}ms, fanout={}）", interval.as_millis(), fanout);
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
        // 根据全量同步标记选择限流值：全量同步期间临时放开到更高上限
        let (max_bytes, max_msgs) = if self.is_full_sync_in_progress() {
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
            // 数据丢失。全量同步由 full_sync_in_progress flag 控制，完成后恢复正常防护。
            if !self.is_full_sync_in_progress() {
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
            outbox.drain(..count).collect()
        };

        if batches.is_empty() {
            return;
        }

        // 随机选择 fanout 个已连接邻居（先完成所有随机选择，避免 rng 跨 await）
        let conns = self.connection_manager.all_connections();
        if conns.is_empty() {
            // 无连接，把消息放回 outbox（下次重试）
            self.outbox.write().extend(batches);
            return;
        }

        // 收集所有待发送的 (connection, batch) 对（rng 在此作用域内使用完毕）
        let send_targets: Vec<(Arc<Connection>, &GossipBatchMessage)> = {
            use rand::seq::SliceRandom;
            let mut rng = rand::rngs::StdRng::from_entropy();
            let mut targets = Vec::new();
            for batch in &batches {
                let filtered: Vec<_> = conns
                    .iter()
                    .filter(|c| NodeId(batch.origin) != c.node_id)
                    .cloned()
                    .collect();
                let selected: Vec<_> = filtered
                    .choose_multiple(&mut rng, fanout.min(filtered.len()))
                    .cloned()
                    .collect();
                for conn in selected {
                    targets.push((conn, batch));
                }
            }
            targets
        };

        // 记录发送失败的 batch msg_id，用于回退 outbox
        let mut failed_msg_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        // 记录因发送端限流跳过的 batch msg_id，回退 outbox 待下 tick（不计入失败/重试）
        let mut rate_skipped_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        // 按节点跟踪本次 tick 的发送结果
        let mut failed_nodes: FxHashSet<NodeId> = FxHashSet::default();
        let mut successful_nodes: FxHashSet<NodeId> = FxHashSet::default();

        for (conn, batch) in send_targets {
            let node_id = conn.node_id;
            // 发送端速率限制：按全局出口字节数/消息数限流，超限则跳过（不视为失败）
            let batch_bytes = bincode::serialized_size(batch).unwrap_or(0) + FRAME_HEADER_SIZE as u64;
            if !self.rate_try_consume(batch_bytes, 1) {
                debug!(
                    "[federation] Gossip 速率超限，跳过发送 msg_id={} 到 {}（{} 字节）",
                    batch.msg_id, node_id, batch_bytes
                );
                rate_skipped_ids.insert(batch.msg_id);
                continue;
            }
            if let Err(e) = conn.send_message(MessageType::GossipBatch, batch).await {
                warn!("[federation] Gossip 发送到 {} 失败: {}", node_id, e);
                failed_msg_ids.insert(batch.msg_id);
                failed_nodes.insert(node_id);
                // 连接已关闭类错误（os 10058/ECONNRESET/BrokenPipe）：立即移除，不等连续失败计数
                let err_str = e.to_string();
                if err_str.contains("10058") || err_str.contains("Connection reset") || err_str.contains("Broken pipe") || err_str.contains("closed") {
                    warn!("[federation] 检测到连接 {} 已关闭，立即移除", node_id);
                    self.connection_manager.remove_connection(&node_id);
                }
            } else {
                self.metrics.record_gossip_propagation();
                self.metrics.record_message_sent();
                successful_nodes.insert(node_id);
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

        // 将发送失败的 batch 放回 outbox，带重试次数限制
        if !failed_msg_ids.is_empty() {
            let mut retry_guard = self.retry_counts.write();
            let mut returned: Vec<GossipBatchMessage> = Vec::new();

            for batch in &batches {
                if !failed_msg_ids.contains(&batch.msg_id) {
                    // 发送成功，清理重试计数
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
                self.outbox.write().extend(returned);
            }
        }

        // 限流跳过的 batch 回退 outbox（不计重试、不计连续失败），下 tick 再发
        if !rate_skipped_ids.is_empty() {
            let mut outbox = self.outbox.write();
            for batch in &batches {
                if rate_skipped_ids.contains(&batch.msg_id) {
                    outbox.push(batch.clone());
                }
            }
            debug!("[federation] 限流跳过 {} 条 batch 回退 outbox", rate_skipped_ids.len());
        }

        debug!("[federation] Gossip 传播: {} 条消息发送到 {} 个邻居", batches.len(), fanout);
    }

    /// 处理收到的 Gossip 消息
    ///
    /// 检查 msg_id 去重，未处理则加入 seen_msgs，返回 entries 供上层写入本地，
    /// 并将 batch 加入 outbox 继续转发。
    pub fn handle_gossip_batch(&self, batch: GossipBatchMessage) -> Vec<SyncEntry> {
        self.metrics.record_gossip_received();
        self.metrics.record_message_recv();

        let mut seen = self.seen_msgs.write();
        let dedup_key = (NodeId(batch.origin), batch.msg_id);
        if seen.contains(&dedup_key) {
            debug!("[federation] Gossip 消息已处理，跳过: msg_id={}", batch.msg_id);
            return Vec::new();
        }
        seen.put(dedup_key, ());
        drop(seen);

        let entries = batch.entries.clone();
        debug!(
            "[federation] Gossip 收到新消息: msg_id={}, origin={}, repo_type={}, entries={}",
            batch.msg_id,
            NodeId(batch.origin),
            batch.repo_type,
            entries.len()
        );

        // 单连接场景优化：如果只有1个连接，且消息来自该连接，则不需要加入outbox
        // 因为传播不出去（只会发回给发送方，而发送方已经处理过这条消息了）
        let conn_count = self.connection_manager.connection_count();
        let should_propagate = if conn_count <= 1 {
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

        if should_propagate {
            // 加入 outbox 继续传播（流行病协议）
            self.outbox.write().push(batch);
        }

        entries
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

        // 发送 Node repo 的 MerkleDigest（阶段2主要同步 Node/Peer/Infohash）
        for repo_type in &[repo_type::NODE, repo_type::PEER, repo_type::INFOHASH] {
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
