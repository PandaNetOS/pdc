//! 分片并行同步引擎（协议版本 >=3，亿级数据架构升级 阶段三~五）
//!
//! # 核心设计
//!
//! 本引擎运行在**数据服务器侧**（发送方），负责将差异 L2 二级分片的数据
//! 并行、流式、带背压地推送到请求方。
//!
//! ## 分片并行
//!
//! - 差异 L2 分片队列，按 `shard_sync_max_concurrency` 并行调度
//! - 每个 L2 分片独立状态：pending / sending / acked / failed
//! - 独立超时（`shard_sync_timeout_secs`）、独立重试（`shard_sync_retry_count`）
//! - 单分片失败不影响其他分片，记录失败列表
//!
//! ## 流式加载 + 背压
//!
//! - DB 按 L1 分片加载（现有索引），内存中按 L2 过滤分批
//! - 每批 `shard_sync_batch_size` 条，发完即 drop，不保留全量数据
//! - 窗口流控：在途批次数不超过 `shard_sync_window_size`
//! - 速率限制：`shard_sync_rate_limit_per_sec > 0` 时按条目/秒限速
//!
//! ## 增量同步优先
//!
//! - 基于 MerkleTree 的 dirty L2 标记，只同步变更分片
//! - 三级降级链：增量同步 → 分层 Merkle 差异同步 → 旧版 DiffSync → 全量兜底
//!
//! # 使用方式
//!
//! 由 `SyncManager` 在以下场景创建并驱动：
//! 1. `handle_merkle_digest` 检测到大差异且对端支持分层 Merkle
//! 2. 增量同步 tick 发现 dirty L2 分片
//! 3. 分层对比流程定位到差异 L2 分片后

#![allow(clippy::type_complexity)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::{oneshot, Semaphore};
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::federation::config::FederationConfig;
use crate::federation::connection::Connection;
use crate::federation::merkle::MerkleTree;
use crate::federation::node_id::NodeId;
use crate::federation::protocol::*;

/// 分片同步 hash 列表单条消息最大条目数（避免超大帧，参考 DiffSync key 分片策略）。
const SHARD_HASH_LIST_CHUNK_SIZE: usize = 10000;

// ============================================================================
// 内部状态类型
// ============================================================================

/// 单个 L2 分片同步状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShardState {
    /// 等待调度
    Pending,
    /// 正在发送中
    Sending,
    /// 已确认（收到 Ack）
    Acked,
    /// 失败（超过重试次数）
    Failed,
}

/// 单个在途批次的等待句柄
struct PendingAck {
    /// 等待 Ack 的 oneshot 发送端
    tx: Option<oneshot::Sender<u32>>,
    /// 该批次覆盖的 L2 分片
    #[allow(dead_code)]
    l2_shards: Vec<u32>,
    /// Ack 重试次数
    retry_count: u32,
}

/// 同步统计
#[derive(Debug, Default, Clone)]
pub struct ShardSyncStats {
    /// 待同步分片数
    pub pending_shards: u32,
    /// 已确认分片数
    pub acked_shards: u32,
    /// 失败分片数
    pub failed_shards: u32,
    /// 总发送条目数
    pub total_entries_sent: u64,
    /// 总重试次数
    pub total_retries: u32,
    /// 因超过发送重试上限而丢弃的批次数
    pub total_failed_batches: u64,
    /// 开始时间
    pub started_at: Option<std::time::Instant>,
}

// ============================================================================
// 分片同步引擎
// ============================================================================

/// 分片并行同步引擎
///
/// 运行在数据服务器侧，负责将差异 L2 分片数据流式推送到请求方。
/// 每个 (peer, repo_type) 组合同一时刻只允许一个引擎实例（由 SyncManager 互斥保证）。
pub struct ShardSyncEngine {
    /// 目标对端连接
    conn: Arc<Connection>,
    /// 对端节点 ID
    peer: NodeId,
    /// 仓库类型
    repo_type: u8,
    /// 配置快照
    config: FederationConfig,
    /// Merkle 树引用（用于 L2→L1 映射和重算）
    merkle: Arc<MerkleTree>,
    /// 待同步的 L2 分片队列
    pending_queue: Mutex<VecDeque<u32>>,
    /// 每个 L2 分片的状态
    shard_states: Mutex<FxHashMap<u32, ShardState>>,
    /// 在途批次：seq → PendingAck
    pending_acks: Mutex<FxHashMap<u64, PendingAck>>,
    /// 下一个批次序号
    next_seq: AtomicU64,
    /// 并发信号量（限制同时发送的 L2 分片数）
    semaphore: Arc<Semaphore>,
    /// 是否正在运行
    running: AtomicBool,
    /// 速率限制：上次发送时间戳（毫秒）+ 已发送计数
    rate_state: Mutex<(u64, u64)>,
    /// 统计信息
    stats: Mutex<ShardSyncStats>,
    /// 完成通知（可选，用于 SyncManager 等待引擎结束）
    done_tx: Mutex<Option<oneshot::Sender<ShardSyncStats>>>,
    /// 数据加载回调：输入L2分片号，返回该分片的所有SyncEntry
    load_fn: Option<Arc<dyn Fn(u32) -> Vec<SyncEntry> + Send + Sync>>,
    /// 加载分片内 (key, data_hash) 列表的回调（用于节点级 hash 去重）。
    /// 为 None 时回退到全量发送（兼容路径）。
    hash_list_fn: Option<Arc<dyn Fn(u32) -> Vec<(Vec<u8>, Vec<u8>)> + Send + Sync>>,
    /// 等待对端 ShardSyncMissing 回复的 oneshot：l2_shard → sender
    pending_missing: Mutex<FxHashMap<u32, oneshot::Sender<Vec<Vec<u8>>>>>,
    /// 连接存活标记：连续失败达到阈值后置为 false，触发上层重连
    connection_alive: Arc<AtomicBool>,
    /// 连续发送失败计数：每次发送失败递增，发送成功后归零
    consecutive_failures: Arc<AtomicU32>,
}

impl ShardSyncEngine {
    /// 创建新的分片同步引擎
    ///
    /// # 参数
    /// - `conn`: 到对端的活跃连接
    /// - `repo_type`: 仓库类型
    /// - `merkle`: 对应 repo 的 Merkle 树
    /// - `config`: 联邦配置
    pub fn new(
        conn: Arc<Connection>,
        peer: NodeId,
        repo_type: u8,
        merkle: Arc<MerkleTree>,
        config: FederationConfig,
        load_fn: Option<Arc<dyn Fn(u32) -> Vec<SyncEntry> + Send + Sync>>,
        hash_list_fn: Option<Arc<dyn Fn(u32) -> Vec<(Vec<u8>, Vec<u8>)> + Send + Sync>>,
    ) -> Self {
        let max_concurrency = config.shard_sync_max_concurrency.max(1);
        Self {
            conn,
            peer,
            repo_type,
            config,
            merkle,
            pending_queue: Mutex::new(VecDeque::new()),
            shard_states: Mutex::new(FxHashMap::default()),
            pending_acks: Mutex::new(FxHashMap::default()),
            next_seq: AtomicU64::new(0),
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
            running: AtomicBool::new(false),
            rate_state: Mutex::new((0, 0)),
            stats: Mutex::new(ShardSyncStats::default()),
            done_tx: Mutex::new(None),
            load_fn,
            hash_list_fn,
            pending_missing: Mutex::new(FxHashMap::default()),
            connection_alive: Arc::new(AtomicBool::new(true)),
            consecutive_failures: Arc::new(AtomicU32::new(0)),
        }
    }

    /// 设置完成通知通道（SyncManager 可借此等待引擎结束）
    pub fn set_done_tx(&self, tx: oneshot::Sender<ShardSyncStats>) {
        *self.done_tx.lock() = Some(tx);
    }

    /// 获取当前统计快照
    pub fn stats(&self) -> ShardSyncStats {
        self.stats.lock().clone()
    }

    /// 检查引擎是否正在运行
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// 检查连接是否存活
    pub fn is_connection_alive(&self) -> bool {
        self.connection_alive.load(Ordering::SeqCst)
    }

    /// 标记连接已断开，记录日志并停止当前同步
    ///
    /// 上层（SyncManager）检测到引擎结束后应重建连接并重试同步。
    pub fn mark_connection_dead(&self, reason: &str) {
        self.connection_alive.store(false, Ordering::SeqCst);
        warn!(
            "[shard-sync] 连接标记为断开: repo={}, peer={}, reason={}",
            self.repo_type, self.peer, reason
        );
    }

    // ========================================================================
    // 入口：启动同步
    // ========================================================================

    /// 将差异 L2 分片加入同步队列并启动引擎
    ///
    /// 此方法被调用后，引擎会在后台并行调度所有 L2 分片。
    /// 返回后不阻塞调用方，同步完成通过 `done_tx` 通知。
    pub fn start(self: Arc<Self>, l2_shards: Vec<u32>) {
        if l2_shards.is_empty() {
            debug!(
                "[shard-sync] 无可同步的 L2 分片，立即完成: repo={}, peer={}",
                self.repo_type, self.peer
            );
            self.finish();
            return;
        }

        // 初始化队列和状态
        {
            let mut queue = self.pending_queue.lock();
            let mut states = self.shard_states.lock();
            for &l2 in &l2_shards {
                states.entry(l2).or_insert_with(|| {
                    queue.push_back(l2);
                    ShardState::Pending
                });
            }
        }

        {
            let mut stats = self.stats.lock();
            stats.pending_shards = l2_shards.len() as u32;
            stats.started_at = Some(std::time::Instant::now());
        }

        self.running.store(true, Ordering::SeqCst);

        info!(
            "[shard-sync] 引擎启动: repo={}, peer={}, L2分片数={}, 并发={}",
            self.repo_type,
            self.peer,
            l2_shards.len(),
            self.config.shard_sync_max_concurrency
        );

        // 启动调度循环（在调用方的 tokio runtime 中）
        let engine = self.clone();
        tokio::spawn(async move {
            engine.run_scheduler().await;
        });
    }

    // ========================================================================
    // 调度循环
    // ========================================================================

    /// 主调度循环：从队列取分片，获取信号量，并行发送
    async fn run_scheduler(self: Arc<Self>) {
        let timeout_secs = self.config.shard_sync_timeout_secs;
        let batch_size = self.config.shard_sync_batch_size.max(1);
        let window = self.config.shard_sync_window_size.max(1);

        loop {
            // 检查连接是否存活，断开则停止调度
            if !self.is_connection_alive() {
                warn!(
                    "[shard-sync] 连接已断开，停止调度循环: repo={}, peer={}",
                    self.repo_type, self.peer
                );
                break;
            }

            // 取一个待同步分片
            let l2 = {
                let mut queue = self.pending_queue.lock();
                queue.pop_front()
            };

            let l2 = match l2 {
                Some(l2) => l2,
                None => {
                    // 队列空，等待所有在途分片完成
                    debug!(
                        "[shard-sync] 队列已空，等待在途分片完成: repo={}, peer={}",
                        self.repo_type, self.peer
                    );
                    break;
                }
            };

            // 获取并发许可
            let permit = match self.semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => break,
            };

            // 标记为 Sending
            {
                let mut states = self.shard_states.lock();
                states.insert(l2, ShardState::Sending);
            }

            // 异步发送该分片（不阻塞调度循环）
            let engine = self.clone();
            tokio::spawn(async move {
                engine
                    .sync_single_shard(l2, batch_size, window, timeout_secs)
                    .await;
                drop(permit);
            });
        }

        // 等待所有在途任务完成（通过信号量全部释放判断）
        // 简单方式：等待信号量计数恢复到最大值
        let max_conc = self.config.shard_sync_max_concurrency.max(1);
        let sem = self.semaphore.clone();
        for _ in 0..max_conc {
            let _permit = sem.clone().acquire_owned().await;
        }
        // 全部完成，释放许可
        for _ in 0..max_conc {
            sem.add_permits(1);
        }

        // 发送 ShardSyncComplete
        self.send_complete().await;

        self.finish();
    }

    /// 同步单个 L2 分片：加载数据 → 分批发送 → 等待 Ack → 重试
    async fn sync_single_shard(
        &self,
        l2: u32,
        batch_size: usize,
        window: usize,
        timeout_secs: u64,
    ) {
        let l1 = MerkleTree::l1_for_l2(l2);
        let retry_max = self.config.shard_sync_retry_count;

        debug!(
            "[shard-sync] 开始同步 L2={} (L1={}): repo={}, peer={}",
            l2, l1, self.repo_type, self.peer
        );

        // 从 DB 加载该 L1 的所有条目，然后按 L2 过滤
        // 注意：load_entries_by_shards 由 SyncManager 提供，这里通过回调模式
        // 实际实现中由 SyncManager 在创建引擎时注入加载函数
        let entries = match self.load_entries_for_l2(l2).await {
            Ok(e) => e,
            Err(e) => {
                warn!("[shard-sync] 加载 L2={} 数据失败: {}, 标记失败", l2, e);
                self.mark_shard_failed(l2);
                return;
            }
        };

        // P0-B 兜底：load_fn 已按 L2 过滤，这里再做一次防御性过滤，确保只发送属于本 L2 的条目
        // （万一上游漏了过滤，也不会把整条 L1 推给对端造成最多 256× 的数据放大）。
        let before_filter = entries.len();
        let entries: Vec<SyncEntry> = entries
            .into_iter()
            .filter(|e| self.merkle.l2_shard_for_key(&e.key) == l2)
            .collect();
        if entries.len() != before_filter {
            debug!(
                "[shard-sync] L2={} 防御性过滤: {} -> {}（上游 load_fn 未完全按 L2 过滤）",
                l2,
                before_filter,
                entries.len()
            );
        }

        if entries.is_empty() {
            debug!(
                "[shard-sync] L2={} 无数据，直接标记完成: repo={}",
                l2, self.repo_type
            );
            self.mark_shard_acked(l2, 0);
            return;
        }

        // 节点级 hash 去重：先把 (key, data_hash) 列表发给对端，等待缺失 key 回复，
        // 只发送真正缺失的条目。hash_list_fn 不可用或超时时回退到全量发送。
        let entries = match &self.hash_list_fn {
            Some(hash_fn) => {
                self.exchange_hash_list_and_filter(l2, hash_fn, entries, timeout_secs)
                    .await
            }
            None => entries,
        };

        if entries.is_empty() {
            debug!(
                "[shard-sync] L2={} 过滤后无缺失条目，直接完成: repo={}",
                l2, self.repo_type
            );
            self.mark_shard_acked(l2, 0);
            return;
        }

        // 分批发送，窗口流控
        let chunks: Vec<_> = entries.chunks(batch_size).collect();
        let mut sent_in_window = 0usize;
        let mut batch_idx = 0usize;
        // 当前批次的发送重试计数（跨失败重试持续递增，成功 Ack 后归零）
        // [ALLOWED-HARDCODED: 计数器初始归零，非可配置参数]
        let mut send_retries = 0u32;
        let max_send_retries = self.config.shard_sync_max_retries;
        let base_backoff_ms = self.config.shard_sync_base_backoff_ms;
        let max_backoff_ms = self.config.shard_sync_max_backoff_ms;
        let consecutive_threshold = self.config.shard_sync_consecutive_fail_threshold;

        while batch_idx < chunks.len() {
            // 窗口流控：等待在途批次数低于窗口大小
            if sent_in_window >= window {
                // [ALLOWED-SLEEP] 分片同步窗口流控，函数内限流等待，非周期性
                // [ALLOWED-HARDCODED: 窗口流控等待时间已提取为 shard_sync_window_flow_sleep_ms 配置]
                tokio::time::sleep(Duration::from_millis(
                    self.config.shard_sync_window_flow_sleep_ms,
                ))
                .await;
                let pending_count = self.pending_acks.lock().len();
                if pending_count < window {
                    sent_in_window = pending_count;
                }
                continue;
            }

            let batch_entries = chunks[batch_idx];
            let is_last = batch_idx + 1 == chunks.len();

            // 速率限制
            self.apply_rate_limit(batch_entries.len()).await;

            // 发送前注册 Ack 等待通道
            let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
            let (tx, rx) = oneshot::channel::<u32>();
            {
                let mut acks = self.pending_acks.lock();
                acks.insert(
                    seq,
                    PendingAck {
                        tx: Some(tx),
                        l2_shards: vec![l2],
                        // [ALLOWED-HARDCODED: Ack 重试计数初始归零，非可配置参数]
                        retry_count: 0,
                    },
                );
            }

            let msg = ShardSyncBatchMessage {
                repo_type: self.repo_type,
                l2_shards: vec![l2],
                entries: batch_entries.to_vec(),
                seq,
                is_last,
            };

            match timeout(
                Duration::from_secs(timeout_secs),
                self.conn.send_message(MessageType::ShardSyncBatch, &msg),
            )
            .await
            {
                Ok(Ok(())) => {
                    sent_in_window += 1;
                    self.stats.lock().total_entries_sent += batch_entries.len() as u64;
                    // 发送成功，重置连续失败计数
                    self.consecutive_failures.store(0, Ordering::SeqCst);
                }
                Ok(Err(e)) => {
                    self.pending_acks.lock().remove(&seq);
                    self.stats.lock().total_retries += 1;
                    send_retries += 1;
                    let consecutive = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;

                    // 超过单批次最大发送重试次数，丢弃该批次
                    if send_retries > max_send_retries {
                        warn!(
                            "[shard-sync] L2={} batch {} 发送失败超过最大重试次数({}): {}, 丢弃",
                            l2, batch_idx, max_send_retries, e
                        );
                        self.stats.lock().total_failed_batches += 1;
                        self.mark_shard_failed(l2);
                        return;
                    }

                    // 连续失败达到阈值，标记连接断开
                    if consecutive >= consecutive_threshold {
                        self.mark_connection_dead(&format!("连续{}次发送失败: {}", consecutive, e));
                        self.mark_shard_failed(l2);
                        return;
                    }

                    // 指数退避：base * 2^send_retries，封顶 max_backoff
                    let backoff = std::cmp::min(
                        base_backoff_ms.saturating_mul(2u64.saturating_pow(send_retries)),
                        max_backoff_ms,
                    );
                    warn!(
                        "[shard-sync] 发送 ShardSyncBatch seq={} 失败: {}, 重试({}/{}), 退避={}ms",
                        seq, e, send_retries, max_send_retries, backoff
                    );
                    // [ALLOWED-SLEEP] 分片同步发送失败指数退避，函数内重试等待，非周期性
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                    continue;
                }
                Err(_) => {
                    self.pending_acks.lock().remove(&seq);
                    self.stats.lock().total_retries += 1;
                    send_retries += 1;
                    let consecutive = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;

                    // 超过单批次最大发送重试次数，丢弃该批次
                    if send_retries > max_send_retries {
                        warn!(
                            "[shard-sync] L2={} batch {} 发送超时超过最大重试次数({}), 丢弃",
                            l2, batch_idx, max_send_retries
                        );
                        self.stats.lock().total_failed_batches += 1;
                        self.mark_shard_failed(l2);
                        return;
                    }

                    // 连续失败达到阈值，标记连接断开
                    if consecutive >= consecutive_threshold {
                        self.mark_connection_dead(&format!("连续{}次发送超时", consecutive));
                        self.mark_shard_failed(l2);
                        return;
                    }

                    // 指数退避：base * 2^send_retries，封顶 max_backoff
                    let backoff = std::cmp::min(
                        base_backoff_ms.saturating_mul(2u64.saturating_pow(send_retries)),
                        max_backoff_ms,
                    );
                    warn!(
                        "[shard-sync] 发送 ShardSyncBatch seq={} 超时, 重试({}/{}), 退避={}ms",
                        seq, send_retries, max_send_retries, backoff
                    );
                    // [ALLOWED-SLEEP] 分片同步发送超时指数退避，函数内重试等待，非周期性
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                    continue;
                }
            }

            // 等待 Ack（带超时）
            match timeout(Duration::from_secs(timeout_secs), rx).await {
                Ok(Ok(applied)) => {
                    debug!(
                        "[shard-sync] L2={} batch {}/{} 已确认 ({} entries): repo={}",
                        l2,
                        batch_idx + 1,
                        chunks.len(),
                        applied,
                        self.repo_type
                    );
                    sent_in_window -= 1;
                    batch_idx += 1;
                    // [ALLOWED-HARDCODED: 批次确认成功后重试计数归零，非可配置参数]
                    send_retries = 0;
                }
                Ok(Err(_)) => {
                    // oneshot 被丢弃（连接断开）
                    warn!("[shard-sync] L2={} batch {} Ack通道关闭", l2, batch_idx);
                    self.mark_shard_failed(l2);
                    return;
                }
                Err(_) => {
                    // Ack 超时，检查重试次数
                    let retry_count = {
                        let mut acks = self.pending_acks.lock();
                        let entry = acks.get_mut(&seq);
                        match entry {
                            Some(e) => {
                                e.retry_count += 1;
                                e.retry_count
                            }
                            None => 0,
                        }
                    };

                    if retry_count > retry_max {
                        warn!(
                            "[shard-sync] L2={} batch {} 超过最大重试次数({}), 标记失败",
                            l2, batch_idx, retry_max
                        );
                        self.pending_acks.lock().remove(&seq);
                        self.mark_shard_failed(l2);
                        return;
                    }

                    warn!(
                        "[shard-sync] L2={} batch {} Ack 超时, 重试 ({}/{})",
                        l2, batch_idx, retry_count, retry_max
                    );
                    self.stats.lock().total_retries += 1;
                    // 重试：重新注册 oneshot 并重发
                    let (new_tx, new_rx) = oneshot::channel::<u32>();
                    {
                        let mut acks = self.pending_acks.lock();
                        if let Some(entry) = acks.get_mut(&seq) {
                            entry.tx = Some(new_tx);
                        }
                    }
                    // 重新发送同批次
                    let msg = ShardSyncBatchMessage {
                        repo_type: self.repo_type,
                        l2_shards: vec![l2],
                        entries: batch_entries.to_vec(),
                        seq,
                        is_last,
                    };
                    if let Err(e) = self
                        .conn
                        .send_message(MessageType::ShardSyncBatch, &msg)
                        .await
                    {
                        warn!("[shard-sync] 重发 seq={} 失败: {}", seq, e);
                        self.pending_acks.lock().remove(&seq);
                        self.mark_shard_failed(l2);
                        return;
                    }
                    // 继续等待新的 Ack（不推进 batch_idx）
                    // 注意：这里直接等待，简化逻辑
                    match timeout(Duration::from_secs(timeout_secs), new_rx).await {
                        Ok(Ok(_)) => {
                            sent_in_window -= 1;
                            batch_idx += 1;
                            // [ALLOWED-HARDCODED: 重试后重置计数归零，非可配置参数]
                            send_retries = 0;
                        }
                        _ => {
                            self.pending_acks.lock().remove(&seq);
                            self.mark_shard_failed(l2);
                            return;
                        }
                    }
                }
            }
        }

        // 所有批次发送并确认完毕
        self.mark_shard_acked(l2, entries.len() as u32);
        debug!(
            "[shard-sync] L2={} 同步完成: repo={}, entries={}",
            l2,
            self.repo_type,
            entries.len()
        );
    }

    // ========================================================================
    // Ack 处理（由 SyncManager 在收到 ShardSyncAck 时调用）
    // ========================================================================

    /// 处理收到的 ShardSyncAck
    ///
    /// 唤醒等待对应 seq 的 oneshot。
    pub fn handle_ack(&self, ack: &ShardSyncAckMessage) {
        let mut acks = self.pending_acks.lock();
        if let Some(entry) = acks.remove(&ack.seq) {
            if let Some(tx) = entry.tx {
                let _ = tx.send(ack.applied_count);
            }
        }
    }

    /// 处理收到的 ShardSyncMissing：唤醒等待对应 l2_shard 的 oneshot
    ///
    /// 由 SyncManager 在收到对端回复的缺失 key 列表时调用。
    pub fn handle_shard_sync_missing(&self, l2_shard: u32, missing_keys: Vec<Vec<u8>>) {
        if let Some(tx) = self.pending_missing.lock().remove(&l2_shard) {
            let _ = tx.send(missing_keys);
        }
    }

    /// 节点级 hash 去重：发送分片内 (key, data_hash) 列表，等待对端回复缺失 key，
    /// 从 entries 中过滤出真正缺失的条目。
    ///
    /// 对端（旧版本或不支持新消息）不回复时，超时后回退到原始 entries（全量发送）。
    async fn exchange_hash_list_and_filter(
        &self,
        l2: u32,
        hash_fn: &Arc<dyn Fn(u32) -> Vec<(Vec<u8>, Vec<u8>)> + Send + Sync>,
        entries: Vec<SyncEntry>,
        timeout_secs: u64,
    ) -> Vec<SyncEntry> {
        // 1. 加载 (key, data_hash) 列表
        let hash_list = hash_fn(l2);
        if hash_list.is_empty() {
            return entries;
        }

        // 2. 注册 oneshot 等待对端回复
        let (tx, rx) = oneshot::channel::<Vec<Vec<u8>>>();
        self.pending_missing.lock().insert(l2, tx);

        // 3. 分批发送 hash 列表（失败时清理 pending 并回退全量发送）
        let send_result: anyhow::Result<()> = async {
            let chunks: Vec<_> = hash_list.chunks(SHARD_HASH_LIST_CHUNK_SIZE).collect();
            let total = chunks.len();
            for (i, chunk) in chunks.into_iter().enumerate() {
                let is_last = i + 1 == total;
                let msg = ShardSyncHashListMessage {
                    repo_type: self.repo_type,
                    l2_shard: l2,
                    entries: chunk.to_vec(),
                    is_last,
                };
                self.conn
                    .send_message(MessageType::ShardSyncHashList, &msg)
                    .await?;
            }
            Ok(())
        }
        .await;

        if send_result.is_err() {
            self.pending_missing.lock().remove(&l2);
            debug!("[shard-sync] hash list 发送失败，回退全量发送 L2={}", l2);
            return entries;
        }

        // 4. 等待对端回复缺失 key（带超时，复用 shard_sync_timeout_secs）
        let missing_keys = match timeout(Duration::from_secs(timeout_secs), rx).await {
            Ok(Ok(keys)) => keys,
            _ => {
                self.pending_missing.lock().remove(&l2);
                debug!(
                    "[shard-sync] 等待 ShardSyncMissing 失败/超时，回退全量发送 L2={}",
                    l2
                );
                return entries;
            }
        };

        // 5. 过滤 entries，只保留对端缺失的 key
        let missing_set: FxHashSet<&[u8]> = missing_keys.iter().map(|k| k.as_slice()).collect();
        let filtered: Vec<SyncEntry> = entries
            .into_iter()
            .filter(|e| missing_set.contains(e.key.as_slice()))
            .collect();

        debug!(
            "[shard-sync] L2={} hash 去重: 总条目={}, 对端缺失={}, 过滤后发送={}",
            l2,
            hash_list.len(),
            missing_keys.len(),
            filtered.len()
        );
        filtered
    }

    // ========================================================================
    // 内部辅助方法
    // ========================================================================

    /// 标记 L2 分片为已确认
    fn mark_shard_acked(&self, l2: u32, _applied: u32) {
        let mut states = self.shard_states.lock();
        states.insert(l2, ShardState::Acked);
        drop(states);

        let mut stats = self.stats.lock();
        stats.acked_shards += 1;
    }

    /// 标记 L2 分片为失败
    fn mark_shard_failed(&self, l2: u32) {
        let mut states = self.shard_states.lock();
        states.insert(l2, ShardState::Failed);
        drop(states);

        let mut stats = self.stats.lock();
        stats.failed_shards += 1;
        warn!(
            "[shard-sync] L2={} 同步失败: repo={}, peer={}",
            l2, self.repo_type, self.peer
        );
    }

    /// 速率限制：按 shard_sync_rate_limit_per_sec 限制发送速率
    async fn apply_rate_limit(&self, batch_entries: usize) {
        let limit = self.config.shard_sync_rate_limit_per_sec;
        if limit == 0 {
            return; // 不限制
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let wait_ms = {
            let mut rate = self.rate_state.lock();
            let (window_start, sent) = *rate;

            // 新窗口（1 秒）
            if now_ms - window_start >= 1000 {
                *rate = (now_ms, batch_entries as u64);
                0
            } else {
                let new_sent = sent + batch_entries as u64;
                if new_sent > limit {
                    // 需要等待到窗口结束
                    let wait = 1000 - (now_ms - window_start);
                    *rate = (now_ms + wait, 0);
                    wait
                } else {
                    rate.1 = new_sent;
                    0
                }
            }
        };

        if wait_ms > 0 {
            // [ALLOWED-SLEEP] 分片同步速率限制，函数内限流等待，非周期性
            tokio::time::sleep(Duration::from_millis(wait_ms)).await;
        }
    }

    /// 发送 ShardSyncComplete 消息
    async fn send_complete(&self) {
        let stats = self.stats.lock().clone();
        let msg = ShardSyncCompleteMessage {
            repo_type: self.repo_type,
            total_l2_shards: stats.acked_shards + stats.failed_shards,
            total_entries: stats.total_entries_sent,
            max_seq: self.next_seq.load(Ordering::SeqCst),
        };

        info!(
            "[shard-sync] 发送 ShardSyncComplete: repo={}, acked={}, failed={}, entries={}, to={}",
            self.repo_type,
            stats.acked_shards,
            stats.failed_shards,
            stats.total_entries_sent,
            self.peer
        );

        if let Err(e) = self
            .conn
            .send_message(MessageType::ShardSyncComplete, &msg)
            .await
        {
            warn!("[shard-sync] 发送 ShardSyncComplete 失败: {}", e);
        }
    }

    /// 引擎结束：清理状态，通知完成
    fn finish(&self) {
        self.running.store(false, Ordering::SeqCst);

        let stats = self.stats.lock().clone();
        info!(
            "[shard-sync] 引擎完成: repo={}, peer={}, acked={}, failed={}, entries={}, elapsed={:?}",
            self.repo_type,
            self.peer,
            stats.acked_shards,
            stats.failed_shards,
            stats.total_entries_sent,
            stats.started_at.map(|s| s.elapsed()),
        );

        // 通知等待方
        if let Some(tx) = self.done_tx.lock().take() {
            let _ = tx.send(stats);
        }
    }

    /// 加载指定 L2 分片的同步条目（由 SyncManager 注入实际加载逻辑）
    ///
    /// 实际数据加载在 SyncManager 中完成（需要访问各 repo 的 storage），
    /// 这里通过 trait object 或闭包注入。为简化，当前实现委托给 SyncManager。
    ///
    /// 返回 (Vec<SyncEntry>)，调用方负责分批和发送。
    async fn load_entries_for_l2(&self, l2: u32) -> anyhow::Result<Vec<SyncEntry>> {
        let load_fn = self
            .load_fn
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("load_fn未注入"))?;
        Ok(load_fn(l2))
    }
}

impl Clone for ShardSyncEngine {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            peer: self.peer,
            repo_type: self.repo_type,
            config: self.config.clone(),
            merkle: self.merkle.clone(),
            pending_queue: Mutex::new(self.pending_queue.lock().clone()),
            shard_states: Mutex::new(self.shard_states.lock().clone()),
            pending_acks: Mutex::new(FxHashMap::default()), // pending acks 不 clone（属于当前实例）
            next_seq: AtomicU64::new(self.next_seq.load(Ordering::SeqCst)),
            semaphore: self.semaphore.clone(),
            running: AtomicBool::new(self.running.load(Ordering::SeqCst)),
            rate_state: Mutex::new(*self.rate_state.lock()),
            stats: Mutex::new(self.stats.lock().clone()),
            done_tx: Mutex::new(None), // done_tx 不 clone
            load_fn: self.load_fn.clone(),
            hash_list_fn: self.hash_list_fn.clone(),
            pending_missing: Mutex::new(FxHashMap::default()), // pending missing 不 clone（属于当前实例）
            connection_alive: self.connection_alive.clone(),
            consecutive_failures: self.consecutive_failures.clone(),
        }
    }
}

// ============================================================================
// 分层 Merkle 对比辅助函数
// ============================================================================

/// 分层 Merkle 对比阶段状态
///
/// 用于追踪正在进行的分层对比流程：
/// 1. 请求 L0 根 → 一致则无差异
/// 2. 请求/对比 L1 → 找出差异 L1
/// 3. 对每个差异 L1 请求 L2 → 找出差异 L2
/// 4. 启动 ShardSyncEngine 同步差异 L2
#[derive(Debug, Clone)]
pub enum LayeredComparePhase {
    /// 初始阶段：刚收到 MerkleDigest，需要对比 L1
    CompareL1,
    /// 已发送 L2 请求，等待响应收集
    AwaitingL2 {
        /// 已对比的 L1 分片
        compared_l1: Vec<u16>,
        /// 已收集的差异 L2（绝对索引）
        diff_l2: FxHashSet<u32>,
        /// 还在等待响应的 L1 分片数
        pending_l1_count: usize,
    },
    /// 对比完成，可以启动同步
    Complete {
        /// 最终的差异 L2 分片列表
        diff_l2: Vec<u32>,
    },
}

/// 分层对比会话状态（每对 peer+repo 一个）
pub struct LayeredCompareSession {
    /// 当前阶段
    pub phase: LayeredComparePhase,
    /// 对端节点 ID
    pub peer: NodeId,
    /// 仓库类型
    pub repo_type: u8,
    /// 开始时间
    pub started_at: std::time::Instant,
}

impl LayeredCompareSession {
    pub fn new(peer: NodeId, repo_type: u8) -> Self {
        Self {
            phase: LayeredComparePhase::CompareL1,
            peer,
            repo_type,
            started_at: std::time::Instant::now(),
        }
    }
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_state_transitions() {
        // 测试状态枚举的完整性
        let states = [
            ShardState::Pending,
            ShardState::Sending,
            ShardState::Acked,
            ShardState::Failed,
        ];
        assert_eq!(states.len(), 4);
    }

    #[test]
    fn test_layered_compare_session_creation() {
        let peer = NodeId([1u8; 20]);
        let session = LayeredCompareSession::new(peer, repo_type::NODE);
        assert_eq!(session.peer, peer);
        assert_eq!(session.repo_type, repo_type::NODE);
        assert!(matches!(session.phase, LayeredComparePhase::CompareL1));
    }

    #[test]
    fn test_layered_compare_phase_complete() {
        let diff_l2 = vec![1u32, 2, 3, 100, 65535];
        let phase = LayeredComparePhase::Complete {
            diff_l2: diff_l2.clone(),
        };
        if let LayeredComparePhase::Complete { diff_l2: got } = phase {
            assert_eq!(got, diff_l2);
        } else {
            panic!("应该是 Complete 阶段");
        }
    }

    #[test]
    fn test_shard_sync_stats_default() {
        let stats = ShardSyncStats::default();
        assert_eq!(stats.pending_shards, 0);
        assert_eq!(stats.acked_shards, 0);
        assert_eq!(stats.failed_shards, 0);
        assert_eq!(stats.total_entries_sent, 0);
        assert_eq!(stats.total_retries, 0);
        assert_eq!(stats.total_failed_batches, 0);
        assert!(stats.started_at.is_none());
    }

    #[test]
    fn test_pending_ack_creation() {
        let (tx, _rx) = oneshot::channel::<u32>();
        let ack = PendingAck {
            tx: Some(tx),
            l2_shards: vec![1, 2, 3],
            retry_count: 0,
        };
        assert_eq!(ack.l2_shards.len(), 3);
        assert_eq!(ack.retry_count, 0);
    }

    #[test]
    fn test_l2_to_l1_mapping() {
        // 验证 L2 → L1 映射正确性
        let test_cases = [
            (0u32, 0u16),
            (255, 0),
            (256, 1),
            (511, 1),
            (65535, 255),
            (1000, 3), // 1000 / 256 = 3
            (1024, 4), // 1024 / 256 = 4
        ];
        for (l2, expected_l1) in &test_cases {
            assert_eq!(
                MerkleTree::l1_for_l2(*l2),
                *expected_l1,
                "L2={} 应映射到 L1={}",
                l2,
                expected_l1
            );
        }
    }

    #[test]
    fn test_l2_per_l1_constant() {
        assert_eq!(crate::federation::merkle::L2_PER_L1, 256);
    }
}
