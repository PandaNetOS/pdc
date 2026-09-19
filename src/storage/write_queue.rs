//! SQLite 写入队列（兼容层）
//!
//! 保留原有公共 API（new/send/flush/stats），内部根据 IOScheduler 配置：
//! - enabled=false（默认）：走原始 mpsc + 攒批事务写入逻辑
//! - enabled=true：委托给 IOScheduler（优先级队列 + 令牌桶 + 请求合并）
//!
//! 现有调用方零修改即可工作。

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex as ParkingMutex;
use rusqlite::Connection;
use std::sync::Mutex as StdMutex;
use tokio::sync::mpsc;

use crate::storage::io_scheduler::{IoPriority, IoScheduler};

/// 写入请求（闭包形式，调用 Storage 已有方法）
pub type WriteRequest = Box<dyn FnOnce(&Connection) -> anyhow::Result<()> + Send>;

/// 写入统计
#[derive(Debug, Default, Clone)]
pub struct WriteStats {
    pub total_requests: u64,
    pub total_batches: u64,
    pub queue_size: usize,
    /// 因队列满/通道关闭而丢弃的请求数（>0 表示发生了背压丢写，需关注）
    pub dropped_requests: u64,
}

/// 写入队列通道容量上限。满时 `send` 不阻塞、丢弃并计数（避免无界增长 OOM）。
const WRITE_QUEUE_CAPACITY: usize = 100_000;

/// 写入队列（兼容层）
///
/// 事件驱动消费者 + TaskScheduler 定时 flush 双模式：
/// - writer_loop 持续监听 mpsc 通道，攒批后批量写入
/// - flush() 由 TaskScheduler 定期调用，强制刷出未达批量阈值的数据
///
/// 当 IOScheduler 启用时，send() 委托给 IOScheduler 的优先级队列。
pub struct WriteQueue {
    /// 原始 mpsc sender（IO Scheduler 未启用时使用）
    sender: mpsc::Sender<WriteRequest>,
    stats: Arc<ParkingMutex<WriteStats>>,
    batch: Arc<ParkingMutex<Vec<WriteRequest>>>,
    conn: Arc<StdMutex<Connection>>,
    /// IOScheduler（启用时 Some，send() 委托到此）
    io_scheduler: Option<Arc<IoScheduler>>,
}

impl WriteQueue {
    /// 创建写入队列并启动 writer task（原始模式，向后兼容）
    ///
    /// `flush_interval` 参数保留用于向后兼容，实际定时 flush 由 TaskScheduler 调用 `flush()` 驱动。
    pub fn new(
        conn: Arc<StdMutex<Connection>>,
        batch_size: usize,
        _flush_interval: Duration,
    ) -> Self {
        let (sender, receiver) = mpsc::channel::<WriteRequest>(WRITE_QUEUE_CAPACITY);
        let stats = Arc::new(ParkingMutex::new(WriteStats::default()));
        let batch = Arc::new(ParkingMutex::new(Vec::with_capacity(batch_size)));

        let batch_clone = batch.clone();
        let conn_clone = conn.clone();
        let stats_clone = stats.clone();

        tokio::spawn(async move {
            Self::writer_loop(conn_clone, receiver, batch_size, batch_clone, stats_clone).await;
        });

        Self {
            sender,
            stats,
            batch,
            conn,
            io_scheduler: None,
        }
    }

    /// 创建写入队列（IOScheduler 模式）
    ///
    /// send() 委托给 IOScheduler（Normal 优先级），flush() 调用 IOScheduler 的 barrier。
    pub fn with_scheduler(conn: Arc<StdMutex<Connection>>, io_scheduler: Arc<IoScheduler>) -> Self {
        let (sender, _receiver) = mpsc::channel::<WriteRequest>(WRITE_QUEUE_CAPACITY);
        let stats = Arc::new(ParkingMutex::new(WriteStats::default()));
        let batch = Arc::new(ParkingMutex::new(Vec::new()));

        Self {
            sender,
            stats,
            batch,
            conn,
            io_scheduler: Some(io_scheduler),
        }
    }

    /// 发送写入请求
    ///
    /// - IOScheduler 启用时：以 Normal 优先级提交到 IOScheduler
    /// - 原始模式：发送到 mpsc 通道，由 writer_loop 攒批写入
    pub fn send<F>(&self, f: F)
    where
        F: FnOnce(&Connection) -> anyhow::Result<()> + Send + 'static,
    {
        // 统计
        self.stats.lock().total_requests += 1;

        if let Some(ref sched) = self.io_scheduler {
            // IOScheduler 模式：以 Normal 优先级提交
            if let Err(e) = sched.submit(IoPriority::Normal, None, f, 1) {
                tracing::warn!("[write_queue] IOScheduler 提交失败: {}", e);
                self.stats.lock().dropped_requests += 1;
            }
        } else {
            // 原始模式：有界 mpsc 通道，满则丢弃并计数（不阻塞调用方）
            match self.sender.try_send(Box::new(f)) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    let mut s = self.stats.lock();
                    s.dropped_requests += 1;
                    tracing::error!(
                        "[write_queue] 写入队列已满（容量 {}），丢弃请求（累计 {}）",
                        WRITE_QUEUE_CAPACITY,
                        s.dropped_requests
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.stats.lock().dropped_requests += 1;
                    tracing::error!("[write_queue] 写入队列已关闭，请求被丢弃");
                }
            }
        }
    }

    /// 强制 flush 当前批次（由 TaskScheduler 定期调用）
    ///
    /// - IOScheduler 启用时：提交 Critical barrier 请求，触发队列排空
    /// - 原始模式：flush 本地攒批 buffer
    pub fn flush(&self) {
        if let Some(ref sched) = self.io_scheduler {
            // IOScheduler 模式：提交 barrier
            sched.flush_barrier();
        } else {
            // 原始模式：flush 本地 batch
            let mut batch = self.batch.lock();
            if !batch.is_empty() {
                Self::flush_batch(&self.conn, &mut batch, &self.stats);
            }
        }
    }

    /// 获取统计
    pub fn stats(&self) -> WriteStats {
        self.stats.lock().clone()
    }

    /// writer 主循环（纯事件驱动，无独立定时）— 仅原始模式使用
    async fn writer_loop(
        conn: Arc<StdMutex<Connection>>,
        mut receiver: mpsc::Receiver<WriteRequest>,
        batch_size: usize,
        batch: Arc<ParkingMutex<Vec<WriteRequest>>>,
        stats: Arc<ParkingMutex<WriteStats>>,
    ) {
        loop {
            match receiver.recv().await {
                Some(req) => {
                    // 锁序固定为 batch → conn（与 flush() 一致），在 batch 锁内直接刷批
                    let mut b = batch.lock();
                    b.push(req);
                    if b.len() >= batch_size {
                        Self::flush_batch(&conn, &mut b, &stats);
                    }
                }
                None => {
                    let mut b = batch.lock();
                    if !b.is_empty() {
                        Self::flush_batch(&conn, &mut b, &stats);
                    }
                    break;
                }
            }
        }
    }

    /// 批量写入（单事务）
    fn flush_batch(
        conn: &Arc<StdMutex<Connection>>,
        batch: &mut Vec<WriteRequest>,
        stats: &Arc<ParkingMutex<WriteStats>>,
    ) {
        let requests = std::mem::take(batch);
        let count = requests.len();

        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = match conn.unchecked_transaction() {
            Ok(tx) => tx,
            Err(e) => {
                tracing::warn!("[write_queue] 事务开启失败: {}", e);
                return;
            }
        };

        // 单个 payload panic 不得穿出 writer_loop；出现 panic 时整批回滚
        let mut degraded = false;
        for req in requests {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| req(&conn)));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!("[write_queue] 写入失败: {}", e),
                Err(_) => {
                    tracing::error!("[write_queue] payload panic，本批事务回滚");
                    degraded = true;
                    break;
                }
            }
        }

        if degraded {
            drop(tx); // Transaction 的 Drop 会回滚
        } else if let Err(e) = tx.commit() {
            tracing::warn!("[write_queue] 事务提交失败: {}", e);
        }

        let mut s = stats.lock();
        s.total_batches += 1;
        s.queue_size = count;
    }
}
