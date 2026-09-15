//! SQLite 写入队列
//!
//! 专用 writer task + mpsc 队列，所有写入攒批后一次性事务写入。
//! 避免频繁小事务写入，提升 SQLite 写入效率 10x+。
//!
//! 写入请求使用闭包，调用 Storage 已有的 save 方法，避免重复实现 SQL。

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex as ParkingMutex;
use rusqlite::Connection;
use std::sync::Mutex as StdMutex;
use tokio::sync::mpsc;

/// 写入请求（闭包形式，调用 Storage 已有方法）
pub type WriteRequest = Box<dyn FnOnce(&Connection) -> anyhow::Result<()> + Send>;

/// 写入统计
#[derive(Debug, Default, Clone)]
pub struct WriteStats {
    pub total_requests: u64,
    pub total_batches: u64,
    pub queue_size: usize,
}

/// 写入队列
///
/// 事件驱动消费者 + TaskScheduler 定时 flush 双模式：
/// - writer_loop 持续监听 mpsc 通道，攒批后批量写入
/// - flush() 由 TaskScheduler 定期调用，强制刷出未达批量阈值的数据
pub struct WriteQueue {
    sender: mpsc::UnboundedSender<WriteRequest>,
    stats: Arc<ParkingMutex<WriteStats>>,
    batch: Arc<ParkingMutex<Vec<WriteRequest>>>,
    conn: Arc<StdMutex<Connection>>,
}

impl WriteQueue {
    /// 创建写入队列并启动 writer task
    ///
    /// `flush_interval` 参数保留用于向后兼容，实际定时 flush 由 TaskScheduler 调用 `flush()` 驱动。
    pub fn new(
        conn: Arc<StdMutex<Connection>>,
        batch_size: usize,
        _flush_interval: Duration,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel::<WriteRequest>();
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
        }
    }

    /// 发送写入请求
    pub fn send<F>(&self, f: F)
    where
        F: FnOnce(&Connection) -> anyhow::Result<()> + Send + 'static,
    {
        let _ = self.sender.send(Box::new(f));
        self.stats.lock().total_requests += 1;
    }

    /// 强制 flush 当前批次（由 TaskScheduler 定期调用）
    pub fn flush(&self) {
        let mut batch = self.batch.lock();
        if !batch.is_empty() {
            Self::flush_batch(&self.conn, &mut batch, &self.stats);
        }
    }

    /// 获取统计
    pub fn stats(&self) -> WriteStats {
        self.stats.lock().clone()
    }

    /// writer 主循环（纯事件驱动，无独立定时）
    async fn writer_loop(
        conn: Arc<StdMutex<Connection>>,
        mut receiver: mpsc::UnboundedReceiver<WriteRequest>,
        batch_size: usize,
        batch: Arc<ParkingMutex<Vec<WriteRequest>>>,
        stats: Arc<ParkingMutex<WriteStats>>,
    ) {
        loop {
            match receiver.recv().await {
                Some(req) => {
                    let mut b = batch.lock();
                    b.push(req);
                    if b.len() >= batch_size {
                        drop(b);
                        let mut guard = batch.lock();
                        Self::flush_batch(&conn, &mut guard, &stats);
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

        let conn = conn.lock().unwrap();
        let tx = match conn.unchecked_transaction() {
            Ok(tx) => tx,
            Err(e) => {
                tracing::warn!("[write_queue] 事务开启失败: {}", e);
                return;
            }
        };

        for req in requests {
            if let Err(e) = req(&conn) {
                tracing::warn!("[write_queue] 写入失败: {}", e);
            }
        }

        if let Err(e) = tx.commit() {
            tracing::warn!("[write_queue] 事务提交失败: {}", e);
        }

        let mut s = stats.lock();
        s.total_batches += 1;
        s.queue_size = count;
    }
}
