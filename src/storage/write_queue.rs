//! SQLite 写入队列
//!
//! 专用 writer task + mpsc 队列，所有写入攒批后一次性事务写入。
//! 避免频繁小事务写入，提升 SQLite 写入效率 10x+。
//!
//! 写入请求使用闭包，调用 Storage 已有的 save 方法，避免重复实现 SQL。

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rusqlite::Connection;
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
pub struct WriteQueue {
    sender: mpsc::UnboundedSender<WriteRequest>,
    stats: Arc<Mutex<WriteStats>>,
}

impl WriteQueue {
    /// 创建写入队列并启动 writer task
    pub fn new(conn: Arc<Mutex<Connection>>, batch_size: usize, flush_interval: Duration) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel::<WriteRequest>();
        let stats = Arc::new(Mutex::new(WriteStats::default()));

        let stats_clone = stats.clone();
        tokio::spawn(async move {
            Self::writer_loop(conn, receiver, batch_size, flush_interval, stats_clone).await;
        });

        Self { sender, stats }
    }

    /// 发送写入请求
    pub fn send<F>(&self, f: F)
    where
        F: FnOnce(&Connection) -> anyhow::Result<()> + Send + 'static,
    {
        let _ = self.sender.send(Box::new(f));
        self.stats.lock().total_requests += 1;
    }

    /// 获取统计
    pub fn stats(&self) -> WriteStats {
        self.stats.lock().clone()
    }

    /// writer 主循环
    async fn writer_loop(
        conn: Arc<Mutex<Connection>>,
        mut receiver: mpsc::UnboundedReceiver<WriteRequest>,
        batch_size: usize,
        flush_interval: Duration,
        stats: Arc<Mutex<WriteStats>>,
    ) {
        let mut batch: Vec<WriteRequest> = Vec::with_capacity(batch_size);
        let mut interval = tokio::time::interval(flush_interval);

        loop {
            tokio::select! {
                req = receiver.recv() => {
                    match req {
                        Some(req) => {
                            batch.push(req);
                            if batch.len() >= batch_size {
                                Self::flush_batch(&conn, &mut batch, &stats);
                            }
                        }
                        None => {
                            if !batch.is_empty() {
                                Self::flush_batch(&conn, &mut batch, &stats);
                            }
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        Self::flush_batch(&conn, &mut batch, &stats);
                    }
                }
            }
        }
    }

    /// 批量写入（单事务）
    fn flush_batch(
        conn: &Arc<Mutex<Connection>>,
        batch: &mut Vec<WriteRequest>,
        stats: &Arc<Mutex<WriteStats>>,
    ) {
        let requests = std::mem::take(batch);
        let count = requests.len();

        let conn = conn.lock();
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
