//! Merkle 树异步批量更新队列
//!
//! 联邦同步 apply 时不再同步调用 merkle.update_batch，而是将 (repo_type, key, payload)
//! 入队，由后台任务每 interval 毫秒或达到 batch_size 时批量 flush，降低 apply 路径的 CPU 占用。

use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;

/// 单条待更新条目：(repo_type, key, payload)
pub type MerkleUpdateEntry = (u8, Vec<u8>, Vec<u8>);

/// Merkle 树异步批量更新队列
pub struct MerkleUpdateQueue {
    pending: Mutex<Vec<MerkleUpdateEntry>>,
    notify: Notify,
}

impl MerkleUpdateQueue {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(Vec::new()),
            notify: Notify::new(),
        }
    }

    /// 入队一条更新，唤醒后台 flush 任务。
    /// 多次 push 在后台任务未消费时会被 Notify 合并为一次唤醒，不会造成惊群。
    pub fn push(&self, repo_type: u8, key: Vec<u8>, payload: Vec<u8>) {
        self.pending.lock().push((repo_type, key, payload));
        self.notify.notify_one();
    }

    /// 批量入队，只通知一次（比逐条 push 减少锁竞争与 notify 调用）。
    pub fn push_batch<I>(&self, items: I)
    where
        I: IntoIterator<Item = MerkleUpdateEntry>,
    {
        let mut pending = self.pending.lock();
        pending.extend(items);
        drop(pending);
        self.notify.notify_one();
    }

    /// 取出全部待更新条目
    pub fn drain(&self) -> Vec<MerkleUpdateEntry> {
        std::mem::take(&mut *self.pending.lock())
    }

    /// 当前队列长度
    pub fn len(&self) -> usize {
        self.pending.lock().len()
    }

    /// 队列是否为空
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 获取 Notify 引用（供后台任务 await）
    pub fn notify(&self) -> &Notify {
        &self.notify
    }

    /// 同步等待队列排空（最多 timeout），返回是否已排空。
    /// 用于反熵对账前确保 Merkle 值已更新完毕，避免用旧 Merkle 值对账导致误判。
    /// 以 10ms 粒度轮询，由后台 flush 任务排空队列。
    pub fn wait_drain(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if self.is_empty() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        self.is_empty()
    }
}

impl Default for MerkleUpdateQueue {
    fn default() -> Self {
        Self::new()
    }
}
