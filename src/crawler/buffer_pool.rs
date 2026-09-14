//! 爬虫专用接收缓冲区池
//!
//! B3：将对象池接入爬虫热路径。
//! 多 socket 接收循环共享一个池，避免每次 `vec![0u8; 65536]` 分配。
//!
//! 设计说明：
//! - 底层复用 [`crate::utils::object_pool::ObjectPool`]（crossbeam ArrayQueue 无锁实现）。
//! - 归还时缓冲区被 `clear()`（len=0，capacity 保留），acquire 后需 `resize` 到
//!   [`RECV_BUFFER_SIZE`] 再用于 `recv_from`。
//! - `PooledObject` 通过 `DerefMut` 暴露 `&mut Vec<u8>`，可直接作为 `&mut [u8]` 使用。

use std::sync::Arc;

use crate::utils::object_pool::{create_buffer_pool, ObjectPool, PooledObject};

/// 接收缓冲区大小（64KB，与 A2 改造一致）。
pub const RECV_BUFFER_SIZE: usize = 65536;

/// 爬虫接收缓冲区池
///
/// 多 socket 接收循环共享一个池。`capacity` 建议按 `socket_count * 2` 设置，
/// 确保并发接收时不缺缓冲区；池为空时会临时新建对象，不会阻塞。
#[derive(Clone)]
pub struct CrawlerBufferPool {
    pool: Arc<ObjectPool<Vec<u8>>>,
}

impl CrawlerBufferPool {
    /// 创建缓冲区池。
    ///
    /// `capacity` 为池容量，建议 `socket_count * 2`。
    pub fn new(capacity: usize) -> Self {
        Self {
            pool: Arc::new(create_buffer_pool(capacity, RECV_BUFFER_SIZE)),
        }
    }

    /// 获取一个缓冲区（RAII，drop 时自动归还）。
    ///
    /// 注意：归还时缓冲区已被 `clear()`（len=0），使用前需：
    /// ```ignore
    /// let mut buf = pool.acquire();
    /// buf.resize(RECV_BUFFER_SIZE, 0);
    /// let (n, addr) = socket.recv_from(&mut buf[..]).await?;
    /// ```
    pub fn acquire(&self) -> PooledObject<'_, Vec<u8>> {
        self.pool.acquire()
    }

    /// 池中当前空闲对象数。
    pub fn pooled_count(&self) -> usize {
        self.pool.pooled_count()
    }

    /// 池容量。
    pub fn capacity(&self) -> usize {
        self.pool.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crawler_buffer_pool_acquire_release() {
        let pool = CrawlerBufferPool::new(4);
        assert_eq!(pool.pooled_count(), 0);

        {
            let mut buf = pool.acquire();
            // 归还后被 clear，需要 resize 到 RECV_BUFFER_SIZE
            buf.resize(RECV_BUFFER_SIZE, 0);
            assert_eq!(buf.len(), RECV_BUFFER_SIZE);
            assert!(buf.capacity() >= RECV_BUFFER_SIZE);
            assert_eq!(pool.pooled_count(), 0);

            // 模拟写入部分数据
            buf[0] = 0x41;
        }
        // drop 后归还，缓冲区被 clear
        assert_eq!(pool.pooled_count(), 1);

        let buf = pool.acquire();
        assert_eq!(buf.len(), 0);
        assert!(buf.capacity() >= RECV_BUFFER_SIZE);
    }

    #[test]
    fn test_crawler_buffer_pool_clone_shares_pool() {
        let pool = CrawlerBufferPool::new(2);
        let pool2 = pool.clone();

        {
            let _buf = pool.acquire();
            assert_eq!(pool2.pooled_count(), 0);
        }
        // 归还后两个句柄看到的是同一个池
        assert_eq!(pool.pooled_count(), 1);
        assert_eq!(pool2.pooled_count(), 1);
    }
}
