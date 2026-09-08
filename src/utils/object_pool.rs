//! 对象池模块
//!
//! P2 优化：频繁分配对象的内存复用，减少 allocator 压力。
//! 适用于 UDP 缓冲区、DHT 消息序列化缓冲区等频繁创建销毁的大对象。
//!
//! 使用 crossbeam::ArrayQueue 实现无锁对象池。

use std::sync::Arc;

use crossbeam::queue::ArrayQueue;

/// 通用对象池
///
/// 从池中获取对象，使用完后归还。
/// 对象在归还时会被重置（clear），避免脏数据。
pub struct ObjectPool<T> {
    pool: Arc<ArrayQueue<T>>,
    /// 创建新对象的工厂函数
    factory: Box<dyn Fn() -> T + Send + Sync>,
    /// 重置对象的函数（归还时调用）
    reset: Box<dyn Fn(&mut T) + Send + Sync>,
    /// 池容量
    capacity: usize,
}

impl<T> ObjectPool<T> {
    /// 创建对象池
    pub fn new<F, R>(capacity: usize, factory: F, reset: R) -> Self
    where
        F: Fn() -> T + Send + Sync + 'static,
        R: Fn(&mut T) + Send + Sync + 'static,
    {
        Self {
            pool: Arc::new(ArrayQueue::new(capacity)),
            factory: Box::new(factory),
            reset: Box::new(reset),
            capacity,
        }
    }

    /// 从池中获取对象
    /// 如果池为空，创建新对象
    pub fn acquire(&self) -> PooledObject<T> {
        let obj = self.pool.pop().unwrap_or_else(|| (self.factory)());
        PooledObject {
            obj: Some(obj),
            pool: self.pool.clone(),
            reset: &self.reset,
        }
    }

    /// 当前池中的对象数
    pub fn pooled_count(&self) -> usize {
        self.pool.len()
    }

    /// 池容量
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// 池化对象（RAII，drop 时自动归还）
pub struct PooledObject<'a, T> {
    obj: Option<T>,
    pool: Arc<ArrayQueue<T>>,
    reset: &'a (dyn Fn(&mut T) + Send + Sync),
}

impl<'a, T> std::ops::Deref for PooledObject<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.obj.as_ref().unwrap()
    }
}

impl<'a, T> std::ops::DerefMut for PooledObject<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.obj.as_mut().unwrap()
    }
}

impl<'a, T> Drop for PooledObject<'a, T> {
    fn drop(&mut self) {
        if let Some(mut obj) = self.obj.take() {
            (self.reset)(&mut obj);
            // 池满时丢弃对象（不阻塞）
            let _ = self.pool.push(obj);
        }
    }
}

/// 创建字节缓冲区对象池（最常用）
///
/// 缓冲区大小固定，归还时 clear。
pub fn create_buffer_pool(capacity: usize, buffer_size: usize) -> ObjectPool<Vec<u8>> {
    ObjectPool::new(
        capacity,
        move || Vec::with_capacity(buffer_size),
        |buf| buf.clear(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool() {
        let pool = create_buffer_pool(4, 1024);
        assert_eq!(pool.pooled_count(), 0);

        {
            let mut buf = pool.acquire();
            buf.extend_from_slice(b"hello");
            assert_eq!(buf.len(), 5);
            assert_eq!(pool.pooled_count(), 0);
        }
        // drop 后归还
        assert_eq!(pool.pooled_count(), 1);

        // 再次获取应该是同一个对象（已 reset）
        let buf = pool.acquire();
        assert_eq!(buf.len(), 0);
        assert!(buf.capacity() >= 1024);
    }

    #[test]
    fn test_pool_overflow() {
        let pool = create_buffer_pool(1, 64);
        {
            let _buf1 = pool.acquire();
            let _buf2 = pool.acquire();
        }
        // 池容量 1，第二个对象 drop 时会被丢弃
        assert_eq!(pool.pooled_count(), 1);
    }
}
