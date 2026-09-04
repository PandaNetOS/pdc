//! 事件总线
//!
//! 基于 tokio::sync::broadcast 的发布/订阅事件总线，是终极形态架构的解耦核心。
//!
//! 所有模块通过事件总线通信：
//! - 发现器发现 peer → 发布 PeerDiscovered
//! - 缓存层订阅 PeerDiscovered → 更新缓存
//! - 统计引擎订阅 PeerDiscovered / InfohashSeen → 更新统计
//! - 爬虫引擎发布 CrawlProgress → 控制面/API 订阅
//! - 超级 Tracker 收到 announce → 发布 AnnounceRequest
//! - 控制面发布 ConfigChanged → 所有模块订阅

use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::types::Event;

/// 事件总线
///
/// 包装 broadcast::Sender，提供发布和订阅接口。
/// 克隆成本低（内部是 Arc），可以安全地在各模块间共享。
#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<Event>,
}

impl EventBus {
    /// 创建新的事件总线
    ///
    /// # 参数
    /// - `capacity`: 广播通道容量，建议 1024-4096
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// 发布事件
    ///
    /// 如果没有订阅者，事件会被丢弃（不阻塞）。
    /// 如果通道满了，最旧的事件会被丢弃（broadcast 的 lag 行为）。
    pub fn publish(&self, event: Event) {
        match self.sender.send(event) {
            Ok(n) => debug!("[event_bus] 事件已发布，{} 个订阅者收到", n),
            Err(_) => debug!("[event_bus] 事件发布失败（无订阅者）"),
        }
    }

    /// 订阅事件
    ///
    /// 返回一个 broadcast::Receiver，可以循环 recv() 获取事件。
    /// 订阅者处理慢时会收到 RecvError::Lagged，表示有事件被丢弃。
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }

    /// 当前订阅者数量
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(1024)
    }
}

/// 启动一个事件消费者任务
///
/// 便捷函数：订阅事件总线，对每个事件调用 handler。
/// handler 返回 false 时停止消费。
///
/// # 示例
/// ```ignore
/// use PeerDiscoveryCenter::event_bus::{EventBus, spawn_consumer};
///
/// let bus = EventBus::default();
/// let bus_clone = bus.clone();
/// tokio::spawn(async move {
///     spawn_consumer(bus_clone, |event| {
///         println!("收到事件: {:?}", event);
///         true
///     }).await;
/// });
/// ```
pub async fn spawn_consumer<F>(bus: EventBus, mut handler: F)
where
    F: FnMut(Event) -> bool,
{
    let mut rx = bus.subscribe();
    loop {
        match rx.recv().await {
            Ok(event) => {
                if !handler(event) {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("[event_bus] 消费者滞后，丢弃了 {} 个事件", n);
            }
            Err(broadcast::error::RecvError::Closed) => {
                debug!("[event_bus] 事件总线已关闭，消费者退出");
                break;
            }
        }
    }
}

/// 便捷宏：创建一个过滤特定事件类型的消费者
///
/// # 示例
/// ```ignore
/// use PeerDiscoveryCenter::event_bus::{EventBus, spawn_filtered_consumer};
/// use PeerDiscoveryCenter::types::Event;
///
/// spawn_filtered_consumer(bus, |event| matches!(event, Event::PeerDiscovered { .. }), |event| {
///     if let Event::PeerDiscovered { infohash, peers, .. } = event {
///         println!("发现 {} 个 peer", peers.len());
///     }
///     true
/// }).await;
/// ```
pub async fn spawn_filtered_consumer<F, G>(bus: EventBus, filter: F, mut handler: G)
where
    F: Fn(&Event) -> bool,
    G: FnMut(Event) -> bool,
{
    let mut rx = bus.subscribe();
    loop {
        match rx.recv().await {
            Ok(event) => {
                if filter(&event) && !handler(event) {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("[event_bus] 消费者滞后，丢弃了 {} 个事件", n);
            }
            Err(broadcast::error::RecvError::Closed) => {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Infohash, PeerInfo, PeerSource};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn make_peer(port: u16) -> PeerInfo {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        PeerInfo::new(addr, PeerSource::Tracker)
    }

    #[tokio::test]
    async fn test_publish_subscribe() {
        let bus = EventBus::new(16);
        let mut rx = bus.subscribe();

        let infohash: Infohash = [0u8; 20];
        let peers = vec![make_peer(6881)];
        bus.publish(Event::PeerDiscovered {
            infohash,
            peers: peers.clone(),
            source: "test".to_string(),
        });

        let received = rx.recv().await.unwrap();
        match received {
            Event::PeerDiscovered {
                infohash: ih,
                peers: p,
                ..
            } => {
                assert_eq!(ih, infohash);
                assert_eq!(p.len(), 1);
            }
            _ => panic!(" unexpected event type"),
        }
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let bus = EventBus::new(16);
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();

        bus.publish(Event::ConfigChanged);

        let e1 = rx1.recv().await.unwrap();
        let e2 = rx2.recv().await.unwrap();
        assert!(matches!(e1, Event::ConfigChanged));
        assert!(matches!(e2, Event::ConfigChanged));
    }

    #[test]
    fn test_subscriber_count() {
        let bus = EventBus::new(16);
        assert_eq!(bus.subscriber_count(), 0);
        let _rx = bus.subscribe();
        assert_eq!(bus.subscriber_count(), 1);
    }
}
