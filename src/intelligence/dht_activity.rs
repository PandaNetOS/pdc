//! DHT 活跃度统计模块
//!
//! 统计每个 infohash 在 DHT 网络中的活跃度：
//! - get_peers 查询频率（次/小时）
//! - announce_peer 上报频率（次/小时）
//!
//! 使用滑动时间窗口，保留最近1小时的数据。
//! 这是 InfohashScorer 流行度维度的关键数据源（高覆盖率）。

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;

use crate::types::Infohash;

/// 单个活跃度事件
#[derive(Debug, Clone)]
struct ActivityEvent {
    timestamp: Instant,
    event_type: ActivityEventType,
}

/// 活跃度事件类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityEventType {
    /// DHT get_peers 查询
    GetPeersQuery,
    /// DHT announce_peer 上报
    AnnouncePeer,
}

/// 单个 infohash 的活跃度统计
#[derive(Debug)]
struct InfohashActivity {
    /// 事件队列（滑动窗口，保留最近1小时）
    events: RwLock<VecDeque<ActivityEvent>>,
    /// 最后一次活动时间
    last_active: RwLock<Instant>,
}

impl InfohashActivity {
    fn new() -> Self {
        Self {
            events: RwLock::new(VecDeque::new()),
            last_active: RwLock::new(Instant::now()),
        }
    }

    /// 记录一个事件
    fn record(&self, event_type: ActivityEventType) {
        let now = Instant::now();
        let mut events = self.events.write();
        events.push_back(ActivityEvent {
            timestamp: now,
            event_type,
        });
        // 清理超过1小时的旧事件
        let cutoff = now - Duration::from_secs(3600);
        while events.front().map_or(false, |e| e.timestamp < cutoff) {
            events.pop_front();
        }
        *self.last_active.write() = now;
    }

    /// 获取最近1小时内的事件数
    fn count_recent(&self, event_type: Option<ActivityEventType>) -> u32 {
        let now = Instant::now();
        let cutoff = now - Duration::from_secs(3600);
        let events = self.events.read();
        events
            .iter()
            .filter(|e| e.timestamp >= cutoff)
            .filter(|e| event_type.map_or(true, |t| e.event_type == t))
            .count() as u32
    }

    /// 获取最后活动时间距现在的秒数
    fn last_active_secs_ago(&self) -> u64 {
        self.last_active.read().elapsed().as_secs()
    }
}

/// DHT 活跃度统计器
///
/// 统计每个 infohash 在 DHT 网络中的查询和上报频率。
/// 由 CrawlerEngine 在收到 get_peers/announce_peer 时调用 record()。
pub struct DhtActivityTracker {
    /// infohash -> 活跃度统计
    activities: DashMap<Infohash, Arc<InfohashActivity>>,
    /// 全局事件计数（用于监控）
    total_events: RwLock<u64>,
    /// 滑动窗口大小（秒，默认3600=1小时）
    window_secs: u64,
}

impl DhtActivityTracker {
    /// 创建新的活跃度统计器
    pub fn new() -> Self {
        Self {
            activities: DashMap::new(),
            total_events: RwLock::new(0),
            window_secs: 3600,
        }
    }

    /// 创建指定窗口大小的活跃度统计器
    pub fn with_window(window_secs: u64) -> Self {
        Self {
            activities: DashMap::new(),
            total_events: RwLock::new(0),
            window_secs,
        }
    }

    /// 记录一个 DHT 活跃度事件
    ///
    /// 由 CrawlerEngine 在收到 get_peers/announce_peer 时调用
    pub fn record(&self, infohash: Infohash, event_type: ActivityEventType) {
        let activity = self
            .activities
            .entry(infohash)
            .or_insert_with(|| Arc::new(InfohashActivity::new()));
        activity.record(event_type);
        *self.total_events.write() += 1;
    }

    /// 记录 get_peers 查询
    pub fn record_get_peers(&self, infohash: Infohash) {
        self.record(infohash, ActivityEventType::GetPeersQuery);
    }

    /// 记录 announce_peer 上报
    pub fn record_announce_peer(&self, infohash: Infohash) {
        self.record(infohash, ActivityEventType::AnnouncePeer);
    }

    /// 获取某个 infohash 最近1小时的 get_peers 查询频率（次/小时）
    pub fn get_peers_rate(&self, infohash: &Infohash) -> u32 {
        self.activities
            .get(infohash)
            .map(|a| a.count_recent(Some(ActivityEventType::GetPeersQuery)))
            .unwrap_or(0)
    }

    /// 获取某个 infohash 最近1小时的 announce_peer 上报频率（次/小时）
    pub fn announce_peer_rate(&self, infohash: &Infohash) -> u32 {
        self.activities
            .get(infohash)
            .map(|a| a.count_recent(Some(ActivityEventType::AnnouncePeer)))
            .unwrap_or(0)
    }

    /// 获取某个 infohash 最近1小时的总活跃度（次/小时）
    pub fn total_activity_rate(&self, infohash: &Infohash) -> u32 {
        self.activities
            .get(infohash)
            .map(|a| a.count_recent(None))
            .unwrap_or(0)
    }

    /// 获取某个 infohash 最后活动时间距现在的秒数
    pub fn last_active_secs_ago(&self, infohash: &Infohash) -> Option<u64> {
        self.activities
            .get(infohash)
            .map(|a| a.last_active_secs_ago())
    }

    /// 获取活跃的 infohash 数量（最近1小时有活动）
    pub fn active_infohash_count(&self) -> usize {
        let cutoff = Instant::now() - Duration::from_secs(self.window_secs);
        self.activities
            .iter()
            .filter(|a| *a.last_active.read() >= cutoff)
            .count()
    }

    /// 获取全局事件总数
    pub fn total_events(&self) -> u64 {
        *self.total_events.read()
    }

    /// 清理长时间无活动的 infohash（超过24小时无活动则移除）
    pub fn cleanup_expired(&self) -> usize {
        let cutoff = Instant::now() - Duration::from_secs(86400); // 24小时
        let mut removed = 0;
        self.activities.retain(|_, v| {
            if *v.last_active.read() < cutoff {
                removed += 1;
                false
            } else {
                true
            }
        });
        removed
    }

    /// 获取最活跃的 N 个 infohash（按最近1小时总活跃度排序）
    pub fn top_active(&self, n: usize) -> Vec<(Infohash, u32)> {
        let mut entries: Vec<(Infohash, u32)> = self
            .activities
            .iter()
            .map(|entry| (*entry.key(), entry.count_recent(None)))
            .filter(|(_, count)| *count > 0)
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(n);
        entries
    }
}

impl Default for DhtActivityTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_query() {
        let tracker = DhtActivityTracker::new();
        let infohash = [1u8; 20];

        assert_eq!(tracker.get_peers_rate(&infohash), 0);
        assert_eq!(tracker.announce_peer_rate(&infohash), 0);

        tracker.record_get_peers(infohash);
        tracker.record_get_peers(infohash);
        tracker.record_announce_peer(infohash);

        assert_eq!(tracker.get_peers_rate(&infohash), 2);
        assert_eq!(tracker.announce_peer_rate(&infohash), 1);
        assert_eq!(tracker.total_activity_rate(&infohash), 3);
    }

    #[test]
    fn test_total_events() {
        let tracker = DhtActivityTracker::new();
        let infohash = [2u8; 20];

        tracker.record_get_peers(infohash);
        tracker.record_announce_peer(infohash);

        assert_eq!(tracker.total_events(), 2);
    }

    #[test]
    fn test_active_infohash_count() {
        let tracker = DhtActivityTracker::new();
        let ih1 = [1u8; 20];
        let ih2 = [2u8; 20];

        tracker.record_get_peers(ih1);
        tracker.record_announce_peer(ih2);

        assert_eq!(tracker.active_infohash_count(), 2);
    }

    #[test]
    fn test_top_active() {
        let tracker = DhtActivityTracker::new();
        let ih1 = [1u8; 20];
        let ih2 = [2u8; 20];
        let ih3 = [3u8; 20];

        for _ in 0..10 {
            tracker.record_get_peers(ih1);
        }
        for _ in 0..5 {
            tracker.record_get_peers(ih2);
        }
        // ih3 无活动

        let top = tracker.top_active(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, ih1);
        assert_eq!(top[0].1, 10);
        assert_eq!(top[1].0, ih2);
        assert_eq!(top[1].1, 5);
    }

    #[test]
    fn test_last_active() {
        let tracker = DhtActivityTracker::new();
        let infohash = [4u8; 20];

        assert!(tracker.last_active_secs_ago(&infohash).is_none());

        tracker.record_get_peers(infohash);
        assert!(tracker.last_active_secs_ago(&infohash).is_some());
        assert!(tracker.last_active_secs_ago(&infohash).unwrap() < 5);
    }
}
