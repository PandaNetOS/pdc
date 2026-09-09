//! Peer 历史快照模块
//!
//! 定期快照每个 infohash 的 peer 数量，用于计算增长率（growth_rate）。
//! 这是 InfohashScorer 流行度维度的关键数据源。
//!
//! 【设计】
//! - 保留最近30分钟的快照，每1分钟一个快照点
//! - 增长率 = (当前peer数 - 10分钟前peer数) / 10分钟前peer数
//! - 增长率范围：-1.0 ~ 1.0（超过1.0截断为1.0）

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;

use crate::types::Infohash;

/// 单个时间点的快照
#[derive(Debug, Clone)]
struct Snapshot {
    timestamp: Instant,
    peer_count: u32,
}

/// 单个 infohash 的历史快照
#[derive(Debug)]
struct InfohashHistory {
    /// 快照队列（保留最近30分钟）
    snapshots: RwLock<VecDeque<Snapshot>>,
    /// 最后一次快照时间
    last_snapshot: RwLock<Instant>,
}

impl InfohashHistory {
    fn new() -> Self {
        Self {
            snapshots: RwLock::new(VecDeque::new()),
            last_snapshot: RwLock::new(Instant::now()),
        }
    }

    /// 记录一个快照
    fn record(&self, peer_count: u32) {
        let now = Instant::now();
        let mut snapshots = self.snapshots.write();
        snapshots.push_back(Snapshot {
            timestamp: now,
            peer_count,
        });
        // 清理超过30分钟的旧快照
        let cutoff = now - Duration::from_secs(1800);
        while snapshots.front().map_or(false, |s| s.timestamp < cutoff) {
            snapshots.pop_front();
        }
        *self.last_snapshot.write() = now;
    }

    /// 获取指定时间前的 peer 数（找不到则返回最早的快照）
    fn peer_count_at(&self, secs_ago: u64) -> Option<u32> {
        let target = Instant::now() - Duration::from_secs(secs_ago);
        let snapshots = self.snapshots.read();
        if snapshots.is_empty() {
            return None;
        }
        // 找到最接近目标时间的快照
        let mut closest = None;
        let mut min_diff = Duration::from_secs(u64::MAX);
        for s in snapshots.iter() {
            let diff = if s.timestamp > target {
                s.timestamp - target
            } else {
                target - s.timestamp
            };
            if diff < min_diff {
                min_diff = diff;
                closest = Some(s.peer_count);
            }
        }
        closest
    }

    /// 获取当前 peer 数（最后一个快照）
    fn current_peer_count(&self) -> Option<u32> {
        self.snapshots.read().back().map(|s| s.peer_count)
    }
}

/// Peer 历史快照管理器
///
/// 定期快照每个 infohash 的 peer 数量，计算增长率。
/// 由 ScoreMaintainer 或独立定时任务调用 record_snapshot()。
pub struct PeerHistoryManager {
    /// infohash -> 历史快照
    histories: DashMap<Infohash, Arc<InfohashHistory>>,
    /// 增长率计算的回溯时间（秒，默认600=10分钟）
    growth_lookback_secs: u64,
    /// 快照保留时间（秒，默认1800=30分钟）
    retention_secs: u64,
}

impl PeerHistoryManager {
    /// 创建新的历史快照管理器
    pub fn new() -> Self {
        Self {
            histories: DashMap::new(),
            growth_lookback_secs: 600, // 10分钟
            retention_secs: 1800,      // 30分钟
        }
    }

    /// 创建自定义参数的历史快照管理器
    pub fn with_params(growth_lookback_secs: u64, retention_secs: u64) -> Self {
        Self {
            histories: DashMap::new(),
            growth_lookback_secs,
            retention_secs,
        }
    }

    /// 记录一个快照
    ///
    /// 由定时任务调用，传入当前每个 infohash 的 peer 数
    pub fn record_snapshot(&self, infohash: Infohash, peer_count: u32) {
        let history = self
            .histories
            .entry(infohash)
            .or_insert_with(|| Arc::new(InfohashHistory::new()));
        history.record(peer_count);
    }

    /// 批量记录快照
    pub fn record_snapshots(&self, snapshots: &[(Infohash, u32)]) {
        for (infohash, peer_count) in snapshots {
            self.record_snapshot(*infohash, *peer_count);
        }
    }

    /// 计算某个 infohash 的增长率（-1.0 ~ 1.0）
    ///
    /// 增长率 = (当前peer数 - lookback前peer数) / lookback前peer数
    /// 如果历史数据不足，返回 0.0（中性）
    pub fn growth_rate(&self, infohash: &Infohash) -> f64 {
        let history = match self.histories.get(infohash) {
            Some(h) => h,
            None => return 0.0,
        };

        let current = match history.current_peer_count() {
            Some(c) => c as f64,
            None => return 0.0,
        };

        let past = match history.peer_count_at(self.growth_lookback_secs) {
            Some(p) => p as f64,
            None => return 0.0, // 历史数据不足
        };

        if past <= 0.0 {
            // 之前没有peer，现在有peer → 最大正增长
            return if current > 0.0 { 1.0 } else { 0.0 };
        }

        let rate = (current - past) / past;
        rate.max(-1.0).min(1.0)
    }

    /// 获取当前 peer 数
    pub fn current_peer_count(&self, infohash: &Infohash) -> Option<u32> {
        self.histories
            .get(infohash)
            .and_then(|h| h.current_peer_count())
    }

    /// 获取有历史数据的 infohash 数量
    pub fn tracked_infohash_count(&self) -> usize {
        self.histories.len()
    }

    /// 清理长时间无快照的 infohash（超过保留时间则移除）
    pub fn cleanup_expired(&self) -> usize {
        let cutoff = Instant::now() - Duration::from_secs(self.retention_secs);
        let mut removed = 0;
        self.histories.retain(|_, v| {
            if *v.last_snapshot.read() < cutoff {
                removed += 1;
                false
            } else {
                true
            }
        });
        removed
    }

    /// 获取增长最快的 N 个 infohash
    pub fn top_growing(&self, n: usize) -> Vec<(Infohash, f64)> {
        let mut entries: Vec<(Infohash, f64)> = self
            .histories
            .iter()
            .map(|entry| (*entry.key(), self.growth_rate(entry.key())))
            .filter(|(_, rate)| *rate > 0.0)
            .collect();
        entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        entries.truncate(n);
        entries
    }
}

impl Default for PeerHistoryManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_current() {
        let manager = PeerHistoryManager::new();
        let infohash = [1u8; 20];

        assert_eq!(manager.current_peer_count(&infohash), None);

        manager.record_snapshot(infohash, 10);
        assert_eq!(manager.current_peer_count(&infohash), Some(10));

        manager.record_snapshot(infohash, 20);
        assert_eq!(manager.current_peer_count(&infohash), Some(20));
    }

    #[test]
    fn test_growth_rate_no_history() {
        let manager = PeerHistoryManager::new();
        let infohash = [2u8; 20];

        // 无历史数据时返回中性 0.0
        assert_eq!(manager.growth_rate(&infohash), 0.0);
    }

    #[test]
    fn test_tracked_count() {
        let manager = PeerHistoryManager::new();
        let ih1 = [1u8; 20];
        let ih2 = [2u8; 20];

        manager.record_snapshot(ih1, 10);
        manager.record_snapshot(ih2, 20);

        assert_eq!(manager.tracked_infohash_count(), 2);
    }

    #[test]
    fn test_batch_record() {
        let manager = PeerHistoryManager::new();
        let snapshots = [([1u8; 20], 10), ([2u8; 20], 20), ([3u8; 20], 30)];

        manager.record_snapshots(&snapshots);

        assert_eq!(manager.tracked_infohash_count(), 3);
        assert_eq!(manager.current_peer_count(&[1u8; 20]), Some(10));
        assert_eq!(manager.current_peer_count(&[2u8; 20]), Some(20));
        assert_eq!(manager.current_peer_count(&[3u8; 20]), Some(30));
    }

    #[test]
    fn test_growth_from_zero() {
        let manager = PeerHistoryManager::new();
        let infohash = [5u8; 20];

        // 模拟：之前0个peer，现在10个peer
        // 由于快照时间都是现在，增长率会返回0.0（历史不足）
        // 这个测试主要验证不会panic
        manager.record_snapshot(infohash, 0);
        manager.record_snapshot(infohash, 10);

        let rate = manager.growth_rate(&infohash);
        assert!(rate >= -1.0 && rate <= 1.0);
    }
}
