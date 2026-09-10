//! 联邦网络监控指标
//!
//! 使用 AtomicU64 记录各类计数器，提供快照用于序列化和展示。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::Serialize;

/// 联邦网络指标（原子计数器）
#[derive(Debug, Default)]
pub struct FederationMetrics {
    /// 发送消息总数
    pub total_messages_sent: AtomicU64,
    /// 接收消息总数
    pub total_messages_recv: AtomicU64,
    /// 发送字节总数（序列化后的完整帧字节数，含帧头）
    pub bytes_sent: AtomicU64,
    /// 接收字节总数（序列化后的完整帧字节数，含帧头）
    pub bytes_recv: AtomicU64,
    /// Gossip 传播次数
    pub gossip_propagations: AtomicU64,
    /// Gossip 接收次数
    pub gossip_received: AtomicU64,
    /// 同步条目应用数
    pub sync_entries_applied: AtomicU64,
    /// Node 同步计数
    pub node_sync_count: AtomicU64,
    /// Peer 同步计数
    pub peer_sync_count: AtomicU64,
    /// Infohash 同步计数
    pub infohash_sync_count: AtomicU64,
    /// Tracker 同步计数
    pub tracker_sync_count: AtomicU64,
    /// 打洞尝试次数
    pub hole_punch_attempts: AtomicU64,
    /// 打洞成功次数
    pub hole_punch_successes: AtomicU64,
    /// 中继转发字节数
    pub relay_bytes_forwarded: AtomicU64,
    /// 中继通道被接受次数
    pub relay_channels_accepted: AtomicU64,
    /// 中继通道被拒绝次数（过载）
    pub relay_channels_rejected: AtomicU64,
    /// 中继通道关闭次数
    pub relay_channels_closed: AtomicU64,
    /// Merkle 修复次数
    pub merkle_repairs: AtomicU64,
    /// 签名验证失败次数
    pub signature_verification_failures: AtomicU64,
    /// 连接建立成功次数
    pub connections_established: AtomicU64,
    /// 连接断开次数
    pub connections_closed: AtomicU64,
}

impl FederationMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_message_sent(&self) {
        self.total_messages_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_message_recv(&self) {
        self.total_messages_recv.fetch_add(1, Ordering::Relaxed);
    }

    /// 累加发送字节数（序列化后的完整帧字节长度）
    pub fn record_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// 累加接收字节数（序列化后的完整帧字节长度）
    pub fn record_bytes_recv(&self, bytes: u64) {
        self.bytes_recv.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_gossip_propagation(&self) {
        self.gossip_propagations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_gossip_received(&self) {
        self.gossip_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_sync_entries(&self, count: u64) {
        self.sync_entries_applied.fetch_add(count, Ordering::Relaxed);
    }

    /// 记录 Node 同步应用条数
    pub fn record_node_sync(&self, count: u64) {
        self.node_sync_count.fetch_add(count, Ordering::Relaxed);
    }

    /// 记录 Peer 同步应用条数
    pub fn record_peer_sync(&self, count: u64) {
        self.peer_sync_count.fetch_add(count, Ordering::Relaxed);
    }

    /// 记录 Infohash 同步应用条数
    pub fn record_infohash_sync(&self, count: u64) {
        self.infohash_sync_count.fetch_add(count, Ordering::Relaxed);
    }

    /// 记录 Tracker 同步应用条数
    pub fn record_tracker_sync(&self, count: u64) {
        self.tracker_sync_count.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_hole_punch_attempt(&self) {
        self.hole_punch_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_hole_punch_success(&self) {
        self.hole_punch_successes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_relay_bytes(&self, bytes: u64) {
        self.relay_bytes_forwarded.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_relay_channel_accepted(&self) {
        self.relay_channels_accepted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_relay_channel_rejected(&self) {
        self.relay_channels_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_relay_channel_closed(&self) {
        self.relay_channels_closed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_merkle_repair(&self) {
        self.merkle_repairs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_signature_failure(&self) {
        self.signature_verification_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_connection_established(&self) {
        self.connections_established.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_connection_closed(&self) {
        self.connections_closed.fetch_add(1, Ordering::Relaxed);
    }

    /// 生成快照（将 AtomicU64 转为普通字段）
    pub fn snapshot(&self) -> FederationMetricsSnapshot {
        FederationMetricsSnapshot {
            total_messages_sent: self.total_messages_sent.load(Ordering::Relaxed),
            total_messages_recv: self.total_messages_recv.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_recv: self.bytes_recv.load(Ordering::Relaxed),
            gossip_propagations: self.gossip_propagations.load(Ordering::Relaxed),
            gossip_received: self.gossip_received.load(Ordering::Relaxed),
            sync_entries_applied: self.sync_entries_applied.load(Ordering::Relaxed),
            node_sync_count: self.node_sync_count.load(Ordering::Relaxed),
            peer_sync_count: self.peer_sync_count.load(Ordering::Relaxed),
            infohash_sync_count: self.infohash_sync_count.load(Ordering::Relaxed),
            tracker_sync_count: self.tracker_sync_count.load(Ordering::Relaxed),
            hole_punch_attempts: self.hole_punch_attempts.load(Ordering::Relaxed),
            hole_punch_successes: self.hole_punch_successes.load(Ordering::Relaxed),
            relay_bytes_forwarded: self.relay_bytes_forwarded.load(Ordering::Relaxed),
            relay_channels_accepted: self.relay_channels_accepted.load(Ordering::Relaxed),
            relay_channels_rejected: self.relay_channels_rejected.load(Ordering::Relaxed),
            relay_channels_closed: self.relay_channels_closed.load(Ordering::Relaxed),
            merkle_repairs: self.merkle_repairs.load(Ordering::Relaxed),
            signature_verification_failures: self.signature_verification_failures.load(Ordering::Relaxed),
            connections_established: self.connections_established.load(Ordering::Relaxed),
            connections_closed: self.connections_closed.load(Ordering::Relaxed),
        }
    }
}

/// 指标快照（可序列化）
#[derive(Debug, Clone, Serialize)]
pub struct FederationMetricsSnapshot {
    pub total_messages_sent: u64,
    pub total_messages_recv: u64,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
    pub gossip_propagations: u64,
    pub gossip_received: u64,
    pub sync_entries_applied: u64,
    pub node_sync_count: u64,
    pub peer_sync_count: u64,
    pub infohash_sync_count: u64,
    pub tracker_sync_count: u64,
    pub hole_punch_attempts: u64,
    pub hole_punch_successes: u64,
    pub relay_bytes_forwarded: u64,
    pub relay_channels_accepted: u64,
    pub relay_channels_rejected: u64,
    pub relay_channels_closed: u64,
    pub merkle_repairs: u64,
    pub signature_verification_failures: u64,
    pub connections_established: u64,
    pub connections_closed: u64,
}

impl Default for FederationMetricsSnapshot {
    fn default() -> Self {
        Self {
            total_messages_sent: 0,
            total_messages_recv: 0,
            bytes_sent: 0,
            bytes_recv: 0,
            gossip_propagations: 0,
            gossip_received: 0,
            sync_entries_applied: 0,
            node_sync_count: 0,
            peer_sync_count: 0,
            infohash_sync_count: 0,
            tracker_sync_count: 0,
            hole_punch_attempts: 0,
            hole_punch_successes: 0,
            relay_bytes_forwarded: 0,
            relay_channels_accepted: 0,
            relay_channels_rejected: 0,
            relay_channels_closed: 0,
            merkle_repairs: 0,
            signature_verification_failures: 0,
            connections_established: 0,
            connections_closed: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_counters() {
        let m = FederationMetrics::new();
        assert_eq!(m.snapshot().total_messages_sent, 0);

        m.record_message_sent();
        m.record_message_sent();
        m.record_message_recv();
        assert_eq!(m.snapshot().total_messages_sent, 2);
        assert_eq!(m.snapshot().total_messages_recv, 1);
    }

    #[test]
    fn test_metrics_all_counters() {
        let m = FederationMetrics::new();
        m.record_gossip_propagation();
        m.record_gossip_received();
        m.record_sync_entries(5);
        m.record_hole_punch_attempt();
        m.record_hole_punch_success();
        m.record_relay_bytes(1024);
        m.record_relay_channel_accepted();
        m.record_relay_channel_rejected();
        m.record_relay_channel_closed();
        m.record_merkle_repair();
        m.record_signature_failure();
        m.record_connection_established();
        m.record_connection_closed();

        let s = m.snapshot();
        assert_eq!(s.gossip_propagations, 1);
        assert_eq!(s.gossip_received, 1);
        assert_eq!(s.sync_entries_applied, 5);
        assert_eq!(s.hole_punch_attempts, 1);
        assert_eq!(s.hole_punch_successes, 1);
        assert_eq!(s.relay_bytes_forwarded, 1024);
        assert_eq!(s.relay_channels_accepted, 1);
        assert_eq!(s.relay_channels_rejected, 1);
        assert_eq!(s.relay_channels_closed, 1);
        assert_eq!(s.merkle_repairs, 1);
        assert_eq!(s.signature_verification_failures, 1);
        assert_eq!(s.connections_established, 1);
        assert_eq!(s.connections_closed, 1);
    }

    #[test]
    fn test_metrics_snapshot_serialize() {
        let m = FederationMetrics::new();
        m.record_message_sent();
        let s = m.snapshot();
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("\"total_messages_sent\":1"));
    }

    #[test]
    fn test_metrics_arc_share() {
        let m = Arc::new(FederationMetrics::new());
        let m2 = m.clone();
        m.record_message_sent();
        m2.record_message_recv();
        assert_eq!(m.snapshot().total_messages_sent, 1);
        assert_eq!(m2.snapshot().total_messages_recv, 1);
    }
}
