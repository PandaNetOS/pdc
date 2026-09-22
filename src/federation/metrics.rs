//! 鑱旈偊缃戠粶鐩戞帶鎸囨爣
//!
//! 浣跨敤 AtomicU64 璁板綍鍚勭被璁℃暟鍣紝鎻愪緵蹇収鐢ㄤ簬搴忓垪鍖栧拰灞曠ず銆?

use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

/// 鑱旈偊缃戠粶鎸囨爣锛堝師瀛愯鏁板櫒锛?
#[derive(Debug, Default)]
pub struct FederationMetrics {
    /// 鍙戦€佹秷鎭€绘暟
    pub total_messages_sent: AtomicU64,
    /// 鎺ユ敹娑堟伅鎬绘暟
    pub total_messages_recv: AtomicU64,
    /// 鍙戦€佸瓧鑺傛€绘暟锛堝簭鍒楀寲鍚庣殑瀹屾暣甯у瓧鑺傛暟锛屽惈甯уご锛?
    pub bytes_sent: AtomicU64,
    /// 鎺ユ敹瀛楄妭鎬绘暟锛堝簭鍒楀寲鍚庣殑瀹屾暣甯у瓧鑺傛暟锛屽惈甯уご锛?
    pub bytes_recv: AtomicU64,
    /// Gossip 浼犳挱娆℃暟
    pub gossip_propagations: AtomicU64,
    /// Gossip 鎺ユ敹娆℃暟
    pub gossip_received: AtomicU64,
    /// 鍚屾鏉＄洰搴旂敤鏁?
    pub sync_entries_applied: AtomicU64,
    /// Node 鍚屾璁℃暟
    pub node_sync_count: AtomicU64,
    /// Peer 鍚屾璁℃暟
    pub peer_sync_count: AtomicU64,
    /// Infohash 鍚屾璁℃暟
    pub infohash_sync_count: AtomicU64,
    /// Tracker 鍚屾璁℃暟
    pub tracker_sync_count: AtomicU64,
    /// 鎵撴礊灏濊瘯娆℃暟
    pub hole_punch_attempts: AtomicU64,
    /// 鎵撴礊鎴愬姛娆℃暟
    pub hole_punch_successes: AtomicU64,
    /// 涓户杞彂瀛楄妭鏁?
    pub relay_bytes_forwarded: AtomicU64,
    /// 涓户閫氶亾琚帴鍙楁鏁?
    pub relay_channels_accepted: AtomicU64,
    /// 涓户閫氶亾琚嫆缁濇鏁帮紙杩囪浇锛?
    pub relay_channels_rejected: AtomicU64,
    /// 涓户閫氶亾鍏抽棴娆℃暟
    pub relay_channels_closed: AtomicU64,
    /// Merkle 淇娆℃暟
    pub merkle_repairs: AtomicU64,
    /// 绛惧悕楠岃瘉澶辫触娆℃暟
    pub signature_verification_failures: AtomicU64,
    /// 杩炴帴寤虹珛鎴愬姛娆℃暟
    pub connections_established: AtomicU64,
    /// 杩炴帴鏂紑娆℃暟
    pub connections_closed: AtomicU64,
    /// P3-C: how many anti-entropy rounds actually executed
    /// (TaskScheduler -> `GossipNetwork::anti_entropy_tick`).
    /// Before this counter existed there was no way to prove the periodic
    /// anti-entropy task was ever invoked.
    pub anti_entropy_ticks: AtomicU64,
    /// P3-C: how many MerkleDigest messages anti-entropy actually sent
    /// (one per due repo per tick).
    pub anti_entropy_digests_sent: AtomicU64,
    /// P3-C: ticks that returned early because no peer connection existed.
    pub anti_entropy_no_conn: AtomicU64,
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

    /// 绱姞鍙戦€佸瓧鑺傛暟锛堝簭鍒楀寲鍚庣殑瀹屾暣甯у瓧鑺傞暱搴︼級
    pub fn record_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// 绱姞鎺ユ敹瀛楄妭鏁帮紙搴忓垪鍖栧悗鐨勫畬鏁村抚瀛楄妭闀垮害锛?
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
        self.sync_entries_applied
            .fetch_add(count, Ordering::Relaxed);
    }

    /// 璁板綍 Node 鍚屾搴旂敤鏉℃暟
    pub fn record_node_sync(&self, count: u64) {
        self.node_sync_count.fetch_add(count, Ordering::Relaxed);
    }

    /// 璁板綍 Peer 鍚屾搴旂敤鏉℃暟
    pub fn record_peer_sync(&self, count: u64) {
        self.peer_sync_count.fetch_add(count, Ordering::Relaxed);
    }

    /// 璁板綍 Infohash 鍚屾搴旂敤鏉℃暟
    pub fn record_infohash_sync(&self, count: u64) {
        self.infohash_sync_count.fetch_add(count, Ordering::Relaxed);
    }

    /// 璁板綍 Tracker 鍚屾搴旂敤鏉℃暟
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
        self.relay_bytes_forwarded
            .fetch_add(bytes, Ordering::Relaxed);
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
        self.signature_verification_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_connection_established(&self) {
        self.connections_established.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_connection_closed(&self) {
        self.connections_closed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_anti_entropy_tick(&self) {
        self.anti_entropy_ticks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_anti_entropy_digest(&self) {
        self.anti_entropy_digests_sent
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_anti_entropy_no_conn(&self) {
        self.anti_entropy_no_conn.fetch_add(1, Ordering::Relaxed);
    }

    /// 鐢熸垚蹇収锛堝皢 AtomicU64 杞负鏅€氬瓧娈碉級
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
            signature_verification_failures: self
                .signature_verification_failures
                .load(Ordering::Relaxed),
            connections_established: self.connections_established.load(Ordering::Relaxed),
            connections_closed: self.connections_closed.load(Ordering::Relaxed),
            anti_entropy_ticks: self.anti_entropy_ticks.load(Ordering::Relaxed),
            anti_entropy_digests_sent: self.anti_entropy_digests_sent.load(Ordering::Relaxed),
            anti_entropy_no_conn: self.anti_entropy_no_conn.load(Ordering::Relaxed),
        }
    }
}

/// 鎸囨爣蹇収锛堝彲搴忓垪鍖栵級
#[derive(Debug, Clone, Serialize, Default)]
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
    pub anti_entropy_ticks: u64,
    pub anti_entropy_digests_sent: u64,
    pub anti_entropy_no_conn: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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
