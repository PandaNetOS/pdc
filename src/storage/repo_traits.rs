//! 数据层 Repository trait 定义
//!
//! 统一归口所有关键数据：Node、Peer、Infohash、Tracker。
//! 所有业务层只通过 trait 访问数据，不直接操作底层结构。

use std::net::SocketAddr;

use async_trait::async_trait;

use crate::dht::kbucket::{KBucketEntry, NodeState};
use crate::types::{Infohash, PeerInfo, PeerSource};

/// DHT 节点 ID
pub type NodeId = [u8; 20];

// ---------------------------------------------------------------------------
// NodeRepository — DHT 节点归口（替代 crawler 路由表直接访问）
// ---------------------------------------------------------------------------

#[async_trait]
pub trait NodeRepository: Send + Sync {
    // CRUD
    async fn add_node(&self, id: NodeId, addr: SocketAddr) -> bool;
    async fn remove_node(&self, addr: &SocketAddr) -> bool;
    async fn get_node(&self, addr: &SocketAddr) -> Option<KBucketEntry>;
    async fn all_nodes(&self) -> Vec<KBucketEntry>;
    async fn node_count(&self) -> usize;
    async fn is_empty(&self) -> bool;

    // 评分排序查询
    async fn top_nodes(&self, n: usize) -> Vec<KBucketEntry>;
    async fn closest_nodes(&self, target: &NodeId, n: usize) -> Vec<KBucketEntry>;

    // 同步便捷方法（爬虫/选择系统高频调用，避免 async 开销）
    fn top_nodes_sync(&self, n: usize) -> Vec<KBucketEntry>;
    fn len_sync(&self) -> usize;

    // 评分与统计（由智能层计算后写入）
    async fn update_score(&self, addr: &SocketAddr, score: f64);
    /// 批量更新评分（一次事务，避免逐个更新的锁竞争）
    async fn update_scores_batch(&self, scores: &[(SocketAddr, f64)]);
    async fn record_query(&self, addr: &SocketAddr, success: bool, latency_ms: u64);
    async fn set_node_state(&self, addr: &SocketAddr, state: NodeState);
    /// 刷新所有节点状态（基于最后活跃时间更新 Good/Questionable）
    async fn refresh_all_states(&self);
    /// 节点统计信息（避免全量克隆，用于健康度计算和监控）
    async fn stats(&self) -> crate::storage::node_repo::NodeStats;

    // 脏标记（用于增量评分：统计数据变化时标记，评分系统只重算脏节点）
    /// 标记节点为脏（统计数据已变化，需要重算评分）
    async fn mark_dirty(&self, addr: &SocketAddr);
    /// 获取所有脏节点地址
    async fn dirty_nodes(&self) -> Vec<SocketAddr>;
    /// 清除单个节点的脏标记
    async fn clear_dirty(&self, addr: &SocketAddr);
    /// 清除所有脏标记
    async fn clear_all_dirty(&self);

    // 路由表操作
    async fn bucket_count(&self) -> usize;
    async fn non_empty_bucket_targets(&self) -> Vec<NodeId>;
    async fn rescore_all(&self);

    // 持久化
    async fn save_all(&self) -> anyhow::Result<()>;
    async fn load_all(&self) -> anyhow::Result<usize>;
}

// ---------------------------------------------------------------------------
// PeerRepository — BT Peer 归口（合并 PeerCache + PEX池 + Probe队列 + SuperTracker）
// ---------------------------------------------------------------------------

#[async_trait]
pub trait PeerRepository: Send + Sync {
    // 按 infohash 分组的 CRUD
    async fn add_peer(&self, infohash: Infohash, peer: PeerInfo);
    async fn add_peers(&self, infohash: Infohash, peers: Vec<PeerInfo>);
    async fn get_peers(&self, infohash: &Infohash, limit: usize) -> Vec<PeerInfo>;
    async fn remove_peer(&self, infohash: &Infohash, addr: &SocketAddr);

    // 全局查询（跨 infohash 去重）
    async fn all_peers(&self) -> Vec<PeerInfo>;
    async fn peer_count(&self) -> usize;
    async fn infohash_count(&self) -> usize;
    async fn top_peers(&self, infohash: &Infohash, n: usize) -> Vec<PeerInfo>;

    // 评分与探测统计（由 ProbeService 和 PeerScorer 写入）
    async fn update_score(&self, addr: &SocketAddr, score: f64);
    async fn update_probe_stats(&self, addr: &SocketAddr, tcp_ok: bool, supports_dht: bool);
    async fn get_peer_global(&self, addr: &SocketAddr) -> Option<PeerInfo>;
    /// 获取该 peer 出现在多少个 infohash 下（多 infohash 共享维度）
    async fn get_peer_infohash_count(&self, addr: &SocketAddr) -> u32;

    // TTL 清理
    async fn cleanup_expired(&self, ttl_secs: u64);

    // 全量持久化
    async fn save_all(&self) -> anyhow::Result<()>;
    async fn load_all(&self) -> anyhow::Result<usize>;

    // 历史持久化
    async fn save_history(&self, infohash: Infohash, peer: &PeerInfo);
    async fn query_history(&self, infohash: &Infohash, limit: usize) -> Vec<PeerInfo>;
}

// ---------------------------------------------------------------------------
// InfohashRepository — Infohash 归口（合并 seen_infohashes + 引用计数）
// ---------------------------------------------------------------------------

#[async_trait]
pub trait InfohashRepository: Send + Sync {
    async fn register(&self, infohash: Infohash, source: &str);
    async fn unregister(&self, infohash: &Infohash);
    async fn ref_count(&self, infohash: &Infohash) -> u32;
    async fn all_infohashes(&self) -> Vec<Infohash>;
    async fn count(&self) -> usize;
    async fn cleanup_zero_ref(&self) -> usize;
}

// ---------------------------------------------------------------------------
// TrackerRepository — Tracker 归口（替代 TrackerDiscoverer 内部 HashMap）
// ---------------------------------------------------------------------------

/// Tracker 运行时状态
#[derive(Debug, Clone)]
pub struct TrackerEntry {
    pub url: String,
    pub score: f64,
    pub disabled: bool,
    pub total_requests: u64,
    pub success_requests: u64,
    pub failed_requests: u64,
    pub total_peers_discovered: u64,
    pub avg_response_time_ms: f64,
    pub consecutive_failures: u32,
    pub last_used: Option<u64>,
}

#[async_trait]
pub trait TrackerRepository: Send + Sync {
    // CRUD
    async fn add_tracker(&self, url: String);
    async fn remove_tracker(&self, url: &str);
    async fn get_tracker(&self, url: &str) -> Option<TrackerEntry>;
    async fn all_trackers(&self) -> Vec<TrackerEntry>;
    async fn active_trackers(&self) -> Vec<TrackerEntry>;
    async fn top_trackers(&self, n: usize) -> Vec<TrackerEntry>;
    async fn count(&self) -> usize;

    // 评分与统计
    async fn update_score(&self, url: &str, score: f64);
    async fn record_request(&self, url: &str, success: bool, peers: u64, latency_ms: u64);
    async fn set_disabled(&self, url: &str, disabled: bool);

    // 持久化
    async fn save_all(&self) -> anyhow::Result<()>;
    async fn load_all(&self) -> anyhow::Result<usize>;
}
