//! 智能层 Scorer trait 定义
//!
//! 评分系统独立，纯计算无状态，输入=数据层快照，输出=评分写回数据层。

use async_trait::async_trait;

use crate::storage::repo_traits::{
    InfohashRepository, NodeRepository, PeerRepository, TrackerRepository,
};
use crate::types::PeerInfo;
use crate::dht::kbucket::KBucketEntry;

// ---------------------------------------------------------------------------
// NodeScorer — DHT 节点评分（4维度加权）
// ---------------------------------------------------------------------------

#[async_trait]
pub trait NodeScorer: Send + Sync {
    /// 全量重算所有节点评分（兜底用，平时用增量）
    async fn rescore_all(&self, repo: &dyn NodeRepository);
    /// 增量重算脏节点评分（只重算统计数据有变化的节点），返回重算数量
    async fn rescore_dirty(&self, repo: &dyn NodeRepository) -> usize;
    fn calculate(&self, node: &KBucketEntry) -> f64;
}

// ---------------------------------------------------------------------------
// TrackerScorer — Tracker 评分（4维度加权）
// ---------------------------------------------------------------------------

#[async_trait]
pub trait TrackerScorer: Send + Sync {
    async fn rescore_all(&self, repo: &dyn TrackerRepository);
    fn calculate(
        &self,
        total_requests: u64,
        success_requests: u64,
        total_peers: u64,
        avg_latency_ms: f64,
        consecutive_failures: u32,
        disabled: bool,
    ) -> f64;
}

// ---------------------------------------------------------------------------
// PeerScorer — BT Peer 评分（5维度加权）
// ---------------------------------------------------------------------------

#[async_trait]
pub trait PeerScorer: Send + Sync {
    async fn rescore_all(&self, repo: &dyn PeerRepository);
    fn calculate(&self, peer: &PeerInfo, infohash_count: u32) -> f64;
}

// ---------------------------------------------------------------------------
// HealthScorer — 系统健康度（三层加权）
// ---------------------------------------------------------------------------

pub struct HealthReport {
    pub overall: f64,
    pub tracker_layer: f64,
    pub dht_layer: f64,
    pub peer_layer: f64,
    pub active_trackers: usize,
    pub total_trackers: usize,
    pub avg_tracker_score: f64,
}

#[async_trait]
pub trait HealthScorer: Send + Sync {
    async fn calculate(
        &self,
        trackers: &dyn TrackerRepository,
        nodes: &dyn NodeRepository,
        peers: &dyn PeerRepository,
        infohashes: &dyn InfohashRepository,
    ) -> HealthReport;
}
