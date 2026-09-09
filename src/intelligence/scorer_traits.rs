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
// InfohashScorer — Infohash 热门度评分（分层评估：流行度+健康度）
// ---------------------------------------------------------------------------

/// Infohash 评分输入数据（由评分系统从多源聚合）
///
/// 【设计原则】多源融合 + 数据完整性感知
/// - 每个维度都有对应的数据来源标记（Option），无数据时给中性分而非0分
/// - 流行度和健康度分开评估，避免"热门但不健康"的误判
/// - 数据完整性低的 infohash 评分上限受限，避免冷启动误判
#[derive(Debug, Clone, Default)]
pub struct InfohashScoreInput {
    // ===== 流行度维度（Popularity）=====
    /// 唯一 peer 数量（去重后的不同 IP:Port 数）— 来源：PeerRepo（高覆盖率）
    pub unique_peers: u32,
    /// DHT get_peers 查询频率（次/小时）— 来源：DHT爬虫监听（高覆盖率）
    pub dht_query_rate: u32,
    /// 超级 Tracker announce 频率（次/5分钟）— 来源：超级Tracker（低覆盖率，有则用）
    pub announce_rate_5m: Option<u32>,
    /// 来源多样性（从多少个不同来源发现）— 来源：PeerRepo（高覆盖率）
    pub source_count: u32,
    /// 最近10分钟 peer 增长率（-1.0 ~ 1.0）— 来源：peer历史快照
    pub peer_growth_rate: f64,

    // ===== 健康度维度（Health）=====
    /// 做种者数量（已下载完成的 peer）— 来源：超级Tracker（低覆盖率）或外部Scrape
    pub seeders: Option<u32>,
    /// 下载者数量 — 来源：超级Tracker（低覆盖率）或外部Scrape
    pub leechers: Option<u32>,
    /// 外部 Tracker scrape 的做种者数（多源融合）— 来源：外部Tracker主动Scrape
    pub external_seeders: Option<u32>,
    /// 外部 Tracker scrape 的下载者数
    pub external_leechers: Option<u32>,
    /// 持续时长（秒，从首次发现到现在）— 来源：infohashes表（高覆盖率）
    pub longevity_secs: u64,
    /// 可用性代理（0.0~1.0，peer>10且有做种者则高）— 来源：综合计算
    pub availability_proxy: f64,

    // ===== 元数据（P3 扩展）=====
    /// 是否有 metadata（文件名称、大小等）
    pub has_metadata: bool,
    /// 文件总大小（字节）
    pub total_size: u64,
}

impl InfohashScoreInput {
    /// 计算数据完整性分数（0.0~1.0）
    ///
    /// 有多少个维度有有效数据，用于数据完整性感知调整
    pub fn data_completeness(&self) -> f64 {
        let mut valid = 0u32;
        let total = 10u32;

        if self.unique_peers > 0 { valid += 1; }
        if self.dht_query_rate > 0 { valid += 1; }
        if self.announce_rate_5m.is_some() { valid += 1; }
        if self.source_count > 0 { valid += 1; }
        if self.peer_growth_rate != 0.0 { valid += 1; }
        if self.seeders.is_some() { valid += 1; }
        if self.leechers.is_some() { valid += 1; }
        if self.external_seeders.is_some() { valid += 1; }
        if self.longevity_secs > 0 { valid += 1; }
        if self.has_metadata { valid += 1; }

        valid as f64 / total as f64
    }

    /// 获取融合后的做种者数（超级Tracker + 外部Scrape取最大）
    pub fn fused_seeders(&self) -> u32 {
        let mut max = 0u32;
        if let Some(s) = self.seeders { max = max.max(s); }
        if let Some(s) = self.external_seeders { max = max.max(s); }
        max
    }

    /// 获取融合后的下载者数
    pub fn fused_leechers(&self) -> u32 {
        let mut max = 0u32;
        if let Some(l) = self.leechers { max = max.max(l); }
        if let Some(l) = self.external_leechers { max = max.max(l); }
        max
    }

    /// 获取融合后的总 peer 数
    pub fn fused_total_peers(&self) -> u32 {
        self.fused_seeders().saturating_add(self.fused_leechers())
    }

    /// 做种者比例（0.0~1.0），无数据时返回中性值 0.5
    pub fn seeder_ratio(&self) -> f64 {
        let total = self.fused_total_peers();
        if total == 0 {
            0.5 // 无数据时给中性分
        } else {
            self.fused_seeders() as f64 / total as f64
        }
    }
}

#[async_trait]
pub trait InfohashScorer: Send + Sync {
    /// 全量重算所有 infohash 评分（兜底用）
    async fn rescore_all(&self, repo: &dyn InfohashRepository);
    /// 计算单个 infohash 的热门度评分（0-100）
    fn calculate(&self, input: &InfohashScoreInput) -> f64;
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
