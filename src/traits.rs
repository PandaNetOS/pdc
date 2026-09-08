//! 统一 trait 定义
//!
//! 所有 peer 发现机制（Tracker、DHT、PEX、LPD、WebSeed）都实现 `PeerDiscoverer` trait，
//! 聚合器和插件注册表可以统一调度，屏蔽底层差异。

use std::time::SystemTime;

use async_trait::async_trait;

use crate::types::{Infohash, PeerInfo};

/// Peer 发现器统一接口（插件接口）
///
/// 所有发现器必须实现此 trait，通过 DiscovererRegistry 注册后由聚合器统一调度。
#[async_trait]
pub trait PeerDiscoverer: Send + Sync {
    /// 发现器名称（用于日志和统计，唯一标识）
    fn name(&self) -> &str;

    /// 发现器类型
    fn discoverer_type(&self) -> DiscovererType;

    /// 是否启用
    fn is_enabled(&self) -> bool;

    /// 发现指定 infohash 的 peer
    ///
    /// # 参数
    /// - `infohash`: torrent 的 infohash（20 字节）
    /// - `limit`: 最大返回 peer 数量
    ///
    /// # 返回
    /// 发现的 peer 列表，包含来源信息
    async fn discover_peers(
        &self,
        infohash: &Infohash,
        limit: usize,
    ) -> anyhow::Result<Vec<PeerInfo>>;

    /// 宣告自己正在下载/做种
    ///
    /// 向 tracker/DHT 宣告自己的存在，让其他 peer 能找到你
    async fn announce(
        &self,
        infohash: &Infohash,
        port: u16,
        event: AnnounceEvent,
    ) -> anyhow::Result<()>;

    /// 健康检查（返回是否健康）
    async fn health_check(&self) -> bool;

    /// 获取统计信息
    fn stats(&self) -> DiscovererStats;

    /// 获取 tracker 评分列表（仅 Tracker 发现器实现）
    /// 返回 (tracker_url, score, disabled) 列表，按评分降序
    fn tracker_scores(&self) -> Option<Vec<(String, f64, bool)>> {
        None
    }

    /// 添加 peer 到 PEX 连接池（仅 PEX 发现器实现）
    /// Tracker 发现的 peer 通过此方法喂给 PEX，实现 PEX 二次扩散
    fn add_peer_for_pex(&self, _addr: std::net::SocketAddr) {}
}

/// 发现器类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DiscovererType {
    Tracker,
    Dht,
    Pex,
    /// 局域网多播发现（BEP 标准）
    Lpd,
    /// HTTP/Web Seed（BEP 19/17）
    WebSeed,
    /// 自定义发现器（第三方插件）
    Custom,
}

impl DiscovererType {
    pub fn as_str(&self) -> &'static str {
        match self {
            DiscovererType::Tracker => "tracker",
            DiscovererType::Dht => "dht",
            DiscovererType::Pex => "pex",
            DiscovererType::Lpd => "lpd",
            DiscovererType::WebSeed => "webseed",
            DiscovererType::Custom => "custom",
        }
    }
}

/// 宣告事件
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnounceEvent {
    Started,
    Stopped,
    Completed,
    /// 定期更新（无事件）
    None,
}

impl AnnounceEvent {
    pub fn as_str(&self) -> &'static str {
        match self {
            AnnounceEvent::Started => "started",
            AnnounceEvent::Stopped => "stopped",
            AnnounceEvent::Completed => "completed",
            AnnounceEvent::None => "none",
        }
    }
}

/// 发现器统计
#[derive(Debug, Clone, Default)]
pub struct DiscovererStats {
    /// 总请求次数
    pub total_requests: u64,
    /// 成功次数
    pub success_requests: u64,
    /// 失败次数
    pub failed_requests: u64,
    /// 累计发现 peer 数
    pub total_peers_discovered: u64,
    /// 平均响应时间（毫秒）
    pub avg_response_time_ms: f64,
    /// 最后一次成功时间
    pub last_success_at: Option<SystemTime>,
    /// 最后一次失败时间
    pub last_failure_at: Option<SystemTime>,
}

impl DiscovererStats {
    /// 记录一次成功请求
    pub fn record_success(&mut self, peers_count: usize, response_time_ms: f64) {
        self.total_requests += 1;
        self.success_requests += 1;
        self.total_peers_discovered += peers_count as u64;
        self.last_success_at = Some(SystemTime::now());
        // 简单移动平均
        if self.avg_response_time_ms == 0.0 {
            self.avg_response_time_ms = response_time_ms;
        } else {
            self.avg_response_time_ms = self.avg_response_time_ms * 0.9 + response_time_ms * 0.1;
        }
    }

    /// 记录一次失败请求
    pub fn record_failure(&mut self) {
        self.total_requests += 1;
        self.failed_requests += 1;
        self.last_failure_at = Some(SystemTime::now());
    }

    /// 成功率
    pub fn success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            return 0.0;
        }
        self.success_requests as f64 / self.total_requests as f64
    }
}
