//! 公共数据结构
//!
//! 定义 PeerDiscovery 模块中使用的所有公共类型。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

/// Peer 信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Peer 地址
    pub addr: SocketAddr,
    /// Peer ID（可选，20 字节）
    #[serde(with = "peer_id_serde", skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<[u8; 20]>,
    /// 来源（Tracker/DHT/PEX）
    pub source: PeerSource,
    /// 首次发现时间
    pub first_seen: SystemTime,
    /// 最后一次活跃时间
    pub last_active: SystemTime,
    /// 优先级评分（越高越优先连接）
    pub priority_score: u32,
    /// 连接尝试次数
    pub connection_attempts: u32,
    /// 连接成功次数
    pub connection_successes: u32,
    /// 是否为 IPv6
    pub is_ipv6: bool,
    /// 元数据（扩展字段）
    pub metadata: HashMap<String, String>,
}

impl PeerInfo {
    /// 创建新的 PeerInfo
    pub fn new(addr: SocketAddr, source: PeerSource) -> Self {
        let now = SystemTime::now();
        Self {
            addr,
            peer_id: None,
            source,
            first_seen: now,
            last_active: now,
            priority_score: source.base_score(),
            connection_attempts: 0,
            connection_successes: 0,
            is_ipv6: addr.is_ipv6(),
            metadata: Default::default(),
        }
    }

    /// 计算优先级评分
    pub fn calculate_priority(&mut self) {
        let mut score = self.source.base_score();

        // IPv4 优先
        if !self.is_ipv6 {
            score += 50;
        }

        // 常见 BT 端口优先（更可能是长期做种的）
        match self.addr.port() {
            6881..=6889 => score += 30,
            51413 => score += 20,
            _ => {}
        }

        // 连接成功率高的优先
        if self.connection_attempts > 0 {
            let success_rate = self.connection_successes as f64 / self.connection_attempts as f64;
            score += (success_rate * 100.0) as u32;
        }

        // 最近活跃的优先
        if let Ok(elapsed) = self.last_active.elapsed() {
            if elapsed < Duration::from_secs(300) {
                score += 40;
            } else if elapsed < Duration::from_secs(1800) {
                score += 20;
            }
        }

        self.priority_score = score;
    }

    /// 是否过期（超过 24 小时没活跃）
    pub fn is_expired(&self) -> bool {
        self.last_active
            .elapsed()
            .map(|e| e > Duration::from_secs(86400))
            .unwrap_or(false)
    }
}

/// Peer 来源
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PeerSource {
    Tracker,
    Dht,
    Pex,
    /// 手动添加
    Manual,
}

impl PeerSource {
    /// 基础优先级评分
    pub fn base_score(&self) -> u32 {
        match self {
            PeerSource::Tracker => 100, // tracker 返回的通常更活跃
            PeerSource::Dht => 80,      // DHT 发现的
            PeerSource::Pex => 60,      // PEX 交换的
            PeerSource::Manual => 50,   // 手动添加的
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            PeerSource::Tracker => "tracker",
            PeerSource::Dht => "dht",
            PeerSource::Pex => "pex",
            PeerSource::Manual => "manual",
        }
    }
}

/// 发现结果
#[derive(Debug, Clone, Default)]
pub struct DiscoveryResult {
    /// 发现的 peer 列表
    pub peers: Vec<PeerInfo>,
    /// 各来源统计
    pub source_stats: HashMap<PeerSource, usize>,
    /// 总耗时
    pub total_duration: Duration,
    /// 各发现器耗时
    pub discoverer_durations: HashMap<String, Duration>,
}

/// Infohash（20 字节）
pub type Infohash = [u8; 20];

/// Peer ID 序列化辅助模块
mod peer_id_serde {

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<[u8; 20]>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(id) => s.serialize_str(&hex::encode(id)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<[u8; 20]>, D::Error> {
        let opt: Option<String> = Option::deserialize(d)?;
        match opt {
            Some(s) => {
                let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
                if bytes.len() != 20 {
                    return Err(serde::de::Error::custom("peer_id must be 20 bytes"));
                }
                let mut arr = [0u8; 20];
                arr.copy_from_slice(&bytes);
                Ok(Some(arr))
            }
            None => Ok(None),
        }
    }
}
