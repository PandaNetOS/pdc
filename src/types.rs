//! 公共数据结构
//!
//! 定义 PeerDiscoveryCenter 中使用的所有公共类型，包括：
//! - Peer 信息、来源、发现结果
//! - 事件总线事件类型
//! - HTTP Tracker 协议类型（announce/scrape）

use rustc_hash::FxHashMap;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Peer 基础类型
// ---------------------------------------------------------------------------

/// Peer 信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Peer 地址
    pub addr: SocketAddr,
    /// Peer ID（可选，20 字节）
    #[serde(with = "peer_id_serde", skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<[u8; 20]>,
    /// 来源（Tracker/DHT/PEX/LPD/WebSeed/Manual）
    pub source: PeerSource,
    /// 首次发现时间
    pub first_seen: SystemTime,
    /// 最后一次活跃时间
    pub last_active: SystemTime,
    /// 优先级评分（越高越优先连接）
    pub priority_score: f64,
    /// 连接尝试次数
    pub connection_attempts: u32,
    /// 连接成功次数
    pub connection_successes: u32,
    /// 是否为 IPv6
    pub is_ipv6: bool,
    /// 元数据（扩展字段）
    pub metadata: FxHashMap<String, String>,
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
            priority_score: 45.0,  // 中性初始分，等待 ScoreMaintainer 重算（评分唯一性原则）
            connection_attempts: 0,
            connection_successes: 0,
            is_ipv6: addr.is_ipv6(),
            metadata: Default::default(),
        }
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
    /// 局域网多播发现（BEP 标准）
    Lpd,
    /// HTTP/Web Seed（BEP 19/17）
    WebSeed,
    /// 超级 Tracker 本地 announce 存储
    SuperTracker,
    /// 手动添加
    Manual,
    /// uTP 协议连接发现（BEP 29）
    Utp,
}

impl PeerSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            PeerSource::Tracker => "tracker",
            PeerSource::Dht => "dht",
            PeerSource::Pex => "pex",
            PeerSource::Lpd => "lpd",
            PeerSource::WebSeed => "webseed",
            PeerSource::SuperTracker => "super_tracker",
            PeerSource::Manual => "manual",
            PeerSource::Utp => "utp",
        }
    }
}

/// 发现结果
#[derive(Debug, Clone, Default)]
pub struct DiscoveryResult {
    /// 发现的 peer 列表
    pub peers: Vec<PeerInfo>,
    /// 各来源统计
    pub source_stats: FxHashMap<PeerSource, usize>,
    /// 总耗时
    pub total_duration: Duration,
    /// 各发现器耗时
    pub discoverer_durations: FxHashMap<String, Duration>,
}

/// Infohash（20 字节）
pub type Infohash = [u8; 20];

// ---------------------------------------------------------------------------
// 事件总线事件类型
// ---------------------------------------------------------------------------

/// 事件总线事件
#[derive(Debug, Clone, serde::Serialize)]
pub enum Event {
    /// 发现了新的 peer
    PeerDiscovered {
        infohash: Infohash,
        peers: Vec<PeerInfo>,
        source: String,
    },
    /// 看到了新的 infohash（爬虫被动收集）
    InfohashSeen {
        infohash: Infohash,
        source: String,
        seen_at: SystemTime,
    },
    /// 节点健康状态更新
    NodeHealthUpdate {
        discoverer: String,
        healthy: bool,
        message: Option<String>,
    },
    /// 收到 announce 请求（超级 Tracker）
    AnnounceRequest {
        infohash: Infohash,
        peer_addr: SocketAddr,
        peer_id: Option<[u8; 20]>,
        event: AnnounceEvent,
        uploaded: u64,
        downloaded: u64,
        left: u64,
    },
    /// Peer 质量评分更新
    PeerQualityScore {
        addr: SocketAddr,
        score: f64,
        reason: String,
    },
    /// 爬虫进度
    CrawlProgress {
        nodes_crawled: u64,
        infohashes_collected: u64,
        peers_collected: u64,
        message: String,
    },
    /// 配置变更
    ConfigChanged,
}

// ---------------------------------------------------------------------------
// HTTP Tracker 协议类型（BEP 3 / BEP 48）
// ---------------------------------------------------------------------------

/// Tracker announce 请求参数
#[derive(Debug, Clone)]
pub struct TrackerAnnounceRequest {
    pub info_hash: Infohash,
    pub peer_id: [u8; 20],
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub event: Option<AnnounceEvent>,
    pub compact: bool,
    pub numwant: Option<usize>,
    pub no_peer_id: bool,
    pub key: Option<String>,
    pub trackerid: Option<String>,
    /// 请求方地址（从连接中获取，用于排除自己）
    pub remote_addr: SocketAddr,
}

/// Tracker announce 响应
#[derive(Debug, Clone)]
pub struct TrackerAnnounceResponse {
    /// 推荐的再次 announce 间隔（秒）
    pub interval: i64,
    /// 最小间隔（秒）
    pub min_interval: Option<i64>,
    /// Tracker ID
    pub tracker_id: Option<String>,
    /// 完整的 peer 数（做种者）
    pub complete: i64,
    /// 下载中的 peer 数
    pub incomplete: i64,
    /// Peer 列表
    pub peers: Vec<SocketAddr>,
    /// 错误信息（如果有）
    pub failure_reason: Option<String>,
    /// 警告信息
    pub warning_message: Option<String>,
}

impl TrackerAnnounceResponse {
    /// 序列化为 bencode（compact 格式，手动构造不依赖库内部 API）
    pub fn to_bencode_compact(&self) -> Vec<u8> {
        let mut out = Vec::new();

        if let Some(ref reason) = self.failure_reason {
            // d14:failure reasonN:<reason>e
            out.extend_from_slice(b"d14:failure reason");
            out.extend_from_slice(reason.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(reason.as_bytes());
            out.push(b'e');
            return out;
        }

        out.push(b'd');

        // interval
        out.extend_from_slice(b"8:interval");
        out.push(b'i');
        out.extend_from_slice(self.interval.to_string().as_bytes());
        out.push(b'e');

        // min interval
        if let Some(min) = self.min_interval {
            out.extend_from_slice(b"12:min interval");
            out.push(b'i');
            out.extend_from_slice(min.to_string().as_bytes());
            out.push(b'e');
        }

        // tracker id
        if let Some(ref tid) = self.tracker_id {
            out.extend_from_slice(b"10:tracker id");
            out.extend_from_slice(tid.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(tid.as_bytes());
        }

        // complete
        out.extend_from_slice(b"8:complete");
        out.push(b'i');
        out.extend_from_slice(self.complete.to_string().as_bytes());
        out.push(b'e');

        // incomplete
        out.extend_from_slice(b"10:incomplete");
        out.push(b'i');
        out.extend_from_slice(self.incomplete.to_string().as_bytes());
        out.push(b'e');

        // warning message
        if let Some(ref warning) = self.warning_message {
            out.extend_from_slice(b"15:warning message");
            out.extend_from_slice(warning.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(warning.as_bytes());
        }

        // peers (compact: 每 6 字节一个 peer，4字节IPv4 + 2字节端口)
        out.extend_from_slice(b"5:peers");
        let mut compact = Vec::with_capacity(self.peers.len() * 6);
        for peer in &self.peers {
            if let std::net::IpAddr::V4(ipv4) = peer.ip() {
                compact.extend_from_slice(&ipv4.octets());
                compact.extend_from_slice(&peer.port().to_be_bytes());
            }
        }
        out.extend_from_slice(compact.len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(&compact);

        out.push(b'e');
        out
    }
}

/// Tracker scrape 请求
#[derive(Debug, Clone)]
pub struct TrackerScrapeRequest {
    pub info_hashes: Vec<Infohash>,
}

/// 单个 infohash 的 scrape 统计
#[derive(Debug, Clone, Default)]
pub struct ScrapeEntry {
    pub complete: i64,
    pub downloaded: i64,
    pub incomplete: i64,
    pub name: Option<String>,
}

/// Tracker scrape 响应
#[derive(Debug, Clone, Default)]
pub struct TrackerScrapeResponse {
    pub files: FxHashMap<Infohash, ScrapeEntry>,
    pub failure_reason: Option<String>,
}

impl TrackerScrapeResponse {
    /// 序列化为 bencode（手动构造）
    pub fn to_bencode(&self) -> Vec<u8> {
        let mut out = Vec::new();

        if let Some(ref reason) = self.failure_reason {
            out.extend_from_slice(b"d14:failure reason");
            out.extend_from_slice(reason.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(reason.as_bytes());
            out.push(b'e');
            return out;
        }

        out.push(b'd');
        out.extend_from_slice(b"5:files");
        out.push(b'd');

        for (infohash, entry) in &self.files {
            // key: 20 字节 infohash
            out.extend_from_slice(b"20:");
            out.extend_from_slice(infohash);

            // value: dict
            out.push(b'd');

            // complete
            out.extend_from_slice(b"8:complete");
            out.push(b'i');
            out.extend_from_slice(entry.complete.to_string().as_bytes());
            out.push(b'e');

            // downloaded
            out.extend_from_slice(b"10:downloaded");
            out.push(b'i');
            out.extend_from_slice(entry.downloaded.to_string().as_bytes());
            out.push(b'e');

            // incomplete
            out.extend_from_slice(b"10:incomplete");
            out.push(b'i');
            out.extend_from_slice(entry.incomplete.to_string().as_bytes());
            out.push(b'e');

            // name
            if let Some(ref name) = entry.name {
                out.extend_from_slice(b"4:name");
                out.extend_from_slice(name.len().to_string().as_bytes());
                out.push(b':');
                out.extend_from_slice(name.as_bytes());
            }

            out.push(b'e');
        }

        out.push(b'e'); // end files
        out.push(b'e'); // end root
        out
    }
}

/// 宣告事件
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
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

impl std::str::FromStr for AnnounceEvent {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "started" => AnnounceEvent::Started,
            "stopped" => AnnounceEvent::Stopped,
            "completed" => AnnounceEvent::Completed,
            _ => AnnounceEvent::None,
        })
    }
}

// ---------------------------------------------------------------------------
// Peer ID 序列化辅助
// ---------------------------------------------------------------------------

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_announce_response_bencode() {
        let resp = TrackerAnnounceResponse {
            interval: 1800,
            min_interval: Some(900),
            tracker_id: Some("pdc".to_string()),
            complete: 5,
            incomplete: 3,
            peers: vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                6881,
            )],
            failure_reason: None,
            warning_message: None,
        };
        let bytes = resp.to_bencode_compact();
        let s = String::from_utf8_lossy(&bytes);
        assert!(s.starts_with("d8:intervali1800e"));
        assert!(s.contains("12:min intervali900e"));
        assert!(s.contains("8:completei5e"));
        assert!(s.contains("10:incompletei3e"));
        assert!(s.contains("5:peers6:"));
    }

    #[test]
    fn test_announce_response_failure() {
        let resp = TrackerAnnounceResponse {
            interval: 0,
            min_interval: None,
            tracker_id: None,
            complete: 0,
            incomplete: 0,
            peers: vec![],
            failure_reason: Some("test error".to_string()),
            warning_message: None,
        };
        let bytes = resp.to_bencode_compact();
        let s = String::from_utf8_lossy(&bytes);
        assert!(s.contains("failure reason"));
        assert!(s.contains("test error"));
    }

    #[test]
    fn test_scrape_response_bencode() {
        let mut files = FxHashMap::default();
        files.insert(
            [0u8; 20],
            ScrapeEntry {
                complete: 10,
                downloaded: 100,
                incomplete: 5,
                name: None,
            },
        );
        let resp = TrackerScrapeResponse {
            files,
            failure_reason: None,
        };
        let bytes = resp.to_bencode();
        let s = String::from_utf8_lossy(&bytes);
        assert!(s.starts_with("d5:filesd20:"));
        assert!(s.contains("8:completei10e"));
        assert!(s.contains("10:downloadedi100e"));
        assert!(s.contains("10:incompletei5e"));
    }

    #[test]
    fn test_announce_event_from_str() {
        assert_eq!("started".parse(), Ok(AnnounceEvent::Started));
        assert_eq!("stopped".parse(), Ok(AnnounceEvent::Stopped));
        assert_eq!("completed".parse(), Ok(AnnounceEvent::Completed));
        assert_eq!("unknown".parse(), Ok(AnnounceEvent::None));
    }
}
