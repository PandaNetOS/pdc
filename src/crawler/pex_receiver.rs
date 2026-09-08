//! PEX 被动接收器（BEP 11）
//!
//! 目的：从其他 BT 客户端的 PEX（Peer Exchange）消息中被动获取 peer 列表，
//! 加入 PeerRepo。PEX 消息通过 BT 扩展协议（BEP 10）发送。
//!
//! PEX 消息格式（bencode 字典）：
//! - added: compact peer 列表（每 6 字节：4 字节 IP + 2 字节端口）
//! - added.f: 每个 peer 的标志位（1 字节）
//! - dropped: 已删除的 peer 列表
//! - added6: IPv6 peer 列表
//! - added6.f: IPv6 peer 标志
//!
//! PEX 标志位（added.f）：
//! - 0x01: 加密偏好（PE_ENCRYPTION）
//! - 0x02: 上传只（PE_SEED）
//! - 0x04: 支持 uTP（PE_UTP）
//! - 0x08: 支持 holepunch（PE_HOLEPUNCH）

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use serde_bencode::value::Value as BencodeValue;
use tracing::{debug, info, warn};

use crate::storage::PeerRepoImpl;
use crate::types::{Infohash, PeerInfo, PeerSource};

/// PEX peer 标志
#[derive(Debug, Clone, Copy, Default)]
pub struct PexPeerFlags {
    pub encryption: bool,
    pub seed: bool,
    pub utp: bool,
    pub holepunch: bool,
}

impl PexPeerFlags {
    pub fn from_byte(b: u8) -> Self {
        Self {
            encryption: b & 0x01 != 0,
            seed: b & 0x02 != 0,
            utp: b & 0x04 != 0,
            holepunch: b & 0x08 != 0,
        }
    }
}

/// PEX 消息解析结果
#[derive(Debug, Clone, Default)]
pub struct PexMessage {
    /// 新增的 IPv4 peer 列表（addr, flags）
    pub added: Vec<(SocketAddr, PexPeerFlags)>,
    /// 删除的 IPv4 peer 列表
    pub dropped: Vec<SocketAddr>,
    /// 新增的 IPv6 peer 列表
    pub added6: Vec<(SocketAddr, PexPeerFlags)>,
    /// 删除的 IPv6 peer 列表
    pub dropped6: Vec<SocketAddr>,
}

impl PexMessage {
    /// 从 bencode 字节解析 PEX 消息
    pub fn parse(data: &[u8]) -> Option<Self> {
        let value: BencodeValue = serde_bencode::from_bytes(data).ok()?;
        let dict = match value {
            BencodeValue::Dict(d) => d,
            _ => return None,
        };

        let mut msg = PexMessage::default();

        // 解析 added（IPv4 compact peers）
        if let Some(BencodeValue::Bytes(added_bytes)) = dict.get(b"added".as_ref()) {
            let flags = match dict.get(b"added.f".as_ref()) {
                Some(BencodeValue::Bytes(f)) => f.as_slice(),
                _ => &[],
            };
            msg.added = parse_compact_peers_with_flags(added_bytes, flags);
        }

        // 解析 dropped（IPv4 compact peers）
        if let Some(BencodeValue::Bytes(dropped_bytes)) = dict.get(b"dropped".as_ref()) {
            msg.dropped = parse_compact_peers(dropped_bytes);
        }

        // 解析 added6（IPv6 compact peers）
        if let Some(BencodeValue::Bytes(added6_bytes)) = dict.get(b"added6".as_ref()) {
            let flags = match dict.get(b"added6.f".as_ref()) {
                Some(BencodeValue::Bytes(f)) => f.as_slice(),
                _ => &[],
            };
            msg.added6 = parse_compact_peers6_with_flags(added6_bytes, flags);
        }

        // 解析 dropped6
        if let Some(BencodeValue::Bytes(dropped6_bytes)) = dict.get(b"dropped6".as_ref()) {
            msg.dropped6 = parse_compact_peers6(dropped6_bytes);
        }

        Some(msg)
    }
}

/// 解析 IPv4 compact peers（每 6 字节：4 字节 IP + 2 字节端口）
fn parse_compact_peers(data: &[u8]) -> Vec<SocketAddr> {
    let mut peers = vec![];
    let (chunks, _) = data.as_chunks::<6>();
    for chunk in chunks {
        let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        peers.push(SocketAddr::new(IpAddr::V4(ip), port));
    }
    peers
}

/// 解析 IPv4 compact peers with flags
fn parse_compact_peers_with_flags(data: &[u8], flags: &[u8]) -> Vec<(SocketAddr, PexPeerFlags)> {
    let peers = parse_compact_peers(data);
    peers
        .into_iter()
        .enumerate()
        .map(|(i, addr)| {
            let flag = if i < flags.len() {
                PexPeerFlags::from_byte(flags[i])
            } else {
                PexPeerFlags::default()
            };
            (addr, flag)
        })
        .collect()
}

/// 解析 IPv6 compact peers（每 18 字节：16 字节 IP + 2 字节端口）
fn parse_compact_peers6(data: &[u8]) -> Vec<SocketAddr> {
    let mut peers = vec![];
    let (chunks, _) = data.as_chunks::<18>();
    for chunk in chunks {
        let mut ip_bytes = [0u8; 16];
        ip_bytes.copy_from_slice(&chunk[0..16]);
        let ip = Ipv6Addr::from(ip_bytes);
        let port = u16::from_be_bytes([chunk[16], chunk[17]]);
        peers.push(SocketAddr::new(IpAddr::V6(ip), port));
    }
    peers
}

/// 解析 IPv6 compact peers with flags
fn parse_compact_peers6_with_flags(data: &[u8], flags: &[u8]) -> Vec<(SocketAddr, PexPeerFlags)> {
    let peers = parse_compact_peers6(data);
    peers
        .into_iter()
        .enumerate()
        .map(|(i, addr)| {
            let flag = if i < flags.len() {
                PexPeerFlags::from_byte(flags[i])
            } else {
                PexPeerFlags::default()
            };
            (addr, flag)
        })
        .collect()
}

/// BT 扩展握手消息解析（BEP 10）
///
/// 扩展握手是一个 bencode 字典，包含：
/// - m: 支持的扩展消息映射（名称 -> ID）
/// - p: 监听端口
/// - v: 客户端版本
/// - yourip: 对端看到的我们的 IP
#[derive(Debug, Clone, Default)]
pub struct ExtensionHandshake {
    /// 支持的扩展消息映射（名称 -> ID）
    pub messages: HashMap<String, u8>,
    /// 对端监听端口
    pub port: Option<u16>,
    /// 客户端版本
    pub version: Option<String>,
    /// 对端看到的我们的 IP
    pub your_ip: Option<SocketAddr>,
    /// ut_pex 的消息 ID（如果支持）
    pub ut_pex_id: Option<u8>,
}

impl ExtensionHandshake {
    /// 从 bencode 字节解析扩展握手
    pub fn parse(data: &[u8]) -> Option<Self> {
        let value: BencodeValue = serde_bencode::from_bytes(data).ok()?;
        let dict = match value {
            BencodeValue::Dict(d) => d,
            _ => return None,
        };

        let mut handshake = ExtensionHandshake::default();

        // 解析 m（扩展消息映射）
        if let Some(BencodeValue::Dict(m_dict)) = dict.get(b"m".as_ref()) {
            for (key, val) in m_dict.iter() {
                if let (Ok(name), BencodeValue::Int(id)) = (std::str::from_utf8(key), val) {
                    if let Ok(id_u8) = u8::try_from(*id) {
                        handshake.messages.insert(name.to_string(), id_u8);
                        if name == "ut_pex" {
                            handshake.ut_pex_id = Some(id_u8);
                        }
                    }
                }
            }
        }

        // 解析 p（端口）
        if let Some(BencodeValue::Int(p)) = dict.get(b"p".as_ref()) {
            if let Ok(port) = u16::try_from(*p) {
                handshake.port = Some(port);
            }
        }

        // 解析 v（版本）
        if let Some(BencodeValue::Bytes(v_bytes)) = dict.get(b"v".as_ref()) {
            if let Ok(version) = String::from_utf8(v_bytes.to_vec()) {
                handshake.version = Some(version);
            }
        }

        // 解析 yourip
        if let Some(BencodeValue::Bytes(ip_bytes)) = dict.get(b"yourip".as_ref()) {
            if ip_bytes.len() == 4 {
                let ip = Ipv4Addr::new(ip_bytes[0], ip_bytes[1], ip_bytes[2], ip_bytes[3]);
                handshake.your_ip = Some(SocketAddr::new(IpAddr::V4(ip), 0));
            }
        }

        Some(handshake)
    }

    /// 是否支持 ut_pex
    pub fn supports_pex(&self) -> bool {
        self.ut_pex_id.is_some()
    }
}

/// PEX 接收器统计
#[derive(Debug, Clone, Default)]
pub struct PexReceiverStats {
    /// 收到的扩展握手数
    pub extension_handshakes: u64,
    /// 支持 PEX 的对端数
    pub pex_supported: u64,
    /// 收到的 PEX 消息数
    pub pex_messages: u64,
    /// 从 PEX 提取的 peer 总数
    pub peers_extracted: u64,
    /// IPv4 peer 数
    pub ipv4_peers: u64,
    /// IPv6 peer 数
    pub ipv6_peers: u64,
    /// 支持 uTP 的 peer 数
    pub utp_peers: u64,
    /// 支持 holepunch 的 peer 数
    pub holepunch_peers: u64,
    /// 解析错误数
    pub parse_errors: u64,
    /// 去重跳过的 peer 数
    pub dedup_skipped: u64,
}

/// PEX 被动接收器
pub struct PexReceiver {
    /// PeerRepo（用于存储提取的 peer）
    peer_repo: Option<Arc<PeerRepoImpl>>,
    /// 统计
    stats: parking_lot::RwLock<PexReceiverStats>,
    /// 短期去重缓存（peer addr -> 最后处理时间）
    /// 避免同一批 PEX 消息中的重复 peer 重复处理
    dedup_cache: parking_lot::RwLock<std::collections::HashMap<SocketAddr, std::time::Instant>>,
    /// 去重缓存的过期时间（默认 60 秒）
    dedup_ttl: std::time::Duration,
}

impl PexReceiver {
    /// 创建新的 PEX 接收器
    pub fn new() -> Self {
        Self {
            peer_repo: None,
            stats: parking_lot::RwLock::new(PexReceiverStats::default()),
            dedup_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
            dedup_ttl: std::time::Duration::from_secs(60),
        }
    }

    /// 设置去重缓存的过期时间
    pub fn with_dedup_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.dedup_ttl = ttl;
        self
    }

    /// 设置 PeerRepo
    pub fn with_peer_repo(mut self, peer_repo: Arc<PeerRepoImpl>) -> Self {
        self.peer_repo = Some(peer_repo);
        self
    }

    /// 获取统计
    pub fn stats(&self) -> PexReceiverStats {
        self.stats.read().clone()
    }

    /// 处理扩展握手消息
    /// 返回对端的 ut_pex 消息 ID（如果支持）
    pub fn handle_extension_handshake(&self, data: &[u8]) -> Option<u8> {
        match ExtensionHandshake::parse(data) {
            Some(handshake) => {
                {
                    let mut stats = self.stats.write();
                    stats.extension_handshakes += 1;
                    if handshake.supports_pex() {
                        stats.pex_supported += 1;
                    }
                }
                debug!(
                    "[PEX] 收到扩展握手（版本={:?}, 端口={:?}, 支持PEX={}）",
                    handshake.version,
                    handshake.port,
                    handshake.supports_pex()
                );
                handshake.ut_pex_id
            }
            None => {
                warn!("[PEX] 扩展握手解析失败");
                {
                    let mut stats = self.stats.write();
                    stats.parse_errors += 1;
                }
                None
            }
        }
    }

    /// 处理 PEX 消息
    /// infohash: 这些 peer 所属的 infohash
    pub fn handle_pex_message(&self, data: &[u8], infohash: &Infohash) {
        match PexMessage::parse(data) {
            Some(msg) => {
                let total_added = msg.added.len() + msg.added6.len();
                let utp_count = msg
                    .added
                    .iter()
                    .filter(|(_, f)| f.utp)
                    .count()
                    + msg.added6.iter().filter(|(_, f)| f.utp).count();
                let holepunch_count = msg
                    .added
                    .iter()
                    .filter(|(_, f)| f.holepunch)
                    .count()
                    + msg.added6.iter().filter(|(_, f)| f.holepunch).count();

                {
                    let mut stats = self.stats.write();
                    stats.pex_messages += 1;
                    stats.peers_extracted += total_added as u64;
                    stats.ipv4_peers += msg.added.len() as u64;
                    stats.ipv6_peers += msg.added6.len() as u64;
                    stats.utp_peers += utp_count as u64;
                    stats.holepunch_peers += holepunch_count as u64;
                }

                debug!(
                    "[PEX] 收到 PEX 消息（新增 IPv4={}, IPv6={}, 删除={}, uTP={}, holepunch={}）",
                    msg.added.len(),
                    msg.added6.len(),
                    msg.dropped.len(),
                    utp_count,
                    holepunch_count
                );

                // 加入 PeerRepo
                if let Some(repo) = &self.peer_repo {
                    let now = std::time::SystemTime::now();
                    let mut peers_to_add: Vec<PeerInfo> = Vec::with_capacity(total_added);

                    for (addr, flags) in &msg.added {
                        let mut peer = PeerInfo::new(*addr, PeerSource::Pex);
                        peer.first_seen = now;
                        peer.last_active = now;
                        // 记录 uTP 和 holepunch 支持到 metadata
                        if flags.utp {
                            peer.metadata.insert("supports_utp".to_string(), "true".to_string());
                        }
                        if flags.holepunch {
                            peer.metadata
                                .insert("supports_holepunch".to_string(), "true".to_string());
                        }
                        if flags.encryption {
                            peer.metadata
                                .insert("encryption_preferred".to_string(), "true".to_string());
                        }
                        if flags.seed {
                            peer.metadata.insert("is_seed".to_string(), "true".to_string());
                        }
                        peers_to_add.push(peer);
                    }

                    for (addr, flags) in &msg.added6 {
                        let mut peer = PeerInfo::new(*addr, PeerSource::Pex);
                        peer.first_seen = now;
                        peer.last_active = now;
                        if flags.utp {
                            peer.metadata.insert("supports_utp".to_string(), "true".to_string());
                        }
                        if flags.holepunch {
                            peer.metadata
                                .insert("supports_holepunch".to_string(), "true".to_string());
                        }
                        peers_to_add.push(peer);
                    }

                    // 短期去重：跳过最近 60 秒内已处理过的 peer
                    let now = std::time::Instant::now();
                    let mut dedup_skipped = 0;
                    {
                        let mut cache = self.dedup_cache.write();
                        // 清理过期的缓存条目
                        cache.retain(|_, t| now.duration_since(*t) < self.dedup_ttl);
                        // 过滤掉已在缓存中的 peer
                        peers_to_add.retain(|p| {
                            if cache.contains_key(&p.addr) {
                                dedup_skipped += 1;
                                false
                            } else {
                                cache.insert(p.addr, now);
                                true
                            }
                        });
                    }
                    if dedup_skipped > 0 {
                        let mut stats = self.stats.write();
                        stats.dedup_skipped += dedup_skipped as u64;
                    }

                    if !peers_to_add.is_empty() {
                        repo.add_peers_sync(infohash, &peers_to_add);
                        info!(
                            "[PEX] 从 PEX 消息提取 {} 个 peer 加入 PeerRepo（去重跳过 {}，infohash={}）",
                            peers_to_add.len(),
                            dedup_skipped,
                            hex::encode(infohash)
                        );
                    }
                }
            }
            None => {
                warn!("[PEX] PEX 消息解析失败");
                {
                    let mut stats = self.stats.write();
                    stats.parse_errors += 1;
                }
            }
        }
    }
}

impl Default for PexReceiver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_compact_peers() {
        // 构造两个 peer: 192.168.1.1:6881, 10.0.0.1:8080
        let data = [
            192, 168, 1, 1, 0x1A, 0xE1, // 6881
            10, 0, 0, 1, 0x1F, 0x90, // 8080
        ];
        let peers = parse_compact_peers(&data);
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].to_string(), "192.168.1.1:6881");
        assert_eq!(peers[1].to_string(), "10.0.0.1:8080");
    }

    #[test]
    fn test_pex_peer_flags() {
        let flags = PexPeerFlags::from_byte(0x0D); // 0x01 + 0x04 + 0x08
        assert!(flags.encryption);
        assert!(!flags.seed);
        assert!(flags.utp);
        assert!(flags.holepunch);
    }

    #[test]
    fn test_extension_handshake_parse() {
        // 构造一个简单的扩展握手 bencode
        // d1:md5:ut_pexi1e4:ut_uti2ee1:pi6881e1:v11:TestCliente
        let data = b"d1:md5:ut_pexi1e4:ut_uti2ee1:pi6881e1:v11:TestCliente";
        let handshake = ExtensionHandshake::parse(data).unwrap();
        assert_eq!(handshake.ut_pex_id, Some(1));
        assert_eq!(handshake.port, Some(6881));
        assert_eq!(handshake.version, Some("TestClient".to_string()));
        assert!(handshake.supports_pex());
    }
}
