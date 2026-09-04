//! PEX 消息编解码
//!
//! 实现 BitTorrent 握手、扩展协议（BEP 10）和 ut_pex（BEP 11）消息编解码。

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use serde_bencode::from_bytes;
use serde_bencode::value::Value as BencodeValue;

use crate::types::Infohash;

/// BitTorrent 协议名
const BITTORRENT_PROTOCOL: &[u8] = b"BitTorrent protocol";

/// 扩展协议消息类型
const EXTENDED_MESSAGE_ID: u8 = 20;

/// 扩展握手的扩展 ID
#[allow(dead_code)]
const EXTENSION_HANDSHAKE_ID: u8 = 0;

/// BitTorrent 握手消息（68 字节）
pub struct BtHandshake {
    pub infohash: [u8; 20],
    pub peer_id: [u8; 20],
    pub reserved: [u8; 8],
}

impl BtHandshake {
    /// 构建握手消息
    pub fn build(infohash: &Infohash, peer_id: &[u8; 20]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(68);
        buf.push(19); // protocol name length
        buf.extend_from_slice(BITTORRENT_PROTOCOL);
        buf.extend_from_slice(&[0u8; 8]); // reserved bytes
        buf.extend_from_slice(infohash);
        buf.extend_from_slice(peer_id);
        buf
    }

    /// 解析握手消息
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 68 {
            return None;
        }
        if data[0] != 19 {
            return None;
        }
        if &data[1..20] != BITTORRENT_PROTOCOL {
            return None;
        }

        let mut reserved = [0u8; 8];
        reserved.copy_from_slice(&data[20..28]);

        let mut infohash = [0u8; 20];
        infohash.copy_from_slice(&data[28..48]);

        let mut peer_id = [0u8; 20];
        peer_id.copy_from_slice(&data[48..68]);

        Some(BtHandshake {
            infohash,
            peer_id,
            reserved,
        })
    }

    /// 检查是否支持扩展协议（reserved 第 20 位，即第 5 字节的第 4 位）
    pub fn supports_extension(&self) -> bool {
        self.reserved[5] & 0x10 != 0
    }
}

/// 扩展协议消息
pub struct ExtendedMessage {
    pub extension_id: u8,
    pub payload: Vec<u8>,
}

impl ExtendedMessage {
    /// 构建扩展协议消息
    ///
    /// 格式：4字节长度（大端）+ 1字节消息类型(20) + 1字节扩展ID + payload
    pub fn build(extension_id: u8, payload: &[u8]) -> Vec<u8> {
        let length = 2 + payload.len() as u32;
        let mut buf = Vec::with_capacity(4 + length as usize);
        buf.extend_from_slice(&length.to_be_bytes());
        buf.push(EXTENDED_MESSAGE_ID);
        buf.push(extension_id);
        buf.extend_from_slice(payload);
        buf
    }

    /// 从 TCP 流中读取一条消息
    ///
    /// 返回 (message_type, payload)
    pub async fn read_message<R>(reader: &mut R) -> std::io::Result<(u8, Vec<u8>)>
    where
        R: tokio::io::AsyncReadExt + Unpin,
    {
        // 读取 4 字节长度
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let length = u32::from_be_bytes(len_buf) as usize;

        if length == 0 {
            // keep-alive
            return Ok((255, vec![]));
        }

        // 读取消息体
        let mut msg_buf = vec![0u8; length];
        reader.read_exact(&mut msg_buf).await?;

        let msg_type = msg_buf[0];
        let payload = msg_buf[1..].to_vec();

        Ok((msg_type, payload))
    }
}

/// 扩展握手响应
pub struct ExtensionHandshake {
    /// 对方支持的扩展消息映射（扩展名称 -> 扩展ID）
    pub m: HashMap<String, u8>,
    /// 对方的 peer ID
    pub peer_id: Option<[u8; 20]>,
    /// 对方的端口
    pub port: Option<u16>,
    /// 客户端版本
    pub version: Option<String>,
}

impl ExtensionHandshake {
    /// 构建扩展握手请求
    pub fn build_request(our_peer_id: &[u8; 20], listen_port: u16) -> Vec<u8> {
        // 手动构造 bencode
        // d1:md11:ut_pex1e1:pi20:<peer_id>4:porti<port>e1:v17:PeerDiscoveryCentere
        let mut buf = Vec::new();
        buf.extend_from_slice(b"d1:md11:ut_pex");
        buf.extend_from_slice(b"1"); // 我们分配给 ut_pex 的扩展 ID = 1
        buf.extend_from_slice(b"e");
        buf.extend_from_slice(b"1:pi20:");
        buf.extend_from_slice(our_peer_id);
        buf.extend_from_slice(b"4:porti");
        buf.extend_from_slice(listen_port.to_string().as_bytes());
        buf.extend_from_slice(b"e1:v21:PeerDiscoveryCenter/0.2e");
        buf
    }

    /// 解析扩展握手响应
    pub fn parse(data: &[u8]) -> Option<Self> {
        let value: BencodeValue = from_bytes(data).ok()?;
        let dict = value.as_dict()?;

        let mut m = HashMap::new();
        if let Some(m_dict) = dict.get(b"m".as_slice()).and_then(|v| v.as_dict()) {
            for (key, val) in m_dict {
                if let (Ok(name), Some(id)) =
                    (std::str::from_utf8(key), val.as_int().map(|i| i as u8))
                {
                    m.insert(name.to_string(), id);
                }
            }
        }

        let peer_id = dict
            .get(b"pi".as_slice())
            .and_then(|v| v.as_bytes())
            .filter(|b| b.len() == 20)
            .map(|b| {
                let mut id = [0u8; 20];
                id.copy_from_slice(b);
                id
            });

        let port = dict
            .get(b"port".as_slice())
            .and_then(|v| v.as_int())
            .map(|p| p as u16);

        let version = dict
            .get(b"v".as_slice())
            .and_then(|v| v.as_bytes())
            .and_then(|b| String::from_utf8(b.clone()).ok());

        Some(ExtensionHandshake {
            m,
            peer_id,
            port,
            version,
        })
    }

    /// 获取 ut_pex 的扩展 ID
    pub fn ut_pex_id(&self) -> Option<u8> {
        self.m.get("ut_pex").copied()
    }
}

/// ut_pex 消息
pub struct UtPexMessage {
    /// 新增的 peer（compact peer info）
    pub added: Vec<SocketAddr>,
    /// 新增 peer 的 flags
    pub added_flags: Vec<u8>,
    /// 移除的 peer
    pub dropped: Vec<SocketAddr>,
    /// 移除 peer 的 flags
    pub dropped_flags: Vec<u8>,
}

impl UtPexMessage {
    /// 构建 ut_pex 消息
    pub fn build(added: &[SocketAddr], dropped: &[SocketAddr]) -> Vec<u8> {
        let added_compact = Self::encode_compact_peers(added);
        let dropped_compact = Self::encode_compact_peers(dropped);

        // 手动构造 bencode
        // d5:added<len>:<data>7:added.f<len>:<flags>7:dropped<len>:<data>9:dropped.f<len>:<flags>e
        let mut buf = Vec::new();
        buf.extend_from_slice(b"d5:added");
        buf.extend_from_slice(format!("{}:", added_compact.len()).as_bytes());
        buf.extend_from_slice(&added_compact);

        // added.f: 每个 peer 1 字节 flag，默认 0
        let added_flags = vec![0u8; added.len()];
        buf.extend_from_slice(b"7:added.f");
        buf.extend_from_slice(format!("{}:", added_flags.len()).as_bytes());
        buf.extend_from_slice(&added_flags);

        buf.extend_from_slice(b"7:dropped");
        buf.extend_from_slice(format!("{}:", dropped_compact.len()).as_bytes());
        buf.extend_from_slice(&dropped_compact);

        let dropped_flags = vec![0u8; dropped.len()];
        buf.extend_from_slice(b"9:dropped.f");
        buf.extend_from_slice(format!("{}:", dropped_flags.len()).as_bytes());
        buf.extend_from_slice(&dropped_flags);

        buf.push(b'e');
        buf
    }

    /// 解析 ut_pex 消息
    pub fn parse(data: &[u8]) -> Option<Self> {
        let value: BencodeValue = from_bytes(data).ok()?;
        let dict = value.as_dict()?;

        let added = dict
            .get(b"added".as_slice())
            .and_then(|v| v.as_bytes())
            .map(|b| Self::decode_compact_peers(b))
            .unwrap_or_default();

        let added_flags = dict
            .get(b"added.f".as_slice())
            .and_then(|v| v.as_bytes())
            .cloned()
            .unwrap_or_default();

        let dropped = dict
            .get(b"dropped".as_slice())
            .and_then(|v| v.as_bytes())
            .map(|b| Self::decode_compact_peers(b))
            .unwrap_or_default();

        let dropped_flags = dict
            .get(b"dropped.f".as_slice())
            .and_then(|v| v.as_bytes())
            .cloned()
            .unwrap_or_default();

        Some(UtPexMessage {
            added,
            added_flags,
            dropped,
            dropped_flags,
        })
    }

    /// 编码 compact peers（每 6 字节：4字节IP + 2字节端口）
    fn encode_compact_peers(peers: &[SocketAddr]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(peers.len() * 6);
        for peer in peers {
            if let IpAddr::V4(ip) = peer.ip() {
                buf.extend_from_slice(&ip.octets());
                buf.extend_from_slice(&peer.port().to_be_bytes());
            }
        }
        buf
    }

    /// 解码 compact peers
    fn decode_compact_peers(data: &[u8]) -> Vec<SocketAddr> {
        let mut peers = vec![];
        let (chunks, _) = data.as_chunks::<6>();
        for chunk in chunks {
            let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            if port != 0 {
                peers.push(SocketAddr::new(IpAddr::V4(ip), port));
            }
        }
        peers
    }
}

/// BencodeValue 扩展 trait
trait BencodeExt {
    fn as_dict(&self) -> Option<&HashMap<Vec<u8>, BencodeValue>>;
    #[allow(dead_code)]
    fn as_list(&self) -> Option<&Vec<BencodeValue>>;
    fn as_bytes(&self) -> Option<&Vec<u8>>;
    fn as_int(&self) -> Option<i64>;
}

impl BencodeExt for BencodeValue {
    fn as_dict(&self) -> Option<&HashMap<Vec<u8>, BencodeValue>> {
        match self {
            BencodeValue::Dict(d) => Some(d),
            _ => None,
        }
    }

    fn as_list(&self) -> Option<&Vec<BencodeValue>> {
        match self {
            BencodeValue::List(l) => Some(l),
            _ => None,
        }
    }

    fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            BencodeValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    fn as_int(&self) -> Option<i64> {
        match self {
            BencodeValue::Int(i) => Some(*i),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bt_handshake_build_parse() {
        let infohash = [1u8; 20];
        let peer_id = [2u8; 20];
        let msg = BtHandshake::build(&infohash, &peer_id);
        assert_eq!(msg.len(), 68);

        let parsed = BtHandshake::parse(&msg).unwrap();
        assert_eq!(parsed.infohash, infohash);
        assert_eq!(parsed.peer_id, peer_id);
    }

    #[test]
    fn test_bt_handshake_supports_extension() {
        let mut handshake = BtHandshake {
            infohash: [0u8; 20],
            peer_id: [0u8; 20],
            reserved: [0u8; 8],
        };
        assert!(!handshake.supports_extension());

        handshake.reserved[5] = 0x10;
        assert!(handshake.supports_extension());
    }

    #[test]
    fn test_extended_message_build() {
        let payload = b"test payload";
        let msg = ExtendedMessage::build(1, payload);
        assert_eq!(msg.len(), 4 + 2 + payload.len());
        assert_eq!(msg[4], EXTENDED_MESSAGE_ID);
        assert_eq!(msg[5], 1);
    }

    #[test]
    fn test_extension_handshake_build_request() {
        let peer_id = [0u8; 20];
        let msg = ExtensionHandshake::build_request(&peer_id, 6881);
        let s = String::from_utf8_lossy(&msg);
        assert!(s.contains("ut_pex"));
        assert!(s.contains("6881"));
    }

    #[test]
    fn test_ut_pex_build_parse() {
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6881);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 6882);

        let msg = UtPexMessage::build(&[addr1, addr2], &[]);
        let parsed = UtPexMessage::parse(&msg).unwrap();
        assert_eq!(parsed.added.len(), 2);
        assert_eq!(parsed.added[0], addr1);
        assert_eq!(parsed.added[1], addr2);
    }

    #[test]
    fn test_compact_peers_encode_decode() {
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6881);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 6882);

        let encoded = UtPexMessage::encode_compact_peers(&[addr1, addr2]);
        assert_eq!(encoded.len(), 12);

        let decoded = UtPexMessage::decode_compact_peers(&encoded);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], addr1);
        assert_eq!(decoded[1], addr2);
    }
}
