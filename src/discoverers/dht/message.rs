//! DHT 消息编解码（BEP 5）
//!
//! DHT 消息是 bencode 编码的字典，包含：
//! - `t`: transaction_id（2 字节，用于匹配请求/响应）
//! - `y`: 消息类型（q=query, r=response, e=error）
//! - `q`: 查询方法（ping/find_node/get_peers/announce_peer）
//! - `a`: 查询参数字典
//! - `r`: 响应字典
//! - `e`: 错误信息 [code, message]

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use serde_bencode::from_bytes;
use serde_bencode::value::Value as BencodeValue;

use crate::types::Infohash;

/// DHT 查询方法
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryMethod {
    Ping,
    FindNode,
    GetPeers,
    AnnouncePeer,
}

impl QueryMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            QueryMethod::Ping => "ping",
            QueryMethod::FindNode => "find_node",
            QueryMethod::GetPeers => "get_peers",
            QueryMethod::AnnouncePeer => "announce_peer",
        }
    }
}

/// DHT 节点信息（compact node info: 20字节ID + 4字节IP + 2字节端口）
#[derive(Debug, Clone)]
pub struct DhtNode {
    pub id: [u8; 20],
    pub addr: SocketAddr,
}

impl DhtNode {
    /// 从 compact node info 字节解析
    pub fn from_compact(data: &[u8]) -> Option<Self> {
        if data.len() < 26 {
            return None;
        }
        let mut id = [0u8; 20];
        id.copy_from_slice(&data[0..20]);
        let ip = Ipv4Addr::new(data[20], data[21], data[22], data[23]);
        let port = u16::from_be_bytes([data[24], data[25]]);
        Some(DhtNode {
            id,
            addr: SocketAddr::new(IpAddr::V4(ip), port),
        })
    }

    /// 解析 compact nodes 字符串（多个节点拼接）
    pub fn parse_compact_nodes(data: &[u8]) -> Vec<DhtNode> {
        let mut nodes = vec![];
        let (chunks, _) = data.as_chunks::<26>();
        for chunk in chunks {
            if let Some(node) = DhtNode::from_compact(chunk) {
                nodes.push(node);
            }
        }
        nodes
    }
}

/// 解析 compact peers（每 6 字节：4字节IP + 2字节端口）
pub fn parse_compact_peers(data: &[u8]) -> Vec<SocketAddr> {
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

/// get_peers 响应结果
#[derive(Debug, Clone, Default)]
pub struct GetPeersResponse {
    /// 响应节点的 ID
    pub node_id: [u8; 20],
    /// token（用于后续 announce_peer）
    pub token: Option<Vec<u8>>,
    /// 直接返回的 peer 列表
    pub values: Vec<SocketAddr>,
    /// 更近的节点列表（需要继续查询）
    pub nodes: Vec<DhtNode>,
}

/// DHT 消息编解码工具
pub struct DhtMessage;

impl DhtMessage {
    /// 构建 get_peers 查询
    ///
    /// # 参数
    /// - `transaction_id`: 2 字节事务 ID
    /// - `node_id`: 自己的节点 ID（20 字节）
    /// - `info_hash`: 要查询的 infohash（20 字节）
    pub fn build_get_peers(
        transaction_id: &[u8; 2],
        node_id: &[u8; 20],
        info_hash: &Infohash,
    ) -> Vec<u8> {
        // 手动构造 bencode，避免复杂的 serde 结构
        // d1:ad2:id20:<node_id>9:info_hash20:<info_hash>e1:q9:get_peers1:t2:<tid>1:y1:qe
        let mut buf = Vec::new();
        buf.extend_from_slice(b"d1:ad2:id20:");
        buf.extend_from_slice(node_id);
        buf.extend_from_slice(b"9:info_hash20:");
        buf.extend_from_slice(info_hash);
        buf.extend_from_slice(b"e1:q9:get_peers1:t2:");
        buf.extend_from_slice(transaction_id);
        buf.extend_from_slice(b"1:y1:qe");
        buf
    }

    /// 构建 find_node 查询
    pub fn build_find_node(
        transaction_id: &[u8; 2],
        node_id: &[u8; 20],
        target_id: &[u8; 20],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"d1:ad2:id20:");
        buf.extend_from_slice(node_id);
        buf.extend_from_slice(b"9:target20:");
        buf.extend_from_slice(target_id);
        buf.extend_from_slice(b"e1:q9:find_node1:t2:");
        buf.extend_from_slice(transaction_id);
        buf.extend_from_slice(b"1:y1:qe");
        buf
    }

    /// 构建 ping 查询
    pub fn build_ping(transaction_id: &[u8; 2], node_id: &[u8; 20]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"d1:ad2:id20:");
        buf.extend_from_slice(node_id);
        buf.extend_from_slice(b"e1:q4:ping1:t2:");
        buf.extend_from_slice(transaction_id);
        buf.extend_from_slice(b"1:y1:qe");
        buf
    }

    /// 解析 DHT 查询消息
    ///
    /// 返回 (transaction_id, 查询方法, infohash(如果有))
    pub fn parse_query(data: &[u8]) -> Option<(Vec<u8>, QueryMethod, Option<Infohash>)> {
        let value: BencodeValue = from_bytes(data).ok()?;
        let dict = value.as_dict()?;

        // 检查是查询
        let y = dict.get(b"y".as_slice())?.as_bytes()?;
        if y != b"q" {
            return None;
        }

        // transaction_id
        let tid = dict
            .get(b"t".as_slice())
            .and_then(|v| v.as_bytes())
            .cloned()
            .unwrap_or_default();

        // 查询方法
        let q = dict.get(b"q".as_slice())?.as_bytes()?;
        let method = match q.as_slice() {
            b"ping" => QueryMethod::Ping,
            b"find_node" => QueryMethod::FindNode,
            b"get_peers" => QueryMethod::GetPeers,
            b"announce_peer" => QueryMethod::AnnouncePeer,
            _ => return None,
        };

        // 提取 infohash（get_peers 和 announce_peer 有）
        let mut infohash = None;
        if let Some(args) = dict.get(b"a".as_slice()).and_then(|v| v.as_dict()) {
            if let Some(ih_bytes) = args.get(b"info_hash".as_slice()).and_then(|v| v.as_bytes()) {
                if ih_bytes.len() == 20 {
                    let mut ih = [0u8; 20];
                    ih.copy_from_slice(ih_bytes);
                    infohash = Some(ih);
                }
            }
        }

        Some((tid, method, infohash))
    }

    /// 构建 ping 响应
    pub fn build_ping_response(transaction_id: &[u8], node_id: &[u8; 20]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"d1:rd2:id20:");
        buf.extend_from_slice(node_id);
        buf.extend_from_slice(b"e1:t");
        buf.extend_from_slice(format!("{}:", transaction_id.len()).as_bytes());
        buf.extend_from_slice(transaction_id);
        buf.extend_from_slice(b"1:y1:re");
        buf
    }

    /// 构建 find_node 响应（返回空节点列表）
    pub fn build_find_node_response(transaction_id: &[u8], node_id: &[u8; 20]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"d1:rd2:id20:");
        buf.extend_from_slice(node_id);
        buf.extend_from_slice(b"5:nodes0:e1:t");
        buf.extend_from_slice(format!("{}:", transaction_id.len()).as_bytes());
        buf.extend_from_slice(transaction_id);
        buf.extend_from_slice(b"1:y1:re");
        buf
    }

    /// 构建 get_peers 响应（返回空节点列表，无 peers）
    pub fn build_get_peers_response(
        transaction_id: &[u8],
        node_id: &[u8; 20],
        token: &[u8],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"d1:rd2:id20:");
        buf.extend_from_slice(node_id);
        buf.extend_from_slice(b"5:nodes0:e5:token");
        buf.extend_from_slice(format!("{}:", token.len()).as_bytes());
        buf.extend_from_slice(token);
        buf.extend_from_slice(b"e1:t");
        buf.extend_from_slice(format!("{}:", transaction_id.len()).as_bytes());
        buf.extend_from_slice(transaction_id);
        buf.extend_from_slice(b"1:y1:re");
        buf
    }

    /// 构建 announce_peer 查询
    ///
    /// # 参数
    /// - `transaction_id`: 事务 ID
    /// - `node_id`: 自己的节点 ID
    /// - `info_hash`: 要 announce 的 infohash
    /// - `port`: 监听端口
    /// - `token`: 从 get_peers 响应中获取的 token
    pub fn build_announce_peer(
        transaction_id: &[u8],
        node_id: &[u8; 20],
        info_hash: &Infohash,
        port: u16,
        token: &[u8],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"d1:ad2:id20:");
        buf.extend_from_slice(node_id);
        buf.extend_from_slice(b"12:implied_porti0e9:info_hash20:");
        buf.extend_from_slice(info_hash);
        buf.extend_from_slice(b"4:porti");
        buf.extend_from_slice(port.to_string().as_bytes());
        buf.extend_from_slice(b"e5:token");
        buf.extend_from_slice(format!("{}:", token.len()).as_bytes());
        buf.extend_from_slice(token);
        buf.extend_from_slice(b"e1:q13:announce_peer1:t");
        buf.extend_from_slice(format!("{}:", transaction_id.len()).as_bytes());
        buf.extend_from_slice(transaction_id);
        buf.extend_from_slice(b"1:y1:qe");
        buf
    }

    /// 解析 get_peers 响应
    ///
    /// 返回 (transaction_id, GetPeersResponse)
    pub fn parse_get_peers_response(data: &[u8]) -> Option<([u8; 2], GetPeersResponse)> {
        let value: BencodeValue = from_bytes(data).ok()?;
        let dict = value.as_dict()?;

        // 检查是响应
        let y = dict.get(b"y".as_slice())?.as_bytes()?;
        if y != b"r" {
            // 可能是 error，返回 None
            return None;
        }

        // transaction_id
        let t = dict.get(b"t".as_slice())?.as_bytes()?;
        let mut tid = [0u8; 2];
        if t.len() >= 2 {
            tid.copy_from_slice(&t[0..2]);
        }

        // 响应字典
        let r = dict.get(b"r".as_slice())?.as_dict()?;

        // node_id
        let mut node_id = [0u8; 20];
        if let Some(id_bytes) = r.get(b"id".as_slice()).and_then(|v| v.as_bytes()) {
            if id_bytes.len() == 20 {
                node_id.copy_from_slice(id_bytes);
            }
        }

        // token
        let token = r
            .get(b"token".as_slice())
            .and_then(|v| v.as_bytes())
            .map(|b| b.to_vec());

        let mut result = GetPeersResponse {
            node_id,
            token,
            values: vec![],
            nodes: vec![],
        };

        // values（peer 列表，列表 of compact peer）
        if let Some(values) = r.get(b"values".as_slice()).and_then(|v| v.as_list()) {
            for item in values {
                if let Some(peer_bytes) = item.as_bytes() {
                    result.values.extend(parse_compact_peers(peer_bytes));
                }
            }
        }

        // nodes（compact node info 字符串）
        if let Some(nodes_bytes) = r.get(b"nodes".as_slice()).and_then(|v| v.as_bytes()) {
            result.nodes = DhtNode::parse_compact_nodes(nodes_bytes);
        }

        Some((tid, result))
    }

    /// 解析 find_node 响应，返回节点列表
    pub fn parse_find_node_response(data: &[u8]) -> Option<([u8; 2], Vec<DhtNode>)> {
        let value: BencodeValue = from_bytes(data).ok()?;
        let dict = value.as_dict()?;

        let y = dict.get(b"y".as_slice())?.as_bytes()?;
        if y != b"r" {
            return None;
        }

        let t = dict.get(b"t".as_slice())?.as_bytes()?;
        let mut tid = [0u8; 2];
        if t.len() >= 2 {
            tid.copy_from_slice(&t[0..2]);
        }

        let r = dict.get(b"r".as_slice())?.as_dict()?;
        let nodes = r
            .get(b"nodes".as_slice())
            .and_then(|v| v.as_bytes())
            .map(|b| DhtNode::parse_compact_nodes(b))
            .unwrap_or_default();

        Some((tid, nodes))
    }

    /// 计算两个 20 字节 ID 的 XOR 距离
    pub fn xor_distance(a: &[u8; 20], b: &[u8; 20]) -> [u8; 20] {
        let mut result = [0u8; 20];
        for i in 0..20 {
            result[i] = a[i] ^ b[i];
        }
        result
    }

    /// 比较两个距离，返回 true 如果 a < b
    pub fn distance_less(a: &[u8; 20], b: &[u8; 20]) -> bool {
        for i in 0..20 {
            if a[i] < b[i] {
                return true;
            }
            if a[i] > b[i] {
                return false;
            }
        }
        false
    }
}

/// BencodeValue 扩展 trait，方便提取字段
trait BencodeExt {
    fn as_dict(&self) -> Option<&HashMap<Vec<u8>, BencodeValue>>;
    fn as_list(&self) -> Option<&Vec<BencodeValue>>;
    fn as_bytes(&self) -> Option<&Vec<u8>>;
    #[allow(dead_code)]
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
    fn test_build_ping() {
        let tid = [0x01, 0x02];
        let node_id = [0u8; 20];
        let msg = DhtMessage::build_ping(&tid, &node_id);
        let s = String::from_utf8_lossy(&msg);
        assert!(s.contains("4:ping"));
        assert!(s.contains("1:y1:q"));
    }

    #[test]
    fn test_build_get_peers() {
        let tid = [0x01, 0x02];
        let node_id = [0u8; 20];
        let infohash = [1u8; 20];
        let msg = DhtMessage::build_get_peers(&tid, &node_id, &infohash);
        let s = String::from_utf8_lossy(&msg);
        assert!(s.contains("9:get_peers"));
        assert!(s.contains("9:info_hash"));
    }

    #[test]
    fn test_parse_compact_peers() {
        let data = [127, 0, 0, 1, 0x1A, 0xE1, 192, 168, 1, 1, 0x1A, 0xE2];
        let peers = parse_compact_peers(&data);
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].to_string(), "127.0.0.1:6881");
        assert_eq!(peers[1].to_string(), "192.168.1.1:6882");
    }

    #[test]
    fn test_dht_node_from_compact() {
        let mut data = vec![0u8; 26];
        data[0] = 1; // node_id 第一字节
        data[20] = 127;
        data[21] = 0;
        data[22] = 0;
        data[23] = 1;
        data[24] = 0x1A;
        data[25] = 0xE1;

        let node = DhtNode::from_compact(&data).unwrap();
        assert_eq!(node.id[0], 1);
        assert_eq!(node.addr.to_string(), "127.0.0.1:6881");
    }

    #[test]
    fn test_xor_distance() {
        let a = [0u8; 20];
        let b = [0xFFu8; 20];
        let dist = DhtMessage::xor_distance(&a, &b);
        assert_eq!(dist, [0xFFu8; 20]);

        let c = [0u8; 20];
        let dist2 = DhtMessage::xor_distance(&a, &c);
        assert_eq!(dist2, [0u8; 20]);
    }

    #[test]
    fn test_distance_less() {
        let a = [0u8; 20];
        let mut b = [0u8; 20];
        b[19] = 1;
        assert!(DhtMessage::distance_less(&a, &b));
        assert!(!DhtMessage::distance_less(&b, &a));
    }
}
