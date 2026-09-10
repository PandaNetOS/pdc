//! 联邦网络二进制帧协议
//!
//! 帧格式：`[4字节大端长度][1字节消息类型][payload]`
//! payload 使用 bincode 序列化。

use std::net::SocketAddr;
use serde::{Deserialize, Serialize};

use crate::federation::node_id::NodeAddress;

/// 消息类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MessageType {
    /// 握手问候
    Hello = 0,
    /// 握手确认
    HelloAck = 1,
    /// 心跳请求
    Ping = 2,
    /// 心跳响应
    Pong = 3,
    /// 请求节点列表
    GetNodes = 4,
    /// 节点列表响应
    Nodes = 5,
    /// 节点交换（PEX）
    ExchangeNodes = 6,
    /// 打洞信令
    Signaling = 7,
    /// 中继设置
    RelaySetup = 8,
    /// 中继数据
    RelayData = 9,
    /// Gossip 批量消息
    GossipBatch = 10,
    /// Merkle 摘要
    MerkleDigest = 11,
    /// Merkle 请求
    MerkleRequest = 12,
    /// 同步批量
    SyncBatch = 13,
    /// 断开通知
    Goodbye = 14,
    /// Merkle 修复数据
    MerkleRepair = 15,
    /// 全量同步开始
    FullSyncStart = 16,
    /// 全量同步批次
    FullSyncBatch = 17,
    /// 全量同步确认
    FullSyncAck = 18,
    /// 全量同步完成
    FullSyncComplete = 19,
    /// Gossip 批量合并帧（多个 GossipBatch 合并为一个大帧发送，减少网络往返）。
    /// 假设对端支持此消息类型（当前所有对端同版本），未来需加能力协商。
    GossipBatchBulk = 20,
    /// 全量同步请求（纯对等拉取：新节点向选中的数据源请求全量数据，
    /// 数据源收到后才推送，否则不主动发，避免多节点重复推送）。
    FullSyncRequest = 21,
}

impl MessageType {
    /// 从 u8 转换
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(MessageType::Hello),
            1 => Some(MessageType::HelloAck),
            2 => Some(MessageType::Ping),
            3 => Some(MessageType::Pong),
            4 => Some(MessageType::GetNodes),
            5 => Some(MessageType::Nodes),
            6 => Some(MessageType::ExchangeNodes),
            7 => Some(MessageType::Signaling),
            8 => Some(MessageType::RelaySetup),
            9 => Some(MessageType::RelayData),
            10 => Some(MessageType::GossipBatch),
            11 => Some(MessageType::MerkleDigest),
            12 => Some(MessageType::MerkleRequest),
            13 => Some(MessageType::SyncBatch),
            14 => Some(MessageType::Goodbye),
            15 => Some(MessageType::MerkleRepair),
            16 => Some(MessageType::FullSyncStart),
            17 => Some(MessageType::FullSyncBatch),
            18 => Some(MessageType::FullSyncAck),
            19 => Some(MessageType::FullSyncComplete),
            20 => Some(MessageType::GossipBatchBulk),
            21 => Some(MessageType::FullSyncRequest),
            _ => None,
        }
    }

    /// 转为 u8
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}

/// 握手消息（阶段2：Ed25519 签名认证）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloMessage {
    /// 发送方节点 ID
    pub node_id: [u8; 20],
    /// 发送方 Ed25519 公钥
    pub public_key: [u8; 32],
    /// 发送方地址列表
    pub addresses: Vec<NodeAddress>,
    /// 协议版本
    pub version: u32,
    /// 是否为中继节点
    pub is_relay: bool,
    /// 对其余字段的 Ed25519 签名
    #[serde(with = "serde_big_array::BigArray")]
    pub signature: [u8; 64],
}

impl HelloMessage {
    /// 构造并签名 HelloMessage
    pub fn sign_and_build(
        identity: &crate::federation::node_id::NodeIdentity,
        addresses: Vec<NodeAddress>,
        version: u32,
        is_relay: bool,
    ) -> Self {
        let public_key = identity.public_key_bytes();
        let mut msg = Self {
            node_id: identity.node_id.0,
            public_key,
            addresses,
            version,
            is_relay,
            signature: [0u8; 64],
        };
        // 签名：先将签名字段置零，序列化后签名
        let data = Self::signature_payload(&msg);
        let sig = identity.sign(&data);
        msg.signature = sig.to_bytes();
        msg
    }

    /// 构造签名负载（node_id + public_key + addresses + version + is_relay）
    fn signature_payload(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.node_id);
        data.extend_from_slice(&self.public_key);
        // 序列化 addresses（bincode）
        if let Ok(addr_bytes) = bincode::serialize(&self.addresses) {
            data.extend_from_slice(&(addr_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(&addr_bytes);
        } else {
            data.extend_from_slice(&0u32.to_be_bytes());
        }
        data.extend_from_slice(&self.version.to_le_bytes());
        data.push(if self.is_relay { 1 } else { 0 });
        data
    }

    /// 验证签名
    pub fn verify_signature(&self) -> bool {
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};
        let pk = match VerifyingKey::from_bytes(&self.public_key) {
            Ok(pk) => pk,
            Err(_) => return false,
        };
        let data = self.signature_payload();
        let sig = Signature::from_bytes(&self.signature);
        pk.verify(&data, &sig).is_ok()
    }
}

/// 打洞信令消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalingMessage {
    /// 会话 ID
    pub session_id: u64,
    /// 发起方节点 ID
    pub from_node: [u8; 20],
    /// 目标方节点 ID
    pub to_node: [u8; 20],
    /// 发起方公网地址
    pub from_addr: Option<SocketAddr>,
    /// 发起方 NAT 类型
    pub nat_type: Option<String>,
    /// 动作：0=Request, 1=Response, 2=Punch, 3=Success, 4=Failed
    pub action: u8,
}

/// 中继建立消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelaySetupMessage {
    /// 通道 ID
    pub channel_id: u64,
    /// 目标节点 ID
    pub target_node: [u8; 20],
    /// 动作：0=Request, 1=Accept, 2=Reject, 3=Close
    pub action: u8,
}

/// 中继数据消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayDataMessage {
    /// 通道 ID
    pub channel_id: u64,
    /// 数据
    pub data: Vec<u8>,
}

/// 心跳请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingMessage {
    /// 发送时间戳（毫秒）
    pub timestamp: u64,
}

/// 心跳响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PongMessage {
    /// 回显的请求时间戳
    pub timestamp: u64,
    /// 估算的 RTT（毫秒）
    pub rtt_estimate_ms: u32,
}

/// 请求节点列表
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetNodesMessage {
    /// 请求数量
    pub count: u16,
}

/// 节点列表响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodesMessage {
    /// 节点列表
    pub nodes: Vec<NodeAddress>,
}

/// 节点交换消息（PEX）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeNodesMessage {
    /// 节点列表
    pub nodes: Vec<NodeAddress>,
}

/// 同步条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncEntry {
    /// 条目标识（如 IP:port 字节、infohash 等）
    pub key: Vec<u8>,
    /// 操作类型（0=新增/更新，1=删除）
    pub operation: u8,
    /// 版本号（用于 LWW 冲突解决）
    pub version: u64,
    /// 负载数据
    pub payload: Vec<u8>,
}

/// Gossip 批量消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipBatchMessage {
    /// 消息 ID
    pub msg_id: u64,
    /// 原始发起节点 ID
    pub origin: [u8; 20],
    /// 仓库类型（1=Node, 2=Peer, 3=Infohash, 4=Tracker）
    pub repo_type: u8,
    /// 同步条目
    pub entries: Vec<SyncEntry>,
    /// 时间戳
    pub timestamp: u64,
    /// 本地缓存的序列化大小（不含帧头），submit 时计算一次，发送时直接读取。
    /// 不参与线网序列化（serde skip），仅用于限流和 bulk 大小统计。
    #[serde(skip)]
    pub serialized_size: u64,
}

/// Gossip 批量合并帧：将多个 GossipBatch 合并为一个大帧发送，
/// 减少网络往返次数和序列化/反序列化开销。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipBatchBulkMessage {
    /// 合并的多个 batch
    pub batches: Vec<GossipBatchMessage>,
}

/// 同步批量消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncBatchMessage {
    /// 仓库类型（1=Node, 2=Peer, 3=Infohash, 4=Tracker）
    pub repo_type: u8,
    /// 同步条目
    pub entries: Vec<SyncEntry>,
}

/// Merkle 摘要
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleDigestMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 分片数
    pub shard_count: u16,
    /// 各分片 Merkle 根
    pub roots: Vec<[u8; 32]>,
    /// 各分片条目数
    pub entry_counts: Vec<u32>,
}

/// Merkle 分片请求（对账发现差异后，请求指定分片的全量条目）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleRequestMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 请求的分片索引列表
    pub shards: Vec<u16>,
}

/// Merkle 修复数据（响应分片请求，携带指定分片的所有条目）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleRepairMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 修复的同步条目
    pub entries: Vec<SyncEntry>,
}

/// 全量同步开始
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullSyncStartMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 总条目数
    pub total_entries: u64,
}

/// 全量同步批次
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullSyncBatchMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 同步条目
    pub entries: Vec<SyncEntry>,
    /// 批次序号
    pub seq: u64,
}

/// 全量同步确认
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullSyncAckMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 已确认的批次序号
    pub seq: u64,
}

/// 全量同步完成
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullSyncCompleteMessage {
    /// 仓库类型
    pub repo_type: u8,
}

/// 全量同步请求（纯对等拉取）
///
/// 新节点（数据较少者）连接多个对等节点后，按「数据最完整 + 延迟最低」
/// 选出唯一数据源，仅向其发送本消息；数据源收到后才推送全量数据。
/// 请求方携带本地各 repo 的条目数，供数据源判断是否需要推送
/// （避免新节点向老节点无谓推送 / 老节点向新节点空拉）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FullSyncRequestMessage {
    /// 发起方本地各 repo 的总条目数（顺序与 repo_type::NODE/PEER/INFOHASH/TRACKER 一致），
    /// 0 表示未知/为空。
    pub local_entry_counts: Vec<u32>,
}

/// 仓库类型常量
pub mod repo_type {
    /// Node 仓库
    pub const NODE: u8 = 1;
    /// Peer 仓库
    pub const PEER: u8 = 2;
    /// Infohash 仓库
    pub const INFOHASH: u8 = 3;
    /// Tracker 仓库
    pub const TRACKER: u8 = 4;
}

/// 操作类型常量
pub mod operation {
    /// 新增/更新
    pub const UPSERT: u8 = 0;
    /// 删除
    pub const DELETE: u8 = 1;
}

/// 帧头大小：4 字节长度 + 1 字节类型
pub const FRAME_HEADER_SIZE: usize = 5;

/// 最大帧大小（16 MB，防止恶意大包）
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// 编码消息为完整帧
///
/// 帧格式：`[4字节大端长度(含类型字节)][1字节类型][bincode payload]`
pub fn encode_message<T: Serialize>(msg_type: MessageType, msg: &T) -> anyhow::Result<Vec<u8>> {
    let payload = bincode::serialize(msg).map_err(|e| anyhow::anyhow!("bincode 序列化失败: {}", e))?;
    let total_len = 1 + payload.len(); // 1 字节类型 + payload
    if total_len > MAX_FRAME_SIZE {
        anyhow::bail!("帧过大: {} 字节 > {} 字节", total_len, MAX_FRAME_SIZE);
    }
    let mut frame = Vec::with_capacity(FRAME_HEADER_SIZE + payload.len());
    frame.extend_from_slice(&(total_len as u32).to_be_bytes());
    frame.push(msg_type.as_u8());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// 解码帧头，返回消息类型和 payload 切片
///
/// `data` 必须包含完整帧（至少 FRAME_HEADER_SIZE 字节）。
/// 返回 `(消息类型, payload 切片)`。
pub fn decode_frame(data: &[u8]) -> anyhow::Result<(MessageType, &[u8])> {
    if data.len() < FRAME_HEADER_SIZE {
        anyhow::bail!("数据不足，需要至少 {} 字节，实际 {}", FRAME_HEADER_SIZE, data.len());
    }
    let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if length == 0 || length > MAX_FRAME_SIZE {
        anyhow::bail!("无效帧长度: {}", length);
    }
    if data.len() < FRAME_HEADER_SIZE + length - 1 {
        anyhow::bail!(
            "数据不完整，需要 {} 字节，实际 {}",
            FRAME_HEADER_SIZE + length - 1,
            data.len()
        );
    }
    let msg_type = MessageType::from_u8(data[4])
        .ok_or_else(|| anyhow::anyhow!("未知消息类型: {}", data[4]))?;
    let payload = &data[FRAME_HEADER_SIZE..FRAME_HEADER_SIZE + length - 1];
    Ok((msg_type, payload))
}

/// 检查缓冲区中是否有完整帧，返回完整帧的总字节数（含帧头）
///
/// 如果缓冲区不足，返回 None。
pub fn frame_size_in_buffer(data: &[u8]) -> Option<usize> {
    if data.len() < 4 {
        return None;
    }
    let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if length == 0 || length > MAX_FRAME_SIZE {
        return None;
    }
    let total = FRAME_HEADER_SIZE + length - 1;
    if data.len() >= total {
        Some(total)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_type_roundtrip() {
        for i in 0..=14u8 {
            let mt = MessageType::from_u8(i).unwrap();
            assert_eq!(mt.as_u8(), i);
        }
        assert!(MessageType::from_u8(255).is_none());
    }

    #[test]
    fn test_encode_decode_ping() {
        let ping = PingMessage { timestamp: 12345 };
        let frame = encode_message(MessageType::Ping, &ping).unwrap();
        assert_eq!(frame.len(), FRAME_HEADER_SIZE + bincode::serialized_size(&ping).unwrap() as usize);

        let (msg_type, payload) = decode_frame(&frame).unwrap();
        assert_eq!(msg_type, MessageType::Ping);
        let decoded: PingMessage = bincode::deserialize(payload).unwrap();
        assert_eq!(decoded.timestamp, 12345);
    }

    #[test]
    fn test_encode_decode_hello() {
        use crate::federation::node_id::NodeIdentity;
        let identity = NodeIdentity::generate();
        let hello = HelloMessage::sign_and_build(&identity, vec![], 1, false);
        assert!(hello.verify_signature());

        let frame = encode_message(MessageType::Hello, &hello).unwrap();
        let (msg_type, payload) = decode_frame(&frame).unwrap();
        assert_eq!(msg_type, MessageType::Hello);
        let decoded: HelloMessage = bincode::deserialize(payload).unwrap();
        assert_eq!(decoded.node_id, identity.node_id.0);
        assert_eq!(decoded.public_key, identity.public_key_bytes());
        assert_eq!(decoded.version, 1);
        assert!(!decoded.is_relay);
        assert!(decoded.verify_signature());
    }

    #[test]
    fn test_hello_signature_tamper_detection() {
        use crate::federation::node_id::NodeIdentity;
        let identity = NodeIdentity::generate();
        let mut hello = HelloMessage::sign_and_build(&identity, vec![], 1, false);
        assert!(hello.verify_signature());
        // 篡改 node_id
        hello.node_id = [0xff; 20];
        assert!(!hello.verify_signature());
    }

    #[test]
    fn test_signaling_message_serde() {
        let msg = SignalingMessage {
            session_id: 42,
            from_node: [1; 20],
            to_node: [2; 20],
            from_addr: Some("1.2.3.4:6885".parse().unwrap()),
            nat_type: Some("FullCone".to_string()),
            action: 0,
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: SignalingMessage = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.session_id, 42);
        assert_eq!(decoded.from_node, [1; 20]);
        assert_eq!(decoded.action, 0);
        assert_eq!(decoded.from_addr, Some("1.2.3.4:6885".parse().unwrap()));
    }

    #[test]
    fn test_encode_decode_sync_batch() {
        let batch = SyncBatchMessage {
            repo_type: repo_type::NODE,
            entries: vec![
                SyncEntry {
                    key: b"127.0.0.1:6885".to_vec(),
                    operation: operation::UPSERT,
                    version: 1,
                    payload: vec![1, 2, 3],
                },
                SyncEntry {
                    key: b"10.0.0.1:6885".to_vec(),
                    operation: operation::UPSERT,
                    version: 2,
                    payload: vec![4, 5, 6],
                },
            ],
        };
        let frame = encode_message(MessageType::SyncBatch, &batch).unwrap();
        let (msg_type, payload) = decode_frame(&frame).unwrap();
        assert_eq!(msg_type, MessageType::SyncBatch);
        let decoded: SyncBatchMessage = bincode::deserialize(payload).unwrap();
        assert_eq!(decoded.repo_type, 1);
        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.entries[0].key, b"127.0.0.1:6885");
        assert_eq!(decoded.entries[1].version, 2);
    }

    #[test]
    fn test_decode_frame_insufficient_data() {
        assert!(decode_frame(&[0, 0, 0]).is_err());
        assert!(decode_frame(&[0, 0, 0, 10, 0]).is_err()); // 声称10字节但只有1字节payload
    }

    #[test]
    fn test_decode_frame_invalid_length() {
        let data = [0xff, 0xff, 0xff, 0xff, 0]; // 超大长度
        assert!(decode_frame(&data).is_err());
    }

    #[test]
    fn test_frame_size_in_buffer() {
        let ping = PingMessage { timestamp: 1 };
        let frame = encode_message(MessageType::Ping, &ping).unwrap();
        let frame_len = frame.len();

        // 完整帧
        assert_eq!(frame_size_in_buffer(&frame), Some(frame_len));
        // 不足4字节
        assert_eq!(frame_size_in_buffer(&frame[..3]), None);
        // 有帧头但数据不足
        assert_eq!(frame_size_in_buffer(&frame[..frame_len - 1]), None);
        // 多帧拼接
        let mut double = frame.clone();
        double.extend_from_slice(&frame);
        assert_eq!(frame_size_in_buffer(&double), Some(frame_len));
    }

    #[test]
    fn test_max_frame_size() {
        let big = vec![0u8; MAX_FRAME_SIZE + 1];
        let result = encode_message(MessageType::Pong, &big);
        assert!(result.is_err());
    }
}
