//! 联邦网络二进制帧协议
//!
//! 帧格式：`[4字节大端长度][1字节消息类型][payload]`
//! payload 使用 bincode 序列化。

use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use crate::federation::node_id::{NodeAddress, NodeId};

/// 当前 UNIX 毫秒时间戳
fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

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
    /// 差量同步请求（Merkle 差异≥20%时触发，携带本地 Merkle 摘要，对端只推差异分片）
    DiffSyncRequest = 21,
    /// 节点信息（握手后立即双向发送，携带本地各 repo 条目数，
    /// 供对端在数据源选择时判断哪个节点数据最完整）。
    PeerInfo = 22,
    // 编号 23 / 24 / 25：已废弃并移除。
    // 原为 Push-Pull Gossip 的 GossipDigest / GossipPullRequest / GossipPullResponse；
    // P1-9 移除该同步路径后不再收发（反熵改由分层 Merkle + Range 接管），
    // 编号在此保留空位，避免与历史对端的编号语义错位。
    /// 实时 peer 查询请求（announce 时本地 peer 不足，向联邦节点查询）
    PeerQueryRequest = 26,
    /// 实时 peer 查询响应
    PeerQueryResponse = 27,
    /// DiffSync key 列表请求（数据服务器 → 请求方）：携带差异分片的 key 列表分片。
    /// 仅协议版本 >=2 的对端使用；旧版本对端走原始全量推送。
    DiffSyncKeyRequest = 28,
    /// DiffSync key 列表响应（请求方 → 数据服务器）：返回请求方缺失的 key 列表分片。
    DiffSyncKeyResponse = 29,
    /// 分层 Merkle 层级请求：请求某层某分片的子哈希列表（协议版本 >=3）。
    /// 对比流程：L0根 → L1一级分片(256) → L2二级分片(65536)，最多3轮定位差异。
    MerkleLevelRequest = 30,
    /// 分层 Merkle 层级响应：返回请求层级的子哈希列表（协议版本 >=3）。
    MerkleLevelResponse = 31,
    /// 分片同步批次：携带差异 L2 二级分片的数据条目（协议版本 >=3）。
    /// 替代旧版全量 key 交换，只同步差异 L2 分片，支持并行+断点续传。
    ShardSyncBatch = 32,
    /// 分片同步确认：接收方确认收到指定批次（协议版本 >=3）。
    ShardSyncAck = 33,
    /// 分片同步完成：发送方通知所有差异 L2 分片已同步完毕（协议版本 >=3）。
    ShardSyncComplete = 34,
    /// 分片同步 hash 列表（数据服务器 → 请求方）：携带某 L2 分片内所有条目的
    /// (key, data_hash) 列表。请求方对比本地 DB 后回传缺失 key（ShardSyncMissing），
    /// 数据服务器只推送真正缺失的条目，把重复率从 ~97% 降到 <5%。
    ShardSyncHashList = 35,
    /// 分片同步缺失 key 列表（请求方 → 数据服务器）：请求方对比本地 DB 后，
    /// 回传本地缺失的 key 列表，数据服务器只推送这些 key 的完整数据。
    ShardSyncMissing = 36,
    /// P1-3：增量（delta）拉取请求（B → A）：`OpsRequest { repo, since_seq, limit }`。
    /// 仅当 `federation.delta_sync_enabled = true` 时发送；对端不支持时该消息被丢弃（回退反熵）。
    OpsRequest = 37,
    /// P1-3：增量（delta）拉取响应（A → B）：`OpsBatch { repo, ops, next_seq, has_more }`。
    OpsBatch = 38,
    /// P1-4：Range-based 反熵请求（B → A）：`RangeReconcile { repo, lo, hi, digest, depth }`。
    /// 有序区间 `[lo, hi)` 的摘要对账；对端回摘要 + 分界点（或叶级行指纹）。
    /// 仅当 `federation.range_reconcile_enabled = true` 时发送；对端 < v5 不支持时退化为既有分层 Merkle。
    RangeReconcileRequest = 39,
    /// P1-4：Range-based 反熵响应（A → B）。
    RangeReconcileResponse = 40,
    /// P2-1：bootstrap 清单请求（B → A）：请求指定 repo 的全量分块清单。
    /// 仅当 `federation.bootstrap_enabled = true` 且对端 >= v6 时发送。
    BootstrapManifestRequest = 41,
    /// P2-1：bootstrap 清单响应（A → B）：携带 `BootstrapManifest`。
    BootstrapManifestResponse = 42,
    /// P2-1：bootstrap 分块请求（B → A）：请求清单中第 `index` 块的数据。
    BootstrapChunkRequest = 43,
    /// P2-1：bootstrap 分块响应（A → B）：携带该块完整条目。
    BootstrapChunkResponse = 44,
    /// P1-4：Range 反熵推送（A → B）：发现本地多后，直接推送本地多的节点数据给对端。
    RangeReconcilePush = 45,
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
            21 => Some(MessageType::DiffSyncRequest),
            22 => Some(MessageType::PeerInfo),
            // 23 / 24 / 25：已废弃（原 Push-Pull Gossip），不再映射
            26 => Some(MessageType::PeerQueryRequest),
            27 => Some(MessageType::PeerQueryResponse),
            28 => Some(MessageType::DiffSyncKeyRequest),
            29 => Some(MessageType::DiffSyncKeyResponse),
            30 => Some(MessageType::MerkleLevelRequest),
            31 => Some(MessageType::MerkleLevelResponse),
            32 => Some(MessageType::ShardSyncBatch),
            33 => Some(MessageType::ShardSyncAck),
            34 => Some(MessageType::ShardSyncComplete),
            35 => Some(MessageType::ShardSyncHashList),
            36 => Some(MessageType::ShardSyncMissing),
            37 => Some(MessageType::OpsRequest),
            38 => Some(MessageType::OpsBatch),
            39 => Some(MessageType::RangeReconcileRequest),
            40 => Some(MessageType::RangeReconcileResponse),
            41 => Some(MessageType::BootstrapManifestRequest),
            42 => Some(MessageType::BootstrapManifestResponse),
            43 => Some(MessageType::BootstrapChunkRequest),
            44 => Some(MessageType::BootstrapChunkResponse),
            45 => Some(MessageType::RangeReconcilePush),
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
    /// 发送时间戳（UNIX 毫秒），用于重放窗口校验
    pub timestamp_ms: u64,
    /// 随机 nonce，用于重放去重
    pub nonce: [u8; 16],
    /// 对其余字段的 Ed25519 签名
    #[serde(with = "serde_big_array::BigArray")]
    pub signature: [u8; 64],
}

impl HelloMessage {
    /// Hello 可接受的时间偏差窗口（毫秒）：超出则视为过期/未来消息，拒绝以防重放。
    pub const REPLAY_WINDOW_MS: u64 = 120_000;

    /// 构造并签名 HelloMessage
    pub fn sign_and_build(
        identity: &crate::federation::node_id::NodeIdentity,
        addresses: Vec<NodeAddress>,
        version: u32,
        is_relay: bool,
    ) -> Self {
        let public_key = identity.public_key_bytes();
        let mut nonce = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut nonce);
        let mut msg = Self {
            node_id: identity.node_id.0,
            public_key,
            addresses,
            version,
            is_relay,
            timestamp_ms: now_unix_ms(),
            nonce,
            signature: [0u8; 64],
        };
        // 签名：先将签名字段置零，序列化后签名
        let data = Self::signature_payload(&msg);
        let sig = identity.sign(&data);
        msg.signature = sig.to_bytes();
        msg
    }

    /// 时间戳是否落在允许的重放窗口内
    pub fn is_fresh(&self) -> bool {
        now_unix_ms().abs_diff(self.timestamp_ms) <= Self::REPLAY_WINDOW_MS
    }

    /// 校验 node_id 是否由 public_key 派生（身份绑定），防止用自建密钥冒充任意 node_id。
    pub fn verify_identity_binding(&self) -> bool {
        NodeId::matches_public_key(&NodeId(self.node_id), &self.public_key)
    }

    /// nonce 的十六进制字符串（用于重放缓存去重键）
    pub fn nonce_hex(&self) -> String {
        let mut s = String::with_capacity(32);
        for b in &self.nonce {
            s.push_str(&format!("{:02x}", b));
        }
        s
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
        data.extend_from_slice(&self.timestamp_ms.to_le_bytes());
        data.extend_from_slice(&self.nonce);
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MerkleDigestMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 分片数
    pub shard_count: u16,
    /// 各分片 Merkle 根（热+冷合并根 combined_roots）
    pub roots: Vec<[u8; 32]>,
    /// 各分片条目数（热+冷合计）
    pub entry_counts: Vec<u32>,
    /// L0 全量根（热+冷全量数据的根哈希）。
    /// Option 用于向后兼容：旧版本节点不发送此字段，接收方为 None 时跳过 full_root 快速比较。
    #[serde(default)]
    pub full_root: Option<[u8; 32]>,
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

/// 差量同步请求（Merkle 差异≥20%时触发）
///
/// 请求方携带本地单个 repo 的 Merkle 摘要，对端对比后找出差异分片，
/// 只推送差异分片的条目（不是全量）。
/// 每个 repo 独立触发差量同步，独立并发。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiffSyncRequestMessage {
    /// 发起方本地单个 repo 的 Merkle 摘要
    pub digest: MerkleDigestMessage,
}

/// DiffSync key 列表请求（数据服务器 → 请求方，协议版本 >=2）。
///
/// 数据服务器发现差异分片后，不直接推送全部条目（重复率 ~90%），而是先把差异分片的
/// key 列表分片发给请求方。请求方对比本地 key 后回传缺失 key（DiffSyncKeyResponse），
/// 数据服务器只推送缺失条目，重复率降至 <5%。
///
/// 为避免单条消息过大，key 列表按 `DIFF_SYNC_KEY_CHUNK_SIZE` 分片发送，
/// 接收方累计直到 `is_last == true`。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiffSyncKeyRequestMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 差异分片索引列表（请求方据此从本地 DB 加载同分片 key 做对比）
    pub shards: Vec<u16>,
    /// 本片 key 列表（每个 key 为 SyncEntry.key 的原始字节）
    pub keys: Vec<Vec<u8>>,
    /// 是否为最后一片 key（请求方收到最后一片后才对比并回复）
    pub is_last: bool,
}

/// DiffSync key 列表响应（请求方 → 数据服务器，协议版本 >=2）。
///
/// 请求方对比本地 key 后，把缺失的 key 列表分片回传。同样按分片发送，
/// 数据服务器累计直到 `is_last == true`，再只加载/推送这些缺失 key 的完整条目。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiffSyncKeyResponseMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 请求方缺失的 key 列表（本片）
    pub missing_keys: Vec<Vec<u8>>,
    /// 是否为最后一片缺失 key（数据服务器收到最后一片后开始推送）
    pub is_last: bool,
}

/// DiffSync key 列表单条消息最大 key 数（约 200KB，避免超大帧）。
pub const DIFF_SYNC_KEY_CHUNK_SIZE: usize = 10000;

// ============================================================================
// 分层 Merkle 对比协议（协议版本 >=3，亿级数据架构升级）
// ============================================================================

/// 分层 Merkle 层级请求（协议版本 >=3）。
///
/// 请求对端返回指定层级的子哈希列表，用于逐层定位差异分片：
/// - level=0: 请求 L0 根哈希（parent_shard 忽略，返回 1 个哈希）
/// - level=1: 请求 L1 一级分片哈希（parent_shard 忽略，返回 256 个哈希）
/// - level=2: 请求指定 L1 下的 L2 二级分片哈希（parent_shard 指定 L1，返回 256 个哈希）
///
/// 对比流程最多 3 轮：交换根 → 不一致则交换 L1 → 对差异 L1 交换 L2 → 只同步差异 L2。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MerkleLevelRequestMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 请求层级（0=L0根, 1=L1一级分片, 2=L2二级分片）
    pub level: u8,
    /// 父分片索引（level=2 时为 L1 分片索引；level=0/1 时忽略）
    pub parent_shard: u16,
}

/// 分层 Merkle 层级响应（协议版本 >=3）。
///
/// 返回请求层级的子哈希列表和对应条目数。
/// - level=0: hashes 含 1 个根哈希
/// - level=1: hashes 含 256 个 L1 哈希
/// - level=2: hashes 含 256 个 L2 哈希（指定 L1 下）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MerkleLevelResponseMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 响应层级（0=L0根, 1=L1一级分片, 2=L2二级分片）
    pub level: u8,
    /// 父分片索引（与请求对应）
    pub parent_shard: u16,
    /// 子哈希列表（数量取决于层级：1/256/256）
    pub hashes: Vec<[u8; 32]>,
    /// 各子分片条目数（与 hashes 一一对应）
    pub entry_counts: Vec<u32>,
}

// ============================================================================
// 分片同步协议（协议版本 >=3，替代旧版全量 key 交换）
// ============================================================================

/// 分片同步批次（协议版本 >=3）。
///
/// 携带差异 L2 二级分片的数据条目。发送方按 L2 分片分组，
/// 每批可覆盖一个或多个 L2 分片，接收方独立应用并确认。
/// 支持并行同步（多个 L2 分片同时传输）、独立超时重试、断点续传。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShardSyncBatchMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 本批次覆盖的 L2 二级分片索引列表
    pub l2_shards: Vec<u32>,
    /// 同步条目
    pub entries: Vec<SyncEntry>,
    /// 批次序号（用于确认和去重）
    pub seq: u64,
    /// 是否为最后一批（发送方通知同步完成）
    pub is_last: bool,
}

/// 分片同步确认（协议版本 >=3）。
///
/// 接收方确认收到并应用指定批次，发送方据此推进窗口和记录进度。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShardSyncAckMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 已确认的批次序号
    pub seq: u64,
    /// 本批次应用的条目数
    pub applied_count: u32,
}

/// 分片同步完成（协议版本 >=3）。
///
/// 发送方通知所有差异 L2 分片已同步完毕，携带统计信息供监控和对账。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShardSyncCompleteMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 同步的 L2 分片总数
    pub total_l2_shards: u32,
    /// 同步的条目总数
    pub total_entries: u64,
    /// 总批次序号上限（接收方据此判断是否有遗漏）
    pub max_seq: u64,
}

/// 分片同步 hash 列表（数据服务器 → 请求方）。
///
/// 携带某 L2 分片内所有条目的 (key, data_hash)，用于对端对比找出缺失条目。
/// 若分片内条目数超过单条消息上限，按片发送，接收方累计直到 `is_last == true`。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShardSyncHashListMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 对应的 L2 二级分片索引（接收方据此定位本地 L1 分片做对比）
    pub l2_shard: u32,
    /// (key, data_hash) 列表
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// 是否为最后一片
    pub is_last: bool,
}

/// 分片同步缺失 key 列表（请求方 → 数据服务器）。
///
/// 请求方对比本地 DB 后，把缺失的 key 列表回传，数据服务器只推送这些 key 的完整数据。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShardSyncMissingMessage {
    /// 仓库类型
    pub repo_type: u8,
    /// 对应的 L2 二级分片索引
    pub l2_shard: u32,
    /// 请求方缺失的 key 列表
    pub missing_keys: Vec<Vec<u8>>,
    /// 是否为最后一片
    pub is_last: bool,
}

/// P1-3：单条变更操作（oplog 条目在协议上的表示）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OpEntry {
    /// oplog 全局 seq（对端以最大 seq 推进版本向量）
    pub seq: u64,
    /// true = delete（墓碑），false = upsert
    pub is_delete: bool,
    /// 条目 key（node="ip:port"，peer="<hex_ih>:<ip>:<port>"，infohash=原始字节，tracker=url）
    pub key: Vec<u8>,
    /// upsert 时携带完整 payload（与 SyncEntry.payload 同格式）；delete 时为空
    pub value: Vec<u8>,
}

/// P1-3：增量拉取请求（B → A）。成本 O(Δ)，与库总量无关。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OpsRequestMessage {
    /// 仓库类型（repo_type::NODE/PEER/INFOHASH/TRACKER）
    pub repo: u8,
    /// 请求方已同步到的 oplog seq（断点）
    pub since_seq: u64,
    /// 单批上限
    pub limit: u32,
}

/// P1-3：增量拉取响应（A → B）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OpsBatchMessage {
    /// 仓库类型
    pub repo: u8,
    /// 本批 ops（seq 升序）
    pub ops: Vec<OpEntry>,
    /// 下一断点：本批最后一条 op 的 seq；本批为空时回显请求的 since_seq
    pub next_seq: u64,
    /// 是否还有更多（true 时请求方应立即再发一次 OpsRequest）
    pub has_more: bool,
    /// F2：应答方**在该 repo 上**的 oplog 最高 seq（= 该 repo 最后一条变更的 seq）。
    ///
    /// 请求方据此算真实落后量 `server_max_seq - synced_seq`（二者同属应答方 seq 空间、
    /// 且同一 repo）。该 repo 在应答方无任何变更时为 0，此时可观测性里的 `lag_seq`
    /// 返回 null —— 而非修复前那样「跨节点空间」或「跨 repo 维度」相减出的噪声/虚高值。
    #[serde(default)]
    pub server_max_seq: u64,
}

/// P1-4：Range-based 反熵请求（有序区间 `[lo, hi)` 摘要对账）。
///
/// 空 `lo` 表示下界 -∞，空 `hi` 表示上界 +∞（key 为非空字节串，故空串可安全作哨兵）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RangeReconcileRequestMessage {
    /// 仓库类型（当前仅 NODE 走 range 通道）
    pub repo: u8,
    /// 区间下界（含）；空 = -∞
    pub lo: Vec<u8>,
    /// 区间上界（不含）；空 = +∞
    pub hi: Vec<u8>,
    /// 请求方该区间的摘要（`range_digest`）
    pub digest: [u8; 32],
    /// 请求方建议的叶级行数阈值
    pub leaf_rows: u32,
    /// 当前下钻深度（应答方原样回显，供请求方递归计数）
    pub depth: u8,
}

/// P1-4：Range-based 反熵响应。
///
/// - 非叶（`is_leaf == false`）：`digest` + `split_points`，请求方据此继续下钻；
/// - 叶（`is_leaf == true`）：`digest` + `entries`（该区间内应答方的 `(key, data_hash)` 清单），
///   请求方对本地清单求集合差。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RangeReconcileResponseMessage {
    /// 仓库类型
    pub repo: u8,
    /// 区间下界（回显）
    pub lo: Vec<u8>,
    /// 区间上界（回显）
    pub hi: Vec<u8>,
    /// 应答方该区间的摘要
    pub digest: [u8; 32],
    /// 分界点（非叶时非空；叶时为空）
    pub split_points: Vec<Vec<u8>>,
    /// 行指纹清单（叶时携带；非叶为空）
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// 应答方该区间是否为叶
    pub is_leaf: bool,
    /// 下钻深度（回显请求）
    pub depth: u8,
}

/// P1-4：Range 反熵推送消息（A → B）。
/// 发现本地多 N 个 key 后，本地直接推送这 N 个 key 对应的完整节点数据给对端。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RangeReconcilePushMessage {
    /// 仓库类型
    pub repo: u8,
    /// 节点数据列表（id, ip, port, score, state, ...）
    pub nodes: Vec<PushNodeEntry>,
}

/// 推送的单条节点数据
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PushNodeEntry {
    pub id: Vec<u8>,
    pub ip: String,
    pub port: u16,
    pub score: f64,
    pub state: u8,
    pub query_count: u64,
    pub success_count: u64,
    pub total_latency_ms: u64,
    pub consecutive_failures: u32,
    pub nodes_returned: u64,
    pub last_active: i64,
}

/// P2-1：bootstrap 清单请求（B → A）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BootstrapManifestRequestMessage {
    /// 仓库类型（按 repo 分别 bootstrap，铁律 7）
    pub repo: u8,
}

/// P2-1：bootstrap 清单响应（A → B）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BootstrapManifestResponseMessage {
    /// 分块清单（含 w0 水位、显式区间边界与内容哈希）
    pub manifest: crate::federation::sync::bootstrap::BootstrapManifest,
}

/// P2-1：bootstrap 分块请求（B → A）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BootstrapChunkRequestMessage {
    /// 仓库类型
    pub repo: u8,
    /// 块序号（对应清单 `chunks` 下标）
    pub index: u32,
}

/// P2-1：bootstrap 分块响应（A → B）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BootstrapChunkResponseMessage {
    /// 仓库类型
    pub repo: u8,
    /// 块序号
    pub index: u32,
    /// 块内条目（接收方**批量 upsert**，严禁逐条 INSERT）
    pub entries: Vec<SyncEntry>,
    /// 服务端该块内容哈希（应等于清单中同 index 块的 `hash`）
    pub hash: [u8; 32],
    /// 是否为最后一块
    pub is_last: bool,
}

/// 节点信息消息（握手后立即双向发送）
///
/// 携带发送方本地各 repo 的总条目数，供对端在全量同步数据源选择时
/// 判断哪个节点数据最完整。轻量（仅 4 个 u32），不等待响应。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerInfoMessage {
    /// 发送方本地各 repo 的总条目数（顺序与 repo_type::NODE/PEER/INFOHASH/TRACKER 一致）。
    pub local_entry_counts: Vec<u32>,
}

/// 实时 peer 查询请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerQueryRequestMessage {
    /// 要查询的 infohash
    pub infohash: [u8; 20],
    /// 期望返回的 peer 数量上限
    pub limit: u32,
}

/// 实时 peer 查询响应中的单个 peer 条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerQueryEntry {
    /// peer IP 地址（字符串形式，兼容 IPv4/IPv6）
    pub ip: String,
    /// peer 端口
    pub port: u16,
    /// peer 来源（tracker/dht/pex/super_tracker/lpd/webseed/manual）
    pub source: String,
    /// 优先级评分
    pub score: f64,
}

/// 实时 peer 查询响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerQueryResponseMessage {
    /// 查询的 infohash
    pub infohash: [u8; 20],
    /// 查到的 peer 列表
    pub peers: Vec<PeerQueryEntry>,
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
    let payload =
        bincode::serialize(msg).map_err(|e| anyhow::anyhow!("bincode 序列化失败: {}", e))?;
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
        anyhow::bail!(
            "数据不足，需要至少 {} 字节，实际 {}",
            FRAME_HEADER_SIZE,
            data.len()
        );
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
/// 返回 `Ok(Some(total))` 表示有完整帧；`Ok(None)` 表示数据不足需继续读；
/// `Err` 表示帧长度非法（0 或超过上限）——调用方应据此断开连接，而不是继续累积缓冲。
pub fn frame_size_in_buffer(data: &[u8]) -> anyhow::Result<Option<usize>> {
    if data.len() < 4 {
        return Ok(None);
    }
    let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if length == 0 || length > MAX_FRAME_SIZE {
        anyhow::bail!("非法帧长度: {}", length);
    }
    let total = FRAME_HEADER_SIZE + length - 1;
    if data.len() >= total {
        Ok(Some(total))
    } else {
        Ok(None)
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
        assert_eq!(
            frame.len(),
            FRAME_HEADER_SIZE + bincode::serialized_size(&ping).unwrap() as usize
        );

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
        // node_id 由公钥派生，身份绑定校验应通过
        assert!(decoded.verify_identity_binding());
        // 新生成的 Hello 时间戳应在重放窗口内
        assert!(decoded.is_fresh());
    }

    #[test]
    fn test_hello_identity_binding_rejects_spoof() {
        use crate::federation::node_id::NodeIdentity;
        let identity = NodeIdentity::generate();
        let mut hello = HelloMessage::sign_and_build(&identity, vec![], 1, false);
        assert!(hello.verify_identity_binding());
        // 冒充：篡改 node_id 后不再由公钥派生
        hello.node_id = [0xab; 20];
        assert!(!hello.verify_identity_binding());
    }

    #[test]
    fn test_hello_stale_timestamp_rejected() {
        use crate::federation::node_id::NodeIdentity;
        let identity = NodeIdentity::generate();
        let mut hello = HelloMessage::sign_and_build(&identity, vec![], 1, false);
        assert!(hello.is_fresh());
        // 时间戳回拨超过窗口
        hello.timestamp_ms = hello
            .timestamp_ms
            .saturating_sub(HelloMessage::REPLAY_WINDOW_MS + 1);
        assert!(!hello.is_fresh());
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
        assert_eq!(frame_size_in_buffer(&frame).unwrap(), Some(frame_len));
        // 不足4字节
        assert_eq!(frame_size_in_buffer(&frame[..3]).unwrap(), None);
        // 有帧头但数据不足
        assert_eq!(frame_size_in_buffer(&frame[..frame_len - 1]).unwrap(), None);
        // 非法长度（0）应报错，而不是当作"未收全"
        assert!(frame_size_in_buffer(&[0u8; 4]).is_err());
        // 多帧拼接
        let mut double = frame.clone();
        double.extend_from_slice(&frame);
        assert_eq!(frame_size_in_buffer(&double).unwrap(), Some(frame_len));
    }

    #[test]
    fn test_max_frame_size() {
        let big = vec![0u8; MAX_FRAME_SIZE + 1];
        let result = encode_message(MessageType::Pong, &big);
        assert!(result.is_err());
    }
}
