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
    // 编号 11 / 12：已废弃并移除（原 MerkleDigest / MerkleRequest，随 Merkle 反熵退场）。
    /// 同步批量
    SyncBatch = 13,
    /// 断开通知
    Goodbye = 14,
    // 编号 15-19：已废弃并移除（原 MerkleRepair / FullSyncStart/Batch/Ack/Complete，
    // 其唯一发送者 DiffSync 随 Merkle 反熵退场一并退役）。
    /// Gossip 批量合并帧（多个 GossipBatch 合并为一个大帧发送，减少网络往返）。
    /// 假设对端支持此消息类型（当前所有对端同版本），未来需加能力协商。
    GossipBatchBulk = 20,
    // 编号 21：已废弃并移除（原 DiffSyncRequest）。
    /// 节点信息（握手后立即双向发送，携带本地各 repo 条目数，
    /// 供 bootstrap 自动触发时判断对端数据量）。
    PeerInfo = 22,
    // 编号 23 / 24 / 25：已废弃并移除。
    // 原为 Push-Pull Gossip 的 GossipDigest / GossipPullRequest / GossipPullResponse；
    // P1-9 移除该同步路径后不再收发，编号保留空位避免与历史对端错位。
    /// 实时 peer 查询请求（announce 时本地 peer 不足，向联邦节点查询）
    PeerQueryRequest = 26,
    /// 实时 peer 查询响应
    PeerQueryResponse = 27,
    // 编号 28-36：已废弃并移除（原 DiffSync key 交换 / 分层 Merkle 层级请求 / 分片同步家族）。
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
    // 编号 45：已废弃并移除（原 NODE 专属 RangeReconcilePush/PushNodeEntry，v8 起由 49 号
    // `RangeReconcilePush2`（4 repo 通用 SyncEntry）取代，编号保留空位避免与历史对端错位）。
    /// v7 建连协商（双向互发）：互报各 repo 数据量 / 收发能力 / 吞吐提示。
    SyncNegotiate = 46,
    /// v7 建连协商确认：按 repo 下发同步策略（DELTA / BOOTSTRAP / NONE）。
    SyncNegotiateAck = 47,
    /// v8 Range 反熵按键拉取（B → A）：叶级对账发现「对端多」的 key 列表，
    /// 请对端按 key 加载完整条目后以 `RangeReconcilePush2` 回发。
    RangeReconcilePull = 48,
    /// v8 Range 反熵推送（4 repo 通用）：携带完整 `SyncEntry`（含 payload），
    /// 接收方直接走 `handle_sync_batch` 幂等 apply（不写 oplog、不传播）。
    RangeReconcilePush2 = 49,
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
            // 11 / 12：已废弃（原 MerkleDigest / MerkleRequest），不再映射
            13 => Some(MessageType::SyncBatch),
            14 => Some(MessageType::Goodbye),
            // 15-19：已废弃（原 MerkleRepair / FullSync 家族），不再映射
            20 => Some(MessageType::GossipBatchBulk),
            // 21：已废弃（原 DiffSyncRequest），不再映射
            22 => Some(MessageType::PeerInfo),
            // 23 / 24 / 25：已废弃（原 Push-Pull Gossip），不再映射
            26 => Some(MessageType::PeerQueryRequest),
            27 => Some(MessageType::PeerQueryResponse),
            // 28-36：已废弃（原 DiffSync key 交换 / 分层 Merkle / 分片同步家族），不再映射
            37 => Some(MessageType::OpsRequest),
            38 => Some(MessageType::OpsBatch),
            39 => Some(MessageType::RangeReconcileRequest),
            40 => Some(MessageType::RangeReconcileResponse),
            41 => Some(MessageType::BootstrapManifestRequest),
            42 => Some(MessageType::BootstrapManifestResponse),
            43 => Some(MessageType::BootstrapChunkRequest),
            44 => Some(MessageType::BootstrapChunkResponse),
            // 45：已废弃（原 NODE 专属 RangeReconcilePush），不再映射
            46 => Some(MessageType::SyncNegotiate),
            47 => Some(MessageType::SyncNegotiateAck),
            48 => Some(MessageType::RangeReconcilePull),
            49 => Some(MessageType::RangeReconcilePush2),
            _ => None,
        }
    }

    /// 转为 u8
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}

/// 本节点联邦协议版本（`Hello`/`HelloAck` 的 `version` 字段）。
/// 1：旧版本（仅全量推送差异分片）。
/// 2：支持 DiffSync key 列表交换（先交换 key 列表，只推送对方缺失条目，重复率 ~90%→<5%）。
/// 3：支持分层 Merkle 对比 + 分片并行同步（L0→L1→L2 三层定位差异，只同步差异 L2 分片，支持并行+断点续传+流式加载）。
/// 4：支持增量（delta）同步通道（OpsRequest/OpsBatch，基于本地 oplog 的 O(Δ) 稳态同步）。
/// 5：支持 Range-based（有序区间 + 分界点下钻）反熵（RangeReconcileRequest/Response）。
/// 6：支持 bootstrap 专用通道（BootstrapManifest*/BootstrapChunk*，与在线反熵解耦的全量引导）。
/// 7：支持建连协商（SyncNegotiate/SyncNegotiateAck）：连接建立后先互报数据量/能力并协商
///    per-repo 同步策略，稳定性门控（连接存活 ≥ `strategy_min_conn_secs`）通过后才开大通道。
/// 8：Range 反熵修复通道通用化（RangeReconcilePull/RangeReconcilePush2）：4 repo 均可
///    推送/按键拉取完整 SyncEntry，Merkle 反熵协议族（11/12/15/21/28-36/45）同版退役。
/// 对端 version < 2 时回退到原始全量推送；version == 2 时使用 DiffSync key 交换；version >= 3 时使用分层 Merkle；
/// version >= 4 且 `federation.delta_sync_enabled=true` 时启用 delta 通道；
/// version >= 5 且 `federation.range_reconcile_enabled=true` 时启用 range 反熵；
/// version >= 6 且 `federation.bootstrap_enabled=true` 时启用 bootstrap 通道；
/// version >= 7 且 `federation.negotiation_enabled=true` 时 delta/bootstrap 大通道需协商通过后才启动
/// （对端 < v7 回落旧行为：不协商直接按既有开关运行）；
/// version >= 8 时启用 range 修复的 Pull/Push2 通用通道（对端 < v8 时不发，仅保留叶级对账）。
///
/// G4 迁移：原定义在 `federation/connection.rs`，随握手实现一并归位到协议层
/// （`connection.rs` 保留 `pub use` 转发）。
pub const HELLO_PROTOCOL_VERSION: u32 = 8;

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

// ---------------------------------------------------------------------------
// v7 建连协商（SyncNegotiate / SyncNegotiateAck）
// ---------------------------------------------------------------------------

/// 协商策略：该 repo 在两端之间不做大迁移（数据已同步 / 双方皆空）。
pub const STRATEGY_NONE: u8 = 0;
/// 协商策略：稳态增量（oplog 水位续拉，受批量/间隔约束）。
pub const STRATEGY_DELTA: u8 = 1;
/// 协商策略：大差集 / 冷启动 —— 由缺数据一方通过 bootstrap 分块通道拉取快照。
pub const STRATEGY_BOOTSTRAP: u8 = 2;

/// v7 协商：单 repo 状态摘要（发送方本地实况）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoSyncState {
    /// repo 类型（0=NODE 1=PEER 2=INFOHASH 3=TRACKER）
    pub repo: u8,
    /// 本 repo 当前行数
    pub row_count: u64,
    /// 本 repo oplog 最大 seq（0 = 无记录）
    pub max_seq: u64,
    /// oplog 保留窗口内最小 seq（0 = 无记录）
    pub min_seq: u64,
    /// oplog 保留窗口（秒；0 = 永不裁剪）
    pub retention_secs: u64,
}

/// v7 协商：发送方能力（收发限速 / 吞吐提示 / 建议批量）。
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SyncCaps {
    /// 发送能力上限（字节/秒；0 = 不限）
    pub send_rate_bytes_per_sec: u64,
    /// 接收能力上限（字节/秒；0 = 不限）
    pub recv_rate_bytes_per_sec: u64,
    /// 本地磁盘顺序读吞吐提示（字节/秒；启动时自测，0 = 未知）
    pub disk_throughput_hint: u64,
    /// 建议的单批条数上限
    pub batch_limit: u32,
}

/// v7 建连协商请求（建连后双方互发一次；对端回 Ack）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncNegotiateMessage {
    /// 各 repo 本地状态
    pub repos: Vec<RepoSyncState>,
    /// 发送方能力
    pub caps: SyncCaps,
    /// 发送时间戳（UNIX 毫秒，仅观测用）
    pub timestamp_ms: u64,
}

/// v7 协商：单 repo 策略（Ack 发送方为「接收方应如何从我这取数」给出的裁定）。
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RepoStrategy {
    pub repo: u8,
    /// `STRATEGY_NONE` / `STRATEGY_DELTA` / `STRATEGY_BOOTSTRAP`
    pub strategy: u8,
    /// 建议速率上限（字节/秒；= min(双方 caps)，0 = 不限）
    pub rate_bytes_per_sec: u64,
    /// 建议单批条数上限
    pub batch_limit: u32,
}

/// v7 建连协商确认。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncNegotiateAckMessage {
    /// 各 repo 协商出的策略
    pub repos: Vec<RepoStrategy>,
    /// 发送时间戳（UNIX 毫秒，仅观测用）
    pub timestamp_ms: u64,
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

/// v8：Range 反熵按键拉取（B → A）。
/// 叶级对账发现「对端多」后，把缺失 key 列表发给对端，由对端按 key 加载完整条目
/// 以 `RangeReconcilePush2` 回发。仅对 `protocol_version >= 8` 的对端发送。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RangeReconcilePullMessage {
    /// 仓库类型
    pub repo: u8,
    /// 请求方缺失、对端持有的 key 列表（`load_repo_key_hashes_in_range` 的 DB 形态）
    pub keys: Vec<Vec<u8>>,
}

/// v8：Range 反熵推送消息（4 repo 通用，A → B）。
/// 携带完整 `SyncEntry`（含 payload），接收方经 `handle_sync_batch` 幂等 apply——
/// 入站路径不写 oplog、不提交 gossip（「入站不写回」不变量）。
/// 仅对 `protocol_version >= 8` 的对端发送；取代 NODE 专属的 45 号旧推送。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RangeReconcilePush2Message {
    /// 仓库类型
    pub repo: u8,
    /// 完整同步条目（key/payload 与各 repo gossip 条目同构）
    pub entries: Vec<SyncEntry>,
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
        // v8 起编号 11/12、15-19、21、28-36、45 退役为空位，不再连续可 roundtrip；
        // 改为抽查存活编号（含退役空位必须返回 None）。
        for i in [0u8, 10, 13, 14, 20, 22, 26, 27, 37, 40, 44, 46, 47, 48, 49] {
            let mt = MessageType::from_u8(i).unwrap();
            assert_eq!(mt.as_u8(), i);
        }
        for i in [11u8, 12, 15, 16, 19, 21, 28, 36, 45] {
            assert!(
                MessageType::from_u8(i).is_none(),
                "已退役编号 {i} 不应再映射"
            );
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
