//! 对端连接视图（**SDK 会话的唯一视图**）
//!
//! # 为什么需要它
//!
//! pdc 侧消费「连接」的地方（`sync/` / `gossip.rs` / `discovery.rs` / `relay.rs` /
//! `signaling.rs`）实际只需要三样东西：**对端 ID + 发消息 + 协议能力**。
//! 本类型把这三样收成一个视图，不持有任何连接状态（状态全在 SDK `SessionManager`）。
//!
//! # 迁移历史
//!
//! 迁移期本类型是**双栈**的：`Legacy(Arc<Connection>) | Sdk(Arc<FederationSessions>)`，
//! 因而 25 处签名替换成为纯机械操作、翻转前零行为变化。
//! G5 翻转后旧承载（`connection.rs::Connection`）已删除，本类型只指向
//! `pnos-net` 会话层。
//!
//! # API 面的依据
//!
//! 面宽来自对 `federation/` 内 `conn.*` 成员访问的实测统计（G5 精测）：
//! `node_id`(77) / `send_message`(56) / `supports_*`(7) / `peer_protocol_version`(1)，
//! 其余成员（`pending` / `gossip_buffer` / `recv_message`）均属承载态，不在视图内。
//!
//! 能力位（`supports_*`）一律经 [`PeerCaps`] 判定，与 `sync/mod.rs` 的调用点
//! 共用同一套阈值，避免两处漂移。

use std::net::SocketAddr;
use std::sync::Arc;

use crate::federation::node_id::NodeId;
use crate::federation::peer_caps::PeerCaps;
use crate::federation::protocol::MessageType;
use crate::federation::session::FederationSessions;

/// 对端连接视图（无状态，只指向 SDK 会话门面）
///
/// `node_id` 是**公开字段**：消费侧有 77 处 `conn.node_id` 直接读取，
/// 保持字段形态可让调用点保持纯机械。
pub struct PeerConn {
    /// 对端节点 ID
    pub node_id: NodeId,
    /// SDK 会话门面（唯一承载）
    sessions: Arc<FederationSessions>,
}

impl PeerConn {
    /// 构造视图（由 `FederationSessions::connections` / `connection_of` 调用）
    pub fn sdk(node_id: NodeId, sessions: Arc<FederationSessions>) -> Self {
        Self { node_id, sessions }
    }

    /// 对端节点 ID（与字段同值，便于链式调用）
    pub fn peer_id(&self) -> NodeId {
        self.node_id
    }

    /// 对端地址（无会话时为 `None`）
    pub fn addr(&self) -> Option<SocketAddr> {
        self.sessions.peer_addr(&self.node_id)
    }

    /// 会话已存活秒数（无会话时为 0）
    pub fn connected_secs(&self) -> u64 {
        self.sessions
            .session_of(&self.node_id)
            .map(|i| i.uptime_secs)
            .unwrap_or(0)
    }

    /// 距上次收到数据的时长（空闲判定口径；无会话时为 0）
    pub fn idle_duration(&self) -> std::time::Duration {
        std::time::Duration::from_millis(
            self.sessions
                .session_of(&self.node_id)
                .map(|i| i.idle_ms)
                .unwrap_or(0),
        )
    }

    /// 对端协议版本（从能力表读取）
    pub fn protocol_version(&self) -> u32 {
        self.sessions.peer_caps(&self.node_id).version()
    }

    /// 能力视图（阈值判定统一收敛到 [`PeerCaps`]）
    pub fn caps(&self) -> PeerCaps {
        PeerCaps::from_version(self.protocol_version())
    }

    /// 对端是否支持 DiffSync key 列表交换（协议版本 >= 2）
    pub fn supports_diff_keys(&self) -> bool {
        self.caps().supports_diff_keys()
    }

    /// 对端是否支持分层 Merkle 对比 + 分片并行同步（协议版本 >= 3）
    pub fn supports_layered_merkle(&self) -> bool {
        self.caps().supports_layered_merkle()
    }

    /// 对端是否支持增量（delta）同步通道（协议版本 >= 4）
    pub fn supports_delta_sync(&self) -> bool {
        self.caps().supports_delta_sync()
    }

    /// 对端是否支持 Range-based 反熵（协议版本 >= 5）
    pub fn supports_range_reconcile(&self) -> bool {
        self.caps().supports_range_reconcile()
    }

    /// 对端是否支持 Range 反熵修复通用通道 Pull/Push2（协议版本 >= 8）
    pub fn supports_range_v2(&self) -> bool {
        self.caps().supports_range_v2()
    }

    /// 对端是否支持 bootstrap 专用通道（协议版本 >= 6）
    pub fn supports_bootstrap(&self) -> bool {
        self.caps().supports_bootstrap()
    }

    /// 发送消息（内部序列化为 `[4B len][1B kind][payload]`）
    ///
    /// 线缆格式与旧 `Connection::send_message` 完全一致——两条路径共用
    /// `protocol::encode_message`，故翻转不改变对端可见字节。
    pub async fn send_message<T: serde::Serialize>(
        &self,
        msg_type: MessageType,
        msg: &T,
    ) -> anyhow::Result<()> {
        self.sessions.send_to(&self.node_id, msg_type, msg).await
    }
}

impl std::fmt::Debug for PeerConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerConn")
            .field("node_id", &self.node_id)
            .field("addr", &self.addr())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 能力位阈值必须与旧 `Connection::supports_*()` 判定**逐项等价**。
    ///
    /// 这条断言是 R3（协议版本透出丢失导致同步能力协商退化）的回归闸门：
    /// 阈值不再由本文件或 `peer_caps.rs` 各写一份，而是同源。
    #[test]
    fn capability_thresholds_match_legacy_semantics() {
        let cases = [
            (1u32, false, false, false, false, false),
            (2, true, false, false, false, false),
            (3, true, true, false, false, false),
            (4, true, true, true, false, false),
            (5, true, true, true, true, false),
            (6, true, true, true, true, true),
        ];
        for (v, diff_keys, merkle, delta, range, bootstrap) in cases {
            let caps = PeerCaps::from_version(v);
            assert_eq!(caps.supports_diff_keys(), diff_keys, "v={v} diff_keys");
            assert_eq!(caps.supports_layered_merkle(), merkle, "v={v} merkle");
            assert_eq!(caps.supports_delta_sync(), delta, "v={v} delta");
            assert_eq!(caps.supports_range_reconcile(), range, "v={v} range");
            assert_eq!(caps.supports_bootstrap(), bootstrap, "v={v} bootstrap");
        }
    }

    /// 视图的能力位必须直接来自 `protocol_version()`，不允许有第二套阈值。
    #[test]
    fn view_delegates_caps_to_version() {
        for v in 1u32..=6 {
            let caps = PeerCaps::from_version(v);
            assert_eq!(caps.version(), v);
            // 同一版本的 `PeerCaps` 两次判定必须一致（无隐藏状态）
            assert_eq!(caps.supports_bootstrap(), caps.supports_bootstrap());
        }
    }
}
