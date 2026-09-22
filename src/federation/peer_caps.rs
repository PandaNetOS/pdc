//! 对端协议能力表（`peer_id → protocol_version`）
//!
//! # 为什么单独一张表
//!
//! 协议版本（Hello/HelloAck 的 `version` 字段）是 **pdc 的业务语义**，
//! 却被塞在通用连接对象 `Connection` 里（`peer_protocol_version` + 5 个 `supports_*()`），
//! 调用点 7 处全部集中在 `sync/mod.rs`（`:1204 :1908 :2487 :2654 :3018 :3538 :3668`）。
//!
//! 连接层下沉 `pnos-net` 后，SDK 只认识「帧 + 会话」，不认识协议版本。
//! 版本号的传递通路是：
//!
//! ```text
//! 握手（pdc 的 FederationAuthenticator）
//!   └─ HelloMessage.version (u32)
//!        └─ PeerIdentity.metadata = version.to_le_bytes()   ← SDK 只透传，不解释
//!             └─ PeerCapsTable.set(peer_id, version)          ← pdc 收到 Connected 事件时写入
//!                  └─ self.peer_caps.of(&peer_id).supports_diff_keys()
//! ```
//!
//! 这条链路证明 trait 注入设计是自洽的：**握手产生的应用层元数据有正式出口，
//! 不需要 SDK 认识它**（见迁移计划 §2.2 / K4）。

use dashmap::DashMap;

use crate::federation::node_id::NodeId;

/// 单节点的协议能力视图（不可变快照，拷贝开销极小）
///
/// 语义与旧 `Connection::supports_*()` **逐项等价**，只是把「版本从连接对象读」
/// 改成「版本从能力表读」，从而不再依赖连接状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerCaps {
    version: u32,
}

impl PeerCaps {
    /// 未知对端：按最保守的版本 1 处理（与旧实现 `AtomicU32::new(1)` 默认值一致）
    pub const fn unknown() -> Self {
        Self { version: 1 }
    }

    /// 由握手读到的版本构造
    pub const fn from_version(version: u32) -> Self {
        Self { version }
    }

    /// 原始协议版本号
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// 对端是否支持 DiffSync key 列表交换（协议版本 >= 2）
    pub fn supports_diff_keys(&self) -> bool {
        self.version >= 2
    }

    /// 对端是否支持分层 Merkle 对比 + 分片并行同步（协议版本 >= 3）
    pub fn supports_layered_merkle(&self) -> bool {
        self.version >= 3
    }

    /// 对端是否支持增量（delta）同步通道（协议版本 >= 4）
    ///
    /// 逐字复用旧 `Connection::supports_delta_sync()` 的判定，避免语义漂移。
    pub fn supports_delta_sync(&self) -> bool {
        crate::federation::sync::delta::supports_delta_sync(self.version)
    }

    /// 对端是否支持 Range-based（有序区间下钻）反熵（协议版本 >= 5）
    pub fn supports_range_reconcile(&self) -> bool {
        crate::federation::sync::range_reconcile::supports_range_reconcile(self.version)
    }

    /// 对端是否支持 Range 反熵修复通用通道（Pull/Push2，协议版本 >= 8）
    pub fn supports_range_v2(&self) -> bool {
        self.version >= 8
    }

    /// 对端是否支持 bootstrap 专用通道（协议版本 >= 6）
    pub fn supports_bootstrap(&self) -> bool {
        self.version >= crate::federation::sync::bootstrap::BOOTSTRAP_PROTOCOL_VERSION
    }
}

impl Default for PeerCaps {
    fn default() -> Self {
        Self::unknown()
    }
}

/// `peer_id → protocol_version` 表（并发安全，握手成功后写入，断连时移除）
#[derive(Default)]
pub struct PeerCapsTable {
    inner: DashMap<NodeId, u32>,
}

impl PeerCapsTable {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    /// 记录对端协议版本（握手成功 / 收到 `SessionEvent::Connected` 时调用）
    pub fn set(&self, peer: NodeId, version: u32) {
        self.inner.insert(peer, version);
    }

    /// 移除对端记录（收到 `SessionEvent::Disconnected` 时调用）
    pub fn remove(&self, peer: &NodeId) {
        self.inner.remove(peer);
    }

    /// 查询对端能力；未知对端按版本 1 处理（保守回退）
    pub fn of(&self, peer: &NodeId) -> PeerCaps {
        self.inner
            .get(peer)
            .map(|v| PeerCaps::from_version(*v))
            .unwrap_or_else(PeerCaps::unknown)
    }

    /// 已知版本的对端数量
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// 清空（关闭/测试用）
    pub fn clear(&self) {
        self.inner.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(b: u8) -> NodeId {
        NodeId([b; 20])
    }

    #[test]
    fn test_unknown_defaults_to_version_1() {
        let table = PeerCapsTable::new();
        let caps = table.of(&id(1));
        assert_eq!(caps.version(), 1);
        assert!(!caps.supports_diff_keys());
        assert!(!caps.supports_layered_merkle());
        assert!(!caps.supports_delta_sync());
        assert!(!caps.supports_range_reconcile());
        assert!(!caps.supports_bootstrap());
    }

    #[test]
    fn test_set_and_of_roundtrip() {
        let table = PeerCapsTable::new();
        table.set(id(2), 6);
        let caps = table.of(&id(2));
        assert_eq!(caps.version(), 6);
        assert!(caps.supports_diff_keys());
        assert!(caps.supports_layered_merkle());
        assert!(caps.supports_delta_sync());
        assert!(caps.supports_range_reconcile());
        assert!(caps.supports_bootstrap());
    }

    #[test]
    fn test_threshold_boundaries() {
        // 逐项等价于旧 Connection::supports_*() 的阈值
        let v2 = PeerCaps::from_version(2);
        assert!(v2.supports_diff_keys() && !v2.supports_layered_merkle());
        let v3 = PeerCaps::from_version(3);
        assert!(v3.supports_layered_merkle());
        let v5 = PeerCaps::from_version(5);
        assert!(v5.supports_range_reconcile() && !v5.supports_bootstrap());
    }

    #[test]
    fn test_remove_and_len() {
        let table = PeerCapsTable::new();
        assert!(table.is_empty());
        table.set(id(3), 6);
        assert_eq!(table.len(), 1);
        table.remove(&id(3));
        assert!(table.is_empty());
        // 移除后回退到保守默认
        assert_eq!(table.of(&id(3)).version(), 1);
    }

    #[test]
    fn test_default_is_unknown() {
        assert_eq!(PeerCaps::default().version(), 1);
    }
}
