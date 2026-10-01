//! 联邦同步通道状态（监控面板轮询 `GET /api/v1/sync/channels`）。
//!
//! 设计要点：
//! - 三个通道（Bootstrap 全量快照 / Delta oplog 增量 / Range 反熵）各自维护一组
//!   计数器，由各业务路径在已有更新点旁「顺手」写一行，**不改变任何业务逻辑**。
//! - 状态容器用 [`parking_lot::RwLock`] 包在 [`Arc`] 里（与本仓库 `io_scheduler` 一致），
//!   写锁临界区只更新几个数字字段，不跨 await、不持锁做 IO。
//! - 通过 [`global`] 暴露全局单例（`std::sync::OnceLock` 保证只初始化一次），业务侧
//!   更新点无需改函数签名即可拿到句柄；同时在 `AppState` 里再挂一份同一个 `Arc`
//!   供 REST handler 读快照——**不是两份状态**，是同一个实例的两个引用。
//!
//! 所有字段 `#[serde(default)]`：历史配置/旧版本反序列化缺字段时回退零值，不报错。

use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Bootstrap（全量快照）通道状态。
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BootstrapChannelStatus {
    /// 是否有 bootstrap 正在进行（收到清单后 true；切 delta / 竣工 / 失败后 false）。
    #[serde(default)]
    pub active: bool,
    /// 对端 node_id 的 hex 字符串；无活跃 bootstrap 时为空串。
    #[serde(default)]
    pub peer_id: String,
    /// 当前 bootstrap 的 repo（1..4）。
    #[serde(default)]
    pub repo: u64,
    /// 清单总块数。
    #[serde(default)]
    pub total_chunks: u64,
    /// 已完成块数（最大连续前缀）。
    #[serde(default)]
    pub done_chunks: u64,
    /// 当前在途（已请求未响应）块数。
    #[serde(default)]
    pub inflight: u32,
    /// 本地已存在、hash 一致而跳过下载的块数。
    #[serde(default)]
    pub skipped_identical: u64,
    /// 阶段（小写 snake）：`manifest` / `transfer` / `tail_follow` / `verify` / `done` / `idle`。
    #[serde(default)]
    pub phase: String,
    /// 窗口级空闲看门狗已回收重发的次数。
    #[serde(default)]
    pub idle_recoveries: u32,
    /// 方向：`"send"`（本地向对端推块）/ `"recv"`（本地从对端拉块）/ `""`（未确定/空闲）。
    /// 由核心运行时在 sync 各写入点落值；观测侧只读。
    #[serde(default)]
    pub direction: String,
}

/// Delta（oplog 增量）通道状态。
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DeltaChannelStatus {
    /// 本地 oplog 当前长度。
    #[serde(default)]
    pub oplog_len: u64,
    /// 累计已应用的同步条目数（与 `FederationMetrics::record_sync_entries` 同口径）。
    #[serde(default)]
    pub sync_entries_applied: u64,
    /// 对端拉取水位（已复制到的最大 seq）。
    #[serde(default)]
    pub since_seq: u64,
    /// OpsBatch 网络发送失败累计次数。
    #[serde(default)]
    pub batch_send_failures: u64,
    /// 是否检测到 oplog 空洞（被裁剪、中间段缺失）。
    #[serde(default)]
    pub oplog_gap_detected: bool,
}

/// Range（区间反熵）通道状态。
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RangeReconcileStatus {
    /// 累计到达叶级、做过集合差的区间数。
    #[serde(default)]
    pub leaf_compares: u64,
    /// 累计「本地多」的 key 数。
    #[serde(default)]
    pub local_extra: u64,
    /// 累计「对端多」的 key 数。
    #[serde(default)]
    pub remote_extra: u64,
    /// 累计触发修复（Pull/Push2）次数。
    #[serde(default)]
    pub repairs_triggered: u64,
    /// 已完成的抽样对账轮数。
    #[serde(default)]
    pub rounds_completed: u64,
    /// 运行模式（小写 snake）：`repair`（实际修复）/ `diagnostic`（只读诊断）/ `idle`。
    #[serde(default)]
    pub mode: String,
}

/// 三个通道的聚合快照（REST handler 直接序列化整个结构体）。
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SyncChannelsStatus {
    #[serde(default)]
    pub bootstrap: BootstrapChannelStatus,
    #[serde(default)]
    pub delta: DeltaChannelStatus,
    #[serde(default)]
    pub range_reconcile: RangeReconcileStatus,
}

static GLOBAL: OnceLock<Arc<RwLock<SyncChannelsStatus>>> = OnceLock::new();

/// 全局共享句柄（单例）。首次调用时初始化，之后所有调用方拿到同一个 `Arc`。
///
/// `AppState` 注入与业务更新点都从这里取，保证状态只有一份。
pub fn global() -> Arc<RwLock<SyncChannelsStatus>> {
    GLOBAL
        .get_or_init(|| Arc::new(RwLock::new(SyncChannelsStatus::default())))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_serializes_expected_keys() {
        let status = SyncChannelsStatus::default();
        let v = serde_json::to_value(&status).expect("serialize");

        // 三个顶层 key
        for k in ["bootstrap", "delta", "range_reconcile"] {
            assert!(v.get(k).is_some(), "顶层缺少 key: {}, body={}", k, v);
        }

        // bootstrap 子结构 10 个字段（含 E3 direction）
        let b = v.get("bootstrap").unwrap();
        for k in [
            "active",
            "peer_id",
            "repo",
            "total_chunks",
            "done_chunks",
            "inflight",
            "skipped_identical",
            "phase",
            "idle_recoveries",
            "direction",
        ] {
            assert!(b.get(k).is_some(), "bootstrap 缺少字段: {}", k);
        }

        // delta 子结构 5 个字段
        let d = v.get("delta").unwrap();
        for k in [
            "oplog_len",
            "sync_entries_applied",
            "since_seq",
            "batch_send_failures",
            "oplog_gap_detected",
        ] {
            assert!(d.get(k).is_some(), "delta 缺少字段: {}", k);
        }

        // range_reconcile 子结构 6 个字段
        let r = v.get("range_reconcile").unwrap();
        for k in [
            "leaf_compares",
            "local_extra",
            "remote_extra",
            "repairs_triggered",
            "rounds_completed",
            "mode",
        ] {
            assert!(r.get(k).is_some(), "range_reconcile 缺少字段: {}", k);
        }

        // 默认值：active=false、peer_id 空串、phase/direction/mode 空串（业务写入后才填）
        assert_eq!(b["active"], serde_json::json!(false));
        assert_eq!(b["peer_id"], serde_json::json!(""));
        assert_eq!(
            b["direction"],
            serde_json::json!(""),
            "E3 direction 默认应为空串"
        );
    }

    #[test]
    fn global_is_singleton() {
        let a = global();
        let b = global();
        // 同一个 Arc 指向同一个实例
        assert!(Arc::ptr_eq(&a, &b));
        // 写一份，另一个引用立刻可见
        a.write().bootstrap.active = true;
        assert!(b.read().bootstrap.active);
        // 复位（避免污染其他用例）
        b.write().bootstrap.active = false;
    }
}
