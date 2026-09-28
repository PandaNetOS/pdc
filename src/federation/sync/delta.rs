//! P1-3：oplog 增量（delta）同步通道。
//!
//! 设计见 `docs/architecture/12-federation-sync-reconciliation.md` §5.2 / §6.3。
//!
//! 与反熵（Merkle 对账，兜底）的分工：
//! - **稳态主线 = delta**：对端只发 `OpsRequest { repo, since_seq }`，本端从 `feed_oplog`
//!   取 `seq > since_seq` 的变更回批次（对端 ≥ v9 回 `OpsBatchV2`（ops 携带真实 version），
//!   v4-v8 回旧 `OpsBatch`），成本 **O(Δ)**（Δ = 单轮新增变更数），
//!   与库总量 N、与差异量 d 都无关 —— 这是根治「差异散落全部分片 → 每轮重传整表」的关键。
//! - **兜底 = 反熵**：delta 丢包/裁剪窗口越界/首次上线时，靠 Merkle 对账收敛。
//!
//! 关键约束：
//! - 入站 apply **不写回 oplog**（否则 A→B→A 回环）；本通道只读本地 oplog、只写本地数据。
//! - 幂等：重复的 op 应用必须无害（upsert by key / delete by key）。
//! - 版本向量（每个对端每个 repo 已同步到的 seq）持久化在 SQLite `delta_peer_seq` 表，
//!   重启后续传而不重来。
//! - 默认 `federation.delta_sync_enabled = false`：关闭时完全不发 OpsRequest/OpsBatch，
//!   行为与改造前一致（两端升级后再开启，避免与旧版不兼容）。

use rusqlite::{params, Connection};
use tracing::{debug, warn};

use crate::federation::protocol::{operation, OpEntry, OpEntryV2, SyncEntry};
use crate::storage::oplog::OpRecord;

/// 支持 delta 通道的协议版本（用于握手能力协商）。
pub const DELTA_SYNC_PROTOCOL_VERSION: u32 = 4;

/// F8/v9：支持 delta 通道 **version 透传**（OpsBatchV2）的协议版本。
///
/// 背景（实测）：旧 OpsBatch 的 ops 每条 version 恒置 0（wire 上根本没有 version 字段），
/// 接收端对「本地已存在」的条目按旧语义跳过 → **delta 通道对既有条目的更新（版本提升）
/// 被静默丢弃**，一致性全靠 Range 反熵兜底。OpsBatchV2 让 ops 携带真实 version，
/// 接收端走正常 LWW（version 大者胜）。
///
/// 协议演进遵循项目先例（v7 新增 SyncNegotiate/SyncNegotiateAck）：bincode 对结构体
/// 追加字段不兼容旧字节流，故**新增消息类型** OpsBatchV2（50 号）而非改 OpsBatch；
/// 对端握手版本 ≥ 9 时响应方发 V2，否则回退旧 OpsBatch（行为与改造前完全一致）。
pub const DELTA_SYNC_PROTOCOL_VERSION_V2: u32 = 9;

/// 单批默认上限（条）。1万条批在慢盘上会造成读/apply 长时间持锁（51 事故根因），
/// 降到千级使单批持锁时间从数十秒降到亚秒级。
pub const DELTA_BATCH_LIMIT_DEFAULT: u32 = 1_000;

/// 单批字节上限（不含帧头）：OpsBatch 组装时按 key+payload 累计估算，超过即截断本批。
/// 与 `gossip_bulk_max_bytes` 同量级，防止大 value 场景下批帧失控。
///
/// 取 1.5 MiB 并显著小于接收组帧缓冲上限（`MAX_READ_BUF`，17 MiB），为帧头、
/// 消息封装及高负载下已排队的后续字节预留充足余量，避免单批帧触发接收端缓冲保护。
pub const DELTA_BATCH_MAX_BYTES: usize = 1536 * 1024;

/// 支持建连协商（SyncNegotiate/Ack）的协议版本。
pub const NEGOTIATION_PROTOCOL_VERSION: u32 = 7;

/// 建 delta 版本向量表（幂等）。
pub fn init_delta_tables(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS delta_peer_seq (
            peer   BLOB NOT NULL,     -- 对端 20B node_id
            repo   INTEGER NOT NULL,  -- 0..3
            seq    INTEGER NOT NULL,  -- 该对端该 repo 已同步到的 oplog seq
            PRIMARY KEY (peer, repo)
        );
        "#,
    )?;
    Ok(())
}

impl crate::storage::db::Storage {
    /// 读取某对端在某 repo 上已同步到的 seq（不存在时 0）。
    pub fn get_peer_seq(&self, peer: &[u8], repo: u8) -> anyhow::Result<i64> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        let v: i64 = conn
            .query_row(
                "SELECT seq FROM delta_peer_seq WHERE peer = ?1 AND repo = ?2",
                params![peer, repo as i64],
                |r| r.get(0),
            )
            .unwrap_or(0);
        Ok(v)
    }

    /// 写入某对端在某 repo 上已同步到的 seq（幂等 upsert；仅前进，不回退）。
    pub fn set_peer_seq(&self, peer: &[u8], repo: u8, seq: i64) -> anyhow::Result<()> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "INSERT INTO delta_peer_seq (peer, repo, seq) VALUES (?1, ?2, ?3) \
             ON CONFLICT(peer, repo) DO UPDATE SET seq = MAX(delta_peer_seq.seq, excluded.seq)",
            params![peer, repo as i64, seq],
        )?;
        Ok(())
    }

    /// 列出全部 (peer, repo, seq)，用于诊断/可观测性。
    pub fn all_peer_seqs(&self) -> anyhow::Result<Vec<(Vec<u8>, u8, i64)>> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare("SELECT peer, repo, seq FROM delta_peer_seq")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, i64>(1)? as u8,
                row.get::<_, i64>(2)?,
            ))
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }
}

/// 把旧 `OpsBatch.ops` 转换成统一的 `SyncEntry`，供既有 `apply_*_sync` 幂等应用。
///
/// **旧通道（对端 v4-v8）专用**：旧 OpsBatch 的 wire 格式里没有 version 字段，
/// 因此转换结果 version 置 0 —— 既有 `apply_*_sync` 对「version==0 且本地已存在」的条目
/// 会跳过（避免旧数据覆盖新数据），即本函数维持改造前的保守语义。
/// 「对既有条目的更新被跳过」由周期反熵兜底；真实 version 透传走
/// [`ops_to_sync_entries_v2`]（OpsBatchV2，协议 v9）。
pub fn ops_to_sync_entries(ops: &[OpEntry]) -> Vec<SyncEntry> {
    ops.iter()
        .map(|o| SyncEntry {
            key: o.key.clone(),
            operation: if o.is_delete {
                operation::DELETE
            } else {
                operation::UPSERT
            },
            version: 0,
            payload: o.value.clone(),
        })
        .collect()
}

/// F8/v9：把 `OpsBatchV2.ops` 转换成统一的 `SyncEntry`，**透传真实 version**。
///
/// 与 [`ops_to_sync_entries`] 的唯一差异：version 不再置 0，而是取 oplog 里
/// 产生该变更时的 LWW 版本号（与 `SyncEntry.version` 同源）。接收端 apply 走
/// 正常 LWW —— 「对既有条目的版本提升」不再被静默丢弃。
/// 例外：oplog 迁移前的历史行 version=0，此时沿用旧「已存在即跳过」语义（安全回退）。
pub fn ops_to_sync_entries_v2(ops: &[OpEntryV2]) -> Vec<SyncEntry> {
    ops.iter()
        .map(|o| SyncEntry {
            key: o.key.clone(),
            operation: if o.is_delete {
                operation::DELETE
            } else {
                operation::UPSERT
            },
            version: o.version,
            payload: o.value.clone(),
        })
        .collect()
}

/// F8/v9：把 V2 条目降级成旧 `OpEntry`（丢弃 version），供对 v4-v8 对端回退旧 OpsBatch。
///
/// 旧消息的 bincode 字节流不含 version 字段，响应方必须显式降级而不是直接复用 V2 结构，
/// 否则旧对端反序列化会错位/失败。字段语义与 OpEntry 逐项一致。
pub fn to_legacy_entries(ops: &[OpEntryV2]) -> Vec<OpEntry> {
    ops.iter()
        .map(|o| OpEntry {
            seq: o.seq,
            is_delete: o.is_delete,
            key: o.key.clone(),
            value: o.value.clone(),
        })
        .collect()
}

/// 从 oplog `OpRecord` 列表构建 `OpEntryV2` 列表（供接收方组装 OpsBatchV2）。
///
/// F8：version 从 `OpRecord` 原样透传（此前旧 `records_to_entries` 构建旧 OpEntry 时
/// 无 version 可带，是 delta 通道「更新被静默丢弃」的根因之一）。
/// oplog 迁移前的历史行 version=0，透传 0 即旧语义，无需特殊处理。
pub fn records_to_entries(records: &[OpRecord]) -> Vec<OpEntryV2> {
    records
        .iter()
        .map(|r| OpEntryV2 {
            seq: r.seq.max(0) as u64,
            is_delete: r.op == crate::storage::oplog::OP_DELETE,
            key: r.key.clone(),
            value: r.value.clone(),
            version: r.version,
        })
        .collect()
}

/// 记录一次 delta 应用结果（日志/可观测性）。
pub fn log_applied(repo: u8, applied: usize, next_seq: u64) {
    if applied > 0 {
        debug!(
            "[delta] 应用增量: repo={}, ops={}, next_seq={}",
            repo, applied, next_seq
        );
    }
}

/// 检查对端协议版本是否支持 delta 通道（纯判定，不做网络动作）。
pub fn supports_delta_sync(peer_protocol_version: u32) -> bool {
    peer_protocol_version >= DELTA_SYNC_PROTOCOL_VERSION
}

/// F8/v9：响应方按对端协议版本判定是否用 OpsBatchV2 发送（纯判定，不做网络动作）。
///
/// ≥ [`DELTA_SYNC_PROTOCOL_VERSION_V2`]（9）→ V2（ops 携带真实 version）；
/// 否则回退旧 OpsBatch（v4-v8，wire 上无 version 字段，字节格式与行为完全不变）。
pub fn supports_ops_batch_v2(peer_protocol_version: u32) -> bool {
    peer_protocol_version >= DELTA_SYNC_PROTOCOL_VERSION_V2
}

/// 安全地把 `u64` seq 转 `i64`（SQLite 存储用）。
pub fn seq_to_i64(seq: u64) -> i64 {
    if seq > i64::MAX as u64 {
        warn!("[delta] seq {} 超出 i64 表示，截断为 i64::MAX", seq);
        i64::MAX
    } else {
        seq as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::protocol::operation;

    #[test]
    fn test_peer_seq_roundtrip() {
        let st = crate::storage::db::Storage::memory().unwrap();
        assert_eq!(st.get_peer_seq(b"peerA", 0).unwrap(), 0);
        st.set_peer_seq(b"peerA", 0, 42).unwrap();
        assert_eq!(st.get_peer_seq(b"peerA", 0).unwrap(), 42);
        // 仅前进：写更小值不生效
        st.set_peer_seq(b"peerA", 0, 10).unwrap();
        assert_eq!(st.get_peer_seq(b"peerA", 0).unwrap(), 42);
        // 不同 repo 独立
        assert_eq!(st.get_peer_seq(b"peerA", 1).unwrap(), 0);
        assert_eq!(st.all_peer_seqs().unwrap().len(), 1);
    }

    #[test]
    fn test_ops_to_sync_entries() {
        let ops = vec![
            OpEntry {
                seq: 1,
                is_delete: false,
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            },
            OpEntry {
                seq: 2,
                is_delete: true,
                key: b"k2".to_vec(),
                value: Vec::new(),
            },
        ];
        let es = ops_to_sync_entries(&ops);
        assert_eq!(es.len(), 2);
        assert_eq!(es[0].operation, operation::UPSERT);
        assert_eq!(es[0].key, b"k1");
        assert_eq!(es[0].payload, b"v1");
        // 旧通道（v4-v8）wire 上无 version 字段，转换结果恒为 0（保守跳过语义）
        assert_eq!(es[0].version, 0);
        assert_eq!(es[1].operation, operation::DELETE);
        assert_eq!(es[1].version, 0);
    }

    /// F8/v9：V2 转换必须透传真实 version —— 这是「对既有条目的更新不再被静默丢弃」
    /// 的转换层闸门（接收端 apply 以 SyncEntry.version 做 LWW）。
    #[test]
    fn test_ops_to_sync_entries_v2() {
        let ops = vec![
            OpEntryV2 {
                seq: 10,
                is_delete: false,
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                version: 1_734_000_000,
            },
            OpEntryV2 {
                seq: 11,
                is_delete: true,
                key: b"k2".to_vec(),
                value: Vec::new(),
                version: 1_734_000_001,
            },
            // oplog 迁移前的历史行：version=0 透传 0（接收端维持旧跳过语义）
            OpEntryV2 {
                seq: 12,
                is_delete: false,
                key: b"k3".to_vec(),
                value: b"v3".to_vec(),
                version: 0,
            },
        ];
        let es = ops_to_sync_entries_v2(&ops);
        assert_eq!(es.len(), 3);
        assert_eq!(es[0].operation, operation::UPSERT);
        assert_eq!(es[0].key, b"k1");
        assert_eq!(es[0].payload, b"v1");
        assert_eq!(es[0].version, 1_734_000_000);
        assert_eq!(es[1].operation, operation::DELETE);
        assert_eq!(es[1].version, 1_734_000_001);
        assert_eq!(es[2].version, 0);
    }

    /// F8/v9：V2 → 旧 OpEntry 降级丢弃 version、其余字段逐项保留
    /// （对 v4-v8 对端回退旧 OpsBatch 的字节兼容保障）。
    #[test]
    fn test_to_legacy_entries() {
        let ops = vec![OpEntryV2 {
            seq: 5,
            is_delete: false,
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            version: 999,
        }];
        let legacy = to_legacy_entries(&ops);
        assert_eq!(legacy.len(), 1);
        assert_eq!(legacy[0].seq, 5);
        assert_eq!(legacy[0].key, b"k1");
        assert_eq!(legacy[0].value, b"v1");
        assert!(!legacy[0].is_delete);
    }

    #[test]
    fn test_records_to_entries() {
        let st = crate::storage::db::Storage::memory().unwrap();
        use crate::federation::protocol::{operation, SyncEntry};
        let entries = vec![
            SyncEntry {
                key: b"a".to_vec(),
                operation: operation::UPSERT,
                version: 7,
                payload: b"pa".to_vec(),
            },
            SyncEntry {
                key: b"b".to_vec(),
                operation: operation::DELETE,
                version: 9,
                payload: Vec::new(),
            },
        ];
        st.append_ops_from_entries(0, &entries).unwrap();
        let recs = st.load_ops_since(0, 0, 10).unwrap();
        let ops = records_to_entries(&recs);
        assert_eq!(ops.len(), 2);
        assert!(!ops[0].is_delete);
        assert_eq!(ops[0].key, b"a");
        assert_eq!(ops[0].value, b"pa");
        // F8：version 必须从 oplog 原样透传（此前旧 OpEntry 无 version 可带）
        assert_eq!(ops[0].version, 7);
        assert!(ops[1].is_delete);
        assert_eq!(ops[1].version, 9);
    }

    /// F8/v9：版本选择逻辑 —— ≥9 走 OpsBatchV2（version 透传），<9 回退旧 OpsBatch。
    #[test]
    fn test_supports_ops_batch_v2_version_selection() {
        assert!(!supports_ops_batch_v2(1));
        assert!(!supports_ops_batch_v2(4));
        assert!(!supports_ops_batch_v2(8));
        assert!(supports_ops_batch_v2(9));
        assert!(supports_ops_batch_v2(10));
    }

    #[test]
    fn test_supports_delta_sync() {
        assert!(!supports_delta_sync(3));
        assert!(supports_delta_sync(4));
        assert!(supports_delta_sync(5));
    }
}
