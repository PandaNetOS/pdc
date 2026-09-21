//! P1-3：oplog 增量（delta）同步通道。
//!
//! 设计见 `docs/architecture/12-federation-sync-reconciliation.md` §5.2 / §6.3。
//!
//! 与反熵（Merkle 对账，兜底）的分工：
//! - **稳态主线 = delta**：对端只发 `OpsRequest { repo, since_seq }`，本端从 `feed_oplog`
//!   取 `seq > since_seq` 的变更回 `OpsBatch`，成本 **O(Δ)**（Δ = 单轮新增变更数），
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

use crate::federation::protocol::{operation, OpEntry, SyncEntry};
use crate::storage::oplog::OpRecord;

/// 支持 delta 通道的协议版本（用于握手能力协商）。
pub const DELTA_SYNC_PROTOCOL_VERSION: u32 = 4;

/// 单批默认上限（条）。1万条批在慢盘上会造成读/apply 长时间持锁（51 事故根因），
/// 降到千级使单批持锁时间从数十秒降到亚秒级。
pub const DELTA_BATCH_LIMIT_DEFAULT: u32 = 1_000;

/// 单批字节上限（不含帧头）：OpsBatch 组装时按 key+payload 累计估算，超过即截断本批。
/// 与 `gossip_bulk_max_bytes` 同量级，防止大 value 场景下批帧失控。
pub const DELTA_BATCH_MAX_BYTES: usize = 2 * 1024 * 1024;

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

/// 把 `OpsBatch.ops` 转换成统一的 `SyncEntry`，供既有 `apply_*_sync` 幂等应用。
///
/// `version` 置 0：delta 语义是「按 key 覆盖」，与既有反熵/全量的 LWW 版本无关。
/// 既有 `apply_node_sync` 在 `version == 0` 且本地已存在时会跳过（避免旧数据覆盖新数据），
/// 因此删除不受影响；upsert 与本地重叠的少量条目会被跳过（delta 的 key 以「本地缺失」为主，
/// 跳过量很小且安全 —— 真正的收敛由周期反熵兜底）。
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

/// 从 oplog `OpRecord` 列表构建 `OpEntry` 列表（供接收方组装 OpsBatch）。
pub fn records_to_entries(records: &[OpRecord]) -> Vec<OpEntry> {
    records
        .iter()
        .map(|r| OpEntry {
            seq: r.seq.max(0) as u64,
            is_delete: r.op == crate::storage::oplog::OP_DELETE,
            key: r.key.clone(),
            value: r.value.clone(),
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
        assert_eq!(es[1].operation, operation::DELETE);
    }

    #[test]
    fn test_records_to_entries() {
        let st = crate::storage::db::Storage::memory().unwrap();
        use crate::federation::protocol::{operation, SyncEntry};
        let entries = vec![
            SyncEntry {
                key: b"a".to_vec(),
                operation: operation::UPSERT,
                version: 1,
                payload: b"pa".to_vec(),
            },
            SyncEntry {
                key: b"b".to_vec(),
                operation: operation::DELETE,
                version: 2,
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
        assert!(ops[1].is_delete);
    }

    #[test]
    fn test_supports_delta_sync() {
        assert!(!supports_delta_sync(3));
        assert!(supports_delta_sync(4));
        assert!(supports_delta_sync(5));
    }
}
