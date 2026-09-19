//! P1-2：变更日志（oplog）—— 联邦增量同步（delta sync）的权威来源。
//!
//! 设计见 `docs/architecture/12-federation-sync-reconciliation.md` §6.2 / §6.3。
//!
//! 动机（根治「差异散落全部分片 → 每轮重传整表」的永久闭环）：
//! 稳态同步不应再依赖「全表求差集」，而应由**本地变更日志**驱动 —— 对端只需
//! `OpsRequest { repo, since_seq }`，本端回 `OpsBatch { ops, next_seq, has_more }`，
//! 成本 O(Δ)（Δ = 单轮新增变更数），与库总量 N、与差异量 d 都无关。
//!
//! 关键约束：
//! - `feed_oplog` 表由本模块拥有，建表走 [`init_oplog_table`]，在 `Storage::init_tables` 中调用。
//! - **本地产生的变更**才写 oplog；入站 apply（对端发来的数据）**不写回**，否则 A→B→A 回环。
//! - 保留窗口必须 > 预估 bootstrap 时长（见架构文档铁律 4），默认 24h，可通过配置调整。
//! - 裁剪按 `ts_ms < cutoff` 进行；`seq` 全局单调，对端以 `since_seq` 作为断点。
//!
//! 写入时机说明：本模块的写入点在**联邦变更产生处**（repo `propagate` / `remove_node` 等，
//! 与 `GossipEngine::submit_gossip` 同一逻辑时刻），而非 SQLite 行 upsert 的同一事务内 ——
//! 业务写入路径（`save_*_in_tx`）拿不到完整 `SyncEntry`（key/op/version/payload）。
//! 若进程在「落库后、写 oplog 前」崩溃，最坏情况是该条变更不进 delta 通道，由周期反熵兜底收敛。
//! 任何 oplog 写入失败都只告警，**绝不阻断业务写入**。

use rusqlite::{params, Connection};
use tracing::{debug, warn};

use crate::federation::protocol::{operation, SyncEntry};

/// 本地节点 origin（20B node_id）。由联邦层启动时注入；未注入时为空（仅用于诊断）。
static LOCAL_ORIGIN: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();

/// 注入本地节点 origin（幂等，仅首次生效）。
pub fn set_local_origin(origin: Vec<u8>) {
    let _ = LOCAL_ORIGIN.set(origin);
}

/// 读取本地节点 origin（未注入时返回空 vec）。
pub fn local_origin() -> Vec<u8> {
    LOCAL_ORIGIN.get().cloned().unwrap_or_default()
}

/// upsert 操作码
pub const OP_UPSERT: &str = "upsert";
/// delete 操作码
pub const OP_DELETE: &str = "delete";

/// 一条 oplog 记录。
#[derive(Debug, Clone)]
pub struct OpRecord {
    pub seq: i64,
    pub op: String,
    pub repo: u8,
    pub key: Vec<u8>,
    /// upsert 时携带 payload；delete 时为空
    pub value: Vec<u8>,
    pub ts_ms: i64,
    pub origin: Vec<u8>,
}

/// 建 oplog 表（幂等）。由 `Storage::init_tables` 调用。
pub fn init_oplog_table(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS feed_oplog (
            seq     INTEGER PRIMARY KEY AUTOINCREMENT,  -- 全局单调，对端 since_seq 断点
            op      TEXT NOT NULL,                      -- 'upsert' | 'delete'
            repo    INTEGER NOT NULL,                   -- 0..3
            key     BLOB NOT NULL,
            value   BLOB,                               -- upsert 携带 payload；delete 为空
            ts_ms   INTEGER NOT NULL,
            origin  BLOB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_feed_oplog_repo_seq ON feed_oplog(repo, seq);
        CREATE INDEX IF NOT EXISTS idx_feed_oplog_ts ON feed_oplog(ts_ms);
        "#,
    )?;
    Ok(())
}

/// 当前 unix 毫秒
fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl super::db::Storage {
    /// 在已有连接上追加一条 op（调用方负责事务/锁）。返回新 op 的 seq。
    pub fn append_op_in_tx(
        conn: &Connection,
        repo: u8,
        op: &str,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<i64> {
        let origin = local_origin();
        conn.execute(
            "INSERT INTO feed_oplog (op, repo, key, value, ts_ms, origin) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![op, repo as i64, key, value, now_millis(), origin],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// 追加单个 op（自取锁，autocommit）。
    pub fn append_op(&self, repo: u8, op: &str, key: &[u8], value: &[u8]) -> anyhow::Result<i64> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        Self::append_op_in_tx(&conn, repo, op, key, value)
    }

    /// 把一批**本地产生的**联邦变更（`SyncEntry`）追加进 oplog（一次锁 + 一个事务）。
    ///
    /// 仅用于本地变更点（repo `propagate` / `remove_node` 等），**严禁**用于入站 apply 或
    /// 全量 bootstrap 推送（`run_full_sync_push`），否则会把对端数据/整表回灌成"本地变更"。
    /// 失败仅告警并返回 `Ok(0)`，不阻断业务写入。
    pub fn append_ops_from_entries(
        &self,
        repo: u8,
        entries: &[SyncEntry],
    ) -> anyhow::Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn.unchecked_transaction()?;
        let mut n = 0usize;
        {
            for e in entries {
                let op = if e.operation == operation::DELETE {
                    OP_DELETE
                } else {
                    OP_UPSERT
                };
                Self::append_op_in_tx(&tx, repo, op, &e.key, &e.payload)?;
                n += 1;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    /// 按 `repo` 增量拉取 `seq > since_seq` 的 op（升序，最多 `limit` 条）。
    /// `repo = u8::MAX` 表示不限 repo。
    pub fn load_ops_since(
        &self,
        repo: u8,
        since_seq: i64,
        limit: usize,
    ) -> anyhow::Result<Vec<OpRecord>> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        let limit = limit.max(1) as i64;
        let mut out: Vec<OpRecord> = Vec::new();
        if repo == u8::MAX {
            let mut stmt = conn.prepare(
                "SELECT seq, op, repo, key, value, ts_ms, origin FROM feed_oplog \
                 WHERE seq > ?1 ORDER BY seq ASC LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![since_seq, limit], |row| {
                Ok(OpRecord {
                    seq: row.get(0)?,
                    op: row.get(1)?,
                    repo: row.get::<_, i64>(2)? as u8,
                    key: row.get(3)?,
                    value: row.get::<_, Option<Vec<u8>>>(4)?.unwrap_or_default(),
                    ts_ms: row.get(5)?,
                    origin: row.get::<_, Option<Vec<u8>>>(6)?.unwrap_or_default(),
                })
            })?;
            for r in rows {
                out.push(r?);
            }
        } else {
            let mut stmt = conn.prepare(
                "SELECT seq, op, repo, key, value, ts_ms, origin FROM feed_oplog \
                 WHERE repo = ?1 AND seq > ?2 ORDER BY seq ASC LIMIT ?3",
            )?;
            let rows = stmt.query_map(params![repo as i64, since_seq, limit], |row| {
                Ok(OpRecord {
                    seq: row.get(0)?,
                    op: row.get(1)?,
                    repo: row.get::<_, i64>(2)? as u8,
                    key: row.get(3)?,
                    value: row.get::<_, Option<Vec<u8>>>(4)?.unwrap_or_default(),
                    ts_ms: row.get(5)?,
                    origin: row.get::<_, Option<Vec<u8>>>(6)?.unwrap_or_default(),
                })
            })?;
            for r in rows {
                out.push(r?);
            }
        }
        Ok(out)
    }

    /// 当前最大 seq（无记录时 0）。
    pub fn oplog_max_seq(&self) -> anyhow::Result<i64> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        let v: i64 = conn.query_row("SELECT COALESCE(MAX(seq), 0) FROM feed_oplog", [], |r| {
            r.get(0)
        })?;
        Ok(v)
    }

    /// 最小 seq（无记录时 0）。
    pub fn oplog_min_seq(&self) -> anyhow::Result<i64> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        let v: i64 = conn.query_row("SELECT COALESCE(MIN(seq), 0) FROM feed_oplog", [], |r| {
            r.get(0)
        })?;
        Ok(v)
    }

    /// 裁剪 `ts_ms < older_than_ms` 的 op，返回删除条数。
    pub fn trim_oplog(&self, older_than_ms: i64) -> anyhow::Result<usize> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        let n = conn.execute(
            "DELETE FROM feed_oplog WHERE ts_ms < ?1",
            params![older_than_ms],
        )?;
        if n > 0 {
            debug!("[oplog] 裁剪 {} 条（ts_ms < {}）", n, older_than_ms);
        }
        Ok(n)
    }

    /// oplog 行数（可观测性用）。
    pub fn oplog_len(&self) -> anyhow::Result<u64> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        let v: i64 = conn.query_row("SELECT COUNT(*) FROM feed_oplog", [], |r| r.get(0))?;
        Ok(v.max(0) as u64)
    }

    /// 按保留窗口（秒）裁剪 oplog。`retention_secs = 0` 时不做任何裁剪。
    pub fn trim_oplog_by_retention(&self, retention_secs: u64) -> anyhow::Result<usize> {
        if retention_secs == 0 {
            return Ok(0);
        }
        let cutoff = now_millis() - (retention_secs as i64) * 1000;
        self.trim_oplog(cutoff)
    }
}

/// 便捷包装：记录一批本地变更；失败只告警不抛出（业务路径不应被 oplog 影响）。
pub fn record_local_ops(storage: &super::db::Storage, repo: u8, entries: &[SyncEntry]) {
    if entries.is_empty() {
        return;
    }
    if let Err(e) = storage.append_ops_from_entries(repo, entries) {
        warn!("[oplog] 记录本地变更失败（不影响业务写入）: {}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::protocol::{operation, SyncEntry};

    fn entry(key: &[u8], op: u8, payload: &[u8], version: u64) -> SyncEntry {
        SyncEntry {
            key: key.to_vec(),
            operation: op,
            version,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn test_oplog_append_and_load_since() {
        let st = super::super::db::Storage::memory().unwrap();
        // 空表
        assert_eq!(st.oplog_max_seq().unwrap(), 0);
        assert!(st.load_ops_since(0, 0, 10).unwrap().is_empty());

        // 追加 upsert + delete
        let entries = vec![
            entry(b"1.2.3.4:6881", operation::UPSERT, b"payload-A", 100),
            entry(b"5.6.7.8:6881", operation::DELETE, b"", 101),
        ];
        let n = st.append_ops_from_entries(0, &entries).unwrap();
        assert_eq!(n, 2);
        assert_eq!(st.oplog_len().unwrap(), 2);
        assert_eq!(st.oplog_max_seq().unwrap(), 2);

        let ops = st.load_ops_since(0, 0, 10).unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].op, OP_UPSERT);
        assert_eq!(ops[0].key, b"1.2.3.4:6881");
        assert_eq!(ops[0].value, b"payload-A");
        assert_eq!(ops[1].op, OP_DELETE);
        assert!(ops[1].value.is_empty());

        // since_seq 断点：只要 seq>1 的那条
        let ops = st.load_ops_since(0, 1, 10).unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].seq, 2);

        // 按 repo 过滤：repo=1 无记录
        assert!(st.load_ops_since(1, 0, 10).unwrap().is_empty());
        // limit 生效
        assert_eq!(st.load_ops_since(0, 0, 1).unwrap().len(), 1);
    }

    #[test]
    fn test_oplog_trim_by_retention() {
        let st = super::super::db::Storage::memory().unwrap();
        st.append_ops_from_entries(0, &[entry(b"k1", operation::UPSERT, b"v", 1)])
            .unwrap();
        // 把所有记录视为过期
        let removed = st.trim_oplog(now_millis() + 1).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(st.oplog_len().unwrap(), 0);
        // retention=0 表示不裁剪
        st.append_ops_from_entries(0, &[entry(b"k2", operation::UPSERT, b"v", 2)])
            .unwrap();
        assert_eq!(st.trim_oplog_by_retention(0).unwrap(), 0);
        assert_eq!(st.oplog_len().unwrap(), 1);
    }
}
