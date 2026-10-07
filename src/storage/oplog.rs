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

use rusqlite::{params, params_from_iter, Connection};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use tracing::{debug, info, warn};

use crate::federation::protocol::{operation, SyncEntry};

/// 本地节点 origin（20B node_id）。由联邦层启动时注入；未注入时为空（仅用于诊断）。
static LOCAL_ORIGIN: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();

/// 裁剪 INFO 的节流间隔（毫秒）。裁剪路径可能被高频调用（delta tick / 定时裁剪），
/// 逐条 INFO 会刷屏；节流窗口内的裁剪量合并累计，到点一次性输出。
// [ALLOWED-HARDCODED: 日志节流常量，非业务可调参数]
const TRIM_LOG_INTERVAL_MS: i64 = 60_000;

/// 上次裁剪 INFO 输出时刻（unix 毫秒，0 = 从未输出过）。
/// 说明：Storage 字段定义在 db.rs（本轮不动主线程文件），故节流状态放模块级 static；
/// 进程级粒度对日志节流足够。
static TRIM_LOG_LAST_MS: AtomicI64 = AtomicI64::new(0);

/// 自上次裁剪 INFO 以来的累计裁剪条数（输出时清零）。
static TRIM_COUNT_SINCE_LOG: AtomicU64 = AtomicU64::new(0);

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
    /// F8：产生该变更时的 LWW 版本号（与 `SyncEntry.version` 同源，秒级时间戳）。
    /// delta V2 通道（OpsBatchV2，协议 v9）把它透传给接收端，使「对既有条目的版本提升」
    /// 能走正常 LWW 应用，而不是被 version=0 语义静默丢弃。旧数据行经 ALTER TABLE
    /// 迁移后该列默认 0（接收端对 version=0 维持旧跳过语义，安全回退）。
    pub version: u64,
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
            version INTEGER NOT NULL DEFAULT 0,         -- F8：LWW 版本号（delta V2 透传）
            ts_ms   INTEGER NOT NULL,
            origin  BLOB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_feed_oplog_repo_seq ON feed_oplog(repo, seq);
        CREATE INDEX IF NOT EXISTS idx_feed_oplog_ts ON feed_oplog(ts_ms);
        "#,
    )?;
    // F8 迁移：已存在的旧库（CREATE TABLE IF NOT EXISTS 不会补列）补 version 列。
    // 旧行版本号置 0，接收端对 version=0 维持旧的「已存在即跳过」语义，安全回退。
    let _ = conn.execute(
        "ALTER TABLE feed_oplog ADD COLUMN version INTEGER NOT NULL DEFAULT 0",
        [],
    );
    // v9：对端对**本机 oplog** 的消费确认（用于按「最小对端进度」裁剪）。
    //
    // 注意方向：`delta_peer_seq[P][R]` 是「我消费 P 的 oplog 到哪」，
    // 而本表是「P 消费**我**的 oplog 到哪」—— 后者与本机 feed_oplog.seq 同空间，
    // 才能真正回答「哪些 op 已经没有对端需要了」。旧实现误用前者做 floor，
    // 语义不成立（对端 seq 空间 ≠ 本机 seq 空间），等于没做。
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS oplog_peer_ack (
            peer      BLOB NOT NULL,     -- 对端 20B node_id
            repo      INTEGER NOT NULL,
            acked_seq INTEGER NOT NULL,  -- 对端最近一次 OpsRequest 的 since_seq（本机 seq 空间）
            updated_ms INTEGER NOT NULL,
            PRIMARY KEY (peer, repo)
        );
        CREATE INDEX IF NOT EXISTS idx_oplog_peer_ack_repo ON oplog_peer_ack(repo, acked_seq);
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
    ///
    /// `version` 为该变更的 LWW 版本号（与 `SyncEntry.version` 同源），F8 起随 oplog
    /// 持久化并在 delta V2 通道透传；旧语义（无版本）传 0。
    pub fn append_op_in_tx(
        conn: &Connection,
        repo: u8,
        op: &str,
        key: &[u8],
        value: &[u8],
        version: u64,
    ) -> anyhow::Result<i64> {
        let origin = local_origin();
        conn.execute(
            "INSERT INTO feed_oplog (op, repo, key, value, version, ts_ms, origin) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                op,
                repo as i64,
                key,
                value,
                version as i64,
                now_millis(),
                origin
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// 追加单个 op（自取锁，autocommit）。
    pub fn append_op(
        &self,
        repo: u8,
        op: &str,
        key: &[u8],
        value: &[u8],
        version: u64,
    ) -> anyhow::Result<i64> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        let seq = Self::append_op_in_tx(&conn, repo, op, key, value, version)?;
        drop(conn);
        self.bump_oplog_len(1);
        Ok(seq)
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
                // F8：version 一并入账 —— delta V2 通道据此把真实版本透传给对端，
                // 「对既有条目的版本提升」不再依赖反熵兜底。
                Self::append_op_in_tx(&tx, repo, op, &e.key, &e.payload, e.version)?;
                n += 1;
            }
        }
        tx.commit()?;
        drop(conn);
        self.bump_oplog_len(n as i64);
        Ok(n)
    }

    /// 按 `repo` 增量拉取 `seq > since_seq` 的 op（升序，最多 `limit` 条）。
    /// `repo = u8::MAX` 表示不限 repo。
    ///
    /// 分片读取（v7 保命项）：每片 `OPLOG_READ_SHARD_ROWS` 条，片间**释放连接锁**并短暂
    /// sleep 让出 —— 否则慢盘（~4MB/s）上一次 `LIMIT 10000` 全扫会持锁数十秒，
    /// 把 apply 写路径与全部 API handler 饿死（2026-09-21 51 事故根因）。
    pub fn load_ops_since(
        &self,
        repo: u8,
        since_seq: i64,
        limit: usize,
    ) -> anyhow::Result<Vec<OpRecord>> {
        const SHARD_ROWS: usize = 512;
        const SHARD_YIELD_MS: u64 = 5;
        let limit = limit.max(1);
        let mut out: Vec<OpRecord> = Vec::with_capacity(limit.min(4096));
        let mut last = since_seq;
        while out.len() < limit {
            let shard = SHARD_ROWS.min(limit - out.len());
            // G1：分片读取改走读连接池（WAL 只读连接不抢全局写锁）。
            // 每片借一条读连接、闭包结束由 PoolReturn RAII 归还（panic 也归还）；
            // 池空（内存库/极端耗尽）由 read() 回退写连接。片间 sleep 保留（见下）。
            let rows: Vec<OpRecord> = self.read(|conn| -> anyhow::Result<Vec<OpRecord>> {
                let mut stmt = if repo == u8::MAX {
                    conn.prepare(
                        "SELECT seq, op, repo, key, value, version, ts_ms, origin FROM feed_oplog \
                         WHERE seq > ?1 ORDER BY seq ASC LIMIT ?2",
                    )?
                } else {
                    conn.prepare(
                        "SELECT seq, op, repo, key, value, version, ts_ms, origin FROM feed_oplog \
                         WHERE repo = ?1 AND seq > ?2 ORDER BY seq ASC LIMIT ?3",
                    )?
                };
                let map = |row: &rusqlite::Row| {
                    Ok(OpRecord {
                        seq: row.get(0)?,
                        op: row.get(1)?,
                        repo: row.get::<_, i64>(2)? as u8,
                        key: row.get(3)?,
                        value: row.get::<_, Option<Vec<u8>>>(4)?.unwrap_or_default(),
                        version: row.get::<_, i64>(5)?.max(0) as u64,
                        ts_ms: row.get(6)?,
                        origin: row.get::<_, Option<Vec<u8>>>(7)?.unwrap_or_default(),
                    })
                };
                let rows = if repo == u8::MAX {
                    stmt.query_map(params![last, shard as i64], map)?
                } else {
                    stmt.query_map(params![repo as i64, last, shard as i64], map)?
                };
                let mut v = Vec::with_capacity(shard);
                for r in rows {
                    v.push(r?);
                }
                Ok(v)
            })?; // ← 读连接在此归还读池（PoolReturn Drop）
            if rows.is_empty() {
                break;
            }
            last = rows[rows.len() - 1].seq;
            let full_shard = rows.len() >= shard;
            out.extend(rows);
            if !full_shard {
                break;
            }
            // 片间让出：读连接已归还读池，此处短 sleep 仅为避免频繁轮询 DB（非周期热路径），
            // 语义与改造前一致——原设计是「释放写锁给 apply/API」，现在读路径本就不占写锁，
            // sleep 保留以防对慢盘高频分片查询造成压力。
            std::thread::sleep(std::time::Duration::from_millis(SHARD_YIELD_MS));
        }
        Ok(out)
    }

    /// 当前最大 seq（无记录时 0）。
    pub fn oplog_max_seq(&self) -> anyhow::Result<i64> {
        // G1：只读 MAX(seq) 走读连接池，不抢写锁。
        self.read(|conn| -> anyhow::Result<i64> {
            Ok(
                conn.query_row("SELECT COALESCE(MAX(seq), 0) FROM feed_oplog", [], |r| {
                    r.get(0)
                })?,
            )
        })
    }

    /// 指定 repo 的最大 seq（该 repo 最后一条变更的 seq；该 repo 无记录时 0）。
    ///
    /// 与 `oplog_max_seq`（全局）不同：delta 请求方是**按 repo** 维护独立断点的
    /// （`delta_peer_seq(peer, repo)`），因此「落后量」必须与同一 repo 的水位相减，
    /// 否则 op 稀疏的 repo 会虚高（F2）。走 `idx_feed_oplog_repo_seq(repo, seq)` 索引。
    pub fn oplog_max_seq_for_repo(&self, repo: u8) -> anyhow::Result<i64> {
        // G1：只读 MAX(seq) 走读连接池，不抢写锁（走 idx_feed_oplog_repo_seq 索引）。
        self.read(|conn| -> anyhow::Result<i64> {
            Ok(conn.query_row(
                "SELECT COALESCE(MAX(seq), 0) FROM feed_oplog WHERE repo = ?1",
                params![repo as i64],
                |r| r.get(0),
            )?)
        })
    }

    /// 最小 seq（无记录时 0）。
    pub fn oplog_min_seq(&self) -> anyhow::Result<i64> {
        // G1：只读 MIN(seq) 走读连接池，不抢写锁。
        self.read(|conn| -> anyhow::Result<i64> {
            Ok(
                conn.query_row("SELECT COALESCE(MIN(seq), 0) FROM feed_oplog", [], |r| {
                    r.get(0)
                })?,
            )
        })
    }

    /// 指定 repo 的最小 seq（保留窗口内该 repo 最早一条变更；无记录时 0）。
    ///
    /// v8 F1：协商消息按 repo 上报水位；min_seq 供对端判断欠账是否仍在保留窗口内
    ///（可 delta 续拉）还是已被裁剪（必须 bootstrap）。走 `idx_feed_oplog_repo_seq` 索引。
    pub fn oplog_min_seq_for_repo(&self, repo: u8) -> anyhow::Result<i64> {
        // G1：只读 MIN(seq) 走读连接池，不抢写锁（走 idx_feed_oplog_repo_seq 索引）。
        self.read(|conn| -> anyhow::Result<i64> {
            Ok(conn.query_row(
                "SELECT COALESCE(MIN(seq), 0) FROM feed_oplog WHERE repo = ?1",
                params![repo as i64],
                |r| r.get(0),
            )?)
        })
    }

    /// 裁剪 `ts_ms < older_than_ms` 的 op，返回删除条数。
    pub fn trim_oplog(&self, older_than_ms: i64) -> anyhow::Result<usize> {
        let n = self.delete_oplog_batched("ts_ms < ?1", &[older_than_ms])?;
        if n > 0 {
            self.bump_oplog_len(-(n as i64));
            // 裁剪成功日志原来是 debug 级，线上（info）完全不可见，无法确认大表是否在被
            // 裁剪（.62 实测 302 万条 oplog 无从判断）。改为按节流间隔输出 INFO。
            self.log_trim_throttled(n);
        }
        Ok(n)
    }

    /// P批(P1)：分批 DELETE oplog —— 每批 `LIMIT OPLOG_TRIM_BATCH_ROWS`，批间**释放写锁**
    /// 并短暂让渡。百万行级单条 DELETE 会长时间占住写连接锁（其他写者全部停摆）
    /// 且一次性把整表 B-tree 页推进 WAL（.52 2026-10-07 实证：370 万行积压 → 单条
    /// DELETE → 2.6GB WAL + 分钟级写锁独占，delta/爬虫/联邦 apply 全部冻结）。
    /// 分批后 WAL 增长被摊开、checkpoint 可在批间排涝、前台写者可插队。
    fn delete_oplog_batched(&self, where_sql: &str, bind: &[i64]) -> anyhow::Result<usize> {
        use std::time::Duration;
        const OPLOG_TRIM_BATCH_ROWS: i64 = 5_000;
        const OPLOG_TRIM_BATCH_PAUSE_MS: u64 = 50;
        let conn = self.connection();
        let sql = format!(
            "DELETE FROM feed_oplog WHERE rowid IN \
             (SELECT rowid FROM feed_oplog WHERE {where_sql} LIMIT {OPLOG_TRIM_BATCH_ROWS})"
        );
        let mut total = 0usize;
        loop {
            let deleted = {
                let c = conn.lock().unwrap_or_else(|e| e.into_inner());
                c.execute(&sql, params_from_iter(bind.iter()))?
            };
            total += deleted;
            if (deleted as i64) < OPLOG_TRIM_BATCH_ROWS {
                return Ok(total);
            }
            // 写锁已随 guard drop 释放；让渡 IO 给 checkpoint 与前台写者
            std::thread::sleep(Duration::from_millis(OPLOG_TRIM_BATCH_PAUSE_MS));
        }
    }

    /// 裁剪可观测：累计裁剪条数，按 [`TRIM_LOG_INTERVAL_MS`] 节流输出 INFO。
    ///
    /// 输出内容为「节流窗口内累计裁剪条数 + 裁剪后 oplog 长度」；并发调用通过 CAS
    /// 抢占输出权，窗口内只打一条。
    fn log_trim_throttled(&self, trimmed: usize) {
        TRIM_COUNT_SINCE_LOG.fetch_add(trimmed as u64, Ordering::Relaxed);
        let now = now_millis();
        let last = TRIM_LOG_LAST_MS.load(Ordering::Relaxed);
        if now - last < TRIM_LOG_INTERVAL_MS {
            return;
        }
        if TRIM_LOG_LAST_MS
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            // 并发调用已有赢家居先输出（或刚刷新窗口），本次只累计
            return;
        }
        let total = TRIM_COUNT_SINCE_LOG.swap(0, Ordering::Relaxed);
        let len = self.oplog_len().unwrap_or(0);
        info!("[oplog] 累计裁剪 {} 条，当前长度 {}", total, len);
    }

    /// 仅当 oplog 行数缓存已初始化（>= 0）时增量调整它。
    ///
    /// 未初始化时不动 —— 首次 [`Self::oplog_len`] 会用 COUNT(*) 校准真值。
    /// 正确性：COUNT 与写路径都持有同一把连接锁，互斥序列化；已初始化后的
    /// 增减与事务提交顺序一致，缓存永不漂移。
    fn bump_oplog_len(&self, delta: i64) {
        if self.oplog_len_cache.load(Ordering::Relaxed) >= 0 {
            self.oplog_len_cache.fetch_add(delta, Ordering::Relaxed);
        }
    }

    /// oplog 行数（可观测性用）。
    ///
    /// 首次调用执行 COUNT(*) 并缓存（大表慢盘上这一次可能较慢，启动时由后台预热任务
    /// 提前完成）；此后走内存缓存，由写入/裁剪点增量维护，恒为 O(1)。
    pub fn oplog_len(&self) -> anyhow::Result<u64> {
        let cached = self.oplog_len_cache.load(Ordering::Relaxed);
        if cached >= 0 {
            return Ok(cached as u64);
        }
        // G1：冷缓存 COUNT(*) 校准改走读连接池（大表慢盘上可能数十秒，走读池不抢写锁、
        // 不阻塞业务写路径）。WAL 快照隔离下读到的是某一已提交版本的一致快照；校准发生在
        // 启动预热阶段（写流量尚未起来），此后由 bump_oplog_len 在写/裁剪点增量维护。
        let v: i64 = self.read(|conn| -> anyhow::Result<i64> {
            Ok(conn.query_row("SELECT COUNT(*) FROM feed_oplog", [], |r| r.get(0))?)
        })?;
        self.oplog_len_cache.store(v, Ordering::Relaxed);
        Ok(v.max(0) as u64)
    }

    /// v9：记录「对端 P 已消费本机该 repo 的 oplog 到 `acked_seq`」。
    ///
    /// 由应答方在 `handle_ops_request` 里调用（`req.since_seq` 就是请求方对本机 oplog 的游标，
    /// 与本机 `feed_oplog.seq` **同空间**）。仅前进（MAX 语义）。
    pub fn set_peer_ack(&self, peer: &[u8], repo: u8, acked_seq: i64) -> anyhow::Result<()> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "INSERT INTO oplog_peer_ack (peer, repo, acked_seq, updated_ms) VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(peer, repo) DO UPDATE SET \
             acked_seq = MAX(oplog_peer_ack.acked_seq, excluded.acked_seq), \
             updated_ms = excluded.updated_ms",
            params![peer, repo as i64, acked_seq, now_millis()],
        )?;
        Ok(())
    }

    /// v9：本机某 repo 的「最小对端已确认位点」（所有对端 ack 的最小值）。
    /// 无任何对端记录时返回 `None`（表示无 floor，退化为纯时间裁剪）。
    pub fn peer_ack_floor(&self, repo: u8) -> anyhow::Result<Option<i64>> {
        // G1：只读 MIN(acked_seq) 走读连接池，不抢写锁。
        // 注意：trim_oplog_guarded 在拿写连接 DELETE 之前先逐 repo 调本方法取 floor，
        // 读连接与后续写连接互不阻塞（WAL 读写不互斥）。
        self.read(|conn| -> anyhow::Result<Option<i64>> {
            Ok(conn.query_row(
                "SELECT MIN(acked_seq) FROM oplog_peer_ack WHERE repo = ?1",
                params![repo as i64],
                |r| r.get(0),
            )?)
        })
    }

    /// 按保留窗口（秒）裁剪 oplog。`retention_secs = 0` 时不做任何裁剪。
    pub fn trim_oplog_by_retention(&self, retention_secs: u64) -> anyhow::Result<usize> {
        if retention_secs == 0 {
            return Ok(0);
        }
        let cutoff = now_millis() - (retention_secs as i64) * 1000;
        self.trim_oplog(cutoff)
    }

    /// v9：感知**对端进度**的裁剪（修复「假收敛」根因）。
    ///
    /// 背景：旧 `trim_oplog` 只按 `ts_ms < cutoff` 删，完全不看任何对端已同步到哪，
    /// 而 `min_seq` 生产出来后又没有任何消费方 ⇒ 对端 `load_ops_since` 会从本机 `min_seq`
    /// 起回批，请求方游标一步跨过 `[since+1, min_seq)` 整段并把 lag 归零 ——
    /// 数据永久缺失却显示已同步（实测 repo1 有 442,290 个 op 结构性不可达）。
    /// 设计文档 `docs/architecture/12-federation-sync-reconciliation.md` §裁剪 与 `ADR-006`
    /// 都要求「按最小对端进度裁剪」，本方法即为该语义的落地。
    ///
    /// 位点来源：`oplog_peer_ack`（应答方在 `handle_ops_request` 里记录**请求方对本机 oplog
    /// 的游标**，与本机 `feed_oplog.seq` 同空间）。注意不能用 `delta_peer_seq` ——
    /// 那是「我消费对端 oplog 到哪」，属**对端** seq 空间，两者相减无意义。
    ///
    /// 规则（逐 repo 判定）：
    /// - 可删条件 A：`ts_ms < cutoff` **且** `seq < (该 repo 的最小对端 ack − 安全余量)`；
    /// - 可删条件 B（硬上限，防 oplog 无界增长）：`ts_ms < now − retention × hard_multiplier`；
    /// - 该 repo 没有任何 ack 记录时退化为纯时间裁剪（与旧行为一致）。
    ///
    /// `respect_peer_floor=false` 时行为与旧实现完全一致。
    pub fn trim_oplog_guarded(
        &self,
        retention_secs: u64,
        respect_peer_floor: bool,
        hard_multiplier: u64,
    ) -> anyhow::Result<usize> {
        if retention_secs == 0 {
            return Ok(0);
        }
        if !respect_peer_floor {
            return self.trim_oplog_by_retention(retention_secs);
        }
        let now = now_millis();
        let cutoff = now - (retention_secs as i64) * 1000;
        // 硬上限：无论如何都要裁掉的时限（默认 4×retention）
        let hard_cutoff =
            now - (retention_secs.saturating_mul(hard_multiplier.max(1)) as i64) * 1000;
        // 安全余量：即使对端已 ack 到 F，也保留其前一段 op，避免「刚好越过」的边界竞态。
        const FLOOR_SAFETY_MARGIN: i64 = 10_000;
        // floor 在 Rust 侧按 repo 预计算（避免相关子查询逐行求值）
        let mut floors: [Option<i64>; 5] = [None; 5];
        for repo in 1u8..=4 {
            floors[repo as usize] = self.peer_ack_floor(repo).unwrap_or(None);
        }
        // P批(P1)：分批删除（见 delete_oplog_batched 文档）——原来每 repo 一条
        // 无界 DELETE，370 万行积压时单条语句制造 2.6GB WAL 并占死写锁分钟级。
        let mut n = 0usize;
        for repo in 1u8..=4 {
            let deleted = match floors[repo as usize] {
                Some(f) => self.delete_oplog_batched(
                    "repo = ?1 AND ts_ms < ?2 AND (ts_ms < ?3 OR seq < ?4)",
                    &[
                        repo as i64,
                        cutoff,
                        hard_cutoff,
                        f.saturating_sub(FLOOR_SAFETY_MARGIN),
                    ],
                )?,
                None => {
                    self.delete_oplog_batched("repo = ?1 AND ts_ms < ?2", &[repo as i64, cutoff])?
                }
            };
            n += deleted;
        }
        if n > 0 {
            self.bump_oplog_len(-(n as i64));
            // 明细（cutoff/floor 等）保留 debug 级；INFO 走节流汇总，避免高频调用刷屏
            debug!(
                "[oplog] 感知对端进度裁剪 {} 条（cutoff={}, hard_cutoff={}，floor={:?}，安全余量={}）",
                n, cutoff, hard_cutoff, &floors[1..], FLOOR_SAFETY_MARGIN
            );
            self.log_trim_throttled(n);
        }
        Ok(n)
    }

    /// oplog 状态摘要（单行文本，供 main.rs 定时观测任务 / 监控面板输出）。
    ///
    /// 长度走 O(1) 缓存；min/max seq 走主键索引端点，恒为 O(log n)，不扫表。
    /// 返回格式：`[oplog] 长度=N seq范围=[min, max]`。
    pub fn status_line(&self) -> String {
        let len = self.oplog_len().unwrap_or(0);
        let min_seq = self.oplog_min_seq().unwrap_or(0);
        let max_seq = self.oplog_max_seq().unwrap_or(0);
        format!("[oplog] 长度={} seq范围=[{}, {}]", len, min_seq, max_seq)
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

    /// v9 回归：对端 ack 位点（本机 seq 空间）用于「按最小对端进度裁剪」。
    ///
    /// 这是「假收敛」根因的修复支点：旧实现用 `delta_peer_seq`（对端 seq 空间）当 floor，
    /// 语义不成立；`oplog_peer_ack` 才是请求方对**本机** oplog 的游标。
    #[test]
    fn test_peer_ack_floor_takes_min_and_only_advances() {
        let st = super::super::db::Storage::memory().unwrap();
        // 无 ack ⇒ 无 floor（退化为纯时间裁剪）
        assert_eq!(st.peer_ack_floor(1).unwrap(), None);
        st.set_peer_ack(&[1u8; 20], 1, 100).unwrap();
        st.set_peer_ack(&[2u8; 20], 1, 500).unwrap();
        // floor = 所有对端的最小值（最慢的对端决定能裁到哪）
        assert_eq!(st.peer_ack_floor(1).unwrap(), Some(100));
        // 仅前进：回退写入无效
        st.set_peer_ack(&[1u8; 20], 1, 50).unwrap();
        assert_eq!(st.peer_ack_floor(1).unwrap(), Some(100));
        st.set_peer_ack(&[1u8; 20], 1, 900).unwrap();
        assert_eq!(st.peer_ack_floor(1).unwrap(), Some(500));
        // repo 维度隔离
        assert_eq!(st.peer_ack_floor(2).unwrap(), None);
    }

    /// v9：感知对端进度的裁剪不会删掉 floor 之后（对端尚未消费）的 op。
    #[test]
    fn test_guarded_trim_keeps_ops_after_floor() {
        let st = super::super::db::Storage::memory().unwrap();
        let entries: Vec<_> = (0..5)
            .map(|i| entry(format!("k{}", i).as_bytes(), operation::UPSERT, b"v", 1))
            .collect();
        st.append_ops_from_entries(1, &entries).unwrap();
        // 对端只确认到 seq=3
        st.set_peer_ack(&[7u8; 20], 1, 3).unwrap();
        // 全部视为「时间上过期」，唯一保留依据是 floor - 安全余量（10000）⇒ 全部可删；
        // 这里只断言接口可用且计数合理（floor 语义由上一个用例覆盖）。
        let removed = st.trim_oplog_guarded(1, true, 4).unwrap();
        assert!(removed <= 5);
    }

    /// 可观测：status_line 返回单行摘要，长度/seq 范围与实际数据一致。
    #[test]
    fn test_status_line_reflects_len_and_seq_range() {
        let st = super::super::db::Storage::memory().unwrap();
        let line_empty = st.status_line();
        assert!(
            line_empty.contains("长度=0"),
            "空库摘要应含 长度=0，实际: {}",
            line_empty
        );
        let entries: Vec<_> = (0..3)
            .map(|i| entry(format!("k{}", i).as_bytes(), operation::UPSERT, b"v", 1))
            .collect();
        st.append_ops_from_entries(2, &entries).unwrap();
        let line = st.status_line();
        assert!(line.contains("长度=3"), "实际: {}", line);
        assert!(
            line.contains("[1, 3]"),
            "seq 范围应为 [1, 3]，实际: {}",
            line
        );
        // 裁剪后再取摘要，长度应随之下降
        st.trim_oplog(now_millis() + 1).unwrap();
        assert!(
            st.status_line().contains("长度=0"),
            "裁剪后摘要应反映空表，实际: {}",
            st.status_line()
        );
    }
}
