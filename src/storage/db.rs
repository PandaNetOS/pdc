//! SQLite 持久化存储
//!
//! 存储路由表、tracker 池、历史数据、统计数据。

#![allow(clippy::type_complexity)]

use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection};
use tracing::{debug, info};

use blake3;

/// 写入统计（用于定位 IO 来源）
#[derive(Debug, Clone, Default)]
pub struct WriteStats {
    pub dht_nodes_writes: u64,
    pub dht_nodes_rows: u64,
    pub peers_writes: u64,
    pub peers_rows: u64,
    pub peer_history_writes: u64,
    pub peer_history_rows: u64,
    pub trackers_writes: u64,
    pub trackers_rows: u64,
    pub infohashes_writes: u64,
    pub infohashes_rows: u64,
    pub stats_writes: u64,
    pub total_writes: u64,
}

/// 存储层
pub struct Storage {
    conn: Arc<Mutex<Connection>>,
    write_stats: Arc<Mutex<WriteStats>>,
}

impl Storage {
    /// 打开或创建数据库（使用默认 SQLite 配置）
    pub fn open<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        Self::open_with_config(path, &crate::config::SqliteConfig::default())
    }

    /// 打开或创建数据库（使用指定 SQLite 配置）
    ///
    /// 所有 PRAGMA 参数均来自配置，禁止硬编码。
    /// mmap_size 默认 2GB：Windows 上 mmap 虽可能造成额外磁盘 IO，但对千万级数据量
    /// 的查询性能提升显著，作为性能调优选项可配置为 0 禁用。
    pub fn open_with_config<P: AsRef<Path>>(
        path: P,
        config: &crate::config::SqliteConfig,
    ) -> anyhow::Result<Self> {
        let path_ref = path.as_ref();
        if let Some(parent) = path_ref.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let conn = Connection::open(path_ref)?;
        let pragma_sql = format!(
            "PRAGMA journal_mode=WAL;              PRAGMA synchronous={};              PRAGMA mmap_size={};              PRAGMA cache_size={};              PRAGMA temp_store={};              PRAGMA wal_autocheckpoint={};              PRAGMA busy_timeout={};",
            config.synchronous,
            config.mmap_size,
            config.cache_size,
            config.temp_store,
            config.wal_autocheckpoint,
            config.busy_timeout_ms,
        );
        conn.execute_batch(&pragma_sql)?;

        let storage = Self {
            conn: Arc::new(Mutex::new(conn)),
            write_stats: Arc::new(Mutex::new(WriteStats::default())),
        };
        storage.init_tables()?;
        info!("[storage] 数据库已打开: {:?}", path_ref);
        Ok(storage)
    }

    /// 内存数据库（用于测试）
    pub fn memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        let storage = Self {
            conn: Arc::new(Mutex::new(conn)),
            write_stats: Arc::new(Mutex::new(WriteStats::default())),
        };
        storage.init_tables()?;
        Ok(storage)
    }

    /// 获取写入统计
    pub fn write_stats(&self) -> WriteStats {
        self.write_stats
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// 获取底层连接的 Arc<Mutex<Connection>>（用于 WriteQueue）
    pub fn connection(&self) -> Arc<Mutex<Connection>> {
        self.conn.clone()
    }

    /// 记录写入统计
    fn record_write(&self, table: &str, rows: u64) {
        let mut stats = self.write_stats.lock().unwrap_or_else(|e| e.into_inner());
        stats.total_writes += 1;
        match table {
            "dht_nodes" => {
                stats.dht_nodes_writes += 1;
                stats.dht_nodes_rows += rows;
            }
            "peers" => {
                stats.peers_writes += 1;
                stats.peers_rows += rows;
            }
            "peer_history" => {
                stats.peer_history_writes += 1;
                stats.peer_history_rows += rows;
            }
            "trackers" => {
                stats.trackers_writes += 1;
                stats.trackers_rows += rows;
            }
            "infohashes" => {
                stats.infohashes_writes += 1;
                stats.infohashes_rows += rows;
            }
            "stats" => {
                stats.stats_writes += 1;
            }
            _ => {}
        }
    }

    /// 手动执行 WAL checkpoint（将 WAL 合并到主数据库文件）
    /// PASSIVE 模式：不阻塞写入，日常高频使用
    pub fn checkpoint(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
        debug!("[storage] WAL checkpoint(PASSIVE) 已执行");
        Ok(())
    }

    /// 执行 WAL checkpoint 并截断 WAL 文件
    /// TRUNCATE 模式：会阻塞写入，但会将 WAL 文件压缩到最小
    /// 建议低频调用（如每小时一次），避免 IO 尖峰
    pub fn checkpoint_truncate(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        info!("[storage] WAL checkpoint(TRUNCATE) 已执行，WAL 已压缩");
        Ok(())
    }

    /// 在已有连接上执行 WAL checkpoint（供 IOScheduler 回调使用）
    /// 使用 PASSIVE 模式，不阻塞、不全量写回，避免 IO 尖峰
    pub fn checkpoint_in_tx(conn: &Connection) -> anyhow::Result<()> {
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
        Ok(())
    }

    /// 执行 VACUUM（清理碎片，压缩数据库）
    pub fn vacuum(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute_batch("VACUUM;")?;
        info!("[storage] VACUUM 已完成");
        Ok(())
    }

    /// 初始化表结构
    fn init_tables(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS dht_nodes (
                id BLOB NOT NULL,
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                score REAL DEFAULT 0,
                state TEXT DEFAULT 'Good',
                query_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                total_latency_ms INTEGER DEFAULT 0,
                consecutive_failures INTEGER DEFAULT 0,
                nodes_returned INTEGER DEFAULT 0,
                last_query_time INTEGER,
                last_active INTEGER,
                first_seen INTEGER,
                l2_shard INTEGER DEFAULT 0,
                PRIMARY KEY (ip, port)
            );

            CREATE TABLE IF NOT EXISTS trackers (
                url TEXT PRIMARY KEY,
                score REAL DEFAULT 0,
                state TEXT DEFAULT 'active',
                total_requests INTEGER DEFAULT 0,
                success_requests INTEGER DEFAULT 0,
                failed_requests INTEGER DEFAULT 0,
                total_peers_discovered INTEGER DEFAULT 0,
                total_response_time_ms REAL DEFAULT 0,
                consecutive_failures INTEGER DEFAULT 0,
                disabled INTEGER DEFAULT 0,
                last_used INTEGER,
                l2_shard INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS infohashes (
                infohash BLOB PRIMARY KEY,
                ref_count INTEGER DEFAULT 1,
                first_source TEXT,
                first_seen INTEGER,
                last_seen INTEGER,
                score REAL DEFAULT 0,
                l2_shard INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS peers (
                infohash BLOB NOT NULL,
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                source TEXT,
                score REAL DEFAULT 0,
                connection_attempts INTEGER DEFAULT 0,
                connection_successes INTEGER DEFAULT 0,
                last_active INTEGER,
                l2_shard INTEGER DEFAULT 0,
                PRIMARY KEY (infohash, ip, port)
            );
            CREATE INDEX IF NOT EXISTS idx_peers_infohash ON peers(infohash);

            -- 冷数据归档表（超过 2 小时无活跃的 peer 迁移到此，减少主表体积）
            CREATE TABLE IF NOT EXISTS peers_archive (
                infohash BLOB NOT NULL,
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                source TEXT,
                score REAL DEFAULT 0,
                connection_attempts INTEGER DEFAULT 0,
                connection_successes INTEGER DEFAULT 0,
                last_active INTEGER,
                archived_at INTEGER,
                PRIMARY KEY (infohash, ip, port)
            );
            CREATE INDEX IF NOT EXISTS idx_peers_archive_infohash ON peers_archive(infohash);
            CREATE INDEX IF NOT EXISTS idx_peers_archive_last_active ON peers_archive(last_active);

            CREATE TABLE IF NOT EXISTS peer_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                infohash BLOB NOT NULL,
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                source TEXT,
                score REAL DEFAULT 0,
                discovered_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_peer_history_infohash ON peer_history(infohash);
            CREATE INDEX IF NOT EXISTS idx_peer_history_time ON peer_history(discovered_at);

            CREATE TABLE IF NOT EXISTS stats_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                metric TEXT NOT NULL,
                value REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_stats_history_metric ON stats_history(metric, timestamp);

            CREATE TABLE IF NOT EXISTS stats_aggregate (
                metric TEXT PRIMARY KEY,
                value REAL NOT NULL,
                updated_at INTEGER NOT NULL
            );
            "#,
        )?;

        // 向后兼容迁移：为已存在的 dht_nodes 表添加新列
        let _ = conn.execute(
            "ALTER TABLE dht_nodes ADD COLUMN nodes_returned INTEGER DEFAULT 0",
            [],
        );
        let _ = conn.execute(
            "ALTER TABLE dht_nodes ADD COLUMN last_query_time INTEGER",
            [],
        );
        // 向后兼容迁移：为已存在的 infohashes 表添加 score 列
        let _ = conn.execute("ALTER TABLE infohashes ADD COLUMN score REAL DEFAULT 0", []);
        // 向后兼容迁移：为各表添加 l2_shard 分片列（分层 Merkle 同步用）
        // 旧表此前没有该列，必须先 ALTER 再建索引；新表已在上方 CREATE TABLE 中包含此列，
        // 重复 ALTER 会报 duplicate column，用 let _ = 吞掉。
        let _ = conn.execute(
            "ALTER TABLE dht_nodes ADD COLUMN l2_shard INTEGER DEFAULT 0",
            [],
        );
        let _ = conn.execute(
            "ALTER TABLE trackers ADD COLUMN l2_shard INTEGER DEFAULT 0",
            [],
        );
        let _ = conn.execute(
            "ALTER TABLE infohashes ADD COLUMN l2_shard INTEGER DEFAULT 0",
            [],
        );
        let _ = conn.execute(
            "ALTER TABLE peers ADD COLUMN l2_shard INTEGER DEFAULT 0",
            [],
        );

        // ===== P1-1：删除闭环 + 版本向量基础列 =====
        // version   —— 该行最后写入的逻辑版本（LWW 比较用；0 = 未知/旧数据）
        // origin_node—— 该行最初来源节点 node_id（20B，诊断/溯源用，可空）
        // updated_at —— 该行最后写入的 unix 秒（可空）
        // deleted_at —— 软删除墓碑：非 NULL 表示已删除，查询一律过滤（删除闭环的关键，
        //              否则「侧删了」在集合语义下与「侧从来没有」无法区分，Merkle 永远收敛不到 0）
        // 全部 ADD COLUMN 用 let _ 吞掉重复列错误，兼容新旧库。
        for tbl in ["dht_nodes", "trackers", "infohashes", "peers"] {
            let _ = conn.execute(
                &format!("ALTER TABLE {} ADD COLUMN version INTEGER DEFAULT 0", tbl),
                [],
            );
            let _ = conn.execute(
                &format!("ALTER TABLE {} ADD COLUMN origin_node BLOB", tbl),
                [],
            );
            let _ = conn.execute(
                &format!(
                    "ALTER TABLE {} ADD COLUMN updated_at INTEGER DEFAULT 0",
                    tbl
                ),
                [],
            );
            let _ = conn.execute(
                &format!("ALTER TABLE {} ADD COLUMN deleted_at INTEGER", tbl),
                [],
            );
        }

        // l2_shard 索引必须在 ALTER TABLE 之后创建（旧表此时才具备该列）。
        // 严禁放回上方 CREATE TABLE 的 execute_batch 块中：旧库 CREATE TABLE IF NOT EXISTS
        // 是 no-op，紧接着的 CREATE INDEX 会因列尚不存在而报 no such column: l2_shard 导致启动失败。
        conn.execute_batch(
            r#"
            CREATE INDEX IF NOT EXISTS idx_dht_nodes_l2_shard ON dht_nodes(l2_shard);
            CREATE INDEX IF NOT EXISTS idx_trackers_l2_shard ON trackers(l2_shard);
            CREATE INDEX IF NOT EXISTS idx_infohashes_l2_shard ON infohashes(l2_shard);
            CREATE INDEX IF NOT EXISTS idx_peers_l2_shard ON peers(l2_shard);
            CREATE INDEX IF NOT EXISTS idx_dht_nodes_deleted ON dht_nodes(deleted_at);
            CREATE INDEX IF NOT EXISTS idx_trackers_deleted ON trackers(deleted_at);
            CREATE INDEX IF NOT EXISTS idx_infohashes_deleted ON infohashes(deleted_at);
            CREATE INDEX IF NOT EXISTS idx_peers_deleted ON peers(deleted_at);
            "#,
        )?;

        // P1-2：变更日志表（联邦 delta 同步的权威来源）
        crate::storage::oplog::init_oplog_table(&conn)?;

        // P1-3 / P2-1：联邦层拥有的两张表也在此统一建（幂等）。
        // 放在 init_tables 是为了让 `Storage::memory()`（单元测试）与 `Storage::open`（生产）
        // 都能拿到完整 schema，避免「运行期才发现 no such table」的隐患。
        crate::federation::sync::delta::init_delta_tables(&conn)?;
        crate::federation::sync::bootstrap::init_bootstrap_table(&conn)?;

        debug!("[storage] 表结构初始化完成");
        Ok(())
    }

    // ---- DHT 节点 ----

    /// 保存 DHT 节点（upsert）
    #[allow(clippy::too_many_arguments)]
    pub fn save_dht_node(
        &self,
        id: &[u8; 20],
        ip: &str,
        port: u16,
        score: f64,
        state: &str,
        query_count: u64,
        success_count: u64,
        total_latency_ms: u64,
        consecutive_failures: u32,
        nodes_returned: u64,
        last_query_time: Option<i64>,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp();
        // P1-7：写入即填 l2_shard（key = "ip:port"），不再依赖周期全表回填
        let l2 = compute_l2_shard(format!("{}:{}", ip, port).as_bytes()) as i64;
        conn.execute(
            r#"INSERT INTO dht_nodes (id, ip, port, score, state, query_count, success_count,
                total_latency_ms, consecutive_failures, nodes_returned, last_query_time,
                last_active, first_seen, l2_shard, updated_at, deleted_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12, ?13, ?14, NULL)
               ON CONFLICT(ip, port) DO UPDATE SET
                id=excluded.id, score=excluded.score, state=excluded.state,
                query_count=excluded.query_count, success_count=excluded.success_count,
                total_latency_ms=excluded.total_latency_ms,
                consecutive_failures=excluded.consecutive_failures,
                nodes_returned=excluded.nodes_returned,
                last_query_time=excluded.last_query_time,
                last_active=excluded.last_active,
                l2_shard=excluded.l2_shard, updated_at=excluded.updated_at, deleted_at=NULL"#,
            params![
                id.as_slice(),
                ip,
                port as i64,
                score,
                state,
                query_count as i64,
                success_count as i64,
                total_latency_ms as i64,
                consecutive_failures as i64,
                nodes_returned as i64,
                last_query_time,
                now,
                l2,
                now
            ],
        )?;
        Ok(())
    }

    /// 加载所有 DHT 节点
    pub fn load_dht_nodes(&self) -> anyhow::Result<Vec<DhtNodeRow>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare("SELECT id, ip, port, score, state, query_count, success_count, total_latency_ms, consecutive_failures, nodes_returned, last_query_time FROM dht_nodes WHERE deleted_at IS NULL")?;
        let rows = stmt.query_map([], |row| {
            let id: Vec<u8> = row.get(0)?;
            let mut id_arr = [0u8; 20];
            if id.len() == 20 {
                id_arr.copy_from_slice(&id);
            }
            Ok(DhtNodeRow {
                id: id_arr,
                ip: row.get(1)?,
                port: row.get::<_, i64>(2)? as u16,
                score: row.get(3)?,
                state: row.get(4)?,
                query_count: row.get::<_, i64>(5)? as u64,
                success_count: row.get::<_, i64>(6)? as u64,
                total_latency_ms: row.get::<_, i64>(7)? as u64,
                consecutive_failures: row.get::<_, i64>(8)? as u32,
                nodes_returned: row.get::<_, i64>(9).unwrap_or(0) as u64,
                last_query_time: row.get::<_, Option<i64>>(10).unwrap_or(None),
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// 清空 DHT 节点表
    pub fn clear_dht_nodes(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute("DELETE FROM dht_nodes", [])?;
        Ok(())
    }

    /// P1-6：软删除单个 DHT 节点（写 deleted_at 墓碑，不物理删除）。
    /// 返回受影响行数（0 表示行不存在或已删除）。upsert 会自动清除墓碑以支持复活。
    pub fn soft_delete_node(&self, ip: &str, port: u16) -> anyhow::Result<usize> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp();
        let n = conn.execute(
            "UPDATE dht_nodes SET deleted_at = ?1, updated_at = ?1 \
             WHERE ip = ?2 AND port = ?3 AND deleted_at IS NULL",
            params![now, ip, port as i64],
        )?;
        Ok(n)
    }

    /// P1-6：批量软删除 DHT 节点（一次事务）。
    pub fn soft_delete_nodes_batch(&self, addrs: &[(String, u16)]) -> anyhow::Result<usize> {
        if addrs.is_empty() {
            return Ok(0);
        }
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp();
        let tx = conn.unchecked_transaction()?;
        let mut total = 0usize;
        {
            let mut stmt = tx.prepare(
                "UPDATE dht_nodes SET deleted_at = ?1, updated_at = ?1 \
                 WHERE ip = ?2 AND port = ?3 AND deleted_at IS NULL",
            )?;
            for (ip, port) in addrs {
                total += stmt.execute(params![now, ip, *port as i64])?;
            }
        }
        tx.commit()?;
        Ok(total)
    }

    /// 批量保存 DHT 节点（事务批量插入，一次获取锁完成所有操作）
    pub fn save_dht_nodes_batch(&self, nodes: &[DhtNodeRow]) -> anyhow::Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        self.record_write("dht_nodes", nodes.len() as u64);
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        Self::save_dht_nodes_batch_conn(&conn, nodes)
    }

    /// 使用给定连接批量保存 DHT 节点（供 WriteQueue 闭包调用，避免重复加锁）
    pub fn save_dht_nodes_batch_conn(
        conn: &Connection,
        nodes: &[DhtNodeRow],
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare(
                r#"INSERT INTO dht_nodes (id, ip, port, score, state, query_count, success_count,
                    total_latency_ms, consecutive_failures, nodes_returned, last_query_time,
                    last_active, first_seen, l2_shard, updated_at, deleted_at)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12, ?13, ?14, NULL)
                   ON CONFLICT(ip, port) DO UPDATE SET
                    id=excluded.id, score=excluded.score, state=excluded.state,
                    query_count=excluded.query_count, success_count=excluded.success_count,
                    total_latency_ms=excluded.total_latency_ms,
                    consecutive_failures=excluded.consecutive_failures,
                    nodes_returned=excluded.nodes_returned,
                    last_query_time=excluded.last_query_time,
                    last_active=excluded.last_active,
                    l2_shard=excluded.l2_shard, updated_at=excluded.updated_at, deleted_at=NULL"#,
            )?;
            for node in nodes {
                let l2 = compute_l2_shard(format!("{}:{}", node.ip, node.port).as_bytes()) as i64;
                stmt.execute(params![
                    node.id.as_slice(),
                    node.ip.as_str(),
                    node.port as i64,
                    node.score,
                    node.state.as_str(),
                    node.query_count as i64,
                    node.success_count as i64,
                    node.total_latency_ms as i64,
                    node.consecutive_failures as i64,
                    node.nodes_returned as i64,
                    node.last_query_time,
                    now,
                    l2,
                    now
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// 批量保存 DHT 节点（调用方已开启事务，不重复开启）
    /// 供 WriteQueue 在批量事务中调用，避免事务嵌套。
    pub fn save_dht_nodes_batch_in_tx(
        conn: &Connection,
        nodes: &[DhtNodeRow],
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp();
        let mut stmt = conn.prepare(
            r#"INSERT INTO dht_nodes (id, ip, port, score, state, query_count, success_count,
                total_latency_ms, consecutive_failures, nodes_returned, last_query_time,
                last_active, first_seen, l2_shard, updated_at, deleted_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12, ?13, ?14, NULL)
               ON CONFLICT(ip, port) DO UPDATE SET
                id=excluded.id, score=excluded.score, state=excluded.state,
                query_count=excluded.query_count, success_count=excluded.success_count,
                total_latency_ms=excluded.total_latency_ms,
                consecutive_failures=excluded.consecutive_failures,
                nodes_returned=excluded.nodes_returned,
                last_query_time=excluded.last_query_time,
                last_active=excluded.last_active,
                l2_shard=excluded.l2_shard, updated_at=excluded.updated_at, deleted_at=NULL"#,
        )?;
        for node in nodes {
            let l2 = compute_l2_shard(format!("{}:{}", node.ip, node.port).as_bytes()) as i64;
            stmt.execute(params![
                node.id.as_slice(),
                node.ip.as_str(),
                node.port as i64,
                node.score,
                node.state.as_str(),
                node.query_count as i64,
                node.success_count as i64,
                node.total_latency_ms as i64,
                node.consecutive_failures as i64,
                node.nodes_returned as i64,
                node.last_query_time,
                now,
                l2,
                now
            ])?;
        }
        Ok(())
    }

    // ---- Tracker ----

    /// 保存 tracker（upsert）
    #[allow(clippy::too_many_arguments)]
    pub fn save_tracker(
        &self,
        url: &str,
        score: f64,
        total_requests: u64,
        success_requests: u64,
        failed_requests: u64,
        total_peers_discovered: u64,
        total_response_time_ms: f64,
        consecutive_failures: u32,
        disabled: bool,
    ) -> anyhow::Result<()> {
        self.record_write("trackers", 1);
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp();
        // P1-7：写入即填 l2_shard（key = url）
        let l2 = compute_l2_shard(url.as_bytes()) as i64;
        conn.execute(
            r#"INSERT INTO trackers (url, score, total_requests, success_requests, failed_requests,
                total_peers_discovered, total_response_time_ms, consecutive_failures, disabled, last_used,
                l2_shard, updated_at, deleted_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, NULL)
               ON CONFLICT(url) DO UPDATE SET
                score=excluded.score, total_requests=excluded.total_requests,
                success_requests=excluded.success_requests,
                failed_requests=excluded.failed_requests,
                total_peers_discovered=excluded.total_peers_discovered,
                total_response_time_ms=excluded.total_response_time_ms,
                consecutive_failures=excluded.consecutive_failures,
                disabled=excluded.disabled, last_used=excluded.last_used,
                l2_shard=excluded.l2_shard, updated_at=excluded.updated_at, deleted_at=NULL"#,
            params![
                url, score, total_requests as i64, success_requests as i64,
                failed_requests as i64, total_peers_discovered as i64,
                total_response_time_ms, consecutive_failures as i64,
                disabled as i64, now, l2, now
            ],
        )?;
        Ok(())
    }

    /// P1-6：软删除指定 tracker（写 `deleted_at` 墓碑，不物理删除）。用于 `remove_tracker`。
    /// 物理删除会让「本地删了」与「本地从来没有」在集合语义下无法区分，重启后
    /// `load_trackers` 回源又会把它"复活"；改成墓碑 + 查询过滤后两端 Merkle 才能收敛到 0。
    pub fn soft_delete_tracker(&self, url: &str) -> anyhow::Result<usize> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp();
        let n = conn.execute(
            "UPDATE trackers SET deleted_at = ?1, updated_at = ?1 \
             WHERE url = ?2 AND deleted_at IS NULL",
            params![now, url],
        )?;
        Ok(n)
    }

    /// 在已有连接上批量保存 trackers（供 WriteQueue/IOScheduler 闭包使用，调用方已开启事务）
    pub fn save_trackers_batch_in_tx(
        conn: &Connection,
        trackers: &[TrackerRow],
    ) -> anyhow::Result<()> {
        if trackers.is_empty() {
            return Ok(());
        }
        let now = chrono::Utc::now().timestamp();
        let mut stmt = conn.prepare(
            r#"INSERT INTO trackers (url, score, total_requests, success_requests, failed_requests,
                total_peers_discovered, total_response_time_ms, consecutive_failures, disabled, last_used,
                l2_shard, updated_at, deleted_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, NULL)
               ON CONFLICT(url) DO UPDATE SET
                score=excluded.score, total_requests=excluded.total_requests,
                success_requests=excluded.success_requests,
                failed_requests=excluded.failed_requests,
                total_peers_discovered=excluded.total_peers_discovered,
                total_response_time_ms=excluded.total_response_time_ms,
                consecutive_failures=excluded.consecutive_failures,
                disabled=excluded.disabled, last_used=excluded.last_used,
                l2_shard=excluded.l2_shard, updated_at=excluded.updated_at, deleted_at=NULL"#,
        )?;
        for t in trackers {
            let l2 = compute_l2_shard(t.url.as_bytes()) as i64;
            stmt.execute(params![
                t.url.as_str(),
                t.score,
                t.total_requests as i64,
                t.success_requests as i64,
                t.failed_requests as i64,
                t.total_peers_discovered as i64,
                t.total_response_time_ms,
                t.consecutive_failures as i64,
                t.disabled as i64,
                now,
                l2,
                now,
            ])?;
        }
        Ok(())
    }

    pub fn load_trackers(&self) -> anyhow::Result<Vec<TrackerRow>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare("SELECT url, score, total_requests, success_requests, failed_requests, total_peers_discovered, total_response_time_ms, consecutive_failures, disabled FROM trackers WHERE deleted_at IS NULL")?;
        let rows = stmt.query_map([], |row| {
            Ok(TrackerRow {
                url: row.get(0)?,
                score: row.get(1)?,
                total_requests: row.get::<_, i64>(2)? as u64,
                success_requests: row.get::<_, i64>(3)? as u64,
                failed_requests: row.get::<_, i64>(4)? as u64,
                total_peers_discovered: row.get::<_, i64>(5)? as u64,
                total_response_time_ms: row.get(6)?,
                consecutive_failures: row.get::<_, i64>(7)? as u32,
                disabled: row.get::<_, i64>(8)? != 0,
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    // ---- Infohash ----

    /// 保存 infohash（upsert）
    pub fn save_infohash(
        &self,
        infohash: &[u8; 20],
        ref_count: u32,
        first_source: &str,
        score: f64,
    ) -> anyhow::Result<()> {
        self.record_write("infohashes", 1);
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        Self::save_infohash_in_tx(&conn, infohash, ref_count, first_source, score)
    }

    /// 在已有连接（事务）上写入单个 infohash（供 WriteQueue/IOScheduler 闭包使用）
    pub fn save_infohash_in_tx(
        conn: &Connection,
        infohash: &[u8; 20],
        ref_count: u32,
        first_source: &str,
        score: f64,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp();
        // P1-7：写入即填 l2_shard（key = infohash 原始字节）
        let l2 = compute_l2_shard(infohash) as i64;
        conn.execute(
            r#"INSERT INTO infohashes (infohash, ref_count, first_source, first_seen, last_seen, score,
                l2_shard, updated_at, deleted_at)
               VALUES (?1, ?2, ?3, ?4, ?4, ?5, ?6, ?4, NULL)
               ON CONFLICT(infohash) DO UPDATE SET
                ref_count=excluded.ref_count, last_seen=excluded.last_seen, score=excluded.score,
                l2_shard=excluded.l2_shard, updated_at=excluded.updated_at, deleted_at=NULL"#,
            params![infohash.as_slice(), ref_count as i64, first_source, now, score, l2],
        )?;
        Ok(())
    }

    /// 在已有连接上批量保存 infohash（供 WriteQueue/IOScheduler 闭包使用，调用方已开启事务）
    pub fn save_infohashes_batch_in_tx(
        conn: &Connection,
        entries: &[InfohashRow],
    ) -> anyhow::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let now = chrono::Utc::now().timestamp();
        let mut stmt = conn.prepare(
            r#"INSERT INTO infohashes (infohash, ref_count, first_source, first_seen, last_seen, score,
                l2_shard, updated_at, deleted_at)
               VALUES (?1, ?2, ?3, ?4, ?4, ?5, ?6, ?4, NULL)
               ON CONFLICT(infohash) DO UPDATE SET
                ref_count=excluded.ref_count, last_seen=excluded.last_seen, score=excluded.score,
                l2_shard=excluded.l2_shard, updated_at=excluded.updated_at, deleted_at=NULL"#,
        )?;
        for entry in entries {
            let l2 = compute_l2_shard(entry.infohash.as_slice()) as i64;
            stmt.execute(params![
                entry.infohash.as_slice(),
                entry.ref_count as i64,
                entry.first_source.as_str(),
                now,
                entry.score,
                l2,
            ])?;
        }
        Ok(())
    }

    /// 批量保存 infohash（在已有连接上执行，供 IOScheduler 回调）
    /// 加载所有 infohash
    pub fn load_infohashes(&self) -> anyhow::Result<Vec<InfohashRow>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt =
            conn.prepare("SELECT infohash, ref_count, first_source, score FROM infohashes WHERE deleted_at IS NULL")?;
        let rows = stmt.query_map([], |row| {
            let ih: Vec<u8> = row.get(0)?;
            let mut arr = [0u8; 20];
            if ih.len() == 20 {
                arr.copy_from_slice(&ih);
            }
            Ok(InfohashRow {
                infohash: arr,
                ref_count: row.get::<_, i64>(1)? as u32,
                first_source: row.get(2)?,
                score: row.get::<_, f64>(3).unwrap_or(0.0),
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// 更新 infohash 评分
    pub fn update_infohash_score(&self, infohash: &[u8; 20], score: f64) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "UPDATE infohashes SET score = ?1 WHERE infohash = ?2",
            params![score, infohash.as_slice()],
        )?;
        Ok(())
    }

    /// 在已有连接上更新单个 infohash 评分（供 WriteQueue/IOScheduler 闭包使用，调用方已开启事务）
    pub fn update_infohash_score_in_tx(
        conn: &Connection,
        infohash: &[u8; 20],
        score: f64,
    ) -> anyhow::Result<()> {
        conn.execute(
            "UPDATE infohashes SET score = ?1 WHERE infohash = ?2",
            params![score, infohash.as_slice()],
        )?;
        Ok(())
    }

    /// 批量更新 infohash 评分（一次事务）
    pub fn update_infohash_scores_batch(&self, scores: &[([u8; 20], f64)]) -> anyhow::Result<()> {
        if scores.is_empty() {
            return Ok(());
        }
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare("UPDATE infohashes SET score = ?1 WHERE infohash = ?2")?;
            for (infohash, score) in scores {
                stmt.execute(params![score, infohash.as_slice()])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// 在已有连接上批量更新 infohash 评分（供 WriteQueue/IOScheduler 闭包使用，调用方已开启事务）
    pub fn update_infohash_scores_batch_in_tx(
        conn: &Connection,
        scores: &[([u8; 20], f64)],
    ) -> anyhow::Result<()> {
        if scores.is_empty() {
            return Ok(());
        }
        let mut stmt = conn.prepare("UPDATE infohashes SET score = ?1 WHERE infohash = ?2")?;
        for (infohash, score) in scores {
            stmt.execute(params![score, infohash.as_slice()])?;
        }
        Ok(())
    }

    /// 清空 infohash 表
    pub fn clear_infohashes(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute("DELETE FROM infohashes", [])?;
        Ok(())
    }

    // ---- Peers（运行时活跃 peer 全量持久化）----

    /// 保存 peer（upsert）
    #[allow(clippy::too_many_arguments)]
    pub fn save_peer(
        &self,
        infohash: &[u8; 20],
        ip: &str,
        port: u16,
        source: &str,
        score: f64,
        connection_attempts: u32,
        connection_successes: u32,
        last_active: i64,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp();
        // P1-7：写入即填 l2_shard（key = "<hex_ih>:<ip>:<port>"）
        let l2 = peer_shard_index(infohash, ip, port);
        conn.execute(
            r#"INSERT INTO peers (infohash, ip, port, source, score, connection_attempts, connection_successes, last_active,
                l2_shard, updated_at, deleted_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL)
               ON CONFLICT(infohash, ip, port) DO UPDATE SET
                source=excluded.source, score=excluded.score,
                connection_attempts=excluded.connection_attempts,
                connection_successes=excluded.connection_successes,
                last_active=excluded.last_active,
                l2_shard=excluded.l2_shard, updated_at=excluded.updated_at, deleted_at=NULL"#,
            params![
                infohash.as_slice(), ip, port as i64, source,
                score, connection_attempts as i64,
                connection_successes as i64, last_active,
                l2, now,
            ],
        )?;
        Ok(())
    }

    /// 加载所有 peer
    pub fn load_peers(&self) -> anyhow::Result<Vec<PeerRow>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare("SELECT infohash, ip, port, source, score, connection_attempts, connection_successes, last_active FROM peers WHERE deleted_at IS NULL")?;
        let rows = stmt.query_map([], |row| {
            let ih: Vec<u8> = row.get(0)?;
            let mut arr = [0u8; 20];
            if ih.len() == 20 {
                arr.copy_from_slice(&ih);
            }
            Ok(PeerRow {
                infohash: arr,
                ip: row.get(1)?,
                port: row.get::<_, i64>(2)? as u16,
                source: row.get(3)?,
                score: row.get(4)?,
                connection_attempts: row.get::<_, i64>(5)? as u32,
                connection_successes: row.get::<_, i64>(6)? as u32,
                last_active: row.get(7)?,
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// 清空 peers 表
    pub fn clear_peers(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute("DELETE FROM peers", [])?;
        Ok(())
    }

    /// 批量保存 peers（事务批量插入）
    pub fn save_peers_batch(&self, peers: &[PeerRow]) -> anyhow::Result<()> {
        if peers.is_empty() {
            return Ok(());
        }
        self.record_write("peers", peers.len() as u64);
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare(
                r#"INSERT INTO peers (infohash, ip, port, source, score, connection_attempts, connection_successes, last_active,
                    l2_shard, updated_at, deleted_at)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL)
                   ON CONFLICT(infohash, ip, port) DO UPDATE SET
                    source=excluded.source, score=excluded.score,
                    connection_attempts=excluded.connection_attempts,
                    connection_successes=excluded.connection_successes,
                    last_active=excluded.last_active,
                    l2_shard=excluded.l2_shard, updated_at=excluded.updated_at, deleted_at=NULL"#,
            )?;
            for peer in peers {
                let l2 = peer_shard_index(&peer.infohash, peer.ip.as_str(), peer.port);
                stmt.execute(params![
                    peer.infohash.as_slice(),
                    peer.ip.as_str(),
                    peer.port as i64,
                    peer.source.as_str(),
                    peer.score,
                    peer.connection_attempts as i64,
                    peer.connection_successes as i64,
                    peer.last_active,
                    l2,
                    now,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// 在已有连接上批量保存 peers（供 WriteQueue/IOScheduler 闭包使用，调用方已开启事务）
    pub fn save_peers_batch_in_tx(conn: &Connection, peers: &[PeerRow]) -> anyhow::Result<()> {
        if peers.is_empty() {
            return Ok(());
        }
        let now = chrono::Utc::now().timestamp();
        let mut stmt = conn.prepare(
            r#"INSERT INTO peers (infohash, ip, port, source, score, connection_attempts, connection_successes, last_active,
                l2_shard, updated_at, deleted_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL)
               ON CONFLICT(infohash, ip, port) DO UPDATE SET
                source=excluded.source, score=excluded.score,
                connection_attempts=excluded.connection_attempts,
                connection_successes=excluded.connection_successes,
                last_active=excluded.last_active,
                l2_shard=excluded.l2_shard, updated_at=excluded.updated_at, deleted_at=NULL"#,
        )?;
        for peer in peers {
            let l2 = peer_shard_index(&peer.infohash, peer.ip.as_str(), peer.port);
            stmt.execute(params![
                peer.infohash.as_slice(),
                peer.ip.as_str(),
                peer.port as i64,
                peer.source.as_str(),
                peer.score,
                peer.connection_attempts as i64,
                peer.connection_successes as i64,
                peer.last_active,
                l2,
                now,
            ])?;
        }
        Ok(())
    }

    /// 归档冷数据：将超过指定时间无活跃的 peer 从主表迁移到归档表
    /// 返回归档的 peer 数量
    pub fn archive_cold_peers(&self, older_than_secs: i64) -> anyhow::Result<usize> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp();
        let threshold = now - older_than_secs;

        // 先查询需要归档的数量
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM peers WHERE last_active < ?1",
            params![threshold],
            |row| row.get(0),
        )?;

        if count == 0 {
            return Ok(0);
        }

        let tx = conn.unchecked_transaction()?;
        // 插入到归档表
        tx.execute(
            "INSERT OR IGNORE INTO peers_archive (infohash, ip, port, source, score, connection_attempts, connection_successes, last_active, archived_at)
             SELECT infohash, ip, port, source, score, connection_attempts, connection_successes, last_active, ?1
             FROM peers WHERE last_active < ?2",
            params![now, threshold],
        )?;
        // 从主表删除
        tx.execute(
            "DELETE FROM peers WHERE last_active < ?1",
            params![threshold],
        )?;
        tx.commit()?;

        Ok(count as usize)
    }

    /// 在已有连接上归档冷 peer（供 WriteQueue/IOScheduler 闭包使用，调用方已开启事务）
    /// `older_than_secs` 为时间阈值（秒），last_active 早于 now - older_than_secs 的 peer 迁移到归档表
    pub fn archive_cold_peers_in_tx(conn: &Connection, older_than_secs: i64) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp();
        let threshold = now - older_than_secs;
        // 插入到归档表
        conn.execute(
            "INSERT OR IGNORE INTO peers_archive (infohash, ip, port, source, score, connection_attempts, connection_successes, last_active, archived_at)
             SELECT infohash, ip, port, source, score, connection_attempts, connection_successes, last_active, ?1
             FROM peers WHERE last_active < ?2",
            params![now, threshold],
        )?;
        // 从主表删除
        conn.execute(
            "DELETE FROM peers WHERE last_active < ?1",
            params![threshold],
        )?;
        Ok(())
    }

    // ---- Peer History ----

    /// 记录 peer 发现历史
    pub fn record_peer_history(
        &self,
        infohash: &[u8; 20],
        ip: &str,
        port: u16,
        source: &str,
        score: f64,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO peer_history (infohash, ip, port, source, score, discovered_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![infohash.as_slice(), ip, port as i64, source, score, now],
        )?;
        Ok(())
    }

    /// 批量写入 peer 历史（攒批写入，减少 fsync 次数）
    pub fn save_peer_history_batch(&self, entries: &[PeerHistoryEntry]) -> anyhow::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.record_write("peer_history", entries.len() as u64);
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        // 单独调用时自行开启事务（批量原子性）；WriteQueue/IOScheduler 路径使用 _in_tx 变体
        let tx = conn.unchecked_transaction()?;
        Self::save_peer_history_batch_in_tx(&tx, entries)?;
        tx.commit()?;
        Ok(())
    }

    /// 在已有连接/事务上批量写入 peer_history（供 WriteQueue/IOScheduler 闭包使用）
    ///
    /// 注意：调用方需自行保证外层事务已开启；本函数不再开启事务。
    pub fn save_peer_history_batch_in_tx(
        conn: &Connection,
        entries: &[PeerHistoryEntry],
    ) -> anyhow::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut stmt = conn.prepare(
            "INSERT INTO peer_history (infohash, ip, port, source, score, discovered_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
        )?;
        for entry in entries {
            stmt.execute(params![
                entry.infohash.as_slice(),
                entry.ip.as_str(),
                entry.port as i64,
                entry.source.as_str(),
                entry.score,
                entry.discovered_at,
            ])?;
        }
        Ok(())
    }

    /// 查询某 infohash 的 peer 历史
    pub fn query_peer_history(
        &self,
        infohash: &[u8; 20],
        limit: usize,
    ) -> anyhow::Result<Vec<PeerHistoryRow>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare("SELECT ip, port, source, score, discovered_at FROM peer_history WHERE infohash = ?1 ORDER BY discovered_at DESC LIMIT ?2")?;
        let rows = stmt.query_map(params![infohash.as_slice(), limit as i64], |row| {
            Ok(PeerHistoryRow {
                ip: row.get(0)?,
                port: row.get::<_, i64>(1)? as u16,
                source: row.get(2)?,
                score: row.get(3)?,
                discovered_at: row.get(4)?,
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// 清理过期 peer 历史（保留 days 天）
    pub fn cleanup_peer_history(&self, days: u64) -> anyhow::Result<usize> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let cutoff = chrono::Utc::now().timestamp() - (days as i64 * 86400);
        let deleted = conn.execute(
            "DELETE FROM peer_history WHERE discovered_at < ?1",
            params![cutoff],
        )?;
        if deleted > 0 {
            debug!("[storage] 清理了 {} 条过期 peer 历史", deleted);
        }
        Ok(deleted)
    }

    // ---- Stats History ----

    /// 记录统计快照
    pub fn record_stats(&self, metric: &str, value: f64) -> anyhow::Result<()> {
        self.record_write("stats", 1);
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO stats_history (timestamp, metric, value) VALUES (?1, ?2, ?3)",
            params![now, metric, value],
        )?;
        Ok(())
    }

    /// 在已有连接上记录统计快照（供 WriteQueue/IOScheduler 闭包使用，调用方已开启事务）
    pub fn record_stats_in_tx(conn: &Connection, metric: &str, value: f64) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO stats_history (timestamp, metric, value) VALUES (?1, ?2, ?3)",
            params![now, metric, value],
        )?;
        Ok(())
    }

    /// 查询统计历史
    pub fn query_stats_history(&self, metric: &str, hours: u64) -> anyhow::Result<Vec<(i64, f64)>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let cutoff = chrono::Utc::now().timestamp() - (hours as i64 * 3600);
        let mut stmt = conn.prepare("SELECT timestamp, value FROM stats_history WHERE metric = ?1 AND timestamp >= ?2 ORDER BY timestamp")?;
        let rows = stmt.query_map(params![metric, cutoff], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    // ---- Stats Aggregate ----

    /// 更新累计统计
    pub fn update_aggregate(&self, metric: &str, value: f64) -> anyhow::Result<()> {
        self.record_write("stats", 1);
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO stats_aggregate (metric, value, updated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(metric) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            params![metric, value, now],
        )?;
        Ok(())
    }

    /// 在已有连接上更新累计统计（供 WriteQueue/IOScheduler 闭包使用，调用方已开启事务）
    pub fn update_aggregate_in_tx(
        conn: &Connection,
        metric: &str,
        value: f64,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO stats_aggregate (metric, value, updated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(metric) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            params![metric, value, now],
        )?;
        Ok(())
    }

    /// 加载累计统计
    pub fn load_aggregate(&self, metric: &str) -> Option<f64> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.query_row(
            "SELECT value FROM stats_aggregate WHERE metric = ?1",
            params![metric],
            |row| row.get(0),
        )
        .ok()
    }

    // ---- 分层 Merkle 同步：按 L2 精确分片加载行 ----

    /// 构建 `WHERE col IN (?1, ?2, ...)` 占位符 SQL 片段
    fn in_placeholders(n: usize) -> String {
        (1..=n)
            .map(|i| format!("?{}", i))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// 按 **L2 二级分片**加载 DHT 节点行（id, ip, port），走 l2_shard 索引。
    /// P1-5：分片列存真 L2（0..65535），传入 L2 值即可精确命中，避免整条 L1 加载的 256× 放大。
    pub fn load_node_rows_by_shards(
        &self,
        shards: &[u16],
    ) -> anyhow::Result<Vec<(Vec<u8>, String, u16)>> {
        if shards.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let sql = format!(
            "SELECT id, ip, port FROM dht_nodes WHERE l2_shard IN ({}) AND deleted_at IS NULL",
            Self::in_placeholders(shards.len())
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(shards.iter().map(|&s| s as i64)),
            |row| {
                let id: Vec<u8> = row.get(0)?;
                let ip: String = row.get(1)?;
                let port: i64 = row.get(2)?;
                Ok((id, ip, port as u16))
            },
        )?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// 按 **L2 二级分片**加载 Peer 行（infohash, ip, port, source, last_active），走 l2_shard 索引。
    pub fn load_peer_rows_by_shards(
        &self,
        shards: &[u16],
    ) -> anyhow::Result<Vec<(Vec<u8>, String, u16, String, i64)>> {
        if shards.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let sql = format!(
            "SELECT infohash, ip, port, source, last_active FROM peers WHERE l2_shard IN ({}) AND deleted_at IS NULL",
            Self::in_placeholders(shards.len())
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(shards.iter().map(|&s| s as i64)),
            |row| {
                let infohash: Vec<u8> = row.get(0)?;
                let ip: String = row.get(1)?;
                let port: i64 = row.get(2)?;
                let source: String = row.get(3)?;
                let last_active: i64 = row.get(4)?;
                Ok((infohash, ip, port as u16, source, last_active))
            },
        )?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// 按 **L2 二级分片**加载 Infohash 行（infohash, last_seen, first_source），走 l2_shard 索引。
    pub fn load_infohash_rows_by_shards(
        &self,
        shards: &[u16],
    ) -> anyhow::Result<Vec<(Vec<u8>, i64, String)>> {
        if shards.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let sql = format!(
            "SELECT infohash, last_seen, first_source FROM infohashes WHERE l2_shard IN ({}) AND deleted_at IS NULL",
            Self::in_placeholders(shards.len())
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(shards.iter().map(|&s| s as i64)),
            |row| {
                let infohash: Vec<u8> = row.get(0)?;
                let last_seen: i64 = row.get(1)?;
                let first_source: String = row.get::<_, Option<String>>(2)?.unwrap_or_default();
                Ok((infohash, last_seen, first_source))
            },
        )?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// 按 **L2 二级分片**加载 Tracker 行（url, disabled, last_used），走 l2_shard 索引。
    pub fn load_tracker_rows_by_shards(
        &self,
        shards: &[u16],
    ) -> anyhow::Result<Vec<(String, bool, Option<i64>)>> {
        if shards.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let sql = format!(
            "SELECT url, disabled, last_used FROM trackers WHERE l2_shard IN ({}) AND deleted_at IS NULL",
            Self::in_placeholders(shards.len())
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(shards.iter().map(|&s| s as i64)),
            |row| {
                let url: String = row.get(0)?;
                let disabled: i64 = row.get(1)?;
                let last_used: Option<i64> = row.get(2)?;
                Ok((url, disabled != 0, last_used))
            },
        )?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    // ---- 分层 Merkle 同步：全量 (key, data_hash) 冷重算 ----

    /// 加载全部节点的 (key, data_hash)，用于 Merkle 冷根重算。
    /// key = "ip:port"，data_hash = blake3(id || ip || port_le)，与 build_node_sync_entry 一致。
    pub fn load_all_node_keys_hashes(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt =
            conn.prepare("SELECT id, ip, port FROM dht_nodes WHERE deleted_at IS NULL")?;
        let rows = stmt.query_map([], |row| {
            let id: Vec<u8> = row.get(0)?;
            let ip: String = row.get(1)?;
            let port: i64 = row.get(2)?;
            Ok((id, ip, port))
        })?;
        let mut out = Vec::new();
        for r in rows {
            let (id, ip, port) = r?;
            let key = format!("{}:{}", ip, port).into_bytes();
            let mut buf = Vec::with_capacity(id.len() + ip.len() + 2);
            buf.extend_from_slice(&id);
            buf.extend_from_slice(ip.as_bytes());
            buf.extend_from_slice(&port.to_le_bytes());
            let data_hash = blake3::hash(&buf).as_bytes().to_vec();
            out.push((key, data_hash));
        }
        Ok(out)
    }

    /// 加载全部 Peer 的 (key, data_hash)，用于 Merkle 冷根重算。
    /// key = "<hex_infohash>:<ip>:<port>"，data_hash = blake3(infohash || ip || port_le)。
    pub fn load_all_peer_keys_hashes(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt =
            conn.prepare("SELECT infohash, ip, port FROM peers WHERE deleted_at IS NULL")?;
        let rows = stmt.query_map([], |row| {
            let ih: Vec<u8> = row.get(0)?;
            let ip: String = row.get(1)?;
            let port: i64 = row.get(2)?;
            Ok((ih, ip, port))
        })?;
        let mut out = Vec::new();
        for r in rows {
            let (ih, ip, port) = r?;
            let mut arr = [0u8; 20];
            if ih.len() == 20 {
                arr.copy_from_slice(&ih);
            }
            let ih_hex = arr.iter().map(|b| format!("{:02x}", b)).collect::<String>();
            let key = format!("{}:{}:{}", ih_hex, ip, port).into_bytes();
            let mut buf = Vec::with_capacity(ih.len() + ip.len() + 2);
            buf.extend_from_slice(&ih);
            buf.extend_from_slice(ip.as_bytes());
            buf.extend_from_slice(&port.to_le_bytes());
            let data_hash = blake3::hash(&buf).as_bytes().to_vec();
            out.push((key, data_hash));
        }
        Ok(out)
    }

    /// 加载全部 Infohash 的 (key, data_hash)，用于 Merkle 冷根重算。
    /// key = infohash 原始字节，data_hash = blake3(infohash)。
    pub fn load_all_infohash_keys_hashes(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare("SELECT infohash FROM infohashes WHERE deleted_at IS NULL")?;
        let rows = stmt.query_map([], |row| {
            let ih: Vec<u8> = row.get(0)?;
            Ok(ih)
        })?;
        let mut out = Vec::new();
        for r in rows {
            let ih = r?;
            let data_hash = blake3::hash(&ih).as_bytes().to_vec();
            out.push((ih, data_hash));
        }
        Ok(out)
    }

    /// 加载全部 Tracker 的 (key, data_hash)，用于 Merkle 冷根重算。
    /// key = url 字节，data_hash = blake3(url)。
    pub fn load_all_tracker_keys_hashes(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare("SELECT url FROM trackers WHERE deleted_at IS NULL")?;
        let rows = stmt.query_map([], |row| {
            let url: String = row.get(0)?;
            Ok(url)
        })?;
        let mut out = Vec::new();
        for r in rows {
            let url = r?;
            let data_hash = blake3::hash(url.as_bytes()).as_bytes().to_vec();
            out.push((url.into_bytes(), data_hash));
        }
        Ok(out)
    }

    // ---- 按需单行加载 / 统计 ----

    /// 加载热/温 DHT 节点（最近活跃或高分），按评分降序 + LIMIT，供分层缓存启动加载。
    pub fn load_hot_warm_nodes(
        &self,
        warm_threshold_secs: u64,
        min_score: f64,
        limit: usize,
    ) -> anyhow::Result<Vec<DhtNodeRow>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp();
        let warm_cutoff = now - warm_threshold_secs as i64;
        let mut stmt = conn.prepare(
            r#"SELECT id, ip, port, score, state, query_count, success_count,
                  total_latency_ms, consecutive_failures, nodes_returned, last_query_time
               FROM dht_nodes
               WHERE deleted_at IS NULL AND (last_active > ?1 OR score >= ?2)
               ORDER BY score DESC
               LIMIT ?3"#,
        )?;
        let rows = stmt.query_map(params![warm_cutoff, min_score, limit as i64], |row| {
            let id: Vec<u8> = row.get(0)?;
            let mut id_arr = [0u8; 20];
            if id.len() == 20 {
                id_arr.copy_from_slice(&id);
            }
            Ok(DhtNodeRow {
                id: id_arr,
                ip: row.get(1)?,
                port: row.get::<_, i64>(2)? as u16,
                score: row.get(3)?,
                state: row.get(4)?,
                query_count: row.get::<_, i64>(5)? as u64,
                success_count: row.get::<_, i64>(6)? as u64,
                total_latency_ms: row.get::<_, i64>(7)? as u64,
                consecutive_failures: row.get::<_, i64>(8)? as u32,
                nodes_returned: row.get::<_, i64>(9).unwrap_or(0) as u64,
                last_query_time: row.get::<_, Option<i64>>(10).unwrap_or(None),
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// 限量加载 peers（按最近活跃降序 + LIMIT），供启动预加载
    pub fn load_limited_peers(&self, limit: usize) -> anyhow::Result<Vec<PeerRow>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare(
            "SELECT infohash, ip, port, source, score, connection_attempts, connection_successes, last_active
             FROM peers WHERE deleted_at IS NULL
             ORDER BY last_active DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |row| {
            let ih: Vec<u8> = row.get(0)?;
            let mut arr = [0u8; 20];
            if ih.len() == 20 {
                arr.copy_from_slice(&ih);
            }
            Ok(PeerRow {
                infohash: arr,
                ip: row.get(1)?,
                port: row.get::<_, i64>(2)? as u16,
                source: row.get(3)?,
                score: row.get(4)?,
                connection_attempts: row.get::<_, i64>(5)? as u32,
                connection_successes: row.get::<_, i64>(6)? as u32,
                last_active: row.get(7)?,
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// 限量加载 infohashes（按引用数降序 + LIMIT），供启动预加载
    pub fn load_limited_infohashes(&self, limit: usize) -> anyhow::Result<Vec<InfohashRow>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare(
            "SELECT infohash, ref_count, first_source, score
             FROM infohashes WHERE deleted_at IS NULL
             ORDER BY ref_count DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |row| {
            let ih: Vec<u8> = row.get(0)?;
            let mut arr = [0u8; 20];
            if ih.len() == 20 {
                arr.copy_from_slice(&ih);
            }
            Ok(InfohashRow {
                infohash: arr,
                ref_count: row.get::<_, i64>(1)? as u32,
                first_source: row.get(2)?,
                score: row.get::<_, f64>(3).unwrap_or(0.0),
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// 按 (ip, port) 加载单个 DHT 节点（缓存未命中时按需加载）。
    pub fn load_dht_node_by_addr(&self, ip: &str, port: u16) -> anyhow::Result<Option<DhtNodeRow>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare(
            "SELECT id, ip, port, score, state, query_count, success_count, total_latency_ms, \
             consecutive_failures, nodes_returned, last_query_time FROM dht_nodes \
             WHERE ip = ?1 AND port = ?2 AND deleted_at IS NULL",
        )?;
        let result = stmt.query_row(params![ip, port as i64], |row| {
            let id: Vec<u8> = row.get(0)?;
            let mut id_arr = [0u8; 20];
            if id.len() == 20 {
                id_arr.copy_from_slice(&id);
            }
            Ok(DhtNodeRow {
                id: id_arr,
                ip: row.get(1)?,
                port: row.get::<_, i64>(2)? as u16,
                score: row.get(3)?,
                state: row.get(4)?,
                query_count: row.get::<_, i64>(5)? as u64,
                success_count: row.get::<_, i64>(6)? as u64,
                total_latency_ms: row.get::<_, i64>(7)? as u64,
                consecutive_failures: row.get::<_, i64>(8)? as u32,
                nodes_returned: row.get::<_, i64>(9).unwrap_or(0) as u64,
                last_query_time: row.get::<_, Option<i64>>(10).unwrap_or(None),
            })
        });
        match result {
            Ok(row) => Ok(Some(row)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// 按 (ip, port) 加载单个 Peer（缓存未命中时按需加载）。
    pub fn load_peer_by_addr(&self, ip: &str, port: u16) -> anyhow::Result<Option<PeerRow>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare(
            "SELECT infohash, ip, port, source, score, connection_attempts, connection_successes, \
             last_active FROM peers WHERE ip = ?1 AND port = ?2 AND deleted_at IS NULL",
        )?;
        let result = stmt.query_row(params![ip, port as i64], |row| {
            let ih: Vec<u8> = row.get(0)?;
            let mut arr = [0u8; 20];
            if ih.len() == 20 {
                arr.copy_from_slice(&ih);
            }
            Ok(PeerRow {
                infohash: arr,
                ip: row.get(1)?,
                port: row.get::<_, i64>(2)? as u16,
                source: row.get(3)?,
                score: row.get(4)?,
                connection_attempts: row.get::<_, i64>(5)? as u32,
                connection_successes: row.get::<_, i64>(6)? as u32,
                last_active: row.get(7)?,
            })
        });
        match result {
            Ok(row) => Ok(Some(row)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// 按 infohash 加载单个 Infohash 行（缓存未命中时按需加载）。
    pub fn load_infohash_by_hash(&self, ih: &[u8; 20]) -> anyhow::Result<Option<InfohashRow>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare(
            "SELECT infohash, ref_count, first_source, score FROM infohashes \
             WHERE infohash = ?1 AND deleted_at IS NULL",
        )?;
        let result = stmt.query_row(params![ih.as_slice()], |row| {
            let ih: Vec<u8> = row.get(0)?;
            let mut arr = [0u8; 20];
            if ih.len() == 20 {
                arr.copy_from_slice(&ih);
            }
            Ok(InfohashRow {
                infohash: arr,
                ref_count: row.get::<_, i64>(1)? as u32,
                first_source: row.get(2)?,
                score: row.get::<_, f64>(3).unwrap_or(0.0),
            })
        });
        match result {
            Ok(row) => Ok(Some(row)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// 按 url 加载单个 Tracker 行（缓存未命中时按需加载）。
    pub fn load_tracker_by_url(&self, url: &str) -> anyhow::Result<Option<TrackerRow>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare(
            "SELECT url, score, total_requests, success_requests, failed_requests, \
             total_peers_discovered, total_response_time_ms, consecutive_failures, disabled \
             FROM trackers WHERE url = ?1 AND deleted_at IS NULL",
        )?;
        let result = stmt.query_row(params![url], |row| {
            Ok(TrackerRow {
                url: row.get(0)?,
                score: row.get(1)?,
                total_requests: row.get::<_, i64>(2)? as u64,
                success_requests: row.get::<_, i64>(3)? as u64,
                failed_requests: row.get::<_, i64>(4)? as u64,
                total_peers_discovered: row.get::<_, i64>(5)? as u64,
                total_response_time_ms: row.get(6)?,
                consecutive_failures: row.get::<_, i64>(7)? as u32,
                disabled: row.get::<_, i64>(8)? != 0,
            })
        });
        match result {
            Ok(row) => Ok(Some(row)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// 统计表的总行数（表名为调用方硬编码常量，无注入风险）。
    pub fn count_table(&self, table: &str) -> anyhow::Result<u64> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let count: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM {}", table), [], |row| {
            row.get(0)
        })?;
        Ok(count as u64)
    }

    /// 按 L2 二级分片加载节点 (key, data_hash)，用于 Merkle 增量重算。
    pub fn load_node_keys_hashes_by_shards(
        &self,
        shards: &[u16],
    ) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.load_node_rows_by_shards(shards)?;
        let mut out = Vec::with_capacity(rows.len());
        for (id, ip, port) in rows {
            let key = format!("{}:{}", ip, port).into_bytes();
            let mut buf = Vec::with_capacity(id.len() + ip.len() + 2);
            buf.extend_from_slice(&id);
            buf.extend_from_slice(ip.as_bytes());
            buf.extend_from_slice(&port.to_le_bytes());
            let data_hash = blake3::hash(&buf).as_bytes().to_vec();
            out.push((key, data_hash));
        }
        Ok(out)
    }

    /// P2-1：按 key（"ip:port" 字符串）升序加载 NODE 原始行 `(id, ip, port)`，范围 `[lo, hi)`，最多 `limit` 条。
    ///
    /// 供 bootstrap 分块服务端组装完整 `SyncEntry`（需要 id/ip/port 构造 payload）使用；
    /// 排序/比较键与 [`Self::load_node_key_hashes_in_range`] 严格一致。
    pub fn load_node_rows_in_range(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Vec<u8>, String, i64)>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let lo_s = lo.map(|b| String::from_utf8_lossy(b).into_owned());
        let hi_s = hi.map(|b| String::from_utf8_lossy(b).into_owned());
        let mut stmt = conn.prepare(
            "SELECT id, ip, port FROM dht_nodes \
             WHERE deleted_at IS NULL \
               AND (?1 IS NULL OR (ip || ':' || port) >= ?1) \
               AND (?2 IS NULL OR (ip || ':' || port) < ?2) \
             ORDER BY (ip || ':' || port) ASC LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![lo_s, hi_s, limit.max(1) as i64], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// P1-4：按 key（"ip:port" 字符串）升序加载 NODE 的 (key, data_hash)，范围 `[lo, hi)`，最多 `limit` 条。
    ///
    /// - 用表达式 `(ip || ':' || port)` 作为排序/比较键，与 Merkle 的 key 编码严格一致，
    ///   保证「按 key 排序」与「区间比较」自洽（若用 ip/port 双列排序会产生偏差）。
    /// - `lo = None` 表示下界 -∞；`hi = None` 表示上界 +∞。
    pub fn load_node_key_hashes_in_range(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let lo_s = lo.map(|b| String::from_utf8_lossy(b).into_owned());
        let hi_s = hi.map(|b| String::from_utf8_lossy(b).into_owned());
        let mut stmt = conn.prepare(
            "SELECT id, ip, port FROM dht_nodes \
             WHERE deleted_at IS NULL \
               AND (?1 IS NULL OR (ip || ':' || port) >= ?1) \
               AND (?2 IS NULL OR (ip || ':' || port) < ?2) \
             ORDER BY (ip || ':' || port) ASC LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![lo_s, hi_s, limit.max(1) as i64], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        let mut out = Vec::new();
        for r in rows {
            let (id, ip, port) = r?;
            let key = format!("{}:{}", ip, port).into_bytes();
            let mut buf = Vec::with_capacity(id.len() + ip.len() + 2);
            buf.extend_from_slice(&id);
            buf.extend_from_slice(ip.as_bytes());
            buf.extend_from_slice(&port.to_le_bytes());
            out.push((key, blake3::hash(&buf).as_bytes().to_vec()));
        }
        Ok(out)
    }

    /// P1-4：均匀抽取 `n` 个 NODE key 作为区间分界（用 rowid 伪随机探针，避免全表扫描）。
    ///
    /// 返回**已排序去重**的 key 列表。`MAX(rowid)` 为 O(1)，每个探针为 O(log N) 的 rowid 查找，
    /// 整体 O(n log N)（相对 O(N) 全表扫描可忽略）。
    pub fn sample_node_range_keys(&self, n: usize) -> anyhow::Result<Vec<Vec<u8>>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let max_rowid: i64 =
            conn.query_row("SELECT COALESCE(MAX(rowid), 0) FROM dht_nodes", [], |r| {
                r.get(0)
            })?;
        if max_rowid <= 0 {
            return Ok(Vec::new());
        }
        // 手写 LCG（避免引入 rand 依赖；seed 取自当前时间，保证每轮抽样不同）
        let mut state: u64 = (chrono::Utc::now().timestamp_millis() as u64) ^ 0x9E37_79B9_7F4A_7C15;
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(n);
        let mut stmt = conn.prepare_cached(
            "SELECT ip, port FROM dht_nodes WHERE deleted_at IS NULL AND rowid >= ?1 \
             ORDER BY rowid ASC LIMIT 1",
        )?;
        for _ in 0..n {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let probe = ((state >> 33) % (max_rowid as u64)) as i64;
            if let Ok((ip, port)) = stmt.query_row(params![probe], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            }) {
                keys.push(format!("{}:{}", ip, port).into_bytes());
            }
        }
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    /// 按 L2 二级分片加载 Peer (key, data_hash)，用于 Merkle 增量重算。
    pub fn load_peer_keys_hashes_by_shards(
        &self,
        shards: &[u16],
    ) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.load_peer_rows_by_shards(shards)?;
        let mut out = Vec::with_capacity(rows.len());
        for (ih, ip, port, _source, _last_active) in rows {
            let mut arr = [0u8; 20];
            if ih.len() == 20 {
                arr.copy_from_slice(&ih);
            }
            let ih_hex = arr.iter().map(|b| format!("{:02x}", b)).collect::<String>();
            let key = format!("{}:{}:{}", ih_hex, ip, port).into_bytes();
            let mut buf = Vec::with_capacity(ih.len() + ip.len() + 2);
            buf.extend_from_slice(&ih);
            buf.extend_from_slice(ip.as_bytes());
            buf.extend_from_slice(&port.to_le_bytes());
            let data_hash = blake3::hash(&buf).as_bytes().to_vec();
            out.push((key, data_hash));
        }
        Ok(out)
    }

    /// 按 L2 二级分片加载 Infohash (key, data_hash)，用于 Merkle 增量重算。
    pub fn load_infohash_keys_hashes_by_shards(
        &self,
        shards: &[u16],
    ) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.load_infohash_rows_by_shards(shards)?;
        let mut out = Vec::with_capacity(rows.len());
        for (ih, _last_seen, _first_source) in rows {
            let data_hash = blake3::hash(&ih).as_bytes().to_vec();
            out.push((ih, data_hash));
        }
        Ok(out)
    }

    /// 按 L2 二级分片加载 Tracker (key, data_hash)，用于 Merkle 增量重算。
    pub fn load_tracker_keys_hashes_by_shards(
        &self,
        shards: &[u16],
    ) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.load_tracker_rows_by_shards(shards)?;
        let mut out = Vec::with_capacity(rows.len());
        for (url, _disabled, _last_used) in rows {
            let data_hash = blake3::hash(url.as_bytes()).as_bytes().to_vec();
            out.push((url.into_bytes(), data_hash));
        }
        Ok(out)
    }

    /// 一次性迁移/回填 `l2_shard` 列（P1-5）。
    ///
    /// 以 SQLite 内置 `PRAGMA user_version` 作为迁移标记：达到 [`SHARD_SCHEME_VERSION`] 即视为
    /// 已是「真 L2」方案，直接返回（O(1) 元数据检查，不扫表）。否则**全表重算**四表的 `l2_shard`
    /// 为真 L2 值并写回标记。
    ///
    /// 为什么必须全表重算而非只补 `l2_shard = 0`：旧库该列存的是 **L1**（0..255），与新方案（L2，
    /// 0..65535）语义不同，只补 0 值会让两套语义混存，导致按 L2 查询漏行/错行。故本次一次性全表
    /// 重算；此后所有写入路径都「写入即填」真 L2，不再需要任何回填。
    /// 启动时调用一次，失败不阻断启动（下次启动会因标记未写入而重试）。
    pub fn backfill_shards(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let uv: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap_or(0);
        if uv >= SHARD_SCHEME_VERSION {
            return Ok(());
        }

        let tx = conn.unchecked_transaction()?;
        // dht_nodes: key = "ip:port"
        {
            let mut sel = tx.prepare("SELECT id, ip, port FROM dht_nodes")?;
            let rows: Vec<(Vec<u8>, String, i64)> = sel
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                .filter_map(|r| r.ok())
                .collect();
            drop(sel);
            let mut upd =
                tx.prepare("UPDATE dht_nodes SET l2_shard = ?1 WHERE ip = ?2 AND port = ?3")?;
            for (_id, ip, port) in &rows {
                let key = format!("{}:{}", ip, port);
                let shard = compute_l2_shard(key.as_bytes()) as i64;
                upd.execute(params![shard, ip, port])?;
            }
        }
        // peers: key = "<hex_ih>:<ip>:<port>"
        {
            let mut sel = tx.prepare("SELECT infohash, ip, port FROM peers")?;
            let rows: Vec<(Vec<u8>, String, i64)> = sel
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                .filter_map(|r| r.ok())
                .collect();
            drop(sel);
            let mut upd = tx.prepare(
                "UPDATE peers SET l2_shard = ?1 WHERE infohash = ?2 AND ip = ?3 AND port = ?4",
            )?;
            for (ih, ip, port) in &rows {
                let mut arr = [0u8; 20];
                if ih.len() == 20 {
                    arr.copy_from_slice(ih);
                }
                let ih_hex = arr.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                let key = format!("{}:{}:{}", ih_hex, ip, port);
                let shard = compute_l2_shard(key.as_bytes()) as i64;
                upd.execute(params![shard, ih, ip, port])?;
            }
        }
        // infohashes: key = infohash 原始字节
        {
            let mut sel = tx.prepare("SELECT infohash FROM infohashes")?;
            let rows: Vec<Vec<u8>> = sel
                .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                .filter_map(|r| r.ok())
                .collect();
            drop(sel);
            let mut upd = tx.prepare("UPDATE infohashes SET l2_shard = ?1 WHERE infohash = ?2")?;
            for ih in &rows {
                let shard = compute_l2_shard(ih) as i64;
                upd.execute(params![shard, ih])?;
            }
        }
        // trackers: key = url
        {
            let mut sel = tx.prepare("SELECT url FROM trackers")?;
            let rows: Vec<String> = sel
                .query_map([], |row| row.get::<_, String>(0))?
                .filter_map(|r| r.ok())
                .collect();
            drop(sel);
            let mut upd = tx.prepare("UPDATE trackers SET l2_shard = ?1 WHERE url = ?2")?;
            for url in &rows {
                let shard = compute_l2_shard(url.as_bytes()) as i64;
                upd.execute(params![shard, url])?;
            }
        }
        tx.commit()?;
        // 迁移标记写入必须成功，否则下次启动会重复全表重算（幂等，无副作用）
        conn.execute_batch(&format!("PRAGMA user_version = {}", SHARD_SCHEME_VERSION))?;
        info!(
            "[storage] l2_shard 迁移完成：4 表已重算为真 L2 分片（scheme v{}）",
            SHARD_SCHEME_VERSION
        );
        Ok(())
    }

    /// 释放 SQLite 内部缓存内存（PRAGMA shrink_memory），内存压力大时调用。
    pub fn shrink_memory(&self) {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let _ = conn.execute_batch("PRAGMA shrink_memory;");
    }
}

/// 计算 key 所属 L1 分片（0..255），与 `MerkleTree::shard_for_key` 一致。
/// 取 blake3(key) 首字节（shard_count=256 时 bytes[0] 即 L1 = L2/256）。
pub fn compute_shard(key: &[u8]) -> u16 {
    blake3::hash(key).as_bytes()[0] as u16
}

/// 计算 key 所属 L2 二级分片（0..65535）。
/// L2 = blake3(key)[0] * 256 + blake3(key)[1]，与 `MerkleTree::l2_shard_for_key` 在
/// shard_count=256（生产固定值）时**完全一致**。L1 = L2 / 256 = blake3(key)[0]。
///
/// P1-5：`dht_nodes/trackers/infohashes/peers` 四表的 `l2_shard` 列自 scheme v2 起**存真 L2 值**
/// （此前存的是 L1，属历史遗留）；写入即填此值，查询按精确 L2 命中，从而消除「按 L2 取数却整条
/// L1 加载」的 256× 放大（R3）。
pub fn compute_l2_shard(key: &[u8]) -> u32 {
    let h = blake3::hash(key);
    let b = h.as_bytes();
    (b[0] as u32) * 256 + (b[1] as u32)
}

/// 四表 `l2_shard` 列的分片方案版本。v2 = 存真 L2；v0/v1 = 历史 L1 方案。
/// 以 SQLite 内置 `PRAGMA user_version` 作为一次性迁移标记。
const SHARD_SCHEME_VERSION: i64 = 2;

/// peers 表分片索引值（真 L2）。
/// key = "<hex_ih>:<ip>:<port>"，与 `load_all_peer_keys_hashes` / Merkle 侧 peer key 约定一致。
fn peer_shard_index(infohash: &[u8], ip: &str, port: u16) -> i64 {
    let hex: String = infohash.iter().map(|b| format!("{:02x}", b)).collect();
    compute_l2_shard(format!("{}:{}:{}", hex, ip, port).as_bytes()) as i64
}

/// DHT 节点行
#[derive(Debug, Clone)]
pub struct DhtNodeRow {
    pub id: [u8; 20],
    pub ip: String,
    pub port: u16,
    pub score: f64,
    pub state: String,
    pub query_count: u64,
    pub success_count: u64,
    pub total_latency_ms: u64,
    pub consecutive_failures: u32,
    pub nodes_returned: u64,
    pub last_query_time: Option<i64>,
}

/// Tracker 行
#[derive(Debug, Clone)]
pub struct TrackerRow {
    pub url: String,
    pub score: f64,
    pub total_requests: u64,
    pub success_requests: u64,
    pub failed_requests: u64,
    pub total_peers_discovered: u64,
    pub total_response_time_ms: f64,
    pub consecutive_failures: u32,
    pub disabled: bool,
}

/// Infohash 行
#[derive(Debug, Clone)]
pub struct InfohashRow {
    pub infohash: [u8; 20],
    pub ref_count: u32,
    pub first_source: String,
    pub score: f64,
}

/// Peer 行（运行时活跃 peer）
#[derive(Debug, Clone)]
pub struct PeerRow {
    pub infohash: [u8; 20],
    pub ip: String,
    pub port: u16,
    pub source: String,
    pub score: f64,
    pub connection_attempts: u32,
    pub connection_successes: u32,
    pub last_active: i64,
}

/// Peer 历史行
#[derive(Debug, Clone)]
pub struct PeerHistoryRow {
    pub ip: String,
    pub port: u16,
    pub source: String,
    pub score: f64,
    pub discovered_at: i64,
}

/// Peer 历史记录（包含 infohash，用于批量写入）
#[derive(Debug, Clone)]
pub struct PeerHistoryEntry {
    pub infohash: [u8; 20],
    pub ip: String,
    pub port: u16,
    pub source: String,
    pub score: f64,
    pub discovered_at: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_tables() {
        let _storage = Storage::memory().unwrap();
        // 表创建成功
    }

    #[test]
    fn test_dht_node_save_load() {
        let storage = Storage::memory().unwrap();
        let id = [1u8; 20];
        storage
            .save_dht_node(
                &id,
                "127.0.0.1",
                6881,
                85.5,
                "Good",
                10,
                8,
                5000,
                1,
                64,
                None,
            )
            .unwrap();
        let nodes = storage.load_dht_nodes().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].ip, "127.0.0.1");
        assert_eq!(nodes[0].port, 6881);
        assert!((nodes[0].score - 85.5).abs() < 0.01);
    }

    #[test]
    fn test_tracker_save_load() {
        let storage = Storage::memory().unwrap();
        storage
            .save_tracker(
                "http://example.com/announce",
                70.0,
                100,
                80,
                20,
                500,
                15000.0,
                1,
                false,
            )
            .unwrap();
        let trackers = storage.load_trackers().unwrap();
        assert_eq!(trackers.len(), 1);
        assert_eq!(trackers[0].url, "http://example.com/announce");
        assert_eq!(trackers[0].total_requests, 100);
    }

    #[test]
    fn test_peer_history() {
        let storage = Storage::memory().unwrap();
        let ih = [2u8; 20];
        storage
            .record_peer_history(&ih, "10.0.0.1", 5000, "tracker", 50.0)
            .unwrap();
        storage
            .record_peer_history(&ih, "10.0.0.2", 5001, "dht", 60.0)
            .unwrap();
        let history = storage.query_peer_history(&ih, 10).unwrap();
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn test_stats_aggregate() {
        let storage = Storage::memory().unwrap();
        storage.update_aggregate("total_requests", 1000.0).unwrap();
        let val = storage.load_aggregate("total_requests").unwrap();
        assert_eq!(val, 1000.0);
    }
}
