//! SQLite 持久化存储
//!
//! 存储路由表、tracker 池、历史数据、统计数据。

#![allow(clippy::type_complexity)]

use std::net::SocketAddr;
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
    /// oplog 行数缓存（-1 = 未初始化）。
    /// `SELECT COUNT(*) FROM feed_oplog` 在大表上要扫整个 B-tree，冷缓存 + 慢盘实测可达
    /// 数十秒（2026-09-21：51 节点 58 万行 oplog 冷查询 28s，盘吞吐仅 ~4MB/s），
    /// 而 `MIN/MAX(seq)` 走主键索引端点恒为 O(log n) 不需要缓存。
    /// 由 oplog 写入/裁剪点增量维护（见 `storage/oplog.rs`），首次读取时 COUNT 校准。
    pub(crate) oplog_len_cache: std::sync::atomic::AtomicI64,
    /// F9: 实体表有效行数缓存（[dht_nodes, peers, peers_archive, infohashes, trackers]，-1 = 未校准）。
    /// 统计口径 = `deleted_at IS NULL`（软删墓碑不入数）。内存 repo 为热/温数据，
    /// 冷数据在本 DB；总数以 DB 为唯一权威口径，由周期任务校准（见 main.rs db_entity_stats）。
    pub(crate) entity_counts_cache: [std::sync::atomic::AtomicI64; 5],
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
            oplog_len_cache: std::sync::atomic::AtomicI64::new(-1),
            entity_counts_cache: Default::default(),
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
            oplog_len_cache: std::sync::atomic::AtomicI64::new(-1),
            entity_counts_cache: Default::default(),
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

    // ---- F9: 实体表 DB 级统计（唯一权威口径：deleted_at IS NULL，内存 repo 为热/温子集）----

    /// 各实体表有效行数：[dht_nodes, peers, peers_archive, infohashes, trackers]。
    /// 软删墓碑（deleted_at 非 NULL）不计入。
    pub fn valid_entity_counts(&self) -> [i64; 5] {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap_or(-2) };
        [
            count("SELECT COUNT(*) FROM dht_nodes WHERE deleted_at IS NULL"),
            count("SELECT COUNT(*) FROM peers WHERE deleted_at IS NULL"),
            count("SELECT COUNT(*) FROM peers_archive"),
            count("SELECT COUNT(*) FROM infohashes WHERE deleted_at IS NULL"),
            count("SELECT COUNT(*) FROM trackers WHERE deleted_at IS NULL"),
        ]
    }

    /// 重算并写回缓存（周期任务调用；COUNT 较重，调用方应放阻塞线程）。
    pub fn refresh_entity_counts(&self) -> [i64; 5] {
        use std::sync::atomic::Ordering;
        let c = self.valid_entity_counts();
        for (i, v) in c.iter().enumerate() {
            self.entity_counts_cache[i].store(*v, Ordering::Relaxed);
        }
        c
    }

    /// 读取缓存计数；未校准（-1）时先 COUNT 校准（仅一次，后续走缓存）。
    pub fn entity_counts_cached(&self) -> [i64; 5] {
        use std::sync::atomic::Ordering;
        if self.entity_counts_cache[0].load(Ordering::Relaxed) < 0 {
            return self.refresh_entity_counts();
        }
        let mut out = [0i64; 5];
        for (i, a) in self.entity_counts_cache.iter().enumerate() {
            out[i] = a.load(Ordering::Relaxed);
        }
        out
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

    /// P0-3：判断指定 tracker 是否存在软删墓碑（`deleted_at` 非 NULL）。
    ///
    /// 用于入站 upsert 的仲裁：本地已删除的 tracker 不得被对端回推的 upsert 复活，
    /// 否则形成「A 删 → B 未删 → 反熵判差异 → B 回推 upsert → A 复活 → 反熵再判差异」
    /// 的永动闭环，删除操作在联邦内结构性不可能收敛。
    pub fn is_tracker_tombstoned(&self, url: &str) -> bool {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.query_row(
            "SELECT 1 FROM trackers WHERE url = ?1 AND deleted_at IS NOT NULL LIMIT 1",
            params![url],
            |_| Ok(true),
        )
        .unwrap_or(false)
    }

    /// P0-3：入站 UPSERT 的安全写入 —— 命中本地墓碑时**保留墓碑**（不复活），
    /// 否则与 [`Self::save_tracker`] 完全一致。
    ///
    /// 与直接改 `save_tracker` 的 upsert（`deleted_at=NULL`）区分开：本地主动重新发现
    /// tracker 时仍走 `save_tracker` 正常复活，只有入站路径才受墓碑约束。
    #[allow(clippy::too_many_arguments)]
    pub fn save_tracker_keep_tombstone(
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
                l2_shard=excluded.l2_shard, updated_at=excluded.updated_at"#,
            params![
                url, score, total_requests as i64, success_requests as i64,
                failed_requests as i64, total_peers_discovered as i64,
                total_response_time_ms, consecutive_failures as i64,
                disabled as i64, now, l2, now
            ],
        )?;
        Ok(())
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

    /// P2-1：按 key（"ip:port" 字符串）升序加载 NODE 原始行 `(id, ip, port)`，范围 `[lo, hi)`，最多 `limit` 条。
    ///
    /// 供 bootstrap 分块服务端与 range 修复通道组装完整 `SyncEntry` 使用；
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

    /// 根据 key 列表（ip:port）批量加载节点完整数据，用于 Range 反熵推送。
    pub fn load_nodes_by_keys(
        &self,
        keys: &[Vec<u8>],
    ) -> anyhow::Result<Vec<crate::storage::db::DhtNodeRow>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let placeholders = vec!["?"; keys.len()].join(",");
        let sql = format!(
            "SELECT id, ip, port, score, state, query_count, success_count,              total_latency_ms, consecutive_failures, nodes_returned              FROM dht_nodes              WHERE deleted_at IS NULL AND (ip || ':' || port) IN ({})",
            placeholders
        );
        let mut stmt = conn.prepare(&sql)?;
        // 把 keys 转成 ip:port 字符串
        let key_strs: Vec<String> = keys
            .iter()
            .map(|k| String::from_utf8_lossy(k).to_string())
            .collect();
        let params: Vec<&dyn rusqlite::ToSql> =
            key_strs.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let rows = stmt.query_map(params.as_slice(), |row| {
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
                nodes_returned: row.get::<_, i64>(9)? as u64,
                last_query_time: None,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
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

    // ---- v7：统一 range 反熵（全 repo 支持）----

    /// v7：按 repo 加载 `(key, data_hash)`，范围 `[lo, hi)`（`None` = ±∞），最多 `limit` 条。
    ///
    /// key 编码与各 repo 的 Merkle / `load_all_*_keys_hashes` 严格一致：
    /// NODE=`ip:port`、PEER=`hex(ih):ip:port`、INFOHASH=原始 20B、TRACKER=url 字节。
    pub fn load_repo_key_hashes_in_range(
        &self,
        repo: u8,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // 与 crate::federation::protocol::repo_type 一致（NODE=1 PEER=2 INFOHASH=3 TRACKER=4）
        const NODE: u8 = 1;
        const PEER: u8 = 2;
        const INFOHASH: u8 = 3;
        const TRACKER: u8 = 4;
        match repo {
            NODE => self.load_node_key_hashes_in_range(lo, hi, limit),
            PEER => {
                let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
                let lo_s = lo.map(|b| String::from_utf8_lossy(b).into_owned());
                let hi_s = hi.map(|b| String::from_utf8_lossy(b).into_owned());
                let key_expr = "(lower(hex(infohash)) || ':' || ip || ':' || port)";
                let mut stmt = conn.prepare(&format!(
                    "SELECT infohash, ip, port FROM peers \
                     WHERE deleted_at IS NULL \
                       AND (?1 IS NULL OR {key_expr} >= ?1) \
                       AND (?2 IS NULL OR {key_expr} < ?2) \
                     ORDER BY {key_expr} ASC LIMIT ?3"
                ))?;
                let rows = stmt.query_map(params![lo_s, hi_s, limit.max(1) as i64], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
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
                    out.push((key, blake3::hash(&buf).as_bytes().to_vec()));
                }
                Ok(out)
            }
            INFOHASH => {
                let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
                let mut stmt = conn.prepare(
                    "SELECT infohash FROM infohashes \
                     WHERE deleted_at IS NULL \
                       AND (?1 IS NULL OR infohash >= ?1) \
                       AND (?2 IS NULL OR infohash < ?2) \
                     ORDER BY infohash ASC LIMIT ?3",
                )?;
                let rows = stmt.query_map(params![lo, hi, limit.max(1) as i64], |row| {
                    row.get::<_, Vec<u8>>(0)
                })?;
                let mut out = Vec::new();
                for r in rows {
                    let ih = r?;
                    out.push((ih.clone(), blake3::hash(&ih).as_bytes().to_vec()));
                }
                Ok(out)
            }
            TRACKER => {
                let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
                let lo_s = lo.map(|b| String::from_utf8_lossy(b).into_owned());
                let hi_s = hi.map(|b| String::from_utf8_lossy(b).into_owned());
                let mut stmt = conn.prepare(
                    "SELECT url FROM trackers \
                     WHERE deleted_at IS NULL \
                       AND (?1 IS NULL OR url >= ?1) \
                       AND (?2 IS NULL OR url < ?2) \
                     ORDER BY url ASC LIMIT ?3",
                )?;
                let rows = stmt.query_map(params![lo_s, hi_s, limit.max(1) as i64], |row| {
                    row.get::<_, String>(0)
                })?;
                let mut out = Vec::new();
                for r in rows {
                    let url = r?;
                    out.push((
                        url.clone().into_bytes(),
                        blake3::hash(url.as_bytes()).as_bytes().to_vec(),
                    ));
                }
                Ok(out)
            }
            _ => Ok(Vec::new()),
        }
    }

    /// v7：按 repo 随机抽样 `n` 个分界 key（LCG 探 rowid，同 [`Self::sample_node_range_keys`]）。
    pub fn sample_repo_range_keys(&self, repo: u8, n: usize) -> anyhow::Result<Vec<Vec<u8>>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        // (表名, key 表达式)：key 表达式须与 load_repo_key_hashes_in_range 的排序键一致
        // 与 crate::federation::protocol::repo_type 一致（NODE=1 PEER=2 INFOHASH=3 TRACKER=4）
        const NODE: u8 = 1;
        const PEER: u8 = 2;
        const INFOHASH: u8 = 3;
        const TRACKER: u8 = 4;
        let (table, key_sql): (&str, &str) = match repo {
            NODE => ("dht_nodes", "(ip || ':' || port)"),
            PEER => (
                "peers",
                "(lower(hex(infohash)) || ':' || ip || ':' || port)",
            ),
            INFOHASH => ("infohashes", "infohash"),
            TRACKER => ("trackers", "url"),
            _ => return Ok(Vec::new()),
        };
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let max_rowid: i64 = conn.query_row(
            &format!("SELECT COALESCE(MAX(rowid), 0) FROM {}", table),
            [],
            |r| r.get(0),
        )?;
        if max_rowid <= 0 {
            return Ok(Vec::new());
        }
        let mut state: u64 = (chrono::Utc::now().timestamp_millis() as u64) ^ 0x9E37_79B9_7F4A_7C15;
        let sql = format!(
            "SELECT {key_sql} FROM {table} WHERE deleted_at IS NULL AND rowid >= ?1 \
             ORDER BY rowid ASC LIMIT 1"
        );
        let mut stmt = conn.prepare_cached(&sql)?;
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(n);
        for _ in 0..n {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let probe = ((state >> 33) % (max_rowid as u64)) as i64;
            // key 可能是 TEXT（NODE/PEER/TRACKER）或 BLOB（INFOHASH），用 Value 中转
            if let Ok(v) = stmt.query_row(params![probe], |row| {
                row.get::<_, rusqlite::types::Value>(0)
            }) {
                let k = match v {
                    rusqlite::types::Value::Text(s) => s.into_bytes(),
                    rusqlite::types::Value::Blob(b) => b,
                    _ => continue,
                };
                keys.push(k);
            }
        }
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    /// v7：按 repo 加载 `[lo, hi)` 区间内的完整 `SyncEntry`（bootstrap 分块服务用）。
    ///
    /// 条目编码与各 repo 的 gossip 同步条目严格一致（复用 `build_*_sync_entry` 系列函数）。
    pub fn load_repo_sync_entries_in_range(
        &self,
        repo: u8,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        limit: usize,
    ) -> anyhow::Result<Vec<crate::federation::protocol::SyncEntry>> {
        use crate::federation::protocol::{operation, SyncEntry};
        const NODE: u8 = 1;
        const PEER: u8 = 2;
        const INFOHASH: u8 = 3;
        const TRACKER: u8 = 4;
        let to_entry = |t: Option<(Vec<u8>, Vec<u8>, Vec<u8>)>| {
            t.map(|(key, payload, _dh)| SyncEntry {
                key,
                operation: operation::UPSERT,
                version: 0,
                payload,
            })
        };
        match repo {
            NODE => {
                let rows = self.load_node_rows_in_range(lo, hi, limit)?;
                let mut out = Vec::with_capacity(rows.len());
                for (id, ip, port) in rows {
                    let mut arr = [0u8; 20];
                    if id.len() == 20 {
                        arr.copy_from_slice(&id);
                    }
                    if let Ok(addr) = format!("{}:{}", ip, port).parse::<SocketAddr>() {
                        if let Some(e) =
                            to_entry(crate::federation::sync::build_node_sync_entry(arr, addr))
                        {
                            out.push(e);
                        }
                    }
                }
                Ok(out)
            }
            PEER => {
                let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
                let lo_s = lo.map(|b| String::from_utf8_lossy(b).into_owned());
                let hi_s = hi.map(|b| String::from_utf8_lossy(b).into_owned());
                let key_expr = "(lower(hex(infohash)) || ':' || ip || ':' || port)";
                let mut stmt = conn.prepare(&format!(
                    "SELECT infohash, ip, port FROM peers \
                     WHERE deleted_at IS NULL \
                       AND (?1 IS NULL OR {key_expr} >= ?1) \
                       AND (?2 IS NULL OR {key_expr} < ?2) \
                     ORDER BY {key_expr} ASC LIMIT ?3"
                ))?;
                let rows = stmt.query_map(params![lo_s, hi_s, limit.max(1) as i64], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })?;
                let mut out = Vec::new();
                for r in rows {
                    let (ih, ip, port) = r?;
                    let mut arr = [0u8; 20];
                    if ih.len() == 20 {
                        arr.copy_from_slice(&ih);
                    }
                    if let Ok(addr) = format!("{}:{}", ip, port).parse::<SocketAddr>() {
                        if let Some(e) = to_entry(
                            crate::federation::sync::peer_sync::build_peer_sync_entry(arr, addr),
                        ) {
                            out.push(e);
                        }
                    }
                }
                Ok(out)
            }
            INFOHASH => {
                let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
                let mut stmt = conn.prepare(
                    "SELECT infohash FROM infohashes \
                     WHERE deleted_at IS NULL \
                       AND (?1 IS NULL OR infohash >= ?1) \
                       AND (?2 IS NULL OR infohash < ?2) \
                     ORDER BY infohash ASC LIMIT ?3",
                )?;
                let rows = stmt.query_map(params![lo, hi, limit.max(1) as i64], |row| {
                    row.get::<_, Vec<u8>>(0)
                })?;
                let mut out = Vec::new();
                for r in rows {
                    let ih = r?;
                    if ih.len() != 20 {
                        continue;
                    }
                    let mut arr = [0u8; 20];
                    arr.copy_from_slice(&ih);
                    if let Some(e) = to_entry(
                        crate::federation::sync::infohash_sync::build_infohash_sync_entry(arr),
                    ) {
                        out.push(e);
                    }
                }
                Ok(out)
            }
            TRACKER => {
                let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
                let lo_s = lo.map(|b| String::from_utf8_lossy(b).into_owned());
                let hi_s = hi.map(|b| String::from_utf8_lossy(b).into_owned());
                let mut stmt = conn.prepare(
                    "SELECT url FROM trackers \
                     WHERE deleted_at IS NULL \
                       AND (?1 IS NULL OR url >= ?1) \
                       AND (?2 IS NULL OR url < ?2) \
                     ORDER BY url ASC LIMIT ?3",
                )?;
                let rows = stmt.query_map(params![lo_s, hi_s, limit.max(1) as i64], |row| {
                    row.get::<_, String>(0)
                })?;
                let mut out = Vec::new();
                for r in rows {
                    let url = r?;
                    if let Some(e) = to_entry(
                        crate::federation::sync::tracker_sync::build_tracker_sync_entry(&url),
                    ) {
                        out.push(e);
                    }
                }
                Ok(out)
            }
            _ => Ok(Vec::new()),
        }
    }

    /// v8：按 repo 按 key 列表精确加载完整 `SyncEntry`（range 反熵修复通道用）。
    ///
    /// key 必须是 `load_repo_key_hashes_in_range` 产出的 DB 形态：
    /// NODE `"ip:port"`（IPv6 无方括号）/ PEER `"hex(ih):ip:port"` / INFOHASH 20B 原文 / TRACKER url。
    /// 查询走各表主键（dht_nodes (ip,port)、peers (infohash,ip,port)、infohashes/trackers 单列主键），
    /// 按批绑定参数，防超 SQLite 变量上限。
    pub fn load_repo_entries_by_keys(
        &self,
        repo: u8,
        keys: &[Vec<u8>],
    ) -> anyhow::Result<Vec<crate::federation::protocol::SyncEntry>> {
        use crate::federation::protocol::{operation, SyncEntry};
        const NODE: u8 = 1;
        const PEER: u8 = 2;
        const INFOHASH: u8 = 3;
        const TRACKER: u8 = 4;
        let to_entry = |t: Option<(Vec<u8>, Vec<u8>, Vec<u8>)>| {
            t.map(|(key, payload, _dh)| SyncEntry {
                key,
                operation: operation::UPSERT,
                version: 0,
                payload,
            })
        };
        match repo {
            NODE => {
                let mut pairs: Vec<(String, i64)> = Vec::with_capacity(keys.len());
                for k in keys {
                    let Ok(s) = std::str::from_utf8(k) else {
                        continue;
                    };
                    // rsplit 取最后一个 ':'：IPv4 "1.2.3.4:6881" 与 IPv6 "::1:8080" 都正确
                    let Some((ip, port_s)) = s.rsplit_once(':') else {
                        continue;
                    };
                    let Ok(port) = port_s.parse::<i64>() else {
                        continue;
                    };
                    pairs.push((ip.to_string(), port));
                }
                pairs.sort();
                pairs.dedup();
                let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
                let mut out = Vec::with_capacity(pairs.len());
                for chunk in pairs.chunks(400) {
                    let placeholders = chunk
                        .iter()
                        .map(|_| "(? , ?)")
                        .collect::<Vec<_>>()
                        .join(", ");
                    let sql = format!(
                        "SELECT id, ip, port FROM dht_nodes \
                         WHERE deleted_at IS NULL AND (ip, port) IN ({placeholders})"
                    );
                    let mut stmt = conn.prepare(&sql)?;
                    let mut bind: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(chunk.len() * 2);
                    for (ip, port) in chunk {
                        bind.push(ip);
                        bind.push(port);
                    }
                    let rows = stmt.query_map(bind.as_slice(), |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    })?;
                    for r in rows {
                        let (id, ip, port) = r?;
                        let mut arr = [0u8; 20];
                        if id.len() == 20 {
                            arr.copy_from_slice(&id);
                        }
                        if let Ok(addr) = format!("{}:{}", ip, port).parse::<SocketAddr>() {
                            if let Some(e) =
                                to_entry(crate::federation::sync::build_node_sync_entry(arr, addr))
                            {
                                out.push(e);
                            }
                        }
                    }
                }
                Ok(out)
            }
            PEER => {
                // key = "hex(ih):ip:port"：rsplit 两段得到 port 与 ip，剩余前缀为 ih 的 hex
                let mut triples: Vec<(Vec<u8>, String, i64)> = Vec::with_capacity(keys.len());
                for k in keys {
                    let Ok(s) = std::str::from_utf8(k) else {
                        continue;
                    };
                    let Some((rest, port_s)) = s.rsplit_once(':') else {
                        continue;
                    };
                    let Some((ih_hex, ip)) = rest.rsplit_once(':') else {
                        continue;
                    };
                    let (Ok(port), Ok(ih)) = (port_s.parse::<i64>(), hex::decode(ih_hex)) else {
                        continue;
                    };
                    if ih.len() != 20 {
                        continue;
                    }
                    triples.push((ih, ip.to_string(), port));
                }
                triples.sort();
                triples.dedup();
                let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
                let mut out = Vec::with_capacity(triples.len());
                for chunk in triples.chunks(300) {
                    let placeholders = chunk
                        .iter()
                        .map(|_| "(? , ? , ?)")
                        .collect::<Vec<_>>()
                        .join(", ");
                    let sql = format!(
                        "SELECT infohash, ip, port FROM peers \
                         WHERE deleted_at IS NULL AND (infohash, ip, port) IN ({placeholders})"
                    );
                    let mut stmt = conn.prepare(&sql)?;
                    let mut bind: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(chunk.len() * 3);
                    for (ih, ip, port) in chunk {
                        bind.push(ih);
                        bind.push(ip);
                        bind.push(port);
                    }
                    let rows = stmt.query_map(bind.as_slice(), |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    })?;
                    for r in rows {
                        let (ih, ip, port) = r?;
                        let mut arr = [0u8; 20];
                        if ih.len() == 20 {
                            arr.copy_from_slice(&ih);
                        }
                        if let Ok(addr) = format!("{}:{}", ip, port).parse::<SocketAddr>() {
                            if let Some(e) =
                                to_entry(crate::federation::sync::peer_sync::build_peer_sync_entry(
                                    arr, addr,
                                ))
                            {
                                out.push(e);
                            }
                        }
                    }
                }
                Ok(out)
            }
            INFOHASH => {
                let ih_keys: Vec<Vec<u8>> =
                    keys.iter().filter(|k| k.len() == 20).cloned().collect();
                let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
                let mut out = Vec::with_capacity(ih_keys.len());
                for chunk in ih_keys.chunks(900) {
                    let placeholders = std::iter::repeat_n("?", chunk.len())
                        .collect::<Vec<_>>()
                        .join(", ");
                    let sql = format!(
                        "SELECT infohash FROM infohashes \
                         WHERE deleted_at IS NULL AND infohash IN ({placeholders})"
                    );
                    let mut stmt = conn.prepare(&sql)?;
                    let bind: Vec<&dyn rusqlite::ToSql> =
                        chunk.iter().map(|k| k as &dyn rusqlite::ToSql).collect();
                    let rows = stmt.query_map(bind.as_slice(), |row| row.get::<_, Vec<u8>>(0))?;
                    for r in rows {
                        let ih = r?;
                        if ih.len() != 20 {
                            continue;
                        }
                        let mut arr = [0u8; 20];
                        arr.copy_from_slice(&ih);
                        if let Some(e) = to_entry(
                            crate::federation::sync::infohash_sync::build_infohash_sync_entry(arr),
                        ) {
                            out.push(e);
                        }
                    }
                }
                Ok(out)
            }
            TRACKER => {
                let mut urls: Vec<String> = keys
                    .iter()
                    .map(|k| String::from_utf8_lossy(k).into_owned())
                    .collect();
                urls.sort();
                urls.dedup();
                let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
                let mut out = Vec::with_capacity(urls.len());
                for chunk in urls.chunks(500) {
                    let placeholders = std::iter::repeat_n("?", chunk.len())
                        .collect::<Vec<_>>()
                        .join(", ");
                    let sql = format!(
                        "SELECT url FROM trackers \
                         WHERE deleted_at IS NULL AND url IN ({placeholders})"
                    );
                    let mut stmt = conn.prepare(&sql)?;
                    let bind: Vec<&dyn rusqlite::ToSql> =
                        chunk.iter().map(|u| u as &dyn rusqlite::ToSql).collect();
                    let rows = stmt.query_map(bind.as_slice(), |row| row.get::<_, String>(0))?;
                    for r in rows {
                        let url = r?;
                        if let Some(e) = to_entry(
                            crate::federation::sync::tracker_sync::build_tracker_sync_entry(&url),
                        ) {
                            out.push(e);
                        }
                    }
                }
                Ok(out)
            }
            _ => Ok(Vec::new()),
        }
    }

    /// 释放 SQLite 内部缓存内存（PRAGMA shrink_memory），内存压力大时调用。
    pub fn shrink_memory(&self) {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let _ = conn.execute_batch("PRAGMA shrink_memory;");
    }
}
/// 计算 key 所属 L2 二级分片（0..65535）。
/// L2 = blake3(key)[0] * 256 + blake3(key)[1]，与各 repo 写入路径（`peer_shard_index` 等）
/// 采用同一公式。
/// P1-5：`dht_nodes/trackers/infohashes/peers` 四表的 `l2_shard` 列自 scheme v2 起**存真 L2 值**
/// （此前存的是 L1，属历史遗留）；写入即填此值，查询按精确 L2 命中，从而消除「按 L2 取数却整条
/// L1 加载」的 256× 放大（R3）。
pub fn compute_l2_shard(key: &[u8]) -> u32 {
    let h = blake3::hash(key);
    let b = h.as_bytes();
    (b[0] as u32) * 256 + (b[1] as u32)
}

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
