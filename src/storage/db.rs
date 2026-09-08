//! SQLite 持久化存储
//!
//! 存储路由表、tracker 池、历史数据、统计数据。

use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection};
use tracing::{debug, info};

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
    conn: Mutex<Connection>,
    write_stats: Arc<Mutex<WriteStats>>,
}

impl Storage {
    /// 打开或创建数据库
    pub fn open<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path_ref = path.as_ref();
        if let Some(parent) = path_ref.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let conn = Connection::open(path_ref)?;
        // WAL 模式 + NORMAL 同步 + 2000 页自动 checkpoint（减少 checkpoint 频率，提升写入性能）
        // 注意：不使用 mmap，Windows 上 mmap 会造成持续磁盘 IO（内存页面持续刷盘）
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA wal_autocheckpoint=2000; PRAGMA temp_store=MEMORY;")?;

        let storage = Self {
            conn: Mutex::new(conn),
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
            conn: Mutex::new(conn),
            write_stats: Arc::new(Mutex::new(WriteStats::default())),
        };
        storage.init_tables()?;
        Ok(storage)
    }

    /// 获取写入统计
    pub fn write_stats(&self) -> WriteStats {
        self.write_stats.lock().unwrap().clone()
    }

    /// 记录写入统计
    fn record_write(&self, table: &str, rows: u64) {
        let mut stats = self.write_stats.lock().unwrap();
        stats.total_writes += 1;
        match table {
            "dht_nodes" => { stats.dht_nodes_writes += 1; stats.dht_nodes_rows += rows; }
            "peers" => { stats.peers_writes += 1; stats.peers_rows += rows; }
            "peer_history" => { stats.peer_history_writes += 1; stats.peer_history_rows += rows; }
            "trackers" => { stats.trackers_writes += 1; stats.trackers_rows += rows; }
            "infohashes" => { stats.infohashes_writes += 1; stats.infohashes_rows += rows; }
            "stats" => { stats.stats_writes += 1; }
            _ => {}
        }
    }

    /// 手动执行 WAL checkpoint（将 WAL 合并到主数据库文件）
    pub fn checkpoint(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        debug!("[storage] WAL checkpoint 已执行");
        Ok(())
    }

    /// 执行 VACUUM（清理碎片，压缩数据库）
    pub fn vacuum(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("VACUUM;")?;
        info!("[storage] VACUUM 已完成");
        Ok(())
    }

    /// 初始化表结构
    fn init_tables(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
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
                last_used INTEGER
            );

            CREATE TABLE IF NOT EXISTS infohashes (
                infohash BLOB PRIMARY KEY,
                ref_count INTEGER DEFAULT 1,
                first_source TEXT,
                first_seen INTEGER,
                last_seen INTEGER
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
        let _ = conn.execute("ALTER TABLE dht_nodes ADD COLUMN nodes_returned INTEGER DEFAULT 0", []);
        let _ = conn.execute("ALTER TABLE dht_nodes ADD COLUMN last_query_time INTEGER", []);

        debug!("[storage] 表结构初始化完成");
        Ok(())
    }

    // ---- DHT 节点 ----

    /// 保存 DHT 节点（upsert）
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
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            r#"INSERT INTO dht_nodes (id, ip, port, score, state, query_count, success_count,
                total_latency_ms, consecutive_failures, nodes_returned, last_query_time,
                last_active, first_seen)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12)
               ON CONFLICT(ip, port) DO UPDATE SET
                id=excluded.id, score=excluded.score, state=excluded.state,
                query_count=excluded.query_count, success_count=excluded.success_count,
                total_latency_ms=excluded.total_latency_ms,
                consecutive_failures=excluded.consecutive_failures,
                nodes_returned=excluded.nodes_returned,
                last_query_time=excluded.last_query_time,
                last_active=excluded.last_active"#,
            params![
                id.as_slice(), ip, port as i64, score, state,
                query_count as i64, success_count as i64, total_latency_ms as i64,
                consecutive_failures as i64, nodes_returned as i64, last_query_time, now
            ],
        )?;
        Ok(())
    }

    /// 加载所有 DHT 节点
    pub fn load_dht_nodes(&self) -> anyhow::Result<Vec<DhtNodeRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id, ip, port, score, state, query_count, success_count, total_latency_ms, consecutive_failures, nodes_returned, last_query_time FROM dht_nodes")?;
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
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM dht_nodes", [])?;
        Ok(())
    }

    /// 批量保存 DHT 节点（事务批量插入，一次获取锁完成所有操作）
    pub fn save_dht_nodes_batch(&self, nodes: &[DhtNodeRow]) -> anyhow::Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        self.record_write("dht_nodes", nodes.len() as u64);
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare(
                r#"INSERT INTO dht_nodes (id, ip, port, score, state, query_count, success_count,
                    total_latency_ms, consecutive_failures, nodes_returned, last_query_time,
                    last_active, first_seen)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12)
                   ON CONFLICT(ip, port) DO UPDATE SET
                    id=excluded.id, score=excluded.score, state=excluded.state,
                    query_count=excluded.query_count, success_count=excluded.success_count,
                    total_latency_ms=excluded.total_latency_ms,
                    consecutive_failures=excluded.consecutive_failures,
                    nodes_returned=excluded.nodes_returned,
                    last_query_time=excluded.last_query_time,
                    last_active=excluded.last_active"#,
            )?;
            for node in nodes {
                stmt.execute(params![
                    node.id.as_slice(), node.ip.as_str(), node.port as i64, node.score,
                    node.state.as_str(), node.query_count as i64, node.success_count as i64,
                    node.total_latency_ms as i64, node.consecutive_failures as i64,
                    node.nodes_returned as i64, node.last_query_time, now
                ])?;
            }
        }
        tx.commit()?;
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
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            r#"INSERT INTO trackers (url, score, total_requests, success_requests, failed_requests,
                total_peers_discovered, total_response_time_ms, consecutive_failures, disabled, last_used)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
               ON CONFLICT(url) DO UPDATE SET
                score=excluded.score, total_requests=excluded.total_requests,
                success_requests=excluded.success_requests,
                failed_requests=excluded.failed_requests,
                total_peers_discovered=excluded.total_peers_discovered,
                total_response_time_ms=excluded.total_response_time_ms,
                consecutive_failures=excluded.consecutive_failures,
                disabled=excluded.disabled, last_used=excluded.last_used"#,
            params![
                url, score, total_requests as i64, success_requests as i64,
                failed_requests as i64, total_peers_discovered as i64,
                total_response_time_ms, consecutive_failures as i64,
                disabled as i64, now
            ],
        )?;
        Ok(())
    }

    /// 加载所有 tracker
    pub fn load_trackers(&self) -> anyhow::Result<Vec<TrackerRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT url, score, total_requests, success_requests, failed_requests, total_peers_discovered, total_response_time_ms, consecutive_failures, disabled FROM trackers")?;
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
    ) -> anyhow::Result<()> {
        self.record_write("infohashes", 1);
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            r#"INSERT INTO infohashes (infohash, ref_count, first_source, first_seen, last_seen)
               VALUES (?1, ?2, ?3, ?4, ?4)
               ON CONFLICT(infohash) DO UPDATE SET
                ref_count=excluded.ref_count, last_seen=excluded.last_seen"#,
            params![infohash.as_slice(), ref_count as i64, first_source, now],
        )?;
        Ok(())
    }

    /// 加载所有 infohash
    pub fn load_infohashes(&self) -> anyhow::Result<Vec<InfohashRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT infohash, ref_count, first_source FROM infohashes")?;
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
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// 清空 infohash 表
    pub fn clear_infohashes(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM infohashes", [])?;
        Ok(())
    }

    // ---- Peers（运行时活跃 peer 全量持久化）----

    /// 保存 peer（upsert）
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
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"INSERT INTO peers (infohash, ip, port, source, score, connection_attempts, connection_successes, last_active)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
               ON CONFLICT(infohash, ip, port) DO UPDATE SET
                source=excluded.source, score=excluded.score,
                connection_attempts=excluded.connection_attempts,
                connection_successes=excluded.connection_successes,
                last_active=excluded.last_active"#,
            params![
                infohash.as_slice(), ip, port as i64, source,
                score, connection_attempts as i64,
                connection_successes as i64, last_active,
            ],
        )?;
        Ok(())
    }

    /// 加载所有 peer
    pub fn load_peers(&self) -> anyhow::Result<Vec<PeerRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT infohash, ip, port, source, score, connection_attempts, connection_successes, last_active FROM peers")?;
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
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM peers", [])?;
        Ok(())
    }

    /// 批量保存 peers（事务批量插入）
    pub fn save_peers_batch(&self, peers: &[PeerRow]) -> anyhow::Result<()> {
        if peers.is_empty() {
            return Ok(());
        }
        self.record_write("peers", peers.len() as u64);
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare(
                r#"INSERT INTO peers (infohash, ip, port, source, score, connection_attempts, connection_successes, last_active)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                   ON CONFLICT(infohash, ip, port) DO UPDATE SET
                    source=excluded.source, score=excluded.score,
                    connection_attempts=excluded.connection_attempts,
                    connection_successes=excluded.connection_successes,
                    last_active=excluded.last_active"#,
            )?;
            for peer in peers {
                stmt.execute(params![
                    peer.infohash.as_slice(), peer.ip.as_str(), peer.port as i64,
                    peer.source.as_str(), peer.score,
                    peer.connection_attempts as i64, peer.connection_successes as i64,
                    peer.last_active
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// 归档冷数据：将超过指定时间无活跃的 peer 从主表迁移到归档表
    /// 返回归档的 peer 数量
    pub fn archive_cold_peers(&self, older_than_secs: i64) -> anyhow::Result<usize> {
        let conn = self.conn.lock().unwrap();
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
        tx.execute("DELETE FROM peers WHERE last_active < ?1", params![threshold])?;
        tx.commit()?;

        Ok(count as usize)
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
        let conn = self.conn.lock().unwrap();
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
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare(
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
        }
        tx.commit()?;
        Ok(())
    }

    /// 查询某 infohash 的 peer 历史
    pub fn query_peer_history(&self, infohash: &[u8; 20], limit: usize) -> anyhow::Result<Vec<PeerHistoryRow>> {
        let conn = self.conn.lock().unwrap();
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
        let conn = self.conn.lock().unwrap();
        let cutoff = chrono::Utc::now().timestamp() - (days as i64 * 86400);
        let deleted = conn.execute("DELETE FROM peer_history WHERE discovered_at < ?1", params![cutoff])?;
        if deleted > 0 {
            debug!("[storage] 清理了 {} 条过期 peer 历史", deleted);
        }
        Ok(deleted)
    }

    // ---- Stats History ----

    /// 记录统计快照
    pub fn record_stats(&self, metric: &str, value: f64) -> anyhow::Result<()> {
        self.record_write("stats", 1);
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO stats_history (timestamp, metric, value) VALUES (?1, ?2, ?3)",
            params![now, metric, value],
        )?;
        Ok(())
    }

    /// 查询统计历史
    pub fn query_stats_history(&self, metric: &str, hours: u64) -> anyhow::Result<Vec<(i64, f64)>> {
        let conn = self.conn.lock().unwrap();
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
        let conn = self.conn.lock().unwrap();
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
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT value FROM stats_aggregate WHERE metric = ?1",
            params![metric],
            |row| row.get(0),
        )
        .ok()
    }
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
        let storage = Storage::memory().unwrap();
        // 表创建成功
    }

    #[test]
    fn test_dht_node_save_load() {
        let storage = Storage::memory().unwrap();
        let id = [1u8; 20];
        storage.save_dht_node(&id, "127.0.0.1", 6881, 85.5, "Good", 10, 8, 5000, 1, 64, None).unwrap();
        let nodes = storage.load_dht_nodes().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].ip, "127.0.0.1");
        assert_eq!(nodes[0].port, 6881);
        assert!((nodes[0].score - 85.5).abs() < 0.01);
    }

    #[test]
    fn test_tracker_save_load() {
        let storage = Storage::memory().unwrap();
        storage.save_tracker("http://example.com/announce", 70.0, 100, 80, 20, 500, 15000.0, 1, false).unwrap();
        let trackers = storage.load_trackers().unwrap();
        assert_eq!(trackers.len(), 1);
        assert_eq!(trackers[0].url, "http://example.com/announce");
        assert_eq!(trackers[0].total_requests, 100);
    }

    #[test]
    fn test_peer_history() {
        let storage = Storage::memory().unwrap();
        let ih = [2u8; 20];
        storage.record_peer_history(&ih, "10.0.0.1", 5000, "tracker", 50.0).unwrap();
        storage.record_peer_history(&ih, "10.0.0.2", 5001, "dht", 60.0).unwrap();
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
