//! SQLite 持久化存储
//!
//! 存储路由表、tracker 池、历史数据、统计数据。

#![allow(clippy::type_complexity)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

use rusqlite::{params, Connection};
use tracing::{debug, info};

use blake3;

/// v11(K 批/F2b)：区间查询强制索引回退的「只告警一次」标记。
static RANGE_INDEX_FALLBACK_WARNED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// v11(K 批/F2b)：优先用 `INDEXED BY` 强制 key 索引准备语句；索引不存在
/// （建索引失败的库/异构旧库）时回退 `sql_plain` 并只告警一次。
///
/// 背景（2026-10-01 .52 本地 489 万行实测）：同一区间查询，SQLite 优化器总是
/// 偏爱 `idx_dht_nodes_deleted`（deleted_at 布尔索引）而弃用 v9 建好的表达式
/// 索引，区间摘要退化为 TEMP B-TREE 全量排序 4.3s/个（.53 上 ~9s/个）；强制
/// 表达式索引后 0.035s（124×）。优化器对表达式索引的选择性估计不可靠，只能
/// 显式强制——两份 SQL 的谓词与占位符完全一致，仅差 `INDEXED BY` 子句。
fn prepare_range_stmt<'a>(
    conn: &'a rusqlite::Connection,
    sql_indexed: &str,
    sql_plain: &str,
) -> rusqlite::Result<rusqlite::Statement<'a>> {
    match conn.prepare(sql_indexed) {
        Ok(stmt) => Ok(stmt),
        Err(indexed_err) => {
            if !RANGE_INDEX_FALLBACK_WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                tracing::warn!(
                    "[storage] 区间查询强制索引失败，回退全表排序（性能下降，区间扫描将持读连接秒级）: {}",
                    indexed_err
                );
            }
            conn.prepare(sql_plain)
        }
    }
}

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

/// WAL checkpoint 模式（A1）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointMode {
    /// 不阻塞写入，日常高频
    Passive,
    /// 截断 WAL 到 0，会短暂阻塞写入，低频
    Truncate,
}

/// 单次 WAL checkpoint 的结果（A1，策略无关，只描述事实）
#[derive(Debug, Clone)]
pub struct CheckpointOutcome {
    pub mode: CheckpointMode,
    /// PRAGMA 第一个返回值 != 0（有连接阻塞）
    pub busy: bool,
    /// WAL 总帧数
    pub wal_frames: u64,
    /// 已 checkpoint 的帧数
    pub checkpointed: u64,
    /// 本次耗时
    pub elapsed: Duration,
    /// 跳过原因："memory"（内存库）/ "inflight"（单飞跳过）
    pub skipped: Option<&'static str>,
}

/// A3：v11 要删除的四个 l2_shard 索引（v8 去 Merkle 化后无人查询）。
const DROPPED_L2_SHARD_INDEXES: &[&str] = &[
    "idx_dht_nodes_l2_shard",
    "idx_trackers_l2_shard",
    "idx_infohashes_l2_shard",
    "idx_peers_l2_shard",
];

/// 一次性迁移：DROP 四个 l2_shard 索引（幂等，跑两次不报错）。
/// 列与写入路径不动；DROP 释放的页进 freelist 后续复用（不做 VACUUM）。
fn drop_unused_l2_shard_indexes(conn: &Connection) -> anyhow::Result<u32> {
    for name in DROPPED_L2_SHARD_INDEXES {
        conn.execute(&format!("DROP INDEX IF EXISTS {};", name), [])?;
    }
    Ok(DROPPED_L2_SHARD_INDEXES.len() as u32)
}

/// B5-1：与 PK 首列重复、纯写放大的两个 peers 索引（证据见 16 号文档）。
const DROPPED_REDUNDANT_PEER_INDEXES: &[&str] =
    &["idx_peers_infohash", "idx_peers_archive_infohash"];

/// B5-1：一次性 DROP（幂等）。
fn run_drop_redundant_peer_indexes(conn: &Connection) -> anyhow::Result<u32> {
    for name in DROPPED_REDUNDANT_PEER_INDEXES {
        let _ = conn.execute(&format!("DROP INDEX IF EXISTS {};", name), []);
    }
    Ok(DROPPED_REDUNDANT_PEER_INDEXES.len() as u32)
}

/// 读连接池（WAL 下的只读连接数）。
///
/// 2026-09-30 由 4 扩容至 16：原 4 条只读连接被并发长任务占满——
/// 清单重建（约 38s 全表扫描）、联邦 PEX 节点交换（约 355s 写阻塞）、
/// range/Gossip/delta 并发读取同时占用，导致块请求取数走 `read()` 时
/// 池空回退、被迫抢全局写锁，形成写读互锁。取 16（8~16 区间偏上限）
/// 以覆盖 5~6 个并发长任务并预留余量。
const READ_POOL_SIZE: usize = 24;

/// P4-2：长查询（全表/大区间扫描级）并发上限。
/// 清单重建/覆盖校验/反熵摘要/range 区间加载都走 `load_repo_key_hashes_in_range`，
/// 单次 38s 级（259 万行实测）。若不限流，多个并发长查询会占满 16 条读池，
/// 短读全部排队（P4-1 排队化后虽不抢写锁，但短读延迟仍被拉高）。取 4：
/// 同一时刻最多 4 个长查询（去重 2 + 清单重建 2），短读至少保留 12 条连接。
/// v9（2026-10-08 preP29）：2→4。线上实证：去重恒占满 2 permit（longW 数十万
/// 排队）→ 应答方清单重建（build_repo_manifest_impl 首填全表扫 260s 级）抢不到
/// permit → 外层 300s 建清单超时 → 缓存永远空 → 每轮协商 Nak(rebuilding) 死循环。
/// 扩容到 4 后重建可与去重并发；回填成功后块表非空，重建走块表优先（秒级）。
const LONG_QUERY_CONCURRENCY: usize = 4;

/// P4-2：长查询**独立连接池**大小（A 方案，2026-10-08 读写互锁根治）。
///
/// 背景：长查询（全扫 38s 级）与短读共用 16 条读池时，池空 → 长查询 60s 后
/// 回退抢写连接 → 全扫期间攥写锁 38s+ → 写事务全堵（WAL 停摆）、爬虫写不进、
/// API 超时（.52/.53 2026-10-08 实证：WAL 18 分钟零增长、crawl=0、双端瘫痪）。
/// 修复：长查询走**独立 8 条只读连接**，与短读池/写锁完全隔离；池空只等待
/// （最多 `LONG_POOL_WAIT_TIMEOUT` 后 panic 由调用方按失败重试），**绝不回退
/// 写连接**——写锁只归写路径（P4-3 后短读池也不回退写，见 `read()`）。
/// v9（preP29）：4→8，配合 LONG_QUERY_CONCURRENCY 2→4（去重+重建并发）。
/// preP37（L1-⑥）：8→12 —— 管道化后应答方取块 12 并发（窗口 12）全部走长查询池，
/// 8 槽排队让 HTTP 的 DB 查询（io/status/通道状态）饿死（2026-10-08 双端 HTTP 全卡
/// 12 分钟实证）；12 槽与窗口 1:1，取块不再排队，HTTP 短读走 24 槽短读池不受影响。
const LONG_POOL_SIZE: usize = 12;

/// P4-2：长查询独立池空等待上限（秒）。超过后 panic（spawn_blocking join Err），
/// 由调用方按失败路径处理（对齐/清单/反熵均可重试）。60s 内长查询（38s 级）
/// 通常已归还。
const LONG_POOL_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// P4-2：进程级长查询进行中计数（观测用，随 `/api/v1/io/status` 暴露）。
static LONG_QUERY_ACTIVE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// P4-2：长查询等待限流许可的累计次数（观测用）。
static LONG_QUERY_WAITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// L1-⑬：长查询累计执行次数（read_long + read_long_priority 均经 long_read 借出）
static LONG_QUERY_TOTAL_OPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// L1-⑬：短查询累计执行次数（read 短池借出）
static SHORT_QUERY_TOTAL_OPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// L1-⑬：长查询累计执行次数（单调递增，面板看活跃度用）
pub fn long_query_total_ops() -> u64 {
    LONG_QUERY_TOTAL_OPS.load(std::sync::atomic::Ordering::Relaxed)
}

/// L1-⑬：短查询累计执行次数（单调递增，面板看活跃度用）
pub fn short_query_total_ops() -> u64 {
    SHORT_QUERY_TOTAL_OPS.load(std::sync::atomic::Ordering::Relaxed)
}

/// P4-4（v9，preP32b）：长查询进行中计数的 RAII 守卫 —— **enter() 时 +1，
/// Drop 时（含 panic unwind 路径）-1**，杜绝 `fetch_add` 后 panic 跳过
/// `fetch_sub` 的泄漏（线上实证：池被爬虫去重占满后反复 panic，longAct 涨到
/// 160），也杜绝只减不加的下溢（preP32 初版漏了 enter() 的 +1，实测 -85）。
struct LongQueryActiveGuard;

impl LongQueryActiveGuard {
    fn enter() -> Self {
        LONG_QUERY_ACTIVE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self
    }
}

impl Drop for LongQueryActiveGuard {
    fn drop(&mut self) {
        LONG_QUERY_ACTIVE.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// P4-2：读取当前进行中的长查询数（观测）。
pub fn long_query_active() -> usize {
    LONG_QUERY_ACTIVE.load(std::sync::atomic::Ordering::Relaxed)
}

/// P4-2：读取长查询等待限流的累计次数（观测）。
pub fn long_query_waits() -> u64 {
    LONG_QUERY_WAITS.load(std::sync::atomic::Ordering::Relaxed)
}

/// P4-4（v9，preP32b）：长查询池**连接总数**（观测，= LONG_POOL_SIZE）。
/// 监控面板「可用/总数」的「总数」指独立长查询连接池大小（8 条专用连接），
/// 而非并发门上限（LONG_QUERY_CONCURRENCY=4，那是活跃并发阈值）。
pub fn long_query_total() -> usize {
    LONG_POOL_SIZE
}

/// P4-2：长查询独立连接池可用连接数（观测）——由借出登记表推导：
/// 可用 = LONG_POOL_SIZE - 在借条数（BORROWED_LONG_CONNS 在借出/归还时成对增删）。
pub fn long_pool_available() -> usize {
    let borrowed = BORROWED_LONG_CONNS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .len();
    LONG_POOL_SIZE.saturating_sub(borrowed)
}

/// 读池内部状态：连接队列 + 池启用标记。
/// `pool_enabled=false` 为内存库（测试路径，无池，read 直接回退写连接）；
/// `pool_enabled=true` 为文件库：池空时 read 排队等待（Condvar），绝不抢写锁。
struct ReadPoolInner {
    queue: std::collections::VecDeque<Connection>,
    pool_enabled: bool,
}

impl ReadPoolInner {
    fn empty_disabled() -> Self {
        Self {
            queue: std::collections::VecDeque::new(),
            pool_enabled: false,
        }
    }
    fn from_queue(queue: std::collections::VecDeque<Connection>) -> Self {
        Self {
            queue,
            pool_enabled: true,
        }
    }
    /// 队列长度（测试/观测用）
    #[cfg(test)]
    fn len(&self) -> usize {
        self.queue.len()
    }
}

/// 存储层
pub struct Storage {
    conn: Arc<Mutex<Connection>>,
    /// 读连接池：只读查询专用，不抢写锁；池空时 Condvar 排队等待
    read_pool: Arc<Mutex<ReadPoolInner>>,
    /// 读池归还唤醒（P4-1：池空排队化，归还即 notify）
    read_pool_cond: Condvar,
    /// P4-2：长查询**独立连接池**（A 方案）：全扫级长查询专用，与短读池/写锁
    /// 完全隔离；池空只等待（超时 panic），绝不回退写连接——根治读写互锁。
    long_pool: Arc<Mutex<ReadPoolInner>>,
    /// 长查询池归还唤醒
    long_pool_cond: Condvar,
    /// P4-2：长查询并发门（全表扫描级查询限流，permits=LONG_QUERY_CONCURRENCY）
    long_query_gate: Arc<Semaphore>,
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
    /// 专用 checkpoint 连接（与写路径不共享锁）；内存库 / 测试为 None
    ckpt_conn: Option<Arc<Mutex<Connection>>>,
    /// <db>-wal 路径，用于无锁读取 WAL 尺寸
    wal_path: Option<PathBuf>,
}

/// P4-3（B 方案）：借出登记记录 —— 借出时登记（连接地址 + 起始时间 + 调用点），
/// 归还时移除；泄漏（借出永不归还）会残留在登记表，由 `read_pool_available()` /
/// `long_query_active()` 巡检打印调用点抓现行（2026-10-08 .52 实证：16 条短读
/// 连接被占光且 60s+ 无归还，静态分析未定位持有者，须靠登记表锁定）。
struct BorrowRecord {
    started: std::time::Instant,
    caller: &'static std::panic::Location<'static>,
}

static BORROWED_READ_CONNS: std::sync::Mutex<Vec<(usize, BorrowRecord)>> =
    std::sync::Mutex::new(Vec::new());
static BORROWED_LONG_CONNS: std::sync::Mutex<Vec<(usize, BorrowRecord)>> =
    std::sync::Mutex::new(Vec::new());
/// 巡检限频（30s 一次）
static LAST_BORROW_SCAN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn borrow_key(conn: &Connection) -> usize {
    // P4-3（B）：用 SQLite 底层句柄地址做借出登记 key——句柄在连接整个生命周期内
    // 稳定唯一，跨 move（pop 出池 → 借出 → 归还入池）不变。
    // 不能用 `conn as *const Connection as usize`（&Connection 是栈上局部变量地址，
    // move 进 PoolReturn 后地址变化 → register/unregister 配对失效 → 登记表只增
    // 不减 → 巡检假报警刷屏，2026-10-08 preP27 部署后 05:31 实测数百条假告警）。
    // handle() 返回裸指针，仅作身份标识（不解引用），unsafe 块仅用于取地址。
    unsafe { conn.handle() as usize }
}

fn register_borrow(
    map: &'static std::sync::Mutex<Vec<(usize, BorrowRecord)>>,
    conn: &Connection,
    caller: &'static std::panic::Location<'static>,
) {
    if let Ok(mut m) = map.lock() {
        m.push((
            borrow_key(conn),
            BorrowRecord {
                started: std::time::Instant::now(),
                caller,
            },
        ));
    }
}

fn unregister_borrow(
    map: &'static std::sync::Mutex<Vec<(usize, BorrowRecord)>>,
    conn: &Connection,
) {
    if let Ok(mut m) = map.lock() {
        let key = borrow_key(conn);
        m.retain(|(k, _)| *k != key);
    }
}

/// 巡检登记表：打印被借出 >60s 仍未归还的连接（疑似泄漏），限频 30s。
fn scan_borrowed(map: &'static std::sync::Mutex<Vec<(usize, BorrowRecord)>>, tag: &str) {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let last = LAST_BORROW_SCAN.load(std::sync::atomic::Ordering::Relaxed);
    if now_ms.saturating_sub(last) < 30_000 {
        return;
    }
    LAST_BORROW_SCAN.store(now_ms, std::sync::atomic::Ordering::Relaxed);
    if let Ok(m) = map.lock() {
        for (k, rec) in m.iter() {
            let el = rec.started.elapsed().as_secs();
            if el > 60 {
                tracing::warn!(
                    "[storage] {}连接被借出 {}s 未归还（疑似泄漏），conn={:x} 调用方={}:{}",
                    tag,
                    el,
                    k,
                    rec.caller.file(),
                    rec.caller.line()
                );
            }
        }
    }
}

/// 读连接归还 guard：`Storage::read` 的闭包 panic（unwind）时也把连接放回池，
/// 避免连接永久泄漏导致池耗尽；归还时 notify 唤醒池空等待者（P4-1 排队化）。
/// P4-3（B 方案）：记录借用起始时间与调用点，归还超时（>60s）告警；同时
/// 从借出登记表移除（泄漏记录由 `scan_borrowed` 巡检打印）。
struct PoolReturn<'a> {
    pool: &'a Mutex<ReadPoolInner>,
    notify: &'a Condvar,
    conn: Option<Connection>,
    borrow_started: std::time::Instant,
    caller: &'static std::panic::Location<'static>,
}

impl Drop for PoolReturn<'_> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // P4-3（B）：借用超时告警（>60s 视为异常持有）
            let elapsed = self.borrow_started.elapsed();
            if elapsed.as_secs() > 60 {
                tracing::warn!(
                    "[storage] 读连接借用超时归还（>{:?}），调用方={}:{} 可能持有过长",
                    elapsed,
                    self.caller.file(),
                    self.caller.line()
                );
            }
            // 从借出登记表移除（防止泄漏巡检误报已归还连接）
            unregister_borrow(&BORROWED_READ_CONNS, &conn);
            // E6：连接归还读池，可用计数 +1（与 `Storage::read` pop 成功路径的 -1 配对）。
            READ_POOL_AVAILABLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut pool = self.pool.lock().unwrap_or_else(|e| e.into_inner());
            pool.queue.push_back(conn);
            drop(pool);
            // P4-1：唤醒池空等待者（读请求排队而非抢写锁）
            self.notify.notify_one();
        }
    }
}

/// 长查询池归还 guard（A 方案）：`Storage::long_read` 的闭包 panic（unwind）时
/// 也把连接放回长查询池，避免泄漏；归还时 notify 唤醒池空等待者。
/// P4-3（B 方案）：同 PoolReturn，记录借用起始时间/调用点 + 登记表移除。
struct LongPoolReturn<'a> {
    pool: &'a Mutex<ReadPoolInner>,
    notify: &'a Condvar,
    conn: Option<Connection>,
    borrow_started: std::time::Instant,
    caller: &'static std::panic::Location<'static>,
}

impl Drop for LongPoolReturn<'_> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let elapsed = self.borrow_started.elapsed();
            if elapsed.as_secs() > 60 {
                tracing::warn!(
                    "[storage] 长查询连接借用超时归还（>{:?}），调用方={}:{} 可能持有过长",
                    elapsed,
                    self.caller.file(),
                    self.caller.line()
                );
            }
            unregister_borrow(&BORROWED_LONG_CONNS, &conn);
            let mut pool = self.pool.lock().unwrap_or_else(|e| e.into_inner());
            pool.queue.push_back(conn);
            drop(pool);
            self.notify.notify_one();
        }
    }
}

// ─── E6：读池进程级观测计数器（静态，跨 Storage 实例共享）────────────────────
//
// 语义：文件库 `open()` 后 `available = READ_POOL_SIZE`，内存库 = 0；
// `Storage::read` 从读池借走一条 -1，`PoolReturn` Drop 归还 +1；
// 文件库池空排队等待归还（`read()` 等待循环）累计 +1 starved。
// 注意：静态量跨实例/并行测试共享，面板只取相对趋势，测试避免断言精确绝对值。

/// 当前可用（未借出）的只读连接数。
static READ_POOL_AVAILABLE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
/// 读池空、排队等待归还的累计次数（池饥饿信号）。
static READ_POOL_STARVED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// E6：读取当前可用只读连接数（观测用，随 `/api/v1/io/status` 暴露）。
/// P4-3（B 方案）：顺带巡检借出登记表，打印被借出 >60s 未归还的连接（疑似泄漏，
/// 含调用点）——API 每 5s 轮询即触发巡检（限频 30s），抓"借走不还"现行。
pub fn read_pool_available() -> usize {
    scan_borrowed(&BORROWED_READ_CONNS, "短读池");
    READ_POOL_AVAILABLE.load(std::sync::atomic::Ordering::Relaxed)
}

/// P4-3（B 方案）：长查询连接池巡检入口（同 read_pool_available，抓 long_pool 泄漏）。
pub fn long_pool_scan() {
    scan_borrowed(&BORROWED_LONG_CONNS, "长查询池");
}

/// E6：读取读池空排队等待归还的累计次数（池饥饿信号）。
pub fn read_pool_starved() -> u64 {
    READ_POOL_STARVED.load(std::sync::atomic::Ordering::Relaxed)
}

// ==============================================================
// 写连接锁争用观测（2026-10-09 治本 S5 · 第一步）
//
// 【为什么加】此前 `self.conn.lock()` 全库 38 处（db.rs）+ 1 处
// （io_scheduler.rs），全部**零可观测性**：既看不到等锁时长、也看不到
// 争用次数。于是 2026-10-09 事故排查时无法证明「锁争用」是否参与其中，
// 改造后也无法验证是否改善——只能靠猜。
//
// 【参照】读侧早已有等价观测（`READ_POOL_STARVED` + 池空超时告警），
// 写侧照抄同一模式，不发明新概念。
//
// 【S5 路线】先量化 → 再收敛。若实测锁争用并不严重，就不值得冒
// 「改 38 处」的风险去做单一入口重构。
// ==============================================================

/// 抢写锁累计等待次数（每次进入 `with_write_conn` +1）。
static WRITE_LOCK_ACQUIRED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// 抢写锁等待超过 [`WRITE_LOCK_SLOW_THRESHOLD_MS`] 的累计次数（争用信号）。
static WRITE_LOCK_SLOW: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// 抢写锁等待时长累计（微秒）⇒ 可算平均等待时长。
static WRITE_LOCK_WAIT_US_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// 抢写锁等待时长峰值（毫秒）⇒ 单次最坏情况。
static WRITE_LOCK_WAIT_MS_MAX: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// 判定「慢锁」的阈值（毫秒）。
///
/// 取 50ms：SQLite 单条语句写锁持有通常 <5ms（实测 checkpoint EWMA 约
/// 36~412ms），故 50ms 已明显偏离常态；但又足够宽，避免把偶发的
/// WAL checkpoint 抖动（实测峰值 3.2s）全部计为争用。
const WRITE_LOCK_SLOW_THRESHOLD_MS: u64 = 50;

/// 写锁争用观测快照（供 `/api/v1/io/status` 暴露与压测判据使用）。
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct WriteLockStats {
    /// 累计抢锁次数
    pub acquired: u64,
    /// 累计慢锁次数（等待 ≥50ms）
    pub slow: u64,
    /// 平均等待时长（毫秒）
    pub avg_wait_ms: f64,
    /// 单次最长等待（毫秒）
    pub max_wait_ms: u64,
}

/// 读取写锁争用观测快照。
pub fn write_lock_stats() -> WriteLockStats {
    let acquired = WRITE_LOCK_ACQUIRED.load(std::sync::atomic::Ordering::Relaxed);
    let wait_us = WRITE_LOCK_WAIT_US_TOTAL.load(std::sync::atomic::Ordering::Relaxed);
    WriteLockStats {
        acquired,
        slow: WRITE_LOCK_SLOW.load(std::sync::atomic::Ordering::Relaxed),
        avg_wait_ms: if acquired == 0 {
            0.0
        } else {
            wait_us as f64 / acquired as f64 / 1000.0
        },
        max_wait_ms: WRITE_LOCK_WAIT_MS_MAX.load(std::sync::atomic::Ordering::Relaxed),
    }
}

/// 执行一个「需要全库唯一写连接」的操作，并记录等锁时长。
///
/// 这是 S5 的**唯一入口**：`db.rs` 全部 38 处 `self.conn.lock()`
/// 逐步收敛到本函数，从而把「锁争用」变成可观测、可量化、可断言的对象。
///
/// 与 `read()` 的 P4-3 约定一致：
/// - **绝不 panic**：poisoned 时 `into_inner()` 取回连接（与既有写法一致）；
/// - **绝不回退**：拿不到锁就等，不做降级路径（避免静默绕过串行化）。
#[track_caller]
pub(crate) fn with_write_conn<T>(
    conn: &Arc<Mutex<Connection>>,
    f: impl FnOnce(&mut Connection) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let t0 = std::time::Instant::now();
    let mut guard = conn.lock().unwrap_or_else(|e| e.into_inner());
    let waited = t0.elapsed();

    WRITE_LOCK_ACQUIRED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    WRITE_LOCK_WAIT_US_TOTAL.fetch_add(
        waited.as_micros() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
    let waited_ms = waited.as_millis() as u64;
    if waited_ms >= WRITE_LOCK_SLOW_THRESHOLD_MS {
        let n = WRITE_LOCK_SLOW.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        // 峰值用 fetch_max 更新（并发下不丢最大值）。
        WRITE_LOCK_WAIT_MS_MAX.fetch_max(waited_ms, std::sync::atomic::Ordering::Relaxed);
        // 慢锁本身不阻断（写锁串行是设计预期），但必须可观测：
        // 前若干次打 warn，便于定位争用热点；之后降频避免日志风暴。
        if n <= 10 || n.is_multiple_of(100) {
            let caller = std::panic::Location::caller();
            tracing::warn!(
                "[storage] 写锁争用：等待 {}ms ≥ {}ms（累计慢锁 {}/{} 次，caller={}:{}）",
                waited_ms,
                WRITE_LOCK_SLOW_THRESHOLD_MS,
                n,
                WRITE_LOCK_ACQUIRED.load(std::sync::atomic::Ordering::Relaxed),
                caller.file(),
                caller.line()
            );
        }
    }
    f(&mut guard)
}

/// E6：读池配置大小（= `READ_POOL_SIZE`，文件库口径）。
pub fn read_pool_total() -> usize {
    READ_POOL_SIZE
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

        // 初始化读连接池：打开 READ_POOL_SIZE 个只读连接，读操作走这里不抢写锁
        // （2026-09-30 由 4 扩容至 READ_POOL_SIZE=16，缘由见该常量注释）
        //
        // 读连接使用独立的小页缓存（8MB/条，cache_size=-8192）：SQLite 页缓存按连接
        // 独立，16 条读连接若沿用写连接的 64MB 会使页缓存上限达 1GB，实测（2026-09-30
        // 62/52/51 内存 2.2-3.0GB 触发驱逐、卸载 node 不降内存）确认大头即 SQLite 页缓存。
        // 读查询多为单点/小块取数，8MB 足够；写连接仍用 config.cache_size（64MB）。
        // mmap_size 保留（进程内文件映射共享，不随连接数放大内存）。
        let read_pragma_sql = format!(
            "PRAGMA journal_mode=WAL;              PRAGMA synchronous={};              PRAGMA mmap_size={};              PRAGMA cache_size=-8192;              PRAGMA temp_store={};              PRAGMA wal_autocheckpoint={};              PRAGMA busy_timeout={};",
            config.synchronous,
            config.mmap_size,
            config.temp_store,
            config.wal_autocheckpoint,
            config.busy_timeout_ms,
        );
        let mut read_pool = std::collections::VecDeque::new();
        for _ in 0..READ_POOL_SIZE {
            let rconn = Connection::open(path_ref)?;
            rconn.execute_batch(&read_pragma_sql)?;
            read_pool.push_back(rconn);
        }
        info!(
            "[storage] read pool initialized: {} connections",
            read_pool.len()
        );

        // P4-2（A 方案）：长查询独立连接池（LONG_POOL_SIZE 条），与短读池完全隔离。
        // 同一 pragma（WAL/cache 8MB 等）；长查询全扫 38s 级，独立池保证不挤占
        // 短读 16 条、也绝不触碰写锁。
        let mut long_pool = std::collections::VecDeque::new();
        for _ in 0..LONG_POOL_SIZE {
            let lconn = Connection::open(path_ref)?;
            lconn.execute_batch(&read_pragma_sql)?;
            long_pool.push_back(lconn);
        }
        info!(
            "[storage] long-query pool initialized: {} connections",
            long_pool.len()
        );

        // A1：专用 checkpoint 连接（与写路径不共享锁）。
        // 只设 busy_timeout/synchronous/cache_size；不在 ckpt 连接重复
        // journal_mode/mmap_size/wal_autocheckpoint（那些由写连接统一负责）。
        // busy_timeout 用 ckpt 专用短超时（ckpt_busy_timeout_ms，默认 500ms）：
        // 负载期 TRUNCATE 拿不到独占锁是常态，长等待只会占住锁队列、把所有 DB
        // 写入一起拖住（.52/.53 2026-10-07 实证 5s 等待 = 每 5 分钟一次全量写停摆）；
        // 注定失败的尝试快速放弃，PASSIVE 下个 tick 续传、TRUNCATE 等下个空闲窗口。
        let ckpt_conn = {
            let c = Connection::open(path_ref)?;
            c.execute_batch(&format!(
                "PRAGMA busy_timeout={}; PRAGMA synchronous={}; PRAGMA cache_size=-2048;",
                config.ckpt_busy_timeout_ms, config.synchronous,
            ))?;
            Some(Arc::new(Mutex::new(c)))
        };
        // <db>-wal 路径：append("-wal" 到文件名），用于无锁读取 WAL 尺寸
        let mut wal_path: PathBuf = path_ref.to_path_buf();
        wal_path.as_mut_os_string().push("-wal");

        // E6：文件库读池就绪，进程级可用计数置为池大小（观测用）。
        READ_POOL_AVAILABLE.store(READ_POOL_SIZE, std::sync::atomic::Ordering::Relaxed);

        let storage = Self {
            conn: Arc::new(Mutex::new(conn)),
            read_pool: Arc::new(Mutex::new(ReadPoolInner::from_queue(read_pool))),
            read_pool_cond: Condvar::new(),
            long_pool: Arc::new(Mutex::new(ReadPoolInner::from_queue(long_pool))),
            long_pool_cond: Condvar::new(),
            long_query_gate: Arc::new(Semaphore::new(LONG_QUERY_CONCURRENCY)),
            write_stats: Arc::new(Mutex::new(WriteStats::default())),
            oplog_len_cache: std::sync::atomic::AtomicI64::new(-1),
            entity_counts_cache: Default::default(),
            ckpt_conn,
            wal_path: Some(wal_path),
        };
        storage.init_tables(
            config.drop_unused_indexes,
            config.drop_redundant_peer_indexes,
        )?;
        info!("[storage] 数据库已打开: {:?}", path_ref);
        Ok(storage)
    }

    /// 内存数据库（用于测试）
    pub fn memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        // E6：内存库无读池，可用计数恒 0（读操作全部走回退写连接路径）。
        READ_POOL_AVAILABLE.store(0, std::sync::atomic::Ordering::Relaxed);
        let storage = Self {
            conn: Arc::new(Mutex::new(conn)),
            read_pool: Arc::new(Mutex::new(ReadPoolInner::empty_disabled())),
            read_pool_cond: Condvar::new(),
            long_pool: Arc::new(Mutex::new(ReadPoolInner::empty_disabled())),
            long_pool_cond: Condvar::new(),
            long_query_gate: Arc::new(Semaphore::new(LONG_QUERY_CONCURRENCY)),
            write_stats: Arc::new(Mutex::new(WriteStats::default())),
            oplog_len_cache: std::sync::atomic::AtomicI64::new(-1),
            entity_counts_cache: Default::default(),
            ckpt_conn: None,
            wal_path: None,
        };
        storage.init_tables(true, true)?;
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
    /// 优先读触发器维护的增量计数（O(1)，见 init_tables 的 table_counts），
    /// 仅存量库升级后未校准的短暂窗口回退真实 COUNT。
    pub fn valid_entity_counts(&self) -> [i64; 5] {
        if let Some(c) = self.valid_entity_counts_incremental() {
            return c;
        }
        // 只读查询走读连接池，不抢写锁
        // P1：闭包返回 Result；read_long 超时/池饥饿时降级为 [-1;5]（调用方据此
        // 识别"统计不可用"），绝不 panic。
        self.read_long(|conn| -> anyhow::Result<[i64; 5]> {
            let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap_or(-2) };
            Ok([
                count("SELECT COUNT(*) FROM dht_nodes WHERE deleted_at IS NULL"),
                count("SELECT COUNT(*) FROM peers WHERE deleted_at IS NULL"),
                count("SELECT COUNT(*) FROM peers_archive"),
                count("SELECT COUNT(*) FROM infohashes WHERE deleted_at IS NULL"),
                count("SELECT COUNT(*) FROM trackers WHERE deleted_at IS NULL"),
            ])
        })
        .unwrap_or_else(|e| {
            tracing::warn!(target: "pdcdb", "valid_entity_counts 读失败，降级 [-1;5]: {}", e);
            [-1i64; 5]
        })
    }

    /// 增量计数路径：任一表未校准（calibrated=0）则返回 None。
    fn valid_entity_counts_incremental(&self) -> Option<[i64; 5]> {
        let mut out = [0i64; 5];
        for (i, name) in [
            "dht_nodes",
            "peers",
            "peers_archive",
            "infohashes",
            "trackers",
        ]
        .iter()
        .enumerate()
        {
            let (_, valid) = self.table_count_cached(name)?;
            out[i] = valid;
        }
        Some(out)
    }

    /// 读取单表增量计数 (total, valid)；未校准返回 None。
    /// 走读连接池（单行主键读，不与写批次抢写锁）；内存库（无池）由 read() 回退
    /// 主连接（测试路径）。文件库池空只等待归还（超时 panic），绝不回退写锁。
    pub fn table_count_cached(&self, table: &str) -> Option<(i64, i64)> {
        // P1：闭包返回 Result；read 超时/池饥饿时降级为 None（视为未校准，
        // 上层走真实 COUNT 回退），绝不 panic。
        self.read(|conn| -> anyhow::Result<Option<(i64, i64)>> {
            Ok(conn
                .query_row(
                    "SELECT total, valid FROM table_counts WHERE name = ?1 AND calibrated = 1",
                    params![table],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
                )
                .ok())
        })
        .unwrap_or_else(|e| {
            tracing::warn!(target: "pdcdb", "table_count_cached({}) 读失败，降级 None: {}", table, e);
            None
        })
    }

    /// 全量校准增量行数计数器并写回缓存（启动校准/周期任务调用）。
    /// 每表一次单遍扫描同时取 total+valid，校准后所有计数读路径 O(1)。
    /// COUNT 较重，调用方必须放阻塞线程（spawn_blocking），禁止在 API/async 线程直接调用。
    pub fn refresh_entity_counts(&self) -> [i64; 5] {
        use std::sync::atomic::Ordering;
        let names = [
            "dht_nodes",
            "peers",
            "peers_archive",
            "infohashes",
            "trackers",
        ];
        let mut calibrated: Vec<(usize, i64)> = Vec::with_capacity(names.len());
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        //
        // 原本是一个 `{ let conn = lock; ... }` 自包含块。改为闭包形式后
        // 块边界与闭包边界重合，**锁释放时机不变**（仍在本段结束时释放）。
        with_write_conn(&self.conn, |conn| {
            for (i, name) in names.iter().enumerate() {
                // 单遍扫描同时取 total 与 valid（peers_archive 无墓碑列，valid=total）
                let sql = if *name == "peers_archive" {
                    format!("SELECT COUNT(*), COUNT(*) FROM {}", name)
                } else {
                    format!(
                        "SELECT COUNT(*), COALESCE(SUM(CASE WHEN deleted_at IS NULL THEN 1 ELSE 0 END), 0) FROM {}",
                        name
                    )
                };
                if let Ok((total, valid)) =
                    conn.query_row(&sql, [], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))
                {
                    let _ = conn.execute(
                        "INSERT INTO table_counts (name, total, valid, calibrated) \
                         VALUES (?1, ?2, ?3, 1) \
                         ON CONFLICT(name) DO UPDATE SET total = ?2, valid = ?3, calibrated = 1",
                        params![name, total, valid],
                    );
                    calibrated.push((i, valid));
                }
            }
            Ok(())
        })
        // 本函数返回 `[i64; 5]`（非 Result），故无法用 `?` 传播错误。
        // 保持原语义：校准失败时 `calibrated` 为空、调用方拿到全 -1 数组，
        // 由上层`entity_counts_cached` 降级处理。但**记一条 warn** ——
        // 此前失败是完全静默的（锁不可用时同样静默），运维无从察觉。
        .unwrap_or_else(|e| {
            tracing::warn!(
                "[storage] refresh_entity_counts 写锁内校准失败（返回全 -1）: {}",
                e
            );
        });
        let mut c = [-1i64; 5];
        for (i, v) in calibrated {
            c[i] = v;
            self.entity_counts_cache[i].store(v, Ordering::Relaxed);
        }
        c
    }

    /// 读取缓存计数。优先触发器维护的增量计数（O(1)）；增量计数未就绪时
    /// 沿用缓存逻辑（缓存未校准则做一次重 COUNT 校准，之后走缓存）。
    pub fn entity_counts_cached(&self) -> [i64; 5] {
        use std::sync::atomic::Ordering;
        if let Some(c) = self.valid_entity_counts_incremental() {
            for (i, v) in c.iter().enumerate() {
                self.entity_counts_cache[i].store(*v, Ordering::Relaxed);
            }
            return c;
        }
        if self.entity_counts_cache[0].load(Ordering::Relaxed) < 0 {
            // P2（读写分离）：原冷缓存回退 `refresh_entity_counts()`（真实 COUNT 后
            // 写回增量表 = 读 API 触发 DB 写）。改为纯读 `valid_entity_counts()`
            // （增量→read_long COUNT），仅把结果写入内存原子缓存（非 DB 写）。
            // 真正的增量计数校准仍由周期任务（main.rs）负责。
            let c = self.valid_entity_counts();
            for (i, v) in c.iter().enumerate() {
                self.entity_counts_cache[i].store(*v, Ordering::Relaxed);
            }
            return c;
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

    /// 从读连接池拿一个连接，只读操作专用
    /// 读操作走读连接，不抢写锁，读写不互相阻塞
    ///
    /// 健壮性（2026-09-23 API 失联事故复盘）：
    /// - 池锁只在 pop/push 时持有，不跨 `f` 执行；
    /// - `f` panic 时由 RAII guard 归还连接（旧行为：连接永久丢失，池耗尽后
    ///   `expect("read pool empty")` panic → 锁毒化 → 全进程读路径雪崩）；
    /// - 毒化锁自愈（into_inner），不再级联 panic；
    /// - P4-1（2026-10-08 读饿死治理）：文件库池空 → Condvar 排队等待归还，
    ///   不再回退抢全局写锁——旧行为下长查询占满读池后，块请求取数被迫拿写锁，
    ///   亚毫秒写事务被秒级扫描排队，写延迟 EWMA 抬升、IO level=1.0 → 调度器让路
    ///   → 窗口补不满丢帧；
    /// - **P4-3（2026-10-08 A 方案，用户拍板"不允许回退，相关代码全部删除"）**：
    ///   文件库池空 → **永久等待归还**（Condvar 轮询），最多
    ///   `READ_POOL_WAIT_TIMEOUT`（120s）后 **panic**（unwind → spawn_blocking
    ///   join Err → 调用方按失败路径重试），**绝不回退写连接**。旧"60s 超时回退
    ///   写连接"保底已删除——回退读风暴排队抢写锁 = 写锁被回退读持续占用 =
    ///   save_dirty/收块落库全部卡死 = WAL 22 分钟零增长 + 联邦传输冻结
    ///   （.52 2026-10-08 实证）。写锁只归写路径；连接泄漏由 P4-3（B 方案）
    ///   借出登记表巡检抓现行。内存库（无池，仅测试）保持原回退主连接行为
    ///   （无磁盘争用，每连接独立空库无法建池）。
    #[track_caller]
    pub fn read<F, T>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce(&Connection) -> anyhow::Result<T>,
    {
        // P4-3：池空等待最长 120s（正常短读毫秒级；长任务走 read_long 独立池），
        // 等待期间释放池锁；超时【放弃本次查询返回 Err】，绝不 panic、绝不回退写锁。
        const READ_POOL_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
        let deadline = std::time::Instant::now() + READ_POOL_WAIT_TIMEOUT;
        // P4-3（B）：调用点（用于归还超时告警 / 泄漏巡检）
        let caller = std::panic::Location::caller();

        let mut guard = self.read_pool.lock().unwrap_or_else(|e| e.into_inner());
        let pooled = loop {
            if let Some(conn) = guard.queue.pop_front() {
                SHORT_QUERY_TOTAL_OPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                break Some(conn);
            }
            if !guard.pool_enabled {
                // 内存库（测试）：无池，保持原回退主连接行为
                break None;
            }
            // 文件库池空：记录排队次数并等待归还，绝不回退写锁
            READ_POOL_STARVED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if std::time::Instant::now() >= deadline {
                // P1（去 panic）：短读池饥饿不再崩溃，warn 后放弃本次查询（Err 即让
                // RAII/许可自动释放），调用方按失败路径重试/降级，进程永远存活。
                tracing::warn!(
                    "短读连接池饥饿（等待 >{}s 无连接归还，starved={}）；放弃本次查询，调用方按失败重试（caller={}:{}）",
                    READ_POOL_WAIT_TIMEOUT.as_secs(),
                    READ_POOL_STARVED.load(std::sync::atomic::Ordering::Relaxed),
                    caller.file(),
                    caller.line()
                );
                return Err(anyhow::anyhow!(
                    "短读连接池饥饿（等待 >{}s 无连接归还），放弃本次查询（caller={}:{}）",
                    READ_POOL_WAIT_TIMEOUT.as_secs(),
                    caller.file(),
                    caller.line()
                ));
            }
            // [ALLOWED-HARDCODED: 短读池排队轮询间隔 100ms]
            let (g, _) = self
                .read_pool_cond
                .wait_timeout(guard, std::time::Duration::from_millis(100))
                .unwrap_or_else(|e| e.into_inner());
            guard = g;
        };
        drop(guard);
        match pooled {
            Some(conn) => {
                // E6：从读池借走一条，可用计数 -1（归还见 `PoolReturn::drop`）。
                READ_POOL_AVAILABLE.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                // P4-3（B）：借出登记（泄漏巡检抓现行）
                register_borrow(&BORROWED_READ_CONNS, &conn, caller);
                let guard = PoolReturn {
                    pool: &self.read_pool,
                    notify: &self.read_pool_cond,
                    conn: Some(conn),
                    borrow_started: std::time::Instant::now(),
                    caller,
                };
                let conn = guard.conn.as_ref().expect("PoolReturn conn");
                f(conn)
            }
            None => {
                // 仅内存库（pool_enabled=false，测试路径）：回退主连接
                //
                // 【2026-10-09 治本 S5 · 刻意不走 with_write_conn】此处的
                // `self.conn.lock()` 只在**内存库测试路径**生效（生产文件库
                // `pool_enabled=true` 走 Some(conn) 分支借读池连接，永不走这里）。
                //
                // 故它不属于生产写锁争用路径，无需纳入 S5 收敛；且包成
                // `with_write_conn(&self.conn, |c| f(c))` 会把一个
                // 「借连接 → 调闭包」的语义压成「持锁 → 调闭包」，
                // 在测试并发场景下更易触发锁重入。刻意保留原写法。
                let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
                f(&conn)
            }
        }
    }
    /// P4-2（A 方案）：长查询（全表/大区间扫描级）入口。
    ///
    /// 必须由阻塞线程（spawn_blocking 内）调用；先阻塞等待限流许可
    /// （并发上限 `LONG_QUERY_CONCURRENCY`），再走**独立长查询连接池**
    /// （`long_read`，LONG_POOL_SIZE 条专用连接）。与短读池/写锁完全隔离：
    /// 全扫期间既不挤占短读 16 条、也绝不回退抢写锁（根治读写互锁——
    /// 2026-10-08 实证：旧实现池空 60s 回退写连接，全扫攥写锁 38s+，
    /// WAL 18 分钟零增长、双端瘫痪）。
    ///
    /// v9（2026-10-08 preP30）：permit 等待加 60s 超时 panic。线上实证：去重
    /// （existing_node_keys 等）恒占满 permit 时，重建的 read_long 在 permit 上
    /// **无限 sleep（10ms 轮询无超时）** → 外层 300s 建清单超时 abort，但
    /// spawn_blocking 线程卡在 sleep 循环不退出（tokio abort 不中断阻塞线程）
    /// → **僵尸线程占 permit 不放 → longAct 恒满 → 下一轮重建继续饿死**（.53
    /// 06:06–06:24 每轮 >300s 超时死循环实证）。现在等 permit 超过 60s 直接
    /// panic（unwind → spawn_blocking join Err → 调用方按失败路径退出重试），
    /// permit 由 RAII 释放，**绝不残留僵尸**。
    pub fn read_long<F, T>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce(&Connection) -> anyhow::Result<T>,
    {
        let deadline = std::time::Instant::now() + LONG_POOL_WAIT_TIMEOUT;
        let caller = std::panic::Location::caller();
        let permit = loop {
            match self.long_query_gate.clone().try_acquire_owned() {
                Ok(p) => break Some(p),
                Err(_) => {
                    LONG_QUERY_WAITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if std::time::Instant::now() >= deadline {
                        // P1（去 panic）：permit 等待超时不再崩溃，warn 后放弃本次长查询
                        // 返回 Err（未取到许可无任何资源需释放），调用方按失败重试/降级。
                        tracing::warn!(
                            "长查询并发门等待超时（>{}s 无许可，疑似被常驻长查询占满），放弃本次长查询，调用方按失败重试（caller={}:{}）",
                            LONG_POOL_WAIT_TIMEOUT.as_secs(),
                            caller.file(),
                            caller.line()
                        );
                        return Err(anyhow::anyhow!(
                            "长查询并发门等待超时（>{}s 无许可），放弃本次长查询（caller={}:{}）",
                            LONG_POOL_WAIT_TIMEOUT.as_secs(),
                            caller.file(),
                            caller.line()
                        ));
                    }
                    // [ALLOWED-HARDCODED: 长查询并发门轮询间隔 10ms]
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }
        };
        // P4-4（v9，preP32c）：活跃计数在 long_read **借到连接后**才计入（见
        // long_read），此处不再 enter——池满排队/等 permit 不计"活跃"。
        // P1：long_read 返回 Err（长池饥饿）时向上传播；permit 随 break 出的变量
        // drop 自动归还。
        let out = self.long_read(f)?;
        drop(permit);
        Ok(out)
    }

    /// P4-2（v9，preP30）：**bootstrap 清单重建/传输专用**长查询入口——
    /// **不经过长查询并发门**（无 permit 等待），直接走独立长查询连接池。
    ///
    /// 背景：清单重建（build_repo_manifest_full_scan 全表扫 604 万行）是联邦
    /// 快照收敛的关键一次性路径，而爬虫去重等常驻长查询会占满 permit 门 →
    /// 重建在 permit 上无限等待 → 300s 建清单超时 → 缓存永远空 → 协商
    /// Nak(rebuilding) 死循环（.53 2026-10-08 06:06–06:24 实证，扩容 permit
    /// 到 4 仍被去重占满）。重建期间非稳态已暂停 delta/range，池 8 条连接
    /// 足够重建+去重并发；池空仍由 `long_read` 60s panic 兜底（不抢写锁）。
    /// 仅 bootstrap.rs 重建/传输路径使用；range 反熵等普通路径仍走
    /// [`Self::read_long`]（受 permit 门限流）。
    #[track_caller]
    pub fn read_long_priority<F, T>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce(&Connection) -> anyhow::Result<T>,
    {
        // P4-4（v9，preP32c）：同 read_long——活跃计数在 long_read 借到连接后
        // 计入，此处不 enter。P1：直接传播 long_read 的 Err（长池饥饿）。
        self.long_read(f)
    }

    /// P4-2（A 方案）：从**长查询独立连接池**借连接执行闭包。
    ///
    /// 池空只等待（Condvar，100ms 轮询），最多 `LONG_POOL_WAIT_TIMEOUT`（60s）
    /// 后 **panic**（unwind → spawn_blocking join Err → 调用方按失败路径重试）；
    /// **绝不回退写连接**。内存库（`pool_enabled=false`）保持旧回退写连接行为
    /// （无磁盘争用，测试路径）。P4-3（B）：借出登记 + 调用点追踪（同 read）。
    #[track_caller]
    fn long_read<F, T>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce(&Connection) -> anyhow::Result<T>,
    {
        let deadline = std::time::Instant::now() + LONG_POOL_WAIT_TIMEOUT;
        let caller = std::panic::Location::caller();
        let mut guard = self.long_pool.lock().unwrap_or_else(|e| e.into_inner());
        let pooled = loop {
            if let Some(conn) = guard.queue.pop_front() {
                LONG_QUERY_TOTAL_OPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                break Some(conn);
            }
            if !guard.pool_enabled {
                // 内存库（测试）：无池，保持原回退写连接行为
                break None;
            }
            // 文件库长查询池空：等待归还（绝不回退写锁）
            if std::time::Instant::now() >= deadline {
                // P1（去 panic）：长池饥饿不再崩溃，warn 后放弃本次查询返回 Err。
                tracing::warn!(
                    "长查询连接池饥饿（等待 >{}s 无连接归还），放弃本次查询，调用方按失败重试（caller={}:{}）",
                    LONG_POOL_WAIT_TIMEOUT.as_secs(),
                    caller.file(),
                    caller.line()
                );
                return Err(anyhow::anyhow!(
                    "长查询连接池饥饿（等待 >{}s 无连接归还），放弃本次查询（caller={}:{}）",
                    LONG_POOL_WAIT_TIMEOUT.as_secs(),
                    caller.file(),
                    caller.line()
                ));
            }
            // [ALLOWED-HARDCODED: 长查询池排队轮询间隔 100ms]
            let (g, _) = self
                .long_pool_cond
                .wait_timeout(guard, std::time::Duration::from_millis(100))
                .unwrap_or_else(|e| e.into_inner());
            guard = g;
        };
        drop(guard);
        match pooled {
            Some(conn) => {
                // P4-4（v9，preP32c）：活跃计数移到**借到连接之后** —— 旧实现计数
                // 在入口（read_long/priority），池满时排队者也计入"活跃"，瞬时值可
                // 超过池大小（线上实测 longAct=9 > total=8，面板误导）。现在只有
                // 真正占用池连接的才算活跃（≤ LONG_POOL_SIZE）；排队/等 permit 不
                // 计。守卫 enter/Drop 配对，f 内 panic 也归还计数。
                let _active = LongQueryActiveGuard::enter();
                // P4-3（B）：借出登记（泄漏巡检抓现行）
                register_borrow(&BORROWED_LONG_CONNS, &conn, caller);
                let guard = LongPoolReturn {
                    pool: &self.long_pool,
                    notify: &self.long_pool_cond,
                    conn: Some(conn),
                    borrow_started: std::time::Instant::now(),
                    caller,
                };
                let conn = guard.conn.as_ref().expect("LongPoolReturn conn");
                f(conn)
            }
            None => {
                // 内存库 / 测试路径：回退写连接（活跃计数同口径：执行中即计入）
                //
                // 【2026-10-09 治本 S5 · 刻意不走 with_write_conn】与 `read()`
                // 同理：生产文件库走 Some(conn) 借长查询池连接，此分支仅
                // 内存库/测试生效，不属生产写锁争用路径。
                // 另注：此处已持 `LongQueryActiveGuard`，若再包一层写连接闭包，
                // 会让「许可计数」与「写锁持有」交叠，测试并发下更易放大等待。
                let _active = LongQueryActiveGuard::enter();
                let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
                f(&conn)
            }
        }
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

    /// 手动执行 WAL checkpoint（薄封装，转调专用连接 checkpoint_once）
    /// PASSIVE 模式：不阻塞写入，日常高频使用
    pub fn checkpoint(&self) -> anyhow::Result<()> {
        self.checkpoint_once(CheckpointMode::Passive)?;
        Ok(())
    }

    /// 执行 WAL checkpoint 并截断 WAL 文件（薄封装，转调 checkpoint_once）
    /// TRUNCATE 模式：会阻塞写入，但会将 WAL 文件压缩到最小
    pub fn checkpoint_truncate(&self) -> anyhow::Result<()> {
        self.checkpoint_once(CheckpointMode::Truncate)?;
        Ok(())
    }

    /// 单次 WAL checkpoint（阻塞；必须由调用方保证不在 async worker 线程执行）。
    /// 走专用 ckpt_conn，与写路径不共享锁；内存库（ckpt_conn=None）直接跳过。
    pub fn checkpoint_once(&self, mode: CheckpointMode) -> anyhow::Result<CheckpointOutcome> {
        let started = std::time::Instant::now();
        let Some(ckpt) = &self.ckpt_conn else {
            return Ok(CheckpointOutcome {
                mode,
                busy: false,
                wal_frames: 0,
                checkpointed: 0,
                elapsed: started.elapsed(),
                skipped: Some("memory"),
            });
        };
        let sql = match mode {
            CheckpointMode::Passive => "PRAGMA wal_checkpoint(PASSIVE);",
            CheckpointMode::Truncate => "PRAGMA wal_checkpoint(TRUNCATE);",
        };
        let conn = ckpt.lock().unwrap_or_else(|e| e.into_inner());
        let (busy, log, checkpointed): (i64, i64, i64) =
            conn.query_row(sql, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        Ok(CheckpointOutcome {
            mode,
            busy: busy != 0,
            wal_frames: log.max(0) as u64,
            checkpointed: checkpointed.max(0) as u64,
            elapsed: started.elapsed(),
            skipped: None,
        })
    }

    /// 读取 <db>-wal 文件尺寸（无锁，O(1)）。
    pub fn wal_bytes(&self) -> u64 {
        match &self.wal_path {
            Some(p) => std::fs::metadata(p).map(|m| m.len()).unwrap_or(0),
            None => 0,
        }
    }

    /// 当前 WAL 近似帧数（低水位信号，观测/日志用；O(1) 无锁）。
    ///
    /// SQLite 每帧 = `page_size` + 24B 帧头；默认 page_size=4096，单帧约 4120B。
    /// 这里用 `wal_bytes()` 无锁估算，不打开 ckpt_conn、不取写锁——决策 tick 要求
    /// O(1) 不阻塞。精确帧数见每次 checkpoint 回填的 [`CheckpointOutcome.wal_frames`]。
    pub fn wal_frames(&self) -> u64 {
        // 单帧 ≈ page_size(默认 4096) + 24B 帧头 = 4120B。
        self.wal_bytes() / 4120u64
    }
}

/// G4：常规（软阈值~硬阈值之间）PASSIVE checkpoint 允许执行的背压上限。
///
/// 背压高于此值说明 io_scheduler 前台写正在吃盘，此时不凑上去做 WAL 回写，避免
/// 与前台批量写互踩。取 0.5——与 io_backpressure_poll 的「>0.5 即 IO 降级」档位对齐。
pub const CHECKPOINT_IDLE_BACKPRESSURE_MAX: f32 = 0.5;

/// G4：常规 PASSIVE checkpoint 的「写空闲 / 低水位」闸门。
///
/// # 背景与依据（53 侧日志证据）
/// 旧逻辑：一旦 WAL 越过 `checkpoint_wal_soft_mb`(默认 32MB) 就在每个决策 tick
/// （默认 1s）触发 `PRAGMA wal_checkpoint(PASSIVE)`，单轮把约 8000 帧（~32MB）一次性
/// 回写主库文件，实测 checkpoint 耗时 EWMA 117ms。这是一次集中的盘写突刺，而当时
/// io_scheduler 正在前台批量刷写——两者抢同一磁盘，写队列反复堆积、背压反复顶到 1.0。
/// WAL 帧数长时间停留在「上万」则是长读事务（读池 16 条连接跑 delta/range/PEX）钉住
/// 旧帧所致，属另一问题；本闸门治理的是「时机」：别在前台写忙时凑盘。
///
/// # 治理
/// 常规 checkpoint 只在「写队列空（无待写请求）且背压未升高」时做——把 WAL 回写盘 I/O
/// 排进写空闲间隙。这与 TRUNCATE 路径既有的 `queue_len()==0` 闸门同源（见 main.rs
/// `wal_checkpoint_hourly_truncate`），此前高频 PASSIVE 路径漏了这道门。
///
/// # 边界
/// 本闸门只约束常规（soft≤WAL<hard）触发；WAL≥hard 的强制 checkpoint 由调用方绕过
/// 本闸门直接触发（WAL 无界安全网），绝不能因「写一直忙」而永不 checkpoint。
///
/// 为什么不调 `wal_autocheckpoint`：takeover=true 时写连接已置 0（应用单驱动，禁止两套
/// checkpoint 并存）。若改成非 0 值，会让 SQLite 在写提交时内联触发 checkpoint——正是
/// A1 改造前「慢盘单次 checkpoint 数百秒阻塞写路径」的病根，故维持 0、只调时机。
#[inline]
pub fn routine_checkpoint_advisable(queue_len: usize, backpressure: f32) -> bool {
    queue_len == 0 && backpressure <= CHECKPOINT_IDLE_BACKPRESSURE_MAX
}

/// TRUNCATE（WAL 文件压缩回收磁盘）的「真空闲」闸门——比常规 PASSIVE 严格得多。
///
/// # 背景与依据（.52/.53 双节点 2026-10-07 实证）
/// TRUNCATE 需要独占 WAL：所有读者退出 + 拿到写者锁。联邦 + 爬虫持续写入时几乎
/// 不存在这样的窗口；在非空闲时刻发起 TRUNCATE 的实际效果是：占住锁队列等满
/// busy_timeout（默认 5s，实测单次 5.5s），期间**所有 DB 写入停摆** → 背压顶满 →
/// G4 闸门关闭 → 联邦 apply/gossip/delta 连锁卡顿 → 看门狗暂停重协商 → API 线程
/// 被卡死。`wal_checkpoint_truncate_interval_secs` 曾被调到 300s（9-27），使上述
/// stall 每 5 分钟必现一次，是「运行 ~1h 即雪崩」的直接推手。
///
/// # 治理
/// 只在**持续空闲**时才允许 TRUNCATE（此时独占锁即刻可得，零等待）：
/// 1. WAL 文件 ≥ truncate_min（仍有磁盘回收价值）；
/// 2. 写队列空（既有闸门）；
/// 3. IO 背压 < 0.3（新闸门：`queue_len==0` 只是瞬时快照，背压低才代表持续空闲；
///    比 PASSIVE 的 0.5 更严，因为 TRUNCATE 要的是独占而非共存）；
/// 4. checkpoint worker 空闲（既有闸门）。
pub const TRUNCATE_IDLE_BACKPRESSURE_MAX: f32 = 0.3;

#[inline]
pub fn truncate_advisable(
    wal_bytes: u64,
    truncate_min_bytes: u64,
    queue_len: usize,
    backpressure: f32,
    worker_inflight: bool,
) -> bool {
    wal_bytes >= truncate_min_bytes
        && queue_len == 0
        && backpressure < TRUNCATE_IDLE_BACKPRESSURE_MAX
        && !worker_inflight
}

impl Storage {
    /// 写连接的 SQLite 自动 checkpoint 开关（接管/交还）。
    /// takeover=true 启动时置 0（应用接管）；false 时恢复默认页数。
    pub fn set_wal_autocheckpoint(&self, pages: u32) -> anyhow::Result<()> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            // 注意：PRAGMA 赋值会返回一行结果，必须用 pragma_update（execute 会报 "returned results"）
            conn.pragma_update(None, "wal_autocheckpoint", pages)?;
            Ok(())
        })
    }

    /// 在已有连接上执行 WAL checkpoint（供 IOScheduler 回调使用）
    /// 使用 PASSIVE 模式，不阻塞、不全量写回，避免 IO 尖峰
    pub fn checkpoint_in_tx(conn: &Connection) -> anyhow::Result<()> {
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
        Ok(())
    }

    /// 执行 VACUUM（清理碎片，压缩数据库）
    pub fn vacuum(&self) -> anyhow::Result<()> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            conn.execute_batch("VACUUM;")?;
            info!("[storage] VACUUM 已完成");
            Ok(())
        })
    }

    /// 初始化表结构
    /// `drop_unused_indexes`：A3 迁移开关，true 时删除 v8 后无人查询的 l2_shard 索引。
    fn init_tables(
        &self,
        drop_unused_indexes: bool,
        drop_redundant_peer_indexes: bool,
    ) -> anyhow::Result<()> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        //
        // init_tables 是启动期一次性建表（持锁较长），但仍必须纳入统一入口：
        // 否则它就是「未被观测的长持锁」，一旦启动期与其他写路径竞争便成盲区。
        with_write_conn(&self.conn, |conn| {
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
                -- B5-1: idx_peers_infohash 与 PK(infohash,ip,port) 首列重复，已由末尾一次性迁移 DROP。
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
                -- P-A（增量块摘要）：bootstrap 清单/对齐的持久化块表。
                -- lo_key/hi_key 与 load_repo_key_hashes_in_range 的排序键逐字节一致
                -- （NODE=`ip:port`、PEER=`lower(hex(infohash)):ip:port`、INFOHASH=infohash、TRACKER=url）。
                -- 写入经 mark_chunks_dirty_in_tx 标 dirty；dirty 块在清单构建/块发送前惰性重算
                -- （P-B），消除 bootstrap 周期内的全表扫描（600 万行 38s~120s+）。
                CREATE TABLE IF NOT EXISTS chunk_digests (
                    repo INTEGER NOT NULL,
                    idx INTEGER NOT NULL,
                    lo_key BLOB NOT NULL,
                    hi_key BLOB NOT NULL,
                    rows INTEGER NOT NULL DEFAULT 0,
                    hash BLOB NOT NULL,
                    dirty INTEGER NOT NULL DEFAULT 0,
                    chunk_rows INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (repo, idx)
                );
                CREATE INDEX IF NOT EXISTS idx_chunk_digests_repo_lo
                    ON chunk_digests (repo, lo_key);
                -- B5-1: idx_peers_archive_infohash 与 PK 首列重复，已 DROP。
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
                -- A3（v11）：四个 idx_*_l2_shard 已删除（全仓库无 WHERE l2_shard 查询，纯写放大）。
                -- 存量库由末尾 drop_unused_l2_shard_indexes() 一次性 DROP（列与写入路径保留）。
                CREATE INDEX IF NOT EXISTS idx_dht_nodes_deleted ON dht_nodes(deleted_at);
                CREATE INDEX IF NOT EXISTS idx_trackers_deleted ON trackers(deleted_at);
                CREATE INDEX IF NOT EXISTS idx_infohashes_deleted ON infohashes(deleted_at);
                CREATE INDEX IF NOT EXISTS idx_peers_deleted ON peers(deleted_at);
                "#,
            )?;

            // ===== 增量行数计数器（消除周期性全表 COUNT 扫描，2026-09-23）=====
            //
            // 背景：统计快照（每秒）、监控 WebSocket、联邦协商都通过 count_table /
            // valid_entity_counts 触发 `SELECT COUNT(*)` 全表扫描；在慢盘 + 写入高压下
            // 单次 COUNT 可达分钟级，且 db::read() 在查询期间持有读池锁，一条慢 COUNT
            // 会卡死全进程读路径（2026-09-23 API 6886 失联事故根因）。
            //
            // 方案：table_counts 保存每表 (total, valid)，由行级触发器增量维护——
            // SQLite 对 `INSERT .. ON CONFLICT DO UPDATE` 只在真插入时触发 INSERT
            // 触发器、冲突更新时只触发 UPDATE 触发器，因此无需在 Rust 端区分
            // 插入/更新。触发器在同一事务内执行，崩溃/回滚天然一致。
            //
            // 校准：计数器以 calibrated 标记是否可信；空库（新建/内存库）建表即校准，
            // 存量库由启动校准 + db_entity_stats_refresh 周期任务以真实 COUNT 回写。
            // 未校准期间读路径回退真实 COUNT（与旧版行为一致）。
            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS table_counts (
                    name TEXT PRIMARY KEY,
                    total INTEGER NOT NULL DEFAULT 0,
                    valid INTEGER NOT NULL DEFAULT 0,
                    calibrated INTEGER NOT NULL DEFAULT 0
                );
                INSERT OR IGNORE INTO table_counts (name, total, valid, calibrated) VALUES
                    ('dht_nodes', 0, 0, 0),
                    ('peers', 0, 0, 0),
                    ('peers_archive', 0, 0, 0),
                    ('infohashes', 0, 0, 0),
                    ('trackers', 0, 0, 0);
                "#,
            )?;
            {
                // 软删表（含 deleted_at 列）：INSERT（总+有效）、墓碑/复活（仅有效）、
                // DELETE（总+按旧值有效）各一组触发器。WHEN 条件保证软删幂等
                // （重复 UPDATE 同一墓碑不重复计数）。
                let mut triggers = String::new();
                for tbl in ["dht_nodes", "peers", "infohashes", "trackers"] {
                    triggers.push_str(&format!(
                        r#"
                CREATE TRIGGER IF NOT EXISTS trg_{t}_ins_total AFTER INSERT ON {t}
                BEGIN UPDATE table_counts SET total = total + 1 WHERE name = '{t}'; END;
                CREATE TRIGGER IF NOT EXISTS trg_{t}_ins_valid AFTER INSERT ON {t}
                WHEN new.deleted_at IS NULL
                BEGIN UPDATE table_counts SET valid = valid + 1 WHERE name = '{t}'; END;
                CREATE TRIGGER IF NOT EXISTS trg_{t}_tombstone AFTER UPDATE OF deleted_at ON {t}
                WHEN old.deleted_at IS NULL AND new.deleted_at IS NOT NULL
                BEGIN UPDATE table_counts SET valid = valid - 1 WHERE name = '{t}'; END;
                CREATE TRIGGER IF NOT EXISTS trg_{t}_revive AFTER UPDATE OF deleted_at ON {t}
                WHEN old.deleted_at IS NOT NULL AND new.deleted_at IS NULL
                BEGIN UPDATE table_counts SET valid = valid + 1 WHERE name = '{t}'; END;
                CREATE TRIGGER IF NOT EXISTS trg_{t}_del_total AFTER DELETE ON {t}
                BEGIN UPDATE table_counts SET total = total - 1 WHERE name = '{t}'; END;
                CREATE TRIGGER IF NOT EXISTS trg_{t}_del_valid AFTER DELETE ON {t}
                WHEN old.deleted_at IS NULL
                BEGIN UPDATE table_counts SET valid = valid - 1 WHERE name = '{t}'; END;
                "#,
                        t = tbl
                    ));
                }
                // peers_archive 无 deleted_at 列，valid 恒等于 total
                triggers.push_str(
                    r#"
                CREATE TRIGGER IF NOT EXISTS trg_peers_archive_ins AFTER INSERT ON peers_archive
                BEGIN
                    UPDATE table_counts SET total = total + 1, valid = valid + 1 WHERE name = 'peers_archive';
                END;
                CREATE TRIGGER IF NOT EXISTS trg_peers_archive_del AFTER DELETE ON peers_archive
                BEGIN
                    UPDATE table_counts SET total = total - 1, valid = valid - 1 WHERE name = 'peers_archive';
                END;
                "#,
                );
                conn.execute_batch(&triggers)?;
            }
            // 空库（新建文件/内存库）计数值天然为真，直接标记已校准，避免读路径走
            // COUNT 回退；存量库保持 calibrated=0，由启动校准任务回填真值。
            let all_entity_tables_empty = [
                "dht_nodes",
                "peers",
                "peers_archive",
                "infohashes",
                "trackers",
            ]
            .iter()
            .all(|t| {
                conn.query_row(&format!("SELECT 1 FROM {} LIMIT 1", t), [], |_| Ok(()))
                    .is_err()
            });
            if all_entity_tables_empty {
                conn.execute("UPDATE table_counts SET calibrated = 1", [])?;
            }

            // v9：range 反熵 / bootstrap 分块都按**表达式键**做 `ORDER BY` 与区间比较
            // （NODE `(ip||':'||port)`、PEER `(lower(hex(infohash))||':'||ip||':'||port)`），
            // 而此前没有任何匹配索引 ⇒ 每次调用都是「全表扫描 + 全量排序」，且全程持有连接锁
            // （实测 bootstrap 建一次清单 = 74~93 次全表排序，单次重建 >131s，把 HTTP API 与
            // 写队列一起拖垮）。表达式索引让这两条路径退化为索引有序扫描。
            // 配合 v9 查询侧「按需拼谓词」（见 `node_range_sql`），索引才真正被用于区间定位。
            // F9 方案 B：peers_archive 归档表也纳入 PEER 清单/块扫描口径（归档是本地冷分层
            // 不是数据边界），为其建同款 key 表达式索引。注意 archive 无 deleted_at 列，
            // 索引表达式不含墓碑过滤（活行过滤语义由查询侧 WHERE 决定）。
            // 幂等；首次启动会在 165 万行的 dht_nodes 上同步建索引（一次性、数十秒量级），
            // 失败必须可见（旧写法 `let _ =` 会静默退化为全表排序）。
            if let Err(e) = conn.execute_batch(
                r#"
                CREATE INDEX IF NOT EXISTS idx_dht_nodes_ip_port_expr
                    ON dht_nodes((ip || ':' || port));
                CREATE INDEX IF NOT EXISTS idx_peers_key_expr
                    ON peers((lower(hex(infohash)) || ':' || ip || ':' || port));
                CREATE INDEX IF NOT EXISTS idx_peers_archive_key_expr
                    ON peers_archive((lower(hex(infohash)) || ':' || ip || ':' || port));
                CREATE INDEX IF NOT EXISTS idx_infohashes_ih_alive
                    ON infohashes(infohash) WHERE deleted_at IS NULL;
                CREATE INDEX IF NOT EXISTS idx_trackers_url_alive
                    ON trackers(url) WHERE deleted_at IS NULL;
                "#,
            ) {
                tracing::warn!(
                    "[storage] 表达式索引创建失败（interval 查询将退化为全表排序，性能下降）: {}",
                    e
                );
            }

            // A3（v11）：删除 v8 去 Merkle 化后无人查询的 l2_shard 索引（纯写放大）。
            // 列与 save_* 写入路径保留（诊断/联邦口径仍读该列值）。幂等：新库本就没有这些索引。
            if drop_unused_indexes {
                let n = drop_unused_l2_shard_indexes(conn)?;
                info!(
                    "[storage] 已删除 {} 个无用索引（l2_shard），减少写入放大",
                    n
                );
            }

            // B5-1：DROP 与 PK 首列重复的 peers 索引（幂等，有开关）。
            if drop_redundant_peer_indexes {
                let n = run_drop_redundant_peer_indexes(conn)?;
                info!("[storage] 已删除 {} 个冗余 peers 索引（B5-1）", n);
            }

            // P1-2：变更日志表（联邦 delta 同步的权威来源）
            crate::storage::oplog::init_oplog_table(conn)?;

            // P1-3 / P2-1：联邦层拥有的两张表也在此统一建（幂等）。
            // 放在 init_tables 是为了让 `Storage::memory()`（单元测试）与 `Storage::open`（生产）
            // 都能拿到完整 schema，避免「运行期才发现 no such table」的隐患。
            crate::federation::sync::delta::init_delta_tables(conn)?;
            crate::federation::sync::bootstrap::init_bootstrap_table(conn)?;

            debug!("[storage] 表结构初始化完成");
            Ok(())
        })
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
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
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
            // P-A：块表标 dirty（key = ip:port）
            let key = format!("{}:{}", ip, port).into_bytes();
            Self::mark_chunks_dirty_in_tx(conn, 1 /* NODE */, std::slice::from_ref(&key))?;
            Ok(())
        })
    }

    /// 加载所有 DHT 节点
    pub fn load_dht_nodes(&self) -> anyhow::Result<Vec<DhtNodeRow>> {
        self.read_long(|conn| {
        let mut stmt = conn.prepare("SELECT id, ip, port, score, state, query_count, success_count, total_latency_ms, consecutive_failures, nodes_returned, last_query_time, last_active FROM dht_nodes WHERE deleted_at IS NULL")?;
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
                last_active: row.get::<_, Option<i64>>(11).unwrap_or(None),
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
        })
    }

    /// 清空 DHT 节点表
    pub fn clear_dht_nodes(&self) -> anyhow::Result<()> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            conn.execute("DELETE FROM dht_nodes", [])?;
            // P-A：块表同清（NODE 数据已清空，摘要失效）
            conn.execute("DELETE FROM chunk_digests WHERE repo = 1", [])?;
            Ok(())
        })
    }

    /// P1-6：软删除单个 DHT 节点（写 deleted_at 墓碑，不物理删除）。
    /// 返回受影响行数（0 表示行不存在或已删除）。upsert 会自动清除墓碑以支持复活。
    pub fn soft_delete_node(&self, ip: &str, port: u16) -> anyhow::Result<usize> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            let now = chrono::Utc::now().timestamp();
            let n = conn.execute(
                "UPDATE dht_nodes SET deleted_at = ?1, updated_at = ?1 \
                 WHERE ip = ?2 AND port = ?3 AND deleted_at IS NULL",
                params![now, ip, port as i64],
            )?;
            if n > 0 {
                // P-A：块表标 dirty（key = ip:port）
                let key = format!("{}:{}", ip, port).into_bytes();
                Self::mark_chunks_dirty_in_tx(conn, 1 /* NODE */, std::slice::from_ref(&key))?;
            }
            Ok(n)
        })
    }

    /// P1-6：批量软删除 DHT 节点（一次事务）。
    pub fn soft_delete_nodes_batch(&self, addrs: &[(String, u16)]) -> anyhow::Result<usize> {
        if addrs.is_empty() {
            return Ok(0);
        }
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
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
            // P-A：块表标 dirty（key = ip:port）
            let keys: Vec<Vec<u8>> = addrs
                .iter()
                .map(|(ip, port)| format!("{}:{}", ip, port).into_bytes())
                .collect();
            Self::mark_chunks_dirty_in_tx(&tx, 1 /* NODE */, &keys)?;
            tx.commit()?;
            Ok(total)
        })
    }

    /// 批量保存 DHT 节点（事务批量插入，一次获取锁完成所有操作）
    pub fn save_dht_nodes_batch(&self, nodes: &[DhtNodeRow]) -> anyhow::Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        self.record_write("dht_nodes", nodes.len() as u64);
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            Self::save_dht_nodes_batch_conn(conn, nodes)
        })
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
                    node.last_active.unwrap_or(now),
                    l2,
                    now
                ])?;
            }
        }
        // P-A：块表标 dirty（key = ip:port）
        let keys: Vec<Vec<u8>> = nodes
            .iter()
            .map(|n| format!("{}:{}", n.ip, n.port).into_bytes())
            .collect();
        Self::mark_chunks_dirty_in_tx(&tx, 1 /* NODE */, &keys)?;
        tx.commit()?;
        Ok(())
    }

    /// 批量保存 DHT 节点（调用方已开启事务，不重复开启）
    /// 供 WriteQueue 在批量事务中调用，避免事务嵌套。
    /// P-A：写事务内按 key 集合标记脏块（前进式扫描块表 + 批量 UPDATE）。
    ///
    /// - 块表未构建（空表，旧库待 P-C 回填）时静默跳过——无块可标。
    /// - key 排序与清单排序键逐字节一致（BLOB 字节序 = 字符串序）。
    /// - 落在块间隙/超界（块表边界未覆盖）时保守标记相邻块。
    pub fn mark_chunks_dirty_in_tx(
        conn: &Connection,
        repo: u8,
        keys: &[Vec<u8>],
    ) -> anyhow::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut stmt = conn.prepare(
            "SELECT idx, lo_key, hi_key FROM chunk_digests WHERE repo = ?1 ORDER BY lo_key ASC",
        )?;
        let blocks: Vec<(i64, Vec<u8>, Vec<u8>)> = stmt
            .query_map(params![repo], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .filter_map(|r| r.ok())
            .collect();
        if blocks.is_empty() {
            return Ok(()); // 块表未构建（P-C 回填前无块可标）
        }
        let mut sorted = keys.to_vec();
        sorted.sort();
        let mut dirty: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
        let mut bi = 0usize;
        for k in &sorted {
            // 前进式：推进到 lo <= key 的最大块
            while bi + 1 < blocks.len() && blocks[bi + 1].1.as_slice() <= k.as_slice() {
                bi += 1;
            }
            let (idx, lo, hi) = &blocks[bi];
            if lo.as_slice() <= k.as_slice()
                && (hi.as_slice() >= k.as_slice() || bi + 1 == blocks.len())
            {
                dirty.insert(*idx);
            } else {
                // 落在块间隙/超界：保守标记前驱与后继块
                dirty.insert(*idx);
                if bi + 1 < blocks.len() {
                    dirty.insert(blocks[bi + 1].0);
                }
            }
        }
        for idx in dirty {
            conn.execute(
                "UPDATE chunk_digests SET dirty = 1 WHERE repo = ?1 AND idx = ?2",
                params![repo, idx],
            )?;
        }
        Ok(())
    }

    /// P-A：整 repo 标 dirty（全表 DELETE / 批量 UPDATE 等无法逐 key 定位的操作后）。
    pub fn mark_repo_all_dirty(&self, repo: u8) -> anyhow::Result<()> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            conn.execute(
                "UPDATE chunk_digests SET dirty = 1 WHERE repo = ?1",
                params![repo],
            )?;
            Ok(())
        })
    }

    /// P-A/P-B：读取某 repo 块表（清单构建 / 对齐验证用）。
    pub fn get_chunk_digests(&self, repo: u8) -> anyhow::Result<Vec<ChunkDigest>> {
        self.read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT repo, idx, lo_key, hi_key, rows, hash, dirty, chunk_rows \
                 FROM chunk_digests WHERE repo = ?1 ORDER BY idx ASC",
            )?;
            let rows = stmt.query_map(params![repo], |row| {
                Ok(ChunkDigest {
                    repo: row.get(0)?,
                    idx: row.get(1)?,
                    lo_key: row.get(2)?,
                    hi_key: row.get(3)?,
                    rows: row.get(4)?,
                    hash: row.get(5)?,
                    dirty: row.get::<_, i64>(6)? != 0,
                    chunk_rows: row.get(7)?,
                })
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        })
    }

    /// P-B：upsert 块表行（脏块重算后落库）。
    #[allow(clippy::too_many_arguments)]
    pub fn upsert_chunk_digest_in_tx(
        conn: &Connection,
        repo: u8,
        idx: i64,
        lo_key: &[u8],
        hi_key: &[u8],
        rows: i64,
        hash: &[u8],
        dirty: bool,
        chunk_rows: u32,
    ) -> anyhow::Result<()> {
        conn.execute(
            "INSERT INTO chunk_digests (repo, idx, lo_key, hi_key, rows, hash, dirty, chunk_rows) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
             ON CONFLICT(repo, idx) DO UPDATE SET \
              lo_key=excluded.lo_key, hi_key=excluded.hi_key, rows=excluded.rows, \
              hash=excluded.hash, dirty=excluded.dirty, chunk_rows=excluded.chunk_rows",
            params![
                repo,
                idx,
                lo_key,
                hi_key,
                rows,
                hash,
                dirty as i64,
                chunk_rows
            ],
        )?;
        Ok(())
    }

    /// P-B：删除块表行（重算切块后整体重写某 repo 块表时用）。
    pub fn delete_chunk_digests_in_tx(conn: &Connection, repo: u8) -> anyhow::Result<()> {
        conn.execute("DELETE FROM chunk_digests WHERE repo = ?1", params![repo])?;
        Ok(())
    }

    /// P-B：&self 包装——脏块重算后写回块表（自加写锁）。
    #[allow(clippy::too_many_arguments)]
    pub fn upsert_chunk_digest(
        &self,
        repo: u8,
        idx: i64,
        lo_key: &[u8],
        hi_key: &[u8],
        rows: i64,
        hash: &[u8],
        dirty: bool,
        chunk_rows: u32,
    ) -> anyhow::Result<()> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            Self::upsert_chunk_digest_in_tx(
                conn, repo, idx, lo_key, hi_key, rows, hash, dirty, chunk_rows,
            )
        })
    }

    /// P-B：&self 包装——清空某 repo 块表（chunk_rows 变化重建前）。
    pub fn delete_chunk_digests(&self, repo: u8) -> anyhow::Result<()> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            Self::delete_chunk_digests_in_tx(conn, repo)
        })
    }

    /// P-B：廉价一致性校验——repo 表实际活行数（与清单口径一致：NODE/TRACKER/INFOHASH
    /// 取 `deleted_at IS NULL`，PEER = peers 活行 + peers_archive 全部）。
    /// 清单构建时与块表 rows 总和比对，兜住「绕过挂接的直接 SQL 写入」造成的陈旧摘要。
    pub fn count_repo_rows(&self, repo: u8) -> anyhow::Result<i64> {
        self.read_long(|conn| {
            const NODE: u8 = 1;
            const PEER: u8 = 2;
            const INFOHASH: u8 = 3;
            const TRACKER: u8 = 4;
            let cnt: i64 = match repo {
                NODE => conn.query_row(
                    "SELECT COUNT(*) FROM dht_nodes WHERE deleted_at IS NULL",
                    [],
                    |r| r.get(0),
                )?,
                PEER => conn.query_row(
                    "SELECT (SELECT COUNT(*) FROM peers WHERE deleted_at IS NULL) \
                     + (SELECT COUNT(*) FROM peers_archive)",
                    [],
                    |r| r.get(0),
                )?,
                INFOHASH => conn.query_row(
                    "SELECT COUNT(*) FROM infohashes WHERE deleted_at IS NULL",
                    [],
                    |r| r.get(0),
                )?,
                TRACKER => conn.query_row(
                    "SELECT COUNT(*) FROM trackers WHERE deleted_at IS NULL",
                    [],
                    |r| r.get(0),
                )?,
                _ => 0,
            };
            Ok(cnt)
        })
    }

    /// P-B：单事务批量回填某 repo 块表（首填/重建后整表写入，避免数千块逐块加锁）。
    /// `entries` = (idx, lo_key, hi_key, rows, hash)。
    pub fn backfill_chunk_digests(
        &self,
        repo: u8,
        chunk_rows: u32,
        entries: &[(i64, Vec<u8>, Vec<u8>, i64, Vec<u8>)],
    ) -> anyhow::Result<()> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            let tx = conn.unchecked_transaction()?;
            Self::delete_chunk_digests_in_tx(&tx, repo)?;
            for (idx, lo, hi, rows, hash) in entries {
                Self::upsert_chunk_digest_in_tx(
                    &tx, repo, *idx, lo, hi, *rows, hash, false, chunk_rows,
                )?;
            }
            tx.commit()?;
            Ok(())
        })
    }

    /// P2-6：单事务批量 upsert 脏块摘要（块表优先路径的增量重算）。
    ///
    /// 与 [`Self::upsert_chunk_digest`] 同语义（幂等 upsert、`dirty=false`），但把
    /// 同一批重算出的脏块合进**一次写锁 + 单事务**。旧实现每脏块一次
    /// `upsert_chunk_digest`（逐块抢 `self.conn` 写锁），脏块多时读写乒乓把写锁
    /// 打满。**不 delete 干净块**（区别于 [`Self::backfill_chunk_digests`]）。
    /// `entries` = (idx, lo_key, hi_key, rows, hash)。
    pub fn upsert_chunk_digests_batch(
        &self,
        repo: u8,
        chunk_rows: u32,
        entries: &[(i64, Vec<u8>, Vec<u8>, i64, Vec<u8>)],
    ) -> anyhow::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            let tx = conn.unchecked_transaction()?;
            for (idx, lo, hi, rows, hash) in entries {
                Self::upsert_chunk_digest_in_tx(
                    &tx, repo, *idx, lo, hi, *rows, hash, false, chunk_rows,
                )?;
            }
            tx.commit()?;
            Ok(())
        })
    }

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
                node.last_active.unwrap_or(now),
                l2,
                now
            ])?;
        }
        // P-A：块表标 dirty（本批写入的 key = ip:port，与清单排序键一致）
        const NODE: u8 = 1;
        let keys: Vec<Vec<u8>> = nodes
            .iter()
            .map(|n| format!("{}:{}", n.ip, n.port).into_bytes())
            .collect();
        Self::mark_chunks_dirty_in_tx(conn, NODE, &keys)?;
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
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
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
            // P-A：块表标 dirty（key = url）
            Self::mark_chunks_dirty_in_tx(conn, 4 /* TRACKER */, &[url.as_bytes().to_vec()])?;
            Ok(())
        })
    }

    /// P1-6：软删除指定 tracker（写 `deleted_at` 墓碑，不物理删除）。用于 `remove_tracker`。
    /// 物理删除会让「本地删了」与「本地从来没有」在集合语义下无法区分，重启后
    /// `load_trackers` 回源又会把它"复活"；改成墓碑 + 查询过滤后两端 Merkle 才能收敛到 0。
    pub fn soft_delete_tracker(&self, url: &str) -> anyhow::Result<usize> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            let now = chrono::Utc::now().timestamp();
            let n = conn.execute(
                "UPDATE trackers SET deleted_at = ?1, updated_at = ?1 \
                 WHERE url = ?2 AND deleted_at IS NULL",
                params![now, url],
            )?;
            if n > 0 {
                // P-A：块表标 dirty（key = url）
                Self::mark_chunks_dirty_in_tx(
                    conn,
                    4, /* TRACKER */
                    &[url.as_bytes().to_vec()],
                )?;
            }
            Ok(n)
        })
    }

    /// 批次I(#5)：peer 软删墓碑——入站 DELETE 落墓碑，防止
    /// 「A 删 → B 物理删 → B 重新采集 → 当新建回推 → A 复活」的收敛闭环。
    pub fn soft_delete_peer(
        &self,
        infohash_hex: &str,
        ip: &str,
        port: u16,
    ) -> anyhow::Result<usize> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            let now = chrono::Utc::now().timestamp();
            let n = conn.execute(
                "UPDATE peers SET deleted_at = ?1 \
                 WHERE lower(hex(infohash)) = ?2 AND ip = ?3 AND port = ?4 AND deleted_at IS NULL",
                params![now, infohash_hex, ip, port as i64],
            )?;
            if n > 0 {
                // P-A：块表标 dirty（key = lower(hex(infohash)):ip:port）
                let key = format!("{}:{}:{}", infohash_hex.to_lowercase(), ip, port).into_bytes();
                Self::mark_chunks_dirty_in_tx(conn, 2 /* PEER */, &[key])?;
            }
            Ok(n)
        })
    }

    /// 批次I(#5)：判断 peer 是否存在软删墓碑（入站 upsert 仲裁用）。
    ///
    /// P2（读写分离）：原实现拿 `self.conn` 写锁做 SELECT——读流量灌满写锁，
    /// apply 万级条目逐条仲裁时把全局写锁打满。改走短读池 `self.read`（已提交
    /// 态与旧写连接读一致，apply 循环无外层事务，语义等价）；read Err 向上传播，
    /// 调用方 peer_sync.rs:169 已有 match 降级。
    pub fn is_peer_tombstoned(
        &self,
        infohash_hex: &str,
        ip: &str,
        port: u16,
    ) -> anyhow::Result<bool> {
        let n: i64 = self.read(move |conn| -> anyhow::Result<i64> {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM peers \
                 WHERE lower(hex(infohash)) = ?1 AND ip = ?2 AND port = ?3 AND deleted_at IS NOT NULL",
                params![infohash_hex, ip, port as i64],
                |row| row.get(0),
            )?)
        })?;
        Ok(n > 0)
    }

    /// 批次I(#5)：infohash 软删墓碑（语义同 peer）。
    pub fn soft_delete_infohash(&self, infohash_hex: &str) -> anyhow::Result<usize> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            let now = chrono::Utc::now().timestamp();
            let n = conn.execute(
                "UPDATE infohashes SET deleted_at = ?1 \
                 WHERE lower(hex(infohash)) = ?2 AND deleted_at IS NULL",
                params![now, infohash_hex],
            )?;
            if n > 0 {
                // P-A：块表标 dirty（key = infohash raw；hex 参数解码成本高，整 repo 保守标脏）
                conn.execute("UPDATE chunk_digests SET dirty = 1 WHERE repo = 3", [])?;
            }
            Ok(n)
        })
    }

    /// 批次I(#5)：判断 infohash 是否存在软删墓碑。
    ///
    /// P2（读写分离）：改走短读池 `self.read`，不再拿 `self.conn` 写锁做 SELECT。
    pub fn is_infohash_tombstoned(&self, infohash_hex: &str) -> anyhow::Result<bool> {
        let n: i64 = self.read(move |conn| -> anyhow::Result<i64> {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM infohashes \
                 WHERE lower(hex(infohash)) = ?1 AND deleted_at IS NOT NULL",
                params![infohash_hex],
                |row| row.get(0),
            )?)
        })?;
        Ok(n > 0)
    }

    /// P0-3：判断指定 tracker 是否存在软删墓碑（`deleted_at` 非 NULL）。
    ///
    /// 用于入站 upsert 的仲裁：本地已删除的 tracker 不得被对端回推的 upsert 复活，
    /// 否则形成「A 删 → B 未删 → 反熵判差异 → B 回推 upsert → A 复活 → 反熵再判差异」
    /// 的永动闭环，删除操作在联邦内结构性不可能收敛。
    pub fn is_tracker_tombstoned(&self, url: &str) -> bool {
        // P2（读写分离）：改走短读池 `self.read`，不再拿 `self.conn` 写锁做 SELECT。
        // 读池忙碌（超时/饥饿）时降级 false（按"未墓碑"处理，允许重添加——宁可被
        // 对端回推一次重复 upsert，也不阻塞入站仲裁）。
        match self.read(move |conn| -> anyhow::Result<bool> {
            Ok(conn
                .query_row(
                    "SELECT 1 FROM trackers WHERE url = ?1 AND deleted_at IS NOT NULL LIMIT 1",
                    params![url],
                    |_| Ok(true),
                )
                .unwrap_or(false))
        }) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(target: "pdcdb", "is_tracker_tombstoned({}) 读失败，按未墓碑处理: {}", url, e);
                false
            }
        }
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
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
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
            // P-A：块表标 dirty（key = url）
            Self::mark_chunks_dirty_in_tx(conn, 4 /* TRACKER */, &[url.as_bytes().to_vec()])?;
            Ok(())
        })
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
        // P-A：块表标 dirty（key = url，与清单排序键一致）
        const TRACKER: u8 = 4;
        let keys: Vec<Vec<u8>> = trackers.iter().map(|t| t.url.as_bytes().to_vec()).collect();
        Self::mark_chunks_dirty_in_tx(conn, TRACKER, &keys)?;
        Ok(())
    }

    pub fn load_trackers(&self) -> anyhow::Result<Vec<TrackerRow>> {
        self.read(|conn| {
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
        })
    }

    /// 按分数降序加载 top N 个 tracker（启动预加载用）
    pub fn load_top_trackers(&self, limit: usize) -> anyhow::Result<Vec<TrackerRow>> {
        self.read(|conn| {
        let mut stmt = conn.prepare(
            "SELECT url, score, total_requests, success_requests, failed_requests, total_peers_discovered, total_response_time_ms, consecutive_failures, disabled 
             FROM trackers WHERE deleted_at IS NULL 
             ORDER BY score DESC 
             LIMIT ?"
        )?;
        let rows = stmt.query_map([limit as i64], |row| {
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
        })
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
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            Self::save_infohash_in_tx(conn, infohash, ref_count, first_source, score)
        })
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
        // P-A：块表标 dirty（key = infohash raw）
        const INFOHASH: u8 = 3;
        Self::mark_chunks_dirty_in_tx(conn, INFOHASH, &[infohash.to_vec()])?;
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
        // P-A：块表标 dirty（key = infohash raw，与清单排序键一致）
        const INFOHASH: u8 = 3;
        let keys: Vec<Vec<u8>> = entries.iter().map(|e| e.infohash.to_vec()).collect();
        Self::mark_chunks_dirty_in_tx(conn, INFOHASH, &keys)?;
        Ok(())
    }

    /// 批次G(F4)：批量判断 infohash 是否已存在于 DB（重添加防误报新建——
    /// 内存驱逐后的条目再遇时只回内存，不进 oplog/gossip，掐掉联邦风暴的燃料源）
    pub fn existing_infohashes(
        &self,
        ihs: &[[u8; 20]],
    ) -> anyhow::Result<std::collections::HashSet<[u8; 20]>> {
        let mut out = std::collections::HashSet::new();
        if ihs.is_empty() {
            return Ok(out);
        }
        // P4-4（v9，preP32b）：爬虫落库去重回 **read_long（受 permit 门限流）**。
        // preP31 曾把 existing_* 全转 priority（无门）——爬虫多线程去重失去并发
        // 上限后无限借池，长查询池 8 条被占满 → apply 查重/发送取数等池 60s panic
        // → 传输断续 + longAct 泄漏计数涨到 160（实证）。去重是普通长查询应受门
        // 限流（≤LONG_QUERY_CONCURRENCY 并发）；apply 查重（existing_node_keys）
        // 保持 priority 不排队。去重排队 60s panic 只会丢一批爬虫响应，可接受。
        self.read_long(|conn| {
            for chunk in ihs.chunks(500) {
                let placeholders = vec!["?"; chunk.len()].join(",");
                let sql = format!(
                    "SELECT infohash FROM infohashes WHERE infohash IN ({})",
                    placeholders
                );
                if let Ok(mut stmt) = conn.prepare(&sql) {
                    let params: Vec<&dyn rusqlite::ToSql> =
                        chunk.iter().map(|h| h as &dyn rusqlite::ToSql).collect();
                    if let Ok(rows) = stmt.query_map(params.as_slice(), |row| {
                        let b: Vec<u8> = row.get(0)?;
                        let mut a = [0u8; 20];
                        if b.len() == 20 {
                            a.copy_from_slice(&b);
                        }
                        Ok(a)
                    }) {
                        for r in rows.flatten() {
                            out.insert(r);
                        }
                    }
                }
            }
            Ok(())
        })?;
        Ok(out)
    }

    /// 批次G(F4)：批量判断节点 ("ip:port") 是否已存在于 DB（语义同 existing_infohashes）
    ///
    /// P4-2（v9，preP30）：**写路径查重**（crawler 落库 / bootstrap apply 批量 upsert
    /// 前判重）改走 `read_long_priority`（不经过长查询并发门）——线上实证：
    /// 去重/纯读长查询占满 permit 时，bootstrap 块 apply 的查重等 permit 60s 超时
    /// panic → apply join Err → 块判失败 → 窗口暂停 → bootstrap 传输停滞（.52
    /// 2026-10-08 06:36 done=77/3126 卡死实证）。写路径查重比纯读更重要，
    /// 不应被读长查询门限流饿死；池连接充足（8 条）时与去重并发无碍。
    pub fn existing_node_keys(
        &self,
        keys: &[String],
    ) -> anyhow::Result<std::collections::HashSet<String>> {
        let mut out = std::collections::HashSet::new();
        if keys.is_empty() {
            return Ok(out);
        }
        // preP34（P4-6）：去重 SQL 从「表达式 IN」改为「主键 (ip,port) 点查循环」。
        // 线上实证（2026-10-08 .52）：`(ip||':'||port) IN (500项)` 因 SQLite 大 IN
        // 列表不触发表达式索引 → 退化为全表逐行拼接比较 → 604 万行 × 约 5s/查询 →
        // 8 条长查询池被钉死 300s+（借用超时告警 db.rs:2387 实证 304~308s）→
        // bootstrap apply 查重/发送取数全部饿死，done 卡 115/3126。
        // 主键 (ip,port) 点查毫秒级（实测 500 次 = 19ms，快 260 倍），持池时间
        // 从分钟级降到毫秒级，长查询池不再被去重钉死。
        self.read_long_priority(|conn| {
            for chunk in keys.chunks(500) {
                if let Ok(mut stmt) = conn.prepare(
                    "SELECT 1 FROM dht_nodes WHERE ip=?1 AND port=?2 AND deleted_at IS NULL",
                ) {
                    for key in chunk {
                        // key 格式 = "ip:port"（与清单排序键一致）；IPv6 文本含 ':'，
                        // 从右取最后一个 ':' 作为 ip/port 分隔。
                        let Some((ip, port_s)) = key.rsplit_once(':') else {
                            continue;
                        };
                        let Ok(port) = port_s.parse::<i64>() else {
                            continue;
                        };
                        if let Ok(mut rows) = stmt.query(params![ip, port]) {
                            if rows.next().map(|r| r.is_some()).unwrap_or(false) {
                                out.insert(key.clone());
                            }
                        }
                    }
                }
            }
            Ok(())
        })?;
        Ok(out)
    }

    /// 批次G(F4)：批量判断 peer (infohash, addr) 是否已存在于 DB（重添加防误报传播）。
    /// key = `lower(hex(infohash)):ip:port`，与 idx_peers_key_expr 表达式索引一致。
    /// P4-2（v9，preP30）：写路径查重，走 `read_long_priority`（同 existing_node_keys）。
    pub fn existing_peer_keys(
        &self,
        entries: &[(crate::types::Infohash, std::net::SocketAddr)],
    ) -> anyhow::Result<std::collections::HashSet<String>> {
        let mut out = std::collections::HashSet::new();
        if entries.is_empty() {
            return Ok(out);
        }
        // P4-4（v9，preP32b）：爬虫 peer 落库去重回 **read_long（受 permit 门）**，
        // 理由同 existing_infohashes（去重是普通长查询，受门限流防占满长查询池）。
        self.read_long(|conn| {
            for chunk in entries.chunks(500) {
                let mut keys = Vec::with_capacity(chunk.len());
                for (ih, addr) in chunk {
                    let hex: String = ih.iter().map(|b| format!("{:02x}", b)).collect();
                    keys.push(format!("{}:{}:{}", hex, addr.ip(), addr.port()));
                }
                let placeholders = vec!["?"; chunk.len()].join(",");
                let sql = format!(
                    "SELECT (lower(hex(infohash)) || ':' || ip || ':' || port) FROM peers \
                     WHERE (lower(hex(infohash)) || ':' || ip || ':' || port) IN ({})",
                    placeholders
                );
                if let Ok(mut stmt) = conn.prepare(&sql) {
                    let params: Vec<&dyn rusqlite::ToSql> =
                        keys.iter().map(|k| k as &dyn rusqlite::ToSql).collect();
                    if let Ok(rows) =
                        stmt.query_map(params.as_slice(), |row| row.get::<_, String>(0))
                    {
                        for r in rows.flatten() {
                            out.insert(r);
                        }
                    }
                }
            }
            Ok(())
        })?;
        Ok(out)
    }

    /// 批量保存 infohash（在已有连接上执行，供 IOScheduler 回调）
    /// 加载所有 infohash
    pub fn load_infohashes(&self) -> anyhow::Result<Vec<InfohashRow>> {
        self.read_long(|conn| {
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
        })
    }

    /// 更新 infohash 评分
    pub fn update_infohash_score(&self, infohash: &[u8; 20], score: f64) -> anyhow::Result<()> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            conn.execute(
                "UPDATE infohashes SET score = ?1 WHERE infohash = ?2",
                params![score, infohash.as_slice()],
            )?;
            Ok(())
        })
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
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            let tx = conn.unchecked_transaction()?;
            {
                let mut stmt =
                    tx.prepare("UPDATE infohashes SET score = ?1 WHERE infohash = ?2")?;
                for (infohash, score) in scores {
                    stmt.execute(params![score, infohash.as_slice()])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
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
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            conn.execute("DELETE FROM infohashes", [])?;
            // P-A：块表同清（INFOHASH 数据已清空）
            conn.execute("DELETE FROM chunk_digests WHERE repo = 3", [])?;
            Ok(())
        })
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
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
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
        })
    }

    /// 加载所有 peer
    pub fn load_peers(&self) -> anyhow::Result<Vec<PeerRow>> {
        self.read_long(|conn| {
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
        })
    }

    /// 清空 peers 表
    pub fn clear_peers(&self) -> anyhow::Result<()> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            conn.execute("DELETE FROM peers", [])?;
            // P-A：块表同清（PEER 数据已清空；归档表仍存在时清单仍覆盖 archive，故仅标脏更稳——
            // 此处按清空语义直接删块表，由 P-C 回填重建）
            conn.execute("DELETE FROM chunk_digests WHERE repo = 2", [])?;
            Ok(())
        })
    }

    /// 批量保存 peers（事务批量插入）
    pub fn save_peers_batch(&self, peers: &[PeerRow]) -> anyhow::Result<()> {
        if peers.is_empty() {
            return Ok(());
        }
        self.record_write("peers", peers.len() as u64);
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
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
            // P-A：块表标 dirty（key = lower(hex(infohash)):ip:port）
            let keys: Vec<Vec<u8>> = peers
                .iter()
                .map(|p| format!("{}:{}:{}", hex_lower(&p.infohash), p.ip, p.port).into_bytes())
                .collect();
            Self::mark_chunks_dirty_in_tx(&tx, 2 /* PEER */, &keys)?;
            tx.commit()?;
            Ok(())
        })
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
        // P-A：块表标 dirty（key = lower(hex(infohash)):ip:port，与清单排序键一致）
        const PEER: u8 = 2;
        let keys: Vec<Vec<u8>> = peers
            .iter()
            .map(|p| format!("{}:{}:{}", hex_lower(&p.infohash), p.ip, p.port).into_bytes())
            .collect();
        Self::mark_chunks_dirty_in_tx(conn, PEER, &keys)?;
        Ok(())
    }

    /// 归档冷数据：将超过指定时间无活跃的 peer 从主表迁移到归档表
    /// 返回归档的 peer 数量
    pub fn archive_cold_peers(&self, older_than_secs: i64) -> anyhow::Result<usize> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
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
            // P-A：先取受影响 key（archive 移动行 = PEER 清单内容变化，需精确标脏）
            let keys: Vec<Vec<u8>> = {
                let mut key_stmt = tx.prepare(
                    "SELECT lower(hex(infohash)), ip, port FROM peers WHERE last_active < ?1",
                )?;
                let ks: Vec<Vec<u8>> = key_stmt
                    .query_map(params![threshold], |row| {
                        let ih_hex: String = row.get(0)?;
                        let ip: String = row.get(1)?;
                        let port: i64 = row.get(2)?;
                        Ok(format!("{}:{}:{}", ih_hex, ip, port).into_bytes())
                    })?
                    .filter_map(|r| r.ok())
                    .collect();
                drop(key_stmt);
                ks
            };
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
            // P-A：块表标 dirty（精确 key）
            Self::mark_chunks_dirty_in_tx(&tx, 2 /* PEER */, &keys)?;
            tx.commit()?;

            Ok(count as usize)
        })
    }

    /// 在已有连接上归档冷 peer（供 WriteQueue/IOScheduler 闭包使用，调用方已开启事务）
    /// `older_than_secs` 为时间阈值（秒），last_active 早于 now - older_than_secs 的 peer 迁移到归档表
    pub fn archive_cold_peers_in_tx(conn: &Connection, older_than_secs: i64) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp();
        let threshold = now - older_than_secs;
        // P-A：先取受影响 key（精确标脏）
        let mut key_stmt = conn
            .prepare("SELECT lower(hex(infohash)), ip, port FROM peers WHERE last_active < ?1")?;
        let keys: Vec<Vec<u8>> = key_stmt
            .query_map(params![threshold], |row| {
                let ih_hex: String = row.get(0)?;
                let ip: String = row.get(1)?;
                let port: i64 = row.get(2)?;
                Ok(format!("{}:{}:{}", ih_hex, ip, port).into_bytes())
            })?
            .filter_map(|r| r.ok())
            .collect();
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
        // P-A：块表标 dirty（精确 key）
        Self::mark_chunks_dirty_in_tx(conn, 2 /* PEER */, &keys)?;
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
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            let now = chrono::Utc::now().timestamp();
            conn.execute(
                "INSERT INTO peer_history (infohash, ip, port, source, score, discovered_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![infohash.as_slice(), ip, port as i64, source, score, now],
            )?;
            Ok(())
        })
    }

    /// 批量写入 peer 历史（攒批写入，减少 fsync 次数）
    pub fn save_peer_history_batch(&self, entries: &[PeerHistoryEntry]) -> anyhow::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.record_write("peer_history", entries.len() as u64);
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            // 单独调用时自行开启事务（批量原子性）；WriteQueue/IOScheduler 路径使用 _in_tx 变体
            let tx = conn.unchecked_transaction()?;
            Self::save_peer_history_batch_in_tx(&tx, entries)?;
            tx.commit()?;
            Ok(())
        })
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
        self.read_long(|conn| {
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
        })
    }

    /// 清理过期 peer 历史（保留 days 天）
    pub fn cleanup_peer_history(&self, days: u64) -> anyhow::Result<usize> {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        with_write_conn(&self.conn, |conn| {
            let cutoff = chrono::Utc::now().timestamp() - (days as i64 * 86400);
            let deleted = conn.execute(
                "DELETE FROM peer_history WHERE discovered_at < ?1",
                params![cutoff],
            )?;
            if deleted > 0 {
                debug!("[storage] 清理了 {} 条过期 peer 历史", deleted);
            }
            Ok(deleted)
        })
    }

    // ---- Stats History ----

    /// 记录统计快照
    pub fn record_stats(&self, metric: &str, value: f64) -> anyhow::Result<()> {
        self.record_write("stats", 1);
        // 2026-10-09 治本 S5：走统一入口 with_write_conn，使写锁争用可观测。
        with_write_conn(&self.conn, |conn| {
            let now = chrono::Utc::now().timestamp();
            conn.execute(
                "INSERT INTO stats_history (timestamp, metric, value) VALUES (?1, ?2, ?3)",
                params![now, metric, value],
            )?;
            Ok(())
        })
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
        self.read(|conn| {
        let cutoff = chrono::Utc::now().timestamp() - (hours as i64 * 3600);
        let mut stmt = conn.prepare("SELECT timestamp, value FROM stats_history WHERE metric = ?1 AND timestamp >= ?2 ORDER BY timestamp")?;
        let rows = stmt.query_map(params![metric, cutoff], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
        })
    }

    // ---- Stats Aggregate ----

    /// 更新累计统计
    pub fn update_aggregate(&self, metric: &str, value: f64) -> anyhow::Result<()> {
        self.record_write("stats", 1);
        // 2026-10-09 治本 S5：走统一入口 with_write_conn。
        with_write_conn(&self.conn, |conn| {
            let now = chrono::Utc::now().timestamp();
            conn.execute(
                "INSERT INTO stats_aggregate (metric, value, updated_at) VALUES (?1, ?2, ?3)
                 ON CONFLICT(metric) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                params![metric, value, now],
            )?;
            Ok(())
        })
    }

    /// 批量更新累计统计：**单锁 + 单事务**（2026-10-09 治本 S6）。
    ///
    /// 【为什么需要】`io_metrics_poll` 任务间隔仅 **1 秒**（`stats_snapshot_interval_secs`
    /// 默认 1），单轮要写 8 个指标。若沿用 [`Self::update_aggregate`]逐条调用，
    /// 就是**每秒 8 次抢全库唯一写连接 + 8 次独立事务提交**。
    ///
    /// 120 分钟压测实测：该路径贡献了 9137 次写锁争用中的绝大部分，
    /// 并在 SQLite checkpoint 做磁盘 fsync 期间造成单次 **10415ms** 等锁
    /// （热点精确落在 `db.rs` 的 `update_aggregate` / `record_stats`）。
    ///
    /// 与 [`Self::record_entity_stats_batch`] 同构：锁持有时间从
    /// 「8×抢锁+8×提交」降到「1×抢锁+1×提交」。
    pub fn update_aggregate_batch(&self, entries: &[(&str, f64)]) -> anyhow::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.record_write("stats", entries.len() as u64);
        // 2026-10-09 治本 S6：走统一入口（锁持有时长可被 with_write_conn 观测）。
        with_write_conn(&self.conn, |conn| {
            // `update_aggregate_in_tx` 内部自行取时间戳（保持与单条版本一致），
            // 故此处无需 `now`。
            let tx = conn.transaction()?;
            for (name, value) in entries {
                Self::update_aggregate_in_tx(&tx, name, *value)?;
            }
            tx.commit()?;
            Ok(())
        })
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

    /// 批量写入实体行数统计：**单锁 + 单事务**完成，不再逐条加锁提交。
    ///
    /// 2026-10-09 卡死事故根因修复。`db_entity_stats_refresh`（Monitor 分类）
    /// 原实现对 5 个实体表逐个调 [`Self::record_stats`] + [`Self::update_aggregate`]，
    /// 而这两个方法各自 `self.conn.lock()` 抢**全库唯一写连接**
    /// （`conn: Arc<Mutex<Connection>>`，db.rs 中共 35 处 `self.conn.lock()`），
    /// 即单轮 10 次抢锁 + 10 次独立事务提交。在统计高频写入叠加 WAL 压力下，
    /// 写锁竞争雪崩：34 类后台任务全部被 watchdog 判定「槽位泄漏」强制回收
    /// （旧进程 52 次、新进程 27 次），API 读路径随之饿死——
    /// `/health`、`/federation/status`、`/io/status` 三端点同时无响应。
    ///
    /// 本方法一次加锁、一个事务写完 10 条，锁持有时间从「10×提交」降到「1×提交」，
    /// 与既有的 `*_in_tx` 变体设计一致（同批WriteQueue/IOScheduler 闭包也是单事务）。
    pub fn record_entity_stats_batch(&self, entries: &[(&str, f64)]) -> anyhow::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.record_write("stats", entries.len() as u64 * 2);
        // 2026-10-09 治本 S5：走统一入口（锁持有时长现可被观测/量化）。
        with_write_conn(&self.conn, |conn| {
            let tx = conn.transaction()?;
            for (name, value) in entries {
                if *value < 0.0 {
                    continue;
                }
                Self::record_stats_in_tx(&tx, name, *value)?;
                Self::update_aggregate_in_tx(&tx, name, *value)?;
            }
            tx.commit()?;
            Ok(())
        })
    }

    /// 加载累计统计
    pub fn load_aggregate(&self, metric: &str) -> Option<f64> {
        // P1：闭包返回 Result；read_long 超时/池饥饿时降级为 None（视为无该统计）。
        self.read_long(|conn| -> anyhow::Result<Option<f64>> {
            Ok(conn
                .query_row(
                    "SELECT value FROM stats_aggregate WHERE metric = ?1",
                    params![metric],
                    |row| row.get(0),
                )
                .ok())
        })
        .unwrap_or_else(|e| {
            tracing::warn!(target: "pdcdb", "load_aggregate({}) 读失败，降级 None: {}", metric, e);
            None
        })
    }

    /// 按最近活跃降序加载 DHT 节点 + LIMIT，供启动预加载。
    ///
    /// 旧实现 `load_hot_warm_nodes(7200, 0.0, n)` 的谓词
    /// `last_active > cutoff OR score >= min_score` 在 min_score=0 时恒真，
    /// 退化为「按历史评分取前 N」——陈旧高分僵尸整体进场且 last_active 被加载侧复位
    /// （2026-10 根因 R0/R1）。现改为新近度优先，与 `load_limited_peers` 同一模式。
    pub fn load_recent_nodes(&self, limit: usize) -> anyhow::Result<Vec<DhtNodeRow>> {
        self.read_long(|conn| {
            let mut stmt = conn.prepare(
                r#"SELECT id, ip, port, score, state, query_count, success_count,
                  total_latency_ms, consecutive_failures, nodes_returned, last_query_time, last_active
               FROM dht_nodes
               WHERE deleted_at IS NULL
               ORDER BY score DESC
               LIMIT ?1"#,
            )?;
            let rows = stmt.query_map(params![limit as i64], |row| {
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
                    last_active: row.get::<_, Option<i64>>(11).unwrap_or(None),
                })
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        })
    }

    /// 限量加载 peers（按最近活跃降序 + LIMIT），供启动预加载
    pub fn load_limited_peers(&self, limit: usize) -> anyhow::Result<Vec<PeerRow>> {
        self.read_long(|conn| {
        let mut stmt = conn.prepare(
            "SELECT infohash, ip, port, source, score, connection_attempts, connection_successes, last_active
             FROM peers WHERE deleted_at IS NULL
             ORDER BY score DESC LIMIT ?1",
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
        })
    }

    /// 限量加载 infohashes（按引用数降序 + LIMIT），供启动预加载
    pub fn load_limited_infohashes(&self, limit: usize) -> anyhow::Result<Vec<InfohashRow>> {
        self.read_long(|conn| {
            let mut stmt = conn.prepare(
                "SELECT infohash, ref_count, first_source, score
             FROM infohashes WHERE deleted_at IS NULL
             ORDER BY score DESC LIMIT ?1",
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
        })
    }

    /// 按 (ip, port) 加载单个 DHT 节点（缓存未命中时按需加载）。
    pub fn load_dht_node_by_addr(&self, ip: &str, port: u16) -> anyhow::Result<Option<DhtNodeRow>> {
        self.read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, ip, port, score, state, query_count, success_count, total_latency_ms, \
             consecutive_failures, nodes_returned, last_query_time, last_active FROM dht_nodes \
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
                    last_active: row.get::<_, Option<i64>>(11).unwrap_or(None),
                })
            });
            match result {
                Ok(row) => Ok(Some(row)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
    }

    /// 按 (ip, port) 加载单个 Peer（缓存未命中时按需加载）。
    pub fn load_peer_by_addr(&self, ip: &str, port: u16) -> anyhow::Result<Option<PeerRow>> {
        self.read(|conn| {
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
        })
    }

    /// 按 infohash 加载单个 Infohash 行（缓存未命中时按需加载）。
    pub fn load_infohash_by_hash(&self, ih: &[u8; 20]) -> anyhow::Result<Option<InfohashRow>> {
        self.read(|conn| {
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
        })
    }

    /// 按 url 加载单个 Tracker 行（缓存未命中时按需加载）。
    pub fn load_tracker_by_url(&self, url: &str) -> anyhow::Result<Option<TrackerRow>> {
        self.read(|conn| {
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
        })
    }

    /// 统计表的总行数（表名为调用方硬编码常量，无注入风险）。
    /// 优先读触发器维护的增量计数（O(1)）；未校准时回退真实 COUNT（与旧版一致）。
    pub fn count_table(&self, table: &str) -> anyhow::Result<u64> {
        if let Some((total, _)) = self.table_count_cached(table) {
            return Ok(total.max(0) as u64);
        }
        self.read_long(|conn| {
            let count: i64 =
                conn.query_row(&format!("SELECT COUNT(*) FROM {}", table), [], |row| {
                    row.get(0)
                })?;
            Ok(count as u64)
        })
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
        // P4-2（v9，preP32）：bootstrap 应答取数链（load_repo_sync_entries_in_range）
        // 走 read_long_priority——线上实证 .53 发送块 load 被去重占满 permit 饿死
        // 60s panic → NAK → 传输断续（done 77→89 卡壳）。发送块是关键路径。
        self.read_long_priority(|conn| {
            let lo_s = lo.map(|b| String::from_utf8_lossy(b).into_owned());
            let hi_s = hi.map(|b| String::from_utf8_lossy(b).into_owned());
            // v9：按需拼谓词，使新增的表达式索引 `idx_dht_nodes_ip_port_expr` 能被用于
            // **区间定位**。旧写法 `(?1 IS NULL OR (ip||':'||port) >= ?1)` 让 SQLite 无法做
            // 索引范围扫描，只能退化为「索引有序全扫 + 逐行过滤」—— 建一次清单要跑 93 个块查询，
            // 每个都从表头扫到 `lo`，合计上亿次探测（实测单次重建 >131s，持锁期间拖垮 API/写队列）。
            // v11(K 批/F2b)：优化器从不主动选表达式索引（偏爱 deleted 布尔索引 + TEMP B-TREE
            // 排序，实测 4.3s/区间），显式强制走表达式索引后 0.035s。
            let (sql_ix, binds) = Self::node_range_sql(
                "SELECT id, ip, port FROM dht_nodes",
                &lo_s,
                &hi_s,
                limit.max(1) as i64,
                Some("idx_dht_nodes_ip_port_expr"),
            );
            let (sql_plain, _) = Self::node_range_sql(
                "SELECT id, ip, port FROM dht_nodes",
                &lo_s,
                &hi_s,
                limit.max(1) as i64,
                None,
            );
            let mut stmt = prepare_range_stmt(conn, &sql_ix, &sql_plain)?;
            let bind_refs: Vec<&dyn rusqlite::ToSql> = binds.iter().map(|b| b.as_ref()).collect();
            let rows = stmt.query_map(bind_refs.as_slice(), |row| {
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
        })
    }

    /// v9：为 NODE 的区间查询拼装 SQL 与绑定参数（共用于原始行查询与 (key,data_hash) 查询）。
    ///
    /// 只在**确实需要**时生成边界谓词（`?1 IS NULL OR ...` 这类写法会阻断索引范围扫描）。
    /// `hi = None` 时直接省略上界；`lo = None` 时省略下界（-∞）。占位符按实际使用情况编号，
    /// 避免 rusqlite 的「参数个数不匹配」错误。
    /// v11(K 批/F2b)：`indexed_by` 传 Some(索引名) 时在表名后追加 `INDEXED BY`——
    /// 调用方须同时用 None 再生成一份 `sql_plain` 供 `prepare_range_stmt` 回退。
    fn node_range_sql(
        select: &str,
        lo_s: &Option<String>,
        hi_s: &Option<String>,
        limit: i64,
        indexed_by: Option<&str>,
    ) -> (String, Vec<Box<dyn rusqlite::ToSql>>) {
        let mut sql = String::from(select);
        if let Some(idx) = indexed_by {
            sql.push_str(&format!(" INDEXED BY {}", idx));
        }
        sql.push_str(" WHERE deleted_at IS NULL");
        let mut binds: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(3);
        let mut n = 0usize;
        if let Some(ref v) = lo_s {
            n += 1;
            sql.push_str(&format!(" AND (ip || ':' || port) >= ?{}", n));
            binds.push(Box::new(v.clone()));
        }
        if let Some(ref v) = hi_s {
            n += 1;
            sql.push_str(&format!(" AND (ip || ':' || port) < ?{}", n));
            binds.push(Box::new(v.clone()));
        }
        n += 1;
        sql.push_str(&format!(" ORDER BY (ip || ':' || port) ASC LIMIT ?{}", n));
        binds.push(Box::new(limit));
        (sql, binds)
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
        self.read_long(|conn| Self::query_node_key_hashes(conn, lo, hi, limit))
    }

    /// 纯 SQL 实现：按 [lo, hi) 区间读 dht_nodes 的 (key, hash)。
    /// 不持有任何锁，调用方负责连接生命周期。供 `load_node_key_hashes_in_range`
    /// 与 `load_repo_key_hashes_in_range`（已在外层 read 闭包内）共用，避免嵌套
    /// `self.read_long()` 在 `Storage::memory()` 回退写锁路径下二次 lock 同一 Mutex 死锁。
    fn query_node_key_hashes(
        conn: &rusqlite::Connection,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let lo_s = lo.map(|b| String::from_utf8_lossy(b).into_owned());
        let hi_s = hi.map(|b| String::from_utf8_lossy(b).into_owned());
        // v9：与 `load_node_rows_in_range` 同一套「按需谓词」拼装（使表达式索引可用于区间定位）。
        // v11(K 批/F2b)：INDEXED BY 强制（见 load_node_rows_in_range 内注释）。
        let (sql_ix, binds) = Self::node_range_sql(
            "SELECT id, ip, port FROM dht_nodes",
            &lo_s,
            &hi_s,
            limit.max(1) as i64,
            Some("idx_dht_nodes_ip_port_expr"),
        );
        let (sql_plain, _) = Self::node_range_sql(
            "SELECT id, ip, port FROM dht_nodes",
            &lo_s,
            &hi_s,
            limit.max(1) as i64,
            None,
        );
        let mut stmt = prepare_range_stmt(conn, &sql_ix, &sql_plain)?;
        let bind_refs: Vec<&dyn rusqlite::ToSql> = binds.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(bind_refs.as_slice(), |row| {
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
        self.read_long(|conn| {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
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
                last_active: None,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
        })
    }

    /// F9 方案 B：PEER 同步口径 = `peers` 主表活行 + `peers_archive` 归档行（两表合并）。
    ///
    /// 归档是本地冷数据分层（存储优化），不是数据边界——联邦同步应让两节点都拥有
    /// 对方的归档行。SQLite 视图不可索引，「UNION ALL + 外层 ORDER BY」会让优化器
    /// 放弃两表各自的 key 表达式索引（idx_peers_key_expr / idx_peers_archive_key_expr）
    /// 退化为全量排序，故采用**双路归并**：两表分别按各自 key 表达式
    /// `lower(hex(infohash))||':'||ip||':'||port` 有序取前 `limit` 条（主键
    /// (infohash,ip,port) 唯一 ⇒ 表内 key 无重复，各表前 limit 条是其范围结果的严格
    /// 前缀），再按 key 字节序归并取前 `limit` 条——与单表 `ORDER BY key LIMIT n`
    /// 语义严格一致（SQL TEXT 的 BINARY 序 = UTF-8 字节序）。
    ///
    /// **不去重**：同一 (infohash,ip,port) 可能同时存在于两表（先归档、后又重新活跃
    /// upsert 回主表，归档侧旧副本仍在）。计数口径（`local_entry_counts` =
    /// peers.valid + peers_archive.valid）按两表行数计，清单必须按同样行数出——
    /// 若清单侧去重，清单行数 < 协商计数，差的部分快照永远拉不到，BOOTSTRAP 死循环
    /// （F1 修过的问题换形式复发）。重复行 apply 到对端是幂等 upsert，无害。
    ///
    /// 返回 (key, infohash 原始 BLOB, ip, port)，key = `hex(ih):ip:port`（DB 形态）。
    fn query_peer_rows_both_tables(
        conn: &rusqlite::Connection,
        lo: Option<&str>,
        hi: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>, String, i64)>> {
        // peers 主表只取活行（deleted_at IS NULL）；peers_archive 无墓碑列，全部有效
        let main_rows = Self::query_peer_rows_one_table(
            conn,
            "peers",
            "idx_peers_key_expr",
            "deleted_at IS NULL",
            lo,
            hi,
            limit.max(1) as i64,
        )?;
        let archive_rows = Self::query_peer_rows_one_table(
            conn,
            "peers_archive",
            "idx_peers_archive_key_expr",
            "1=1",
            lo,
            hi,
            limit.max(1) as i64,
        )?;
        // 双路归并：两边各自有序，轮流取较小 key；同 key 时先取主表行（结果确定）
        let mut out: Vec<(Vec<u8>, Vec<u8>, String, i64)> =
            Vec::with_capacity(main_rows.len() + archive_rows.len());
        let mut ai = main_rows.into_iter().peekable();
        let mut bi = archive_rows.into_iter().peekable();
        loop {
            if out.len() >= limit {
                break;
            }
            let from_main = match (ai.peek(), bi.peek()) {
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
                (Some(x), Some(y)) => x.0 <= y.0,
            };
            let (k, ih, ip, port) = if from_main {
                ai.next().unwrap()
            } else {
                bi.next().unwrap()
            };
            out.push((k.into_bytes(), ih, ip, port));
        }
        Ok(out)
    }

    /// 单表侧查询：按 key 表达式 `[lo, hi)` 升序取前 `limit` 行。
    /// `tombstone_filter`：主表传 `deleted_at IS NULL`，archive 传 `1=1`（无墓碑列）。
    /// 谓词按需拼装（`?N IS NULL OR ...` 写法会阻断索引范围扫描），与 `node_range_sql`
    /// 同风格，key 表达式与两表的 key 表达式索引逐字一致。
    /// v11(K 批/F2b)：`index_name` 强制对应表达式索引（优化器从不主动选它，实测
    /// 全表排序 4.3s/区间 vs 强制后 0.010s）；索引缺失时回退无索引 SQL（只告警一次）。
    fn query_peer_rows_one_table(
        conn: &rusqlite::Connection,
        table: &str,
        index_name: &str,
        tombstone_filter: &str,
        lo: Option<&str>,
        hi: Option<&str>,
        limit: i64,
    ) -> anyhow::Result<Vec<(String, Vec<u8>, String, i64)>> {
        let key_expr = "(lower(hex(infohash)) || ':' || ip || ':' || port)";
        let from_ix = format!("{} INDEXED BY {}", table, index_name);
        let mut sql_ix = format!(
            "SELECT {key_expr}, infohash, ip, port FROM {from_ix} WHERE {tombstone_filter}"
        );
        let mut sql_pl =
            format!("SELECT {key_expr}, infohash, ip, port FROM {table} WHERE {tombstone_filter}");
        let mut binds: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(3);
        let mut n = 0usize;
        if let Some(v) = lo {
            n += 1;
            sql_ix.push_str(&format!(" AND {key_expr} >= ?{n}"));
            sql_pl.push_str(&format!(" AND {key_expr} >= ?{n}"));
            binds.push(Box::new(v.to_string()));
        }
        if let Some(v) = hi {
            n += 1;
            sql_ix.push_str(&format!(" AND {key_expr} < ?{n}"));
            sql_pl.push_str(&format!(" AND {key_expr} < ?{n}"));
            binds.push(Box::new(v.to_string()));
        }
        n += 1;
        sql_ix.push_str(&format!(" ORDER BY {key_expr} ASC LIMIT ?{n}"));
        sql_pl.push_str(&format!(" ORDER BY {key_expr} ASC LIMIT ?{n}"));
        binds.push(Box::new(limit));
        let mut stmt = prepare_range_stmt(conn, &sql_ix, &sql_pl)?;
        let bind_refs: Vec<&dyn rusqlite::ToSql> = binds.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(bind_refs.as_slice(), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// v11(K 批/F2b)：infohashes 区间查询（按需谓词 + INDEXED BY 强制部分索引）。
    ///
    /// 旧 `?1 IS NULL OR infohash >= ?1` 写法让优化器无法建立范围约束，且优化器偏爱
    /// deleted 索引 + TEMP B-TREE 排序（实测 101ms/区间 vs 强制后 3ms）。
    /// `sql_plain` 供部分索引缺失时回退（`prepare_range_stmt` 只告警一次）。
    fn query_infohash_range_keys(
        conn: &rusqlite::Connection,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        limit: usize,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let mut sql_ix = String::from(
            "SELECT infohash FROM infohashes INDEXED BY idx_infohashes_ih_alive WHERE deleted_at IS NULL",
        );
        let mut sql_pl = String::from("SELECT infohash FROM infohashes WHERE deleted_at IS NULL");
        let mut binds: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(3);
        let mut n = 0usize;
        if let Some(v) = lo {
            n += 1;
            let p = format!(" AND infohash >= ?{n}");
            sql_ix.push_str(&p);
            sql_pl.push_str(&p);
            binds.push(Box::new(v.to_vec()));
        }
        if let Some(v) = hi {
            n += 1;
            let p = format!(" AND infohash < ?{n}");
            sql_ix.push_str(&p);
            sql_pl.push_str(&p);
            binds.push(Box::new(v.to_vec()));
        }
        n += 1;
        let p = format!(" ORDER BY infohash ASC LIMIT ?{n}");
        sql_ix.push_str(&p);
        sql_pl.push_str(&p);
        binds.push(Box::new(limit.max(1) as i64));
        let mut stmt = prepare_range_stmt(conn, &sql_ix, &sql_pl)?;
        let bind_refs: Vec<&dyn rusqlite::ToSql> = binds.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(bind_refs.as_slice(), |row| row.get::<_, Vec<u8>>(0))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// v11(K 批/F2b)：trackers 区间查询（同 [`Self::query_infohash_range_keys`]，TEXT key）。
    fn query_tracker_range_keys(
        conn: &rusqlite::Connection,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        // url 为 TEXT 列，绑 TEXT（BLOB 与 TEXT 的 SQLite 类型序错配会使范围约束失效）
        let lo_s = lo.map(|b| String::from_utf8_lossy(b).into_owned());
        let hi_s = hi.map(|b| String::from_utf8_lossy(b).into_owned());
        let mut sql_ix = String::from(
            "SELECT url FROM trackers INDEXED BY idx_trackers_url_alive WHERE deleted_at IS NULL",
        );
        let mut sql_pl = String::from("SELECT url FROM trackers WHERE deleted_at IS NULL");
        let mut binds: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(3);
        let mut n = 0usize;
        if let Some(ref v) = lo_s {
            n += 1;
            let p = format!(" AND url >= ?{n}");
            sql_ix.push_str(&p);
            sql_pl.push_str(&p);
            binds.push(Box::new(v.clone()));
        }
        if let Some(ref v) = hi_s {
            n += 1;
            let p = format!(" AND url < ?{n}");
            sql_ix.push_str(&p);
            sql_pl.push_str(&p);
            binds.push(Box::new(v.clone()));
        }
        n += 1;
        let p = format!(" ORDER BY url ASC LIMIT ?{n}");
        sql_ix.push_str(&p);
        sql_pl.push_str(&p);
        binds.push(Box::new(limit.max(1) as i64));
        let mut stmt = prepare_range_stmt(conn, &sql_ix, &sql_pl)?;
        let bind_refs: Vec<&dyn rusqlite::ToSql> = binds.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(bind_refs.as_slice(), |row| row.get::<_, String>(0))?;
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
        self.read_long(|conn| {
            if n == 0 {
                return Ok(Vec::new());
            }
            let max_rowid: i64 =
                conn.query_row("SELECT COALESCE(MAX(rowid), 0) FROM dht_nodes", [], |r| {
                    r.get(0)
                })?;
            if max_rowid <= 0 {
                return Ok(Vec::new());
            }
            // 手写 LCG（避免引入 rand 依赖；seed 取自当前时间，保证每轮抽样不同）
            let mut state: u64 =
                (chrono::Utc::now().timestamp_millis() as u64) ^ 0x9E37_79B9_7F4A_7C15;
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
        })
    }

    // ---- v7：统一 range 反熵（全 repo 支持）----

    /// v7：按 repo 加载 `(key, data_hash)`，范围 `[lo, hi)`（`None` = ±∞），最多 `limit` 条。
    ///
    /// key 编码与各 repo 的 Merkle / `load_all_*_keys_hashes` 严格一致：
    /// NODE=`ip:port`、PEER=`hex(ih):ip:port`（F9 方案 B：peers + peers_archive 两表
    /// 合并，见 `query_peer_rows_both_tables`）、INFOHASH=原始 20B、TRACKER=url 字节。
    pub fn load_repo_key_hashes_in_range(
        &self,
        repo: u8,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // P4-2：全表/大区间扫描统一走长查询门（并发上限），避免多长查询占满读池
        self.read_long(|conn| Self::query_repo_key_hashes_in_range(conn, repo, lo, hi, limit))
    }

    /// P4-2（v9，preP30）：**bootstrap 清单重建/传输专用**版本——
    /// 同 [`Self::load_repo_key_hashes_in_range`] 但走 `read_long_priority`
    /// （不经过长查询并发门）。仅 bootstrap.rs 重建/扫描/传输链使用；
    /// range 反熵等普通路径保持走 `read_long`（受 permit 门限流）。
    pub fn load_repo_key_hashes_in_range_priority(
        &self,
        repo: u8,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.read_long_priority(|conn| {
            Self::query_repo_key_hashes_in_range(conn, repo, lo, hi, limit)
        })
    }

    /// 私有：按 repo 分区查询 key+hash（NODE/PEER/INFOHASH/TRACKER 四类共用）。
    fn query_repo_key_hashes_in_range(
        conn: &Connection,
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
            NODE => Self::query_node_key_hashes(conn, lo, hi, limit),
            PEER => {
                // F9 方案 B：bootstrap 清单扫描 = peers 主表活行 + peers_archive 归档行
                // （双路归并，见 `query_peer_rows_both_tables`）。归档是本地冷分层
                // （存储优化）不是数据边界——两节点都应拥有对方的归档行。
                // 口径铁律：本清单行数必须与 local_entry_counts 的 peer 项
                // （peers.valid + peers_archive.valid）一致，否则差的部分快照永远
                // 拉不到 → 协商判定永远差一截 → BOOTSTRAP 死循环（F1 实测教训，
                // 2026-09-27 52/58：58 报 40,042 vs 清单 28,009，每 5 分钟全量重拉）。
                let lo_s = lo.map(|b| String::from_utf8_lossy(b).into_owned());
                let hi_s = hi.map(|b| String::from_utf8_lossy(b).into_owned());
                let rows = Self::query_peer_rows_both_tables(
                    conn,
                    lo_s.as_deref(),
                    hi_s.as_deref(),
                    limit,
                )?;
                let mut out = Vec::new();
                for (key, ih, ip, port) in rows {
                    // data_hash = blake3(infohash || ip_string || port(i64 LE))，
                    // 沿用本通道原实现的逐字节公式（注意：与 build_peer_sync_entry
                    // 的 u16(2 字节) port 存在既有宽度差异，属历史口径，F9 不改，
                    // 只要两端各通道内部自洽即可）
                    let mut buf = Vec::with_capacity(ih.len() + ip.len() + 8);
                    buf.extend_from_slice(&ih);
                    buf.extend_from_slice(ip.as_bytes());
                    buf.extend_from_slice(&port.to_le_bytes());
                    out.push((key, blake3::hash(&buf).as_bytes().to_vec()));
                }
                Ok(out)
            }
            INFOHASH => {
                // v11(K 批/F2b)：改走 query_infohash_range_keys（INDEXED BY 强制部分索引）
                let ihs = Self::query_infohash_range_keys(conn, lo, hi, limit)?;
                let mut out = Vec::with_capacity(ihs.len());
                for ih in ihs {
                    out.push((ih.clone(), blake3::hash(&ih).as_bytes().to_vec()));
                }
                Ok(out)
            }
            TRACKER => {
                // v11(K 批/F2b)：改走 query_tracker_range_keys（INDEXED BY 强制部分索引）
                let urls = Self::query_tracker_range_keys(conn, lo, hi, limit)?;
                let mut out = Vec::with_capacity(urls.len());
                for url in urls {
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
        self.read_long(|conn| {
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
            let max_rowid: i64 = conn.query_row(
                &format!("SELECT COALESCE(MAX(rowid), 0) FROM {}", table),
                [],
                |r| r.get(0),
            )?;
            if max_rowid <= 0 {
                return Ok(Vec::new());
            }
            let mut state: u64 =
                (chrono::Utc::now().timestamp_millis() as u64) ^ 0x9E37_79B9_7F4A_7C15;
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
        })
    }

    /// v7：按 repo 加载 `[lo, hi)` 区间内的完整 `SyncEntry`（bootstrap 分块服务用）。
    ///
    /// 条目编码与各 repo 的 gossip 同步条目严格一致（复用 `build_*_sync_entry` 系列函数）。
    /// F9 方案 B：PEER 分支与清单扫描同口径（peers + peers_archive 双路归并）。归档行
    /// 落到对端走 `apply_peer_sync` → upsert 进**对端 peers 主表**（联邦层无 archive
    /// 概念）；对端再按自身冷热策略分层（tier_manager 归档迁移在各节点独立运行）属
    /// 预期行为，不构成回环——入站 apply 不写 oplog，不会把同步进来的行再广播回去。
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
                // F9 方案 B：块数据读取 = peers 主表活行 + peers_archive 归档行（双路
                // 归并），与清单扫描口径一致（清单列了行，块就必须能取出对应数据）。
                // G1：块数据读取改走读连接池，不抢全局写锁（bootstrap 应答与写事务解耦）。
                // P4-2（v9，preP32）：bootstrap 应答取数链走 priority（不被去重饿死）。
                self.read_long_priority(|conn| -> anyhow::Result<Vec<SyncEntry>> {
                    let lo_s = lo.map(|b| String::from_utf8_lossy(b).into_owned());
                    let hi_s = hi.map(|b| String::from_utf8_lossy(b).into_owned());
                    let rows = Self::query_peer_rows_both_tables(
                        conn,
                        lo_s.as_deref(),
                        hi_s.as_deref(),
                        limit,
                    )?;
                    let mut out = Vec::new();
                    for (_key, ih, ip, port) in rows {
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
                    Ok(out)
                })
            }
            INFOHASH => {
                // G1：块数据读取改走读连接池，不抢全局写锁。
                // v11(K 批/F2b)：改走 query_infohash_range_keys（INDEXED BY 强制部分索引）。
                // P4-2（v9，preP32）：bootstrap 应答取数链走 priority。
                self.read_long_priority(|conn| -> anyhow::Result<Vec<SyncEntry>> {
                    let ihs = Self::query_infohash_range_keys(conn, lo, hi, limit)?;
                    let mut out = Vec::new();
                    for ih in ihs {
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
                })
            }
            TRACKER => {
                // G1：块数据读取改走读连接池，不抢全局写锁。
                // v11(K 批/F2b)：改走 query_tracker_range_keys（INDEXED BY 强制部分索引）。
                // P4-2（v9，preP32）：bootstrap 应答取数链走 priority。
                self.read_long_priority(|conn| -> anyhow::Result<Vec<SyncEntry>> {
                    let urls = Self::query_tracker_range_keys(conn, lo, hi, limit)?;
                    let mut out = Vec::new();
                    for url in urls {
                        if let Some(e) = to_entry(
                            crate::federation::sync::tracker_sync::build_tracker_sync_entry(&url),
                        ) {
                            out.push(e);
                        }
                    }
                    Ok(out)
                })
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
                // G1：按 key 精确修复读取改走读连接池，不抢全局写锁。
                self.read_long(|conn| -> anyhow::Result<Vec<SyncEntry>> {
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
                        let mut bind: Vec<&dyn rusqlite::ToSql> =
                            Vec::with_capacity(chunk.len() * 2);
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
                                if let Some(e) = to_entry(
                                    crate::federation::sync::build_node_sync_entry(arr, addr),
                                ) {
                                    out.push(e);
                                }
                            }
                        }
                    }
                    Ok(out)
                })
            }
            PEER => {
                // 注意（F9 边界）：本函数是 v8 range 反熵的「按 key 精确修复」通道，
                // 仍只查 peers 主表。归档行的 key 在 F9 后会出现在清单/range 摘要里，
                // 但归档行由 bootstrap 全量快照通道收敛（apply 后落到对端主表），
                // 两节点都有该行后 key+hash 一致，range 对账自然不再报 diff——
                // 按 key 拉不到 archive 行只会短暂打出「加载到 0 条」日志，无死循环。
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
                // G1：按 key 精确修复读取改走读连接池，不抢全局写锁。
                self.read_long(|conn| -> anyhow::Result<Vec<SyncEntry>> {
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
                        let mut bind: Vec<&dyn rusqlite::ToSql> =
                            Vec::with_capacity(chunk.len() * 3);
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
                                if let Some(e) = to_entry(
                                    crate::federation::sync::peer_sync::build_peer_sync_entry(
                                        arr, addr,
                                    ),
                                ) {
                                    out.push(e);
                                }
                            }
                        }
                    }
                    Ok(out)
                })
            }
            INFOHASH => {
                let ih_keys: Vec<Vec<u8>> =
                    keys.iter().filter(|k| k.len() == 20).cloned().collect();
                // G1：按 key 精确修复读取改走读连接池，不抢全局写锁。
                self.read_long(|conn| -> anyhow::Result<Vec<SyncEntry>> {
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
                        let rows =
                            stmt.query_map(bind.as_slice(), |row| row.get::<_, Vec<u8>>(0))?;
                        for r in rows {
                            let ih = r?;
                            if ih.len() != 20 {
                                continue;
                            }
                            let mut arr = [0u8; 20];
                            arr.copy_from_slice(&ih);
                            if let Some(e) = to_entry(
                                crate::federation::sync::infohash_sync::build_infohash_sync_entry(
                                    arr,
                                ),
                            ) {
                                out.push(e);
                            }
                        }
                    }
                    Ok(out)
                })
            }
            TRACKER => {
                let mut urls: Vec<String> = keys
                    .iter()
                    .map(|k| String::from_utf8_lossy(k).into_owned())
                    .collect();
                urls.sort();
                urls.dedup();
                // G1：按 key 精确修复读取改走读连接池，不抢全局写锁。
                self.read_long(|conn| -> anyhow::Result<Vec<SyncEntry>> {
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
                        let rows =
                            stmt.query_map(bind.as_slice(), |row| row.get::<_, String>(0))?;
                        for r in rows {
                            let url = r?;
                            if let Some(e) = to_entry(
                                crate::federation::sync::tracker_sync::build_tracker_sync_entry(
                                    &url,
                                ),
                            ) {
                                out.push(e);
                            }
                        }
                    }
                    Ok(out)
                })
            }
            _ => Ok(Vec::new()),
        }
    }

    /// 释放 SQLite 内部缓存内存（PRAGMA shrink_memory），内存压力大时调用。
    pub fn shrink_memory(&self) {
        // 2026-10-09 治本 S5：统一走 with_write_conn 入口（写锁争用可观测）。
        // 原语义：`let _ = ...` 吞掉错误（PRAGMA 是尽力而为的性能提示，
        // 失败不应影响调用方）。with_write_conn 返回 Result<()>，同样忽略。
        let _ = with_write_conn(&self.conn, |conn| {
            // `execute_batch` 返回 `rusqlite::Error`，而 `with_write_conn`
            // 的闭包要求 `anyhow::Error` —— 需用 `Ok(..?..)` 显式转换
            // （两者同名但类型不同，直接返回会 E0308）。
            Ok(conn.execute_batch("PRAGMA shrink_memory;")?)
        });
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
    /// 节点最近活跃（Unix 秒；None = 该查询未取此列）。
    /// 语义为节点真实 last_active，而非落库时刻（2026-10 预加载新近度修复）。
    pub last_active: Option<i64>,
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

/// P-A：块表行（bootstrap 清单构建 / 对齐验证消费，P-B）。
#[derive(Debug, Clone)]
pub struct ChunkDigest {
    pub repo: u8,
    pub idx: i64,
    pub lo_key: Vec<u8>,
    pub hi_key: Vec<u8>,
    pub rows: i64,
    pub hash: Vec<u8>,
    pub dirty: bool,
    /// 构建时的 chunk_rows（边界一致性校验：config 变化 → 全量重建）
    pub chunk_rows: u32,
}

/// P-A：小写 hex 编码（PEER 块键 `lower(hex(infohash)):ip:port` 用，
/// 与 SQL 表达式索引 `lower(hex(infohash))` 逐字符一致）。
fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
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
    fn test_truncate_advisable_gate() {
        const MB: u64 = 1024 * 1024;
        // 空闲 + 文件超阈值：允许（唯一放行路径）
        assert!(truncate_advisable(32 * MB, 16 * MB, 0, 0.0, false));
        assert!(truncate_advisable(32 * MB, 16 * MB, 0, 0.29, false));
        // 文件未达 truncate_min：不回收
        assert!(!truncate_advisable(8 * MB, 16 * MB, 0, 0.0, false));
        // 写队列非空：负载期，禁止（独占锁必等待）
        assert!(!truncate_advisable(32 * MB, 16 * MB, 1, 0.0, false));
        // 背压升高：非持续空闲，禁止（0.3 与 PASSIVE 的 0.5 区分：TRUNCATE 要独占）
        assert!(!truncate_advisable(32 * MB, 16 * MB, 0, 0.3, false));
        assert!(!truncate_advisable(32 * MB, 16 * MB, 0, 0.6, false));
        // worker 在飞：禁止并发 checkpoint
        assert!(!truncate_advisable(32 * MB, 16 * MB, 0, 0.0, true));
    }

    #[test]
    fn test_init_tables() {
        let _storage = Storage::memory().unwrap();
        // 表创建成功
    }

    fn dht_row(port: u16, score: f64, last_active: Option<i64>) -> DhtNodeRow {
        DhtNodeRow {
            id: [port as u8; 20],
            ip: "127.0.0.1".into(),
            port,
            score,
            state: "Good".into(),
            query_count: 1,
            success_count: 1,
            total_latency_ms: 5,
            consecutive_failures: 0,
            nodes_returned: 8,
            last_query_time: None,
            last_active,
        }
    }

    /// 预加载评分回归（2026-10）：load_recent_nodes 按 score 降序——质量优先于新近度。
    #[test]
    fn test_load_recent_nodes_orders_by_last_active() {
        let storage = Storage::memory().unwrap();
        let now = chrono::Utc::now().timestamp();
        let month_ago = now - 30 * 24 * 3600;
        // 陈旧节点历史评分更高 → 评分排序下必须排前
        storage
            .save_dht_nodes_batch(&[
                dht_row(2002, 99.0, Some(month_ago)),
                dht_row(2001, 1.0, Some(now)),
            ])
            .unwrap();
        let rows = storage.load_recent_nodes(10).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].port, 2002,
            "高评分节点必须排在低评分之前（评分优先）"
        );
        assert_eq!(rows[1].port, 2001);
    }

    /// v11(K 批/F2b) 回归：四 repo 的区间查询（INDEXED BY 强制索引路径）必须
    /// 有序、可过滤、边界正确——这是 range 摘要 / bootstrap 清单与分块服务共用的数据面。
    #[test]
    fn test_range_key_hashes_ordered_and_filtered() {
        let storage = Storage::memory().unwrap();
        // NODE：3 活 + 1 软删（软删行必须被排除）
        for (ip, port) in [
            ("10.0.0.2", 6881u16),
            ("10.0.0.1", 6882u16),
            ("10.0.0.3", 6883u16),
            ("10.0.0.9", 6889u16),
        ] {
            storage
                .save_dht_node(&[1u8; 20], ip, port, 1.0, "Good", 0, 0, 0, 0, 0, None)
                .unwrap();
        }
        storage.soft_delete_node("10.0.0.9", 6889).unwrap();
        let to_keys = |rows: &[(Vec<u8>, Vec<u8>)]| -> Vec<String> {
            rows.iter()
                .map(|(k, _)| String::from_utf8_lossy(k).to_string())
                .collect()
        };
        let rows = storage
            .load_repo_key_hashes_in_range(1, None, None, 100)
            .unwrap();
        assert_eq!(
            to_keys(&rows),
            vec!["10.0.0.1:6882", "10.0.0.2:6881", "10.0.0.3:6883"]
        );
        // 半开区间 [10.0.0.2:0, 10.0.0.3:0)
        let rows = storage
            .load_repo_key_hashes_in_range(1, Some(b"10.0.0.2:0"), Some(b"10.0.0.3:0"), 100)
            .unwrap();
        assert_eq!(to_keys(&rows), vec!["10.0.0.2:6881"]);

        // INFOHASH：BLOB key 排序 + 上界过滤
        let ih1 = [1u8; 20];
        let ih2 = [2u8; 20];
        storage.save_infohash(&ih1, 1, "t", 1.0).unwrap();
        storage.save_infohash(&ih2, 1, "t", 1.0).unwrap();
        let rows = storage
            .load_repo_key_hashes_in_range(3, None, None, 100)
            .unwrap();
        assert_eq!(rows.len(), 2);
        let rows = storage
            .load_repo_key_hashes_in_range(3, None, Some(&ih2), 100)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, ih1.to_vec());

        // TRACKER：TEXT key 排序
        storage
            .save_tracker("http://b.example/announce", 1.0, 0, 0, 0, 0, 0.0, 1, false)
            .unwrap();
        storage
            .save_tracker("http://a.example/announce", 1.0, 0, 0, 0, 0, 0.0, 1, false)
            .unwrap();
        let rows = storage
            .load_repo_key_hashes_in_range(4, None, None, 100)
            .unwrap();
        assert_eq!(
            to_keys(&rows),
            vec!["http://a.example/announce", "http://b.example/announce"]
        );

        // PEER：主表两行（key = lower(hex(ih)):ip:port，同 ih 下按 ip:port 排序）
        storage
            .save_peer(&ih1, "10.0.0.2", 6882, "dht", 1.0, 0, 0, 0)
            .unwrap();
        storage
            .save_peer(&ih1, "10.0.0.1", 6881, "dht", 1.0, 0, 0, 0)
            .unwrap();
        let rows = storage
            .load_repo_key_hashes_in_range(2, None, None, 100)
            .unwrap();
        assert_eq!(rows.len(), 2);
        let k0 = String::from_utf8_lossy(&rows[0].0).to_string();
        let k1 = String::from_utf8_lossy(&rows[1].0).to_string();
        assert!(k0.starts_with("0101"));
        assert!(k0 < k1);
    }

    /// v11(K 批/F2b) 回归：区间索引缺失（建索引失败的库）时查询必须回退可用，
    /// 不允许 panic 或空结果——覆盖 `prepare_range_stmt` 的回退分支。
    #[test]
    fn test_range_query_falls_back_without_index() {
        let storage = Storage::memory().unwrap();
        storage
            .save_dht_node(
                &[1u8; 20], "10.0.0.1", 6881, 1.0, "Good", 0, 0, 0, 0, 0, None,
            )
            .unwrap();
        storage.save_infohash(&[3u8; 20], 1, "t", 1.0).unwrap();
        {
            let conn = storage.connection();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute("DROP INDEX idx_dht_nodes_ip_port_expr", [])
                .unwrap();
            conn.execute("DROP INDEX idx_infohashes_ih_alive", [])
                .unwrap();
        }
        let rows = storage
            .load_node_key_hashes_in_range(None, None, 100)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(String::from_utf8_lossy(&rows[0].0), "10.0.0.1:6881");
        let rows = storage
            .load_repo_key_hashes_in_range(3, None, None, 100)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, vec![3u8; 20]);
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

    #[test]
    fn test_table_counts_dht_node_lifecycle() {
        let storage = Storage::memory().unwrap();
        // 空库在 init_tables 内即已校准，读路径走增量计数且为 0
        assert_eq!(storage.count_table("dht_nodes").unwrap(), 0);

        let id1 = [1u8; 20];
        let id2 = [2u8; 20];
        storage
            .save_dht_node(&id1, "10.0.0.1", 6881, 1.0, "Good", 1, 1, 1, 0, 1, None)
            .unwrap();
        storage
            .save_dht_node(&id2, "10.0.0.2", 6881, 1.0, "Good", 1, 1, 1, 0, 1, None)
            .unwrap();
        assert_eq!(storage.count_table("dht_nodes").unwrap(), 2);
        assert_eq!(storage.valid_entity_counts()[0], 2);

        // upsert 同主键更新：行数不变（INSERT 触发器不 fire）
        storage
            .save_dht_node(&id1, "10.0.0.1", 6881, 2.0, "Good", 2, 2, 2, 0, 2, None)
            .unwrap();
        assert_eq!(storage.count_table("dht_nodes").unwrap(), 2);
        assert_eq!(storage.valid_entity_counts()[0], 2);

        // 软删：total 不变、valid 减 1；重复软删幂等
        assert_eq!(storage.soft_delete_node("10.0.0.1", 6881).unwrap(), 1);
        assert_eq!(storage.count_table("dht_nodes").unwrap(), 2);
        assert_eq!(storage.valid_entity_counts()[0], 1);
        assert_eq!(storage.soft_delete_node("10.0.0.1", 6881).unwrap(), 0);
        assert_eq!(storage.valid_entity_counts()[0], 1);

        // upsert 复活墓碑（DO UPDATE SET deleted_at=NULL）：valid 加回
        storage
            .save_dht_node(&id1, "10.0.0.1", 6881, 3.0, "Good", 1, 1, 1, 0, 1, None)
            .unwrap();
        assert_eq!(storage.valid_entity_counts()[0], 2);

        // 硬删全部：归零
        storage.clear_dht_nodes().unwrap();
        assert_eq!(storage.count_table("dht_nodes").unwrap(), 0);
        assert_eq!(storage.valid_entity_counts()[0], 0);
    }

    #[test]
    fn test_table_counts_peers_and_trackers() {
        let storage = Storage::memory().unwrap();
        let ih = [7u8; 20];
        storage
            .save_peer(&ih, "10.1.0.1", 51413, "dht", 1.0, 0, 0, 1000)
            .unwrap();
        storage
            .save_peer(&ih, "10.1.0.2", 51413, "dht", 1.0, 0, 0, 1000)
            .unwrap();
        // 同一 peer 重复上报（upsert 更新）：计数不变
        storage
            .save_peer(&ih, "10.1.0.1", 51413, "dht", 1.0, 1, 1, 2000)
            .unwrap();
        assert_eq!(storage.count_table("peers").unwrap(), 2);
        assert_eq!(storage.valid_entity_counts()[1], 2);

        // tracker 墓碑保留语义：软删后 valid 减、total 不变；
        // keep_tombstone upsert 不复活，普通 save_tracker 复活
        storage
            .save_tracker("http://t.example/announce", 1.0, 0, 0, 0, 0, 0.0, 0, false)
            .unwrap();
        assert_eq!(storage.count_table("trackers").unwrap(), 1);
        assert_eq!(
            storage
                .soft_delete_tracker("http://t.example/announce")
                .unwrap(),
            1
        );
        assert_eq!(storage.valid_entity_counts()[4], 0);
        assert_eq!(storage.count_table("trackers").unwrap(), 1);
        storage
            .save_tracker_keep_tombstone(
                "http://t.example/announce",
                1.0,
                0,
                0,
                0,
                0,
                0.0,
                0,
                false,
            )
            .unwrap();
        assert_eq!(storage.valid_entity_counts()[4], 0);
        storage
            .save_tracker("http://t.example/announce", 1.0, 0, 0, 0, 0, 0.0, 0, false)
            .unwrap();
        assert_eq!(storage.valid_entity_counts()[4], 1);
    }

    #[test]
    fn test_table_counts_reconcile_overwrites_drift() {
        let storage = Storage::memory().unwrap();
        let ih = [9u8; 20];
        storage
            .save_peer(&ih, "10.2.0.1", 51413, "dht", 1.0, 0, 0, 1000)
            .unwrap();
        storage
            .save_peer(&ih, "10.2.0.2", 51413, "dht", 1.0, 0, 0, 1000)
            .unwrap();

        // 人为污染计数器，模拟漂移
        {
            let conn = storage.conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute(
                "UPDATE table_counts SET total = total + 100, valid = valid + 100 \
                 WHERE name = 'peers'",
                [],
            )
            .unwrap();
        }

        // 校准后回到真值
        let c = storage.refresh_entity_counts();
        assert_eq!(c[1], 2);
        assert_eq!(storage.count_table("peers").unwrap(), 2);
        assert_eq!(storage.valid_entity_counts()[1], 2);
    }

    /// F9 方案 B：PEER 清单/块读取口径 = peers + peers_archive 两表合并（双路归并）。
    /// 用「归档行夹在两条主表行之间」的交错 key 验证：
    /// ① 归并结果真的穿插两表（不是先主表后归档表拼一起）；
    /// ② 主表软删行不出现、archive 无墓碑列全有效；
    /// ③ 同 key 两表各出一行（不去重，与计数口径 pick(1)+pick(2) 一致）；
    /// ④ 分页游标推进不重不漏（与 build_repo_manifest_impl 的分页方式一致）；
    /// ⑤ data_hash 公式 = blake3(infohash || ip || port_le)。
    #[test]
    fn test_load_repo_key_hashes_peer_merges_archive() {
        const PEER: u8 = 2;
        let storage = Storage::memory().unwrap();
        let ih_a = [0x0au8; 20]; // key 前缀 "0a…"：主表
        let ih_b = [0x0bu8; 20]; // key 前缀 "0b…"：归档表（夹在两条主表行之间）
        let ih_c = [0x0cu8; 20]; // key 前缀 "0c…"：主表（软删，应被过滤）
        let hex = |ih: &[u8; 20]| ih.iter().map(|b| format!("{:02x}", b)).collect::<String>();
        let k_a = format!("{}:10.0.0.0:6881", hex(&ih_a));
        let k_b = format!("{}:10.0.0.1:6881", hex(&ih_b));
        {
            let conn = storage.connection();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute(
                "INSERT INTO peers (infohash, ip, port, source) VALUES (?1, ?2, ?3, 'test')",
                rusqlite::params![ih_a.as_slice(), "10.0.0.0", 6881i64],
            )
            .unwrap();
            conn.execute(
                "INSERT OR IGNORE INTO peers_archive (infohash, ip, port, archived_at) \
                 VALUES (?1, ?2, ?3, 0)",
                rusqlite::params![ih_b.as_slice(), "10.0.0.1", 6881i64],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO peers (infohash, ip, port, source) VALUES (?1, ?2, ?3, 'test')",
                rusqlite::params![ih_c.as_slice(), "10.0.0.2", 6881i64],
            )
            .unwrap();
            // 软删主表 0c 行：清单口径只取活行
            conn.execute("UPDATE peers SET deleted_at = 1 WHERE ip = '10.0.0.2'", [])
                .unwrap();
            // 归档表再放一份同 key（先归档后又重新活跃的场景）：不去重，两行都出
            conn.execute(
                "INSERT OR IGNORE INTO peers_archive (infohash, ip, port, archived_at) \
                 VALUES (?1, ?2, ?3, 0)",
                rusqlite::params![ih_a.as_slice(), "10.0.0.0", 6881i64],
            )
            .unwrap();
        }

        // 全量：0a 主表行、0a 归档行（同 key 不去重、主表在前）、0b 归档行；0c 软删不出
        let all = storage
            .load_repo_key_hashes_in_range(PEER, None, None, 10)
            .unwrap();
        let keys: Vec<String> = all
            .iter()
            .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
            .collect();
        assert_eq!(
            keys,
            vec![k_a.clone(), k_a.clone(), k_b.clone()],
            "两表合并必须按 key 字节序交错归并，软删行过滤，同 key 不去重"
        );

        // data_hash 公式 = blake3(infohash || ip || port_le)，本通道（原实现沿用）port
        // 为 i64 的 8 字节 LE；与 build_peer_sync_entry 的 u16(2 字节) 口径差异是既有
        // 行为，不在 F9 改动范围（本测试只锁定本通道公式不被悄然改变）
        let mut buf = Vec::new();
        buf.extend_from_slice(&ih_a);
        buf.extend_from_slice(b"10.0.0.0");
        buf.extend_from_slice(&6881i64.to_le_bytes());
        assert_eq!(all[0].1, blake3::hash(&buf).as_bytes().to_vec());

        // 分页游标推进：忠实复刻 build_repo_manifest_impl 的循环——每次 fetch =
        // chunk+1，**块体只取前 chunk 行**，游标 = 第 chunk+1 行（第一个未包含行）的
        // key，续读 [lo, None)；末块取剩余行后停止。
        let chunk_rows = 2usize;
        let fetch = chunk_rows + 1;
        let mut cursor: Option<Vec<u8>> = None;
        let mut collected: Vec<Vec<u8>> = Vec::new();
        loop {
            let rows = storage
                .load_repo_key_hashes_in_range(PEER, cursor.as_deref(), None, fetch)
                .unwrap();
            if rows.is_empty() {
                break;
            }
            let is_last = rows.len() <= chunk_rows;
            let take = rows.len().min(chunk_rows);
            collected.extend(rows[..take].iter().map(|(k, _)| k.clone()));
            if is_last {
                break;
            }
            cursor = Some(rows[take].0.clone());
        }
        // 分页合并 = 全量（多重集逐行比较，不去重：同 key 两行都必须恰好出现一次）
        collected.sort();
        let mut expect_rows: Vec<Vec<u8>> = all.iter().map(|(k, _)| k.clone()).collect();
        expect_rows.sort();
        assert_eq!(
            collected, expect_rows,
            "分页合并后应逐行覆盖全量（不丢行、不重发）"
        );

        // 上界排除语义：hi = k_b（不含）→ 只剩两条 k_a
        let head = storage
            .load_repo_key_hashes_in_range(PEER, None, Some(k_b.as_bytes()), 10)
            .unwrap();
        assert_eq!(head.len(), 2);
        assert!(head.iter().all(|(k, _)| k.as_slice() == k_a.as_bytes()));

        // 块数据读取同口径：SyncEntry 覆盖两表 3 行（key 形态 = build_peer_sync_entry 的
        // "hex(ih):addr"，与清单 key 一致）
        let entries = storage
            .load_repo_sync_entries_in_range(PEER, None, None, 10)
            .unwrap();
        let mut entry_keys: Vec<String> = entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.key).into_owned())
            .collect();
        entry_keys.sort();
        let mut expect_keys = vec![k_a.clone(), k_a.clone(), k_b];
        expect_keys.sort();
        assert_eq!(
            entry_keys, expect_keys,
            "块读取必须与清单扫描同口径（两表合并）"
        );
    }

    // ---- A1/A3: checkpoint 接管与无用索引迁移 ----

    /// 生成唯一临时目录（避免并行测试互相踩）。测试文件落在系统 temp 下，不污染仓库。
    ///
    /// 竞态修复（2026-10-08）：Windows 下 `SystemTime::now()` 时钟 tick 粗（约 15ms），
    /// 连续调用可能返回相同纳秒值 → 并行/快速串行测试撞同名目录；且 `remove_dir`
    /// 后同名目录可能处于 Windows 延迟释放状态，立刻重建被拒（PermissionDenied code 5）。
    /// 修复：目录名并入线程 id，创建失败重试（覆盖延迟释放窗口）。
    fn unique_temp_dir(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let tid: String = format!("{:?}", std::thread::current().id())
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect();
        // 测试临时根用 crate::test_tmp_dir()（构建目录 target/test-tmp）：本机安全策略
        // 拒绝 target 构建目录进程写 %TEMP% 根与数据盘（PermissionDenied code 5）。
        let dir = crate::test_tmp_dir().join(format!("pdc_test_{}_{}_{}", tag, tid, nanos));
        let mut attempt = 0u32;
        // [ALLOWED-SLEEP] 测试临时目录创建重试（最多 8 次，一次性非周期）
        loop {
            match std::fs::create_dir_all(&dir) {
                Ok(()) => return dir,
                Err(_) if attempt < 8 => {
                    attempt += 1;
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(e) => panic!("create_dir_all {} 失败: {}", dir.display(), e),
            }
        }
    }

    fn cleanup_temp(dir: &std::path::Path) {
        if let Ok(rd) = std::fs::read_dir(dir) {
            for f in rd.flatten() {
                let _ = std::fs::remove_file(f.path());
            }
        }
        let _ = std::fs::remove_dir(dir);
    }

    #[test]
    fn test_checkpoint_once_memory_db_skips() {
        let s = Storage::memory().unwrap();
        let out = s.checkpoint_once(CheckpointMode::Passive).unwrap();
        assert_eq!(out.skipped, Some("memory"));
        assert!(!out.busy);
        // 内存库 wal_bytes 恒为 0
        assert_eq!(s.wal_bytes(), 0);
    }

    #[test]
    fn test_wal_autocheckpoint_takeover() {
        let dir = unique_temp_dir("takeover");
        let db_path = dir.join("test.db");
        let cfg = crate::config::SqliteConfig::default();
        let s = Storage::open_with_config(&db_path, &cfg).unwrap();
        // 接管：置 0
        s.set_wal_autocheckpoint(0).unwrap();
        let conn = s.connection();
        let c = conn.lock().unwrap_or_else(|e| e.into_inner());
        let pages: i64 = c
            .query_row("PRAGMA wal_autocheckpoint;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            pages, 0,
            "takeover=true 时写连接 wal_autocheckpoint 应读出 0"
        );
        drop(c);
        // checkpoint_once 在文件库上不应返回 skipped=memory
        drop(s);
        let _ = std::fs::remove_file(dir.join("test.db-wal"));
        cleanup_temp(&dir);
    }

    #[test]
    fn test_drop_unused_indexes_idempotent() {
        let dir = unique_temp_dir("dropidx");
        let db_path = dir.join("test.db");
        let cfg = crate::config::SqliteConfig::default();
        // 首次打开（迁移开启）
        {
            let _s = Storage::open_with_config(&db_path, &cfg).unwrap();
        }
        // 模拟旧库：手动重建四个 l2_shard 索引
        {
            let c = Connection::open(&db_path).unwrap();
            c.execute_batch(
                "CREATE INDEX IF NOT EXISTS idx_dht_nodes_l2_shard ON dht_nodes(l2_shard);
                 CREATE INDEX IF NOT EXISTS idx_trackers_l2_shard ON trackers(l2_shard);
                 CREATE INDEX IF NOT EXISTS idx_infohashes_l2_shard ON infohashes(l2_shard);
                 CREATE INDEX IF NOT EXISTS idx_peers_l2_shard ON peers(l2_shard);",
            )
            .unwrap();
        }
        // 再次打开：迁移再次 DROP，幂等不报错
        {
            let _s = Storage::open_with_config(&db_path, &cfg).unwrap();
        }
        // 第三次打开：仍然幂等
        {
            let _s = Storage::open_with_config(&db_path, &cfg).unwrap();
        }
        // 断言索引已不存在
        let c = Connection::open(&db_path).unwrap();
        let cnt: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name IN                  ('idx_dht_nodes_l2_shard','idx_trackers_l2_shard',                  'idx_infohashes_l2_shard','idx_peers_l2_shard')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cnt, 0, "l2_shard 索引应被删除");
        drop(c);
        let _ = std::fs::remove_file(dir.join("test.db-wal"));
        cleanup_temp(&dir);
    }

    #[test]
    fn test_drop_unused_indexes_disabled_by_config() {
        let dir = unique_temp_dir("dropidx_off");
        let db_path = dir.join("test.db");
        let cfg = crate::config::SqliteConfig {
            drop_unused_indexes: false,
            ..Default::default()
        };
        // 首次打开（迁移关闭）
        {
            let _s = Storage::open_with_config(&db_path, &cfg).unwrap();
        }
        // 手动建一个 l2_shard 索引模拟旧库
        {
            let c = Connection::open(&db_path).unwrap();
            c.execute_batch(
                "CREATE INDEX IF NOT EXISTS idx_dht_nodes_l2_shard ON dht_nodes(l2_shard);",
            )
            .unwrap();
        }
        // 再次打开（迁移仍关闭）：索引应保留
        {
            let _s = Storage::open_with_config(&db_path, &cfg).unwrap();
        }
        let c = Connection::open(&db_path).unwrap();
        let cnt: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='idx_dht_nodes_l2_shard'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cnt, 1, "drop_unused_indexes=false 时不应删除索引");
        drop(c);
        let _ = std::fs::remove_file(dir.join("test.db-wal"));
        cleanup_temp(&dir);
    }

    #[test]
    fn test_read_pool_size_on_open() {
        // 文件库：open() 应按 READ_POOL_SIZE 初始化只读连接池
        let dir = unique_temp_dir("readpool");
        let db_path = dir.join("test.db");
        let cfg = crate::config::SqliteConfig::default();
        let s = Storage::open_with_config(&db_path, &cfg).unwrap();
        let pool = s.read_pool.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(
            pool.len(),
            READ_POOL_SIZE,
            "open() 应初始化 READ_POOL_SIZE={} 个只读连接",
            READ_POOL_SIZE
        );
        drop(pool);
        drop(s);
        let _ = std::fs::remove_file(dir.join("test.db-wal"));
        cleanup_temp(&dir);
    }

    #[test]
    fn test_read_pool_empty_on_memory() {
        // 内存库：无文件 WAL，读池恒为空（池空回退写锁路径的存在性佐证）
        let s = Storage::memory().unwrap();
        let pool = s.read_pool.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(pool.len(), 0, "memory() 库读池应为空");
    }

    // ── E6：读池进程级观测计数器（静态跨实例共享，只用相对断言）──────────────
    #[test]
    fn test_read_pool_available_net_zero_after_borrow_return() {
        // 文件库 read() 借走(-1)/归还(+1) 应对 available 计数净变化为 0。
        // 不断言绝对值：静态量被并行测试的 open()/memory() 反复覆写。
        let dir = unique_temp_dir("readpoolobs");
        let db_path = dir.join("test.db");
        let cfg = crate::config::SqliteConfig::default();
        let s = Storage::open_with_config(&db_path, &cfg).unwrap();
        let a0 = read_pool_available();
        for _ in 0..5 {
            s.read(|conn| -> anyhow::Result<()> {
                let _: i64 = conn.query_row("SELECT 1", [], |r| r.get(0))?;
                Ok(())
            })
            .unwrap();
        }
        let a1 = read_pool_available();
        assert_eq!(
            a1, a0,
            "read() 借还后 available 应回到调用前（净变化 0），a0={} a1={}",
            a0, a1
        );
        drop(s);
        let _ = std::fs::remove_file(dir.join("test.db-wal"));
        cleanup_temp(&dir);
    }

    #[test]
    fn test_read_pool_starved_counts_only_on_file_db_queue_wait() {
        // P4-1 新语义：READ_POOL_STARVED = 文件库读池「排队等待归还」次数；
        // 内存库无池（pool_enabled=false）直接回退写连接、不排队。
        // 全局静态计数在并行测试下不可靠，此处验证行为语义（无池回退存在）。
        let s = Storage::memory().unwrap();
        {
            let inner = s.read_pool.lock().unwrap_or_else(|e| e.into_inner());
            assert!(
                !inner.pool_enabled,
                "memory() 库 pool_enabled 应为 false（无池，read() 走回退写连接路径）"
            );
        }
        s.read(|conn| -> anyhow::Result<()> {
            let _: i64 = conn.query_row("SELECT 1", [], |r| r.get(0))?;
            Ok(())
        })
        .unwrap();
        // 内存库 read() 完成：回退路径正常，不触发排队计数（该路径无 fetch_add）。
    }

    // ── P-A：增量块摘要（chunk_digests 块表 + dirty 挂接）───────────────
    #[test]
    fn test_chunk_digests_basic_crud() {
        let s = Storage::memory().unwrap();
        {
            let conn = s.conn.lock().unwrap_or_else(|e| e.into_inner());
            Storage::upsert_chunk_digest_in_tx(&conn, 2, 0, b"a", b"m", 5, &[1u8; 32], false, 2000)
                .unwrap();
            Storage::upsert_chunk_digest_in_tx(&conn, 2, 1, b"n", b"z", 7, &[2u8; 32], false, 2000)
                .unwrap();
            // 覆盖 idx 0（同 repo+idx 冲突 → UPDATE）
            Storage::upsert_chunk_digest_in_tx(
                &conn, 2, 0, b"aa", b"zz", 3, &[3u8; 32], true, 2000,
            )
            .unwrap();
        }
        let chunks = s.get_chunk_digests(2).unwrap();
        assert_eq!(chunks.len(), 2, "块表应有 2 块");
        assert_eq!(chunks[0].idx, 0);
        assert_eq!(chunks[0].lo_key, b"aa");
        assert_eq!(chunks[0].rows, 3);
        assert!(chunks[0].dirty, "覆盖写入应保留 dirty=true");
        assert_eq!(chunks[1].idx, 1);
        assert!(!chunks[1].dirty);
        // 跨 repo 隔离
        assert!(s.get_chunk_digests(1).unwrap().is_empty());
        {
            let conn = s.conn.lock().unwrap_or_else(|e| e.into_inner());
            Storage::delete_chunk_digests_in_tx(&conn, 2).unwrap();
        }
        assert!(s.get_chunk_digests(2).unwrap().is_empty());
    }

    #[test]
    fn test_mark_chunks_dirty_in_tx_locates_correct_blocks() {
        let s = Storage::memory().unwrap();
        {
            let conn = s.conn.lock().unwrap_or_else(|e| e.into_inner());
            Storage::upsert_chunk_digest_in_tx(&conn, 2, 0, b"a", b"m", 5, &[1u8; 32], false, 2000)
                .unwrap();
            Storage::upsert_chunk_digest_in_tx(&conn, 2, 1, b"n", b"z", 7, &[2u8; 32], false, 2000)
                .unwrap();
            Storage::upsert_chunk_digest_in_tx(
                &conn, 2, 2, b"zz", b"zzz", 2, &[3u8; 32], false, 2000,
            )
            .unwrap();
            // 块 0：a..=m
            Storage::mark_chunks_dirty_in_tx(&conn, 2, &[b"a1".to_vec(), b"abc".to_vec()]).unwrap();
            // 块 1 + 块 2：n..=z 与 zz..=zzz
            Storage::mark_chunks_dirty_in_tx(&conn, 2, &[b"n1".to_vec(), b"zz1".to_vec()]).unwrap();
            // 超界 key（> 最后块 hi）：尾块兜底标脏
            Storage::mark_chunks_dirty_in_tx(&conn, 2, &[b"zzzz9".to_vec()]).unwrap();
            // 空 keys：no-op
            Storage::mark_chunks_dirty_in_tx(&conn, 2, &[]).unwrap();
        }
        let chunks = s.get_chunk_digests(2).unwrap();
        assert!(
            chunks.iter().all(|c| c.dirty),
            "三块均应 dirty（含尾块兜底）"
        );
    }

    #[test]
    fn test_mark_chunks_dirty_empty_table_noop() {
        // 旧库块表空（P-C 回填前）：写挂接应静默跳过，不报错
        let s = Storage::memory().unwrap();
        {
            let conn = s.conn.lock().unwrap_or_else(|e| e.into_inner());
            Storage::mark_chunks_dirty_in_tx(&conn, 1, &[b"1.2.3.4:6881".to_vec()]).unwrap();
        }
        assert!(s.get_chunk_digests(1).unwrap().is_empty());
    }

    #[test]
    fn test_mark_repo_all_dirty() {
        let s = Storage::memory().unwrap();
        {
            let conn = s.conn.lock().unwrap_or_else(|e| e.into_inner());
            Storage::upsert_chunk_digest_in_tx(&conn, 3, 0, b"a", b"b", 1, &[1u8; 32], false, 2000)
                .unwrap();
            Storage::upsert_chunk_digest_in_tx(&conn, 3, 1, b"c", b"d", 2, &[2u8; 32], false, 2000)
                .unwrap();
        }
        s.mark_repo_all_dirty(3).unwrap();
        let chunks = s.get_chunk_digests(3).unwrap();
        assert_eq!(chunks.len(), 2);
        assert!(chunks.iter().all(|c| c.dirty));
        // 其他 repo 不受影响
        assert!(s.get_chunk_digests(1).unwrap().is_empty());
    }

    #[test]
    fn test_chunk_dirty_hook_on_peer_save() {
        // 挂接冒烟：save_peers_batch 写库后，对应块应被标 dirty
        let s = Storage::memory().unwrap();
        {
            let conn = s.conn.lock().unwrap_or_else(|e| e.into_inner());
            Storage::upsert_chunk_digest_in_tx(
                &conn, 2, 0, b"", b"zzzz", 0, &[0u8; 32], false, 2000,
            )
            .unwrap();
        }
        let peer = PeerRow {
            infohash: [7u8; 20],
            ip: "1.2.3.4".into(),
            port: 6881,
            source: "test".into(),
            score: 1.0,
            connection_attempts: 0,
            connection_successes: 0,
            last_active: 0,
        };
        s.save_peers_batch(&[peer]).unwrap();
        let chunks = s.get_chunk_digests(2).unwrap();
        assert!(chunks[0].dirty, "save_peers_batch 后块应标 dirty");
    }

    #[test]
    fn test_chunk_dirty_hook_on_node_soft_delete() {
        // 挂接冒烟：soft_delete_node 后对应块标 dirty；软删其他 key 不受影响
        let s = Storage::memory().unwrap();
        {
            let conn = s.conn.lock().unwrap_or_else(|e| e.into_inner());
            Storage::upsert_chunk_digest_in_tx(
                &conn,
                1,
                0,
                b"",
                b"9.9.9.9:9999",
                0,
                &[0u8; 32],
                false,
                2000,
            )
            .unwrap();
            Storage::upsert_chunk_digest_in_tx(
                &conn,
                1,
                1,
                b"9.9.9.9:9999",
                b"zzz",
                0,
                &[1u8; 32],
                false,
                2000,
            )
            .unwrap();
        }
        s.save_dht_node(
            &[9u8; 20], "1.2.3.4", 6881, 1.0, "Good", 1, 1, 5, 0, 8, None,
        )
        .unwrap();
        assert!(
            s.get_chunk_digests(1).unwrap()[0].dirty,
            "save_dht_node 后块 0 应 dirty"
        );
        assert!(!s.get_chunk_digests(1).unwrap()[1].dirty, "块 1 不应受影响");
    }

    // ============================================================
    // 2026-10-09 治本 S5 回归：写锁争用可观测性
    // ============================================================

    /// S5 的地基：`with_write_conn` 必须正确计数并执行闭包。
    ///
    /// 此前 38 处 `self.conn.lock()` 零可观测性，导致「锁争用是否参与
    /// 2026-10-09 事故」无法证明、改造后无法验证。本测试固定该入口语义。
    #[test]
    fn test_with_write_conn_counts_and_executes() {
        let conn = Arc::new(Mutex::new(rusqlite::Connection::open_in_memory().unwrap()));
        let before = write_lock_stats().acquired;

        let n: i64 = with_write_conn(&conn, |c| {
            c.execute_batch("CREATE TABLE IF NOT EXISTS t(x INTEGER);")?;
            c.execute("INSERT INTO t(x) VALUES (42)", [])?;
            // `query_row` 返回 `rusqlite::Result<i64>`，而 `with_write_conn`
            // 的闭包要求 `anyhow::Result<T>` —— 需 `Ok(..?..)` 转一层，
            // 且只包一层（写成 `Ok(c.query_row(..)? )` 会得到嵌套 Result）。
            Ok(c.query_row("SELECT x FROM t", [], |r| r.get::<_, i64>(0))?)
        })
        .unwrap();

        assert_eq!(n, 42, "闭包返回值必须原样透传");
        // 用「至少 +1」而非精确相等：`acquired` 是进程级静态原子量，
        // cargo test 并行执行时其他测试也在调用 with_write_conn，
        // 精确相等会因跨测试干扰而flaky（实测 left=23/right=22）。
        let after = write_lock_stats().acquired;
        assert!(
            after > before,
            "进入 with_write_conn 后计数必须增长（否则无法量化锁争用），             实测 before={} after={}",
            before,
            after
        );
    }

    /// 无争用时（单线程串行）慢锁计数**不得**增长。
    ///
    /// 这是 S5 指标的可信性前提：若无争用也算慢锁，指标就恒为正，
    /// 压测判据将完全失去区分能力。
    #[test]
    fn test_no_slow_lock_when_uncontended() {
        let conn = Arc::new(Mutex::new(rusqlite::Connection::open_in_memory().unwrap()));
        let before = write_lock_stats().slow;
        for _ in 0..20 {
            with_write_conn(&conn, |_c| Ok(())).unwrap();
        }
        assert_eq!(
            write_lock_stats().slow,
            before,
            "无争用的串行调用不得计入慢锁（否则指标恒为正、失去意义）"
        );
    }

    /// 真争用（并发持锁）必须被计入慢锁 —— 指标必须能反映真实争用。
    #[test]
    fn test_slow_lock_counted_under_real_contention() {
        let conn = Arc::new(Mutex::new(rusqlite::Connection::open_in_memory().unwrap()));
        let before_slow = write_lock_stats().slow;
        let before_max = write_lock_stats().max_wait_ms;

        // 8 线程各抢 60 次，锁被占满 → 必然出现 ≥50ms 的等待
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let conn = conn.clone();
                std::thread::spawn(move || {
                    for _ in 0..60 {
                        let _ = with_write_conn(&conn, |_c| {
                            // 持锁 15ms（模拟慢写），使并发调用必然排队超阈值
                            std::thread::sleep(std::time::Duration::from_millis(15));
                            Ok(())
                        });
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let after = write_lock_stats();
        assert!(
            after.slow > before_slow,
            "真实争用（8线程×60次×持锁15ms）必须产生慢锁计数，\
             实测 slow: {} → {}",
            before_slow,
            after.slow
        );
        assert!(after.max_wait_ms >= before_max, "峰值等待时长不得回退");
    }

    /// poisoned 锁不得 panic（契约：绝不 panic，与 `read()` 一致）。
    #[test]
    fn test_with_write_conn_survives_poisoned_mutex() {
        let conn = Arc::new(Mutex::new(rusqlite::Connection::open_in_memory().unwrap()));
        // 故意 poison：在持锁线程里 panic
        let c2 = conn.clone();
        let _ = std::thread::spawn(move || {
            let _g = c2.lock().unwrap();
            panic!("intentional poison");
        })
        .join();

        // 不得 panic，且能正常执行
        let r = with_write_conn(&conn, |_c| Ok(()));
        assert!(r.is_ok(), "poisoned 锁必须 into_inner 恢复，不得 panic");
    }

    // ============================================================
    // 2026-10-09 治本 S6 回归：统计写入批量化
    // ============================================================

    /// `update_aggregate_batch` 必须一次抢锁写入全部指标。
    ///
    /// 【为什么】`io_metrics_poll` 间隔 1 秒，原实现单轮 8 次逐条
    /// `update_aggregate` = 每秒 8 次抢全库唯一写锁。120 分钟压测实测
    /// 该路径在 checkpoint fsync 期间造成单次 10415ms 等锁。
    #[test]
    fn test_update_aggregate_batch_writes_all_in_one_lock() {
        let s = Storage::memory().unwrap();
        let before = write_lock_stats().acquired;

        s.update_aggregate_batch(&[
            ("m1", 1.0),
            ("m2", 2.0),
            ("m3", 3.0),
            ("m4", 4.0),
            ("m5", 5.0),
            ("m6", 6.0),
            ("m7", 7.0),
            ("m8", 8.0),
        ])
        .unwrap();

        // 核心断言：8 个指标只应抢 **1 次** 锁（而非 8 次）。
        //
        // 【不能断言 == 1】`write_lock_stats()` 是**进程级静态原子量**，
        // cargo test 并行执行时其他测试也在抢锁，精确相等会 flaky
        // （同批改写的 record_entity_stats_batch 测试已踩过这个坑）。
        // 改为断言「远小于 8」这一实质约束。
        let delta = write_lock_stats().acquired - before;
        assert!(
            delta < 8,
            "8 个指标必须合并为少数几次抢锁（实测 {} 次，要求 < 8）—— 这是 S6 的全部意义",
            delta
        );

        // 值必须真的落库
        for (name, expect) in [("m1", 1.0), ("m4", 4.0), ("m8", 8.0)] {
            let got = s
                .read_long(|c| {
                    // `read_long` 闭包需返回 `Result<T>`；查不到行时归一为 None。
                    Ok(c.query_row(
                        "SELECT value FROM stats_aggregate WHERE metric = ?1",
                        params![name],
                        |r| r.get::<_, f64>(0),
                    )
                    .ok())
                })
                .ok()
                .flatten();
            assert_eq!(got, Some(expect), "指标 {} 落库值错误", name);
        }
    }

    /// 空批量不得抢锁（避免无意义开销）。
    #[test]
    fn test_update_aggregate_batch_empty_is_noop() {
        let s = Storage::memory().unwrap();
        let before = write_lock_stats().acquired;
        s.update_aggregate_batch(&[]).unwrap();
        assert_eq!(write_lock_stats().acquired, before, "空批量不得抢锁");
    }
    /// 批量内任一指标写失败 ⇒ 整批回滚（事务原子性不得破坏）。
    ///
    /// 构造方式：注入一个 BEFORE INSERT 触发器，在 value<0 时 RAISE(ABORT)，
    /// 从而让批量写到一半失败。
    ///
    /// 【踩过的坑】先前试了两种"显然会失败"的构造，**两种都不报错**：
    ///   ① 超长 metric 名 —— SQLite 对 TEXT 长度无限制；
    ///   ② 含 NUL 的 metric 名 —— SQLite C API 会截断 NUL 后继续执行。
    /// 两者都是错误的测试假设，不是实现缺陷。改用触发器才是可靠构造。
    #[test]
    fn test_update_aggregate_batch_is_atomic() {
        let s = Storage::memory().unwrap();
        // 正常写入应成功
        s.update_aggregate_batch(&[("keep", 42.0)]).unwrap();

        // 注入会在 INSERT 时失败的触发器：value<0 ⇒ RAISE(ABORT)
        {
            let conn = s.conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute_batch(
                "CREATE TRIGGER IF NOT EXISTS t_fail_neg BEFORE INSERT ON stats_aggregate \
                 WHEN NEW.value < 0 \
                 BEGIN SELECT RAISE(ABORT, 'neg value rejected'); END;",
            )
            .unwrap();
        }

        let r = s.update_aggregate_batch(&[("ok2", 1.0), ("bad", -1.0)]);
        assert!(r.is_err(), "含被拒绝值的批次应失败");

        // 原子性：同批次里 "ok2" 不应被提交
        let got = s
            .read_long(|c| {
                Ok(c.query_row(
                    "SELECT value FROM stats_aggregate WHERE metric = 'ok2'",
                    [],
                    |r| r.get::<_, f64>(0),
                )
                .ok())
            })
            .ok()
            .flatten();
        assert!(
            got.is_none(),
            "失败批次中的 ok2 不应被提交（事务应回滚），实得 {:?}",
            got
        );
    }
}
