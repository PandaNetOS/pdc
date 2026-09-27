//! P2-1/P2-2：bootstrap 专用通道（六阶段）——把「新节点首同步」与在线反熵彻底解耦。
//!
//! 设计见 `docs/architecture/12-federation-sync-reconciliation.md` §5.4 / §6.8。
//!
//! 动机：一亿行 vs 新节点，「差异」就是全量 —— 用在线 diff 找「哪一亿行不同」纯属浪费，
//! 且必然失败（没有快照点永远追不上、没有 manifest 不知传到哪、没有限流会打垮生产节点）。
//! 正确做法是把它当成**一次数据迁移**，独立成专用通道：
//!
//! | 阶段 | 动作 | 本实现产物 |
//! |---|---|---|
//! | ① 协商与冻结 | 握手对齐协议版本；取水位 `w0 = oplog_max_seq()` | `BootstrapManifest.w0_seq` |
//! | ② 分块清单 | 按 key 有序区间切块，每块附内容哈希 | `ManifestChunk{lo,hi,rows,hash}` |
//! | ③ 并行限流传输 | 逐块拉取，服务端按 `bootstrap_rate_bytes_per_sec` 令牌桶限流 | `TokenBucket` |
//! | ④ 落地 | 新节点**批量 upsert**（复用既有 `apply_node_sync`），严禁逐条 INSERT | — |
//! | ⑤ 增量追尾 | 拉 `seq > w0` 的 oplog（复用 P1-3 delta 通道），多轮追到 Δ < 阈值 | — |
//! | ⑥ 校验 | **传输完整性**校验（声明行数 vs 实收条数）+ 失败计数自愈 | `verify_transport` |
//!
//! **一致性说明（v9）**：本实现以「**显式区间边界的逻辑分块 + W0 水位 + 传输完整性校验**」
//! 替代物理快照文件（`VACUUM INTO`）。bootstrap **只负责搬数据**：一致性（谁多了谁少了）
//! 由 Range 反熵负责 —— 早期版本在阶段⑥按块重算本地 `[lo,hi)` 摘要与清单哈希比对，
//! 只要接收方在该区间内有任何对端没有的行（双方独立爬取的普遍情况）就恒失配，
//! 导致「连续 3 次失败 → 重拉清单 → 再失败」的死循环，已废弃（`verify_chunk` 保留仅供诊断）。
//!
//! 关键约束：
//! - 默认 `federation.bootstrap_enabled = false`：不注册、不发起、不响应任何 bootstrap 消息。
//! - 幂等：任何 chunk / op 重复应用必须无害（upsert by key）。
//! - 断点续传：进度持久化在 SQLite `bootstrap_state` 表，重启后从 `done_chunks` 续传。
//! - 进度可观测：`BootstrapProgress` 经 `/api/v1/federation/sync-observability` 暴露。

use rusqlite::{params, Connection as SqliteConnection};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::storage::db::Storage;

/// 支持 bootstrap 专用通道的协议版本（握手能力协商）。
pub const BOOTSTRAP_PROTOCOL_VERSION: u32 = 6;

/// 单块默认行数（约 2 万行 × ~100 B ≈ 2 MB，远低于 `MAX_FRAME_SIZE`）。
pub const DEFAULT_CHUNK_ROWS: u32 = 20_000;
/// 默认服务端带宽预算（字节/秒）。0 = 不限流。
pub const DEFAULT_RATE_BYTES_PER_SEC: u64 = 8 * 1024 * 1024;
/// bootstrap 追尾后判定「已收敛」的 Δ 阈值（行）。
pub const DEFAULT_TAIL_CONVERGE_DELTA: u64 = 10_000;

/// bootstrap 阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BootstrapPhase {
    /// 未开始。
    Idle,
    /// ② 已请求/收到 manifest。
    Manifest,
    /// ③④ 拉块 + 落地中。
    Transfer,
    /// ⑤ 拉 `seq > w0` 的 oplog 追尾中。
    TailFollow,
    /// ⑥ 校验中。
    Verify,
    /// 完成。
    Done,
    /// 失败（携带原因）。
    Failed,
}

impl BootstrapPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            BootstrapPhase::Idle => "idle",
            BootstrapPhase::Manifest => "manifest",
            BootstrapPhase::Transfer => "transfer",
            BootstrapPhase::TailFollow => "tail_follow",
            BootstrapPhase::Verify => "verify",
            BootstrapPhase::Done => "done",
            BootstrapPhase::Failed => "failed",
        }
    }
}

/// 进度（可观测性 + 断点续传）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapProgress {
    pub repo: u8,
    pub peer: Vec<u8>,
    pub phase: BootstrapPhase,
    pub version: u32,
    pub w0_seq: u64,
    pub total_chunks: u64,
    pub done_chunks: u64,
    pub bytes: u64,
    pub started_ms: i64,
    pub updated_ms: i64,
    /// v10(B2)：已传输到的最后一个块的上界 key（hi）。重拉清单后以此在新清单中
    /// 定位续传起点 —— 块边界会随活表写入漂移，key 游标不随边界失效。
    /// `None` = 旧进度无游标（归零重来）；`Some(空 vec)` = 已传到 +∞（全量完成）。
    #[serde(default)]
    pub last_key: Option<Vec<u8>>,
    /// 失败原因（`phase == Failed` 时非空）。
    pub error: Option<String>,
}

impl BootstrapProgress {
    pub fn new(repo: u8, peer: Vec<u8>, now_ms: i64) -> Self {
        Self {
            repo,
            peer,
            phase: BootstrapPhase::Idle,
            version: 0,
            w0_seq: 0,
            total_chunks: 0,
            done_chunks: 0,
            bytes: 0,
            last_key: None,
            started_ms: now_ms,
            updated_ms: now_ms,
            error: None,
        }
    }

    /// 完成度（0.0 ~ 1.0）。
    pub fn ratio(&self) -> f64 {
        if self.total_chunks == 0 {
            0.0
        } else {
            (self.done_chunks as f64 / self.total_chunks as f64).clamp(0.0, 1.0)
        }
    }
}

/// 清单中的单个块。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestChunk {
    /// 块序号（升序，与 `chunks` 下标一致）。
    pub index: u32,
    /// 区间下界（含）；空 = -∞。
    pub lo: Vec<u8>,
    /// 区间上界（不含）；空 = +∞。
    pub hi: Vec<u8>,
    /// 行数。
    pub rows: u64,
    /// 该块 `(key, data_hash)` 有序流的内容哈希（与 `range_reconcile::range_digest` 同构）。
    pub hash: [u8; 32],
}

/// bootstrap 清单。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BootstrapManifest {
    /// 仓库类型。
    pub repo: u8,
    /// 清单版本（重新打快照即变；用于判废旧进度）。
    pub version: u32,
    /// 快照水位：`oplog_max_seq` 在打快照时刻的值（⑤ 追尾以此为起点）。
    pub w0_seq: u64,
    /// 单块行数。
    pub chunk_rows: u32,
    /// 总行数。
    pub total_rows: u64,
    /// 分块列表（`lo`/`hi` 为显式边界，服务端按此区间原样取数）。
    pub chunks: Vec<ManifestChunk>,
}

/// 建 bootstrap 状态表（幂等）。由 `Storage::init_tables` 调用。
///
/// v9：主键由 `repo` 改为 **`(peer, repo)`**。
///
/// 旧实现 `repo INTEGER PRIMARY KEY` 意味着一台机器每个 repo 只有一行进度，
/// 而 `peer` 只是「最后一次写入的对端」；后果：
/// 1. 多对端时彼此覆盖进度、chunk 响应被记到错误对端（`max_connections` 默认 32）；
/// 2. `delta_sync_tick` 的 `running` 判定按 repo 命中该行后，**该 repo 对所有对端的
///    delta 一起停摆**，与「bootstrap 卡死」构成互锁闭环。
///
/// 旧库自动迁移：把 `bootstrap_state` 改名为 `bootstrap_state_legacy`，解析 payload JSON
/// 里的 `peer` 字段后写入新表（peer 缺失的行直接丢弃 —— 那些正是无法归属的脏进度）。
pub fn init_bootstrap_table(conn: &SqliteConnection) -> anyhow::Result<()> {
    let exists: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='bootstrap_state'",
        [],
        |r| r.get(0),
    )?;
    if exists > 0 && !bootstrap_table_has_peer_column(conn)? {
        conn.execute_batch("ALTER TABLE bootstrap_state RENAME TO bootstrap_state_legacy;")?;
    }
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS bootstrap_state (
            peer       BLOB NOT NULL,     -- 对端 20B node_id（v9 起进入主键）
            repo       INTEGER NOT NULL,  -- 1..4
            payload    BLOB NOT NULL,     -- BootstrapProgress 的 JSON
            manifest   BLOB,              -- BootstrapManifest 的 JSON（未取到时为 NULL）
            updated_ms INTEGER NOT NULL,
            PRIMARY KEY (peer, repo)
        );
        "#,
    )?;
    migrate_bootstrap_state_best_effort(conn);
    Ok(())
}

/// v9：迁移的安全包装 —— 迁移失败**不得阻断 Agent 启动**。
///
/// 迁移会重建表并搬数据；任一行读写失败若直接上抛，会让 `init_tables` 失败 →
/// `Storage::open` 失败 → 进程起不来。这里降级为「保留 legacy 表 + 告警 + 以空进度继续」
/// （bootstrap 进度丢失是可接受的代价：它只是断点，快照可重打）。
pub fn migrate_bootstrap_state_best_effort(conn: &SqliteConnection) {
    if let Err(e) = migrate_legacy_bootstrap_rows(conn) {
        tracing::error!(
            "[bootstrap] 旧库 bootstrap 进度迁移失败（保留 legacy 表、以空进度继续）: {}",
            e
        );
    }
}

/// `bootstrap_state` 是否已带 `peer` 列。
fn bootstrap_table_has_peer_column(conn: &SqliteConnection) -> anyhow::Result<bool> {
    let mut stmt = conn.prepare("PRAGMA table_info(bootstrap_state)")?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        if r.get::<_, String>(1)? == "peer" {
            return Ok(true);
        }
    }
    Ok(false)
}

/// 迁移旧库（`repo` 单主键）里的 bootstrap 进度到 `(peer, repo)` 新表，然后删掉影子表。
fn migrate_legacy_bootstrap_rows(conn: &SqliteConnection) -> anyhow::Result<()> {
    let legacy: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='bootstrap_state_legacy'",
        [],
        |r| r.get(0),
    )?;
    if legacy == 0 {
        return Ok(());
    }
    // 先把 legacy 行读进内存（释放语句借用），再在一个事务里搬数据 + 删影子表，
    // 保证「要么全搬完，要么下次启动重来」。
    let legacy_rows: Vec<(i64, Vec<u8>, Option<Vec<u8>>, i64)> = {
        let mut stmt =
            conn.prepare("SELECT repo, payload, manifest, updated_ms FROM bootstrap_state_legacy")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, Option<Vec<u8>>>(2)?,
                r.get::<_, i64>(3)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        out
    };
    let mut dropped = 0usize;
    let mut migrated = 0usize;
    {
        let tx = conn.unchecked_transaction()?;
        for (repo, payload, manifest, updated_ms) in legacy_rows {
            let Ok(progress) = serde_json::from_slice::<BootstrapProgress>(&payload) else {
                dropped += 1;
                continue;
            };
            // 无法归属到具体对端的进度直接丢弃：新表主键要求 peer 非空。
            if progress.peer.len() != 20 {
                dropped += 1;
                continue;
            }
            tx.execute(
                "INSERT OR REPLACE INTO bootstrap_state (peer, repo, payload, manifest, updated_ms) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![&progress.peer, repo, payload, manifest, updated_ms],
            )?;
            migrated += 1;
        }
        tx.execute_batch("DROP TABLE bootstrap_state_legacy;")?;
        tx.commit()?;
    }
    if dropped > 0 {
        tracing::warn!(
            "[bootstrap] 迁移丢弃 {} 行无法归属对端的旧进度（peer 字段缺失/非法）",
            dropped
        );
    }
    if migrated > 0 {
        debug!(
            "[bootstrap] 旧库 bootstrap 进度迁移完成: {} 行 → (peer, repo) 主键",
            migrated
        );
    }
    Ok(())
}

/// 内容哈希：对按 key 升序的 `(key, data_hash)` 流取 blake3（每段带 4 字节长度前缀）。
///
/// 与 `range_reconcile::range_digest` 同构：服务端建 manifest、客户端落地后重算校验都用它。
pub fn chunk_hash(rows: &[(Vec<u8>, Vec<u8>)]) -> [u8; 32] {
    crate::federation::sync::range_reconcile::range_digest(rows)
}

impl Storage {
    /// 保存某 (peer, repo) 的 bootstrap 进度（幂等 upsert）。
    ///
    /// v9：主键为 `(progress.peer, progress.repo)`；`peer` 不足 20 字节时拒绝写入
    /// （无法归属的进度只会污染其它对端的续传）。
    pub fn bootstrap_save(
        &self,
        progress: &BootstrapProgress,
        manifest: Option<&BootstrapManifest>,
    ) -> anyhow::Result<()> {
        if progress.peer.len() != 20 {
            anyhow::bail!(
                "bootstrap_save: peer 必须为 20 字节，实际 {}",
                progress.peer.len()
            );
        }
        let payload = serde_json::to_vec(progress)?;
        let mf = match manifest {
            Some(m) => Some(serde_json::to_vec(m)?),
            None => None,
        };
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "INSERT INTO bootstrap_state (peer, repo, payload, manifest, updated_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(peer, repo) DO UPDATE SET payload = excluded.payload, \
             manifest = COALESCE(excluded.manifest, bootstrap_state.manifest), \
             updated_ms = excluded.updated_ms",
            params![
                &progress.peer,
                progress.repo as i64,
                payload,
                mf,
                progress.updated_ms
            ],
        )?;
        Ok(())
    }

    /// 读取某 (peer, repo) 的 bootstrap 进度与清单。
    pub fn bootstrap_load(
        &self,
        peer: &[u8],
        repo: u8,
    ) -> anyhow::Result<Option<(BootstrapProgress, Option<BootstrapManifest>)>> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        let row = conn.query_row(
            "SELECT payload, manifest FROM bootstrap_state WHERE peer = ?1 AND repo = ?2",
            params![peer, repo as i64],
            |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Option<Vec<u8>>>(1)?)),
        );
        match row {
            Ok((payload, mf)) => {
                let progress: BootstrapProgress = serde_json::from_slice(&payload)?;
                let manifest = match mf {
                    Some(b) => Some(serde_json::from_slice(&b)?),
                    None => None,
                };
                Ok(Some((progress, manifest)))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// 清除某 (peer, repo) 的 bootstrap 进度（完成或放弃时调用）。
    pub fn bootstrap_clear(&self, peer: &[u8], repo: u8) -> anyhow::Result<()> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "DELETE FROM bootstrap_state WHERE peer = ?1 AND repo = ?2",
            params![peer, repo as i64],
        )?;
        Ok(())
    }

    /// 列出所有 bootstrap 进度（重启后恢复用）。`payload` 内含 peer，调用方自行归属。
    pub fn bootstrap_list(&self) -> anyhow::Result<Vec<BootstrapProgress>> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare("SELECT payload FROM bootstrap_state ORDER BY repo")?;
        let rows = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
        let mut out = Vec::new();
        for p in rows.flatten() {
            if let Ok(progress) = serde_json::from_slice::<BootstrapProgress>(&p) {
                out.push(progress);
            }
        }
        Ok(out)
    }
}

/// 全 repo 通用清单构建（流式分块，内存开销 O(chunk_rows)）。
///
/// 逐块推进游标：每块取 `chunk_rows + 1` 行，多取的 1 行用作下一块的 `lo`（即本块 `hi`），
/// 使 `[lo, hi)` 恰好覆盖 `chunk_rows` 行；末块的 `hi = None`（+∞）。
///
/// A7：这是**唯一**的清单构建入口，区间读取走 `load_repo_key_hashes_in_range(repo, …)`。
/// 原 `build_node_manifest` 包装函数把 repo 写死成 NODE，与「全 repo 统一逻辑」冲突，
/// 已删除 —— 调用方必须显式传 repo。
pub fn build_repo_manifest_impl(
    storage: &Storage,
    repo: u8,
    chunk_rows: u32,
    w0_seq: u64,
    version: u32,
) -> anyhow::Result<BootstrapManifest> {
    let chunk_rows = chunk_rows.max(1) as usize;
    let mut chunks: Vec<ManifestChunk> = Vec::new();
    let mut cursor: Option<Vec<u8>> = None; // -∞ 起
    let mut total_rows: u64 = 0;
    let mut index: u32 = 0;

    loop {
        let fetch = chunk_rows + 1;
        let rows = storage.load_repo_key_hashes_in_range(repo, cursor.as_deref(), None, fetch)?;
        if rows.is_empty() {
            break;
        }
        let is_last = rows.len() <= chunk_rows;
        let take = rows.len().min(chunk_rows);
        let body = &rows[..take];
        let lo = if index == 0 || cursor.is_none() {
            Vec::new()
        } else {
            cursor.clone().unwrap_or_default()
        };
        let hi = if is_last {
            Vec::new()
        } else {
            rows[take].0.clone()
        };
        let hash = chunk_hash(body);
        chunks.push(ManifestChunk {
            index,
            lo,
            hi: hi.clone(),
            rows: take as u64,
            hash,
        });
        total_rows += take as u64;
        if is_last {
            break;
        }
        cursor = Some(hi);
        index += 1;
    }

    // P0-2：清单 `repo` 必须回填入参，不能硬编码 NODE。
    // 旧实现写死 NODE，导致应答方为 PEER/INFOHASH/TRACKER 构造出的清单被标记成 NODE，
    // 请求方（handle_bootstrap_manifest_response）按 repo 校验时全部错位丢弃。
    Ok(BootstrapManifest {
        repo,
        version,
        w0_seq,
        chunk_rows: chunk_rows as u32,
        total_rows,
        chunks,
    })
}

/// 简单令牌桶（字节维度），用于服务端 bootstrap 限流（铁律 1：低优先级、可抢占）。
pub struct TokenBucket {
    rate: u64,     // 字节/秒；0 = 不限流
    capacity: f64, // 最大突发字节
    tokens: f64,
    last: std::time::Instant,
}

impl TokenBucket {
    pub fn new(rate_bytes_per_sec: u64) -> Self {
        let capacity = if rate_bytes_per_sec == 0 {
            0.0
        } else {
            (rate_bytes_per_sec as f64).max(1024.0)
        };
        Self {
            rate: rate_bytes_per_sec,
            capacity,
            tokens: capacity,
            last: std::time::Instant::now(),
        }
    }

    fn refill(&mut self) {
        if self.rate == 0 {
            return;
        }
        let now = std::time::Instant::now();
        let dt = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + dt * self.rate as f64).min(self.capacity);
    }

    /// 需要等待的时长（不睡眠，纯计算，便于测试）。
    pub fn wait_duration(&mut self, bytes: u64) -> std::time::Duration {
        if self.rate == 0 {
            return std::time::Duration::ZERO;
        }
        self.refill();
        let need = bytes as f64;
        if need <= self.tokens {
            self.tokens -= need;
            std::time::Duration::ZERO
        } else {
            let deficit = need - self.tokens;
            self.tokens = 0.0;
            std::time::Duration::from_secs_f64(deficit / self.rate as f64)
        }
    }

    /// 取够 `bytes` 的配额（不足则睡眠等待）。
    pub async fn acquire(&mut self, bytes: u64) {
        let d = self.wait_duration(bytes);
        if !d.is_zero() {
            tokio::time::sleep(d).await;
        }
    }
}

/// P0-4：**传输完整性**校验 —— 只校验收到的条目本身，不重算本地区间。
///
/// 取代旧的一致性语义 `verify_chunk`（落地后重算本地 `[lo,hi)` 摘要与清单 hash 比对）。
/// 旧语义必然恒失配：只要接收方在该 key 区间内有**任何对端没有的行**（双方各自独立爬取
/// 产生的 ~2% 差异，全域均匀分布），或对端在传输期有写入，每个块都判失败。后果是 F7 的
/// 「连续 3 次失败重拉清单」陷入死循环 —— 重拉回来的清单仍是对端 DB，接收方的多余行还在，
/// 永远对不上。一致性校验应交给 range 反熵（v8 下唯一兜底通道）。
///
/// 判定规则：
/// - 声明 0 行的块必须收到 0 条；
/// - 声明 N 行的块必须收到 ≥1 条。
///
/// v10(B2)：**活表语义** —— 对端表持续写入，区间实收可能**超过**清单时点的声明行数
/// （新增 key 落进 [lo,hi)），超收是正常演进、不判失败（此前 `received <= expected`
/// 在持续写入的大表上必败 → 块 3 次失败 → 重拉清单 → 归零，快照永不传完，
/// 实测 52/58 NODE 148 块反复归零）。空回包（对端 NAK）仍是唯一失败态；
/// 重叠/缺失行由竣工后的 delta 追尾与 Range 反熵兜底。
pub fn verify_transport(expected_rows: u64, received: usize) -> bool {
    if expected_rows == 0 {
        return received == 0;
    }
    received > 0
}

/// v10(B2)：在新清单中定位续传起点（key 游标断点）。
///
/// `last_key` = 旧进度已传到的最后一个块上界（hi）。块边界会随活表写入漂移
/// （边界比对在新清单上必然失配），但 key 游标稳定：新清单中第一个
/// `hi > last_key`（或 hi 为 +∞）的块即为续传块，与 last_key 横跨的部分重传、
/// 幂等 upsert 无害。`last_key` 为 `None`（旧进度无游标）→ 从 0；
/// `Some(空 vec)`（已传到 +∞）→ 全跳过。
pub fn locate_resume_index(chunks: &[ManifestChunk], last_key: Option<&[u8]>) -> u64 {
    let Some(k) = last_key else {
        return 0;
    };
    if k.is_empty() {
        return chunks.len() as u64;
    }
    chunks
        .iter()
        .position(|c| c.hi.is_empty() || c.hi.as_slice() > k)
        .map(|i| i as u64)
        .unwrap_or(chunks.len() as u64)
}

/// 校验收到的行是否与清单块哈希一致。
///
/// ⚠️ 语义为「一致性」校验（本地重算 vs 清单），P0-4 后 bootstrap 主流程已改用
/// [`verify_transport`]；此函数保留供离线/诊断使用，勿再接入主流程。
pub fn verify_chunk(expected: &[u8; 32], rows: &[(Vec<u8>, Vec<u8>)]) -> bool {
    let got = chunk_hash(rows);
    let ok = &got == expected;
    if !ok {
        debug!(
            "[bootstrap] 块哈希不符: expected={}, got={}",
            hex32(expected),
            hex32(&got)
        );
    }
    ok
}

fn hex32(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::db::Storage;

    fn now_ms() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }

    fn seed_nodes(st: &Storage, n: u32) {
        // 直接写 dht_nodes（key = "ip:port" 升序可预测）
        let conn = st.connection();
        let conn = conn.lock().unwrap();
        for i in 0..n {
            let ip = format!("10.0.{}.{}", i / 256, i % 256);
            let port = 6881i64;
            let id = vec![(i % 256) as u8; 20];
            conn.execute(
                "INSERT INTO dht_nodes (id, ip, port, l2_shard, deleted_at) VALUES (?1, ?2, ?3, 0, NULL)",
                params![id, ip, port],
            )
            .unwrap();
        }
    }

    #[test]
    fn test_manifest_chunking_covers_all_rows() {
        let st = Storage::memory().unwrap();
        seed_nodes(&st, 1000);
        let mf =
            build_repo_manifest_impl(&st, crate::federation::sync::repo_type::NODE, 256, 42, 1)
                .unwrap();
        assert_eq!(mf.w0_seq, 42);
        assert_eq!(mf.total_rows, 1000);
        // 1000 / 256 = 3 满块 + 1 残块 = 4 块
        assert_eq!(mf.chunks.len(), 4);
        // 每块行数之和 == 总行数
        let sum: u64 = mf.chunks.iter().map(|c| c.rows).sum();
        assert_eq!(sum, 1000);
        // 前缀块 lo/hi 连续：block[i].hi == block[i+1].lo
        for w in mf.chunks.windows(2) {
            assert_eq!(w[0].hi, w[1].lo);
        }
        // 首块 lo 为空（-∞），末块 hi 为空（+∞）
        assert!(mf.chunks.first().unwrap().lo.is_empty());
        assert!(mf.chunks.last().unwrap().hi.is_empty());
        // 每块哈希与「按 lo/hi 重取」一致
        for c in &mf.chunks {
            let lo = if c.lo.is_empty() {
                None
            } else {
                Some(c.lo.as_slice())
            };
            let hi = if c.hi.is_empty() {
                None
            } else {
                Some(c.hi.as_slice())
            };
            let rows = st
                .load_node_key_hashes_in_range(lo, hi, c.rows as usize + 1)
                .unwrap();
            assert_eq!(rows.len() as u64, c.rows);
            assert!(verify_chunk(&c.hash, &rows), "块 {} 哈希不符", c.index);
        }
    }

    #[test]
    fn test_manifest_empty_db() {
        let st = Storage::memory().unwrap();
        let mf = build_repo_manifest_impl(&st, crate::federation::sync::repo_type::NODE, 100, 0, 1)
            .unwrap();
        assert_eq!(mf.total_rows, 0);
        assert!(mf.chunks.is_empty());
    }

    #[test]
    fn test_bootstrap_state_roundtrip() {
        let st = Storage::memory().unwrap();
        let peer = vec![9u8; 20];
        assert!(st.bootstrap_load(&peer, 1).unwrap().is_none());
        let mut p = BootstrapProgress::new(1, peer.clone(), now_ms());
        p.phase = BootstrapPhase::Transfer;
        p.total_chunks = 10;
        p.done_chunks = 3;
        p.w0_seq = 777;
        let mf = BootstrapManifest {
            repo: 1,
            version: 1,
            w0_seq: 777,
            chunk_rows: 256,
            total_rows: 2560,
            chunks: vec![],
        };
        st.bootstrap_save(&p, Some(&mf)).unwrap();
        // 二次保存（不带 manifest）应保留原 manifest
        let mut p2 = p.clone();
        p2.done_chunks = 5;
        st.bootstrap_save(&p2, None).unwrap();

        let (got, got_mf) = st.bootstrap_load(&peer, 1).unwrap().unwrap();
        assert_eq!(got.done_chunks, 5);
        assert_eq!(got.phase, BootstrapPhase::Transfer);
        assert_eq!(got.ratio(), 0.5);
        assert_eq!(got_mf.unwrap().w0_seq, 777);
        assert_eq!(st.bootstrap_list().unwrap().len(), 1);
        st.bootstrap_clear(&peer, 1).unwrap();
        assert!(st.bootstrap_load(&peer, 1).unwrap().is_none());
    }

    /// v9 回归：进度必须按 (peer, repo) 隔离 —— 同一 repo 上两个对端的进度互不覆盖。
    /// 旧实现 `repo` 单主键会让 B 的进度覆盖 A 的，并使该 repo 对**所有**对端的 delta 停摆。
    #[test]
    fn test_bootstrap_state_isolated_per_peer() {
        let st = Storage::memory().unwrap();
        let pa = vec![0xAAu8; 20];
        let pb = vec![0xBBu8; 20];
        let mut a = BootstrapProgress::new(1, pa.clone(), now_ms());
        a.total_chunks = 10;
        a.done_chunks = 4;
        let mut b = BootstrapProgress::new(1, pb.clone(), now_ms());
        b.total_chunks = 20;
        b.done_chunks = 7;
        st.bootstrap_save(&a, None).unwrap();
        st.bootstrap_save(&b, None).unwrap();

        let (ga, _) = st.bootstrap_load(&pa, 1).unwrap().unwrap();
        let (gb, _) = st.bootstrap_load(&pb, 1).unwrap().unwrap();
        assert_eq!(ga.done_chunks, 4);
        assert_eq!(gb.done_chunks, 7);
        assert_eq!(st.bootstrap_list().unwrap().len(), 2);
        // 清除 A 不应影响 B
        st.bootstrap_clear(&pa, 1).unwrap();
        assert!(st.bootstrap_load(&pa, 1).unwrap().is_none());
        assert!(st.bootstrap_load(&pb, 1).unwrap().is_some());
    }

    #[test]
    fn test_token_bucket() {
        // 不限流
        let mut tb = TokenBucket::new(0);
        assert_eq!(tb.wait_duration(10_000_000), std::time::Duration::ZERO);
        // 限流：容量 = rate，首取大量应需等待
        let mut tb = TokenBucket::new(1000); // 1000 B/s
        assert_eq!(tb.wait_duration(1000), std::time::Duration::ZERO); // 用掉整桶
        let d = tb.wait_duration(1000); // 桶空，需 ~1s
        assert!(d.as_secs_f64() > 0.5, "应需等待，实际 {:?}", d);
    }
}
