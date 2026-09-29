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
use std::time::{Duration, Instant};
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
    /// 活锁治理(任务4)：最近一次**块成功落地**的时刻（毫秒时间戳）。停滞判定
    /// （`progress_stalled`，range 反熵让路解除）据此计算；旧进度行缺该字段时
    /// serde 回退 0，判定函数自动回落 `updated_ms`。
    #[serde(default)]
    pub last_progress_ms: i64,
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
            last_progress_ms: now_ms,
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

    /// 活锁治理(任务4)：参与停滞判定的「最近块落地时刻」。旧进度行（无
    /// `last_progress_ms`，serde 回退 0）回落 `updated_ms`，行为与引入前一致。
    pub fn effective_progress_ms(&self) -> i64 {
        if self.last_progress_ms > 0 {
            self.last_progress_ms
        } else {
            self.updated_ms
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
/// 判定规则（D批 D3 收紧：防「块返回极少行也判过」的假竣工）：
/// - 声明 0 行的块必须收到 0 条；
/// - 声明 N 行的块，实收必须达到 ⌊N/2⌋（整除向下取整）才算通过 —— 旧规则只要求
///   `received > 0`，对端清单重建/NAK 回包即使只回极少几行也被判成功，块窗口照推进、
///   done 照抬，数据没真落库却「竣工」。
/// - 超收（`received > N`）自然通过（`received >= ⌊N/2⌋` 恒真）。
///
/// v10(B2)：**活表语义** —— 对端表持续写入，区间实收可能**超过**清单时点的声明行数
/// （新增 key 落进 [lo,hi)），超收是正常演进、不判失败（此前 `received <= expected`
/// 在持续写入的大表上必败 → 块 3 次失败 → 重拉清单 → 归零，快照永不传完，
/// 实测 52/58 NODE 148 块反复归零）。空回包（对端 NAK）与实收不足半数仍是失败态；
/// 重叠/缺失行由竣工后的 delta 追尾与 Range 反熵兜底。
pub fn verify_transport(expected_rows: u64, received: usize) -> bool {
    if expected_rows == 0 {
        return received == 0;
    }
    (received as u64) >= expected_rows / 2
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

/// D批(D3)：断点继承的「逐块 hash/rows 验证 + 连续前缀裁剪」纯函数。
///
/// 输入：远端清单块列表 `remote_chunks` 与本地按相同 `chunk_rows`/水位重建出的清单
/// `local_mf`。输出二元组：
/// - `.0` = 验证通过的**最大连续前缀长度**：从 index 0 起逐块确认与本地 hash&rows 一致，
///   首块不一致即截断（该长度即持久化的 `done_chunks`）；
/// - `.1` = 全部 hash & rows 与本地一致的远端块 index 集合（含前缀内一致块，也含前缀外
///   经 hash 比对确认一致的块）——即断点继承 / skip 集合。
///
/// 调用方据此构造 ChunkWindow 的 skip 集合（= `.1`）与连续进度（= `.0`）。
/// 设计动机：旧实现把旧 `done_chunks` 前缀**无条件**继承进 skip 集合、并照写进
/// `done_chunks`，断点里的块若实际没在本地落库（51 式假竣工）也被当作已完成 →
/// 全部 seed 覆盖块数时直接写 Done。现在前缀块也必须逐块对得上本地内容才能留在 skip 里，
/// 对不上的块自然落入待拉集合由 ChunkWindow 补发。
pub fn align_bootstrap_seed(
    remote_chunks: &[ManifestChunk],
    local_mf: &BootstrapManifest,
) -> (u32, std::collections::HashSet<u32>) {
    let matching: std::collections::HashSet<u32> = remote_chunks
        .iter()
        .filter(|c| {
            local_mf
                .chunks
                .iter()
                .any(|l| l.index == c.index && l.hash == c.hash && l.rows == c.rows)
        })
        .map(|c| c.index)
        .collect();
    // 连续前缀：从 index 0 起逐块确认在 matching 中，首块缺失即截断。
    let mut prefix = 0u32;
    while matching.contains(&prefix) {
        prefix += 1;
    }
    (prefix, matching)
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

// ========================================================================
// 活锁治理（2026-09-30）：传输失败退避 / 清单漂移与进度继承 / 停滞判定。
// 纯函数，便于单测；SyncManager 侧只做状态读写与 IO 编排。
// ========================================================================

/// 活锁治理(任务1)：分块传输失败的退避重试状态。
///
/// 存放于 SyncManager 的 `bootstrap_chunk_attempt` 表（(peer, repo) → 状态）。
/// 传输类失败（send 失败/超时/无会话）只记入本状态做指数退避重发，
/// **永不**升级为重拉清单（旧实现按失败次数升级重拉，是三节点互拉活锁的第一环）。
#[derive(Debug, Clone, Copy)]
pub struct ChunkRetryState {
    /// 当前关注的块 index（换块即重置连续失败计数）。
    pub index: u32,
    /// 连续传输类失败次数（成功收到任意响应即整条清零）。
    pub fails: u32,
    /// 最近一次失败时刻。resume tick 据此按 [`backoff_delay`] 决定何时重发。
    pub last_fail_at: Instant,
}

/// 活锁治理(任务1)：第 `fails` 次连续失败后的退避等待时长。
///
/// `min(initial × 2^(fails-1), max)`：默认 30s 基数、600s 封顶，
/// 序列 30→60→120→240→480→600→600…。`fails == 0` 视为首次失败（返回 initial）。
pub fn backoff_delay(initial: Duration, max: Duration, fails: u32) -> Duration {
    let exp = fails.max(1).saturating_sub(1).min(31);
    initial.saturating_mul(1u32 << exp).min(max.max(initial))
}

/// 活锁治理(任务4)：bootstrap 停滞判定（纯函数）。
///
/// 距「最近一次块成功落地」超过 `threshold_ms` 即停滞。`last_progress_ms <= 0`
/// 的旧进度行回落 `updated_ms`（与引入前口径一致）。
pub fn progress_stalled(
    last_progress_ms: i64,
    updated_ms: i64,
    now_ms: i64,
    threshold_ms: i64,
) -> bool {
    let base = if last_progress_ms > 0 {
        last_progress_ms
    } else {
        updated_ms
    };
    now_ms.saturating_sub(base) > threshold_ms
}

/// 活锁治理(任务1-b/任务3)：重拉清单与上次清单的漂移判定（纯函数）。
///
/// 返回 `(是否结构性漂移, 是否可继承旧进度)`：
/// - 结构性漂移：`total_rows` 变化超过 `rows_pct%`（重拉的合法触发条件之一，
///   说明对端数据量级变了，旧断点已无意义）；
/// - 可继承：`total_chunks` 差异 ≤ `chunk_pct%`（分块结构未变，旧进度按 index
///   继承不归零；块内容幂等重传安全）。
pub fn manifest_drift(
    old_rows: u64,
    old_chunks: u64,
    new_rows: u64,
    new_chunks: u64,
    rows_pct: u32,
    chunk_pct: u32,
) -> (bool, bool) {
    // 无旧清单（首次 bootstrap）→ 无漂移、无可继承
    if old_chunks == 0 {
        return (false, false);
    }
    let structural = if rows_pct == 0 {
        false
    } else {
        let base = old_rows.max(1);
        let drift = new_rows.abs_diff(old_rows).saturating_mul(100);
        drift > base.saturating_mul(rows_pct as u64)
    };
    let chunk_drift = new_chunks.abs_diff(old_chunks).saturating_mul(100);
    let inheritable = chunk_drift <= old_chunks.max(1).saturating_mul(chunk_pct as u64);
    (structural, inheritable)
}

/// 活锁治理(任务3)：重拉清单后的继承进度（纯函数）。
///
/// `inheritable == true` 时旧进度 `old_done` 按 index 继承（clamp 到新清单块数），
/// 与 D3 逐块验证出的 `verified_done` 取 max；结构性漂移则从零（保留验证值）。
pub fn inherit_done_chunks(
    old_done: u64,
    new_total: u64,
    inheritable: bool,
    verified_done: u64,
) -> u64 {
    if !inheritable {
        return verified_done;
    }
    verified_done.max(old_done.min(new_total))
}

// ========================================================================
// v10(C)：块传输窗口调度器 —— 窗口化并发预取的状态机（纯逻辑，可单测）。
// ========================================================================

/// v10(C)：bootstrap 块传输的窗口状态机。
///
/// 旧实现是**链式**传输：收到第 i 块响应才请求第 i+1 块，单块「生成+传输+RTT」
/// 延迟线性叠加，实测吞吐 0.33MB/s（千兆内网跑不满 1/300）。窗口化后同时挂
///  个在途请求，乱序接收（块落地是幂等 upsert， 用 max 连续
/// 前缀推进，乱序安全）；全部块收齐即竣工，不再依赖 is_last 标记的到达顺序。
///
/// 纯状态机：不持锁不做 IO， 计算下一批应发送的 index， 驱动状态迁移；NAK/校验失败的块移出在途集合由  自动重发。
#[derive(Debug)]
pub struct ChunkWindow {
    total: u32,
    window: usize,
    /// 下一个未请求的 index（严格递增；重试块单独走 retry 队列）
    next_request: u32,
    /// 已请求未响应的 index 集合
    inflight: std::collections::HashSet<u32>,
    /// 每个在途 index 的发送时刻（reap_timed_out 用）
    sent_at: std::collections::HashMap<u32, Instant>,
    /// 已成功响应的 index 集合（恢复时以 0..done 为基数）
    received: std::collections::HashSet<u32>,
    /// 每 index 的失败次数（NAK/校验失败），超限触发重拉清单
    attempts: std::collections::HashMap<u32, u32>,
    /// 重试队列（失败/NAK 的块，fill() 优先重发）
    retry: std::collections::VecDeque<u32>,
    /// E2：最后一块成功落地（on_response）的时刻。窗口级空闲看门狗据此判定
    /// 「距最后一块落地已超过阈值仍有在途块」→ 在途块丢失（OOM 驱逐阻塞 actor 后
    /// 消息超时丢帧），回收重发。从未收到过块时为 None（由逐块 sent_at 超时兜底）。
    last_response_at: Option<Instant>,
    /// E2：窗口级空闲恢复已执行的次数。超过 `idle_max_retries` 即放弃本窗口重拉清单，
    /// 避免对端不可达时无限重刷同一批在途块。
    idle_recoveries: u32,
}

impl ChunkWindow {
    /// 恢复续传的连续前缀长度（持久化的 done_chunks），
    /// 其对应块视作已成功（基数进入 received）。
    pub fn new(total: u32, window: usize, resume_from: u32) -> Self {
        let resume = resume_from.min(total);
        let received = (0..resume).collect::<std::collections::HashSet<_>>();
        Self {
            total,
            window: window.max(1),
            next_request: resume,
            inflight: std::collections::HashSet::new(),
            sent_at: std::collections::HashMap::new(),
            received,
            attempts: std::collections::HashMap::new(),
            retry: std::collections::VecDeque::new(),
            last_response_at: None,
            idle_recoveries: 0,
        }
    }

    /// D批(D1)：以「已收块集合」初始化窗口 —— 除连续续传前缀外，还把 hash 比对一致的
    /// 非连续块标记为已完成，只请求其余块。
    ///
    /// `skip` 中 `< total` 的 index 全部进入 `received` 基数；`next_request` 从 0 起，
    /// [`fill`](Self::fill) 会跳过所有已在 `received` 中的 index。其余字段与 [`new`](Self::new)
    /// 一致。竣工判定仍为 `received.len() >= total`（与连续/非连续无关）；`done_prefix`
    /// 仍只反映最大连续前缀，供持久化续传使用。
    pub fn with_skip(total: u32, window: usize, skip: &std::collections::HashSet<u32>) -> Self {
        let mut received = std::collections::HashSet::new();
        for &i in skip {
            if i < total {
                received.insert(i);
            }
        }
        Self {
            total,
            window: window.max(1),
            next_request: 0,
            inflight: std::collections::HashSet::new(),
            sent_at: std::collections::HashMap::new(),
            received,
            attempts: std::collections::HashMap::new(),
            retry: std::collections::VecDeque::new(),
            last_response_at: None,
            idle_recoveries: 0,
        }
    }

    /// 计算当前应发送的 index 列表（填满窗口；重试块优先）。
    /// 传 0 表示使用自身 window 配置。
    pub fn fill(&mut self, max_inflight: usize) -> Vec<u32> {
        let cap = if max_inflight == 0 {
            self.window
        } else {
            max_inflight
        };
        let now = Instant::now();
        let mut out = Vec::new();
        // 重试块优先（失败计数不在此清零，由 on_response 成功时清）
        while self.inflight.len() < cap {
            match self.retry.pop_front() {
                Some(i) => {
                    if self.inflight.insert(i) {
                        self.sent_at.insert(i, now);
                        out.push(i);
                    }
                }
                None => break,
            }
        }
        while self.next_request < self.total && self.inflight.len() < cap {
            let i = self.next_request;
            self.next_request += 1;
            // D批(D1)：跳过已在 received 中的块（with_skip 注入的 hash 一致块）。
            // 对 new() 用法向后兼容：其 received 恰为 0..resume，而 next_request 从 resume
            // 起，不会命中已收集合。
            if self.received.contains(&i) {
                continue;
            }
            self.inflight.insert(i);
            self.sent_at.insert(i, now);
            out.push(i);
        }
        out
    }

    /// 记录一次成功响应。返回 true = 全部块收齐（竣工）。
    pub fn on_response(&mut self, index: u32) -> bool {
        self.inflight.remove(&index);
        self.sent_at.remove(&index);
        self.attempts.remove(&index);
        self.received.insert(index);
        self.last_response_at = Some(Instant::now());
        // 活锁治理(任务1)：E2 的「连续」空闲恢复计数在真进度出现时归零 ——
        // 否则窗口生命周期内累计 3 次就放弃，长传输（数百块）中途一次慢段即被误判不可恢复。
        self.idle_recoveries = 0;
        self.received.len() as u32 >= self.total
    }

    /// 记录一次失败（NAK/校验失败）：移出在途并进重试队列，返回累计失败次数。
    pub fn on_failure(&mut self, index: u32) -> u32 {
        self.inflight.remove(&index);
        self.sent_at.remove(&index);
        let e = self.attempts.entry(index).or_insert(0);
        *e = e.saturating_add(1);
        self.retry.push_back(index);
        *e
    }

    /// 回收超时未响应的在途块（网络丢帧/对端静默）：移出在途、移入重试队列，
    /// 返回被回收的 index 列表（调用方随后 fill() 会优先重发它们）。
    pub fn reap_timed_out(&mut self, timeout: Duration) -> Vec<u32> {
        let now = Instant::now();
        let expired: Vec<u32> = self
            .sent_at
            .iter()
            .filter_map(|(&i, &t)| {
                if now.duration_since(t) >= timeout {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();
        for i in &expired {
            self.inflight.remove(i);
            self.sent_at.remove(i);
            self.retry.push_back(*i);
        }
        expired
    }

    /// E2：窗口级空闲看门狗 —— 距最后一块落地（`on_response`）已超过 `idle_timeout`
    /// 且仍有在途块时，把全部在途块回收进重试队列（下次 `fill` 优先重发），返回被回收的
    /// index。覆盖 OOM 驱逐阻塞 bootstrap actor 后、在途块消息超时丢失、窗口永久卡住的
    /// 场景（与逐块 `reap_timed_out` 互补：后者管单发块无人应，本项管曾有进展后整体停摆）。
    ///
    /// 每次真正回收会计一次空闲恢复（`idle_recoveries` 自增）；调用方据此在超过
    /// `idle_max_retries` 后放弃本窗口、重拉清单。从未收到过块（`last_response_at == None`）
    /// 时不动作 —— 由逐块 sent_at 超时兜底。
    pub fn idle_reap(&mut self, idle_timeout: Duration) -> Vec<u32> {
        let Some(last) = self.last_response_at else {
            return Vec::new();
        };
        if self.inflight.is_empty() {
            return Vec::new();
        }
        if Instant::now().duration_since(last) < idle_timeout {
            return Vec::new();
        }
        let expired: Vec<u32> = self.inflight.iter().copied().collect();
        for i in &expired {
            self.inflight.remove(i);
            self.sent_at.remove(i);
            self.retry.push_back(*i);
        }
        self.idle_recoveries = self.idle_recoveries.saturating_add(1);
        expired
    }

    /// E2：窗口级空闲恢复已执行的次数。
    pub fn idle_recovery_count(&self) -> u32 {
        self.idle_recoveries
    }

    /// 全部块是否收齐。
    pub fn is_complete(&self) -> bool {
        self.received.len() as u32 >= self.total
    }

    /// 已收块数（含恢复基数）。
    pub fn received_count(&self) -> u32 {
        self.received.len() as u32
    }

    /// 最大连续完成前缀（持久化到 done_chunks，供重启后续传定位）。
    pub fn done_prefix(&self) -> u32 {
        let mut n = 0u32;
        while self.received.contains(&n) {
            n += 1;
        }
        n
    }

    /// 某 index 的累计失败次数。
    pub fn attempts_of(&self, index: u32) -> u32 {
        self.attempts.get(&index).copied().unwrap_or(0)
    }

    pub fn inflight_len(&self) -> usize {
        self.inflight.len()
    }

    /// 活锁治理(任务1)：任取一个在途 index（无序集合取最小，仅为退避记录提供
    /// 「当前卡在哪个块」的稳定锚点；空在途返回 None）。
    pub fn first_inflight(&self) -> Option<u32> {
        self.inflight.iter().copied().min()
    }

    pub fn total(&self) -> u32 {
        self.total
    }
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

    /// D批(D3)：verify_transport 50% 收紧规则 —— 实收必须 ≥ ⌊声明/2⌋；
    /// 声明 0 行必须实收 0；超收自然通过；奇数声明向下取整。
    #[test]
    fn test_verify_transport_half_threshold() {
        // 声明 0 行：实收必须为 0
        assert!(verify_transport(0, 0));
        assert!(!verify_transport(0, 1));
        // 声明 100：门槛 ⌊100/2⌋ = 50
        assert!(verify_transport(100, 50), "实收恰好半数应通过");
        assert!(!verify_transport(100, 49), "实收差一行到半数必须失败");
        assert!(!verify_transport(100, 0), "空回包必须失败");
        // 奇数声明 101：⌊101/2⌋ = 50（向下取整），实收 50 即通过
        assert!(verify_transport(101, 50), "奇数声明向下取整为 50");
        // 超收 / 充足实收正常通过
        assert!(verify_transport(200, 150), "实收远超半数应通过");
        assert!(!verify_transport(200, 99), "实收 99 < 门槛 100 必须失败");
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

    /// v10(C)：窗口化并发预取状态机 —— fill 填满窗口、乱序响应、重试、竣工。
    #[test]
    fn test_chunk_window_pipeline() {
        let mut w = ChunkWindow::new(10, 4, 0);
        // 初始填满窗口
        let batch = w.fill(0);
        assert_eq!(batch, vec![0, 1, 2, 3]);
        assert_eq!(w.inflight_len(), 4);
        // 乱序响应(先 2 后 0):done_prefix 只计连续前缀
        assert!(!w.on_response(2), "仅收 1 块不应竣工");
        assert_eq!(w.done_prefix(), 0);
        assert!(!w.on_response(0), "仅收 2 块不应竣工");
        assert_eq!(w.done_prefix(), 1);
        // 响应后补发:窗口回落,fill 补新块
        let batch = w.fill(0);
        assert_eq!(batch, vec![4, 5]);
        // 失败:NAK/校验失败移出在途进重试,attempt 计数
        assert_eq!(w.on_failure(1), 1);
        let batch = w.fill(0);
        assert_eq!(batch, vec![1], "失败块经重试队列优先重发");
        assert_eq!(w.attempts_of(1), 1);
        // 重发成功
        assert!(!w.on_response(1), "3/10 块不应竣工");
        assert_eq!(w.attempts_of(1), 0, "成功后清失败计数");
        // 补齐剩余在途响应(块 3/4/5),清空窗口后进入推进循环
        w.on_response(3);
        w.on_response(4);
        w.on_response(5);
        // 持续推进直到全部收齐(on_response 在收齐最后一块时返回 true,属预期)
        loop {
            let batch = w.fill(0);
            if batch.is_empty() {
                break;
            }
            for i in batch {
                w.on_response(i);
            }
        }
        assert!(w.is_complete());
        assert_eq!(w.done_prefix(), 10);
    }

    /// v10(C)：恢复续传 —— resume_from 前缀进入 received 基数,续传起点正确。
    #[test]
    fn test_chunk_window_resume() {
        let mut w = ChunkWindow::new(10, 4, 6);
        assert_eq!(w.received_count(), 6);
        assert_eq!(w.done_prefix(), 6);
        let batch = w.fill(0);
        assert_eq!(batch, vec![6, 7, 8, 9], "从 resume 点继续填窗");
        for i in batch {
            w.on_response(i);
        }
        assert!(w.is_complete());
    }

    /// v10(C)：全量继承(继承判定 done>=total)→ 构造即竣工。
    #[test]
    fn test_chunk_window_fully_inherited() {
        let mut w = ChunkWindow::new(5, 4, 5);
        assert!(w.is_complete());
        assert!(w.fill(0).is_empty(), "已全量继承不应再发任何请求");
    }

    /// v10(C)：超时回收 —— 在途块超过 timeout 未响应 → 移入重试队列，fill 优先重发。
    #[test]
    fn test_chunk_window_reap_timed_out() {
        let mut w = ChunkWindow::new(10, 4, 0);
        let batch = w.fill(0);
        assert_eq!(batch, vec![0, 1, 2, 3]);
        // 刚发出，不超时
        assert!(w.reap_timed_out(Duration::from_secs(30)).is_empty());
        // 模拟 sent_at 全部老化：手动把 sent_at 时间调到 1 小时前
        let old = Instant::now() - Duration::from_secs(3600);
        for t in w.sent_at.values_mut() {
            *t = old;
        }
        let expired = w.reap_timed_out(Duration::from_secs(30));
        assert_eq!(expired.len(), 4, "4 个在途块全部超时");
        assert_eq!(w.inflight_len(), 0, "超时块全部移出在途");
        // fill 会从重试队列取出重发
        let batch = w.fill(0);
        assert_eq!(batch.len(), 4, "超时块经重试队列重发");
        assert_eq!(w.inflight_len(), 4);
    }

    /// E2：窗口长时间无新块落地后，空闲看门狗回收仍在途的未完成块供重发；
    /// 且每次回收计一次恢复次数，供上层在超上限后放弃窗口。
    #[test]
    fn test_chunk_window_idle_reap_requeues_inflight() {
        let mut w = ChunkWindow::new(10, 4, 0);
        let batch = w.fill(0);
        assert_eq!(batch, vec![0, 1, 2, 3]);
        // 刚发出、尚未收过任何块 → last_response_at 为 None，不动作（逐块超时兜底）
        assert!(w.idle_reap(Duration::from_secs(60)).is_empty());
        // 收到一块（刷新 last_response_at），窗口仍有 3 个在途块 1/2/3
        assert!(!w.on_response(0));
        assert_eq!(w.inflight_len(), 3);
        // 刚收过块、未空闲 → 不回收
        assert!(w.idle_reap(Duration::from_secs(60)).is_empty());
        assert_eq!(w.idle_recovery_count(), 0);
        // 把「最后一块落地」拨到 61s 前 → 判定空闲，回收全部在途块
        w.last_response_at = Some(Instant::now() - Duration::from_secs(61));
        let reaped = w.idle_reap(Duration::from_secs(60));
        assert_eq!(reaped.len(), 3, "3 个在途块应被回收");
        assert_eq!(w.inflight_len(), 0, "回收后在途清空");
        assert_eq!(w.idle_recovery_count(), 1, "计一次空闲恢复");
        // fill 从重试队列取出回收块重发，并继续预取新块填满窗口
        let batch = w.fill(0);
        assert!(
            batch.contains(&1) && batch.contains(&2) && batch.contains(&3),
            "回收块 1/2/3 必须经重试队列重发, got={:?}",
            batch
        );
        assert_eq!(w.inflight_len(), 4, "窗口填满（3 回收块 + 预取新块）");
    }

    /// D批(D1)：with_skip —— hash 一致的非连续块进入 received 基数，首批 fill 跳过它们，
    /// 其余块正常请求；补齐后 is_complete()。
    #[test]
    fn test_chunk_window_with_skip() {
        let mut skip = std::collections::HashSet::new();
        skip.insert(0u32);
        skip.insert(2u32);
        skip.insert(5u32);
        let mut w = ChunkWindow::with_skip(10, 4, &skip);
        assert_eq!(w.received_count(), 3, "3 个 hash 一致块进入基数");
        // 首批跳过 0/2/5：应发 1,3,4,6
        let batch = w.fill(0);
        assert_eq!(batch, vec![1, 3, 4, 6], "首批不得包含已跳过的 0/2/5");
        assert_eq!(w.inflight_len(), 4);
        for &i in &[1, 3, 4, 6] {
            w.on_response(i);
        }
        // 持续推进直到全部收齐
        loop {
            let b = w.fill(0);
            if b.is_empty() {
                break;
            }
            for i in b {
                w.on_response(i);
            }
        }
        assert!(w.is_complete(), "收齐 10 块应竣工");
        assert_eq!(w.done_prefix(), 10);
    }

    /// D批(D1)：本地/对端清单逐块 hash 比对 —— 相同数据 hash/rows 全等；
    /// 数据变化后至少一块不等（证明比对能识别差异）。
    #[test]
    fn test_manifest_hash_compare_detects_diff() {
        let st = Storage::memory().unwrap();
        seed_nodes(&st, 500);
        let mf_a =
            build_repo_manifest_impl(&st, crate::federation::sync::repo_type::NODE, 200, 1, 1)
                .unwrap();
        let mf_b =
            build_repo_manifest_impl(&st, crate::federation::sync::repo_type::NODE, 200, 1, 1)
                .unwrap();
        assert_eq!(mf_a.chunks.len(), mf_b.chunks.len());
        for (a, b) in mf_a.chunks.iter().zip(mf_b.chunks.iter()) {
            assert_eq!(a.index, b.index);
            assert_eq!(&a.hash, &b.hash, "相同数据块 {} hash 应一致", a.index);
            assert_eq!(a.rows, b.rows);
        }
        // 追加 300 行（key 与前 500 不冲突）→ 至少一块 hash/rows 变化
        {
            let conn = st.connection();
            let conn = conn.lock().unwrap();
            for i in 500..800i64 {
                let ip = format!("10.0.{}.{}", i / 256, i % 256);
                let id = vec![(i % 256) as u8; 20];
                conn.execute(
                    "INSERT INTO dht_nodes (id, ip, port, l2_shard, deleted_at) VALUES (?1, ?2, ?3, 0, NULL)",
                    params![id, ip, 6881i64],
                )
                .unwrap();
            }
        }
        let mf_c =
            build_repo_manifest_impl(&st, crate::federation::sync::repo_type::NODE, 200, 1, 1)
                .unwrap();
        let mut any_diff = false;
        for a in &mf_a.chunks {
            if let Some(c) = mf_c.chunks.iter().find(|c| c.index == a.index) {
                if c.hash != a.hash || c.rows != a.rows {
                    any_diff = true;
                }
            }
        }
        assert!(any_diff, "数据变化后应至少有一块 hash/rows 不同");
    }

    /// 活锁治理(任务1)：传输失败退避表 —— 30s 基数逐次翻倍，600s 封顶；
    /// 首次失败（fails=0 按首次算）返回基数。
    #[test]
    fn test_backoff_delay_doubles_and_caps() {
        let base = Duration::from_secs(30);
        let max = Duration::from_secs(600);
        assert_eq!(backoff_delay(base, max, 0), Duration::from_secs(30));
        assert_eq!(backoff_delay(base, max, 1), Duration::from_secs(30));
        assert_eq!(backoff_delay(base, max, 2), Duration::from_secs(60));
        assert_eq!(backoff_delay(base, max, 3), Duration::from_secs(120));
        assert_eq!(backoff_delay(base, max, 4), Duration::from_secs(240));
        assert_eq!(backoff_delay(base, max, 5), Duration::from_secs(480));
        assert_eq!(
            backoff_delay(base, max, 6),
            Duration::from_secs(600),
            "封顶"
        );
        assert_eq!(
            backoff_delay(base, max, 50),
            Duration::from_secs(600),
            "长期失败恒为封顶值"
        );
        // 非法配置兜底：max < base 时不得产出小于基数的等待
        assert_eq!(backoff_delay(base, Duration::from_secs(1), 2), base);
    }

    /// 活锁治理(任务1-b/任务3)：清单漂移判定 —— total_rows >1% 判结构性漂移；
    /// total_chunks 差 ≤2% 可继承；无旧清单（首次）两者皆否；rows_pct=0 关闭判定。
    #[test]
    fn test_manifest_drift_and_inheritance() {
        // 同结构小漂移：1000 行 → 1005 行（0.5% ≤ 1%）、块数不变 → 非结构性、可继承
        let (structural, inheritable) = manifest_drift(1000, 50, 1005, 50, 1, 2);
        assert!(!structural);
        assert!(inheritable);
        // 行数漂移 2%（>1%）→ 结构性漂移；块数差 4%（>2%）→ 不可继承
        let (structural, inheritable) = manifest_drift(1000, 50, 1020, 52, 1, 2);
        assert!(structural, "total_rows 变化 >1% 必须判结构性漂移");
        assert!(!inheritable, "total_chunks 差 >2% 不得继承");
        // 边界：块数恰差 2% → 可继承
        let (_, inheritable) = manifest_drift(100_000, 50, 100_000, 51, 1, 2);
        assert!(inheritable, "块数差 2%（1/50）应可继承");
        // 首次 bootstrap（无旧清单）→ 无漂移、无可继承
        let (structural, inheritable) = manifest_drift(0, 0, 5000, 10, 1, 2);
        assert!(!structural && !inheritable);
        // rows_pct=0 关闭行数漂移判定
        let (structural, _) = manifest_drift(1000, 50, 2000, 50, 0, 2);
        assert!(!structural, "rows_pct=0 应关闭结构性漂移判定");
        // 继承值 clamp：旧进度 40、新清单 50 块、验证前缀 3 → 继承 40；
        // 结构性漂移 → 只保留验证前缀 3；旧进度超新清单块数 → clamp 到新块数
        assert_eq!(inherit_done_chunks(40, 50, true, 3), 40);
        assert_eq!(inherit_done_chunks(40, 50, false, 3), 3);
        assert_eq!(inherit_done_chunks(60, 50, true, 3), 50);
    }

    /// 活锁治理(任务4)：停滞判定 —— 距最近块落地超过阈值即停滞；
    /// 旧行（last_progress_ms=0）回落 updated_ms；新进度不停滞。
    #[test]
    fn test_progress_stalled() {
        let now = 1_700_000_000_000i64;
        // 301s 前落地，阈值 300s → 停滞
        assert!(progress_stalled(now - 301_000, now - 301_000, now, 300_000));
        // 299s 前落地 → 未停滞
        assert!(!progress_stalled(
            now - 299_000,
            now - 299_000,
            now,
            300_000
        ));
        // last_progress 新、updated 旧 → 以 last_progress 为准（未停滞）
        assert!(!progress_stalled(now - 1_000, now - 900_000, now, 300_000));
        // 旧行回落 updated_ms：updated 在 400s 前 → 停滞
        assert!(progress_stalled(0, now - 400_000, now, 300_000));
        // 旧行 updated 新鲜 → 未停滞
        assert!(!progress_stalled(0, now - 100_000, now, 300_000));
    }

    /// 活锁治理(任务1)：E2 空闲恢复计数在真进度（on_response）后归零 ——
    /// 长传输中途一次慢段不再被累计成「连续 3 次放弃窗口」。
    #[test]
    fn test_chunk_window_idle_recoveries_reset_on_progress() {
        let mut w = ChunkWindow::new(10, 4, 0);
        let batch = w.fill(0);
        assert_eq!(batch, vec![0, 1, 2, 3]);
        assert!(!w.on_response(0));
        // 拨动时钟制造一次空闲回收
        w.last_response_at = Some(Instant::now() - Duration::from_secs(61));
        assert_eq!(w.idle_reap(Duration::from_secs(60)).len(), 3);
        assert_eq!(w.idle_recovery_count(), 1);
        // 回收块经重试队列重发，重新进入在途（3 回收块 + 1 预取新块）
        let batch = w.fill(0);
        assert_eq!(batch.len(), 4);
        // 新块落地 → 连续空闲计数归零
        assert!(!w.on_response(1));
        assert_eq!(w.idle_recovery_count(), 0, "真进度必须清零连续空闲恢复计数");
        assert_eq!(
            w.first_inflight(),
            Some(2),
            "剩余在途块的最小 index（2/3/4 中取 2）"
        );
        w.on_response(2);
        w.on_response(3);
        w.on_response(4);
        assert_eq!(w.first_inflight(), None, "无在途块时返回 None");
    }
}
