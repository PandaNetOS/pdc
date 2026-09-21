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
//! | ⑥ 校验 | 按块重算 DB 摘要与 manifest 哈希比对；切 delta + 周期反熵兜底 | — |
//!
//! **一致性说明**：本实现以「**显式区间边界的逻辑分块 + W0 水位 + 末块哈希校验**」替代物理
//! 快照文件（`VACUUM INTO`）。传输期间若有落在 `[lo,hi)` 内新写入的行，会使该块哈希与
//! manifest 不符 —— 由阶段 ⑥ 的校验**发现**并重拉该块（幂等，无副作用），而不是静默出错。
//! 这样避免引入快照文件的生命周期管理（清理、TTL、多节点复用）；物理快照为后续优化。
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
pub fn init_bootstrap_table(conn: &SqliteConnection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS bootstrap_state (
            repo       INTEGER PRIMARY KEY,  -- 0..3（按 repo 分别 bootstrap，铁律 7）
            payload    BLOB NOT NULL,        -- BootstrapProgress 的 JSON
            manifest   BLOB,                 -- BootstrapManifest 的 JSON（未取到时为 NULL）
            updated_ms INTEGER NOT NULL
        );
        "#,
    )?;
    Ok(())
}

/// 内容哈希：对按 key 升序的 `(key, data_hash)` 流取 blake3（每段带 4 字节长度前缀）。
///
/// 与 `range_reconcile::range_digest` 同构：服务端建 manifest、客户端落地后重算校验都用它。
pub fn chunk_hash(rows: &[(Vec<u8>, Vec<u8>)]) -> [u8; 32] {
    crate::federation::sync::range_reconcile::range_digest(rows)
}

impl Storage {
    /// 保存某 repo 的 bootstrap 进度（幂等 upsert）。
    pub fn bootstrap_save(
        &self,
        progress: &BootstrapProgress,
        manifest: Option<&BootstrapManifest>,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(progress)?;
        let mf = match manifest {
            Some(m) => Some(serde_json::to_vec(m)?),
            None => None,
        };
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "INSERT INTO bootstrap_state (repo, payload, manifest, updated_ms) VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(repo) DO UPDATE SET payload = excluded.payload, \
             manifest = COALESCE(excluded.manifest, bootstrap_state.manifest), updated_ms = excluded.updated_ms",
            params![progress.repo as i64, payload, mf, progress.updated_ms],
        )?;
        Ok(())
    }

    /// 读取某 repo 的 bootstrap 进度与清单。
    pub fn bootstrap_load(
        &self,
        repo: u8,
    ) -> anyhow::Result<Option<(BootstrapProgress, Option<BootstrapManifest>)>> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        let row = conn.query_row(
            "SELECT payload, manifest FROM bootstrap_state WHERE repo = ?1",
            params![repo as i64],
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

    /// 清除某 repo 的 bootstrap 进度（完成或放弃时调用）。
    pub fn bootstrap_clear(&self, repo: u8) -> anyhow::Result<()> {
        let conn = self.connection();
        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "DELETE FROM bootstrap_state WHERE repo = ?1",
            params![repo as i64],
        )?;
        Ok(())
    }

    /// 列出所有未完成的 bootstrap 进度（重启后恢复用）。
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

/// 为 NODE repo 构建 bootstrap 清单（流式分块，内存开销 O(chunk_rows)）。
///
/// 逐块推进游标：每块取 `chunk_rows + 1` 行，多取的 1 行用作下一块的 `lo`（即本块 `hi`），
/// 使 `[lo, hi)` 恰好覆盖 `chunk_rows` 行；末块的 `hi = None`（+∞）。
pub fn build_node_manifest(
    storage: &Storage,
    chunk_rows: u32,
    w0_seq: u64,
    version: u32,
) -> anyhow::Result<BootstrapManifest> {
    build_repo_manifest_impl(
        storage,
        crate::federation::sync::repo_type::NODE,
        chunk_rows,
        w0_seq,
        version,
    )
}

/// v7：全 repo 通用清单构建（与 [`build_node_manifest`] 同算法，
/// 区间读取换成 `load_repo_key_hashes_in_range(repo, …)`）。
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

    Ok(BootstrapManifest {
        repo: crate::federation::protocol::repo_type::NODE,
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

/// 校验收到的行是否与清单块哈希一致。
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
        let mf = build_node_manifest(&st, 256, 42, 1).unwrap();
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
        let mf = build_node_manifest(&st, 100, 0, 1).unwrap();
        assert_eq!(mf.total_rows, 0);
        assert!(mf.chunks.is_empty());
    }

    #[test]
    fn test_bootstrap_state_roundtrip() {
        let st = Storage::memory().unwrap();
        assert!(st.bootstrap_load(1).unwrap().is_none());
        let mut p = BootstrapProgress::new(1, vec![9u8; 20], now_ms());
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

        let (got, got_mf) = st.bootstrap_load(1).unwrap().unwrap();
        assert_eq!(got.done_chunks, 5);
        assert_eq!(got.phase, BootstrapPhase::Transfer);
        assert_eq!(got.ratio(), 0.5);
        assert_eq!(got_mf.unwrap().w0_seq, 777);
        assert_eq!(st.bootstrap_list().unwrap().len(), 1);
        st.bootstrap_clear(1).unwrap();
        assert!(st.bootstrap_load(1).unwrap().is_none());
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
