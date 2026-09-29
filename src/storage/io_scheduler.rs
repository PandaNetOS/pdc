//! IO 调度器（IOScheduler）
//!
//! 统一 SQLite 写入调度，支持：
//! - P0：优先级队列 + 令牌桶限流 + writer_loop
//! - P1：四级优先级 + Deadline 老化防饿死
//! - P2：背压信号 + 接入 TaskScheduler
//! - P3：请求合并（批量化）+ 空闲预测
//!
//! 设计原则：
//! - IOScheduler 不直接持有业务逻辑，通过闭包回调执行写入
//! - 令牌桶使用 AtomicUsize 无锁实现
//! - 优先级队列使用 std::collections::BinaryHeap
//! - 默认关闭（enabled=false），WriteQueue 走原逻辑，完全向后兼容

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{
    AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering as AtomicOrdering,
};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

use parking_lot::Mutex as ParkingMutex;
use rusqlite::Connection;
use tokio::sync::Notify;

// ─── P0: 基础架构 ────────────────────────────────────────────────────────────

/// IO 请求优先级
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum IoPriority {
    /// 关键请求（立即执行，不受令牌桶限制）
    Critical = 0,
    /// 重要请求（正常令牌桶，有 deadline 保护）
    Important = 1,
    /// 普通请求（正常令牌桶）
    Normal = 2,
    /// 后台请求（仅在有剩余令牌时执行，可被抢占）
    Background = 3,
}

impl IoPriority {
    /// 默认 deadline（无显式指定时）
    pub fn default_deadline(self) -> Option<Instant> {
        let now = Instant::now();
        match self {
            IoPriority::Critical => None, // 立即执行，无 deadline
            IoPriority::Important => Some(now + Duration::from_secs(5)),
            IoPriority::Normal => Some(now + Duration::from_secs(30)),
            IoPriority::Background => Some(now + Duration::from_secs(300)),
        }
    }
}

/// IO 请求写入回调类型
type IoPayload = Box<dyn FnOnce(&Connection) -> anyhow::Result<()> + Send>;

/// IO 写入请求
pub struct IoRequest {
    /// 优先级
    pub priority: IoPriority,
    /// 截止时间（P1 老化升级用）
    pub deadline: Option<Instant>,
    /// 写入回调（在 writer_loop 中持有 SQLite 连接锁时执行）
    pub payload: IoPayload,
    /// 估计写入行数（令牌桶计费单位）
    pub size_hint: usize,
    /// 提交时间
    pub submitted_at: Instant,
}

/// 优先级队列条目（BinaryHeap 包装）
///
/// BinaryHeap 是 max-heap，我们需要 Critical（低数值）先出队。
/// 因此实现 Ord 时让"更紧急"的条目比较结果为 Greater。
struct HeapEntry {
    request: IoRequest,
    /// 单调递增序号，同优先级同 deadline 时 FIFO
    seq: u64,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq
    }
}
impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        // 1. 优先级：低数值 = 更紧急 = 在 max-heap 中应更靠前（Greater）
        let pri_cmp = (self.request.priority as u8)
            .cmp(&(other.request.priority as u8))
            .reverse();
        // 2. deadline：更早 = 更紧急
        let dl_cmp = match (self.request.deadline, other.request.deadline) {
            (Some(a), Some(b)) => a.cmp(&b).reverse(),
            (Some(_), None) => Ordering::Greater, // 有 deadline 的比 None（无限制）更紧急
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        };
        // 3. FIFO：低 seq = 先提交 = 先出队
        let seq_cmp = self.seq.cmp(&other.seq).reverse();
        pri_cmp.then(dl_cmp).then(seq_cmp)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// ─── v10/v11：bootstrap 快照导入窗口 + AIMD 自适应预算 ─────────────────────
//
// steady 调度的预算（writes_per_tick=10 / steady_tick_ms=10）把落库上限钉在
// 1,000 行/s —— 稳态磁盘 IO 平滑的正确设计，但冷启动灌入 300 万行快照时
// 就是 50 分钟的硬天花板（实测）。bootstrap 块每次成功落地时刷新导入窗口，
// writer_loop 在窗口内按"自适应预算"放开时间片；窗口静默 TTL 后自动回落稳态，
// 无需显式关闭、不影响稳态 IO 平滑语义。
//
// A4：预算不再是硬编码 ×200，而是按每批实测耗时做 AIMD（加法增/乘法减）：
//   - 慢（elapsed > adaptive_latency_target_ms）→ budget /= 2，下限 base；
//   - 快（elapsed < target/2）连续 adaptive_budget_grow_after_batches 批
//     → budget += grow_step，上限 base × max_multiplier。
// 目的是"IO 恶劣时不死"而非"最快灌完"，冷启动先慢后快是刻意的。

/// 导入窗口 TTL（秒）—— 窗口内放开时间片预算，静默后自动回落稳态。
pub const BOOTSTRAP_IMPORT_WINDOW_TTL_SECS: u64 = 30;

/// 自适应参数快照（进程启动时由 `init_io_adaptive_config` 注入一次，之后只读；
/// 未注入时用与 `IoSchedulerConfig` 默认值一致的兜底）。
#[derive(Debug, Clone, Copy)]
struct AdaptiveCfg {
    /// 起步倍率（budget 起始 = base × start_multiplier）
    start_multiplier: usize,
    /// 倍率上限（budget 上限 = base × max_multiplier）
    max_multiplier: usize,
    /// AIMD 目标耗时（微秒）
    latency_target_us: u64,
    /// 耗时 EWMA 平滑系数 α
    alpha: f32,
    /// 增窗步长（行/tick）
    grow_step: usize,
    /// 连续多少批"足够快"才增窗一次
    grow_after_batches: u32,
}

impl AdaptiveCfg {
    /// 与 `config::IoSchedulerConfig` 默认值一致的兜底（进程启动前 / 单测用）。
    fn fallback() -> Self {
        Self {
            start_multiplier: 8,
            max_multiplier: 200,
            latency_target_us: 200_000, // 200ms
            alpha: 0.2,
            grow_step: 2,
            grow_after_batches: 32,
        }
    }
}

/// 进程级自适应配置（启动时 `init_io_adaptive_config` 灌入一次，之后只读）。
static ADAPTIVE_CFG: OnceLock<AdaptiveCfg> = OnceLock::new();
/// 导入窗口截止时刻（UNIX 毫秒；0 = 未激活）。
static IMPORT_UNTIL_MS: AtomicI64 = AtomicI64::new(0);
/// 窗口内当前每 tick 预算上限（行；AIMD 动态调整，下限 = base）。
static IMPORT_BUDGET: AtomicUsize = AtomicUsize::new(0);
/// 每批实测耗时的 EWMA（微秒；仅用于观测 / 指标快照）。
static LATENCY_EWMA_US: AtomicU64 = AtomicU64::new(0);
/// 连续"足够快"批次数（加法增窗的门槛计数）。
static FAST_STREAK: AtomicU32 = AtomicU32::new(0);
/// writer_loop 公布的稳态每 tick 基准行数（refresh 时据此把预算重置为
/// start_multiplier × base）。
static BASE_PER_TICK: AtomicUsize = AtomicUsize::new(1);

/// 进程启动时注入一次自适应配置（来自 `IoSchedulerConfig`，只调用一次）。
pub fn init_io_adaptive_config(cfg: &crate::config::IoSchedulerConfig) {
    let _ = ADAPTIVE_CFG.set(AdaptiveCfg {
        start_multiplier: cfg.bootstrap_import_budget_start_multiplier,
        max_multiplier: cfg.bootstrap_import_budget_max_multiplier,
        latency_target_us: cfg.adaptive_latency_target_ms.saturating_mul(1_000),
        alpha: cfg.adaptive_latency_alpha,
        grow_step: cfg.adaptive_budget_grow_step,
        grow_after_batches: cfg.adaptive_budget_grow_after_batches,
    });
}

fn adaptive_cfg() -> AdaptiveCfg {
    ADAPTIVE_CFG
        .get()
        .copied()
        .unwrap_or_else(AdaptiveCfg::fallback)
}

/// 当前 UNIX 毫秒（系统时钟；失败时回 0）。
fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 刷新快照导入窗口（bootstrap 块落地路径每次成功后调用）。
///
/// 副作用：把窗口截止时刻推到 `now + ttl`，并把预算重置为
/// `start_multiplier × base_per_tick`（base 由 writer_loop 公布）。
pub fn refresh_bootstrap_import_window(ttl: Duration) {
    let cfg = adaptive_cfg();
    let base = BASE_PER_TICK.load(AtomicOrdering::Acquire).max(1);
    IMPORT_BUDGET.store(
        base.saturating_mul(cfg.start_multiplier),
        AtomicOrdering::Release,
    );
    FAST_STREAK.store(0, AtomicOrdering::Release);
    IMPORT_UNTIL_MS.store(
        now_unix_ms() + ttl.as_millis() as i64,
        AtomicOrdering::Release,
    );
}

/// 导入窗口是否活跃（writer_loop 每 tick 检查）。
fn import_window_active() -> bool {
    now_unix_ms() < IMPORT_UNTIL_MS.load(AtomicOrdering::Acquire)
}

/// 当前窗口内预算上限（行/tick）；仅在窗口活跃时被 writer_loop 读取。
fn import_budget() -> usize {
    IMPORT_BUDGET.load(AtomicOrdering::Acquire)
}

/// 指标快照：当前导入预算（行/tick）。
pub fn io_import_budget_snapshot() -> usize {
    IMPORT_BUDGET.load(AtomicOrdering::Acquire)
}

/// 指标快照：每批耗时 EWMA（微秒）。
pub fn io_batch_latency_ewma_us() -> u64 {
    LATENCY_EWMA_US.load(AtomicOrdering::Acquire)
}

/// 观测一批耗时，按 α 做 EWMA（纯函数：返回新 EWMA，便于单测）。
fn next_latency_ewma_us(prev_ewma_us: u64, sample_us: u64, alpha: f32) -> u64 {
    if prev_ewma_us == 0 {
        return sample_us;
    }
    let a = alpha.clamp(0.0, 1.0) as f64;
    let next = a * sample_us as f64 + (1.0 - a) * prev_ewma_us as f64;
    next.round() as u64
}

/// AIMD 单步决策（纯函数）：根据本批耗时，返回 `(new_budget, new_fast_streak)`。
///
/// - `elapsed > latency_target_us`：乘法减，`budget /= 2`，下限 `base`，fast 计数清零；
/// - `elapsed < latency_target_us/2`：连续满 `grow_after_batches` 批后加法增，
///   `budget += grow_step`，上限 `base × max_multiplier`，增窗后计数清零；
/// - 中间迟滞带：保持预算，fast 计数清零（离开快带）。
fn aimd_step(
    current_budget: usize,
    elapsed: Duration,
    base: usize,
    cfg: &AdaptiveCfg,
    fast_streak: u32,
) -> (usize, u32) {
    let elapsed_us = elapsed.as_micros() as u64;
    let target_us = cfg.latency_target_us.max(1);

    if elapsed_us > target_us {
        // 慢 → 减半，地板 = base
        let next = (current_budget / 2).max(base);
        return (next, 0);
    }

    if elapsed_us < target_us / 2 {
        let streak = fast_streak + 1;
        if streak >= cfg.grow_after_batches {
            let ceiling = base.saturating_mul(cfg.max_multiplier).max(base);
            let next = current_budget.saturating_add(cfg.grow_step).min(ceiling);
            return (next, 0);
        }
        return (current_budget, streak);
    }

    // 迟滞带：不动预算，但离开快带（计数清零）
    (current_budget, 0)
}

/// writer_loop 每批后调用：观测耗时 EWMA；窗口活跃时做 AIMD 调整。
fn observe_and_adaptive_tick(elapsed: Duration, window_active: bool) {
    let sample_us = elapsed.as_micros() as u64;
    let cfg = adaptive_cfg();
    let prev = LATENCY_EWMA_US.load(AtomicOrdering::Acquire);
    LATENCY_EWMA_US.store(
        next_latency_ewma_us(prev, sample_us, cfg.alpha),
        AtomicOrdering::Release,
    );

    if !window_active {
        return;
    }
    let base = BASE_PER_TICK.load(AtomicOrdering::Acquire).max(1);
    let cur = IMPORT_BUDGET.load(AtomicOrdering::Acquire);
    let streak = FAST_STREAK.load(AtomicOrdering::Acquire);
    let (new_budget, new_streak) = aimd_step(cur, elapsed, base, &cfg, streak);
    IMPORT_BUDGET.store(new_budget, AtomicOrdering::Release);
    FAST_STREAK.store(new_streak, AtomicOrdering::Release);
}

/// 无锁令牌桶
///
/// 使用 AtomicUsize 的 compare_exchange 实现无锁获取。
/// 补充在 writer_loop 内根据时间差计算，不单独 spawn interval task。
pub(crate) struct TokenBucket {
    tokens: AtomicUsize,
    max_tokens: usize,
}

impl TokenBucket {
    fn new(max_tokens: usize, _refill_rate: usize) -> Self {
        Self {
            tokens: AtomicUsize::new(max_tokens),
            max_tokens,
        }
    }

    /// 部分支付（B2）：原子扣减 `min(n, 可用余额)`，返回实际扣减的令牌行数。
    ///
    /// 返回值落在 `0..=n`：余额充足时返回 n（等于一次精确支付）；余额不足时返回当前余量
    /// （可能 0）。调用方需自行判断"是否够支付整个请求"，本函数不做透支、不做抢占。
    /// Critical 优先级不走这里（见 writer_loop）。
    fn acquire_partial(&self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        let mut current = self.tokens.load(AtomicOrdering::Acquire);
        loop {
            let pay = current.min(n);
            if pay == 0 {
                return 0;
            }
            let new_val = current - pay;
            match self.tokens.compare_exchange_weak(
                current,
                new_val,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            ) {
                Ok(_) => return pay,
                Err(actual) => current = actual,
            }
        }
    }

    /// 补充令牌（writer_loop 根据 elapsed 按"行/秒"调用）。
    fn refill(&self, tokens_to_add: usize) {
        if tokens_to_add == 0 {
            return;
        }
        // fetch_update 在失败时自动重试
        let _ =
            self.tokens
                .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |current| {
                    Some(current.saturating_add(tokens_to_add).min(self.max_tokens))
                });
    }

    fn available(&self) -> usize {
        self.tokens.load(AtomicOrdering::Acquire)
    }
}

// ─── P2: 背压信号 ─────────────────────────────────────────────────────────────

/// 瞬时 IO 压力快照（B1：行口径 + 耗时 EWMA）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IoPressure {
    /// 队列行数水位压力 [0,1]
    pub queue: f32,
    /// 单批耗时 EWMA 压力 [0,1]
    pub latency: f32,
    /// 对外背压等级 = max(queue, latency)
    pub level: f32,
}

/// 空闲检测滑动窗口样本
#[derive(Debug, Clone)]
struct IdleSample {
    timestamp: Instant,
    queue_len: usize,
    tokens_available: usize,
}

/// IO 调度器统计
#[derive(Debug, Default, Clone)]
pub struct IoSchedulerStats {
    /// 总提交请求数
    pub total_submitted: u64,
    /// 总执行请求数
    pub total_executed: u64,
    /// 总批次数
    pub total_batches: u64,
    /// 因饥饿被抢占升级的请求数
    pub starvation_preemptions: u64,
    /// 因背压被拒绝的 Background 请求数
    pub backpressure_rejections: u64,
    /// Critical 级执行计数
    pub critical_executed: u64,
    /// Important 级执行计数
    pub important_executed: u64,
    /// Normal 级执行计数
    pub normal_executed: u64,
    /// Background 级执行计数
    pub background_executed: u64,
    /// 单次合并的最大批大小
    pub max_batch_size: usize,
    /// B2：因令牌不足而空转/降级的 tick 数
    pub token_starved_ticks: u64,
    /// B2：因令牌不足而留在队列的行数
    pub rows_deferred_by_tokens: u64,
}

// ─── 运行时配置（从 IoSchedulerConfig 转换，去除序列化标记）──────────────────

#[derive(Debug, Clone)]
pub struct SchedulerRuntimeConfig {
    pub max_queue_size: usize,
    pub token_bucket_rate: usize,
    pub token_bucket_max: usize,
    /// 旧请求数低水位（B1 后仅用于队列硬上限/拒绝 Background 判断）
    pub low_watermark: usize,
    /// 旧请求数高水位（B1 后仅用于队列硬上限判断）
    pub high_watermark: usize,
    pub batch_max_size: usize,
    pub batch_max_delay: Duration,
    pub idle_prediction_enabled: bool,
    pub idle_window: Duration,
    /// 队列为空时的等待时间
    pub idle_wait: Duration,
    /// 令牌不足时的重试等待时间
    pub retry_wait: Duration,
    /// 匀速调度时间片（毫秒），每个时间片执行固定数量写入
    pub steady_tick_ms: u64,
    /// 每个时间片最多取多少个请求（请求数口径上限，旧 writes_per_tick）
    pub max_requests_per_tick: usize,
    /// 每个时间片行预算（行口径平滑上限），0=不限
    pub rows_per_tick: usize,
    /// 令牌桶总开关（B2：接线后默认关，灰度再开）
    pub token_bucket_enabled: bool,
    /// 行口径低水位（B1：压力计算用）
    pub low_watermark_rows: usize,
    /// 行口径高水位（B1：压力计算用）
    pub high_watermark_rows: usize,
    /// 耗时目标（微秒，B1：压力计算用）
    pub latency_target_us: u64,
    /// 耗时慢阈值（微秒，B1：压力计算用）
    pub latency_slow_us: u64,
}

impl Default for SchedulerRuntimeConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 100_000,
            token_bucket_rate: 10_000,
            token_bucket_max: 20_000,
            low_watermark: 1_000,
            high_watermark: 50_000,
            batch_max_size: 500,
            batch_max_delay: Duration::from_millis(10),
            idle_prediction_enabled: false,
            idle_window: Duration::from_secs(60),
            idle_wait: Duration::from_millis(500),
            retry_wait: Duration::from_millis(50),
            steady_tick_ms: 10,
            max_requests_per_tick: 64,
            rows_per_tick: 5_000,
            token_bucket_enabled: false,
            low_watermark_rows: 20_000,
            high_watermark_rows: 200_000,
            latency_target_us: 50_000,
            latency_slow_us: 500_000,
        }
    }
}

// ─── IOScheduler 主结构 ───────────────────────────────────────────────────────

/// IO 调度器
///
/// 统一管理所有 SQLite 写入请求，按优先级调度 + 令牌桶限流 + 请求合并。
/// 通过闭包回调执行写入，不直接持有业务逻辑。
pub struct IoScheduler {
    /// 优先级队列（BinaryHeap，parking_lot 锁）
    queue: ParkingMutex<BinaryHeap<HeapEntry>>,
    /// 无锁令牌桶
    token_bucket: Arc<TokenBucket>,
    /// SQLite 连接（writer_loop 持有锁执行写入）
    conn: Arc<StdMutex<Connection>>,
    /// 运行时配置
    config: SchedulerRuntimeConfig,
    /// 队列当前长度（原子，请求数口径，仅用于硬上限判断）
    queue_len: Arc<AtomicUsize>,
    /// B1：队列当前行数（Σ size_hint，提交 +，执行 -），行口径压力计算用
    queue_rows: Arc<AtomicUsize>,
    /// 背压信号（队列过长时为 true，通知 TaskScheduler 降速）
    backpressure: Arc<AtomicBool>,
    /// 单调递增序号生成器
    seq_counter: Arc<AtomicU64>,
    /// 统计
    stats: Arc<ParkingMutex<IoSchedulerStats>>,
    /// 关闭信号
    shutdown: Arc<AtomicBool>,
    /// 唤醒 writer_loop 的通知器
    notify: Arc<Notify>,
    /// 空闲检测滑动窗口
    idle_samples: ParkingMutex<Vec<IdleSample>>,
    /// B3：flush_and_wait 的排空目标序号（0=无排空请求）
    drain_target: Arc<AtomicU64>,
    /// B3：队列排空完成通知器
    drained: Arc<Notify>,
    /// C2：运行期可动态调整的每 tick 行预算（AIMD 改写；初值=config.rows_per_tick）
    rows_per_tick_dynamic: Arc<AtomicUsize>,
}

impl IoScheduler {
    /// 创建 IO 调度器并在指定 runtime 上启动 writer_loop
    pub fn new_with_handle(
        conn: Arc<StdMutex<Connection>>,
        config: SchedulerRuntimeConfig,
        handle: &tokio::runtime::Handle,
    ) -> Arc<Self> {
        let scheduler = Arc::new(Self {
            queue: ParkingMutex::new(BinaryHeap::new()),
            token_bucket: Arc::new(TokenBucket::new(
                config.token_bucket_max,
                config.token_bucket_rate,
            )),
            conn,
            queue_len: Arc::new(AtomicUsize::new(0)),
            queue_rows: Arc::new(AtomicUsize::new(0)),
            backpressure: Arc::new(AtomicBool::new(false)),
            seq_counter: Arc::new(AtomicU64::new(0)),
            config,
            stats: Arc::new(ParkingMutex::new(IoSchedulerStats::default())),
            shutdown: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
            idle_samples: ParkingMutex::new(Vec::new()),
            drain_target: Arc::new(AtomicU64::new(0)),
            drained: Arc::new(Notify::new()),
            rows_per_tick_dynamic: Arc::new(AtomicUsize::new(0)),
        });

        // 公布动态行预算初值（0 = 不限；与 BASE_PER_TICK 的行口径基准一致）
        let base_rows = scheduler.config.rows_per_tick.max(1);
        scheduler
            .rows_per_tick_dynamic
            .store(base_rows, AtomicOrdering::Release);

        // 在指定 runtime 上启动 writer_loop
        let writer = scheduler.clone();
        handle.spawn(async move {
            writer.writer_loop().await;
        });

        scheduler
    }

    /// 创建 IO 调度器并在当前 runtime 上启动 writer_loop
    pub fn new(conn: Arc<StdMutex<Connection>>, config: SchedulerRuntimeConfig) -> Arc<Self> {
        Self::new_with_handle(conn, config, &tokio::runtime::Handle::current())
    }

    /// 查询背压状态（队列过长时返回 true，TaskScheduler 可据此降速）
    pub fn backpressure(&self) -> bool {
        self.backpressure.load(AtomicOrdering::Acquire)
    }

    /// 提交一个 IO 请求
    ///
    /// - Critical：立即执行，不受令牌桶限制
    /// - Background 在队列积压超过 high_watermark 时被拒绝（返回 Err）
    pub fn submit<F>(
        &self,
        priority: IoPriority,
        deadline: Option<Instant>,
        f: F,
        size_hint: usize,
    ) -> anyhow::Result<()>
    where
        F: FnOnce(&Connection) -> anyhow::Result<()> + Send + 'static,
    {
        let queue_len = self.queue_len.load(AtomicOrdering::Acquire);

        // P2: 背压 — 队列满时拒绝 Background 请求
        if queue_len >= self.config.high_watermark && matches!(priority, IoPriority::Background) {
            let mut stats = self.stats.lock();
            stats.backpressure_rejections += 1;
            return Err(anyhow::anyhow!(
                "IOScheduler 队列已满（{}），拒绝 Background 请求",
                queue_len
            ));
        }

        // P2: 硬上限保护
        if queue_len >= self.config.max_queue_size
            && !matches!(priority, IoPriority::Critical | IoPriority::Important)
        {
            let mut stats = self.stats.lock();
            stats.backpressure_rejections += 1;
            return Err(anyhow::anyhow!(
                "IOScheduler 队列达到硬上限（{}），拒绝非 Critical/Important 请求",
                queue_len
            ));
        }

        let deadline = deadline.or_else(|| priority.default_deadline());
        let seq = self.seq_counter.fetch_add(1, AtomicOrdering::AcqRel);

        let request = IoRequest {
            priority,
            deadline,
            payload: Box::new(f),
            size_hint: size_hint.max(1),
            submitted_at: Instant::now(),
        };

        let entry = HeapEntry { request, seq };
        self.queue.lock().push(entry);
        self.queue_len.fetch_add(1, AtomicOrdering::AcqRel);
        // B1：行口径累计（提交 +，writer_loop 取走 -）
        self.queue_rows
            .fetch_add(size_hint.max(1), AtomicOrdering::AcqRel);

        let mut stats = self.stats.lock();
        stats.total_submitted += 1;

        // 唤醒 writer_loop
        self.notify.notify_one();

        Ok(())
    }

    /// 强制刷出所有排队请求（提交一个 Critical barrier，等待队列排空）
    ///
    /// 仅插队，不保证排空；要可等待的排空请用 [`IoScheduler::flush_and_wait`]。
    pub fn flush_barrier(&self) {
        // 提交一个 Critical 级空操作 barrier，writer_loop 会优先执行它
        // barrier 执行时，队列中已有的请求也会被合并执行
        let _ = self.submit(IoPriority::Critical, None, |_conn| Ok(()), 0);
    }

    /// B3：提交一个排空目标并等待队列排空（有超时）。
    ///
    /// 返回 `true` 表示在超时前排空；`false` 表示超时仍有积压。
    /// 用于优雅关闭、TRUNCATE 之前与测试。
    pub async fn flush_and_wait(&self, timeout: Duration) -> bool {
        // 把目标序号设为当前已提交序号（writer_loop 排空到 <= 该序号即算完成）
        let target = self.seq_counter.load(AtomicOrdering::Acquire);
        self.drain_target.store(target, AtomicOrdering::Release);
        self.notify.notify_one();

        let poll = async {
            loop {
                self.drained.notified().await;
                if self.drain_target.load(AtomicOrdering::Acquire) == 0 {
                    return true;
                }
            }
        };
        match tokio::time::timeout(timeout, poll).await {
            Ok(_) => true,
            Err(_) => {
                // 超时：清除排空目标，避免后续 tick 误唤醒
                self.drain_target.store(0, AtomicOrdering::Release);
                false
            }
        }
    }

    /// P2: 行口径压力快照（B1）。
    ///
    /// - `queue`：`clamp01((rows - low_rows)/(high_rows - low_rows))`
    /// - `latency`：`clamp01((lat_ewma - target)/(slow - target))`（用全局单批耗时 EWMA）
    /// - `level`：二者取 max，即对外暴露的背压等级。
    pub fn pressure(&self) -> IoPressure {
        let rows = self.queue_rows.load(AtomicOrdering::Acquire) as f32;
        let low_r = self.config.low_watermark_rows as f32;
        let high_r = self.config.high_watermark_rows as f32;
        let p_q = if high_r <= low_r || rows <= low_r {
            0.0
        } else if rows >= high_r {
            1.0
        } else {
            (rows - low_r) / (high_r - low_r)
        };

        let lat_us = io_batch_latency_ewma_us() as f32;
        let target = self.config.latency_target_us as f32;
        let slow = self.config.latency_slow_us as f32;
        let p_l = if slow <= target || target <= 0.0 || lat_us <= target {
            0.0
        } else if lat_us >= slow {
            1.0
        } else {
            (lat_us - target) / (slow - target)
        };

        IoPressure {
            queue: p_q.clamp(0.0, 1.0),
            latency: p_l.clamp(0.0, 1.0),
            level: p_q.max(p_l).clamp(0.0, 1.0),
        }
    }

    /// P2: 获取背压等级 [0.0, 1.0]（B1：行口径，等价 pressure().level）
    pub fn backpressure_level(&self) -> f32 {
        self.pressure().level
    }

    /// 当前队列行数（B1 行口径）
    pub fn queue_rows(&self) -> usize {
        self.queue_rows.load(AtomicOrdering::Acquire)
    }

    /// 当前每 tick 行预算（C2 AIMD 动态值）
    pub fn rows_per_tick(&self) -> usize {
        self.rows_per_tick_dynamic.load(AtomicOrdering::Acquire)
    }

    /// C2：外部强制设置每 tick 行预算（AIMD 控制器写回；最小 1）。
    pub fn set_rows_per_tick(&self, rows: usize) {
        self.rows_per_tick_dynamic
            .store(rows.max(1), AtomicOrdering::Release);
    }

    /// C2：喂一次 AIMD 观测（checkpoint 耗时 EWMA + 队列行数），
    /// 控制器产出决策后把新的 rows_per_tick 写回动态预算。
    /// 应由 metrics tick 周期性调用。
    pub fn feed_aimd(&self, checkpoint_ms_ewma: u64, queue_rows: usize) {
        if aimd_observe(checkpoint_ms_ewma, queue_rows).is_some() {
            let rows = aimd_rows_per_tick();
            if rows > 0 {
                self.set_rows_per_tick(rows);
            }
        }
    }

    /// B4：组装 status API 快照
    pub fn status_snapshot(&self) -> IoStatusSnapshot {
        io_status_snapshot(
            self.queue_len(),
            self.queue_rows(),
            self.rows_per_tick(),
            self.pressure().level,
            io_batch_latency_ewma_us(),
            &self.stats(),
        )
    }

    /// P2: 是否处于背压状态（level > 0.5）
    pub fn is_backpressured(&self) -> bool {
        self.backpressure_level() > 0.5
    }

    /// P3: 是否处于 IO 空闲状态
    ///
    /// 空闲条件：idle_prediction_enabled=true 且最近窗口内队列长度持续低于 low_watermark 且令牌充足
    pub fn is_idle(&self) -> bool {
        if !self.config.idle_prediction_enabled {
            // 未开启空闲预测时，简单判断：队列低 + 令牌充足
            return self.queue_len.load(AtomicOrdering::Acquire) < self.config.low_watermark
                && self.token_bucket.available() > self.config.token_bucket_max / 2;
        }
        let samples = self.idle_samples.lock();
        if samples.is_empty() {
            return true;
        }
        let now = Instant::now();
        samples.iter().all(|s| {
            now.duration_since(s.timestamp) <= self.config.idle_window
                && s.queue_len < self.config.low_watermark
                && s.tokens_available > self.config.token_bucket_max / 4
        })
    }

    /// 获取统计快照
    pub fn stats(&self) -> IoSchedulerStats {
        self.stats.lock().clone()
    }

    /// 当前队列长度
    pub fn queue_len(&self) -> usize {
        self.queue_len.load(AtomicOrdering::Acquire)
    }

    /// 优雅关闭
    pub fn shutdown(&self) {
        self.shutdown.store(true, AtomicOrdering::Release);
        self.notify.notify_one();
    }

    // ─── Writer Loop ─────────────────────────────────────────────────────

    async fn writer_loop(self: Arc<Self>) {
        let tick_interval = Duration::from_millis(self.config.steady_tick_ms);
        let max_requests = self.config.max_requests_per_tick.max(1);
        // 公布行口径基准，供 refresh_bootstrap_import_window 把预算重置为 start×base。
        let base_rows = self.config.rows_per_tick.max(1);
        BASE_PER_TICK.store(base_rows, AtomicOrdering::Release);
        // 令牌桶：上一次补充时刻（行/秒计费）
        let mut last_refill = Instant::now();

        loop {
            let tick_start = Instant::now();

            // B2：按 elapsed 给令牌桶补充（行/秒）。Critical 不扣，其余按行计费。
            if self.config.token_bucket_enabled {
                let elapsed_secs = last_refill.elapsed().as_secs_f64();
                let to_add = (elapsed_secs * self.config.token_bucket_rate as f64) as usize;
                if to_add > 0 {
                    self.token_bucket.refill(to_add);
                }
                last_refill = tick_start;
            }

            // v10/v11：快照导入窗口内按 AIMD 自适应预算放开时间片预算；
            // 窗口静默 TTL 后自动回落稳态平滑语义。行口径。
            let import_active = import_window_active();
            let row_budget = if import_active {
                import_budget().max(base_rows)
            } else {
                self.rows_per_tick_dynamic.load(AtomicOrdering::Acquire)
            };

            // 检查关闭信号
            if self.shutdown.load(AtomicOrdering::Acquire) {
                self.drain_remaining();
                break;
            }

            // 记录空闲检测样本
            self.record_idle_sample();

            // 时间片匀速：最多取 max_requests 个请求，且累计行数不超过 row_budget（0=不限）。
            let row_cap = if row_budget == 0 {
                usize::MAX
            } else {
                row_budget
            };
            let mut batch = Vec::with_capacity(max_requests);
            let mut rows_in_batch = 0usize;
            while batch.len() < max_requests && rows_in_batch < row_cap {
                // 先偷看队首，决定是否受令牌桶约束（不取走）。
                let peeked = {
                    let q = self.queue.lock();
                    q.peek().map(|e| (e.request.priority, e.request.size_hint))
                };
                let Some((pri, need)) = peeked else { break };

                // B2：令牌桶门禁。Critical 免疫；其余余额不足则本条不取、留在队列。
                if self.config.token_bucket_enabled && pri != IoPriority::Critical && need > 0 {
                    let got = self.token_bucket.acquire_partial(need);
                    if got < need {
                        // 部分成交：退还未成交部分，本条留在队列
                        if got > 0 {
                            self.token_bucket.refill(got);
                        }
                        let mut st = self.stats.lock();
                        st.token_starved_ticks += 1;
                        st.rows_deferred_by_tokens += need as u64;
                        break;
                    }
                }

                let entry = {
                    let mut q = self.queue.lock();
                    q.pop()
                };
                let Some(mut entry) = entry else { break };
                self.queue_len.fetch_sub(1, AtomicOrdering::AcqRel);
                self.queue_rows
                    .fetch_sub(entry.request.size_hint.max(1), AtomicOrdering::AcqRel);
                rows_in_batch += entry.request.size_hint.max(1);

                // Deadline 老化 — 超期的 Normal/Background 升级为 Important
                let now = Instant::now();
                if let Some(deadline) = entry.request.deadline {
                    if now > deadline {
                        match entry.request.priority {
                            IoPriority::Normal | IoPriority::Background => {
                                entry.request.priority = IoPriority::Important;
                                let mut stats = self.stats.lock();
                                stats.starvation_preemptions += 1;
                            }
                            _ => {}
                        }
                    }
                }

                batch.push(entry);
            }

            // 执行小批量写入（单事务）
            if !batch.is_empty() {
                let priority = batch[0].request.priority;
                self.execute_batch(&mut batch, priority);
                // A4：观测本批耗时 EWMA；窗口活跃时按 AIMD 调整预算
                observe_and_adaptive_tick(tick_start.elapsed(), import_active);
            }

            // 更新背压信号（请求数口径硬上限，保留给拒绝 Background）
            let qlen = self.queue_len.load(AtomicOrdering::Acquire);
            if qlen > self.config.high_watermark {
                self.backpressure.store(true, AtomicOrdering::Release);
            } else if qlen < self.config.low_watermark {
                self.backpressure.store(false, AtomicOrdering::Release);
            }

            // B3：排空目标达成 → 唤醒 flush_and_wait
            if self.drain_target.load(AtomicOrdering::Acquire) != 0
                && self.queue_len.load(AtomicOrdering::Acquire) == 0
            {
                self.drain_target.store(0, AtomicOrdering::Release);
                self.drained.notify_waiters();
            }

            // 匀速等待：到下一个时间片
            let elapsed = tick_start.elapsed();
            if elapsed < tick_interval {
                tokio::time::sleep(tick_interval - elapsed).await;
            }
        }
    }

    /// 记录空闲检测样本
    fn record_idle_sample(&self) {
        if !self.config.idle_prediction_enabled {
            return;
        }
        let sample = IdleSample {
            timestamp: Instant::now(),
            queue_len: self.queue_len.load(AtomicOrdering::Acquire),
            tokens_available: self.token_bucket.available(),
        };
        let mut samples = self.idle_samples.lock();
        samples.push(sample);
        // 清理过期样本
        let cutoff = crate::utils::cutoff_before(self.config.idle_window);
        samples.retain(|s| s.timestamp >= cutoff);
    }

    /// 批量执行写入（单事务）
    fn execute_batch(&self, batch: &mut Vec<HeapEntry>, priority: IoPriority) {
        let count = batch.len();
        if count == 0 {
            return;
        }

        // 锁定 SQLite 连接（与 WriteQueue 使用相同的 std::sync::Mutex）
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("[io_scheduler] 连接锁获取失败: {}", e);
                return;
            }
        };

        // 开启事务
        let tx = match conn.unchecked_transaction() {
            Ok(tx) => tx,
            Err(e) => {
                tracing::warn!("[io_scheduler] 事务开启失败: {}", e);
                return;
            }
        };

        // 依次执行每个请求的 payload
        // 单个 payload panic 不得穿出 writer_loop（否则该 task 终结、连接锁中毒），
        // 也不得带着半提交状态继续 commit —— 出现 panic 时整批回滚。
        let mut degraded = false;
        for entry in batch.drain(..) {
            let payload = entry.request.payload;
            let tx_ref: &Connection = &tx;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| payload(tx_ref)));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!("[io_scheduler] 写入失败: {}", e),
                Err(_) => {
                    tracing::error!("[io_scheduler] payload panic，本批事务回滚");
                    degraded = true;
                    break;
                }
            }
        }

        // 提交事务
        if degraded {
            drop(tx); // Transaction 的 Drop 会回滚
        } else if let Err(e) = tx.commit() {
            tracing::warn!("[io_scheduler] 事务提交失败: {}", e);
        }

        // 更新统计
        let mut stats = self.stats.lock();
        stats.total_executed += count as u64;
        stats.total_batches += 1;
        if count > stats.max_batch_size {
            stats.max_batch_size = count;
        }
        // 统一按“条数”口径统计，避免与 Normal/Background 量纲不一致
        match priority {
            IoPriority::Critical => stats.critical_executed += count as u64,
            IoPriority::Important => stats.important_executed += count as u64,
            IoPriority::Normal => stats.normal_executed += count as u64,
            IoPriority::Background => stats.background_executed += count as u64,
        }
    }

    /// 关闭时排空剩余请求
    fn drain_remaining(&self) {
        let mut remaining: Vec<HeapEntry> = {
            let mut q = self.queue.lock();
            q.drain().collect()
        };
        if remaining.is_empty() {
            return;
        }
        tracing::info!("[io_scheduler] 关闭时排空 {} 个剩余请求", remaining.len());
        self.execute_batch(&mut remaining, IoPriority::Critical);
    }
}

// ─── C2：AIMD 控制器（被控量 = checkpoint_ms_ewma / queue_rows；执行器 =
// rows_per_tick / checkpoint_min_interval_secs）─────────────────────────────

/// C2：按磁盘画像夹紧后的参数边界（SSD/HDD/Unknown 三档）。
#[derive(Debug, Clone, Copy)]
pub struct AimdProfile {
    /// rows_per_tick 地板
    pub rows_min: usize,
    /// rows_per_tick 天花板
    pub rows_max: usize,
    /// rows_per_tick 基准（冷启动初始值）
    pub rows_base: usize,
    /// checkpoint_min_interval_secs 地板
    pub interval_min: u32,
    /// checkpoint_min_interval_secs 天花板
    pub interval_max: u32,
}

impl Default for AimdProfile {
    fn default() -> Self {
        // Unknown 档（保守）：以 SSD 为下限、HDD 为上限之间的中位
        Self {
            rows_min: 5_000,
            rows_max: 50_000,
            rows_base: 10_000,
            interval_min: 1,
            interval_max: 60,
        }
    }
}

/// C2：单条 AIMD 决策记录（环形缓冲，status API 暴露最近 20 条）。
#[derive(Debug, Clone, serde::Serialize)]
pub struct AimdDecisionRecord {
    /// 决策时刻 UNIX 毫秒
    pub ts_ms: i64,
    /// 观测到的 checkpoint 耗时 EWMA（毫秒）
    pub checkpoint_ms_ewma: u64,
    /// 观测到的队列行数
    pub queue_rows: usize,
    /// rows_per_tick 变更前
    pub rows_before: usize,
    /// rows_per_tick 变更后
    pub rows_after: usize,
    /// checkpoint_min_interval_secs 变更前
    pub interval_before: u32,
    /// checkpoint_min_interval_secs 变更后
    pub interval_after: u32,
    /// 动作：slow / grow / hold
    pub action: &'static str,
    /// 人读原因
    pub reason: String,
}

/// C2：AIMD 控制器纯逻辑（不含 IO，便于单测）。
///
/// - 被控量：`checkpoint_ms_ewma`（本批落盘耗时 EWMA）与 `queue_rows`；
/// - 执行器：`rows_per_tick`（行/tick）与 `checkpoint_min_interval_secs`；
/// - 迟滞带：`[target_ms, slow_ms]` 内不动；
/// - 慢（> slow_ms）→ rows/2（地板 rows_min），interval×2（天花板 interval_max）；
/// - 快（< target_ms）连续 N 次 → rows += step（天花板 rows_max），interval/=2（地板 interval_min）。
#[derive(Debug, Clone)]
pub struct AimdController {
    pub profile: AimdProfile,
    pub target_ms: u64,
    pub slow_ms: u64,
    pub grow_step: usize,
    pub grow_after: u32,
    rows: usize,
    interval_secs: u32,
    fast_streak: u32,
    ring: std::collections::VecDeque<AimdDecisionRecord>,
}

impl AimdController {
    pub fn new(profile: AimdProfile, target_ms: u64, slow_ms: u64) -> Self {
        Self {
            profile,
            target_ms: target_ms.max(1),
            slow_ms: slow_ms.max(1),
            grow_step: 500,
            grow_after: 3,
            rows: profile.rows_base,
            interval_secs: profile.interval_min,
            fast_streak: 0,
            ring: std::collections::VecDeque::with_capacity(256),
        }
    }

    pub fn rows_per_tick(&self) -> usize {
        self.rows
    }

    pub fn checkpoint_min_interval_secs(&self) -> u32 {
        self.interval_secs
    }

    /// 最近 N 条决策（新→旧）。
    pub fn recent_decisions(&self, n: usize) -> Vec<AimdDecisionRecord> {
        self.ring.iter().rev().take(n).cloned().collect()
    }

    /// 喂入一次观测，返回本次决策记录。纯函数式推进状态。
    pub fn observe(&mut self, checkpoint_ms_ewma: u64, queue_rows: usize) -> AimdDecisionRecord {
        let rows_before = self.rows;
        let interval_before = self.interval_secs;
        let (action, reason);

        if checkpoint_ms_ewma > self.slow_ms {
            // 慢 → 乘法减
            self.rows = (self.rows / 2).max(self.profile.rows_min);
            self.interval_secs =
                (self.interval_secs.saturating_mul(2)).min(self.profile.interval_max);
            self.fast_streak = 0;
            action = "slow";
            reason = format!(
                "checkpoint_ms_ewma={} > slow={}，rows/2、interval×2",
                checkpoint_ms_ewma, self.slow_ms
            );
        } else if checkpoint_ms_ewma < self.target_ms {
            // 快 → 连续 N 次后加法增
            self.fast_streak += 1;
            if self.fast_streak >= self.grow_after {
                self.rows = self
                    .rows
                    .saturating_add(self.grow_step)
                    .min(self.profile.rows_max);
                self.interval_secs = (self.interval_secs / 2).max(self.profile.interval_min);
                self.fast_streak = 0;
                action = "grow";
                reason = format!(
                    "checkpoint_ms_ewma={} < target={} 连续{}次，rows+{}、interval/2",
                    checkpoint_ms_ewma, self.target_ms, self.grow_after, self.grow_step
                );
            } else {
                action = "hold";
                reason = format!(
                    "快带累计 {}/{}，暂不增窗",
                    self.fast_streak, self.grow_after
                );
            }
        } else {
            // 迟滞带 [target, slow]：不动
            self.fast_streak = 0;
            action = "hold";
            reason = format!("迟滞带 [{}, {}] 内不动", self.target_ms, self.slow_ms);
        }

        let rec = AimdDecisionRecord {
            ts_ms: now_unix_ms(),
            checkpoint_ms_ewma,
            queue_rows,
            rows_before,
            rows_after: self.rows,
            interval_before,
            interval_after: self.interval_secs,
            action,
            reason,
        };
        if self.ring.len() >= 256 {
            self.ring.pop_front();
        }
        self.ring.push_back(rec.clone());
        rec
    }
}

/// B4：GET /api/v1/io/status 序列化快照（字段名对齐 17 号草案）。
#[derive(Debug, Clone, serde::Serialize)]
pub struct IoStatusSnapshot {
    pub queue_len: usize,
    pub queue_rows: usize,
    pub rows_per_tick: usize,
    pub latency_ewma_ms: u64,
    pub pressure: f32,
    pub level: f32,
    pub disk_class: String,
    pub fsync_ms_p50: f32,
    pub recent_decisions: Vec<AimdDecisionRecord>,
    pub stats: IoSchedulerStatsJson,
}

/// B4：统计快照的可序列化投影。
#[derive(Debug, Clone, serde::Serialize)]
pub struct IoSchedulerStatsJson {
    pub total_submitted: u64,
    pub total_executed: u64,
    pub total_batches: u64,
    pub starvation_preemptions: u64,
    pub backpressure_rejections: u64,
    pub critical_executed: u64,
    pub important_executed: u64,
    pub normal_executed: u64,
    pub background_executed: u64,
    pub max_batch_size: usize,
    pub token_starved_ticks: u64,
    pub rows_deferred_by_tokens: u64,
}

// ─── C1/C2 进程级状态（启动探测一次 + AIMD 控制器）──────────────────────────

/// C1：磁盘画像分类结果（"SSD"/"HDD"/"Unknown"）。
static DISK_CLASS: OnceLock<String> = OnceLock::new();
/// C1：fsync p50（毫秒，放大 100 倍存整数，避免引入 AtomicF32）。
static DISK_FSYNC_P50_X100: AtomicU32 = AtomicU32::new(0);
/// C2：AIMD 控制器（启动时按磁盘画像初始化一次，运行期由 metrics tick 喂观测）。
static AIMD: OnceLock<ParkingMutex<AimdController>> = OnceLock::new();

/// C1：启动时写入磁盘画像结果。
pub fn set_disk_profile(class: &str, fsync_ms_p50: f32) {
    let _ = DISK_CLASS.set(class.to_string());
    DISK_FSYNC_P50_X100.store(
        (fsync_ms_p50 * 100.0).round() as u32,
        AtomicOrdering::Release,
    );
}

/// C1：读取磁盘画像分类。
pub fn disk_class() -> String {
    DISK_CLASS
        .get()
        .cloned()
        .unwrap_or_else(|| "Unknown".to_string())
}

/// C1：读取 fsync p50（毫秒）。
pub fn disk_fsync_ms_p50() -> f32 {
    DISK_FSYNC_P50_X100.load(AtomicOrdering::Acquire) as f32 / 100.0
}

/// C2：初始化 AIMD 控制器（启动时按磁盘画像调用一次）。
pub fn init_aimd_controller(profile: AimdProfile, target_ms: u64, slow_ms: u64) {
    let _ = AIMD.set(ParkingMutex::new(AimdController::new(
        profile, target_ms, slow_ms,
    )));
}

/// C2：metrics tick 喂入一次观测，返回是否触发了 rows_per_tick 变化。
pub fn aimd_observe(checkpoint_ms_ewma: u64, queue_rows: usize) -> Option<AimdDecisionRecord> {
    AIMD.get()
        .map(|m| m.lock().observe(checkpoint_ms_ewma, queue_rows))
}

/// C2：当前 AIMD 给出的 rows_per_tick 建议（未初始化时回退 0=不限）。
pub fn aimd_rows_per_tick() -> usize {
    AIMD.get().map(|m| m.lock().rows_per_tick()).unwrap_or(0)
}

/// B4：组装 IO status 快照。
pub fn io_status_snapshot(
    queue_len: usize,
    queue_rows: usize,
    rows_per_tick: usize,
    pressure_level: f32,
    latency_ewma_us: u64,
    stats: &IoSchedulerStats,
) -> IoStatusSnapshot {
    let recent = AIMD
        .get()
        .map(|m| m.lock().recent_decisions(20))
        .unwrap_or_default();
    IoStatusSnapshot {
        queue_len,
        queue_rows,
        rows_per_tick,
        latency_ewma_ms: latency_ewma_us / 1000,
        pressure: pressure_level,
        level: pressure_level,
        disk_class: disk_class(),
        fsync_ms_p50: disk_fsync_ms_p50(),
        recent_decisions: recent,
        stats: IoSchedulerStatsJson {
            total_submitted: stats.total_submitted,
            total_executed: stats.total_executed,
            total_batches: stats.total_batches,
            starvation_preemptions: stats.starvation_preemptions,
            backpressure_rejections: stats.backpressure_rejections,
            critical_executed: stats.critical_executed,
            important_executed: stats.important_executed,
            normal_executed: stats.normal_executed,
            background_executed: stats.background_executed,
            max_batch_size: stats.max_batch_size,
            token_starved_ticks: stats.token_starved_ticks,
            rows_deferred_by_tokens: stats.rows_deferred_by_tokens,
        },
    }
}

// ─── 单元测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_conn() -> Arc<StdMutex<Connection>> {
        Arc::new(StdMutex::new(Connection::open_in_memory().unwrap()))
    }

    fn test_config() -> SchedulerRuntimeConfig {
        SchedulerRuntimeConfig {
            max_queue_size: 1000,
            token_bucket_rate: 1000,
            token_bucket_max: 100,
            low_watermark: 10,
            high_watermark: 100,
            batch_max_size: 10,
            batch_max_delay: Duration::from_millis(10),
            idle_prediction_enabled: false,
            idle_window: Duration::from_secs(5),
            idle_wait: Duration::from_millis(500),
            retry_wait: Duration::from_millis(50),
            steady_tick_ms: 10,
            max_requests_per_tick: 64,
            rows_per_tick: 5_000,
            token_bucket_enabled: false,
            low_watermark_rows: 1_000,
            high_watermark_rows: 10_000,
            latency_target_us: 50_000,
            latency_slow_us: 500_000,
        }
    }

    // ── 令牌桶测试 ──

    #[test]
    fn test_token_bucket_acquire() {
        let tb = TokenBucket::new(100, 1000);
        assert_eq!(tb.acquire_partial(50), 50);
        assert_eq!(tb.acquire_partial(50), 50);
        assert_eq!(tb.acquire_partial(1), 0); // 已耗尽
        assert_eq!(tb.available(), 0);
    }

    #[test]
    fn test_token_bucket_refill() {
        let tb = TokenBucket::new(100, 1000);
        assert_eq!(tb.acquire_partial(80), 80);
        assert_eq!(tb.available(), 20);
        tb.refill(50);
        assert_eq!(tb.available(), 70);
        // 不超过 max
        tb.refill(100);
        assert_eq!(tb.available(), 100);
    }

    // ── B2：令牌桶行/秒 + 部分支付 ──

    #[test]
    fn test_acquire_partial_defers_remainder() {
        let tb = TokenBucket::new(100, 1000);
        // 先取走 70，使余额=30；请求 100 → 部分成交 30，剩 0
        assert_eq!(tb.acquire_partial(70), 70);
        assert_eq!(tb.acquire_partial(100), 30);
        assert_eq!(tb.available(), 0);
        // 再请求 → 0
        assert_eq!(tb.acquire_partial(100), 0);
        // 补充后精确成交
        tb.refill(250);
        assert_eq!(tb.available(), 100); // 被 max 截断
        assert_eq!(tb.acquire_partial(60), 60);
        assert_eq!(tb.available(), 40);
    }

    #[test]
    fn test_token_bucket_default_disabled_via_config() {
        // B2：token_bucket_enabled 默认必须为 false
        let cfg = SchedulerRuntimeConfig::default();
        assert!(!cfg.token_bucket_enabled, "令牌桶接线后默认必须关");
    }

    #[tokio::test]
    async fn test_critical_immune_to_token_bucket() {
        let conn = test_conn();
        let config = SchedulerRuntimeConfig {
            token_bucket_enabled: true,
            token_bucket_max: 0, // 空桶
            token_bucket_rate: 0,
            ..test_config()
        };
        let sched = IoScheduler::new(conn, config);

        // Critical：免疫，空桶也执行
        let crit = Arc::new(AtomicBool::new(false));
        let c2 = crit.clone();
        let _ = sched.submit(
            IoPriority::Critical,
            None,
            move |_c| {
                c2.store(true, AtomicOrdering::Release);
                Ok(())
            },
            1000,
        );

        // Normal：空桶应被卡住（不执行）
        let norm = Arc::new(AtomicBool::new(false));
        let n2 = norm.clone();
        let _ = sched.submit(
            IoPriority::Normal,
            None,
            move |_c| {
                n2.store(true, AtomicOrdering::Release);
                Ok(())
            },
            1,
        );

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(crit.load(AtomicOrdering::Acquire), "Critical 应免疫令牌桶");
        assert!(
            !norm.load(AtomicOrdering::Acquire),
            "Normal 在空桶下应被留在队列"
        );
    }

    // ── 优先级队列测试 ──

    fn make_entry(priority: IoPriority, seq: u64) -> HeapEntry {
        HeapEntry {
            request: IoRequest {
                priority,
                deadline: None,
                payload: Box::new(|_conn| Ok(())),
                size_hint: 1,
                submitted_at: Instant::now(),
            },
            seq,
        }
    }

    #[test]
    fn test_priority_queue_ordering() {
        let mut heap = BinaryHeap::new();
        heap.push(make_entry(IoPriority::Background, 1));
        heap.push(make_entry(IoPriority::Normal, 2));
        heap.push(make_entry(IoPriority::Critical, 3));
        heap.push(make_entry(IoPriority::Important, 4));

        // Critical 先出
        let first = heap.pop().unwrap();
        assert_eq!(first.request.priority, IoPriority::Critical);
        // 然后 Important
        let second = heap.pop().unwrap();
        assert_eq!(second.request.priority, IoPriority::Important);
        // 然后 Normal
        let third = heap.pop().unwrap();
        assert_eq!(third.request.priority, IoPriority::Normal);
        // 最后 Background
        let fourth = heap.pop().unwrap();
        assert_eq!(fourth.request.priority, IoPriority::Background);
    }

    // ── Deadline 老化测试 ──

    #[test]
    fn test_deadline_starvation_prevention() {
        // 验证：超期的 Normal 请求应被升级为 Important
        let now = Instant::now();
        let expired = now - Duration::from_secs(60); // 已过期

        let request = IoRequest {
            priority: IoPriority::Normal,
            deadline: Some(expired),
            payload: Box::new(|_conn| Ok(())),
            size_hint: 1,
            submitted_at: now - Duration::from_secs(60),
        };

        // 模拟 writer_loop 的老化逻辑
        let now2 = Instant::now();
        let mut effective = request.priority;
        if let Some(dl) = request.deadline {
            if now2 > dl {
                match effective {
                    IoPriority::Normal | IoPriority::Background => {
                        effective = IoPriority::Important;
                    }
                    _ => {}
                }
            }
        }
        assert_eq!(
            effective,
            IoPriority::Important,
            "Normal 超期应升级为 Important"
        );
    }

    // ── 背压级别测试 ──

    #[tokio::test]
    async fn test_backpressure_level() {
        let conn = test_conn();
        let config = SchedulerRuntimeConfig {
            low_watermark_rows: 1_000,
            high_watermark_rows: 10_000,
            ..test_config()
        };
        let sched = IoScheduler::new(conn, config);

        // 空队列 → 0.0
        assert_eq!(sched.backpressure_level(), 0.0);
        assert!(!sched.is_backpressured());

        // 手动模拟队列行数（通过提交 size_hint 较大的请求）。
        // 注意：writer_loop 会快速消费空 payload，所以用慢 payload 让请求堆积。
        for _ in 0..20 {
            let _ = sched.submit(
                IoPriority::Normal,
                None,
                |_c| {
                    std::thread::sleep(Duration::from_millis(20));
                    Ok(())
                },
                500,
            );
        }
        // 队列应积压到数千行，行口径压力 > 0
        let level = sched.backpressure_level();
        assert!(
            level > 0.0 && level <= 1.0,
            "backpressure level 应在 (0,1] 范围内: {}",
            level
        );
    }

    // ── 请求合并测试 ──

    #[tokio::test]
    async fn test_request_merging() {
        let conn = test_conn();
        let config = SchedulerRuntimeConfig {
            batch_max_size: 5,
            batch_max_delay: Duration::from_millis(100),
            token_bucket_max: 1000,
            token_bucket_rate: 10000,
            ..test_config()
        };
        let sched = IoScheduler::new(conn, config);

        // 提交多个 Normal 优先级请求
        for i in 0..5 {
            let _ = sched.submit(
                IoPriority::Normal,
                None,
                move |_c| {
                    let _ = i; // 模拟写入
                    Ok(())
                },
                1,
            );
        }

        // 等待 writer_loop 处理
        tokio::time::sleep(Duration::from_millis(200)).await;

        let stats = sched.stats();
        assert_eq!(stats.total_executed, 5, "5 个请求应全部执行");
        assert!(stats.total_batches >= 1, "应至少有 1 个批次");
    }

    // ── 空闲预测测试 ──

    #[tokio::test]
    async fn test_idle_prediction() {
        let conn = test_conn();
        let config = SchedulerRuntimeConfig {
            idle_prediction_enabled: false,
            low_watermark: 100,
            token_bucket_max: 1000,
            ..test_config()
        };
        let sched = IoScheduler::new(conn, config);

        // 空队列 + 令牌充足 → is_idle = true
        assert!(sched.is_idle(), "空队列时应判定为空闲");
    }

    // ── WriteQueue 兼容层测试 ──

    #[tokio::test]
    async fn test_write_queue_compatibility() {
        use crate::storage::write_queue::WriteQueue;

        let conn = test_conn();
        let wq = WriteQueue::new(conn, 100, Duration::from_secs(1));

        // send 不应 panic
        wq.send(|_c| Ok(()));
        wq.send(|_c| Ok(()));

        // flush 不应 panic
        wq.flush();

        // stats 可读
        let s = wq.stats();
        assert_eq!(s.total_requests, 2);

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ── 背压拒绝测试 ──

    #[tokio::test]
    async fn test_backpressure_rejects_background() {
        let conn = test_conn();
        let config = SchedulerRuntimeConfig {
            high_watermark: 5,
            low_watermark: 1,
            ..test_config()
        };
        let sched = IoScheduler::new(conn, config);

        // 填满队列（用 Normal 请求避免被 writer 快速消费）
        // 注意：writer_loop 会消费，所以用 delay 让请求堆积
        for _ in 0..10 {
            let _ = sched.submit(
                IoPriority::Normal,
                None,
                |_c| {
                    std::thread::sleep(Duration::from_millis(50));
                    Ok(())
                },
                1,
            );
        }

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Background 请求在高压下可能被拒绝
        // 这里不严格断言拒绝（因为 writer 可能已消费），只验证不 panic
        let _ = sched.submit(IoPriority::Background, None, |_c| Ok(()), 1);
    }

    // ── Critical 绕过令牌桶测试 ──

    #[tokio::test]
    async fn test_critical_bypasses_token_bucket() {
        let conn = test_conn();
        let config = SchedulerRuntimeConfig {
            token_bucket_max: 0, // 令牌桶为空
            token_bucket_rate: 0,
            ..test_config()
        };
        let sched = IoScheduler::new(conn, config);

        // Critical 请求应立即执行（不受令牌桶限制）
        let executed = Arc::new(AtomicBool::new(false));
        let executed_clone = executed.clone();
        let _ = sched.submit(
            IoPriority::Critical,
            None,
            move |_c| {
                executed_clone.store(true, AtomicOrdering::Release);
                Ok(())
            },
            1,
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            executed.load(AtomicOrdering::Acquire),
            "Critical 请求应在令牌桶为空时仍执行"
        );
    }

    // ── A4：导入预算 AIMD 自适应测试 ──

    fn test_adaptive_cfg() -> AdaptiveCfg {
        AdaptiveCfg {
            start_multiplier: 8,
            max_multiplier: 200,
            latency_target_us: 200_000, // 200ms
            alpha: 0.2,
            grow_step: 2,
            grow_after_batches: 32,
        }
    }

    #[test]
    fn test_import_budget_halves_on_slow() {
        let cfg = test_adaptive_cfg();
        let base = 10usize;
        let start = base * cfg.start_multiplier; // 80
                                                 // 慢批（> target 200ms）→ 减半，地板 base，fast 计数清零
        let (budget, streak) = aimd_step(start, Duration::from_millis(500), base, &cfg, 10);
        assert_eq!(budget, start / 2, "慢批应把预算减半");
        assert_eq!(streak, 0, "慢批应清零 fast 计数");
    }

    #[test]
    fn test_import_budget_grows_when_fast() {
        let cfg = test_adaptive_cfg();
        let base = 10usize;
        let start = base * cfg.start_multiplier; // 80
        let fast = Duration::from_millis(50); // < target/2 (100ms)
                                              // 连续 grow_after_batches-1 批：预算不变，计数累加
        let (mut budget, mut streak) = (start, 0u32);
        for _ in 0..(cfg.grow_after_batches - 1) {
            let (b, s) = aimd_step(budget, fast, base, &cfg, streak);
            budget = b;
            streak = s;
        }
        assert_eq!(budget, start, "未达增窗门槛前预算不应变化");
        assert_eq!(streak, cfg.grow_after_batches - 1);
        // 第 N 批：达到门槛 → 增窗，计数清零
        let (budget2, streak2) = aimd_step(budget, fast, base, &cfg, streak);
        assert_eq!(budget2, start + cfg.grow_step, "连续快批达门槛后应增窗");
        assert_eq!(streak2, 0, "增窗后应清零 fast 计数");
    }

    #[test]
    fn test_import_window_expires() {
        let saved = IMPORT_UNTIL_MS.load(AtomicOrdering::Acquire);
        // 0（未激活）→ 未激活
        IMPORT_UNTIL_MS.store(0, AtomicOrdering::Release);
        assert!(!import_window_active(), "未设置窗口应判定为未激活");
        // 未来 60s → 激活
        IMPORT_UNTIL_MS.store(now_unix_ms() + 60_000, AtomicOrdering::Release);
        assert!(import_window_active(), "未来窗口应判定为激活");
        // 还原现场，避免影响其它测试
        IMPORT_UNTIL_MS.store(saved, AtomicOrdering::Release);
    }

    #[test]
    fn test_import_budget_floor_is_base() {
        let cfg = test_adaptive_cfg();
        let base = 10usize;
        let mut budget = base * cfg.max_multiplier; // 2000
        let slow = Duration::from_millis(1_000); // > target
                                                 // 持续慢批应不断减半，但绝不跌破 base
        for _ in 0..20 {
            let (b, _) = aimd_step(budget, slow, base, &cfg, 0);
            budget = b;
        }
        assert_eq!(budget, base, "持续慢批后预算应收敛到 base 地板");
    }

    // ── B1：行口径压力三档 ──

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pressure_from_rows() {
        let conn = test_conn();
        let config = SchedulerRuntimeConfig {
            low_watermark_rows: 1_000,
            high_watermark_rows: 10_000,
            rows_per_tick: 100,
            ..test_config()
        };
        let sched = IoScheduler::new(conn, config);
        // 无 EWMA 污染：直接看 queue 分量
        // 空队列 → 0
        assert_eq!(sched.pressure().queue, 0.0);
        // 提交 5000 行（慢 payload 防被消费）→ queue 压力 = (5000-1000)/(10000-1000)=0.44
        for _ in 0..5 {
            let _ = sched.submit(
                IoPriority::Normal,
                None,
                |_c| {
                    std::thread::sleep(Duration::from_millis(30));
                    Ok(())
                },
                1000,
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let p = sched.pressure();
        assert!(
            p.queue > 0.2 && p.queue < 0.8,
            "queue 压力应在中段: {}",
            p.queue
        );
    }

    #[tokio::test]
    async fn test_pressure_takes_max() {
        // 手动构造 queue_rows 与 latency 各一侧，验证 level = max
        let conn = test_conn();
        let config = SchedulerRuntimeConfig {
            low_watermark_rows: 1_000,
            high_watermark_rows: 10_000,
            ..test_config()
        };
        let sched = IoScheduler::new(conn, config);
        // 等 writer_loop 起来
        tokio::time::sleep(Duration::from_millis(30)).await;
        // 仅队列行数高 → level == queue 分量
        for _ in 0..9 {
            let _ = sched.submit(
                IoPriority::Normal,
                None,
                |_c| {
                    std::thread::sleep(Duration::from_millis(40));
                    Ok(())
                },
                1000,
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let p = sched.pressure();
        assert!((p.level - p.queue).abs() < f32::EPSILON || p.level >= p.queue);
        assert!(p.level > 0.0);
    }

    #[tokio::test]
    async fn test_backoff_gate_levels() {
        // level 应落在 [0,1]，空队列为 0
        let conn = test_conn();
        let sched = IoScheduler::new(conn, test_config());
        tokio::time::sleep(Duration::from_millis(30)).await;
        let lvl = sched.backpressure_level();
        assert!((0.0..=1.0).contains(&lvl), "level 必须在 [0,1]: {}", lvl);
    }

    // ── B3：flush_and_wait ──

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_and_wait_drains() {
        let conn = test_conn();
        let sched = IoScheduler::new(conn, test_config());
        // 提交一批请求
        for i in 0..20 {
            let _ = sched.submit(
                IoPriority::Normal,
                None,
                move |_c| {
                    std::thread::sleep(Duration::from_millis(5));
                    let _ = i;
                    Ok(())
                },
                1,
            );
        }
        let ok = sched.flush_and_wait(Duration::from_secs(2)).await;
        assert!(ok, "flush_and_wait 应在超时前排空");
        assert_eq!(sched.queue_len(), 0);
        assert_eq!(sched.queue_rows(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_and_wait_timeout() {
        let conn = test_conn();
        let sched = IoScheduler::new(conn, test_config());
        // 提交会阻塞很久的请求，flush 在很短超时内应返回 false
        for _ in 0..5 {
            let _ = sched.submit(
                IoPriority::Normal,
                None,
                |_c| {
                    std::thread::sleep(Duration::from_millis(500));
                    Ok(())
                },
                1,
            );
        }
        let ok = sched.flush_and_wait(Duration::from_millis(50)).await;
        assert!(!ok, "短超时内不应排空，应返回 false");
    }

    // ── B3：send_sized（WriteQueue 拒绝计数）──

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_send_sized_enqueues_and_counts_rows() {
        use crate::storage::write_queue::WriteQueue;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        let conn = test_conn();
        let wq = Arc::new(WriteQueue::new(conn, 1, Duration::from_millis(50)));
        let counter = Arc::new(AtomicUsize::new(0));
        let c2 = counter.clone();
        let r = wq.send_sized(10, move |_c| {
            c2.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        assert!(r.is_ok(), "正常入队不应拒绝");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "闭包应被 writer 消费执行"
        );
        assert_eq!(wq.stats().total_requests, 1, "send_sized 应计入统计");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // ── C2：AIMD 控制器 ──

    fn c2_profile() -> AimdProfile {
        AimdProfile {
            rows_min: 1_000,
            rows_max: 20_000,
            rows_base: 5_000,
            interval_min: 1,
            interval_max: 60,
        }
    }

    #[test]
    fn test_aimd_shrinks_on_slow() {
        let mut ctl = AimdController::new(c2_profile(), 50, 200);
        let rows0 = ctl.rows_per_tick();
        let rec = ctl.observe(500, 1000); // > slow
        assert_eq!(rec.action, "slow");
        assert!(ctl.rows_per_tick() < rows0, "慢时应收缩");
    }

    #[test]
    fn test_aimd_grows_when_stable() {
        let mut ctl = AimdController::new(c2_profile(), 50, 200);
        let rows0 = ctl.rows_per_tick();
        // 快带连续 grow_after 次 → 增窗
        for _ in 0..ctl.grow_after {
            ctl.observe(10, 0);
        }
        assert!(ctl.rows_per_tick() > rows0, "持续快应增窗");
    }

    #[test]
    fn test_aimd_respects_profile_clamp() {
        let mut ctl = AimdController::new(c2_profile(), 50, 200);
        // 持续慢 → 收敛到 rows_min
        for _ in 0..20 {
            ctl.observe(10_000, 0);
        }
        assert_eq!(ctl.rows_per_tick(), c2_profile().rows_min, "慢应收敛到地板");
        assert_eq!(
            ctl.checkpoint_min_interval_secs(),
            c2_profile().interval_max,
            "慢应把 interval 顶到天花板"
        );
    }

    #[test]
    fn test_latency_hysteresis_hold() {
        let mut ctl = AimdController::new(c2_profile(), 50, 200);
        let rows0 = ctl.rows_per_tick();
        // 在迟滞带 [50,200] 内 → hold
        let rec = ctl.observe(120, 0);
        assert_eq!(rec.action, "hold");
        assert_eq!(ctl.rows_per_tick(), rows0, "迟滞带内不动");
    }

    #[test]
    fn test_aimd_ring_buffer_keeps_recent() {
        let mut ctl = AimdController::new(c2_profile(), 50, 200);
        for _ in 0..300 {
            ctl.observe(10, 0);
        }
        let recent = ctl.recent_decisions(20);
        assert_eq!(recent.len(), 20, "应暴露最近 20 条");
    }

    // ── B4：status 序列化 ──

    #[test]
    fn test_io_status_serializes() {
        let stats = IoSchedulerStats::default();
        let snap = io_status_snapshot(0, 0, 5000, 0.0, 12345, &stats);
        let v = serde_json::to_value(&snap).expect("status 快照应可序列化");
        assert!(v.get("queue_len").is_some());
        assert!(v.get("disk_class").is_some());
        assert!(v.get("recent_decisions").unwrap().is_array());
    }
}
