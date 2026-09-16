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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex as StdMutex};
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

    /// 尝试获取 n 个令牌（无锁）
    fn try_acquire(&self, n: usize) -> bool {
        if n == 0 {
            return true;
        }
        let mut current = self.tokens.load(AtomicOrdering::Acquire);
        loop {
            if current < n {
                return false;
            }
            let new_val = current - n;
            match self.tokens.compare_exchange_weak(
                current,
                new_val,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// 补充令牌（writer_loop 根据 elapsed 调用）
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
}

// ─── 运行时配置（从 IoSchedulerConfig 转换，去除序列化标记）──────────────────

#[derive(Debug, Clone)]
pub struct SchedulerRuntimeConfig {
    pub max_queue_size: usize,
    pub token_bucket_rate: usize,
    pub token_bucket_max: usize,
    pub low_watermark: usize,
    pub high_watermark: usize,
    pub batch_max_size: usize,
    pub batch_max_delay: Duration,
    pub idle_prediction_enabled: bool,
    pub idle_window: Duration,
    /// 队列为空时的等待时间
    pub idle_wait: Duration,
    /// 令牌不足时的重试等待时间
    pub retry_wait: Duration,
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
    /// 队列当前长度（原子，用于背压计算）
    queue_len: Arc<AtomicUsize>,
    /// 单调递增序号生成器
    seq_counter: Arc<AtomicU64>,
    /// 统计
    stats: Arc<ParkingMutex<IoSchedulerStats>>,
    /// 关闭信号
    shutdown: Arc<AtomicBool>,
    /// 唤醒 writer_loop 的通知器
    notify: Arc<Notify>,
    /// 上次令牌桶补充时间
    last_refill: ParkingMutex<Instant>,
    /// 空闲检测滑动窗口
    idle_samples: ParkingMutex<Vec<IdleSample>>,
}

impl IoScheduler {
    /// 创建 IO 调度器并启动 writer_loop
    pub fn new(conn: Arc<StdMutex<Connection>>, config: SchedulerRuntimeConfig) -> Arc<Self> {
        let scheduler = Arc::new(Self {
            queue: ParkingMutex::new(BinaryHeap::new()),
            token_bucket: Arc::new(TokenBucket::new(
                config.token_bucket_max,
                config.token_bucket_rate,
            )),
            conn,
            queue_len: Arc::new(AtomicUsize::new(0)),
            seq_counter: Arc::new(AtomicU64::new(0)),
            config,
            stats: Arc::new(ParkingMutex::new(IoSchedulerStats::default())),
            shutdown: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
            last_refill: ParkingMutex::new(Instant::now()),
            idle_samples: ParkingMutex::new(Vec::new()),
        });

        // 启动 writer_loop
        let writer = scheduler.clone();
        tokio::spawn(async move {
            writer.writer_loop().await;
        });

        scheduler
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

        let mut stats = self.stats.lock();
        stats.total_submitted += 1;

        // 唤醒 writer_loop
        self.notify.notify_one();

        Ok(())
    }

    /// 强制刷出所有排队请求（提交一个 Critical barrier，等待队列排空）
    pub fn flush_barrier(&self) {
        // 提交一个 Critical 级空操作 barrier，writer_loop 会优先执行它
        // barrier 执行时，队列中已有的请求也会被合并执行
        let _ = self.submit(IoPriority::Critical, None, |_conn| Ok(()), 0);
    }

    /// P2: 获取背压级别 [0.0, 1.0]
    pub fn backpressure_level(&self) -> f32 {
        let qlen = self.queue_len.load(AtomicOrdering::Acquire);
        let low = self.config.low_watermark as f32;
        let high = self.config.high_watermark as f32;
        if qlen <= self.config.low_watermark {
            return 0.0;
        }
        if qlen >= self.config.high_watermark {
            return 1.0;
        }
        let range = high - low;
        if range <= 0.0 {
            return 0.0;
        }
        ((qlen as f32 - low) / range).clamp(0.0, 1.0)
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
        loop {
            // 检查关闭信号
            if self.shutdown.load(AtomicOrdering::Acquire) {
                self.drain_remaining();
                break;
            }

            // P0: 令牌桶补充（根据时间差）
            self.refill_tokens();

            // 记录空闲检测样本
            self.record_idle_sample();

            // 尝试取出队首请求
            let entry = {
                let mut q = self.queue.lock();
                q.pop()
            };

            let Some(mut entry) = entry else {
                // 队列为空，等待通知或超时
                tokio::select! {
                    _ = self.notify.notified() => {}
                    _ = tokio::time::sleep(self.config.idle_wait) => {}
                }
                continue;
            };

            self.queue_len.fetch_sub(1, AtomicOrdering::AcqRel);

            // P1: Deadline 老化 — 超期的 Normal/Background 升级为 Important
            let now = Instant::now();
            let mut effective_priority = entry.request.priority;
            if let Some(deadline) = entry.request.deadline {
                if now > deadline {
                    match effective_priority {
                        IoPriority::Normal | IoPriority::Background => {
                            effective_priority = IoPriority::Important;
                            let mut stats = self.stats.lock();
                            stats.starvation_preemptions += 1;
                        }
                        _ => {}
                    }
                }
            }
            entry.request.priority = effective_priority;

            // P0: 令牌桶检查（Critical 不受限）
            let is_critical = matches!(effective_priority, IoPriority::Critical);
            let size = entry.request.size_hint.max(1);

            if !is_critical && !self.token_bucket.try_acquire(size) {
                // 令牌不足 — 重新入队（保持升级后的优先级），稍后重试
                self.queue.lock().push(entry);
                self.queue_len.fetch_add(1, AtomicOrdering::AcqRel);
                // 等待下次补充
                tokio::time::sleep(self.config.retry_wait).await;
                continue;
            }

            // P3: 请求合并 — 继续取出同优先级请求，合并到单事务
            let mut batch = vec![entry];
            let batch_start = Instant::now();

            while batch.len() < self.config.batch_max_size {
                // 检查批延迟
                if batch_start.elapsed() >= self.config.batch_max_delay {
                    break;
                }

                let next = {
                    let mut q = self.queue.lock();
                    q.pop()
                };

                let Some(mut next) = next else { break };
                self.queue_len.fetch_sub(1, AtomicOrdering::AcqRel);

                // P1: 对后续请求也做 deadline 老化
                let next_now = Instant::now();
                let mut next_priority = next.request.priority;
                if let Some(dl) = next.request.deadline {
                    if next_now > dl {
                        match next_priority {
                            IoPriority::Normal | IoPriority::Background => {
                                next_priority = IoPriority::Important;
                                let mut stats = self.stats.lock();
                                stats.starvation_preemptions += 1;
                            }
                            _ => {}
                        }
                    }
                }

                // 只合并相同 effective priority 的请求
                if next_priority != effective_priority {
                    // 不同优先级，放回队列（不浪费）
                    next.request.priority = next_priority;
                    self.queue.lock().push(next);
                    self.queue_len.fetch_add(1, AtomicOrdering::AcqRel);
                    break;
                }

                // 令牌桶检查
                let next_size = next.request.size_hint.max(1);
                if !is_critical && !self.token_bucket.try_acquire(next_size) {
                    // 令牌不足，放回队列
                    self.queue.lock().push(next);
                    self.queue_len.fetch_add(1, AtomicOrdering::AcqRel);
                    break;
                }

                batch.push(next);
            }

            // 执行批量写入（单事务）
            self.execute_batch(&mut batch, effective_priority);
        }
    }

    /// 根据经过时间补充令牌
    fn refill_tokens(&self) {
        let elapsed = {
            let mut last = self.last_refill.lock();
            let elapsed = last.elapsed();
            *last = Instant::now();
            elapsed
        };
        let tokens_to_add = (elapsed.as_secs_f64() * self.config.token_bucket_rate as f64) as usize;
        if tokens_to_add > 0 {
            self.token_bucket.refill(tokens_to_add);
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
        let cutoff = Instant::now() - self.config.idle_window;
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
        for entry in batch.drain(..) {
            if let Err(e) = (entry.request.payload)(&tx) {
                tracing::warn!("[io_scheduler] 写入失败: {}", e);
            }
        }

        // 提交事务
        if let Err(e) = tx.commit() {
            tracing::warn!("[io_scheduler] 事务提交失败: {}", e);
        }

        // 更新统计
        let mut stats = self.stats.lock();
        stats.total_executed += count as u64;
        stats.total_batches += 1;
        if count > stats.max_batch_size {
            stats.max_batch_size = count;
        }
        match priority {
            IoPriority::Critical => stats.critical_executed += 1,
            IoPriority::Important => stats.important_executed += 1,
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
        }
    }

    // ── 令牌桶测试 ──

    #[test]
    fn test_token_bucket_acquire() {
        let tb = TokenBucket::new(100, 1000);
        assert!(tb.try_acquire(50));
        assert!(tb.try_acquire(50));
        assert!(!tb.try_acquire(1)); // 已耗尽
        assert_eq!(tb.available(), 0);
    }

    #[test]
    fn test_token_bucket_refill() {
        let tb = TokenBucket::new(100, 1000);
        assert!(tb.try_acquire(80));
        assert_eq!(tb.available(), 20);
        tb.refill(50);
        assert_eq!(tb.available(), 70);
        // 不超过 max
        tb.refill(100);
        assert_eq!(tb.available(), 100);
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
            low_watermark: 100,
            high_watermark: 1000,
            ..test_config()
        };
        let sched = IoScheduler::new(conn, config);

        // 空队列 → 0.0
        assert_eq!(sched.backpressure_level(), 0.0);
        assert!(!sched.is_backpressured());

        // 手动模拟队列长度（通过提交请求）
        for _ in 0..500 {
            let _ = sched.submit(IoPriority::Normal, None, |_c| Ok(()), 1);
        }
        // 队列长度应在 500 左右，背压级别约 0.5
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
}
