//! WAL checkpoint 专用执行线程（A1）。
//!
//! 背景：checkpoint 与所有写入共用 `storage.conn` 那把 `std::sync::Mutex`，且旧实现直接在
//! async fn 里同步执行——慢盘单次 checkpoint 可达数百秒，期间全节点写路径停摆、任务槽堆积。
//! 本模块把 checkpoint 搬到独立 OS 线程（`pdc-ckpt`）+ 独立连接，通过 mpsc 收任务，
//! 并以 `AtomicBool` 做单飞：永远只有一个 checkpoint 在跑，消除堆积。
//!
//! 策略（何时触发/退避）不在本模块：本模块只负责"执行 + 回填统计"，决策见 main.rs 的
//! `io_checkpoint_tick` 与 [`decide_checkpoint`] 纯函数。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::storage::db::{CheckpointMode, CheckpointOutcome, Storage};

/// 每累计多少次成功 checkpoint 输出一条 INFO（成败可观测：连续静默成功会让人无法
/// 判断 worker 是否存活，.52 事故 WAL 298MB 穿透阈值时全天零日志）。
// [ALLOWED-HARDCODED: 日志频率常量，非业务可调参数]
const SUCCESS_LOG_EVERY: u64 = 20;

/// 执行线程所需的策略参数（从 `IoSchedulerConfig` 裁出来，避免 worker 持有整份配置）。
#[derive(Debug, Clone, Copy)]
pub struct CheckpointWorkerConfig {
    /// 单次 checkpoint 超过此毫秒判定为"慢"
    pub slow_ms: u64,
    /// 慢后退避基准（秒）
    pub backoff_base_secs: u64,
    /// 退避上限（秒）
    pub backoff_max_secs: u64,
    /// 退避系数（next_allowed = base * factor^streak）
    pub backoff_factor: f32,
}

/// worker 回填的运行统计（tick 读取以做决策/告警）。
#[derive(Debug, Clone, Default)]
pub struct WorkerStat {
    /// 上次完成时刻
    pub last_at: Option<Instant>,
    /// 下次允许触发时刻（熔断退避窗口）
    pub next_allowed: Option<Instant>,
    /// 连续慢次数（一次快结果归零）
    pub streak: u32,
    /// 上次耗时（ms）
    pub last_ms: u64,
    /// 累计慢次数
    pub slow_total: u64,
    /// 累计执行次数
    pub total: u64,
    /// checkpoint 耗时 EWMA（ms）
    pub last_ewma_ms: f64,
    /// 累计成功次数（执行完成且未被跳过）
    pub ok_total: u64,
    /// 累计失败次数（执行闭包返回 Err）
    pub fail_total: u64,
    /// 累计 busy 次数（PRAGMA 报告有连接阻塞，部分完成）
    pub busy_total: u64,
    /// 最近一次成功 TRUNCATE 完成时刻
    pub last_truncate_at: Option<Instant>,
    /// 最近一次成功 TRUNCATE 耗时（ms）
    pub last_truncate_ms: u64,
    /// 最近一次成功 TRUNCATE 时 WAL 中剩余帧数
    pub last_truncate_wal_frames: u64,
}

struct Job {
    mode: CheckpointMode,
    reply: Option<oneshot::Sender<CheckpointOutcome>>,
}

/// 专用 checkpoint worker：单飞执行，永不堆积。
pub struct CheckpointWorker {
    tx: mpsc::Sender<Job>,
    inflight: Arc<AtomicBool>,
    stat: Arc<Mutex<WorkerStat>>,
}

impl CheckpointWorker {
    /// 启动 `pdc-ckpt` 线程。
    pub fn spawn(storage: Arc<Storage>, cfg: CheckpointWorkerConfig) -> Arc<Self> {
        Self::spawn_with_runner(storage, cfg, |s, mode| s.checkpoint_once(mode))
    }

    /// 可注入执行闭包的构造（便于单测）。生产路径走 [`spawn`]。
    fn spawn_with_runner<F>(
        storage: Arc<Storage>,
        cfg: CheckpointWorkerConfig,
        runner: F,
    ) -> Arc<Self>
    where
        F: Fn(&Storage, CheckpointMode) -> anyhow::Result<CheckpointOutcome> + Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<Job>();
        let inflight = Arc::new(AtomicBool::new(false));
        let stat = Arc::new(Mutex::new(WorkerStat::default()));

        let inflight2 = inflight.clone();
        let stat2 = stat.clone();
        std::thread::Builder::new()
            .name("pdc-ckpt".to_string())
            .spawn(move || worker_loop(rx, inflight2, stat2, storage, cfg, runner))
            .expect("spawn pdc-ckpt thread");

        Arc::new(Self { tx, inflight, stat })
    }

    /// 非阻塞触发；返回 false 表示"已有单飞在跑，本次跳过"。
    pub fn trigger(&self, mode: CheckpointMode) -> bool {
        // CAS 单飞：仅当 inflight==false 时抢到执行权
        if self
            .inflight
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return false;
        }
        let job = Job { mode, reply: None };
        if self.tx.send(job).is_err() {
            // worker 线程已退出：归还标志，避免永久卡死
            self.inflight.store(false, Ordering::SeqCst);
            return false;
        }
        true
    }

    /// 是否有 checkpoint 正在执行。
    pub fn inflight(&self) -> bool {
        self.inflight.load(Ordering::SeqCst)
    }

    /// 读取运行统计（快照）。
    pub fn stat(&self) -> WorkerStat {
        self.stat.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// 最近一次成功 TRUNCATE checkpoint 的记录（供决策 tick / 观测任务查询）。
    /// 从未成功执行过 TRUNCATE 时返回 None。
    pub fn last_truncate_success(&self) -> Option<TruncateRecord> {
        let st = self.stat();
        Some(TruncateRecord {
            at: st.last_truncate_at?,
            elapsed_ms: st.last_truncate_ms,
            wal_frames: st.last_truncate_wal_frames,
        })
    }
}

/// 最近一次成功 TRUNCATE checkpoint 的信息。
#[derive(Debug, Clone, Copy)]
pub struct TruncateRecord {
    /// 完成时刻
    pub at: Instant,
    /// 耗时（ms）
    pub elapsed_ms: u64,
    /// 完成时 WAL 中剩余帧数（TRUNCATE 成功后通常为 0）
    pub wal_frames: u64,
}

/// worker 主循环：串行处理 job，执行完回填 stat 后释放 inflight。
fn worker_loop<F>(
    rx: Receiver<Job>,
    inflight: Arc<AtomicBool>,
    stat: Arc<Mutex<WorkerStat>>,
    storage: Arc<Storage>,
    cfg: CheckpointWorkerConfig,
    runner: F,
) where
    F: Fn(&Storage, CheckpointMode) -> anyhow::Result<CheckpointOutcome>,
{
    for job in rx {
        // 成败可观测：runner 的 Err 不能像旧实现那样静默吞掉（.52 全天零成败日志的教训）
        let (out, fail_msg) = match runner(&storage, job.mode) {
            Ok(o) => (o, None),
            Err(e) => {
                let synthetic = CheckpointOutcome {
                    mode: job.mode,
                    busy: true,
                    wal_frames: 0,
                    checkpointed: 0,
                    elapsed: Duration::ZERO,
                    skipped: Some("error"),
                };
                (synthetic, Some(e.to_string()))
            }
        };
        // 回填统计（持锁极短），日志素材锁内采集、锁外输出
        let log = {
            let mut st = stat.lock().unwrap_or_else(|e| e.into_inner());
            st.total += 1;
            let ms = out.elapsed.as_millis() as u64;
            let now = Instant::now();
            st.last_at = Some(now);
            st.last_ms = ms;
            const EWMA_ALPHA: f64 = 0.2;
            st.last_ewma_ms = if st.last_ewma_ms == 0.0 {
                ms as f64
            } else {
                EWMA_ALPHA * ms as f64 + (1.0 - EWMA_ALPHA) * st.last_ewma_ms
            };
            let is_slow = ms >= cfg.slow_ms;
            if is_slow {
                st.streak += 1;
                st.slow_total += 1;
            } else {
                st.streak = 0;
            }
            st.next_allowed = Some(compute_next_allowed(
                now,
                st.streak,
                Duration::from_secs(cfg.backoff_base_secs),
                cfg.backoff_factor,
                Duration::from_secs(cfg.backoff_max_secs),
            ));
            // 分类计数：失败 / 跳过（如内存库）/ 真实成功（含 busy 部分完成）
            match &fail_msg {
                Some(_) => st.fail_total += 1,
                None => match out.skipped {
                    Some(_) => {}
                    None => {
                        st.ok_total += 1;
                        if out.busy {
                            st.busy_total += 1;
                        }
                        if job.mode == CheckpointMode::Truncate {
                            st.last_truncate_at = Some(now);
                            st.last_truncate_ms = ms;
                            st.last_truncate_wal_frames = out.wal_frames;
                        }
                    }
                },
            }
            // 日志素材：失败/慢判定 WARN；每 SUCCESS_LOG_EVERY 次成功一条 INFO
            if let Some(msg) = &fail_msg {
                Some(CkptLog::Fail(st.fail_total, msg.clone(), job.mode))
            } else if is_slow {
                Some(CkptLog::Slow(
                    ms,
                    cfg.slow_ms,
                    st.streak,
                    st.slow_total,
                    job.mode,
                ))
            } else if out.skipped.is_none() && st.ok_total.is_multiple_of(SUCCESS_LOG_EVERY) {
                Some(CkptLog::Periodic(
                    st.ok_total,
                    out.wal_frames,
                    st.busy_total,
                    ms,
                    st.last_ewma_ms,
                    job.mode,
                ))
            } else {
                None
            }
        };
        match log {
            Some(CkptLog::Fail(fail_total, msg, mode)) => warn!(
                "[checkpoint] 执行失败（累计 {} 次）：{}，mode={:?}",
                fail_total, msg, mode
            ),
            Some(CkptLog::Slow(ms, slow_ms, streak, slow_total, mode)) => warn!(
                "[checkpoint] 慢判定：mode={:?} 耗时 {}ms ≥ 慢阈值 {}ms，连续慢 {} 次，累计慢 {} 次",
                mode, ms, slow_ms, streak, slow_total
            ),
            Some(CkptLog::Periodic(ok_total, wal_frames, busy_total, ms, ewma_ms, mode)) => info!(
                "[checkpoint] 累计成功 {} 次：mode={:?} wal_frames={} busy累计={} 本次耗时 {}ms，EWMA {:.0}ms",
                ok_total, mode, wal_frames, busy_total, ms, ewma_ms
            ),
            None => {}
        }
        // 先释放单飞，再回结果（顺序无关紧要，但避免持锁发送）
        inflight.store(false, Ordering::SeqCst);
        if let Some(tx) = job.reply {
            let _ = tx.send(out);
        }
    }
}

/// 单次执行完成后的日志素材（锁内采集、锁外输出，避免日志宏耗时占锁）。
enum CkptLog {
    /// 执行失败：累计失败次数、错误信息、模式
    Fail(u64, String, CheckpointMode),
    /// 慢判定：本次耗时、慢阈值、连续慢次数、累计慢次数、模式
    Slow(u64, u64, u32, u64, CheckpointMode),
    /// 周期性成功汇报：累计成功、WAL 帧数、busy 累计、本次耗时 ms、EWMA ms、模式
    Periodic(u64, u64, u64, u64, f64, CheckpointMode),
}

/// 纯函数：根据上次完成时刻与连续慢次数，计算下次允许触发时刻。
///
/// `next_allowed = last_at + min(base * factor^streak, max)`；
/// `streak == 0`（一次快结果）时退避为 0（立即允许）。提成纯函数以便单测。
pub fn compute_next_allowed(
    last_at: Instant,
    streak: u32,
    base: Duration,
    factor: f32,
    max: Duration,
) -> Instant {
    if streak == 0 {
        return last_at;
    }
    let backoff = base.as_secs_f64() * (factor as f64).powi(streak as i32);
    let backoff = Duration::from_secs_f64(backoff).min(max);
    last_at + backoff
}

/// 决策 tick 的纯函数（A2）：根据当前 stat / WAL 字节数，决定是否触发 checkpoint。
///
/// 规则（O(1)，不阻塞）：
/// - inflight → 跳过；
/// - 在退避窗口 `next_allowed` 内 → 跳过；
/// - `wal_bytes < soft` → 跳过（未到软阈值）；
/// - 距上次 < min_interval 且 `wal_bytes < hard` → 跳过；
/// - 否则触发 `Passive`。
pub fn decide_checkpoint(
    stat: &WorkerStat,
    wal_bytes: u64,
    inflight: bool,
    now: Instant,
    soft_bytes: u64,
    hard_bytes: u64,
    min_interval: Duration,
) -> Option<CheckpointMode> {
    if inflight {
        return None;
    }
    if let Some(next_allowed) = stat.next_allowed {
        if now < next_allowed {
            return None;
        }
    }
    if wal_bytes < soft_bytes {
        return None;
    }
    if let Some(last_at) = stat.last_at {
        if now < last_at + min_interval && wal_bytes < hard_bytes {
            return None;
        }
    }
    Some(CheckpointMode::Passive)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn test_checkpoint_breaker_backoff() {
        let t0 = Instant::now();
        let base = Duration::from_secs(5);
        let factor = 2.0f32;
        let max = Duration::from_secs(30);
        // streak=0：不退避
        assert_eq!(compute_next_allowed(t0, 0, base, factor, max), t0);
        // streak=1：5 * 2 = 10s
        assert_eq!(
            compute_next_allowed(t0, 1, base, factor, max).duration_since(t0),
            Duration::from_secs(10)
        );
        // streak=2：5 * 4 = 20s
        assert_eq!(
            compute_next_allowed(t0, 2, base, factor, max).duration_since(t0),
            Duration::from_secs(20)
        );
        // streak=3：5 * 8 = 40s，封顶 30s
        assert_eq!(
            compute_next_allowed(t0, 3, base, factor, max).duration_since(t0),
            Duration::from_secs(30)
        );
        // streak=5：仍封顶 30s
        assert_eq!(
            compute_next_allowed(t0, 5, base, factor, max).duration_since(t0),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn test_checkpoint_worker_single_flight() {
        let storage = Arc::new(Storage::memory().unwrap());
        let cfg = CheckpointWorkerConfig {
            slow_ms: 1000,
            backoff_base_secs: 1,
            backoff_max_secs: 10,
            backoff_factor: 2.0,
        };
        let started = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let executed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started_c = started.clone();
        let release_c = release.clone();
        let executed_c = executed.clone();
        let w = CheckpointWorker::spawn_with_runner(
            storage,
            cfg,
            move |_s: &Storage, _m: CheckpointMode| {
                executed_c.fetch_add(1, Ordering::SeqCst);
                started_c.wait(); // 通知主线程：worker 已在执行
                release_c.wait(); // 等待主线程放行
                Ok(CheckpointOutcome {
                    mode: CheckpointMode::Passive,
                    busy: false,
                    wal_frames: 0,
                    checkpointed: 0,
                    elapsed: Duration::from_millis(1),
                    skipped: None,
                })
            },
        );

        // 第一次 trigger 抢到执行权，worker 阻塞在 runner 内
        assert!(w.trigger(CheckpointMode::Passive));
        started.wait(); // 等到 worker 真的进了 runner（inflight=true）
        assert!(w.inflight());

        // 并发 trigger ×100：全部应被单飞拒绝
        let mut handles = Vec::new();
        for _ in 0..100 {
            let w = w.clone();
            handles.push(std::thread::spawn(move || {
                w.trigger(CheckpointMode::Passive)
            }));
        }
        let mut ok = 0usize;
        for h in handles {
            if h.join().unwrap() {
                ok += 1;
            }
        }
        assert_eq!(ok, 0, "inflight 时并发 trigger 必须全部跳过");
        assert_eq!(executed.load(Ordering::SeqCst), 1);

        release.wait(); // 放行 worker
                        // 等 worker 结束、inflight 复位
        std::thread::sleep(Duration::from_millis(50));
        assert!(!w.inflight());
    }

    #[test]
    fn test_checkpoint_tick_skips_below_soft() {
        let now = Instant::now();
        let stat = WorkerStat::default();
        // WAL 远低于 soft：跳过
        assert_eq!(
            decide_checkpoint(
                &stat,
                1_000_000,
                false,
                now,
                32 * 1024 * 1024,
                128 * 1024 * 1024,
                Duration::from_secs(5)
            ),
            None
        );
    }

    #[test]
    fn test_checkpoint_tick_forces_above_hard() {
        let now = Instant::now();
        // 刚 checkpoint 过 1s（< min_interval=5s），但 WAL 已超过 hard → 强制触发
        let stat = WorkerStat {
            last_at: Some(now - Duration::from_secs(1)),
            ..Default::default()
        };
        let hard = 128u64 * 1024 * 1024;
        let wal = hard + 1;
        assert_eq!(
            decide_checkpoint(
                &stat,
                wal,
                false,
                now,
                32 * 1024 * 1024,
                hard,
                Duration::from_secs(5)
            ),
            Some(CheckpointMode::Passive)
        );
        // inflight 时即使超 hard 也跳过
        assert_eq!(
            decide_checkpoint(
                &stat,
                wal,
                true,
                now,
                32 * 1024 * 1024,
                hard,
                Duration::from_secs(5)
            ),
            None
        );
    }
}
