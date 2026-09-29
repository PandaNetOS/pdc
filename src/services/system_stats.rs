//! 进程运行时状态缓存（CPU / 内存）—— 供 HTTP API 与监控任务统一读取。
//!
//! 背景（2026-09-30 .51 事故）：`/api/v1/system` 的 system_handler 与 main.rs 的
//! memory_monitor 每次都执行 `System::new_all()`，Windows 上会全量枚举进程/磁盘/网络，
//! 重负载下单次可达数秒到数分钟，监控面板持续轮询直接把 api_runtime 的 worker 全部
//! park 死（连无锁的 /metrics 都超时）。本模块持有全局唯一的 `sysinfo::System` 实例，
//! 只做**最小刷新**（仅自身进程的 CPU + 内存），并按 `MIN_REFRESH_INTERVAL` 限频：
//! 距上次刷新不足间隔时直接返回缓存值。任何调用方都**禁止**再触碰 `new_all()`。
//!
//! 说明：cpu_usage 是两次刷新之间的差值，首次刷新为 0；缓存周期刷新（≥1s 间隔）
//! 天然满足差值条件，第二次起即为有效值。

use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// 两次真实刷新之间的最小间隔。刷新成本仅为"自身进程"的最小刷新（无全量枚举），
/// 1 秒粒度对监控面板/内存巡检足够。
// [ALLOWED-HARDCODED: 缓存刷新粒度常量，非业务可调参数；最小刷新无 IO 风险]
const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// 一次进程运行时状态快照。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SystemSnapshot {
    /// 自身进程 CPU 使用率（%）。首次刷新为 0，第二次起为两次刷新间的差值。
    pub cpu_usage_percent: f64,
    /// 自身进程物理内存（字节；sysinfo 0.32 在 Windows 上同样返回字节）。
    pub mem_bytes: u64,
    /// 快照数据距今的毫秒数：0 表示本次调用就是刚刷新的，>0 表示命中缓存。
    pub staleness_ms: u64,
}

/// 缓存内部状态（Mutex 保护；临界区内只做一次自身进程的最小刷新，无全量枚举）。
struct Inner {
    /// 全局唯一 System 实例（绝不在请求路径重建）
    sys: System,
    /// 自身进程 PID（None = 平台不支持，快照恒为 0）
    pid: Option<Pid>,
    /// 最近一次真实刷新时刻
    last_refreshed: Option<Instant>,
    /// 最近一次刷新得到的快照
    snapshot: SystemSnapshot,
    /// 真实刷新次数（观测用：可用于确认调用方确实命中了缓存）
    refresh_count: u64,
}

/// 进程状态缓存。通过 [`snapshot`]（全局单例）使用；`new` 保留给单测构造独立实例。
pub struct SystemStatsCache {
    inner: Mutex<Inner>,
}

impl SystemStatsCache {
    /// 构造缓存实例（不刷新；首次 snapshot 时才做最小刷新）。
    fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                sys: System::new(),
                pid: sysinfo::get_current_pid().ok(),
                last_refreshed: None,
                snapshot: SystemSnapshot {
                    cpu_usage_percent: 0.0,
                    mem_bytes: 0,
                    staleness_ms: 0,
                },
                refresh_count: 0,
            }),
        }
    }

    /// 读取进程状态快照：距上次刷新 ≥ [`MIN_REFRESH_INTERVAL`] 才真正刷新，否则走缓存。
    pub fn snapshot(&self) -> SystemSnapshot {
        let now = Instant::now();
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let should_refresh = match g.last_refreshed {
            None => true,
            Some(t) => now.duration_since(t) >= MIN_REFRESH_INTERVAL,
        };
        if should_refresh {
            // 最小刷新：只刷自身 PID 的 CPU + 内存，绝不用 new_all()。
            g.refresh_inner(now);
        }
        let mut snap = g.snapshot;
        snap.staleness_ms = g
            .last_refreshed
            .map(|t| now.duration_since(t).as_millis() as u64)
            .unwrap_or(0);
        snap
    }

    /// 真实刷新次数（观测/自测用）。
    pub fn refresh_count(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .refresh_count
    }
}

impl Inner {
    /// 对自身进程做最小刷新并回填快照。调用方持锁。
    fn refresh_inner(&mut self, now: Instant) {
        if let Some(pid) = self.pid {
            let kind = ProcessRefreshKind::new().with_cpu().with_memory();
            self.sys
                .refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, kind);
            let (cpu, mem) = match self.sys.process(pid) {
                Some(p) => (p.cpu_usage() as f64, p.memory()),
                // 理论不可达（自身进程必然存在）；保守归零，等下个周期再刷
                None => (0.0, 0),
            };
            self.snapshot = SystemSnapshot {
                cpu_usage_percent: cpu,
                mem_bytes: mem,
                staleness_ms: 0,
            };
        }
        self.last_refreshed = Some(now);
        self.refresh_count += 1;
    }
}

/// 全局单例。
static GLOBAL_CACHE: OnceLock<SystemStatsCache> = OnceLock::new();

/// 进程状态快照（全局入口）。内部按 1 秒限频刷新，高频调用安全。
///
/// 替代原先每请求一次的 `System::new_all()`（.51 事故根因）。
pub fn snapshot() -> SystemSnapshot {
    GLOBAL_CACHE.get_or_init(SystemStatsCache::new).snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_second_call_hits_cache() {
        let cache = SystemStatsCache::new();
        let s1 = cache.snapshot();
        assert_eq!(cache.refresh_count(), 1, "首次调用必须真实刷新");
        let s2 = cache.snapshot();
        assert_eq!(cache.refresh_count(), 1, "间隔内第二次调用必须命中缓存");
        // 命中缓存：测量字段完全一致（staleness 随墙钟增长，不做全等比较）
        assert_eq!(s1.cpu_usage_percent, s2.cpu_usage_percent);
        assert_eq!(s1.mem_bytes, s2.mem_bytes);
        assert!(
            s2.staleness_ms >= s1.staleness_ms,
            "缓存命中时 staleness 单调不减：{} vs {}",
            s1.staleness_ms,
            s2.staleness_ms
        );
    }

    #[test]
    fn test_snapshot_fields_plausible() {
        let cache = SystemStatsCache::new();
        let s1 = cache.snapshot();
        // 自身进程内存必然大于 0
        assert!(s1.mem_bytes > 0, "进程内存应 > 0，实际 {}", s1.mem_bytes);
        // CPU 使用率百分比范围（首次刷新差值为 0）
        assert!(
            (0.0..=100.0).contains(&s1.cpu_usage_percent),
            "CPU 使用率应在 [0, 100]，实际 {}",
            s1.cpu_usage_percent
        );
        assert_eq!(s1.staleness_ms, 0, "刚刷新完 staleness 应为 0");
    }

    #[test]
    fn test_snapshot_staleness_grows_when_cached() {
        let cache = SystemStatsCache::new();
        let _ = cache.snapshot();
        std::thread::sleep(Duration::from_millis(20));
        let s2 = cache.snapshot();
        assert!(
            s2.staleness_ms >= 20,
            "缓存命中时 staleness 应随时间增长，实际 {}",
            s2.staleness_ms
        );
    }

    #[test]
    fn test_global_snapshot_returns_consistent_type() {
        // 全局入口可用且字段合理（与其他测试共享单例，只做宽松断言）
        let s = snapshot();
        assert!((0.0..=100.0).contains(&s.cpu_usage_percent));
    }
}
