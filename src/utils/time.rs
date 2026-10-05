//! 时间工具：`Instant` 安全运算，避免减法下溢 panic。
//!
//! `Instant::now() - Duration` 在 `Duration` 超过进程已运行时长时会 panic。
//! 当配置的窗口/TTL 较大（如数小时、数天）而进程刚启动时，就会触发。
//! 本模块提供 `cutoff_before` 作为安全替代。

use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// 进程启动时刻（近似，首次调用时记录）。
fn process_start() -> Instant {
    static START: OnceLock<Instant> = OnceLock::new();
    *START.get_or_init(Instant::now)
}

/// 安全计算 `Instant::now() - d`。
///
/// 当 `d` 超过进程已运行时长时，`Instant::now() - d` 会下溢 panic；
/// 此时返回进程启动时刻。由于所有记录的时间戳都晚于进程启动时刻，
/// 该回退值与"无限早"的 cutoff 在比较语义上完全等价。
pub fn cutoff_before(d: Duration) -> Instant {
    Instant::now().checked_sub(d).unwrap_or_else(process_start)
}

/// 判断 `ts` 距现在是否在 `window` 之内（等价于 `ts >= now - window`，且不下溢 panic）。
pub fn within(ts: Instant, window: Duration) -> bool {
    ts.elapsed() <= window
}

fn system_unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// `Instant` → Unix 秒（供持久化 last_active 等字段）。
///
/// 基于"当前墙钟 − 已运行时长"换算；进程内单调一致，
/// 重启后由 [`unix_secs_to_instant`] 还原（精度受墙钟调整影响，可接受）。
pub fn instant_to_unix_secs(t: Instant) -> i64 {
    system_unix_secs() - t.elapsed().as_secs() as i64
}

/// Unix 秒 → `Instant`（持久化时间戳还原）。
///
/// 未来时间截断为 now；早于进程启动的时间回退为进程启动时刻
/// （与 [`cutoff_before`] 的下溢语义一致，避免 panic）。
pub fn unix_secs_to_instant(secs: i64) -> Instant {
    let now_unix = system_unix_secs();
    if secs >= now_unix {
        return Instant::now();
    }
    cutoff_before(Duration::from_secs((now_unix - secs) as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cutoff_before_normal() {
        let c = cutoff_before(Duration::from_secs(1));
        assert!(c <= Instant::now());
    }

    #[test]
    fn test_cutoff_before_underflow_falls_back() {
        // 极大窗口：真实 cutoff 会早于进程启动，回退到进程启动时刻而不 panic
        let c = cutoff_before(Duration::from_secs(u64::MAX / 2));
        assert!(c <= Instant::now());
    }

    #[test]
    fn test_within() {
        assert!(within(Instant::now(), Duration::from_secs(1)));
    }

    #[test]
    fn test_unix_instant_round_trip() {
        let t = Instant::now();
        let secs = instant_to_unix_secs(t);
        let back = unix_secs_to_instant(secs);
        // 还原误差 ≤ 1 秒
        assert!(back.elapsed() <= t.elapsed() + Duration::from_secs(1));
        assert!(t.elapsed() <= back.elapsed() + Duration::from_secs(1));
    }

    #[test]
    fn test_unix_secs_to_instant_future_clamps_to_now() {
        let future = system_unix_secs() + 3600;
        let t = unix_secs_to_instant(future);
        assert!(t.elapsed().as_secs() < 2);
    }

    #[test]
    fn test_unix_secs_to_instant_stale_falls_back_to_process_start() {
        // 早于进程启动的时间戳：回退为进程启动时刻，不 panic
        let stale = system_unix_secs() - 100 * 365 * 24 * 3600;
        let t = unix_secs_to_instant(stale);
        assert!(t.elapsed() >= process_start().elapsed());
    }
}
