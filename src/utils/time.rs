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
}
