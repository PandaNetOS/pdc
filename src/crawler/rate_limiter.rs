//! 对端响应率自适应限速模块
//!
//! 每个 socket 独立统计滑动窗口内的发送/响应比，响应率过低时自动跳过发送。
//! 响应率 < 30% 进入限速，恢复到 > 50% 解除。

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// 默认滑动窗口大小（秒）
const DEFAULT_WINDOW_SECS: u64 = 60;

/// 单个 socket 的响应率统计（滑动窗口，最近60秒）
pub struct SocketRateStats {
    /// 发送记录（时间戳）
    requests: VecDeque<Instant>,
    /// 接收响应记录（时间戳）
    responses: VecDeque<Instant>,
    /// 窗口大小
    window: Duration,
    /// 是否被限速（跳过发送）
    throttled: bool,
}

impl SocketRateStats {
    pub fn new(window: Duration) -> Self {
        Self {
            requests: VecDeque::new(),
            responses: VecDeque::new(),
            window,
            throttled: false,
        }
    }

    /// 记录一次发送
    pub fn record_request(&mut self) {
        let now = Instant::now();
        self.requests.push_back(now);
        self.cleanup(now);
    }

    /// 记录一次响应接收
    pub fn record_response(&mut self) {
        let now = Instant::now();
        self.responses.push_back(now);
        self.cleanup(now);
    }

    /// 计算当前响应率（0.0 - 1.0）
    pub fn response_rate(&self) -> f64 {
        if self.requests.is_empty() {
            return 1.0; // 无请求时视为100%
        }
        self.responses.len() as f64 / self.requests.len() as f64
    }

    /// 是否应该跳过发送（限速）
    /// 响应率 < 30% 时限速，恢复到 > 50% 时解除
    pub fn should_skip(&mut self) -> bool {
        let rate = self.response_rate();
        if self.throttled {
            if rate > 0.5 {
                self.throttled = false;
            }
        } else if rate < 0.3 && self.requests.len() >= 10 {
            // 至少有10个请求样本才判断，避免冷启动误判
            self.throttled = true;
        }
        self.throttled
    }

    /// 清理窗口外的记录
    fn cleanup(&mut self, now: Instant) {
        let cutoff = now - self.window;
        while self.requests.front().is_some_and(|t| *t < cutoff) {
            self.requests.pop_front();
        }
        while self.responses.front().is_some_and(|t| *t < cutoff) {
            self.responses.pop_front();
        }
    }

    pub fn request_count(&self) -> usize {
        self.requests.len()
    }
    pub fn response_count(&self) -> usize {
        self.responses.len()
    }
    pub fn is_throttled(&self) -> bool {
        self.throttled
    }
}

impl Default for SocketRateStats {
    fn default() -> Self {
        Self::new(Duration::from_secs(DEFAULT_WINDOW_SECS))
    }
}

/// 多 socket 限速管理器
pub struct RateLimiter {
    stats: Vec<parking_lot::Mutex<SocketRateStats>>,
    enabled: bool,
}

impl RateLimiter {
    pub fn new(socket_count: usize, enabled: bool, window: Duration) -> Self {
        Self {
            stats: (0..socket_count)
                .map(|_| parking_lot::Mutex::new(SocketRateStats::new(window)))
                .collect(),
            enabled,
        }
    }

    pub fn record_request(&self, socket_idx: usize) {
        if !self.enabled {
            return;
        }
        if let Some(s) = self.stats.get(socket_idx) {
            s.lock().record_request();
        }
    }

    pub fn record_response(&self, socket_idx: usize) {
        if !self.enabled {
            return;
        }
        if let Some(s) = self.stats.get(socket_idx) {
            s.lock().record_response();
        }
    }

    /// 检查指定 socket 是否应该跳过发送
    pub fn should_skip(&self, socket_idx: usize) -> bool {
        if !self.enabled {
            return false;
        }
        self.stats
            .get(socket_idx)
            .map(|s| s.lock().should_skip())
            .unwrap_or(false)
    }

    /// 获取所有 socket 的响应率（用于状态查询）
    pub fn response_rates(&self) -> Vec<f64> {
        self.stats
            .iter()
            .map(|s| s.lock().response_rate())
            .collect()
    }

    /// 获取限速状态
    pub fn throttled_sockets(&self) -> Vec<bool> {
        self.stats.iter().map(|s| s.lock().is_throttled()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_WINDOW: Duration = Duration::from_secs(DEFAULT_WINDOW_SECS);

    #[test]
    fn test_no_requests_rate_is_full() {
        let stats = SocketRateStats::new(TEST_WINDOW);
        assert_eq!(stats.response_rate(), 1.0);
    }

    #[test]
    fn test_response_rate_calculation() {
        let mut stats = SocketRateStats::new(TEST_WINDOW);
        for _ in 0..10 {
            stats.record_request();
        }
        for _ in 0..3 {
            stats.record_response();
        }
        assert!((stats.response_rate() - 0.3).abs() < f64::EPSILON);
    }

    #[test]
    fn test_throttle_when_rate_low() {
        let mut stats = SocketRateStats::new(TEST_WINDOW);
        // 发送10次，只收到2次响应（20%）
        for _ in 0..10 {
            stats.record_request();
        }
        for _ in 0..2 {
            stats.record_response();
        }
        assert!(stats.should_skip());
        assert!(stats.is_throttled());
    }

    #[test]
    fn test_no_throttle_before_sample_threshold() {
        let mut stats = SocketRateStats::new(TEST_WINDOW);
        // 发送3次，0响应 — 样本不足，不应限速
        for _ in 0..3 {
            stats.record_request();
        }
        assert!(!stats.should_skip());
        assert!(!stats.is_throttled());
    }

    #[test]
    fn test_rate_limiter_disabled() {
        let limiter = RateLimiter::new(4, false, TEST_WINDOW);
        assert!(!limiter.should_skip(0));
        limiter.record_request(0);
        limiter.record_response(0);
    }

    #[test]
    fn test_rate_limiter_multi_socket() {
        let limiter = RateLimiter::new(3, true, TEST_WINDOW);
        limiter.record_request(0);
        limiter.record_request(1);
        limiter.record_response(0);
        let rates = limiter.response_rates();
        assert_eq!(rates.len(), 3);
        assert_eq!(rates[0], 1.0);
        assert_eq!(rates[1], 0.0); // 1 request, 0 response
        assert_eq!(rates[2], 1.0); // no requests -> 1.0
    }
}
