//! 对端响应率自适应限速模块
//!
//! 每个 socket 独立统计滑动窗口内的发送/响应比，响应率过低时自动进入限速状态。
//! 限速时不再完全跳过发送，而是按 `throttle_skip_ratio` 比例降频跳过。
//! 响应率 < enter_threshold（默认 15%）进入限速，恢复到 > exit_threshold（默认 30%）解除。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// 默认滑动窗口大小（秒）
const DEFAULT_WINDOW_SECS: u64 = 60;

/// 默认进入限速阈值（响应率低于此值进入限速）
const DEFAULT_ENTER_THRESHOLD: f64 = 0.15;
/// 默认解除限速阈值（响应率高于此值解除限速）
const DEFAULT_EXIT_THRESHOLD: f64 = 0.30;
/// 默认最小请求样本数（样本不足时不做限速判断）
const DEFAULT_MIN_SAMPLES: usize = 50;
/// 限速时跳过比例（0.5 表示每 2 次发送跳过 1 次）
const DEFAULT_THROTTLE_SKIP_RATIO: f64 = 0.5;

/// 单个 socket 的响应率统计（滑动窗口，最近60秒）
pub struct SocketRateStats {
    /// 发送记录（时间戳）
    requests: VecDeque<Instant>,
    /// 接收响应记录（时间戳）
    responses: VecDeque<Instant>,
    /// 窗口大小
    window: Duration,
    /// 是否被限速（进入限速状态）
    throttled: bool,
    /// 进入限速阈值（响应率低于此值进入限速）
    enter_threshold: f64,
    /// 解除限速阈值（响应率高于此值解除限速）
    exit_threshold: f64,
    /// 限速判断所需最小请求样本数
    min_samples: usize,
}

impl SocketRateStats {
    pub fn new(window: Duration) -> Self {
        Self::with_config(
            window,
            DEFAULT_ENTER_THRESHOLD,
            DEFAULT_EXIT_THRESHOLD,
            DEFAULT_MIN_SAMPLES,
        )
    }

    /// 带阈值配置的构造函数
    pub fn with_config(
        window: Duration,
        enter_threshold: f64,
        exit_threshold: f64,
        min_samples: usize,
    ) -> Self {
        Self {
            requests: VecDeque::new(),
            responses: VecDeque::new(),
            window,
            throttled: false,
            enter_threshold,
            exit_threshold,
            min_samples,
        }
    }

    /// 运行时更新限速阈值（保持向后兼容，无需重建对象）
    pub fn set_thresholds(&mut self, enter: f64, exit: f64, min_samples: usize) {
        self.enter_threshold = enter;
        self.exit_threshold = exit;
        self.min_samples = min_samples;
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

    /// 更新并返回当前是否处于限速状态。
    ///
    /// 注意：此方法只负责维护 `throttled` 状态机（基于配置阈值），
    /// 不再决定是否真正跳过发送。真正的"按比例跳过"逻辑由
    /// [`RateLimiter::should_skip`] 在外层根据 `throttle_skip_ratio` 完成。
    ///
    /// 响应率 < enter_threshold 且样本数 >= min_samples 时进入限速；
    /// 限速中响应率 > exit_threshold 时解除限速。
    pub fn should_skip(&mut self) -> bool {
        let rate = self.response_rate();
        if self.throttled {
            if rate > self.exit_threshold {
                self.throttled = false;
            }
        } else if rate < self.enter_threshold && self.requests.len() >= self.min_samples {
            // 样本数足够才判断，避免冷启动误判
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
    /// 进入限速阈值（响应率低于此值进入限速）
    enter_threshold: f64,
    /// 解除限速阈值（响应率高于此值解除限速）
    exit_threshold: f64,
    /// 限速判断所需最小请求样本数
    min_samples: usize,
    /// 限速时跳过比例（0.5 表示每 2 次发送跳过 1 次）
    throttle_skip_ratio: f64,
    /// 每个 socket 的降频跳过计数器（与 socket 数量等长）
    throttle_skip_counter: Vec<AtomicUsize>,
}

impl RateLimiter {
    /// 使用默认限速阈值构造。保持与历史调用方兼容。
    pub fn new(socket_count: usize, enabled: bool, window: Duration) -> Self {
        Self::with_config(
            socket_count,
            enabled,
            window,
            DEFAULT_ENTER_THRESHOLD,
            DEFAULT_EXIT_THRESHOLD,
            DEFAULT_MIN_SAMPLES,
            DEFAULT_THROTTLE_SKIP_RATIO,
        )
    }

    /// 显式传入全部限速阈值构造。
    pub fn with_config(
        socket_count: usize,
        enabled: bool,
        window: Duration,
        enter_threshold: f64,
        exit_threshold: f64,
        min_samples: usize,
        throttle_skip_ratio: f64,
    ) -> Self {
        Self {
            stats: (0..socket_count)
                .map(|_| {
                    parking_lot::Mutex::new(SocketRateStats::with_config(
                        window,
                        enter_threshold,
                        exit_threshold,
                        min_samples,
                    ))
                })
                .collect(),
            enabled,
            enter_threshold,
            exit_threshold,
            min_samples,
            throttle_skip_ratio,
            // 每个 socket 一个计数器，初始化为 0
            throttle_skip_counter: (0..socket_count).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    /// 运行时热更新限速配置（阈值/样本数/跳过比例）。
    ///
    /// 会同时把新阈值同步下发给每个 socket 的统计对象，
    /// `throttle_skip_ratio` 直接作用于后续 `should_skip` 调用。
    pub fn update_config(
        &mut self,
        enter_threshold: f64,
        exit_threshold: f64,
        min_samples: usize,
        throttle_skip_ratio: f64,
    ) {
        self.enter_threshold = enter_threshold;
        self.exit_threshold = exit_threshold;
        self.min_samples = min_samples;
        self.throttle_skip_ratio = throttle_skip_ratio;
        for s in &self.stats {
            s.lock()
                .set_thresholds(enter_threshold, exit_threshold, min_samples);
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

    /// 检查指定 socket 是否应该跳过发送。
    ///
    /// 核心逻辑：
    /// 1. 先更新该 socket 的限速状态机（基于 enter/exit_threshold 与 min_samples）；
    /// 2. 若未处于限速状态，返回 `false`（正常发送，不跳过）；
    /// 3. 若处于限速状态，按 `throttle_skip_ratio` 比例降频跳过：
    ///    使用原子计数器累加，每 `round(1/ratio)` 次发送跳过 1 次。
    ///    ratio=0.5 时即 `counter % 2 == 0` 时跳过（约 50%）。
    pub fn should_skip(&self, socket_idx: usize) -> bool {
        if !self.enabled {
            return false;
        }
        let Some(s) = self.stats.get(socket_idx) else {
            return false;
        };
        // 1. 更新限速状态，得到是否处于限速状态
        let throttled = s.lock().should_skip();
        // 2. 未限速：正常发送，不跳过
        if !throttled {
            return false;
        }
        // 3. 限速状态：按比例降频跳过
        let Some(counter) = self.throttle_skip_counter.get(socket_idx) else {
            return true;
        };
        let c = counter.fetch_add(1, Ordering::Relaxed);
        // 跳过周期 = round(1 / ratio)。ratio=0.5 → 2；ratio=0.25 → 4
        let period = (1.0 / self.throttle_skip_ratio).round() as usize;
        if period == 0 {
            return true;
        }
        c % period == 0
    }

    /// 计算所有 socket 的综合响应率（总响应数 / 总请求数），用于监控展示。
    ///
    /// 总请求数为 0 时返回 0.0。
    pub fn global_response_rate(&self) -> f64 {
        let mut total_req = 0usize;
        let mut total_resp = 0usize;
        for s in &self.stats {
            let g = s.lock();
            total_req += g.request_count();
            total_resp += g.response_count();
        }
        if total_req == 0 {
            0.0
        } else {
            total_resp as f64 / total_req as f64
        }
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
        // 发送100次，只收到10次响应（10% < 15% 进入阈值），且样本数 100 >= 50
        for _ in 0..100 {
            stats.record_request();
        }
        for _ in 0..10 {
            stats.record_response();
        }
        assert!(stats.should_skip());
        assert!(stats.is_throttled());
    }

    #[test]
    fn test_no_throttle_before_sample_threshold() {
        let mut stats = SocketRateStats::new(TEST_WINDOW);
        // 发送3次，0响应 — 样本数 < 50，不应限速
        for _ in 0..3 {
            stats.record_request();
        }
        assert!(!stats.should_skip());
        assert!(!stats.is_throttled());
    }

    #[test]
    fn test_exit_threshold_releases_throttle() {
        let mut stats = SocketRateStats::new(TEST_WINDOW);
        // 先用低响应率进入限速：100 请求 10 响应 = 10% < 15%
        for _ in 0..100 {
            stats.record_request();
        }
        for _ in 0..10 {
            stats.record_response();
        }
        assert!(stats.should_skip());
        assert!(stats.is_throttled());
        // 模拟窗口清理后高响应率（> 30%）解除限速：
        // 直接重置为新窗口：100 请求 40 响应 = 40% > 30%
        stats.requests.clear();
        stats.responses.clear();
        for _ in 0..100 {
            stats.record_request();
        }
        for _ in 0..40 {
            stats.record_response();
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

    #[test]
    fn test_throttle_skips_by_ratio_half() {
        let limiter = RateLimiter::new(1, true, TEST_WINDOW);
        // 制造限速状态：100 请求 5 响应 = 5% < 15%，样本数足够
        for _ in 0..100 {
            limiter.record_request(0);
        }
        for _ in 0..5 {
            limiter.record_response(0);
        }
        // 前 20 次 should_skip 调用：counter 从 0 递增，
        // ratio=0.5 → period=2，偶数次跳过（0,2,4,...,18）共 10 次
        let mut skip_count = 0;
        for _ in 0..20 {
            if limiter.should_skip(0) {
                skip_count += 1;
            }
        }
        assert_eq!(skip_count, 10);
        assert!(limiter.throttled_sockets()[0]);
    }

    #[test]
    fn test_no_skip_when_not_throttled() {
        let limiter = RateLimiter::new(1, true, TEST_WINDOW);
        // 正常高响应率：100 请求 80 响应 = 80% > 30%
        for _ in 0..100 {
            limiter.record_request(0);
        }
        for _ in 0..80 {
            limiter.record_response(0);
        }
        // 不应跳过
        for _ in 0..10 {
            assert!(!limiter.should_skip(0));
        }
    }

    #[test]
    fn test_global_response_rate() {
        let limiter = RateLimiter::new(2, true, TEST_WINDOW);
        for _ in 0..10 {
            limiter.record_request(0);
        }
        for _ in 0..4 {
            limiter.record_response(0);
        }
        for _ in 0..10 {
            limiter.record_request(1);
        }
        for _ in 0..6 {
            limiter.record_response(1);
        }
        // 总 20 请求，10 响应 → 0.5
        assert!((limiter.global_response_rate() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_global_response_rate_no_requests() {
        let limiter = RateLimiter::new(1, true, TEST_WINDOW);
        assert_eq!(limiter.global_response_rate(), 0.0);
    }

    #[test]
    fn test_update_config_changes_thresholds() {
        let mut limiter = RateLimiter::new(1, true, TEST_WINDOW);
        // 用更严格的进入阈值 0.5（50%）：100 请求 40 响应 = 40% < 50% → 进入限速
        limiter.update_config(0.5, 0.8, 50, 0.5);
        for _ in 0..100 {
            limiter.record_request(0);
        }
        for _ in 0..40 {
            limiter.record_response(0);
        }
        assert!(limiter.should_skip(0));
        assert!(limiter.throttled_sockets()[0]);
    }
}
