//! 限流与 QPS 监控模块
//!
//! P2 优化：超级 Tracker 高并发场景下的限流与监控。
//! - QPS 实时统计（滑动窗口）
//! - 单 IP 限流（令牌桶）
//! - 异常 IP 临时封禁

use std::collections::VecDeque;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rustc_hash::FxHashMap;

/// QPS 统计（滑动窗口，1 秒精度）
#[derive(Debug, Clone)]
pub struct QpsStats {
    pub total_requests: u64,
    pub requests_last_sec: u64,
    pub connect_requests: u64,
    pub announce_requests: u64,
    pub scrape_requests: u64,
    pub error_requests: u64,
    pub blocked_requests: u64,
}

impl Default for QpsStats {
    fn default() -> Self {
        Self {
            total_requests: 0,
            requests_last_sec: 0,
            connect_requests: 0,
            announce_requests: 0,
            scrape_requests: 0,
            error_requests: 0,
            blocked_requests: 0,
        }
    }
}

/// 请求类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestType {
    Connect,
    Announce,
    Scrape,
    Error,
}

/// IP 限流状态
#[derive(Debug, Clone)]
struct IpRateState {
    /// 令牌桶剩余令牌
    tokens: f64,
    /// 上次补充令牌时间
    last_refill: Instant,
    /// 最近请求时间（用于异常检测）
    recent_requests: VecDeque<Instant>,
    /// 封禁截止时间
    banned_until: Option<Instant>,
}

impl IpRateState {
    fn new() -> Self {
        Self {
            tokens: 100.0, // 初始 100 令牌
            last_refill: Instant::now(),
            recent_requests: VecDeque::with_capacity(64),
            banned_until: None,
        }
    }
}

/// 限流器
pub struct RateLimiter {
    /// 每秒最大请求数（单 IP）
    max_per_sec: f64,
    /// 突发令牌数
    burst: f64,
    /// IP 限流状态
    ip_states: Mutex<FxHashMap<IpAddr, IpRateState>>,
    /// QPS 统计
    stats: Mutex<QpsStats>,
    /// 每秒请求计数（用于滑动窗口）
    sec_window: Mutex<VecDeque<(Instant, u64)>>,
    /// 封禁时长
    ban_duration: Duration,
    /// 异常阈值（1 秒内超过此请求数视为异常）
    anomaly_threshold: u64,
}

impl RateLimiter {
    /// 创建限流器
    pub fn new(max_per_sec: f64, burst: f64) -> Self {
        Self {
            max_per_sec,
            burst,
            ip_states: Mutex::new(FxHashMap::default()),
            stats: Mutex::new(QpsStats::default()),
            sec_window: Mutex::new(VecDeque::new()),
            ban_duration: Duration::from_secs(60), // 封禁 60 秒
            anomaly_threshold: 500, // 1 秒 500 请求视为异常
        }
    }

    /// 检查是否允许请求（令牌桶限流 + 封禁检查）
    pub fn check_and_record(&self, ip: IpAddr, req_type: RequestType) -> bool {
        let now = Instant::now();

        // 更新 QPS 统计
        {
            let mut stats = self.stats.lock();
            stats.total_requests += 1;
            match req_type {
                RequestType::Connect => stats.connect_requests += 1,
                RequestType::Announce => stats.announce_requests += 1,
                RequestType::Scrape => stats.scrape_requests += 1,
                RequestType::Error => stats.error_requests += 1,
            }
        }

        // 更新滑动窗口
        {
            let mut window = self.sec_window.lock();
            window.push_back((now, 1));
            // 清理超过 1 秒的记录
            while window.front().map_or(false, |(t, _)| now.duration_since(*t) > Duration::from_secs(1)) {
                window.pop_front();
            }
        }

        // IP 限流检查
        let mut states = self.ip_states.lock();
        let state = states.entry(ip).or_insert_with(IpRateState::new);

        // 检查是否被封禁
        if let Some(banned_until) = state.banned_until {
            if now < banned_until {
                let mut stats = self.stats.lock();
                stats.blocked_requests += 1;
                return false; // 被封禁
            } else {
                state.banned_until = None; // 封禁到期
            }
        }

        // 令牌桶补充
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        state.tokens = (state.tokens + elapsed * self.max_per_sec).min(self.burst);
        state.last_refill = now;

        // 记录请求时间（用于异常检测）
        state.recent_requests.push_back(now);
        while state.recent_requests.front().map_or(false, |t| now.duration_since(*t) > Duration::from_secs(1)) {
            state.recent_requests.pop_front();
        }

        // 异常检测：1 秒内请求数超过阈值，临时封禁
        if state.recent_requests.len() as u64 >= self.anomaly_threshold {
            state.banned_until = Some(now + self.ban_duration);
            state.recent_requests.clear();
            tracing::warn!("[rate_limiter] IP {} 异常流量，临时封禁 60 秒", ip);
            let mut stats = self.stats.lock();
            stats.blocked_requests += 1;
            return false;
        }

        // 令牌桶检查
        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            true
        } else {
            let mut stats = self.stats.lock();
            stats.blocked_requests += 1;
            false
        }
    }

    /// 获取 QPS 统计
    pub fn stats(&self) -> QpsStats {
        let mut stats = self.stats.lock().clone();
        // 计算最近 1 秒请求数
        let window = self.sec_window.lock();
        stats.requests_last_sec = window.iter().map(|(_, c)| c).sum();
        stats
    }

    /// 获取当前 QPS
    pub fn current_qps(&self) -> u64 {
        let window = self.sec_window.lock();
        window.iter().map(|(_, c)| c).sum()
    }

    /// 记录错误请求（可能触发封禁）
    pub fn record_error(&self, ip: IpAddr) {
        let mut states = self.ip_states.lock();
        let state = states.entry(ip).or_insert_with(IpRateState::new);
        // 错误请求也计入 recent_requests，连续错误可能触发异常封禁
        state.recent_requests.push_back(Instant::now());
    }

    /// 获取被封禁的 IP 数量
    pub fn banned_count(&self) -> usize {
        let now = Instant::now();
        let states = self.ip_states.lock();
        states.values().filter(|s| s.banned_until.map_or(false, |t| now < t)).count()
    }
}

/// 创建默认限流器（超级 Tracker 用：单 IP 100 QPS，突发 200）
pub fn default_tracker_limiter() -> Arc<RateLimiter> {
    Arc::new(RateLimiter::new(100.0, 200.0))
}
