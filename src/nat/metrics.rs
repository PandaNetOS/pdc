//! NAT 增强指标统计
//!
//! 提供完整的 NAT 穿透指标统计，包括：
//! - 映射成功率/延迟分布（P50/P95/P99）
//! - 各协议统计
//! - 状态转换统计
//! - 公网可达性统计
//! - 续租统计
//! - 网关健康统计

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::provider::MappingOperationStats;
use super::NatState;

// ---------------------------------------------------------------------------
// 延迟统计（滑动窗口）
// ---------------------------------------------------------------------------

/// 延迟统计（滑动窗口，保留最近 N 个样本）
#[derive(Debug, Clone)]
pub struct LatencyStats {
    samples: Vec<u64>,
    max_samples: usize,
    sum: u64,
}

impl LatencyStats {
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: Vec::with_capacity(max_samples),
            max_samples,
            sum: 0,
        }
    }

    /// 记录一个延迟样本（毫秒）
    pub fn record(&mut self, latency_ms: u64) {
        if self.samples.len() >= self.max_samples {
            if let Some(oldest) = self.samples.first() {
                self.sum = self.sum.saturating_sub(*oldest);
            }
            self.samples.remove(0);
        }
        self.samples.push(latency_ms);
        self.sum += latency_ms;
    }

    /// 平均延迟（毫秒）
    pub fn avg(&self) -> f64 {
        if self.samples.is_empty() {
            0.0
        } else {
            self.sum as f64 / self.samples.len() as f64
        }
    }

    /// P50 延迟（毫秒）
    pub fn p50(&self) -> u64 {
        self.percentile(50)
    }

    /// P95 延迟（毫秒）
    pub fn p95(&self) -> u64 {
        self.percentile(95)
    }

    /// P99 延迟（毫秒）
    pub fn p99(&self) -> u64 {
        self.percentile(99)
    }

    /// 计算百分位延迟
    fn percentile(&self, p: u8) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let idx = ((p as f64 / 100.0) * sorted.len() as f64) as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    /// 样本数
    pub fn count(&self) -> usize {
        self.samples.len()
    }

    /// 最小延迟
    pub fn min(&self) -> u64 {
        self.samples.iter().copied().min().unwrap_or(0)
    }

    /// 最大延迟
    pub fn max(&self) -> u64 {
        self.samples.iter().copied().max().unwrap_or(0)
    }
}

impl Default for LatencyStats {
    fn default() -> Self {
        Self::new(1000)
    }
}

// ---------------------------------------------------------------------------
// 协议统计
// ---------------------------------------------------------------------------

/// 单个协议的统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProtocolStats {
    /// 映射成功次数
    pub mapping_success: u64,
    /// 映射失败次数
    pub mapping_failure: u64,
    /// 续租成功次数
    pub renew_success: u64,
    /// 续租失败次数
    pub renew_failure: u64,
    /// 网关发现成功次数
    pub discover_success: u64,
    /// 网关发现失败次数
    pub discover_failure: u64,
    /// 平均映射延迟（毫秒）
    pub avg_mapping_latency_ms: f64,
}

impl ProtocolStats {
    /// 映射成功率（0-100）
    pub fn success_rate(&self) -> f64 {
        let total = self.mapping_success + self.mapping_failure;
        if total == 0 {
            0.0
        } else {
            self.mapping_success as f64 / total as f64 * 100.0
        }
    }
}

// ---------------------------------------------------------------------------
// 增强指标
// ---------------------------------------------------------------------------

/// NAT 增强指标统计
pub struct NatMetricsExt {
    /// 各协议统计
    protocol_stats: Arc<RwLock<HashMap<String, ProtocolStats>>>,
    /// 映射延迟统计（全局）
    mapping_latency: Arc<RwLock<LatencyStats>>,
    /// 续租延迟统计
    renew_latency: Arc<RwLock<LatencyStats>>,
    /// 网关发现延迟统计
    discover_latency: Arc<RwLock<LatencyStats>>,
    /// 最近的映射操作记录
    recent_operations: Arc<RwLock<Vec<MappingOperationStats>>>,
    /// 状态停留时间统计
    state_durations: Arc<RwLock<HashMap<String, u64>>>,
    /// 公网可达性检测次数
    reachability_checks: Arc<RwLock<u64>>,
    /// 公网可达性成功次数
    reachability_success: Arc<RwLock<u64>>,
    /// 启动时间
    start_time: Instant,
}

impl NatMetricsExt {
    /// 创建增强指标
    pub fn new() -> Self {
        Self {
            protocol_stats: Arc::new(RwLock::new(HashMap::new())),
            mapping_latency: Arc::new(RwLock::new(LatencyStats::new(1000))),
            renew_latency: Arc::new(RwLock::new(LatencyStats::new(1000))),
            discover_latency: Arc::new(RwLock::new(LatencyStats::new(1000))),
            recent_operations: Arc::new(RwLock::new(Vec::with_capacity(100))),
            state_durations: Arc::new(RwLock::new(HashMap::new())),
            reachability_checks: Arc::new(RwLock::new(0)),
            reachability_success: Arc::new(RwLock::new(0)),
            start_time: Instant::now(),
        }
    }

    /// 记录映射操作
    pub fn record_mapping(&self, stats: MappingOperationStats) {
        // 更新协议统计
        let mut proto_stats = self.protocol_stats.write();
        let entry = proto_stats
            .entry(stats.protocol.clone())
            .or_insert_with(ProtocolStats::default);
        if stats.success {
            entry.mapping_success += 1;
        } else {
            entry.mapping_failure += 1;
        }
        // 更新平均延迟（简单滑动平均）
        entry.avg_mapping_latency_ms =
            (entry.avg_mapping_latency_ms * (entry.mapping_success + entry.mapping_failure - 1) as f64
                + stats.duration_ms as f64)
                / (entry.mapping_success + entry.mapping_failure) as f64;

        // 更新全局延迟统计
        self.mapping_latency.write().record(stats.duration_ms);

        // 记录最近操作
        let mut ops = self.recent_operations.write();
        ops.push(stats);
        if ops.len() > 100 {
            ops.remove(0);
        }
    }

    /// 记录续租操作
    pub fn record_renew(&self, protocol: &str, success: bool, latency_ms: u64) {
        let mut proto_stats = self.protocol_stats.write();
        let entry = proto_stats
            .entry(protocol.to_string())
            .or_insert_with(ProtocolStats::default);
        if success {
            entry.renew_success += 1;
        } else {
            entry.renew_failure += 1;
        }
        self.renew_latency.write().record(latency_ms);
    }

    /// 记录网关发现操作
    pub fn record_discover(&self, protocol: &str, success: bool, latency_ms: u64) {
        let mut proto_stats = self.protocol_stats.write();
        let entry = proto_stats
            .entry(protocol.to_string())
            .or_insert_with(ProtocolStats::default);
        if success {
            entry.discover_success += 1;
        } else {
            entry.discover_failure += 1;
        }
        self.discover_latency.write().record(latency_ms);
    }

    /// 记录公网可达性检测
    pub fn record_reachability(&self, success: bool) {
        *self.reachability_checks.write() += 1;
        if success {
            *self.reachability_success.write() += 1;
        }
    }

    /// 记录状态停留时间
    pub fn record_state_duration(&self, state: NatState, duration_ms: u64) {
        let mut durations = self.state_durations.write();
        *durations.entry(state.as_str().to_string()).or_insert(0) += duration_ms;
    }

    /// 获取映射延迟统计
    pub fn mapping_latency(&self) -> LatencyStats {
        self.mapping_latency.read().clone()
    }

    /// 获取续租延迟统计
    pub fn renew_latency(&self) -> LatencyStats {
        self.renew_latency.read().clone()
    }

    /// 获取网关发现延迟统计
    pub fn discover_latency(&self) -> LatencyStats {
        self.discover_latency.read().clone()
    }

    /// 获取各协议统计
    pub fn protocol_stats(&self) -> HashMap<String, ProtocolStats> {
        self.protocol_stats.read().clone()
    }

    /// 获取最近的映射操作
    pub fn recent_operations(&self, n: usize) -> Vec<MappingOperationStats> {
        let ops = self.recent_operations.read();
        ops.iter().rev().take(n).cloned().collect()
    }

    /// 获取公网可达性统计
    pub fn reachability_stats(&self) -> (u64, u64, f64) {
        let checks = *self.reachability_checks.read();
        let success = *self.reachability_success.read();
        let rate = if checks == 0 {
            0.0
        } else {
            success as f64 / checks as f64 * 100.0
        };
        (checks, success, rate)
    }

    /// 获取运行时间
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// 获取汇总统计（用于监控显示）
    pub fn summary(&self) -> NatMetricsSummary {
        let mapping = self.mapping_latency();
        let renew = self.renew_latency();
        let discover = self.discover_latency();
        let (reach_checks, reach_success, reach_rate) = self.reachability_stats();
        let proto_stats = self.protocol_stats();

        let total_mapping_success: u64 = proto_stats.values().map(|s| s.mapping_success).sum();
        let total_mapping_failure: u64 = proto_stats.values().map(|s| s.mapping_failure).sum();
        let total_success_rate = if total_mapping_success + total_mapping_failure == 0 {
            0.0
        } else {
            total_mapping_success as f64 / (total_mapping_success + total_mapping_failure) as f64 * 100.0
        };

        NatMetricsSummary {
            total_mapping_success,
            total_mapping_failure,
            total_success_rate,
            mapping_latency_p50: mapping.p50(),
            mapping_latency_p95: mapping.p95(),
            mapping_latency_p99: mapping.p99(),
            mapping_latency_avg: mapping.avg(),
            renew_success: proto_stats.values().map(|s| s.renew_success).sum(),
            renew_failure: proto_stats.values().map(|s| s.renew_failure).sum(),
            discover_success: proto_stats.values().map(|s| s.discover_success).sum(),
            discover_failure: proto_stats.values().map(|s| s.discover_failure).sum(),
            reachability_checks: reach_checks,
            reachability_success: reach_success,
            reachability_rate: reach_rate,
            uptime_seconds: self.uptime().as_secs(),
            protocol_count: proto_stats.len(),
        }
    }
}

impl Default for NatMetricsExt {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for NatMetricsExt {
    fn clone(&self) -> Self {
        Self {
            protocol_stats: self.protocol_stats.clone(),
            mapping_latency: self.mapping_latency.clone(),
            renew_latency: self.renew_latency.clone(),
            discover_latency: self.discover_latency.clone(),
            recent_operations: self.recent_operations.clone(),
            state_durations: self.state_durations.clone(),
            reachability_checks: self.reachability_checks.clone(),
            reachability_success: self.reachability_success.clone(),
            start_time: self.start_time,
        }
    }
}

// ---------------------------------------------------------------------------
// 汇总统计（用于监控显示）
// ---------------------------------------------------------------------------

/// NAT 指标汇总（用于监控显示和序列化）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NatMetricsSummary {
    /// 总映射成功次数
    pub total_mapping_success: u64,
    /// 总映射失败次数
    pub total_mapping_failure: u64,
    /// 总映射成功率（0-100）
    pub total_success_rate: f64,
    /// 映射延迟 P50（毫秒）
    pub mapping_latency_p50: u64,
    /// 映射延迟 P95（毫秒）
    pub mapping_latency_p95: u64,
    /// 映射延迟 P99（毫秒）
    pub mapping_latency_p99: u64,
    /// 映射延迟平均值（毫秒）
    pub mapping_latency_avg: f64,
    /// 续租成功次数
    pub renew_success: u64,
    /// 续租失败次数
    pub renew_failure: u64,
    /// 网关发现成功次数
    pub discover_success: u64,
    /// 网关发现失败次数
    pub discover_failure: u64,
    /// 公网可达性检测次数
    pub reachability_checks: u64,
    /// 公网可达性成功次数
    pub reachability_success: u64,
    /// 公网可达性成功率（0-100）
    pub reachability_rate: f64,
    /// 运行时间（秒）
    pub uptime_seconds: u64,
    /// 已使用的协议数
    pub protocol_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_stats() {
        let mut stats = LatencyStats::new(10);
        stats.record(10);
        stats.record(20);
        stats.record(30);
        assert_eq!(stats.count(), 3);
        assert_eq!(stats.avg(), 20.0);
        assert_eq!(stats.p50(), 20);
        assert_eq!(stats.min(), 10);
        assert_eq!(stats.max(), 30);
    }

    #[test]
    fn test_latency_stats_overflow() {
        let mut stats = LatencyStats::new(3);
        stats.record(10);
        stats.record(20);
        stats.record(30);
        stats.record(40); // 应该移除 10
        assert_eq!(stats.count(), 3);
        assert_eq!(stats.min(), 20);
    }

    #[test]
    fn test_protocol_stats() {
        let mut stats = ProtocolStats::default();
        stats.mapping_success = 90;
        stats.mapping_failure = 10;
        assert!((stats.success_rate() - 90.0).abs() < 0.01);
    }

    #[test]
    fn test_metrics_ext() {
        let metrics = NatMetricsExt::new();
        let op = MappingOperationStats {
            protocol: "upnp".to_string(),
            internal_port: 6880,
            external_port: 6880,
            success: true,
            duration_ms: 50,
            retries: 0,
            error: None,
            timestamp: 0,
        };
        metrics.record_mapping(op);
        let summary = metrics.summary();
        assert_eq!(summary.total_mapping_success, 1);
        assert_eq!(summary.protocol_count, 1);
        assert!(summary.mapping_latency_avg > 0.0);
    }

    #[test]
    fn test_reachability_stats() {
        let metrics = NatMetricsExt::new();
        metrics.record_reachability(true);
        metrics.record_reachability(true);
        metrics.record_reachability(false);
        let (checks, success, rate) = metrics.reachability_stats();
        assert_eq!(checks, 3);
        assert_eq!(success, 2);
        assert!((rate - 66.67).abs() < 0.1);
    }
}
