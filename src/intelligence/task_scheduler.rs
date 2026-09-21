//! 智能任务调度中心（TaskScheduler）
//!
//! 统一管理所有定时任务，解决多全量任务集中爆发导致的周期性资源占用峰值问题。
//!
//! 核心功能：
//! - 任务统一注册与元数据管理
//! - 优先级队列（P0关键 > P1重要 > P2普通 > P3后台）
//! - 令牌桶限流（同时执行的全量任务≤2个）
//! - 错峰调度（同周期任务分散到时间窗口）
//! - 随机抖动（±10s 避免规律性冲突）
//! - 资源感知调度（CPU>80%延迟非关键任务）
//! - 依赖管理（按依赖顺序执行）
//! - 执行监控（耗时统计、超时检测、失败重试）
//! - 自适应调度（基于历史数据动态调整）

use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex as ParkingMutex, RwLock};
use tracing::{debug, error, info, warn};

use futures::FutureExt;

use crate::intelligence::adaptive_controller::AdaptiveController;

// ---------------------------------------------------------------------------
// 任务优先级
// ---------------------------------------------------------------------------

/// 任务优先级（4级）
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TaskPriority {
    /// P0 关键（不可延迟）：健康检查、超级Tracker响应、DHT消息处理
    Critical = 0,
    /// P1 重要（可短延迟≤30s）：增量评分、DHT爬虫主动任务、Peer快照
    Important = 1,
    /// P2 普通（可长延迟≤300s）：全量评分、统计输出、bucket刷新、缓存清理
    Normal = 2,
    /// P3 后台（可暂停）：TierManager归档、持久化、WAL checkpoint、订阅导入
    Background = 3,
}

/// 各优先级对应的最大可延迟时间（协议级调度常量）
const CRITICAL_MAX_DELAY: Duration = Duration::from_secs(0);
const IMPORTANT_MAX_DELAY: Duration = Duration::from_secs(30);
const NORMAL_MAX_DELAY: Duration = Duration::from_secs(300);
const BACKGROUND_MAX_DELAY: Duration = Duration::from_secs(600);

/// 调度器内核节奏常量
const SCHEDULER_TICK_INTERVAL: Duration = Duration::from_millis(100);
const SCHEDULER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const DEPENDENCY_RETRY_DELAY: Duration = Duration::from_secs(30);
const CRITICAL_RESOURCE_DELAY: Duration = Duration::from_secs(15);
const STRESSED_FULL_TASK_DELAY: Duration = Duration::from_secs(10);
const CONCURRENCY_FULL_DELAY: Duration = Duration::from_secs(5);

/// Watchdog 检测间隔（秒）：独立 OS 线程定期检查心跳
const WATCHDOG_CHECK_INTERVAL_SECS: u64 = 30;
/// Watchdog 心跳超时阈值（秒）：超过此时长判定为线程饥饿
const WATCHDOG_HEARTBEAT_TIMEOUT_SECS: i64 = 60;

impl TaskPriority {
    pub fn max_delay(&self) -> Duration {
        match self {
            TaskPriority::Critical => CRITICAL_MAX_DELAY,
            TaskPriority::Important => IMPORTANT_MAX_DELAY,
            TaskPriority::Normal => NORMAL_MAX_DELAY,
            TaskPriority::Background => BACKGROUND_MAX_DELAY,
        }
    }
}

// ---------------------------------------------------------------------------
// 任务分类（分级并发控制）
// ---------------------------------------------------------------------------

/// 任务分类（用于分级并发控制）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TaskCategory {
    /// 爬虫类（主动爬行、get_peers、sample 等）
    Crawl,
    /// 持久化类（增量保存、checkpoint 等）
    Persistence,
    /// 监控类（资源监控、状态采集、健康检查等）
    #[default]
    Monitor,
    /// 网络类（发现器、NAT 等）
    Network,
    /// 联邦类（联邦同步、shard-sync、merkle 计算）
    Federation,
    /// Tracker 类（超级 Tracker 后台任务）
    Tracker,
}

/// 各分类并发度配置
#[derive(Debug, Clone, Copy)]
pub struct CategoryConcurrency {
    pub crawl: u32,
    pub persistence: u32,
    pub monitor: u32,
    pub network: u32,
    pub federation: u32,
    pub tracker: u32,
}

impl Default for CategoryConcurrency {
    fn default() -> Self {
        Self {
            crawl: 4,
            persistence: 1,
            monitor: 2,
            network: 4,
            // v9: 联邦专用并发 4→8（20+ fed_* 周期任务 + 慢 IO 任务如 delta 拉取/bootstrap，
            // 4 槽在欠账大时会被慢任务占满导致联邦内部饿死；federation runtime 本身 ≥4 线程）
            federation: 8,
            tracker: 2,
        }
    }
}

impl CategoryConcurrency {
    /// 查询指定分类的最大并发度
    pub fn max_for(&self, cat: TaskCategory) -> u32 {
        match cat {
            TaskCategory::Crawl => self.crawl,
            TaskCategory::Persistence => self.persistence,
            TaskCategory::Monitor => self.monitor,
            TaskCategory::Network => self.network,
            TaskCategory::Federation => self.federation,
            TaskCategory::Tracker => self.tracker,
        }
    }
}

// ---------------------------------------------------------------------------
// 资源消耗等级
// ---------------------------------------------------------------------------

/// 资源消耗等级
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceLevel {
    Low,
    Medium,
    High,
    Extreme,
}

/// 任务资源消耗画像
#[derive(Debug, Clone)]
pub struct ResourceProfile {
    pub cpu: ResourceLevel,
    pub memory: ResourceLevel,
    pub io: ResourceLevel,
    pub network: ResourceLevel,
    /// 是否为全量任务（占用令牌桶配额）
    pub is_full_task: bool,
}

impl Default for ResourceProfile {
    fn default() -> Self {
        Self {
            cpu: ResourceLevel::Medium,
            memory: ResourceLevel::Medium,
            io: ResourceLevel::Low,
            network: ResourceLevel::Low,
            is_full_task: false,
        }
    }
}

// ---------------------------------------------------------------------------
// 任务元数据
// ---------------------------------------------------------------------------

/// 任务元数据
#[derive(Debug, Clone)]
pub struct TaskMetadata {
    /// 任务唯一ID
    pub id: String,
    /// 任务名称
    pub name: String,
    /// 执行周期
    pub interval: Duration,
    /// 优先级
    pub priority: TaskPriority,
    /// 资源消耗画像
    pub resource: ResourceProfile,
    /// 是否可延迟
    pub deferrable: bool,
    /// 最大延迟时间
    pub max_delay: Duration,
    /// 依赖的任务ID列表（这些任务完成后才能执行）
    pub dependencies: Vec<String>,
    /// 初始延迟（错峰用）
    pub initial_delay: Duration,
    /// 随机抖动范围（±jitter）
    pub jitter: Duration,
    /// 预计执行时长（历史统计平均值）
    pub estimated_duration: Duration,
    /// 超时时间（超过则告警）
    pub timeout: Duration,
    /// 失败最大重试次数
    pub max_retries: u32,
    /// 任务分类（用于分级并发控制）
    pub category: TaskCategory,
    /// 是否为保活类任务（心跳/健康检查），不受资源准入限制
    pub is_keepalive: bool,
    /// 自适应闭环：当前实际执行间隔（秒）。默认等于基准 interval.as_secs()。
    /// 自适应关闭时始终等于基准间隔，行为与改造前一致。
    pub current_interval_secs: u64,
    /// 自适应闭环：当前间隔相对基准间隔的比例（1.0 = 未偏离）。
    pub adaptive_deviation: f32,
}

/// TaskMetadata::new 的默认字段值
const DEFAULT_INITIAL_DELAY: Duration = Duration::from_secs(0);
const DEFAULT_JITTER: Duration = Duration::from_secs(10);
const DEFAULT_ESTIMATED_DURATION: Duration = Duration::from_secs(5);
const DEFAULT_TASK_TIMEOUT: Duration = Duration::from_secs(300);
/// TaskMetadata::new 的默认最大重试次数
const DEFAULT_MAX_RETRIES: u32 = 3;

impl TaskMetadata {
    pub fn new(id: impl Into<String>, name: impl Into<String>, interval: Duration) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            interval,
            priority: TaskPriority::Normal,
            resource: ResourceProfile::default(),
            deferrable: true,
            max_delay: TaskPriority::Normal.max_delay(),
            dependencies: Vec::new(),
            initial_delay: DEFAULT_INITIAL_DELAY,
            jitter: DEFAULT_JITTER,
            estimated_duration: DEFAULT_ESTIMATED_DURATION,
            timeout: DEFAULT_TASK_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
            category: TaskCategory::default(),
            is_keepalive: false,
            current_interval_secs: interval.as_secs(),
            adaptive_deviation: 1.0,
        }
    }

    pub fn with_priority(mut self, p: TaskPriority) -> Self {
        self.priority = p;
        if !self.deferrable {
            self.max_delay = Duration::ZERO;
        } else {
            self.max_delay = p.max_delay();
        }
        self
    }

    pub fn with_resource(mut self, r: ResourceProfile) -> Self {
        self.resource = r;
        self
    }

    pub fn with_initial_delay(mut self, d: Duration) -> Self {
        self.initial_delay = d;
        self
    }

    pub fn with_jitter(mut self, j: Duration) -> Self {
        self.jitter = j;
        self
    }

    pub fn non_deferrable(mut self) -> Self {
        self.deferrable = false;
        self.max_delay = Duration::ZERO;
        self
    }

    pub fn with_dependencies(mut self, deps: Vec<String>) -> Self {
        self.dependencies = deps;
        self
    }

    pub fn with_category(mut self, c: TaskCategory) -> Self {
        self.category = c;
        self
    }

    /// 标记为保活类任务（心跳/健康检查），不受资源准入延迟影响
    pub fn with_keepalive(mut self) -> Self {
        self.is_keepalive = true;
        self
    }

    /// 是否参与自适应间隔闭环。
    ///
    /// 保活任务（心跳/健康检查）与亚秒级任务（gossip flush 等）不参与，
    /// 始终使用基准间隔。
    pub fn adaptive_eligible(&self) -> bool {
        !self.is_keepalive && self.interval.as_secs() >= 1
    }
}

// ---------------------------------------------------------------------------
// 任务执行统计
// ---------------------------------------------------------------------------

/// 任务执行统计
#[derive(Debug, Clone, Default)]
pub struct TaskStats {
    pub total_executions: u64,
    pub success_count: u64,
    pub failure_count: u64,
    pub total_duration_ms: u64,
    pub max_duration_ms: u64,
    pub min_duration_ms: u64,
    pub last_execution: Option<Instant>,
    pub last_duration: Option<Duration>,
    pub consecutive_failures: u32,
    /// 最近10次执行耗时（毫秒），用于滑动窗口均值
    pub recent_durations_ms: VecDeque<u64>,
}

impl TaskStats {
    pub fn avg_duration_ms(&self) -> u64 {
        self.total_duration_ms
            .checked_div(self.total_executions)
            .unwrap_or(0)
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_executions > 0 {
            self.success_count as f64 / self.total_executions as f64
        } else {
            1.0
        }
    }

    /// 最近10次执行耗时均值（毫秒）
    pub fn recent_avg_duration_ms(&self) -> u64 {
        if self.recent_durations_ms.is_empty() {
            return 0;
        }
        self.recent_durations_ms.iter().sum::<u64>() / self.recent_durations_ms.len() as u64
    }

    fn record_success(&mut self, duration: Duration) {
        self.total_executions += 1;
        self.success_count += 1;
        let ms = duration.as_millis() as u64;
        self.total_duration_ms += ms;
        self.max_duration_ms = self.max_duration_ms.max(ms);
        if self.min_duration_ms == 0 || ms < self.min_duration_ms {
            self.min_duration_ms = ms;
        }
        // 滑动窗口：保留最近10条
        self.recent_durations_ms.push_back(ms);
        if self.recent_durations_ms.len() > 10 {
            self.recent_durations_ms.pop_front();
        }
        self.last_execution = Some(Instant::now());
        self.last_duration = Some(duration);
        self.consecutive_failures = 0;
    }

    fn record_failure(&mut self) {
        self.total_executions += 1;
        self.failure_count += 1;
        self.consecutive_failures += 1;
        self.last_execution = Some(Instant::now());
    }
}

// ---------------------------------------------------------------------------
// 资源监控
// ---------------------------------------------------------------------------

/// 系统资源状态
#[derive(Debug, Clone, Copy)]
pub struct ResourceState {
    pub cpu_usage: f64,    // 0.0 - 1.0
    pub memory_usage: f64, // 0.0 - 1.0
    pub io_busy: bool,
    pub network_busy: bool,
    /// IO 负载（0.0-1.0），供资源感知准入控制使用
    pub io_usage: f64,
    pub timestamp: Instant,
}

impl Default for ResourceState {
    fn default() -> Self {
        Self {
            cpu_usage: 0.0,
            memory_usage: 0.0,
            io_busy: false,
            network_busy: false,
            io_usage: 0.0,
            timestamp: Instant::now(),
        }
    }
}

impl ResourceState {
    /// 是否资源紧张（>80%）
    pub fn is_stressed(&self) -> bool {
        self.cpu_usage > 0.8 || self.memory_usage > 0.8
    }

    /// 是否资源极度紧张（>95%）
    pub fn is_critical(&self) -> bool {
        self.cpu_usage > 0.95 || self.memory_usage > 0.95
    }
}

/// 资源监控器
pub struct ResourceMonitor {
    state: RwLock<ResourceState>,
    system: ParkingMutex<sysinfo::System>,
}

impl ResourceMonitor {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(ResourceState::default()),
            system: ParkingMutex::new(sysinfo::System::new_all()),
        }
    }

    pub fn current(&self) -> ResourceState {
        *self.state.read()
    }

    pub fn update(&self, cpu: f64, memory: f64) {
        let mut state = self.state.write();
        state.cpu_usage = cpu.clamp(0.0, 1.0);
        state.memory_usage = memory.clamp(0.0, 1.0);
        state.timestamp = Instant::now();
    }

    /// 从系统读取真实 CPU / 内存使用率并更新状态
    pub fn refresh(&self) {
        {
            let mut sys = self.system.lock();
            sys.refresh_cpu_usage();
            sys.refresh_memory();
        }
        let sys = self.system.lock();
        let cpu = sys.global_cpu_usage() as f64 / 100.0;
        let total_mem = sys.total_memory() as f64;
        let used_mem = sys.used_memory() as f64;
        let mem = if total_mem > 0.0 {
            used_mem / total_mem
        } else {
            0.0
        };
        drop(sys);
        let mut state = self.state.write();
        state.cpu_usage = cpu.clamp(0.0, 1.0);
        state.memory_usage = mem.clamp(0.0, 1.0);
        state.timestamp = Instant::now();
    }

    pub fn set_io_busy(&self, busy: bool) {
        self.state.write().io_busy = busy;
    }

    pub fn set_network_busy(&self, busy: bool) {
        self.state.write().network_busy = busy;
    }

    /// 设置 IO 负载（0.0-1.0），供准入控制判定
    pub fn set_io_load(&self, io: f64) {
        self.state.write().io_usage = io.clamp(0.0, 1.0);
    }
}

impl Default for ResourceMonitor {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// 调度器运行时旋钮（由 config.rs 传入，调度器内部不直接读 config）
// ---------------------------------------------------------------------------

/// 调度器运行时可调旋钮。
///
/// 全部带有合理默认值；所有"新功能"默认关闭，关闭后调度行为与改造前完全一致。
#[derive(Debug, Clone)]
pub struct SchedulerKnobs {
    /// 资源感知准入控制总开关（默认 false）
    pub admission_control_enabled: bool,
    /// CPU 使用率准入阈值（0.0-1.0）
    pub admission_cpu_threshold: f32,
    /// IO 负载准入阈值（0.0-1.0）
    pub admission_io_threshold: f32,
    /// 准入延迟最大 tick 数，达到后强制执行避免饥饿
    pub admission_max_delay_ticks: u32,
    /// 每轮随机错峰抖动总开关（默认 false）
    pub random_jitter_enabled: bool,
    /// 随机抖动比例（±ratio），如 0.1 = ±10%
    pub random_jitter_ratio: f32,
    /// 任务画像 EWMA 平滑系数
    pub profile_ewma_alpha: f32,
    /// 预测式调度总开关（S1-P2，默认 false）
    pub predictive_scheduling_enabled: bool,
    /// 负载预测 EWMA 平滑系数 alpha
    pub predict_ewma_alpha: f32,
    /// 负载预测历史窗口大小（采样点数）
    pub predict_history_size: usize,
    /// 预测前向 tick 数
    pub predict_lookahead_ticks: u32,
    /// 自适应执行间隔总开关（S1-P3，默认 false）
    pub adaptive_interval_enabled: bool,
    /// 自适应间隔目标负载（0.0-1.0）
    pub adaptive_target_load: f32,
    /// 自适应间隔最小缩放比例
    pub adaptive_min_ratio: f32,
    /// 自适应间隔最大缩放比例
    pub adaptive_max_ratio: f32,
    /// 闭环重算自适应间隔的 tick 周期
    pub adaptive_recalc_ticks: u32,
}

impl Default for SchedulerKnobs {
    fn default() -> Self {
        Self {
            admission_control_enabled: false,
            admission_cpu_threshold: 0.8,
            admission_io_threshold: 0.8,
            admission_max_delay_ticks: 5,
            random_jitter_enabled: false,
            random_jitter_ratio: 0.1,
            profile_ewma_alpha: 0.2,
            predictive_scheduling_enabled: false,
            predict_ewma_alpha: 0.3,
            predict_history_size: 20,
            predict_lookahead_ticks: 3,
            adaptive_interval_enabled: false,
            adaptive_target_load: 0.6,
            adaptive_min_ratio: 0.5,
            adaptive_max_ratio: 2.0,
            adaptive_recalc_ticks: 5,
        }
    }
}

impl SchedulerKnobs {
    /// 从 `config.task_scheduler` 构建运行时旋钮。
    ///
    /// `main.rs` 在装配 `TaskScheduler` 时调用（`with_knobs`），使 YAML 中
    /// 的准入控制 / 随机抖动 / 预测式调度 / 自适应间隔参数真正生效。
    /// 全部字段默认关闭时，行为与未注入旋钮（改造前）完全一致。
    pub fn from_config(cfg: &crate::config::TaskSchedulerConfig) -> Self {
        Self {
            admission_control_enabled: cfg.admission_control_enabled,
            admission_cpu_threshold: cfg.admission_cpu_threshold,
            admission_io_threshold: cfg.admission_io_threshold,
            admission_max_delay_ticks: cfg.admission_max_delay_ticks,
            random_jitter_enabled: cfg.random_jitter_enabled,
            random_jitter_ratio: cfg.random_jitter_ratio,
            profile_ewma_alpha: cfg.profile_ewma_alpha,
            predictive_scheduling_enabled: cfg.predictive_scheduling_enabled,
            predict_ewma_alpha: cfg.predict_ewma_alpha,
            predict_history_size: cfg.predict_history_size,
            predict_lookahead_ticks: cfg.predict_lookahead_ticks,
            adaptive_interval_enabled: cfg.adaptive_interval_enabled,
            adaptive_target_load: cfg.adaptive_target_load,
            adaptive_min_ratio: cfg.adaptive_min_ratio,
            adaptive_max_ratio: cfg.adaptive_max_ratio,
            adaptive_recalc_ticks: cfg.adaptive_recalc_ticks,
        }
    }
}

#[cfg(test)]
mod knobs_from_config_tests {
    use super::*;
    use crate::config::TaskSchedulerConfig;

    /// 默认配置必须映射为「全部新功能关闭」，行为与改造前一致（回归守卫）。
    #[test]
    fn test_knobs_from_default_config_is_backward_compatible() {
        let k = SchedulerKnobs::from_config(&TaskSchedulerConfig::default());
        let d = SchedulerKnobs::default();
        assert!(!k.admission_control_enabled);
        assert!(!k.random_jitter_enabled);
        assert!(!k.predictive_scheduling_enabled);
        assert!(!k.adaptive_interval_enabled);
        assert_eq!(k.admission_cpu_threshold, d.admission_cpu_threshold);
        assert_eq!(k.admission_io_threshold, d.admission_io_threshold);
        assert_eq!(k.admission_max_delay_ticks, d.admission_max_delay_ticks);
        assert_eq!(k.random_jitter_ratio, d.random_jitter_ratio);
        assert_eq!(k.profile_ewma_alpha, d.profile_ewma_alpha);
        assert_eq!(k.predict_ewma_alpha, d.predict_ewma_alpha);
        assert_eq!(k.predict_history_size, d.predict_history_size);
        assert_eq!(k.predict_lookahead_ticks, d.predict_lookahead_ticks);
        assert_eq!(k.adaptive_target_load, d.adaptive_target_load);
        assert_eq!(k.adaptive_min_ratio, d.adaptive_min_ratio);
        assert_eq!(k.adaptive_max_ratio, d.adaptive_max_ratio);
        assert_eq!(k.adaptive_recalc_ticks, d.adaptive_recalc_ticks);
    }

    /// 16 个字段逐个透传，不得有漏接或错位。
    #[test]
    fn test_knobs_from_config_field_by_field() {
        let cfg = TaskSchedulerConfig {
            admission_control_enabled: true,
            admission_cpu_threshold: 0.55,
            admission_io_threshold: 0.66,
            admission_max_delay_ticks: 7,
            random_jitter_enabled: true,
            random_jitter_ratio: 0.33,
            profile_ewma_alpha: 0.44,
            predictive_scheduling_enabled: true,
            predict_ewma_alpha: 0.22,
            predict_history_size: 42,
            predict_lookahead_ticks: 4,
            adaptive_interval_enabled: true,
            adaptive_target_load: 0.77,
            adaptive_min_ratio: 0.4,
            adaptive_max_ratio: 1.5,
            adaptive_recalc_ticks: 9,
            ..Default::default()
        };
        let k = SchedulerKnobs::from_config(&cfg);
        assert!(k.admission_control_enabled);
        assert_eq!(k.admission_cpu_threshold, 0.55);
        assert_eq!(k.admission_io_threshold, 0.66);
        assert_eq!(k.admission_max_delay_ticks, 7);
        assert!(k.random_jitter_enabled);
        assert_eq!(k.random_jitter_ratio, 0.33);
        assert_eq!(k.profile_ewma_alpha, 0.44);
        assert!(k.predictive_scheduling_enabled);
        assert_eq!(k.predict_ewma_alpha, 0.22);
        assert_eq!(k.predict_history_size, 42);
        assert_eq!(k.predict_lookahead_ticks, 4);
        assert!(k.adaptive_interval_enabled);
        assert_eq!(k.adaptive_target_load, 0.77);
        assert_eq!(k.adaptive_min_ratio, 0.4);
        assert_eq!(k.adaptive_max_ratio, 1.5);
        assert_eq!(k.adaptive_recalc_ticks, 9);
    }
}

// ---------------------------------------------------------------------------
// 负载采样（可插拔 trait + 默认 no-op 实现）
// ---------------------------------------------------------------------------

/// 采样到的瞬时系统负载
#[derive(Debug, Clone, Copy, Default)]
pub struct LoadSample {
    /// CPU 使用率 0.0-1.0
    pub cpu: f64,
    /// IO 负载 0.0-1.0
    pub io: f64,
}

/// 负载采样器抽象。默认实现直接复用 ResourceMonitor 的最近一次采样值；
/// 测试时可注入 mock 采样器以驱动准入控制逻辑。
pub trait LoadSampler: Send + Sync {
    fn sample(&self) -> LoadSample;
}

/// 基于 ResourceMonitor 的实时采样器（生产用）
pub struct ResourceMonitorSampler {
    monitor: Arc<ResourceMonitor>,
}

impl ResourceMonitorSampler {
    pub fn new(monitor: Arc<ResourceMonitor>) -> Self {
        Self { monitor }
    }
}

impl LoadSampler for ResourceMonitorSampler {
    fn sample(&self) -> LoadSample {
        let s = self.monitor.current();
        LoadSample {
            cpu: s.cpu_usage,
            io: s.io_usage,
        }
    }
}

/// 准入控制决策结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmissionDecision {
    /// 允许立即执行
    Allow,
    /// 当前负载超阈值且延迟预算未用尽，延迟一个 tick
    Delay,
    /// 延迟预算已用尽，强制执行（防饥饿）
    Force,
}

/// 纯函数：判定单个任务在给定负载下是否应被准入延迟。
///
/// - 关闭总开关 / 保活任务 / Critical 任务：始终 Allow
/// - 负载超阈值且已延迟次数 < max_delay_ticks：Delay
/// - 达到或超过 max_delay_ticks：Force
fn admission_decide(
    knobs: &SchedulerKnobs,
    meta: &TaskMetadata,
    sample: LoadSample,
    delayed_ticks: u32,
) -> AdmissionDecision {
    if !knobs.admission_control_enabled
        || meta.is_keepalive
        || meta.priority == TaskPriority::Critical
    {
        return AdmissionDecision::Allow;
    }
    let over_cpu = sample.cpu >= knobs.admission_cpu_threshold as f64;
    let over_io = sample.io >= knobs.admission_io_threshold as f64;
    if over_cpu || over_io {
        if delayed_ticks >= knobs.admission_max_delay_ticks {
            AdmissionDecision::Force
        } else {
            AdmissionDecision::Delay
        }
    } else {
        AdmissionDecision::Allow
    }
}

// ---------------------------------------------------------------------------
// S1-P2: EWMA 负载预测器
// ---------------------------------------------------------------------------

/// 负载预测状态快照（供监控查询）
#[derive(Debug, Clone, Copy, Default)]
pub struct PredictorSummary {
    /// 历史窗口当前采样点数
    pub history_len: usize,
    /// EWMA 平滑后的当前 CPU 负载
    pub cpu_ewma: f64,
    /// EWMA 平滑后的当前 IO 负载
    pub io_ewma: f64,
    /// 预测未来 1 tick 的 CPU 负载
    pub predicted_cpu_1tick: f64,
    /// 预测未来 1 tick 的 IO 负载
    pub predicted_io_1tick: f64,
}

/// EWMA 负载预测器：基于历史 LoadSample 对 CPU/IO 做趋势外推。
///
/// - 维护最近 `history_size` 个采样点（VecDeque 环形）。
/// - 对 CPU / IO 分别维护 EWMA 水平值与 EWMA 趋势增量。
/// - `predict(ticks_ahead)` = 当前 EWMA 水平 + alpha * 趋势 * ticks_ahead，clamp 到 [0,1]。
pub struct LoadPredictor {
    alpha: f64,
    history_size: usize,
    history: VecDeque<LoadSample>,
    cpu_ewma: f64,
    cpu_trend: f64,
    io_ewma: f64,
    io_trend: f64,
    initialized: bool,
}

impl LoadPredictor {
    pub fn new(alpha: f32, history_size: usize) -> Self {
        Self {
            alpha: alpha.clamp(0.0, 1.0) as f64,
            history_size: history_size.max(1),
            history: VecDeque::with_capacity(history_size.max(1)),
            cpu_ewma: 0.0,
            cpu_trend: 0.0,
            io_ewma: 0.0,
            io_trend: 0.0,
            initialized: false,
        }
    }

    /// 喂入一个新采样点，更新 EWMA 水平与趋势。
    pub fn record(&mut self, sample: LoadSample) {
        self.history.push_back(sample);
        if self.history.len() > self.history_size {
            self.history.pop_front();
        }
        if !self.initialized {
            self.cpu_ewma = sample.cpu.clamp(0.0, 1.0);
            self.io_ewma = sample.io.clamp(0.0, 1.0);
            self.cpu_trend = 0.0;
            self.io_trend = 0.0;
            self.initialized = true;
            return;
        }
        let a = self.alpha;
        // 趋势：delta 的 EWMA
        let cpu_delta = sample.cpu - self.cpu_ewma;
        let io_delta = sample.io - self.io_ewma;
        self.cpu_trend = a * cpu_delta + (1.0 - a) * self.cpu_trend;
        self.io_trend = a * io_delta + (1.0 - a) * self.io_trend;
        // 水平：采样值的 EWMA
        self.cpu_ewma = a * sample.cpu + (1.0 - a) * self.cpu_ewma;
        self.io_ewma = a * sample.io + (1.0 - a) * self.io_ewma;
    }

    /// 预测未来 `ticks_ahead` 个 tick 后的 CPU/IO 负载，clamp 到 [0,1]。
    pub fn predict(&self, ticks_ahead: u32) -> LoadSample {
        let t = ticks_ahead as f64;
        let cpu = (self.cpu_ewma + self.alpha * self.cpu_trend * t).clamp(0.0, 1.0);
        let io = (self.io_ewma + self.alpha * self.io_trend * t).clamp(0.0, 1.0);
        LoadSample { cpu, io }
    }

    pub fn history_len(&self) -> usize {
        self.history.len()
    }

    pub fn summary(&self) -> PredictorSummary {
        let p1 = self.predict(1);
        PredictorSummary {
            history_len: self.history.len(),
            cpu_ewma: self.cpu_ewma,
            io_ewma: self.io_ewma,
            predicted_cpu_1tick: p1.cpu,
            predicted_io_1tick: p1.io,
        }
    }
}

/// 是否为预测式调度可推迟的低优先级任务（Normal/Background，非保活）。
///
/// 高优先级（Critical/Important）与保活任务不受预测式调度影响。
fn is_predictive_deferrable(meta: &TaskMetadata) -> bool {
    !meta.is_keepalive
        && (meta.priority == TaskPriority::Normal || meta.priority == TaskPriority::Background)
}

/// 预测负载是否超过准入阈值（CPU 或 IO 任一超阈值即视为高负载）。
fn predictive_over_threshold(knobs: &SchedulerKnobs, predicted: LoadSample) -> bool {
    predicted.cpu >= knobs.admission_cpu_threshold as f64
        || predicted.io >= knobs.admission_io_threshold as f64
}

/// S1-P3: 纯函数 —— 根据当前负载计算自适应执行间隔（秒）。
///
/// - 关闭/亚秒级/保活：由调用方过滤，本函数仅做数值计算。
/// - `factor = clamp(load / target, min_ratio, max_ratio)`。
/// - 结果再 clamp 到 `[base*min_ratio, base*max_ratio]`。
fn compute_adaptive_interval_secs(
    base_secs: u64,
    load: f64,
    target_load: f32,
    min_ratio: f32,
    max_ratio: f32,
) -> u64 {
    if base_secs == 0 {
        return base_secs;
    }
    let target = (target_load as f64).max(0.01);
    let factor = (load / target).clamp(min_ratio as f64, max_ratio as f64);
    let lo = (base_secs as f64) * min_ratio as f64;
    let hi = (base_secs as f64) * max_ratio as f64;
    let adjusted = (base_secs as f64) * factor;
    adjusted.clamp(lo, hi).round() as u64
}

// ---------------------------------------------------------------------------
// 任务画像记录（EWMA）
// ---------------------------------------------------------------------------

/// 单个任务的耗时画像
#[derive(Debug, Clone, Copy)]
pub struct ProfileEntry {
    /// EWMA 平滑后的平均耗时（毫秒）
    pub avg_duration_ms: f64,
    /// 累计运行次数
    pub run_count: u64,
    /// 最近一次运行耗时（毫秒）
    pub last_run_ms: u64,
}

/// 任务画像存储：记录每个任务实际耗时，EWMA 平滑。线程安全。
pub(crate) struct TaskProfileStore {
    entries: RwLock<HashMap<String, ProfileEntry>>,
    /// EWMA 系数，可热更新
    ewma_alpha: std::sync::atomic::AtomicU32,
}

impl TaskProfileStore {
    /// alpha 以 u32 位浮点比特存储，避免 AtomicF32 跨平台问题
    pub fn new(alpha: f32) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            ewma_alpha: std::sync::atomic::AtomicU32::new(alpha.to_bits()),
        }
    }

    pub fn set_alpha(&self, alpha: f32) {
        self.ewma_alpha.store(alpha.to_bits(), Ordering::Relaxed);
    }

    fn alpha(&self) -> f32 {
        f32::from_bits(self.ewma_alpha.load(Ordering::Relaxed))
    }

    /// 记录一次执行耗时（毫秒），EWMA 平滑
    pub fn record(&self, task_id: &str, duration_ms: f64) {
        let alpha = self.alpha() as f64;
        let mut entries = self.entries.write();
        let entry = entries.entry(task_id.to_string()).or_insert(ProfileEntry {
            avg_duration_ms: duration_ms,
            run_count: 0,
            last_run_ms: 0,
        });
        // 首次直接采用实际值，后续 EWMA 平滑
        if entry.run_count > 0 {
            entry.avg_duration_ms = alpha * duration_ms + (1.0 - alpha) * entry.avg_duration_ms;
        } else {
            entry.avg_duration_ms = duration_ms;
        }
        entry.run_count += 1;
        entry.last_run_ms = duration_ms as u64;
    }

    /// 查询某任务 EWMA 平均耗时（毫秒）
    pub fn get(&self, task_id: &str) -> Option<f64> {
        self.entries.read().get(task_id).map(|e| e.avg_duration_ms)
    }

    /// 全量快照（供监控查询）
    pub fn snapshot(&self) -> HashMap<String, ProfileEntry> {
        self.entries.read().clone()
    }
}

/// 纯函数：计算下一次执行间隔。
///
/// - `enabled=false`：返回原 interval（退化为固定间隔，行为与改造前一致）
/// - `enabled=true`：在 interval 上施加 ±ratio 的随机扰动
fn apply_interval_jitter(interval: Duration, enabled: bool, ratio: f32) -> Duration {
    if !enabled || ratio <= 0.0 {
        return interval;
    }
    use rand::Rng;
    let mut rng = rand::thread_rng();
    // uniform(-1.0, 1.0)
    let factor = rng.gen_range(-1.0f32..=1.0f32);
    let millis = interval.as_millis() as f64;
    let jittered = millis * (1.0 + (ratio * factor) as f64);
    Duration::from_millis(jittered.max(0.0) as u64)
}

// ---------------------------------------------------------------------------
// 调度队列项
// ---------------------------------------------------------------------------

struct ScheduledItem {
    task_id: String,
    scheduled_at: Instant,
    priority: TaskPriority,
    seq: u64,
}

impl Ord for ScheduledItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap 是最大堆：cmp 返回 Greater 的元素排在队首
        // 时间早的在前：other.scheduled_at.cmp(&self.scheduled_at)
        //   当 self 时间更早时，other.scheduled_at > self.scheduled_at，返回 Greater，self 排队首
        // 同时间优先级高的在前；同优先级 seq 小的在前
        other
            .scheduled_at
            .cmp(&self.scheduled_at)
            .then_with(|| other.priority.cmp(&self.priority))
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

impl PartialOrd for ScheduledItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ScheduledItem {
    fn eq(&self, other: &Self) -> bool {
        self.task_id == other.task_id && self.seq == other.seq
    }
}

impl Eq for ScheduledItem {}

// ---------------------------------------------------------------------------
// 任务调度器
// ---------------------------------------------------------------------------

type TaskFn =
    Arc<dyn Fn() -> futures::future::BoxFuture<'static, anyhow::Result<()>> + Send + Sync>;

/// RAII 守卫：任务执行体退出（含 panic 展开）时释放分类并发槽位，保证每次执行只释放一次。
struct CategorySlotGuard {
    scheduler: Arc<TaskScheduler>,
    category: TaskCategory,
}

impl Drop for CategorySlotGuard {
    fn drop(&mut self) {
        let mut running = self.scheduler.running_by_category.write();
        if let Some(cnt) = running.get_mut(&self.category) {
            *cnt = cnt.saturating_sub(1);
        }
    }
}

/// 六个 runtime 的 Handle 集合，用于跨 runtime 调度任务
#[derive(Clone)]
pub struct RuntimeHandles {
    pub crawler: tokio::runtime::Handle,
    pub federation: tokio::runtime::Handle,
    pub tracker: tokio::runtime::Handle,
    pub api: tokio::runtime::Handle,
    pub scheduler: tokio::runtime::Handle,
    pub persistence: tokio::runtime::Handle,
}

/// 智能任务调度器
pub struct TaskScheduler {
    tasks: RwLock<HashMap<String, TaskMetadata>>,
    task_fns: RwLock<HashMap<String, TaskFn>>,
    stats: RwLock<HashMap<String, TaskStats>>,
    queue: RwLock<BinaryHeap<ScheduledItem>>,
    /// 各分类当前运行任务数
    running_by_category: RwLock<HashMap<TaskCategory, u32>>,
    /// 各分类最大并发度
    max_concurrency: CategoryConcurrency,
    resource_monitor: Arc<ResourceMonitor>,
    seq_counter: RwLock<u64>,
    completed_dependencies: RwLock<HashSet<String>>,
    scheduler_started: RwLock<bool>,
    /// 最近一次心跳时间戳（毫秒，UNIX epoch），供独立 watchdog 线程读取
    last_heartbeat: Arc<AtomicI64>,
    /// watchdog 线程运行标志（false 时 watchdog 线程退出）
    watchdog_running: Arc<AtomicBool>,
    /// 自适应控制器（可选，None 时行为与改造前完全一致）
    adaptive_controller: Option<Arc<AdaptiveController>>,
    /// 运行时旋钮（准入/抖动/画像系数），全部默认关闭以保持向后兼容
    knobs: SchedulerKnobs,
    /// 任务画像（EWMA 耗时统计）
    profile_store: Arc<TaskProfileStore>,
    /// 负载采样器（可注入 mock）
    load_sampler: Arc<dyn LoadSampler>,
    /// 每个任务当前已被准入控制延迟的 tick 数（防饥饿）
    admission_delays: RwLock<HashMap<String, u32>>,
    /// S1-P2: EWMA 负载预测器（仅在 predictive_scheduling_enabled 时喂数/查询）
    load_predictor: RwLock<LoadPredictor>,
    /// S1-P2: 每个任务当前已被预测式调度推迟的 tick 数（防饥饿）
    predicted_delays: RwLock<HashMap<String, u32>>,
    /// S1-P2: 预测式推迟累计计数（监控用）
    predicted_defer_total: Arc<AtomicU64>,
    /// S1-P3/三: 外部注入的 IO 背压级别 [0.0, 1.0]（IOScheduler Agent 调用）
    external_io_backpressure: Arc<ParkingMutex<f32>>,
    /// S1-P3: 外部注入的 dirty 积压量（闭环反馈信号之一）
    dirty_backlog: Arc<AtomicI64>,
    /// 六个 runtime 的 Handle（None 时退化为当前 runtime，保持向后兼容）
    runtime_handles: Option<RuntimeHandles>,
}

impl TaskScheduler {
    pub fn new() -> Self {
        let resource_monitor = Arc::new(ResourceMonitor::new());
        let load_sampler: Arc<dyn LoadSampler> =
            Arc::new(ResourceMonitorSampler::new(resource_monitor.clone()));
        Self {
            tasks: RwLock::new(HashMap::new()),
            task_fns: RwLock::new(HashMap::new()),
            stats: RwLock::new(HashMap::new()),
            queue: RwLock::new(BinaryHeap::new()),
            running_by_category: RwLock::new({
                let mut m = HashMap::new();
                m.insert(TaskCategory::Crawl, 0);
                m.insert(TaskCategory::Persistence, 0);
                m.insert(TaskCategory::Monitor, 0);
                m.insert(TaskCategory::Network, 0);
                m.insert(TaskCategory::Federation, 0);
                m.insert(TaskCategory::Tracker, 0);
                m
            }),
            max_concurrency: CategoryConcurrency::default(),
            resource_monitor,
            seq_counter: RwLock::new(0),
            completed_dependencies: RwLock::new(HashSet::new()),
            scheduler_started: RwLock::new(false),
            last_heartbeat: Arc::new(AtomicI64::new(0)),
            watchdog_running: Arc::new(AtomicBool::new(false)),
            adaptive_controller: None,
            knobs: SchedulerKnobs::default(),
            profile_store: Arc::new(TaskProfileStore::new(
                SchedulerKnobs::default().profile_ewma_alpha,
            )),
            load_sampler,
            admission_delays: RwLock::new(HashMap::new()),
            load_predictor: RwLock::new(LoadPredictor::new(
                SchedulerKnobs::default().predict_ewma_alpha,
                SchedulerKnobs::default().predict_history_size,
            )),
            predicted_delays: RwLock::new(HashMap::new()),
            predicted_defer_total: Arc::new(AtomicU64::new(0)),
            external_io_backpressure: Arc::new(ParkingMutex::new(0.0)),
            dirty_backlog: Arc::new(AtomicI64::new(0)),
            runtime_handles: None,
        }
    }

    pub fn resource_monitor(&self) -> Arc<ResourceMonitor> {
        self.resource_monitor.clone()
    }

    /// 获取自适应控制器引用（供监控/外部访问）
    pub fn adaptive_controller(&self) -> Option<Arc<AdaptiveController>> {
        self.adaptive_controller.clone()
    }

    /// 设置各分类并发度
    pub fn with_category_concurrency(mut self, config: CategoryConcurrency) -> Self {
        self.max_concurrency = config;
        self
    }

    /// 注入六个 runtime 的 Handle，启用跨 runtime 调度
    pub fn with_runtime_handles(mut self, handles: RuntimeHandles) -> Self {
        self.runtime_handles = Some(handles);
        self
    }

    /// 注入自适应控制器（ICC 预测式自适应）
    pub fn with_adaptive_controller(mut self, controller: Arc<AdaptiveController>) -> Self {
        self.adaptive_controller = Some(controller);
        self
    }

    /// 注入运行时旋钮（准入/随机抖动/画像 EWMA 系数）。
    /// 所有新功能默认关闭，未注入时行为与改造前完全一致。
    pub fn with_knobs(mut self, knobs: SchedulerKnobs) -> Self {
        self.profile_store.set_alpha(knobs.profile_ewma_alpha);
        // 重建负载预测器以采用新的 alpha / 历史窗口
        *self.load_predictor.write() =
            LoadPredictor::new(knobs.predict_ewma_alpha, knobs.predict_history_size);
        self.knobs = knobs;
        self
    }

    /// S1-P3/三: 注入外部 IO 背压级别 [0.0, 1.0]（IOScheduler Agent 在 main.rs 中调用）。
    /// 0.0 表示无背压，1.0 表示满背压。注入后会与 ResourceMonitor 的 IO 负载取 max，
    /// 使准入控制与自适应间隔感知 IO 队列积压。
    pub fn set_external_io_backpressure(&self, level: f32) {
        let clamped = level.clamp(0.0, 1.0);
        *self.external_io_backpressure.lock() = clamped;
    }

    /// 查询当前外部 IO 背压级别 [0.0, 1.0]。
    pub fn external_io_backpressure(&self) -> f32 {
        *self.external_io_backpressure.lock()
    }

    /// 融合后的有效 IO 负载 = max(ResourceMonitor.io_usage, 外部背压)。
    /// 外部背压为 0 时返回值与 ResourceMonitor 一致（向后兼容）。
    pub fn io_load(&self) -> f32 {
        let monitor_io = self.resource_monitor.current().io_usage as f32;
        let bp = *self.external_io_backpressure.lock();
        monitor_io.max(bp)
    }

    /// 注入 dirty 积压量（闭环反馈信号）。主要供持久化/外部任务调用。
    pub fn set_dirty_backlog(&self, count: i64) {
        self.dirty_backlog.store(count.max(0), Ordering::Relaxed);
    }

    /// 查询当前 dirty 积压量。
    pub fn dirty_backlog(&self) -> i64 {
        self.dirty_backlog.load(Ordering::Relaxed)
    }

    /// 融合外部 IO 背压后的负载采样（准入/预测/自适应统一使用此采样）。
    fn effective_sample(&self) -> LoadSample {
        let mut s = self.load_sampler.sample();
        let bp = *self.external_io_backpressure.lock();
        s.io = s.io.max(bp as f64);
        s
    }

    /// S1-P2: 负载预测状态快照（供监控查询）。
    pub fn predictor_summary(&self) -> PredictorSummary {
        self.load_predictor.read().summary()
    }

    /// 注入自定义负载采样器（主要用于测试注入 mock）
    pub fn with_load_sampler(mut self, sampler: Arc<dyn LoadSampler>) -> Self {
        self.load_sampler = sampler;
        self
    }

    /// 任务画像快照（供监控查询，符合"可查询"原则）
    pub fn profile_snapshot(&self) -> HashMap<String, ProfileEntry> {
        self.profile_store.snapshot()
    }

    /// 任务画像查询单个任务平均耗时（毫秒）
    pub fn profile_avg(&self, task_id: &str) -> Option<f64> {
        self.profile_store.get(task_id)
    }

    /// 当前系统负载快照（供监控查询）
    pub fn load_snapshot(&self) -> LoadSample {
        self.load_sampler.sample()
    }

    /// 查询指定分类当前运行任务数
    pub fn running_count(&self, cat: TaskCategory) -> u32 {
        *self.running_by_category.read().get(&cat).unwrap_or(&0)
    }

    /// 查询指定分类最大并发度
    pub fn max_concurrency_for(&self, cat: TaskCategory) -> u32 {
        self.max_concurrency.max_for(cat)
    }

    /// 注册任务
    pub fn register<F, Fut>(&self, metadata: TaskMetadata, task_fn: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let id = metadata.id.clone();
        let name = metadata.name.clone();
        let task_fn: TaskFn = Arc::new(move || {
            let fut = task_fn();
            Box::pin(fut)
        });

        self.tasks.write().insert(id.clone(), metadata);
        self.task_fns.write().insert(id.clone(), task_fn);
        self.stats.write().insert(id.clone(), TaskStats::default());

        debug!("[task_scheduler] 任务已注册: {} ({})", id, name);
    }

    /// 批量注册任务并自动错峰（同周期任务分散到时间窗口）
    pub fn register_with_stagger<F, Fut>(&self, metadatas: Vec<TaskMetadata>, task_fns: Vec<F>)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        // 按周期分组
        let mut by_interval: HashMap<u64, Vec<usize>> = HashMap::new();
        for (i, meta) in metadatas.iter().enumerate() {
            let secs = meta.interval.as_secs();
            by_interval.entry(secs).or_default().push(i);
        }

        let mut adjusted_metadatas = metadatas;

        // 同周期任务错峰
        for indices in by_interval.values() {
            let count = indices.len();
            if count > 1 {
                for (j, &idx) in indices.iter().enumerate() {
                    // 分散到周期的前 80% 时间窗口
                    let stagger_secs =
                        (adjusted_metadatas[idx].interval.as_secs() as f64 * 0.8 * j as f64
                            / count as f64) as u64;
                    adjusted_metadatas[idx].initial_delay = Duration::from_secs(stagger_secs);
                    debug!(
                        "[task_scheduler] 错峰: {} 初始延迟 {}s",
                        adjusted_metadatas[idx].id, stagger_secs
                    );
                }
            }
        }

        for (meta, task_fn) in adjusted_metadatas.into_iter().zip(task_fns) {
            self.register(meta, task_fn);
        }
    }

    /// 启动调度器
    pub fn start(self: &Arc<Self>) {
        // 原子 check-and-set：避免 read 与 write 之间的 TOCTOU 竞态导致重复启动
        // （两个调用者同时通过判断 → 启动两个 run 循环 + 两个 watchdog 线程）
        {
            let mut started = self.scheduler_started.write();
            if *started {
                warn!("[task_scheduler] 调度器已启动，忽略重复启动");
                return;
            }
            *started = true;
        }

        let scheduler = self.clone();
        if let Some(ref handles) = self.runtime_handles {
            handles.scheduler.spawn(async move {
                scheduler.run().await;
            });
        } else {
            tokio::spawn(async move {
                scheduler.run().await;
            });
        }

        // P1-3: 启动独立 OS 线程 watchdog，不依赖 tokio runtime
        // 即使 runtime 线程被全部占满，watchdog 仍能检测到心跳停止
        self.watchdog_running.store(true, Ordering::Relaxed);
        let watchdog_heartbeat = self.last_heartbeat.clone();
        let watchdog_running = self.watchdog_running.clone();
        std::thread::Builder::new()
            .name("task-scheduler-watchdog".to_string())
            .spawn(move || {
                // [ALLOWED-SLEEP] 调度器自身 watchdog 独立 OS 线程（std::thread），非 tokio 任务，
                // 目的是 runtime 线程全占满时仍能检测心跳停摆，不能注册到 TaskScheduler
                loop {
                    std::thread::sleep(Duration::from_secs(WATCHDOG_CHECK_INTERVAL_SECS));
                    if !watchdog_running.load(Ordering::Relaxed) {
                        break;
                    }
                    let last_ms = watchdog_heartbeat.load(Ordering::Relaxed);
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    if last_ms > 0 {
                        let elapsed_secs = (now_ms - last_ms) / 1000;
                        if elapsed_secs > WATCHDOG_HEARTBEAT_TIMEOUT_SECS {
                            error!(
                                "[task_scheduler] WATCHDOG: 调度器心跳停止超过 {} 秒，可能发生线程饥饿！最后心跳: {} 秒前",
                                WATCHDOG_HEARTBEAT_TIMEOUT_SECS, elapsed_secs
                            );
                        }
                    }
                }
            })
            .expect("failed to spawn watchdog thread");

        info!(
            "[task_scheduler] 智能任务调度中心已启动（分级并发: crawl={}, persistence={}, monitor={}, network={}, federation={}, tracker={}）",
            self.max_concurrency.crawl,
            self.max_concurrency.persistence,
            self.max_concurrency.monitor,
            self.max_concurrency.network,
            self.max_concurrency.federation,
            self.max_concurrency.tracker
        );
    }

    /// 优雅停止调度器：置位运行标志，主循环与 watchdog 线程都会在下一轮检查后退出。
    ///
    /// 之前 watchdog 线程只有 `watchdog_running` 一个退出条件却无人置位，
    /// 相当于永久驻留的宿主线程；此处提供正式停止入口。
    pub fn stop(&self) {
        let was_running = *self.scheduler_started.read();
        *self.scheduler_started.write() = false;
        self.watchdog_running.store(false, Ordering::Relaxed);
        if was_running {
            info!("[task_scheduler] 已请求停止调度器（主循环与 watchdog 将退出）");
        }
    }

    /// 调度器是否正在运行
    pub fn is_running(&self) -> bool {
        *self.scheduler_started.read()
    }

    /// 调度器主循环
    async fn run(self: Arc<Self>) {
        // 初始化：为每个任务安排第一次执行
        {
            let tasks = self.tasks.read();
            for (id, meta) in tasks.iter() {
                let jitter_secs = if meta.jitter.as_secs() > 0 {
                    use rand::Rng;
                    let mut rng = rand::thread_rng();
                    rng.gen_range(0..=meta.jitter.as_secs())
                } else {
                    0
                };
                let scheduled_at =
                    Instant::now() + meta.initial_delay + Duration::from_secs(jitter_secs);
                self.schedule_task(id, scheduled_at, meta.priority);
            }
        }

        // [ALLOWED-INTERVAL] TaskScheduler 自身 tick，调度器内核
        let mut tick_interval = tokio::time::interval(SCHEDULER_TICK_INTERVAL);
        let mut heartbeat = Instant::now();
        let mut adaptive_tick: u64 = 0;

        loop {
            tick_interval.tick().await;
            // 支持优雅停止：stop() 会将 scheduler_started 置 false，主循环退出
            if !*self.scheduler_started.read() {
                info!("[task_scheduler] 调度器已停止，主循环退出");
                break;
            }
            // S1-P3: 闭环重算自适应间隔（每 N 个 tick；关闭时 no-op）
            adaptive_tick = adaptive_tick.wrapping_add(1);
            let recalc_period = self.knobs.adaptive_recalc_ticks.max(1) as u64;
            if self.knobs.adaptive_interval_enabled && adaptive_tick.is_multiple_of(recalc_period) {
                self.recalc_adaptive_intervals();
            }
            if heartbeat.elapsed() >= SCHEDULER_HEARTBEAT_INTERVAL {
                let queue_len = self.queue.read().len();
                let by_cat = self.running_by_category.read();
                info!(
                    "[task_scheduler] 调度器心跳: 队列待执行={}, 运行中[crawl={}, persistence={}, monitor={}, network={}]",
                    queue_len,
                    by_cat.get(&TaskCategory::Crawl).unwrap_or(&0),
                    by_cat.get(&TaskCategory::Persistence).unwrap_or(&0),
                    by_cat.get(&TaskCategory::Monitor).unwrap_or(&0),
                    by_cat.get(&TaskCategory::Network).unwrap_or(&0),
                );
                drop(by_cat);
                heartbeat = Instant::now();
                // P1-3: 更新原子心跳时间戳，供独立 watchdog 线程检测
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                self.last_heartbeat.store(now_ms, Ordering::Relaxed);
            }
            Self::process_queue(self.clone()).await;
        }
    }

    /// 安排任务执行
    fn schedule_task(&self, task_id: &str, at: Instant, priority: TaskPriority) {
        let seq = {
            let mut counter = self.seq_counter.write();
            *counter += 1;
            *counter
        };
        self.queue.write().push(ScheduledItem {
            task_id: task_id.to_string(),
            scheduled_at: at,
            priority,
            seq,
        });
    }

    /// 处理调度队列
    async fn process_queue(scheduler: Arc<Self>) {
        let now = Instant::now();
        let resource = scheduler.resource_monitor.current();

        // S1-P2: 每 tick 喂入融合背压后的采样点给负载预测器（仅在启用时）
        if scheduler.knobs.predictive_scheduling_enabled {
            let sample = scheduler.effective_sample();
            scheduler.load_predictor.write().record(sample);
        }

        // 收集到期的任务
        let mut due_tasks: Vec<ScheduledItem> = Vec::new();
        {
            let mut queue = scheduler.queue.write();
            while let Some(item) = queue.peek() {
                if item.scheduled_at <= now {
                    due_tasks.push(queue.pop().unwrap());
                } else {
                    break;
                }
            }
        }

        for item in due_tasks {
            // 检查依赖
            if !scheduler.check_dependencies(&item.task_id) {
                // 依赖未完成，延迟30秒重试
                scheduler.schedule_task(
                    &item.task_id,
                    Instant::now() + DEPENDENCY_RETRY_DELAY,
                    item.priority,
                );
                continue;
            }

            // 资源感知调度
            let meta = match scheduler.tasks.read().get(&item.task_id).cloned() {
                Some(m) => m,
                None => continue,
            };

            if meta.deferrable {
                if resource.is_critical() && meta.priority != TaskPriority::Critical {
                    // 资源极度紧张，延迟非关键任务
                    debug!(
                        "[task_scheduler] 资源极度紧张，延迟任务: {} (CPU={:.0}%)",
                        meta.name,
                        resource.cpu_usage * 100.0
                    );
                    scheduler.schedule_task(
                        &item.task_id,
                        Instant::now() + CRITICAL_RESOURCE_DELAY,
                        item.priority,
                    );
                    continue;
                }

                if resource.is_stressed()
                    && meta.priority >= TaskPriority::Normal
                    && meta.resource.is_full_task
                {
                    // 资源紧张，延迟全量普通任务
                    debug!(
                        "[task_scheduler] 资源紧张，延迟全量任务: {} (CPU={:.0}%)",
                        meta.name,
                        resource.cpu_usage * 100.0
                    );
                    scheduler.schedule_task(
                        &item.task_id,
                        Instant::now() + STRESSED_FULL_TASK_DELAY,
                        item.priority,
                    );
                    continue;
                }
            }

            // 分级并发控制：按任务分类限流
            {
                let cat = meta.category;
                let running = scheduler.running_count(cat);
                let max = scheduler.max_concurrency_for(cat);
                if running >= max {
                    debug!(
                        "[task_scheduler] 分类并发已满（{:?} {}/{}），延迟: {}",
                        cat, running, max, meta.name
                    );
                    scheduler.schedule_task(
                        &item.task_id,
                        Instant::now() + CONCURRENCY_FULL_DELAY,
                        item.priority,
                    );
                    continue;
                }
            }

            // 资源感知准入控制（默认关闭；关闭后 admission_decide 恒为 Allow，行为不变）
            {
                let sample = scheduler.effective_sample();
                let delayed = *scheduler
                    .admission_delays
                    .read()
                    .get(&item.task_id)
                    .unwrap_or(&0);
                match admission_decide(&scheduler.knobs, &meta, sample, delayed) {
                    AdmissionDecision::Delay => {
                        *scheduler
                            .admission_delays
                            .write()
                            .entry(item.task_id.clone())
                            .or_insert(0) += 1;
                        debug!(
                            "[task_scheduler] 准入延迟: {} (CPU={:.0}% IO={:.0}%, 第 {} tick)",
                            meta.name,
                            sample.cpu * 100.0,
                            sample.io * 100.0,
                            delayed + 1
                        );
                        scheduler.schedule_task(
                            &item.task_id,
                            Instant::now() + SCHEDULER_TICK_INTERVAL,
                            item.priority,
                        );
                        continue;
                    }
                    AdmissionDecision::Force => {
                        warn!(
                            "[task_scheduler] 准入延迟预算用尽，强制执行: {} (CPU={:.0}% IO={:.0}%)",
                            meta.name,
                            sample.cpu * 100.0,
                            sample.io * 100.0
                        );
                        scheduler.admission_delays.write().remove(&item.task_id);
                    }
                    AdmissionDecision::Allow => {
                        // 正常准入，清除历史延迟计数
                        if delayed > 0 {
                            scheduler.admission_delays.write().remove(&item.task_id);
                        }
                    }
                }
            }

            // S1-P2: 预测式调度（默认关闭；仅推迟 Normal/Background 低优先级任务）
            if scheduler.knobs.predictive_scheduling_enabled && is_predictive_deferrable(&meta) {
                let predicted_over = {
                    let p = scheduler.load_predictor.read();
                    let lookahead = scheduler.knobs.predict_lookahead_ticks.max(1);
                    (1..=lookahead)
                        .any(|t| predictive_over_threshold(&scheduler.knobs, p.predict(t)))
                };
                if predicted_over {
                    let delayed = *scheduler
                        .predicted_delays
                        .read()
                        .get(&item.task_id)
                        .unwrap_or(&0);
                    if delayed >= scheduler.knobs.admission_max_delay_ticks {
                        // 预测式推迟预算用尽，强制执行避免饥饿
                        scheduler.predicted_delays.write().remove(&item.task_id);
                        debug!(
                            "[task_scheduler] 预测式推迟预算用尽，强制执行: {}",
                            meta.name
                        );
                    } else {
                        *scheduler
                            .predicted_delays
                            .write()
                            .entry(item.task_id.clone())
                            .or_insert(0) += 1;
                        scheduler
                            .predicted_defer_total
                            .fetch_add(1, Ordering::Relaxed);
                        debug!(
                            "[task_scheduler] 预测式推迟低优先级任务: {} (第 {} tick)",
                            meta.name,
                            delayed + 1
                        );
                        scheduler.schedule_task(
                            &item.task_id,
                            Instant::now() + SCHEDULER_TICK_INTERVAL,
                            item.priority,
                        );
                        continue;
                    }
                } else {
                    scheduler.predicted_delays.write().remove(&item.task_id);
                }
            }

            // 执行任务
            debug!(
                "[task_scheduler] 执行任务: {} (优先级={:?})",
                item.task_id, item.priority
            );
            Self::execute_task(scheduler.clone(), item).await;
        }
    }

    /// 检查任务依赖是否都已完成
    fn check_dependencies(&self, task_id: &str) -> bool {
        let meta = match self.tasks.read().get(task_id) {
            Some(m) => m.clone(),
            None => return true,
        };
        if meta.dependencies.is_empty() {
            return true;
        }
        let completed = self.completed_dependencies.read();
        meta.dependencies.iter().all(|dep| completed.contains(dep))
    }

    /// S1-P3: 闭环重算各任务自适应执行间隔。
    ///
    /// 反馈信号：融合外部背压后的 CPU/IO 负载 + dirty 积压量。
    /// - 保活任务与亚秒级任务不参与（adaptive_eligible=false）。
    /// - dirty 积压高时，优先延长非持久化任务间隔，给持久化让资源。
    fn recalc_adaptive_intervals(&self) {
        if !self.knobs.adaptive_interval_enabled {
            return;
        }
        let sample = self.effective_sample();
        let load = sample.cpu.max(sample.io);
        let target = self.knobs.adaptive_target_load;
        let min_ratio = self.knobs.adaptive_min_ratio;
        let max_ratio = self.knobs.adaptive_max_ratio;
        let dirty = self.dirty_backlog.load(Ordering::Relaxed);

        // dirty 积压越重，非持久化任务间隔延长越多（线性到 max_ratio）
        let dirty_extend = if dirty > 0 {
            let ratio = (dirty as f64 / 1000.0).clamp(0.0, 1.0);
            1.0 + ratio * (max_ratio as f64 - 1.0)
        } else {
            1.0
        };

        let mut tasks = self.tasks.write();
        for meta in tasks.values_mut() {
            if !meta.adaptive_eligible() {
                continue;
            }
            let base = meta.interval.as_secs();
            let mut adjusted =
                compute_adaptive_interval_secs(base, load, target, min_ratio, max_ratio);
            // dirty 积压高：延长非持久化任务
            if meta.category != TaskCategory::Persistence && dirty_extend > 1.0 {
                let lo = (base as f64) * min_ratio as f64;
                let hi = (base as f64) * max_ratio as f64;
                adjusted = ((adjusted as f64) * dirty_extend).clamp(lo, hi).round() as u64;
            }
            meta.current_interval_secs = adjusted;
            meta.adaptive_deviation = if base > 0 {
                adjusted as f32 / base as f32
            } else {
                1.0
            };
        }
    }

    /// 执行任务
    async fn execute_task(scheduler: Arc<Self>, item: ScheduledItem) {
        let meta = match scheduler.tasks.read().get(&item.task_id).cloned() {
            Some(m) => m,
            None => return,
        };
        let task_fn = match scheduler.task_fns.read().get(&item.task_id).cloned() {
            Some(f) => f,
            None => return,
        };

        let category = meta.category;
        *scheduler
            .running_by_category
            .write()
            .entry(category)
            .or_insert(0) += 1;

        // RAII 守卫：无论正常结束/超时/失败/panic，分类并发槽位只释放一次
        let slot_guard = CategorySlotGuard {
            scheduler: scheduler.clone(),
            category,
        };

        let task_id = item.task_id.clone();

        let runtime_handle = scheduler.runtime_handles.as_ref().map(|h| match category {
            TaskCategory::Crawl | TaskCategory::Network => h.crawler.clone(),
            TaskCategory::Federation => h.federation.clone(),
            TaskCategory::Tracker => h.tracker.clone(),
            TaskCategory::Persistence => h.persistence.clone(),
            TaskCategory::Monitor => h.api.clone(),
        });

        let fut = async move {
            // 移入 fut：执行体退出（含 panic 展开）时由 Drop 释放槽位
            let _slot_guard = slot_guard;

            let start = Instant::now();
            // 捕获任务 panic：不得穿出 fut，否则槽位不释放、该任务静默停跑
            let raw = std::panic::AssertUnwindSafe(tokio::time::timeout(meta.timeout, task_fn()))
                .catch_unwind()
                .await;

            let duration = start.elapsed();

            // 任务画像：无论成功/失败/超时都记录实际耗时（EWMA 平滑）
            scheduler
                .profile_store
                .record(&task_id, duration.as_secs_f64() * 1000.0);

            // 归一化为 成功/失败/超时/panic 四态
            enum Outcome {
                Ok,
                Failed(String),
                Timeout,
                Panicked,
            }
            let outcome = match raw {
                Ok(Ok(Ok(()))) => Outcome::Ok,
                Ok(Ok(Err(e))) => Outcome::Failed(e.to_string()),
                Ok(Err(_elapsed)) => Outcome::Timeout,
                Err(_panic) => Outcome::Panicked,
            };

            let mut retry_scheduled = false;
            match outcome {
                Outcome::Ok => {
                    if let Some(s) = scheduler.stats.write().get_mut(&task_id) {
                        s.record_success(duration);
                    }
                    debug!(
                        "[task_scheduler] 任务完成: {} ({:.2}s)",
                        meta.name,
                        duration.as_secs_f64()
                    );
                }
                Outcome::Failed(ref e) => {
                    let consecutive = {
                        let mut stats = scheduler.stats.write();
                        match stats.get_mut(&task_id) {
                            Some(s) => {
                                s.record_failure();
                                s.consecutive_failures
                            }
                            None => 0,
                        }
                    };
                    warn!(
                        "[task_scheduler] 任务失败: {} - {} (连续失败 {})",
                        meta.name, e, consecutive
                    );
                    // 失败重试（指数退避）；重试后不重复安排正常周期
                    if consecutive < meta.max_retries {
                        let backoff = Duration::from_secs(2u64.pow(consecutive.min(5)));
                        scheduler.schedule_task(&task_id, Instant::now() + backoff, meta.priority);
                        retry_scheduled = true;
                    }
                    // 超过最大重试次数：走下方正常周期继续尝试
                }
                Outcome::Timeout => {
                    if let Some(s) = scheduler.stats.write().get_mut(&task_id) {
                        s.record_failure();
                    }
                    warn!(
                        "[task_scheduler] 任务超时: {} (>{:.0}s)",
                        meta.name,
                        meta.timeout.as_secs_f64()
                    );
                }
                Outcome::Panicked => {
                    if let Some(s) = scheduler.stats.write().get_mut(&task_id) {
                        s.record_failure();
                    }
                    error!(
                        "[task_scheduler] 任务 panic: {} —— 已记录失败并按正常周期重排",
                        meta.name
                    );
                }
            }

            // 标记依赖完成
            scheduler
                .completed_dependencies
                .write()
                .insert(task_id.clone());

            // 重试已单独排期 → 直接返回（槽位由 _slot_guard 的 Drop 释放）
            if retry_scheduled {
                return;
            }

            // 安排下一次执行（叠加既有固定 jitter + 可选每轮比例随机抖动）
            let jitter_secs = if meta.jitter.as_secs() > 0 {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                rng.gen_range(0..=meta.jitter.as_secs())
            } else {
                0
            };
            // S1-P3: 自适应间隔（关闭时回退基准 meta.interval，行为与改造前一致）
            // 重新读取最新的 current_interval_secs（闭环可能在任务运行期间重算）
            let base_interval = if scheduler.knobs.adaptive_interval_enabled {
                scheduler
                    .tasks
                    .read()
                    .get(&task_id)
                    .filter(|m| m.adaptive_eligible())
                    .map(|m| Duration::from_secs(m.current_interval_secs))
                    .unwrap_or(meta.interval)
            } else {
                meta.interval
            };
            let interval_with_ratio = apply_interval_jitter(
                base_interval,
                scheduler.knobs.random_jitter_enabled,
                scheduler.knobs.random_jitter_ratio,
            );
            scheduler.schedule_task(
                &task_id,
                Instant::now() + interval_with_ratio + Duration::from_secs(jitter_secs),
                meta.priority,
            );
        };

        match runtime_handle {
            Some(h) => h.spawn(fut),
            None => tokio::spawn(fut),
        };
    }

    /// 获取任务统计
    pub fn get_stats(&self, task_id: &str) -> Option<TaskStats> {
        self.stats.read().get(task_id).cloned()
    }

    /// 获取所有任务统计
    pub fn get_all_stats(&self) -> HashMap<String, TaskStats> {
        self.stats.read().clone()
    }

    /// 获取已注册任务列表
    pub fn list_tasks(&self) -> Vec<TaskMetadata> {
        self.tasks.read().values().cloned().collect()
    }

    /// 获取调度器状态摘要
    pub fn summary(&self) -> TaskSchedulerSummary {
        let tasks = self.tasks.read();
        let stats = self.stats.read();
        let running_by_cat = self.running_by_category.read().clone();
        let max_conc = self.max_concurrency;
        let queue_len = self.queue.read().len();

        let mut total_executions = 0u64;
        let mut total_failures = 0u64;
        for s in stats.values() {
            total_executions += s.total_executions;
            total_failures += s.failure_count;
        }

        TaskSchedulerSummary {
            registered_tasks: tasks.len(),
            running_by_category: running_by_cat,
            max_concurrency: max_conc,
            queued_tasks: queue_len,
            total_executions,
            total_failures,
            resource: self.resource_monitor.current(),
            adaptive_interval_enabled: self.knobs.adaptive_interval_enabled,
            adaptive_task_count: tasks.values().filter(|m| m.adaptive_eligible()).count(),
            avg_adaptive_deviation: {
                let elig: Vec<&TaskMetadata> =
                    tasks.values().filter(|m| m.adaptive_eligible()).collect();
                if elig.is_empty() {
                    1.0
                } else {
                    elig.iter().map(|m| m.adaptive_deviation).sum::<f32>() / elig.len() as f32
                }
            },
            predictive_defer_total: self.predicted_defer_total.load(Ordering::Relaxed),
            external_io_backpressure: *self.external_io_backpressure.lock(),
            dirty_backlog: self.dirty_backlog.load(Ordering::Relaxed),
        }
    }

    /// 各分类当前运行任务数（按分类名聚合，供监控查询）
    pub fn running_by_category(&self) -> HashMap<String, u32> {
        self.running_by_category
            .read()
            .iter()
            .map(|(cat, n)| {
                let name = match cat {
                    TaskCategory::Crawl => "crawl",
                    TaskCategory::Persistence => "persistence",
                    TaskCategory::Monitor => "monitor",
                    TaskCategory::Network => "network",
                    TaskCategory::Federation => "federation",
                    TaskCategory::Tracker => "tracker",
                };
                (name.to_string(), *n)
            })
            .collect()
    }

    /// 任务等待队列长度
    pub fn queue_len(&self) -> usize {
        self.queue.read().len()
    }

    /// 各任务最近10次平均耗时（毫秒）
    pub fn task_recent_avg_durations(&self) -> HashMap<String, u64> {
        self.stats
            .read()
            .iter()
            .map(|(id, s)| (id.clone(), s.recent_avg_duration_ms()))
            .collect()
    }
}

impl Default for TaskScheduler {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// 调度器状态摘要
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TaskSchedulerSummary {
    pub registered_tasks: usize,
    /// 各分类当前运行任务数
    pub running_by_category: HashMap<TaskCategory, u32>,
    /// 各分类最大并发度
    pub max_concurrency: CategoryConcurrency,
    pub queued_tasks: usize,
    pub total_executions: u64,
    pub total_failures: u64,
    pub resource: ResourceState,
    /// S1-P3: 自适应间隔是否启用
    pub adaptive_interval_enabled: bool,
    /// S1-P3: 参与自适应闭环的任务数
    pub adaptive_task_count: usize,
    /// S1-P3: 参与自适应任务的平均偏离基准比例
    pub avg_adaptive_deviation: f32,
    /// S1-P2: 预测式推迟累计次数
    pub predictive_defer_total: u64,
    /// 三: 当前外部 IO 背压级别 [0.0, 1.0]
    pub external_io_backpressure: f32,
    /// S1-P3: 当前 dirty 积压量
    pub dirty_backlog: i64,
}

// ---------------------------------------------------------------------------
// 单元测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_priority_ordering() {
        assert!(TaskPriority::Critical < TaskPriority::Important);
        assert!(TaskPriority::Important < TaskPriority::Normal);
        assert!(TaskPriority::Normal < TaskPriority::Background);
    }

    #[test]
    fn test_task_metadata_builder() {
        let meta = TaskMetadata::new("test", "Test Task", Duration::from_secs(60))
            .with_priority(TaskPriority::Important)
            .with_initial_delay(Duration::from_secs(30))
            .non_deferrable();

        assert_eq!(meta.id, "test");
        assert_eq!(meta.priority, TaskPriority::Important);
        assert_eq!(meta.initial_delay, Duration::from_secs(30));

        assert!(!meta.deferrable);
        assert_eq!(meta.max_delay, Duration::from_secs(0));
    }

    #[test]
    fn test_task_stats() {
        let mut stats = TaskStats::default();
        stats.record_success(Duration::from_millis(100));

        stats.record_success(Duration::from_millis(200));

        stats.record_failure();

        assert_eq!(stats.total_executions, 3);
        assert_eq!(stats.success_count, 2);
        assert_eq!(stats.failure_count, 1);
        assert_eq!(stats.avg_duration_ms(), 100); // (100+200)/2
        assert_eq!(stats.max_duration_ms, 200);
        assert_eq!(stats.min_duration_ms, 100);
        assert_eq!(stats.consecutive_failures, 1);
        assert!((stats.success_rate() - 2.0 / 3.0).abs() < 0.001);
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn test_resource_state() {
        let mut state = ResourceState::default();
        state.cpu_usage = 0.5;
        assert!(!state.is_stressed());
        assert!(!state.is_critical());

        state.cpu_usage = 0.85;
        assert!(state.is_stressed());
        assert!(!state.is_critical());

        state.cpu_usage = 0.96;
        assert!(state.is_stressed());
        assert!(state.is_critical());
    }

    #[test]
    fn test_resource_monitor() {
        let monitor = ResourceMonitor::new();
        monitor.update(0.75, 0.60);
        let state = monitor.current();
        assert!((state.cpu_usage - 0.75).abs() < 0.001);
        assert!((state.memory_usage - 0.60).abs() < 0.001);

        // 测试 clamp
        monitor.update(1.5, -0.5);
        let state = monitor.current();
        assert!((state.cpu_usage - 1.0).abs() < 0.001);
        assert!((state.memory_usage - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_scheduled_item_ordering() {
        let item1 = ScheduledItem {
            task_id: "a".to_string(),
            scheduled_at: Instant::now(),
            priority: TaskPriority::Critical,
            seq: 1,
        };
        let item2 = ScheduledItem {
            task_id: "b".to_string(),
            scheduled_at: Instant::now(),
            priority: TaskPriority::Normal,
            seq: 2,
        };

        // Critical 应该排在 Normal 前面
        assert!(item1 > item2);
    }

    #[tokio::test]
    async fn test_scheduler_register_and_summary() {
        let scheduler = Arc::new(TaskScheduler::new());

        scheduler.register(
            TaskMetadata::new("test1", "Test Task 1", Duration::from_secs(60))
                .with_priority(TaskPriority::Important),
            || async { Ok(()) },
        );

        let summary = scheduler.summary();
        assert_eq!(summary.registered_tasks, 1);
        assert_eq!(summary.max_concurrency.crawl, 4);
        assert_eq!(summary.max_concurrency.persistence, 1);

        let tasks = scheduler.list_tasks();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].id, "test1");
    }

    #[tokio::test]
    async fn test_scheduler_stagger_registration() {
        let scheduler = Arc::new(TaskScheduler::new());

        let metadatas = vec![
            TaskMetadata::new("t1", "Task 1", Duration::from_secs(300)),
            TaskMetadata::new("t2", "Task 2", Duration::from_secs(300)),
            TaskMetadata::new("t3", "Task 3", Duration::from_secs(300)),
            TaskMetadata::new("t4", "Task 4", Duration::from_secs(300)),
            TaskMetadata::new("t5", "Task 5", Duration::from_secs(300)),
        ];

        let task_fns: Vec<_> = (0..5).map(|_| || async { Ok(()) }).collect();

        scheduler.register_with_stagger(metadatas, task_fns);

        let tasks = scheduler.list_tasks();
        assert_eq!(tasks.len(), 5);

        // 验证错峰：初始延迟应该不同
        let delays: Vec<u64> = tasks.iter().map(|t| t.initial_delay.as_secs()).collect();
        let unique_delays: HashSet<u64> = delays.iter().cloned().collect();
        assert!(unique_delays.len() > 1, "错峰应该产生不同的初始延迟");
    }

    // ---- S1-P0: TaskProfileStore EWMA ----

    #[test]
    fn test_profile_store_ewma_convergence() {
        let store = TaskProfileStore::new(0.2);
        // 首次直接采用实际值
        store.record("t1", 1000.0);
        assert!((store.get("t1").unwrap() - 1000.0).abs() < 1e-6);

        // 之后持续记录 2000ms，EWMA 应单调收敛向 2000（alpha=0.2）
        let mut prev = 1000.0;
        for _ in 0..20 {
            store.record("t1", 2000.0);
            let cur = store.get("t1").unwrap();
            assert!(cur > prev - 1e-9, "EWMA 应收敛向新值: {} -> {}", prev, cur);
            prev = cur;
        }
        // 收敛后应明显大于初值且接近 2000
        let avg = store.get("t1").unwrap();
        assert!(avg > 1500.0, "平均耗时应收敛向 2000，实际 {}", avg);
        assert!(
            avg < 2000.0,
            "alpha=0.2 未到稳态，应略小于 2000，实际 {}",
            avg
        );

        let snap = store.snapshot();
        let e = snap.get("t1").unwrap();
        assert_eq!(e.run_count, 21);
        assert_eq!(e.last_run_ms, 2000);
    }

    #[test]
    fn test_profile_store_unknown_task() {
        let store = TaskProfileStore::new(0.2);
        assert!(store.get("nope").is_none());
        assert!(store.snapshot().is_empty());
    }

    // ---- S1-P1: 每轮随机抖动 ----

    #[test]
    fn test_jitter_disabled_equals_fixed_interval() {
        let interval = Duration::from_secs(60);
        for _ in 0..100 {
            let out = apply_interval_jitter(interval, false, 0.1);
            assert_eq!(out, interval, "关闭抖动时必须等于固定间隔");
        }
    }

    #[test]
    fn test_jitter_enabled_produces_variation() {
        let interval = Duration::from_secs(60);
        let ratio = 0.1;
        let mut seen = HashSet::new();
        for _ in 0..200 {
            let out = apply_interval_jitter(interval, true, ratio);
            // 必须落在 [54s, 66s] 范围内
            assert!(out >= Duration::from_secs(54), "抖动下界: {:?}", out);
            assert!(out <= Duration::from_secs(66), "抖动上界: {:?}", out);
            seen.insert(out);
        }
        assert!(seen.len() > 1, "开启抖动时多次结果不应全部相同");
    }

    // ---- S1-P1: 资源感知准入控制 ----

    #[test]
    fn test_admission_disabled_always_allows() {
        let knobs = SchedulerKnobs {
            admission_control_enabled: false,
            admission_cpu_threshold: 0.8,
            admission_io_threshold: 0.8,
            admission_max_delay_ticks: 5,
            ..Default::default()
        };
        let meta = TaskMetadata::new("t", "t", Duration::from_secs(60));
        // 即使负载爆表也允许
        let d = admission_decide(
            &knobs,
            &meta,
            LoadSample {
                cpu: 0.99,
                io: 0.99,
            },
            0,
        );
        assert_eq!(d, AdmissionDecision::Allow);
    }

    #[test]
    fn test_admission_delays_when_over_threshold_then_forces() {
        let knobs = SchedulerKnobs {
            admission_control_enabled: true,
            admission_cpu_threshold: 0.8,
            admission_io_threshold: 0.8,
            admission_max_delay_ticks: 3,
            ..Default::default()
        };
        let meta = TaskMetadata::new("t", "t", Duration::from_secs(60))
            .with_priority(TaskPriority::Normal);
        let heavy = LoadSample { cpu: 0.95, io: 0.1 };

        // 未超预算：延迟
        assert_eq!(
            admission_decide(&knobs, &meta, heavy, 0),
            AdmissionDecision::Delay
        );
        assert_eq!(
            admission_decide(&knobs, &meta, heavy, 2),
            AdmissionDecision::Delay
        );
        // 达到预算：强制执行
        assert_eq!(
            admission_decide(&knobs, &meta, heavy, 3),
            AdmissionDecision::Force
        );
        assert_eq!(
            admission_decide(&knobs, &meta, heavy, 9),
            AdmissionDecision::Force
        );
    }

    #[test]
    fn test_admission_keepsalive_and_critical_bypass() {
        let knobs = SchedulerKnobs {
            admission_control_enabled: true,
            admission_cpu_threshold: 0.8,
            admission_io_threshold: 0.8,
            admission_max_delay_ticks: 1,
            ..Default::default()
        };
        let heavy = LoadSample {
            cpu: 0.99,
            io: 0.99,
        };

        // 保活任务不受准入限制
        let keepalive = TaskMetadata::new("hb", "hb", Duration::from_secs(5)).with_keepalive();
        assert_eq!(
            admission_decide(&knobs, &keepalive, heavy, 99),
            AdmissionDecision::Allow
        );

        // Critical 任务不受准入限制
        let critical = TaskMetadata::new("h", "h", Duration::from_secs(1))
            .with_priority(TaskPriority::Critical);
        assert_eq!(
            admission_decide(&knobs, &critical, heavy, 99),
            AdmissionDecision::Allow
        );

        // 负载不高时普通任务也允许
        let light = LoadSample { cpu: 0.2, io: 0.2 };
        let normal = TaskMetadata::new("n", "n", Duration::from_secs(60));
        assert_eq!(
            admission_decide(&knobs, &normal, light, 0),
            AdmissionDecision::Allow
        );
    }

    #[test]
    fn test_io_threshold_triggers_admission() {
        let knobs = SchedulerKnobs {
            admission_control_enabled: true,
            admission_cpu_threshold: 0.8,
            admission_io_threshold: 0.8,
            admission_max_delay_ticks: 2,
            ..Default::default()
        };
        let meta = TaskMetadata::new("t", "t", Duration::from_secs(60));
        // CPU 低但 IO 超阈值 -> 延迟
        assert_eq!(
            admission_decide(&knobs, &meta, LoadSample { cpu: 0.1, io: 0.9 }, 0),
            AdmissionDecision::Delay
        );
    }

    // ---- S1-P2: LoadPredictor EWMA ----

    #[test]
    fn test_load_predictor_ewma() {
        let mut p = LoadPredictor::new(0.3, 10);

        // 先喂 10 个恒定低负载采样，收敛到水平值
        for _ in 0..10 {
            p.record(LoadSample { cpu: 0.2, io: 0.1 });
        }
        assert_eq!(p.history_len(), 10);
        // 稳态下预测应接近当前值（趋势≈0）
        let flat = p.predict(3);
        assert!(
            (flat.cpu - 0.2).abs() < 0.05,
            "稳态 CPU 预测应≈0.2: {}",
            flat.cpu
        );

        // 喂入一段上升序列，趋势应为正
        for cpu in [0.3, 0.4, 0.5, 0.6, 0.7] {
            p.record(LoadSample { cpu, io: 0.1 });
        }
        // 历史窗口=10，喂了15个采样后仍保持10
        assert_eq!(p.history_len(), 10);

        let now = p.predict(0);
        let ahead = p.predict(3);
        // 上升趋势下，前向预测应高于当前水平，且 clamp 到 [0,1]
        assert!(
            ahead.cpu > now.cpu,
            "趋势外推应上升: {} -> {}",
            now.cpu,
            ahead.cpu
        );
        assert!(ahead.cpu <= 1.0 && ahead.cpu >= 0.0);
        assert!(now.cpu <= 1.0 && now.cpu >= 0.0);
        // IO 未变化，预测应保持低位
        assert!(ahead.io < 0.2);
    }

    #[test]
    fn test_predictive_scheduling_defers_low_priority() {
        // Normal / Background 低优先级可被预测式推迟
        let normal = TaskMetadata::new("n", "n", Duration::from_secs(60));
        let background = TaskMetadata::new("b", "b", Duration::from_secs(60))
            .with_priority(TaskPriority::Background);
        assert!(is_predictive_deferrable(&normal));
        assert!(is_predictive_deferrable(&background));

        // Critical / Important 高优先级不受预测式调度影响
        let critical = TaskMetadata::new("c", "c", Duration::from_secs(1))
            .with_priority(TaskPriority::Critical);
        let important = TaskMetadata::new("i", "i", Duration::from_secs(30))
            .with_priority(TaskPriority::Important);
        assert!(!is_predictive_deferrable(&critical));
        assert!(!is_predictive_deferrable(&important));

        // 保活任务即使是 Normal 也不被推迟
        let keepalive = TaskMetadata::new("hb", "hb", Duration::from_secs(5)).with_keepalive();
        assert!(!is_predictive_deferrable(&keepalive));

        // 预测超阈值判定
        let knobs = SchedulerKnobs {
            admission_cpu_threshold: 0.8,
            admission_io_threshold: 0.8,
            ..Default::default()
        };
        assert!(predictive_over_threshold(
            &knobs,
            LoadSample { cpu: 0.9, io: 0.1 }
        ));
        assert!(predictive_over_threshold(
            &knobs,
            LoadSample { cpu: 0.1, io: 0.9 }
        ));
        assert!(!predictive_over_threshold(
            &knobs,
            LoadSample { cpu: 0.5, io: 0.5 }
        ));
    }

    // ---- S1-P3: 自适应间隔 ----

    #[test]
    fn test_adaptive_interval_shrinks_in_low_load() {
        // base=60s, target=0.6, min=0.5, max=2.0；load=0.2 -> factor clamp 到 0.5 -> 30s
        let secs = compute_adaptive_interval_secs(60, 0.2, 0.6, 0.5, 2.0);
        assert_eq!(secs, 30, "低负载应缩短到基准一半: {}", secs);
    }

    #[test]
    fn test_adaptive_interval_extends_in_high_load() {
        // load=0.9 -> factor=0.9/0.6=1.5 -> 90s
        let secs = compute_adaptive_interval_secs(60, 0.9, 0.6, 0.5, 2.0);
        assert_eq!(secs, 90, "高负载应延长: {}", secs);
        // 极高负载仍受 max_ratio=2.0 上界约束
        let capped = compute_adaptive_interval_secs(60, 2.0, 0.6, 0.5, 2.0);
        assert_eq!(capped, 120, "不应超过基准两倍: {}", capped);
    }

    #[test]
    fn test_adaptive_interval_keepalive_excluded() {
        // 普通长周期任务参与自适应
        let normal = TaskMetadata::new("n", "n", Duration::from_secs(60));
        assert!(normal.adaptive_eligible());

        // 保活任务不参与
        let hb = TaskMetadata::new("hb", "hb", Duration::from_secs(5)).with_keepalive();
        assert!(!hb.adaptive_eligible());

        // 亚秒级任务（gossip flush）不参与
        let fast = TaskMetadata::new("gf", "gf", Duration::from_millis(100));
        assert!(!fast.adaptive_eligible());
    }

    // ---- 三: 外部 IO 背压注入 ----

    #[tokio::test]
    async fn test_external_io_backpressure_injection() {
        let s = Arc::new(TaskScheduler::new());
        assert!((s.external_io_backpressure() - 0.0).abs() < 1e-6);
        assert!((s.io_load() - 0.0).abs() < 1e-6);

        // 注入背压后，io_load 应反映该背压
        s.set_external_io_backpressure(0.9);
        assert!((s.external_io_backpressure() - 0.9).abs() < 1e-6);
        assert!(
            (s.io_load() - 0.9).abs() < 1e-6,
            "io_load 应反映背压: {}",
            s.io_load()
        );

        // 超出范围 clamp 到 [0,1]
        s.set_external_io_backpressure(2.0);
        assert!((s.external_io_backpressure() - 1.0).abs() < 1e-6);
        s.set_external_io_backpressure(-0.5);
        assert!((s.external_io_backpressure() - 0.0).abs() < 1e-6);

        // summary 中也能查到
        let sum = s.summary();
        assert!((sum.external_io_backpressure - 0.0).abs() < 1e-6);
    }
}
