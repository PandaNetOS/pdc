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
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex as ParkingMutex, RwLock};
use tracing::{debug, error, info, warn};

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
    /// 网络类（发现器、联邦同步、NAT 等）
    Network,
}

/// 各分类并发度配置
#[derive(Debug, Clone, Copy)]
pub struct CategoryConcurrency {
    pub crawl: u32,
    pub persistence: u32,
    pub monitor: u32,
    pub network: u32,
}

impl Default for CategoryConcurrency {
    fn default() -> Self {
        Self {
            crawl: 4,
            persistence: 1,
            monitor: 2,
            network: 4,
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
    /// 系统负载采样间隔（秒）
    pub load_sample_interval_secs: u64,
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
            load_sample_interval_secs: 5,
        }
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

    /// 注入自适应控制器（ICC 预测式自适应）
    pub fn with_adaptive_controller(mut self, controller: Arc<AdaptiveController>) -> Self {
        self.adaptive_controller = Some(controller);
        self
    }

    /// 注入运行时旋钮（准入/随机抖动/画像 EWMA 系数）。
    /// 所有新功能默认关闭，未注入时行为与改造前完全一致。
    pub fn with_knobs(mut self, knobs: SchedulerKnobs) -> Self {
        self.profile_store.set_alpha(knobs.profile_ewma_alpha);
        self.knobs = knobs;
        self
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
        if *self.scheduler_started.read() {
            warn!("[task_scheduler] 调度器已启动，忽略重复启动");
            return;
        }
        *self.scheduler_started.write() = true;

        let scheduler = self.clone();
        tokio::spawn(async move {
            scheduler.run().await;
        });

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
            "[task_scheduler] 智能任务调度中心已启动（分级并发: crawl={}, persistence={}, monitor={}, network={}）",
            self.max_concurrency.crawl,
            self.max_concurrency.persistence,
            self.max_concurrency.monitor,
            self.max_concurrency.network
        );
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

        loop {
            tick_interval.tick().await;
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
                let sample = scheduler.load_sampler.sample();
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

        let task_id = item.task_id.clone();

        tokio::spawn(async move {
            let start = Instant::now();
            let result = tokio::time::timeout(meta.timeout, task_fn()).await;

            let duration = start.elapsed();

            // 任务画像：无论成功/失败/超时都记录实际耗时（EWMA 平滑）
            scheduler
                .profile_store
                .record(&task_id, duration.as_secs_f64() * 1000.0);

            match result {
                Ok(Ok(())) => {
                    scheduler
                        .stats
                        .write()
                        .get_mut(&task_id)
                        .unwrap()
                        .record_success(duration);
                    debug!(
                        "[task_scheduler] 任务完成: {} ({:.2}s)",
                        meta.name,
                        duration.as_secs_f64()
                    );
                }
                Ok(Err(e)) => {
                    scheduler
                        .stats
                        .write()
                        .get_mut(&task_id)
                        .unwrap()
                        .record_failure();
                    let consecutive = scheduler
                        .stats
                        .read()
                        .get(&task_id)
                        .map(|s| s.consecutive_failures)
                        .unwrap_or(0);
                    warn!(
                        "[task_scheduler] 任务失败: {} - {} (连续失败 {})",
                        meta.name, e, consecutive
                    );
                    // 标记依赖完成 + 释放分类计数（重试路径也需要）
                    scheduler
                        .completed_dependencies
                        .write()
                        .insert(task_id.clone());
                    {
                        let mut running = scheduler.running_by_category.write();
                        if let Some(cnt) = running.get_mut(&category) {
                            if *cnt > 0 {
                                *cnt -= 1;
                            }
                        }
                    }
                    // 失败重试（指数退避），重试后 return 避免与下方正常周期重复安排
                    if consecutive < meta.max_retries {
                        let backoff = Duration::from_secs(2u64.pow(consecutive.min(5)));
                        scheduler.schedule_task(&task_id, Instant::now() + backoff, meta.priority);
                        return;
                    }
                    // 超过最大重试次数：走下方正常周期继续尝试
                }
                Err(_) => {
                    scheduler
                        .stats
                        .write()
                        .get_mut(&task_id)
                        .unwrap()
                        .record_failure();
                    warn!(
                        "[task_scheduler] 任务超时: {} (>{:.0}s)",
                        meta.name,
                        meta.timeout.as_secs_f64()
                    );
                }
            }

            // 标记依赖完成
            scheduler
                .completed_dependencies
                .write()
                .insert(task_id.clone());

            {
                let mut running = scheduler.running_by_category.write();
                if let Some(cnt) = running.get_mut(&category) {
                    if *cnt > 0 {
                        *cnt -= 1;
                    }
                }
            }

            // 安排下一次执行（叠加既有固定 jitter + 可选每轮比例随机抖动）
            let jitter_secs = if meta.jitter.as_secs() > 0 {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                rng.gen_range(0..=meta.jitter.as_secs())
            } else {
                0
            };
            let interval_with_ratio = apply_interval_jitter(
                meta.interval,
                scheduler.knobs.random_jitter_enabled,
                scheduler.knobs.random_jitter_ratio,
            );
            scheduler.schedule_task(
                &task_id,
                Instant::now() + interval_with_ratio + Duration::from_secs(jitter_secs),
                meta.priority,
            );
        });
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
}
