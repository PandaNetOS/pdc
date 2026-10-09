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
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
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
/// 分类并发「饥饿补偿」：同一任务连续被分类并发拒绝达到该次数后，允许超额准入。
/// 6 次 × 5s = 30s，足以区分「暂满」与「结构性饿死」。
const CONCURRENCY_STARVE_ROUNDS: u32 = 6;
/// 饥饿补偿的**每分类**超额上限（硬并发 = max + 该值）。
const CONCURRENCY_OVERSUBSCRIBE: u32 = 2;
///
/// 【判据1】队列中「已到期却仍未被取出」连续命中轮数达到该值 ⇒ 主循环已停。
///
/// watchdog 每 `WATCHDOG_CHECK_INTERVAL_SECS`(30s) 跑一轮，
/// 取 2 轮 ⇒ 约 60s 持续积压才判定，避免单轮抖动误判。
const STALL_OVERDUE_ROUNDS: u32 = 2;

/// 【判据2】**在飞高水位归零** + 队列全在未来 ⇒ 全局停摆。
///
/// **为什么不能用「in_flight==0 + 队列项距今很大」**：这两种状态从队列
/// 本身**无法区分**——
///   · 事故态：20 个执行体全部挂在 `.await` 上被唤醒丢失，队列停在未来；
///   · 正常空闲态：系统本来就没有待跑任务，长任务（`interval` 达 300s 的
///     bootstrap / range 反熵类）刚入队，同样是「在飞 0 + 队列全在未来」。
/// 单元测试 `test_no_false_trigger_when_long_task_just_scheduled` 已证明
/// 仅靠队列判据必然误杀正常空闲期。
///
/// **区分信号 = 在飞高水位**（`peak_in_flight`）：
///   · 事故态：崩溃前有大量在飞（本次事故 33 个泄漏槽位），高水位 > 0，
///     之后归零 ⇒ 「曾有活、现在全没了」⇒ 停摆；
///   · 正常空闲态：高水位恒为 0 ⇒ 从未有过在飞 ⇒ 不是停摆。
const STALL_PEAK_IN_FLIGHT_MIN: usize = 1;

/// 判据2 的连续命中轮数（30s/轮 ⇒ 2 轮 ≈ 60s 持续空闲且无到期项）。
const STALL_IDLE_ROUNDS: u32 = 2;

/// 自愈触发最小间隔（秒）：避免反复触发把正常长任务误当成停摆。
const STALL_MIN_TRIGGER_INTERVAL_SECS: u64 = 30;

/// 【启动宽限期】调度器启动后的这段时间内**不做停摆判定**（秒）。
///
/// 依据：2026-10-09 首次部署自愈时实测误报——启动后 ~2 分钟内
/// 任务正陆续注册，`in_flight == 0` 是**正常**的（还没轮到执行），
/// 而高水位已因部分任务完成而累积，于是判据2 命中并误触发一次 abort。
///
/// 启动期与停摆期的形态差异在于：启动期任务仍在被陆续准入
///（`admitted_total` 持续增长），停摆期则完全停滞。故以
/// 「最近一次准入距今」作为豁免依据。
const STALL_STARTUP_GRACE_SECS: u64 = 300;

/// 分类「卡死保底」阈值（秒）：某分类在飞任务最老年龄超过该值时，
/// 判定该分类已被慢任务实质性卡死，**保底放行**该分类任务（不再等槽），
/// 避免面板/监控类被写路径饿死。
///
/// 背景（2026-10-09 生产实证）：Persistence 上限 1，被`WAL checkpoint` /
/// `WriteQueue 刷盘` / `TRUNCATE` / `oplog裁剪` 互相关联任务长期争抢唯一槽位
/// （00:49起持续 2.5 小时、50 次「已连续延迟 6 轮」）。Monitor 只有 2 槽且排在
/// 后面拿不到 → 面板指标不刷新 → `io/status` 15s 超时，而 CPU 100% 空闲、
/// 调度器心跳正常。**监控拿不到数据 ⇒ 故障不可见**，比慢更糟。
/// 保底放行让监控永远能拿到数据，代价是最多多 1~2 个并发监控任务。
pub const CATEGORY_STALL_FALLBACK_SECS: u64 = 120;

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
            // 2026-10-09 根治：1 → 2。原值 1 让`WAL checkpoint` / `WriteQueue 刷盘`
            // / `WAL TRUNCATE` / `oplog裁剪` / `全量持久化兜底` 这几个**互相关联**
            // 的写路径任务串行抢同一个槽位（生产实证：00:49 起持续 2.5 小时、
            // 50 次「已连续延迟 6 轮」，oplog 3 小时仅增长 919 行）。
            // 单槽的语义是「写库全局串行」，但 SQLite 侧本就有单写连接 + WAL 串行化，
            // 再叠加调度器单槽只会把「等写锁」与「等调度槽」两个等待串在一起，
            // 任何一个慢任务（如 1.4GB 库上的 TRUNCATE）就把整类停摆。
            // 2 槽允许「刷盘」与「checkpoint」并发推进——它们在 SQLite 写锁处
            // 天然互斥，调度器层不需要重复串行化。
            persistence: 2,
            // 监控类保底 +1：面板/指标是故障可观测性的唯一来源，
            // 与其被写路径饿死（导致 15s 超时、故障不可见），不如多留一个槽。
            // 配合 `CATEGORY_STALL_FALLBACK_SECS` 卡死保底双保险。
            monitor: 3,
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

    /// 设置任务超时（默认 300s）。
    ///
    /// 长耗时任务（如 range 反熵全仓对账、bootstrap 清单构建）需要更长预算：
    /// `tokio::time::timeout` 到点会直接掐死执行中的 future，任务反复重跑做重复功。
    pub fn with_timeout(mut self, t: Duration) -> Self {
        self.timeout = t;
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
    /// 槽位泄漏强制回收总开关（**默认 true**）。
    ///
    /// 在飞任务超过「超时 × [`Self::stale_slot_factor`]」仍不返回时判定为槽位泄漏，
    /// 强制回收该分类槽位。不回收的后果是分类并发槽被永久占满 ⇒ 该分类所有任务停摆
    /// （2026-09-21 远端 51 事故：联邦 runtime 静默后 10 个在飞任务永久占槽，
    /// Federation 分类封印至进程结束，日志刷了 5 万条）。
    /// 仅在确认「泄漏判定有误报风险」时才建议关闭。
    pub stale_slot_reclaim_enabled: bool,
    /// 槽位泄漏判定系数：在飞时长 > 任务 `timeout` × 该系数 ⇒ 判定泄漏。
    pub stale_slot_factor: f32,
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
            stale_slot_reclaim_enabled: true,
            stale_slot_factor: 1.5,
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
            stale_slot_reclaim_enabled: cfg.stale_slot_reclaim_enabled,
            stale_slot_factor: cfg.stale_slot_factor,
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
        // 槽位泄漏回收是**修 bug** 项，默认必须开启（关掉等于放任分类被永久封印）
        assert!(k.stale_slot_reclaim_enabled);
        assert_eq!(k.stale_slot_factor, d.stale_slot_factor);
    }

    /// 18 个字段逐个透传，不得有漏接或错位。
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
            stale_slot_reclaim_enabled: false,
            stale_slot_factor: 2.25,
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
        assert!(!k.stale_slot_reclaim_enabled);
        assert_eq!(k.stale_slot_factor, 2.25);
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

/// 「在飞」任务快照：准入时登记，释放时移除。
///
/// 这张表是**分类并发槽的唯一释放凭据**：
/// - 正常路径：执行体跑完 → [`CategorySlotGuard`] Drop → 摘除登记 + `running -= 1`；
/// - 异常路径：执行体因 runtime 停摆 / 同步段占死 worker 而永不返回
///   （`tokio::time::timeout` 只在 await 点取消，此时永不触发）→ 由
///   [`TaskScheduler::reclaim_stale_slots`] 强制回收。
///
/// 二者通过 `token` 互斥：谁先摘除登记谁负责递减计数，保证一个槽位只释放一次。
#[derive(Debug, Clone)]
struct InFlightTask {
    task_id: String,
    name: String,
    category: TaskCategory,
    started_at: Instant,
    timeout: Duration,
    /// 执行体的中止句柄（2026-10-09 新增）。
    ///
    /// 此前强制回收只把登记摘掉 + 计数减1，**执行体本身仍在跑**（多半卡在
    /// 同步阻塞段，`abort` 对它无效，但至少 await 点能被取消）。于是出现
    /// 「账面并发 = max，实际并发 = max + 泄漏数」——新任务被放进来后，
    /// 真实并发已经超限，资源进一步恶化。保留句柄是为了在回收时**尽力取消**：
    /// 卡在 await 的执行体立即停止并Drop 槽位；卡在同步段的则等它自己结束
    /// （此时 `CategorySlotGuard` 发现登记已被摘除，不会重复减计数）。
    abort_handle: Option<tokio::task::AbortHandle>,
}

/// RAII 守卫：任务执行体退出（含 panic 展开）时释放分类并发槽位。
///
/// 带 `token` 是为了与「watchdog 强制回收」互斥：若登记已被回收方摘除
/// （判定为泄漏槽位），此处不再递减，避免同一槽位被释放两次。
struct CategorySlotGuard {
    scheduler: Arc<TaskScheduler>,
    category: TaskCategory,
    token: u64,
}

impl Drop for CategorySlotGuard {
    fn drop(&mut self) {
        let still_owner = self
            .scheduler
            .in_flight
            .write()
            .remove(&self.token)
            .is_some();
        if !still_owner {
            // 已被强制回收（槽位泄漏）：不得重复递减
            debug!(
                "[task_scheduler] 槽位已被强制回收，跳过重复释放（token={}, {:?}）",
                self.token, self.category
            );
            return;
        }
        self.scheduler
            .released_total
            .fetch_add(1, Ordering::Relaxed);
        let mut running = self.scheduler.running_by_category.write();
        if let Some(cnt) = running.get_mut(&self.category) {
            *cnt = cnt.saturating_sub(1);
            debug!(
                "[task_scheduler] 任务释放槽位（{:?} 剩 {}）",
                self.category, *cnt
            );
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
    /// 监控专用 runtime（2026-10-09 治本 S4）
    ///
    /// 为何不能复用 `api`：本次事故中 Monitor 类全部挂在 api runtime
    /// （仅 4 worker 且与 HTTP 共用），保底放行把 Monitor 堆到 16 个在飞，
    /// 4 个 worker 全被长await 占死 ⇒ memory_monitor / checkpoint /
    /// 健康检查 / 实体统计校准 集体静默 41 分钟。监控是唯一可观测性来源，
    /// 必须与请求处理线程池隔离。
    pub monitor: tokio::runtime::Handle,
}

/// 智能任务调度器
pub struct TaskScheduler {
    tasks: RwLock<HashMap<String, TaskMetadata>>,
    task_fns: RwLock<HashMap<String, TaskFn>>,
    stats: RwLock<HashMap<String, TaskStats>>,
    queue: RwLock<BinaryHeap<ScheduledItem>>,
    /// 各分类当前运行任务数
    running_by_category: RwLock<HashMap<TaskCategory, u32>>,
    /// 各分类最大并发度（RwLock 包装：支持运行时热更，见 `update_category_concurrency`）
    max_concurrency: RwLock<CategoryConcurrency>,
    /// 各分类最大在飞年龄（秒）：某分类在飞任务超过该值即判定为「卡死」，
    /// 准入时对该分类任务**一律放行**（不再等槽），避免面板/监控类被写路径饿死。
    /// Key = 分类，Value = 秒。缺省表示该分类无此保底。
    category_stall_threshold_secs: RwLock<HashMap<TaskCategory, u64>>,
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
    /// 运行时旋钮（准入/抖动/画像系数），全部默认关闭以保持向后兼容。
    /// RwLock 包装：支持运行时热更，见 `update_knobs`。
    knobs: RwLock<SchedulerKnobs>,
    /// 任务画像（EWMA 耗时统计）
    profile_store: Arc<TaskProfileStore>,
    /// 负载采样器（可注入 mock）
    load_sampler: Arc<dyn LoadSampler>,
    /// 每个任务当前已被准入控制延迟的 tick 数（防饥饿）
    admission_delays: RwLock<HashMap<String, u32>>,
    /// 每个任务因「分类并发已满」被连续拒绝的轮数（防分类槽饿死，见
    /// [`CONCURRENCY_STARVE_ROUNDS`]）。短任务被长 await 任务占满分类槽时会无限期
    /// 排不到执行 —— 实测 bootstrap 续传 32 次被延迟 / 仅 3 次执行，82 块一块没传。
    concurrency_starve: RwLock<HashMap<String, u32>>,
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
    /// 「在飞」登记表：token → 在飞快照。准入时插入，释放/回收时摘除。
    /// 是槽位释放的唯一凭据，也是「最老在飞任务年龄」的数据来源。
    in_flight: RwLock<HashMap<u64, InFlightTask>>,
    /// 在飞 token 自增器（每次准入取一个新 token）
    in_flight_seq: AtomicU64,
    /// 累计准入次数（对账：准入 − 释放 − 强制回收 = 当前在飞）
    admitted_total: Arc<AtomicU64>,
    /// 累计正常释放次数
    released_total: Arc<AtomicU64>,
    /// 累计被强制回收的泄漏槽位数（> 0 说明发生过槽位泄漏事故）
    reclaimed_total: Arc<AtomicU64>,
    /// 累计触发全局停摆自愈的次数（2026-10-09 治本 S2）
    ///
    /// > 0 说明本进程发生过「任务体挂在 .await 上无人唤醒」型全局停摆。
    /// > 该事故原表现为：33 槽位回收后在飞/队列恒定 41 分钟，无人恢复。
    stall_recovery_total: Arc<AtomicU64>,
    /// 判据1 的连续命中轮数（已到期项积压持续性）
    stall_overdue_rounds: Arc<AtomicU32>,
    /// 判据2 的连续命中轮数（空闲但队列全在未来，持续性）
    stall_idle_rounds: Arc<AtomicU32>,
    /// 最近一次任务准入的毫秒时间戳（2026-10-09 治本 S2）
    ///
    /// 用于「启动/预热宽限期」判定：只要近期还有任务在被准入，
    /// 说明系统仍在正常调度，不该判为停摆。
    last_admit_ms: Arc<AtomicU64>,
    /// 在飞高水位（历史峰值，2026-10-09 治本 S2）
    ///
    /// **这是区分「事故态」与「正常空闲态」的唯一信号**：
    /// 正常空闲期从未有在飞（=0），事故态曾有大量在飞后归零。
    /// 见 `STALL_PEAK_IN_FLIGHT_MIN` 注释。
    peak_in_flight: Arc<AtomicUsize>,
    /// 上次触发自愈的毫秒时间戳（限流，避免反复触发误伤正常长任务）
    last_stall_recovery: Arc<AtomicU64>,
    /// 自愈后请求下一 tick 立即重跑 `process_queue`（缩短恢复延迟）
    process_queue_nudge: Arc<AtomicBool>,
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
            max_concurrency: RwLock::new(CategoryConcurrency::default()),
            category_stall_threshold_secs: RwLock::new(HashMap::new()),
            resource_monitor,
            seq_counter: RwLock::new(0),
            completed_dependencies: RwLock::new(HashSet::new()),
            scheduler_started: RwLock::new(false),
            last_heartbeat: Arc::new(AtomicI64::new(0)),
            watchdog_running: Arc::new(AtomicBool::new(false)),
            adaptive_controller: None,
            knobs: RwLock::new(SchedulerKnobs::default()),
            profile_store: Arc::new(TaskProfileStore::new(
                SchedulerKnobs::default().profile_ewma_alpha,
            )),
            load_sampler,
            admission_delays: RwLock::new(HashMap::new()),
            concurrency_starve: RwLock::new(HashMap::new()),
            load_predictor: RwLock::new(LoadPredictor::new(
                SchedulerKnobs::default().predict_ewma_alpha,
                SchedulerKnobs::default().predict_history_size,
            )),
            predicted_delays: RwLock::new(HashMap::new()),
            predicted_defer_total: Arc::new(AtomicU64::new(0)),
            external_io_backpressure: Arc::new(ParkingMutex::new(0.0)),
            dirty_backlog: Arc::new(AtomicI64::new(0)),
            runtime_handles: None,
            in_flight: RwLock::new(HashMap::new()),
            in_flight_seq: AtomicU64::new(0),
            admitted_total: Arc::new(AtomicU64::new(0)),
            released_total: Arc::new(AtomicU64::new(0)),
            reclaimed_total: Arc::new(AtomicU64::new(0)),
            // 2026-10-09 治本 S2：全局停摆自愈状态
            stall_recovery_total: Arc::new(AtomicU64::new(0)),
            stall_overdue_rounds: Arc::new(AtomicU32::new(0)),
            stall_idle_rounds: Arc::new(AtomicU32::new(0)),
            last_admit_ms: Arc::new(AtomicU64::new(0)),
            peak_in_flight: Arc::new(AtomicUsize::new(0)),
            last_stall_recovery: Arc::new(AtomicU64::new(0)),
            process_queue_nudge: Arc::new(AtomicBool::new(false)),
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
    pub fn with_category_concurrency(self, config: CategoryConcurrency) -> Self {
        *self.max_concurrency.write() = config;
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
    pub fn with_knobs(self, knobs: SchedulerKnobs) -> Self {
        self.profile_store.set_alpha(knobs.profile_ewma_alpha);
        // 重建负载预测器以采用新的 alpha / 历史窗口
        *self.load_predictor.write() =
            LoadPredictor::new(knobs.predict_ewma_alpha, knobs.predict_history_size);
        *self.knobs.write() = knobs;
        self
    }

    /// 运行时热更运行时旋钮（config reloader 调用）。
    ///
    /// 画像 EWMA 系数与负载预测器窗口仅影响此后新建的画像/预测状态，
    /// 已存在的 profile_store / load_predictor 不重建（与启动注入行为一致）。
    pub fn update_knobs(&self, knobs: SchedulerKnobs) {
        *self.knobs.write() = knobs;
    }

    /// 运行时热更各分类最大并发度（config reloader 调用）。
    /// 下一次准入判定即生效；已运行任务的槽位不受影响。
    pub fn update_category_concurrency(&self, config: CategoryConcurrency) {
        *self.max_concurrency.write() = config;
    }

    /// 运行时热更任务执行周期（config reloader 调用）。
    ///
    /// 直接改写任务元数据的基准 `interval` 并重置自适应偏离；
    /// 任务本次执行完成后按新周期重排（`current_interval_secs` 重置为新基准）。
    /// 返回 false 表示任务不存在。
    pub fn update_interval(&self, task_id: &str, interval: Duration) -> bool {
        let mut tasks = self.tasks.write();
        match tasks.get_mut(task_id) {
            Some(meta) => {
                meta.interval = interval;
                meta.current_interval_secs = interval.as_secs();
                meta.adaptive_deviation = 1.0;
                true
            }
            None => false,
        }
    }

    /// 查询任务当前基准周期（config reloader 推断单位用）
    pub fn task_interval(&self, task_id: &str) -> Option<Duration> {
        self.tasks.read().get(task_id).map(|m| m.interval)
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

    /// 某分类当前**最老在飞任务**的年龄（秒）；无在飞任务时返回 0。
    ///
    /// 用于「分类卡死保底」判定：以 `in_flight` 登记表为**唯一事实来源**，
    /// 不依赖可能失真的 `running_by_category` 计数（见 `reconcile_counters`）。
    pub fn category_oldest_in_flight_secs(&self, cat: TaskCategory) -> u64 {
        let in_flight = self.in_flight.read();
        in_flight
            .values()
            .filter(|e| e.category == cat)
            .map(|e| e.started_at.elapsed().as_secs())
            .max()
            .unwrap_or(0)
    }

    /// 某分类当前在飞任务数，**以 `in_flight` 登记表实时统计**。
    ///
    /// 准入判据必须用这个而不是 [`Self::running_count`]（读`running_by_category`）：
    /// 后者是独立维护的可变计数器，跨锁竞争下会与登记表漂移，漂移后会把分类
    /// 永久判成「已满」而把所有任务拒之门外（2026-10-09 实证）。
    fn category_running_count(&self, cat: TaskCategory) -> u32 {
        let in_flight = self.in_flight.read();
        in_flight.values().filter(|e| e.category == cat).count() as u32
    }

    /// 读取某分类的卡死保底阈值（秒）；0 表示未设置。
    fn category_stall_threshold(&self, cat: TaskCategory) -> u64 {
        self.category_stall_threshold_secs
            .read()
            .get(&cat)
            .copied()
            .unwrap_or(0)
    }

    /// 该分类是否已「卡死」（最老在飞任务年龄超过阈值）。
    fn category_is_stalled(&self, cat: TaskCategory) -> bool {
        let threshold = self
            .category_stall_threshold_secs
            .read()
            .get(&cat)
            .copied()
            .unwrap_or(0);
        if threshold == 0 {
            return false;
        }
        self.category_oldest_in_flight_secs(cat) >= threshold
    }

    /// 设置某分类的「卡死保底」阈值（秒）；传 0 关闭该分类的保底放行。
    ///
    /// 供 main.rs 按分类装配：Monitor 类设 [`CATEGORY_STALL_FALLBACK_SECS`]，
    /// 其余分类不设（保持原有严格并发语义不变）。
    pub fn set_category_stall_threshold(&self, cat: TaskCategory, secs: u64) {
        self.category_stall_threshold_secs.write().insert(cat, secs);
    }

    /// 以 `in_flight` 登记表为唯一事实来源，重建 `running_by_category`。
    ///
    /// 为什么需要（2026-10-09 生产实证）：`running_by_category` 与 `in_flight`
    /// 是两套独立维护的状态——准入时 +1、释放/回收时 -1。任一路径漏减（或
    /// 强回收与正常释放跨锁竞争）都会让两者**永久漂移**，实测出现
    /// `monitor=4/2`（超限）而 `在飞=1`、`最老在飞=-` 的自相矛盾状态：
    /// 计数超限 ⇒ 准入被拒 ⇒ 监控任务永远排不上；`最老在飞=-` 又让卡死保底
    /// 判定失效（读的是登记表而非计数器），两条路一起堵死。
    ///
    /// 改为**每次心跳从登记表重算**，漂移最多存活一个心跳周期（30s），
    /// 且 `最老在飞` 与 `在飞` 与准入判据三者恒等���。
    fn reconcile_counters(&self) {
        let mut by_cat: HashMap<TaskCategory, u32> = HashMap::new();
        {
            let in_flight = self.in_flight.read();
            for e in in_flight.values() {
                *by_cat.entry(e.category).or_insert(0) += 1;
            }
        }
        let mut running = self.running_by_category.write();
        for cat in [
            TaskCategory::Crawl,
            TaskCategory::Persistence,
            TaskCategory::Monitor,
            TaskCategory::Network,
            TaskCategory::Federation,
            TaskCategory::Tracker,
        ] {
            let truth = by_cat.get(&cat).copied().unwrap_or(0);
            let recorded = running.get(&cat).copied().unwrap_or(0);
            if recorded != truth {
                warn!(
                    "[task_scheduler] 分类计数漂移自愈: {:?} 记录={} 实际在飞={}，按登记表校正",
                    cat, recorded, truth
                );
                running.insert(cat, truth);
            }
        }
    }

    /// 查询指定分类最大并发度
    pub fn max_concurrency_for(&self, cat: TaskCategory) -> u32 {
        self.max_concurrency.read().max_for(cat)
    }

    /// 当前「在飞」任务数（= 已准入但执行体尚未返回）
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.read().len()
    }

    /// 累计被强制回收的泄漏槽位数；> 0 表示本进程发生过槽位泄漏
    pub fn reclaimed_total(&self) -> u64 {
        self.reclaimed_total.load(Ordering::Relaxed)
    }

    /// 最老的「在飞」任务：(任务名, 在飞秒数, 分类)。无在飞任务时为 None。
    fn oldest_in_flight(&self) -> Option<(String, u64, TaskCategory)> {
        let in_flight = self.in_flight.read();
        in_flight
            .values()
            .max_by_key(|e| e.started_at.elapsed())
            .map(|e| (e.name.clone(), e.started_at.elapsed().as_secs(), e.category))
    }

    /// 六个分类的 `名称=running/max` 简报（供心跳/诊断；此前只打 4 类，
    /// 恰好漏掉最容易出事的 federation / tracker）。
    fn category_brief(&self) -> String {
        let running = self.running_by_category.read();
        let mut parts: Vec<String> = Vec::with_capacity(6);
        for (cat, name) in [
            (TaskCategory::Crawl, "crawl"),
            (TaskCategory::Persistence, "persistence"),
            (TaskCategory::Monitor, "monitor"),
            (TaskCategory::Network, "network"),
            (TaskCategory::Federation, "federation"),
            (TaskCategory::Tracker, "tracker"),
        ] {
            parts.push(format!(
                "{}={}/{}",
                name,
                running.get(&cat).unwrap_or(&0),
                self.max_concurrency.read().max_for(cat)
            ));
        }
        parts.join(", ")
    }

    /// 回收「超龄在飞」槽位，返回本次回收数量。
    ///
    /// 「超龄」= 在飞时长 > 该任务 `timeout` × `knobs.stale_slot_factor`（默认 1.5）。
    /// 正常任务在 `timeout` 到点即被 `tokio::time::timeout` 取消并归还槽位；
    /// 超过 1.5 倍仍不返回，意味着**取消机制本身失效**（worker 不再被 poll，
    /// 或卡在同步阻塞段），此时执行体的 Drop 永远不会触发，槽位若不强制回收，
    /// 该分类会被永久占满，`running < max + 超额` 判据恒假 ⇒ 分类永久封印。
    ///
    /// 与 [`CategorySlotGuard`] 通过 `in_flight` 登记互斥，保证一个槽位只释放一次。
    ///
    /// `cat = None` 表示巡检全部分类（watchdog 线程使用）。
    pub fn reclaim_stale_slots(&self, cat: Option<TaskCategory>) -> u32 {
        let (stale_enabled, factor_knob) = {
            let knobs = self.knobs.read();
            (knobs.stale_slot_reclaim_enabled, knobs.stale_slot_factor)
        };
        if !stale_enabled {
            return 0;
        }
        let factor = factor_knob.max(1.0) as f64;

        // ① 只读扫描：避免持写锁做耗时判断
        let stale: Vec<(u64, InFlightTask, f64)> = {
            let in_flight = self.in_flight.read();
            in_flight
                .iter()
                .filter(|(_, e)| cat.is_none_or(|c| e.category == c))
                .filter_map(|(token, e)| {
                    let age = e.started_at.elapsed().as_secs_f64();
                    // 判据 = min(任务自身预算 × factor, 按任务分级的绝对上限)。
                    //
                    // 2026-10-09 方案A（分级，取代此前的单一 240s 上限）：
                    // 原来的 240s 对**所有**任务一视同仁，把默认任务
                    // （timeout=300s）的真实阈值 450s 压到 240s —— 配置项
                    // `stale_slot_factor=1.5` 被静默架空，日志打出「已在飞 240s >
                    // 超时 300s×1.5」这种自相矛盾判据，无法区分「真泄漏」与「只是慢」。
                    // 但也不能整体放开到 600s：tick 级任务（timeout 30~120s）
                    // 挂死数百秒本身就是缺陷，必须有硬兜底，否则句柄/内存会累积
                    // （.52 曾观测 5.89GB 提交内存）。
                    //
                    // 故按**任务自身 timeout** 分级（不是按分类——同分类内既有
                    // tick 也有长任务）：
                    //   · tick 级（timeout ≤ `RECLAIM_TICK_TIMEOUT_SECS`）：保留 240s
                    //     硬上限，行为与 b99bf4e 一致，不回退该缺陷修复；
                    //   · 长任务（timeout > 阈值）：**不设绝对上限**，只用
                    //     `timeout × factor`。bootstrap / range 反熵 / 清单重建
                    //     这类任务 timeout 本就 300~900s，240s 判据会把它们
                    //     在正常执行途中误杀（实测在飞 240~268s 被误回收）。
                    const RECLAIM_ABS_CAP_SECS: f64 = 240.0;
                    const RECLAIM_TICK_TIMEOUT_SECS: f64 = 180.0;
                    let own_budget = e.timeout.as_secs_f64() * factor;
                    let cap = if e.timeout.as_secs_f64() <= RECLAIM_TICK_TIMEOUT_SECS {
                        Some(RECLAIM_ABS_CAP_SECS)
                    } else {
                        None
                    };
                    let is_stale = match cap {
                        Some(c) => age > own_budget.min(c),
                        None => age > own_budget,
                    };
                    if is_stale {
                        Some((*token, e.clone(), age))
                    } else {
                        None
                    }
                })
                .collect()
        };
        if stale.is_empty() {
            return 0;
        }

        // ② 摘除登记（与 CategorySlotGuard::drop 竞争，谁摘到谁负责递减）
        let mut reclaimed_by_cat: HashMap<TaskCategory, u32> = HashMap::new();
        let mut abort_handles: Vec<tokio::task::AbortHandle> = Vec::new();
        {
            let mut in_flight = self.in_flight.write();
            for (token, entry, age_secs) in &stale {
                if let Some(removed) = in_flight.remove(token) {
                    if let Some(h) = removed.abort_handle {
                        abort_handles.push(h);
                    }
                } else {
                    continue;
                }
                *reclaimed_by_cat.entry(entry.category).or_insert(0) += 1;
                error!(
                    "[task_scheduler] 槽位泄漏：任务 {}（id={}）已在飞 {:.0}s > 超时 {:.0}s×{:.1}，\
                     强制回收槽位（{:?}）。执行体已不可能返回，不回收则该分类将被永久占满",
                    entry.name,
                    entry.task_id,
                    age_secs,
                    entry.timeout.as_secs_f64(),
                    factor,
                    entry.category
                );
            }
        }

        // ②.5 尽力中止仍在跑的执行体（2026-10-09 新增）。
        // 此前只回收计数不中止 ⇒ 「账面并发 = max，实际并发 = max + 泄漏数」，
        // 放行新任务时真实并发已超限，资源继续恶化。卡在 await 的执行体会立即
        // 停止；卡在同步阻塞段的 abort 无效，但登记已摘除，其 Drop 不会重复减计数。
        for h in &abort_handles {
            h.abort();
        }
        if !abort_handles.is_empty() {
            debug!(
                "[task_scheduler] 已中止 {} 个泄漏执行体（回收槽位后）",
                abort_handles.len()
            );
        }

        // ③ 递减分类计数
        let mut total = 0u32;
        {
            let mut running = self.running_by_category.write();
            for (c, n) in reclaimed_by_cat {
                if let Some(cnt) = running.get_mut(&c) {
                    *cnt = cnt.saturating_sub(n);
                }
                total += n;
            }
        }
        if total > 0 {
            self.reclaimed_total
                .fetch_add(total as u64, Ordering::Relaxed);
        }
        total
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
        // P0: 槽位泄漏巡检需要访问调度器本体（in_flight 登记表 / 分类计数）
        let watchdog_scheduler = self.clone();
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
                    // P0: 槽位泄漏巡检 —— 运行在本独立 OS 线程上，不依赖任何 tokio runtime，
                    // 因此即使所有 runtime 都被同步段占死，也能回收被永久占用的分类槽。
                    // 这是「执行体永不返回 ⇒ 槽位永不释放」的唯一出路。
                    let reclaimed = watchdog_scheduler.reclaim_stale_slots(None);
                    if reclaimed > 0 {
                        error!(
                            "[task_scheduler] WATCHDOG: 本轮强制回收 {} 个泄漏槽位（累计 {}），在飞剩 {}",
                            reclaimed,
                            watchdog_scheduler.reclaimed_total(),
                            watchdog_scheduler.in_flight_count()
                        );
                    }
                    // P0/治本 S2（2026-10-09）：全局停摆探测 + 自愈。
                    //
                    // 必须在 `reclaim_stale_slots` **之后**执行：槽位回收只摘登记、
                    // 不重驱动队列，2026-10-09 事故正是「回收 33 个槽位后在飞/队列
                    // 恒定 41 分钟」—— 本方法才是唯一能让系统自己爬起来的出路。
                    watchdog_scheduler.detect_and_recover_global_stall();
                }
            })
            .expect("failed to spawn watchdog thread");

        {
            let mc = self.max_concurrency.read();
            info!(
                "[task_scheduler] 智能任务调度中心已启动（分级并发: crawl={}, persistence={}, monitor={}, network={}, federation={}, tracker={}）",
                mc.crawl, mc.persistence, mc.monitor, mc.network, mc.federation, mc.tracker
            );
        }
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

    /// 全局停摆自愈阈值（2026-10-09 治本 S2）。
    /// 全局停摆探测 + 自愈（2026-10-09 治本 S2）。
    ///
    /// 2026-10-09 事故：08:10~08:21 watchdog 强制回收 33 个槽位后，
    /// `crawl=0/8` 与 `队列待执行=21` 恒定 **41 分钟**，无人恢复。
    /// 根因是任务体挂在 `.await` 上无人唤醒，而「执行完成后重新入队」
    /// 的代码在 `fut` 内部 —— 执行体永不返回 ⇒ 永不重新入队。
    ///
    /// 本方法给出**唯一能自愈**的路径，运行在 watchdog 独立OS 线程上
    /// （不依赖任何 tokio runtime，故runtime 全停时仍可执行）：
    /// 1. abort 全部在飞执行体（`0da7730` 已回填 `abort_handle`）
    /// 2. 把队列中所有项的 `scheduled_at` 重置为 now
    /// 3. 下一tick 让 `process_queue` 正常取走
    ///
    /// 判据（任一成立即触发）：
    /// - 【判据1】队列中有已到期项却未被取走，且该状态连续命中
    ///   `STALL_OVERDUE_ROUNDS` 轮（watchdog 每 `WATCHDOG_CHECK_INTERVAL_SECS`
    ///   = 30s 一轮 ⇒ 60s ≈ 连续 2 轮）⇒ 调度主循环已停。
    /// - 【判据2】在飞为 0、**队列无到期项**、队列最晚到期项距今超过
    ///   `STALL_IDLE_GAP_SECS`，连续命中 `STALL_IDLE_ROUNDS` 轮。
    fn detect_and_recover_global_stall(&self) -> bool {
        let in_flight = self.in_flight_count();
        let queue_len = self.queue.read().len();
        let overdue = self.queue_overdue_count();
        let span = self.queue_delay_span();
        let (min_gap, max_gap) = span.unwrap_or((0, 0));

        if queue_len == 0 {
            // 队列空 ⇒ 无积压。顺带清零两个判据的连续计数，避免残留导致误判。
            self.stall_overdue_rounds.store(0, Ordering::Relaxed);
            self.stall_idle_rounds.store(0, Ordering::Relaxed);
            return false;
        }

        // 判据1：已到期项积压 ⇒ 调度主循环（tick）已停。
        // 用连续命中轮数表达「持续」，单轮命中不足以判定（可能有正常抖动）。
        let tripped_by_overdue = if overdue > 0 && min_gap == 0 {
            let n = self.stall_overdue_rounds.fetch_add(1, Ordering::Relaxed) + 1;
            n >= STALL_OVERDUE_ROUNDS
        } else {
            self.stall_overdue_rounds.store(0, Ordering::Relaxed);
            false
        };
        // 判据2：**在飞高水位归零** + 队列全在未来 ⇒ 执行体全挂死。
        //
        // `peak_in_flight` 是唯一能区分「事故态」与「正常空闲态」的信号：
        // 正常空闲期从没有过在飞（高水位=0），事故态则曾有大量在飞后归零。
        // 详见 STALL_PEAK_IN_FLIGHT_MIN 常量注释。
        let tripped_by_idle_queue = if in_flight == 0
            && overdue == 0
            && self
                .peak_in_flight
                .load(Ordering::Relaxed)
                .saturating_sub(in_flight)
                >= STALL_PEAK_IN_FLIGHT_MIN
        {
            let n = self.stall_idle_rounds.fetch_add(1, Ordering::Relaxed) + 1;
            n >= STALL_IDLE_ROUNDS
        } else {
            self.stall_idle_rounds.store(0, Ordering::Relaxed);
            false
        };
        if !(tripped_by_overdue || tripped_by_idle_queue) {
            return false;
        }

        // 触发间隔限流：避免反复触发把正常的长任务误当成停摆。
        let last = self.last_stall_recovery.load(Ordering::Relaxed);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // 【豁免1】系统仍在正常调度：近期（宽限期内）还有任务被准入。
        // 启动/预热期 `in_flight==0` 属正常，若无此豁免会误报。
        let since_admit_ms = now_ms.saturating_sub(self.last_admit_ms.load(Ordering::Relaxed));
        let still_scheduling = since_admit_ms < STALL_STARTUP_GRACE_SECS * 1000;
        if still_scheduling {
            debug!(
                "[task_scheduler] STALL 判定豁免：最近准入距今 {}ms（< {}ms 宽限期），系统仍在正常调度",
                since_admit_ms, STALL_STARTUP_GRACE_SECS * 1000
            );
            return false;
        }
        if last != 0 && now_ms.saturating_sub(last) / 1000 < STALL_MIN_TRIGGER_INTERVAL_SECS {
            return false;
        }

        let total = self.stall_recovery_total.fetch_add(1, Ordering::Relaxed) + 1;
        self.last_stall_recovery.store(now_ms, Ordering::Relaxed);
        error!(
            "[task_scheduler] STALL 自愈触发（第 {} 次）: 在飞={} 队列={} 已到期积压={} 队列延迟=+{}s..+{}s。\
             执行体挂在 .await 上无人唤醒（重新入队代码在 fut 内部，永不执行）⇒ 立即 abort 全部执行体并重置队列",
            total, in_flight, queue_len, overdue, min_gap, max_gap
        );

        // ① abort 全部在飞执行体：释放它们的 socket / DB 占用，让下一次执行
        //    拿到干净环境。注意 abort 只对已 poll 过的 future 生效；
        //    若执行体正卡在同步段，abort 会在下一次 await 点生效。
        let aborted = self.abort_all_in_flight();

        // ② 队列全部重置为 now：清掉「停在未来」的远期项。
        //    必须在 abort 之后做——执行体被 abort 时也会试图重新入队
        //    （若它恰好已poll 到末尾），避免被覆盖掉。
        let reset = self.reset_queue_due_now();

        // ③ 主动触发一轮准入：下一 tick（≤100ms）process_queue 会取走全部到期项。
        //    这里显式 nudge 一次，缩短自愈延迟。
        self.nudge_process_queue();

        error!(
            "[task_scheduler] STALL 自愈完成（第 {} 次）: abort {} 个执行体，重置 {} 个队列项为立即到期，\
             调度主循环将在下一 tick 重新拉起全部任务",
            total, aborted, reset
        );
        true
    }

    /// abort 全部在飞登记项对应的执行体（2026-10-09 治本 S2）。
    fn abort_all_in_flight(&self) -> usize {
        let handles: Vec<_> = {
            let in_flight = self.in_flight.read();
            in_flight
                .values()
                .filter_map(|e| e.abort_handle.clone())
                .collect()
        };
        let n = handles.len();
        for h in &handles {
            h.abort();
        }
        n
    }

    /// 把队列中所有项的 `scheduled_at` 重置为 now（2026-10-09 治本 S2）。
    fn reset_queue_due_now(&self) -> usize {
        let mut q = self.queue.write();
        let n = q.len();
        let now = Instant::now();
        // BinaryHeap 无 `iter_mut`，重建为等价集合（保留原 task_id/priority/seq）。
        let items: Vec<ScheduledItem> = q.drain().collect();
        for mut it in items {
            it.scheduled_at = now;
            q.push(it);
        }
        n
    }

    /// 发起一次立即重跑 `process_queue`（缩短自愈延迟，2026-10-09 治本 S2）。
    ///
    /// `process_queue` 已在主循环每 100ms 调用一次；本方法在自愈后请求
    /// 主循环「下一次心跳窗口内额外跑一轮」，把恢复延迟从 ≤100ms 压到 ~0。
    fn nudge_process_queue(&self) {
        self.process_queue_nudge.store(true, Ordering::Release);
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
            // 读取后在 await 前释放锁守卫
            let (adaptive_enabled, recalc_period) = {
                let knobs = self.knobs.read();
                (
                    knobs.adaptive_interval_enabled,
                    knobs.adaptive_recalc_ticks.max(1) as u64,
                )
            };
            if adaptive_enabled && adaptive_tick.is_multiple_of(recalc_period) {
                self.recalc_adaptive_intervals();
            }
            if heartbeat.elapsed() >= SCHEDULER_HEARTBEAT_INTERVAL {
                // 先按`in_flight` 登记表校正分类计数，消除准入/释放/强回收
                // 三路维护留下的漂移（本轮判据与打印都用校正后的值）。
                self.reconcile_counters();
                let queue_len = self.queue.read().len();
                // 2026-10-09 治本 S1：把「队列里有没有真正会执行的任务」打进心跳。
                // 旧日志只有 `队列待执行=N`，无法区分「N 个正常等待」与
                // 「N 个全被推到未来、一个都不会执行」——后者即本次事故全貌。
                let (overdue, span) = (self.queue_overdue_count(), self.queue_delay_span());
                let queue_health = match (overdue, span) {
                    (0, Some((min_d, max_d))) => {
                        format!(
                            "已到期={} 最早到期=+{}s 最晚到期=+{}s",
                            overdue, min_d, max_d
                        )
                    }
                    (0, None) => "队列空".to_string(),
                    (n, _) => format!(
                        "⚠ 已到期却未被执行={} 最早到期=+{:?}s",
                        n,
                        span.map(|(a, _)| a).unwrap_or(0)
                    ),
                };
                let brief = self.category_brief();
                let in_flight = self.in_flight_count();
                let oldest = match self.oldest_in_flight() {
                    Some((name, secs, cat)) => format!("最老在飞={}({:?} {}s)", name, cat, secs),
                    None => "最老在飞=-".to_string(),
                };
                // 对账不变式：累计准入 − 累计释放 − 强制回收 = 当前在飞
                info!(
                    "[task_scheduler] 调度器心跳: 队列待执行={}（{}）, 运行中[{}], 在飞={}（准入{} − 释放{} − 回收{}）, {}",
                    queue_len,
                    queue_health,
                    brief,
                    in_flight,
                    self.admitted_total.load(Ordering::Relaxed),
                    self.released_total.load(Ordering::Relaxed),
                    self.reclaimed_total.load(Ordering::Relaxed),
                    oldest,
                );
                heartbeat = Instant::now();
                // P1-3: 更新原子心跳时间戳，供独立 watchdog 线程检测
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                self.last_heartbeat.store(now_ms, Ordering::Relaxed);
                // 2026-10-09 治本 S2：自愈后立即重跑一轮，不等下一个 100ms tick。
                // 闸门在 watchdog 线程的「主循环心跳已停」分支（不可达，因为
                // 该分支本身就是心跳停止才进入），故此处只消费 nudge 标记。
                if self.process_queue_nudge.swap(false, Ordering::AcqRel) {
                    Self::process_queue(self.clone()).await;
                    continue;
                }
            }
            Self::process_queue(self.clone()).await;
        }
    }

    /// 队列延迟观测（2026-10-09 治本 S1）。
    ///
    /// 返回 `(最早到期距今秒, 最晚到期距今秒)`。队列为空时返回 `None`。
    ///
    /// **为什么必须暴露**：2026-10-09 事故里心跳只打 `队列待执行=21`，
    /// 运维无法区分「21 个任务在正常等待」与「21 个任务全被推到未来、
    /// 一个都不会执行」——后者正是 41 分钟全局停摆的全貌。这是事故
    /// 不可见的直接原因，本方法补上这个判据。
    pub fn queue_delay_span(&self) -> Option<(u64, u64)> {
        let q = self.queue.read();
        let now = Instant::now();
        let mut min_d = u64::MAX;
        let mut max_d = 0u64;
        // BinaryHeap 无 iter 全序遍历保证，此处仅做统计取min/max，不依赖顺序。
        for item in q.iter() {
            // scheduled_at 可能已被重置为 now（停摆自愈后），负值夹到 0。
            let d = item.scheduled_at.saturating_duration_since(now).as_secs();
            min_d = min_d.min(d);
            max_d = max_d.max(d);
        }
        if min_d == u64::MAX {
            None
        } else {
            Some((min_d, max_d))
        }
    }

    /// 队列中「已经到期却仍未被取出」的任务数（2026-10-09 治本 S1）。
    ///
    /// 正常恒为 0 —— `process_queue` 每 tick（100ms）必然取走全部到期项。
    /// 若持续 > 0，说明调度主循环已停摆（tick 断了或被卡住），
    /// 此时即使 `in_flight == 0` 也不代表系统健康。
    pub fn queue_overdue_count(&self) -> usize {
        let q = self.queue.read();
        let now = Instant::now();
        q.iter()
            .filter(|item| item.scheduled_at <= now)
            .map(|item| item.task_id.clone())
            .collect::<std::collections::HashSet<_>>()
            .len()
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
        if scheduler.knobs.read().predictive_scheduling_enabled {
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

            // 分级并发控制：按任务分类限流（含饥饿补偿 + 槽位泄漏逃生阀 + 卡死保底）
            {
                let cat = meta.category;
                // 计数以 `in_flight` 登记表为事实来源实时统计，避免计数器漂移
                // 把分类永久判成「已满」（见 `reconcile_counters`）。
                let running = scheduler.category_running_count(cat);
                let max = scheduler.max_concurrency_for(cat);
                // 卡死保底是否已放行本次准入（放行则跳过饥饿补偿/排队分支）
                let mut stall_admitted = false;
                if running >= max {
                    // 卡死保底：某分类被慢任务实质性占死时（最老在飞超阈值），
                    // 保底放行——**监控必须永远能拿到数据**，否则故障不可见。
                    // 这是 2026-10-09「Persistence 霸占唯一槽 → 面板 15s 超时」
                    // 事故的直接根治点：此前 Monitor 只有饥饿补偿（需连续被拒
                    // 6 轮 = 30s）才放行，且该补偿与计数器漂移叠加后完全失效。
                    if scheduler.category_is_stalled(cat) {
                        warn!(
                            "[task_scheduler] 分类卡死保底放行（{:?} {}/{}，最老在飞 {}s ≥ 阈值 {}s）: {}",
                            cat,
                            running,
                            max,
                            scheduler.category_oldest_in_flight_secs(cat),
                            scheduler.category_stall_threshold(cat),
                            meta.name
                        );
                        scheduler.concurrency_starve.write().remove(&item.task_id);
                        stall_admitted = true;
                    }
                }
                if running >= max && !stall_admitted {
                    // 饥饿补偿：分类槽被长 await 任务长期占满时，短任务会无限期排不到执行。
                    // 连续被拒 CONCURRENCY_STARVE_ROUNDS 轮后，允许在硬上限（max + 超额）内
                    // 准入一次，避免「永远不执行」；超额幅度很小（+2），不会打爆资源。
                    let rounds = {
                        let mut m = scheduler.concurrency_starve.write();
                        let n = m.entry(item.task_id.clone()).or_insert(0);
                        *n += 1;
                        *n
                    };
                    let oversubscribe = rounds >= CONCURRENCY_STARVE_ROUNDS
                        && running < max + CONCURRENCY_OVERSUBSCRIBE;

                    // P0 逃生阀（2026-09-21 远端 51 事故）：硬上限也顶满时，原判据
                    // `running < max + OVER` 恒为假 ⇒ 分类被**永久封印**（实测联邦 runtime
                    // 静默后 10 个在飞任务永久占槽，Federation 分类封印到进程结束，
                    // 日志刷了 5 万条）。此处先尝试回收「超龄在飞」槽位：只要存在泄漏，
                    // 无论running 顶到多少都能解开，不再自锁。
                    let reclaimed = if !oversubscribe && running >= max + CONCURRENCY_OVERSUBSCRIBE
                    {
                        scheduler.reclaim_stale_slots(Some(cat))
                    } else {
                        0
                    };

                    if !oversubscribe && reclaimed == 0 {
                        // 无泄漏可回收 ⇒ 属于真过载，延迟重排。
                        // 日志限流：前 CONCURRENCY_STARVE_ROUNDS 轮逐轮打，之后每 60 轮
                        // 一次（5s × 60 = 5 分钟），避免像事故那样单点刷 5 万行日志。
                        if rounds <= CONCURRENCY_STARVE_ROUNDS || rounds.is_multiple_of(60) {
                            debug!(
                                "[task_scheduler] 分类并发已满（{:?} {}/{}），延迟: {}（已连续 {} 轮）",
                                cat, running, max, meta.name, rounds
                            );
                        }
                        scheduler.schedule_task(
                            &item.task_id,
                            Instant::now() + CONCURRENCY_FULL_DELAY,
                            item.priority,
                        );
                        continue;
                    }
                    if reclaimed > 0 {
                        warn!(
                            "[task_scheduler] 已强制回收 {} 个泄漏槽位（{:?} {}/{}），本次准入: {}（已连续延迟 {} 轮）",
                            reclaimed, cat, running, max, meta.name, rounds
                        );
                    } else {
                        warn!(
                            "[task_scheduler] 分类并发饥饿补偿准入（{:?} {}/{}，已连续延迟 {} 轮）: {}",
                            cat, running, max, rounds, meta.name
                        );
                    }
                }
                // 准入成功：清零饥饿计数
                scheduler.concurrency_starve.write().remove(&item.task_id);
            }

            // 资源感知准入控制（默认关闭；关闭后 admission_decide 恒为 Allow，行为不变）
            {
                let sample = scheduler.effective_sample();
                let delayed = *scheduler
                    .admission_delays
                    .read()
                    .get(&item.task_id)
                    .unwrap_or(&0);
                match admission_decide(&scheduler.knobs.read(), &meta, sample, delayed) {
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
            if scheduler.knobs.read().predictive_scheduling_enabled
                && is_predictive_deferrable(&meta)
            {
                let predicted_over = {
                    let p = scheduler.load_predictor.read();
                    let knobs = scheduler.knobs.read();
                    let lookahead = knobs.predict_lookahead_ticks.max(1);
                    (1..=lookahead).any(|t| predictive_over_threshold(&knobs, p.predict(t)))
                };
                if predicted_over {
                    let delayed = *scheduler
                        .predicted_delays
                        .read()
                        .get(&item.task_id)
                        .unwrap_or(&0);
                    if delayed >= scheduler.knobs.read().admission_max_delay_ticks {
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
        let (adaptive_enabled, target, min_ratio, max_ratio) = {
            let knobs = self.knobs.read();
            (
                knobs.adaptive_interval_enabled,
                knobs.adaptive_target_load,
                knobs.adaptive_min_ratio,
                knobs.adaptive_max_ratio,
            )
        };
        if !adaptive_enabled {
            return;
        }
        let sample = self.effective_sample();
        let load = sample.cpu.max(sample.io);
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
        // 登记「在飞」+ 占槽。登记表与计数必须同时变更；释放端见 CategorySlotGuard::drop。
        let token = scheduler.in_flight_seq.fetch_add(1, Ordering::Relaxed);
        scheduler.in_flight.write().insert(
            token,
            InFlightTask {
                task_id: item.task_id.clone(),
                name: meta.name.clone(),
                category,
                started_at: Instant::now(),
                timeout: meta.timeout,
                // spawn 之后回填（见本函数末尾）：此处先占位，
                // 使「登记已存在但还没有句柄」这个窗口里回收逻辑不会 panic。
                abort_handle: None,
            },
        );
        let running_now = {
            let mut running = scheduler.running_by_category.write();
            let cnt = running.entry(category).or_insert(0);
            *cnt += 1;
            *cnt
        };
        scheduler.admitted_total.fetch_add(1, Ordering::Relaxed);
        // 2026-10-09 治本 S2：维护在飞高水位。
        //
        // 这是「全局停摆自愈」判据2 的核心信号：正常空闲期高水位恒为 0，
        // 事故态则是「曾有大量在飞、之后归零」。用 fetch_max 记录峰值。
        scheduler
            .peak_in_flight
            .fetch_max(running_now as usize, Ordering::Relaxed);
        // 打点最近准入时刻：供停摆判定做「系统仍在调度」豁免。
        scheduler.last_admit_ms.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            Ordering::Relaxed,
        );
        // 准入埋点：此前只打 task_id，running 不可见 ⇒ 槽位爬升轨迹无法从日志还原
        debug!(
            "[task_scheduler] 任务准入: {}（{:?} {}/{}）",
            meta.name,
            category,
            running_now,
            scheduler.max_concurrency_for(category)
        );

        // RAII 守卫：无论正常结束/超时/失败/panic，分类并发槽位只释放一次
        let slot_guard = CategorySlotGuard {
            scheduler: scheduler.clone(),
            category,
            token,
        };

        let task_id = item.task_id.clone();

        let runtime_handle = scheduler.runtime_handles.as_ref().map(|h| match category {
            TaskCategory::Crawl | TaskCategory::Network => h.crawler.clone(),
            TaskCategory::Federation => h.federation.clone(),
            TaskCategory::Tracker => h.tracker.clone(),
            TaskCategory::Persistence => h.persistence.clone(),
            // 2026-10-09 治本 S4：Monitor 走**独立 monitor runtime**，
            // 不再与 HTTP(api) 争抢 4 个 worker。事故中 Monitor 堆到 16 个
            // 在飞把api worker 全占死，监控静默 41 分钟。
            TaskCategory::Monitor => h.monitor.clone(),
        });

        // 克隆一份引用供 spawn 之后回填中止句柄用（`scheduler` 本身被 move 进 fut）
        let scheduler_ref = scheduler.clone();

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
            let (adaptive_enabled, jitter_enabled, jitter_ratio) = {
                let knobs = scheduler.knobs.read();
                (
                    knobs.adaptive_interval_enabled,
                    knobs.random_jitter_enabled,
                    knobs.random_jitter_ratio,
                )
            };
            let base_interval = if adaptive_enabled {
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
            let interval_with_ratio =
                apply_interval_jitter(base_interval, jitter_enabled, jitter_ratio);
            scheduler.schedule_task(
                &task_id,
                Instant::now() + interval_with_ratio + Duration::from_secs(jitter_secs),
                meta.priority,
            );
        };

        // 2026-10-09：回填中止句柄，让「强制回收」能真正中止执行体。
        // 必须在 spawn 之后——`JoinHandle::abort_handle()` 才能拿到句柄。
        // 若此刻登记已被 watchdog 回收（极小窗口），则不再回填：
        // 回收方已按「无句柄」处理过，无需补中止。
        // 注意 `scheduler` 已被move 进 `fut`，故用执行前克隆的 `scheduler_ref`。
        let join_handle = match runtime_handle {
            Some(h) => h.spawn(fut),
            None => tokio::spawn(fut),
        };
        let abort_handle = join_handle.abort_handle();
        let mut in_flight = scheduler_ref.in_flight.write();
        if let Some(entry) = in_flight.get_mut(&token) {
            entry.abort_handle = Some(abort_handle);
        }
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
        let max_conc = *self.max_concurrency.read();
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
            adaptive_interval_enabled: self.knobs.read().adaptive_interval_enabled,
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
            in_flight_tasks: self.in_flight_count(),
            category_stats: self.category_stats(),
            reclaimed_slots: self.reclaimed_total(),
        }
    }

    /// 六个分类的 `(名称, running, max, 最老在飞秒数)`，顺序固定，供监控/摘要使用。
    pub fn category_stats(&self) -> Vec<(String, u32, u32, u64)> {
        // 每个分类取该分类内最老的「在飞」任务年龄
        let oldest_by_cat: HashMap<TaskCategory, u64> = {
            let in_flight = self.in_flight.read();
            let mut m: HashMap<TaskCategory, u64> = HashMap::new();
            for e in in_flight.values() {
                let age = e.started_at.elapsed().as_secs();
                let slot = m.entry(e.category).or_insert(0);
                if age > *slot {
                    *slot = age;
                }
            }
            m
        };
        let running = self.running_by_category.read();
        [
            (TaskCategory::Crawl, "crawl"),
            (TaskCategory::Persistence, "persistence"),
            (TaskCategory::Monitor, "monitor"),
            (TaskCategory::Network, "network"),
            (TaskCategory::Federation, "federation"),
            (TaskCategory::Tracker, "tracker"),
        ]
        .into_iter()
        .map(|(cat, name)| {
            (
                name.to_string(),
                *running.get(&cat).unwrap_or(&0),
                self.max_concurrency.read().max_for(cat),
                *oldest_by_cat.get(&cat).unwrap_or(&0),
            )
        })
        .collect()
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

    /// 累计准入执行数（19号 D4：调度器指标导出用）
    pub fn admitted_total(&self) -> u64 {
        self.admitted_total.load(Ordering::Relaxed)
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
    /// 当前在飞任务数（已准入但执行体尚未返回）
    pub in_flight_tasks: usize,
    /// 各分类 `(名称, running, max, 最老在飞秒数)`，六个分类固定顺序
    pub category_stats: Vec<(String, u32, u32, u64)>,
    /// 累计被强制回收的泄漏槽位数（> 0 表示发生过槽位泄漏）
    pub reclaimed_slots: u64,
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

    #[test]
    fn test_update_interval_resets_adaptive_state() {
        let scheduler = TaskScheduler::new();
        scheduler.register(
            TaskMetadata::new("task_a", "Task A", Duration::from_secs(30)),
            || async { Ok(()) },
        );
        assert_eq!(
            scheduler.task_interval("task_a"),
            Some(Duration::from_secs(30))
        );

        // 热更周期：基准 interval 与自适应偏离都应重置
        assert!(scheduler.update_interval("task_a", Duration::from_secs(5)));
        assert_eq!(
            scheduler.task_interval("task_a"),
            Some(Duration::from_secs(5))
        );
        let meta = scheduler
            .list_tasks()
            .into_iter()
            .find(|m| m.id == "task_a")
            .unwrap();
        assert_eq!(meta.current_interval_secs, 5);
        assert_eq!(meta.adaptive_deviation, 1.0);

        // 不存在的任务返回 false
        assert!(!scheduler.update_interval("no_such", Duration::from_secs(1)));
    }

    #[test]
    fn test_update_category_concurrency_hot() {
        let scheduler = TaskScheduler::new();
        let before = scheduler.max_concurrency_for(TaskCategory::Crawl);
        let cc = CategoryConcurrency {
            crawl: before + 7,
            ..CategoryConcurrency::default()
        };
        scheduler.update_category_concurrency(cc);
        assert_eq!(
            scheduler.max_concurrency_for(TaskCategory::Crawl),
            before + 7
        );
        // 其他分类不受影响
        assert_eq!(
            scheduler.max_concurrency_for(TaskCategory::Monitor),
            CategoryConcurrency::default().monitor
        );
    }

    #[test]
    fn test_update_knobs_hot() {
        let scheduler = TaskScheduler::new();
        assert!(!scheduler.summary().adaptive_interval_enabled);
        let knobs = SchedulerKnobs {
            adaptive_interval_enabled: true,
            admission_cpu_threshold: 0.66,
            ..SchedulerKnobs::default()
        };
        scheduler.update_knobs(knobs);
        assert!(scheduler.summary().adaptive_interval_enabled);
        // 准入判定读取的是新旋钮（admission_decide 内部读 knobs）
        let knobs = SchedulerKnobs::default();
        let meta = TaskMetadata::new("k", "K", Duration::from_secs(1));
        let sample = LoadSample { cpu: 0.0, io: 0.0 };
        assert!(matches!(
            admission_decide(&knobs, &meta, sample, 0),
            AdmissionDecision::Allow
        ));
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
        // 2026-10-09 根治：persistence 1 → 2。断言随之更新（原值 1 会让
        // WAL checkpoint / WriteQueue 刷盘 / TRUNCATE / oplog 裁剪 互相关联
        // 任务串行抢唯一槽位，生产实证停摆 2.5 小时）。
        assert_eq!(summary.max_concurrency.persistence, 2);

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

    // ---- P0: 槽位泄漏回收 / 在飞登记（2026-09-21 远端 51 事故回归守卫）----

    /// 造一个「在飞已超龄」的登记项，模拟 runtime 停摆后永不返回的执行体。
    fn insert_stale(s: &Arc<TaskScheduler>, token: u64, cat: TaskCategory, age: Duration) {
        let now = Instant::now();
        s.in_flight.write().insert(
            token,
            InFlightTask {
                task_id: format!("t{token}"),
                name: format!("任务{token}"),
                category: cat,
                // Instant 不能表示"过去"，只能从当前时刻往前退
                started_at: now.checked_sub(age).unwrap_or(now),
                timeout: Duration::from_secs(10),
                abort_handle: None,
            },
        );
        *s.running_by_category.write().entry(cat).or_insert(0) += 1;
    }

    /// 超龄在飞槽位必须能被回收，且回收精确到分类、幂等。
    #[test]
    fn test_reclaim_stale_slots_releases_leaked_slot() {
        let s = Arc::new(TaskScheduler::new());
        // token=1：在飞 600s 远超 timeout(10s)×1.5
        insert_stale(&s, 1, TaskCategory::Federation, Duration::from_secs(60));
        // token=2：刚准入的「新鲜」在飞项，不得被误回收
        s.in_flight.write().insert(
            2,
            InFlightTask {
                task_id: "fresh".to_string(),
                name: "新鲜任务".to_string(),
                category: TaskCategory::Federation,
                started_at: Instant::now(),
                timeout: Duration::from_secs(300),
                abort_handle: None,
            },
        );
        *s.running_by_category
            .write()
            .entry(TaskCategory::Federation)
            .or_insert(0) += 1;

        assert_eq!(s.running_count(TaskCategory::Federation), 2);
        assert_eq!(s.reclaim_stale_slots(Some(TaskCategory::Federation)), 1);
        assert_eq!(
            s.running_count(TaskCategory::Federation),
            1,
            "只回收超龄那一个"
        );
        assert_eq!(s.in_flight_count(), 1);
        assert_eq!(s.reclaimed_total(), 1);
        // 幂等：再扫一次无可回收
        assert_eq!(s.reclaim_stale_slots(Some(TaskCategory::Federation)), 0);
    }

    /// 分类级回收不得越界影响其它分类。
    #[test]
    fn test_reclaim_stale_slots_is_per_category() {
        let s = Arc::new(TaskScheduler::new());
        insert_stale(&s, 1, TaskCategory::Federation, Duration::from_secs(60));
        insert_stale(&s, 2, TaskCategory::Crawl, Duration::from_secs(60));

        assert_eq!(s.reclaim_stale_slots(Some(TaskCategory::Federation)), 1);
        assert_eq!(s.running_count(TaskCategory::Crawl), 1, "其它分类不受影响");
        assert_eq!(s.running_count(TaskCategory::Federation), 0);
        assert_eq!(s.in_flight_count(), 1);
        // 全分类巡检应把剩下那个也收掉
        assert_eq!(s.reclaim_stale_slots(None), 1);
        assert_eq!(s.in_flight_count(), 0);
    }

    /// 开关关闭时不得回收（保留人工关闭的逃生口）。
    #[test]
    fn test_reclaim_respects_disabled_knob() {
        let s = Arc::new(TaskScheduler::new().with_knobs(SchedulerKnobs {
            stale_slot_reclaim_enabled: false,
            ..Default::default()
        }));
        insert_stale(&s, 1, TaskCategory::Federation, Duration::from_secs(60));
        assert_eq!(s.reclaim_stale_slots(None), 0);
        assert_eq!(s.running_count(TaskCategory::Federation), 1);
        assert_eq!(s.in_flight_count(), 1);
    }

    /// 关键回归：回收后执行体的 Drop **不得**重复递减同一槽位。
    #[tokio::test]
    async fn test_slot_guard_does_not_double_release_after_reclaim() {
        let s = Arc::new(TaskScheduler::new());
        insert_stale(&s, 7, TaskCategory::Federation, Duration::from_secs(60));
        assert_eq!(s.running_count(TaskCategory::Federation), 1);

        // watchdog 先判定泄漏并回收
        assert_eq!(s.reclaim_stale_slots(Some(TaskCategory::Federation)), 1);
        assert_eq!(s.running_count(TaskCategory::Federation), 0);

        // 执行体随后（现实中不会发生）Drop：token 已不在登记表 ⇒ 不再是槽位主人
        {
            let _g = CategorySlotGuard {
                scheduler: s.clone(),
                category: TaskCategory::Federation,
                token: 7,
            };
        }
        assert_eq!(
            s.running_count(TaskCategory::Federation),
            0,
            "回收后不得重复递减（否则计数会负向漂移、放大并发）"
        );
        assert_eq!(s.reclaimed_total(), 1);
    }

    /// 正常路径对账：任务跑完 ⇒ 在飞登记清空、槽位归还。
    #[tokio::test]
    async fn test_in_flight_registry_balances_after_normal_run() {
        let s = Arc::new(
            TaskScheduler::new().with_category_concurrency(CategoryConcurrency {
                crawl: 1,
                persistence: 1,
                monitor: 1,
                network: 1,
                federation: 1,
                tracker: 1,
            }),
        );
        let hits = Arc::new(AtomicU64::new(0));
        let h = hits.clone();
        s.register(
            TaskMetadata::new("burst", "对账压测任务", Duration::from_secs(30))
                .with_initial_delay(Duration::from_millis(10))
                .with_jitter(Duration::from_secs(0))
                .with_category(TaskCategory::Federation),
            move || {
                let h = h.clone();
                async move {
                    h.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            },
        );

        s.start();
        tokio::time::sleep(Duration::from_millis(600)).await;
        s.stop();
        // 给主循环一个 tick 收尾
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(hits.load(Ordering::Relaxed) >= 1, "任务至少执行一次");
        assert_eq!(s.in_flight_count(), 0, "正常任务跑完必须清空在飞登记");
        assert_eq!(s.running_count(TaskCategory::Federation), 0, "槽位必须归还");
        assert_eq!(s.reclaimed_total(), 0, "正常路径不应触发任何回收");
        // 分类摘要必须覆盖全部 6 类，且 federation 在列（此前心跳漏掉它）
        let cats = s.category_stats();
        assert_eq!(cats.len(), 6);
        assert!(cats.iter().any(|(n, ..)| n == "federation"));
        assert!(cats.iter().any(|(n, ..)| n == "tracker"));
    }

    /// 造一个「刚准入」的在飞登记项（用于验证不误回收）。
    fn insert_fresh(s: &Arc<TaskScheduler>, token: u64, cat: TaskCategory) {
        s.in_flight.write().insert(
            token,
            InFlightTask {
                task_id: format!("fresh{token}"),
                name: format!("新鲜任务{token}"),
                category: cat,
                started_at: Instant::now(),
                timeout: Duration::from_secs(10),
                abort_handle: None,
            },
        );
        *s.running_by_category.write().entry(cat).or_insert(0) += 1;
    }

    /// 【核心回归】复现 2026-09-21 远端 51 的"永久封印"：
    /// `running` 顶到 `max + 超额` 且存在超龄在飞槽位时，准入必须能被解开；
    /// 若只是"真过载"（在飞都是新鲜的），则照旧延迟，不误杀。
    #[tokio::test]
    async fn test_admission_escape_valve_breaks_permanent_seal() {
        // ---- 场景 A：槽位泄漏 ⇒ 必须能准入 ----
        let s = Arc::new(
            TaskScheduler::new().with_category_concurrency(CategoryConcurrency {
                crawl: 1,
                persistence: 1,
                monitor: 1,
                network: 1,
                federation: 1, // max = 1，硬上限 = max + 超额 2 = 3
                tracker: 1,
            }),
        );
        let hits = Arc::new(AtomicU64::new(0));
        let h = hits.clone();
        s.register(
            TaskMetadata::new("victim", "受害者任务", Duration::from_secs(10))
                .with_jitter(Duration::from_secs(0))
                .non_deferrable()
                .with_category(TaskCategory::Federation),
            move || {
                let h = h.clone();
                async move {
                    h.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            },
        );
        // 3 个超龄在飞（= max + 超额），模拟 runtime 静默后永不返回的联邦任务
        for token in 1..=3 {
            insert_stale(&s, token, TaskCategory::Federation, Duration::from_secs(60));
        }
        assert_eq!(s.running_count(TaskCategory::Federation), 3);

        s.schedule_task("victim", Instant::now(), TaskPriority::Normal);
        TaskScheduler::process_queue(s.clone()).await;

        assert_eq!(
            s.reclaimed_total(),
            3,
            "顶到硬上限时必须触发泄漏回收，否则判据恒假、分类永久封印"
        );
        assert_eq!(
            s.in_flight_count(),
            1,
            "回收 3 个后应只剩本次准入的 1 个在飞"
        );
        assert_eq!(
            s.running_count(TaskCategory::Federation),
            1,
            "3 收回 + 1 准入 = 1"
        );
        assert_eq!(
            s.queue.read().len(),
            0,
            "被解开后任务应当被准入，而不是重新排队"
        );
        s.stop();

        // ---- 场景 B：真过载（在飞都新鲜）⇒ 继续延迟，不得误杀 ----
        let s2 = Arc::new(
            TaskScheduler::new().with_category_concurrency(CategoryConcurrency {
                crawl: 1,
                persistence: 1,
                monitor: 1,
                network: 1,
                federation: 1,
                tracker: 1,
            }),
        );
        s2.register(
            TaskMetadata::new("victim2", "受害者任务2", Duration::from_secs(10))
                .with_jitter(Duration::from_secs(0))
                .non_deferrable()
                .with_category(TaskCategory::Federation),
            || async { Ok(()) },
        );
        for token in 1..=3 {
            insert_fresh(&s2, token, TaskCategory::Federation);
        }
        s2.schedule_task("victim2", Instant::now(), TaskPriority::Normal);
        TaskScheduler::process_queue(s2.clone()).await;

        assert_eq!(s2.reclaimed_total(), 0, "新鲜在飞不得被误判为泄漏");
        assert_eq!(s2.in_flight_count(), 3, "真过载时不应凭空放行");
        assert_eq!(s2.queue.read().len(), 1, "应当被延迟重排，等待下一轮");
        s2.stop();
    }

    // -----------------------------------------------------------------------
    // 2026-10-09 根治「运行一段时间卡住」：计数漂移自愈 + 卡死保底
    // -----------------------------------------------------------------------

    /// 计数器与 `in_flight` 漂移后，必须被自愈校正回真值。
    ///
    /// 回归现场（.52 2026-10-09）：心跳打出 `monitor=4/2`（超限）而
    /// `在飞=1`、`最老在飞=-`，三者自相矛盾——计数超限把准入全拒之门外，
    /// 而 `最老在飞=-` 又让任何基于登记表年龄的保底判定失效，两条路一起堵死。
    #[test]
    fn test_reconcile_counters_heals_drift() {
        let s = Arc::new(TaskScheduler::new());
        // 登记表里真实只有 1 个在飞任务，但计数器被历史路径写成 4
        s.in_flight.write().insert(
            1,
            InFlightTask {
                task_id: "t1".to_string(),
                name: "真实在飞".to_string(),
                category: TaskCategory::Monitor,
                started_at: Instant::now(),
                timeout: Duration::from_secs(300),
                abort_handle: None,
            },
        );
        *s.running_by_category
            .write()
            .entry(TaskCategory::Monitor)
            .or_insert(0) = 4;
        assert_eq!(
            s.running_count(TaskCategory::Monitor),
            4,
            "前置：模拟漂移态"
        );

        s.reconcile_counters();

        assert_eq!(
            s.running_count(TaskCategory::Monitor),
            1,
            "必须按 in_flight 登记表校正为真值"
        );
    }

    /// 心跳的自愈必须让「准入判据 / 在飞数 / 最老在飞」三者恒等。
    #[test]
    fn test_reconcile_keeps_admission_and_counters_consistent() {
        let s = Arc::new(TaskScheduler::new());
        for (i, cat) in [
            TaskCategory::Monitor,
            TaskCategory::Persistence,
            TaskCategory::Crawl,
        ]
        .into_iter()
        .enumerate()
        {
            s.in_flight.write().insert(
                i as u64 + 1,
                InFlightTask {
                    task_id: format!("t{i}"),
                    name: format!("任务{i}"),
                    category: cat,
                    started_at: Instant::now(),
                    timeout: Duration::from_secs(300),
                    abort_handle: None,
                },
            );
            // 计数器全部错写成 99
            *s.running_by_category.write().entry(cat).or_insert(0) = 99;
        }

        s.reconcile_counters();

        for cat in [
            TaskCategory::Monitor,
            TaskCategory::Persistence,
            TaskCategory::Crawl,
        ] {
            assert_eq!(s.category_running_count(cat), 1);
            assert_eq!(
                s.running_count(cat),
                s.category_running_count(cat),
                "{:?}：准入判据与展示计数必须一致",
                cat
            );
        }
        assert_eq!(s.in_flight_count(), 3);
    }

    /// 卡死保底：未设阈值时永不启用（保持既有严格并发语义）。
    #[test]
    fn test_stall_fallback_disabled_by_default() {
        let s = Arc::new(TaskScheduler::new());
        insert_stale(&s, 1, TaskCategory::Monitor, Duration::from_secs(600));
        assert!(
            !s.category_is_stalled(TaskCategory::Monitor),
            "未设阈值 ⇒ 不启用保底"
        );
    }

    /// 卡死保底：最老在飞超阈值时应判定为已卡死，且阈值可热设。
    #[test]
    fn test_stall_fallback_triggers_past_threshold() {
        let s = Arc::new(TaskScheduler::new());
        s.set_category_stall_threshold(TaskCategory::Monitor, 120);
        // 最老在飞 600s > 120s ⇒ 卡死
        insert_stale(&s, 1, TaskCategory::Monitor, Duration::from_secs(600));
        assert!(s.category_is_stalled(TaskCategory::Monitor));

        // 新鲜在飞（0s）⇒ 未卡死
        let s2 = Arc::new(TaskScheduler::new());
        s2.set_category_stall_threshold(TaskCategory::Monitor, 120);
        insert_fresh(&s2, 1, TaskCategory::Monitor);
        assert!(!s2.category_is_stalled(TaskCategory::Monitor));

        // 阈值调大到 10000s ⇒ 600s 不再超限
        s2.set_category_stall_threshold(TaskCategory::Monitor, 10_000);
        assert!(!s2.category_is_stalled(TaskCategory::Monitor));

        // 传0 关闭保底
        s2.set_category_stall_threshold(TaskCategory::Monitor, 0);
        assert!(!s2.category_is_stalled(TaskCategory::Monitor));
    }

    /// 端到端回归：Monitor 分类被慢任务实质占死时，**监控任务必须能被放行**。
    ///
    /// 这是本次生产事故的直接断言——修复前该场景下监控任务被永久拒绝，
    /// 面板数据源断流（`io/status` 15s 超时而 CPU 100% 空闲）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_monitor_task_admitted_under_stall() {
        let s2 = Arc::new(TaskScheduler::new());
        s2.set_category_stall_threshold(TaskCategory::Monitor, CATEGORY_STALL_FALLBACK_SECS);
        // 一个「卡死」的 Monitor 在飞任务：把槽占满
        insert_stale(&s2, 1, TaskCategory::Monitor, Duration::from_secs(600));
        assert_eq!(s2.running_count(TaskCategory::Monitor), 1);

        let ran = Arc::new(AtomicBool::new(false));
        let r2 = ran.clone();
        s2.register(
            TaskMetadata::new("m_probe", "面板指标采样", Duration::from_secs(30))
                .with_category(TaskCategory::Monitor)
                .with_timeout(Duration::from_secs(5)),
            move || {
                let r = r2.clone();
                async move {
                    r.store(true, Ordering::SeqCst);
                    Ok(())
                }
            },
        );
        // 直接排到「现在」执行，绕开 initial_delay/jitter
        s2.schedule_task("m_probe", Instant::now(), TaskPriority::Normal);
        TaskScheduler::process_queue(s2.clone()).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert!(
            ran.load(Ordering::SeqCst),
            "卡死保底必须放行监控任务（否则面板数据源断流）"
        );
        s2.stop();
    }

    /// 泄漏判据分级回归（方案A）：判据按任务自身 timeout 分级。
    ///
    /// 背景：b99bf4e 引入的单一 240s 绝对上限把默认任务（timeout=300s）的真实
    /// 阈值 450s 压到 240s，架空 `stale_slot_factor=1.5`，日志打出
    /// 「已在飞 240s > 超时 300s×1.5」的自相矛盾判据。
    /// 方案A：**tick 级保留 240s 硬兜底，长任务不设绝对上限**。
    #[test]
    fn test_stale_threshold_tiered_by_task_timeout() {
        let s = Arc::new(TaskScheduler::new());

        // ① 长任务（timeout=300s > 180s 阈值）：240s 时不得误杀，
        //    须等到自己的 450s 预算
        s.in_flight.write().insert(
            1,
            InFlightTask {
                task_id: "long".to_string(),
                name: "长任务".to_string(),
                category: TaskCategory::Crawl,
                started_at: Instant::now()
                    .checked_sub(Duration::from_secs(240))
                    .unwrap_or_else(Instant::now),
                timeout: Duration::from_secs(300),
                abort_handle: None,
            },
        );
        assert_eq!(
            s.reclaim_stale_slots(None),
            0,
            "长任务在飞 240s（< 自身预算 450s）不得被误杀"
        );
        assert_eq!(s.in_flight_count(), 1);

        // 超过自身预算 450s ⇒ 判泄漏
        s.in_flight.write().get_mut(&1).unwrap().started_at = Instant::now()
            .checked_sub(Duration::from_secs(500))
            .unwrap_or_else(Instant::now);
        assert_eq!(s.reclaim_stale_slots(None), 1, "长任务超 450s 应判泄漏");

        // ② tick 级任务（timeout=60s ≤ 180s 阈值）：保留 240s 硬上限，
        //    这是 b99bf4e 的缺陷修复，**不得回退**
        let s2 = Arc::new(TaskScheduler::new());
        s2.in_flight.write().insert(
            1,
            InFlightTask {
                task_id: "tick".to_string(),
                name: "tick任务".to_string(),
                category: TaskCategory::Monitor,
                // timeout×1.5=90s，早就该判泄漏；但年龄取 200s，
                // 验证「硬上限」这一层在起作用（对长任务已不适用）
                started_at: Instant::now()
                    .checked_sub(Duration::from_secs(200))
                    .unwrap_or_else(Instant::now),
                timeout: Duration::from_secs(60),
                abort_handle: None,
            },
        );
        assert_eq!(
            s2.reclaim_stale_slots(None),
            1,
            "tick 级任务挂死 200s（自身预算仅 90s）必须判泄漏"
        );
    }

    /// tick 级任务即便`timeout` 很大，只要 ≤180s 就必须受 240s 硬上限约束。
    ///
    /// 这条锁定「分级边界」本身：`timeout=180s` 是 tick 与长任务的分界，
    /// 若将来有人调大该阈值，tick 级兜底会被静默放宽。
    #[test]
    fn test_tick_grade_keeps_240s_hard_cap() {
        let s = Arc::new(TaskScheduler::new());
        s.in_flight.write().insert(
            1,
            InFlightTask {
                task_id: "boundary".to_string(),
                name: "边界任务".to_string(),
                category: TaskCategory::Monitor,
                // timeout=180s →预算 270s；若 240s 硬上限被移除，
                // 270s 以下都不该判泄漏。取 250s 断言「仍被判泄漏」=硬上限生效。
                started_at: Instant::now()
                    .checked_sub(Duration::from_secs(250))
                    .unwrap_or_else(Instant::now),
                timeout: Duration::from_secs(180),
                abort_handle: None,
            },
        );
        assert_eq!(
            s.reclaim_stale_slots(None),
            1,
            "timeout=180s 属 tick 级，250s 应被 240s 硬上限判泄漏"
        );
    }

    /// 分类并发默认值回归：persistence / monitor 必须 ≥2（2026-10-09 根治）。
    #[test]
    fn test_persistence_and_monitor_concurrency_raised() {
        let c = CategoryConcurrency::default();
        assert!(
            c.persistence >= 2,
            "persistence 单槽会让互相关联的写路径任务互锁（生产实证 2.5h 停摆）"
        );
        assert!(c.monitor >= 3, "监控是唯一可观测性来源，需要额外余量");
    }

    // ============================================================
    // 2026-10-09 治本 S1/S2 回归测试：全局停摆探测与自愈
    // ============================================================

    /// 构造一个只含单个 tick 任务、且**不启动主循环**的调度器。
    ///
    /// 不启动主循环是刻意的：这样队列不会被 `process_queue` 消费，
    /// 测试才能精确构造「队列停在未来、在飞为 0」的停摆形态。
    fn scheduler_without_loop() -> Arc<TaskScheduler> {
        Arc::new(TaskScheduler::new())
    }

    /// 往队列塞一个指定「距今多少秒后到期」的任务。
    fn push_due_in(s: &Arc<TaskScheduler>, task_id: &str, secs_from_now: u64) {
        s.queue.write().push(ScheduledItem {
            task_id: task_id.to_string(),
            scheduled_at: Instant::now() + Duration::from_secs(secs_from_now),
            priority: TaskPriority::Background,
            seq: {
                let mut c = s.seq_counter.write();
                *c += 1;
                *c
            },
        });
    }

    /// 回归（治本 S1）：心跳必须暴露队列延迟，否则运维无法区分
    /// 「N 个任务正常等待」与「N 个任务全停在未来」——后者即本次事故全貌。
    #[test]
    fn test_queue_delay_span_reports_future_items() {
        let s = scheduler_without_loop();
        assert!(s.queue_delay_span().is_none(), "空队列应返回 None");

        push_due_in(&s, "a", 10);
        push_due_in(&s, "b", 200);

        let (min_gap, max_gap) = s.queue_delay_span().expect("队列非空");
        assert!(
            (9..=11).contains(&min_gap),
            "最早到期应≈10s，实得 {}",
            min_gap
        );
        assert!(
            (199..=201).contains(&max_gap),
            "最晚到期应≈200s，实得 {}",
            max_gap
        );
    }

    /// 回归（治本 S1）：`queue_overdue_count` 正常必须恒为 0。
    /// 它非0 是「调度主循环已停」的唯一直接信号。
    #[test]
    fn test_queue_overdue_count_zero_when_healthy() {
        let s = scheduler_without_loop();
        push_due_in(&s, "future", 60);
        assert_eq!(s.queue_overdue_count(), 0, "未到期项不应计入 overdue");
    }

    /// 核心回归（治本 S2）：复现 2026-10-09 事故形态 ——
    /// 队列 21 个任务全部停在未来、在飞为 0 ⇒ 必须被判停摆并自愈。
    #[test]
    fn test_stall_detected_when_queue_stuck_in_future() {
        let s = scheduler_without_loop();
        // 复现事故前提：崩溃前曾有大量在飞（本次事故 33 个泄漏槽位）。
        // 高水位是区分「事故态」与「正常空闲态」的唯一信号，必须先置高。
        s.peak_in_flight.store(30, Ordering::Relaxed);
        for i in 0..21 {
            push_due_in(&s, &format!("t{}", i), 300);
        }

        // 第1 轮：连续命中次数不足（需连续 2 轮），不应触发
        assert!(
            !s.detect_and_recover_global_stall(),
            "第 1 轮命中数不足，不应触发自愈"
        );

        // 第 2 轮：连续命中达阈值 ⇒ 触发
        assert!(
            s.detect_and_recover_global_stall(),
            "连续 2 轮满足停摆判据，必须触发自愈"
        );
        assert_eq!(
            s.stall_recovery_total.load(Ordering::Relaxed),
            1,
            "应累计一次自愈"
        );

        // 自愈后：队列全部被重置为立即到期，下一 tick 必然被 process_queue 取走
        assert_eq!(
            s.queue_overdue_count(),
            21,
            "自愈后所有队列项应变为已到期，等待主循环取走"
        );
    }

    /// 核心防误杀回归：正常空闲期**高水位恒为 0**，
    /// 即使队列里全是停在未来的长任务，也绝不能判为停摆。
    ///
    /// 这是本次测试迭代中最关键的一条：纯队列判据无法区分
    /// 「事故态」与「正常空闲态」，必须靠高水位区分。
    #[test]
    fn test_idle_system_with_zero_peak_never_triggers() {
        let s = scheduler_without_loop();
        assert_eq!(
            s.peak_in_flight.load(Ordering::Relaxed),
            0,
            "尚未准入过任何任务"
        );
        for i in 0..21 {
            push_due_in(&s, &format!("idle{}", i), 300);
        }
        for _ in 0..5 {
            assert!(
                !s.detect_and_recover_global_stall(),
                "高水位=0 ⇒ 正常空闲期，无论持续多久都不得触发自愈"
            );
        }
        assert_eq!(s.stall_recovery_total.load(Ordering::Relaxed), 0);
    }

    /// 对照：一旦高水位被抬起来（说明系统确实跑过任务），
    /// 同样的队列形态就必须被判停摆 —— 证明上一条不是靠「永不触发」过测试。
    #[test]
    fn test_same_queue_shape_triggers_once_peak_is_nonzero() {
        let s = scheduler_without_loop();
        for i in 0..21 {
            push_due_in(&s, &format!("x{}", i), 300);
        }
        // 高水位 0 ⇒ 不触发
        assert!(!s.detect_and_recover_global_stall());
        // 抬升高水位（模拟此前有 33 个任务在飞后全部挂死）。
        // 判据2 需连续 STALL_IDLE_ROUNDS(2) 轮命中，故调两轮。
        s.peak_in_flight.store(33, Ordering::Relaxed);
        assert!(
            !s.detect_and_recover_global_stall(),
            "高水位刚抬起的第 1 轮，连续命中数不足"
        );
        assert!(
            s.detect_and_recover_global_stall(),
            "高水位>0 且连续 2 轮命中 ⇒ 必须触发"
        );
    }

    /// 关键防误杀回归（治本 S2）：系统存在 `interval` 达 300s 的长任务
    /// （bootstrap / range 反熵类）。若只判「在飞为 0 且队列项距今 ≥120s」，
    /// 正常空闲期会被误杀并触发无谓 abort。
    ///
    /// 这里构造「长任务刚入队 + 系统空闲」：在飞为 0、队列项距今 300s，
    /// 但**此时存在已到期项**（另一个 5s 后到期的 tick 任务被推为现在到期）
    /// ⇒ 必须不触发。
    #[test]
    fn test_no_false_trigger_when_long_task_just_scheduled() {
        let s = scheduler_without_loop();
        // 长任务排到 300s 后
        push_due_in(&s, "long_bootstrap", 300);
        assert!(
            !s.detect_and_recover_global_stall(),
            "空闲期 + 长任务在未来 ⇒ 不得误判为停摆"
        );
        // 再来一轮，仍不得触发（连续计数也不该累积到阈值）
        assert!(
            !s.detect_and_recover_global_stall(),
            "空闲期持续也不得误判为停摆（防误杀是硬要求）"
        );
    }

    /// 关键防误杀：单轮命中不得触发（watchdog 有 30s 抖动，一次不算停摆）。
    #[test]
    fn test_single_round_overdue_does_not_trigger() {
        let s = scheduler_without_loop();
        // 造一个「已到期但未被取走」：scheduled_at 在过去
        s.queue.write().push(ScheduledItem {
            task_id: "stuck".to_string(),
            scheduled_at: Instant::now() - Duration::from_secs(5),
            priority: TaskPriority::Background,
            seq: 1,
        });
        assert!(
            !s.detect_and_recover_global_stall(),
            "单轮已到期积压不应触发（需连续 2 轮）"
        );
    }

    /// 队列为空时不得触发（避免正常空闲期被abort）。
    #[test]
    fn test_no_trigger_when_queue_empty() {
        let s = scheduler_without_loop();
        for _ in 0..3 {
            assert!(!s.detect_and_recover_global_stall(), "空队列不得触发自愈");
        }
        assert_eq!(s.stall_recovery_total.load(Ordering::Relaxed), 0);
    }

    /// 回归：启动/预热期不得误判停摆（2026-10-09 首次部署实测误报）。
    ///
    /// 现场：进程启动后 ~2 分钟内任务正陆续注册，`in_flight==0` 属正常，
    /// 而高水位已因部分任务完成而累积 ⇒ 判据2 命中并误触发一次 abort。
    /// 该abort 本身无害（系统随后正常），但它是**误报**，必须消除。
    #[test]
    fn test_no_trigger_during_warmup_while_still_scheduling() {
        let s = scheduler_without_loop();
        // 高水位已累积（有任务跑过）
        s.peak_in_flight.store(43, Ordering::Relaxed);
        // 但最近刚刚还有任务被准入 ⇒ 系统仍在正常调度
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        s.last_admit_ms.store(now_ms, Ordering::Relaxed);
        for i in 0..54 {
            push_due_in(&s, &format!("warm{}", i), 3500);
        }
        for _ in 0..3 {
            assert!(
                !s.detect_and_recover_global_stall(),
                "预热期仍在准入任务 ⇒ 不得判为停摆（不得误abort）"
            );
        }
        assert_eq!(s.stall_recovery_total.load(Ordering::Relaxed), 0);
    }

    /// 对照：超过宽限期且长期无准入 ⇒ 必须能触发（确保上一条不是靠豁免掩盖问题）。
    #[test]
    fn test_triggers_after_warmup_expires() {
        let s = scheduler_without_loop();
        s.peak_in_flight.store(43, Ordering::Relaxed);
        // 最近准入时间设为很久以前（模拟宽限期已过）
        let old_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
            .saturating_sub((STALL_STARTUP_GRACE_SECS + 600) * 1000);
        s.last_admit_ms.store(old_ms, Ordering::Relaxed);
        for i in 0..54 {
            push_due_in(&s, &format!("cold{}", i), 3500);
        }
        assert!(!s.detect_and_recover_global_stall(), "第 1 轮连续数不足");
        assert!(
            s.detect_and_recover_global_stall(),
            "宽限期已过且长期无准入 ⇒ 必须能触发停摆自愈"
        );
    }

    /// 自愈限流：连续触发不得高频抖动（否则正常长任务会被反复 abort）。
    #[test]
    fn test_stall_recovery_rate_limited() {
        let s = scheduler_without_loop();
        s.peak_in_flight.store(30, Ordering::Relaxed);
        for i in 0..21 {
            push_due_in(&s, &format!("t{}", i), 300);
        }
        // 判据2 需连续 2 轮命中
        assert!(!s.detect_and_recover_global_stall(), "第 1 轮连续数不足");
        assert!(s.detect_and_recover_global_stall(), "第 2 轮应触发");
        // 立刻重跑：受 STALL_MIN_TRIGGER_INTERVAL_SECS 限流，不应再次触发
        assert!(
            !s.detect_and_recover_global_stall(),
            "限流窗口内不得重复触发"
        );
        assert_eq!(
            s.stall_recovery_total.load(Ordering::Relaxed),
            1,
            "限流窗口内累计数不得增加"
        );
    }
}
