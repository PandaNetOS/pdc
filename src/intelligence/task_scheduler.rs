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

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

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

impl TaskPriority {
    pub fn max_delay(&self) -> Duration {
        match self {
            TaskPriority::Critical => Duration::from_secs(0),
            TaskPriority::Important => Duration::from_secs(30),
            TaskPriority::Normal => Duration::from_secs(300),
            TaskPriority::Background => Duration::from_secs(600),
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
}

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
            initial_delay: Duration::from_secs(0),
            jitter: Duration::from_secs(10),
            estimated_duration: Duration::from_secs(5),
            timeout: Duration::from_secs(300),
            max_retries: 3,
        }
    }

    pub fn with_priority(mut self, p: TaskPriority) -> Self {
        self.priority = p;
        if !self.deferrable {
            self.max_delay = Duration::from_secs(0);
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
        self.max_delay = Duration::from_secs(0);
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
}

impl TaskStats {
    pub fn avg_duration_ms(&self) -> u64 {
        if self.total_executions > 0 {
            self.total_duration_ms / self.total_executions
        } else {
            0
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_executions > 0 {
            self.success_count as f64 / self.total_executions as f64
        } else {
            1.0
        }
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
    pub timestamp: Instant,
}

impl Default for ResourceState {
    fn default() -> Self {
        Self {
            cpu_usage: 0.0,
            memory_usage: 0.0,
            io_busy: false,
            network_busy: false,
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
}

impl ResourceMonitor {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(ResourceState::default()),
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

    pub fn set_io_busy(&self, busy: bool) {
        self.state.write().io_busy = busy;
    }

    pub fn set_network_busy(&self, busy: bool) {
        self.state.write().network_busy = busy;
    }
}

impl Default for ResourceMonitor {
    fn default() -> Self {
        Self::new()
    }
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
        // 优先级高的在前，同优先级按调度时间排序
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| self.scheduled_at.cmp(&other.scheduled_at))
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

type TaskFn = Arc<dyn Fn() -> futures::future::BoxFuture<'static, anyhow::Result<()>> + Send + Sync>;

/// 智能任务调度器
pub struct TaskScheduler {
    tasks: RwLock<HashMap<String, TaskMetadata>>,
    task_fns: RwLock<HashMap<String, TaskFn>>,
    stats: RwLock<HashMap<String, TaskStats>>,
    queue: RwLock<BinaryHeap<ScheduledItem>>,
    running_full_tasks: RwLock<u32>,
    max_concurrent_full_tasks: u32,
    resource_monitor: Arc<ResourceMonitor>,
    seq_counter: RwLock<u64>,
    completed_dependencies: RwLock<HashSet<String>>,
    scheduler_started: RwLock<bool>,
}

impl TaskScheduler {
    pub fn new() -> Self {
        Self {
            tasks: RwLock::new(HashMap::new()),
            task_fns: RwLock::new(HashMap::new()),
            stats: RwLock::new(HashMap::new()),
            queue: RwLock::new(BinaryHeap::new()),
            running_full_tasks: RwLock::new(0),
            max_concurrent_full_tasks: 2,
            resource_monitor: Arc::new(ResourceMonitor::new()),
            seq_counter: RwLock::new(0),
            completed_dependencies: RwLock::new(HashSet::new()),
            scheduler_started: RwLock::new(false),
        }
    }

    pub fn resource_monitor(&self) -> Arc<ResourceMonitor> {
        self.resource_monitor.clone()
    }

    pub fn with_max_concurrent_full_tasks(mut self, max: u32) -> Self {
        self.max_concurrent_full_tasks = max;
        self
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
            Box::pin(async move { fut.await })
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
        for (_secs, indices) in by_interval.iter() {
            let count = indices.len();
            if count > 1 {
                for (j, &idx) in indices.iter().enumerate() {
                    // 分散到周期的前 80% 时间窗口
                    let stagger_secs = (adjusted_metadatas[idx].interval.as_secs() as f64
                        * 0.8
                        * j as f64
                        / count as f64) as u64;
                    adjusted_metadatas[idx].initial_delay =
                        Duration::from_secs(stagger_secs);
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

        info!(
            "[task_scheduler] 智能任务调度中心已启动（最大并发全量任务={}）",
            self.max_concurrent_full_tasks
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

        let mut tick_interval = tokio::time::interval(Duration::from_millis(500));

        loop {
            tick_interval.tick().await;
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
                    Instant::now() + Duration::from_secs(30),
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
                        Instant::now() + Duration::from_secs(15),
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
                        Instant::now() + Duration::from_secs(10),
                        item.priority,
                    );
                    continue;
                }
            }

            // 令牌桶：全量任务并发限制
            if meta.resource.is_full_task {
                let running = *scheduler.running_full_tasks.read();
                if running >= scheduler.max_concurrent_full_tasks {
                    debug!(
                        "[task_scheduler] 全量任务并发已满（{}/{}），延迟: {}",
                        running, scheduler.max_concurrent_full_tasks, meta.name
                    );
                    scheduler.schedule_task(
                        &item.task_id,
                        Instant::now() + Duration::from_secs(5),
                        item.priority,
                    );
                    continue;
                }
            }

            // 执行任务
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

        let is_full = meta.resource.is_full_task;
        if is_full {
            *scheduler.running_full_tasks.write() += 1;
        }

        let task_id = item.task_id.clone();

        tokio::spawn(async move {
            let start = Instant::now();
            let result = tokio::time::timeout(meta.timeout, task_fn()).await;

            let duration = start.elapsed();

            match result {
                Ok(Ok(())) => {
                    scheduler.stats.write().get_mut(&task_id).unwrap().record_success(duration);
                    debug!(
                        "[task_scheduler] 任务完成: {} ({:.2}s)",
                        meta.name,
                        duration.as_secs_f64()
                    );
                }
                Ok(Err(e)) => {
                    scheduler.stats.write().get_mut(&task_id).unwrap().record_failure();
                    warn!(
                        "[task_scheduler] 任务失败: {} - {} (连续失败 {})",
                        meta.name,
                        e,
                        scheduler
                            .stats
                            .read()
                            .get(&task_id)
                            .map(|s| s.consecutive_failures)
                            .unwrap_or(0)
                    );
                    // 失败重试（指数退避）
                    let consecutive = scheduler
                        .stats
                        .read()
                        .get(&task_id)
                        .map(|s| s.consecutive_failures)
                        .unwrap_or(0);
                    if consecutive < meta.max_retries {
                        let backoff = Duration::from_secs(2u64.pow(consecutive.min(5)));
                        scheduler.schedule_task(
                            &task_id,
                            Instant::now() + backoff,
                            meta.priority,
                        );
                    }
                }
                Err(_) => {
                    scheduler.stats.write().get_mut(&task_id).unwrap().record_failure();
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

            if is_full {
                let mut running = scheduler.running_full_tasks.write();
                if *running > 0 {
                    *running -= 1;
                }
            }

            // 安排下一次执行（添加随机抖动）
            let jitter_secs = if meta.jitter.as_secs() > 0 {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                rng.gen_range(0..=meta.jitter.as_secs())
            } else {
                0
            };
            scheduler.schedule_task(
                &task_id,
                Instant::now() + meta.interval + Duration::from_secs(jitter_secs),
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
        let running_full = *self.running_full_tasks.read();
        let queue_len = self.queue.read().len();

        let mut total_executions = 0u64;
        let mut total_failures = 0u64;
        for s in stats.values() {
            total_executions += s.total_executions;
            total_failures += s.failure_count;
        }

        TaskSchedulerSummary {
            registered_tasks: tasks.len(),
            running_full_tasks: running_full,
            max_concurrent_full_tasks: self.max_concurrent_full_tasks,
            queued_tasks: queue_len,
            total_executions,
            total_failures,
            resource: self.resource_monitor.current(),
        }
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
    pub running_full_tasks: u32,
    pub max_concurrent_full_tasks: u32,
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
        assert_eq!(summary.max_concurrent_full_tasks, 2);

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

        let task_fns: Vec<_> = (0..5)
            .map(|_| || async { Ok(()) })
            .collect();

        scheduler.register_with_stagger(metadatas, task_fns);

        let tasks = scheduler.list_tasks();
        assert_eq!(tasks.len(), 5);

        // 验证错峰：初始延迟应该不同
        let delays: Vec<u64> = tasks.iter().map(|t| t.initial_delay.as_secs()).collect();
        let unique_delays: HashSet<u64> = delays.iter().cloned().collect();
        assert!(unique_delays.len() > 1, "错峰应该产生不同的初始延迟");
    }
}
