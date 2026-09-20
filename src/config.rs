//! 配置管理
//!
//! 支持从 config.yaml 加载配置，也支持环境变量覆盖。
//! 配置结构按模块组织：server、super_tracker、discoverers、cache、health_check、crawler。

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// 根配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PdcConfig {
    /// 服务端配置
    #[serde(default)]
    pub server: ServerConfig,
    /// 超级 Tracker 配置
    #[serde(default)]
    pub super_tracker: SuperTrackerConfig,
    /// 发现器配置
    #[serde(default)]
    pub discoverers: DiscoverersConfig,
    /// 缓存配置
    #[serde(default)]
    pub cache: CacheConfig,
    /// 健康检查配置
    #[serde(default)]
    pub health_check: HealthCheckConfig,
    /// 爬虫配置
    #[serde(default)]
    pub crawler: CrawlerConfig,
    /// NAT 穿透配置
    #[serde(default)]
    pub nat: NatConfig,
    /// 持久化存储配置
    #[serde(default)]
    pub storage: StorageConfig,
    /// 持久化优化配置
    #[serde(default)]
    pub persistence: PersistenceConfig,
    /// SQLite 调优配置
    #[serde(default)]
    pub sqlite: SqliteConfig,
    /// TaskScheduler 各分类并发度
    #[serde(default)]
    pub task_scheduler: TaskSchedulerConfig,
    /// IO 调度器配置
    #[serde(default)]
    pub io_scheduler: IoSchedulerConfig,
    /// 联邦网络配置
    #[serde(default)]
    pub federation: crate::federation::config::FederationConfig,
    /// 日志级别
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// 是否启用端口自动探测（启动时自动探测一组可用端口）
    #[serde(default = "default_port_auto_alloc")]
    pub port_auto_alloc: bool,
    /// 端口组之间的步长（端口 = 基础端口 + offset * step）
    #[serde(default = "default_port_step")]
    pub port_step: u16,
    /// 是否自动添加 Windows 防火墙入站规则
    #[serde(default = "default_auto_firewall_rule")]
    pub auto_firewall_rule: bool,
    /// 配置热更新间隔（秒，0=禁用）
    #[serde(default = "default_config_reload_interval_secs")]
    pub config_reload_interval_secs: u64,
    /// Tokio runtime worker 线程数（0=自动按 CPU 核数，默认 12）
    #[serde(default = "default_runtime_worker_threads")]
    pub runtime_worker_threads: usize,
    /// Tracker runtime 线程数（默认 4）
    #[serde(default = "default_tracker_runtime_threads")]
    pub tracker_runtime_threads: usize,
    /// API runtime 线程数（默认 4）
    #[serde(default = "default_api_runtime_threads")]
    pub api_runtime_threads: usize,
    /// 联邦 runtime 线程数（0=自动按 CPU 核数）
    #[serde(default = "default_federation_runtime_threads")]
    pub federation_runtime_threads: usize,
    /// 调度器 runtime 线程数（默认 2）
    #[serde(default = "default_scheduler_runtime_threads")]
    pub scheduler_runtime_threads: usize,
    /// 持久化 runtime 线程数（默认 4）
    #[serde(default = "default_persistence_runtime_threads")]
    pub persistence_runtime_threads: usize,
    /// 统计快照更新间隔（秒，默认 1）
    #[serde(default = "default_stats_snapshot_interval_secs")]
    pub stats_snapshot_interval_secs: u64,
    /// Runtime 关闭超时（秒，默认 5）
    #[serde(default = "default_runtime_shutdown_timeout_secs")]
    pub runtime_shutdown_timeout_secs: u64,
    /// 自适应控制器配置
    #[serde(default)]
    pub adaptive: AdaptiveConfig,
    /// 冷热分层缓存配置
    #[serde(default)]
    pub tier: TierConfig,
}

/// TaskScheduler 各分类并发度配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSchedulerConfig {
    /// 爬虫类并发度
    #[serde(default = "default_crawl_concurrency")]
    pub crawl_concurrency: u32,
    /// 持久化类并发度
    #[serde(default = "default_persistence_concurrency")]
    pub persistence_concurrency: u32,
    /// 监控类并发度
    #[serde(default = "default_monitor_concurrency")]
    pub monitor_concurrency: u32,
    /// 网络类并发度
    #[serde(default = "default_network_concurrency")]
    pub network_concurrency: u32,
    /// 各任务调度间隔覆盖表。
    ///
    /// key 为 TaskScheduler.register 的任务名（如 `"crawler_tick"`），value 为间隔数值。
    /// - 绝大多数任务：单位为秒（如 `crawler_active_crawl = 30`）。
    /// - 亚秒级任务（gossip flush / propagation）：单位为毫秒
    ///   （`fed_gossip_flush = 50`、`fed_gossip_propagation = 100`、`fed_merkle_flush = 1000`）。
    /// - 此外可用 `<任务名>_initial_delay` / `<任务名>_jitter` 覆盖该任务的初始延迟与抖动。
    ///
    /// 未配置的任务一律使用代码内默认值，行为与未引入本配置前完全一致（向后兼容）。
    #[serde(default = "default_task_scheduler_intervals")]
    pub intervals: HashMap<String, u64>,
    /// 资源感知准入控制总开关。false（默认）时调度行为与改造前完全一致。
    #[serde(default = "default_admission_control_enabled")]
    pub admission_control_enabled: bool,
    /// CPU 使用率准入阈值（0.0-1.0），超过则延迟非保活任务。
    #[serde(default = "default_admission_cpu_threshold")]
    pub admission_cpu_threshold: f32,
    /// IO 负载准入阈值（0.0-1.0），超过则延迟非保活任务。
    #[serde(default = "default_admission_io_threshold")]
    pub admission_io_threshold: f32,
    /// 准入延迟的最大 tick 数，达到后强制执行避免饥饿。
    #[serde(default = "default_admission_max_delay_ticks")]
    pub admission_max_delay_ticks: u32,
    /// 每轮执行随机错峰抖动总开关。false（默认）时退化为固定间隔。
    #[serde(default = "default_random_jitter_enabled")]
    pub random_jitter_enabled: bool,
    /// 随机抖动比例（±ratio），0.1 表示间隔 ±10%。
    #[serde(default = "default_random_jitter_ratio")]
    pub random_jitter_ratio: f32,
    /// 任务画像 EWMA 平滑系数 alpha（0.0-1.0）。
    #[serde(default = "default_profile_ewma_alpha")]
    pub profile_ewma_alpha: f32,
    /// 预测式调度总开关（S1-P2）。false（默认）时调度行为与改造前完全一致。
    #[serde(default = "default_predictive_scheduling_enabled")]
    pub predictive_scheduling_enabled: bool,
    /// 负载预测 EWMA 平滑系数 alpha（0.0-1.0）。
    #[serde(default = "default_predict_ewma_alpha")]
    pub predict_ewma_alpha: f32,
    /// 负载预测保留的历史采样点数量（环形缓冲区大小）。
    #[serde(default = "default_predict_history_size")]
    pub predict_history_size: usize,
    /// 预测前向 tick 数（1-5），用于预测式调度。
    #[serde(default = "default_predict_lookahead_ticks")]
    pub predict_lookahead_ticks: u32,
    /// 自适应执行间隔总开关（S1-P3）。false（默认）时使用固定周期。
    #[serde(default = "default_adaptive_interval_enabled")]
    pub adaptive_interval_enabled: bool,
    /// 自适应间隔目标负载（0.0-1.0），当前负载围绕该值上下调整间隔。
    #[serde(default = "default_adaptive_target_load")]
    pub adaptive_target_load: f32,
    /// 自适应间隔最小缩放比例（最短缩到基准的该比例倍）。
    #[serde(default = "default_adaptive_min_ratio")]
    pub adaptive_min_ratio: f32,
    /// 自适应间隔最大缩放比例（最长延到基准的该比例倍）。
    #[serde(default = "default_adaptive_max_ratio")]
    pub adaptive_max_ratio: f32,
    /// 闭环重新计算自适应间隔的 tick 周期数。
    #[serde(default = "default_adaptive_recalc_ticks")]
    pub adaptive_recalc_ticks: u32,
}

fn default_crawl_concurrency() -> u32 {
    4
}

fn default_persistence_concurrency() -> u32 {
    1
}

fn default_monitor_concurrency() -> u32 {
    2
}

fn default_network_concurrency() -> u32 {
    4
}

fn default_admission_control_enabled() -> bool {
    false
}

fn default_admission_cpu_threshold() -> f32 {
    0.8
}

fn default_admission_io_threshold() -> f32 {
    0.8
}

fn default_admission_max_delay_ticks() -> u32 {
    5
}

fn default_random_jitter_enabled() -> bool {
    false
}

fn default_random_jitter_ratio() -> f32 {
    0.1
}

fn default_profile_ewma_alpha() -> f32 {
    0.2
}

fn default_predictive_scheduling_enabled() -> bool {
    false
}

fn default_predict_ewma_alpha() -> f32 {
    0.3
}

fn default_predict_history_size() -> usize {
    20
}

fn default_predict_lookahead_ticks() -> u32 {
    3
}

fn default_adaptive_interval_enabled() -> bool {
    false
}

fn default_adaptive_target_load() -> f32 {
    0.6
}

fn default_adaptive_min_ratio() -> f32 {
    0.5
}

fn default_adaptive_max_ratio() -> f32 {
    2.0
}

fn default_adaptive_recalc_ticks() -> u32 {
    5
}

/// `task_scheduler.intervals` 的字段级 serde 默认值。
///
/// 必须与 `TaskSchedulerConfig` 的 `impl Default` 保持一致，否则
/// 「YAML 写了 `task_scheduler:` 节但省略 `intervals`」时会静默丢掉这里的默认覆盖项。
fn default_task_scheduler_intervals() -> HashMap<String, u64> {
    let mut m = HashMap::new();
    // 5 分钟（原 3600 秒 / 1 小时，缩短后 WAL 更频繁压缩）
    m.insert("wal_checkpoint_truncate_interval_secs".to_string(), 300u64);
    m
}

/// 读取任务间隔数值。未在 `intervals` 中配置时返回 `default_value`，保持向后兼容。
///
/// 单位由调用方决定（见 [`TaskSchedulerConfig.intervals`] 文档：秒级任务用秒，
/// 亚秒级任务用毫秒）。本函数只负责“配置覆盖或回退默认值”。
pub fn get_interval_secs(
    intervals: &HashMap<String, u64>,
    task_name: &str,
    default_value: u64,
) -> u64 {
    intervals.get(task_name).copied().unwrap_or(default_value)
}

impl Default for TaskSchedulerConfig {
    fn default() -> Self {
        Self {
            crawl_concurrency: default_crawl_concurrency(),
            persistence_concurrency: default_persistence_concurrency(),
            monitor_concurrency: default_monitor_concurrency(),
            network_concurrency: default_network_concurrency(),
            intervals: default_task_scheduler_intervals(),
            admission_control_enabled: default_admission_control_enabled(),
            admission_cpu_threshold: default_admission_cpu_threshold(),
            admission_io_threshold: default_admission_io_threshold(),
            admission_max_delay_ticks: default_admission_max_delay_ticks(),
            random_jitter_enabled: default_random_jitter_enabled(),
            random_jitter_ratio: default_random_jitter_ratio(),
            profile_ewma_alpha: default_profile_ewma_alpha(),
            predictive_scheduling_enabled: default_predictive_scheduling_enabled(),
            predict_ewma_alpha: default_predict_ewma_alpha(),
            predict_history_size: default_predict_history_size(),
            predict_lookahead_ticks: default_predict_lookahead_ticks(),
            adaptive_interval_enabled: default_adaptive_interval_enabled(),
            adaptive_target_load: default_adaptive_target_load(),
            adaptive_min_ratio: default_adaptive_min_ratio(),
            adaptive_max_ratio: default_adaptive_max_ratio(),
            adaptive_recalc_ticks: default_adaptive_recalc_ticks(),
        }
    }
}

/// 持久化存储配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// 数据库文件路径
    #[serde(default = "default_storage_path")]
    pub path: String,
    /// 是否启用持久化
    #[serde(default = "default_storage_enabled")]
    pub enabled: bool,
}

fn default_storage_path() -> String {
    "./data/pdc.db".to_string()
}

fn default_storage_enabled() -> bool {
    true
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: default_storage_path(),
            enabled: default_storage_enabled(),
        }
    }
}

/// 持久化优化配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    /// 批量写入大小
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// WriteQueue 异步刷盘间隔（秒，攒批事务写入）
    #[serde(default = "default_flush_interval_secs")]
    pub flush_interval_secs: u64,
}

fn default_batch_size() -> usize {
    500
}
fn default_flush_interval_secs() -> u64 {
    10
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            flush_interval_secs: default_flush_interval_secs(),
        }
    }
}

/// SQLite 深度调优配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteConfig {
    /// mmap 大小（字节，0=禁用）
    #[serde(default = "default_sqlite_mmap_size")]
    pub mmap_size: i64,
    /// 页缓存大小（负数表示页数，如 -262144 = 256MB 页缓存）
    #[serde(default = "default_sqlite_cache_size")]
    pub cache_size: i64,
    /// WAL 自动 checkpoint 页数
    #[serde(default = "default_sqlite_wal_autocheckpoint")]
    pub wal_autocheckpoint: u32,
    /// 临时存储模式（MEMORY/FILE）
    #[serde(default = "default_sqlite_temp_store")]
    pub temp_store: String,
    /// 同步模式（NORMAL/FULL/OFF）
    #[serde(default = "default_sqlite_synchronous")]
    pub synchronous: String,
    /// 忙等待超时（毫秒，0=不等待）。避免 WAL checkpoint/VACUUM 与写事务并发时直接返回 SQLITE_BUSY。
    #[serde(default = "default_sqlite_busy_timeout_ms")]
    pub busy_timeout_ms: u32,
}

fn default_sqlite_busy_timeout_ms() -> u32 {
    5000 // 5s：等待其他写事务/checkpoint 释放锁，而非立即 SQLITE_BUSY 丢写
}

fn default_sqlite_mmap_size() -> i64 {
    134_217_728 // 128MB（原256MB过大，配合64MB cache控制内存）
}
fn default_sqlite_cache_size() -> i64 {
    -65_536 // 64MB 页缓存（负数表示页数，原256MB过大导致内存占用高）
}
fn default_sqlite_wal_autocheckpoint() -> u32 {
    1000 // 恢复SQLite自动checkpoint兜底（1000页），IOScheduler仍为主调度
}
fn default_sqlite_temp_store() -> String {
    "MEMORY".to_string()
}
fn default_sqlite_synchronous() -> String {
    "NORMAL".to_string()
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            mmap_size: default_sqlite_mmap_size(),
            cache_size: default_sqlite_cache_size(),
            wal_autocheckpoint: default_sqlite_wal_autocheckpoint(),
            temp_store: default_sqlite_temp_store(),
            synchronous: default_sqlite_synchronous(),
            busy_timeout_ms: default_sqlite_busy_timeout_ms(),
        }
    }
}

// ---------------------------------------------------------------------------
// IO 调度器配置
// ---------------------------------------------------------------------------

/// IO 调度器配置（统一 SQLite 写入调度：优先级队列 + 令牌桶 + 请求合并）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoSchedulerConfig {
    /// 是否启用 IO 调度器（false 时 WriteQueue 走原逻辑，完全向后兼容）
    #[serde(default = "default_io_scheduler_enabled")]
    pub enabled: bool,
    /// 队列最大容量（超过此值拒绝非 Critical/Important 请求）
    #[serde(default = "default_io_max_queue_size")]
    pub max_queue_size: usize,
    /// 令牌桶补充速率（行/秒）
    #[serde(default = "default_io_token_bucket_rate")]
    pub token_bucket_rate: usize,
    /// 令牌桶最大容量（突发上限）
    #[serde(default = "default_io_token_bucket_max")]
    pub token_bucket_max: usize,
    /// 背压低水位（低于此值无背压）
    #[serde(default = "default_io_low_watermark")]
    pub low_watermark: usize,
    /// 背压高水位（高于此值满背压，拒绝 Background 请求）
    #[serde(default = "default_io_high_watermark")]
    pub high_watermark: usize,
    /// 请求合并最大批大小
    #[serde(default = "default_io_batch_max_size")]
    pub batch_max_size: usize,
    /// 请求合并最大延迟（毫秒）
    #[serde(default = "default_io_batch_max_delay_ms")]
    pub batch_max_delay_ms: u64,
    /// 是否启用空闲预测
    #[serde(default = "default_true")]
    pub idle_prediction_enabled: bool,
    /// 空闲预测滑动窗口（秒）
    #[serde(default = "default_io_idle_window_secs")]
    pub idle_window_secs: u64,
    /// 背压轮询间隔（秒，注入 TaskScheduler）
    #[serde(default = "default_io_backpressure_poll_interval_secs")]
    pub backpressure_poll_interval_secs: u64,
    /// 队列为空时的等待时间（毫秒）
    #[serde(default = "default_io_idle_wait_ms")]
    pub idle_wait_ms: u64,
    /// 令牌不足时的重试等待时间（毫秒）
    #[serde(default = "default_io_retry_wait_ms")]
    pub retry_wait_ms: u64,
}

fn default_io_scheduler_enabled() -> bool {
    true
}
fn default_io_max_queue_size() -> usize {
    100_000
}
fn default_io_token_bucket_rate() -> usize {
    10_000
}
fn default_io_token_bucket_max() -> usize {
    20_000
}
fn default_io_low_watermark() -> usize {
    1_000
}
fn default_io_high_watermark() -> usize {
    50_000
}
fn default_io_batch_max_size() -> usize {
    500
}
fn default_io_batch_max_delay_ms() -> u64 {
    10
}
fn default_io_idle_window_secs() -> u64 {
    60
}
fn default_io_backpressure_poll_interval_secs() -> u64 {
    5
}
fn default_io_idle_wait_ms() -> u64 {
    500
}
fn default_io_retry_wait_ms() -> u64 {
    50
}

impl Default for IoSchedulerConfig {
    fn default() -> Self {
        Self {
            enabled: default_io_scheduler_enabled(),
            max_queue_size: default_io_max_queue_size(),
            token_bucket_rate: default_io_token_bucket_rate(),
            token_bucket_max: default_io_token_bucket_max(),
            low_watermark: default_io_low_watermark(),
            high_watermark: default_io_high_watermark(),
            batch_max_size: default_io_batch_max_size(),
            batch_max_delay_ms: default_io_batch_max_delay_ms(),
            idle_prediction_enabled: default_true(),
            idle_window_secs: default_io_idle_window_secs(),
            backpressure_poll_interval_secs: default_io_backpressure_poll_interval_secs(),
            idle_wait_ms: default_io_idle_wait_ms(),
            retry_wait_ms: default_io_retry_wait_ms(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_port_auto_alloc() -> bool {
    true
}

fn default_port_step() -> u16 {
    10
}

fn default_auto_firewall_rule() -> bool {
    true
}

fn default_config_reload_interval_secs() -> u64 {
    30
}

fn default_runtime_worker_threads() -> usize {
    12
}

fn default_tracker_runtime_threads() -> usize {
    4
}

fn default_api_runtime_threads() -> usize {
    4
}

fn default_federation_runtime_threads() -> usize {
    0
}

fn default_scheduler_runtime_threads() -> usize {
    2
}

fn default_persistence_runtime_threads() -> usize {
    4
}

fn default_stats_snapshot_interval_secs() -> u64 {
    1
}

fn default_runtime_shutdown_timeout_secs() -> u64 {
    5
}

/// 冷热分层缓存配置
///
/// 所有 Repo 数据永久保存在 SQLite，内存只保留 Hot+Warm。
/// 冷数据自动从内存卸载，需要时按需加载。全局内存硬限制由 memory_limit_mb 控制。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierConfig {
    /// 是否启用冷热分层（默认 true；false 时退化为全量加载）
    #[serde(default = "default_tier_enabled")]
    pub enabled: bool,
    /// 全局内存硬限制（MB，默认 500）
    #[serde(default = "default_tier_memory_limit_mb")]
    pub memory_limit_mb: usize,
    /// Hot LRU 最大条目数（默认 500,000）
    #[serde(default = "default_tier_hot_max_count")]
    pub hot_max_count: usize,
    /// Warm LRU 最大条目数（默认 100,000）
    #[serde(default = "default_tier_warm_max_count")]
    pub warm_max_count: usize,
    /// PeerRepo 专属 Warm LRU 上限（默认 50,000；peer 条目较大，单独下调以省内存）
    #[serde(default = "default_tier_peer_warm_max_count")]
    pub peer_warm_max_count: usize,
    /// Hot→Warm 降级阈值（秒，默认 1800=30分钟）
    #[serde(default = "default_tier_hot_threshold_secs")]
    pub hot_threshold_secs: u64,
    /// Warm→Cold 卸载阈值（秒，默认 7200=2小时）
    #[serde(default = "default_tier_warm_threshold_secs")]
    pub warm_threshold_secs: u64,
    /// 分层驱逐任务间隔（秒，默认 60）
    #[serde(default = "default_tier_evict_interval_secs")]
    pub evict_interval_secs: u64,
    /// 全局内存监控采样间隔（秒，默认 30）
    #[serde(default = "default_tier_memory_monitor_interval_secs")]
    pub memory_monitor_interval_secs: u64,
    /// 紧急驱逐触发阈值（内存占比，默认 0.90=90%）
    #[serde(default = "default_tier_emergency_threshold")]
    pub emergency_threshold: f64,
    /// 启动预加载条目数（默认 100000；其余数据保留在 DB 中按需迁移）
    #[serde(default = "default_tier_preload_top_n")]
    pub preload_top_n: usize,
}

impl Default for TierConfig {
    fn default() -> Self {
        Self {
            enabled: default_tier_enabled(),
            memory_limit_mb: default_tier_memory_limit_mb(),
            hot_max_count: default_tier_hot_max_count(),
            warm_max_count: default_tier_warm_max_count(),
            peer_warm_max_count: default_tier_peer_warm_max_count(),
            hot_threshold_secs: default_tier_hot_threshold_secs(),
            warm_threshold_secs: default_tier_warm_threshold_secs(),
            evict_interval_secs: default_tier_evict_interval_secs(),
            memory_monitor_interval_secs: default_tier_memory_monitor_interval_secs(),
            emergency_threshold: default_tier_emergency_threshold(),
            preload_top_n: default_tier_preload_top_n(),
        }
    }
}

fn default_tier_enabled() -> bool {
    true
}
fn default_tier_memory_limit_mb() -> usize {
    500
}
fn default_tier_hot_max_count() -> usize {
    200_000
}
fn default_tier_warm_max_count() -> usize {
    100_000
}
fn default_tier_peer_warm_max_count() -> usize {
    50_000
}
fn default_tier_hot_threshold_secs() -> u64 {
    1800
}
fn default_tier_warm_threshold_secs() -> u64 {
    7200
}
fn default_tier_evict_interval_secs() -> u64 {
    60
}
fn default_tier_memory_monitor_interval_secs() -> u64 {
    30
}
fn default_tier_emergency_threshold() -> f64 {
    0.90
}
fn default_tier_preload_top_n() -> usize {
    100_000
}

impl Default for PdcConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            super_tracker: SuperTrackerConfig::default(),
            discoverers: DiscoverersConfig::default(),
            cache: CacheConfig::default(),
            health_check: HealthCheckConfig::default(),
            crawler: CrawlerConfig::default(),
            nat: NatConfig::default(),
            storage: StorageConfig::default(),
            persistence: PersistenceConfig::default(),
            sqlite: SqliteConfig::default(),
            task_scheduler: TaskSchedulerConfig::default(),
            io_scheduler: IoSchedulerConfig::default(),
            federation: Default::default(),
            log_level: default_log_level(),
            port_auto_alloc: default_port_auto_alloc(),
            port_step: default_port_step(),
            auto_firewall_rule: default_auto_firewall_rule(),
            config_reload_interval_secs: default_config_reload_interval_secs(),
            runtime_worker_threads: default_runtime_worker_threads(),
            tracker_runtime_threads: default_tracker_runtime_threads(),
            api_runtime_threads: default_api_runtime_threads(),
            federation_runtime_threads: default_federation_runtime_threads(),
            scheduler_runtime_threads: default_scheduler_runtime_threads(),
            persistence_runtime_threads: default_persistence_runtime_threads(),
            stats_snapshot_interval_secs: default_stats_snapshot_interval_secs(),
            runtime_shutdown_timeout_secs: default_runtime_shutdown_timeout_secs(),
            adaptive: AdaptiveConfig::default(),
            tier: TierConfig::default(),
        }
    }
}

impl PdcConfig {
    /// 从 YAML 文件加载配置
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: PdcConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// 从 YAML 字符串加载配置
    pub fn from_yaml(content: &str) -> anyhow::Result<Self> {
        let config: PdcConfig = serde_yaml::from_str(content)?;
        Ok(config)
    }

    /// 加载配置：优先从指定文件加载，文件不存在则使用默认值
    pub fn load_or_default<P: AsRef<Path>>(path: P) -> Self {
        match Self::from_file(&path) {
            Ok(config) => {
                tracing::info!("[config] 已从 {:?} 加载配置", path.as_ref());
                config
            }
            Err(e) => {
                tracing::warn!(
                    "[config] 加载配置文件失败（{:?}），使用默认配置: {}",
                    path.as_ref(),
                    e
                );
                Self::default()
            }
        }
    }

    /// 序列化为 YAML（用于保存配置）
    pub fn to_yaml(&self) -> anyhow::Result<String> {
        Ok(serde_yaml::to_string(self)?)
    }
}

// ---------------------------------------------------------------------------
// 服务端配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// 监听地址
    #[serde(default = "default_listen")]
    pub listen: String,
    /// 监听端口（超级 Tracker HTTP+UDP）
    #[serde(default = "default_port")]
    pub port: u16,
    /// API/监控端口（绑定局域网，不映射公网，需 token 鉴权）
    #[serde(default = "default_api_port")]
    pub api_port: u16,
    /// API 鉴权 token（为空则不鉴权）
    #[serde(default)]
    pub token: Option<String>,
}

fn default_listen() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    6880
}

fn default_api_port() -> u16 {
    6886
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            port: default_port(),
            api_port: default_api_port(),
            token: None,
        }
    }
}

// ---------------------------------------------------------------------------
// 超级 Tracker 配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuperTrackerConfig {
    /// 是否启用超级 Tracker（/announce /scrape）
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 推荐的再次 announce 间隔（秒）
    #[serde(default = "default_interval")]
    pub interval: i64,
    /// 最小间隔（秒）
    #[serde(default = "default_min_interval")]
    pub min_interval: i64,
    /// 每次 announce 返回的最大 peer 数
    #[serde(default = "default_numwant")]
    pub max_numwant: usize,
    /// Peer 在超级 Tracker 中的过期时间（秒）
    #[serde(default = "default_peer_ttl")]
    pub peer_ttl_secs: u64,
    /// 是否在 announce 时触发后端发现器主动发现
    #[serde(default = "default_true")]
    pub trigger_backend_discovery: bool,
    /// UDP Tracker 端口（None 则与 HTTP 端口相同）
    #[serde(default)]
    pub udp_port: Option<u16>,
    /// 中继服务器端口（UDP+TCP，打洞失败时流量转发）
    #[serde(default = "default_relay_port")]
    pub relay_port: u16,
    /// announce 结果缓存 TTL（秒，0=禁用）
    #[serde(default = "default_announce_cache_ttl_secs")]
    pub announce_cache_ttl_secs: u64,
}

fn default_true() -> bool {
    true
}
fn default_interval() -> i64 {
    30
}
fn default_min_interval() -> i64 {
    30
}
fn default_numwant() -> usize {
    200
}
fn default_peer_ttl() -> u64 {
    3600
}
fn default_relay_port() -> u16 {
    6881
}
fn default_announce_cache_ttl_secs() -> u64 {
    5
}

impl Default for SuperTrackerConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            interval: default_interval(),
            min_interval: default_min_interval(),
            max_numwant: default_numwant(),
            peer_ttl_secs: default_peer_ttl(),
            trigger_backend_discovery: default_true(),
            udp_port: None,
            relay_port: default_relay_port(),
            announce_cache_ttl_secs: default_announce_cache_ttl_secs(),
        }
    }
}

// ---------------------------------------------------------------------------
// 发现器配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverersConfig {
    /// 是否启用 Tracker 发现器
    #[serde(default = "default_true")]
    pub enable_tracker: bool,
    /// 是否启用 DHT 发现器
    #[serde(default = "default_true")]
    pub enable_dht: bool,
    /// 是否启用 PEX 发现器
    #[serde(default = "default_true")]
    pub enable_pex: bool,
    /// 是否启用 LPD 发现器（局域网多播）
    #[serde(default = "default_true")]
    pub enable_lpd: bool,
    /// 是否启用 WebSeed 发现器
    #[serde(default = "default_true")]
    pub enable_webseed: bool,
    /// 单次发现超时（秒）
    #[serde(default = "default_discovery_timeout")]
    pub discovery_timeout_secs: u64,
    /// 最大并发发现器数
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
    /// 自定义 Tracker 列表（为空则使用内置公共 Tracker）
    #[serde(default)]
    pub custom_trackers: Vec<String>,
    /// 是否启用远程 Tracker 列表自动拉取
    #[serde(default = "default_true")]
    pub enable_remote_tracker: bool,
    /// 远程 Tracker 列表 URL
    #[serde(default = "default_remote_tracker_url")]
    pub remote_tracker_url: String,
    /// DHT 监听端口
    #[serde(default = "default_dht_port")]
    pub dht_listen_port: u16,
    /// LPD 多播端口（局域网发现，默认禁用）
    #[serde(default = "default_lpd_multicast_port")]
    pub lpd_multicast_port: u16,
    /// 发现超时（Duration，供聚合器使用）
    #[serde(skip)]
    pub discovery_timeout: Duration,
}

fn default_discovery_timeout() -> u64 {
    30
}
fn default_max_concurrent() -> usize {
    10
}
fn default_dht_port() -> u16 {
    6881
}
fn default_lpd_multicast_port() -> u16 {
    6771
}
fn default_remote_tracker_url() -> String {
    "https://cdn.jsdelivr.net/gh/adysec/tracker@main/trackers_best.txt".to_string()
}

impl Default for DiscoverersConfig {
    fn default() -> Self {
        Self {
            enable_tracker: default_true(),
            enable_dht: default_true(),
            enable_pex: default_true(),
            enable_lpd: true,
            enable_webseed: true,
            discovery_timeout_secs: default_discovery_timeout(),
            max_concurrent: default_max_concurrent(),
            custom_trackers: vec![],
            enable_remote_tracker: default_true(),
            remote_tracker_url: default_remote_tracker_url(),
            dht_listen_port: default_dht_port(),
            lpd_multicast_port: default_lpd_multicast_port(),
            discovery_timeout: Duration::from_secs(default_discovery_timeout()),
        }
    }
}

// ---------------------------------------------------------------------------
// 缓存配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Peer 过期时间（秒）
    #[serde(default = "default_cache_ttl")]
    pub peer_ttl_secs: u64,
    /// 每次发现的最大 peer 数
    #[serde(default = "default_max_peers")]
    pub max_peers_per_discovery: usize,
}

fn default_cache_ttl() -> u64 {
    86400
}
fn default_max_peers() -> usize {
    200
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            peer_ttl_secs: default_cache_ttl(),
            max_peers_per_discovery: default_max_peers(),
        }
    }
}

// ---------------------------------------------------------------------------
// 健康检查配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// 发现器健康检查间隔（秒）
    #[serde(default = "default_hc_interval")]
    pub interval_secs: u64,
    /// 缓存清理间隔（秒）
    #[serde(default = "default_cleanup_interval")]
    pub cache_cleanup_interval_secs: u64,
    /// 统计输出间隔（秒）
    #[serde(default = "default_stats_interval")]
    pub stats_output_interval_secs: u64,
}

fn default_hc_interval() -> u64 {
    300
}
fn default_cleanup_interval() -> u64 {
    600
}
fn default_stats_interval() -> u64 {
    300
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_hc_interval(),
            cache_cleanup_interval_secs: default_cleanup_interval(),
            stats_output_interval_secs: default_stats_interval(),
        }
    }
}

// ---------------------------------------------------------------------------
// 爬虫配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrawlerConfig {
    /// 是否启用爬虫引擎
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 最大收集的 infohash 数
    #[serde(default = "default_max_infohashes")]
    pub max_infohashes: usize,
    /// 爬虫监听的 UDP 端口
    #[serde(default = "default_crawler_listen_port")]
    pub listen_port: u16,
    /// uTP 服务端监听端口（UDP）
    #[serde(default = "default_utp_port")]
    pub utp_port: u16,
    /// TCP-PEX 服务端监听端口（TCP）
    #[serde(default = "default_tcp_pex_port")]
    pub tcp_pex_port: u16,
    /// 爬虫 UDP socket 数量（多 socket 并行收包，上限10）
    #[serde(default = "default_crawler_socket_count")]
    pub socket_count: u16,
    /// 是否启用对端响应率自适应限速
    #[serde(default = "default_adaptive_rate_limit")]
    pub adaptive_rate_limit: bool,
    /// 自适应限速滑动窗口大小（秒）
    #[serde(default = "default_rate_limit_window_secs")]
    pub rate_limit_window_secs: u64,
    /// Bootstrap 节点列表
    #[serde(default = "default_crawler_bootstrap_nodes")]
    pub bootstrap_nodes: Vec<(String, u16)>,
    /// 启动预热节点数（从数据库加载最近活跃节点到内存）
    #[serde(default = "default_warmup_node_count")]
    pub warmup_node_count: u32,
    /// 预热时并行 bootstrap 引导节点数
    #[serde(default = "default_warmup_bootstrap_concurrent")]
    pub warmup_bootstrap_concurrent: u32,
    /// 消息处理并发上限（0=自动，按 CPU 核数/4 计算）
    #[serde(default = "default_max_concurrent_msg_handlers")]
    pub max_concurrent_msg_handlers: u32,
    /// 每轮并发发送的 socket 数量（默认1，上限8）
    #[serde(default = "default_concurrent_sockets")]
    pub concurrent_sockets: usize,
    /// 入站来源节点集合上限（超过时清空，防止无界增长）
    #[serde(default = "default_inbound_sources_max")]
    pub inbound_sources_max: usize,
}

/// NAT 穿透配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatConfig {
    /// 是否启用 NAT 自动端口映射
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 映射租期（秒，0=永久，建议 3600）
    #[serde(default = "default_nat_lease")]
    pub lease_duration: u32,
    /// STUN 服务器列表（用于公网可达性自检和 NAT 类型检测）
    #[serde(default = "default_stun_servers")]
    pub stun_servers: Vec<String>,
    /// 是否启用公网可达性自检
    #[serde(default = "default_true")]
    pub enable_reachability_check: bool,
}

fn default_nat_lease() -> u32 {
    3600
}

fn default_stun_servers() -> Vec<String> {
    vec![
        "stun.l.google.com:19302".to_string(),
        "stun1.l.google.com:19302".to_string(),
        "stun.ekiga.net:3478".to_string(),
    ]
}

impl Default for NatConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            lease_duration: default_nat_lease(),
            stun_servers: default_stun_servers(),
            enable_reachability_check: true,
        }
    }
}

fn default_max_infohashes() -> usize {
    100000
}
fn default_crawler_listen_port() -> u16 {
    6882
}
fn default_utp_port() -> u16 {
    6883
}
fn default_tcp_pex_port() -> u16 {
    6884
}
fn default_crawler_socket_count() -> u16 {
    1
}
fn default_adaptive_rate_limit() -> bool {
    true
}
fn default_rate_limit_window_secs() -> u64 {
    60
}
fn default_crawler_bootstrap_nodes() -> Vec<(String, u16)> {
    vec![
        // 主流公共 DHT 路由器
        ("router.bittorrent.com".to_string(), 6881),
        ("dht.transmissionbt.com".to_string(), 6881),
        ("router.utorrent.com".to_string(), 6881),
        ("dht.aelitis.com".to_string(), 6881),
        ("router.bitcomet.com".to_string(), 6881),
        ("dht.libtorrent.org".to_string(), 25401),
        // 额外公共节点
        ("dht.aria2.net".to_string(), 6881),
        ("router.magnet2torrent.com".to_string(), 6881),
        ("dht.download.free.fr".to_string(), 6881),
        ("dht.cdnbye.com".to_string(), 6881),
        ("dht.pps001.cn".to_string(), 6881),
        ("tracker1.itzmx.com".to_string(), 6881),
    ]
}

fn default_warmup_node_count() -> u32 {
    100_000
}

fn default_warmup_bootstrap_concurrent() -> u32 {
    8
}

fn default_max_concurrent_msg_handlers() -> u32 {
    0 // 0 = 自动（CPU 核数 / 4）
}

fn default_concurrent_sockets() -> usize {
    1
}

fn default_inbound_sources_max() -> usize {
    100_000
}

impl Default for CrawlerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_infohashes: default_max_infohashes(),
            listen_port: default_crawler_listen_port(),
            utp_port: default_utp_port(),
            tcp_pex_port: default_tcp_pex_port(),
            socket_count: default_crawler_socket_count(),
            adaptive_rate_limit: default_adaptive_rate_limit(),
            rate_limit_window_secs: default_rate_limit_window_secs(),
            bootstrap_nodes: default_crawler_bootstrap_nodes(),
            warmup_node_count: default_warmup_node_count(),
            warmup_bootstrap_concurrent: default_warmup_bootstrap_concurrent(),
            max_concurrent_msg_handlers: default_max_concurrent_msg_handlers(),
            concurrent_sockets: default_concurrent_sockets(),
            inbound_sources_max: default_inbound_sources_max(),
        }
    }
}

// ---------------------------------------------------------------------------
// 自适应控制器配置（ICC 预测式自适应）
// ---------------------------------------------------------------------------

/// 自适应控制器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveConfig {
    /// 是否启用自适应控制器
    #[serde(default = "default_adaptive_enabled")]
    pub enabled: bool,
    /// 倍率下限
    #[serde(default = "default_adaptive_min_multiplier")]
    pub min_multiplier: f64,
    /// 倍率上限
    #[serde(default = "default_adaptive_max_multiplier")]
    pub max_multiplier: f64,
    /// 冷启动轮数（此阶段倍率固定为 1.0）
    #[serde(default = "default_adaptive_warmup_rounds")]
    pub warmup_rounds: u64,
    /// 独立模式目标响应率
    #[serde(default = "default_adaptive_target_rate_standalone")]
    pub target_rate_standalone: f64,
    /// 提升步长（预测达标时倍率乘数）
    #[serde(default = "default_adaptive_rate_step_up")]
    pub rate_step_up: f64,
    /// 轻度下降步长（预测低于目标但高于 70% 目标时）
    #[serde(default = "default_adaptive_rate_step_down_light")]
    pub rate_step_down_light: f64,
    /// 重度下降步长（预测低于 70% 目标时）
    #[serde(default = "default_adaptive_rate_step_down_heavy")]
    pub rate_step_down_heavy: f64,
}

fn default_adaptive_enabled() -> bool {
    true
}
fn default_adaptive_min_multiplier() -> f64 {
    0.2
}
fn default_adaptive_max_multiplier() -> f64 {
    2.0
}
fn default_adaptive_warmup_rounds() -> u64 {
    50
}
fn default_adaptive_target_rate_standalone() -> f64 {
    0.30
}
fn default_adaptive_rate_step_up() -> f64 {
    1.1
}
fn default_adaptive_rate_step_down_light() -> f64 {
    0.8
}
fn default_adaptive_rate_step_down_heavy() -> f64 {
    0.5
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            enabled: default_adaptive_enabled(),
            min_multiplier: default_adaptive_min_multiplier(),
            max_multiplier: default_adaptive_max_multiplier(),
            warmup_rounds: default_adaptive_warmup_rounds(),
            target_rate_standalone: default_adaptive_target_rate_standalone(),
            rate_step_up: default_adaptive_rate_step_up(),
            rate_step_down_light: default_adaptive_rate_step_down_light(),
            rate_step_down_heavy: default_adaptive_rate_step_down_heavy(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PdcConfig::default();
        assert_eq!(config.server.port, 6880);
        assert!(config.super_tracker.enabled);
        assert_eq!(config.super_tracker.interval, 30);
        assert!(config.discoverers.enable_tracker);
        assert!(config.discoverers.enable_lpd);
        assert!(config.crawler.enabled);
    }

    /// 已删除的失效配置项若仍残留在旧配置文件中，应被 serde 静默忽略（向后兼容）。
    #[test]
    fn test_removed_legacy_keys_are_ignored() {
        let yaml = r#"
server:
  work_dir: pdc-data
cache:
  max_cached_peers: 12345
crawler:
  socket_count: 8
  crawl_interval_secs: 60
nat:
  bind_ip: 10.0.0.1
  max_retries: 10
persistence:
  max_hot_in_memory: 9999
  wal_autocheckpoint_pages: 2000
federation:
  heartbeat_interval_secs: 30
log_level: debug
"#;
        let config = PdcConfig::from_yaml(yaml).unwrap();
        assert_eq!(config.log_level, "debug");
        assert_eq!(config.server.port, 6880);
        // 未删除的有效字段仍生效
        assert_eq!(config.crawler.socket_count, 8);
    }

    /// 调度器 4 个「新功能」开关默认必须关闭：接线后行为与改造前一致。
    #[test]
    fn test_scheduler_new_features_default_off() {
        let c = PdcConfig::default();
        assert!(!c.task_scheduler.admission_control_enabled);
        assert!(!c.task_scheduler.random_jitter_enabled);
        assert!(!c.task_scheduler.predictive_scheduling_enabled);
        assert!(!c.task_scheduler.adaptive_interval_enabled);
        // 真正生效的并发度默认值保持不变
        assert_eq!(c.task_scheduler.crawl_concurrency, 4);
        assert_eq!(c.task_scheduler.persistence_concurrency, 1);
        assert_eq!(c.task_scheduler.monitor_concurrency, 2);
        assert_eq!(c.task_scheduler.network_concurrency, 4);
    }

    /// health_check 三个周期字段现在由 TaskScheduler 以默认值下发，默认值与历史一致。
    #[test]
    fn test_health_check_intervals_defaults() {
        let c = PdcConfig::default();
        assert_eq!(c.health_check.interval_secs, 300);
        assert_eq!(c.health_check.cache_cleanup_interval_secs, 600);
        assert_eq!(c.health_check.stats_output_interval_secs, 300);
    }

    /// `task_scheduler.intervals` 的字段级 serde 默认必须与 `impl Default` 一致：
    /// YAML 出现 `task_scheduler:` 节但省略 `intervals` 时，默认覆盖项不得丢失。
    #[test]
    fn test_task_scheduler_intervals_default_consistent() {
        const KEY: &str = "wal_checkpoint_truncate_interval_secs";

        // 路径 1：整个 task_scheduler 节缺失 -> impl Default
        let c1 = PdcConfig::default();
        assert_eq!(c1.task_scheduler.intervals.get(KEY).copied(), Some(300));

        // 路径 2：节存在但省略 intervals -> 字段级 serde 默认
        let yaml = "task_scheduler:\n  crawl_concurrency: 4\n";
        let c2 = PdcConfig::from_yaml(yaml).unwrap();
        assert_eq!(c2.task_scheduler.crawl_concurrency, 4);
        assert_eq!(c2.task_scheduler.intervals.get(KEY).copied(), Some(300));

        // 路径 3：两条路径产出的默认表完全一致
        assert_eq!(c1.task_scheduler.intervals, c2.task_scheduler.intervals);

        // 路径 4：用户显式写 intervals 时仍按「整体替换」语义生效
        let yaml2 = "task_scheduler:\n  intervals:\n    custom_task: 42\n";
        let c3 = PdcConfig::from_yaml(yaml2).unwrap();
        assert_eq!(c3.task_scheduler.intervals.len(), 1);
        assert_eq!(
            c3.task_scheduler.intervals.get("custom_task").copied(),
            Some(42)
        );
    }

    #[test]
    fn test_parse_yaml() {
        let yaml = r#"
server:
  port: 9090
super_tracker:
  interval: 3600
discoverers:
  enable_lpd: true
crawler:
  enabled: true
"#;
        let config = PdcConfig::from_yaml(yaml).unwrap();
        assert_eq!(config.server.port, 9090);
        assert_eq!(config.super_tracker.interval, 3600);
        assert!(config.discoverers.enable_lpd);
        assert!(config.crawler.enabled);
        // 未指定的字段使用默认值
        assert_eq!(config.cache.peer_ttl_secs, 86400);
    }
}
