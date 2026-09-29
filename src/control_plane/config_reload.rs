//! 配置热重载：解析 → diff → 分类 → 应用 → 事件广播。
//!
//! 两个触发源共用同一条 apply 管线（策略与执行分离）：
//! - 周期任务 `config_reloader`（main.rs 注册到 TaskScheduler）：mtime 轮询 + 防抖；
//! - API 手动触发（`POST /api/v1/config/reload`）：跳过 mtime 检查立即重载。
//!
//! 分类规则见 [`crate::config::classify_delta`]：
//! - A 类（纯策略）：限速阈值 / 日志级别 / 超级 Tracker 节配置 → 定向应用到运行时模块；
//! - B 类（调度参数）：任务周期 / 分类并发度 / 调度旋钮 → TaskScheduler 热更接口；
//! - C 类（结构性）：端口 / socket 数 / runtime 线程数等 → 不热切，仅告警提示重启生效。
//!
//! 安全护栏：坏配置不应用（保留旧配置继续运行）、连续失败计数升级告警、
//! 白名单外字段一律按 C 类处理、每次重载打 diff 明细并发布 `Event::ConfigChanged`。
//!
//! ICC 预留（ADR-005）：[`crate::config::ControlSource::Icc`] 为 pk/ICC 意图下发预留，
//! apply 层按「pk 意图 > ICC 策略 > 本地文件 > 默认值」仲裁；P0/P1 阶段 `controlled_by`
//! 恒为 None，ICC P3 接入时在此插入优先级仲裁即可，无需改管线结构。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use parking_lot::RwLock;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::{
    classify_delta, diff_configs, ConfigDelta, ControlSource, DeltaClass, PdcConfig,
};
use crate::crawler::CrawlerEngine;
use crate::data_plane::http_tracker::SuperTrackerState;
use crate::intelligence::task_scheduler::{CategoryConcurrency, SchedulerKnobs, TaskScheduler};

/// 日志级别热更句柄（main.rs init_logging 构建并传入）
pub type LogFilterHandle =
    tracing_subscriber::reload::Handle<EnvFilter, tracing_subscriber::Registry>;

/// 连续解析失败达到该次数后升级为 ERROR（提示人工介入）
const MAX_PARSE_FAILURES_BEFORE_ERROR: u64 = 3;
/// 防抖期间 mtime 仍未稳定的最大轮数（每轮等待 config_reload_debounce_ms）
const MAX_DEBOUNCE_ROUNDS: usize = 3;

/// 一次重载的结果报告（API 响应体 / 日志摘要）
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct ReloadReport {
    /// 配置是否有变化（无变化时不发布事件）
    pub changed: bool,
    /// A 类：已应用到运行时模块的变更
    pub applied: Vec<String>,
    /// B 类：已应用到 TaskScheduler 的变更
    pub scheduler_applied: Vec<String>,
    /// C 类：需重启生效的变更
    pub restart_required: Vec<String>,
    /// 本应应用但被跳过的变更（如目标模块未启用）
    pub skipped: Vec<String>,
    /// 解析/应用过程中的错误
    pub errors: Vec<String>,
}

impl ReloadReport {
    /// 单行日志摘要
    pub fn summary(&self) -> String {
        format!(
            "applied={} scheduler={} restart_required={} skipped={} errors={}",
            self.applied.len(),
            self.scheduler_applied.len(),
            self.restart_required.len(),
            self.skipped.len(),
            self.errors.len()
        )
    }
}

/// 配置热重载器。见模块级文档。
pub struct ConfigReloader {
    /// 被监听的配置文件路径
    config_path: Option<String>,
    /// 当前生效配置快照（与 AppState.config 共享同一 Arc）
    snapshot: Arc<RwLock<PdcConfig>>,
    /// 控制面：apply 完成后经 update_config 更新配置并发布 ConfigChanged 事件
    control_plane: super::ControlPlane,
    /// 超级 Tracker 状态（A 类：super_tracker.* 整节热替换）
    super_tracker: Option<Arc<SuperTrackerState>>,
    /// 任务调度器（B 类：周期/并发度/旋钮热更）
    task_scheduler: Option<Arc<TaskScheduler>>,
    /// 爬虫引擎（A 类：rate_limit_* 阈值热更）
    crawler: Option<Arc<CrawlerEngine>>,
    /// 日志过滤器热更句柄（A 类：log_level）
    log_filter: Option<LogFilterHandle>,
    /// 上次已处理的文件 mtime（None = 尚未建立基线）
    last_mtime: Mutex<Option<std::time::SystemTime>>,
    /// 连续解析失败计数（成功后清零）
    consecutive_failures: AtomicU64,
}

impl ConfigReloader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config_path: Option<String>,
        snapshot: Arc<RwLock<PdcConfig>>,
        control_plane: super::ControlPlane,
        super_tracker: Option<Arc<SuperTrackerState>>,
        task_scheduler: Option<Arc<TaskScheduler>>,
        crawler: Option<Arc<CrawlerEngine>>,
        log_filter: Option<LogFilterHandle>,
    ) -> Self {
        Self {
            config_path,
            snapshot,
            control_plane,
            super_tracker,
            task_scheduler,
            crawler,
            log_filter,
            last_mtime: Mutex::new(None),
            consecutive_failures: AtomicU64::new(0),
        }
    }

    /// 周期任务入口：mtime 未变化返回 None；变化则防抖后重载。
    ///
    /// 解析失败时不推进 mtime 基线 → 下个周期自动重试（无需再次保存文件）。
    pub async fn check_and_reload(&self) -> Option<ReloadReport> {
        let path = self.config_path.clone()?;
        let current = std::fs::metadata(&path)
            .ok()
            .and_then(|m| m.modified().ok())?;
        {
            let mut last = self.last_mtime.lock().unwrap();
            match *last {
                None => {
                    // 首轮：建立基线，不触发重载
                    *last = Some(current);
                    return None;
                }
                Some(prev) if prev == current => return None,
                Some(_) => {}
            }
        }

        // 防抖：等待编辑器写完（mtime 稳定为止，最多 MAX_DEBOUNCE_ROUNDS 轮）
        let debounce_ms = self.snapshot.read().config_reload_debounce_ms;
        let mut stable = current;
        for _ in 0..MAX_DEBOUNCE_ROUNDS {
            if debounce_ms > 0 {
                tokio::time::sleep(Duration::from_millis(debounce_ms)).await;
            }
            match std::fs::metadata(&path)
                .ok()
                .and_then(|m| m.modified().ok())
            {
                Some(next) if next != stable => stable = next,
                _ => break,
            }
        }

        let report = self.reload_from_file().await;
        if report.errors.is_empty() {
            // 成功（含"无变化"）才推进基线；失败保留基线，下周期自动重试
            *self.last_mtime.lock().unwrap() = Some(stable);
        }
        Some(report)
    }

    /// API 手动触发：跳过 mtime 检查，立即重载。
    pub async fn reload_now(&self) -> ReloadReport {
        self.reload_from_file().await
    }

    async fn reload_from_file(&self) -> ReloadReport {
        let mut report = ReloadReport::default();
        let Some(path) = &self.config_path else {
            report
                .errors
                .push("未指定配置文件路径（--config 或工作目录 config/config.yaml）".to_string());
            return report;
        };
        let new_config = match PdcConfig::from_file(path) {
            Ok(c) => c,
            Err(e) => {
                // 护栏：坏配置绝不应用，保留旧配置继续运行
                let n = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
                let msg = format!("解析配置文件失败（保留旧配置继续运行）: {e}");
                if n >= MAX_PARSE_FAILURES_BEFORE_ERROR {
                    error!("[config_reload] 连续第 {n} 次失败: {msg}");
                } else {
                    warn!("[config_reload] 第 {n} 次失败: {msg}");
                }
                report.errors.push(msg);
                return report;
            }
        };
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.apply(new_config, ControlSource::FileWatcher, &mut report)
            .await;
        report
    }

    /// 差异对比 + 分类应用 + 快照/事件发布。ICC 接入时复用本方法（换 source）。
    pub async fn apply(
        &self,
        new_config: PdcConfig,
        source: ControlSource,
        report: &mut ReloadReport,
    ) {
        let old_config = self.snapshot.read().clone();
        let deltas: Vec<ConfigDelta> = diff_configs(&old_config, &new_config)
            .into_iter()
            .map(|mut d| {
                d.source = source;
                d
            })
            .collect();
        if deltas.is_empty() {
            info!("[config_reload] 配置无变化");
            return;
        }
        report.changed = true;

        let mut rate_limit_delta = false;
        let mut super_tracker_delta = false;
        let mut new_log_level: Option<String> = None;
        let mut knobs_changed = false;
        let mut concurrency_changed = false;
        let mut interval_deltas: Vec<ConfigDelta> = Vec::new();

        for d in &deltas {
            match classify_delta(&d.path) {
                DeltaClass::ApplyPolicy => {
                    if d.path.starts_with("crawler.rate_limit_") {
                        rate_limit_delta = true;
                    } else if d.path.starts_with("super_tracker.") {
                        super_tracker_delta = true;
                    } else if d.path == "log_level" {
                        new_log_level = Some(new_config.log_level.clone());
                    }
                    report.applied.push(format!("{}: {}", d.path, d.summary()));
                }
                DeltaClass::ApplyScheduler => {
                    if let Some(task_id) = d.path.strip_prefix("task_scheduler.intervals.") {
                        let mut it = d.clone();
                        it.path = task_id.to_string();
                        interval_deltas.push(it);
                    } else if d.path.starts_with("task_scheduler.")
                        && d.path.ends_with("_concurrency")
                    {
                        concurrency_changed = true;
                    } else if d.path.starts_with("task_scheduler.") {
                        knobs_changed = true;
                    }
                    report
                        .scheduler_applied
                        .push(format!("{}: {}", d.path, d.summary()));
                }
                DeltaClass::RestartRequired => {
                    warn!(
                        "[config_reload] {} 变化需重启生效（{}）",
                        d.path,
                        d.summary()
                    );
                    report
                        .restart_required
                        .push(format!("{}: {}", d.path, d.summary()));
                }
            }
        }

        // --- A 类：纯策略参数 ---
        if let Some(level) = new_log_level {
            match &self.log_filter {
                Some(h) => match EnvFilter::try_new(&level)
                    .map_err(|e| e.to_string())
                    .and_then(|f| h.reload(f).map_err(|e| e.to_string()))
                {
                    Ok(()) => info!("[config_reload] 日志级别已热更: {level}"),
                    Err(e) => report
                        .errors
                        .push(format!("log_level 热更失败 ({level}): {e}")),
                },
                None => report
                    .skipped
                    .push("log_level: 日志过滤器句柄不可用".to_string()),
            }
        }
        if rate_limit_delta {
            match self.crawler.as_ref().and_then(|c| c.rate_limiter()) {
                Some(rl) => {
                    let c = &new_config.crawler;
                    rl.update_config(
                        c.rate_limit_enter_threshold,
                        c.rate_limit_exit_threshold,
                        c.rate_limit_min_samples as usize,
                        c.rate_limit_throttle_skip_ratio,
                    );
                    info!(
                        "[config_reload] 爬虫限速阈值已热更: enter={} exit={} min_samples={} skip_ratio={}",
                        c.rate_limit_enter_threshold,
                        c.rate_limit_exit_threshold,
                        c.rate_limit_min_samples,
                        c.rate_limit_throttle_skip_ratio
                    );
                }
                None => report
                    .skipped
                    .push("crawler.rate_limit_*: 爬虫未启用或未启用自适应限速".to_string()),
            }
        }
        if super_tracker_delta {
            match &self.super_tracker {
                Some(st) => {
                    st.update_config(new_config.super_tracker.clone()).await;
                    info!("[config_reload] 超级 Tracker 节配置已热更");
                }
                None => report
                    .skipped
                    .push("super_tracker.*: 超级 Tracker 状态不可用".to_string()),
            }
        }

        // --- B 类：调度参数 ---
        if let Some(ts) = &self.task_scheduler {
            for d in &interval_deltas {
                self.apply_task_interval(ts, &d.path, d.old.as_u64(), d.new.as_u64(), report);
            }
            if concurrency_changed {
                let c = &new_config.task_scheduler;
                ts.update_category_concurrency(CategoryConcurrency {
                    crawl: c.crawl_concurrency,
                    persistence: c.persistence_concurrency,
                    monitor: c.monitor_concurrency,
                    network: c.network_concurrency,
                    federation: c.federation_concurrency,
                    tracker: c.tracker_concurrency,
                });
                info!("[config_reload] TaskScheduler 分类并发度已热更");
            }
            if knobs_changed {
                ts.update_knobs(SchedulerKnobs::from_config(&new_config.task_scheduler));
                info!("[config_reload] TaskScheduler 调度旋钮已热更");
            }
            // reloader 自身周期（config_reload_interval_secs 变化 → 改自己的任务周期）
            if old_config.config_reload_interval_secs != new_config.config_reload_interval_secs {
                let secs = new_config.config_reload_interval_secs;
                if secs > 0 && ts.update_interval("config_reloader", Duration::from_secs(secs)) {
                    info!("[config_reload] config_reloader 周期已热更: {secs}s");
                }
            }
        } else if !interval_deltas.is_empty() || knobs_changed || concurrency_changed {
            report
                .skipped
                .push("task_scheduler.*: 调度器句柄不可用".to_string());
        }
        // config_reload_debounce_ms 无需定向应用：check_and_reload 每轮从快照读取

        // --- 发布：更新快照 + 经控制面广播 ConfigChanged（ws 转发给前端） ---
        *self.snapshot.write() = new_config.clone();
        self.control_plane.update_config(new_config);

        info!(
            "[config_reload] 配置热重载完成（{}）: applied=[{}] scheduler=[{}] restart_required=[{}]",
            source_text(source),
            report.applied.join("; "),
            report.scheduler_applied.join("; "),
            report.restart_required.join("; ")
        );
    }

    /// 应用单条任务周期变更。
    ///
    /// 单位推断：intervals 表大多数任务为秒，少数（fed_gossip_* 等）为毫秒，
    /// 与注册站点 `get_interval_secs` 的用法一致。这里用「旧值换算后 == 任务当前
    /// 周期」反推单位，推断不出（外部改过周期/首次对不上）则跳过并记录，绝不猜。
    fn apply_task_interval(
        &self,
        ts: &Arc<TaskScheduler>,
        task_id: &str,
        old_value: Option<u64>,
        new_value: Option<u64>,
        report: &mut ReloadReport,
    ) {
        // 任务不存在是常见情形（intervals 表可预写未启用任务的覆盖项），单独提示
        let Some(current) = ts.task_interval(task_id) else {
            report
                .skipped
                .push(format!("intervals.{task_id}: 任务未注册（可能未启用）"));
            return;
        };
        let Some(new_v) = new_value else {
            report
                .skipped
                .push(format!("intervals.{task_id}: 新值非数值"));
            return;
        };
        let new_interval = match old_value {
            // 单位推断：intervals 表大多数任务为秒，少数（fed_gossip_* 等）为毫秒，
            // 与注册站点 `get_interval_secs` 的用法一致。用「旧值换算后 == 任务当前
            // 周期」反推单位，推断不出（键为新增/外部改过周期）则跳过并记录，绝不猜。
            Some(old_v) if Duration::from_secs(old_v) == current => Duration::from_secs(new_v),
            Some(old_v) if Duration::from_millis(old_v) == current => Duration::from_millis(new_v),
            _ => {
                report.skipped.push(format!(
                    "intervals.{task_id}: 无法推断时间单位（当前 {current:?} 与旧值不匹配），跳过"
                ));
                return;
            }
        };
        if ts.update_interval(task_id, new_interval) {
            info!("[config_reload] 任务周期已热更: {task_id} -> {new_interval:?}");
        } else {
            report
                .skipped
                .push(format!("intervals.{task_id}: 更新失败"));
        }
    }
}

fn source_text(source: ControlSource) -> &'static str {
    match source {
        ControlSource::FileWatcher => "来源:文件监听",
        ControlSource::Api => "来源:API",
        ControlSource::Icc => "来源:ICC",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discoverers::DiscovererRegistry;
    use crate::event_bus::EventBus;
    use crate::intelligence::task_scheduler::{TaskCategory, TaskMetadata};

    fn make_reloader(
        snapshot: Arc<RwLock<PdcConfig>>,
        ts: Option<Arc<TaskScheduler>>,
    ) -> ConfigReloader {
        let registry = Arc::new(DiscovererRegistry::new());
        let cp = crate::control_plane::ControlPlane::new(
            snapshot.read().clone(),
            registry,
            EventBus::default(),
        );
        ConfigReloader::new(None, snapshot, cp, None, ts, None, None)
    }

    #[tokio::test]
    async fn test_apply_updates_snapshot_and_skips_unavailable_targets() {
        let old = PdcConfig::default();
        let base = PdcConfig::default();
        let new = PdcConfig {
            crawler: crate::config::CrawlerConfig {
                // A 类，但爬虫句柄为 None → skipped
                rate_limit_enter_threshold: 0.25,
                ..base.crawler.clone()
            },
            // A 类，但日志句柄为 None → skipped
            log_level: "debug".to_string(),
            // C 类 → restart_required
            server: crate::config::ServerConfig {
                port: 9999,
                ..base.server.clone()
            },
            ..base
        };

        let snapshot = Arc::new(RwLock::new(old));
        let reloader = make_reloader(snapshot.clone(), None);
        let mut report = ReloadReport::default();
        reloader
            .apply(new, ControlSource::FileWatcher, &mut report)
            .await;

        assert!(report.changed);
        assert_eq!(report.errors.len(), 0);
        assert!(report
            .restart_required
            .iter()
            .any(|s| s.contains("server.port")));
        assert!(report.skipped.iter().any(|s| s.contains("rate_limit")));
        assert!(report.skipped.iter().any(|s| s.contains("log_level")));
        // 快照已更新为新配置
        assert_eq!(snapshot.read().server.port, 9999);
        assert_eq!(snapshot.read().crawler.rate_limit_enter_threshold, 0.25);
    }

    #[tokio::test]
    async fn test_apply_scheduler_interval_unit_inference() {
        let mut old = PdcConfig::default();
        let mut new = PdcConfig::default();
        // 旧配置显式写 30（与任务注册周期一致）→ 反推单位为秒；新值 5 → 热更为 5s
        old.task_scheduler
            .intervals
            .insert("crawler_tick".into(), 30);
        new.task_scheduler
            .intervals
            .insert("crawler_tick".into(), 5);

        let snapshot = Arc::new(RwLock::new(old));
        let ts = Arc::new(TaskScheduler::new());
        ts.register(
            TaskMetadata::new("crawler_tick", "爬虫 tick", Duration::from_secs(30)),
            || async { Ok(()) },
        );
        let reloader = make_reloader(snapshot.clone(), Some(ts.clone()));
        let mut report = ReloadReport::default();
        reloader
            .apply(new, ControlSource::FileWatcher, &mut report)
            .await;

        assert!(report.errors.is_empty(), "errors={:?}", report.errors);
        assert_eq!(
            ts.task_interval("crawler_tick"),
            Some(Duration::from_secs(5))
        );
        assert!(report
            .scheduler_applied
            .iter()
            .any(|s| s.contains("task_scheduler.intervals.crawler_tick")));
    }

    #[tokio::test]
    async fn test_apply_scheduler_interval_unit_mismatch_skipped() {
        let mut old = PdcConfig::default();
        let mut new = PdcConfig::default();
        old.task_scheduler
            .intervals
            .insert("crawler_tick".into(), 30);
        new.task_scheduler
            .intervals
            .insert("crawler_tick".into(), 5);

        let snapshot = Arc::new(RwLock::new(old));
        let ts = Arc::new(TaskScheduler::new());
        // 注册周期 45s，与旧值 30（秒/毫秒）都对不上 → 单位无法推断 → 跳过
        ts.register(
            TaskMetadata::new("crawler_tick", "爬虫 tick", Duration::from_secs(45)),
            || async { Ok(()) },
        );
        let reloader = make_reloader(snapshot.clone(), Some(ts.clone()));
        let mut report = ReloadReport::default();
        reloader
            .apply(new, ControlSource::FileWatcher, &mut report)
            .await;

        assert_eq!(
            ts.task_interval("crawler_tick"),
            Some(Duration::from_secs(45))
        );
        assert!(report
            .skipped
            .iter()
            .any(|s| s.contains("无法推断时间单位")));
    }

    #[tokio::test]
    async fn test_apply_unregistered_task_skipped() {
        let old = PdcConfig::default();
        let mut new = PdcConfig::default();
        new.task_scheduler
            .intervals
            .insert("no_such_task".into(), 7);

        let snapshot = Arc::new(RwLock::new(old));
        let ts = Arc::new(TaskScheduler::new());
        let reloader = make_reloader(snapshot.clone(), Some(ts));
        let mut report = ReloadReport::default();
        reloader
            .apply(new, ControlSource::FileWatcher, &mut report)
            .await;
        assert!(report.skipped.iter().any(|s| s.contains("任务未注册")));
    }

    #[tokio::test]
    async fn test_apply_concurrency_and_knobs() {
        let old = PdcConfig::default();
        let base = PdcConfig::default();
        let new = PdcConfig {
            task_scheduler: crate::config::TaskSchedulerConfig {
                crawl_concurrency: 9,
                admission_cpu_threshold: 0.55,
                ..base.task_scheduler.clone()
            },
            ..base
        };

        let snapshot = Arc::new(RwLock::new(old));
        let ts = Arc::new(TaskScheduler::new());
        let reloader = make_reloader(snapshot.clone(), Some(ts.clone()));
        let mut report = ReloadReport::default();
        reloader
            .apply(new, ControlSource::FileWatcher, &mut report)
            .await;

        assert_eq!(ts.max_concurrency_for(TaskCategory::Crawl), 9);
        assert_eq!(ts.summary().max_concurrency.crawl, 9);
        assert!(!report.scheduler_applied.is_empty());
    }

    #[tokio::test]
    async fn test_apply_interval_secs_updates_reloader_task() {
        let old = PdcConfig::default();
        let new = PdcConfig {
            config_reload_interval_secs: 60,
            ..PdcConfig::default()
        };

        let snapshot = Arc::new(RwLock::new(old));
        let ts = Arc::new(TaskScheduler::new());
        ts.register(
            TaskMetadata::new("config_reloader", "配置热更新监听", Duration::from_secs(30)),
            || async { Ok(()) },
        );
        let reloader = make_reloader(snapshot.clone(), Some(ts.clone()));
        let mut report = ReloadReport::default();
        reloader
            .apply(new, ControlSource::FileWatcher, &mut report)
            .await;
        assert_eq!(
            ts.task_interval("config_reloader"),
            Some(Duration::from_secs(60))
        );
    }

    #[tokio::test]
    async fn test_no_change_report_unchanged() {
        let old = PdcConfig::default();
        let new = PdcConfig::default();
        let snapshot = Arc::new(RwLock::new(old));
        let reloader = make_reloader(snapshot.clone(), None);
        let mut report = ReloadReport::default();
        reloader.apply(new, ControlSource::Api, &mut report).await;
        assert!(!report.changed);
        // 无变化时快照保持原值（默认配置）
        assert_eq!(snapshot.read().config_reload_interval_secs, 30);
    }
}
