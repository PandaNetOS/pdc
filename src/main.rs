//! PeerDiscoveryCenter 二进制入口
//!
//! 启动独立的 PDC 服务，包含：
//! - 超级 Tracker（/announce、/scrape）
//! - REST API（/health、/api/v1/*）
//! - 健康检查后台任务
//! - 爬虫引擎（可选）
//!
//! 用法：
//! ```text
//! pdc                    # 使用默认配置启动
//! pdc --config config.yaml  # 使用指定配置文件
//! ```

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

// 全局使用mimalloc高性能内存分配器，减少Heap碎片
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// oplog 行数缓存预热的延迟（秒）：让进程先完成基础启动，再做一次 COUNT 校准。
const OPLOG_LEN_PREWARM_DELAY_SECS: u64 = 5;

use parking_lot::RwLock;
use rand::Rng;
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

use PeerDiscoveryCenter::config::get_interval_secs;
use PeerDiscoveryCenter::config::PdcConfig;
use PeerDiscoveryCenter::control_plane::ControlPlane;
use PeerDiscoveryCenter::crawler::Crawler;
use PeerDiscoveryCenter::crawler::CrawlerEngine;
use PeerDiscoveryCenter::data_plane::http_tracker::SuperTrackerState;
use PeerDiscoveryCenter::data_plane::relay::RelayServer;
use PeerDiscoveryCenter::data_plane::stats_snapshot;
use PeerDiscoveryCenter::data_plane::udp_tracker::UdpTrackerServer;
use PeerDiscoveryCenter::data_plane::{AppState, DataPlane};
use PeerDiscoveryCenter::discoverers::DiscovererRegistry;
use PeerDiscoveryCenter::event_bus::EventBus;
use PeerDiscoveryCenter::federation::FederationService;
use PeerDiscoveryCenter::firewall::FirewallManager;
use PeerDiscoveryCenter::health_check::HealthCheckTask;
use PeerDiscoveryCenter::intelligence::{
    AdaptiveController, AvailabilityCalculator, CategoryConcurrency, DhtActivityTracker,
    PeerHistoryManager, ResourceLevel, ResourceProfile, TaskCategory, TaskMetadata, TaskPriority,
    TaskScheduler,
};
use PeerDiscoveryCenter::nat::NatManager;
use PeerDiscoveryCenter::net::socket_opts::create_udp_socket;
use PeerDiscoveryCenter::port_allocator::PortAllocator;
use PeerDiscoveryCenter::services::{MetadataService, ScrapeService};
use PeerDiscoveryCenter::storage::{
    InfohashRepository, NodeRepository, TieredCacheConfig, TrackerRepository,
};

/// 工作目录管理（pnos-spec 未提供 workdir 模块，pdc 自实现）
/// 目录结构：<root>/config/config.yaml, <root>/data/*.db, <root>/logs/
#[derive(Debug, Clone)]
struct WorkDir {
    pub root: std::path::PathBuf,
    pub logs_dir: std::path::PathBuf,
}

impl WorkDir {
    fn standalone(dir: impl AsRef<std::path::Path>, _name: &str) -> Self {
        let root = dir.as_ref().to_path_buf();
        Self {
            logs_dir: root.join("logs"),
            root,
        }
    }

    fn auto_detect(_name: &str) -> Self {
        let root = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
        Self {
            logs_dir: root.join("logs"),
            root,
        }
    }

    fn ensure_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(self.root.join("config"))?;
        std::fs::create_dir_all(self.root.join("data"))?;
        std::fs::create_dir_all(&self.logs_dir)?;
        Ok(())
    }

    fn config_file(&self) -> std::path::PathBuf {
        self.root.join("config").join("config.yaml")
    }

    fn db_file(&self, name: &str) -> std::path::PathBuf {
        self.root.join("data").join(format!("{}.db", name))
    }

    fn node_id_file(&self) -> std::path::PathBuf {
        self.root.join("data").join("node_id")
    }
}

/// P1-5：把「受影响 L2 列表 + 从 DB 精确加载到的 (key, data_hash)」整理成
/// `MerkleTree::recompute_l2_subset_from_db` 所需的入参：
/// - `by_l1`：L1 → 该 L1 下本次加载到的条目（仅含受影响的 L2 的条目即可）；
/// - `dirty_in_l1`：L1 → 该 L1 下需要重算的 L2 子集。
///
/// 对被删除后变空的 L2（加载不到条目），`recompute_l2_subset_from_db` 会写回 `[0u8;32], 0`
/// 的「缺席」表示，从而让删除正确折进 Merkle。
type L1EntryMap = FxHashMap<u16, Vec<(Vec<u8>, Vec<u8>)>>;
type L1ShardSet = FxHashMap<u16, FxHashSet<u32>>;

fn build_l2_recompute_input(
    merkle: &PeerDiscoveryCenter::federation::merkle::MerkleTree,
    l2s: &[u32],
    keys_hashes: Vec<(Vec<u8>, Vec<u8>)>,
) -> (L1EntryMap, L1ShardSet) {
    let mut dirty_in_l1: L1ShardSet = FxHashMap::default();
    for &l2 in l2s {
        dirty_in_l1
            .entry(PeerDiscoveryCenter::federation::merkle::MerkleTree::l1_for_l2(l2))
            .or_default()
            .insert(l2);
    }
    let mut by_l1: L1EntryMap = FxHashMap::default();
    for (k, h) in keys_hashes {
        let l1 = PeerDiscoveryCenter::federation::merkle::MerkleTree::l1_for_l2(
            merkle.l2_shard_for_key(&k),
        );
        by_l1.entry(l1).or_default().push((k, h));
    }
    (by_l1, dirty_in_l1)
}

fn main() -> anyhow::Result<()> {
    // 0. 初始化统一工作目录（pnos-spec WorkDir）
    //    --work-dir 参数 → Standalone 模式；PNOS_APP_ID 环境变量 → Managed 模式；否则 Standalone(当前目录)
    let work_dir = if let Some(dir) = parse_work_dir_arg() {
        WorkDir::standalone(&dir, "pdc")
    } else {
        WorkDir::auto_detect("pdc")
    };
    if let Err(e) = work_dir.ensure_dirs() {
        eprintln!("[main] 创建工作目录失败: {}", e);
    }
    eprintln!("[main] 工作目录: {}", work_dir.root.display());

    // 0.1 安装崩溃捕获（panic hook + Windows SEH），日志写入 work_dir.logs_dir/
    install_crash_handler(&work_dir.logs_dir);

    // 0.2 安装 Windows 控制台控制处理器（防止后台运行时父控制台关闭导致 CTRL_CLOSE_EVENT 瞬间终止进程）
    #[cfg(windows)]
    install_console_ctrl_handler();

    // 1. 初始化日志（级别：RUST_LOG 环境变量 > 配置文件 log_level > info）
    init_logging(&peek_log_level(&work_dir));

    // 1.5 初始化 Prometheus metrics
    PeerDiscoveryCenter::data_plane::metrics::init_metrics();
    info!("[main] Prometheus metrics 已初始化");

    info!("========================================");
    info!("PeerDiscoveryCenter v{} 启动", PeerDiscoveryCenter::VERSION);
    info!("========================================");

    // 2. 加载配置
    //    优先级：-c/--config 显式指定 > work_dir.config_file() > 自动生成默认配置
    let explicit_config = parse_config_path();
    let config_path = match explicit_config {
        Some(p) => Some(p),
        None => {
            let p = work_dir.config_file();
            if p.exists() {
                info!("[main] 自动发现配置文件: {}", p.display());
                Some(p.to_string_lossy().to_string())
            } else {
                // 自动生成默认配置文件
                let default_config = PdcConfig::default();
                match serde_yaml::to_string(&default_config) {
                    Ok(yaml) => {
                        let _ = std::fs::write(&p, &yaml);
                        info!("[main] 未发现配置文件，已生成默认配置: {}", p.display());
                    }
                    Err(e) => warn!("[main] 生成默认配置文件失败: {}", e),
                }
                Some(p.to_string_lossy().to_string())
            }
        }
    };
    let mut config = match &config_path {
        Some(path) => {
            info!("[main] 从 {} 加载配置", path);
            PdcConfig::load_or_default(path)
        }
        None => {
            info!("[main] 使用默认配置");
            PdcConfig::default()
        }
    };

    // 2.1 统一数据路径到 work_dir.db_file("pdc")（工作目录规范）
    config.storage.path = work_dir.db_file("pdc").to_string_lossy().to_string();
    info!("[main] 数据路径: {}", config.storage.path);

    // 构建六个独立 Tokio runtime，彻底隔离爬虫 / 联邦 / Tracker / API / 调度器 / 持久化
    let worker_threads = if config.runtime_worker_threads == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8)
    } else {
        config.runtime_worker_threads
    };
    let worker_threads = worker_threads.max(4);
    let tracker_threads = config.tracker_runtime_threads.max(2);
    let api_threads = config.api_runtime_threads.max(2);
    let federation_threads = if config.federation_runtime_threads == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8)
    } else {
        config.federation_runtime_threads
    };
    let federation_threads = federation_threads.max(4);
    let scheduler_threads = config.scheduler_runtime_threads.max(2);
    let persistence_threads = config.persistence_runtime_threads.max(2);
    info!(
        "[main] 六 runtime 隔离: crawler={}, federation={}, tracker={}, api={}, scheduler={}, persistence={}",
        worker_threads, federation_threads, tracker_threads, api_threads, scheduler_threads, persistence_threads
    );

    let crawler_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .thread_name("pdc-crawler")
        .enable_all()
        .build()?;
    let federation_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(federation_threads)
        .thread_name("pdc-federation")
        .enable_all()
        .build()?;
    let tracker_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(tracker_threads)
        .thread_name("pdc-tracker")
        .enable_all()
        .build()?;
    let api_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(api_threads)
        .thread_name("pdc-api")
        .enable_all()
        .build()?;
    let scheduler_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(scheduler_threads)
        .thread_name("pdc-scheduler")
        .enable_all()
        .build()?;
    let persistence_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(persistence_threads)
        .thread_name("pdc-persistence")
        .enable_all()
        .build()?;

    let crawler_handle = crawler_runtime.handle().clone();
    let tracker_handle = tracker_runtime.handle().clone();
    let api_handle = api_runtime.handle().clone();
    let federation_handle = federation_runtime.handle().clone();
    let scheduler_handle = scheduler_runtime.handle().clone();
    let persistence_handle = persistence_runtime.handle().clone();
    let shutdown_timeout = std::time::Duration::from_secs(config.runtime_shutdown_timeout_secs);

    let result = crawler_runtime.block_on(async_main(
        work_dir,
        config_path,
        config,
        tracker_handle,
        api_handle,
        crawler_handle,
        federation_handle,
        scheduler_handle,
        persistence_handle,
    ));

    // 关闭 tracker/api/federation/scheduler/persistence runtime（crawler_runtime 随 block_on 返回自然结束）
    info!("[main] 关闭 tracker_runtime...");
    tracker_runtime.shutdown_timeout(shutdown_timeout);
    info!("[main] 关闭 api_runtime...");
    api_runtime.shutdown_timeout(shutdown_timeout);
    info!("[main] 关闭 federation_runtime...");
    federation_runtime.shutdown_timeout(shutdown_timeout);
    info!("[main] 关闭 scheduler_runtime...");
    scheduler_runtime.shutdown_timeout(shutdown_timeout);
    info!("[main] 关闭 persistence_runtime...");
    persistence_runtime.shutdown_timeout(shutdown_timeout);
    info!("[main] 所有 runtime 已关闭");

    result
}

/// 异步主逻辑：所有 .await 操作在此运行（由 main 中的手动 runtime 驱动）
#[allow(clippy::too_many_arguments)]
async fn async_main(
    work_dir: WorkDir,
    config_path: Option<String>,
    mut config: PdcConfig,
    tracker_handle: tokio::runtime::Handle,
    api_handle: tokio::runtime::Handle,
    crawler_handle: tokio::runtime::Handle,
    federation_handle: tokio::runtime::Handle,
    scheduler_handle: tokio::runtime::Handle,
    persistence_handle: tokio::runtime::Handle,
) -> anyhow::Result<()> {
    // 2.2 加载或生成 PEX/uTP 节点身份（持久化到 work_dir.node_id_file()）
    let pex_node_id = load_or_generate_node_id(&work_dir.node_id_file());

    // 2.4 声明 crawler socket 列表（端口探测后填充，传递给 CrawlerEngine）
    let mut crawler_sockets: Vec<Arc<tokio::net::UdpSocket>> = Vec::new();

    // 2.5 端口自动探测（零配置核心：自动寻找可用端口组）
    // 必须在创建任何服务/NAT/联邦/HTTP 之前完成，确保后续所有模块使用实际分配的端口。
    if config.port_auto_alloc {
        info!("[main] 启动端口自动探测（step={}）...", config.port_step);
        let allocator = PortAllocator::from_config(&config);
        match allocator.allocate() {
            Ok(mut allocation) => {
                info!(
                    "[main] 端口探测成功，offset={}，API端口={}，联邦端口={}",
                    allocation.offset, allocation.ports.api_port, allocation.ports.federation_port
                );
                allocation.apply_to_config(&mut config);
                // 同步 federation.api_port（与 API 端口一致）
                config.federation.api_port = config.server.api_port;
                // 提取 crawler socket（不释放，转为 tokio UdpSocket 后传给 CrawlerEngine）
                crawler_sockets = allocation
                    .crawler_sockets
                    .drain(..)
                    .flatten()
                    .filter_map(|std_s| tokio::net::UdpSocket::from_std(std_s).ok())
                    .map(Arc::new)
                    .collect();
                info!("[main] 提取 {} 个 crawler socket", crawler_sockets.len());
                // 释放其他探测 socket（实际监听器会立即重新绑定）
                allocation.release_all();
            }
            Err(e) => {
                warn!("[main] 端口自动探测失败（{}），使用配置端口继续", e);
            }
        }
    }

    info!(
        "[main] 配置: 监听 {}:{}, 超级Tracker={}, 爬虫={}",
        config.server.listen,
        config.server.port,
        config.super_tracker.enabled,
        config.crawler.enabled
    );

    // 3. 创建核心组件
    let event_bus = EventBus::new(4096);
    let registry = Arc::new(DiscovererRegistry::new());

    // 3.5 初始化持久化存储（提前，供所有 repo 使用）
    let storage = if config.storage.enabled {
        info!("[main] 初始化持久化存储: {}", config.storage.path);
        Arc::new(
            PeerDiscoveryCenter::storage::Storage::open_with_config(
                &config.storage.path,
                &config.sqlite,
            )
            .expect("无法打开数据库"),
        )
    } else {
        info!("[main] 持久化存储未启用，使用内存模式");
        Arc::new(PeerDiscoveryCenter::storage::Storage::memory().expect("无法创建内存数据库"))
    };

    // 3.5b shard 列一次性回填（旧库历史数据 shard=0 需修正为正确的 blake3(key)%256）
    if let Err(e) = storage.backfill_shards() {
        warn!("[main] shard 列回填失败（不影响启动）: {}", e);
    }

    // 3.6 创建所有数据层 Repo（统一数据归口）
    // P2: 先创建 WriteQueue/IOScheduler，再注入到各 Repo
    let io_scheduler: Option<Arc<PeerDiscoveryCenter::storage::IoScheduler>> =
        if config.io_scheduler.enabled {
            use PeerDiscoveryCenter::storage::io_scheduler::SchedulerRuntimeConfig;
            let rt_cfg = SchedulerRuntimeConfig {
                max_queue_size: config.io_scheduler.max_queue_size,
                token_bucket_rate: config.io_scheduler.token_bucket_rate,
                token_bucket_max: config.io_scheduler.token_bucket_max,
                low_watermark: config.io_scheduler.low_watermark,
                high_watermark: config.io_scheduler.high_watermark,
                batch_max_size: config.io_scheduler.batch_max_size,
                batch_max_delay: std::time::Duration::from_millis(
                    config.io_scheduler.batch_max_delay_ms,
                ),
                idle_prediction_enabled: config.io_scheduler.idle_prediction_enabled,
                idle_window: std::time::Duration::from_secs(config.io_scheduler.idle_window_secs),
                idle_wait: std::time::Duration::from_millis(config.io_scheduler.idle_wait_ms),
                retry_wait: std::time::Duration::from_millis(config.io_scheduler.retry_wait_ms),
                steady_tick_ms: 10,  // 10ms 时间片，匀速写入
                writes_per_tick: 10, // 每个时间片最多 10 条写入
            };
            let sched = PeerDiscoveryCenter::storage::IoScheduler::new_with_handle(
                storage.connection(),
                rt_cfg,
                &persistence_handle,
            );
            info!(
                "[main] IOScheduler 已启用（令牌桶 {} 行/秒，队列上限 {}）",
                config.io_scheduler.token_bucket_rate, config.io_scheduler.max_queue_size
            );
            Some(sched)
        } else {
            None
        };

    let write_queue = if let Some(ref sched) = io_scheduler {
        Arc::new(PeerDiscoveryCenter::storage::WriteQueue::with_scheduler(
            storage.connection(),
            sched.clone(),
        ))
    } else {
        Arc::new(PeerDiscoveryCenter::storage::WriteQueue::new(
            storage.connection(),
            config.persistence.batch_size,
            std::time::Duration::from_secs(config.persistence.flush_interval_secs),
        ))
    };

    // 3.6 冷热分层缓存配置（从 config.tier 读取，所有 Repo 共享同一配置）
    let tier_cache_config = TieredCacheConfig {
        hot_max_count: config.tier.hot_max_count,
        warm_max_count: config.tier.warm_max_count,
        hot_threshold_secs: config.tier.hot_threshold_secs,
        warm_threshold_secs: config.tier.warm_threshold_secs,
    };
    let tier_enabled = config.tier.enabled;
    info!(
        "[main] 冷热分层缓存: enabled={}, hot_max={}, warm_max={}",
        tier_enabled, config.tier.hot_max_count, config.tier.warm_max_count
    );

    // Peer 条目较大，单独下调 Warm 上限（tier.peer_warm_max_count，默认 5 万），其余 repo 用全局值
    let peer_tier_config = TieredCacheConfig {
        warm_max_count: config.tier.peer_warm_max_count,
        ..tier_cache_config.clone()
    };
    let peer_repo = Arc::new(
        PeerDiscoveryCenter::storage::PeerRepoImpl::with_tier_config(
            storage.clone(),
            peer_tier_config,
            tier_enabled,
        )
        .with_write_queue(write_queue.clone()),
    );
    let infohash_repo = Arc::new(
        PeerDiscoveryCenter::storage::InfohashRepoImpl::with_tier_config(
            storage.clone(),
            tier_cache_config.clone(),
            tier_enabled,
        )
        .with_write_queue(write_queue.clone()),
    );
    let tracker_repo = Arc::new(
        PeerDiscoveryCenter::storage::TrackerRepoImpl::with_tier_config(
            storage.clone(),
            tier_cache_config.clone(),
            tier_enabled,
        )
        .with_write_queue(write_queue.clone()),
    );
    let node_repo = Arc::new(
        PeerDiscoveryCenter::storage::NodeRepoImpl::with_tier_config(
            storage.clone(),
            tier_cache_config.clone(),
            tier_enabled,
        )
        .with_write_queue(write_queue.clone()),
    );
    info!("[main] 数据层 Repo 已初始化（冷热分层已启用）");

    // 3.7 从 SQLite 加载持久化数据
    match tracker_repo.load_all().await {
        Ok(n) if n > 0 => info!("[main] 从 SQLite 加载了 {} 个 Tracker", n),
        _ => {}
    }
    let preload = config.tier.preload_top_n;
    match infohash_repo.load_initial(preload).await {
        Ok(n) if n > 0 => info!("[main] 预加载 {} 个 Infohash（上限 {}）", n, preload),
        _ => {}
    }
    match node_repo.load_initial(preload).await {
        Ok(n) if n > 0 => info!(
            "[main] 从 SQLite 预加载了 {} 个 DHT 节点（上限 {}）",
            n, preload
        ),
        _ => {}
    }
    match peer_repo.load_initial(preload).await {
        Ok(n) if n > 0 => info!("[main] 预加载 {} 个 Peer（上限 {}）", n, preload),
        _ => {}
    }

    // 3.8 创建控制面（注入 tracker_repo，发现器初始化时自动同步）
    let mut control_plane = ControlPlane::new(config.clone(), registry.clone(), event_bus.clone())
        .with_tracker_repo(tracker_repo.clone());

    // 4. 初始化默认发现器（TrackerDiscoverer 会自动注入 tracker_repo）
    control_plane.init_default_discoverers();
    info!("[main] 已注册 {} 个发现器", control_plane.registry().len());

    // 6.5 创建 NAT 管理器，UPnP 端口映射异步后台初始化（不阻塞 HTTP 服务启动）
    let nat_config = PeerDiscoveryCenter::nat::NatConfig {
        enabled: config.nat.enabled,
        lease_duration: config.nat.lease_duration,
        ..Default::default()
    };
    let nat = Arc::new(NatManager::new(nat_config));
    let udp_port = config.super_tracker.udp_port.unwrap_or(config.server.port);
    let relay_port = config.super_tracker.relay_port;
    let utp_port = config.crawler.utp_port;
    let tcp_pex_port = config.crawler.tcp_pex_port;
    let crawler_port = if config.crawler.enabled {
        config.crawler.listen_port
    } else {
        0
    };
    // UPnP 初始化放到后台异步执行，避免阻塞 HTTP 服务启动
    if config.nat.enabled {
        let nat_clone = nat.clone();
        let http_port = config.server.port;
        tokio::spawn(async move {
            match nat_clone
                .init(
                    http_port,
                    udp_port,
                    crawler_port,
                    relay_port,
                    utp_port,
                    tcp_pex_port,
                    if config.federation.enabled {
                        config.federation.listen_port
                    } else {
                        0
                    },
                )
                .await
            {
                Err(e) => {
                    warn!("[main] NAT/UPnP 初始化失败: {}", e);
                    warn!("[main] 如果处于 NAT 网络后，请手动配置端口转发或启用路由器 UPnP");
                }
                Ok(_) => {
                    let status = nat_clone.status();
                    if let Some(ip) = &status.external_ip {
                        info!("[main] 公网访问地址: http://{}:{}", ip, http_port);
                    }
                    info!("[main] UPnP 端口映射初始化完成");
                }
            }
        });
    }

    // 6.8 创建联邦网络服务（如果启用）
    let federation_service: Option<Arc<FederationService>> = if config.federation.enabled {
        // 联邦数据目录从配置存储路径的父目录获取（零配置：不再硬编码 target/data）
        let data_dir = std::path::Path::new(&config.storage.path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("./data"));
        match FederationService::new(
            config.federation.clone(),
            nat.clone(),
            node_repo.clone(),
            data_dir,
            control_plane.dht_discoverer(),
            Some(event_bus.clone()),
            Some(peer_repo.clone()),
            Some(infohash_repo.clone()),
            Some(tracker_repo.clone()),
        ) {
            Ok(svc) => {
                let svc = Arc::new(svc);
                info!("[main] 联邦网络服务已创建");
                Some(svc)
            }
            Err(e) => {
                warn!("[main] 联邦网络服务创建失败: {}", e);
                None
            }
        }
    } else {
        None
    };

    // P1-2：把本地节点身份注入 oplog 的 origin 列（用于诊断与将来的冲突仲裁）
    if let Some(ref f) = federation_service {
        PeerDiscoveryCenter::storage::oplog::set_local_origin(f.identity.node_id.0.to_vec());
    }

    // 5. 创建超级 Tracker 状态（注入 peer_repo + 联邦服务）
    // 注意：创建时机推迟到 federation_service 之后，以便注入联邦引用
    let mut super_tracker_builder =
        SuperTrackerState::new(config.super_tracker.clone()).with_peer_repo(peer_repo.clone());
    if let Some(ref fed) = federation_service {
        super_tracker_builder = super_tracker_builder.with_federation(fed.clone());
    }
    let super_tracker = Arc::new(super_tracker_builder);

    // 6.8.5 自动防火墙规则配置（零配置协同：确保局域网发现和联邦连接不被拦截）
    // 失败时只记录 warning，不影响主流程启动。
    if config.auto_firewall_rule {
        let node_id_bytes = federation_service
            .as_ref()
            .map(|f| f.identity.node_id.0)
            .unwrap_or([0u8; 20]);
        let fw_manager = FirewallManager::new(&node_id_bytes, true);
        let fw_ports: Vec<(u16, &str, &str)> = vec![
            (config.server.api_port, "TCP", "api"),
            (
                config.super_tracker.udp_port.unwrap_or(config.server.port),
                "UDP",
                "super-tracker-udp",
            ),
            (config.super_tracker.relay_port, "TCP", "relay"),
            (config.discoverers.dht_listen_port, "UDP", "dht"),
            (config.crawler.listen_port, "UDP", "crawler"),
            (config.crawler.utp_port, "UDP", "utp"),
            (config.crawler.tcp_pex_port, "TCP", "tcp-pex"),
            (config.federation.listen_port, "TCP", "federation"),
            (config.federation.listen_port, "UDP", "federation-udp"),
            (
                config.federation.federation_lpd_multicast_port,
                "UDP",
                "lpd",
            ),
        ];
        match fw_manager.add_rules(&fw_ports) {
            Ok(result) => {
                info!(
                    "[main] 防火墙规则配置完成：新增{}条，跳过{}条，失败{}条",
                    result.added.len(),
                    result.skipped.len(),
                    result.failed.len()
                );
                for (name, err) in &result.failed {
                    warn!("[main] 防火墙规则添加失败 {}: {}", name, err);
                }
            }
            Err(e) => {
                warn!("[main] 防火墙规则配置异常（不影响主流程）: {}", e);
            }
        }
    }

    // 6.8.1 注入联邦引用到各 Repo：本地写入后统一更新 Merkle + 提交 Gossip
    // （FederationService 已创建，Merkl/Gossip 句柄此时可用；联邦未启用时跳过）
    if let Some(ref fed) = federation_service {
        let sm = &fed.sync_manager;
        let gossip = fed.gossip_engine.clone();
        if let Some(merkle) = sm.peer_merkle() {
            peer_repo.set_federation_refs(merkle, gossip.clone());
        }
        if let Some(merkle) = sm.infohash_merkle() {
            infohash_repo.set_federation_refs(merkle, gossip.clone());
        }
        if let Some(merkle) = sm.tracker_merkle() {
            tracker_repo.set_federation_refs(merkle, gossip.clone());
        }
        node_repo.set_federation_refs(sm.node_merkle(), gossip);
        info!("[main] 联邦引用已注入各 Repo（本地写入 -> Merkle + Gossip）");
    }

    // 6.9 提取全量同步暂停门（联邦启用时），供非核心模块在全量同步期间暂停主动工作
    let full_sync_gate: Option<Arc<AtomicBool>> =
        federation_service.as_ref().map(|s| s.full_sync_gate());

    // 6.95 创建自适应控制器（ICC 预测式自适应，共享给 TaskScheduler 和 CrawlerEngine）
    // 注意：倍率决策由 CrawlerEngine 调用 next_rate_multiplier() 完成，TaskScheduler 仅持有引用供监控/扩展使用
    let adaptive_controller = Arc::new(AdaptiveController::new(config.adaptive.clone()));

    // 7. 创建爬虫引擎（如果启用），在 AppState 之前创建以便共享状态
    let (crawler_state, crawler_routing_table, crawler_ref) = if config.crawler.enabled {
        let crawler = CrawlerEngine::new(config.crawler.clone(), event_bus.clone())
            .with_sockets(crawler_sockets)
            .with_peer_repo(peer_repo.clone())
            .with_storage(storage.clone())
            .with_infohash_repo(infohash_repo.clone())
            .with_node_repo(node_repo.clone())
            .with_pause_gate(full_sync_gate.clone())
            .with_adaptive_controller(adaptive_controller.clone());
        let state_arc = crawler.state_arc();
        let routing_table = crawler.routing_table();
        let crawler = Arc::new(crawler);

        // 启动爬虫
        let crawler_clone = crawler.clone();
        crawler_handle.spawn(async move {
            if let Err(e) = crawler_clone.start().await {
                warn!("[main] 爬虫引擎启动失败: {}", e);
            }
        });
        // 启动预热（不阻塞，预热完成前主动爬行会跳过）
        let crawler_warmup = crawler.clone();
        crawler_handle.spawn(async move {
            crawler_warmup.warmup().await;
        });
        info!("[main] 爬虫引擎已启动（主动模式）");
        (Some(state_arc), Some(routing_table), Some(crawler))
    } else {
        info!("[main] 爬虫引擎未启用（config.crawler.enabled = false）");
        (None, None, None)
    };

    // 7.5 创建 DHT 探测器（如果有爬虫路由表）
    // dht_probe_ref 会传入 AppState 保活，探测任务不会退出
    // 统一探测来源：DhtProbe 定期从 PeerRepo 拉取未探测的 peer，不再需要各来源手动提交
    let probe_sender;
    let dht_probe_ref: Option<Arc<PeerDiscoveryCenter::dht::DhtProbe>>;
    if let Some(ref rt) = crawler_routing_table {
        let probe = Arc::new(PeerDiscoveryCenter::dht::DhtProbe::new(
            rt.clone(),
            Some(node_repo.clone()),
            Some(peer_repo.clone()),
            full_sync_gate.clone(),
        ));
        probe_sender = Some(probe.sender());
        dht_probe_ref = Some(probe);
        info!("[main] DHT 探测器已启动（统一从 PeerRepo 拉取 peer → ping → 路由表+NodeRepo）");
    } else {
        probe_sender = None;
        dht_probe_ref = None;
    };

    // 7.52 创建 TrackerPeerFetcher（主动向 tracker 拉 peer → 存入 PeerRepo → DhtProbe 统一探测）
    let fetcher_ref: Option<Arc<PeerDiscoveryCenter::services::TrackerPeerFetcher>> = {
        let tracker_discoverer = Arc::new(
            PeerDiscoveryCenter::discoverers::tracker::TrackerDiscoverer::with_default_config()
                .with_tracker_repo(tracker_repo.clone()),
        );
        let fetcher =
            PeerDiscoveryCenter::services::TrackerPeerFetcher::new(tracker_discoverer.clone())
                .with_peer_repo(peer_repo.clone())
                .with_infohash_repo(infohash_repo.clone());
        let fetcher = Arc::new(fetcher);
        // tracker_fetcher 已迁移到 TaskScheduler（tracker_fetcher）
        info!("[main] TrackerPeerFetcher 已创建（主动拉取 peer → 存入 PeerRepo）");

        // 远程 Tracker 列表自动拉取（已迁移到 TaskScheduler 的 remote_tracker_refresh 任务）
        let _remote_discoverer_clone = tracker_discoverer.clone();

        Some(fetcher)
    };

    // 7.53 创建 SubscriptionService（外部订阅源导入 → InfohashRepo）
    let subscription_config = PeerDiscoveryCenter::services::SubscriptionConfig::default();
    let subscription_service =
        PeerDiscoveryCenter::services::SubscriptionService::new(subscription_config)
            .with_infohash_repo(infohash_repo.clone());
    let subscription_service = Arc::new(subscription_service);
    tokio::spawn(async move {
        // subscription_service 已迁移到 TaskScheduler（subscription_fetch）
    });
    info!("[main] SubscriptionService 已启动（外部订阅源导入 → InfohashRepo）");

    // 7.54 创建 KeywordSearchService（DHT 关键词搜索 BEP 44 → InfohashRepo）
    let keyword_config = PeerDiscoveryCenter::services::KeywordSearchConfig::default();
    // 从 NodeRepo 获取前 8 个高评分节点作为查询入口
    let keyword_nodes = node_repo
        .top_nodes_sync(8)
        .into_iter()
        .map(|n| n.addr)
        .collect();
    let keyword_service = PeerDiscoveryCenter::services::KeywordSearchService::new(keyword_config)
        .with_infohash_repo(infohash_repo.clone())
        .with_node_addrs(keyword_nodes);
    let keyword_service = Arc::new(keyword_service);
    // keyword_search 已迁移到 TaskScheduler（keyword_search）
    info!("[main] KeywordSearchService 已创建（DHT 关键词搜索 BEP 44 → InfohashRepo）");

    // 7.5.1 创建限流器（P2优化：QPS监控+单IP限流+异常封禁）
    let rate_limiter =
        Arc::new(PeerDiscoveryCenter::data_plane::rate_limiter::RateLimiter::new(100.0, 200.0));
    info!("[main] 限流器已创建（单IP 100 QPS，突发 200）");

    // 7.5 初始化 PEX/uTP 服务（默认启用）
    // 使用持久化的节点身份（work_dir/data/node_id），重启后保持一致
    let node_id = pex_node_id;

    // PEX 接收器（核心，被其他服务引用）
    let pex_receiver = Arc::new(
        PeerDiscoveryCenter::crawler::pex_receiver::PexReceiver::new()
            .with_peer_repo(peer_repo.clone()),
    );

    // uTP 服务端（UDP）
    let utp_server = match create_udp_socket(([0, 0, 0, 0], utp_port).into()).await {
        Ok(socket) => {
            let server =
                PeerDiscoveryCenter::crawler::utp_server::UtpServer::new(Arc::new(socket), node_id)
                    .with_peer_repo(peer_repo.clone())
                    .with_pex_receiver(pex_receiver.clone());
            let server = Arc::new(server);
            let s = server.clone();
            crawler_handle.spawn(async move {
                s.run().await;
            });
            info!("[main] uTP 服务端已启动（UDP {}）", utp_port);
            Some(server)
        }
        Err(e) => {
            warn!("[main] uTP 服务端启动失败（UDP {}）: {}", utp_port, e);
            None
        }
    };

    // TCP-PEX 服务端（TCP）
    let tcp_pex_server = {
        let listen_addr = format!("0.0.0.0:{}", tcp_pex_port).parse().unwrap();
        let server =
            PeerDiscoveryCenter::crawler::tcp_pex_server::TcpPexServer::new(listen_addr, node_id)
                .with_peer_repo(peer_repo.clone())
                .with_pex_receiver(pex_receiver.clone());
        let server = Arc::new(server);
        let s = server.clone();
        crawler_handle.spawn(async move {
            if let Err(e) = s.run().await {
                warn!("[main] TCP-PEX 服务端运行错误: {}", e);
            }
        });
        info!("[main] TCP-PEX 服务端已启动（TCP {}）", tcp_pex_port);
        Some(server)
    };

    // 主动 PEX 请求器
    let active_pex = {
        let requester = PeerDiscoveryCenter::crawler::active_pex::ActivePexRequester::new(
            peer_repo.clone(),
            node_id,
        )
        .with_pex_receiver(pex_receiver.clone())
        .with_interval(std::time::Duration::from_secs(get_interval_secs(
            &config.task_scheduler.intervals,
            "active_pex",
            30,
        )))
        .with_batch_size(20)
        .with_pause_gate(full_sync_gate.clone());
        let requester = Arc::new(requester);
        // active_pex 已迁移到 TaskScheduler（active_pex）
        info!("[main] 主动 PEX 请求器已创建");
        Some(requester)
    };

    // 6.9 启动联邦网络服务（如果启用）
    if let Some(ref fed_svc) = federation_service {
        let fed_clone = fed_svc.clone();
        federation_handle.spawn(async move {
            if let Err(e) = fed_clone.start().await {
                warn!("[main] 联邦网络服务启动失败: {}", e);
            }
        });
        info!("[main] 联邦网络服务启动中...");
    }

    // 7.6 创建 AppState
    // 7.6a 创建中继服务器（UDP+TCP，监听 relay_port，为打洞失败的节点提供流量转发）
    let relay_server: Option<Arc<RelayServer>> = {
        let addr = format!("0.0.0.0:{}", relay_port).parse().unwrap();
        let server = Arc::new(RelayServer::new(addr));
        let server_clone = server.clone();
        crawler_handle.spawn(async move {
            if let Err(e) = server_clone.start().await {
                warn!("[main] 中继服务器启动失败: {}", e);
            }
        });
        info!("[main] 中继服务器已启动: {} (UDP+TCP)", addr);
        Some(server)
    };
    // 保留引用用于 TaskScheduler 注册（relay_server 会被 move 到 AppState）
    let relay_server_ref = relay_server.clone();

    // 创建 UDP Tracker 服务端实例（用于 TaskScheduler 注册过期清理）
    let udp_tracker_instance = if config.super_tracker.enabled {
        let udp_port = config.super_tracker.udp_port.unwrap_or(config.server.port);
        if let Ok(listen_addr) =
            format!("{}:{}", config.server.listen, udp_port).parse::<std::net::SocketAddr>()
        {
            let mut server = UdpTrackerServer::new(
                listen_addr,
                super_tracker.clone(),
                peer_repo.clone(),
                config.super_tracker.clone(),
                rate_limiter.clone(),
            );
            server = server.with_infohash_repo(infohash_repo.clone());
            Some(Arc::new(server))
        } else {
            None
        }
    } else {
        None
    };

    // 统计快照（后台任务定期更新，API 只读）
    let stats_snapshot = Arc::new(stats_snapshot::StatsSnapshot::new());

    let app_state = AppState {
        control_plane: control_plane.clone(),
        super_tracker: super_tracker.clone(),
        peer_repo: peer_repo.clone(),
        event_bus: event_bus.clone(),
        config: Arc::new(RwLock::new(config.clone())),
        nat: nat.clone(),
        crawler_state,
        crawler_routing_table,
        probe_sender,
        storage: storage.clone(),
        node_repo: Some(node_repo.clone()),
        infohash_repo: Some(infohash_repo.clone()),
        tracker_repo: Some(tracker_repo.clone()),
        fetcher: fetcher_ref.clone(),
        dht_probe: dht_probe_ref,
        rate_limiter,
        hole_punch_signaling: Arc::new(
            PeerDiscoveryCenter::data_plane::hole_punch_signaling::HolePunchSignaling::new(),
        ),
        utp_server,
        pex_receiver: Some(pex_receiver),
        tcp_pex_server,
        active_pex,
        federation: federation_service.clone(),
        relay_server,
        udp_tracker: udp_tracker_instance,
        stats_snapshot: stats_snapshot.clone(),
    };
    // 保留引用用于 TaskScheduler 注册（已被 move 到 AppState）
    let dht_probe_clone = app_state.dht_probe.clone();
    let active_pex_clone = app_state.active_pex.clone();

    // 8. 健康检查任务（注册到 TaskScheduler）
    //    三个周期（主检查/统计输出/缓存清理）在下方 register 处以
    //    config.health_check.* 作为默认值下发，可被 task_scheduler.intervals 覆盖。
    let health_check = Arc::new(
        HealthCheckTask::new(
            registry.clone(),
            peer_repo.clone(),
            Some(node_repo.clone()),
            Some(tracker_repo.clone()),
            Some(infohash_repo.clone()),
            Some(storage.clone()),
        )
        .with_pause_gate(full_sync_gate.clone()),
    );

    // 8.5 创建统一 TaskScheduler（所有后台任务纳管，按分类分级并发 + 六 runtime 隔离）
    use PeerDiscoveryCenter::intelligence::task_scheduler::{RuntimeHandles, SchedulerKnobs};
    let task_scheduler = Arc::new(
        TaskScheduler::new()
            .with_category_concurrency(CategoryConcurrency {
                crawl: config.task_scheduler.crawl_concurrency,
                persistence: config.task_scheduler.persistence_concurrency,
                monitor: config.task_scheduler.monitor_concurrency,
                network: config.task_scheduler.network_concurrency,
                // v9 fix: 与 CategoryConcurrency::default() 对齐（此前字面量 4 覆盖 Default 8，
                // 导致 C3「联邦并发 4→8」实际不生效）
                federation: 8,
                tracker: 2,
            })
            .with_runtime_handles(RuntimeHandles {
                crawler: crawler_handle.clone(),
                federation: federation_handle.clone(),
                tracker: tracker_handle.clone(),
                api: api_handle.clone(),
                scheduler: scheduler_handle.clone(),
                persistence: persistence_handle.clone(),
            })
            .with_adaptive_controller(adaptive_controller.clone())
            // 把 config.task_scheduler 的准入/抖动/预测/自适应旋钮真正注入调度器（乙类接线）
            .with_knobs(SchedulerKnobs::from_config(&config.task_scheduler)),
    );

    // 任务间隔覆盖表：未配置的任务使用代码内默认值（与改造前行为一致）。
    // key=任务名（或 <任务名>_initial_delay / <任务名>_jitter）。
    let intervals = &config.task_scheduler.intervals;

    // 8.5.1 资源监控任务（每5秒，Critical，non_deferrable，读取真实系统资源）
    {
        let rm = task_scheduler.resource_monitor();
        task_scheduler.register(
            TaskMetadata::new(
                "resource_monitor",
                "系统资源监控",
                std::time::Duration::from_secs(get_interval_secs(intervals, "resource_monitor", 5)),
            )
            .with_category(TaskCategory::Monitor)
            .with_priority(TaskPriority::Critical)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .non_deferrable(),
            move || {
                let rm = rm.clone();
                async move {
                    rm.refresh();
                    Ok(())
                }
            },
        );
    }

    // 8.5.2 定期增量持久化任务（每10秒，Background）
    {
        let nr = node_repo.clone();
        let tr = tracker_repo.clone();
        let ir = infohash_repo.clone();
        let pr = peer_repo.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "periodic_persistence",
                "定期增量持久化",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "periodic_persistence",
                    10,
                )),
            )
            .with_category(TaskCategory::Persistence)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Medium,
                memory: ResourceLevel::Low,
                io: ResourceLevel::High,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "periodic_persistence_initial_delay",
                30,
            )))
            .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "periodic_persistence_jitter",
                10,
            ))),
            move || {
                let nr = nr.clone();
                let tr = tr.clone();
                let ir = ir.clone();
                let pr = pr.clone();
                async move {
                    let mut saved = 0u64;
                    match nr.save_dirty().await {
                        Ok(_) => saved += 1,
                        Err(e) => warn!("[persistence] NodeRepo 保存失败: {}", e),
                    }
                    match tr.save_all().await {
                        Ok(_) => saved += 1,
                        Err(e) => warn!("[persistence] TrackerRepo 保存失败: {}", e),
                    }
                    match ir.save_all().await {
                        Ok(_) => saved += 1,
                        Err(e) => warn!("[persistence] InfohashRepo 保存失败: {}", e),
                    }
                    match pr.save_all().await {
                        Ok(_) => saved += 1,
                        Err(e) => warn!("[persistence] PeerRepo 保存失败: {}", e),
                    }
                    match pr.flush_history().await {
                        Ok(n) if n > 0 => {
                            debug!("[persistence] PeerRepo history flushed: {} records", n)
                        }
                        Err(e) => warn!("[persistence] PeerRepo history flush 失败: {}", e),
                        _ => {}
                    }
                    // WAL checkpoint 已移至独立高频任务（wal_checkpoint_steady，每100ms），
                    // 此处不再执行，避免与持久化操作叠加形成 IO 尖峰
                    debug!("[persistence] 增量持久化完成（{} 项）", saved);
                    Ok(())
                }
            },
        );
    }

    // 8.5.2a 分片列回填：已由「写入即填」取代（P1-7）。
    // 四个 repo 的全部 INSERT/UPSERT 路径（db.rs 的 save_dht_node(s)/save_peer(s)/save_infohash(es)/
    // save_tracker(s) 及其 _in_tx 变体）现在都显式写入 l2_shard，且这些表的 key 均为主键、不会被
    // UPDATE 改写，故分片索引不会再漂移。historically 这里每 300s 全表扫 `l2_shard = 0` 回填，
    // 在亿级规模下会形成周期性全表扫描积压（R5），故取消。旧库的历史行仍由启动时的一次性
    // `storage.backfill_shards()`（见上方启动流程）修正。
    // （保留 interval 配置键 "shard_backfill" 不再读取，向后兼容旧 config 文件。）

    // 8.5.2b WriteQueue 定时 flush（每5秒，Persistence）
    {
        let wq = write_queue.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "write_queue_flush",
                "WriteQueue定时刷盘",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "write_queue_flush",
                    5,
                )),
            )
            .with_category(TaskCategory::Persistence)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::High,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "write_queue_flush_initial_delay",
                10,
            ))),
            move || {
                let wq = wq.clone();
                async move {
                    wq.flush();
                    Ok(())
                }
            },
        );
    }

    // 8.5.2b2 WAL checkpoint 高频任务（每100ms，Persistence，PASSIVE模式不阻塞写入）
    {
        let storage_clone = storage.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "wal_checkpoint_steady",
                "WAL checkpoint 高频稳态",
                std::time::Duration::from_millis(get_interval_secs(
                    intervals,
                    "wal_checkpoint_interval_ms",
                    100,
                )),
            )
            .with_category(TaskCategory::Persistence)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Medium,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "wal_checkpoint_steady_initial_delay",
                5,
            ))),
            move || {
                let storage = storage_clone.clone();
                async move {
                    // PASSIVE checkpoint 不阻塞写入，高频执行保持 WAL 小巧
                    match storage.checkpoint() {
                        Ok(_) => info!("[persistence] WAL checkpoint(PASSIVE) 执行成功"),
                        Err(e) => warn!("[persistence] WAL checkpoint 失败: {}", e),
                    }
                    Ok(())
                }
            },
        );
    }

    // 8.5.2b3 WAL TRUNCATE 压缩（每小时一次，Persistence，会短暂阻塞写入但压缩 WAL）
    {
        let storage_clone = storage.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "wal_checkpoint_hourly_truncate",
                "WAL TRUNCATE 每小时压缩",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "wal_checkpoint_truncate_interval_secs",
                    3600,
                )),
            )
            .with_category(TaskCategory::Persistence)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::High,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "wal_checkpoint_truncate_initial_delay",
                60,
            ))),
            move || {
                let storage = storage_clone.clone();
                async move {
                    // TRUNCATE 模式会短暂阻塞写入，但会将 WAL 文件压缩到最小
                    // 每小时执行一次，平衡 IO 平滑与磁盘/内存占用
                    match storage.checkpoint_truncate() {
                        Ok(_) => info!("[persistence] WAL checkpoint(TRUNCATE) 每小时压缩完成"),
                        Err(e) => warn!("[persistence] WAL TRUNCATE 失败: {}", e),
                    }
                    Ok(())
                }
            },
        );
    }

    // 8.5.2c IOScheduler 背压轮询（仅启用时注册）
    if let Some(ref sched) = io_scheduler {
        let sched_clone = sched.clone();
        let rm = task_scheduler.resource_monitor();
        let bp_interval = config.io_scheduler.backpressure_poll_interval_secs;
        task_scheduler.register(
            TaskMetadata::new(
                "io_backpressure_poll",
                "IOScheduler背压采样",
                std::time::Duration::from_secs(bp_interval),
            )
            .with_category(TaskCategory::Monitor)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(bp_interval)),
            move || {
                let s = sched_clone.clone();
                let r = rm.clone();
                async move {
                    let level = s.backpressure_level();
                    r.set_io_load(level as f64);
                    Ok(())
                }
            },
        );
    }

    // 8.5.3 健康检查任务（3个：主检查/统计输出/缓存清理）
    {
        let hc = health_check.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "health_check_main",
                "健康检查主循环",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "health_check_main",
                    config.health_check.interval_secs,
                )),
            )
            .with_category(TaskCategory::Monitor)
            .with_priority(TaskPriority::Important)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Medium,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "health_check_main_jitter",
                10,
            ))),
            move || {
                let hc = hc.clone();
                async move {
                    hc.check_discoverers_health().await;
                    Ok(())
                }
            },
        );
    }
    {
        let hc = health_check.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "health_check_stats",
                "健康统计输出",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "health_check_stats",
                    config.health_check.stats_output_interval_secs,
                )),
            )
            .with_category(TaskCategory::Monitor)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "health_check_stats_initial_delay",
                75,
            )))
            .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "health_check_stats_jitter",
                10,
            ))),
            move || {
                let hc = hc.clone();
                async move {
                    hc.output_stats().await;
                    Ok(())
                }
            },
        );
    }
    {
        let hc = health_check.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "health_cache_cleanup",
                "过期Peer缓存清理",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "health_cache_cleanup",
                    config.health_check.cache_cleanup_interval_secs,
                )),
            )
            .with_category(TaskCategory::Monitor)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Medium,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "health_cache_cleanup_initial_delay",
                420,
            )))
            .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "health_cache_cleanup_jitter",
                15,
            )))
            .with_dependencies(vec!["score_full".to_string()]),
            move || {
                let hc = hc.clone();
                async move {
                    hc.cleanup_expired_peers().await;
                    Ok(())
                }
            },
        );
    }

    // 8.6 ScoreMaintainer: multi-source infohash scoring
    let maintainer = {
        use PeerDiscoveryCenter::intelligence::{
            InfohashScorerImpl, NodeScorerImpl, PeerScorerImpl, ScoreMaintainer, TrackerScorerImpl,
        };

        let dht_activity = Arc::new(DhtActivityTracker::new());
        let peer_history = Arc::new(PeerHistoryManager::new());
        let scrape_service = Arc::new(ScrapeService::new().with_tracker_repo(tracker_repo.clone()
            as Arc<dyn PeerDiscoveryCenter::storage::repo_traits::TrackerRepository>));
        let metadata_service = Arc::new(MetadataService::new());
        let availability_calculator = Arc::new(AvailabilityCalculator::new());

        info!("[main] Infohash multi-source scoring services created");

        Arc::new(
            ScoreMaintainer::new(
                Arc::new(NodeScorerImpl::new()),
                Arc::new(PeerScorerImpl::new()),
                Arc::new(TrackerScorerImpl::new()),
                Arc::new(InfohashScorerImpl::new()),
            )
            .with_node_repo(node_repo.clone()
                as Arc<dyn PeerDiscoveryCenter::storage::repo_traits::NodeRepository>)
            .with_peer_repo(peer_repo.clone()
                as Arc<dyn PeerDiscoveryCenter::storage::repo_traits::PeerRepository>)
            .with_tracker_repo(tracker_repo.clone()
                as Arc<dyn PeerDiscoveryCenter::storage::repo_traits::TrackerRepository>)
            .with_infohash_repo(infohash_repo.clone()
                as Arc<dyn PeerDiscoveryCenter::storage::repo_traits::InfohashRepository>)
            .with_dht_activity(dht_activity.clone())
            .with_peer_history(peer_history.clone())
            .with_scrape_service(scrape_service.clone())
            .with_metadata_service(metadata_service.clone())
            .with_availability_calculator(availability_calculator.clone())
            .with_super_tracker(super_tracker.clone()),
        )
    };

    // 8.6.1 评分任务注册（3个：增量/全量/快照）
    {
        let m = maintainer.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "score_incremental",
                "增量评分重算",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "score_incremental",
                    60,
                )),
            )
            .with_category(TaskCategory::Monitor)
            .with_priority(TaskPriority::Important)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Medium,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "score_incremental_jitter",
                10,
            ))),
            move || {
                let m = m.clone();
                async move {
                    m.rescore_incremental().await;
                    Ok(())
                }
            },
        );
    }
    {
        let m = maintainer.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "score_full",
                "全量评分重算",
                std::time::Duration::from_secs(get_interval_secs(intervals, "score_full", 600)),
            )
            .with_category(TaskCategory::Monitor)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::High,
                memory: ResourceLevel::Medium,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: true,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "score_full_initial_delay",
                360,
            )))
            .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "score_full_jitter",
                15,
            )))
            .with_dependencies(vec!["periodic_persistence".to_string()]),
            move || {
                let m = m.clone();
                async move {
                    m.rescore_all().await;
                    Ok(())
                }
            },
        );
    }
    {
        let m = maintainer.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "score_snapshot",
                "Peer快照",
                std::time::Duration::from_secs(get_interval_secs(intervals, "score_snapshot", 120)),
            )
            .with_category(TaskCategory::Monitor)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Medium,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "score_snapshot_initial_delay",
                60,
            )))
            .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "score_snapshot_jitter",
                10,
            ))),
            move || {
                let m = m.clone();
                async move {
                    m.snapshot_peers().await;
                    Ok(())
                }
            },
        );
    }
    {
        let m = maintainer.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "cache_cleanup",
                "评分缓存清理",
                std::time::Duration::from_secs(get_interval_secs(intervals, "cache_cleanup", 600)),
            )
            .with_category(TaskCategory::Monitor)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Medium,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "cache_cleanup_initial_delay",
                420,
            )))
            .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "cache_cleanup_jitter",
                15,
            )))
            .with_dependencies(vec!["score_full".to_string()]),
            move || {
                let m = m.clone();
                async move {
                    m.cleanup_caches().await;
                    Ok(())
                }
            },
        );
    }

    // 8.7 TierManager 冷热分层任务
    {
        use PeerDiscoveryCenter::intelligence::TierManager;
        let tier_mgr = Arc::new(
            TierManager::new(
                PeerDiscoveryCenter::intelligence::tier_manager::TierConfig {
                    hot_threshold_secs: config.tier.hot_threshold_secs,
                    warm_threshold_secs: config.tier.warm_threshold_secs,
                    max_hot_in_memory: config.tier.hot_max_count,
                    check_interval_secs: config.tier.evict_interval_secs,
                },
            )
            .with_storage(storage.clone())
            .with_peer_repo(peer_repo.clone()
                as Arc<dyn PeerDiscoveryCenter::storage::repo_traits::PeerRepository>)
            .with_node_repo(node_repo.clone()
                as Arc<dyn PeerDiscoveryCenter::storage::repo_traits::NodeRepository>),
        );
        let tm = tier_mgr.clone();
        let io_sched = io_scheduler.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "tier_check",
                "冷热分层检查",
                std::time::Duration::from_secs(get_interval_secs(intervals, "tier_check", 300)),
            )
            .with_category(TaskCategory::Monitor)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Medium,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "tier_check_initial_delay",
                225,
            )))
            .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "tier_check_jitter",
                10,
            ))),
            move || {
                let tm = tm.clone();
                let io_sched = io_sched.clone();
                async move {
                    // P3: 仅在 IO 空闲时执行冷驱逐（避免与前台写入竞争）
                    if let Some(ref s) = io_sched {
                        if !s.is_idle() {
                            tracing::debug!("[tier_check] IO 繁忙，跳过本轮冷驱逐");
                            return Ok(());
                        }
                    }
                    tm.check_all().await;
                    Ok(())
                }
            },
        );
    }
    // 8.7.1 冷热分层驱逐任务（每60秒执行 tier_check + evict_if_needed）
    {
        let nr = node_repo.clone();
        let pr = peer_repo.clone();
        let ir = infohash_repo.clone();
        let tr = tracker_repo.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "tier_evict",
                "冷热分层驱逐",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "tier_evict",
                    config.tier.evict_interval_secs,
                )),
            )
            .with_category(TaskCategory::Monitor)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            // [ALLOWED-HARDCODED: TaskScheduler 任务首次启动延迟，一次性启动装配参数，不影响运行时行为]
            .with_initial_delay(std::time::Duration::from_secs(60)),
            move || {
                let nr = nr.clone();
                let pr = pr.clone();
                let ir = ir.clone();
                let tr = tr.clone();
                async move {
                    nr.tier_evict();
                    pr.tier_evict();
                    ir.tier_evict();
                    tr.tier_evict();
                    Ok(())
                }
            },
        );
    }

    // 8.7.2 全局内存监控任务（每30秒采样，超阈值触发紧急驱逐）
    {
        let nr = node_repo.clone();
        let pr = peer_repo.clone();
        let ir = infohash_repo.clone();
        let tr = tracker_repo.clone();
        let st = storage.clone();
        let memory_limit_mb = config.tier.memory_limit_mb;
        let emergency_threshold = config.tier.emergency_threshold;
        task_scheduler.register(
            TaskMetadata::new(
                "memory_monitor",
                "全局内存监控",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "memory_monitor",
                    config.tier.memory_monitor_interval_secs,
                )),
            )
            .with_category(TaskCategory::Monitor)
            .with_priority(TaskPriority::Important)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
                        // [ALLOWED-HARDCODED: TaskScheduler 任务首次启动延迟，一次性启动装配参数，不影响运行时行为]
            .with_initial_delay(std::time::Duration::from_secs(30)),
            move || {
                let nr = nr.clone();
                let pr = pr.clone();
                let ir = ir.clone();
                let tr = tr.clone();
                let st = st.clone();
                async move {
                    use sysinfo::System;
                    // 使用 new_all 确保在 SYSTEM 账户下也能获取进程信息
                    let sys = System::new_all();
                    let pid = sysinfo::get_current_pid().unwrap();
                    if let Some(proc) = sys.process(pid) {
                        // sysinfo memory() 在 Windows 返回字节，转换为 MB
                        let memory_mb = proc.memory() / (1024 * 1024);
                        let threshold_mb = (memory_limit_mb as f64 * emergency_threshold) as u64;
                        tracing::info!(
                            "[memory_monitor] 内存检测: {}MB / 阈值 {}MB",
                            memory_mb,
                            threshold_mb
                        );
                        if memory_mb > threshold_mb {
                            tracing::warn!(
                                "[memory_monitor] 内存 {}MB 超过阈值 {}MB，触发渐进式驱逐",
                                memory_mb,
                                threshold_mb
                            );
                            // 渐进式驱逐：每次驱逐10%，循环直到内存达标或达到上限（最多5轮=50%）
                            // 数量小于10万的repo不驱散，避免小repo被误驱逐
                            let min_evict_threshold = 100_000usize;
                            let max_rounds = 5usize;
                            let mut total_evicted_1 = 0usize;
                            let mut total_evicted_2 = 0usize;
                            let mut _total_evicted_3 = 0usize;
                            let mut _total_evicted_4 = 0usize;
                            let (h1_start, w1_start, _) = nr.cache_stats();
                            let (h2_start, w2_start, _) = pr.cache_stats();

                            for round in 0..max_rounds {
                                let (h1, w1, _) = nr.cache_stats();
                                let (h2, w2, _) = pr.cache_stats();
                                let (h3, w3, _) = ir.cache_stats();
                                let (h4, w4, _) = tr.cache_stats();
                                let evict_1 = if h1 + w1 >= min_evict_threshold { (h1 + w1) / 10 } else { 0 };
                                let evict_2 = if h2 + w2 >= min_evict_threshold { (h2 + w2) / 10 } else { 0 };
                                let evict_3 = if h3 + w3 >= min_evict_threshold { (h3 + w3) / 10 } else { 0 };
                                let evict_4 = if h4 + w4 >= min_evict_threshold { (h4 + w4) / 10 } else { 0 };

                                if evict_1 + evict_2 + evict_3 + evict_4 == 0 {
                                    tracing::info!("[memory_monitor] 第{}轮：所有repo均低于驱逐阈值，停止", round + 1);
                                    break;
                                }

                                let nr_clone = nr.clone();
                                let pr_clone = pr.clone();
                                let ir_clone = ir.clone();
                                let tr_clone = tr.clone();
                                let st_clone = st.clone();
                                tokio::task::spawn_blocking(move || {
                                    if evict_1 > 0 { nr_clone.emergency_evict(evict_1); }
                                    if evict_2 > 0 { pr_clone.emergency_evict(evict_2); }
                                    if evict_3 > 0 { ir_clone.emergency_evict(evict_3); }
                                    if evict_4 > 0 { tr_clone.emergency_evict(evict_4); }
                                    st_clone.shrink_memory();
                                })
                                .await
                                .ok();

                                total_evicted_1 += evict_1;
                                total_evicted_2 += evict_2;
                                _total_evicted_3 += evict_3;
                                _total_evicted_4 += evict_4;

                                // 重新检测内存
                                let sys = System::new_all();
                                let pid = sysinfo::get_current_pid().unwrap();
                                let current_mb = if let Some(proc) = sys.process(pid) {
                                    proc.memory() / (1024 * 1024)
                                } else {
                                    memory_mb
                                };
                                tracing::info!(
                                    "[memory_monitor] 第{}轮驱逐完成: node驱逐{}，内存{}MB",
                                    round + 1, evict_1, current_mb
                                );

                                if current_mb <= threshold_mb {
                                    tracing::info!("[memory_monitor] 内存已降到阈值以下，停止驱逐");
                                    break;
                                }
                            }

                            let (h1a, w1a, _) = nr.cache_stats();
                            let (h2a, w2a, _) = pr.cache_stats();
                            tracing::warn!(
                                "[memory_monitor] 渐进驱逐完成: node {}/{}->{}/{} (共驱逐{}), peer {}/{}->{}/{} (共驱逐{})",
                                h1_start, w1_start, h1a, w1a, total_evicted_1,
                                h2_start, w2_start, h2a, w2a, total_evicted_2
                            );
                        }
                    } else {
                        tracing::warn!("[memory_monitor] 无法获取当前进程信息 (pid={:?})", pid);
                    }
                    Ok(())
                }
            },
        );
    }

    // 8.8 联邦任务注册（4个：心跳/节点同步/DHT发现/Merkle反熵）
    if let Some(ref fed) = federation_service {
        // fed_heartbeat
        let cm = fed.sessions.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "fed_heartbeat",
                "联邦心跳",
                std::time::Duration::from_secs(get_interval_secs(intervals, "fed_heartbeat", 30)),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Important)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_heartbeat_jitter",
                5,
            ))),
            move || {
                let cm = cm.clone();
                async move {
                    cm.heartbeat_tick().await;
                    Ok(())
                }
            },
        );

        // fed_node_sync
        let sm = fed.sync_manager.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "fed_node_sync",
                "联邦节点同步",
                std::time::Duration::from_secs(get_interval_secs(intervals, "fed_node_sync", 300)),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Medium,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_node_sync_initial_delay",
                150,
            )))
            .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_node_sync_jitter",
                10,
            ))),
            move || {
                let sm = sm.clone();
                async move {
                    sm.do_node_sync().await;
                    Ok(())
                }
            },
        );

        // fed_dht_discovery
        if let Some(ref dht_disc) = fed.dht_discovery {
            let dd = dht_disc.clone();
            task_scheduler.register(
                TaskMetadata::new(
                    "fed_dht_discovery",
                    "联邦DHT发现",
                    std::time::Duration::from_secs(get_interval_secs(
                        intervals,
                        "fed_dht_discovery",
                        300,
                    )),
                )
                .with_category(TaskCategory::Federation)
                .with_priority(TaskPriority::Normal)
                .with_resource(ResourceProfile {
                    cpu: ResourceLevel::Low,
                    memory: ResourceLevel::Low,
                    io: ResourceLevel::Low,
                    network: ResourceLevel::Medium,
                    is_full_task: false,
                })
                .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "fed_dht_discovery_jitter",
                    10,
                ))),
                move || {
                    let dd = dd.clone();
                    async move {
                        dd.discovery_tick().await;
                        Ok(())
                    }
                },
            );
        }

        // fed_merkle_anti_entropy
        // P2-5：tick 周期取各 repo 周期的较小值（默认 NODE 30s / 其余 300s），
        // 具体 repo 是否本轮对账由 SyncManager::anti_entropy_due 按 repo 差异化控制。
        let g = fed.gossip_engine.clone();
        let s = fed.sync_manager.clone();
        let ae_tick_secs = config
            .federation
            .anti_entropy_node_interval_secs
            .min(config.federation.anti_entropy_other_interval_secs);
        task_scheduler.register(
            TaskMetadata::new(
                "fed_merkle_anti_entropy",
                "联邦Merkle反熵",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "fed_merkle_anti_entropy",
                    ae_tick_secs,
                )),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Medium,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_merkle_anti_entropy_initial_delay",
                45,
            )))
            .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_merkle_anti_entropy_jitter",
                10,
            ))),
            move || {
                let g = g.clone();
                let s = s.clone();
                async move {
                    g.anti_entropy_tick(s).await;
                    Ok(())
                }
            },
        );

        // fed_delta_sync: F1 修复 —— delta 通道的周期驱动
        // 默认 delta_sync_enabled=false 时为空转（no-op）；开启后按 delta_sync_interval_secs
        // 周期向每个已连接对端追问新 op，否则 delta 只在建连时拉一次、之后完全停摆。
        let sm_delta = fed.sync_manager.clone();
        let delta_tick_secs = config.federation.delta_sync_interval_secs.max(1);
        task_scheduler.register(
            TaskMetadata::new(
                "fed_delta_sync",
                "联邦delta增量拉取",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "fed_delta_sync",
                    delta_tick_secs,
                )),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Medium,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_delta_sync_initial_delay",
                20,
            )))
            .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_delta_sync_jitter",
                5,
            ))),
            move || {
                let sm = sm_delta.clone();
                async move {
                    sm.delta_sync_tick().await;
                    Ok(())
                }
            },
        );

        // fed_range_reconcile: P1-4 Range-based 反熵抽样对账
        // 默认 range_reconcile_enabled=false 时为空转（no-op）；开启后先以只读诊断模式运行。
        let sm_range = fed.sync_manager.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "fed_range_reconcile",
                "联邦Range反熵对账",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "fed_range_reconcile",
                    60,
                )),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Medium,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_range_reconcile_initial_delay",
                90,
            )))
            .with_jitter(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_range_reconcile_jitter",
                15,
            ))),
            move || {
                let sm = sm_range.clone();
                async move {
                    sm.range_reconcile_tick().await;
                    Ok(())
                }
            },
        );

        // fed_bootstrap_resume: P2-1 bootstrap 断点续传（默认 bootstrap_enabled=false 时空转）
        let sm_bootstrap = fed.sync_manager.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "fed_bootstrap_resume",
                "联邦bootstrap续传",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "fed_bootstrap_resume",
                    60,
                )),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Medium,
                network: ResourceLevel::Medium,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_bootstrap_resume_initial_delay",
                120,
            ))),
            move || {
                let sm = sm_bootstrap.clone();
                async move {
                    sm.bootstrap_resume_tick().await;
                    Ok(())
                }
            },
        );

        // fed_public_addr_sync: 公网地址同步（300s）
        let fed_clone = fed.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "fed_public_addr_sync",
                "联邦公网地址同步",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "fed_public_addr_sync",
                    300,
                )),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_public_addr_sync_initial_delay",
                60,
            ))),
            move || {
                let f = fed_clone.clone();
                async move {
                    f.sync_public_addr();
                    Ok(())
                }
            },
        );

        // fed_relay_channel_cleanup: 中继通道清理（30s）
        let rm = fed.relay_manager.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "fed_relay_channel_cleanup",
                "联邦中继通道清理",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "fed_relay_channel_cleanup",
                    30,
                )),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_relay_channel_cleanup_initial_delay",
                60,
            ))),
            move || {
                let r = rm.clone();
                async move {
                    r.cleanup_expired();
                    Ok(())
                }
            },
        );

        // fed_diff_sync_watcher: 差量同步超时监控（30s）
        let sm_watch = fed.sync_manager.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "fed_diff_sync_watcher",
                "联邦差量同步超时监控",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "fed_diff_sync_watcher",
                    30,
                )),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_diff_sync_watcher_initial_delay",
                60,
            ))),
            move || {
                let sm = sm_watch.clone();
                async move {
                    sm.check_diff_sync_timeouts();
                    Ok(())
                }
            },
        );

        // fed_pex_exchange: PEX 节点交换（30s）
        let disc_pex = fed.discovery.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "fed_pex_exchange",
                "联邦PEX节点交换",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "fed_pex_exchange",
                    30,
                )),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Medium,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_pex_exchange_initial_delay",
                30,
            ))),
            move || {
                let d = disc_pex.clone();
                async move {
                    d.pex_exchange_tick().await;
                    Ok(())
                }
            },
        );

        // fed_connection_maintain: 连接维护（30s）
        let disc_maint = fed.discovery.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "fed_connection_maintain",
                "联邦连接维护",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "fed_connection_maintain",
                    30,
                )),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Important)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Medium,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_connection_maintain_initial_delay",
                45,
            ))),
            move || {
                let d = disc_maint.clone();
                async move {
                    d.connection_maintainer_tick().await;
                    Ok(())
                }
            },
        );

        // fed_peer_cache_save: 节点缓存保存（300s）
        let disc_cache = fed.discovery.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "fed_peer_cache_save",
                "联邦节点缓存保存",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "fed_peer_cache_save",
                    300,
                )),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Medium,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_peer_cache_save_initial_delay",
                120,
            ))),
            move || {
                let d = disc_cache.clone();
                async move {
                    d.peer_cache_save_tick().await;
                    Ok(())
                }
            },
        );

        // fed_nat_refresh: NAT地址刷新（300s）
        let nat = fed.nat_integration.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "fed_nat_refresh",
                "联邦NAT地址刷新",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "fed_nat_refresh",
                    300,
                )),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Medium,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_nat_refresh_initial_delay",
                60,
            ))),
            move || {
                let n = nat.clone();
                async move {
                    n.refresh_tick().await;
                    Ok(())
                }
            },
        );

        // fed_tracker_sync: Tracker全量同步（3600s）
        if let Some(ts) = fed.sync_manager.tracker_sync() {
            let ts_clone = ts.clone();
            task_scheduler.register(
                TaskMetadata::new(
                    "fed_tracker_sync",
                    "联邦Tracker全量同步",
                    std::time::Duration::from_secs(get_interval_secs(
                        intervals,
                        "fed_tracker_sync",
                        3600,
                    )),
                )
                .with_category(TaskCategory::Federation)
                .with_priority(TaskPriority::Background)
                .with_resource(ResourceProfile {
                    cpu: ResourceLevel::Low,
                    memory: ResourceLevel::Medium,
                    io: ResourceLevel::Medium,
                    network: ResourceLevel::Low,
                    is_full_task: false,
                })
                .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "fed_tracker_sync_initial_delay",
                    300,
                ))),
                move || {
                    let t = ts_clone.clone();
                    async move {
                        t.do_full_sync();
                        Ok(())
                    }
                },
            );
        }

        // fed_gossip_flush: Gossip批量flush（50ms）
        let cm_flush = fed.dispatcher.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "fed_gossip_flush",
                "联邦Gossip批量flush",
                std::time::Duration::from_millis(get_interval_secs(
                    intervals,
                    "fed_gossip_flush",
                    50,
                )),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Important)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            }),
            move || {
                let cm = cm_flush.clone();
                async move {
                    cm.flush_all_gossip_buffers().await;
                    Ok(())
                }
            },
        );

        // fed_gossip_propagation: Gossip传播（100ms，方法内动态判断全量期处理量）
        let gossip_prop = fed.gossip_engine.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "fed_gossip_propagation",
                "联邦Gossip传播",
                std::time::Duration::from_millis(get_interval_secs(
                    intervals,
                    "fed_gossip_propagation",
                    100,
                )),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Important)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Medium,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Medium,
                is_full_task: false,
            }),
            move || {
                let g = gossip_prop.clone();
                async move {
                    g.gossip_propagation_tick().await;
                    Ok(())
                }
            },
        );

        // fed_merkle_flush: Merkle异步批量flush（1000ms）
        let sm_flush = fed.sync_manager.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "fed_merkle_flush",
                "联邦Merkle批量flush",
                std::time::Duration::from_millis(get_interval_secs(
                    intervals,
                    "fed_merkle_flush",
                    1000,
                )),
            )
            .with_category(TaskCategory::Federation)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Medium,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "fed_merkle_flush_initial_delay",
                10,
            ))),
            move || {
                let sm = sm_flush.clone();
                async move {
                    sm.merkle_flush_tick();
                    Ok(())
                }
            },
        );

        // 8.8.4 Merkle 重算任务（4 个 repo 独立，Persistence 分类并发=1 串行执行，避免 DB 竞争）
        // P1-5：常态维护已下沉到「增量任务」（按 dirty L2 精确重算，10s 一次）；
        // 全量冷重算降级为**低频兜底**（默认 3600s = 1h），不再每 300s 全表重算
        // （亿级规模下 300s 全表重算不可行，且与增量维护重复）。
        let cold_rebuild_interval = config.federation.merkle_cold_rebuild_interval_secs;
        let incremental_interval = config.federation.merkle_incremental_update_interval_secs;
        info!(
            "[main] 注册 Merkle 冷重算任务（4 repo，兜底间隔 {}s）+ 增量更新任务（{}s，按 L2 精确重算）",
            cold_rebuild_interval, incremental_interval
        );

        // === Merkle 增量更新任务（10s，取出 dirty 分片从 DB 重算） ===

        // Node 增量更新
        {
            let st = storage.clone();
            let sm = fed.sync_manager.clone();
            task_scheduler.register(
                TaskMetadata::new(
                    "merkle_incremental_node",
                    "Merkle增量更新-Node",
                    std::time::Duration::from_secs(incremental_interval),
                )
                .with_category(TaskCategory::Persistence)
                .with_priority(TaskPriority::Background)
                .with_resource(ResourceProfile {
                    cpu: ResourceLevel::Low,
                    memory: ResourceLevel::Low,
                    io: ResourceLevel::Medium,
                    network: ResourceLevel::Low,
                    is_full_task: false,
                })
                // [ALLOWED-HARDCODED: TaskScheduler 任务首次启动延迟，一次性启动装配参数，不影响运行时行为]
                .with_initial_delay(std::time::Duration::from_secs(15)),
                move || {
                    let st = st.clone();
                    let sm = sm.clone();
                    async move {
                        let merkle = sm.node_merkle();
                        let dirty = merkle.take_dirty_l2_shards();
                        if dirty.is_empty() {
                            return Ok(());
                        }
                        // P1-5：按真 L2 精确加载受影响的 L2（DB 分片列已存 L2），
                        // 用批量 recompute_l2_subset_from_db 一次锁内完成，替代旧的「L1 粒度 + 整 L1 重算」。
                        let l2s: Vec<u32> = dirty.into_iter().collect();
                        let shards: Vec<u16> = l2s.iter().map(|&l| l as u16).collect();
                        match st.load_node_keys_hashes_by_shards(&shards) {
                            Ok(keys_hashes) => {
                                let (by_l1, dirty_in_l1) =
                                    build_l2_recompute_input(&merkle, &l2s, keys_hashes);
                                merkle.recompute_l2_subset_from_db(&by_l1, &dirty_in_l1);
                            }
                            Err(e) => {
                                warn!("[merkle_incremental] Node 失败: {}", e);
                                for &l2 in &l2s {
                                    merkle.mark_dirty_l2(l2);
                                }
                            }
                        }
                        Ok(())
                    }
                },
            );
        }

        // Peer 增量更新
        {
            let st = storage.clone();
            let sm = fed.sync_manager.clone();
            task_scheduler.register(
                TaskMetadata::new(
                    "merkle_incremental_peer",
                    "Merkle增量更新-Peer",
                    std::time::Duration::from_secs(incremental_interval),
                )
                .with_category(TaskCategory::Persistence)
                .with_priority(TaskPriority::Background)
                .with_resource(ResourceProfile {
                    cpu: ResourceLevel::Low,
                    memory: ResourceLevel::Low,
                    io: ResourceLevel::Medium,
                    network: ResourceLevel::Low,
                    is_full_task: false,
                })
                // [ALLOWED-HARDCODED: TaskScheduler 任务首次启动延迟，一次性启动装配参数，不影响运行时行为]
                .with_initial_delay(std::time::Duration::from_secs(20)),
                move || {
                    let st = st.clone();
                    let sm = sm.clone();
                    async move {
                        if let Some(merkle) = sm.peer_merkle() {
                            let dirty = merkle.take_dirty_l2_shards();
                            if dirty.is_empty() {
                                return Ok(());
                            }
                            let l2s: Vec<u32> = dirty.into_iter().collect();
                            let shards: Vec<u16> = l2s.iter().map(|&l| l as u16).collect();
                            match st.load_peer_keys_hashes_by_shards(&shards) {
                                Ok(keys_hashes) => {
                                    let (by_l1, dirty_in_l1) =
                                        build_l2_recompute_input(&merkle, &l2s, keys_hashes);
                                    merkle.recompute_l2_subset_from_db(&by_l1, &dirty_in_l1);
                                }
                                Err(e) => {
                                    warn!("[merkle_incremental] Peer 失败: {}", e);
                                    for &l2 in &l2s {
                                        merkle.mark_dirty_l2(l2);
                                    }
                                }
                            }
                        }
                        Ok(())
                    }
                },
            );
        }

        // Infohash 增量更新
        {
            let st = storage.clone();
            let sm = fed.sync_manager.clone();
            task_scheduler.register(
                TaskMetadata::new(
                    "merkle_incremental_infohash",
                    "Merkle增量更新-Infohash",
                    std::time::Duration::from_secs(incremental_interval),
                )
                .with_category(TaskCategory::Persistence)
                .with_priority(TaskPriority::Background)
                .with_resource(ResourceProfile {
                    cpu: ResourceLevel::Low,
                    memory: ResourceLevel::Low,
                    io: ResourceLevel::Medium,
                    network: ResourceLevel::Low,
                    is_full_task: false,
                })
                // [ALLOWED-HARDCODED: TaskScheduler 任务首次启动延迟，一次性启动装配参数，不影响运行时行为]
                .with_initial_delay(std::time::Duration::from_secs(25)),
                move || {
                    let st = st.clone();
                    let sm = sm.clone();
                    async move {
                        if let Some(merkle) = sm.infohash_merkle() {
                            let dirty = merkle.take_dirty_l2_shards();
                            if dirty.is_empty() {
                                return Ok(());
                            }
                            let l2s: Vec<u32> = dirty.into_iter().collect();
                            let shards: Vec<u16> = l2s.iter().map(|&l| l as u16).collect();
                            match st.load_infohash_keys_hashes_by_shards(&shards) {
                                Ok(keys_hashes) => {
                                    let (by_l1, dirty_in_l1) =
                                        build_l2_recompute_input(&merkle, &l2s, keys_hashes);
                                    merkle.recompute_l2_subset_from_db(&by_l1, &dirty_in_l1);
                                }
                                Err(e) => {
                                    warn!("[merkle_incremental] Infohash 失败: {}", e);
                                    for &l2 in &l2s {
                                        merkle.mark_dirty_l2(l2);
                                    }
                                }
                            }
                        }
                        Ok(())
                    }
                },
            );
        }

        // Tracker 增量更新
        {
            let st = storage.clone();
            let sm = fed.sync_manager.clone();
            task_scheduler.register(
                TaskMetadata::new(
                    "merkle_incremental_tracker",
                    "Merkle增量更新-Tracker",
                    std::time::Duration::from_secs(incremental_interval),
                )
                .with_category(TaskCategory::Persistence)
                .with_priority(TaskPriority::Background)
                .with_resource(ResourceProfile {
                    cpu: ResourceLevel::Low,
                    memory: ResourceLevel::Low,
                    io: ResourceLevel::Medium,
                    network: ResourceLevel::Low,
                    is_full_task: false,
                })
                // [ALLOWED-HARDCODED: TaskScheduler 任务首次启动延迟，一次性启动装配参数，不影响运行时行为]
                .with_initial_delay(std::time::Duration::from_secs(30)),
                move || {
                    let st = st.clone();
                    let sm = sm.clone();
                    async move {
                        if let Some(merkle) = sm.tracker_merkle() {
                            let dirty = merkle.take_dirty_l2_shards();
                            if dirty.is_empty() {
                                return Ok(());
                            }
                            let l2s: Vec<u32> = dirty.into_iter().collect();
                            let shards: Vec<u16> = l2s.iter().map(|&l| l as u16).collect();
                            match st.load_tracker_keys_hashes_by_shards(&shards) {
                                Ok(keys_hashes) => {
                                    let (by_l1, dirty_in_l1) =
                                        build_l2_recompute_input(&merkle, &l2s, keys_hashes);
                                    merkle.recompute_l2_subset_from_db(&by_l1, &dirty_in_l1);
                                }
                                Err(e) => {
                                    warn!("[merkle_incremental] Tracker 失败: {}", e);
                                    for &l2 in &l2s {
                                        merkle.mark_dirty_l2(l2);
                                    }
                                }
                            }
                        }
                        Ok(())
                    }
                },
            );
        }

        // Node 冷重算
        {
            let st = storage.clone();
            let sm = fed.sync_manager.clone();
            task_scheduler.register(
                TaskMetadata::new(
                    "merkle_cold_rebuild_node",
                    "Merkle冷数据重算-Node",
                    std::time::Duration::from_secs(cold_rebuild_interval),
                )
                .with_category(TaskCategory::Persistence)
                .with_priority(TaskPriority::Background)
                .with_resource(ResourceProfile {
                    cpu: ResourceLevel::Medium,
                    memory: ResourceLevel::Medium,
                    io: ResourceLevel::High,
                    network: ResourceLevel::Low,
                    is_full_task: false,
                })
                .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "merkle_cold_rebuild_node_initial_delay",
                    60,
                ))),
                move || {
                    let st = st.clone();
                    let sm = sm.clone();
                    async move {
                        match st.load_all_node_keys_hashes() {
                            Ok(keys_hashes) => {
                                let count = keys_hashes.len();
                                sm.node_merkle().rebuild_cold_from_db(&keys_hashes);
                                info!("[merkle_cold_rebuild] Node: {} 条，冷根已重算", count);
                            }
                            Err(e) => warn!("[merkle_cold_rebuild] Node 失败: {}", e),
                        }
                        Ok(())
                    }
                },
            );
        }

        // Peer 冷重算
        {
            let st = storage.clone();
            let sm = fed.sync_manager.clone();
            task_scheduler.register(
                TaskMetadata::new(
                    "merkle_cold_rebuild_peer",
                    "Merkle冷数据重算-Peer",
                    std::time::Duration::from_secs(cold_rebuild_interval),
                )
                .with_category(TaskCategory::Persistence)
                .with_priority(TaskPriority::Background)
                .with_resource(ResourceProfile {
                    cpu: ResourceLevel::Medium,
                    memory: ResourceLevel::Medium,
                    io: ResourceLevel::High,
                    network: ResourceLevel::Low,
                    is_full_task: false,
                })
                .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "merkle_cold_rebuild_peer_initial_delay",
                    75,
                ))),
                move || {
                    let st = st.clone();
                    let sm = sm.clone();
                    async move {
                        if let Some(merkle) = sm.peer_merkle() {
                            match st.load_all_peer_keys_hashes() {
                                Ok(keys_hashes) => {
                                    let count = keys_hashes.len();
                                    merkle.rebuild_cold_from_db(&keys_hashes);
                                    info!("[merkle_cold_rebuild] Peer: {} 条，冷根已重算", count);
                                }
                                Err(e) => warn!("[merkle_cold_rebuild] Peer 失败: {}", e),
                            }
                        }
                        Ok(())
                    }
                },
            );
        }

        // Infohash 冷重算
        {
            let st = storage.clone();
            let sm = fed.sync_manager.clone();
            task_scheduler.register(
                TaskMetadata::new(
                    "merkle_cold_rebuild_infohash",
                    "Merkle冷数据重算-Infohash",
                    std::time::Duration::from_secs(cold_rebuild_interval),
                )
                .with_category(TaskCategory::Persistence)
                .with_priority(TaskPriority::Background)
                .with_resource(ResourceProfile {
                    cpu: ResourceLevel::Medium,
                    memory: ResourceLevel::Medium,
                    io: ResourceLevel::High,
                    network: ResourceLevel::Low,
                    is_full_task: false,
                })
                .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "merkle_cold_rebuild_infohash_initial_delay",
                    90,
                ))),
                move || {
                    let st = st.clone();
                    let sm = sm.clone();
                    async move {
                        if let Some(merkle) = sm.infohash_merkle() {
                            match st.load_all_infohash_keys_hashes() {
                                Ok(keys_hashes) => {
                                    let count = keys_hashes.len();
                                    merkle.rebuild_cold_from_db(&keys_hashes);
                                    info!(
                                        "[merkle_cold_rebuild] Infohash: {} 条，冷根已重算",
                                        count
                                    );
                                }
                                Err(e) => warn!("[merkle_cold_rebuild] Infohash 失败: {}", e),
                            }
                        }
                        Ok(())
                    }
                },
            );
        }

        // Tracker 冷重算
        {
            let st = storage.clone();
            let sm = fed.sync_manager.clone();
            task_scheduler.register(
                TaskMetadata::new(
                    "merkle_cold_rebuild_tracker",
                    "Merkle冷数据重算-Tracker",
                    std::time::Duration::from_secs(cold_rebuild_interval),
                )
                .with_category(TaskCategory::Persistence)
                .with_priority(TaskPriority::Background)
                .with_resource(ResourceProfile {
                    cpu: ResourceLevel::Low,
                    memory: ResourceLevel::Low,
                    io: ResourceLevel::Medium,
                    network: ResourceLevel::Low,
                    is_full_task: false,
                })
                .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "merkle_cold_rebuild_tracker_initial_delay",
                    105,
                ))),
                move || {
                    let st = st.clone();
                    let sm = sm.clone();
                    async move {
                        if let Some(merkle) = sm.tracker_merkle() {
                            match st.load_all_tracker_keys_hashes() {
                                Ok(keys_hashes) => {
                                    let count = keys_hashes.len();
                                    merkle.rebuild_cold_from_db(&keys_hashes);
                                    info!(
                                        "[merkle_cold_rebuild] Tracker: {} 条，冷根已重算",
                                        count
                                    );
                                }
                                Err(e) => warn!("[merkle_cold_rebuild] Tracker 失败: {}", e),
                            }
                        }
                        Ok(())
                    }
                },
            );
        }

        // P1-2：oplog 保留窗口裁剪（窗口默认 24h，必须 > 预估 bootstrap 时长）
        {
            let st = storage.clone();
            let retention = config.federation.oplog_retention_secs;
            task_scheduler.register(
                TaskMetadata::new(
                    "oplog_trim",
                    "oplog保留窗口裁剪",
                    std::time::Duration::from_secs(get_interval_secs(
                        intervals,
                        "oplog_trim_interval",
                        config.federation.oplog_trim_interval_secs,
                    )),
                )
                .with_category(TaskCategory::Persistence)
                .with_priority(TaskPriority::Background)
                .with_resource(ResourceProfile {
                    cpu: ResourceLevel::Low,
                    memory: ResourceLevel::Low,
                    io: ResourceLevel::Low,
                    network: ResourceLevel::Low,
                    is_full_task: false,
                })
                .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "oplog_trim_initial_delay",
                    300,
                ))),
                move || {
                    let st = st.clone();
                    async move {
                        match st.trim_oplog_by_retention(retention) {
                            Ok(n) if n > 0 => {
                                debug!("[oplog] 裁剪 {} 条（保留窗口 {}s）", n, retention)
                            }
                            Ok(_) => {}
                            Err(e) => warn!("[oplog] 裁剪失败: {}", e),
                        }
                        Ok(())
                    }
                },
            );
        }

        // oplog 行数缓存预热：首次 COUNT(feed_oplog) 在大表慢盘节点上可达数十秒
        // （2026-09-21 实测 51 节点 58 万行冷查询 28s，曾把首个 /sync-observability 请求拖到超时）。
        // 启动 5 秒后在后台完成一次校准，此后 `oplog_len()` 恒为 O(1)。
        {
            let st = storage.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(OPLOG_LEN_PREWARM_DELAY_SECS))
                    .await;
                match st.oplog_len() {
                    Ok(n) => info!("[oplog] 行数缓存预热完成: {} 条", n),
                    Err(e) => warn!("[oplog] 行数缓存预热失败: {}", e),
                }
            });
        }
    }

    // 8.9 远程 Tracker 列表定期刷新任务
    if config.discoverers.enable_remote_tracker {
        let remote_url = config.discoverers.remote_tracker_url.clone();
        let _discoverer_ref = fetcher_ref.clone();
        let tracker_repo_clone = tracker_repo.clone();
        task_scheduler.register(
            TaskMetadata::new("remote_tracker_refresh", "远程Tracker列表刷新", std::time::Duration::from_secs(get_interval_secs(intervals, "remote_tracker_refresh", 3600)))
            .with_category(TaskCategory::Network)
                .with_priority(TaskPriority::Background)
                .with_resource(ResourceProfile {
                    cpu: ResourceLevel::Low,
                    memory: ResourceLevel::Low,
                    io: ResourceLevel::Low,
                    network: ResourceLevel::Low,
                    is_full_task: false,
                })
                .with_jitter(std::time::Duration::from_secs(get_interval_secs(intervals, "remote_tracker_refresh_jitter", 30))),
            move || {
                let remote_url = remote_url.clone();
                let tracker_repo = tracker_repo_clone.clone();
                async move {
                    info!("[main] 定时刷新远程 Tracker 列表: {}", remote_url);
                    match PeerDiscoveryCenter::discoverers::tracker::TrackerDiscoverer::fetch_remote_trackers(&remote_url).await {
                        Ok(trackers) => {
                            for url in &trackers {
                                tracker_repo.add_tracker(url.clone()).await;
                            }
                            info!("[main] 远程 Tracker 列表刷新成功，共 {} 个", trackers.len());
                        }
                        Err(e) => {
                            warn!("[main] 远程 Tracker 列表刷新失败: {}", e);
                        }
                    }
                    Ok(())
                }
            },
        );
        info!("[main] 远程 Tracker 列表刷新任务已注册到 TaskScheduler");
    }

    // 8.10 tracker_fetcher: Tracker主动拉取peer（300s）
    if let Some(ref tf) = fetcher_ref {
        let t = tf.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "tracker_fetcher",
                "Tracker主动拉取peer",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "tracker_fetcher",
                    300,
                )),
            )
            .with_category(TaskCategory::Network)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Medium,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "tracker_fetcher_initial_delay",
                120,
            ))),
            move || {
                let t = t.clone();
                async move {
                    t.run_once().await;
                    Ok(())
                }
            },
        );
    }

    // 8.11 subscription_fetch: 外部订阅源拉取（3600s）
    let ss = subscription_service.clone();
    task_scheduler.register(
        TaskMetadata::new(
            "subscription_fetch",
            "外部订阅源拉取",
            std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "subscription_fetch",
                3600,
            )),
        )
        .with_category(TaskCategory::Network)
        .with_priority(TaskPriority::Background)
        .with_resource(ResourceProfile {
            cpu: ResourceLevel::Low,
            memory: ResourceLevel::Low,
            io: ResourceLevel::Low,
            network: ResourceLevel::Medium,
            is_full_task: false,
        })
        .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
            intervals,
            "subscription_fetch_initial_delay",
            180,
        ))),
        move || {
            let s = ss.clone();
            async move {
                s.run_once().await;
                Ok(())
            }
        },
    );

    // 8.12 keyword_search: DHT关键词搜索（3600s）
    let ks = keyword_service.clone();
    task_scheduler.register(
        TaskMetadata::new(
            "keyword_search",
            "DHT关键词搜索",
            std::time::Duration::from_secs(get_interval_secs(intervals, "keyword_search", 3600)),
        )
        .with_category(TaskCategory::Network)
        .with_priority(TaskPriority::Background)
        .with_resource(ResourceProfile {
            cpu: ResourceLevel::Low,
            memory: ResourceLevel::Low,
            io: ResourceLevel::Low,
            network: ResourceLevel::Medium,
            is_full_task: false,
        })
        .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
            intervals,
            "keyword_search_initial_delay",
            240,
        ))),
        move || {
            let k = ks.clone();
            async move {
                k.run_once().await;
                Ok(())
            }
        },
    );

    // 8.13 dht_probe_poll: DHT探测PeerRepo拉取（15s）
    if let Some(ref dp) = dht_probe_clone {
        let d = dp.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "dht_probe_poll",
                "DHT探测PeerRepo拉取",
                std::time::Duration::from_secs(get_interval_secs(intervals, "dht_probe_poll", 15)),
            )
            .with_category(TaskCategory::Network)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            }),
            move || {
                let d = d.clone();
                async move {
                    d.run_once().await;
                    Ok(())
                }
            },
        );
    }

    // 8.14 active_pex: 主动PEX请求（30s）
    if let Some(ref ap) = active_pex_clone {
        let a = ap.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "active_pex",
                "主动PEX请求",
                std::time::Duration::from_secs(get_interval_secs(intervals, "active_pex", 30)),
            )
            .with_category(TaskCategory::Network)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Medium,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "active_pex_initial_delay",
                30,
            ))),
            move || {
                let a = a.clone();
                async move {
                    a.run_once().await;
                    Ok(())
                }
            },
        );
    }

    // 8.15 http_tracker_cleanup: HTTP Tracker过期peer清理（60s）
    let st = super_tracker.clone();
    task_scheduler.register(
        TaskMetadata::new(
            "http_tracker_cleanup",
            "HTTP Tracker过期peer清理",
            std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "http_tracker_cleanup",
                60,
            )),
        )
        .with_category(TaskCategory::Monitor)
        .with_priority(TaskPriority::Background)
        .with_resource(ResourceProfile {
            cpu: ResourceLevel::Low,
            memory: ResourceLevel::Low,
            io: ResourceLevel::Low,
            network: ResourceLevel::Low,
            is_full_task: false,
        }),
        move || {
            let s = st.clone();
            async move {
                s.cleanup_expired().await;
                Ok(())
            }
        },
    );

    // 8.16 relay_cleanup: 中继过期连接清理（30s）
    if let Some(ref relay) = relay_server_ref {
        let r = relay.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "relay_cleanup",
                "中继过期连接清理",
                std::time::Duration::from_secs(get_interval_secs(intervals, "relay_cleanup", 30)),
            )
            .with_category(TaskCategory::Network)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "relay_cleanup_initial_delay",
                60,
            ))),
            move || {
                let r = r.clone();
                async move {
                    r.cleanup_expired();
                    Ok(())
                }
            },
        );
    }

    // 8.17 udp_tracker_cleanup: UDP Tracker连接ID过期清理（60s）
    if let Some(ref ut) = app_state.udp_tracker {
        let ut_clone = ut.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "udp_tracker_cleanup",
                "UDP Tracker连接ID过期清理",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "udp_tracker_cleanup",
                    60,
                )),
            )
            .with_category(TaskCategory::Network)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "udp_tracker_cleanup_initial_delay",
                120,
            ))),
            move || {
                let u = ut_clone.clone();
                async move {
                    u.cleanup_expired_connections().await;
                    Ok(())
                }
            },
        );
    }

    // 8.18 crawler 定时任务（8个）
    if let Some(ref crawler) = crawler_ref {
        // crawler_bootstrap: 重新bootstrap（120s）
        let c1 = crawler.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "crawler_bootstrap",
                "爬虫Bootstrap",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "crawler_bootstrap",
                    120,
                )),
            )
            .with_category(TaskCategory::Crawl)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Medium,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "crawler_bootstrap_initial_delay",
                120,
            ))),
            move || {
                let c = c1.clone();
                async move {
                    c.bootstrap().await;
                    Ok(())
                }
            },
        );

        // crawler_keepalive: 节点保活ping（60s）
        let c2 = crawler.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "crawler_keepalive",
                "爬虫节点保活",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "crawler_keepalive",
                    60,
                )),
            )
            .with_category(TaskCategory::Crawl)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "crawler_keepalive_initial_delay",
                60,
            ))),
            move || {
                let c = c2.clone();
                async move {
                    c.active_keepalive().await;
                    Ok(())
                }
            },
        );

        // crawler_active_crawl: 主动爬行（30s）
        let c3 = crawler.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "crawler_active_crawl",
                "爬虫主动爬行",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "crawler_active_crawl",
                    30,
                )),
            )
            .with_category(TaskCategory::Crawl)
            .with_priority(TaskPriority::Important)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Medium,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::High,
                is_full_task: false,
            }),
            move || {
                let c = c3.clone();
                async move {
                    c.active_crawl().await;
                    Ok(())
                }
            },
        );

        // crawler_get_peers: 主动get_peers（10s）
        let c4 = crawler.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "crawler_get_peers",
                "爬虫主动get_peers",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "crawler_get_peers",
                    10,
                )),
            )
            .with_category(TaskCategory::Crawl)
            .with_priority(TaskPriority::Important)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Medium,
                is_full_task: false,
            }),
            move || {
                let c = c4.clone();
                async move {
                    c.active_get_peers().await;
                    Ok(())
                }
            },
        );

        // crawler_sample_infohashes: 主动sample_infohashes（15s）
        let c5 = crawler.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "crawler_sample_infohashes",
                "爬虫主动sample_infohashes",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "crawler_sample_infohashes",
                    15,
                )),
            )
            .with_category(TaskCategory::Crawl)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Medium,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "crawler_sample_infohashes_initial_delay",
                30,
            ))),
            move || {
                let c = c5.clone();
                async move {
                    c.active_sample_infohashes().await;
                    Ok(())
                }
            },
        );

        // crawler_scrape: 主动scrape（60s）
        let c6 = crawler.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "crawler_scrape",
                "爬虫主动scrape",
                std::time::Duration::from_secs(get_interval_secs(intervals, "crawler_scrape", 60)),
            )
            .with_category(TaskCategory::Crawl)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Medium,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "crawler_scrape_initial_delay",
                60,
            ))),
            move || {
                let c = c6.clone();
                async move {
                    c.active_scrape().await;
                    Ok(())
                }
            },
        );

        // crawler_cleanup_pending: 清理超时pending请求（30s）
        let c7 = crawler.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "crawler_cleanup_pending",
                "爬虫清理超时请求",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "crawler_cleanup_pending",
                    30,
                )),
            )
            .with_category(TaskCategory::Crawl)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            }),
            move || {
                let c = c7.clone();
                async move {
                    c.cleanup_pending();
                    Ok(())
                }
            },
        );

        // crawler_bucket_refresh: 路由表bucket刷新（300s）
        let c8 = crawler.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "crawler_bucket_refresh",
                "爬虫Bucket刷新",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "crawler_bucket_refresh",
                    300,
                )),
            )
            .with_category(TaskCategory::Crawl)
            .with_priority(TaskPriority::Normal)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Medium,
                is_full_task: false,
            })
            .with_initial_delay(std::time::Duration::from_secs(get_interval_secs(
                intervals,
                "crawler_bucket_refresh_initial_delay",
                300,
            ))),
            move || {
                let c = c8.clone();
                async move {
                    c.refresh_buckets().await;
                    Ok(())
                }
            },
        );

        // crawler_update_metrics: 刷新监控指标（PPS/响应率/pending分片长度/丢包估算），每5秒
        let c9 = crawler.clone();
        task_scheduler.register(
            TaskMetadata::new(
                "crawler_update_metrics",
                "爬虫监控指标刷新",
                std::time::Duration::from_secs(get_interval_secs(
                    intervals,
                    "crawler_update_metrics",
                    5,
                )),
            )
            .with_category(TaskCategory::Monitor)
            .with_priority(TaskPriority::Background)
            .with_resource(ResourceProfile {
                cpu: ResourceLevel::Low,
                memory: ResourceLevel::Low,
                io: ResourceLevel::Low,
                network: ResourceLevel::Low,
                is_full_task: false,
            }),
            move || {
                let c = c9.clone();
                async move {
                    c.update_metrics();
                    Ok(())
                }
            },
        );
    }

    // 8.5.15 配置热更新监听任务（简化版：检查 mtime + 日志，不实际更新运行时参数）
    {
        let reload_interval = std::time::Duration::from_secs(config.config_reload_interval_secs);
        if reload_interval.as_secs() > 0 {
            let watch_path = config_path.clone();
            let last_mtime: Arc<std::sync::Mutex<Option<std::time::SystemTime>>> =
                Arc::new(std::sync::Mutex::new(None));
            task_scheduler.register(
                TaskMetadata::new(
                    "config_reloader",
                    "配置热更新监听",
                    reload_interval,
                )
                .with_category(TaskCategory::Monitor)
                .with_priority(TaskPriority::Normal)
                .with_resource(ResourceProfile {
                    cpu: ResourceLevel::Low,
                    memory: ResourceLevel::Low,
                    io: ResourceLevel::Low,
                    network: ResourceLevel::Low,
                    is_full_task: false,
                }),
                move || {
                    let watch_path = watch_path.clone();
                    let last_mtime = last_mtime.clone();
                    async move {
                        let Some(ref path) = watch_path else { return Ok(()); };
                        let metadata = match std::fs::metadata(path) {
                            Ok(m) => m,
                            Err(_) => return Ok(()),
                        };
                        let current_mtime = match metadata.modified() {
                            Ok(t) => t,
                            Err(_) => return Ok(()),
                        };
                        let mut last = last_mtime.lock().unwrap();
                        match *last {
                            None => {
                                *last = Some(current_mtime);
                            }
                            Some(prev) if prev != current_mtime => {
                                info!(
                                    "[config] 检测到配置文件变化: {}（当前简化版仅记录日志，运行时参数暂不生效）",
                                    path
                                );
                                *last = Some(current_mtime);
                            }
                            Some(_) => {}
                        }
                        Ok(())
                    }
                },
            );
            info!(
                "[main] 配置热更新监听任务已注册（间隔 {} 秒）",
                config.config_reload_interval_secs
            );
        } else {
            info!("[main] 配置热更新已禁用（config_reload_interval_secs = 0）");
        }
    }

    // 启动统一任务调度中心
    task_scheduler.start();
    info!("[main] TaskScheduler 已启动（统一调度所有后台任务，14个任务已注册）");

    // 启动统计快照后台更新任务（API stats 只读快照，不阻塞 API 线程）
    let stats_shutdown = stats_snapshot::spawn_snapshot_updater(
        app_state.clone(),
        config.stats_snapshot_interval_secs,
    );

    // 9. 启动 UDP Tracker 服务（BEP 15）—— 在 tracker_runtime 上运行，与爬虫隔离
    if config.super_tracker.enabled {
        let udp_state = app_state.clone();
        tracker_handle.spawn(async move {
            if let Err(e) = DataPlane::serve_udp(udp_state).await {
                warn!("[main] UDP Tracker 服务异常: {}", e);
            }
        });
        let udp_port = config.super_tracker.udp_port.unwrap_or(config.server.port);
        info!(
            "[main] UDP Tracker 已启动(tracker_runtime): udp://{}:{}",
            config.server.listen, udp_port
        );
    }

    // 11. 启动 HTTP 服务（超级 Tracker + API/监控 分离）
    info!(
        "[main] 超级 Tracker HTTP: http://{}:{}/announce (公网)",
        config.server.listen, config.server.port
    );
    info!(
        "[main]   Scrape:           http://{}:{}/scrape",
        config.server.listen, config.server.port
    );
    info!(
        "[main] API/监控 HTTP:    http://{}:{}/api/v1/stats (局域网+token鉴权，不映射公网)",
        config.server.listen, config.server.api_port
    );
    info!(
        "[main]   健康检查:         http://{}:{}/health",
        config.server.listen, config.server.api_port
    );
    info!(
        "[main]   联邦状态:         http://{}:{}/api/v1/federation/status",
        config.server.listen, config.server.api_port
    );
    info!(
        "[main]   WebSocket:        ws://{}:{}/ws",
        config.server.listen, config.server.api_port
    );

    // 11. 启动 HTTP 服务（三 runtime 隔离：Tracker 在 tracker_runtime，API 在 api_runtime）
    // 超级 Tracker HTTP 在 tracker_runtime 上运行
    if config.super_tracker.enabled {
        let tracker_state = app_state.clone();
        tracker_handle.spawn(async move {
            if let Err(e) = DataPlane::serve_tracker(tracker_state).await {
                warn!("[main] 超级 Tracker HTTP 异常退出: {}", e);
            }
        });
        info!(
            "[main] 超级 Tracker HTTP 已启动(tracker_runtime): http://{}:{}/announce",
            config.server.listen, config.server.port
        );
    }

    // API/监控 HTTP 在 api_runtime 上运行
    let api_state = app_state.clone();
    let api_join = api_handle.spawn(async move {
        if let Err(e) = DataPlane::serve_api(api_state).await {
            warn!("[main] API/监控 HTTP 异常退出: {}", e);
        }
    });
    info!(
        "[main] API/监控 HTTP 已启动(api_runtime): http://{}:{}/api/v1/stats",
        config.server.listen, config.server.api_port
    );

    // 优雅关闭：等待 Ctrl+C
    tokio::signal::ctrl_c().await.ok();
    info!("[main] 收到关闭信号，开始优雅关闭...");

    // 通知统计快照后台任务退出
    stats_shutdown.notify_waiters();

    info!("[main] 正在保存数据...");

    // 全量保存所有 Repo 到 SQLite（统一数据归口）
    if let Some(ref repo) = app_state.node_repo {
        match repo.save_dirty().await {
            Ok(_) => info!(
                "[main] NodeRepo 已保存（{} 个节点）",
                repo.node_count().await
            ),
            Err(e) => warn!("[main] NodeRepo 保存失败: {}", e),
        }
    }
    if let Some(ref repo) = app_state.tracker_repo {
        match repo.save_all().await {
            Ok(_) => info!(
                "[main] TrackerRepo 已保存（{} 个 tracker）",
                repo.count().await
            ),
            Err(e) => warn!("[main] TrackerRepo 保存失败: {}", e),
        }
    }
    if let Some(ref repo) = app_state.infohash_repo {
        match repo.save_all().await {
            Ok(_) => info!(
                "[main] InfohashRepo 已保存（{} 个 infohash）",
                repo.count().await
            ),
            Err(e) => warn!("[main] InfohashRepo 保存失败: {}", e),
        }
    }
    {
        let repo = &app_state.peer_repo;
        match repo.save_all().await {
            Ok(_) => info!("[main] PeerRepo 已保存（{} 个 peer）", repo.len()),
            Err(e) => warn!("[main] PeerRepo 保存失败: {}", e),
        }
    }

    // 释放 UPnP 映射
    app_state.nat.release_all().await;
    info!("[main] UPnP 映射已释放");

    // 中止 API 服务任务（tracker_runtime 和 api_runtime 由 main() 统一 shutdown）
    api_join.abort();
    info!("[main] 优雅关闭完成");

    info!("[main] 主函数正常返回，进程退出");
    Ok(())
}

/// 初始化日志
/// 安装崩溃捕获：panic hook + Windows SEH 异常过滤器
/// 捕获 Rust panic、栈溢出、access violation 等，写入 logs_dir/crash.log
fn install_crash_handler(logs_dir: &std::path::Path) {
    let _ = std::fs::create_dir_all(logs_dir);
    let crash_log = logs_dir.join("crash.log");
    let panic_log = logs_dir.join("crash-panic.log");
    let seh_log = logs_dir.join("crash-seh.log");
    let crash_log_str = crash_log.to_string_lossy().to_string();
    let panic_log_str = panic_log.to_string_lossy().to_string();
    let _seh_log_str = seh_log.to_string_lossy().to_string();

    // 1. Rust panic hook（捕获 unwind panic，含 tokio task panic）
    std::panic::set_hook(Box::new(move |info| {
        let msg = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "未知 panic 信息".to_string());
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "未知位置".to_string());
        let backtrace = std::backtrace::Backtrace::force_capture();
        let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let report = format!(
            "===== PANIC CRASH REPORT =====\n\
             时间: {}\n\
             线程: {:?}\n\
             位置: {}\n\
             信息: {}\n\
             堆栈:\n{}\n\
             ===============================\n",
            timestamp,
            std::thread::current().name(),
            location,
            msg,
            backtrace
        );
        eprintln!("{}", report);
        let _ = std::fs::write(&crash_log_str, &report);
        let _ = std::fs::write(&panic_log_str, &report);
    }));

    // 2. Windows SEH 异常过滤器（捕获 access violation、栈溢出等非 Rust panic）
    #[cfg(windows)]
    {
        use std::ffi::c_void;
        type ExceptionFilter = unsafe extern "system" fn(*mut c_void) -> u32;
        extern "system" {
            fn SetUnhandledExceptionFilter(lpFilter: ExceptionFilter) -> ExceptionFilter;
        }
        unsafe extern "system" fn seh_filter(exception_ptrs: *mut c_void) -> u32 {
            let code = if !exception_ptrs.is_null() {
                let record_ptr = *(exception_ptrs as *const *const u32);
                if !record_ptr.is_null() {
                    *record_ptr
                } else {
                    0
                }
            } else {
                0
            };
            let code_name = match code {
                0xC0000005 => "ACCESS_VIOLATION",
                0xC00000FD => "STACK_OVERFLOW",
                0xC000001D => "ILLEGAL_INSTRUCTION",
                0xC0000094 => "INTEGER_DIVIDE_BY_ZERO",
                0xC0000095 => "INTEGER_OVERFLOW",
                0xC000008C => "ARRAY_BOUNDS_EXCEEDED",
                0xE06D7363 => "CPP_EXCEPTION",
                _ => "UNKNOWN",
            };
            let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
            let report = format!(
                "===== SEH CRASH REPORT =====\n\
                 时间: {}\n\
                 异常码: 0x{:08X} ({})\n\
                 ExceptionPointers: {:?}\n\
                 =============================\n",
                timestamp, code, code_name, exception_ptrs
            );
            eprintln!("{}", report);
            // 注意：seh_filter 是 extern fn，不能直接捕获外部变量，用固定路径
            let _ = std::fs::write("./logs/crash.log", &report);
            let _ = std::fs::write("./logs/crash-seh.log", &report);
            1 // EXCEPTION_EXECUTE_HANDLER
        }
        unsafe {
            SetUnhandledExceptionFilter(seh_filter);
        }
        eprintln!(
            "[crash-handler] Windows SEH 异常过滤器已安装（日志: {}）",
            logs_dir.display()
        );
    }
}

/// 安装 Windows 控制台控制处理器
///
/// 后台运行时，父控制台关闭会发送 CTRL_CLOSE_EVENT，默认处理器直接调用 ExitProcess
/// 瞬间终止进程（无 panic、无日志、无优雅关闭）。本处理器忽略 CTRL_CLOSE_EVENT，
/// 让进程在后台持续运行。CTRL_C_EVENT 和 CTRL_BREAK_EVENT 仍由默认处理器处理
/// （tokio::signal::ctrl_c 会捕获）。
#[cfg(windows)]
fn install_console_ctrl_handler() {
    use std::ffi::c_int;
    type HandlerRoutine = unsafe extern "system" fn(c_int) -> i32;
    extern "system" {
        fn SetConsoleCtrlHandler(handler: Option<HandlerRoutine>, add: i32) -> i32;
    }
    // CTRL_C_EVENT = 0, CTRL_BREAK_EVENT = 1, CTRL_CLOSE_EVENT = 2
    unsafe extern "system" fn console_ctrl_handler(ctrl_type: c_int) -> i32 {
        match ctrl_type {
            2 => {
                // CTRL_CLOSE_EVENT: 忽略，防止后台进程被父控制台关闭瞬间终止
                eprintln!("[console] 收到 CTRL_CLOSE_EVENT，忽略（后台持续运行）");
                1 // TRUE = 已处理，不调用后续处理器
            }
            _ => 0, // FALSE = 交给默认处理器（tokio::signal::ctrl_c 捕获 Ctrl+C）
        }
    }
    unsafe {
        SetConsoleCtrlHandler(Some(console_ctrl_handler), 1);
    }
    eprintln!("[console] Windows 控制台控制处理器已安装（忽略 CTRL_CLOSE_EVENT）");
}

/// 解析 --work-dir 命令行参数（返回 Some 时使用 Standalone 模式，否则由 WorkDir::auto_detect 决定）
fn parse_work_dir_arg() -> Option<std::path::PathBuf> {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if args[i].as_str() == "--work-dir" && i + 1 < args.len() {
            return Some(std::path::PathBuf::from(&args[i + 1]));
        }
        i += 1;
    }
    None
}

/// 加载或生成 PEX/uTP 节点身份（持久化到 node_id_path）
/// 文件格式：十六进制字符串（40字符），一行
fn load_or_generate_node_id(node_id_path: &std::path::Path) -> [u8; 20] {
    if node_id_path.exists() {
        if let Ok(content) = std::fs::read_to_string(node_id_path) {
            let hex_str = content.trim();
            if hex_str.len() == 40 {
                let mut bytes = [0u8; 20];
                let mut ok = true;
                for i in 0..20 {
                    match u8::from_str_radix(&hex_str[i * 2..i * 2 + 2], 16) {
                        Ok(b) => bytes[i] = b,
                        Err(_) => {
                            ok = false;
                            break;
                        }
                    }
                }
                if ok {
                    info!("[main] 已加载节点身份: {}", hex_str);
                    return bytes;
                }
            }
            warn!("[main] node_id 文件格式错误，重新生成");
        }
    }
    // 生成新的随机节点身份
    let mut id = [0u8; 20];
    rand::thread_rng().fill(&mut id);
    let hex_str: String = id.iter().map(|b| format!("{:02x}", b)).collect();
    let _ = std::fs::write(node_id_path, &hex_str);
    info!(
        "[main] 已生成新节点身份: {}（持久化到 {}）",
        hex_str,
        node_id_path.display()
    );
    id
}

/// 预读配置文件中的 `log_level`（此时完整配置尚未加载，只取该字段）。
///
/// 路径解析顺序与 [`main`] 一致：`-c/--config` 显式指定 > 工作目录下 config.yaml。
/// 文件不存在 / 解析失败 / 值为空时回落 `info`。
fn peek_log_level(work_dir: &WorkDir) -> String {
    let path = match parse_config_path() {
        Some(p) => p,
        None => {
            let p = work_dir.config_file();
            if !p.exists() {
                return "info".to_string();
            }
            p.to_string_lossy().to_string()
        }
    };
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|content| serde_yaml::from_str::<serde_yaml::Value>(&content).ok())
        .and_then(|v| {
            v.get("log_level")
                .and_then(|x| x.as_str())
                .map(|s| s.trim().to_string())
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "info".to_string())
}

/// 初始化日志。
///
/// 级别来源优先级：`RUST_LOG` 环境变量 > 配置文件 `log_level` > `info`。
fn init_logging(config_log_level: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::try_new(config_log_level).unwrap_or_else(|_| EnvFilter::new("info"))
    });

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();
}

/// 解析命令行参数中的配置文件路径
fn parse_config_path() -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" | "-c" => {
                if i + 1 < args.len() {
                    return Some(args[i + 1].clone());
                }
            }
            "--help" | "-h" => {
                println!("PeerDiscoveryCenter v{}", PeerDiscoveryCenter::VERSION);
                println!();
                println!("用法:");
                println!("  pdc [OPTIONS]");
                println!();
                println!("选项:");
                println!("  -c, --config <FILE>     指定配置文件路径（优先级高于工作目录）");
                println!("      --work-dir <DIR>    指定工作目录（默认当前目录，或 PDC_WORK_DIR 环境变量）");
                println!("  -h, --help              显示帮助信息");
                println!();
                println!("工作目录结构:");
                println!("  <work_dir>/pdc-agent/config/config.yaml  配置文件（不存在则自动生成）");
                println!("  <work_dir>/pdc-agent/data/pdc.db         数据库文件");
                println!("  <work_dir>/pdc-agent/data/node_id        节点身份（持久化）");
                println!("  <work_dir>/pdc-agent/logs/               日志目录");
                println!();
                println!("默认配置:");
                println!("  监听: 0.0.0.0，默认端口 6880");
                println!("  超级 Tracker: 启用");
                println!("  发现器: tracker + dht + pex");
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }
    None
}
