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

use std::sync::Arc;

use parking_lot::RwLock;
use rand::Rng;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

use PeerDiscoveryCenter::config::PdcConfig;
use PeerDiscoveryCenter::control_plane::ControlPlane;
use PeerDiscoveryCenter::crawler::Crawler;
use PeerDiscoveryCenter::crawler::CrawlerEngine;
use PeerDiscoveryCenter::data_plane::http_tracker::SuperTrackerState;
use PeerDiscoveryCenter::data_plane::{AppState, DataPlane};
use PeerDiscoveryCenter::discoverers::DiscovererRegistry;
use PeerDiscoveryCenter::event_bus::EventBus;
use PeerDiscoveryCenter::health_check::{HealthCheckConfig, HealthCheckTask};
use PeerDiscoveryCenter::nat::NatManager;
use PeerDiscoveryCenter::storage::{InfohashRepository, NodeRepository, TrackerRepository};

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> anyhow::Result<()> {
    // 1. 初始化日志
    init_logging();

    // 1.5 初始化 Prometheus metrics
    PeerDiscoveryCenter::data_plane::metrics::init_metrics();
    info!("[main] Prometheus metrics 已初始化");

    info!("========================================");
    info!("PeerDiscoveryCenter v{} 启动", PeerDiscoveryCenter::VERSION);
    info!("========================================");

    // 2. 解析命令行参数，加载配置
    let config_path = parse_config_path();
    let config = match &config_path {
        Some(path) => {
            info!("[main] 从 {} 加载配置", path);
            PdcConfig::load_or_default(path)
        }
        None => {
            info!("[main] 未指定配置文件，使用默认配置");
            PdcConfig::default()
        }
    };

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
        Arc::new(PeerDiscoveryCenter::storage::Storage::open(&config.storage.path).expect("无法打开数据库"))
    } else {
        info!("[main] 持久化存储未启用，使用内存模式");
        Arc::new(PeerDiscoveryCenter::storage::Storage::memory().expect("无法创建内存数据库"))
    };

    // 3.6 创建所有数据层 Repo（统一数据归口）
    let peer_repo = Arc::new(PeerDiscoveryCenter::storage::PeerRepoImpl::new(storage.clone()));
    let infohash_repo = Arc::new(PeerDiscoveryCenter::storage::InfohashRepoImpl::new(storage.clone()));
    let tracker_repo = Arc::new(PeerDiscoveryCenter::storage::TrackerRepoImpl::new(storage.clone()));
    let node_repo = Arc::new(PeerDiscoveryCenter::storage::NodeRepoImpl::new(storage.clone()));
    info!("[main] 数据层 Repo 已初始化（Node/Peer/Infohash/Tracker）");

    // 3.7 从 SQLite 加载持久化数据
    match tracker_repo.load_all().await {
        Ok(n) if n > 0 => info!("[main] 从 SQLite 加载了 {} 个 Tracker", n),
        _ => {}
    }
    match infohash_repo.load_all().await {
        Ok(n) if n > 0 => info!("[main] 从 SQLite 加载了 {} 个 Infohash", n),
        _ => {}
    }
    match node_repo.load_all().await {
        Ok(n) if n > 0 => info!("[main] 从 SQLite 加载了 {} 个 DHT 节点", n),
        _ => {}
    }
    match peer_repo.load_all().await {
        Ok(n) if n > 0 => info!("[main] 从 SQLite 加载了 {} 个 Peer", n),
        _ => {}
    }

    // 3.8 创建控制面（注入 tracker_repo，发现器初始化时自动同步）
    let control_plane = ControlPlane::new(config.clone(), registry.clone(), event_bus.clone())
        .with_tracker_repo(tracker_repo.clone());

    // 4. 初始化默认发现器（TrackerDiscoverer 会自动注入 tracker_repo）
    control_plane.init_default_discoverers();
    info!("[main] 已注册 {} 个发现器", control_plane.registry().len());

    // 5. 创建超级 Tracker 状态（注入 peer_repo，announce peer 双写）
    let super_tracker = Arc::new(
        SuperTrackerState::new(config.super_tracker.clone())
            .with_peer_repo(peer_repo.clone()),
    );

    // 6.5 创建 NAT 管理器，UPnP 端口映射异步后台初始化（不阻塞 HTTP 服务启动）
    let nat_config = PeerDiscoveryCenter::nat::NatConfig {
        enabled: config.nat.enabled,
        lease_duration: config.nat.lease_duration,
        ..Default::default()
    };
    let nat = Arc::new(NatManager::new(nat_config));
    let udp_port = config.super_tracker.udp_port.unwrap_or(config.server.port);
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
            match nat_clone.init(http_port, udp_port, crawler_port, 6881, 6883, 6884).await {
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

    // 7. 创建爬虫引擎（如果启用），在 AppState 之前创建以便共享状态
    let (crawler_state, crawler_routing_table) = if config.crawler.enabled {
        let crawler = CrawlerEngine::new(config.crawler.clone(), event_bus.clone())
            .with_peer_repo(peer_repo.clone())
            .with_storage(storage.clone())
            .with_infohash_repo(infohash_repo.clone())
            .with_node_repo(node_repo.clone());
        let state_arc = crawler.state_arc();
        let routing_table = crawler.routing_table();

        // 启动爬虫
        tokio::spawn(async move {
            if let Err(e) = crawler.start().await {
                warn!("[main] 爬虫引擎启动失败: {}", e);
            }
        });
        info!("[main] 爬虫引擎已启动（主动模式）");
        (Some(state_arc), Some(routing_table))
    } else {
        info!("[main] 爬虫引擎未启用（config.crawler.enabled = false）");
        (None, None)
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
        let fetcher = PeerDiscoveryCenter::services::TrackerPeerFetcher::new(tracker_discoverer.clone())
            .with_peer_repo(peer_repo.clone())
            .with_infohash_repo(infohash_repo.clone());
        let fetcher = Arc::new(fetcher);
        fetcher.start();
        info!("[main] TrackerPeerFetcher 已启动（主动拉取 peer → 存入 PeerRepo）");

        // 远程 Tracker 列表自动拉取
        if config.discoverers.enable_remote_tracker {
            let remote_url = config.discoverers.remote_tracker_url.clone();
            let refresh_secs = config.discoverers.remote_tracker_refresh_secs;
            let discoverer_clone = tracker_discoverer.clone();
            let tracker_repo_clone = tracker_repo.clone();

            tokio::spawn(async move {
                // 启动时立即拉取一次
                info!("[main] 开始从远程拉取 Tracker 列表: {}", remote_url);
                match PeerDiscoveryCenter::discoverers::tracker::TrackerDiscoverer::fetch_remote_trackers(&remote_url).await {
                    Ok(trackers) => {
                        discoverer_clone.update_trackers(trackers.clone());
                        // 同步到 TrackerRepo
                        for url in &trackers {
                            tracker_repo_clone.add_tracker(url.clone()).await;
                        }
                        info!("[main] 远程 Tracker 列表拉取成功，共 {} 个", trackers.len());
                    }
                    Err(e) => {
                        warn!("[main] 远程 Tracker 列表拉取失败，使用内置默认列表: {}", e);
                    }
                }

                // 定时刷新（跳过第一次立即触发）
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(refresh_secs));
                let mut first_tick = true;
                loop {
                    interval.tick().await;
                    if first_tick {
                        first_tick = false;
                        continue;
                    }
                    info!("[main] 定时刷新远程 Tracker 列表: {}", remote_url);
                    match PeerDiscoveryCenter::discoverers::tracker::TrackerDiscoverer::fetch_remote_trackers(&remote_url).await {
                        Ok(trackers) => {
                            discoverer_clone.update_trackers(trackers.clone());
                            for url in &trackers {
                                tracker_repo_clone.add_tracker(url.clone()).await;
                            }
                            info!("[main] 远程 Tracker 列表刷新成功，共 {} 个", trackers.len());
                        }
                        Err(e) => {
                            warn!("[main] 远程 Tracker 列表刷新失败，保持当前列表: {}", e);
                        }
                    }
                }
            });
            info!("[main] 远程 Tracker 列表自动拉取已启用（间隔 {} 秒）", refresh_secs);
        }

        Some(fetcher)
    };

    // 7.53 创建 SubscriptionService（外部订阅源导入 → InfohashRepo）
    let subscription_config = PeerDiscoveryCenter::services::SubscriptionConfig::default();
    let subscription_service = PeerDiscoveryCenter::services::SubscriptionService::new(subscription_config)
        .with_infohash_repo(infohash_repo.clone());
    let subscription_service = Arc::new(subscription_service);
    tokio::spawn(async move {
        subscription_service.start().await;
    });
    info!("[main] SubscriptionService 已启动（外部订阅源导入 → InfohashRepo）");

    // 7.54 创建 KeywordSearchService（DHT 关键词搜索 BEP 44 → InfohashRepo）
    let keyword_config = PeerDiscoveryCenter::services::KeywordSearchConfig::default();
    // 从 NodeRepo 获取前 8 个高评分节点作为查询入口
    let keyword_nodes = node_repo.top_nodes_sync(8).into_iter().map(|n| n.addr).collect();
    let keyword_service = PeerDiscoveryCenter::services::KeywordSearchService::new(keyword_config)
        .with_infohash_repo(infohash_repo.clone())
        .with_node_addrs(keyword_nodes);
    let keyword_service = Arc::new(keyword_service);
    tokio::spawn(async move {
        keyword_service.start().await;
    });
    info!("[main] KeywordSearchService 已启动（DHT 关键词搜索 BEP 44 → InfohashRepo）");

    // 7.5.1 创建限流器（P2优化：QPS监控+单IP限流+异常封禁）
    let rate_limiter = Arc::new(PeerDiscoveryCenter::data_plane::rate_limiter::RateLimiter::new(100.0, 200.0));
    info!("[main] 限流器已创建（单IP 100 QPS，突发 200）");

    // 7.5 初始化 PEX/uTP 服务（默认启用）
    let node_id = {
        let mut id = [0u8; 20];
        rand::thread_rng().fill(&mut id);
        id
    };

    // PEX 接收器（核心，被其他服务引用）
    let pex_receiver = Arc::new(
        PeerDiscoveryCenter::crawler::pex_receiver::PexReceiver::new()
            .with_peer_repo(peer_repo.clone()),
    );

    // uTP 服务端（UDP 6883）
    let utp_server = match tokio::net::UdpSocket::bind("0.0.0.0:6883").await {
        Ok(socket) => {
            let server = PeerDiscoveryCenter::crawler::utp_server::UtpServer::new(
                Arc::new(socket),
                node_id,
            )
            .with_peer_repo(peer_repo.clone())
            .with_pex_receiver(pex_receiver.clone());
            let server = Arc::new(server);
            let s = server.clone();
            tokio::spawn(async move {
                s.run().await;
            });
            info!("[main] uTP 服务端已启动（UDP 6883）");
            Some(server)
        }
        Err(e) => {
            warn!("[main] uTP 服务端启动失败（UDP 6883）: {}", e);
            None
        }
    };

    // TCP-PEX 服务端（TCP 6884）
    let tcp_pex_server = {
        let listen_addr = "0.0.0.0:6884".parse().unwrap();
        let server = PeerDiscoveryCenter::crawler::tcp_pex_server::TcpPexServer::new(
            listen_addr,
            node_id,
        )
        .with_peer_repo(peer_repo.clone())
        .with_pex_receiver(pex_receiver.clone());
        let server = Arc::new(server);
        let s = server.clone();
        tokio::spawn(async move {
            if let Err(e) = s.run().await {
                warn!("[main] TCP-PEX 服务端运行错误: {}", e);
            }
        });
        info!("[main] TCP-PEX 服务端已启动（TCP 6884）");
        Some(server)
    };

    // 主动 PEX 请求器
    let active_pex = {
        let requester = PeerDiscoveryCenter::crawler::active_pex::ActivePexRequester::new(
            peer_repo.clone(),
            node_id,
        )
        .with_pex_receiver(pex_receiver.clone())
        .with_interval(std::time::Duration::from_secs(30))
        .with_batch_size(20);
        let requester = Arc::new(requester);
        let r = requester.clone();
        tokio::spawn(async move {
            r.run().await;
        });
        info!("[main] 主动 PEX 请求器已启动");
        Some(requester)
    };

    // 7.6 创建 AppState
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
        fetcher: fetcher_ref,
        dht_probe: dht_probe_ref,
        rate_limiter,
        hole_punch_signaling: Arc::new(PeerDiscoveryCenter::data_plane::hole_punch_signaling::HolePunchSignaling::new()),
        utp_server,
        pex_receiver: Some(pex_receiver),
        tcp_pex_server,
        active_pex,
    };

    // 8. 启动健康检查任务
    let hc_config = HealthCheckConfig {
        interval: std::time::Duration::from_secs(config.health_check.interval_secs),
        cache_cleanup_interval: std::time::Duration::from_secs(
            config.health_check.cache_cleanup_interval_secs,
        ),
        stats_output_interval: std::time::Duration::from_secs(
            config.health_check.stats_output_interval_secs,
        ),
    };
    let health_check = Arc::new(HealthCheckTask::new(
        registry.clone(),
        peer_repo.clone(),
        Some(node_repo.clone()),
        Some(tracker_repo.clone()),
        Some(infohash_repo.clone()),
        hc_config,
        Some(storage.clone()),
    ));
    tokio::spawn(async move {
        health_check.run().await;
    });
    info!("[main] 健康检查任务已启动");

    // 8.5 启动定期持久化任务（每 60 秒全量保存所有 Repo 到 SQLite）
    // 启动后延迟 30 秒开始，与 ScoreMaintainer（第0秒开始）错开，避免同时竞争锁
    {
        let node_repo = node_repo.clone();
        let tracker_repo = tracker_repo.clone();
        let infohash_repo = infohash_repo.clone();
        let peer_repo = peer_repo.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            loop {
                if let Err(e) = node_repo.save_all().await {
                    debug!("[main] 定期持久化 NodeRepo 失败: {}", e);
                }
                if let Err(e) = tracker_repo.save_all().await {
                    debug!("[main] 定期持久化 TrackerRepo 失败: {}", e);
                }
                if let Err(e) = infohash_repo.save_all().await {
                    debug!("[main] 定期持久化 InfohashRepo 失败: {}", e);
                }
                if let Err(e) = peer_repo.save_all().await {
                    debug!("[main] 定期持久化 PeerRepo 失败: {}", e);
                }
                tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            }
        });
        info!("[main] 定期持久化任务已启动（每 5 分钟，延迟 30 秒开始）");
    }

    // 8.5.1 启动定期 WAL checkpoint 任务（每 10 分钟执行一次，减少 WAL 文件大小）
    {
        let storage = storage.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            loop {
                if let Err(e) = storage.checkpoint() {
                    warn!("[main] WAL checkpoint 失败: {}", e);
                }
                tokio::time::sleep(std::time::Duration::from_secs(600)).await;
            }
        });
        info!("[main] 定期 WAL checkpoint 任务已启动（每 10 分钟）");
    }

    // 8.5.2 启动定期 flush peer_history 任务（每 30 秒批量写入，减少 fsync）
    {
        let peer_repo = peer_repo.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                match peer_repo.flush_history().await {
                    Ok(count) if count > 0 => debug!("[main] peer_history 批量写入: {} 条", count),
                    Ok(_) => {}
                    Err(e) => warn!("[main] peer_history flush 失败: {}", e),
                }
            }
        });
        info!("[main] 定期 peer_history flush 任务已启动（每 30 秒）");
    }

    // 8.5.3 写入统计任务（每 10 秒输出一次，用于定位 IO 来源）
    {
        let storage = storage.clone();
        tokio::spawn(async move {
            let mut last_stats = storage.write_stats();
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                let cur = storage.write_stats();
                let dht_writes = cur.dht_nodes_writes - last_stats.dht_nodes_writes;
                let dht_rows = cur.dht_nodes_rows - last_stats.dht_nodes_rows;
                let peer_writes = cur.peers_writes - last_stats.peers_writes;
                let peer_rows = cur.peers_rows - last_stats.peers_rows;
                let hist_writes = cur.peer_history_writes - last_stats.peer_history_writes;
                let hist_rows = cur.peer_history_rows - last_stats.peer_history_rows;
                let tracker_writes = cur.trackers_writes - last_stats.trackers_writes;
                let ih_writes = cur.infohashes_writes - last_stats.infohashes_writes;
                let stats_writes = cur.stats_writes - last_stats.stats_writes;
                info!("[io-stats] 10s写入: dht_nodes={}次/{}行, peers={}次/{}行, history={}次/{}行, trackers={}次, infohashes={}次, stats={}次",
                    dht_writes, dht_rows, peer_writes, peer_rows, hist_writes, hist_rows, tracker_writes, ih_writes, stats_writes);
                last_stats = cur;
            }
        });
        info!("[main] 写入统计任务已启动（每 10 秒）");
    }

    // 8.5.2 启动冷热分层管理器（每 5 分钟检查一次温度，清理冷数据）
    {
        use PeerDiscoveryCenter::intelligence::{TierManager, TierConfig};
        let tier_manager = Arc::new(
            TierManager::new(TierConfig::default())
                .with_storage(storage.clone())
                .with_peer_repo(peer_repo.clone() as Arc<dyn PeerDiscoveryCenter::storage::repo_traits::PeerRepository>)
                .with_node_repo(node_repo.clone() as Arc<dyn PeerDiscoveryCenter::storage::repo_traits::NodeRepository>)
        );
        tokio::spawn(async move {
            tier_manager.run().await;
        });
        info!("[main] 冷热分层管理器已启动（每 5 分钟）");
    }

    // 8.6 启动统一评分维护任务（ScoreMaintainer：每10秒增量重算脏节点，每5分钟全量重算兜底）
    {
        use PeerDiscoveryCenter::intelligence::{NodeScorerImpl, PeerScorerImpl, ScoreMaintainer, TrackerScorerImpl};
        let maintainer = Arc::new(
            ScoreMaintainer::new(
                Arc::new(NodeScorerImpl::new()),
                Arc::new(PeerScorerImpl::new()),
                Arc::new(TrackerScorerImpl::new()),
            )
            .with_node_repo(node_repo.clone() as Arc<dyn PeerDiscoveryCenter::storage::repo_traits::NodeRepository>)
            .with_peer_repo(peer_repo.clone() as Arc<dyn PeerDiscoveryCenter::storage::repo_traits::PeerRepository>)
            .with_tracker_repo(tracker_repo.clone() as Arc<dyn PeerDiscoveryCenter::storage::repo_traits::TrackerRepository>),
        );
        maintainer.start();
    }

    // 9. 启动 UDP Tracker 服务（BEP 15）
    if config.super_tracker.enabled {
        let udp_state = app_state.clone();
        tokio::spawn(async move {
            if let Err(e) = DataPlane::serve_udp(udp_state).await {
                warn!("[main] UDP Tracker 服务异常: {}", e);
            }
        });
        let udp_port = config.super_tracker.udp_port.unwrap_or(config.server.port);
        info!(
            "[main] UDP Tracker 已启动: udp://{}:{}",
            config.server.listen, udp_port
        );
    }

    // 11. 启动 HTTP 服务（带优雅关闭）
    info!(
        "[main] HTTP 服务启动: http://{}:{}",
        config.server.listen, config.server.port
    );
    info!(
        "[main]   超级 Tracker: http://{}:{}/announce",
        config.server.listen, config.server.port
    );
    info!(
        "[main]   Scrape:       http://{}:{}/scrape",
        config.server.listen, config.server.port
    );
    info!(
        "[main]   健康检查:     http://{}:{}/health",
        config.server.listen, config.server.port
    );
    info!(
        "[main]   统计:         http://{}:{}/api/v1/stats",
        config.server.listen, config.server.port
    );
    info!(
        "[main]   发现器列表:   http://{}:{}/api/v1/discoverers",
        config.server.listen, config.server.port
    );
    info!(
        "[main]   Peer反馈:     POST http://{}:{}/api/v1/peer-feedback",
        config.server.listen, config.server.port
    );

    // 优雅关闭：等待 Ctrl+C 或 HTTP 服务退出
    let shutdown = async {
        tokio::signal::ctrl_c().await.ok();
        info!("[main] 收到关闭信号，开始优雅关闭...");
    };

    tokio::select! {
        result = DataPlane::serve(app_state.clone()) => {
            if let Err(e) = result {
                warn!("[main] HTTP 服务异常: {}", e);
            }
        }
        _ = shutdown => {
            info!("[main] 正在保存数据...");

            // 全量保存所有 Repo 到 SQLite（统一数据归口）
            if let Some(ref repo) = app_state.node_repo {
                match repo.save_all().await {
                    Ok(_) => info!("[main] NodeRepo 已保存（{} 个节点）", repo.node_count().await),
                    Err(e) => warn!("[main] NodeRepo 保存失败: {}", e),
                }
            }
            if let Some(ref repo) = app_state.tracker_repo {
                match repo.save_all().await {
                    Ok(_) => info!("[main] TrackerRepo 已保存（{} 个 tracker）", repo.count().await),
                    Err(e) => warn!("[main] TrackerRepo 保存失败: {}", e),
                }
            }
            if let Some(ref repo) = app_state.infohash_repo {
                match repo.save_all().await {
                    Ok(_) => info!("[main] InfohashRepo 已保存（{} 个 infohash）", repo.count().await),
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
            info!("[main] 优雅关闭完成");
        }
    }

    Ok(())
}

/// 初始化日志
fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

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
                println!("  -c, --config <FILE>  指定配置文件路径");
                println!("  -h, --help           显示帮助信息");
                println!();
                println!("默认配置:");
                println!("  监听: 0.0.0.0:6880");
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
