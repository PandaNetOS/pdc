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
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use PeerDiscoveryCenter::cache::PeerCache;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 初始化日志
    init_logging();

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
    let control_plane = ControlPlane::new(config.clone(), registry.clone(), event_bus.clone());

    // 4. 初始化默认发现器
    control_plane.init_default_discoverers();
    info!("[main] 已注册 {} 个发现器", control_plane.registry().len());

    // 5. 创建缓存
    let cache = Arc::new(PeerCache::new(
        config.cache.max_cached_peers,
        std::time::Duration::from_secs(config.cache.peer_ttl_secs),
    ));

    // 6. 创建超级 Tracker 状态
    let super_tracker = Arc::new(SuperTrackerState::new(config.super_tracker.clone()));

    // 6.5 创建 NAT 管理器并初始化 UPnP 端口映射
    let nat = Arc::new(NatManager::new(
        config.nat.enabled,
        config.nat.lease_duration,
    ));
    let udp_port = config.super_tracker.udp_port.unwrap_or(config.server.port);
    let crawler_port = if config.crawler.enabled {
        config.crawler.listen_port
    } else {
        0
    };
    if let Err(e) = nat.init(config.server.port, udp_port, crawler_port).await {
        warn!("[main] NAT/UPnP 初始化失败: {}", e);
        warn!("[main] 如果处于 NAT 网络后，请手动配置端口转发或启用路由器 UPnP");
    } else if config.nat.enabled {
        let status = nat.status();
        if let Some(ip) = &status.external_ip {
            info!("[main] 公网访问地址: http://{}:{}", ip, config.server.port);
        }
    }

    // 7. 创建 AppState
    let app_state = AppState {
        control_plane: control_plane.clone(),
        super_tracker: super_tracker.clone(),
        cache: cache.clone(),
        event_bus: event_bus.clone(),
        config: Arc::new(RwLock::new(config.clone())),
        nat: nat.clone(),
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
        cache.clone(),
        hc_config,
    ));
    tokio::spawn(async move {
        health_check.run().await;
    });
    info!("[main] 健康检查任务已启动");

    // 9. 启动爬虫引擎（如果启用）
    if config.crawler.enabled {
        let crawler = CrawlerEngine::new(config.crawler.clone(), event_bus.clone());
        if let Err(e) = crawler.start().await {
            warn!("[main] 爬虫引擎启动失败: {}", e);
        } else {
            info!("[main] 爬虫引擎已启动");
        }
    } else {
        info!("[main] 爬虫引擎未启用（config.crawler.enabled = false）");
    }

    // 10. 启动 UDP Tracker 服务（BEP 15）
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

    // 11. 启动 HTTP 服务（阻塞）
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

    DataPlane::serve(app_state).await?;

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
