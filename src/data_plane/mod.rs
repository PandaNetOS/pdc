//! 数据面
//!
//! 数据面负责无状态处理外部请求，包括：
//! - HTTP Tracker 协议（/announce、/scrape）—— 超级 Tracker 形态
//! - REST API（/health、/api/v1/stats、/api/v1/discover）
//!
//! 数据面不做策略决策，所有策略由控制面提供。
//! 数据面可以水平扩展（多实例无状态），状态存储在共享缓存中。

pub mod http_tracker;
pub mod metrics;
pub mod rest_api;
pub mod udp_tracker;
pub mod rate_limiter;
pub mod relay;
pub mod hole_punch_signaling;
pub mod ws;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::PdcConfig;
use crate::control_plane::ControlPlane;
use crate::crawler::CrawlerState;
use crate::data_plane::http_tracker::SuperTrackerState;
use crate::data_plane::udp_tracker::UdpTrackerServer;
use crate::event_bus::EventBus;
use crate::nat::NatManager;
use crate::storage::PeerRepoImpl;

/// 数据面共享状态
#[derive(Clone)]
pub struct AppState {
    /// 控制面
    pub control_plane: ControlPlane,
    /// 超级 Tracker 状态（announce 上来的 peer 存储）
    pub super_tracker: Arc<SuperTrackerState>,
    /// Peer 仓库（统一归口 PeerRepoImpl）
    pub peer_repo: Arc<PeerRepoImpl>,
    /// 事件总线
    pub event_bus: EventBus,
    /// 配置
    pub config: Arc<RwLock<PdcConfig>>,
    /// NAT 管理器
    pub nat: Arc<NatManager>,
    /// 爬虫状态（可选，未启用爬虫时为 None）
    pub crawler_state: Option<Arc<RwLock<CrawlerState>>>,
    /// 爬虫路由表（可选）
    pub crawler_routing_table: Option<Arc<RwLock<crate::dht::routing_table::RoutingTable>>>,
    /// DHT 探测发送端（可选，用于把 tracker peer 加入探测队列）
    pub probe_sender: Option<mpsc::UnboundedSender<SocketAddr>>,
    /// 持久化存储
    pub storage: Arc<crate::storage::Storage>,
    /// 数据层 Repo（统一归口）
    pub node_repo: Option<Arc<crate::storage::NodeRepoImpl>>,
    pub infohash_repo: Option<Arc<crate::storage::InfohashRepoImpl>>,
    pub tracker_repo: Option<Arc<crate::storage::TrackerRepoImpl>>,
    /// TrackerPeerFetcher 统计（可选）
    pub fetcher: Option<Arc<crate::services::TrackerPeerFetcher>>,
    /// DHT 探测器统计（可选）
    pub dht_probe: Option<Arc<crate::dht::DhtProbe>>,
    /// 限流器（P2优化：QPS监控+单IP限流+异常封禁）
    pub rate_limiter: Arc<crate::data_plane::rate_limiter::RateLimiter>,
    /// UDP打洞信令服务器（协调NAT后节点交换公网地址）
    pub hole_punch_signaling: Arc<crate::data_plane::hole_punch_signaling::HolePunchSignaling>,
    /// uTP 服务端（BEP 29，接收 BT 客户端主动连接）
    pub utp_server: Option<Arc<crate::crawler::utp_server::UtpServer>>,
    /// PEX 接收器（BEP 11，被动接收 peer 交换消息）
    pub pex_receiver: Option<Arc<crate::crawler::pex_receiver::PexReceiver>>,
    /// TCP PEX 服务端（BEP 11，标准 TCP BT 连接被动接收 PEX）
    pub tcp_pex_server: Option<Arc<crate::crawler::tcp_pex_server::TcpPexServer>>,
    /// 主动 PEX 请求器（BEP 11，主动连接 peer 请求 PEX 消息）
    pub active_pex: Option<Arc<crate::crawler::active_pex::ActivePexRequester>>,
    /// 联邦网络服务（可选）
    pub federation: Option<Arc<crate::federation::FederationService>>,
    /// 中继服务器（UDP+TCP 流量转发，打洞失败时使用）
    pub relay_server: Option<Arc<crate::data_plane::relay::RelayServer>>,
}

/// 数据面
///
/// 构建 axum 路由，启动 HTTP 服务。
pub struct DataPlane;

impl DataPlane {
    /// 构建超级 Tracker 路由（/announce /scrape，公网公开，无需鉴权）
    pub fn build_tracker_router(state: AppState) -> Router {
        http_tracker::routes(state)
    }

    /// 构建 API/监控路由（/api/v1/* /ws /metrics，需 token 鉴权）
    pub fn build_api_router(state: AppState) -> Router {
        use axum::http::{Request, StatusCode};
        use axum::middleware::{from_fn, Next};
        use axum::response::Response;

        let token = state.config.read().server.token.clone();

        let app = rest_api::routes(state);

        // 配置了 token 时加鉴权中间件
        if let Some(expected_token) = token {
            if !expected_token.is_empty() {
                let expected = expected_token.clone();
                return app.layer(from_fn(move |req: Request<axum::body::Body>, next: Next| {
                    let expected = expected.clone();
                    async move {
                        let auth_valid = req.headers()
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .map(|v| v.trim_start_matches("Bearer ").trim() == expected)
                            .unwrap_or(false)
                            || req.headers()
                                .get("x-api-key")
                                .and_then(|v| v.to_str().ok())
                                .map(|v| v == expected)
                                .unwrap_or(false);

                        if auth_valid {
                            Ok(next.run(req).await)
                        } else {
                            Err((StatusCode::UNAUTHORIZED, "Unauthorized: invalid or missing token"))
                        }
                    }
                }));
            }
        }
        app
    }

    /// 启动超级 Tracker HTTP 服务（6880，公网）
    pub async fn serve_tracker(state: AppState) -> anyhow::Result<()> {
        let config = state.config.read().clone();
        let addr = format!("{}:{}", config.server.listen, config.server.port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        info!("[data_plane] 超级 Tracker HTTP 启动，监听 {}", addr);

        let app = Self::build_tracker_router(state);
        axum::serve(listener, app).await?;
        Ok(())
    }

    /// 启动 API/监控 HTTP 服务（6886，局域网+token鉴权，不映射公网）
    pub async fn serve_api(state: AppState) -> anyhow::Result<()> {
        let config = state.config.read().clone();
        let addr = format!("{}:{}", config.server.listen, config.server.api_port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        let has_token = config.server.token.as_ref().map(|t| !t.is_empty()).unwrap_or(false);
        info!("[data_plane] API/监控 HTTP 启动，监听 {}（{}）", addr, if has_token { "token鉴权已启用" } else { "未配置token，无鉴权" });

        let app = Self::build_api_router(state);
        axum::serve(listener, app).await?;
        Ok(())
    }

    /// 启动全部 HTTP 服务（超级 Tracker + API/监控）
    pub async fn serve(state: AppState) -> anyhow::Result<()> {
        let tracker_state = state.clone();
        let api_state = state.clone();

        let tracker_handle = tokio::spawn(async move {
            if let Err(e) = Self::serve_tracker(tracker_state).await {
                warn!("[data_plane] 超级 Tracker HTTP 异常退出: {}", e);
            }
        });
        let api_handle = tokio::spawn(async move {
            if let Err(e) = Self::serve_api(api_state).await {
                warn!("[data_plane] API/监控 HTTP 异常退出: {}", e);
            }
        });

        // 等待任一退出
        tokio::select! {
            _ = tracker_handle => {},
            _ = api_handle => {},
        }
        Ok(())
    }

    /// 启动 UDP Tracker 服务（BEP 15）
    ///
    /// 与 HTTP 服务共用同一个端口（UDP/TCP 可以同端口），
    /// 或使用配置中指定的 UDP 端口。
    pub async fn serve_udp(state: AppState) -> anyhow::Result<()> {
        let config = state.config.read().clone();
        let udp_port = config.super_tracker.udp_port.unwrap_or(config.server.port);
        let listen_addr = format!("{}:{}", config.server.listen, udp_port).parse()?;

        let mut server = UdpTrackerServer::new(
            listen_addr,
            state.super_tracker.clone(),
            state.peer_repo.clone(),
            config.super_tracker,
            state.rate_limiter.clone(),
        );
        if let Some(ref repo) = state.infohash_repo {
            server = server.with_infohash_repo(repo.clone());
        }

        server.start().await
    }
}
