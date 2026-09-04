//! 数据面
//!
//! 数据面负责无状态处理外部请求，包括：
//! - HTTP Tracker 协议（/announce、/scrape）—— 超级 Tracker 形态
//! - REST API（/health、/api/v1/stats、/api/v1/discover）
//!
//! 数据面不做策略决策，所有策略由控制面提供。
//! 数据面可以水平扩展（多实例无状态），状态存储在共享缓存中。

pub mod http_tracker;
pub mod rest_api;
pub mod udp_tracker;

use std::sync::Arc;

use axum::Router;
use parking_lot::RwLock;
use tracing::info;

use crate::cache::PeerCache;
use crate::config::PdcConfig;
use crate::control_plane::ControlPlane;
use crate::data_plane::http_tracker::SuperTrackerState;
use crate::data_plane::udp_tracker::UdpTrackerServer;
use crate::event_bus::EventBus;
use crate::nat::NatManager;

/// 数据面共享状态
#[derive(Clone)]
pub struct AppState {
    /// 控制面
    pub control_plane: ControlPlane,
    /// 超级 Tracker 状态（announce 上来的 peer 存储）
    pub super_tracker: Arc<SuperTrackerState>,
    /// Peer 缓存
    pub cache: Arc<PeerCache>,
    /// 事件总线
    pub event_bus: EventBus,
    /// 配置
    pub config: Arc<RwLock<PdcConfig>>,
    /// NAT 管理器
    pub nat: Arc<NatManager>,
}

/// 数据面
///
/// 构建 axum 路由，启动 HTTP 服务。
pub struct DataPlane;

impl DataPlane {
    /// 构建 axum Router
    pub fn build_router(state: AppState) -> Router {
        Router::new()
            .merge(http_tracker::routes(state.clone()))
            .merge(rest_api::routes(state))
    }

    /// 启动 HTTP 服务
    pub async fn serve(state: AppState) -> anyhow::Result<()> {
        let config = state.config.read().clone();
        let addr = format!("{}:{}", config.server.listen, config.server.port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        info!("[data_plane] HTTP 服务启动，监听 {}", addr);

        let app = Self::build_router(state);
        axum::serve(listener, app).await?;
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

        let server = UdpTrackerServer::new(
            listen_addr,
            state.super_tracker.clone(),
            state.cache.clone(),
            config.super_tracker,
        );

        server.start().await
    }
}
