//! 连接管理器
//!
//! 负责节点连接的生命周期管理：
//! - 从 NodeTable 读取候选节点
//! - 按优先级（内网优先）选择最佳地址尝试连接
//! - 维护 target_neighbors 个活跃连接
//! - 连接失败/断开时自动重连
//! - 给上层（sync/gossip/relay）提供稳定的连接获取接口

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tracing::{debug, info};

use crate::federation::node_id::NodeId;
use crate::federation::node_table::NodeTable;
use crate::federation::peer_conn::PeerConn;
use crate::federation::session::SessionsHandle;

/// 连接管理器配置
#[derive(Debug, Clone)]
pub struct ConnectionManagerConfig {
    /// 目标邻居数
    pub target_neighbors: usize,
    /// 维护 tick 间隔（秒）
    pub maintain_interval_secs: u64,
    /// 种子节点列表
    pub seed_nodes: Vec<String>,
    /// 默认监听端口（种子节点未指定端口时用）
    pub listen_port: u16,
}

impl Default for ConnectionManagerConfig {
    fn default() -> Self {
        Self {
            target_neighbors: 8,
            maintain_interval_secs: 30,
            seed_nodes: Vec::new(),
            listen_port: 6885,
        }
    }
}

/// 连接管理器
pub struct ConnectionManager {
    /// 节点表（只读，从 Discovery 共享）
    node_table: Arc<NodeTable>,
    /// 会话管理器（负责底层连接）
    sessions: Arc<SessionsHandle>,
    /// 配置
    config: ConnectionManagerConfig,
    /// 本机节点 ID（跳过自己）
    my_node_id: NodeId,
    /// 关闭信号
    shutdown: broadcast::Sender<()>,
}

impl ConnectionManager {
    /// 创建新连接管理器
    pub fn new(
        node_table: Arc<NodeTable>,
        sessions: Arc<SessionsHandle>,
        my_node_id: NodeId,
        config: ConnectionManagerConfig,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            node_table,
            sessions,
            config,
            my_node_id,
            shutdown,
        }
    }

    /// 获取指定节点的连接（如果已连接）
    pub fn get_connection(&self, node_id: &NodeId) -> Option<Arc<PeerConn>> {
        self.sessions.get_connection(node_id)
    }

    /// 获取所有活跃连接
    pub fn all_connections(&self) -> Vec<Arc<PeerConn>> {
        self.sessions.all_connections()
    }

    /// 当前活跃连接数
    pub fn connection_count(&self) -> usize {
        self.sessions.connection_count()
    }

    /// 启动后台连接维护任务
    pub fn spawn_maintainer(self: Arc<Self>) {
        let mut shutdown_rx = self.shutdown.subscribe();
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(Duration::from_secs(self.config.maintain_interval_secs));
            info!(
                "[federation-conn] 连接维护任务已启动（目标 {} 邻居，间隔 {}s）",
                self.config.target_neighbors, self.config.maintain_interval_secs
            );
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = self.maintain_tick().await {
                            debug!("[federation-conn] 维护 tick 出错: {}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation-conn] 收到关闭信号，退出维护任务");
                        break;
                    }
                }
            }
        });
    }

    /// 单次维护 tick：补齐目标邻居数
    pub async fn maintain_tick(&self) -> anyhow::Result<()> {
        // TODO: 实现补链逻辑
        Ok(())
    }
}
