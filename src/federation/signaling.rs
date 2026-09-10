//! 打洞信令中继
//!
//! 通过已连接的联邦节点转发打洞信令，协调双方同时进行 UDP 打洞。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::federation::connection::ConnectionManager;
use crate::federation::metrics::FederationMetrics;
use crate::federation::node_id::{NodeIdentity, NodeId};
use crate::federation::node_table::NodeTable;
use crate::federation::protocol::*;
use crate::federation::transport::UdpTransport;

/// 信令动作
pub mod signaling_action {
    pub const REQUEST: u8 = 0;
    pub const RESPONSE: u8 = 1;
    pub const PUNCH: u8 = 2;
    pub const SUCCESS: u8 = 3;
    pub const FAILED: u8 = 4;
}

/// 打洞会话状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignalingState {
    Initiating,
    Waiting,
    Punching,
    Connected,
    Failed,
}

/// 打洞会话
struct SignalingSession {
    session_id: u64,
    target_node_id: NodeId,
    target_addr: Option<SocketAddr>,
    relay_node_id: Option<NodeId>,
    created_at: Instant,
    state: SignalingState,
}

/// 打洞信令服务
pub struct SignalingService {
    connection_manager: Arc<ConnectionManager>,
    node_table: Arc<NodeTable>,
    identity: Arc<NodeIdentity>,
    pending_sessions: RwLock<FxHashMap<u64, SignalingSession>>,
    udp_transport: Arc<UdpTransport>,
    metrics: Arc<FederationMetrics>,
    next_session_id: std::sync::atomic::AtomicU64,
    shutdown: broadcast::Sender<()>,
}

impl SignalingService {
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        node_table: Arc<NodeTable>,
        identity: Arc<NodeIdentity>,
        udp_transport: Arc<UdpTransport>,
        metrics: Arc<FederationMetrics>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            connection_manager,
            node_table,
            identity,
            pending_sessions: RwLock::new(FxHashMap::default()),
            udp_transport,
            metrics,
            next_session_id: std::sync::atomic::AtomicU64::new(1),
            shutdown,
        }
    }

    /// 发起打洞
    pub fn initiate_hole_punch(
        &self,
        target_node_id: NodeId,
        relay_node_id: Option<NodeId>,
    ) -> u64 {
        let session_id = self
            .next_session_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.metrics.record_hole_punch_attempt();

        // 获取目标地址
        let target_addr = self
            .node_table
            .get(&target_node_id)
            .and_then(|e| e.info.preferred_addr());

        let session = SignalingSession {
            session_id,
            target_node_id,
            target_addr,
            relay_node_id,
            created_at: Instant::now(),
            state: SignalingState::Initiating,
        };
        self.pending_sessions.write().insert(session_id, session);

        // 获取本地公网地址
        let local_addr = self
            .identity
            .addresses_snapshot()
            .iter()
            .find_map(|a| a.ipv4_addr)
            .or_else(|| {
                self.identity
                    .addresses_snapshot()
                    .iter()
                    .find_map(|a| a.ipv6_addr)
            });

        let nat_type = self
            .identity
            .addresses_snapshot()
            .first()
            .and_then(|a| a.nat_type.clone());

        let msg = SignalingMessage {
            session_id,
            from_node: self.identity.node_id.0,
            to_node: target_node_id.0,
            from_addr: local_addr,
            nat_type,
            action: signaling_action::REQUEST,
        };

        // 通过中继或直接连接发送
        if let Some(relay_id) = relay_node_id {
            self.forward_to(&relay_id, &msg);
        } else if let Some(conn) = self.connection_manager.get_connection(&target_node_id) {
            let cm = self.connection_manager.clone();
            let msg_clone = msg.clone();
            tokio::spawn(async move {
                if let Err(e) = conn.send_message(MessageType::Signaling, &msg_clone).await {
                    debug!("[federation] 信令发送失败: {}", e);
                }
            });
        } else {
            warn!("[federation] 无法到达目标节点 {}，无连接也无中继", target_node_id);
        }

        info!(
            "[federation] 发起打洞 session={}, target={}, relay={:?}",
            session_id, target_node_id, relay_node_id
        );

        session_id
    }

    /// 处理收到的信令消息
    pub fn handle_signaling(&self, from_node: NodeId, msg: SignalingMessage) {
        let target = NodeId(msg.to_node);
        let is_for_me = target == self.identity.node_id;

        if is_for_me {
            self.handle_signaling_for_me(from_node, msg);
        } else {
            // 不是发给自己的，检查是否需要中继转发
            self.maybe_forward(from_node, msg);
        }
    }

    /// 处理发给自己的信令
    fn handle_signaling_for_me(&self, from_node: NodeId, msg: SignalingMessage) {
        match msg.action {
            signaling_action::REQUEST => {
                info!(
                    "[federation] 收到打洞请求 session={}, from={}",
                    msg.session_id, from_node
                );

                // 更新会话状态
                {
                    let mut sessions = self.pending_sessions.write();
                    sessions.insert(
                        msg.session_id,
                        SignalingSession {
                            session_id: msg.session_id,
                            target_node_id: from_node,
                            target_addr: msg.from_addr,
                            relay_node_id: None,
                            created_at: Instant::now(),
                            state: SignalingState::Waiting,
                        },
                    );
                }

                // 回复 Response，携带自己的公网地址
                let local_addr = self
                    .identity
                    .addresses_snapshot()
                    .iter()
                    .find_map(|a| a.ipv4_addr);

                let response = SignalingMessage {
                    session_id: msg.session_id,
                    from_node: self.identity.node_id.0,
                    to_node: from_node.0,
                    from_addr: local_addr,
                    nat_type: self
                        .identity
                        .addresses_snapshot()
                        .first()
                        .and_then(|a| a.nat_type.clone()),
                    action: signaling_action::RESPONSE,
                };

                if let Some(conn) = self.connection_manager.get_connection(&from_node) {
                    let cm = self.connection_manager.clone();
                    tokio::spawn(async move {
                        let _ = conn.send_message(MessageType::Signaling, &response).await;
                    });
                }

                // 同时开始打洞
                if let Some(addr) = msg.from_addr {
                    self.start_hole_punch(msg.session_id, addr);
                }
            }
            signaling_action::RESPONSE => {
                info!(
                    "[federation] 收到打洞响应 session={}, from={}",
                    msg.session_id, from_node
                );
                if let Some(addr) = msg.from_addr {
                    // 更新目标地址并开始打洞
                    if let Some(session) = self.pending_sessions.write().get_mut(&msg.session_id)
                    {
                        session.target_addr = Some(addr);
                        session.state = SignalingState::Punching;
                    }
                    self.start_hole_punch(msg.session_id, addr);
                }
            }
            signaling_action::PUNCH => {
                debug!("[federation] 收到 Punch 信令 session={}", msg.session_id);
                if let Some(addr) = msg.from_addr {
                    self.start_hole_punch(msg.session_id, addr);
                }
            }
            signaling_action::SUCCESS => {
                info!("[federation] 打洞成功 session={}", msg.session_id);
                self.metrics.record_hole_punch_success();
                if let Some(session) = self.pending_sessions.write().get_mut(&msg.session_id)
                {
                    session.state = SignalingState::Connected;
                }
            }
            signaling_action::FAILED => {
                warn!("[federation] 打洞失败 session={}", msg.session_id);
                if let Some(session) = self.pending_sessions.write().get_mut(&msg.session_id)
                {
                    session.state = SignalingState::Failed;
                }
            }
            _ => {}
        }
    }

    /// 尝试中继转发
    fn maybe_forward(&self, from_node: NodeId, msg: SignalingMessage) {
        let target = NodeId(msg.to_node);
        // 如果自己和目标有连接，转发
        if let Some(conn) = self.connection_manager.get_connection(&target) {
            debug!(
                "[federation] 中继转发信令 session={}, {} -> {}",
                msg.session_id, from_node, target
            );
            let cm = self.connection_manager.clone();
            tokio::spawn(async move {
                let _ = conn.send_message(MessageType::Signaling, &msg).await;
            });
        }
    }

    /// 转发到指定节点
    fn forward_to(&self, node_id: &NodeId, msg: &SignalingMessage) {
        if let Some(conn) = self.connection_manager.get_connection(node_id) {
            let cm = self.connection_manager.clone();
            let msg_clone = msg.clone();
            tokio::spawn(async move {
                let _ = conn.send_message(MessageType::Signaling, &msg_clone).await;
            });
        }
    }

    /// 开始 UDP 打洞（持续5秒）
    fn start_hole_punch(&self, session_id: u64, target_addr: SocketAddr) {
        let udp = self.udp_transport.clone();
        let node_id = self.identity.node_id.0;
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            debug!(
                "[federation] 开始 UDP 打洞 session={}, target={}",
                session_id, target_addr
            );
            if let Err(e) = udp
                .hole_punch(target_addr, &node_id, Duration::from_secs(5))
                .await
            {
                debug!("[federation] 打洞异常 session={}: {}", session_id, e);
            }
        });
    }

    /// 清理过期会话（超过5分钟）
    pub fn cleanup_expired(&self) {
        let mut sessions = self.pending_sessions.write();
        sessions.retain(|_, s| s.created_at.elapsed() < Duration::from_secs(300));
    }

    /// 活跃会话数
    pub fn active_sessions(&self) -> usize {
        self.pending_sessions.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::config::FederationConfig;

    fn make_config() -> FederationConfig {
        FederationConfig {
            enabled: true,
            listen_port: 0,
            max_connections: 10,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_signaling_service_creation() {
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cm_shutdown, _) = broadcast::channel(1);
        let cm = Arc::new(ConnectionManager::new(
            node_table.clone(),
            identity.clone(),
            make_config(),
            cm_shutdown,
            Arc::new(FederationMetrics::new()),
        ));
        let udp = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let metrics = Arc::new(FederationMetrics::new());
        let signaling = SignalingService::new(
            cm,
            node_table,
            identity,
            udp,
            metrics,
            shutdown_tx,
        );
        assert_eq!(signaling.active_sessions(), 0);
    }

    #[test]
    fn test_signaling_action_constants() {
        assert_eq!(signaling_action::REQUEST, 0);
        assert_eq!(signaling_action::RESPONSE, 1);
        assert_eq!(signaling_action::PUNCH, 2);
        assert_eq!(signaling_action::SUCCESS, 3);
        assert_eq!(signaling_action::FAILED, 4);
    }

    #[test]
    fn test_signaling_message_serde() {
        let msg = SignalingMessage {
            session_id: 42,
            from_node: [1; 20],
            to_node: [2; 20],
            from_addr: Some("1.2.3.4:6885".parse().unwrap()),
            nat_type: Some("FullCone".to_string()),
            action: signaling_action::REQUEST,
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: SignalingMessage = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.session_id, 42);
        assert_eq!(decoded.action, 0);
        assert_eq!(decoded.from_addr, Some("1.2.3.4:6885".parse().unwrap()));
    }
}
