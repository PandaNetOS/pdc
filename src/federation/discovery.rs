//! 节点发现服务
//!
//! 负责种子节点引导、节点列表交换（PEX）和新节点发现。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::federation::config::FederationConfig;
use crate::federation::connection::{Connection, ConnectionManager};
use crate::federation::node_id::{NodeAddress, NodeIdentity, NodeId, Reachability};
use crate::federation::node_table::NodeTable;
use crate::federation::protocol::*;

/// 节点发现服务
pub struct DiscoveryService {
    /// 连接管理器
    connection_manager: Arc<ConnectionManager>,
    /// 节点表
    node_table: Arc<NodeTable>,
    /// 节点身份
    identity: Arc<NodeIdentity>,
    /// 配置
    config: FederationConfig,
    /// 关闭信号
    shutdown: broadcast::Sender<()>,
}

impl DiscoveryService {
    /// 创建发现服务
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        node_table: Arc<NodeTable>,
        identity: Arc<NodeIdentity>,
        config: FederationConfig,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            connection_manager,
            node_table,
            identity,
            config,
            shutdown,
        }
    }

    /// 引导连接：解析种子节点并连接
    pub async fn bootstrap(self: Arc<Self>) {
        if self.config.seed_nodes.is_empty() {
            info!("[federation] 未配置种子节点，跳过引导");
            return;
        }

        info!(
            "[federation] 开始引导，共 {} 个种子节点",
            self.config.seed_nodes.len()
        );

        for seed in &self.config.seed_nodes {
            let self_clone = self.clone();
            let seed = seed.clone();
            tokio::spawn(async move {
                if let Err(e) = self_clone.connect_seed(&seed).await {
                    warn!("[federation] 种子节点 {} 连接失败: {}", seed, e);
                }
            });
        }
    }

    /// 连接单个种子节点
    async fn connect_seed(self: Arc<Self>, seed: &str) -> anyhow::Result<()> {
        // 解析 DNS
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host(seed)
            .await
            .map_err(|e| anyhow::anyhow!("DNS 解析失败 {}: {}", seed, e))?
            .collect();

        if addrs.is_empty() {
            anyhow::bail!("DNS 解析无结果: {}", seed);
        }

        // 尝试每个地址
        for addr in addrs {
            // 种子节点的 node_id 未知，先用随机 ID 占位，握手后会更新
            let temp_id = NodeId::random();
            self.node_table.add_or_update(NodeAddress {
                node_id: temp_id.0,
                ipv4_addr: if addr.is_ipv4() { Some(addr) } else { None },
                ipv6_addr: if addr.is_ipv6() { Some(addr) } else { None },
                reachability: Reachability::Unknown,
                last_seen: 0,
                nat_type: None,
            });

            match self.connection_manager.clone().connect_to(temp_id, addr).await {
                Ok(conn) => {
                    info!("[federation] 种子节点连接成功: {} ({})", conn.node_id, addr);
                    // 连接成功后发送 GetNodes
                    let req = GetNodesMessage { count: 32 };
                    if let Err(e) = conn.send_message(MessageType::GetNodes, &req).await {
                        warn!("[federation] 发送 GetNodes 失败: {}", e);
                    }
                    return Ok(());
                }
                Err(e) => {
                    debug!("[federation] 种子节点地址 {} 连接失败: {}", addr, e);
                }
            }
        }

        anyhow::bail!("所有地址均连接失败: {}", seed)
    }

    /// 处理 GetNodes 请求：返回本地最活跃的节点
    pub async fn handle_get_nodes(&self, conn: &Connection, count: u16) {
        let nodes = self.node_table.top_active_nodes(count as usize);
        let addresses: Vec<NodeAddress> = nodes
            .into_iter()
            .filter(|n| NodeId(n.info.node_id) != conn.node_id) // 不返回请求方自己
            .map(|n| n.info)
            .collect();

        let msg = NodesMessage { nodes: addresses };
        if let Err(e) = conn.send_message(MessageType::Nodes, &msg).await {
            debug!("[federation] 发送 Nodes 失败: {}", e);
        }
    }

    /// 处理收到的节点列表
    pub fn handle_nodes_received(&self, nodes: Vec<NodeAddress>) {
        self.process_new_nodes(nodes);
    }

    /// 处理 ExchangeNodes 消息
    pub fn handle_exchange_nodes(&self, nodes: Vec<NodeAddress>) {
        self.process_new_nodes(nodes);
    }

    /// 处理新发现的节点：加入节点表，尝试连接未连接的
    fn process_new_nodes(&self, nodes: Vec<NodeAddress>) {
        let mut new_count = 0;
        for node_info in &nodes {
            // 跳过自己
            if NodeId(node_info.node_id) == self.identity.node_id {
                continue;
            }
            if self.node_table.add_or_update(node_info.clone()) {
                new_count += 1;
            }
        }

        if new_count > 0 {
            debug!("[federation] 发现 {} 个新节点，节点表总数: {}", new_count, self.node_table.len());
        }

        // 尝试连接未连接的节点（不超过 target_neighbors）
        let connected = self.node_table.connected_count();
        if connected < self.config.target_neighbors {
            let need = self.config.target_neighbors - connected;
            let candidates = self.node_table
                .all_nodes()
                .into_iter()
                .filter(|e| {
                    e.status != crate::federation::node_table::NodeStatus::Connected
                        && e.info.preferred_addr().is_some()
                })
                .take(need)
                .collect::<Vec<_>>();

            for entry in candidates {
                if let Some(addr) = entry.info.preferred_addr() {
                    let cm = self.connection_manager.clone();
                    let node_id = NodeId(entry.info.node_id);
                    tokio::spawn(async move {
                        if let Err(e) = cm.connect_to(node_id, addr).await {
                            debug!("[federation] 自动连接 {} 失败: {}", node_id, e);
                        }
                    });
                }
            }
        }
    }

    /// 启动 PEX 交换后台任务（每5分钟向所有连接发 ExchangeNodes）
    pub fn spawn_pex_exchange(self: Arc<Self>) {
        let mut shutdown_rx = self.shutdown.subscribe();
        let interval = Duration::from_secs(300); // 5 分钟

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // 跳过第一次立即触发
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self.clone().pex_exchange_tick().await;
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] PEX 交换任务收到关闭信号");
                        break;
                    }
                }
            }
        });
        info!("[federation] PEX 交换任务已启动（间隔 300s）");
    }

    /// PEX 交换单次执行
    async fn pex_exchange_tick(self: Arc<Self>) {
        let conns = self.connection_manager.all_connections();
        if conns.is_empty() {
            return;
        }

        // 获取本地已知节点（排除自己和已连接的对端）
        let known_nodes: Vec<NodeAddress> = self
            .node_table
            .top_active_nodes(32)
            .into_iter()
            .map(|e| e.info)
            .collect();

        if known_nodes.is_empty() {
            return;
        }

        let conn_count = conns.len();
        for conn in &conns {
            // 过滤掉对端自己
            let nodes: Vec<NodeAddress> = known_nodes
                .iter()
                .filter(|n| NodeId(n.node_id) != conn.node_id)
                .cloned()
                .collect();

            if nodes.is_empty() {
                continue;
            }

            let msg = ExchangeNodesMessage { nodes };
            if let Err(e) = conn.send_message(MessageType::ExchangeNodes, &msg).await {
                debug!("[federation] PEX 交换发送失败 to {}: {}", conn.node_id, e);
            }
        }

        debug!("[federation] PEX 交换完成，向 {} 个连接发送了节点信息", conn_count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::node_table::NodeTable;

    fn make_test_config() -> FederationConfig {
        FederationConfig {
            enabled: true,
            listen_port: 0,
            max_connections: 10,
            target_neighbors: 4,
            seed_nodes: vec![],
            ..Default::default()
        }
    }

    fn make_node_address(id: u8, port: u16) -> NodeAddress {
        NodeAddress {
            node_id: [id; 20],
            ipv4_addr: Some(format!("127.0.0.1:{}", port).parse().unwrap()),
            ipv6_addr: None,
            reachability: Reachability::Mapped,
            last_seen: 100,
            nat_type: None,
        }
    }

    #[tokio::test]
    async fn test_handle_nodes_received() {
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cm_shutdown, _) = broadcast::channel(1);
        let cm = Arc::new(ConnectionManager::new(
            node_table.clone(),
            identity.clone(),
            make_test_config(),
            cm_shutdown,
        ));
        let discovery = DiscoveryService::new(cm, node_table.clone(), identity, make_test_config(), shutdown_tx);

        let nodes = vec![
            make_node_address(1, 6885),
            make_node_address(2, 6886),
            make_node_address(3, 6887),
        ];
        discovery.handle_nodes_received(nodes);
        assert_eq!(node_table.len(), 3);

        // 重复添加不应增加
        discovery.handle_nodes_received(vec![make_node_address(1, 6885)]);
        assert_eq!(node_table.len(), 3);
    }

    #[tokio::test]
    async fn test_handle_exchange_nodes() {
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cm_shutdown, _) = broadcast::channel(1);
        let cm = Arc::new(ConnectionManager::new(
            node_table.clone(),
            identity.clone(),
            make_test_config(),
            cm_shutdown,
        ));
        let discovery = DiscoveryService::new(cm, node_table.clone(), identity, make_test_config(), shutdown_tx);

        let nodes = vec![make_node_address(5, 6890), make_node_address(6, 6891)];
        discovery.handle_exchange_nodes(nodes);
        assert_eq!(node_table.len(), 2);
    }

    #[tokio::test]
    async fn test_skip_self_in_nodes() {
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cm_shutdown, _) = broadcast::channel(1);
        let cm = Arc::new(ConnectionManager::new(
            node_table.clone(),
            identity.clone(),
            make_test_config(),
            cm_shutdown,
        ));
        let discovery = DiscoveryService::new(cm, node_table.clone(), identity.clone(), make_test_config(), shutdown_tx);

        // 包含自己的节点
        let mut self_addr = make_node_address(0, 6885);
        self_addr.node_id = identity.node_id.0;
        let nodes = vec![self_addr, make_node_address(1, 6886)];
        discovery.handle_nodes_received(nodes);
        // 自己不应被加入
        assert_eq!(node_table.len(), 1);
        assert!(node_table.get(&identity.node_id).is_none());
    }

    #[tokio::test]
    async fn test_bootstrap_no_seeds() {
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cm_shutdown, _) = broadcast::channel(1);
        let cm = Arc::new(ConnectionManager::new(
            node_table.clone(),
            identity.clone(),
            make_test_config(),
            cm_shutdown,
        ));
        let discovery = Arc::new(DiscoveryService::new(cm, node_table, identity, make_test_config(), shutdown_tx));

        // 没有种子节点，应该立即返回
        discovery.bootstrap().await;
    }
}
