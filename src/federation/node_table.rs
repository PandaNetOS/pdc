//! 节点表
//!
//! 维护联邦网络中已知的所有节点信息，包括地址、连接状态、RTT 等。

use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rustc_hash::FxHashMap;

use crate::federation::node_id::{NodeAddress, NodeId};

/// 节点连接状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    /// 未连接
    Disconnected,
    /// 连接中
    Connecting,
    /// 已连接
    Connected,
    /// 连接失败
    Failed,
}

/// 节点表条目
#[derive(Debug, Clone)]
pub struct NodeEntry {
    /// 节点地址信息
    pub info: NodeAddress,
    /// 连接状态
    pub status: NodeStatus,
    /// 往返延迟（毫秒）
    pub rtt_ms: Option<u32>,
    /// 当前连接数
    pub connection_count: u32,
    /// 连续失败次数
    pub consecutive_failures: u32,
    /// 是否为中继节点
    pub is_relay: bool,
    /// 首次发现时间
    pub first_seen: Instant,
}

impl NodeEntry {
    /// 创建新条目
    pub fn new(info: NodeAddress) -> Self {
        Self {
            info,
            status: NodeStatus::Disconnected,
            rtt_ms: None,
            connection_count: 0,
            consecutive_failures: 0,
            is_relay: false,
            first_seen: Instant::now(),
        }
    }

    /// 活跃度评分（用于选择最活跃节点）
    pub fn activity_score(&self) -> f64 {
        let mut score = 0.0;
        match self.status {
            NodeStatus::Connected => score += 100.0,
            NodeStatus::Connecting => score += 30.0,
            NodeStatus::Disconnected => score += 10.0,
            NodeStatus::Failed => score += 0.0,
        }
        if let Some(rtt) = self.rtt_ms {
            // RTT 越低分越高，100ms 内满分
            score += (100.0 - rtt.min(1000) as f64 / 10.0).max(0.0);
        }
        score -= self.consecutive_failures as f64 * 5.0;
        score
    }
}

/// 节点表
pub struct NodeTable {
    /// 节点映射（node_id -> entry）
    nodes: RwLock<FxHashMap<NodeId, NodeEntry>>,
    /// 最大节点数
    max_nodes: usize,
}

impl NodeTable {
    /// 创建新节点表
    pub fn new(max_nodes: usize) -> Self {
        Self {
            nodes: RwLock::new(FxHashMap::default()),
            max_nodes,
        }
    }

    /// 添加或更新节点
    ///
    /// 如果节点已存在，更新地址信息和 last_seen；如果不存在且未满则添加。
    /// 返回 true 表示新增，false 表示更新。
    pub fn add_or_update(&self, mut info: NodeAddress) -> bool {
        let node_id = NodeId(info.node_id);
        let mut nodes = self.nodes.write();

        if let Some(entry) = nodes.get_mut(&node_id) {
            // 多地址聚合（2026-09-27）：不再直接覆盖 ipv4_addr，
            // 而是把新来源的地址并入 endpoints，局域网/公网地址同时保留。
            entry.info.merge(info);
            false
        } else {
            // 新节点：先把单地址字段并入 endpoints
            info.normalize_endpoints(pnos_net::types::DiscoverySource::PeerCache);
            // 新节点
            if nodes.len() >= self.max_nodes {
                // 已满，移除活跃度最低的节点
                if let Some((&worst_id, _)) = nodes.iter().min_by(|a, b| {
                    a.1.activity_score()
                        .partial_cmp(&b.1.activity_score())
                        .unwrap_or(std::cmp::Ordering::Equal)
                }) {
                    nodes.remove(&worst_id);
                }
            }
            let entry = NodeEntry::new(info);
            nodes.insert(node_id, entry);
            true
        }
    }

    /// 获取节点条目
    pub fn get(&self, node_id: &NodeId) -> Option<NodeEntry> {
        self.nodes.read().get(node_id).cloned()
    }

    /// 移除节点
    pub fn remove(&self, node_id: &NodeId) -> bool {
        self.nodes.write().remove(node_id).is_some()
    }

    /// 获取所有节点快照
    pub fn all_nodes(&self) -> Vec<NodeEntry> {
        self.nodes.read().values().cloned().collect()
    }

    /// 获取所有已连接节点
    pub fn connected_nodes(&self) -> Vec<NodeEntry> {
        self.nodes
            .read()
            .values()
            .filter(|e| e.status == NodeStatus::Connected)
            .cloned()
            .collect()
    }

    /// 随机选择 n 个邻居节点
    ///
    /// 优先选择已连接节点，不足时从所有节点中随机补充。
    pub fn random_neighbors(&self, n: usize) -> Vec<NodeEntry> {
        use rand::seq::SliceRandom;

        let nodes = self.nodes.read();
        let mut connected: Vec<&NodeEntry> = nodes
            .values()
            .filter(|e| e.status == NodeStatus::Connected)
            .collect();
        connected.shuffle(&mut rand::thread_rng());

        let mut result: Vec<NodeEntry> = connected.into_iter().take(n).cloned().collect();

        if result.len() < n {
            let mut all: Vec<&NodeEntry> = nodes.values().collect();
            all.shuffle(&mut rand::thread_rng());
            for entry in all {
                if result.len() >= n {
                    break;
                }
                if !result
                    .iter()
                    .any(|r| NodeId(r.info.node_id) == NodeId(entry.info.node_id))
                {
                    result.push(entry.clone());
                }
            }
        }

        result
    }

    /// 获取最活跃的 n 个节点（按活跃度评分降序）
    pub fn top_active_nodes(&self, n: usize) -> Vec<NodeEntry> {
        let mut all: Vec<NodeEntry> = self.nodes.read().values().cloned().collect();
        all.sort_by(|a, b| {
            b.activity_score()
                .partial_cmp(&a.activity_score())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(n);
        all
    }

    /// 标记节点为已连接
    pub fn mark_connected(&self, node_id: &NodeId, rtt_ms: Option<u32>) {
        let mut nodes = self.nodes.write();
        let target_addrs: Vec<std::net::SocketAddr> = nodes
            .get(node_id)
            .map(|e| {
                let mut v = Vec::new();
                if let Some(a) = e.info.ipv4_addr {
                    v.push(a);
                }
                if let Some(a) = e.info.ipv6_addr {
                    v.push(a);
                }
                v
            })
            .unwrap_or_default();
        if let Some(entry) = nodes.get_mut(node_id) {
            entry.status = NodeStatus::Connected;
            entry.connection_count += 1;
            entry.consecutive_failures = 0;
            entry.rtt_ms = rtt_ms;
        }
        // 批次I(#8)：同地址旧身份清退（v9 遗留 #3「同一地址多身份」）——
        // 拨号占位/历史身份条目滞留 Disconnected，会让 /federation/nodes
        // 在会话已建立后仍显示离线。只清退从未真正连接过的条目。
        if !target_addrs.is_empty() {
            let stale: Vec<NodeId> = nodes
                .iter()
                .filter(|(id, e)| {
                    id != &node_id
                        && e.connection_count == 0
                        && (e
                            .info
                            .ipv4_addr
                            .map(|a| target_addrs.contains(&a))
                            .unwrap_or(false)
                            || e.info
                                .ipv6_addr
                                .map(|a| target_addrs.contains(&a))
                                .unwrap_or(false))
                })
                .map(|(id, _)| *id)
                .collect();
            for id in stale {
                nodes.remove(&id);
            }
        }
    }

    /// 标记节点连接失败
    pub fn mark_failed(&self, node_id: &NodeId) {
        if let Some(entry) = self.nodes.write().get_mut(node_id) {
            entry.status = NodeStatus::Failed;
            entry.consecutive_failures += 1;
        }
    }

    /// 标记节点为断开
    pub fn mark_disconnected(&self, node_id: &NodeId) {
        if let Some(entry) = self.nodes.write().get_mut(node_id) {
            entry.status = NodeStatus::Disconnected;
        }
    }

    /// 标记节点为连接中
    pub fn mark_connecting(&self, node_id: &NodeId) {
        if let Some(entry) = self.nodes.write().get_mut(node_id) {
            entry.status = NodeStatus::Connecting;
        }
    }

    /// 重置所有处于 Connecting 状态的节点为 Disconnected（防止连接状态卡住）
    pub fn reset_stale_connecting(&self) -> usize {
        let mut nodes = self.nodes.write();
        let mut count = 0;
        for entry in nodes.values_mut() {
            if entry.status == NodeStatus::Connecting {
                entry.status = NodeStatus::Disconnected;
                count += 1;
            }
        }
        count
    }

    /// 更新节点 RTT
    pub fn update_rtt(&self, node_id: &NodeId, rtt_ms: u32) {
        if let Some(entry) = self.nodes.write().get_mut(node_id) {
            entry.rtt_ms = Some(rtt_ms);
        }
    }

    /// 清理过期节点（超过 timeout 未活跃且未连接的）
    pub fn cleanup_expired(&self, timeout: Duration) -> usize {
        let now = Instant::now();
        let mut nodes = self.nodes.write();
        let before = nodes.len();
        nodes.retain(|_, entry| {
            // 已连接节点不清理
            if entry.status == NodeStatus::Connected {
                return true;
            }
            // 未连接且超过 timeout 未活跃的节点清理掉
            now.duration_since(entry.first_seen) < timeout
        });
        before - nodes.len()
    }

    /// 清理「从未成功握手」的占位 / 垃圾节点。
    ///
    /// 握手成功后 SDK 以**真实 node_id** 注册并 `mark_connected`（`connection_count≥1`）；
    /// 拨号用的占位 temp_id 条目不会被 `mark_connected`（`connection_count==0`）。
    /// 本方法清理存在超过 `grace`、仍 `connection_count==0` 且未在连接中的条目，
    /// 消除经 PEX 交叉传播的 temp_id 垃圾（反复连自己 / 连旧节点、重复拨号）。
    /// 曾连上过的真实节点（断开后 `connection_count` 仍 ≥1）不受影响。
    pub fn prune_never_connected(&self, grace: Duration) -> usize {
        let now = Instant::now();
        let mut nodes = self.nodes.write();
        let before = nodes.len();
        nodes.retain(|_, entry| {
            if matches!(entry.status, NodeStatus::Connected | NodeStatus::Connecting) {
                return true;
            }
            // 曾成功连过的真实节点保留
            if entry.connection_count > 0 {
                return true;
            }
            // 从未连过但仍在宽限期内，保留（给握手 / 重试时间）
            now.duration_since(entry.first_seen) < grace
        });
        before - nodes.len()
    }

    /// 节点总数
    pub fn len(&self) -> usize {
        self.nodes.read().len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.nodes.read().is_empty()
    }

    /// 已连接节点数
    pub fn connected_count(&self) -> usize {
        self.nodes
            .read()
            .values()
            .filter(|e| e.status == NodeStatus::Connected)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::node_id::Reachability;

    fn make_node(id: u8, last_seen: u64) -> NodeAddress {
        NodeAddress {
            node_id: [id; 20],
            ipv4_addr: Some(format!("127.0.0.{}:6885", id).parse().unwrap()),
            ipv6_addr: None,
            reachability: Reachability::Mapped,
            last_seen,
            nat_type: None,
            endpoints: vec![],
        }
    }

    #[test]
    fn test_add_and_get() {
        let table = NodeTable::new(100);
        let info = make_node(1, 100);
        assert!(table.add_or_update(info.clone())); // 新增
        assert!(!table.add_or_update(info)); // 更新

        let entry = table.get(&NodeId([1; 20])).unwrap();
        assert_eq!(
            entry.info.ipv4_addr,
            Some("127.0.0.1:6885".parse().unwrap())
        );
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn test_remove() {
        let table = NodeTable::new(100);
        table.add_or_update(make_node(1, 100));
        assert!(table.remove(&NodeId([1; 20])));
        assert!(!table.remove(&NodeId([1; 20])));
        assert!(table.is_empty());
    }

    #[test]
    fn test_mark_connected_failed() {
        let table = NodeTable::new(100);
        table.add_or_update(make_node(1, 100));

        table.mark_connecting(&NodeId([1; 20]));
        assert_eq!(
            table.get(&NodeId([1; 20])).unwrap().status,
            NodeStatus::Connecting
        );

        table.mark_connected(&NodeId([1; 20]), Some(50));
        let entry = table.get(&NodeId([1; 20])).unwrap();
        assert_eq!(entry.status, NodeStatus::Connected);
        assert_eq!(entry.rtt_ms, Some(50));
        assert_eq!(entry.connection_count, 1);
        assert_eq!(entry.consecutive_failures, 0);

        table.mark_failed(&NodeId([1; 20]));
        let entry = table.get(&NodeId([1; 20])).unwrap();
        assert_eq!(entry.status, NodeStatus::Failed);
        assert_eq!(entry.consecutive_failures, 1);
    }

    #[test]
    fn test_connected_nodes() {
        let table = NodeTable::new(100);
        for i in 1..=5 {
            table.add_or_update(make_node(i, 100));
        }
        table.mark_connected(&NodeId([1; 20]), None);
        table.mark_connected(&NodeId([3; 20]), None);

        assert_eq!(table.connected_count(), 2);
        assert_eq!(table.connected_nodes().len(), 2);
    }

    #[test]
    fn test_random_neighbors() {
        let table = NodeTable::new(100);
        for i in 1..=10 {
            table.add_or_update(make_node(i, 100));
        }
        table.mark_connected(&NodeId([1; 20]), None);
        table.mark_connected(&NodeId([2; 20]), None);

        let neighbors = table.random_neighbors(5);
        assert_eq!(neighbors.len(), 5);
        // 应该包含已连接节点
        assert!(neighbors
            .iter()
            .any(|n| NodeId(n.info.node_id) == NodeId([1; 20])));
    }

    #[test]
    fn test_top_active_nodes() {
        let table = NodeTable::new(100);
        for i in 1..=5 {
            table.add_or_update(make_node(i, 100));
        }
        table.mark_connected(&NodeId([3; 20]), Some(10));
        table.mark_connected(&NodeId([5; 20]), Some(100));

        let top = table.top_active_nodes(2);
        assert_eq!(top.len(), 2);
        // RTT 低的应该排前面
        assert_eq!(NodeId(top[0].info.node_id), NodeId([3; 20]));
    }

    #[test]
    fn test_max_nodes_eviction() {
        let table = NodeTable::new(3);
        for i in 1..=3 {
            table.add_or_update(make_node(i, 100));
        }
        assert_eq!(table.len(), 3);

        // 标记一个为已连接（高活跃）
        table.mark_connected(&NodeId([1; 20]), None);

        // 添加第4个，应该淘汰最低活跃的
        table.add_or_update(make_node(4, 100));
        assert_eq!(table.len(), 3);
        // 已连接的节点应该保留
        assert!(table.get(&NodeId([1; 20])).is_some());
    }

    #[test]
    fn test_cleanup_expired() {
        let table = NodeTable::new(100);
        table.add_or_update(make_node(1, 100));
        table.add_or_update(make_node(2, 100));
        table.mark_connected(&NodeId([1; 20]), None);

        // timeout=0 表示所有未连接节点都视为过期
        let removed = table.cleanup_expired(Duration::from_secs(0));
        assert_eq!(removed, 1); // 节点2被清理
        assert!(table.get(&NodeId([1; 20])).is_some());
        assert!(table.get(&NodeId([2; 20])).is_none());
    }

    #[test]
    fn test_activity_score() {
        let mut entry = NodeEntry::new(make_node(1, 100));
        let base = entry.activity_score();

        entry.status = NodeStatus::Connected;
        assert!(entry.activity_score() > base);

        entry.rtt_ms = Some(10);
        let with_rtt = entry.activity_score();
        assert!(with_rtt > base);

        entry.consecutive_failures = 5;
        assert!(entry.activity_score() < with_rtt);
    }
}
