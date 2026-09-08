//! NodeRepository 实现
//!
//! 独立的 DHT 节点存储（无容量限制），作为爬虫候选池的唯一归口。
//! 路由表只负责 DHT 路由响应，NodeRepo 负责爬虫候选节点的存储和评分。
//! 内存 HashMap + SQLite 持久化双写。

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::dht::kbucket::{KBucketEntry, NodeState};
use crate::intelligence::calculate_node_score;
use crate::storage::db::Storage;
use crate::storage::repo_traits::{NodeId, NodeRepository};

/// 节点统计信息（避免全量克隆）
#[derive(Debug, Clone)]
pub struct NodeStats {
    pub total: usize,
    pub good: usize,
    pub questionable: usize,
    pub bad: usize,
    pub active: usize,
    pub avg_score: f64,
}

pub struct NodeRepoImpl {
    /// 独立节点存储（无容量限制，按 addr 去重）
    nodes: RwLock<HashMap<SocketAddr, KBucketEntry>>,
    /// 脏节点集合（统计数据已变化，需要重算评分）
    dirty: RwLock<HashSet<SocketAddr>>,
    storage: Arc<Storage>,
}

impl NodeRepoImpl {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            dirty: RwLock::new(HashSet::new()),
            storage,
        }
    }

    /// 兼容旧接口：从 crawler 路由表创建（现在忽略路由表，独立存储）
    pub fn from_crawler(_routing_table: Arc<parking_lot::RwLock<crate::dht::routing_table::RoutingTable>>, storage: Arc<Storage>) -> Self {
        Self::new(storage)
    }

    // ── 同步便捷方法（爬虫高频调用，避免 async 开销）──

    pub fn add_node_sync(&self, id: NodeId, addr: SocketAddr) -> bool {
        let mut nodes = self.nodes.write();
        if let Some(existing) = nodes.get_mut(&addr) {
            existing.id = id;
            existing.last_active = Instant::now();
            false
        } else {
            let mut entry = KBucketEntry::new(id, addr);
            // 新节点立即计算初始评分（无查询记录给中性分45）
            entry.score = calculate_node_score(&entry);
            nodes.insert(addr, entry);
            true
        }
    }

    pub fn contains_sync(&self, addr: SocketAddr) -> bool {
        self.nodes.read().contains_key(&addr)
    }

    pub fn len_sync(&self) -> usize {
        self.nodes.read().len()
    }

    /// 节点统计信息（避免全量克隆，用于健康度计算和监控）
    pub fn stats_sync(&self) -> NodeStats {
        let nodes = self.nodes.read();
        let mut good = 0;
        let mut questionable = 0;
        let mut bad = 0;
        let mut active = 0;
        let mut avg_score = 0.0;
        for node in nodes.values() {
            match node.state {
                NodeState::Good => good += 1,
                NodeState::Questionable => questionable += 1,
                NodeState::Bad => bad += 1,
            }
            if node.query_count > 0 {
                active += 1;
            }
            avg_score += node.score;
        }
        let total = nodes.len();
        if total > 0 {
            avg_score /= total as f64;
        }
        NodeStats { total, good, questionable, bad, active, avg_score }
    }

    pub fn top_nodes_sync(&self, n: usize) -> Vec<KBucketEntry> {
        let mut all: Vec<KBucketEntry> = self.nodes.read().values().cloned().collect();
        all.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        all.truncate(n);
        all
    }

    pub fn all_nodes_sync(&self) -> Vec<KBucketEntry> {
        self.nodes.read().values().cloned().collect()
    }

    pub fn record_query_sync(&self, addr: SocketAddr, success: bool, latency_ms: u64) {
        let mut nodes = self.nodes.write();
        if let Some(entry) = nodes.get_mut(&addr) {
            entry.query_count += 1;
            entry.last_query_time = Some(Instant::now());
            if success {
                entry.success_count += 1;
                entry.total_latency_ms += latency_ms;
                entry.last_active = Instant::now();
                entry.state = NodeState::Good;
                entry.consecutive_failures = 0;
            } else {
                entry.consecutive_failures += 1;
                if entry.consecutive_failures >= 3 {
                    entry.state = NodeState::Bad;
                }
            }
            // 标记为脏：统计数据已变化，需要重算评分
            drop(nodes);
            self.dirty.write().insert(addr);
        }
    }

    /// 记录查询成功及返回的节点数（用于节点产出维度评分）
    pub fn record_query_with_nodes_sync(&self, addr: SocketAddr, latency_ms: u64, nodes_returned: u64) {
        let mut nodes = self.nodes.write();
        if let Some(entry) = nodes.get_mut(&addr) {
            entry.query_count += 1;
            entry.success_count += 1;
            entry.total_latency_ms += latency_ms;
            entry.nodes_returned += nodes_returned;
            entry.last_active = Instant::now();
            entry.last_query_time = Some(Instant::now());
            entry.state = NodeState::Good;
            entry.consecutive_failures = 0;
            // 标记为脏：统计数据已变化，需要重算评分
            drop(nodes);
            self.dirty.write().insert(addr);
        }
    }

    /// 刷新所有节点状态（基于最后活跃时间更新 Good/Questionable）
    pub fn refresh_all_states_sync(&self) {
        let mut nodes = self.nodes.write();
        for entry in nodes.values_mut() {
            entry.refresh_state();
        }
    }

    pub fn rescore_all_sync(&self) {
        let nodes: Vec<KBucketEntry> = self.nodes.read().values().cloned().collect();
        let mut write = self.nodes.write();
        for node in &nodes {
            if let Some(entry) = write.get_mut(&node.addr) {
                entry.score = calculate_node_score(entry);
            }
        }
    }

    // ── 脏标记同步方法（用于增量评分）──

    pub fn mark_dirty_sync(&self, addr: SocketAddr) {
        self.dirty.write().insert(addr);
    }

    pub fn dirty_nodes_sync(&self) -> Vec<SocketAddr> {
        self.dirty.read().iter().cloned().collect()
    }

    pub fn clear_dirty_sync(&self, addr: &SocketAddr) {
        self.dirty.write().remove(addr);
    }

    pub fn clear_all_dirty_sync(&self) {
        self.dirty.write().clear();
    }

    /// 批量更新评分（一次写锁，避免逐个更新的锁竞争）
    pub fn update_scores_batch_sync(&self, scores: &[(SocketAddr, f64)]) {
        let mut nodes = self.nodes.write();
        for (addr, score) in scores {
            if let Some(entry) = nodes.get_mut(addr) {
                entry.score = *score;
            }
        }
    }
}

#[async_trait]
impl NodeRepository for NodeRepoImpl {
    async fn add_node(&self, id: NodeId, addr: SocketAddr) -> bool {
        self.add_node_sync(id, addr)
    }

    async fn remove_node(&self, addr: &SocketAddr) -> bool {
        self.nodes.write().remove(addr).is_some()
    }

    async fn get_node(&self, addr: &SocketAddr) -> Option<KBucketEntry> {
        self.nodes.read().get(addr).cloned()
    }

    async fn all_nodes(&self) -> Vec<KBucketEntry> {
        self.all_nodes_sync()
    }

    async fn node_count(&self) -> usize {
        self.len_sync()
    }

    async fn is_empty(&self) -> bool {
        self.nodes.read().is_empty()
    }

    async fn top_nodes(&self, n: usize) -> Vec<KBucketEntry> {
        self.top_nodes_sync(n)
    }

    fn top_nodes_sync(&self, n: usize) -> Vec<KBucketEntry> {
        NodeRepoImpl::top_nodes_sync(self, n)
    }

    fn len_sync(&self) -> usize {
        NodeRepoImpl::len_sync(self)
    }

    async fn closest_nodes(&self, target: &NodeId, n: usize) -> Vec<KBucketEntry> {
        // NodeRepo 不维护路由表，按 XOR 距离排序
        let mut all: Vec<KBucketEntry> = self.all_nodes_sync();
        all.sort_by_key(|e| crate::dht::xor_distance(&e.id, target));
        all.truncate(n);
        all
    }

    async fn update_score(&self, addr: &SocketAddr, score: f64) {
        if let Some(entry) = self.nodes.write().get_mut(addr) {
            entry.score = score;
        }
    }

    async fn update_scores_batch(&self, scores: &[(SocketAddr, f64)]) {
        self.update_scores_batch_sync(scores);
    }

    async fn record_query(&self, addr: &SocketAddr, success: bool, latency_ms: u64) {
        self.record_query_sync(*addr, success, latency_ms);
    }

    async fn set_node_state(&self, addr: &SocketAddr, state: NodeState) {
        if let Some(entry) = self.nodes.write().get_mut(addr) {
            entry.state = state;
        }
        // 状态变化也标记为脏
        self.dirty.write().insert(*addr);
    }

    async fn refresh_all_states(&self) {
        self.refresh_all_states_sync();
    }

    async fn stats(&self) -> NodeStats {
        self.stats_sync()
    }

    async fn mark_dirty(&self, addr: &SocketAddr) {
        self.mark_dirty_sync(*addr);
    }

    async fn dirty_nodes(&self) -> Vec<SocketAddr> {
        self.dirty_nodes_sync()
    }

    async fn clear_dirty(&self, addr: &SocketAddr) {
        self.clear_dirty_sync(addr);
    }

    async fn clear_all_dirty(&self) {
        self.clear_all_dirty_sync();
    }

    async fn bucket_count(&self) -> usize {
        0 // NodeRepo 不维护 bucket
    }

    async fn non_empty_bucket_targets(&self) -> Vec<NodeId> {
        Vec::new() // NodeRepo 不维护 bucket
    }

    async fn rescore_all(&self) {
        self.rescore_all_sync();
    }

    async fn save_all(&self) -> anyhow::Result<()> {
        let nodes = self.all_nodes_sync();
        // 注意：冷热判定统一由 intelligence 层的 TierSystem 负责
        // 这里全量保存所有节点（定期每5分钟一次，数据量可接受）
        // 构建批量行数据
        let batch: Vec<crate::storage::db::DhtNodeRow> = nodes
            .iter()
            .map(|node| {
                let state_str = match node.state {
                    NodeState::Good => "Good",
                    NodeState::Questionable => "Questionable",
                    NodeState::Bad => "Bad",
                };
                crate::storage::db::DhtNodeRow {
                    id: node.id,
                    ip: node.addr.ip().to_string(),
                    port: node.addr.port(),
                    score: node.score,
                    state: state_str.to_string(),
                    query_count: node.query_count,
                    success_count: node.success_count,
                    total_latency_ms: node.total_latency_ms,
                    consecutive_failures: node.consecutive_failures,
                    nodes_returned: node.nodes_returned,
                    last_query_time: node.last_query_time.map(|t| t.elapsed().as_secs() as i64),
                }
            })
            .collect();
        let storage = self.storage.clone();
        // 用 spawn_blocking 包装数据库操作，避免阻塞 tokio 工作线程
        tokio::task::spawn_blocking(move || storage.save_dht_nodes_batch(&batch)).await??;
        Ok(())
    }

    async fn load_all(&self) -> anyhow::Result<usize> {
        let rows = self.storage.load_dht_nodes()?;
        let mut nodes = self.nodes.write();
        let mut count = 0;
        for row in rows {
            let addr = SocketAddr::new(row.ip.parse().unwrap_or([127, 0, 0, 1].into()), row.port);
            let mut entry = KBucketEntry::new(row.id, addr);
            entry.score = row.score;
            entry.query_count = row.query_count;
            entry.success_count = row.success_count;
            entry.total_latency_ms = row.total_latency_ms;
            entry.consecutive_failures = row.consecutive_failures;
            entry.nodes_returned = row.nodes_returned;
            entry.last_query_time = row.last_query_time.map(|secs| Instant::now() - Duration::from_secs(secs.max(0) as u64));
            entry.state = match row.state.as_str() {
                "Good" => NodeState::Good,
                "Questionable" => NodeState::Questionable,
                _ => NodeState::Bad,
            };
            nodes.insert(addr, entry);
            count += 1;
        }
        Ok(count)
    }
}
