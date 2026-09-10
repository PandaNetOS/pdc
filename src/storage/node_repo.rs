//! NodeRepository 实现
//!
//! 独立的 DHT 节点存储（无容量限制），作为爬虫候选池的唯一归口。
//! 路由表只负责 DHT 路由响应，NodeRepo 负责爬虫候选节点的存储和评分。
//! 内存 FxHashMap + SQLite 增量持久化。
//! 千万级性能优化：FxHashMap 替代 std::HashMap，增量持久化只保存 dirty 节点。

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::federation::gossip::GossipEngine;
use crate::federation::merkle::MerkleTree;
use crate::federation::protocol::{operation, repo_type, SyncEntry};

use crate::dht::kbucket::{KBucketEntry, NodeState};
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
    /// 独立节点存储（无容量限制，按 addr 去重）— FxHashMap 高性能
    nodes: RwLock<FxHashMap<SocketAddr, KBucketEntry>>,
    /// 脏节点集合（统计数据已变化，需要重算评分 + 增量持久化）
    dirty: RwLock<FxHashSet<SocketAddr>>,
    storage: Arc<Storage>,
    /// 联邦引用（OnceLock 注入；未设置时本地写入不触发 Merkle/Gossip，repo 正常工作）
    merkle: OnceLock<Arc<MerkleTree>>,
    gossip: OnceLock<Arc<GossipEngine>>,
}

impl NodeRepoImpl {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            nodes: RwLock::new(FxHashMap::default()),
            dirty: RwLock::new(FxHashSet::default()),
            storage,
            merkle: OnceLock::new(),
            gossip: OnceLock::new(),
        }
    }

    /// 兼容旧接口：从 crawler 路由表创建（现在忽略路由表，独立存储）
    pub fn from_crawler(_routing_table: Arc<parking_lot::RwLock<crate::dht::routing_table::RoutingTable>>, storage: Arc<Storage>) -> Self {
        Self::new(storage)
    }

    /// 注入联邦 Merkle 树与 Gossip 引擎引用（main.rs 在 FederationService 创建后调用）。
    /// 未调用时（如单元测试），本地写入不触发传播，repo 行为完全不变。
    pub fn set_federation_refs(&self, merkle: Arc<MerkleTree>, gossip: Arc<GossipEngine>) {
        let _ = self.merkle.set(merkle);
        let _ = self.gossip.set(gossip);
    }

    /// 将本地新写入的条目批量更新 Merkle 并提交 Gossip（写锁外执行，纯内存操作）。
    /// merkle/gossip 未注入时直接跳过，不 panic。
    #[inline]
    fn propagate(&self, rt: u8, built: Vec<(Vec<u8>, Vec<u8>)>) {
        if built.is_empty() {
            return;
        }
        let Some(merkle) = self.merkle.get() else { return; };
        let Some(gossip) = self.gossip.get() else { return; };
        let refs: Vec<(&[u8], &[u8])> = built
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        merkle.update_batch(&refs);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entries: Vec<SyncEntry> = built
            .into_iter()
            .map(|(key, payload)| SyncEntry {
                key,
                operation: operation::UPSERT,
                version: now,
                payload,
            })
            .collect();
        gossip.submit_gossip(rt, entries);
    }

    // ── 同步便捷方法（爬虫高频调用，避免 async 开销）──

    /// 内部写入：批量新增节点 + 标记 dirty，不触发 Merkle/Gossip。
    /// 返回真正新增的 (node_id, addr) 对。
    /// 联邦同步入站（apply_node_sync）调用本方法，避免 Merkle 重复更新与 Gossip 回环。
    pub(crate) fn add_nodes_batch_internal(&self, items: &[(NodeId, SocketAddr)]) -> Vec<(NodeId, SocketAddr)> {
        if items.is_empty() {
            return Vec::new();
        }
        let mut nodes = self.nodes.write();
        let mut new_pairs: Vec<(NodeId, SocketAddr)> = Vec::new();
        for (id, addr) in items {
            if let Some(existing) = nodes.get_mut(addr) {
                existing.id = *id;
                existing.last_active = Instant::now();
            } else {
                let mut entry = KBucketEntry::new(*id, *addr);
                // 新节点初始评分 45.0（中性分），后续由 ScoreMaintainer 统一更新
                entry.score = 45.0;
                nodes.insert(*addr, entry);
                new_pairs.push((*id, *addr));
            }
        }
        drop(nodes);

        // 新节点统一标记 dirty（需要增量持久化），一次写锁
        if !new_pairs.is_empty() {
            let mut dirty = self.dirty.write();
            for (_, addr) in &new_pairs {
                dirty.insert(*addr);
            }
        }
        new_pairs
    }

    /// 把新节点列表构建成 merkle/gossip 条目并传播。
    fn propagate_nodes(&self, new_pairs: Vec<(NodeId, SocketAddr)>) {
        if new_pairs.is_empty() {
            return;
        }
        let mut built: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(new_pairs.len());
        for (id, addr) in &new_pairs {
            if let Some((k, p)) = crate::federation::sync::build_node_sync_entry(*id, *addr) {
                built.push((k, p));
            }
        }
        self.propagate(repo_type::NODE, built);
    }

    /// 同步加入单个节点（本地爬虫路径）：写入后更新 Merkle + 提交 Gossip。
    /// 返回 true 表示是新节点。
    pub fn add_node_sync(&self, id: NodeId, addr: SocketAddr) -> bool {
        let new_pairs = self.add_nodes_batch_internal(&[(id, addr)]);
        let is_new = !new_pairs.is_empty();
        self.propagate_nodes(new_pairs);
        is_new
    }

    /// 批量加入节点（一次写锁），返回新加入数。
    /// 本地写入路径：新节点更新 Merkle + 提交 Gossip。
    /// 联邦同步入站（apply_node_sync）请改用 add_nodes_batch_internal，避免回环。
    pub fn add_nodes_sync_batch(&self, items: &[(NodeId, SocketAddr)]) -> usize {
        let new_pairs = self.add_nodes_batch_internal(items);
        let count = new_pairs.len();
        self.propagate_nodes(new_pairs);
        count
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
            // 标记为脏：统计数据已变化，需要重算评分 + 增量持久化
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
            // 标记为脏
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

    // ── 脏标记同步方法（用于增量评分 + 增量持久化）──

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

    /// 取出所有 dirty 节点并清空（原子操作，用于增量持久化）
    pub fn take_dirty_sync(&self) -> Vec<SocketAddr> {
        let mut dirty = self.dirty.write();
        let addrs: Vec<SocketAddr> = dirty.iter().cloned().collect();
        dirty.clear();
        addrs
    }

    /// dirty 节点数量
    pub fn dirty_count_sync(&self) -> usize {
        self.dirty.read().len()
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
        let removed = self.nodes.write().remove(addr).is_some();
        if removed {
            self.dirty.write().insert(*addr);
        }
        removed
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
        // 评分由 ScoreMaintainer 统一维护，Repo 不具备算分权限
    }

    async fn save_all(&self) -> anyhow::Result<()> {
        // 【增量持久化】只保存 dirty 节点，避免全量保存千万级数据
        let dirty_addrs = self.take_dirty_sync();
        if dirty_addrs.is_empty() {
            return Ok(());
        }

        // 用独立作用域构建 batch，确保 read guard 在作用域结束时释放
        let batch: Vec<crate::storage::db::DhtNodeRow> = {
            let nodes = self.nodes.read();
            dirty_addrs
                .iter()
                .filter_map(|addr| nodes.get(addr))
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
                .collect()
        };

        if batch.is_empty() {
            return Ok(());
        }

        let storage = self.storage.clone();
        let count = batch.len();
        tracing::debug!("[node_repo] 增量保存 {} 个 dirty 节点", count);
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
