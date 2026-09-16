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
use crate::storage::db::{DhtNodeRow, Storage};
use crate::storage::repo_traits::{NodeId, NodeRepository};
use crate::storage::write_queue::WriteQueue;

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
    /// /24 网段索引（IPv4 前 3 字节 -> 该网段内节点 ID 列表），用于 O(1) 取网段
    /// 仅 IPv4 节点入索引；IPv6 节点忽略。增删节点时同步维护。
    subnet_index: RwLock<FxHashMap<[u8; 3], Vec<NodeId>>>,
    /// 热节点地址集合（最近 hot_threshold_secs 内被访问的节点）
    hot_addrs: RwLock<FxHashSet<SocketAddr>>,
    /// 冷节点地址集合（超过 hot_threshold_secs 未访问，由外部定时任务迁移）
    cold_addrs: RwLock<FxHashSet<SocketAddr>>,
    storage: Arc<Storage>,
    /// 联邦引用（OnceLock 注入；未设置时本地写入不触发 Merkle/Gossip，repo 正常工作）
    merkle: OnceLock<Arc<MerkleTree>>,
    gossip: OnceLock<Arc<GossipEngine>>,
    /// 写入队列（可选，None 时退化为同步写入）
    write_queue: Option<Arc<WriteQueue>>,
}

impl NodeRepoImpl {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            nodes: RwLock::new(FxHashMap::default()),
            dirty: RwLock::new(FxHashSet::default()),
            subnet_index: RwLock::new(FxHashMap::default()),
            hot_addrs: RwLock::new(FxHashSet::default()),
            cold_addrs: RwLock::new(FxHashSet::default()),
            storage,
            merkle: OnceLock::new(),
            gossip: OnceLock::new(),
            write_queue: None,
        }
    }

    /// 兼容旧接口：从 crawler 路由表创建（现在忽略路由表，独立存储）
    pub fn from_crawler(
        _routing_table: Arc<parking_lot::RwLock<crate::dht::routing_table::RoutingTable>>,
        storage: Arc<Storage>,
    ) -> Self {
        Self::new(storage)
    }

    /// 注入写入队列（builder 模式）
    pub fn with_write_queue(mut self, wq: Arc<WriteQueue>) -> Self {
        self.write_queue = Some(wq);
        self
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
        let Some(merkle) = self.merkle.get() else {
            return;
        };
        let Some(gossip) = self.gossip.get() else {
            return;
        };
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

    /// 提取 SocketAddr 的 IPv4 前 3 字节作为 /24 网段 key。
    /// 仅 IPv4 返回 Some；IPv6 返回 None（不入网段索引）。
    #[inline]
    fn subnet_key(addr: SocketAddr) -> Option<[u8; 3]> {
        match addr.ip() {
            std::net::IpAddr::V4(v4) => {
                let o = v4.octets();
                Some([o[0], o[1], o[2]])
            }
            std::net::IpAddr::V6(_) => None,
        }
    }

    /// 把 (node_id, subnet) 加入 /24 网段索引。
    /// 调用方持有 self.subnet_index 写锁（在批量操作内联完成，避免额外锁竞争）。
    #[inline]
    fn index_subnet(
        subnet_index: &mut FxHashMap<[u8; 3], Vec<NodeId>>,
        subnet: [u8; 3],
        id: NodeId,
    ) {
        subnet_index.entry(subnet).or_default().push(id);
    }

    /// 从 /24 网段索引中移除指定节点 id（按地址定位网段）。
    #[inline]
    fn unindex_subnet(&self, addr: &SocketAddr, id: &NodeId) {
        let Some(subnet) = Self::subnet_key(*addr) else {
            return;
        };
        let mut idx = self.subnet_index.write();
        if let Some(bucket) = idx.get_mut(&subnet) {
            bucket.retain(|x| x != id);
            if bucket.is_empty() {
                idx.remove(&subnet);
            }
        }
    }

    /// 内部写入：批量新增节点 + 标记 dirty，不触发 Merkle/Gossip。
    /// 返回真正新增的 (node_id, addr) 对。
    /// 联邦同步入站（apply_node_sync）调用本方法，避免 Merkle 重复更新与 Gossip 回环。
    pub(crate) fn add_nodes_batch_internal(
        &self,
        items: &[(NodeId, SocketAddr)],
    ) -> Vec<(NodeId, SocketAddr)> {
        if items.is_empty() {
            return Vec::new();
        }
        let mut nodes = self.nodes.write();
        let mut subnet_index = self.subnet_index.write();
        let mut new_pairs: Vec<(NodeId, SocketAddr)> = Vec::new();
        for (id, addr) in items {
            if let Some(existing) = nodes.get_mut(addr) {
                // 同地址节点 ID 更新：若 ID 变化，同步更新网段索引中的旧 ID
                if existing.id != *id {
                    if let Some(subnet) = Self::subnet_key(*addr) {
                        if let Some(bucket) = subnet_index.get_mut(&subnet) {
                            bucket.retain(|x| x != &existing.id);
                        }
                        Self::index_subnet(&mut subnet_index, subnet, *id);
                    }
                }
                existing.id = *id;
                existing.last_active = Instant::now();
            } else {
                let mut entry = KBucketEntry::new(*id, *addr);
                // 新节点初始评分 45.0（中性分），后续由 ScoreMaintainer 统一更新
                entry.score = 45.0;
                nodes.insert(*addr, entry);
                new_pairs.push((*id, *addr));
                // IPv4 节点入 /24 索引
                if let Some(subnet) = Self::subnet_key(*addr) {
                    Self::index_subnet(&mut subnet_index, subnet, *id);
                }
            }
        }
        drop(nodes);
        drop(subnet_index);

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

    // ── /24 网段索引查询（O(1) 定位网段，供节点选择/监控使用）──

    /// 获取指定 /24 网段的节点 ID 列表
    pub fn nodes_by_subnet_sync(&self, subnet: [u8; 3]) -> Vec<NodeId> {
        self.subnet_index
            .read()
            .get(&subnet)
            .cloned()
            .unwrap_or_default()
    }

    /// 获取所有 /24 网段列表
    pub fn all_subnets_sync(&self) -> Vec<[u8; 3]> {
        self.subnet_index.read().keys().copied().collect()
    }

    /// /24 网段数量
    pub fn subnet_count_sync(&self) -> usize {
        self.subnet_index.read().len()
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
        NodeStats {
            total,
            good,
            questionable,
            bad,
            active,
            avg_score,
        }
    }

    pub fn top_nodes_sync(&self, n: usize) -> Vec<KBucketEntry> {
        let mut all: Vec<KBucketEntry> = self.nodes.read().values().cloned().collect();
        all.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
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
    pub fn record_query_with_nodes_sync(
        &self,
        addr: SocketAddr,
        latency_ms: u64,
        nodes_returned: u64,
    ) {
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

    // ── 冷热分层索引（内存索引框架，不搬数据，由外部 TaskScheduler 调度迁移）──

    /// 标记节点被访问（更新 last_accessed，移入 hot 集合，从 cold 移除）
    pub fn mark_accessed_sync(&self, addr: SocketAddr) {
        let found = {
            let mut nodes = self.nodes.write();
            if let Some(entry) = nodes.get_mut(&addr) {
                entry.last_accessed = Some(Instant::now());
                true
            } else {
                false
            }
        };
        if found {
            self.hot_addrs.write().insert(addr);
            self.cold_addrs.write().remove(&addr);
        }
    }

    /// 返回热节点列表（hot 集合中的节点，按评分降序由调用方排序）
    pub fn hot_nodes_sync(&self) -> Vec<KBucketEntry> {
        let nodes = self.nodes.read();
        self.hot_addrs
            .read()
            .iter()
            .filter_map(|addr| nodes.get(addr).cloned())
            .collect()
    }

    /// 将超过阈值的节点从 hot 移到 cold（由外部 TaskScheduler 定时调用，模块内不自跑定时）
    /// 返回本次迁移的节点数
    pub fn migrate_hot_to_cold_sync(&self, threshold_secs: u64) -> usize {
        let cutoff = Instant::now() - Duration::from_secs(threshold_secs);
        let nodes = self.nodes.read();
        let mut hot = self.hot_addrs.write();
        let mut cold = self.cold_addrs.write();
        let to_move: Vec<SocketAddr> = hot
            .iter()
            .filter(|addr| {
                nodes
                    .get(addr)
                    .and_then(|e| e.last_accessed)
                    .map(|t| t < cutoff)
                    .unwrap_or(true)
            })
            .copied()
            .collect();
        let moved = to_move.len();
        for addr in to_move {
            hot.remove(&addr);
            cold.insert(addr);
        }
        moved
    }

    /// 热节点数量
    pub fn hot_count_sync(&self) -> usize {
        self.hot_addrs.read().len()
    }

    /// 冷节点数量
    pub fn cold_count_sync(&self) -> usize {
        self.cold_addrs.read().len()
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

    /// WriteQueue 队列长度（如果接入了写入队列）
    pub fn write_queue_len_sync(&self) -> usize {
        self.write_queue
            .as_ref()
            .map(|wq| wq.stats().queue_size)
            .unwrap_or(0)
    }

    /// 根据 dirty 地址列表构建 DhtNodeRow 批量（从内存 nodes 读取，不修改任何状态）
    fn build_dirty_batch(&self, dirty_addrs: &[SocketAddr]) -> Vec<DhtNodeRow> {
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
                DhtNodeRow {
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
    }

    /// 批量更新评分（一次写锁，避免逐个更新的锁竞争）
    pub fn update_scores_batch_sync(&self, scores: &[(SocketAddr, f64)]) {
        let mut nodes = self.nodes.write();
        let mut dirty = self.dirty.write();
        for (addr, score) in scores {
            if let Some(entry) = nodes.get_mut(addr) {
                entry.score = *score;
                dirty.insert(*addr);
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
        let removed_entry = self.nodes.write().remove(addr);
        let Some(entry) = removed_entry else {
            return false;
        };
        // 从 /24 网段索引中移除该节点
        self.unindex_subnet(addr, &entry.id);
        self.dirty.write().insert(*addr);
        true
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

    fn nodes_by_subnet_sync(&self, subnet: [u8; 3]) -> Vec<NodeId> {
        NodeRepoImpl::nodes_by_subnet_sync(self, subnet)
    }

    fn subnet_count_sync(&self) -> usize {
        NodeRepoImpl::subnet_count_sync(self)
    }

    fn hot_nodes_sync(&self) -> Vec<KBucketEntry> {
        NodeRepoImpl::hot_nodes_sync(self)
    }

    fn mark_accessed_sync(&self, addr: SocketAddr) {
        NodeRepoImpl::mark_accessed_sync(self, addr);
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

    async fn save_dirty(&self) -> anyhow::Result<()> {
        // 【增量持久化】只保存 dirty 节点，避免全量保存千万级数据
        if let Some(wq) = &self.write_queue {
            // 异步模式：原子取出并清空 dirty，非阻塞入队 WriteQueue
            let dirty_addrs = self.take_dirty_sync();
            if dirty_addrs.is_empty() {
                return Ok(());
            }
            let batch = self.build_dirty_batch(&dirty_addrs);
            if batch.is_empty() {
                return Ok(());
            }
            let wq = wq.clone();
            let count = batch.len();
            wq.send(move |conn| Storage::save_dht_nodes_batch_in_tx(conn, &batch));
            tracing::debug!("[node_repo] 异步入队保存 {} 个 dirty 节点", count);
            Ok(())
        } else {
            // 同步模式：先查看 dirty（不清空），保存成功后再清空，失败则保留重试
            let dirty_addrs = self.dirty_nodes_sync();
            if dirty_addrs.is_empty() {
                return Ok(());
            }
            let batch = self.build_dirty_batch(&dirty_addrs);
            if batch.is_empty() {
                // 内存中已不存在的 dirty 节点（可能已被删除），清理标记
                let mut dirty = self.dirty.write();
                for addr in &dirty_addrs {
                    dirty.remove(addr);
                }
                return Ok(());
            }
            let storage = self.storage.clone();
            let count = batch.len();
            tracing::debug!("[node_repo] 增量保存 {} 个 dirty 节点", count);
            let result =
                tokio::task::spawn_blocking(move || storage.save_dht_nodes_batch(&batch)).await?;
            match result {
                Ok(()) => {
                    let mut dirty = self.dirty.write();
                    for addr in &dirty_addrs {
                        dirty.remove(addr);
                    }
                    Ok(())
                }
                Err(e) => {
                    tracing::warn!("[node_repo] 增量保存失败（保留 dirty 待重试）: {}", e);
                    Err(e)
                }
            }
        }
    }

    async fn load_all(&self) -> anyhow::Result<usize> {
        let rows = self.storage.load_dht_nodes()?;
        let mut nodes = self.nodes.write();
        let mut subnet_index = self.subnet_index.write();
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
            entry.last_query_time = row
                .last_query_time
                .map(|secs| Instant::now() - Duration::from_secs(secs.max(0) as u64));
            entry.state = match row.state.as_str() {
                "Good" => NodeState::Good,
                "Questionable" => NodeState::Questionable,
                _ => NodeState::Bad,
            };
            nodes.insert(addr, entry);
            // 重建 /24 网段索引
            if let Some(subnet) = Self::subnet_key(addr) {
                Self::index_subnet(&mut subnet_index, subnet, row.id);
            }
            count += 1;
        }
        Ok(count)
    }

    async fn remove_cold_nodes(&self, older_than_secs: u64) -> anyhow::Result<usize> {
        // 驱逐前若存在脏数据，先落库，避免丢失尚未持久化的节点更新。
        // 失败不阻断驱逐（节点在 DB 中仍有上一份快照，不丢行）。
        if self.dirty_count_sync() > 0 {
            if let Err(e) = self.save_dirty().await {
                tracing::warn!("[node_repo] 冷节点驱逐前增量持久化失败（继续驱逐）: {}", e);
            }
        }

        // cutoff：last_active 早于该时刻的节点视为冷节点。
        // 使用 checked_sub 避免阈值过大导致 Instant 下溢；未来时间的 last_active 天然晚于 cutoff，不会被误删。
        let cutoff = match Instant::now().checked_sub(Duration::from_secs(older_than_secs)) {
            Some(c) => c,
            None => return Ok(0),
        };

        // 第一遍：读锁内快照候选地址（避免持写锁长时间遍历）
        let candidates: Vec<(SocketAddr, NodeId)> = {
            let nodes = self.nodes.read();
            nodes
                .iter()
                .filter(|(_, e)| e.last_active < cutoff)
                .map(|(addr, e)| (*addr, e.id))
                .collect()
        };
        if candidates.is_empty() {
            return Ok(0);
        }

        // 第二遍：持写锁批量移除，同步清理 /24 索引、热/冷集合与脏标记。
        // 注意：不把被移除节点加入 dirty 集合——DB 行永久保留，删除仅作用于内存。
        let mut nodes = self.nodes.write();
        let mut subnet_index = self.subnet_index.write();
        let mut hot = self.hot_addrs.write();
        let mut cold = self.cold_addrs.write();
        let mut dirty = self.dirty.write();
        let mut removed = 0;
        for (addr, id) in &candidates {
            if nodes.remove(addr).is_some() {
                removed += 1;
                if let Some(subnet) = Self::subnet_key(*addr) {
                    if let Some(bucket) = subnet_index.get_mut(&subnet) {
                        bucket.retain(|x| x != id);
                        if bucket.is_empty() {
                            subnet_index.remove(&subnet);
                        }
                    }
                }
                hot.remove(addr);
                cold.remove(addr);
                dirty.remove(addr);
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_repo() -> NodeRepoImpl {
        let storage = Arc::new(Storage::memory().unwrap());
        NodeRepoImpl::new(storage)
    }

    fn addr(oct: u8, port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, oct)), port)
    }

    #[tokio::test]
    async fn test_remove_cold_nodes_removes_only_cold() {
        let repo = test_repo();
        let hot = addr(1, 1001);
        let warm = addr(2, 1002);
        let cold = addr(3, 1003);

        repo.add_node_sync([1u8; 20], hot);
        repo.add_node_sync([2u8; 20], warm);
        repo.add_node_sync([3u8; 20], cold);
        assert_eq!(repo.len_sync(), 3);

        // 把 warm 推到阈值内偏久、cold 推到超过 warm 阈值（7200s）
        let warm_cutoff = Instant::now() - Duration::from_secs(3600);
        let cold_cutoff = Instant::now() - Duration::from_secs(10_000);
        repo.nodes.write().get_mut(&warm).unwrap().last_active = warm_cutoff;
        repo.nodes.write().get_mut(&cold).unwrap().last_active = cold_cutoff;

        // warm_threshold = 7200s：只有 cold 应被驱逐
        let removed = repo.remove_cold_nodes(7200).await.unwrap();
        assert_eq!(removed, 1, "应只移除 1 个冷节点");
        assert!(repo.contains_sync(hot), "热节点必须保留");
        assert!(repo.contains_sync(warm), "温节点必须保留");
        assert!(!repo.contains_sync(cold), "冷节点应被移除");
        assert_eq!(repo.len_sync(), 2);
    }

    #[tokio::test]
    async fn test_remove_cold_nodes_empty_when_nothing_cold() {
        let repo = test_repo();
        let a = addr(9, 1009);
        repo.add_node_sync([9u8; 20], a);
        // 新建节点 last_active = now，全部为热节点，不应移除
        let removed = repo.remove_cold_nodes(7200).await.unwrap();
        assert_eq!(removed, 0);
        assert!(repo.contains_sync(a));
    }
}
