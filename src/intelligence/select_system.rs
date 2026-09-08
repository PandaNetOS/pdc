//! 统一节点选择系统（SelectSystem）
//!
//! 【架构原则】所有节点/peer 选择逻辑统一收口到 intelligence 层。
//! 业务层（爬虫、探测等）不自己实现选择逻辑，通过 SelectSystem 获取最优节点。
//!
//! 【优化】选择过程中遍历引用不克隆，只在最后返回选中节点时克隆，
//! 避免全量克隆 60000+ 节点造成的内存分配和 CPU 开销。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::dht::kbucket::{KBucketEntry, NodeState};
use crate::storage::repo_traits::NodeRepository;

/// 统一节点选择系统
pub struct SelectSystem;

impl SelectSystem {
    pub fn new() -> Self {
        Self
    }

    /// 多样性选择节点（爬虫用）
    ///
    /// 选取规则：
    /// 1. 过滤 Bad 状态节点，按评分降序取 top 候选池
    /// 2. Kademlia ID 空间分桶轮询（按 ID 高 4 位分 16 桶），保证 ID 空间多样性
    /// 3. IP /24 网段去重（同一网段最多选 max_per_subnet 个），保证网络多样性
    /// 4. 每轮从各桶取评分最高的节点，轮询直到选满 count 个
    ///
    /// 【优化】遍历引用不克隆，只在最后返回选中节点时克隆
    pub fn select_diverse_nodes(
        repo: &dyn NodeRepository,
        count: usize,
        max_per_subnet: usize,
    ) -> Vec<KBucketEntry> {
        // 1. 取候选池：用 top_nodes_sync 获取 top 500（内部已排序），避免全量克隆
        let candidates: Vec<KBucketEntry> = repo
            .top_nodes_sync(500)
            .into_iter()
            .filter(|n| n.state != NodeState::Bad)
            .collect();

        if candidates.is_empty() {
            return Vec::new();
        }

        // 2. 按 ID 高 4 位分桶（16 桶）
        let mut buckets: HashMap<u8, Vec<KBucketEntry>> = HashMap::new();
        for node in candidates {
            let bucket_key = node.id[0] >> 4; // 高4位 → 0-15
            buckets.entry(bucket_key).or_default().push(node);
        }

        // 3. 轮询各桶选取，IP /24 去重
        let mut result: Vec<KBucketEntry> = Vec::with_capacity(count);
        let mut subnet_count: HashMap<[u8; 3], usize> = HashMap::new();
        let mut bucket_indices: HashMap<u8, usize> = HashMap::new();
        let bucket_keys: Vec<u8> = buckets.keys().copied().collect();

        'outer: loop {
            if result.len() >= count {
                break;
            }
            let mut picked_any = false;
            for &key in &bucket_keys {
                if result.len() >= count {
                    break 'outer;
                }
                let idx = bucket_indices.entry(key).or_insert(0);
                let bucket = match buckets.get(&key) {
                    Some(b) => b,
                    None => continue,
                };
                // 跳过当前桶中已选完的
                while *idx < bucket.len() {
                    let node = &bucket[*idx];
                    *idx += 1;
                    // IP /24 去重检查
                    let ip = node.addr.ip();
                    if let std::net::IpAddr::V4(v4) = ip {
                        let octets = v4.octets();
                        let subnet = [octets[0], octets[1], octets[2]];
                        let cnt = subnet_count.entry(subnet).or_insert(0);
                        if *cnt >= max_per_subnet {
                            continue; // 该网段已达上限，跳过
                        }
                        *cnt += 1;
                    }
                    result.push(node.clone());
                    picked_any = true;
                    break;
                }
            }
            if !picked_any {
                break; // 所有桶都选完了
            }
        }

        result
    }

    /// 按评分选 Top N 节点
    pub fn select_top_nodes(repo: &dyn NodeRepository, n: usize) -> Vec<KBucketEntry> {
        repo.top_nodes_sync(n)
    }

    /// 爬虫候选选择（热节点优先 + 高评分 + 多样性）
    ///
    /// 策略：
    /// 1. 优先从热节点（最近活跃）中选择
    /// 2. 热节点不足时，从温节点中补充
    /// 3. 保证 ID 空间和网络多样性
    pub fn select_crawl_candidates(
        repo: &dyn NodeRepository,
        count: usize,
        max_per_subnet: usize,
    ) -> Vec<KBucketEntry> {
        // 目前复用多样性选择，热节点优先通过 top_nodes 评分排序间接实现
        // （热节点通常有更多查询记录，评分更高）
        Self::select_diverse_nodes(repo, count, max_per_subnet)
    }

    /// 获取节点总数（用于监控和统计）
    pub fn node_count(repo: &dyn NodeRepository) -> usize {
        repo.len_sync()
    }
}

impl Default for SelectSystem {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    // 测试用的 mock NodeRepository
    struct MockRepo {
        nodes: parking_lot::RwLock<Vec<KBucketEntry>>,
    }

    #[async_trait::async_trait]
    impl NodeRepository for MockRepo {
        async fn add_node(&self, _id: [u8; 20], _addr: SocketAddr) -> bool { false }
        async fn remove_node(&self, _addr: &SocketAddr) -> bool { false }
        async fn get_node(&self, _addr: &SocketAddr) -> Option<KBucketEntry> { None }
        async fn all_nodes(&self) -> Vec<KBucketEntry> { self.nodes.read().clone() }
        async fn node_count(&self) -> usize { self.nodes.read().len() }
        async fn is_empty(&self) -> bool { self.nodes.read().is_empty() }
        async fn top_nodes(&self, n: usize) -> Vec<KBucketEntry> {
            let mut all = self.nodes.read().clone();
            all.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
            all.truncate(n);
            all
        }
        fn top_nodes_sync(&self, n: usize) -> Vec<KBucketEntry> {
            let mut all = self.nodes.read().clone();
            all.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
            all.truncate(n);
            all
        }
        fn len_sync(&self) -> usize { self.nodes.read().len() }
        async fn stats(&self) -> crate::storage::node_repo::NodeStats {
            let nodes = self.nodes.read();
            crate::storage::node_repo::NodeStats {
                total: nodes.len(),
                good: 0,
                questionable: 0,
                bad: 0,
                active: nodes.iter().filter(|n| n.query_count > 0).count(),
                avg_score: if nodes.is_empty() { 0.0 } else { nodes.iter().map(|n| n.score).sum::<f64>() / nodes.len() as f64 },
            }
        }
        async fn closest_nodes(&self, _target: &[u8; 20], _n: usize) -> Vec<KBucketEntry> { Vec::new() }
        async fn update_score(&self, _addr: &SocketAddr, _score: f64) {}
        async fn update_scores_batch(&self, _scores: &[(SocketAddr, f64)]) {}
        async fn record_query(&self, _addr: &SocketAddr, _success: bool, _latency_ms: u64) {}
        async fn set_node_state(&self, _addr: &SocketAddr, _state: NodeState) {}
        async fn refresh_all_states(&self) {}
        async fn bucket_count(&self) -> usize { 0 }
        async fn non_empty_bucket_targets(&self) -> Vec<[u8; 20]> { Vec::new() }
        async fn rescore_all(&self) {}
        async fn save_all(&self) -> anyhow::Result<()> { Ok(()) }
        async fn load_all(&self) -> anyhow::Result<usize> { Ok(0) }
        async fn mark_dirty(&self, _addr: &SocketAddr) {}
        async fn dirty_nodes(&self) -> Vec<SocketAddr> { Vec::new() }
        async fn clear_dirty(&self, _addr: &SocketAddr) {}
        async fn clear_all_dirty(&self) {}
    }

    #[test]
    fn test_select_diverse_nodes_empty() {
        let repo = MockRepo { nodes: parking_lot::RwLock::new(Vec::new()) };
        let result = SelectSystem::select_diverse_nodes(&repo, 10, 2);
        assert!(result.is_empty());
    }

    #[test]
    fn test_select_top_nodes() {
        let mut nodes = Vec::new();
        for i in 0..5 {
            let mut entry = KBucketEntry::new([i as u8; 20], SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, i as u8)), 6881));
            entry.score = i as f64 * 10.0;
            nodes.push(entry);
        }
        let repo = MockRepo { nodes: parking_lot::RwLock::new(nodes) };
        let result = SelectSystem::select_top_nodes(&repo, 3);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].score, 40.0); // 最高分
    }
}
