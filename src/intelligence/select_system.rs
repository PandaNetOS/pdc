//! 统一节点选择系统（SelectSystem）
//!
//! 【架构原则】所有节点/peer 选择逻辑统一收口到 intelligence 层。
//! 业务层（爬虫、探测等）不自己实现选择逻辑，通过 SelectSystem 获取最优节点。
//!
//! 【新近度分层选择（2026-10，20号方案 D2）】
//! 发送预算按「已验证活 > 新鲜未验证 > 探索」分配：
//! - L0 Verified：本会话响应成功过（last_verified 在窗口内），最强活性证据
//! - L1 Fresh：热池中其余节点（本会话新发现/近期验证），强活性先验
//! - L2+ Explore：全池按评分 top500 补足，预算受限（explore_ratio）——
//!   无论陈旧池多大，每轮损耗上限即该比例
//!
//! 热池语义 = 已验证响应/本会话新发现（由 NodeRepo 反馈路径维护），
//! 不再由「被选中」自我强化（旧 mark_accessed 回路已移除）。

use rustc_hash::FxHashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use crate::dht::kbucket::{KBucketEntry, NodeState};
use crate::storage::repo_traits::NodeRepository;

/// 选择层标签（随条目返回，供发送侧计入分层反馈账目）
pub const LAYER_VERIFIED: u8 = 0;
pub const LAYER_FRESH: u8 = 1;
pub const LAYER_EXPLORE: u8 = 2;

/// 分层选择参数
pub struct SelectionParams<'a> {
    /// false = legacy 模式（热池优先 + top500 评分，旧行为逃生通道）
    pub layered: bool,
    /// 探索（L2+）预算占比，0.0-1.0
    pub explore_ratio: f64,
    /// L0 已验证窗口（秒）
    pub verified_recent_secs: u64,
    /// 同节点复探最小间隔（秒）：距上次查询不足此间隔的节点跳过（批次K #12）
    pub reprobe_min_interval_secs: u64,
    /// 在飞节点（pending 中）——本轮排除，避免重复消耗预算
    pub exclude: &'a HashSet<SocketAddr>,
}

/// 批次K(#12)：是否处于复探冷却——距上次查询不足间隔的节点本轮跳过
fn in_reprobe_cooldown(n: &KBucketEntry, interval_secs: u64) -> bool {
    if interval_secs == 0 {
        return false;
    }
    n.last_query_time
        .map(|t| t.elapsed() < Duration::from_secs(interval_secs))
        .unwrap_or(false)
}

/// 统一节点选择系统
pub struct SelectSystem;

impl SelectSystem {
    pub fn new() -> Self {
        Self
    }

    /// 多样性选择节点（爬虫用）— legacy 路径（旧行为：热池优先 + top500）
    ///
    /// 选取规则：
    /// 1. 优先从热节点（最近被访问）中按多样性选取
    /// 2. 热节点不足时，从全量 top 节点补充
    /// 3. Kademlia ID 空间分桶轮询（按 ID 高 4 位分 16 桶），保证 ID 空间多样性
    /// 4. IP /24 网段去重（同一网段最多选 max_per_subnet 个），保证网络多样性
    ///
    /// 【优化】遍历引用不克隆，只在最后返回选中节点时克隆
    pub fn select_diverse_nodes(
        repo: &dyn NodeRepository,
        count: usize,
        max_per_subnet: usize,
    ) -> Vec<KBucketEntry> {
        let mut result: Vec<KBucketEntry> = Vec::with_capacity(count);
        let mut subnet_count: FxHashMap<[u8; 3], usize> = FxHashMap::default();
        let empty: HashSet<SocketAddr> = HashSet::new();

        // 1. 优先从热节点选取
        let hot_candidates: Vec<KBucketEntry> = repo
            .hot_nodes_sync()
            .into_iter()
            .filter(|n| n.state != NodeState::Bad)
            .collect();
        Self::pick_from_candidates(
            &hot_candidates,
            count,
            max_per_subnet,
            &mut result,
            &mut subnet_count,
            &empty,
        );

        // 2. 热节点不足，从全量 top 节点补充（排除已选）
        if result.len() < count {
            let existing_addrs: std::collections::HashSet<_> =
                result.iter().map(|n| n.addr).collect();
            let all_candidates: Vec<KBucketEntry> = repo
                .top_nodes_sync(500)
                .into_iter()
                .filter(|n| n.state != NodeState::Bad && !existing_addrs.contains(&n.addr))
                .collect();
            Self::pick_from_candidates(
                &all_candidates,
                count,
                max_per_subnet,
                &mut result,
                &mut subnet_count,
                &empty,
            );
        }

        // 3. 标记选中节点为热节点（legacy 语义保留：选中即热，自我强化回路）
        for node in &result {
            repo.mark_accessed_sync(node.addr);
        }

        result
    }

    /// 新近度分层选择（2026-10 D2）：返回 (节点, 层标签)。
    ///
    /// 预算分配：exploit = count × (1 - explore_ratio) 优先给 L0（已验证）→ L1（新鲜），
    /// 不足与探索预算统一由 top500 评分序补足（层内仍走 ID 分桶 + /24 去重）。
    pub fn select_diverse_nodes_tagged(
        repo: &dyn NodeRepository,
        count: usize,
        max_per_subnet: usize,
        params: &SelectionParams<'_>,
    ) -> Vec<(KBucketEntry, u8)> {
        if !params.layered {
            // legacy 模式：旧行为，全部记为探索层（无分层语义）
            return Self::select_diverse_nodes(repo, count, max_per_subnet)
                .into_iter()
                .map(|e| (e, LAYER_EXPLORE))
                .collect();
        }
        if count == 0 {
            return Vec::new();
        }

        let mut result: Vec<KBucketEntry> = Vec::with_capacity(count);
        let mut subnet_count: FxHashMap<[u8; 3], usize> = FxHashMap::default();
        let explore_target = ((count as f64 * params.explore_ratio).round() as usize).min(count);
        let exploit_target = count - explore_target;
        let verified_window = Duration::from_secs(params.verified_recent_secs);

        let hot = repo.hot_nodes_sync();

        // L0：已验证活（本会话响应成功且在窗口内），按评分降序
        let mut verified: Vec<KBucketEntry> = hot
            .iter()
            .filter(|n| {
                n.state != NodeState::Bad
                    && !params.exclude.contains(&n.addr)
                    && !in_reprobe_cooldown(n, params.reprobe_min_interval_secs)
                    && n.last_verified
                        .map(|t| t.elapsed() <= verified_window)
                        .unwrap_or(false)
            })
            .cloned()
            .collect();
        verified.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let verified_n = result.len();
        Self::pick_from_candidates(
            &verified,
            exploit_target,
            max_per_subnet,
            &mut result,
            &mut subnet_count,
            params.exclude,
        );
        let verified_count = result.len() - verified_n;

        // L1：热池其余（本会话新发现/较早验证），按评分降序
        if result.len() < exploit_target {
            let chosen: HashSet<SocketAddr> = result.iter().map(|n| n.addr).collect();
            let mut fresh: Vec<KBucketEntry> = hot
                .iter()
                .filter(|n| {
                    n.state != NodeState::Bad
                        && !params.exclude.contains(&n.addr)
                        && !in_reprobe_cooldown(n, params.reprobe_min_interval_secs)
                        && !chosen.contains(&n.addr)
                })
                .cloned()
                .collect();
            fresh.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            Self::pick_from_candidates(
                &fresh,
                exploit_target,
                max_per_subnet,
                &mut result,
                &mut subnet_count,
                params.exclude,
            );
        }

        // top500 补足：exploit 缺口 + 探索预算（无论陈旧池多大，探索损耗受 explore_target 约束）
        let mut tagged: Vec<(KBucketEntry, u8)> = Vec::with_capacity(count);
        for (idx, entry) in result.iter().enumerate() {
            let layer = if idx < verified_count {
                LAYER_VERIFIED
            } else {
                LAYER_FRESH
            };
            tagged.push((entry.clone(), layer));
        }
        if tagged.len() < count {
            let chosen: HashSet<SocketAddr> = tagged.iter().map(|(e, _)| e.addr).collect();
            let pool: Vec<KBucketEntry> = repo
                .top_nodes_sync(500)
                .into_iter()
                .filter(|n| {
                    n.state != NodeState::Bad
                        && !params.exclude.contains(&n.addr)
                        && !in_reprobe_cooldown(n, params.reprobe_min_interval_secs)
                        && !chosen.contains(&n.addr)
                })
                .collect();
            let before = result.len();
            Self::pick_from_candidates(
                &pool,
                count,
                max_per_subnet,
                &mut result,
                &mut subnet_count,
                params.exclude,
            );
            for entry in &result[before..] {
                tagged.push((entry.clone(), LAYER_EXPLORE));
            }
        }
        tagged
    }

    /// 从候选节点中按 ID 分桶 + /24 去重选取，结果追加到 result（内部辅助方法）。
    /// `exclude` 中的地址（在飞节点）跳过不选。
    fn pick_from_candidates(
        candidates: &[KBucketEntry],
        target_count: usize,
        max_per_subnet: usize,
        result: &mut Vec<KBucketEntry>,
        subnet_count: &mut FxHashMap<[u8; 3], usize>,
        exclude: &HashSet<SocketAddr>,
    ) {
        if candidates.is_empty() {
            return;
        }
        if result.len() >= target_count {
            return;
        }

        // 按 ID 高 4 位分桶（16 桶）
        let mut buckets: FxHashMap<u8, Vec<&KBucketEntry>> = FxHashMap::default();
        for node in candidates {
            let bucket_key = node.id[0] >> 4; // 高4位 → 0-15
            buckets.entry(bucket_key).or_default().push(node);
        }

        let mut bucket_indices: FxHashMap<u8, usize> = FxHashMap::default();
        let bucket_keys: Vec<u8> = buckets.keys().copied().collect();

        'outer: loop {
            if result.len() >= target_count {
                break;
            }
            let mut picked_any = false;
            for &key in &bucket_keys {
                if result.len() >= target_count {
                    break 'outer;
                }
                let idx = bucket_indices.entry(key).or_insert(0);
                let bucket = match buckets.get(&key) {
                    Some(b) => b,
                    None => continue,
                };
                while *idx < bucket.len() {
                    let node = &bucket[*idx];
                    *idx += 1;
                    // 在飞去重：pending 中的节点本轮不重复消耗预算
                    if exclude.contains(&node.addr) {
                        continue;
                    }
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
                    result.push((*node).clone());
                    picked_any = true;
                    break;
                }
            }
            if !picked_any {
                break; // 所有桶都选完了
            }
        }
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
    use std::time::Instant;

    // 测试用的 mock NodeRepository：hot = 热池子集；all = 全量池（top_nodes 来源）
    struct MockRepo {
        hot: parking_lot::RwLock<Vec<KBucketEntry>>,
        all: parking_lot::RwLock<Vec<KBucketEntry>>,
        accessed: std::sync::atomic::AtomicUsize,
    }

    impl MockRepo {
        fn new(hot: Vec<KBucketEntry>, all: Vec<KBucketEntry>) -> Self {
            Self {
                hot: parking_lot::RwLock::new(hot),
                all: parking_lot::RwLock::new(all),
                accessed: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl NodeRepository for MockRepo {
        async fn add_node(&self, _id: [u8; 20], _addr: SocketAddr) -> bool {
            false
        }
        async fn remove_node(&self, _addr: &SocketAddr) -> bool {
            false
        }
        async fn get_node(&self, _addr: &SocketAddr) -> Option<KBucketEntry> {
            None
        }
        async fn all_nodes(&self) -> Vec<KBucketEntry> {
            self.all.read().clone()
        }
        async fn node_count(&self) -> usize {
            self.all.read().len()
        }
        async fn is_empty(&self) -> bool {
            self.all.read().is_empty()
        }
        async fn top_nodes(&self, n: usize) -> Vec<KBucketEntry> {
            let mut all = self.all.read().clone();
            all.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            all.truncate(n);
            all
        }
        fn top_nodes_sync(&self, n: usize) -> Vec<KBucketEntry> {
            let mut all = self.all.read().clone();
            all.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            all.truncate(n);
            all
        }
        fn hot_nodes_sync(&self) -> Vec<KBucketEntry> {
            self.hot.read().clone()
        }
        fn mark_accessed_sync(&self, _addr: SocketAddr) {
            self.accessed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        fn len_sync(&self) -> usize {
            self.all.read().len()
        }
        async fn stats(&self) -> crate::storage::node_repo::NodeStats {
            let nodes = self.all.read();
            crate::storage::node_repo::NodeStats {
                total: nodes.len(),
                good: 0,
                questionable: 0,
                bad: 0,
                active: nodes.iter().filter(|n| n.query_count > 0).count(),
                avg_score: if nodes.is_empty() {
                    0.0
                } else {
                    nodes.iter().map(|n| n.score).sum::<f64>() / nodes.len() as f64
                },
            }
        }
        async fn closest_nodes(&self, _target: &[u8; 20], _n: usize) -> Vec<KBucketEntry> {
            Vec::new()
        }
        async fn update_score(&self, _addr: &SocketAddr, _score: f64) {}
        async fn update_scores_batch(&self, _scores: &[(SocketAddr, f64)]) {}
        async fn record_query(&self, _addr: &SocketAddr, _success: bool, _latency_ms: u64) {}
        async fn set_node_state(&self, _addr: &SocketAddr, _state: NodeState) {}
        async fn refresh_all_states(&self) {}
        async fn bucket_count(&self) -> usize {
            0
        }
        async fn non_empty_bucket_targets(&self) -> Vec<[u8; 20]> {
            Vec::new()
        }
        async fn rescore_all(&self) {}
        async fn save_dirty(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn load_all(&self) -> anyhow::Result<usize> {
            Ok(0)
        }
        async fn remove_cold_nodes(&self, _older_than_secs: u64) -> anyhow::Result<usize> {
            Ok(0)
        }
        fn evict_by_count(&self, _max_count: usize) -> usize {
            0
        }
        async fn mark_dirty(&self, _addr: &SocketAddr) {}
        async fn dirty_nodes(&self) -> Vec<SocketAddr> {
            Vec::new()
        }
        async fn clear_dirty(&self, _addr: &SocketAddr) {}
        async fn clear_dirty_batch(&self, _addrs: &[SocketAddr]) {}
        async fn clear_all_dirty(&self) {}
    }

    /// 构造测试节点：id/网段由 oct 区分（保证 /24 多样性），verified 决定 L0 归属
    fn entry(oct: u8, port: u16, score: f64, verified: bool) -> KBucketEntry {
        let mut e = KBucketEntry::new(
            [oct; 20],
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, oct, oct, oct)), port),
        );
        e.score = score;
        if verified {
            e.last_verified = Some(Instant::now());
        }
        e
    }

    fn params_layered<'a>(exclude: &'a HashSet<SocketAddr>, ratio: f64) -> SelectionParams<'a> {
        SelectionParams {
            layered: true,
            explore_ratio: ratio,
            verified_recent_secs: 600,
            reprobe_min_interval_secs: 0,
            exclude,
        }
    }

    #[test]
    fn test_select_diverse_nodes_empty() {
        let repo = MockRepo::new(Vec::new(), Vec::new());
        let result = SelectSystem::select_diverse_nodes(&repo, 10, 2);
        assert!(result.is_empty());
    }

    #[test]
    fn test_select_top_nodes() {
        let all: Vec<KBucketEntry> = (0..5)
            .map(|i| entry(i as u8, 6881, i as f64 * 10.0, false))
            .collect();
        let repo = MockRepo::new(Vec::new(), all);
        let result = SelectSystem::select_top_nodes(&repo, 3);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].score, 40.0); // 最高分
    }

    /// 分层预算：count=10、ratio=0.2 → exploit 8 个从 L0 已验证池，explore 2 个从 top500
    #[test]
    fn test_layered_exploit_and_explore_budget() {
        let hot: Vec<KBucketEntry> = (1..=10)
            .map(|i| entry(i as u8, 6000 + i as u16, 10.0, true))
            .collect();
        // 全量池 = 热池 + 20 个陈旧高分节点（评分 100，未被验证）。
        // id 高 4 位决定分桶：stale id 用 i*13+3 铺满全部 16 个桶，
        // 保证每个桶的队首都是高分 stale（贴近真实池的桶分布）
        let mut all = hot.clone();
        for i in 0..20u32 {
            let oct = (i * 13 + 3) as u8;
            all.push(entry(oct, 7000 + i as u16, 100.0, false));
        }
        let repo = MockRepo::new(hot, all);
        let exclude = HashSet::new();
        let tagged = SelectSystem::select_diverse_nodes_tagged(
            &repo,
            10,
            30,
            &params_layered(&exclude, 0.2),
        );
        assert_eq!(tagged.len(), 10);
        let verified_n = tagged.iter().filter(|(_, l)| *l == LAYER_VERIFIED).count();
        let explore_n = tagged.iter().filter(|(_, l)| *l == LAYER_EXPLORE).count();
        assert_eq!(verified_n, 8, "exploit 预算 8 个应全部来自 L0 已验证层");
        assert_eq!(explore_n, 2, "探索预算 2 个应来自 top500 陈旧池");
        // 探索位应选中陈旧高分节点（它们评分 100 排在 top500 前列）
        assert!(tagged
            .iter()
            .skip(8)
            .all(|(e, l)| *l == LAYER_EXPLORE && e.score == 100.0));
    }

    /// exploit 不足：热池仅 3 个已验证 → 3 个 L0 + 7 个由 top500 补足，总数仍为 count
    #[test]
    fn test_layered_exploit_shortfall_fills_from_top() {
        let hot: Vec<KBucketEntry> = (1..=3)
            .map(|i| entry(i as u8, 6000 + i as u16, 10.0, true))
            .collect();
        let mut all = hot.clone();
        for i in 1..=20u8 {
            all.push(entry(100 + i, 7000 + i as u16, 50.0, false));
        }
        let repo = MockRepo::new(hot, all);
        let exclude = HashSet::new();
        let tagged = SelectSystem::select_diverse_nodes_tagged(
            &repo,
            10,
            30,
            &params_layered(&exclude, 0.2),
        );
        assert_eq!(tagged.len(), 10, "exploit 缺口必须由 top500 补足");
        let verified_n = tagged.iter().filter(|(_, l)| *l == LAYER_VERIFIED).count();
        assert_eq!(verified_n, 3);
    }

    /// 在飞去重：exclude 中的节点不得入选
    #[test]
    fn test_layered_excludes_inflight() {
        let hot: Vec<KBucketEntry> = (1..=5)
            .map(|i| entry(i as u8, 6000 + i as u16, 10.0, true))
            .collect();
        let repo = MockRepo::new(hot.clone(), hot);
        let mut exclude = HashSet::new();
        exclude.insert(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 1, 1, 1)),
            6001,
        ));
        let tagged =
            SelectSystem::select_diverse_nodes_tagged(&repo, 5, 30, &params_layered(&exclude, 0.0));
        assert!(
            tagged.iter().all(|(e, _)| !exclude.contains(&e.addr)),
            "在飞节点不得被选中"
        );
    }

    /// 复探冷却（批次K #12）：距上次查询不足间隔的节点跳过，超过则可复选
    #[test]
    fn test_reprobe_cooldown_filter() {
        let mut hot: Vec<KBucketEntry> = (1..=2)
            .map(|i| entry(i as u8, 6000 + i as u16, 10.0, true))
            .collect();
        // 节点1 刚被查询过（100s 前 < 300s 间隔）；节点2 从未查询
        hot[0].last_query_time = Some(Instant::now() - Duration::from_secs(100));
        let repo = MockRepo::new(hot.clone(), hot);
        let exclude = HashSet::new();
        let p = SelectionParams {
            layered: true,
            explore_ratio: 0.0,
            verified_recent_secs: 600,
            reprobe_min_interval_secs: 300,
            exclude: &exclude,
        };
        let tagged = SelectSystem::select_diverse_nodes_tagged(&repo, 5, 30, &p);
        let addrs: Vec<SocketAddr> = tagged.iter().map(|(e, _)| e.addr).collect();
        let recently_probed = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 1, 1, 1)), 6001);
        assert!(
            !addrs.contains(&recently_probed),
            "复探冷却期内的节点不得入选"
        );
    }

    /// legacy 模式：旧行为（含 mark_accessed 自我强化语义），全部记探索层
    #[test]
    fn test_legacy_mode_keeps_old_behavior() {
        let hot: Vec<KBucketEntry> = (1..=5)
            .map(|i| entry(i as u8, 6000 + i as u16, 10.0, false))
            .collect();
        let repo = MockRepo::new(hot, Vec::new());
        let exclude = HashSet::new();
        let p = SelectionParams {
            layered: false,
            explore_ratio: 0.2,
            verified_recent_secs: 600,
            reprobe_min_interval_secs: 0,
            exclude: &exclude,
        };
        let tagged = SelectSystem::select_diverse_nodes_tagged(&repo, 5, 30, &p);
        assert_eq!(tagged.len(), 5);
        assert!(tagged.iter().all(|(_, l)| *l == LAYER_EXPLORE));
        assert_eq!(
            repo.accessed.load(std::sync::atomic::Ordering::Relaxed),
            5,
            "legacy 模式保留选中即热语义"
        );
    }

    /// layered 模式不再调用 mark_accessed（自我强化回路移除）
    #[test]
    fn test_layered_does_not_self_mark_hot() {
        let hot: Vec<KBucketEntry> = (1..=5)
            .map(|i| entry(i as u8, 6000 + i as u16, 10.0, true))
            .collect();
        let repo = MockRepo::new(hot, Vec::new());
        let exclude = HashSet::new();
        let _ =
            SelectSystem::select_diverse_nodes_tagged(&repo, 5, 30, &params_layered(&exclude, 0.0));
        assert_eq!(repo.accessed.load(std::sync::atomic::Ordering::Relaxed), 0);
    }
}
