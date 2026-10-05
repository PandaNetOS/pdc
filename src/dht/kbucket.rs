//! K-Bucket 实现
//!
//! 每个 K-Bucket 保存最多 K 个节点，按最后活跃时间排序。
//! 新节点插入到尾部，活跃节点移到尾部，最不活跃的在头部。

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
/// Kademlia K 值：每个 bucket 最多保存的节点数
/// 从 8 提升到 16，增加路由表容量（Phase 11 协议优化）
pub const K: usize = 16;

/// 节点进入 Questionable 状态的空闲时间阈值（15 分钟）
const QUESTIONABLE_IDLE_THRESHOLD: Duration = Duration::from_secs(900);

/// 节点状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// 健康：最近有成功通信
    Good,
    /// 可疑：超过 15 分钟无通信
    Questionable,
    /// 不健康：连续失败 ≥3 次
    Bad,
}

/// K-Bucket 中的节点条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KBucketEntry {
    /// 节点 ID（20 字节）
    pub id: [u8; 20],
    /// 节点地址
    pub addr: SocketAddr,
    /// 最后一次活跃时间
    #[serde(with = "instant_serializer")]
    pub last_active: Instant,
    /// 最后一次查询时间（用于统计时效性衰减，不持久化）
    #[serde(skip)]
    pub last_query_time: Option<Instant>,
    /// 最后一次被访问/选中时间（内存字段，不持久化，用于冷热分层）
    #[serde(skip)]
    pub last_accessed: Option<Instant>,
    /// 最后一次**已验证**活跃时间（本会话内收到该节点的响应；内存字段，不持久化）。
    /// 与 last_active（任何接触，含"在别人响应中被提及"）分离——
    /// mention 刷新 last_active 不再伪造活性（2026-10 R5）。
    #[serde(skip)]
    pub last_verified: Option<Instant>,
    /// 最后一次被提及时间（仅出现在别人响应里，未验证；内存字段，不持久化）。
    /// 用于未验证先验分的弱新鲜加成，不参与状态翻转（2026-10 D3）。
    #[serde(skip)]
    pub last_mentioned: Option<Instant>,
    /// 近期窗口统计（指数衰减近似 EMA；内存字段，不持久化）。
    /// 终身累计 success/query 会被历史稀释，近期窗口让连续失败立即反映到评分（2026-10 R4）。
    #[serde(skip)]
    pub recent_query: f64,
    #[serde(skip)]
    pub recent_success: f64,
    #[serde(skip)]
    pub recent_updated: Option<Instant>,
    /// 连续失败次数
    pub consecutive_failures: u32,
    /// 总查询次数
    pub query_count: u64,
    /// 成功查询次数
    pub success_count: u64,
    /// 累计延迟（毫秒）
    pub total_latency_ms: u64,
    /// 累计返回节点数（用于节点产出维度评分）
    #[serde(default)]
    pub nodes_returned: u64,
    /// 节点状态
    pub state: NodeState,
    /// 综合评分（0-100），Phase 3 填充
    #[serde(default)]
    pub score: f64,
}

mod instant_serializer {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::{Duration, Instant, SystemTime};

    pub fn serialize<S: Serializer>(instant: &Instant, serializer: S) -> Result<S::Ok, S::Error> {
        // 序列化为相对于 UNIX_EPOCH 的毫秒数（近似）
        let duration = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            - instant.elapsed();
        serializer.serialize_u64(duration.as_millis() as u64)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Instant, D::Error> {
        let millis = u64::deserialize(deserializer)?;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let elapsed = now.as_millis() as u64 - millis;
        Ok(crate::utils::cutoff_before(Duration::from_millis(elapsed)))
    }
}

impl KBucketEntry {
    pub fn new(id: [u8; 20], addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            last_active: Instant::now(),
            last_query_time: None,
            last_accessed: None,
            last_verified: None,
            last_mentioned: None,
            recent_query: 0.0,
            recent_success: 0.0,
            recent_updated: None,
            consecutive_failures: 0,
            query_count: 0,
            success_count: 0,
            total_latency_ms: 0,
            nodes_returned: 0,
            state: NodeState::Good,
            score: 0.0,
        }
    }

    /// 近期窗口指数衰减因子（alpha 为每小时衰减速率）
    pub fn decay_factor(alpha_per_hour: f64, elapsed: Duration) -> f64 {
        (-alpha_per_hour * elapsed.as_secs_f64() / 3600.0).exp()
    }

    /// 近期窗口响应率（优先于终身统计）：
    /// 返回 `Some(rate)` 当衰减后有效样本数 ≥ min_samples，否则 None（回落终身值）。
    pub fn windowed_response_rate(&self, alpha: f64, min_samples: f64) -> Option<f64> {
        if self.recent_query <= 0.0 {
            return None;
        }
        let decay = self
            .recent_updated
            .map(|t| Self::decay_factor(alpha, t.elapsed()))
            .unwrap_or(1.0);
        let effective = self.recent_query * decay;
        if effective < min_samples {
            return None;
        }
        Some(self.recent_success * decay / effective)
    }

    /// 记录一次成功查询
    pub fn record_success(&mut self, latency_ms: u64) {
        self.last_active = Instant::now();
        self.last_query_time = Some(Instant::now());
        self.consecutive_failures = 0;
        self.query_count += 1;
        self.success_count += 1;
        self.total_latency_ms += latency_ms;
        self.state = NodeState::Good;
    }

    /// 记录一次成功查询及返回的节点数
    pub fn record_success_with_nodes(&mut self, latency_ms: u64, nodes_returned: u64) {
        self.record_success(latency_ms);
        self.nodes_returned += nodes_returned;
    }

    /// 记录一次失败查询
    pub fn record_failure(&mut self) {
        self.last_query_time = Some(Instant::now());
        self.consecutive_failures += 1;
        self.query_count += 1;
        if self.consecutive_failures >= 3 {
            self.state = NodeState::Bad;
        }
    }

    /// 平均每次查询返回的节点数
    pub fn avg_nodes_returned(&self) -> f64 {
        if self.query_count == 0 {
            0.0
        } else {
            self.nodes_returned as f64 / self.query_count as f64
        }
    }

    /// 刷新状态（基于最后活跃时间）
    pub fn refresh_state(&mut self) {
        if self.state == NodeState::Bad {
            return;
        }
        if self.last_active.elapsed() > QUESTIONABLE_IDLE_THRESHOLD {
            self.state = NodeState::Questionable;
        } else {
            self.state = NodeState::Good;
        }
    }

    /// 平均响应时间（毫秒）
    pub fn avg_latency_ms(&self) -> f64 {
        if self.success_count == 0 {
            0.0
        } else {
            self.total_latency_ms as f64 / self.success_count as f64
        }
    }

    /// 成功率
    pub fn success_rate(&self) -> f64 {
        if self.query_count == 0 {
            0.0
        } else {
            self.success_count as f64 / self.query_count as f64
        }
    }
}

/// K-Bucket：保存最多 K 个节点
#[derive(Debug, Clone)]
pub struct KBucket {
    entries: Vec<KBucketEntry>,
}

impl KBucket {
    pub fn new() -> Self {
        Self {
            entries: Vec::with_capacity(K),
        }
    }

    /// 插入或更新节点。返回 true 如果是新节点，false 如果是更新已有节点。
    /// 如果 bucket 已满且最老的节点是 Good，拒绝插入（返回 false）。
    /// 如果 bucket 已满且最老的节点是 Bad/Questionable，替换它。
    pub fn insert(&mut self, entry: KBucketEntry) -> bool {
        // 检查是否已存在（按地址去重）
        if let Some(pos) = self.entries.iter().position(|e| e.addr == entry.addr) {
            // 更新已有节点：保留统计，更新活跃时间
            let existing = &mut self.entries[pos];
            existing.last_active = entry.last_active;
            existing.id = entry.id;
            // 移到尾部（最近活跃）
            if pos < self.entries.len() - 1 {
                let entry = self.entries.remove(pos);
                self.entries.push(entry);
            }
            return false;
        }

        // 新节点
        if self.entries.len() < K {
            self.entries.push(entry);
            true
        } else {
            // bucket 已满，检查最老的节点（头部）
            if let Some(oldest) = self.entries.first_mut() {
                oldest.refresh_state();
                if oldest.state != NodeState::Good {
                    // 替换不健康的节点
                    self.entries.remove(0);
                    self.entries.push(entry);
                    true
                } else {
                    // 最老节点是 Good，拒绝插入
                    false
                }
            } else {
                self.entries.push(entry);
                true
            }
        }
    }

    /// 移除指定地址的节点
    pub fn remove(&mut self, addr: SocketAddr) -> bool {
        if let Some(pos) = self.entries.iter().position(|e| e.addr == addr) {
            self.entries.remove(pos);
            true
        } else {
            false
        }
    }

    /// 查找指定地址的节点
    pub fn find_by_addr(&self, addr: SocketAddr) -> Option<&KBucketEntry> {
        self.entries.iter().find(|e| e.addr == addr)
    }

    /// 查找指定地址的节点（可变）
    pub fn find_by_addr_mut(&mut self, addr: SocketAddr) -> Option<&mut KBucketEntry> {
        self.entries.iter_mut().find(|e| e.addr == addr)
    }

    /// 返回离 target 最近的 n 个节点（按 XOR 距离升序）
    pub fn closest(&self, target: &[u8; 20], n: usize) -> Vec<KBucketEntry> {
        let mut sorted: Vec<KBucketEntry> = self.entries.clone();
        sorted.sort_by(|a, b| {
            let da = crate::dht::xor_distance(&a.id, target);
            let db = crate::dht::xor_distance(&b.id, target);
            if crate::dht::distance_less(&da, &db) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        });
        sorted.truncate(n);
        sorted
    }

    /// 刷新所有节点状态，移除 Bad 节点
    pub fn refresh(&mut self) {
        for entry in self.entries.iter_mut() {
            entry.refresh_state();
        }
        self.entries.retain(|e| e.state != NodeState::Bad);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[KBucketEntry] {
        &self.entries
    }

    pub fn entries_mut(&mut self) -> &mut [KBucketEntry] {
        &mut self.entries
    }

    /// 按评分降序返回节点
    pub fn by_score_desc(&self) -> Vec<KBucketEntry> {
        let mut sorted = self.entries.clone();
        sorted.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        sorted
    }
}

impl Default for KBucket {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(id_byte: u8, port: u16) -> KBucketEntry {
        let mut id = [0u8; 20];
        id[19] = id_byte;
        KBucketEntry::new(id, SocketAddr::from(([127, 0, 0, 1], port)))
    }

    #[test]
    fn test_insert_and_dup() {
        let mut bucket = KBucket::new();
        assert!(bucket.insert(make_entry(1, 1001)));
        assert!(!bucket.insert(make_entry(1, 1001))); // 重复地址
        assert_eq!(bucket.len(), 1);
    }

    #[test]
    fn test_bucket_overflow_good_rejected() {
        let mut bucket = KBucket::new();
        for i in 0..K {
            assert!(bucket.insert(make_entry(i as u8, 1000 + i as u16)));
        }
        assert_eq!(bucket.len(), K);
        // 所有节点都是 Good，新节点被拒绝
        assert!(!bucket.insert(make_entry(99, 1099)));
        assert_eq!(bucket.len(), K);
    }

    #[test]
    fn test_bucket_overflow_bad_replaced() {
        let mut bucket = KBucket::new();
        for i in 0..K {
            let mut e = make_entry(i as u8, 1000 + i as u16);
            if i == 0 {
                e.consecutive_failures = 5;
                e.state = NodeState::Bad;
            }
            bucket.entries.push(e);
        }
        assert_eq!(bucket.len(), K);
        // Bad 节点被替换
        assert!(bucket.insert(make_entry(99, 1099)));
        assert_eq!(bucket.len(), K);
        assert!(bucket
            .find_by_addr(SocketAddr::from(([127, 0, 0, 1], 1099)))
            .is_some());
    }

    #[test]
    fn test_closest_sorting() {
        let mut bucket = KBucket::new();
        bucket.insert(make_entry(0x01, 1001));
        bucket.insert(make_entry(0x0F, 1002));
        bucket.insert(make_entry(0xF0, 1003));

        let target = [0u8; 20];
        let closest = bucket.closest(&target, 2);
        assert_eq!(closest.len(), 2);
        assert_eq!(closest[0].addr.port(), 1001); // 0x01 离 0 最近
    }

    #[test]
    fn test_record_success_failure() {
        let mut entry = make_entry(1, 1001);
        entry.record_success(50);
        entry.record_success(100);
        assert_eq!(entry.success_count, 2);
        assert_eq!(entry.avg_latency_ms(), 75.0);
        assert_eq!(entry.state, NodeState::Good);

        entry.record_failure();
        entry.record_failure();
        entry.record_failure();
        assert_eq!(entry.state, NodeState::Bad);
    }

    #[test]
    fn test_decay_factor_math() {
        // alpha=0 → 不衰减；alpha=0.3、1 小时 → exp(-0.3) ≈ 0.7408
        assert!((KBucketEntry::decay_factor(0.0, Duration::from_secs(3600)) - 1.0).abs() < 1e-9);
        let d = KBucketEntry::decay_factor(0.3, Duration::from_secs(3600));
        assert!((d - (-0.3f64).exp()).abs() < 1e-9);
    }

    #[test]
    fn test_windowed_response_rate_decay_invariant() {
        // 衰减因子同时作用于分子分母 → 比值不变；有效样本数受衰减限制
        let mut e = make_entry(2, 1002);
        assert_eq!(e.windowed_response_rate(0.3, 1.0), None, "无样本返回 None");

        e.recent_query = 4.0;
        e.recent_success = 3.0;
        e.recent_updated = Some(Instant::now());
        let r = e.windowed_response_rate(0.3, 1.0).unwrap();
        assert!((r - 0.75).abs() < 1e-9);

        // 距上次更新很久 → 有效样本衰减到阈值以下 → 回落 None
        e.recent_updated = Some(crate::utils::cutoff_before(Duration::from_secs(48 * 3600)));
        assert_eq!(
            e.windowed_response_rate(0.3, 1.0),
            None,
            "有效样本 = 4×exp(-14.4) ≈ 0.003 < 1"
        );
    }
}
