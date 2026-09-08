//! Kademlia 路由表
//!
//! 160 个 K-Bucket，按 node_id 的 XOR 距离分桶。

use std::net::SocketAddr;

use super::kbucket::{KBucket, KBucketEntry};
use super::{bucket_index_for_distance, xor_distance};

/// Kademlia 路由表：160 个 bucket
#[derive(Debug, Clone)]
pub struct RoutingTable {
    /// 本地节点 ID
    node_id: [u8; 20],
    /// 160 个 bucket
    buckets: Vec<KBucket>,
}

impl RoutingTable {
    /// 创建新路由表
    pub fn new(node_id: [u8; 20]) -> Self {
        let buckets = (0..160).map(|_| KBucket::new()).collect();
        Self { node_id, buckets }
    }

    /// 本地节点 ID
    pub fn node_id(&self) -> &[u8; 20] {
        &self.node_id
    }

    /// 计算目标 ID 对应的 bucket 索引
    pub fn bucket_index(&self, target: &[u8; 20]) -> usize {
        let distance = xor_distance(&self.node_id, target);
        bucket_index_for_distance(&distance)
    }

    /// 添加节点。返回 true 如果是新节点。
    pub fn add_node(&mut self, id: [u8; 20], addr: SocketAddr) -> bool {
        let idx = self.bucket_index(&id);
        let entry = KBucketEntry::new(id, addr);
        self.buckets[idx].insert(entry)
    }

    /// 添加已有条目（带统计信息）
    pub fn add_entry(&mut self, entry: KBucketEntry) -> bool {
        let idx = self.bucket_index(&entry.id);
        self.buckets[idx].insert(entry)
    }

    /// 移除节点
    pub fn remove_node(&mut self, addr: SocketAddr) -> bool {
        for bucket in self.buckets.iter_mut() {
            if bucket.remove(addr) {
                return true;
            }
        }
        false
    }

    /// 查找指定地址的节点
    pub fn find_by_addr(&self, addr: SocketAddr) -> Option<&KBucketEntry> {
        for bucket in self.buckets.iter() {
            if let Some(entry) = bucket.find_by_addr(addr) {
                return Some(entry);
            }
        }
        None
    }

    /// 查找指定地址的节点（可变）
    pub fn find_by_addr_mut(&mut self, addr: SocketAddr) -> Option<&mut KBucketEntry> {
        for bucket in self.buckets.iter_mut() {
            if let Some(entry) = bucket.find_by_addr_mut(addr) {
                return Some(entry);
            }
        }
        None
    }

    /// 返回离 target 最近的 n 个节点（跨 bucket 搜索）
    pub fn find_closest(&self, target: &[u8; 20], n: usize) -> Vec<KBucketEntry> {
        let mut all: Vec<KBucketEntry> = Vec::new();
        for bucket in self.buckets.iter() {
            all.extend(bucket.entries().iter().cloned());
        }
        all.sort_by(|a, b| {
            let da = xor_distance(&a.id, target);
            let db = xor_distance(&b.id, target);
            if super::distance_less(&da, &db) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        });
        all.truncate(n);
        all
    }

    /// 返回所有节点
    pub fn all_nodes(&self) -> Vec<KBucketEntry> {
        let mut all = Vec::new();
        for bucket in self.buckets.iter() {
            all.extend(bucket.entries().iter().cloned());
        }
        all
    }

    /// 按状态统计节点数
    pub fn count_by_state(&self) -> (usize, usize, usize) {
        let mut good = 0;
        let mut questionable = 0;
        let mut bad = 0;
        for bucket in self.buckets.iter() {
            for entry in bucket.entries() {
                match entry.state {
                    super::kbucket::NodeState::Good => good += 1,
                    super::kbucket::NodeState::Questionable => questionable += 1,
                    super::kbucket::NodeState::Bad => bad += 1,
                }
            }
        }
        (good, questionable, bad)
    }

    /// 总节点数
    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bucket 分布统计：非空bucket数 / 最满bucket节点数 / 满bucket数
    pub fn buckets_summary(&self) -> String {
        let non_empty = self.buckets.iter().filter(|b| !b.entries().is_empty()).count();
        let max_fill = self.buckets.iter().map(|b| b.len()).max().unwrap_or(0);
        let full = self.buckets.iter().filter(|b| b.len() >= super::kbucket::K).count();
        format!("非空{}/160 最满{}/K 满{}", non_empty, max_fill, full)
    }

    /// 获取指定 bucket（只读）
    pub fn get_bucket(&self, index: usize) -> Option<&KBucket> {
        self.buckets.get(index)
    }

    /// 返回所有非空 bucket 的随机 target ID（用于 bucket 刷新）
    ///
    /// 每个 target 落在对应 bucket 的 ID 空间范围内，
    /// 即 target 与本地 node_id 的 XOR 距离最高位等于 bucket 索引。
    pub fn non_empty_bucket_targets(&self) -> Vec<[u8; 20]> {
        let mut targets = Vec::new();
        for (idx, bucket) in self.buckets.iter().enumerate() {
            if bucket.entries().is_empty() {
                continue;
            }
            // bucket 0 是自身 ID，不需要刷新
            if idx == 0 {
                continue;
            }
            // 生成落在 bucket idx 范围内的随机 target
            // 距离最高位是 bit (160 - idx)，即字节 (160-idx)/8，位 (160-idx)%8
            let bit_pos = 160 - idx; // 0-159，0 是最高位
            let byte_idx = bit_pos / 8;
            let bit_idx = 7 - (bit_pos % 8); // 转换为字节内位索引（0=最高位）

            let mut target = self.node_id;
            // 高于 bit_pos 的位保持与 node_id 相同（即距离为 0）
            // bit_pos 位设为与 node_id 不同
            target[byte_idx] ^= 1 << bit_idx;
            // 低于 bit_pos 的位随机
            for b in &mut target[byte_idx + 1..] {
                *b = rand::random();
            }
            // 当前字节的低位也随机
            let mask = (1 << bit_idx) - 1;
            target[byte_idx] = (target[byte_idx] & !mask) | (rand::random::<u8>() & mask);

            targets.push(target);
        }
        targets
    }

    /// 获取指定 bucket（可变）
    pub fn get_bucket_mut(&mut self, index: usize) -> Option<&mut KBucket> {
        self.buckets.get_mut(index)
    }

    /// 刷新所有 bucket（更新状态、移除 Bad）
    pub fn refresh(&mut self) {
        for bucket in self.buckets.iter_mut() {
            bucket.refresh();
        }
    }

    /// 按评分降序返回 top n 节点
    pub fn top_nodes_by_score(&self, n: usize) -> Vec<KBucketEntry> {
        let mut all = self.all_nodes();
        all.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        all.truncate(n);
        all
    }

    /// 序列化为 JSON（用于持久化）
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        let nodes = self.all_nodes();
        serde_json::to_string_pretty(&nodes)
    }

    /// 从 JSON 反序列化加载
    pub fn from_json(node_id: [u8; 20], json: &str) -> Result<Self, serde_json::Error> {
        let entries: Vec<KBucketEntry> = serde_json::from_str(json)?;
        let mut table = Self::new(node_id);
        for entry in entries {
            table.add_entry(entry);
        }
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::kbucket::K;

    #[test]
    fn test_bucket_index() {
        let node_id = [0u8; 20];
        let table = RoutingTable::new(node_id);

        // 距离为 0 → bucket 0
        assert_eq!(table.bucket_index(&[0u8; 20]), 0);

        // 最后一位为 1 → bucket 159
        let mut target = [0u8; 20];
        target[19] = 0x01;
        assert_eq!(table.bucket_index(&target), 159);

        // 第一位最高位为 1 → bucket 0
        let mut target = [0u8; 20];
        target[0] = 0x80;
        assert_eq!(table.bucket_index(&target), 0);
    }

    #[test]
    fn test_add_and_find() {
        let node_id = [0u8; 20];
        let mut table = RoutingTable::new(node_id);

        let mut id1 = [0u8; 20];
        id1[19] = 0x01;
        let addr1 = SocketAddr::from(([127, 0, 0, 1], 1001));

        assert!(table.add_node(id1, addr1));
        assert!(!table.add_node(id1, addr1)); // 重复
        assert_eq!(table.len(), 1);
        assert!(table.find_by_addr(addr1).is_some());
    }

    #[test]
    fn test_find_closest() {
        let node_id = [0u8; 20];
        let mut table = RoutingTable::new(node_id);

        for i in 0..20u8 {
            let mut id = [0u8; 20];
            id[19] = i;
            table.add_node(id, SocketAddr::from(([127, 0, 0, 1], 1000 + i as u16)));
        }

        let target = [0u8; 20];
        let closest = table.find_closest(&target, 5);
        assert_eq!(closest.len(), 5);
        assert_eq!(closest[0].addr.port(), 1000); // id=0 最近
    }

    #[test]
    fn test_json_roundtrip() {
        let node_id = [0u8; 20];
        let mut table = RoutingTable::new(node_id);

        let mut id = [0u8; 20];
        id[19] = 0x42;
        table.add_node(id, SocketAddr::from(([127, 0, 0, 1], 1234)));

        let json = table.to_json().unwrap();
        let loaded = RoutingTable::from_json(node_id, &json).unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(loaded.find_by_addr(SocketAddr::from(([127, 0, 0, 1], 1234))).is_some());
    }

    #[test]
    fn test_bucket_capacity_k() {
        let node_id = [0u8; 20];
        let mut table = RoutingTable::new(node_id);

        // id[19] = 64..71，XOR 距离最高位都是 bit 6（值 64），同一 bucket 153
        for i in 0..K {
            let mut id = [0u8; 20];
            id[19] = 64 + i as u8;
            table.add_node(id, SocketAddr::from(([127, 0, 0, 1], 1000 + i as u16)));
        }
        // 同一 bucket 第 K+1 个 Good 节点被拒绝
        let mut id = [0u8; 20];
        id[19] = 72; // 也在 64-127 范围，同一 bucket
        assert!(!table.add_node(id, SocketAddr::from(([127, 0, 0, 1], 1099))));
    }
}
