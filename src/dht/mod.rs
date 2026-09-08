//! Kademlia DHT 路由表模块
//!
//! 实现标准 Kademlia K-Bucket 路由表：
//! - 160 个 bucket（对应 node_id 每一位）
//! - 每个 bucket 容量 K=8
//! - 按 XOR 距离组织节点
//! - 支持节点状态管理（Good/Questionable/Bad）

pub mod kbucket;
pub mod probe;
pub mod routing_table;

pub use kbucket::{KBucket, KBucketEntry, K};
pub use probe::DhtProbe;
pub use routing_table::RoutingTable;

/// 计算两个 20 字节 ID 的 XOR 距离
pub fn xor_distance(a: &[u8; 20], b: &[u8; 20]) -> [u8; 20] {
    let mut result = [0u8; 20];
    for i in 0..20 {
        result[i] = a[i] ^ b[i];
    }
    result
}

/// 计算 XOR 距离的最高有效位位置（0-159），用于确定 bucket 索引
/// 返回 0 表示距离为 0（相同 ID）
pub fn bucket_index_for_distance(distance: &[u8; 20]) -> usize {
    for i in 0..20 {
        if distance[i] != 0 {
            // 找到第一个非零字节，计算其最高位
            let leading_zeros = distance[i].leading_zeros() as usize;
            return i * 8 + leading_zeros;
        }
    }
    0 // 距离为 0
}

/// 比较两个距离，返回 true 如果 a < b
pub fn distance_less(a: &[u8; 20], b: &[u8; 20]) -> bool {
    for i in 0..20 {
        if a[i] < b[i] {
            return true;
        }
        if a[i] > b[i] {
            return false;
        }
    }
    false // 相等
}
