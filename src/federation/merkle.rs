//! Merkle Tree 对账
//!
//! 基于 blake3 的分片 Merkle Tree，用于联邦节点间的数据一致性对账。
//! 将 key 空间按 shard_count 分片，每片维护独立的根哈希。

use std::sync::Arc;

use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use serde::Serialize;

use crate::federation::protocol::MerkleDigestMessage;

/// Merkle Tree（分片式）
pub struct MerkleTree {
    /// 分片数
    shard_count: u16,
    /// 每片根哈希
    roots: RwLock<Vec<[u8; 32]>>,
    /// 每片条目数
    entry_counts: RwLock<Vec<u32>>,
    /// 所有条目 key -> data
    entries: RwLock<FxHashMap<Vec<u8>, Vec<u8>>>,
}

impl MerkleTree {
    /// 创建新 Merkle Tree
    pub fn new(shard_count: u16) -> Self {
        let count = shard_count.max(1) as usize;
        Self {
            shard_count: shard_count.max(1),
            roots: RwLock::new(vec![[0u8; 32]; count]),
            entry_counts: RwLock::new(vec![0u32; count]),
            entries: RwLock::new(FxHashMap::default()),
        }
    }

    /// 计算 key 所属分片
    fn shard_for_key(&self, key: &[u8]) -> u16 {
        let hash = blake3::hash(key);
        let bytes = hash.as_bytes();
        (u16::from_le_bytes([bytes[0], bytes[1]]) % self.shard_count as u16)
    }

    /// 重算指定分片的根哈希
    fn recompute_shard(&self, shard: u16) {
        let entries = self.entries.read();
        let mut shard_entries: Vec<(&Vec<u8>, &Vec<u8>)> = entries
            .iter()
            .filter(|(k, _)| self.shard_for_key(k) == shard)
            .collect();
        // 按 key 排序
        shard_entries.sort_by(|a, b| a.0.cmp(b.0));

        let mut hasher = blake3::Hasher::new();
        for (key, data) in &shard_entries {
            let key_hash = blake3::hash(key);
            let data_hash = blake3::hash(data);
            hasher.update(key_hash.as_bytes());
            hasher.update(data_hash.as_bytes());
        }
        let root = hasher.finalize();

        let mut roots = self.roots.write();
        roots[shard as usize] = *root.as_bytes();
        let mut counts = self.entry_counts.write();
        counts[shard as usize] = shard_entries.len() as u32;
    }

    /// 更新/插入条目
    pub fn update(&self, key: &[u8], data: &[u8]) {
        let shard = self.shard_for_key(key);
        self.entries
            .write()
            .insert(key.to_vec(), data.to_vec());
        self.recompute_shard(shard);
    }

    /// 删除条目
    pub fn remove(&self, key: &[u8]) {
        let shard = self.shard_for_key(key);
        if self.entries.write().remove(key).is_some() {
            self.recompute_shard(shard);
        }
    }

    /// 生成全量摘要
    pub fn digest(&self, repo_type: u8) -> MerkleDigestMessage {
        MerkleDigestMessage {
            repo_type,
            shard_count: self.shard_count,
            roots: self.roots.read().clone(),
            entry_counts: self.entry_counts.read().clone(),
        }
    }

    /// 对比找出根哈希不同的分片索引
    pub fn diff(&self, other: &MerkleDigestMessage) -> Vec<u16> {
        let roots = self.roots.read();
        let mut diffs = Vec::new();
        let count = self.shard_count.min(other.shard_count) as usize;
        for i in 0..count {
            if roots[i] != other.roots[i] {
                diffs.push(i as u16);
            }
        }
        // 如果分片数不同，额外的分片也算差异
        if self.shard_count > other.shard_count {
            for i in count..self.shard_count as usize {
                diffs.push(i as u16);
            }
        }
        diffs
    }

    /// 获取某分片所有条目
    pub fn get_shard_entries(&self, shard: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
        let entries = self.entries.read();
        entries
            .iter()
            .filter(|(k, _)| self.shard_for_key(k) == shard)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// 总条目数
    pub fn total_entries(&self) -> usize {
        self.entries.read().len()
    }

    /// 分片数
    pub fn shard_count(&self) -> u16 {
        self.shard_count
    }
}

/// Merkle 提供者 trait（用于 GossipEngine 的 anti-entropy 回调）
pub trait MerkleProvider: Send + Sync {
    fn get_digest(&self, repo_type: u8) -> MerkleDigestMessage;
    fn get_shard_entries(&self, repo_type: u8, shard: u16) -> Vec<(Vec<u8>, Vec<u8>)>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merkle_update_and_digest() {
        let mt = MerkleTree::new(256);
        mt.update(b"key1", b"data1");
        mt.update(b"key2", b"data2");

        let digest = mt.digest(1);
        assert_eq!(digest.repo_type, 1);
        assert_eq!(digest.shard_count, 256);
        assert!(digest.entry_counts.iter().sum::<u32>() >= 2);
    }

    #[test]
    fn test_merkle_diff() {
        let mt1 = MerkleTree::new(4);
        let mt2 = MerkleTree::new(4);

        mt1.update(b"key1", b"data1");
        mt2.update(b"key1", b"data1");
        // 相同数据，无差异
        let digest2 = mt2.digest(1);
        assert!(mt1.diff(&digest2).is_empty());

        // mt2 修改数据，应有差异
        mt2.update(b"key1", b"data2");
        let digest2 = mt2.digest(1);
        let diffs = mt1.diff(&digest2);
        assert!(!diffs.is_empty());
    }

    #[test]
    fn test_merkle_remove() {
        let mt = MerkleTree::new(4);
        mt.update(b"key1", b"data1");
        assert_eq!(mt.total_entries(), 1);
        mt.remove(b"key1");
        assert_eq!(mt.total_entries(), 0);
    }

    #[test]
    fn test_merkle_get_shard_entries() {
        let mt = MerkleTree::new(4);
        mt.update(b"key1", b"data1");
        mt.update(b"key2", b"data2");

        // 找到至少一个分片有条目
        let mut found = false;
        for shard in 0..4 {
            let entries = mt.get_shard_entries(shard);
            if !entries.is_empty() {
                found = true;
                assert!(entries[0].0 == b"key1" || entries[0].0 == b"key2");
            }
        }
        assert!(found);
    }

    #[test]
    fn test_merkle_deterministic() {
        let mt1 = MerkleTree::new(16);
        let mt2 = MerkleTree::new(16);

        mt1.update(b"a", b"1");
        mt1.update(b"b", b"2");
        mt2.update(b"b", b"2");
        mt2.update(b"a", b"1");

        let d1 = mt1.digest(1);
        let d2 = mt2.digest(1);
        assert_eq!(d1.roots, d2.roots);
    }

    #[test]
    fn test_merkle_shard_distribution() {
        let mt = MerkleTree::new(8);
        for i in 0..100 {
            let key = format!("key{}", i);
            mt.update(key.as_bytes(), b"data");
        }
        assert_eq!(mt.total_entries(), 100);
        // 条目应分布在多个分片
        let digest = mt.digest(1);
        let non_empty = digest.entry_counts.iter().filter(|&&c| c > 0).count();
        assert!(non_empty > 1);
    }
}
