//! Availability（可用性）计算模块
//!
//! 计算 torrent 的可用性：所有分片是否都至少有一个 peer 拥有。
//! availability > 1.0 表示所有分片都有至少一个副本，可以完整下载。
//!
//! 【计算方法】
//! 1. 精确计算：连接 peer 获取 bitfield，聚合计算每个分片的副本数
//! 2. 估算计算：基于做种者数量和 peer 数量估算（做种者拥有所有分片）
//!
//! 【参考】Neglia et al. "Availability in BitTorrent Systems", Infocom 2007

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::debug;

use crate::types::Infohash;

/// 单个分片的可用性信息
#[derive(Debug, Clone)]
pub struct PieceAvailability {
    /// 分片索引
    pub piece_index: u32,
    /// 拥有该分片的 peer 数
    pub replica_count: u32,
    /// 是否可用（至少有1个副本）
    pub available: bool,
}

/// 单个 infohash 的可用性计算结果
#[derive(Debug, Clone)]
pub struct AvailabilityResult {
    /// infohash
    pub infohash: Infohash,
    /// 总分片数
    pub total_pieces: u32,
    /// 可用分片数（至少有1个副本）
    pub available_pieces: u32,
    /// 不可用分片数
    pub unavailable_pieces: u32,
    /// 可用性比例（0.0~1.0，1.0表示所有分片都可用）
    pub availability_ratio: f64,
    /// 平均副本数（所有分片的平均副本数量）
    pub avg_replicas: f64,
    /// 最少副本数（最稀有分片的副本数）
    pub min_replicas: u32,
    /// 做种者数（拥有所有分片的 peer）
    pub seeder_count: u32,
    /// 总 peer 数
    pub total_peers: u32,
    /// 计算时间
    pub calculated_at: Instant,
    /// 计算方式（精确/估算）
    pub method: AvailabilityMethod,
}

/// 可用性计算方式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvailabilityMethod {
    /// 精确计算（基于 bitfield 聚合）
    Exact,
    /// 估算计算（基于做种者数量和 peer 数量）
    Estimated,
}

impl AvailabilityMethod {
    pub fn label(&self) -> &'static str {
        match self {
            AvailabilityMethod::Exact => "精确",
            AvailabilityMethod::Estimated => "估算",
        }
    }
}

/// 单个 peer 的 bitfield（拥有的分片）
#[derive(Debug, Clone)]
struct PeerBitfield {
    /// peer 地址
    peer_addr: String,
    /// 拥有的分片索引集合
    pieces: Vec<u32>,
    /// 是否为做种者（拥有所有分片）
    is_seeder: bool,
    /// 最后更新时间
    last_seen: Instant,
}

/// Availability 计算器
pub struct AvailabilityCalculator {
    /// infohash -> 可用性结果缓存
    cache: DashMap<Infohash, AvailabilityResult>,
    /// infohash -> peer bitfield 集合（用于精确计算）
    bitfields: DashMap<Infohash, Vec<PeerBitfield>>,
    /// 缓存有效期（默认 300 秒=5分钟）
    cache_ttl: Duration,
    /// bitfield 有效期（默认 1800 秒=30分钟）
    bitfield_ttl: Duration,
}

impl AvailabilityCalculator {
    /// 创建新的可用性计算器
    pub fn new() -> Self {
        Self {
            cache: DashMap::new(),
            bitfields: DashMap::new(),
            cache_ttl: Duration::from_secs(300),
            bitfield_ttl: Duration::from_secs(1800),
        }
    }

    /// 记录一个 peer 的 bitfield（用于精确计算）
    pub fn record_bitfield(&self, infohash: Infohash, peer_addr: &str, pieces: Vec<u32>, total_pieces: u32) {
        let is_seeder = pieces.len() as u32 >= total_pieces;
        let bitfield = PeerBitfield {
            peer_addr: peer_addr.to_string(),
            pieces,
            is_seeder,
            last_seen: Instant::now(),
        };

        let mut entry = self.bitfields.entry(infohash).or_insert_with(Vec::new);
        // 移除同一 peer 的旧记录
        entry.retain(|p| p.peer_addr != peer_addr);
        entry.push(bitfield);
    }

    /// 记录一个做种者（简化方式，不需要完整 bitfield）
    pub fn record_seeder(&self, infohash: Infohash, peer_addr: &str, total_pieces: u32) {
        // 做种者拥有所有分片
        let pieces: Vec<u32> = (0..total_pieces).collect();
        self.record_bitfield(infohash, peer_addr, pieces, total_pieces);
    }

    /// 精确计算可用性（基于已记录的 bitfield）
    pub fn calculate_exact(&self, infohash: Infohash, total_pieces: u32) -> Option<AvailabilityResult> {
        let bitfields = self.bitfields.get(&infohash)?;
        if bitfields.is_empty() {
            return None;
        }

        // 清理过期的 bitfield
        let cutoff = Instant::now() - self.bitfield_ttl;
        let valid_bitfields: Vec<&PeerBitfield> = bitfields
            .iter()
            .filter(|p| p.last_seen >= cutoff)
            .collect();

        if valid_bitfields.is_empty() {
            return None;
        }

        // 聚合每个分片的副本数
        let mut piece_replicas: HashMap<u32, u32> = HashMap::new();
        let mut seeder_count = 0u32;

        for bf in &valid_bitfields {
            if bf.is_seeder {
                seeder_count += 1;
                // 做种者拥有所有分片
                for i in 0..total_pieces {
                    *piece_replicas.entry(i).or_insert(0) += 1;
                }
            } else {
                for &piece in &bf.pieces {
                    *piece_replicas.entry(piece).or_insert(0) += 1;
                }
            }
        }

        // 统计可用分片
        let mut available_pieces = 0u32;
        let mut min_replicas = u32::MAX;
        let mut total_replicas = 0u64;

        for i in 0..total_pieces {
            let replicas = *piece_replicas.get(&i).unwrap_or(&0);
            total_replicas += replicas as u64;
            min_replicas = min_replicas.min(replicas);
            if replicas > 0 {
                available_pieces += 1;
            }
        }

        if min_replicas == u32::MAX {
            min_replicas = 0;
        }

        let unavailable_pieces = total_pieces - available_pieces;
        let availability_ratio = if total_pieces > 0 {
            available_pieces as f64 / total_pieces as f64
        } else {
            0.0
        };
        let avg_replicas = if total_pieces > 0 {
            total_replicas as f64 / total_pieces as f64
        } else {
            0.0
        };

        let result = AvailabilityResult {
            infohash,
            total_pieces,
            available_pieces,
            unavailable_pieces,
            availability_ratio,
            avg_replicas,
            min_replicas,
            seeder_count,
            total_peers: valid_bitfields.len() as u32,
            calculated_at: Instant::now(),
            method: AvailabilityMethod::Exact,
        };

        // 写入缓存
        self.cache.insert(infohash, result.clone());

        Some(result)
    }

    /// 估算可用性（基于做种者数量和 peer 数量）
    ///
    /// 【估算模型】
    /// - 如果有做种者，availability = 1.0（做种者拥有所有分片）
    /// - 如果没有做种者，基于 peer 数量和随机分布估算
    /// - 使用概率模型：P(分片可用) = 1 - (1 - p)^n，其中 p=平均每个peer拥有的分片比例
    pub fn estimate(
        &self,
        infohash: Infohash,
        total_pieces: u32,
        seeder_count: u32,
        total_peers: u32,
    ) -> AvailabilityResult {
        // 检查缓存
        if let Some(cached) = self.cache.get(&infohash) {
            if cached.calculated_at.elapsed() < self.cache_ttl {
                return cached.clone();
            }
        }

        let availability_ratio = if seeder_count > 0 {
            // 有做种者，所有分片都可用
            1.0
        } else if total_peers == 0 || total_pieces == 0 {
            0.0
        } else {
            // 无做种者，使用概率模型估算
            // 假设每个 peer 平均拥有 50% 的分片（保守估计）
            let avg_piece_ratio: f64 = 0.5;
            // P(单个分片不可用) = (1 - avg_piece_ratio)^total_peers
            let base: f64 = 1.0 - avg_piece_ratio;
            let p_unavailable = base.powi(total_peers as i32);
            // P(分片可用) = 1 - P(不可用)
            let p_available: f64 = 1.0 - p_unavailable;
            p_available.min(1.0_f64).max(0.0_f64)
        };

        let available_pieces = (total_pieces as f64 * availability_ratio) as u32;
        let unavailable_pieces = total_pieces - available_pieces;

        // 估算平均副本数
        let avg_replicas = if total_pieces > 0 {
            (seeder_count as f64) + (total_peers.saturating_sub(seeder_count) as f64 * 0.5)
        } else {
            0.0
        };

        // 估算最少副本数
        let min_replicas = if seeder_count > 0 {
            seeder_count
        } else if total_peers > 0 {
            // 无做种者时，最少副本数可能为0（稀有分片）
            0
        } else {
            0
        };

        let result = AvailabilityResult {
            infohash,
            total_pieces,
            available_pieces,
            unavailable_pieces,
            availability_ratio,
            avg_replicas,
            min_replicas,
            seeder_count,
            total_peers,
            calculated_at: Instant::now(),
            method: AvailabilityMethod::Estimated,
        };

        // 写入缓存
        self.cache.insert(infohash, result.clone());

        result
    }

    /// 获取缓存的可用性结果
    pub fn get_cached(&self, infohash: &Infohash) -> Option<AvailabilityResult> {
        let result = self.cache.get(infohash)?;
        if result.calculated_at.elapsed() > self.cache_ttl {
            return None;
        }
        Some(result.clone())
    }

    /// 清理过期缓存和 bitfield
    pub fn cleanup_expired(&self) -> usize {
        let mut removed = 0;

        // 清理缓存
        let cache_cutoff = Instant::now() - self.cache_ttl;
        self.cache.retain(|_, v| {
            if v.calculated_at < cache_cutoff {
                removed += 1;
                false
            } else {
                true
            }
        });

        // 清理 bitfield
        let bf_cutoff = Instant::now() - self.bitfield_ttl;
        self.bitfields.retain(|_, v| {
            v.retain(|p| p.last_seen >= bf_cutoff);
            !v.is_empty()
        });

        removed
    }

    /// 获取缓存中的可用性结果数量
    pub fn cached_count(&self) -> usize {
        self.cache.len()
    }

    /// 获取记录了 bitfield 的 infohash 数量
    pub fn tracked_infohash_count(&self) -> usize {
        self.bitfields.len()
    }
}

impl Default for AvailabilityCalculator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_with_seeders() {
        let calc = AvailabilityCalculator::new();
        let infohash = [1u8; 20];

        let result = calc.estimate(infohash, 100, 5, 10);
        assert_eq!(result.availability_ratio, 1.0);
        assert_eq!(result.available_pieces, 100);
        assert_eq!(result.unavailable_pieces, 0);
        assert_eq!(result.seeder_count, 5);
        assert_eq!(result.total_peers, 10);
        assert_eq!(result.method, AvailabilityMethod::Estimated);
    }

    #[test]
    fn test_estimate_no_peers() {
        let calc = AvailabilityCalculator::new();
        let infohash = [2u8; 20];

        let result = calc.estimate(infohash, 100, 0, 0);
        assert_eq!(result.availability_ratio, 0.0);
        assert_eq!(result.available_pieces, 0);
        assert_eq!(result.unavailable_pieces, 100);
    }

    #[test]
    fn test_estimate_without_seeders() {
        let calc = AvailabilityCalculator::new();
        let infohash = [3u8; 20];

        // 10个peer，无做种者
        let result = calc.estimate(infohash, 100, 0, 10);
        // 概率模型：P(可用) = 1 - 0.5^10 ≈ 0.999
        assert!(result.availability_ratio > 0.9);
        assert!(result.availability_ratio <= 1.0);
    }

    #[test]
    fn test_exact_calculation() {
        let calc = AvailabilityCalculator::new();
        let infohash = [4u8; 20];

        // 记录2个做种者
        calc.record_seeder(infohash, "1.1.1.1:6881", 10);
        calc.record_seeder(infohash, "2.2.2.2:6881", 10);

        // 记录1个部分peer（拥有分片0-4）
        calc.record_bitfield(infohash, "3.3.3.3:6881", vec![0, 1, 2, 3, 4], 10);

        let result = calc.calculate_exact(infohash, 10).unwrap();
        assert_eq!(result.total_pieces, 10);
        assert_eq!(result.available_pieces, 10);
        assert_eq!(result.unavailable_pieces, 0);
        assert_eq!(result.availability_ratio, 1.0);
        assert_eq!(result.seeder_count, 2);
        assert_eq!(result.total_peers, 3);
        assert_eq!(result.method, AvailabilityMethod::Exact);
        // 平均副本数：做种者每个分片2个副本 + 部分peer前5个分片各1个 = (2*10 + 1*5)/10 = 2.5
        assert!((result.avg_replicas - 2.5).abs() < 0.01);
    }

    #[test]
    fn test_exact_calculation_missing_pieces() {
        let calc = AvailabilityCalculator::new();
        let infohash = [5u8; 20];

        // 只记录1个部分peer（拥有分片0-4）
        calc.record_bitfield(infohash, "1.1.1.1:6881", vec![0, 1, 2, 3, 4], 10);

        let result = calc.calculate_exact(infohash, 10).unwrap();
        assert_eq!(result.available_pieces, 5);
        assert_eq!(result.unavailable_pieces, 5);
        assert_eq!(result.availability_ratio, 0.5);
        assert_eq!(result.min_replicas, 0);
    }

    #[test]
    fn test_cache() {
        let calc = AvailabilityCalculator::new();
        let infohash = [6u8; 20];

        assert!(calc.get_cached(&infohash).is_none());

        let result = calc.estimate(infohash, 100, 1, 5);
        assert!(calc.get_cached(&infohash).is_some());
        assert_eq!(calc.cached_count(), 1);
    }

    #[test]
    fn test_availability_method_label() {
        assert_eq!(AvailabilityMethod::Exact.label(), "精确");
        assert_eq!(AvailabilityMethod::Estimated.label(), "估算");
    }

    #[test]
    fn test_piece_availability() {
        let pa = PieceAvailability {
            piece_index: 5,
            replica_count: 3,
            available: true,
        };
        assert_eq!(pa.piece_index, 5);
        assert_eq!(pa.replica_count, 3);
        assert!(pa.available);
    }
}
