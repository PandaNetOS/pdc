//! 分层 Merkle Tree（L0根 + L1 256分片 + L2 65536子分片）
//!
//! 三层架构（2026-09-18 亿级数据架构升级）：
//! - **L0**：根哈希（1个）= blake3(256个L1哈希拼接)
//! - **L1**：一级分片哈希（256个）= blake3(其下256个L2哈希拼接)
//! - **L2**：二级分片哈希（65536个）= 从该子分片数据条目按key排序后累加计算
//!
//! # Key 到分片的映射
//!
//! 取 blake3(key) 前两字节 [b0, b1]：
//! - L2 分片 = b0 * 256 + b1（范围 0..65535）
//! - L1 分片 = L2 / 256 = b0（范围 0..255）
//!
//! L1 映射与旧版 `shard_for_key` 完全一致（`u16::from_le_bytes([b0,b1]) % 256 = b0`），
//! 因此 DB 分片索引（256）无需变更，L2 是纯逻辑子分片。
//!
//! # DB 驱动架构
//!
//! Merkle 哈希直接从数据库数据计算，不依赖内存热数据：
//! - 写入路径只标记 dirty L2 分片，不做实时重算
//! - 后台增量任务（每10s）取出 dirty L2，按 L1 分组后从 DB 加载数据重算
//! - 全量兜底任务（每5min）重算所有分片
//! - 重算时自动向上传播：L2 → L1 → L0
//!
//! # 内存占用
//!
//! L2 哈希：65536 × 32B = 2MB；L2 计数：65536 × 4B = 256KB；
//! L1 哈希+计数：~9KB。总计 ~2.3MB，亿级数据下完全可控。
//!
//! # 叶子公式（保持不变）
//!
//! `blake3(key) ++ data_hash`，按 key 排序后累加。

#![allow(clippy::type_complexity)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::federation::protocol::MerkleDigestMessage;

/// 每个 L1 分片下的 L2 分片数量
pub const L2_PER_L1: u16 = 256;

/// 分层哈希状态（L2 + L1 + L0 + 计数，单一锁保护）
///
/// 纯 DB 驱动：本状态只缓存 L2/L1/L0 的哈希与计数（约 2.3MB），
/// **不**缓存任何 (key, data_hash) 明细。写入路径只标记 dirty L2，
/// 后台任务从 DB 加载该 L2 数据重算哈希，避免全量明细常驻内存。
struct HashState {
    /// L2 二级分片哈希（size = shard_count * L2_PER_L1）
    l2_hashes: Vec<[u8; 32]>,
    /// L2 二级分片条目数
    l2_counts: Vec<u32>,
    /// L1 一级分片哈希（size = shard_count，向后兼容 shard_roots）
    l1_hashes: Vec<[u8; 32]>,
    /// L1 一级分片条目数（向后兼容 shard_entry_counts）
    l1_counts: Vec<u32>,
    /// L0 根哈希
    root: [u8; 32],
}

impl HashState {
    fn new(shard_count: usize) -> Self {
        let l2_count = shard_count * L2_PER_L1 as usize;
        Self {
            l2_hashes: vec![[0u8; 32]; l2_count],
            l2_counts: vec![0u32; l2_count],
            l1_hashes: vec![[0u8; 32]; shard_count],
            l1_counts: vec![0u32; shard_count],
            root: [0u8; 32],
        }
    }

    /// 重算指定 L1 分片的哈希和计数（从其下 256 个 L2 聚合）
    fn recompute_l1(&mut self, l1: u16) {
        let l1_idx = l1 as usize;
        if l1_idx >= self.l1_hashes.len() {
            return;
        }
        let l2_start = l1_idx * L2_PER_L1 as usize;
        let l2_end = l2_start + L2_PER_L1 as usize;

        // L1 哈希 = blake3(256个L2哈希拼接)；空 L1（count_sum==0）规范为全零，
        // 与全新树初值一致，保证「全量 rebuild」与「逐条增量插入」结果相同。
        let mut hasher = blake3::Hasher::new();
        let mut count_sum: u32 = 0;
        for i in l2_start..l2_end {
            hasher.update(&self.l2_hashes[i]);
            count_sum = count_sum.wrapping_add(self.l2_counts[i]);
        }
        self.l1_hashes[l1_idx] = if count_sum == 0 {
            [0u8; 32]
        } else {
            *hasher.finalize().as_bytes()
        };
        self.l1_counts[l1_idx] = count_sum;
    }

    /// 重算 L0 根哈希（从所有 L1 哈希聚合）
    fn recompute_root(&mut self) {
        let mut hasher = blake3::Hasher::new();
        for h in &self.l1_hashes {
            hasher.update(h);
        }
        self.root = *hasher.finalize().as_bytes();
    }

    /// 批量更新 L2 分片后，重算受影响的 L1 和 L0
    ///
    /// `updates` 为 (l2_shard, hash, count) 列表。
    fn apply_l2_updates(&mut self, updates: &[(u32, [u8; 32], u32)]) {
        if updates.is_empty() {
            return;
        }
        let mut affected_l1 = FxHashSet::default();
        for &(l2, hash, count) in updates {
            let l2_idx = l2 as usize;
            if l2_idx < self.l2_hashes.len() {
                self.l2_hashes[l2_idx] = hash;
                self.l2_counts[l2_idx] = count;
                affected_l1.insert((l2 / L2_PER_L1 as u32) as u16);
            }
        }
        for &l1 in &affected_l1 {
            self.recompute_l1(l1);
        }
        self.recompute_root();
    }
}

/// 分层 Merkle Tree（L0 + L1 + L2，DB 驱动）
pub struct MerkleTree {
    /// L1 分片数（始终 256）
    shard_count: u16,
    /// 分层哈希状态
    state: RwLock<HashState>,
    /// 脏 L2 分片集合（写入后由后台任务取出重算）
    dirty_l2: RwLock<FxHashSet<u32>>,
    /// 上次全量重算时间
    last_full_rebuild: RwLock<Instant>,
    /// 本节点正在接收全量数据（接收方）
    receiving_full_sync: AtomicBool,
    /// 本节点正在向对端发送全量数据（发送方）
    sending_full_sync: AtomicBool,
}

impl MerkleTree {
    /// 创建新分层 Merkle Tree
    pub fn new(shard_count: u16) -> Self {
        let count = shard_count.max(1) as usize;
        Self {
            shard_count: shard_count.max(1),
            state: RwLock::new(HashState::new(count)),
            dirty_l2: RwLock::new(FxHashSet::default()),
            last_full_rebuild: RwLock::new(Instant::now()),
            receiving_full_sync: AtomicBool::new(false),
            sending_full_sync: AtomicBool::new(false),
        }
    }

    // ========================================================================
    // Key 到分片的映射
    // ========================================================================

    /// 计算 key 所属 L1 分片（公开，向后兼容，与 db::compute_shard 一致）
    ///
    /// 取 blake3(key) 前两字节的小端 u16 对 shard_count 取模。
    /// 当 shard_count=256 时结果为 bytes[0]，与 L2/256 一致。
    pub fn shard_for_key(&self, key: &[u8]) -> u16 {
        let hash = blake3::hash(key);
        let bytes = hash.as_bytes();
        u16::from_le_bytes([bytes[0], bytes[1]]) % self.shard_count
    }

    /// 计算 key 所属 L2 二级分片（0..65535）
    ///
    /// L2 = bytes[0] * 256 + bytes[1]，L1 = L2 / 256 = bytes[0]。
    pub fn l2_shard_for_key(&self, key: &[u8]) -> u32 {
        let hash = blake3::hash(key);
        let bytes = hash.as_bytes();
        let total_l2 = self.shard_count as u32 * L2_PER_L1 as u32;
        (u32::from(bytes[0]) * 256 + u32::from(bytes[1])) % total_l2
    }

    /// L2 分片所属的 L1 父分片
    pub fn l1_for_l2(l2: u32) -> u16 {
        (l2 / L2_PER_L1 as u32) as u16
    }

    // ========================================================================
    // 哈希计算辅助
    // ========================================================================

    /// 从 (key, data_hash) 列表计算单分片根哈希和条目数（叶子公式不变）
    ///
    /// 叶子公式：`blake3(key) ++ data_hash`，按 key 排序后累加。
    fn compute_shard_root(keys_hashes: &[(Vec<u8>, Vec<u8>)]) -> ([u8; 32], u32) {
        let mut sorted: Vec<(&Vec<u8>, &Vec<u8>)> =
            keys_hashes.iter().map(|(k, h)| (k, h)).collect();
        sorted.sort_by(|a, b| a.0.cmp(b.0));

        let mut hasher = blake3::Hasher::new();
        for (key, data_hash) in &sorted {
            let key_hash = blake3::hash(key);
            hasher.update(key_hash.as_bytes());
            hasher.update(data_hash);
        }
        let root = *hasher.finalize().as_bytes();
        (root, sorted.len() as u32)
    }

    /// 将条目列表按 L2 分片分组，计算每个 L2 的 (hash, count)
    fn group_by_l2(
        &self,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> FxHashMap<u32, Vec<(Vec<u8>, Vec<u8>)>> {
        let mut by_l2: FxHashMap<u32, Vec<(Vec<u8>, Vec<u8>)>> = FxHashMap::default();
        for (key, hash) in entries {
            let l2 = self.l2_shard_for_key(key);
            by_l2
                .entry(l2)
                .or_default()
                .push((key.clone(), hash.clone()));
        }
        by_l2
    }

    // ========================================================================
    // Dirty 标记（写入路径）
    // ========================================================================

    /// 标记单个 L1 分片为脏（向后兼容）：标记其下所有 256 个 L2 为脏。
    /// 收/发全量同步期间不标记。
    pub fn mark_dirty(&self, shard: u16) {
        if self.full_sync_active() {
            return;
        }
        let l1 = shard as u32;
        let l2_start = l1 * L2_PER_L1 as u32;
        let mut dirty = self.dirty_l2.write();
        for i in 0..L2_PER_L1 as u32 {
            dirty.insert(l2_start + i);
        }
    }

    /// 批量标记 L1 分片为脏（向后兼容）
    pub fn mark_dirty_batch(&self, shards: &[u16]) {
        if self.full_sync_active() || shards.is_empty() {
            return;
        }
        let mut dirty = self.dirty_l2.write();
        for &shard in shards {
            let l2_start = shard as u32 * L2_PER_L1 as u32;
            for i in 0..L2_PER_L1 as u32 {
                dirty.insert(l2_start + i);
            }
        }
    }

    /// 从 key 列表计算 L2 分片并批量标记脏（精准到 L2 粒度）
    pub fn mark_dirty_by_keys(&self, keys: &[Vec<u8>]) {
        if self.full_sync_active() || keys.is_empty() {
            return;
        }
        let mut dirty = self.dirty_l2.write();
        for key in keys {
            let l2 = self.l2_shard_for_key(key);
            dirty.insert(l2);
        }
    }

    /// 标记单个 L2 分片为脏（新接口，精准粒度）
    pub fn mark_dirty_l2(&self, l2: u32) {
        if self.full_sync_active() {
            return;
        }
        self.dirty_l2.write().insert(l2);
    }

    /// 取出并清空脏 L2 分片集合（原子操作，供后台增量任务调用）
    pub fn take_dirty_l2_shards(&self) -> FxHashSet<u32> {
        std::mem::take(&mut *self.dirty_l2.write())
    }

    /// 取出并清空脏 L1 分片集合（向后兼容）：返回有脏 L2 的 L1 父分片去重列表
    pub fn take_dirty_shards(&self) -> FxHashSet<u16> {
        let dirty_l2 = std::mem::take(&mut *self.dirty_l2.write());
        dirty_l2.into_iter().map(Self::l1_for_l2).collect()
    }

    // ========================================================================
    // 重算方法（从 DB 数据）
    // ========================================================================

    /// 重算指定 L1 分片下的所有 L2 二级分片（从 DB 加载的该 L1 全量数据）。
    ///
    /// 调用方传入该 L1 分片的所有 (key, data_hash)，内部按 L2 分组计算，
    /// 然后自动向上传播到 L1 和 L0。
    pub fn recompute_l1_from_db(&self, l1: u16, entries: &[(Vec<u8>, Vec<u8>)]) {
        let by_l2 = self.group_by_l2(entries);
        let l2_start = l1 as u32 * L2_PER_L1 as u32;

        let mut state = self.state.write();
        let mut updates = Vec::with_capacity(L2_PER_L1 as usize);
        for i in 0..L2_PER_L1 as u32 {
            let l2 = l2_start + i;
            match by_l2.get(&l2) {
                Some(e) => {
                    let (hash, count) = Self::compute_shard_root(e);
                    updates.push((l2, hash, count));
                }
                None => {
                    updates.push((l2, [0u8; 32], 0u32));
                }
            }
        }
        state.apply_l2_updates(&updates);
    }

    /// 重算指定 L2 子分片（从 DB 加载的该 L2 数据）。
    ///
    /// 仅重算单个 L2，然后传播到其父 L1 和 L0。
    /// 用于增量更新中精准重算少量脏 L2。
    ///
    /// 空集语义：`entries` 为空表示该 L2 已无行（例如删除了最后一个 key），必须写回
    /// `([0u8;32], 0)` 这一「缺席」表示，与 `recompute_l1_from_db` / `rebuild_all_from_db`
    /// 保持一致；若直接 `compute_shard_root(&[])` 会得到 blake3 空输入的非零哈希，导致同一
    /// 「空 L2」在不同重算路径下产生不同哈希，反熵永远收敛不到 0。
    pub fn recompute_l2_from_db(&self, l2: u32, entries: &[(Vec<u8>, Vec<u8>)]) {
        let (hash, count) = if entries.is_empty() {
            ([0u8; 32], 0u32)
        } else {
            Self::compute_shard_root(entries)
        };
        let mut state = self.state.write();
        state.apply_l2_updates(&[(l2, hash, count)]);
    }

    /// 批量重算多个 L2 子分片（按 L1 分组的 DB 数据）。
    ///
    /// `entries_by_l1` 为 L1 → 该 L1 全量条目的映射；
    /// `dirty_l2_in_l1` 为每个 L1 下需要重算的 L2 子集。
    /// 仅重算 dirty L2，同 L1 下其他 L2 保持不变。
    pub fn recompute_l2_subset_from_db(
        &self,
        entries_by_l1: &FxHashMap<u16, Vec<(Vec<u8>, Vec<u8>)>>,
        dirty_l2_in_l1: &FxHashMap<u16, FxHashSet<u32>>,
    ) {
        let mut updates = Vec::new();
        for (l1, dirty_l2s) in dirty_l2_in_l1 {
            // 该 L1 的所有条目按 L2 分组
            let all_entries = entries_by_l1.get(l1).map(|v| v.as_slice()).unwrap_or(&[]);
            let by_l2 = self.group_by_l2(all_entries);

            for &l2 in dirty_l2s {
                // 只处理属于该 L1 的 L2
                if l2 / L2_PER_L1 as u32 != *l1 as u32 {
                    continue;
                }
                let (hash, count) = match by_l2.get(&l2) {
                    Some(e) => Self::compute_shard_root(e),
                    None => ([0u8; 32], 0u32),
                };
                updates.push((l2, hash, count));
            }
        }

        if !updates.is_empty() {
            let mut state = self.state.write();
            state.apply_l2_updates(&updates);
        }
    }

    /// 重算指定分片的根（向后兼容，内部委托给 recompute_l1_from_db）
    pub fn recompute_shard_from_db(&self, shard: u16, keys_hashes: &[(Vec<u8>, Vec<u8>)]) {
        self.recompute_l1_from_db(shard, keys_hashes);
    }

    /// 全量重算：从 DB 加载的全量 (key, data_hash) 重算所有 L2/L1/L0。
    /// 纯 DB 驱动，不维护内存明细 map。
    pub fn rebuild_all_from_db(&self, all_keys_hashes: &[(Vec<u8>, Vec<u8>)]) {
        let by_l2 = self.group_by_l2(all_keys_hashes);
        let total_l2 = self.shard_count as usize * L2_PER_L1 as usize;
        let mut updates = Vec::with_capacity(total_l2);

        let mut state = self.state.write();
        for l2 in 0..total_l2 as u32 {
            match by_l2.get(&l2) {
                Some(entries) => {
                    let (hash, count) = Self::compute_shard_root(entries);
                    updates.push((l2, hash, count));
                }
                None => {
                    updates.push((l2, [0u8; 32], 0u32));
                }
            }
        }
        state.apply_l2_updates(&updates);
        drop(state);

        *self.last_full_rebuild.write() = Instant::now();
        // 清空 dirty 标记（全量重算已覆盖）
        self.dirty_l2.write().clear();
    }

    /// 重算 L0 全量根（向后兼容：从当前 L1 哈希重新聚合）
    pub fn recompute_full_root(&self) {
        let mut state = self.state.write();
        state.recompute_root();
    }

    // ========================================================================
    // 增量更新方法（联邦同步入站路径：只标记 dirty，由后台从 DB 重算）
    // ========================================================================

    /// 增量更新：插入/更新一个条目。纯 DB 驱动——只标记该 L2 分片为 dirty，
    /// 由后台任务从 DB 加载数据重算哈希，不在内存维护 key→data_hash 明细。
    /// full_sync_active 时跳过（mark_dirty_l2 内部已判断）。
    pub fn update_incremental(&self, key: &[u8], _data_hash: &[u8]) {
        let l2 = self.l2_shard_for_key(key);
        self.mark_dirty_l2(l2);
    }

    /// 增量更新指定 L2 分片的一个条目（显式指定 L2）。纯 DB 驱动——只标记 dirty。
    pub fn update_l2_shard_incremental(&self, l2: u32, _key: &[u8], _data_hash: &[u8]) {
        self.mark_dirty_l2(l2);
    }

    /// 批量增量更新：一批 (key, data_hash)。纯 DB 驱动——只标记 dirty L2。
    ///
    /// full_sync_active 或空输入时直接返回。
    pub fn update_incremental_batch(&self, entries: &[(&[u8], &[u8])]) {
        if entries.is_empty() {
            return;
        }
        if self.full_sync_active() {
            return;
        }
        let mut dirty = self.dirty_l2.write();
        for (key, _data_hash) in entries {
            let l2 = self.l2_shard_for_key(key);
            dirty.insert(l2);
        }
    }

    /// 增量删除：只标记该 key 所属 L2 分片为 dirty，由后台任务从 DB 重算。
    pub fn remove_incremental(&self, key: &[u8]) {
        let l2 = self.l2_shard_for_key(key);
        self.mark_dirty_l2(l2);
    }

    /// 获取指定 L2 分片的条目数（来自 DB 重算后的 L2 计数，同 l2_entry_count）。
    pub fn get_l2_shard_count(&self, l2: u32) -> u32 {
        self.l2_entry_count(l2)
    }

    // ========================================================================
    // 写入路径兼容方法（只标记 dirty，不维护内存）
    // ========================================================================

    /// 更新/插入条目（写入路径）。只标记 dirty L2 分片。
    pub fn update(&self, key: &[u8], _payload: &[u8], _data_hash: &[u8]) {
        let l2 = self.l2_shard_for_key(key);
        self.mark_dirty_l2(l2);
    }

    /// 批量更新/插入条目。只标记 dirty L2 分片。
    pub fn update_batch(&self, entries: &[(&[u8], &[u8], &[u8])]) {
        if entries.is_empty() {
            return;
        }
        if self.full_sync_active() {
            return;
        }
        let mut dirty = self.dirty_l2.write();
        for (key, _, _) in entries {
            let l2 = self.l2_shard_for_key(key);
            dirty.insert(l2);
        }
    }

    /// 删除条目：标记 dirty L2
    pub fn remove(&self, key: &[u8]) {
        let l2 = self.l2_shard_for_key(key);
        self.mark_dirty_l2(l2);
    }

    /// 批量删除条目
    pub fn remove_batch(&self, keys: &[Vec<u8>]) {
        self.mark_dirty_by_keys(keys);
    }

    /// 驱逐条目到冷数据：DB 中数据不变，无需重算。保留为空操作兼容调用方。
    pub fn evict_to_cold(&self, _keys: &[Vec<u8>]) -> usize {
        0
    }

    /// 标记墓碑：DB 中已删除，标记 dirty L2 分片。
    pub fn mark_tombstone(&self, key: &[u8]) {
        let l2 = self.l2_shard_for_key(key);
        self.mark_dirty_l2(l2);
    }

    /// 批量标记墓碑
    pub fn mark_tombstone_batch(&self, keys: &[Vec<u8>]) {
        self.mark_dirty_by_keys(keys);
    }

    // ========================================================================
    // 全量同步标志
    // ========================================================================

    /// 本节点正在接收全量数据（接收方）
    pub fn set_receiving_full_sync(&self, v: bool) {
        self.receiving_full_sync.store(v, Ordering::SeqCst);
    }

    /// 本节点正在向对端发送全量数据（发送方）
    pub fn set_sending_full_sync(&self, v: bool) {
        self.sending_full_sync.store(v, Ordering::SeqCst);
    }

    /// 收或发全量同步任一进行中（增量更新均暂停）
    fn full_sync_active(&self) -> bool {
        self.receiving_full_sync.load(Ordering::SeqCst)
            || self.sending_full_sync.load(Ordering::SeqCst)
    }

    /// 查询全量同步是否进行中（接收或发送任一）
    pub fn is_full_sync_in_progress(&self) -> bool {
        self.full_sync_active()
    }

    // ========================================================================
    // 全量重建
    // ========================================================================

    /// 全量重建：标记所有 L2 分片为 dirty（由后台任务从 DB 重算）
    pub fn rebuild_all(&self) {
        let total_l2 = self.shard_count as u32 * L2_PER_L1 as u32;
        let mut dirty = self.dirty_l2.write();
        for l2 in 0..total_l2 {
            dirty.insert(l2);
        }
    }

    /// 从 DB 全量重算冷数据（每小时调用一次）。等价于 rebuild_all_from_db。
    pub fn rebuild_cold_from_db(&self, keys_hashes: &[(Vec<u8>, Vec<u8>)]) {
        self.rebuild_all_from_db(keys_hashes);
    }

    // ========================================================================
    // 查询方法（分层访问）
    // ========================================================================

    /// 获取 L0 根哈希
    pub fn root_hash(&self) -> [u8; 32] {
        self.state.read().root
    }

    /// 获取 L0 全量根（向后兼容，同 root_hash）
    pub fn full_root(&self) -> [u8; 32] {
        self.state.read().root
    }

    /// 获取所有 L1 一级分片哈希（256个，向后兼容 shard_roots）
    pub fn level1_hashes(&self) -> Vec<[u8; 32]> {
        self.state.read().l1_hashes.clone()
    }

    /// 获取指定 L1 分片下的所有 L2 二级分片哈希（256个）
    pub fn level2_hashes(&self, l1_shard: u16) -> Vec<[u8; 32]> {
        let state = self.state.read();
        let l1_idx = l1_shard as usize;
        if l1_idx >= state.l1_hashes.len() {
            return Vec::new();
        }
        let l2_start = l1_idx * L2_PER_L1 as usize;
        let l2_end = l2_start + L2_PER_L1 as usize;
        state.l2_hashes[l2_start..l2_end].to_vec()
    }

    /// 获取指定 L2 分片的哈希
    pub fn l2_hash(&self, l2: u32) -> Option<[u8; 32]> {
        let state = self.state.read();
        if (l2 as usize) < state.l2_hashes.len() {
            Some(state.l2_hashes[l2 as usize])
        } else {
            None
        }
    }

    /// 获取 L1 分片条目数
    pub fn l1_entry_count(&self, l1: u16) -> u32 {
        let state = self.state.read();
        if (l1 as usize) < state.l1_counts.len() {
            state.l1_counts[l1 as usize]
        } else {
            0
        }
    }

    /// 获取 L2 分片条目数
    pub fn l2_entry_count(&self, l2: u32) -> u32 {
        let state = self.state.read();
        if (l2 as usize) < state.l2_counts.len() {
            state.l2_counts[l2 as usize]
        } else {
            0
        }
    }

    /// 生成全量摘要（携带 L1 哈希和 L0 根，向后兼容旧协议）
    pub fn digest(&self, repo_type: u8) -> MerkleDigestMessage {
        let state = self.state.read();
        MerkleDigestMessage {
            repo_type,
            shard_count: self.shard_count,
            roots: state.l1_hashes.clone(),
            entry_counts: state.l1_counts.clone(),
            full_root: Some(state.root),
        }
    }

    /// 对比找出 L1 根哈希不同的分片索引（向后兼容）
    ///
    /// 先比较 L0 根（如果对端提供），相同则直接返回空。
    /// 不同则逐 L1 比较。
    pub fn diff(&self, other: &MerkleDigestMessage) -> Vec<u16> {
        // L0 快速路径：对端提供 full_root 且与本地一致 → 无差异
        if let Some(other_full) = other.full_root {
            if other_full == self.state.read().root {
                return Vec::new();
            }
        }

        let state = self.state.read();
        let mut diffs = Vec::new();
        let count = self.shard_count.min(other.shard_count) as usize;
        for i in 0..count {
            // 本地哈希缺失 → 记为差异
            let local = match state.l1_hashes.get(i) {
                Some(h) => h,
                None => {
                    diffs.push(i as u16);
                    continue;
                }
            };
            // 对端 roots 长度不足（bincode 不校验三者长度一致）→ 视为差异，避免越界 panic
            let remote = match other.roots.get(i) {
                Some(h) => h,
                None => {
                    diffs.push(i as u16);
                    continue;
                }
            };
            if local != remote {
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

    /// 对比 L1 分片哈希，返回差异的 L1 索引（新接口，语义同 diff 但命名更清晰）
    pub fn diff_level1(&self, other_l1: &[[u8; 32]]) -> Vec<u16> {
        let state = self.state.read();
        let count = self.shard_count as usize;
        let other_count = other_l1.len().min(count);
        let mut diffs = Vec::new();
        for (i, other_hash) in other_l1.iter().enumerate().take(other_count) {
            if state.l1_hashes[i] != *other_hash {
                diffs.push(i as u16);
            }
        }
        for i in other_count..count {
            diffs.push(i as u16);
        }
        diffs
    }

    /// 对比指定 L1 下的 L2 分片哈希，返回差异的 L2 索引（相对于该 L1 的偏移 0..255）
    pub fn diff_level2(&self, l1_shard: u16, other_l2: &[[u8; 32]]) -> Vec<u16> {
        let state = self.state.read();
        let l1_idx = l1_shard as usize;
        if l1_idx >= state.l1_hashes.len() {
            return (0..other_l2.len() as u16).collect();
        }
        let l2_start = l1_idx * L2_PER_L1 as usize;
        let mut diffs = Vec::new();
        let count = L2_PER_L1 as usize;
        let other_count = other_l2.len().min(count);
        for (i, other_hash) in other_l2.iter().enumerate().take(other_count) {
            if state.l2_hashes[l2_start + i] != *other_hash {
                diffs.push(i as u16);
            }
        }
        for i in other_count..count {
            diffs.push(i as u16);
        }
        diffs
    }

    // ========================================================================
    // 兼容旧接口的查询方法
    // ========================================================================

    /// 获取某分片所有条目（DB 驱动后无内存数据，返回空）
    pub fn get_shard_entries(&self, _shard: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
        Vec::new()
    }

    /// 条目总数（从 L1 计数求和）
    pub fn total_entries(&self) -> usize {
        self.state.read().l1_counts.iter().sum::<u32>() as usize
    }

    /// 冷数据条目数（DB 驱动后无冷热分离，返回 0 兼容）
    pub fn cold_total_entries(&self) -> u64 {
        0
    }

    /// 分片数（L1 分片数）
    pub fn shard_count(&self) -> u16 {
        self.shard_count
    }

    /// L2 总分片数
    pub fn l2_total_count(&self) -> u32 {
        self.shard_count as u32 * L2_PER_L1 as u32
    }

    /// 获取指定分片集合的所有条目（DB 驱动后无内存数据，返回空）
    pub fn entries_for_shards(&self, _shards: &[u16]) -> Vec<(Vec<u8>, Vec<u8>)> {
        Vec::new()
    }

    /// 判断 key 是否存在（DB 驱动后无内存索引，返回 false）
    pub fn contains_key(&self, _key: &[u8]) -> bool {
        false
    }

    /// 获取 key 对应的 payload（DB 驱动后无内存数据，返回 None）
    pub fn get(&self, _key: &[u8]) -> Option<Vec<u8>> {
        None
    }

    /// 获取分片根快照（用于监控/调试，兼容旧接口，返回 L1 哈希）
    pub fn cold_roots_snapshot(&self) -> Vec<[u8; 32]> {
        self.state.read().l1_hashes.clone()
    }

    /// 获取分片条目数快照（用于监控/调试，兼容旧接口，返回 L1 计数）
    pub fn cold_entry_counts_snapshot(&self) -> Vec<u32> {
        self.state.read().l1_counts.clone()
    }

    /// 墓碑数量（DB 驱动后无墓碑集合，返回 0 兼容）
    pub fn tombstone_count(&self) -> usize {
        0
    }

    /// 上次全量重算时间（用于监控）
    pub fn last_full_rebuild(&self) -> Instant {
        *self.last_full_rebuild.read()
    }

    /// dirty L1 分片数量（用于监控，向后兼容）
    pub fn dirty_shard_count(&self) -> usize {
        let dirty = self.dirty_l2.read();
        dirty
            .iter()
            .map(|&l2| Self::l1_for_l2(l2))
            .collect::<FxHashSet<_>>()
            .len()
    }

    /// dirty L2 分片数量（用于监控）
    pub fn dirty_l2_count(&self) -> usize {
        self.dirty_l2.read().len()
    }
}

/// Merkle 提供者 trait（用于 GossipEngine 的 anti-entropy 回调）
pub trait MerkleProvider: Send + Sync {
    fn get_digest(&self, repo_type: u8) -> MerkleDigestMessage;
    fn get_shard_entries(&self, repo_type: u8, shard: u16) -> Vec<(Vec<u8>, Vec<u8>)>;
    /// P1-4：该 repo 是否已由 range-based（有序区间下钻）反熵接管。
    /// 返回 true 时反熵不再对该 repo 发送 MerkleDigest（避免与 range 通道双重对账）。
    /// 默认 false：全部 repo 继续走既有分层 Merkle，行为与改造前一致。
    fn range_reconcile_owns(&self, _repo_type: u8) -> bool {
        false
    }
    /// P2-5：该 repo 本轮是否「到期」需要反熵对账（按 repo 差异化周期）。
    /// 默认 true：每次 tick 都对所有 repo 对账，行为与改造前一致。
    fn anti_entropy_due(&self, _repo_type: u8) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试用 data_hash：直接取 payload 的 blake3
    fn dh(payload: &[u8]) -> Vec<u8> {
        blake3::hash(payload).as_bytes().to_vec()
    }

    /// 构建测试数据集
    fn build_data(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n)
            .map(|i| {
                let key = format!("key{}", i).into_bytes();
                let hash = blake3::hash(format!("data{}", i).as_bytes())
                    .as_bytes()
                    .to_vec();
                (key, hash)
            })
            .collect()
    }

    #[test]
    fn test_l2_shard_mapping() {
        let mt = MerkleTree::new(256);
        let key = b"test_key";
        let l2 = mt.l2_shard_for_key(key);
        let l1 = mt.shard_for_key(key);
        // L1 = L2 / 256
        assert_eq!(l1, MerkleTree::l1_for_l2(l2));
        // L2 在有效范围
        assert!(l2 < 65536);
    }

    #[test]
    fn test_l2_shard_distribution() {
        let mt = MerkleTree::new(256);
        let data = build_data(1000);
        let l2s: FxHashSet<u32> = data.iter().map(|(k, _)| mt.l2_shard_for_key(k)).collect();
        // 1000 个 key 应分布在多个 L2 分片
        assert!(l2s.len() > 10);
        // 所有 L2 都在范围内
        assert!(l2s.iter().all(|&l2| l2 < 65536));
    }

    #[test]
    fn test_merkle_rebuild_and_digest() {
        let mt = MerkleTree::new(256);
        let data = build_data(100);
        mt.rebuild_all_from_db(&data);

        let digest = mt.digest(1);
        assert_eq!(digest.repo_type, 1);
        assert_eq!(digest.shard_count, 256);
        assert_eq!(digest.entry_counts.iter().sum::<u32>(), 100);
        assert!(digest.full_root.is_some());
    }

    #[test]
    fn test_layered_hashes_consistency() {
        let mt = MerkleTree::new(256);
        let data = build_data(500);
        mt.rebuild_all_from_db(&data);

        // L1 哈希应等于其下 256 个 L2 哈希的 blake3
        for l1 in 0..4u16 {
            let l2_hashes = mt.level2_hashes(l1);
            assert_eq!(l2_hashes.len(), 256);
            let mut hasher = blake3::Hasher::new();
            for h in &l2_hashes {
                hasher.update(h);
            }
            let computed_l1 = *hasher.finalize().as_bytes();
            let l1_hashes = mt.level1_hashes();
            assert_eq!(l1_hashes[l1 as usize], computed_l1);
        }

        // L0 根应等于 256 个 L1 哈希的 blake3
        let l1_hashes = mt.level1_hashes();
        let mut hasher = blake3::Hasher::new();
        for h in &l1_hashes {
            hasher.update(h);
        }
        let computed_root = *hasher.finalize().as_bytes();
        assert_eq!(mt.root_hash(), computed_root);
    }

    #[test]
    fn test_merkle_diff() {
        let mt1 = MerkleTree::new(4);
        let mt2 = MerkleTree::new(4);

        let data1 = build_data(10);
        mt1.rebuild_all_from_db(&data1);
        mt2.rebuild_all_from_db(&data1);

        // 相同数据，无差异
        let digest2 = mt2.digest(1);
        assert!(mt1.diff(&digest2).is_empty());

        // mt2 修改数据，应有差异
        let mut data2 = data1.clone();
        data2.push((b"key_new".to_vec(), dh(b"new_data")));
        mt2.rebuild_all_from_db(&data2);
        let digest2 = mt2.digest(1);
        let diffs = mt1.diff(&digest2);
        assert!(!diffs.is_empty());
    }

    #[test]
    fn test_diff_level1_level2() {
        let mt1 = MerkleTree::new(256);
        let mt2 = MerkleTree::new(256);

        let data = build_data(200);
        mt1.rebuild_all_from_db(&data);
        mt2.rebuild_all_from_db(&data);

        // 相同数据无差异
        let l1_2 = mt2.level1_hashes();
        assert!(mt1.diff_level1(&l1_2).is_empty());

        // 修改 mt2 中一条数据
        let mut data2 = data.clone();
        data2[0].1 = dh(b"modified_data");
        mt2.rebuild_all_from_db(&data2);

        // L1 应有差异
        let l1_2 = mt2.level1_hashes();
        let diff_l1 = mt1.diff_level1(&l1_2);
        assert!(!diff_l1.is_empty());

        // 对差异 L1，L2 应有差异
        for l1 in &diff_l1 {
            let l2_2 = mt2.level2_hashes(*l1);
            let diff_l2 = mt1.diff_level2(*l1, &l2_2);
            assert!(!diff_l2.is_empty());
            // L2 差异应在 0..256
            assert!(diff_l2.iter().all(|&l2| l2 < 256));
        }
    }

    #[test]
    fn test_merkle_dirty_l2_take() {
        let mt = MerkleTree::new(256);
        // 模拟写入：标记 dirty L2
        mt.update(b"key1", b"p1", &dh(b"p1"));
        mt.update(b"key2", b"p2", &dh(b"p2"));

        let dirty = mt.take_dirty_l2_shards();
        assert!(!dirty.is_empty());

        // 取出后应为空
        let dirty2 = mt.take_dirty_l2_shards();
        assert!(dirty2.is_empty());
    }

    #[test]
    fn test_merkle_dirty_l1_backward_compat() {
        let mt = MerkleTree::new(256);
        mt.mark_dirty(5); // 标记 L1=5，应标记其下 256 个 L2

        let dirty_l2 = mt.take_dirty_l2_shards();
        assert_eq!(dirty_l2.len(), 256);
        // 所有 dirty L2 都属于 L1=5
        assert!(dirty_l2.iter().all(|&l2| MerkleTree::l1_for_l2(l2) == 5));
    }

    #[test]
    fn test_recompute_l2_from_db() {
        let mt = MerkleTree::new(256);
        let data = build_data(100);
        mt.rebuild_all_from_db(&data);
        let root_before = mt.root_hash();

        // 找到 key1 所属 L2
        let l2 = mt.l2_shard_for_key(b"key1");
        // 模拟该 L2 数据变更
        let l2_data: Vec<(Vec<u8>, Vec<u8>)> = data
            .iter()
            .filter(|(k, _)| mt.l2_shard_for_key(k) == l2)
            .cloned()
            .chain(std::iter::once((b"key_extra".to_vec(), dh(b"extra"))))
            .collect();
        mt.recompute_l2_from_db(l2, &l2_data);

        // 根应变化
        assert_ne!(mt.root_hash(), root_before);
    }

    #[test]
    fn test_recompute_l1_from_db() {
        let mt = MerkleTree::new(256);
        let data = build_data(200);
        mt.rebuild_all_from_db(&data);

        // 找一个有数据的 L1
        let l1 = mt.shard_for_key(b"key0");
        let l1_data: Vec<(Vec<u8>, Vec<u8>)> = data
            .iter()
            .filter(|(k, _)| mt.shard_for_key(k) == l1)
            .cloned()
            .collect();

        let l1_hash_before = mt.level1_hashes()[l1 as usize];
        // 重新计算同一 L1（数据不变），结果应一致
        mt.recompute_l1_from_db(l1, &l1_data);
        let l1_hash_after = mt.level1_hashes()[l1 as usize];
        assert_eq!(l1_hash_before, l1_hash_after);
    }

    #[test]
    fn test_merkle_cold_rebuild_deterministic() {
        let mt1 = MerkleTree::new(16);
        let mt2 = MerkleTree::new(16);

        let data = build_data(100);
        mt1.rebuild_cold_from_db(&data);
        // 打乱顺序输入，结果应一致
        let mut data_shuffled = data.clone();
        data_shuffled.reverse();
        mt2.rebuild_cold_from_db(&data_shuffled);

        assert_eq!(mt1.root_hash(), mt2.root_hash());
        assert_eq!(mt1.level1_hashes(), mt2.level1_hashes());
    }

    #[test]
    fn test_merkle_full_root_fast_path() {
        let mt1 = MerkleTree::new(4);
        let mt2 = MerkleTree::new(4);

        let data = build_data(10);
        mt1.rebuild_all_from_db(&data);
        mt2.rebuild_all_from_db(&data);

        let digest = mt2.digest(1);
        // full_root 相同 → diff 应为空（快速路径）
        assert!(mt1.diff(&digest).is_empty());

        // 构造一个 full_root 为 None 的摘要（旧版本节点）
        let old_digest = MerkleDigestMessage {
            full_root: None,
            ..digest
        };
        // 没有 full_root 时走分片比较，也应为空
        assert!(mt1.diff(&old_digest).is_empty());
    }

    #[test]
    fn test_merkle_deterministic() {
        let mt1 = MerkleTree::new(16);
        let mt2 = MerkleTree::new(16);

        let data = build_data(50);
        mt1.rebuild_all_from_db(&data);
        mt2.rebuild_all_from_db(&data);

        let d1 = mt1.digest(1);
        let d2 = mt2.digest(1);
        assert_eq!(d1.roots, d2.roots);
        assert_eq!(d1.full_root, d2.full_root);
    }

    #[test]
    fn test_merkle_shard_distribution() {
        let mt = MerkleTree::new(8);
        let data = build_data(100);
        mt.rebuild_all_from_db(&data);
        // 条目应分布在多个分片
        let digest = mt.digest(1);
        let non_empty = digest.entry_counts.iter().filter(|&&c| c > 0).count();
        assert!(non_empty > 1);
        assert_eq!(digest.entry_counts.iter().sum::<u32>(), 100);
    }

    #[test]
    fn test_merkle_rebuild_all_marks_dirty() {
        let mt = MerkleTree::new(4);
        // 写入标记 dirty
        mt.update(b"key1", b"p1", &dh(b"p1"));
        assert!(!mt.take_dirty_l2_shards().is_empty());

        // rebuild_all_from_db 应清空 dirty
        let data = build_data(10);
        mt.rebuild_all_from_db(&data);
        assert!(mt.take_dirty_l2_shards().is_empty());
    }

    #[test]
    fn test_merkle_shard_for_key_consistency() {
        let mt = MerkleTree::new(256);
        // shard_for_key 应与 db.rs::compute_shard 一致
        let key = b"test_key_123";
        let s1 = mt.shard_for_key(key);
        let s2 = crate::storage::db::compute_shard(key);
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_l2_entry_counts() {
        let mt = MerkleTree::new(256);
        let data = build_data(500);
        mt.rebuild_all_from_db(&data);

        // L1 计数 = 其下 256 个 L2 计数之和
        for l1 in 0..8u16 {
            let l2_start = l1 as u32 * 256;
            let l2_sum: u32 = (0..256u32).map(|i| mt.l2_entry_count(l2_start + i)).sum();
            assert_eq!(mt.l1_entry_count(l1), l2_sum);
        }

        // 总数 = 所有 L1 计数之和
        let total: u32 = (0..256u16).map(|l1| mt.l1_entry_count(l1)).sum();
        assert_eq!(total as usize, mt.total_entries());
        assert_eq!(total, 500);
    }

    #[test]
    fn test_empty_tree_hashes() {
        let mt = MerkleTree::new(256);
        // 空树所有哈希应为零
        assert_eq!(mt.root_hash(), [0u8; 32]);
        assert!(mt.level1_hashes().iter().all(|h| *h == [0u8; 32]));
        assert!(mt.level2_hashes(0).iter().all(|h| *h == [0u8; 32]));
        assert_eq!(mt.total_entries(), 0);
    }

    #[test]
    fn test_mark_dirty_by_keys_precision() {
        let mt = MerkleTree::new(256);
        let keys: Vec<Vec<u8>> = (0..10).map(|i| format!("key{}", i).into_bytes()).collect();
        mt.mark_dirty_by_keys(&keys);

        let dirty_l2 = mt.take_dirty_l2_shards();
        // 每个 key 对应一个 L2，去重后应 <= 10
        assert!(dirty_l2.len() <= 10);
        assert!(!dirty_l2.is_empty());
    }

    #[test]
    fn test_incremental_vs_full_rebuild_consistency() {
        // 1) 全量 rebuild 作为基准
        let mt_full = MerkleTree::new(256);
        let data = build_data(500);
        mt_full.rebuild_all_from_db(&data);
        let root_full = mt_full.root_hash();
        let total_full = mt_full.total_entries();

        // 2) 另一棵树：增量写入只标记 dirty，再由后台任务从 DB 全量重算，结果应一致
        let mt_inc = MerkleTree::new(256);
        let refs: Vec<(&[u8], &[u8])> = data
            .iter()
            .map(|(k, h)| (k.as_slice(), h.as_slice()))
            .collect();
        mt_inc.update_incremental_batch(&refs);
        // 纯 DB 驱动：增量写入只标 dirty，哈希由后台从 DB 重算
        mt_inc.rebuild_all_from_db(&data);
        assert_eq!(mt_inc.root_hash(), root_full);
        assert_eq!(mt_inc.total_entries(), total_full);
        assert_eq!(mt_inc.level1_hashes(), mt_full.level1_hashes());

        // 3) 某条目 data_hash 变更：标 dirty 后从 DB 重算，根变化且 count 不增加
        let mut data2 = data.clone();
        let old_key = b"key0";
        let l2_of_key = mt_inc.l2_shard_for_key(old_key);
        let count_before = mt_inc.get_l2_shard_count(l2_of_key);
        data2[0].1 = dh(b"totally_different_payload");
        mt_inc.update_incremental(old_key, &data2[0].1);
        mt_inc.rebuild_all_from_db(&data2);
        assert_ne!(mt_inc.root_hash(), root_full);
        assert_eq!(mt_inc.get_l2_shard_count(l2_of_key), count_before);

        // 4) 删除一条，重算后 count 减少
        let mut data3 = data2.clone();
        data3.remove(0);
        mt_inc.remove_incremental(b"key0");
        mt_inc.rebuild_all_from_db(&data3);
        assert_eq!(mt_inc.total_entries(), total_full - 1);
    }

    #[test]
    fn test_incremental_empty_and_guard() {
        let mt = MerkleTree::new(256);
        // 空批量调用不应 panic，也不应标记 dirty
        mt.update_incremental_batch(&[]);
        assert_eq!(mt.root_hash(), [0u8; 32]);
        assert_eq!(mt.total_entries(), 0);
        assert!(mt.take_dirty_l2_shards().is_empty());

        // 非全量同步：增量写入标记 dirty
        mt.update_incremental(b"k", &dh(b"v"));
        assert!(!mt.take_dirty_l2_shards().is_empty());

        // full_sync_active 时增量写入不标记 dirty（与 mark_dirty 一致）
        mt.set_receiving_full_sync(true);
        mt.update_incremental(b"k2", &dh(b"v2"));
        assert!(mt.take_dirty_l2_shards().is_empty());
        mt.set_receiving_full_sync(false);
    }
}
