//! P1-4：Range-based（有序区间 + 分界点下钻）反熵。
//!
//! 设计见 `docs/architecture/12-federation-sync-reconciliation.md` §5.3 / §6.4。
//!
//! 从「哈希分桶」改为「**有序区间 + 分界点下钻**」：
//! - 请求方发一个 key 区间 `[lo, hi)` 及其本地摘要；
//! - 应答方回该区间的摘要 + 分界点（split points，非叶时）或行指纹清单（叶时）；
//! - 摘要相同 → 剪枝；不同 → 按分界点继续下钻，直到叶级交换行指纹清单求集合差。
//! - 最坏只访问 O(d · log(N/d)) 个节点；**不需要预先知道 d**，自适应收敛。
//! - 天然解决「差异撒满全部分片」—— 因为差异跟着 key 位置走，不再跟着哈希走。
//!
//! 灰度策略（架构文档 §9 风险缓解）：默认 `range_reconcile_enabled=false`；开启后先以
//! **只读诊断模式**（`range_reconcile_diagnostic_only=true`）运行 —— 只求差集并打印统计，
//! 不修改任何数据；确认与既有反熵口径一致后，再置 false 走实际修复。
//!
//! 本模块只放**纯逻辑**（摘要 / 分界点 / 集合差 / 下钻决策）+ 常量，网络与存储访问在
//! `sync/mod.rs` 的 handler 与 `storage/db.rs` 的区间加载里，便于本地单元测试。

/// 支持 range-based 反熵的协议版本（握手能力协商）。
pub const RANGE_RECONCILE_PROTOCOL_VERSION: u32 = 5;

/// 叶级行数阈值：区间内行数 ≤ 该值即到叶，交换行指纹清单。
pub const DEFAULT_LEAF_ROWS: u32 = 512;
/// 单次响应的最大分界点数。
pub const DEFAULT_MAX_SPLITS: u32 = 16;
/// 最大下钻深度（防御性，避免病态区间无限下钻）。
pub const DEFAULT_MAX_DEPTH: u8 = 16;
/// 单条响应携带的最大叶子条数（防止超大帧）。
pub const MAX_LEAF_ENTRIES: usize = 4096;
/// 单轮诊断抽样的区间数。
pub const DEFAULT_SAMPLE_RANGES: u32 = 8;

/// 区间摘要：对按 key 升序的 `(key, data_hash)` 列表拼接后取 blake3。
///
/// 每段带 4 字节小端长度前缀，避免不同划分产生相同字节流（防拼接歧义）。
/// **要求调用方保证 `rows` 已按 key 升序**；本函数不排序以免隐藏 O(n log n) 开销。
pub fn range_digest(rows: &[(Vec<u8>, Vec<u8>)]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for (k, h) in rows {
        hasher.update(&(k.len() as u32).to_le_bytes());
        hasher.update(k);
        hasher.update(&(h.len() as u32).to_le_bytes());
        hasher.update(h);
    }
    *hasher.finalize().as_bytes()
}

/// 取分界点：把有序 `rows` 均分为至多 `max_splits + 1` 段，返回各段边界 key（不含首尾）。
///
/// 返回值严格递增且互不相同；调用方据此把 `[lo, hi)` 切成子区间递归下钻。
pub fn split_points(rows: &[(Vec<u8>, Vec<u8>)], max_splits: usize) -> Vec<Vec<u8>> {
    if rows.len() < 2 || max_splits == 0 {
        return Vec::new();
    }
    let segs = max_splits + 1;
    let mut out: Vec<Vec<u8>> = Vec::new();
    for i in 1..segs {
        let idx = rows.len() * i / segs;
        if idx == 0 || idx >= rows.len() {
            continue;
        }
        let p = &rows[idx].0;
        if out
            .last()
            .map(|l| l.as_slice() == p.as_slice())
            .unwrap_or(false)
        {
            continue;
        }
        out.push(p.clone());
    }
    out
}

/// 两集合（按 key）差集：返回 `(仅本地有的 key, 仅对端有的 key)`。
///
/// key 相同但 data_hash 不同也算差异（key 进入 `local_only` / `remote_only`）。
pub fn key_diff(
    local: &[(Vec<u8>, Vec<u8>)],
    remote: &[(Vec<u8>, Vec<u8>)],
) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    use rustc_hash::FxHashMap;
    let remote_map: FxHashMap<&[u8], &[u8]> = remote
        .iter()
        .map(|(k, h)| (k.as_slice(), h.as_slice()))
        .collect();
    let local_map: FxHashMap<&[u8], &[u8]> = local
        .iter()
        .map(|(k, h)| (k.as_slice(), h.as_slice()))
        .collect();

    let mut local_only = Vec::new();
    for (k, h) in local {
        match remote_map.get(k.as_slice()) {
            Some(rh) if *rh == h.as_slice() => {}
            _ => local_only.push(k.clone()),
        }
    }
    let mut remote_only = Vec::new();
    for (k, h) in remote {
        match local_map.get(k.as_slice()) {
            Some(lh) if *lh == h.as_slice() => {}
            _ => remote_only.push(k.clone()),
        }
    }
    (local_only, remote_only)
}

/// 下钻决策。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeDecision {
    /// 摘要相同：剪枝（该区间无差异）。
    Prune,
    /// 可求差：对端给了行指纹清单（叶），或已到深度上限。
    Leaf,
    /// 按分界点继续下钻。
    Descend,
}

/// 决策（纯函数，便于测试）。
///
/// - 摘要相等 → `Prune`；
/// - 对端为叶（携带行指纹）→ `Leaf`（本地即可求集合差）；
/// - 深度到顶 → `Leaf`（best-effort，避免无限下钻）；
/// - 否则 → `Descend`。
pub fn decide(
    local_digest: &[u8; 32],
    remote_digest: &[u8; 32],
    responder_is_leaf: bool,
    depth: u8,
    max_depth: u8,
) -> RangeDecision {
    if local_digest == remote_digest {
        return RangeDecision::Prune;
    }
    if responder_is_leaf || depth >= max_depth {
        return RangeDecision::Leaf;
    }
    RangeDecision::Descend
}

/// 对端协议版本是否支持 range-based 反熵（纯判定）。
pub fn supports_range_reconcile(peer_protocol_version: u32) -> bool {
    peer_protocol_version >= RANGE_RECONCILE_PROTOCOL_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(k: &[u8], h: &[u8]) -> (Vec<u8>, Vec<u8>) {
        (k.to_vec(), h.to_vec())
    }

    #[test]
    fn test_range_digest_stable_and_sensitive() {
        let a = vec![row(b"1.1.1.1:6881", b"h1"), row(b"2.2.2.2:6881", b"h2")];
        let b = a.clone();
        assert_eq!(range_digest(&a), range_digest(&b));

        // 改 hash → 变
        let mut c = a.clone();
        c[1].1 = b"h2x".to_vec();
        assert_ne!(range_digest(&a), range_digest(&c));

        // 改顺序（非同序）→ 变（摘要依赖有序拼接）
        let mut d = a.clone();
        d.reverse();
        assert_ne!(range_digest(&a), range_digest(&d));

        // 空集摘要稳定
        assert_eq!(range_digest(&[]), range_digest(&[]));
    }

    #[test]
    fn test_split_points_bounds_and_monotonic() {
        let rows: Vec<_> = (0..100u32)
            .map(|i| row(format!("k{:04}", i).as_bytes(), b"h"))
            .collect();
        let sp = split_points(&rows, 4);
        assert!(!sp.is_empty());
        assert!(sp.len() <= 4);
        // 严格递增
        for w in sp.windows(2) {
            assert!(w[0] < w[1]);
        }
        // 都是区间内的 key
        for p in &sp {
            assert!(rows.iter().any(|(k, _)| k == p));
        }
        // 单行 / 零分界点 → 空
        assert!(split_points(&rows[..1], 4).is_empty());
        assert!(split_points(&rows, 0).is_empty());
    }

    #[test]
    fn test_key_diff() {
        let local = vec![row(b"a", b"1"), row(b"b", b"2"), row(b"c", b"3")];
        let remote = vec![
            row(b"b", b"2"),  // 相同
            row(b"c", b"3x"), // hash 不同 → 差异
            row(b"d", b"4"),  // 仅对端有
        ];
        let (lo, ro) = key_diff(&local, &remote);
        assert_eq!(lo, vec![b"a".to_vec(), b"c".to_vec()]);
        assert_eq!(ro, vec![b"c".to_vec(), b"d".to_vec()]);

        // 空集
        let (lo, ro) = key_diff(&[], &[]);
        assert!(lo.is_empty() && ro.is_empty());
    }

    #[test]
    fn test_decide() {
        let d = [7u8; 32];
        let e = [8u8; 32];
        // 摘要相同 → 剪枝
        assert_eq!(decide(&d, &d, false, 0, 16), RangeDecision::Prune);
        // 对端是叶 → 可直接求差
        assert_eq!(decide(&d, &e, true, 0, 16), RangeDecision::Leaf);
        // 非叶且未到深度 → 下钻
        assert_eq!(decide(&d, &e, false, 3, 16), RangeDecision::Descend);
        // 非叶但到深度上限 → 叶（best-effort）
        assert_eq!(decide(&d, &e, false, 16, 16), RangeDecision::Leaf);
    }

    #[test]
    fn test_supports_range_reconcile() {
        assert!(!supports_range_reconcile(4));
        assert!(supports_range_reconcile(5));
        assert!(supports_range_reconcile(6));
    }
}
