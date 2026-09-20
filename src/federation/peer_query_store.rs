//! 联邦实时 peer 查询响应收集器（PT 业务，从 `ConnectionManager` 剥离）
//!
//! # 为什么不属于连接层
//!
//! `ConnectionManager` 曾持有 `peer_query_responses: DashMap<[u8;20], Vec<PeerQueryEntry>>`
//! 与两个方法 `clear_peer_query_responses` / `take_peer_query_responses`（调用点 `mod.rs:606` `:628`）。
//! 这是**按 infohash 索引的 PT peer 结果聚合**——纯粹的业务语义，
//! 与「谁能连上谁」无关，因此不能随连接层一起下沉到通用 SDK。
//!
//! 剥离后：本表按 infohash 索引，由 `query_peers` 发起时清空、超时后取走；
//! `dispatch` 收到 `PeerQueryResponse` 时 push。

use dashmap::DashMap;

use crate::federation::protocol::PeerQueryEntry;

/// `infohash → 各联邦节点返回的 peer 条目`
#[derive(Default)]
pub struct PeerQueryStore {
    inner: DashMap<[u8; 20], Vec<PeerQueryEntry>>,
}

impl PeerQueryStore {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    /// 清空指定 infohash 的旧响应（发起查询前调用，避免混入上一轮结果）
    pub fn clear(&self, infohash: &[u8; 20]) {
        self.inner.remove(infohash);
    }

    /// 追加单条 peer 条目（收到 `PeerQueryResponse` 时调用）
    pub fn push(&self, infohash: [u8; 20], entry: PeerQueryEntry) {
        self.inner.entry(infohash).or_default().push(entry);
    }

    /// 批量追加（一个响应帧携带多个 peer）
    pub fn push_many(&self, infohash: [u8; 20], entries: impl IntoIterator<Item = PeerQueryEntry>) {
        let mut bucket = self.inner.entry(infohash).or_default();
        bucket.extend(entries);
    }

    /// 取走并删除指定 infohash 的所有响应条目（查询超时后调用）
    pub fn take(&self, infohash: &[u8; 20]) -> Vec<PeerQueryEntry> {
        self.inner
            .remove(infohash)
            .map(|(_, v)| v)
            .unwrap_or_default()
    }

    /// 当前仍在收集的 infohash 数量
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(port: u16) -> PeerQueryEntry {
        PeerQueryEntry {
            ip: "10.0.0.1".to_string(),
            port,
            source: "dht".to_string(),
            score: 1.0,
        }
    }

    #[test]
    fn test_clear_then_take_is_empty() {
        let store = PeerQueryStore::new();
        let ih = [7u8; 20];
        store.push(ih, entry(1));
        assert_eq!(store.len(), 1);
        store.clear(&ih);
        assert!(store.take(&ih).is_empty());
        assert!(store.is_empty());
    }

    #[test]
    fn test_push_accumulates_and_take_drains() {
        let store = PeerQueryStore::new();
        let ih = [9u8; 20];
        store.push(ih, entry(1));
        store.push_many(ih, vec![entry(2), entry(3)]);
        let got = store.take(&ih);
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].port, 1);
        assert_eq!(got[2].port, 3);
        // take 是「取走」：再次取应为空
        assert!(store.take(&ih).is_empty());
    }

    #[test]
    fn test_keys_are_isolated() {
        let store = PeerQueryStore::new();
        store.push([1u8; 20], entry(11));
        store.push([2u8; 20], entry(22));
        assert_eq!(store.len(), 2);
        assert_eq!(store.take(&[1u8; 20])[0].port, 11);
        assert_eq!(store.take(&[2u8; 20])[0].port, 22);
    }
}
