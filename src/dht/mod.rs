//! DHT 发现机制
//!
//! 通过 BitTorrent DHT（分布式哈希表）发现 peer。
//!
//! 支持：
//! - Kademlia 路由表
//! - Bootstrap 节点启动
//! - 路由表自动刷新
//! - 路由表持久化
//! - 节点健康检查和自动淘汰

pub mod client;

pub use client::{DhtConfig, DhtDiscoverer};

/// 常用 DHT Bootstrap 节点
pub const DHT_BOOTSTRAP_NODES: &[(&str, u16)] = &[
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
    ("dht.aelitis.com", 6881),     // Vuze
    ("router.bitcomet.com", 6881), // BitComet
    ("dht.libtorrent.org", 25401), // libtorrent
    // 国内可用的节点
    ("dht.cfcdn.club", 6881),
    ("tracker1.itzmx.com", 6881),
];
