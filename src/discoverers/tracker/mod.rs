//! Tracker 发现机制
//!
//! 通过 BitTorrent Tracker 协议（HTTP/UDP）发现 peer。
//!
//! 支持：
//! - HTTP/HTTPS Tracker
//! - UDP Tracker（更高效）
//! - 多 Tracker 并发请求
//! - 自动重试和故障转移
//! - Tracker 健康检查和自动淘汰

pub mod client;
pub mod udp;

pub use client::{TrackerConfig, TrackerDiscoverer};

/// 常用公共 Tracker 列表
pub const PUBLIC_TRACKERS: &[&str] = &[
    // === HTTP Tracker（经过 DNS 验证可用）===
    "http://tracker1.itzmx.com:8080/announce",
    "http://tracker2.itzmx.com:6961/announce",
    "http://tracker3.itzmx.com:6961/announce",
    "http://tracker.k.vu:6969/announce",
    "http://www.peckservers.com:9000/announce",
    "http://tracker.opentrackr.org:1337/announce",
    // === HTTPS Tracker（经过 DNS 验证可用）===
    "https://tracker.kuroy.me/announce",
    "https://www.peckservers.com:9443/announce",
    "https://trackers.mlsub.net/announce",
    // === UDP Tracker（速度更快，连接数更多，经过 DNS 验证可用）===
    "udp://tracker1.itzmx.com:8080/announce",
    "udp://tracker2.itzmx.com:6961/announce",
    "udp://tracker3.itzmx.com:6961/announce",
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.moeking.me:6969/announce",
    "udp://tracker.dler.org:6969/announce",
];
