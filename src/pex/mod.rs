//! PEX 发现机制
//!
//! 通过 Peer Exchange（PEX）从已连接的 peer 发现更多 peer。
//!
//! PEX 是 BitTorrent 协议的扩展，允许 peer 之间交换它们知道的其他 peer 列表。
//! 这是一种去中心化的 peer 发现方式，不依赖 tracker 或 DHT。
//!
//! 支持：
//! - PEX v1（扩展协议）
//! - PEX v2（ut_pex）
//! - 从已连接 peer 持续获取新 peer
//! - peer 质量评分和优先级排序

pub mod client;

pub use client::{PexConfig, PexDiscoverer};
