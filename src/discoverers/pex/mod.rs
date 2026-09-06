//! PEX 发现机制
//!
//! 通过 Peer Exchange（PEX）从已连接的 peer 发现更多 peer。
//!
//! PEX 是 BitTorrent 协议的扩展，允许 peer 之间交换它们知道的其他 peer 列表。
//! 这是一种去中心化的 peer 发现方式，不依赖 tracker 或 DHT。

pub mod client;
pub mod message;

pub use client::{PexConfig, PexDiscoverer};
