//! WebSeed 发现器
//!
//! 实现 BEP 19：HTTP/FTP 直链下载源（url-list）。
//! WebSeed 不是传统意义上的 peer，而是 HTTP 下载源，
//! 通过 PeerInfo.metadata 中的 "webseed_url" 字段传递 URL。

pub mod client;

pub use client::{WebSeedConfig, WebSeedDiscoverer};
