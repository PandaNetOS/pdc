//! LPD（Local Peer Discovery）发现器
//!
//! 实现 BEP 14：通过多播 UDP 在局域网内发现 peer。
//! 多播地址 239.192.152.143:6771。

pub mod client;

pub use client::{LpdConfig, LpdDiscoverer};

/// LPD 多播地址
pub const LPD_MULTICAST_ADDR: &str = "239.192.152.143";
/// LPD 多播端口
pub const LPD_MULTICAST_PORT: u16 = 6771;
