//! DNS 解析池（re-export pnos-net::DnsPool）
//!
//! 原 pdc 自实现的 DnsPool 已迁移到 pnos-net，作为生态通用能力。
//! 此处保留 re-export，兼容现有代码。

pub use pnos_net::DnsPool;
