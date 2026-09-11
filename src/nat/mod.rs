//! NAT 模块兼容层
//!
//! 实际实现已迁移到 pnos-net crate（pnos-sdk/crates/pnos-net/src/nat/）。
//! 此处保留 re-export 以兼容现有 `crate::nat::xxx` 引用。

pub use pnos_net::nat::*;
