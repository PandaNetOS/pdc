//! DNS 解析池（re-export pnos-net::DnsPool）+ 进程级实例
//!
//! 原 pdc 自实现的 DnsPool 已迁移到 pnos-net，作为生态通用能力。
//! 此处保留 re-export，并补一层「进程级单例」：DNS 解析策略是**整个进程的
//! 环境级配置**（类似时区），各子系统（crawler / iroh / federation / HTTP
//! 客户端）必须共用同一份，否则会出现「有的走内置池、有的偷偷走系统 DNS」。
//!
//! 启动时由 `main` 调用 [`init_global`] 注入 `config.dns`；
//! 其余模块通过 [`global`] 取用。未初始化时按
//! [`DnsConfig::default`] 惰性创建（内置公共 DNS，不读系统 DNS）。

pub use pnos_net::dns::DnsConfig;
pub use pnos_net::DnsPool;

use std::sync::{Arc, OnceLock};

static GLOBAL_POOL: OnceLock<Arc<DnsPool>> = OnceLock::new();

/// 用配置初始化进程级 DNS 池
///
/// 只会生效一次；重复调用返回已存在的实例（保证全进程共用一份缓存与连接）。
pub fn init_global(cfg: &DnsConfig) -> anyhow::Result<Arc<DnsPool>> {
    if let Some(existing) = GLOBAL_POOL.get() {
        return Ok(existing.clone());
    }
    let pool = Arc::new(DnsPool::from_config(cfg)?);
    Ok(GLOBAL_POOL.get_or_init(|| pool).clone())
}

/// 取得进程级 DNS 池
///
/// 未显式初始化时按默认配置（内置 5 个公共 DNS、不读系统 DNS）惰性创建。
pub fn global() -> Arc<DnsPool> {
    GLOBAL_POOL
        .get_or_init(|| {
            Arc::new(DnsPool::from_config(&DnsConfig::default()).expect("DNS 池默认配置初始化失败"))
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 未初始化时也能取到池，且默认拒绝系统 DNS 回退。
    #[test]
    fn test_global_lazy_defaults() {
        let pool = global();
        assert!(!pool.allow_system_fallback(), "默认不得启用系统 DNS 回退");
        assert!(!pool.servers().is_empty(), "默认必须有可用 DNS 服务器");
        // 重复取用必须是同一实例（共用缓存）
        assert!(Arc::ptr_eq(&pool, &global()));
    }

    /// init_global 幂等：重复调用不会替换已存在的实例。
    #[test]
    fn test_init_global_is_idempotent() {
        let cfg = DnsConfig {
            servers: vec!["9.9.9.9".to_string()],
            ..Default::default()
        };
        let first = init_global(&cfg).expect("init_global 失败");
        let other = DnsConfig {
            servers: vec!["1.0.0.1".to_string()],
            ..Default::default()
        };
        let second = init_global(&other).expect("init_global 第二次失败");
        assert!(Arc::ptr_eq(&first, &second), "重复初始化必须返回同一实例");
        assert!(Arc::ptr_eq(&first, &global()), "global() 必须返回该实例");
    }
}
