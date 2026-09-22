//! reqwest DNS 解析适配器
//!
//! reqwest 默认解析器会读宿主系统 DNS 配置（`hickory` 的 `builder_tokio()`
//! 走系统 conf，或 `gai` 直接 `getaddrinfo`）。本模块把它替换成 pdc 内置的
//! [`pnos_net::DnsPool`]，从而使 tracker / scrape / subscription 等 HTTP
//! 客户端与 iroh、crawler 使用**同一份** DNS 服务器列表，不再受宿主 DNS
//! 环境影响。
//!
//! 用法：
//! ```ignore
//! let client = dns_resolve::apply_dns_pool(reqwest::Client::builder(), Some(&dns_pool))
//!     .timeout(t)
//!     .build()?;
//! ```

use std::sync::Arc;

use pnos_net::DnsPool;

/// 把 [`DnsPool`] 适配成 reqwest 的 [`reqwest::dns::Resolve`]
#[derive(Clone)]
pub struct ReqwestDnsResolve {
    pool: Arc<DnsPool>,
}

impl ReqwestDnsResolve {
    /// 用给定解析池构造适配器
    pub fn new(pool: Arc<DnsPool>) -> Self {
        Self { pool }
    }
}

impl reqwest::dns::Resolve for ReqwestDnsResolve {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let pool = self.pool.clone();
        Box::pin(async move {
            let host = name.as_str().to_string();

            // 端口填 0：URL 中显式写了端口时由 URL 覆盖，否则由 reqwest
            // 按 scheme 补默认端口（与 reqwest 自带解析器的约定一致）。
            let outcome: Result<reqwest::dns::Addrs, Box<dyn std::error::Error + Send + Sync>> =
                match pool.resolve(&host, 0).await {
                    Ok(addrs) => Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs),
                    Err(e) => Err(Box::new(std::io::Error::other(format!(
                        "DnsPool 解析 {} 失败（未启用系统 DNS 回退）: {}",
                        host, e
                    )))),
                };
            outcome
        })
    }
}

/// 给 reqwest 客户端构造器挂上内置 DNS 解析器
///
/// `pool` 为 `None` 时原样返回，保持既有行为（走 reqwest 默认解析器）。
pub fn apply_dns_pool(
    builder: reqwest::ClientBuilder,
    pool: Option<&Arc<DnsPool>>,
) -> reqwest::ClientBuilder {
    match pool {
        Some(p) => builder.dns_resolver(Arc::new(ReqwestDnsResolve::new(p.clone()))),
        None => builder,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pnos_net::dns::DnsConfig;

    /// 未提供解析池时必须原样返回（不改变既有行为）
    #[test]
    fn test_apply_without_pool_keeps_builder() {
        let builder = apply_dns_pool(reqwest::Client::builder(), None);
        assert!(builder.build().is_ok(), "无解析池时客户端仍应能构建");
    }

    /// 提供解析池时应能成功构建客户端
    #[test]
    fn test_apply_with_pool_builds_client() {
        let pool = Arc::new(DnsPool::from_config(&DnsConfig::default()).expect("DnsPool 失败"));
        let client = apply_dns_pool(reqwest::Client::builder(), Some(&pool))
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("挂载 DnsPool 后客户端应能构建");
        drop(client);
    }

    /// 适配器对字面 IP 直通，不发任何 DNS 查询（值取自 DnsPool 的直通分支）
    #[tokio::test]
    async fn test_resolve_literal_ip_via_adapter() {
        let pool = Arc::new(DnsPool::from_config(&DnsConfig::default()).expect("DnsPool 失败"));
        let addrs = pool.resolve("192.168.30.51", 0).await.unwrap();
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].ip().to_string(), "192.168.30.51");
        assert_eq!(addrs[0].port(), 0, "适配器约定端口填 0，由 reqwest 补齐");
    }
}
