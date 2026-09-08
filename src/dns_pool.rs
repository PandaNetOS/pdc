//! DNS 解析池
//!
//! 并行向多个公共 DNS 服务器发起查询，取最快响应，避免单一 DNS 故障或污染。
//! 内置 5 个 DNS：114、阿里、腾讯、谷歌、Cloudflare。
//! hickory-resolver 自带缓存，无需额外实现。

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;
use hickory_resolver::config::{NameServerConfig, Protocol, ResolverConfig, ResolverOpts};
use hickory_resolver::TokioAsyncResolver;
use tracing::debug;

/// 5 个公共 DNS 服务器
const DNS_SERVERS: &[&str] = &[
    "114.114.114.114:53", // 114 DNS
    "223.5.5.5:53",       // 阿里 DNS
    "119.29.29.29:53",    // 腾讯 DNS
    "8.8.8.8:53",         // 谷歌 DNS
    "1.1.1.1:53",         // Cloudflare DNS
];

/// DNS 解析池
pub struct DnsPool {
    resolver: TokioAsyncResolver,
}

impl DnsPool {
    /// 创建 DNS 池，并行查询 5 个公共 DNS
    pub fn new() -> Result<Self> {
        let mut config = ResolverConfig::new();

        for dns in DNS_SERVERS {
            let socket_addr: SocketAddr = dns.parse()?;
            config.add_name_server(NameServerConfig {
                socket_addr,
                protocol: Protocol::Udp,
                tls_dns_name: None,
                trust_negative_responses: false,
                bind_addr: None,
            });
        }

        let mut opts = ResolverOpts::default();
        opts.timeout = Duration::from_secs(3);
        opts.attempts = 2;
        // 并行查询所有 nameserver，取最快响应
        opts.num_concurrent_reqs = DNS_SERVERS.len();

        let resolver = TokioAsyncResolver::tokio(config, opts);

        debug!("[dns_pool] 已初始化 {} 个 DNS 服务器", DNS_SERVERS.len());

        Ok(Self { resolver })
    }

    /// 解析主机名+端口为 SocketAddr 列表
    ///
    /// 并行向 5 个 DNS 发查询，取第一个成功返回的结果。
    /// 解析结果由 hickory-resolver 自动缓存（默认 TTL）。
    pub async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>> {
        let response = self.resolver.lookup_ip(host).await?;

        let addrs: Vec<SocketAddr> = response
            .iter()
            .map(|ip| SocketAddr::new(ip, port))
            .collect();

        if addrs.is_empty() {
            anyhow::bail!("DNS 解析 {} 未返回任何记录", host);
        }

        debug!("[dns_pool] {} -> {} 个地址", host, addrs.len());
        Ok(addrs)
    }
}
