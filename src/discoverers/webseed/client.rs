//! WebSeed 客户端
//!
//! 实现 BEP 19 WebSeed（HTTP/FTP 直链下载源）。
//!
//! WebSeed 不是传统的 BitTorrent peer，而是 HTTP 下载源。
//! 在 PDC 中通过 PeerInfo 承载：
//! - addr 使用虚拟地址 `0.0.0.0:<index>`
//! - source = PeerSource::WebSeed
//! - metadata["webseed_url"] = 实际 URL
//!
//! 下载引擎应检查 source == WebSeed，从 metadata 中获取 URL，
//! 使用 HTTP Range 请求下载，而不是 BitTorrent 协议。

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::RwLock;
use tracing::debug;

use crate::traits::{AnnounceEvent, DiscovererStats, DiscovererType, PeerDiscoverer};
use crate::types::{Infohash, PeerInfo, PeerSource};

/// WebSeed 配置
#[derive(Debug, Clone, Default)]
pub struct WebSeedConfig {
    /// URL 列表（HTTP/FTP 直链）
    pub urls: Vec<String>,
    /// 每个 infohash 对应的 URL 覆盖（可选）
    pub infohash_urls: HashMap<Infohash, Vec<String>>,
    /// 是否启用
    pub enabled: bool,
}

/// WebSeed 发现器
pub struct WebSeedDiscoverer {
    config: WebSeedConfig,
    stats: Arc<RwLock<DiscovererStats>>,
}

impl WebSeedDiscoverer {
    /// 创建新的 WebSeed 发现器
    pub fn new(config: WebSeedConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RwLock::new(DiscovererStats::default())),
        }
    }

    /// 创建默认配置的 WebSeed 发现器
    pub fn with_default_config() -> Self {
        Self::new(WebSeedConfig::default())
    }

    /// 添加全局 URL
    pub fn add_url(&mut self, url: String) {
        self.config.urls.push(url);
    }

    /// 为指定 infohash 添加 URL
    pub fn add_infohash_url(&mut self, infohash: Infohash, url: String) {
        self.config
            .infohash_urls
            .entry(infohash)
            .or_default()
            .push(url);
    }

    /// 获取指定 infohash 可用的所有 URL
    fn get_urls_for_infohash(&self, infohash: &Infohash) -> Vec<String> {
        let mut urls = self.config.urls.clone();
        if let Some(specific) = self.config.infohash_urls.get(infohash) {
            urls.extend(specific.iter().cloned());
        }
        urls
    }

    /// 将 URL 转换为 PeerInfo
    fn url_to_peer_info(url: &str, index: usize) -> PeerInfo {
        // 用虚拟地址 0.0.0.0:<index+1> 区分不同 URL
        let virtual_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), (index + 1) as u16);
        let mut info = PeerInfo::new(virtual_addr, PeerSource::WebSeed);
        info.metadata
            .insert("webseed_url".to_string(), url.to_string());
        info.priority_score = 200.0; // WebSeed 通常速度快，高优先级
        info
    }

    /// 记录请求结果
    fn record_result(&self, success: bool, peers_count: usize, duration: Duration) {
        let mut stats = self.stats.write();
        if success {
            stats.record_success(peers_count, duration.as_millis() as f64);
        } else {
            stats.record_failure();
        }
    }
}

#[async_trait]
impl PeerDiscoverer for WebSeedDiscoverer {
    fn name(&self) -> &str {
        "webseed"
    }

    fn discoverer_type(&self) -> DiscovererType {
        DiscovererType::WebSeed
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled && !self.config.urls.is_empty()
    }

    async fn discover_peers(
        &self,
        infohash: &Infohash,
        limit: usize,
    ) -> anyhow::Result<Vec<PeerInfo>> {
        let start = Instant::now();

        let urls = self.get_urls_for_infohash(infohash);
        let peers: Vec<PeerInfo> = urls
            .iter()
            .take(limit)
            .enumerate()
            .map(|(i, url)| Self::url_to_peer_info(url, i))
            .collect();

        self.record_result(true, peers.len(), start.elapsed());

        debug!(
            "[webseed] 发现完成: {} 个 HTTP 源 (耗时 {:?})",
            peers.len(),
            start.elapsed()
        );

        Ok(peers)
    }

    async fn announce(
        &self,
        _infohash: &Infohash,
        _port: u16,
        _event: AnnounceEvent,
    ) -> anyhow::Result<()> {
        // WebSeed 不需要 announce
        Ok(())
    }

    async fn health_check(&self) -> bool {
        // WebSeed 是静态配置，只要有 URL 就健康
        !self.config.urls.is_empty() || !self.config.infohash_urls.is_empty()
    }

    fn stats(&self) -> DiscovererStats {
        self.stats.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_webseed_config_default() {
        let config = WebSeedConfig::default();
        assert!(config.urls.is_empty());
        assert!(!config.enabled);
    }

    #[test]
    fn test_webseed_discoverer_creation() {
        let discoverer = WebSeedDiscoverer::with_default_config();
        assert_eq!(discoverer.name(), "webseed");
        assert!(!discoverer.is_enabled()); // 无 URL 时不启用
    }

    #[test]
    fn test_add_url() {
        let mut discoverer = WebSeedDiscoverer::with_default_config();
        discoverer.config.enabled = true;
        discoverer.add_url("http://example.com/file".to_string());
        assert!(discoverer.is_enabled());
    }

    #[tokio::test]
    async fn test_discover_peers() {
        let mut config = WebSeedConfig::default();
        config.enabled = true;
        config.urls = vec![
            "http://example.com/file1".to_string(),
            "http://example.com/file2".to_string(),
        ];
        let discoverer = WebSeedDiscoverer::new(config);

        let infohash = [0u8; 20];
        let peers = discoverer.discover_peers(&infohash, 10).await.unwrap();
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].source, PeerSource::WebSeed);
        assert_eq!(
            peers[0].metadata.get("webseed_url").unwrap(),
            "http://example.com/file1"
        );
        assert_eq!(peers[0].priority_score, 200.0);
    }

    #[tokio::test]
    async fn test_discover_peers_with_limit() {
        let mut config = WebSeedConfig::default();
        config.enabled = true;
        config.urls = vec![
            "http://example.com/1".to_string(),
            "http://example.com/2".to_string(),
            "http://example.com/3".to_string(),
        ];
        let discoverer = WebSeedDiscoverer::new(config);

        let infohash = [0u8; 20];
        let peers = discoverer.discover_peers(&infohash, 2).await.unwrap();
        assert_eq!(peers.len(), 2);
    }

    #[test]
    fn test_url_to_peer_info() {
        let info = WebSeedDiscoverer::url_to_peer_info("http://example.com/file", 0);
        assert_eq!(info.addr.to_string(), "0.0.0.0:1");
        assert_eq!(info.source, PeerSource::WebSeed);
        assert_eq!(
            info.metadata.get("webseed_url").unwrap(),
            "http://example.com/file"
        );
    }

    #[test]
    fn test_infohash_specific_urls() {
        let mut config = WebSeedConfig::default();
        config.enabled = true;
        config.urls = vec!["http://global.com".to_string()];
        let infohash = [1u8; 20];
        config
            .infohash_urls
            .insert(infohash, vec!["http://specific.com".to_string()]);

        let discoverer = WebSeedDiscoverer::new(config);
        let urls = discoverer.get_urls_for_infohash(&infohash);
        assert_eq!(urls.len(), 2);
        assert!(urls.contains(&"http://global.com".to_string()));
        assert!(urls.contains(&"http://specific.com".to_string()));
    }
}
