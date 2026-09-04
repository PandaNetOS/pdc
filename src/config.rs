//! 配置管理
//!
//! 支持从 config.yaml 加载配置，也支持环境变量覆盖。
//! 配置结构按模块组织：server、super_tracker、discoverers、cache、health_check、crawler。

use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// 根配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PdcConfig {
    /// 服务端配置
    #[serde(default)]
    pub server: ServerConfig,
    /// 超级 Tracker 配置
    #[serde(default)]
    pub super_tracker: SuperTrackerConfig,
    /// 发现器配置
    #[serde(default)]
    pub discoverers: DiscoverersConfig,
    /// 缓存配置
    #[serde(default)]
    pub cache: CacheConfig,
    /// 健康检查配置
    #[serde(default)]
    pub health_check: HealthCheckConfig,
    /// 爬虫配置
    #[serde(default)]
    pub crawler: CrawlerConfig,
    /// NAT 穿透配置
    #[serde(default)]
    pub nat: NatConfig,
    /// 日志级别
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for PdcConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            super_tracker: SuperTrackerConfig::default(),
            discoverers: DiscoverersConfig::default(),
            cache: CacheConfig::default(),
            health_check: HealthCheckConfig::default(),
            crawler: CrawlerConfig::default(),
            nat: NatConfig::default(),
            log_level: default_log_level(),
        }
    }
}

impl PdcConfig {
    /// 从 YAML 文件加载配置
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: PdcConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// 从 YAML 字符串加载配置
    pub fn from_yaml(content: &str) -> anyhow::Result<Self> {
        let config: PdcConfig = serde_yaml::from_str(content)?;
        Ok(config)
    }

    /// 加载配置：优先从指定文件加载，文件不存在则使用默认值
    pub fn load_or_default<P: AsRef<Path>>(path: P) -> Self {
        match Self::from_file(&path) {
            Ok(config) => {
                tracing::info!("[config] 已从 {:?} 加载配置", path.as_ref());
                config
            }
            Err(e) => {
                tracing::warn!(
                    "[config] 加载配置文件失败（{:?}），使用默认配置: {}",
                    path.as_ref(),
                    e
                );
                Self::default()
            }
        }
    }

    /// 序列化为 YAML（用于保存配置）
    pub fn to_yaml(&self) -> anyhow::Result<String> {
        Ok(serde_yaml::to_string(self)?)
    }
}

// ---------------------------------------------------------------------------
// 服务端配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// 监听地址
    #[serde(default = "default_listen")]
    pub listen: String,
    /// 监听端口
    #[serde(default = "default_port")]
    pub port: u16,
    /// API 鉴权 token（为空则不鉴权）
    #[serde(default)]
    pub token: Option<String>,
    /// 工作目录
    #[serde(default = "default_work_dir")]
    pub work_dir: String,
}

fn default_listen() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    6880
}
fn default_work_dir() -> String {
    "pdc-data".to_string()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            port: default_port(),
            token: None,
            work_dir: default_work_dir(),
        }
    }
}

// ---------------------------------------------------------------------------
// 超级 Tracker 配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuperTrackerConfig {
    /// 是否启用超级 Tracker（/announce /scrape）
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// announce 路径
    #[serde(default = "default_announce_path")]
    pub announce_path: String,
    /// scrape 路径
    #[serde(default = "default_scrape_path")]
    pub scrape_path: String,
    /// 推荐的再次 announce 间隔（秒）
    #[serde(default = "default_interval")]
    pub interval: i64,
    /// 最小间隔（秒）
    #[serde(default = "default_min_interval")]
    pub min_interval: i64,
    /// 每次 announce 返回的最大 peer 数
    #[serde(default = "default_numwant")]
    pub max_numwant: usize,
    /// Peer 在超级 Tracker 中的过期时间（秒）
    #[serde(default = "default_peer_ttl")]
    pub peer_ttl_secs: u64,
    /// 是否在 announce 时触发后端发现器主动发现
    #[serde(default = "default_true")]
    pub trigger_backend_discovery: bool,
    /// UDP Tracker 端口（None 则与 HTTP 端口相同）
    #[serde(default)]
    pub udp_port: Option<u16>,
}

fn default_true() -> bool {
    true
}
fn default_announce_path() -> String {
    "/announce".to_string()
}
fn default_scrape_path() -> String {
    "/scrape".to_string()
}
fn default_interval() -> i64 {
    1800
}
fn default_min_interval() -> i64 {
    900
}
fn default_numwant() -> usize {
    100
}
fn default_peer_ttl() -> u64 {
    3600
}

impl Default for SuperTrackerConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            announce_path: default_announce_path(),
            scrape_path: default_scrape_path(),
            interval: default_interval(),
            min_interval: default_min_interval(),
            max_numwant: default_numwant(),
            peer_ttl_secs: default_peer_ttl(),
            trigger_backend_discovery: default_true(),
            udp_port: None,
        }
    }
}

// ---------------------------------------------------------------------------
// 发现器配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverersConfig {
    /// 是否启用 Tracker 发现器
    #[serde(default = "default_true")]
    pub enable_tracker: bool,
    /// 是否启用 DHT 发现器
    #[serde(default = "default_true")]
    pub enable_dht: bool,
    /// 是否启用 PEX 发现器
    #[serde(default = "default_true")]
    pub enable_pex: bool,
    /// 是否启用 LPD 发现器（局域网多播）
    #[serde(default)]
    pub enable_lpd: bool,
    /// 是否启用 WebSeed 发现器
    #[serde(default)]
    pub enable_webseed: bool,
    /// 单次发现超时（秒）
    #[serde(default = "default_discovery_timeout")]
    pub discovery_timeout_secs: u64,
    /// 最大并发发现器数
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
    /// 自定义 Tracker 列表（为空则使用内置公共 Tracker）
    #[serde(default)]
    pub custom_trackers: Vec<String>,
    /// DHT 监听端口
    #[serde(default = "default_dht_port")]
    pub dht_listen_port: u16,
    /// 发现超时（Duration，供聚合器使用）
    #[serde(skip)]
    pub discovery_timeout: Duration,
}

fn default_discovery_timeout() -> u64 {
    30
}
fn default_max_concurrent() -> usize {
    10
}
fn default_dht_port() -> u16 {
    6881
}

impl Default for DiscoverersConfig {
    fn default() -> Self {
        Self {
            enable_tracker: default_true(),
            enable_dht: default_true(),
            enable_pex: default_true(),
            enable_lpd: false,
            enable_webseed: false,
            discovery_timeout_secs: default_discovery_timeout(),
            max_concurrent: default_max_concurrent(),
            custom_trackers: vec![],
            dht_listen_port: default_dht_port(),
            discovery_timeout: Duration::from_secs(default_discovery_timeout()),
        }
    }
}

// ---------------------------------------------------------------------------
// 缓存配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// 最大缓存 peer 数（全局）
    #[serde(default = "default_max_cached")]
    pub max_cached_peers: usize,
    /// Peer 过期时间（秒）
    #[serde(default = "default_cache_ttl")]
    pub peer_ttl_secs: u64,
    /// 每次发现的最大 peer 数
    #[serde(default = "default_max_peers")]
    pub max_peers_per_discovery: usize,
}

fn default_max_cached() -> usize {
    10000
}
fn default_cache_ttl() -> u64 {
    86400
}
fn default_max_peers() -> usize {
    200
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_cached_peers: default_max_cached(),
            peer_ttl_secs: default_cache_ttl(),
            max_peers_per_discovery: default_max_peers(),
        }
    }
}

// ---------------------------------------------------------------------------
// 健康检查配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// 发现器健康检查间隔（秒）
    #[serde(default = "default_hc_interval")]
    pub interval_secs: u64,
    /// 缓存清理间隔（秒）
    #[serde(default = "default_cleanup_interval")]
    pub cache_cleanup_interval_secs: u64,
    /// 统计输出间隔（秒）
    #[serde(default = "default_stats_interval")]
    pub stats_output_interval_secs: u64,
}

fn default_hc_interval() -> u64 {
    300
}
fn default_cleanup_interval() -> u64 {
    600
}
fn default_stats_interval() -> u64 {
    300
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_hc_interval(),
            cache_cleanup_interval_secs: default_cleanup_interval(),
            stats_output_interval_secs: default_stats_interval(),
        }
    }
}

// ---------------------------------------------------------------------------
// 爬虫配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrawlerConfig {
    /// 是否启用爬虫引擎
    #[serde(default)]
    pub enabled: bool,
    /// 爬行间隔（秒）
    #[serde(default = "default_crawl_interval")]
    pub crawl_interval_secs: u64,
    /// 最大爬行节点数
    #[serde(default = "default_max_crawl_nodes")]
    pub max_nodes: usize,
    /// 最大收集的 infohash 数
    #[serde(default = "default_max_infohashes")]
    pub max_infohashes: usize,
    /// 爬虫监听的 UDP 端口
    #[serde(default = "default_crawler_listen_port")]
    pub listen_port: u16,
    /// Bootstrap 节点列表
    #[serde(default = "default_crawler_bootstrap_nodes")]
    pub bootstrap_nodes: Vec<(String, u16)>,
}

/// NAT 穿透配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatConfig {
    /// 是否启用 UPnP 自动端口映射
    #[serde(default)]
    pub enabled: bool,
    /// 映射租期（秒，0=永久，建议 3600）
    #[serde(default = "default_nat_lease")]
    pub lease_duration: u32,
}

fn default_nat_lease() -> u32 {
    3600
}

impl Default for NatConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            lease_duration: default_nat_lease(),
        }
    }
}

fn default_crawl_interval() -> u64 {
    60
}
fn default_max_crawl_nodes() -> usize {
    1000
}
fn default_max_infohashes() -> usize {
    100000
}
fn default_crawler_listen_port() -> u16 {
    6882
}
fn default_crawler_bootstrap_nodes() -> Vec<(String, u16)> {
    vec![
        ("router.bittorrent.com".to_string(), 6881),
        ("dht.transmissionbt.com".to_string(), 6881),
        ("router.utorrent.com".to_string(), 6881),
        ("dht.aelitis.com".to_string(), 6881),
        ("bootstap.bitcomet.com".to_string(), 6881),
    ]
}

impl Default for CrawlerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            crawl_interval_secs: default_crawl_interval(),
            max_nodes: default_max_crawl_nodes(),
            max_infohashes: default_max_infohashes(),
            listen_port: default_crawler_listen_port(),
            bootstrap_nodes: default_crawler_bootstrap_nodes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PdcConfig::default();
        assert_eq!(config.server.port, 6880);
        assert!(config.super_tracker.enabled);
        assert_eq!(config.super_tracker.interval, 1800);
        assert!(config.discoverers.enable_tracker);
        assert!(!config.discoverers.enable_lpd);
        assert!(!config.crawler.enabled);
    }

    #[test]
    fn test_parse_yaml() {
        let yaml = r#"
server:
  port: 9090
super_tracker:
  interval: 3600
discoverers:
  enable_lpd: true
crawler:
  enabled: true
"#;
        let config = PdcConfig::from_yaml(yaml).unwrap();
        assert_eq!(config.server.port, 9090);
        assert_eq!(config.super_tracker.interval, 3600);
        assert!(config.discoverers.enable_lpd);
        assert!(config.crawler.enabled);
        // 未指定的字段使用默认值
        assert_eq!(config.cache.max_cached_peers, 10000);
    }
}
