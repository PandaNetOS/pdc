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
    /// 持久化存储配置
    #[serde(default)]
    pub storage: StorageConfig,
    /// 持久化优化配置
    #[serde(default)]
    pub persistence: PersistenceConfig,
    /// 联邦网络配置
    #[serde(default)]
    pub federation: crate::federation::config::FederationConfig,
    /// 日志级别
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// 是否启用端口自动探测（启动时自动探测一组可用端口）
    #[serde(default = "default_port_auto_alloc")]
    pub port_auto_alloc: bool,
    /// 端口组之间的步长（端口 = 基础端口 + offset * step）
    #[serde(default = "default_port_step")]
    pub port_step: u16,
    /// 是否自动添加 Windows 防火墙入站规则
    #[serde(default = "default_auto_firewall_rule")]
    pub auto_firewall_rule: bool,
}

/// 持久化存储配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// 数据库文件路径
    #[serde(default = "default_storage_path")]
    pub path: String,
    /// 是否启用持久化
    #[serde(default = "default_storage_enabled")]
    pub enabled: bool,
}

fn default_storage_path() -> String {
    "./data/pdc.db".to_string()
}

fn default_storage_enabled() -> bool {
    true
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: default_storage_path(),
            enabled: default_storage_enabled(),
        }
    }
}

/// 持久化优化配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    /// 全量保存间隔（秒）
    #[serde(default = "default_save_interval")]
    pub save_interval_secs: u64,
    /// WAL checkpoint 间隔（秒）
    #[serde(default = "default_checkpoint_interval")]
    pub wal_checkpoint_interval_secs: u64,
    /// WAL 自动 checkpoint 页数
    #[serde(default = "default_wal_autocheckpoint")]
    pub wal_autocheckpoint_pages: u32,
    /// 批量写入大小
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// 是否启用增量持久化（变更日志）
    #[serde(default = "default_false")]
    pub enable_incremental: bool,
    /// 冷热分层检查间隔（秒）
    #[serde(default = "default_tier_check_interval")]
    pub tier_check_interval_secs: u64,
    /// 热数据阈值（秒，最近 N 秒内有活跃视为热）
    #[serde(default = "default_hot_threshold")]
    pub hot_threshold_secs: u64,
    /// 温数据阈值（秒，超过则为冷）
    #[serde(default = "default_warm_threshold")]
    pub warm_threshold_secs: u64,
    /// 内存热数据上限
    #[serde(default = "default_max_hot_in_memory")]
    pub max_hot_in_memory: usize,
}

fn default_save_interval() -> u64 { 60 }
fn default_checkpoint_interval() -> u64 { 600 }
fn default_wal_autocheckpoint() -> u32 { 2000 }
fn default_batch_size() -> usize { 500 }
fn default_false() -> bool { false }
fn default_tier_check_interval() -> u64 { 300 }
fn default_hot_threshold() -> u64 { 1800 }
fn default_warm_threshold() -> u64 { 7200 }
fn default_max_hot_in_memory() -> usize { 5000 }

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            save_interval_secs: default_save_interval(),
            wal_checkpoint_interval_secs: default_checkpoint_interval(),
            wal_autocheckpoint_pages: default_wal_autocheckpoint(),
            batch_size: default_batch_size(),
            enable_incremental: default_false(),
            tier_check_interval_secs: default_tier_check_interval(),
            hot_threshold_secs: default_hot_threshold(),
            warm_threshold_secs: default_warm_threshold(),
            max_hot_in_memory: default_max_hot_in_memory(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_port_auto_alloc() -> bool {
    true
}

fn default_port_step() -> u16 {
    10
}

fn default_auto_firewall_rule() -> bool {
    true
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
            storage: StorageConfig::default(),
            persistence: PersistenceConfig::default(),
            federation: Default::default(),
            log_level: default_log_level(),
            port_auto_alloc: default_port_auto_alloc(),
            port_step: default_port_step(),
            auto_firewall_rule: default_auto_firewall_rule(),
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
    /// 监听端口（超级 Tracker HTTP+UDP）
    #[serde(default = "default_port")]
    pub port: u16,
    /// API/监控端口（绑定局域网，不映射公网，需 token 鉴权）
    #[serde(default = "default_api_port")]
    pub api_port: u16,
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

fn default_api_port() -> u16 {
    6886
}
fn default_work_dir() -> String {
    "pdc-data".to_string()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            port: default_port(),
            api_port: default_api_port(),
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
    /// 中继服务器端口（UDP+TCP，打洞失败时流量转发）
    #[serde(default = "default_relay_port")]
    pub relay_port: u16,
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
fn default_relay_port() -> u16 {
    6881
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
            relay_port: default_relay_port(),
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
    /// 是否启用远程 Tracker 列表自动拉取
    #[serde(default = "default_true")]
    pub enable_remote_tracker: bool,
    /// 远程 Tracker 列表 URL
    #[serde(default = "default_remote_tracker_url")]
    pub remote_tracker_url: String,
    /// 远程 Tracker 列表刷新间隔（秒）
    #[serde(default = "default_remote_tracker_refresh")]
    pub remote_tracker_refresh_secs: u64,
    /// DHT 监听端口
    #[serde(default = "default_dht_port")]
    pub dht_listen_port: u16,
    /// LPD 多播端口（局域网发现，默认禁用）
    #[serde(default = "default_lpd_multicast_port")]
    pub lpd_multicast_port: u16,
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
fn default_lpd_multicast_port() -> u16 {
    6771
}
fn default_remote_tracker_url() -> String {
    "https://cdn.jsdelivr.net/gh/adysec/tracker@main/trackers_best.txt".to_string()
}
fn default_remote_tracker_refresh() -> u64 {
    3600  // 每小时刷新一次
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
            enable_remote_tracker: default_true(),
            remote_tracker_url: default_remote_tracker_url(),
            remote_tracker_refresh_secs: default_remote_tracker_refresh(),
            dht_listen_port: default_dht_port(),
            lpd_multicast_port: default_lpd_multicast_port(),
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
    #[serde(default = "default_true")]
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
    /// uTP 服务端监听端口（UDP）
    #[serde(default = "default_utp_port")]
    pub utp_port: u16,
    /// TCP-PEX 服务端监听端口（TCP）
    #[serde(default = "default_tcp_pex_port")]
    pub tcp_pex_port: u16,
    /// Bootstrap 节点列表
    #[serde(default = "default_crawler_bootstrap_nodes")]
    pub bootstrap_nodes: Vec<(String, u16)>,
}

/// NAT 穿透协议类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NatProtocol {
    /// UPnP IGDv1/v2
    Upnp,
    /// NAT-PMP（Apple/企业路由器）
    #[serde(rename = "natpmp")]
    NatPmp,
    /// PCP（端口控制协议，NAT-PMP 继任者）
    Pcp,
}

impl NatProtocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            NatProtocol::Upnp => "upnp",
            NatProtocol::NatPmp => "natpmp",
            NatProtocol::Pcp => "pcp",
        }
    }
}

/// NAT 穿透配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatConfig {
    /// 是否启用 NAT 自动端口映射
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 映射租期（秒，0=永久，建议 3600）
    #[serde(default = "default_nat_lease")]
    pub lease_duration: u32,
    /// 协议优先级（按顺序尝试，第一个成功的为准）
    #[serde(default = "default_nat_protocols")]
    pub protocols: Vec<NatProtocol>,
    /// 指定绑定网卡名称（为空自动选择有默认路由的网卡）
    #[serde(default)]
    pub bind_interface: Option<String>,
    /// 指定绑定 IP（为空自动检测）
    #[serde(default)]
    pub bind_ip: Option<String>,
    /// STUN 服务器列表（用于公网可达性自检和 NAT 类型检测）
    #[serde(default = "default_stun_servers")]
    pub stun_servers: Vec<String>,
    /// 是否启用公网可达性自检
    #[serde(default = "default_true")]
    pub enable_reachability_check: bool,
    /// 是否启用网关自动重连和映射恢复
    #[serde(default = "default_true")]
    pub enable_auto_recover: bool,
    /// 网关健康检查间隔（秒）
    #[serde(default = "default_nat_health_check_interval")]
    pub health_check_interval: u64,
    /// 映射失败时最大重试次数
    #[serde(default = "default_nat_max_retries")]
    pub max_retries: u32,
}

fn default_nat_lease() -> u32 {
    3600
}

fn default_nat_protocols() -> Vec<NatProtocol> {
    vec![NatProtocol::Upnp, NatProtocol::NatPmp, NatProtocol::Pcp]
}

fn default_stun_servers() -> Vec<String> {
    vec![
        "stun.l.google.com:19302".to_string(),
        "stun1.l.google.com:19302".to_string(),
        "stun.ekiga.net:3478".to_string(),
    ]
}

fn default_nat_health_check_interval() -> u64 {
    300 // 5 分钟
}

fn default_nat_max_retries() -> u32 {
    10
}

impl Default for NatConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            lease_duration: default_nat_lease(),
            protocols: default_nat_protocols(),
            bind_interface: None,
            bind_ip: None,
            stun_servers: default_stun_servers(),
            enable_reachability_check: true,
            enable_auto_recover: true,
            health_check_interval: default_nat_health_check_interval(),
            max_retries: default_nat_max_retries(),
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
fn default_utp_port() -> u16 {
    6883
}
fn default_tcp_pex_port() -> u16 {
    6884
}
fn default_crawler_bootstrap_nodes() -> Vec<(String, u16)> {
    vec![
        // 主流公共 DHT 路由器
        ("router.bittorrent.com".to_string(), 6881),
        ("dht.transmissionbt.com".to_string(), 6881),
        ("router.utorrent.com".to_string(), 6881),
        ("dht.aelitis.com".to_string(), 6881),
        ("router.bitcomet.com".to_string(), 6881),
        ("dht.libtorrent.org".to_string(), 25401),
        // 额外公共节点
        ("dht.aria2.net".to_string(), 6881),
        ("router.magnet2torrent.com".to_string(), 6881),
        ("dht.download.free.fr".to_string(), 6881),
        ("dht.cdnbye.com".to_string(), 6881),
        ("dht.pps001.cn".to_string(), 6881),
        ("tracker1.itzmx.com".to_string(), 6881),
    ]
}

impl Default for CrawlerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            crawl_interval_secs: default_crawl_interval(),
            max_nodes: default_max_crawl_nodes(),
            max_infohashes: default_max_infohashes(),
            listen_port: default_crawler_listen_port(),
            utp_port: default_utp_port(),
            tcp_pex_port: default_tcp_pex_port(),
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
        assert!(config.crawler.enabled);
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
