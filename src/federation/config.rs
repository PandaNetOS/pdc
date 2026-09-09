//! 联邦网络配置
//!
//! 控制 PDC 联邦网络的所有行为参数，包括监听端口、连接数、心跳、NAT 映射、同步等。

use serde::{Deserialize, Serialize};

/// 联邦网络配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationConfig {
    /// 是否启用联邦网络
    #[serde(default)]
    pub enabled: bool,
    /// 自定义节点 ID（十六进制字符串，40 字符），为空则自动生成
    #[serde(default)]
    pub node_id: Option<String>,
    /// 种子节点列表（host:port 格式）
    #[serde(default)]
    pub seed_nodes: Vec<String>,
    /// 联邦监听端口
    #[serde(default = "default_listen_port")]
    pub listen_port: u16,
    /// 最大连接数
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// 目标邻居数
    #[serde(default = "default_target_neighbors")]
    pub target_neighbors: usize,
    /// 心跳间隔（秒）
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,
    /// 心跳超时（秒），超过此时间无响应则断开
    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout_secs: u64,
    /// 是否启用 NAT 端口映射
    #[serde(default = "default_true")]
    pub nat_mapping_enabled: bool,
    /// STUN 服务器列表
    #[serde(default)]
    pub stun_servers: Vec<String>,
    /// 是否启用中继（阶段3实现）
    #[serde(default = "default_true")]
    pub enable_relay: bool,
    /// 中继带宽限制（Mbps）
    #[serde(default = "default_relay_bandwidth")]
    pub relay_bandwidth_limit_mbps: u32,
    /// 中继最大连接数
    #[serde(default = "default_relay_max_connections")]
    pub relay_max_connections: usize,
    /// Gossip 间隔（毫秒，阶段2实现）
    #[serde(default = "default_gossip_interval")]
    pub gossip_interval_ms: u64,
    /// Gossip 扇出数（阶段2实现）
    #[serde(default = "default_gossip_fanout")]
    pub gossip_fanout: usize,
    /// 是否启用 Peer 同步（阶段2实现）
    #[serde(default = "default_true")]
    pub sync_peer_enabled: bool,
    /// 是否启用 Node 同步
    #[serde(default = "default_true")]
    pub sync_node_enabled: bool,
    /// Node 同步间隔（秒）
    #[serde(default = "default_sync_node_interval")]
    pub sync_node_interval_secs: u64,
    /// 是否启用 Infohash 同步（阶段2实现）
    #[serde(default = "default_true")]
    pub sync_infohash_enabled: bool,
    /// 是否启用 Tracker 同步（阶段3实现）
    #[serde(default = "default_true")]
    pub sync_tracker_enabled: bool,
}

fn default_listen_port() -> u16 { 6885 }
fn default_max_connections() -> usize { 32 }
fn default_target_neighbors() -> usize { 8 }
fn default_heartbeat_interval() -> u64 { 30 }
fn default_heartbeat_timeout() -> u64 { 90 }
fn default_true() -> bool { true }
fn default_relay_bandwidth() -> u32 { 10 }
fn default_relay_max_connections() -> usize { 5 }
fn default_gossip_interval() -> u64 { 1000 }
fn default_gossip_fanout() -> usize { 3 }
fn default_sync_node_interval() -> u64 { 300 }

impl Default for FederationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            node_id: None,
            seed_nodes: Vec::new(),
            listen_port: default_listen_port(),
            max_connections: default_max_connections(),
            target_neighbors: default_target_neighbors(),
            heartbeat_interval_secs: default_heartbeat_interval(),
            heartbeat_timeout_secs: default_heartbeat_timeout(),
            nat_mapping_enabled: default_true(),
            stun_servers: Vec::new(),
            enable_relay: default_true(),
            relay_bandwidth_limit_mbps: default_relay_bandwidth(),
            relay_max_connections: default_relay_max_connections(),
            gossip_interval_ms: default_gossip_interval(),
            gossip_fanout: default_gossip_fanout(),
            sync_peer_enabled: default_true(),
            sync_node_enabled: default_true(),
            sync_node_interval_secs: default_sync_node_interval(),
            sync_infohash_enabled: default_true(),
            sync_tracker_enabled: default_true(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = FederationConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.listen_port, 6885);
        assert_eq!(cfg.max_connections, 32);
        assert_eq!(cfg.target_neighbors, 8);
        assert_eq!(cfg.heartbeat_interval_secs, 30);
        assert_eq!(cfg.heartbeat_timeout_secs, 90);
        assert!(cfg.nat_mapping_enabled);
        assert!(cfg.enable_relay);
        assert_eq!(cfg.relay_bandwidth_limit_mbps, 10);
        assert_eq!(cfg.relay_max_connections, 5);
        assert_eq!(cfg.gossip_interval_ms, 1000);
        assert_eq!(cfg.gossip_fanout, 3);
        assert!(cfg.sync_node_enabled);
        assert_eq!(cfg.sync_node_interval_secs, 300);
    }

    #[test]
    fn test_config_yaml_roundtrip() {
        let yaml = r#"
enabled: true
listen_port: 7000
seed_nodes:
  - "1.2.3.4:6885"
  - "5.6.7.8:6885"
max_connections: 64
"#;
        let cfg: FederationConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.listen_port, 7000);
        assert_eq!(cfg.seed_nodes.len(), 2);
        assert_eq!(cfg.max_connections, 64);
        // 未指定字段使用默认值
        assert_eq!(cfg.target_neighbors, 8);
        assert!(cfg.sync_node_enabled);
    }

    #[test]
    fn test_config_serialize() {
        let cfg = FederationConfig::default();
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        assert!(yaml.contains("listen_port: 6885"));
        assert!(yaml.contains("enabled: true"));
    }
}
