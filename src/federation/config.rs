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
    #[serde(default = "default_stun_servers")]
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
    /// 出站直连建立后是否自动与对端协商中继通道。
    /// 注意：联邦中继是建立在“已有直连控制连接”之上的数据隧道，
    /// 无法在打洞失败（无直连）时作为 NAT 回退手段。
    #[serde(default = "default_relay_auto_setup_on_connect")]
    pub relay_auto_setup_on_connect: bool,
    /// Gossip 间隔（毫秒，阶段2实现）
    #[serde(default = "default_gossip_interval")]
    pub gossip_interval_ms: u64,
    /// Gossip 扇出数（阶段2实现）
    #[serde(default = "default_gossip_fanout")]
    pub gossip_fanout: usize,
    /// Gossip 已处理消息去重缓存（seen_msgs）的分片数。
    /// 每片独立一把读写锁，消除多 repo 并行 flush 时的全局写锁竞争。
    /// 总去重容量固定为 10000，每片容量 = 总容量 / 分片数。
    #[serde(default = "default_gossip_seen_shards")]
    pub gossip_seen_shards: usize,
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
    /// 是否启用 DHT 魔法 infohash 自动发现
    #[serde(default = "default_true")]
    pub dht_discovery_enabled: bool,
    /// DHT 发现间隔（秒）
    #[serde(default = "default_dht_interval")]
    pub dht_discovery_interval_secs: u64,
    /// 是否启用节点缓存持久化（重启后自动重连历史节点）
    #[serde(default = "default_true")]
    pub peer_cache_enabled: bool,
    /// 最大缓存节点数
    #[serde(default = "default_peer_cache_max")]
    pub peer_cache_max_nodes: usize,
    /// TCP 传输写入超时（秒），超过此时间 write_all 未完成则报错
    #[serde(default = "default_transport_write_timeout")]
    pub transport_write_timeout_secs: u64,
    /// Gossip 连续发送失败断开阈值，达到此次数则主动断开连接
    #[serde(default = "default_gossip_max_consecutive_failures")]
    pub gossip_max_consecutive_failures: u32,
    /// 重连冷却时间（秒），断开后此时间内不主动重连同一节点
    #[serde(default = "default_reconnect_cooldown_secs")]
    pub reconnect_cooldown_secs: u64,
    /// 重量级消息处理（GossipBatch/MerkleRepair）的最大并发数。
    /// 这些处理涉及大量 DB 写入，通过 Semaphore 限制 spawn_blocking 并发，
    /// 避免无界生成任务导致内存暴涨，同时不阻塞消息接收循环。
    #[serde(default = "default_heavy_task_max_concurrency")]
    pub heavy_task_max_concurrency: usize,
    /// Gossip 发送端全局出口速率限制：每秒最多发送字节数（含帧头）。
    /// 超限则跳过本 tick 的后续发送，消息留待下 tick 重试，
    /// 防止对端接收缓冲区打满导致 write_all 超时断连。
    #[serde(default = "default_gossip_max_bytes_per_second")]
    pub gossip_max_bytes_per_second: u64,
    /// Gossip 发送端全局出口速率限制：每秒最多发送消息条数。
    #[serde(default = "default_gossip_max_messages_per_second")]
    pub gossip_max_messages_per_second: u32,
    /// 接收端单连接待处理消息数阈值，超过则输出背压告警日志。
    /// 仅监控告警（TCP 流控 + 发送端限流为主要手段）。
    #[serde(default = "default_receive_pending_threshold")]
    pub receive_pending_threshold: u32,
    /// 初始全量同步每批条目数（发送端 submit_gossip_batch 的 batch_size）。
    /// 默认 5000，比旧的硬编码 100 减少 50 倍消息开销。
    #[serde(default = "default_initial_sync_batch_size")]
    pub initial_sync_batch_size: usize,
    /// 接收端 GossipBatch 攒批刷新间隔（毫秒）。
    /// 收到的 GossipBatch 先进入 per-connection 缓冲区，到期后一次性 flush。
    #[serde(default = "default_gossip_flush_interval_ms")]
    pub gossip_flush_interval_ms: u64,
    /// 接收端 GossipBatch 攒批最大批次数，达到则立即刷新。
    #[serde(default = "default_gossip_flush_max_batches")]
    pub gossip_flush_max_batches: usize,
    /// 全量同步专门通道每批条目数（FullSyncBatch 的 entries 数量）。
    #[serde(default = "default_full_sync_batch_size")]
    pub full_sync_batch_size: usize,
    /// 全量同步窗口大小（发送方未确认的在途批次数）。
    #[serde(default = "default_full_sync_window_size")]
    pub full_sync_window_size: usize,
    /// 全量同步期间 Gossip 每秒最多发送消息条数（临时放开限流）。
    /// SyncManager 在 trigger_initial_sync 期间通过 GossipEngine flag 切换到此上限。
    #[serde(default = "default_full_sync_gossip_max_messages_per_second")]
    pub full_sync_gossip_max_messages_per_second: u32,
    /// 全量同步期间 Gossip 每秒最多发送字节数（临时放开限流）。
    #[serde(default = "default_full_sync_gossip_max_bytes_per_second")]
    pub full_sync_gossip_max_bytes_per_second: u64,
    /// Merkle 树异步批量更新间隔（毫秒）。
    /// 联邦同步 apply 时仅入队，后台任务每此间隔批量 flush 到 Merkle 树，降低 apply 路径 CPU 占用。
    #[serde(default = "default_merkle_async_update_interval_ms")]
    pub merkle_async_update_interval_ms: u64,
    /// Merkle 树异步批量更新触发阈值（条数）。
    /// 队列累计达到此条数时通过 Notify 立即唤醒后台 flush，无需等待间隔。
    #[serde(default = "default_merkle_async_update_batch_size")]
    pub merkle_async_update_batch_size: usize,
    /// 发送端 GossipBatch 合并为 bulk 帧的最大 batch 数量。
    /// 同一连接的多个 batch 合并为一个 GossipBatchBulk 发送，减少网络往返。
    #[serde(default = "default_gossip_bulk_max_batches")]
    pub gossip_bulk_max_batches: usize,
    /// 发送端 GossipBatch 合并为 bulk 帧的最大总字节数（不含帧头）。
    /// 达到此限制立即发送当前 bulk，开始下一个。
    #[serde(default = "default_gossip_bulk_max_bytes")]
    pub gossip_bulk_max_bytes: usize,
    /// 多连接传播时是否并行发送到所有连接。
    /// true（默认）：每个连接独立 tokio task 并行发送，多节点场景下超级节点带宽
    /// 不被串行分摊（测试中 node2 仅 8,450 条/s vs node1 35,800 条/s 的主因）。
    /// 限流计数器为 AtomicU64，多 task 并发安全。
    /// false：串行发送（单连接 localhost 场景并行无收益且放大 write timeout，保留作为 fallback）。
    #[serde(default = "default_parallel_propagation")]
    pub parallel_propagation: bool,
    /// 出站连接就绪后，等待多少毫秒再选择全量数据源并发送 FullSyncRequest。
    /// 留出时间让其他对等节点也完成连接建立，便于在多个对端中选出最佳数据源。
    #[serde(default = "default_full_sync_source_select_settle_ms")]
    pub full_sync_source_select_settle_ms: u64,
    /// 发起拉取后，接收全量期间保持「收到的 Gossip 只本地写入不转发」的稳态时长（毫秒）。
    /// 超过此时长自动恢复正常转发（Gossip 拉取无显式完成信号，用可配置时长兜底）。
    #[serde(default = "default_full_sync_receiving_settle_ms")]
    pub full_sync_receiving_settle_ms: u64,
}

fn default_listen_port() -> u16 { 6885 }
fn default_max_connections() -> usize { 32 }
fn default_target_neighbors() -> usize { 8 }
fn default_heartbeat_interval() -> u64 { 30 }
fn default_heartbeat_timeout() -> u64 { 90 }
fn default_true() -> bool { true }
fn default_stun_servers() -> Vec<String> {
    // 国内可达的 STUN 服务器优先（小米/B站/腾讯），其次 Cloudflare，
    // 最后 Google（国内默认被墙，仅在有代理环境时作为兜底）。
    vec![
        "stun.miwifi.com:3478".to_string(),
        "stun.chat.bilibili.com:3478".to_string(),
        "stun.qq.com:3478".to_string(),
        "stun.cloudflare.com:3478".to_string(),
        "stun.l.google.com:19302".to_string(),
    ]
}
fn default_relay_bandwidth() -> u32 { 10 }
fn default_relay_max_connections() -> usize { 5 }
fn default_relay_auto_setup_on_connect() -> bool { true }
fn default_gossip_interval() -> u64 { 1000 }
fn default_gossip_fanout() -> usize { 3 }
fn default_gossip_seen_shards() -> usize { 16 }
fn default_sync_node_interval() -> u64 { 300 }
fn default_dht_interval() -> u64 { 300 }
fn default_peer_cache_max() -> usize { 100 }
fn default_transport_write_timeout() -> u64 { 5 }
fn default_gossip_max_consecutive_failures() -> u32 { 3 }
fn default_reconnect_cooldown_secs() -> u64 { 30 }
fn default_heavy_task_max_concurrency() -> usize { 8 }
fn default_gossip_max_bytes_per_second() -> u64 { 5 * 1024 * 1024 }
fn default_gossip_max_messages_per_second() -> u32 { 100 }
fn default_receive_pending_threshold() -> u32 { 1000 }
fn default_initial_sync_batch_size() -> usize { 5000 }
fn default_gossip_flush_interval_ms() -> u64 { 50 }
fn default_gossip_flush_max_batches() -> usize { 10 }
fn default_full_sync_batch_size() -> usize { 5000 }
fn default_full_sync_window_size() -> usize { 3 }
fn default_full_sync_gossip_max_messages_per_second() -> u32 { 5000 }
fn default_full_sync_gossip_max_bytes_per_second() -> u64 { 50 * 1024 * 1024 }
fn default_merkle_async_update_interval_ms() -> u64 { 1000 }
fn default_merkle_async_update_batch_size() -> usize { 10000 }
fn default_gossip_bulk_max_batches() -> usize { 50 }
fn default_gossip_bulk_max_bytes() -> usize { 2 * 1024 * 1024 } // 2MB
fn default_parallel_propagation() -> bool { true }
fn default_full_sync_source_select_settle_ms() -> u64 { 2000 }
fn default_full_sync_receiving_settle_ms() -> u64 { 60_000 }

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
            stun_servers: default_stun_servers(),
            enable_relay: default_true(),
            relay_bandwidth_limit_mbps: default_relay_bandwidth(),
            relay_max_connections: default_relay_max_connections(),
            relay_auto_setup_on_connect: default_relay_auto_setup_on_connect(),
            gossip_interval_ms: default_gossip_interval(),
            gossip_fanout: default_gossip_fanout(),
            gossip_seen_shards: default_gossip_seen_shards(),
            sync_peer_enabled: default_true(),
            sync_node_enabled: default_true(),
            sync_node_interval_secs: default_sync_node_interval(),
            sync_infohash_enabled: default_true(),
            sync_tracker_enabled: default_true(),
            dht_discovery_enabled: default_true(),
            dht_discovery_interval_secs: default_dht_interval(),
            peer_cache_enabled: default_true(),
            peer_cache_max_nodes: default_peer_cache_max(),
            transport_write_timeout_secs: default_transport_write_timeout(),
            gossip_max_consecutive_failures: default_gossip_max_consecutive_failures(),
            reconnect_cooldown_secs: default_reconnect_cooldown_secs(),
            heavy_task_max_concurrency: default_heavy_task_max_concurrency(),
            gossip_max_bytes_per_second: default_gossip_max_bytes_per_second(),
            gossip_max_messages_per_second: default_gossip_max_messages_per_second(),
            receive_pending_threshold: default_receive_pending_threshold(),
            initial_sync_batch_size: default_initial_sync_batch_size(),
            gossip_flush_interval_ms: default_gossip_flush_interval_ms(),
            gossip_flush_max_batches: default_gossip_flush_max_batches(),
            full_sync_batch_size: default_full_sync_batch_size(),
            full_sync_window_size: default_full_sync_window_size(),
            full_sync_gossip_max_messages_per_second: default_full_sync_gossip_max_messages_per_second(),
            full_sync_gossip_max_bytes_per_second: default_full_sync_gossip_max_bytes_per_second(),
            merkle_async_update_interval_ms: default_merkle_async_update_interval_ms(),
            merkle_async_update_batch_size: default_merkle_async_update_batch_size(),
            gossip_bulk_max_batches: default_gossip_bulk_max_batches(),
            gossip_bulk_max_bytes: default_gossip_bulk_max_bytes(),
            parallel_propagation: default_parallel_propagation(),
            full_sync_source_select_settle_ms: default_full_sync_source_select_settle_ms(),
            full_sync_receiving_settle_ms: default_full_sync_receiving_settle_ms(),
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
        assert!(cfg.dht_discovery_enabled);
        assert_eq!(cfg.dht_discovery_interval_secs, 300);
        assert!(cfg.peer_cache_enabled);
        assert_eq!(cfg.peer_cache_max_nodes, 100);
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
