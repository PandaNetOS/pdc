//! 联邦网络配置
//!
//! 控制 PDC 联邦网络的所有行为参数，包括监听端口、连接数、心跳、NAT 映射、同步等。

use serde::{Deserialize, Serialize};

/// 联邦网络配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationConfig {
    /// 是否启用联邦网络
    #[serde(default = "default_true")]
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
    /// 传输模式：tcp_only / iroh_only / auto（默认 auto，Iroh+TCP 并行 race）
    #[serde(default = "default_transport_mode")]
    pub transport_mode: String,
    /// API/HTTP 监控端口（LPD 广播时告知对端，端口自动探测后为实际值）
    #[serde(default = "default_api_port")]
    pub api_port: u16,
    /// LPD 多播端口（局域网零配置发现，默认 6771，与 BitTorrent LPD 对齐）
    #[serde(default = "default_lpd_multicast_port")]
    pub lpd_multicast_port: u16,
    /// 最大连接数
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// 目标邻居数
    #[serde(default = "default_target_neighbors")]
    pub target_neighbors: usize,
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
    /// TCP 传输写入超时（秒），超过此时间 write_all 未完成则报错（随后触发重试）
    #[serde(default = "default_transport_write_timeout")]
    pub transport_write_timeout_secs: u64,
    /// TCP 写入超时后的最大重试次数（不含首次）。
    /// 超时后按退避基数指数退避重试，超过此次数才判定写入失败并断开。
    #[serde(default = "default_transport_write_max_retries")]
    pub transport_write_max_retries: u32,
    /// TCP 写入重试退避基数（毫秒）。
    /// 第 n 次重试前等待 retry_base_ms * 2^(n-1)。
    #[serde(default = "default_transport_write_retry_base_ms")]
    pub transport_write_retry_base_ms: u64,
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
    /// Merkle 树异步批量更新触发阈值（条数）。
    /// 队列累计达到此条数时通过 Notify 立即唤醒后台 flush，无需等待间隔。
    #[serde(default = "default_merkle_async_update_batch_size")]
    pub merkle_async_update_batch_size: usize,
    /// Merkle 冷数据重算间隔（秒）。
    /// 每小时从 DB 全量加载 key+hash 重算冷分片根，默认 3600 秒。
    #[serde(default = "default_merkle_cold_rebuild_interval_secs")]
    pub merkle_cold_rebuild_interval_secs: u64,
    /// Merkle 增量更新间隔（秒）。
    /// 后台任务每此间隔取出 dirty 分片，从 DB 加载对应分片数据重算分片根和全量根。
    /// 默认 10 秒。
    #[serde(default = "default_merkle_incremental_update_interval_secs")]
    pub merkle_incremental_update_interval_secs: u64,
    /// P1-2：oplog 保留窗口（秒）。默认 86400（24h）。
    /// **必须 > 预估 bootstrap 时长**（架构文档铁律 4），否则新节点追尾时水位 W0 之后的 op
    /// 已被裁掉，只能重打快照。设为 0 表示永不裁剪（表会无界增长，不建议）。
    #[serde(default = "default_oplog_retention_secs")]
    pub oplog_retention_secs: u64,
    /// P1-2：oplog 裁剪任务间隔（秒）。默认 3600（1h）。
    #[serde(default = "default_oplog_trim_interval_secs")]
    pub oplog_trim_interval_secs: u64,
    /// P1-3：是否启用 delta（oplog 增量）同步通道。默认 false。
    /// 该通道改变线上协议（新增 OpsRequest/OpsBatch 消息），必须两端同版本后开启；
    /// 关闭时完全不发这些消息，行为与改造前一致。
    #[serde(default)]
    pub delta_sync_enabled: bool,
    /// P1-3：delta 增量拉取周期（秒）。默认 15。
    ///
    /// 建连时的首次拉取之外，还需该周期任务持续向每个已连接对端追问「有没有新 op」，
    /// 否则稳态新写入只能靠 gossip 传播，delta 通道会退化成一次性拉取（F1）。
    #[serde(default = "default_delta_sync_interval_secs")]
    pub delta_sync_interval_secs: u64,
    /// P1-4：是否启用 Range-based（有序区间 + 分界点下钻）反熵作为 NODE repo 的反熵主链。
    /// 默认 false：行为与改造前一致（既有分层 Merkle 反熵），必须两端同版本（>= v5）后开启。
    #[serde(default)]
    pub range_reconcile_enabled: bool,
    /// P1-4 灰度：只读诊断模式。true（默认）时只求差集并打印统计、**不修改任何数据**。
    #[serde(default = "default_true")]
    pub range_reconcile_diagnostic_only: bool,
    /// P1-4：叶级行数阈值（区间内行数 ≤ 该值即交换行指纹清单求差）。
    #[serde(default = "default_range_leaf_rows")]
    pub range_reconcile_leaf_rows: u32,
    /// P1-4：单次响应的最大分界点数。
    #[serde(default = "default_range_max_splits")]
    pub range_reconcile_max_splits: u32,
    /// P1-4：最大下钻深度（防御性，避免病态区间无限下钻）。
    #[serde(default = "default_range_max_depth")]
    pub range_reconcile_max_depth: u8,
    /// P1-4：单轮诊断抽样的区间数。
    #[serde(default = "default_range_sample_ranges")]
    pub range_reconcile_sample_ranges: u32,
    /// P2-1：是否启用 bootstrap 专用通道（与在线反熵解耦的全量引导）。
    /// 默认 false：不注册、不发起、不响应任何 bootstrap 消息。
    #[serde(default)]
    pub bootstrap_enabled: bool,
    /// P2-1：bootstrap 单块行数。
    #[serde(default = "default_bootstrap_chunk_rows")]
    pub bootstrap_chunk_rows: u32,
    /// P2-1：bootstrap 服务端带宽预算（字节/秒）。0 = 不限流。
    #[serde(default = "default_bootstrap_rate_bytes_per_sec")]
    pub bootstrap_rate_bytes_per_sec: u64,
    /// P2-5：NODE repo 的反熵周期（秒）。NODE churn 最高，用更短周期。
    #[serde(default = "default_anti_entropy_node_interval_secs")]
    pub anti_entropy_node_interval_secs: u64,
    /// P2-5：其余 repo 的反熵周期（秒）。
    #[serde(default = "default_anti_entropy_other_interval_secs")]
    pub anti_entropy_other_interval_secs: u64,
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
    /// 发起拉取后，接收全量期间保持「收到的 Gossip 只本地写入不转发」的稳态时长（毫秒）。
    /// 超过此时长自动恢复正常转发（Gossip 拉取无显式完成信号，用可配置时长兜底）。
    #[serde(default = "default_full_sync_receiving_settle_ms")]
    pub full_sync_receiving_settle_ms: u64,
    /// DiffSync key 列表交换超时（秒）。
    /// 数据服务器发出差异分片 key 列表后，等待对端回传缺失 key 列表的最长时间。
    /// 超时则回退到原始全量推送逻辑（对端不支持新协议或网络异常时的兜底）。
    #[serde(default = "default_diff_key_exchange_timeout_secs")]
    pub diff_key_exchange_timeout_secs: u64,
    /// 是否启用分层 Merkle 对比协议（协议版本 >=3）。
    /// 启用后使用 L0→L1→L2 三层对比定位差异，替代旧版全量 key 交换。
    /// 对端不支持时自动回退到旧协议。
    #[serde(default = "default_true")]
    pub layered_merkle_enabled: bool,
    /// 分片并行同步最大并发数（同时同步的 L2 分片数）。
    /// 默认 4，每个分片独立超时、独立重试，单分片失败不影响其他分片。
    #[serde(default = "default_shard_sync_max_concurrency")]
    pub shard_sync_max_concurrency: usize,
    /// 单个 L2 分片同步超时（秒）。
    /// 超过此时间未收到确认则重试该分片，超过重试次数后标记失败。
    #[serde(default = "default_shard_sync_timeout_secs")]
    pub shard_sync_timeout_secs: u64,
    /// 单个 L2 分片同步最大重试次数（不含首次）。
    #[serde(default = "default_shard_sync_retry_count")]
    pub shard_sync_retry_count: u32,
    /// 分片同步每批条目数（ShardSyncBatch 的 entries 数量）。
    /// 默认 5000，平衡消息大小和网络往返次数。
    #[serde(default = "default_shard_sync_batch_size")]
    pub shard_sync_batch_size: usize,
    /// 分片同步速率限制（条目/秒），0 表示不限制。
    /// 用于避免同步期间占用过多磁盘 IO 和网络带宽，影响爬虫和 Tracker 正常运行。
    #[serde(default = "default_shard_sync_rate_limit_per_sec")]
    pub shard_sync_rate_limit_per_sec: u64,
    /// 分片同步窗口大小（发送方未确认的在途批次数）。
    #[serde(default = "default_shard_sync_window_size")]
    pub shard_sync_window_size: usize,
    /// 单批次最大发送重试次数（不含首次）。
    /// 发送失败后按指数退避重试，超过此次数则丢弃该批次并记录失败统计。
    #[serde(default = "default_shard_sync_max_retries")]
    pub shard_sync_max_retries: u32,
    /// 分片同步发送失败指数退避基数（毫秒）。
    /// 第 n 次重试前等待 base_backoff_ms * 2^(send_retry_count)。
    #[serde(default = "default_shard_sync_base_backoff_ms")]
    pub shard_sync_base_backoff_ms: u64,
    /// 分片同步发送失败最大退避上限（毫秒）。
    /// 指数退避超过此值后不再增长，封顶等待。
    #[serde(default = "default_shard_sync_max_backoff_ms")]
    pub shard_sync_max_backoff_ms: u64,
    /// 连续发送失败断开阈值。
    /// 连续达到此次数发送失败则标记连接断开，停止当前同步并触发上层重连。
    #[serde(default = "default_shard_sync_consecutive_fail_threshold")]
    pub shard_sync_consecutive_fail_threshold: u32,
    /// 分片同步引擎完成后轮询清理间隔（秒）。
    /// 后台任务轮询检查引擎是否已结束，结束后从活跃列表移除。
    #[serde(default = "default_shard_sync_engine_poll_interval_secs")]
    pub shard_sync_engine_poll_interval_secs: u64,
    /// 分片同步窗口流控等待时间（毫秒）。
    /// 窗口满时等待此时间后重新检查在途批次数。
    #[serde(default = "default_shard_sync_window_flow_sleep_ms")]
    pub shard_sync_window_flow_sleep_ms: u64,
}

fn default_listen_port() -> u16 {
    6885
}
fn default_api_port() -> u16 {
    6880
}
fn default_transport_mode() -> String {
    "auto".to_string()
}
fn default_lpd_multicast_port() -> u16 {
    6772
}
fn default_max_connections() -> usize {
    32
}
fn default_target_neighbors() -> usize {
    8
}
fn default_heartbeat_timeout() -> u64 {
    90
}
fn default_true() -> bool {
    true
}
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
fn default_relay_bandwidth() -> u32 {
    10
}
fn default_relay_max_connections() -> usize {
    5
}
fn default_relay_auto_setup_on_connect() -> bool {
    true
}
fn default_gossip_interval() -> u64 {
    1000
}
fn default_gossip_fanout() -> usize {
    8
}
fn default_gossip_seen_shards() -> usize {
    16
}
fn default_dht_interval() -> u64 {
    300
}
fn default_peer_cache_max() -> usize {
    100
}
fn default_transport_write_timeout() -> u64 {
    30
}
/// TCP 写入超时后的最大重试次数（不含首次）。
/// 超时后按退避基数指数退避重试，超过此次数才判定写入失败并断开。
fn default_transport_write_max_retries() -> u32 {
    3
}
/// TCP 写入重试退避基数（毫秒）。
/// 第 n 次重试前等待 retry_base_ms * 2^(n-1)（100/200/400...）。
fn default_transport_write_retry_base_ms() -> u64 {
    100
}
fn default_gossip_max_consecutive_failures() -> u32 {
    3
}
fn default_reconnect_cooldown_secs() -> u64 {
    30
}
fn default_heavy_task_max_concurrency() -> usize {
    8
}
fn default_gossip_max_bytes_per_second() -> u64 {
    5 * 1024 * 1024
}
fn default_gossip_max_messages_per_second() -> u32 {
    100
}
fn default_receive_pending_threshold() -> u32 {
    1000
}
fn default_initial_sync_batch_size() -> usize {
    5000
}
fn default_gossip_flush_max_batches() -> usize {
    10
}
fn default_full_sync_batch_size() -> usize {
    2000
}
fn default_full_sync_window_size() -> usize {
    3
}
fn default_full_sync_gossip_max_messages_per_second() -> u32 {
    5000
}
fn default_full_sync_gossip_max_bytes_per_second() -> u64 {
    50 * 1024 * 1024
}
fn default_merkle_async_update_batch_size() -> usize {
    10000
}
fn default_merkle_cold_rebuild_interval_secs() -> u64 {
    3600
}
fn default_merkle_incremental_update_interval_secs() -> u64 {
    10
}
fn default_oplog_retention_secs() -> u64 {
    86_400
}
fn default_oplog_trim_interval_secs() -> u64 {
    3_600
}
fn default_delta_sync_interval_secs() -> u64 {
    15
}
fn default_range_leaf_rows() -> u32 {
    crate::federation::sync::range_reconcile::DEFAULT_LEAF_ROWS
}
fn default_range_max_splits() -> u32 {
    crate::federation::sync::range_reconcile::DEFAULT_MAX_SPLITS
}
fn default_range_max_depth() -> u8 {
    crate::federation::sync::range_reconcile::DEFAULT_MAX_DEPTH
}
fn default_range_sample_ranges() -> u32 {
    crate::federation::sync::range_reconcile::DEFAULT_SAMPLE_RANGES
}
fn default_bootstrap_chunk_rows() -> u32 {
    crate::federation::sync::bootstrap::DEFAULT_CHUNK_ROWS
}
fn default_bootstrap_rate_bytes_per_sec() -> u64 {
    crate::federation::sync::bootstrap::DEFAULT_RATE_BYTES_PER_SEC
}
fn default_anti_entropy_node_interval_secs() -> u64 {
    30
}
fn default_anti_entropy_other_interval_secs() -> u64 {
    300
}
fn default_gossip_bulk_max_batches() -> usize {
    50
}
fn default_gossip_bulk_max_bytes() -> usize {
    2 * 1024 * 1024
} // 2MB
fn default_parallel_propagation() -> bool {
    true
}
fn default_full_sync_receiving_settle_ms() -> u64 {
    60_000
}
fn default_diff_key_exchange_timeout_secs() -> u64 {
    30
}
fn default_shard_sync_max_concurrency() -> usize {
    4
}
fn default_shard_sync_timeout_secs() -> u64 {
    60
}
fn default_shard_sync_retry_count() -> u32 {
    3
}
fn default_shard_sync_batch_size() -> usize {
    5000
}
fn default_shard_sync_rate_limit_per_sec() -> u64 {
    0
}
fn default_shard_sync_window_size() -> usize {
    3
}
fn default_shard_sync_max_retries() -> u32 {
    3
}
fn default_shard_sync_base_backoff_ms() -> u64 {
    200
}
fn default_shard_sync_max_backoff_ms() -> u64 {
    5000
}
fn default_shard_sync_consecutive_fail_threshold() -> u32 {
    5
}
fn default_shard_sync_engine_poll_interval_secs() -> u64 {
    5
}
fn default_shard_sync_window_flow_sleep_ms() -> u64 {
    50
}

impl Default for FederationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            node_id: None,
            seed_nodes: Vec::new(),
            listen_port: default_listen_port(),
            transport_mode: default_transport_mode(),
            api_port: default_api_port(),
            lpd_multicast_port: default_lpd_multicast_port(),
            max_connections: default_max_connections(),
            target_neighbors: default_target_neighbors(),
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
            sync_infohash_enabled: default_true(),
            sync_tracker_enabled: default_true(),
            dht_discovery_enabled: default_true(),
            dht_discovery_interval_secs: default_dht_interval(),
            peer_cache_enabled: default_true(),
            peer_cache_max_nodes: default_peer_cache_max(),
            transport_write_timeout_secs: default_transport_write_timeout(),
            transport_write_max_retries: default_transport_write_max_retries(),
            transport_write_retry_base_ms: default_transport_write_retry_base_ms(),
            gossip_max_consecutive_failures: default_gossip_max_consecutive_failures(),
            reconnect_cooldown_secs: default_reconnect_cooldown_secs(),
            heavy_task_max_concurrency: default_heavy_task_max_concurrency(),
            gossip_max_bytes_per_second: default_gossip_max_bytes_per_second(),
            gossip_max_messages_per_second: default_gossip_max_messages_per_second(),
            receive_pending_threshold: default_receive_pending_threshold(),
            initial_sync_batch_size: default_initial_sync_batch_size(),
            gossip_flush_max_batches: default_gossip_flush_max_batches(),
            full_sync_batch_size: default_full_sync_batch_size(),
            full_sync_window_size: default_full_sync_window_size(),
            full_sync_gossip_max_messages_per_second:
                default_full_sync_gossip_max_messages_per_second(),
            full_sync_gossip_max_bytes_per_second: default_full_sync_gossip_max_bytes_per_second(),
            merkle_async_update_batch_size: default_merkle_async_update_batch_size(),
            merkle_cold_rebuild_interval_secs: default_merkle_cold_rebuild_interval_secs(),
            merkle_incremental_update_interval_secs:
                default_merkle_incremental_update_interval_secs(),
            oplog_retention_secs: default_oplog_retention_secs(),
            oplog_trim_interval_secs: default_oplog_trim_interval_secs(),
            delta_sync_enabled: true,
            delta_sync_interval_secs: default_delta_sync_interval_secs(),
            range_reconcile_enabled: true,
            range_reconcile_diagnostic_only: false,
            range_reconcile_leaf_rows: default_range_leaf_rows(),
            range_reconcile_max_splits: default_range_max_splits(),
            range_reconcile_max_depth: default_range_max_depth(),
            range_reconcile_sample_ranges: default_range_sample_ranges(),
            bootstrap_enabled: true,
            bootstrap_chunk_rows: default_bootstrap_chunk_rows(),
            bootstrap_rate_bytes_per_sec: default_bootstrap_rate_bytes_per_sec(),
            anti_entropy_node_interval_secs: default_anti_entropy_node_interval_secs(),
            anti_entropy_other_interval_secs: default_anti_entropy_other_interval_secs(),
            gossip_bulk_max_batches: default_gossip_bulk_max_batches(),
            gossip_bulk_max_bytes: default_gossip_bulk_max_bytes(),
            parallel_propagation: default_parallel_propagation(),
            full_sync_receiving_settle_ms: default_full_sync_receiving_settle_ms(),
            diff_key_exchange_timeout_secs: default_diff_key_exchange_timeout_secs(),
            layered_merkle_enabled: default_true(),
            shard_sync_max_concurrency: default_shard_sync_max_concurrency(),
            shard_sync_timeout_secs: default_shard_sync_timeout_secs(),
            shard_sync_retry_count: default_shard_sync_retry_count(),
            shard_sync_batch_size: default_shard_sync_batch_size(),
            shard_sync_rate_limit_per_sec: default_shard_sync_rate_limit_per_sec(),
            shard_sync_window_size: default_shard_sync_window_size(),
            shard_sync_max_retries: default_shard_sync_max_retries(),
            shard_sync_base_backoff_ms: default_shard_sync_base_backoff_ms(),
            shard_sync_max_backoff_ms: default_shard_sync_max_backoff_ms(),
            shard_sync_consecutive_fail_threshold: default_shard_sync_consecutive_fail_threshold(),
            shard_sync_engine_poll_interval_secs: default_shard_sync_engine_poll_interval_secs(),
            shard_sync_window_flow_sleep_ms: default_shard_sync_window_flow_sleep_ms(),
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
        assert_eq!(cfg.heartbeat_timeout_secs, 90);
        assert!(cfg.nat_mapping_enabled);
        assert!(cfg.enable_relay);
        assert_eq!(cfg.relay_bandwidth_limit_mbps, 10);
        assert_eq!(cfg.relay_max_connections, 5);
        assert_eq!(cfg.gossip_interval_ms, 1000);
        assert_eq!(cfg.gossip_fanout, 8);
        assert!(cfg.sync_node_enabled);
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

    /// 回归：`federation:` 节存在但省略 `enabled` 时必须回退为 `true`，
    /// 与 `FederationConfig::default()` 保持一致。此前该字段是裸 `#[serde(default)]`，
    /// serde 字段级回退为 `bool::default()` = false，导致"写了 federation 节却漏掉
    /// enabled"时联邦被静默关闭（`main.rs` 的 else 分支不打任何日志）。
    #[test]
    fn test_enabled_defaults_true_when_key_omitted() {
        // 1) 有 federation 节但省略 enabled → true
        let yaml = r#"
listen_port: 7000
max_connections: 64
"#;
        let cfg: FederationConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(
            cfg.enabled,
            "省略 enabled 时应回退为 true，而非 bool::default()=false"
        );

        // 2) 显式 enabled: false 仍应被尊重
        let yaml_off = "enabled: false";
        let cfg_off: FederationConfig = serde_yaml::from_str(yaml_off).unwrap();
        assert!(!cfg_off.enabled, "显式 enabled: false 应保持 false");

        // 3) 与 impl Default 保持一致
        assert_eq!(cfg.enabled, FederationConfig::default().enabled);
    }

    #[test]
    fn test_config_serialize() {
        let cfg = FederationConfig::default();
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        assert!(yaml.contains("listen_port: 6885"));
        assert!(yaml.contains("enabled: true"));
    }
}
