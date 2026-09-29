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
    /// 联邦层 LPD 多播端口（局域网零配置发现；SDK 联邦公告使用，默认 6772）
    ///
    /// ⚠️ 与 `crate::config::DiscoverersConfig::lpd_multicast_port`（业务层 BEP14 LPD，
    /// 默认 **6771**）**同名不同用途、默认值也不同**，改错会导致局域网发现静默失效。
    /// 此处刻意改名为 `federation_lpd_multicast_port` 以消除歧义；
    /// `alias` 保留对旧配置键 `lpd_multicast_port` 的兼容。
    #[serde(
        default = "default_federation_lpd_multicast_port",
        alias = "lpd_multicast_port"
    )]
    pub federation_lpd_multicast_port: u16,
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
    /// v10(F2 配套)：从未成功握手节点占位（temp_id）的保留宽限（秒），超期即清理。
    /// 旧实现硬编码 120s（合规 #9 P0）。
    #[serde(default = "default_never_connected_prune_secs")]
    pub never_connected_prune_secs: u64,
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
    /// P1-2：oplog 保留窗口（秒）。默认 86400（24h）。
    /// **必须 > 预估 bootstrap 时长**（架构文档铁律 4），否则新节点追尾时水位 W0 之后的 op
    /// 已被裁掉，只能重打快照。设为 0 表示永不裁剪（表会无界增长，不建议）。
    #[serde(default = "default_oplog_retention_secs")]
    pub oplog_retention_secs: u64,
    /// P1-2：oplog 裁剪任务间隔（秒）。默认 3600（1h）。
    #[serde(default = "default_oplog_trim_interval_secs")]
    pub oplog_trim_interval_secs: u64,
    /// P1-3：是否启用 delta（oplog 增量）同步通道。默认 true（v8 起与 impl Default 对齐）。
    #[serde(default = "default_true")]
    pub delta_sync_enabled: bool,
    /// P1-3：delta 增量拉取周期（秒）。默认 60（稳态足够，欠账期配合批量 1 千条防慢盘打死）。
    ///
    /// 建连时的首次拉取之外，还需该周期任务持续向每个已连接对端追问「有没有新 op」，
    /// 否则稳态新写入只能靠 gossip 传播，delta 通道会退化成一次性拉取（F1）。
    #[serde(default = "default_delta_sync_interval_secs")]
    pub delta_sync_interval_secs: u64,
    /// v7：是否启用建连协商（SyncNegotiate/Ack）。默认 true。
    /// 协商通过前，v7+ 对端的 delta/bootstrap 大通道不启动（稳定性门控）；对端 < v7 回落旧行为。
    #[serde(default = "default_negotiation_enabled")]
    pub negotiation_enabled: bool,
    /// v7：稳定性门控 —— 连接存活达到该秒数后才允许大通道启动。默认 30。
    #[serde(default = "default_strategy_min_conn_secs")]
    pub strategy_min_conn_secs: u64,
    /// v7：delta 看门狗 —— 连续 N 个拉取周期零进展且 lag>0 时暂停并触发重协商。默认 5。
    #[serde(default = "default_delta_watchdog_stall_ticks")]
    pub delta_watchdog_stall_ticks: u32,
    /// v7：range 叶级差集大差集阈值（行数）。默认 10000。
    #[serde(default = "default_range_bulk_threshold_rows")]
    pub range_bulk_threshold_rows: u64,
    /// P1-4：是否启用 Range-based（有序区间 + 分界点下钻）反熵。默认 true。
    /// v8 起 range 是**唯一**反熵通道（Merkle 反熵已退役），四个 repo 全部走此通道。
    #[serde(default = "default_true")]
    pub range_reconcile_enabled: bool,
    /// P1-4 灰度遗留开关：只读诊断模式。默认 false（v8 起与 impl Default 对齐，直接修复）。
    #[serde(default)]
    pub range_reconcile_diagnostic_only: bool,
    /// B1：range 反熵 per-repo 周期（秒），顺序 NODE/PEER/INFOHASH/TRACKER。
    ///
    /// 四个 repo 走**同一套**反熵逻辑与触发条件，仅周期可按 repo 调；四项**全部来自配置**
    /// （旧实现把 30/120/300/600 写死在 `sync::mod::RANGE_INTERVAL_SECS` 常量里，不可调，
    /// 也不满足「参数同源于配置」）。
    #[serde(default = "default_range_interval_secs")]
    pub range_reconcile_interval_secs: [u64; 4],
    /// P1-4：叶级行数阈值（区间内行数 ≤ 该值即交换行指纹清单求差）。
    #[serde(default = "default_range_leaf_rows")]
    pub range_reconcile_leaf_rows: u32,
    /// B2：per-repo 叶级行数阈值，顺序 NODE/PEER/INFOHASH/TRACKER。
    ///
    /// 与 `range_reconcile_interval_secs` 同理：同一套下钻逻辑，四项阈值全部走配置。
    /// 旧实现里 TRACKER=512、PEER/INFOHASH=2048 是写死常量，NODE 却去读
    /// `range_reconcile_leaf_rows` —— 一个读配置两个写死，属策略来源不一致。
    #[serde(default = "default_range_leaf_rows_per_repo")]
    pub range_reconcile_leaf_rows_per_repo: [u32; 4],
    /// P1-4：单次响应的最大分界点数。
    #[serde(default = "default_range_max_splits")]
    pub range_reconcile_max_splits: u32,
    /// P1-4：最大下钻深度（防御性，避免病态区间无限下钻）。
    #[serde(default = "default_range_max_depth")]
    pub range_reconcile_max_depth: u8,
    /// P1-4：单轮诊断抽样的区间数。
    #[serde(default = "default_range_sample_ranges")]
    pub range_reconcile_sample_ranges: u32,
    /// P2-1：是否启用 bootstrap 专用通道（与在线反熵解耦的全量引导）。默认 true。
    #[serde(default = "default_true")]
    pub bootstrap_enabled: bool,
    /// P2-1：bootstrap 单块行数。
    #[serde(default = "default_bootstrap_chunk_rows")]
    pub bootstrap_chunk_rows: u32,
    /// P2-1：bootstrap 服务端带宽预算（字节/秒）。0 = 不限流。
    #[serde(default = "default_bootstrap_rate_bytes_per_sec")]
    pub bootstrap_rate_bytes_per_sec: u64,
    /// v10(C)：bootstrap 块传输的并发窗口（在途块请求数）。链式传输的吞吐被
    /// 单块「生成+传输+RTT」线性叠加钉死（实测 0.33MB/s）；窗口化预取后吞吐
    /// 随窗口扩大，直至撞上落库/磁盘上限。
    #[serde(default = "default_bootstrap_window_size")]
    pub bootstrap_window_size: usize,
    /// D批(D1)：收到对端 bootstrap 清单后，本地按相同 chunk_rows 重建清单并逐块比对 hash，
    /// hash 一致的块不再请求（只拉差异块）。默认 true；置 false 退回全量盲拉。
    /// 本地清单重建是全表扫描，内部已 spawn_blocking，不阻塞 tokio worker。
    #[serde(default = "default_true")]
    pub bootstrap_skip_identical_chunks: bool,
    /// D批(D3)：请求方在 bootstrap 块响应成功路径，对每个完成块做的节流 sleep（毫秒）。
    /// 工程约束「同步不能影响对端正常运行」——16 路窗口满速回包会把对端正常业务挤爆，
    /// 每收一块退避一小段时间以摊薄压力。0 = 不节流。默认 30ms。
    #[serde(default = "default_bootstrap_peer_protect_delay_ms")]
    pub bootstrap_peer_protect_delay_ms: u64,
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
    // ========================================================================
    // v9 收敛修复：bootstrap 生命周期 / delta 兜底 / range 预算 / gossip 丢批
    // ========================================================================
    /// v9：bootstrap 停滞判定（秒）。该 (peer,repo) 的 `updated_ms` 超过此时长未推进，
    /// 即视为卡死 —— 不再阻塞 delta，并把进度重置为 Idle 以便重打清单。
    ///
    /// 背景：旧实现里 `running = 存在 phase != Done 的行`（无超时、无 peer 维度），
    /// 一行卡在 Transfer 就能让该 repo 的 delta 永久停摆（实测 repo1/2/3 零进展 25 分钟）。
    #[serde(default = "default_bootstrap_stall_secs")]
    pub bootstrap_stall_secs: u64,
    /// v9：同一 index 的分块请求「无响应」达到该次数后重拉清单（自愈）。
    #[serde(default = "default_bootstrap_chunk_max_attempts")]
    pub bootstrap_chunk_max_attempts: u32,
    /// v9：同一 index 的分块请求保持无响应的最长时间（秒），超时同样重拉清单。
    #[serde(default = "default_bootstrap_chunk_timeout_secs")]
    pub bootstrap_chunk_timeout_secs: u64,
    /// v9：bootstrap 是否阻断同 repo 的 delta。默认 **false**（两通道并行）。
    ///
    /// 旧实现无条件 `continue` 跳过 delta，与「bootstrap 卡死」构成互锁闭环；
    /// 置 true 可退回旧行为（仅供对照排查）。
    #[serde(default)]
    pub bootstrap_blocks_delta: bool,
    /// v9：应答方「清单重建」租约（秒）。租约内到达的块请求直接回显式空块（NAK），
    /// 让请求方按失败计数自愈，而不是静默不回帧。
    #[serde(default = "default_bootstrap_rebuild_lease_secs")]
    pub bootstrap_rebuild_lease_secs: u64,
    /// v9：单轮 range 反熵最多处理的 repo 数（1 = 每轮只做一个 repo）。
    /// 旧实现一轮串行处理 4 个 repo（合计约 505 帧、实测单次占槽 166~226s），
    /// 会把同分类的 delta/bootstrap/心跳一起饿死。
    #[serde(default = "default_range_repos_per_tick")]
    pub range_repos_per_tick: u32,
    /// v10(F2)：Range 抽样对账每 tick 发送的区间预算。单轮 161 区间拆为多 tick 发送、
    /// 游标断点续跑 —— 消灭「单轮 >300s 被 TaskScheduler 杀掉、进度作废、从头再来」
    /// 的超时循环（2026-09-27 实测 52/58 双端超时）。
    #[serde(default = "default_range_ranges_per_tick")]
    pub range_ranges_per_tick: u32,
    /// v10(F4)：快照冷却期（秒）。bootstrap 竣工后该 (peer,repo) 在此期间内，
    /// 协商裁定与巡检一律强制 DELTA —— 竣工会清协商强制重裁（B5 修复的副作用），
    /// 而裁定输入（行数）不会立刻变化，无冷却必然重裁 BOOTSTRAP（死循环通道）。
    #[serde(default = "default_bootstrap_cooldown_secs")]
    pub bootstrap_cooldown_secs: u64,
    /// v9：同一 (peer,repo) 叶区间修复的最小间隔（秒），避免同一批差异每轮重复推拉。
    #[serde(default = "default_range_repair_min_interval_secs")]
    pub range_repair_min_interval_secs: u64,
    /// v9：Gossip 批次重试时间预算（秒）。0 = 不按时间丢弃（仅靠 outbox 上限兜底）。
    ///
    /// 旧实现硬编码 30s：对端慢或被分类槽饿死时，批次会在 30s 内被判「重试超预算」永久丢弃。
    #[serde(default = "default_gossip_retry_budget_secs")]
    pub gossip_retry_budget_secs: u64,
    /// v9：本地写入的 Gossip 攒批阈值（条）。达到即提交一个 GossipBatch。
    ///
    /// 旧实现每条 entry 一个独立 batch（被动收集路径），出口限流按「帧数」计 ⇒
    /// 出口被钉死在 `gossip_max_messages_per_second` 条 entry/s。
    #[serde(default = "default_gossip_coalesce_batch_size")]
    pub gossip_coalesce_batch_size: usize,
    /// v9：delta 续拉（has_more）发送失败后是否立即重试（不等待周期间隔）。
    #[serde(default = "default_true")]
    pub delta_retry_immediately: bool,
    /// v9：oplog 裁剪是否尊重「最小对端已同步水位」，避免裁出对端永远拉不到的空洞。
    ///
    /// 背景（假收敛根因）：`trim_oplog` 只看时间不看对端进度，`min_seq` 生产出来后无人消费，
    /// 请求方游标会一步跨过被裁掉的段并把 lag 归零 —— 实测 repo1 有 442,290 个 op 结构性不可达。
    #[serde(default = "default_true")]
    pub oplog_trim_respect_peer_floor: bool,
    /// v9：上述 floor 的硬上限倍数 —— 超过 `retention × 该倍数` 仍强制裁剪，防 oplog 无界增长。
    #[serde(default = "default_oplog_hard_retention_multiplier")]
    pub oplog_hard_retention_multiplier: u64,
    // ========================================================================
    // E1/E2：bootstrap 运行时差异复检 + 块窗口空闲看门狗（修复 bootstrap 中途
    // OOM/超时断裂后永久停在 DELTA、在途块丢失后窗口卡死两个缺陷）
    // ========================================================================
    /// E1：bootstrap 中途断裂后，巡检路径用 D2 双向阈值（ratio>1.3 且 diff>50000）
    /// 重新比对「本地实时行数 vs 对端最近自报行数」，超阈值即重触发 bootstrap。
    /// 同一 (peer, repo) 在此时长内只重触发一次，防抖动（默认 300s = 5 分钟）。
    /// 与 v10(F4) 的 `bootstrap_cooldown_secs`（竣工后防死循环）是两条独立冷却：
    /// F4 管「刚竣工别立刻重打」，本项管「中途断掉后多久允许重打一次」。
    #[serde(default = "default_bootstrap_rediff_cooldown_secs")]
    pub bootstrap_rediff_cooldown_secs: u64,
    /// E2：块传输窗口级空闲看门狗 —— 距最后一块落地超过此时长仍有在途块时，
    /// 把全部在途块回收进重试队列并重发（默认 60s）。覆盖 OOM 驱逐阻塞 actor 后
    /// 在途块消息超时丢失、窗口永久卡住的场景；与逐块 `bootstrap_chunk_timeout_secs`
    /// 互补（后者管「单发块无人应」，本项管「曾有进展后整体停摆」）。
    #[serde(default = "default_bootstrap_window_idle_timeout_secs")]
    pub bootstrap_window_idle_timeout_secs: u64,
    /// E2：窗口空闲恢复的最大次数。连续这么多轮空闲看门狗都没等到新块落地，
    /// 判定该窗口不可恢复，放弃窗口并重拉清单（默认 3，约 3 个 resume tick）。
    #[serde(default = "default_bootstrap_window_idle_max_retries")]
    pub bootstrap_window_idle_max_retries: u32,
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
fn default_federation_lpd_multicast_port() -> u16 {
    6772
}
fn default_max_connections() -> usize {
    32
}
fn default_target_neighbors() -> usize {
    8
}
fn default_heartbeat_timeout() -> u64 {
    300
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
    32
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
fn default_oplog_retention_secs() -> u64 {
    86_400
}
fn default_oplog_trim_interval_secs() -> u64 {
    3_600
}
fn default_delta_sync_interval_secs() -> u64 {
    // 60s：稳态足够；欠账期配合批量 1 千条，避免慢盘节点被高频大拉取打死（51 事故）。
    60
}
/// v7：建连协商默认开启（协议版本门控保护旧对端；两端同版本时协商生效）。
fn default_negotiation_enabled() -> bool {
    true
}
/// v7：稳定性门控 —— 连接存活达到该秒数后才允许启动 delta/bootstrap 大通道。
fn default_strategy_min_conn_secs() -> u64 {
    30
}
/// v7：delta 看门狗 —— 连续 N 个 interval 零进展且 lag>0 时暂停拉取并触发重协商。
fn default_delta_watchdog_stall_ticks() -> u32 {
    5
}
/// v7：range 叶级差集超过该行数即视为「大差集」，按协商策略走 bootstrap 快照通道。
fn default_range_bulk_threshold_rows() -> u64 {
    10_000
}
fn default_range_leaf_rows() -> u32 {
    crate::federation::sync::range_reconcile::DEFAULT_LEAF_ROWS
}
/// B1：range 反熵 per-repo 周期默认值（秒），顺序 NODE/PEER/INFOHASH/TRACKER。
fn default_range_interval_secs() -> [u64; 4] {
    [30, 120, 300, 600]
}
/// B2：per-repo 叶级行数阈值默认值，顺序 NODE/PEER/INFOHASH/TRACKER。
fn default_range_leaf_rows_per_repo() -> [u32; 4] {
    [
        default_range_leaf_rows(), // NODE：沿用既有配置默认值
        2048,                      // PEER
        2048,                      // INFOHASH
        512,                       // TRACKER（小库）
    ]
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
/// v10(C)：bootstrap 块传输并发窗口（见 `bootstrap_window_size`）。
fn default_bootstrap_window_size() -> usize {
    16
}
/// D批(D3)：对端负载保护节流（见 `bootstrap_peer_protect_delay_ms`）。默认 30ms。
fn default_bootstrap_peer_protect_delay_ms() -> u64 {
    30
}
fn default_gossip_bulk_max_batches() -> usize {
    50
}
fn default_gossip_bulk_max_bytes() -> usize {
    1536 * 1024
} // 1.5MB（与 DELTA_BATCH_MAX_BYTES 对齐，留帧头/封装余量）
fn default_parallel_propagation() -> bool {
    true
}
fn default_full_sync_receiving_settle_ms() -> u64 {
    60_000
}
/// v9：bootstrap 停滞判定（秒）。3 分钟足够覆盖百万行库的一次分块往返（实测单块 2 万行）。
fn default_bootstrap_stall_secs() -> u64 {
    180
}
fn default_bootstrap_chunk_max_attempts() -> u32 {
    3
}
fn default_bootstrap_chunk_timeout_secs() -> u64 {
    90
}
/// v9：清单重建租约（秒）。旧值 900 过长：租约内所有块请求被静默丢弃（实测卡死主因之一）。
fn default_bootstrap_rebuild_lease_secs() -> u64 {
    60
}
fn default_range_repos_per_tick() -> u32 {
    1
}
/// v10(F2)：从未握手成功占位的清理宽限（秒）（见 `never_connected_prune_secs`）。
fn default_never_connected_prune_secs() -> u64 {
    120
}
/// v10(F2)：Range 每 tick 区间预算（见 `range_ranges_per_tick`）。
fn default_range_ranges_per_tick() -> u32 {
    32
}
/// v10(F4)：快照冷却期（秒），覆盖至少 2 个巡检周期（见 `bootstrap_cooldown_secs`）。
fn default_bootstrap_cooldown_secs() -> u64 {
    600
}
fn default_range_repair_min_interval_secs() -> u64 {
    60
}
fn default_gossip_retry_budget_secs() -> u64 {
    300
}
fn default_gossip_coalesce_batch_size() -> usize {
    256
}
fn default_oplog_hard_retention_multiplier() -> u64 {
    4
}
/// E1：运行时差异复检的 per-(peer,repo) 重触发冷却（秒）。
/// 5 分钟足够覆盖一次 bootstrap 断裂后的恢复与下一轮巡检（300s），又不会让差异长期无人补。
fn default_bootstrap_rediff_cooldown_secs() -> u64 {
    300
}
/// E2：块窗口空闲看门狗阈值（秒）。60s 无新块落地即判定在途块丢失，回收重发。
fn default_bootstrap_window_idle_timeout_secs() -> u64 {
    60
}
/// E2：窗口空闲恢复最大次数，超过则放弃窗口重拉清单。
fn default_bootstrap_window_idle_max_retries() -> u32 {
    3
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
            federation_lpd_multicast_port: default_federation_lpd_multicast_port(),
            max_connections: default_max_connections(),
            target_neighbors: default_target_neighbors(),
            heartbeat_timeout_secs: default_heartbeat_timeout(),
            nat_mapping_enabled: default_true(),
            stun_servers: default_stun_servers(),
            enable_relay: default_true(),
            relay_bandwidth_limit_mbps: default_relay_bandwidth(),
            relay_max_connections: default_relay_max_connections(),
            relay_auto_setup_on_connect: default_relay_auto_setup_on_connect(),
            range_ranges_per_tick: default_range_ranges_per_tick(),
            gossip_interval_ms: default_gossip_interval(),
            gossip_fanout: default_gossip_fanout(),
            gossip_seen_shards: default_gossip_seen_shards(),
            sync_peer_enabled: default_true(),
            sync_node_enabled: default_true(),
            sync_infohash_enabled: default_true(),
            sync_tracker_enabled: default_true(),
            dht_discovery_enabled: default_true(),
            bootstrap_window_size: default_bootstrap_window_size(),
            bootstrap_skip_identical_chunks: default_true(),
            bootstrap_peer_protect_delay_ms: default_bootstrap_peer_protect_delay_ms(),
            dht_discovery_interval_secs: default_dht_interval(),
            peer_cache_enabled: default_true(),
            peer_cache_max_nodes: default_peer_cache_max(),
            transport_write_timeout_secs: default_transport_write_timeout(),
            transport_write_max_retries: default_transport_write_max_retries(),
            transport_write_retry_base_ms: default_transport_write_retry_base_ms(),
            gossip_max_consecutive_failures: default_gossip_max_consecutive_failures(),
            reconnect_cooldown_secs: default_reconnect_cooldown_secs(),
            never_connected_prune_secs: default_never_connected_prune_secs(),
            bootstrap_cooldown_secs: default_bootstrap_cooldown_secs(),
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
            oplog_retention_secs: default_oplog_retention_secs(),
            oplog_trim_interval_secs: default_oplog_trim_interval_secs(),
            delta_sync_enabled: true,
            delta_sync_interval_secs: default_delta_sync_interval_secs(),
            range_reconcile_enabled: true,
            range_reconcile_diagnostic_only: false,
            range_reconcile_interval_secs: default_range_interval_secs(),
            range_reconcile_leaf_rows: default_range_leaf_rows(),
            range_reconcile_leaf_rows_per_repo: default_range_leaf_rows_per_repo(),
            range_reconcile_max_splits: default_range_max_splits(),
            range_reconcile_max_depth: default_range_max_depth(),
            range_reconcile_sample_ranges: default_range_sample_ranges(),
            bootstrap_enabled: true,
            bootstrap_chunk_rows: default_bootstrap_chunk_rows(),
            bootstrap_rate_bytes_per_sec: default_bootstrap_rate_bytes_per_sec(),
            gossip_bulk_max_batches: default_gossip_bulk_max_batches(),
            gossip_bulk_max_bytes: default_gossip_bulk_max_bytes(),
            parallel_propagation: default_parallel_propagation(),
            full_sync_receiving_settle_ms: default_full_sync_receiving_settle_ms(),
            negotiation_enabled: default_negotiation_enabled(),
            strategy_min_conn_secs: default_strategy_min_conn_secs(),
            delta_watchdog_stall_ticks: default_delta_watchdog_stall_ticks(),
            range_bulk_threshold_rows: default_range_bulk_threshold_rows(),
            bootstrap_stall_secs: default_bootstrap_stall_secs(),
            bootstrap_chunk_max_attempts: default_bootstrap_chunk_max_attempts(),
            bootstrap_chunk_timeout_secs: default_bootstrap_chunk_timeout_secs(),
            bootstrap_blocks_delta: false,
            bootstrap_rebuild_lease_secs: default_bootstrap_rebuild_lease_secs(),
            range_repos_per_tick: default_range_repos_per_tick(),
            range_repair_min_interval_secs: default_range_repair_min_interval_secs(),
            gossip_retry_budget_secs: default_gossip_retry_budget_secs(),
            gossip_coalesce_batch_size: default_gossip_coalesce_batch_size(),
            delta_retry_immediately: true,
            oplog_trim_respect_peer_floor: true,
            oplog_hard_retention_multiplier: default_oplog_hard_retention_multiplier(),
            bootstrap_rediff_cooldown_secs: default_bootstrap_rediff_cooldown_secs(),
            bootstrap_window_idle_timeout_secs: default_bootstrap_window_idle_timeout_secs(),
            bootstrap_window_idle_max_retries: default_bootstrap_window_idle_max_retries(),
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
        // 300 是 2026-09-20「连接稳定性修复」的有意调参（90→300）：放宽空闲断连窗口
        // 以降低慢链路 / NAT 抖动下的误杀。该断言当时漏同步，长期 FAIL，此处对齐实现。
        assert_eq!(cfg.heartbeat_timeout_secs, 300);
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
        // D批(D3)：对端负载保护节流默认 30ms（必须走 default fn，不能是裸 serde(default)=0）
        assert_eq!(cfg.bootstrap_peer_protect_delay_ms, 30);
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

    /// 联邦层 LPD 多播端口：默认值应为 6772（与业务层 BEP14 LPD 的 6771 区分），
    /// 且必须同时接受新键 `federation_lpd_multicast_port` 与遗留键 `lpd_multicast_port`。
    #[test]
    fn test_federation_lpd_multicast_port_default_and_alias() {
        // 1) 默认值
        let cfg = FederationConfig::default();
        assert_eq!(cfg.federation_lpd_multicast_port, 6772);

        // 2) 省略该键 → 回落到默认值（而非 0）
        let cfg_omitted: FederationConfig = serde_yaml::from_str("listen_port: 7000").unwrap();
        assert_eq!(cfg_omitted.federation_lpd_multicast_port, 6772);

        // 3) 新键
        let cfg_new: FederationConfig =
            serde_yaml::from_str("federation_lpd_multicast_port: 7001").unwrap();
        assert_eq!(cfg_new.federation_lpd_multicast_port, 7001);

        // 4) 遗留键（旧部署的配置文件）仍必须生效
        let cfg_legacy: FederationConfig =
            serde_yaml::from_str("lpd_multicast_port: 7002").unwrap();
        assert_eq!(
            cfg_legacy.federation_lpd_multicast_port, 7002,
            "旧配置键 lpd_multicast_port 应通过 serde alias 继续生效"
        );

        // 5) 两个键不同时以最长前缀匹配？serde alias 下新键优先；
        //    这里只确保互不干扰：只用旧键时不报错、值正确。
        assert_ne!(
            cfg_legacy.federation_lpd_multicast_port, 6771,
            "不应误取业务层 LPD 的 6771"
        );
    }
}
