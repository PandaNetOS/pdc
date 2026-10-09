//! 爬虫引擎实现
//!
//! 主动 + 被动混合 DHT 爬虫：
//! 1. 加入 DHT 网络（通过 bootstrap 节点发 find_node 获取初始路由）
//! 2. 主动爬行：定期向已知节点发 find_node，探索 DHT 网络，收集节点
//! 3. 被动监听：其他节点发来的 get_peers / announce_peer 中提取 infohash
//! 4. 对发现的 infohash 主动发 get_peers，收集 peer 并存入缓存

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// v10(C2)：全爬虫统一发送门（进程级）——所有 UDP 请求发送路径共用同一限速器，
/// 根治「仅链式采集节流、其余路径未限」导致的 112/s 风暴（.53 00:47 长查询门
/// 被爬虫占满 → federation read_long 60s 超时 panic 崩溃的根因）。
pub static GP_SEND_LAST: AtomicU64 = AtomicU64::new(0);
/// 每秒发送轮数上限：100ms/轮 = ≤10/s（总门，覆盖 get_peers/scrape/announce 等全部路径）
pub const GP_SEND_INTERVAL_MS: u64 = 100;

use async_trait::async_trait;
use parking_lot::RwLock;
use rand::Rng;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::CrawlerConfig;
use crate::dht::kbucket::KBucketEntry;
use crate::dht::routing_table::RoutingTable;
use crate::discoverers::dht::message::{DhtMessage, DhtNode, QueryMethod};
use crate::event_bus::EventBus;
use crate::net::socket_opts::create_udp_socket;
use crate::storage::repo_traits::InfohashRepository;
use crate::storage::PeerRepoImpl;
use crate::types::{Event, Infohash, PeerInfo, PeerSource};

use super::buffer_pool::CrawlerBufferPool;
use super::rate_limiter::RateLimiter;
use crate::intelligence::AdaptiveController;

use super::Crawler;

/// PPS 统计快照类型（上次统计时间 + 各 socket 发送累计 + 接收累计）
type PpsSnapshot = Option<(Instant, Vec<u64>, Vec<u64>)>;

/// 内置热门 infohash（用于主动 get_peers，增加被查询概率）
const POPULAR_INFOHASHES: &[&str] = &[
    "aa8a2f25763f0b165766690847bf732476490396", // xubuntu-25.10
    "299671d28121049a9265be9062d503c4d8402cfb", // kubuntu-24.04.4
    "c84b227d26b6c05f6ab92f5073d18a9cb84dacd6", // lubuntu-24.04.4
    "92b187cdfc4d926a13e7620d44bd18695f0150fb", // kubuntu-25.10
    "7751f52345b9a0b4bc03782611b9e676d63ce294", // kubuntu-26.04.1
    "1235d8b8e4e0314c80ddd39c755d5582803c9609", // lubuntu-25.10
    "7ddcb3cf9dbbc96d3c08fb808f57d59fa42e8bfb", // kubuntu-22.04.5
    "337ef6470ff715be2e09882624daeb9b64d4cb10", // lubuntu-22.04.5
    "f73430dbfaf0031f9c5fddcf0adc340456db4c91", // edubuntu-24.04.4
    "2b66980093bc11806fab50cb3cb41835b95a0362", // ubuntu-24.04
];

/// 新 infohash 发现日志采样计数器（每 100 条打一次日志）
static INFOHASH_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);

/// 爬行进度日志上次打印时间（Unix 毫秒），用于按时间间隔采样
static LAST_PROGRESS_LOG_MS: AtomicU64 = AtomicU64::new(0);

/// 未识别/error 消息日志采样计数器（每 200 条打一次日志；2026-10 fix2 观测用）
static UNPARSED_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);

/// 解析消息处理并发上限：配置为 0 时按 CPU 核数 / 4 自动计算，下限 1。
///
/// 同步 repo 调用已移到 `spawn_blocking` 线程池执行，Semaphore 仅作为
/// blocking 任务的并发上限，避免一次性把 blocking 线程池占满拖垮 API。
fn resolve_max_concurrent_msg_handlers(cfg: u32) -> usize {
    if cfg > 0 {
        return cfg as usize;
    }
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    (cpus / 4).max(1)
}

/// 爬虫运行状态
#[derive(Debug, Clone, Default)]
pub struct CrawlerState {
    /// 是否在运行
    pub running: bool,
    /// 已爬行的节点数
    pub nodes_crawled: u64,
    /// 已收集的 infohash 数
    pub infohashes_collected: u64,
    /// 已收集的 peer 数
    pub peers_collected: u64,
    /// 已知节点数
    pub known_nodes: usize,
    /// 爬行开始时间
    pub started_at: Option<Instant>,
    /// 最后一次爬行时间
    pub last_crawl_at: Option<Instant>,
    /// 错误数
    pub errors: u64,
    /// 收到的消息总数
    pub messages_received: u64,
    /// 主动发送的请求数
    pub requests_sent: u64,
    /// 入站 ping 请求数（被动打洞指标）
    pub inbound_ping: u64,
    /// 入站 find_node 请求数（被动打洞指标）
    pub inbound_find_node: u64,
    /// 入站 get_peers 请求数（被动打洞指标）
    pub inbound_get_peers: u64,
    /// 入站 announce_peer 请求数（被动打洞指标）
    pub inbound_announce_peer: u64,
    /// 入站 sample_infohashes 请求数
    pub inbound_sample_infohashes: u64,
    /// 入站请求总数（其他节点主动连接我们的次数）
    pub inbound_total: u64,
    /// 入站请求唯一来源节点数（被动打洞效果指标）
    pub inbound_unique_sources: usize,
    /// tid 命中 pending 的真实响应数（进入响应率信号）
    pub responses_matched_total: u64,
    /// 响应形态但 tid 未命中的迟到/伪造响应数（不进入响应率信号）
    pub late_responses_total: u64,
    /// 真实 RTT 的 EMA（毫秒），替代原 pending 队列年龄估算
    pub latency_ema_ms: u64,
    /// 每 socket 发送 PPS（滑动窗口，最近10秒）
    pub socket_send_pps: Vec<u64>,
    /// 每 socket 接收 PPS（滑动窗口，最近10秒）
    pub socket_recv_pps: Vec<u64>,
    /// 每 socket 响应率（0.0-1.0，复用 RateLimiter 数据）
    pub socket_response_rates: Vec<f64>,
    /// 全局综合响应率（0.0-1.0，RateLimiter 聚合所有 socket）
    pub global_response_rate: f64,
    /// pending 表各分片长度
    pub pending_shard_lens: Vec<usize>,
    /// paced 发送队列当前长度（round 模式恒 0；19号 D3）
    pub send_queue_len: u64,
    /// paced 队列满/限速收敛丢弃累计（19号 D3）
    pub enqueue_dropped_total: u64,
    /// 流式平滑发送模式是否启用（19号 D3）
    pub paced_mode: bool,
    /// UDP 丢包估算（发送-响应-超时，最近60s）
    pub udp_packet_loss_estimate: f64,
    /// 节点选择耗时（最近10次均值，微秒）
    pub node_select_avg_us: u64,
    /// 当前自适应发送倍率（0.2-2.0）
    pub adaptive_multiplier: f64,
    /// 预测的下一轮响应率（None=模型置信度不足）
    pub predicted_response_rate: Option<f64>,
    /// 预测模型更新次数
    pub model_update_count: u64,
    /// 历史记录条数
    pub history_len: usize,
    /// 当前实际使用的并发 socket 数
    pub concurrent_sockets_in_use: usize,
}

/// 待响应的请求记录
struct PendingRequest {
    _method: QueryMethod,
    target: [u8; 20],
    addr: SocketAddr,
    sent_at: Instant,
    /// 所属规划轮次（0 = 非规划发送，不参与轮次反馈统计）
    round_seq: u64,
    /// 选择层标签（0=已验证 L0 / 1=新鲜 L1 / 2=探索；非规划发送无分层语义，记 0）
    layer: u8,
}

/// 单个规划轮次的真实应答/超时累计（tid 命中时自增，增量报告后留存至 300s 兜底清理）
#[derive(Debug)]
struct RoundFeedbackAcc {
    responded: u64,
    timed_out: u64,
    /// 已报告水位（增量报告用）：responded 与 timed_out 各自独立计数，
    /// 单一水位在二者不等时会重复上报或下溢（2026-10 修复）
    reported_responded: u64,
    reported_timed_out: u64,
    started_at: Instant,
    /// 分层账目：按下标 [L0 已验证, L1 新鲜, 探索] 记发送数/响应数/已报告数
    layer_sent: [u64; 3],
    layer_responded: [u64; 3],
    layer_reported: [u64; 3],
}

impl Default for RoundFeedbackAcc {
    fn default() -> Self {
        Self {
            responded: 0,
            timed_out: 0,
            reported_responded: 0,
            reported_timed_out: 0,
            started_at: Instant::now(),
            layer_sent: [0; 3],
            layer_responded: [0; 3],
            layer_reported: [0; 3],
        }
    }
}

/// 流式发送队列条目（19号 D3）：生产者只入队，paced 消费者按预算平滑发送
/// 2026-10 fix2 后 active_* 改为直发（不再入队），部分变体暂未构造——
/// 保留枚举完整性（链式采集/未来回退入队路径仍引用类型），clippy 放行。
#[derive(Debug)]
#[allow(dead_code)]
enum SendWorkItem {
    FindNode {
        addr: SocketAddr,
        target: [u8; 20],
        layer: u8,
        round_seq: u64,
    },
    GetPeers {
        addr: SocketAddr,
        infohash: [u8; 20],
        layer: u8,
        round_seq: u64,
    },
    Sample {
        addr: SocketAddr,
        layer: u8,
        round_seq: u64,
    },
    Scrape {
        addr: SocketAddr,
        infohash: [u8; 20],
        layer: u8,
        round_seq: u64,
    },
}

/// pending 请求分片：16 个 RwLock<HashMap>，按 tid 字节路由
type PendingShards = Vec<RwLock<HashMap<Vec<u8>, PendingRequest>>>;

/// `handle_query_sync` 产物：在 blocking 线程池完成解析与响应字节构建后，
/// 回到 async 层发送响应、按需回发 get_peers、发布 infohash 事件。
struct QuerySyncResult {
    /// 待发送的 DHT 查询响应字节
    resp_bytes: Vec<u8>,
    /// GetPeers 分支：需在 async 层回发 get_peers（注册 pending + 发送）
    followup_get_peers: Option<Infohash>,
    /// 新发现的 infohash（GetPeers / AnnouncePeer 分支）
    discovered_infohash: Option<Infohash>,
}

/// 根据 transaction_id 选择分片（取第一个字节 mod 16），减少锁竞争
#[inline]
fn pending_shard(tid: &[u8]) -> usize {
    (tid[3] as usize) % 16
}

/// paced 发送队列容量（19号 D3）：兜底轮次突发的缓冲深度
const PACED_QUEUE_CAPACITY: usize = 8192;

/// 零响应熔断参数（2026-10-09 新增，回应生产事故）。
///
/// 全部为**协议级安全常量**，非业务可调参数：熔断是止损手段，取值偏保守
/// （宁可多空转几轮，也不误杀正常爬取）。
mod zero_response_circuit {
    /// 连续多少轮「有发送但零响应」后熔断。
    /// 取 5：active_crawl 默认轮周期 40s，5 轮 ≈ 3.3 分钟才止损。
    ///
    /// 2026-10-09 实测修正（原 3）：3 轮仅约 2 分钟，而 DHT 单轮抖动很常见，
    /// 生产观测 25 分钟内熔断 5 次、每轮都打断有效探测；但熔断机制本身是
    /// 有效的（恢复后 L1 响应率可从 0 回升到 28），敏感的是阈值而非逻辑。
    /// 取 5 仍能在「持续约 3 分钟完全无响应」时止损，且能容忍单轮抖动。
    pub const TRIGGER_ROUNDS: u32 = 5;
    /// 熔断后的冷却时长（秒），到期后放行一轮「探测」。
    pub const COOLDOWN_SECS: u64 = 120;
    /// 探测轮成功（收到任意响应）后，连续多少轮健康才完全解除熔断。
    pub const RECOVER_ROUNDS: u32 = 2;
    /// 单轮「有发送」的最低阈值：低于此值不算「零响应」（样本不足，勿误判）。
    pub const MIN_SENT_FOR_JUDGEMENT: u64 = 10;
}

/// 零响应熔断状态机。
///
/// 语义：`Idle`（未熔断）→ 连续 [`TRIGGER_ROUNDS`](zero_response_circuit::TRIGGER_ROUNDS)
/// 轮零响应 → `Tripped`（停止主动外发）→ 冷却 [`COOLDOWN_SECS`](zero_response_circuit::COOLDOWN_SECS)
/// → `HalfOpen`（放行一轮探测）→ 探测有响应则计数，够
/// [`RECOVER_ROUNDS`](zero_response_circuit::RECOVER_ROUNDS) 轮完全恢复；
/// 探测仍零响应则回到 `Tripped` 并再计一次冷却。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircuitState {
    /// 正常放行
    Idle,
    /// 已熔断：冷却中，主动外发一律跳过
    Tripped,
    /// 冷却到期：放行一轮探测
    HalfOpen,
}

/// 零响应熔断状态（Mutex 保护，与 `round_feedback` 同层）。
#[derive(Debug)]
struct ZeroResponseCircuit {
    state: CircuitState,
    /// 连续零响应轮数
    zero_rounds: u32,
    /// 连续健康轮数（仅 HalfOpen 探测时累加）
    healthy_rounds: u32,
    /// 熔断/冷却起始时刻
    tripped_at: Option<Instant>,
    /// 累计熔断次数（观测用）
    trip_count: u64,
}

impl Default for ZeroResponseCircuit {
    fn default() -> Self {
        Self {
            state: CircuitState::Idle,
            zero_rounds: 0,
            healthy_rounds: 0,
            tripped_at: None,
            trip_count: 0,
        }
    }
}

impl ZeroResponseCircuit {
    /// 喂入一轮观测结果，判定是否熔断；返回熔断后的新状态。
    fn observe(&mut self, sent: u64, responded: u64) -> CircuitState {
        use zero_response_circuit as zrc;
        // 样本不足不判定（避免低流量时误熔）
        if sent < zrc::MIN_SENT_FOR_JUDGEMENT {
            return self.state;
        }
        let got_response = responded > 0;
        match self.state {
            CircuitState::Idle => {
                if got_response {
                    self.zero_rounds = 0;
                } else {
                    self.zero_rounds += 1;
                    if self.zero_rounds >= zrc::TRIGGER_ROUNDS {
                        self.state = CircuitState::Tripped;
                        self.tripped_at = Some(Instant::now());
                        self.trip_count += 1;
                        self.healthy_rounds = 0;
                    }
                }
            }
            CircuitState::Tripped => {
                // 冷却未到，继续熔断
                let cooled = self
                    .tripped_at
                    .map(|t| t.elapsed().as_secs() >= zrc::COOLDOWN_SECS)
                    .unwrap_or(true);
                if cooled {
                    // 进入半开。**迁移轮的响应数据同样计入 healthy**：冷却期间
                    // 仍在发探测包，这些包的响应是有效证据。若丢弃它，恢复会
                    // 多等一轮（RECOVER_ROUNDS），平白拉长不可观测窗口。
                    self.state = CircuitState::HalfOpen;
                    self.healthy_rounds = 0;
                    if got_response {
                        self.healthy_rounds = 1;
                        if self.healthy_rounds >= zrc::RECOVER_ROUNDS {
                            self.state = CircuitState::Idle;
                            self.zero_rounds = 0;
                            self.healthy_rounds = 0;
                            self.tripped_at = None;
                        }
                    }
                }
            }
            CircuitState::HalfOpen => {
                if got_response {
                    self.healthy_rounds += 1;
                    if self.healthy_rounds >= zrc::RECOVER_ROUNDS {
                        // 完全恢复
                        self.state = CircuitState::Idle;
                        self.zero_rounds = 0;
                        self.healthy_rounds = 0;
                        self.tripped_at = None;
                    }
                } else {
                    // 探测仍零响应 → 重新熔断并再计一次冷却
                    self.state = CircuitState::Tripped;
                    self.tripped_at = Some(Instant::now());
                    self.trip_count += 1;
                    self.healthy_rounds = 0;
                }
            }
        }
        self.state
    }

    /// 当前是否应**跳过**主动外发。
    fn should_skip_send(&self) -> bool {
        self.state == CircuitState::Tripped
    }
}

/// 爬虫引擎
///
/// 主动 + 被动混合 DHT 爬虫。
pub struct CrawlerEngine {
    config: CrawlerConfig,
    state: Arc<RwLock<CrawlerState>>,
    event_bus: EventBus,
    shutdown: Arc<tokio::sync::Notify>,
    /// 已收集的 infohash（去重）
    seen_infohashes: Arc<RwLock<HashSet<Infohash>>>,
    /// 入站请求来源节点集合（被动打洞效果分析）
    inbound_sources: Arc<RwLock<HashSet<std::net::SocketAddr>>>,
    /// Kademlia 路由表
    known_nodes: Arc<RwLock<RoutingTable>>,
    /// 待响应的请求（transaction_id -> PendingRequest），16分片减少锁竞争
    pending: Arc<PendingShards>,
    /// Peer 仓库（可选，用于存入发现的 peer）
    peer_repo: Option<Arc<PeerRepoImpl>>,
    /// 持久化存储（可选）
    storage: Option<Arc<crate::storage::Storage>>,
    /// 节点数据仓库（可选，与路由表共享，负责 SQLite 持久化）
    node_repo: Option<Arc<crate::storage::NodeRepoImpl>>,
    /// Infohash 数据仓库（可选，发现新 infohash 时 register）
    infohash_repo: Option<Arc<crate::storage::InfohashRepoImpl>>,
    /// 我们的节点 ID
    node_id: [u8; 20],
    /// 虚拟节点 ID 列表（多虚拟节点，覆盖更多 DHT ID 空间区域）
    virtual_node_ids: Vec<[u8; 20]>,
    /// DNS 解析池（5 个公共 DNS 并行查询）
    dns_pool: Arc<crate::dns_pool::DnsPool>,
    /// 全量同步暂停门：为 true 时跳过主动爬行（让带宽/CPU 给联邦同步）
    pause_gate: Option<Arc<AtomicBool>>,
    /// UDP socket 列表（多 socket 并行收包，socket_count 个）
    sockets: Vec<Arc<UdpSocket>>,
    /// 轮询发送的 socket 索引（原子操作，跨 clone 共享）
    send_socket_idx: Arc<AtomicUsize>,
    /// 全部限速时的轮询计数器（原子操作，跨 clone 共享，避免兜底固定单端口）
    throttled_round_robin: Arc<AtomicUsize>,
    /// 对端响应率自适应限速器（None=禁用）
    rate_limiter: Option<Arc<RateLimiter>>,
    /// 自适应控制器（ICC 预测式自适应，None=禁用，行为与改造前一致）
    adaptive_controller: Option<Arc<AdaptiveController>>,
    /// 接收缓冲区对象池（多 socket 共享，避免每次 64KB 分配）
    buffer_pool: Arc<CrawlerBufferPool>,
    /// 预热是否完成（完成前不触发全量爬行）
    warmup_done: Arc<AtomicBool>,
    /// 零响应熔断状态（2026-10-09 新增）：连续 N 轮「有发送但零响应」后置位，
    /// 暂停主动外发，直到冷却到期或探测恢复。
    ///
    /// 背景（2026-10-09 生产实证）：三级响应率`L0 0/82 L1 0/903 探索 0/409`
    /// 全零的条件下，「链式直接采集」仍每8 秒发20轮 get_peers 空包且**无成功率
    /// 熔断**——纯烧 socket 与带宽、零产出。更糟的是这些空包仍走pending 登记
    /// 与超时清理，写侧持续产出待落盘数据，把 Persistence 分类的唯一槽位压死，
    /// 间接引发全局停摆（详见 task_scheduler.rs 卡死保底注释）。
    /// 熔断让「无效外发」在源头停下，而不是在下游靠槽位抢救。
    zero_response_circuit: Arc<Mutex<ZeroResponseCircuit>>,
    /// 消息处理并发限制（避免多 recv_loop 同时阻塞 worker 线程导致 API 饥饿）
    message_semaphore: Arc<tokio::sync::Semaphore>,
    /// 每 socket 累计发送数（用于计算 PPS，无锁原子计数，Arc 共享）
    socket_send_total: Arc<Vec<AtomicU64>>,
    /// 每 socket 累计接收数（用于计算 PPS，无锁原子计数，Arc 共享）
    socket_recv_total: Arc<Vec<AtomicU64>>,
    /// 上次统计 PPS 的时间和累计值（Mutex 保护，Arc 共享）
    pps_last: Arc<Mutex<PpsSnapshot>>,
    /// 规划轮次序号发生器（0 保留给非规划发送）
    round_seq: Arc<AtomicU64>,
    /// 各规划轮次的真实应答/超时累计（报告后移除，>60s 兜底清理）
    round_feedback: Arc<Mutex<BTreeMap<u64, RoundFeedbackAcc>>>,
    /// 真实 RTT 的 EMA（毫秒），tid 命中时更新
    latency_ema_ms: Arc<Mutex<Option<f64>>>,
    /// paced 流式发送队列（19号 D3）：start() 时建立；round 模式保持 None
    send_tx: OnceLock<mpsc::Sender<SendWorkItem>>,
    /// paced 消费端（Arc 共享给 clone_for_async）
    send_rx: OnceLock<Arc<tokio::sync::Mutex<mpsc::Receiver<SendWorkItem>>>>,
    /// 队列当前长度（入队 +1 / 出队 -1）
    send_queue_len: Arc<AtomicU64>,
    /// 队列满/限速收敛时被丢弃的条目累计
    enqueue_dropped_total: Arc<AtomicU64>,
    /// paced 模式标记（启动时确定，1=启用）
    paced_mode: Arc<AtomicU64>,
}

impl CrawlerEngine {
    /// 创建新的爬虫引擎
    pub fn new(config: CrawlerConfig, event_bus: EventBus) -> Self {
        let mut node_id = [0u8; 20];
        rand::thread_rng().fill(&mut node_id);

        // 生成 8 个虚拟节点 ID，覆盖更多 DHT ID 空间区域
        let mut virtual_node_ids = vec![node_id];
        for _ in 0..7 {
            let mut vid = [0u8; 20];
            rand::thread_rng().fill(&mut vid);
            virtual_node_ids.push(vid);
        }

        let dns_pool = Arc::new(crate::dns_pool::DnsPool::new().unwrap_or_else(|e| {
            tracing::warn!("[crawler] DNS 池初始化失败: {}，将使用系统 DNS", e);
            // 降级：仍然创建一个池（内部会用默认配置）
            crate::dns_pool::DnsPool::new().unwrap()
        }));

        // 消息处理并发上限：配置为 0 时按 CPU 核数/4 自动计算
        let max_concurrent =
            resolve_max_concurrent_msg_handlers(config.max_concurrent_msg_handlers);

        Self {
            config,
            state: Arc::new(RwLock::new(CrawlerState::default())),
            event_bus,
            shutdown: Arc::new(tokio::sync::Notify::new()),
            seen_infohashes: Arc::new(RwLock::new(HashSet::new())),
            inbound_sources: Arc::new(RwLock::new(HashSet::new())),
            known_nodes: Arc::new(RwLock::new(RoutingTable::new(node_id))),
            pending: Arc::new((0..16).map(|_| RwLock::new(HashMap::new())).collect()),
            peer_repo: None,
            storage: None,
            node_repo: None,
            infohash_repo: None,
            node_id,
            virtual_node_ids,
            dns_pool,
            pause_gate: None,
            sockets: Vec::new(),
            send_socket_idx: Arc::new(AtomicUsize::new(0)),
            throttled_round_robin: Arc::new(AtomicUsize::new(0)),
            rate_limiter: None,
            adaptive_controller: None,
            buffer_pool: Arc::new(CrawlerBufferPool::new(16)),
            warmup_done: Arc::new(AtomicBool::new(false)),
            zero_response_circuit: Arc::new(Mutex::new(ZeroResponseCircuit::default())),
            message_semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrent)),
            socket_send_total: Arc::new((0..16).map(|_| AtomicU64::new(0)).collect()),
            socket_recv_total: Arc::new((0..16).map(|_| AtomicU64::new(0)).collect()),
            pps_last: Arc::new(Mutex::new(None)),
            round_seq: Arc::new(AtomicU64::new(0)),
            round_feedback: Arc::new(Mutex::new(BTreeMap::new())),
            latency_ema_ms: Arc::new(Mutex::new(None)),
            send_tx: OnceLock::new(),
            send_rx: OnceLock::new(),
            send_queue_len: Arc::new(AtomicU64::new(0)),
            enqueue_dropped_total: Arc::new(AtomicU64::new(0)),
            paced_mode: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 设置 Peer 仓库引用
    pub fn with_peer_repo(mut self, peer_repo: Arc<PeerRepoImpl>) -> Self {
        self.peer_repo = Some(peer_repo);
        self
    }

    /// 覆盖 DNS 解析池
    ///
    /// 默认在构造时用 [`crate::dns_pool::DnsPool::new`]（即内置公共 DNS，
    /// 不读系统 DNS）；此处用于换成启动时按 `config.dns` 初始化的进程级实例。
    pub fn with_dns_pool(mut self, pool: Arc<crate::dns_pool::DnsPool>) -> Self {
        self.dns_pool = pool;
        self
    }

    /// 随机返回一个虚拟节点 ID（多虚拟节点，覆盖更多 DHT ID 空间）
    fn random_virtual_node_id(&self) -> [u8; 20] {
        let idx = rand::random::<usize>() % self.virtual_node_ids.len();
        self.virtual_node_ids[idx]
    }

    /// 设置持久化存储
    pub fn with_storage(mut self, storage: Arc<crate::storage::Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// 设置节点数据仓库（与路由表共享，负责 SQLite 持久化）
    pub fn with_node_repo(mut self, node_repo: Arc<crate::storage::NodeRepoImpl>) -> Self {
        self.node_repo = Some(node_repo);
        self
    }

    /// 设置 Infohash 数据仓库（发现新 infohash 时 register）
    pub fn with_infohash_repo(mut self, repo: Arc<crate::storage::InfohashRepoImpl>) -> Self {
        self.infohash_repo = Some(repo);
        self
    }

    /// 设置全量同步暂停门（为 true 时暂停主动爬行）
    pub fn with_pause_gate(mut self, gate: Option<Arc<AtomicBool>>) -> Self {
        self.pause_gate = gate;
        self
    }

    /// 注入自适应控制器（ICC 预测式自适应）
    pub fn with_adaptive_controller(mut self, controller: Arc<AdaptiveController>) -> Self {
        self.adaptive_controller = Some(controller);
        self
    }

    /// 注入 UDP socket 列表（多 socket 并行收包）
    pub fn with_sockets(mut self, sockets: Vec<Arc<UdpSocket>>) -> Self {
        self.sockets = sockets;
        if self.config.adaptive_rate_limit && !self.sockets.is_empty() {
            self.rate_limiter = Some(Arc::new(RateLimiter::with_config(
                self.sockets.len(),
                true,
                std::time::Duration::from_secs(self.config.rate_limit_window_secs),
                self.config.rate_limit_enter_threshold,
                self.config.rate_limit_exit_threshold,
                self.config.rate_limit_min_samples as usize,
                self.config.rate_limit_throttle_skip_ratio,
            )));
        }
        self
    }

    /// 获取状态快照
    pub fn state(&self) -> CrawlerState {
        let mut s = self.state.read().clone();
        // 方向C：known_nodes 显示 NodeRepo 节点数（主候选池），路由表只用于 DHT 路由
        s.known_nodes = self
            .node_repo
            .as_ref()
            .map(|r| r.len_sync())
            .unwrap_or_else(|| self.known_nodes.read().len());
        // 自适应控制器指标
        if let Some(ac) = &self.adaptive_controller {
            s.adaptive_multiplier = ac.current_multiplier();
            s.predicted_response_rate = ac.predicted_rate();
            s.model_update_count = ac.model_update_count();
            s.history_len = ac.history_len();
        }
        s
    }

    /// 路由表引用（供外部访问）
    pub fn routing_table(&self) -> Arc<RwLock<RoutingTable>> {
        self.known_nodes.clone()
    }

    /// 响应率自适应限速器引用（供 config reloader 热更阈值；未启用时为 None）
    pub fn rate_limiter(&self) -> Option<Arc<RateLimiter>> {
        self.rate_limiter.clone()
    }

    /// 获取状态的 Arc 引用（用于共享到 HTTP API）
    pub fn state_arc(&self) -> Arc<RwLock<CrawlerState>> {
        self.state.clone()
    }

    /// 更新监控指标（由定时任务调用，刷新滑动窗口/分片长度等聚合指标）
    pub fn update_metrics(&self) {
        let mut state = self.state.write();
        // pending 分片长度
        state.pending_shard_lens = self.pending.iter().map(|s| s.read().len()).collect();
        // 响应率（从 rate_limiter 取）
        if let Some(rl) = &self.rate_limiter {
            state.socket_response_rates = rl.response_rates();
            state.global_response_rate = rl.global_response_rate();
        }
        // socket 数量对齐
        let n = self.sockets.len().max(1);

        state.socket_send_pps.resize(n, 0);
        state.socket_recv_pps.resize(n, 0);

        // 计算 PPS（滑动窗口：当前累计 - 上次累计 / 时间差）
        let now = Instant::now();
        let current_send: Vec<u64> = (0..n)
            .map(|i| self.socket_send_total[i].load(Ordering::Relaxed))
            .collect();
        let current_recv: Vec<u64> = (0..n)
            .map(|i| self.socket_recv_total[i].load(Ordering::Relaxed))
            .collect();
        let mut pps_last = self.pps_last.lock().unwrap();
        if let Some((last_time, last_send, last_recv)) = pps_last.as_ref() {
            let elapsed = now.duration_since(*last_time).as_secs_f64();
            if elapsed > 0.0 {
                debug!("[crawler] PPS debug: elapsed={:.2}s current_recv={:?} last_recv={:?} current_send={:?} last_send={:?}", elapsed, current_recv, last_recv, current_send, last_send);
                state.socket_send_pps = current_send
                    .iter()
                    .zip(last_send.iter())
                    .map(|(c, l)| ((c.saturating_sub(*l)) as f64 / elapsed) as u64)
                    .collect();
                state.socket_recv_pps = current_recv
                    .iter()
                    .zip(last_recv.iter())
                    .map(|(c, l)| ((c.saturating_sub(*l)) as f64 / elapsed) as u64)
                    .collect();
            }
        }
        *pps_last = Some((now, current_send, current_recv));
        // UDP 丢包估算窗口化（19号 D1.5）：1 − 限速器 60s 滑动窗口全局响应率，
        // 与字段注释一致；原实现用生命周期累计值，启动初期恒偏低且无窗口语义
        state.udp_packet_loss_estimate = match &self.rate_limiter {
            Some(rl) => (1.0 - rl.global_response_rate()).clamp(0.0, 1.0),
            None => 0.0,
        };
        // paced 发送队列观测（19号 D3）
        state.send_queue_len = self.send_queue_len.load(Ordering::Relaxed);
        state.enqueue_dropped_total = self.enqueue_dropped_total.load(Ordering::Relaxed);
        state.paced_mode = self.is_paced();
        // 自适应控制器指标
        if let Some(ac) = &self.adaptive_controller {
            state.adaptive_multiplier = ac.current_multiplier();
            state.predicted_response_rate = ac.predicted_rate();
            state.model_update_count = ac.model_update_count();
            state.history_len = ac.history_len();
        }
        state.concurrent_sockets_in_use = self.config.concurrent_sockets.min(self.sockets.len());
    }

    /// 获取已收集的 infohash 集合（用于 TrackerPeerFetcher 同步）
    pub fn seen_infohashes(&self) -> Arc<RwLock<HashSet<Infohash>>> {
        self.seen_infohashes.clone()
    }

    /// 获取第一个 UDP socket（兼容旧调用）
    pub fn get_socket(&self) -> Option<Arc<UdpSocket>> {
        self.sockets.first().cloned()
    }

    /// 轮询获取一个发送用的 socket 及其索引（自适应限速跳过低响应率 socket）
    fn next_send_socket(&self) -> (usize, Arc<UdpSocket>) {
        let n = self.sockets.len();
        for _ in 0..n {
            let idx = self.send_socket_idx.fetch_add(1, Ordering::Relaxed) % n;
            if let Some(rl) = &self.rate_limiter {
                if rl.should_skip(idx) {
                    debug!(
                        "[crawler] next_send_socket: idx={} skipped (throttled)",
                        idx
                    );
                    continue;
                }
            }
            debug!("[crawler] next_send_socket: selected idx={}", idx);
            return (idx, self.sockets[idx].clone());
        }
        // 全部被限速：轮询所有 socket 发送，避免兜底固定在单端口（#0）导致发送集中
        let idx = self.throttled_round_robin.fetch_add(1, Ordering::Relaxed) % n;
        debug!(
            "[crawler] next_send_socket: all throttled, fallback idx={}",
            idx
        );
        (idx, self.sockets[idx].clone())
    }

    /// 根据 socket 索引派生独立 node_id（node_id[0] ^ socket_idx）
    fn socket_node_id(&self, idx: usize) -> [u8; 20] {
        let mut id = self.node_id;
        id[0] ^= idx as u8;
        id
    }

    /// 从 NodeRepo 多样性选取高评分节点
    ///
    /// 【统一收口】节点选择逻辑统一由 intelligence 层的 SelectSystem 负责
    /// 选取规则：
    /// 1. 过滤 Bad 状态节点，按评分降序取 top 候选池
    /// 2. Kademlia ID 空间分桶轮询（按 ID 高 4 位分 16 桶），保证 ID 空间多样性
    /// 3. IP /24 网段去重（同一网段最多选 max_per_subnet 个），保证网络多样性
    /// 4. 每轮从各桶取评分最高的节点，轮询直到选满 count 个
    fn select_diverse_nodes(&self, count: usize, max_per_subnet: usize) -> Vec<KBucketEntry> {
        self.select_diverse_nodes_tagged(count, max_per_subnet)
            .into_iter()
            .map(|(e, _)| e)
            .collect()
    }

    /// 收集在飞请求地址（pending 表），供本轮选择排除——避免同一节点
    /// 在上一轮请求未超时时再次入选、重复消耗预算（2026-10 D2/R7）
    fn pending_addrs(&self) -> std::collections::HashSet<SocketAddr> {
        let mut set = std::collections::HashSet::new();
        for shard in self.pending.iter() {
            for req in shard.read().values() {
                set.insert(req.addr);
            }
        }
        set
    }

    /// 【统一收口】分层选择（2026-10 20号方案 D2）：返回 (节点, 层标签)。
    /// 层标签随 pending 注册进入分层反馈账目（L0 已验证 / L1 新鲜 / 探索），
    /// 每轮日志输出分层响应率——选节点质量的核心观测指标。
    fn select_diverse_nodes_tagged(
        &self,
        count: usize,
        max_per_subnet: usize,
    ) -> Vec<(KBucketEntry, u8)> {
        let repo = match &self.node_repo {
            Some(r) => r,
            None => return Vec::new(),
        };
        let exclude = self.pending_addrs();
        let params = crate::intelligence::select_system::SelectionParams {
            layered: self.config.select_mode != "legacy",
            explore_ratio: self.config.select_explore_ratio,
            verified_recent_secs: self.config.select_verified_recent_secs,
            reprobe_min_interval_secs: self.config.reprobe_min_interval_secs,
            exclude: &exclude,
        };
        crate::intelligence::SelectSystem::select_diverse_nodes_tagged(
            repo.as_ref(),
            count,
            max_per_subnet,
            &params,
        )
    }

    /// 从 NodeRepo(SQLite) 加载路由表（启动时调用）
    /// 优先从 SQLite 加载，兼容旧版 JSON 文件
    async fn load_routing_table(&self) -> usize {
        // NodeRepo 已由 main.rs 的 load_initial() 预加载到内存，
        // crawler 直接使用内存中的节点，不再全量从 SQLite 加载。
        if let Some(repo) = &self.node_repo {
            let count = repo.len_sync();
            if count > 0 {
                info!("[crawler] 使用 NodeRepo 内存中 {} 个节点", count);
                let top_nodes = repo.top_nodes_sync(128);
                let mut rt = self.known_nodes.write();
                let mut rt_added = 0;
                for entry in &top_nodes {
                    if rt.add_node(entry.id, entry.addr) {
                        rt_added += 1;
                    }
                }
                info!(
                    "[crawler] 路由表加入 {}/{} 个 top 节点（K=16限制）",
                    rt_added,
                    top_nodes.len()
                );
                return count;
            }
        }
        info!("[crawler] 无路由表持久化数据，冷启动");
        0
    }

    /// 生成随机 20 字节 target
    fn random_target(&self) -> [u8; 20] {
        let mut t = [0u8; 20];
        rand::thread_rng().fill(&mut t);
        t
    }

    /// 加入 DHT 网络：向 bootstrap 节点发 find_node 获取初始路由
    /// Bootstrap：向硬编码公共 DHT 路由器发 find_node，把响应节点加入路由表
    ///
    /// 路由表节点的持续爬行由 active_crawl 负责，bootstrap 只负责冷启动注入种子节点。
    pub async fn bootstrap(&self) -> usize {
        if self.sockets.is_empty() {
            return 0;
        }
        let socket = self.sockets[0].clone();
        let mut success = 0;
        let target = self.random_target();

        for (host, port) in &self.config.bootstrap_nodes {
            match self.dns_pool.resolve(host, *port).await {
                Ok(addrs) => {
                    for addr in addrs {
                        // 方向C：bootstrap 节点加入 NodeRepo（无容量限制）
                        if let Some(repo) = &self.node_repo {
                            let bootstrap_id = [0u8; 20]; // bootstrap 节点 ID 未知，用占位符，响应后会更新
                            repo.add_node_sync(bootstrap_id, addr);
                        }
                        if self.send_find_node(&socket, 0, addr, target).await {
                            success += 1;
                        }
                    }
                }
                Err(e) => {
                    debug!("[crawler] DNS 解析 {} 失败: {}", host, e);
                }
            }
        }

        success
    }

    /// 启动预热：从数据库加载最近活跃节点到内存，并行 bootstrap
    ///
    /// 在 start() 之后调用（sockets 已绑定）。预热完成前，
    /// active_crawl 等主动爬行方法会跳过执行。
    pub async fn warmup(&self) {
        info!(
            "[crawler] 启动预热（加载最多 {} 节点，并行 bootstrap {} 个）...",
            self.config.warmup_node_count, self.config.warmup_bootstrap_concurrent
        );

        // 1. 从 NodeRepo(SQLite) 加载最近活跃节点到路由表
        let loaded = self.load_routing_table().await;
        info!("[crawler] 预热: 从数据库加载了 {} 个节点", loaded);

        // 2. bootstrap 引导节点（crawl_loop 中也会执行，此处再次执行确保种子节点充足）
        let bootstrap_count = self.bootstrap().await;
        info!(
            "[crawler] 预热: 向 {} 个 bootstrap 地址发送了 find_node",
            bootstrap_count
        );

        // 3. 标记预热完成，允许主动爬行
        self.warmup_done.store(true, Ordering::Relaxed);
        info!("[crawler] 预热完成，主动爬行已解锁");
    }

    /// 向单个节点发 find_node 请求，返回是否发送成功
    async fn send_find_node(
        &self,
        socket: &UdpSocket,
        socket_idx: usize,
        addr: SocketAddr,
        target: [u8; 20],
    ) -> bool {
        let mut tid = rand::thread_rng().gen::<[u8; 4]>();
        tid[0] = socket_idx as u8;
        let msg = DhtMessage::build_find_node(&tid, &self.node_id, &target);

        {
            let shard = pending_shard(&tid);
            let mut pending = self.pending[shard].write();
            pending.insert(
                tid.to_vec(),
                PendingRequest {
                    _method: QueryMethod::FindNode,
                    target,
                    addr,
                    sent_at: Instant::now(),
                    round_seq: 0,
                    layer: 0,
                },
            );
        }

        {
            let __nw = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let __l = GP_SEND_LAST.load(Ordering::Relaxed);
            let __w = GP_SEND_INTERVAL_MS.saturating_sub(__nw.saturating_sub(__l));
            if __w > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(__w)).await;
            }
            GP_SEND_LAST.store(__nw.saturating_add(__w), Ordering::Relaxed);
        }
        {
            let __nw = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let __l = GP_SEND_LAST.load(Ordering::Relaxed);
            let __w = GP_SEND_INTERVAL_MS.saturating_sub(__nw.saturating_sub(__l));
            if __w > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(__w)).await;
            }
            GP_SEND_LAST.store(__nw.saturating_add(__w), Ordering::Relaxed);
        }
        if socket.send_to(&msg, addr).await.is_ok() {
            self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
            let mut state = self.state.write();
            state.requests_sent += 1;
            if let Some(rl) = &self.rate_limiter {
                rl.record_request(socket_idx);
            }
            true
        } else {
            false
        }
    }

    // ==================== 自适应 + 多 socket 并发发送辅助方法 ====================

    /// 计算所有 pending 分片的总长度
    fn pending_total(&self) -> usize {
        self.pending.iter().map(|s| s.read().len()).sum()
    }

    /// 分配新的规划轮次序号（0 保留给非规划发送，不参与反馈统计）
    fn next_round_seq(&self) -> u64 {
        self.round_seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// 打开轮次反馈账目（报告即使零应答也能取到）；
    /// layer_sent 为本轮各层 [L0, L1, 探索] 的选中发送数（分层响应率观测用）。
    fn open_round_feedback(&self, seq: u64, layer_sent: [u64; 3]) {
        if seq != 0 {
            let acc = RoundFeedbackAcc {
                layer_sent,
                ..Default::default()
            };
            self.round_feedback.lock().unwrap().insert(seq, acc);
        }
    }

    /// 取走并清除轮次反馈（responded, timed_out），顺带清理超过 300s 的陈旧账目。
    /// 仅用于不参与自适应决策的轮次（get_peers/sample）清理防泄漏。
    fn take_round_feedback(&self, seq: u64) -> (u64, u64) {
        let mut map = self.round_feedback.lock().unwrap();
        let acc = map.remove(&seq);
        // [ALLOWED-HARDCODED: 轮次反馈账目陈旧清理的固定窗口常量，非业务可调参数]
        map.retain(|_, acc| acc.started_at.elapsed() < Duration::from_secs(300));
        match acc {
            Some(a) => (a.responded, a.timed_out),
            None => (0, 0),
        }
    }

    /// 聚合上报所有未过期账目（≤300s）的增量（19号批次F 修复）：
    /// 响应到达延迟实测可达 ~2 分钟（数倍于 40s 轮周期），此前 active_crawl 只
    /// drain「上一轮」账目一次，>40s 才到达的认领虽已计入账目（限速器窗口可见 ~70%），
    /// 却永远不会再被上报给控制器（仅 ~11%）→ 倍率被锁死下限。
    /// 现改为每轮聚合全部账目「自上次上报以来」的增量（reported 水位终于生效）：
    /// 响应无论多晚到达，都会在后续某轮的聚合中被计入恰好一次。
    fn drain_all_round_feedback_delta(&self) -> (u64, u64, [u64; 3], [u64; 3]) {
        let mut map = self.round_feedback.lock().unwrap();
        // [ALLOWED-HARDCODED: 轮次反馈账目陈旧清理的固定窗口常量，非业务可调参数]
        map.retain(|_, acc| acc.started_at.elapsed() < Duration::from_secs(300));
        let mut responded = 0u64;
        let mut timed_out = 0u64;
        let mut layer_sent = [0u64; 3];
        let mut layer_resp = [0u64; 3];
        for acc in map.values_mut() {
            responded += acc.responded.saturating_sub(acc.reported_responded);
            acc.reported_responded = acc.responded;
            timed_out += acc.timed_out.saturating_sub(acc.reported_timed_out);
            acc.reported_timed_out = acc.timed_out;
            for ((lr, cur), rep) in layer_resp
                .iter_mut()
                .zip(acc.layer_responded.iter())
                .zip(acc.layer_reported.iter_mut())
            {
                *lr += cur.saturating_sub(*rep);
                *rep = *cur;
            }
            for (ls, s) in layer_sent.iter_mut().zip(acc.layer_sent.iter()) {
                *ls += *s;
            }
        }
        (responded, timed_out, layer_sent, layer_resp)
    }

    /// 真实 RTT 的 EMA（毫秒），无样本时返回 100.0
    fn current_latency_ema_ms(&self) -> f64 {
        self.latency_ema_ms.lock().unwrap().unwrap_or(100.0)
    }

    /// 更新真实 RTT 的 EMA（α = 0.2）
    fn update_latency_ema(&self, sample_ms: u64) {
        let mut ema = self.latency_ema_ms.lock().unwrap();
        let s = sample_ms as f64;
        *ema = Some(match *ema {
            Some(v) => v * 0.8 + s * 0.2,
            None => s,
        });
    }

    /// 全量同步暂停门是否置位
    fn is_paused(&self) -> bool {
        self.pause_gate
            .as_ref()
            .map(|g| g.load(Ordering::Relaxed))
            .unwrap_or(false)
    }

    /// 是否处于 paced 流式发送模式（19号 D3）
    fn is_paced(&self) -> bool {
        self.paced_mode.load(Ordering::Relaxed) == 1
    }

    /// 建立 paced 发送通道（幂等；供 start() 与测试使用）
    fn init_paced_channel(&self, capacity: usize) {
        if self.send_tx.get().is_some() {
            return;
        }
        let (tx, rx) = mpsc::channel(capacity);
        let _ = self.send_tx.set(tx);
        let _ = self.send_rx.set(Arc::new(tokio::sync::Mutex::new(rx)));
    }

    /// paced 预算数学（19号 D3）：每 socket 每 tick 配额 × 活跃 socket 数 × 自适应倍率，min 1
    fn paced_budget(max_per_socket: u32, sockets: usize, multiplier: f64) -> usize {
        ((max_per_socket as f64 * sockets.max(1) as f64 * multiplier.clamp(0.2, 4.0)).round()
            as usize)
            .max(1)
    }

    /// 入队一条流式发送；队列满即丢弃并计数（容量兜底轮次突发）
    fn enqueue_send(&self, item: SendWorkItem) -> bool {
        let tx = match self.send_tx.get() {
            Some(tx) => tx,
            None => return false,
        };
        match tx.try_send(item) {
            Ok(()) => {
                self.send_queue_len.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(_) => {
                self.enqueue_dropped_total.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// 批量入队 find_node（active_crawl 用），返回成功入队数
    /// 2026-10 fix2 后 active_crawl 直发（不经过入队/消费者），本方法保留供
    /// paced 模式回退与兼容测试引用，clippy 放行 dead_code。
    #[allow(dead_code)]
    fn enqueue_find_nodes(&self, nodes: &[(KBucketEntry, u8)], round_seq: u64) -> u64 {
        let mut n = 0u64;
        for (entry, layer) in nodes {
            if self.enqueue_send(SendWorkItem::FindNode {
                addr: entry.addr,
                target: self.random_target(),
                layer: *layer,
                round_seq,
            }) {
                n += 1;
            }
        }
        n
    }

    /// paced 消费循环（19号 D3）：每 tick 按预算出队发送；
    /// 全 socket 限速时整 tick 让路（条目保留，等限速解除）。
    /// pause_gate 置位时跳过出队（堆积由队列容量兜底）。
    async fn paced_send_loop(&self) {
        let tick = Duration::from_millis(self.config.paced_tick_ms.max(50));
        // [ALLOWED-INTERVAL] paced 消费 tick：engine 自有常驻任务，不经 TaskScheduler
        // （调度器 300s 硬超时会周期性杀死常驻任务；与 crawl_loop/recv_loop 先例一致，19号 D3）
        let mut interval = tokio::time::interval(tick);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // 2026-10 fix2：消费者健康观测（每 60s 一条，确认队列消费是否正常）
        let mut last_obs = std::time::Instant::now();
        let mut consumed_total = 0u64;
        loop {
            interval.tick().await;
            if self.is_paused() {
                continue;
            }
            let rx = match self.send_rx.get() {
                Some(r) => r.clone(),
                None => continue,
            };
            // 全 socket 限速 → 本 tick 不消费
            if let Some(rl) = &self.rate_limiter {
                let n = self.sockets.len();
                if n > 0 && (0..n).all(|i| rl.should_skip(i)) {
                    continue;
                }
            }
            let multiplier = self
                .adaptive_controller
                .as_ref()
                .map(|c| c.next_rate_multiplier())
                .unwrap_or(1.0);
            let budget = Self::paced_budget(
                self.config.paced_max_per_socket_per_tick,
                self.sockets.len(),
                multiplier,
            );
            for _ in 0..budget {
                let item = {
                    let mut guard = rx.lock().await;
                    guard.try_recv().ok()
                };
                let item = match item {
                    Some(i) => i,
                    None => break,
                };
                self.send_queue_len.fetch_sub(1, Ordering::Relaxed);
                let (socket_idx, socket) = self.next_send_socket();
                if let Some(rl) = &self.rate_limiter {
                    if rl.should_skip(socket_idx) {
                        // next_send_socket 已尽量避免选中限速 socket；
                        // 全限速竞态下收敛本 tick，该条计入丢弃
                        self.enqueue_dropped_total.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
                self.dispatch_send_item(&item, socket_idx, &socket).await;
                consumed_total += 1;
            }
            // [ALLOWED-HARDCODED: paced 消费者健康观测日志周期 60s，纯观测无逻辑影响，保持固定节奏便于日志对齐]
            if last_obs.elapsed() >= Duration::from_secs(60) {
                let dropped_now = self.enqueue_dropped_total.load(Ordering::Relaxed);
                let queue_now = self.send_queue_len.load(Ordering::Relaxed);
                info!(
                    "[crawler] paced 消费者: 60s内消费 {} 条, 队列={}, 累计丢弃={}, 预算={}/tick",
                    consumed_total, queue_now, dropped_now, budget
                );
                last_obs = std::time::Instant::now();
                consumed_total = 0;
            }
        }
    }

    /// 按条目类型构造 tid/pending/发送（paced 消费者分发）
    async fn dispatch_send_item(&self, item: &SendWorkItem, socket_idx: usize, socket: &UdpSocket) {
        match item {
            SendWorkItem::FindNode {
                addr,
                target,
                layer,
                round_seq,
            } => {
                let mut tid = rand::thread_rng().gen::<[u8; 4]>();
                tid[0] = socket_idx as u8;
                let msg = DhtMessage::build_find_node(&tid, &self.node_id, target);
                {
                    let shard = pending_shard(&tid);
                    let mut pending = self.pending[shard].write();
                    pending.insert(
                        tid.to_vec(),
                        PendingRequest {
                            _method: QueryMethod::FindNode,
                            target: *target,
                            addr: *addr,
                            sent_at: Instant::now(),
                            round_seq: *round_seq,
                            layer: *layer,
                        },
                    );
                }
                {
                    let __nw = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let __l = GP_SEND_LAST.load(Ordering::Relaxed);
                    let __w = GP_SEND_INTERVAL_MS.saturating_sub(__nw.saturating_sub(__l));
                    if __w > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(__w)).await;
                    }
                    GP_SEND_LAST.store(__nw.saturating_add(__w), Ordering::Relaxed);
                }
                {
                    let __nw = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let __l = GP_SEND_LAST.load(Ordering::Relaxed);
                    let __w = GP_SEND_INTERVAL_MS.saturating_sub(__nw.saturating_sub(__l));
                    if __w > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(__w)).await;
                    }
                    GP_SEND_LAST.store(__nw.saturating_add(__w), Ordering::Relaxed);
                }
                if socket.send_to(&msg, *addr).await.is_ok() {
                    self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                    let mut s = self.state.write();
                    s.requests_sent += 1;
                    s.nodes_crawled += 1;
                    if let Some(rl) = &self.rate_limiter {
                        rl.record_request(socket_idx);
                    }
                }
            }
            SendWorkItem::GetPeers {
                addr,
                infohash,
                layer,
                round_seq,
            } => {
                let mut tid = rand::thread_rng().gen::<[u8; 4]>();
                tid[0] = socket_idx as u8;
                let vid = self.random_virtual_node_id();
                let msg = DhtMessage::build_get_peers(&tid, &vid, infohash);
                {
                    let shard = pending_shard(&tid);
                    let mut pending = self.pending[shard].write();
                    pending.insert(
                        tid.to_vec(),
                        PendingRequest {
                            _method: QueryMethod::GetPeers,
                            target: *infohash,
                            addr: *addr,
                            sent_at: Instant::now(),
                            round_seq: *round_seq,
                            layer: *layer,
                        },
                    );
                }
                {
                    let __nw = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let __l = GP_SEND_LAST.load(Ordering::Relaxed);
                    let __w = GP_SEND_INTERVAL_MS.saturating_sub(__nw.saturating_sub(__l));
                    if __w > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(__w)).await;
                    }
                    GP_SEND_LAST.store(__nw.saturating_add(__w), Ordering::Relaxed);
                }
                {
                    let __nw = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let __l = GP_SEND_LAST.load(Ordering::Relaxed);
                    let __w = GP_SEND_INTERVAL_MS.saturating_sub(__nw.saturating_sub(__l));
                    if __w > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(__w)).await;
                    }
                    GP_SEND_LAST.store(__nw.saturating_add(__w), Ordering::Relaxed);
                }
                if socket.send_to(&msg, *addr).await.is_ok() {
                    self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                    let mut s = self.state.write();
                    s.requests_sent += 1;
                    if let Some(rl) = &self.rate_limiter {
                        rl.record_request(socket_idx);
                    }
                }
            }
            SendWorkItem::Sample {
                addr,
                layer,
                round_seq,
            } => {
                let mut tid = rand::thread_rng().gen::<[u8; 4]>();
                tid[0] = socket_idx as u8;
                let vid = self.random_virtual_node_id();
                let msg = DhtMessage::build_sample_infohashes(&tid, &vid);
                {
                    let shard = pending_shard(&tid);
                    let mut pending = self.pending[shard].write();
                    pending.insert(
                        tid.to_vec(),
                        PendingRequest {
                            _method: QueryMethod::SampleInfohashes,
                            target: [0u8; 20],
                            addr: *addr,
                            sent_at: Instant::now(),
                            round_seq: *round_seq,
                            layer: *layer,
                        },
                    );
                }
                {
                    let __nw = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let __l = GP_SEND_LAST.load(Ordering::Relaxed);
                    let __w = GP_SEND_INTERVAL_MS.saturating_sub(__nw.saturating_sub(__l));
                    if __w > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(__w)).await;
                    }
                    GP_SEND_LAST.store(__nw.saturating_add(__w), Ordering::Relaxed);
                }
                {
                    let __nw = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let __l = GP_SEND_LAST.load(Ordering::Relaxed);
                    let __w = GP_SEND_INTERVAL_MS.saturating_sub(__nw.saturating_sub(__l));
                    if __w > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(__w)).await;
                    }
                    GP_SEND_LAST.store(__nw.saturating_add(__w), Ordering::Relaxed);
                }
                if socket.send_to(&msg, *addr).await.is_ok() {
                    self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                    let mut s = self.state.write();
                    s.requests_sent += 1;
                    if let Some(rl) = &self.rate_limiter {
                        rl.record_request(socket_idx);
                    }
                }
            }
            SendWorkItem::Scrape {
                addr,
                infohash,
                layer,
                round_seq,
            } => {
                let mut tid = rand::thread_rng().gen::<[u8; 4]>();
                tid[0] = socket_idx as u8;
                let vid = self.random_virtual_node_id();
                let msg = DhtMessage::build_scrape(&tid, &vid, infohash);
                {
                    let shard = pending_shard(&tid);
                    let mut pending = self.pending[shard].write();
                    pending.insert(
                        tid.to_vec(),
                        PendingRequest {
                            _method: QueryMethod::Scrape,
                            target: *infohash,
                            addr: *addr,
                            sent_at: Instant::now(),
                            round_seq: *round_seq,
                            layer: *layer,
                        },
                    );
                }
                {
                    let __nw = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let __l = GP_SEND_LAST.load(Ordering::Relaxed);
                    let __w = GP_SEND_INTERVAL_MS.saturating_sub(__nw.saturating_sub(__l));
                    if __w > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(__w)).await;
                    }
                    GP_SEND_LAST.store(__nw.saturating_add(__w), Ordering::Relaxed);
                }
                {
                    let __nw = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let __l = GP_SEND_LAST.load(Ordering::Relaxed);
                    let __w = GP_SEND_INTERVAL_MS.saturating_sub(__nw.saturating_sub(__l));
                    if __w > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(__w)).await;
                    }
                    GP_SEND_LAST.store(__nw.saturating_add(__w), Ordering::Relaxed);
                }
                if socket.send_to(&msg, *addr).await.is_ok() {
                    self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                    let mut s = self.state.write();
                    s.requests_sent += 1;
                    if let Some(rl) = &self.rate_limiter {
                        rl.record_request(socket_idx);
                    }
                }
            }
        }
    }

    /// 响应 tid 认领：仅当 tid 命中 pending 表时计入响应率与轮次反馈，
    /// 并原子移除（防止迟到重复响应重复计数）。
    ///
    /// 返回 `Some((latency_ms, target))` 表示命中；未命中为迟到/伪造响应，
    /// 只计入 `late_responses_total`，调用方仍可继续处理报文中的节点数据。
    fn claim_pending(&self, socket_idx: usize, tid: &[u8]) -> Option<(u64, [u8; 20])> {
        let removed = {
            let mut pending = self.pending[pending_shard(tid)].write();
            pending.remove(tid)
        };
        match removed {
            Some(req) => {
                if let Some(rl) = &self.rate_limiter {
                    rl.record_response(socket_idx);
                }
                if req.round_seq != 0 {
                    if let Some(acc) = self.round_feedback.lock().unwrap().get_mut(&req.round_seq) {
                        acc.responded += 1;
                        acc.layer_responded[(req.layer as usize).min(2)] += 1;
                    }
                }
                self.update_latency_ema(req.sent_at.elapsed().as_millis() as u64);
                let mut state = self.state.write();
                state.responses_matched_total += 1;
                Some((req.sent_at.elapsed().as_millis() as u64, req.target))
            }
            None => {
                let mut state = self.state.write();
                state.late_responses_total += 1;
                None
            }
        }
    }

    /// 解析实际并发 socket 数（clamp 到 [1, min(config.concurrent_sockets, sockets.len(), 32)]）
    fn resolve_concurrent_sockets(&self, node_count: usize) -> usize {
        let configured = self.config.concurrent_sockets.clamp(1, 32);
        let available = self.sockets.len().max(1);
        configured.min(available).min(node_count.max(1))
    }

    /// find_node 多 socket 并发发送（核心并发逻辑）
    /// concurrent=1 时走单 socket 兼容路径，行为与改造前一致
    /// nodes 为 (节点, 选择层标签) 切片：层标签随 pending 注册进入分层反馈账目
    async fn send_find_node_concurrent(
        &self,
        nodes: &[(KBucketEntry, u8)],
        concurrent: usize,
        round_seq: u64,
    ) -> u64 {
        if concurrent <= 1 || nodes.len() <= 1 {
            let (socket_idx, socket) = self.next_send_socket();
            return self
                .send_find_node_to_socket(nodes, socket_idx, &socket, round_seq)
                .await;
        }

        // 选取 concurrent 个 socket（轮询）
        let base_idx = self.send_socket_idx.load(Ordering::Relaxed);
        let socket_indices: Vec<usize> = (0..concurrent)
            .map(|i| (base_idx + i) % self.sockets.len())
            .collect();
        self.send_socket_idx.store(
            (base_idx + concurrent) % self.sockets.len(),
            Ordering::Relaxed,
        );

        let per_group = nodes.len().div_ceil(concurrent);
        let sent_total = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        for (group_idx, &socket_idx) in socket_indices.iter().enumerate() {
            let start = group_idx * per_group;
            let end = (start + per_group).min(nodes.len());
            if start >= end {
                continue;
            }

            let group: Vec<(KBucketEntry, u8)> = nodes[start..end].to_vec();
            let socket = self.sockets[socket_idx].clone();
            let pending = self.pending.clone();
            let state = self.state.clone();
            let rate_limiter = self.rate_limiter.clone();
            let socket_send_total = self.socket_send_total.clone();
            let sent_counter = sent_total.clone();
            let node_id = self.node_id;

            handles.push(tokio::spawn(async move {
                let mut local_sent = 0u64;
                let num_targets = 16;
                let targets: Vec<[u8; 20]> = (0..num_targets)
                    .map(|_| {
                        let mut t = [0u8; 20];
                        rand::thread_rng().fill(&mut t);
                        t
                    })
                    .collect();
                let per_target = group.len().div_ceil(num_targets);

                for (i, (node, layer)) in group.iter().enumerate() {
                    // fix20：find_node 不再走 should_skip 降频 —— 响应率低触发
                    // throttled 时 get_peers/sample/find_node 共享 skip 周期，实测
                    // 总发送被砍半、find_node 净 5,700/h（目标 10,000/h）。find_node
                    // 峰值仅 128/30s≈15,360/h（8 socket ≈ 32/s/口），UDP 压力可控；
                    // rate_limiter 的 record_request 仍记录，防爆兜底保留。
                    let target_idx = (i / per_target).min(num_targets - 1);
                    let target = targets[target_idx];
                    let mut tid = rand::thread_rng().gen::<[u8; 4]>();
                    tid[0] = socket_idx as u8;

                    let msg = DhtMessage::build_find_node(&tid, &node_id, &target);

                    {
                        let shard = (tid[3] as usize) % 16;
                        let mut pending_map = pending[shard].write();
                        pending_map.insert(
                            tid.to_vec(),
                            PendingRequest {
                                _method: QueryMethod::FindNode,
                                target,
                                addr: node.addr,
                                sent_at: Instant::now(),
                                round_seq,
                                layer: *layer,
                            },
                        );
                    }

                    if socket.send_to(&msg, node.addr).await.is_ok() {
                        socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                        let mut s = state.write();
                        s.requests_sent += 1;
                        s.nodes_crawled += 1;
                        if let Some(rl) = &rate_limiter {
                            rl.record_request(socket_idx);
                        }
                        local_sent += 1;
                    }
                }
                sent_counter.fetch_add(local_sent, Ordering::Relaxed);
            }));
        }

        for h in handles {
            let _ = h.await;
        }

        sent_total.load(Ordering::Relaxed)
    }

    /// 单 socket find_node 发送（concurrent=1 兼容路径，行为与改造前一致）
    async fn send_find_node_to_socket(
        &self,
        nodes: &[(KBucketEntry, u8)],
        socket_idx: usize,
        socket: &UdpSocket,
        round_seq: u64,
    ) -> u64 {
        let num_targets = 16;
        let targets: Vec<[u8; 20]> = (0..num_targets).map(|_| self.random_target()).collect();
        let per_target = nodes.len().div_ceil(num_targets);
        let mut sent = 0u64;

        for (i, (node, layer)) in nodes.iter().enumerate() {
            let target_idx = (i / per_target).min(num_targets - 1);
            let target = targets[target_idx];
            let mut tid = rand::thread_rng().gen::<[u8; 4]>();
            tid[0] = socket_idx as u8;
            let msg = DhtMessage::build_find_node(&tid, &self.node_id, &target);

            {
                let shard = pending_shard(&tid);
                let mut pending = self.pending[shard].write();
                pending.insert(
                    tid.to_vec(),
                    PendingRequest {
                        _method: QueryMethod::FindNode,
                        target,
                        addr: node.addr,
                        sent_at: Instant::now(),
                        round_seq,
                        layer: *layer,
                    },
                );
            }

            if socket.send_to(&msg, node.addr).await.is_ok() {
                self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                let mut state = self.state.write();
                state.requests_sent += 1;
                state.nodes_crawled += 1;
                if let Some(rl) = &self.rate_limiter {
                    rl.record_request(socket_idx);
                }
                sent += 1;
            }
        }
        sent
    }

    /// get_peers 多 socket 并发发送
    /// concurrent=1 时走单 socket 兼容路径，行为与改造前一致
    async fn send_get_peers_concurrent(
        &self,
        nodes: &[KBucketEntry],
        concurrent: usize,
        infohashes: &[Infohash],
        round_seq: u64,
    ) -> u64 {
        if concurrent <= 1 || nodes.len() <= 1 {
            let (socket_idx, socket) = self.next_send_socket();
            return self
                .send_get_peers_to_socket(nodes, socket_idx, &socket, infohashes, round_seq)
                .await;
        }

        let base_idx = self.send_socket_idx.load(Ordering::Relaxed);
        let socket_indices: Vec<usize> = (0..concurrent)
            .map(|i| (base_idx + i) % self.sockets.len())
            .collect();
        self.send_socket_idx.store(
            (base_idx + concurrent) % self.sockets.len(),
            Ordering::Relaxed,
        );

        let per_group = nodes.len().div_ceil(concurrent);
        let sent_total = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        for (group_idx, &socket_idx) in socket_indices.iter().enumerate() {
            let start = group_idx * per_group;
            let end = (start + per_group).min(nodes.len());
            if start >= end {
                continue;
            }

            let group: Vec<KBucketEntry> = nodes[start..end].to_vec();
            let socket = self.sockets[socket_idx].clone();
            let pending = self.pending.clone();
            let state = self.state.clone();
            let rate_limiter = self.rate_limiter.clone();
            let socket_send_total = self.socket_send_total.clone();
            let sent_counter = sent_total.clone();
            let infohashes = infohashes.to_vec();
            let virtual_ids = self.virtual_node_ids.clone();

            handles.push(tokio::spawn(async move {
                let mut local_sent = 0u64;
                for node in &group {
                    // 2026-10 fix8：5 → 2。观测：128 节点 × 5 = 640 条/轮，10s 周期
                    // 发不完（实际 87s/轮），拖慢全部 Crawl 任务。降为 2 次/节点后
                    // 单轮 256 条，周期恢复 10s；values 采集当前恒 0，无实质损失。
                    for _ in 0..2 {
                        // 限速跳过检查：被限速的 socket 按 skip_ratio 降频跳过
                        if let Some(rl) = &rate_limiter {
                            if rl.should_skip(socket_idx) {
                                continue;
                            }
                        }
                        let idx = rand::random::<usize>() % infohashes.len();
                        let ih = infohashes[idx];
                        let mut tid = rand::random::<[u8; 4]>();
                        tid[0] = socket_idx as u8;
                        let vid_idx = rand::random::<usize>() % virtual_ids.len();
                        let vid = virtual_ids[vid_idx];
                        let msg = DhtMessage::build_get_peers(&tid, &vid, &ih);

                        {
                            let shard = (tid[3] as usize) % 16;
                            let mut pending_map = pending[shard].write();
                            pending_map.insert(
                                tid.to_vec(),
                                PendingRequest {
                                    _method: QueryMethod::GetPeers,
                                    target: ih,
                                    addr: node.addr,
                                    sent_at: Instant::now(),
                                    round_seq,
                                    layer: 0,
                                },
                            );
                        }

                        if socket.send_to(&msg, node.addr).await.is_ok() {
                            socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                            let mut s = state.write();
                            s.requests_sent += 1;
                            if let Some(rl) = &rate_limiter {
                                rl.record_request(socket_idx);
                            }
                            local_sent += 1;
                        }
                    }
                }
                sent_counter.fetch_add(local_sent, Ordering::Relaxed);
            }));
        }

        for h in handles {
            let _ = h.await;
        }

        sent_total.load(Ordering::Relaxed)
    }

    /// 单 socket get_peers 发送（concurrent=1 兼容路径）
    async fn send_get_peers_to_socket(
        &self,
        nodes: &[KBucketEntry],
        socket_idx: usize,
        socket: &UdpSocket,
        infohashes: &[Infohash],
        round_seq: u64,
    ) -> u64 {
        let mut sent = 0u64;
        for node in nodes {
            // 2026-10 fix8：5 → 2（同 send_get_peers_concurrent，控制单轮发送量）
            for _ in 0..2 {
                let idx = rand::random::<usize>() % infohashes.len();
                let ih = infohashes[idx];
                let mut tid = rand::random::<[u8; 4]>();
                tid[0] = socket_idx as u8;
                let vid = self.random_virtual_node_id();
                let msg = DhtMessage::build_get_peers(&tid, &vid, &ih);

                {
                    let shard = pending_shard(&tid);
                    let mut pending = self.pending[shard].write();
                    pending.insert(
                        tid.to_vec(),
                        PendingRequest {
                            _method: QueryMethod::GetPeers,
                            target: ih,
                            addr: node.addr,
                            sent_at: Instant::now(),
                            round_seq,
                            layer: 0,
                        },
                    );
                }

                if socket.send_to(&msg, node.addr).await.is_ok() {
                    self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                    let mut state = self.state.write();
                    state.requests_sent += 1;
                    if let Some(rl) = &self.rate_limiter {
                        rl.record_request(socket_idx);
                    }
                    sent += 1;
                }
            }
        }
        sent
    }

    /// sample_infohashes 多 socket 并发发送
    /// concurrent=1 时走单 socket 兼容路径，行为与改造前一致
    async fn send_sample_infohashes_concurrent(
        &self,
        nodes: &[KBucketEntry],
        concurrent: usize,
        round_seq: u64,
    ) -> u64 {
        if concurrent <= 1 || nodes.len() <= 1 {
            let (socket_idx, socket) = self.next_send_socket();
            return self
                .send_sample_to_socket(nodes, socket_idx, &socket, round_seq)
                .await;
        }

        let base_idx = self.send_socket_idx.load(Ordering::Relaxed);
        let socket_indices: Vec<usize> = (0..concurrent)
            .map(|i| (base_idx + i) % self.sockets.len())
            .collect();
        self.send_socket_idx.store(
            (base_idx + concurrent) % self.sockets.len(),
            Ordering::Relaxed,
        );

        let per_group = nodes.len().div_ceil(concurrent);
        let sent_total = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        for (group_idx, &socket_idx) in socket_indices.iter().enumerate() {
            let start = group_idx * per_group;
            let end = (start + per_group).min(nodes.len());
            if start >= end {
                continue;
            }

            let group: Vec<KBucketEntry> = nodes[start..end].to_vec();
            let socket = self.sockets[socket_idx].clone();
            let pending = self.pending.clone();
            let state = self.state.clone();
            let rate_limiter = self.rate_limiter.clone();
            let socket_send_total = self.socket_send_total.clone();
            let sent_counter = sent_total.clone();
            let virtual_ids = self.virtual_node_ids.clone();

            handles.push(tokio::spawn(async move {
                let mut local_sent = 0u64;
                for node in &group {
                    // 限速跳过检查：被限速的 socket 按 skip_ratio 降频跳过
                    if let Some(rl) = &rate_limiter {
                        if rl.should_skip(socket_idx) {
                            continue;
                        }
                    }
                    let mut tid = rand::thread_rng().gen::<[u8; 4]>();
                    tid[0] = socket_idx as u8;
                    let vid_idx = rand::random::<usize>() % virtual_ids.len();
                    let vid = virtual_ids[vid_idx];
                    let msg = DhtMessage::build_sample_infohashes(&tid, &vid);

                    {
                        let shard = (tid[3] as usize) % 16;
                        let mut pending_map = pending[shard].write();
                        pending_map.insert(
                            tid.to_vec(),
                            PendingRequest {
                                _method: QueryMethod::SampleInfohashes,
                                target: [0u8; 20],
                                addr: node.addr,
                                sent_at: Instant::now(),
                                round_seq,
                                layer: 0,
                            },
                        );
                    }

                    if socket.send_to(&msg, node.addr).await.is_ok() {
                        socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                        if let Some(rl) = &rate_limiter {
                            rl.record_request(socket_idx);
                        }
                        local_sent += 1;
                    }
                }
                let mut s = state.write();
                s.requests_sent += local_sent;
                sent_counter.fetch_add(local_sent, Ordering::Relaxed);
            }));
        }

        for h in handles {
            let _ = h.await;
        }

        sent_total.load(Ordering::Relaxed)
    }

    /// 单 socket sample_infohashes 发送（concurrent=1 兼容路径）
    async fn send_sample_to_socket(
        &self,
        nodes: &[KBucketEntry],
        socket_idx: usize,
        socket: &UdpSocket,
        round_seq: u64,
    ) -> u64 {
        let mut sent = 0u64;
        for node in nodes {
            let mut tid = rand::thread_rng().gen::<[u8; 4]>();
            tid[0] = socket_idx as u8;
            let vid = self.random_virtual_node_id();
            let msg = DhtMessage::build_sample_infohashes(&tid, &vid);

            {
                let shard = pending_shard(&tid);
                let mut pending = self.pending[shard].write();
                pending.insert(
                    tid.to_vec(),
                    PendingRequest {
                        _method: QueryMethod::SampleInfohashes,
                        target: [0u8; 20],
                        addr: node.addr,
                        sent_at: Instant::now(),
                        round_seq,
                        layer: 0,
                    },
                );
            }

            if socket.send_to(&msg, node.addr).await.is_ok() {
                self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                if let Some(rl) = &self.rate_limiter {
                    rl.record_request(socket_idx);
                }
                sent += 1;
            }
        }
        let mut state = self.state.write();
        state.requests_sent += sent;
        sent
    }

    /// 主动爬行：向已知节点发 find_node 探索网络
    pub async fn active_crawl(&self) {
        if !self.warmup_done.load(Ordering::Relaxed) {
            debug!("[crawler] 预热未完成，跳过全量爬行");
            return;
        }
        if self.sockets.is_empty() {
            return;
        }
        if self.is_paused() {
            debug!("[crawler] 暂停门置位，跳过主动爬行");
            return;
        }

        // 1. 获取自适应倍率（唯一的倍率决策点）
        let multiplier = self
            .adaptive_controller
            .as_ref()
            .map(|c| c.next_rate_multiplier())
            .unwrap_or(1.0);

        // 2. 计算发送节点数（基础128 × 倍率，clamp 到 [64, 512]）
        // fix21：128 → 192 —— 实测 crawl 周期被 get_peers/sample 排队拖到 35-40s、
        // 发送命中 ~85%，128/轮净 8.6-10.8k/h 波动不达 1 万目标。192/轮按 37s×85%
        // ≈ 15.8k/h，留 50% 余量稳定超线。select 毫秒级、发送 2-3ms，负担可忽略。
        let target_count = ((192.0_f64 * multiplier.max(0.5)).round() as usize).clamp(64, 512);

        // fix11 探针：分段耗时定位（select / 发送）
        let t0 = std::time::Instant::now();

        // 3. 选取节点（分层：L0 已验证 / L1 新鲜 / 探索预算受限）
        let tagged_nodes = if self.node_repo.is_some() {
            self.select_diverse_nodes_tagged(target_count, 3)
        } else {
            let known = self.known_nodes.write();
            if known.is_empty() {
                return;
            }
            let mut all = known.all_nodes();
            all.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            all.truncate(target_count.min(32));
            all.into_iter().map(|e| (e, 2u8)).collect()
        };
        if tagged_nodes.is_empty() {
            return;
        }
        info!(
            "[crawler][probe] crawl select 耗时 {}ms，选中 {}",
            t0.elapsed().as_millis(),
            tagged_nodes.len()
        );
        let mut layer_sent = [0u64; 3];
        for (_, layer) in &tagged_nodes {
            layer_sent[(*layer as usize).min(2)] += 1;
        }

        // 4. 记录 pending_before + 打开本轮反馈账目
        let round_seq = self.next_round_seq();
        self.open_round_feedback(round_seq, layer_sent);
        let pending_before = self.pending_total();

        // 5. 发送：paced 模式入队流式平滑发送（消费者按预算出队）；round 模式轮次突发直发
        let concurrent = self.resolve_concurrent_sockets(tagged_nodes.len());
        {
            let mut state = self.state.write();
            state.concurrent_sockets_in_use = if self.is_paced() {
                self.sockets.len()
            } else {
                concurrent
            };
        }
        let t_send = std::time::Instant::now();
        // 2026-10 fix2：paced 模式也直发（不再入队）。
        // 观测证据：paced 消费者在运行中偶发饿死/不消费（requests_sent 仅 ~2/s，
        // 远低于理论 38/s；分层账目"选中 255 响应 0"实为入队后未发送），
        // 入队≈不发送。直发受 concurrent 并发限制 + RateLimiter 节流保护。
        let _sent_total = self
            .send_find_node_concurrent(&tagged_nodes, concurrent, round_seq)
            .await;
        info!(
            "[crawler][probe] crawl 发送耗时 {}ms（select 后总计 {}ms）",
            t_send.elapsed().as_millis(),
            t0.elapsed().as_millis()
        );

        // 6. 记录 pending_after
        let pending_after = self.pending_total();

        // 7. 聚合上报所有未过期账目的增量（19号批次F）：
        // 响应到达延迟可达 ~2 分钟（数倍于 40s 轮周期），此前「只 drain 上一轮账目一次」
        // 使 >40s 才到达的认领永远不会进入控制器输入（限速器 70% vs 控制器 11% 的背离根因）。
        // 现在每轮聚合全部账目的自上次上报以来增量：响应无论多晚到达都会恰好计入一次。
        //
        // 2026-10-09：零响应熔断的观测点放在这里（**不放在
        // `if let Some(ac)` 内**）——熔断是止损兜底，不能依赖自适应控制器是否启用。
        {
            let avg_latency = self.current_latency_ema_ms();
            let (responded, timed_out, layer_sent, layer_resp) =
                self.drain_all_round_feedback_delta();
            let sent: u64 = layer_sent.iter().sum();
            debug!(
                "[crawler] active_crawl 轮次反馈: sent={} responded={} timed_out={}",
                sent, responded, timed_out
            );
            info!(
                "[crawler] 分层响应率: L0已验证 {}/{} L1新鲜 {}/{} 探索 {}/{}（300s窗口 响应增量/选中）",
                layer_resp[0],
                layer_sent[0],
                layer_resp[1],
                layer_sent[1],
                layer_resp[2],
                layer_sent[2]
            );
            // 熔断状态机推进（幂等：连续零响应达阈值 → Tripped）
            let prev_state = {
                let mut circuit = self.zero_response_circuit.lock().unwrap();
                let prev = circuit.state;
                // 2026-10-09：observe() 在healthy_rounds 达标时会**立即清零**，
                // 所以恢复日志若在 observe() 之后读该字段，永远打印「0/2」——
                // 运维会误判「熔断失效」并去调参。先取 observe 前的快照。
                let prev_healthy = circuit.healthy_rounds;
                circuit.observe(sent, responded);
                if circuit.state != prev {
                    if circuit.state == CircuitState::Tripped {
                        warn!(
                            "[crawler] 零响应熔断触发：连续 {}/{} 轮有发送但零响应（累计熔断 {} 次），\
                             暂停主动外发 {}s 后放行一轮探测",
                            circuit.zero_rounds,
                            zero_response_circuit::TRIGGER_ROUNDS,
                            circuit.trip_count,
                            zero_response_circuit::COOLDOWN_SECS
                        );
                    } else {
                        // 半开 → 空闲这一跃迁等价于「刚好攒满 RECOVER_ROUNDS 轮」。
                        let shown_healthy = if prev == CircuitState::HalfOpen {
                            zero_response_circuit::RECOVER_ROUNDS
                        } else {
                            prev_healthy
                        };
                        info!(
                            "[crawler] 零响应熔断恢复：探测收到响应（健康 {}/{} 轮），状态 {:?}",
                            shown_healthy,
                            zero_response_circuit::RECOVER_ROUNDS,
                            circuit.state
                        );
                    }
                }
                circuit.state
            };
            let _ = prev_state;
            if let Some(ac) = &self.adaptive_controller {
                ac.report_round_result(
                    sent,
                    responded,
                    avg_latency,
                    pending_before,
                    pending_after,
                    0,
                    1.0,
                );
            }
        }

        {
            let mut state = self.state.write();
            state.last_crawl_at = Some(Instant::now());
        }
    }

    /// 主动 get_peers：向高评分多样性节点发热门 infohash 的 get_peers
    /// 目的：1) 直接收集 peer  2) 增加我们节点在其他节点路由表中的出现概率  3) 间接增加被动收到查询的概率
    pub async fn active_get_peers(&self) {
        if !self.warmup_done.load(Ordering::Relaxed) {
            debug!("[crawler] 预热未完成，跳过主动 get_peers");
            return;
        }
        if self.sockets.is_empty() {
            return;
        }
        if self.is_paused() {
            debug!("[crawler] 暂停门置位，跳过主动 get_peers");
            return;
        }

        // 1. 获取自适应倍率
        let multiplier = self
            .adaptive_controller
            .as_ref()
            .map(|c| c.next_rate_multiplier())
            .unwrap_or(1.0);
        let target_count = ((128.0_f64 * multiplier.max(0.5)).round() as usize).clamp(64, 512);

        // fix11 探针：分段耗时定位（select / 发送）
        let t0 = std::time::Instant::now();

        // 2. 选取节点（2026-10 fix2：改分层选择，与 active_crawl 统一；L0 已验证优先）
        let tagged_nodes = if self.node_repo.is_some() {
            self.select_diverse_nodes_tagged(target_count, 4)
        } else {
            let known = self.known_nodes.read();
            if known.is_empty() {
                return;
            }
            let mut all = known.all_nodes();
            all.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            all.truncate(16);
            all.into_iter().map(|e| (e, 2u8)).collect()
        };
        let nodes: Vec<KBucketEntry> = tagged_nodes.into_iter().map(|(e, _)| e).collect();

        if nodes.is_empty() {
            return;
        }
        info!(
            "[crawler][probe] get_peers select 耗时 {}ms，选中 {}",
            t0.elapsed().as_millis(),
            nodes.len()
        );

        // 解析内置热门 infohash
        let mut infohashes: Vec<Infohash> = Vec::new();
        for hex_str in POPULAR_INFOHASHES {
            if let Ok(bytes) = hex::decode(hex_str) {
                if bytes.len() == 20 {
                    let mut ih = [0u8; 20];
                    ih.copy_from_slice(&bytes);
                    infohashes.push(ih);
                }
            }
        }

        // 也加入已收集的 infohash（最多5个）
        {
            let seen = self.seen_infohashes.read();
            for ih in seen.iter().take(5) {
                if !infohashes.contains(ih) {
                    infohashes.push(*ih);
                }
            }
        }

        if infohashes.is_empty() {
            return;
        }

        // 3. 打开本轮反馈账目
        let round_seq = self.next_round_seq();
        self.open_round_feedback(round_seq, [0, 0, 0]);

        // 4. 发送（每节点发5个 infohash 查询）：2026-10 fix2 统一直发
        //    （paced 入队已被观测证实在运行中可能不消费，见 active_crawl 注释）
        let concurrent = self.resolve_concurrent_sockets(nodes.len());
        {
            let mut state = self.state.write();
            state.concurrent_sockets_in_use = concurrent;
        }
        let t_send = std::time::Instant::now();
        let _ = self
            .send_get_peers_concurrent(&nodes, concurrent, &infohashes, round_seq)
            .await;
        info!(
            "[crawler][probe] get_peers 发送耗时 {}ms（select 后总计 {}ms）",
            t_send.elapsed().as_millis(),
            t0.elapsed().as_millis()
        );

        // 5. get_peers 不参与自适应决策（发送量小、反馈无代表性），仅清理本轮账目防泄漏
        let _ = self.take_round_feedback(round_seq);

        info!(
            "[crawler] 主动 get_peers: 向 {} 节点发送了 {} infohash 查询",
            nodes.len(),
            infohashes.len()
        );
    }

    /// 主动 sample_infohashes（BEP 51: DHT Infohash Indexing）
    /// 向高评分多样性节点发送 sample_infohashes 请求，批量获取它们已知的 infohash
    /// 这是主动发现新 infohash 的核心方法
    pub async fn active_sample_infohashes(&self) {
        if !self.warmup_done.load(Ordering::Relaxed) {
            debug!("[crawler] 预热未完成，跳过 sample_infohashes");
            return;
        }
        if self.sockets.is_empty() {
            return;
        }
        if self.is_paused() {
            debug!("[crawler] 暂停门置位，跳过 sample_infohashes");
            return;
        }

        // 1. 获取自适应倍率
        let multiplier = self
            .adaptive_controller
            .as_ref()
            .map(|c| c.next_rate_multiplier())
            .unwrap_or(1.0);
        let target_count = ((128.0_f64 * multiplier.max(0.5)).round() as usize).clamp(64, 512);

        // fix11 探针：分段耗时定位（select / 发送）
        let t0 = std::time::Instant::now();

        // 2. 选取节点（2026-10 fix2：改分层选择，与 active_crawl 统一；L0 已验证优先）
        let tagged_nodes = if self.node_repo.is_some() {
            self.select_diverse_nodes_tagged(target_count, 3)
        } else {
            let known = self.known_nodes.read();
            if known.is_empty() {
                return;
            }
            let mut all = known.all_nodes();
            all.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            all.truncate(32);
            all.into_iter().map(|e| (e, 2u8)).collect()
        };
        let nodes: Vec<KBucketEntry> = tagged_nodes.into_iter().map(|(e, _)| e).collect();

        if nodes.is_empty() {
            return;
        }
        info!(
            "[crawler][probe] sample select 耗时 {}ms，选中 {}",
            t0.elapsed().as_millis(),
            nodes.len()
        );

        // 3. 打开本轮反馈账目
        let round_seq = self.next_round_seq();
        self.open_round_feedback(round_seq, [0, 0, 0]);

        // 4. 发送：2026-10 fix2 统一直发（paced 入队已被观测证实在运行中可能不消费）
        let concurrent = self.resolve_concurrent_sockets(nodes.len());
        {
            let mut state = self.state.write();
            state.concurrent_sockets_in_use = concurrent;
        }
        let t_send = std::time::Instant::now();
        let _ = self
            .send_sample_infohashes_concurrent(&nodes, concurrent, round_seq)
            .await;
        info!(
            "[crawler][probe] sample 发送耗时 {}ms",
            t_send.elapsed().as_millis()
        );

        // 5. sample_infohashes 不参与自适应决策（发送量小、反馈无代表性），仅清理本轮账目防泄漏
        let _ = self.take_round_feedback(round_seq);

        info!(
            "[crawler] 主动 sample_infohashes: 向 {} 节点发送了请求",
            nodes.len()
        );
    }

    /// 主动 scrape（BEP 33: DHT Scrapes）
    /// 向高评分节点发送 scrape 请求，评估已知 infohash 的热度（seeder/leecher）
    pub async fn active_scrape(&self) {
        if !self.warmup_done.load(Ordering::Relaxed) {
            debug!("[crawler] 预热未完成，跳过 active_scrape");
            return;
        }
        if self.sockets.is_empty() {
            return;
        }
        if self.is_paused() {
            debug!("[crawler] 暂停门置位，跳过 active_scrape");
            return;
        }
        let (socket_idx, socket) = self.next_send_socket();
        // 获取要查询的 infohash（从 InfohashRepo 中取前 5 个）
        let infohashes = if let Some(repo) = &self.infohash_repo {
            let all = repo.all_sync();
            all.into_iter()
                .take(5)
                .map(|(ih, _)| ih)
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        if infohashes.is_empty() {
            return;
        }

        // 从 NodeRepo 选取高评分节点
        let nodes = if self.node_repo.is_some() {
            self.select_diverse_nodes(8, 2) // 选8个节点
        } else {
            return;
        };

        if nodes.is_empty() {
            return;
        }

        if self.is_paced() {
            // paced：入队流式平滑发送（scrape 量小，语义等价直发，统一走消费者限速）
            for node in &nodes {
                let ih = infohashes[rand::random::<usize>() % infohashes.len()];
                self.enqueue_send(SendWorkItem::Scrape {
                    addr: node.addr,
                    infohash: ih,
                    layer: 0,
                    round_seq: 0,
                });
            }
            return;
        }

        let mut requests_sent = 0;
        for node in &nodes {
            // 每个节点查询随机 1 个 infohash
            let ih = infohashes[rand::random::<usize>() % infohashes.len()];
            let mut tid = rand::thread_rng().gen::<[u8; 4]>();
            tid[0] = socket_idx as u8;
            let vid = self.random_virtual_node_id();
            let msg = DhtMessage::build_scrape(&tid, &vid, &ih);

            {
                let shard = pending_shard(&tid);
                let mut pending = self.pending[shard].write();
                pending.insert(
                    tid.to_vec(),
                    PendingRequest {
                        _method: QueryMethod::Scrape,
                        target: ih,
                        addr: node.addr,
                        sent_at: Instant::now(),
                        round_seq: 0,
                        layer: 0,
                    },
                );
            }

            if socket.send_to(&msg, node.addr).await.is_ok() {
                self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                if let Some(rl) = &self.rate_limiter {
                    rl.record_request(socket_idx);
                }
                requests_sent += 1;
            }
        }

        info!("[crawler] 主动 scrape: 向 {} 节点发送了请求", requests_sent);
    }

    /// 链式爬行：收到响应后，立即向新发现的节点发 find_node
    /// 形成链式反应，大幅提高节点发现速度
    async fn chain_crawl(
        &self,
        socket: &UdpSocket,
        socket_idx: usize,
        new_nodes: &[crate::discoverers::dht::message::DhtNode],
    ) {
        if new_nodes.is_empty() {
            return;
        }

        // 限制每次链式爬行最多发 8 个请求，避免风暴
        let max_chain = 8;

        for node in new_nodes.iter().take(max_chain) {
            if node.addr.port() == 0 {
                continue;
            }

            // 方向C：检查是否已经在 NodeRepo 里（无容量限制，避免重复请求）
            if let Some(repo) = &self.node_repo {
                if repo.contains_sync(node.addr) {
                    continue;
                }
            } else {
                // 兼容：没有 NodeRepo 时检查路由表
                let known = self.known_nodes.read();
                if known.find_by_addr(node.addr).is_some() {
                    continue;
                }
            }

            // 每个链式请求用不同随机 target，扩大覆盖面
            let chain_target = self.random_target();

            let mut tid = rand::thread_rng().gen::<[u8; 4]>();
            tid[0] = socket_idx as u8;
            let vid = self.random_virtual_node_id();
            let msg = DhtMessage::build_find_node(&tid, &vid, &chain_target);

            {
                let shard = pending_shard(&tid);
                let mut pending = self.pending[shard].write();
                pending.insert(
                    tid.to_vec(),
                    PendingRequest {
                        _method: QueryMethod::FindNode,
                        target: chain_target,
                        addr: node.addr,
                        sent_at: Instant::now(),
                        round_seq: 0,
                        layer: 0,
                    },
                );
            }

            if socket.send_to(&msg, node.addr).await.is_ok() {
                self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                let mut state = self.state.write();
                state.requests_sent += 1;
                if let Some(rl) = &self.rate_limiter {
                    rl.record_request(socket_idx);
                }
                state.nodes_crawled += 1;
            }
        }

        // 2026-10 fix3：实时邻居直接采集。
        // 观测结论：主动路径发往 hot 池（历史活跃节点为主）响应率趋近 0（L1 0/252），
        // 而链式爬行发往「刚响应过的节点返回的邻居」matched 持续增长——实时邻居活性
        // 远高于 hot 池。对其直接发 get_peers（内置热门 infohash）即可最短链路采集 peer，
        // 并借 get_peers 成功响应（claim 命中 → record_query_with_nodes_sync）为这些节点
        // 打上 last_verified → L0 池即时积累（打破「L0 空 → 只能发 hot 池死节点」死循环）。
        // 量级：每次响应 ≤8 节点 × 2 查询 = ≤16 个 get_peers，风暴可控。
        const MAX_CHAIN_GET_PEERS: usize = 8;
        const GET_PEERS_PER_NODE: usize = 2;
        // 2026-10 fix5：查询目标优先用「最近活跃 infohash」（score 活跃度排序）。
        // 内置 POPULAR 为 2010 年代镜像 infohash，国内 DHT 节点不跟踪 → get_peers
        // 响应 values 恒空（fix4 观测 5 条响应全 peers=0）；top_infohashes 反映最近
        // 被发现/验证的活跃 infohash，节点持有其 peers 的概率高。空/超时回退 POPULAR。
        let infohashes: Vec<Infohash> = {
            let mut ihs: Vec<Infohash> = Vec::new();
            if let Some(repo) = &self.infohash_repo {
                if let Ok(active) = tokio::time::timeout(
                    // [ALLOWED-HARDCODED: top_infohashes 查询保护性超时 50ms，防 repo 锁竞争阻塞发送循环；配置化收益低]
                    std::time::Duration::from_millis(50),
                    repo.top_infohashes(16),
                )
                .await
                {
                    ihs = active.into_iter().map(|(ih, _)| ih).collect();
                }
            }
            if ihs.is_empty() {
                for hex_str in POPULAR_INFOHASHES {
                    if let Ok(bytes) = hex::decode(hex_str) {
                        if bytes.len() == 20 {
                            let mut ih = [0u8; 20];
                            ih.copy_from_slice(&bytes);
                            ihs.push(ih);
                        }
                    }
                }
            }
            ihs
        };
        if infohashes.is_empty() {
            return;
        }
        // 2026-10-09 零响应熔断：连续多轮「有发送零响应」时停止链式直接采集。
        //
        // 生产实证（.52，2026-10-09 02:48~00:49）：三级响应率全零
        // （L0 0/82L1 0/903 探索 0/409）时，此处仍每 8 秒发 20 轮 get_peers 空包，
        // 累计 1001→1121 轮无一成功。空包并非「无害」：它们照样进pending 登记、
        // 照样等超时清理，写侧持续产出待落盘记录，把 Persistence 唯一槽位压死，
        // 最终引发全局停摆。止损要发生在**源头**，而不是在下游靠槽位抢救。
        if self
            .zero_response_circuit
            .lock()
            .unwrap()
            .should_skip_send()
        {
            debug!("[crawler] 链式直接采集：零响应熔断中，跳过本轮 get_peers 外发");
            return;
        }
        for node in new_nodes.iter().take(MAX_CHAIN_GET_PEERS) {
            if node.addr.port() == 0 {
                continue;
            }
            for _ in 0..GET_PEERS_PER_NODE {
                let ih = infohashes[rand::random::<usize>() % infohashes.len()];
                let mut tid = rand::thread_rng().gen::<[u8; 4]>();
                tid[0] = socket_idx as u8;
                let vid = self.random_virtual_node_id();
                let msg = DhtMessage::build_get_peers(&tid, &vid, &ih);

                {
                    let shard = pending_shard(&tid);
                    let mut pending = self.pending[shard].write();
                    pending.insert(
                        tid.to_vec(),
                        PendingRequest {
                            _method: QueryMethod::GetPeers,
                            target: ih,
                            addr: node.addr,
                            sent_at: Instant::now(),
                            round_seq: 0,
                            layer: 0,
                        },
                    );
                }

                // L1-⑪：链式采集硬节流 —— 每秒 get_peers 轮数上限 20（根治 10 万轮风暴：
                // 每响应 8-16 节点 × 每节点 DB 去重+写 ≈ 500 次/秒串行 DB 操作拖垮 SQLite
                // 单写锁与调度器；40-60 轮/秒不是"这点速率"，放大链路才是瓶颈）。
                {
                    static LAST_GP_SEND: std::sync::atomic::AtomicU64 =
                        std::sync::atomic::AtomicU64::new(0);
                    const GP_RATE_LIMIT_MS: u64 = 100; // 每秒 ≤10 轮（50ms 仍实测 112/s 超标，收紧一倍）
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let last = LAST_GP_SEND.load(Ordering::Relaxed);
                    let wait = GP_RATE_LIMIT_MS.saturating_sub(now_ms.saturating_sub(last));
                    if wait > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
                    }
                    LAST_GP_SEND.store(now_ms.saturating_add(wait), Ordering::Relaxed);
                }
                if socket.send_to(&msg, node.addr).await.is_ok() {
                    self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                    let mut state = self.state.write();
                    state.requests_sent += 1;
                    if let Some(rl) = &self.rate_limiter {
                        rl.record_request(socket_idx);
                    }
                }
            }
        }
        // 2026-10 fix4：链式直接采集发送观测（每 20 次采样一条）
        {
            static CHAIN_GP_LOG: AtomicU64 = AtomicU64::new(0);
            let n = CHAIN_GP_LOG.fetch_add(1, Ordering::Relaxed);
            if n.is_multiple_of(20) {
                info!(
                    "[crawler] 链式直接采集: 已对实时邻居累计发起 {} 轮 get_peers",
                    n + 1
                );
            }
        }
    }

    /// 刷新所有非空 bucket（Kademlia 标准 bucket 刷新）
    /// 对每个非空 bucket，用该 bucket 范围内的随机 ID 做 find_node
    pub async fn refresh_buckets(&self) {
        if !self.warmup_done.load(Ordering::Relaxed) {
            debug!("[crawler] 预热未完成，跳过 bucket 刷新");
            return;
        }
        if self.sockets.is_empty() {
            return;
        }
        let (socket_idx, socket) = self.next_send_socket();
        let bucket_targets = {
            let known = self.known_nodes.read();
            known.non_empty_bucket_targets()
        };

        if bucket_targets.is_empty() {
            return;
        }

        info!(
            "[crawler] 开始 bucket 刷新，共 {} 个非空 bucket",
            bucket_targets.len()
        );

        for target in bucket_targets {
            let nodes = {
                let known = self.known_nodes.read();
                let mut near = known.find_closest(&target, 4);
                near.truncate(4);
                near
            };

            for node in nodes {
                let mut tid = rand::thread_rng().gen::<[u8; 4]>();
                tid[0] = socket_idx as u8;
                let msg = DhtMessage::build_find_node(&tid, &self.node_id, &target);

                {
                    let shard = pending_shard(&tid);
                    let mut pending = self.pending[shard].write();
                    pending.insert(
                        tid.to_vec(),
                        PendingRequest {
                            _method: QueryMethod::FindNode,
                            target,
                            addr: node.addr,
                            sent_at: Instant::now(),
                            round_seq: 0,
                            layer: 0,
                        },
                    );
                }

                if socket.send_to(&msg, node.addr).await.is_ok() {
                    self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                    let mut state = self.state.write();
                    state.requests_sent += 1;
                    if let Some(rl) = &self.rate_limiter {
                        rl.record_request(socket_idx);
                    }
                }
            }
        }

        info!("[crawler] bucket 刷新完成");
    }

    /// 清理超时的 pending 请求，并记录失败统计到 NodeRepo
    pub fn cleanup_pending(&self) {
        // pending 超时由配置控制（默认 120s）：响应到达/处理延迟实测远超 15s，
        // 过短超时会在响应到达前清空 pending → claim 未命中 → responded 失真
        let timeout = Duration::from_secs(self.config.pending_timeout_secs);
        // 2026-10 fix2：单轮失败记录上限。观测：响应延迟可达 2 分钟+，超时堆积时
        // 单轮逐个 record_query_sync（每次一个写锁）会造成写锁风暴，曾致
        // cleanup_pending 任务在飞 457s 被强制回收、同期 API 8-20s 超时。
        // 移除（内存操作）仍全量进行；失败记录限流，余量留待下轮。
        // 2026-10 fix9：100 → 50、批次 50 → 20。fix7 已从 300 降到 100，但写锁竞争下
        // cleanup 仍卡 128s+（40 轮心跳观测）。单轮记录进一步减半、批次缩小，
        // 让 cleanup 在 spawn_blocking + 45s 超时下尽快结束，释放 Crawl 槽位。
        const MAX_FAILURE_RECORDS_PER_ROUND: usize = 50;

        // 跨 16 个分片收集超时请求的地址（去重），用于记录失败统计
        let mut expired_addrs: Vec<SocketAddr> = Vec::new();
        let mut seen_addrs: std::collections::HashSet<SocketAddr> =
            std::collections::HashSet::with_capacity(256);
        for shard in self.pending.iter() {
            let mut map = shard.write();
            for (_, req) in map
                .iter()
                .filter(|(_, req)| req.sent_at.elapsed() >= timeout)
            {
                if seen_addrs.insert(req.addr) {
                    expired_addrs.push(req.addr);
                }
            }
            map.retain(|_, req| req.sent_at.elapsed() < timeout);
        }

        // 记录失败统计到 NodeRepo（分批写，每批 20 减少单次持锁时长）
        if !expired_addrs.is_empty() {
            if let Some(repo) = &self.node_repo {
                let batch: Vec<SocketAddr> = expired_addrs
                    .iter()
                    .take(MAX_FAILURE_RECORDS_PER_ROUND)
                    .copied()
                    .collect();
                for chunk in batch.chunks(20) {
                    for addr in chunk {
                        repo.record_query_sync(*addr, false, 0);
                    }
                }
                if expired_addrs.len() > MAX_FAILURE_RECORDS_PER_ROUND {
                    debug!(
                        "[crawler] 超时请求 {} 个（本轮记录 {} 个失败，余量留待下轮）",
                        expired_addrs.len(),
                        batch.len()
                    );
                } else {
                    debug!(
                        "[crawler] 超时请求 {} 个，已记录失败统计",
                        expired_addrs.len()
                    );
                }
            }
        }
    }

    /// 处理收到的 DHT 响应消息（同步核心）
    ///
    /// 纯同步方法：完成消息解析、repo 更新、pending 清理，返回需要链式爬行的
    /// 节点分组（每组对应一次 chain_crawl）。由 `recv_loop` 在 `spawn_blocking` 中
    /// 调用，同步 repo 调用不占用 async worker 线程（8 socket 下避免阻塞累积）。
    fn handle_response_sync(
        &self,
        data: &[u8],
        socket_idx: usize,
        from: SocketAddr,
    ) -> Vec<Vec<DhtNode>> {
        let mut chain_groups: Vec<Vec<DhtNode>> = Vec::new();

        // 注意：响应率计数收敛到 claim_pending（tid 命中 pending 才计）。
        // 此处不再对每个入站数据报无条件计数——其他节点的未请求查询、
        // 迟到/伪造响应都不再进入响应率信号。
        // 2026-10 fix4：特化类型判定——get_peers/sample 响应不得由 find_node 分支吞掉。
        // parse_get_peers/sample 已严格化（分别要求 values|token / num|samples），但
        // parse_find_node_response 对任何 y=r 响应都会成功（nodes 可为空），若 find_node
        // 分支先处理并 return，get_peers 的 peers（values）与 sample 的 infohash 均被
        // 丢弃——此前 peers/infos 采集恒为 0 的直接根因。携带 values/token 或
        // num/samples 特征的响应放行给下方对应分支处理。
        let is_get_peers_resp = DhtMessage::response_has_field(data, b"values")
            || DhtMessage::response_has_field(data, b"token");
        let is_sample_resp = DhtMessage::response_has_field(data, b"num")
            || DhtMessage::response_has_field(data, b"samples");
        if !is_get_peers_resp && !is_sample_resp {
            // 尝试解析 find_node 响应
            if let Some((tid, nodes)) = DhtMessage::parse_find_node_response(data) {
                debug!(
                    "[crawler] 收到 find_node 响应 from {}, nodes={}",
                    from,
                    nodes.len()
                );

                // tid 认领：命中则计入响应率/轮次反馈并移除 pending，未命中计迟到
                let claimed = self.claim_pending(socket_idx, &tid);

                // 记录查询统计：命中记成功，迟到/伪造（tid 未命中）记失败——
                // 旧实现无条件记成功，迟到响应会把死节点刷成 Good+热池（2026-10 修复#3）
                match &claimed {
                    Some((latency_ms, _)) => {
                        if let Some(repo) = &self.node_repo {
                            repo.record_query_with_nodes_sync(
                                from,
                                *latency_ms,
                                nodes.len() as u64,
                            );
                        }
                    }
                    None => {
                        if let Some(repo) = &self.node_repo {
                            repo.record_query_sync(from, false, 0);
                        }
                    }
                }

                // 方向C：新节点加入 NodeRepo（无容量限制，主候选池），同时尝试加入路由表（DHT路由用）
                if !nodes.is_empty() {
                    // 过滤无效端口，批量处理以减少锁获取次数和 Merkle/Gossip 传播次数
                    let valid: Vec<([u8; 20], SocketAddr)> = nodes
                        .iter()
                        .filter(|n| n.addr.port() != 0)
                        .map(|n| (n.id, n.addr))
                        .collect();

                    let repo_added = if let Some(repo) = &self.node_repo {
                        repo.add_nodes_sync_batch(&valid)
                    } else {
                        0
                    };

                    // 批量加入路由表（一次写锁）
                    let rt_added = {
                        let mut known = self.known_nodes.write();
                        valid
                            .iter()
                            .filter(|(id, addr)| known.add_node(*id, *addr))
                            .count()
                    };

                    if repo_added > 0 || rt_added > 0 {
                        debug!("[crawler] 响应节点={} NodeRepo新增={} 路由表新增={} NodeRepo总计={} 路由表={}",
                        nodes.len(), repo_added, rt_added,
                        self.node_repo.as_ref().map(|r| r.len_sync()).unwrap_or(0),
                        self.known_nodes.read().len());
                    }

                    // 链式爬行节点：收集后由 async 层统一发送
                    chain_groups.push(nodes.clone());
                }

                // pending 已在 claim_pending 中移除
                // 注意：NodeRepo 持久化由定期任务（每5分钟）负责，不在每次响应后全量保存，避免大量磁盘 IO
                return chain_groups;
            }
        }

        // 尝试解析 get_peers 响应
        if let Some((tid, resp)) = DhtMessage::parse_get_peers_response(data) {
            // L1-⑨：响应日志降级 debug —— 线上实证 get_peers 风暴（累计 3.3 万轮/秒级数十条
            // 响应）把 stdout.log 打到 89MB、tracing 全局写锁被占满 → 独立 api runtime 的
            // HTTP handler 也被日志锁拖到超时。响应级观测移入采样（链式采集每 20 轮一条）。
            debug!(
                "[crawler] 收到 get_peers 响应 from {}, peers={}, nodes={}",
                from,
                resp.values.len(),
                resp.nodes.len()
            );

            // tid 认领：命中则计入响应率/轮次反馈并移除 pending，未命中计迟到
            let claimed = self.claim_pending(socket_idx, &tid);

            // 记录查询统计：命中记成功，迟到/伪造记失败（2026-10 修复#3，同 find_node 分支）
            match &claimed {
                Some((latency_ms, _)) => {
                    if let Some(repo) = &self.node_repo {
                        repo.record_query_with_nodes_sync(
                            from,
                            *latency_ms,
                            resp.nodes.len() as u64,
                        );
                    }
                }
                None => {
                    if let Some(repo) = &self.node_repo {
                        repo.record_query_sync(from, false, 0);
                    }
                }
            }

            // 存入 peer 缓存
            if !resp.values.is_empty() {
                if let Some(peer_repo) = &self.peer_repo {
                    // 从认领结果中取 infohash（原 pending target）
                    let ih = claimed.as_ref().map(|(_, target)| *target);

                    if let Some(infohash) = ih {
                        let peers: Vec<PeerInfo> = resp
                            .values
                            .iter()
                            .map(|addr| PeerInfo::new(*addr, PeerSource::Dht))
                            .collect();
                        peer_repo.add_peers_sync(&infohash, &peers);

                        {
                            let mut state = self.state.write();
                            state.peers_collected += peers.len() as u64;
                        }

                        // 发布事件
                        self.event_bus.publish(Event::PeerDiscovered {
                            infohash,
                            peers,
                            source: "dht-crawler".to_string(),
                        });
                    }
                }
            }

            // 加入新节点到路由表 + NodeRepo + 链式爬行
            if !resp.nodes.is_empty() {
                let new_nodes = resp.nodes.clone();
                // 批量加入路由表（一次写锁）
                {
                    let mut known = self.known_nodes.write();
                    for node in &resp.nodes {
                        if node.addr.port() != 0 {
                            known.add_node(node.id, node.addr);
                        }
                    }
                }
                // 批量同步到 NodeRepo（一次写锁 + 一次 Merkle/Gossip 传播）
                if let Some(repo) = &self.node_repo {
                    let valid: Vec<([u8; 20], SocketAddr)> = resp
                        .nodes
                        .iter()
                        .filter(|n| n.addr.port() != 0)
                        .map(|n| (n.id, n.addr))
                        .collect();
                    repo.add_nodes_sync_batch(&valid);
                }
                // 链式爬行节点：收集后由 async 层统一发送
                chain_groups.push(new_nodes);
                // 注意：NodeRepo 持久化由定期任务（每5分钟）负责，不在每次响应后全量保存，避免大量磁盘 IO
            }

            // 移除 pending
            {
                let shard = pending_shard(&tid);
                let mut pending = self.pending[shard].write();
                pending.remove(&tid.to_vec());
            }
        }

        // 尝试解析 sample_infohashes 响应（BEP 51: DHT Infohash Indexing）
        if let Some((tid, resp)) = DhtMessage::parse_sample_infohashes_response(data) {
            info!(
                "[crawler] 收到 sample_infohashes 响应 from {}, num={}, samples={}",
                from,
                resp.num,
                resp.samples.len()
            );

            // tid 认领：与 find_node/get_peers 统一——计入响应率/轮次反馈/matched/late，
            // 并原子移除 pending（旧实现不 claim：sample 响应既不进响应率信号，
            // 又把迟到响应无条件记为成功，污染评分与热池。2026-10 修复#3）
            let claimed = self.claim_pending(socket_idx, &tid);

            // 记录查询统计：命中记成功，迟到/伪造记失败
            match &claimed {
                Some((latency_ms, _)) => {
                    if let Some(repo) = &self.node_repo {
                        repo.record_query_with_nodes_sync(
                            from,
                            *latency_ms,
                            resp.samples.len() as u64,
                        );
                    }
                }
                None => {
                    if let Some(repo) = &self.node_repo {
                        repo.record_query_sync(from, false, 0);
                    }
                }
            }

            // 注册新发现的 infohash 到 InfohashRepo
            if !resp.samples.is_empty() {
                if let Some(repo) = &self.infohash_repo {
                    let mut new_count = 0;
                    for ih in &resp.samples {
                        let is_new = {
                            let mut seen = self.seen_infohashes.write();
                            // 内存上限：超过 crawler.max_infohashes 时清空旧去重集合，
                            // 防止无界 HashSet 持续膨胀（权威去重由 InfohashRepo 负责）
                            if seen.len() >= self.config.max_infohashes {
                                seen.clear();
                            }
                            seen.insert(*ih)
                        };
                        if is_new {
                            repo.register_sync(*ih, "dht_sample_infohashes");
                            new_count += 1;
                        }
                    }
                    if new_count > 0 {
                        info!(
                            "[crawler] sample_infohashes 新增 {} 个 infohash (来自 {})",
                            new_count, from
                        );
                    }
                }
            }

            // 移除 pending
            {
                let shard = pending_shard(&tid);
                let mut pending = self.pending[shard].write();
                pending.remove(&tid.to_vec());
            }
        }

        // 尝试解析 scrape 响应（BEP 33: DHT Scrapes）
        if let Some((tid, resp)) = DhtMessage::parse_scrape_response(data) {
            if !resp.files.is_empty() {
                for file in &resp.files {
                    info!(
                        "[crawler] scrape 响应 from {} ih={} complete={} incomplete={} downloaded={}",
                        from,
                        hex::encode(&file.infohash[..4]),
                        file.complete,
                        file.incomplete,
                        file.downloaded
                    );
                }
            }

            // 记录查询成功统计
            {
                let latency_ms = {
                    let pending = self.pending[pending_shard(&tid)].read();
                    pending
                        .get(&tid.to_vec())
                        .map(|r| r.sent_at.elapsed().as_millis() as u64)
                };
                if let Some(repo) = &self.node_repo {
                    repo.record_query_with_nodes_sync(
                        from,
                        latency_ms.unwrap_or(0),
                        resp.files.len() as u64,
                    );
                }
            }

            // 移除 pending
            {
                let shard = pending_shard(&tid);
                let mut pending = self.pending[shard].write();
                pending.remove(&tid.to_vec());
            }
        }

        chain_groups
    }

    /// 处理收到的 DHT 查询消息（被动响应，同步核心）
    ///
    /// 纯同步方法：解析查询、被动收集节点、构建响应字节，返回响应产物。
    /// 由 `recv_loop` 在 `spawn_blocking` 中调用；响应发送与 get_peers 回发回到 async 层。
    fn handle_query_sync(
        &self,
        data: &[u8],
        socket_idx: usize,
        from: SocketAddr,
    ) -> Option<QuerySyncResult> {
        let (tid, method, infohash, announce_port) = DhtMessage::parse_query(data)?;

        // 统计入站请求（被动打洞指标：其他节点主动连接我们的次数）
        {
            let mut state = self.state.write();
            state.inbound_total += 1;
            match method {
                QueryMethod::Ping => state.inbound_ping += 1,
                QueryMethod::FindNode => state.inbound_find_node += 1,
                QueryMethod::GetPeers => state.inbound_get_peers += 1,
                QueryMethod::AnnouncePeer => state.inbound_announce_peer += 1,
                QueryMethod::SampleInfohashes => state.inbound_sample_infohashes += 1,
                QueryMethod::Scrape => {}
            }
        }

        // 入站来源节点统计（被动打洞效果分析）— 有界集合，超上限时清空防止内存泄漏
        {
            let mut sources = self.inbound_sources.write();
            if sources.len() >= self.config.inbound_sources_max {
                sources.clear();
                tracing::debug!(
                    "[crawler] inbound_sources 达到上限 {}，已清空",
                    self.config.inbound_sources_max
                );
            }
            sources.insert(from);
            let mut state = self.state.write();
            state.inbound_unique_sources = sources.len();
        }

        // 被动收集节点：任何发送 DHT 查询的节点都是 DHT 节点，加入路由表 + NodeRepo
        if let Some(node_id) = DhtMessage::extract_query_node_id(data) {
            let mut known = self.known_nodes.write();
            if known.add_node(node_id, from) {
                debug!(
                    "[crawler] 被动收集节点: {} (id={})",
                    from,
                    hex::encode(&node_id[..4])
                );
            }
            // 同步到 NodeRepo（统一数据归口）
            if let Some(repo) = &self.node_repo {
                repo.add_node_sync(node_id, from);
            }
        }

        // 构建响应字节（同步），发送动作回到 async 层
        let (resp_bytes, followup_get_peers, discovered_infohash) = match method {
            QueryMethod::Ping => {
                let nid = self.socket_node_id(socket_idx);
                let resp = DhtMessage::build_ping_response(&tid, &nid);
                (resp, None, None)
            }
            QueryMethod::FindNode => {
                // 完善响应：返回路由表中最接近目标的 K 个节点
                let target = infohash.unwrap_or([0u8; 20]);
                let closest_nodes: Vec<DhtNode> = {
                    let known = self.known_nodes.read();
                    known
                        .find_closest(&target, 8)
                        .into_iter()
                        .map(|e| DhtNode {
                            id: e.id,
                            addr: e.addr,
                        })
                        .collect()
                };
                let nid = self.socket_node_id(socket_idx);
                let resp =
                    DhtMessage::build_find_node_response_with_nodes(&tid, &nid, &closest_nodes);
                (resp, None, None)
            }
            QueryMethod::GetPeers => {
                let token = rand::thread_rng().gen::<[u8; 4]>();
                // 完善响应：如果 PeerRepo 中有这个 infohash 的 peer，返回它们
                let peers_for_ih: Vec<SocketAddr> = if let Some(ih) = infohash {
                    if let Some(peer_repo) = &self.peer_repo {
                        peer_repo
                            .get_peers_sync(&ih, 20)
                            .into_iter()
                            .map(|p| p.addr)
                            .collect()
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                };
                let closest_nodes: Vec<DhtNode> = {
                    let known = self.known_nodes.read();
                    known
                        .find_closest(&infohash.unwrap_or([0u8; 20]), 8)
                        .into_iter()
                        .map(|e| DhtNode {
                            id: e.id,
                            addr: e.addr,
                        })
                        .collect()
                };
                let nid = self.socket_node_id(socket_idx);
                let resp = DhtMessage::build_get_peers_response_full(
                    &tid,
                    &nid,
                    &token,
                    &peers_for_ih,
                    &closest_nodes,
                );

                // 同时主动发 get_peers 回去，收集这个 infohash 的 peer（async 层执行）
                (resp, infohash, infohash)
            }
            QueryMethod::AnnouncePeer => {
                let nid = self.socket_node_id(socket_idx);
                let resp = DhtMessage::build_ping_response(&tid, &nid);

                if let Some(ih) = infohash {
                    info!(
                        "[crawler] 收到 announce_peer from {} (ih={:?})",
                        from,
                        &ih[..4]
                    );
                    // BEP 5: announce_peer 的来源地址本身就是该 infohash 的一个 peer。
                    // 端口优先取 a.port（BT 下载端口），implied_port=1/缺省时回退来源 UDP 端口。
                    let port = announce_port.unwrap_or(from.port());
                    let peer_addr = SocketAddr::new(from.ip(), port);
                    if let Some(peer_repo) = &self.peer_repo {
                        let peers = vec![PeerInfo::new(peer_addr, PeerSource::Dht)];
                        peer_repo.add_peers_sync(&ih, &peers);
                        let mut state = self.state.write();
                        state.peers_collected += peers.len() as u64;
                        self.event_bus.publish(Event::PeerDiscovered {
                            infohash: ih,
                            peers,
                            source: "dht-announce".to_string(),
                        });
                    }
                }
                (resp, None, infohash)
            }
            QueryMethod::SampleInfohashes => {
                // BEP 51: 返回我们已知的 infohash 样本
                let all_infohashes = if let Some(repo) = &self.infohash_repo {
                    repo.all_sync().into_iter().map(|(ih, _)| ih).collect()
                } else {
                    vec![]
                };

                // 随机采样最多 20 个 infohash（避免包过大）
                let mut samples: Vec<Infohash> = all_infohashes.to_vec();
                if samples.len() > 20 {
                    use rand::seq::SliceRandom;
                    let mut rng = rand::thread_rng();
                    samples.shuffle(&mut rng);
                    samples.truncate(20);
                }

                let nid = self.socket_node_id(socket_idx);
                let resp = DhtMessage::build_sample_infohashes_response(
                    &tid,
                    &nid,
                    all_infohashes.len() as i64,
                    &samples,
                );
                (resp, None, None)
            }
            QueryMethod::Scrape => {
                // BEP 33: 返回空的 scrape 响应（当前不维护 seeder/leecher 统计）
                // 响应格式: d1:rd2:id20:<node_id>5:filesde1:t2:<tid>1:y1:re
                let mut resp = Vec::new();
                resp.extend_from_slice(b"d1:rd2:id20:");
                let nid = self.socket_node_id(socket_idx);
                resp.extend_from_slice(&nid);
                resp.extend_from_slice(b"5:filesde1:t2:");
                resp.extend_from_slice(&tid);
                resp.extend_from_slice(b"1:y1:re");
                (resp, None, None)
            }
        };

        Some(QuerySyncResult {
            resp_bytes,
            followup_get_peers,
            discovered_infohash,
        })
    }

    /// 向指定节点主动发 get_peers 查询
    async fn query_get_peers(
        &self,
        socket: &UdpSocket,
        socket_idx: usize,
        addr: SocketAddr,
        infohash: Infohash,
    ) {
        let mut tid = rand::thread_rng().gen::<[u8; 4]>();
        tid[0] = socket_idx as u8;
        let msg = DhtMessage::build_get_peers(&tid, &self.node_id, &infohash);

        {
            let shard = pending_shard(&tid);
            let mut pending = self.pending[shard].write();
            pending.insert(
                tid.to_vec(),
                PendingRequest {
                    _method: QueryMethod::GetPeers,
                    target: infohash,
                    addr,
                    sent_at: Instant::now(),
                    round_seq: 0,
                    layer: 0,
                },
            );
        }

        {
            let __nw = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let __l = GP_SEND_LAST.load(Ordering::Relaxed);
            let __w = GP_SEND_INTERVAL_MS.saturating_sub(__nw.saturating_sub(__l));
            if __w > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(__w)).await;
            }
            GP_SEND_LAST.store(__nw.saturating_add(__w), Ordering::Relaxed);
        }
        {
            let __nw = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let __l = GP_SEND_LAST.load(Ordering::Relaxed);
            let __w = GP_SEND_INTERVAL_MS.saturating_sub(__nw.saturating_sub(__l));
            if __w > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(__w)).await;
            }
            GP_SEND_LAST.store(__nw.saturating_add(__w), Ordering::Relaxed);
        }
        if socket.send_to(&msg, addr).await.is_ok() {
            self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
            let mut state = self.state.write();
            state.requests_sent += 1;
            if let Some(rl) = &self.rate_limiter {
                rl.record_request(socket_idx);
            }
            debug!(
                "[crawler] 主动 get_peers -> {} (ih={})",
                addr,
                hex::encode(infohash)
            );
        }
    }

    /// 爬行循环：多 socket 并行收包
    async fn crawl_loop(&self) {
        info!(
            "[crawler] DHT 爬虫引擎启动（主动模式），socket_count={}",
            self.sockets.len()
        );

        // 如果未注入 socket，回退创建单 socket（兼容旧调用）
        let sockets: Vec<Arc<UdpSocket>> = if self.sockets.is_empty() {
            match create_udp_socket(SocketAddr::from(([0, 0, 0, 0], self.config.listen_port))).await
            {
                Ok(s) => vec![Arc::new(s)],
                Err(e) => {
                    warn!("[crawler] 绑定端口 {} 失败: {}", self.config.listen_port, e);
                    let mut state = self.state.write();
                    state.running = false;
                    state.errors += 1;
                    return;
                }
            }
        } else {
            self.sockets.clone()
        };

        // 加入 DHT 网络（只用第一个 socket）
        self.load_routing_table().await;
        let bootstrap_count = self.bootstrap().await;
        info!(
            "[crawler] 向 {} 个 bootstrap 地址发送了 find_node",
            bootstrap_count
        );

        // 每个 socket 独立接收循环
        let mut handles = Vec::new();
        for (idx, socket) in sockets.iter().enumerate() {
            let engine = self.clone_for_async();
            let socket = socket.clone();
            handles.push(tokio::spawn(async move {
                engine.recv_loop(idx, socket).await;
            }));
        }

        // 等待 shutdown
        self.shutdown.notified().await;
        info!(
            "[crawler] 爬虫引擎收到停止信号号，等待 {} 个 recv_loop 退出",
            handles.len()
        );

        for h in handles {
            let _ = h.await;
        }

        info!("[crawler] 爬虫引擎已停止");
    }

    /// 单个 socket 的接收循环
    async fn recv_loop(&self, socket_idx: usize, socket: Arc<UdpSocket>) {
        let mut buf = self.buffer_pool.acquire();
        buf.resize(65536, 0);
        info!("[crawler] recv_loop #{} 已启动", socket_idx);

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => {
                    info!("[crawler] recv_loop #{} 收到停止信号号", socket_idx);
                    break;
                }
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((n, from)) => {
                            let data = &buf[..n];

                            // 更新消息计数 + PPS 接收计数
                            {
                                self.socket_recv_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                                let mut state = self.state.write();
                                state.messages_received += 1;
                            }

                            // 限制并发消息处理数：同步 repo 调用已移到 spawn_blocking 线程池，
                            // Semaphore 仅限制 blocking 任务并发，避免一次性占满 blocking 线程池。
                            let _msg_permit = match self.message_semaphore.acquire().await {
                                Ok(p) => p,
                                Err(_) => continue, // semaphore 已关闭，跳过
                            };

                            let preview = String::from_utf8_lossy(&data[..std::cmp::min(n, 60)]);
                            debug!("[crawler] recv_loop #{} 收到 {} 字节 from {}: {}", socket_idx, n, from, preview);

                            // 同步处理（解析 + repo 更新 + 响应字节构建）放到 blocking 线程池，
                            // 不占用 async worker 线程；多 socket 下消息量翻倍时避免同步 _sync()
                            // 调用持锁阻塞 worker 导致 API 饥饿超时。
                            let data_vec = data.to_vec();
                            let engine = self.clone_for_async();
                            let processed = tokio::task::spawn_blocking(move || {
                                let chain_groups = engine.handle_response_sync(&data_vec, socket_idx, from);
                                let query_out = engine.handle_query_sync(&data_vec, socket_idx, from);
                                (chain_groups, query_out)
                            })
                            .await;

                            match processed {
                                Ok((chain_groups, query_out)) => {
                                    // 2026-10 fix2：未识别/error 消息可观测
                                    // （响应/查询解析均未命中 → error 响应或非 DHT 数据；采样日志防刷屏）
                                    if chain_groups.is_empty() && query_out.is_none() {
                                        let n = UNPARSED_LOG_COUNTER.fetch_add(1, Ordering::Relaxed);
                                        if n.is_multiple_of(200) {
                                            info!(
                                                "[crawler] 未识别/error 消息累计 {} 条（采样 from {}）",
                                                n + 1,
                                                from
                                            );
                                        }
                                    }
                                    // 链式爬行（异步发送，回到 async worker）
                                    for nodes in chain_groups {
                                        if !nodes.is_empty() {
                                            self.chain_crawl(&socket, socket_idx, &nodes).await;
                                        }
                                    }

                                    if let Some(q) = query_out {
                                        // 异步发送查询响应字节
                                        let _ = socket.send_to(&q.resp_bytes, from).await;

                                        // GetPeers 分支：主动回发 get_peers 收集 peer（注册 pending + 发送）
                                        if let Some(ih) = q.followup_get_peers {
                                            self.query_get_peers(&socket, socket_idx, from, ih).await;
                                        }

                                        // 新发现 infohash：seen 去重 + 事件发布（与原逻辑一致）
                                        if let Some(infohash) = q.discovered_infohash {
                                            let is_new = {
                                                let mut seen = self.seen_infohashes.write();
                                                // 内存上限：超过 crawler.max_infohashes 时清空旧去重集合
                                                if seen.len() >= self.config.max_infohashes {
                                                    seen.clear();
                                                }
                                                seen.insert(infohash)
                                            };

                                            if is_new {
                                                let hex_ih = hex::encode(infohash);
                                                let ih_log_count = INFOHASH_LOG_COUNTER.fetch_add(1, Ordering::Relaxed);
                                                if ih_log_count.is_multiple_of(100) {
                                                    info!("[crawler] 发现新 infohash #{}: {} (来自 {})", ih_log_count, hex_ih, from);
                                                }

                                                // 同步到 InfohashRepo（统一数据归口）
                                                if let Some(repo) = &self.infohash_repo {
                                                    repo.register_sync(infohash, "dht-crawler");
                                                }

                                                {
                                                    let mut state = self.state.write();
                                                    state.infohashes_collected += 1;
                                                }

                                                self.event_bus.publish(Event::InfohashSeen {
                                                    infohash,
                                                    source: "dht-crawler".to_string(),
                                                    seen_at: std::time::SystemTime::now(),
                                                });

                                                // 定期发布爬行进度
                                                let state = self.state.read();
                                                if state.infohashes_collected.is_multiple_of(10) {
                                                    let known_nodes = self.node_repo.as_ref().map(|r| r.len_sync()).unwrap_or_else(|| self.known_nodes.read().len());
                                                    self.event_bus.publish(Event::CrawlProgress {
                                                        nodes_crawled: state.nodes_crawled,
                                                        infohashes_collected: state.infohashes_collected,
                                                        peers_collected: state.peers_collected,
                                                        message: format!(
                                                            "已收集 {} infohash, {} peer, {} 已知节点",
                                                            state.infohashes_collected,
                                                            state.peers_collected,
                                                            known_nodes
                                                        ),
                                                    });

                                                    // 进度日志按 30 秒间隔采样，避免高频刷屏（事件仍每次发布）
                                                    let now_ms = std::time::SystemTime::now()
                                                        .duration_since(std::time::UNIX_EPOCH)
                                                        .map(|d| d.as_millis() as u64)
                                                        .unwrap_or(0);
                                                    let last_log = LAST_PROGRESS_LOG_MS.load(Ordering::Relaxed);
                                                    if now_ms.saturating_sub(last_log) >= 30_000 {
                                                        LAST_PROGRESS_LOG_MS.store(now_ms, Ordering::Relaxed);
                                                        info!(
                                                            "[crawler] 爬行进度: 已收集 {} infohash, {} peer, {} 已知节点",
                                                            state.infohashes_collected,
                                                            state.peers_collected,
                                                            known_nodes
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    debug!("[crawler] recv_loop #{} 消息处理任务失败: {}", socket_idx, e);
                                    let mut state = self.state.write();
                                    state.errors += 1;
                                }
                            }
                        }
                        Err(e) => {
                            debug!("[crawler] recv_loop #{} 接收消息失败: {}", socket_idx, e);
                            let mut state = self.state.write();
                            state.errors += 1;
                        }
                    }
                }
            }
        }
    }

    /// 节点活跃度维护：向 top 20 高评分节点发送 ping
    ///
    /// 目的：保持我们的节点在其他节点路由表中的活跃度，
    /// 让其他节点更频繁地向我们发送请求（被动打洞正循环）。
    pub async fn active_keepalive(&self) {
        if !self.warmup_done.load(Ordering::Relaxed) {
            debug!("[crawler] 预热未完成，跳过 keepalive");
            return;
        }
        if self.sockets.is_empty() {
            return;
        }
        let (socket_idx, socket) = self.next_send_socket();
        let top_nodes: Vec<std::net::SocketAddr> = {
            let table = self.known_nodes.read();
            table
                .top_nodes_by_score(20)
                .into_iter()
                .map(|n| n.addr)
                .collect()
        };
        if top_nodes.is_empty() {
            return;
        }
        let mut sent = 0;
        for addr in &top_nodes {
            let tid = rand::random::<[u8; 4]>();
            let msg = DhtMessage::build_ping(&tid, &self.node_id);
            {
                let __nw = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let __l = GP_SEND_LAST.load(Ordering::Relaxed);
                let __w = GP_SEND_INTERVAL_MS.saturating_sub(__nw.saturating_sub(__l));
                if __w > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(__w)).await;
                }
                GP_SEND_LAST.store(__nw.saturating_add(__w), Ordering::Relaxed);
            }
            {
                let __nw = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let __l = GP_SEND_LAST.load(Ordering::Relaxed);
                let __w = GP_SEND_INTERVAL_MS.saturating_sub(__nw.saturating_sub(__l));
                if __w > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(__w)).await;
                }
                GP_SEND_LAST.store(__nw.saturating_add(__w), Ordering::Relaxed);
            }
            if socket.send_to(&msg, addr).await.is_ok() {
                self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
                // keepalive 成功发送计入限速器分母（19号 D1.3：其响应经 claim 计分子，
                // 分母缺失会让响应率虚高）
                if let Some(rl) = &self.rate_limiter {
                    rl.record_request(socket_idx);
                }
                sent += 1;
            }
        }
        {
            let mut state = self.state.write();
            state.requests_sent += sent;
        }
        debug!(
            "[crawler] 活跃度维护: 向 {}/{} 个高评分节点发送了 ping",
            sent,
            top_nodes.len()
        );
    }
}

#[async_trait]
impl Crawler for CrawlerEngine {
    fn name(&self) -> &str {
        "dht-crawler"
    }

    async fn start(&self) -> anyhow::Result<()> {
        if !self.config.enabled {
            warn!("[crawler] 爬虫未启用（config.crawler.enabled = false）");
            return Ok(());
        }

        {
            let mut state = self.state.write();
            if state.running {
                warn!("[crawler] 爬虫已在运行");
                return Ok(());
            }
            state.running = true;
            state.started_at = Some(Instant::now());
        }

        // 路由表加载：使用 NodeRepo 内存中已预加载的节点（不再全量从 SQLite 加载）
        self.load_routing_table().await;

        let engine = self.clone_for_async();
        tokio::spawn(async move {
            engine.crawl_loop().await;
        });

        // 流式平滑发送（19号 D3）：engine 自有常驻任务，不经 TaskScheduler
        // （调度器对每次执行有 300s 硬超时，会周期性杀死常驻任务；
        //   与 crawl_loop/recv_loop 的既有先例同级）
        if self.config.send_mode == "paced" {
            self.init_paced_channel(PACED_QUEUE_CAPACITY);
            self.paced_mode.store(1, Ordering::Relaxed);
            let engine = self.clone_for_async();
            tokio::spawn(async move {
                engine.paced_send_loop().await;
            });
            info!(
                "[crawler] paced 流式发送已启用：tick={}ms 配额={}/socket/tick 容量={}",
                self.config.paced_tick_ms,
                self.config.paced_max_per_socket_per_tick,
                PACED_QUEUE_CAPACITY
            );
        } else {
            info!("[crawler] 发送模式 = round（轮次突发，旧行为逃生通道）");
        }

        info!("[crawler] 爬虫引擎已启动（主动模式）");
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        {
            let mut state = self.state.write();
            if !state.running {
                return Ok(());
            }
            state.running = false;
        }
        self.shutdown.notify_waiters();
        info!("[crawler] 爬虫引擎停止信号已发送");
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.state.read().running
    }

    fn state(&self) -> CrawlerState {
        self.state()
    }
}

impl CrawlerEngine {
    /// 克隆一个可用于 async 任务的引用
    fn clone_for_async(&self) -> CrawlerEngine {
        CrawlerEngine {
            config: self.config.clone(),
            state: self.state.clone(),
            event_bus: self.event_bus.clone(),
            shutdown: self.shutdown.clone(),
            seen_infohashes: self.seen_infohashes.clone(),
            inbound_sources: self.inbound_sources.clone(),
            known_nodes: self.known_nodes.clone(),
            pending: self.pending.clone(),
            peer_repo: self.peer_repo.clone(),
            storage: self.storage.clone(),
            node_repo: self.node_repo.clone(),
            infohash_repo: self.infohash_repo.clone(),
            node_id: self.node_id,
            virtual_node_ids: self.virtual_node_ids.clone(),
            dns_pool: self.dns_pool.clone(),
            pause_gate: self.pause_gate.clone(),
            sockets: self.sockets.clone(),
            send_socket_idx: self.send_socket_idx.clone(),
            throttled_round_robin: self.throttled_round_robin.clone(),
            rate_limiter: self.rate_limiter.clone(),
            adaptive_controller: self.adaptive_controller.clone(),
            buffer_pool: self.buffer_pool.clone(),
            warmup_done: self.warmup_done.clone(),
            // 熔断状态必须共享：否则各 async 克隆体各持一份独立状态，
            // 熔断判定会被稀释（每份都只看到自己那几轮的观测），
            // 退化成"几乎永不熔断"——正是要根治的失败模式。
            zero_response_circuit: self.zero_response_circuit.clone(),
            message_semaphore: self.message_semaphore.clone(),
            socket_send_total: self.socket_send_total.clone(),
            socket_recv_total: self.socket_recv_total.clone(),
            pps_last: self.pps_last.clone(),
            round_seq: self.round_seq.clone(),
            round_feedback: self.round_feedback.clone(),
            latency_ema_ms: self.latency_ema_ms.clone(),
            send_tx: self.send_tx.clone(),
            send_rx: self.send_rx.clone(),
            send_queue_len: self.send_queue_len.clone(),
            enqueue_dropped_total: self.enqueue_dropped_total.clone(),
            paced_mode: self.paced_mode.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // 2026-10-09 零响应熔断（回归 .52 生产事故：三级响应率全零时空转 2.5h）
    // -----------------------------------------------------------------------

    /// 正常有响应时永不熔断。
    #[test]
    fn test_circuit_stays_idle_with_responses() {
        let mut c = ZeroResponseCircuit::default();
        for _ in 0..50 {
            assert_eq!(c.observe(100, 5), CircuitState::Idle);
        }
        assert!(!c.should_skip_send());
        assert_eq!(c.trip_count, 0);
    }

    /// 核心回归：连续零响应达阈值必须熔断并停止外发。
    #[test]
    fn test_circuit_trips_on_sustained_zero_response() {
        let mut c = ZeroResponseCircuit::default();
        for i in 0..zero_response_circuit::TRIGGER_ROUNDS - 1 {
            assert_eq!(
                c.observe(100, 0),
                CircuitState::Idle,
                "第 {} 轮不应过早熔断",
                i + 1
            );
            assert!(!c.should_skip_send());
        }
        assert_eq!(c.observe(100, 0), CircuitState::Tripped);
        assert!(
            c.should_skip_send(),
            "熔断后必须停止主动外发，否则空包持续压死持久化槽位"
        );
        assert_eq!(c.trip_count, 1);
    }

    /// 低流量不判定：样本不足时不得熔断（避免误杀正常低频爬取）。
    #[test]
    fn test_circuit_ignores_low_sample() {
        let mut c = ZeroResponseCircuit::default();
        for _ in 0..20 {
            assert_eq!(
                c.observe(zero_response_circuit::MIN_SENT_FOR_JUDGEMENT - 1, 0),
                CircuitState::Idle
            );
        }
        assert!(!c.should_skip_send());
    }

    /// 冷却 → 半开 → 探测成功 → 完全恢复的完整路径。
    #[test]
    fn test_circuit_recovers_after_cooldown_and_probe() {
        let mut c = ZeroResponseCircuit::default();
        for _ in 0..zero_response_circuit::TRIGGER_ROUNDS {
            c.observe(100, 0);
        }
        assert_eq!(c.state, CircuitState::Tripped);

        // 冷却未到 ⇒ 仍熔断
        assert_eq!(c.observe(100, 0), CircuitState::Tripped);

        // 模拟冷却已过（回拨 tripped_at）
        c.tripped_at =
            Some(Instant::now() - Duration::from_secs(zero_response_circuit::COOLDOWN_SECS + 1));
        // 迁移轮（Tripped → HalfOpen）的响应计入 healthy=1；
        // 半开再成功一轮⇒ 攒满 RECOVER_ROUNDS ⇒ 完全恢复。
        assert_eq!(c.observe(100, 3), CircuitState::HalfOpen);
        assert!(!c.should_skip_send(), "半开应放行一轮探测，否则无法恢复");
        assert_eq!(c.observe(100, 3), CircuitState::Idle);
        assert!(!c.should_skip_send());
        assert_eq!(c.trip_count, 1, "恢复不得增加熔断次数");
    }

    /// 半开探测仍零响应 ⇒ 重新熔断（不得因为冷却到期就盲目放行）。
    #[test]
    fn test_circuit_retrips_on_failed_probe() {
        let mut c = ZeroResponseCircuit::default();
        for _ in 0..zero_response_circuit::TRIGGER_ROUNDS {
            c.observe(100, 0);
        }
        c.tripped_at =
            Some(Instant::now() - Duration::from_secs(zero_response_circuit::COOLDOWN_SECS + 1));
        let _ = c.observe(100, 0);
        assert_eq!(c.state, CircuitState::HalfOpen);
        // 探测仍零响应
        assert_eq!(c.observe(100, 0), CircuitState::Tripped);
        assert!(c.should_skip_send());
        assert_eq!(c.trip_count, 2, "应累计两次熔断");
    }

    /// 回归（2026-10-09）：熔断恢复日志必须显示真实攒满的健康轮数。
    ///
    /// 缺陷背景：`healthy_rounds` 在 `observe()` 内达标时会被**立即清零**，
    /// 而恢复日志在其后读取，于是永远打印「健康 0/2 轮，状态 Idle」，
    /// 运维据此会误判「熔断形同虚设」并去调参。修复后半开→空闲的跃迁
    /// 应显示 RECOVER_ROUNDS。
    #[test]
    fn test_recovery_log_reports_full_healthy_rounds() {
        let mut c = ZeroResponseCircuit::default();
        for _ in 0..zero_response_circuit::TRIGGER_ROUNDS {
            c.observe(100, 0);
        }
        assert_eq!(c.state, CircuitState::Tripped);
        c.tripped_at =
            Some(Instant::now() - Duration::from_secs(zero_response_circuit::COOLDOWN_SECS + 1));

        // 半开首轮：healthy_rounds=1，仍是 HalfOpen
        let prev = c.state;
        let prev_healthy = c.healthy_rounds;
        assert_eq!(c.observe(100, 3), CircuitState::HalfOpen);
        assert_eq!(prev, CircuitState::Tripped);
        assert_eq!(prev_healthy, 0, "迁移轮前 healthy 应为 0");

        // 半开次轮：攒满 ⇒ Idle，但 healthy_rounds 已被 observe 清零
        let prev = c.state;
        let prev_healthy = c.healthy_rounds;
        assert_eq!(c.observe(100, 3), CircuitState::Idle);
        assert_eq!(prev, CircuitState::HalfOpen);
        assert_eq!(prev_healthy, 1);
        assert_eq!(
            c.healthy_rounds, 0,
            "observe() 达标后会清零，这正是原日志打印 0/2 的根因"
        );
        // 日志应展示 prev_healthy(1) + 本轮1 = RECOVER_ROUNDS，而非清零后的 0
        assert_eq!(prev_healthy + 1, zero_response_circuit::RECOVER_ROUNDS);
    }

    /// 分片均匀性（19号 D2）：pending_shard 按 tid[3]%16 路由，随机 tid 均匀落 16 片；
    /// socket_idx 编码在字节 0，不参与分片
    #[test]
    fn test_pending_shard_uniform() {
        let mut counts = [0usize; 16];
        for i in 0..1600u32 {
            let mut tid = [0u8; 4];
            tid[3] = ((i.wrapping_mul(2654435761) >> 8) % 256) as u8;
            counts[pending_shard(&tid)] += 1;
        }
        for c in counts {
            assert!((60..=140).contains(&c), "分片应大致均匀，实测 {:?}", counts);
        }
        let mut shards = std::collections::HashSet::new();
        for b in 0..=255u8 {
            let tid = [3u8, 0, 0, b];
            shards.insert(pending_shard(&tid));
        }
        assert!(
            shards.len() >= 14,
            "byte3 变化应覆盖多数分片，实测 {}",
            shards.len()
        );
    }

    #[test]
    fn test_crawler_state_default() {
        let state = CrawlerState::default();
        assert!(!state.running);
        assert_eq!(state.nodes_crawled, 0);
        assert_eq!(state.infohashes_collected, 0);
    }

    /// paced 预算数学（19号 D3）：配额 x socket 数 x 倍率，min 1
    #[test]
    fn test_paced_budget_math() {
        assert_eq!(CrawlerEngine::paced_budget(8, 8, 1.0), 64);
        assert_eq!(CrawlerEngine::paced_budget(8, 8, 0.2), 13);
        assert_eq!(CrawlerEngine::paced_budget(8, 4, 2.0), 64);
        assert_eq!(
            CrawlerEngine::paced_budget(8, 0, 1.0),
            8,
            "sockets=0 按 1 参与（实际发送由 socket 缺失保护）"
        );
        assert_eq!(
            CrawlerEngine::paced_budget(1, 1, 0.05),
            1,
            "极小倍率兜底 min 1"
        );
    }

    /// paced 入队（19号 D3）：通道容量上限 + 溢出丢弃计数 + 队列长度守恒
    #[tokio::test]
    async fn test_paced_enqueue_overflow() {
        let engine = CrawlerEngine::new(CrawlerConfig::default(), EventBus::default());
        assert!(!engine.is_paced(), "start() 前不处于 paced 模式");
        engine.init_paced_channel(2);
        let addr: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let mk = || SendWorkItem::Sample {
            addr,
            layer: 0,
            round_seq: 1,
        };
        assert!(engine.enqueue_send(mk()));
        assert!(engine.enqueue_send(mk()));
        assert!(!engine.enqueue_send(mk()), "队列满应返回 false");
        assert_eq!(engine.enqueue_dropped_total.load(Ordering::Relaxed), 1);
        assert_eq!(engine.send_queue_len.load(Ordering::Relaxed), 2);
        // round 模式（未 init 通道）enqueue 直接拒绝
        let engine2 = CrawlerEngine::new(CrawlerConfig::default(), EventBus::default());
        assert!(!engine2.enqueue_send(mk()));
    }

    #[test]
    fn test_crawler_engine_creation() {
        let config = CrawlerConfig::default();
        let bus = EventBus::default();
        let engine = CrawlerEngine::new(config, bus);
        assert_eq!(engine.name(), "dht-crawler");
        assert!(!engine.is_running());
    }

    #[tokio::test]
    async fn test_crawler_start_disabled() {
        let config = CrawlerConfig {
            enabled: false,
            ..Default::default()
        };
        let bus = EventBus::default();
        let engine = CrawlerEngine::new(config, bus);
        let result = engine.start().await;
        assert!(result.is_ok());
        assert!(!engine.is_running());
    }

    /// 分层账目聚合上报（19号批次F 修复）：迟到响应跨轮计入恰好一次
    #[tokio::test]
    async fn test_round_feedback_layer_accounting() {
        let config = CrawlerConfig::default();
        let bus = EventBus::default();
        let engine = CrawlerEngine::new(config, bus);

        // 轮 N：打开账目 L0=5/L1=3/探索=2
        let seq_n = engine.next_round_seq();
        engine.open_round_feedback(seq_n, [5, 3, 2]);
        // 轮 N+1 聚合（响应尚未到达）：零增量，账目保留
        let (r0, t0, sent, resp0) = engine.drain_all_round_feedback_delta();
        assert_eq!((r0, t0), (0, 0));
        assert!(sent.iter().sum::<u64>() >= 5);
        assert_eq!(resp0, [0, 0, 0]);

        // 轮 N+2 前夕：轮 N 的两个迟到响应到达（L0=1、探索=1）
        {
            let mut map = engine.round_feedback.lock().unwrap();
            let acc = map.get_mut(&seq_n).unwrap();
            acc.responded += 2;
            acc.layer_responded[0] += 1;
            acc.layer_responded[2] += 1;
        }
        // 聚合必须包含迟到的 2 个响应（旧实现 drain 一次后即丢失）
        let (r1, _t1, _s, resp1) = engine.drain_all_round_feedback_delta();
        assert_eq!(r1, 2, "迟到响应必须被聚合上报");
        assert_eq!(resp1, [1, 0, 1]);
        // 再次聚合：零增量（水位推进，恰好计入一次）
        let (r2, _, _s, resp2) = engine.drain_all_round_feedback_delta();
        assert_eq!((r2, resp2), (0, [0, 0, 0]));
    }

    /// 多账目并存（连续两轮 open）：聚合覆盖全部账目且互不重复
    #[tokio::test]
    async fn test_round_feedback_multi_account_aggregate() {
        let config = CrawlerConfig::default();
        let bus = EventBus::default();
        let engine = CrawlerEngine::new(config, bus);

        let seq_a = engine.next_round_seq();
        engine.open_round_feedback(seq_a, [10, 0, 0]);
        let seq_b = engine.next_round_seq();
        engine.open_round_feedback(seq_b, [0, 8, 0]);

        {
            let mut map = engine.round_feedback.lock().unwrap();
            map.get_mut(&seq_a).unwrap().responded += 3;
            map.get_mut(&seq_a).unwrap().layer_responded[0] += 3;
            map.get_mut(&seq_b).unwrap().responded += 4;
            map.get_mut(&seq_b).unwrap().layer_responded[1] += 4;
        }
        let (r, _t, s, resp) = engine.drain_all_round_feedback_delta();
        assert_eq!(r, 7, "两账目增量求和");
        assert_eq!(resp, [3, 4, 0]);
        assert_eq!(
            (s[0], s[1], s[2]),
            (10, 8, 0),
            "layer_sent 为窗口内全部账目之和"
        );
    }
}
