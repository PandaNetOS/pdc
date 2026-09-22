//! 爬虫引擎实现
//!
//! 主动 + 被动混合 DHT 爬虫：
//! 1. 加入 DHT 网络（通过 bootstrap 节点发 find_node 获取初始路由）
//! 2. 主动爬行：定期向已知节点发 find_node，探索 DHT 网络，收集节点
//! 3. 被动监听：其他节点发来的 get_peers / announce_peer 中提取 infohash
//! 4. 对发现的 infohash 主动发 get_peers，收集 peer 并存入缓存

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::RwLock;
use rand::Rng;
use tokio::net::UdpSocket;
use tracing::{debug, info, warn};

use crate::config::CrawlerConfig;
use crate::dht::kbucket::KBucketEntry;
use crate::dht::routing_table::RoutingTable;
use crate::discoverers::dht::message::{DhtMessage, DhtNode, QueryMethod};
use crate::event_bus::EventBus;
use crate::net::socket_opts::create_udp_socket;
use crate::storage::PeerRepoImpl;
use crate::types::{Event, Infohash, PeerInfo, PeerSource};

use super::buffer_pool::CrawlerBufferPool;
use super::rate_limiter::RateLimiter;
use crate::intelligence::AdaptiveController;

use super::Crawler;

/// pending 请求超时（P2 优化：缩短以加速节点轮换）
const PENDING_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

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
}

/// pending 请求分片：16 个 RwLock<HashMap>，按 tid[0] % 16 路由
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
    (tid[0] as usize) % 16
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
    /// 消息处理并发限制（避免多 recv_loop 同时阻塞 worker 线程导致 API 饥饿）
    message_semaphore: Arc<tokio::sync::Semaphore>,
    /// 每 socket 累计发送数（用于计算 PPS，无锁原子计数，Arc 共享）
    socket_send_total: Arc<Vec<AtomicU64>>,
    /// 每 socket 累计接收数（用于计算 PPS，无锁原子计数，Arc 共享）
    socket_recv_total: Arc<Vec<AtomicU64>>,
    /// 上次统计 PPS 的时间和累计值（Mutex 保护，Arc 共享）
    pps_last: Arc<Mutex<PpsSnapshot>>,
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
            message_semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrent)),
            socket_send_total: Arc::new((0..16).map(|_| AtomicU64::new(0)).collect()),
            socket_recv_total: Arc::new((0..16).map(|_| AtomicU64::new(0)).collect()),
            pps_last: Arc::new(Mutex::new(None)),
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
            self.rate_limiter = Some(Arc::new(RateLimiter::new(
                self.sockets.len(),
                true,
                std::time::Duration::from_secs(self.config.rate_limit_window_secs),
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
        // UDP 丢包估算：1 - (messages_received / requests_sent)，粗略估算
        if state.requests_sent > 0 {
            state.udp_packet_loss_estimate =
                1.0 - (state.messages_received as f64 / state.requests_sent as f64);
        } else {
            state.udp_packet_loss_estimate = 0.0;
        }
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
        let repo = match &self.node_repo {
            Some(r) => r,
            None => return Vec::new(),
        };
        // 统一调用 SelectSystem（内部优化：不全量克隆，只在最后返回时克隆）
        crate::intelligence::SelectSystem::select_diverse_nodes(
            repo.as_ref(),
            count,
            max_per_subnet,
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
        let mut tid = rand::thread_rng().gen::<[u8; 2]>();
        tid[0] = (tid[0] & 0x0F) | ((socket_idx as u8 & 0x0F) << 4);
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
                },
            );
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

    /// 估算平均延迟（毫秒）：基于 pending 表中请求的平均等待时间
    fn estimate_avg_latency_ms(&self) -> f64 {
        let now = Instant::now();
        let mut total_ms = 0f64;
        let mut count = 0usize;
        for shard in self.pending.iter() {
            for req in shard.read().values() {
                total_ms += now.duration_since(req.sent_at).as_secs_f64() * 1000.0;
                count += 1;
            }
        }
        if count > 0 {
            total_ms / count as f64
        } else {
            100.0
        }
    }

    /// 估算本轮响应数：基于 rate_limiter 全局响应率
    fn estimate_responded(&self, sent: u64) -> u64 {
        if let Some(rl) = &self.rate_limiter {
            let rate = rl.global_response_rate();
            (sent as f64 * rate).round() as u64
        } else {
            (sent as f64 * 0.3).round() as u64
        }
    }

    /// 解析实际并发 socket 数（clamp 到 [1, min(config.concurrent_sockets, sockets.len(), 8)]）
    fn resolve_concurrent_sockets(&self, node_count: usize) -> usize {
        let configured = self.config.concurrent_sockets.clamp(1, 8);
        let available = self.sockets.len().max(1);
        configured.min(available).min(node_count.max(1))
    }

    /// find_node 多 socket 并发发送（核心并发逻辑）
    /// concurrent=1 时走单 socket 兼容路径，行为与改造前一致
    async fn send_find_node_concurrent(&self, nodes: &[KBucketEntry], concurrent: usize) -> u64 {
        if concurrent <= 1 || nodes.len() <= 1 {
            let (socket_idx, socket) = self.next_send_socket();
            return self
                .send_find_node_to_socket(nodes, socket_idx, &socket)
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

            let group: Vec<KBucketEntry> = nodes[start..end].to_vec();
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

                for (i, node) in group.iter().enumerate() {
                    let target_idx = (i / per_target).min(num_targets - 1);
                    let target = targets[target_idx];
                    let mut tid = rand::thread_rng().gen::<[u8; 2]>();
                    tid[0] = (tid[0] & 0x0F) | ((socket_idx as u8 & 0x0F) << 4);

                    let msg = DhtMessage::build_find_node(&tid, &node_id, &target);

                    {
                        let shard = (tid[0] as usize) % 16;
                        let mut pending_map = pending[shard].write();
                        pending_map.insert(
                            tid.to_vec(),
                            PendingRequest {
                                _method: QueryMethod::FindNode,
                                target,
                                addr: node.addr,
                                sent_at: Instant::now(),
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
        nodes: &[KBucketEntry],
        socket_idx: usize,
        socket: &UdpSocket,
    ) -> u64 {
        let num_targets = 16;
        let targets: Vec<[u8; 20]> = (0..num_targets).map(|_| self.random_target()).collect();
        let per_target = nodes.len().div_ceil(num_targets);
        let mut sent = 0u64;

        for (i, node) in nodes.iter().enumerate() {
            let target_idx = (i / per_target).min(num_targets - 1);
            let target = targets[target_idx];
            let mut tid = rand::thread_rng().gen::<[u8; 2]>();
            tid[0] = (tid[0] & 0x0F) | ((socket_idx as u8 & 0x0F) << 4);
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
    ) -> u64 {
        if concurrent <= 1 || nodes.len() <= 1 {
            let (socket_idx, socket) = self.next_send_socket();
            return self
                .send_get_peers_to_socket(nodes, socket_idx, &socket, infohashes)
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
                    for _ in 0..5 {
                        let idx = rand::random::<usize>() % infohashes.len();
                        let ih = infohashes[idx];
                        let mut tid = rand::random::<[u8; 2]>();
                        tid[0] = (tid[0] & 0x0F) | ((socket_idx as u8 & 0x0F) << 4);
                        let vid_idx = rand::random::<usize>() % virtual_ids.len();
                        let vid = virtual_ids[vid_idx];
                        let msg = DhtMessage::build_get_peers(&tid, &vid, &ih);

                        {
                            let shard = (tid[0] as usize) % 16;
                            let mut pending_map = pending[shard].write();
                            pending_map.insert(
                                tid.to_vec(),
                                PendingRequest {
                                    _method: QueryMethod::GetPeers,
                                    target: ih,
                                    addr: node.addr,
                                    sent_at: Instant::now(),
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
    ) -> u64 {
        let mut sent = 0u64;
        for node in nodes {
            for _ in 0..5 {
                let idx = rand::random::<usize>() % infohashes.len();
                let ih = infohashes[idx];
                let mut tid = rand::random::<[u8; 2]>();
                tid[0] = (tid[0] & 0x0F) | ((socket_idx as u8 & 0x0F) << 4);
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
    ) -> u64 {
        if concurrent <= 1 || nodes.len() <= 1 {
            let (socket_idx, socket) = self.next_send_socket();
            return self.send_sample_to_socket(nodes, socket_idx, &socket).await;
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
            let socket_send_total = self.socket_send_total.clone();
            let sent_counter = sent_total.clone();
            let virtual_ids = self.virtual_node_ids.clone();

            handles.push(tokio::spawn(async move {
                let mut local_sent = 0u64;
                for node in &group {
                    let mut tid = rand::thread_rng().gen::<[u8; 2]>();
                    tid[0] = (tid[0] & 0x0F) | ((socket_idx as u8 & 0x0F) << 4);
                    let vid_idx = rand::random::<usize>() % virtual_ids.len();
                    let vid = virtual_ids[vid_idx];
                    let msg = DhtMessage::build_sample_infohashes(&tid, &vid);

                    {
                        let shard = (tid[0] as usize) % 16;
                        let mut pending_map = pending[shard].write();
                        pending_map.insert(
                            tid.to_vec(),
                            PendingRequest {
                                _method: QueryMethod::SampleInfohashes,
                                target: [0u8; 20],
                                addr: node.addr,
                                sent_at: Instant::now(),
                            },
                        );
                    }

                    if socket.send_to(&msg, node.addr).await.is_ok() {
                        socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
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
    ) -> u64 {
        let mut sent = 0u64;
        for node in nodes {
            let mut tid = rand::thread_rng().gen::<[u8; 2]>();
            tid[0] = (tid[0] & 0x0F) | ((socket_idx as u8 & 0x0F) << 4);
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
                    },
                );
            }

            if socket.send_to(&msg, node.addr).await.is_ok() {
                self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
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

        // 1. 获取自适应倍率（唯一的倍率决策点）
        let multiplier = self
            .adaptive_controller
            .as_ref()
            .map(|c| c.next_rate_multiplier())
            .unwrap_or(1.0);

        // 2. 计算发送节点数（基础64 × 倍率，clamp 到 [8, 256]）
        let target_count = ((64.0_f64 * multiplier).round() as usize).clamp(8, 256);

        // 3. 选取节点
        let nodes = if self.node_repo.is_some() {
            self.select_diverse_nodes(target_count, 3)
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
            all
        };
        if nodes.is_empty() {
            return;
        }

        // 4. 记录 pending_before
        let pending_before = self.pending_total();

        // 5. 多 socket 并发发送
        let concurrent = self.resolve_concurrent_sockets(nodes.len());
        {
            let mut state = self.state.write();
            state.concurrent_sockets_in_use = concurrent;
        }
        let sent_total = self.send_find_node_concurrent(&nodes, concurrent).await;

        // 6. 记录 pending_after
        let pending_after = self.pending_total();

        // 7. 反馈本轮结果
        if let Some(ac) = &self.adaptive_controller {
            let avg_latency = self.estimate_avg_latency_ms();
            let responded = self.estimate_responded(sent_total);
            ac.report_round_result(
                sent_total,
                responded,
                avg_latency,
                pending_before,
                pending_after,
                0,
                1.0,
            );
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

        // 1. 获取自适应倍率
        let multiplier = self
            .adaptive_controller
            .as_ref()
            .map(|c| c.next_rate_multiplier())
            .unwrap_or(1.0);
        let target_count = ((64.0_f64 * multiplier).round() as usize).clamp(8, 256);

        // 2. 选取节点
        let nodes = if self.node_repo.is_some() {
            self.select_diverse_nodes(target_count, 4)
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
            all
        };

        if nodes.is_empty() {
            return;
        }

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

        // 3. 记录 pending_before
        let pending_before = self.pending_total();

        // 4. 多 socket 并发发送（每节点发5个 infohash 查询）
        let concurrent = self.resolve_concurrent_sockets(nodes.len());
        {
            let mut state = self.state.write();
            state.concurrent_sockets_in_use = concurrent;
        }
        let sent_total = self
            .send_get_peers_concurrent(&nodes, concurrent, &infohashes)
            .await;

        // 5. 记录 pending_after + 反馈
        let pending_after = self.pending_total();
        if let Some(ac) = &self.adaptive_controller {
            let avg_latency = self.estimate_avg_latency_ms();
            let responded = self.estimate_responded(sent_total);
            ac.report_round_result(
                sent_total,
                responded,
                avg_latency,
                pending_before,
                pending_after,
                0,
                1.0,
            );
        }

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

        // 1. 获取自适应倍率
        let multiplier = self
            .adaptive_controller
            .as_ref()
            .map(|c| c.next_rate_multiplier())
            .unwrap_or(1.0);
        let target_count = ((64.0_f64 * multiplier).round() as usize).clamp(8, 256);

        // 2. 选取节点
        let nodes = if self.node_repo.is_some() {
            self.select_diverse_nodes(target_count, 3)
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
            all
        };

        if nodes.is_empty() {
            return;
        }

        // 3. 记录 pending_before
        let pending_before = self.pending_total();

        // 4. 多 socket 并发发送
        let concurrent = self.resolve_concurrent_sockets(nodes.len());
        {
            let mut state = self.state.write();
            state.concurrent_sockets_in_use = concurrent;
        }
        let sent_total = self
            .send_sample_infohashes_concurrent(&nodes, concurrent)
            .await;

        // 5. 记录 pending_after + 反馈
        let pending_after = self.pending_total();
        if let Some(ac) = &self.adaptive_controller {
            let avg_latency = self.estimate_avg_latency_ms();
            let responded = self.estimate_responded(sent_total);
            ac.report_round_result(
                sent_total,
                responded,
                avg_latency,
                pending_before,
                pending_after,
                0,
                1.0,
            );
        }

        info!(
            "[crawler] 主动 sample_infohashes: 向 {} 节点发送了请求",
            sent_total
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

        let mut requests_sent = 0;
        for node in &nodes {
            // 每个节点查询随机 1 个 infohash
            let ih = infohashes[rand::random::<usize>() % infohashes.len()];
            let mut tid = rand::thread_rng().gen::<[u8; 2]>();
            tid[0] = (tid[0] & 0x0F) | ((socket_idx as u8 & 0x0F) << 4);
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
                    },
                );
            }

            if socket.send_to(&msg, node.addr).await.is_ok() {
                self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
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

            let mut tid = rand::thread_rng().gen::<[u8; 2]>();
            tid[0] = (tid[0] & 0x0F) | ((socket_idx as u8 & 0x0F) << 4);
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
                let mut tid = rand::thread_rng().gen::<[u8; 2]>();
                tid[0] = (tid[0] & 0x0F) | ((socket_idx as u8 & 0x0F) << 4);
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
        let timeout = PENDING_REQUEST_TIMEOUT;

        // 跨 16 个分片收集超时请求的地址，用于记录失败统计
        let mut expired_addrs: Vec<SocketAddr> = Vec::new();
        for shard in self.pending.iter() {
            let mut map = shard.write();
            expired_addrs.extend(
                map.iter()
                    .filter(|(_, req)| req.sent_at.elapsed() >= timeout)
                    .map(|(_, req)| req.addr),
            );
            map.retain(|_, req| req.sent_at.elapsed() < timeout);
        }

        // 记录失败统计到 NodeRepo
        if !expired_addrs.is_empty() {
            if let Some(repo) = &self.node_repo {
                for addr in &expired_addrs {
                    repo.record_query_sync(*addr, false, 0);
                }
            }
            debug!(
                "[crawler] 超时请求 {} 个，已记录失败统计",
                expired_addrs.len()
            );
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

        if let Some(rl) = &self.rate_limiter {
            rl.record_response(socket_idx);
        }
        // 尝试解析 find_node 响应
        if let Some((tid, nodes)) = DhtMessage::parse_find_node_response(data) {
            debug!(
                "[crawler] 收到 find_node 响应 from {}, nodes={}",
                from,
                nodes.len()
            );

            // 记录查询成功统计（响应来源节点 → NodeRepo，含返回节点数）
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
                        nodes.len() as u64,
                    );
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

            // 移除 pending
            {
                let shard = pending_shard(&tid);
                let mut pending = self.pending[shard].write();
                pending.remove(&tid.to_vec());
            }
            // 注意：NodeRepo 持久化由定期任务（每5分钟）负责，不在每次响应后全量保存，避免大量磁盘 IO
            return chain_groups;
        }

        // 尝试解析 get_peers 响应
        if let Some((tid, resp)) = DhtMessage::parse_get_peers_response(data) {
            info!(
                "[crawler] 收到 get_peers 响应 from {}, peers={}, nodes={}",
                from,
                resp.values.len(),
                resp.nodes.len()
            );

            // 记录查询成功统计（响应来源节点 → NodeRepo，含返回节点数）
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
                        resp.nodes.len() as u64,
                    );
                }
            }

            // 存入 peer 缓存
            if !resp.values.is_empty() {
                if let Some(peer_repo) = &self.peer_repo {
                    // 从 pending 中找 infohash
                    let ih = {
                        let pending = self.pending[pending_shard(&tid)].read();
                        pending.get(&tid.to_vec()).map(|r| r.target)
                    };

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
                        resp.samples.len() as u64,
                    );
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
        let (tid, method, infohash) = DhtMessage::parse_query(data)?;

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
        let mut tid = rand::thread_rng().gen::<[u8; 2]>();
        tid[0] = (tid[0] & 0x0F) | ((socket_idx as u8 & 0x0F) << 4);
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
                },
            );
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
            let tid = rand::random::<[u8; 2]>();
            let msg = DhtMessage::build_ping(&tid, &self.node_id);
            if socket.send_to(&msg, addr).await.is_ok() {
                self.socket_send_total[socket_idx].fetch_add(1, Ordering::Relaxed);
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
            message_semaphore: self.message_semaphore.clone(),
            socket_send_total: self.socket_send_total.clone(),
            socket_recv_total: self.socket_recv_total.clone(),
            pps_last: self.pps_last.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crawler_state_default() {
        let state = CrawlerState::default();
        assert!(!state.running);
        assert_eq!(state.nodes_crawled, 0);
        assert_eq!(state.infohashes_collected, 0);
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
}
