//! 爬虫引擎实现
//!
//! 主动 + 被动混合 DHT 爬虫：
//! 1. 加入 DHT 网络（通过 bootstrap 节点发 find_node 获取初始路由）
//! 2. 主动爬行：定期向已知节点发 find_node，探索 DHT 网络，收集节点
//! 3. 被动监听：其他节点发来的 get_peers / announce_peer 中提取 infohash
//! 4. 对发现的 infohash 主动发 get_peers，收集 peer 并存入缓存

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
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
use crate::storage::repo_traits::NodeRepository;
use crate::storage::PeerRepoImpl;
use crate::types::{Event, Infohash, PeerInfo, PeerSource};

use super::Crawler;

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
}

/// 待响应的请求记录
struct PendingRequest {
    method: QueryMethod,
    target: [u8; 20],
    addr: SocketAddr,
    sent_at: Instant,
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
    /// 待响应的请求（transaction_id -> PendingRequest）
    pending: Arc<RwLock<HashMap<Vec<u8>, PendingRequest>>>,
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

        Self {
            config,
            state: Arc::new(RwLock::new(CrawlerState::default())),
            event_bus,
            shutdown: Arc::new(tokio::sync::Notify::new()),
            seen_infohashes: Arc::new(RwLock::new(HashSet::new())),
            inbound_sources: Arc::new(RwLock::new(HashSet::new())),
            known_nodes: Arc::new(RwLock::new(RoutingTable::new(node_id))),
            pending: Arc::new(RwLock::new(HashMap::new())),
            peer_repo: None,
            storage: None,
            node_repo: None,
            infohash_repo: None,
            node_id,
            virtual_node_ids,
            dns_pool,
        }
    }

    /// 设置 Peer 仓库引用
    pub fn with_peer_repo(mut self, peer_repo: Arc<PeerRepoImpl>) -> Self {
        self.peer_repo = Some(peer_repo);
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

    /// 获取状态快照
    pub fn state(&self) -> CrawlerState {
        let mut s = self.state.read().clone();
        // 方向C：known_nodes 显示 NodeRepo 节点数（主候选池），路由表只用于 DHT 路由
        s.known_nodes = self.node_repo.as_ref().map(|r| r.len_sync()).unwrap_or_else(|| self.known_nodes.read().len());
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

    /// 获取已收集的 infohash 集合（用于 TrackerPeerFetcher 同步）
    pub fn seen_infohashes(&self) -> Arc<RwLock<HashSet<Infohash>>> {
        self.seen_infohashes.clone()
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
        crate::intelligence::SelectSystem::select_diverse_nodes(repo.as_ref(), count, max_per_subnet)
    }

    /// 从 NodeRepo(SQLite) 加载路由表（启动时调用）
    /// 优先从 SQLite 加载，兼容旧版 JSON 文件
    async fn load_routing_table(&self) -> usize {
        // 从 NodeRepo(SQLite) 加载到 NodeRepo（无容量限制，主候选池）
        if let Some(repo) = &self.node_repo {
            match repo.load_all().await {
                Ok(count) => {
                    if count > 0 {
                        info!("[crawler] 从 SQLite(NodeRepo) 加载了 {} 个节点", count);
                        // 同时把 top 节点加入路由表（用于 DHT 路由响应，K限制可能拒绝部分）
                        let top_nodes = repo.top_nodes_sync(128);
                        let mut rt = self.known_nodes.write();
                        let mut rt_added = 0;
                        for entry in &top_nodes {
                            if rt.add_node(entry.id, entry.addr) {
                                rt_added += 1;
                            }
                        }
                        info!("[crawler] 路由表加入 {}/{} 个 top 节点（K=16限制）", rt_added, top_nodes.len());
                        return count;
                    }
                }
                Err(e) => warn!("[crawler] 从 SQLite 加载失败: {}", e),
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
    async fn bootstrap(&self, socket: &UdpSocket) -> usize {
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
                        if self.send_find_node(socket, addr, target).await {
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

    /// 向单个节点发 find_node 请求，返回是否发送成功
    async fn send_find_node(&self, socket: &UdpSocket, addr: SocketAddr, target: [u8; 20]) -> bool {
        let tid = rand::thread_rng().gen::<[u8; 2]>();
        let msg = DhtMessage::build_find_node(&tid, &self.node_id, &target);

        {
            let mut pending = self.pending.write();
            pending.insert(
                tid.to_vec(),
                PendingRequest {
                    method: QueryMethod::FindNode,
                    target,
                    addr,
                    sent_at: Instant::now(),
                },
            );
        }

        if socket.send_to(&msg, addr).await.is_ok() {
            let mut state = self.state.write();
            state.requests_sent += 1;
            true
        } else {
            false
        }
    }

    /// 主动爬行：向已知节点发 find_node 探索网络
    async fn active_crawl(&self, socket: &UdpSocket) {
        // 从 NodeRepo 多样性选取高评分节点（高评分 + ID空间多样性 + IP网段多样性）
        // 评分由 ScoreMaintainer 统一维护，爬虫不自行重算
        let nodes = if self.node_repo.is_some() {
            self.select_diverse_nodes(64, 3) // 选64个（P2优化：并发翻倍），同一/24网段最多3个
        } else {
            // 兼容：没有 NodeRepo 时从路由表选
            let mut known = self.known_nodes.write();
            if known.is_empty() {
                return;
            }
            let mut all = known.all_nodes();
            all.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
            all.truncate(32);
            all
        };

        if nodes.is_empty() {
            return;
        }

        // target ID 多样化：生成 8 个不同 target（从4提升到8），覆盖更广的 ID 空间
        let num_targets = 16; // P2优化：target多样化翻倍，覆盖更广ID空间
        let targets: Vec<[u8; 20]> = (0..num_targets).map(|_| self.random_target()).collect();
        let per_target = (nodes.len() + num_targets - 1) / num_targets;

        for (i, node) in nodes.iter().enumerate() {
            let target_idx = (i / per_target).min(num_targets - 1);
            let target = targets[target_idx];
            let tid = rand::thread_rng().gen::<[u8; 2]>();
            let msg = DhtMessage::build_find_node(&tid, &self.node_id, &target);

            {
                let mut pending = self.pending.write();
                pending.insert(
                    tid.to_vec(),
                    PendingRequest {
                        method: QueryMethod::FindNode,
                        target,
                        addr: node.addr,
                        sent_at: Instant::now(),
                    },
                );
            }

            if socket.send_to(&msg, node.addr).await.is_ok() {
                let mut state = self.state.write();
                state.requests_sent += 1;
                state.nodes_crawled += 1;
            }
        }

        {
            let mut state = self.state.write();
            state.last_crawl_at = Some(Instant::now());
        }
    }

    /// 主动 get_peers：向高评分多样性节点发热门 infohash 的 get_peers
    /// 目的：1) 直接收集 peer  2) 增加我们节点在其他节点路由表中的出现概率  3) 间接增加被动收到查询的概率
    async fn active_get_peers(&self, socket: &UdpSocket) {
        // 从 NodeRepo 多样性选取高评分节点（评分由 ScoreMaintainer 统一维护）
        let nodes = if self.node_repo.is_some() {
            self.select_diverse_nodes(64, 4) // 选64个，同一/24网段最多4个（加速传播）
        } else {
            let known = self.known_nodes.read();
            if known.is_empty() { return; }
            let mut all = known.all_nodes();
            all.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
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

        // 向每个节点发送随机 5 个 infohash 的 get_peers（加速传播）
        for node in &nodes {
            for _ in 0..5 {
                let idx = rand::random::<usize>() % infohashes.len();
                let ih = infohashes[idx];
                let tid = rand::random::<[u8; 2]>();
                let vid = self.random_virtual_node_id();
                let msg = DhtMessage::build_get_peers(&tid, &vid, &ih);

                {
                    let mut pending = self.pending.write();
                    pending.insert(
                        tid.to_vec(),
                        PendingRequest {
                            method: QueryMethod::GetPeers,
                            target: ih,
                            addr: node.addr,
                            sent_at: Instant::now(),
                        },
                    );
                }

                if socket.send_to(&msg, node.addr).await.is_ok() {
                    let mut state = self.state.write();
                    state.requests_sent += 1;
                }
            }
        }

        info!("[crawler] 主动 get_peers: 向 {} 节点发送了 {} infohash 查询", nodes.len(), infohashes.len());
    }

    /// 主动 sample_infohashes（BEP 51: DHT Infohash Indexing）
    /// 向高评分多样性节点发送 sample_infohashes 请求，批量获取它们已知的 infohash
    /// 这是主动发现新 infohash 的核心方法
    async fn active_sample_infohashes(&self, socket: &UdpSocket) {
        // 从 NodeRepo 多样性选取高评分节点
        let nodes = if self.node_repo.is_some() {
            self.select_diverse_nodes(64, 3) // 选64个（P2优化：并发翻倍），同一/24网段最多3个
        } else {
            let known = self.known_nodes.read();
            if known.is_empty() { return; }
            let mut all = known.all_nodes();
            all.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
            all.truncate(32);
            all
        };

        if nodes.is_empty() {
            return;
        }

        let mut requests_sent = 0;
        for node in &nodes {
            let tid = rand::thread_rng().gen::<[u8; 2]>();
            let vid = self.random_virtual_node_id();
            let msg = DhtMessage::build_sample_infohashes(&tid, &vid);

            {
                let mut pending = self.pending.write();
                pending.insert(
                    tid.to_vec(),
                    PendingRequest {
                        method: QueryMethod::SampleInfohashes,
                        target: [0u8; 20], // sample_infohashes 不需要 target
                        addr: node.addr,
                        sent_at: Instant::now(),
                    },
                );
            }

            if socket.send_to(&msg, node.addr).await.is_ok() {
                requests_sent += 1;
            }
        }

        {
            let mut state = self.state.write();
            state.requests_sent += requests_sent;
        }

        info!("[crawler] 主动 sample_infohashes: 向 {} 节点发送了请求", requests_sent);
    }

    /// 主动 scrape（BEP 33: DHT Scrapes）
    /// 向高评分节点发送 scrape 请求，评估已知 infohash 的热度（seeder/leecher）
    async fn active_scrape(&self, socket: &UdpSocket) {
        // 获取要查询的 infohash（从 InfohashRepo 中取前 5 个）
        let infohashes = if let Some(repo) = &self.infohash_repo {
            let all = repo.all_sync();
            all.into_iter().take(5).collect::<Vec<_>>()
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
            let tid = rand::thread_rng().gen::<[u8; 2]>();
            let vid = self.random_virtual_node_id();
            let msg = DhtMessage::build_scrape(&tid, &vid, &ih);

            {
                let mut pending = self.pending.write();
                pending.insert(
                    tid.to_vec(),
                    PendingRequest {
                        method: QueryMethod::Scrape,
                        target: ih,
                        addr: node.addr,
                        sent_at: Instant::now(),
                    },
                );
            }

            if socket.send_to(&msg, node.addr).await.is_ok() {
                requests_sent += 1;
            }
        }

        info!("[crawler] 主动 scrape: 向 {} 节点发送了请求", requests_sent);
    }

    /// 链式爬行：收到响应后，立即向新发现的节点发 find_node
    /// 形成链式反应，大幅提高节点发现速度
    async fn chain_crawl(&self, socket: &UdpSocket, new_nodes: &[crate::discoverers::dht::message::DhtNode]) {
        if new_nodes.is_empty() {
            return;
        }

        // 限制每次链式爬行最多发 8 个请求，避免风暴
        let max_chain = 8;

        for (i, node) in new_nodes.iter().take(max_chain).enumerate() {
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

            let tid = rand::thread_rng().gen::<[u8; 2]>();
            let vid = self.random_virtual_node_id();
            let msg = DhtMessage::build_find_node(&tid, &vid, &chain_target);

            {
                let mut pending = self.pending.write();
                pending.insert(
                    tid.to_vec(),
                    PendingRequest {
                        method: QueryMethod::FindNode,
                        target: chain_target,
                        addr: node.addr,
                        sent_at: Instant::now(),
                    },
                );
            }

            if socket.send_to(&msg, node.addr).await.is_ok() {
                let mut state = self.state.write();
                state.requests_sent += 1;
                state.nodes_crawled += 1;
            }
        }
    }

    /// 刷新所有非空 bucket（Kademlia 标准 bucket 刷新）
    /// 对每个非空 bucket，用该 bucket 范围内的随机 ID 做 find_node
    async fn refresh_buckets(&self, socket: &UdpSocket) {
        let bucket_targets = {
            let known = self.known_nodes.read();
            known.non_empty_bucket_targets()
        };

        if bucket_targets.is_empty() {
            return;
        }

        info!("[crawler] 开始 bucket 刷新，共 {} 个非空 bucket", bucket_targets.len());

        for target in bucket_targets {
            let nodes = {
                let known = self.known_nodes.read();
                let mut near = known.find_closest(&target, 4);
                near.truncate(4);
                near
            };

            for node in nodes {
                let tid = rand::thread_rng().gen::<[u8; 2]>();
                let msg = DhtMessage::build_find_node(&tid, &self.node_id, &target);

                {
                    let mut pending = self.pending.write();
                    pending.insert(
                        tid.to_vec(),
                        PendingRequest {
                            method: QueryMethod::FindNode,
                            target,
                            addr: node.addr,
                            sent_at: Instant::now(),
                        },
                    );
                }

                if socket.send_to(&msg, node.addr).await.is_ok() {
                    let mut state = self.state.write();
                    state.requests_sent += 1;
                }
            }
        }

        info!("[crawler] bucket 刷新完成");
    }

    /// 清理超时的 pending 请求，并记录失败统计到 NodeRepo
    fn cleanup_pending(&self) {
        let mut pending = self.pending.write();
        let timeout = Duration::from_secs(15); // P2优化：超时缩短，加速节点轮换

        // 收集超时请求的地址，用于记录失败统计
        let expired_addrs: Vec<SocketAddr> = pending
            .iter()
            .filter(|(_, req)| req.sent_at.elapsed() >= timeout)
            .map(|(_, req)| req.addr)
            .collect();

        // 记录失败统计到 NodeRepo
        if !expired_addrs.is_empty() {
            if let Some(repo) = &self.node_repo {
                for addr in &expired_addrs {
                    repo.record_query_sync(*addr, false, 0);
                }
            }
            debug!("[crawler] 超时请求 {} 个，已记录失败统计", expired_addrs.len());
        }

        pending.retain(|_, req| req.sent_at.elapsed() < timeout);
    }

    /// 处理收到的 DHT 响应消息
    async fn handle_response(&self, socket: &UdpSocket, data: &[u8], from: SocketAddr) {
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
                    let pending = self.pending.read();
                    pending.get(&tid.to_vec()).map(|r| r.sent_at.elapsed().as_millis() as u64)
                };
                if let Some(repo) = &self.node_repo {
                    repo.record_query_with_nodes_sync(from, latency_ms.unwrap_or(0), nodes.len() as u64);
                }
            }

            // 方向C：新节点加入 NodeRepo（无容量限制，主候选池），同时尝试加入路由表（DHT路由用）
            let added_nodes = if !nodes.is_empty() {
                let mut repo_added = 0;
                let mut rt_added = 0;
                for node in &nodes {
                    if node.addr.port() == 0 {
                        continue;
                    }
                    // 主存储：NodeRepo（无容量限制）
                    if let Some(repo) = &self.node_repo {
                        if repo.add_node_sync(node.id, node.addr) {
                            repo_added += 1;
                        }
                    }
                    // 同时加入路由表（用于 DHT 路由响应，可能因 K 限制被拒绝）
                    let mut known = self.known_nodes.write();
                    if known.add_node(node.id, node.addr) {
                        rt_added += 1;
                    }
                }
                if repo_added > 0 || rt_added > 0 {
                    debug!("[crawler] 响应节点={} NodeRepo新增={} 路由表新增={} NodeRepo总计={} 路由表={}",
                        nodes.len(), repo_added, rt_added,
                        self.node_repo.as_ref().map(|r| r.len_sync()).unwrap_or(0),
                        self.known_nodes.read().len());
                }
                nodes
            } else {
                Vec::new()
            };

            // 链式爬行：立即向新发现的节点发 find_node
            if !added_nodes.is_empty() {
                self.chain_crawl(socket, &added_nodes).await;
                // 注意：NodeRepo 持久化由定期任务（每5分钟）负责，不在每次响应后全量保存，避免大量磁盘 IO
            }

            // 移除 pending
            {
                let mut pending = self.pending.write();
                pending.remove(&tid.to_vec());
            }
            return;
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
                    let pending = self.pending.read();
                    pending.get(&tid.to_vec()).map(|r| r.sent_at.elapsed().as_millis() as u64)
                };
                if let Some(repo) = &self.node_repo {
                    repo.record_query_with_nodes_sync(from, latency_ms.unwrap_or(0), resp.nodes.len() as u64);
                }
            }

            // 存入 peer 缓存
            if !resp.values.is_empty() {
                if let Some(peer_repo) = &self.peer_repo {
                    // 从 pending 中找 infohash
                    let ih = {
                        let pending = self.pending.read();
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

            // 加入新节点到路由表 + NodeRepo + 链式爬行 + 立即持久化
            if !resp.nodes.is_empty() {
                let new_nodes = resp.nodes.clone();
                {
                    let mut known = self.known_nodes.write();
                    for node in &resp.nodes {
                        if node.addr.port() != 0 {
                            known.add_node(node.id, node.addr);
                        }
                    }
                }
                // 同步到 NodeRepo（统一数据归口，无容量限制）
                if let Some(repo) = &self.node_repo {
                    for node in &resp.nodes {
                        if node.addr.port() != 0 {
                            repo.add_node_sync(node.id, node.addr);
                        }
                    }
                }
                // 链式爬行
                self.chain_crawl(socket, &new_nodes).await;
                // 注意：NodeRepo 持久化由定期任务（每5分钟）负责，不在每次响应后全量保存，避免大量磁盘 IO
            }

            // 移除 pending
            {
                let mut pending = self.pending.write();
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
                    let pending = self.pending.read();
                    pending.get(&tid.to_vec()).map(|r| r.sent_at.elapsed().as_millis() as u64)
                };
                if let Some(repo) = &self.node_repo {
                    repo.record_query_with_nodes_sync(from, latency_ms.unwrap_or(0), resp.samples.len() as u64);
                }
            }

            // 注册新发现的 infohash 到 InfohashRepo
            if !resp.samples.is_empty() {
                if let Some(repo) = &self.infohash_repo {
                    let mut new_count = 0;
                    for ih in &resp.samples {
                        let is_new = {
                            let mut seen = self.seen_infohashes.write();
                            seen.insert(*ih)
                        };
                        if is_new {
                            repo.register_sync(*ih, "dht_sample_infohashes");
                            new_count += 1;
                        }
                    }
                    if new_count > 0 {
                        info!("[crawler] sample_infohashes 新增 {} 个 infohash (来自 {})", new_count, from);
                    }
                }
            }

            // 移除 pending
            {
                let mut pending = self.pending.write();
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
                    let pending = self.pending.read();
                    pending.get(&tid.to_vec()).map(|r| r.sent_at.elapsed().as_millis() as u64)
                };
                if let Some(repo) = &self.node_repo {
                    repo.record_query_with_nodes_sync(from, latency_ms.unwrap_or(0), resp.files.len() as u64);
                }
            }

            // 移除 pending
            {
                let mut pending = self.pending.write();
                pending.remove(&tid.to_vec());
            }
        }
    }

    /// 处理收到的 DHT 查询消息（被动响应）
    ///
    /// 返回新发现的 infohash（如果有）
    async fn handle_query(
        &self,
        socket: &UdpSocket,
        data: &[u8],
        from: SocketAddr,
    ) -> Option<Infohash> {
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

        // 入站来源节点统计（被动打洞效果分析）
        {
            let mut sources = self.inbound_sources.write();
            sources.insert(from);
            let mut state = self.state.write();
            state.inbound_unique_sources = sources.len();
        }

        // 被动收集节点：任何发送 DHT 查询的节点都是 DHT 节点，加入路由表 + NodeRepo
        if let Some(node_id) = DhtMessage::extract_query_node_id(data) {
            let mut known = self.known_nodes.write();
            if known.add_node(node_id, from) {
                debug!("[crawler] 被动收集节点: {} (id={})", from, hex::encode(&node_id[..4]));
            }
            // 同步到 NodeRepo（统一数据归口）
            if let Some(repo) = &self.node_repo {
                repo.add_node_sync(node_id, from);
            }
        }

        match method {
            QueryMethod::Ping => {
                let resp = DhtMessage::build_ping_response(&tid, &self.node_id);
                let _ = socket.send_to(&resp, from).await;
            }
            QueryMethod::FindNode => {
                // 完善响应：返回路由表中最接近目标的 K 个节点
                let target = infohash.unwrap_or([0u8; 20]);
                let closest_nodes: Vec<DhtNode> = {
                    let known = self.known_nodes.read();
                    known.find_closest(&target, 8)
                        .into_iter()
                        .map(|e| DhtNode { id: e.id, addr: e.addr })
                        .collect()
                };
                let resp = DhtMessage::build_find_node_response_with_nodes(&tid, &self.node_id, &closest_nodes);
                let _ = socket.send_to(&resp, from).await;
            }
            QueryMethod::GetPeers => {
                let token = rand::thread_rng().gen::<[u8; 4]>();
                // 完善响应：如果 PeerRepo 中有这个 infohash 的 peer，返回它们
                let peers_for_ih: Vec<SocketAddr> = if let Some(ih) = infohash {
                    if let Some(peer_repo) = &self.peer_repo {
                        peer_repo.get_peers_sync(&ih, 20)
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
                    known.find_closest(&infohash.unwrap_or([0u8; 20]), 8)
                        .into_iter()
                        .map(|e| DhtNode { id: e.id, addr: e.addr })
                        .collect()
                };
                let resp = DhtMessage::build_get_peers_response_full(&tid, &self.node_id, &token, &peers_for_ih, &closest_nodes);
                let _ = socket.send_to(&resp, from).await;

                // 同时主动发 get_peers 回去，收集这个 infohash 的 peer
                if let Some(ih) = infohash {
                    self.query_get_peers(socket, from, ih).await;
                    return Some(ih);
                }
            }
            QueryMethod::AnnouncePeer => {
                // 发送 announce_peer 响应
                let resp = DhtMessage::build_ping_response(&tid, &self.node_id);
                let _ = socket.send_to(&resp, from).await;

                if let Some(ih) = infohash {
                    info!("[crawler] 收到 announce_peer from {} (ih={:?})", from, &ih[..4]);
                    return Some(ih);
                }
            }
            QueryMethod::SampleInfohashes => {
                // BEP 51: 返回我们已知的 infohash 样本
                let all_infohashes = if let Some(repo) = &self.infohash_repo {
                    repo.all_sync()
                } else {
                    vec![]
                };

                // 随机采样最多 20 个 infohash（避免包过大）
                let mut samples: Vec<Infohash> = all_infohashes.iter().copied().collect();
                if samples.len() > 20 {
                    use rand::seq::SliceRandom;
                    let mut rng = rand::thread_rng();
                    samples.shuffle(&mut rng);
                    samples.truncate(20);
                }

                let resp = DhtMessage::build_sample_infohashes_response(
                    &tid,
                    &self.node_id,
                    all_infohashes.len() as i64,
                    &samples,
                );
                let _ = socket.send_to(&resp, from).await;
            }
            QueryMethod::Scrape => {
                // BEP 33: 返回空的 scrape 响应（当前不维护 seeder/leecher 统计）
                // 响应格式: d1:rd2:id20:<node_id>5:filesde1:t2:<tid>1:y1:re
                let mut resp = Vec::new();
                resp.extend_from_slice(b"d1:rd2:id20:");
                resp.extend_from_slice(&self.node_id);
                resp.extend_from_slice(b"5:filesde1:t2:");
                resp.extend_from_slice(&tid);
                resp.extend_from_slice(b"1:y1:re");
                let _ = socket.send_to(&resp, from).await;
            }
        }

        None
    }

    /// 向指定节点主动发 get_peers 查询
    async fn query_get_peers(&self, socket: &UdpSocket, addr: SocketAddr, infohash: Infohash) {
        let tid = rand::thread_rng().gen::<[u8; 2]>();
        let msg = DhtMessage::build_get_peers(&tid, &self.node_id, &infohash);

        {
            let mut pending = self.pending.write();
            pending.insert(
                tid.to_vec(),
                PendingRequest {
                    method: QueryMethod::GetPeers,
                    target: infohash,
                    addr,
                    sent_at: Instant::now(),
                },
            );
        }

        if socket.send_to(&msg, addr).await.is_ok() {
            let mut state = self.state.write();
            state.requests_sent += 1;
            debug!("[crawler] 主动 get_peers -> {} (ih={})", addr, hex::encode(infohash));
        }
    }

    /// 爬行循环：主动 + 被动混合
    async fn crawl_loop(&self) {
        info!(
            "[crawler] DHT 爬虫启动（主动模式），监听端口 {}",
            self.config.listen_port
        );

        let socket = match UdpSocket::bind(format!("0.0.0.0:{}", self.config.listen_port)).await {
            Ok(s) => s,
            Err(e) => {
                warn!("[crawler] 绑定端口 {} 失败: {}", self.config.listen_port, e);
                let mut state = self.state.write();
                state.running = false;
                state.errors += 1;
                return;
            }
        };

        // 加入 DHT 网络
        self.load_routing_table().await;
        let bootstrap_count = self.bootstrap(&socket).await;
        info!(
            "[crawler] 向 {} 个 bootstrap 地址发送了 find_node",
            bootstrap_count
        );

        let mut buf = vec![0u8; 8192]; // P2优化：接收缓冲区翻倍，减少丢包
        let mut last_bootstrap = Instant::now();
        let mut last_active_crawl = Instant::now();
        let mut last_active_get_peers = Instant::now();
        let mut last_active_sample_infohashes = Instant::now();
        let mut last_active_scrape = Instant::now();
        let mut last_cleanup = Instant::now();
        let mut last_bucket_refresh = Instant::now();
        let mut last_keepalive = Instant::now();
        let bootstrap_interval = Duration::from_secs(120); // 每 2 分钟重新 bootstrap
        let keepalive_interval = Duration::from_secs(60); // 每 60 秒向高评分节点发 ping 保持活跃度
        let crawl_interval = Duration::from_millis(self.config.crawl_interval_secs * 1000 / 2); // 主动爬行间隔
        let get_peers_interval = Duration::from_secs(10); // 主动 get_peers 间隔（加速节点传播）
        let sample_infohashes_interval = Duration::from_secs(15); // 主动 sample_infohashes 间隔（BEP 51）
        let scrape_interval = Duration::from_secs(60); // 主动 scrape 间隔（BEP 33）
        let bucket_refresh_interval = Duration::from_secs(300); // 每 5 分钟刷新所有 bucket

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => {
                    info!("[crawler] 爬虫引擎收到停止信号");
                    break;
                }
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((n, from)) => {
                            let data = &buf[..n];

                            // 更新消息计数
                            {
                                let mut state = self.state.write();
                                state.messages_received += 1;
                            }

                            let preview = String::from_utf8_lossy(&data[..std::cmp::min(n, 60)]);
                            debug!("[crawler] 收到 {} 字节 from {}: {}", n, from, preview);

                            // 先尝试解析为响应（主动请求的回复）
                            self.handle_response(&socket, data, from).await;

                            // 再尝试解析为查询（被动监听）
                            if let Some(infohash) = self.handle_query(&socket, data, from).await {
                                let is_new = {
                                    let mut seen = self.seen_infohashes.write();
                                    seen.insert(infohash)
                                };

                                if is_new {
                                    let hex_ih = hex::encode(infohash);
                                    info!("[crawler] 发现新 infohash: {} (来自 {})", hex_ih, from);

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
                                    if state.infohashes_collected % 10 == 0 {
                                        self.event_bus.publish(Event::CrawlProgress {
                                            nodes_crawled: state.nodes_crawled,
                                            infohashes_collected: state.infohashes_collected,
                                            peers_collected: state.peers_collected,
                                            message: format!(
                                                "已收集 {} infohash, {} peer, {} 已知节点",
                                                state.infohashes_collected,
                                                state.peers_collected,
                                                self.node_repo.as_ref().map(|r| r.len_sync()).unwrap_or_else(|| self.known_nodes.read().len())
                                            ),
                                        });
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            debug!("[crawler] 接收消息失败: {}", e);
                            let mut state = self.state.write();
                            state.errors += 1;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(500)) => {
                    // 同步 known_nodes 到 state（API 读取的是 state.known_nodes）
                    {
                        let mut state = self.state.write();
                        state.known_nodes = self.node_repo.as_ref().map(|r| r.len_sync()).unwrap_or_else(|| self.known_nodes.read().len());
                    }

                    // 定期主动爬行
                    if last_active_crawl.elapsed() >= crawl_interval {
                        self.active_crawl(&socket).await;
                        last_active_crawl = Instant::now();
                    }

                    // 定期主动 get_peers（收集 peer + 增加被查询概率）
                    if last_active_get_peers.elapsed() >= get_peers_interval {
                        self.active_get_peers(&socket).await;
                        last_active_get_peers = Instant::now();
                    }

                    // 定期主动 sample_infohashes（BEP 51: 主动批量获取 infohash）
                    if last_active_sample_infohashes.elapsed() >= sample_infohashes_interval {
                        self.active_sample_infohashes(&socket).await;
                        last_active_sample_infohashes = Instant::now();
                    }

                    // 定期主动 scrape（BEP 33: 评估 infohash 热度）
                    if last_active_scrape.elapsed() >= scrape_interval {
                        self.active_scrape(&socket).await;
                        last_active_scrape = Instant::now();
                    }

                    // 定期重新 bootstrap
                    if last_bootstrap.elapsed() >= bootstrap_interval {
                        let count = self.bootstrap(&socket).await;
                        debug!("[crawler] 定期 bootstrap，向 {} 个地址发送 find_node", count);
                        last_bootstrap = Instant::now();
                    }

                    // 定期清理 pending
                    if last_cleanup.elapsed() >= Duration::from_secs(10) {
                        self.cleanup_pending();
                        last_cleanup = Instant::now();
                    }

                    // 定期刷新所有 bucket（Kademlia 标准）
                    if last_bucket_refresh.elapsed() >= bucket_refresh_interval {
                        self.refresh_buckets(&socket).await;
                        last_bucket_refresh = Instant::now();
                    }

                    // 定期节点活跃度维护（向高评分节点发 ping，保持我们在其他节点路由表中的活跃度）
                    if last_keepalive.elapsed() >= keepalive_interval {
                        self.active_keepalive(&socket).await;
                        last_keepalive = Instant::now();
                    }
                }
            }
        }

        info!("[crawler] 爬虫引擎已停止");
    }

    /// 节点活跃度维护：向 top 20 高评分节点发送 ping
    ///
    /// 目的：保持我们的节点在其他节点路由表中的活跃度，
    /// 让其他节点更频繁地向我们发送请求（被动打洞正循环）。
    async fn active_keepalive(&self, socket: &UdpSocket) {
        let top_nodes: Vec<std::net::SocketAddr> = {
            let table = self.known_nodes.read();
            table.top_nodes_by_score(20)
                .into_iter()
                .map(|n| n.addr)
                .collect()
        };
        if top_nodes.is_empty() { return; }
        let mut sent = 0;
        for addr in &top_nodes {
            let tid = rand::random::<[u8; 2]>();
            let msg = DhtMessage::build_ping(&tid, &self.node_id);
            if socket.send_to(&msg, addr).await.is_ok() { sent += 1; }
        }
        {
            let mut state = self.state.write();
            state.requests_sent += sent;
        }
        debug!("[crawler] 活跃度维护: 向 {}/{} 个高评分节点发送了 ping", sent, top_nodes.len());
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

        // 从 SQLite 加载路由表
        if let Some(storage) = &self.storage {
            match storage.load_dht_nodes() {
                Ok(rows) if !rows.is_empty() => {
                    let mut table = self.known_nodes.write();
                    for row in &rows {
                        let addr = format!("{}:{}", row.ip, row.port).parse().unwrap_or_else(|_| {
                            std::net::SocketAddr::new(
                                std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                                0,
                            )
                        });
                        let mut entry = crate::dht::kbucket::KBucketEntry::new(row.id, addr);
                        entry.score = row.score;
                        entry.query_count = row.query_count;
                        entry.success_count = row.success_count;
                        entry.total_latency_ms = row.total_latency_ms;
                        entry.consecutive_failures = row.consecutive_failures;
                        table.add_entry(entry);
                    }
                    info!("[crawler] 从 SQLite 加载了 {} 个 DHT 节点", rows.len());
                }
                Ok(_) => {
                    info!("[crawler] SQLite 中无 DHT 节点记录，使用 bootstrap 节点");
                }
                Err(e) => {
                    warn!("[crawler] 从 SQLite 加载 DHT 节点失败: {}", e);
                }
            }
        }

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
