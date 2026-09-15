//! TrackerPeerFetcher — 主动向 tracker 拉 peer 的组件
//!
//! 数据来源统一：InfohashRepo（唯一数据源）
//! 1. 内置热门公共种子 infohash → 启动时注册到 InfohashRepo
//! 2. DHT 收集的 infohash → crawler 直接注册到 InfohashRepo
//! 3. 超级 Tracker announce → 直接注册到 InfohashRepo
//! 4. 每轮从 InfohashRepo 调取 infohash，向 tracker 拉 peer
//! 5. 获取到的 peer → 探测队列 → DHT ping → NodeRepo
//!
//! 四阶段自适应查询（方案 I）：
//!   阶段1：scrape 筛选（轮换 top N，获取 complete 数）
//!   阶段2：综合排序（scrape complete + 历史产出加权）
//!   阶段3：announce 拉取（top 高价值 + 探索新 infohash）
//!   阶段4：反馈更新（记录每个 infohash 的产出）

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tracing::{info, warn};

use crate::discoverers::tracker::TrackerDiscoverer;
use crate::storage::repo_traits::InfohashRepository;
use crate::traits::PeerDiscoverer;
use crate::types::Infohash;

/// 热门公共种子 infohash（保底用，确保持续有 peer）
/// 来源：Ubuntu 官方 tracker，均为活跃种子，complete 数高
pub const POPULAR_INFOHASHES: &[&str] = &[
    // xubuntu-25.10-desktop-amd64.iso (234 complete)
    "aa8a2f25763f0b165766690847bf732476490396",
    // kubuntu-24.04.4-desktop-amd64.iso (120 complete)
    "299671d28121049a9265be9062d503c4d8402cfb",
    // lubuntu-24.04.4-desktop-amd64.iso (91 complete)
    "c84b227d26b6c05f6ab92f5073d18a9cb84dacd6",
    // kubuntu-25.10-desktop-amd64.iso (83 complete)
    "92b187cdfc4d926a13e7620d44bd18695f0150fb",
    // kubuntu-26.04.1-desktop-amd64.iso (78 complete)
    "7751f52345b9a0b4bc03782611b9e676d63ce294",
    // lubuntu-25.10-desktop-amd64.iso (49 complete)
    "1235d8b8e4e0314c80ddd39c755d5582803c9609",
    // kubuntu-22.04.5-desktop-amd64.iso (56 complete)
    "7ddcb3cf9dbbc96d3c08fb808f57d59fa42e8bfb",
    // lubuntu-22.04.5-desktop-amd64.iso (55 complete)
    "337ef6470ff715be2e09882624daeb9b64d4cb10",
    // edubuntu-24.04.4-desktop-amd64.iso (39 complete)
    "f73430dbfaf0031f9c5fddcf0adc340456db4c91",
    // Ubuntu 24.04 desktop amd64
    "2b66980093bc11806fab50cb3cb41835b95a0362",
];

/// scrape 轮换池大小：从 top N 中分段轮换 scrape
const SCRAPE_POOL_SIZE: usize = 100;
/// 每轮 scrape 的 infohash 数量
const SCRAPE_PER_ROUND: usize = 20;
/// announce 高价值 infohash 数量
const ANNOUNCE_TOP_COUNT: usize = 15;
/// 探索新 infohash 数量
const EXPLORE_COUNT: usize = 5;
/// 连续无产出降权阈值
const EMPTY_DEGRADE_THRESHOLD: u32 = 3;

/// infohash 查询统计（用于自适应加权）
#[derive(Debug, Clone, Default)]
struct InfohashQueryStats {
    /// 总查询次数
    total_queries: u64,
    /// 累计获取 peer 数
    total_peers_fetched: u64,
    /// 上次查询时间戳（秒）
    last_query_at: u64,
    /// 连续无产出次数
    consecutive_empty: u32,
}

impl InfohashQueryStats {
    /// 平均每次查询产出
    fn avg_yield(&self) -> f64 {
        if self.total_queries == 0 {
            0.0
        } else {
            self.total_peers_fetched as f64 / self.total_queries as f64
        }
    }

    /// 降权系数：连续无产出越多，权重越低
    fn degrade_factor(&self) -> f64 {
        if self.consecutive_empty >= EMPTY_DEGRADE_THRESHOLD {
            0.5
        } else {
            1.0
        }
    }
}

/// TrackerPeerFetcher — 主动向 tracker 拉 peer 的组件
///
/// infohash 来源统一为 InfohashRepo，不再维护独立的内存池。
/// peer 统一存入 PeerRepo，DhtProbe 定期从 PeerRepo 拉取探测。
pub struct TrackerPeerFetcher {
    discoverer: Arc<TrackerDiscoverer>,
    peer_repo: Option<Arc<crate::storage::PeerRepoImpl>>,
    /// Infohash 仓库（唯一数据源，必需）
    infohash_repo: Option<Arc<crate::storage::InfohashRepoImpl>>,
    _interval_secs: u64,
    infohashes_per_round: usize,
    peers_per_infohash: usize,
    /// infohash 查询统计（自适应加权用）
    query_stats: Arc<RwLock<HashMap<Infohash, InfohashQueryStats>>>,
    /// scrape 轮换偏移量
    scrape_offset: Arc<AtomicUsize>,
    // 统计
    pub total_rounds: Arc<AtomicU64>,
    pub total_peers_fetched: Arc<AtomicU64>,
    pub last_round_peers: Arc<AtomicU64>,
}

impl TrackerPeerFetcher {
    pub fn new(discoverer: Arc<TrackerDiscoverer>) -> Self {
        Self {
            discoverer,
            peer_repo: None,
            infohash_repo: None,
            _interval_secs: 60,
            infohashes_per_round: 20,
            peers_per_infohash: 50,
            query_stats: Arc::new(RwLock::new(HashMap::new())),
            scrape_offset: Arc::new(AtomicUsize::new(0)),
            total_rounds: Arc::new(AtomicU64::new(0)),
            total_peers_fetched: Arc::new(AtomicU64::new(0)),
            last_round_peers: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 绑定 PeerRepo，获取到的 peer 同时存入 PeerRepo
    pub fn with_peer_repo(mut self, repo: Arc<crate::storage::PeerRepoImpl>) -> Self {
        self.peer_repo = Some(repo);
        self
    }

    /// 绑定 InfohashRepo（唯一数据源），并把内置热门 infohash 注册到 Repo
    pub fn with_infohash_repo(mut self, repo: Arc<crate::storage::InfohashRepoImpl>) -> Self {
        // 把内置热门 infohash 注册到 InfohashRepo
        let mut registered = 0;
        for hex_str in POPULAR_INFOHASHES {
            if let Ok(bytes) = hex::decode(hex_str) {
                if bytes.len() == 20 {
                    let mut ih = [0u8; 20];
                    ih.copy_from_slice(&bytes);
                    repo.register_sync(ih, "fetcher_builtin");
                    registered += 1;
                }
            }
        }
        info!(
            "[tracker_fetcher] 已注册 {} 个内置热门 infohash 到 InfohashRepo",
            registered
        );
        self.infohash_repo = Some(repo);
        self
    }

    /// 获取当前 InfohashRepo 中的 infohash 数量
    pub fn infohash_count(&self) -> usize {
        self.infohash_repo
            .as_ref()
            .map(|r| r.count_sync())
            .unwrap_or(0)
    }

    /// 当前时间戳（秒）
    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// 阶段1：获取本轮要 scrape 的 infohash（轮换 top pool）
    fn select_scrape_infohashes(&self, top_pool: &[(Infohash, f64)]) -> Vec<Infohash> {
        if top_pool.is_empty() {
            return Vec::new();
        }
        let offset = self.scrape_offset.load(Ordering::Relaxed);
        let pool_size = top_pool.len().min(SCRAPE_POOL_SIZE);
        let start = offset % pool_size;
        let mut result = Vec::with_capacity(SCRAPE_PER_ROUND);
        for i in 0..SCRAPE_PER_ROUND.min(pool_size) {
            let idx = (start + i) % pool_size;
            result.push(top_pool[idx].0);
        }
        // 更新偏移量
        self.scrape_offset.store(
            (offset + SCRAPE_PER_ROUND) % pool_size.max(1),
            Ordering::Relaxed,
        );
        result
    }

    /// 阶段2：计算综合分数并排序
    /// 分数 = complete_count * 0.6 + 历史平均产出 * 0.3 + 时间衰减 * 0.1
    fn rank_infohashes(
        &self,
        scrape_results: &[(Infohash, i64, i64, i64)],
    ) -> Vec<(Infohash, f64)> {
        let stats = self.query_stats.read();
        let now = Self::now_secs();
        let mut ranked: Vec<(Infohash, f64)> = scrape_results
            .iter()
            .map(|(ih, complete, _incomplete, _downloaded)| {
                let stat = stats.get(ih);
                let historical_yield = stat.map(|s| s.avg_yield()).unwrap_or(0.0);
                let degrade = stat.map(|s| s.degrade_factor()).unwrap_or(1.0);
                // 时间衰减：越久没查，分数越高（促进轮换）
                let recency_bonus = if let Some(s) = stat {
                    let hours_since = (now.saturating_sub(s.last_query_at)) as f64 / 3600.0;
                    hours_since.min(24.0) / 24.0
                } else {
                    1.0 // 从未查过的给满分
                };
                let score =
                    (*complete as f64 * 0.6 + historical_yield * 0.3 + recency_bonus * 10.0 * 0.1)
                        * degrade;
                (*ih, score)
            })
            .collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked
    }

    /// 阶段3：选择探索用的 infohash（从 top pool 中选未查过的）
    fn select_explore_infohashes(
        &self,
        top_pool: &[(Infohash, f64)],
        exclude: &[Infohash],
    ) -> Vec<Infohash> {
        let stats = self.query_stats.read();
        let mut unexplored: Vec<Infohash> = top_pool
            .iter()
            .map(|(ih, _)| *ih)
            .filter(|ih| !exclude.contains(ih) && !stats.contains_key(ih))
            .collect();
        // 随机洗牌
        for i in (1..unexplored.len()).rev() {
            let j = rand::random::<usize>() % (i + 1);
            unexplored.swap(i, j);
        }
        unexplored.truncate(EXPLORE_COUNT);
        // 如果未查过的不够，从 top pool 中随机补充
        if unexplored.len() < EXPLORE_COUNT {
            let mut candidates: Vec<Infohash> = top_pool
                .iter()
                .map(|(ih, _)| *ih)
                .filter(|ih| !exclude.contains(ih) && !unexplored.contains(ih))
                .collect();
            for i in (1..candidates.len()).rev() {
                let j = rand::random::<usize>() % (i + 1);
                candidates.swap(i, j);
            }
            for ih in candidates {
                if unexplored.len() >= EXPLORE_COUNT {
                    break;
                }
                unexplored.push(ih);
            }
        }
        unexplored
    }

    /// 阶段4：更新查询统计
    fn update_query_stats(&self, ih: &Infohash, peers_fetched: usize) {
        let mut stats = self.query_stats.write();
        let entry = stats.entry(*ih).or_default();
        entry.total_queries += 1;
        entry.total_peers_fetched += peers_fetched as u64;
        entry.last_query_at = Self::now_secs();
        if peers_fetched == 0 {
            entry.consecutive_empty += 1;
        } else {
            entry.consecutive_empty = 0;
        }
    }

    /// 执行一次 tracker 拉取（由 TaskScheduler 按间隔调度）
    /// 四阶段自适应查询：scrape 筛选 → 综合排序 → announce 拉取 → 反馈更新
    pub async fn run_once(&self) {
        let repo = match &self.infohash_repo {
            Some(r) => r,
            None => return,
        };

        // 获取 top pool（按 score 排序）
        let top_pool = repo.top_infohashes(SCRAPE_POOL_SIZE).await;
        if top_pool.is_empty() {
            // 评分数据为空时回退到随机选择
            let all = repo.all_sync();
            if all.is_empty() {
                return;
            }
            let mut shuffled = all;
            for i in (1..shuffled.len()).rev() {
                let j = rand::random::<usize>() % (i + 1);
                shuffled.swap(i, j);
            }
            shuffled.truncate(self.infohashes_per_round);
            let ihs: Vec<Infohash> = shuffled.into_iter().map(|(ih, _)| ih).collect();
            self.do_announce_batch(&ihs).await;
            return;
        }

        // 阶段1：scrape 筛选（轮换 top pool）
        let scrape_ihs = self.select_scrape_infohashes(&top_pool);
        if scrape_ihs.is_empty() {
            return;
        }

        info!(
            "[tracker_fetcher] 阶段1: scrape {} 个 infohash (offset={}, pool={})",
            scrape_ihs.len(),
            self.scrape_offset.load(Ordering::Relaxed),
            top_pool.len()
        );

        let scrape_results = self.discoverer.scrape(&scrape_ihs).await;
        if !scrape_results.is_empty() {
            info!(
                "[tracker_fetcher] scrape 完成: {} 个 infohash 有统计",
                scrape_results.len()
            );
        }

        // 阶段2：综合排序
        let ranked = if scrape_results.is_empty() {
            // scrape 全部失败时，直接用 top pool 的 score 排序
            top_pool
                .iter()
                .take(ANNOUNCE_TOP_COUNT + EXPLORE_COUNT)
                .map(|(ih, score)| (*ih, *score))
                .collect()
        } else {
            self.rank_infohashes(&scrape_results)
        };

        // 阶段3：announce 拉取（top 高价值 + 探索）
        let top_ihs: Vec<Infohash> = ranked
            .iter()
            .take(ANNOUNCE_TOP_COUNT)
            .map(|(ih, _)| *ih)
            .collect();

        let explore_ihs = self.select_explore_infohashes(&top_pool, &top_ihs);

        let mut announce_ihs = top_ihs.clone();
        announce_ihs.extend(explore_ihs.iter());

        info!(
            "[tracker_fetcher] 阶段3: announce {} 个 (top={}, explore={})",
            announce_ihs.len(),
            top_ihs.len(),
            explore_ihs.len()
        );

        // 执行 announce 并记录每个 infohash 的产出
        let mut total_peers = 0;
        let mut total_added = 0;

        for ih in &announce_ihs {
            let peers_count = match self
                .discoverer
                .discover_peers(ih, self.peers_per_infohash)
                .await
            {
                Ok(peers) => {
                    let count = peers.len();
                    total_peers += count;
                    // 存入 PeerRepo（统一数据归口，DhtProbe 会定期从 PeerRepo 拉取探测）
                    if let Some(repo) = &self.peer_repo {
                        if !peers.is_empty() {
                            repo.add_peers_sync(ih, &peers);
                            total_added += count;
                        }
                    }
                    info!(
                        "[tracker_fetcher] ih={} 获取 {} peer, 存入 PeerRepo",
                        &hex::encode(&ih[..4]),
                        count
                    );
                    count
                }
                Err(e) => {
                    warn!(
                        "[tracker_fetcher] ih={} 拉取失败: {}",
                        &hex::encode(&ih[..4]),
                        e
                    );
                    0
                }
            };

            // 阶段4：反馈更新
            self.update_query_stats(ih, peers_count);
        }

        // 更新统计
        self.total_rounds.fetch_add(1, Ordering::Relaxed);
        self.total_peers_fetched
            .fetch_add(total_peers as u64, Ordering::Relaxed);
        self.last_round_peers
            .store(total_peers as u64, Ordering::Relaxed);

        info!(
            "[tracker_fetcher] 本轮完成: 获取 {} peer, 加入探测队列 {}, InfohashRepo={}",
            total_peers,
            total_added,
            self.infohash_count()
        );
    }

    /// 回退路径：直接对一批 infohash 做 announce（scrape 不可用时）
    async fn do_announce_batch(&self, ihs: &[Infohash]) {
        let mut total_peers = 0;

        for ih in ihs {
            match self
                .discoverer
                .discover_peers(ih, self.peers_per_infohash)
                .await
            {
                Ok(peers) => {
                    total_peers += peers.len();
                    if let Some(repo) = &self.peer_repo {
                        if !peers.is_empty() {
                            repo.add_peers_sync(ih, &peers);
                        }
                    }
                    self.update_query_stats(ih, peers.len());
                }
                Err(e) => {
                    warn!(
                        "[tracker_fetcher] ih={} 拉取失败: {}",
                        &hex::encode(&ih[..4]),
                        e
                    );
                    self.update_query_stats(ih, 0);
                }
            }
        }

        self.total_rounds.fetch_add(1, Ordering::Relaxed);
        self.total_peers_fetched
            .fetch_add(total_peers as u64, Ordering::Relaxed);
        self.last_round_peers
            .store(total_peers as u64, Ordering::Relaxed);
    }
}
