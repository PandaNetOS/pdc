//! TrackerPeerFetcher — 主动向 tracker 拉 peer 的组件
//!
//! 数据来源统一：InfohashRepo（唯一数据源）
//! 1. 内置热门公共种子 infohash → 启动时注册到 InfohashRepo
//! 2. DHT 收集的 infohash → crawler 直接注册到 InfohashRepo
//! 3. 超级 Tracker announce → 直接注册到 InfohashRepo
//! 4. 每轮从 InfohashRepo 调取 infohash，向 tracker 拉 peer
//! 5. 获取到的 peer → 探测队列 → DHT ping → NodeRepo

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{info, warn};

use crate::discoverers::tracker::TrackerDiscoverer;
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

/// TrackerPeerFetcher — 主动向 tracker 拉 peer 的组件
///
/// infohash 来源统一为 InfohashRepo，不再维护独立的内存池。
/// peer 统一存入 PeerRepo，DhtProbe 定期从 PeerRepo 拉取探测。
pub struct TrackerPeerFetcher {
    discoverer: Arc<TrackerDiscoverer>,
    peer_repo: Option<Arc<crate::storage::PeerRepoImpl>>,
    /// Infohash 仓库（唯一数据源，必需）
    infohash_repo: Option<Arc<crate::storage::InfohashRepoImpl>>,
    interval_secs: u64,
    trackers_per_round: usize,
    infohashes_per_round: usize,
    peers_per_infohash: usize,
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
            interval_secs: 60,
            trackers_per_round: 10,
            infohashes_per_round: 5,
            peers_per_infohash: 50,
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
        info!("[tracker_fetcher] 已注册 {} 个内置热门 infohash 到 InfohashRepo", registered);
        self.infohash_repo = Some(repo);
        self
    }

    /// 获取当前 InfohashRepo 中的 infohash 数量
    pub fn infohash_count(&self) -> usize {
        self.infohash_repo.as_ref().map(|r| r.count_sync()).unwrap_or(0)
    }

    /// 启动后台拉取任务
    pub fn start(&self) {
        let discoverer = self.discoverer.clone();
        let peer_repo = self.peer_repo.clone();
        let infohash_repo = self.infohash_repo.clone();
        let trackers_per_round = self.trackers_per_round;
        let infohashes_per_round = self.infohashes_per_round;
        let peers_per_infohash = self.peers_per_infohash;
        let interval_secs = self.interval_secs;
        let total_rounds = self.total_rounds.clone();
        let total_peers_fetched = self.total_peers_fetched.clone();
        let last_round_peers = self.last_round_peers.clone();

        tokio::spawn(async move {
            info!("[tracker_fetcher] 后台拉取任务启动，间隔 {}s", interval_secs);
            let mut ticker = interval(Duration::from_secs(interval_secs));
            // 启动后立即执行一次
            ticker.tick().await;

            loop {
                ticker.tick().await;

                // 从 InfohashRepo 调取所有 infohash，随机选 N 个
                let ihs: Vec<Infohash> = if let Some(repo) = &infohash_repo {
                    let all = repo.all_sync();
                    if all.is_empty() {
                        warn!("[tracker_fetcher] InfohashRepo 为空，跳过本轮");
                        continue;
                    }
                    // Fisher-Yates 洗牌
                    let mut shuffled = all;
                    for i in (1..shuffled.len()).rev() {
                        let j = rand::random::<usize>() % (i + 1);
                        shuffled.swap(i, j);
                    }
                    shuffled.truncate(infohashes_per_round);
                    shuffled.into_iter().map(|(ih, _)| ih).collect()
                } else {
                    warn!("[tracker_fetcher] 未绑定 InfohashRepo，跳过本轮");
                    continue;
                };

                info!("[tracker_fetcher] 开始本轮拉取: {} infohash (Repo总计 {}), 最多 {} tracker/infohash",
                    ihs.len(), infohash_repo.as_ref().map(|r| r.count_sync()).unwrap_or(0), trackers_per_round);

                // 批量 scrape，收集 infohash 统计（complete/incomplete/downloaded）
                let scrape_results = discoverer.scrape(&ihs).await;
                if !scrape_results.is_empty() {
                    info!("[tracker_fetcher] scrape 完成: {} 个 infohash 有统计", scrape_results.len());
                    for (ih, complete, incomplete, downloaded) in &scrape_results {
                        info!("[tracker_fetcher] ih={} complete={} incomplete={} downloaded={}",
                            &hex::encode(&ih[..4]), complete, incomplete, downloaded);
                    }
                }

                let mut total_peers = 0;
                let mut total_added = 0;

                for ih in &ihs {
                    match discoverer.discover_peers(ih, peers_per_infohash).await {
                        Ok(peers) => {
                            total_peers += peers.len();
                            // 存入 PeerRepo（统一数据归口，DhtProbe 会定期从 PeerRepo 拉取探测）
                            if let Some(repo) = &peer_repo {
                                if !peers.is_empty() {
                                    repo.add_peers_sync(ih, &peers);
                                    total_added += peers.len();
                                }
                            }
                            info!("[tracker_fetcher] ih={} 获取 {} peer, 存入 PeerRepo",
                                &hex::encode(&ih[..4]), peers.len());
                        }
                        Err(e) => {
                            warn!("[tracker_fetcher] ih={} 拉取失败: {}",
                                &hex::encode(&ih[..4]), e);
                        }
                    }
                }

                // 更新统计
                total_rounds.fetch_add(1, Ordering::Relaxed);
                total_peers_fetched.fetch_add(total_peers as u64, Ordering::Relaxed);
                last_round_peers.store(total_peers as u64, Ordering::Relaxed);

                info!("[tracker_fetcher] 本轮完成: 获取 {} peer, 加入探测队列 {}, InfohashRepo={}",
                    total_peers, total_added, infohash_repo.as_ref().map(|r| r.count_sync()).unwrap_or(0));
            }
        });
    }
}
