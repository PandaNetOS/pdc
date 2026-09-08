//! TrackerService — 超级 Tracker 业务层

use std::sync::Arc;
use rustc_hash::FxHashMap;

use crate::storage::repo_traits::{InfohashRepository, PeerRepository, TrackerRepository};
use crate::storage::{InfohashRepoImpl, PeerRepoImpl, TrackerRepoImpl};
use crate::types::{Infohash, PeerInfo, TrackerAnnounceRequest, TrackerAnnounceResponse, TrackerScrapeRequest, TrackerScrapeResponse};

pub struct TrackerService {
    peer_repo: Arc<PeerRepoImpl>,
    infohash_repo: Arc<InfohashRepoImpl>,
    tracker_repo: Arc<TrackerRepoImpl>,
}

impl TrackerService {
    pub fn new(
        peer_repo: Arc<PeerRepoImpl>,
        infohash_repo: Arc<InfohashRepoImpl>,
        tracker_repo: Arc<TrackerRepoImpl>,
    ) -> Self {
        Self {
            peer_repo,
            infohash_repo,
            tracker_repo,
        }
    }

    pub async fn announce(&self, req: TrackerAnnounceRequest) -> TrackerAnnounceResponse {
        // 注册 infohash
        self.infohash_repo.register(req.info_hash, "super_tracker").await;

        // 把请求方加入 peer 池
        let peer = PeerInfo::new(req.remote_addr, crate::types::PeerSource::SuperTracker);
        PeerRepository::add_peer(&*self.peer_repo, req.info_hash, peer).await;

        // 返回该 infohash 下的其他 peer
        let peers = PeerRepository::get_peers(&*self.peer_repo, &req.info_hash, 50).await;
        let peer_addrs: Vec<std::net::SocketAddr> = peers
            .into_iter()
            .filter(|p| p.addr != req.remote_addr)
            .map(|p| p.addr)
            .collect();

        let complete = peer_addrs.len() as i64;

        TrackerAnnounceResponse {
            interval: 1800,
            min_interval: Some(900),
            tracker_id: Some("pdc".to_string()),
            complete,
            incomplete: 0,
            peers: peer_addrs,
            failure_reason: None,
            warning_message: None,
        }
    }

    pub async fn scrape(&self, req: TrackerScrapeRequest) -> TrackerScrapeResponse {
        let mut files = FxHashMap::default();
        for ih in &req.info_hashes {
            let peers = PeerRepository::get_peers(&*self.peer_repo, ih, 1000).await;
            files.insert(
                *ih,
                crate::types::ScrapeEntry {
                    complete: peers.len() as i64,
                    downloaded: 0,
                    incomplete: 0,
                    name: None,
                },
            );
        }
        TrackerScrapeResponse {
            files,
            failure_reason: None,
        }
    }

    pub fn peer_repo(&self) -> &Arc<PeerRepoImpl> {
        &self.peer_repo
    }

    pub fn tracker_repo(&self) -> &Arc<TrackerRepoImpl> {
        &self.tracker_repo
    }
}
