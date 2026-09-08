//! PexService — PEX 交换业务层

use std::net::SocketAddr;
use std::sync::Arc;

use crate::discoverers::pex::client::PexDiscoverer;
use crate::storage::PeerRepoImpl;

pub struct PexService {
    pex: Arc<PexDiscoverer>,
    peer_repo: Arc<PeerRepoImpl>,
}

impl PexService {
    pub fn new(pex: Arc<PexDiscoverer>, peer_repo: Arc<PeerRepoImpl>) -> Self {
        Self { pex, peer_repo }
    }

    /// 把 peer 加入 PEX 池
    pub fn add_peer(&self, addr: SocketAddr) {
        self.pex.add_connected_peer(addr, None);
    }

    pub fn pex(&self) -> &Arc<PexDiscoverer> {
        &self.pex
    }

    pub fn peer_repo(&self) -> &Arc<PeerRepoImpl> {
        &self.peer_repo
    }
}
