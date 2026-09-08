//! ProbeService — 节点探测业务层
//!
//! TCP BT握手 + UDP DHT ping，从 PeerRepo 取、结果写 PeerRepo、成功升级写 NodeRepo。

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::dht::probe::DhtProbe;
use crate::storage::{NodeRepoImpl, PeerRepoImpl};

pub struct ProbeService {
    probe: Arc<DhtProbe>,
    peer_repo: Arc<PeerRepoImpl>,
    node_repo: Arc<NodeRepoImpl>,
    sender: mpsc::UnboundedSender<SocketAddr>,
}

impl ProbeService {
    pub fn new(
        probe: Arc<DhtProbe>,
        peer_repo: Arc<PeerRepoImpl>,
        node_repo: Arc<NodeRepoImpl>,
        sender: mpsc::UnboundedSender<SocketAddr>,
    ) -> Self {
        Self {
            probe,
            peer_repo,
            node_repo,
            sender,
        }
    }

    /// 提交一个地址到探测队列
    pub fn submit(&self, addr: SocketAddr) {
        let _ = self.sender.send(addr);
    }

    pub fn probe(&self) -> &Arc<DhtProbe> {
        &self.probe
    }

    pub fn peer_repo(&self) -> &Arc<PeerRepoImpl> {
        &self.peer_repo
    }

    pub fn node_repo(&self) -> &Arc<NodeRepoImpl> {
        &self.node_repo
    }
}
