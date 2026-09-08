//! CrawlerService — DHT 爬虫业务层

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::crawler::engine::{CrawlerEngine, CrawlerState};
use crate::dns_pool::DnsPool;
use crate::storage::NodeRepoImpl;

pub struct CrawlerService {
    engine: Arc<RwLock<CrawlerEngine>>,
    node_repo: Arc<NodeRepoImpl>,
    dns_pool: Arc<DnsPool>,
}

impl CrawlerService {
    pub fn new(
        engine: Arc<RwLock<CrawlerEngine>>,
        node_repo: Arc<NodeRepoImpl>,
        dns_pool: Arc<DnsPool>,
    ) -> Self {
        Self {
            engine,
            node_repo,
            dns_pool,
        }
    }

    pub fn engine(&self) -> &Arc<RwLock<CrawlerEngine>> {
        &self.engine
    }

    pub fn node_repo(&self) -> &Arc<NodeRepoImpl> {
        &self.node_repo
    }

    pub fn dns_pool(&self) -> &Arc<DnsPool> {
        &self.dns_pool
    }

    pub async fn stats(&self) -> CrawlerStats {
        let engine = self.engine.read().await;
        let state: CrawlerState = engine.state();
        let rt = engine.routing_table();
        let nodes_total = rt.read().len();

        CrawlerStats {
            nodes_total,
            requests_sent: state.requests_sent,
            responses_received: state.messages_received,
            infohashes_collected: state.infohashes_collected,
            peers_collected: state.peers_collected,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CrawlerStats {
    pub nodes_total: usize,
    pub requests_sent: u64,
    pub responses_received: u64,
    pub infohashes_collected: u64,
    pub peers_collected: u64,
}
