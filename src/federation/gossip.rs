//! Gossip 引擎
//!
//! 基于流行病协议的消息传播。节点将同步数据提交到 outbox，
//! 后台任务定期随机选择 fanout 个邻居传播。已处理消息通过 LRU 去重。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use lru::LruCache;
use parking_lot::RwLock;
use rand::SeedableRng;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::federation::config::FederationConfig;
use crate::federation::connection::{Connection, ConnectionManager};
use crate::federation::merkle::MerkleProvider;
use crate::federation::metrics::FederationMetrics;
use crate::federation::node_id::NodeId;
use crate::federation::protocol::*;

/// Gossip 引擎
pub struct GossipEngine {
    /// 待传播队列
    outbox: RwLock<Vec<GossipBatchMessage>>,
    /// 已处理消息 ID 去重
    seen_msgs: RwLock<LruCache<u64, ()>>,
    /// 连接管理器
    connection_manager: Arc<ConnectionManager>,
    /// 配置
    config: FederationConfig,
    /// 消息 ID 计数器
    next_msg_id: AtomicU64,
    /// 本地节点 ID
    local_node_id: NodeId,
    /// 指标
    metrics: Arc<FederationMetrics>,
    /// 关闭信号
    shutdown: broadcast::Sender<()>,
}

impl GossipEngine {
    /// 创建 Gossip 引擎
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        config: FederationConfig,
        local_node_id: NodeId,
        metrics: Arc<FederationMetrics>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            outbox: RwLock::new(Vec::new()),
            seen_msgs: RwLock::new(LruCache::new(
                std::num::NonZeroUsize::new(10000).unwrap(),
            )),
            connection_manager,
            config,
            next_msg_id: AtomicU64::new(1),
            local_node_id,
            metrics,
            shutdown,
        }
    }

    /// 提交同步数据到 outbox
    pub fn submit_gossip(&self, repo_type: u8, entries: Vec<SyncEntry>) {
        if entries.is_empty() {
            return;
        }
        let msg_id = self.next_msg_id.fetch_add(1, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry_count = entries.len();
        let batch = GossipBatchMessage {
            msg_id,
            origin: self.local_node_id.0,
            repo_type,
            entries,
            timestamp: now,
        };

        // 标记自己发出的消息为已处理（避免回环）
        self.seen_msgs.write().put(msg_id, ());
        self.outbox.write().push(batch);
        debug!("[federation] Gossip 提交: msg_id={}, repo_type={}, entries={}", msg_id, repo_type, entry_count);
    }

    /// 启动 Gossip 传播后台任务
    pub fn spawn_gossip_propagation(self: Arc<Self>) {
        let interval = Duration::from_millis(self.config.gossip_interval_ms);
        let fanout = self.config.gossip_fanout;
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // 跳过第一次

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self.clone().propagation_tick(fanout).await;
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] Gossip 传播任务收到关闭信号");
                        break;
                    }
                }
            }
        });
        debug!("[federation] Gossip 传播任务已启动（间隔 {}ms, fanout={}）", interval.as_millis(), fanout);
    }

    /// 单次传播：从 outbox 取一批，随机选 fanout 个邻居发送
    async fn propagation_tick(self: Arc<Self>, fanout: usize) {
        // 取出最多 10 条待传播消息
        let batches: Vec<GossipBatchMessage> = {
            let mut outbox = self.outbox.write();
            if outbox.is_empty() {
                return;
            }
            let count = outbox.len().min(10);
            outbox.drain(..count).collect()
        };

        if batches.is_empty() {
            return;
        }

        // 随机选择 fanout 个已连接邻居（先完成所有随机选择，避免 rng 跨 await）
        let conns = self.connection_manager.all_connections();
        if conns.is_empty() {
            // 无连接，把消息放回 outbox（下次重试）
            self.outbox.write().extend(batches);
            return;
        }

        // 收集所有待发送的 (connection, batch) 对（rng 在此作用域内使用完毕）
        let send_targets: Vec<(Arc<Connection>, &GossipBatchMessage)> = {
            use rand::seq::SliceRandom;
            let mut rng = rand::rngs::StdRng::from_entropy();
            let mut targets = Vec::new();
            for batch in &batches {
                let filtered: Vec<_> = conns
                    .iter()
                    .filter(|c| NodeId(batch.origin) != c.node_id)
                    .cloned()
                    .collect();
                let selected: Vec<_> = filtered
                    .choose_multiple(&mut rng, fanout.min(filtered.len()))
                    .cloned()
                    .collect();
                for conn in selected {
                    targets.push((conn, batch));
                }
            }
            targets
        };

        for (conn, batch) in send_targets {
            if let Err(e) = conn.send_message(MessageType::GossipBatch, batch).await {
                debug!("[federation] Gossip 发送到 {} 失败: {}", conn.node_id, e);
            } else {
                self.metrics.record_gossip_propagation();
                self.metrics.record_message_sent();
            }
        }

        debug!("[federation] Gossip 传播: {} 条消息发送到 {} 个邻居", batches.len(), fanout);
    }

    /// 处理收到的 Gossip 消息
    ///
    /// 检查 msg_id 去重，未处理则加入 seen_msgs，返回 entries 供上层写入本地，
    /// 并将 batch 加入 outbox 继续转发。
    pub fn handle_gossip_batch(&self, batch: GossipBatchMessage) -> Vec<SyncEntry> {
        self.metrics.record_gossip_received();
        self.metrics.record_message_recv();

        let mut seen = self.seen_msgs.write();
        if seen.contains(&batch.msg_id) {
            debug!("[federation] Gossip 消息已处理，跳过: msg_id={}", batch.msg_id);
            return Vec::new();
        }
        seen.put(batch.msg_id, ());
        drop(seen);

        let entries = batch.entries.clone();
        debug!(
            "[federation] Gossip 收到新消息: msg_id={}, origin={}, repo_type={}, entries={}",
            batch.msg_id,
            NodeId(batch.origin),
            batch.repo_type,
            entries.len()
        );

        // 加入 outbox 继续传播（流行病协议）
        self.outbox.write().push(batch);

        entries
    }

    /// 启动反熵（anti-entropy）后台任务
    ///
    /// 每60秒随机选1个邻居，发送 MerkleDigest 进行对账。
    pub fn spawn_anti_entropy<M: MerkleProvider + 'static>(self: Arc<Self>, merkle_provider: Arc<M>) {
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(60));
            ticker.tick().await; // 跳过第一次

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self.clone().anti_entropy_tick(merkle_provider.clone()).await;
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] 反熵任务收到关闭信号");
                        break;
                    }
                }
            }
        });
        debug!("[federation] 反熵任务已启动（间隔 60s）");
    }

    /// 单次反熵：随机选1个邻居，发送所有 repo_type 的 MerkleDigest
    async fn anti_entropy_tick<M: MerkleProvider>(self: Arc<Self>, merkle_provider: Arc<M>) {
        let conns = self.connection_manager.all_connections();
        if conns.is_empty() {
            return;
        }

        use rand::seq::SliceRandom;
        let mut rng = rand::rngs::StdRng::from_entropy();
        let conn = match conns.choose(&mut rng) {
            Some(c) => c.clone(),
            None => return,
        };

        // 发送 Node repo 的 MerkleDigest（阶段2主要同步 Node/Peer/Infohash）
        for repo_type in &[repo_type::NODE, repo_type::PEER, repo_type::INFOHASH] {
            let digest = merkle_provider.get_digest(*repo_type);
            if let Err(e) = conn.send_message(MessageType::MerkleDigest, &digest).await {
                debug!("[federation] 反熵 MerkleDigest 发送失败: {}", e);
            } else {
                self.metrics.record_message_sent();
            }
        }

        debug!("[federation] 反熵对账发送到 {}", conn.node_id);
    }

    /// outbox 大小
    pub fn outbox_size(&self) -> usize {
        self.outbox.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::node_id::NodeIdentity;
    use crate::federation::node_table::NodeTable;

    fn make_config() -> FederationConfig {
        FederationConfig {
            enabled: true,
            listen_port: 0,
            max_connections: 10,
            gossip_interval_ms: 100,
            gossip_fanout: 3,
            ..Default::default()
        }
    }

    #[test]
    fn test_submit_and_handle_gossip() {
        let identity = NodeIdentity::generate();
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cm_shutdown, _) = broadcast::channel(1);
        let cm = Arc::new(ConnectionManager::new(
            node_table,
            Arc::new(identity),
            make_config(),
            cm_shutdown,
        ));
        let metrics = Arc::new(FederationMetrics::new());
        let engine = GossipEngine::new(
            cm,
            make_config(),
            NodeId([1; 20]),
            metrics,
            shutdown_tx,
        );

        let entries = vec![
            SyncEntry { key: b"k1".to_vec(), operation: 0, version: 1, payload: vec![1] },
        ];
        engine.submit_gossip(repo_type::NODE, entries.clone());
        assert_eq!(engine.outbox_size(), 1);

        // 构造一个收到的 batch（不同 msg_id）
        let batch = GossipBatchMessage {
            msg_id: 999,
            origin: [2; 20],
            repo_type: repo_type::NODE,
            entries: entries.clone(),
            timestamp: 100,
        };
        let result = engine.handle_gossip_batch(batch);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, b"k1");

        // 重复处理应返回空
        let batch2 = GossipBatchMessage {
            msg_id: 999,
            origin: [2; 20],
            repo_type: repo_type::NODE,
            entries,
            timestamp: 100,
        };
        let result2 = engine.handle_gossip_batch(batch2);
        assert!(result2.is_empty());
    }

    #[test]
    fn test_submit_empty_entries() {
        let identity = NodeIdentity::generate();
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cm_shutdown, _) = broadcast::channel(1);
        let cm = Arc::new(ConnectionManager::new(
            node_table,
            Arc::new(identity),
            make_config(),
            cm_shutdown,
        ));
        let metrics = Arc::new(FederationMetrics::new());
        let engine = GossipEngine::new(
            cm,
            make_config(),
            NodeId([1; 20]),
            metrics,
            shutdown_tx,
        );

        engine.submit_gossip(repo_type::NODE, vec![]);
        assert_eq!(engine.outbox_size(), 0);
    }

    #[test]
    fn test_msg_id_increment() {
        let identity = NodeIdentity::generate();
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cm_shutdown, _) = broadcast::channel(1);
        let cm = Arc::new(ConnectionManager::new(
            node_table,
            Arc::new(identity),
            make_config(),
            cm_shutdown,
        ));
        let metrics = Arc::new(FederationMetrics::new());
        let engine = GossipEngine::new(
            cm,
            make_config(),
            NodeId([1; 20]),
            metrics,
            shutdown_tx,
        );

        let entries = vec![SyncEntry { key: b"k".to_vec(), operation: 0, version: 1, payload: vec![] }];
        engine.submit_gossip(1, entries.clone());
        engine.submit_gossip(1, entries.clone());
        engine.submit_gossip(1, entries);
        assert_eq!(engine.outbox_size(), 3);
    }
}
