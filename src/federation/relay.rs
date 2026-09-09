//! 数据中继
//!
//! 阶段3核心模块。为无法直连的节点提供 TCP 数据中继，
//! 支持令牌桶带宽限流和过载保护。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::federation::config::FederationConfig;
use crate::federation::connection::ConnectionManager;
use crate::federation::metrics::FederationMetrics;
use crate::federation::node_id::{NodeIdentity, NodeId};
use crate::federation::protocol::*;

/// 中继动作
pub mod relay_action {
    pub const REQUEST: u8 = 0;
    pub const ACCEPT: u8 = 1;
    pub const REJECT: u8 = 2;
    pub const CLOSE: u8 = 3;
}

/// 中继通道
struct RelayChannel {
    channel_id: u64,
    source_node: NodeId,
    target_node: NodeId,
    created_at: Instant,
    last_activity: RwLock<Instant>,
    bytes_forwarded: AtomicU64,
    active: AtomicBool,
}

impl RelayChannel {
    fn new(channel_id: u64, source_node: NodeId, target_node: NodeId) -> Self {
        Self {
            channel_id,
            source_node,
            target_node,
            created_at: Instant::now(),
            last_activity: RwLock::new(Instant::now()),
            bytes_forwarded: AtomicU64::new(0),
            active: AtomicBool::new(true),
        }
    }

    fn touch(&self) {
        *self.last_activity.write() = Instant::now();
    }

    fn idle_duration(&self) -> Duration {
        self.last_activity.read().elapsed()
    }

    fn record_bytes(&self, bytes: u64) {
        self.bytes_forwarded.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// 带宽跟踪器（滑动窗口令牌桶）
pub struct BandwidthTracker {
    total_bytes: AtomicU64,
    window_start: RwLock<Instant>,
    /// 总带宽限制（字节/秒）
    limit_bytes_per_sec: u64,
    /// 单连接带宽限制（字节/秒）
    per_conn_limit: u64,
    per_conn_bytes: RwLock<FxHashMap<u64, u64>>,
    per_conn_window: RwLock<FxHashMap<u64, Instant>>,
}

impl BandwidthTracker {
    pub fn new(limit_mbps: u32, per_conn_mbps: u32) -> Self {
        Self {
            total_bytes: AtomicU64::new(0),
            window_start: RwLock::new(Instant::now()),
            limit_bytes_per_sec: (limit_mbps as u64) * 1024 * 1024 / 8,
            per_conn_limit: (per_conn_mbps as u64) * 1024 * 1024 / 8,
            per_conn_bytes: RwLock::new(FxHashMap::default()),
            per_conn_window: RwLock::new(FxHashMap::default()),
        }
    }

    /// 尝试消费带宽，返回 true 表示允许
    pub fn try_consume(&self, bytes: u64, channel_id: u64) -> bool {
        let now = Instant::now();

        // 全局窗口检查
        {
            let mut window_start = self.window_start.write();
            if now.duration_since(*window_start) >= Duration::from_secs(1) {
                *window_start = now;
                self.total_bytes.store(0, Ordering::Relaxed);
            }
        }

        let current_total = self.total_bytes.load(Ordering::Relaxed);
        if current_total + bytes > self.limit_bytes_per_sec {
            return false;
        }

        // 单连接窗口检查
        {
            let mut conn_window = self.per_conn_window.write();
            let conn_start = conn_window.entry(channel_id).or_insert(now);
            if now.duration_since(*conn_start) >= Duration::from_secs(1) {
                *conn_start = now;
                self.per_conn_bytes.write().insert(channel_id, 0);
            }
        }

        let mut conn_bytes = self.per_conn_bytes.write();
        let current_conn = conn_bytes.entry(channel_id).or_insert(0);
        if *current_conn + bytes > self.per_conn_limit {
            return false;
        }

        // 通过检查，消费带宽
        *current_conn += bytes;
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        true
    }

    /// 当前总带宽使用（字节/秒，近似值）
    pub fn current_bandwidth_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }
}

/// 中继管理器
pub struct RelayManager {
    channels: RwLock<FxHashMap<u64, Arc<RelayChannel>>>,
    connection_manager: Arc<ConnectionManager>,
    identity: Arc<NodeIdentity>,
    config: FederationConfig,
    metrics: Arc<FederationMetrics>,
    bandwidth_tracker: Arc<BandwidthTracker>,
    next_channel_id: AtomicU64,
    shutdown: broadcast::Sender<()>,
}

impl RelayManager {
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        identity: Arc<NodeIdentity>,
        config: FederationConfig,
        metrics: Arc<FederationMetrics>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        let bandwidth_tracker = Arc::new(BandwidthTracker::new(
            config.relay_bandwidth_limit_mbps,
            2, // 单连接默认 2Mbps
        ));
        Self {
            channels: RwLock::new(FxHashMap::default()),
            connection_manager,
            identity,
            config,
            metrics,
            bandwidth_tracker,
            next_channel_id: AtomicU64::new(1),
            shutdown,
        }
    }

    /// 发起中继建立
    pub fn setup_relay(&self, target_node: NodeId) -> anyhow::Result<u64> {
        if !self.config.enable_relay {
            anyhow::bail!("中继未启用");
        }

        // 过载检查
        if self.active_channel_count() >= self.config.relay_max_connections {
            anyhow::bail!("中继通道已满（{}）", self.config.relay_max_connections);
        }

        let channel_id = self.next_channel_id.fetch_add(1, Ordering::Relaxed);
        let channel = Arc::new(RelayChannel::new(channel_id, self.identity.node_id, target_node));
        self.channels.write().insert(channel_id, channel);

        // 发送 RelaySetup Request
        let msg = RelaySetupMessage {
            channel_id,
            target_node: target_node.0,
            action: relay_action::REQUEST,
        };

        if let Some(conn) = self.connection_manager.get_connection(&target_node) {
            let cm = self.connection_manager.clone();
            tokio::spawn(async move {
                let _ = conn.send_message(MessageType::RelaySetup, &msg).await;
            });
        } else {
            // 目标不直连，需要通过其他节点转发（阶段3简化：直接报错）
            self.channels.write().remove(&channel_id);
            anyhow::bail!("目标节点 {} 无直连，中继需要多跳（暂不支持）", target_node);
        }

        info!(
            "[federation] 发起中继 channel={}, target={}",
            channel_id, target_node
        );
        Ok(channel_id)
    }

    /// 处理中继建立消息
    pub fn handle_relay_setup(&self, from_node: NodeId, msg: RelaySetupMessage) {
        match msg.action {
            relay_action::REQUEST => {
                self.handle_relay_request(from_node, msg);
            }
            relay_action::ACCEPT => {
                self.handle_relay_accept(from_node, msg);
            }
            relay_action::REJECT => {
                self.handle_relay_reject(from_node, msg);
            }
            relay_action::CLOSE => {
                self.handle_relay_close(from_node, msg);
            }
            _ => {}
        }
    }

    fn handle_relay_request(&self, from_node: NodeId, msg: RelaySetupMessage) {
        // 过载检查
        if self.active_channel_count() >= self.config.relay_max_connections {
            warn!(
                "[federation] 中继请求被拒（过载）: channel={}, from={}",
                msg.channel_id, from_node
            );
            let reject = RelaySetupMessage {
                channel_id: msg.channel_id,
                target_node: from_node.0,
                action: relay_action::REJECT,
            };
            if let Some(conn) = self.connection_manager.get_connection(&from_node) {
                let cm = self.connection_manager.clone();
                tokio::spawn(async move {
                    let _ = conn.send_message(MessageType::RelaySetup, &reject).await;
                });
            }
            return;
        }

        // 接受中继
        let channel = Arc::new(RelayChannel::new(msg.channel_id, from_node, NodeId(msg.target_node)));
        self.channels.write().insert(msg.channel_id, channel);

        let accept = RelaySetupMessage {
            channel_id: msg.channel_id,
            target_node: from_node.0,
            action: relay_action::ACCEPT,
        };
        if let Some(conn) = self.connection_manager.get_connection(&from_node) {
            let cm = self.connection_manager.clone();
            tokio::spawn(async move {
                let _ = conn.send_message(MessageType::RelaySetup, &accept).await;
            });
        }

        info!(
            "[federation] 中继请求已接受: channel={}, from={}",
            msg.channel_id, from_node
        );
    }

    fn handle_relay_accept(&self, from_node: NodeId, msg: RelaySetupMessage) {
        if let Some(channel) = self.channels.read().get(&msg.channel_id) {
            channel.touch();
            info!(
                "[federation] 中继已建立: channel={}, target={}",
                msg.channel_id, from_node
            );
        }
    }

    fn handle_relay_reject(&self, from_node: NodeId, msg: RelaySetupMessage) {
        self.channels.write().remove(&msg.channel_id);
        warn!(
            "[federation] 中继被拒绝: channel={}, from={}",
            msg.channel_id, from_node
        );
    }

    fn handle_relay_close(&self, _from_node: NodeId, msg: RelaySetupMessage) {
        if let Some(channel) = self.channels.write().remove(&msg.channel_id) {
            let bytes = channel.bytes_forwarded.load(Ordering::Relaxed);
            info!(
                "[federation] 中继关闭: channel={}, 转发 {} 字节",
                msg.channel_id, bytes
            );
        }
    }

    /// 处理中继数据
    pub fn handle_relay_data(&self, from_node: NodeId, msg: RelayDataMessage) {
        let channel = {
            let channels = self.channels.read();
            match channels.get(&msg.channel_id) {
                Some(c) => c.clone(),
                None => {
                    debug!("[federation] 中继数据到达未知通道: channel={}", msg.channel_id);
                    return;
                }
            }
        };

        // 确认来源
        if channel.source_node != from_node && channel.target_node != from_node {
            warn!(
                "[federation] 中继数据来源不匹配: channel={}, from={}",
                msg.channel_id, from_node
            );
            return;
        }

        // 带宽限流
        let data_len = msg.data.len() as u64;
        if !self.bandwidth_tracker.try_consume(data_len, msg.channel_id) {
            debug!(
                "[federation] 中继数据被限流丢弃: channel={}, {} bytes",
                msg.channel_id, data_len
            );
            return;
        }

        // 确定转发目标
        let forward_to = if from_node == channel.source_node {
            channel.target_node
        } else {
            channel.source_node
        };

        // 转发
        if let Some(conn) = self.connection_manager.get_connection(&forward_to) {
            let forward_msg = RelayDataMessage {
                channel_id: msg.channel_id,
                data: msg.data,
            };
            let cm = self.connection_manager.clone();
            tokio::spawn(async move {
                if let Err(e) = conn.send_message(MessageType::RelayData, &forward_msg).await {
                    debug!("[federation] 中继转发失败: {}", e);
                }
            });
            channel.touch();
            channel.record_bytes(data_len);
            self.metrics.record_relay_bytes(data_len);
        } else {
            debug!(
                "[federation] 中继转发目标无连接: channel={}, target={}",
                msg.channel_id, forward_to
            );
        }
    }

    /// 关闭中继通道
    pub fn close_channel(&self, channel_id: u64) {
        if let Some(channel) = self.channels.write().remove(&channel_id) {
            let close_msg = RelaySetupMessage {
                channel_id,
                target_node: channel.target_node.0,
                action: relay_action::CLOSE,
            };
            if let Some(conn) = self.connection_manager.get_connection(&channel.target_node) {
                let cm = self.connection_manager.clone();
                tokio::spawn(async move {
                    let _ = conn.send_message(MessageType::RelaySetup, &close_msg).await;
                });
            }
            info!("[federation] 中继通道关闭: channel={}", channel_id);
        }
    }

    /// 关闭所有通道
    pub fn close_all(&self) {
        let channel_ids: Vec<u64> = self.channels.read().keys().cloned().collect();
        for id in channel_ids {
            self.close_channel(id);
        }
    }

    /// 活跃通道数
    pub fn active_channel_count(&self) -> usize {
        self.channels
            .read()
            .values()
            .filter(|c| c.active.load(Ordering::Relaxed))
            .count()
    }

    /// 总转发字节数
    pub fn total_bytes_forwarded(&self) -> u64 {
        self.channels
            .read()
            .values()
            .map(|c| c.bytes_forwarded.load(Ordering::Relaxed))
            .sum()
    }

    /// 当前带宽（Mbps）
    pub fn current_bandwidth_mbps(&self) -> f64 {
        let bytes = self.bandwidth_tracker.current_bandwidth_bytes();
        (bytes as f64 * 8.0) / (1024.0 * 1024.0)
    }

    /// 启动通道清理后台任务
    pub fn spawn_channel_cleanup(self: Arc<Self>) {
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self.clone().cleanup_expired();
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] 中继清理任务收到关闭信号");
                        break;
                    }
                }
            }
        });
        info!("[federation] 中继通道清理任务已启动（间隔 30s，超时 120s）");
    }

    /// 清理超时通道（>120秒无活动）
    fn cleanup_expired(self: Arc<Self>) {
        let expired: Vec<u64> = {
            let channels = self.channels.read();
            channels
                .iter()
                .filter(|(_, c)| c.idle_duration() > Duration::from_secs(120))
                .map(|(id, _)| *id)
                .collect()
        };

        let count = expired.len();
        for id in &expired {
            self.close_channel(*id);
        }

        if count > 0 {
            debug!("[federation] 清理 {} 个超时中继通道", count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::node_table::NodeTable;

    fn make_config() -> FederationConfig {
        FederationConfig {
            enabled: true,
            listen_port: 0,
            max_connections: 10,
            enable_relay: true,
            relay_max_connections: 5,
            relay_bandwidth_limit_mbps: 10,
            ..Default::default()
        }
    }

    fn make_relay_manager() -> Arc<RelayManager> {
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cm_shutdown, _) = broadcast::channel(1);
        let cm = Arc::new(ConnectionManager::new(
            node_table,
            identity.clone(),
            make_config(),
            cm_shutdown,
        ));
        let metrics = Arc::new(FederationMetrics::new());
        Arc::new(RelayManager::new(cm, identity, make_config(), metrics, shutdown_tx))
    }

    #[test]
    fn test_bandwidth_tracker_basic() {
        let tracker = BandwidthTracker::new(10, 2);
        // 小数据包应该通过
        assert!(tracker.try_consume(100, 1));
        assert!(tracker.try_consume(200, 1));
    }

    #[test]
    fn test_bandwidth_tracker_total_limit() {
        // 1 Mbps 总限制 = 131072 字节/秒
        let tracker = BandwidthTracker::new(1, 10);
        assert!(tracker.try_consume(100000, 1));
        // 超出总限制
        assert!(!tracker.try_consume(100000, 2));
    }

    #[test]
    fn test_bandwidth_tracker_per_conn_limit() {
        // 单连接 1 Mbps
        let tracker = BandwidthTracker::new(100, 1);
        assert!(tracker.try_consume(100000, 1));
        // 同一连接超出限制
        assert!(!tracker.try_consume(100000, 1));
        // 不同连接可以通过
        assert!(tracker.try_consume(100000, 2));
    }

    #[test]
    fn test_relay_manager_creation() {
        let relay = make_relay_manager();
        assert_eq!(relay.active_channel_count(), 0);
        assert_eq!(relay.total_bytes_forwarded(), 0);
    }

    #[test]
    fn test_relay_setup_relay_disabled() {
        let mut config = make_config();
        config.enable_relay = false;
        let identity = Arc::new(NodeIdentity::generate());
        let node_table = Arc::new(NodeTable::new(100));
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cm_shutdown, _) = broadcast::channel(1);
        let cm = Arc::new(ConnectionManager::new(
            node_table,
            identity.clone(),
            config.clone(),
            cm_shutdown,
        ));
        let metrics = Arc::new(FederationMetrics::new());
        let relay = RelayManager::new(cm, identity, config, metrics, shutdown_tx);

        let result = relay.setup_relay(NodeId([2; 20]));
        assert!(result.is_err());
    }

    #[test]
    fn test_relay_handle_accept_and_close() {
        let relay = make_relay_manager();
        let from = NodeId([1; 20]);

        // 模拟收到中继请求
        let request = RelaySetupMessage {
            channel_id: 100,
            target_node: [3; 20],
            action: relay_action::REQUEST,
        };
        relay.handle_relay_setup(from, request);
        assert_eq!(relay.active_channel_count(), 1);

        // 关闭
        relay.close_channel(100);
        assert_eq!(relay.active_channel_count(), 0);
    }

    #[test]
    fn test_relay_handle_reject() {
        let relay = make_relay_manager();
        let from = NodeId([1; 20]);

        // 先建立通道
        let request = RelaySetupMessage {
            channel_id: 200,
            target_node: [3; 20],
            action: relay_action::REQUEST,
        };
        relay.handle_relay_setup(from, request);
        assert_eq!(relay.active_channel_count(), 1);

        // 收到拒绝（从目标端）
        let reject = RelaySetupMessage {
            channel_id: 200,
            target_node: [1; 20],
            action: relay_action::REJECT,
        };
        relay.handle_relay_setup(NodeId([3; 20]), reject);
        assert_eq!(relay.active_channel_count(), 0);
    }

    #[test]
    fn test_relay_data_unknown_channel() {
        let relay = make_relay_manager();
        // 未知通道不应 panic
        let data = RelayDataMessage {
            channel_id: 999,
            data: vec![1, 2, 3],
        };
        relay.handle_relay_data(NodeId([1; 20]), data);
    }

    #[test]
    fn test_relay_action_constants() {
        assert_eq!(relay_action::REQUEST, 0);
        assert_eq!(relay_action::ACCEPT, 1);
        assert_eq!(relay_action::REJECT, 2);
        assert_eq!(relay_action::CLOSE, 3);
    }

    #[test]
    fn test_relay_setup_message_serde() {
        let msg = RelaySetupMessage {
            channel_id: 42,
            target_node: [1; 20],
            action: relay_action::REQUEST,
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: RelaySetupMessage = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.channel_id, 42);
        assert_eq!(decoded.action, 0);
    }

    #[test]
    fn test_relay_data_message_serde() {
        let msg = RelayDataMessage {
            channel_id: 10,
            data: vec![1, 2, 3, 4, 5],
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: RelayDataMessage = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.channel_id, 10);
        assert_eq!(decoded.data, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_relay_channel_idle_duration() {
        let channel = RelayChannel::new(1, NodeId([1; 20]), NodeId([2; 20]));
        assert!(channel.idle_duration().as_secs() < 1);
        channel.touch();
        assert!(channel.idle_duration().as_secs() < 1);
    }

    #[test]
    fn test_relay_bandwidth_tracker_limit() {
        let tracker = BandwidthTracker::new(10, 2);
        // 小流量应通过
        assert!(tracker.try_consume(100, 1));
        // 超大流量应被拒绝（10Mbps = ~1.3MB/s，100MB远超限制）
        assert!(!tracker.try_consume(100 * 1024 * 1024, 1));
    }

    #[test]
    fn test_relay_close_all() {
        let relay = make_relay_manager();
        let from = NodeId([1; 20]);

        // 建立多个通道
        for i in 0..3 {
            let request = RelaySetupMessage {
                channel_id: i,
                target_node: [3; 20],
                action: relay_action::REQUEST,
            };
            relay.handle_relay_setup(from, request);
        }
        assert_eq!(relay.active_channel_count(), 3);

        relay.close_all();
        assert_eq!(relay.active_channel_count(), 0);
    }
}
