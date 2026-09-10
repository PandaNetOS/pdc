//! DHT 魔法 infohash 发现服务
//!
//! 通过 BitTorrent DHT 网络的固定魔法 infohash 发现其他 PDC 联邦节点，
//! 实现零配置自动组网。所有 PDC 节点使用同一个 20 字节魔法 infohash，
//! 定期 announce 自己并 get_peers 发现其他节点。
//!
//! 工作原理：
//! 1. 每个 PDC 节点启动后，定期向魔法 infohash announce 自己的联邦端口
//! 2. 同时定期 get_peers 查询魔法 infohash，获取其他已 announce 的 PDC 节点地址
//! 3. 将发现的地址加入 NodeTable，由 ConnectionManager 自动连接
//!
//! 注意：DhtDiscoverer 是独立实例，使用独立的路由表，不会影响主 DHT 爬虫功能。

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::discoverers::dht::{DhtConfig, DhtDiscoverer};
use crate::federation::node_id::{NodeAddress, NodeId, Reachability};
use crate::federation::node_table::NodeTable;
use crate::traits::{AnnounceEvent, PeerDiscoverer};
use crate::types::Infohash;

/// PDC 联邦网络魔法 infohash
///
/// 前 3 字节 "PDC" = [0x50, 0x44, 0x43]，后续字节为固定标识 "FEDERATION_MAGICV"
/// 所有 PDC 节点必须使用同一个魔法 infohash 才能互相发现。
pub const PDC_FEDERATION_MAGIC_INFOHASH: Infohash = [
    0x50, 0x44, 0x43, 0x46, 0x45, 0x44, 0x45, 0x52, // PDC FEDER
    0x41, 0x54, 0x49, 0x4F, 0x4E, 0x5F, 0x4D, 0x41, // ATION_MA
    0x47, 0x49, 0x43, 0x56, // GICV
];

/// DHT 发现服务
pub struct DhtDiscoveryService {
    /// DHT 客户端（独立实例，不影响主爬虫）
    dht: DhtDiscoverer,
    /// 节点表
    node_table: Arc<NodeTable>,
    /// 联邦监听端口（announce 时告知其他节点）
    federation_port: u16,
    /// 发现间隔（秒）
    interval_secs: u64,
    /// 关闭信号
    shutdown: broadcast::Sender<()>,
}

impl DhtDiscoveryService {
    /// 创建 DHT 发现服务
    ///
    /// # 参数
    /// - `node_table`: 节点表，发现的新节点会加入此处
    /// - `federation_port`: 本机联邦监听端口，announce 到 DHT 网络
    /// - `interval_secs`: 发现周期（秒）
    /// - `shutdown`: 关闭信号发送端
    pub fn new(
        node_table: Arc<NodeTable>,
        federation_port: u16,
        interval_secs: u64,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        let dht = DhtDiscoverer::new(DhtConfig::default());
        Self {
            dht,
            node_table,
            federation_port,
            interval_secs,
            shutdown,
        }
    }

    /// 启动后台发现任务（announce + get_peers 交替执行）
    pub fn spawn(self: Arc<Self>) {
        let mut shutdown_rx = self.shutdown.subscribe();
        tokio::spawn(async move {
            // 先初始化 DHT（引导到公共 DHT 网络）
            if let Err(e) = self.dht.init().await {
                warn!("[federation] DHT 初始化失败: {}", e);
                return;
            }
            info!(
                "[federation] DHT 魔法 infohash 发现已启动，联邦端口: {}, 间隔: {}s",
                self.federation_port, self.interval_secs
            );

            let mut ticker = tokio::time::interval(Duration::from_secs(self.interval_secs));
            ticker.tick().await; // 跳过第一次立即触发

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self.clone().discovery_tick().await;
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] DHT 发现任务收到关闭信号");
                        break;
                    }
                }
            }
        });
    }

    /// 单次发现：先 announce 自己，再 get_peers 发现其他节点
    async fn discovery_tick(self: Arc<Self>) {
        // 1. announce 自己到魔法 infohash，让其他 PDC 节点能找到我们
        if let Err(e) = self
            .dht
            .announce(
                &PDC_FEDERATION_MAGIC_INFOHASH,
                self.federation_port,
                AnnounceEvent::Started,
            )
            .await
        {
            debug!("[federation] DHT announce 失败: {}", e);
        }

        // 2. get_peers 发现其他已 announce 的 PDC 节点
        match self
            .dht
            .discover_peers(&PDC_FEDERATION_MAGIC_INFOHASH, 50)
            .await
        {
            Ok(peers) => {
                let mut new_count = 0;
                for peer in &peers {
                    let addr = peer.addr;
                    // 构造 NodeAddress，node_id 未知用随机占位，连接握手后会更新为真实 ID
                    let temp_id = NodeId::random();
                    let now = current_unix_secs();
                    let node_addr = NodeAddress {
                        node_id: temp_id.0,
                        ipv4_addr: if addr.is_ipv4() { Some(addr) } else { None },
                        ipv6_addr: if addr.is_ipv6() { Some(addr) } else { None },
                        reachability: Reachability::Unknown,
                        last_seen: now,
                        nat_type: None,
                    };
                    if self.node_table.add_or_update(node_addr) {
                        new_count += 1;
                    }
                }
                if new_count > 0 {
                    info!(
                        "[federation] DHT 发现 {} 个新 PDC 节点，节点表总数: {}",
                        new_count,
                        self.node_table.len()
                    );
                }
            }
            Err(e) => {
                debug!("[federation] DHT get_peers 失败: {}", e);
            }
        }
    }
}

/// 获取当前 Unix 时间戳（秒）
fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
