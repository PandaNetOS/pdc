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
use crate::storage::node_repo::NodeRepoImpl;
use crate::traits::{AnnounceEvent, PeerDiscoverer};
use crate::types::Infohash;
use pnos_net::types::{DiscoveredNode, DiscoverySource, Reachability as NetReachability};

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
    /// DHT 客户端（优先复用主爬虫实例，路由表更健康）
    dht: Arc<DhtDiscoverer>,
    /// 节点表
    node_table: Arc<NodeTable>,
    /// 主爬虫节点库（用于注入种子节点）
    node_repo: Option<Arc<NodeRepoImpl>>,
    /// 联邦监听端口（announce 时告知其他节点）
    federation_port: u16,
    /// 发现间隔（秒）
    interval_secs: u64,
    /// 关闭信号
    shutdown: broadcast::Sender<()>,
    /// pnos-net 发现事件发送端
    discovered_tx: Option<broadcast::Sender<DiscoveredNode>>,
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
        node_repo: Option<Arc<NodeRepoImpl>>,
        external_dht: Option<Arc<DhtDiscoverer>>,
    ) -> Self {
        let dht = external_dht.unwrap_or_else(|| Arc::new(DhtDiscoverer::new(DhtConfig::default())));
        Self {
            dht,
            node_table,
            node_repo,
            federation_port,
            interval_secs,
            shutdown,
            discovered_tx: None,
        }
    }

    /// 初始化并执行首次发现（种子注入 + DHT init + 首次 discovery_tick），不再自跑循环
    /// 后续周期性调用由 TaskScheduler 触发 discovery_tick()
    pub async fn init_and_first_tick(self: Arc<Self>) {
        // 0. 从主爬虫 NodeRepo 注入种子节点（仅自建 DHT 时）
        if let Some(repo) = &self.node_repo {
            let seeds: Vec<(String, u16)> = repo.top_nodes_sync(2000).into_iter()
                .map(|e| (e.addr.ip().to_string(), e.addr.port()))
                .collect();
            let injected = self.dht.seed_from_entries(&seeds);
            info!("[dht] 从 NodeRepo 注入 {} 个种子节点", injected);
        }

        // 先初始化 DHT（引导到公共 DHT 网络）
        if let Err(e) = self.dht.init().await {
            warn!("[federation] DHT 初始化失败: {}", e);
            return;
        }
        info!(
            "[federation] DHT 魔法 infohash 发现已启动，联邦端口: {}, 间隔: {}s（由 TaskScheduler 调度）",
            self.federation_port, self.interval_secs
        );

        // 立即执行第一次发现
        self.clone().discovery_tick().await;
    }

    /// 单次发现：先 announce 自己，再 get_peers 发现其他节点（由 TaskScheduler 调度）
    pub async fn discovery_tick(self: Arc<Self>) {
        // 迭代式查找 + 同步 announce（保证 announce 与 discover 查询同一批节点，100% 重叠）
        match self
            .dht
            .discover_and_announce(&PDC_FEDERATION_MAGIC_INFOHASH, self.federation_port)
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
                debug!("[federation] DHT discover+announce 失败: {}", e);
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

/// 处理 DHT 发现的 peer 列表：加入节点表 + 发送到 pnos-net 事件通道
fn process_peers(
    peers: &[crate::types::PeerInfo],
    node_table: &Arc<NodeTable>,
    discovered_tx: Option<&broadcast::Sender<DiscoveredNode>>,
) {
    let now = current_unix_secs();
    let mut new_count = 0;
    for peer in peers {
        let addr = peer.addr;
        let temp_id = NodeId::random();
        let node_addr = NodeAddress {
            node_id: temp_id.0,
            ipv4_addr: if addr.is_ipv4() { Some(addr) } else { None },
            ipv6_addr: if addr.is_ipv6() { Some(addr) } else { None },
            reachability: Reachability::Unknown,
            last_seen: now,
            nat_type: None,
        };
        if node_table.add_or_update(node_addr) {
            new_count += 1;
        }
        if let Some(tx) = discovered_tx {
            let discovered = DiscoveredNode {
                node_id: pnos_net::types::NodeId(temp_id.0),
                addresses: vec![addr],
                reachability: NetReachability::Unknown,
                nat_type: None,
                source: DiscoverySource::Dht,
                last_seen: now,
            };
            let _ = tx.send(discovered);
        }
    }
    if new_count > 0 {
        info!(
            "[federation] DHT 发现 {} 个新 PDC 节点，节点表总数: {}",
            new_count,
            node_table.len()
        );
    }
}
