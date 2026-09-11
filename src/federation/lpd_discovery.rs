//! LPD（Local Peer Discovery）局域网多播发现
//!
//! 通过 UDP 多播在局域网内自动发现同一网络中的其他 PDC 联邦节点，
//! 无需手动配置 seed_nodes 即可实现零配置局域网组网。
//!
//! 工作原理：
//! 1. 每个 PDC 节点启动后，每隔固定间隔向 LPD 多播组 announce 自己的身份与端口
//! 2. 同时监听多播组，接收其他节点的 announce
//! 3. 将收到的远端节点加入 NodeTable，由已有的 connection_maintainer 自动尝试连接
//!
//! 消息格式：4 字节魔数 `b"PDCL"` + bincode 序列化的 [`LpdAnnounceMessage`]。

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tracing::{debug, info, trace, warn};

use crate::federation::node_id::{NodeAddress, NodeIdentity, Reachability};
use crate::federation::node_table::NodeTable;

/// LPD 多播消息魔数（4 字节），用于过滤非 PDC 的多播流量
pub const LPD_MAGIC: &[u8; 4] = b"PDCL";

/// 默认 LPD 多播地址（本地管理组）
pub const DEFAULT_LPD_MULTICAST_ADDR: Ipv4Addr = Ipv4Addr::new(239, 255, 43, 21);

/// 默认广播间隔（秒）
pub const DEFAULT_LPD_BROADCAST_INTERVAL_SECS: u64 = 5;

/// LPD 广播消息（bincode 序列化，与联邦协议一致）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LpdAnnounceMessage {
    /// 发送方节点 ID（20字节）
    pub node_id: [u8; 20],
    /// 联邦监听端口（TCP+UDP）
    pub federation_port: u16,
    /// API/HTTP 监控端口
    pub api_port: u16,
    /// 能力位掩码（预留，当前为 0）
    pub capabilities: u32,
    /// 本地数据条目总数（用于对端判断数据完整度）
    pub data_entry_count: u32,
}

impl LpdAnnounceMessage {
    /// 序列化为「魔数 + payload」的完整多播报文
    pub fn to_wire(&self) -> anyhow::Result<Vec<u8>> {
        let payload = bincode::serialize(self)?;
        let mut wire = Vec::with_capacity(LPD_MAGIC.len() + payload.len());
        wire.extend_from_slice(LPD_MAGIC);
        wire.extend_from_slice(&payload);
        Ok(wire)
    }

    /// 从完整多播报文中解析（校验魔数后反序列化）
    pub fn from_wire(data: &[u8]) -> anyhow::Result<Self> {
        if data.len() < LPD_MAGIC.len() || &data[..LPD_MAGIC.len()] != LPD_MAGIC {
            anyhow::bail!("LPD 魔数不匹配，忽略非本协议多播流量");
        }
        let msg = bincode::deserialize(&data[LPD_MAGIC.len()..])?;
        Ok(msg)
    }
}

/// LPD 局域网多播发现服务
pub struct LpdDiscoveryService {
    /// 节点表（发现的新节点加入此处）
    node_table: Arc<NodeTable>,
    /// 本机节点身份
    identity: Arc<NodeIdentity>,
    /// 本机联邦监听端口（announce 时告知其他节点）
    federation_port: u16,
    /// 本机 API/HTTP 监控端口
    api_port: u16,
    /// 多播地址
    multicast_addr: Ipv4Addr,
    /// 多播端口（从配置读取，默认 6771）
    multicast_port: u16,
    /// 广播间隔（秒）
    broadcast_interval_secs: u64,
    /// 关闭信号
    shutdown: broadcast::Sender<()>,
}

impl LpdDiscoveryService {
    /// 创建 LPD 发现服务
    ///
    /// # 参数
    /// - `node_table`: 节点表，发现的新节点会加入此处
    /// - `identity`: 本机节点身份
    /// - `federation_port`: 本机联邦监听端口
    /// - `api_port`: 本机 API/HTTP 监控端口
    /// - `multicast_port`: LPD 多播端口（来自配置 `discoverers.lpd_multicast_port`）
    /// - `shutdown`: 关闭信号发送端
    ///
    /// 多播地址默认 `239.255.43.21`，广播间隔默认 5 秒，可通过
    /// [`with_multicast_addr`](Self::with_multicast_addr) 和
    /// [`with_broadcast_interval`](Self::with_broadcast_interval) 覆盖。
    pub fn new(
        node_table: Arc<NodeTable>,
        identity: Arc<NodeIdentity>,
        federation_port: u16,
        api_port: u16,
        multicast_port: u16,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            node_table,
            identity,
            federation_port,
            api_port,
            multicast_addr: DEFAULT_LPD_MULTICAST_ADDR,
            multicast_port,
            broadcast_interval_secs: DEFAULT_LPD_BROADCAST_INTERVAL_SECS,
            shutdown,
        }
    }

    /// 覆盖默认多播地址
    pub fn with_multicast_addr(mut self, addr: Ipv4Addr) -> Self {
        self.multicast_addr = addr;
        self
    }

    /// 覆盖默认广播间隔（秒）
    pub fn with_broadcast_interval(mut self, secs: u64) -> Self {
        self.broadcast_interval_secs = secs;
        self
    }

    /// 启动后台任务（绑定多播端口 + 加入多播组 + 定期广播 + 接收处理）
    pub fn spawn(self: Arc<Self>) {
        let mut shutdown_rx = self.shutdown.subscribe();
        tokio::spawn(async move {
            // 1. 绑定 0.0.0.0:multicast_port（使用 SO_REUSEADDR 允许多节点同机共享多播端口）
            let bind_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, self.multicast_port));
            let std_socket = match (|| -> anyhow::Result<std::net::UdpSocket> {
                let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
                sock.set_reuse_address(true)?;
                sock.bind(&bind_addr.into())?;
                Ok(sock.into())
            })() {
                Ok(s) => s,
                Err(e) => {
                    warn!("[federation-lpd] 绑定多播端口 {} 失败，LPD 未启动: {}", self.multicast_port, e);
                    return;
                }
            };
            // tokio::net::UdpSocket 不实现 Clone，用 Arc 共享给广播与接收两个分支
            let socket = Arc::new(match UdpSocket::from_std(std_socket) {
                Ok(s) => s,
                Err(e) => {
                    warn!("[federation-lpd] 转换 tokio UDP socket 失败，LPD 未启动: {}", e);
                    return;
                }
            });

            // 2. 加入多播组
            if let Err(e) = socket.join_multicast_v4(self.multicast_addr, Ipv4Addr::UNSPECIFIED) {
                warn!("[federation-lpd] 加入多播组 {} 失败，LPD 未启动: {}", self.multicast_addr, e);
                return;
            }
            // 允许收到自己发出的多播回环（接收循环内会按 node_id 跳过自己）
            let _ = socket.set_multicast_loop_v4(true);

            info!(
                "[federation-lpd] LPD 多播发现已启动: group={}:{}, 联邦端口:{}, API端口:{}, 间隔:{}s",
                self.multicast_addr,
                self.multicast_port,
                self.federation_port,
                self.api_port,
                self.broadcast_interval_secs
            );

            // 3. 多播目的地址
            let dst = SocketAddr::new(std::net::IpAddr::V4(self.multicast_addr), self.multicast_port);

            let mut ticker = tokio::time::interval(Duration::from_secs(self.broadcast_interval_secs));
            let mut buf = vec![0u8; 2048];

            // 4. 主循环：广播 + 接收 + 关闭信号（recv/send 均取 &socket，无需克隆）
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = self.broadcast_once(&socket, &dst).await {
                            debug!("[federation-lpd] 广播失败: {}", e);
                        }
                    }
                    recv_result = socket.recv_from(&mut buf) => {
                        match recv_result {
                            Ok((len, src)) => {
                                self.handle_received_message(&buf[..len], src);
                            }
                            Err(e) => {
                                debug!("[federation-lpd] 接收失败: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation-lpd] 收到关闭信号，退出 LPD 发现任务");
                        break;
                    }
                }
            }

            // socket drop 时 OS 自动离开多播组
            info!("[federation-lpd] LPD 多播发现已停止");
        });
    }

    /// 向多播组发送一次本机 announce
    async fn broadcast_once(&self, socket: &UdpSocket, dst: &SocketAddr) -> anyhow::Result<()> {
        let msg = LpdAnnounceMessage {
            node_id: self.identity.node_id.0,
            federation_port: self.federation_port,
            api_port: self.api_port,
            capabilities: 0,
            data_entry_count: 0,
        };
        let wire = msg.to_wire()?;
        let sent = socket.send_to(&wire, dst).await?;
        trace!(
            "[federation-lpd] 已广播 announce，{} 字节 -> {}",
            sent,
            dst
        );
        Ok(())
    }

    /// 处理一条收到的多播消息（纯逻辑，无网络，便于单元测试直接调用）
    ///
    /// 返回 true 表示成功处理了一个远端节点并加入/更新节点表；
    /// 返回 false 表示消息无效、为自己发出、或源地址非 IPv4。
    pub fn handle_received_message(&self, data: &[u8], src: SocketAddr) -> bool {
        // 1. 校验魔数并反序列化
        let msg = match LpdAnnounceMessage::from_wire(data) {
            Ok(m) => m,
            Err(_) => return false,
        };

        // 2. 跳过自己（比较 node_id）
        if msg.node_id == self.identity.node_id.0 {
            debug!("[federation-lpd] 跳过自己的广播");
            return false;
        }

        // 3. 仅处理 IPv4 源地址（本模块为 IPv4 多播）
        let peer_ip = match src.ip() {
            std::net::IpAddr::V4(ip) => ip,
            std::net::IpAddr::V6(_) => return false,
        };

        // 4. 构造 NodeAddress
        let now = current_unix_secs();
        let node_addr = NodeAddress {
            node_id: msg.node_id,
            ipv4_addr: Some(SocketAddr::new(
                std::net::IpAddr::V4(peer_ip),
                msg.federation_port,
            )),
            ipv6_addr: None,
            reachability: Reachability::Unknown,
            last_seen: now,
            nat_type: None,
        };

        // 5. 加入节点表（connection_maintainer 后续会自动尝试连接）
        let is_new = self.node_table.add_or_update(node_addr);
        if is_new {
            info!(
                "[federation-lpd] 发现局域网新节点: {} ({}:{}), API端口:{}, 节点表总数: {}",
                hex::encode(&msg.node_id[..8]),
                peer_ip,
                msg.federation_port,
                msg.api_port,
                self.node_table.len()
            );
        } else {
            debug!(
                "[federation-lpd] 更新局域网节点: {}:{}",
                peer_ip, msg.federation_port
            );
        }
        true
    }
}

/// 获取当前 Unix 时间戳（秒）
fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::node_id::NodeId;
    use crate::federation::node_table::NodeTable;

    /// 构造一个测试用 LPD 服务（真实 NodeIdentity + 空 NodeTable）
    fn make_test_service(multicast_port: u16) -> LpdDiscoveryService {
        let (shutdown_tx, _rx) = broadcast::channel(1);
        LpdDiscoveryService::new(
            Arc::new(NodeTable::new(100)),
            Arc::new(NodeIdentity::generate()),
            6885,
            6880,
            multicast_port,
            shutdown_tx,
        )
    }

    /// 构造一条模拟远端节点的 LPD 广播报文
    fn make_remote_wire(node_id: [u8; 20], federation_port: u16) -> Vec<u8> {
        let msg = LpdAnnounceMessage {
            node_id,
            federation_port,
            api_port: 9090,
            capabilities: 0,
            data_entry_count: 12345,
        };
        msg.to_wire().unwrap()
    }

    #[test]
    fn test_lpd_message_serde_roundtrip() {
        let msg = LpdAnnounceMessage {
            node_id: [0xAB; 20],
            federation_port: 6885,
            api_port: 6880,
            capabilities: 3,
            data_entry_count: 999,
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: LpdAnnounceMessage = bincode::deserialize(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_lpd_message_with_magic() {
        let msg = LpdAnnounceMessage {
            node_id: [0x11; 20],
            federation_port: 6885,
            api_port: 6880,
            capabilities: 0,
            data_entry_count: 42,
        };
        let wire = msg.to_wire().unwrap();
        // 前 4 字节是魔数
        assert_eq!(&wire[..4], b"PDCL");
        // 正确解析
        let decoded = LpdAnnounceMessage::from_wire(&wire).unwrap();
        assert_eq!(decoded, msg);
        // 魔数被篡改后解析失败
        let mut bad = wire.clone();
        bad[0] = 0x00;
        assert!(LpdAnnounceMessage::from_wire(&bad).is_err());
        // 过短报文解析失败
        assert!(LpdAnnounceMessage::from_wire(b"PD").is_err());
    }

    #[test]
    fn test_service_creation() {
        let svc = make_test_service(6771);
        assert_eq!(svc.multicast_addr, Ipv4Addr::new(239, 255, 43, 21));
        assert_eq!(svc.multicast_port, 6771);
        assert_eq!(svc.broadcast_interval_secs, 5);
        assert_eq!(svc.federation_port, 6885);
        assert_eq!(svc.api_port, 6880);
        assert!(svc.node_table.is_empty());

        // builder 覆盖默认值
        let svc2 = make_test_service(16771)
            .with_multicast_addr(Ipv4Addr::new(239, 255, 43, 99))
            .with_broadcast_interval(10);
        assert_eq!(svc2.multicast_addr, Ipv4Addr::new(239, 255, 43, 99));
        assert_eq!(svc2.multicast_port, 16771);
        assert_eq!(svc2.broadcast_interval_secs, 10);
    }

    #[test]
    fn test_skip_self() {
        let svc = make_test_service(6771);
        // 构造一条 node_id 等于自己的广播
        let self_wire = make_remote_wire(svc.identity.node_id.0, 6885);
        let src: SocketAddr = "192.168.1.100:54321".parse().unwrap();
        let processed = svc.handle_received_message(&self_wire, src);
        assert!(!processed, "自己的广播不应被加入节点表");
        assert!(svc.node_table.is_empty(), "跳过自己后节点表应保持为空");
    }

    #[test]
    fn test_process_remote_node() {
        let svc = make_test_service(6771);
        // 构造一个与自己不同 node_id 的远端节点
        let mut remote_id = [0u8; 20];
        remote_id.copy_from_slice(b"REMOTE_NODE_ID_12345"); // 恰好 20 字节
        let wire = make_remote_wire(remote_id, 6885);
        let src: SocketAddr = "192.168.1.200:54321".parse().unwrap();

        let processed = svc.handle_received_message(&wire, src);
        assert!(processed, "远端节点应被处理并加入节点表");
        assert_eq!(svc.node_table.len(), 1);

        let entry = svc
            .node_table
            .get(&NodeId(remote_id))
            .expect("远端节点应存在于节点表");
        // ipv4 地址应来自源 IP + 消息中的 federation_port
        assert_eq!(
            entry.info.ipv4_addr,
            Some("192.168.1.200:6885".parse().unwrap())
        );
        assert_eq!(entry.info.ipv6_addr, None);
        assert_eq!(entry.info.reachability, Reachability::Unknown);
        assert_eq!(entry.info.nat_type, None);
        assert_eq!(entry.info.node_id, remote_id);
        assert!(entry.info.last_seen > 0);

        // 无效魔数报文不应加入节点表
        let garbage: SocketAddr = "10.0.0.5:1111".parse().unwrap();
        assert!(!svc.handle_received_message(b"XXXXgarbage", garbage));
        assert_eq!(svc.node_table.len(), 1);
    }
}
