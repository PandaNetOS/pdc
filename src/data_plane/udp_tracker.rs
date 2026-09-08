//! UDP Tracker 服务端（BEP 15）
//!
//! 实现 UDP Tracker 协议，对 qBittorrent 等客户端暴露标准 UDP Tracker 接口。
//! 与 HTTP Tracker 共享 SuperTrackerState 的 peer 存储。
//!
//! 协议：
//! - connect: 16 字节请求 → 16 字节响应（含 connection_id）
//! - announce: 98 字节请求 → 动态响应（含 compact peers）
//! - scrape: 可选（暂未实现）

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::net::UdpSocket;
use tracing::{debug, info, warn};

use crate::storage::PeerRepoImpl;
use crate::config::SuperTrackerConfig;
use crate::data_plane::http_tracker::SuperTrackerState;
use crate::types::AnnounceEvent;

// BEP 15 协议常量
const PROTOCOL_ID: u64 = 0x41727101980;
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
const ACTION_SCRAPE: u32 = 2;
const ACTION_ERROR: u32 = 3;

/// connection_id 条目
struct ConnectionEntry {
    /// 关联的远程地址（验证用）
    remote_addr: SocketAddr,
    /// 创建时间
    created_at: Instant,
}

/// UDP Tracker 服务端
pub struct UdpTrackerServer {
    /// 监听地址
    listen_addr: SocketAddr,
    /// 超级 Tracker 状态（共享）
    super_tracker: Arc<SuperTrackerState>,
    /// Peer 缓存（共享）
    cache: Arc<PeerRepoImpl>,
    /// Infohash 仓库（可选，统一数据归口）
    infohash_repo: Option<Arc<crate::storage::InfohashRepoImpl>>,
    /// 配置
    config: SuperTrackerConfig,
    /// connection_id 映射
    connections: Arc<RwLock<HashMap<u64, ConnectionEntry>>>,
}

impl UdpTrackerServer {
    /// 创建新的 UDP Tracker 服务端
    pub fn new(
        listen_addr: SocketAddr,
        super_tracker: Arc<SuperTrackerState>,
        cache: Arc<PeerRepoImpl>,
        config: SuperTrackerConfig,
    ) -> Self {
        Self {
            listen_addr,
            super_tracker,
            cache,
            infohash_repo: None,
            config,
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 设置 Infohash 仓库
    pub fn with_infohash_repo(mut self, repo: Arc<crate::storage::InfohashRepoImpl>) -> Self {
        self.infohash_repo = Some(repo);
        self
    }

    /// 启动 UDP Tracker 服务端（后台运行）
    pub async fn start(&self) -> anyhow::Result<()> {
        let socket = Arc::new(UdpSocket::bind(self.listen_addr).await?);
        info!(
            "[udp_tracker] UDP Tracker 服务端启动，监听 {}",
            self.listen_addr
        );

        let super_tracker = self.super_tracker.clone();
        let cache = self.cache.clone();
        let infohash_repo = self.infohash_repo.clone();
        let config = self.config.clone();
        let connections = self.connections.clone();
        let interval = config.interval.max(0) as u32;

        // 启动过期清理任务
        let conn_clone = connections.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let mut conns = conn_clone.write();
                let before = conns.len();
                conns.retain(|_, entry| entry.created_at.elapsed() < Duration::from_secs(120));
                let removed = before - conns.len();
                if removed > 0 {
                    debug!("[udp_tracker] 清理了 {} 个过期 connection_id", removed);
                }
            }
        });

        let mut buf = vec![0u8; 2048];
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((n, from)) => {
                    let data = buf[..n].to_vec();
                    let socket = socket.clone();
                    let super_tracker = super_tracker.clone();
                    let cache = cache.clone();
                    let infohash_repo = infohash_repo.clone();
                    let connections = connections.clone();

                    tokio::spawn(async move {
                        if let Some(response) = Self::handle_packet(
                            &data,
                            from,
                            &super_tracker,
                            &cache,
                            infohash_repo.as_ref(),
                            &connections,
                            interval,
                        )
                        .await
                        {
                            if let Err(e) = socket.send_to(&response, from).await {
                                debug!("[udp_tracker] 发送响应失败: {}", e);
                            }
                        }
                    });
                }
                Err(e) => {
                    warn!("[udp_tracker] 接收失败: {}", e);
                }
            }
        }
    }

    /// 处理单个 UDP 数据包
    async fn handle_packet(
        data: &[u8],
        from: SocketAddr,
        super_tracker: &SuperTrackerState,
        cache: &PeerRepoImpl,
        infohash_repo: Option<&Arc<crate::storage::InfohashRepoImpl>>,
        connections: &RwLock<HashMap<u64, ConnectionEntry>>,
        interval: u32,
    ) -> Option<Vec<u8>> {
        if data.len() < 16 {
            return None;
        }

        let connection_id = u64::from_be_bytes(data[0..8].try_into().ok()?);
        let action = u32::from_be_bytes(data[8..12].try_into().ok()?);
        let transaction_id = u32::from_be_bytes(data[12..16].try_into().ok()?);

        match action {
            ACTION_CONNECT => {
                Self::handle_connect(connection_id, transaction_id, from, connections)
            }
            ACTION_ANNOUNCE => {
                Self::handle_announce(
                    data,
                    transaction_id,
                    from,
                    super_tracker,
                    cache,
                    infohash_repo,
                    connections,
                    interval,
                )
                .await
            }
            ACTION_SCRAPE => {
                // 暂未实现 scrape，返回错误
                Some(Self::build_error(transaction_id, "scrape not implemented"))
            }
            _ => Some(Self::build_error(transaction_id, "invalid action")),
        }
    }

    /// 处理 connect 请求
    fn handle_connect(
        connection_id: u64,
        transaction_id: u32,
        from: SocketAddr,
        connections: &RwLock<HashMap<u64, ConnectionEntry>>,
    ) -> Option<Vec<u8>> {
        // 验证 protocol_id
        if connection_id != PROTOCOL_ID {
            return Some(Self::build_error(transaction_id, "invalid protocol id"));
        }

        // 生成新的 connection_id
        let new_connection_id = rand::random::<u64>();

        // 存储
        connections.write().insert(
            new_connection_id,
            ConnectionEntry {
                remote_addr: from,
                created_at: Instant::now(),
            },
        );

        // 构建响应：action(4) + transaction_id(4) + connection_id(8) = 16 字节
        let mut resp = Vec::with_capacity(16);
        resp.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
        resp.extend_from_slice(&transaction_id.to_be_bytes());
        resp.extend_from_slice(&new_connection_id.to_be_bytes());

        debug!(
            "[udp_tracker] connect: {} → connection_id={:016x}",
            from, new_connection_id
        );

        Some(resp)
    }

    /// 处理 announce 请求
    async fn handle_announce(
        data: &[u8],
        transaction_id: u32,
        from: SocketAddr,
        super_tracker: &SuperTrackerState,
        cache: &PeerRepoImpl,
        infohash_repo: Option<&Arc<crate::storage::InfohashRepoImpl>>,
        connections: &RwLock<HashMap<u64, ConnectionEntry>>,
        interval: u32,
    ) -> Option<Vec<u8>> {
        // announce 请求至少 98 字节
        if data.len() < 98 {
            return Some(Self::build_error(
                transaction_id,
                "announce packet too short",
            ));
        }

        let connection_id = u64::from_be_bytes(data[0..8].try_into().ok()?);

        // 验证 connection_id
        {
            let conns = connections.read();
            let entry = conns.get(&connection_id)?;
            if entry.remote_addr.ip() != from.ip() {
                return Some(Self::build_error(transaction_id, "invalid connection id"));
            }
        }

        // 解析字段
        let mut infohash = [0u8; 20];
        infohash.copy_from_slice(&data[16..36]);

        let mut peer_id = [0u8; 20];
        peer_id.copy_from_slice(&data[36..56]);

        let downloaded = u64::from_be_bytes(data[56..64].try_into().ok()?);
        let left = u64::from_be_bytes(data[64..72].try_into().ok()?);
        let uploaded = u64::from_be_bytes(data[72..80].try_into().ok()?);
        let event_raw = u32::from_be_bytes(data[80..84].try_into().ok()?);
        // data[84..88] = IP（0 = 使用源地址）
        // data[88..92] = key
        let num_want = i32::from_be_bytes(data[92..96].try_into().ok()?);
        let port = u16::from_be_bytes(data[96..98].try_into().ok()?);

        let event = match event_raw {
            1 => AnnounceEvent::Completed,
            2 => AnnounceEvent::Started,
            3 => AnnounceEvent::Stopped,
            _ => AnnounceEvent::None,
        };

        debug!(
            "[udp_tracker] announce: {} infohash={} port={} event={:?} numwant={}",
            from,
            hex::encode(&infohash[..4]),
            port,
            event,
            num_want
        );

        // 调用超级 Tracker 处理
        let (peers, seeders, leechers) = super_tracker
            .handle_udp_announce(
                infohash, peer_id, port, from, uploaded, downloaded, left, event, cache,
            )
            .await;

        // 统一数据归口：announce 的 infohash 注册到 InfohashRepo
        if let Some(repo) = infohash_repo {
            repo.register_sync(infohash, "udp_announce");
        }

        // 限制 numwant
        let num_want = if !(0..=100).contains(&num_want) {
            peers.len()
        } else {
            num_want as usize
        };
        let peers: Vec<SocketAddr> = peers.into_iter().take(num_want).collect();

        // 构建响应：action(4) + transaction_id(4) + interval(4) + leechers(4) + seeders(4) + compact peers
        let mut resp = Vec::with_capacity(20 + peers.len() * 6);
        resp.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
        resp.extend_from_slice(&transaction_id.to_be_bytes());
        resp.extend_from_slice(&interval.to_be_bytes());
        resp.extend_from_slice(&(leechers as u32).to_be_bytes());
        resp.extend_from_slice(&(seeders as u32).to_be_bytes());

        // compact peers
        for peer in &peers {
            if let std::net::IpAddr::V4(ip) = peer.ip() {
                resp.extend_from_slice(&ip.octets());
                resp.extend_from_slice(&peer.port().to_be_bytes());
            }
        }

        debug!(
            "[udp_tracker] announce 响应: {} peers, {} seeders, {} leechers",
            peers.len(),
            seeders,
            leechers
        );

        Some(resp)
    }

    /// 构建错误响应
    fn build_error(transaction_id: u32, message: &str) -> Vec<u8> {
        let mut resp = Vec::new();
        resp.extend_from_slice(&ACTION_ERROR.to_be_bytes());
        resp.extend_from_slice(&transaction_id.to_be_bytes());
        resp.extend_from_slice(message.as_bytes());
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_constants() {
        assert_eq!(PROTOCOL_ID, 0x41727101980);
        assert_eq!(ACTION_CONNECT, 0);
        assert_eq!(ACTION_ANNOUNCE, 1);
        assert_eq!(ACTION_ERROR, 3);
    }

    #[test]
    fn test_build_error() {
        let resp = UdpTrackerServer::build_error(12345, "test error");
        assert_eq!(resp.len(), 8 + 10); // action + tid + message
        let action = u32::from_be_bytes(resp[0..4].try_into().unwrap());
        assert_eq!(action, ACTION_ERROR);
        let tid = u32::from_be_bytes(resp[4..8].try_into().unwrap());
        assert_eq!(tid, 12345);
    }

    #[test]
    fn test_connect_request_parsing() {
        // 构造 connect 请求
        let mut req = Vec::new();
        req.extend_from_slice(&PROTOCOL_ID.to_be_bytes());
        req.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
        req.extend_from_slice(&12345u32.to_be_bytes());

        assert_eq!(req.len(), 16);

        let connection_id = u64::from_be_bytes(req[0..8].try_into().unwrap());
        let action = u32::from_be_bytes(req[8..12].try_into().unwrap());
        let transaction_id = u32::from_be_bytes(req[12..16].try_into().unwrap());

        assert_eq!(connection_id, PROTOCOL_ID);
        assert_eq!(action, ACTION_CONNECT);
        assert_eq!(transaction_id, 12345);
    }

    #[test]
    fn test_announce_request_parsing() {
        // 构造 announce 请求（98 字节）
        let mut req = Vec::new();
        req.extend_from_slice(&0x1234567890ABCDEFu64.to_be_bytes()); // connection_id
        req.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
        req.extend_from_slice(&12345u32.to_be_bytes()); // transaction_id
        req.extend_from_slice(&[1u8; 20]); // infohash
        req.extend_from_slice(&[2u8; 20]); // peer_id
        req.extend_from_slice(&0u64.to_be_bytes()); // downloaded
        req.extend_from_slice(&1000u64.to_be_bytes()); // left
        req.extend_from_slice(&0u64.to_be_bytes()); // uploaded
        req.extend_from_slice(&2u32.to_be_bytes()); // event = started
        req.extend_from_slice(&0u32.to_be_bytes()); // IP = 0 (use source)
        req.extend_from_slice(&0xABCDEFu32.to_be_bytes()); // key
        req.extend_from_slice(&(-1i32).to_be_bytes()); // num_want = default
        req.extend_from_slice(&6881u16.to_be_bytes()); // port

        assert_eq!(req.len(), 98);

        let action = u32::from_be_bytes(req[8..12].try_into().unwrap());
        assert_eq!(action, ACTION_ANNOUNCE);

        let port = u16::from_be_bytes(req[96..98].try_into().unwrap());
        assert_eq!(port, 6881);
    }
}
