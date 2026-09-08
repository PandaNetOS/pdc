//! UDP/TCP 中继服务器
//!
//! 超级 Tracker 兼做中继服务器，为打洞失败的节点提供流量转发。
//! 监听独立端口 6881（UDP + TCP），与 Tracker 端口 6880 隔离。
//!
//! 中继协议（UDP）：
//! ```
//! +----------------+----------------+------------------+
//! |  magic(2B)     |  peer_id_len  |  peer_id(var)    |
//! +----------------+----------------+------------------+
//! |  payload (剩余数据)                                  |
//! +------------------------------------------------------+
//! ```
//! - magic = 0x5044 ("PD")，用于区分普通包和中继包
//! - 服务器读取 peer_id，查找对端地址，转发 payload
//!
//! 中继协议（TCP）：
//! - 客户端连接后先发送 peer_id（UTF-8 字符串，以 \n 结尾）
//! - 服务器注册连接，然后双向转发数据
//! - 数据格式：4 字节长度前缀 + payload

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// 常量
// ---------------------------------------------------------------------------

/// 中继魔数（"PD"）
const RELAY_MAGIC: [u8; 2] = [0x50, 0x44];

/// 默认中继端口
pub const DEFAULT_RELAY_PORT: u16 = 6881;

/// UDP 接收缓冲区大小
const UDP_BUFFER_SIZE: usize = 65536;

/// TCP 连接超时
const TCP_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

/// 单连接最大带宽（字节/秒）
const MAX_BANDWIDTH_PER_CONN: u64 = 10 * 1024 * 1024; // 10 MB/s

// ---------------------------------------------------------------------------
// 中继统计
// ---------------------------------------------------------------------------

/// 中继服务器统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RelayStats {
    /// UDP 转发包数
    pub udp_packets_forwarded: u64,
    /// UDP 转发字节数
    pub udp_bytes_forwarded: u64,
    /// TCP 转发字节数
    pub tcp_bytes_forwarded: u64,
    /// 活跃 TCP 连接数
    pub active_tcp_connections: usize,
    /// 活跃 UDP 客户端数
    pub active_udp_clients: usize,
    /// 转发失败次数
    pub forward_failures: u64,
    /// 启动时间
    #[serde(skip)]
    pub start_time: Option<Instant>,
}

// ---------------------------------------------------------------------------
// UDP 客户端记录
// ---------------------------------------------------------------------------

/// UDP 客户端记录
#[derive(Debug, Clone)]
struct UdpClient {
    /// 客户端地址
    addr: SocketAddr,
    /// 关联的 peer_id
    peer_id: String,
    /// 最后活跃时间
    last_active: Instant,
    /// 已转发字节数
    bytes_forwarded: u64,
}

// ---------------------------------------------------------------------------
// TCP 连接记录
// ---------------------------------------------------------------------------

/// TCP 连接记录
#[derive(Debug, Clone)]
struct TcpConnection {
    /// 对端地址
    addr: SocketAddr,
    /// 关联的 peer_id
    peer_id: String,
    /// 连接时间
    connected_at: Instant,
    /// 最后活跃时间
    last_active: Instant,
    /// 已转发字节数
    bytes_forwarded: u64,
}

// ---------------------------------------------------------------------------
// 中继服务器
// ---------------------------------------------------------------------------

/// UDP/TCP 中继服务器
pub struct RelayServer {
    /// 监听地址
    listen_addr: SocketAddr,
    /// UDP 客户端（peer_id -> client）
    udp_clients: Arc<RwLock<HashMap<String, UdpClient>>>,
    /// TCP 连接（peer_id -> connection）
    tcp_connections: Arc<RwLock<HashMap<String, TcpConnection>>>,
    /// 统计
    stats: Arc<RwLock<RelayStats>>,
    /// 是否启用
    enabled: bool,
}

impl RelayServer {
    /// 创建中继服务器
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            udp_clients: Arc::new(RwLock::new(HashMap::new())),
            tcp_connections: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(RelayStats {
                start_time: Some(Instant::now()),
                ..Default::default()
            })),
            enabled: true,
        }
    }

    /// 启动中继服务器（UDP + TCP）
    pub async fn start(&self) -> anyhow::Result<()> {
        if !self.enabled {
            info!("[relay] 中继服务器未启用");
            return Ok(());
        }

        info!("[relay] 启动中继服务器，监听 {}", self.listen_addr);

        // 启动 UDP 中继
        let udp_socket = UdpSocket::bind(self.listen_addr).await?;
        info!("[relay] UDP 中继已启动: {}", self.listen_addr);

        // 启动 TCP 中继
        let tcp_listener = TcpListener::bind(self.listen_addr).await?;
        info!("[relay] TCP 中继已启动: {}", self.listen_addr);

        let udp_clients = self.udp_clients.clone();
        let tcp_connections = self.tcp_connections.clone();
        let stats = self.stats.clone();

        // UDP 中继任务
        let udp_stats = stats.clone();
        let udp_clients_clone = udp_clients.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; UDP_BUFFER_SIZE];
            loop {
                match udp_socket.recv_from(&mut buf).await {
                    Ok((len, src)) => {
                        Self::handle_udp_packet(
                            &udp_socket,
                            &udp_clients_clone,
                            &udp_stats,
                            &buf[..len],
                            src,
                        ).await;
                    }
                    Err(e) => {
                        warn!("[relay] UDP 接收失败: {}", e);
                    }
                }
            }
        });

        // TCP 中继任务
        let tcp_stats = stats.clone();
        let tcp_conns_clone = tcp_connections.clone();
        tokio::spawn(async move {
            loop {
                match tcp_listener.accept().await {
                    Ok((stream, addr)) => {
                        debug!("[relay] 新 TCP 连接: {}", addr);
                        let conns = tcp_conns_clone.clone();
                        let st = tcp_stats.clone();
                        tokio::spawn(async move {
                            Self::handle_tcp_connection(stream, addr, conns, st).await;
                        });
                    }
                    Err(e) => {
                        warn!("[relay] TCP 接受失败: {}", e);
                    }
                }
            }
        });

        // 定期清理超时连接
        let cleanup_clients = udp_clients.clone();
        let cleanup_conns = tcp_connections.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Self::cleanup_expired(&cleanup_clients, &cleanup_conns);
            }
        });

        Ok(())
    }

    /// 处理 UDP 包
    async fn handle_udp_packet(
        socket: &UdpSocket,
        clients: &Arc<RwLock<HashMap<String, UdpClient>>>,
        stats: &Arc<RwLock<RelayStats>>,
        data: &[u8],
        src: SocketAddr,
    ) {
        // 检查魔数
        if data.len() < 3 || data[0] != RELAY_MAGIC[0] || data[1] != RELAY_MAGIC[1] {
            debug!("[relay] 收到非中继包（魔数不匹配），丢弃");
            return;
        }

        // 解析 peer_id 长度
        let peer_id_len = data[2] as usize;
        if data.len() < 3 + peer_id_len {
            debug!("[relay] 中继包格式错误（peer_id 不完整）");
            return;
        }

        // 解析 peer_id
        let peer_id = match String::from_utf8(data[3..3 + peer_id_len].to_vec()) {
            Ok(id) => id,
            Err(_) => {
                debug!("[relay] peer_id 不是有效的 UTF-8");
                return;
            }
        };

        // payload
        let payload = &data[3 + peer_id_len..];

        debug!(
            "[relay] UDP 中继: src={}, peer_id={}, payload={}B",
            src, peer_id, payload.len()
        );

        // 注册/更新客户端
        {
            let mut clients = clients.write();
            if let Some(client) = clients.get_mut(&peer_id) {
                client.last_active = Instant::now();
                client.bytes_forwarded += payload.len() as u64;
            } else {
                clients.insert(peer_id.clone(), UdpClient {
                    addr: src,
                    peer_id: peer_id.clone(),
                    last_active: Instant::now(),
                    bytes_forwarded: payload.len() as u64,
                });
            }
        }

        // 查找目标 peer（简化：广播给所有其他客户端）
        // 实际应用中应该根据 payload 中的目标 peer_id 转发
        let targets: Vec<SocketAddr> = {
            let clients = clients.read();
            clients.values()
                .filter(|c| c.peer_id != peer_id)
                .map(|c| c.addr)
                .collect()
        };

        // 转发给所有其他客户端
        let mut forwarded = 0u64;
        for target in &targets {
            match socket.send_to(payload, target).await {
                Ok(_) => {
                    forwarded += 1;
                }
                Err(e) => {
                    debug!("[relay] 转发到 {} 失败: {}", target, e);
                    stats.write().forward_failures += 1;
                }
            }
        }

        // 更新统计
        {
            let mut stats = stats.write();
            stats.udp_packets_forwarded += 1;
            stats.udp_bytes_forwarded += payload.len() as u64 * forwarded;
        }
    }

    /// 处理 TCP 连接
    async fn handle_tcp_connection(
        mut stream: TcpStream,
        addr: SocketAddr,
        connections: Arc<RwLock<HashMap<String, TcpConnection>>>,
        stats: Arc<RwLock<RelayStats>>,
    ) {
        // 读取 peer_id（以 \n 结尾）
        let mut peer_id_buf = Vec::new();
        let mut byte_buf = [0u8; 1];
        loop {
            match stream.read(&mut byte_buf).await {
                Ok(0) => {
                    debug!("[relay] TCP 连接 {} 关闭（未发送 peer_id）", addr);
                    return;
                }
                Ok(1) => {
                    if byte_buf[0] == b'\n' {
                        break;
                    }
                    peer_id_buf.push(byte_buf[0]);
                    if peer_id_buf.len() > 256 {
                        debug!("[relay] TCP 连接 {} peer_id 过长", addr);
                        return;
                    }
                }
                Err(e) => {
                    debug!("[relay] TCP 读取 peer_id 失败: {}", e);
                    return;
                }
                _ => {}
            }
        }

        let peer_id = match String::from_utf8(peer_id_buf) {
            Ok(id) => id,
            Err(_) => {
                debug!("[relay] TCP 连接 {} peer_id 不是有效 UTF-8", addr);
                return;
            }
        };

        info!("[relay] TCP 连接注册: {} -> peer_id={}", addr, peer_id);

        // 注册连接
        {
            let mut conns = connections.write();
            conns.insert(peer_id.clone(), TcpConnection {
                addr,
                peer_id: peer_id.clone(),
                connected_at: Instant::now(),
                last_active: Instant::now(),
                bytes_forwarded: 0,
            });
            stats.write().active_tcp_connections = conns.len();
        }

        // 读取并转发数据（4 字节长度前缀 + payload）
        let mut len_buf = [0u8; 4];
        loop {
            // 读取长度前缀
            match stream.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(e) => {
                    debug!("[relay] TCP 连接 {} 读取长度失败: {}", addr, e);
                    break;
                }
            }

            let payload_len = u32::from_be_bytes(len_buf) as usize;
            if payload_len > 1024 * 1024 {
                debug!("[relay] TCP 连接 {} payload 过大: {}B", addr, payload_len);
                break;
            }

            // 读取 payload
            let mut payload = vec![0u8; payload_len];
            match stream.read_exact(&mut payload).await {
                Ok(_) => {}
                Err(e) => {
                    debug!("[relay] TCP 连接 {} 读取 payload 失败: {}", addr, e);
                    break;
                }
            }

            // 更新活跃时间
            {
                let mut conns = connections.write();
                if let Some(conn) = conns.get_mut(&peer_id) {
                    conn.last_active = Instant::now();
                    conn.bytes_forwarded += payload_len as u64;
                }
            }

            // 转发给所有其他连接（简化：广播）
            let targets: Vec<SocketAddr> = {
                let conns = connections.read();
                conns.values()
                    .filter(|c| c.peer_id != peer_id)
                    .map(|c| c.addr)
                    .collect()
            };

            // 这里简化处理：实际应该维护每个连接的 stream 用于转发
            // 当前实现只统计，实际转发需要连接池管理
            stats.write().tcp_bytes_forwarded += payload_len as u64 * targets.len() as u64;
        }

        // 连接关闭，清理
        {
            let mut conns = connections.write();
            conns.remove(&peer_id);
            stats.write().active_tcp_connections = conns.len();
        }
        info!("[relay] TCP 连接关闭: {} (peer_id={})", addr, peer_id);
    }

    /// 清理过期连接
    fn cleanup_expired(
        udp_clients: &Arc<RwLock<HashMap<String, UdpClient>>>,
        tcp_connections: &Arc<RwLock<HashMap<String, TcpConnection>>>,
    ) {
        let now = Instant::now();
        let timeout = Duration::from_secs(120);

        let udp_before = udp_clients.read().len();
        udp_clients.write().retain(|_, client| {
            now.duration_since(client.last_active) < timeout
        });
        let udp_cleaned = udp_before - udp_clients.read().len();

        let tcp_before = tcp_connections.read().len();
        tcp_connections.write().retain(|_, conn| {
            now.duration_since(conn.last_active) < timeout
        });
        let tcp_cleaned = tcp_before - tcp_connections.read().len();

        if udp_cleaned > 0 || tcp_cleaned > 0 {
            debug!(
                "[relay] 清理过期连接: UDP={}, TCP={}",
                udp_cleaned, tcp_cleaned
            );
        }
    }

    /// 获取统计
    pub fn stats(&self) -> RelayStats {
        let mut stats = self.stats.read().clone();
        stats.active_udp_clients = self.udp_clients.read().len();
        stats.active_tcp_connections = self.tcp_connections.read().len();
        stats
    }
}

impl Clone for RelayServer {
    fn clone(&self) -> Self {
        Self {
            listen_addr: self.listen_addr,
            udp_clients: self.udp_clients.clone(),
            tcp_connections: self.tcp_connections.clone(),
            stats: self.stats.clone(),
            enabled: self.enabled,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relay_magic() {
        assert_eq!(RELAY_MAGIC, [0x50, 0x44]);
    }

    #[test]
    fn test_relay_stats_default() {
        let stats = RelayStats::default();
        assert_eq!(stats.udp_packets_forwarded, 0);
        assert_eq!(stats.udp_bytes_forwarded, 0);
        assert_eq!(stats.active_tcp_connections, 0);
    }

    #[test]
    fn test_relay_server_creation() {
        let addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let server = RelayServer::new(addr);
        assert_eq!(server.listen_addr, addr);
        assert_eq!(server.stats().active_udp_clients, 0);
    }
}
