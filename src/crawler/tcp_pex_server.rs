//! TCP PEX 接收器（BEP 11）
//!
//! 通过标准 TCP BT 连接被动接收 PEX 消息，提取 peer 加入 PeerRepo。
//! 与 uTP 服务端互补，支持不支持 uTP 的传统 BT 客户端。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::storage::PeerRepoImpl;
use crate::types::Infohash;

use super::pex_receiver::PexReceiver;

/// TCP PEX 服务端统计
#[derive(Debug, Clone, Default)]
pub struct TcpPexStats {
    /// 接受的 TCP 连接数
    pub connections_accepted: u64,
    /// 成功完成 BT 握手的连接数
    pub handshakes_completed: u64,
    /// 收到的扩展握手数
    pub extension_handshakes: u64,
    /// 支持 PEX 的对端数
    pub pex_supported: u64,
    /// 收到的 PEX 消息数
    pub pex_messages: u64,
    /// 提取的 peer 数
    pub peers_extracted: u64,
    /// 超时关闭的连接数
    pub timeouts: u64,
    /// 错误数
    pub errors: u64,
    /// 当前活跃连接数
    pub active_connections: usize,
}

/// TCP PEX 服务端
pub struct TcpPexServer {
    /// 监听地址
    listen_addr: SocketAddr,
    /// PeerRepo
    peer_repo: Option<Arc<PeerRepoImpl>>,
    /// PEX 接收器
    pex_receiver: Option<Arc<PexReceiver>>,
    /// 我们的节点 ID
    node_id: [u8; 20],
    /// 统计
    stats: RwLock<TcpPexStats>,
    /// 活跃连接数
    active_count: Arc<RwLock<usize>>,
    /// 最大并发连接数
    max_connections: usize,
}

impl TcpPexServer {
    /// 创建新的 TCP PEX 服务端
    pub fn new(listen_addr: SocketAddr, node_id: [u8; 20]) -> Self {
        Self {
            listen_addr,
            peer_repo: None,
            pex_receiver: None,
            node_id,
            stats: RwLock::new(TcpPexStats::default()),
            active_count: Arc::new(RwLock::new(0)),
            max_connections: 50,
        }
    }

    /// 设置 PeerRepo
    pub fn with_peer_repo(mut self, peer_repo: Arc<PeerRepoImpl>) -> Self {
        self.peer_repo = Some(peer_repo);
        self
    }

    /// 设置 PEX 接收器
    pub fn with_pex_receiver(mut self, pex_receiver: Arc<PexReceiver>) -> Self {
        self.pex_receiver = Some(pex_receiver);
        self
    }

    /// 设置最大并发连接数
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// 获取统计
    pub fn stats(&self) -> TcpPexStats {
        let s = self.stats.read();
        TcpPexStats {
            connections_accepted: s.connections_accepted,
            handshakes_completed: s.handshakes_completed,
            extension_handshakes: s.extension_handshakes,
            pex_supported: s.pex_supported,
            pex_messages: s.pex_messages,
            peers_extracted: s.peers_extracted,
            timeouts: s.timeouts,
            errors: s.errors,
            active_connections: *self.active_count.read(),
        }
    }

    /// 启动 TCP PEX 服务端
    pub async fn run(&self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        info!("[TCP-PEX] 服务端启动，监听 {}", self.listen_addr);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    // 检查连接数上限
                    {
                        let active = *self.active_count.read();
                        if active >= self.max_connections {
                            debug!("[TCP-PEX] 连接数已满（{}），拒绝连接 from {}", self.max_connections, addr);
                            continue;
                        }
                    }

                    {
                        let mut active = self.active_count.write();
                        *active += 1;
                    }
                    {
                        let mut stats = self.stats.write();
                        stats.connections_accepted += 1;
                    }

                    let peer_repo = self.peer_repo.clone();
                    let pex_receiver = self.pex_receiver.clone();
                    let node_id = self.node_id;
                    let active_count = self.active_count.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(
                            stream,
                            addr,
                            node_id,
                            peer_repo,
                            pex_receiver,
                        )
                        .await
                        {
                            debug!("[TCP-PEX] 连接处理错误 from {}: {}", addr, e);
                        }
                        {
                            let mut active = active_count.write();
                            if *active > 0 {
                                *active -= 1;
                            }
                        }
                    });
                }
                Err(e) => {
                    warn!("[TCP-PEX] accept 错误: {}", e);
                    {
                        let mut stats = self.stats.write();
                        stats.errors += 1;
                    }
                }
            }
        }
    }
}

/// 处理单个 TCP 连接
async fn handle_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    node_id: [u8; 20],
    peer_repo: Option<Arc<PeerRepoImpl>>,
    pex_receiver: Option<Arc<PexReceiver>>,
) -> anyhow::Result<()> {
    // 1. 读取 BT 握手（使用 tokio 超时）
    let mut handshake_buf = [0u8; 68];
    stream.read_exact(&mut handshake_buf).await?;

    // 解析 BT 握手
    if handshake_buf[0] != 19 || &handshake_buf[1..20] != b"BitTorrent protocol" {
        anyhow::bail!("无效的 BT 握手");
    }

    let infohash: Infohash = handshake_buf[28..48].try_into()?;
    let peer_id: [u8; 20] = handshake_buf[48..68].try_into()?;

    debug!("[TCP-PEX] 收到 BT 握手 from {} (infohash={})", addr, hex::encode(infohash));

    // 2. 发送我们的 BT 握手响应
    let mut our_handshake = Vec::with_capacity(68);
    our_handshake.push(19);
    our_handshake.extend_from_slice(b"BitTorrent protocol");
    let mut reserved = [0u8; 8];
    reserved[5] |= 0x10; // 支持扩展协议
    our_handshake.extend_from_slice(&reserved);
    our_handshake.extend_from_slice(&infohash);
    our_handshake.extend_from_slice(&node_id);
    stream.write_all(&our_handshake).await?;

    // 3. 加入 PeerRepo
    if let Some(repo) = &peer_repo {
        let mut peer_info = crate::types::PeerInfo::new(addr, crate::types::PeerSource::Pex);
        peer_info.peer_id = Some(peer_id);
        repo.add_peers_sync(&infohash, &[peer_info]);
    }

    // 4. 发送扩展握手
    let ext_handshake = build_extension_handshake(6884);
    let mut ext_msg = Vec::with_capacity(5 + ext_handshake.len());
    ext_msg.extend_from_slice(&(ext_handshake.len() as u32 + 1).to_be_bytes());
    ext_msg.push(20); // 扩展消息类型
    ext_msg.push(0); // 扩展握手 ID
    ext_msg.extend_from_slice(&ext_handshake);
    stream.write_all(&ext_msg).await?;

    // 5. 循环读取消息，等待 PEX 消息
    let mut ut_pex_id: Option<u8> = None;
    let deadline = Instant::now() + Duration::from_secs(60);

    while Instant::now() < deadline {
        // 读取消息长度
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(_) => break,
        }
        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len == 0 {
            continue; // keep-alive
        }
        if msg_len > 1_000_000 {
            anyhow::bail!("消息过大: {}", msg_len);
        }

        // 读取消息内容
        let mut msg_buf = vec![0u8; msg_len];
        match stream.read_exact(&mut msg_buf).await {
            Ok(_) => {}
            Err(_) => break,
        }

        let msg_type = msg_buf[0];

        match msg_type {
            20 => {
                // 扩展消息
                if msg_buf.len() < 2 {
                    continue;
                }
                let ext_id = msg_buf[1];
                let ext_payload = &msg_buf[2..];

                if ext_id == 0 {
                    // 扩展握手
                    if let Some(receiver) = &pex_receiver {
                        if let Some(id) = receiver.handle_extension_handshake(ext_payload) {
                            ut_pex_id = Some(id);
                            debug!("[TCP-PEX] 对端支持 PEX (ut_pex_id={})", id);
                        }
                    }
                } else if Some(ext_id) == ut_pex_id {
                    // PEX 消息
                    if let Some(receiver) = &pex_receiver {
                        receiver.handle_pex_message(ext_payload, &infohash);
                    }
                }
            }
            9 => {
                // port 消息
                if msg_buf.len() >= 3 {
                    let port = u16::from_be_bytes([msg_buf[1], msg_buf[2]]);
                    debug!("[TCP-PEX] 对端监听端口: {}", port);
                }
            }
            _ => {
                // 其他消息忽略
            }
        }
    }

    debug!("[TCP-PEX] 连接关闭 from {}", addr);
    Ok(())
}

/// 构建扩展握手消息
fn build_extension_handshake(listen_port: u16) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"d");
    buf.extend_from_slice(b"1:m");
    buf.extend_from_slice(b"d");
    buf.extend_from_slice(b"5:ut_pex");
    buf.extend_from_slice(b"i1e");
    buf.extend_from_slice(b"e");
    buf.extend_from_slice(b"1:p");
    buf.extend_from_slice(format!("i{}e", listen_port).as_bytes());
    buf.extend_from_slice(b"1:v");
    let version = b"PDC TCP PEX";
    buf.extend_from_slice(format!("{}:", version.len()).as_bytes());
    buf.extend_from_slice(version);
    buf.extend_from_slice(b"e");
    buf
}
