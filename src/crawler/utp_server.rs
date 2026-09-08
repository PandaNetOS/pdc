//! uTP 轻量级服务端（BEP 29）
//!
//! 目的：让 μTorrent/qBittorrent 等 BT 客户端主动连接我们，
//! 通过 uTP 连接建立后的 BT 握手消息提取 peer_id 和 infohash，
//! 加入 PeerRepo。不需要完整的 uTP 数据传输，只处理连接建立和握手。
//!
//! uTP 协议头（20 字节）：
//! - type/ver (1 字节): 高 4 位类型，低 4 位版本
//! - extension (1 字节)
//! - connection_id (2 字节)
//! - timestamp_microseconds (4 字节)
//! - timestamp_difference_microseconds (4 字节)
//! - wnd_size (4 字节)
//! - seq_nr (2 字节)
//! - ack_nr (2 字节)

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::net::UdpSocket;
use tracing::{debug, info, warn};

use crate::storage::PeerRepoImpl;
use crate::types::Infohash;

/// uTP 包类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UtpPacketType {
    Data = 0,
    Fin = 1,
    State = 2,
    Reset = 3,
    Syn = 4,
}

impl UtpPacketType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Data),
            1 => Some(Self::Fin),
            2 => Some(Self::State),
            3 => Some(Self::Reset),
            4 => Some(Self::Syn),
            _ => None,
        }
    }
}

/// uTP 包头
#[derive(Debug, Clone)]
pub struct UtpHeader {
    pub packet_type: UtpPacketType,
    pub version: u8,
    pub extension: u8,
    pub connection_id: u16,
    pub timestamp_micros: u32,
    pub timestamp_difference_micros: u32,
    pub wnd_size: u32,
    pub seq_nr: u16,
    pub ack_nr: u16,
}

impl UtpHeader {
    /// 从字节解析 uTP 包头（至少 20 字节）
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 20 {
            return None;
        }
        let type_ver = data[0];
        let packet_type = UtpPacketType::from_u8(type_ver >> 4)?;
        let version = type_ver & 0x0f;
        if version != 1 {
            return None; // uTP 版本必须是 1
        }
        Some(Self {
            packet_type,
            version,
            extension: data[1],
            connection_id: u16::from_be_bytes([data[2], data[3]]),
            timestamp_micros: u32::from_be_bytes([data[4], data[5], data[6], data[7]]),
            timestamp_difference_micros: u32::from_be_bytes([data[8], data[9], data[10], data[11]]),
            wnd_size: u32::from_be_bytes([data[12], data[13], data[14], data[15]]),
            seq_nr: u16::from_be_bytes([data[16], data[17]]),
            ack_nr: u16::from_be_bytes([data[18], data[19]]),
        })
    }

    /// 序列化为字节
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(20);
        buf.push((self.packet_type as u8) << 4 | self.version);
        buf.push(self.extension);
        buf.extend_from_slice(&self.connection_id.to_be_bytes());
        buf.extend_from_slice(&self.timestamp_micros.to_be_bytes());
        buf.extend_from_slice(&self.timestamp_difference_micros.to_be_bytes());
        buf.extend_from_slice(&self.wnd_size.to_be_bytes());
        buf.extend_from_slice(&self.seq_nr.to_be_bytes());
        buf.extend_from_slice(&self.ack_nr.to_be_bytes());
        buf
    }
}

/// uTP 连接状态
#[derive(Debug, Clone)]
struct UtpConnection {
    /// 对端地址
    peer_addr: SocketAddr,
    /// 对端 connection_id
    peer_conn_id: u16,
    /// 我们的 connection_id（对端 conn_id - 1）
    our_conn_id: u16,
    /// 我们的下一个序列号
    our_seq: u16,
    /// 对端的下一个期望序列号
    peer_seq_expected: u16,
    /// 连接创建时间
    created_at: Instant,
    /// 接收到的不完整数据（用于重组 BT 消息）
    recv_buffer: Vec<u8>,
    /// 是否已收到 BT 握手
    got_handshake: bool,
    /// 对端的 infohash（从 BT 握手中提取）
    infohash: Option<Infohash>,
    /// 对端的 peer_id
    peer_id: Option<[u8; 20]>,
    /// 是否已收到扩展握手
    got_extension_handshake: bool,
    /// 对端的 ut_pex 消息 ID（如果支持 PEX）
    ut_pex_id: Option<u8>,
    /// 最后一次收到数据的时间（用于超时判断）
    last_data_time: Instant,
}

/// uTP 服务端统计
#[derive(Debug, Clone, Default)]
pub struct UtpServerStats {
    /// 收到的 SYN 包数
    pub syn_received: u64,
    /// 成功建立的连接数
    pub connections_established: u64,
    /// 收到的 BT 握手数
    pub handshakes_received: u64,
    /// 提取的 peer 数
    pub peers_extracted: u64,
    /// 收到的 RESET 数
    pub resets_received: u64,
    /// 超时关闭的连接数
    pub timeouts: u64,
    /// 错误数
    pub errors: u64,
    /// 当前活跃连接数
    pub active_connections: usize,
    /// 因连接池满被拒绝的连接数
    pub connections_rejected: u64,
    /// 因 LRU 淘汰被关闭的连接数
    pub connections_evicted: u64,
}

/// uTP 轻量级服务端
pub struct UtpServer {
    /// UDP Socket
    socket: Arc<UdpSocket>,
    /// 活跃连接（key: (peer_addr, peer_conn_id)）
    connections: RwLock<HashMap<(SocketAddr, u16), UtpConnection>>,
    /// PeerRepo（用于存储提取的 peer）
    peer_repo: Option<Arc<PeerRepoImpl>>,
    /// PEX 接收器（用于处理 PEX 消息）
    pex_receiver: Option<Arc<crate::crawler::pex_receiver::PexReceiver>>,
    /// 我们的节点 ID（用于 BT 握手响应）
    node_id: [u8; 20],
    /// 最大并发连接数（连接池上限）
    max_connections: usize,
    /// 统计
    stats: RwLock<UtpServerStats>,
}

impl UtpServer {
    /// 创建新的 uTP 服务端
    pub fn new(socket: Arc<UdpSocket>, node_id: [u8; 20]) -> Self {
        Self {
            socket,
            connections: RwLock::new(HashMap::new()),
            peer_repo: None,
            pex_receiver: None,
            node_id,
            max_connections: 100, // 默认最大 100 个并发连接
            stats: RwLock::new(UtpServerStats::default()),
        }
    }

    /// 设置最大并发连接数
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// 设置 PeerRepo
    pub fn with_peer_repo(mut self, peer_repo: Arc<PeerRepoImpl>) -> Self {
        self.peer_repo = Some(peer_repo);
        self
    }

    /// 设置 PEX 接收器
    pub fn with_pex_receiver(mut self, pex_receiver: Arc<crate::crawler::pex_receiver::PexReceiver>) -> Self {
        self.pex_receiver = Some(pex_receiver);
        self
    }

    /// 获取统计
    pub fn stats(&self) -> UtpServerStats {
        self.stats.read().clone()
    }

    /// 启动 uTP 服务端（接收循环）
    pub async fn run(&self) {
        info!("[uTP] 服务端启动，监听 {}", self.socket.local_addr().unwrap());
        let mut buf = vec![0u8; 65536];

        loop {
            match self.socket.recv_from(&mut buf).await {
                Ok((n, from)) => {
                    if n < 20 {
                        continue;
                    }
                    self.handle_packet(&buf[..n], from).await;
                }
                Err(e) => {
                    debug!("[uTP] recv_from 错误: {}", e);
                    {
                        let mut stats = self.stats.write();
                        stats.errors += 1;
                    }
                }
            }

            // 定期清理超时连接（每 100 个包检查一次）
            self.cleanup_timeout_connections();
        }
    }

    /// 处理收到的 uTP 包
    async fn handle_packet(&self, data: &[u8], from: SocketAddr) {
        let header = match UtpHeader::parse(data) {
            Some(h) => h,
            None => return,
        };

        let payload = &data[20..];

        match header.packet_type {
            UtpPacketType::Syn => {
                self.handle_syn(&header, from).await;
            }
            UtpPacketType::Data => {
                self.handle_data(&header, payload, from).await;
            }
            UtpPacketType::State => {
                // 对端的 ACK，暂时忽略
                debug!("[uTP] 收到 STATE from {} (seq={}, ack={})", from, header.seq_nr, header.ack_nr);
            }
            UtpPacketType::Fin => {
                self.handle_fin(&header, from).await;
            }
            UtpPacketType::Reset => {
                debug!("[uTP] 收到 RESET from {}", from);
                {
                    let mut stats = self.stats.write();
                    stats.resets_received += 1;
                }
                // 移除连接
                self.connections.write().remove(&(from, header.connection_id));
            }
        }
    }

    /// 处理 SYN 包
    async fn handle_syn(&self, header: &UtpHeader, from: SocketAddr) {
        debug!("[uTP] 收到 SYN from {} (conn_id={}, seq={})", from, header.connection_id, header.seq_nr);

        {
            let mut stats = self.stats.write();
            stats.syn_received += 1;
        }

        // 连接池管理：检查是否超过最大连接数
        let mut should_reject = false;
        {
            let mut conns = self.connections.write();
            if conns.len() >= self.max_connections {
                // LRU 淘汰：找到最老的空闲连接（已完成握手的连接）并关闭
                let mut oldest_key: Option<(SocketAddr, u16)> = None;
                let mut oldest_time = Instant::now();
                for (key, conn) in conns.iter() {
                    if conn.got_handshake && conn.last_data_time < oldest_time {
                        oldest_time = conn.last_data_time;
                        oldest_key = Some(*key);
                    }
                }
                if let Some(key) = oldest_key {
                    conns.remove(&key);
                    {
                        let mut stats = self.stats.write();
                        stats.connections_evicted += 1;
                    }
                    debug!("[uTP] LRU 淘汰最老连接: {:?}", key);
                } else {
                    // 没有可淘汰的连接，标记为拒绝
                    should_reject = true;
                    {
                        let mut stats = self.stats.write();
                        stats.connections_rejected += 1;
                    }
                }
            }
        }
        // guard 已释放

        // 拒绝新连接（在 guard 外发送 RESET）
        if should_reject {
            debug!("[uTP] 连接池已满（{}），拒绝新连接 from {}", self.max_connections, from);
            let reset = UtpHeader {
                packet_type: UtpPacketType::Reset,
                version: 1,
                extension: 0,
                connection_id: header.connection_id.wrapping_sub(1),
                timestamp_micros: 0,
                timestamp_difference_micros: 0,
                wnd_size: 0,
                seq_nr: 0,
                ack_nr: header.seq_nr,
            };
            let _ = self.socket.send_to(&reset.to_bytes(), from).await;
            return;
        }

        // 创建连接
        let our_conn_id = header.connection_id.wrapping_sub(1);
        let conn = UtpConnection {
            peer_addr: from,
            peer_conn_id: header.connection_id,
            our_conn_id,
            our_seq: 1,
            peer_seq_expected: header.seq_nr.wrapping_add(1),
            created_at: Instant::now(),
            recv_buffer: Vec::new(),
            got_handshake: false,
            infohash: None,
            peer_id: None,
            got_extension_handshake: false,
            ut_pex_id: None,
            last_data_time: Instant::now(),
        };

        self.connections.write().insert((from, header.connection_id), conn);

        // 发送 STATE（SYN-ACK）
        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u32;

        let response = UtpHeader {
            packet_type: UtpPacketType::State,
            version: 1,
            extension: 0,
            connection_id: our_conn_id,
            timestamp_micros: now_micros,
            timestamp_difference_micros: 0,
            wnd_size: 1048576, // 1 MB 窗口
            seq_nr: 1,
            ack_nr: header.seq_nr,
        };

        if self.socket.send_to(&response.to_bytes(), from).await.is_ok() {
            debug!("[uTP] 已发送 SYN-ACK to {} (our_conn_id={})", from, our_conn_id);
            {
                let mut stats = self.stats.write();
                stats.connections_established += 1;
            }
        }
    }

    /// 处理 DATA 包
    /// 处理 DATA 包
    /// 处理 DATA 包
    /// 处理 DATA 包
    async fn handle_data(&self, header: &UtpHeader, payload: &[u8], from: SocketAddr) {
        let key = (from, header.connection_id);
        let mut should_close = false;
        let mut need_handshake_response = false;
        let mut our_conn_id_for_resp = 0u16;
        let mut our_seq_for_resp = 1u16;
        let mut infohash_for_resp: Option<Infohash> = None;
        let mut pex_message: Option<(Vec<u8>, Infohash)> = None;
        let mut ext_handshake_data: Option<Vec<u8>> = None;
        let mut no_connection = false;

        {
            let mut conns = self.connections.write();
            if let Some(conn) = conns.get_mut(&key) {
                conn.recv_buffer.extend_from_slice(payload);
                conn.peer_seq_expected = header.seq_nr.wrapping_add(1);
                conn.last_data_time = Instant::now();
                our_conn_id_for_resp = conn.our_conn_id;
                our_seq_for_resp = conn.our_seq;

                // 阶段1：如果还没收到 BT 握手，尝试解析
                if !conn.got_handshake {
                    if let Some((peer_id, infohash)) = parse_bittorrent_handshake(&conn.recv_buffer) {
                        conn.got_handshake = true;
                        conn.peer_id = Some(peer_id);
                        conn.infohash = Some(infohash);
                        infohash_for_resp = Some(infohash);
                        need_handshake_response = true;

                        info!("[uTP] 收到 BT 握手 from {} (peer_id={}, infohash={})", from, hex::encode(&peer_id[..8]), hex::encode(infohash));

                        // 加入 PeerRepo
                        if let Some(repo) = &self.peer_repo {
                            let now = std::time::SystemTime::now();
                            let peer_info = crate::types::PeerInfo {
                                addr: from,
                                peer_id: Some(peer_id),
                                source: crate::types::PeerSource::Utp,
                                first_seen: now,
                                last_active: now,
                                priority_score: 45.0,  // 中性初始分，等待 ScoreMaintainer 重算
                                connection_attempts: 0,
                                connection_successes: 0,
                                is_ipv6: from.is_ipv6(),
                                metadata: Default::default(),
                            };
                            repo.add_peers_sync(&infohash, &[peer_info]);
                        }

                        {
                            let mut stats = self.stats.write();
                            stats.handshakes_received += 1;
                            stats.peers_extracted += 1;
                        }
                    }
                } else {
                    // 阶段2：已收到 BT 握手，解析 BT 协议消息
                    while conn.recv_buffer.len() >= 4 {
                        let msg_len = u32::from_be_bytes([conn.recv_buffer[0], conn.recv_buffer[1], conn.recv_buffer[2], conn.recv_buffer[3]]) as usize;
                        if msg_len == 0 {
                            conn.recv_buffer.drain(0..4);
                            continue;
                        }
                        if conn.recv_buffer.len() < 4 + msg_len {
                            break;
                        }
                        let msg_type = conn.recv_buffer[4];
                        let msg_payload = conn.recv_buffer[5..4 + msg_len].to_vec();
                        conn.recv_buffer.drain(0..4 + msg_len);

                        match msg_type {
                            20 => {
                                if !msg_payload.is_empty() {
                                    let ext_id = msg_payload[0];
                                    let ext_payload = msg_payload[1..].to_vec();
                                    if ext_id == 0 {
                                        ext_handshake_data = Some(ext_payload);
                                    } else if Some(ext_id) == conn.ut_pex_id {
                                        if let Some(ih) = conn.infohash {
                                            pex_message = Some((ext_payload, ih));
                                        }
                                    }
                                }
                            }
                            9 => {
                                if msg_payload.len() >= 2 {
                                    let port = u16::from_be_bytes([msg_payload[0], msg_payload[1]]);
                                    debug!("[uTP] 对端监听端口: {}", port);
                                }
                            }
                            _ => {}
                        }
                    }
                }

                if conn.last_data_time.elapsed() > Duration::from_secs(60) {
                    should_close = true;
                }
            } else {
                no_connection = true;
            }
        }
        // guard 已完全释放

        // 没有对应的连接，发送 RESET
        if no_connection {
            let reset = UtpHeader {
                packet_type: UtpPacketType::Reset,
                version: 1,
                extension: 0,
                connection_id: header.connection_id.wrapping_sub(1),
                timestamp_micros: 0,
                timestamp_difference_micros: 0,
                wnd_size: 0,
                seq_nr: 0,
                ack_nr: header.seq_nr,
            };
            let _ = self.socket.send_to(&reset.to_bytes(), from).await;
            debug!("[uTP] 收到 DATA 但无连接 from {}，发送 RESET", from);
            return;
        }

        // 处理扩展握手
        if let Some(ext_data) = ext_handshake_data {
            if let Some(ut_pex_id) = self.pex_receiver.as_ref().and_then(|r| r.handle_extension_handshake(&ext_data)) {
                let mut conns = self.connections.write();
                if let Some(conn) = conns.get_mut(&key) {
                    conn.got_extension_handshake = true;
                    conn.ut_pex_id = Some(ut_pex_id);
                }
                debug!("[uTP] 对端支持 PEX (ut_pex_id={})", ut_pex_id);
            }
        }

        // 处理 PEX 消息
        if let Some((pex_data, ih)) = pex_message {
            if let Some(receiver) = &self.pex_receiver {
                receiver.handle_pex_message(&pex_data, &ih);
            }
        }

        // 发送握手响应
        if need_handshake_response {
            if let Some(ih) = infohash_for_resp {
                let our_handshake = build_bittorrent_handshake(&self.node_id, &ih);
                let _ = self.send_data(from, our_conn_id_for_resp, &mut our_seq_for_resp, &our_handshake).await;
                let ext_handshake = build_extension_handshake(6883);
                let _ = self.send_extended_message(from, our_conn_id_for_resp, &mut our_seq_for_resp, 0, &ext_handshake).await;
                let mut conns = self.connections.write();
                if let Some(conn) = conns.get_mut(&key) {
                    conn.our_seq = our_seq_for_resp;
                }
                debug!("[uTP] 已发送握手响应和扩展握手，等待 PEX 消息...");
            }
        }

        // 发送 ACK（STATE）
        let now_micros = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_micros() as u32;
        let ack = UtpHeader {
            packet_type: UtpPacketType::State,
            version: 1,
            extension: 0,
            connection_id: our_conn_id_for_resp,
            timestamp_micros: now_micros,
            timestamp_difference_micros: 0,
            wnd_size: 1048576,
            seq_nr: 1,
            ack_nr: header.seq_nr,
        };
        let _ = self.socket.send_to(&ack.to_bytes(), from).await;

        // 如果需要关闭连接
        if should_close {
            let fin = UtpHeader {
                packet_type: UtpPacketType::Fin,
                version: 1,
                extension: 0,
                connection_id: our_conn_id_for_resp,
                timestamp_micros: now_micros,
                timestamp_difference_micros: 0,
                wnd_size: 0,
                seq_nr: 2,
                ack_nr: header.seq_nr,
            };
            let _ = self.socket.send_to(&fin.to_bytes(), from).await;
            self.connections.write().remove(&key);
            debug!("[uTP] 连接超时关闭 from {}", from);
        }
    }

    async fn send_data(&self, to: SocketAddr, conn_id: u16, seq: &mut u16, data: &[u8]) -> std::io::Result<usize> {
        let now_micros = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_micros() as u32;
        let header = UtpHeader {
            packet_type: UtpPacketType::Data,
            version: 1,
            extension: 0,
            connection_id: conn_id,
            timestamp_micros: now_micros,
            timestamp_difference_micros: 0,
            wnd_size: 1048576,
            seq_nr: *seq,
            ack_nr: 0,
        };
        *seq = seq.wrapping_add(1);
        let mut packet = header.to_bytes();
        packet.extend_from_slice(data);
        self.socket.send_to(&packet, to).await
    }

    /// 发送扩展消息（辅助方法）
    async fn send_extended_message(&self, to: SocketAddr, conn_id: u16, seq: &mut u16, ext_id: u8, payload: &[u8]) -> std::io::Result<usize> {
        let mut data = Vec::with_capacity(1 + payload.len());
        data.push(ext_id);
        data.extend_from_slice(payload);
        self.send_data(to, conn_id, seq, &data).await
    }

    async fn handle_fin(&self, header: &UtpHeader, from: SocketAddr) {
        debug!("[uTP] 收到 FIN from {}", from);
        // 移除连接
        self.connections.write().remove(&(from, header.connection_id));
        {
            let mut stats = self.stats.write();
            stats.active_connections = self.connections.read().len();
        }
        // 发送 ACK
        let ack = UtpHeader {
            packet_type: UtpPacketType::State,
            version: 1,
            extension: 0,
            connection_id: header.connection_id.wrapping_sub(1),
            timestamp_micros: 0,
            timestamp_difference_micros: 0,
            wnd_size: 0,
            seq_nr: 0,
            ack_nr: header.seq_nr,
        };
        let _ = self.socket.send_to(&ack.to_bytes(), from).await;
    }

    /// 清理超时连接
    fn cleanup_timeout_connections(&self) {
        let mut conns = self.connections.write();
        let timeout = Duration::from_secs(30);
        let before = conns.len();
        conns.retain(|_, conn| conn.created_at.elapsed() < timeout);
        let removed = before - conns.len();
        if removed > 0 {
            debug!("[uTP] 清理 {} 个超时连接", removed);
            let mut stats = self.stats.write();
            stats.timeouts += removed as u64;
        }
    }
}

/// 解析 BT 握手消息
///
/// BT 握手格式：
/// - pstrlen (1 字节): 通常是 19
/// - pstr (19 字节): "BitTorrent protocol"
/// - reserved (8 字节)
/// - infohash (20 字节)
/// - peer_id (20 字节)
///
/// 返回 (peer_id, infohash)
fn parse_bittorrent_handshake(data: &[u8]) -> Option<([u8; 20], Infohash)> {
    if data.len() < 68 {
        return None;
    }
    let pstrlen = data[0] as usize;
    if pstrlen != 19 {
        return None;
    }
    if &data[1..20] != b"BitTorrent protocol" {
        return None;
    }
    // reserved: data[20..28]
    let infohash: Infohash = data[28..48].try_into().ok()?;
    let peer_id: [u8; 20] = data[48..68].try_into().ok()?;
    Some((peer_id, infohash))
}

/// 构建 BT 握手消息
///
/// BT 握手格式：
/// - pstrlen (1 字节): 19
/// - pstr (19 字节): "BitTorrent protocol"
/// - reserved (8 字节): 扩展支持位
/// - infohash (20 字节)
/// - peer_id (20 字节)
fn build_bittorrent_handshake(peer_id: &[u8; 20], infohash: &Infohash) -> Vec<u8> {
    let mut buf = Vec::with_capacity(68);
    buf.push(19); // pstrlen
    buf.extend_from_slice(b"BitTorrent protocol");
    // reserved: 设置扩展协议支持位（第 20 位 = 支持扩展协议 BEP 10）
    let mut reserved = [0u8; 8];
    reserved[5] |= 0x10; // 支持扩展协议
    buf.extend_from_slice(&reserved);
    buf.extend_from_slice(infohash);
    buf.extend_from_slice(peer_id);
    buf
}

/// 构建 BT 扩展握手消息（BEP 10）
///
/// 扩展握手是一个 bencode 字典，包含：
/// - m: 支持的扩展消息映射（名称 -> ID）
/// - p: 监听端口
/// - v: 客户端版本
/// - e: 支持的扩展标志
fn build_extension_handshake(listen_port: u16) -> Vec<u8> {
    // 手动构造 bencode 字典
    // d1:md5:ut_pexi1ee1:pi6883e1:v13:PDC uTP Server1:ei0ee
    let mut buf = Vec::new();
    buf.extend_from_slice(b"d"); // 字典开始

    // m: 扩展消息映射
    buf.extend_from_slice(b"1:m");
    buf.extend_from_slice(b"d"); // 子字典开始
    buf.extend_from_slice(b"5:ut_pex"); // key: ut_pex
    buf.extend_from_slice(b"i1e"); // value: 1 (ut_pex 的消息 ID)
    buf.extend_from_slice(b"e"); // 子字典结束

    // p: 监听端口
    buf.extend_from_slice(b"1:p");
    buf.extend_from_slice(format!("i{}e", listen_port).as_bytes());

    // v: 客户端版本
    buf.extend_from_slice(b"1:v");
    let version = b"PDC uTP Server";
    buf.extend_from_slice(format!("{}:", version.len()).as_bytes());
    buf.extend_from_slice(version);

    // e: 扩展标志（0 = 无特殊扩展）
    buf.extend_from_slice(b"1:e");
    buf.extend_from_slice(b"i0e");

    buf.extend_from_slice(b"e"); // 字典结束
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utp_header_parse() {
        // 构造一个 SYN 包
        let header = UtpHeader {
            packet_type: UtpPacketType::Syn,
            version: 1,
            extension: 0,
            connection_id: 12345,
            timestamp_micros: 1000000,
            timestamp_difference_micros: 0,
            wnd_size: 1048576,
            seq_nr: 1,
            ack_nr: 0,
        };
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), 20);
        let parsed = UtpHeader::parse(&bytes).unwrap();
        assert_eq!(parsed.packet_type, UtpPacketType::Syn);
        assert_eq!(parsed.version, 1);
        assert_eq!(parsed.connection_id, 12345);
        assert_eq!(parsed.seq_nr, 1);
    }

    #[test]
    fn test_parse_bittorrent_handshake() {
        let mut data = vec![0u8; 68];
        data[0] = 19;
        data[1..20].copy_from_slice(b"BitTorrent protocol");
        // reserved 全 0
        // infohash
        data[28..48].copy_from_slice(&[1u8; 20]);
        // peer_id
        data[48..68].copy_from_slice(&[2u8; 20]);

        let (peer_id, infohash) = parse_bittorrent_handshake(&data).unwrap();
        assert_eq!(peer_id, [2u8; 20]);
        assert_eq!(infohash, [1u8; 20]);
    }
}
