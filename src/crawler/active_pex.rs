//! 主动 PEX 请求器（BEP 11）
//!
//! 定期主动连接已知 peer，发送 BT 握手和扩展握手，等待对端发送 PEX 消息，
//! 提取 peer 加入 PeerRepo。与被动接收互补，主动获取更多 peer。

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::storage::PeerRepoImpl;
use crate::types::Infohash;

use super::pex_receiver::PexReceiver;

/// BT 握手响应读取超时
const HANDSHAKE_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// PEX 消息分段读取超时
const PEX_MSG_READ_TIMEOUT: Duration = Duration::from_secs(2);
/// 单轮批量请求总超时（40 目标 × 15s/连接 ÷ 10 并发 = 60s）
const BATCH_TOTAL_TIMEOUT: Duration = Duration::from_secs(60);
/// 单个连接子任务总超时（connect_timeout 5s + pex_wait 10s + 5s 余量）
const PER_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// fn new() 默认批量大小
const DEFAULT_BATCH_SIZE: usize = 20;
/// fn new() 默认轮询间隔
const DEFAULT_INTERVAL: Duration = Duration::from_secs(60);
/// fn new() 默认连接超时
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// fn new() 默认 PEX 等待时间
const DEFAULT_PEX_WAIT_TIME: Duration = Duration::from_secs(10);
/// fn new() 默认最大并发连接数
const DEFAULT_MAX_CONCURRENT: usize = 10;
/// fn new() 默认连接间隔（毫秒）
const DEFAULT_CONNECT_INTERVAL_MS: u64 = 100;

/// 主动 PEX 请求器统计
#[derive(Debug, Clone, Default)]
pub struct ActivePexStats {
    /// 主动连接尝试数
    pub connection_attempts: u64,
    /// 成功建立的连接数
    pub connections_succeeded: u64,
    /// 连接失败数
    pub connections_failed: u64,
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
    /// 超时数
    pub timeouts: u64,
    /// 错误数
    pub errors: u64,
}

/// 主动 PEX 请求器
pub struct ActivePexRequester {
    /// PeerRepo
    peer_repo: Arc<PeerRepoImpl>,
    /// PEX 接收器
    pex_receiver: Option<Arc<PexReceiver>>,
    /// 我们的节点 ID
    node_id: [u8; 20],
    /// 统计
    stats: Arc<RwLock<ActivePexStats>>,
    /// 运行标志
    running: Arc<RwLock<bool>>,
    /// 每轮连接的 peer 数
    batch_size: usize,
    /// 轮询间隔
    interval: Duration,
    /// 单个连接的超时时间
    connect_timeout: Duration,
    /// 等待 PEX 消息的时间
    pex_wait_time: Duration,
    /// 最大并发连接数（速率限制）
    max_concurrent: usize,
    /// 连接间隔（毫秒，避免瞬间建立大量连接）
    connect_interval_ms: u64,
    /// 全量同步暂停门：为 true 时跳过 run_batch
    pause_gate: Option<Arc<AtomicBool>>,
}

impl ActivePexRequester {
    /// 创建新的主动 PEX 请求器
    pub fn new(peer_repo: Arc<PeerRepoImpl>, node_id: [u8; 20]) -> Self {
        Self {
            peer_repo,
            pex_receiver: None,
            node_id,
            stats: Arc::new(RwLock::new(ActivePexStats::default())),
            running: Arc::new(RwLock::new(false)),
            batch_size: DEFAULT_BATCH_SIZE,
            interval: DEFAULT_INTERVAL,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            pex_wait_time: DEFAULT_PEX_WAIT_TIME,
            max_concurrent: DEFAULT_MAX_CONCURRENT,
            connect_interval_ms: DEFAULT_CONNECT_INTERVAL_MS,
            pause_gate: None,
        }
    }

    /// 设置最大并发连接数
    pub fn with_max_concurrent(mut self, max: usize) -> Self {
        self.max_concurrent = max;
        self
    }

    /// 设置连接间隔（毫秒）
    pub fn with_connect_interval_ms(mut self, ms: u64) -> Self {
        self.connect_interval_ms = ms;
        self
    }

    /// 设置 PEX 接收器
    pub fn with_pex_receiver(mut self, pex_receiver: Arc<PexReceiver>) -> Self {
        self.pex_receiver = Some(pex_receiver);
        self
    }

    /// 设置每轮连接的 peer 数
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// 设置轮询间隔
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// 设置全量同步暂停门（为 true 时暂停主动 PEX 请求）
    pub fn with_pause_gate(mut self, gate: Option<Arc<AtomicBool>>) -> Self {
        self.pause_gate = gate;
        self
    }

    /// 获取统计
    pub fn stats(&self) -> ActivePexStats {
        let s = self.stats.read();
        ActivePexStats {
            connection_attempts: s.connection_attempts,
            connections_succeeded: s.connections_succeeded,
            connections_failed: s.connections_failed,
            handshakes_completed: s.handshakes_completed,
            extension_handshakes: s.extension_handshakes,
            pex_supported: s.pex_supported,
            pex_messages: s.pex_messages,
            peers_extracted: s.peers_extracted,
            timeouts: s.timeouts,
            errors: s.errors,
        }
    }

    /// 停止运行
    pub fn stop(&self) {
        *self.running.write() = false;
    }

    /// 执行一次主动 PEX 批量请求（由 TaskScheduler 按间隔调度）
    pub async fn run_once(&self) {
        // 全量同步期间暂停主动 PEX，把带宽/CPU 让给联邦同步
        if self
            .pause_gate
            .as_ref()
            .map(|g| g.load(Ordering::Relaxed))
            .unwrap_or(false)
        {
            return;
        }
        // P0-1: 总超时保护，防止 run_batch 卡死导致调度器线程被占满
        match timeout(BATCH_TOTAL_TIMEOUT, self.run_batch()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!("[Active-PEX] 批量请求错误: {}", e);
                let mut stats = self.stats.write();
                stats.errors += 1;
            }
            Err(_) => {
                // 总超时：视为正常结束，不计入 errors，仅记录 warn + timeouts
                warn!(
                    "[Active-PEX] 批量请求总超时（{}s），强制结束本轮",
                    BATCH_TOTAL_TIMEOUT.as_secs()
                );
                let mut stats = self.stats.write();
                stats.timeouts += 1;
            }
        }
    }

    /// 运行一轮批量请求
    async fn run_batch(&self) -> anyhow::Result<()> {
        // P0-2: all_peers_sync() 是同步阻塞操作（SQLite + 内存全量读取），
        // 放入 spawn_blocking 避免阻塞 tokio runtime 线程
        let peer_repo = self.peer_repo.clone();
        let all_peers = tokio::task::spawn_blocking(move || {
            let mut peers = peer_repo.all_peers_sync();
            // 按评分降序排序
            peers.sort_by(|a, b| {
                b.priority_score
                    .partial_cmp(&a.priority_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            peers
        })
        .await?;

        if all_peers.is_empty() {
            debug!("[Active-PEX] PeerRepo 中没有可用的 peer");
            return Ok(());
        }

        // 支持 IPv4 + IPv6，取前 batch_size * 2 个
        let targets: Vec<(SocketAddr, Infohash)> = all_peers
            .into_iter()
            .take(self.batch_size * 2)
            .map(|p| (p.addr, [0u8; 20])) // 简化：使用空 infohash
            .collect();

        if targets.is_empty() {
            debug!("[Active-PEX] 没有可用的 peer");
            return Ok(());
        }

        debug!(
            "[Active-PEX] 开始批量连接 {} 个 peer（最大并发 {}，间隔 {}ms）",
            targets.len(),
            self.max_concurrent,
            self.connect_interval_ms
        );

        // 使用信号量限制并发连接数
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.max_concurrent));
        let mut handles = Vec::new();

        for (addr, infohash) in targets {
            let peer_repo = self.peer_repo.clone();
            let pex_receiver = self.pex_receiver.clone();
            let node_id = self.node_id;
            let connect_timeout = self.connect_timeout;
            let pex_wait_time = self.pex_wait_time;
            let stats = self.stats.clone();
            let sem = semaphore.clone();
            let interval_ms = self.connect_interval_ms;

            handles.push(tokio::spawn(async move {
                // 速率限制：每个连接之间间隔
                if interval_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                }

                // 获取信号量许可（RAII，任务结束自动释放）
                let _permit = match sem.acquire().await {
                    Ok(p) => p,
                    Err(_) => return,
                };

                // P1-1: 单个连接总超时保护（20s = connect 5s + pex_wait 10s + 5s 余量）
                // 超时从获取 permit 后开始计算，interval_ms 的 sleep 不计入
                match timeout(
                    PER_CONNECT_TIMEOUT,
                    connect_and_request_pex(
                        addr,
                        infohash,
                        node_id,
                        peer_repo,
                        pex_receiver,
                        connect_timeout,
                        pex_wait_time,
                        stats,
                    ),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        debug!("[Active-PEX] 连接 {} 失败: {}", addr, e);
                    }
                    Err(_) => {
                        debug!(
                            "[Active-PEX] 连接 {} 总超时（{}s）",
                            addr,
                            PER_CONNECT_TIMEOUT.as_secs()
                        );
                    }
                }
            }));
        }

        // 等待所有连接完成
        for handle in handles {
            let _ = handle.await;
        }

        let stats = self.stats.read();
        info!(
            "[Active-PEX] 批量完成：尝试 {}，成功 {}，失败 {}，PEX 消息 {}，提取 peer {}",
            stats.connection_attempts,
            stats.connections_succeeded,
            stats.connections_failed,
            stats.pex_messages,
            stats.peers_extracted,
        );

        Ok(())
    }
}

/// 连接单个 peer 并请求 PEX
#[allow(clippy::too_many_arguments)]
async fn connect_and_request_pex(
    addr: SocketAddr,
    infohash: Infohash,
    node_id: [u8; 20],
    _peer_repo: Arc<PeerRepoImpl>,
    pex_receiver: Option<Arc<PexReceiver>>,
    connect_timeout: Duration,
    pex_wait_time: Duration,
    stats: Arc<RwLock<ActivePexStats>>,
) -> anyhow::Result<()> {
    {
        let mut s = stats.write();
        s.connection_attempts += 1;
    }

    // 1. 建立 TCP 连接
    let mut stream = match timeout(connect_timeout, TcpStream::connect(addr)).await {
        Ok(Ok(s)) => {
            {
                let mut s = stats.write();
                s.connections_succeeded += 1;
            }
            s
        }
        Ok(Err(e)) => {
            {
                let mut s = stats.write();
                s.connections_failed += 1;
            }
            anyhow::bail!("连接失败: {}", e);
        }
        Err(_) => {
            {
                let mut s = stats.write();
                s.connections_failed += 1;
                s.timeouts += 1;
            }
            anyhow::bail!("连接超时");
        }
    };

    // 2. 发送 BT 握手
    let mut handshake = Vec::with_capacity(68);
    handshake.push(19);
    handshake.extend_from_slice(b"BitTorrent protocol");
    let mut reserved = [0u8; 8];
    reserved[5] |= 0x10; // 支持扩展协议
    handshake.extend_from_slice(&reserved);
    handshake.extend_from_slice(&infohash);
    handshake.extend_from_slice(&node_id);
    stream.write_all(&handshake).await?;

    // 3. 读取 BT 握手响应
    let mut resp_buf = [0u8; 68];
    match timeout(HANDSHAKE_READ_TIMEOUT, stream.read_exact(&mut resp_buf)).await {
        Ok(_) => {}
        Err(_) => {
            {
                let mut s = stats.write();
                s.timeouts += 1;
            }
            anyhow::bail!("握手响应超时");
        }
    }

    if resp_buf[0] != 19 || &resp_buf[1..20] != b"BitTorrent protocol" {
        anyhow::bail!("无效的握手响应");
    }

    {
        let mut s = stats.write();
        s.handshakes_completed += 1;
    }

    // 4. 发送扩展握手
    let ext_handshake = build_extension_handshake(6884);
    let mut ext_msg = Vec::with_capacity(5 + ext_handshake.len());
    ext_msg.extend_from_slice(&(ext_handshake.len() as u32 + 1).to_be_bytes());
    ext_msg.push(20); // 扩展消息类型
    ext_msg.push(0); // 扩展握手 ID
    ext_msg.extend_from_slice(&ext_handshake);
    stream.write_all(&ext_msg).await?;

    // 5. 主动发送 PEX 请求（空 PEX 消息，触发对端发送 PEX 响应）
    // 注意：需要先等待扩展握手响应，获取 ut_pex_id
    // 我们先发送一个空的扩展消息来触发对端响应
    // 实际上大多数客户端会在扩展握手后主动发送 PEX 消息

    // 5. 等待 PEX 消息
    let mut ut_pex_id: Option<u8> = None;
    let mut pex_request_sent = false;
    let deadline = Instant::now() + pex_wait_time;

    while Instant::now() < deadline {
        // 读取消息长度
        let mut len_buf = [0u8; 4];
        match timeout(PEX_MSG_READ_TIMEOUT, stream.read_exact(&mut len_buf)).await {
            Ok(_) => {}
            Err(_) => break,
        }
        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len == 0 {
            continue;
        }
        if msg_len > 1_000_000 {
            break;
        }

        // 读取消息内容
        let mut msg_buf = vec![0u8; msg_len];
        match timeout(PEX_MSG_READ_TIMEOUT, stream.read_exact(&mut msg_buf)).await {
            Ok(_) => {}
            Err(_) => break,
        }

        let msg_type = msg_buf[0];

        if msg_type == 20 && msg_buf.len() >= 2 {
            let ext_id = msg_buf[1];
            let ext_payload = &msg_buf[2..];

            if ext_id == 0 {
                // 扩展握手
                {
                    let mut s = stats.write();
                    s.extension_handshakes += 1;
                }
                if let Some(receiver) = &pex_receiver {
                    if let Some(id) = receiver.handle_extension_handshake(ext_payload) {
                        ut_pex_id = Some(id);
                        {
                            let mut s = stats.write();
                            s.pex_supported += 1;
                        }
                        debug!("[Active-PEX] 对端 {} 支持 PEX (ut_pex_id={})", addr, id);

                        // 主动发送 PEX 请求（空 PEX 消息，触发对端发送 PEX 响应）
                        if !pex_request_sent {
                            pex_request_sent = true;
                            let pex_req = build_empty_pex_message(id);
                            if stream.write_all(&pex_req).await.is_ok() {
                                debug!("[Active-PEX] 已向 {} 发送 PEX 请求", addr);
                            }
                        }
                    }
                }
            } else if Some(ext_id) == ut_pex_id {
                // PEX 消息
                {
                    let mut s = stats.write();
                    s.pex_messages += 1;
                }
                if let Some(receiver) = &pex_receiver {
                    receiver.handle_pex_message(ext_payload, &infohash);
                }
            }
        }
    }

    debug!("[Active-PEX] 连接 {} 完成", addr);
    Ok(())
}

/// 构建空的 PEX 请求消息（触发对端发送 PEX 响应）
fn build_empty_pex_message(ut_pex_id: u8) -> Vec<u8> {
    // PEX 消息格式：bencode 字典，added 为空列表
    let pex_payload = b"d5:addedle";
    let mut msg = Vec::with_capacity(5 + pex_payload.len());
    msg.extend_from_slice(&(pex_payload.len() as u32 + 2).to_be_bytes());
    msg.push(20); // 扩展消息类型
    msg.push(ut_pex_id); // ut_pex 消息 ID
    msg.extend_from_slice(pex_payload);
    msg
}

/// 构建扩展握手消息
fn build_extension_handshake(listen_port: u16) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"d");
    buf.extend_from_slice(b"1:m");
    buf.extend_from_slice(b"d");
    buf.extend_from_slice(b"6:ut_pex");
    buf.extend_from_slice(b"i1e");
    buf.extend_from_slice(b"e");
    buf.extend_from_slice(b"1:p");
    buf.extend_from_slice(format!("i{}e", listen_port).as_bytes());
    buf.extend_from_slice(b"1:v");
    let version = b"PDC Active PEX";
    buf.extend_from_slice(format!("{}:", version.len()).as_bytes());
    buf.extend_from_slice(version);
    buf.extend_from_slice(b"e");
    buf
}
