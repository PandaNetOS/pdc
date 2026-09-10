//! DHT 节点探测模块
//!
//! 从 tracker 获得 peer 地址后，主动发送 DHT ping 探测，
//! 响应成功的节点加入 Kademlia 路由表，扩充 DHT 节点池。
//!
//! 策略：
//! 1. 异步探测，不阻塞 tracker 查询
//! 2. 地址去重，已探测过的不再重复探测
//! 3. 批量并发 ping（默认 20 并发）
//! 4. 超时 3 秒，成功的提取 node_id 加入路由表

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use rand::Rng;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::discoverers::dht::message::DhtMessage;
use crate::dht::routing_table::RoutingTable;
use crate::storage::{NodeRepoImpl, PeerRepoImpl};

/// 并发探测数
const MAX_CONCURRENT: usize = 20;
/// ping 超时
const PING_TIMEOUT: Duration = Duration::from_secs(3);
/// 已探测地址缓存上限（防止内存膨胀）
const MAX_PROBED_CACHE: usize = 100_000;
/// 从 PeerRepo 拉取未探测 peer 的间隔
const PEER_REPO_POLL_INTERVAL: Duration = Duration::from_secs(15);
/// 每次从 PeerRepo 拉取的最大 peer 数
const MAX_PEERS_PER_POLL: usize = 100;

/// DHT 探测器
pub struct DhtProbe {
    routing_table: Arc<RwLock<RoutingTable>>,
    node_repo: Option<Arc<NodeRepoImpl>>,
    pub probed: Arc<RwLock<HashSet<SocketAddr>>>,
    node_id: [u8; 20],
    sender: mpsc::UnboundedSender<SocketAddr>,
    // 统计
    pub total_probed: Arc<AtomicU64>,
    pub total_success: Arc<AtomicU64>,
    pub total_added: Arc<AtomicU64>,
    /// 全量同步暂停门：为 true 时暂停从 PeerRepo 拉取新探测任务
    pause_gate: Option<Arc<AtomicBool>>,
}

impl DhtProbe {
    /// 创建探测器
    pub fn new(
        routing_table: Arc<RwLock<RoutingTable>>,
        node_repo: Option<Arc<NodeRepoImpl>>,
        peer_repo: Option<Arc<PeerRepoImpl>>,
        pause_gate: Option<Arc<AtomicBool>>,
    ) -> Self {
        let mut node_id = [0u8; 20];
        rand::thread_rng().fill(&mut node_id);
        let (sender, receiver) = mpsc::unbounded_channel();

        let probe = Self {
            routing_table: routing_table.clone(),
            node_repo: node_repo.clone(),
            probed: Arc::new(RwLock::new(HashSet::new())),
            node_id,
            sender: sender.clone(),
            total_probed: Arc::new(AtomicU64::new(0)),
            total_success: Arc::new(AtomicU64::new(0)),
            total_added: Arc::new(AtomicU64::new(0)),
            pause_gate,
        };

        probe.start(receiver);
        // 启动 PeerRepo 定期拉取任务（统一探测来源）
        if let Some(repo) = peer_repo {
            probe.start_peer_repo_poller(repo);
        }
        probe
    }

    /// 启动 PeerRepo 定期拉取任务：从 PeerRepo 获取未探测的 peer，加入探测队列
    /// 统一探测来源，所有 peer 都经过 PeerRepo，避免遗漏和重复代码
    fn start_peer_repo_poller(&self, peer_repo: Arc<PeerRepoImpl>) {
        let probed = self.probed.clone();
        let sender = self.sender.clone();
        let pause_gate = self.pause_gate.clone();

        tokio::spawn(async move {
            info!("[dht_probe] PeerRepo 拉取任务已启动（间隔 {:?}）", PEER_REPO_POLL_INTERVAL);
            let mut interval = tokio::time::interval(PEER_REPO_POLL_INTERVAL);
            interval.tick().await; // 跳过第一次立即触发

            loop {
                interval.tick().await;

                // 全量同步期间暂停拉取新探测任务，把带宽/CPU 让给联邦同步
                if pause_gate.as_ref().map(|g| g.load(Ordering::Relaxed)).unwrap_or(false) {
                    continue;
                }

                // 从 PeerRepo 获取所有 peer
                let all_peers = peer_repo.all_peers_sync();
                if all_peers.is_empty() {
                    continue;
                }

                // 过滤掉已探测的，按优先级排序（高优先级优先探测）
                let mut to_probe: Vec<_> = {
                    let probed_set = probed.read();
                    all_peers
                        .into_iter()
                        .filter(|p| !probed_set.contains(&p.addr))
                        .collect()
                };

                if to_probe.is_empty() {
                    debug!("[dht_probe] PeerRepo 中所有 peer 都已探测，跳过");
                    continue;
                }

                // 按优先级降序排序（高优先级优先）
                to_probe.sort_by(|a, b| b.priority_score.partial_cmp(&a.priority_score).unwrap_or(std::cmp::Ordering::Equal));
                to_probe.truncate(MAX_PEERS_PER_POLL);

                // 加入探测队列
                let mut count = 0;
                {
                    let mut probed_set = probed.write();
                    for peer in &to_probe {
                        if probed_set.contains(&peer.addr) {
                            continue;
                        }
                        probed_set.insert(peer.addr);
                        // 防止缓存无限膨胀
                        if probed_set.len() > MAX_PROBED_CACHE {
                            let keys: Vec<SocketAddr> = probed_set.iter().take(MAX_PROBED_CACHE / 2).copied().collect();
                            for k in keys {
                                probed_set.remove(&k);
                            }
                        }
                        let _ = sender.send(peer.addr);
                        count += 1;
                    }
                }

                info!("[dht_probe] 从 PeerRepo 拉取 {} 个未探测 peer 加入队列（优先级最高的前 {} 个）", count, MAX_PEERS_PER_POLL);
            }
        });
    }

    /// 获取发送端（用于提交待探测地址）
    pub fn sender(&self) -> mpsc::UnboundedSender<SocketAddr> {
        self.sender.clone()
    }

    /// 批量提交待探测地址（自动去重）
    pub fn enqueue(&self, addrs: &[SocketAddr]) {
        let mut probed = self.probed.write();
        for addr in addrs {
            if probed.contains(addr) {
                continue;
            }
            probed.insert(*addr);
            // 防止缓存无限膨胀
            if probed.len() > MAX_PROBED_CACHE {
                // 简单策略：清空一半（FIFO 不保证，用随机淘汰）
                let keys: Vec<SocketAddr> = probed.iter().take(MAX_PROBED_CACHE / 2).copied().collect();
                for k in keys {
                    probed.remove(&k);
                }
            }
            let _ = self.sender.send(*addr);
        }
    }

    /// 启动后台探测任务
    fn start(&self, mut receiver: mpsc::UnboundedReceiver<SocketAddr>) {
        let routing_table = self.routing_table.clone();
        let node_repo = self.node_repo.clone();
        let probed = self.probed.clone();
        let node_id = self.node_id;
        let total_probed = self.total_probed.clone();
        let total_success = self.total_success.clone();
        let total_added = self.total_added.clone();

        tokio::spawn(async move {
            info!("[dht_probe] 探测任务已启动");

            loop {
                // 批量收集地址
                let mut batch = Vec::new();
                while batch.len() < MAX_CONCURRENT {
                    match receiver.recv().await {
                        Some(addr) => batch.push(addr),
                        None => {
                            info!("[dht_probe] channel 关闭，探测任务退出");
                            return;
                        }
                    }
                    // 尝试非阻塞地多取一些
                    while let Ok(addr) = receiver.try_recv() {
                        batch.push(addr);
                        if batch.len() >= MAX_CONCURRENT {
                            break;
                        }
                    }
                    if !batch.is_empty() {
                        break;
                    }
                }

                if batch.is_empty() {
                    continue;
                }

                debug!("[dht_probe] 批量探测 {} 个地址", batch.len());

                // 并发探测：直接 UDP ping（DHT 节点标准探测方式，跳过 TCP 握手）
                let mut tasks = Vec::new();
                for addr in batch {
                    // 过滤无效地址：私网、组播、保留地址、未指定地址
                    if Self::is_invalid_addr(addr) {
                        debug!("[dht_probe] 跳过无效地址: {}", addr);
                        continue;
                    }
                    let rt = routing_table.clone();
                    let probed_set = probed.clone();
                    let node_repo = node_repo.clone();
                    let total_probed = total_probed.clone();
                    let total_success = total_success.clone();
                    let total_added = total_added.clone();
                    tasks.push(tokio::spawn(async move {
                        total_probed.fetch_add(1, Ordering::Relaxed);
                        // 直接 UDP ping 确认 DHT 可达
                        match Self::ping_node(addr, node_id).await {
                            Ok(responder_id) => {
                                total_success.fetch_add(1, Ordering::Relaxed);
                                // 记录查询成功统计到 NodeRepo
                                if let Some(repo) = &node_repo {
                                    repo.record_query_sync(addr, true, 0);
                                }
                                // 方向C：同时加入路由表（DHT路由）和 NodeRepo（爬虫候选池，无容量限制）
                                let mut rt_added = false;
                                {
                                    let mut table = rt.write();
                                    if table.add_node(responder_id, addr) {
                                        rt_added = true;
                                    }
                                }
                                let repo_added = if let Some(repo) = &node_repo {
                                    repo.add_node_sync(responder_id, addr)
                                } else { false };
                                if rt_added || repo_added {
                                    total_added.fetch_add(1, Ordering::Relaxed);
                                    info!("[dht_probe] 节点加入: {} (id={}) 路由表={} NodeRepo={}",
                                        addr, hex::encode(&responder_id[..4]), rt_added, repo_added);
                                }
                            }
                            Err(e) => {
                                debug!("[dht_probe] UDP ping 失败 {}: {}", addr, e);
                                // 记录查询失败统计到 NodeRepo
                                if let Some(repo) = &node_repo {
                                    repo.record_query_sync(addr, false, 0);
                                }
                                // 失败的地址从 probed 移除，允许以后重试
                                let mut p = probed_set.write();
                                p.remove(&addr);
                            }
                        }
                    }));
                }

                for task in tasks {
                    let _ = task.await;
                }
            }
        });
    }

    /// 向单个节点发送 DHT ping，返回 responder node_id
    async fn ping_node(addr: SocketAddr, our_id: [u8; 20]) -> anyhow::Result<[u8; 20]> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        let tid = rand::thread_rng().gen::<[u8; 2]>();
        let request = DhtMessage::build_ping(&tid, &our_id);

        socket.send_to(&request, addr).await?;

        let mut buf = vec![0u8; 2048];
        let (n, _from) = tokio::time::timeout(PING_TIMEOUT, socket.recv_from(&mut buf)).await??;
        buf.truncate(n);

        // 解析 ping 响应
        let (_resp_tid, responder_id) = DhtMessage::parse_ping_response(&buf)
            .ok_or_else(|| anyhow::anyhow!("无法解析 ping 响应"))?;

        Ok(responder_id)
    }

    /// 检查地址是否无效（私网、组播、保留、未指定地址）
    fn is_invalid_addr(addr: SocketAddr) -> bool {
        let ip = addr.ip();
        if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
            return true;
        }
        if let std::net::IpAddr::V4(v4) = ip {
            let octets = v4.octets();
            // 私网地址
            if octets[0] == 10
                || (octets[0] == 172 && (16..=31).contains(&octets[1]))
                || (octets[0] == 192 && octets[1] == 168)
            {
                return true;
            }
            // 链路本地
            if octets[0] == 169 && octets[1] == 254 {
                return true;
            }
            // 保留地址
            if octets[0] == 0 || octets[0] >= 224 {
                return true;
            }
        }
        if let std::net::IpAddr::V6(v6) = ip {
            if v6.is_loopback() || v6.is_multicast() || v6.is_unspecified() {
                return true;
            }
        }
        false
    }

    /// TCP BT 握手预过滤（已弃用，保留供参考）
    ///
    /// 返回 Ok(true) 表示支持 DHT，Ok(false) 表示在线但不支持 DHT，
    /// Err 表示连接失败或握手失败。
    async fn tcp_handshake_check(addr: SocketAddr) -> anyhow::Result<bool> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        // TCP 连接（超时 5 秒）
        let stream = tokio::time::timeout(
            Duration::from_secs(5),
            TcpStream::connect(addr),
        ).await??;

        // 构造 BT 握手
        let mut handshake = Vec::with_capacity(68);
        handshake.push(19); // protocol name length
        handshake.extend_from_slice(b"BitTorrent protocol");
        // reserved bytes：设置 DHT 扩展位（第 7 字节最低位）
        let mut reserved = [0u8; 8];
        reserved[7] |= 0x01;
        handshake.extend_from_slice(&reserved);
        // infohash：随机（握手只检查支持的扩展，不验证 infohash）
        let mut infohash = [0u8; 20];
        rand::thread_rng().fill(&mut infohash);
        handshake.extend_from_slice(&infohash);
        // peer_id
        let mut peer_id = [0u8; 20];
        peer_id[0..8].copy_from_slice(b"-PD0003-");
        rand::thread_rng().fill(&mut peer_id[8..]);
        handshake.extend_from_slice(&peer_id);

        let mut stream = stream;
        stream.write_all(&handshake).await?;

        // 读取响应握手（68 字节）
        let mut resp = vec![0u8; 68];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut resp)).await??;

        // 验证响应格式
        if resp[0] != 19 || &resp[1..20] != b"BitTorrent protocol" {
            return Err(anyhow::anyhow!("无效的 BT 握手响应"));
        }

        // 检查 DHT 扩展位（reserved 第 7 字节最低位）
        let supports_dht = resp[27] & 0x01 != 0;
        Ok(supports_dht)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_probe_creation() {
        let rt = Arc::new(RwLock::new(RoutingTable::new([0u8; 20])));
        let probe = DhtProbe::new(rt, None, None, None);
        let _sender = probe.sender();
    }

    #[test]
    fn test_parse_ping_response() {
        // 构造 ping 响应
        let response = DhtMessage::build_ping_response(b"aa", &[1u8; 20]);
        let (tid, node_id) = DhtMessage::parse_ping_response(&response).unwrap();
        assert_eq!(tid, b"aa");
        assert_eq!(node_id, [1u8; 20]);
    }

    #[test]
    fn test_parse_ping_response_invalid() {
        let invalid = b"d1:rd2:id10:1234567890e1:y1:re";
        assert!(DhtMessage::parse_ping_response(invalid).is_none());
    }
}
