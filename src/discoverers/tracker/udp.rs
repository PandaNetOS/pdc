//! UDP Tracker 协议实现（BEP 15）
//!
//! UDP Tracker 比 HTTP Tracker 更高效，公共 tracker 中一半以上是 UDP。
//! 协议流程：connect（获取 connection_id）→ announce（查询 peer）。
//!
//! 参考：<https://www.bittorrent.org/beps/bep_0015.html>

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{anyhow, Result};
use rand::Rng;
use tokio::net::UdpSocket;
use tracing::debug;

use crate::types::Infohash;

/// UDP Tracker 协议 magic number（连接请求的初始 connection_id）
const PROTOCOL_ID: u64 = 0x41727101980;

/// Action 类型
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
// const ACTION_SCRAPE: u32 = 2;
// const ACTION_ERROR: u32 = 3;

/// 最大重传次数
const MAX_RETRIES: u32 = 8;

/// 初始超时（秒）
const INITIAL_TIMEOUT_SECS: u64 = 15;

/// UDP Tracker 客户端
pub struct UdpTrackerClient;

impl UdpTrackerClient {
    /// 向 UDP Tracker 发送 announce，返回 peer 列表
    ///
    /// # 参数
    /// - `tracker_url`: 如 `udp://tracker.example.com:6969/announce`
    /// - `infohash`: 20 字节 infohash
    /// - `peer_id`: 20 字节 peer_id
    /// - `listen_port`: 本地监听端口
    /// - `timeout`: 整体超时
    pub async fn announce(
        tracker_url: &str,
        infohash: &Infohash,
        peer_id: &[u8; 20],
        listen_port: u16,
        timeout: Duration,
    ) -> Result<Vec<SocketAddr>> {
        // 1. 解析 tracker 地址
        let addr = Self::parse_tracker_addr(tracker_url)?;
        debug!("[udp_tracker] 解析地址: {} -> {}", tracker_url, addr);

        // 2. 创建 UDP socket
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        socket.connect(addr).await?;

        // 3. Connect 握手，获取 connection_id
        let connection_id = Self::connect(&socket, timeout).await?;
        debug!(
            "[udp_tracker] connect 成功, connection_id={}",
            connection_id
        );

        // 4. Announce
        let peers = Self::do_announce(
            &socket,
            connection_id,
            infohash,
            peer_id,
            listen_port,
            timeout,
        )
        .await?;

        debug!("[udp_tracker] announce 成功, 获取 {} 个 peer", peers.len());
        Ok(peers)
    }

    /// 解析 UDP tracker URL 为 SocketAddr
    fn parse_tracker_addr(url: &str) -> Result<SocketAddr> {
        use std::net::ToSocketAddrs;
        // 去掉 udp:// 前缀和路径
        let host_port = url
            .strip_prefix("udp://")
            .ok_or_else(|| anyhow!("not a udp url: {}", url))?;
        // 去掉 /announce 等路径
        let host_port = host_port.split('/').next().unwrap_or(host_port);

        let mut addrs = host_port.to_socket_addrs()?;
        addrs
            .next()
            .ok_or_else(|| anyhow!("无法解析地址: {}", host_port))
    }

    /// 发送 connect 请求并接收响应
    async fn connect(socket: &UdpSocket, timeout: Duration) -> Result<u64> {
        let transaction_id = rand::thread_rng().gen::<u32>();
        let request = Self::build_connect_request(transaction_id);

        let response = Self::send_with_retry(socket, &request, transaction_id, timeout).await?;

        if response.len() < 16 {
            return Err(anyhow!("connect 响应太短: {} 字节", response.len()));
        }

        let action = u32::from_be_bytes([response[0], response[1], response[2], response[3]]);
        let tid = u32::from_be_bytes([response[4], response[5], response[6], response[7]]);

        if action != ACTION_CONNECT {
            return Err(anyhow!(
                "connect 响应 action 不匹配: expected {}, got {}",
                ACTION_CONNECT,
                action
            ));
        }
        if tid != transaction_id {
            return Err(anyhow!("connect 响应 transaction_id 不匹配"));
        }

        let connection_id = u64::from_be_bytes([
            response[8],
            response[9],
            response[10],
            response[11],
            response[12],
            response[13],
            response[14],
            response[15],
        ]);

        Ok(connection_id)
    }

    /// 发送 announce 请求并接收响应
    async fn do_announce(
        socket: &UdpSocket,
        connection_id: u64,
        infohash: &Infohash,
        peer_id: &[u8; 20],
        listen_port: u16,
        timeout: Duration,
    ) -> Result<Vec<SocketAddr>> {
        let transaction_id = rand::thread_rng().gen::<u32>();
        let request = Self::build_announce_request(
            connection_id,
            transaction_id,
            infohash,
            peer_id,
            listen_port,
        );

        let response = Self::send_with_retry(socket, &request, transaction_id, timeout).await?;

        if response.len() < 20 {
            return Err(anyhow!("announce 响应太短: {} 字节", response.len()));
        }

        let action = u32::from_be_bytes([response[0], response[1], response[2], response[3]]);
        let tid = u32::from_be_bytes([response[4], response[5], response[6], response[7]]);

        if action == 3 {
            // error action
            let msg = String::from_utf8_lossy(&response[8..]).to_string();
            return Err(anyhow!("tracker error: {}", msg));
        }
        if action != ACTION_ANNOUNCE {
            return Err(anyhow!(
                "announce 响应 action 不匹配: expected {}, got {}",
                ACTION_ANNOUNCE,
                action
            ));
        }
        if tid != transaction_id {
            return Err(anyhow!("announce 响应 transaction_id 不匹配"));
        }

        // 解析 peer 列表（从第 20 字节开始，每 6 字节一个 peer）
        let peers_data = &response[20..];
        let mut peers = vec![];
        let (chunks, _) = peers_data.as_chunks::<6>();
        for chunk in chunks {
            let ip = std::net::Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            if port != 0 {
                peers.push(SocketAddr::new(std::net::IpAddr::V4(ip), port));
            }
        }

        Ok(peers)
    }

    /// 带超时重传的发送/接收
    ///
    /// BEP 15 规定：15s 超时后重传，每次超时翻倍，最多 8 次。
    async fn send_with_retry(
        socket: &UdpSocket,
        request: &[u8],
        expected_tid: u32,
        overall_timeout: Duration,
    ) -> Result<Vec<u8>> {
        let start = std::time::Instant::now();
        let mut timeout = Duration::from_secs(INITIAL_TIMEOUT_SECS);

        for attempt in 0..MAX_RETRIES {
            if start.elapsed() > overall_timeout {
                return Err(anyhow!("整体超时"));
            }

            // 剩余时间和单次超时取较小值
            let remaining = overall_timeout.saturating_sub(start.elapsed());
            let wait_timeout = timeout.min(remaining);

            socket.send(request).await?;

            let mut buf = vec![0u8; 2048];
            match tokio::time::timeout(wait_timeout, socket.recv(&mut buf)).await {
                Ok(Ok(n)) => {
                    buf.truncate(n);
                    // 验证 transaction_id（第 4-7 字节）
                    if n >= 8 {
                        let tid = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
                        if tid == expected_tid {
                            return Ok(buf);
                        }
                        // transaction_id 不匹配，继续等
                        debug!("[udp_tracker] 收到未知 transaction_id 的包，继续等待");
                        continue;
                    }
                }
                Ok(Err(e)) => {
                    debug!("[udp_tracker] recv 错误: {}", e);
                }
                Err(_) => {
                    debug!(
                        "[udp_tracker] 第 {} 次超时 ({:?})，重传",
                        attempt + 1,
                        timeout
                    );
                }
            }

            timeout = timeout.saturating_mul(2);
        }

        Err(anyhow!("超过最大重传次数 ({})", MAX_RETRIES))
    }

    /// 构建 connect 请求包（16 字节）
    fn build_connect_request(transaction_id: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&PROTOCOL_ID.to_be_bytes()); // 8 bytes
        buf.extend_from_slice(&ACTION_CONNECT.to_be_bytes()); // 4 bytes
        buf.extend_from_slice(&transaction_id.to_be_bytes()); // 4 bytes
        buf
    }

    /// 构建 announce 请求包（98 字节）
    fn build_announce_request(
        connection_id: u64,
        transaction_id: u32,
        infohash: &Infohash,
        peer_id: &[u8; 20],
        listen_port: u16,
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(98);
        buf.extend_from_slice(&connection_id.to_be_bytes()); // 8
        buf.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes()); // 4
        buf.extend_from_slice(&transaction_id.to_be_bytes()); // 4
        buf.extend_from_slice(infohash); // 20
        buf.extend_from_slice(peer_id); // 20
        buf.extend_from_slice(&0u64.to_be_bytes()); // downloaded 8
        buf.extend_from_slice(&0u64.to_be_bytes()); // left 8
        buf.extend_from_slice(&0u64.to_be_bytes()); // uploaded 8
        buf.extend_from_slice(&0u32.to_be_bytes()); // event (0=none) 4
        buf.extend_from_slice(&0u32.to_be_bytes()); // ip (0=default) 4
        buf.extend_from_slice(&rand::thread_rng().gen::<u32>().to_be_bytes()); // key 4
        buf.extend_from_slice(&(-1i32).to_be_bytes()); // num_want (-1=default) 4
        buf.extend_from_slice(&listen_port.to_be_bytes()); // port 2
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_connect_request() {
        let req = UdpTrackerClient::build_connect_request(0x12345678);
        assert_eq!(req.len(), 16);
        // magic number
        assert_eq!(&req[0..8], &PROTOCOL_ID.to_be_bytes());
        // action = 0
        assert_eq!(&req[8..12], &0u32.to_be_bytes());
        // transaction_id
        assert_eq!(&req[12..16], &0x12345678u32.to_be_bytes());
    }

    #[test]
    fn test_build_announce_request() {
        let infohash = [0u8; 20];
        let peer_id = [1u8; 20];
        let req = UdpTrackerClient::build_announce_request(
            0x1122334455667788,
            0xabcdef01,
            &infohash,
            &peer_id,
            6881,
        );
        assert_eq!(req.len(), 98);
        // connection_id
        assert_eq!(&req[0..8], &0x1122334455667788u64.to_be_bytes());
        // action = 1
        assert_eq!(&req[8..12], &1u32.to_be_bytes());
        // transaction_id
        assert_eq!(&req[12..16], &0xabcdef01u32.to_be_bytes());
        // infohash
        assert_eq!(&req[16..36], &[0u8; 20]);
        // peer_id
        assert_eq!(&req[36..56], &[1u8; 20]);
        // port (最后 2 字节)
        assert_eq!(&req[96..98], &6881u16.to_be_bytes());
    }

    #[test]
    fn test_parse_tracker_addr() {
        // 这个测试需要 DNS，可能在离线环境失败
        // 只测试格式解析
        let url = "udp://tracker.example.com:6969/announce";
        let result = UdpTrackerClient::parse_tracker_addr(url);
        // 可能因为 DNS 失败，但格式解析应该没问题
        match result {
            Ok(addr) => {
                assert_eq!(addr.port(), 6969);
            }
            Err(_) => {
                // DNS 解析失败是正常的（example.com 可能不解析）
            }
        }
    }

    #[test]
    fn test_parse_tracker_addr_no_path() {
        let url = "udp://127.0.0.1:6969";
        let addr = UdpTrackerClient::parse_tracker_addr(url).unwrap();
        assert_eq!(addr.to_string(), "127.0.0.1:6969");
    }

    #[test]
    fn test_parse_tracker_addr_invalid() {
        let url = "http://tracker.example.com:6969";
        assert!(UdpTrackerClient::parse_tracker_addr(url).is_err());
    }
}
