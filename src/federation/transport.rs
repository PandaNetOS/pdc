//! UDP 打洞传输层
//!
//! 封装 tokio UdpSocket，用于 NAT 打洞与 UDP 打洞信令。
//!
//! # TCP 承载已下沉 SDK
//!
//! 原 `TcpTransport`（粘包/半包处理、写超时 + 退避重试、读写分离双锁）是**纯通用**能力，
//! 已提取为 `pnos-net` 的 `session/frame.rs::FrameTransport`（迁移计划 K2「提取而非搬运」）。
//! 本文件因此只保留 UDP 打洞所需的 [`UdpTransport`]。

use std::sync::Arc;
use std::time::Duration;

use crate::net::socket_opts::{create_std_udp_socket, create_udp_socket};

/// 打洞包发送轮询间隔
const TRANSPORT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// UDP 打洞传输层
///
/// 封装 tokio UdpSocket，用于 NAT 打洞和 UDP 数据传输。
pub struct UdpTransport {
    socket: tokio::net::UdpSocket,
}

/// 打洞包魔数 "PDCF"
pub const HOLE_PUNCH_MAGIC: u32 = 0x5044_4346;

impl UdpTransport {
    /// 绑定 UDP 端口
    pub async fn bind(addr: std::net::SocketAddr) -> anyhow::Result<Arc<Self>> {
        let socket = create_udp_socket(addr)
            .await
            .map_err(|e| anyhow::anyhow!("UDP 绑定失败 {}: {}", addr, e))?;
        Ok(Arc::new(Self { socket }))
    }

    /// 同步绑定（用于非 async 上下文）
    pub fn try_bind(addr: std::net::SocketAddr) -> anyhow::Result<Arc<Self>> {
        let std_socket = create_std_udp_socket(addr)
            .map_err(|e| anyhow::anyhow!("UDP 绑定失败 {}: {}", addr, e))?;
        std_socket
            .set_nonblocking(true)
            .map_err(|e| anyhow::anyhow!("设置非阻塞失败: {}", e))?;
        let socket = tokio::net::UdpSocket::from_std(std_socket)
            .map_err(|e| anyhow::anyhow!("转换 tokio UdpSocket 失败: {}", e))?;
        Ok(Arc::new(Self { socket }))
    }

    /// 向目标持续发送 UDP 打洞包，持续指定时长
    ///
    /// 打洞包格式：4字节魔数 + 20字节 node_id
    pub async fn hole_punch(
        &self,
        remote_addr: std::net::SocketAddr,
        node_id: &[u8; 20],
        duration: std::time::Duration,
    ) -> anyhow::Result<()> {
        let mut packet = Vec::with_capacity(24);
        packet.extend_from_slice(&HOLE_PUNCH_MAGIC.to_be_bytes());
        packet.extend_from_slice(node_id);

        let start = std::time::Instant::now();
        // [ALLOWED-INTERVAL] 联邦协议级维护循环，后续 ICC 阶段迁移到 TaskScheduler
        let mut interval = tokio::time::interval(TRANSPORT_POLL_INTERVAL);

        while start.elapsed() < duration {
            interval.tick().await;
            match self.socket.send_to(&packet, remote_addr).await {
                Ok(_) => {}
                Err(e) => {
                    tracing::debug!("[federation] UDP 打洞发送失败 {}: {}", remote_addr, e);
                }
            }
        }
        Ok(())
    }

    /// 发送数据到指定地址
    pub async fn send_to(&self, data: &[u8], addr: std::net::SocketAddr) -> anyhow::Result<usize> {
        self.socket
            .send_to(data, addr)
            .await
            .map_err(|e| anyhow::anyhow!("UDP 发送失败: {}", e))
    }

    /// 接收数据
    pub async fn recv_from(&self, buf: &mut [u8]) -> anyhow::Result<(usize, std::net::SocketAddr)> {
        self.socket
            .recv_from(buf)
            .await
            .map_err(|e| anyhow::anyhow!("UDP 接收失败: {}", e))
    }

    /// 本地地址
    pub fn local_addr(&self) -> anyhow::Result<std::net::SocketAddr> {
        self.socket
            .local_addr()
            .map_err(|e| anyhow::anyhow!("获取本地地址失败: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_udp_transport_bind() {
        let udp = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = udp.local_addr().unwrap();
        assert!(addr.port() > 0);
    }

    #[tokio::test]
    async fn test_udp_send_recv() {
        let receiver = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let recv_addr = receiver.local_addr().unwrap();

        let sender = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let data = b"hello udp";
        sender.send_to(data, recv_addr).await.unwrap();

        let mut buf = [0u8; 64];
        let (len, _from) = receiver.recv_from(&mut buf).await.unwrap();
        assert_eq!(len, data.len());
        assert_eq!(&buf[..len], data);
    }

    #[tokio::test]
    async fn test_hole_punch_magic() {
        assert_eq!(HOLE_PUNCH_MAGIC.to_be_bytes(), *b"PDCF");
    }
}
