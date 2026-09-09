//! TCP 传输层
//!
//! 封装 tokio TcpStream，处理粘包/半包，提供基于帧的消息收发。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex as TokioMutex;

use crate::federation::protocol::{
    decode_frame, encode_message, frame_size_in_buffer, MessageType, FRAME_HEADER_SIZE,
};

/// TCP 读取端（含读取缓冲区）
struct TcpReader {
    reader: tokio::net::tcp::OwnedReadHalf,
    read_buf: BytesMut,
}

/// TCP 写入端
struct TcpWriter {
    writer: tokio::net::tcp::OwnedWriteHalf,
}

/// TCP 传输层（读写分离，独立锁，可并发收发）
///
/// 内部使用 owned split 分离读写，读和写各持独立锁，
/// 发送和接收可并发执行，互不阻塞。
pub struct TcpTransport {
    reader: TokioMutex<TcpReader>,
    writer: TokioMutex<TcpWriter>,
    peer: Option<SocketAddr>,
    local: Option<SocketAddr>,
}

impl TcpTransport {
    /// 从已有的 TcpStream 创建传输层
    pub fn new(stream: TcpStream) -> Self {
        let peer = stream.peer_addr().ok();
        let local = stream.local_addr().ok();
        let (reader, writer) = stream.into_split();
        Self {
            reader: TokioMutex::new(TcpReader {
                reader,
                read_buf: BytesMut::with_capacity(8192),
            }),
            writer: TokioMutex::new(TcpWriter { writer }),
            peer,
            local,
        }
    }

    /// 主动连接到远端
    pub async fn connect(addr: SocketAddr) -> anyhow::Result<Self> {
        let stream = tokio::time::timeout(
            Duration::from_secs(10),
            TcpStream::connect(addr),
        )
        .await
        .map_err(|_| anyhow::anyhow!("连接超时: {}", addr))?
        .map_err(|e| anyhow::anyhow!("连接失败 {}: {}", addr, e))?;
        stream.set_nodelay(true).map_err(|e| anyhow::anyhow!("set_nodelay 失败: {}", e))?;
        Ok(Self::new(stream))
    }

    /// 绑定监听地址，返回 TcpListener
    pub async fn bind(addr: SocketAddr) -> anyhow::Result<TcpListener> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| anyhow::anyhow!("绑定失败 {}: {}", addr, e))?;
        Ok(listener)
    }

    /// 发送消息（编码为完整帧并写入）
    pub async fn send_message<T: Serialize>(
        &self,
        msg_type: MessageType,
        msg: &T,
    ) -> anyhow::Result<()> {
        let frame = encode_message(msg_type, msg)?;
        let mut writer = self.writer.lock().await;
        writer
            .writer
            .write_all(&frame)
            .await
            .map_err(|e| anyhow::anyhow!("写入失败: {}", e))?;
        Ok(())
    }

    /// 发送原始帧字节
    pub async fn send_raw(&self, frame: &[u8]) -> anyhow::Result<()> {
        let mut writer = self.writer.lock().await;
        writer
            .writer
            .write_all(frame)
            .await
            .map_err(|e| anyhow::anyhow!("写入失败: {}", e))?;
        Ok(())
    }

    /// 接收一条完整消息
    ///
    /// 循环读取直到获得完整帧，返回 `(消息类型, payload 字节)`。
    pub async fn recv_message(&self) -> anyhow::Result<(MessageType, Vec<u8>)> {
        let mut reader = self.reader.lock().await;
        loop {
            // 检查缓冲区中是否已有完整帧
            if let Some(frame_len) = frame_size_in_buffer(&reader.read_buf) {
                let frame = reader.read_buf.split_to(frame_len);
                let (msg_type, payload) = decode_frame(&frame)?;
                return Ok((msg_type, payload.to_vec()));
            }

            // 缓冲区不足，读取更多数据
            let mut tmp = [0u8; 8192];
            let n = reader
                .reader
                .read(&mut tmp)
                .await
                .map_err(|e| anyhow::anyhow!("读取失败: {}", e))?;
            if n == 0 {
                anyhow::bail!("连接已关闭");
            }
            reader.read_buf.put_slice(&tmp[..n]);
        }
    }

    /// 获取对端地址
    pub fn peer_addr(&self) -> anyhow::Result<SocketAddr> {
        self.peer.ok_or_else(|| anyhow::anyhow!("获取对端地址失败"))
    }

    /// 获取本地地址
    pub fn local_addr(&self) -> anyhow::Result<SocketAddr> {
        self.local.ok_or_else(|| anyhow::anyhow!("获取本地地址失败"))
    }

    /// 关闭连接
    pub async fn close(&self) -> anyhow::Result<()> {
        let mut writer = self.writer.lock().await;
        writer
            .writer
            .shutdown()
            .await
            .map_err(|e| anyhow::anyhow!("关闭连接失败: {}", e))?;
        Ok(())
    }
}

impl std::fmt::Debug for TcpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpTransport")
            .field("peer", &self.peer)
            .field("local", &self.local)
            .finish()
    }
}


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
        let socket = tokio::net::UdpSocket::bind(addr)
            .await
            .map_err(|e| anyhow::anyhow!("UDP 绑定失败 {}: {}", addr, e))?;
        Ok(Arc::new(Self { socket }))
    }

    /// 同步绑定（用于非 async 上下文）
    pub fn try_bind(addr: std::net::SocketAddr) -> anyhow::Result<Arc<Self>> {
        let std_socket = std::net::UdpSocket::bind(addr)
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
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));

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
    use crate::federation::protocol::{PingMessage, PongMessage};

    #[tokio::test]
    async fn test_transport_ping_pong() {
        let listener = TcpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let server_addr = listener.local_addr().unwrap();

        // 服务端接受连接
        let server_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut transport = TcpTransport::new(stream);
            let (msg_type, payload) = transport.recv_message().await.unwrap();
            assert_eq!(msg_type, MessageType::Ping);
            let ping: PingMessage = bincode::deserialize(&payload).unwrap();
            // 回复 Pong
            let pong = PongMessage {
                timestamp: ping.timestamp,
                rtt_estimate_ms: 1,
            };
            transport
                .send_message(MessageType::Pong, &pong)
                .await
                .unwrap();
        });

        // 客户端连接
        let mut client = TcpTransport::connect(server_addr).await.unwrap();
        let ping = PingMessage { timestamp: 42 };
        client.send_message(MessageType::Ping, &ping).await.unwrap();

        let (msg_type, payload) = client.recv_message().await.unwrap();
        assert_eq!(msg_type, MessageType::Pong);
        let pong: PongMessage = bincode::deserialize(&payload).unwrap();
        assert_eq!(pong.timestamp, 42);
        assert_eq!(pong.rtt_estimate_ms, 1);

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_transport_multiple_messages() {
        let listener = TcpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut transport = TcpTransport::new(stream);
            for i in 0..5 {
                let (msg_type, payload) = transport.recv_message().await.unwrap();
                assert_eq!(msg_type, MessageType::Ping);
                let ping: PingMessage = bincode::deserialize(&payload).unwrap();
                assert_eq!(ping.timestamp, i);
            }
        });

        let mut client = TcpTransport::connect(server_addr).await.unwrap();
        for i in 0..5 {
            let ping = PingMessage { timestamp: i };
            client.send_message(MessageType::Ping, &ping).await.unwrap();
        }

        server_handle.await.unwrap();
    }

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
        let (len, from) = receiver.recv_from(&mut buf).await.unwrap();
        assert_eq!(len, data.len());
        assert_eq!(&buf[..len], data);
    }

    #[tokio::test]
    async fn test_hole_punch_magic() {
        assert_eq!(HOLE_PUNCH_MAGIC.to_be_bytes(), *b"PDCF");
    }

    #[tokio::test]
    async fn test_transport_connect_timeout() {
        // 连接一个不可达的地址，应该超时
        let result = TcpTransport::connect("10.255.255.1:1".parse().unwrap()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_transport_peer_addr() {
        let listener = TcpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let transport = TcpTransport::new(stream);
            assert!(transport.peer_addr().is_ok());
        });

        let client = TcpTransport::connect(server_addr).await.unwrap();
        assert_eq!(client.peer_addr().unwrap(), server_addr);

        server_handle.await.unwrap();
    }
}
