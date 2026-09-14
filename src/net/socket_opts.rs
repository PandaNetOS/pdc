//! UDP socket 选项封装
//!
//! 统一创建 UDP socket 并设置内核缓冲区大小，减少高并发场景下的丢包。
//!
//! 使用 socket2 设置 SO_RCVBUF / SO_SNDBUF，因为 tokio/std 的 UdpSocket 不直接暴露这些设置。

use std::net::SocketAddr;

use anyhow::Result;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

/// 默认接收缓冲区大小：4 MB
pub const UDP_RECV_BUF_SIZE: usize = 4 * 1024 * 1024;
/// 默认发送缓冲区大小：1 MB
pub const UDP_SEND_BUF_SIZE: usize = 1024 * 1024;

/// 根据地址选择 IPv4 或 IPv6 域
fn domain_for_addr(addr: &SocketAddr) -> Domain {
    if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    }
}

/// 创建并配置 UDP socket（tokio 异步）
///
/// - SO_RCVBUF = 4 MB（[`UDP_RECV_BUF_SIZE`]）
/// - SO_SNDBUF = 1 MB（[`UDP_SEND_BUF_SIZE`]）
///
/// 缓冲区大小设置失败时静默忽略（操作系统可能限制最大值）。
pub async fn create_udp_socket(addr: SocketAddr) -> Result<UdpSocket> {
    let std_socket = create_raw_std_socket(addr)?;
    let tokio_socket = UdpSocket::from_std(std_socket)?;
    Ok(tokio_socket)
}

/// 创建并配置 UDP socket（std::net，同步）
///
/// 适用于需要先设置选项再转换为 tokio 的场景（如 federation try_bind）。
/// 返回的 socket 已设为非阻塞模式，可直接用于 `UdpSocket::from_std()`。
pub fn create_std_udp_socket(addr: SocketAddr) -> Result<std::net::UdpSocket> {
    create_raw_std_socket(addr)
}

/// 内部：用 socket2 创建、配置、绑定 UDP socket，设为非阻塞
fn create_raw_std_socket(addr: SocketAddr) -> Result<std::net::UdpSocket> {
    let socket = Socket::new(domain_for_addr(&addr), Type::DGRAM, Some(Protocol::UDP))?;
    // 设置缓冲区大小，忽略错误（OS 可能限制最大值）
    let _ = socket.set_recv_buffer_size(UDP_RECV_BUF_SIZE);
    let _ = socket.set_send_buffer_size(UDP_SEND_BUF_SIZE);
    // 允许地址复用（崩溃后快速重启）
    let _ = socket.set_reuse_address(true);
    socket.bind(&addr.into())?;
    // 转为非阻塞，供 tokio 使用
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}
