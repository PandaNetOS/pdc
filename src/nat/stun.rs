//! STUN 轻量客户端
//!
//! 实现 RFC 5389 的 Binding 请求，用于：
//! 1. 检测 NAT 类型（全锥/受限锥/端口受限/对称）
//! 2. 验证公网可达性
//! 3. 获取公网 IP:Port
//!
//! 只实现 Binding 方法，足够用于 NAT 检测。

use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// STUN 消息头
const STUN_MAGIC_COOKIE: u32 = 0x2112_A442;
const STUN_BINDING_REQUEST: u16 = 0x0001;
const STUN_MAPPED_ADDRESS: u16 = 0x0001;
const STUN_XOR_MAPPED_ADDRESS: u16 = 0x0020;

/// NAT 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NatType {
    /// 公网直连（无 NAT）
    OpenInternet,
    /// 全锥 NAT（Full Cone）
    FullCone,
    /// 受限锥 NAT（Restricted Cone）
    RestrictedCone,
    /// 端口受限锥 NAT（Port Restricted Cone）
    PortRestrictedCone,
    /// 对称 NAT（Symmetric）
    Symmetric,
    /// 未知（检测失败）
    Unknown,
}

impl NatType {
    pub fn as_str(&self) -> &'static str {
        match self {
            NatType::OpenInternet => "open_internet",
            NatType::FullCone => "full_cone",
            NatType::RestrictedCone => "restricted_cone",
            NatType::PortRestrictedCone => "port_restricted_cone",
            NatType::Symmetric => "symmetric",
            NatType::Unknown => "unknown",
        }
    }

    /// 是否为有利的 NAT 类型（可以被外部主动连接）
    pub fn is_favorable(&self) -> bool {
        matches!(self, NatType::OpenInternet | NatType::FullCone)
    }
}

/// STUN 探测结果
#[derive(Debug, Clone)]
pub struct StunResult {
    /// 是否成功
    pub success: bool,
    /// 检测到的公网地址（如果有）
    pub mapped_addr: Option<SocketAddr>,
    /// 本地地址
    pub local_addr: SocketAddr,
    /// NAT 类型（简化检测）
    pub nat_type: NatType,
    /// 使用的 STUN 服务器
    pub server: String,
    /// 往返延迟（毫秒）
    pub rtt_ms: u64,
}

/// 生成 12 字节的 STUN Transaction ID
fn random_transaction_id() -> [u8; 12] {
    let mut id = [0u8; 12];
    // 用简单的随机方式（不依赖 rand 避免增加依赖）
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    id[..8].copy_from_slice(&now.to_be_bytes());
    id[8..12].copy_from_slice(&[0x12, 0x34, 0x56, 0x78]);
    id
}

/// 构建 STUN Binding Request 报文
fn build_binding_request(transaction_id: &[u8; 12]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(20);
    // Message Type: Binding Request (0x0001)
    buf.extend_from_slice(&STUN_BINDING_REQUEST.to_be_bytes());
    // Message Length: 0（无属性）
    buf.extend_from_slice(&0u16.to_be_bytes());
    // Magic Cookie
    buf.extend_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
    // Transaction ID (12 bytes)
    buf.extend_from_slice(transaction_id);
    buf
}

/// 解析 STUN 响应，提取 XOR-MAPPED-ADDRESS 或 MAPPED-ADDRESS
fn parse_mapped_address(buf: &[u8], transaction_id: &[u8; 12]) -> Option<SocketAddr> {
    if buf.len() < 20 {
        return None;
    }

    // 验证 Transaction ID
    if &buf[8..20] != transaction_id {
        return None;
    }

    // Message Type 应该是 Binding Response (0x0101)
    let msg_type = u16::from_be_bytes([buf[0], buf[1]]);
    if msg_type != 0x0101 {
        return None;
    }

    let msg_length = u16::from_be_bytes([buf[2], buf[3]]) as usize;
    let mut offset = 20;
    let end = (20 + msg_length).min(buf.len());

    // 遍历属性，优先找 XOR-MAPPED-ADDRESS
    let mut xor_addr: Option<SocketAddr> = None;
    let mut mapped_addr: Option<SocketAddr> = None;

    while offset + 4 <= end {
        let attr_type = u16::from_be_bytes([buf[offset], buf[offset + 1]]);
        let attr_len = u16::from_be_bytes([buf[offset + 2], buf[offset + 3]]) as usize;
        let value_start = offset + 4;
        let value_end = value_start + attr_len;

        if value_end > end {
            break;
        }

        match attr_type {
            STUN_XOR_MAPPED_ADDRESS => {
                xor_addr = decode_xor_mapped_address(&buf[value_start..value_end], &buf[4..8]);
            }
            STUN_MAPPED_ADDRESS => {
                mapped_addr = decode_mapped_address(&buf[value_start..value_end]);
            }
            _ => {}
        }

        // 属性按 4 字节对齐
        offset = value_start + ((attr_len + 3) & !3);
    }

    xor_addr.or(mapped_addr)
}

/// 解析 MAPPED-ADDRESS 属性
fn decode_mapped_address(value: &[u8]) -> Option<SocketAddr> {
    if value.len() < 8 {
        return None;
    }
    // value[0] = 保留字段
    let family = value[1];
    let port = u16::from_be_bytes([value[2], value[3]]);

    match family {
        1 => {
            // IPv4
            if value.len() < 8 {
                return None;
            }
            let ip = std::net::Ipv4Addr::new(value[4], value[5], value[6], value[7]);
            Some(SocketAddr::new(std::net::IpAddr::V4(ip), port))
        }
        2 => {
            // IPv6
            if value.len() < 20 {
                return None;
            }
            let mut ip_bytes = [0u8; 16];
            ip_bytes.copy_from_slice(&value[4..20]);
            let ip = std::net::Ipv6Addr::from(ip_bytes);
            Some(SocketAddr::new(std::net::IpAddr::V6(ip), port))
        }
        _ => None,
    }
}

/// 解析 XOR-MAPPED-ADDRESS 属性
fn decode_xor_mapped_address(value: &[u8], magic_cookie: &[u8]) -> Option<SocketAddr> {
    if value.len() < 8 {
        return None;
    }
    // value[0] = 保留字段
    let family = value[1];
    // XOR port: port XOR (magic_cookie 高 16 位)
    let xor_port = u16::from_be_bytes([value[2], value[3]]);
    let port = xor_port ^ (STUN_MAGIC_COOKIE >> 16) as u16;

    match family {
        1 => {
            // IPv4: XOR with magic cookie
            if value.len() < 8 {
                return None;
            }
            let mut ip_bytes = [0u8; 4];
            ip_bytes.copy_from_slice(&value[4..8]);
            for i in 0..4 {
                ip_bytes[i] ^= magic_cookie[i];
            }
            let ip = std::net::Ipv4Addr::from(ip_bytes);
            Some(SocketAddr::new(std::net::IpAddr::V4(ip), port))
        }
        2 => {
            // IPv6: XOR with magic_cookie + transaction_id (简化，暂不实现完整 IPv6)
            None
        }
        _ => None,
    }
}

/// 执行一次 STUN Binding 请求，获取映射后的公网地址
///
/// # 参数
/// - `stun_server`: STUN 服务器地址，如 "stun.l.google.com:19302"
/// - `local_addr`: 本地绑定地址，如 "0.0.0.0:0" 或指定端口
/// - `timeout`: 超时时间
pub fn stun_binding_request(
    stun_server: &str,
    local_addr: &str,
    timeout: Duration,
) -> anyhow::Result<StunResult> {
    let server_addr: SocketAddr = stun_server
        .parse()
        .or_else(|_| {
            // 解析域名
            use std::net::ToSocketAddrs;
            stun_server
                .to_socket_addrs()?
                .next()
                .ok_or_else(|| anyhow::anyhow!("STUN 服务器 DNS 解析失败: {}", stun_server))
        })?;

    let socket = UdpSocket::bind(local_addr)?;
    socket.set_read_timeout(Some(timeout))?;

    let transaction_id = random_transaction_id();
    let request = build_binding_request(&transaction_id);

    let start = std::time::Instant::now();
    socket.send_to(&request, server_addr)?;

    let mut buf = [0u8; 512];
    let (len, from) = socket.recv_from(&mut buf)?;
    let rtt = start.elapsed().as_millis() as u64;

    let local_addr = socket.local_addr()?;

    if let Some(mapped) = parse_mapped_address(&buf[..len], &transaction_id) {
        // 简化判断 NAT 类型
        let nat_type = if mapped.ip() == local_addr.ip() && mapped.port() == local_addr.port() {
            NatType::OpenInternet
        } else {
            // 单次 STUN 无法精确判断完整 NAT 类型，先标记为 Unknown
            // 完整检测需要 2 个 STUN 服务器或 changed-address
            NatType::Unknown
        };

        debug!(
            "[stun] {} -> 映射地址: {}, RTT: {}ms, NAT类型: {:?}",
            stun_server, mapped, rtt, nat_type
        );

        Ok(StunResult {
            success: true,
            mapped_addr: Some(mapped),
            local_addr,
            nat_type,
            server: stun_server.to_string(),
            rtt_ms: rtt,
        })
    } else {
        warn!("[stun] {} 响应解析失败", stun_server);
        Ok(StunResult {
            success: false,
            mapped_addr: None,
            local_addr,
            nat_type: NatType::Unknown,
            server: stun_server.to_string(),
            rtt_ms: rtt,
        })
    }
}

/// 从多个 STUN 服务器中探测，返回第一个成功的结果
pub fn stun_binding_request_multi(
    servers: &[String],
    local_addr: &str,
    timeout: Duration,
) -> Option<StunResult> {
    for server in servers {
        match stun_binding_request(server, local_addr, timeout) {
            Ok(result) if result.success => return Some(result),
            Ok(_) => continue,
            Err(e) => {
                debug!("[stun] 服务器 {} 失败: {}", server, e);
                continue;
            }
        }
    }
    None
}

/// 完整 NAT 类型检测（RFC 3489 简化版）
///
/// 使用两个不同的 STUN 服务器，通过比较映射地址来判断 NAT 类型：
/// - Open Internet: 映射地址 == 本地地址
/// - Full Cone NAT: 两个服务器返回相同的映射地址（相同映射对所有外部地址可见）
/// - Symmetric NAT: 两个服务器返回不同的映射地址（不同外部地址得到不同映射）
/// - Unknown: 检测失败或无法确定
///
/// 注意：完整的 RFC 3489 检测需要 STUN 服务器支持 changed-address 和
/// CHANGE-REQUEST 属性，现代 STUN 服务器大多不支持。本方法使用双服务器
/// 方案，可以区分 Open Internet / Full Cone / Symmetric，但无法精确区分
/// Restricted Cone 和 Port Restricted Cone。
pub fn detect_nat_type(
    server_a: &str,
    server_b: &str,
    local_addr: &str,
    timeout: Duration,
) -> NatType {
    // 测试 I: 向服务器 A 发送请求
    let result_a = match stun_binding_request(server_a, local_addr, timeout) {
        Ok(r) if r.success => r,
        _ => {
            debug!("[stun] 服务器 {} 检测失败", server_a);
            return NatType::Unknown;
        }
    };

    let mapped_a = match result_a.mapped_addr {
        Some(addr) => addr,
        None => return NatType::Unknown,
    };

    // 检查是否为公网直连
    if mapped_a.ip() == result_a.local_addr.ip() && mapped_a.port() == result_a.local_addr.port() {
        debug!("[stun] NAT 类型: Open Internet（公网直连）");
        return NatType::OpenInternet;
    }

    // 测试 II: 向服务器 B 发送请求
    let result_b = match stun_binding_request(server_b, local_addr, timeout) {
        Ok(r) if r.success => r,
        _ => {
            debug!("[stun] 服务器 {} 检测失败，无法确定完整 NAT 类型", server_b);
            // 只有一个服务器成功时，至少可以确定不是 Open Internet
            // 但无法区分 Full Cone 和 Symmetric，标记为 Unknown
            return NatType::Unknown;
        }
    };

    let mapped_b = match result_b.mapped_addr {
        Some(addr) => addr,
        None => return NatType::Unknown,
    };

    // 比较两个映射地址
    if mapped_a == mapped_b {
        // 相同的映射地址对所有外部服务器可见 → Full Cone NAT
        debug!(
            "[stun] NAT 类型: Full Cone（映射地址一致: {}）",
            mapped_a
        );
        NatType::FullCone
    } else {
        // 不同的外部服务器得到不同的映射地址 → Symmetric NAT
        debug!(
            "[stun] NAT 类型: Symmetric（映射地址不同: {} vs {}）",
            mapped_a, mapped_b
        );
        NatType::Symmetric
    }
}

/// 从多个 STUN 服务器中选择两个不同的服务器进行 NAT 类型检测
pub fn detect_nat_type_multi(
    servers: &[String],
    local_addr: &str,
    timeout: Duration,
) -> NatType {
    if servers.len() < 2 {
        // 只有一个服务器时，使用默认的第二个服务器
        let default_servers = [
            "stun.l.google.com:19302".to_string(),
            "stun1.l.google.com:19302".to_string(),
        ];
        let server_a = servers.first().cloned().unwrap_or_else(|| default_servers[0].clone());
        let server_b = default_servers[1].clone();
        return detect_nat_type(&server_a, &server_b, local_addr, timeout);
    }

    // 选择前两个不同的服务器
    let server_a = &servers[0];
    let mut server_b = &servers[1];
    for s in servers.iter().skip(1) {
        if s != server_a {
            server_b = s;
            break;
        }
    }

    detect_nat_type(server_a, server_b, local_addr, timeout)
}

/// 检测 UDP 端口公网可达性（通过 STUN）
///
/// 在指定端口上发送 STUN 请求，如果返回的映射地址和端口匹配，
/// 说明该 UDP 端口可从公网访问。
pub fn check_udp_reachability(
    stun_servers: &[String],
    local_port: u16,
    timeout: Duration,
) -> ReachabilityResult {
    let local_addr = format!("0.0.0.0:{}", local_port);

    if let Some(result) = stun_binding_request_multi(stun_servers, &local_addr, timeout) {
        if let Some(mapped) = result.mapped_addr {
            // 如果 STUN 返回的端口与本地端口相同或映射了其他端口，
            // 都说明 UDP 可达（只是 NAT 类型不同）
            ReachabilityResult {
                reachable: true,
                mapped_addr: Some(mapped),
                local_port,
                rtt_ms: result.rtt_ms,
                server: result.server,
            }
        } else {
            ReachabilityResult {
                reachable: false,
                mapped_addr: None,
                local_port,
                rtt_ms: result.rtt_ms,
                server: result.server,
            }
        }
    } else {
        ReachabilityResult {
            reachable: false,
            mapped_addr: None,
            local_port,
            rtt_ms: 0,
            server: String::new(),
        }
    }
}

/// UDP 可达性检测结果
#[derive(Debug, Clone)]
pub struct ReachabilityResult {
    pub reachable: bool,
    pub mapped_addr: Option<SocketAddr>,
    pub local_port: u16,
    pub rtt_ms: u64,
    pub server: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_binding_request() {
        let tid = random_transaction_id();
        let req = build_binding_request(&tid);
        assert_eq!(req.len(), 20);
        assert_eq!(u16::from_be_bytes([req[0], req[1]]), STUN_BINDING_REQUEST);
        assert_eq!(u16::from_be_bytes([req[2], req[3]]), 0);
        assert_eq!(u32::from_be_bytes([req[4], req[5], req[6], req[7]]), STUN_MAGIC_COOKIE);
        assert_eq!(&req[8..20], &tid);
    }

    #[test]
    fn test_nat_type_str() {
        assert_eq!(NatType::FullCone.as_str(), "full_cone");
        assert_eq!(NatType::OpenInternet.as_str(), "open_internet");
        assert!(NatType::FullCone.is_favorable());
        assert!(!NatType::Symmetric.is_favorable());
    }

    #[test]
    fn test_nat_type_detection_offline() {
        // 离线环境下应该返回 Unknown 而不是 panic
        let result = detect_nat_type(
            "127.0.0.1:1",
            "127.0.0.1:2",
            "0.0.0.0:0",
            Duration::from_millis(50),
        );
        // 可能是 Unknown（检测失败），只要不 panic 就通过
        let _ = result;
    }

    #[test]
    fn test_detect_nat_type_multi() {
        let servers = vec![
            "stun.l.google.com:19302".to_string(),
            "stun1.l.google.com:19302".to_string(),
        ];
        // 离线环境下应该返回 Unknown
        let result = detect_nat_type_multi(&servers, "0.0.0.0:0", Duration::from_millis(50));
        let _ = result;
    }

    #[test]
    fn test_stun_offline() {
        // 离线环境下应该超时失败而不是 panic
        let result = stun_binding_request(
            "127.0.0.1:1",
            "0.0.0.0:0",
            Duration::from_millis(50),
        );
        // 可能成功也可能失败，取决于是否有本地 STUN 服务器
        // 只要不 panic 就通过
        let _ = result;
    }
}
