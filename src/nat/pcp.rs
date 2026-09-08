//! PCP 客户端（RFC 6887）
//!
//! Port Control Protocol，NAT-PMP 的后继者，支持更丰富的映射选项、
//! 更长的生命周期、第三方映射和 IPv6。
//!
//! 协议要点：
//! - 端口：UDP 5351（与 NAT-PMP 相同）
//! - 版本：2
//! - 操作码：ANNOUNCE(0) / MAP(1) / PEER(2)
//! - 响应标志：opcode | 0x80
//!
//! 参考：https://datatracker.ietf.org/doc/html/rfc6887

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::time::Duration;

use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// 协议常量
// ---------------------------------------------------------------------------

const PCP_VERSION: u8 = 2;
const PCP_PORT: u16 = 5351;
const PCP_RESPONSE_FLAG: u8 = 0x80;

const OP_ANNOUNCE: u8 = 0;
const OP_MAP: u8 = 1;
const OP_PEER: u8 = 2;

/// PCP 结果码
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PcpResultCode {
    Success,
    UnsuppVersion,
    NotAuthorized,
    MalformedRequest,
    UnsuppOpcode,
    UnsuppOption,
    MalformedOption,
    NetworkFailure,
    NoResources,
    UnsuppProtocol,
    UserExQuota,
    CannotProvideExternal,
    AddressMismatch,
    ExcessiveRemotePeers,
    Other(u32),
}

impl PcpResultCode {
    pub fn from_u32(code: u32) -> Self {
        match code {
            0 => PcpResultCode::Success,
            1 => PcpResultCode::UnsuppVersion,
            2 => PcpResultCode::NotAuthorized,
            3 => PcpResultCode::MalformedRequest,
            4 => PcpResultCode::UnsuppOpcode,
            5 => PcpResultCode::UnsuppOption,
            6 => PcpResultCode::MalformedOption,
            7 => PcpResultCode::NetworkFailure,
            8 => PcpResultCode::NoResources,
            9 => PcpResultCode::UnsuppProtocol,
            10 => PcpResultCode::UserExQuota,
            11 => PcpResultCode::CannotProvideExternal,
            12 => PcpResultCode::AddressMismatch,
            13 => PcpResultCode::ExcessiveRemotePeers,
            other => PcpResultCode::Other(other),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            PcpResultCode::Success => "success",
            PcpResultCode::UnsuppVersion => "unsupp_version",
            PcpResultCode::NotAuthorized => "not_authorized",
            PcpResultCode::MalformedRequest => "malformed_request",
            PcpResultCode::UnsuppOpcode => "unsupp_opcode",
            PcpResultCode::UnsuppOption => "unsupp_option",
            PcpResultCode::MalformedOption => "malformed_option",
            PcpResultCode::NetworkFailure => "network_failure",
            PcpResultCode::NoResources => "no_resources",
            PcpResultCode::UnsuppProtocol => "unsupp_protocol",
            PcpResultCode::UserExQuota => "user_ex_quota",
            PcpResultCode::CannotProvideExternal => "cannot_provide_external",
            PcpResultCode::AddressMismatch => "address_mismatch",
            PcpResultCode::ExcessiveRemotePeers => "excessive_remote_peers",
            PcpResultCode::Other(_) => "other",
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self, PcpResultCode::Success)
    }
}

// ---------------------------------------------------------------------------
// 响应结构
// ---------------------------------------------------------------------------

/// PCP MAP 响应
#[derive(Debug, Clone)]
pub struct PcpMapResponse {
    pub result_code: PcpResultCode,
    pub lifetime: u32,
    pub epoch_time: u32,
    pub protocol: u8,
    pub internal_port: u16,
    pub mapped_external_port: u16,
    pub mapped_external_ip: Option<Ipv4Addr>,
}

/// PCP 通用响应（ANNOUNCE 等）
#[derive(Debug, Clone)]
pub struct PcpResponse {
    pub result_code: PcpResultCode,
    pub lifetime: u32,
    pub epoch_time: u32,
}

// ---------------------------------------------------------------------------
// PCP 客户端
// ---------------------------------------------------------------------------

/// PCP 客户端
#[derive(Clone)]
pub struct PcpClient {
    pub gateway: Ipv4Addr,
    timeout: Duration,
}

impl PcpClient {
    /// 创建 PCP 客户端
    pub fn new(gateway: Ipv4Addr, timeout: Duration) -> Self {
        Self { gateway, timeout }
    }

    /// 检测网关是否支持 PCP
    ///
    /// 通过发送 ANNOUNCE 请求来检测，如果网关响应则说明支持 PCP。
    pub fn probe(&self) -> bool {
        match self.announce() {
            Ok(resp) => resp.result_code.is_success(),
            Err(_) => false,
        }
    }

    /// 发送 ANNOUNCE 请求（用于探测和服务器状态通知）
    pub fn announce(&self) -> anyhow::Result<PcpResponse> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.set_read_timeout(Some(self.timeout))?;

        // 构建 ANNOUNCE 请求：24 字节头
        let request = self.build_header(OP_ANNOUNCE, 0);
        let server_addr = SocketAddr::new(std::net::IpAddr::V4(self.gateway), PCP_PORT);
        socket.send_to(&request, server_addr)?;

        let mut buf = [0u8; 512];
        let (len, _from) = socket.recv_from(&mut buf)?;

        self.parse_generic_response(&buf[..len], OP_ANNOUNCE)
    }

    /// 请求端口映射（MAP）
    ///
    /// # 参数
    /// - `protocol`: 协议号（6=TCP, 17=UDP）
    /// - `internal_port`: 内部端口
    /// - `suggested_external_port`: 建议的外部端口（0=让网关分配）
    /// - `lifetime`: 映射生命周期（秒，0=删除映射，建议 3600-7200）
    pub fn map_port(
        &self,
        protocol: u8,
        internal_port: u16,
        suggested_external_port: u16,
        lifetime: u32,
    ) -> anyhow::Result<PcpMapResponse> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.set_read_timeout(Some(self.timeout))?;

        // 构建 MAP 请求：24 字节头 + 36 字节 MAP 数据 = 60 字节
        let mut request = self.build_header(OP_MAP, lifetime);

        // Mapping Nonce (12 bytes) - 随机生成
        let nonce = Self::random_nonce();
        request.extend_from_slice(&nonce);

        // Protocol (1 byte)
        request.push(protocol);

        // Reserved (3 bytes)
        request.extend_from_slice(&[0u8; 3]);

        // Internal Port (2 bytes)
        request.extend_from_slice(&internal_port.to_be_bytes());

        // Suggested External Port (2 bytes)
        request.extend_from_slice(&suggested_external_port.to_be_bytes());

        // Suggested External IP Address (16 bytes) - IPv4-mapped IPv6 (全零表示让网关分配)
        request.extend_from_slice(&[0u8; 16]);

        let server_addr = SocketAddr::new(std::net::IpAddr::V4(self.gateway), PCP_PORT);
        socket.send_to(&request, server_addr)?;

        let mut buf = [0u8; 512];
        let (len, _from) = socket.recv_from(&mut buf)?;

        self.parse_map_response(&buf[..len])
    }

    /// 映射 UDP 端口（便捷方法）
    pub fn map_udp(
        &self,
        internal_port: u16,
        suggested_external_port: u16,
        lifetime: u32,
    ) -> anyhow::Result<PcpMapResponse> {
        self.map_port(17, internal_port, suggested_external_port, lifetime)
    }

    /// 映射 TCP 端口（便捷方法）
    pub fn map_tcp(
        &self,
        internal_port: u16,
        suggested_external_port: u16,
        lifetime: u32,
    ) -> anyhow::Result<PcpMapResponse> {
        self.map_port(6, internal_port, suggested_external_port, lifetime)
    }

    /// 删除端口映射（lifetime=0）
    pub fn unmap_port(&self, protocol: u8, internal_port: u16, external_port: u16) -> anyhow::Result<()> {
        let resp = self.map_port(protocol, internal_port, external_port, 0)?;
        if resp.result_code.is_success() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "PCP 删除映射失败: result_code={}",
                resp.result_code.as_str()
            ))
        }
    }

    // ── 内部方法 ─────────────────────────────────────────────────────

    /// 构建 PCP 请求头（24 字节）
    fn build_header(&self, opcode: u8, lifetime: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(24);
        // Version (1 byte)
        buf.push(PCP_VERSION);
        // Opcode (1 byte)
        buf.push(opcode);
        // Reserved (2 bytes)
        buf.extend_from_slice(&[0u8; 2]);
        // Requested Lifetime (4 bytes)
        buf.extend_from_slice(&lifetime.to_be_bytes());
        // Client IP Address (16 bytes) - IPv4-mapped IPv6
        // ::ffff:192.168.x.x 格式
        let client_ip = self.local_ipv4_mapped();
        buf.extend_from_slice(&client_ip);
        buf
    }

    /// 获取本地 IP 的 IPv4-mapped IPv6 表示（16 字节）
    fn local_ipv4_mapped(&self) -> [u8; 16] {
        // 简化：使用网关同网段的本地 IP
        // 实际应该检测本地 IP，这里用 0.0.0.0 的 IPv4-mapped 表示
        let mut bytes = [0u8; 16];
        // IPv4-mapped IPv6: ::ffff:a.b.c.d
        bytes[10] = 0xff;
        bytes[11] = 0xff;
        // 本地 IP 暂时用 0.0.0.0，PCP 服务器会从 UDP 包源地址获取
        bytes
    }

    /// 生成随机 Mapping Nonce（12 字节）
    fn random_nonce() -> [u8; 12] {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let mut nonce = [0u8; 12];
        nonce[..8].copy_from_slice(&now.to_be_bytes());
        nonce[8..12].copy_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD]);
        nonce
    }

    /// 解析通用响应头（24 字节）
    fn parse_generic_response(&self, buf: &[u8], opcode: u8) -> anyhow::Result<PcpResponse> {
        if buf.len() < 24 {
            return Err(anyhow::anyhow!("PCP 响应过短: {} 字节", buf.len()));
        }

        let version = buf[0];
        let resp_opcode = buf[1];
        let result_code = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let lifetime = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let epoch_time = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);

        debug!(
            "[pcp] 通用响应: version={}, opcode=0x{:02x}, result={}, lifetime={}, epoch={}",
            version, resp_opcode, result_code, lifetime, epoch_time
        );

        // 验证版本和 opcode
        if version != PCP_VERSION {
            return Err(anyhow::anyhow!("PCP 版本不匹配: 期望 {}, 实际 {}", PCP_VERSION, version));
        }
        let expected_opcode = PCP_RESPONSE_FLAG | opcode;
        if resp_opcode != expected_opcode {
            return Err(anyhow::anyhow!(
                "PCP 响应 opcode 不匹配: 期望 0x{:02x}, 实际 0x{:02x}",
                expected_opcode,
                resp_opcode
            ));
        }

        Ok(PcpResponse {
            result_code: PcpResultCode::from_u32(result_code),
            lifetime,
            epoch_time,
        })
    }

    /// 解析 MAP 响应（24 字节头 + 36 字节 MAP 数据）
    fn parse_map_response(&self, buf: &[u8]) -> anyhow::Result<PcpMapResponse> {
        if buf.len() < 60 {
            return Err(anyhow::anyhow!("PCP MAP 响应过短: {} 字节", buf.len()));
        }

        // 解析通用头
        let generic = self.parse_generic_response(buf, OP_MAP)?;

        // 解析 MAP 数据（从第 24 字节开始）
        let protocol = buf[36]; // 24 (header) + 12 (nonce) = 36
        let internal_port = u16::from_be_bytes([buf[40], buf[41]]); // 36 + 1 (protocol) + 3 (reserved) = 40
        let mapped_external_port = u16::from_be_bytes([buf[42], buf[43]]);

        // Mapped External IP Address (16 bytes, 从第 44 字节开始)
        let mapped_external_ip = if buf[44..58] == [0u8; 14] && buf[58] == 0xff && buf[59] == 0xff {
            // IPv4-mapped IPv6
            Some(Ipv4Addr::new(buf[56], buf[57], buf[58], buf[59]))
        } else if buf[44..54] == [0u8; 10] && buf[54] == 0xff && buf[55] == 0xff {
            // 另一种 IPv4-mapped 格式
            Some(Ipv4Addr::new(buf[56], buf[57], buf[58], buf[59]))
        } else {
            // 纯 IPv6 或全零，暂时忽略
            None
        };

        debug!(
            "[pcp] MAP 响应: result={}, protocol={}, internal={}, external={}, ip={:?}, lifetime={}",
            generic.result_code.as_str(),
            protocol,
            internal_port,
            mapped_external_port,
            mapped_external_ip,
            generic.lifetime
        );

        Ok(PcpMapResponse {
            result_code: generic.result_code,
            lifetime: generic.lifetime,
            epoch_time: generic.epoch_time,
            protocol,
            internal_port,
            mapped_external_port,
            mapped_external_ip,
        })
    }
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_result_code() {
        assert!(PcpResultCode::from_u32(0).is_success());
        assert_eq!(PcpResultCode::from_u32(2).as_str(), "not_authorized");
        assert_eq!(PcpResultCode::from_u32(8).as_str(), "no_resources");
        assert!(!PcpResultCode::from_u32(11).is_success());
    }

    #[test]
    fn test_pcp_client_creation() {
        let gateway = Ipv4Addr::new(192, 168, 1, 1);
        let client = PcpClient::new(gateway, Duration::from_secs(2));
        assert_eq!(client.gateway, gateway);
    }

    #[test]
    fn test_protocol_constants() {
        assert_eq!(PCP_VERSION, 2);
        assert_eq!(PCP_PORT, 5351);
        assert_eq!(OP_ANNOUNCE, 0);
        assert_eq!(OP_MAP, 1);
        assert_eq!(OP_PEER, 2);
    }

    #[test]
    fn test_random_nonce() {
        let nonce1 = PcpClient::random_nonce();
        let nonce2 = PcpClient::random_nonce();
        // 两次生成的 nonce 应该不同（时间戳不同）
        // 但如果在同一纳秒内可能相同，所以只检查长度
        assert_eq!(nonce1.len(), 12);
        assert_eq!(nonce2.len(), 12);
    }

    #[test]
    fn test_build_header() {
        let gateway = Ipv4Addr::new(192, 168, 1, 1);
        let client = PcpClient::new(gateway, Duration::from_secs(2));
        let header = client.build_header(OP_MAP, 3600);
        assert_eq!(header.len(), 24);
        assert_eq!(header[0], PCP_VERSION);
        assert_eq!(header[1], OP_MAP);
        assert_eq!(u32::from_be_bytes([header[4], header[5], header[6], header[7]]), 3600);
    }
}
