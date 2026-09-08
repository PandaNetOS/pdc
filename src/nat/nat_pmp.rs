//! NAT-PMP 客户端（RFC 6886）
//!
//! NAT Port Mapping Protocol，用于在支持 NAT-PMP 的网关上配置端口映射。
//! 相比 UPnP，NAT-PMP 更轻量、更安全、响应更快。
//!
//! 协议要点：
//! - 网关地址：默认网关（无需 SSDP 发现）
//! - 端口：UDP 5351
//! - 版本：0
//! - 操作码：0=外部地址, 1=映射UDP, 2=映射TCP
//!
//! 参考：https://datatracker.ietf.org/doc/html/rfc6886

use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::time::Duration;

use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// 协议常量
// ---------------------------------------------------------------------------

const NAT_PMP_VERSION: u8 = 0;
const NAT_PMP_PORT: u16 = 5351;
const OP_EXTERNAL_ADDRESS: u8 = 0;
const OP_MAP_UDP: u8 = 1;
const OP_MAP_TCP: u8 = 2;

/// 结果码
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NatPmpResultCode {
    /// 成功
    Success,
    /// 不支持的版本
    UnsupportedVersion,
    /// 未授权（NAT-PMP 被禁用）
    NotAuthorized,
    /// 网络故障
    NetworkFailure,
    /// 资源不足（无可用端口）
    OutOfResources,
    /// 不支持的操作码
    UnsupportedOpcode,
    /// 其他未知错误
    Other(u16),
}

impl NatPmpResultCode {
    pub fn from_u16(code: u16) -> Self {
        match code {
            0 => NatPmpResultCode::Success,
            1 => NatPmpResultCode::UnsupportedVersion,
            2 => NatPmpResultCode::NotAuthorized,
            3 => NatPmpResultCode::NetworkFailure,
            4 => NatPmpResultCode::OutOfResources,
            5 => NatPmpResultCode::UnsupportedOpcode,
            other => NatPmpResultCode::Other(other),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            NatPmpResultCode::Success => "success",
            NatPmpResultCode::UnsupportedVersion => "unsupported_version",
            NatPmpResultCode::NotAuthorized => "not_authorized",
            NatPmpResultCode::NetworkFailure => "network_failure",
            NatPmpResultCode::OutOfResources => "out_of_resources",
            NatPmpResultCode::UnsupportedOpcode => "unsupported_opcode",
            NatPmpResultCode::Other(_) => "other",
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self, NatPmpResultCode::Success)
    }
}

// ---------------------------------------------------------------------------
// 响应结构
// ---------------------------------------------------------------------------

/// 外部地址响应
#[derive(Debug, Clone)]
pub struct ExternalAddressResponse {
    pub result_code: NatPmpResultCode,
    pub epoch_time: u32,
    pub public_ip: Ipv4Addr,
}

/// 端口映射响应
#[derive(Debug, Clone)]
pub struct MapResponse {
    pub result_code: NatPmpResultCode,
    pub epoch_time: u32,
    pub internal_port: u16,
    pub mapped_external_port: u16,
    pub mapping_lifetime: u32,
}

// ---------------------------------------------------------------------------
// NAT-PMP 客户端
// ---------------------------------------------------------------------------

/// NAT-PMP 客户端
#[derive(Clone)]
pub struct NatPmpClient {
    pub gateway: Ipv4Addr,
    timeout: Duration,
}

impl NatPmpClient {
    /// 创建 NAT-PMP 客户端
    ///
    /// # 参数
    /// - `gateway`: 网关地址（通常是默认网关，如 192.168.1.1）
    /// - `timeout`: 请求超时
    pub fn new(gateway: Ipv4Addr, timeout: Duration) -> Self {
        Self { gateway, timeout }
    }

    /// 检测网关是否支持 NAT-PMP
    ///
    /// 通过发送外部地址请求来检测，如果网关响应则说明支持 NAT-PMP。
    pub fn probe(&self) -> bool {
        match self.get_external_address() {
            Ok(resp) => resp.result_code.is_success(),
            Err(_) => false,
        }
    }

    /// 获取公网 IP 地址
    pub fn get_external_address(&self) -> anyhow::Result<ExternalAddressResponse> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.set_read_timeout(Some(self.timeout))?;

        // 构建请求：version(1) + opcode(1) = 2 字节
        let request = [NAT_PMP_VERSION, OP_EXTERNAL_ADDRESS];
        let server_addr = SocketAddr::new(std::net::IpAddr::V4(self.gateway), NAT_PMP_PORT);

        socket.send_to(&request, server_addr)?;

        let mut buf = [0u8; 128];
        let (len, _from) = socket.recv_from(&mut buf)?;

        if len < 12 {
            return Err(anyhow::anyhow!("NAT-PMP 响应过短: {} 字节", len));
        }

        // 解析响应
        let version = buf[0];
        let opcode = buf[1];
        let result_code = u16::from_be_bytes([buf[2], buf[3]]);
        let epoch_time = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let public_ip = Ipv4Addr::new(buf[8], buf[9], buf[10], buf[11]);

        debug!(
            "[nat-pmp] 外部地址响应: version={}, opcode=0x{:02x}, result={}, epoch={}, ip={}",
            version, opcode, result_code, epoch_time, public_ip
        );

        // 验证 opcode 高位置 1（响应标志）
        if opcode != 0x80 | OP_EXTERNAL_ADDRESS {
            return Err(anyhow::anyhow!("NAT-PMP 响应 opcode 不匹配: 0x{:02x}", opcode));
        }

        Ok(ExternalAddressResponse {
            result_code: NatPmpResultCode::from_u16(result_code),
            epoch_time,
            public_ip,
        })
    }

    /// 请求端口映射
    ///
    /// # 参数
    /// - `protocol`: 协议类型（OP_MAP_UDP 或 OP_MAP_TCP）
    /// - `internal_port`: 内部端口
    /// - `requested_external_port`: 请求的外部端口（0=让网关分配）
    /// - `lifetime`: 映射生命周期（秒，0=删除映射，建议 3600）
    pub fn map_port(
        &self,
        protocol: u8,
        internal_port: u16,
        requested_external_port: u16,
        lifetime: u32,
    ) -> anyhow::Result<MapResponse> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.set_read_timeout(Some(self.timeout))?;

        // 构建请求：12 字节
        // version(1) + opcode(1) + reserved(2) + internal_port(2) + requested_external_port(2) + lifetime(4)
        let mut request = [0u8; 12];
        request[0] = NAT_PMP_VERSION;
        request[1] = protocol;
        // request[2..4] = reserved (0)
        request[4..6].copy_from_slice(&internal_port.to_be_bytes());
        request[6..8].copy_from_slice(&requested_external_port.to_be_bytes());
        request[8..12].copy_from_slice(&lifetime.to_be_bytes());

        let server_addr = SocketAddr::new(std::net::IpAddr::V4(self.gateway), NAT_PMP_PORT);
        socket.send_to(&request, server_addr)?;

        let mut buf = [0u8; 128];
        let (len, _from) = socket.recv_from(&mut buf)?;

        if len < 16 {
            return Err(anyhow::anyhow!("NAT-PMP 映射响应过短: {} 字节", len));
        }

        // 解析响应：16 字节
        let version = buf[0];
        let opcode = buf[1];
        let result_code = u16::from_be_bytes([buf[2], buf[3]]);
        let epoch_time = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let internal_port_resp = u16::from_be_bytes([buf[8], buf[9]]);
        let mapped_external_port = u16::from_be_bytes([buf[10], buf[11]]);
        let mapping_lifetime = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);

        let proto_name = if protocol == OP_MAP_UDP { "UDP" } else { "TCP" };
        debug!(
            "[nat-pmp] {} 映射响应: version={}, opcode=0x{:02x}, result={}, internal={}, external={}, lifetime={}",
            proto_name, version, opcode, result_code, internal_port_resp, mapped_external_port, mapping_lifetime
        );

        // 验证 opcode
        let expected_opcode = 0x80 | protocol;
        if opcode != expected_opcode {
            return Err(anyhow::anyhow!(
                "NAT-PMP 映射响应 opcode 不匹配: 期望 0x{:02x}, 实际 0x{:02x}",
                expected_opcode,
                opcode
            ));
        }

        Ok(MapResponse {
            result_code: NatPmpResultCode::from_u16(result_code),
            epoch_time,
            internal_port: internal_port_resp,
            mapped_external_port,
            mapping_lifetime,
        })
    }

    /// 映射 UDP 端口（便捷方法）
    pub fn map_udp(
        &self,
        internal_port: u16,
        requested_external_port: u16,
        lifetime: u32,
    ) -> anyhow::Result<MapResponse> {
        self.map_port(OP_MAP_UDP, internal_port, requested_external_port, lifetime)
    }

    /// 映射 TCP 端口（便捷方法）
    pub fn map_tcp(
        &self,
        internal_port: u16,
        requested_external_port: u16,
        lifetime: u32,
    ) -> anyhow::Result<MapResponse> {
        self.map_port(OP_MAP_TCP, internal_port, requested_external_port, lifetime)
    }

    /// 删除端口映射（lifetime=0）
    pub fn unmap_port(&self, protocol: u8, internal_port: u16, external_port: u16) -> anyhow::Result<()> {
        let resp = self.map_port(protocol, internal_port, external_port, 0)?;
        if resp.result_code.is_success() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "删除映射失败: result_code={}",
                resp.result_code.as_str()
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// 默认网关检测
// ---------------------------------------------------------------------------

/// 检测默认网关地址
///
/// 平台实现：
/// - Windows: 通过 `netsh interface ipv4 show route` 解析 0.0.0.0/0 的网关
/// - Linux/macOS: 读取路由表
///
/// 如果检测失败，返回 None。
pub fn detect_default_gateway() -> Option<Ipv4Addr> {
    #[cfg(windows)]
    {
        detect_default_gateway_windows()
    }
    #[cfg(not(windows))]
    {
        detect_default_gateway_unix()
    }
}

#[cfg(windows)]
fn detect_default_gateway_windows() -> Option<Ipv4Addr> {
    // 方法1: 通过 netsh 解析默认路由
    let output = std::process::Command::new("netsh")
        .args(&["interface", "ipv4", "show", "route"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    // 查找 0.0.0.0/0 的行，格式类似：
    // 0.0.0.0/0  0  192.168.1.1  以太网  0  0
    for line in stdout.lines() {
        let line = line.trim();
        // netsh 输出格式：No  Manual  0  0.0.0.0/0  15  192.168.31.1
        // 行开头是 Publish 列（No/Yes），所以用 contains 而不是 starts_with
        if line.contains("0.0.0.0/0") || line.contains("0.0.0.0 ") {
            // 提取第一个看起来像 IP 的字段（排除 0.0.0.0 本身）
            for part in line.split_whitespace() {
                if let Ok(ip) = part.parse::<Ipv4Addr>() {
                    if !ip.is_unspecified() && !ip.is_loopback() {
                        debug!("[nat-pmp] 检测到默认网关(netsh): {}", ip);
                        return Some(ip);
                    }
                }
            }
        }
    }

    // 方法2: 通过 ipconfig 解析
    let output = std::process::Command::new("ipconfig")
        .output()
        .ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout);

    let mut current_adapter_has_dhcp = false;
    for line in stdout.lines() {
        let line = line.trim();
        if line.contains("默认网关") || line.contains("Default Gateway") {
            if let Some(ip_str) = line.split(':').nth(1) {
                let ip_str = ip_str.trim();
                if let Ok(ip) = ip_str.parse::<Ipv4Addr>() {
                    if !ip.is_unspecified() {
                        debug!("[nat-pmp] 检测到默认网关(ipconfig): {}", ip);
                        return Some(ip);
                    }
                }
            }
        }
        let _ = current_adapter_has_dhcp;
    }

    warn!("[nat-pmp] 无法检测默认网关");
    None
}

#[cfg(not(windows))]
fn detect_default_gateway_unix() -> Option<Ipv4Addr> {
    // Linux: 读取 /proc/net/route
    // 格式：Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT
    if let Ok(content) = std::fs::read_to_string("/proc/net/route") {
        for line in content.lines().skip(1) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 {
                let destination = u32::from_str_radix(parts[1], 16).unwrap_or(1);
                if destination == 0 {
                    // 默认路由
                    let gateway_hex = parts[2];
                    if let Ok(gateway_raw) = u32::from_str_radix(gateway_hex, 16) {
                        // /proc/net/route 中的 IP 是小端序
                        let ip = Ipv4Addr::from(gateway_raw.to_be());
                        if !ip.is_unspecified() {
                            debug!("[nat-pmp] 检测到默认网关(/proc/net/route): {}", ip);
                            return Some(ip);
                        }
                    }
                }
            }
        }
    }

    // macOS: route -n get default
    let output = std::process::Command::new("route")
        .args(&["-n", "get", "default"])
        .output()
        .ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if line.contains("gateway:") {
            if let Some(ip_str) = line.split(':').nth(1) {
                let ip_str = ip_str.trim();
                if let Ok(ip) = ip_str.parse::<Ipv4Addr>() {
                    debug!("[nat-pmp] 检测到默认网关(route): {}", ip);
                    return Some(ip);
                }
            }
        }
    }

    warn!("[nat-pmp] 无法检测默认网关");
    None
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_result_code() {
        assert!(NatPmpResultCode::from_u16(0).is_success());
        assert_eq!(NatPmpResultCode::from_u16(2).as_str(), "not_authorized");
        assert_eq!(NatPmpResultCode::from_u16(4).as_str(), "out_of_resources");
        assert!(!NatPmpResultCode::from_u16(5).is_success());
    }

    #[test]
    fn test_detect_gateway_doesnt_panic() {
        // 不断言结果，确保不 panic
        let _ = detect_default_gateway();
    }

    #[test]
    fn test_nat_pmp_client_creation() {
        let gateway = Ipv4Addr::new(192, 168, 1, 1);
        let client = NatPmpClient::new(gateway, Duration::from_secs(2));
        assert_eq!(client.gateway, gateway);
    }

    #[test]
    fn test_protocol_constants() {
        assert_eq!(NAT_PMP_VERSION, 0);
        assert_eq!(NAT_PMP_PORT, 5351);
        assert_eq!(OP_EXTERNAL_ADDRESS, 0);
        assert_eq!(OP_MAP_UDP, 1);
        assert_eq!(OP_MAP_TCP, 2);
    }
}
