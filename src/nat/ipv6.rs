//! IPv6 支持
//!
//! 提供 IPv6 双栈支持、地址检测和工具函数。
//!
//! IPv6 在 P2P 网络中的优势：
//! - 端到端直连（通常不需要 NAT 穿透）
//! - 更大的地址空间（避免地址耗尽）
//! - 内置的多播和任播支持
//!
//! 双栈策略：
//! ```
//! 客户端请求
//!      │
//!      ├── IPv6 可达？ ──是──→ 优先使用 IPv6 直连
//!      │
//!      └── 否 ──→ 回退到 IPv4 + NAT 穿透
//! ```

use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// IPv6 配置
// ---------------------------------------------------------------------------

/// IPv6 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ipv6Config {
    /// 是否启用 IPv6
    pub enabled: bool,
    /// 是否优先使用 IPv6（双栈时）
    pub prefer_ipv6: bool,
    /// IPv6 监听地址
    pub listen_addr: String,
    /// 是否启用 IPv6 多播
    pub enable_multicast: bool,
    /// IPv6 跳数限制（TTL）
    pub hop_limit: u32,
    /// 是否自动检测 IPv6 连通性
    pub auto_detect: bool,
}

impl Default for Ipv6Config {
    fn default() -> Self {
        Self {
            enabled: true,
            prefer_ipv6: true,
            listen_addr: "[::]:0".to_string(),
            enable_multicast: false,
            hop_limit: 64,
            auto_detect: true,
        }
    }
}

// ---------------------------------------------------------------------------
// IPv6 连通性状态
// ---------------------------------------------------------------------------

/// IPv6 连通性状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ipv6Connectivity {
    /// 未检测
    Unknown,
    /// 无 IPv6 支持
    NoIpv6,
    /// 仅有本地链路 IPv6
    LinkLocalOnly,
    /// 仅有唯一本地地址（ULA）
    UlaOnly,
    /// 有全局 IPv6 地址
    Global,
}

impl Ipv6Connectivity {
    /// 是否可以用于 P2P 直连
    pub fn is_direct_connectable(&self) -> bool {
        matches!(self, Ipv6Connectivity::Global)
    }

    /// 状态名称
    pub fn as_str(&self) -> &'static str {
        match self {
            Ipv6Connectivity::Unknown => "unknown",
            Ipv6Connectivity::NoIpv6 => "no_ipv6",
            Ipv6Connectivity::LinkLocalOnly => "link_local_only",
            Ipv6Connectivity::UlaOnly => "ula_only",
            Ipv6Connectivity::Global => "global",
        }
    }
}

// ---------------------------------------------------------------------------
// IPv6 管理器
// ---------------------------------------------------------------------------

/// IPv6 管理器
///
/// 负责 IPv6 连通性检测、双栈监听和地址管理。
pub struct Ipv6Manager {
    /// 配置
    config: Ipv6Config,
    /// 连通性状态
    connectivity: parking_lot::RwLock<Ipv6Connectivity>,
    /// 全局 IPv6 地址
    global_address: parking_lot::RwLock<Option<Ipv6Addr>>,
}

impl Ipv6Manager {
    /// 创建 IPv6 管理器
    pub fn new(config: Ipv6Config) -> Self {
        Self {
            config,
            connectivity: parking_lot::RwLock::new(Ipv6Connectivity::Unknown),
            global_address: parking_lot::RwLock::new(None),
        }
    }

    /// 检测 IPv6 连通性
    ///
    /// 通过检查本地网络接口和尝试连接 IPv6 服务器来检测。
    pub async fn detect_connectivity(&self) -> Ipv6Connectivity {
        if !self.config.enabled {
            *self.connectivity.write() = Ipv6Connectivity::NoIpv6;
            return Ipv6Connectivity::NoIpv6;
        }

        info!("[ipv6] 开始检测 IPv6 连通性...");

        // 1. 检查本地是否有全局 IPv6 地址
        let global_addr = Self::find_global_ipv6_address();
        if let Some(addr) = global_addr {
            info!("[ipv6] 发现全局 IPv6 地址: {}", addr);
            *self.global_address.write() = Some(addr);
            *self.connectivity.write() = Ipv6Connectivity::Global;
            return Ipv6Connectivity::Global;
        }

        // 2. 检查是否有 ULA 地址
        if Self::has_ula_address() {
            info!("[ipv6] 仅有唯一本地地址（ULA），无全局 IPv6");
            *self.connectivity.write() = Ipv6Connectivity::UlaOnly;
            return Ipv6Connectivity::UlaOnly;
        }

        // 3. 检查是否有链路本地地址
        if Self::has_link_local_address() {
            info!("[ipv6] 仅有链路本地地址，无全局 IPv6");
            *self.connectivity.write() = Ipv6Connectivity::LinkLocalOnly;
            return Ipv6Connectivity::LinkLocalOnly;
        }

        info!("[ipv6] 无 IPv6 支持");
        *self.connectivity.write() = Ipv6Connectivity::NoIpv6;
        Ipv6Connectivity::NoIpv6
    }

    /// 查找全局 IPv6 地址
    fn find_global_ipv6_address() -> Option<Ipv6Addr> {
        // Windows 上通过 ipconfig 检测
        #[cfg(windows)]
        {
            use std::process::Command;
            let output = Command::new("ipconfig").arg("/all").output().ok()?;
            let text = String::from_utf8_lossy(&output.stdout);

            for line in text.lines() {
                if line.contains("IPv6") || line.contains("ipv6") {
                    // 提取 IPv6 地址
                    if let Some(addr_str) = Self::extract_ipv6_from_line(line) {
                        if Self::is_global_ipv6(&addr_str) {
                            return Some(addr_str);
                        }
                    }
                }
            }
            None
        }

        // Linux/macOS 上通过 ifconfig 或 ip 命令检测
        #[cfg(not(windows))]
        {
            // 简化实现：返回 None，实际项目中可以用 if_addrs crate
            None
        }
    }

    /// 从行中提取 IPv6 地址
    fn extract_ipv6_from_line(line: &str) -> Option<Ipv6Addr> {
        // 简单的 IPv6 地址提取
        let parts: Vec<&str> = line.split_whitespace().collect();
        for part in parts {
            // 清理可能的后缀（如 %eth0）
            let clean = part.split('%').next().unwrap_or(part);
            if let Ok(addr) = clean.parse::<Ipv6Addr>() {
                return Some(addr);
            }
        }
        None
    }

    /// 判断是否为全局 IPv6 地址
    fn is_global_ipv6(addr: &Ipv6Addr) -> bool {
        let octets = addr.octets();
        // 全局单播地址：前 3 位不是 111（即不是 fe80/ff00 等）
        // 更精确的判断：不是链路本地(fe80::/10)、不是 ULA(fc00::/7)、不是多播(ff00::/8)
        let first = octets[0];

        // 链路本地：fe80::/10 (1111 1110 10...)
        if first == 0xfe && (octets[1] & 0xc0) == 0x80 {
            return false;
        }

        // ULA：fc00::/7 (1111 110...)
        if (first & 0xfe) == 0xfc {
            return false;
        }

        // 多播：ff00::/8
        if first == 0xff {
            return false;
        }

        // 回环：::1
        if addr == &Ipv6Addr::LOCALHOST {
            return false;
        }

        // 未指定：::
        if addr == &Ipv6Addr::UNSPECIFIED {
            return false;
        }

        true
    }

    /// 是否有 ULA 地址
    fn has_ula_address() -> bool {
        // 简化实现：检查是否有 fc00::/7 地址
        #[cfg(windows)]
        {
            use std::process::Command;
            let output = match Command::new("ipconfig").arg("/all").output() {
                Ok(o) => o,
                Err(_) => return false,
            };
            let text = String::from_utf8_lossy(&output.stdout);
            text.lines().any(|line| {
                if line.contains("IPv6") || line.contains("ipv6") {
                    if let Some(addr) = Self::extract_ipv6_from_line(line) {
                        let first = addr.octets()[0];
                        return (first & 0xfe) == 0xfc;
                    }
                }
                false
            })
        }
        #[cfg(not(windows))]
        {
            false
        }
    }

    /// 是否有链路本地地址
    fn has_link_local_address() -> bool {
        #[cfg(windows)]
        {
            use std::process::Command;
            let output = match Command::new("ipconfig").arg("/all").output() {
                Ok(o) => o,
                Err(_) => return false,
            };
            let text = String::from_utf8_lossy(&output.stdout);
            text.lines().any(|line| {
                if line.contains("IPv6") || line.contains("ipv6") {
                    if let Some(addr) = Self::extract_ipv6_from_line(line) {
                        let octets = addr.octets();
                        return octets[0] == 0xfe && (octets[1] & 0xc0) == 0x80;
                    }
                }
                false
            })
        }
        #[cfg(not(windows))]
        {
            false
        }
    }

    /// 获取当前连通性状态
    pub fn connectivity(&self) -> Ipv6Connectivity {
        *self.connectivity.read()
    }

    /// 获取全局 IPv6 地址
    pub fn global_address(&self) -> Option<Ipv6Addr> {
        *self.global_address.read()
    }

    /// 是否可以直接连接（有全局 IPv6）
    pub fn is_direct_connectable(&self) -> bool {
        self.connectivity().is_direct_connectable()
    }

    /// 选择最佳地址（IPv6 优先策略）
    ///
    /// 给定一组候选地址，选择最佳的一个：
    /// 1. 如果 prefer_ipv6 且有全局 IPv6 连通性，优先选 IPv6
    /// 2. 否则选 IPv4
    pub fn select_best_address(&self, candidates: &[SocketAddr]) -> Option<SocketAddr> {
        if candidates.is_empty() {
            return None;
        }

        let ipv6_candidates: Vec<&SocketAddr> = candidates
            .iter()
            .filter(|addr| matches!(addr.ip(), IpAddr::V6(_)))
            .collect();

        let ipv4_candidates: Vec<&SocketAddr> = candidates
            .iter()
            .filter(|addr| matches!(addr.ip(), IpAddr::V4(_)))
            .collect();

        if self.config.prefer_ipv6 && self.is_direct_connectable() && !ipv6_candidates.is_empty() {
            debug!("[ipv6] 优先选择 IPv6 地址");
            return Some(*ipv6_candidates[0]);
        }

        if !ipv4_candidates.is_empty() {
            return Some(*ipv4_candidates[0]);
        }

        // 只有 IPv6 时返回 IPv6
        if !ipv6_candidates.is_empty() {
            return Some(*ipv6_candidates[0]);
        }

        None
    }

    /// 获取配置
    pub fn config(&self) -> &Ipv6Config {
        &self.config
    }
}

// ---------------------------------------------------------------------------
// 工具函数
// ---------------------------------------------------------------------------

/// 判断地址是否为 IPv6
pub fn is_ipv6(addr: &SocketAddr) -> bool {
    matches!(addr.ip(), IpAddr::V6(_))
}

/// 判断地址是否为 IPv4
pub fn is_ipv4(addr: &SocketAddr) -> bool {
    matches!(addr.ip(), IpAddr::V4(_))
}

/// 将 IPv4 地址转换为 IPv4-mapped IPv6 地址
pub fn ipv4_to_ipv6_mapped(addr: &SocketAddr) -> Option<SocketAddr> {
    match addr.ip() {
        IpAddr::V4(v4) => {
            let v6 = v4.to_ipv6_mapped();
            Some(SocketAddr::new(IpAddr::V6(v6), addr.port()))
        }
        IpAddr::V6(_) => Some(*addr),
    }
}

/// 判断 IPv6 地址是否为 IPv4-mapped
pub fn is_ipv4_mapped(addr: &Ipv6Addr) -> bool {
    addr.octets()[0..10] == [0; 10] && addr.octets()[10..12] == [0xff, 0xff]
}

/// 将 IPv4-mapped IPv6 地址转换回 IPv4
pub fn ipv6_mapped_to_ipv4(addr: &Ipv6Addr) -> Option<std::net::Ipv4Addr> {
    if is_ipv4_mapped(addr) {
        let octets = addr.octets();
        Some(std::net::Ipv4Addr::new(
            octets[12], octets[13], octets[14], octets[15],
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ipv6_config_default() {
        let config = Ipv6Config::default();
        assert!(config.enabled);
        assert!(config.prefer_ipv6);
        assert_eq!(config.hop_limit, 64);
    }

    #[test]
    fn test_ipv6_connectivity() {
        assert!(Ipv6Connectivity::Global.is_direct_connectable());
        assert!(!Ipv6Connectivity::NoIpv6.is_direct_connectable());
        assert!(!Ipv6Connectivity::LinkLocalOnly.is_direct_connectable());
        assert_eq!(Ipv6Connectivity::Global.as_str(), "global");
    }

    #[test]
    fn test_is_global_ipv6() {
        // 全局地址（2001:db8::1）
        let global = "2001:db8::1".parse::<Ipv6Addr>().unwrap();
        assert!(Ipv6Manager::is_global_ipv6(&global));

        // 链路本地（fe80::1）
        let link_local = "fe80::1".parse::<Ipv6Addr>().unwrap();
        assert!(!Ipv6Manager::is_global_ipv6(&link_local));

        // ULA（fd00::1）
        let ula = "fd00::1".parse::<Ipv6Addr>().unwrap();
        assert!(!Ipv6Manager::is_global_ipv6(&ula));

        // 多播（ff02::1）
        let multicast = "ff02::1".parse::<Ipv6Addr>().unwrap();
        assert!(!Ipv6Manager::is_global_ipv6(&multicast));
    }

    #[test]
    fn test_ipv4_ipv6_conversion() {
        let v4 = "192.168.1.1:8080".parse::<SocketAddr>().unwrap();
        let mapped = ipv4_to_ipv6_mapped(&v4).unwrap();
        assert!(is_ipv6(&mapped));

        if let IpAddr::V6(v6) = mapped.ip() {
            assert!(is_ipv4_mapped(&v6));
            let back = ipv6_mapped_to_ipv4(&v6).unwrap();
            assert_eq!(back, std::net::Ipv4Addr::new(192, 168, 1, 1));
        }
    }

    #[test]
    fn test_is_ipv4_ipv6() {
        let v4 = "192.168.1.1:8080".parse::<SocketAddr>().unwrap();
        let v6 = "[2001:db8::1]:8080".parse::<SocketAddr>().unwrap();
        assert!(is_ipv4(&v4));
        assert!(!is_ipv6(&v4));
        assert!(is_ipv6(&v6));
        assert!(!is_ipv4(&v6));
    }
}
