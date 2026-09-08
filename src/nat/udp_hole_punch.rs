//! UDP 打洞（UDP Hole Punching）
//!
//! 在两个都在 NAT 后面的节点之间建立直接的 UDP 连接。
//!
//! 打洞原理：
//! ```
//! 节点 A (NAT后)                    节点 B (NAT后)
//!      │                                  │
//!      │──── 1. 获取公网映射地址 ────→ STUN
//!      │                                  │
//!      │──── 2. 交换公网地址 ───────→ 信令服务器
//!      │                                  │
//!      │──── 3. 同时向对端发送UDP包 ────→│
//!      │  (打洞，NAT记录出站连接)         │  (打洞，NAT记录出站连接)
//!      │                                  │
//!      │◄─── 4. 对端包通过NAT ───────────│
//!      │    (直接UDP连接建立)             │
//! ```
//!
//! NAT 类型与打洞成功率：
//! - Full Cone NAT: 100%（任意外部地址都能通过）
//! - Restricted Cone NAT: 高（需要对端先发送包）
//! - Port Restricted Cone NAT: 中（需要精确端口匹配）
//! - Symmetric NAT: 低（每次连接映射不同端口，需要预测端口）

use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket as TokioUdpSocket;
use tracing::{debug, info, warn};

use super::stun::{stun_binding_request, NatType};

// ---------------------------------------------------------------------------
// 打洞配置
// ---------------------------------------------------------------------------

/// UDP 打洞配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HolePunchConfig {
    /// 打洞超时时间（毫秒）
    pub punch_timeout_ms: u64,
    /// 最大重试次数
    pub max_retries: u32,
    /// 打洞包发送间隔（毫秒）
    pub punch_interval_ms: u64,
    /// 打洞包数量（每次打洞发送的包数）
    pub punch_packet_count: u32,
    /// STUN 服务器列表（用于获取公网映射地址）
    pub stun_servers: Vec<String>,
    /// 是否启用 Symmetric NAT 端口预测
    pub enable_port_prediction: bool,
    /// 端口预测范围（Symmetric NAT 时尝试的端口范围）
    pub port_prediction_range: u16,
}

impl Default for HolePunchConfig {
    fn default() -> Self {
        Self {
            punch_timeout_ms: 5000,
            max_retries: 3,
            punch_interval_ms: 100,
            punch_packet_count: 10,
            stun_servers: vec![
                "stun.l.google.com:19302".to_string(),
                "stun1.l.google.com:19302".to_string(),
            ],
            enable_port_prediction: true,
            port_prediction_range: 100,
        }
    }
}

// ---------------------------------------------------------------------------
// 打洞结果
// ---------------------------------------------------------------------------

/// 打洞结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HolePunchResult {
    /// 是否成功
    pub success: bool,
    /// 对端地址（打洞成功后确认的地址）
    pub peer_addr: Option<SocketAddr>,
    /// 本地映射地址
    pub local_mapped_addr: Option<SocketAddr>,
    /// 耗时（毫秒）
    pub duration_ms: u64,
    /// 重试次数
    pub retries: u32,
    /// NAT 类型
    pub nat_type: String,
    /// 错误信息（如果失败）
    pub error: Option<String>,
    /// 使用的策略
    pub strategy: String,
}

// ---------------------------------------------------------------------------
// 打洞统计
// ---------------------------------------------------------------------------

/// 打洞统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HolePunchStats {
    /// 总打洞次数
    pub total_attempts: u64,
    /// 成功次数
    pub success_count: u64,
    /// 失败次数
    pub failure_count: u64,
    /// 平均耗时（毫秒）
    pub avg_duration_ms: f64,
    /// 各 NAT 类型成功次数
    pub nat_type_success: HashMap<String, u64>,
    /// 各 NAT 类型失败次数
    pub nat_type_failure: HashMap<String, u64>,
}

impl HolePunchStats {
    /// 成功率（0-100）
    pub fn success_rate(&self) -> f64 {
        if self.total_attempts == 0 {
            0.0
        } else {
            self.success_count as f64 / self.total_attempts as f64 * 100.0
        }
    }

    /// 记录一次打洞结果
    pub fn record(&mut self, result: &HolePunchResult) {
        self.total_attempts += 1;
        if result.success {
            self.success_count += 1;
            *self.nat_type_success.entry(result.nat_type.clone()).or_insert(0) += 1;
        } else {
            self.failure_count += 1;
            *self.nat_type_failure.entry(result.nat_type.clone()).or_insert(0) += 1;
        }
        // 更新平均耗时（滑动平均）
        self.avg_duration_ms =
            (self.avg_duration_ms * (self.total_attempts - 1) as f64 + result.duration_ms as f64)
                / self.total_attempts as f64;
    }
}

// ---------------------------------------------------------------------------
// UDP 打洞器
// ---------------------------------------------------------------------------

/// UDP 打洞器
///
/// 负责在 NAT 后面的节点之间建立直接的 UDP 连接。
pub struct HolePuncher {
    /// 本地 UDP socket（用于打洞）
    socket: Arc<TokioUdpSocket>,
    /// 配置
    config: HolePunchConfig,
    /// 本地映射地址（通过 STUN 获取）
    mapped_addr: RwLock<Option<SocketAddr>>,
    /// NAT 类型
    nat_type: RwLock<NatType>,
    /// 统计
    stats: Arc<RwLock<HolePunchStats>>,
    /// 活跃的打洞会话
    active_sessions: Arc<RwLock<HashMap<String, HolePunchSession>>>,
}

/// 打洞会话
#[derive(Debug, Clone)]
struct HolePunchSession {
    /// 对端 ID
    peer_id: String,
    /// 对端地址
    peer_addr: SocketAddr,
    /// 开始时间
    start_time: Instant,
    /// 重试次数
    retries: u32,
    /// 是否成功
    success: bool,
}

impl HolePuncher {
    /// 创建打洞器（绑定到指定地址）
    pub async fn new(bind_addr: &str, config: HolePunchConfig) -> anyhow::Result<Self> {
        let socket = TokioUdpSocket::bind(bind_addr).await?;
        info!("[udp-hole-punch] 打洞器绑定到 {}", bind_addr);

        Ok(Self {
            socket: Arc::new(socket),
            config,
            mapped_addr: RwLock::new(None),
            nat_type: RwLock::new(NatType::Unknown),
            stats: Arc::new(RwLock::new(HolePunchStats::default())),
            active_sessions: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// 通过 STUN 获取本地公网映射地址
    pub async fn discover_mapped_address(&self) -> anyhow::Result<SocketAddr> {
        let local_addr = self.socket.local_addr()?;
        let local_str = format!("{}", local_addr);

        for stun_server in &self.config.stun_servers {
            match stun_binding_request(
                stun_server,
                &local_str,
                Duration::from_millis(2000),
            ) {
                Ok(result) if result.success => {
                    if let Some(mapped) = result.mapped_addr {
                        info!(
                            "[udp-hole-punch] 公网映射地址: {} (本地: {})",
                            mapped, local_addr
                        );
                        *self.mapped_addr.write() = Some(mapped);
                        return Ok(mapped);
                    }
                }
                _ => {
                    debug!("[udp-hole-punch] STUN 服务器 {} 检测失败", stun_server);
                }
            }
        }

        Err(anyhow::anyhow!("所有 STUN 服务器均无法获取映射地址"))
    }

    /// 检测 NAT 类型
    pub async fn detect_nat_type(&self) -> NatType {
        if self.config.stun_servers.len() < 2 {
            *self.nat_type.write() = NatType::Unknown;
            return NatType::Unknown;
        }

        let local_addr = match self.socket.local_addr() {
            Ok(addr) => format!("{}", addr),
            Err(_) => return NatType::Unknown,
        };

        let nat_type = super::stun::detect_nat_type(
            &self.config.stun_servers[0],
            &self.config.stun_servers[1],
            &local_addr,
            Duration::from_millis(2000),
        );

        *self.nat_type.write() = nat_type;
        info!("[udp-hole-punch] NAT 类型: {:?}", nat_type);
        nat_type
    }

    /// 执行 UDP 打洞（向指定对端地址打洞）
    ///
    /// 这是核心打洞方法：向对端地址发送一系列 UDP 包，
    /// 同时等待对端的包到达。
    pub async fn punch(&self, peer_addr: SocketAddr, peer_id: &str) -> HolePunchResult {
        let start = Instant::now();
        let nat_type = *self.nat_type.read();
        let local_mapped = *self.mapped_addr.read();

        // 记录会话
        let session = HolePunchSession {
            peer_id: peer_id.to_string(),
            peer_addr,
            start_time: start,
            retries: 0,
            success: false,
        };
        self.active_sessions.write().insert(peer_id.to_string(), session);

        info!(
            "[udp-hole-punch] 开始打洞: peer={}, addr={}, nat_type={:?}",
            peer_id, peer_addr, nat_type
        );

        // 根据 NAT 类型选择打洞策略
        let strategy = match nat_type {
            NatType::OpenInternet => "direct_connection",
            NatType::FullCone => "simple_punch",
            NatType::RestrictedCone => "sequential_punch",
            NatType::PortRestrictedCone => "simultaneous_punch",
            NatType::Symmetric => {
                if self.config.enable_port_prediction {
                    "port_prediction"
                } else {
                    "simultaneous_punch"
                }
            }
            NatType::Unknown => "simultaneous_punch",
        };

        let mut result = HolePunchResult {
            success: false,
            peer_addr: None,
            local_mapped_addr: local_mapped,
            duration_ms: 0,
            retries: 0,
            nat_type: format!("{:?}", nat_type),
            error: None,
            strategy: strategy.to_string(),
        };

        // 执行打洞（最多重试 max_retries 次）
        for retry in 0..self.config.max_retries {
            result.retries = retry;

            match self.punch_once(peer_addr, nat_type).await {
                Ok(confirmed_addr) => {
                    result.success = true;
                    result.peer_addr = Some(confirmed_addr);
                    result.duration_ms = start.elapsed().as_millis() as u64;
                    info!(
                        "[udp-hole-punch] 打洞成功: peer={}, addr={}, 耗时={}ms, 重试={}",
                        peer_id, confirmed_addr, result.duration_ms, retry
                    );
                    break;
                }
                Err(e) => {
                    debug!(
                        "[udp-hole-punch] 打洞第 {} 次失败: {}",
                        retry + 1,
                        e
                    );
                    if retry == self.config.max_retries - 1 {
                        result.error = Some(format!("{}", e));
                    }
                    // 重试前等待
                    tokio::time::sleep(Duration::from_millis(self.config.punch_interval_ms * 2)).await;
                }
            }
        }

        if !result.success {
            result.duration_ms = start.elapsed().as_millis() as u64;
            warn!(
                "[udp-hole-punch] 打洞失败: peer={}, 耗时={}ms, 重试={}",
                peer_id, result.duration_ms, result.retries
            );
        }

        // 记录统计
        self.stats.write().record(&result);

        // 更新会话
        if let Some(session) = self.active_sessions.write().get_mut(peer_id) {
            session.success = result.success;
            session.retries = result.retries;
        }

        result
    }

    /// 执行一次打洞
    async fn punch_once(&self, peer_addr: SocketAddr, nat_type: NatType) -> anyhow::Result<SocketAddr> {
        let punch_data = b"PDC_HOLE_PUNCH";
        let timeout = Duration::from_millis(self.config.punch_timeout_ms);

        match nat_type {
            NatType::OpenInternet | NatType::FullCone => {
                // 简单打洞：发送几个包，然后等待响应
                for _ in 0..self.config.punch_packet_count {
                    self.socket.send_to(punch_data, peer_addr).await?;
                    tokio::time::sleep(Duration::from_millis(self.config.punch_interval_ms)).await;
                }
                // 等待对端响应
                self.wait_for_response(timeout).await
            }
            NatType::Symmetric if self.config.enable_port_prediction => {
                // Symmetric NAT 端口预测：尝试多个端口
                let base_port = peer_addr.port();
                let range = self.config.port_prediction_range;

                for offset in 0..range {
                    let target_port = base_port.wrapping_add(offset);
                    let target_addr = SocketAddr::new(peer_addr.ip(), target_port);
                    self.socket.send_to(punch_data, target_addr).await?;

                    // 每隔几个包检查一次响应
                    if offset % 10 == 0 {
                        if let Ok(addr) = self.wait_for_response(Duration::from_millis(50)).await {
                            return Ok(addr);
                        }
                    }
                }
                // 最后等待响应
                self.wait_for_response(timeout).await
            }
            _ => {
                // 同时打洞：快速发送一系列包，等待对端包到达
                for _ in 0..self.config.punch_packet_count {
                    self.socket.send_to(punch_data, peer_addr).await?;
                    // 不等待，快速发送
                }
                // 等待对端响应
                self.wait_for_response(timeout).await
            }
        }
    }

    /// 等待对端响应
    async fn wait_for_response(&self, timeout: Duration) -> anyhow::Result<SocketAddr> {
        let mut buf = [0u8; 1024];
        match tokio::time::timeout(timeout, self.socket.recv_from(&mut buf)).await {
            Ok(Ok((len, addr))) => {
                debug!("[udp-hole-punch] 收到对端响应: {} ({} 字节)", addr, len);
                Ok(addr)
            }
            Ok(Err(e)) => Err(anyhow::anyhow!("接收失败: {}", e)),
            Err(_) => Err(anyhow::anyhow!("等待响应超时")),
        }
    }

    /// 获取本地映射地址
    pub fn mapped_address(&self) -> Option<SocketAddr> {
        *self.mapped_addr.read()
    }

    /// 获取 NAT 类型
    pub fn nat_type(&self) -> NatType {
        *self.nat_type.read()
    }

    /// 获取统计
    pub fn stats(&self) -> HolePunchStats {
        self.stats.read().clone()
    }

    /// 获取活跃会话数
    pub fn active_session_count(&self) -> usize {
        self.active_sessions.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hole_punch_config_default() {
        let config = HolePunchConfig::default();
        assert_eq!(config.punch_timeout_ms, 5000);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.punch_packet_count, 10);
        assert!(config.enable_port_prediction);
    }

    #[test]
    fn test_hole_punch_stats() {
        let mut stats = HolePunchStats::default();
        assert_eq!(stats.total_attempts, 0);
        assert_eq!(stats.success_rate(), 0.0);

        let result = HolePunchResult {
            success: true,
            peer_addr: None,
            local_mapped_addr: None,
            duration_ms: 100,
            retries: 0,
            nat_type: "FullCone".to_string(),
            error: None,
            strategy: "simple_punch".to_string(),
        };
        stats.record(&result);
        assert_eq!(stats.total_attempts, 1);
        assert_eq!(stats.success_count, 1);
        assert_eq!(stats.avg_duration_ms, 100.0);
    }

    #[tokio::test]
    async fn test_hole_puncher_creation() {
        // 绑定到随机端口
        let config = HolePunchConfig::default();
        let puncher = HolePuncher::new("127.0.0.1:0", config).await;
        assert!(puncher.is_ok());

        let puncher = puncher.unwrap();
        assert_eq!(puncher.active_session_count(), 0);
        assert_eq!(puncher.nat_type(), NatType::Unknown);
    }
}
