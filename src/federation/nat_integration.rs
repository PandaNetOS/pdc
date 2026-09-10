//! NAT 集成
//!
//! 与 NatManager 集成，获取公网地址和端口映射，更新节点身份的地址信息。
//! 阶段2扩展：集成 STUN 探测，综合判定可达性。

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::federation::config::FederationConfig;
use crate::federation::node_id::{NodeAddress, NodeIdentity, Reachability};
use crate::nat::stun::{detect_nat_type_multi, stun_binding_request_multi, StunResult};
use crate::nat::NatManager;

/// NAT 集成服务
pub struct NatIntegration {
    /// NAT 管理器
    nat_manager: Arc<NatManager>,
    /// 节点身份
    identity: Arc<NodeIdentity>,
    /// 配置
    config: FederationConfig,
    /// 关闭信号
    shutdown: broadcast::Sender<()>,
    /// 最近一次 STUN 探测结果
    last_stun: parking_lot::RwLock<Option<StunResult>>,
}

impl NatIntegration {
    /// 创建 NAT 集成服务
    pub fn new(
        nat_manager: Arc<NatManager>,
        identity: Arc<NodeIdentity>,
        config: FederationConfig,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            nat_manager,
            identity,
            config,
            shutdown,
            last_stun: parking_lot::RwLock::new(None),
        }
    }

    /// 执行 STUN 探测
    ///
    /// 使用多服务器绑定请求获取公网映射地址，并用双服务器对比检测 NAT 类型。
    /// 如果所有 STUN 服务器都不可达，返回 None。
    pub fn stun_probe(&self) -> Option<StunResult> {
        if self.config.stun_servers.is_empty() {
            warn!("[federation] 未配置 STUN 服务器，跳过探测，可达性将保持 Unknown");
            return None;
        }

        // 绑定临时端口，不要绑定联邦 listen_port：
        // udp_transport 已在构造时占用 UDP listen_port（mod.rs 中 UdpTransport::try_bind），
        // 再次 bind 同一端口会直接 EADDRINUSE，导致所有 STUN 服务器看似全部失败。
        // STUN 只需要公网 IP 和 NAT 类型，映射端口后续由 UPnP/默认端口决定。
        let local_addr = "0.0.0.0:0";
        let timeout = Duration::from_secs(5);

        info!(
            "[federation] STUN 探测开始: 服务器数={}, 列表={:?}, 本地绑定={}",
            self.config.stun_servers.len(),
            self.config.stun_servers,
            local_addr
        );

        // 1. 多服务器绑定请求：获取公网映射地址（第一个成功的服务器）
        let binding = stun_binding_request_multi(&self.config.stun_servers, local_addr, timeout);
        let binding = match binding {
            Some(b) => b,
            None => {
                warn!(
                    "[federation] STUN 探测失败: 所有 {} 个服务器均无响应（检查网络/防火墙/服务器列表）",
                    self.config.stun_servers.len()
                );
                return None;
            }
        };

        // 2. 双服务器 NAT 类型检测：区分 Full Cone / Symmetric / Open Internet
        let nat_type = detect_nat_type_multi(&self.config.stun_servers, local_addr, timeout);

        info!(
            "[federation] STUN 探测成功: mapped={:?}, nat_type={:?}, rtt={}ms, server={}",
            binding.mapped_addr, nat_type, binding.rtt_ms, binding.server
        );

        let result = StunResult {
            nat_type,
            ..binding
        };

        *self.last_stun.write() = Some(result.clone());
        Some(result)
    }

    /// 设置端口映射（从 NatManager.status() 读取映射结果和公网IP，更新 identity.addresses）
    pub fn setup_mapping(&self) {
        if !self.config.nat_mapping_enabled {
            info!("[federation] NAT 映射未启用");
            return;
        }

        let status = self.nat_manager.status();
        let listen_port = self.config.listen_port;

        // 查找联邦端口的映射
        let fed_mapping = status.mappings.iter().find(|m| {
            m.description.contains("Federation") && m.internal_port == listen_port
        });

        let external_port = fed_mapping.map(|m| m.external_port).unwrap_or(listen_port);

        // 获取公网 IP（优先 STUN 结果，其次 NatManager）
        let stun = self.last_stun.read();
        let public_ip = stun
            .as_ref()
            .and_then(|s| s.mapped_addr.map(|a| a.ip().to_string()))
            .or_else(|| status.external_ip.clone());

        // 构造公网地址
        let public_addr = public_ip.as_ref().and_then(|ip| {
            ip.parse::<IpAddr>()
                .ok()
                .map(|ip| SocketAddr::new(ip, external_port))
        });

        // 检测可达性（综合 STUN + UPnP）
        let reachability = self.detect_reachability(&status, stun.as_ref());

        // NAT 类型（优先 STUN）
        let nat_type_str = stun
            .as_ref()
            .map(|s| format!("{:?}", s.nat_type))
            .unwrap_or_else(|| format!("{:?}", status.nat_type));

        // 构造本地地址
        let local_ip = status.local_ip.clone().and_then(|ip| ip.parse::<IpAddr>().ok());
        let local_addr = local_ip.map(|ip| SocketAddr::new(ip, listen_port));

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut addresses = Vec::new();

        // 公网地址
        if let Some(addr) = public_addr {
            if addr.is_ipv4() {
                addresses.push(NodeAddress {
                    node_id: self.identity.node_id.0,
                    ipv4_addr: Some(addr),
                    ipv6_addr: None,
                    reachability,
                    last_seen: now,
                    nat_type: Some(nat_type_str.clone()),
                });
            } else {
                addresses.push(NodeAddress {
                    node_id: self.identity.node_id.0,
                    ipv4_addr: None,
                    ipv6_addr: Some(addr),
                    reachability: Reachability::PublicIpv6,
                    last_seen: now,
                    nat_type: Some(nat_type_str.clone()),
                });
            }
        }

        // 本地地址（如果与公网不同）
        if let Some(addr) = local_addr {
            if Some(addr) != public_addr {
                addresses.push(NodeAddress {
                    node_id: self.identity.node_id.0,
                    ipv4_addr: if addr.is_ipv4() { Some(addr) } else { None },
                    ipv6_addr: if addr.is_ipv6() { Some(addr) } else { None },
                    reachability: Reachability::Unknown,
                    last_seen: now,
                    nat_type: Some(nat_type_str),
                });
            }
        }

        self.identity.update_addresses(addresses);
        info!(
            "[federation] NAT 映射设置完成，公网地址: {:?}, 可达性: {}",
            public_addr, reachability
        );
    }

    /// 获取公网地址
    pub fn get_public_address(&self) -> Option<SocketAddr> {
        let status = self.nat_manager.status();
        let listen_port = self.config.listen_port;

        let fed_mapping = status.mappings.iter().find(|m| {
            m.description.contains("Federation") && m.internal_port == listen_port
        });
        let external_port = fed_mapping.map(|m| m.external_port).unwrap_or(listen_port);

        // 优先 STUN 结果
        let stun = self.last_stun.read();
        if let Some(ref s) = *stun {
            if let Some(addr) = s.mapped_addr {
                return Some(SocketAddr::new(addr.ip(), external_port));
            }
        }

        status.external_ip.as_ref().and_then(|ip| {
            ip.parse::<IpAddr>()
                .ok()
                .map(|ip| SocketAddr::new(ip, external_port))
        })
    }

    /// 综合判定可达性等级（STUN + UPnP 映射）
    pub fn detect_reachability(
        &self,
        status: &crate::nat::NatStatus,
        stun: Option<&StunResult>,
    ) -> Reachability {
        use crate::nat::stun::NatType;

        // 有公网 IP 且映射验证通过
        let has_mapping = status
            .mappings
            .iter()
            .any(|m| m.description.contains("Federation") && m.verified);

        if has_mapping && status.external_ip.is_some() {
            return Reachability::Mapped;
        }

        // 优先使用 STUN 探测的 NAT 类型
        let nat_type = stun.map(|s| s.nat_type).unwrap_or(status.nat_type);

        let result = match nat_type {
            NatType::OpenInternet | NatType::FullCone => Reachability::HolePunchable,
            NatType::RestrictedCone | NatType::PortRestrictedCone => Reachability::HolePunchable,
            NatType::Symmetric => Reachability::OutboundOnly,
            NatType::Unknown => {
                if status.external_ip.is_some() {
                    Reachability::Mapped
                } else if stun_succeeded(stun) {
                    // STUN 成功收到响应，说明出站 UDP 可达，
                    // 但无法确定具体 NAT 类型（可能单服务器不足或网络受限）。
                    // 至少是 OutboundOnly，不应降级为 Unknown。
                    Reachability::OutboundOnly
                } else {
                    // STUN 完全不可达且无公网 IP，真正的未知状态
                    Reachability::Unknown
                }
            }
        };

        debug!(
            "[federation] 可达性判定: has_mapping={}, external_ip={:?}, stun_nat_type={:?}, stun_succeeded={}, status_nat_type={:?} => {}",
            has_mapping,
            status.external_ip,
            stun.map(|s| s.nat_type),
            stun_succeeded(stun),
            status.nat_type,
            result
        );

        result
    }

    /// 启动地址刷新后台任务（每5分钟重新探测公网地址 + STUN）
    pub fn spawn_address_refresh(self: Arc<Self>) {
        let mut shutdown_rx = self.shutdown.subscribe();
        let interval = Duration::from_secs(300);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        self.clone().refresh_addresses();
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[federation] 地址刷新任务收到关闭信号");
                        break;
                    }
                }
            }
        });
        info!("[federation] 地址刷新任务已启动（间隔 300s，含 STUN 探测）");
    }

    /// 刷新地址（STUN 探测 + 公网地址变化检测）
    fn refresh_addresses(self: Arc<Self>) {
        // 执行 STUN 探测
        let _ = self.stun_probe();

        let old_addrs = self.identity.addresses_snapshot();
        let old_public = old_addrs.iter().find_map(|a| a.ipv4_addr);

        self.setup_mapping();

        let new_addrs = self.identity.addresses_snapshot();
        let new_public = new_addrs.iter().find_map(|a| a.ipv4_addr);

        if old_public != new_public {
            info!(
                "[federation] 公网地址变化: {:?} -> {:?}",
                old_public, new_public
            );
        } else {
            debug!("[federation] 地址刷新完成，公网地址未变: {:?}", new_public);
        }
    }
}

/// 检查 STUN 探测是否成功（收到了有效响应）
fn stun_succeeded(stun: Option<&StunResult>) -> bool {
    stun.map(|s| s.success && s.mapped_addr.is_some()).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nat::{NatConfig, NatStatus, NatType};

    fn make_test_config() -> FederationConfig {
        FederationConfig {
            enabled: true,
            listen_port: 6885,
            nat_mapping_enabled: true,
            stun_servers: vec!["stun.l.google.com:19302".to_string()],
            ..Default::default()
        }
    }

    #[test]
    fn test_nat_integration_creation() {
        let nat_config = NatConfig {
            enabled: false,
            ..Default::default()
        };
        let nat = Arc::new(NatManager::new(nat_config));
        let identity = Arc::new(NodeIdentity::generate());
        let (shutdown_tx, _) = broadcast::channel(1);
        let integration = NatIntegration::new(nat, identity, make_test_config(), shutdown_tx);
        assert!(integration.get_public_address().is_none());
    }

    #[test]
    fn test_detect_reachability_mapped() {
        let nat_config = NatConfig {
            enabled: false,
            ..Default::default()
        };
        let nat = Arc::new(NatManager::new(nat_config));
        let identity = Arc::new(NodeIdentity::generate());
        let (shutdown_tx, _) = broadcast::channel(1);
        let integration = NatIntegration::new(nat, identity, make_test_config(), shutdown_tx);

        let status = NatStatus {
            enabled: true,
            gateway_found: true,
            gateway_healthy: true,
            gateway_addr: Some("192.168.1.1".to_string()),
            external_ip: Some("1.2.3.4".to_string()),
            local_ip: Some("192.168.1.100".to_string()),
            nat_type: NatType::FullCone,
            reachability_score: 80,
            mappings: vec![crate::nat::NatMapping {
                protocol: "TCP".to_string(),
                internal_port: 6885,
                external_port: 6885,
                description: "PDC Federation TCP".to_string(),
                verified: true,
                reachable: true,
            }],
            metrics: Default::default(),
            last_error: None,
        };

        assert_eq!(
            integration.detect_reachability(&status, None),
            Reachability::Mapped
        );
    }

    #[test]
    fn test_detect_reachability_hole_punch() {
        let nat_config = NatConfig {
            enabled: false,
            ..Default::default()
        };
        let nat = Arc::new(NatManager::new(nat_config));
        let identity = Arc::new(NodeIdentity::generate());
        let (shutdown_tx, _) = broadcast::channel(1);
        let integration = NatIntegration::new(nat, identity, make_test_config(), shutdown_tx);

        let status = NatStatus {
            enabled: true,
            gateway_found: false,
            gateway_healthy: false,
            gateway_addr: None,
            external_ip: None,
            local_ip: Some("192.168.1.100".to_string()),
            nat_type: NatType::FullCone,
            reachability_score: 50,
            mappings: vec![],
            metrics: Default::default(),
            last_error: None,
        };

        assert_eq!(
            integration.detect_reachability(&status, None),
            Reachability::HolePunchable
        );
    }

    #[test]
    fn test_detect_reachability_outbound_only() {
        let nat_config = NatConfig {
            enabled: false,
            ..Default::default()
        };
        let nat = Arc::new(NatManager::new(nat_config));
        let identity = Arc::new(NodeIdentity::generate());
        let (shutdown_tx, _) = broadcast::channel(1);
        let integration = NatIntegration::new(nat, identity, make_test_config(), shutdown_tx);

        let status = NatStatus {
            enabled: true,
            gateway_found: false,
            gateway_healthy: false,
            gateway_addr: None,
            external_ip: None,
            local_ip: Some("192.168.1.100".to_string()),
            nat_type: NatType::Symmetric,
            reachability_score: 20,
            mappings: vec![],
            metrics: Default::default(),
            last_error: None,
        };

        assert_eq!(
            integration.detect_reachability(&status, None),
            Reachability::OutboundOnly
        );
    }

    #[test]
    fn test_setup_mapping_disabled() {
        let nat_config = NatConfig {
            enabled: false,
            ..Default::default()
        };
        let nat = Arc::new(NatManager::new(nat_config));
        let identity = Arc::new(NodeIdentity::generate());
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut config = make_test_config();
        config.nat_mapping_enabled = false;
        let integration = NatIntegration::new(nat, identity.clone(), config, shutdown_tx);

        integration.setup_mapping();
        assert!(identity.addresses_snapshot().is_empty());
    }

    #[test]
    fn test_stun_probe_no_servers() {
        let nat_config = NatConfig {
            enabled: false,
            ..Default::default()
        };
        let nat = Arc::new(NatManager::new(nat_config));
        let identity = Arc::new(NodeIdentity::generate());
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut config = make_test_config();
        config.stun_servers = vec![];
        let integration = NatIntegration::new(nat, identity, config, shutdown_tx);

        // 无 STUN 服务器时应返回 None
        assert!(integration.stun_probe().is_none());
    }
}
