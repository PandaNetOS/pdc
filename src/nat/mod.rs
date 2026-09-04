//! NAT 穿透模块
//!
//! 自动配置路由器端口映射，确保 PDC 服务从公网可达。
//!
//! 策略：
//! 1. UPnP IGD 发现网关
//! 2. 映射端口（冲突时自动尝试备用端口，最多 10 次）
//! 3. 映射后通过枚举路由器映射验证
//! 4. 定期续租（lease_duration / 2 间隔）
//! 5. 启动时清理同名旧映射，退出时释放
//!
//! 支持协议：UPnP IGDv1/IGDv2

use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use serde::Serialize;
use tracing::{debug, info, warn};

/// 单条端口映射
#[derive(Debug, Clone, Serialize)]
pub struct NatMapping {
    /// 协议：TCP / UDP
    pub protocol: String,
    /// 内部监听端口
    pub internal_port: u16,
    /// 实际映射的外部端口
    pub external_port: u16,
    /// 映射描述（用于识别和清理）
    pub description: String,
    /// 是否验证通过（路由器上确实存在）
    pub verified: bool,
}

/// NAT 整体状态
#[derive(Debug, Clone, Serialize)]
pub struct NatStatus {
    /// 是否已启用
    pub enabled: bool,
    /// UPnP 网关是否发现成功
    pub gateway_found: bool,
    /// 网关地址
    pub gateway_addr: Option<String>,
    /// 公网 IP
    pub external_ip: Option<String>,
    /// 映射列表
    pub mappings: Vec<NatMapping>,
    /// 最后错误
    pub last_error: Option<String>,
}

/// NAT 管理器
pub struct NatManager {
    enabled: bool,
    lease_duration: u32,
    local_ip: Ipv4Addr,
    mappings: Arc<RwLock<Vec<NatMapping>>>,
    gateway: Arc<RwLock<Option<igd::Gateway>>>,
    external_ip: Arc<RwLock<Option<String>>>,
    last_error: Arc<RwLock<Option<String>>>,
}

impl NatManager {
    /// 创建 NAT 管理器
    pub fn new(enabled: bool, lease_duration: u32) -> Self {
        let local_ip = Self::detect_local_ip().unwrap_or(Ipv4Addr::new(0, 0, 0, 0));
        Self {
            enabled,
            lease_duration,
            local_ip,
            mappings: Arc::new(RwLock::new(Vec::new())),
            gateway: Arc::new(RwLock::new(None)),
            external_ip: Arc::new(RwLock::new(None)),
            last_error: Arc::new(RwLock::new(None)),
        }
    }

    /// 检测本地 IPv4 地址（通过连接外部地址获取出站 IP）
    fn detect_local_ip() -> Option<Ipv4Addr> {
        let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
        sock.connect("8.8.8.8:80").ok()?;
        let addr = sock.local_addr().ok()?;
        match addr.ip() {
            std::net::IpAddr::V4(v4) => Some(v4),
            _ => None,
        }
    }

    /// 初始化：发现网关 + 映射端口 + 启动续租任务
    ///
    /// # 参数
    /// - `http_port`: HTTP 服务端口（TCP）
    /// - `udp_port`: UDP Tracker 端口（UDP）
    /// - `crawler_port`: DHT 爬虫端口（UDP，为 0 则跳过）
    pub async fn init(
        &self,
        http_port: u16,
        udp_port: u16,
        crawler_port: u16,
    ) -> anyhow::Result<()> {
        if !self.enabled {
            info!("[nat] UPnP 未启用，跳过端口映射");
            return Ok(());
        }

        info!(
            "[nat] 开始 UPnP 端口映射初始化（本地 IP: {}）...",
            self.local_ip
        );

        // 1. 发现网关
        let gateway = match self.discover_gateway().await {
            Ok(gw) => gw,
            Err(e) => {
                let msg = format!("UPnP 网关发现失败: {}", e);
                warn!("[nat] {}", msg);
                *self.last_error.write() = Some(msg.clone());
                return Err(anyhow::anyhow!(msg));
            }
        };

        info!("[nat] 发现网关: {}", gateway.addr);
        *self.gateway.write() = Some(gateway.clone());

        // 2. 获取公网 IP
        match self.get_external_ip(&gateway).await {
            Ok(ip) => {
                info!("[nat] 公网 IP: {}", ip);
                *self.external_ip.write() = Some(ip.to_string());
            }
            Err(e) => {
                warn!("[nat] 获取公网 IP 失败: {}", e);
            }
        }

        // 3. 清理旧映射（同名的）
        self.cleanup_old_mappings(&gateway).await;

        // 4. 映射端口
        let mut success_count = 0;

        // HTTP TCP
        match self
            .map_port_with_retry(
                &gateway,
                igd::PortMappingProtocol::TCP,
                http_port,
                "PDC HTTP Tracker",
            )
            .await
        {
            Ok(ext_port) => {
                info!("[nat] TCP {} → 外部 {} 映射成功", http_port, ext_port);
                self.mappings.write().push(NatMapping {
                    protocol: "TCP".to_string(),
                    internal_port: http_port,
                    external_port: ext_port,
                    description: "PDC HTTP Tracker".to_string(),
                    verified: false,
                });
                success_count += 1;
            }
            Err(e) => {
                warn!("[nat] TCP {} 映射失败: {}", http_port, e);
                *self.last_error.write() = Some(format!("TCP {} 映射失败: {}", http_port, e));
            }
        }

        // UDP Tracker
        match self
            .map_port_with_retry(
                &gateway,
                igd::PortMappingProtocol::UDP,
                udp_port,
                "PDC UDP Tracker",
            )
            .await
        {
            Ok(ext_port) => {
                info!("[nat] UDP {} → 外部 {} 映射成功", udp_port, ext_port);
                self.mappings.write().push(NatMapping {
                    protocol: "UDP".to_string(),
                    internal_port: udp_port,
                    external_port: ext_port,
                    description: "PDC UDP Tracker".to_string(),
                    verified: false,
                });
                success_count += 1;
            }
            Err(e) => {
                warn!("[nat] UDP {} 映射失败: {}", udp_port, e);
                *self.last_error.write() = Some(format!("UDP {} 映射失败: {}", udp_port, e));
            }
        }

        // DHT 爬虫 UDP（如果配置了非零端口）
        if crawler_port > 0 {
            match self
                .map_port_with_retry(
                    &gateway,
                    igd::PortMappingProtocol::UDP,
                    crawler_port,
                    "PDC DHT Crawler",
                )
                .await
            {
                Ok(ext_port) => {
                    info!(
                        "[nat] UDP {} → 外部 {} 映射成功（DHT爬虫）",
                        crawler_port, ext_port
                    );
                    self.mappings.write().push(NatMapping {
                        protocol: "UDP".to_string(),
                        internal_port: crawler_port,
                        external_port: ext_port,
                        description: "PDC DHT Crawler".to_string(),
                        verified: false,
                    });
                    success_count += 1;
                }
                Err(e) => {
                    warn!("[nat] UDP {} 映射失败（DHT爬虫）: {}", crawler_port, e);
                }
            }
        }

        // 5. 验证所有映射
        self.verify_mappings(&gateway).await;

        // 6. 启动续租任务
        if self.lease_duration > 0 {
            self.start_renewal_task();
        }

        let verified_count = self.mappings.read().iter().filter(|m| m.verified).count();
        info!(
            "[nat] UPnP 初始化完成: {}/{} 映射成功, {} 验证通过",
            success_count,
            if crawler_port > 0 { 3 } else { 2 },
            verified_count
        );

        if success_count == 0 {
            return Err(anyhow::anyhow!(
                "所有端口映射均失败，请检查路由器 UPnP 设置或手动配置端口转发"
            ));
        }

        Ok(())
    }

    /// 发现 UPnP 网关
    async fn discover_gateway(&self) -> anyhow::Result<igd::Gateway> {
        tokio::task::spawn_blocking(|| igd::search_gateway(igd::SearchOptions::default()))
            .await
            .map_err(|e| anyhow::anyhow!("网关发现任务失败: {}", e))?
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 获取公网 IP
    async fn get_external_ip(&self, gateway: &igd::Gateway) -> anyhow::Result<Ipv4Addr> {
        let gw = gateway.clone();
        tokio::task::spawn_blocking(move || gw.get_external_ip())
            .await
            .map_err(|e| anyhow::anyhow!("获取公网IP任务失败: {}", e))?
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 映射端口，冲突时自动尝试备用端口（最多 10 次）
    ///
    /// 返回实际映射成功的外部端口
    async fn map_port_with_retry(
        &self,
        gateway: &igd::Gateway,
        protocol: igd::PortMappingProtocol,
        preferred_port: u16,
        description: &str,
    ) -> anyhow::Result<u16> {
        let max_retries = 10;
        let local_ip = self.local_ip;
        let lease = self.lease_duration;

        for offset in 0..max_retries {
            let external_port = preferred_port.wrapping_add(offset);
            if external_port == 0 {
                continue;
            }

            let local_addr = SocketAddrV4::new(local_ip, preferred_port);
            let gw = gateway.clone();
            let desc = description.to_string();

            let result = tokio::task::spawn_blocking(move || {
                gw.add_port(protocol, external_port, local_addr, lease, &desc)
            })
            .await
            .map_err(|e| anyhow::anyhow!("映射任务失败: {}", e))?;

            match result {
                Ok(()) => return Ok(external_port),
                Err(igd::AddPortError::PortInUse) => {
                    debug!("[nat] 端口 {} 被占用，尝试下一个...", external_port);
                    continue;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("映射失败: {}", e));
                }
            }
        }

        Err(anyhow::anyhow!(
            "映射失败，已尝试 {} 个端口均被占用",
            max_retries
        ))
    }

    /// 验证映射是否在路由器上真实存在
    async fn verify_mappings(&self, gateway: &igd::Gateway) {
        let gw = gateway.clone();

        // 枚举路由器上的所有映射
        let router_mappings = tokio::task::spawn_blocking(move || {
            let mut result = Vec::new();
            for i in 0..200 {
                match gw.get_generic_port_mapping_entry(i) {
                    Ok(entry) => result.push(entry),
                    Err(_) => break,
                }
            }
            result
        })
        .await
        .unwrap_or_default();

        // 逐条验证
        let mut mappings = self.mappings.write();
        for m in mappings.iter_mut() {
            let proto = match m.protocol.as_str() {
                "TCP" => igd::PortMappingProtocol::TCP,
                "UDP" => igd::PortMappingProtocol::UDP,
                _ => continue,
            };

            m.verified = router_mappings.iter().any(|e| {
                e.enabled
                    && e.external_port == m.external_port
                    && e.protocol == proto
                    && e.internal_port == m.internal_port
            });

            if m.verified {
                debug!("[nat] 映射验证通过: {} {}", m.protocol, m.external_port);
            } else {
                warn!(
                    "[nat] 映射验证失败（路由器上未找到）: {} {}",
                    m.protocol, m.external_port
                );
            }
        }
    }

    /// 清理旧的 PDC 映射（防止上次崩溃残留）
    async fn cleanup_old_mappings(&self, gateway: &igd::Gateway) {
        let gw = gateway.clone();
        let old_mappings = tokio::task::spawn_blocking(move || {
            let mut result = Vec::new();
            for i in 0..200 {
                match gw.get_generic_port_mapping_entry(i) {
                    Ok(entry) => {
                        if entry.port_mapping_description.contains("PDC") {
                            result.push(entry);
                        }
                    }
                    Err(_) => break,
                }
            }
            result
        })
        .await
        .unwrap_or_default();

        for entry in old_mappings {
            let gw = gateway.clone();
            let port = entry.external_port;
            let proto = entry.protocol;
            tokio::task::spawn_blocking(move || {
                let _ = gw.remove_port(proto, port);
            })
            .await
            .ok();
            debug!("[nat] 清理旧映射: {:?} {}", proto, port);
        }
    }

    /// 启动定期续租任务
    fn start_renewal_task(&self) {
        let mappings = self.mappings.clone();
        let gateway = self.gateway.clone();
        let lease = self.lease_duration;
        let external_ip = self.external_ip.clone();
        let local_ip = self.local_ip;

        let renew_interval = Duration::from_secs((lease.max(60) / 2) as u64);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(renew_interval).await;

                let gw = match gateway.read().clone() {
                    Some(g) => g,
                    None => continue,
                };

                let current_mappings = mappings.read().clone();
                for m in &current_mappings {
                    let proto = match m.protocol.as_str() {
                        "TCP" => igd::PortMappingProtocol::TCP,
                        "UDP" => igd::PortMappingProtocol::UDP,
                        _ => continue,
                    };

                    let external_port = m.external_port;
                    let internal_port = m.internal_port;
                    let desc = m.description.clone();
                    let local_addr = SocketAddrV4::new(local_ip, internal_port);
                    let gw_clone = gw.clone();

                    let result = tokio::task::spawn_blocking(move || {
                        gw_clone.add_port(proto, external_port, local_addr, lease, &desc)
                    })
                    .await;

                    match result {
                        Ok(Ok(())) => {
                            debug!("[nat] 续租成功: {} {}", m.protocol, m.external_port);
                        }
                        Ok(Err(e)) => {
                            warn!("[nat] 续租失败 {} {}: {}", m.protocol, m.external_port, e);
                        }
                        Err(e) => {
                            warn!("[nat] 续租任务异常: {}", e);
                        }
                    }
                }

                // 刷新公网 IP
                let gw_clone = gw.clone();
                if let Ok(Ok(ip)) =
                    tokio::task::spawn_blocking(move || gw_clone.get_external_ip()).await
                {
                    *external_ip.write() = Some(ip.to_string());
                }
            }
        });

        info!("[nat] 续租任务已启动，间隔 {:?}", renew_interval);
    }

    /// 释放所有映射（退出时调用）
    pub async fn release_all(&self) {
        let gw = match self.gateway.read().clone() {
            Some(g) => g,
            None => return,
        };

        let mappings_snapshot = self.mappings.read().clone();
        for m in &mappings_snapshot {
            let proto = match m.protocol.as_str() {
                "TCP" => igd::PortMappingProtocol::TCP,
                "UDP" => igd::PortMappingProtocol::UDP,
                _ => continue,
            };
            let external_port = m.external_port;
            let gw = gw.clone();
            let _ = tokio::task::spawn_blocking(move || gw.remove_port(proto, external_port)).await;
            info!("[nat] 释放映射: {} {}", m.protocol, m.external_port);
        }

        self.mappings.write().clear();
    }

    /// 获取当前 NAT 状态
    pub fn status(&self) -> NatStatus {
        NatStatus {
            enabled: self.enabled,
            gateway_found: self.gateway.read().is_some(),
            gateway_addr: self.gateway.read().as_ref().map(|g| g.addr.to_string()),
            external_ip: self.external_ip.read().clone(),
            mappings: self.mappings.read().clone(),
            last_error: self.last_error.read().clone(),
        }
    }
}

impl Default for NatManager {
    fn default() -> Self {
        Self::new(false, 3600)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nat_manager_default() {
        let mgr = NatManager::default();
        let status = mgr.status();
        assert!(!status.enabled);
        assert!(!status.gateway_found);
        assert!(status.mappings.is_empty());
    }

    #[test]
    fn test_nat_mapping_serialize() {
        let m = NatMapping {
            protocol: "TCP".to_string(),
            internal_port: 6880,
            external_port: 6880,
            description: "test".to_string(),
            verified: true,
        };
        let json = serde_json::to_string(&m).unwrap();
        assert!(json.contains("6880"));
        assert!(json.contains("TCP"));
    }

    #[test]
    fn test_nat_status_serialize() {
        let status = NatStatus {
            enabled: true,
            gateway_found: true,
            gateway_addr: Some("192.168.1.1".to_string()),
            external_ip: Some("1.2.3.4".to_string()),
            mappings: vec![],
            last_error: None,
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("1.2.3.4"));
        assert!(json.contains("192.168.1.1"));
    }

    #[test]
    fn test_detect_local_ip() {
        // 这个测试在有网络的环境下会返回 Some
        let ip = NatManager::detect_local_ip();
        // 不断言具体值，只确保不 panic
        let _ = ip;
    }
}
