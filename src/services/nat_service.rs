//! NatService — NAT 穿透业务层
//!
//! 封装 NatManager，提供面向业务的接口：
//! - 状态查询
//! - 手动触发映射/释放
//! - metrics 统计
//! - 可达性检测

use std::sync::Arc;

use tracing::warn;

use crate::nat::{NatManager, NatStatus, NatType, ReachabilityResult};

/// STUN UDP 可达性检测超时
const STUN_REACHABILITY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

pub struct NatService {
    manager: Arc<NatManager>,
}

impl NatService {
    pub fn new(manager: Arc<NatManager>) -> Self {
        Self { manager }
    }

    pub fn manager(&self) -> &Arc<NatManager> {
        &self.manager
    }

    /// 获取 NAT 状态
    pub fn status(&self) -> NatStatus {
        self.manager.status()
    }

    /// 获取 NAT 类型
    pub fn nat_type(&self) -> NatType {
        self.manager.status().nat_type
    }

    /// 公网可达性评分（0-100）
    pub fn reachability_score(&self) -> u8 {
        self.manager.status().reachability_score
    }

    /// 网关是否健康
    pub fn gateway_healthy(&self) -> bool {
        self.manager.status().gateway_healthy
    }

    /// 获取公网 IP
    pub fn external_ip(&self) -> Option<String> {
        self.manager.status().external_ip
    }

    /// 获取本地 IP
    pub fn local_ip(&self) -> Option<String> {
        self.manager.status().local_ip
    }

    /// 当前活跃映射数
    pub fn active_mappings(&self) -> usize {
        self.manager.status().mappings.len()
    }

    /// 手动释放所有映射
    pub async fn release_all(&self) {
        self.manager.release_all().await;
    }

    /// 手动检测 UDP 端口可达性（通过 STUN）
    ///
    /// 服务器列表先经内置 [`crate::dns_pool`] 预解析成 `ip:port` 字面量，再交给
    /// 同步 STUN 探测 —— 同步 Binding 内部走 `to_socket_addrs()`（即系统 DNS），
    /// 直接把域名递下去会在宿主解析器不可用时把**全部**服务器误判为不可达。
    ///
    /// 返回 `None` 表示**无法检测**：可达性自检被关闭，或 STUN 服务器列表全部
    /// 解析失败（此时不能得出「不可达」的结论，只能说不具备检测条件）。
    pub async fn check_udp_port(&self, port: u16) -> Option<ReachabilityResult> {
        // 先取出所需配置，避免持有 &NatConfig 跨越 await
        let (enabled, stun_servers) = {
            let config = self.manager.config();
            (
                config.enable_reachability_check,
                config.stun_servers.clone(),
            )
        };
        if !enabled {
            return None;
        }

        let servers = crate::dns_pool::global()
            .resolve_endpoints(&stun_servers, 3478)
            .await;
        if servers.is_empty() {
            warn!(
                "[nat_service] STUN 服务器（配置 {} 项）全部解析失败，跳过 UDP {} 可达性检测",
                stun_servers.len(),
                port
            );
            return None;
        }

        // 同步 STUN 最长阻塞 STUN_REACHABILITY_TIMEOUT × 服务器数，挪到阻塞线程池
        tokio::task::spawn_blocking(move || {
            crate::nat::stun::check_udp_reachability(&servers, port, STUN_REACHABILITY_TIMEOUT)
        })
        .await
        .ok()
    }

    /// 获取映射统计摘要
    pub fn mapping_summary(&self) -> MappingSummary {
        let status = self.manager.status();
        let tcp_total = status
            .mappings
            .iter()
            .filter(|m| m.protocol == "TCP")
            .count();
        let udp_total = status
            .mappings
            .iter()
            .filter(|m| m.protocol == "UDP")
            .count();
        let verified = status.mappings.iter().filter(|m| m.verified).count();
        let reachable = status.mappings.iter().filter(|m| m.reachable).count();

        MappingSummary {
            total: status.mappings.len(),
            tcp_total,
            udp_total,
            verified,
            reachable,
            reachability_score: status.reachability_score,
            gateway_healthy: status.gateway_healthy,
            nat_type: status.nat_type,
        }
    }
}

/// 映射摘要（用于监控面板快速展示）
#[derive(Debug, Clone)]
pub struct MappingSummary {
    pub total: usize,
    pub tcp_total: usize,
    pub udp_total: usize,
    pub verified: usize,
    pub reachable: usize,
    pub reachability_score: u8,
    pub gateway_healthy: bool,
    pub nat_type: NatType,
}
