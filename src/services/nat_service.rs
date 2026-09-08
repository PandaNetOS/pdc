//! NatService — NAT 穿透业务层
//!
//! 封装 NatManager，提供面向业务的接口：
//! - 状态查询
//! - 手动触发映射/释放
//! - metrics 统计
//! - 可达性检测

use std::sync::Arc;

use crate::nat::{NatManager, NatStatus, NatType, ReachabilityResult};

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
    pub fn check_udp_port(&self, port: u16) -> Option<ReachabilityResult> {
        let config = self.manager.config();
        if !config.enable_reachability_check {
            return None;
        }
        let servers = config.stun_servers.clone();
        let result = crate::nat::stun::check_udp_reachability(
            &servers,
            port,
            std::time::Duration::from_secs(5),
        );
        Some(result)
    }

    /// 获取映射统计摘要
    pub fn mapping_summary(&self) -> MappingSummary {
        let status = self.manager.status();
        let tcp_total = status.mappings.iter().filter(|m| m.protocol == "TCP").count();
        let udp_total = status.mappings.iter().filter(|m| m.protocol == "UDP").count();
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
