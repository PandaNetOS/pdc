//! NAT 穿透插件化框架
//!
//! 将 UPnP/NAT-PMP/PCP 抽象为统一的 NatProvider trait，
//! 支持动态注册、按优先级排序、热插拔。
//!
//! 插件化架构：
//! ```
//! NatManager
//!   ├── providers: Vec<Arc<dyn NatProvider>>  (按优先级排序)
//!   │     ├── UpnpProvider
//!   │     ├── NatPmpProvider
//!   │     └── PcpProvider
//!   ├── state: NatStateMachine
//!   └── metrics: NatMetricsExt
//! ```

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use igd::PortMappingProtocol;
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::{GatewayBackend, NatMapping, NatProtocol};

// ---------------------------------------------------------------------------
// NatProvider trait
// ---------------------------------------------------------------------------

/// NAT 穿透提供者 trait（插件化接口）
///
/// 所有 NAT 穿透协议（UPnP/NAT-PMP/PCP）都实现此 trait，
/// NatManager 通过 trait 对象统一调度。
#[async_trait]
pub trait NatProvider: Send + Sync {
    /// 提供者名称
    fn name(&self) -> &str;

    /// 协议类型
    fn protocol(&self) -> NatProtocol;

    /// 优先级（数值越小优先级越高）
    fn priority(&self) -> u8;

    /// 发现网关
    async fn discover(&self) -> anyhow::Result<GatewayBackend>;

    /// 获取公网 IP
    async fn get_external_ip(&self, gateway: &GatewayBackend) -> anyhow::Result<Ipv4Addr>;

    /// 映射端口（含冲突重试）
    ///
    /// 返回 (实际映射的外部端口, 使用的协议)
    async fn map_port(
        &self,
        gateway: &GatewayBackend,
        protocol: PortMappingProtocol,
        internal_port: u16,
        preferred_port: u16,
        lifetime: u32,
        max_retries: u32,
    ) -> anyhow::Result<(u16, NatProtocol)>;

    /// 验证映射是否在路由器上真实存在
    async fn verify_mappings(&self, gateway: &GatewayBackend, mappings: &mut [NatMapping]);

    /// 清理旧的 PDC 映射
    async fn cleanup_old_mappings(&self, gateway: &GatewayBackend);

    /// 释放所有映射
    async fn release_all(&self, gateway: &GatewayBackend, mappings: &[NatMapping]);

    /// 是否支持定期续租
    fn supports_renewal(&self) -> bool;

    /// 是否支持枚举所有映射（用于验证和清理）
    fn supports_enumeration(&self) -> bool;
}

// ---------------------------------------------------------------------------
// Provider 注册表
// ---------------------------------------------------------------------------

/// Provider 注册表
///
/// 管理所有已注册的 NatProvider，按优先级排序。
pub struct ProviderRegistry {
    providers: Vec<Arc<dyn NatProvider>>,
}

impl ProviderRegistry {
    /// 创建空注册表
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    /// 注册 provider（按优先级插入）
    pub fn register(&mut self, provider: Arc<dyn NatProvider>) {
        let name = provider.name().to_string();
        let priority = provider.priority();

        // 检查是否已注册
        if self.providers.iter().any(|p| p.name() == name) {
            debug!("[nat-provider] {} 已注册，跳过", name);
            return;
        }

        // 按优先级插入（数值越小越靠前）
        let pos = self
            .providers
            .iter()
            .position(|p| p.priority() > priority)
            .unwrap_or(self.providers.len());
        self.providers.insert(pos, provider);

        debug!("[nat-provider] 已注册 {} (priority={}, 位置={})", name, priority, pos);
    }

    /// 注销 provider
    pub fn unregister(&mut self, name: &str) -> Option<Arc<dyn NatProvider>> {
        if let Some(pos) = self.providers.iter().position(|p| p.name() == name) {
            let provider = self.providers.remove(pos);
            debug!("[nat-provider] 已注销 {}", name);
            Some(provider)
        } else {
            None
        }
    }

    /// 获取所有 provider（按优先级排序）
    pub fn providers(&self) -> &[Arc<dyn NatProvider>] {
        &self.providers
    }

    /// 按协议类型获取 provider
    pub fn get_by_protocol(&self, protocol: NatProtocol) -> Option<&Arc<dyn NatProvider>> {
        self.providers.iter().find(|p| p.protocol() == protocol)
    }

    /// 按名称获取 provider
    pub fn get_by_name(&self, name: &str) -> Option<&Arc<dyn NatProvider>> {
        self.providers.iter().find(|p| p.name() == name)
    }

    /// 已注册的 provider 数量
    pub fn len(&self) -> usize {
        self.providers.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    /// 获取所有 provider 名称
    pub fn names(&self) -> Vec<String> {
        self.providers.iter().map(|p| p.name().to_string()).collect()
    }
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// 映射操作统计（用于 metrics）
// ---------------------------------------------------------------------------

/// 单次映射操作的统计
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingOperationStats {
    /// 协议类型
    pub protocol: String,
    /// 内部端口
    pub internal_port: u16,
    /// 外部端口
    pub external_port: u16,
    /// 是否成功
    pub success: bool,
    /// 耗时（毫秒）
    pub duration_ms: u64,
    /// 重试次数
    pub retries: u32,
    /// 错误信息（如果失败）
    pub error: Option<String>,
    /// 时间戳
    pub timestamp: u64,
}

impl MappingOperationStats {
    /// 创建成功统计
    pub fn success(
        protocol: NatProtocol,
        internal_port: u16,
        external_port: u16,
        start: Instant,
        retries: u32,
    ) -> Self {
        Self {
            protocol: protocol.as_str().to_string(),
            internal_port,
            external_port,
            success: true,
            duration_ms: start.elapsed().as_millis() as u64,
            retries,
            error: None,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// 创建失败统计
    pub fn failure(
        protocol: NatProtocol,
        internal_port: u16,
        start: Instant,
        retries: u32,
        error: String,
    ) -> Self {
        Self {
            protocol: protocol.as_str().to_string(),
            internal_port,
            external_port: 0,
            success: false,
            duration_ms: start.elapsed().as_millis() as u64,
            retries,
            error: Some(error),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_registry() {
        let mut registry = ProviderRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_mapping_operation_stats() {
        let start = Instant::now();
        let stats = MappingOperationStats::success(
            NatProtocol::Upnp,
            6880,
            6880,
            start,
            0,
        );
        assert!(stats.success);
        assert_eq!(stats.internal_port, 6880);
        assert_eq!(stats.external_port, 6880);
        assert_eq!(stats.retries, 0);
    }
}
