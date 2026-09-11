//! 控制面
//!
//! 控制面负责策略决策、配置管理、发现器生命周期管理。
//! 与数据面分离：控制面做决策，数据面无状态执行。
//!
//! 职责：
//! - 管理发现器注册表（注册/移除/启用/禁用）
//! - 配置热更新（订阅 ConfigChanged 事件）
//! - 调度策略（根据 infohash 选择最优发现器组合）
//! - 协调聚合器和爬虫引擎
//! - 健康检查调度

pub mod policy;

use std::sync::Arc;

use parking_lot::RwLock;
use tracing::info;

use crate::config::PdcConfig;
use crate::discoverers::dht::DhtDiscoverer;
use crate::discoverers::DiscovererRegistry;
use crate::event_bus::EventBus;
use crate::traits::PeerDiscoverer;

/// 控制面
///
/// 持有全局状态的引用，协调各模块工作。
/// 克隆成本低（内部都是 Arc），可以安全共享。
#[derive(Clone)]
pub struct ControlPlane {
    /// 全局配置
    config: Arc<RwLock<PdcConfig>>,
    /// 发现器注册表
    registry: Arc<DiscovererRegistry>,
    /// 事件总线
    event_bus: EventBus,
    /// TrackerRepo（可选，注入到 TrackerDiscoverer 实现统一数据归口）
    tracker_repo: Option<Arc<crate::storage::TrackerRepoImpl>>,
    /// 主爬虫 DHT 发现器（共享给联邦 DHT 发现复用路由表）
    dht_discoverer: Option<Arc<DhtDiscoverer>>,
}

impl ControlPlane {
    /// 创建控制面
    pub fn new(config: PdcConfig, registry: Arc<DiscovererRegistry>, event_bus: EventBus) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            registry,
            event_bus,
            tracker_repo: None,
            dht_discoverer: None,
        }
    }

    /// 注入 TrackerRepo（统一数据归口 + 持久化）
    pub fn with_tracker_repo(mut self, repo: Arc<crate::storage::TrackerRepoImpl>) -> Self {
        self.tracker_repo = Some(repo);
        self
    }

    /// 获取配置快照
    pub fn config(&self) -> PdcConfig {
        self.config.read().clone()
    }

    /// 更新配置（热更新）
    ///
    /// 更新后发布 ConfigChanged 事件，所有订阅者会收到通知。
    pub fn update_config(&self, new_config: PdcConfig) {
        let old = self.config.read().clone();
        *self.config.write() = new_config;
        info!("[control_plane] 配置已更新");
        self.event_bus.publish(crate::types::Event::ConfigChanged);
        // 这里可以对比 old 和 new，做差异化处理（如启用/禁用发现器）
        let _ = old;
    }

    /// 获取发现器注册表引用
    pub fn registry(&self) -> Arc<DiscovererRegistry> {
        self.registry.clone()
    }

    /// 获取事件总线引用
    pub fn event_bus(&self) -> EventBus {
        self.event_bus.clone()
    }

    /// 注册发现器
    pub fn register_discoverer(&self, discoverer: Box<dyn PeerDiscoverer>) {
        self.registry.register(discoverer);
    }

    /// 移除发现器
    pub fn unregister_discoverer(&self, name: &str) {
        self.registry.unregister(name);
    }

    /// 根据配置初始化默认发现器
    ///
    /// 根据配置中的 enable_tracker/enable_dht/enable_pex 等开关，
    /// 创建并注册对应的发现器。
    pub fn init_default_discoverers(&mut self) {
        let config = self.config();

        if config.discoverers.enable_tracker {
            let tracker = if config.discoverers.custom_trackers.is_empty() {
                crate::discoverers::tracker::TrackerDiscoverer::with_default_config()
            } else {
                crate::discoverers::tracker::TrackerDiscoverer::with_trackers(
                    config.discoverers.custom_trackers.clone(),
                )
            };
            // 注入 TrackerRepo（统一数据归口 + 持久化）
            let tracker = if let Some(ref repo) = self.tracker_repo {
                tracker.with_tracker_repo(repo.clone())
            } else {
                tracker
            };
            self.registry.register(Box::new(tracker));
        }

        if config.discoverers.enable_dht {
            let dht_config = crate::discoverers::dht::DhtConfig {
                listen_port: config.discoverers.dht_listen_port,
                ..Default::default()
            };
            let dht = Arc::new(crate::discoverers::dht::DhtDiscoverer::new(dht_config));
            self.dht_discoverer = Some(dht.clone());
            self.registry.register(Box::new((*dht).clone()));
        }

        if config.discoverers.enable_pex {
            self.registry.register(Box::new(
                crate::discoverers::pex::PexDiscoverer::with_default_config(),
            ));
        }

        if config.discoverers.enable_lpd {
            let lpd_config = crate::discoverers::lpd::LpdConfig {
                listen_port: config.discoverers.dht_listen_port,
                multicast_port: config.discoverers.lpd_multicast_port,
                ..Default::default()
            };
            self.registry
                .register(Box::new(crate::discoverers::lpd::LpdDiscoverer::new(
                    lpd_config,
                )));
        }

        if config.discoverers.enable_webseed {
            self.registry.register(Box::new(
                crate::discoverers::webseed::WebSeedDiscoverer::with_default_config(),
            ));
        }

        info!(
            "[control_plane] 默认发现器初始化完成，共注册 {} 个",
            self.registry.len()
        );
    }

    /// 获取调度策略
    /// 获取主爬虫 DHT 发现器（共享给联邦 DHT 发现复用路由表）
    pub fn dht_discoverer(&self) -> Option<Arc<DhtDiscoverer>> {
        self.dht_discoverer.clone()
    }

    pub fn policy(&self) -> policy::DiscoveryPolicy {
        let config = self.config();
        policy::DiscoveryPolicy::from_config(&config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_control_plane_creation() {
        let config = PdcConfig::default();
        let registry = Arc::new(DiscovererRegistry::new());
        let bus = EventBus::default();
        let cp = ControlPlane::new(config, registry, bus);
        assert_eq!(cp.registry().len(), 0);
    }

    #[test]
    fn test_config_update() {
        let config = PdcConfig::default();
        let registry = Arc::new(DiscovererRegistry::new());
        let bus = EventBus::default();
        let cp = ControlPlane::new(config, registry, bus);

        let mut new_config = PdcConfig::default();
        new_config.server.port = 9999;
        cp.update_config(new_config);

        assert_eq!(cp.config().server.port, 9999);
    }

    #[test]
    fn test_init_default_discoverers() {
        let config = PdcConfig::default();
        let registry = Arc::new(DiscovererRegistry::new());
        let bus = EventBus::default();
        let cp = ControlPlane::new(config, registry, bus);
        cp.init_default_discoverers();
        // 默认启用 tracker + dht + pex = 3 个
        assert_eq!(cp.registry().len(), 3);
    }
}
