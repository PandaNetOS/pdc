//! 爬虫引擎实现
//!
//! 实现 DHT 被动监听爬虫：
//! 1. 加入 DHT 网络（通过 bootstrap 节点）
//! 2. 监听其他节点发来的 get_peers / announce_peer 消息
//! 3. 从中提取 infohash，去重后发布到事件总线
//! 4. 对查询发送响应，维持在 DHT 网络中的存在感

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::RwLock;
use rand::Rng;
use tokio::net::UdpSocket;
use tracing::{debug, info, warn};

use crate::config::CrawlerConfig;
use crate::discoverers::dht::message::{DhtMessage, QueryMethod};
use crate::event_bus::EventBus;
use crate::types::{Event, Infohash};

use super::Crawler;

/// 爬虫运行状态
#[derive(Debug, Clone, Default)]
pub struct CrawlerState {
    /// 是否在运行
    pub running: bool,
    /// 已爬行的节点数
    pub nodes_crawled: u64,
    /// 已收集的 infohash 数
    pub infohashes_collected: u64,
    /// 已收集的 peer 数
    pub peers_collected: u64,
    /// 爬行开始时间
    pub started_at: Option<Instant>,
    /// 最后一次爬行时间
    pub last_crawl_at: Option<Instant>,
    /// 错误数
    pub errors: u64,
    /// 收到的消息总数
    pub messages_received: u64,
}

/// 爬虫引擎
///
/// 被动监听 DHT 网络，收集 infohash。
pub struct CrawlerEngine {
    config: CrawlerConfig,
    state: Arc<RwLock<CrawlerState>>,
    event_bus: EventBus,
    shutdown: Arc<tokio::sync::Notify>,
    /// 已收集的 infohash（去重）
    seen_infohashes: Arc<RwLock<HashSet<Infohash>>>,
    /// 我们的节点 ID
    node_id: [u8; 20],
}

impl CrawlerEngine {
    /// 创建新的爬虫引擎
    pub fn new(config: CrawlerConfig, event_bus: EventBus) -> Self {
        let mut node_id = [0u8; 20];
        rand::thread_rng().fill(&mut node_id);

        Self {
            config,
            state: Arc::new(RwLock::new(CrawlerState::default())),
            event_bus,
            shutdown: Arc::new(tokio::sync::Notify::new()),
            seen_infohashes: Arc::new(RwLock::new(HashSet::new())),
            node_id,
        }
    }

    /// 获取状态快照
    pub fn state(&self) -> CrawlerState {
        self.state.read().clone()
    }

    /// 加入 DHT 网络：向 bootstrap 节点发送 ping
    async fn bootstrap(&self, socket: &UdpSocket) -> usize {
        let mut success = 0;
        for (host, port) in &self.config.bootstrap_nodes {
            let addr_str = format!("{}:{}", host, port);
            if let Ok(addrs) = tokio::net::lookup_host(addr_str.clone()).await {
                for addr in addrs {
                    let tid = rand::thread_rng().gen::<[u8; 2]>();
                    let ping = DhtMessage::build_ping(&tid, &self.node_id);
                    if socket.send_to(&ping, addr).await.is_ok() {
                        success += 1;
                        debug!("[crawler] 向 bootstrap {} 发送 ping", addr);
                    }
                }
            }
        }
        success
    }

    /// 处理收到的 DHT 消息
    ///
    /// 返回新发现的 infohash（如果有）
    async fn handle_message(
        &self,
        socket: &UdpSocket,
        data: &[u8],
        from: SocketAddr,
    ) -> Option<Infohash> {
        // 更新消息计数
        {
            let mut state = self.state.write();
            state.messages_received += 1;
        }

        // 解析查询消息
        let (tid, method, infohash) = DhtMessage::parse_query(data)?;

        match method {
            QueryMethod::Ping => {
                // 响应 ping
                let resp = DhtMessage::build_ping_response(&tid, &self.node_id);
                let _ = socket.send_to(&resp, from).await;
                debug!("[crawler] 响应 {} 的 ping", from);
            }
            QueryMethod::FindNode => {
                // 响应 find_node（空节点列表）
                let resp = DhtMessage::build_find_node_response(&tid, &self.node_id);
                let _ = socket.send_to(&resp, from).await;
                debug!("[crawler] 响应 {} 的 find_node", from);
            }
            QueryMethod::GetPeers => {
                // 响应 get_peers（空节点列表）
                let token = rand::thread_rng().gen::<[u8; 4]>();
                let resp = DhtMessage::build_get_peers_response(&tid, &self.node_id, &token);
                let _ = socket.send_to(&resp, from).await;
                debug!("[crawler] 响应 {} 的 get_peers", from);

                if let Some(ih) = infohash {
                    return Some(ih);
                }
            }
            QueryMethod::AnnouncePeer => {
                // announce_peer 不需要响应（或响应空）
                debug!("[crawler] 收到 {} 的 announce_peer", from);

                if let Some(ih) = infohash {
                    return Some(ih);
                }
            }
        }

        None
    }

    /// 爬行循环：被动监听 DHT 消息
    async fn crawl_loop(&self) {
        info!(
            "[crawler] DHT 爬虫启动，监听端口 {}",
            self.config.listen_port
        );

        // 创建 UDP socket
        let socket = match UdpSocket::bind(format!("0.0.0.0:{}", self.config.listen_port)).await {
            Ok(s) => s,
            Err(e) => {
                warn!("[crawler] 绑定端口 {} 失败: {}", self.config.listen_port, e);
                let mut state = self.state.write();
                state.running = false;
                state.errors += 1;
                return;
            }
        };

        // 加入 DHT 网络
        let bootstrap_count = self.bootstrap(&socket).await;
        info!(
            "[crawler] 向 {} 个 bootstrap 地址发送了 ping",
            bootstrap_count
        );

        let mut buf = vec![0u8; 4096];
        let mut last_bootstrap = Instant::now();
        let bootstrap_interval = Duration::from_secs(300); // 每 5 分钟重新 bootstrap

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => {
                    info!("[crawler] 爬虫引擎收到停止信号");
                    break;
                }
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((n, from)) => {
                            let data = &buf[..n];
                            if let Some(infohash) = self.handle_message(&socket, data, from).await {
                                // 检查是否是新 infohash
                                let is_new = {
                                    let mut seen = self.seen_infohashes.write();
                                    seen.insert(infohash)
                                };

                                if is_new {
                                    let hex_ih = hex::encode(infohash);
                                    debug!("[crawler] 发现新 infohash: {} (来自 {})", hex_ih, from);

                                    // 更新状态
                                    {
                                        let mut state = self.state.write();
                                        state.infohashes_collected += 1;
                                        state.last_crawl_at = Some(Instant::now());
                                    }

                                    // 发布事件
                                    self.event_bus.publish(Event::InfohashSeen {
                                        infohash,
                                        source: "dht-crawler".to_string(),
                                        seen_at: std::time::SystemTime::now(),
                                    });

                                    // 定期发布爬行进度
                                    let state = self.state.read();
                                    if state.infohashes_collected.is_multiple_of(100) {
                                        self.event_bus.publish(Event::CrawlProgress {
                                            nodes_crawled: state.messages_received,
                                            infohashes_collected: state.infohashes_collected,
                                            peers_collected: 0,
                                            message: format!("已收集 {} 个 infohash", state.infohashes_collected),
                                        });
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            debug!("[crawler] 接收消息失败: {}", e);
                            let mut state = self.state.write();
                            state.errors += 1;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    // 定期重新 bootstrap，保持在 DHT 网络中
                    if last_bootstrap.elapsed() >= bootstrap_interval {
                        let count = self.bootstrap(&socket).await;
                        debug!("[crawler] 定期 bootstrap，向 {} 个地址发送 ping", count);
                        last_bootstrap = Instant::now();
                    }
                }
            }
        }

        info!("[crawler] 爬虫引擎已停止");
    }
}

#[async_trait]
impl Crawler for CrawlerEngine {
    fn name(&self) -> &str {
        "dht-crawler"
    }

    async fn start(&self) -> anyhow::Result<()> {
        if !self.config.enabled {
            warn!("[crawler] 爬虫未启用（config.crawler.enabled = false）");
            return Ok(());
        }

        {
            let mut state = self.state.write();
            if state.running {
                warn!("[crawler] 爬虫已在运行");
                return Ok(());
            }
            state.running = true;
            state.started_at = Some(Instant::now());
        }

        let engine = self.clone_for_async();
        tokio::spawn(async move {
            engine.crawl_loop().await;
        });

        info!("[crawler] 爬虫引擎已启动");
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        {
            let mut state = self.state.write();
            if !state.running {
                return Ok(());
            }
            state.running = false;
        }
        self.shutdown.notify_waiters();
        info!("[crawler] 爬虫引擎停止信号已发送");
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.state.read().running
    }

    fn state(&self) -> CrawlerState {
        self.state()
    }
}

impl CrawlerEngine {
    /// 克隆一个可用于 async 任务的引用
    fn clone_for_async(&self) -> CrawlerEngine {
        CrawlerEngine {
            config: self.config.clone(),
            state: self.state.clone(),
            event_bus: self.event_bus.clone(),
            shutdown: self.shutdown.clone(),
            seen_infohashes: self.seen_infohashes.clone(),
            node_id: self.node_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crawler_state_default() {
        let state = CrawlerState::default();
        assert!(!state.running);
        assert_eq!(state.nodes_crawled, 0);
        assert_eq!(state.infohashes_collected, 0);
    }

    #[test]
    fn test_crawler_engine_creation() {
        let config = CrawlerConfig::default();
        let bus = EventBus::default();
        let engine = CrawlerEngine::new(config, bus);
        assert_eq!(engine.name(), "dht-crawler");
        assert!(!engine.is_running());
    }

    #[tokio::test]
    async fn test_crawler_start_disabled() {
        let config = CrawlerConfig {
            enabled: false,
            ..Default::default()
        };
        let bus = EventBus::default();
        let engine = CrawlerEngine::new(config, bus);
        let result = engine.start().await;
        assert!(result.is_ok());
        assert!(!engine.is_running());
    }

    #[test]
    fn test_parse_query_ping() {
        let tid = [0x01, 0x02];
        let node_id = [0u8; 20];
        let msg = DhtMessage::build_ping(&tid, &node_id);
        let (parsed_tid, method, ih) = DhtMessage::parse_query(&msg).unwrap();
        assert_eq!(method, QueryMethod::Ping);
        assert_eq!(parsed_tid, vec![0x01, 0x02]);
        assert!(ih.is_none());
    }

    #[test]
    fn test_parse_query_get_peers() {
        let tid = [0x01, 0x02];
        let node_id = [0u8; 20];
        let infohash = [1u8; 20];
        let msg = DhtMessage::build_get_peers(&tid, &node_id, &infohash);
        let (_, method, ih) = DhtMessage::parse_query(&msg).unwrap();
        assert_eq!(method, QueryMethod::GetPeers);
        assert_eq!(ih, Some(infohash));
    }

    #[test]
    fn test_build_ping_response() {
        let tid = vec![0x01, 0x02];
        let node_id = [0u8; 20];
        let resp = DhtMessage::build_ping_response(&tid, &node_id);
        let s = String::from_utf8_lossy(&resp);
        assert!(s.contains("1:y1:r"));
    }
}
