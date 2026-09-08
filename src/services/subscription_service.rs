//! 外部订阅源服务
//!
//! 定期拉取 RSS/Atom 订阅源，解析 torrent/magnet 链接，提取 infohash 注册到 InfohashRepo。
//! 支持的来源：
//! - RSS/Atom 中的 magnet 链接（xt=urn:btih:XXX）
//! - RSS/Atom 中的 .torrent 文件链接（需下载解析，暂不支持）

use crate::storage::InfohashRepoImpl;
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// 订阅源配置
#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    /// RSS/Atom 源 URL 列表
    pub feeds: Vec<String>,
    /// 拉取间隔（秒）
    pub interval_secs: u64,
    /// 每个源每次最多提取的 infohash 数
    pub max_per_feed: usize,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            feeds: vec![],
            interval_secs: 3600, // 默认1小时
            max_per_feed: 50,
        }
    }
}

/// 外部订阅源服务
pub struct SubscriptionService {
    config: SubscriptionConfig,
    infohash_repo: Option<Arc<InfohashRepoImpl>>,
    client: reqwest::Client,
}

impl SubscriptionService {
    /// 创建新的订阅源服务
    pub fn new(config: SubscriptionConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("PandaNetOS-PDC/0.2.0")
            .build()
            .expect("failed to build reqwest client");

        Self {
            config,
            infohash_repo: None,
            client,
        }
    }

    /// 设置 InfohashRepo
    pub fn with_infohash_repo(mut self, repo: Arc<InfohashRepoImpl>) -> Self {
        self.infohash_repo = Some(repo);
        self
    }

    /// 启动订阅源服务（定期拉取）
    pub async fn start(self: Arc<Self>) {
        if self.config.feeds.is_empty() {
            info!("[subscription] 未配置订阅源，跳过");
            return;
        }

        info!(
            "[subscription] 启动订阅源服务: {} 个源, 间隔 {}s",
            self.config.feeds.len(),
            self.config.interval_secs
        );

        // 启动时立即拉取一次
        self.fetch_all().await;

        let mut interval =
            tokio::time::interval(Duration::from_secs(self.config.interval_secs));

        loop {
            interval.tick().await;
            self.fetch_all().await;
        }
    }

    /// 拉取所有订阅源
    pub async fn fetch_all(&self) {
        let mut total_infohashes = 0;

        for feed_url in &self.config.feeds {
            match self.fetch_feed(feed_url).await {
                Ok(count) => {
                    total_infohashes += count;
                    info!("[subscription] 源 {} 提取 {} 个 infohash", feed_url, count);
                }
                Err(e) => {
                    warn!("[subscription] 源 {} 拉取失败: {}", feed_url, e);
                }
            }
        }

        if total_infohashes > 0 {
            info!("[subscription] 本轮共提取 {} 个 infohash", total_infohashes);
        }
    }

    /// 拉取单个订阅源，返回提取的 infohash 数量
    async fn fetch_feed(&self, feed_url: &str) -> Result<usize> {
        let resp = self.client.get(feed_url).send().await?;
        let body = resp.text().await?;

        // 从 RSS/Atom 内容中提取所有 magnet 链接的 infohash
        // magnet 链接格式: xt=urn:btih:XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        let mut infohashes = vec![];
        let marker = "xt=urn:btih:";
        let mut search_start = 0;
        while let Some(pos) = body[search_start..].find(marker) {
            let abs_pos = search_start + pos;
            let ih_start = abs_pos + marker.len();
            if ih_start + 40 <= body.len() {
                let ih_hex = &body[ih_start..ih_start + 40];
                let mut ih = [0u8; 20];
                if hex::decode_to_slice(ih_hex, &mut ih).is_ok() {
                    if !infohashes.contains(&ih) {
                        infohashes.push(ih);
                    }
                }
            }
            search_start = ih_start + 40;
            if infohashes.len() >= self.config.max_per_feed {
                break;
            }
        }

        // 注册到 InfohashRepo
        if let Some(repo) = &self.infohash_repo {
            for ih in &infohashes {
                repo.register_sync(*ih, "subscription");
            }
        }

        Ok(infohashes.len())
    }
}
