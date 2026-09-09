//! 外部 Tracker 主动 Scrape 服务
//!
//! 对热门 infohash 主动向公共 Tracker 发送 scrape 请求，获取外部视角的
//! seeders/leechers/downloaded 统计。这是 InfohashScorer 健康度维度的
//! 重要数据源（中等覆盖率），弥补 PDC 超级 Tracker 使用率低的问题。
//!
//! 【设计】
//! - 支持 HTTP Tracker（BEP 3/48）和 UDP Tracker（BEP 15）两种协议
//! - 结果缓存：每个 infohash 的 scrape 结果缓存 5 分钟
//! - 限流：每个 Tracker 每秒最多 1 个请求
//! - 失败重试：连续失败 3 次的 Tracker 暂时禁用 5 分钟

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;
use reqwest::Client;
use serde_bencode::from_bytes;
use serde_bencode::value::Value as BencodeValue;
use tracing::{debug, info, warn};
use url::Url;

use crate::storage::repo_traits::{TrackerRepository, TrackerEntry};
use crate::types::{Infohash, ScrapeEntry};

/// 单个 infohash 的 scrape 结果（融合多个 Tracker 的结果）
#[derive(Debug, Clone)]
pub struct ScrapeResult {
    /// 做种者数（取所有 Tracker 中的最大值）
    pub complete: i64,
    /// 下载者数（取所有 Tracker 中的最大值）
    pub incomplete: i64,
    /// 总下载完成数（取所有 Tracker 中的最大值）
    pub downloaded: i64,
    /// 数据来源 Tracker 数量
    pub source_count: u32,
    /// 最后更新时间
    pub last_updated: Instant,
}

impl Default for ScrapeResult {
    fn default() -> Self {
        Self {
            complete: 0,
            incomplete: 0,
            downloaded: 0,
            source_count: 0,
            last_updated: Instant::now(),
        }
    }
}

/// Tracker 限流状态
struct TrackerRateLimit {
    /// 最后请求时间
    last_request: RwLock<Instant>,
    /// 连续失败次数
    consecutive_failures: RwLock<u32>,
    /// 禁用截止时间
    disabled_until: RwLock<Option<Instant>>,
}

impl TrackerRateLimit {
    fn new() -> Self {
        Self {
            last_request: RwLock::new(Instant::now() - Duration::from_secs(3600)),
            consecutive_failures: RwLock::new(0),
            disabled_until: RwLock::new(None),
        }
    }

    /// 是否可以发送请求（限流+禁用检查）
    fn can_request(&self, min_interval: Duration) -> bool {
        // 检查是否被禁用
        if let Some(until) = *self.disabled_until.read() {
            if Instant::now() < until {
                return false;
            }
        }
        // 检查限流
        Instant::now().duration_since(*self.last_request.read()) >= min_interval
    }

    /// 记录请求成功
    fn record_success(&self) {
        *self.last_request.write() = Instant::now();
        *self.consecutive_failures.write() = 0;
    }

    /// 记录请求失败
    fn record_failure(&self, max_failures: u32, cooldown: Duration) {
        *self.last_request.write() = Instant::now();
        let mut failures = self.consecutive_failures.write();
        *failures += 1;
        if *failures >= max_failures {
            *self.disabled_until.write() = Some(Instant::now() + cooldown);
            *failures = 0;
        }
    }
}

/// 外部 Tracker Scrape 服务
pub struct ScrapeService {
    /// HTTP 客户端
    http_client: Client,
    /// infohash -> scrape 结果缓存
    cache: DashMap<Infohash, ScrapeResult>,
    /// Tracker URL -> 限流状态
    rate_limits: DashMap<String, Arc<TrackerRateLimit>>,
    /// TrackerRepo（获取活跃 Tracker 列表）
    tracker_repo: Option<Arc<dyn TrackerRepository>>,
    /// 缓存有效期（默认 300 秒=5分钟）
    cache_ttl_secs: u64,
    /// 每个 Tracker 的最小请求间隔（默认 1 秒）
    min_request_interval: Duration,
    /// 最大连续失败次数（默认 3）
    max_consecutive_failures: u32,
    /// 失败后禁用时间（默认 300 秒=5分钟）
    cooldown_duration: Duration,
    /// 单次 scrape 最多查询的 Tracker 数量（默认 5）
    max_trackers_per_scrape: usize,
    /// 请求超时（默认 10 秒）
    request_timeout: Duration,
}

impl ScrapeService {
    /// 创建新的 Scrape 服务
    pub fn new() -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("PeerDiscoveryCenter/1.0")
            .build()
            .unwrap_or_default();

        Self {
            http_client,
            cache: DashMap::new(),
            rate_limits: DashMap::new(),
            tracker_repo: None,
            cache_ttl_secs: 300,
            min_request_interval: Duration::from_secs(1),
            max_consecutive_failures: 3,
            cooldown_duration: Duration::from_secs(300),
            max_trackers_per_scrape: 5,
            request_timeout: Duration::from_secs(10),
        }
    }

    /// 注入 TrackerRepo
    pub fn with_tracker_repo(mut self, repo: Arc<dyn TrackerRepository>) -> Self {
        self.tracker_repo = Some(repo);
        self
    }

    /// 获取某个 infohash 的 scrape 结果（优先用缓存）
    pub fn get_scrape_result(&self, infohash: &Infohash) -> Option<ScrapeResult> {
        let result = self.cache.get(infohash)?;
        // 检查缓存是否过期
        if result.last_updated.elapsed() > Duration::from_secs(self.cache_ttl_secs) {
            return None; // 缓存过期，需要重新 scrape
        }
        Some(result.clone())
    }

    /// 主动 scrape 某个 infohash（向多个公共 Tracker 查询）
    ///
    /// 返回融合后的 scrape 结果
    pub async fn scrape_infohash(&self, infohash: Infohash) -> ScrapeResult {
        // 先检查缓存
        if let Some(cached) = self.get_scrape_result(&infohash) {
            debug!("[scrape_service] 缓存命中: {}", hex::encode(&infohash[..8]));
            return cached;
        }

        // 获取活跃 Tracker 列表
        let trackers = self.get_active_trackers().await;
        if trackers.is_empty() {
            warn!("[scrape_service] 没有可用的 Tracker");
            return ScrapeResult::default();
        }

        // 向多个 Tracker 并发 scrape
        let mut merged = ScrapeResult::default();
        let mut tasks = Vec::new();

        for tracker_url in trackers.iter().take(self.max_trackers_per_scrape) {
            let rate_limit = self.get_or_create_rate_limit(tracker_url);
            if !rate_limit.can_request(self.min_request_interval) {
                continue;
            }

            let infohash = infohash;
            let tracker_url = tracker_url.clone();
            let http_client = self.http_client.clone();
            let timeout = self.request_timeout;

            tasks.push(tokio::spawn(async move {
                let result = scrape_single_tracker(&http_client, &tracker_url, &infohash, timeout).await;
                (tracker_url, result)
            }));
        }

        // 等待所有任务完成
        for task in tasks {
            if let Ok((tracker_url, result)) = task.await {
                let rate_limit = self.get_or_create_rate_limit(&tracker_url);
                match result {
                    Ok(entry) => {
                        rate_limit.record_success();
                        merged.complete = merged.complete.max(entry.complete);
                        merged.incomplete = merged.incomplete.max(entry.incomplete);
                        merged.downloaded = merged.downloaded.max(entry.downloaded);
                        merged.source_count += 1;
                        debug!(
                            "[scrape_service] {} scrape成功: complete={}, incomplete={}",
                            tracker_url, entry.complete, entry.incomplete
                        );
                    }
                    Err(e) => {
                        rate_limit.record_failure(self.max_consecutive_failures, self.cooldown_duration);
                        debug!("[scrape_service] {} scrape失败: {}", tracker_url, e);
                    }
                }
            }
        }

        merged.last_updated = Instant::now();

        // 写入缓存
        if merged.source_count > 0 {
            self.cache.insert(infohash, merged.clone());
        }

        merged
    }

    /// 批量 scrape 多个 infohash
    pub async fn scrape_infohashes(&self, infohashes: &[Infohash]) -> HashMap<Infohash, ScrapeResult> {
        let mut results = HashMap::new();
        for &infohash in infohashes {
            let result = self.scrape_infohash(infohash).await;
            results.insert(infohash, result);
        }
        results
    }

    /// 获取活跃 Tracker 列表
    async fn get_active_trackers(&self) -> Vec<String> {
        if let Some(repo) = &self.tracker_repo {
            let active = repo.active_trackers().await;
            active.into_iter().map(|t| t.url).collect()
        } else {
            // 没有 TrackerRepo 时使用内置公共 Tracker
            crate::discoverers::tracker::PUBLIC_TRACKERS
                .iter()
                .take(self.max_trackers_per_scrape)
                .map(|s| s.to_string())
                .collect()
        }
    }

    /// 获取或创建 Tracker 的限流状态
    fn get_or_create_rate_limit(&self, url: &str) -> Arc<TrackerRateLimit> {
        self.rate_limits
            .entry(url.to_string())
            .or_insert_with(|| Arc::new(TrackerRateLimit::new()))
            .clone()
    }

    /// 清理过期缓存
    pub fn cleanup_expired_cache(&self) -> usize {
        let cutoff = Instant::now() - Duration::from_secs(self.cache_ttl_secs * 2);
        let mut removed = 0;
        self.cache.retain(|_, v| {
            if v.last_updated < cutoff {
                removed += 1;
                false
            } else {
                true
            }
        });
        removed
    }

    /// 获取缓存中的 infohash 数量
    pub fn cached_count(&self) -> usize {
        self.cache.len()
    }
}

impl Default for ScrapeService {
    fn default() -> Self {
        Self::new()
    }
}

/// 向单个 Tracker 发送 scrape 请求
async fn scrape_single_tracker(
    client: &Client,
    tracker_url: &str,
    infohash: &Infohash,
    timeout: Duration,
) -> Result<ScrapeEntry, String> {
    // 判断协议类型
    if tracker_url.starts_with("http://") || tracker_url.starts_with("https://") {
        scrape_http_tracker(client, tracker_url, infohash, timeout).await
    } else if tracker_url.starts_with("udp://") {
        // UDP Tracker scrape 暂不支持（需要单独实现 UDP 协议）
        Err("UDP Tracker scrape not supported yet".to_string())
    } else {
        Err(format!("Unknown tracker protocol: {}", tracker_url))
    }
}

/// 向 HTTP Tracker 发送 scrape 请求
async fn scrape_http_tracker(
    client: &Client,
    tracker_url: &str,
    infohash: &Infohash,
    timeout: Duration,
) -> Result<ScrapeEntry, String> {
    // 构造 scrape URL：将 announce 替换为 scrape
    let scrape_url = if tracker_url.contains("/announce") {
        tracker_url.replace("/announce", "/scrape")
    } else {
        // 如果 URL 不含 announce，直接在末尾加 /scrape
        format!("{}/scrape", tracker_url.trim_end_matches('/'))
    };

    // 构造查询参数：info_hash 需要 URL 编码原始字节
    let info_hash_encoded = urlencode_infohash(infohash);
    let full_url = format!("{}?info_hash={}", scrape_url, info_hash_encoded);

    debug!("[scrape_service] HTTP scrape: {}", full_url);

    // 发送请求
    let response = client
        .get(&full_url)
        .timeout(timeout)
        .send()
        .await
        .map_err(|e| format!("HTTP request failed: {}", e))?;

    if !response.status().is_success() {
        return Err(format!("HTTP status: {}", response.status()));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|e| format!("Failed to read response: {}", e))?;

    // 解析 bencode 响应
    parse_scrape_response(&bytes, infohash)
}

/// URL 编码 infohash（原始字节，非 UTF-8）
fn urlencode_infohash(infohash: &Infohash) -> String {
    let mut encoded = String::with_capacity(60);
    for &byte in infohash.iter() {
        // 未保留字符直接输出，其他用 %XX 编码
        if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' || byte == b'.' || byte == b'~' {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{:02X}", byte));
        }
    }
    encoded
}

/// 解析 scrape 响应（bencode 格式）
fn parse_scrape_response(bytes: &[u8], infohash: &Infohash) -> Result<ScrapeEntry, String> {
    let value: BencodeValue = from_bytes(bytes).map_err(|e| format!("Bencode parse error: {}", e))?;

    // 响应格式：d5:filesd20:<infohash>d8:completeiN e10:incompleteiN e10:downloadediN eee
    let dict = match value {
        BencodeValue::Dict(d) => d,
        _ => return Err("Response is not a dict".to_string()),
    };

    // 检查 failure reason
    if let Some(BencodeValue::Bytes(reason)) = dict.get(b"failure reason".as_ref()) {
        return Err(format!("Tracker failure: {}", String::from_utf8_lossy(reason)));
    }

    // 获取 files 字典
    let files = match dict.get(b"files".as_ref()) {
        Some(BencodeValue::Dict(f)) => f,
        _ => return Err("No files in response".to_string()),
    };

    // 查找目标 infohash
    let entry_dict = match files.get(infohash.as_ref()) {
        Some(BencodeValue::Dict(d)) => d,
        _ => return Err("Infohash not found in scrape response".to_string()),
    };

    // 解析 complete（做种者）
    let complete = match entry_dict.get(b"complete".as_ref()) {
        Some(BencodeValue::Int(n)) => *n,
        _ => 0,
    };

    // 解析 incomplete（下载者）
    let incomplete = match entry_dict.get(b"incomplete".as_ref()) {
        Some(BencodeValue::Int(n)) => *n,
        _ => 0,
    };

    // 解析 downloaded（总下载完成数）
    let downloaded = match entry_dict.get(b"downloaded".as_ref()) {
        Some(BencodeValue::Int(n)) => *n,
        _ => 0,
    };

    Ok(ScrapeEntry {
        complete,
        downloaded,
        incomplete,
        name: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_urlencode_infohash() {
        let infohash = [0u8; 20];
        let encoded = urlencode_infohash(&infohash);
        // 全0应该全部编码为 %00
        assert_eq!(encoded, "%00%00%00%00%00%00%00%00%00%00%00%00%00%00%00%00%00%00%00%00");

        // 测试可打印字符
        let mut infohash2 = [0u8; 20];
        infohash2[0] = b'A';
        infohash2[1] = b'B';
        infohash2[2] = b'1';
        let encoded2 = urlencode_infohash(&infohash2);
        assert!(encoded2.starts_with("AB1"));
    }

    #[test]
    fn test_parse_scrape_response() {
        // 测试无效的 bencode 数据解析
        let bencode = b"invalid bencode data";
        let infohash = [b'A'; 20];
        let result = parse_scrape_response(bencode, &infohash);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_scrape_response_failure() {
        // "test error" 是10个字符
        let bencode = b"d14:failure reason10:test errore";
        let infohash = [b'A'; 20];
        let result = parse_scrape_response(bencode, &infohash);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("test error"));
    }

    #[test]
    fn test_scrape_result_default() {
        let result = ScrapeResult::default();
        assert_eq!(result.complete, 0);
        assert_eq!(result.incomplete, 0);
        assert_eq!(result.downloaded, 0);
        assert_eq!(result.source_count, 0);
    }

    #[test]
    fn test_rate_limit() {
        let rl = TrackerRateLimit::new();

        // 初始状态应该可以请求
        assert!(rl.can_request(Duration::from_secs(1)));

        // 记录成功后，应该被限流
        rl.record_success();
        assert!(!rl.can_request(Duration::from_secs(10)));

        // 记录失败
        rl.record_failure(3, Duration::from_secs(300));
        rl.record_failure(3, Duration::from_secs(300));
        rl.record_failure(3, Duration::from_secs(300));
        // 第3次失败后应该被禁用
        assert!(!rl.can_request(Duration::from_secs(0)));
    }
}
