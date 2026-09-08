//! DHT 关键词搜索服务（BEP 44 Mutable Data）
//!
//! 通过 BEP 44 mutable data 协议，在 DHT 网络上搜索包含 infohash 的关键词数据。
//!
//! BEP 44 协议概述：
//! - 发布者用 ed25519 私钥签名，把可变数据存储到 DHT
//! - 数据通过 (public_key, salt) 唯一定位
//! - 搜索者用相同的 (public_key, salt) 查询 DHT，获取发布的数据
//!
//! 关键词搜索约定：
//! - salt 格式: "keyword:<关键词>"
//! - 数据 (v 字段) 格式: JSON 或 bencode，包含 infohash 列表
//! - 本服务定期查询预设的 (public_key, salt) 组合，提取 infohash
//!
//! 注意：当前为基础框架，未实现 ed25519 签名验证，仅提取数据字段。
//! 后续应添加签名验证以防止恶意数据注入。

use crate::storage::InfohashRepoImpl;
use crate::types::Infohash;
use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tracing::{info, warn};

/// 关键词搜索配置
#[derive(Debug, Clone)]
pub struct KeywordSearchConfig {
    /// 要搜索的 (public_key_hex, salt) 组合列表
    /// public_key 是 32 字节 ed25519 公钥的 hex 编码（64字符）
    pub targets: Vec<(String, String)>,
    /// 搜索间隔（秒）
    pub interval_secs: u64,
    /// 每次查询的 DHT 节点数
    pub nodes_per_query: usize,
}

impl Default for KeywordSearchConfig {
    fn default() -> Self {
        Self {
            targets: vec![],
            interval_secs: 1800, // 默认30分钟
            nodes_per_query: 8,
        }
    }
}

/// BEP 44 可变数据响应
#[derive(Debug, Clone)]
pub struct MutableDataResponse {
    pub public_key: Vec<u8>,
    pub salt: Vec<u8>,
    pub seq: i64,
    pub value: Vec<u8>, // v 字段的原始字节
    pub signature: Vec<u8>,
}

/// DHT 关键词搜索服务
pub struct KeywordSearchService {
    config: KeywordSearchConfig,
    infohash_repo: Option<Arc<InfohashRepoImpl>>,
    node_addrs: Vec<SocketAddr>, // 用于查询的 DHT 节点地址列表
}

impl KeywordSearchService {
    /// 创建新的关键词搜索服务
    pub fn new(config: KeywordSearchConfig) -> Self {
        Self {
            config,
            infohash_repo: None,
            node_addrs: vec![],
        }
    }

    /// 设置 InfohashRepo
    pub fn with_infohash_repo(mut self, repo: Arc<InfohashRepoImpl>) -> Self {
        self.infohash_repo = Some(repo);
        self
    }

    /// 设置用于查询的 DHT 节点地址列表
    pub fn with_node_addrs(mut self, addrs: Vec<SocketAddr>) -> Self {
        self.node_addrs = addrs;
        self
    }

    /// 启动关键词搜索服务（定期搜索）
    pub async fn start(self: Arc<Self>) {
        if self.config.targets.is_empty() {
            info!("[keyword_search] 未配置搜索目标，跳过");
            return;
        }

        if self.node_addrs.is_empty() {
            warn!("[keyword_search] 未配置 DHT 节点地址，跳过");
            return;
        }

        info!(
            "[keyword_search] 启动关键词搜索服务: {} 个目标, {} 个 DHT 节点, 间隔 {}s",
            self.config.targets.len(),
            self.node_addrs.len(),
            self.config.interval_secs
        );

        // 启动时立即搜索一次
        self.search_all().await;

        let mut interval =
            tokio::time::interval(Duration::from_secs(self.config.interval_secs));

        loop {
            interval.tick().await;
            self.search_all().await;
        }
    }

    /// 搜索所有目标
    pub async fn search_all(&self) {
        let mut total_infohashes = 0;

        for (public_key_hex, salt) in &self.config.targets {
            match self.search_target(public_key_hex, salt).await {
                Ok(count) => {
                    total_infohashes += count;
                    info!(
                        "[keyword_search] 目标 pk={} salt={} 提取 {} 个 infohash",
                        &public_key_hex[..8.min(public_key_hex.len())],
                        salt,
                        count
                    );
                }
                Err(e) => {
                    warn!(
                        "[keyword_search] 目标 pk={} salt={} 搜索失败: {}",
                        &public_key_hex[..8.min(public_key_hex.len())],
                        salt,
                        e
                    );
                }
            }
        }

        if total_infohashes > 0 {
            info!("[keyword_search] 本轮共提取 {} 个 infohash", total_infohashes);
        }
    }

    /// 搜索单个目标，返回提取的 infohash 数量
    async fn search_target(&self, public_key_hex: &str, salt: &str) -> Result<usize> {
        let public_key = hex::decode(public_key_hex)?;
        if public_key.len() != 32 {
            return Err(anyhow::anyhow!("public_key 长度应为32字节，实际{}", public_key.len()));
        }

        // 向多个 DHT 节点发送 BEP 44 get 请求
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        let mut all_values = vec![];

        for addr in self.node_addrs.iter().take(self.config.nodes_per_query) {
            if let Ok(value) = self.query_mutable(&socket, *addr, &public_key, salt.as_bytes()).await {
                all_values.push(value);
            }
        }

        // 从所有响应中提取 infohash
        let mut infohashes = vec![];
        for value in &all_values {
            let extracted = extract_infohashes_from_value(value);
            for ih in extracted {
                if !infohashes.contains(&ih) {
                    infohashes.push(ih);
                }
            }
        }

        // 注册到 InfohashRepo
        if let Some(repo) = &self.infohash_repo {
            for ih in &infohashes {
                repo.register_sync(*ih, "dht_keyword_search");
            }
        }

        Ok(infohashes.len())
    }

    /// 向单个 DHT 节点发送 BEP 44 get 请求，返回 value 字段
    async fn query_mutable(
        &self,
        socket: &UdpSocket,
        addr: SocketAddr,
        public_key: &[u8],
        salt: &[u8],
    ) -> Result<Vec<u8>> {
        let tid = rand::random::<[u8; 2]>();
        let request = build_bep44_get_request(&tid, public_key, salt);

        socket.send_to(&request, addr).await?;

        let mut buf = [0u8; 2048];
        let (len, _) = tokio::time::timeout(Duration::from_secs(5), socket.recv_from(&mut buf)).await??;

        let value = parse_bep44_get_response(&buf[..len])?;
        Ok(value)
    }
}

/// 构建 BEP 44 get 请求
/// 格式: d1:ad2:id20:<node_id>3:key32:<public_key>4:salt<len>:<salt>e1:q3:get1:t2:<tid>1:y1:qe
fn build_bep44_get_request(tid: &[u8; 2], public_key: &[u8], salt: &[u8]) -> Vec<u8> {
    let node_id = [0u8; 20]; // 用全零 node_id，实际应使用本地节点 ID
    let mut buf = Vec::new();

    buf.extend_from_slice(b"d1:ad2:id20:");
    buf.extend_from_slice(&node_id);
    buf.extend_from_slice(b"3:key32:");
    buf.extend_from_slice(public_key);
    buf.extend_from_slice(&format!("4:salt{}:", salt.len()).into_bytes());
    buf.extend_from_slice(salt);
    buf.extend_from_slice(b"e1:q3:get1:t2:");
    buf.extend_from_slice(tid);
    buf.extend_from_slice(b"1:y1:qe");

    buf
}

/// 解析 BEP 44 get 响应，返回 value (v) 字段
fn parse_bep44_get_response(data: &[u8]) -> Result<Vec<u8>> {
    use serde_bencode::value::Value;

    let value: Value = serde_bencode::from_bytes(data)?;
    let dict = match value {
        Value::Dict(d) => d,
        _ => return Err(anyhow::anyhow!("响应不是字典")),
    };

    let r = dict.get(&b"r".to_vec())
        .ok_or_else(|| anyhow::anyhow!("响应缺少 r 字段"))?;
    let r_dict = match r {
        Value::Dict(d) => d,
        _ => return Err(anyhow::anyhow!("r 字段不是字典")),
    };

    let v = r_dict.get(&b"v".to_vec())
        .ok_or_else(|| anyhow::anyhow!("响应缺少 v 字段"))?;
    match v {
        Value::Bytes(b) => Ok(b.clone()),
        _ => Err(anyhow::anyhow!("v 字段不是字节串")),
    }
}

/// 从 BEP 44 value 字段中提取 infohash
/// 支持的格式：
/// - 原始 20 字节 infohash（连续出现）
/// - 40 字符 hex 编码的 infohash
/// - JSON/bencode 中的 infohash 字段
fn extract_infohashes_from_value(value: &[u8]) -> Vec<Infohash> {
    let mut infohashes = vec![];

    // 尝试1：从原始字节中查找 20 字节的 infohash（通过查找 40 字符 hex 间接判断）
    // 直接扫描 20 字节窗口太不可靠，优先用 hex 格式

    // 尝试2：查找 40 字符 hex 编码的 infohash
    if let Ok(text) = std::str::from_utf8(value) {
        let mut i = 0;
        while i + 40 <= text.len() {
            let candidate = &text[i..i + 40];
            if candidate.chars().all(|c| c.is_ascii_hexdigit()) {
                let mut ih = [0u8; 20];
                if hex::decode_to_slice(candidate, &mut ih).is_ok() {
                    if !infohashes.contains(&ih) {
                        infohashes.push(ih);
                    }
                }
            }
            i += 1;
        }
    }

    infohashes
}
