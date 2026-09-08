//! 爬虫引擎
//!
//! 主动爬行 DHT 网络，收集 infohash 和 peer 信息。
//! 与被动查询（数据面）互补，构成双引擎架构。
//!
//! 注意：当前为接口定义 + 空实现，实际 DHT 爬行逻辑待后续填充。

pub mod active_pex;
pub mod engine;
pub mod pex_receiver;
pub mod tcp_pex_server;
pub mod utp_server;

pub use crate::config::CrawlerConfig;
pub use engine::{CrawlerEngine, CrawlerState};

use async_trait::async_trait;

/// 爬虫引擎 trait
///
/// 所有爬虫实现必须实现此 trait。
/// 可以有多种实现：DHT 爬虫、Tracker 爬虫、PEX 爬虫等。
#[async_trait]
pub trait Crawler: Send + Sync {
    /// 爬虫名称
    fn name(&self) -> &str;

    /// 启动爬虫
    async fn start(&self) -> anyhow::Result<()>;

    /// 停止爬虫
    async fn stop(&self) -> anyhow::Result<()>;

    /// 是否在运行
    fn is_running(&self) -> bool;

    /// 获取爬行状态
    fn state(&self) -> CrawlerState;
}
