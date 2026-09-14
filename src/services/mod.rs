//! 业务层 Service
//!
//! 6 个 Service：Crawler/Discover/Tracker/Probe/Pex/Nat
//! 只调 Repo + Scorer，不碰底层数据结构。

pub mod crawler_service;
pub mod discover_service;
pub mod keyword_search_service;
pub mod metadata_service;
pub mod nat_service;
pub mod pex_service;
pub mod probe_service;
pub mod scrape_service;
pub mod subscription_service;
pub mod tracker_fetcher;
pub mod tracker_service;

pub use crawler_service::CrawlerService;
pub use discover_service::DiscoverService;
pub use keyword_search_service::{KeywordSearchConfig, KeywordSearchService};
pub use metadata_service::{MetadataService, TorrentMetadata};
pub use nat_service::NatService;
pub use pex_service::PexService;
pub use probe_service::ProbeService;
pub use scrape_service::{ScrapeResult, ScrapeService};
pub use subscription_service::{SubscriptionConfig, SubscriptionService};
pub use tracker_fetcher::TrackerPeerFetcher;
pub use tracker_service::TrackerService;
