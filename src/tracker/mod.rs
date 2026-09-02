//! Tracker 发现机制
//!
//! 通过 BitTorrent Tracker 协议（HTTP/UDP）发现 peer。
//!
//! 支持：
//! - HTTP/HTTPS Tracker
//! - UDP Tracker（更高效）
//! - 多 Tracker 并发请求
//! - 自动重试和故障转移
//! - Tracker 健康检查和自动淘汰

pub mod client;

pub use client::{TrackerConfig, TrackerDiscoverer};

/// 常用公共 Tracker 列表
pub const PUBLIC_TRACKERS: &[&str] = &[
    // HTTP/HTTPS Tracker
    "http://tracker1.itzmx.com:8080/announce",
    "http://tracker2.itzmx.com:6961/announce",
    "http://tracker3.itzmx.com:6961/announce",
    "http://tracker4.itzmx.com:2710/announce",
    "http://tracker.opentrackr.org:1337/announce",
    "http://open.acgtracker.com:1096/announce",
    "http://tracker.dler.com:6969/announce",
    "http://tracker.dler.org:6969/announce",
    "http://tracker.edkj.club:6969/announce",
    "http://tracker.files.fm:6969/announce",
    "http://tracker.gbitt.info/announce",
    "http://tracker.k.vu:6969/announce",
    "http://tracker.mywaifu.best:6969/announce",
    "http://tracker.qu.ax:6969/announce",
    "http://tracker.renfei.net:8080/announce",
    "http://wepzone.net:6969/announce",
    "http://www.all4nothin.net/announce.php",
    "http://www.peckservers.com:9000/announce",
    "http://www.wareztorrent.com/announce",
    // HTTPS Tracker
    "https://1337.abcvg.info/announce",
    "https://pybittrack.retiolus.net/announce",
    "https://t.peer-exchange.download/announce",
    "https://tr.burnabyhighstar.com/announce",
    "https://tracker.cloudit.top/announce",
    "https://tracker.gbitt.info/announce",
    "https://tracker.gcrenwp.top/announce",
    "https://tracker.ipfsscan.io/announce",
    "https://tracker.kuroy.me/announce",
    "https://tracker.lilithraws.org/announce",
    "https://tracker.loligirl.cn/announce",
    "https://tracker.pmman.tech/announce",
    "https://tracker.renfei.net/announce",
    "https://tracker.tamersunion.org/announce",
    "https://tracker.yemekyedim.com/announce",
    "https://tracker1.520.jp/announce",
    "https://trackers.mlsub.net/announce",
    "https://trackers.run/announce",
    "https://www.peckservers.com:9443/announce",
    // UDP Tracker（速度更快，连接数更多）
    "udp://tracker1.itzmx.com:8080/announce",
    "udp://tracker2.itzmx.com:6961/announce",
    "udp://tracker3.itzmx.com:6961/announce",
    "udp://tracker4.itzmx.com:2710/announce",
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://open.demonii.si:80/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.dler.org:6969/announce",
    "udp://tracker.filemail.com:6969/announce",
    "udp://tracker.moeking.me:6969/announce",
    "udp://tracker.srv00.com:6969/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://explodie.org:6969/announce",
    "udp://tracker.cyberia.is:6969/announce",
    "udp://retracker.hotplug.ru:2710/announce",
    "udp://tracker.birkenwald.de:6969/announce",
    "udp://tracker.bittor.pw:1337/announce",
    "udp://tracker.ccp.ovh:6969/announce",
    "udp://tracker.darkness.services:6969/announce",
    "udp://tracker.ddunlimited.net:6969/announce",
    "udp://tracker.deadorbit.nl:6969/announce",
    "udp://tracker.draatman.uk:6969/announce",
    "udp://tracker.dump.cl:6969/announce",
    "udp://tracker.farted.net:6969/announce",
    "udp://tracker.fnix.net:6969/announce",
    "udp://tracker.gmi.gd:6969/announce",
    "udp://tracker.jamesthebard.net:6969/announce",
    "udp://tracker.qu.ax:6969/announce",
    "udp://tracker.silksa.co.za:6969/announce",
    "udp://tracker.skyts.net:6969/announce",
    "udp://tracker.tryhackx.org:6969/announce",
    "udp://tracker1.myporn.club:9337/announce",
    "udp://tracker2.dler.org:80/announce",
    "udp://ttk2.nbaonlineservice.com:6969/announce",
    "udp://u.peer-exchange.download:6969/announce",
    "udp://wepzone.net:6969/announce",
    "udp://www.torrent.eu.org:451/announce",
    "udp://z.mercax.com:53/announce",
];
