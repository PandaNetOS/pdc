//! WebSocket 实时推送
//!
//! 提供 /ws 端点，客户端可订阅事件类型，实时接收推送。
//! 所有消息统一格式: {"event_type": "...", "data": {...}, "timestamp": ...}

use std::collections::HashSet;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Extension, State};
use axum::response::Response;
use futures::{SinkExt, StreamExt};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::data_plane::AppState;
use crate::event_bus::EventBus;
use crate::types::Event;

/// 默认 uTP 监听端口（状态上报用）
const DEFAULT_UTP_REPORT_PORT: u16 = 6883;
/// 默认 TCP PEX 监听端口（状态上报用）
const DEFAULT_TCP_PEX_REPORT_PORT: u16 = 6884;
/// WebSocket 连接心跳间隔
const WS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
/// WebSocket 状态推送间隔
const WS_STATUS_PUSH_INTERVAL: Duration = Duration::from_secs(5);

/// status 组装超时上限：超时则回退上次成功快照，绝不让组装拖死 WS task。
/// 快照化后组装仅为 serde JSON 构造（微秒级），500ms 是极宽松的兜底。
const STATUS_ASSEMBLE_TIMEOUT: Duration = Duration::from_millis(500);

/// 最近一次成功组装的 status data（用于组装超时/join 失败时兜底）。
///
/// 绝不因组装异常断 WS：任何一次 tick 组装失败都从这里取上一份好数据。
/// parking_lot RwLock，读多写少，写仅在每次成功组装后发生。
/// 用 OnceLock 惰性初始化（兼容较旧工具链，不依赖 once_cell / Lazy）。
static LAST_STATUS_DATA: std::sync::OnceLock<parking_lot::RwLock<Option<serde_json::Value>>> =
    std::sync::OnceLock::new();

fn last_status_cache() -> &'static parking_lot::RwLock<Option<serde_json::Value>> {
    LAST_STATUS_DATA.get_or_init(|| parking_lot::RwLock::new(None))
}

/// 读取上次成功的 status data（兜底用）；无历史时返回占位。
fn last_status_fallback() -> serde_json::Value {
    last_status_cache()
        .read()
        .clone()
        .unwrap_or_else(|| serde_json::json!({"warning": "status assembly unavailable"}))
}

/// 记录一份成功组装的 status data（供后续 tick 兜底）。
fn remember_status_data(v: &serde_json::Value) {
    *last_status_cache().write() = Some(v.clone());
}

/// 客户端订阅请求
#[derive(Debug, serde::Deserialize)]
struct SubscribeRequest {
    #[serde(default = "default_subscribe")]
    subscribe: Vec<String>,
}

fn default_subscribe() -> Vec<String> {
    vec!["all".to_string()]
}

/// 处理 WebSocket 升级请求
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Extension(event_bus): Extension<EventBus>,
    State(state): State<AppState>,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, event_bus, state))
}

/// 构建统一格式的 JSON 消息
fn build_message(event_type: &str, data: serde_json::Value) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    serde_json::json!({
        "event_type": event_type,
        "data": data,
        "timestamp": ts
    })
    .to_string()
}

/// 将 Event 转换为 JSON Value（手动构建，避免序列化问题）
fn event_to_json(event: &Event) -> Option<(String, serde_json::Value)> {
    match event {
        Event::PeerDiscovered {
            infohash,
            peers,
            source,
        } => {
            let ih_hex = hex::encode(infohash);
            let peers_json: Vec<serde_json::Value> = peers
                .iter()
                .map(|p| {
                    serde_json::json!({
                        "addr": p.addr.to_string(),
                        "peer_id": p.peer_id.as_ref().map(hex::encode),
                    })
                })
                .collect();
            Some((
                "peer_discovered".to_string(),
                serde_json::json!({
                    "infohash": ih_hex,
                    "peers": peers_json,
                    "source": source,
                }),
            ))
        }
        Event::InfohashSeen {
            infohash,
            source,
            seen_at,
        } => {
            let ih_hex = hex::encode(infohash);
            let ts = seen_at
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            Some((
                "infohash_seen".to_string(),
                serde_json::json!({
                    "infohash": ih_hex,
                    "source": source,
                    "seen_at": ts,
                }),
            ))
        }
        Event::NodeHealthUpdate {
            discoverer,
            healthy,
            message,
        } => Some((
            "node_health_update".to_string(),
            serde_json::json!({
                "discoverer": discoverer,
                "healthy": healthy,
                "message": message,
            }),
        )),
        Event::AnnounceRequest {
            infohash,
            peer_addr,
            peer_id,
            event,
            uploaded,
            downloaded,
            left,
        } => {
            let ih_hex = hex::encode(infohash);
            Some((
                "announce_request".to_string(),
                serde_json::json!({
                    "infohash": ih_hex,
                    "peer_addr": peer_addr.to_string(),
                    "peer_id": peer_id.as_ref().map(hex::encode),
                    "event": format!("{:?}", event),
                    "uploaded": uploaded,
                    "downloaded": downloaded,
                    "left": left,
                }),
            ))
        }
        Event::PeerQualityScore {
            addr,
            score,
            reason,
        } => Some((
            "peer_quality_score".to_string(),
            serde_json::json!({
                "addr": addr.to_string(),
                "score": score,
                "reason": reason,
            }),
        )),
        Event::CrawlProgress {
            nodes_crawled,
            infohashes_collected,
            peers_collected,
            message,
        } => Some((
            "crawl_progress".to_string(),
            serde_json::json!({
                "nodes_crawled": nodes_crawled,
                "infohashes_collected": infohashes_collected,
                "peers_collected": peers_collected,
                "message": message,
            }),
        )),
        Event::ConfigChanged => Some(("config_changed".to_string(), serde_json::json!({}))),
    }
}

/// 收集状态快照（pub：WS 推送与 REST `/api/v1/status` 共用）。
///
/// D4/E8 健壮化口径（对齐 rest_api 用户 WIP「API/WS 线程零 DB 接触」风格）：
/// - 健康分不再 `HealthScorerImpl::calculate().await` 同步遍历全 Repo，
///   改由后台 `stats_snapshot` 纯计算 `health_report_from_snapshot`；
/// - 近 1h 活跃 peer 数不再 `all_peers_sync()` O(N) 克隆，改 `active_1h_count()` O(1) 原子读；
/// - 各 repo 冷热 tier 不再现场 `cache_stats()+total_count_sync()`（后者是同步 SQLite COUNT），
///   直接取后台快照已算好的 `*_repo_metrics.tier`；
/// - tracker 列表不再 `all_trackers_sync()` O(M) 克隆，取快照 `tracker_scores`；
/// - 仅把最终 `serde_json::json!` 大对象构造（纯 CPU/分配、不持锁不碰 DB）放进
///   `spawn_blocking`，不撑开 api_runtime worker。
///
/// 组装成功后写入 [`LAST_STATUS_DATA`] 供 ticker 兜底；join 失败时回退上次成功快照。
pub async fn collect_status(state: &AppState) -> serde_json::Value {
    // ===== 廉价读取（async worker 上：无 DB、无全量克隆、无跨 await 持锁）=====
    let snap = state.stats_snapshot.get();
    let health_report = crate::data_plane::stats_snapshot::health_report_from_snapshot(&snap);
    let health_status = crate::health_check::SystemHealth::from_score(health_report.overall);

    // 近 1h 活跃 peer：O(1) 原子读（替代 all_peers_sync() 全量克隆过滤）
    let active_peers = state.peer_repo.active_1h_count();
    let (cache_infohashes, cache_peers) = state.peer_repo.stats();

    // NodeStats：O(1) 原子计数器组装（stats_sync 已改造为无锁）
    let node_stats = state.node_repo.as_ref().map(|r| r.stats_sync());

    // NAT / 联邦状态：小结构体克隆（mappings 通常个位数）
    let nat_status = state.nat.status();
    let fed_status = state.federation.as_ref().map(|f| f.status());

    // crawler_state 短读（parking_lot RwLock read，作用域内 drop，不跨 await）
    let crawler_json = state
        .crawler_state
        .as_ref()
        .map(|cs| {
            let s = cs.read();
            serde_json::json!({
                "enabled": true,
                "running": s.running,
                "known_nodes": s.known_nodes,
                "messages_received": s.messages_received,
                "requests_sent": s.requests_sent,
                "errors": s.errors,
                "infohashes_collected": s.infohashes_collected,
                "peers_collected": s.peers_collected,
                "nodes_crawled": s.nodes_crawled,
                "adaptive_multiplier": s.adaptive_multiplier,
                "predicted_response_rate": s.predicted_response_rate,
                "model_update_count": s.model_update_count,
                "history_len": s.history_len,
                "concurrent_sockets_in_use": s.concurrent_sockets_in_use,
            })
        })
        .unwrap_or_else(|| serde_json::json!({"enabled": false}));

    // 其余原子/短读
    let super_tracker_ih = state.super_tracker.infohash_count();
    let super_tracker_peers = state.super_tracker.peer_count();
    let rate_qps = state.rate_limiter.current_qps();
    let rate_total = state.rate_limiter.stats().total_requests;
    let rate_blocked = state.rate_limiter.stats().blocked_requests;
    let rate_banned = state.rate_limiter.banned_count();

    let fetcher_stats = state.fetcher.as_ref().map(|f| {
        (
            f.infohash_count(),
            f.total_rounds.load(std::sync::atomic::Ordering::Relaxed),
            f.total_peers_fetched
                .load(std::sync::atomic::Ordering::Relaxed),
            f.last_round_peers
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    });
    let dht_probe_stats = state.dht_probe.as_ref().map(|p| {
        (
            p.total_probed.load(std::sync::atomic::Ordering::Relaxed),
            p.total_success.load(std::sync::atomic::Ordering::Relaxed),
            p.total_added.load(std::sync::atomic::Ordering::Relaxed),
            p.probed.read().len(),
        )
    });
    let utp_stats = state.utp_server.as_ref().map(|s| s.stats());
    let pex_stats = state.pex_receiver.as_ref().map(|r| r.stats());
    let tcp_pex_stats = state.tcp_pex_server.as_ref().map(|s| s.stats());
    let active_pex_stats = state.active_pex.as_ref().map(|s| s.stats());

    // ===== JSON 组装移入 blocking 线程（纯 CPU/分配，不持锁不碰 DB）=====
    let assembled = tokio::task::spawn_blocking(move || {
        // RepoTierStats -> {total, hot, warm, cold}（面板 tier 数据）
        let tier_json = |t: &crate::data_plane::rest_api::RepoTierStats| {
            serde_json::json!({
                "total": t.total,
                "hot": t.hot,
                "warm": t.warm,
                "cold": t.cold,
            })
        };
        let peer_tier = snap.peer_repo_metrics.as_ref().map(|m| tier_json(&m.tier));
        let infohash_tier = snap
            .infohash_repo_metrics
            .as_ref()
            .map(|m| tier_json(&m.tier));
        let node_tier = snap.node_repo_metrics.as_ref().map(|m| tier_json(&m.tier));
        let tracker_tier = snap
            .tracker_repo_metrics
            .as_ref()
            .map(|m| tier_json(&m.tier));

        let tracker_scores: Vec<serde_json::Value> = snap
            .tracker_scores
            .iter()
            .map(|t| {
                serde_json::json!({
                    "url": t.url,
                    "score": t.score,
                    "disabled": t.disabled,
                })
            })
            .collect();

        // NAT 映射分类计数
        let tcp_mapped = nat_status
            .mappings
            .iter()
            .filter(|m| m.protocol == "TCP")
            .count();
        let udp_mapped = nat_status
            .mappings
            .iter()
            .filter(|m| m.protocol == "UDP")
            .count();
        let tcp_verified = nat_status
            .mappings
            .iter()
            .filter(|m| m.protocol == "TCP" && m.verified)
            .count();
        let udp_verified = nat_status
            .mappings
            .iter()
            .filter(|m| m.protocol == "UDP" && m.verified)
            .count();
        let udp_reachable = nat_status
            .mappings
            .iter()
            .filter(|m| m.protocol == "UDP" && m.reachable)
            .count();

        serde_json::json!({
            "health": {
                "overall_score": health_report.overall,
                "status": format!("{:?}", health_status),
                "tracker_layer_score": health_report.tracker_layer,
                "dht_layer_score": health_report.dht_layer,
                "peer_layer_score": health_report.peer_layer,
                "active_trackers": health_report.active_trackers,
                "total_trackers": health_report.total_trackers,
                "avg_tracker_score": health_report.avg_tracker_score,
            },
            "crawler": crawler_json,
            "peer_repo": {
                "infohashes": cache_infohashes,
                "peers": cache_peers,
                "active_peers": active_peers,
                "tier": peer_tier,
            },
            "super_tracker": {
                "infohashes": super_tracker_ih,
                "peers": super_tracker_peers,
            },
            "rate_limiter": {
                "qps": rate_qps,
                "total_requests": rate_total,
                "blocked_requests": rate_blocked,
                "banned_ips": rate_banned,
            },
            "tracker_scores": tracker_scores,
            // InfohashRepo（total 取后台快照，避免 API/WS 线程打 DB COUNT）
            "infohash_repo": {
                "count": snap.infohash_repo_metrics.as_ref().map(|m| m.tier.total).unwrap_or(0),
                "tier": infohash_tier,
            },
            // TrackerRepo
            "tracker_repo": {
                "tier": tracker_tier,
            },
            // NodeRepo（stats_sync = O(1) 原子读；tier 取后台快照）
            "node_repo": match &node_stats {
                Some(ns) => serde_json::json!({
                    "total": ns.total,
                    "active": ns.active,
                    "avg_score": ns.avg_score,
                    "bad": ns.bad,
                    "questionable": ns.questionable,
                    "good": ns.good,
                    "tier": node_tier,
                }),
                None => serde_json::json!({"total": 0, "active": 0}),
            },
            // NAT/UPnP
            "nat": {
                "enabled": nat_status.enabled,
                "gateway_found": nat_status.gateway_found,
                "gateway_healthy": nat_status.gateway_healthy,
                "gateway_addr": nat_status.gateway_addr,
                "external_ip": nat_status.external_ip,
                "local_ip": nat_status.local_ip,
                "nat_type": nat_status.nat_type.as_str(),
                "tcp_mapped": tcp_mapped,
                "udp_mapped": udp_mapped,
                "tcp_verified": tcp_verified,
                "udp_verified": udp_verified,
                "udp_reachable": udp_reachable,
                "total_mappings": nat_status.mappings.len(),
                "reachability_score": nat_status.reachability_score,
                "metrics": nat_status.metrics,
            },
            // TrackerPeerFetcher
            "fetcher": match fetcher_stats {
                Some((ih_count, rounds, fetched, last_round)) => serde_json::json!({
                    "infohash_repo_count": ih_count,
                    "total_rounds": rounds,
                    "total_peers_fetched": fetched,
                    "last_round_peers": last_round,
                }),
                None => serde_json::json!({"enabled": false}),
            },
            // DHT 探测器
            "dht_probe": match dht_probe_stats {
                Some((probed, success, added, cache_len)) => serde_json::json!({
                    "total_probed": probed,
                    "total_success": success,
                    "total_added": added,
                    "probed_cache": cache_len,
                }),
                None => serde_json::json!({"enabled": false}),
            },
            // uTP 服务端（BEP 29）
            "utp_server": match &utp_stats {
                Some(s) => serde_json::json!({
                    "enabled": true,
                    "port": DEFAULT_UTP_REPORT_PORT,
                    "syn_received": s.syn_received,
                    "connections_established": s.connections_established,
                    "handshakes_received": s.handshakes_received,
                    "peers_extracted": s.peers_extracted,
                    "active_connections": s.active_connections,
                    "connections_rejected": s.connections_rejected,
                    "connections_evicted": s.connections_evicted,
                    "max_connections": 100,
                }),
                None => serde_json::json!({"enabled": false}),
            },
            // PEX 接收器（BEP 11）
            "pex_receiver": match &pex_stats {
                Some(r) => serde_json::json!({
                    "enabled": true,
                    "extension_handshakes": r.extension_handshakes,
                    "pex_supported": r.pex_supported,
                    "pex_messages": r.pex_messages,
                    "peers_extracted": r.peers_extracted,
                    "ipv4_peers": r.ipv4_peers,
                    "ipv6_peers": r.ipv6_peers,
                    "utp_peers": r.utp_peers,
                    "holepunch_peers": r.holepunch_peers,
                    "dedup_skipped": r.dedup_skipped,
                    "parse_errors": r.parse_errors,
                }),
                None => serde_json::json!({"enabled": false}),
            },
            // TCP PEX 服务端（BEP 11，端口 6884）
            "tcp_pex": match &tcp_pex_stats {
                Some(s) => serde_json::json!({
                    "enabled": true,
                    "port": DEFAULT_TCP_PEX_REPORT_PORT,
                    "connections_accepted": s.connections_accepted,
                    "handshakes_completed": s.handshakes_completed,
                    "pex_messages": s.pex_messages,
                    "peers_extracted": s.peers_extracted,
                    "active_connections": s.active_connections,
                }),
                None => serde_json::json!({"enabled": false}),
            },
            // 主动 PEX 请求器（BEP 11）
            "active_pex": match &active_pex_stats {
                Some(s) => serde_json::json!({
                    "enabled": true,
                    "connection_attempts": s.connection_attempts,
                    "connections_succeeded": s.connections_succeeded,
                    "connections_failed": s.connections_failed,
                    "handshakes_completed": s.handshakes_completed,
                    "extension_handshakes": s.extension_handshakes,
                    "pex_supported": s.pex_supported,
                    "pex_messages": s.pex_messages,
                    "peers_extracted": s.peers_extracted,
                    "timeouts": s.timeouts,
                    "errors": s.errors,
                }),
                None => serde_json::json!({"enabled": false}),
            },
            // 联邦网络状态
            "federation": match &fed_status {
                Some(st) => serde_json::json!({
                    "enabled": st.enabled,
                    "node_id": st.node_id,
                    "connections": st.connections,
                    "known_nodes": st.known_nodes,
                    "reachability": st.reachability,
                    "uptime_secs": st.uptime_secs,
                    "gossip_queue_size": st.gossip_queue_size,
                    "relay_channels": st.relay_channels,
                }),
                None => serde_json::json!({"enabled": false}),
            },
        })
    })
    .await;

    match assembled {
        Ok(value) => {
            remember_status_data(&value);
            value
        }
        Err(e) => {
            warn!(
                "[ws] collect_status 组装线程 join 失败: {}，回退上次成功快照",
                e
            );
            last_status_fallback()
        }
    }
}

/// 带超时兜底的 status data 采集：超时/失败一律回退上次成功快照，绝不 panic/断 WS。
async fn collect_status_guarded(state: &AppState) -> serde_json::Value {
    match tokio::time::timeout(STATUS_ASSEMBLE_TIMEOUT, collect_status(state)).await {
        Ok(v) => v,
        Err(_elapsed) => {
            warn!(
                "[ws] status 组装超过 {:?}，回退上次成功快照",
                STATUS_ASSEMBLE_TIMEOUT
            );
            last_status_fallback()
        }
    }
}

/// 处理单个 WebSocket 连接
async fn handle_socket(socket: WebSocket, event_bus: EventBus, state: AppState) {
    let (mut sender, mut receiver) = socket.split();

    let mut subscriptions: HashSet<String> = HashSet::new();
    subscriptions.insert("all".to_string());

    let mut rx = event_bus.subscribe();
    info!(
        "[ws] 新客户端连接，订阅者总数: {}",
        event_bus.subscriber_count()
    );

    // 发送欢迎消息
    let welcome = build_message(
        "welcome",
        serde_json::json!({
            "message": "Connected to PDC WebSocket API",
            "subscriptions": ["all"]
        }),
    );
    if sender.send(Message::Text(welcome)).await.is_err() {
        warn!("[ws] 发送欢迎消息失败");
        return;
    }

    // 立即推送一次状态（带超时兜底：组装失败用上一次成功快照，绝不因首推失败断连）
    let status_json = build_message("status", collect_status_guarded(&state).await);
    let _ = sender.send(Message::Text(status_json)).await;

    // [ALLOWED-INTERVAL] 连接级心跳，随 WebSocket 连接生命周期
    let mut heartbeat = tokio::time::interval(WS_HEARTBEAT_INTERVAL);
    heartbeat.tick().await;
    // [ALLOWED-INTERVAL] 连接级状态推送，随 WebSocket 连接生命周期
    // 状态推送间隔 5 秒（避免每秒克隆 60000+ 节点造成大量内存分配和磁盘 IO）
    let mut status_ticker = tokio::time::interval(WS_STATUS_PUSH_INTERVAL);
    status_ticker.tick().await;

    loop {
        tokio::select! {
            // 接收客户端消息
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(req) = serde_json::from_str::<SubscribeRequest>(&text) {
                            subscriptions.clear();
                            for sub in &req.subscribe {
                                subscriptions.insert(sub.to_lowercase());
                            }
                            debug!("[ws] 客户端更新订阅: {:?}", subscriptions);
                            let ack = build_message(
                                "subscribed",
                                serde_json::json!({"subscriptions": req.subscribe}),
                            );
                            let _ = sender.send(Message::Text(ack)).await;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        let _ = sender.send(Message::Pong(payload)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        info!("[ws] 客户端断开连接");
                        break;
                    }
                    _ => {}
                }
            }

            // 接收事件总线事件
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        if let Some((event_type, data)) = event_to_json(&event) {
                            if subscriptions.contains("all") || subscriptions.contains(&event_type) {
                                let json = build_message(&event_type, data);
                                if sender.send(Message::Text(json)).await.is_err() {
                                    debug!("[ws] 发送事件失败，客户端可能已断开");
                                    break;
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("[ws] 事件滞后，丢弃 {} 个事件", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }

            // 周期推送状态（5s）：组装走 spawn_blocking + 超时兜底，异常回退上次成功快照，
            // 绝不因组装失败 break 循环断 WS；只有真正 send 失败（客户端断开）才退出。
            _ = status_ticker.tick() => {
                let data = collect_status_guarded(&state).await;
                let json = build_message("status", data);
                if sender.send(Message::Text(json)).await.is_err() {
                    debug!("[ws] 发送状态失败，客户端断开");
                    break;
                }
            }

            // 心跳
            _ = heartbeat.tick() => {
                if sender.send(Message::Ping(vec![])).await.is_err() {
                    debug!("[ws] 心跳失败，客户端断开");
                    break;
                }
            }
        }
    }
}
