//! WebSocket 实时推送
//!
//! 提供 /ws 端点，客户端可订阅事件类型，实时接收推送。
//! 所有消息统一格式: {"event_type": "...", "data": {...}, "timestamp": ...}

use std::collections::HashSet;
use std::time::Duration;

use crate::intelligence::scorer_traits::HealthScorer;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Extension, State};
use axum::response::Response;
use futures::{SinkExt, StreamExt};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::data_plane::AppState;
use crate::event_bus::EventBus;
use crate::types::Event;

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
        Event::PeerDiscovered { infohash, peers, source } => {
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
        Event::InfohashSeen { infohash, source, seen_at } => {
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
        Event::NodeHealthUpdate { discoverer, healthy, message } => Some((
            "node_health_update".to_string(),
            serde_json::json!({
                "discoverer": discoverer,
                "healthy": healthy,
                "message": message,
            }),
        )),
        Event::AnnounceRequest { infohash, peer_addr, peer_id, event, uploaded, downloaded, left } => {
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
        Event::PeerQualityScore { addr, score, reason } => Some((
            "peer_quality_score".to_string(),
            serde_json::json!({
                "addr": addr.to_string(),
                "score": score,
                "reason": reason,
            }),
        )),
        Event::CrawlProgress { nodes_crawled, infohashes_collected, peers_collected, message } => Some((
            "crawl_progress".to_string(),
            serde_json::json!({
                "nodes_crawled": nodes_crawled,
                "infohashes_collected": infohashes_collected,
                "peers_collected": peers_collected,
                "message": message,
            }),
        )),
        Event::ConfigChanged => Some((
            "config_changed".to_string(),
            serde_json::json!({}),
        )),
    }
}

/// 收集状态快照
async fn collect_status(state: &AppState) -> serde_json::Value {
    let registry = state.control_plane.registry();
    let cache_stats = state.peer_repo.stats();
    // 计算活跃 peer 数（最近1小时内有活跃的 peer）
    let active_peers = {
        let all = state.peer_repo.all_peers_sync();
        let now = std::time::SystemTime::now();
        all.iter().filter(|p| {
            now.duration_since(p.last_active)
                .map(|d| d.as_secs() < 3600)
                .unwrap_or(false)
        }).count()
    };
    // 从 crawler_state 获取入站连通性评分
    let inbound_score = state.crawler_state.as_ref().map(|cs| {
        let s = cs.read();
        let uptime = s.started_at.map(|t| t.elapsed().as_secs()).unwrap_or(0);
        let inbound_per_min = if uptime > 0 {
            (s.inbound_total as f64 / uptime as f64) * 60.0
        } else {
            0.0
        };
        if inbound_per_min >= 10.0 { 100.0 }
        else if inbound_per_min >= 5.0 { 80.0 }
        else if inbound_per_min >= 1.0 { 60.0 }
        else if inbound_per_min >= 0.1 { 40.0 }
        else if inbound_per_min > 0.0 { 20.0 }
        else { 0.0 }
    });

    // 使用 HealthScorerImpl 统一计算健康度（唯一计算路径，评分统一收口）
    let health_scorer = crate::intelligence::health_scorer::HealthScorerImpl::new();
    let health = if let (Some(node_repo), Some(tracker_repo), Some(infohash_repo)) =
        (state.node_repo.as_ref(), state.tracker_repo.as_ref(), state.infohash_repo.as_ref())
    {
        let report = health_scorer.calculate(
            tracker_repo.as_ref() as &dyn crate::storage::repo_traits::TrackerRepository,
            node_repo.as_ref() as &dyn crate::storage::repo_traits::NodeRepository,
            state.peer_repo.as_ref() as &dyn crate::storage::repo_traits::PeerRepository,
            infohash_repo.as_ref() as &dyn crate::storage::repo_traits::InfohashRepository,
        ).await;
        let status = crate::health_check::SystemHealth::from_score(report.overall);
        crate::health_check::SystemHealth {
            overall_score: report.overall,
            status,
            tracker_layer_score: report.tracker_layer,
            dht_layer_score: report.dht_layer,
            peer_layer_score: report.peer_layer,
            active_trackers: report.active_trackers,
            total_trackers: report.total_trackers,
            avg_tracker_score: report.avg_tracker_score,
        }
    } else {
        crate::health_check::SystemHealth {
            overall_score: 0.0,
            status: crate::health_check::HealthStatus::Unhealthy,
            tracker_layer_score: 0.0,
            dht_layer_score: 0.0,
            peer_layer_score: 0.0,
            active_trackers: 0,
            total_trackers: 0,
            avg_tracker_score: 0.0,
        }
    };

    let crawler = state.crawler_state.as_ref().map(|cs| {
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
        })
    }).unwrap_or_else(|| serde_json::json!({"enabled": false}));

    // 从 TrackerRepo（数据层）获取 tracker 分数，而不是 registry（控制面）
    // 评分系统统一维护 TrackerRepo 的分数，其他地方均从数据 repo 调取
    let tracker_scores: Vec<serde_json::Value> = state.tracker_repo.as_ref()
        .map(|repo| {
            repo.all_trackers_sync()
                .into_iter()
                .map(|t| serde_json::json!({
                    "url": t.url,
                    "score": t.score,
                    "disabled": t.disabled,
                }))
                .collect()
        })
        .unwrap_or_default();

    // NAT 状态
    let nat_status = state.nat.status();
    let tcp_mapped = nat_status.mappings.iter().filter(|m| m.protocol == "TCP").count();
    let udp_mapped = nat_status.mappings.iter().filter(|m| m.protocol == "UDP").count();
    let tcp_verified = nat_status.mappings.iter().filter(|m| m.protocol == "TCP" && m.verified).count();
    let udp_verified = nat_status.mappings.iter().filter(|m| m.protocol == "UDP" && m.verified).count();
    let udp_reachable = nat_status.mappings.iter().filter(|m| m.protocol == "UDP" && m.reachable).count();

    serde_json::json!({
        "health": {
            "overall_score": health.overall_score,
            "status": format!("{:?}", health.status),
            "tracker_layer_score": health.tracker_layer_score,
            "dht_layer_score": health.dht_layer_score,
            "peer_layer_score": health.peer_layer_score,
            "active_trackers": health.active_trackers,
            "total_trackers": health.total_trackers,
            "avg_tracker_score": health.avg_tracker_score,
        },
        "crawler": crawler,
        "peer_repo": {
            "infohashes": cache_stats.0,
            "peers": cache_stats.1,
            "active_peers": active_peers,
        },
        "super_tracker": {
            "infohashes": state.super_tracker.infohash_count(),
            "peers": state.super_tracker.peer_count(),
        },
        "rate_limiter": {
            "qps": state.rate_limiter.current_qps(),
            "total_requests": state.rate_limiter.stats().total_requests,
            "blocked_requests": state.rate_limiter.stats().blocked_requests,
            "banned_ips": state.rate_limiter.banned_count(),
        },
        "tracker_scores": tracker_scores,
        // InfohashRepo
        "infohash_repo": {
            "count": state.infohash_repo.as_ref().map(|r| r.count_sync()).unwrap_or(0),
        },
        // NodeRepo（使用 stats_sync 避免全量克隆 60000+ 节点）
        "node_repo": state.node_repo.as_ref().map(|r| {
            let stats = r.stats_sync();
            serde_json::json!({
                "total": stats.total,
                "active": stats.active,
                "avg_score": stats.avg_score,
                "bad": stats.bad,
                "questionable": stats.questionable,
                "good": stats.good,
            })
        }).unwrap_or_else(|| serde_json::json!({"total": 0, "active": 0})),
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
        "fetcher": state.fetcher.as_ref().map(|f| {
            serde_json::json!({
                "infohash_repo_count": f.infohash_count(),
                "total_rounds": f.total_rounds.load(std::sync::atomic::Ordering::Relaxed),
                "total_peers_fetched": f.total_peers_fetched.load(std::sync::atomic::Ordering::Relaxed),
                "last_round_peers": f.last_round_peers.load(std::sync::atomic::Ordering::Relaxed),
            })
        }).unwrap_or_else(|| serde_json::json!({"enabled": false})),
        // DHT 探测器
        "dht_probe": state.dht_probe.as_ref().map(|p| {
            serde_json::json!({
                "total_probed": p.total_probed.load(std::sync::atomic::Ordering::Relaxed),
                "total_success": p.total_success.load(std::sync::atomic::Ordering::Relaxed),
                "total_added": p.total_added.load(std::sync::atomic::Ordering::Relaxed),
                "probed_cache": p.probed.read().len(),
            })
        }).unwrap_or_else(|| serde_json::json!({"enabled": false})),
        // uTP 服务端（BEP 29）
        "utp_server": state.utp_server.as_ref().map(|s| {
            let stats = s.stats();
            serde_json::json!({
                "enabled": true,
                "port": 6883,
                "syn_received": stats.syn_received,
                "connections_established": stats.connections_established,
                "handshakes_received": stats.handshakes_received,
                "peers_extracted": stats.peers_extracted,
                "active_connections": stats.active_connections,
                "connections_rejected": stats.connections_rejected,
                "connections_evicted": stats.connections_evicted,
                "max_connections": 100,
            })
        }).unwrap_or_else(|| serde_json::json!({"enabled": false})),
        // PEX 接收器（BEP 11）
        "pex_receiver": state.pex_receiver.as_ref().map(|r| {
            let stats = r.stats();
            serde_json::json!({
                "enabled": true,
                "extension_handshakes": stats.extension_handshakes,
                "pex_supported": stats.pex_supported,
                "pex_messages": stats.pex_messages,
                "peers_extracted": stats.peers_extracted,
                "ipv4_peers": stats.ipv4_peers,
                "ipv6_peers": stats.ipv6_peers,
                "utp_peers": stats.utp_peers,
                "holepunch_peers": stats.holepunch_peers,
                "dedup_skipped": stats.dedup_skipped,
                "parse_errors": stats.parse_errors,
            })
        }).unwrap_or_else(|| serde_json::json!({"enabled": false})),
        // TCP PEX 服务端（BEP 11，端口 6884）
        "tcp_pex": state.tcp_pex_server.as_ref().map(|s| {
            let stats = s.stats();
            serde_json::json!({
                "enabled": true,
                "port": 6884,
                "connections_accepted": stats.connections_accepted,
                "handshakes_completed": stats.handshakes_completed,
                "pex_messages": stats.pex_messages,
                "peers_extracted": stats.peers_extracted,
                "active_connections": stats.active_connections,
            })
        }).unwrap_or_else(|| serde_json::json!({"enabled": false})),
        // 主动 PEX 请求器（BEP 11）
        "active_pex": state.active_pex.as_ref().map(|s| {
            let stats = s.stats();
            serde_json::json!({
                "enabled": true,
                "connection_attempts": stats.connection_attempts,
                "connections_succeeded": stats.connections_succeeded,
                "connections_failed": stats.connections_failed,
                "handshakes_completed": stats.handshakes_completed,
                "extension_handshakes": stats.extension_handshakes,
                "pex_supported": stats.pex_supported,
                "pex_messages": stats.pex_messages,
                "peers_extracted": stats.peers_extracted,
                "timeouts": stats.timeouts,
                "errors": stats.errors,
            })
        }).unwrap_or_else(|| serde_json::json!({"enabled": false})),
        // 联邦网络状态
        "federation": state.federation.as_ref().map(|f| {
            let status = f.status();
            serde_json::json!({
                "enabled": status.enabled,
                "node_id": status.node_id,
                "connections": status.connections,
                "known_nodes": status.known_nodes,
                "reachability": status.reachability,
                "uptime_secs": status.uptime_secs,
                "gossip_queue_size": status.gossip_queue_size,
                "relay_channels": status.relay_channels,
            })
        }).unwrap_or_else(|| serde_json::json!({"enabled": false})),
    })
}

/// 处理单个 WebSocket 连接
async fn handle_socket(socket: WebSocket, event_bus: EventBus, state: AppState) {
    let (mut sender, mut receiver) = socket.split();

    let mut subscriptions: HashSet<String> = HashSet::new();
    subscriptions.insert("all".to_string());

    let mut rx = event_bus.subscribe();
    info!("[ws] 新客户端连接，订阅者总数: {}", event_bus.subscriber_count());

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

    // 立即推送一次状态
    let status_json = build_message("status", collect_status(&state).await);
    let _ = sender.send(Message::Text(status_json)).await;

    let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
    heartbeat.tick().await;
    // 状态推送间隔 5 秒（避免每秒克隆 60000+ 节点造成大量内存分配和磁盘 IO）
    let mut status_ticker = tokio::time::interval(Duration::from_secs(5));
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

            // 每秒推送状态
            _ = status_ticker.tick() => {
                let json = build_message("status", collect_status(&state).await);
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
