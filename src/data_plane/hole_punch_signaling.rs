//! UDP 打洞信令服务器
//!
//! 超级 Tracker 兼做打洞信令服务器，协调两个 NAT 后面的节点交换公网地址，
//! 然后同时开始 UDP 打洞。
//!
//! 流程：
//! ```
//! 节点 A                    信令服务器                    节点 B
//!   │                           │                           │
//!   │── POST /initiate ───────→│                           │
//!   │  {target, my_addr}        │                           │
//!   │←── {session_id} ──────────│                           │
//!   │                           │── GET /wait/{id} ───────→│
//!   │                           │←── {my_addr} ────────────│
//!   │←── {peer_addr} ───────────│                           │
//!   │                           │── {peer_addr} ───────────→│
//!   │                           │                           │
//!   │═══ 同时开始 UDP 打洞 ════════════════════════════════│
//!   │                           │                           │
//!   │── POST /complete/{id} ──→│                           │
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// 会话状态
// ---------------------------------------------------------------------------

/// 打洞会话状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    /// 等待对端加入
    Waiting,
    /// 双方已加入，地址已交换
    Ready,
    /// 打洞完成
    Completed,
    /// 已超时
    Timeout,
}

/// 打洞会话
#[derive(Debug, Clone)]
pub struct HolePunchSession {
    /// 会话 ID
    pub session_id: String,
    /// 发起方 peer_id
    pub initiator_peer_id: String,
    /// 目标方 peer_id
    pub target_peer_id: String,
    /// 发起方公网映射地址
    pub initiator_addr: Option<SocketAddr>,
    /// 目标方公网映射地址
    pub target_addr: Option<SocketAddr>,
    /// 创建时间
    pub created_at: Instant,
    /// 最后活跃时间
    pub last_active: Instant,
    /// 状态
    pub status: SessionStatus,
    /// 打洞结果（成功/失败）
    pub result: Option<bool>,
}

impl HolePunchSession {
    /// 创建新会话
    pub fn new(initiator_peer_id: String, target_peer_id: String, initiator_addr: SocketAddr) -> Self {
        let now = Instant::now();
        Self {
            session_id: Uuid::new_v4().to_string(),
            initiator_peer_id,
            target_peer_id,
            initiator_addr: Some(initiator_addr),
            target_addr: None,
            created_at: now,
            last_active: now,
            status: SessionStatus::Waiting,
            result: None,
        }
    }

    /// 是否过期（超过 60 秒无活动）
    pub fn is_expired(&self) -> bool {
        self.last_active.elapsed() > Duration::from_secs(60)
    }

    /// 双方地址是否都已就绪
    pub fn is_ready(&self) -> bool {
        self.initiator_addr.is_some() && self.target_addr.is_some()
    }
}

// ---------------------------------------------------------------------------
// 请求/响应类型
// ---------------------------------------------------------------------------

/// 发起打洞请求
#[derive(Debug, Deserialize)]
pub struct InitiateRequest {
    /// 目标 peer_id
    pub target_peer_id: String,
    /// 发起方 peer_id
    pub initiator_peer_id: String,
    /// 发起方公网映射地址（通过 STUN 获取）
    pub initiator_addr: String,
}

/// 发起打洞响应
#[derive(Debug, Serialize)]
pub struct InitiateResponse {
    /// 会话 ID
    pub session_id: String,
    /// 状态
    pub status: String,
}

/// 等待对端响应
#[derive(Debug, Serialize)]
pub struct WaitResponse {
    /// 会话 ID
    pub session_id: String,
    /// 对端公网映射地址
    pub peer_addr: String,
    /// 状态
    pub status: String,
}

/// 上报打洞结果请求
#[derive(Debug, Deserialize)]
pub struct CompleteRequest {
    /// 打洞是否成功
    pub success: bool,
    /// 上报方 peer_id
    pub peer_id: String,
    /// 错误信息（如果失败）
    pub error: Option<String>,
}

/// 上报打洞结果响应
#[derive(Debug, Serialize)]
pub struct CompleteResponse {
    /// 会话 ID
    pub session_id: String,
    /// 状态
    pub status: String,
    /// 双方是否都已上报
    pub both_reported: bool,
}

// ---------------------------------------------------------------------------
// 信令服务器
// ---------------------------------------------------------------------------

/// UDP 打洞信令服务器
pub struct HolePunchSignaling {
    /// 活跃会话（session_id -> session）
    sessions: Arc<RwLock<HashMap<String, HolePunchSession>>>,
    /// 按 peer_id 索引的会话（peer_id -> session_id），用于快速查找
    peer_sessions: Arc<RwLock<HashMap<String, String>>>,
}

impl HolePunchSignaling {
    /// 创建信令服务器
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            peer_sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 发起打洞
    pub fn initiate(&self, req: InitiateRequest) -> anyhow::Result<InitiateResponse> {
        let initiator_addr: SocketAddr = req.initiator_addr.parse()
            .map_err(|e| anyhow::anyhow!("无效的发起方地址: {}", e))?;

        // 检查发起方是否已有活跃会话
        {
            let peer_sessions = self.peer_sessions.read();
            if let Some(existing_id) = peer_sessions.get(&req.initiator_peer_id) {
                let sessions = self.sessions.read();
                if let Some(existing) = sessions.get(existing_id) {
                    if !existing.is_expired() && existing.status != SessionStatus::Completed {
                        debug!("[signaling] 发起方 {} 已有活跃会话 {}", req.initiator_peer_id, existing_id);
                        return Ok(InitiateResponse {
                            session_id: existing_id.clone(),
                            status: "waiting".to_string(),
                        });
                    }
                }
            }
        }

        // 创建新会话
        let session = HolePunchSession::new(
            req.initiator_peer_id.clone(),
            req.target_peer_id.clone(),
            initiator_addr,
        );
        let session_id = session.session_id.clone();

        info!(
            "[signaling] 发起打洞会话: {} ({} -> {})",
            session_id, req.initiator_peer_id, req.target_peer_id
        );

        self.sessions.write().insert(session_id.clone(), session);
        self.peer_sessions.write().insert(req.initiator_peer_id, session_id.clone());

        Ok(InitiateResponse {
            session_id,
            status: "waiting".to_string(),
        })
    }

    /// 等待对端加入（长轮询）
    ///
    /// 目标方调用此方法，传入自己的地址，等待发起方的地址。
    /// 发起方也可以调用此方法，等待目标方加入。
    pub async fn wait(&self, session_id: &str, peer_id: &str, my_addr: &str) -> anyhow::Result<WaitResponse> {
        let my_addr: SocketAddr = my_addr.parse()
            .map_err(|e| anyhow::anyhow!("无效的地址: {}", e))?;

        let deadline = Instant::now() + Duration::from_secs(30);

        loop {
            // 检查会话是否存在
            {
                let mut sessions = self.sessions.write();
                if let Some(session) = sessions.get_mut(session_id) {
                    // 更新最后活跃时间
                    session.last_active = Instant::now();

                    // 判断是发起方还是目标方
                    let is_initiator = session.initiator_peer_id == peer_id;
                    let is_target = session.target_peer_id == peer_id;

                    if !is_initiator && !is_target {
                        return Err(anyhow::anyhow!("peer_id {} 不属于会话 {}", peer_id, session_id));
                    }

                    // 记录地址
                    if is_target && session.target_addr.is_none() {
                        session.target_addr = Some(my_addr);
                        debug!("[signaling] 目标方 {} 已加入会话 {}", peer_id, session_id);
                    }

                    // 检查是否双方都已就绪
                    if session.is_ready() {
                        session.status = SessionStatus::Ready;
                        let peer_addr = if is_initiator {
                            session.target_addr.unwrap()
                        } else {
                            session.initiator_addr.unwrap()
                        };
                        info!("[signaling] 会话 {} 双方就绪，地址已交换", session_id);
                        return Ok(WaitResponse {
                            session_id: session_id.to_string(),
                            peer_addr: peer_addr.to_string(),
                            status: "ready".to_string(),
                        });
                    }

                    // 检查是否超时
                    if session.is_expired() {
                        session.status = SessionStatus::Timeout;
                        return Err(anyhow::anyhow!("会话 {} 已超时", session_id));
                    }
                } else {
                    return Err(anyhow::anyhow!("会话 {} 不存在", session_id));
                }
            }

            // 检查是否超过等待截止时间
            if Instant::now() > deadline {
                return Err(anyhow::anyhow!("等待对端加入超时"));
            }

            // 等待 500ms 后重试
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// 上报打洞结果
    pub fn complete(&self, session_id: &str, req: CompleteRequest) -> anyhow::Result<CompleteResponse> {
        let mut sessions = self.sessions.write();
        let session = sessions.get_mut(session_id)
            .ok_or_else(|| anyhow::anyhow!("会话 {} 不存在", session_id))?;

        session.last_active = Instant::now();
        session.status = SessionStatus::Completed;
        session.result = Some(req.success);

        // 清理 peer_sessions 索引
        self.peer_sessions.write().remove(&req.peer_id);

        info!(
            "[signaling] 会话 {} 打洞完成: success={}, peer={}",
            session_id, req.success, req.peer_id
        );

        Ok(CompleteResponse {
            session_id: session_id.to_string(),
            status: "completed".to_string(),
            both_reported: true, // 简化：一方上报即视为完成
        })
    }

    /// 清理过期会话
    pub fn cleanup_expired(&self) -> usize {
        let mut sessions = self.sessions.write();
        let mut peer_sessions = self.peer_sessions.write();

        let before = sessions.len();
        sessions.retain(|id, session| {
            if session.is_expired() {
                debug!("[signaling] 清理过期会话: {}", id);
                peer_sessions.remove(&session.initiator_peer_id);
                peer_sessions.remove(&session.target_peer_id);
                false
            } else {
                true
            }
        });

        let cleaned = before - sessions.len();
        if cleaned > 0 {
            debug!("[signaling] 清理了 {} 个过期会话", cleaned);
        }
        cleaned
    }

    /// 获取活跃会话数
    pub fn active_session_count(&self) -> usize {
        self.sessions.read().len()
    }

    /// 获取会话统计
    pub fn stats(&self) -> SignalingStats {
        let sessions = self.sessions.read();
        let waiting = sessions.values().filter(|s| s.status == SessionStatus::Waiting).count();
        let ready = sessions.values().filter(|s| s.status == SessionStatus::Ready).count();
        let completed = sessions.values().filter(|s| s.status == SessionStatus::Completed).count();
        SignalingStats {
            total: sessions.len(),
            waiting,
            ready,
            completed,
        }
    }
}

impl Default for HolePunchSignaling {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for HolePunchSignaling {
    fn clone(&self) -> Self {
        Self {
            sessions: self.sessions.clone(),
            peer_sessions: self.peer_sessions.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// 统计
// ---------------------------------------------------------------------------

/// 信令服务器统计
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalingStats {
    /// 总会话数
    pub total: usize,
    /// 等待中的会话
    pub waiting: usize,
    /// 已就绪的会话
    pub ready: usize,
    /// 已完成的会话
    pub completed: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_expiry() {
        let session = HolePunchSession::new(
            "peer_a".to_string(),
            "peer_b".to_string(),
            "1.2.3.4:5678".parse().unwrap(),
        );
        assert!(!session.is_expired());
        assert!(!session.is_ready());
    }

    #[test]
    fn test_initiate_and_complete() {
        let signaling = HolePunchSignaling::new();

        // 发起
        let req = InitiateRequest {
            target_peer_id: "peer_b".to_string(),
            initiator_peer_id: "peer_a".to_string(),
            initiator_addr: "1.2.3.4:5678".to_string(),
        };
        let resp = signaling.initiate(req).unwrap();
        assert!(!resp.session_id.is_empty());
        assert_eq!(resp.status, "waiting");

        // 上报完成
        let complete_req = CompleteRequest {
            success: true,
            peer_id: "peer_a".to_string(),
            error: None,
        };
        let complete_resp = signaling.complete(&resp.session_id, complete_req).unwrap();
        assert_eq!(complete_resp.status, "completed");
    }

    #[test]
    fn test_cleanup_expired() {
        let signaling = HolePunchSignaling::new();
        assert_eq!(signaling.cleanup_expired(), 0);
        assert_eq!(signaling.active_session_count(), 0);
    }

    #[test]
    fn test_stats() {
        let signaling = HolePunchSignaling::new();
        let stats = signaling.stats();
        assert_eq!(stats.total, 0);
        assert_eq!(stats.waiting, 0);
    }
}
