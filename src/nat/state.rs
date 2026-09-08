//! NAT 状态机
//!
//! 管理 NAT 模块的生命周期状态，支持状态转换和监控。
//!
//! 状态转换图：
//! ```
//! Uninitialized
//!      │
//!      ▼
//! Initializing ──► Mapping ──► Active
//!      │              │           │
//!      │              │           ▼
//!      │              │      GatewayLost
//!      │              │           │
//!      │              │           ▼
//!      │              │      Recovering ──► Active
//!      │              │
//!      │              ▼
//!      │         Degraded（部分映射失败）
//!      │
//!      ▼
//!    Error（不可恢复错误）
//!
//! 任意状态 ──► Released（主动释放）
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// NAT 状态枚举
// ---------------------------------------------------------------------------

/// NAT 模块状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NatState {
    /// 未初始化
    Uninitialized,
    /// 初始化中（发现网关）
    Initializing,
    /// 映射中（正在配置端口映射）
    Mapping,
    /// 活跃（映射成功且网关健康）
    Active,
    /// 降级（部分映射失败，但至少一个成功）
    Degraded,
    /// 网关失联（健康检查失败）
    GatewayLost,
    /// 恢复中（自动恢复映射）
    Recovering,
    /// 已释放（主动释放所有映射）
    Released,
    /// 错误（不可恢复错误）
    Error,
}

impl NatState {
    /// 状态名称
    pub fn as_str(&self) -> &'static str {
        match self {
            NatState::Uninitialized => "uninitialized",
            NatState::Initializing => "initializing",
            NatState::Mapping => "mapping",
            NatState::Active => "active",
            NatState::Degraded => "degraded",
            NatState::GatewayLost => "gateway_lost",
            NatState::Recovering => "recovering",
            NatState::Released => "released",
            NatState::Error => "error",
        }
    }

    /// 是否为稳定状态（Active/Degraded）
    pub fn is_stable(&self) -> bool {
        matches!(self, NatState::Active | NatState::Degraded)
    }

    /// 是否为错误状态
    pub fn is_error(&self) -> bool {
        matches!(self, NatState::Error | NatState::Released)
    }

    /// 是否允许转换到目标状态
    pub fn can_transition_to(&self, target: NatState) -> bool {
        match (self, target) {
            // 从任意状态可以转换到 Released 或 Error
            (_, NatState::Released) | (_, NatState::Error) => true,
            // 正常流程
            (NatState::Uninitialized, NatState::Initializing) => true,
            (NatState::Initializing, NatState::Mapping) => true,
            (NatState::Initializing, NatState::Error) => true,
            (NatState::Mapping, NatState::Active) => true,
            (NatState::Mapping, NatState::Degraded) => true,
            (NatState::Mapping, NatState::Error) => true,
            // 健康检查相关
            (NatState::Active, NatState::GatewayLost) => true,
            (NatState::Degraded, NatState::GatewayLost) => true,
            (NatState::GatewayLost, NatState::Recovering) => true,
            (NatState::Recovering, NatState::Active) => true,
            (NatState::Recovering, NatState::Degraded) => true,
            (NatState::Recovering, NatState::GatewayLost) => true,
            // Active 和 Degraded 之间可以互相转换
            (NatState::Active, NatState::Degraded) => true,
            (NatState::Degraded, NatState::Active) => true,
            // 重新初始化
            (NatState::Released, NatState::Initializing) => true,
            (NatState::Error, NatState::Initializing) => true,
            // 其他转换不允许
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// 状态转换记录
// ---------------------------------------------------------------------------

/// 状态转换记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateTransition {
    /// 源状态
    pub from: String,
    /// 目标状态
    pub to: String,
    /// 转换时间戳
    pub timestamp: u64,
    /// 转换原因
    pub reason: String,
    /// 持续时间（毫秒，在源状态停留的时间）
    pub duration_ms: u64,
}

// ---------------------------------------------------------------------------
// 状态机
// ---------------------------------------------------------------------------

/// NAT 状态机
///
/// 管理 NAT 模块的状态转换，记录转换历史，提供状态查询。
pub struct NatStateMachine {
    state: Arc<RwLock<NatState>>,
    last_transition: Arc<RwLock<Instant>>,
    transition_history: Arc<RwLock<Vec<StateTransition>>>,
    max_history: usize,
}

impl NatStateMachine {
    /// 创建状态机（初始状态 Uninitialized）
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(NatState::Uninitialized)),
            last_transition: Arc::new(RwLock::new(Instant::now())),
            transition_history: Arc::new(RwLock::new(Vec::new())),
            max_history: 100,
        }
    }

    /// 获取当前状态
    pub fn current_state(&self) -> NatState {
        *self.state.read()
    }

    /// 尝试转换状态
    ///
    /// 如果转换不允许，返回 false 并记录警告。
    pub fn transition(&self, target: NatState, reason: &str) -> bool {
        let current = *self.state.read();

        if !current.can_transition_to(target) {
            warn!(
                "[nat-state] 非法状态转换: {} -> {} (原因: {})",
                current.as_str(),
                target.as_str(),
                reason
            );
            return false;
        }

        let duration = self.last_transition.read().elapsed().as_millis() as u64;

        // 记录转换历史
        let transition = StateTransition {
            from: current.as_str().to_string(),
            to: target.as_str().to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            reason: reason.to_string(),
            duration_ms: duration,
        };

        let mut history = self.transition_history.write();
        history.push(transition);
        if history.len() > self.max_history {
            history.remove(0);
        }

        // 更新状态
        *self.state.write() = target;
        *self.last_transition.write() = Instant::now();

        info!(
            "[nat-state] 状态转换: {} -> {} (原因: {}, 持续: {}ms)",
            current.as_str(),
            target.as_str(),
            reason,
            duration
        );

        true
    }

    /// 强制转换状态（不检查转换规则，用于错误恢复）
    pub fn force_transition(&self, target: NatState, reason: &str) {
        let current = *self.state.read();
        let duration = self.last_transition.read().elapsed().as_millis() as u64;

        let transition = StateTransition {
            from: current.as_str().to_string(),
            to: target.as_str().to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            reason: format!("[FORCE] {}", reason),
            duration_ms: duration,
        };

        let mut history = self.transition_history.write();
        history.push(transition);
        if history.len() > self.max_history {
            history.remove(0);
        }

        *self.state.write() = target;
        *self.last_transition.write() = Instant::now();

        warn!(
            "[nat-state] 强制状态转换: {} -> {} (原因: {})",
            current.as_str(),
            target.as_str(),
            reason
        );
    }

    /// 获取当前状态持续时间
    pub fn current_state_duration(&self) -> Duration {
        self.last_transition.read().elapsed()
    }

    /// 获取转换历史
    pub fn transition_history(&self) -> Vec<StateTransition> {
        self.transition_history.read().clone()
    }

    /// 获取最近 N 次转换
    pub fn recent_transitions(&self, n: usize) -> Vec<StateTransition> {
        let history = self.transition_history.read();
        history.iter().rev().take(n).cloned().collect()
    }

    /// 获取状态统计（各状态停留总时间）
    pub fn state_statistics(&self) -> std::collections::HashMap<String, u64> {
        let mut stats = std::collections::HashMap::new();
        let history = self.transition_history.read();
        for t in history.iter() {
            *stats.entry(t.from.clone()).or_insert(0) += t.duration_ms;
        }
        // 加上当前状态的持续时间
        let current = self.current_state();
        let current_duration = self.current_state_duration().as_millis() as u64;
        *stats.entry(current.as_str().to_string()).or_insert(0) += current_duration;
        stats
    }

    /// 是否为活跃状态（Active 或 Degraded）
    pub fn is_active(&self) -> bool {
        self.current_state().is_stable()
    }

    /// 是否为错误状态
    pub fn is_error(&self) -> bool {
        self.current_state().is_error()
    }
}

impl Default for NatStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for NatStateMachine {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            last_transition: self.last_transition.clone(),
            transition_history: self.transition_history.clone(),
            max_history: self.max_history,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_transitions() {
        let sm = NatStateMachine::new();
        assert_eq!(sm.current_state(), NatState::Uninitialized);

        // 正常流程
        assert!(sm.transition(NatState::Initializing, "开始初始化"));
        assert!(sm.transition(NatState::Mapping, "开始映射"));
        assert!(sm.transition(NatState::Active, "映射成功"));
        assert_eq!(sm.current_state(), NatState::Active);

        // 非法转换
        assert!(!sm.transition(NatState::Uninitialized, "非法转换"));
        assert_eq!(sm.current_state(), NatState::Active);
    }

    #[test]
    fn test_gateway_lost_recovery() {
        let sm = NatStateMachine::new();
        sm.force_transition(NatState::Active, "初始化");

        // 网关失联
        assert!(sm.transition(NatState::GatewayLost, "健康检查失败"));
        assert!(sm.transition(NatState::Recovering, "开始恢复"));
        assert!(sm.transition(NatState::Active, "恢复成功"));
        assert_eq!(sm.current_state(), NatState::Active);
    }

    #[test]
    fn test_force_transition() {
        let sm = NatStateMachine::new();
        sm.force_transition(NatState::Error, "强制错误");
        assert_eq!(sm.current_state(), NatState::Error);

        // 从 Error 可以重新初始化
        assert!(sm.transition(NatState::Initializing, "重新初始化"));
        assert_eq!(sm.current_state(), NatState::Initializing);
    }

    #[test]
    fn test_transition_history() {
        let sm = NatStateMachine::new();
        sm.transition(NatState::Initializing, "test1");
        sm.transition(NatState::Mapping, "test2");
        sm.transition(NatState::Active, "test3");

        let history = sm.transition_history();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].from, "uninitialized");
        assert_eq!(history[2].to, "active");
    }

    #[test]
    fn test_state_as_str() {
        assert_eq!(NatState::Active.as_str(), "active");
        assert_eq!(NatState::GatewayLost.as_str(), "gateway_lost");
        assert!(NatState::Active.is_stable());
        assert!(!NatState::GatewayLost.is_stable());
    }
}
