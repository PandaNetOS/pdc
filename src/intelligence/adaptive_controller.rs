//! 自适应控制器（ICC 预测式自适应）
//!
//! 根据历史响应率和预测模型，动态调整爬虫发送倍率。
//! 支持三种运行模式：Managed（pk 设置目标）、Standalone（默认目标 30%）、Degraded（保守，倍率不超过 1.0）。

use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use super::crawler_history::{CrawlerHistory, CrawlerRoundRecord, ResponseRatePredictor};
use crate::config::AdaptiveConfig;

/// 运行模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    /// 托管模式：pk 设置目标响应率
    Managed,
    /// 独立模式：用默认目标 30%
    Standalone,
    /// 降级模式：保守，倍率不超过 1.0
    Degraded,
}

/// 轻量原子 f64（通过 AtomicU64 bit 表示，当前工具链无 AtomicF64）
struct AtomicF64Compat(AtomicU64);

impl AtomicF64Compat {
    fn new(val: f64) -> Self {
        Self(AtomicU64::new(val.to_bits()))
    }
    fn load(&self, order: Ordering) -> f64 {
        f64::from_bits(self.0.load(order))
    }
    fn store(&self, val: f64, order: Ordering) {
        self.0.store(val.to_bits(), order);
    }
}

/// 自适应控制器核心
pub struct AdaptiveController {
    history: Mutex<CrawlerHistory>,
    predictor: Mutex<ResponseRatePredictor>,
    current_multiplier: AtomicF64Compat,
    mode: Mutex<RunMode>,
    target_response_rate: AtomicF64Compat,
    config: AdaptiveConfig,
    /// 最近一轮实际响应率（预测置信度不足时回退使用）
    last_response_rate: AtomicF64Compat,
}

impl AdaptiveController {
    pub fn new(config: AdaptiveConfig) -> Self {
        Self {
            history: Mutex::new(CrawlerHistory::new(1000)),
            predictor: Mutex::new(ResponseRatePredictor::new()),
            current_multiplier: AtomicF64Compat::new(1.0),
            mode: Mutex::new(RunMode::Standalone),
            target_response_rate: AtomicF64Compat::new(config.target_rate_standalone),
            config,
            last_response_rate: AtomicF64Compat::new(0.0),
        }
    }

    /// 返回当前发送倍率（只读，不调整）。冷启动或未启用时返回 1.0
    pub fn next_rate_multiplier(&self) -> f64 {
        if !self.config.enabled {
            return 1.0;
        }
        let history_len = self.history.lock().len();
        if history_len < self.config.warmup_rounds as usize {
            return 1.0;
        }
        self.current_multiplier.load(Ordering::Relaxed)
    }

    /// 根据预测/实际响应率调整倍率（每轮调用一次，在 report_round_result 末尾执行）
    fn adjust_multiplier(&self) {
        if !self.config.enabled {
            return;
        }
        let history_len = self.history.lock().len();
        if history_len < self.config.warmup_rounds as usize {
            return;
        }

        // 获取目标响应率
        let target = match *self.mode.lock() {
            RunMode::Managed => self.target_response_rate.load(Ordering::Relaxed),
            RunMode::Standalone | RunMode::Degraded => self.config.target_rate_standalone,
        };

        // 获取预测响应率与最近 3 轮滑动均值（无特征时回退瞬时值）。
        // 决策用滑动均值替代瞬时值：单轮抖动（启动过渡期、网络瞬态）不应触发大幅降倍率。
        let (predicted, actual_smooth) = {
            let history = self.history.lock();
            match history.extract_features() {
                Some(features) => {
                    let predictor = self.predictor.lock();
                    let pred = predictor.predict(&features);
                    (pred, features.rate_last_3)
                }
                None => (None, self.last_response_rate.load(Ordering::Relaxed)),
            }
        };

        // 预测与滑动均值严重背离（相对差 > 50%）时视为模型失真，回退用滑动均值决策，
        // 防止失真预测把倍率错误锁死（2026-10-05 实测：预测 2.76% vs 累计实际 91.7%）。
        // actual == 0.0 时无法比较，保留预测值走原逻辑。
        let predicted = predicted.filter(|pred| {
            actual_smooth <= 0.0 || (pred - actual_smooth).abs() <= actual_smooth * 0.5
        });

        let current = self.current_multiplier.load(Ordering::Relaxed);
        let new_mult = if let Some(pred) = predicted {
            if pred < target * 0.7 {
                current * self.config.rate_step_down_heavy
            } else if pred < target {
                current * self.config.rate_step_down_light
            } else {
                current * self.config.rate_step_up
            }
        } else {
            if actual_smooth < target * 0.7 {
                current * self.config.rate_step_down_heavy
            } else if actual_smooth < target {
                current * self.config.rate_step_down_light
            } else {
                current * self.config.rate_step_up
            }
        };

        // Degraded 模式：倍率不超过 1.0
        let new_mult = match *self.mode.lock() {
            RunMode::Degraded => new_mult.min(1.0),
            _ => new_mult,
        };

        // clamp 到 [min_multiplier, max_multiplier]
        let new_mult = new_mult.clamp(self.config.min_multiplier, self.config.max_multiplier);
        self.current_multiplier.store(new_mult, Ordering::Relaxed);
    }

    /// 报告一轮结果：记录历史、提取特征、更新预测模型、调整倍率（每轮一次）
    #[allow(clippy::too_many_arguments)]
    pub fn report_round_result(
        &self,
        sent: u64,
        responded: u64,
        avg_latency_ms: f64,
        pending_before: usize,
        pending_after: usize,
        socket_idx: usize,
        nodes_quality: f64,
    ) {
        // 无数据轮次直接跳过：sent == 0 表示该轮未发出任何请求，
        // 不是"0 响应"，不得污染历史/模型/倍率决策。
        // （2026-10-05 实测：倍率被打到下限后发送骤停，sent≈0 的轮次把
        //  response_rate=0 持续喂给决策与训练，倍率被永久锁死 0.2）
        if sent == 0 {
            return;
        }

        let response_rate = if sent > 0 {
            responded as f64 / sent as f64
        } else {
            0.0
        };

        self.last_response_rate
            .store(response_rate, Ordering::Relaxed);

        // round_id 传 0，CrawlerHistory::record 内部会覆盖为 next_round_id
        let record = CrawlerRoundRecord {
            round_id: 0,
            timestamp: Instant::now(),
            packets_sent: sent,
            packets_responded: responded,
            response_rate,
            avg_latency_ms,
            pending_before,
            pending_after,
            socket_idx,
            nodes_quality,
        };

        let mut history = self.history.lock();
        history.record(record);

        // 够提取特征后更新预测模型。
        // 预热期内只记录、不训练：预热阶段（倍率固定 1.0、全速发送）的响应率往往被高丢包
        // 压低，若此时训练模型会把权重带偏，导致预热结束后预测持续悲观、倍率被锁到下限。
        // （2026-10-05 实测：预热期 8 socket 全速丢包 0.66~0.77，模型训练出负权重后预测
        //  0.4% vs 实际 56%，倍率被 clamp 到 0.2，新节点率从 11,700/h 跌回 1,380/h）
        if history.len() >= 5 && history.len() > self.config.warmup_rounds as usize {
            if let Some(features) = history.extract_features() {
                let mut predictor = self.predictor.lock();
                predictor.update(&features, response_rate);
            }
        }

        // 每轮结束后调整一次倍率（释放 history 锁后执行，避免死锁）
        drop(history);
        self.adjust_multiplier();
    }

    pub fn set_mode(&self, mode: RunMode) {
        *self.mode.lock() = mode;
    }

    pub fn set_target_response_rate(&self, target: f64) {
        self.target_response_rate.store(target, Ordering::Relaxed);
    }

    pub fn current_multiplier(&self) -> f64 {
        self.current_multiplier.load(Ordering::Relaxed)
    }

    pub fn predicted_rate(&self) -> Option<f64> {
        let history = self.history.lock();
        match history.extract_features() {
            Some(features) => {
                let predictor = self.predictor.lock();
                predictor.predict(&features)
            }
            None => None,
        }
    }

    pub fn model_update_count(&self) -> u64 {
        let predictor = self.predictor.lock();
        predictor.update_count()
    }

    pub fn history_len(&self) -> usize {
        self.history.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adaptive_config_default() {
        let config = AdaptiveConfig::default();
        assert!(config.enabled);
        assert_eq!(config.min_multiplier, 0.2);
        assert_eq!(config.max_multiplier, 2.0);
        assert_eq!(config.warmup_rounds, 50);
        assert!((config.target_rate_standalone - 0.30).abs() < f64::EPSILON);
        assert!((config.rate_step_up - 1.1).abs() < f64::EPSILON);
        assert!((config.rate_step_down_light - 0.8).abs() < f64::EPSILON);
        assert!((config.rate_step_down_heavy - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_controller_cold_start_returns_one() {
        let config = AdaptiveConfig::default();
        let controller = AdaptiveController::new(config);
        // 0 轮历史，冷启动
        assert_eq!(controller.next_rate_multiplier(), 1.0);
    }

    #[test]
    fn test_controller_disabled_returns_one() {
        let config = AdaptiveConfig {
            enabled: false,
            ..Default::default()
        };
        let controller = AdaptiveController::new(config);
        // 报告 60 轮低响应率
        for _ in 0..60 {
            controller.report_round_result(100, 5, 10.0, 100, 200, 0, 0.5);
        }
        assert_eq!(controller.next_rate_multiplier(), 1.0);
    }

    #[test]
    fn test_controller_adjusts_multiplier() {
        let config = AdaptiveConfig::default();
        let controller = AdaptiveController::new(config);
        // 报告 60 轮极低响应率（5%），远超 warmup_rounds=50
        for _ in 0..60 {
            controller.report_round_result(100, 5, 10.0, 100, 200, 0, 0.5);
        }
        let mult = controller.next_rate_multiplier();
        assert!(mult < 1.0, "expected multiplier < 1.0, got {}", mult);
    }

    #[test]
    fn test_warmup_no_training() {
        let config = AdaptiveConfig::default();
        let controller = AdaptiveController::new(config);
        // 预热期（warmup_rounds=50）内只记录历史、不训练模型：
        // 预热阶段全速发送的高丢包数据不得污染预测模型权重
        for _ in 0..50 {
            controller.report_round_result(100, 5, 10.0, 100, 200, 0, 0.5);
        }
        assert_eq!(
            controller.model_update_count(),
            0,
            "warmup rounds must not train the model"
        );
        // 预热结束后才开始训练（第 51 轮起，每轮 +1）
        controller.report_round_result(100, 5, 10.0, 100, 200, 0, 0.5);
        assert_eq!(controller.model_update_count(), 1);
    }

    #[test]
    fn test_prediction_divergence_fallback() {
        let config = AdaptiveConfig::default();
        let controller = AdaptiveController::new(config);
        // 第一阶段：60 轮低响应率（5%）。预热结束后模型被训练为悲观，倍率跌到下限 0.2
        for _ in 0..60 {
            controller.report_round_result(100, 5, 10.0, 100, 200, 0, 0.5);
        }
        assert!(
            controller.current_multiplier() <= 0.2 + 1e-9,
            "should be clamped at floor, got {}",
            controller.current_multiplier()
        );
        // 第二阶段：30 轮高响应率（80%）。若预测失真（与实测背离 > 50%），应回退用滑动
        // 均值决策（80% > 目标 30%*0.7 → step_up），倍率从下限回升，不得继续锁死 0.2
        for _ in 0..30 {
            controller.report_round_result(100, 80, 10.0, 100, 200, 0, 0.9);
        }
        let mult = controller.current_multiplier();
        assert!(
            mult > 0.2,
            "multiplier should recover from floor after divergence fallback, got {}",
            mult
        );
    }

    #[test]
    fn test_zero_sent_rounds_ignored() {
        let config = AdaptiveConfig::default();
        let controller = AdaptiveController::new(config);
        // 预热结束后报告 60 轮有效数据（倍率进入调整期）
        for _ in 0..60 {
            controller.report_round_result(100, 5, 10.0, 100, 200, 0, 0.5);
        }
        let mult_before = controller.current_multiplier();
        let hist_before = controller.history_len();
        let upd_before = controller.model_update_count();
        // sent=0 的无数据轮次：不记录、不训练、不调整倍率（区别于"0 响应"）
        for _ in 0..10 {
            controller.report_round_result(0, 0, 10.0, 0, 0, 0, 0.5);
        }
        assert_eq!(
            controller.history_len(),
            hist_before,
            "zero-sent rounds must not be recorded"
        );
        assert_eq!(
            controller.model_update_count(),
            upd_before,
            "zero-sent rounds must not train the model"
        );
        assert_eq!(
            controller.current_multiplier(),
            mult_before,
            "zero-sent rounds must not adjust multiplier"
        );
    }

    #[test]
    fn test_smoothing_avoids_instant_crash() {
        let config = AdaptiveConfig::default();
        let controller = AdaptiveController::new(config);
        // 预热期 50 轮：高响应率（网络健康）
        for _ in 0..50 {
            controller.report_round_result(100, 70, 10.0, 100, 200, 0, 0.8);
        }
        // 预热结束后：单轮瞬时低响应率（启动抖动），但最近 3 轮滑动均值仍高（0.7,0.7,0.03→0.48）
        controller.report_round_result(100, 3, 10.0, 100, 200, 0, 0.5);
        let mult = controller.current_multiplier();
        assert!(
            mult >= 1.0,
            "single instant low-rate round must not crash multiplier (smoothing), got {}",
            mult
        );
    }

    #[test]
    fn test_controller_degraded_mode_caps_at_one() {
        let config = AdaptiveConfig::default();
        let controller = AdaptiveController::new(config);
        controller.set_mode(RunMode::Degraded);
        // 报告 60 轮高响应率（95%）
        for _ in 0..60 {
            controller.report_round_result(100, 95, 10.0, 100, 200, 0, 0.9);
        }
        let mult = controller.next_rate_multiplier();
        assert!(
            mult <= 1.0,
            "expected multiplier <= 1.0 in degraded mode, got {}",
            mult
        );
    }

    #[test]
    fn test_controller_managed_mode_target() {
        let config = AdaptiveConfig::default();
        let controller = AdaptiveController::new(config);
        controller.set_mode(RunMode::Managed);
        // 设置极低目标（1%），即使实际 5% 也会触发提升
        controller.set_target_response_rate(0.01);
        // 报告 50 轮（=warmup_rounds），预测器仅 46 次更新 < 50，predict 返回 None，
        // 回退到最近实际响应率 0.05
        for _ in 0..50 {
            controller.report_round_result(100, 5, 10.0, 100, 200, 0, 0.5);
        }
        let mult = controller.next_rate_multiplier();
        // 实际率 5% >= 目标 1%，应触发 step_up（1.1），倍率 > 1.0
        assert!(
            mult > 1.0,
            "expected multiplier > 1.0 with low managed target, got {}",
            mult
        );
    }
}
