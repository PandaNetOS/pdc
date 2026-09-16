//! 爬虫历史数据采集、特征工程和增量线性回归预测模型
//!
//! 本模块为 PDC 智能控制中心（ICC）提供爬虫轮次历史数据支撑：
//! - [`CrawlerRoundRecord`]：每轮爬虫运行的结构化记录
//! - [`PredictionFeatures`]：从历史窗口提取的 10 维预测特征
//! - [`CrawlerHistory`]：环形缓冲区存储历史记录，负责特征工程
//! - [`ResponseRatePredictor`]：增量线性回归（SGD）模型，预测下一轮响应率
//!
//! 【设计原则】纯 std 实现，无外部依赖；所有 f64 计算含除零保护。

use std::collections::VecDeque;
use std::time::Instant;

// ---------------------------------------------------------------------------
// CrawlerRoundRecord —— 每轮爬虫记录
// ---------------------------------------------------------------------------

/// 单轮爬虫运行的结构化记录
#[derive(Debug, Clone)]
pub struct CrawlerRoundRecord {
    /// 轮次 ID（由 `CrawlerHistory::record` 自动分配）
    pub round_id: u64,
    /// 记录时间戳
    pub timestamp: Instant,
    /// 本轮发送的数据包数
    pub packets_sent: u64,
    /// 本轮收到响应的数据包数
    pub packets_responded: u64,
    /// 响应率（0.0 ~ 1.0）
    pub response_rate: f64,
    /// 平均延迟（毫秒）
    pub avg_latency_ms: f64,
    /// 本轮开始前的 pending 队列长度
    pub pending_before: usize,
    /// 本轮结束后的 pending 队列长度
    pub pending_after: usize,
    /// 所属 socket 索引（多 socket 架构）
    pub socket_idx: usize,
    /// 节点质量评分（0.0 ~ 1.0）
    pub nodes_quality: f64,
}

// ---------------------------------------------------------------------------
// PredictionFeatures —— 10 维预测特征
// ---------------------------------------------------------------------------

/// 从历史窗口提取的 10 维预测特征
#[derive(Debug, Clone)]
pub struct PredictionFeatures {
    /// 最近 1 轮响应率
    pub rate_last_1: f64,
    /// 最近 3 轮响应率均值
    pub rate_last_3: f64,
    /// 最近 5 轮响应率均值
    pub rate_last_5: f64,
    /// 最近 5 轮响应率线性回归斜率
    pub rate_trend: f64,
    /// 当前 pending_after（最近 1 轮）
    pub pending_current: f64,
    /// 最近 3 轮 pending_after 平均增量（/轮）
    pub pending_growth: f64,
    /// 最近 3 轮平均延迟均值
    pub latency_last_3: f64,
    /// 最近 3 轮平均延迟线性回归斜率
    pub latency_trend: f64,
    /// 最近 1 轮发送包数
    pub sent_last_1: f64,
    /// 最近 3 轮发送包数均值
    pub sent_last_3: f64,
}

impl PredictionFeatures {
    /// 按字段声明顺序返回 10 维向量
    pub fn to_vector(&self) -> Vec<f64> {
        vec![
            self.rate_last_1,
            self.rate_last_3,
            self.rate_last_5,
            self.rate_trend,
            self.pending_current,
            self.pending_growth,
            self.latency_last_3,
            self.latency_trend,
            self.sent_last_1,
            self.sent_last_3,
        ]
    }
}

// ---------------------------------------------------------------------------
// CrawlerHistory —— 历史数据 + 特征提取
// ---------------------------------------------------------------------------

/// 爬虫历史记录环形缓冲区，负责特征工程
pub struct CrawlerHistory {
    /// 历史记录队列（按时间顺序，尾部最新）
    records: VecDeque<CrawlerRoundRecord>,
    /// 最大保留记录数
    max_records: usize,
    /// 下一个轮次 ID
    next_round_id: u64,
}

impl CrawlerHistory {
    /// 创建指定容量的历史缓冲区
    pub fn new(max_records: usize) -> Self {
        Self {
            records: VecDeque::with_capacity(max_records),
            max_records,
            next_round_id: 0,
        }
    }

    /// 记录一轮结果，自动分配 round_id，超过 max_records 时弹出最旧记录
    pub fn record(&mut self, mut record: CrawlerRoundRecord) {
        record.round_id = self.next_round_id;
        self.next_round_id += 1;
        self.records.push_back(record);
        if self.records.len() > self.max_records {
            self.records.pop_front();
        }
    }

    /// 从历史记录提取 10 维特征。数据不足（< 5 轮）时返回 None
    pub fn extract_features(&self) -> Option<PredictionFeatures> {
        if self.records.len() < 5 {
            return None;
        }

        let last5 = self.last_n(5);
        let last3 = self.last_n(3);
        let last1 = self.records.back().expect("len >= 5 ensures non-empty");

        // --- 响应率特征 ---
        let rate_last_1 = last1.response_rate;
        let rate_last_3 = mean(last3.iter().map(|r| r.response_rate));
        let rate_last_5 = mean(last5.iter().map(|r| r.response_rate));
        let rate_trend = linear_slope(&last5.iter().map(|r| r.response_rate).collect::<Vec<_>>());

        // --- pending 特征 ---
        let pending_current = last1.pending_after as f64;
        let pending_after_3: Vec<f64> = last3.iter().map(|r| r.pending_after as f64).collect();
        // 最近 3 轮的平均增量 = (last - first) / (n-1)
        let pending_growth = if pending_after_3.len() >= 2 {
            let steps = (pending_after_3.len() - 1) as f64;
            (pending_after_3.last().unwrap() - pending_after_3[0]) / steps
        } else {
            0.0
        };

        // --- 延迟特征 ---
        let latency_last_3 = mean(last3.iter().map(|r| r.avg_latency_ms));
        let latency_trend =
            linear_slope(&last3.iter().map(|r| r.avg_latency_ms).collect::<Vec<_>>());

        // --- 发包数特征 ---
        let sent_last_1 = last1.packets_sent as f64;
        let sent_last_3 = mean(last3.iter().map(|r| r.packets_sent as f64));

        Some(PredictionFeatures {
            rate_last_1,
            rate_last_3,
            rate_last_5,
            rate_trend,
            pending_current,
            pending_growth,
            latency_last_3,
            latency_trend,
            sent_last_1,
            sent_last_3,
        })
    }

    /// 当前记录数
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// 取最近 n 条记录（按时间正序：最旧在前，最新在后）
    fn last_n(&self, n: usize) -> Vec<&CrawlerRoundRecord> {
        let total = self.records.len();
        let start = total.saturating_sub(n);
        self.records.iter().skip(start).collect()
    }
}

// ---------------------------------------------------------------------------
// ResponseRatePredictor —— 增量线性回归预测模型
// ---------------------------------------------------------------------------

/// 增量线性回归（SGD）预测模型，预测下一轮响应率
pub struct ResponseRatePredictor {
    /// 10 维权重向量
    weights: Vec<f64>,
    /// 偏置项
    bias: f64,
    /// 学习率
    learning_rate: f64,
    /// 累计更新次数
    update_count: u64,
    /// 开始预测所需的最小更新次数
    min_updates_for_prediction: u64,
}

impl ResponseRatePredictor {
    /// 创建新预测器（权重全零，偏置零，学习率 0.01，最小更新 50 次）
    pub fn new() -> Self {
        Self {
            weights: vec![0.0; 10],
            bias: 0.0,
            learning_rate: 0.01,
            update_count: 0,
            min_updates_for_prediction: 50,
        }
    }

    /// 预测下一轮响应率。update_count < min_updates_for_prediction 时返回 None
    pub fn predict(&self, features: &PredictionFeatures) -> Option<f64> {
        if self.update_count < self.min_updates_for_prediction {
            return None;
        }
        let fv = features.to_vector();
        let dot: f64 = self.weights.iter().zip(fv.iter()).map(|(w, f)| w * f).sum();
        Some(dot + self.bias)
    }

    /// 用真实响应率更新模型（随机梯度下降）。
    /// 预测值 = dot(weights, features) + bias，
    /// 误差 = actual - predicted，
    /// 更新 weights += lr * error * feature，bias += lr * error
    pub fn update(&mut self, features: &PredictionFeatures, actual_rate: f64) {
        let fv = features.to_vector();
        let predicted: f64 = self
            .weights
            .iter()
            .zip(fv.iter())
            .map(|(w, f)| w * f)
            .sum::<f64>()
            + self.bias;
        let error = actual_rate - predicted;
        for (w, &f) in self.weights.iter_mut().zip(fv.iter()) {
            *w += self.learning_rate * error * f;
        }
        self.bias += self.learning_rate * error;
        self.update_count += 1;
    }

    /// 置信度 0.0 ~ 1.0，基于 update_count：min(1.0, update_count / 200.0)
    pub fn confidence(&self) -> f64 {
        (self.update_count as f64 / 200.0).min(1.0)
    }

    /// 累计更新次数
    pub fn update_count(&self) -> u64 {
        self.update_count
    }
}

impl Default for ResponseRatePredictor {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// 内部工具函数
// ---------------------------------------------------------------------------

/// 计算均值，空迭代器返回 0.0
fn mean(values: impl Iterator<Item = f64>) -> f64 {
    let v: Vec<f64> = values.collect();
    if v.is_empty() {
        return 0.0;
    }
    v.iter().sum::<f64>() / v.len() as f64
}

/// 对等间距 x = 0,1,2,...,n-1 的序列做最小二乘线性回归，返回斜率。
/// 数据点不足 2 个或分母为 0 时返回 0.0。
fn linear_slope(ys: &[f64]) -> f64 {
    let n = ys.len() as f64;
    if ys.len() < 2 {
        return 0.0;
    }
    let sum_x: f64 = (0..ys.len()).map(|i| i as f64).sum();
    let sum_y: f64 = ys.iter().sum();
    let sum_xy: f64 = ys.iter().enumerate().map(|(i, &y)| i as f64 * y).sum();
    let sum_x2: f64 = (0..ys.len()).map(|i| (i as f64) * (i as f64)).sum();
    let denom = n * sum_x2 - sum_x * sum_x;
    if denom == 0.0 {
        return 0.0;
    }
    (n * sum_xy - sum_x * sum_y) / denom
}

// ---------------------------------------------------------------------------
// 单元测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// 浮点近似比较（容差 1e-6）
    fn approx_eq(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-6
    }

    /// 构造一条测试用爬虫记录（round_id 由 record() 自动分配）
    fn make_record(rate: f64, latency: f64, pending_after: usize, sent: u64) -> CrawlerRoundRecord {
        CrawlerRoundRecord {
            round_id: 0,
            timestamp: Instant::now(),
            packets_sent: sent,
            packets_responded: 0,
            response_rate: rate,
            avg_latency_ms: latency,
            pending_before: 0,
            pending_after,
            socket_idx: 0,
            nodes_quality: 0.0,
        }
    }

    /// 构造一组归一化样本特征（所有值在 0~1 范围，用于预测器测试）
    fn sample_features() -> PredictionFeatures {
        PredictionFeatures {
            rate_last_1: 0.9,
            rate_last_3: 0.8,
            rate_last_5: 0.7,
            rate_trend: 0.1,
            pending_current: 0.18,
            pending_growth: 0.02,
            latency_last_3: 0.13,
            latency_trend: 0.01,
            sent_last_1: 0.14,
            sent_last_3: 0.13,
        }
    }

    #[test]
    fn test_crawler_history_record_and_len() {
        let mut hist = CrawlerHistory::new(3);
        assert!(hist.is_empty());
        assert_eq!(hist.len(), 0);

        for i in 0..5u64 {
            hist.record(make_record(0.5 + i as f64 * 0.01, 100.0, 100, 1000));
        }
        // 超过 max_records=3，只保留最新 3 条
        assert_eq!(hist.len(), 3);
        assert!(!hist.is_empty());

        // round_id 应为 2, 3, 4（前两条已弹出）
        let ids: Vec<u64> = hist.records.iter().map(|r| r.round_id).collect();
        assert_eq!(ids, vec![2, 3, 4]);
    }

    #[test]
    fn test_extract_features_insufficient_data() {
        let mut hist = CrawlerHistory::new(100);

        // 4 轮 → None
        for _ in 0..4 {
            hist.record(make_record(0.5, 100.0, 100, 1000));
        }
        assert!(hist.extract_features().is_none());

        // 5 轮 → Some
        hist.record(make_record(0.5, 100.0, 100, 1000));
        assert!(hist.extract_features().is_some());
    }

    #[test]
    fn test_extract_features_values() {
        let mut hist = CrawlerHistory::new(100);

        // 5 轮线性递增数据
        let rates = [0.5, 0.6, 0.7, 0.8, 0.9];
        let latencies = [100.0, 110.0, 120.0, 130.0, 140.0];
        let pendings = [100, 120, 140, 160, 180];
        let sents = [1000, 1100, 1200, 1300, 1400];

        for i in 0..5 {
            hist.record(make_record(rates[i], latencies[i], pendings[i], sents[i]));
        }

        let f = hist
            .extract_features()
            .expect("5 rounds should yield features");

        // rate_last_1 = 0.9
        assert!(
            approx_eq(f.rate_last_1, 0.9),
            "rate_last_1 got {}",
            f.rate_last_1
        );
        // rate_last_3 = (0.7+0.8+0.9)/3 = 0.8
        assert!(
            approx_eq(f.rate_last_3, 0.8),
            "rate_last_3 got {}",
            f.rate_last_3
        );
        // rate_last_5 = (0.5+0.6+0.7+0.8+0.9)/5 = 0.7
        assert!(
            approx_eq(f.rate_last_5, 0.7),
            "rate_last_5 got {}",
            f.rate_last_5
        );
        // rate_trend: y=[0.5,0.6,0.7,0.8,0.9], x=[0,1,2,3,4] → slope=0.1
        assert!(
            approx_eq(f.rate_trend, 0.1),
            "rate_trend got {}",
            f.rate_trend
        );
        // pending_current = 180.0
        assert!(
            approx_eq(f.pending_current, 180.0),
            "pending_current got {}",
            f.pending_current
        );
        // pending_growth = (180-140)/2 = 20.0
        assert!(
            approx_eq(f.pending_growth, 20.0),
            "pending_growth got {}",
            f.pending_growth
        );
        // latency_last_3 = (120+130+140)/3 = 130.0
        assert!(
            approx_eq(f.latency_last_3, 130.0),
            "latency_last_3 got {}",
            f.latency_last_3
        );
        // latency_trend: y=[120,130,140], x=[0,1,2] → slope=10.0
        assert!(
            approx_eq(f.latency_trend, 10.0),
            "latency_trend got {}",
            f.latency_trend
        );
        // sent_last_1 = 1400.0
        assert!(
            approx_eq(f.sent_last_1, 1400.0),
            "sent_last_1 got {}",
            f.sent_last_1
        );
        // sent_last_3 = (1200+1300+1400)/3 = 1300.0
        assert!(
            approx_eq(f.sent_last_3, 1300.0),
            "sent_last_3 got {}",
            f.sent_last_3
        );
    }

    #[test]
    fn test_predictor_cold_start() {
        let pred = ResponseRatePredictor::new();
        let f = sample_features();

        // 0 次更新 → predict 返回 None
        assert!(pred.predict(&f).is_none());
        assert_eq!(pred.update_count(), 0);
        assert!(approx_eq(pred.confidence(), 0.0));
    }

    #[test]
    fn test_predictor_update_and_predict() {
        let mut pred = ResponseRatePredictor::new();
        let f = sample_features();

        // 60 次更新（> 50）→ predict 返回 Some
        for _ in 0..60 {
            pred.update(&f, 0.85);
        }
        assert_eq!(pred.update_count(), 60);

        let prediction = pred.predict(&f).expect("should predict after 60 updates");
        // 预测值应在合理范围（响应率 0.0 ~ 2.0）
        assert!(
            prediction > 0.0 && prediction < 2.0,
            "prediction out of reasonable range: {}",
            prediction
        );
    }

    #[test]
    fn test_predictor_confidence() {
        let mut pred = ResponseRatePredictor::new();

        // 0 次 → 0.0
        assert!(approx_eq(pred.confidence(), 0.0));

        // 50 次 → 50/200 = 0.25
        for _ in 0..50 {
            pred.update(&sample_features(), 0.8);
        }
        assert!(
            approx_eq(pred.confidence(), 0.25),
            "confidence got {}",
            pred.confidence()
        );

        // 200 次 → 200/200 = 1.0
        for _ in 0..150 {
            pred.update(&sample_features(), 0.8);
        }
        assert!(
            approx_eq(pred.confidence(), 1.0),
            "confidence got {}",
            pred.confidence()
        );

        // 400 次 → min(1.0, 400/200) = 1.0（封顶）
        for _ in 0..200 {
            pred.update(&sample_features(), 0.8);
        }
        assert!(
            approx_eq(pred.confidence(), 1.0),
            "confidence got {}",
            pred.confidence()
        );
    }

    #[test]
    fn test_prediction_features_to_vector() {
        let f = sample_features();
        let v = f.to_vector();

        assert_eq!(v.len(), 10);
        assert!(approx_eq(v[0], f.rate_last_1));
        assert!(approx_eq(v[1], f.rate_last_3));
        assert!(approx_eq(v[2], f.rate_last_5));
        assert!(approx_eq(v[3], f.rate_trend));
        assert!(approx_eq(v[4], f.pending_current));
        assert!(approx_eq(v[5], f.pending_growth));
        assert!(approx_eq(v[6], f.latency_last_3));
        assert!(approx_eq(v[7], f.latency_trend));
        assert!(approx_eq(v[8], f.sent_last_1));
        assert!(approx_eq(v[9], f.sent_last_3));
    }
}
