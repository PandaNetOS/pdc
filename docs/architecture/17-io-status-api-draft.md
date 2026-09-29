# `/api/v1/io/status` 接口设计草案（B4 落地前评审稿）

> 状态：**草案，未接线**。本文件只描述"将来在哪里插、插什么、字段语义是什么"，
> **不要把本文中的 handler 代码直接写进 `src/data_plane/rest_api.rs`**。
> 上游依据：[15-adaptive-io-implementation-plan.md](./15-adaptive-io-implementation-plan.md) §B4（字段表与事件口径）、§B1（压力口径）、§B2（令牌桶指标）。
> 本文所有行号基于当前工作区 `feat/adaptive-io-a` 分支的实际文件（2026-09-28 侦察）。

---

## 1. 路由注册位置（rest_api.rs 现状 + 计划插入点）

路由统一在 `src/data_plane/rest_api.rs` 的自由函数 `pub fn routes(state: AppState) -> Router` 里以**链式 `.route(...)`** 注册，函数起点在 **`rest_api.rs:288`**：

```rust
// src/data_plane/rest_api.rs:288
pub fn routes(state: AppState) -> Router {
    let event_bus = state.event_bus.clone();
    Router::new()
        .route("/health", get(health_handler))                       // :291
        .route("/api/v1/stats", get(stats_handler))                 // :292
        // ... 中间若干 /api/v1/* 路由 ...
        .route("/api/v1/relay/stats", get(relay_stats_handler))     // :324  ← 最后一条 /api/v1/*
        .route("/metrics", get(crate::data_plane::metrics::metrics_handler)) // :325
        .route("/ws", get(crate::data_plane::ws::ws_handler))       // :326
        .with_state(state)                                          // :327
        .layer(Extension(event_bus))
        .layer(CorsLayer::permissive())
}
```

**计划插入点**：在当前 **第 324 行**（`/api/v1/relay/stats`）之后、第 325 行（`/metrics`）之前，插入一行：

```rust
.route("/api/v1/io/status", get(io_status_handler))
```

即新路由落在**新的第 325 行**；原 325/326/327 行整体下移一行（`/metrics`→326、`/ws`→327、`.with_state(state)`→328）。
理由：把业务 API 路由（`/api/v1/*`）集中在一起，`/metrics`、`/ws` 作为基础设施路由留在链尾，与现有排列习惯一致。

> 注：15 号文档 §B4 写的是"挨着现有 `get(...)` 列表，`rest_api.rs:303` 附近"——那是旧文件版本的行号；以本文上面这份基于当前工作区的行号为准。

### 1.1 AppState 扩展点

`AppState` **不在** `rest_api.rs` 里定义，而在 `src/data_plane/mod.rs:39-88`。当前最后一个字段是第 87 行：

```rust
// src/data_plane/mod.rs:86-88
    /// 统计快照（后台任务定期更新，API 只读）
    pub stats_snapshot: Arc<stats_snapshot::StatsSnapshot>,
}
```

按 15 号 §B4 第 1 点，接线时需在**第 87 行之后、第 88 行 `}` 之前**新增一个字段：

```rust
    /// IO 调度器句柄（B4：/api/v1/io/status 只读它的统计快照；未启用/内存库为 None）
    pub io_scheduler: Option<Arc<crate::storage::IoScheduler>>,
```

`main.rs` 中 `io_scheduler` 在 `main.rs:368` 已构造，早于 `AppState` 构造点 `main.rs:924`，顺序安全（15 号 §B4 已核对）。

---

## 2. `GET /api/v1/io/status` 响应 Schema

字段逐列对齐 15 号 §B4 给出的 JSON 示例（下例数值为样例，非真实采样）：

```json
{
  "queue": { "requests": 3, "rows": 12400 },
  "pressure": { "queue": 0.0, "latency": 0.42, "level": 0.42 },
  "latency_ms": { "ewma": 12.3, "p50": 9.1, "p95": 41.7, "p99": 88.0 },
  "budget": { "per_tick_rows": 5000, "per_tick_requests": 64, "import_active": true, "import_budget": 40000 },
  "checkpoint": { "mode": "passive", "inflight": false, "last_ms": 812, "ewma_ms": 640,
                  "slow_total": 2, "backoff_secs": 0, "takeover": true },
  "wal_bytes": 41231360,
  "counters": { "submitted": 1, "executed": 1, "batches": 1, "rejected": 0, "preemptions": 0 }
}
```

### 2.1 逐字段表

| 字段路径 | 类型 | 单位 | 含义 |
|---|---|---|---|
| `queue.requests` | `u64` | 个 | 当前写队列中待处理的请求条数（旧口径，仅用于队列硬上限判断） |
| `queue.rows` | `u64` | 行 | 当前队列中待处理总行数（Σ `size_hint`，B1 行口径；提交 +、执行 -） |
| `pressure.queue` | `f64` | 无量纲 [0,1] | 队列水位压力 = `clamp01((rows - low_rows)/(high_rows - low_rows))` |
| `pressure.latency` | `f64` | 无量纲 [0,1] | 耗时压力 = `clamp01((lat_ewma_ms - target_ms)/(slow_ms - target_ms))` |
| `pressure.level` | `f64` | 无量纲 [0,1] | 背压等级 = `max(pressure.queue, pressure.latency)`，即对外暴露的 `backpressure_level()` |
| `latency_ms.ewma` | `f64` | 毫秒 | writer_loop 单批实测耗时 EWMA（α = `adaptive_latency_alpha`） |
| `latency_ms.p50` | `f64` | 毫秒 | 固定桶直方图（1/2/5/10/20/50/100/200/500/1000/5000 ms）的中位数 |
| `latency_ms.p95` | `f64` | 毫秒 | 同上直方图的 95 分位 |
| `latency_ms.p99` | `f64` | 毫秒 | 同上直方图的 99 分位 |
| `budget.per_tick_rows` | `u64` | 行/tick | 当前每 tick 行预算（平滑上限，C1 画像/C2 AIMD 会改写它） |
| `budget.per_tick_requests` | `u64` | 请求/tick | 当前每 tick 请求数上限（旧 `max_requests_per_tick`） |
| `budget.import_active` | `bool` | — | bootstrap 导入窗口当前是否活跃（TTL 内） |
| `budget.import_budget` | `u64` | 行 | 导入窗口内当前行预算上限（AIMD 调整后值，非配置常数） |
| `checkpoint.mode` | `string` enum | — | 最近一次 checkpoint 模式：`"passive"` 或 `"truncate"` |
| `checkpoint.inflight` | `bool` | — | 是否有 checkpoint 正在执行（单飞：同一时刻恒 ≤1） |
| `checkpoint.last_ms` | `u64` | 毫秒 | 最近一次 checkpoint 实际耗时 |
| `checkpoint.ewma_ms` | `f64` | 毫秒 | checkpoint 耗时 EWMA（慢盘熔断判定输入） |
| `checkpoint.slow_total` | `u64` | 次 | 累计"慢"checkpoint 次数（单次 > `checkpoint_slow_ms`） |
| `checkpoint.backoff_secs` | `u64` | 秒 | 当前熔断退避窗口剩余秒数（0 = 不在退避） |
| `checkpoint.takeover` | `bool` | — | 应用是否接管 WAL checkpoint（启动时是否置 `wal_autocheckpoint=0`） |
| `wal_bytes` | `u64` | 字节 | `<db>-wal` 当前文件尺寸（无锁 `fs::metadata`，O(1)） |
| `counters.submitted` | `u64` | 次 | 累计提交到写队列的请求数 |
| `counters.executed` | `u64` | 次 | 累计已执行完成的写入操作数 |
| `counters.batches` | `u64` | 批 | writer_loop 累计执行批次数 |
| `counters.rejected` | `u64` | 次 | 累计被拒绝/限流的请求数 |
| `counters.preemptions` | `u64` | 次 | 累计被高优先级抢占的次数 |

### 2.2 扩展预留（B2 令牌桶指标，本草案不进首版 schema）

15 号 §B2 明确要求 `token_starved_ticks` / `rows_deferred_by_tokens` / `tokens_available` 也落到本接口，否则"限流生效无法自证"。首版 schema 落地时建议新增并列子对象 `token_bucket`：

| 预留字段路径 | 类型 | 单位 | 含义 |
|---|---|---|---|
| `token_bucket.enabled` | `bool` | — | 令牌桶总开关（默认 false 灰度） |
| `token_bucket.rate_rows_per_sec` | `u64` | 行/秒 | 当前补充速率 |
| `token_bucket.tokens_available` | `u64` | 行 | 当前桶内可用令牌余量 |
| `token_bucket.starved_ticks` | `u64` | 次 | 累计因令牌不足而空转/降级的 tick 数 |
| `token_bucket.rows_deferred` | `u64` | 行 | 累计因令牌不足而留在队列的行数 |

---

## 3. 伪 Rust handler 草案

> ⚠️ **这是草案，不要写进 `rest_api.rs`。** 仅用于评审 `State<AppState>` 取值方式与 `None` 降级分支的形态。
> 真实接线时 handler 体应改为调用 `io_scheduler.status_snapshot()`（一个 O(1) 克隆快照的原子方法，B4 第 4 点要求新增），而不是在 handler 里逐字段加锁。

```rust
// ===== 草案开始（请勿落到 rest_api.rs）=====
use axum::Json;
use std::sync::Arc;

/// GET /api/v1/io/status
async fn io_status_handler(State(state): State<AppState>) -> Response {
    // 从 State<AppState> 取 B4 扩展字段；未注入（内存库 / 老配置 / 开关关闭）时走降级分支
    let sched = match &state.io_scheduler {
        Some(s) => s.clone(),
        None => {
            // 降级：200 + enabled=false 的最小报文，而不是 500/404，
            // 让监控面板在未启用 IO 调度时也能稳定拉到一个结构化 JSON。
            return Json(serde_json::json!({
                "enabled": false,
                "reason": "io_scheduler not injected (memory db / disabled)"
            }))
            .into_response();
        }
    };

    // 取一次 O(1) 快照（B4 新增的原子方法；内部只读原子量 + fs::metadata(wal)）
    let snap = sched.io_status_snapshot();

    Json(serde_json::json!({
        "enabled": true,
        "queue": {
            "requests": snap.queue_requests,
            "rows": snap.queue_rows,
        },
        "pressure": {
            "queue": snap.pressure_queue,
            "latency": snap.pressure_latency,
            "level": snap.pressure_level,
        },
        "latency_ms": {
            "ewma": snap.latency_ewma_ms,
            "p50": snap.latency_p50_ms,
            "p95": snap.latency_p95_ms,
            "p99": snap.latency_p99_ms,
        },
        "budget": {
            "per_tick_rows": snap.per_tick_rows,
            "per_tick_requests": snap.per_tick_requests,
            "import_active": snap.import_active,
            "import_budget": snap.import_budget,
        },
        "checkpoint": {
            "mode": format!("{:?}", snap.checkpoint_mode).to_lowercase(),
            "inflight": snap.checkpoint_inflight,
            "last_ms": snap.checkpoint_last_ms,
            "ewma_ms": snap.checkpoint_ewma_ms,
            "slow_total": snap.checkpoint_slow_total,
            "backoff_secs": snap.checkpoint_backoff_secs,
            "takeover": snap.checkpoint_takeover,
        },
        "wal_bytes": snap.wal_bytes,
        "counters": {
            "submitted": snap.cnt_submitted,
            "executed": snap.cnt_executed,
            "batches": snap.cnt_batches,
            "rejected": snap.cnt_rejected,
            "preemptions": snap.cnt_preemptions,
        },
    }))
    .into_response()
}
// ===== 草案结束 =====
```

---

## 4. 指标迟滞 / 边沿触发伪码

15 号 §B4 第 3 点要求：背压档位跨阈值时才 `event_bus.publish`，**避免每 tick 刷事件**。
规则：**只在 `pressure.level` 边沿翻转（穿越 0.5）时推一次事件，且带迟滞带与连续确认计数**，不在迟滞带内反复横跳。

```text
# 状态（进程内单例，放在 IoScheduler 里）
prev_level_bucket : enum { Low, High } = Low      # 上次发布时所处档位
consecutive_samples : u32 = 0                     # 当前档位已连续采样数
HYSTERESIS_LOW   = 0.4   # 从 High 跌回 Low 的下沿（低于 0.4 且连续 N 次才算恢复）
HYSTERESIS_HIGH  = 0.5   # 从 Low 升 High 的上沿（高于 0.5 且连续 N 次才算恶化）
CONFIRM_N        = 3     # 连续 N 个 tick 跨阈值才翻转，抗抖动

# 每个决策 tick（checkpoint_tick_ms / writer_loop 末）执行：
on_tick(level: f64):
    if prev_level_bucket == Low:
        if level >= HYSTERESIS_HIGH:
            consecutive_samples += 1
            if consecutive_samples >= CONFIRM_N:
                publish_event(IoDegraded {
                    from: "low", to: "high",
                    level, at: now()
                })
                prev_level_bucket = High
                consecutive_samples = 0
        else:
            consecutive_samples = 0          # 掉出带上沿，计数清零，不发布
    else:  # High
        if level <= HYSTERESIS_LOW:
            consecutive_samples += 1
            if consecutive_samples >= CONFIRM_N:
                publish_event(IoRecovered {
                    from: "high", to: "low",
                    level, at: now()
                })
                prev_level_bucket = Low
                consecutive_samples = 0
        else:
            consecutive_samples = 0
    # 落在 (0.4, 0.5) 迟滞带内：什么都不做，不发事件、不改档位

# 推送阈值小结：
#   - 上升沿发布：level 连续 3 次 >= 0.5  → IoDegraded
#   - 下降沿发布：level 连续 3 次 <= 0.4  → IoRecovered
#   - 0.4 < level < 0.5：迟滞带内静默，不发事件
#   - /api/v1/io/status 本身是轮询快照，不受迟滞影响，每次都返回当前真实 level
```

> 同样的"迟滞 + 连续 N 次确认"模式复用于 C1 磁盘运行期再分类（15 号 §C1：连续 N 次跨阈值才切 SSD/HDD 档），避免一次偶发慢盘就把参数档位来回切。

---

## 5. 落地顺序提醒（与硬约束对齐）

1. 本文件只是草案；接线需在能跑 cargo 的环境进行。
2. 接线顺序：先扩 `AppState.io_scheduler` 字段（§1.1）→ 再在 `rest_api.rs:325` 插路由 → 再实现 `io_status_snapshot()` 原子方法 → 最后接迟滞发布（§4）。
3. 首版 schema 按 §2.1 落地；§2.2 令牌桶子对象随 B2 接线时再补，不阻塞 B4 首版。
