# PDC 智能控制中心（ICC）升级方案

> 文档编号：PDC-ARCH-010
> 版本：v1.0
> 日期：2026-09-14
> 状态：待确认

## 一、现状与问题

### 当前架构

pdc intelligence 模块当前是"资源持有者 + 决策者 + 执行者"三重身份混合体：

- TaskScheduler 直接调度 25+ 个周期性业务任务
- ResourceMonitor 已存在但 `refresh()` 未被主循环调用，资源感知失效
- 评分引擎（ScoreEngine）直接持有 storage 引用
- 无意图抽象，任务间无依赖编排，无执行反馈闭环

### 核心问题

1. **决策与执行耦合**：本地策略硬编码在各模块，无法切换 pk 托管模式
2. **资源持有僵化**：intelligence 直接依赖 storage/data_plane，无法注入远程资源
3. **无降级能力**：pk 失联时无保守策略兜底
4. **执行无反馈**：任务执行结果不回传，无法形成闭环
5. **TaskScheduler 健壮性不足**：panic 令牌桶泄漏、seq 排序错误、超时不重试、依赖集合内存泄漏

---

## 二、目标架构

### 核心定位

ICC = **意图执行中枢**（"小脑"），接收决策源意图 → 编排资源达成意图 → 回传执行结果。自身不产生全局决策，但保留独立运行时的本地决策能力。

### 架构分层

```
决策源层  PkDecisionSource(托管) / LocalDecisionSource(独立/降级)
    ↓ Intent（统一意图，唯一契约）
执行层  IntentParser → TaskScheduler → ResourceOrchestrator
    ↓ ResourceProvider（接口注入）
资源层  PeerStore / Discoverer / ScoreStore（远程 or 本地，按模式注入）
    ↓
监控层  ResourceMonitor（双模式角色） / ExecutionFeedback（闭环回传）
```

### 核心组件清单

| 组件 | 职责 | 状态 |
|---|---|---|
| DecisionSource | 产出意图 | 新增抽象 |
| Intent | 统一意图结构 | 新增 |
| IntentParser | 意图解析为动作序列 | 新增 |
| TaskScheduler | 执行引擎 | 保留，语义重定义 |
| ResourceOrchestrator | 按意图编排资源调用 | 新增 |
| ResourceProvider | 资源抽象接口 | 新增 |
| ResourceMonitor | 资源感知，双模式角色 | 保留，改造 |
| ExecutionFeedback | 执行结果回传 | 新增 |
| ScoreEngine | 评分算法 | 保留，数据外置 |

### 三种运行模式

| 模式 | 触发条件 | 决策源 | 资源实现 | Monitor 角色 |
|---|---|---|---|---|
| **Managed（托管）** | pk 在线 | PkDecisionSource | RemoteProvider | 低频刷新(30s)，仅上报 |
| **Standalone（独立）** | 显式配置 / pk 长期不可用 | LocalDecisionSource | LocalProvider | 完整刷新(5s)，参与决策 |
| **Degraded（降级）** | pk 心跳超时 | LocalDecisionSource(保守) | LocalProvider(保守) | 完整刷新(5s)，安全兜底 |

### 模式切换机制

```
                    pk 首次下发意图
    Standalone ──────────────────────► Managed
         ▲                                │
         │ 显式配置                        │ pk 心跳超时 N 次
         │                                ▼
         └────────────────────────── Degraded
                      pk 心跳恢复
```

切换时由 `ModeController` 统一管理：

- Standalone → Managed：保留本地评分数据，切换远程 Provider，ResourceMonitor 降频
- Managed → Degraded：自动切换本地 Provider，启用保守策略，记录待上报结果
- Degraded → Managed：批量上报降级期间执行结果，恢复远程 Provider
- Managed → Standalone：显式配置触发，切换本地 Provider，恢复完整刷新

---

## 三、核心抽象设计

### 3.1 意图（Intent）

```rust
struct Intent {
    id: String,
    goal: Goal,                    // 目标：如 EnsurePeerCoverage
    target: Target,                // 目标对象：如 infohash + min_peers
    constraints: Constraints,      // 约束：deadline、max_cost、quality
    context: Context,              // 上下文：requester、priority、trace_id
    source: DecisionSourceType,    // 来源：Pk / Local / Degraded
}
```

设计原则：

- **目标式而非命令式**：说"确保 peer 覆盖"，不说"执行 DHT 查询"
- **约束驱动而非步骤驱动**：说"延迟低于 3 秒"，不说"先查缓存再查 DHT"
- **可合并可去重**：相同 goal + target 的意图可合并

### 3.2 决策源（DecisionSource）

```rust
trait DecisionSource {
    async fn next_intent(&self) -> Option<Intent>;
    async fn report_feedback(&self, feedback: ExecutionFeedback);
    fn mode(&self) -> RunMode;
}
```

- **PkDecisionSource**：从 pk 通道接收意图，通过通道回传反馈
- **LocalDecisionSource**：从 ResourceMonitor + 本地策略生成意图，反馈写入本地日志和指标

### 3.3 资源提供者（ResourceProvider）

```rust
trait ResourceProvider {
    async fn query_peers(&self, infohash: &str, limit: usize) -> Result<Vec<Peer>>;
    async fn write_peers(&self, infohash: &str, peers: &[Peer]) -> Result<()>;
    async fn query_score(&self, peer: &Peer) -> Result<f64>;
    async fn write_score(&self, peer: &Peer, score: f64) -> Result<()>;
}
```

- **RemoteProvider**：托管模式下注入，通过 RPC 调用独立资源服务
- **LocalProvider**：独立/降级模式下注入，调用本地 storage 和 data_plane

### 3.4 执行反馈（ExecutionFeedback）

```rust
struct ExecutionFeedback {
    intent_id: String,
    status: ExecutionStatus,        // Completed / Partial / Failed
    achieved: Achievement,          // 达成度：peer_count、quality
    path: Vec<ExecutionStep>,       // 执行路径
    latency_ms: u64,
    resource_calls: Vec<ResourceCall>,
    errors: Vec<ExecutionError>,
    mode: RunMode,
}
```

反馈分级：Critical 实时、Normal 批量、Background 聚合。

---

## 四、执行流程

### 4.1 托管模式

```
pk 大脑
  │ 下发意图 ensure_peer_coverage(infohash=abc, min_peers=50)
  ▼
PkDecisionSource → Intent
  ▼
IntentParser
  │ 解析为动作序列：query_cache → parallel(dht, tracker) → aggregate → score_sort → write
  ▼
TaskScheduler
  │ 按优先级/截止时间调度，检查并发令牌
  ▼
ResourceOrchestrator
  │ 通过 RemoteProvider 调用资源服务，超时控制、重试、降级
  ▼
ExecutionFeedback → 回传 pk
```

### 4.2 独立模式

```
ResourceMonitor
  │ 刷新 CPU/内存，输出 ResourceState
  ▼
LocalDecisionSource
  │ 根据资源状态 + 本地策略生成意图
  ▼
Intent (source = Local) → IntentParser → TaskScheduler → ResourceOrchestrator
  │ 通过 LocalProvider 调用本地资源
  ▼
ExecutionFeedback → 写入本地日志和指标
```

### 4.3 降级模式

```
pk 心跳超时 → ModeController 切换到 Degraded
  ▼
LocalDecisionSource（保守策略）
  │ 只生成 Critical 意图，资源选择偏好：缓存优先、少并发、短超时
  ▼
执行 → ExecutionFeedback 缓冲到 pending_feedbacks 队列
  ▼
pk 心跳恢复 → 批量上报 → 切换回 Managed
```

---

## 五、ResourceMonitor 双模式设计

```rust
enum MonitorMode {
    Standalone { interval: 5s },    // 完整刷新，参与决策
    Managed { interval: 30s },      // 低频刷新，仅上报
    Degraded { interval: 5s },      // 完整刷新，安全兜底
}
```

各模式角色：

- **Standalone**：每轮调度前刷新，ResourceState 直接参与意图准入判断。CPU > 80% 时延迟 P2/P3 意图，> 95% 时只执行 Critical
- **Managed**：低频刷新，数据打包进 ExecutionFeedback 上报 pk。不参与本地决策，但保留快照用于降级切换连续性
- **Degraded**：完整刷新，启用保守策略。过载时暂停 Background 意图，避免系统崩溃

> **关键修复**：`ResourceMonitor::refresh()` 之前未被主循环调用，导致资源感知完全失效。三种模式下 refresh 均须由主循环或独立监控任务周期性调用，确保快照时效性。

---

## 六、TaskScheduler 重新定位

### 6.1 语义重定义

| 维度 | 旧语义 | 新语义 |
|---|---|---|
| 调度对象 | pdc 内部定时任务 | 意图执行动作 |
| 优先级 | P0-P3 内部任务 | Critical/Important/Normal/Background 意图 |
| 依赖 | 任务间依赖 | 动作间依赖 + 意图间依赖 |
| 并发控制 | 全量任务 ≤ 2 | 按意图类型分级限流 |
| 资源感知 | 内部 ResourceMonitor | 模式感知：托管不用，独立/降级用 |
| 失败重试 | 指数退避 | 保留，超时纳入重试 |

### 6.2 必须修复的健壮性问题

1. **panic 令牌桶泄漏**：用 `catch_unwind` 包裹动作执行，确保 `running_full_tasks` 在任何情况下释放
2. **seq 排序方向**：修正为 `other.seq.cmp(&self.seq)`，保证先安排的意图先执行
3. **超时纳入重试**：Critical/Important 意图超时后进入重试路径，不等到下个周期
4. **依赖集合清理**：`completed_dependencies` 定期清理已不存在意图的 ID，避免长期内存增长

---

## 七、与 pdc 其他模块的边界

| 模块 | 关系 | 接口 |
|---|---|---|
| control_plane | 托管模式下作为意图中转站 | Intent 接收 + ExecutionFeedback 回传 |
| data_plane | 通过 ResourceProvider 调用 | query_peers / write_peers |
| discoverers | 作为可调用资源 | ResourceProvider 的发现器实现 |
| storage | 通过 ResourceProvider 调用 | ScoreStore / PeerStore 接口 |
| federation | 执行状态同步 | 同步 ExecutionFeedback 摘要，不同步 peer 数据 |
| services | 对外 API 不变 | 内部走 intelligence 编排 |

> **关键变化**：intelligence 从"资源持有者 + 决策者 + 执行者"三重身份，收缩为"意图解析者 + 执行编排者"双重身份。

---

## 八、分阶段实施路径

### 阶段0：前置基础设施（进行中）

**目标**：所有周期性行为纳管，为意图化改造铺路

- TaskScheduler 提调一切周期性业务任务
- 白名单声明规范 `[ALLOWED-INTERVAL]` / `[ALLOWED-SLEEP]`
- 合规检查第15项强制阻断未声明的 interval/sleep
- tick 降到 100ms

**验收**：cargo check 通过 + 合规检查第15项 PASS + 无未纳管的业务定时任务

### 阶段1：抽象定义，行为不变

**目标**：引入核心抽象，不改变现有运行行为

- 定义 `Intent` 结构体（id/goal/target/constraints/context/source）
- 定义 `DecisionSource` trait（next_intent / report_feedback / mode）
- 定义 `ResourceProvider` trait（query_peers / write_peers / query_score / write_score）
- 定义 `ExecutionFeedback` 结构体
- 定义 `RunMode` 枚举和 `Goal`/`Target`/`Constraints` 类型

**验收**：抽象编译通过，现有行为零变化，单元测试覆盖抽象定义

### 阶段2：ResourceMonitor 双模式 + ModeController

**目标**：修复资源感知失效，建立模式切换基础

- 修复 `ResourceMonitor::refresh()` 被主循环周期性调用（关键 bug）
- 引入 `MonitorMode` 枚举（Standalone 5s / Managed 30s / Degraded 5s）
- 实现 `ModeController`：统一管理模式切换，保留切换前快照
- Standalone 模式：CPU>80% 延迟 P2/P3，>95% 只执行 Critical
- Managed 模式：低频刷新，数据打包进 ExecutionFeedback 上报
- Degraded 模式：完整刷新 + 保守策略，过载时暂停 Background

**验收**：三种模式下 refresh 均被调用，模式切换无状态丢失，单元测试覆盖切换逻辑

### 阶段3：IntentParser + ResourceOrchestrator 接入

**目标**：从"任务调度"升级为"意图编排"

- 实现 `IntentParser`：意图 → 动作序列
- 实现 `ResourceOrchestrator`：按动作序列通过 ResourceProvider 调用资源，超时控制/重试/降级
- control_plane 改为意图中转站（接收 pk 意图 → 注入 PkDecisionSource）
- 现有 25+ 周期性任务包装为 `LocalDecisionSource` 产生的意图
- TaskScheduler 调度对象从"任务"变为"意图动作"

**验收**：pk 下发意图可端到端执行，本地周期性任务通过 LocalDecisionSource 正常运行，集成测试覆盖

### 阶段4：TaskScheduler 健壮性修复

**目标**：修复已知健壮性问题

- panic 令牌桶泄漏：`catch_unwind` 包裹动作执行
- seq 排序方向修正
- 超时纳入重试：Critical/Important 意图超时立即进入重试路径
- 依赖集合清理：`completed_dependencies` 定期清理
- 按意图类型分级限流（替代全量 ≤2 的粗粒度控制）

**验收**：故障注入测试通过（panic/超时/依赖循环），内存稳定

### 阶段5：资源外置 + 反馈闭环

**目标**：完成托管模式闭环

- 实现 `RemoteProvider`：通过 pnos-sdk RPC 调用独立资源服务
- 实现 `LocalProvider`：封装现有 storage/data_plane 调用
- ExecutionFeedback 分级回传：Critical 实时 / Normal 批量 / Background 聚合
- 降级期间 feedback 缓冲到本地 SQLite，pk 恢复后批量重放
- 联邦同步 ExecutionFeedback 摘要（不同步 peer 数据）

**验收**：托管模式端到端闭环（pk 下发 → 执行 → 反馈回传），降级切换无结果丢失

---

## 九、关键技术决策

| 决策点 | 选择 | 理由 |
|---|---|---|
| 意图粒度 | 目标式（ensure_peer_coverage），非命令式 | 决策源与执行层解耦，解析器可优化路径 |
| 资源注入 | trait 对象 + 按模式构造 | 托管用远程，独立用本地，切换零代码改动 |
| 模式切换 | ModeController 统一管理，快照保留 | 切换无状态丢失，降级可恢复 |
| 反馈分级 | Critical实时/Normal批量/Background聚合 | 避免反馈风暴淹没 pk |
| 降级持久化 | SQLite 存储 pending_feedbacks | pk 恢复后可重放，不丢结果 |
| 联邦边界 | 只同步 ExecutionFeedback 摘要 | 不同步 peer 数据，避免冲突 |
| TaskScheduler 演进 | 先纳管任务，再升级为意图动作 | 渐进式，不破坏现有稳定性 |

---

## 十、与当前工作的衔接

```
当前（阶段0）正在做：
  TaskScheduler 纳管所有周期性任务
  ↓ 完成后
阶段1-2：抽象定义 + Monitor双模式（可并行开发）
  ↓
阶段3：意图化改造（最大改动，需冻结接口）
  ↓
阶段4：健壮性修复（可与阶段3并行）
  ↓
阶段5：资源外置 + 闭环（依赖 pnos-sdk RPC 能力）
```

**关键里程碑**：阶段3完成后，ICC 从"任务调度器"正式升级为"意图执行中枢"，pk 可开始下发意图。

---

## 十一、风险与应对

| 风险 | 应对 |
|---|---|
| 意图解析器变成新决策者 | 意图中指定约束，解析器只在约束下选择资源 |
| 反馈风暴淹没 pk | 分级回传：Critical 实时、Normal 批量、Background 聚合 |
| 模式切换时状态丢失 | ModeController 保留切换前的快照，切换后恢复 |
| 降级期间结果丢失 | pending_feedbacks 持久化到本地 SQLite，pk 恢复后重放 |
| 资源发现延迟 | 维护资源热缓存，常用资源能力常驻内存 |
| 联邦执行状态冲突 | 意图 ID 全局唯一，联邦同步时按 ID 去重 |
| 阶段3改造面过大 | 先包装现有任务为 LocalDecisionSource 意图，保持行为不变，再逐步接入 pk 意图 |
| RemoteProvider 依赖 pnos-sdk RPC | 阶段5前先用 LocalProvider，RPC 接口预留 |

---

## 十二、关键设计原则

1. **意图是唯一契约**：决策源与执行层只通过 `Intent` 通信，不传递命令、不共享状态
2. **资源通过接口注入**：不持有资源，只持有算法。资源实现按模式注入，托管用远程，独立用本地
3. **模式感知但不模式分裂**：同一套代码支持三种模式，通过 `ModeController` 统一切换
4. **反馈闭环不可省**：无论哪种模式，执行结果都必须有去处——托管回传 pk，独立写入本地，降级缓冲待传
5. **降级不是失效**：降级模式下仍能执行 Critical 意图，只是策略更保守
6. **ResourceMonitor 是独立运行的基石**：不是可踢掉的冗余，而是独立模式下的决策输入、托管模式下的上报数据、降级模式下的安全兜底

---

## 十三、一句话总结

**先把所有周期性行为纳管（阶段0），再引入意图抽象（阶段1-2），然后把任务调度升级为意图编排（阶段3），同时修复健壮性（阶段4），最后完成资源外置和反馈闭环（阶段5）。ICC 从"自治智能体"升级为"意图执行中枢"——托管时听 pk 的，独立时自己决策，降级时保守兜底，执行结果必有去处。**

---

## 参考文档

- `08-intelligent-control-center.md` — ICC 初始设计（2026-09-10）
- `../adr/005-intelligent-control-center.md` — ICC 架构决策记录
- `D:\PNOS\AGENTS.md` — 生态架构基准
