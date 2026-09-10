# ADR-005: 智能控制中心（ICC）设立与统一控制收口

> 状态：✅ 已采纳
> 日期：2026-09-10
> 决策者：项目维护者
> 相关：智能层、控制面、任务调度、资源管理、pk 主控对接

## 背景 (Context)

在 ADR-002 中，智能层完成了**决策收口**（评分/冷热/选择统一在 intelligence 层）。但控制面仍处于分散状态，存在以下问题：

1. **任务调度分散**：
   - `TaskScheduler` 引擎已实现（`intelligence/task_scheduler.rs`），但运行时只收编了 2 个任务（`periodic_persistence`、`resource_monitor`，见 `main.rs`）。
   - `health_check`、`ScoreMaintainer.start()`、NAT/UPnP、`CrawlerEngine`、`keyword_search_service`、`subscription_service`、`tracker_fetcher` 等仍各自 `tokio::spawn` + `interval()` 独立循环。
   - 任务错峰、资源让位仍依赖 `05-runtime-flow.md` 里人工排的启动延迟，无法动态调整。

2. **资源管理无账本**：
   - `ResourceMonitor` 只有 CPU/内存读数 + IO/网络忙闲标志，且 `main.rs` 里还是写死的示例值。
   - 没有「谁占用多少连接/带宽/并发/内存配额」的账本，无法统一提调（分配/回收/限额）。

3. **模块协调仅覆盖发现器**：
   - `ControlPlane`（`control_plane/mod.rs`）只协调 5 个发现器（tracker/dht/pex/lpd/webseed）的注册与策略。
   - crawler、probe、pex、nat、tracker service、federation 等业务模块不在其协调范围内，模块间靠 EventBus + 各自逻辑松散耦合。

4. **约束条件**：
   - 超级 Tracker 的 announce 热路径必须保持纯本地内存 <100μs、10 万 QPS（ADR-004），任何控制逻辑不得侵入该同步调用链。
   - 千万级数据目标（ADR-004），控制中心不能引入重开销。
   - 生态定位：pk 是上层业务主控 Agent（大脑），pdc 是节点发现 Agent（AGENTS.md），两者都注册到 pnos-runtime。

## 决策 (Decision)

在智能层设立**智能控制中心（Intelligent Control Center，ICC）**，作为 PDC 进程内的自治控制中枢（"小脑"），将 ADR-002 的「决策收口」升级为「控制收口」，实现三个统一：

### 1. 统一提调所有资源 → ResourceLedger（资源账本，新建）

把 `ResourceMonitor` 从"读数器"升级为"记账 + 提调"：

```rust
pub enum ResourceKind {
    Concurrency, // 爬虫并发 / probe 并发 / tracker 活跃连接
    Bandwidth,   // DHT 出站 / 爬取速率 / 本进程出站中继
    Memory,      // 冷热分层热/温内存预算
    DiskIo,      // 持久化写入预算
}

pub struct ResourceLedger {
    limits: RwLock<ResourceLimits>,   // 总预算
    usage:  RwLock<ResourceUsage>,    // 当前占用
    leases: RwLock<FxHashMap<ResourceId, Lease>>, // 谁占什么
}
```

### 2. 统一安排所有任务 → TaskScheduler 全覆盖收编

复用已实现的 `TaskScheduler` 引擎，把散落的 `tokio::spawn`/`interval()` 循环全部收编注册（增量/全量评分、健康检查、冷热分层、持久化、资源监控、NAT/UPnP、联邦心跳、爬虫 tick、probe 扫描等），统一由优先级队列 + 令牌桶 + 资源感知调度管理。

### 3. 统一协调各业务模块 → ControlPlane 扩权为 ControlBus

把 `ControlPlane` 从"发现器协调器"升级为面向**所有业务模块**的控制总线，统一控制面/数据面分离：

```rust
#[async_trait]
pub trait Controllable {
    fn module_id(&self) -> &'static str;
    async fn start(&self, ctx: &ControlCtx) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    async fn apply_policy(&self, policy: &ControlPolicy) -> Result<()>;
    async fn snapshot(&self) -> ModuleSnapshot;
}
```

### 4. 控制闭环（Observe-Decide-Act）

感知（ResourceMonitor + HealthScorer + metrics）→ 决策（PolicyEngine 规则引擎 + 已有 ScoreMaintainer/TierManager/SelectSystem）→ 执行（ResourceLedger 分配 + TaskScheduler 排程 + ControlBus 下发），状态回流形成闭环。

### 5. 三条关键边界（定稿）

| 边界 | 决策 |
|---|---|
| **pk ↔ ICC 通信** | 统一走 `pnos-sdk`：pk 通过组件调用（`app.call("pdc")`）下发声明式意图，ICC 通过事件订阅（`pdc.control.*`）+ 心跳 + 状态快照回传。**ICC 的 SDK 路由复用 PDC 现有 6880 HTTP server 挂载**，不自建私有 RPC。 |
| **联邦多实例** | **不纳入 ICC 提调范围**。ResourceLedger/ControlBus 只含单进程内资源；FederationService 仅作为可启停模块纳入生命周期，不做动态预算/提调。 |
| **策略引擎形态** | 小脑纯规则、不接模型。ICC 的 PolicyEngine = 可配置规则（TOML）。**pk 大脑后期接大模型**，ICC 输出结构化状态快照作为大脑决策输入，大脑只回传声明式意图。 |

### 6. 五条铁律

1. **announce 热路径零侵入**：超级 Tracker 的 `handle_udp_announce` 仍是纯本地内存 <100μs，ICC 只通过后台异步通道间接影响。
2. **评分唯一性不破**：ICC 只读 ScoreMaintainer 的评分做决策，绝不自行算分（延续 ADR-002）。
3. **可配置 + 默认值**：ICC 所有阈值/权重/预算走 TOML 配置，带 `#[serde(default)]`（延续 AGENTS.md 约束）。
4. **Fail-open 降级**：ICC 或策略引擎失效时，各模块退回自身默认调度自主运行，不允许中心坏了全线瘫。
5. **数据层唯一真相源不破**：ICC 只通过 Repo trait 读写，不旁路直连存储。

## 后果 (Consequences)

### 正面影响

- **控制收口**：从「决策收口」延伸到「控制收口」，任务/资源/模块三条线统一归 ICC 调度，消除分散控制。
- **动态资源调配**：pk 给总预算，ICC 在预算内把资源动态切给最需要的模块，替代硬编码争抢。
- **声明式对接 pk**：ICC 输出状态快照、接收声明式意图，为 pk 后期接大模型预留了稳定接口（接大模型时 ICC 零改动）。
- **可观测可控**：统一任务统计、资源账本、模块状态，便于监控、定位、告警。
- **复用现有成果**：TaskScheduler/ControlPlane/Intelligence/EventBus 均已存在，ICC 是整合升级而非推倒重来。

### 负面影响 / 代价

- **集成改造**：需收编散落的 spawn 循环、扩权 ControlPlane、接入 pnos-sdk，有一定重构量。
- **中心化风险**：ICC 可能成为单点，需 fail-open 降级对冲。
- **学习成本**：新增 ResourceLedger/ControlBus/PolicyEngine 概念。

### 风险

- **热路径误伤**：任何 ICC 调用若进入 announce 同步链都会破坏 <100μs 目标。
- **收编回归**：把散循环收进调度器可能引入时序变化。
- **与 pk 职责重叠**：ICC 若做跨 Agent 编排就越界了。

### 缓解措施

- **架构评审红线**：ICC 调用禁止进入 announce 热路径，加性能回归测试。
- **灰度收编**：P1 阶段只收编、不改行为，逐模块验证。
- **边界写死**：ICC 只碰 PDC 进程内；跨 Agent 编排归 pk。
- **Fail-open**：ICC 失效时模块退回默认调度（等同现状）。

## 替代方案 (Alternatives)

### 方案 A：保持现状（分散控制）

- 优点：无需改造。
- 缺点：任务错峰靠人肉排期、资源无账本无法提调、模块协调不统一，无法支撑千万级与动态目标。
- 不选择的原因：控制面分散是明确的技术债，且阻碍 pk 下发目标的落地。

### 方案 B：ICC 扩展为跨 Agent 全局控制

- 优点：单一控制中枢。
- 缺点：与 pk 的「上层业务主控 Agent」定位冲突，pdc 越权管理 spde/pk。
- 不选择的原因：违反 AGENTS.md 生态定位，大脑（pk）与小脑（ICC）必须分层。

### 方案 C：ICC 策略引擎接入模型

- 优点：决策更智能。
- 缺点：小脑引入模型带来不可解释性、资源开销与部署复杂度，且与「pk 后期接大模型」的规划重复。
- 不选择的原因：模型决策归大脑（pk），小脑只做规则落地。

### 方案 D：把联邦多实例纳入 ICC 提调

- 优点：跨实例统一调配。
- 缺点：跨实例资源提调需引入分布式账本/一致性，复杂度高，且与 07 系列文档的 Gossip 去中心化方向相悖。
- 不选择的原因：当前阶段先单实例闭环，联邦提调后置，避免过早分布式化。

## 实施计划 (Implementation Plan)

- [ ] **P0（骨架）**：新建 `intelligence/resource_ledger.rs` + `intelligence/control_center.rs` 门面 + `config.rs` 的 `[control_center]` 段；ICC 门面聚合账本/调度/总线/策略，暴露 `handle_intent` / `status_snapshot` 两个 SDK 入口；单元测试。
- [ ] **P1（收编任务）**：ScoreMaintainer / 健康检查 / 冷热分层 / 持久化 / NAT 全部注册进 TaskScheduler，接入真实资源读数（sysinfo）；只收编不改行为，灰度验证。
- [ ] **P2（提调资源）**：爬虫并发、tracker 连接、probe 并发、冷热内存预算纳入 ResourceLedger，支持 pk 动态调预算。
- [ ] **P3（协调模块 + 对接 pk）**：ControlPlane 扩权为 ControlBus，crawler/probe/pex/nat 实现 `Controllable`；PolicyEngine 上线；PDC 通过 `PnosApp` 接入 pnos-sdk，复用 6880 HTTP server 挂载 `/api/v1/control/*` 路由。

## 验证标准 (Verification Criteria)

- [ ] announce 热路径延迟仍 <100μs，无 ICC 调用进入同步链（性能回归测试通过）。
- [ ] 所有定时任务统一由 TaskScheduler 管理，无散落的 `tokio::spawn` 周期循环。
- [ ] ResourceLedger 能对爬虫并发/带宽/内存做申请、限额、回收，超限可观测。
- [ ] pk 能通过 `app.call("pdc").post("/api/v1/control/intent", ...)` 下发意图，ICC 通过 `/api/v1/control/status` 返回状态快照。
- [ ] 策略引擎规则全部来自 TOML 配置，无模型/权重文件。
- [ ] ICC 停摆时各模块退回默认调度，系统仍正常运行（fail-open）。
- [ ] 全部单元测试通过，现有行为无回归。

## 参考资料 (References)

- [ADR-001: 架构边界与分层原则](001-architecture-boundary.md)
- [ADR-002: 智能层统一收口](002-intelligence-layer.md)
- [ADR-004: 千万级数据性能目标与优化架构](004-performance-targets.md)
- [03-intelligence.md — 智能层详细设计](../architecture/03-intelligence.md)
- [08-intelligent-control-center.md — ICC 详细设计](../architecture/08-intelligent-control-center.md)
- [pnos-sdk README](../../../pnos-sdk/README.md) — 通信 SDK（组件调用/事件订阅/心跳）
- [AGENTS.md](../../../AGENTS.md) — 生态架构基准

## 变更记录 (Changelog)

| 日期 | 版本 | 变更内容 | 作者 |
|---|---|---|---|
| 2026-09-10 | 1.0 | 初始版本，记录 ICC 设立与统一控制收口决策 | 项目维护者 |
