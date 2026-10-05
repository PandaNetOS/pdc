# 爬虫选节点质量优化方案：新近度分层选择 + 评分窗口化 + 池卫生

> 状态：研究中（2026-10-05），待批准
> 前置关联：19号文档（发送链路重构：流式发送+信号去噪+tid扩宽，已批准未实施）、[爬虫效率调优记录](../crawler-efficiency-tuning-2026-10-05.md)（反馈链路 G2 根治，已落地未提交）
> 目标对齐：根 AGENTS.md 性能目标「爬虫效率 10,000 新节点/小时」

## 背景

2026-10-05 反馈链路根治（方案 G/G2）后，自适应控制器已看到真实响应率，倍率恢复 1.0，新节点发现速率从 240/h 回到 4,620/h。剩余瓶颈明确：**真实响应率仅 5–15%（DHT 正常 30–70%）**——发送预算大量打到 DB 加载的历史节点（多为已下线死节点）。调优记录结论：根治需从节点选择端入手（提升命中率而非放宽阈值）。

本方案回答一个问题：**同样的发送预算，发给谁**。与 19 号文档（怎么发：pacing/限速/tid）、G2（反馈怎么算）正交，互不依赖，可并行实施。

## 现网证据（2026-10-05，实例 uptime 41min，19,407 次发送，API /api/v1/status 实测）

| 指标 | 值 | 含义 |
|---|---|---|
| NodeRepo 总量 | 106,670 | 预加载 ~10 万 + 会话新增 |
| Good | 3,061（2.9%） | 近 15 分钟内有 last_active |
| Questionable | **103,333（96.9%）** | 唯一被大量选中的池，仅 0.7 评分惩罚 |
| Bad | 276 | 41 分钟仅判死 276 个（≈400/h） |
| active（query_count>0） | **429（0.4%）** | 发了 19,407 次却只涉及 429 个节点？——见 R6：发送集中在重复选择的小集合 |
| avg_score | 26.96 | 低于「未查询中性分」26.6~30.4 区间下沿，池内大量低分/零分 |

关键推算：按 400/h 的判死速率清完 ~10 万陈旧池需要 **~250 小时**；在此之前发送预算持续被死节点吞掉，响应率无法自然恢复。

## 根因清单（含代码证据）

### R0 预加载过滤失效——「僵尸高分馆」的直接来源

`db.rs:1952` 预加载查询：

```sql
WHERE deleted_at IS NULL AND (last_active > ?1 OR score >= ?2) ORDER BY score DESC LIMIT ?3
```

调用处 `node_repo.rs load_initial → load_hot_warm_nodes(7200, 0.0, preload)`，`min_score=0.0` 使 `score >= 0.0` **恒真**，OR 短路掉 last_active 过滤 ⇒ 实际加载「DB 中历史评分最高的任意 10 万行」——全库按旧评分排序的博物馆，与最近是否活跃无关。

### R1 加载后 last_active 复位为 now

SELECT 列表（`db.rs:1949`）**不含 last_active**（列存在且写入侧维护），`KBucketEntry::new` 置 `last_active = Instant::now()`（`kbucket.rs:90`）⇒ 三个月前下线的节点以「刚刚活跃」进场，15 分钟后统一翻 Questionable，此后彼此无差异。

### R2 热集合自我强化——「最近被选中」≠「最近有响应」

- `load_initial` 把全部加载节点标 hot（`node_repo.rs` 加载尾部）；60s 后 `tier_evict` 虽会迁移冷节点，但之后 hot 的唯一来源是**选择动作本身**：`select_system.rs:73` 对每个选中节点 `mark_accessed_sync`（移入 hot）⇒ 死节点一旦被选中就留在热池反复入选。
- 真正的活性信号（响应成功）走 `record_query_with_nodes_sync`，与选中无关联。

### R3 选择无新近度维度

`select_diverse_nodes`（`select_system.rs:31`）仅过滤 `state != Bad`：热池优先（HashSet 无序）→ top500 按**终身累计评分**补足。没有「刚刚响应过 > 刚刚被发现 > 最近活跃 > 更老」的分层，也没有排除在飞（pending 中）节点。

### R4 评分模型对新节点不利、对陈旧高分节点宽容

- 响应率/产出用**终身累计** `success_count/query_count`（`node_score.rs:91`）：历史 1000 次 90% 成功的节点，近期连续失败对分数几乎无影响。
- 新节点初始 45 分（`node_repo.rs:367`）→ 首次重算即降为「未查询中性分」38 × Questionable 惩罚 0.7 ≈ **26.6**，结构性低于历史高分死节点 ⇒ **刚被邻居证实活着的节点排位最差**，这是方向性错误。
- 时间衰减（`decay_start_hours=2h` 起、24h 衰减到 0.2 倍）依赖 `last_query_time`，只覆盖「本会话查询过的节点」；「2 小时内活过但现已死」的节点无任何惩罚，而 DHT 节点会话中位时长恰在小时级。

### R5 mention 刷新活性——死节点被提及即「保活」

`add_nodes_batch_internal` 对已存在节点仅因**出现在别人的响应里**（未经验证）就 `existing.last_active = Instant::now()`（`node_repo.rs:361`）⇒ 大节点被邻居高频提及、永不老化；「已验证响应」与「被提及」共用同一字段，选择与评分无法区分。现网 100,112/106,670 节点 last_active < 30min，绝大部分由 mention 贡献。

### R6 判死慢 + 发送集中

- 判死需 3 次连续失败，每次失败要等 `pending_timeout_secs=120s`（G2 正确性要求，不可调短）+ 30s cleanup 周期 + 重选延迟 ⇒ 单节点判死 ≥ 6 分钟；叠加 R2/R3 的重复选择，41 分钟仅判死 276 个。
- pending 未超时的节点可再次入选（无在飞去重），同一死节点短窗内重复消耗预算。

### R7 Bad 永久冻结，无复活通道

`refresh_state` 对 Bad 直接 return（`kbucket.rs:141`）；mention/联邦重见均不解除 ⇒ 误判无回头路（对「刚搬家/暂时丢包」的节点是永久损失），也使 Bad 计数只增不降、失去信号意义。

## 设计

### D1 恢复真实新近度（加载链路，零 schema 变更，止血优先级最高）

1. `load_hot_warm_nodes` 的 SELECT 补 `last_active` 列，`DhtNodeRow` 加字段；谓词修正为**新近度优先**：`ORDER BY last_active DESC`（或两段加载：`last_active > 24h` 全量 + 历史高分探索段限量）。`min_score` 语义修复（不再传 0.0 造成恒真 OR）。
2. `load_initial` 恢复 `entry.last_active = row.last_active`；按真实 last_active 重算初始 state（超过 warm 阈值 → Questionable，超过冷阈值 → 可直接不加载）。
3. 加载节点**不再全部标 hot**（hot 语义收归 D3 的「已验证」来源）。

### D2 新近度分层选择（核心改动，SelectSystem）

候选分四层（层内仍按评分排序 + 既有 ID 分桶/网段去重）：

| 层 | 定义（可配阈值） | 先验 |
|---|---|---|
| L0 已验证活 | 本会话响应成功过（`last_verified` < 10min） | 最强 |
| L1 新鲜未验证 | 本会话新发现 / `last_active` < 30min 且未在飞 | 强（发现即活性证据） |
| L2 温 | `last_active` < 24h | 中 |
| L3 冷探索 | 其余，按评分排序 | 弱 |

- 每轮 `target_count` 按 **exploit（L0+L1）80% / explore（L2+L3）20%** 分配（`select_explore_ratio` 可配）；explore 段保证池持续换血与 last_seen 刷新，且**无论僵尸池多大，每轮损耗上限就是这 20%**。
- **在飞去重**：选择时排除 pending 表中的 addr。
- L1 强化：`add_nodes_sync_batch` 返回的 `new_pairs`（本会话真新增）注入一个有界的新鲜队列（如 2×target），下一轮优先消费——把 chain_crawl（每组 8 个）之外的新鲜节点也纳入正式配额。

### D3 评分与状态修正（verified / mentioned 分离）

1. `KBucketEntry` 增加 `last_verified: Option<Instant>`（仅响应成功时更新，内存字段起步）；`refresh_state` 的 Good/Questionable 翻转与 D2 分层、R4 惩罚均以 `last_verified` 为准，`last_active` 保留「任何接触」语义（mention 刷新它不再伪造活性）。
2. 评分窗口化：新增 recent 滑动计数（`recent_query/recent_success`，指数衰减 α 可配，仅内存）；响应率维度优先窗口值、无窗口数据回落终身值——近期连续失败立即反映到分数，不再被历史稀释。
3. 新节点先验分：初始分 = f(新近度)，刚发现 ≈ 80，随未验证时长向中性分衰减（`new_node_prior_score` 可配）；首次重算不再把「未验证但新鲜」压到池底。
4. 持久化：`last_verified` 复用新列或先不持久化（重启后由会话内重建，回落现行为，`#[serde(default)]` 兼容）。

### D4 快速失败与池卫生

1. `consecutive_failures` 判 Bad 阈值配置化（默认 3 不变，可调 2）；另加「轮级零响应降分」：进入 L0/L1 的节点本轮发送、下轮账目无响应 → 直接扣分（不动 state），使死节点在 1–2 轮内跌出 exploit 层，而不是等 3×120s。
2. **Bad 复活**：Bad 节点被 mention 且距判死 > 10min → 降回 Questionable、失败计数减半（有冷却的第二次机会，恢复 Bad 计数的信号意义）。
3. 审查 TierManager 驱逐参数（`warm_threshold_secs=7200` + `max_hot_in_memory=5000` 与 preload 10 万的矛盾）：确保「内存池规模」有真实上界，陈旧池靠 R0 修复从源头不再进入。

### D5 可观测性（验证本方案的核心指标，与 19 号文档 D4 衔接）

- 按层计数：`select_layer_selected{layer}` / `select_layer_sent{layer}` / `select_layer_responded{layer}`（进 CrawlerState → /api/v1/stats，Prometheus 族 `pdc_crawler_layer_response_rate{layer}`）。
- 有效性判据：L0 响应率 > 60%、L1 > 40%、整体真实响应率 ≥ 30%。

## 新增配置（全部 `#[serde(default)]`，缺省即启用新行为；提供 `crawler.select_mode = "layered" | "legacy"` 逃生开关）

| 字段 | 默认 | 说明 |
|---|---|---|
| `crawler.select_mode` | `layered` | `legacy` 一键回退现行为 |
| `crawler.select_explore_ratio` | 0.2 | 每轮探索（L2+L3）预算占比 |
| `crawler.select_verified_recent_secs` | 600 | L0 阈值（last_verified） |
| `crawler.select_fresh_secs` | 1800 | L1 阈值（last_active） |
| `crawler.select_warm_secs` | 86400 | L2 阈值 |
| `crawler.bad_after_failures` | 3 | 判 Bad 连败阈值（可调 2） |
| `crawler.new_node_prior_score` | 80 | 新节点先验分（随时间衰减至中性分） |
| `scorer.recent_decay_alpha` | 0.3 | recent 窗口指数衰减系数 |
| `tier.preload_min_score` | （改传有值或弃用） | 修复 R0 的恒真 OR；预加载改 last_active DESC |

## 测试计划（沿用 in-module `#[cfg(test)]` 惯例，约 12 个单测）

| 测试 | 验证点 |
|---|---|
| 预加载谓词 | min_score=0 时不再全量命中；ORDER BY last_active DESC 生效 |
| 加载恢复 | row.last_active → entry.last_active/state 重算正确；陈旧节点不进 hot |
| 分层归属 | 构造 verified/mentioned/陈旧混合池，四层归类与阈值边界正确 |
| exploit/explore 配额 | target=64、ratio=0.2 → L0/L1 选 51、L2/L3 选 13（取整规则） |
| 在飞去重 | pending 中 addr 不被选中；超时清理后可复选 |
| 新鲜队列 | new_pairs 注入有界队列、下轮优先消费、不无限膨胀 |
| mention 不伪造 verified | 仅 mention 的节点 last_verified 为 None、refresh_state 翻 Questionable |
| 窗口评分 | 近期连败使分数迅速跌破陈旧高分节点；终身值回落路径 |
| 先验分衰减 | 刚发现 80 → 未验证 2h 后接近中性分 |
| Bad 复活 | 冷却期内不复活；复活后 Questionable、计数减半 |
| legacy 模式 | `select_mode=legacy` 时选择结果与现行为等价 |
| 轮级零响应降分 | L0/L1 节点下轮账目零响应 → 分数下降、state 不变 |

既有 579+ 测试全绿；`record_query*`/pending/账目机制不动（G2 语义保持）。

## 实施批次（每批独立过合规门禁）

1. `fix(storage): 节点预加载恢复真实 last_active 并修正恒真过滤`（D1，止血：池从源头换血）
2. `feat(crawler): 新近度分层选择与在飞去重`（D2 + D5 计数埋点）
3. `feat(intelligence): 评分窗口化与 verified/mentioned 分离`（D3）
4. `feat(crawler): 快速失败、Bad 复活与池卫生参数`（D4）
5. `docs: 配置表与 AGENTS.md 同步`（合规 #24）

批次 1 与 19 号文档无交集可先行；批次 2–4 依赖批次 1 的真实 last_active。

## 验证步骤

1. 合规全绿：fmt / clippy / test / release build（飞牛编译环境）。
2. 部署 `D:\test\pdc` 重启实例，观察 30–60 分钟：
   - **分层响应率**：L0 > 60%、L1 > 40%（方案生效的直接信号）；
   - 整体真实响应率 5–15% → ≥ 30%；
   - 新节点发现速率 ≥ 10,000/h（并发 8 + 倍率 1.0 + 命中率提升三因素叠加）；
   - NodeRepo 内存规模受 TierManager 上界约束；Bad/复活计数双向流动。
3. 达标后推远端（双端联邦不受影响：本方案不触协议与线网格式）。

## 回滚方案

- 行为级：`crawler.select_mode = "legacy"` 一键回退（批次 2–4 全部收口于该开关）；D1 为正确性修复，独立保留。
- 版本级：各批独立 revert；D1 仅增 SELECT 列与内存字段赋值，回滚无数据兼容问题。

## 与 19 号文档 / G2 的边界

| 线 | 问题域 | 状态 |
|---|---|---|
| G2（2026-10-05） | 反馈怎么算（账目/claim/pending 超时） | 已落地未提交 |
| 19号文档 | 怎么发（pacing/信号去噪/tid 扩宽/metrics 接线） | 已批准未实施 |
| 本方案（20号） | **发给谁**（选择/评分/加载/淘汰） | 研究中 |

三者叠加路径：选对节点（本方案）→ 平滑发送（19号）→ 真实反馈（G2）⇒ 响应率与发现速率同时回到正常区间。
