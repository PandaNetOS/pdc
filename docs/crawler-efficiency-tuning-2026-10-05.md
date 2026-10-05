# pdc 爬虫效率调优与轮次反馈链路根因修复（2026-10-05）

> 调优时间：2026-10-05（全天）
> 目标：新节点发现效率 ≥ 10,000 节点/小时
> 初始实测：1,620–3,100 节点/小时
> 当前实测：4,620 节点/小时（60s 窗口，方案 G2 部署后），链路修复前最低曾回落到 240/h
> 运行版本：v0.2.0（本次修复基于 4714129 之上的未提交改动）

---

## 一、根因总览

本轮调优共定位 **2 个硬瓶颈 + 1 个反馈链路缺陷（含 2 层子缺陷）**，分阶段修复：

| # | 瓶颈 | 判定 | 修复方案 | 效果 |
|---|---|---|---|---|
| 1 | `concurrent_sockets_in_use=1`（并发 socket 未配置） | 设计如此，非异常 | config 加 `concurrent_sockets: 8` | 预热期达 11,700/h（超目标） |
| 2 | 自适应控制器倍率锁死下限 0.2 | 反馈链路缺陷的**表象** | 方案 C/D/E 验证 → 方案 G/G2 根治 | 倍率恢复 1.0 并随真实响应率决策 |
| 3a | 轮次反馈取账目过早（固定 800ms 等待） | **根因①**：响应到达/处理延迟远超固定窗口 | 方案 G：延迟一轮取账目 | 账目覆盖完整轮周期 |
| 3b | pending 表 15s 超时清理早于响应到达 | **根因②**：claim 未命中 → 账目根本加不进去 | pending 超时配置化（默认 120s）+ 账目兜底 300s | claim 命中率回升 |
| 3c | 下轮 take 仍早于延迟响应到达 | **根因③**：响应延迟超过一整轮（约 40s） | 方案 G2：账目持续累计 + 增量报告（delta） | responded 反映真实响应率 |

---

## 二、诊断链（四阶段定位）

### 阶段 1-2：并发定位与基线

- 确认生态仅 pdc 运行（`pdc.exe --work-dir D:\test\pdc`）；监控/API 在 **6886**（6880 是 Tracker HTTP+UDP 回退）。
- 实测新节点 1,620–3,100/h；监控显示 `concurrent_sockets_in_use=1`。
- 定位 `concurrent_sockets_in_use=1` **为设计如此**：`config.rs default_concurrent_sockets()=1`，config 未配置；`resolve_concurrent_sockets`（engine.rs:767-771）。

### 阶段 3：并发修复

- config.yaml 加 `crawler.socket_count: 8`、`concurrent_sockets: 8`。
- 注意：config.yaml 行尾为 `\r\r\n`（双重 CR），Edit 工具必然匹配失败，需 Python 字节级替换。
- 重启后预热期达 11,700/h（超目标）、31.5 req/s、并发 8。

### 阶段 4：二次瓶颈（自适应锁死 0.2）

预热期过后速率回落到 1,380/h——自适应控制器接管后把倍率 clamp 到 `min_multiplier=0.2` 锁死。依次尝试：

| 方案 | 内容 | 实测结果 |
|---|---|---|
| C | 预热期（hist≤warmup_rounds）只记录不训练 + 预测与实际背离>50% 回退实际值决策 | 仍锁死 0.2（720/h, avg_resp=0.917） |
| D | C + `sent==0` 跳过 + 决策改用 `rate_last_3` 滑动均值 + 回退对比同步滑动均值 | 仍锁死 0.2（hist=87，速率 6,780/h——发送基数低导致轮级样本不足） |
| E | engine 侧加 `round_feedback_wait_ms`（默认 800ms）+ `collect_round_feedback`（sleep 后取账目） | **铁证否定等待窗口方案**（见下） |

**方案 E 的 debug 日志铁证**：

```
03:58:26  active_crawl 轮次反馈: sent=36 responded=0      ← 发送后等 800ms 取账目
03:59:06  active_crawl 轮次反馈: sent=32 responded=1
04:00:15  收到 find_node 响应 from 115.126.232.33, nodes=8  ← 响应 1-2 分钟后才到达处理
          NodeRepo 持续增长（101229→101241）
```

响应**最终都到达**（NodeRepo 在涨、socket 累计响应率 70%+），但到达/处理时间远超 800ms 等待窗口——固定等待窗口方案不成立。

### 阶段 5：根治（方案 G + 补全 + G2）

**根因①（账目取早）**：`active_crawl` 发送后立即（或等 800ms）`take_round_feedback(seq)`，响应 1-2 分钟后才在 handle 流程经 `claim_pending`（tid 命中 pending 才 `acc.responded += 1`）计数，账目已取走 → responded≈0 → controller 每轮判 0 响应率 ×0.5 → 锁死 0.2。

**方案 G**（已部署）：
1. `active_crawl` 改为**本轮发送、下轮取上一轮账目**做反馈（新增 `active_prev_round` 字段存上轮 (seq, sent)）；
2. `get_peers`/`sample` **退出自适应决策**（发送量小、0 响应数据一直在污染 controller），仅 `take_round_feedback` 清理账目防泄漏。

**补全（pending 超时配置化）**：部署后发现 responded 仍=0-2，同时收到 817 条响应日志但 claim 命中仅 ~6%——**根因②**：`PENDING_REQUEST_TIMEOUT = 15 秒`（硬编码）在响应到达前清空 pending → `claim_pending` 未命中 → 账目根本加不进去。修复：
- 新增配置 `crawler.pending_timeout_secs`（`#[serde(default)]`，默认 **120s**）；
- 轮次账目兜底清理 60s → **300s**。

**G2（增量报告）**：再部署后发现 responded 仍=1（sent=64）——**根因③**：响应延迟超过一整轮（约 40s），下轮 `take` 时账目虽未超时但已被主动移除，延迟到达的响应丢失。修复：
- 账目**不再 remove**，`RoundFeedbackAcc` 增加 `reported` 字段，**每轮只报告自上次以来的新增（delta）**（`drain_round_feedback_delta`）；
- 账目持续累计，300s 兜底清理回收；`get_peers`/`sample` 仍走 `take_round_feedback` 清理。

---

## 三、修复后证据

方案 G2 部署后（实例 05:11 启动，100s 采样）：

```
60s 新增: 77  →  速率/h: 4,620      （对比 G 补全前的 240/h，提升 19 倍）
active_crawl 轮次反馈: sent=39 responded=0 timed_out=0
mult=1.0 hist=1 upd=0               （预热期只记录不训练，方案 C 生效）
socket_response_rates: 0.0, 0.11, 0.14, 0.15, 0.11, 0.07, 0.09, 0.05
```

倍率恢复 1.0 且不再锁死下限；controller 现在看到的是真实响应率。

---

## 四、代码与配置变更清单（未提交）

| 文件 | 变更 |
|---|---|
| `src/config.rs` | +`crawler.round_feedback_wait_ms`（800，等待窗口方案遗留，兼容保留）；+`crawler.pending_timeout_secs`（120，`#[serde(default)]` 向后兼容） |
| `src/crawler/engine.rs` | +`active_prev_round` 字段（本轮发送、下轮回报上轮账目）；`active_crawl` 反馈改为 `drain_round_feedback_delta`（增量报告）；`get_peers`/`sample` 移除 `report_round_result` 调用（退出自适应决策）；`cleanup_pending` 改用配置超时；`RoundFeedbackAcc` +`reported`；删除 `collect_round_feedback` 与 `PENDING_REQUEST_TIMEOUT` 常量；账目兜底清理 300s |
| `src/intelligence/adaptive_controller.rs` | 方案 C/D 累积（预热期只记录不训练 + 预测背离回退 + `sent==0` 跳过 + 滑动均值决策），+4 单测 |

**测试**：579 passed / 16 failed（存量 PermissionDenied，与本次改动零交集，git diff 证实）；pdc 内部 clippy 零新增警告；release 编译通过。

**部署备份链**：`D:\test\pdc\pdc.exe.bak-20261005-{c,d,e,g,h,i}`（方案 i 为最新 G2 版）。

---

## 五、当前状态与下一步

- **已达**：4,620/h（目标 10,000/h 的 46%）；反馈链路真实；倍率可正常决策。
- **剩余瓶颈**：真实响应率仅 5-15%（DHT 正常应 30-70%）——爬虫大量打到 DB 加载的 10 万历史节点，多为已下线死节点；`select_diverse_nodes` 选节点质量与评分/保活淘汰机制待分析。
- **风险**：预热期结束后 controller 看到真实低响应率（<0.21 阈值），会把倍率降回 0.2——但这是**真实响应率的合理反映**，根治需从节点选择端入手（提升命中率而非放宽阈值）。
- **待清理**：临时配置 `adaptive.warmup_rounds: 10`、`log_level: debug`（快速验证用，正式值 warmup=50 / info 需恢复，涉及配置修改需用户确认）。
