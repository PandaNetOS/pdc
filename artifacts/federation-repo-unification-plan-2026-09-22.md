# 联邦同步「全 repo 统一逻辑与策略」方案

> 日期：2026-09-22　状态：**待评审**（未执行完，见「已动工未验证」一节）
> 目标：NODE / PEER / INFOHASH / TRACKER 四个 repo 走**同一条代码路径、同一套触发策略**，消除 NODE 特化分支。

## 一、现状盘点：全仓 `repo_type::NODE` 特化点

### A 类 —— 代码路径特化（必须消除）

| # | 位置 | 现状 | 后果 | 统一化做法 |
|---|---|---|---|---|
| A1 | `sync/mod.rs:1210` `use_bootstrap` | `&& rt == repo_type::NODE` | 非 NODE 永不进快照通道 | 删门控，四 repo 同判定 |
| A2 | `sync/mod.rs:1213` | 非 NODE 打 debug「回落 delta」 | — | 随 A1 删除 |
| A3 | `sync/mod.rs:2043` 清单响应 | `if mf.repo != NODE { return; }` | 非 NODE 清单被丢弃 | 改为范围校验 `NODE..=TRACKER` |
| A4 | `sync/mod.rs:2091` 分块落地 | 硬编码 `apply_node_sync` | 拿到 peer 块也按 node 落库 | 改 `handle_sync_batch(resp.repo, &entries)`（该函数已是 4 repo dispatch） |
| A5 | `sync/mod.rs:2298/2316` `check_and_trigger_bootstrap` | 只看 `counts[0]`、硬编码 `repo_type::NODE`、只查 NODE 的 20% 差异 | peer 差 44% 也不会触发快照 | 改为对 4 个 repo 循环：各自取 `local_counts[idx]` vs `peer_digests[idx]`，复用同一 20% 阈值与「一次只起一个」约束 |
| A6 | `sync/mod.rs:2319` `start_bootstrap` | `if repo != NODE { return; }` | 调用方放通了也被拦 | 改为范围校验 |
| A7 | `sync/bootstrap.rs:264` `build_node_manifest` | 包装函数硬编码 NODE | 若被调用则产出错误 repo 的清单 | 确认调用方；无调用则删，保留 `build_repo_manifest_impl` 单一入口 |
| A8 | `sync/mod.rs:120/153` `bootstrap_manifests` / `bootstrap_rebuild_at` | key 只有 `NodeId` | 多 repo 并行 bootstrap 互相覆盖清单 → 按错误边界取数 | key 改 `(NodeId, u8)` |

**已确认无需改**（本身就是 4 repo 通用，只是语法上出现 NODE 常量）：
- `mod.rs:484` 握手后 4 repo 循环 `trigger_delta_sync` ✅
- `mod.rs:889/932` `(NODE..=TRACKER)` 循环 ✅
- `mod.rs:299` `handle_sync_batch` 4 repo dispatch ✅
- `dispatch.rs:807` 4 repo 分组 ✅
- `gossip.rs:1041+`、`protocol.rs:894` 仅测试代码 ✅

### B 类 —— 策略参数分档（需拍板，见第三节）

| # | 位置 | 现状 | 说明 |
|---|---|---|---|
| B1 | `mod.rs:63` `RANGE_INTERVAL_SECS` | NODE 30s / PEER 120s / INFOHASH 300s / TRACKER 600s | **违反「相同策略」**：peer 对账频率只有 node 的 1/4 |
| B2 | `mod.rs:70/72` + `leaf_rows_for_repo` | TRACKER 512 / PEER·INFOHASH 2048 / NODE 走 config `range_reconcile_leaf_rows` | 三档阈值，NODE 与其余三个来源不同（一个读 config、两个写死） |

## 二、统一化后的目标形态

四个 repo 共享同一条决策链，**唯一差异只剩数据本身**：

```
握手 → 4 repo 各发 OpsRequest（已有）
     ↓
delta_sync_tick（每 repo 同逻辑）
  ├─ 协商裁定 BOOTSTRAP 或 lag > threshold → start_bootstrap(repo)   ← A1/A6
  └─ 否则 → OpsRequest 增量拉取 + 看门狗
     ↓
check_and_trigger_bootstrap（每 repo 同逻辑，20% 阈值）              ← A5
     ↓
start_bootstrap → 清单请求(带 repo) → 应答方 build_repo_manifest_impl(repo)
     ↓
清单响应（按 repo 校验，不滤 NODE）                                  ← A3
     ↓
分块请求/响应 → handle_sync_batch(repo, entries) 落地                 ← A4
     ↓
range 反熵（每 repo 同周期、同叶阈值）                                ← B1/B2（待拍板）
```

## 三、需要拍板的决策点

### 决策 1：B1/B2 的「相同策略」指什么？

- **选项 a（完全同参数）**：`RANGE_INTERVAL_SECS` 统一为单一值（如都 30s 或都 60s）；`leaf_rows` 统一为 config 的 `range_reconcile_leaf_rows`。
  - 代价：TRACKER（384 行）和 NODE（164 万行）用同一频率与叶阈值，小库被过度扫描、大库下钻粒度可能偏粗。
  - 收益：语义最干净，符合「相同策略」字面要求。
- **选项 b（同逻辑 + per-repo 参数表）**：保留一张 4 项配置表，但**四项都必须来自配置**（不许两个写死、一个读 config），且默认给同值。
  - 代价：仍是「参数不同」，但来源是同一套机制、默认值一致，可按观测数据调。

### 决策 2：peer 走 bootstrap 后的收敛形态

peer 是瞬时数据（谁在下载什么）。拉对端快照后：
- 本机 `peer_repo_total` 会**先跳一波**（并集），随后本机自身过期清理又降下来 → 呈**锯齿**而非单调下降。
- 需要确认：是否接受用 `peer_repo_total` 作为验收指标？还是改用 `peer_repo_active`（1h 活跃，当前差 1,060）？

### 决策 3：`SNAPSHOT_MIN_ROWS=1000` / `SNAPSHOT_RATIO_THRESHOLD=1.2`

当前是全局常量（四 repo 共用）✅ 已符合「相同策略」。确认是否保留。

## 四、风险

| 风险 | 说明 | 缓解 |
|---|---|---|
| 四 repo 同时 bootstrap 抢 Federation 槽 | 每个 bootstrap 都是长任务；已有饥饿补偿（30s 后超额准入 2） | `check_and_trigger_bootstrap` 保持「一次只起一个」；可加跨 repo 串行约束 |
| 清单重建全表扫描放大 | 打通后 4 个 repo 都可能触发现场重建（每次数分钟） | 已有 600s 租约；建议补充「同时只允许一个重建在跑」 |
| peer 快照落地后本机过期清理回吐 | 锯齿形态被误判为不收敛 | 验收改用 active 口径或看 30 分钟窗口的均值趋势 |
| 旧对端（未升级二进制）不认非 NODE 清单 | 协议版本 `supports_bootstrap()` 已做校验 | 双端同版本部署，风险可控 |

## 五、已动工、未编译验证的改动

以下两处**已写入源码但尚未 `cargo check`**（用户要求先评审方案，已停手）：

1. `bootstrap_manifests` / `bootstrap_rebuild_at` 的 key 由 `NodeId` 改为 `(NodeId, u8)`，同步修改 6 处引用（A8）。
2. `start_bootstrap` 的 `repo != NODE → return` 改为范围校验 `NODE..=TRACKER`（A6）。

若评审否决本方案，这两处需回滚（A8 的 key 重构本身无害，可保留）。

## 六、建议执行顺序

1. A3 / A4 / A1（三处一行级改动，打通请求方链路）
2. A5（check_and_trigger_bootstrap 循环化，核心）
3. A7（确认并删除 `build_node_manifest` 或改签名）
4. `cargo fmt` + `clippy -D warnings` + release 构建 + 双端部署
5. 观测：4 个 repo 的 `ops_lag`、`bootstrap.done_chunks`、`peer_repo_total` 趋势
6. B1/B2 按决策 1 的结论单独一轮处理
