# ADR-007: Merkle 反熵全退场，Range 反熵成为唯一兜底通道

- 状态：已实施（2026-09-22）
- 关联：[ADR-006 联邦同步收敛](006-federation-sync-convergence.md)、[架构文档 12](../architecture/12-federation-sync-reconciliation.md)
- 协议版本：8（`HELLO_PROTOCOL_VERSION`）

## 背景

架构文档 12 规划的三层同步（delta 稳态 / 反熵兜底 / bootstrap 引导）落地后，Merkle 反熵暴露出结构性问题：

1. **哈希公式双轨**：`data_hash` 的 port 编码在「增量/fold 路径（u16）」与「冷重算/range 路径（i64）」不一致。两节点数据完全一致时，同一 L2 分片的哈希取决于最后由哪条路径重算——反熵每轮都判出幻影差异，且随重算来源翻转永不收敛（2026-09-22 排查 #1）。
2. **粒度数学必然 100%**：2% 均匀 churn 撒满全部 256 个 L1 桶，历史回环（45 分钟 55GB）的根因在 L2 下钻后虽有缓解，但 L1 侧依赖仍在。
3. **维护成本失控**：4 棵树 ×（增量 10s + 冷重算 1h）任务、脏标记队列、`rebuild_all` 后 65536 dirty 超过 SQLite 参数上限导致重算任务必败循环（排查 #6）。
4. **交接已是半成品**：v7 起仅 NODE 的主动对账权交给 range（`range_reconcile_owns` 只判 NODE），PEER/INFOHASH/TRACKER 处于「Merkle + range 双通道并行」状态。

同时，Range 反熵（有序区间 + 分界点下钻）已验证具备独立承担兜底的能力，但其修复路径当时未打通：非 NODE repo 的「本地多」无推送通道、「对端多」委托 delta 而 delta 对无 oplog 数据结构性失效。

## 决策

1. **删除全部 Merkle 反熵实现与协议**：4 棵 MerkleTree、MerkleDigest/MerkleRequest/MerkleRepair/MerkleLevel*/ShardSync*/DiffSync*/FullSync* 协议族（编号 11/12/15-19/21/28-36 退役留空）、分片同步引擎、merkle_updater、相关任务与配置字段。DB 的 `l2_shard` 列与写入路径保留（schema 稳定，向后兼容）。
2. **先补全 range 修复路径再删**（关键顺序约束）：
   - 新增 `RangeReconcilePull(48)`（按 key 拉取）与 `RangeReconcilePush2(49)`（4 repo 通用完整 SyncEntry 推送），对端协议 ≥ v8 才启用；
   - 叶级对账「本地多」→ 按主键索引批量加载（`load_repo_entries_by_keys`）直接推；「对端多」→ 发 key 列表请对端回推。修复不再依赖对端 oplog；
   - 接收统一走 `handle_sync_batch` 幂等 apply，维持「入站不写 oplog、不回灌」不变量。
3. **serde 默认值与 `impl Default` 对齐**（`delta_sync_enabled`/`range_reconcile_enabled`/`bootstrap_enabled` → true；`range_reconcile_diagnostic_only` → false）：删除 Merkle 后 range 是唯一反熵，yaml 缺省时若仍默认关闭，生产环境将没有任何兜底对账在跑。

## 后果

**正面**

- 消灭 Merkle 幻影差异整类问题（哈希双轨、L1 粒度 100%、65536 参数必败循环、冷重算滞后）——不再有「内存树 vs DB」两个真相源，对账口径唯一（DB 区间摘要）。
- 删除约 4600 行维护负担（merkle.rs 1209 + shard_sync_engine 1087 + merkle_updater 91 + sync/mod.rs 方法族 + db 加载器 + config 21 字段），TaskScheduler 减少 11 个注册任务。
- 四个 repo 获得统一、对称的修复路径（Pull/Push2），修复正确性不再依赖「数据必须有 oplog 条目」的假设。

**负面 / 风险**

- **协议不兼容**：两端必须同步升级 v8 二进制；混跑时 v8 节点对 < v8 对端的 range 修复回落 delta 委托（仅稳态增量有效），< v8 对端发来的已退役消息被丢弃（编号空位）。
- range 对账的 key 是 `(ip || ':' || port)` 表达式，NODE/PEER 无表达式索引，区间扫描是全表扫——大库上的对账成本靠抽样（`range_reconcile_sample_ranges`）与 per-repo 节流（30s/120s/300s/600s）控制。若成为瓶颈，后续可为 key 表达式建索引（P1）。
- 顺带删除了 `trigger_initial_sync`/FullSync wire 家族等已无调用点的死代码，行为无变化，但排查日志时注意这些名字已不存在。

## 验证

- `cargo check/clippy --all-targets`：0 error、0 warning（pdc 侧）
- `cargo test --all`：422 passed / 0 failed
- `cargo build --release`：通过
- 双节点实测（.57 ↔ .51）待部署后按 v1.1 诊断报告的验收口径复核：`bootstrap.done_chunks` 单调、`ops_lag` 双向收敛、`range_stats` 修复触发数增长、DB 直查差异收敛。
