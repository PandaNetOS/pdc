# pdc AGENTS.md

> 本文件是 AI 代理进入 pdc 仓库时的首读指南。
> 生态级全局约束请参考 [根目录 AGENTS.md](../AGENTS.md)。

## 仓库定位

pdc（Peer Discovery Center）是 PandaNetOS 生态的**节点发现 Agent**，Agent 级独立进程，注册到 pnos-runtime。负责 DHT 爬虫、超级 Tracker、PEX、联邦同步等节点发现能力。

## 架构概览

```
pdc/
├── 控制层 (control_plane)   # HTTP API、WebSocket 监控、策略引擎
├── 数据层 (data_plane)      # UDP Tracker、中继、超级 Tracker
├── 智能层 (intelligence)    # TaskScheduler、自适应控制器、评分引擎、ICC
├── 发现层 (discoverers)     # DHT、Tracker、PEX、LPD 发现器
├── 爬虫层 (crawler)         # DHT 爬虫（8 socket 多并发）、TrackerFetcher
├── 存储层 (storage)         # NodeRepo、PeerRepo、InfohashRepo、WriteQueue
├── 联邦层 (federation)      # 多实例同步：Gossip + delta(oplog) + Range 反熵 + bootstrap
└── 网络层 (net)             # socket_opts、连接管理
```

核心数据流：爬虫发现节点 → NodeRepo → 评分引擎 → 选择高质量节点 → 继续爬行

## 目录结构

```
pdc/
├── src/
│   ├── main.rs                  # 入口，TaskScheduler 注册中心
│   ├── config.rs                # 配置（所有端口/间隔可配置）
│   ├── crawler/                 # DHT 爬虫（engine、rate_limiter）
│   ├── intelligence/            # 智能层（task_scheduler、adaptive_controller、crawler_history）
│   ├── storage/                 # 存储（node_repo、peer_repo、write_queue）
│   ├── data_plane/              # 数据层（udp_tracker、relay）
│   ├── control_plane/           # 控制层（HTTP API、WebSocket）
│   ├── discoverers/             # 发现器（dht、tracker、pex、lpd）
│   ├── federation/              # 联邦同步
│   └── net/                     # 网络工具（socket_opts）
├── config/                      # 配置文件
├── docs/architecture/           # 架构文档（00-08）
├── docs/adr/                    # 架构决策记录
└── Cargo.toml
```

## 构建与测试

| 命令 | 说明 |
|---|---|
| `cargo build --release` | Release 构建（约 2-3 分钟） |
| `cargo test --all` | 运行所有测试（300+ 用例） |
| `cargo fmt --all -- --check` | 格式检查 |
| `cargo clippy --all-targets -- -D warnings` | 静态分析 |

## 关键配置

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `server.port` | 6880 | HTTP 监控；UDP Tracker 未配置时回退此端口 |
| `server.api_port` | 6886 | TCP API |
| `super_tracker.udp_port` | 未配置（回退 `server.port`） | UDP 超级 Tracker |
| `discoverers.dht_listen_port` | 6881 | DHT 发现器 |
| `crawler.socket_count` | 1（上限10） | 爬虫 socket 数量 |
| `crawler.concurrent_sockets` | 4 | 每轮并发 socket 数 |
| `crawler.listen_port` | 6882 | DHT 爬虫监听；多 socket 额外端口由 PortAllocator 在同百位段内整组平移分配 |
| `crawler.utp_port` | 6883 | uTP |
| `crawler.tcp_pex_port` | 6884 | TCP-PEX |
| `federation.listen_port` | 6885 | 联邦 |
| `discoverers.lpd_multicast_port` | 6771 | LPD 多播 |

## 依赖关系

- **依赖**：`pnos`（path，pnos-spec）、`pnos-net`（path，pnos-sdk）
- **注意**：Cargo.toml 中 crate 名仍为 `PeerDiscoveryCenter`（历史遗留）
- **被依赖**：pk（通过 pnos-runtime 间接调用）

## 注意事项

1. **TaskScheduler 提调一切**：所有周期性任务必须注册到 TaskScheduler，禁止模块内部自跑定时
2. **8 Socket 架构**：爬虫支持多 socket，tid 高 4 位编码 socket 索引
3. **预测式自适应**：SGD 模型预测响应率，特征归一化+梯度裁剪
4. **测试目录**：测试必须在 `D:\test\pdc` 运行，禁止在仓库目录执行
5. **编译环境**：飞牛服务器 192.168.30.35:2222 Docker 容器
6. **远程节点**：192.168.30.51 的数据不允许删除

## 联邦同步诊断摘要（2026-09-21）

> 原始诊断报告（双端现网数据、F6/F7/F8 改动明细、槽占用与耗时统计）已随时效性过期移除，见 git 历史。其中 Merkle 相关任务已随 v8 去 Merkle 化整体消失；**「Federation 分类并发槽饥饿」根因结论在 v8/v9 下仍然成立且更关键**——Range 反熵成为唯一兜底通道后，其长耗时占槽问题（avg 127.94s / max 253.61s）直接决定 bootstrap 与 delta 能否被调度。

- **根因**：分块传输并非丢包。Federation 分类并发槽（8 个）被长 await 网络任务长期占满（单次可达 100~311s），`fed_bootstrap_resume` 被持续延迟、几乎无法实际执行，表象是「请求方发了块请求、应答方静默」。
- **F8a（`fed_range_reconcile` 超时 300s→900s）判定负收益**：Range 对账本来就跑得久，并非被 300s 掐死；放宽超时等于给它「合法占槽更久」的许可证，连锁饿死 bootstrap 续传等任务。解决方向是**迁槽而非延时**：长任务迁出 Federation 槽（独立 TaskCategory / 不受并发槽限制的后台任务 / 分帧让出），bootstrap 续传拆成单块分帧模式（v9 已部分落地：`federation_concurrency` 入配置、range 单轮公平轮转）。
- **验收标准**：双端 `bootstrap.done_chunks` 单调递增，`ops_lag.repo=1` 双向收敛到 0。

## 变更历史

| 日期 | 版本 | 变更内容 |
|---|---|---|
| 2026-09-16 | v1.0 | 初始版本，记录 8 socket、预测式自适应、TaskScheduler 纳管 |
| 2026-09-21 | v1.1 | 追加联邦同步诊断报告：DB 权威口径收敛数据、F6/F7/F8 落地状态、Federation 槽饥饿根因、F8a 回滚建议 |
| 2026-09-22 | v1.2 | **联邦同步去 Merkle 化（协议 v8）**：Merkle 反熵全退场，Range 反熵成为唯一兜底通道（详见下节与 ADR-007）。v1.1 诊断报告中的 Federation 槽饥饿分析里，Merkle 相关任务（联邦Merkle反熵/联邦Merkle批量flush/Merkle增量×4/Merkle冷重算×4）已随本次重构整体消失 |
| 2026-09-22 | v1.3 | **联邦同步收敛修复（v9，按通道）**：解除 delta↔bootstrap 互锁、oplog 空洞可检测+裁剪感知对端进度、NODE 反熵确定性收敛、gossip 取消安全+攒批、bootstrap 生命周期护栏、表达式索引消除全表排序。**未改线网格式**，两端可分别升级。详见下节 |
| 2026-09-29 | v1.4 | 精简 09-21 联邦诊断报告为摘要（原始数据见 git 历史）；修正关键配置表端口默认值（`server.port` 6880 / `server.api_port` 6886）；README 重写对齐现状 |

## 联邦同步架构 v9（2026-09-22，收敛修复）

**本节为最新基准。** v8 的通道划分不变（delta + Gossip + Range 反熵 + bootstrap），v9 修的是"跑起来但收敛不了/占槽不返回"的一类缺陷。协议版本号未变（无字段变更），但**行为有变**：两端无需锁步升级，服务端收益（per-repo gap 检测、对端 ack、清单对齐）需同版本。

### 修复的六条主线

1. **通道互锁（停摆）**：`lag > 1万` 时 `delta_sync_tick` 无条件 `continue` 关停该 repo 的 delta，而 bootstrap 又永久卡在 `transfer/done=0`（清单路径每次全表重排、分块路径租约内静默不回帧、请求方无 attempt/超时）⇒ 两通道互锁。现在：delta 与 bootstrap **并行**（`federation.bootstrap_blocks_delta` 可回退旧行为）；`delta_channel_allowed` 中 `STRATEGY_BOOTSTRAP` 不再拒绝 delta（策略只决定"优先怎么追"）。
2. **假收敛**：oplog 只按时间裁剪、`min_seq` 无人消费 ⇒ 请求方游标跨过被裁掉的空洞、lag 归零。现在：`handle_ops_batch` 用**对端自报的 per-repo `min_seq`** 判空洞（`cur+1 < peer_min_seq`）并置 `delta_gap` 强制走快照；新增 `oplog_peer_ack` 表记录**请求方对本机 oplog 的游标**（与本机 seq 同空间），裁剪按"最小对端 ack − 安全余量"执行（`oplog_trim_respect_peer_floor`），hard_cutoff 兜住无界增长。
3. **反熵空转**：NODE 的 `data_hash` 含 `node_id`，而同 key 不同 id 在 DHT 是常态；入站 `version==0 && contains_sync ⇒ 跳过` 让三条入站通道都无法修复它 ⇒ 每轮重新发现、每轮推拉、每轮丢弃。现在：`apply_node_sync` 对同 key 取**字典序较小 node_id** 为规范值（对所有 version 生效），两端规则对称 ⇒ 一轮交换即收敛。
4. **bootstrap 无生命周期**：`bootstrap_state` 主键 `repo` → **`(peer, repo)`**（旧库自动事务化迁移）；`running` 判定加 peer + `updated_ms` 新鲜度，停滞超 `bootstrap_stall_secs` 置 Idle 并放行 delta；清单**缓存复用 + 单飞**；缓存缺失时**只回清单不回块**（消除 index↔区间错位造成的静默空洞）；所有早退路径回**显式 NAK**；请求方分块尝试计数 + 超时升级重拉清单；失败重发同块（不再跳过）；`w0` 改 per-repo 且 `set_peer_seq` 推迟到竣工；空清单不落库；同 version 清单保留已完成进度。
5. **gossip 丢批**：`PropagationGuard` —— 传播 tick 被 300s 超时截断时**回灌已出队批次**并归还 in-flight；本地写入**攒批**（`gossip_coalesce_batch_size`，默认 256，DELETE 走顺序屏障）；丢弃路径全部补计数（`/sync-observability.gossip_drops`）；重试预算改为可配 `gossip_retry_budget_secs`（默认 300s）。
6. **放大器**：`task_scheduler.federation_concurrency` / `tracker_concurrency` 入配置（原硬编码 8/2）；range 单轮按"最久未处理优先"限 `range_repos_per_tick` 个 repo + 按 `(peer,repo)` 公平轮转 + 同叶区间修复冷却；新增表达式索引 `idx_dht_nodes_ip_port_expr` / `idx_peers_key_expr`，NODE 区间查询改**按需拼谓词**（让索引真正可用于区间定位，消除 bootstrap 的 74~93 次全表排序）。

### 诊断方法（可复用）

- 会话死因看 `[session] sX 空闲超时（… > N ms）` 与 `[session] sX 关闭 对端=… 原因=…`；
- "任务占槽久"先看 `调度器心跳 … 最老在飞=…` 与 `槽位泄漏：任务 … 已在飞 Ns`，**不要先怀疑该任务本身的逻辑**（v9 实测 `fed_delta_sync` 450s 的真因是别的任务长期占用全局 SQLite 连接锁）；
- DB 侧看 `WAL checkpoint 高频稳态` / `WAL TRUNCATE` 的单次耗时（远端实测 435s / 1244s）——它们与 IOScheduler、所有只读查询**共用同一把 `Arc<Mutex<Connection>>`**。

### 已知遗留（v9 未修，下一轮优先级）

1. **全局单连接 + 长 PRAGMA 是"IO 慢 → 全节点停摆"的根**：`checkpoint`/`checkpoint_truncate` 在共享连接上执行且无超时（慢盘单次 435~1244s）；100ms 的 `wal_checkpoint_steady` 与 `wal_autocheckpoint=1000` 重复且是最长持锁者。方案：独立连接 + 硬超时 + 降频；读写连接分离（WAL 支持 1 writer + N readers）。
2. **统计字段直查库、频率过高**：`stats_snapshot` 任务默认 **1 秒** × 4 个 `SELECT COUNT(*)`（1.7M 行表，且非 `spawn_blocking`）；WS 状态推送每 5s × 4 个 COUNT × 客户端数；`实体表 DB 级统计校准` 每 300s × 5 个 COUNT（远端 32.6s）。方案：行数改写路径增量计数；统计只留一个低频快照任务（独立只读连接）；`entity_counts_cached()` 未校准不再同步 COUNT。
3. **会话层**：teardown 不发 `Goodbye`（对端仍持陈旧会话 → 新握手被判重复而关闭）；同 node_id 新会话应**接管**而非拒绝；`pnos-net` 冷却粒度是**地址级**（应 `(node_id, addr)`）；pdc peer 缓存对同一地址保留多个互斥身份（现场 4 个），应只留最近身份。
4. `send_range_push` 无字节上限（单帧可含数千条）；delta tick 内 `send_message` 串行等待写锁，无预算/让出；控制帧（心跳/OpsRequest）与批量帧（GossipBulk/RangePush2）无优先级区分。
5. PEER/INFOHASH 入站 DELETE 不落软删墓碑；PEER 收敛口径把 `peers_archive` 计入（指标虚高于同步范围）；bootstrap 块内容校验（`resp.hash`）未启用（一致性交给 range）。
6. gossip 接收端背压：`gossip_flush_max_batches` 形参未用、semaphore 无超时、`receive_pending_threshold` 无消费方。


## 联邦同步架构 v8（2026-09-22，去 Merkle 化）

**本节为最新同步架构基准。此前文档（含本文件 v1.1 诊断报告、docs/architecture/07）中涉及 Merkle 对账 / 分片同步引擎 / DiffSync 的描述均已过时。** 决策记录见 `docs/adr/007-range-only-anti-entropy.md`。

### 删除内容

- **协议族**（protocol.rs）：MerkleDigest(11)/MerkleRequest(12)/MerkleRepair(15)/FullSync*(16-19)/DiffSyncRequest(21)/DiffSyncKey*(28,29)/MerkleLevel*(30,31)/ShardSync*(32-36)/RangeReconcilePush(45) 全部退役，编号保留空位；`HELLO_PROTOCOL_VERSION` 7→8
- **实现**：`federation/merkle.rs`、`federation/sync/shard_sync_engine.rs`、`federation/sync/merkle_updater.rs` 整文件删除；SyncManager 的 merkle 字段与方法族、3 个 sync 子模块与 4 个 repo 的 merkle 参数/字段/联动、db.rs 的 Merkle 族加载器（load_all_*_keys_hashes / load_*_by_shards / backfill_shards 等）、config.rs 21 个相关字段（merkle_*/shard_sync_*/anti_entropy_*/layered_merkle_enabled/diff_key_exchange_timeout_secs）
- **TaskScheduler 任务删除**：fed_merkle_anti_entropy、fed_merkle_flush、merkle_incremental_×4、merkle_cold_rebuild_×4、fed_diff_sync_watcher
- **修复连带消灭**：port 哈希双公式（u16/i64 两套路径）导致的 Merkle 幻影差异、rebuild_all 后 65536 dirty 超参数上限必败循环、RangeReconcilePush 走 add_node_sync 回灌 oplog 的不变量违反

### 新增 / 变更

- **range 修复通道通用化**（4 repo 全支持，对端协议 ≥ v8 才启用，`supports_range_v2()` 门控）：
  - 叶级对账「本地多」→ `load_repo_entries_by_keys`（按主键分批 IN，各 repo key 解析：NODE `ip:port` rsplit、PEER `hex(ih):ip:port` 双 rsplit、INFOHASH 20B 原文、TRACKER url）→ `RangeReconcilePush2(49)` 推完整 SyncEntry
  - 叶级对账「对端多」→ `RangeReconcilePull(48)` 发 key 列表（≤4096 条 / ≤2MB）→ 对端按 key 加载回推 Push2。**不再依赖**「对端 oplog 必有对应 op」的错误假设（入站/bootstrap 来源的数据在 oplog 中无 op，旧 delta 委托对它们失效）
  - 接收统一走 `handle_sync_batch` 幂等 apply，保持「入站不写 oplog、不回灌」不变量；< v8 对端回落 delta 委托（仅稳态增量有效）
- **serde 默认值与 impl Default 对齐**：`delta_sync_enabled`/`range_reconcile_enabled`/`bootstrap_enabled` yaml 缺省时默认 true，`range_reconcile_diagnostic_only` 默认 false。此前 serde 缺省全为 false——即生产 yaml 不写这些键时，这几条通道实际是关的
- **协议字段变更（版本 8），两端必须同步升级二进制**；旧版对端连接后 range 修复降级、delta 仍可用

### 现行同步链路（v8）

| 通道 | 角色 | 触发 |
|---|---|---|
| delta（OpsRequest/OpsBatch，oplog 增量） | 稳态主线 | fed_delta_sync 周期 + 建连 + bootstrap 追尾 |
| Gossip（GossipBatch/Bulk） | 实时推送 | 本地写入即时 |
| **Range 反熵（区间下钻 + Pull/Push2 修复）** | **唯一兜底** | fed_range_reconcile（NODE 30s/PEER 120s/INFOHASH 300s/TRACKER 600s） |
| bootstrap（manifest/分块/追尾） | 全量引导 | 协商裁定 BOOTSTRAP 或 lag>1 万；仅 NODE repo |

### 已知遗留（未修，下一轮）

tracker 删除墓碑复活闭环、gossip 限流全跳过丢批与 3-tick 重试丢弃、oplog 裁剪越界静默丢 op（min_seq 无人消费）、bootstrap 非 NODE repo 被 `continue` 跳过 delta、bootstrap 块校验「整区间重算」语义在接收方有本地数据时恒失配。

---

## 2026-09-29 配置热重载完整落地（config hot-reload）

> 本节为配置热重载的最新行为基准。此前「简化版：仅记录日志，不实际生效」的描述已过时。

### 机制

`config_reloader` 周期任务（TaskScheduler 注册，间隔 `config_reload_interval_secs`，0=禁用）：
**mtime 轮询 → 防抖（`config_reload_debounce_ms`，默认 1000ms，mtime 稳定为止）→ `PdcConfig::from_file` 解析 → `diff_configs` 差异对比 → `classify_delta` 分类 → 应用 → 更新快照 + `ControlPlane::update_config` 发布 `Event::ConfigChanged`（ws 转发前端）**。

核心实现：`src/control_plane/config_reload.rs`（ConfigReloader）。同一 apply 管线供两个触发源共用：
- 文件监听（周期任务）；坏配置不推进 mtime 基线，下个周期自动重试；
- `POST /api/v1/config/reload`（跳过 mtime 检查立即重载；继承 API token 鉴权）；`GET /api/v1/config` 返回当前快照。

### 参数分类（白名单外一律按「需重启」处理，新增字段默认安全）

| 类别 | 字段 | 应用方式 |
|---|---|---|
| A 纯策略 | `crawler.rate_limit_enter_threshold/exit_threshold/min_samples/throttle_skip_ratio` | `RateLimiter::update_config(&self)`（内部可变性，Arc 共享热更） |
| A 纯策略 | `log_level` | tracing reload 层热替换（另修复：过滤层此前挂链尾不生效，已移至链首） |
| A 纯策略 | `super_tracker.*`（除 udp_port/relay_port） | `SuperTrackerState::update_config` 整节热替换 |
| B 调度 | `task_scheduler.intervals.*` | `TaskScheduler::update_interval`；时间单位（秒/毫秒）按「旧值换算==当前周期」反推，推断不出则跳过不猜 |
| B 调度 | `task_scheduler.*_concurrency` | `TaskScheduler::update_category_concurrency` |
| B 调度 | `task_scheduler.*`（准入/抖动/预测/自适应旋钮） | `TaskScheduler::update_knobs(SchedulerKnobs::from_config)` |
| B 调度 | `config_reload_interval_secs` | reloader 改自己的任务周期；`config_reload_debounce_ms` 每轮从快照读取 |
| C 结构性 | 端口类 / `socket_count` / `*_runtime_threads` / 存储路径等 | 不热切，仅 WARN 提示重启生效 |

### 安全护栏

1. 坏配置绝不应用：解析失败保留旧配置继续运行，连续失败 ≥3 次升级 ERROR；
2. 防抖防半写入：mtime 变化后等 `config_reload_debounce_ms`，最长 3 轮；
3. C 类字段只告警不应用；每次重载打 diff 明细（长值截断）；
4. `TaskScheduler` 的 `knobs`/`max_concurrency` 改为 `RwLock` 内部可变性（读取点已全部改为短临界区，不跨 await 持锁）。

### 新增配置字段（全部带 `#[serde(default)]`，缺省行为与引入前一致）

| 字段 | 默认值 | 说明 |
|---|---|---|
| `crawler.rate_limit_enter_threshold` | 0.15 | 进入限速阈值（此前为模块内常量，本节落地为配置） |
| `crawler.rate_limit_exit_threshold` | 0.30 | 解除限速阈值 |
| `crawler.rate_limit_min_samples` | 50 | 限速判断最小样本数 |
| `crawler.rate_limit_throttle_skip_ratio` | 0.5 | 限速时降频跳过比例 |
| `config_reload_debounce_ms` | 1000 | 热重载防抖等待（毫秒） |

### ICC 预留（ADR-005）

`ControlSource::{FileWatcher, Api, Icc}` 与 `ConfigDelta.controlled_by` 为 pk/ICC 意图下发预留位；
ICC P3 接入时在 `ConfigReloader::apply` 插入「pk 意图 > ICC 策略 > 本地文件 > 默认值」优先级仲裁，
apply 管线结构不变。
