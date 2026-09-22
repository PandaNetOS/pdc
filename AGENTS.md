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
| `server.port` | 6886 | HTTP API/监控端口 |
| `super_tracker.udp_port` | 6880 | UDP 超级 Tracker |
| `discoverers.dht_listen_port` | 6881 | DHT 发现器 |
| `crawler.socket_count` | 1（上限10） | 爬虫 socket 数量 |
| `crawler.concurrent_sockets` | 4 | 每轮并发 socket 数 |
| `crawler.listen_port` | 6802-6892 | 爬虫 socket 端口范围（PortAllocator 分配） |
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

## 联邦同步诊断报告（2026-09-21 23:20，执行中）

> 数据源：双端 REST（`/api/v1/federation/status`、`/api/v1/federation/sync-observability`）+ 双方 stdout.log 尾部窗口。
> 结论一句话：**分块传输「没有在丢包」，是 bootstrap 续传任务被 Federation 分类并发槽饿死，从未实际执行。**
>
> ⚠️ **时效性注记（2026-09-22 v8 去 Merkle 化后）**：本报告基于协议 v7 采集的数据。其中提到的 Merkle 相关任务（`fed_merkle_anti_entropy` / `fed_merkle_flush` / `merkle_incremental_*` / `merkle_cold_rebuild_*`）与 F8b 改动已随 Merkle 移除而整体消失；但 **§4 的「Federation 分类并发槽饥饿」结论不仅仍然成立，反而更关键** —— 移除 Merkle 后 Range 反熵成为唯一兜底通道，其长耗时占槽问题（avg 127.94s / max 253.61s）会直接决定 bootstrap 与 delta 能否被调度。§5 的 F8a 回滚建议与 §7 的 P0 项在 v8 下优先级更高。

### 1. 现网实例状态

| 项 | 本机 127.0.0.1:6886 | 远端 192.168.30.51:6886 |
|---|---|---|
| node_id | `22335aa9e00a7db1…` | `3b9a051289e69aad…` |
| 进程 | `D:\test\pdc\pdc.exe`（22:46:18 起） | `D:\ShareCenter\test`（同批部署） |
| 版本 | 0.2.0 | 0.2.0 |
| uptime | 1492s | 1482s |
| connections / known_nodes | 1 / 8 | 2 / 9 |

### 2. 收敛现状（口径 = DB 唯一数据源，F9 已生效）

| Repo | 本机 total | 51 total | 差距 | 判读 |
|---|---|---|---|---|
| node | 1,657,680 | 1,526,886 | **本机领先 130,794** | 上一轮 13.9 万 → 略收窄，仍在收敛中 |
| peer | 23,250 | 12,437 | 本机领先 10,813 | 51 侧 peer 摄入明显偏少 |
| infohash | 22,314 | 22,318 | 51 领先 4 | 基本一致 |
| tracker | 383 | 383 | 0 | 完全一致 |

- 内存热/温：`node_repo_hot_total` 本机 110,679 / 51 124,802；`peer_repo_hot_total` 本机 11,439 / 51 659 —— **hot 不再用于判定收敛口径**，仅作观测。
- oplog：本机 len 693,818（max_seq 1,591,228）；51 len 655,061（max_seq 1,180,967）。
- ops_lag（双向）：本机→51 repo=1 lag **95,091**；51→本机 repo=1 lag **41,494**；repo=2/3/4 均为 0（repo=2 在 51 侧为 `null`，见「待确认」）。**node repo 双向都欠账**，说明两侧都在持续新增、追不上。
- range_stats：本机 leaf 138 / local_only 522 / remote_only 341 / repair_triggers 138；51 leaf 144 / local_only 617 / remote_only 887。
- bootstrap 进度（双端均卡死）：本机 repo=1 `phase=transfer, done 0/74, bytes 0, w0_seq 1,047,571`；51 `phase=transfer, done 0/82, bytes 0, w0_seq 1,535,777`。

### 3. 本轮改动（F6/F7/F8，已编译已部署，**未提交**）

`git status`：`src/federation/sync/mod.rs`、`src/intelligence/task_scheduler.rs`、`src/main.rs`（+83 / -5）。

| 编号 | 位置 | 改动 | 现网验证 |
|---|---|---|---|
| F6 | `sync/mod.rs` 分块请求应答处 | 应答方 `bootstrap_manifests` 缺失时不再 WARN 返回，改为**按当前 DB 现场重建清单**并回写缓存 | ✅ 生效：51 端已出现「清单缓存缺失，现场重建: peer=22335aa9, repo=1, 块数=77」，原「无清单缓存」刷屏消失 |
| F7 | `sync/mod.rs` 块校验处 + 结构体 | 新增 `bootstrap_verify_fails: RwLock<FxHashMap<u8,u32>>`；校验失败计数，连续 ≥3 次判清单漂移 → 重发清单请求自愈 | ⏸ 未验证（被调度饿死阻塞） |
| F8a | `main.rs:2061` | `fed_range_reconcile` 超时 300s → **900s**（新增 `TaskMetadata::with_timeout`） | ❌ **判定负收益，建议回滚**（见 §5） |
| F8b | `main.rs:2531/2587/2693` | 4 个 `merkle_incremental_*` 加 `.with_keepalive()` 豁免资源准入延迟 | ⚠️ 部分生效/待确认（见 §6） |
| — | `task_scheduler.rs:292` | 新增 `with_timeout` builder（基础设施，无害） | ✅ |

### 4. 根因：Federation 分类并发槽饥饿（已定位，非丢包）

之前「请求方发了块请求、应答方静默」的判断**不成立**。真实链路：

```
Federation 分类并发上限 = 8
  → 槽内多为长 await 的网络任务（单次可达 100~311s，见下表）
  → 任务周期是秒级，占比却是百秒级 → 8 个槽恒满
  → fed_bootstrap_resume 进不去 → 块请求根本没发出
  → done_chunks 恒为 0，看起来像「响应静默丢失」
```

**本机证据（最近约 10MB 日志窗口，含 3163 次任务完成）**

| 观测 | 数值 |
|---|---|
| `Federation 8/8` 槽满次数 | **493** |
| `Persistence 1/1` 槽满次数 | **925** |
| `fed_bootstrap_resume` 被延迟次数 | **32**（top6 里唯一被卡的业务主线） |
| `fed_bootstrap_resume` 实际执行次数 | **3**（15:10:09 / 15:11:20 / 15:13:02） |
| resume 日志「从块 0 继续」次数 | **16**（全部恒等地从块 0） |

被延迟最多的联邦任务（同窗口）：联邦中继通道清理 69、联邦Merkle批量flush 64、联邦连接维护 61、联邦差量同步超时监控 59、联邦Gossip批量flush 57、联邦bootstrap续传 32、联邦PEX节点交换 32、联邦delta增量拉取 32、联邦Merkle反熵 30、联邦Range反熵对账 17。

**单任务耗时 Top（本机，同窗口）——槽被谁占住**

| 任务 | max | avg | n |
|---|---|---|---|
| 联邦Gossip传播 | **311.43s** | 11.5s | 65 |
| 联邦Range反熵对账 | **253.61s** | **127.94s** | 4 |
| 联邦delta增量拉取 | 247.12s | 39.16s | 14 |
| 联邦PEX节点交换 | 176.2s | 19.99s | 31 |
| 联邦bootstrap续传 | 164.36s | 32.68s | 17 |
| 联邦Merkle反熵 | 161.73s | 6.71s | 30 |
| Merkle增量更新-Node | 20.26s | 3.97s | 93 |

> 注意「联邦bootstrap续传」自身 avg 32.68s / max 164.36s —— 它一旦排进去会**长期占槽**，进一步恶化同分类饥饿。

**51 端证据（15:16:30 → 15:19:37，连续 3 分 7 秒）**：`fed_bootstrap_resume` 每 5 秒被 `分类并发已满（Federation 8/8）` 延迟一次，**零次实际执行**。所以 51 侧 82 块一块没传。

### 5. F8a（timeout 300→900s）判定：副作用大于收益

原意是「避免 Range 对账被 `tokio::time::timeout` 掐死做重复功」。实测后果：

- Range 对账 avg 已达 127.94s、max 253.61s，**它并不是被 300s 掐死，而是本来就跑得久**；
- 把上限放宽到 900s 等于给它「合法占用 Federation 槽更久」的许可证，直接放大 §4 的饥饿；
- 因此 bootstrap 续传、Merkle flush、中继清理等被连锁饿死。

**建议：回滚 `with_timeout(900s)`，改用「迁槽」而非「延时」来解决**（见 §7）。

### 6. 待确认 / 次要风险

1. **keepalive 是否豁免「分类并发」**：日志显示 `Persistence 1/1` 满 925 次影响的是 WriteQueue 刷盘等，而 Merkle 增量仍能跑（n=93，max 20.26s），倾向认为 keepalive 生效；但 keepalive 豁免的是资源准入还是分类并发，需复核 `task_scheduler.rs` 后定论。
2. **51 侧 repo=2 `peer_max_seq=null`**：本机→51 peer repo lag 无法计算（本机侧显示 lag=0）。peer 总数差距 10,813 与此是否相关，待查。
3. **w0_seq 双端不一致**（本机 1,047,571 / 51 1,535,777）导致 F6 现场重建出的清单块数不同（本机 74 / 51 82），重建清单天然漂移 —— 这是 F7 自愈要覆盖的场景，也说明「现场重建」只是权宜，请求方持有旧清单时必然反复校验失败。
4. 本机日志 `stdout.log` 已 477MB、51 侧 173MB，建议滚动/降级 DEBUG。

### 7. 下一步建议（**未执行，待确认**）

| 优先级 | 动作 |
|---|---|
| P0 | 回滚 F8a 的 900s 超时，先把 Federation 槽饥饿解除，验证 F6/F7 真实效果 |
| P0 | 把长 await 的网络任务（Range 对账、Gossip 传播、delta 拉取）迁出 Federation 槽——独立 `TaskCategory` 或后台独立 tokio task 不受并发槽限制；或改成分帧让出（跑 N 行就返回，下一轮续跑） |
| P1 | bootstrap 续传本身不允许长期占槽：拆成「发一块 → 返回 → 下一轮续一块」的分帧模式（当前一次跑 164s） |
| P1 | F6 的现场重建改为**持久化 manifest 到 DB**（而非仅内存），消除重启漂移；F7 自愈才有稳定基准 |
| P2 | 验收标准定为：双端 `bootstrap.done_chunks` 单调递增，`ops_lag.repo=1` 双向收敛到 0 |

## 变更历史

| 日期 | 版本 | 变更内容 |
|---|---|---|
| 2026-09-16 | v1.0 | 初始版本，记录 8 socket、预测式自适应、TaskScheduler 纳管 |
| 2026-09-21 | v1.1 | 追加联邦同步诊断报告：DB 权威口径收敛数据、F6/F7/F8 落地状态、Federation 槽饥饿根因、F8a 回滚建议 |
| 2026-09-22 | v1.2 | **联邦同步去 Merkle 化（协议 v8）**：Merkle 反熵全退场，Range 反熵成为唯一兜底通道（详见下节与 ADR-007）。v1.1 诊断报告中的 Federation 槽饥饿分析里，Merkle 相关任务（联邦Merkle反熵/联邦Merkle批量flush/Merkle增量×4/Merkle冷重算×4）已随本次重构整体消失 |

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
