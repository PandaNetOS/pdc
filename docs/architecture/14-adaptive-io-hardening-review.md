# 自适应 IO 调度加固评审：让 PDC 在恶劣 IO 下活下来

> 状态：评审 + P0 提案（本文不含代码改动）
> 方法：静态通读 `src/`（io_scheduler / write_queue / db / main / task_scheduler / repos）+ 与 [13-adaptive-io-scheduler.md](./13-adaptive-io-scheduler.md)、ADR-004、`pdc/AGENTS.md` v9 遗留清单交叉核对
> 限制：本次评审环境 shell 不可用（`pwsh` 启动即 `0xC0000142`），未做运行期压测、未取 git 历史、未编译；所有结论均可按文中 `文件:行` 复核
> 关联：[06-performance.md](./06-performance.md)、[11-billion-scale-storage.md](./11-billion-scale-storage.md)、[13-adaptive-io-scheduler.md](./13-adaptive-io-scheduler.md)
> 可执行方案见 [15-adaptive-io-implementation-plan.md](./15-adaptive-io-implementation-plan.md)（本文 §4 的清单已在其中展开为逐项改动点、代码要点与验收）

---

## 1. 结论速览

1. **13 号文档是设计稿，代码里一层都没落地。** `DiskProfiler` / `WriteCoalescer` / `MemTable` / cwnd+PI 自适应刷盘 / WAL 归档，在全仓库零命中；现网跑的仍是 P0–P3 版 IOScheduler（优先级队列 + 10ms 时间片匀速 + 一个 v10 的 bootstrap 导入窗口）。
2. **现有"自适应闭环"在生产里是断的。** 令牌桶是死代码（`try_acquire`/`refill` 标了 `#[allow(dead_code)]`，`writer_loop` 从不取令牌）；背压唯一的消费方准入控制默认关闭；`set_external_io_backpressure()` 只有单元测试调用；`steady_tick_ms`/`writes_per_tick` 在 `main.rs:385-386` 硬编码 10/10，配置结构体里根本没有这两个字段（违反 AGENTS.md 约束 6）。
3. **恶劣 IO 下的主要杀手不是 fsync 次数。** 按影响排序是：① checkpoint 与所有写入共用**同一把连接锁**、且在 async 线程里**同步无超时**执行（慢盘单次数百秒 → 全节点停摆）；② 每行 upsert 触及 **4–5 个索引 B-tree**，checkpoint 时全部变成主库随机回写；③ 100ms `PASSIVE` checkpoint 把"WAL 顺序追加"主动换成"主库随机回写"，方向与 HDD 物理特性相反；④ 反馈信号测的是"队列里的请求条数"（生产环境恒为个位数），不是耗时/WAL/磁盘。
4. **最便宜的高收益改动在索引和 checkpoint 生命周期上**，不需要动架构：去掉 4 个无人查询的 `l2_shard` 索引、checkpoint 搬离共享写连接并加硬熔断、日志降噪、把 bootstrap 的"全局 ×200 解限"改成受约束的配额。

---

## 2. 现状核实：设计 L0–L5 vs 代码

| 层 | 13 号文档的设计 | 代码现状 | 证据 |
|---|---|---|---|
| L0 写合并 WriteCoalescer | 每 repo 一个 BTreeMap，同 key 覆盖 | **无**。repo 层只有 `dirty: RwLock<FxHashSet<SocketAddr>>` 去重（无序、非覆盖合并），`take_dirty_sync()` 取走即清 | `node_repo.rs:41,683-693,903-917` |
| L1 MemTable | 内存表 + 批量顺序刷盘 | **无** | — |
| L2 自适应刷盘（cwnd+PI） | 连续调节 flush 间隔/批量 | **无**。固定 `tick_interval = steady_tick_ms`，每 tick 最多 `writes_per_tick` 个请求、单事务提交 | `io_scheduler.rs:496-568` |
| L3 磁盘画像 DiskProfiler | 读 OS 计数器判 SSD/HDD | **无**。`rotational` / `PhysicalDisk` / `DiskType` / `sysinfo::Disks` / 写延迟字段全仓库 0 命中 | grep 全 `src/` |
| L4 跨模块 IO 感知 | 联邦/爬虫按 `is_slow()` / `backpressure_level()` 让路 | **信号存在但无人消费**：`io_backpressure_poll` 只写 ResourceMonitor；准入控制默认关闭；`set_external_io_backpressure()` 仅单测调用 | `main.rs:1294-1323`、`task_scheduler.rs:744-748,1249-1265` |
| L5 WAL 归档 | 超阈值 rename + 后台合并 | **无**。只有 100ms `PASSIVE` + 每小时 `TRUNCATE` | `main.rs:1210-1291`、`db.rs:337-361` |
| （额外）v10 导入窗口 | 文档未提 | **已实现**：全局静态 `IMPORT_UNTIL_MS`，窗口内预算 ×200，静默 30s 回落 | `io_scheduler.rs:116-149,505-509`、`sync/mod.rs:3043-3045` |

补充事实：

- `SchedulerRuntimeConfig` 有 `steady_tick_ms`/`writes_per_tick` 字段，但 `IoSchedulerConfig`（`config.rs:476-516`）**没有**对应配置项 → 生产不可调。
- `token_bucket_rate`（10000）/`token_bucket_max`（20000）两个配置项**实际不生效**：`TokenBucket::available()` 只在 `is_idle()` 里被读，取令牌路径从未被 `writer_loop` 调用。
- `IoSchedulerStats`（`io_scheduler.rs:224-246`）只有计数，**没有任何耗时/字节/行数字段**；也没有 HTTP 端点暴露它（全仓库只有 `main.rs` 与 `storage/mod.rs` 引用 IOScheduler）。
- `batch_max_size` / `batch_max_delay` 两个"请求合并"配置同样未参与 `writer_loop` 决策（合并实际由"时间片内最多取 N 个请求"完成）。
- 生产 `config/config.yaml` 里**没有** `io_scheduler:` 与 `task_scheduler:` 节 → 全部走默认值（`io_scheduler.enabled=true`、`persistence_concurrency=1`、`admission_control_enabled=false`）。

---

## 3. 恶劣 IO 下的因果链

### 3.1 单一写连接 + 长 PRAGMA = 全局停摆（P0）

- IOScheduler 持有 `storage.connection()`：写入在 `execute_batch()` 里 `self.conn.lock()` 后 `unchecked_transaction()` → 整个批次的执行期间持有这把 `std::sync::Mutex`（`io_scheduler.rs:303,596-637`）。
- `Storage::checkpoint()` / `checkpoint_truncate()` 用的是**同一把锁**（`db.rs:339-354`）。
- 两个 checkpoint 任务在 async fn 里**同步**调用它们（`main.rs:1241,1283`），不是 `spawn_blocking`。
- 任务默认超时 300s（`task_scheduler.rs:300-307`），但 `tokio::time::timeout` 掐不死"已经卡在 `std::sync::Mutex::lock()` 上的线程"：worker 线程不返回，future 就不会被 drop，锁依然被等。
- 结果：watchdog 只能回收**槽位登记**（`task_scheduler.rs:1374-1414`，"执行体已不可能返回"），于是每 100ms 又起一个 checkpoint 去抢同一把锁 → 阻塞任务堆积。这与现网"checkpoint 跑 452s 被强杀 / 累计 16 个泄漏任务"（AGENTS.md v9 遗留 #1）完全吻合。
- persistence runtime 只有 4 个 worker（`config.rs:615`），几个卡死的 checkpoint 就足以让 `periodic_persistence`、`write_queue_flush`、统计任务一起停摆。
- 更糟的是 `wal_checkpoint_steady` 本身属于 `Persistence` 分类、并发上限 1（`config.rs:209`）——一个卡死的 checkpoint 直接堵死整条持久化链路。

### 3.2 100ms PASSIVE checkpoint 在 HDD 上方向反了（P0）

- WAL 模式下业务写入是**顺序追加**（廉价，HDD 顺序 150MB/s 无压力）；`wal_checkpoint` 把 WAL 帧按**页号**回写主库，是**随机写**（HDD 昂贵，~100 IOPS 量级）。
- 也就是说 checkpoint 是一个"把顺序 IO 兑换成随机 IO"的动作。HDD 上正确策略是**少做、做大**（长 WAL、低频 checkpoint），而不是每 100ms 做一次。
- 现网还叠了第二重：`wal_autocheckpoint` 默认 **1000 页**（`config.rs:447-449`）→ SQLite 自己也在 checkpoint，与 100ms 任务重复、互相踩（v9 遗留 #1 已点名"重复且是最长持锁者"）。
- 每次 checkpoint 还要 fsync 主库（WAL + `synchronous=NORMAL` 只在 checkpoint fsync）→ 100ms 级 ≈ 10 次 fsync/s；HDD 每次寻道 10–50ms，叠加业务写入即饱和（13 号文档 §1.3 的实测口径）。
- `TRUNCATE` 更重：它需要独占，慢盘实测 435s（PASSIVE）/1244s（TRUNCATE）级别，且期间**同时**阻塞写与读。

### 3.3 索引写放大：每行 4–5 个 B-tree 随机回写（P0/P1，性价比最高）

`dht_nodes` 每次 UPSERT 触及的 B-tree：

| # | B-tree | 是否有查询使用 | 证据 |
|---|---|---|---|
| 1 | 表本身 | — | `db.rs:376-392` |
| 2 | `sqlite_autoindex_dht_nodes_1`（PK `(ip,port)`） | 大量单点查 | `db.rs:391` |
| 3 | `idx_dht_nodes_ip_port_expr((ip\|\|':' \|\|port))` | 是（range/bootstrap 的 `ORDER BY` 表达式键） | `db.rs:658-659,1945-1954` |
| 4 | `idx_dht_nodes_l2_shard(l2_shard)` | **否**（Merkle 已 v8 退场；全仓库无 `WHERE l2_shard` 查询） | `db.rs:542`；grep `l2_shard` 仅见写入 |
| 5 | `idx_dht_nodes_deleted(deleted_at)` | 仅 `deleted_at IS NOT NULL LIMIT 1` 一处；`IS NULL` 谓词用不上它 | `db.rs:546`、`db.rs:979` |

同类冗余：

- 四张表各有一个**无人查询的** `idx_*_l2_shard`（`db.rs:542-545`）。
- `idx_peers_infohash ON peers(infohash)` 与 PK `(infohash,ip,port)` 前缀完全重复（`db.rs:429,431`）；`idx_peers_archive_infohash` 同理（`db.rs:444,446`）。
- 四张表的 `deleted_at` 是**全量**索引，而实际需要的是"墓碑很少"的部分索引。
- `peer_history` 是 append-only 历史表，带两个索引（`db.rs:458-459`），批量 flush 时每行两次索引插入。

**为什么这直接决定 HDD 存亡**：WAL 只是把随机写延后到 checkpoint；随机回写的页数 ≈ 每行触及的 B-tree 页数。每行 5 个 B-tree 就是每行 5 次随机页写，1000 行/s = 5000 随机页/s——远超机械盘能力。去掉 2–3 个索引即同比例削掉 checkpoint 的随机写量，**这是投入产出比最高的一刀**（DROP INDEX 是一次性元数据操作，列与 `compute_l2_shard` 写入逻辑可原样保留）。

### 3.4 反馈信号测错了对象（P0）

- 背压口径 = 队列里的**请求条数**：`backpressure_level()` 用 `queue_len` 对 `low_watermark=1000`/`high_watermark=50000` 做线性映射（`io_scheduler.rs:435-450`）。
- 而生产者的提交粒度是"每 repo 每 10s 一个大 batch 请求"（`main.rs:1033-1104` 的 `periodic_persistence` → `node_repo.save_dirty()` → `wq.send()`，`write_queue.rs:112` 固定 `size_hint=1`），排空能力是 1000 请求/s。
- 结论：`queue_len` 常年个位数，两个水位线**永远碰不到** → `backpressure_level()` 恒为 0 → `io_backpressure_poll` 恒写入 `io_load=0`。
- 即使不为 0 也没有消费方：唯一读它的 `admission_decide()` 在 `admission_control_enabled=false` 时直接 `Allow`（`task_scheduler.rs:744-748`），而生产 yaml 没开。
- 已实现的"外部背压注入"接口 `set_external_io_backpressure()`（`task_scheduler.rs:1249-1252`）在 `main.rs` 里**从未被调用**（`main.rs:1318` 调的是 `ResourceMonitor::set_io_load`）→ AGENTS.md v9 记录的"S1-P3/三 注入外部 IO 背压"实际是断线。
- 真正该采的反馈量（目前一个都没有）：单次 batch `tx.commit()` 的耗时分布、WAL 帧数/字节、待落库**行数**（不是请求数）、队列字节数、OS 级磁盘队列/延迟。
- `set_dirty_backlog()`（`task_scheduler.rs:1268-1275`）同样无调用方。

### 3.5 "加速开关"没有刹车（P0）

- v10 的 bootstrap 导入窗口是一个**进程级全局静态**（`io_scheduler.rs:130`），窗口内 `writes_per_tick × 200`（`:505-509`），刷新点是联邦 bootstrap 每块落库成功后（`sync/mod.rs:3043-3045`）。
- 它不区分磁盘类型、不看队列积压、不看耗时反馈：一旦联邦在灌数据，**所有**写入（含爬虫稳态）一起解限。
- 这正是 2026-09-28 在 51（HDD）上"新节点全量同步 + 爬虫 + checkpoint 三路叠加打满盘"的机制之一——13 号文档给 HDD 的目标是"500–2000 条/批、30–60s 间隔"，而导入窗口给的是"2000 请求/10ms"。
- 建议方向：把"解除预算"改成"受约束的配额"——bootstrap 走独立优先级 + 独立速率上限（配置化），由实测耗时反馈收缩，且窗口内也给 checkpoint / 业务写入留保底配额。

### 3.6 异步持久化路径没有刷盘确认（P1，数据安全）

- `node_repo.save_dirty()` 的异步分支**先 `take_dirty_sync()` 清空 dirty，再 `wq.send()`**（`node_repo.rs:903-917`）。此后若事务 commit 失败、连接锁获取失败、队列拒绝（`high_watermark`/`max_queue_size`）或 payload panic 回滚，dirty 标记**已经丢了**，没有重试路径（失败只在 `io_scheduler.rs:623,636` 打一行 warn）。
- 对比同步分支（`node_repo.rs:921-950`）是"保存成功后才清 dirty，失败保留重试"——异步分支语义不一致，且慢盘恰恰是失败率最高的场景。
- `WriteQueue::flush()`（`write_queue.rs:141-152`）在调度器模式下只是提交一个 Critical 空 payload：**不等待、无完成信号、也不保证排空**（Critical 只是插队到队头，随后同批继续 pop 其余请求）。注释"提交 Critical barrier 触发队列排空"与实际语义不符；`IoScheduler::flush_barrier()`（`io_scheduler.rs:427-432`）同此。
- 影响：任何"flush 之后再做 checkpoint / 关停 / 校准"的调用点都拿不到顺序保证，符合 AGENTS.md"暴露原子能力、可查询、可中断"的整改要求。

### 3.7 日志本身就是一个 IO 源（P1）

- `wal_checkpoint_steady` 每次成功都用 `info!`（`main.rs:1242`），100ms 一次 = 10 行/s ≈ 86 万行/天；`log_level: info` 写在生产 yaml 里。
- 现网 `stdout.log` 已 477MB（AGENTS.md v9 §6.4）。慢盘上"日志追加 + 刷盘"与 DB 抢同一块盘。
- 建议：稳态成功改 `debug!` 或按"每 N 次/状态变化"聚合；关键数值改 metrics 而不是日志；日志目录加滚动与大小上限。

### 3.8 其它慢盘放大器

- `write_queue_flush` 每 5s 提交 Critical 空 payload（`main.rs:1116-1150`）：Critical 会插到 Normal 之前，虽然同批仍会 continue pop，但"时间片匀速"的语义被变形；且它同样只是入队，不解决 flush 语义。
- `mmap_size=128MB` + `cache_size=64MB`（`config.rs:441-446`）：HDD 上 mmap 的脏页由 OS 异步回写，节奏不可控，建议在画像里允许配 0 并对照测量。
- `busy_timeout=5s`（`config.rs:437-439`）：只读连接池（4 条）在 WAL 下不与写者互斥，问题不大；但 `TRUNCATE` 需要独占，慢盘下与读者互踩会放大成秒级等待。

---

## 4. 优化清单（含收益 / 成本 / 风险）

### P0 —— 1–2 天，不改架构，先止血

| # | 动作 | 预期收益 | 风险 |
|---|---|---|---|
| P0-1 | checkpoint 搬离共享写连接：专用连接 + 专用 OS 线程（`spawn_blocking` 或独立 thread）+ 硬超时 + **熔断**（超时后指数退避到分钟级，连续成功再恢复） | 消除"checkpoint 慢 → 全节点停摆"；不再出现 300s 泄漏任务堆积 | 低；注意 checkpoint 与写者仍会在 SQLite 层面短暂抢 WAL 写锁（PASSIVE 会主动让路） |
| P0-2 | checkpoint 触发改"WAL 帧数/字节阈值 + 最小间隔"，HDD 画像下低频大块；**`wal_autocheckpoint` 与应用侧 checkpoint 二选一**（不能两套并存）；设 `journal_size_limit` | HDD 上把随机回写从"每秒 10 次"降到"每 30–60s 一次大块"，fsync 次数降 1–2 个数量级 | 中；WAL 会变大（需要 P0-1 的熔断与 WAL 尺寸监控兜底），崩溃恢复时间变长（可接受，节点数据可从 DHT 重建） |
| P0-3 | `DROP INDEX` 4 个 `idx_*_l2_shard`；`idx_peers_infohash`/`idx_peers_archive_infohash`（PK 前缀重复）在压测确认后删除 | 每行少 1–3 个索引页 → checkpoint 随机回写量同比例下降（预计 −20%~40%） | 低；`DROP INDEX` 一次性、可回滚（重建索引慢，需在维护窗口做） |
| P0-4 | bootstrap 导入窗口从"全局 ×200"改为"bootstrap 专用配额（配置化）+ 耗时反馈收缩 + 保底配额" | 恶劣 IO 下不再是"一开关就打满盘" | 低-中；会降低冷启动灌入速度（需与"50 分钟硬天花板"权衡，改用配额而非解限） |
| P0-5 | checkpoint 日志降噪（info→debug/聚合）；WAL 字节数与 checkpoint 耗时进 metrics/健康探针 | 直接减少日志 IO；获得可观测性 | 低 |
| P0-6 | 接通背压消费方：`main.rs` 改调 `task_scheduler.set_external_io_backpressure(level)`；至少在背压时让 `periodic_persistence`/联邦/爬虫让路 | 让既有信号真正生效，而不是死代码 | 中；建议先以"只降速不拒绝"的方式灰度 |
| P0-7 | 把 `steady_tick_ms`/`writes_per_tick`（及其它已实现但未暴露的旋钮）补进 `IoSchedulerConfig`，带 `#[serde(default)]` | 消除硬编码（AGENTS.md 约束 6 / 校验项 9） | 低 |

### P1 —— 1 周，改信号与索引

| # | 动作 | 说明 |
|---|---|---|
| P1-1 | 给 IOScheduler 加**实测反馈**：每批 `(rows, bytes, tx 耗时)` 直方图 + WAL 帧数 + 队列**行数/字节**口径 | 背压级别改由"耗时 vs 基线"与"WAL 增长 vs 合并速率"共同决定，而不是请求条数 |
| P1-2 | 索引第二刀：4 张表 `deleted_at` 全量索引改 `WHERE deleted_at IS NOT NULL` 部分索引；评估 `dht_nodes` 的表达式索引能否用 `(ip,port)` 元组游标替代（省 1 个 B-tree，但要改 range 游标语义，风险中等） | HDD 上"索引数 = 随机回写量"，值得逐个核算 |
| P1-3 | 异步持久化补 ack/重试：payload 带完成回执，失败回填 dirty；新增 `flush_and_wait(timeout)` 原子能力 | 修 3.6 的静默丢数据；同时让"persistence 耗时"指标反映真实落库而不是入队 |
| P1-4 | 磁盘画像最小可用版：**启动时一次受控小基准 + 运行期耗时聚类**，结论落成枚举档位（只影响选参，不做高频探测） | 比 13 号文档"读 Windows 性能计数器"跨平台成本低得多，也不需要额外权限 |
| P1-5 | 长任务分帧让出：checkpoint、range 对账、bootstrap 落库改"有预算跑一段 → 返回进度 → 下一 tick 续跑" | 与 AGENTS.md v9 遗留 #1/P0（槽饥饿）同源，一并解决 |
| P1-6 | 观测端点：`/api/v1/io/status`（队列表/字节数、WAL 帧数、batch 耗时 p50/p95/p99、checkpoint 次数/耗时/上次结果、当前档位与原因） | 落实"可查询"原则；现有 IOScheduler 对外完全不可见 |

### P2 —— 2–4 周，实现 13 号文档的 L1/L2/L5，但要按修正版

| # | 动作 | 与 13 号文档的差异 |
|---|---|---|
| P2-1 | L2 自适应控制器：被控量取**"主库随机回写速率 / checkpoint 单次耗时"**，目标是把 checkpoint 耗时稳定在阈值内、队列行数稳定在目标带内；PI + 一阶低通；参数全部可配 | 不照抄"cwnd 调 fsync 频率"。fsync 次数是被控量的结果，不是目标 |
| P2-2 | L1 MemTable：分批时按**物理邻近**（rowid / l2 局部性 / 同页聚合）而非 node_id 排序 | 文档"BTreeMap 有序 → HDD 顺序写 150MB/s"在**随机主键**上不成立：排序一个随机 key 集合仍然是随机 IO。目标应改写为"降低随机回写页数" |
| P2-3 | L5 WAL 归档：rename WAL + 新 WAL + 后台合并 | 先解决"归档合并本身也是随机写"；更彻底的方向是按时间/分片把冷数据移出热库，使随机回写被限制在小而活跃的文件里 |
| P2-4 | L0 写合并：在 repo 层做真正的同 key 覆盖合并（现在只有无序 dirty 去重） | 与 MemTable 合并实现，避免两层各写一套 |

### 明确**不**建议做的

- **`synchronous=OFF`**（13 号文档 HDD 降级表）：WAL 下它只省掉 checkpoint 的 fsync，却把"最多丢 60s 数据"升级为"掉电可能损坏数据库"。收益/风险不划算，建议保持 `NORMAL` + 低频 checkpoint。
- **单纯提高 `writes_per_tick`**：慢盘上只是把"队列拥塞"换成"长事务持锁 + 更大的随机回写块"，会让 3.1 的停摆更严重。
- **让 checkpoint 任务设更长的 timeout**：v9 §5 已判定 F8a（300s→900s）为负收益——它给的只是"合法占槽更久"的许可证。正确做法是移出连接、分帧、熔断。

---

## 5. 验收标准与验证方法

指标（与 AGENTS.md 性能目标对齐）：

| 指标 | 目标 |
|---|---|
| WAL 峰值 | < 100MB（且稳态不单调增长） |
| checkpoint 单次耗时 | < 5s（HDD），无超时强杀 |
| 槽位泄漏 / WATCHDOG 回收 | 0 |
| 任一持锁操作时长 | < 1s（超出必须有指标与告警） |
| 稳态磁盘 IO | < 2MB/s；HDD 下写入吞吐 ≥ 500 行/s 且队列行数不发散 |
| 崩溃后数据损失 | ≤ 配置的 flush 窗口（并如实标注） |

方法：

1. 慢盘复现不要依赖真实 HDD：Windows 用 `diskspd`/后台大文件写入加压，Linux 用 `dm-delay`/cgroup `io.latency`（注意飞牛容器内的 `/sys` 可读性）。
2. `DROP INDEX` 前后各跑一轮全量/dirty upsert 压测，对比 `PRAGMA wal_checkpoint` 返回的帧数与主库文件增长字节数，预期帧数下降 25%–40%。
3. 测试位置按仓库规范放 `D:\test\pdc`，禁止在仓库目录跑。
4. 复现 2026-09-28 的 51 场景作为回归用例：HDD + 冷启动全量 bootstrap + 爬虫 + 稳态 checkpoint，观察 WAL 峰值、checkpoint 耗时、槽位泄漏、`fed_bootstrap_resume` 是否被饿死。

---

## 6. 附：本次核对的关键代码位置

| 主题 | 位置 |
|---|---|
| writer_loop / 时间片预算 / 导入窗口 | `src/storage/io_scheduler.rs:116-149,496-568` |
| 令牌桶（死代码） | `src/storage/io_scheduler.rs:155-211` |
| 背压级别（请求条数口径） | `src/storage/io_scheduler.rs:434-455` |
| `execute_batch` 持锁 + 事务 + 静默失败 | `src/storage/io_scheduler.rs:589-653` |
| 统计结构（无耗时字段） | `src/storage/io_scheduler.rs:224-246` |
| 调度器构造与硬编码 10/10 | `src/main.rs:368-400`（385-386） |
| checkpoint 任务（共享连接、async 内同步） | `src/main.rs:1210-1291` |
| 背压轮询（未接通 TaskScheduler） | `src/main.rs:1293-1323` |
| `periodic_persistence`（每 10s，Persistence 分类） | `src/main.rs:1033-1104` |
| IOScheduler 配置（缺 steady 旋钮） | `src/config.rs:474-573` |
| SQLite PRAGMA / 读连接池 | `src/storage/db.rs:78-122` |
| checkpoint / TRUNCATE 实现（同锁） | `src/storage/db.rs:337-361` |
| 索引定义（l2_shard / deleted_at / expr） | `src/storage/db.rs:431,446-447,458-459,542-549,656-664` |
| range SQL 的表达式键与 ORDER BY | `src/storage/db.rs:1933-1957` |
| 异步 save_dirty 先清 dirty | `src/storage/node_repo.rs:903-917` |
| WriteQueue 兼容层 / flush 语义 | `src/storage/write_queue.rs:99-152` |
| 准入控制开关与背压注入接口 | `src/intelligence/task_scheduler.rs:738-761,1246-1265` |
| 槽位泄漏回收（只回收登记） | `src/intelligence/task_scheduler.rs:1374-1414` |
| bootstrap 刷新导入窗口 | `src/federation/sync/mod.rs:3039-3046` |
| 生产配置（无 io_scheduler/task_scheduler 节） | `config/config.yaml` |

---

## 7. 变更历史

| 日期 | 版本 | 变更内容 |
|---|---|---|
| — | v1.0 | 首版评审：核实 13 号文档落地状态、定位恶劣 IO 下的 8 条因果链、给出 P0/P1/P2 优化清单与验收标准 |
