# 自适应 IO 加固实施方案（可派工版）

> 状态：可执行（B2 令牌桶处置已定稿：接线、按行计费、默认关闭）
> 代码改动：未开始。本文件当前为纯文档；实施须在能运行 cargo 的环境进行（本机 shell 不可用）
> 上游：[14-adaptive-io-hardening-review.md](./14-adaptive-io-hardening-review.md)（问题定位与因果链）、[13-adaptive-io-scheduler.md](./13-adaptive-io-scheduler.md)（原设计稿，本方案对其做了三处修正，见 §9）
> 适用对象：`pdc`（`D:\PNOS\pdc`），单进程 SQLite/WAL 架构不变
> 实施地点：本机 shell 当前不可用（`pwsh` 启动即 `0xC0000142`），代码改动需在能跑 cargo 的环境执行（飞牛 `192.168.30.35:2222` 容器，或本地 shell 恢复后）；测试目录 `D:\test\pdc`

---

## 0. 实施硬约束（每条都要在评审时对照）

1. **配置化**：所有新增参数走配置文件，`#[serde(default)]` + 默认值函数；禁止硬编码（合规项 9/10）。
2. **TaskScheduler 提调一切**：不新增模块内自跑定时；checkpoint 的触发是注册任务，执行体搬到专用线程（合规项 15）。
3. **策略与执行分离**：调度器决定"何时/多少"，`Storage`/repo 只负责"怎么做"；`Storage` 不感知背压策略。
4. **原子能力 + 可查询 + 可中断**：新增 `checkpoint_once()`、`dirty_snapshot()`、`flush_and_wait(timeout)`、`io_status()` 等原子接口（合规项 12/13/14）。
5. **部署必须同步 `config/config.yaml`**：只传二进制不传配置视为未完成。
6. **远程节点保护**：`192.168.30.51` 上的数据禁止删除；灰度先本机 → 51 只升二进制 + 配置，不做破坏性迁移。
7. **提交前跑 `check-compliance.ps1`**，全 PASS/WARN 后才 commit/push；commit message 用本文每项给出的前缀。
8. **向后兼容**：所有行为变更必须有"回退开关"，`socket_count`/`enabled` 式的"关掉即回到改造前"语义优先。

---

## 1. 批次划分、依赖与里程碑

| 批次 | 内容 | 依赖 | 工期 | 上线方式 | 回滚方式 |
|---|---|---|---|---|---|
| **A（止血）** | A1 checkpoint 专用连接+专用线程+单飞+熔断；A2 WAL 尺寸触发与自治权；A3 无用索引 DROP；A4 导入窗口自适应配额；A5 日志降噪+基础指标 | 无（互相独立，可并行开发） | 2–3 天 | 直接灰度 | 配置开关逐项回退 |
| **B（信号与语义）** | B1 背压口径重做并接通；B2 预算配置化（去硬编码）；B3 dirty/ack 与 `flush_and_wait`；B4 `/api/v1/io/status`；B5 部分索引迁移（可选、需窗口） | A5（指标）、B3（行口径 `size_hint` 是 B1 的前提）、B2（预算） | 4–6 天 | 先本机 24h → 51 | B1 关 `admission_control_enabled`；B3 保留旧路径开关 |
| **C（自适应）** | C1 磁盘画像；C2 L2 自适应控制器；C3 热冷表分离（降低随机工作集）；C4 WAL 归档/分库 | A1/A2/B1 的实测数据 | 2–4 周 | 分阶段，每阶段独立验收 | 控制器可关闭，回到固定参数 |

**关键路径**：A1 → A2 → B1 → C2。A3/A4/A5 可与 A1 并行。

**里程碑验收门槛**（不达标不进入下一批）：

- M-A：51（HDD）连续 24h：WAL 峰值 < 128MB、无 checkpoint 超时强杀、槽位泄漏 0、`fed_bootstrap_resume` 有实际执行（非恒等 0）。
- M-B：`/api/v1/io/status` 可查；`io_backpressure_poll` 在人为压盘时 level > 0.5 且周期持久化确实让路（日志+指标双证据）。
- M-C：HDD 画像自动命中；预算在压盘时自动收缩到 ≤ 1/4；SSD 上吞吐不低于改造前。

---

## 2. A 批：止血（不改架构）

### A1. checkpoint 专用连接 + 专用线程 + 单飞 + 熔断

**问题**：checkpoint 与所有写入共用 `storage.conn`（`io_scheduler.rs:303,596` / `db.rs:339-354`），且在 async fn 里同步执行（`main.rs:1241,1283`），慢盘单次数百秒 → 全节点停摆 + 任务堆积。

**改动点**

1. `src/storage/db.rs`
   - `Storage` 新增字段：
     ```rust
     /// 专用 checkpoint 连接（与写路径不共享锁）；内存库 / 测试为 None
     ckpt_conn: Option<Arc<Mutex<Connection>>>,
     /// <db>-wal 路径，用于无锁读取 WAL 尺寸
     wal_path: Option<PathBuf>,
     ```
   - `open_with_config()` 里在创建读连接池之后，额外 `Connection::open(path_ref)`，执行**精简 PRAGMA**（`busy_timeout` / `synchronous` / `cache_size=-2048`；不要在 ckpt 连接上重复 `journal_mode`/`mmap_size`/`wal_autocheckpoint` 的设置动作），存入 `ckpt_conn`；同时记录 `wal_path`。`Storage::memory()` 保持 `None`（新字段在 `memory()` 里补 `None`，两处构造点都要改：`db.rs:112-118`、`db.rs:127-133`）。
   - 新增原子接口（策略无关，只做事）：
     ```rust
     #[derive(Debug, Clone, Copy, PartialEq, Eq)]
     pub enum CheckpointMode { Passive, Truncate }

     #[derive(Debug, Clone)]
     pub struct CheckpointOutcome {
         pub mode: CheckpointMode,
         pub busy: bool,            // PRAGMA 第一个返回值 != 0
         pub wal_frames: u64,       // log
         pub checkpointed: u64,     // checkpointed
         pub elapsed: Duration,
         pub skipped: Option<&'static str>, // "memory" / "inflight"
     }

     /// 单次 checkpoint（阻塞；必须由调用方保证不在 async worker 线程执行）
     pub fn checkpoint_once(&self, mode: CheckpointMode) -> anyhow::Result<CheckpointOutcome>;
     /// 读取 <db>-wal 文件尺寸（无锁，O(1)）
     pub fn wal_bytes(&self) -> u64;
     /// 写连接的 SQLite 自动 checkpoint 开关（接管/交还）
     pub fn set_wal_autocheckpoint(&self, pages: u32) -> anyhow::Result<()>;
     ```
     `checkpoint()` / `checkpoint_truncate()`（`db.rs:337-361`）保留为薄封装（供测试与兼容），内部转调 `checkpoint_once`。
2. 新增 `src/storage/checkpoint_worker.rs`（约 150 行）
   ```rust
   pub struct CheckpointWorker {
       tx: std::sync::mpsc::Sender<Job>,
       inflight: Arc<AtomicBool>,
       stat: Arc<Mutex<WorkerStat>>,   // 上次结果、慢次数、退避截止时刻
   }
   struct Job { mode: CheckpointMode, reply: tokio::sync::oneshot::Sender<CheckpointOutcome> }

   impl CheckpointWorker {
       pub fn spawn(storage: Arc<Storage>, cfg: CheckpointConfig) -> Arc<Self>;
       /// 非阻塞触发；返回 None 表示"已有单飞/inflight，本次跳过"
       pub fn trigger(&self, mode: CheckpointMode) -> bool;
       pub fn inflight(&self) -> bool;
       pub fn stat(&self) -> WorkerStat;
   }
   ```
   - 后台线程：`std::thread::Builder::new().name("pdc-ckpt")`，循环 `for job in rx`；执行 `storage.checkpoint_once(mode)`，用 `oneshot` 回结果，最后 `inflight.store(false)`。**永远只有一个 checkpoint 在跑**（单飞），这是消除堆积的关键。
   - 熔断（策略层，放调度任务里，不放 worker）：见 A2。
3. `src/main.rs`：checkpoint 相关任务全部改为"决策 tick"，不再直接调 `storage.checkpoint()`。

**新增配置**（`IoSchedulerConfig` 或新建 `io_checkpoint` 节，全部 `#[serde(default)]`）

```yaml
io_scheduler:
  checkpoint_takeover: true              # true：应用接管 checkpoint，启动时置 wal_autocheckpoint=0
  checkpoint_tick_ms: 1000               # 决策 tick（只读原子量+文件尺寸，永不阻塞）
  checkpoint_min_interval_secs: 5        # 两次 checkpoint 最小间隔
  checkpoint_wal_soft_mb: 32             # WAL 超过此值才允许触发
  checkpoint_wal_hard_mb: 128            # 超过则无视最小间隔强制触发并告警
  checkpoint_slow_ms: 2000               # 单次超过判定为"慢"
  checkpoint_backoff_base_secs: 5
  checkpoint_backoff_max_secs: 300
  checkpoint_backoff_factor: 2.0
  checkpoint_slow_streak_alert: 3        # 连续慢 N 次 → 发事件 + warn
  checkpoint_truncate_min_wal_mb: 16     # WAL 小于此值不做 TRUNCATE
```

**新增测试**（`db.rs` / `checkpoint_worker.rs` 单测）
- `test_checkpoint_once_memory_db_skips`：内存库返回 `skipped=Some("memory")`。
- `test_checkpoint_worker_single_flight`：并发 `trigger()` ×100，只有一个进入执行（用计数注入桩验证）。
- `test_checkpoint_breaker_backoff`：注入"慢"结果序列，验证退避窗口按 factor 增长并封顶 `backoff_max_secs`。
- `test_wal_autocheckpoint_takeover`：`checkpoint_takeover=true` 时写连接 `PRAGMA wal_autocheckpoint` 读出 0。

**验收**：本机用 `diskspd`/大文件写入制造压盘，checkpoint 单次耗时下降且**不再出现** `[task_scheduler] 槽位泄漏：任务 WAL checkpoint...`；`periodic_persistence` 在压盘期间仍能完成。

**风险与回滚**：`checkpoint_takeover=false` → 回到"SQLite 自动 checkpoint + 原任务"旧行为（保留旧代码路径一个版本）。

**提交**：`perf(storage): 独立连接与专用线程执行 WAL checkpoint，消除全局锁停摆`

---

### A2. checkpoint 触发改为 WAL 尺寸 + 最小间隔（策略层）

**改动点**：`src/main.rs` 把 `wal_checkpoint_steady`（`main.rs:1210-1249`）替换为：

```rust
task_scheduler.register(
    TaskMetadata::new("io_checkpoint_tick", "WAL checkpoint 决策",
        Duration::from_millis(cfg.checkpoint_tick_ms))
        .with_category(TaskCategory::Persistence)
        .with_priority(TaskPriority::Background)
        .with_resource(ResourceProfile { cpu: Low, memory: Low, io: Low, network: Low, is_full_task: false })
        .with_initial_delay(...),
    move || { let w = worker.clone(); let s = storage.clone(); let c = cfg.clone(); async move {
        if w.inflight() { return Ok(()); }                       // 单飞
        let stat = w.stat();
        let now = Instant::now();
        if now < stat.next_allowed { return Ok(()); }            // 熔断退避窗口
        let wal = s.wal_bytes();
        if wal < c.wal_soft { return Ok(()); }                    // 未到软阈值
        if now < stat.last_at + c.min_interval && wal < c.wal_hard { return Ok(()); }
        w.trigger(CheckpointMode::Passive);                       // 非阻塞
        Ok(())
    }}
);
```

- 决策 tick 是 **O(1) 且不阻塞**（读 2 个原子 + 1 次 `fs::metadata`），因此 interval 可以小（默认 1000ms），不影响 IO。
- 慢结果由 worker 回填 `WorkerStat`，tick 里在下一次读到时计算退避：`next_allowed = last_at + min(base*factor^streak, max)`；一次快结果把 streak 归零。
- 连续慢 `>= checkpoint_slow_streak_alert` → `event_bus.publish(IoDegraded{...})` + `warn!`（合规项 14：发事件 + 可查询）。
- `wal_checkpoint_hourly_truncate`（`main.rs:1251-1291`）改为：同样走决策 tick（interval 保留 `intervals.wal_checkpoint_truncate_interval_secs`，当前默认 300s），触发条件加 `wal_bytes >= checkpoint_truncate_min_wal_mb` 且 `queue_len()==0`。
- **自治权二选一**：`checkpoint_takeover=true` 时启动即 `set_wal_autocheckpoint(0)`，并打印一行 `info!("[storage] 应用接管 WAL checkpoint（wal_autocheckpoint=0）")`；`false` 时**不注册**上面的 tick，恢复 SQLite 自动 checkpoint。**禁止两套并存。**

**验收**：稳态下 `wal_bytes` 在 `[soft, hard]` 之间振荡而非单调增长；`checkpoint_once` 返回的 `wal_frames` 单次不超过 soft/hard 换算的帧数。

**提交**：`perf(storage): checkpoint 改按 WAL 尺寸与最小间隔触发，慢盘退避熔断`

---

### A3. 删除无人使用的索引（写放大第一刀）

**依据**：`l2_shard` 列全仓库**无任何查询使用**（`grep 'l2_shard' src/` 只有 schema、迁移、写入与 `compute_l2_shard`），Merkle 已于协议 v8 退场；四个索引在每个 upsert 上都有一次 B-tree 写入。

**改动点**

1. `src/storage/db.rs`
   - 从 `init_tables()` 里**删除** `idx_*_l2_shard` 的 `CREATE INDEX`（`db.rs:542-545`）→ 新库不再创建。
   - 新增一次性迁移（放在 `init_tables()` 末尾，此时无并发写入）：
     ```rust
     /// v11：删除 v8 去 Merkle 化后无人查询的 l2_shard 索引（纯写放大）。
     /// 列与写入逻辑保留（诊断/联邦口径仍读该列值）。
     const DROPPED_INDEXES: &[&str] = &[
         "idx_dht_nodes_l2_shard", "idx_trackers_l2_shard",
         "idx_infohashes_l2_shard", "idx_peers_l2_shard",
     ];
     fn drop_unused_indexes(conn: &Connection) -> anyhow::Result<u32>;
     ```
   - 新增 `SqliteConfig.drop_unused_indexes: bool`（默认 **true**，因为可证明无查询依赖），关闭时不执行迁移。
2. `src/storage/db.rs` 的 `save_*` 路径**不动**：`l2_shard` 列值继续写（避免动协议/诊断口径）。
3. 记录一次日志：`info!("[storage] 已删除 {} 个无用索引（l2_shard），减少写入放大", n)`。

**为什么不做 VACUUM**：多 GB 库 + HDD，VACUUM 会把盘打满数小时；删除索引释放的页进 freelist，后续复用。可选（C 批）：`PRAGMA auto_vacuum=INCREMENTAL` 仅对**新库**生效，存量库不迁移。

**验证**（必须做，作为收益证据）
- 压测前后各跑一轮 10 万行 upsert，采集 `PRAGMA wal_checkpoint(PASSIVE)` 的 `wal_frames` 与主库文件增长字节，**预期下降 20%–40%**。
- `EXPLAIN QUERY PLAN` 断言：`SELECT ... FROM dht_nodes WHERE ip=?1 AND port=?2` 仍走 `sqlite_autoindex_dht_nodes_1`。

**新增测试**：`test_drop_unused_indexes_idempotent`（跑两次不报错）、`test_drop_unused_indexes_disabled_by_config`。

**提交**：`perf(storage): 删除 v8 后无人查询的 l2_shard 索引，降低写入放大`

> 注：`idx_peers_infohash` / `idx_peers_archive_infohash`（与 PK 前缀重复）**本批不动**，放 B 批，先做 EXPLAIN 证据（见 B5）。

---

### A4. bootstrap 导入窗口：从"全局 ×200"改为"自适应配额"

**问题**：`io_scheduler.rs:116-149,505-509` 的全局静态 + 固定 ×200 常量，与磁盘类型、队列积压、耗时反馈全部无关；触发方只有 `sync/mod.rs:3043` 一处。

**改动点**（保留自由函数 API，因为 `sync/mod.rs` 不持有调度器句柄 —— 这是刻意的低风险选择）

1. `src/storage/io_scheduler.rs`
   ```rust
   /// 启动时注入一次（进程生命周期内不变）
   static ADAPTIVE_CFG: OnceLock<IoAdaptiveConfig> = OnceLock::new();
   /// 窗口截止时刻（UNIX ms）
   static IMPORT_UNTIL_MS: AtomicI64 = AtomicI64::new(0);
   /// 窗口内当前预算上限（行/请求，随实测耗时 AIMD 调整）
   static IMPORT_BUDGET: AtomicUsize = AtomicUsize::new(0);
   /// 每批实测耗时的 EWMA（微秒）
   static LATENCY_EWMA_US: AtomicU64 = AtomicU64::new(0);

   pub fn init_io_adaptive_config(cfg: IoAdaptiveConfig);      // main.rs 启动调用一次
   pub fn refresh_bootstrap_import_window(ttl: Duration);      // 签名不变，内部重置预算=start_multiplier×base
   fn adjust_import_budget(elapsed: Duration);                 // writer_loop 每批后调用（AIMD）
   ```
   - `writer_loop`（`io_scheduler.rs:496-568`）：
     ```rust
     let tick_start = Instant::now();
     let cap = if import_active() { import_budget() } else { base_per_tick };
     ... execute_batch(...) ...
     let d = tick_start.elapsed();
     observe_latency(d);            // EWMA（新）
     adjust_import_budget(d);       // 仅在窗口活跃时调整（新）
     ```
   - AIMD：`d > target` → `budget /= 2`（下限 `base`）；`d < target/2` 连续 N 轮 → `budget += step`（上限 `base × max_multiplier`）。目标 `adaptive_latency_target_ms`。
2. 新常量/配置替换 `BOOTSTRAP_IMPORT_BUDGET_MULTIPLIER`，旧常量删除（避免"两套预算"）。

```yaml
io_scheduler:
  bootstrap_import_window_ttl_secs: 30
  bootstrap_import_budget_start_multiplier: 8      # 起步倍率（原来是直接 ×200）
  bootstrap_import_budget_max_multiplier: 200
  adaptive_latency_target_ms: 200
  adaptive_latency_alpha: 0.2
  adaptive_budget_grow_step: 2
  adaptive_budget_grow_after_batches: 32
```

**行为变化提示**：冷启动灌入速度会**先慢后快**（起步 ×8，压盘时还会收缩）。这是刻意的——目的是"IO 恶劣时不死"，不是"最快灌完"。若冷启动时间不可接受，用 `bootstrap_import_budget_start_multiplier` 调，不要恢复"无条件 ×200"。

**新增测试**：`test_import_budget_halves_on_slow`、`test_import_budget_grows_when_fast`、`test_import_window_expires`、`test_import_budget_floor_is_base`。

**提交**：`perf(io): bootstrap 导入预算改按实测耗时自适应，去掉无条件 ×200 解限`

---

### A5. 日志降噪 + 基础指标

**改动点**

1. `main.rs:1242`：稳态成功 `info!` → **删除**（或 `debug!`）；只在"状态变化/慢/熔断/接管"时 `info!`/`warn!`。
2. 其它高频 `info!` 排查（同批）：`[persistence] WAL checkpoint(TRUNCATE)`、`[monitor] 实体统计校准` 等，统一规则：**周期性任务的成功路径不得使用 `info!`**（在 `AGENTS.md` 里补一条）。
3. 指标落 `stats_aggregate`（复用 `storage.record_stats` / `update_aggregate`，见 `main.rs:1194` 的既有用法）：
   `io_wal_bytes`、`io_checkpoint_ms_last`、`io_checkpoint_ms_ewma`、`io_checkpoint_total`、`io_checkpoint_slow_total`、`io_batch_ms_ewma`、`io_queue_requests`、`io_import_budget`。
4. 日志文件治理：`logs/` 目录大小上限 + 滚动（配置 `logging.max_file_mb`、`logging.rotate_keep`）；本批至少提供"检测到 stdout.log > N MB 时 warn 一次"。

**验收**：稳态 10 分钟，`stdout.log` 增长 < 1MB（改造前 100ms 一条 info ≈ 6000 行/10min）。

**提交**：`chore(observability): checkpoint 稳态日志降噪并落 IO 指标`

---

## 3. B 批：信号与语义

### B1. 背压口径重做 + 真正接通消费方

**问题**：口径是"请求条数"（生产恒个位数）、消费方关闭、`set_external_io_backpressure` 无调用点。

**改动点**

1. `src/storage/io_scheduler.rs`
   - 预算双口径（同时解决 B2）：
     ```rust
     // 取请求直到 min(max_requests_per_tick, rows_per_tick 行预算)
     while batch.len() < max_requests && (max_rows == 0 || rows_in_batch < max_rows) { pop }
     ```
     `rows_in_batch += entry.request.size_hint;`（`size_hint` 从此前恒为 1 变为真实行数，见 B3）
     > 顺序约定：`size_hint` 的真实化在 B3；若 B1 先落地，压力改用"请求数 × 批均行数（EWMA，由 `execute_batch` 实测回填）"估算，B3 落地后切换为真实行数。
   - 统计/压力：
     ```rust
     queue_rows: Arc<AtomicUsize>,         // Σ size_hint（提交 +，执行 -）
     latency_ewma_us: Arc<AtomicU64>,
     pub fn pressure(&self) -> IoPressure {   // 供 /api/v1/io/status 与监控
         let p_q = clamp01((rows - low_rows) / (high_rows - low_rows));
         let p_l = clamp01((lat_ewma - target) / (slow - target));
         IoPressure { queue: p_q, latency: p_l, level: p_q.max(p_l) }
     }
     pub fn backpressure_level(&self) -> f32 { self.pressure().level }   // 旧签名保留
     ```
   - 旧 `low_watermark`/`high_watermark`（请求数）保留但标注"仅用于队列硬上限判断"，不再参与压力计算。
2. `src/main.rs`
   - `io_backpressure_poll`（`main.rs:1293-1323`）同时注入两边：
     ```rust
     let level = s.backpressure_level();
     r.set_io_load(level as f64);
     task_scheduler.set_external_io_backpressure(level);   // 新增：接通 S1-P3/三
     ```
     为此需把 `task_scheduler` 的 `Arc` 传进该闭包（构造顺序：`task_scheduler` 在 `main.rs:998` 才完成，该任务注册在 1293 → 顺序 OK）。
   - `periodic_persistence`（`main.rs:1033-1104`）执行前检查策略（策略在调度器侧、执行在 repo 侧）：
     ```rust
     if sched.as_ref().is_some_and(|s| s.backpressure_level() > cfg.persistence_backoff_level) {
         debug!("[persistence] IO 背压 {:.2}，本轮让路", level);
         return Ok(());
     }
     ```
     新增配置 `io_scheduler.persistence_backoff_level: 0.7`。
3. 联邦侧（`federation/sync/mod.rs`）：把 `sync_batch_size` / `delay_between_batches` 从固定值改为读 `io_scheduler.backpressure_level()`（若 SyncManager 拿不到句柄，则读 A4 的静态 `LATENCY_EWMA_US` + 队列行数快照，提供 `io_pressure_snapshot()` 自由函数）。

```yaml
io_scheduler:
  low_watermark_rows: 20000
  high_watermark_rows: 200000
  latency_target_ms: 50
  latency_slow_ms: 500
  persistence_backoff_level: 0.7
```

**灰度**：`config.yaml` 先只加指标观察 24h（不改行为），再打开 `task_scheduler.admission_control_enabled: true` + `admission_io_threshold: 0.6`。

**新增测试**：`test_pressure_from_rows`、`test_pressure_from_latency`、`test_pressure_takes_max`、`test_backoff_level_gate`（单测覆盖 0/0.5/0.9 三档）。

**提交**：`feat(io): 背压改按实测耗时与队列行数计算，并接通 TaskScheduler 消费方`

---

### B2. 预算配置化（消除硬编码）

`src/config.rs` 给 `IoSchedulerConfig` 补字段，`src/main.rs:385-386` 改为读配置：

```yaml
io_scheduler:
  steady_tick_ms: 10            # 原硬编码 10
  max_requests_per_tick: 64     # 原硬编码 10（语义：每 tick 最多取多少个请求）
  rows_per_tick: 5000           # 新增：每 tick 行预算，0=不限（HDD 建议 1000-2000）
  token_bucket_enabled: false   # 新增开关；接线后默认关，灰度时在 config.yaml 显式开
  token_bucket_rate: 10000      # 语义改为「行/秒」；启用时按画像给值（SSD 100000 / HDD 2000）
  token_bucket_max: 20000       # 语义改为「行」的突发上限
```

**令牌桶：定稿为「接线，按行计费」**（2026 评审决策，替代原来的"二选一"）

语义与实现：

- **语义统一**：`token_bucket_rate` = **行/秒**（补充速率），`token_bucket_max` = **行**（突发上限）。与配置注释、`IoSchedulerConfig` 文档字符串一起改，杜绝"字段名是行、注释是请求"的歧义。
- **补充点**：`writer_loop` 每 tick 用 `tick_start.elapsed()` 计算补充量并 `refill()`；`TokenBucket::refill()` / `try_acquire()` 的 `#[allow(dead_code)]`（`io_scheduler.rs:170,195`）删除，恢复为生产路径。
- **部分支付**：`try_acquire(n)` 改为 `acquire_partial(n) -> usize`（返回实际可支付的行数）。`execute_batch` 前按 `rows_in_batch` 逐条扣减，令牌不足的那条**不取**、留在队列（队列即缓冲，不做透支、不丢请求）。
- **与 `rows_per_tick` 的关系**：两者取小。`rows_per_tick` 是"**平滑**"上限（每 tick 的行预算，决定抖动），令牌桶是"**速率**"上限（长时间平均，决定吞吐天花板）。原来的请求数预算退化为 `max_requests_per_tick`。
- **优先级语义保持**：`Critical` **不扣令牌**（与 `IoPriority::Critical` 的既有文档"立即执行，不受令牌桶限制"一致，`io_scheduler.rs:33-34`）；`Important`/`Normal` 正常扣减；`Background` 仅在余额充足时执行。deadline 老化（`io_scheduler.rs:531-544`）继续作为令牌饥饿下的防饿死兜底。
- **不做**：跨 tick 透支、令牌抢占、按字节计费（`size_hint` 是行数，字节口径留给 B4 的指标）。
- **指标**：新增 `token_starved_ticks`、`rows_deferred_by_tokens`、`tokens_available`，进 `/api/v1/io/status`（B4）——否则"限流生效"无法自证。

⚠️ **默认值联动（必须一起做，否则接线即退化）**：

| 字段 | 默认值 | 理由 |
|---|---|---|
| `token_bucket_enabled` | **false** | 新增开关。现网令牌桶从未生效，接线后若默认 true 等于同时改了限流行为；默认 false 保持行为不变，在 `config.yaml` 显式开启灰度 |
| `token_bucket_rate` | 10000（保持字段默认） | **但语义从"请求/秒"变为"行/秒"**：SSD 上旧的 10000 会把入库吞吐钉在 1 万行/s，低于"跑满磁盘能力（50000+ 行/s）"的性能目标。因此 C1 磁盘画像落地后必须按档位给值：**SSD 100000 / HDD 2000 / Unknown 10000**；画像落地前若要启用，用 `config.yaml` 手工按盘型给值 |
| `token_bucket_max` | 20000（行） | 突发上限；慢盘建议 ≤ 2×`rows_per_tick`，避免"攒满令牌后一次性突发"破坏匀速语义 |

单测：`test_token_bucket_rate_is_rows_per_sec`、`test_acquire_partial_defers_remainder`、`test_critical_bypasses_tokens`（已有用例，扩展断言）、`test_refill_by_elapsed`、`test_token_bucket_disabled_by_default`。

**提交**：`refactor(io): steady/rows 预算配置化并接通令牌桶（行/秒计费，消除硬编码与死配置）`

---

### B3. dirty 清理时机修正 + `flush_and_wait`

**问题**：异步分支先清 dirty 再入队（`node_repo.rs:903-917`），失败即静默丢；`WriteQueue::flush()` 不等待、不排空（`write_queue.rs:141-152` / `io_scheduler.rs:427-432`）。

**改动点**

1. `src/storage/write_queue.rs`
   - 新增（保留旧 `send` 兼容）：
     ```rust
     pub fn send_sized<F>(&self, rows: usize, f: F) -> anyhow::Result<()>;   // 返回是否入队成功
     ```
   - 旧 `send()` 内部转调 `send_sized(1, f)` 并忽略 Err（保持行为），避免大面积改调用点。
2. `src/storage/node_repo.rs`（`peer_repo` / `infohash_repo` / `tracker_repo` 同构改）
   - 异步分支改为"**成功后清 dirty**"，与同步分支语义对齐：
     ```rust
     let dirty_addrs = self.dirty_nodes_sync();          // 快照，不清空
     if dirty_addrs.is_empty() { return Ok(()); }
     let batch = self.build_dirty_batch(&dirty_addrs);
     let rows = batch.len();
     let repo = self.clone();                            // Arc<NodeRepoImpl>
     let addrs_ack = dirty_addrs.clone();
     if let Err(e) = wq.send_sized(rows, move |conn| {
         Storage::save_dht_nodes_batch_in_tx(conn, &batch)?;
         repo.clear_dirty_batch_sync(&addrs_ack);        // 落库成功才清
         Ok(())
     }) {
         warn!("[node_repo] 提交失败，保留 dirty 待重试: {e}");
     }
     ```
   - 数据安全加分项（B3-b，建议同批做）：`dirty: FxHashSet<SocketAddr>` → `FxHashMap<SocketAddr, u64>`（addr → 最后标脏序号），清除时仅当 `stored_seq <= ack_seq`。这同时修掉现有同步分支"刷盘窗口内的更新被清掉"的竞态。
3. `src/storage/io_scheduler.rs`：新增可等待的原子能力
   ```rust
   /// 提交一个 drain 目标并等待队列排空（有超时）；返回是否在超时前排空
   pub async fn flush_and_wait(&self, timeout: Duration) -> bool;
   ```
   实现：`drain_target.store(seq_counter.load())` → `notify_one()`；`writer_loop` 每 tick 末若 `queue_len()==0 && drain_target!=0` 则 `drained.notify_waiters()`。调用方 `tokio::time::timeout` 包裹。
   用于：优雅关闭（`shutdown()` 路径）、TRUNCATE 之前、以及测试。
   `flush_barrier()` 保留但标注为"仅插队，不保证排空"，并把内部注释改成与语义一致（合规：文档与代码一致）。

**新增测试**：`test_send_sized_reports_rejection`、`test_dirty_cleared_only_after_success`（注入 commit 失败）、`test_flush_and_wait_drains`、`test_flush_and_wait_timeout`。

**提交**：`fix(storage): 异步持久化落库成功后再清 dirty，并新增可等待的 flush_and_wait`

---

### B4. `/api/v1/io/status` 与事件

**改动点**

1. `src/data_plane/mod.rs`：`AppState` 新增 `pub io_scheduler: Option<Arc<crate::storage::IoScheduler>>`（`io_scheduler` 在 `main.rs:368` 已存在，早于 `AppState` 构造点 `main.rs:924`，顺序 OK）。
2. `src/data_plane/rest_api.rs`：新增 handler + 路由（挨着现有 `get(...)` 列表，`rest_api.rs:303` 附近）
   ```
   GET /api/v1/io/status
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
3. 事件：档位/背压跨阈值（0.5 上/下）时 `event_bus.publish`，避免每 tick 刷事件（迟滞 + 边沿触发）。
4. `IoSchedulerStats` 扩展：`total_rows`、`total_bytes_hint`、`rejected`、`avg_rows_per_batch`；`latency` 用固定桶直方图（如 1/2/5/10/20/50/100/200/500/1000/5000 ms）算 p50/p95/p99，避免引入新依赖。

**提交**：`feat(api): 新增 /api/v1/io/status 暴露队列/耗时/checkpoint/预算可观测状态`

---

### B5. 索引第二刀（需证据 + 可选维护窗口）

| 项 | 动作 | 证据要求 | 成本 |
|---|---|---|---|
| B5-1 | 删 `idx_peers_infohash`、`idx_peers_archive_infohash`（与 PK 前缀重复） | 对全部 `WHERE infohash=?` / `(infohash,ip,port)` 查询跑 `EXPLAIN QUERY PLAN`，确认走 `sqlite_autoindex_peers_*` | DROP 很快（元数据 + 释放页） |
| B5-2 | 4 张表 `deleted_at` 全量索引 → `WHERE deleted_at IS NOT NULL` 部分索引 | 确认唯一需要该索引的是墓碑查询（`db.rs:979`、`peers_archive` 无此列） | **需窗口**：`CREATE INDEX` 全表扫（165 万行，HDD 数十秒~分钟） |
| B5-3 | `peer_history` 两索引 → 复合 `(infohash, discovered_at)` + 保留 `(discovered_at)` | `query_peer_history` 的 `ORDER BY discovered_at DESC` 不再需要排序 | 同 B5-2 |
| B5-4 | 评估 `idx_dht_nodes_ip_port_expr` 能否用 `(ip,port)` 元组游标替代 | 需改 range 游标语义（`db.rs:1933-1957` 与 4 个 repo 的 key 编解码），风险中 | 本批只做可行性验证，不实施 |

B5-2/B5-3 以**独立的、显式开启的维护任务**执行（`sqlite_index_v2_migration: false` 默认关，配置开启后由后台低优先级任务分表执行并打印进度），禁止放进启动路径（避免慢盘首启被单表扫拖死）。

**提交**：`perf(storage): 用部分索引替换墓碑全量索引，删除与主键重复的 peer 索引`

---

## 4. C 批：真正的自适应

### C1. 磁盘画像（DiskProfiler 的现实版）

不做"读 Windows 性能计数器"（跨平台 + 权限 + PDH 依赖），改**受控小基准 + 运行期实测**：

```rust
pub enum DiskClass { Ssd, Hdd, Unknown }

pub struct DiskProbe;                  // src/storage/disk_profile.rs
impl DiskProbe {
    /// 在 DB 同目录写一个小文件，测 fsync 延迟与随机/顺序比；总预算 < 2s，启动时跑一次
    pub fn detect(db_dir: &Path, budget: Duration) -> DiskProbeResult;
}
pub struct DiskProbeResult {
    pub class: DiskClass,
    pub fsync_ms_p50: f64,
    pub rand_seq_ratio: f64,
}
```
- 判定：`fsync_ms_p50 > 5ms` 或 `rand_seq_ratio > 20` → `Hdd`；测不动/无权限 → `Unknown`（走保守参数）。
- 结论落 `stats_aggregate`（`io_disk_class`、`io_fsync_ms_p50`）并发事件；运行期用 checkpoint/batch 的 EWMA 做**再分类**（迟滞：连续 N 次跨阈值才切换）。
- 参数选择表（全部可配，画像只决定"用哪一列"）：

| 参数 | Ssd | Hdd | Unknown |
|---|---|---|---|
| `rows_per_tick` | 50000 | 1500 | 5000 |
| `checkpoint_min_interval_secs` | 5 | 30 | 10 |
| `checkpoint_wal_soft_mb` | 32 | 96 | 64 |
| `steady_tick_ms` | 10 | 50 | 20 |
| `mmap_size` | 128MB | 0（建议实测对比） | 64MB |

**提交**：`feat(storage): 启动受控基准探测磁盘类型并据此选择 IO 参数档位`

---

### C2. L2 自适应控制器（修正版）

**被控量**（不是 fsync 频率）：`checkpoint_ms_ewma`、`queue_rows_ewma`。
**执行器**：`rows_per_tick`（AIMD）、`checkpoint_min_interval_secs`（AIMD）。
**规则**（全部参数化，档位切换发事件、可查询）：
- 慢（`ewma > slow` 或 `queue_rows > high`）：`rows_per_tick /= 2`（下限 = 画像下限），`min_interval *= 2`（上限 `checkpoint_backoff_max_secs`）。
- 稳（连续 `grow_after_batches` 批 `ewma < target`）：`rows_per_tick += step`（上限 = 画像上限），`min_interval` 减半（可回落到画像基准）。
- 迟滞带 `[target, slow]` 内不动，避免抖动。
- 每轮决策记录 `(时间, 输入, 输出, 原因)` 环形缓冲 256 条，`/api/v1/io/status` 暴露最近 20 条——**可解释**比"看起来自适应"更重要。

**为什么不用 PI**：本系统被控对象有强非线性（fsync 语义、随机写代价）和阶跃扰动（bootstrap/联邦突增），AIMD 在工程上更稳、更容易验收；PI 需要可靠的开环模型，而我们现在连基线数据都没有。等 B 批指标跑出 1–2 周数据后再评估是否升级为 PI。

**提交**：`feat(io): 引入以 checkpoint 耗时与队列行数为被控量的 AIMD 自适应预算`

---

### C3. 降低随机工作集（对 13 号文档 L1 的修正）

**必须先承认的事实**：`node_id` 是随机 hex、`ip:port` 主键也近似随机，**按主键排序无法把随机写变成顺序写**。13 号文档"BTreeMap 有序 → HDD 顺序写 150MB/s"的前提不成立。真正有效的是**缩小随机工作集 + 减少每次写的页数**：

| 手段 | 说明 | 预期 |
|---|---|---|
| 热/冷表分离 | `dht_nodes_hot`（只存最近活跃 N 万行）+ 冷表归档；随机写集中在热表（小文件/小 B-tree，页缓存命中率高） | 随机回写页数随热表大小下降 |
| 同页合并 | 同一事务内对同一行的多次更新先内存合并（L0 的真实价值） | 减少重复脏页 |
| 事务边界 | 慢盘上"更大事务、更低频率"（A2/C2 已给） | 减少 commit/checkpoint 次数 |
| 分库/分文件 | 按 repo 或按时间分库，把随机写限制在活跃库文件 | 冷库不再被随机写污染 |

C3 是**架构级改动**，本方案只给方向与前置条件（需要 B 批指标先证明瓶颈在随机写而非 checkpoint 频率），不建议与 A/B 同批实施。

### C4. WAL 归档

前置：C1/C2 落地后仍有"WAL 单调增长且 checkpoint 追不上"的场景才做。归档合并本身也是随机写，必须：归档文件只读、按优先级 Background、单飞、可分帧续跑、有硬上限（超过 N 个归档文件则转为显式告警而不是无限归档）。

---

## 5. 配置项汇总（全部 `#[serde(default)]` + 默认值函数）

| 节.字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `io_scheduler.checkpoint_takeover` | bool | true | 应用接管 checkpoint（同时置 `wal_autocheckpoint=0`） |
| `io_scheduler.checkpoint_tick_ms` | u64 | 1000 | 决策 tick 间隔 |
| `io_scheduler.checkpoint_min_interval_secs` | u64 | 5 | 两次 checkpoint 最小间隔 |
| `io_scheduler.checkpoint_wal_soft_mb` | u64 | 32 | 软阈值（达到才允许触发） |
| `io_scheduler.checkpoint_wal_hard_mb` | u64 | 128 | 硬阈值（无视最小间隔 + 告警） |
| `io_scheduler.checkpoint_slow_ms` | u64 | 2000 | 慢判定 |
| `io_scheduler.checkpoint_backoff_base_secs` | u64 | 5 | 退避基准 |
| `io_scheduler.checkpoint_backoff_max_secs` | u64 | 300 | 退避上限 |
| `io_scheduler.checkpoint_backoff_factor` | f32 | 2.0 | 退避系数 |
| `io_scheduler.checkpoint_slow_streak_alert` | u32 | 3 | 连续慢告警阈值 |
| `io_scheduler.checkpoint_truncate_min_wal_mb` | u64 | 16 | TRUNCATE 前的最低 WAL |
| `io_scheduler.steady_tick_ms` | u64 | 10 | 时间片（原硬编码） |
| `io_scheduler.max_requests_per_tick` | usize | 64 | 每 tick 请求数上限（原硬编码 10） |
| `io_scheduler.rows_per_tick` | usize | 5000 | 每 tick 行预算（平滑上限），0=不限 |
| `io_scheduler.token_bucket_enabled` | bool | false | 令牌桶总开关（接线后默认关，灰度再开） |
| `io_scheduler.token_bucket_rate` | usize | 10000 | **行/秒** 平均速率上限；启用时按画像给值（SSD 100000 / HDD 2000 / Unknown 10000） |
| `io_scheduler.token_bucket_max` | usize | 20000 | **行** 突发上限；慢盘建议 ≤ 2×`rows_per_tick` |
| `io_scheduler.low_watermark` | usize | 1000 | 旧请求数低水位（B1 后仅用于硬上限判断） |
| `io_scheduler.high_watermark` | usize | 50000 | 旧请求数高水位（B1 后仅用于拒绝 Background / 硬上限） |
| `io_scheduler.low_watermark_rows` | usize | 20000 | 行口径低水位 |
| `io_scheduler.high_watermark_rows` | usize | 200000 | 行口径高水位 |
| `io_scheduler.latency_target_ms` | u64 | 50 | 耗时目标 |
| `io_scheduler.latency_slow_ms` | u64 | 500 | 耗时慢阈值 |
| `io_scheduler.persistence_backoff_level` | f32 | 0.7 | 周期持久化让路阈值 |
| `io_scheduler.bootstrap_import_window_ttl_secs` | u64 | 30 | 导入窗口 TTL |
| `io_scheduler.bootstrap_import_budget_start_multiplier` | usize | 8 | 起步倍率 |
| `io_scheduler.bootstrap_import_budget_max_multiplier` | usize | 200 | 倍率上限 |
| `io_scheduler.adaptive_latency_target_ms` | u64 | 200 | AIMD 目标耗时 |
| `io_scheduler.adaptive_latency_alpha` | f32 | 0.2 | EWMA α |
| `io_scheduler.adaptive_budget_grow_step` | usize | 2 | 增窗步长 |
| `io_scheduler.adaptive_budget_grow_after_batches` | u32 | 32 | 稳定多少批后增窗 |
| `sqlite.drop_unused_indexes` | bool | true | 删除 l2_shard 索引迁移 |
| `sqlite.journal_size_limit_mb` | u64 | 0 | 0=不设置 |
| `sqlite.index_v2_migration` | bool | false | B5 部分索引迁移（需窗口） |
| `logging.max_file_mb` / `logging.rotate_keep` | u64 / u32 | 256 / 3 | 日志滚动 |
| `storage.disk_probe_enabled` | bool | true | C1 启动探测 |
| `storage.disk_probe_budget_ms` | u64 | 1500 | 探测时间预算 |

`config/config.yaml` 同步写入（部署必须一起传），并只写"与默认值不同"的项 + 注释。

---

## 6. 测试与验收

### 6.1 单测（每个改动项必须带，`cargo test --all`）

A1：`test_checkpoint_once_memory_db_skips`、`test_checkpoint_worker_single_flight`、`test_checkpoint_breaker_backoff`、`test_wal_autocheckpoint_takeover`
A2：`test_checkpoint_tick_skips_below_soft`、`test_checkpoint_tick_forces_above_hard`
A3：`test_drop_unused_indexes_idempotent`、`test_drop_unused_indexes_disabled_by_config`
A4：`test_import_budget_halves_on_slow`、`test_import_budget_grows_when_fast`、`test_import_window_expires`
B1：`test_pressure_from_rows`、`test_pressure_from_latency`、`test_pressure_takes_max`、`test_backoff_gate_levels`
B2：`test_budget_from_config`、`test_token_bucket_rate_is_rows_per_sec`、`test_acquire_partial_defers_remainder`、`test_refill_by_elapsed`、`test_token_bucket_disabled_by_default`
B3：`test_send_sized_reports_rejection`、`test_dirty_cleared_only_after_success`、`test_flush_and_wait_drains`、`test_flush_and_wait_timeout`
B4：`test_io_status_serializes`（契约字段快照）
C1：`test_disk_probe_classifies_hdd`（用注入的 fsync 桩）
C2：`test_aimd_shrinks_on_slow`、`test_aimd_grows_when_stable`、`test_aimd_respects_profile_clamp`、`test_latency_hysteresis`

### 6.2 慢盘模拟（本地复现，禁止只靠 51）

1. Windows：`diskspd -c1G -d120 -w50 -b4K -o32 -t2 -h` 后台压 DB 所在盘；或复制大文件循环。
2. 记录基线：`/api/v1/io/status`、`/metrics`、`/api/v1/federation/status`、`stdout.log` 尾部窗口。
3. 断言：checkpoint 无强杀、无槽位泄漏、WAL 峰值 < 硬阈值 ×2、`periodic_persistence` 完成率 ≥ 99%、`fed_bootstrap_resume` 实际执行次数 > 0。

### 6.3 51 回归用例（等价 2026-09-28 事故场景）

HDD + 冷启动全量 bootstrap + 爬虫 8 socket + 稳态 checkpoint，运行 ≥ 2h，采集 §M-A 的门槛指标。

### 6.4 合规

- 提交前 `.\check-compliance.ps1`（脚本不可用时至少 `cargo fmt --check` + `clippy -D warnings` + `cargo test --all` + `cargo build --release`）。
- 测试目录 `D:\test\pdc`（禁止仓库目录）。
- 涉及架构/配置变更 → 同步更新 `pdc/AGENTS.md` 与 `docs/architecture/`（合规项 24）。

---

## 7. 部署与回滚

| 项 | 做法 |
|---|---|
| 部署内容 | `pdc.exe` **+** `config/config.yaml`（必须同传，AGENTS.md 约束 7） |
| 灰度顺序 | 本机 `D:\test\pdc` → 51（HDD，真实恶劣 IO）→ 其它节点 |
| 回滚粒度 | 每项独立开关：`checkpoint_takeover` / `drop_unused_indexes` / `admission_control_enabled` / `rows_per_tick=0`（回旧语义）/ `disk_probe_enabled`；A4 可用 `bootstrap_import_budget_start_multiplier=200` 近似恢复旧行为（但不推荐） |
| 数据安全 | 无删除/无 schema 破坏性变更；`DROP INDEX` 可重建（`CREATE INDEX` 语句保留在代码注释与迁移脚本里）；不改协议版本，两端可分别升级 |
| 远程节点 | 51 只做二进制+配置升级；不做 `VACUUM`、不做 B5-2/B5-3 全表索引重建，除非单独安排窗口 |

---

## 8. 风险登记

| 风险 | 影响 | 缓解 |
|---|---|---|
| 接管 checkpoint 后 WAL 长期偏大 | 崩溃恢复时间变长、读放大 | `wal_hard_mb` 强制触发 + `journal_size_limit` + 崩溃恢复时间实测（目标 < 30s） |
| `DROP INDEX` 后某条冷查询退化 | 查询变慢（写变快） | 删前 `EXPLAIN QUERY PLAN` 全量核对；只删可证明无引用的索引；保留重建语句 |
| AIMD 在 SSD 上把预算压得过低 | SSD 吞吐下降 | 画像上下限夹紧；SSD 目标耗时更宽（`latency_target_ms` 按画像分档） |
| 冷启动灌入变慢 | 新节点收敛变慢 | 起步倍率可配；冷启动期用 `checkpoint_min_interval` 让路；以"不卡死"优先 |
| B3 改 dirty 语义引入回归 | 数据丢失/重复写 | 幂等 upsert 保证重复安全；先加单测再灰度；保留 `write_queue.send` 旧路径 |
| 设备上 shell/cargo 不可用 | 无法本地验证 | 在飞牛容器执行编译与合规检查（AGENTS.md 既定流程） |

---

## 9. 对 13 号文档的三处修正（务必同步回写设计稿）

1. **L1「BTreeMap 有序 → HDD 顺序写」前提不成立**：随机主键排序后仍是随机 IO。目标改为"缩小随机工作集 + 减少每行页数"（见 C3）。
2. **HDD 降级表的 `synchronous=OFF` 建议删除**：WAL 下只省 checkpoint 的 fsync，却把"丢数据"升级为"可能损坏库"，收益/风险不成立。
3. **L4 的 `is_slow()` / `backpressure_level()` 必须先有消费方与真实口径**：现状口径是请求条数（恒 0）、消费方默认关闭、注入接口无调用点。顺序应是"先接口径（B1）→ 再接通消费方（B1）→ 最后才谈自适应让路（C2）"。

---

## 10. 变更历史

| 日期 | 版本 | 变更内容 |
|---|---|---|
| — | v1.0 | 首版实施方案：A/B/C 三批、逐项改动点与代码要点、配置汇总、测试验收、部署回滚、风险登记 |
| — | v1.1 | 评审决策落定：B2 令牌桶**接线、按行计费**（新增 `token_bucket_enabled` 默认 false 灰度；`token_bucket_rate` 语义改为行/秒并与 C1 磁盘画像档位联动，避免把 SSD 吞吐钉在 1 万行/s）；补充部分支付、Critical 免疫、饥饿指标与 5 个单测 |
