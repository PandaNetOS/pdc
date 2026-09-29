# 自适应 IO 调度器设计方案

> 状态：设计中
> 日期：2026-09-28
> 作者：PDC 团队
> 关联文档：[06-performance.md](./06-performance.md)、[11-billion-scale-storage.md](./11-billion-scale-storage.md)

## 1. 背景与问题

### 1.1 现状

PDC 当前使用 SQLite WAL 模式持久化所有数据（DHT 节点、Peer、Infohash、Tracker）。IOScheduler 统一调度写入，配置为固定速率：

- `steady_tick_ms = 10ms`，`writes_per_tick = 10` → 硬编码 1000 条/s
- `wal_checkpoint_interval_ms = 100ms` → 每秒 10 次 fsync
- `wal_autocheckpoint = 1000` 页 → SQLite 自动 checkpoint
- 无 IO 性能探测，不区分 SSD/HDD

### 1.2 问题现象

2026-09-28 部署到 192.168.30.51（HDD 机械盘）后出现：

| 现象 | 数据 |
|---|---|
| WAL 文件大小 | 333MB（比主库 176MB 还大），且不再增长也不合并 |
| WAL checkpoint 任务 | 跑了 452 秒超时被 WATCHDOG 强杀 |
| Persistence 队列 | 配置 1 并发，实际 3 个任务卡死溢出 |
| Monitor 队列 | 满载，连续延迟 85 轮，健康统计任务跑 477 秒超时 |
| Federation 队列 | 满载，delta 同步 473 秒超时 |
| WATCHDOG 累计回收 | 16 个泄漏任务 |
| 磁盘类型 | HDD（ST8000NM0016 8TB），磁盘活跃时间 38-46% |

### 1.3 根因

HDD 上一次 fsync 需 10-50ms（机械臂寻道），而当前每秒 fsync 10+ 次，磁盘 100% 忙：

- SSD：每次 fsync 0.1ms，10 次/s = 磁盘忙 1ms，99.9% 空闲
- HDD：每次 fsync 20ms，10 次/s = 磁盘忙 200ms，加上业务写入直接饱和

新节点全量同步 + 爬虫 + WAL checkpoint 三路写入叠加，WAL 增长速度超过 checkpoint 合并速度，最终堵死。

### 1.4 设计目标

PDC 必须在极低 IO 环境（HDD/网络盘/慢盘）下稳定运行，自动识别磁盘性能并调节参数，不需要人工配置：

- 不卡死：WAL 不无限增长，checkpoint 不超时
- 自适应：SSD 上跑满性能，HDD 上自动降速保稳定
- 向后兼容：所有新配置带默认值，不改变现有行为
- 崩溃可接受：HDD 降级模式下丢失最多 60 秒数据，节点数据从 DHT 可重建

---

## 2. 整体架构

```
                        业务写入请求
                       (NodeRepo/PeerRepo/...)
                              │
                              ▼
                    ┌─────────────────┐
              L0    │  写合并层       │  同 key 多次更新，内存覆盖，只留最终值
                    │  WriteCoalescer │
                    └───────┬─────────┘
                            │
                            ▼
                    ┌─────────────────┐
              L1    │  内存表层       │  写入先进 MemTable(BTreeMap)
                    │  MemTable       │  不直接 fsync
                    └───────┬─────────┘
                            │
                 ┌──────────┼──────────┐
                 │          │          │
                 ▼          ▼          ▼
            MemTable满  时间到    磁盘空闲?
                 │          │          │
                 └──────────┼──────────┘
                            ▼
                    ┌─────────────────┐
              L2    │  自适应调度器   │  TCP式cwnd + PI控制器
                    │  AdaptiveFlush  │  决定：什么时候刷、刷多少
                    └───────┬─────────┘
                            │
                            ▼
                    ┌─────────────────┐
              L3    │  磁盘画像层     │  OS计数器读SSD/HDD，自动选参数
                    │  DiskProfiler   │  启动探测 + 运行时持续监控
                    └───────┬─────────┘
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
        ┌─────────┐  ┌─────────┐  ┌─────────┐
   L4   │联邦同步  │  │爬虫调度 │  │WAL归档  │
        │限速让路  │  │降并发   │  │L5容错   │
        └─────────┘  └─────────┘  └─────────┘
              │             │             │
              └─────────────┼─────────────┘
                            ▼
                    ┌─────────────────┐
                    │  SQLite 写入    │  单事务批量提交，一次fsync
                    │  (HDD/SSD自适应) │
                    └─────────────────┘
```

---

## 3. 各层详细设计

### L0：写合并层（WriteCoalescer）

**目标：砍掉冗余写入。**

每个 repo 在内存中维护一个 BTreeMap，同 key 的多次 upsert 自动覆盖：

```rust
struct WriteCoalescer<K, V> {
    buffer: parking_lot::Mutex<BTreeMap<K, V>>,
    max_buffer: usize,      // 满了触发刷盘
    max_age: Duration,     // 超时触发刷盘
}
```

- NodeRepo：同一 node_id 的多次 update，只保留最后状态
- PeerRepo：同一 peer_key 的多次 update，只保留最后状态
- InfohashRepo / TrackerRepo 同理

**效果预估**：Active-PEX 每轮 5000 peer 扫描，同 peer 重复更新 3-5 次 → 写入量降 60-80%。

### L1：内存表层（MemTable）

**目标：随机写变顺序写。**

```
写入 → MemTable (BTreeMap，按主键有序)
         │
         ├─ 写满 max_buffer 条 → 触发刷盘
         ├─ 超过 max_age 秒 → 触发刷盘
         └─ 查询时：先查 MemTable，再查 SQLite（合并结果）
         
刷盘：把 BTreeMap 转成批量 INSERT/REPLACE 语句
     单事务执行 → 一次 commit → 一次 fsync
```

**HDD 关键优势**：BTreeMap 按主键有序，批量写 SQLite 时是顺序写，HDD 顺序写 150MB/s 无压力。

**查询一致性**：读路径先查 MemTable 再查 SQLite，合并结果返回。写入路径只写 MemTable。

### L2：自适应刷盘调度（AdaptiveFlush）

**目标：自动找到磁盘能承受的最大刷盘速率。**

借鉴 TCP 拥塞控制：

```
状态：SLOW_START → AVOID → CONGESTION → RECOVERY

SLOW_START:  每周期 cwnd ×2，探测磁盘上限
AVOID:       cwnd 线性 +1，维持平衡
CONGESTION:  检测到延迟上升/队列积压，cwnd 减半
RECOVERY:    磁盘恢复，cwnd 回到上次值的一半
```

同时 PI 控制器维持 MemTable 积压稳定：

```
误差 e = 当前脏数据量 - 目标脏数据量
调整 flush_interval = base_interval × (1 + Kp×e + Ki×∫e)
```

- cwnd 控制每次刷盘的条数（SSD: 5000-50000，HDD: 500-2000）
- flush_interval 控制多久刷一次（SSD: 5s，HDD: 30-60s）
- 两个参数连续调节，无硬编码离散档位

**拥塞检测信号**：
- 单次 batch flush 耗时比上周期增长 3 倍以上
- MemTable 积压量持续增长（消费速率 < 生产速率）
- WAL 增长速率超过 checkpoint 合并速率

### L3：磁盘画像（DiskProfiler）

**目标：不靠猜，直接读 OS 数据。**

```rust
struct DiskProfile {
    disk_type: DiskType,  // Ssd | Hdd | Unknown
    avg_write_latency_us: f64,
    avg_read_latency_us: f64,
    queue_depth: u32,
    // 派生推荐参数
    recommended_sync: SyncMode,
    recommended_flush_interval: Duration,
    recommended_batch_size: usize,
    recommended_wal_autocheckpoint: i32,
}

enum DiskType { Ssd, Hdd, Unknown }
enum SyncMode { Normal, Off }
```

- 启动时：Windows 性能计数器（`\\PhysicalDisk(*)\Avg. Disk sec/Write`）读磁盘指标
- 运行时：每 60 秒采样一次，磁盘性能变化时自动调整
- 不做写入 benchmark，用 OS 已有数据
- 平台适配：Windows 用性能计数器，Linux 读 `/sys/block/<dev>/queue/rotational`

### L4：跨模块 IO 感知

**目标：多写入源互相让路。**

```rust
// 联邦同步每批之前问一下
if io_scheduler.is_slow() {
    sync_batch_size = 500;
    delay_between_batches = 1.secs();
} else {
    sync_batch_size = 5000;
    delay_between_batches = 0;
}

// 爬虫根据 IO 负载自动降并发
if io_scheduler.backpressure_level() > 0.7 {
    crawler.set_active_sockets(current / 2);
}
```

联邦同步、爬虫、WAL checkpoint 不再各写各的，而是通过 IOScheduler 的背压信号协调：
- `io_scheduler.is_slow()` → 联邦同步降速
- `io_scheduler.backpressure_level()` → 爬虫降并发
- `io_scheduler.flush_interval()` → WAL checkpoint 任务读取当前间隔

### L5：WAL 归档容错

**目标：checkpoint 永不卡死。**

```
WAL 正常增长 → checkpoint 合并 → WAL 清空

WAL 超过阈值（如 100MB）→ 不再做 checkpoint
  → Rename 当前 WAL 为 wal_archive_N.db
  → SQLite 自动新建空 WAL
  → 后台低优先级任务慢慢合并归档文件
```

即使前面全部失效，WAL 也不会涨到 333MB 堵死——满了就切新的。归档文件由 Background 优先级任务慢慢合并，不阻塞业务写入。

---

## 4. 降级模式（Degraded Mode）

L3 探测到 HDD/极低 IO 时自动进入降级模式：

| 参数 | SSD 正常模式 | HDD 降级模式 |
|---|---|---|
| `PRAGMA synchronous` | NORMAL | OFF |
| 写入路径 | 直接写 SQLite | 写 MemTable，60s 批量刷 |
| `PRAGMA wal_autocheckpoint` | 1000 页 | 禁用（0），刷盘时手动 TRUNCATE |
| flush 间隔 | 5 秒 | 60 秒 |
| batch size | 500 | 20000 |
| 联邦同步批次 | 5000/批 | 500/批，批间间隔 1s |
| 爬虫 socket 数 | 全量 | 降半 |
| WAL 归档阈值 | 50MB | 100MB |

**触发条件**：
- 启动时 DiskProfiler 识别为 HDD，或写延迟 > 20ms
- 运行中 WAL 增长率 > checkpoint 合并率 × 1.5，持续 30 秒

**退出条件**：
- 磁盘写延迟连续 5 分钟 < 5ms，队列稳定

**崩溃代价**：丢最多 60 秒数据。节点数据从 DHT 可重建，可接受。

---

## 5. 配置项

全部带 `#[serde(default)]`，向后兼容：

```yaml
io_scheduler:
  # 总开关
  adaptive_enabled: true

  # L3: 磁盘画像
  disk_profile_enabled: true
  profile_sample_interval_secs: 60

  # L2: 自适应调度
  slow_start_cwnd: 10
  slow_start_growth: 2
  avoid_growth: 1
  congestion_factor: 0.5
  congestion_latency_multiplier: 3.0
  recovery_multiplier: 0.5

  # L0/L1: 写合并 + 内存表
  write_coalescing_enabled: true
  memtable_max_entries: 20000
  memtable_max_age_secs: 60

  # L5: WAL 归档
  wal_archive_threshold_mb: 100
  wal_archive_merge_priority: Background

  # 降级模式
  degraded_mode_write_latency_us: 20000
  degraded_mode_flush_interval_secs: 60
  degraded_mode_batch_size: 20000

  # SSD 默认参数
  ssd_synchronous: "NORMAL"
  ssd_flush_interval_secs: 5
  ssd_batch_size: 5000
  ssd_wal_autocheckpoint: 1000

  # HDD 默认参数
  hdd_synchronous: "OFF"
  hdd_flush_interval_secs: 60
  hdd_batch_size: 20000
  hdd_wal_autocheckpoint: 0
```

---

## 6. 实施路线图

| 阶段 | 内容 | 工作量 | 见效速度 |
|---|---|---|---|
| **P0（立即）** | L5 WAL 归档 + 降级模式配置参数 | 2 天 | 防止再次卡死 |
| **P1** | L0 写合并（NodeRepo/PeerRepo） | 3 天 | 写入量降 60-80% |
| **P2** | L3 磁盘画像 + 自动选参 | 2 天 | 不猜阈值 |
| **P3** | L2 TCP 式自适应刷盘 | 3 天 | 自动找最优速率 |
| **P4** | L1 MemTable 完整实现 | 5 天 | 随机写→顺序写 |
| **P5** | L4 跨模块协同 | 2 天 | 联邦/爬虫让路 |

---

## 7. 预期效果

| 指标 | 当前（51 HDD） | 升级后 |
|---|---|---|
| WAL 文件大小 | 333MB 卡死 | <100MB，自动归档 |
| checkpoint 任务超时 | 452s 被强杀 | 不超时，后台合并 |
| 写入吞吐 | 队列堵死 | HDD 顺序写 150MB/s |
| 任务泄漏 | 累计 16 个 | 0 |
| 联邦同步断开 | 频繁 | IO 自动让路，不挤 |
| 新节点启动 | 30 分钟堵死 | 自动适应，稳定运行 |
| SSD 性能 | 固定 1000 条/s | 跑满磁盘能力（50000+ 条/s） |

---

## 8. 遵循的设计原则

- **策略与执行分离**：AdaptiveFlush 决定参数，writer_loop 执行
- **暴露原子能力**：每个 repo 暴露 `flush_dirty()`、`dirty_count()` 等原子接口
- **发事件可查询可中断**：档位切换发事件，flush 可中断，IO 状态可查询
- **预留扩展点**：执行模式用 enum，资源预算用 Option
- **不硬编码周期和阈值**：所有参数可配置，默认值合理

## 修订（来自 15 号方案 §9）

1. L1「BTreeMap 有序 → HDD 顺序写」前提不成立（主键随机），目标改为"缩小随机工作集 + 减少每行页数"。
2. HDD 降级表的 `synchronous=OFF` 建议删除（WAL 下只省 checkpoint fsync，风险不匹配）。
3. `is_slow()` / `backpressure_level()` 必须先有消费方与真实口径，落地顺序：先接接口径（B1）→ 接通消费方（B1）→ 再谈自适应（C2）。
