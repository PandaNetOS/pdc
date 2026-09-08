# 贡献指南

> PeerDiscoveryCenter 项目贡献指南

## 项目概述

PDC 是 PandaNetOS 生态中的节点发现 Agent，负责通过 DHT、Tracker、PEX、爬虫等多种渠道发现 BT Peer 和 DHT 节点。

### 技术栈

- **语言**：Rust 2021 Edition
- **异步运行时**：tokio
- **数据库**：SQLite (rusqlite, WAL 模式)
- **内存同步**：parking_lot::RwLock
- **序列化**：serde + bencode
- **日志**：tracing
- **测试**：cargo test

### 架构分层

```
接入层 (Entry) → 业务服务层 (Services) → 智能层 (Intelligence) → 数据层 (Repositories) → 存储层 (Storage)
```

详细架构请参考 [architecture/00-overview.md](../architecture/00-overview.md)。

## 开发环境搭建

### 前置要求

- Rust 工具链（stable，建议 1.75+）
- Git 2.30+
- Windows / Linux / macOS（推荐 Windows，因为项目主要在 Windows 上开发）

### 克隆项目

```bash
# 注意：PandaNetOS 标准库必须与各项目同级目录
cd D:\PNOS
git clone <pdc-repo-url> PeerDiscoveryCenter
git clone <pandanetos-repo-url> PandaNetOS
```

### 构建

```bash
cd PeerDiscoveryCenter
cargo build --release
```

### 运行

```bash
# 前台运行
./target/release/pdc.exe

# 后台隐藏运行（推荐）
Start-Process -FilePath ".\target\release\pdc.exe" -WindowStyle Hidden

# 打开监控窗口
powershell -ExecutionPolicy Bypass -File .\target\monitor-ws.ps1
```

### 测试

```bash
# 运行所有测试
cargo test --all

# 运行单元测试
cargo test --lib

# 运行特定测试
cargo test test_name
```

详细测试指南请参考 [testing.md](testing.md)。

## 代码规范

### 命名规范

| 类型 | 规范 | 示例 |
|---|---|---|
| 类型/结构体/枚举 | UpperCamelCase | `NodeRepoImpl`, `DataTier` |
| 函数/方法 | snake_case | `calculate_node_score`, `save_all` |
| 变量 | snake_case | `node_count`, `last_active` |
| 常量 | UPPER_SNAKE_CASE | `MAX_HOT_IN_MEMORY` |
| 模块 | snake_case | `node_repo`, `score_maintainer` |
| Trait | UpperCamelCase | `NodeRepository`, `NodeScorer` |

### 注释规范

- 公共 API 必须有文档注释（`///`）
- 复杂逻辑必须有行内注释（`//`）
- TODO/FIXME 必须标注作者和日期：`// TODO(@username, 2026-09-08): ...`

### 错误处理

- 使用 `anyhow::Result` 作为返回类型
- 不要使用 `unwrap()`，除非确定不会 panic（测试代码除外）
- 错误信息要清晰，包含上下文信息

### 异步规范

- 所有 IO 操作必须是异步的
- 数据库操作使用 `spawn_blocking` 包装，避免阻塞 tokio 工作线程
- 不要在 async 函数中使用阻塞调用（`std::thread::sleep` 等）

### 内存安全

- 使用 `parking_lot::RwLock` 替代 `std::sync::RwLock`
- 持有写锁的时间要尽可能短
- 避免在持有锁的情况下调用 async 函数

## 架构约束

### 四层架构边界

| 层 | 可以做什么 | 禁止做什么 |
|---|---|---|
| 接入层 | 协议解析、请求路由 | 业务逻辑、智能决策 |
| 业务服务层 | 业务逻辑、流程编排 | 直接操作底层存储、自行计算评分 |
| 智能层 | 评分、冷热、选择等智能决策 | 直接操作 SQLite、业务逻辑 |
| 数据层 | 数据存储、查询、持久化 | 智能决策、业务逻辑 |
| 存储层 | 内存缓存、SQLite 持久化 | 业务逻辑、智能决策 |

### 数据归口

- DHT 节点 → NodeRepo（唯一归口）
- BT Peer → PeerRepo（唯一归口）
- Tracker → TrackerRepo（唯一归口）
- Infohash → InfohashRepo（唯一归口）

**禁止**同一份数据存储在多个地方。

### 智能层统一收口

- 评分 → ScoreSystem（唯一维护）
- 冷热 → TierSystem（唯一判定）
- 选择 → SelectSystem（唯一选择）

**禁止**业务层或数据层自行计算评分、判断冷热、实现选择逻辑。

## 提交规范

### Commit Message 格式

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Type 类型

| Type | 说明 |
|---|---|
| `feat` | 新功能 |
| `fix` | Bug 修复 |
| `docs` | 文档更新 |
| `style` | 代码格式（不影响功能） |
| `refactor` | 重构（既不是新功能也不是修复） |
| `perf` | 性能优化 |
| `test` | 测试相关 |
| `chore` | 构建/工具/依赖相关 |
| `arch` | 架构决策/重构 |

### Scope 范围

- `crawler` — DHT 爬虫
- `tracker` — Tracker 服务
- `discover` — 发现服务
- `probe` — 探测服务
- `storage` — 数据存储
- `intelligence` — 智能层
- `api` — API 接口
- `config` — 配置
- `docs` — 文档
- `ci` — CI/CD

### 示例

```
feat(intelligence): 增量评分模型与脏标记机制

- NodeRepository trait 添加 mark_dirty/dirty_nodes/clear_dirty 方法
- NodeScorer 添加 rescore_dirty 增量重算方法
- ScoreMaintainer 改为增量(10s)+全量(300s)双模式
- 性能：评分更新延迟从 60s 降到 10s

Refs: #123
ADR: docs/adr/003-scoring-model.md
```

## PR 流程

### 1. 创建分支

```bash
git checkout -b feat/your-feature-name
```

### 2. 开发与测试

- 编写代码
- 编写单元测试
- 运行测试确保通过
- 运行 clippy 确保无警告

### 3. 提交代码

```bash
git add .
git commit -m "feat(scope): description"
```

### 4. 推送并创建 PR

```bash
git push origin feat/your-feature-name
```

在 GitHub 上创建 PR，填写 PR 模板。

### 5. Code Review

- 至少 1 个维护者审查通过
- 所有 CI 检查通过
- 讨论并解决所有评论

### 6. 合并

- Squash and Merge（推荐）
- 合并后删除分支

## 常见问题

### Q: 如何添加新的发现渠道？

A: 在 `src/services/` 下创建新的服务模块，通过 Repo trait 访问数据，通过 SelectSystem 选择节点。不要直接操作底层存储。

### Q: 如何添加新的评分维度？

A: 在 `src/intelligence/` 下修改对应的 Scorer，更新 ScorerConfig 配置。不要在业务层或数据层计算评分。

### Q: 如何调试性能问题？

A: 
1. 查看日志中的性能统计（写入统计、评分重算耗时等）
2. 使用 `cargo flamegraph` 生成火焰图
3. 使用资源监视器查看磁盘 IO 和 CPU 使用率
4. 参考 [architecture/05-runtime-flow.md](../architecture/05-runtime-flow.md) 了解定时任务调度

### Q: 数据库文件在哪里？

A: 默认在 `target/data/pdc.db`（WAL 模式，还有 pdc.db-wal 和 pdc.db-shm）。

## 联系方式

- 项目维护者：@panda
- 生态文档：[AGENTS.md](../../AGENTS.md)
- 架构笔记：[ARCHITECTURE-NOTES.md](../../ARCHITECTURE-NOTES.md)
