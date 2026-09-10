# PeerDiscoveryCenter 文档中心

> P2P 节点发现中心 — 完整文档体系

## 📚 文档导航

### 架构文档 (`docs/architecture/`)

| 文档 | 说明 | C4 层级 |
|---|---|---|
| [00-overview.md](architecture/00-overview.md) | 架构总览，快速了解系统全貌 | — |
| [01-system-context.md](architecture/01-system-context.md) | 系统上下文图，外部依赖与交互 | Level 1 |
| [02-container.md](architecture/02-container.md) | 容器图，内部模块划分与通信 | Level 2 |
| [03-intelligence.md](architecture/03-intelligence.md) | 智能层详细设计（评分/冷热/选择） | Level 3 |
| [04-data-model.md](architecture/04-data-model.md) | 数据模型，SQLite 表结构与 Repo 设计 | — |
| [05-runtime-flow.md](architecture/05-runtime-flow.md) | 运行时流程，关键业务时序图 | — |
| [06-performance.md](architecture/06-performance.md) | 千万级性能优化设计 | — |
| [07-federation.md](architecture/07-federation.md) | 联邦网络架构（多实例数据同步） | — |
| [08-intelligent-control-center.md](architecture/08-intelligent-control-center.md) | 智能控制中心（ICC）详细设计 | Level 3 |

### 架构决策记录 (`docs/adr/`)

| ADR | 标题 | 状态 | 日期 |
|---|---|---|---|
| [001](adr/001-architecture-boundary.md) | 架构边界与分层原则 | ✅ 已采纳 | 2026-09-08 |
| [002](adr/002-intelligence-layer.md) | 智能层统一收口（评分/冷热/选择） | ✅ 已采纳 | 2026-09-08 |
| [003](adr/003-scoring-model.md) | 增量评分模型与脏标记机制 | ✅ 已采纳 | 2026-09-08 |
| [004](adr/004-performance-targets.md) | 千万级数据性能目标与优化架构 | ✅ 已采纳 | 2026-09-08 |
| [005](adr/005-intelligent-control-center.md) | 智能控制中心（ICC）设立与统一控制收口 | ✅ 已采纳 | 2026-09-10 |
| [模板](adr/000-template.md) | ADR 编写模板 | — | — |

### 开发文档 (`docs/development/`)

| 文档 | 说明 |
|---|---|
| [contributing.md](development/contributing.md) | 贡献指南，代码规范与 PR 流程 |
| [testing.md](development/testing.md) | 测试指南，单元/集成/性能测试 |

## 🏗️ 项目管理

### GitHub Project 工作流

```
GitHub
   │
┌──────────────┼──────────────┐
│              │              │
Code         Architecture    Project
│              │              │
│           Mermaid         Issues
│           C4              Tasks
│           ADR             Milestones
│
└──────────────┬──────────────┘
               │
             PR
               │
            Review
               │
             Merge
```

### Issue 分类

- **🐛 Bug**：功能异常、崩溃、性能问题
- **✨ Feature**：新功能、新能力
- **📝 Docs**：文档改进
- **♻️ Refactor**：代码重构、技术债务
- **🔧 Chore**：构建、依赖、CI/CD

### Milestone 规划

| Milestone | 目标 | 状态 |
|---|---|---|
| v0.1 — 基础框架 | DHT 爬虫 + Tracker + 基础存储 | ✅ 完成 |
| v0.2 — 数据统一 | 四大 Repo 统一归口 + 评分系统 | 🔄 进行中 |
| v0.3 — 智能增强 | 增量评分 + 冷热分层 + 节点选择 | 📋 规划中 |
| v1.0 — 生产就绪 | 性能优化 + 监控告警 + 文档完善 | 📋 规划中 |

## 🔗 相关资源

- [PandaNetOS 生态文档](../AGENTS.md) — 生态项目总览
- [架构笔记](../ARCHITECTURE-NOTES.md) — 历史架构讨论记录
- [C4 Model](https://c4model.com/) — 架构图建模方法
- [ADR 最佳实践](https://adr.github.io/) — 架构决策记录指南
