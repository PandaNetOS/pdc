# 架构决策记录 (ADR)

> Architecture Decision Records — 记录重要的架构决策及其背景

## 什么是 ADR

ADR (Architecture Decision Record) 是一种轻量级的文档，用于记录重要的架构决策。每个 ADR 记录：
- **背景**：为什么需要做这个决策
- **决策**：具体做了什么决定
- **后果**：这个决策带来的影响（正面和负面）
- **替代方案**：考虑过哪些其他方案，为什么不选

## ADR 列表

| 编号 | 标题 | 状态 | 日期 |
|---|---|---|---|
| [001](001-architecture-boundary.md) | 架构边界与分层原则 | ✅ 已采纳 | 2026-09-08 |
| [002](002-intelligence-layer.md) | 智能层统一收口（评分/冷热/选择） | ✅ 已采纳 | 2026-09-08 |
| [003](003-scoring-model.md) | 增量评分模型与脏标记机制 | ✅ 已采纳 | 2026-09-08 |
| [004](004-performance-targets.md) | 千万级数据性能目标与优化架构 | ✅ 已采纳 | 2026-09-08 |
| [005](005-intelligent-control-center.md) | 智能控制中心（ICC）设立与统一控制收口 | ✅ 已采纳 | 2026-09-10 |

## 如何编写新 ADR

1. 复制 [000-template.md](000-template.md) 为 `NNN-title.md`
2. 填写各个章节
3. 在本 README 的 ADR 列表中添加条目
4. 提交 PR，在 PR 描述中引用 ADR 编号

## ADR 状态说明

| 状态 | 说明 |
|---|---|
| 📋 提议中 | 正在讨论，尚未决定 |
| ✅ 已采纳 | 已决定并实施 |
| 🔄 修订中 | 已采纳但正在修订 |
| ❌ 已废弃 | 已被新的 ADR 替代 |
| ⏸️ 已暂停 | 暂时搁置，未来可能重新考虑 |

## 相关资源

- [ADR 官方网站](https://adr.github.io/)
- [ThoughtWorks 技术雷达：ADR](https://www.thoughtworks.com/radar/techniques/lightweight-architecture-decision-records)
- [Michael Nygard 原始文章](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
