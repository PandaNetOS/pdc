# 00 — 架构总览

> PeerDiscoveryCenter (PDC) — P2P 节点发现中心

## 一句话定位

PDC 是 PandaNetOS 生态中的**节点发现 Agent**，负责通过 DHT、Tracker、PEX、爬虫等多种渠道发现 BT Peer 和 DHT 节点，为下载执行 Agent (spde) 提供高质量的节点资源。

## 核心能力

| 能力 | 说明 | 状态 |
|---|---|---|
| DHT 爬虫 | 主动爬行 DHT 网络，发现节点和 infohash | ✅ |
| Tracker 发现 | 通过 UDP/HTTP Tracker 获取 Peer 列表 | ✅ |
| 超级 Tracker | 内置 Tracker 服务，接收外部 announce | ✅ |
| PEX 节点交换 | Peer Exchange 协议扩展 | ✅ |
| 智能评分 | 节点/Peer/Tracker 多维度质量评分 | ✅ |
| 冷热分层 | 数据温度管理，优化内存与持久化 | ✅ |
| 统一数据层 | 四大 Repo 归口，SQLite 持久化 | ✅ |

## 架构分层

### 分层架构总览

> 四层架构：业务服务通过智能层进行决策，通过数据层访问数据，最终持久化到存储层。

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'18px', 'fontFamily':'Segoe UI, Arial, sans-serif'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80, 'htmlLabels': true, 'curve':'basis'}}}%%
graph LR
    %% 样式定义：四层四色
    classDef service fill:#bbf,stroke:#333,stroke-width:2px;
    classDef intelligence fill:#bfb,stroke:#333,stroke-width:2px;
    classDef repo fill:#fbb,stroke:#333,stroke-width:1px;
    classDef storage fill:#ffb,stroke:#333,stroke-width:1px;

    subgraph L1 ["业务服务层 Services"]
        direction TB
        S1["🔍 Discover 发现服务"]
        S2["🕷️ Crawler 爬虫服务"]
        S3["📡 Tracker 服务"]
        S4["🔬 Probe 探测服务"]
        S5["🔄 Pex PEX服务"]
        S6["🌐 Nat NAT穿透"]
    end

    subgraph L2 ["智能层 Intelligence"]
        direction TB
        I1["📊 ScoreSystem 评分"]
        I2["🎚️ TierSystem 冷热分层"]
        I3["🎯 SelectSystem 节点选择"]
        I4["💚 HealthScorer 健康度"]
    end

    subgraph L3 ["数据层 Repositories"]
        direction TB
        R1["📍 NodeRepo DHT节点"]
        R2["👥 PeerRepo BT Peer"]
        R3["📋 TrackerRepo Tracker"]
        R4["🔑 InfohashRepo Infohash"]
    end

    subgraph L4 ["存储层 Storage"]
        direction TB
        DB[("💾 SQLite WAL 持久化")]
        MEM["⚡ 内存缓存 parking_lot"]
    end

    %% 层间调用
    L1 ==>|"调用智能决策"| L2
    L2 ==>|"读写数据"| L3
    L3 ==>|"持久化+缓存"| L4
    L1 -.->|"直接数据访问"| L3

    %% 应用样式
    class S1,S2,S3,S4,S5,S6 service;
    class I1,I2,I3,I4 intelligence;
    class R1,R2,R3,R4 repo;
    class DB,MEM storage;
```

### 核心数据流

> 关键依赖路径：实线表示强依赖调用，业务服务可通过 trait 直接访问数据层。

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'18px', 'fontFamily':'Segoe UI, Arial, sans-serif'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80, 'htmlLabels': true, 'curve':'basis'}}}%%
graph LR
    %% 样式定义
    classDef service fill:#bbf,stroke:#333,stroke-width:2px;
    classDef intelligence fill:#bfb,stroke:#333,stroke-width:2px;
    classDef repo fill:#fbb,stroke:#333,stroke-width:1px;

    subgraph SV ["业务服务层"]
        direction TB
        Crawler["🕷️ Crawler 爬虫"]
        Discover["🔍 Discover 发现"]
        Tracker["📡 Tracker 服务"]
    end

    subgraph INT ["智能层"]
        direction TB
        SelectSystem["🎯 SelectSystem 选择"]
        ScoreSystem["📊 ScoreSystem 评分"]
        TierSystem["🎚️ TierSystem 冷热"]
        HealthScorer["💚 HealthScorer 健康"]
    end

    subgraph REPO ["数据层"]
        direction TB
        NodeRepo["📍 NodeRepo"]
        PeerRepo["👥 PeerRepo"]
        TrackerRepo["📋 TrackerRepo"]
        InfohashRepo["🔑 InfohashRepo"]
    end

    %% 路径1：爬虫选择优质节点 → 爬取 → 存储
    Crawler -->|"选择节点"| SelectSystem
    SelectSystem -->|"返回候选"| NodeRepo
    Crawler -->|"存储节点"| NodeRepo

    %% 路径2：发现服务 → 选择节点 → 存储Peer
    Discover -->|"选择节点"| SelectSystem
    Discover -->|"存储Peer"| PeerRepo

    %% 路径3：评分维护 → 多维度评分
    ScoreSystem -->|"更新评分"| NodeRepo
    ScoreSystem -->|"更新评分"| PeerRepo
    ScoreSystem -->|"更新评分"| TrackerRepo

    %% 路径4：冷热分层 → 统一温度判定
    TierSystem -->|"温度判定"| NodeRepo
    TierSystem -->|"温度判定"| PeerRepo
    TierSystem -->|"温度判定"| TrackerRepo
    TierSystem -->|"温度判定"| InfohashRepo

    %% 路径5：健康度计算（只读）
    HealthScorer -->|"只读统计"| NodeRepo
    HealthScorer -->|"只读统计"| PeerRepo
    HealthScorer -->|"只读统计"| TrackerRepo

    %% 路径6：Tracker 直接数据访问
    Tracker -->|"读写"| TrackerRepo
    Tracker -->|"读写"| InfohashRepo
    Tracker -->|"读写"| PeerRepo

    %% 应用样式
    class Crawler,Discover,Tracker service;
    class SelectSystem,ScoreSystem,TierSystem,HealthScorer intelligence;
    class NodeRepo,PeerRepo,TrackerRepo,InfohashRepo repo;
```

## 设计原则

### 1. 智能层统一收口
所有智能决策（评分、冷热、选择）统一在 intelligence 层，数据层只负责存储，业务层只负责业务逻辑。

### 2. 数据层唯一真相源
四大 Repo (Node/Peer/Tracker/Infohash) 是所有关键数据的唯一归口，业务模块通过 trait 访问，不绕过 Repo 直接操作底层。

### 3. 增量优先
评分重算、数据持久化等操作优先采用增量方式，避免全量操作带来的性能问题。

### 4. 可观测性
关键操作都有统计和日志，支持监控和问题定位。

## 技术栈

| 类别 | 技术 |
|---|---|
| 语言 | Rust 2021 Edition |
| 异步运行时 | tokio |
| 数据库 | SQLite (rusqlite, WAL 模式) |
| 内存同步 | parking_lot::RwLock |
| 序列化 | serde + bencode |
| 网络 | tokio::net (UDP/TCP) |
| 日志 | tracing |
| 测试 | cargo test |

## 关键指标

| 指标 | 当前值 | 目标 |
|---|---|---|
| DHT 节点数 | 60,000+ | 1,000,000+ |
| Peer 数 | 800+ | 10,000+ |
| Infohash 数 | 90+ | 1,000+ |
| Tracker 数 | 79 | 200+ |
| 评分重算延迟 | 增量 10s | 增量 5s |
| 磁盘 IO | < 1 MB/s | < 0.5 MB/s |

## 下一步

- 阅读 [01-system-context.md](01-system-context.md) 了解系统上下文
- 阅读 [02-container.md](02-container.md) 了解内部模块划分
- 阅读 [03-intelligence.md](03-intelligence.md) 了解智能层设计
