# 05 — 运行时流程

> 关键业务时序图与定时任务调度

## 启动流程

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'16px'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80}, 'sequence': {'actorMargin': 50, 'messageMargin': 20}}}%%
sequenceDiagram
    participant Main as main.rs
    participant Storage as Storage
    participant Repos as 四大 Repo
    participant Intel as Intelligence 层
    participant Services as 业务服务
    participant Entry as 接入层

    Main->>Storage: open(pdc.db)
    Storage-->>Main: SQLite 连接(WAL)

    Main->>Repos: NodeRepo/PeerRepo/TrackerRepo/InfohashRepo::new()
    Main->>Repos: load_all() 从 SQLite 加载
    Repos-->>Main: 加载完成(60000+节点)

    Main->>Intel: ScoreMaintainer::new()
    Main->>Intel: TierManager::new()
    Main->>Intel: HealthScorer::new()

    Main->>Services: Discover/Crawler/Tracker/Probe/Pex/Nat
    Main->>Entry: HTTP REST API / WebSocket / UDP Tracker / DHT 端口

    Note over Main: 启动定时任务
    Main->>Intel: ScoreMaintainer.start() 增量10s/全量300s
    Main->>Intel: TierManager.run() 每300s检查
    Main->>Main: 数据持久化 每300s
    Main->>Main: peer_history flush 每30s
    Main->>Main: WAL checkpoint 每600s
    Main->>Main: 健康检查 每300s
    Main->>Main: 写入统计 每10s

    Main-->>Main: 启动完成，监听端口
```

## DHT 爬虫流程

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'16px'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80}, 'sequence': {'actorMargin': 50, 'messageMargin': 20}}}%%
sequenceDiagram
    participant DHT as DHT 网络
    participant Crawler as CrawlerService
    participant Select as SelectSystem
    participant NodeRepo as NodeRepo
    participant Score as ScoreMaintainer

    Note over Crawler: 每 crawl_interval 秒
    Crawler->>Select: select_diverse_nodes(count, max_per_subnet)
    Select->>NodeRepo: top_nodes_sync(500)
    NodeRepo-->>Select: Top 500 节点
    Select->>Select: 按 ID 分16桶 + IP /24 去重
    Select-->>Crawler: 多样性节点列表

    loop 每个节点
        Crawler->>DHT: send find_node(target)
        DHT-->>Crawler: find_node 响应(节点列表)
        Crawler->>NodeRepo: add_node_sync(id, addr)
        Note over NodeRepo: 新节点立即计算初始评分
        Crawler->>NodeRepo: record_query_with_nodes_sync(addr, latency, nodes)
        Note over NodeRepo: 更新统计 + mark_dirty(addr)
        Crawler->>Crawler: chain_crawl(新节点)
    end

    Note over Score: 每10秒(增量重算)
    Score->>NodeRepo: dirty_nodes()
    NodeRepo-->>Score: 脏节点地址列表
    Score->>Score: 计算每个脏节点评分
    Score->>NodeRepo: update_scores_batch([(addr, score), ...])
    Score->>NodeRepo: clear_all_dirty()
```

## Tracker 发现流程

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'16px'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80}, 'sequence': {'actorMargin': 50, 'messageMargin': 20}}}%%
sequenceDiagram
    participant Tracker as 公共 Tracker
    participant TrackerSvc as TrackerService
    participant TrackerRepo as TrackerRepo
    participant PeerRepo as PeerRepo
    participant InfohashRepo as InfohashRepo

    Note over TrackerSvc: 每 tracker_interval 秒
    TrackerSvc->>TrackerRepo: active_trackers()
    TrackerRepo-->>TrackerSvc: 活跃 Tracker 列表(按评分排序)

    loop 每个 Tracker
        TrackerSvc->>Tracker: announce(infohash, port)
        Tracker-->>TrackerSvc: announce 响应(Peer 列表)
        TrackerSvc->>TrackerRepo: record_request(url, success, peers, latency)
        TrackerSvc->>PeerRepo: add_peers(infohash, peers)
        TrackerSvc->>InfohashRepo: register(infohash, "tracker")
    end

    Note over Score: 每300秒(全量重算)
    Score->>TrackerRepo: 全量重算 Tracker 评分
    Score->>PeerRepo: 全量重算 Peer 评分
```

## Peer 探测流程

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'16px'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80}, 'sequence': {'actorMargin': 50, 'messageMargin': 20}}}%%
sequenceDiagram
    participant Probe as ProbeService
    participant PeerRepo as PeerRepo
    participant Peer as 远程 Peer

    Note over Probe: 每 probe_interval 秒
    Probe->>PeerRepo: 获取未探测 Peer(Top 100 按评分)
    PeerRepo-->>Probe: Peer 列表

    loop 每个 Peer
        Probe->>Peer: TCP 连接探测
        alt 连接成功
            Peer-->>Probe: 连接成功
            Probe->>PeerRepo: update_probe_stats(addr, tcp_ok=true, supports_dht)
        else 连接失败
            Probe->>PeerRepo: update_probe_stats(addr, tcp_ok=false, supports_dht)
        end
    end

    Note over Score: 每300秒(全量重算)
    Score->>PeerRepo: 全量重算 Peer 评分(含探测统计)
```

## 定时任务调度

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'16px'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80}, 'sequence': {'actorMargin': 50, 'messageMargin': 20}}}%%
gantt
    title PDC 定时任务调度
    dateFormat X
    axisFormat %s

    section 评分系统
    增量重算(10s) :active, 0, 10
    增量重算(10s) :active, 10, 20
    增量重算(10s) :active, 20, 30
    全量重算(300s) :crit, 300, 310

    section 数据持久化
    peer_history flush(30s) :active, 0, 30
    peer_history flush(30s) :active, 30, 60
    全量保存(300s) :crit, 300, 310

    section 冷热分层
    温度检查(300s) :crit, 300, 305

    section 存储
    WAL checkpoint(600s) :crit, 600, 610

    section 监控
    健康检查(300s) :crit, 300, 305
    写入统计(10s) :active, 0, 10
```

### 定时任务清单

| 任务 | 间隔 | 说明 | 优先级 |
|---|---|---|---|
| 评分增量重算 | 10s | ScoreMaintainer 重算脏节点 | 高 |
| 写入统计输出 | 10s | IO 统计日志 | 低 |
| peer_history flush | 30s | 批量写入历史记录 | 中 |
| 评分全量重算 | 300s | ScoreMaintainer 兜底全量 | 高 |
| 数据全量保存 | 300s | 四大 Repo 保存到 SQLite | 高 |
| 冷热分层检查 | 300s | TierManager 温度迁移 | 中 |
| 健康检查 | 300s | 系统健康度评估 | 中 |
| WAL checkpoint | 600s | SQLite WAL 合并 | 中 |

### 任务错开策略

为避免多个任务同时竞争锁和磁盘 IO，任务启动时间错开：
- 评分增量重算：第 0 秒开始
- 数据全量保存：延迟 30 秒开始（与评分错开）
- 评分全量重算：延迟 300 秒开始
- WAL checkpoint：延迟 300 秒开始

## 优雅关闭流程

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'16px'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80}, 'sequence': {'actorMargin': 50, 'messageMargin': 20}}}%%
sequenceDiagram
    participant Signal as 终止信号
    participant Main as main.rs
    participant Services as 业务服务
    participant Repos as 四大 Repo
    participant Nat as NatService

    Signal->>Main: SIGTERM/SIGINT
    Main->>Main: 设置关闭标志

    Note over Main: 停止接受新请求
    Main->>Services: 停止所有服务

    Note over Main: 全量保存数据
    Main->>Repos: NodeRepo.save_all()
    Main->>Repos: TrackerRepo.save_all()
    Main->>Repos: InfohashRepo.save_all()
    Main->>Repos: PeerRepo.save_all()
    Repos-->>Main: 保存完成

    Note over Main: 释放资源
    Main->>Nat: release_all() 释放 UPnP 映射
    Main-->>Main: 优雅关闭完成
```

## 关键性能指标

### 评分系统
- 增量重算延迟：< 100ms（通常 < 1000 个脏节点）
- 全量重算延迟：< 5s（60000 节点）
- 评分更新延迟：< 10s（从统计更新到评分更新）

### 数据持久化
- 全量保存延迟：< 10s（60000 节点批量写入）
- 数据丢失窗口：< 300s（最多丢 5 分钟数据）
- 磁盘 IO：< 1 MB/s（定期批量写入，非持续）

### 节点选择
- 多样性选择延迟：< 50ms（Top 500 候选池）
- 内存分配：500 个节点（非 60000 全量）

## 下一步

- 阅读 [00-overview.md](00-overview.md) 回顾架构总览
- 阅读 [../adr/](../adr/) 了解架构决策记录
