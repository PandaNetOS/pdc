# 05 鈥?杩愯鏃舵祦绋?

> 鍏抽敭涓氬姟鏃跺簭鍥句笌瀹氭椂浠诲姟璋冨害

## 鍚姩娴佺▼

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'16px'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80}, 'sequence': {'actorMargin': 50, 'messageMargin': 20}}}%%
sequenceDiagram
    participant Main as main.rs
    participant Storage as Storage
    participant Repos as 鍥涘ぇ Repo
    participant Intel as Intelligence 灞?
    participant Services as 涓氬姟鏈嶅姟
    participant Entry as 鎺ュ叆灞?

    Main->>Storage: open(pdc.db)
    Storage-->>Main: SQLite 杩炴帴(WAL)

    Main->>Repos: NodeRepo/PeerRepo/TrackerRepo/InfohashRepo::new()
    Main->>Repos: load_all() 浠?SQLite 鍔犺浇
    Repos-->>Main: 鍔犺浇瀹屾垚(60000+鑺傜偣)

    Main->>Intel: ScoreMaintainer::new()
    Main->>Intel: TierManager::new()
    Main->>Intel: HealthScorer::new()

    Main->>Services: Discover/Crawler/Tracker/Probe/Pex/Nat
    Main->>Entry: HTTP REST API / WebSocket / UDP Tracker / DHT 绔彛

    Note over Main: 鍚姩瀹氭椂浠诲姟
    Main->>Intel: ScoreMaintainer.start() 澧為噺10s/鍏ㄩ噺300s
    Main->>Intel: TierManager.run() 姣?00s妫€鏌?
    Main->>Main: 鏁版嵁鎸佷箙鍖?姣?00s
    Main->>Main: peer_history flush 姣?0s
    Main->>Main: WAL checkpoint 姣?00s
    Main->>Main: 鍋ュ悍妫€鏌?姣?00s
    Main->>Main: 鍐欏叆缁熻 姣?0s

    Main-->>Main: 鍚姩瀹屾垚锛岀洃鍚鍙?
```

## DHT 鐖櫕娴佺▼

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'16px'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80}, 'sequence': {'actorMargin': 50, 'messageMargin': 20}}}%%
sequenceDiagram
    participant DHT as DHT 缃戠粶
    participant Crawler as CrawlerService
    participant Select as SelectSystem
    participant NodeRepo as NodeRepo
    participant Score as ScoreMaintainer

    Note over Crawler: 姣?crawl_interval 绉?
    Crawler->>Select: select_diverse_nodes(count, max_per_subnet)
    Select->>NodeRepo: top_nodes_sync(500)
    NodeRepo-->>Select: Top 500 鑺傜偣
    Select->>Select: 鎸?ID 鍒?6妗?+ IP /24 鍘婚噸
    Select-->>Crawler: 澶氭牱鎬ц妭鐐瑰垪琛?

    loop 姣忎釜鑺傜偣
        Crawler->>DHT: send find_node(target)
        DHT-->>Crawler: find_node 鍝嶅簲(鑺傜偣鍒楄〃)
        Crawler->>NodeRepo: add_node_sync(id, addr)
        Note over NodeRepo: 鏂拌妭鐐圭珛鍗宠绠楀垵濮嬭瘎鍒?
        Crawler->>NodeRepo: record_query_with_nodes_sync(addr, latency, nodes)
        Note over NodeRepo: 鏇存柊缁熻 + mark_dirty(addr)
        Crawler->>Crawler: chain_crawl(鏂拌妭鐐?
    end

    Note over Score: 姣?0绉?澧為噺閲嶇畻)
    Score->>NodeRepo: dirty_nodes()
    NodeRepo-->>Score: 鑴忚妭鐐瑰湴鍧€鍒楄〃
    Score->>Score: 璁＄畻姣忎釜鑴忚妭鐐硅瘎鍒?
    Score->>NodeRepo: update_scores_batch([(addr, score), ...])
    Score->>NodeRepo: clear_all_dirty()
```

## Tracker 鍙戠幇娴佺▼

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'16px'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80}, 'sequence': {'actorMargin': 50, 'messageMargin': 20}}}%%
sequenceDiagram
    participant Tracker as 鍏叡 Tracker
    participant TrackerSvc as TrackerService
    participant TrackerRepo as TrackerRepo
    participant PeerRepo as PeerRepo
    participant InfohashRepo as InfohashRepo

    Note over TrackerSvc: 姣?tracker_interval 绉?
    TrackerSvc->>TrackerRepo: active_trackers()
    TrackerRepo-->>TrackerSvc: 娲昏穬 Tracker 鍒楄〃(鎸夎瘎鍒嗘帓搴?

    loop 姣忎釜 Tracker
        TrackerSvc->>Tracker: announce(infohash, port)
        Tracker-->>TrackerSvc: announce 鍝嶅簲(Peer 鍒楄〃)
        TrackerSvc->>TrackerRepo: record_request(url, success, peers, latency)
        TrackerSvc->>PeerRepo: add_peers(infohash, peers)
        TrackerSvc->>InfohashRepo: register(infohash, "tracker")
    end

    Note over Score: 姣?00绉?鍏ㄩ噺閲嶇畻)
    Score->>TrackerRepo: 鍏ㄩ噺閲嶇畻 Tracker 璇勫垎
    Score->>PeerRepo: 鍏ㄩ噺閲嶇畻 Peer 璇勫垎
```

## Peer 鎺㈡祴娴佺▼

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'16px'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80}, 'sequence': {'actorMargin': 50, 'messageMargin': 20}}}%%
sequenceDiagram
    participant Probe as ProbeService
    participant PeerRepo as PeerRepo
    participant Peer as 杩滅▼ Peer

    Note over Probe: 姣?probe_interval 绉?
    Probe->>PeerRepo: 鑾峰彇鏈帰娴?Peer(Top 100 鎸夎瘎鍒?
    PeerRepo-->>Probe: Peer 鍒楄〃

    loop 姣忎釜 Peer
        Probe->>Peer: TCP 杩炴帴鎺㈡祴
        alt 杩炴帴鎴愬姛
            Peer-->>Probe: 杩炴帴鎴愬姛
            Probe->>PeerRepo: update_probe_stats(addr, tcp_ok=true, supports_dht)
        else 杩炴帴澶辫触
            Probe->>PeerRepo: update_probe_stats(addr, tcp_ok=false, supports_dht)
        end
    end

    Note over Score: 姣?00绉?鍏ㄩ噺閲嶇畻)
    Score->>PeerRepo: 鍏ㄩ噺閲嶇畻 Peer 璇勫垎(鍚帰娴嬬粺璁?
```

## 瀹氭椂浠诲姟璋冨害

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'16px'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80}, 'sequence': {'actorMargin': 50, 'messageMargin': 20}}}%%
gantt
    title PDC 瀹氭椂浠诲姟璋冨害
    dateFormat X
    axisFormat %s

    section 璇勫垎绯荤粺
    澧為噺閲嶇畻(10s) :active, 0, 10
    澧為噺閲嶇畻(10s) :active, 10, 20
    澧為噺閲嶇畻(10s) :active, 20, 30
    鍏ㄩ噺閲嶇畻(300s) :crit, 300, 310

    section 鏁版嵁鎸佷箙鍖?
    peer_history flush(30s) :active, 0, 30
    peer_history flush(30s) :active, 30, 60
    鍏ㄩ噺淇濆瓨(300s) :crit, 300, 310

    section 鍐风儹鍒嗗眰
    娓╁害妫€鏌?300s) :crit, 300, 305

    section 瀛樺偍
    WAL checkpoint(600s) :crit, 600, 610

    section 鐩戞帶
    鍋ュ悍妫€鏌?300s) :crit, 300, 305
    鍐欏叆缁熻(10s) :active, 0, 10
```

### 瀹氭椂浠诲姟娓呭崟

| 浠诲姟 | 闂撮殧 | 璇存槑 | 浼樺厛绾?|
|---|---|---|---|
| 璇勫垎澧為噺閲嶇畻 | 10s | ScoreMaintainer 閲嶇畻鑴忚妭鐐?| 楂?|
| 鍐欏叆缁熻杈撳嚭 | 10s | IO 缁熻鏃ュ織 | 浣?|
| peer_history flush | 30s | 鎵归噺鍐欏叆鍘嗗彶璁板綍 | 涓?|
| 璇勫垎鍏ㄩ噺閲嶇畻 | 300s | ScoreMaintainer 鍏滃簳鍏ㄩ噺 | 楂?|
| 鏁版嵁鍏ㄩ噺淇濆瓨 | 300s | 鍥涘ぇ Repo 淇濆瓨鍒?SQLite | 楂?|
| 鍐风儹鍒嗗眰妫€鏌?| 300s | TierManager 娓╁害杩佺Щ | 涓?|
| 鍋ュ悍妫€鏌?| 300s | 绯荤粺鍋ュ悍搴﹁瘎浼?| 涓?|
| WAL checkpoint | 600s | SQLite WAL 鍚堝苟 | 涓?|

### 浠诲姟閿欏紑绛栫暐

涓洪伩鍏嶅涓换鍔″悓鏃剁珵浜夐攣鍜岀鐩?IO锛屼换鍔″惎鍔ㄦ椂闂撮敊寮€锛?
- 璇勫垎澧為噺閲嶇畻锛氱 0 绉掑紑濮?
- 鏁版嵁鍏ㄩ噺淇濆瓨锛氬欢杩?30 绉掑紑濮嬶紙涓庤瘎鍒嗛敊寮€锛?
- 璇勫垎鍏ㄩ噺閲嶇畻锛氬欢杩?300 绉掑紑濮?
- WAL checkpoint锛氬欢杩?300 绉掑紑濮?

## 浼橀泤鍏抽棴娴佺▼

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'16px'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80}, 'sequence': {'actorMargin': 50, 'messageMargin': 20}}}%%
sequenceDiagram
    participant Signal as 缁堟淇″彿
    participant Main as main.rs
    participant Services as 涓氬姟鏈嶅姟
    participant Repos as 鍥涘ぇ Repo
    participant Nat as NatService

    Signal->>Main: SIGTERM/SIGINT
    Main->>Main: 璁剧疆鍏抽棴鏍囧織

    Note over Main: 鍋滄鎺ュ彈鏂拌姹?
    Main->>Services: 鍋滄鎵€鏈夋湇鍔?

    Note over Main: 鍏ㄩ噺淇濆瓨鏁版嵁
    Main->>Repos: NodeRepo.save_all()
    Main->>Repos: TrackerRepo.save_all()
    Main->>Repos: InfohashRepo.save_all()
    Main->>Repos: PeerRepo.save_all()
    Repos-->>Main: 淇濆瓨瀹屾垚

    Note over Main: 閲婃斁璧勬簮
    Main->>Nat: release_all() 閲婃斁 UPnP 鏄犲皠
    Main-->>Main: 浼橀泤鍏抽棴瀹屾垚
```

## 鍏抽敭鎬ц兘鎸囨爣

### 璇勫垎绯荤粺
- 澧為噺閲嶇畻寤惰繜锛? 100ms锛堥€氬父 < 1000 涓剰鑺傜偣锛?
- 鍏ㄩ噺閲嶇畻寤惰繜锛? 5s锛?0000 鑺傜偣锛?
- 璇勫垎鏇存柊寤惰繜锛? 10s锛堜粠缁熻鏇存柊鍒拌瘎鍒嗘洿鏂帮級

### 鏁版嵁鎸佷箙鍖?
- 鍏ㄩ噺淇濆瓨寤惰繜锛? 10s锛?0000 鑺傜偣鎵归噺鍐欏叆锛?
- 鏁版嵁涓㈠け绐楀彛锛? 300s锛堟渶澶氫涪 5 鍒嗛挓鏁版嵁锛?
- 纾佺洏 IO锛? 1 MB/s锛堝畾鏈熸壒閲忓啓鍏ワ紝闈炴寔缁級

### 鑺傜偣閫夋嫨
- 澶氭牱鎬ч€夋嫨寤惰繜锛? 50ms锛圱op 500 鍊欓€夋睜锛?
- 鍐呭瓨鍒嗛厤锛?00 涓妭鐐癸紙闈?60000 鍏ㄩ噺锛?

## 涓嬩竴姝?

- 闃呰 [00-overview.md](00-overview.md) 鍥為【鏋舵瀯鎬昏
- 闃呰 [../adr/](../adr/) 浜嗚В鏋舵瀯鍐崇瓥璁板綍
