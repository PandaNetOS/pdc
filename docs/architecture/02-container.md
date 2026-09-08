# 02 鈥?瀹瑰櫒鍥?(C4 Level 2)

> PeerDiscoveryCenter 鍐呴儴妯″潡鍒掑垎涓庨€氫俊鍏崇郴

## 瀹瑰櫒鍥?

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'fontSize':'16px'}, 'flowchart': {'nodeSpacing': 50, 'rankSpacing': 80}, 'sequence': {'actorMargin': 50, 'messageMargin': 20}}}%%
graph TB
    subgraph "PDC 杩涚▼"
        subgraph "鎺ュ叆灞?Entry"
            HTTP[HTTP REST API<br/>/api/*]
            WS[WebSocket 鐩戞帶<br/>/ws]
            UDPT[UDP Tracker<br/>6880]
            TCPT[TCP Tracker<br/>6880]
            DHTP[DHT 鐖櫕绔彛<br/>6882]
        end

        subgraph "涓氬姟鏈嶅姟灞?Services"
            DISC[DiscoverService<br/>鍙戠幇璋冨害]
            CRAWL[CrawlerService<br/>DHT鐖櫕]
            TRACK[TrackerService<br/>Tracker绠＄悊]
            PROBE[ProbeService<br/>Peer鎺㈡祴]
            PEX[PexService<br/>PEX浜ゆ崲]
            NAT[NatService<br/>NAT绌块€廬
        end

        subgraph "鏅鸿兘灞?Intelligence"
            SCORE[ScoreMaintainer<br/>璇勫垎缁存姢]
            TIER[TierManager<br/>鍐风儹鍒嗗眰]
            SELECT[SelectSystem<br/>鑺傜偣閫夋嫨]
            HEALTH[HealthScorer<br/>鍋ュ悍搴
        end

        subgraph "鏁版嵁灞?Repositories"
            NR[NodeRepo<br/>DHT鑺傜偣]
            PR[PeerRepo<br/>BT Peer]
            TR[TrackerRepo<br/>Tracker]
            IR[InfohashRepo<br/>Infohash]
        end

        subgraph "瀛樺偍灞?Storage"
            MEM[鍐呭瓨缂撳瓨<br/>parking_lot::RwLock]
            DB[(SQLite WAL<br/>pdc.db)]
        end
    end

    %% 鎺ュ叆灞?鈫?涓氬姟灞?
    HTTP --> DISC & TRACK
    WS --> HEALTH
    UDPT --> TRACK
    TCPT --> TRACK
    DHTP --> CRAWL

    %% 涓氬姟灞?鈫?鏅鸿兘灞?
    DISC & CRAWL & TRACK & PROBE & PEX --> SELECT
    CRAWL & PROBE --> SCORE

    %% 鏅鸿兘灞?鈫?鏁版嵁灞?
    SCORE --> NR & PR & TR
    TIER --> NR & PR
    SELECT --> NR & PR
    HEALTH --> NR & PR & TR & IR

    %% 鏁版嵁灞?鈫?瀛樺偍灞?
    NR & PR & TR & IR --> MEM
    NR & PR & TR & IR --> DB
```

## 妯″潡鑱岃矗

### 鎺ュ叆灞?(Entry)

| 妯″潡 | 鑱岃矗 | 鍏抽敭鎺ュ彛 |
|---|---|---|
| HTTP REST API | 瀵瑰鎻愪緵 REST 鎺ュ彛 | /api/nodes, /api/peers, /api/trackers, /api/health |
| WebSocket 鐩戞帶 | 瀹炴椂鐘舵€佹帹閫?| /ws锛坰tatus, stats, health 浜嬩欢锛?|
| UDP Tracker | BEP 15 UDP Tracker 鍗忚 | announce, scrape |
| TCP Tracker | HTTP Tracker 鍗忚 | /announce, /scrape |
| DHT 鐖櫕绔彛 | DHT KRPC 鍗忚 | find_node, get_peers, announce_peer |

### 涓氬姟鏈嶅姟灞?(Services)

| 妯″潡 | 鑱岃矗 | 鍏抽敭鎿嶄綔 |
|---|---|---|
| DiscoverService | 缁熶竴鍙戠幇璋冨害锛屽崗璋冨悇鍙戠幇娓犻亾 | discover(infohash), get_peers(infohash) |
| CrawlerService | DHT 缃戠粶涓诲姩鐖 | crawl(), select_diverse_nodes(), chain_crawl() |
| TrackerService | Tracker 姹犵鐞嗕笌璇锋眰 | add_tracker(), announce(), scrape() |
| ProbeService | Peer 鍙揪鎬ф帰娴?| probe_peer(), update_probe_stats() |
| PexService | Peer Exchange 鍗忚 | pex_handshake(), exchange_peers() |
| NatService | NAT 绌块€忎笌绔彛鏄犲皠 | upnp_map(), nat_pmp_map() |

### 鏅鸿兘灞?(Intelligence)

| 妯″潡 | 鑱岃矗 | 鍏抽敭鎿嶄綔 |
|---|---|---|
| ScoreMaintainer | 缁熶竴璇勫垎缁存姢锛屽閲?鍏ㄩ噺鍙屾ā寮?| rescore_incremental(), rescore_all() |
| TierManager | 鍐风儹鍒嗗眰绠＄悊锛屾俯搴﹁縼绉?| check_and_migrate(), tier_stats() |
| SelectSystem | 缁熶竴鑺傜偣閫夋嫨 | select_diverse_nodes(), select_top_nodes() |
| HealthScorer | 绯荤粺鍋ュ悍搴﹁瘎浼?| calculate() 鈫?HealthReport |

### 鏁版嵁灞?(Repositories)

| Repo | 鑱岃矗 | 鏁版嵁瑙勬ā | 鎸佷箙鍖?|
|---|---|---|---|
| NodeRepo | DHT 鑺傜偣瀛樺偍涓庤瘎鍒?| 60,000+ | SQLite dht_nodes 琛?|
| PeerRepo | BT Peer 瀛樺偍锛堟寜 infohash 鍒嗙粍锛?| 800+ | SQLite peers 琛?|
| TrackerRepo | Tracker 姹犲瓨鍌ㄤ笌璇勫垎 | 79 | SQLite trackers 琛?|
| InfohashRepo | Infohash 娉ㄥ唽涓庡紩鐢ㄨ鏁?| 90+ | SQLite infohashes 琛?|

## 閫氫俊鍏崇郴

### 鍚屾璋冪敤锛堢洿鎺ュ嚱鏁拌皟鐢級

```
涓氬姟灞?鈫?鏅鸿兘灞?鈫?鏁版嵁灞?鈫?瀛樺偍灞?
```

鎵€鏈夊眰鍦ㄥ悓涓€杩涚▼鍐咃紝閫氳繃 trait 瀵硅薄鐩存帴璋冪敤锛屾棤缃戠粶寮€閿€銆?

### 寮傛浠诲姟锛坱okio::spawn锛?

| 浠诲姟 | 闂撮殧 | 璇存槑 |
|---|---|---|
| 璇勫垎澧為噺閲嶇畻 | 10s | ScoreMaintainer 閲嶇畻鑴忚妭鐐?|
| 璇勫垎鍏ㄩ噺閲嶇畻 | 300s | ScoreMaintainer 鍏滃簳鍏ㄩ噺閲嶇畻 |
| 鍐风儹鍒嗗眰妫€鏌?| 300s | TierManager 妫€鏌ユ俯搴﹁縼绉?|
| 鏁版嵁鎸佷箙鍖?| 300s | 鍚?Repo 鍏ㄩ噺淇濆瓨鍒?SQLite |
| peer_history flush | 30s | 鎵归噺鍐欏叆鍘嗗彶璁板綍 |
| WAL checkpoint | 600s | SQLite WAL 鍚堝苟鍒颁富搴?|
| 鍋ュ悍妫€鏌?| 300s | 绯荤粺鍋ュ悍搴﹁瘎浼颁笌缁熻 |

### 浜嬩欢椹卞姩

- DHT 鍝嶅簲鍒拌揪 鈫?瑙﹀彂鑺傜偣娣诲姞 + 鑴忔爣璁?+ 閾惧紡鐖
- Tracker 鍝嶅簲鍒拌揪 鈫?瑙﹀彂 Peer 娣诲姞 + 缁熻鏇存柊
- Probe 瀹屾垚 鈫?瑙﹀彂 Peer 鎺㈡祴缁熻鏇存柊 + 鑴忔爣璁?

## 鏁版嵁娴?

### DHT 鐖櫕鏁版嵁娴?

```
DHT鍝嶅簲 鈫?CrawlerService 鈫?NodeRepo.add_node()
                              鈫?NodeRepo.mark_dirty()
                              鈫?SelectSystem.select_diverse_nodes()
                              鈫?閾惧紡鐖
ScoreMaintainer(10s) 鈫?NodeRepo.dirty_nodes() 鈫?璁＄畻璇勫垎 鈫?update_scores_batch()
```

### Tracker 鍙戠幇鏁版嵁娴?

```
Tracker鍝嶅簲 鈫?TrackerService 鈫?PeerRepo.add_peers()
                                鈫?TrackerRepo.record_request()
                                鈫?InfohashRepo.register()
ScoreMaintainer(300s) 鈫?鍏ㄩ噺閲嶇畻 Tracker/Peer 璇勫垎
```

## 涓嬩竴姝?

- 闃呰 [03-intelligence.md](03-intelligence.md) 浜嗚В鏅鸿兘灞傝缁嗚璁?
- 闃呰 [04-data-model.md](04-data-model.md) 浜嗚В鏁版嵁妯″瀷
