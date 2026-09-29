# B5 索引清理前置证据收集（Index-Cleanup Evidence）

> 范围：只读仓库代码（`src/storage/db.rs`、`peer_repo.rs`、`node_repo.rs`、`tracker_repo.rs`、`infohash_repo.rs` 等），
> 未修改任何 `.rs/.yaml/.toml`，未运行 cargo build/test/clippy/fmt，未做任何 git 操作。
> 所有 `EXPLAIN QUERY PLAN` 在独立临时库 `D:\test\pdc\b5_evidence.db` 上跑出（Python sqlite3，SQLite 3.53.1），
> schema 逐字复刻自 `src/storage/db.rs` 376–473、542–549、656–664。
> 复刻脚本：`D:\test\pdc\b5_evidence.py`；临时库：`D:\test\pdc\b5_evidence.db`。

结论速览：

| 小节 | 对象 | 结论 |
|---|---|---|
| B5-1 | `idx_peers_infohash` / `idx_peers_archive_infohash` | **删** |
| B5-2 | `idx_peers_deleted` / `idx_trackers_deleted` / `idx_infohashes_deleted` / `idx_dht_nodes_deleted` | **需观察窗口** |
| B5-3 | `idx_peer_history_infohash` / `idx_peer_history_time` | **留** |

---

## B5-1 — `peers` / `peers_archive` 上含 `infohash` 的查询，与单列 infohash 索引的去留

### 证据结论：**删**（`idx_peers_infohash`、`idx_peers_archive_infohash` 可删）

两表主键均为 `PRIMARY KEY (infohash, ip, port)`（`db.rs:429`、`db.rs:444`），SQLite 自动生成
`sqlite_autoindex_peers_1` / `sqlite_autoindex_peers_archive_1`。该复合主键的**最左前缀就是 `infohash`**，
因此任何「只按 infohash 定位」的查询本就被主键自动索引覆盖，单列索引 `idx_peers_infohash`（`db.rs:431`）
与 `idx_peers_archive_infohash`（`db.rs:446`）是主键前缀的冗余副本。

### 全仓库 `FROM peers` / `FROM peers_archive` 且 WHERE 含 infohash 的查询清单

全 src 树 Grep `FROM\s+peers_archive|FROM\s+peers\b`，命中全部集中在 `src/storage/db.rs`
（`peer_repo.rs / node_repo.rs / tracker_repo.rs / infohash_repo.rs` 均不直接发 `FROM peers` SQL，统一走 `Storage` 方法）。
其中 WHERE 子句真正含 `infohash` 的只有两条路径：

1. **`db.rs:2629-2630`**（联邦反熵批查，PEER 分支）
   ```sql
   SELECT infohash, ip, port FROM peers
   WHERE deleted_at IS NULL AND (infohash, ip, port) IN ((?, ?, ?), ...)
   ```
   带**完整三列复合** `(infohash, ip, port)`，直接命中主键自动索引。
   - EXPLAIN 摘录（a1）：
     ```
     SEARCH peers USING INDEX sqlite_autoindex_peers_1 (infohash=? AND ip=?)
     ```

2. **`db.rs:2142-2158`**（`query_peer_rows_one_table`，PEER 联邦 bootstrap 清单/块读；
   主表调用点 `db.rs:2089-2096` 传 `tombstone_filter="deleted_at IS NULL"`，
   archive 调用点 `db.rs:2097-2104` 传 `"1=1"`）
   ```sql
   SELECT (lower(hex(infohash))||':'||ip||':'||port) k, infohash, ip, port
   FROM {peers|peers_archive}
   WHERE {deleted_at IS NULL | 1=1}
     AND (lower(hex(infohash))||':'||ip||':'||port) >= ?
     AND (lower(hex(infohash))||':'||ip||':'||port) < ?
   ORDER BY (lower(hex(infohash))||':'||ip||':'||port) ASC LIMIT ?
   ```
   `infohash` 出现在表达式键里，命中专门的表达式索引（`db.rs:660-663`），不碰单列 infohash 索引。
   - EXPLAIN 摘录（a2，peers 主表）：
     ```
     SEARCH peers USING INDEX idx_peers_key_expr (<expr>>? AND <expr><?)
     ```
   - EXPLAIN 摘录（a3，peers_archive）：
     ```
     SEARCH peers_archive USING INDEX idx_peers_archive_key_expr (<expr>>? AND <expr><?)
     ```

### 反证：单列 infohash 索引从不被选中

- EXPLAIN 摘录（a4，`SELECT ip, port FROM peers WHERE infohash = ?`）：
  ```
  SEARCH peers USING COVERING INDEX sqlite_autoindex_peers_1 (infohash=?)
  ```
- EXPLAIN 摘录（a5，peers_archive 同款）：
  ```
  SEARCH peers_archive USING COVERING INDEX sqlite_autoindex_peers_archive_1 (infohash=?)
  ```

两例均走**主键自动索引（覆盖）**，而非 `idx_peers_infohash` / `idx_peers_archive_infohash`。
代码库里也不存在任何 `WHERE peers.infohash = ?` 的独立点查（`db.rs:1821` 的 `WHERE infohash = ?`
属于 `infohashes` 表，不是 `peers`）。

> 附带观察（不在 B5-1 口径内，仅记录）：`db.rs:1789`
> `SELECT ... FROM peers WHERE ip = ?1 AND port = ?2 AND deleted_at IS NULL`
> **不含 infohash**，主键 `(infohash,ip,port)` 用不上、也没有 `(ip,port)` 索引，只能 SCAN。
> 这是另一类问题（缺 `(ip,port)` 反向索引），不属于本次 infohash 索引清理范围，留待后续评估。

---

## B5-2 — `deleted_at` 进 WHERE 的用法，与墓碑索引的去留

### 证据结论：**需观察窗口**（不要立即 `DROP INDEX idx_*_deleted`）

任务假设是「真正需要墓碑索引的查询是不是只有 `WHERE deleted_at IS NOT NULL`」。证据给出的答案：

- **唯一的 `deleted_at IS NOT NULL` 查询根本不需要墓碑索引**——它被主键锚定（见 b1）。
- **反而是 `deleted_at IS NULL` 的启动 COUNT 把墓碑索引当覆盖索引用**（见 b2）。
- 其余 `deleted_at IS NULL` 谓词都骑在更好的访问路径上（主键 / 表达式索引 / rowid 探针 / 反正要全表扫）。

因此不能一刀切删：墓碑索引的唯一真实消费者是一次性的启动校准 COUNT，写放大发生在热路径（每次 peer/tracker/node 写入都维护它），
属于「读省一次、写付全程」的取舍，建议在观察窗口里实测启动校准耗时 vs. 写放大后再定。

### 全仓库 `deleted_at` 进 WHERE 的用法分类（`src/storage/db.rs`）

**(i) `deleted_at IS NOT NULL` —— 全仓库仅 1 处：**

- `db.rs:979`（`is_tracker_tombstoned`，联邦入站 upsert 仲裁）：
  ```sql
  SELECT 1 FROM trackers WHERE url = ?1 AND deleted_at IS NOT NULL LIMIT 1
  ```
  - EXPLAIN 摘录（b1）：
    ```
    SEARCH trackers USING INDEX sqlite_autoindex_trackers_1 (url=?)
    ```
    谓词 `url = ?1` 命中 `trackers.url TEXT PRIMARY KEY`（`db.rs:395`），只取 1 行后再过滤
    `deleted_at IS NOT NULL`；**完全没碰 `idx_trackers_deleted`**。

**(ii) `deleted_at IS NULL` 作为唯一/主过滤条件（计数或全量装载）：**

- `db.rs:161` `SELECT COUNT(*) FROM peers WHERE deleted_at IS NULL`（F9 启动校准）
- `db.rs:160/163/164` 对 dht_nodes / infohashes / trackers 的同款 COUNT
- `db.rs:1318` `SELECT ... FROM peers WHERE deleted_at IS NULL`（全量装载）
- `db.rs:744 / 1078 / 1196` dht_nodes / trackers / infohashes 全量装载
- `db.rs:1699` `load_limited_peers`：`... WHERE deleted_at IS NULL ORDER BY last_active DESC LIMIT ?1`
- `db.rs:1666` dht_nodes 温加载：`WHERE deleted_at IS NULL AND (last_active > ?1 OR score >= ?2) ORDER BY score DESC`

EXPLAIN 摘录：
- b2（`db.rs:161` 校准 COUNT）：
  ```
  SEARCH peers USING COVERING INDEX idx_peers_deleted (deleted_at=?)
  ```
  → 墓碑索引**确实被选中**，作为覆盖索引避免回表。这是它唯一的真实 read 用途。
- b3（`db.rs:1699` load_limited_peers）：
  ```
  SCAN peers
  USE TEMP B-TREE FOR ORDER BY
  ```
  → 因为 `ORDER BY last_active DESC` 必须排序，优化器直接 SCAN 主表再排序，**没用** `idx_peers_deleted`。
- b4（`db.rs:1432` 归档计数 `COUNT(*) FROM peers WHERE last_active < ?1`，无墓碑谓词）：
  ```
  SCAN peers
  ```

**(iii) `deleted_at IS NULL` 作为附加过滤，骑在更优索引上：**

- `db.rs:1754 / 1789 / 1821 / 1850`：`WHERE (ip,port)=? / infohash=? / url=? AND deleted_at IS NULL`
  → 各自主键（dht_nodes PK(ip,port)、peers PK(infohash,ip,port)、infohashes PK(infohash)、trackers PK(url)）先定位，墓碑列只是残留过滤。
- `db.rs:2092 / 2144`：PEER 联邦区间扫描 `WHERE deleted_at IS NULL AND <expr> >= ? AND <expr> < ?`
  → 走 `idx_peers_key_expr`（见 B5-1 a2），`deleted_at IS NULL` 是索引外残留过滤。
- `db.rs:2198 / 2349`：rowid 探针 `WHERE deleted_at IS NULL AND rowid >= ?1`。
- `db.rs:2560 / 2630 / 2676 / 2713`：批量 `(ip,port) IN / (infohash,ip,port) IN / infohash IN / url IN`
  → 全部命中各自主键。
- `db.rs:2273 / 2293 / 2455 / 2485`：infohashes / trackers 区间扫描，走主键。

### 现有墓碑索引清单（`db.rs:546-549`）

```sql
CREATE INDEX IF NOT EXISTS idx_dht_nodes_deleted  ON dht_nodes(deleted_at);
CREATE INDEX IF NOT EXISTS idx_trackers_deleted   ON trackers(deleted_at);
CREATE INDEX IF NOT EXISTS idx_infohashes_deleted ON infohashes(deleted_at);
CREATE INDEX IF NOT EXISTS idx_peers_deleted      ON peers(deleted_at);
```

### 去留理由

- `IS NOT NULL` 探针（b1）不依赖墓碑索引 → 任务原假设「只有 IS NOT NULL 需要」不成立。
- 墓碑列基数极低（线上 ~99.9% 行 `deleted_at IS NULL`），唯一被选中的场景是 b2 的**启动一次性覆盖 COUNT**；
  稳态下有效行数由 `table_counts` 触发器维护（`db.rs:570-621`），不再跑这条 COUNT。
- 但 b2 证明删掉后启动校准会从「窄覆盖索引扫」退化为「主表 SCAN 计数」，对百万行级 peers 不是零成本。
- **建议**：保留观察窗口——在预发环境分别测 (1) 启动 `calibrate_table_counts` 耗时、(2) peers/tracker 写入 TPS，
  对比「有/无 `idx_*_deleted`」两组；若启动增量 < 50ms 且写路径有可测收益，再删。
  其中 `idx_trackers_deleted` 最可疑（trackers 行数远小于 peers，b1 又不用它），可优先观察。

---

## B5-3 — `peer_history` 表查询与索引

### 证据结论：**留**（`idx_peer_history_infohash` 与 `idx_peer_history_time` 都在用，不可删）

`peer_history` 表 DDL（`db.rs:449-457`）：
```sql
CREATE TABLE peer_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    infohash BLOB NOT NULL,
    ip TEXT NOT NULL,
    port INTEGER NOT NULL,
    source TEXT,
    score REAL DEFAULT 0,
    discovered_at INTEGER NOT NULL
);
CREATE INDEX idx_peer_history_infohash ON peer_history(infohash);   -- db.rs:458
CREATE INDEX idx_peer_history_time    ON peer_history(discovered_at); -- db.rs:459
```

### 全仓库 `peer_history` 查询清单

| 文件:行号 | 语句类型 | SQL 要点 |
|---|---|---|
| `db.rs:1493` | INSERT | `INSERT INTO peer_history(...) VALUES(...)` |
| `db.rs:1524` | INSERT 批量 | `INSERT INTO peer_history(...) VALUES(...)`（WriteQueue/IOScheduler 闭包） |
| `db.rs:1546` | SELECT 点查 | `SELECT ip,port,source,score,discovered_at FROM peer_history WHERE infohash = ?1 ORDER BY discovered_at DESC LIMIT ?2` |
| `db.rs:1565` | DELETE 清理 | `DELETE FROM peer_history WHERE discovered_at < ?1` |

调用面：`db.rs:1546` 被 `peer_repo.rs:422 / 764`（冷数据补充）与 `rest_api.rs:722`（`query_peer_history` HTTP handler）调用；
`db.rs:1565` 由 `score_maintainer.rs:383` 周期清理触发。

**确认存在 `ORDER BY discovered_at DESC`**：在 `db.rs:1546`，逐字如下
```sql
SELECT ip, port, source, score, discovered_at FROM peer_history
WHERE infohash = ?1 ORDER BY discovered_at DESC LIMIT ?2
```

### EXPLAIN QUERY PLAN 摘录

- c1（`db.rs:1546` 点查 + 排序）：
  ```
  SEARCH peer_history USING INDEX idx_peer_history_infohash (infohash=?)
  USE TEMP B-TREE FOR ORDER BY
  ```
  → `idx_peer_history_infohash` 被选中做等值定位；排序走临时 B-Tree。`idx_peer_history_time` 未被本查询选用。

- c2（`db.rs:1565` 过期清理，等价 SELECT rowid 预览）：
  ```
  SEARCH peer_history USING COVERING INDEX idx_peer_history_time (discovered_at<?)
  ```
  → `idx_peer_history_time` 被选中做范围删除。

### 去留理由

- 两个单列索引各自由一条不同的生产查询独占使用（c1 用 infohash、c2 用 time），互不冗余，**均不可删**。
- 附注（非清理项，属未来优化）：c1 的 `USE TEMP B-TREE FOR ORDER BY` 说明
  `(infohash, discovered_at DESC)` 复合索引可消掉这次排序，但这是**新增**索引的优化方向，
  与本次「索引清理」目标相反，不在 B5 结论内。

---

## 附：临时复刻脚本与库路径

- 复刻脚本：`D:\test\pdc\b5_evidence.py`（含建表、建索引、示例数据、ANALYZE、全部 EXPLAIN QUERY PLAN）
- 临时库：`D:\test\pdc\b5_evidence.db`（脚本每次运行先 `os.remove` 重建，可安全重复执行）
- 上述全部 `SEARCH/SCAN/USE TEMP B-TREE` 输出均来自该脚本一次干净运行（SQLite 3.53.1）。
