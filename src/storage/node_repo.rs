//! NodeRepository 瀹炵幇
//!
//! 鐙珛鐨?DHT 鑺傜偣瀛樺偍锛堟棤瀹归噺闄愬埗锛夛紝浣滀负鐖櫕鍊欓€夋睜鐨勫敮涓€褰掑彛銆?
//! 璺敱琛ㄥ彧璐熻矗 DHT 璺敱鍝嶅簲锛孨odeRepo 璐熻矗鐖櫕鍊欓€夎妭鐐圭殑瀛樺偍鍜岃瘎鍒嗐€?
//! 鍐呭瓨 FxHashMap + SQLite 澧為噺鎸佷箙鍖栥€?
//! 鍗冧竾绾ф€ц兘浼樺寲锛欶xHashMap 鏇夸唬 std::HashMap锛屽閲忔寔涔呭寲鍙繚瀛?dirty 鑺傜偣銆?

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::federation::gossip::GossipEngine;
use crate::federation::protocol::{operation, repo_type, SyncEntry};

use crate::dht::kbucket::{KBucketEntry, NodeState};
use crate::storage::db::{DhtNodeRow, Storage};
use crate::storage::repo_traits::{NodeId, NodeRepository};
use crate::storage::sharded_map::ShardedHashMap;
use crate::storage::tiered_cache::TieredCacheConfig;
use crate::storage::write_queue::WriteQueue;

/// 鑺傜偣缁熻淇℃伅锛堥伩鍏嶅叏閲忓厠闅嗭級
#[derive(Debug, Clone)]
pub struct NodeStats {
    pub total: usize,
    pub good: usize,
    pub questionable: usize,
    pub bad: usize,
    pub active: usize,
    pub avg_score: f64,
}

/// 原子计数器与全量实测扫描的差值（= 计数器 − 实测扫描值）。
///
/// 供 stats_snapshot 的 60s 后台任务调用 [`NodeRepoImpl::verify_stats_consistency`]
/// 并据此打 WARN（|差值|>5 或 |avg_score_diff|>0.01）。全 0 表示一致。
/// 字段全部 pub，调用方不做任何额外计算。
#[derive(Debug, Clone, Copy, Default)]
pub struct NodeStatsDrift {
    pub total: i64,
    pub good: i64,
    pub questionable: i64,
    pub bad: i64,
    pub active: i64,
    pub avg_score_diff: f64,
}

pub struct NodeRepoImpl {
    /// 鐙珛鑺傜偣瀛樺偍锛堟棤瀹归噺闄愬埗锛屾寜 addr 鍘婚噸锛夆€?FxHashMap 楂樻€ц兘
    nodes: ShardedHashMap<SocketAddr, KBucketEntry>,
    /// 鑴忚妭鐐归泦鍚堬紙缁熻鏁版嵁宸插彉鍖栵紝闇€瑕侀噸绠楄瘎鍒?+ 澧為噺鎸佷箙鍖栵級
    dirty: Arc<RwLock<FxHashSet<SocketAddr>>>,
    /// /24 缃戞绱㈠紩锛圛Pv4 鍓?3 瀛楄妭 -> 璇ョ綉娈靛唴鑺傜偣 ID 鍒楄〃锛夛紝鐢ㄤ簬 O(1) 鍙栫綉娈?
    /// 浠?IPv4 鑺傜偣鍏ョ储寮曪紱IPv6 鑺傜偣蹇界暐銆傚鍒犺妭鐐规椂鍚屾缁存姢銆?
    subnet_index: RwLock<FxHashMap<[u8; 3], Vec<NodeId>>>,
    /// 鐑妭鐐瑰湴鍧€闆嗗悎锛堟渶杩?hot_threshold_secs 鍐呰璁块棶鐨勮妭鐐癸級
    hot_addrs: RwLock<FxHashSet<SocketAddr>>,
    /// 鍐疯妭鐐瑰湴鍧€闆嗗悎锛堣秴杩?hot_threshold_secs 鏈闂紝鐢卞閮ㄥ畾鏃朵换鍔¤縼绉伙級
    cold_addrs: RwLock<FxHashSet<SocketAddr>>,
    storage: Arc<Storage>,
    /// 鑱旈偊寮曠敤锛圤nceLock 娉ㄥ叆锛涙湭璁剧疆鏃舵湰鍦板啓鍏ヤ笉瑙﹀彂 Merkle/Gossip锛宺epo 姝ｅ父宸ヤ綔锛?
    gossip: OnceLock<Arc<GossipEngine>>,
    /// 鍐欏叆闃熷垪锛堝彲閫夛紝None 鏃堕€€鍖栦负鍚屾鍐欏叆锛?
    write_queue: Option<Arc<WriteQueue>>,
    /// 增量统计原子计数器（O(1) stats_sync，避免全量遍历 20 万节点）。
    /// 所有变更点在 nodes 写锁/分片锁内维护；score_sum 存 f64 bit（CAS 循环更新）；Relaxed 即可。
    total: AtomicU64,
    good: AtomicU64,
    questionable: AtomicU64,
    bad: AtomicU64,
    /// query_count>0 的节点数（只增不减，删除时扣减）
    active: AtomicU64,
    /// 所有节点 score 之和（f64 bit 存进 AtomicU64）
    score_sum: AtomicU64,
    /// 近期窗口衰减系数（每小时）：与 NodeScoreConfig.recent_decay_alpha 同源，默认 0.3
    recent_decay_alpha: f64,
    /// 判 Bad 的连续失败阈值（默认 3；可配置加快死节点淘汰，2026-10 D4）
    bad_after_failures: AtomicU64,
}

impl NodeRepoImpl {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self::with_tier_config(storage, Default::default(), true)
    }

    pub fn with_tier_config(
        storage: Arc<Storage>,
        _cache_config: TieredCacheConfig,
        _tier_enabled: bool,
    ) -> Self {
        Self {
            nodes: ShardedHashMap::new(16),
            dirty: Arc::new(RwLock::new(FxHashSet::default())),
            subnet_index: RwLock::new(FxHashMap::default()),
            hot_addrs: RwLock::new(FxHashSet::default()),
            cold_addrs: RwLock::new(FxHashSet::default()),
            storage,
            gossip: OnceLock::new(),
            write_queue: None,
            total: AtomicU64::new(0),
            good: AtomicU64::new(0),
            questionable: AtomicU64::new(0),
            bad: AtomicU64::new(0),
            recent_decay_alpha: crate::intelligence::scorer_config::NodeScoreConfig::default()
                .recent_decay_alpha,
            bad_after_failures: AtomicU64::new(3),
            active: AtomicU64::new(0),
            score_sum: AtomicU64::new(0),
        }
    }

    /// 鍏煎鏃ф帴鍙ｏ細浠?crawler 璺敱琛ㄥ垱寤猴紙鐜板湪蹇界暐璺敱琛紝鐙珛瀛樺偍锛?
    pub fn from_crawler(
        _routing_table: Arc<parking_lot::RwLock<crate::dht::routing_table::RoutingTable>>,
        storage: Arc<Storage>,
    ) -> Self {
        Self::new(storage)
    }

    /// 娉ㄥ叆鍐欏叆闃熷垪锛坆uilder 妯″紡锛?
    pub fn with_write_queue(mut self, wq: Arc<WriteQueue>) -> Self {
        self.write_queue = Some(wq);
        self
    }

    /// 设置近期窗口衰减系数（builder 模式；与评分器配置同源时二者一致）
    pub fn with_recent_decay_alpha(mut self, alpha: f64) -> Self {
        self.recent_decay_alpha = alpha;
        self
    }

    /// 设置判 Bad 的连续失败阈值（builder 模式，2026-10 D4）
    pub fn with_bad_after_failures(self, n: u32) -> Self {
        self.bad_after_failures.store(n as u64, Ordering::Relaxed);
        self
    }

    /// 近期窗口统计更新（指数衰减近似 EMA）：先衰减存量再累加本次。
    /// 终身累计 success/query 会被历史稀释，近期窗口让连续失败立即反映到评分（2026-10 R4）。
    fn record_recent(&self, entry: &mut KBucketEntry, success: bool) {
        let now = Instant::now();
        if let Some(last) = entry.recent_updated {
            let d = KBucketEntry::decay_factor(self.recent_decay_alpha, last.elapsed());
            entry.recent_query *= d;
            entry.recent_success *= d;
        }
        entry.recent_query += 1.0;
        if success {
            entry.recent_success += 1.0;
        }
        entry.recent_updated = Some(now);
    }

    /// 获取底层 Storage 引用（用于联邦同步按分片加载数据）。
    pub fn storage(&self) -> Arc<Storage> {
        self.storage.clone()
    }

    /// 预加载热窗口：加载时仅将最近活跃（此窗口内）的节点标为热。
    /// 与 tier.hot_threshold_secs 默认值一致；陈旧节点不再整体进场热池（2026-10 R2）。
    const PRELOAD_HOT_WITHIN_SECS: u64 = 1800;

    /// Bad 复活冷却期（秒）：判死后需间隔此时长才可因被再次提及而复活（2026-10 D4）
    const BAD_REVIVE_COOLDOWN_SECS: u64 = 600;

    /// 分层缓存统计（与 TrackerRepo 接口一致：(hot, warm, cold_loaded)）。
    /// NodeRepo 全量驻内存，全部计入 hot。
    pub async fn load_initial(&self, limit: usize) -> anyhow::Result<usize> {
        // 按最近活跃降序预加载（新近度优先）；DB 中的 last_active 为节点真实活跃时间，
        // 不再在加载时被复位为进程启动时刻（2026-10 R0/R1）
        let rows = self.storage.load_recent_nodes(limit)?;
        let mut nodes = self.nodes.write_all();
        let mut subnet_index = self.subnet_index.write();
        let mut count = 0;
        for row in rows {
            let addr = SocketAddr::new(row.ip.parse().unwrap_or([127, 0, 0, 1].into()), row.port);
            let mut entry = KBucketEntry::new(row.id, addr);
            entry.score = row.score;
            entry.query_count = row.query_count;
            entry.success_count = row.success_count;
            entry.total_latency_ms = row.total_latency_ms;
            entry.consecutive_failures = row.consecutive_failures;
            entry.nodes_returned = row.nodes_returned;
            entry.last_query_time = row
                .last_query_time
                .map(|secs| crate::utils::cutoff_before(Duration::from_secs(secs.max(0) as u64)));
            entry.last_active = row
                .last_active
                .map(crate::utils::unix_secs_to_instant)
                .unwrap_or_else(Instant::now);
            entry.state = match row.state.as_str() {
                "Good" => NodeState::Good,
                "Questionable" => NodeState::Questionable,
                _ => NodeState::Bad,
            };
            // 用真实 last_active 重算 Good/Questionable（refresh_state 对 Bad 冻结不变）
            entry.refresh_state();
            nodes.insert(addr, entry);
            if let Some(subnet) = Self::subnet_key(addr) {
                Self::index_subnet(&mut subnet_index, subnet, row.id);
            }
            count += 1;
        }
        // 仅将最近活跃（hot 窗口内）的加载节点标为热；陈旧节点不进热池，
        // 热池语义收归「已验证响应/新发现」（2026-10 R2）
        let mut hot_addrs = self.hot_addrs.write();
        for (_addr, entry) in nodes.iter() {
            if entry.last_active.elapsed() < Duration::from_secs(Self::PRELOAD_HOT_WITHIN_SECS) {
                hot_addrs.insert(entry.addr);
            }
        }
        drop(hot_addrs);
        // 校准回填：全量扫描把 6 个计数器直接置为扫描结果（置为而非累加，兼容重复加载）。
        // 启动阶段无并发，安全；此处仍持有 nodes 写锁，复用守卫避免重复加锁死锁。
        self.calibrate_stats(nodes.values(), nodes.len());
        Ok(count)
    }

    pub fn cache_stats(&self) -> (usize, usize, u64) {
        (self.len_sync(), 0, 0)
    }

    /// 数据库中 dht_nodes 表的总行数（同步，用于监控面板）
    pub fn total_count_sync(&self) -> u64 {
        self.storage.count_table("dht_nodes").unwrap_or(0)
    }

    /// 常规分层检查（由 tier_evict 任务每 60 秒调用）。
    ///
    /// 把超过 hot_threshold 未访问的节点从 hot 集合标记到 cold 集合（**不卸载内存**）。
    /// 真正的内存卸载由 [`emergency_evict`](Self::emergency_evict) 在超限时执行。
    pub fn tier_evict(&self) {
        let moved = self.migrate_hot_to_cold_sync(1800);
        if moved > 0 {
            tracing::debug!(
                "[node_repo] 常规分层：{} 个节点 hot → cold（仍驻内存）",
                moved
            );
        }
    }

    /// 紧急驱逐（内存超限时由 memory_monitor 调用）。
    ///
    /// 把 count 个**最久未访问**的节点从内存表真正卸载以释放内存。
    /// 与联邦删除不同：**不写 DB 删除墓碑**（数据行保留在 SQLite），
    /// 节点可通过 load_initial / 定期刷新重新加载，也不会向对端同步删除。
    pub fn emergency_evict(&self, count: usize) {
        // 已在 dirty（未刷盘）集合的节点不卸载，避免丢失尚未持久化的更改
        let dirty_snapshot: FxHashSet<SocketAddr> = self.dirty.read().clone();

        let nodes = self.nodes.read_all();
        let mut ranked: Vec<(SocketAddr, Option<Instant>)> = nodes
            .iter()
            .filter(|(addr, _)| !dirty_snapshot.contains(addr))
            .map(|(addr, e)| (*addr, e.last_accessed))
            .collect();
        // Option 的 Ord：None < Some(Instant)，None 视为最老，正好满足"最久未访问在前"
        ranked.sort_by_key(|a| a.1);
        let targets: Vec<SocketAddr> = ranked.into_iter().take(count).map(|(a, _)| a).collect();
        drop(nodes);

        if targets.is_empty() {
            tracing::warn!("[node_repo] 紧急驱逐：无可卸载节点（候选均在 dirty 或内存为空）");
            return;
        }
        let n = self.evict_from_memory(&targets);
        tracing::warn!(
            "[node_repo] 紧急驱逐：从内存卸载 {} / {} 个节点（DB 保留，可重载）",
            n,
            targets.len()
        );
    }

    /// 从内存表卸载指定节点（不写 DB 墓碑），返回实际卸载条目数。
    fn evict_from_memory(&self, addrs: &[SocketAddr]) -> usize {
        let mut nodes = self.nodes.write_all();
        let mut subnet_index = self.subnet_index.write();
        let mut removed = 0usize;
        for addr in addrs {
            if let Some(entry) = nodes.remove(addr) {
                self.acc_remove_entry(&entry);
                if let Some(subnet) = Self::subnet_key(*addr) {
                    if let Some(bucket) = subnet_index.get_mut(&subnet) {
                        bucket.retain(|x| *x != entry.id);
                        if bucket.is_empty() {
                            subnet_index.remove(&subnet);
                        }
                    }
                }
                removed += 1;
            }
        }
        drop(nodes);
        drop(subnet_index);

        if removed > 0 {
            let mut hot = self.hot_addrs.write();
            let mut cold = self.cold_addrs.write();
            let mut dirty = self.dirty.write();
            for addr in addrs {
                hot.remove(addr);
                cold.remove(addr);
                dirty.remove(addr);
            }
        }
        removed
    }

    /// 娉ㄥ叆鑱旈偊 Merkle 鏍戜笌 Gossip 寮曟搸寮曠敤锛坢ain.rs 鍦?FederationService 鍒涘缓鍚庤皟鐢級銆?
    /// 鏈皟鐢ㄦ椂锛堝鍗曞厓娴嬭瘯锛夛紝鏈湴鍐欏叆涓嶈Е鍙戜紶鎾紝repo 琛屼负瀹屽叏涓嶅彉銆?
    pub fn set_federation_refs(&self, gossip: Arc<GossipEngine>) {
        let _ = self.gossip.set(gossip);
    }

    /// 灏嗘湰鍦版柊鍐欏叆鐨勬潯鐩壒閲忔洿鏂?Merkle 骞舵彁浜?Gossip锛堝啓閿佸鎵ц锛岀函鍐呭瓨鎿嶄綔锛夈€?
    /// merkle/gossip 鏈敞鍏ユ椂鐩存帴璺宠繃锛屼笉 panic銆?
    #[inline]
    fn propagate(&self, rt: u8, built: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>) {
        if built.is_empty() {
            return;
        }
        let Some(gossip) = self.gossip.get() else {
            return;
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entries: Vec<SyncEntry> = built
            .into_iter()
            .map(|(key, payload, _)| SyncEntry {
                key,
                operation: operation::UPSERT,
                version: now,
                payload,
            })
            .collect();
        // P1-2：本地新增/更新记入 oplog（delta 同步来源）；失败只告警。
        crate::storage::oplog::record_local_ops(&self.storage, rt, &entries);
        // v9：改走攒批提交 —— 被动收集路径每次常常只有 1~8 条，逐条提交会让出口限流
        // （按帧计）把联邦出口钉死在 gossip_max_messages_per_second 条 entry/s。
        gossip.submit_gossip_coalesced(rt, entries);
    }

    // 鈹€鈹€ 鍚屾渚挎嵎鏂规硶锛堢埇铏珮棰戣皟鐢紝閬垮厤 async 寮€閿€锛夆攢鈹€

    /// 鎻愬彇 SocketAddr 鐨?IPv4 鍓?3 瀛楄妭浣滀负 /24 缃戞 key銆?
    /// 浠?IPv4 杩斿洖 Some锛汭Pv6 杩斿洖 None锛堜笉鍏ョ綉娈电储寮曪級銆?
    #[inline]
    fn subnet_key(addr: SocketAddr) -> Option<[u8; 3]> {
        match addr.ip() {
            std::net::IpAddr::V4(v4) => {
                let o = v4.octets();
                Some([o[0], o[1], o[2]])
            }
            std::net::IpAddr::V6(_) => None,
        }
    }

    /// 鎶?(node_id, subnet) 鍔犲叆 /24 缃戞绱㈠紩銆?
    /// 璋冪敤鏂规寔鏈?self.subnet_index 鍐欓攣锛堝湪鎵归噺鎿嶄綔鍐呰仈瀹屾垚锛岄伩鍏嶉澶栭攣绔炰簤锛夈€?
    #[inline]
    fn index_subnet(
        subnet_index: &mut FxHashMap<[u8; 3], Vec<NodeId>>,
        subnet: [u8; 3],
        id: NodeId,
    ) {
        subnet_index.entry(subnet).or_default().push(id);
    }

    /// 浠?/24 缃戞绱㈠紩涓Щ闄ゆ寚瀹氳妭鐐?id锛堟寜鍦板潃瀹氫綅缃戞锛夈€?
    #[inline]
    fn unindex_subnet(&self, addr: &SocketAddr, id: &NodeId) {
        let Some(subnet) = Self::subnet_key(*addr) else {
            return;
        };
        let mut idx = self.subnet_index.write();
        if let Some(bucket) = idx.get_mut(&subnet) {
            bucket.retain(|x| x != id);
            if bucket.is_empty() {
                idx.remove(&subnet);
            }
        }
    }

    /// 鍐呴儴鍐欏叆锛氭壒閲忔柊澧炶妭鐐?+ 鏍囪 dirty锛屼笉瑙﹀彂 Merkle/Gossip銆?
    /// 杩斿洖鐪熸鏂板鐨?(node_id, addr) 瀵广€?
    /// 鑱旈偊鍚屾鍏ョ珯锛坅pply_node_sync锛夎皟鐢ㄦ湰鏂规硶锛岄伩鍏?Merkle 閲嶅鏇存柊涓?Gossip 鍥炵幆銆?
    pub(crate) fn add_nodes_batch_internal(
        &self,
        items: &[(NodeId, SocketAddr)],
    ) -> Vec<(NodeId, SocketAddr)> {
        if items.is_empty() {
            return Vec::new();
        }
        let mut nodes = self.nodes.write_all();
        let mut subnet_index = self.subnet_index.write();
        let mut new_pairs: Vec<(NodeId, SocketAddr)> = Vec::new();
        let mut revived: Vec<SocketAddr> = Vec::new();
        for (id, addr) in items {
            if let Some(existing) = nodes.get_mut(addr) {
                // 鍚屽湴鍧€鑺傜偣 ID 鏇存柊锛氳嫢 ID 鍙樺寲锛屽悓姝ユ洿鏂扮綉娈电储寮曚腑鐨勬棫 ID
                if existing.id != *id {
                    if let Some(subnet) = Self::subnet_key(*addr) {
                        if let Some(bucket) = subnet_index.get_mut(&subnet) {
                            bucket.retain(|x| x != &existing.id);
                        }
                        Self::index_subnet(&mut subnet_index, subnet, *id);
                    }
                }
                existing.id = *id;
                // Bad 冷却复活（2026-10 D4/R7）：Bad 被 refresh_state 冻结且此前无复活通道，
                // 误判（暂时丢包/换线）成为永久损失。被再次提及且「最近一次提及或失败」
                // 距今超过冷却期 → 降回 Questionable、失败数减半，评分恢复交给统一重算
                if existing.state == NodeState::Bad {
                    let last_anchor = match (existing.last_mentioned, existing.last_query_time) {
                        (Some(m), Some(q)) => Some(m.max(q)),
                        (Some(m), None) => Some(m),
                        (None, Some(q)) => Some(q),
                        (None, None) => None,
                    };
                    let cooled = last_anchor
                        .map(|t| t.elapsed() >= Duration::from_secs(Self::BAD_REVIVE_COOLDOWN_SECS))
                        .unwrap_or(true);
                    if cooled {
                        existing.state = NodeState::Questionable;
                        existing.consecutive_failures /= 2;
                        self.acc_transition_state(NodeState::Bad, NodeState::Questionable);
                        revived.push(*addr);
                    }
                }
                // mention（在别人响应里被提及，未经验证）只记 last_mentioned，
                // 不再刷新 last_active/last_verified——否则死节点会被邻居高频提及
                // 而永葆「新鲜」（2026-10 R5）；弱新鲜先验由评分器读 last_mentioned 给出
                existing.last_mentioned = Some(Instant::now());
            } else {
                let mut entry = KBucketEntry::new(*id, *addr);
                // 鏂拌妭鐐瑰垵濮嬭瘎鍒?45.0锛堜腑鎬у垎锛夛紝鍚庣画鐢?ScoreMaintainer 缁熶竴鏇存柊
                entry.score = 45.0;
                // 新节点：state=Good（默认）、query_count=0、score=45.0，按其字段累加计数器。
                // 本分支已确认 addr 不存在（get_mut 为 None），纯插入，无需先扣旧值。
                self.acc_add_entry(&entry);
                // 新发现即最强活性先验（刚在别人的响应里出现）→ 进热池；
                // last_accessed 必须同步设置，否则 tier_evict 的迁移会因 None 立即迁冷（2026-10 D2）
                entry.last_accessed = Some(Instant::now());
                nodes.insert(*addr, entry);
                self.hot_addrs.write().insert(*addr);
                new_pairs.push((*id, *addr));
                // IPv4 鑺傜偣鍏?/24 绱㈠紩
                if let Some(subnet) = Self::subnet_key(*addr) {
                    Self::index_subnet(&mut subnet_index, subnet, *id);
                }
            }
        }
        drop(nodes);
        drop(subnet_index);

        // 鏂拌妭鐐圭粺涓€鏍囪 dirty锛堥渶瑕佸閲忔寔涔呭寲锛夛紝涓€娆″啓閿?
        if !new_pairs.is_empty() || !revived.is_empty() {
            let mut dirty = self.dirty.write();
            for (_, addr) in &new_pairs {
                dirty.insert(*addr);
            }
            // 复活的节点也标 dirty（state/failures 变更需要持久化与重算）
            for addr in &revived {
                dirty.insert(*addr);
            }
        }
        new_pairs
    }

    /// 鎶婃柊鑺傜偣鍒楄〃鏋勫缓鎴?merkle/gossip 鏉＄洰骞朵紶鎾€?
    /// 联邦同步入站（apply_node_sync）专用：批量删除节点，**不触发 Merkle/Gossip**，
    /// 避免入站删除又被标 dirty 推回形成回环。返回实际删除的条目数。
    pub(crate) fn remove_nodes_batch_internal(&self, addrs: &[SocketAddr]) -> usize {
        if addrs.is_empty() {
            return 0;
        }
        let mut nodes = self.nodes.write_all();
        let mut subnet_index = self.subnet_index.write();
        let mut removed = 0usize;
        for addr in addrs {
            if let Some(entry) = nodes.remove(addr) {
                self.acc_remove_entry(&entry);
                if let Some(subnet) = Self::subnet_key(*addr) {
                    if let Some(bucket) = subnet_index.get_mut(&subnet) {
                        bucket.retain(|x| x != &entry.id);
                        if bucket.is_empty() {
                            subnet_index.remove(&subnet);
                        }
                    }
                }
                removed += 1;
            }
        }
        drop(nodes);
        drop(subnet_index);

        // 与 remove_node 保持一致的加锁顺序：hot -> cold -> dirty
        if removed > 0 {
            {
                let mut hot = self.hot_addrs.write();
                for addr in addrs {
                    hot.remove(addr);
                }
            }
            {
                let mut cold = self.cold_addrs.write();
                for addr in addrs {
                    cold.remove(addr);
                }
            }
            {
                let mut dirty = self.dirty.write();
                for addr in addrs {
                    dirty.remove(addr);
                }
            }
        }

        // P1-6: 落库软删墓碑（DB deleted_at）。对入站 DELETE 墓碑必须落库，否则重启后旧行会被
        // 重新加载（"删了又活"）。这里对所有 addrs 都写墓碑，而非仅内存命中的那些：本地可能因
        // 冷驱逐先把该节点移出内存，但 DB 行仍在，只有落墓碑才能让两端 Merkle 收敛到 0。
        let pairs: Vec<(String, u16)> = addrs
            .iter()
            .map(|a| (a.ip().to_string(), a.port()))
            .collect();
        if let Err(e) = self.storage.soft_delete_nodes_batch(&pairs) {
            tracing::warn!("[node_repo] 入站删除落库墓碑失败: {}", e);
        }
        removed
    }

    /// 把新节点列表构建成 merkle/gossip 条目并传播。
    fn propagate_nodes(&self, new_pairs: Vec<(NodeId, SocketAddr)>) {
        if new_pairs.is_empty() {
            return;
        }
        let mut built: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::with_capacity(new_pairs.len());
        for (id, addr) in &new_pairs {
            if let Some((k, p, h)) = crate::federation::sync::build_node_sync_entry(*id, *addr) {
                built.push((k, p, h));
            }
        }
        self.propagate(repo_type::NODE, built);
    }

    /// 鍚屾鍔犲叆鍗曚釜鑺傜偣锛堟湰鍦扮埇铏矾寰勶級锛氬啓鍏ュ悗鏇存柊 Merkle + 鎻愪氦 Gossip銆?
    /// 杩斿洖 true 琛ㄧず鏄柊鑺傜偣銆?
    pub fn add_node_sync(&self, id: NodeId, addr: SocketAddr) -> bool {
        let new_pairs = self.add_nodes_batch_internal(&[(id, addr)]);
        let is_new = !new_pairs.is_empty();
        self.propagate_nodes(new_pairs);
        is_new
    }

    /// 鎵归噺鍔犲叆鑺傜偣锛堜竴娆″啓閿侊級锛岃繑鍥炴柊鍔犲叆鏁般€?
    /// 鏈湴鍐欏叆璺緞锛氭柊鑺傜偣鏇存柊 Merkle + 鎻愪氦 Gossip銆?
    /// 鑱旈偊鍚屾鍏ョ珯锛坅pply_node_sync锛夎鏀圭敤 add_nodes_batch_internal锛岄伩鍏嶅洖鐜€?
    pub fn add_nodes_sync_batch(&self, items: &[(NodeId, SocketAddr)]) -> usize {
        let new_pairs = self.add_nodes_batch_internal(items);
        let count = new_pairs.len();
        self.propagate_nodes(new_pairs);
        count
    }

    pub fn contains_sync(&self, addr: SocketAddr) -> bool {
        self.nodes.contains_key(&addr)
    }

    /// v9：读取内存中该地址当前的 node_id（供联邦入站的确定性裁决使用）。
    ///
    /// 联邦同步里同一 `ip:port` 在两端的 node_id 常常不同（DHT 节点重启/重新 announce），
    /// 而 NODE 的区间摘要 `blake3(node_id‖ip‖port)` 对 id 敏感 —— 若不裁决，两端会对同一
    /// key 永久互判「内容不同」，反熵反复推拉却永远抹不平。裁决规则由调用方（`apply_node_sync`）
    /// 统一为「取字典序较小者」，本方法只负责提供本地当前值。
    pub fn node_id_sync(&self, addr: SocketAddr) -> Option<NodeId> {
        self.nodes.get(&addr).map(|e| e.id)
    }

    pub fn len_sync(&self) -> usize {
        self.nodes.len()
    }

    // 鈹€鈹€ /24 缃戞绱㈠紩鏌ヨ锛圤(1) 瀹氫綅缃戞锛屼緵鑺傜偣閫夋嫨/鐩戞帶浣跨敤锛夆攢鈹€

    /// 鑾峰彇鎸囧畾 /24 缃戞鐨勮妭鐐?ID 鍒楄〃
    pub fn nodes_by_subnet_sync(&self, subnet: [u8; 3]) -> Vec<NodeId> {
        self.subnet_index
            .read()
            .get(&subnet)
            .cloned()
            .unwrap_or_default()
    }

    /// 鑾峰彇鎵€鏈?/24 缃戞鍒楄〃
    pub fn all_subnets_sync(&self) -> Vec<[u8; 3]> {
        self.subnet_index.read().keys().copied().collect()
    }

    /// /24 缃戞鏁伴噺
    pub fn subnet_count_sync(&self) -> usize {
        self.subnet_index.read().len()
    }

    /// 全量扫描内存表，返回 (total, good, questionable, bad, active, score_sum)。
    ///
    /// 复用调用方已持有的读/写守卫：load_initial / load_all 已持有 write_all，
    /// 若在其中再 read_all 会因同线程重复加 RwLock 写锁而死锁。
    fn scan_nodes_stats<'a>(
        iter: impl Iterator<Item = &'a KBucketEntry>,
        total: usize,
    ) -> (u64, u64, u64, u64, u64, f64) {
        let mut good = 0u64;
        let mut questionable = 0u64;
        let mut bad = 0u64;
        let mut active = 0u64;
        let mut score_sum = 0.0f64;
        for n in iter {
            match n.state {
                NodeState::Good => good += 1,
                NodeState::Questionable => questionable += 1,
                NodeState::Bad => bad += 1,
            }
            if n.query_count > 0 {
                active += 1;
            }
            score_sum += n.score;
        }
        (total as u64, good, questionable, bad, active, score_sum)
    }

    /// 全量扫描并把 6 个计数器**直接置为**扫描结果（置为而非累加）。
    ///
    /// 仅在 load_initial / load_all 结束时调用（启动阶段无并发）。重复加载也安全：
    /// 无论加载前计数器是什么，都被权威扫描结果覆盖。
    fn calibrate_stats<'a>(&self, iter: impl Iterator<Item = &'a KBucketEntry>, total: usize) {
        let (t, g, q, b, a, s) = Self::scan_nodes_stats(iter, total);
        self.total.store(t, Ordering::Relaxed);
        self.good.store(g, Ordering::Relaxed);
        self.questionable.store(q, Ordering::Relaxed);
        self.bad.store(b, Ordering::Relaxed);
        self.active.store(a, Ordering::Relaxed);
        self.score_sum.store(s.to_bits(), Ordering::Relaxed);
    }

    /// 新增一个内存节点时累加计数器（调用方已持 nodes 写锁/分片锁）。
    #[inline]
    fn acc_add_entry(&self, e: &KBucketEntry) {
        self.total.fetch_add(1, Ordering::Relaxed);
        match e.state {
            NodeState::Good => self.good.fetch_add(1, Ordering::Relaxed),
            NodeState::Questionable => self.questionable.fetch_add(1, Ordering::Relaxed),
            NodeState::Bad => self.bad.fetch_add(1, Ordering::Relaxed),
        };
        if e.query_count > 0 {
            self.active.fetch_add(1, Ordering::Relaxed);
        }
        self.acc_add_score(e.score);
    }

    /// 移除一个内存节点时按其**旧字段**扣减计数器（调用方已持 nodes 写锁/分片锁）。
    #[inline]
    fn acc_remove_entry(&self, e: &KBucketEntry) {
        self.total.fetch_sub(1, Ordering::Relaxed);
        match e.state {
            NodeState::Good => self.good.fetch_sub(1, Ordering::Relaxed),
            NodeState::Questionable => self.questionable.fetch_sub(1, Ordering::Relaxed),
            NodeState::Bad => self.bad.fetch_sub(1, Ordering::Relaxed),
        };
        if e.query_count > 0 {
            self.active.fetch_sub(1, Ordering::Relaxed);
        }
        self.acc_add_score(-e.score);
    }

    /// state 迁移：仅当新旧档不同时，旧档 -1、新档 +1（同档 no-op）。
    #[inline]
    fn acc_transition_state(&self, old: NodeState, new: NodeState) {
        if old == new {
            return;
        }
        match old {
            NodeState::Good => self.good.fetch_sub(1, Ordering::Relaxed),
            NodeState::Questionable => self.questionable.fetch_sub(1, Ordering::Relaxed),
            NodeState::Bad => self.bad.fetch_sub(1, Ordering::Relaxed),
        };
        match new {
            NodeState::Good => self.good.fetch_add(1, Ordering::Relaxed),
            NodeState::Questionable => self.questionable.fetch_add(1, Ordering::Relaxed),
            NodeState::Bad => self.bad.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// score_sum 增量。f64 求和不能对 bit 模式做整数加法，故用 CAS 循环。
    /// 调用方多数在 write_all 内（互斥），仅 with_mut 单分片路径可能并发，CAS 保证正确。
    #[inline]
    fn acc_add_score(&self, delta: f64) {
        let mut old_bits = self.score_sum.load(Ordering::Relaxed);
        loop {
            let new_bits = (f64::from_bits(old_bits) + delta).to_bits();
            match self.score_sum.compare_exchange_weak(
                old_bits,
                new_bits,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(cur) => old_bits = cur,
            }
        }
    }

    /// 鑺傜偣缁熻淇℃伅锛堥伩鍏嶅叏閲忓厠闅嗭紝鐢ㄤ簬鍋ュ悍搴﹁绠楀拰鐩戞帶锛?
    ///
    /// O(1) 无锁无遍历：只读 6 个原子计数器组装。语义与原全量遍历版完全一致：
    /// active = query_count>0 的节点数；avg_score = score_sum/total，total==0 时 0.0。
    pub fn stats_sync(&self) -> NodeStats {
        let total = self.total.load(Ordering::Relaxed) as usize;
        let good = self.good.load(Ordering::Relaxed) as usize;
        let questionable = self.questionable.load(Ordering::Relaxed) as usize;
        let bad = self.bad.load(Ordering::Relaxed) as usize;
        let active = self.active.load(Ordering::Relaxed) as usize;
        let score_sum = f64::from_bits(self.score_sum.load(Ordering::Relaxed));
        let avg_score = if total > 0 {
            score_sum / total as f64
        } else {
            0.0
        };
        NodeStats {
            total,
            good,
            questionable,
            bad,
            active,
            avg_score,
        }
    }

    /// 周期抽查：全量扫描内存表，与原子计数器比对，返回差值（= 计数器 − 实测扫描值）。
    ///
    /// 供 stats_snapshot 的 60s 后台任务调用并据此打 WARN。本方法**不打印日志**，
    /// 也不修改任何状态；仅一次读锁遍历 + 原子读。健康时全 0。
    pub fn verify_stats_consistency(&self) -> NodeStatsDrift {
        let nodes = self.nodes.read_all();
        let (t, g, q, b, a, s) = Self::scan_nodes_stats(nodes.values(), nodes.len());
        drop(nodes);

        let c_total = self.total.load(Ordering::Relaxed);
        let c_good = self.good.load(Ordering::Relaxed);
        let c_questionable = self.questionable.load(Ordering::Relaxed);
        let c_bad = self.bad.load(Ordering::Relaxed);
        let c_active = self.active.load(Ordering::Relaxed);
        let c_sum = f64::from_bits(self.score_sum.load(Ordering::Relaxed));

        let measured_avg = if t > 0 { s / t as f64 } else { 0.0 };
        let counted_avg = if c_total > 0 {
            c_sum / c_total as f64
        } else {
            0.0
        };

        NodeStatsDrift {
            total: c_total as i64 - t as i64,
            good: c_good as i64 - g as i64,
            questionable: c_questionable as i64 - q as i64,
            bad: c_bad as i64 - b as i64,
            active: c_active as i64 - a as i64,
            avg_score_diff: counted_avg - measured_avg,
        }
    }

    pub fn top_nodes_sync(&self, n: usize) -> Vec<KBucketEntry> {
        let mut all: Vec<KBucketEntry> = self.nodes.read_all().values().cloned().collect();
        all.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(n);
        all
    }

    pub fn all_nodes_sync(&self) -> Vec<KBucketEntry> {
        self.nodes.read_all().values().cloned().collect()
    }

    pub fn record_query_sync(&self, addr: SocketAddr, success: bool, latency_ms: u64) {
        let mut nodes = self.nodes.write_all();
        let mut verified = false;
        if let Some(entry) = nodes.get_mut(&addr) {
            // active：query_count 0→1 时 +1（只增不减，删除时扣减）
            if entry.query_count == 0 {
                self.active.fetch_add(1, Ordering::Relaxed);
            }
            entry.query_count += 1;
            entry.last_query_time = Some(Instant::now());
            self.record_recent(entry, success);
            if success {
                entry.success_count += 1;
                entry.total_latency_ms += latency_ms;
                entry.last_active = Instant::now();
                entry.last_verified = Some(Instant::now());
                verified = true;
                let old = entry.state;
                entry.state = NodeState::Good;
                self.acc_transition_state(old, NodeState::Good);
                entry.consecutive_failures = 0;
            } else {
                entry.consecutive_failures += 1;
                let threshold = self.bad_after_failures.load(Ordering::Relaxed) as u32;
                if entry.consecutive_failures >= threshold.max(1) {
                    let old = entry.state;
                    entry.state = NodeState::Bad;
                    self.acc_transition_state(old, NodeState::Bad);
                }
            }
            drop(nodes);
            self.dirty.write().insert(addr);
            // 已验证响应成功 → 进热池（热池语义 = 已验证/新发现，2026-10 R2）
            if verified {
                self.hot_addrs.write().insert(addr);
            }
        }
    }

    /// 璁板綍鏌ヨ鎴愬姛鍙婅繑鍥炵殑鑺傜偣鏁帮紙鐢ㄤ簬鑺傜偣浜у嚭缁村害璇勫垎锛?
    pub fn record_query_with_nodes_sync(
        &self,
        addr: SocketAddr,
        latency_ms: u64,
        nodes_returned: u64,
    ) {
        let mut nodes = self.nodes.write_all();
        if let Some(entry) = nodes.get_mut(&addr) {
            if entry.query_count == 0 {
                self.active.fetch_add(1, Ordering::Relaxed);
            }
            entry.query_count += 1;
            entry.success_count += 1;
            entry.total_latency_ms += latency_ms;
            entry.nodes_returned += nodes_returned;
            entry.last_active = Instant::now();
            entry.last_query_time = Some(Instant::now());
            entry.last_verified = Some(Instant::now());
            self.record_recent(entry, true);
            let old = entry.state;
            entry.state = NodeState::Good;
            self.acc_transition_state(old, NodeState::Good);
            entry.consecutive_failures = 0;
            drop(nodes);
            self.dirty.write().insert(addr);
            // 已验证响应成功 → 进热池（热池语义 = 已验证/新发现，2026-10 R2）
            self.hot_addrs.write().insert(addr);
        }
    }

    /// 鍒锋柊鎵€鏈夎妭鐐圭姸鎬侊紙鍩轰簬鏈€鍚庢椿璺冩椂闂存洿鏂?Good/Questionable锛?
    pub fn refresh_all_states_sync(&self) {
        let mut nodes = self.nodes.write_all();
        // refresh_state() 会把 Good/Questionable 按 last_active 互切（Bad 冻结不变），
        // 故必须捕获旧档并做状态迁移计数；旧==新时 acc_transition_state 为 no-op。
        nodes.for_values_mut(|entry| {
            let old = entry.state;
            entry.refresh_state();
            self.acc_transition_state(old, entry.state);
        });
    }

    // 鈹€鈹€ 鍐风儹鍒嗗眰绱㈠紩锛堝唴瀛樼储寮曟鏋讹紝涓嶆惉鏁版嵁锛岀敱澶栭儴 TaskScheduler 璋冨害杩佺Щ锛夆攢鈹€

    /// 鏍囪鑺傜偣琚闂紙鏇存柊 last_accessed锛岀Щ鍏?hot 闆嗗悎锛屼粠 cold 绉婚櫎锛?
    pub fn mark_accessed_sync(&self, addr: SocketAddr) {
        let found = {
            let mut nodes = self.nodes.write_all();
            if let Some(entry) = nodes.get_mut(&addr) {
                entry.last_accessed = Some(Instant::now());
                true
            } else {
                false
            }
        };
        if found {
            self.hot_addrs.write().insert(addr);
            self.cold_addrs.write().remove(&addr);
        }
    }

    /// 杩斿洖鐑妭鐐瑰垪琛紙hot 闆嗗悎涓殑鑺傜偣锛屾寜璇勫垎闄嶅簭鐢辫皟鐢ㄦ柟鎺掑簭锛?
    pub fn hot_nodes_sync(&self) -> Vec<KBucketEntry> {
        let nodes = self.nodes.read_all();
        self.hot_addrs
            .read()
            .iter()
            .filter_map(|addr| nodes.get(addr).cloned())
            .collect()
    }

    /// 灏嗚秴杩囬槇鍊肩殑鑺傜偣浠?hot 绉诲埌 cold锛堢敱澶栭儴 TaskScheduler 瀹氭椂璋冪敤锛屾ā鍧楀唴涓嶈嚜璺戝畾鏃讹級
    /// 杩斿洖鏈杩佺Щ鐨勮妭鐐规暟
    pub fn migrate_hot_to_cold_sync(&self, threshold_secs: u64) -> usize {
        let cutoff = crate::utils::cutoff_before(Duration::from_secs(threshold_secs));
        let nodes = self.nodes.read_all();
        let mut hot = self.hot_addrs.write();
        let mut cold = self.cold_addrs.write();
        let to_move: Vec<SocketAddr> = hot
            .iter()
            .filter(|addr| {
                nodes
                    .get(addr)
                    .and_then(|e| e.last_accessed)
                    .map(|t| t < cutoff)
                    .unwrap_or(true)
            })
            .copied()
            .collect();
        let moved = to_move.len();
        for addr in to_move {
            hot.remove(&addr);
            cold.insert(addr);
        }
        moved
    }

    /// 鐑妭鐐规暟閲?
    pub fn hot_count_sync(&self) -> usize {
        self.hot_addrs.read().len()
    }

    /// 鍐疯妭鐐规暟閲?
    pub fn cold_count_sync(&self) -> usize {
        self.cold_addrs.read().len()
    }

    // 鈹€鈹€ 鑴忔爣璁板悓姝ユ柟娉曪紙鐢ㄤ簬澧為噺璇勫垎 + 澧為噺鎸佷箙鍖栵級鈹€鈹€

    pub fn mark_dirty_sync(&self, addr: SocketAddr) {
        self.dirty.write().insert(addr);
    }

    pub fn dirty_nodes_sync(&self) -> Vec<SocketAddr> {
        self.dirty.read().iter().cloned().collect()
    }

    pub fn clear_dirty_sync(&self, addr: &SocketAddr) {
        self.dirty.write().remove(addr);
    }

    /// 只清除指定节点的脏标记（避免全量清除误伤并发新增的脏标记）
    pub fn clear_dirty_batch_sync(&self, addrs: &[SocketAddr]) {
        if addrs.is_empty() {
            return;
        }
        let mut dirty = self.dirty.write();
        for addr in addrs {
            dirty.remove(addr);
        }
    }

    pub fn clear_all_dirty_sync(&self) {
        self.dirty.write().clear();
    }

    /// 鍙栧嚭鎵€鏈?dirty 鑺傜偣骞舵竻绌猴紙鍘熷瓙鎿嶄綔锛岀敤浜庡閲忔寔涔呭寲锛?
    pub fn take_dirty_sync(&self) -> Vec<SocketAddr> {
        let mut dirty = self.dirty.write();
        let addrs: Vec<SocketAddr> = dirty.iter().cloned().collect();
        dirty.clear();
        addrs
    }

    /// dirty 鑺傜偣鏁伴噺
    pub fn dirty_count_sync(&self) -> usize {
        self.dirty.read().len()
    }

    /// WriteQueue 闃熷垪闀垮害锛堝鏋滄帴鍏ヤ簡鍐欏叆闃熷垪锛?
    pub fn write_queue_len_sync(&self) -> usize {
        self.write_queue
            .as_ref()
            .map(|wq| wq.stats().queue_size)
            .unwrap_or(0)
    }

    /// 鏍规嵁 dirty 鍦板潃鍒楄〃鏋勫缓 DhtNodeRow 鎵归噺锛堜粠鍐呭瓨 nodes 璇诲彇锛屼笉淇敼浠讳綍鐘舵€侊級
    fn build_dirty_batch(&self, dirty_addrs: &[SocketAddr]) -> Vec<DhtNodeRow> {
        let nodes = self.nodes.read_all();
        dirty_addrs
            .iter()
            .filter_map(|addr| nodes.get(addr))
            .map(|node| {
                let state_str = match node.state {
                    NodeState::Good => "Good",
                    NodeState::Questionable => "Questionable",
                    NodeState::Bad => "Bad",
                };
                DhtNodeRow {
                    id: node.id,
                    ip: node.addr.ip().to_string(),
                    port: node.addr.port(),
                    score: node.score,
                    state: state_str.to_string(),
                    query_count: node.query_count,
                    success_count: node.success_count,
                    total_latency_ms: node.total_latency_ms,
                    consecutive_failures: node.consecutive_failures,
                    nodes_returned: node.nodes_returned,
                    last_query_time: node.last_query_time.map(|t| t.elapsed().as_secs() as i64),
                    // 落库节点真实 last_active（Unix 秒），而非落库时刻——
                    // 加载侧才能还原新近度（2026-10 R1 修复）
                    last_active: Some(crate::utils::instant_to_unix_secs(node.last_active)),
                }
            })
            .collect()
    }

    /// 鎵归噺鏇存柊璇勫垎锛堜竴娆″啓閿侊紝閬垮厤閫愪釜鏇存柊鐨勯攣绔炰簤锛?
    pub fn update_scores_batch_sync(&self, scores: &[(SocketAddr, f64)]) {
        let mut nodes = self.nodes.write_all();
        let mut dirty = self.dirty.write();
        for (addr, score) in scores {
            if let Some(entry) = nodes.get_mut(addr) {
                let old = entry.score;
                entry.score = *score;
                self.acc_add_score(*score - old);
                dirty.insert(*addr);
            }
        }
    }
}

#[async_trait]
impl NodeRepository for NodeRepoImpl {
    async fn add_node(&self, id: NodeId, addr: SocketAddr) -> bool {
        self.add_node_sync(id, addr)
    }

    async fn remove_node(&self, addr: &SocketAddr) -> bool {
        let removed_entry = self.nodes.remove(addr);
        let Some(entry) = removed_entry else {
            return false;
        };
        self.acc_remove_entry(&entry);
        // 浠?/24 缃戞绱㈠紩涓Щ闄よ鑺傜偣
        self.unindex_subnet(addr, &entry.id);
        // 同步清理分层集合，避免 hot/cold 无界残留导致计数虚高与内存泄漏
        self.hot_addrs.write().remove(addr);
        self.cold_addrs.write().remove(addr);
        // 节点已删除，不再是待评分脏节点
        self.dirty.write().remove(addr);
        // P1-6: persist soft-delete tombstone (deleted_at) so a restart cannot resurrect the row.
        // Without this, "removed locally" and "never existed" are indistinguishable in set
        // semantics, so the peer's Merkle never converges to 0. ip/port formatting mirrors
        // to_rows() (ip = addr.ip().to_string()), keeping the UPDATE key aligned with inserts.
        if let Err(e) = self
            .storage
            .soft_delete_node(&addr.ip().to_string(), addr.port())
        {
            tracing::warn!("[node_repo] soft_delete_node failed for {}: {}", addr, e);
        }
        // 联邦删除传播：提交 DELETE 条目，避免对端 upsert-only 合并导致"删除复活"
        let key = addr.to_string().into_bytes();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let items = vec![SyncEntry {
            key,
            operation: operation::DELETE,
            version: now,
            payload: Vec::new(),
        }];
        // P1-2：删除同样记入 oplog（delta 通道需要传播删除，否则对端永不删）
        crate::storage::oplog::record_local_ops(&self.storage, repo_type::NODE, &items);
        if let Some(gossip) = self.gossip.get() {
            gossip.submit_gossip(repo_type::NODE, items);
        }
        true
    }

    async fn get_node(&self, addr: &SocketAddr) -> Option<KBucketEntry> {
        self.nodes.get(addr)
    }

    async fn all_nodes(&self) -> Vec<KBucketEntry> {
        self.all_nodes_sync()
    }

    async fn node_count(&self) -> usize {
        self.len_sync()
    }

    async fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    async fn top_nodes(&self, n: usize) -> Vec<KBucketEntry> {
        self.top_nodes_sync(n)
    }

    fn top_nodes_sync(&self, n: usize) -> Vec<KBucketEntry> {
        NodeRepoImpl::top_nodes_sync(self, n)
    }

    fn len_sync(&self) -> usize {
        NodeRepoImpl::len_sync(self)
    }

    fn nodes_by_subnet_sync(&self, subnet: [u8; 3]) -> Vec<NodeId> {
        NodeRepoImpl::nodes_by_subnet_sync(self, subnet)
    }

    fn subnet_count_sync(&self) -> usize {
        NodeRepoImpl::subnet_count_sync(self)
    }

    fn hot_nodes_sync(&self) -> Vec<KBucketEntry> {
        NodeRepoImpl::hot_nodes_sync(self)
    }

    fn mark_accessed_sync(&self, addr: SocketAddr) {
        NodeRepoImpl::mark_accessed_sync(self, addr);
    }

    async fn closest_nodes(&self, target: &NodeId, n: usize) -> Vec<KBucketEntry> {
        // NodeRepo 涓嶇淮鎶よ矾鐢辫〃锛屾寜 XOR 璺濈鎺掑簭
        let mut all: Vec<KBucketEntry> = self.all_nodes_sync();
        all.sort_by_key(|e| crate::dht::xor_distance(&e.id, target));
        all.truncate(n);
        all
    }

    async fn update_score(&self, addr: &SocketAddr, score: f64) {
        self.nodes.with_mut(addr, |entry| {
            let old = entry.score;
            entry.score = score;
            self.acc_add_score(score - old);
        });
    }

    async fn update_scores_batch(&self, scores: &[(SocketAddr, f64)]) {
        self.update_scores_batch_sync(scores);
    }

    async fn record_query(&self, addr: &SocketAddr, success: bool, latency_ms: u64) {
        self.record_query_sync(*addr, success, latency_ms);
    }

    async fn set_node_state(&self, addr: &SocketAddr, state: NodeState) {
        self.nodes.with_mut(addr, |entry| {
            let old = entry.state;
            entry.state = state;
            self.acc_transition_state(old, state);
        });
        // 鐘舵€佸彉鍖栦篃鏍囪涓鸿剰
        self.dirty.write().insert(*addr);
    }

    async fn refresh_all_states(&self) {
        self.refresh_all_states_sync();
    }

    async fn stats(&self) -> NodeStats {
        self.stats_sync()
    }

    async fn mark_dirty(&self, addr: &SocketAddr) {
        self.mark_dirty_sync(*addr);
    }

    async fn dirty_nodes(&self) -> Vec<SocketAddr> {
        self.dirty_nodes_sync()
    }

    async fn clear_dirty(&self, addr: &SocketAddr) {
        self.clear_dirty_sync(addr);
    }

    async fn clear_dirty_batch(&self, addrs: &[SocketAddr]) {
        self.clear_dirty_batch_sync(addrs);
    }

    async fn clear_all_dirty(&self) {
        self.clear_all_dirty_sync();
    }

    async fn bucket_count(&self) -> usize {
        0 // NodeRepo 涓嶇淮鎶?bucket
    }

    async fn non_empty_bucket_targets(&self) -> Vec<NodeId> {
        Vec::new() // NodeRepo 涓嶇淮鎶?bucket
    }

    async fn rescore_all(&self) {
        // 璇勫垎鐢?ScoreMaintainer 缁熶竴缁存姢锛孯epo 涓嶅叿澶囩畻鍒嗘潈闄?
    }

    async fn save_dirty(&self) -> anyhow::Result<()> {
        // 銆愬閲忔寔涔呭寲銆戝彧淇濆瓨 dirty 鑺傜偣锛岄伩鍏嶅叏閲忎繚瀛樺崈涓囩骇鏁版嵁
        if let Some(wq) = &self.write_queue {
            // 寮傛妯″紡锛氬師瀛愬彇鍑哄苟娓呯┖ dirty锛岄潪闃诲鍏ラ槦 WriteQueue
            // B3：先快照 dirty（不清空），落库成功后才在闭包内清 dirty
            let dirty_addrs = self.dirty_nodes_sync();
            if dirty_addrs.is_empty() {
                return Ok(());
            }
            let batch = self.build_dirty_batch(&dirty_addrs);
            if batch.is_empty() {
                return Ok(());
            }
            let wq = wq.clone();
            let count = batch.len();
            let rows = count;
            let dirty_arc = self.dirty.clone();
            let ack = dirty_addrs.clone();
            let _ = wq.send_sized(rows, move |conn| {
                Storage::save_dht_nodes_batch_in_tx(conn, &batch)?;
                // 落库（同事务）成功后才清 dirty；失败保留待重试
                for a in &ack {
                    dirty_arc.write().remove(a);
                }
                Ok(())
            });
            tracing::debug!("[node_repo] 寮傛鍏ラ槦淇濆瓨 {} 涓?dirty 鑺傜偣", count);
            Ok(())
        } else {
            // 鍚屾妯″紡锛氬厛鏌ョ湅 dirty锛堜笉娓呯┖锛夛紝淇濆瓨鎴愬姛鍚庡啀娓呯┖锛屽け璐ュ垯淇濈暀閲嶈瘯
            let dirty_addrs = self.dirty_nodes_sync();
            if dirty_addrs.is_empty() {
                return Ok(());
            }
            let batch = self.build_dirty_batch(&dirty_addrs);
            if batch.is_empty() {
                // 鍐呭瓨涓凡涓嶅瓨鍦ㄧ殑 dirty 鑺傜偣锛堝彲鑳藉凡琚垹闄わ級锛屾竻鐞嗘爣璁?
                let mut dirty = self.dirty.write();
                for addr in &dirty_addrs {
                    dirty.remove(addr);
                }
                return Ok(());
            }
            let storage = self.storage.clone();
            let count = batch.len();
            tracing::debug!("[node_repo] 澧為噺淇濆瓨 {} 涓?dirty 鑺傜偣", count);
            let result =
                tokio::task::spawn_blocking(move || storage.save_dht_nodes_batch(&batch)).await?;
            match result {
                Ok(()) => {
                    let mut dirty = self.dirty.write();
                    for addr in &dirty_addrs {
                        dirty.remove(addr);
                    }
                    Ok(())
                }
                Err(e) => {
                    tracing::warn!(
                        "[node_repo] 澧為噺淇濆瓨澶辫触锛堜繚鐣?dirty 寰呴噸璇曪級: {}",
                        e
                    );
                    Err(e)
                }
            }
        }
    }

    async fn load_all(&self) -> anyhow::Result<usize> {
        let rows = self.storage.load_dht_nodes()?;
        let mut nodes = self.nodes.write_all();
        let mut subnet_index = self.subnet_index.write();
        let mut count = 0;
        for row in rows {
            let addr = SocketAddr::new(row.ip.parse().unwrap_or([127, 0, 0, 1].into()), row.port);
            let mut entry = KBucketEntry::new(row.id, addr);
            entry.score = row.score;
            entry.query_count = row.query_count;
            entry.success_count = row.success_count;
            entry.total_latency_ms = row.total_latency_ms;
            entry.consecutive_failures = row.consecutive_failures;
            entry.nodes_returned = row.nodes_returned;
            entry.last_query_time = row
                .last_query_time
                .map(|secs| crate::utils::cutoff_before(Duration::from_secs(secs.max(0) as u64)));
            entry.state = match row.state.as_str() {
                "Good" => NodeState::Good,
                "Questionable" => NodeState::Questionable,
                _ => NodeState::Bad,
            };
            nodes.insert(addr, entry);
            // 閲嶅缓 /24 缃戞绱㈠紩
            if let Some(subnet) = Self::subnet_key(addr) {
                Self::index_subnet(&mut subnet_index, subnet, row.id);
            }
            count += 1;
        }
        // 校准回填：全量扫描把 6 个计数器直接置为扫描结果（置为而非累加）。
        // 此处仍持有 nodes 写锁，复用守卫避免重复加锁死锁。
        self.calibrate_stats(nodes.values(), nodes.len());
        Ok(count)
    }

    async fn remove_cold_nodes(&self, older_than_secs: u64) -> anyhow::Result<usize> {
        // 椹遍€愬墠鑻ュ瓨鍦ㄨ剰鏁版嵁锛屽厛钀藉簱锛岄伩鍏嶄涪澶卞皻鏈寔涔呭寲鐨勮妭鐐规洿鏂般€?
        // 澶辫触涓嶉樆鏂┍閫愶紙鑺傜偣鍦?DB 涓粛鏈変笂涓€浠藉揩鐓э紝涓嶄涪琛岋級銆?
        if self.dirty_count_sync() > 0 {
            if let Err(e) = self.save_dirty().await {
                tracing::warn!(
                    "[node_repo] 鍐疯妭鐐归┍閫愬墠澧為噺鎸佷箙鍖栧け璐ワ紙缁х画椹遍€愶級: {}",
                    e
                );
            }
        }

        // cutoff锛歭ast_active 鏃╀簬璇ユ椂鍒荤殑鑺傜偣瑙嗕负鍐疯妭鐐广€?
        // 浣跨敤 checked_sub 閬垮厤闃堝€艰繃澶у鑷?Instant 涓嬫孩锛涙湭鏉ユ椂闂寸殑 last_active 澶╃劧鏅氫簬 cutoff锛屼笉浼氳璇垹銆?
        let cutoff = match Instant::now().checked_sub(Duration::from_secs(older_than_secs)) {
            Some(c) => c,
            None => return Ok(0),
        };

        // 绗竴閬嶏細璇婚攣鍐呭揩鐓у€欓€夊湴鍧€锛堥伩鍏嶆寔鍐欓攣闀挎椂闂撮亶鍘嗭級
        let candidates: Vec<(SocketAddr, NodeId)> = {
            let nodes = self.nodes.read_all();
            nodes
                .iter()
                .filter(|(_, e)| e.last_active < cutoff)
                .map(|(addr, e)| (*addr, e.id))
                .collect()
        };
        if candidates.is_empty() {
            return Ok(0);
        }

        // 绗簩閬嶏細鎸佸啓閿佹壒閲忕Щ闄わ紝鍚屾娓呯悊 /24 绱㈠紩銆佺儹/鍐烽泦鍚堜笌鑴忔爣璁般€?
        // 娉ㄦ剰锛氫笉鎶婅绉婚櫎鑺傜偣鍔犲叆 dirty 闆嗗悎鈥斺€擠B 琛屾案涔呬繚鐣欙紝鍒犻櫎浠呬綔鐢ㄤ簬鍐呭瓨銆?
        let mut nodes = self.nodes.write_all();
        let mut subnet_index = self.subnet_index.write();
        let mut hot = self.hot_addrs.write();
        let mut cold = self.cold_addrs.write();
        let mut dirty = self.dirty.write();
        let mut removed = 0;
        for (addr, id) in &candidates {
            if let Some(entry) = nodes.remove(addr) {
                self.acc_remove_entry(&entry);
                removed += 1;
                if let Some(subnet) = Self::subnet_key(*addr) {
                    if let Some(bucket) = subnet_index.get_mut(&subnet) {
                        bucket.retain(|x| x != id);
                        if bucket.is_empty() {
                            subnet_index.remove(&subnet);
                        }
                    }
                }
                hot.remove(addr);
                cold.remove(addr);
                dirty.remove(addr);
            }
        }
        Ok(removed)
    }

    /// 按数量驱逐：内存中节点数超过 max_count 时，驱逐最久未活跃的节点。
    fn evict_by_count(&self, max_count: usize) -> usize {
        let total = self.nodes.len();
        if total <= max_count {
            return 0;
        }
        let to_remove = total - max_count;

        // 第一遍：读锁内收集候选地址（按 last_active 升序，最久未活跃的在前）
        let candidates: Vec<SocketAddr> = {
            let nodes = self.nodes.read_all();
            let mut entries: Vec<(SocketAddr, Instant)> = nodes
                .iter()
                .map(|(addr, e)| (*addr, e.last_active))
                .collect();
            entries.sort_by_key(|(_, last)| *last);
            entries
                .into_iter()
                .take(to_remove)
                .map(|(addr, _)| addr)
                .collect()
        };

        // 第二遍：写锁批量移除
        let mut nodes = self.nodes.write_all();
        let mut subnet_index = self.subnet_index.write();
        let mut hot = self.hot_addrs.write();
        let mut cold = self.cold_addrs.write();
        let mut dirty = self.dirty.write();
        let mut removed = 0;
        for addr in &candidates {
            if let Some(entry) = nodes.remove(addr) {
                self.acc_remove_entry(&entry);
                removed += 1;
                if let Some(subnet) = Self::subnet_key(*addr) {
                    if let Some(bucket) = subnet_index.get_mut(&subnet) {
                        bucket.retain(|_x| !nodes.contains_key(addr));
                        if bucket.is_empty() {
                            subnet_index.remove(&subnet);
                        }
                    }
                }
                hot.remove(addr);
                cold.remove(addr);
                dirty.remove(addr);
            }
        }
        if removed > 0 {
            tracing::info!(
                "[node_repo] 按数量驱逐: {} 个节点（内存 {}→{}，上限 {}）",
                removed,
                total,
                total - removed,
                max_count
            );
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_repo() -> NodeRepoImpl {
        let storage = Arc::new(Storage::memory().unwrap());
        NodeRepoImpl::new(storage)
    }

    /// 近期窗口统计（2026-10 R4/D3）：record_query 路径同步更新窗口，
    /// α=0 时为精确计数，窗口响应率可覆盖终身统计
    #[tokio::test]
    async fn test_recent_window_accumulates() {
        let storage = Arc::new(Storage::memory().unwrap());
        let repo = NodeRepoImpl::new(storage).with_recent_decay_alpha(0.0);
        let a = addr(7, 3007);
        repo.add_node_sync([7u8; 20], a);
        repo.record_query_sync(a, true, 5);
        repo.record_query_sync(a, true, 5);
        repo.record_query_sync(a, false, 0);

        let e = repo.get_node(&a).await.unwrap();
        assert!((e.recent_query - 3.0).abs() < 1e-9);
        assert!((e.recent_success - 2.0).abs() < 1e-9);
        let r = e.windowed_response_rate(0.0, 1.0).unwrap();
        assert!((r - 2.0 / 3.0).abs() < 1e-9);
    }

    /// 判 Bad 阈值可配置（2026-10 D4）：threshold=2 时两次失败即 Bad
    #[tokio::test]
    async fn test_bad_after_failures_configurable() {
        let storage = Arc::new(Storage::memory().unwrap());
        let repo = NodeRepoImpl::new(storage).with_bad_after_failures(2);
        let a = addr(8, 3008);
        repo.add_node_sync([8u8; 20], a);
        repo.record_query_sync(a, false, 0);
        assert_ne!(repo.get_node(&a).await.unwrap().state, NodeState::Bad);
        repo.record_query_sync(a, false, 0);
        assert_eq!(repo.get_node(&a).await.unwrap().state, NodeState::Bad);
    }

    /// Bad 冷却复活（2026-10 D4/R7）：冷却期后被再次提及 → Questionable、失败数减半；
    /// 冷却期内重复提及不复活
    #[tokio::test]
    async fn test_bad_revival_via_mention() {
        let repo = test_repo();
        let a = addr(11, 3011);
        repo.add_node_sync([11u8; 20], a);
        repo.record_query_sync(a, false, 0);
        repo.record_query_sync(a, false, 0);
        repo.record_query_sync(a, false, 0);
        assert_eq!(repo.get_node(&a).await.unwrap().state, NodeState::Bad);

        // 刚被判死（last_mentioned=now，未过冷却期 600s）→ 提及不复活
        repo.add_nodes_batch_internal(&[([11u8; 20], a)]);
        assert_eq!(
            repo.get_node(&a).await.unwrap().state,
            NodeState::Bad,
            "冷却期内提及不得复活"
        );

        // 把 last_mentioned/last_query_time 都推到冷却期之外 → 再次提及复活
        repo.nodes.with_mut(&a, |e| {
            let old = crate::utils::cutoff_before(Duration::from_secs(700));
            e.last_mentioned = Some(old);
            e.last_query_time = Some(old);
        });
        repo.add_nodes_batch_internal(&[([11u8; 20], a)]);
        let e = repo.get_node(&a).await.unwrap();
        assert_eq!(e.state, NodeState::Questionable, "冷却期后提及应复活");
        assert_eq!(e.consecutive_failures, 1, "失败数应减半（3→1）");
    }

    fn addr(oct: u8, port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, oct)), port)
    }

    #[tokio::test]
    async fn test_remove_cold_nodes_removes_only_cold() {
        let repo = test_repo();
        let hot = addr(1, 1001);
        let warm = addr(2, 1002);
        let cold = addr(3, 1003);

        repo.add_node_sync([1u8; 20], hot);
        repo.add_node_sync([2u8; 20], warm);
        repo.add_node_sync([3u8; 20], cold);
        assert_eq!(repo.len_sync(), 3);

        // 鎶?warm 鎺ㄥ埌闃堝€煎唴鍋忎箙銆乧old 鎺ㄥ埌瓒呰繃 warm 闃堝€硷紙7200s锛?
        let warm_cutoff = crate::utils::cutoff_before(Duration::from_secs(3600));
        let cold_cutoff = crate::utils::cutoff_before(Duration::from_secs(10_000));
        repo.nodes.with_mut(&warm, |e| e.last_active = warm_cutoff);
        repo.nodes.with_mut(&cold, |e| e.last_active = cold_cutoff);

        // warm_threshold = 7200s: only cold should be evicted
        let removed = repo.remove_cold_nodes(7200).await.unwrap();
        assert_eq!(removed, 1, "should remove exactly 1 cold node");
        assert!(repo.contains_sync(hot), "hot node must be kept");
        assert!(repo.contains_sync(warm), "warm node must be kept");
        assert!(!repo.contains_sync(cold), "cold node should be removed");
        assert_eq!(repo.len_sync(), 2);
    }

    #[tokio::test]
    async fn test_remove_cold_nodes_empty_when_nothing_cold() {
        let repo = test_repo();
        let a = addr(9, 1009);
        repo.add_node_sync([9u8; 20], a);
        // new nodes have last_active = now, all hot, should not be removed
        let removed = repo.remove_cold_nodes(7200).await.unwrap();
        assert_eq!(removed, 0);
        assert!(repo.contains_sync(a));
    }

    // ==================== 增量统计原子计数器 ====================

    #[test]
    fn test_stats_after_insert() {
        let repo = test_repo();
        let a = addr(1, 1001);
        repo.add_node_sync([1u8; 20], a);
        // 新节点：state=Good（默认）、query_count=0、score=45.0
        let s = repo.stats_sync();
        assert_eq!(s.total, 1);
        assert_eq!(s.good, 1);
        assert_eq!(s.questionable, 0);
        assert_eq!(s.bad, 0);
        assert_eq!(s.active, 0, "query_count=0 不计入 active");
        assert!((s.avg_score - 45.0).abs() < 1e-9, "avg={}", s.avg_score);
    }

    #[test]
    fn test_stats_empty_repo() {
        let repo = test_repo();
        let s = repo.stats_sync();
        assert_eq!(s.total, 0);
        assert_eq!(s.good, 0);
        assert_eq!(s.active, 0);
        assert_eq!(s.avg_score, 0.0, "total==0 时 avg_score 必须为 0.0");
    }

    #[test]
    fn test_stats_same_addr_id_update_no_double_count() {
        let repo = test_repo();
        let a = addr(2, 1002);
        repo.add_node_sync([1u8; 20], a);
        // 同 addr 第二次插入不同 id → 走 existing 分支，只改 id/last_active，计数不变
        repo.add_node_sync([9u8; 20], a);
        let s = repo.stats_sync();
        assert_eq!(s.total, 1, "同 addr 替换不得翻倍计数");
        assert_eq!(s.good, 1);
        assert!((s.avg_score - 45.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_stats_after_remove_node() {
        let repo = test_repo();
        let a = addr(3, 1003);
        repo.add_node_sync([3u8; 20], a);
        assert_eq!(repo.stats_sync().total, 1);
        let ok = repo.remove_node(&a).await;
        assert!(ok);
        let s = repo.stats_sync();
        assert_eq!(s.total, 0);
        assert_eq!(s.good, 0);
        assert_eq!(s.active, 0);
        assert_eq!(s.avg_score, 0.0);
    }

    #[test]
    fn test_stats_after_emergency_evict() {
        let repo = test_repo();
        let a = addr(4, 1004);
        repo.add_node_sync([4u8; 20], a);
        // emergency_evict 跳过 dirty 节点，先清 dirty 才会真正卸载
        repo.clear_all_dirty_sync();
        repo.emergency_evict(1);
        assert!(!repo.contains_sync(a));
        let s = repo.stats_sync();
        assert_eq!(s.total, 0);
        assert_eq!(s.good, 0);
    }

    #[tokio::test]
    async fn test_set_node_state_transition() {
        let repo = test_repo();
        let a = addr(5, 1005);
        repo.add_node_sync([5u8; 20], a); // 默认 Good
        assert_eq!(repo.stats_sync().good, 1);
        repo.set_node_state(&a, NodeState::Bad).await;
        let s = repo.stats_sync();
        assert_eq!(s.good, 0, "Good→Bad 后 good 应 -1");
        assert_eq!(s.bad, 1, "Good→Bad 后 bad 应 +1");
        assert_eq!(s.questionable, 0);
        // 同状态再设一次：不得重复扣减
        repo.set_node_state(&a, NodeState::Bad).await;
        let s2 = repo.stats_sync();
        assert_eq!(s2.bad, 1, "同状态迁移必须 no-op");
        assert_eq!(s2.good, 0);
    }

    #[test]
    fn test_record_query_active_and_state_flip() {
        let repo = test_repo();
        let a = addr(6, 1006);
        repo.add_node_sync([6u8; 20], a); // Good, qc=0, active=0
                                          // 第 1 次失败：query_count 0→1 → active++；连续失败=1，仍 Good
        repo.record_query_sync(a, false, 0);
        let s1 = repo.stats_sync();
        assert_eq!(s1.active, 1, "首次查询 active 0→1");
        assert_eq!(s1.good, 1, "未满 3 次失败仍 Good");
        // 再失败 2 次：consecutive_failures=3 → Bad
        repo.record_query_sync(a, false, 0);
        repo.record_query_sync(a, false, 0);
        let s2 = repo.stats_sync();
        assert_eq!(s2.bad, 1);
        assert_eq!(s2.good, 0);
        assert_eq!(s2.active, 1, "active 只增不减，不因状态翻转改变");
        // 成功一次：转回 Good
        repo.record_query_sync(a, true, 10);
        let s3 = repo.stats_sync();
        assert_eq!(s3.good, 1);
        assert_eq!(s3.bad, 0);
    }

    #[tokio::test]
    async fn test_score_update_changes_avg() {
        let repo = test_repo();
        let a = addr(7, 1007);
        repo.add_node_sync([7u8; 20], a); // score=45
                                          // 批量更新路径
        repo.update_scores_batch_sync(&[(a, 95.0)]);
        let s = repo.stats_sync();
        assert!((s.avg_score - 95.0).abs() < 1e-9, "avg={}", s.avg_score);
        // 单节点 trait 路径（with_mut 分片锁）
        repo.update_score(&a, 10.0).await;
        let s2 = repo.stats_sync();
        assert!((s2.avg_score - 10.0).abs() < 1e-9, "avg={}", s2.avg_score);
    }

    #[test]
    fn test_mixed_ops_match_full_scan() {
        let repo = test_repo();
        let a1 = addr(8, 1008);
        let a2 = addr(9, 1009);
        let a3 = addr(10, 1010);
        repo.add_node_sync([1u8; 20], a1);
        repo.add_node_sync([2u8; 20], a2);
        repo.add_node_sync([3u8; 20], a3);
        repo.record_query_sync(a1, true, 5); // a1: Good, active, qc=1
        repo.record_query_sync(a2, false, 0);
        repo.record_query_sync(a2, false, 0);
        repo.record_query_sync(a2, false, 0); // a2 → Bad, active, qc=3
        repo.update_scores_batch_sync(&[(a1, 80.0), (a3, 20.0)]);
        // 清 dirty 后驱逐 1 个（最久未访问），计数器必须随驱逐扣减
        repo.clear_all_dirty_sync();
        repo.emergency_evict(1);

        // 手动全量扫描（权威）
        let all = repo.all_nodes_sync();
        let mut good = 0;
        let mut questionable = 0;
        let mut bad = 0;
        let mut active = 0;
        let mut sum = 0.0;
        for n in &all {
            match n.state {
                NodeState::Good => good += 1,
                NodeState::Questionable => questionable += 1,
                NodeState::Bad => bad += 1,
            }
            if n.query_count > 0 {
                active += 1;
            }
            sum += n.score;
        }
        let manual_avg = if all.is_empty() {
            0.0
        } else {
            sum / all.len() as f64
        };

        let s = repo.stats_sync();
        assert_eq!(s.total, all.len());
        assert_eq!(s.good, good);
        assert_eq!(s.questionable, questionable);
        assert_eq!(s.bad, bad);
        assert_eq!(s.active, active);
        assert!(
            (s.avg_score - manual_avg).abs() < 1e-9,
            "avg counted={} manual={}",
            s.avg_score,
            manual_avg
        );
    }

    #[tokio::test]
    async fn test_load_all_recalibrates_and_idempotent() {
        let storage = Arc::new(Storage::memory().unwrap());
        let repo1 = NodeRepoImpl::new(storage.clone());
        let n1 = addr(11, 1011);
        let n2 = addr(12, 1012);
        repo1.add_node_sync([1u8; 20], n1);
        repo1.add_node_sync([2u8; 20], n2);
        // n1 三次失败 → Bad，qc=3；score 改成 30
        repo1.record_query_sync(n1, false, 0);
        repo1.record_query_sync(n1, false, 0);
        repo1.record_query_sync(n1, false, 0);
        repo1.update_scores_batch_sync(&[(n1, 30.0)]);
        repo1.save_dirty().await.unwrap(); // 落库

        // 新 repo 复用同一 storage，load_all 全量加载并校准
        let repo2 = NodeRepoImpl::new(storage);
        let n = repo2.load_all().await.unwrap();
        assert_eq!(n, 2);
        let s = repo2.stats_sync();
        assert_eq!(s.total, 2);
        assert_eq!(s.good, 1, "n2 未查询 → Good");
        assert_eq!(s.bad, 1, "n1 三次失败 → Bad");
        assert_eq!(s.questionable, 0);
        assert_eq!(s.active, 1, "仅 n1 query_count>0");
        assert!((s.avg_score - 37.5).abs() < 1e-9, "avg={}", s.avg_score);

        // 重复加载：校准是"置为扫描值"，total 不得翻倍
        let n2b = repo2.load_all().await.unwrap();
        assert_eq!(n2b, 2);
        let s2 = repo2.stats_sync();
        assert_eq!(s2.total, 2, "重复 load_all 后计数不得翻倍");
        assert_eq!(s2.bad, 1);
        assert_eq!(s2.active, 1);
    }

    #[test]
    fn test_verify_consistency_clean() {
        let repo = test_repo();
        let a = addr(20, 1020);
        repo.add_node_sync([1u8; 20], a);
        repo.record_query_sync(a, true, 5);
        repo.update_scores_batch_sync(&[(a, 88.0)]);
        let d = repo.verify_stats_consistency();
        assert_eq!(d.total, 0);
        assert_eq!(d.good, 0);
        assert_eq!(d.questionable, 0);
        assert_eq!(d.bad, 0);
        assert_eq!(d.active, 0);
        assert!(
            d.avg_score_diff.abs() < 1e-9,
            "avg_diff={}",
            d.avg_score_diff
        );
    }

    #[test]
    fn test_verify_consistency_detects_drift() {
        let repo = test_repo();
        let a = addr(21, 1021);
        repo.add_node_sync([1u8; 20], a); // Good, total=1, good=1
                                          // 人为污染计数器：total 多算 3，bad 多算 1
        repo.total.fetch_add(3, Ordering::Relaxed);
        repo.bad.fetch_add(1, Ordering::Relaxed);
        let d = repo.verify_stats_consistency();
        assert_eq!(d.total, 3, "drift = 计数器 - 实测");
        assert_eq!(d.bad, 1);
        assert_eq!(d.good, 0);
        assert_eq!(d.active, 0);
    }

    #[tokio::test]
    async fn test_refresh_all_states_no_drift() {
        let repo = test_repo();
        let a = addr(22, 1022);
        repo.add_node_sync([1u8; 20], a);
        // refresh_state 对新节点（last_active=now）应保持 Good；
        // 先把节点置 Bad（Bad 被 refresh_state 冻结），再 refresh，计数器不应漂移
        repo.set_node_state(&a, NodeState::Bad).await;
        repo.refresh_all_states_sync();
        let d = repo.verify_stats_consistency();
        assert_eq!(d.total, 0);
        assert_eq!(d.bad, 0);
        assert_eq!(d.good, 0);
    }

    fn dht_row_for_load(port: u16, score: f64, last_active: Option<i64>) -> DhtNodeRow {
        DhtNodeRow {
            id: [port as u8; 20],
            ip: "127.0.0.1".into(),
            port,
            score,
            state: "Good".into(),
            query_count: 2,
            success_count: 2,
            total_latency_ms: 5,
            consecutive_failures: 0,
            nodes_returned: 8,
            last_query_time: None,
            last_active,
        }
    }

    /// 预加载新近度回归（2026-10 R1/R2）：加载必须还原 DB 真实 last_active，
    /// 按其重算 Good/Questionable，且只有 hot 窗口内的节点进热池——
    /// 陈旧高分节点不得以「刚刚活跃」+ Good + 热 的身份进场。
    #[tokio::test]
    async fn test_load_initial_restores_recency_and_state() {
        let repo = test_repo();
        let now = crate::utils::instant_to_unix_secs(Instant::now());
        let month_ago = now - 30 * 24 * 3600;
        let rows = vec![
            dht_row_for_load(3001, 10.0, Some(now)),       // 新近低分
            dht_row_for_load(3002, 95.0, Some(month_ago)), // 陈旧高分（僵尸）
        ];
        repo.storage().save_dht_nodes_batch(&rows).unwrap();

        let loaded = repo.load_initial(100).await.unwrap();
        assert_eq!(loaded, 2);

        let fresh_addr = addr(1, 3001);
        let stale_addr = addr(1, 3002);
        let fresh = repo.get_node(&fresh_addr).await.unwrap();
        let stale = repo.get_node(&stale_addr).await.unwrap();

        // 新近节点：Good 且在热池
        assert_eq!(fresh.state, NodeState::Good);
        assert!(
            fresh.last_active.elapsed().as_secs() < 5,
            "last_active 应从 DB 还原，而非复位为加载时刻"
        );
        assert!(
            repo.hot_nodes_sync().iter().any(|n| n.addr == fresh_addr),
            "hot 窗口内的加载节点应进热池"
        );
        // 陈旧高分节点：被 refresh_state 按 真实 last_active 翻为 Questionable，且不进热池
        assert_eq!(
            stale.state,
            NodeState::Questionable,
            "陈旧节点不得保持 Good（last_active 未复位）"
        );
        assert!(
            !repo.hot_nodes_sync().iter().any(|n| n.addr == stale_addr),
            "陈旧节点不得进热池"
        );
    }

    /// 落库回归（2026-10 R1）：save_dirty 落库的是节点真实 last_active，
    /// 加载侧才能还原新近度。
    #[tokio::test]
    async fn test_save_dirty_persists_real_last_active() {
        let repo = test_repo();
        let a = addr(9, 3009);
        repo.add_node_sync([9u8; 20], a);
        let old = crate::utils::cutoff_before(Duration::from_secs(3600));
        repo.nodes.with_mut(&a, |e| e.last_active = old);
        repo.mark_dirty(&a).await;
        repo.save_dirty().await.unwrap();

        let rows = repo.storage().load_recent_nodes(10).unwrap();
        assert_eq!(rows.len(), 1);
        let la = rows[0].last_active.expect("last_active 必须落库");
        let expected = crate::utils::instant_to_unix_secs(old);
        assert!(
            la <= expected + 2 && la >= expected - 5,
            "落库 last_active({la}) 应为节点真实活跃时间({expected})，而非落库时刻"
        );
    }
}
