//! NodeRepository 瀹炵幇
//!
//! 鐙珛鐨?DHT 鑺傜偣瀛樺偍锛堟棤瀹归噺闄愬埗锛夛紝浣滀负鐖櫕鍊欓€夋睜鐨勫敮涓€褰掑彛銆?
//! 璺敱琛ㄥ彧璐熻矗 DHT 璺敱鍝嶅簲锛孨odeRepo 璐熻矗鐖櫕鍊欓€夎妭鐐圭殑瀛樺偍鍜岃瘎鍒嗐€?
//! 鍐呭瓨 FxHashMap + SQLite 澧為噺鎸佷箙鍖栥€?
//! 鍗冧竾绾ф€ц兘浼樺寲锛欶xHashMap 鏇夸唬 std::HashMap锛屽閲忔寔涔呭寲鍙繚瀛?dirty 鑺傜偣銆?

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::federation::gossip::GossipEngine;
use crate::federation::merkle::MerkleTree;
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

pub struct NodeRepoImpl {
    /// 鐙珛鑺傜偣瀛樺偍锛堟棤瀹归噺闄愬埗锛屾寜 addr 鍘婚噸锛夆€?FxHashMap 楂樻€ц兘
    nodes: ShardedHashMap<SocketAddr, KBucketEntry>,
    /// 鑴忚妭鐐归泦鍚堬紙缁熻鏁版嵁宸插彉鍖栵紝闇€瑕侀噸绠楄瘎鍒?+ 澧為噺鎸佷箙鍖栵級
    dirty: RwLock<FxHashSet<SocketAddr>>,
    /// /24 缃戞绱㈠紩锛圛Pv4 鍓?3 瀛楄妭 -> 璇ョ綉娈靛唴鑺傜偣 ID 鍒楄〃锛夛紝鐢ㄤ簬 O(1) 鍙栫綉娈?
    /// 浠?IPv4 鑺傜偣鍏ョ储寮曪紱IPv6 鑺傜偣蹇界暐銆傚鍒犺妭鐐规椂鍚屾缁存姢銆?
    subnet_index: RwLock<FxHashMap<[u8; 3], Vec<NodeId>>>,
    /// 鐑妭鐐瑰湴鍧€闆嗗悎锛堟渶杩?hot_threshold_secs 鍐呰璁块棶鐨勮妭鐐癸級
    hot_addrs: RwLock<FxHashSet<SocketAddr>>,
    /// 鍐疯妭鐐瑰湴鍧€闆嗗悎锛堣秴杩?hot_threshold_secs 鏈闂紝鐢卞閮ㄥ畾鏃朵换鍔¤縼绉伙級
    cold_addrs: RwLock<FxHashSet<SocketAddr>>,
    storage: Arc<Storage>,
    /// 鑱旈偊寮曠敤锛圤nceLock 娉ㄥ叆锛涙湭璁剧疆鏃舵湰鍦板啓鍏ヤ笉瑙﹀彂 Merkle/Gossip锛宺epo 姝ｅ父宸ヤ綔锛?
    merkle: OnceLock<Arc<MerkleTree>>,
    gossip: OnceLock<Arc<GossipEngine>>,
    /// 鍐欏叆闃熷垪锛堝彲閫夛紝None 鏃堕€€鍖栦负鍚屾鍐欏叆锛?
    write_queue: Option<Arc<WriteQueue>>,
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
            dirty: RwLock::new(FxHashSet::default()),
            subnet_index: RwLock::new(FxHashMap::default()),
            hot_addrs: RwLock::new(FxHashSet::default()),
            cold_addrs: RwLock::new(FxHashSet::default()),
            storage,
            merkle: OnceLock::new(),
            gossip: OnceLock::new(),
            write_queue: None,
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

    /// 获取底层 Storage 引用（用于联邦同步按分片加载数据）。
    pub fn storage(&self) -> Arc<Storage> {
        self.storage.clone()
    }

    /// 分层缓存统计（与 TrackerRepo 接口一致：(hot, warm, cold_loaded)）。
    /// NodeRepo 全量驻内存，全部计入 hot。
    pub async fn load_initial(&self, limit: usize) -> anyhow::Result<usize> {
        let rows = self.storage.load_hot_warm_nodes(7200, 0.0, limit)?;
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
            if let Some(subnet) = Self::subnet_key(addr) {
                Self::index_subnet(&mut subnet_index, subnet, row.id);
            }
            count += 1;
        }
        // 预热加载的节点全部标记为热节点，供爬虫直接选取
        let mut hot_addrs = self.hot_addrs.write();
        for (_addr, entry) in nodes.iter() {
            hot_addrs.insert(entry.addr);
        }
        drop(hot_addrs);
        Ok(count)
    }

    pub fn cache_stats(&self) -> (usize, usize, u64) {
        (self.len_sync(), 0, 0)
    }

    /// 数据库中 dht_nodes 表的总行数（同步，用于监控面板）
    pub fn total_count_sync(&self) -> u64 {
        self.storage.count_table("dht_nodes").unwrap_or(0)
    }

    /// 执行分层检查 + 驱逐（由 TaskScheduler 定时调用）。
    /// 这三个 repo 全量驻内存不使用 TieredCache，为空操作。
    pub fn tier_evict(&self) {
        // no-op: data is fully in memory, no tiered cache to evict from
    }

    /// 紧急驱逐（内存超限时调用）。空操作。
    pub fn emergency_evict(&self, _count: usize) {
        // no-op: data is fully in memory
    }

    /// 娉ㄥ叆鑱旈偊 Merkle 鏍戜笌 Gossip 寮曟搸寮曠敤锛坢ain.rs 鍦?FederationService 鍒涘缓鍚庤皟鐢級銆?
    /// 鏈皟鐢ㄦ椂锛堝鍗曞厓娴嬭瘯锛夛紝鏈湴鍐欏叆涓嶈Е鍙戜紶鎾紝repo 琛屼负瀹屽叏涓嶅彉銆?
    pub fn set_federation_refs(&self, merkle: Arc<MerkleTree>, gossip: Arc<GossipEngine>) {
        let _ = self.merkle.set(merkle);
        let _ = self.gossip.set(gossip);
    }

    /// 灏嗘湰鍦版柊鍐欏叆鐨勬潯鐩壒閲忔洿鏂?Merkle 骞舵彁浜?Gossip锛堝啓閿佸鎵ц锛岀函鍐呭瓨鎿嶄綔锛夈€?
    /// merkle/gossip 鏈敞鍏ユ椂鐩存帴璺宠繃锛屼笉 panic銆?
    #[inline]
    fn propagate(&self, rt: u8, built: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>) {
        if built.is_empty() {
            return;
        }
        let Some(merkle) = self.merkle.get() else {
            return;
        };
        let Some(gossip) = self.gossip.get() else {
            return;
        };
        let refs: Vec<(&[u8], &[u8], &[u8])> = built
            .iter()
            .map(|(k, p, h)| (k.as_slice(), p.as_slice(), h.as_slice()))
            .collect();
        merkle.update_batch(&refs);
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
        gossip.submit_gossip(rt, entries);
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
                existing.last_active = Instant::now();
            } else {
                let mut entry = KBucketEntry::new(*id, *addr);
                // 鏂拌妭鐐瑰垵濮嬭瘎鍒?45.0锛堜腑鎬у垎锛夛紝鍚庣画鐢?ScoreMaintainer 缁熶竴鏇存柊
                entry.score = 45.0;
                nodes.insert(*addr, entry);
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
        if !new_pairs.is_empty() {
            let mut dirty = self.dirty.write();
            for (_, addr) in &new_pairs {
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

    /// 鑺傜偣缁熻淇℃伅锛堥伩鍏嶅叏閲忓厠闅嗭紝鐢ㄤ簬鍋ュ悍搴﹁绠楀拰鐩戞帶锛?
    pub fn stats_sync(&self) -> NodeStats {
        let nodes = self.nodes.read_all();
        let mut good = 0;
        let mut questionable = 0;
        let mut bad = 0;
        let mut active = 0;
        let mut avg_score = 0.0;
        for node in nodes.values() {
            match node.state {
                NodeState::Good => good += 1,
                NodeState::Questionable => questionable += 1,
                NodeState::Bad => bad += 1,
            }
            if node.query_count > 0 {
                active += 1;
            }
            avg_score += node.score;
        }
        let total = nodes.len();
        if total > 0 {
            avg_score /= total as f64;
        }
        NodeStats {
            total,
            good,
            questionable,
            bad,
            active,
            avg_score,
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
        if let Some(entry) = nodes.get_mut(&addr) {
            entry.query_count += 1;
            entry.last_query_time = Some(Instant::now());
            if success {
                entry.success_count += 1;
                entry.total_latency_ms += latency_ms;
                entry.last_active = Instant::now();
                entry.state = NodeState::Good;
                entry.consecutive_failures = 0;
            } else {
                entry.consecutive_failures += 1;
                if entry.consecutive_failures >= 3 {
                    entry.state = NodeState::Bad;
                }
            }
            // 鏍囪涓鸿剰锛氱粺璁℃暟鎹凡鍙樺寲锛岄渶瑕侀噸绠楄瘎鍒?+ 澧為噺鎸佷箙鍖?
            drop(nodes);
            self.dirty.write().insert(addr);
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
            entry.query_count += 1;
            entry.success_count += 1;
            entry.total_latency_ms += latency_ms;
            entry.nodes_returned += nodes_returned;
            entry.last_active = Instant::now();
            entry.last_query_time = Some(Instant::now());
            entry.state = NodeState::Good;
            entry.consecutive_failures = 0;
            // 鏍囪涓鸿剰
            drop(nodes);
            self.dirty.write().insert(addr);
        }
    }

    /// 鍒锋柊鎵€鏈夎妭鐐圭姸鎬侊紙鍩轰簬鏈€鍚庢椿璺冩椂闂存洿鏂?Good/Questionable锛?
    pub fn refresh_all_states_sync(&self) {
        let mut nodes = self.nodes.write_all();
        nodes.for_values_mut(|entry| {
            entry.refresh_state();
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
                entry.score = *score;
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
        // 联动 Merkle：标记该 key 所在分片为 dirty（墓碑）
        let key = addr.to_string().into_bytes();
        if let Some(merkle) = self.merkle.get() {
            merkle.mark_tombstone(&key);
        }
        // 联邦删除传播：提交 DELETE 条目，避免对端 upsert-only 合并导致"删除复活"
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
        self.nodes.with_mut(addr, |entry| entry.score = score);
    }

    async fn update_scores_batch(&self, scores: &[(SocketAddr, f64)]) {
        self.update_scores_batch_sync(scores);
    }

    async fn record_query(&self, addr: &SocketAddr, success: bool, latency_ms: u64) {
        self.record_query_sync(*addr, success, latency_ms);
    }

    async fn set_node_state(&self, addr: &SocketAddr, state: NodeState) {
        self.nodes.with_mut(addr, |entry| entry.state = state);
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
            let dirty_addrs = self.take_dirty_sync();
            if dirty_addrs.is_empty() {
                return Ok(());
            }
            let batch = self.build_dirty_batch(&dirty_addrs);
            if batch.is_empty() {
                return Ok(());
            }
            let wq = wq.clone();
            let count = batch.len();
            wq.send(move |conn| Storage::save_dht_nodes_batch_in_tx(conn, &batch));
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
            if nodes.remove(addr).is_some() {
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
            if nodes.remove(addr).is_some() {
                removed += 1;
                if let Some(subnet) = Self::subnet_key(*addr) {
                    if let Some(bucket) = subnet_index.get_mut(&subnet) {
                        bucket.retain(|x| !nodes.contains_key(addr));
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
}
