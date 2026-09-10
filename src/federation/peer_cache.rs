//! 联邦节点缓存持久化
//!
//! 将已连接过的 PDC 联邦节点地址持久化到磁盘，重启后自动尝试重连，
//! 实现"默认启动自动连接其他 PDC"的零配置体验。
//!
//! 缓存文件路径：`{data_dir}/federation_peers.json`

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// 缓存的节点信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedNode {
    /// 节点 ID（20 字节）
    pub node_id: [u8; 20],
    /// 节点地址（"ip:port" 字符串）
    pub addr: String,
    /// 最后一次活跃时间（Unix 时间戳，秒）
    pub last_seen: u64,
    /// 连接成功次数
    pub success_count: u32,
}

/// 节点缓存
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PeerCache {
    /// 最后更新时间（Unix 时间戳，秒）
    pub last_updated: u64,
    /// 缓存的节点列表
    pub nodes: Vec<CachedNode>,
}

impl PeerCache {
    /// 缓存文件名
    const CACHE_FILE: &'static str = "federation_peers.json";

    /// 从文件加载缓存，文件不存在或解析失败返回空
    pub fn load(data_dir: &Path) -> Self {
        let path = data_dir.join(Self::CACHE_FILE);
        match std::fs::read_to_string(&path) {
            Ok(content) => match serde_json::from_str::<PeerCache>(&content) {
                Ok(cache) => {
                    tracing::debug!(
                        "[federation] 节点缓存加载成功，共 {} 个节点",
                        cache.nodes.len()
                    );
                    cache
                }
                Err(e) => {
                    tracing::warn!("[federation] 节点缓存解析失败，使用空缓存: {}", e);
                    Self::default()
                }
            },
            Err(_) => {
                tracing::debug!("[federation] 节点缓存文件不存在，使用空缓存");
                Self::default()
            }
        }
    }

    /// 原子写入文件（先写 .tmp 再 rename，防止崩溃导致文件损坏）
    pub fn save(&self, data_dir: &Path) -> anyhow::Result<()> {
        let path = data_dir.join(Self::CACHE_FILE);
        let tmp_path = path.with_extension("json.tmp");

        // 确保目录存在
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&tmp_path, json)?;
        std::fs::rename(&tmp_path, &path)?;

        tracing::debug!(
            "[federation] 节点缓存已保存，共 {} 个节点",
            self.nodes.len()
        );
        Ok(())
    }

    /// 添加或更新节点
    ///
    /// - 节点已存在：更新地址和 last_seen，success 时递增 success_count
    /// - 节点不存在：新增条目
    pub fn upsert(&mut self, node_id: &[u8; 20], addr: &str, success: bool) {
        let now = current_unix_secs();

        if let Some(node) = self.nodes.iter_mut().find(|n| &n.node_id == node_id) {
            node.addr = addr.to_string();
            node.last_seen = now;
            if success {
                node.success_count = node.success_count.saturating_add(1);
            }
        } else {
            self.nodes.push(CachedNode {
                node_id: *node_id,
                addr: addr.to_string(),
                last_seen: now,
                success_count: if success { 1 } else { 0 },
            });
        }
        self.last_updated = now;
    }

    /// 清理过期节点
    ///
    /// 只保留最近 7 天内有过成功连接（success_count > 0）的节点，
    /// 按 success_count 降序排列后最多保留 max_nodes 个。
    pub fn prune(&mut self, max_nodes: usize) {
        let now = current_unix_secs();
        let seven_days = 7 * 24 * 3600;

        // 过滤：7 天内活跃 且 至少成功连接过一次
        self.nodes
            .retain(|n| now.saturating_sub(n.last_seen) < seven_days && n.success_count > 0);

        // 按 success_count 降序，截断到 max_nodes
        self.nodes.sort_by(|a, b| b.success_count.cmp(&a.success_count));
        self.nodes.truncate(max_nodes);
    }

    /// 按 success_count 降序返回前 n 个节点的地址字符串
    pub fn top_addrs(&self, n: usize) -> Vec<String> {
        let mut sorted: Vec<&CachedNode> = self.nodes.iter().collect();
        sorted.sort_by(|a, b| b.success_count.cmp(&a.success_count));
        sorted.into_iter().take(n).map(|n| n.addr.clone()).collect()
    }
}

/// 获取当前 Unix 时间戳（秒）
fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upsert_new_node() {
        let mut cache = PeerCache::default();
        let id = [1u8; 20];
        cache.upsert(&id, "1.2.3.4:6885", true);
        assert_eq!(cache.nodes.len(), 1);
        assert_eq!(cache.nodes[0].success_count, 1);
        assert_eq!(cache.nodes[0].addr, "1.2.3.4:6885");
    }

    #[test]
    fn test_upsert_existing_node() {
        let mut cache = PeerCache::default();
        let id = [1u8; 20];
        cache.upsert(&id, "1.2.3.4:6885", true);
        cache.upsert(&id, "1.2.3.4:6886", true);
        assert_eq!(cache.nodes.len(), 1);
        assert_eq!(cache.nodes[0].success_count, 2);
        assert_eq!(cache.nodes[0].addr, "1.2.3.4:6886");
    }

    #[test]
    fn test_upsert_failure_not_increment() {
        let mut cache = PeerCache::default();
        let id = [1u8; 20];
        cache.upsert(&id, "1.2.3.4:6885", false);
        assert_eq!(cache.nodes[0].success_count, 0);
    }

    #[test]
    fn test_top_addrs() {
        let mut cache = PeerCache::default();
        for i in 0..5u8 {
            let id = [i; 20];
            for _ in 0..(i + 1) as u32 {
                cache.upsert(&id, &format!("10.0.0.{}:6885", i), true);
            }
        }
        let top = cache.top_addrs(3);
        assert_eq!(top.len(), 3);
        // success_count 最高的是 id=4（5次），应该排第一
        assert_eq!(top[0], "10.0.0.4:6885");
    }

    #[test]
    fn test_prune_removes_old_and_zero_success() {
        let mut cache = PeerCache::default();
        // 成功连接的节点
        let good_id = [1u8; 20];
        cache.upsert(&good_id, "1.1.1.1:6885", true);
        // 从未成功的节点
        let zero_id = [2u8; 20];
        cache.upsert(&zero_id, "2.2.2.2:6885", false);
        // 手动设置一个 8 天前的节点
        let old_id = [3u8; 20];
        cache.upsert(&old_id, "3.3.3.3:6885", true);
        cache.nodes[2].last_seen = 0; // 模拟很久以前

        cache.prune(100);
        // 只剩 good_id（zero_id 被过滤，old_id 超过7天被过滤）
        assert_eq!(cache.nodes.len(), 1);
        assert_eq!(cache.nodes[0].node_id, good_id);
    }

    #[test]
    fn test_prune_truncate() {
        let mut cache = PeerCache::default();
        for i in 0..20u8 {
            let id = [i; 20];
            cache.upsert(&id, &format!("10.0.0.{}:6885", i), true);
        }
        cache.prune(10);
        assert_eq!(cache.nodes.len(), 10);
    }

    #[test]
    fn test_save_and_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("pdc_peer_cache_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut cache = PeerCache::default();
        let id = [0xAB; 20];
        cache.upsert(&id, "192.168.1.1:6885", true);
        cache.upsert(&id, "192.168.1.1:6885", true);

        cache.save(&dir).unwrap();

        let loaded = PeerCache::load(&dir);
        assert_eq!(loaded.nodes.len(), 1);
        assert_eq!(loaded.nodes[0].node_id, id);
        assert_eq!(loaded.nodes[0].success_count, 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_nonexistent_returns_empty() {
        let dir = std::env::temp_dir().join(format!("pdc_peer_cache_nonexist_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let cache = PeerCache::load(&dir);
        assert!(cache.nodes.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
