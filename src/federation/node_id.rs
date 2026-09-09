//! 节点身份与地址管理
//!
//! 阶段2升级：使用 Ed25519 密钥对替代32字节随机数 identity_key。
//! SigningKey 内部是32字节种子，可 to_bytes()/from_bytes() 持久化。

use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::Path;

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use parking_lot::RwLock;
use rand::RngCore;
use serde::{Deserialize, Serialize};

/// 20 字节节点 ID（与 DHT NodeId 兼容）
#[derive(Clone, Copy)]
pub struct NodeId(pub [u8; 20]);

impl NodeId {
    pub fn random() -> Self {
        let mut id = [0u8; 20];
        rand::thread_rng().fill_bytes(&mut id);
        Self(id)
    }

    pub fn from_hex(s: &str) -> anyhow::Result<Self> {
        if s.len() != 40 {
            anyhow::bail!("节点 ID 必须是 40 字符十六进制字符串，实际长度: {}", s.len());
        }
        let mut id = [0u8; 20];
        for i in 0..20 {
            id[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                .map_err(|e| anyhow::anyhow!("十六进制解析失败: {}", e))?;
        }
        Ok(Self(id))
    }

    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(40);
        for b in &self.0 {
            s.push_str(&format!("{:02x}", b));
        }
        s
    }

    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.to_hex())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.to_hex()[..8])
    }
}

impl PartialEq for NodeId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for NodeId {}

impl Hash for NodeId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Serialize for NodeId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        if bytes.len() != 20 {
            return Err(serde::de::Error::custom(format!("NodeId 需要 20 字节，实际 {}", bytes.len())));
        }
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&bytes);
        Ok(NodeId(arr))
    }
}

/// 节点可达性等级
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Reachability {
    PublicIpv6,
    Mapped,
    HolePunchable,
    OutboundOnly,
    Unknown,
}

impl fmt::Display for Reachability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Reachability::PublicIpv6 => write!(f, "PublicIpv6"),
            Reachability::Mapped => write!(f, "Mapped"),
            Reachability::HolePunchable => write!(f, "HolePunchable"),
            Reachability::OutboundOnly => write!(f, "OutboundOnly"),
            Reachability::Unknown => write!(f, "Unknown"),
        }
    }
}

/// 节点地址信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAddress {
    pub node_id: [u8; 20],
    pub ipv4_addr: Option<SocketAddr>,
    pub ipv6_addr: Option<SocketAddr>,
    pub reachability: Reachability,
    pub last_seen: u64,
    pub nat_type: Option<String>,
}

impl NodeAddress {
    pub fn preferred_addr(&self) -> Option<SocketAddr> {
        self.ipv4_addr.or(self.ipv6_addr)
    }
}

/// 节点身份（Ed25519 密钥对）
pub struct NodeIdentity {
    pub node_id: NodeId,
    /// Ed25519 签名密钥（32字节种子）
    pub signing_key: SigningKey,
    /// 已知地址列表
    pub addresses: RwLock<Vec<NodeAddress>>,
}

impl NodeIdentity {
    /// 生成新的随机身份（Ed25519 密钥对）
    pub fn generate() -> Self {
        let node_id = NodeId::random();
        let mut rng = rand::thread_rng();
        let signing_key = SigningKey::generate(&mut rng);
        Self {
            node_id,
            signing_key,
            addresses: RwLock::new(Vec::new()),
        }
    }

    /// 返回公钥
    pub fn public_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    /// 返回公钥字节（32字节）
    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }

    /// 签名数据
    pub fn sign(&self, data: &[u8]) -> Signature {
        self.signing_key.sign(data)
    }

    /// 静态验证方法
    pub fn verify(public_key: &VerifyingKey, data: &[u8], signature: &Signature) -> bool {
        public_key.verify(data, signature).is_ok()
    }

    /// 从公钥字节构造 VerifyingKey
    pub fn verifying_key_from_bytes(bytes: &[u8; 32]) -> anyhow::Result<VerifyingKey> {
        VerifyingKey::from_bytes(bytes)
            .map_err(|e| anyhow::anyhow!("无效公钥: {}", e))
    }

    /// 从数据目录加载或创建身份
    ///
    /// 持久化格式：20字节node_id + 32字节signing_key种子（共52字节）
    /// 向后兼容：阶段1的 identity.bin（同样52字节）直接当作 signing_key 种子加载
    pub fn load_or_create(data_dir: &Path) -> anyhow::Result<Self> {
        let federation_dir = data_dir.join("federation");
        std::fs::create_dir_all(&federation_dir)?;
        let identity_path = federation_dir.join("identity.bin");

        if identity_path.exists() {
            let data = std::fs::read(&identity_path)?;
            if data.len() >= 52 {
                let mut node_id_bytes = [0u8; 20];
                node_id_bytes.copy_from_slice(&data[0..20]);
                let mut seed = [0u8; 32];
                seed.copy_from_slice(&data[20..52]);
                let signing_key = SigningKey::from_bytes(&seed);
                tracing::info!(
                    "[federation] 已加载节点身份: {}",
                    NodeId(node_id_bytes).to_hex()
                );
                return Ok(Self {
                    node_id: NodeId(node_id_bytes),
                    signing_key,
                    addresses: RwLock::new(Vec::new()),
                });
            }
            tracing::warn!("[federation] 身份文件损坏，重新生成");
        }

        let identity = Self::generate();
        let mut data = Vec::with_capacity(52);
        data.extend_from_slice(&identity.node_id.0);
        data.extend_from_slice(&identity.signing_key.to_bytes());
        std::fs::write(&identity_path, &data)?;
        tracing::info!(
            "[federation] 已生成新节点身份: {}",
            identity.node_id.to_hex()
        );
        Ok(identity)
    }

    pub fn update_addresses(&self, addresses: Vec<NodeAddress>) {
        let mut current = self.addresses.write();
        *current = addresses;
    }

    pub fn addresses_snapshot(&self) -> Vec<NodeAddress> {
        self.addresses.read().clone()
    }
}

impl fmt::Debug for NodeIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeIdentity")
            .field("node_id", &self.node_id)
            .field("public_key", &hex::encode(self.public_key_bytes()))
            .field("addresses_count", &self.addresses.read().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_random_and_hex() {
        let id1 = NodeId::random();
        let id2 = NodeId::random();
        assert_ne!(id1, id2);
        let hex = id1.to_hex();
        assert_eq!(hex.len(), 40);
        let parsed = NodeId::from_hex(&hex).unwrap();
        assert_eq!(id1, parsed);
    }

    #[test]
    fn test_node_id_from_hex_invalid() {
        assert!(NodeId::from_hex("too short").is_err());
        assert!(NodeId::from_hex("zz").is_err());
    }

    #[test]
    fn test_node_id_display() {
        let id = NodeId([0xab; 20]);
        assert_eq!(format!("{}", id), "abababab");
    }

    #[test]
    fn test_node_id_serialize_roundtrip() {
        let id = NodeId::random();
        let bytes = bincode::serialize(&id).unwrap();
        let decoded: NodeId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn test_reachability_display() {
        assert_eq!(Reachability::Mapped.to_string(), "Mapped");
        assert_eq!(Reachability::Unknown.to_string(), "Unknown");
    }

    #[test]
    fn test_node_address_preferred() {
        let addr = NodeAddress {
            node_id: [0u8; 20],
            ipv4_addr: Some("127.0.0.1:6885".parse().unwrap()),
            ipv6_addr: None,
            reachability: Reachability::Mapped,
            last_seen: 0,
            nat_type: None,
        };
        assert_eq!(addr.preferred_addr(), Some("127.0.0.1:6885".parse().unwrap()));
    }

    #[test]
    fn test_ed25519_sign_verify() {
        let identity = NodeIdentity::generate();
        let data = b"hello federation";
        let sig = identity.sign(data);
        let pk = identity.public_key();
        assert!(NodeIdentity::verify(&pk, data, &sig));
        // 错误数据验证失败
        assert!(!NodeIdentity::verify(&pk, b"wrong data", &sig));
    }

    #[test]
    fn test_public_key_bytes() {
        let identity = NodeIdentity::generate();
        let pk_bytes = identity.public_key_bytes();
        assert_eq!(pk_bytes.len(), 32);
        let pk = NodeIdentity::verifying_key_from_bytes(&pk_bytes).unwrap();
        assert_eq!(pk, identity.public_key());
    }

    #[test]
    fn test_signing_key_persistence() {
        let identity = NodeIdentity::generate();
        let seed = identity.signing_key.to_bytes();
        assert_eq!(seed.len(), 32);
        let restored = SigningKey::from_bytes(&seed);
        assert_eq!(restored.verifying_key(), identity.public_key());
    }

    #[test]
    fn test_node_identity_generate() {
        let identity = NodeIdentity::generate();
        assert!(identity.addresses.read().is_empty());
        // signing_key 不应为零
        assert_ne!(identity.signing_key.to_bytes(), [0u8; 32]);
    }

    #[test]
    fn test_node_identity_load_or_create() {
        let dir = std::env::temp_dir().join(format!("pdc_fed_test2_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let id1 = NodeIdentity::load_or_create(&dir).unwrap();
        let hex1 = id1.node_id.to_hex();
        let pk1 = id1.public_key_bytes();

        let id2 = NodeIdentity::load_or_create(&dir).unwrap();
        assert_eq!(id1.node_id, id2.node_id);
        assert_eq!(hex1, id2.node_id.to_hex());
        assert_eq!(pk1, id2.public_key_bytes());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_node_identity_addresses() {
        let identity = NodeIdentity::generate();
        assert!(identity.addresses_snapshot().is_empty());
        let addr = NodeAddress {
            node_id: identity.node_id.0,
            ipv4_addr: Some("1.2.3.4:6885".parse().unwrap()),
            ipv6_addr: None,
            reachability: Reachability::Mapped,
            last_seen: 100,
            nat_type: Some("Cone".to_string()),
        };
        identity.update_addresses(vec![addr.clone()]);
        assert_eq!(identity.addresses_snapshot().len(), 1);
        assert_eq!(identity.addresses_snapshot()[0].last_seen, 100);
    }
}
