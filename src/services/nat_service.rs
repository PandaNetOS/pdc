//! NatService — UPnP 映射业务层

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::nat::{NatManager, NatStatus};

pub struct NatService {
    manager: Arc<RwLock<NatManager>>,
}

impl NatService {
    pub fn new(manager: Arc<RwLock<NatManager>>) -> Self {
        Self { manager }
    }

    pub fn manager(&self) -> &Arc<RwLock<NatManager>> {
        &self.manager
    }

    pub async fn status(&self) -> NatStatus {
        self.manager.read().await.status()
    }
}
