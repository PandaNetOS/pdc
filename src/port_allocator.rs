//! 端口组自动探测模块
//!
//! 节点启动时自动探测一组可用端口，避免手动配置端口冲突。
//! 从基础端口出发，按 10 个百位段整组平移，逐段尝试 bind：
//! - 段内所有端口 bind 成功 → 采用该组，socket 保持绑定（避免竞态）
//! - 任意端口 bind 失败 → 释放本组已绑定的 socket，尝试下一个百位段
//! - 超过 `max_attempts` 组仍失败 → 返回错误
//!
//! 注意：LPD 多播端口 6771 固定，不参与偏移探测。

use std::net::{TcpListener, UdpSocket};

use anyhow::{anyhow, Context, Result};

/// 爬虫 UDP socket 数量上限
const MAX_CRAWLER_SOCKETS: usize = 10;

/// 10 个百位段基准（xx80），从 6880 所在段开始循环尝试。
/// 实际端口 = 整组平移，使 `base_ports.api_port` 落到该段基准（保持组内相对偏移）。
const OFFSET_GROUPS: [u16; 10] = [6880, 6980, 6080, 6180, 6280, 6380, 6480, 6580, 6680, 6780];

/// 根据爬虫主端口与 socket 数量生成多 socket 端口列表。
///
/// 第一个为主端口（6x82），后续在同百位段内按间隔 10 补位（6x02,6x12,...,6x92，跳过 82）。
fn build_crawler_socket_ports(crawler_port: u16, socket_count: u16) -> Vec<u16> {
    let count = (socket_count as usize).clamp(1, MAX_CRAWLER_SOCKETS);
    let block_base = crawler_port - (crawler_port % 100);
    let mut ports: Vec<u16> = Vec::with_capacity(count);
    // 主端口优先；随后同百位段内 02,12,...,92。
    // 去重：若 crawler_port 末两位本身落在该序列（如 6812），否则会与主端口重复，
    // 导致整组 bind 因 EADDRINUSE 失败、端口自动分配彻底不可用。
    let tens: [u16; 9] = [2, 12, 22, 32, 42, 52, 62, 72, 92];
    let candidates =
        std::iter::once(crawler_port).chain(tens.iter().map(|off| block_base.saturating_add(*off)));
    for cand in candidates {
        if ports.len() >= count {
            break;
        }
        if !ports.contains(&cand) {
            ports.push(cand);
        }
    }
    ports
}

/// 一组端口（基础值或实际分配值）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortGroup {
    /// API/HTTP 监控端口（TCP），对应 `config.server.port`
    pub api_port: u16,
    /// API 监控端口（TCP），对应 `config.server.api_port`
    pub api_monitor_port: u16,
    /// 中继服务器端口（TCP），对应 `config.super_tracker.relay_port`
    pub relay_port: u16,
    /// DHT 发现器端口（UDP），对应 `config.discoverers.dht_listen_port`
    pub dht_listen_port: u16,
    /// DHT 爬虫端口（UDP），对应 `config.crawler.listen_port`
    pub crawler_port: u16,
    /// uTP 服务端端口（UDP），对应 `config.crawler.utp_port`
    pub utp_port: u16,
    /// TCP-PEX 服务端端口（TCP），对应 `config.crawler.tcp_pex_port`
    pub tcp_pex_port: u16,
    /// 联邦监听端口（TCP+UDP），对应 `config.federation.listen_port`
    pub federation_port: u16,
    /// 爬虫多 UDP socket 端口列表（主端口排第一，后续同百位段间隔 10）
    pub crawler_socket_ports: Vec<u16>,
}

/// 端口分配结果（包含实际端口和已绑定的 socket）
///
/// 所有 socket 在分配成功后保持绑定状态，直到 [`PortAllocation::release_all`]
/// 或本结构被 drop。调用方可通过对应字段提取 socket 交给真实服务使用。
pub struct PortAllocation {
    /// 实际分配到的端口组
    pub ports: PortGroup,
    /// 实际命中的百位段下标（对应 [`OFFSET_GROUPS`] 索引）
    pub offset: u16,
    /// API/HTTP 监控（TCP）
    pub api_listener: Option<TcpListener>,
    /// API 监控端口（TCP），对应 `config.server.api_port`
    pub api_monitor_listener: Option<TcpListener>,
    /// 中继服务器（TCP）
    pub relay_listener: Option<TcpListener>,
    /// DHT 发现器（UDP）
    pub dht_socket: Option<UdpSocket>,
    /// DHT 爬虫多 socket（UDP，与 `ports.crawler_socket_ports` 一一对应）
    pub crawler_sockets: Vec<Option<UdpSocket>>,
    /// uTP 服务端（UDP）
    pub utp_socket: Option<UdpSocket>,
    /// TCP-PEX 服务端（TCP）
    pub tcp_pex_listener: Option<TcpListener>,
    /// 联邦监听（TCP）
    pub federation_tcp: Option<TcpListener>,
    /// 联邦监听（UDP）
    pub federation_udp: Option<UdpSocket>,
}

impl PortAllocation {
    /// 将实际端口应用到 [`crate::config::PdcConfig`]（替换原配置中的端口值）
    pub fn apply_to_config(&self, config: &mut crate::config::PdcConfig) {
        config.server.port = self.ports.api_port;
        config.server.api_port = self.ports.api_monitor_port;
        config.super_tracker.relay_port = self.ports.relay_port;
        config.discoverers.dht_listen_port = self.ports.dht_listen_port;
        config.crawler.listen_port = self.ports.crawler_port;
        config.crawler.utp_port = self.ports.utp_port;
        config.crawler.tcp_pex_port = self.ports.tcp_pex_port;
        config.federation.listen_port = self.ports.federation_port;
        config.federation.api_port = self.ports.api_port;
    }

    /// 释放所有已绑定的 socket（端口随之释放，可被其他进程重新绑定）
    pub fn release_all(&mut self) {
        self.api_listener.take();
        self.api_monitor_listener.take();
        self.relay_listener.take();
        self.dht_socket.take();
        self.crawler_sockets.clear();
        self.utp_socket.take();
        self.tcp_pex_listener.take();
        self.federation_tcp.take();
        self.federation_udp.take();
    }
}

/// 端口分配器
pub struct PortAllocator {
    /// 基础端口组（offset=0 时的端口值）
    base_ports: PortGroup,
    /// 端口组之间的步长
    step: u16,
    /// 最大尝试组数
    max_attempts: u32,
    /// 绑定地址（如 "0.0.0.0" / "127.0.0.1"）
    bind_addr: String,
}

impl PortAllocator {
    /// 创建分配器（步长 `step`，默认最大尝试 100 组）
    pub fn new(base_ports: PortGroup, step: u16) -> Self {
        Self {
            base_ports,
            step,
            max_attempts: 100,
            bind_addr: "0.0.0.0".to_string(),
        }
    }

    /// 设置最大尝试组数（builder）
    pub fn with_max_attempts(mut self, max: u32) -> Self {
        self.max_attempts = max;
        self
    }

    /// 设置绑定地址（builder，默认 "0.0.0.0"）
    pub fn with_bind_addr(mut self, addr: impl Into<String>) -> Self {
        self.bind_addr = addr.into();
        self
    }

    /// 从 [`crate::config::PdcConfig`] 提取基础端口创建分配器
    pub fn from_config(config: &crate::config::PdcConfig) -> Self {
        let crawler_port = config.crawler.listen_port;
        let socket_count = config
            .crawler
            .socket_count
            .clamp(1, MAX_CRAWLER_SOCKETS as u16);
        let crawler_socket_ports = build_crawler_socket_ports(crawler_port, socket_count);
        let base_ports = PortGroup {
            api_port: config.server.port,
            api_monitor_port: config.server.api_port,
            relay_port: config.super_tracker.relay_port,
            dht_listen_port: config.discoverers.dht_listen_port,
            crawler_port,
            utp_port: config.crawler.utp_port,
            tcp_pex_port: config.crawler.tcp_pex_port,
            federation_port: config.federation.listen_port,
            crawler_socket_ports,
        };
        Self::new(base_ports, config.port_step)
    }

    /// 执行探测，返回第一个可用的端口组分配。
    ///
    /// 按 [`OFFSET_GROUPS`] 的百位段顺序整组平移探测：组内全部 bind 成功即返回；
    /// 任一失败则释放本组已绑定的 socket，尝试下一个百位段。
    pub fn allocate(&self) -> Result<PortAllocation> {
        let attempts = (self.max_attempts as usize).min(OFFSET_GROUPS.len());
        for (idx, &group_base) in OFFSET_GROUPS.iter().take(attempts).enumerate() {
            let Some(ports) = self.ports_for_group(group_base) else {
                tracing::debug!(
                    "[port_allocator] group={} 端口超出 u16 范围，跳过",
                    group_base
                );
                continue;
            };

            match self.try_bind_group(&ports) {
                Ok(mut allocation) => {
                    allocation.offset = idx as u16;
                    tracing::info!(
                        "[port_allocator] 端口组分配成功: group={} (idx={}), ports={:?}",
                        group_base,
                        idx,
                        ports
                    );
                    return Ok(allocation);
                }
                Err(e) => {
                    tracing::debug!(
                        "[port_allocator] group={} 端口组不可用，尝试下一组: {}",
                        group_base,
                        e
                    );
                }
            }
        }

        Err(anyhow!(
            "端口组探测失败：已尝试 {} 组（基础端口={:?}, step={}），均无可用端口",
            attempts,
            self.base_ports,
            self.step
        ))
    }

    /// 计算某个百位段组下的端口组；任何端口溢出 u16 返回 None。
    ///
    /// 整组平移：`shift = group_base - base_ports.api_port`，保持组内相对偏移。
    fn ports_for_group(&self, group_base: u16) -> Option<PortGroup> {
        let shift = group_base as i32 - self.base_ports.api_port as i32;
        let shift_one = |base: u16| -> Option<u16> {
            let p = base as i32 + shift;
            if !(0..=u16::MAX as i32).contains(&p) {
                return None;
            }
            Some(p as u16)
        };
        Some(PortGroup {
            api_port: shift_one(self.base_ports.api_port)?,
            api_monitor_port: shift_one(self.base_ports.api_monitor_port)?,
            relay_port: shift_one(self.base_ports.relay_port)?,
            dht_listen_port: shift_one(self.base_ports.dht_listen_port)?,
            crawler_port: shift_one(self.base_ports.crawler_port)?,
            utp_port: shift_one(self.base_ports.utp_port)?,
            tcp_pex_port: shift_one(self.base_ports.tcp_pex_port)?,
            federation_port: shift_one(self.base_ports.federation_port)?,
            crawler_socket_ports: self
                .base_ports
                .crawler_socket_ports
                .iter()
                .map(|&p| shift_one(p))
                .collect::<Option<Vec<_>>>()?,
        })
    }

    /// 尝试绑定一整组端口。任一失败则返回 Err，且本组已绑定的 socket 自动 drop 释放。
    fn try_bind_group(&self, ports: &PortGroup) -> Result<PortAllocation> {
        let api_listener = self
            .bind_tcp(ports.api_port)
            .with_context(|| format!("API/HTTP 监控端口 {} (TCP) 绑定失败", ports.api_port))?;
        let api_monitor_listener = self
            .bind_tcp(ports.api_monitor_port)
            .with_context(|| format!("API 监控端口 {} (TCP) 绑定失败", ports.api_monitor_port))?;
        let relay_listener = self
            .bind_tcp(ports.relay_port)
            .with_context(|| format!("中继端口 {} (TCP) 绑定失败", ports.relay_port))?;
        let dht_socket = self
            .bind_udp(ports.dht_listen_port)
            .with_context(|| format!("DHT 发现端口 {} (UDP) 绑定失败", ports.dht_listen_port))?;
        // 爬虫多 UDP socket：主端口 + 辅助端口（任一失败即整组回退）
        let mut crawler_sockets = Vec::with_capacity(ports.crawler_socket_ports.len());
        for &p in &ports.crawler_socket_ports {
            let s = self
                .bind_udp(p)
                .with_context(|| format!("DHT 爬虫 socket 端口 {} (UDP) 绑定失败", p))?;
            crawler_sockets.push(Some(s));
        }
        let utp_socket = self
            .bind_udp(ports.utp_port)
            .with_context(|| format!("uTP 端口 {} (UDP) 绑定失败", ports.utp_port))?;
        let tcp_pex_listener = self
            .bind_tcp(ports.tcp_pex_port)
            .with_context(|| format!("TCP-PEX 端口 {} (TCP) 绑定失败", ports.tcp_pex_port))?;
        let federation_tcp = self
            .bind_tcp(ports.federation_port)
            .with_context(|| format!("联邦端口 {} (TCP) 绑定失败", ports.federation_port))?;
        let federation_udp = self
            .bind_udp(ports.federation_port)
            .with_context(|| format!("联邦端口 {} (UDP) 绑定失败", ports.federation_port))?;

        // 注意：relay(TCP) 与 dht(UDP) 可能共用同一端口号，TCP/UDP 协议栈独立，可共存。
        Ok(PortAllocation {
            ports: ports.clone(),
            offset: 0, // 调用方（allocate）会按实际 offset 回填
            api_listener: Some(api_listener),
            api_monitor_listener: Some(api_monitor_listener),
            relay_listener: Some(relay_listener),
            dht_socket: Some(dht_socket),
            crawler_sockets,
            utp_socket: Some(utp_socket),
            tcp_pex_listener: Some(tcp_pex_listener),
            federation_tcp: Some(federation_tcp),
            federation_udp: Some(federation_udp),
        })
    }

    fn bind_tcp(&self, port: u16) -> Result<TcpListener> {
        let addr = format!("{}:{}", self.bind_addr, port);
        let l = TcpListener::bind(&addr)
            .with_context(|| format!("TcpListener::bind({}) 失败", addr))?;
        Ok(l)
    }

    fn bind_udp(&self, port: u16) -> Result<UdpSocket> {
        let addr = format!("{}:{}", self.bind_addr, port);
        let s =
            UdpSocket::bind(&addr).with_context(|| format!("UdpSocket::bind({}) 失败", addr))?;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// 串行化本模块所有端口测试的静态锁。
    static PORT_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        PORT_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// 探测一个当前空闲的 TCP 端口（通过绑定端口 0，随即释放）。
    fn probe_free_port() -> u16 {
        let l = TcpListener::bind("127.0.0.1:0").expect("探测空闲端口失败");
        l.local_addr().expect("读取 local_addr 失败").port()
    }

    /// 基于一个空闲端口构造测试用基础端口组（镜像真实相对偏移）。
    /// - TCP: api=p, api_monitor=p+6, relay=p+1, tcp_pex=p+4, federation=p+5
    /// - UDP: dht=p, crawler=p+2, utp=p+3, federation=p+5
    fn test_base_ports(p: u16, socket_count: u16) -> PortGroup {
        PortGroup {
            api_port: p,
            api_monitor_port: p + 6,
            relay_port: p + 1,
            dht_listen_port: p,
            crawler_port: p + 2,
            utp_port: p + 3,
            tcp_pex_port: p + 4,
            federation_port: p + 5,
            crawler_socket_ports: build_crawler_socket_ports(p + 2, socket_count),
        }
    }

    /// 占用指定端口组（用于测试偏移/失败场景）。返回持有的 socket，丢弃即释放。
    /// 已被外部占用的端口跳过（视为已占用），保证测试幂等。
    fn occupy_group(ports: &PortGroup) -> (Vec<TcpListener>, Vec<UdpSocket>) {
        let mut tcp = Vec::new();
        let mut udp = Vec::new();
        for p in [
            ports.api_port,
            ports.api_monitor_port,
            ports.relay_port,
            ports.tcp_pex_port,
            ports.federation_port,
        ] {
            if let Ok(l) = TcpListener::bind(("127.0.0.1", p)) {
                tcp.push(l);
            }
        }
        for p in std::iter::once(ports.dht_listen_port)
            .chain(ports.crawler_socket_ports.iter().copied())
            .chain(std::iter::once(ports.utp_port))
            .chain(std::iter::once(ports.federation_port))
        {
            if let Ok(s) = UdpSocket::bind(("127.0.0.1", p)) {
                udp.push(s);
            }
        }
        (tcp, udp)
    }

    #[test]
    fn test_port_group_from_config() {
        let _g = test_lock();
        let config = crate::config::PdcConfig::default();
        let allocator = PortAllocator::from_config(&config);
        assert_eq!(allocator.base_ports.api_port, config.server.port);
        assert_eq!(
            allocator.base_ports.api_monitor_port,
            config.server.api_port
        );
        assert_eq!(
            allocator.base_ports.relay_port,
            config.super_tracker.relay_port
        );
        assert_eq!(
            allocator.base_ports.dht_listen_port,
            config.discoverers.dht_listen_port
        );
        assert_eq!(
            allocator.base_ports.crawler_port,
            config.crawler.listen_port
        );
        assert_eq!(allocator.base_ports.utp_port, config.crawler.utp_port);
        assert_eq!(
            allocator.base_ports.tcp_pex_port,
            config.crawler.tcp_pex_port
        );
        assert_eq!(
            allocator.base_ports.federation_port,
            config.federation.listen_port
        );
        // 默认 socket_count=1 → crawler_socket_ports 仅含主端口
        assert_eq!(allocator.base_ports.crawler_socket_ports.len(), 1);
        assert_eq!(
            allocator.base_ports.crawler_socket_ports[0],
            config.crawler.listen_port
        );
        assert_eq!(allocator.step, config.port_step);
        assert_eq!(allocator.max_attempts, 100);
    }

    #[test]
    fn test_allocate_success() {
        let _g = test_lock();
        let base = test_base_ports(probe_free_port(), 1);
        let allocator = PortAllocator::new(base, 100).with_bind_addr("127.0.0.1");
        let allocation = allocator.allocate().expect("应成功分配端口组");
        // 分配到的端口组应与对应百位段一致
        let expected = allocator.ports_for_group(OFFSET_GROUPS[allocation.offset as usize]);
        assert_eq!(allocation.ports, expected.unwrap());
        // 所有 socket 均已绑定
        assert!(allocation.api_listener.is_some());
        assert!(allocation.api_monitor_listener.is_some());
        assert!(allocation.relay_listener.is_some());
        assert!(allocation.dht_socket.is_some());
        assert!(allocation.utp_socket.is_some());
        assert!(allocation.tcp_pex_listener.is_some());
        assert!(allocation.federation_tcp.is_some());
        assert!(allocation.federation_udp.is_some());
        // socket_count=1 → 爬虫多 socket 仅主端口一个
        assert_eq!(allocation.crawler_sockets.len(), 1);
        assert!(allocation.crawler_sockets[0].is_some());
        assert_eq!(allocation.ports.crawler_socket_ports.len(), 1);
        assert_eq!(
            allocation.ports.crawler_socket_ports[0],
            allocation.ports.crawler_port
        );
    }

    #[test]
    fn test_allocate_with_offset() {
        let _g = test_lock();
        let base = test_base_ports(probe_free_port(), 1);
        let allocator = PortAllocator::new(base, 100).with_bind_addr("127.0.0.1");
        // 占用第 0 组（OFFSET_GROUPS[0]），迫使分配器跳到下一个百位段
        let g0 = allocator
            .ports_for_group(OFFSET_GROUPS[0])
            .expect("g0 应可计算");
        let _held = occupy_group(&g0);

        let allocation = allocator.allocate().expect("应偏移到下一组成功");
        assert!(allocation.offset >= 1, "应跳过被占用的第 0 组");
        let expected = allocator.ports_for_group(OFFSET_GROUPS[allocation.offset as usize]);
        assert_eq!(allocation.ports, expected.unwrap());
    }

    #[test]
    fn test_apply_to_config() {
        let _g = test_lock();
        let base = test_base_ports(probe_free_port(), 1);
        let allocator = PortAllocator::new(base, 100).with_bind_addr("127.0.0.1");
        let allocation = allocator.allocate().unwrap();

        let mut config = crate::config::PdcConfig::default();
        allocation.apply_to_config(&mut config);

        assert_eq!(config.server.port, allocation.ports.api_port);
        assert_eq!(config.server.api_port, allocation.ports.api_monitor_port);
        assert_eq!(config.super_tracker.relay_port, allocation.ports.relay_port);
        assert_eq!(
            config.discoverers.dht_listen_port,
            allocation.ports.dht_listen_port
        );
        assert_eq!(config.crawler.listen_port, allocation.ports.crawler_port);
        assert_eq!(config.crawler.utp_port, allocation.ports.utp_port);
        assert_eq!(config.crawler.tcp_pex_port, allocation.ports.tcp_pex_port);
        assert_eq!(
            config.federation.listen_port,
            allocation.ports.federation_port
        );
        assert_eq!(config.federation.api_port, allocation.ports.api_port);
    }

    #[test]
    fn test_release_all() {
        let _g = test_lock();
        let base = test_base_ports(probe_free_port(), 1);
        let allocator = PortAllocator::new(base, 100).with_bind_addr("127.0.0.1");
        let mut allocation = allocator.allocate().unwrap();
        let p = allocation.ports.clone();

        // 分配持有期间，再次绑定应失败
        assert!(TcpListener::bind(("127.0.0.1", p.api_port)).is_err());
        assert!(UdpSocket::bind(("127.0.0.1", p.dht_listen_port)).is_err());

        // 释放后应可重新绑定
        allocation.release_all();
        assert!(TcpListener::bind(("127.0.0.1", p.api_port)).is_ok());
        assert!(UdpSocket::bind(("127.0.0.1", p.dht_listen_port)).is_ok());
    }

    #[test]
    fn test_max_attempts_exceeded() {
        let _g = test_lock();
        let base = test_base_ports(probe_free_port(), 1);
        let allocator = PortAllocator::new(base, 100)
            .with_max_attempts(1)
            .with_bind_addr("127.0.0.1");
        // 占用第 0 组；max_attempts=1 只试第 0 组 → 应报错
        let g0 = allocator.ports_for_group(OFFSET_GROUPS[0]).unwrap();
        let _held = occupy_group(&g0);
        let result = allocator.allocate();
        assert!(result.is_err(), "超过最大尝试次数应返回错误");
    }

    #[test]
    fn test_socket_count_ports() {
        // socket_count=1 → 仅主端口
        assert_eq!(build_crawler_socket_ports(6882, 1), vec![6882]);
        // socket_count=3 → 主端口 + 6802 + 6812
        assert_eq!(build_crawler_socket_ports(6882, 3), vec![6882, 6802, 6812]);
        // socket_count=10 → 主端口 + 全部 9 个辅助端口
        let v = build_crawler_socket_ports(6882, 10);
        assert_eq!(v.len(), 10);
        assert_eq!(v[0], 6882);
        assert!(v.contains(&6892));
        // 主端口 82 只出现一次
        assert_eq!(v.iter().filter(|&&x| x == 6882).count(), 1);
        // 超过上限 clamp 到 10
        assert_eq!(build_crawler_socket_ports(6882, 99).len(), 10);
    }
}
