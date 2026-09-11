//! 端口组自动探测模块
//!
//! 节点启动时自动探测一组可用端口，避免手动配置端口冲突。
//! 从基础端口出发，按步长 `port_step` 整组偏移，逐组尝试 bind：
//! - 组内所有端口 bind 成功 → 采用该组，socket 保持绑定（避免竞态）
//! - 任意端口 bind 失败 → 释放本组已绑定的 socket，offset += 1 继续
//! - 超过 `max_attempts` 组仍失败 → 返回错误
//!
//! 注意：LPD 多播端口 6771 固定，不参与偏移探测。

use std::net::{TcpListener, UdpSocket};

use anyhow::{anyhow, Context, Result};

/// 一组端口（基础值或实际分配值）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortGroup {
    /// API/HTTP 监控端口（TCP），对应 `config.server.port`
    pub api_port: u16,
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
}

/// 端口分配结果（包含实际端口和已绑定的 socket）
///
/// 所有 socket 在分配成功后保持绑定状态，直到 [`PortAllocation::release_all`]
/// 或本结构被 drop。调用方可通过对应字段提取 socket 交给真实服务使用。
pub struct PortAllocation {
    /// 实际分配到的端口组
    pub ports: PortGroup,
    /// 实际使用的偏移量（= 实际端口 - 基础端口 除以 步长）
    pub offset: u16,
    /// API/HTTP 监控（TCP）
    pub api_listener: Option<TcpListener>,
    /// 中继服务器（TCP）
    pub relay_listener: Option<TcpListener>,
    /// DHT 发现器（UDP）
    pub dht_socket: Option<UdpSocket>,
    /// DHT 爬虫（UDP）
    pub crawler_socket: Option<UdpSocket>,
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
        self.relay_listener.take();
        self.dht_socket.take();
        self.crawler_socket.take();
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
        let base_ports = PortGroup {
            api_port: config.server.port,
            relay_port: config.super_tracker.relay_port,
            dht_listen_port: config.discoverers.dht_listen_port,
            crawler_port: config.crawler.listen_port,
            utp_port: config.crawler.utp_port,
            tcp_pex_port: config.crawler.tcp_pex_port,
            federation_port: config.federation.listen_port,
        };
        Self::new(base_ports, config.port_step)
    }

    /// 执行探测，返回第一个可用的端口组分配。
    ///
    /// 整组探测：每个端口 = 基础端口 + offset * step。组内全部 bind 成功即返回；
    /// 任一失败则释放本组已绑定的 socket，offset += 1 继续。
    pub fn allocate(&self) -> Result<PortAllocation> {
        for offset in 0..self.max_attempts {
            let offset_u16 = u16::try_from(offset)
                .map_err(|_| anyhow!("offset {} 超出 u16 范围", offset))?;

            let Some(ports) = self.ports_at(offset_u16) else {
                // 端口溢出 u16，本组合法性已不存在，继续下一组无意义，直接报错
                return Err(anyhow!(
                    "offset={} 时端口超出 u16 范围（基础端口={:?}, step={}）",
                    offset_u16,
                    self.base_ports,
                    self.step
                ));
            };

            match self.try_bind_group(&ports) {
                Ok(mut allocation) => {
                    allocation.offset = offset_u16;
                    tracing::info!(
                        "[port_allocator] 端口组分配成功: offset={}, ports={:?}",
                        offset_u16,
                        ports
                    );
                    return Ok(allocation);
                }
                Err(e) => {
                    tracing::debug!(
                        "[port_allocator] offset={} 端口组不可用，尝试下一组: {}",
                        offset_u16,
                        e
                    );
                    // 失败时已自动释放本组 socket（try_bind_group 内失败即 drop）
                }
            }
        }

        Err(anyhow!(
            "端口组探测失败：已尝试 {} 组（基础端口={:?}, step={}），均无可用端口",
            self.max_attempts,
            self.base_ports,
            self.step
        ))
    }

    /// 计算某个 offset 下的端口组；任何端口溢出 u16 返回 None
    fn ports_at(&self, offset: u16) -> Option<PortGroup> {
        let shift = (offset as u32) * (self.step as u32);
        let shift_one = |base: u16| -> Option<u16> {
            let p = (base as u32).checked_add(shift)?;
            u16::try_from(p).ok()
        };
        Some(PortGroup {
            api_port: shift_one(self.base_ports.api_port)?,
            relay_port: shift_one(self.base_ports.relay_port)?,
            dht_listen_port: shift_one(self.base_ports.dht_listen_port)?,
            crawler_port: shift_one(self.base_ports.crawler_port)?,
            utp_port: shift_one(self.base_ports.utp_port)?,
            tcp_pex_port: shift_one(self.base_ports.tcp_pex_port)?,
            federation_port: shift_one(self.base_ports.federation_port)?,
        })
    }

    /// 尝试绑定一整组端口。任一失败则返回 Err，且本组已绑定的 socket 自动 drop 释放。
    fn try_bind_group(&self, ports: &PortGroup) -> Result<PortAllocation> {
        let api_listener = self.bind_tcp(ports.api_port).with_context(|| {
            format!("API/HTTP 监控端口 {} (TCP) 绑定失败", ports.api_port)
        })?;
        let relay_listener = self.bind_tcp(ports.relay_port).with_context(|| {
            format!("中继端口 {} (TCP) 绑定失败", ports.relay_port)
        })?;
        let dht_socket = self.bind_udp(ports.dht_listen_port).with_context(|| {
            format!("DHT 发现端口 {} (UDP) 绑定失败", ports.dht_listen_port)
        })?;
        let crawler_socket = self.bind_udp(ports.crawler_port).with_context(|| {
            format!("DHT 爬虫端口 {} (UDP) 绑定失败", ports.crawler_port)
        })?;
        let utp_socket = self
            .bind_udp(ports.utp_port)
            .with_context(|| format!("uTP 端口 {} (UDP) 绑定失败", ports.utp_port))?;
        let tcp_pex_listener = self.bind_tcp(ports.tcp_pex_port).with_context(|| {
            format!("TCP-PEX 端口 {} (TCP) 绑定失败", ports.tcp_pex_port)
        })?;
        let federation_tcp = self.bind_tcp(ports.federation_port).with_context(|| {
            format!("联邦端口 {} (TCP) 绑定失败", ports.federation_port)
        })?;
        let federation_udp = self.bind_udp(ports.federation_port).with_context(|| {
            format!("联邦端口 {} (UDP) 绑定失败", ports.federation_port)
        })?;

        // 注意：relay(TCP) 与 dht(UDP) 可能共用同一端口号，TCP/UDP 协议栈独立，可共存。
        Ok(PortAllocation {
            ports: *ports,
            offset: 0, // 调用方（allocate）会按实际 offset 回填
            api_listener: Some(api_listener),
            relay_listener: Some(relay_listener),
            dht_socket: Some(dht_socket),
            crawler_socket: Some(crawler_socket),
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
        let s = UdpSocket::bind(&addr)
            .with_context(|| format!("UdpSocket::bind({}) 失败", addr))?;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// 串行化本模块所有端口测试的静态锁。
    ///
    /// cargo 默认多线程跑测试，而这些测试都通过"探测空闲端口→立即释放→再绑定"
    /// 的方式选端口。并发时本模块测试之间会互相抢占刚释放的临时端口，导致
    /// offset 偏差或 AddrInUse。用一把锁把它们串行化即可消除该竞争。
    static PORT_TEST_LOCK: Mutex<()> = Mutex::new(());

    /// 获取测试锁（即便此前有测试 panic 导致锁中毒，也恢复使用）。
    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        PORT_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// 探测一个当前空闲的 TCP 端口（通过绑定端口 0，随即释放）。
    /// 用于测试中构造不易冲突的基础端口。
    fn probe_free_port() -> u16 {
        let l = TcpListener::bind("127.0.0.1:0").expect("探测空闲端口失败");
        l.local_addr().expect("读取 local_addr 失败").port()
    }

    /// 基于一个空闲端口构造测试用基础端口组。
    ///
    /// 端口分配方案（镜像真实设计：relay(TCP) 与 dht(UDP) 可同号共存）：
    /// - TCP: api=P, relay=P+1, tcp_pex=P+2, federation=P+3
    /// - UDP: dht=P, crawler=P+1, utp=P+2, federation=P+3
    fn test_base_ports(p: u16) -> PortGroup {
        PortGroup {
            api_port: p,
            relay_port: p + 1,
            dht_listen_port: p,
            crawler_port: p + 1,
            utp_port: p + 2,
            tcp_pex_port: p + 2,
            federation_port: p + 3,
        }
    }

    /// 占用指定端口组（用于测试偏移/失败场景）。返回持有的 socket，
    /// 丢弃即释放。
    fn occupy_group(ports: &PortGroup) -> (Vec<TcpListener>, Vec<UdpSocket>) {
        let mut tcp = Vec::new();
        let mut udp = Vec::new();
        for p in [
            ports.api_port,
            ports.relay_port,
            ports.tcp_pex_port,
            ports.federation_port,
        ] {
            tcp.push(TcpListener::bind(("127.0.0.1", p)).unwrap());
        }
        for p in [
            ports.dht_listen_port,
            ports.crawler_port,
            ports.utp_port,
            ports.federation_port,
        ] {
            udp.push(UdpSocket::bind(("127.0.0.1", p)).unwrap());
        }
        (tcp, udp)
    }

    #[test]
    fn test_port_group_from_config() {
        let _g = test_lock();
        let config = crate::config::PdcConfig::default();
        let allocator = PortAllocator::from_config(&config);
        // 默认配置下基础端口应与配置字段一致
        assert_eq!(allocator.base_ports.api_port, config.server.port);
        assert_eq!(allocator.base_ports.relay_port, config.super_tracker.relay_port);
        assert_eq!(
            allocator.base_ports.dht_listen_port,
            config.discoverers.dht_listen_port
        );
        assert_eq!(allocator.base_ports.crawler_port, config.crawler.listen_port);
        assert_eq!(allocator.base_ports.utp_port, config.crawler.utp_port);
        assert_eq!(
            allocator.base_ports.tcp_pex_port,
            config.crawler.tcp_pex_port
        );
        assert_eq!(
            allocator.base_ports.federation_port,
            config.federation.listen_port
        );
        // 步长应取自配置（默认 10）
        assert_eq!(allocator.step, config.port_step);
        assert_eq!(allocator.max_attempts, 100);
    }

    #[test]
    fn test_allocate_success() {
        let _g = test_lock();
        let base = test_base_ports(probe_free_port());
        let allocator = PortAllocator::new(base, 10).with_bind_addr("127.0.0.1");
        let allocation = allocator.allocate().expect("应成功分配端口组");
        // 基础端口空闲，应分配到 offset=0
        assert_eq!(allocation.offset, 0);
        assert_eq!(allocation.ports, base);
        // 所有 socket 均已绑定
        assert!(allocation.api_listener.is_some());
        assert!(allocation.relay_listener.is_some());
        assert!(allocation.dht_socket.is_some());
        assert!(allocation.crawler_socket.is_some());
        assert!(allocation.utp_socket.is_some());
        assert!(allocation.tcp_pex_listener.is_some());
        assert!(allocation.federation_tcp.is_some());
        assert!(allocation.federation_udp.is_some());
        // allocation drop 时释放所有端口
    }

    #[test]
    fn test_allocate_with_offset() {
        let _g = test_lock();
        let base = test_base_ports(probe_free_port());
        // 占用 offset=0 的整组端口
        let _held = occupy_group(&base);

        let allocator = PortAllocator::new(base, 5).with_bind_addr("127.0.0.1");
        let allocation = allocator.allocate().expect("应偏移到下一组成功");

        // offset=0 被占用，应偏移到 offset>=1
        assert!(allocation.offset >= 1);
        let expected = PortGroup {
            api_port: base.api_port + allocation.offset * 5,
            relay_port: base.relay_port + allocation.offset * 5,
            dht_listen_port: base.dht_listen_port + allocation.offset * 5,
            crawler_port: base.crawler_port + allocation.offset * 5,
            utp_port: base.utp_port + allocation.offset * 5,
            tcp_pex_port: base.tcp_pex_port + allocation.offset * 5,
            federation_port: base.federation_port + allocation.offset * 5,
        };
        assert_eq!(allocation.ports, expected);
    }

    #[test]
    fn test_apply_to_config() {
        let _g = test_lock();
        let base = test_base_ports(probe_free_port());
        let allocator = PortAllocator::new(base, 10).with_bind_addr("127.0.0.1");
        let allocation = allocator.allocate().unwrap();

        let mut config = crate::config::PdcConfig::default();
        allocation.apply_to_config(&mut config);

        assert_eq!(config.server.port, allocation.ports.api_port);
        assert_eq!(config.super_tracker.relay_port, allocation.ports.relay_port);
        assert_eq!(
            config.discoverers.dht_listen_port,
            allocation.ports.dht_listen_port
        );
        assert_eq!(config.crawler.listen_port, allocation.ports.crawler_port);
        assert_eq!(config.crawler.utp_port, allocation.ports.utp_port);
        assert_eq!(
            config.crawler.tcp_pex_port,
            allocation.ports.tcp_pex_port
        );
        assert_eq!(
            config.federation.listen_port,
            allocation.ports.federation_port
        );
        assert_eq!(config.federation.api_port, allocation.ports.api_port);
    }

    #[test]
    fn test_release_all() {
        let _g = test_lock();
        let base = test_base_ports(probe_free_port());
        let allocator = PortAllocator::new(base, 10).with_bind_addr("127.0.0.1");
        let mut allocation = allocator.allocate().unwrap();
        let p = allocation.ports;

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
        let base = test_base_ports(probe_free_port());
        // 占用 offset=0 的整组端口
        let _held = occupy_group(&base);

        // step=1, max_attempts=1：只尝试 offset=0，已被占用 → 应报错
        let allocator = PortAllocator::new(base, 1)
            .with_max_attempts(1)
            .with_bind_addr("127.0.0.1");
        let result = allocator.allocate();
        assert!(result.is_err(), "超过最大尝试次数应返回错误");
    }
}
