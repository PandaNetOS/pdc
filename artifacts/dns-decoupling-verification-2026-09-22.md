# pdc 全进程脱离宿主系统 DNS — 双端部署验收报告

- **日期**：2026-09-22
- **背景**：现场 192.168.30.51 网卡 DNS 指向 `192.168.30.35`，该主机不响应 DNS 查询，导致 iroh 端点发现（pkarr 发布/解析、`DnsAddressLookup` TXT、DERP 主机名）与 tracker/scrape/订阅拉取全部解析超时，联邦任务被长时间阻塞。
- **目标（用户确认范围 C）**：**全覆盖 + 配置化** —— iroh + pdc tracker/scrape/subscription + STUN + federation seed 全部改走内置 DNS 池，且通过 `config.yaml` 可配；iroh **保留 N0 preset，只换解析器**。
- **日志时区**：下文日志时间戳为 **UTC**（`05:xx` = 本地 `13:xx`）。

---

## 1. 结论

| 项 | 结果 |
|---|---|
| 双端部署 | ✅ 本机 30.10（`pdc-local`）、远端 30.51（`pdc-service`）均已换新二进制 |
| 二进制一致 | ✅ 两端 `sha=6FE8D588…CD63`，`len=31484928` |
| 内置 DNS 池生效 | ✅ 两端 `[main] DNS 解析池已就绪` + `[iroh-transport] DNS 解析器已注入` |
| hickory 实际出口 | ✅ `NameServerConfig` 100% 指向池内 5 个公共 DNS，**系统 DNS 0 次** |
| 解析器在应答 | ✅ 本端 `NXDOMAIN` 1349 次 / 远端 289 次（权威应答，证明 DNS 真在答） |
| 传输层失败 | ✅ 本端 **0**；远端 **1 次事件 / 22.9 分钟**（DERP 中继主机名，公网 DNS 出口偶发） |
| 系统 DNS 残留 | ✅ 两端均为 **0**（修复前远端 99 次） |
| 判定 | **本端 `[OK]`；远端 `[~]`**（无系统 DNS 残留，1 处偶发可接受） |

**核心结论：宿主系统 DNS 已从解析路径中彻底移除，且解析器工作正常。修复前远端 51 日志里的「解析超时 / Resolve 失败」几乎全部归零。**

---

## 2. 改动清单

### 2.1 pnos-sdk（`pnos-sdk/crates/pnos-net`，3 文件 / +550 −28）

| 文件 | 关键改动 |
|---|---|
| `src/dns.rs` | 新增 `DnsConfig`（`servers` / `allow_system_fallback` / `query_timeout_secs` / `attempts`，全字段 `#[serde(default)]`）；`DEFAULT_DNS_SERVERS` 提为公开常量；新增 `DnsConfig::to_iroh_resolver()`、`DnsPool::from_config / servers / allow_system_fallback`、`resolve_endpoints()`（异步批量预解析）、`split_host_port` / `normalize_host`；`resolve()` 增加字面 IP 直通；`opts.ip_strategy = Ipv4AndIpv6`；**8 个单元测试** |
| `src/transport/iroh.rs` | `IrohTransportConfig` 增 `dns: DnsConfig`；`IrohIdentity::from_pnos_node_id` 增 `dns` 参数，`Endpoint::builder(presets::N0).dns_resolver(...)` 注入 → 一处覆盖 N0 preset 四条解析路径 |
| `src/discovery/mqtt.rs` | 新增 `with_dns_config()`；`allow_system_fallback=false` 时解析失败**显式报错**而不是把域名悄悄交给 rumqttc 走系统 DNS |

> **硬约束（已写入代码注释）**：`DnsPool` **必须纯异步、零 tokio runtime**。hickory 的同步 `Resolver` 内含 `Mutex<Runtime>`，在异步上下文析构会 panic（`Cannot drop a runtime in a context where blocking is not allowed.`）。本次一度引入同步 `Resolver` 导致 `from_pnos_node_id` 这类 `async fn` 里的局部池析构即崩，已回退。

### 2.2 pdc（4 文件）

| 文件 | 关键改动 |
|---|---|
| `src/dns_pool.rs` | 进程级 `OnceLock<Arc<DnsPool>>` 的 `init_global` / `global` |
| `src/dns_resolve.rs`（新增） | `reqwest::dns::Resolve` 适配器 + `apply_dns_pool(builder, Option<&Arc<DnsPool>>)` |
| `src/federation/nat_integration.rs` | `resolved_stun_servers()` 改 **async**，用池预解析成 `ip:port`；`stun_probe` → `stun_probe_with(&[String])`，**函数内不再做任何 DNS** |
| `src/federation/mod.rs` | 启动 STUN：先 `await` 预解析，再 `spawn_blocking(move \|\| nat_clone.stun_probe_with(&servers))` |

### 2.3 配置（`config.yaml`）

```yaml
dns:
  servers:
    - "114.114.114.114:53"
    - "223.5.5.5:53"
    - "119.29.29.29:53"
    - "8.8.8.8:53"
    - "1.1.1.1:53"
  allow_system_fallback: false   # 默认 false：绝不读宿主系统 DNS
  query_timeout_secs: 3
  attempts: 2
```

---

## 3. 部署记录

| 端 | 角色 | 旧 PID → 新 PID | 启动时间 | 备份 |
|---|---|---|---|---|
| 192.168.30.10 | `pdc-local` | 19192 → **4636** | 13:13:34 | `backup_predeploy_dnsfix_20260922_131321` |
| 192.168.30.51 | `pdc-service` | 8812 → **18556** | 13:15:54 | `pdc.exe.preDnsFix.bak` / `config.yaml.preDnsFix.bak` |

- 远端走 **CIM**：`Stop-ScheduledTask` → SMB 覆盖 `pdc.exe` + `config/config.yaml` → `Start-ScheduledTask`（`-CimSession` 需显式 Administrator 凭据；SMB `d$` 读写本身不需凭据）。
- 配置文件**同步部署**（遵循项目约束 7），远端 config 亦为 4405B 含 `dns:` 段。

---

## 4. 验收判据修正（旧判据为什么会误判）

旧版 `tools/pdc_dual_deploy_verify.py` 把 `dns.iroh.link` / `Service 'pkarr' failed` / `Error resolving http request` 当反向锚点，导致反向计数恒为数百（本端 428、远端 300+），永远判不出结论。**三处错误**：

| 旧锚点 | 为什么不是错误信号 | 源码依据 |
|---|---|---|
| `dns.iroh.link` | 该字符串在**连接成功**日志里也出现（`pooling/reuse idle connection for ("https", dns.iroh.link)`） | iroh `address_lookup/pkarr.rs` |
| `Error resolving http request` | 是 `PkarrError::HttpRequest { status: http::StatusCode }`，携带 **HTTP 状态码** —— pkarr relay 回复「该 key 无记录」（404），**与 DNS 无关** | `iroh-1.2.0/src/address_lookup/pkarr.rs:105` |
| `Service 'pkarr' failed` / `Failed to resolve TXT record` | 后者是 `LookupError::LookupFailed { source: DnsError }`，而 `DnsError::NxDomain` 就是**权威应答「域名不存在」** —— 恰恰证明解析器在正常工作 | `iroh-dns-1.3.0/src/dns.rs:184,195` |

**修正后的判据**（已落地到工具）：

- **正向**（需 ≥3 且必须含 NXDOMAIN）：`[main] DNS 解析池已就绪`、`[iroh-transport] DNS 解析器已注入`、`[dns_pool] 已初始化`、`[dns_pool] 预解析`、`NXDOMAIN`
- **反向**（期望 0，按**失败行数**计而非锚点命中数）：系统 DNS 残留 `192.168.30.35`、`Request timed out`、`No response`、`Resolve failed, IPv4`
- **噪声**（只展示不判定）：对端未发布 pkarr/TXT 记录导致的三类
- 另修：运行段定位改为**从尾向前按 `六 runtime 隔离` 标记定位**（日志是追加的，旧版读尾部 40MB 只覆盖最后 ~33 秒）；判定加 `[~]` 中间档避免偶发噪声直接翻红。

---

## 5. 正向证据（两端一致）

```
INFO  [main] DNS 解析池已就绪: servers=["114.114.114.114:53","223.5.5.5:53","119.29.29.29:53","8.8.8.8:53","1.1.1.1:53"], 系统 DNS 回退=false
INFO  [iroh-transport] DNS 解析器已注入（不读系统 DNS）: servers=[...同上...], 系统回退=false
DEBUG [dns_pool] 已初始化 5 个 DNS 服务器（系统 DNS 回退=false）
DEBUG [dns_pool] 预解析 stun.miwifi.com:3478 -> 111.206.174.2:3478（不读系统 DNS）   # 本端 30 次 / 远端 4 次
```

**hickory 实际出口（本端 25.2 分钟窗口）**：

| socket_addr | 次数 |
|---|---|
| `114.114.114.114:53` | 167 |
| `223.5.5.5:53` | 167 |
| `119.29.29.29:53` | 167 |
| `8.8.8.8:53` | 167 |
| `1.1.1.1:53` | 167 |
| **系统 DNS（`192.168.30.35`）** | **0** |

---

## 6. 前后对照（远端 30.51 —— 修复前唯一 DNS 不答查询）

用同一份日志里相邻两个运行段做**同口径字节级统计**（段 A = 修复前二进制，段 B = 修复后）：

| 指标 | A 修复前（46.5 min） | B 修复后（22.9 min） | 归一化 |
|---|---|---|---|
| `192.168.30.35`（系统 DNS） | **99** | **0** | 2.13/min → **0** ✅ |
| `Request timed out` | 48 | 1 | 1.03/min → **0.04/min**（−96%） |
| `Resolve failed, IPv4` | 24 | 1 | 0.52/min → **0.04/min**（−92%） |
| `NXDOMAIN`（DNS 真在答） | 146 | 289 | 3.14/min → **12.6/min**（+301%） |
| `NameServerConfig` | 1280 | 1040 | 27.5/min → 45.4/min |
| `DNS 解析池已就绪` | 0 | **1** | ✅ 新路径生效 |

**关键读法**：修复前远端有 **48 次解析超时 + 24 次 Resolve 失败 + 99 次系统 DNS 痕迹**，这就是「宿主 DNS 整台不答 → 传输层全线超时」的指纹；修复后传输层失败降到 **1 次**，同时 `NXDOMAIN`（权威否定应答）从 3.14/min 升到 12.6/min —— 说明查询从「发不出去」变成「发出去并拿到应答」。

> 注：`address lookup error` / `pkarr failed` 这类噪声计数修复后反而**升高**（本端 15.3/min、远端 4.1/min），原因是查询不再被超时拖住，单位时间**完成的**对端解析次数变多，每个未发布记录的对端都会产生一条。这是吞吐上升的副产物，不是回归。

**旁证（逻辑闭环）**：本端 30.10 的系统 DNS 是**正常**的，却出现**形态完全相同、速率更高**的 pkarr/TXT 噪声（本端 22.8/min vs 远端 5.6/min）。若该噪声与 DNS 可用性相关，健康端不可能比故障端还多。⇒ 该噪声是**对端未发布记录**的固有产物，与系统 DNS 正交。

---

## 7. 本次验证新发现的一个残留缺口（建议跟进）

### 现象

远端 51 的 STUN 预解析有失败项：

```
WARN [dns_pool] 预解析 stun.cloudflare.com:3478 失败（DNS 解析 stun.cloudflare.com 失败
     （未启用系统 DNS 回退）: no record found for Query { name: Name("stun.cloudflare.com."),
      query_type: AAAA, query_class: IN }），保留原值
WARN [dns_pool] 预解析 stun.l.google.com:19302 失败（... AAAA ...），保留原值
```

| federation.stun_servers | 结果 |
|---|---|
| `stun.miwifi.com:3478` | ✅ 稳定成功（111.206.174.2） |
| `stun.chat.bilibili.com:3478` | ✅ 第二次起成功（106.13.248.6） |
| `stun.qq.com:3478` | ✅ 第二次起成功（101.43.100.186） |
| `stun.cloudflare.com:3478` | ❌ 一直失败 |
| `stun.l.google.com:19302` | ❌ 一直失败 |

### 根因

hickory 0.24.4 `lookup_ip.rs:305-354` 的 `ipv4_and_ipv6` 实现是 `(Ok, Err) => Ok(ips)`，**只有 A 与 AAAA 双双失败**才报错；对外报出的是 `future::select` 里**先返回**的那个错误，所以显示 AAAA 只是表象 —— 实际 A 也失败了（日志里 `both of ipv4 or ipv6 lookup failed ... e1: ... query_type: A ...` 可证）。`cloudflare` / `google` 的 A 记录在本网段经国内 DNS 拿不到（限流 / 污染），属**环境事实**，`bilibili`/`qq` 的「先失败后成功」也只是 A 偶发失败。

### 但暴露了真实设计漏洞

`resolve_endpoints()` 在解析失败时**保留原值**（域名），于是下游 `crate::nat::stun` 的同步 Binding 会用 `to_socket_addrs()`（= **系统 DNS**）去解析这个域名 —— 在 51 上必然失败。**「不读系统 DNS」的保证在 STUN 回退路径上有一个洞。**

同类路径还有 `src/services/nat_service.rs:70` `check_udp_port()`：直接用 `nat.stun_servers`（默认 `stun.l.google.com` / `stun1.l.google.com` / `stun.ekiga.net`，本网段全部不可达）调同步 STUN，同样落到系统 DNS。

### 修法（已落地，`allow_system_fallback=false` 时保证「只出字面量」）

1. `resolve_endpoints()`：解析不出 `ip:port` 的条目**直接丢弃**（WARN 明示「已丢弃（未启用系统 DNS 回退，避免下游绕回系统解析器）」），不再保留域名 —— 不变量：**返回值里每一项都可 `parse::<SocketAddr>()`**。需要旧行为时把 `allow_system_fallback` 打开即可（走 `保留原值` 分支）。
2. `nat_service::check_udp_port()`：改 async，先经 `dns_pool::global()` 预解析 STUN 列表，再用 `spawn_blocking` 跑同步探测；全部解析失败时返回 `None`（"无法检测"）而不是假的"不可达"。
3. `nat.stun_servers` 默认值换成与 `federation.stun_servers` 同口径的国内可达列表（小米/B站/腾讯 + Cloudflare + Google 兜底）。
4. **同源第 4 处（本次新发现）**：`pnos-net/src/nat/stun.rs` 的 `detect_nat_type_multi()` 在服务器数 < 2 时会**回退到硬编码的 `stun.l.google.com` / `stun1.l.google.com`** —— 既违反「禁止硬编码可配置参数」，又会在宿主 DNS 不可用时重新踩回 `to_socket_addrs()`（系统 DNS）。已删除该兜底：0 个服务器 → `Unknown`；1 个 → 仅判 `OpenInternet` 否则 `Unknown`。

---

## 8. 第二轮：STUN 回退泄漏修复 + 双端复验（13:55–14:00）

用户决策：**纳入**（提交范围 = B）。改动 4 处：

| 仓库 | 文件 | 改动 |
|---|---|---|
| pnos-net | `src/dns.rs` | `resolve_endpoints()` 严格模式丢弃解析不出的条目（新增 `handle_unresolved()`），保证返回值只含 `ip:port` 字面量；新增 2 个单测（严格丢弃 / 宽松保留） |
| pnos-net | `src/nat/stun.rs` | `detect_nat_type_multi()` 删除硬编码 Google STUN 兜底；新增 `test_detect_nat_type_multi_insufficient_servers` |
| pdc | `src/services/nat_service.rs` | `check_udp_port()` 改 async + 池预解析 + `spawn_blocking`；全解析失败返回 `None` |
| pdc | `src/config.rs` | `nat.stun_servers` 默认值改为国内可达优先（与 `federation.stun_servers` 同口径） |

### 8.1 部署（第二轮）

| 端 | 任务 | 旧 | 新 | 二进制 |
|---|---|---|---|---|
| 本端 30.10 | `pdc-local` | PID 4636 @13:13:34 | **PID 17124 @13:55:09** | `sha=37158722…528C` `len=31500288` |
| 远端 30.51 | `pdc-service` | PID 18556 @13:15:54 | **PID 7616 @13:57:26** | 同上（覆盖后与源逐字节一致） |

备份：本端 `D:\test\pdc\backup_predeploy_stunfix_20260922_135501`；远端 `pdc.exe.preStunFix.bak` / `config.yaml.preStunFix.bak`（6FE8D588…）。
配置未改（`federation.stun_servers` 本来就已是国内优先；`nat.stun_servers` 的默认值只影响未显式配置的场景）。

### 8.2 复验结果（`tools/pdc_dual_deploy_verify.py dns`）

| 项 | 本端 30.10 | 远端 30.51 |
|---|---|---|
| 正向锚点 | **5/5** | **5/5** |
| 失败事件行数 | **0** | **0**（上一轮 1） |
| 系统 DNS 残留 `192.168.30.35` | **0** | **0** |
| `NXDOMAIN`（解析器在应答） | 126 次 / 4.8 min | 200 次 / 2.5 min |
| **STUN 失败条目处置** | 已丢弃 **0** / 保留原值 **0** | 已丢弃 **2** / 保留原值 **0** |
| **`[federation] STUN 探测` 服务器数** | **5 / 5** | **3 / 5** |
| 判定 | `[OK]` | `[OK]` |

**关键证据（远端 51）**：

```
[dns_pool] 预解析 stun.cloudflare.com:3478 失败（...），已丢弃（未启用系统 DNS 回退，避免下游绕回系统解析器）
[federation] STUN 探测开始: 服务器数=3, 列表=["111.206.174.3:3478", "106.13.248.6:3478", "101.43.100.186:3478"]
```

修复前这两个域名会被**原样透传**给同步 STUN 客户端的 `to_socket_addrs()`（= 系统 DNS = 51 上那台不答查询的 `192.168.30.35`）→ 必然失败；现在直接丢弃，探测只用 3 个**字面量 IP**，全程零 DNS 参与。本端 5 个域名全部解析成功（含 Cloudflare `162.159.207.0`、Google `[2001:4860:4864:5:8000::1]`），故 `服务器数=5`、丢弃 0 —— 同一份代码在两种 DNS 环境下都给出正确行为。

### 8.3 待办（未纳入本次提交）

| # | 事项 | 说明 |
|---|---|---|
| 1 | **`nat.stun_servers` 是死配置字段** | `main.rs:476` 用 `..Default::default()` 构造 `pnos_net::nat::NatConfig`，**从未把 `config.nat.stun_servers` 传进去** ⇒ 该字段（以及 `enable_reachability_check`）解析了但不生效。本次只对齐了默认值，未接线。 |
| 2 | `check_udp_port()` 无任何调用方 | 全仓库零调用（`pub` 的 API 面残留），因此本次修复对它而言是"消除隐患"，无运行时效果。修 1 时应一并加调用点或删接口。 |
| 3 | 同类残留（STUN 之外） | `pdc/src/discoverers/tracker/udp.rs:92` `parse_tracker_addr()` 仍用 `to_socket_addrs()`（UDP tracker 主机名 → 系统 DNS）；`pnos-net/src/nat/udp_hole_punch.rs:225` 同理。两者都不在本次"STUN 回退泄漏"范围内。 |
| 4 | 完整合规门禁 | 见 §9。 |

---

## 9. 合规门禁与提交

- 提交范围 = **B**（纯 DNS/STUN 最小集），`pdc` 工作区里的联邦同步大改（`federation/sync/*`、`storage/*`、文档）+ TaskScheduler 槽位回收修复（`task_scheduler.rs` + `config.rs` 的 `stale_slot_*`）**不**纳入本次。
- ⚠️ `tools/` 在 meta 仓库被 `.gitignore` 忽略 ⇒ 验收工具是**本地资产，不进任何提交**；因此验收结论与复现步骤必须落在本报告（`pdc/artifacts/`，已跟踪）。

### 9.1 提交记录

| 仓库 | 分支 | commit | 内容 | 门禁 |
|---|---|---|---|---|
| `pnos-sdk` | `session-layer` | `cb904ae` | 品牌残留清理（`lpd.rs` 魔数 `PDCL`→`PNOS`、`udp_hole_punch.rs` 载荷 `PDC_HOLE_PUNCH`→`PNOS_HOLE_PUNCH`、`mqtt.rs` 字段 `federation_port`→`service_port`）+ DNS 解耦（`dns.rs` 严格模式丢弃、`nat/stun.rs` 去硬编码兜底、`transport/iroh.rs` 注入解析器、`mqtt.rs` 严格化），6 files, +681 −64 | 20 PASS / 0 FAIL / 6 WARN（与重建前同树的 `edfd860` 结论一致） |
| `pdc` | `pdc-session-layer` | `d8ec224` | DNS 池接线 + STUN 预解析 + 配置 + 本报告，13 files, +630 −31 | 21 PASS / 0 FAIL / 5 WARN |

两个仓库各有 **一次提交**，均按「B 范围」**逐文件/逐 hunk**暂存，未夹带联邦同步大改与 TaskScheduler 槽位回收修复（见 §9 第一条）。门禁 WARN 项（pdc: 6/9/10/11/13；sdk: 9/10/11/13/18/20）全部落在**本次未触碰的文件**上，属仓库既有债。

### 9.2 `pnos-sdk/.git` 损坏与提交重建（2026-09-22）

`pnos-sdk/.git` 于 14:37 发生损坏：`.git/refs` 与 `.git/logs` 被整个删除，git 判定该目录不再是仓库后**静默向上回退到父仓库**（`D:\PNOS`），随后一次 `gc --auto` 把已不可达的本地提交对象清除。丢失范围经逐 sha `cat-file -t` 验证，**精确为 3 个提交**：

| 原 commit | 内容 | 处置 |
|---|---|---|
| `2266f36` | session 会话层 | 远端 PR #1 已合并，随 `798f5ea` 回到本地 |
| `fff60cc` | 清理 pdc 品牌残留 | 对象丢失 → 按工作区内容并入重建提交 |
| `edfd860` | 本报告对应的 DNS 修复 | 对象丢失 → 按工作区内容并入重建提交 |

工作区与索引内容完好（索引中另有 32 个 blob 被 gc 清除，已用工作区文件按内容寻址逐一重建，重建后 sha 与索引记录一致）。因此以索引树 `6e7711fe26`（= 原 `edfd860` 的树）为**唯一权威**重建为单个提交 `cb904ae`，parent = 远端 main `798f5ea`；不按不可考的边界拼装 3 个提交，避免失真历史。重建后 `main` == `session-layer` == `cb904ae`，工作区无改动（脏文件数 0）。

`pdc` 侧无损：`d8ec224` / `25af88d` 对象完好；本地 `main` 由 `commit-tree` 造合并提交 `36e4947`（parents = 远端 main `f1f3d67` + `25af88d`，树 = 原 HEAD 树 `1ac5557`），内容零变化，工作区 43 个未暂存改动未受影响。

---

## 附：复现命令

```bash
# DNS 脱离验收（正向 >=3 且含 NXDOMAIN；反向按失败行数计）
python tools/pdc_dual_deploy_verify.py dns

# 双端快照 / 增量 / 日志锚点
python tools/pdc_dual_deploy_verify.py snap
python tools/pdc_dual_deploy_verify.py rate
python tools/pdc_dual_deploy_verify.py logs
```

```powershell
# 完整合规门禁
.\check-compliance.ps1 -ProjectPath D:\PNOS\pdc
.\check-compliance.ps1 -ProjectPath D:\PNOS\pnos-sdk
```
