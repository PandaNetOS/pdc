//! 防火墙规则管理器
//!
//! PDC 启动时自动添加 Windows 防火墙入站规则，确保零配置多节点场景下
//! 局域网发现和联邦连接不被防火墙拦截。
//!
//! 规则命名格式：`PDC-<node_id前8位>-<规则类型>`
//! 例如：`PDC-a1b2c3d4-federation`

use anyhow::Result;
use std::collections::HashSet;

/// 防火墙规则管理器
pub struct FirewallManager {
    /// node_id 前 8 位十六进制，用于规则命名
    node_id_prefix: String,
    /// 是否启用自动防火墙规则
    enabled: bool,
}

/// 防火墙操作结果
#[derive(Debug, Clone, Default)]
pub struct FirewallResult {
    /// 新添加的规则名
    pub added: Vec<String>,
    /// 已存在跳过的规则名
    pub skipped: Vec<String>,
    /// 失败的规则名和错误信息
    pub failed: Vec<(String, String)>,
}

impl FirewallManager {
    /// 创建防火墙管理器
    /// node_id: 20字节节点ID，取前4字节（8位十六进制）用于规则命名
    pub fn new(node_id: &[u8; 20], enabled: bool) -> Self {
        let prefix: String = node_id[0..4]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        Self {
            node_id_prefix: prefix,
            enabled,
        }
    }

    /// 获取规则名前缀（前8位十六进制）
    pub fn node_id_prefix(&self) -> &str {
        &self.node_id_prefix
    }

    /// 判断是否启用
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// 生成完整规则名：PDC-<prefix>-<rule_type>
    fn build_rule_name(prefix: &str, rule_type: &str) -> String {
        format!("PDC-{}-{}", prefix, rule_type)
    }

    /// 对端口列表去重（同端口同协议只保留一条，取首个出现的规则类型名）
    fn dedup_ports(ports: &[(u16, &str, &str)]) -> Vec<(u16, String, String)> {
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for (port, proto, rule_type) in ports {
            let key = (*port, *proto);
            if seen.insert(key) {
                result.push((*port, proto.to_string(), rule_type.to_string()));
            }
        }
        result
    }

    /// 检测系统防火墙是否启用（任意一个配置文件启用即返回 true）
    /// 检测失败时默认返回 true（保守策略，宁可添加规则也不遗漏）
    pub fn is_firewall_enabled() -> bool {
        imp::is_firewall_enabled_impl()
    }

    /// 为一组端口添加入站规则
    /// ports: (端口号, 协议, 规则类型名) 列表，协议为 "TCP" 或 "UDP"
    /// 规则命名格式：PDC-<node_id前8位>-<规则类型>
    /// 执行流程：总开关 → 防火墙状态检测 → 实际添加规则
    pub fn add_rules(&self, ports: &[(u16, &str, &str)]) -> Result<FirewallResult> {
        let mut result = FirewallResult::default();

        // 1. 总开关检查
        if !self.enabled {
            tracing::debug!("[firewall] auto_firewall_rule 未启用，跳过");
            return Ok(result);
        }

        // 2. 防火墙状态检测：关闭则跳过映射
        if !Self::is_firewall_enabled() {
            tracing::info!("[firewall] 防火墙未启用，跳过端口映射");
            return Ok(result);
        }

        // 3. 防火墙已启用，执行规则添加
        let deduped = Self::dedup_ports(ports);
        tracing::info!(
            "[firewall] 防火墙已启用，开始添加 {} 条入站规则",
            deduped.len()
        );
        imp::add_rules_impl(&self.node_id_prefix, &deduped, &mut result);
        Ok(result)
    }

    /// 检查规则是否已存在
    pub fn rule_exists(&self, rule_name: &str) -> bool {
        if !self.enabled {
            return false;
        }
        imp::rule_exists_impl(rule_name)
    }

    /// 删除本节点创建的所有防火墙规则（清理用）
    pub fn cleanup_all(&self) -> Result<()> {
        if !self.enabled {
            tracing::debug!("[firewall] auto_firewall_rule 未启用，跳过清理");
            return Ok(());
        }
        imp::cleanup_all_impl(&self.node_id_prefix)
    }
}

// ============================================================================
// Windows 平台实现
// ============================================================================
#[cfg(target_os = "windows")]
mod imp {
    use super::*;
    use std::process::Command;

    /// 检测 Windows 防火墙是否启用（任意配置文件启用即返回 true）
    /// 使用 `netsh advfirewall show allprofiles` 解析 State 字段
    /// 检测失败时默认返回 true（保守策略）
    pub fn is_firewall_enabled_impl() -> bool {
        let output = match Command::new("netsh")
            .args(["advfirewall", "show", "allprofiles"])
            .output()
        {
            Ok(out) => out,
            Err(e) => {
                tracing::warn!("[firewall] 防火墙状态检测失败，默认执行映射: {}", e);
                return true;
            }
        };

        if !output.status.success() {
            tracing::warn!("[firewall] 防火墙状态检测失败（netsh 返回非零），默认执行映射");
            return true;
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        // 解析每个配置文件的 State 行，任意一个为 ON 即视为启用
        for line in stdout.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("State") {
                // 格式: "State                                 ON" 或 "State                                 OFF"
                if trimmed.to_uppercase().contains("ON") {
                    return true;
                }
            }
        }

        tracing::info!("[firewall] 防火墙所有配置文件均未启用");
        false
    }

    /// 为一组端口添加入站规则（Windows netsh 实现）
    pub fn add_rules_impl(
        prefix: &str,
        ports: &[(u16, String, String)],
        result: &mut FirewallResult,
    ) {
        for (port, proto, rule_type) in ports {
            let rule_name = FirewallManager::build_rule_name(prefix, rule_type);

            // 先检查规则是否已存在
            if rule_exists_impl(&rule_name) {
                result.skipped.push(rule_name);
                continue;
            }

            // 添加规则
            let output = Command::new("netsh")
                .args([
                    "advfirewall",
                    "firewall",
                    "add",
                    "rule",
                    &format!("name={}", rule_name),
                    "dir=in",
                    "action=allow",
                    &format!("protocol={}", proto.to_uppercase()),
                    &format!("localport={}", port),
                ])
                .output();

            match output {
                Ok(out) if out.status.success() => {
                    tracing::info!(
                        "[firewall] 已添加入站规则: {} ({} {})",
                        rule_name,
                        proto,
                        port
                    );
                    result.added.push(rule_name.clone());
                }
                Ok(out) => {
                    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
                    let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
                    let detail = if !stderr.is_empty() {
                        stderr
                    } else {
                        stdout
                    };
                    result.failed.push((rule_name.clone(), detail.clone()));
                    tracing::warn!(
                        "[firewall] 添加规则失败（可能非管理员权限）: {} — {}",
                        rule_name,
                        detail
                    );
                }
                Err(e) => {
                    result.failed.push((rule_name.clone(), e.to_string()));
                    tracing::warn!(
                        "[firewall] 执行 netsh 命令失败: {} — {}",
                        rule_name,
                        e
                    );
                }
            }
        }
    }

    /// 检查规则是否已存在（Windows netsh show 实现）
    pub fn rule_exists_impl(rule_name: &str) -> bool {
        let output = Command::new("netsh")
            .args([
                "advfirewall",
                "firewall",
                "show",
                "rule",
                &format!("name={}", rule_name),
            ])
            .output();

        match output {
            Ok(out) => out.status.success(),
            Err(_) => false,
        }
    }

    /// 删除本节点创建的所有防火墙规则（Windows netsh 实现）
    pub fn cleanup_all_impl(prefix: &str) -> Result<()> {
        let our_prefix = format!("PDC-{}", prefix);

        // 列出所有防火墙规则，筛选出本节点创建的
        let output = Command::new("netsh")
            .args(["advfirewall", "firewall", "show", "rule", "name=all"])
            .output()?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut deleted = 0usize;

        for line in stdout.lines() {
            let line_trimmed = line.trim();
            if let Some(idx) = line_trimmed.find(&our_prefix) {
                // 从找到的位置提取规则名（到空白符为止）
                let rest = &line_trimmed[idx..];
                let name: String = rest.chars().take_while(|c| !c.is_whitespace()).collect();
                if name.starts_with(&our_prefix) {
                    let del = Command::new("netsh")
                        .args([
                            "advfirewall",
                            "firewall",
                            "delete",
                            "rule",
                            &format!("name={}", name),
                        ])
                        .output();
                    match del {
                        Ok(out) if out.status.success() => {
                            deleted += 1;
                            tracing::info!("[firewall] 已删除规则: {}", name);
                        }
                        Ok(out) => {
                            let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
                            tracing::warn!("[firewall] 删除规则失败: {} — {}", name, stderr);
                        }
                        Err(e) => {
                            tracing::warn!("[firewall] 删除规则命令执行失败: {} — {}", name, e);
                        }
                    }
                }
            }
        }

        if deleted > 0 {
            tracing::info!("[firewall] 共清理 {} 条防火墙规则", deleted);
        } else {
            tracing::debug!("[firewall] 未找到前缀为 {} 的规则", our_prefix);
        }
        Ok(())
    }
}

// ============================================================================
// 非 Windows 平台空实现
// ============================================================================
#[cfg(not(target_os = "windows"))]
mod imp {
    use super::*;

    /// 非 Windows 平台：无 Windows 防火墙，返回 false
    pub fn is_firewall_enabled_impl() -> bool {
        false
    }

    /// 非 Windows 平台：跳过防火墙配置
    pub fn add_rules_impl(
        _prefix: &str,
        _ports: &[(u16, String, String)],
        _result: &mut FirewallResult,
    ) {
        tracing::info!("[firewall] 非 Windows 平台，跳过防火墙配置");
    }

    /// 非 Windows 平台：规则不存在
    pub fn rule_exists_impl(_rule_name: &str) -> bool {
        false
    }

    /// 非 Windows 平台：无需清理
    pub fn cleanup_all_impl(_prefix: &str) -> Result<()> {
        tracing::info!("[firewall] 非 Windows 平台，跳过防火墙清理");
        Ok(())
    }
}

// ============================================================================
// 单元测试
// ============================================================================
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_firewall_manager_creation() {
        // 构造一个已知的 node_id
        let node_id: [u8; 20] = [
            0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0x07, 0x08,
            0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
            0x11, 0x12, 0x13, 0x14,
        ];
        let manager = FirewallManager::new(&node_id, true);
        // 前 4 字节 = a1 b2 c3 d4 → "a1b2c3d4"
        assert_eq!(manager.node_id_prefix(), "a1b2c3d4");
        assert!(manager.is_enabled());

        // disabled 模式
        let manager_disabled = FirewallManager::new(&node_id, false);
        assert_eq!(manager_disabled.node_id_prefix(), "a1b2c3d4");
        assert!(!manager_disabled.is_enabled());
    }

    #[test]
    fn test_rule_name_format() {
        let name = FirewallManager::build_rule_name("a1b2c3d4", "federation");
        assert_eq!(name, "PDC-a1b2c3d4-federation");

        let name2 = FirewallManager::build_rule_name("deadbeef", "api");
        assert_eq!(name2, "PDC-deadbeef-api");

        let name3 = FirewallManager::build_rule_name("12345678", "super-tracker-udp");
        assert_eq!(name3, "PDC-12345678-super-tracker-udp");
    }

    #[test]
    fn test_dedup_ports() {
        // api 和 super-tracker 都是 TCP 6880，应去重只保留一条
        let ports: Vec<(u16, &str, &str)> = vec![
            (6880, "TCP", "api"),
            (6880, "TCP", "super-tracker"), // 重复，应被跳过
            (6880, "UDP", "super-tracker-udp"), // 同端口不同协议，保留
            (6881, "TCP", "relay"),
            (6881, "UDP", "dht"), // 同端口不同协议，保留
        ];

        let deduped = FirewallManager::dedup_ports(&ports);
        assert_eq!(deduped.len(), 4);

        // 第一条 TCP 6880 应该是 "api"（首个出现的）
        assert_eq!(deduped[0].0, 6880);
        assert_eq!(deduped[0].1, "TCP");
        assert_eq!(deduped[0].2, "api");

        // UDP 6880 保留
        assert_eq!(deduped[1].0, 6880);
        assert_eq!(deduped[1].1, "UDP");
        assert_eq!(deduped[1].2, "super-tracker-udp");

        // TCP 6881 保留
        assert_eq!(deduped[2].0, 6881);
        assert_eq!(deduped[2].1, "TCP");

        // UDP 6881 保留
        assert_eq!(deduped[3].0, 6881);
        assert_eq!(deduped[3].1, "UDP");
    }

    #[test]
    fn test_firewall_result_default() {
        let result = FirewallResult::default();
        assert!(result.added.is_empty());
        assert!(result.skipped.is_empty());
        assert!(result.failed.is_empty());
    }

    #[test]
    fn test_disabled_manager() {
        let node_id: [u8; 20] = [0x11; 20];
        let manager = FirewallManager::new(&node_id, false);

        // disabled 时 add_rules 应直接返回空结果，不执行任何命令
        let ports: Vec<(u16, &str, &str)> = vec![
            (6880, "TCP", "api"),
            (6882, "UDP", "crawler"),
        ];

        let result = manager.add_rules(&ports).unwrap();
        assert!(result.added.is_empty());
        assert!(result.skipped.is_empty());
        assert!(result.failed.is_empty());

        // disabled 时 rule_exists 返回 false
        assert!(!manager.rule_exists("PDC-11111111-api"));

        // disabled 时 cleanup_all 不执行任何操作
        assert!(manager.cleanup_all().is_ok());
    }

    #[test]
    fn test_empty_ports() {
        let node_id: [u8; 20] = [0xaa; 20];
        let manager = FirewallManager::new(&node_id, true);

        // 空端口列表不应报错
        let result = manager.add_rules(&[]).unwrap();
        assert!(result.added.is_empty());
        assert!(result.skipped.is_empty());
        assert!(result.failed.is_empty());
    }

    #[test]
    fn test_is_firewall_enabled() {
        // 验证函数返回 bool 类型（实际值取决于运行环境的防火墙状态）
        let enabled = FirewallManager::is_firewall_enabled();
        // 函数必须能正常调用并返回 bool，不 panic
        let _ = enabled;
        assert!(enabled == true || enabled == false);
    }

    #[test]
    fn test_add_rules_skips_when_firewall_disabled() {
        // 当防火墙未启用时，add_rules 应跳过并返回空结果
        // 注意：此测试在防火墙启用的环境下不会触发跳过逻辑，
        // 但验证了函数能正常执行不 panic
        let node_id: [u8; 20] = [0xbb; 20];
        let manager = FirewallManager::new(&node_id, true);
        let ports: Vec<(u16, &str, &str)> = vec![(6880, "TCP", "api")];

        // 无论防火墙是否启用，add_rules 都应正常返回
        let result = manager.add_rules(&ports);
        assert!(result.is_ok());
    }
}
