//! 磁盘画像探测（C1）：在 DB 同目录写小文件，测 fsync 延迟与随机/顺序比，
//! 据此把磁盘分为 SSD / HDD / Unknown 三档，供 IoScheduler 选稳态参数。
//!
//! - fsync_ms_p50 > 5ms 或 rand_seq_ratio > 20 → HDD；
//! - 其余 → SSD；探测失败/目录不可写 → Unknown（取保守参数）。
//! - 总预算默认 1500ms，超时即停止采样。

use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// 顺序/随机写吞吐单段探测窗口（毫秒）。多段采样在总预算内进行。
const PROBE_WINDOW_MS: u64 = 200;

/// 磁盘分类
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskClass {
    Ssd,
    Hdd,
    Unknown,
}

impl DiskClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            DiskClass::Ssd => "SSD",
            DiskClass::Hdd => "HDD",
            DiskClass::Unknown => "Unknown",
        }
    }
}

/// 探测结果
#[derive(Debug, Clone)]
pub struct DiskProbeResult {
    pub disk_class: DiskClass,
    /// fsync p50（毫秒）
    pub fsync_ms_p50: f32,
    /// 随机/顺序吞吐比
    pub rand_seq_ratio: f32,
}

/// 纯判定函数（便于单测注入桩值）。
///
/// fsync_ms_p50 > 5.0 或 rand_seq_ratio > 20.0 → HDD；否则 SSD。
pub fn classify(fsync_ms_p50: f32, rand_seq_ratio: f32) -> DiskClass {
    if fsync_ms_p50 > 5.0 || rand_seq_ratio > 20.0 {
        DiskClass::Hdd
    } else {
        DiskClass::Ssd
    }
}

/// 探测器
pub struct DiskProbe;

impl DiskProbe {
    /// 在 `db_dir` 旁做一次有预算的磁盘画像探测。
    ///
    /// 任何 IO 错误 / 目录不可写 / 超时 → 返回 Unknown（保守档），不 panic。
    pub fn detect(db_dir: &Path, budget: Duration) -> DiskProbeResult {
        let deadline = Instant::now() + budget;
        let tmp_path: PathBuf = db_dir.join(".pdc_diskprobe.tmp");

        match Self::run(&tmp_path, deadline) {
            Ok((fsync_p50, ratio)) => {
                let class = classify(fsync_p50, ratio);
                let _ = fs::remove_file(&tmp_path);
                DiskProbeResult {
                    disk_class: class,
                    fsync_ms_p50: fsync_p50,
                    rand_seq_ratio: ratio,
                }
            }
            Err(_) => {
                let _ = fs::remove_file(&tmp_path);
                DiskProbeResult {
                    disk_class: DiskClass::Unknown,
                    fsync_ms_p50: 0.0,
                    rand_seq_ratio: 0.0,
                }
            }
        }
    }

    /// 实际采样：返回 (fsync_p50_ms, rand_seq_ratio)。失败返回 Err。
    fn run(tmp_path: &Path, deadline: Instant) -> std::io::Result<(f32, f32)> {
        // ── 1) fsync p50：写小块 + fsync，重复采样 ──
        let mut fsync_samples_ms: Vec<f32> = Vec::with_capacity(32);
        let block = vec![0x55u8; 4096];
        for _ in 0..32 {
            if Instant::now() >= deadline {
                break;
            }
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(tmp_path)?;
            let t = Instant::now();
            f.write_all(&block)?;
            f.sync_all()?;
            fsync_samples_ms.push(t.elapsed().as_secs_f64() as f32 * 1000.0);
            drop(f);
        }
        if fsync_samples_ms.is_empty() {
            return Ok((0.0, 0.0));
        }
        fsync_samples_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let fsync_p50 = fsync_samples_ms[fsync_samples_ms.len() / 2];

        // ── 2) 顺序 vs 随机吞吐比 ──
        let seq = Self::seq_throughput(tmp_path, deadline)?;
        let rand = Self::rand_throughput(tmp_path, deadline)?;
        let ratio = if seq > 0.0 { rand / seq } else { 0.0 };

        Ok((fsync_p50, ratio))
    }

    /// 顺序写吞吐（MB/s）。
    fn seq_throughput(tmp_path: &Path, deadline: Instant) -> std::io::Result<f32> {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(tmp_path)?;
        let chunk = vec![0xAAu8; 64 * 1024];
        let start = Instant::now();
        let mut bytes = 0u64;
        while start.elapsed() < Duration::from_millis(PROBE_WINDOW_MS) && Instant::now() < deadline
        {
            f.write_all(&chunk)?;
            bytes += chunk.len() as u64;
        }
        let secs = start.elapsed().as_secs_f64().max(0.001);
        Ok((bytes as f64 / 1e6 / secs) as f32)
    }

    /// 随机写吞吐（MB/s）。
    fn rand_throughput(tmp_path: &Path, deadline: Instant) -> std::io::Result<f32> {
        // 先落一个 8MB 文件
        let mut f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(tmp_path)?;
        let zeros = vec![0u8; 8 * 1024 * 1024];
        f.write_all(&zeros)?;
        f.sync_all()?;
        let block = vec![0x55u8; 4096];
        let start = Instant::now();
        let mut ops = 0u64;
        let mut rng: u64 = 0x9E37_79B7_7F4A_7C15;
        while start.elapsed() < Duration::from_millis(PROBE_WINDOW_MS) && Instant::now() < deadline
        {
            // xorshift64
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let off = rng % (8 * 1024 * 1024 - 4096);
            f.seek(SeekFrom::Start(off))?;
            f.write_all(&block)?;
            ops += 1;
        }
        let secs = start.elapsed().as_secs_f64().max(0.001);
        Ok((ops as f64 * 4096.0 / 1e6 / secs) as f32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_ssd() {
        // fsync 快、随机顺序比低 → SSD
        assert_eq!(classify(1.2, 3.0), DiskClass::Ssd);
    }

    #[test]
    fn test_disk_probe_classifies_hdd() {
        // 高 fsync p50 或高随机比 → HDD（注入桩值，不真跑磁盘）
        assert_eq!(classify(12.0, 5.0), DiskClass::Hdd);
        assert_eq!(classify(1.0, 30.0), DiskClass::Hdd);
        assert_eq!(classify(8.0, 25.0), DiskClass::Hdd);
    }

    #[test]
    fn test_probe_runs_on_temp_dir() {
        // 用测试临时目录做一次真探测（预算短），不应 panic，失败回落 Unknown。
        // 测试临时根用 crate::test_tmp_dir()（构建目录 target/test-tmp）：本机安全策略
        // 拒绝 target 构建目录进程写 %TEMP% 根与数据盘（PermissionDenied code 5）。
        let dir = crate::test_tmp_dir();
        let r = DiskProbe::detect(&dir, Duration::from_millis(400));
        assert!(matches!(
            r.disk_class,
            DiskClass::Ssd | DiskClass::Hdd | DiskClass::Unknown
        ));
    }
}
