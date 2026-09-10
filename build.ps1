# PDC 编译脚本 - 自动嵌入版本号到文件名
# 用法: .\build.ps1

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  PDC 编译脚本 (带版本号文件名)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# 1. 获取版本号和 git hash
$version = (Select-String -Path "Cargo.toml" -Pattern '^version\s*=\s*"([^"]+)"').Matches.Groups[1].Value
$gitHash = (& git rev-parse --short HEAD).Trim()
$outputName = "pdc-v$version-$gitHash.exe"

Write-Host ""
Write-Host "版本号: $version" -ForegroundColor Green
Write-Host "Git Hash: $gitHash" -ForegroundColor Green
Write-Host "输出文件名: $outputName" -ForegroundColor Green
Write-Host ""

# 2. 停止正在运行的 pdc（避免文件被占用）
$running = Get-Process -Name "pdc" -ErrorAction SilentlyContinue
if ($running) {
    Write-Host "检测到正在运行的 pdc 进程，正在停止..." -ForegroundColor Yellow
    Stop-Process -Name "pdc" -Force
    Start-Sleep -Seconds 2
}

# 3. 编译
Write-Host "开始编译 (release)..." -ForegroundColor Cyan
cargo build --release
if ($LASTEXITCODE -ne 0) {
    Write-Host "编译失败！" -ForegroundColor Red
    exit 1
}

# 4. 重命名输出文件
$src = "target\release\pdc.exe"
$dst = "target\release\$outputName"

if (Test-Path $dst) {
    Remove-Item $dst -Force
}
Rename-Item -Path $src -NewName $outputName

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  编译完成！" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "输出文件: $dst" -ForegroundColor Green
Write-Host "文件大小: $([math]::Round((Get-Item $dst).Length / 1MB, 2)) MB" -ForegroundColor Green
Write-Host ""

# 5. 验证版本号
Write-Host "验证版本号:" -ForegroundColor Cyan
& $dst --version
Write-Host ""
