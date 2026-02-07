# PollyMorph - Deploy to Oracle Cloud VPS
# Usage: powershell -ExecutionPolicy Bypass -File scripts\deploy.ps1
#
# Prerequisites:
#   1. Set VPS_HOST below (or pass as env var)
#   2. Set VPS_KEY to path of your SSH private key
#   3. Run vps_bootstrap.sh on VPS first

param(
    [string]$VpsHost = $env:VPS_HOST,
    [string]$VpsKey = $env:VPS_KEY,
    [string]$VpsUser = "ubuntu"
)

if (-not $VpsHost) {
    Write-Host "ERROR: Set VPS_HOST environment variable or pass -VpsHost parameter" -ForegroundColor Red
    Write-Host "  Example: `$env:VPS_HOST = '123.45.67.89'" -ForegroundColor Yellow
    Write-Host "  Or: .\scripts\deploy.ps1 -VpsHost 123.45.67.89 -VpsKey C:\path\to\key.pem"
    exit 1
}

$sshTarget = "${VpsUser}@${VpsHost}"
$sshArgs = @()
if ($VpsKey) {
    $sshArgs = @("-i", $VpsKey)
}

Write-Host "=== PollyMorph Deploy ===" -ForegroundColor Cyan
Write-Host "Target: $sshTarget"
Write-Host ""

# Step 1: Sync source code via scp (since rsync may not be on Windows)
Write-Host "--- Syncing source code ---" -ForegroundColor Yellow

# Create a temp archive of the source
$archiveName = "polymorph-deploy.tar.gz"
Write-Host "  Creating archive..."
$filesToInclude = @(
    "Cargo.toml",
    "Cargo.lock",
    "src/",
    "config/",
    "docker/grafana_dashboard.json",
    "scripts/",
    ".cargo/",
    "benches/",
    "tests/",
    ".env.example"
)

# Use tar to create archive (available on Windows 10+)
$includeArgs = $filesToInclude | ForEach-Object { "`"$_`"" }
$tarCmd = "tar czf $archiveName $($includeArgs -join ' ')"
Invoke-Expression $tarCmd
Write-Host "  Archive created: $archiveName"

# Upload archive
Write-Host "  Uploading to VPS..."
$scpArgs = @($sshArgs) + @("$archiveName", "${sshTarget}:~/polymorph-deploy.tar.gz")
& scp @scpArgs
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: scp failed" -ForegroundColor Red
    Remove-Item $archiveName -ErrorAction SilentlyContinue
    exit 1
}

# Extract on VPS
Write-Host "  Extracting on VPS..."
$extractCmd = "mkdir -p ~/PollyMorph && cd ~/PollyMorph && tar xzf ~/polymorph-deploy.tar.gz && rm ~/polymorph-deploy.tar.gz"
& ssh @sshArgs $sshTarget $extractCmd

# Clean up local archive
Remove-Item $archiveName -ErrorAction SilentlyContinue
Write-Host "  Source synced!" -ForegroundColor Green

# Step 2: Check if .env exists on VPS
Write-Host ""
Write-Host "--- Checking .env ---" -ForegroundColor Yellow
$envCheck = & ssh @sshArgs $sshTarget "test -f ~/PollyMorph/.env && echo 'EXISTS' || echo 'MISSING'"
if ($envCheck -match "MISSING") {
    Write-Host "  WARNING: .env not found on VPS!" -ForegroundColor Red
    Write-Host "  Create it with: ssh $sshTarget 'nano ~/PollyMorph/.env'" -ForegroundColor Yellow
    Write-Host "  (Copy values from your local .env)" -ForegroundColor Yellow
} else {
    Write-Host "  .env found on VPS" -ForegroundColor Green
}

# Step 3: Build on VPS
Write-Host ""
Write-Host "--- Building on VPS (this may take a few minutes on first build) ---" -ForegroundColor Yellow
& ssh @sshArgs $sshTarget "source ~/.cargo/env && cd ~/PollyMorph && cargo build --release 2>&1 | tail -5"
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Build failed on VPS" -ForegroundColor Red
    exit 1
}
Write-Host "  Build complete!" -ForegroundColor Green

# Step 4: Restart service
Write-Host ""
Write-Host "--- Restarting PollyMorph service ---" -ForegroundColor Yellow
& ssh @sshArgs $sshTarget "sudo systemctl restart polymorph && sudo systemctl restart prometheus"
Start-Sleep -Seconds 2
$status = & ssh @sshArgs $sshTarget "systemctl is-active polymorph"
if ($status -match "active") {
    Write-Host "  PollyMorph service: RUNNING" -ForegroundColor Green
} else {
    Write-Host "  PollyMorph service: $status" -ForegroundColor Red
    Write-Host "  Check logs: ssh $sshTarget 'journalctl -u polymorph -n 50'" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=== Deploy Complete ===" -ForegroundColor Cyan
Write-Host "  Grafana:    http://${VpsHost}:3000" -ForegroundColor Green
Write-Host "  Prometheus: http://${VpsHost}:9091" -ForegroundColor Green
Write-Host "  Bot logs:   ssh $sshTarget 'journalctl -u polymorph -f'" -ForegroundColor Green
Write-Host ""
