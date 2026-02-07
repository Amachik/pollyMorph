# PollyMorph Shadow Mode Runner - Windows PowerShell
# Usage: .\run_shadow.ps1
# 
# This script:
# 1. Sets IS_SHADOW_MODE=true environment variable
# 2. Builds release binary with native CPU optimizations
# 3. Pins the process to CPU core 1 for consistent latency testing

param(
    [int]$Core = 1,           # CPU core to pin (0-indexed, default: core 1)
    [switch]$SkipBuild,       # Skip cargo build if binary exists
    [switch]$Debug            # Run debug build instead of release
)

$ErrorActionPreference = "Stop"

# Colors for output
function Write-Step { param($msg) Write-Host "`n[*] $msg" -ForegroundColor Cyan }
function Write-Success { param($msg) Write-Host "[+] $msg" -ForegroundColor Green }
function Write-Warn { param($msg) Write-Host "[!] $msg" -ForegroundColor Yellow }
function Write-Err { param($msg) Write-Host "[-] $msg" -ForegroundColor Red }

# Banner
Write-Host @"

  ____       _ _       __  __                  _     
 |  _ \ ___ | | |_   _|  \/  | ___  _ __ _ __ | |__  
 | |_) / _ \| | | | | | |\/| |/ _ \| '__| '_ \| '_ \ 
 |  __/ (_) | | | |_| | |  | | (_) | |  | |_) | | | |
 |_|   \___/|_|_|\__, |_|  |_|\___/|_|  | .__/|_| |_|
                 |___/                  |_|          
                    SHADOW MODE RUNNER

"@ -ForegroundColor Magenta

# Step 1: Set environment variables
Write-Step "Setting environment variables..."
$env:IS_SHADOW_MODE = "true"
$env:RUST_LOG = "info,polymorph_hft=debug"
$env:RUST_BACKTRACE = "1"
Write-Success "IS_SHADOW_MODE=true"

# Step 2: Build release binary
if (-not $SkipBuild) {
    Write-Step "Building release binary with native CPU optimizations..."
    
    # Set RUSTFLAGS for native CPU optimizations (AVX2, etc.)
    $env:RUSTFLAGS = "-C target-cpu=native"
    
    $buildType = if ($Debug) { "" } else { "--release" }
    $buildCmd = "cargo build $buildType"
    
    Write-Host "Running: $buildCmd" -ForegroundColor DarkGray
    Write-Host "RUSTFLAGS: $env:RUSTFLAGS" -ForegroundColor DarkGray
    
    try {
        $buildResult = Invoke-Expression $buildCmd 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Err "Build failed!"
            $buildResult | Write-Host
            exit 1
        }
        Write-Success "Build completed successfully"
    }
    catch {
        Write-Err "Build error: $_"
        exit 1
    }
} else {
    Write-Warn "Skipping build (--SkipBuild flag set)"
}

# Step 3: Determine binary path
$binaryDir = if ($Debug) { "debug" } else { "release" }
$binaryPath = ".\target\$binaryDir\polymorph-hft.exe"

if (-not (Test-Path $binaryPath)) {
    Write-Err "Binary not found at $binaryPath"
    Write-Err "Please run without -SkipBuild flag"
    exit 1
}

Write-Success "Binary found: $binaryPath"

# Step 4: Calculate CPU affinity mask
# Core 0 = 0x1, Core 1 = 0x2, Core 2 = 0x4, etc.
$affinityMask = [math]::Pow(2, $Core)
$affinityHex = "0x{0:X}" -f [int]$affinityMask

Write-Step "Starting PollyMorph with CPU affinity..."
Write-Host "  Target Core: $Core" -ForegroundColor DarkGray
Write-Host "  Affinity Mask: $affinityHex" -ForegroundColor DarkGray

# Step 5: Start process with CPU affinity
Write-Host ""
Write-Host "=" * 60 -ForegroundColor DarkGray
Write-Host "  SHADOW MODE ACTIVE - No real trades will be executed" -ForegroundColor Yellow
Write-Host "  Metrics endpoint: http://localhost:9091/metrics" -ForegroundColor DarkGray
Write-Host "  Grafana dashboard: http://localhost:3000" -ForegroundColor DarkGray
Write-Host "=" * 60 -ForegroundColor DarkGray
Write-Host ""

try {
    # Start the process
    $processInfo = New-Object System.Diagnostics.ProcessStartInfo
    $processInfo.FileName = (Resolve-Path $binaryPath).Path
    $processInfo.UseShellExecute = $false
    $processInfo.RedirectStandardOutput = $false
    $processInfo.RedirectStandardError = $false
    $processInfo.WorkingDirectory = $PWD.Path
    
    # Pass through environment variables
    $processInfo.EnvironmentVariables["IS_SHADOW_MODE"] = "true"
    $processInfo.EnvironmentVariables["RUST_LOG"] = $env:RUST_LOG
    $processInfo.EnvironmentVariables["RUST_BACKTRACE"] = $env:RUST_BACKTRACE
    
    $process = [System.Diagnostics.Process]::Start($processInfo)
    
    # Set CPU affinity after process starts
    Start-Sleep -Milliseconds 100
    $process.ProcessorAffinity = [int]$affinityMask
    
    Write-Success "Process started with PID: $($process.Id)"
    Write-Success "CPU affinity set to core $Core (mask: $affinityHex)"
    
    # Wait for process to exit
    $process.WaitForExit()
    
    $exitCode = $process.ExitCode
    if ($exitCode -eq 0) {
        Write-Success "PollyMorph exited cleanly"
    } else {
        Write-Warn "PollyMorph exited with code: $exitCode"
    }
}
catch {
    Write-Err "Failed to start process: $_"
    exit 1
}
finally {
    # Cleanup
    if ($process -and -not $process.HasExited) {
        Write-Warn "Terminating process..."
        $process.Kill()
    }
}
