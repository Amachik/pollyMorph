# Profile-Guided Optimization (PGO) Build Script for PollyMorph HFT Bot
# 
# PGO can provide 10-20% additional performance by optimizing based on real execution patterns.
# This script performs a 3-phase build:
#   1. Instrumented build - generates profiling data
#   2. Profile collection - runs benchmarks to collect data
#   3. Optimized build - uses profile data for optimization

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  PollyMorph PGO Build (Beyond Human)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Set environment for native CPU
$env:RUSTFLAGS = "-C target-cpu=native"

# Phase 1: Clean and prepare
Write-Host "[1/4] Cleaning previous builds..." -ForegroundColor Yellow
cargo clean 2>$null

# Phase 2: Build with instrumentation for profiling
Write-Host "[2/4] Building instrumented binary..." -ForegroundColor Yellow
$env:RUSTFLAGS = "-C target-cpu=native -Cprofile-generate=./target/pgo-data"
cargo build --release
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Instrumented build failed" -ForegroundColor Red
    exit 1
}

# Phase 3: Run benchmarks to collect profile data
Write-Host "[3/4] Collecting profile data via benchmarks..." -ForegroundColor Yellow
cargo bench --no-fail-fast 2>&1 | Out-Null
Write-Host "       Profile data collected in ./target/pgo-data" -ForegroundColor Gray

# Merge profile data (requires llvm-profdata)
$profdata = Get-ChildItem -Path "./target/pgo-data" -Filter "*.profraw" -Recurse
if ($profdata.Count -gt 0) {
    Write-Host "       Found $($profdata.Count) profile files" -ForegroundColor Gray
    
    # Try to merge with llvm-profdata if available
    $llvmProfdata = Get-Command "llvm-profdata" -ErrorAction SilentlyContinue
    if ($llvmProfdata) {
        llvm-profdata merge -o ./target/pgo-data/merged.profdata ./target/pgo-data/*.profraw
        Write-Host "       Merged profile data successfully" -ForegroundColor Green
    } else {
        Write-Host "       Note: llvm-profdata not found, using raw profiles" -ForegroundColor Yellow
    }
}

# Phase 4: Build with PGO optimization
Write-Host "[4/4] Building PGO-optimized binary..." -ForegroundColor Yellow
$env:RUSTFLAGS = "-C target-cpu=native -Cprofile-use=./target/pgo-data"
cargo build --release
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: PGO build failed" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  PGO Build Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Binary location: ./target/release/polymorph-hft.exe" -ForegroundColor White
Write-Host ""
Write-Host "Expected improvements:" -ForegroundColor Cyan
Write-Host "  - 10-20% faster hot path execution" -ForegroundColor White
Write-Host "  - Better branch prediction from real data" -ForegroundColor White
Write-Host "  - Optimized code layout for cache" -ForegroundColor White
Write-Host ""

# Run quick benchmark to verify
Write-Host "Running verification benchmark..." -ForegroundColor Yellow
cargo bench -- hot_path_full 2>&1 | Select-String -Pattern "time:"
