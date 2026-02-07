# PollyMorph - Latency Test to Polymarket CLOB API
# Tests TCP connect time and HTTP round-trip to Polymarket endpoints
# Usage: powershell -ExecutionPolicy Bypass -File scripts\test_latency.ps1

Write-Host "`n=== PollyMorph Latency Test ===" -ForegroundColor Cyan
Write-Host "Testing from: $env:COMPUTERNAME ($((Get-NetIPAddress -AddressFamily IPv4 | Where-Object {$_.InterfaceAlias -notlike '*Loopback*'} | Select-Object -First 1).IPAddress))"
Write-Host ""

# Resolve Polymarket API IPs
Write-Host "--- DNS Resolution ---" -ForegroundColor Yellow
$restHost = "clob.polymarket.com"
$wsHost = "ws-subscriptions-clob.polymarket.com"

try {
    $restIPs = [System.Net.Dns]::GetHostAddresses($restHost)
    Write-Host "REST API ($restHost): $($restIPs -join ', ')"
} catch {
    Write-Host "Failed to resolve $restHost" -ForegroundColor Red
}

try {
    $wsIPs = [System.Net.Dns]::GetHostAddresses($wsHost)
    Write-Host "WS API   ($wsHost): $($wsIPs -join ', ')"
} catch {
    Write-Host "Failed to resolve $wsHost" -ForegroundColor Red
}
Write-Host ""

# TCP Connect latency (raw socket connect time)
Write-Host "--- TCP Connect Latency (10 samples) ---" -ForegroundColor Yellow
$tcpTimes = @()
for ($i = 1; $i -le 10; $i++) {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $tcp.Connect($restHost, 443)
        $sw.Stop()
        $tcp.Close()
        $ms = [math]::Round($sw.Elapsed.TotalMilliseconds, 2)
        $tcpTimes += $ms
        Write-Host "  [$i/10] TCP connect: ${ms}ms"
    } catch {
        $sw.Stop()
        Write-Host "  [$i/10] TCP connect FAILED" -ForegroundColor Red
    }
    Start-Sleep -Milliseconds 100
}
if ($tcpTimes.Count -gt 0) {
    $avg = [math]::Round(($tcpTimes | Measure-Object -Average).Average, 2)
    $min = [math]::Round(($tcpTimes | Measure-Object -Minimum).Minimum, 2)
    $max = [math]::Round(($tcpTimes | Measure-Object -Maximum).Maximum, 2)
    Write-Host "  TCP Summary: avg=${avg}ms  min=${min}ms  max=${max}ms" -ForegroundColor Green
}
Write-Host ""

# HTTPS round-trip (full TLS handshake + HTTP GET)
Write-Host "--- HTTPS Round-Trip (GET /time, 10 samples) ---" -ForegroundColor Yellow
$httpTimes = @()
for ($i = 1; $i -le 10; $i++) {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $resp = Invoke-WebRequest -Uri "https://clob.polymarket.com/time" -Method GET -UseBasicParsing -TimeoutSec 5
        $sw.Stop()
        $ms = [math]::Round($sw.Elapsed.TotalMilliseconds, 2)
        $httpTimes += $ms
        Write-Host "  [$i/10] HTTPS GET /time: ${ms}ms (status $($resp.StatusCode))"
    } catch {
        $sw.Stop()
        $ms = [math]::Round($sw.Elapsed.TotalMilliseconds, 2)
        Write-Host "  [$i/10] HTTPS GET /time: ${ms}ms (error: $($_.Exception.Message))" -ForegroundColor Yellow
        $httpTimes += $ms
    }
    Start-Sleep -Milliseconds 200
}
if ($httpTimes.Count -gt 0) {
    $avg = [math]::Round(($httpTimes | Measure-Object -Average).Average, 2)
    $min = [math]::Round(($httpTimes | Measure-Object -Minimum).Minimum, 2)
    $max = [math]::Round(($httpTimes | Measure-Object -Maximum).Maximum, 2)
    Write-Host "  HTTPS Summary: avg=${avg}ms  min=${min}ms  max=${max}ms" -ForegroundColor Green
}
Write-Host ""

# ICMP Ping (if not blocked)
Write-Host "--- ICMP Ping ---" -ForegroundColor Yellow
try {
    $ping = Test-Connection -ComputerName $restHost -Count 5 -ErrorAction Stop
    $pingAvg = [math]::Round(($ping.Latency | Measure-Object -Average).Average, 2)
    $pingMin = ($ping.Latency | Measure-Object -Minimum).Minimum
    $pingMax = ($ping.Latency | Measure-Object -Maximum).Maximum
    Write-Host "  Ping Summary: avg=${pingAvg}ms  min=${pingMin}ms  max=${pingMax}ms" -ForegroundColor Green
} catch {
    Write-Host "  ICMP ping blocked or failed (normal for Cloudflare)" -ForegroundColor Yellow
}
Write-Host ""

# Traceroute (limited)
Write-Host "--- Traceroute (first 15 hops) ---" -ForegroundColor Yellow
Write-Host "  Running... (this takes ~15 seconds)"
try {
    $trace = Test-Connection -ComputerName $restHost -Traceroute -ErrorAction Stop
    foreach ($hop in $trace) {
        if ($hop.Hop -le 15) {
            Write-Host "  Hop $($hop.Hop): $($hop.Latency)ms -> $($hop.Address)"
        }
    }
} catch {
    Write-Host "  Traceroute failed or blocked" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=== Latency Guidelines ===" -ForegroundColor Cyan
Write-Host "  < 20ms  = Excellent (US East coast / co-located VPS)"
Write-Host "  20-50ms = Good (US other regions)"
Write-Host "  50-100ms = OK (US West / Canada)"
Write-Host "  100-200ms = Poor (Europe)"
Write-Host "  > 200ms = Bad (Asia/Oceania) - consider a VPS"
Write-Host ""
