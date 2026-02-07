# PollyMorph Shadow Mode - Local Monitoring Stack

High-performance testing environment for PollyMorph HFT bot in Shadow Mode.

## Quick Start

```powershell
# 1. Start monitoring stack
cd docker
docker-compose up -d

# 2. Run PollyMorph in Shadow Mode (from project root)
cd ..
.\run_shadow.ps1
```

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Prometheus | http://localhost:9090 | - |
| Grafana | http://localhost:3000 | admin / polymorph |
| PollyMorph Metrics | http://localhost:9091/metrics | - |

## Grafana Dashboard

The pre-configured dashboard includes:

- **Shadow PnL** - Real-time virtual profit/loss gauge
- **Maker vs Taker Ratio** - Pie chart of fill types
- **Tick-to-Trade Latency** - p50/p99/p99.9 latency percentiles
- **Latency Heatmap** - Distribution of latencies over time
- **Volume Spike Status** - Circuit breaker indicator
- **Gap Protection Status** - Price gap halt indicator
- **Virtual Fill Rate** - Fills per minute
- **Volume Velocity** - 1s vs 5m average volume
- **Tuner Metrics** - Auto-tuner optimization stats

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Host Machine                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │  PollyMorph HFT Bot (Shadow Mode)               │   │
│  │  - Pinned to CPU Core 1                         │   │
│  │  - Metrics on :9091                             │   │
│  └─────────────────────────────────────────────────┘   │
│                         │                               │
│                         │ scrape                        │
│                         ▼                               │
│  ┌─────────────────────────────────────────────────┐   │
│  │  Docker Network                                 │   │
│  │  ┌─────────────┐     ┌─────────────┐           │   │
│  │  │ Prometheus  │────▶│  Grafana    │           │   │
│  │  │ :9090       │     │  :3000      │           │   │
│  │  └─────────────┘     └─────────────┘           │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

## CPU Affinity Testing

The `run_shadow.ps1` script pins PollyMorph to a specific CPU core:

```powershell
# Pin to core 1 (default)
.\run_shadow.ps1

# Pin to core 2
.\run_shadow.ps1 -Core 2

# Skip rebuild
.\run_shadow.ps1 -SkipBuild
```

On Linux:
```bash
# Pin to core 1 (default)
./run_shadow.sh

# Pin to core 2
./run_shadow.sh 2
```

## Verification

1. Check Prometheus targets: http://localhost:9090/targets
2. Verify `polymorph` target is UP
3. Open Grafana dashboard: http://localhost:3000
4. Look for "VIRTUAL FILL" log messages in bot output

## Troubleshooting

### Prometheus can't reach bot
- Ensure `host.docker.internal` resolves (Docker Desktop feature)
- On Linux, you may need `--add-host=host.docker.internal:host-gateway`

### No metrics appearing
- Check bot is running: `curl http://localhost:9091/metrics`
- Verify IS_SHADOW_MODE=true is set

### High latency variance
- Disable CPU frequency scaling: `cpupower frequency-set -g performance`
- Check for other processes on the pinned core
- Consider isolating CPU cores with `isolcpus` kernel parameter

## Cleanup

```bash
docker-compose down -v  # Remove containers and volumes
```
