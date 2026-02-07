# PollyMorph - High-Frequency Trading Bot for Polymarket

A low-latency arbitrage and market-making bot for Polymarket's Central Limit Order Book (CLOB), built in Rust with focus on performance and reliability.

## Features

- **Sub-5ms Order Execution**: Pre-signed EIP-712 orders with cached signatures
- **Zero Heap Allocations**: Hot path uses stack-allocated structures and lock-free atomics
- **Cross-Exchange Arbitrage**: Real-time price monitoring from Binance/Coinbase
- **Fee-Aware Execution**: Accounts for 2026 dynamic taker fees (up to 3.15%)
- **Maker Rebate Strategy**: Place limit orders to earn maker rebates instead of paying taker fees
- **Kill Switch**: Automatic halt when daily losses exceed 5% of bankroll
- **SIMD JSON Parsing**: Ultra-fast WebSocket message processing with `simd-json`

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Polymarket WS  │     │   Binance WS    │     │  Coinbase WS    │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │    Event Processor      │  ← HOT PATH
                    │   (simd-json parsing)   │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │    Pricing Engine       │  ← HOT PATH
                    │  (Arbitrage Detection)  │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │    Risk Manager         │
                    │    (Kill Switch)        │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Order Executor        │  ← HOT PATH
                    │  (Pre-signed EIP-712)   │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Polymarket CLOB       │
                    │      (REST API)         │
                    └─────────────────────────┘
```

## Requirements

- **Rust 1.75+** (for stable async traits)
- **Linux VPS** in AWS us-east-1 (recommended for minimum latency)
- **Polygon Wallet** with USDC for trading
- **Polymarket API Keys**

## Quick Start

### 1. Clone and Build

```bash
git clone https://github.com/your-org/pollymorph.git
cd pollymorph

# Build with release optimizations
cargo build --release
```

### 2. Configure

```bash
# Copy example configuration
cp .env.example .env
cp config/default.toml config/local.toml

# Edit with your credentials
nano .env
nano config/local.toml
```

### 3. Run

```bash
# Run in production mode
RUST_LOG=info ./target/release/polymorph-hft

# Run with debug logging
RUST_LOG=debug cargo run --release
```

## Configuration

### Environment Variables

| Variable | Description |
|----------|-------------|
| `POLYMARKET_API_KEY` | Your Polymarket API key |
| `POLYMARKET_API_SECRET` | Your Polymarket API secret |
| `POLYMARKET_PRIVATE_KEY` | Wallet private key (hex, no 0x) |
| `POLYGON_RPC_URL` | Polygon RPC endpoint |
| `RUST_LOG` | Logging level |

### Trading Parameters

```toml
[trading]
max_taker_fee_bps = 315      # 3.15% max taker fee
slippage_bps = 10            # 0.10% slippage buffer
min_profit_bps = 50          # 0.50% minimum profit
default_order_size = "100"   # $100 per order
maker_mode_enabled = true    # Enable maker strategy
taker_mode_enabled = true    # Enable taker/arb strategy
```

### Risk Parameters

```toml
[risk]
daily_bankroll = "100000"           # $100,000 daily budget
kill_switch_threshold_pct = "5"     # Stop at 5% loss
max_position_per_market = "10000"   # $10k max per market
max_total_exposure = "50000"        # $50k total exposure
loss_cooldown_ms = 60000            # 1 min cooldown after loss
```

## Profit Calculation

The bot only executes trades when:

```
Expected Profit > Taker Fee + Slippage + Gas
```

For a typical trade:
- **Taker Fee**: 3.15% (315 bps)
- **Slippage**: 0.10% (10 bps)
- **Gas**: ~$0.05 (~5 bps on $100)
- **Minimum Spread**: 3.30% + profit margin

### Maker Strategy

When spread is wide, the bot places limit orders to earn maker rebates:
- Places orders at `best_bid + offset` and `best_ask - offset`
- Earns ~10 bps rebate instead of paying 315 bps fee
- Requires patience but significantly more profitable

## Performance Tuning

### Latency Optimization

1. **Colocation**: Deploy in AWS us-east-1 for ~1ms to Polymarket
2. **Single Thread**: Uses `tokio::main(flavor = "current_thread")` for predictable latency
3. **Pre-signing**: Orders are signed ahead of time, submission is just HTTP POST
4. **SIMD JSON**: `simd-json` for 2-4x faster JSON parsing
5. **Lock-free**: Atomic operations in hot path, `RwLock` only for cold path

### Memory Optimization

- Fixed-size order book snapshots (stack-allocated)
- Arena allocator for temporary data
- No heap allocations in trading loop
- Pre-allocated channel buffers

## Monitoring

The bot logs metrics every 60 seconds:

```
📊 Metrics Update:
  P&L: $1,234.56
  Exposure: $25,000.00
  Trades: 142
  Positions: 5
```

### CPU Pinning

Usage:
Default behavior: Pins to the last CPU core (typically has less OS interrupt traffic)
Specify core via environment variable:
bash
TRADING_CORE_ID=2 ./polymorph-hft
For production (Linux) - Isolate cores from the kernel scheduler:
bash
# Add to /etc/default/grub:
GRUB_CMDLINE_LINUX="isolcpus=2,3 nohz_full=2,3 rcu_nocbs=2,3"
 
# Then:
sudo update-grub && reboot
 
# Run bot on isolated core:
TRADING_CORE_ID=2 ./polymorph-hft
Why This Matters:
Without pinning: Linux scheduler moves threads between cores → 10-20ms latency spikes
With pinning: Thread stays on one core → L1/L2/L3 caches stay hot, no TLB flushes
The isolcpus kernel parameter prevents any other process from using those cores, giving your trading thread exclusive access.










### Kill Switch

The kill switch automatically triggers when:
- Daily losses exceed 5% of bankroll
- Manual trigger via API/signal

When triggered:
- All new orders are blocked
- Open orders remain (manual cancel recommended)
- Requires manual reset after review

## Project Structure

```
pollymorph/
├── Cargo.toml           # Dependencies
├── config/
│   └── default.toml     # Default configuration
├── src/
│   ├── main.rs          # Entry point and task orchestration
│   ├── config.rs        # Configuration loading
│   ├── types.rs         # Zero-copy data structures
│   ├── websocket.rs     # WebSocket handlers
│   ├── signing.rs       # EIP-712 order signing
│   ├── pricing.rs       # Pricing engine and arb detection
│   ├── risk.rs          # Risk management and kill switch
│   └── execution.rs     # Order execution
└── benches/
    └── hot_path.rs      # Latency benchmarks
```

## Safety Disclaimer

⚠️ **USE AT YOUR OWN RISK**

This software is provided for educational purposes. Trading prediction markets involves significant financial risk. The authors are not responsible for any losses incurred.

- Always start with small amounts
- Test thoroughly on testnet first
- Monitor the bot continuously
- Have manual intervention procedures ready

## License

MIT License - See LICENSE file for details.
