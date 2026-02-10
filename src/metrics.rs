//! Prometheus metrics module for PollyMorph HFT Bot
//! Exposes /metrics endpoint for monitoring tick-to-trade latency, volume, and health
//! Uses quanta crate for TSC-based high-precision timing

use lazy_static::lazy_static;
use prometheus::{
    self, Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec,
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder, Encoder,
};
pub use quanta::{Clock, Instant as QuantaInstant};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use warp::Filter;
use tracing::{info, error};

lazy_static! {
    /// Global high-precision clock using TSC (Time Stamp Counter)
    /// Provides sub-nanosecond resolution for latency measurement
    pub static ref TSC_CLOCK: Clock = Clock::new();
}

lazy_static! {
    /// Global Prometheus registry
    pub static ref REGISTRY: Registry = Registry::new();
    
    // ============================================================================
    // LATENCY METRICS - Tick-to-Trade tracking
    // ============================================================================
    
    /// Histogram for tick-to-trade latency (WebSocket message received to order ack)
    /// Buckets optimized for HFT: 100µs to 100ms
    pub static ref TICK_TO_TRADE_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "polymorph_tick_to_trade_latency_seconds",
            "Latency from market data tick to trade execution acknowledgment"
        )
        .buckets(vec![
            0.0001,  // 100µs
            0.0002,  // 200µs
            0.0005,  // 500µs
            0.001,   // 1ms
            0.002,   // 2ms
            0.005,   // 5ms - target threshold
            0.010,   // 10ms
            0.020,   // 20ms
            0.050,   // 50ms
            0.100,   // 100ms
            0.500,   // 500ms - something is wrong
            1.0,     // 1s - critical
        ]),
        &["exchange", "side", "order_type"]
    ).expect("Failed to create tick_to_trade_latency histogram");
    
    /// Histogram for WebSocket message processing latency
    pub static ref WS_PROCESSING_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "polymorph_ws_processing_latency_seconds",
            "Latency to process incoming WebSocket messages"
        )
        .buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01]),
        &["source"]
    ).expect("Failed to create ws_processing_latency histogram");
    
    /// Histogram for order signing latency
    pub static ref ORDER_SIGNING_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "polymorph_order_signing_latency_seconds",
            "Latency to sign EIP-712 orders"
        )
        .buckets(vec![0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.05])
    ).expect("Failed to create order_signing_latency histogram");
    
    // ============================================================================
    // VOLUME METRICS - Trading activity tracking
    // ============================================================================
    
    /// Counter for total USDC volume traded
    pub static ref TOTAL_USDC_TRADED: Counter = Counter::with_opts(
        Opts::new(
            "polymorph_total_usdc_traded",
            "Total USDC volume traded"
        )
    ).expect("Failed to create total_usdc_traded counter");
    
    /// Counter for USDC volume by side (buy/sell)
    pub static ref USDC_TRADED_BY_SIDE: CounterVec = CounterVec::new(
        Opts::new(
            "polymorph_usdc_traded_by_side",
            "USDC volume traded by side"
        ),
        &["side"]
    ).expect("Failed to create usdc_traded_by_side counter");
    
    /// Counter for maker trades executed
    pub static ref MAKER_TRADES_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new(
            "polymorph_maker_trades_total",
            "Total number of maker trades executed"
        )
    ).expect("Failed to create maker_trades_total counter");
    
    /// Counter for taker trades executed
    pub static ref TAKER_TRADES_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new(
            "polymorph_taker_trades_total",
            "Total number of taker trades executed"
        )
    ).expect("Failed to create taker_trades_total counter");
    
    /// Counter for trades by type (maker/taker) and outcome
    pub static ref TRADES_BY_TYPE: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "polymorph_trades_by_type",
            "Trades executed by type and outcome"
        ),
        &["trade_type", "outcome"]
    ).expect("Failed to create trades_by_type counter");
    
    /// Counter for orders submitted
    pub static ref ORDERS_SUBMITTED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "polymorph_orders_submitted_total",
            "Total orders submitted by status"
        ),
        &["status"]
    ).expect("Failed to create orders_submitted counter");
    
    // ============================================================================
    // HEALTH METRICS - System health and risk tracking
    // ============================================================================
    
    /// Gauge for current bankroll in USDC
    pub static ref CURRENT_BANKROLL: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_current_bankroll_usdc",
            "Current bankroll in USDC"
        )
    ).expect("Failed to create current_bankroll gauge");
    
    /// Gauge for daily P&L in USDC
    pub static ref DAILY_PNL: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_daily_pnl_usdc",
            "Daily profit and loss in USDC"
        )
    ).expect("Failed to create daily_pnl gauge");
    
    /// Gauge for total exposure across all markets
    pub static ref TOTAL_EXPOSURE: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_total_exposure_usdc",
            "Total exposure across all markets in USDC"
        )
    ).expect("Failed to create total_exposure gauge");
    
    /// Gauge for kill switch status (1 = triggered, 0 = armed)
    pub static ref KILL_SWITCH_STATUS: IntGauge = IntGauge::with_opts(
        Opts::new(
            "polymorph_kill_switch_triggered",
            "Kill switch status: 1 if triggered, 0 if armed"
        )
    ).expect("Failed to create kill_switch_status gauge");
    
    /// Gauge for number of active positions
    pub static ref ACTIVE_POSITIONS: IntGauge = IntGauge::with_opts(
        Opts::new(
            "polymorph_active_positions",
            "Number of active positions"
        )
    ).expect("Failed to create active_positions gauge");
    
    /// Gauge for WebSocket connection status by source
    pub static ref WS_CONNECTION_STATUS: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "polymorph_ws_connection_status",
            "WebSocket connection status: 1 if connected, 0 if disconnected"
        ),
        &["source"]
    ).expect("Failed to create ws_connection_status gauge");
    
    // ============================================================================
    // PROFIT METRICS - For real-time profit tracking
    // ============================================================================
    
    /// Gauge for realized profit by market
    pub static ref REALIZED_PROFIT_BY_MARKET: GaugeVec = GaugeVec::new(
        Opts::new(
            "polymorph_realized_profit_usdc",
            "Realized profit in USDC by market"
        ),
        &["market_id"]
    ).expect("Failed to create realized_profit_by_market gauge");
    
    /// Gauge for unrealized P&L by market
    pub static ref UNREALIZED_PNL_BY_MARKET: GaugeVec = GaugeVec::new(
        Opts::new(
            "polymorph_unrealized_pnl_usdc",
            "Unrealized P&L in USDC by market"
        ),
        &["market_id"]
    ).expect("Failed to create unrealized_pnl_by_market gauge");
    
    /// Counter for fees paid
    pub static ref FEES_PAID: Counter = Counter::with_opts(
        Opts::new(
            "polymorph_fees_paid_usdc",
            "Total fees paid in USDC"
        )
    ).expect("Failed to create fees_paid counter");
    
    /// Counter for rebates earned
    pub static ref REBATES_EARNED: Counter = Counter::with_opts(
        Opts::new(
            "polymorph_rebates_earned_usdc",
            "Total maker rebates earned in USDC"
        )
    ).expect("Failed to create rebates_earned counter");
    
    // ============================================================================
    // ARBITRAGE METRICS — Full lifecycle tracking for arb mode
    // ============================================================================

    // --- Discovery & Opportunity Detection ---

    /// Counter for arb opportunities detected, by asset (BTC/ETH) and source (ws/rest)
    pub static ref ARB_OPPORTUNITIES_DETECTED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "polymorph_arb_opportunities_detected_total",
            "Total arbitrage opportunities detected"
        ),
        &["asset", "source"]
    ).expect("Failed to create arb_opportunities_detected counter");

    /// Counter for arb opportunities executed (orders submitted)
    pub static ref ARB_OPPORTUNITIES_EXECUTED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "polymorph_arb_opportunities_executed_total",
            "Total arbitrage opportunities where orders were submitted"
        ),
        &["asset"]
    ).expect("Failed to create arb_opportunities_executed counter");

    /// Counter for arb opportunities skipped (insufficient capital, already executing, etc.)
    pub static ref ARB_OPPORTUNITIES_SKIPPED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "polymorph_arb_opportunities_skipped_total",
            "Total arb opportunities skipped by reason"
        ),
        &["reason"]
    ).expect("Failed to create arb_opportunities_skipped counter");

    /// Gauge for tracked markets count
    pub static ref ARB_TRACKED_MARKETS: IntGauge = IntGauge::with_opts(
        Opts::new(
            "polymorph_arb_tracked_markets",
            "Number of markets currently tracked by arb engine"
        )
    ).expect("Failed to create arb_tracked_markets gauge");

    /// Gauge for WS-subscribed asset count
    pub static ref ARB_WS_SUBSCRIBED_ASSETS: IntGauge = IntGauge::with_opts(
        Opts::new(
            "polymorph_arb_ws_subscribed_assets",
            "Number of asset IDs subscribed on WebSocket"
        )
    ).expect("Failed to create arb_ws_subscribed_assets gauge");

    // --- Spread & Book Quality ---

    /// Gauge for best pair cost (up_ask + down_ask) per asset
    pub static ref ARB_BEST_PAIR_COST: GaugeVec = GaugeVec::new(
        Opts::new(
            "polymorph_arb_best_pair_cost",
            "Best available pair cost (up_best_ask + down_best_ask)"
        ),
        &["asset"]
    ).expect("Failed to create arb_best_pair_cost gauge");

    /// Gauge for spread percentage per asset: (1 - pair_cost) * 100
    pub static ref ARB_SPREAD_PCT: GaugeVec = GaugeVec::new(
        Opts::new(
            "polymorph_arb_spread_pct",
            "Current arbitrage spread percentage"
        ),
        &["asset"]
    ).expect("Failed to create arb_spread_pct gauge");

    /// Gauge for available pairs at current spread
    pub static ref ARB_AVAILABLE_PAIRS: GaugeVec = GaugeVec::new(
        Opts::new(
            "polymorph_arb_available_pairs",
            "Number of profitable pairs available in the order book"
        ),
        &["asset"]
    ).expect("Failed to create arb_available_pairs gauge");

    /// Histogram for book depth (total ask liquidity) per side
    pub static ref ARB_BOOK_DEPTH: GaugeVec = GaugeVec::new(
        Opts::new(
            "polymorph_arb_book_depth_tokens",
            "Total ask-side depth in tokens"
        ),
        &["asset", "side"]
    ).expect("Failed to create arb_book_depth gauge");

    // --- Execution ---

    /// Counter for orders submitted by side (up/down)
    pub static ref ARB_ORDERS_SUBMITTED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "polymorph_arb_orders_submitted_total",
            "Total arb orders submitted"
        ),
        &["asset", "side"]
    ).expect("Failed to create arb_orders_submitted counter");

    /// Counter for orders filled (got tokens)
    pub static ref ARB_ORDERS_FILLED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "polymorph_arb_orders_filled_total",
            "Total arb orders that received tokens"
        ),
        &["asset", "side"]
    ).expect("Failed to create arb_orders_filled counter");

    /// Counter for orders failed
    pub static ref ARB_ORDERS_FAILED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "polymorph_arb_orders_failed_total",
            "Total arb orders that failed"
        ),
        &["asset", "side"]
    ).expect("Failed to create arb_orders_failed counter");

    /// Counter for tokens bought by side
    pub static ref ARB_TOKENS_BOUGHT: CounterVec = CounterVec::new(
        Opts::new(
            "polymorph_arb_tokens_bought_total",
            "Total tokens bought in arb mode"
        ),
        &["asset", "side"]
    ).expect("Failed to create arb_tokens_bought counter");

    /// Counter for USDC spent on arb orders
    pub static ref ARB_USDC_SPENT: CounterVec = CounterVec::new(
        Opts::new(
            "polymorph_arb_usdc_spent_total",
            "Total USDC spent on arb orders"
        ),
        &["asset", "side"]
    ).expect("Failed to create arb_usdc_spent counter");

    /// Histogram for fill rate (actual_tokens / requested_tokens)
    pub static ref ARB_FILL_RATE: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "polymorph_arb_fill_rate",
            "Fill rate per order (actual / requested)"
        )
        .buckets(vec![0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 1.0])
    ).expect("Failed to create arb_fill_rate histogram");

    /// Histogram for average pair cost per cycle
    pub static ref ARB_AVG_PAIR_COST: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "polymorph_arb_avg_pair_cost",
            "Average pair cost per arb cycle"
        )
        .buckets(vec![0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.0])
    ).expect("Failed to create arb_avg_pair_cost histogram");

    // --- Latency ---

    /// Histogram for end-to-end arb execution latency (detection to last fill)
    pub static ref ARB_EXECUTION_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "polymorph_arb_execution_latency_seconds",
            "Arb execution latency from detection to completion"
        )
        .buckets(vec![0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1.0, 2.0, 5.0]),
        &["phase"]
    ).expect("Failed to create arb_execution_latency histogram");

    // --- Merge & Redeem (On-Chain) ---

    /// Counter for merge attempts / successes / failures
    pub static ref ARB_MERGE_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "polymorph_arb_merge_total",
            "Total merge operations by outcome"
        ),
        &["outcome"]
    ).expect("Failed to create arb_merge_total counter");

    /// Counter for redeem attempts / successes / failures
    pub static ref ARB_REDEEM_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "polymorph_arb_redeem_total",
            "Total redeem operations by outcome"
        ),
        &["outcome"]
    ).expect("Failed to create arb_redeem_total counter");

    /// Counter for USDC recovered via merge
    pub static ref ARB_USDC_MERGED: Counter = Counter::with_opts(
        Opts::new(
            "polymorph_arb_usdc_merged_total",
            "Total USDC recovered via mergePositions"
        )
    ).expect("Failed to create arb_usdc_merged counter");

    /// Counter for USDC recovered via redeem
    pub static ref ARB_USDC_REDEEMED: Counter = Counter::with_opts(
        Opts::new(
            "polymorph_arb_usdc_redeemed_total",
            "Total USDC recovered via redeemPositions"
        )
    ).expect("Failed to create arb_usdc_redeemed counter");

    /// Counter for pairs merged on-chain
    pub static ref ARB_PAIRS_MERGED: Counter = Counter::with_opts(
        Opts::new(
            "polymorph_arb_pairs_merged_total",
            "Total token pairs merged on-chain"
        )
    ).expect("Failed to create arb_pairs_merged counter");

    /// Histogram for merge tx latency (spawn to result)
    pub static ref ARB_MERGE_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "polymorph_arb_merge_latency_seconds",
            "Merge background task latency (spawn to completion)"
        )
        .buckets(vec![5.0, 10.0, 15.0, 20.0, 30.0, 45.0, 60.0, 90.0, 120.0, 150.0])
    ).expect("Failed to create arb_merge_latency histogram");

    // --- Capital & P&L ---

    /// Gauge for available USDC capital
    pub static ref ARB_CAPITAL_AVAILABLE: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_arb_capital_available_usdc",
            "Available USDC capital for arb"
        )
    ).expect("Failed to create arb_capital_available gauge");

    /// Gauge for deployed capital (in open positions)
    pub static ref ARB_CAPITAL_DEPLOYED: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_arb_capital_deployed_usdc",
            "Capital currently deployed in arb positions"
        )
    ).expect("Failed to create arb_capital_deployed gauge");

    /// Gauge for cumulative P&L
    pub static ref ARB_TOTAL_PNL: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_arb_total_pnl_usdc",
            "Cumulative arbitrage P&L in USDC"
        )
    ).expect("Failed to create arb_total_pnl gauge");

    /// Histogram for per-cycle profit
    pub static ref ARB_CYCLE_PROFIT: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "polymorph_arb_cycle_profit_usdc",
            "Profit per arb cycle in USDC"
        )
        .buckets(vec![-5.0, -2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0])
    ).expect("Failed to create arb_cycle_profit histogram");

    /// Gauge for completed cycles count
    pub static ref ARB_CYCLES_COMPLETED: IntGauge = IntGauge::with_opts(
        Opts::new(
            "polymorph_arb_cycles_completed",
            "Total arb cycles completed (merge or redeem)"
        )
    ).expect("Failed to create arb_cycles_completed gauge");

    /// Gauge for active positions count
    pub static ref ARB_ACTIVE_POSITIONS: IntGauge = IntGauge::with_opts(
        Opts::new(
            "polymorph_arb_active_positions",
            "Number of active arb positions"
        )
    ).expect("Failed to create arb_active_positions gauge");

    // --- WebSocket Health ---

    /// Gauge for arb WS connection status (1=connected, 0=disconnected)
    pub static ref ARB_WS_CONNECTED: IntGauge = IntGauge::with_opts(
        Opts::new(
            "polymorph_arb_ws_connected",
            "Arb WebSocket connection status"
        )
    ).expect("Failed to create arb_ws_connected gauge");

    /// Counter for WS book updates received
    pub static ref ARB_WS_BOOK_UPDATES: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "polymorph_arb_ws_book_updates_total",
            "Total book updates received via WebSocket"
        ),
        &["asset"]
    ).expect("Failed to create arb_ws_book_updates counter");

    /// Counter for WS reconnections
    pub static ref ARB_WS_RECONNECTS: IntCounter = IntCounter::with_opts(
        Opts::new(
            "polymorph_arb_ws_reconnects_total",
            "Total WebSocket reconnections"
        )
    ).expect("Failed to create arb_ws_reconnects counter");

    /// Gauge for market scan duration
    pub static ref ARB_SCAN_DURATION: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "polymorph_arb_scan_duration_seconds",
            "Duration of market scan cycle"
        )
        .buckets(vec![0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0])
    ).expect("Failed to create arb_scan_duration histogram");
    
    // ============================================================================
    // INTERNAL LATENCY METRICS - TSC-based high-precision timing
    // ============================================================================
    
    /// Histogram for internal tick-to-trade latency (TSC-based)
    /// Measures from signal generation to order submission (internal path only)
    pub static ref INTERNAL_TICK_TO_TRADE: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "polymorph_internal_tick_to_trade_ns",
            "Internal tick-to-trade latency in nanoseconds (TSC-based)"
        )
        .buckets(vec![
            100.0,      // 100ns
            500.0,      // 500ns  
            1000.0,     // 1µs
            2000.0,     // 2µs
            5000.0,     // 5µs
            10000.0,    // 10µs
            25000.0,    // 25µs
            50000.0,    // 50µs
            100000.0,   // 100µs
            250000.0,   // 250µs
            500000.0,   // 500µs
            1000000.0,  // 1ms
            5000000.0,  // 5ms - target threshold
        ]),
        &["phase"]
    ).expect("Failed to create internal_tick_to_trade histogram");
    
    /// Histogram for signature cache lookup latency
    pub static ref CACHE_LOOKUP_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "polymorph_cache_lookup_latency_ns",
            "Signature cache lookup latency in nanoseconds"
        )
        .buckets(vec![10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0])
    ).expect("Failed to create cache_lookup_latency histogram");
    
    /// Histogram for risk check latency
    pub static ref RISK_CHECK_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "polymorph_risk_check_latency_ns",
            "Risk check latency in nanoseconds"
        )
        .buckets(vec![50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0])
    ).expect("Failed to create risk_check_latency histogram");
    
    /// Gauge for signature cache statistics
    pub static ref SIGNATURE_CACHE_SIZE: IntGauge = IntGauge::with_opts(
        Opts::new(
            "polymorph_signature_cache_size",
            "Number of pre-signed orders in cache"
        )
    ).expect("Failed to create signature_cache_size gauge");
    
    pub static ref SIGNATURE_CACHE_HITS: IntCounter = IntCounter::with_opts(
        Opts::new(
            "polymorph_signature_cache_hits_total",
            "Total signature cache hits"
        )
    ).expect("Failed to create signature_cache_hits counter");
    
    pub static ref SIGNATURE_CACHE_MISSES: IntCounter = IntCounter::with_opts(
        Opts::new(
            "polymorph_signature_cache_misses_total",
            "Total signature cache misses"
        )
    ).expect("Failed to create signature_cache_misses counter");
    
    // ============================================================================
    // SHADOW MODE METRICS - Paper Trading Simulation
    // ============================================================================
    
    /// Gauge for shadow mode virtual balance
    pub static ref SHADOW_BALANCE: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_shadow_balance_usdc",
            "Shadow mode virtual balance in USDC"
        )
    ).expect("Failed to create shadow_balance gauge");
    
    /// Gauge for shadow mode P&L
    pub static ref SHADOW_PNL: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_shadow_pnl_usdc",
            "Shadow mode profit and loss in USDC"
        )
    ).expect("Failed to create shadow_pnl gauge");
    
    /// Counter for shadow mode virtual trades
    pub static ref SHADOW_TRADES_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new(
            "polymorph_shadow_trades_total",
            "Total virtual trades in shadow mode"
        )
    ).expect("Failed to create shadow_trades_total counter");
    
    /// Gauge for shadow mode win rate
    pub static ref SHADOW_WIN_RATE: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_shadow_win_rate",
            "Shadow mode win rate (0.0 to 1.0)"
        )
    ).expect("Failed to create shadow_win_rate gauge");
    
    /// Gauge for shadow mode maker ratio
    pub static ref SHADOW_MAKER_RATIO: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_shadow_maker_ratio",
            "Shadow mode maker fill ratio (0.0 to 1.0)"
        )
    ).expect("Failed to create shadow_maker_ratio gauge");
    
    /// Gauge for shadow mode net fees (rebates - fees)
    pub static ref SHADOW_NET_FEES: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_shadow_net_fees_usdc",
            "Shadow mode net fees (positive = net rebates)"
        )
    ).expect("Failed to create shadow_net_fees gauge");
    
    /// Counter for shadow mode active virtual orders
    pub static ref SHADOW_ACTIVE_ORDERS: IntGauge = IntGauge::with_opts(
        Opts::new(
            "polymorph_shadow_active_orders",
            "Number of active virtual orders in shadow mode"
        )
    ).expect("Failed to create shadow_active_orders gauge");
    
    // ============================================================================
    // AUTO-TUNER METRICS
    // ============================================================================
    
    /// Gauge for inventory health per market
    pub static ref INVENTORY_HEALTH: GaugeVec = GaugeVec::new(
        Opts::new(
            "polymorph_inventory_health",
            "Inventory level per market (positive=long, negative=short)"
        ),
        &["token_id", "health_status"]
    ).expect("Failed to create inventory_health gauge");
    
    /// Gauge for current best grid spacing from tuner
    pub static ref TUNER_BEST_SPACING: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_tuner_best_spacing",
            "Current best grid spacing from auto-tuner optimization"
        )
    ).expect("Failed to create tuner_best_spacing gauge");
    
    /// Gauge for tuner's best environment PnL
    pub static ref TUNER_BEST_PNL: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_tuner_best_pnl_usdc",
            "PnL of best performing virtual environment"
        )
    ).expect("Failed to create tuner_best_pnl gauge");
    
    /// Counter for tuner optimization cycles
    pub static ref TUNER_CYCLES: IntGauge = IntGauge::with_opts(
        Opts::new(
            "polymorph_tuner_cycles_completed",
            "Number of auto-tuner optimization cycles completed"
        )
    ).expect("Failed to create tuner_cycles gauge");
    
    /// Gauge for consecutive profitable hours
    pub static ref CONSECUTIVE_PROFIT_HOURS: IntGauge = IntGauge::with_opts(
        Opts::new(
            "polymorph_consecutive_profit_hours",
            "Consecutive hours of profitability in shadow mode"
        )
    ).expect("Failed to create consecutive_profit_hours gauge");
    
    /// Gauge for ask skew multiplier
    pub static ref ASK_SKEW_MULTIPLIER: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_ask_skew_multiplier",
            "Current ask skew multiplier from inventory management"
        )
    ).expect("Failed to create ask_skew_multiplier gauge");

    // ============================================================================
    // LIVE TRADING METRICS - Critical for real-time monitoring
    // ============================================================================

    /// Gauge for active orders across all markets
    pub static ref ACTIVE_ORDERS_TOTAL: IntGauge = IntGauge::with_opts(
        Opts::new(
            "polymorph_active_orders_total",
            "Total active orders across all markets"
        )
    ).expect("Failed to create active_orders_total gauge");

    /// Gauge for order flow imbalance ratio [-1, +1]
    pub static ref ORDER_FLOW_IMBALANCE: GaugeVec = GaugeVec::new(
        Opts::new(
            "polymorph_order_flow_imbalance",
            "Order flow imbalance ratio: +1=all buys, -1=all sells"
        ),
        &["token_id"]
    ).expect("Failed to create order_flow_imbalance gauge");

    /// Gauge for net inventory position per market (in tokens)
    pub static ref INVENTORY_POSITION: GaugeVec = GaugeVec::new(
        Opts::new(
            "polymorph_inventory_position",
            "Net inventory position per market (positive=long, negative=short)"
        ),
        &["token_id"]
    ).expect("Failed to create inventory_position gauge");

    /// Counter for total fills (completed trades)
    pub static ref FILLS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "polymorph_fills_total",
            "Total fills by side"
        ),
        &["side"]
    ).expect("Failed to create fills_total counter");

    /// Counter for orders cancelled (stale/drifted)
    pub static ref ORDERS_CANCELLED: IntCounter = IntCounter::with_opts(
        Opts::new(
            "polymorph_orders_cancelled_total",
            "Total orders cancelled (stale or drifted)"
        )
    ).expect("Failed to create orders_cancelled counter");

    /// Counter for orders rejected by risk manager
    pub static ref ORDERS_REJECTED_RISK: IntCounter = IntCounter::with_opts(
        Opts::new(
            "polymorph_orders_rejected_risk_total",
            "Orders rejected by risk manager"
        )
    ).expect("Failed to create orders_rejected_risk counter");

    /// Gauge for net PnL including fees (the actual bottom line)
    pub static ref NET_PNL: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_net_pnl_usdc",
            "Net P&L including fees and rebates in USDC"
        )
    ).expect("Failed to create net_pnl gauge");

    /// Gauge for wallet balance (initial + net PnL)
    pub static ref WALLET_BALANCE: Gauge = Gauge::with_opts(
        Opts::new(
            "polymorph_wallet_balance_usdc",
            "Current wallet balance estimate in USDC"
        )
    ).expect("Failed to create wallet_balance gauge");

    /// Counter for rate-limited orders
    pub static ref ORDERS_RATE_LIMITED: IntCounter = IntCounter::with_opts(
        Opts::new(
            "polymorph_orders_rate_limited_total",
            "Orders dropped due to rate limiting"
        )
    ).expect("Failed to create orders_rate_limited counter");
}

/// Register all metrics with the Prometheus registry
pub fn register_metrics() {
    // Latency metrics
    REGISTRY.register(Box::new(TICK_TO_TRADE_LATENCY.clone())).unwrap();
    REGISTRY.register(Box::new(WS_PROCESSING_LATENCY.clone())).unwrap();
    REGISTRY.register(Box::new(ORDER_SIGNING_LATENCY.clone())).unwrap();
    
    // Volume metrics
    REGISTRY.register(Box::new(TOTAL_USDC_TRADED.clone())).unwrap();
    REGISTRY.register(Box::new(USDC_TRADED_BY_SIDE.clone())).unwrap();
    REGISTRY.register(Box::new(MAKER_TRADES_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(TAKER_TRADES_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(TRADES_BY_TYPE.clone())).unwrap();
    REGISTRY.register(Box::new(ORDERS_SUBMITTED.clone())).unwrap();
    
    // Health metrics
    REGISTRY.register(Box::new(CURRENT_BANKROLL.clone())).unwrap();
    REGISTRY.register(Box::new(DAILY_PNL.clone())).unwrap();
    REGISTRY.register(Box::new(TOTAL_EXPOSURE.clone())).unwrap();
    REGISTRY.register(Box::new(KILL_SWITCH_STATUS.clone())).unwrap();
    REGISTRY.register(Box::new(ACTIVE_POSITIONS.clone())).unwrap();
    REGISTRY.register(Box::new(WS_CONNECTION_STATUS.clone())).unwrap();
    
    // Profit metrics
    REGISTRY.register(Box::new(REALIZED_PROFIT_BY_MARKET.clone())).unwrap();
    REGISTRY.register(Box::new(UNREALIZED_PNL_BY_MARKET.clone())).unwrap();
    REGISTRY.register(Box::new(FEES_PAID.clone())).unwrap();
    REGISTRY.register(Box::new(REBATES_EARNED.clone())).unwrap();
    
    // Arbitrage metrics — full lifecycle
    REGISTRY.register(Box::new(ARB_OPPORTUNITIES_DETECTED.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_OPPORTUNITIES_EXECUTED.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_OPPORTUNITIES_SKIPPED.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_TRACKED_MARKETS.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_WS_SUBSCRIBED_ASSETS.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_BEST_PAIR_COST.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_SPREAD_PCT.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_AVAILABLE_PAIRS.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_BOOK_DEPTH.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_ORDERS_SUBMITTED.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_ORDERS_FILLED.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_ORDERS_FAILED.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_TOKENS_BOUGHT.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_USDC_SPENT.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_FILL_RATE.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_AVG_PAIR_COST.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_EXECUTION_LATENCY.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_MERGE_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_REDEEM_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_USDC_MERGED.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_USDC_REDEEMED.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_PAIRS_MERGED.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_MERGE_LATENCY.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_CAPITAL_AVAILABLE.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_CAPITAL_DEPLOYED.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_TOTAL_PNL.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_CYCLE_PROFIT.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_CYCLES_COMPLETED.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_ACTIVE_POSITIONS.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_WS_CONNECTED.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_WS_BOOK_UPDATES.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_WS_RECONNECTS.clone())).unwrap();
    REGISTRY.register(Box::new(ARB_SCAN_DURATION.clone())).unwrap();
    
    // Internal latency metrics (TSC-based)
    REGISTRY.register(Box::new(INTERNAL_TICK_TO_TRADE.clone())).unwrap();
    REGISTRY.register(Box::new(CACHE_LOOKUP_LATENCY.clone())).unwrap();
    REGISTRY.register(Box::new(RISK_CHECK_LATENCY.clone())).unwrap();
    REGISTRY.register(Box::new(SIGNATURE_CACHE_SIZE.clone())).unwrap();
    REGISTRY.register(Box::new(SIGNATURE_CACHE_HITS.clone())).unwrap();
    REGISTRY.register(Box::new(SIGNATURE_CACHE_MISSES.clone())).unwrap();
    
    // Shadow mode metrics
    REGISTRY.register(Box::new(SHADOW_BALANCE.clone())).unwrap();
    REGISTRY.register(Box::new(SHADOW_PNL.clone())).unwrap();
    REGISTRY.register(Box::new(SHADOW_TRADES_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(SHADOW_WIN_RATE.clone())).unwrap();
    REGISTRY.register(Box::new(SHADOW_MAKER_RATIO.clone())).unwrap();
    REGISTRY.register(Box::new(SHADOW_NET_FEES.clone())).unwrap();
    REGISTRY.register(Box::new(SHADOW_ACTIVE_ORDERS.clone())).unwrap();
    
    // Auto-tuner metrics
    REGISTRY.register(Box::new(INVENTORY_HEALTH.clone())).unwrap();
    REGISTRY.register(Box::new(TUNER_BEST_SPACING.clone())).unwrap();
    REGISTRY.register(Box::new(TUNER_BEST_PNL.clone())).unwrap();
    REGISTRY.register(Box::new(TUNER_CYCLES.clone())).unwrap();
    REGISTRY.register(Box::new(CONSECUTIVE_PROFIT_HOURS.clone())).unwrap();
    REGISTRY.register(Box::new(ASK_SKEW_MULTIPLIER.clone())).unwrap();
    
    // Live trading metrics
    REGISTRY.register(Box::new(ACTIVE_ORDERS_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(ORDER_FLOW_IMBALANCE.clone())).unwrap();
    REGISTRY.register(Box::new(INVENTORY_POSITION.clone())).unwrap();
    REGISTRY.register(Box::new(FILLS_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(ORDERS_CANCELLED.clone())).unwrap();
    REGISTRY.register(Box::new(ORDERS_REJECTED_RISK.clone())).unwrap();
    REGISTRY.register(Box::new(NET_PNL.clone())).unwrap();
    REGISTRY.register(Box::new(WALLET_BALANCE.clone())).unwrap();
    REGISTRY.register(Box::new(ORDERS_RATE_LIMITED.clone())).unwrap();
    
    info!("Prometheus metrics registered (with TSC-based timing)");
}

/// Update shadow mode metrics from ShadowStats
pub fn update_shadow_metrics(balance: f64, pnl: f64, _trades: u64, win_rate: f64, maker_ratio: f64, net_fees: f64, active_orders: i64) {
    SHADOW_BALANCE.set(balance);
    SHADOW_PNL.set(pnl);
    SHADOW_WIN_RATE.set(win_rate);
    SHADOW_MAKER_RATIO.set(maker_ratio);
    SHADOW_NET_FEES.set(net_fees);
    SHADOW_ACTIVE_ORDERS.set(active_orders);
}

/// Update inventory health metrics
pub fn update_inventory_health(token_id: u64, health_status: &str, inventory: f64) {
    INVENTORY_HEALTH
        .with_label_values(&[&token_id.to_string(), health_status])
        .set(inventory);
}

/// Update auto-tuner metrics
pub fn update_tuner_metrics(best_spacing: f64, best_pnl: f64, cycles: u64) {
    TUNER_BEST_SPACING.set(best_spacing);
    TUNER_BEST_PNL.set(best_pnl);
    TUNER_CYCLES.set(cycles as i64);
}

/// Update consecutive profit hours
pub fn update_consecutive_profit_hours(hours: u64) {
    CONSECUTIVE_PROFIT_HOURS.set(hours as i64);
}

/// Update ask skew multiplier
pub fn update_ask_skew_multiplier(multiplier: f64) {
    ASK_SKEW_MULTIPLIER.set(multiplier);
}

/// Latency tracker for tick-to-trade measurement
/// Use this to track the full path from data receipt to order acknowledgment
pub struct LatencyTracker {
    start: Instant,
    exchange: &'static str,
    side: &'static str,
    order_type: &'static str,
}

impl LatencyTracker {
    /// Start tracking latency when market data is received
    #[inline(always)]
    pub fn start(exchange: &'static str, side: &'static str, order_type: &'static str) -> Self {
        Self {
            start: Instant::now(),
            exchange,
            side,
            order_type,
        }
    }
    
    /// Record the latency when order acknowledgment is received
    #[inline(always)]
    pub fn finish(self) {
        let duration = self.start.elapsed();
        TICK_TO_TRADE_LATENCY
            .with_label_values(&[self.exchange, self.side, self.order_type])
            .observe(duration.as_secs_f64());
    }
    
    /// Get elapsed time without finishing (for intermediate checks)
    #[inline(always)]
    pub fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }
}

/// TSC-based high-precision latency tracker for internal hot path
/// Uses quanta crate for Time Stamp Counter access (sub-nanosecond resolution)
pub struct TscLatencyTracker {
    start: QuantaInstant,
    phase: &'static str,
}

impl TscLatencyTracker {
    /// Start tracking with TSC timestamp - ZERO ALLOCATION
    #[inline(always)]
    pub fn start(phase: &'static str) -> Self {
        Self {
            start: TSC_CLOCK.now(),
            phase,
        }
    }
    
    /// Get raw TSC start value for external tracking
    #[inline(always)]
    pub fn start_raw() -> QuantaInstant {
        TSC_CLOCK.now()
    }
    
    /// Record the latency in nanoseconds
    #[inline(always)]
    pub fn finish(self) {
        let elapsed_ns = self.start.elapsed().as_nanos() as f64;
        INTERNAL_TICK_TO_TRADE
            .with_label_values(&[self.phase])
            .observe(elapsed_ns);
    }
    
    /// Record latency from raw TSC value
    #[inline(always)]
    pub fn finish_raw(start: QuantaInstant, phase: &'static str) {
        let elapsed_ns = start.elapsed().as_nanos() as f64;
        INTERNAL_TICK_TO_TRADE
            .with_label_values(&[phase])
            .observe(elapsed_ns);
    }
    
    /// Get elapsed nanoseconds without recording
    #[inline(always)]
    pub fn elapsed_ns(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }
    
    /// Get elapsed from raw TSC value
    #[inline(always)]
    pub fn elapsed_ns_raw(start: QuantaInstant) -> u64 {
        start.elapsed().as_nanos() as u64
    }
}

/// Record cache lookup latency (TSC-based)
#[inline(always)]
pub fn record_cache_lookup_ns(latency_ns: f64) {
    CACHE_LOOKUP_LATENCY.observe(latency_ns);
}

/// Record risk check latency (TSC-based)
#[inline(always)]
pub fn record_risk_check_ns(latency_ns: f64) {
    RISK_CHECK_LATENCY.observe(latency_ns);
}

/// Update signature cache statistics
pub fn update_cache_stats(size: usize, hits: u64, misses: u64) {
    SIGNATURE_CACHE_SIZE.set(size as i64);
    // Note: These are cumulative, so we'd need to track deltas in real usage
    // For now, just set the current values
    let current_hits = SIGNATURE_CACHE_HITS.get();
    let current_misses = SIGNATURE_CACHE_MISSES.get();
    if hits > current_hits {
        SIGNATURE_CACHE_HITS.inc_by(hits - current_hits);
    }
    if misses > current_misses {
        SIGNATURE_CACHE_MISSES.inc_by(misses - current_misses);
    }
}

/// Record a trade execution with all relevant metrics
pub fn record_trade(
    is_maker: bool,
    side: &str,
    usdc_amount: f64,
    fee_amount: f64,
    success: bool,
) {
    // Volume tracking
    TOTAL_USDC_TRADED.inc_by(usdc_amount);
    USDC_TRADED_BY_SIDE.with_label_values(&[side]).inc_by(usdc_amount);
    
    // Trade type tracking
    if is_maker {
        MAKER_TRADES_TOTAL.inc();
        REBATES_EARNED.inc_by(fee_amount.abs()); // Maker rebates are negative fees
    } else {
        TAKER_TRADES_TOTAL.inc();
        FEES_PAID.inc_by(fee_amount);
    }
    
    // Outcome tracking
    let trade_type = if is_maker { "maker" } else { "taker" };
    let outcome = if success { "success" } else { "failed" };
    TRADES_BY_TYPE.with_label_values(&[trade_type, outcome]).inc();
}

/// Update health metrics from risk manager state
pub fn update_health_metrics(
    bankroll: f64,
    daily_pnl: f64,
    exposure: f64,
    kill_switch_triggered: bool,
    active_positions: i64,
) {
    CURRENT_BANKROLL.set(bankroll);
    DAILY_PNL.set(daily_pnl);
    TOTAL_EXPOSURE.set(exposure);
    KILL_SWITCH_STATUS.set(if kill_switch_triggered { 1 } else { 0 });
    ACTIVE_POSITIONS.set(active_positions);
}

/// Update WebSocket connection status
pub fn update_ws_status(source: &str, connected: bool) {
    WS_CONNECTION_STATUS.with_label_values(&[source]).set(if connected { 1 } else { 0 });
}

/// Record WebSocket message processing latency
#[inline(always)]
pub fn record_ws_latency(source: &str, duration_secs: f64) {
    WS_PROCESSING_LATENCY.with_label_values(&[source]).observe(duration_secs);
}

/// Record order signing latency
#[inline(always)]
pub fn record_signing_latency(duration_secs: f64) {
    ORDER_SIGNING_LATENCY.observe(duration_secs);
}

/// Record arbitrage opportunity
pub fn record_arb_opportunity(asset: &str, source: &str, executed: bool) {
    ARB_OPPORTUNITIES_DETECTED.with_label_values(&[asset, source]).inc();
    if executed {
        ARB_OPPORTUNITIES_EXECUTED.with_label_values(&[asset]).inc();
    }
}

/// Metrics server configuration
pub struct MetricsServer {
    port: u16,
}

impl MetricsServer {
    pub fn new(port: u16) -> Self {
        Self { port }
    }
    
    /// Start the metrics HTTP server
    pub async fn run(self) {
        // Register all metrics
        register_metrics();
        
        // Define the /metrics endpoint
        let metrics_route = warp::path("metrics")
            .and(warp::get())
            .map(|| {
                let encoder = TextEncoder::new();
                let metric_families = REGISTRY.gather();
                let mut buffer = Vec::new();
                
                if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
                    error!("Failed to encode metrics: {}", e);
                    return warp::reply::with_status(
                        "Internal Server Error".to_string(),
                        warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                    );
                }
                
                warp::reply::with_status(
                    String::from_utf8(buffer).unwrap_or_default(),
                    warp::http::StatusCode::OK,
                )
            });
        
        // Health check endpoint
        let health_route = warp::path("health")
            .and(warp::get())
            .map(|| {
                let kill_switch = KILL_SWITCH_STATUS.get();
                if kill_switch == 1 {
                    warp::reply::with_status(
                        "KILL_SWITCH_TRIGGERED".to_string(),
                        warp::http::StatusCode::SERVICE_UNAVAILABLE,
                    )
                } else {
                    warp::reply::with_status(
                        "OK".to_string(),
                        warp::http::StatusCode::OK,
                    )
                }
            });
        
        // Ready check endpoint
        let ready_route = warp::path("ready")
            .and(warp::get())
            .map(|| {
                // Check if essential WebSocket connections are up
                let poly_status = WS_CONNECTION_STATUS
                    .with_label_values(&["polymarket"])
                    .get();
                
                if poly_status == 1 {
                    warp::reply::with_status("READY".to_string(), warp::http::StatusCode::OK)
                } else {
                    warp::reply::with_status(
                        "NOT_READY".to_string(),
                        warp::http::StatusCode::SERVICE_UNAVAILABLE,
                    )
                }
            });
        
        let routes = metrics_route.or(health_route).or(ready_route);
        
        info!("Starting metrics server on port {}", self.port);
        warp::serve(routes)
            .run(([0, 0, 0, 0], self.port))
            .await;
    }
}

/// Convenience macro for timing code blocks
#[macro_export]
macro_rules! time_operation {
    ($histogram:expr, $code:block) => {{
        let start = std::time::Instant::now();
        let result = $code;
        $histogram.observe(start.elapsed().as_secs_f64());
        result
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_latency_tracker() {
        register_metrics();
        
        let tracker = LatencyTracker::start("binance", "buy", "limit");
        std::thread::sleep(std::time::Duration::from_micros(100));
        assert!(tracker.elapsed_us() >= 100);
        tracker.finish();
    }
    
    #[test]
    fn test_record_trade() {
        register_metrics();
        
        record_trade(true, "buy", 1000.0, -0.10, true);
        record_trade(false, "sell", 500.0, 15.75, true);
        
        // Verify counters incremented
        assert!(TOTAL_USDC_TRADED.get() >= 1500.0);
    }
    
    #[test]
    fn test_health_metrics() {
        register_metrics();
        
        update_health_metrics(100000.0, 1234.56, 25000.0, false, 5);
        
        assert_eq!(CURRENT_BANKROLL.get(), 100000.0);
        assert_eq!(DAILY_PNL.get(), 1234.56);
        assert_eq!(KILL_SWITCH_STATUS.get(), 0);
    }
}
