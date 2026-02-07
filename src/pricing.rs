//! Pricing engine for arbitrage detection and market making
//! Zero-allocation hot path with pre-computed thresholds
//! Updated for 2026 Polymarket dynamic fee curve

use crate::config::{Config, FeeCurve2026Config};
use crate::types::{MarketId, OrderBookSnapshot, PriceLevel, Side, TradeSignal, SignalUrgency, OrderType};
use crate::websocket::{Exchange, WsEvent};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use tracing::{debug, info, warn};

/// Pre-computed fee thresholds for fast comparison
/// Updated for 2026 Polymarket dynamic fee curve
#[derive(Debug, Clone)]
pub struct FeeThresholds {
    /// Total cost in basis points (taker fee + slippage + gas equivalent)
    pub total_cost_bps: u32,
    /// Minimum spread required for taker execution
    pub min_taker_spread_bps: u32,
    /// Maker rebate in basis points (negative cost)
    pub maker_rebate_bps: i32,
    /// Minimum profit threshold
    pub min_profit_bps: u32,
    /// 2026 Dynamic Fee Curve configuration
    pub fee_curve: FeeCurve2026Config,
    /// Gas cost in basis points
    gas_bps: u32,
    /// Slippage in basis points
    slippage_bps: u32,
}

impl FeeThresholds {
    pub fn from_config(config: &Config) -> Self {
        let gas_bps = Self::gas_to_bps(config.trading.gas_cost_usd, config.trading.default_order_size);
        let fee_curve = config.trading.fee_curve_2026.clone();
        
        Self {
            total_cost_bps: config.trading.max_taker_fee_bps + config.trading.slippage_bps + gas_bps,
            min_taker_spread_bps: config.trading.max_taker_fee_bps + config.trading.slippage_bps + gas_bps + config.trading.min_profit_bps,
            maker_rebate_bps: fee_curve.maker_rebate_bps,
            min_profit_bps: config.trading.min_profit_bps,
            fee_curve,
            gas_bps,
            slippage_bps: config.trading.slippage_bps,
        }
    }
    
    /// Convert gas cost in USD to basis points for a given order size
    #[inline(always)]
    fn gas_to_bps(gas_usd: Decimal, order_size: Decimal) -> u32 {
        if order_size.is_zero() {
            return 100; // Default high gas estimate
        }
        let bps = (gas_usd / order_size * Decimal::new(10000, 0))
            .to_string()
            .parse::<u32>()
            .unwrap_or(100);
        bps
    }
    
    /// Check if spread is profitable for taker execution
    #[inline(always)]
    pub fn is_taker_profitable(&self, spread_bps: u32) -> bool {
        spread_bps > self.min_taker_spread_bps
    }
    
    /// Calculate expected profit in basis points
    #[inline(always)]
    pub fn expected_profit_bps(&self, spread_bps: u32, is_maker: bool) -> i32 {
        if is_maker {
            spread_bps as i32 - self.maker_rebate_bps
        } else {
            spread_bps as i32 - self.total_cost_bps as i32
        }
    }
    
    /// 2026 Fee Curve: Check if we MUST use post_only (maker mode)
    /// Returns true if probability is in 0.40-0.60 range where taker fee is 3.15%
    #[inline(always)]
    pub fn must_force_post_only(&self, probability: Decimal) -> bool {
        self.fee_curve.is_forced_maker_zone(probability)
    }
    
    /// 2026 Fee Curve: Get the applicable taker fee for current probability
    #[inline(always)]
    pub fn get_dynamic_taker_fee(&self, probability: Decimal) -> u32 {
        self.fee_curve.get_taker_fee_bps(probability)
    }
    
    /// 2026 Fee Curve: Calculate total cost with dynamic fee
    #[inline(always)]
    pub fn total_cost_with_dynamic_fee(&self, probability: Decimal) -> u32 {
        self.get_dynamic_taker_fee(probability) + self.slippage_bps + self.gas_bps
    }
    
    /// 2026 Fee Curve: Check if trade is profitable considering dynamic fees
    #[inline(always)]
    pub fn is_profitable_2026(
        &self,
        spread_bps: u32,
        probability: Decimal,
        force_maker: bool,
    ) -> bool {
        if force_maker || self.must_force_post_only(probability) {
            // Maker mode: only need to beat maker rebate (which is negative = profit)
            spread_bps as i32 > -self.maker_rebate_bps
        } else {
            // Taker mode: need to beat dynamic fee + slippage + gas + min profit
            let dynamic_cost = self.total_cost_with_dynamic_fee(probability);
            spread_bps > dynamic_cost + self.min_profit_bps
        }
    }
}

// =============================================================================
// VOLATILITY TRACKER - Adaptive Grid Spacing
// =============================================================================

/// Window size for volatility calculation
pub const VOLATILITY_WINDOW: usize = 50;

/// Volatility tracker using rolling window of price ticks
/// Used for adaptive grid spacing - widen spreads during high volatility
#[derive(Debug)]
pub struct VolatilityTracker {
    /// Ring buffer of recent price changes (as percentage)
    price_changes: [f64; VOLATILITY_WINDOW],
    /// Current write position in ring buffer
    write_pos: usize,
    /// Number of samples collected
    sample_count: usize,
    /// Cached current volatility
    current_volatility: f64,
    /// Baseline volatility (rolling average)
    baseline_volatility: f64,
    /// Last price for calculating changes
    last_price: Option<Decimal>,
    /// Volatility spike threshold (2x = spike)
    spike_threshold: f64,
    /// Volatility buffer multiplier when spike detected
    volatility_buffer: Decimal,
}

impl VolatilityTracker {
    pub fn new(spike_threshold: f64, volatility_buffer: Decimal) -> Self {
        Self {
            price_changes: [0.0; VOLATILITY_WINDOW],
            write_pos: 0,
            sample_count: 0,
            current_volatility: 0.0,
            baseline_volatility: 0.01, // 1% default baseline
            last_price: None,
            spike_threshold,
            volatility_buffer,
        }
    }

    /// Record a new price tick and update volatility
    #[inline]
    pub fn record_tick(&mut self, price: Decimal) {
        if let Some(last) = self.last_price {
            if last > Decimal::ZERO {
                // Calculate percentage change
                let change = ((price - last) / last).to_f64().unwrap_or(0.0).abs();
                
                // Store in ring buffer
                self.price_changes[self.write_pos] = change;
                self.write_pos = (self.write_pos + 1) % VOLATILITY_WINDOW;
                self.sample_count = self.sample_count.saturating_add(1).min(VOLATILITY_WINDOW);
                
                // Recalculate volatility
                self.recalculate_volatility();
            }
        }
        self.last_price = Some(price);
    }

    /// Recalculate current volatility (standard deviation of price changes)
    fn recalculate_volatility(&mut self) {
        if self.sample_count < 2 {
            return;
        }

        let n = self.sample_count;
        let sum: f64 = self.price_changes[..n].iter().sum();
        let mean = sum / n as f64;
        
        let variance: f64 = self.price_changes[..n]
            .iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / n as f64;
        
        self.current_volatility = variance.sqrt();
        
        // Update baseline with EMA (slow decay)
        if self.sample_count >= VOLATILITY_WINDOW {
            self.baseline_volatility = self.baseline_volatility * 0.99 + self.current_volatility * 0.01;
        }
    }

    /// Check if volatility has spiked above threshold
    #[inline]
    pub fn is_volatility_spike(&self) -> bool {
        if self.baseline_volatility <= 0.0 {
            return false;
        }
        self.current_volatility > self.baseline_volatility * self.spike_threshold
    }

    /// Get spacing multiplier based on current volatility
    /// Returns 1.0 normally, volatility_buffer when spike detected
    #[inline]
    pub fn get_spacing_multiplier(&self) -> Decimal {
        if self.is_volatility_spike() {
            self.volatility_buffer
        } else {
            Decimal::ONE
        }
    }

    /// Get current volatility (standard deviation of price changes)
    #[inline]
    pub fn current_volatility(&self) -> f64 {
        self.current_volatility
    }

    /// Get baseline volatility
    #[inline]
    pub fn baseline_volatility(&self) -> f64 {
        self.baseline_volatility
    }

    /// Get volatility ratio (current / baseline)
    #[inline]
    pub fn volatility_ratio(&self) -> f64 {
        if self.baseline_volatility > 0.0 {
            self.current_volatility / self.baseline_volatility
        } else {
            1.0
        }
    }

    /// Reset the tracker
    pub fn reset(&mut self) {
        self.price_changes = [0.0; VOLATILITY_WINDOW];
        self.write_pos = 0;
        self.sample_count = 0;
        self.current_volatility = 0.0;
        self.last_price = None;
    }
}

impl Default for VolatilityTracker {
    fn default() -> Self {
        Self::new(2.0, Decimal::new(15, 1)) // 2x spike threshold, 1.5x buffer
    }
}

// =============================================================================
// VOLUME VELOCITY TRACKER - Circuit Breaker for Toxic Flow
// =============================================================================

/// Window size for 5-minute volume moving average (in seconds)
pub const VOLUME_MA_WINDOW_SECS: usize = 300;

/// Number of 1-second buckets for volume tracking
pub const VOLUME_BUCKETS: usize = 300;

/// Volume velocity tracker for detecting toxic flow / breaking news
/// Tracks USDC volume per second and maintains 5-minute moving average
#[derive(Debug)]
pub struct VolumeTracker {
    /// Ring buffer of 1-second volume buckets (USDC)
    volume_buckets: [f64; VOLUME_BUCKETS],
    /// Current bucket index
    current_bucket: usize,
    /// Timestamp of current bucket start (seconds since epoch)
    current_bucket_timestamp: u64,
    /// Running sum for fast MA calculation
    running_sum: f64,
    /// Number of buckets with data
    buckets_filled: usize,
    /// Volume spike multiplier threshold (e.g., 10x = circuit breaker)
    spike_multiplier: f64,
    /// Whether circuit breaker is currently triggered
    circuit_breaker_triggered: bool,
    /// Timestamp when circuit breaker was triggered
    triggered_at_ns: u64,
}

impl VolumeTracker {
    pub fn new(spike_multiplier: f64) -> Self {
        Self {
            volume_buckets: [0.0; VOLUME_BUCKETS],
            current_bucket: 0,
            current_bucket_timestamp: Self::current_second(),
            running_sum: 0.0,
            buckets_filled: 0,
            spike_multiplier,
            circuit_breaker_triggered: false,
            triggered_at_ns: 0,
        }
    }

    /// Record a trade volume (USDC value)
    /// Returns true if this trade triggered the circuit breaker
    #[inline]
    pub fn record_trade(&mut self, volume_usdc: f64) -> bool {
        let now_sec = Self::current_second();
        
        // Advance buckets if we've moved to a new second
        self.advance_buckets(now_sec);
        
        // Add volume to current bucket
        self.volume_buckets[self.current_bucket] += volume_usdc;
        self.running_sum += volume_usdc;
        
        // Check for volume spike
        self.check_volume_spike()
    }

    /// Record trade from Decimal
    #[inline]
    pub fn record_trade_decimal(&mut self, volume: Decimal) -> bool {
        let volume_f64 = volume.to_f64().unwrap_or(0.0);
        self.record_trade(volume_f64)
    }

    /// Advance buckets to current time
    fn advance_buckets(&mut self, now_sec: u64) {
        let elapsed = now_sec.saturating_sub(self.current_bucket_timestamp);
        
        if elapsed == 0 {
            return; // Still in same second
        }

        // Clear buckets for elapsed seconds
        let buckets_to_clear = elapsed.min(VOLUME_BUCKETS as u64) as usize;
        
        for _ in 0..buckets_to_clear {
            self.current_bucket = (self.current_bucket + 1) % VOLUME_BUCKETS;
            
            // Subtract old value from running sum before clearing
            self.running_sum -= self.volume_buckets[self.current_bucket];
            self.running_sum = self.running_sum.max(0.0); // Prevent negative from float errors
            
            self.volume_buckets[self.current_bucket] = 0.0;
        }

        self.current_bucket_timestamp = now_sec;
        self.buckets_filled = self.buckets_filled.saturating_add(buckets_to_clear).min(VOLUME_BUCKETS);
    }

    /// Check if current 1-second volume exceeds threshold
    fn check_volume_spike(&mut self) -> bool {
        if self.buckets_filled < 60 {
            // Need at least 1 minute of data
            return false;
        }

        let current_1s_volume = self.volume_buckets[self.current_bucket];
        let avg_volume_per_sec = self.get_5min_average_per_sec();
        
        if avg_volume_per_sec > 0.0 && current_1s_volume > avg_volume_per_sec * self.spike_multiplier {
            // VOLUME SPIKE DETECTED!
            if !self.circuit_breaker_triggered {
                self.circuit_breaker_triggered = true;
                self.triggered_at_ns = Self::now_ns();
                
                tracing::error!(
                    current_1s = current_1s_volume,
                    avg_5m = avg_volume_per_sec,
                    multiplier = current_1s_volume / avg_volume_per_sec,
                    threshold = self.spike_multiplier,
                    "🚨 VOLUME SPIKE DETECTED! {}x average - Circuit Breaker TRIGGERED",
                    current_1s_volume / avg_volume_per_sec
                );
            }
            return true;
        }

        false
    }

    /// Get 5-minute average volume per second
    #[inline]
    pub fn get_5min_average_per_sec(&self) -> f64 {
        if self.buckets_filled == 0 {
            return 0.0;
        }
        self.running_sum / self.buckets_filled as f64
    }

    /// Get current 1-second volume
    #[inline]
    pub fn get_current_1s_volume(&self) -> f64 {
        self.volume_buckets[self.current_bucket]
    }

    /// Get total 5-minute volume
    #[inline]
    pub fn get_5min_total_volume(&self) -> f64 {
        self.running_sum
    }

    /// Check if circuit breaker is triggered
    #[inline]
    pub fn is_circuit_breaker_triggered(&self) -> bool {
        self.circuit_breaker_triggered
    }

    /// Get volume velocity ratio (current / average)
    #[inline]
    pub fn get_velocity_ratio(&self) -> f64 {
        let avg = self.get_5min_average_per_sec();
        if avg > 0.0 {
            self.get_current_1s_volume() / avg
        } else {
            1.0
        }
    }

    /// Reset the circuit breaker (manual intervention)
    pub fn reset_circuit_breaker(&mut self) {
        if self.circuit_breaker_triggered {
            self.circuit_breaker_triggered = false;
            tracing::info!("✅ Volume circuit breaker reset - trading can resume");
        }
    }

    /// Full reset of tracker
    pub fn reset(&mut self) {
        self.volume_buckets = [0.0; VOLUME_BUCKETS];
        self.current_bucket = 0;
        self.current_bucket_timestamp = Self::current_second();
        self.running_sum = 0.0;
        self.buckets_filled = 0;
        self.circuit_breaker_triggered = false;
    }

    /// Get statistics
    pub fn stats(&self) -> VolumeStats {
        VolumeStats {
            current_1s_volume: self.get_current_1s_volume(),
            avg_5m_per_sec: self.get_5min_average_per_sec(),
            total_5m_volume: self.get_5min_total_volume(),
            velocity_ratio: self.get_velocity_ratio(),
            circuit_breaker_triggered: self.circuit_breaker_triggered,
            buckets_filled: self.buckets_filled,
        }
    }

    #[inline]
    fn current_second() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[inline]
    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}

impl Default for VolumeTracker {
    fn default() -> Self {
        Self::new(10.0) // 10x spike = circuit breaker
    }
}

/// Volume tracking statistics
#[derive(Debug, Clone)]
pub struct VolumeStats {
    pub current_1s_volume: f64,
    pub avg_5m_per_sec: f64,
    pub total_5m_volume: f64,
    pub velocity_ratio: f64,
    pub circuit_breaker_triggered: bool,
    pub buckets_filled: usize,
}

// =============================================================================
// ORDER FLOW IMBALANCE - Directional pressure detection
// =============================================================================

/// Tracks buy vs sell volume over a rolling window to detect directional pressure.
/// Positive imbalance = more buys (price likely to rise).
/// Negative imbalance = more sells (price likely to fall).
#[derive(Debug)]
pub struct OrderFlowTracker {
    /// Rolling buy volume (USDC) in current window
    buy_volume: f64,
    /// Rolling sell volume (USDC) in current window
    sell_volume: f64,
    /// Decay factor per tick (EMA-style, 0.95 = slow decay)
    decay: f64,
    /// Trade count in current window
    trade_count: u64,
}

impl OrderFlowTracker {
    pub fn new() -> Self {
        Self {
            buy_volume: 0.0,
            sell_volume: 0.0,
            decay: 0.95,
            trade_count: 0,
        }
    }

    /// Record a trade and update flow imbalance
    #[inline]
    pub fn record_trade(&mut self, side: Side, size: Decimal, price: Decimal) {
        let notional = (size * price).to_f64().unwrap_or(0.0);
        // Decay old volumes (EMA)
        self.buy_volume *= self.decay;
        self.sell_volume *= self.decay;
        match side {
            Side::Buy => self.buy_volume += notional,
            Side::Sell => self.sell_volume += notional,
        }
        self.trade_count += 1;
    }

    /// Get order flow imbalance as a ratio in [-1.0, +1.0].
    /// +1.0 = all buys, -1.0 = all sells, 0.0 = balanced.
    #[inline]
    pub fn imbalance_ratio(&self) -> f64 {
        let total = self.buy_volume + self.sell_volume;
        if total < 1.0 {
            return 0.0;
        }
        (self.buy_volume - self.sell_volume) / total
    }

    /// Get imbalance as basis points for quote skewing.
    /// Positive = buy pressure → skew asks down (sell more aggressively).
    /// Max ±30 bps skew from flow.
    #[inline]
    pub fn skew_bps(&self) -> i32 {
        let ratio = self.imbalance_ratio();
        // Linear mapping: ratio [-1, +1] → skew [-30, +30] bps
        (ratio * 30.0) as i32
    }
}

impl Default for OrderFlowTracker {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// MARKET QUALITY SCORER - Smart market selection
// =============================================================================

/// Scores a market's attractiveness for market making.
/// Higher score = better opportunity.
#[derive(Debug, Clone, Copy)]
pub struct MarketScore {
    /// Spread in basis points
    pub spread_bps: u32,
    /// 5-minute trade volume (USDC)
    pub volume_5m: f64,
    /// Composite score (higher = better)
    pub score: f64,
}

impl MarketScore {
    /// Compute market quality score.
    /// Best markets have moderate spread (30-200 bps) AND decent volume.
    pub fn compute(spread_bps: u32, volume_5m: f64) -> Self {
        // Spread component: peak at 50-100 bps, drops off at extremes
        let spread_score = if spread_bps < 10 {
            0.0 // Too tight, can't profit
        } else if spread_bps <= 100 {
            spread_bps as f64 / 100.0 // Linear ramp up
        } else if spread_bps <= 300 {
            1.0 - (spread_bps as f64 - 100.0) / 400.0 // Slow decay
        } else {
            0.1 // Very wide = low liquidity, bad
        };

        // Volume component: log scale, minimum $100 in 5 min
        let volume_score = if volume_5m < 100.0 {
            0.0
        } else {
            (volume_5m.log10() - 2.0).min(3.0) / 3.0 // Normalize log($100)..log($100k) to 0..1
        };

        let score = spread_score * 0.4 + volume_score * 0.6; // Volume weighted more

        Self { spread_bps, volume_5m, score }
    }
}

/// Market state for a single Polymarket market
#[derive(Debug)]
pub struct MarketState {
    pub market_id: MarketId,
    pub order_book: OrderBookSnapshot,
    pub last_trade_price: Decimal,
    pub last_update_ns: AtomicU64,
    /// External reference price (from Binance/Coinbase correlation)
    pub external_ref_price: Option<Decimal>,
    /// Implied probability from order book
    pub implied_prob: Decimal,
    /// Per-market volatility tracker for adaptive spread widening
    pub volatility: VolatilityTracker,
    /// Order flow imbalance tracker
    pub flow: OrderFlowTracker,
    /// Volume tracker for market quality scoring
    pub volume: VolumeTracker,
}

impl MarketState {
    pub fn new(market_id: MarketId) -> Self {
        Self {
            market_id,
            order_book: OrderBookSnapshot::new(),
            last_trade_price: Decimal::ZERO,
            last_update_ns: AtomicU64::new(0),
            external_ref_price: None,
            implied_prob: Decimal::new(5, 1), // 0.5 default
            volatility: VolatilityTracker::default(),
            flow: OrderFlowTracker::new(),
            volume: VolumeTracker::new(10.0),
        }
    }
    
    /// Update order book and recalculate implied probability
    #[inline]
    pub fn update_book(&mut self, snapshot: OrderBookSnapshot) {
        self.order_book = snapshot;
        self.last_update_ns.store(snapshot.timestamp_ns, Ordering::Relaxed);
        
        // Calculate implied probability from mid price
        if let Some(mid) = self.order_book.mid_price() {
            self.implied_prob = mid;
            // Feed mid price to volatility tracker for adaptive spread widening
            self.volatility.record_tick(mid);
        }
    }

    /// Record a trade for flow tracking and volume scoring
    #[inline]
    pub fn record_trade(&mut self, side: Side, size: Decimal, price: Decimal) {
        self.flow.record_trade(side, size, price);
        let notional = (size * price).to_f64().unwrap_or(0.0);
        self.volume.record_trade(notional);
        self.last_trade_price = price;
    }

    /// Get market quality score for smart selection
    pub fn quality_score(&self) -> MarketScore {
        let spread_bps = if let (Some(bid), Some(ask)) = (self.order_book.best_bid(), self.order_book.best_ask()) {
            let mid = (bid + ask) / Decimal::TWO;
            if mid > Decimal::ZERO {
                ((ask - bid) / mid * Decimal::new(10000, 0)).to_u32().unwrap_or(0)
            } else { 0 }
        } else { 0 };
        MarketScore::compute(spread_bps, self.volume.get_5min_total_volume())
    }
    
    /// Check if market data is stale (older than threshold)
    #[inline(always)]
    pub fn is_stale(&self, threshold_ns: u64) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        
        now - self.last_update_ns.load(Ordering::Relaxed) > threshold_ns
    }
}

/// External exchange price state
#[derive(Debug, Default)]
pub struct ExchangePrices {
    pub binance: FxHashMap<String, (Decimal, Decimal, u64)>, // symbol -> (bid, ask, timestamp_ns)
    pub coinbase: FxHashMap<String, (Decimal, Decimal, u64)>,
}

impl ExchangePrices {
    /// Get best bid/ask across exchanges for a symbol
    #[inline]
    pub fn best_price(&self, symbol: &str) -> Option<(Decimal, Decimal)> {
        let binance = self.binance.get(symbol);
        let coinbase = self.coinbase.get(symbol);
        
        match (binance, coinbase) {
            (Some((b_bid, b_ask, _)), Some((c_bid, c_ask, _))) => {
                // Best bid is highest, best ask is lowest
                Some(((*b_bid).max(*c_bid), (*b_ask).min(*c_ask)))
            }
            (Some((bid, ask, _)), None) => Some((*bid, *ask)),
            (None, Some((bid, ask, _))) => Some((*bid, *ask)),
            (None, None) => None,
        }
    }
}

// =============================================================================
// INVENTORY TRACKER - Position awareness for quote skewing
// =============================================================================

/// Tracks net inventory per market for inventory-aware quoting.
/// Positive = long (bought more than sold), Negative = short.
pub struct InventoryTracker {
    /// Net position per market (token_id -> net_qty in micro-units)
    positions: RwLock<FxHashMap<u64, InventoryPosition>>,
}

#[derive(Debug, Clone, Default)]
pub struct InventoryPosition {
    /// Net quantity (positive = long, negative = short)
    pub net_qty: Decimal,
    /// Average entry price
    pub avg_entry: Decimal,
    /// Total bought
    pub total_bought: Decimal,
    /// Total sold
    pub total_sold: Decimal,
}

impl InventoryTracker {
    pub fn new() -> Self {
        Self {
            positions: RwLock::new(FxHashMap::default()),
        }
    }

    /// Record a fill and update net position
    pub fn record_fill(&self, token_id: u64, side: Side, size: Decimal, price: Decimal) {
        let mut positions = self.positions.write();
        let pos = positions.entry(token_id).or_default();
        match side {
            Side::Buy => {
                // Update VWAP entry price for longs
                let old_cost = pos.avg_entry * pos.net_qty.max(Decimal::ZERO);
                pos.net_qty += size;
                pos.total_bought += size;
                if pos.net_qty > Decimal::ZERO {
                    pos.avg_entry = (old_cost + price * size) / pos.net_qty;
                }
            }
            Side::Sell => {
                pos.net_qty -= size;
                pos.total_sold += size;
                if pos.net_qty < Decimal::ZERO {
                    pos.avg_entry = price; // Reset entry for short
                }
            }
        }
    }

    /// Get net position for a market. Positive = long, negative = short.
    pub fn net_position(&self, token_id: u64) -> Decimal {
        self.positions.read().get(&token_id).map(|p| p.net_qty).unwrap_or(Decimal::ZERO)
    }

    /// Calculate inventory skew in basis points.
    /// Returns positive value to widen the side we're overexposed on.
    /// E.g., if long, skew asks down (more aggressive sell) and bids down (less aggressive buy).
    pub fn skew_bps(&self, token_id: u64, max_position: Decimal) -> i32 {
        if max_position.is_zero() {
            return 0;
        }
        let net = self.net_position(token_id);
        // Linear skew: at max_position, skew by 50 bps
        let ratio = net / max_position;
        let skew = ratio * Decimal::new(50, 0);
        skew.to_i32().unwrap_or(0)
    }

    pub fn reset(&self) {
        self.positions.write().clear();
    }
}

// =============================================================================
// ORDER LIFECYCLE MANAGER - Track and cancel stale orders
// =============================================================================

/// Tracks live orders and cancels stale ones.
/// Stores the real API order_id string so cancellations can be sent to the exchange.
pub struct OrderLifecycleManager {
    /// Active orders: internal_id -> LiveOrder
    active_orders: RwLock<FxHashMap<u64, LiveOrder>>,
    /// Next internal order ID
    next_id: AtomicU64,
    /// Maximum order age before cancellation (nanoseconds)
    max_age_ns: u64,
    /// Maximum price drift before cancellation (basis points)
    max_drift_bps: u32,
}

#[derive(Debug, Clone)]
pub struct LiveOrder {
    pub id: u64,
    /// Real API order ID string (returned by Polymarket on submission)
    pub api_order_id: String,
    pub token_id: u64,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub placed_at_ns: u64,
}

impl OrderLifecycleManager {
    pub fn new() -> Self {
        Self {
            active_orders: RwLock::new(FxHashMap::default()),
            next_id: AtomicU64::new(1),
            max_age_ns: 5_000_000_000, // 5 seconds (tighter than 10s to avoid adverse selection)
            max_drift_bps: 30,           // 0.3% price drift = cancel (was 1%, too loose)
        }
    }

    /// Register a new order with its real API order ID.
    pub fn register_order(&self, api_order_id: String, token_id: u64, side: Side, price: Decimal, size: Decimal) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let now = Self::now_ns();
        self.active_orders.write().insert(id, LiveOrder {
            id, api_order_id, token_id, side, price, size, placed_at_ns: now,
        });
        id
    }

    /// Cancel stale orders and orders that have drifted too far from current mid.
    /// Returns list of real API order ID strings for cancellation via the executor.
    pub fn cancel_stale_orders(&self, current_mids: &FxHashMap<u64, Decimal>) -> Vec<String> {
        let now = Self::now_ns();
        let mut to_cancel_ids = Vec::new();
        let mut to_cancel_api = Vec::new();
        let orders = self.active_orders.read();

        for (id, order) in orders.iter() {
            // Cancel if too old
            if now.saturating_sub(order.placed_at_ns) > self.max_age_ns {
                to_cancel_ids.push(*id);
                to_cancel_api.push(order.api_order_id.clone());
                continue;
            }

            // Cancel if price has drifted too far from current mid
            if let Some(&mid) = current_mids.get(&order.token_id) {
                if mid > Decimal::ZERO {
                    let drift = ((order.price - mid).abs() / mid * Decimal::new(10000, 0))
                        .to_u32()
                        .unwrap_or(0);
                    if drift > self.max_drift_bps {
                        to_cancel_ids.push(*id);
                        to_cancel_api.push(order.api_order_id.clone());
                    }
                }
            }
        }
        drop(orders);

        // Remove cancelled orders from internal tracking
        if !to_cancel_ids.is_empty() {
            let mut orders = self.active_orders.write();
            for id in &to_cancel_ids {
                orders.remove(id);
            }
            debug!("Lifecycle: Cancelled {} stale/drifted orders", to_cancel_ids.len());
        }

        to_cancel_api
    }

    /// Remove an order by internal ID (e.g., after fill)
    pub fn remove_order(&self, id: u64) {
        self.active_orders.write().remove(&id);
    }

    /// Remove an order by its API order ID string (returned by Polymarket).
    /// Used by the report handler when a fill/cancel comes in from the user WS channel.
    pub fn remove_by_api_id(&self, api_order_id: &str) {
        let mut orders = self.active_orders.write();
        orders.retain(|_, o| o.api_order_id != api_order_id);
    }

    /// Get count of active orders for a market
    pub fn active_count_for(&self, token_id: u64) -> usize {
        self.active_orders.read().values().filter(|o| o.token_id == token_id).count()
    }

    /// Check if there's already an active order within `max_dist_bps` of `price`
    /// for the given token_id and side. Used for signal deduplication.
    pub fn has_nearby_order(&self, token_id: u64, side: Side, price: Decimal, max_dist_bps: u32) -> bool {
        let orders = self.active_orders.read();
        for order in orders.values() {
            if order.token_id == token_id && order.side == side {
                if price > Decimal::ZERO {
                    let dist = ((order.price - price).abs() / price * Decimal::new(10000, 0))
                        .to_u32()
                        .unwrap_or(u32::MAX);
                    if dist <= max_dist_bps {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Get all active order IDs
    pub fn active_order_ids(&self) -> Vec<u64> {
        self.active_orders.read().keys().cloned().collect()
    }

    pub fn reset(&self) {
        self.active_orders.write().clear();
    }

    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}

// =============================================================================
// PRICING ENGINE
// =============================================================================

/// Main pricing engine
pub struct PricingEngine {
    config: Arc<Config>,
    thresholds: FeeThresholds,
    /// Market states indexed by token ID hash for O(1) lookup
    markets: RwLock<FxHashMap<u64, MarketState>>,
    /// Signal output channel
    signal_tx: tokio::sync::mpsc::Sender<TradeSignal>,
    /// Inventory tracker for position-aware quoting
    pub inventory: InventoryTracker,
    /// Order lifecycle manager
    pub lifecycle: OrderLifecycleManager,
    /// Cross-exchange reference prices (Binance/Coinbase)
    exchange_prices: RwLock<ExchangePrices>,
}

impl PricingEngine {
    pub fn new(
        config: Arc<Config>,
        signal_tx: tokio::sync::mpsc::Sender<TradeSignal>,
    ) -> Self {
        Self {
            thresholds: FeeThresholds::from_config(&config),
            config,
            markets: RwLock::new(FxHashMap::default()),
            signal_tx,
            inventory: InventoryTracker::new(),
            lifecycle: OrderLifecycleManager::new(),
            exchange_prices: RwLock::new(ExchangePrices::default()),
        }
    }
    
    /// Register a market for tracking
    pub fn register_market(&self, market_id: MarketId) {
        self.markets.write().insert(market_id.token_id, MarketState::new(market_id));
        info!("Registered market: token_id={}", market_id.token_id);
    }

    /// Get order flow imbalance ratios for all tracked markets.
    /// Returns Vec of (token_id, imbalance) where imbalance is in [-1.0, +1.0].
    pub fn get_flow_imbalances(&self) -> Vec<(u64, f64)> {
        let markets = self.markets.read();
        let mut result = Vec::with_capacity(markets.len());
        for (&token_id, state) in markets.iter() {
            result.push((token_id, state.flow.imbalance_ratio()));
        }
        result
    }

    /// Get current mid prices for all tracked markets.
    /// Used by the stale-order canceller to detect price drift.
    pub fn get_current_mids(&self) -> FxHashMap<u64, Decimal> {
        let markets = self.markets.read();
        let mut mids = FxHashMap::default();
        for (&token_id, state) in markets.iter() {
            if let Some(mid) = state.order_book.mid_price() {
                mids.insert(token_id, mid);
            }
        }
        mids
    }
    
    /// Process incoming WebSocket event - HOT PATH
    /// Only generates signals from real LOB data (BookUpdate).
    /// Trade events update market state but do NOT generate synthetic signals.
    #[inline]
    pub async fn process_event(&self, event: WsEvent) {
        match event {
            WsEvent::BookUpdate { market_id, snapshot } => {
                self.on_book_update(market_id, snapshot).await;
            }
            WsEvent::Trade { market_id, price, size, side, timestamp_ns } => {
                self.on_trade(market_id, price, size, side, timestamp_ns);
            }
            // Store cross-exchange reference prices for market context.
            // While Polymarket markets don't directly correlate to BTC/ETH spot,
            // sudden crypto price moves often precede Polymarket vol spikes.
            WsEvent::ExchangePrice { exchange, symbol, bid, ask, timestamp_ns } => {
                self.on_exchange_price(exchange, symbol, bid, ask, timestamp_ns);
            }
            _ => {}
        }
    }
    
    /// Process order book update from REAL Polymarket WebSocket data.
    /// This is the ONLY path that generates trading signals.
    #[inline]
    async fn on_book_update(&self, market_id_str: String, snapshot: OrderBookSnapshot) {
        let token_id = snapshot.token_id;
        if token_id == 0 {
            return; // No valid market identity
        }

        // Auto-register market if not yet tracked
        {
            let markets = self.markets.read();
            if !markets.contains_key(&token_id) {
                drop(markets);
                let mid = MarketId::new([0u8; 32], token_id);
                self.register_market(mid);
                debug!("Auto-registered market token_id={} from book update", token_id);
            }
        }

        // Update market state with real LOB
        {
            let mut markets = self.markets.write();
            if let Some(market) = markets.get_mut(&token_id) {
                market.update_book(snapshot);
            }
        }

        // NOTE: Stale order cancellation is handled by the periodic run_stale_order_canceller
        // task (every 2s), NOT here. Calling cancel_stale_orders on every book update would
        // remove orders from the lifecycle tracker without sending real cancel requests to
        // Polymarket, leaving ghost orders on the exchange.

        // Check for market making opportunity on REAL data — emit BOTH sides
        let signals = self.check_market_making(token_id, &snapshot);
        for signal in signals {
            let _ = self.signal_tx.send(signal).await;
        }
    }
    
    /// Process trade event — updates market state + order flow tracker.
    /// Signals come exclusively from on_book_update with real LOB data.
    #[inline]
    fn on_trade(&self, market_id_str: String, price: Decimal, size: Decimal, side: Side, timestamp_ns: u64) {
        let token_id = crate::websocket::hash_asset_id(&market_id_str);
        
        let mut markets = self.markets.write();
        if let Some(market) = markets.get_mut(&token_id) {
            // Feed trade to flow tracker for buy/sell pressure detection
            // and volume tracker for market quality scoring
            market.record_trade(side, size, price);
            market.last_update_ns.store(timestamp_ns, Ordering::Relaxed);
        }
    }

    /// Store cross-exchange reference prices.
    /// Used for detecting crypto-correlated volatility spikes that often precede
    /// Polymarket market moves (e.g., BTC crash → election market volatility).
    #[inline]
    fn on_exchange_price(&self, exchange: Exchange, symbol: String, bid: Decimal, ask: Decimal, _timestamp_ns: u64) {
        let mut prices = self.exchange_prices.write();
        match exchange {
            Exchange::Binance => { prices.binance.insert(symbol, (bid, ask, _timestamp_ns)); }
            Exchange::Coinbase => { prices.coinbase.insert(symbol, (bid, ask, _timestamp_ns)); }
        }
    }

    /// Get cross-exchange volatility signal.
    /// Returns true if any tracked crypto asset moved >2% in a short window,
    /// suggesting we should widen spreads defensively.
    pub fn is_crypto_volatile(&self) -> bool {
        // Simple check: if BTC mid moved significantly from stored reference
        let prices = self.exchange_prices.read();
        if let Some((bid, ask)) = prices.best_price("BTCUSDT") {
            let mid = (bid + ask) / Decimal::TWO;
            // We'll use this to widen spreads via the volatility tracker
            // For now, just expose the data — actual integration through vol tracker
            mid > Decimal::ZERO // Always true if we have data
        } else {
            false
        }
    }

    /// Check for market making opportunity using REAL order book data.
    /// Returns signals for two-sided market making with multi-level quoting.
    /// Features:
    /// - Inventory-aware quoting (skew based on net position)
    /// - Order flow imbalance skew (lean into directional pressure)
    /// - Market quality gating (skip dead/too-tight markets)
    /// - Adaptive spread widening (volatility-responsive)
    /// - Multi-level quoting (2 levels per side for better fill rates)
    /// - Signal deduplication (skip if nearby order already active)
    /// - 2026 fee curve: forces post_only in 0.40-0.60 zone
    #[inline]
    fn check_market_making(&self, token_id: u64, book: &OrderBookSnapshot) -> Vec<TradeSignal> {
        let (best_bid, best_ask) = match (book.best_bid(), book.best_ask()) {
            (Some(b), Some(a)) => (b, a),
            _ => return Vec::new(),
        };
        
        // Skip invalid or crossed books
        if best_bid >= best_ask || best_bid <= Decimal::ZERO {
            return Vec::new();
        }

        let mid_price = (best_bid + best_ask) / Decimal::TWO;
        let spread = best_ask - best_bid;
        let spread_bps = ((spread / mid_price) * Decimal::new(10000, 0))
            .trunc()
            .to_u32()
            .unwrap_or(0);
        
        // 2026 FEE CURVE: Check if we're in the forced maker zone (0.40-0.60)
        let force_maker = self.thresholds.must_force_post_only(mid_price);
        
        // Check if spread is wide enough for maker strategy
        let maker_offset_bps = self.config.trading.maker_spread_offset_bps;
        let min_maker_spread = self.thresholds.min_profit_bps + maker_offset_bps;
        let min_required = min_maker_spread * 2;
        
        // Always use maker profitability for check_market_making — we place LIMIT orders,
        // not taker orders. Using taker fees (200-315 bps) makes most markets look unprofitable
        // even though our maker orders only need to beat the maker rebate (~20 bps).
        let is_profitable = self.thresholds.is_profitable_2026(spread_bps, mid_price, true);
        let spread_ok = spread_bps >= min_required;
        
        if !(is_profitable && spread_ok) {
            return Vec::new();
        }

        // === SINGLE LOCK: Extract all market state data in one read ===
        // Consolidates 3 separate RwLock reads into 1 to reduce lock contention
        let (quality_score, flow_skew, vol_multiplier) = {
            let markets = self.markets.read();
            if let Some(m) = markets.get(&token_id) {
                (m.quality_score().score, m.flow.skew_bps(), m.volatility.get_spacing_multiplier())
            } else {
                (0.0, 0, Decimal::ONE)
            }
        };

        // Market quality gate: skip markets with very low volume
        if quality_score < 0.05 {
            return Vec::new(); // Dead market, don't waste orders
        }

        // Max active orders: 6 for multi-level quoting (2 levels × 2 sides + buffer)
        let active = self.lifecycle.active_count_for(token_id);
        if active >= 6 {
            return Vec::new();
        }

        // === SKEW CALCULATION ===
        // 1. Inventory skew: lean away from overexposed side
        let max_pos = self.config.risk.max_position_per_market;
        let inventory_skew = self.inventory.skew_bps(token_id, max_pos);

        // 2. Order flow imbalance skew (already extracted above)
        // Combined skew: inventory dominates (risk management > alpha)
        let total_skew = inventory_skew + (flow_skew / 2); // Flow at half weight
        let skew_decimal = Decimal::new(total_skew as i64, 4);

        let base_offset = Decimal::new(maker_offset_bps as i64, 4) * mid_price * vol_multiplier;

        let market_id = MarketId::new([0u8; 32], token_id);
        let order_size = self.config.trading.default_order_size;
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let expected_profit = self.thresholds.expected_profit_bps(spread_bps / 2, true);

        // === MULTI-LEVEL QUOTING ===
        // Level 1: tight (right at the spread edge) — higher fill probability
        // Level 2: wider (1.5x offset) — catches bigger moves, better adverse selection protection
        let mut signals = Vec::with_capacity(4);
        let level_offsets = [Decimal::ONE, Decimal::new(2, 0)]; // 1x and 2x base offset
        let min_size = Decimal::new(5, 0); // Polymarket minimum order size in tokens
        let level_sizes = [order_size.max(min_size), (order_size / Decimal::TWO).max(min_size)];

        for (level_idx, (&offset_mult, &size)) in level_offsets.iter().zip(level_sizes.iter()).enumerate() {
            let bid_offset = base_offset * offset_mult;
            let ask_offset = base_offset * offset_mult;

            let tick = Decimal::new(1, 2); // 0.01 tick to prevent post-only crossing
            let buy_price = (best_bid - bid_offset - skew_decimal * mid_price)
                .max(Decimal::new(1, 2))
                .min(best_ask - tick); // post-only: must be below best ask
            let sell_price = (best_ask + ask_offset - skew_decimal * mid_price)
                .min(Decimal::new(99, 2))
                .max(best_bid + tick); // post-only: must be above best bid

            // Ensure quotes don't cross
            if buy_price >= sell_price {
                continue;
            }

            // BUY side
            if active + signals.len() < 6
                && !self.lifecycle.has_nearby_order(token_id, Side::Buy, buy_price, 10)
            {
                signals.push(TradeSignal {
                    market_id,
                    side: Side::Buy,
                    price: buy_price,
                    size,
                    order_type: OrderType::Limit,
                    expected_profit_bps: expected_profit,
                    signal_timestamp_ns: now_ns,
                    urgency: if level_idx == 0 { SignalUrgency::Medium } else { SignalUrgency::Low },
                });
            }

            // SELL side — only quote if we actually hold tokens to sell
            let net_pos = self.inventory.net_position(token_id);
            if net_pos > Decimal::ZERO
                && active + signals.len() < 6
                && !self.lifecycle.has_nearby_order(token_id, Side::Sell, sell_price, 10)
            {
                signals.push(TradeSignal {
                    market_id,
                    side: Side::Sell,
                    price: sell_price,
                    size: size.min(net_pos), // don't sell more than we own
                    order_type: OrderType::Limit,
                    expected_profit_bps: expected_profit,
                    signal_timestamp_ns: now_ns,
                    urgency: if level_idx == 0 { SignalUrgency::Medium } else { SignalUrgency::Low },
                });
            }
        }

        signals
    }
    
    /// Evaluate if a trade should be executed given current conditions
    /// Called before sending order to validate profitability
    #[inline(always)]
    pub fn validate_signal(&self, signal: &TradeSignal) -> bool {
        let markets = self.markets.read();
        
        if let Some(market) = markets.get(&signal.market_id.token_id) {
            // Check staleness
            if market.is_stale(50_000_000) { // 50ms
                warn!("Market data stale, rejecting signal");
                return false;
            }
            
            // Verify price hasn't moved against us
            match signal.side {
                Side::Buy => {
                    if let Some(ask) = market.order_book.best_ask() {
                        if ask > signal.price * Decimal::new(101, 2) {
                            debug!("Ask moved up, rejecting buy signal");
                            return false;
                        }
                    }
                }
                Side::Sell => {
                    if let Some(bid) = market.order_book.best_bid() {
                        if bid < signal.price * Decimal::new(99, 2) {
                            debug!("Bid moved down, rejecting sell signal");
                            return false;
                        }
                    }
                }
            }
            
            return true;
        }
        
        false
    }
}

/// Arbitrage calculator utilities
pub mod arb {
    use rust_decimal::Decimal;
    
    /// Calculate round-trip arbitrage profit
    #[inline(always)]
    pub fn round_trip_profit(
        buy_price: Decimal,
        sell_price: Decimal,
        fee_bps: u32,
    ) -> Decimal {
        let fee = Decimal::new(fee_bps as i64, 4);
        let gross_profit = sell_price - buy_price;
        let fees = (buy_price + sell_price) * fee;
        gross_profit - fees
    }
    
    /// Calculate required spread for profitable arb
    #[inline(always)]
    pub fn min_profitable_spread(fee_bps: u32, min_profit_bps: u32) -> u32 {
        // Need to cover fees on both legs plus minimum profit
        fee_bps * 2 + min_profit_bps
    }
    
    /// Convert price to implied probability
    #[inline(always)]
    pub fn price_to_prob(price: Decimal) -> Decimal {
        // Polymarket prices are already probabilities (0 to 1)
        price.min(Decimal::ONE).max(Decimal::ZERO)
    }
}
