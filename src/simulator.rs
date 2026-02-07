//! Queue Position Fill Simulator
//!
//! Isolated fill simulation logic with realistic queue position modeling.
//! Uses conditional compilation to ensure zero overhead in live mode.
//!
//! Features:
//! - Volume-ahead queue tracking from LOB snapshots
//! - Cumulative trade volume tracking per price level
//! - 50ms minimum order life (latency simulation)
//! - Thread-safe using DashMap and parking_lot
//! - Zero-overhead for live trading via `Simulated` trait

use crate::config::Config;
use crate::types::{MarketId, Side, OrderType, TradeSignal, PreparedOrder, OrderBookSnapshot};
use dashmap::DashMap;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::sync::atomic::{AtomicU64, AtomicI64, AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};
use crate::shadow::{TokenBucket, PositionTracker};

// =============================================================================
// CONSTANTS
// =============================================================================

/// Minimum order life before eligible for fills (50ms in nanoseconds)
const MIN_ORDER_LIFE_NS: u64 = 50_000_000;

/// Maximum age for stale orders (30 seconds)
const MAX_ORDER_AGE_NS: u64 = 30_000_000_000;

/// Maximum active orders
const MAX_ACTIVE_ORDERS: usize = 50;

/// Slippage: 1 basis point per $1,000 of order size
const SLIPPAGE_BPS_PER_1000_USD: Decimal = Decimal::ONE;

/// Self-impact threshold: order size > 1% of daily volume triggers slippage
const SELF_IMPACT_THRESHOLD_PCT: Decimal = Decimal::ONE; // 1%

// =============================================================================
// SIMULATED TRAIT - Zero-overhead abstraction
// =============================================================================

/// Trait for simulation-aware components.
/// Live mode implementations are no-ops with #[inline(always)] for complete elimination.
pub trait Simulated {
    /// Called when an order is placed - record queue position
    fn on_order_placed(&self, _order_id: u64, _signal: &TradeSignal, _lob: Option<&OrderBookSnapshot>) {}
    
    /// Called when a trade occurs - check for fills
    fn on_trade(&self, _market_id: &MarketId, _side: Side, _price: Decimal, _size: Decimal, _prob: Decimal) -> Vec<SimulatedFill> {
        Vec::new()
    }
    
    /// Check if simulation is enabled
    fn is_simulating(&self) -> bool { false }
}

/// No-op implementation for live trading - completely optimized away
pub struct LiveMode;

impl Simulated for LiveMode {
    #[inline(always)]
    fn on_order_placed(&self, _: u64, _: &TradeSignal, _: Option<&OrderBookSnapshot>) {}
    
    #[inline(always)]
    fn on_trade(&self, _: &MarketId, _: Side, _: Decimal, _: Decimal, _: Decimal) -> Vec<SimulatedFill> {
        Vec::new()
    }
    
    #[inline(always)]
    fn is_simulating(&self) -> bool { false }
}

// =============================================================================
// SIMULATED ORDER
// =============================================================================

/// Order state in the simulator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SimOrderState {
    Pending = 0,
    Open = 1,
    PartiallyFilled = 2,
    Filled = 3,
    Cancelled = 4,
}

/// A simulated order with queue position tracking
#[derive(Debug, Clone)]
pub struct SimulatedOrder {
    pub id: u64,
    pub market_id: MarketId,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub filled: Decimal,
    pub order_type: OrderType,
    pub state: SimOrderState,
    pub is_maker: bool,
    pub expected_profit_bps: i32,
    
    // Queue position tracking
    /// Volume ahead in queue when order was placed
    pub volume_ahead: Decimal,
    /// Cumulative trade volume at this price since order placement
    pub cumulative_volume: Decimal,
    
    // Timing
    /// Order creation timestamp (nanoseconds)
    pub created_at_ns: u64,
    /// Earliest time order can be filled (created_at + MIN_ORDER_LIFE)
    pub eligible_at_ns: u64,
    /// Fill timestamp
    pub filled_at_ns: Option<u64>,
}

impl SimulatedOrder {
    /// Create from a trade signal with LOB snapshot for queue position
    pub fn new(
        id: u64,
        signal: &TradeSignal,
        is_maker: bool,
        lob: Option<&OrderBookSnapshot>,
    ) -> Self {
        let now = Self::now_ns();
        let volume_ahead = Self::calculate_volume_ahead(signal.side, signal.price, lob);
        
        Self {
            id,
            market_id: signal.market_id,
            side: signal.side,
            price: signal.price,
            size: signal.size,
            filled: Decimal::ZERO,
            order_type: signal.order_type,
            state: SimOrderState::Open,
            is_maker,
            expected_profit_bps: signal.expected_profit_bps,
            volume_ahead,
            cumulative_volume: Decimal::ZERO,
            created_at_ns: now,
            eligible_at_ns: now + MIN_ORDER_LIFE_NS,
            filled_at_ns: None,
        }
    }

    /// Create from a prepared order
    pub fn from_prepared(
        id: u64,
        order: &PreparedOrder,
        is_maker: bool,
        expected_profit_bps: i32,
        lob: Option<&OrderBookSnapshot>,
    ) -> Self {
        let now = Self::now_ns();
        let volume_ahead = Self::calculate_volume_ahead(order.side, order.price, lob);
        
        Self {
            id,
            market_id: order.market_id,
            side: order.side,
            price: order.price,
            size: order.size,
            filled: Decimal::ZERO,
            order_type: order.order_type,
            state: SimOrderState::Open,
            is_maker,
            expected_profit_bps,
            volume_ahead,
            cumulative_volume: Decimal::ZERO,
            created_at_ns: now,
            eligible_at_ns: now + MIN_ORDER_LIFE_NS,
            filled_at_ns: None,
        }
    }

    /// Calculate volume ahead from LOB snapshot
    pub fn calculate_volume_ahead(side: Side, price: Decimal, lob: Option<&OrderBookSnapshot>) -> Decimal {
        let Some(book) = lob else {
            return Decimal::ZERO;
        };

        match side {
            Side::Buy => {
                // For buy orders, sum volume at same or better (higher) bid prices
                let mut volume = Decimal::ZERO;
                for i in 0..book.bid_count as usize {
                    if book.bids[i].price >= price {
                        volume += book.bids[i].size;
                    }
                }
                volume
            }
            Side::Sell => {
                // For sell orders, sum volume at same or better (lower) ask prices
                let mut volume = Decimal::ZERO;
                for i in 0..book.ask_count as usize {
                    if book.asks[i].price <= price {
                        volume += book.asks[i].size;
                    }
                }
                volume
            }
        }
    }

    #[inline(always)]
    pub fn remaining(&self) -> Decimal {
        self.size - self.filled
    }

    #[inline(always)]
    pub fn is_active(&self) -> bool {
        matches!(self.state, SimOrderState::Open | SimOrderState::PartiallyFilled)
    }

    /// Check if order has met minimum life requirement
    #[inline(always)]
    pub fn is_eligible_for_fill(&self) -> bool {
        self.is_active() && Self::now_ns() >= self.eligible_at_ns
    }

    /// Check if order should fill based on queue position
    /// Fills if: trade crosses price OR cumulative volume > volume_ahead
    #[inline]
    pub fn should_fill(&self, trade_price: Decimal, trade_crosses: bool) -> bool {
        if !self.is_eligible_for_fill() {
            return false;
        }

        // Fill if trade crosses our price (aggressive fill)
        if trade_crosses {
            return true;
        }

        // Fill if cumulative volume at our price exceeds our queue position
        self.cumulative_volume > self.volume_ahead
    }

    /// Add to cumulative volume when trade occurs at this price
    #[inline(always)]
    pub fn add_cumulative_volume(&mut self, volume: Decimal) {
        self.cumulative_volume += volume;
    }

    /// Mark order as filled
    pub fn mark_filled(&mut self) {
        self.state = SimOrderState::Filled;
        self.filled = self.size;
        self.filled_at_ns = Some(Self::now_ns());
    }

    /// Partially fill the order
    pub fn partial_fill(&mut self, qty: Decimal) {
        self.filled += qty;
        if self.filled >= self.size {
            self.mark_filled();
        } else {
            self.state = SimOrderState::PartiallyFilled;
        }
    }

    /// Cancel the order
    pub fn cancel(&mut self) {
        self.state = SimOrderState::Cancelled;
    }

    #[inline(always)]
    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}

// =============================================================================
// SIMULATED FILL
// =============================================================================

/// Result of a simulated fill
#[derive(Debug, Clone)]
pub struct SimulatedFill {
    pub order_id: u64,
    pub market_id: MarketId,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub fee_bps: i32,
    pub net_pnl: Decimal,
    pub is_maker: bool,
    pub timestamp_ns: u64,
    /// Was this fill due to queue exhaustion or price crossing?
    pub fill_reason: FillReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillReason {
    /// Trade price crossed our order price
    PriceCross,
    /// Cumulative volume exceeded our queue position
    QueueExhausted,
}

// =============================================================================
// SIMULATED ORDER BOOK
// =============================================================================

/// Thread-safe simulated order book with queue position tracking
pub struct SimulatedOrderBook {
    /// Orders by ID
    orders: DashMap<u64, SimulatedOrder>,
    /// Price index: (market_hash, side, price_cents) -> order_ids
    price_index: DashMap<(u64, u8, i64), Vec<u64>>,
    /// Cumulative volume tracker: (market_hash, side, price_cents) -> cumulative_volume
    cumulative_volumes: DashMap<(u64, u8, i64), Decimal>,
}

impl SimulatedOrderBook {
    pub fn new() -> Self {
        Self {
            orders: DashMap::new(),
            price_index: DashMap::new(),
            cumulative_volumes: DashMap::new(),
        }
    }

    /// Add an order to the book
    pub fn add_order(&self, order: SimulatedOrder) {
        let order_id = order.id;
        let index_key = Self::make_index_key(&order.market_id, order.side, order.price);
        
        self.orders.insert(order_id, order);
        self.price_index
            .entry(index_key)
            .or_insert_with(Vec::new)
            .push(order_id);
    }

    /// Update cumulative volume for a price level and update all orders at that level
    pub fn add_trade_volume(
        &self,
        market_id: &MarketId,
        side: Side,
        price: Decimal,
        volume: Decimal,
    ) {
        let index_key = Self::make_index_key(market_id, side, price);
        
        // Update cumulative volume tracker
        self.cumulative_volumes
            .entry(index_key)
            .and_modify(|v| *v += volume)
            .or_insert(volume);

        // Update all orders at this price level
        if let Some(order_ids) = self.price_index.get(&index_key) {
            for order_id in order_ids.iter() {
                if let Some(mut order) = self.orders.get_mut(order_id) {
                    if order.is_active() {
                        order.add_cumulative_volume(volume);
                    }
                }
            }
        }
    }

    /// Get orders eligible for fill at favorable prices
    pub fn get_fillable_orders(
        &self,
        market_id: &MarketId,
        trade_side: Side,
        trade_price: Decimal,
    ) -> Vec<SimulatedOrder> {
        let matching_side = trade_side.opposite();
        
        self.orders
            .iter()
            .filter(|o| {
                o.is_eligible_for_fill() &&
                o.side == matching_side &&
                Self::hash_market_id(&o.market_id) == Self::hash_market_id(market_id)
            })
            .filter(|o| {
                // Check if trade crosses our order price
                match matching_side {
                    Side::Sell => trade_price >= o.price,
                    Side::Buy => trade_price <= o.price,
                }
            })
            .map(|o| o.clone())
            .collect()
    }

    /// Fill an order
    pub fn fill_order(&self, order_id: u64, fill_size: Decimal) -> Option<SimulatedOrder> {
        self.orders.get_mut(&order_id).map(|mut order| {
            order.partial_fill(fill_size);
            order.clone()
        })
    }

    /// Cancel an order (immediate - used internally)
    pub fn cancel_order(&self, order_id: u64) -> Option<SimulatedOrder> {
        self.orders.get_mut(&order_id).map(|mut order| {
            order.cancel();
            order.clone()
        })
    }

    /// Cancel an order with latency simulation (50ms buffer).
    /// Returns Err if the order is still in its latency window.
    pub fn cancel_order_with_latency(&self, order_id: u64) -> Result<Option<SimulatedOrder>, &'static str> {
        if let Some(order) = self.orders.get(&order_id) {
            if !order.is_active() {
                return Ok(None);
            }
            let now = SimulatedOrder::now_ns();
            if now < order.eligible_at_ns {
                return Err("cancel rejected: order still in latency window");
            }
        } else {
            return Ok(None);
        }
        Ok(self.cancel_order(order_id))
    }

    /// Public accessor for market hash
    pub fn hash_market_id_pub(market_id: &MarketId) -> u64 {
        Self::hash_market_id(market_id)
    }

    /// Get all active orders
    pub fn active_orders(&self) -> Vec<SimulatedOrder> {
        self.orders
            .iter()
            .filter(|o| o.is_active())
            .map(|o| o.clone())
            .collect()
    }

    /// Get active order count
    pub fn active_order_count(&self) -> usize {
        self.orders.iter().filter(|o| o.is_active()).count()
    }

    /// Cancel stale orders
    pub fn cancel_stale_orders(&self) -> usize {
        let now = SimulatedOrder::now_ns();
        let mut cancelled = 0;
        
        for mut entry in self.orders.iter_mut() {
            if entry.is_active() && (now - entry.created_at_ns) > MAX_ORDER_AGE_NS {
                entry.cancel();
                cancelled += 1;
            }
        }
        cancelled
    }

    /// Enforce order limit
    pub fn enforce_order_limit(&self) -> usize {
        let active_count = self.active_order_count();
        if active_count <= MAX_ACTIVE_ORDERS {
            return 0;
        }

        let mut active: Vec<_> = self.orders
            .iter()
            .filter(|o| o.is_active())
            .map(|o| (o.id, o.created_at_ns))
            .collect();
        
        active.sort_by_key(|(_, ts)| *ts);
        
        let to_cancel = active_count - MAX_ACTIVE_ORDERS;
        let mut cancelled = 0;
        
        for (order_id, _) in active.iter().take(to_cancel) {
            if let Some(mut order) = self.orders.get_mut(order_id) {
                order.cancel();
                cancelled += 1;
            }
        }
        cancelled
    }

    fn make_index_key(market_id: &MarketId, side: Side, price: Decimal) -> (u64, u8, i64) {
        (
            Self::hash_market_id(market_id),
            side as u8,
            Self::price_to_cents(price),
        )
    }

    fn hash_market_id(market_id: &MarketId) -> u64 {
        let mut hash = market_id.token_id;
        for (i, byte) in market_id.condition_id.iter().enumerate() {
            hash ^= (*byte as u64) << ((i % 8) * 8);
        }
        hash
    }

    fn price_to_cents(price: Decimal) -> i64 {
        (price * Decimal::new(100, 0))
            .trunc()
            .to_string()
            .parse::<i64>()
            .unwrap_or(0)
    }
}

impl Default for SimulatedOrderBook {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// FILL SIMULATOR
// =============================================================================

/// Queue Position Fill Simulator
/// 
/// Isolated from GridEngine - handles all fill simulation logic.
/// Implements the `Simulated` trait for zero-overhead live mode.
pub struct FillSimulator {
    config: Arc<Config>,
    
    /// Simulated order book with queue tracking
    order_book: SimulatedOrderBook,
    
    /// Order ID generator
    next_order_id: AtomicU64,
    
    /// Balance tracking (micro-cents)
    virtual_balance_micro: AtomicI64,
    initial_balance_micro: i64,
    
    /// PnL tracking (micro-cents)
    realized_pnl_micro: AtomicI64,
    
    /// Trade counters
    total_trades: AtomicU64,
    winning_trades: AtomicU64,
    losing_trades: AtomicU64,
    maker_fills: AtomicU64,
    taker_fills: AtomicU64,
    
    /// Fee tracking (micro-cents)
    total_fees_micro: AtomicI64,
    total_rebates_micro: AtomicI64,
    
    /// Fill history
    fill_history: RwLock<Vec<SimulatedFill>>,
    
    /// Current LOB snapshot for queue position calculation
    current_lob: RwLock<Option<OrderBookSnapshot>>,
    
    /// Enabled flag
    enabled: AtomicBool,

    // ---- Reality Checks ----
    
    /// API rate limiter (60 orders/sec, burst 60)
    rate_limiter: TokenBucket,
    
    /// Orders dropped due to rate limiting
    orders_throttled: AtomicU64,
    
    /// Position tracker for mark-to-market unrealized PnL
    position_tracker: PositionTracker,
    
    /// Estimated daily volume for self-impact calculation (USDC)
    daily_volume_usd: RwLock<Decimal>,
    
    /// Cumulative slippage applied (micro-cents)
    total_slippage_micro: AtomicI64,
}

impl FillSimulator {
    /// Create a new fill simulator
    pub fn new(config: Arc<Config>) -> Self {
        Self::with_balance(config, Decimal::new(10000, 0))
    }

    /// Create with custom initial balance
    pub fn with_balance(config: Arc<Config>, initial_balance: Decimal) -> Self {
        let balance_micro = Self::to_micro(initial_balance);
        
        Self {
            config,
            order_book: SimulatedOrderBook::new(),
            next_order_id: AtomicU64::new(1),
            virtual_balance_micro: AtomicI64::new(balance_micro),
            initial_balance_micro: balance_micro,
            realized_pnl_micro: AtomicI64::new(0),
            total_trades: AtomicU64::new(0),
            winning_trades: AtomicU64::new(0),
            losing_trades: AtomicU64::new(0),
            maker_fills: AtomicU64::new(0),
            taker_fills: AtomicU64::new(0),
            total_fees_micro: AtomicI64::new(0),
            total_rebates_micro: AtomicI64::new(0),
            fill_history: RwLock::new(Vec::new()),
            current_lob: RwLock::new(None),
            enabled: AtomicBool::new(true),
            // Reality checks
            rate_limiter: TokenBucket::new(60, 60),
            orders_throttled: AtomicU64::new(0),
            position_tracker: PositionTracker::new(),
            daily_volume_usd: RwLock::new(Decimal::new(500_000, 0)),
            total_slippage_micro: AtomicI64::new(0),
        }
    }

    /// Update LOB snapshot for queue position calculation
    pub fn update_lob(&self, snapshot: OrderBookSnapshot) {
        *self.current_lob.write() = Some(snapshot);
    }

    /// Track a new order with queue position from current LOB.
    /// Returns None if rate-limited (simulates API 429 rejection).
    pub fn track_order(&self, signal: &TradeSignal, is_maker: bool) -> Option<u64> {
        // Reality Check 1: API rate limiting (60 orders/sec)
        if !self.rate_limiter.try_acquire() {
            let throttled = self.orders_throttled.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(
                throttled_total = throttled,
                side = ?signal.side,
                price = %signal.price,
                "🚫 Simulator: Order THROTTLED (rate limit 60/sec exceeded)"
            );
            return None;
        }

        // Order management
        let stale = self.order_book.cancel_stale_orders();
        let limited = self.order_book.enforce_order_limit();
        
        if stale > 0 || limited > 0 {
            debug!(stale, limited, "🗑️ Simulator: Cancelled old orders");
        }

        let order_id = self.next_order_id.fetch_add(1, Ordering::Relaxed);
        let lob = self.current_lob.read();
        let order = SimulatedOrder::new(order_id, signal, is_maker, lob.as_ref());
        
        info!(
            order_id,
            side = ?signal.side,
            price = %signal.price,
            size = %signal.size,
            volume_ahead = %order.volume_ahead,
            eligible_in_ms = MIN_ORDER_LIFE_NS / 1_000_000,
            "📝 Simulator: Tracking order (queue pos: {})",
            order.volume_ahead
        );
        
        self.order_book.add_order(order);
        Some(order_id)
    }

    /// Track a prepared order. Returns None if rate-limited.
    pub fn track_prepared_order(&self, order: &PreparedOrder, is_maker: bool, expected_profit_bps: i32) -> Option<u64> {
        // Reality Check 1: API rate limiting
        if !self.rate_limiter.try_acquire() {
            self.orders_throttled.fetch_add(1, Ordering::Relaxed);
            warn!("🚫 Simulator: Prepared order THROTTLED (rate limit exceeded)");
            return None;
        }

        let order_id = self.next_order_id.fetch_add(1, Ordering::Relaxed);
        let lob = self.current_lob.read();
        let sim_order = SimulatedOrder::from_prepared(order_id, order, is_maker, expected_profit_bps, lob.as_ref());
        
        info!(
            order_id,
            side = ?order.side,
            price = %order.price,
            volume_ahead = %sim_order.volume_ahead,
            "📝 Simulator: Tracking prepared order"
        );
        
        self.order_book.add_order(sim_order);
        Some(order_id)
    }

    /// Update the estimated daily volume for self-impact calculation
    pub fn update_daily_volume(&self, volume_usd: Decimal) {
        *self.daily_volume_usd.write() = volume_usd;
    }

    /// Update mid price for mark-to-market calculation
    pub fn update_mid_price(&self, market_id: &MarketId, mid_price: Decimal) {
        let market_hash = SimulatedOrderBook::hash_market_id_pub(market_id);
        self.position_tracker.update_mid_price(market_hash, mid_price);
    }

    /// Cancel a simulated order with latency simulation (50ms buffer).
    pub fn cancel_order(&self, order_id: u64) -> Result<Option<SimulatedOrder>, &'static str> {
        self.order_book.cancel_order_with_latency(order_id)
    }

    /// Calculate self-impact slippage in basis points.
    fn calculate_slippage_bps(&self, fill_size: Decimal, fill_price: Decimal) -> Decimal {
        let notional = fill_size * fill_price;
        let daily_vol = *self.daily_volume_usd.read();
        
        if daily_vol <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        let pct_of_daily = (notional / daily_vol) * Decimal::new(100, 0);
        if pct_of_daily <= SELF_IMPACT_THRESHOLD_PCT {
            return Decimal::ZERO;
        }

        let thousands = notional / Decimal::new(1000, 0);
        thousands * SLIPPAGE_BPS_PER_1000_USD
    }

    /// Process a trade update - check for fills using queue position model
    pub fn process_trade(
        &self,
        market_id: &MarketId,
        trade_side: Side,
        trade_price: Decimal,
        trade_size: Decimal,
        market_probability: Decimal,
    ) -> Vec<SimulatedFill> {
        if !self.enabled.load(Ordering::Relaxed) {
            return Vec::new();
        }

        let mut fills = Vec::new();
        let matching_side = trade_side.opposite();

        // Update cumulative volume for orders at matching prices
        self.order_book.add_trade_volume(market_id, matching_side, trade_price, trade_size);

        // Get orders that could be filled
        let candidates = self.order_book.get_fillable_orders(market_id, trade_side, trade_price);
        
        let mut remaining_size = trade_size;

        for order in candidates {
            if remaining_size <= Decimal::ZERO {
                break;
            }

            // Determine fill reason
            let trade_crosses = match order.side {
                Side::Sell => trade_price > order.price,
                Side::Buy => trade_price < order.price,
            };

            let queue_exhausted = order.cumulative_volume > order.volume_ahead;
            
            if !order.should_fill(trade_price, trade_crosses) {
                continue;
            }

            let fill_reason = if trade_crosses {
                FillReason::PriceCross
            } else {
                FillReason::QueueExhausted
            };

            let fill_size = remaining_size.min(order.remaining());
            remaining_size -= fill_size;

            // Calculate fee/rebate
            let fee_bps = self.calculate_fee_bps(order.is_maker, market_probability);
            
            // Reality Check 3: Self-impact slippage
            let slippage_bps = self.calculate_slippage_bps(fill_size, order.price);
            let effective_price = if slippage_bps > Decimal::ZERO {
                let slip_factor = slippage_bps / Decimal::new(10000, 0);
                match order.side {
                    Side::Sell => order.price * (Decimal::ONE - slip_factor),
                    Side::Buy => order.price * (Decimal::ONE + slip_factor),
                }
            } else {
                order.price
            };

            if slippage_bps > Decimal::ZERO {
                let slip_micro = Self::to_micro(fill_size * order.price * slippage_bps / Decimal::new(10000, 0));
                self.total_slippage_micro.fetch_add(slip_micro, Ordering::Relaxed);
            }

            // Calculate PnL using effective price (after slippage)
            let gross_value = fill_size * effective_price;
            let fee_amount = gross_value * Decimal::new(fee_bps as i64, 4);
            let net_pnl = match order.side {
                Side::Sell => gross_value - fee_amount,
                Side::Buy => -gross_value - fee_amount,
            };

            // Update order
            if let Some(_) = self.order_book.fill_order(order.id, fill_size) {
                let fill = SimulatedFill {
                    order_id: order.id,
                    market_id: *market_id,
                    side: order.side,
                    price: effective_price,
                    size: fill_size,
                    fee_bps,
                    net_pnl,
                    is_maker: order.is_maker,
                    timestamp_ns: SimulatedOrder::now_ns(),
                    fill_reason,
                };

                self.log_fill(&fill, &order, market_probability, slippage_bps);

                // Reality Check 2: Track position for M2M
                let market_hash = SimulatedOrderBook::hash_market_id_pub(market_id);
                self.position_tracker.record_fill(market_hash, order.side, fill_size, effective_price);
                self.position_tracker.update_mid_price(market_hash, trade_price);

                self.record_fill(&fill);
                fills.push(fill);
            }
        }

        fills
    }

    fn calculate_fee_bps(&self, is_maker: bool, probability: Decimal) -> i32 {
        if is_maker {
            self.config.trading.fee_curve_2026.maker_rebate_bps
        } else {
            self.config.trading.fee_curve_2026.get_taker_fee_bps(probability) as i32
        }
    }

    fn log_fill(&self, fill: &SimulatedFill, order: &SimulatedOrder, probability: Decimal, slippage_bps: Decimal) {
        let fee_type = if fill.is_maker { "REBATE" } else { "FEE" };
        let fee_str = if fill.fee_bps < 0 {
            format!("+{} bps", -fill.fee_bps)
        } else {
            format!("-{} bps", fill.fee_bps)
        };
        let reason = match fill.fill_reason {
            FillReason::PriceCross => "PRICE_CROSS",
            FillReason::QueueExhausted => "QUEUE_EXHAUSTED",
        };

        let realized = self.get_pnl();
        let unrealized = self.position_tracker.total_unrealized_pnl();
        let net_equity = self.get_balance() + unrealized;

        info!(
            order_id = fill.order_id,
            side = ?fill.side,
            price = %fill.price,
            size = %fill.size,
            fee_type,
            fee = fee_str,
            net_pnl = %fill.net_pnl,
            probability = %probability,
            volume_ahead = %order.volume_ahead,
            cumulative_vol = %order.cumulative_volume,
            fill_reason = reason,
            slippage_bps = %slippage_bps,
            "💰 Realized: ${} | 🔻 Unrealized: ${} | 🏁 Net Equity: ${} | FILL {} @ {} | {} {} | Reason: {} | Slip: {} bps",
            realized, unrealized, net_equity,
            fill.size, fill.price, fee_type, fee_str, reason, slippage_bps
        );
    }

    fn record_fill(&self, fill: &SimulatedFill) {
        let pnl_micro = Self::to_micro(fill.net_pnl);
        self.virtual_balance_micro.fetch_add(pnl_micro, Ordering::Relaxed);
        self.realized_pnl_micro.fetch_add(pnl_micro, Ordering::Relaxed);

        self.total_trades.fetch_add(1, Ordering::Relaxed);
        
        if fill.net_pnl > Decimal::ZERO {
            self.winning_trades.fetch_add(1, Ordering::Relaxed);
        } else if fill.net_pnl < Decimal::ZERO {
            self.losing_trades.fetch_add(1, Ordering::Relaxed);
        }

        if fill.is_maker {
            self.maker_fills.fetch_add(1, Ordering::Relaxed);
            if fill.fee_bps < 0 {
                self.total_rebates_micro.fetch_add(
                    Self::to_micro(fill.size * fill.price * Decimal::new(-fill.fee_bps as i64, 4)),
                    Ordering::Relaxed,
                );
            }
        } else {
            self.taker_fills.fetch_add(1, Ordering::Relaxed);
            self.total_fees_micro.fetch_add(
                Self::to_micro(fill.size * fill.price * Decimal::new(fill.fee_bps as i64, 4)),
                Ordering::Relaxed,
            );
        }

        self.fill_history.write().push(fill.clone());
    }

    // Public getters
    pub fn get_balance(&self) -> Decimal {
        Self::from_micro(self.virtual_balance_micro.load(Ordering::Relaxed))
    }

    pub fn get_pnl(&self) -> Decimal {
        Self::from_micro(self.realized_pnl_micro.load(Ordering::Relaxed))
    }

    pub fn active_order_count(&self) -> usize {
        self.order_book.active_order_count()
    }

    pub fn get_stats(&self) -> SimulatorStats {
        let current_balance = self.get_balance();
        let realized_pnl = self.get_pnl();
        let unrealized_pnl = self.position_tracker.total_unrealized_pnl();
        let net_equity = current_balance + unrealized_pnl;

        SimulatorStats {
            initial_balance: Self::from_micro(self.initial_balance_micro),
            current_balance,
            realized_pnl,
            unrealized_pnl,
            net_equity,
            total_trades: self.total_trades.load(Ordering::Relaxed),
            winning_trades: self.winning_trades.load(Ordering::Relaxed),
            losing_trades: self.losing_trades.load(Ordering::Relaxed),
            total_fees_paid: Self::from_micro(self.total_fees_micro.load(Ordering::Relaxed)),
            total_rebates_earned: Self::from_micro(self.total_rebates_micro.load(Ordering::Relaxed)),
            maker_fills: self.maker_fills.load(Ordering::Relaxed),
            taker_fills: self.taker_fills.load(Ordering::Relaxed),
            orders_throttled: self.orders_throttled.load(Ordering::Relaxed),
            total_slippage: Self::from_micro(self.total_slippage_micro.load(Ordering::Relaxed)),
        }
    }

    pub fn get_fill_history(&self) -> Vec<SimulatedFill> {
        self.fill_history.read().clone()
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.virtual_balance_micro.store(self.initial_balance_micro, Ordering::SeqCst);
        self.realized_pnl_micro.store(0, Ordering::SeqCst);
        self.total_trades.store(0, Ordering::SeqCst);
        self.winning_trades.store(0, Ordering::SeqCst);
        self.losing_trades.store(0, Ordering::SeqCst);
        self.maker_fills.store(0, Ordering::SeqCst);
        self.taker_fills.store(0, Ordering::SeqCst);
        self.total_fees_micro.store(0, Ordering::SeqCst);
        self.total_rebates_micro.store(0, Ordering::SeqCst);
        self.orders_throttled.store(0, Ordering::SeqCst);
        self.total_slippage_micro.store(0, Ordering::SeqCst);
        self.position_tracker.reset();
        self.fill_history.write().clear();
        info!("🔄 Simulator: Reset to initial state");
    }

    fn to_micro(d: Decimal) -> i64 {
        let scaled = d * Decimal::new(1_000_000, 0);
        scaled.trunc().to_i64().unwrap_or(0)
    }

    fn from_micro(micro: i64) -> Decimal {
        Decimal::new(micro, 6)
    }
}

/// Implement Simulated trait for FillSimulator
impl Simulated for FillSimulator {
    fn on_order_placed(&self, _order_id: u64, signal: &TradeSignal, lob: Option<&OrderBookSnapshot>) {
        if let Some(snapshot) = lob {
            self.update_lob(snapshot.clone());
        }
        let _ = self.track_order(signal, true);
    }

    fn on_trade(&self, market_id: &MarketId, side: Side, price: Decimal, size: Decimal, prob: Decimal) -> Vec<SimulatedFill> {
        self.process_trade(market_id, side, price, size, prob)
    }

    fn is_simulating(&self) -> bool {
        self.is_enabled()
    }
}

// =============================================================================
// STATISTICS
// =============================================================================

#[derive(Debug, Clone)]
pub struct SimulatorStats {
    pub initial_balance: Decimal,
    pub current_balance: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub net_equity: Decimal,
    pub total_trades: u64,
    pub winning_trades: u64,
    pub losing_trades: u64,
    pub total_fees_paid: Decimal,
    pub total_rebates_earned: Decimal,
    pub maker_fills: u64,
    pub taker_fills: u64,
    pub orders_throttled: u64,
    pub total_slippage: Decimal,
}

impl SimulatorStats {
    pub fn win_rate(&self) -> f64 {
        if self.total_trades > 0 {
            self.winning_trades as f64 / self.total_trades as f64
        } else {
            0.0
        }
    }

    pub fn maker_ratio(&self) -> f64 {
        let total = self.maker_fills + self.taker_fills;
        if total > 0 {
            self.maker_fills as f64 / total as f64
        } else {
            0.0
        }
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SignalUrgency;

    fn mock_config() -> Arc<Config> {
        Arc::new(Config::load_with_defaults())
    }

    fn mock_market_id() -> MarketId {
        MarketId::new([0xAB; 32], 12345)
    }

    fn mock_signal(side: Side, price: Decimal, size: Decimal) -> TradeSignal {
        TradeSignal {
            market_id: mock_market_id(),
            side,
            price,
            size,
            order_type: OrderType::Limit,
            expected_profit_bps: 50,
            signal_timestamp_ns: 0,
            urgency: SignalUrgency::Low,
        }
    }

    fn mock_lob(bid_price: Decimal, bid_size: Decimal, ask_price: Decimal, ask_size: Decimal) -> OrderBookSnapshot {
        use crate::types::PriceLevel;
        let mut book = OrderBookSnapshot::new();
        book.bids[0] = PriceLevel::new(bid_price, bid_size, 1);
        book.asks[0] = PriceLevel::new(ask_price, ask_size, 1);
        book.bid_count = 1;
        book.ask_count = 1;
        book
    }

    #[test]
    fn test_simulator_creation() {
        let config = mock_config();
        let sim = FillSimulator::new(config);
        
        assert_eq!(sim.get_balance(), Decimal::new(10000, 0));
        assert_eq!(sim.get_pnl(), Decimal::ZERO);
    }

    #[test]
    fn test_order_volume_ahead_calculation() {
        let lob = mock_lob(
            Decimal::new(49, 2), Decimal::new(100, 0),
            Decimal::new(51, 2), Decimal::new(200, 0),
        );
        
        // Buy order at 0.49 should see 100 volume ahead
        let buy_vol = SimulatedOrder::calculate_volume_ahead(
            Side::Buy, Decimal::new(49, 2), Some(&lob)
        );
        assert_eq!(buy_vol, Decimal::new(100, 0));
        
        // Sell order at 0.51 should see 200 volume ahead
        let sell_vol = SimulatedOrder::calculate_volume_ahead(
            Side::Sell, Decimal::new(51, 2), Some(&lob)
        );
        assert_eq!(sell_vol, Decimal::new(200, 0));
    }

    #[test]
    fn test_minimum_order_life() {
        let config = mock_config();
        let sim = FillSimulator::new(config);
        
        let signal = mock_signal(Side::Sell, Decimal::new(50, 2), Decimal::new(10, 0));
        let _ = sim.track_order(&signal, true);
        
        // Immediate trade should NOT fill due to 50ms minimum life
        let fills = sim.process_trade(
            &mock_market_id(),
            Side::Buy,
            Decimal::new(50, 2),
            Decimal::new(10, 0),
            Decimal::new(50, 2),
        );
        
        // Should be empty because order isn't eligible yet
        assert!(fills.is_empty());
    }

    #[test]
    fn test_live_mode_no_op() {
        let live = LiveMode;
        
        assert!(!live.is_simulating());
        
        let signal = mock_signal(Side::Buy, Decimal::new(50, 2), Decimal::new(10, 0));
        live.on_order_placed(1, &signal, None);
        
        let fills = live.on_trade(
            &mock_market_id(),
            Side::Sell,
            Decimal::new(50, 2),
            Decimal::new(10, 0),
            Decimal::new(50, 2),
        );
        
        assert!(fills.is_empty());
    }

    #[test]
    fn test_cumulative_volume_tracking() {
        let book = SimulatedOrderBook::new();
        let market_id = mock_market_id();
        
        let signal = mock_signal(Side::Buy, Decimal::new(49, 2), Decimal::new(10, 0));
        let order = SimulatedOrder::new(1, &signal, true, None);
        book.add_order(order);
        
        // Add trade volume
        book.add_trade_volume(&market_id, Side::Buy, Decimal::new(49, 2), Decimal::new(50, 0));
        
        // Check cumulative volume was updated
        let orders = book.active_orders();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].cumulative_volume, Decimal::new(50, 0));
    }
}
