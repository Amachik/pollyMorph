//! Shadow Mode (Paper Trading) Engine
//!
//! Simulates order execution without broadcasting to the blockchain.
//! Uses the same GridEngine logic as live trading for 100% accurate simulation.
//!
//! Features:
//! - Virtual balance tracking (default 10,000 USDC)
//! - Virtual order book with thread-safe access
//! - Trade feed integration for fill detection
//! - 2026 fee curve simulation (maker rebates, taker fees)
//! - Full PnL and metrics tracking

use crate::config::Config;
use crate::grid::GridEngine;
use crate::types::{MarketId, Side, OrderType, TradeSignal, PreparedOrder};
use crate::types::OrderBookSnapshot;
use dashmap::DashMap;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::sync::atomic::{AtomicU64, AtomicI64, AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};

// =============================================================================
// CONSTANTS
// =============================================================================

/// Minimum order life before eligible for fills (50ms in nanoseconds)
const MIN_ORDER_LIFE_NS: u64 = 50_000_000;

/// Slippage: 1 basis point per $1,000 of order size
const SLIPPAGE_BPS_PER_1000_USD: Decimal = Decimal::ONE;

/// Self-impact threshold: order size > 1% of daily volume triggers slippage
const SELF_IMPACT_THRESHOLD_PCT: Decimal = Decimal::ONE; // 1%

// =============================================================================
// TOKEN BUCKET RATE LIMITER
// =============================================================================

/// Token bucket rate limiter for API throttling simulation.
/// Polymarket allows max 60 orders/sec.
pub struct TokenBucket {
    /// Maximum tokens (burst capacity)
    max_tokens: u64,
    /// Tokens refilled per nanosecond (rate / 1e9)
    refill_rate_per_ns: f64,
    /// Current token count (scaled by 1000 for sub-token precision)
    tokens_milli: AtomicI64,
    /// Last refill timestamp in nanoseconds
    last_refill_ns: AtomicU64,
}

impl TokenBucket {
    /// Create a new token bucket.
    /// `rate_per_sec`: sustained rate (e.g. 60 orders/sec)
    /// `burst`: max burst capacity
    pub fn new(rate_per_sec: u64, burst: u64) -> Self {
        Self {
            max_tokens: burst,
            refill_rate_per_ns: rate_per_sec as f64 / 1_000_000_000.0,
            tokens_milli: AtomicI64::new((burst * 1000) as i64),
            last_refill_ns: AtomicU64::new(Self::now_ns()),
        }
    }

    /// Try to consume one token. Returns true if allowed, false if rate-limited.
    pub fn try_acquire(&self) -> bool {
        self.refill();
        let prev = self.tokens_milli.fetch_sub(1000, Ordering::AcqRel);
        if prev >= 1000 {
            true
        } else {
            // Put it back - we didn't actually have a token
            self.tokens_milli.fetch_add(1000, Ordering::Release);
            false
        }
    }

    fn refill(&self) {
        let now = Self::now_ns();
        let last = self.last_refill_ns.load(Ordering::Acquire);
        let elapsed_ns = now.saturating_sub(last);
        if elapsed_ns == 0 {
            return;
        }

        let new_tokens_milli = (elapsed_ns as f64 * self.refill_rate_per_ns * 1000.0) as i64;
        if new_tokens_milli <= 0 {
            return;
        }

        // CAS loop to update last_refill_ns
        if self.last_refill_ns.compare_exchange(
            last, now, Ordering::AcqRel, Ordering::Relaxed
        ).is_ok() {
            let max_milli = (self.max_tokens * 1000) as i64;
            let old = self.tokens_milli.fetch_add(new_tokens_milli, Ordering::AcqRel);
            // Clamp to max
            let new_val = old + new_tokens_milli;
            if new_val > max_milli {
                self.tokens_milli.store(max_milli, Ordering::Release);
            }
        }
    }

    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}

// =============================================================================
// POSITION TRACKER (Mark-to-Market)
// =============================================================================

/// Tracks net position per market for unrealized PnL calculation.
/// Position = sum of (signed_qty) at weighted average entry price.
pub struct PositionTracker {
    /// (market_hash) -> PositionEntry
    positions: DashMap<u64, PositionEntry>,
    /// Latest mid price per market for M2M
    mid_prices: DashMap<u64, Decimal>,
}

#[derive(Debug, Clone)]
pub struct PositionEntry {
    /// Net position size (positive = long, negative = short)
    pub net_qty: Decimal,
    /// Volume-weighted average entry price
    pub avg_entry_price: Decimal,
    /// Total cost basis (for VWAP calculation)
    pub total_cost: Decimal,
    /// Total absolute qty entered (for VWAP denominator)
    pub total_abs_qty: Decimal,
}

impl PositionEntry {
    pub fn new() -> Self {
        Self {
            net_qty: Decimal::ZERO,
            avg_entry_price: Decimal::ZERO,
            total_cost: Decimal::ZERO,
            total_abs_qty: Decimal::ZERO,
        }
    }

    /// Record a fill into the position
    pub fn record_fill(&mut self, side: Side, qty: Decimal, price: Decimal) {
        let signed_qty = match side {
            Side::Buy => qty,
            Side::Sell => -qty,
        };
        self.net_qty += signed_qty;
        self.total_cost += qty * price;
        self.total_abs_qty += qty;
        if self.total_abs_qty > Decimal::ZERO {
            self.avg_entry_price = self.total_cost / self.total_abs_qty;
        }
    }

    /// Unrealized PnL given current mid price
    pub fn unrealized_pnl(&self, mid_price: Decimal) -> Decimal {
        if self.net_qty == Decimal::ZERO || self.avg_entry_price == Decimal::ZERO {
            return Decimal::ZERO;
        }
        (mid_price - self.avg_entry_price) * self.net_qty
    }
}

impl PositionTracker {
    pub fn new() -> Self {
        Self {
            positions: DashMap::new(),
            mid_prices: DashMap::new(),
        }
    }

    /// Record a fill
    pub fn record_fill(&self, market_hash: u64, side: Side, qty: Decimal, price: Decimal) {
        self.positions
            .entry(market_hash)
            .or_insert_with(PositionEntry::new)
            .record_fill(side, qty, price);
    }

    /// Update the latest mid price for a market
    pub fn update_mid_price(&self, market_hash: u64, mid_price: Decimal) {
        self.mid_prices.insert(market_hash, mid_price);
    }

    /// Calculate total unrealized PnL across all positions
    pub fn total_unrealized_pnl(&self) -> Decimal {
        let mut total = Decimal::ZERO;
        for entry in self.positions.iter() {
            let market_hash = *entry.key();
            if let Some(mid) = self.mid_prices.get(&market_hash) {
                total += entry.value().unrealized_pnl(*mid);
            }
        }
        total
    }

    /// Reset all positions
    pub fn reset(&self) {
        self.positions.clear();
        self.mid_prices.clear();
    }
}

/// Virtual order state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum VirtualOrderState {
    Pending = 0,
    Open = 1,
    PartiallyFilled = 2,
    Filled = 3,
    Cancelled = 4,
}

/// A virtual order in the shadow order book with queue position tracking
#[derive(Debug, Clone)]
pub struct VirtualOrder {
    pub id: u64,
    pub market_id: MarketId,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub filled: Decimal,
    pub order_type: OrderType,
    pub state: VirtualOrderState,
    pub created_at_ns: u64,
    pub filled_at_ns: Option<u64>,
    /// Is this a maker order (post_only)?
    pub is_maker: bool,
    /// Expected profit in BPS at creation
    pub expected_profit_bps: i32,
    
    // Queue position tracking for realistic fill simulation
    /// Volume ahead in queue when order was placed (from LOB snapshot)
    pub volume_ahead: Decimal,
    /// Cumulative trade volume at this price since order placement
    pub cumulative_volume: Decimal,
    /// Earliest time order can be filled (created_at + MIN_ORDER_LIFE_NS)
    pub eligible_at_ns: u64,
}

impl VirtualOrder {
    pub fn new(
        id: u64,
        market_id: MarketId,
        side: Side,
        price: Decimal,
        size: Decimal,
        order_type: OrderType,
        is_maker: bool,
        expected_profit_bps: i32,
    ) -> Self {
        let now = Self::now_ns();
        Self {
            id,
            market_id,
            side,
            price,
            size,
            filled: Decimal::ZERO,
            order_type,
            state: VirtualOrderState::Open,
            created_at_ns: now,
            filled_at_ns: None,
            is_maker,
            expected_profit_bps,
            volume_ahead: Decimal::ZERO,
            cumulative_volume: Decimal::ZERO,
            eligible_at_ns: now + MIN_ORDER_LIFE_NS,
        }
    }

    /// Create with LOB snapshot for queue position calculation
    pub fn new_with_lob(
        id: u64,
        market_id: MarketId,
        side: Side,
        price: Decimal,
        size: Decimal,
        order_type: OrderType,
        is_maker: bool,
        expected_profit_bps: i32,
        lob: Option<&OrderBookSnapshot>,
    ) -> Self {
        let now = Self::now_ns();
        let volume_ahead = Self::calculate_volume_ahead(side, price, lob);
        Self {
            id,
            market_id,
            side,
            price,
            size,
            filled: Decimal::ZERO,
            order_type,
            state: VirtualOrderState::Open,
            created_at_ns: now,
            filled_at_ns: None,
            is_maker,
            expected_profit_bps,
            volume_ahead,
            cumulative_volume: Decimal::ZERO,
            eligible_at_ns: now + MIN_ORDER_LIFE_NS,
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

    pub fn from_signal(id: u64, signal: &TradeSignal, is_maker: bool) -> Self {
        Self::new(
            id,
            signal.market_id,
            signal.side,
            signal.price,
            signal.size,
            signal.order_type,
            is_maker,
            signal.expected_profit_bps,
        )
    }

    pub fn from_prepared(id: u64, order: &PreparedOrder, is_maker: bool, expected_profit_bps: i32) -> Self {
        Self::new(
            id,
            order.market_id,
            order.side,
            order.price,
            order.size,
            order.order_type,
            is_maker,
            expected_profit_bps,
        )
    }

    #[inline(always)]
    pub fn remaining(&self) -> Decimal {
        self.size - self.filled
    }

    #[inline(always)]
    pub fn is_active(&self) -> bool {
        matches!(self.state, VirtualOrderState::Open | VirtualOrderState::PartiallyFilled)
    }

    /// Check if order has met minimum life requirement (50ms latency buffer)
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

    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}

/// Virtual fill event
#[derive(Debug, Clone)]
pub struct VirtualFill {
    pub order_id: u64,
    pub market_id: MarketId,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub fee_bps: i32,        // Negative = rebate
    pub net_pnl: Decimal,    // After fees
    pub is_maker: bool,
    pub timestamp_ns: u64,
}

/// Thread-safe virtual order book
pub struct VirtualOrderBook {
    /// Active orders by ID
    orders: DashMap<u64, VirtualOrder>,
    /// Orders indexed by (market_id_hash, side, price_cents) for fast lookup
    price_index: DashMap<(u64, u8, i64), Vec<u64>>,
}

impl VirtualOrderBook {
    pub fn new() -> Self {
        Self {
            orders: DashMap::new(),
            price_index: DashMap::new(),
        }
    }

    /// Add an order to the book
    pub fn add_order(&self, order: VirtualOrder) {
        let order_id = order.id;
        let index_key = Self::make_index_key(&order);
        
        // Add to main store
        self.orders.insert(order_id, order);
        
        // Add to price index
        self.price_index
            .entry(index_key)
            .or_insert_with(Vec::new)
            .push(order_id);
    }

    /// Get orders at a specific price level
    pub fn get_orders_at_price(
        &self,
        market_id: &MarketId,
        side: Side,
        price: Decimal,
    ) -> Vec<VirtualOrder> {
        let index_key = (
            Self::hash_market_id(market_id),
            side as u8,
            Self::price_to_cents(price),
        );

        if let Some(order_ids) = self.price_index.get(&index_key) {
            order_ids
                .iter()
                .filter_map(|id| self.orders.get(id).map(|o| o.clone()))
                .filter(|o| o.is_active())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Update an order's fill status
    pub fn fill_order(&self, order_id: u64, fill_size: Decimal) -> Option<VirtualOrder> {
        self.orders.get_mut(&order_id).map(|mut order| {
            order.filled += fill_size;
            if order.filled >= order.size {
                order.state = VirtualOrderState::Filled;
                order.filled_at_ns = Some(VirtualOrder::now_ns());
            } else {
                order.state = VirtualOrderState::PartiallyFilled;
            }
            order.clone()
        })
    }

    /// Cancel an order (immediate - used internally for stale cleanup)
    pub fn cancel_order(&self, order_id: u64) -> Option<VirtualOrder> {
        self.orders.get_mut(&order_id).map(|mut order| {
            order.state = VirtualOrderState::Cancelled;
            order.clone()
        })
    }

    /// Cancel an order with latency simulation.
    /// Returns None if the order hasn't met the 50ms minimum life yet
    /// (simulating that the cancel request is still in-flight).
    pub fn cancel_order_with_latency(&self, order_id: u64) -> Result<Option<VirtualOrder>, &'static str> {
        if let Some(order) = self.orders.get(&order_id) {
            if !order.is_active() {
                return Ok(None);
            }
            let now = VirtualOrder::now_ns();
            if now < order.eligible_at_ns {
                return Err("cancel rejected: order still in latency window");
            }
        } else {
            return Ok(None);
        }
        Ok(self.cancel_order(order_id))
    }

    /// Get all active orders
    pub fn active_orders(&self) -> Vec<VirtualOrder> {
        self.orders
            .iter()
            .filter(|o| o.is_active())
            .map(|o| o.clone())
            .collect()
    }

    /// Get order count
    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    /// Get active order count
    pub fn active_order_count(&self) -> usize {
        self.orders.iter().filter(|o| o.is_active()).count()
    }

    /// Cancel stale orders older than max_age_ns
    pub fn cancel_stale_orders(&self, max_age_ns: u64) -> usize {
        let now = VirtualOrder::now_ns();
        let mut cancelled = 0;
        
        for mut entry in self.orders.iter_mut() {
            if entry.is_active() && (now - entry.created_at_ns) > max_age_ns {
                entry.state = VirtualOrderState::Cancelled;
                cancelled += 1;
            }
        }
        cancelled
    }

    /// Cancel oldest orders to keep active count under limit
    pub fn enforce_order_limit(&self, max_active: usize) -> usize {
        let active_count = self.active_order_count();
        if active_count <= max_active {
            return 0;
        }
        
        // Get all active orders sorted by creation time
        let mut active: Vec<_> = self.orders
            .iter()
            .filter(|o| o.is_active())
            .map(|o| (o.id, o.created_at_ns))
            .collect();
        
        active.sort_by_key(|(_, ts)| *ts);
        
        // Cancel oldest orders
        let to_cancel = active_count - max_active;
        let mut cancelled = 0;
        
        for (order_id, _) in active.iter().take(to_cancel) {
            if let Some(mut order) = self.orders.get_mut(order_id) {
                order.state = VirtualOrderState::Cancelled;
                cancelled += 1;
            }
        }
        cancelled
    }

    /// Update cumulative volume for all active orders at a price level
    pub fn add_trade_volume(
        &self,
        market_id: &MarketId,
        side: Side,
        price: Decimal,
        volume: Decimal,
    ) {
        let index_key = (
            Self::hash_market_id(market_id),
            side as u8,
            Self::price_to_cents(price),
        );

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
    ) -> Vec<VirtualOrder> {
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

    fn make_index_key(order: &VirtualOrder) -> (u64, u8, i64) {
        (
            Self::hash_market_id(&order.market_id),
            order.side as u8,
            Self::price_to_cents(order.price),
        )
    }

    fn hash_market_id(market_id: &MarketId) -> u64 {
        // Simple hash of condition_id + token_id
        let mut hash = market_id.token_id;
        for (i, byte) in market_id.condition_id.iter().enumerate() {
            hash ^= (*byte as u64) << ((i % 8) * 8);
        }
        hash
    }

    /// Public accessor for market hash (used by PositionTracker)
    pub fn hash_market_id_pub(market_id: &MarketId) -> u64 {
        Self::hash_market_id(market_id)
    }

    fn price_to_cents(price: Decimal) -> i64 {
        // Convert to cents (2 decimal places) for indexing
        (price * Decimal::new(100, 0))
            .to_string()
            .parse::<i64>()
            .unwrap_or(0)
    }
}

impl Default for VirtualOrderBook {
    fn default() -> Self {
        Self::new()
    }
}

/// Shadow trading statistics
#[derive(Debug, Clone)]
pub struct ShadowStats {
    pub initial_balance: Decimal,
    pub current_balance: Decimal,
    pub total_pnl: Decimal,
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

/// Shadow Mode Engine - Paper Trading Simulator
pub struct ShadowEngine {
    config: Arc<Config>,
    
    /// Virtual USDC balance (atomic for lock-free reads)
    /// Stored as micro-cents (balance * 1_000_000) for precision
    virtual_balance_micro: AtomicI64,
    initial_balance_micro: i64,
    
    /// Virtual order book
    order_book: VirtualOrderBook,
    
    /// Order ID generator
    next_order_id: AtomicU64,
    
    /// PnL tracking (stored as micro-cents)
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
    
    /// Grid engine reference for accurate simulation
    grid_engine: Option<Arc<GridEngine>>,
    
    /// Fill history for analysis
    fill_history: RwLock<Vec<VirtualFill>>,
    
    /// Engine enabled
    enabled: std::sync::atomic::AtomicBool,

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

impl ShadowEngine {
    /// Create a new shadow engine with default 10,000 USDC balance
    pub fn new(config: Arc<Config>) -> Self {
        Self::with_balance(config, Decimal::new(10000, 0))
    }

    /// Create with custom initial balance
    pub fn with_balance(config: Arc<Config>, initial_balance: Decimal) -> Self {
        let balance_micro = Self::to_micro(initial_balance);
        
        Self {
            config,
            virtual_balance_micro: AtomicI64::new(balance_micro),
            initial_balance_micro: balance_micro,
            order_book: VirtualOrderBook::new(),
            next_order_id: AtomicU64::new(1),
            realized_pnl_micro: AtomicI64::new(0),
            total_trades: AtomicU64::new(0),
            winning_trades: AtomicU64::new(0),
            losing_trades: AtomicU64::new(0),
            maker_fills: AtomicU64::new(0),
            taker_fills: AtomicU64::new(0),
            total_fees_micro: AtomicI64::new(0),
            total_rebates_micro: AtomicI64::new(0),
            grid_engine: None,
            fill_history: RwLock::new(Vec::new()),
            enabled: std::sync::atomic::AtomicBool::new(true),
            // Reality checks
            rate_limiter: TokenBucket::new(60, 60), // 60 orders/sec, burst 60
            orders_throttled: AtomicU64::new(0),
            position_tracker: PositionTracker::new(),
            daily_volume_usd: RwLock::new(Decimal::new(500_000, 0)), // default 500k daily vol
            total_slippage_micro: AtomicI64::new(0),
        }
    }

    /// Set grid engine for accurate simulation
    pub fn set_grid_engine(&mut self, grid_engine: Arc<GridEngine>) {
        self.grid_engine = Some(grid_engine);
    }

    /// Track a virtual order (instead of broadcasting to blockchain).
    /// Returns None if rate-limited (simulates API 429 rejection).
    pub fn track_virtual_order(&self, signal: &TradeSignal, is_maker: bool) -> Option<u64> {
        // Reality Check 1: API rate limiting (60 orders/sec)
        if !self.rate_limiter.try_acquire() {
            let throttled = self.orders_throttled.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(
                throttled_total = throttled,
                side = ?signal.side,
                price = %signal.price,
                "🚫 Shadow: Order THROTTLED (rate limit 60/sec exceeded)"
            );
            return None;
        }

        // Order management: cancel stale orders (older than 30 seconds)
        const MAX_ORDER_AGE_NS: u64 = 30_000_000_000; // 30 seconds
        const MAX_ACTIVE_ORDERS: usize = 50;
        
        let stale_cancelled = self.order_book.cancel_stale_orders(MAX_ORDER_AGE_NS);
        let limit_cancelled = self.order_book.enforce_order_limit(MAX_ACTIVE_ORDERS);
        
        if stale_cancelled > 0 || limit_cancelled > 0 {
            debug!(
                stale = stale_cancelled,
                limit = limit_cancelled,
                "🗑️ Shadow: Cancelled old orders"
            );
        }
        
        let order_id = self.next_order_id.fetch_add(1, Ordering::Relaxed);
        let order = VirtualOrder::from_signal(order_id, signal, is_maker);
        
        info!(
            order_id = order_id,
            side = ?signal.side,
            price = %signal.price,
            size = %signal.size,
            is_maker = is_maker,
            "📝 Shadow: Tracking virtual order"
        );
        
        self.order_book.add_order(order);
        Some(order_id)
    }

    /// Track a prepared order. Returns None if rate-limited.
    pub fn track_prepared_order(&self, order: &PreparedOrder, is_maker: bool, expected_profit_bps: i32) -> Option<u64> {
        // Reality Check 1: API rate limiting
        if !self.rate_limiter.try_acquire() {
            self.orders_throttled.fetch_add(1, Ordering::Relaxed);
            warn!("🚫 Shadow: Prepared order THROTTLED (rate limit exceeded)");
            return None;
        }

        let order_id = self.next_order_id.fetch_add(1, Ordering::Relaxed);
        let virtual_order = VirtualOrder::from_prepared(order_id, order, is_maker, expected_profit_bps);
        
        info!(
            order_id = order_id,
            side = ?order.side,
            price = %order.price,
            size = %order.size,
            is_maker = is_maker,
            "📝 Shadow: Tracking virtual order"
        );
        
        self.order_book.add_order(virtual_order);
        Some(order_id)
    }

    /// Track multiple orders from a grid. Skips rate-limited orders.
    pub fn track_grid_orders(&self, signals: &[TradeSignal], is_maker: bool) -> Vec<u64> {
        signals
            .iter()
            .filter_map(|s| self.track_virtual_order(s, is_maker))
            .collect()
    }

    /// Update the estimated daily volume for self-impact calculation
    pub fn update_daily_volume(&self, volume_usd: Decimal) {
        *self.daily_volume_usd.write() = volume_usd;
    }

    /// Update mid price for mark-to-market calculation
    pub fn update_mid_price(&self, market_id: &MarketId, mid_price: Decimal) {
        let market_hash = VirtualOrderBook::hash_market_id_pub(market_id);
        self.position_tracker.update_mid_price(market_hash, mid_price);
    }

    /// Cancel a virtual order with latency simulation (50ms buffer).
    /// Returns Err if the order is still in its latency window.
    pub fn cancel_virtual_order(&self, order_id: u64) -> Result<Option<VirtualOrder>, &'static str> {
        self.order_book.cancel_order_with_latency(order_id)
    }

    /// Calculate self-impact slippage in basis points.
    /// 1 bps per $1,000 of order size, only if size > 1% of daily volume.
    fn calculate_slippage_bps(&self, fill_size: Decimal, fill_price: Decimal) -> Decimal {
        let notional = fill_size * fill_price;
        let daily_vol = *self.daily_volume_usd.read();
        
        if daily_vol <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        // Check if order exceeds 1% of daily volume
        let pct_of_daily = (notional / daily_vol) * Decimal::new(100, 0);
        if pct_of_daily <= SELF_IMPACT_THRESHOLD_PCT {
            return Decimal::ZERO;
        }

        // 1 bps per $1,000 of notional
        let thousands = notional / Decimal::new(1000, 0);
        thousands * SLIPPAGE_BPS_PER_1000_USD
    }

    /// Process a trade update from the websocket feed using Queue Position fill model
    /// 
    /// Fill conditions:
    /// 1. Order must have existed for at least 50ms (latency simulation)
    /// 2. Trade price crosses order price (aggressive fill) OR
    /// 3. Cumulative volume at price exceeds volume_ahead (queue exhaustion)
    pub fn process_trade_update(
        &self,
        market_id: &MarketId,
        trade_side: Side,
        trade_price: Decimal,
        trade_size: Decimal,
        market_probability: Decimal,
    ) -> Vec<VirtualFill> {
        if !self.enabled.load(Ordering::Relaxed) {
            return Vec::new();
        }

        let mut fills = Vec::new();
        let matching_side = trade_side.opposite();

        // Step 1: Update cumulative volume for orders at the matching price level
        // This tracks queue position even if we don't fill now
        self.order_book.add_trade_volume(market_id, matching_side, trade_price, trade_size);

        // Step 2: Get orders eligible for fill (already checked 50ms minimum life)
        let candidates = self.order_book.get_fillable_orders(market_id, trade_side, trade_price);
        
        let mut remaining_size = trade_size;

        for order in candidates {
            if remaining_size <= Decimal::ZERO {
                break;
            }

            // Determine if trade crosses our order price
            let trade_crosses = match order.side {
                Side::Sell => trade_price > order.price,
                Side::Buy => trade_price < order.price,
            };

            // Check fill conditions using queue position model
            if !order.should_fill(trade_price, trade_crosses) {
                continue;
            }

            // Determine fill reason for logging
            let fill_reason = if trade_crosses {
                "PRICE_CROSS"
            } else {
                "QUEUE_EXHAUSTED"
            };

            // Determine fill size
            let fill_size = remaining_size.min(order.remaining());
            remaining_size -= fill_size;

            // Calculate fee/rebate based on 2026 fee curve
            let fee_bps = self.calculate_fee_bps(order.is_maker, market_probability);
            
            // Reality Check 3: Self-impact slippage
            let slippage_bps = self.calculate_slippage_bps(fill_size, order.price);
            let effective_price = if slippage_bps > Decimal::ZERO {
                // Degrade fill price against us
                let slip_factor = slippage_bps / Decimal::new(10000, 0);
                match order.side {
                    Side::Sell => order.price * (Decimal::ONE - slip_factor), // sell lower
                    Side::Buy => order.price * (Decimal::ONE + slip_factor),  // buy higher
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
                let fill = VirtualFill {
                    order_id: order.id,
                    market_id: *market_id,
                    side: order.side,
                    price: effective_price,
                    size: fill_size,
                    fee_bps,
                    net_pnl,
                    is_maker: order.is_maker,
                    timestamp_ns: VirtualOrder::now_ns(),
                };

                // Log with queue position info
                self.log_virtual_fill_with_queue(&fill, &order, market_probability, fill_reason, slippage_bps);

                // Reality Check 2: Track position for M2M
                let market_hash = VirtualOrderBook::hash_market_id_pub(market_id);
                self.position_tracker.record_fill(market_hash, order.side, fill_size, effective_price);
                // Update mid price from trade
                self.position_tracker.update_mid_price(market_hash, trade_price);

                // Update statistics
                self.record_fill(&fill);

                fills.push(fill);
            }
        }

        fills
    }

    /// Calculate fee in BPS using 2026 fee curve
    fn calculate_fee_bps(&self, is_maker: bool, probability: Decimal) -> i32 {
        if is_maker {
            // Maker rebate (negative fee)
            self.config.trading.fee_curve_2026.maker_rebate_bps
        } else {
            // Taker fee based on probability zone
            self.config.trading.fee_curve_2026.get_taker_fee_bps(probability) as i32
        }
    }

    /// Log a virtual fill with queue position info and equity summary
    fn log_virtual_fill_with_queue(
        &self,
        fill: &VirtualFill,
        order: &VirtualOrder,
        probability: Decimal,
        fill_reason: &str,
        slippage_bps: Decimal,
    ) {
        let fee_type = if fill.is_maker { "REBATE" } else { "FEE" };
        let fee_str = if fill.fee_bps < 0 {
            format!("+{} bps", -fill.fee_bps)
        } else {
            format!("-{} bps", fill.fee_bps)
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
            fill_reason,
            slippage_bps = %slippage_bps,
            "💰 Realized: ${} | 🔻 Unrealized: ${} | 🏁 Net Equity: ${} | FILL {} @ {} | {} {} | Reason: {} | Slip: {} bps",
            realized, unrealized, net_equity,
            fill.size, fill.price, fee_type, fee_str, fill_reason, slippage_bps
        );
    }

    /// Record a fill in statistics
    fn record_fill(&self, fill: &VirtualFill) {
        // Update balance
        let pnl_micro = Self::to_micro(fill.net_pnl);
        self.virtual_balance_micro.fetch_add(pnl_micro, Ordering::Relaxed);
        self.realized_pnl_micro.fetch_add(pnl_micro, Ordering::Relaxed);

        // Update trade counters
        self.total_trades.fetch_add(1, Ordering::Relaxed);
        
        if fill.net_pnl > Decimal::ZERO {
            self.winning_trades.fetch_add(1, Ordering::Relaxed);
        } else if fill.net_pnl < Decimal::ZERO {
            self.losing_trades.fetch_add(1, Ordering::Relaxed);
        }

        // Update maker/taker counters
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

        // Store in history
        self.fill_history.write().push(fill.clone());
    }

    /// Get current virtual balance
    pub fn get_balance(&self) -> Decimal {
        Self::from_micro(self.virtual_balance_micro.load(Ordering::Relaxed))
    }

    /// Get total realized PnL
    pub fn get_pnl(&self) -> Decimal {
        Self::from_micro(self.realized_pnl_micro.load(Ordering::Relaxed))
    }

    /// Get comprehensive statistics with mark-to-market unrealized PnL
    pub fn get_stats(&self) -> ShadowStats {
        let current_balance = self.get_balance();
        let realized_pnl = self.get_pnl();
        
        // Reality Check 2: Mark-to-Market unrealized PnL
        // (Current_Mid_Price - Average_Entry_Price) * Net_Position_Size
        let unrealized_pnl = self.position_tracker.total_unrealized_pnl();
        let net_equity = current_balance + unrealized_pnl;

        ShadowStats {
            initial_balance: Self::from_micro(self.initial_balance_micro),
            current_balance,
            total_pnl: realized_pnl + unrealized_pnl,
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

    /// Get fill history
    pub fn get_fill_history(&self) -> Vec<VirtualFill> {
        self.fill_history.read().clone()
    }

    /// Get active order count
    pub fn active_order_count(&self) -> usize {
        self.order_book.active_order_count()
    }

    /// Cancel all active orders (with latency check).
    /// Returns (cancelled, rejected_latency) counts.
    pub fn cancel_all_orders(&self) -> (usize, usize) {
        let active = self.order_book.active_orders();
        let total = active.len();
        let mut cancelled = 0;
        let mut rejected = 0;
        for order in active {
            match self.order_book.cancel_order_with_latency(order.id) {
                Ok(Some(_)) => cancelled += 1,
                Err(_) => rejected += 1,
                Ok(None) => {}
            }
        }
        info!(cancelled, rejected, total, "🚫 Shadow: Cancel all - {} cancelled, {} rejected (latency)", cancelled, rejected);
        (cancelled, rejected)
    }

    /// Reset the shadow engine to initial state
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
        info!("🔄 Shadow: Engine reset to initial state");
    }

    /// Enable/disable the engine
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
    }

    /// Check if enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    // Utility: Convert Decimal to micro-cents (i64)
    fn to_micro(d: Decimal) -> i64 {
        // Multiply by 1M and truncate to integer
        let scaled = d * Decimal::new(1_000_000, 0);
        scaled.trunc().to_i64().unwrap_or(0)
    }

    // Utility: Convert micro-cents to Decimal
    fn from_micro(micro: i64) -> Decimal {
        Decimal::new(micro, 6)
    }
}

// =============================================================================
// METRICS INTEGRATION
// =============================================================================

/// Shadow mode metrics for Prometheus export
pub struct ShadowMetrics {
    pub balance: f64,
    pub pnl: f64,
    pub total_trades: u64,
    pub win_rate: f64,
    pub maker_ratio: f64,
    pub net_fees: f64,
}

impl From<&ShadowStats> for ShadowMetrics {
    fn from(stats: &ShadowStats) -> Self {
        let win_rate = if stats.total_trades > 0 {
            stats.winning_trades as f64 / stats.total_trades as f64
        } else {
            0.0
        };

        let total_fills = stats.maker_fills + stats.taker_fills;
        let maker_ratio = if total_fills > 0 {
            stats.maker_fills as f64 / total_fills as f64
        } else {
            0.0
        };

        let net_fees = stats.total_rebates_earned.to_f64().unwrap_or(0.0)
            - stats.total_fees_paid.to_f64().unwrap_or(0.0);

        Self {
            balance: stats.current_balance.to_f64().unwrap_or(0.0),
            pnl: stats.total_pnl.to_f64().unwrap_or(0.0),
            total_trades: stats.total_trades,
            win_rate,
            maker_ratio,
            net_fees,
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

    #[test]
    fn test_shadow_engine_creation() {
        let config = mock_config();
        let engine = ShadowEngine::new(config);
        
        assert_eq!(engine.get_balance(), Decimal::new(10000, 0));
        assert_eq!(engine.get_pnl(), Decimal::ZERO);
    }

    #[test]
    fn test_track_virtual_order() {
        let config = mock_config();
        let engine = ShadowEngine::new(config);
        
        let signal = mock_signal(Side::Buy, Decimal::new(50, 2), Decimal::new(100, 0));
        let order_id = engine.track_virtual_order(&signal, true);
        
        assert!(order_id.is_some());
        assert!(order_id.unwrap() > 0);
        assert_eq!(engine.active_order_count(), 1);
    }

    #[test]
    fn test_virtual_order_book() {
        let book = VirtualOrderBook::new();
        let market_id = mock_market_id();
        
        let order = VirtualOrder::new(
            1,
            market_id,
            Side::Buy,
            Decimal::new(50, 2),
            Decimal::new(100, 0),
            OrderType::Limit,
            true,
            50,
        );
        
        book.add_order(order);
        
        let orders = book.get_orders_at_price(&market_id, Side::Buy, Decimal::new(50, 2));
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].price, Decimal::new(50, 2));
    }

    #[test]
    fn test_process_trade_no_immediate_fill() {
        // Orders should NOT fill immediately due to 50ms latency buffer
        let config = mock_config();
        let engine = ShadowEngine::new(config);
        let market_id = mock_market_id();
        
        // Place a sell order
        let signal = mock_signal(Side::Sell, Decimal::new(50, 2), Decimal::new(100, 0));
        let _ = engine.track_virtual_order(&signal, true);
        
        // Immediately process a buy trade - should NOT fill (order not eligible yet)
        let fills = engine.process_trade_update(
            &market_id,
            Side::Buy,
            Decimal::new(50, 2),
            Decimal::new(100, 0),
            Decimal::new(50, 2),
        );
        
        // No fills because order hasn't met 50ms minimum life
        assert!(fills.is_empty());
    }

    #[test]
    fn test_volume_ahead_calculation() {
        use crate::types::PriceLevel;
        
        // Create LOB with volume at bid/ask
        let mut lob = OrderBookSnapshot::new();
        lob.bids[0] = PriceLevel::new(Decimal::new(49, 2), Decimal::new(100, 0), 1);
        lob.bids[1] = PriceLevel::new(Decimal::new(48, 2), Decimal::new(200, 0), 1);
        lob.asks[0] = PriceLevel::new(Decimal::new(51, 2), Decimal::new(150, 0), 1);
        lob.bid_count = 2;
        lob.ask_count = 1;
        
        // Buy order at 0.49 should see 100 volume ahead (same price level)
        let buy_vol = VirtualOrder::calculate_volume_ahead(
            Side::Buy, Decimal::new(49, 2), Some(&lob)
        );
        assert_eq!(buy_vol, Decimal::new(100, 0));
        
        // Sell order at 0.51 should see 150 volume ahead
        let sell_vol = VirtualOrder::calculate_volume_ahead(
            Side::Sell, Decimal::new(51, 2), Some(&lob)
        );
        assert_eq!(sell_vol, Decimal::new(150, 0));
    }

    #[test]
    fn test_cumulative_volume_tracking() {
        let book = VirtualOrderBook::new();
        let market_id = mock_market_id();
        
        let order = VirtualOrder::new(
            1,
            market_id,
            Side::Buy,
            Decimal::new(49, 2),
            Decimal::new(10, 0),
            OrderType::Limit,
            true,
            50,
        );
        book.add_order(order);
        
        // Add trade volume at the order's price level
        book.add_trade_volume(&market_id, Side::Buy, Decimal::new(49, 2), Decimal::new(50, 0));
        
        // Check cumulative volume was updated
        let orders = book.get_orders_at_price(&market_id, Side::Buy, Decimal::new(49, 2));
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].cumulative_volume, Decimal::new(50, 0));
    }

    #[test]
    fn test_order_eligibility() {
        let market_id = mock_market_id();
        
        let order = VirtualOrder::new(
            1,
            market_id,
            Side::Buy,
            Decimal::new(50, 2),
            Decimal::new(100, 0),
            OrderType::Limit,
            true,
            50,
        );
        
        // Order should not be eligible immediately (50ms latency)
        assert!(!order.is_eligible_for_fill());
        
        // Order should be active
        assert!(order.is_active());
    }

    #[test]
    fn test_should_fill_price_cross() {
        let market_id = mock_market_id();
        
        // Create order with eligible_at_ns in the past to simulate time passing
        let mut order = VirtualOrder::new(
            1,
            market_id,
            Side::Sell,
            Decimal::new(50, 2),
            Decimal::new(100, 0),
            OrderType::Limit,
            true,
            50,
        );
        // Manually make it eligible by setting eligible_at_ns to past
        order.eligible_at_ns = 0;
        
        // Trade crosses (buys at price > our sell) should fill
        assert!(order.should_fill(Decimal::new(51, 2), true));
        
        // Trade at same price without queue exhaustion should not fill
        order.volume_ahead = Decimal::new(100, 0);
        order.cumulative_volume = Decimal::new(50, 0);
        assert!(!order.should_fill(Decimal::new(50, 2), false));
    }

    #[test]
    fn test_should_fill_queue_exhaustion() {
        let market_id = mock_market_id();
        
        let mut order = VirtualOrder::new(
            1,
            market_id,
            Side::Buy,
            Decimal::new(49, 2),
            Decimal::new(10, 0),
            OrderType::Limit,
            true,
            50,
        );
        // Make eligible
        order.eligible_at_ns = 0;
        order.volume_ahead = Decimal::new(100, 0);
        
        // Not enough cumulative volume yet
        order.cumulative_volume = Decimal::new(50, 0);
        assert!(!order.should_fill(Decimal::new(49, 2), false));
        
        // Queue exhausted - cumulative > volume_ahead
        order.cumulative_volume = Decimal::new(101, 0);
        assert!(order.should_fill(Decimal::new(49, 2), false));
    }

    #[test]
    fn test_rate_limiter_basic() {
        let bucket = TokenBucket::new(60, 60);
        // Should allow 60 orders (burst)
        for _ in 0..60 {
            assert!(bucket.try_acquire());
        }
        // 61st should be rejected
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn test_cancel_with_latency() {
        let book = VirtualOrderBook::new();
        let market_id = mock_market_id();
        
        let order = VirtualOrder::new(
            1, market_id, Side::Buy,
            Decimal::new(50, 2), Decimal::new(10, 0),
            OrderType::Limit, true, 50,
        );
        book.add_order(order);
        
        // Cancel should fail - order is still in latency window
        let result = book.cancel_order_with_latency(1);
        assert!(result.is_err());
    }

    #[test]
    fn test_position_tracker_m2m() {
        let tracker = PositionTracker::new();
        let market_hash = 12345u64;
        
        // Buy 100 @ 0.50
        tracker.record_fill(market_hash, Side::Buy, Decimal::new(100, 0), Decimal::new(50, 2));
        
        // Mid price rises to 0.55 -> unrealized = (0.55 - 0.50) * 100 = 5.0
        tracker.update_mid_price(market_hash, Decimal::new(55, 2));
        let unrealized = tracker.total_unrealized_pnl();
        assert_eq!(unrealized, Decimal::new(5, 0));
        
        // Mid price drops to 0.45 -> unrealized = (0.45 - 0.50) * 100 = -5.0
        tracker.update_mid_price(market_hash, Decimal::new(45, 2));
        let unrealized = tracker.total_unrealized_pnl();
        assert_eq!(unrealized, Decimal::new(-5, 0));
    }

    #[test]
    fn test_slippage_calculation() {
        let config = mock_config();
        let engine = ShadowEngine::new(config);
        // Default daily volume is 500,000
        
        // Small order: $100 notional = 0.02% of daily vol -> no slippage
        let slip = engine.calculate_slippage_bps(
            Decimal::new(200, 0), Decimal::new(50, 2)
        );
        assert_eq!(slip, Decimal::ZERO);
        
        // Large order: $10,000 notional = 2% of daily vol -> slippage applies
        // 10,000 / 1,000 = 10 thousands * 1 bps = 10 bps
        let slip = engine.calculate_slippage_bps(
            Decimal::new(20000, 0), Decimal::new(50, 2)
        );
        assert_eq!(slip, Decimal::new(10, 0));
    }

    #[test]
    fn test_stats_include_new_fields() {
        let config = mock_config();
        let engine = ShadowEngine::new(config);
        let stats = engine.get_stats();
        
        assert_eq!(stats.orders_throttled, 0);
        assert_eq!(stats.total_slippage, Decimal::ZERO);
        assert_eq!(stats.unrealized_pnl, Decimal::ZERO);
        assert_eq!(stats.net_equity, stats.current_balance);
    }
}
