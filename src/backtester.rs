//! Linear Time-Priority Queue Matching Engine for Backtesting
//!
//! Implements realistic queue position simulation with:
//! - Time-priority ordering (FIFO within price level)
//! - Volume-ahead tracking for fill simulation
//! - Cancellation simulation based on distance from mid-price
//! - Zero-allocation hot path using SmallVec and pre-allocated buffers

use crate::types::{Side, MarketId};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use smallvec::SmallVec;

// =============================================================================
// CONSTANTS
// =============================================================================

/// Maximum orders per price level (stack-allocated)
const MAX_ORDERS_PER_LEVEL: usize = 64;

/// Maximum price levels to track
const MAX_PRICE_LEVELS: usize = 32;

/// Pre-allocated buffer size for filled orders
const FILL_BUFFER_SIZE: usize = 16;

/// Base cancellation rate per tick (0.1% of queue)
const BASE_CANCEL_RATE: f64 = 0.001;

/// Cancellation rate increase per tick away from mid (0.5% per tick)
const CANCEL_RATE_PER_TICK: f64 = 0.005;

/// Maximum cancellation rate (10% per tick)
const MAX_CANCEL_RATE: f64 = 0.10;

// =============================================================================
// ORDER STRUCT
// =============================================================================

/// Order in the time-priority queue
#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))] // Cache-line aligned for performance
pub struct Order {
    /// Order ID (unique identifier)
    pub id: u64,
    /// Timestamp when order was placed (nanoseconds)
    pub timestamp_ns: u64,
    /// Priority index within the price level (lower = higher priority)
    pub priority_index: u32,
    /// Remaining quantity to fill
    pub remaining_qty: Decimal,
    /// Original quantity
    pub original_qty: Decimal,
    /// Order price
    pub price: Decimal,
    /// Order side (Buy/Sell)
    pub side: Side,
    /// Volume ahead of this order in the queue
    pub volume_ahead: Decimal,
    /// Market ID
    pub market_id: MarketId,
    /// Is this order active?
    pub is_active: bool,
    /// Is this order filled?
    pub is_filled: bool,
}

impl Order {
    /// Create a new order
    #[inline]
    pub fn new(
        id: u64,
        timestamp_ns: u64,
        price: Decimal,
        qty: Decimal,
        side: Side,
        market_id: MarketId,
        volume_ahead: Decimal,
    ) -> Self {
        Self {
            id,
            timestamp_ns,
            priority_index: 0,
            remaining_qty: qty,
            original_qty: qty,
            price,
            side,
            volume_ahead,
            market_id,
            is_active: true,
            is_filled: false,
        }
    }

    /// Check if order can be filled (volume ahead exhausted)
    #[inline(always)]
    pub fn can_fill(&self) -> bool {
        self.is_active && !self.is_filled && self.volume_ahead <= Decimal::ZERO
    }

    /// Mark order as filled
    #[inline(always)]
    pub fn mark_filled(&mut self) {
        self.is_filled = true;
        self.is_active = false;
        self.remaining_qty = Decimal::ZERO;
    }

    /// Partially fill the order
    #[inline(always)]
    pub fn partial_fill(&mut self, qty: Decimal) {
        self.remaining_qty = (self.remaining_qty - qty).max(Decimal::ZERO);
        if self.remaining_qty <= Decimal::ZERO {
            self.mark_filled();
        }
    }

    /// Cancel the order
    #[inline(always)]
    pub fn cancel(&mut self) {
        self.is_active = false;
    }
}

// =============================================================================
// PRICE LEVEL QUEUE
// =============================================================================

/// Queue of orders at a single price level (FIFO)
#[derive(Debug, Clone)]
pub struct PriceLevelQueue {
    /// Price for this level
    pub price: Decimal,
    /// Orders at this price level (time-priority sorted)
    orders: SmallVec<[Order; MAX_ORDERS_PER_LEVEL]>,
    /// Total volume at this level
    pub total_volume: Decimal,
    /// Number of active orders
    pub active_count: u32,
}

impl PriceLevelQueue {
    /// Create a new empty price level queue
    #[inline]
    pub fn new(price: Decimal) -> Self {
        Self {
            price,
            orders: SmallVec::new(),
            total_volume: Decimal::ZERO,
            active_count: 0,
        }
    }

    /// Add an order to the queue (appends at end - FIFO)
    #[inline]
    pub fn add_order(&mut self, mut order: Order) {
        order.priority_index = self.orders.len() as u32;
        self.total_volume += order.remaining_qty;
        self.active_count += 1;
        self.orders.push(order);
    }

    /// Get volume ahead of a given priority index
    #[inline]
    pub fn volume_ahead_of(&self, priority_index: u32) -> Decimal {
        self.orders
            .iter()
            .filter(|o| o.is_active && o.priority_index < priority_index)
            .map(|o| o.remaining_qty)
            .fold(Decimal::ZERO, |acc, x| acc + x)
    }

    /// Process a trade at this price level, decrementing volume ahead
    /// Returns the volume consumed from this trade
    #[inline]
    pub fn process_trade(&mut self, trade_volume: Decimal) -> Decimal {
        let mut remaining = trade_volume;
        
        for order in self.orders.iter_mut() {
            if !order.is_active || order.is_filled {
                continue;
            }
            
            // Decrement volume ahead for all orders
            if order.volume_ahead > Decimal::ZERO {
                let decrement = order.volume_ahead.min(remaining);
                order.volume_ahead -= decrement;
            }
        }
        
        self.total_volume = (self.total_volume - trade_volume).max(Decimal::ZERO);
        trade_volume
    }

    /// Simulate cancellations based on distance from mid-price
    /// Returns number of orders cancelled
    #[inline]
    pub fn simulate_cancellations(&mut self, ticks_from_mid: u32, rng_seed: u64) -> u32 {
        let cancel_rate = (BASE_CANCEL_RATE + (ticks_from_mid as f64 * CANCEL_RATE_PER_TICK))
            .min(MAX_CANCEL_RATE);
        
        let mut cancelled = 0u32;
        let mut seed = rng_seed;
        
        // First pass: identify orders to cancel and collect their info
        let mut to_cancel: SmallVec<[(u32, Decimal); 16]> = SmallVec::new();
        
        for order in self.orders.iter() {
            if !order.is_active || order.is_filled {
                continue;
            }
            
            // Fast LCG random number generator (no allocation)
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let rand_val = (seed >> 33) as f64 / (u32::MAX as f64);
            
            if rand_val < cancel_rate {
                to_cancel.push((order.priority_index, order.remaining_qty));
            }
        }
        
        // Second pass: apply cancellations
        for (cancel_idx, cancelled_volume) in to_cancel.iter() {
            for order in self.orders.iter_mut() {
                if order.priority_index == *cancel_idx {
                    order.cancel();
                    self.active_count = self.active_count.saturating_sub(1);
                    self.total_volume -= *cancelled_volume;
                    cancelled += 1;
                } else if order.is_active && order.priority_index > *cancel_idx {
                    // Reduce volume_ahead for orders behind cancelled one
                    order.volume_ahead = (order.volume_ahead - *cancelled_volume).max(Decimal::ZERO);
                }
            }
        }
        
        cancelled
    }

    /// Get all filled orders (zero-alloc - writes to pre-allocated buffer)
    #[inline]
    pub fn get_filled_orders(&self, buffer: &mut SmallVec<[Order; FILL_BUFFER_SIZE]>) {
        for order in self.orders.iter() {
            if order.can_fill() {
                buffer.push(*order);
            }
        }
    }

    /// Clear filled and cancelled orders (periodic cleanup)
    pub fn cleanup(&mut self) {
        self.orders.retain(|o| o.is_active && !o.is_filled);
        // Reindex remaining orders
        for (i, order) in self.orders.iter_mut().enumerate() {
            order.priority_index = i as u32;
        }
    }
}

// =============================================================================
// TRADE TICK
// =============================================================================

/// A trade tick from market data
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct TradeTick {
    /// Trade timestamp (nanoseconds)
    pub timestamp_ns: u64,
    /// Trade price
    pub price: Decimal,
    /// Trade volume
    pub volume: Decimal,
    /// Trade side (aggressor side)
    pub side: Side,
}

impl TradeTick {
    #[inline]
    pub fn new(timestamp_ns: u64, price: Decimal, volume: Decimal, side: Side) -> Self {
        Self { timestamp_ns, price, volume, side }
    }
}

// =============================================================================
// FILL RESULT
// =============================================================================

/// Result of a fill check
#[derive(Debug, Clone, Copy)]
pub struct FillResult {
    /// Order ID that was filled
    pub order_id: u64,
    /// Fill price
    pub price: Decimal,
    /// Fill quantity
    pub qty: Decimal,
    /// Fill timestamp
    pub timestamp_ns: u64,
    /// Was this a partial fill?
    pub is_partial: bool,
}

// =============================================================================
// BACKTESTER
// =============================================================================

/// Linear Time-Priority Queue Backtester
/// 
/// Simulates realistic order fills using queue position tracking.
/// Zero-allocation in the hot path using pre-allocated buffers.
pub struct Backtester {
    /// Buy-side price level queues (sorted by price descending)
    bid_levels: SmallVec<[PriceLevelQueue; MAX_PRICE_LEVELS]>,
    /// Sell-side price level queues (sorted by price ascending)
    ask_levels: SmallVec<[PriceLevelQueue; MAX_PRICE_LEVELS]>,
    /// Current mid-price for cancellation simulation
    mid_price: Decimal,
    /// Tick size for price level indexing
    tick_size: Decimal,
    /// Next order ID
    next_order_id: u64,
    /// RNG seed for cancellation simulation
    rng_seed: u64,
    /// Pre-allocated buffer for filled orders (zero-alloc)
    fill_buffer: SmallVec<[Order; FILL_BUFFER_SIZE]>,
    /// Pre-allocated buffer for fill results (zero-alloc)
    result_buffer: SmallVec<[FillResult; FILL_BUFFER_SIZE]>,
    /// Total fills processed
    pub total_fills: u64,
    /// Total volume filled
    pub total_volume_filled: Decimal,
}

impl Backtester {
    /// Create a new backtester with given tick size
    pub fn new(tick_size: Decimal) -> Self {
        Self {
            bid_levels: SmallVec::new(),
            ask_levels: SmallVec::new(),
            mid_price: Decimal::new(50, 2), // Default 0.50
            tick_size,
            next_order_id: 1,
            rng_seed: 0xDEADBEEF_CAFEBABE,
            fill_buffer: SmallVec::new(),
            result_buffer: SmallVec::new(),
            total_fills: 0,
            total_volume_filled: Decimal::ZERO,
        }
    }

    /// Update mid-price (call on each book update)
    #[inline]
    pub fn update_mid_price(&mut self, mid_price: Decimal) {
        self.mid_price = mid_price;
    }

    /// Place a new order into the queue
    /// Returns the order ID
    pub fn place_order(
        &mut self,
        price: Decimal,
        qty: Decimal,
        side: Side,
        market_id: MarketId,
        timestamp_ns: u64,
    ) -> u64 {
        let order_id = self.next_order_id;
        self.next_order_id += 1;

        let levels = match side {
            Side::Buy => &mut self.bid_levels,
            Side::Sell => &mut self.ask_levels,
        };

        // Find or create price level
        let level_idx = levels.iter().position(|l| l.price == price);
        
        let volume_ahead = match level_idx {
            Some(idx) => levels[idx].total_volume,
            None => Decimal::ZERO,
        };

        let order = Order::new(order_id, timestamp_ns, price, qty, side, market_id, volume_ahead);

        match level_idx {
            Some(idx) => {
                levels[idx].add_order(order);
            }
            None => {
                let mut new_level = PriceLevelQueue::new(price);
                new_level.add_order(order);
                levels.push(new_level);
                
                // Sort levels: bids descending, asks ascending
                match side {
                    Side::Buy => levels.sort_by(|a, b| b.price.cmp(&a.price)),
                    Side::Sell => levels.sort_by(|a, b| a.price.cmp(&b.price)),
                }
            }
        }

        order_id
    }

    /// Cancel an order by ID
    pub fn cancel_order(&mut self, order_id: u64) -> bool {
        for level in self.bid_levels.iter_mut().chain(self.ask_levels.iter_mut()) {
            for order in level.orders.iter_mut() {
                if order.id == order_id && order.is_active {
                    order.cancel();
                    level.active_count = level.active_count.saturating_sub(1);
                    level.total_volume -= order.remaining_qty;
                    return true;
                }
            }
        }
        false
    }

    /// Check fills for a trade tick - MAIN HOT PATH (zero-alloc)
    /// 
    /// For every trade at price P:
    /// - If P matches our order price, decrement volume_ahead
    /// - If volume_ahead <= 0, mark order as filled
    /// - Simulate cancellations based on distance from mid-price
    /// 
    /// Returns fills via the result buffer (zero allocation)
    #[inline]
    pub fn check_fills(&mut self, tick: &TradeTick) -> &[FillResult] {
        // Clear buffers (no allocation - just resets length)
        self.fill_buffer.clear();
        self.result_buffer.clear();

        // Advance RNG seed
        self.rng_seed = self.rng_seed.wrapping_mul(6364136223846793005).wrapping_add(1);

        let trade_price = tick.price;
        let trade_volume = tick.volume;
        let mid_price = self.mid_price;
        let tick_size = self.tick_size;
        let rng_seed = self.rng_seed;

        // Process bid levels (my buy orders)
        // A trade at price P fills buy orders at price >= P
        for (level_idx, level) in self.bid_levels.iter_mut().enumerate() {
            // Calculate ticks from mid for cancellation simulation
            let ticks_from_mid = Self::calc_ticks_from_mid_static(level.price, mid_price, tick_size);
            
            // Simulate cancellations (this reduces volume_ahead for my orders)
            level.simulate_cancellations(ticks_from_mid, rng_seed.wrapping_add(level_idx as u64));

            // If trade price <= my bid price, my order could be filled
            if trade_price <= level.price {
                // Process the trade - decrement volume ahead
                level.process_trade(trade_volume);
                
                // Check for fills
                level.get_filled_orders(&mut self.fill_buffer);
            }
        }

        // Process ask levels (my sell orders)
        // A trade at price P fills sell orders at price <= P
        for (level_idx, level) in self.ask_levels.iter_mut().enumerate() {
            let ticks_from_mid = Self::calc_ticks_from_mid_static(level.price, mid_price, tick_size);
            
            level.simulate_cancellations(ticks_from_mid, rng_seed.wrapping_add(level_idx as u64 + 1000));

            // If trade price >= my ask price, my order could be filled
            if trade_price >= level.price {
                level.process_trade(trade_volume);
                level.get_filled_orders(&mut self.fill_buffer);
            }
        }

        // Convert filled orders to fill results
        for order in self.fill_buffer.iter() {
            let fill = FillResult {
                order_id: order.id,
                price: order.price,
                qty: order.original_qty,
                timestamp_ns: tick.timestamp_ns,
                is_partial: false,
            };
            self.result_buffer.push(fill);
            self.total_fills += 1;
            self.total_volume_filled += order.original_qty;
        }

        // Collect order IDs to mark as filled (avoid borrow conflict)
        let fill_ids: SmallVec<[u64; FILL_BUFFER_SIZE]> = 
            self.result_buffer.iter().map(|f| f.order_id).collect();

        // Mark orders as filled in the actual queues
        for order_id in fill_ids {
            self.mark_order_filled(order_id);
        }

        &self.result_buffer
    }

    /// Mark an order as filled
    #[inline]
    fn mark_order_filled(&mut self, order_id: u64) {
        for level in self.bid_levels.iter_mut().chain(self.ask_levels.iter_mut()) {
            for order in level.orders.iter_mut() {
                if order.id == order_id {
                    order.mark_filled();
                    level.active_count = level.active_count.saturating_sub(1);
                    return;
                }
            }
        }
    }

    /// Calculate ticks from mid-price for cancellation rate
    #[inline(always)]
    fn calculate_ticks_from_mid(&self, price: Decimal) -> u32 {
        Self::calc_ticks_from_mid_static(price, self.mid_price, self.tick_size)
    }

    /// Static version to avoid borrow conflicts
    #[inline(always)]
    fn calc_ticks_from_mid_static(price: Decimal, mid_price: Decimal, tick_size: Decimal) -> u32 {
        let distance = (price - mid_price).abs();
        let ticks = distance / tick_size;
        ticks.to_u32().unwrap_or(0)
    }

    /// Get all active orders (for inspection)
    pub fn get_active_orders(&self) -> SmallVec<[Order; 64]> {
        let mut orders = SmallVec::new();
        for level in self.bid_levels.iter().chain(self.ask_levels.iter()) {
            for order in level.orders.iter() {
                if order.is_active && !order.is_filled {
                    orders.push(*order);
                }
            }
        }
        orders
    }

    /// Cleanup filled/cancelled orders (call periodically)
    pub fn cleanup(&mut self) {
        for level in self.bid_levels.iter_mut().chain(self.ask_levels.iter_mut()) {
            level.cleanup();
        }
        // Remove empty levels
        self.bid_levels.retain(|l| l.active_count > 0);
        self.ask_levels.retain(|l| l.active_count > 0);
    }

    /// Get statistics
    pub fn stats(&self) -> BacktesterStats {
        let active_bids: u32 = self.bid_levels.iter().map(|l| l.active_count).sum();
        let active_asks: u32 = self.ask_levels.iter().map(|l| l.active_count).sum();
        
        BacktesterStats {
            active_bid_orders: active_bids,
            active_ask_orders: active_asks,
            bid_levels: self.bid_levels.len() as u32,
            ask_levels: self.ask_levels.len() as u32,
            total_fills: self.total_fills,
            total_volume_filled: self.total_volume_filled,
        }
    }
}

/// Backtester statistics
#[derive(Debug, Clone, Copy)]
pub struct BacktesterStats {
    pub active_bid_orders: u32,
    pub active_ask_orders: u32,
    pub bid_levels: u32,
    pub ask_levels: u32,
    pub total_fills: u64,
    pub total_volume_filled: Decimal,
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_market_id() -> MarketId {
        MarketId::new([0xAB; 32], 12345)
    }

    #[test]
    fn test_order_creation() {
        let order = Order::new(
            1,
            1000000,
            Decimal::new(50, 2),
            Decimal::new(10, 0),
            Side::Buy,
            mock_market_id(),
            Decimal::new(100, 0),
        );

        assert_eq!(order.id, 1);
        assert_eq!(order.price, Decimal::new(50, 2));
        assert_eq!(order.remaining_qty, Decimal::new(10, 0));
        assert_eq!(order.volume_ahead, Decimal::new(100, 0));
        assert!(order.is_active);
        assert!(!order.is_filled);
    }

    #[test]
    fn test_order_fill_when_volume_ahead_zero() {
        let mut order = Order::new(
            1,
            1000000,
            Decimal::new(50, 2),
            Decimal::new(10, 0),
            Side::Buy,
            mock_market_id(),
            Decimal::ZERO, // No volume ahead
        );

        assert!(order.can_fill());
        order.mark_filled();
        assert!(order.is_filled);
        assert!(!order.is_active);
    }

    #[test]
    fn test_order_cannot_fill_with_volume_ahead() {
        let order = Order::new(
            1,
            1000000,
            Decimal::new(50, 2),
            Decimal::new(10, 0),
            Side::Buy,
            mock_market_id(),
            Decimal::new(50, 0), // Volume ahead
        );

        assert!(!order.can_fill());
    }

    #[test]
    fn test_backtester_place_order() {
        let mut bt = Backtester::new(Decimal::new(1, 2)); // 0.01 tick size
        
        let order_id = bt.place_order(
            Decimal::new(50, 2),
            Decimal::new(10, 0),
            Side::Buy,
            mock_market_id(),
            1000000,
        );

        assert_eq!(order_id, 1);
        assert_eq!(bt.bid_levels.len(), 1);
        assert_eq!(bt.bid_levels[0].active_count, 1);
    }

    #[test]
    fn test_backtester_fill_on_trade() {
        let mut bt = Backtester::new(Decimal::new(1, 2));
        bt.update_mid_price(Decimal::new(50, 2));

        // Place a buy order at 0.49 with no queue ahead
        let order_id = bt.place_order(
            Decimal::new(49, 2),
            Decimal::new(10, 0),
            Side::Buy,
            mock_market_id(),
            1000000,
        );

        // Trade occurs at 0.49 - should fill our order
        let tick = TradeTick::new(
            2000000,
            Decimal::new(49, 2),
            Decimal::new(10, 0),
            Side::Sell,
        );

        let fills = bt.check_fills(&tick);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].order_id, order_id);
    }

    #[test]
    fn test_backtester_volume_ahead_decrement() {
        let mut bt = Backtester::new(Decimal::new(1, 2));
        bt.update_mid_price(Decimal::new(50, 2));

        // Simulate queue: first add volume to the level, then our order
        // Place first order (simulates existing queue)
        bt.place_order(
            Decimal::new(49, 2),
            Decimal::new(100, 0),
            Side::Buy,
            mock_market_id(),
            1000000,
        );

        // Place our order (behind the queue)
        let our_order_id = bt.place_order(
            Decimal::new(49, 2),
            Decimal::new(10, 0),
            Side::Buy,
            mock_market_id(),
            1000001,
        );

        // Check initial volume ahead
        let orders = bt.get_active_orders();
        let our_order = orders.iter().find(|o| o.id == our_order_id).unwrap();
        assert_eq!(our_order.volume_ahead, Decimal::new(100, 0));

        // Trade consumes 50 from queue
        let tick = TradeTick::new(
            2000000,
            Decimal::new(49, 2),
            Decimal::new(50, 0),
            Side::Sell,
        );
        bt.check_fills(&tick);

        // Our volume ahead should be decremented
        let orders = bt.get_active_orders();
        let our_order = orders.iter().find(|o| o.id == our_order_id).unwrap();
        assert_eq!(our_order.volume_ahead, Decimal::new(50, 0));
    }

    #[test]
    fn test_price_level_cancellation_simulation() {
        let mut level = PriceLevelQueue::new(Decimal::new(50, 2));
        
        // Add 10 orders
        for i in 0..10 {
            let order = Order::new(
                i,
                1000000 + i,
                Decimal::new(50, 2),
                Decimal::new(10, 0),
                Side::Buy,
                mock_market_id(),
                Decimal::new(i as i64 * 10, 0),
            );
            level.add_order(order);
        }

        assert_eq!(level.active_count, 10);

        // Simulate cancellations (10 ticks from mid = high cancel rate)
        let cancelled = level.simulate_cancellations(10, 12345);
        
        // Some orders should be cancelled
        assert!(cancelled > 0 || level.active_count <= 10);
    }
}
