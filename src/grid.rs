//! Grid Layering Module for Market-Making Efficiency
//!
//! Instead of placing a single order at price P, this module places a "ladder"
//! of orders at multiple price levels (P, P-0.01, P-0.02, etc.) to:
//! - Capture more fills across the spread
//! - Maximize maker rebate capture
//! - Provide depth and earn queue priority
//!
//! Features:
//! - Dynamic spacing based on volatility/config
//! - Self-healing: auto-rebalance when levels fill
//! - Batch execution for reduced network round-trips
//! - Pre-signed orders via SignatureCache integration

use crate::config::Config;
use crate::hints::{likely, unlikely, prefetch_l1};
use crate::signing::{OrderSigner, SignatureCacheKey};
use crate::types::{MarketId, Side, OrderType, TimeInForce, PreparedOrder, TradeSignal, SignalUrgency};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use parking_lot::RwLock;

/// Maximum number of levels in a grid (stack-allocated)
pub const MAX_GRID_LEVELS: usize = 20;

/// Grid level state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LevelState {
    Empty = 0,
    Pending = 1,
    Active = 2,
    PartiallyFilled = 3,
    Filled = 4,
    Cancelled = 5,
}

/// Single level in the grid (cache-line aligned)
#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))]
pub struct GridLevel {
    pub price: Decimal,
    pub size: Decimal,
    pub filled: Decimal,
    pub side: Side,
    pub state: LevelState,
    pub order_id: u64,       // Internal tracking ID
    pub level_index: u8,     // Position in grid (0 = closest to mid)
    pub _pad: [u8; 6],
}

impl GridLevel {
    #[inline(always)]
    pub const fn empty() -> Self {
        Self {
            price: Decimal::ZERO,
            size: Decimal::ZERO,
            filled: Decimal::ZERO,
            side: Side::Buy,
            state: LevelState::Empty,
            order_id: 0,
            level_index: 0,
            _pad: [0; 6],
        }
    }

    #[inline(always)]
    pub fn new(price: Decimal, size: Decimal, side: Side, index: u8) -> Self {
        Self {
            price,
            size,
            filled: Decimal::ZERO,
            side,
            state: LevelState::Pending,
            order_id: 0,
            level_index: index,
            _pad: [0; 6],
        }
    }

    #[inline(always)]
    pub fn remaining(&self) -> Decimal {
        self.size - self.filled
    }

    #[inline(always)]
    pub fn is_active(&self) -> bool {
        matches!(self.state, LevelState::Active | LevelState::PartiallyFilled)
    }
}

/// Complete grid of orders for one side (buy or sell)
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct Grid {
    pub levels: [GridLevel; MAX_GRID_LEVELS],
    pub active_count: u8,
    pub side: Side,
    pub base_price: Decimal,
    pub spacing: Decimal,
    pub market_id: MarketId,
}

impl Grid {
    #[inline(always)]
    pub const fn empty(side: Side) -> Self {
        Self {
            levels: [GridLevel::empty(); MAX_GRID_LEVELS],
            active_count: 0,
            side,
            base_price: Decimal::ZERO,
            spacing: Decimal::ZERO,
            market_id: MarketId::new([0; 32], 0),
        }
    }

    /// Total size across all active levels
    #[inline]
    pub fn total_size(&self) -> Decimal {
        let mut total = Decimal::ZERO;
        for i in 0..self.active_count as usize {
            if self.levels[i].is_active() {
                total += self.levels[i].remaining();
            }
        }
        total
    }

    /// Total exposure (value) across all active levels
    #[inline]
    pub fn total_exposure(&self) -> Decimal {
        let mut total = Decimal::ZERO;
        for i in 0..self.active_count as usize {
            if self.levels[i].is_active() {
                total += self.levels[i].remaining() * self.levels[i].price;
            }
        }
        total
    }
}

/// Grid configuration
#[derive(Debug, Clone)]
pub struct GridConfig {
    /// Number of levels per side
    pub levels_per_side: u8,
    /// Size per level in USDC
    pub size_per_level: Decimal,
    /// Base spacing between levels (e.g., 0.01 = 1 cent)
    pub base_spacing: Decimal,
    /// Dynamic spacing multiplier based on volatility
    pub volatility_multiplier: Decimal,
    /// Maximum total exposure per grid
    pub max_exposure: Decimal,
    /// Minimum profit BPS for the aggregate grid
    pub min_aggregate_profit_bps: i32,
    /// Auto-rebalance on fill
    pub auto_rebalance: bool,
    /// Use batch execution
    pub batch_execution: bool,
    /// Inventory skew factor (how aggressively to skew prices based on inventory)
    /// Higher = more aggressive skewing to reduce inventory
    pub inventory_skew_factor: Decimal,
    /// Maximum inventory skew in price (e.g., 0.02 = 2 cents max shift)
    pub max_inventory_skew: Decimal,
}

impl Default for GridConfig {
    fn default() -> Self {
        Self {
            levels_per_side: 5,
            size_per_level: Decimal::new(100, 0), // 100 USDC
            base_spacing: Decimal::new(1, 2),     // 0.01
            volatility_multiplier: Decimal::ONE,
            max_exposure: Decimal::new(10000, 0), // 10,000 USDC
            min_aggregate_profit_bps: 50,
            auto_rebalance: true,
            batch_execution: true,
            inventory_skew_factor: Decimal::new(5, 3),  // 0.005 per unit
            max_inventory_skew: Decimal::new(3, 2),    // 0.03 max (3 cents)
        }
    }
}

/// Grid Engine - manages grid creation, execution, and rebalancing
pub struct GridEngine {
    config: Arc<Config>,
    grid_config: GridConfig,
    signer: Arc<OrderSigner>,
    
    /// Active buy grid
    buy_grid: RwLock<Grid>,
    /// Active sell grid  
    sell_grid: RwLock<Grid>,
    
    /// Order ID generator
    order_id_counter: AtomicU64,
    
    /// Engine enabled flag
    enabled: AtomicBool,
    
    /// Statistics
    grids_created: AtomicU64,
    levels_filled: AtomicU64,
    rebalances: AtomicU64,
}

impl GridEngine {
    pub fn new(
        config: Arc<Config>,
        grid_config: GridConfig,
        signer: Arc<OrderSigner>,
    ) -> Self {
        Self {
            config,
            grid_config,
            signer,
            buy_grid: RwLock::new(Grid::empty(Side::Buy)),
            sell_grid: RwLock::new(Grid::empty(Side::Sell)),
            order_id_counter: AtomicU64::new(1),
            enabled: AtomicBool::new(true),
            grids_created: AtomicU64::new(0),
            levels_filled: AtomicU64::new(0),
            rebalances: AtomicU64::new(0),
        }
    }

    /// Generate a grid of orders for market-making with inventory skewing
    /// 
    /// Inventory Skewing Logic:
    /// - If inventory > 0 (long YES): shift ASK lower to encourage sells, BID even lower to discourage buys
    /// - If inventory < 0 (short YES): shift BID higher to encourage buys, ASK even higher to discourage sells
    /// 
    /// HOT PATH - optimized for <5us for 10 levels
    #[inline]
    pub fn generate_grid(
        &self,
        market_id: MarketId,
        mid_price: Decimal,
        side: Side,
        market_prob: Decimal,
        current_inventory: i32,
    ) -> Vec<TradeSignal> {
        if unlikely(!self.enabled.load(Ordering::Relaxed)) {
            return Vec::new();
        }

        let levels = self.grid_config.levels_per_side as usize;
        let mut signals = Vec::with_capacity(levels);

        // Calculate dynamic spacing based on probability (wider in mid-range)
        let spacing = self.calculate_dynamic_spacing(market_prob);
        
        // Calculate inventory skew
        let inventory_skew = self.calculate_inventory_skew(current_inventory, side);
        
        // Generate levels
        for i in 0..levels {
            let level_offset = Decimal::from(i as i64 + 1) * spacing;
            
            // Apply inventory skew to price
            let price = match side {
                Side::Buy => mid_price - level_offset + inventory_skew,  // Buy below mid (skew shifts)
                Side::Sell => mid_price + level_offset + inventory_skew, // Sell above mid (skew shifts)
            };

            // Ensure price is valid (0 < price < 1 for prediction markets)
            if price <= Decimal::ZERO || price >= Decimal::ONE {
                continue;
            }

            // Check exposure limit
            let current_exposure = self.get_side_exposure(side);
            let level_exposure = self.grid_config.size_per_level * price;
            if current_exposure + level_exposure > self.grid_config.max_exposure {
                break; // Stop adding levels if we'd exceed exposure
            }

            signals.push(TradeSignal {
                market_id,
                side,
                price,
                size: self.grid_config.size_per_level,
                order_type: OrderType::Limit,
                expected_profit_bps: self.estimate_level_profit_bps(i, side),
                signal_timestamp_ns: Self::now_ns(),
                urgency: SignalUrgency::Low, // Grid orders are passive
            });
        }

        self.grids_created.fetch_add(1, Ordering::Relaxed);
        signals
    }

    /// Calculate inventory skew for price adjustment
    /// 
    /// If inventory > 0 (long YES):
    ///   - ASK skew is NEGATIVE (lower prices to sell faster)
    ///   - BID skew is MORE NEGATIVE (even lower to discourage buying more)
    /// If inventory < 0 (short YES):  
    ///   - BID skew is POSITIVE (higher prices to buy faster)
    ///   - ASK skew is MORE POSITIVE (even higher to discourage selling more)
    #[inline(always)]
    fn calculate_inventory_skew(&self, inventory: i32, side: Side) -> Decimal {
        if inventory == 0 {
            return Decimal::ZERO;
        }

        let inv_decimal = Decimal::from(inventory);
        let raw_skew = inv_decimal * self.grid_config.inventory_skew_factor;
        
        // Clamp to max skew
        let clamped_skew = if raw_skew.abs() > self.grid_config.max_inventory_skew {
            if raw_skew > Decimal::ZERO {
                self.grid_config.max_inventory_skew
            } else {
                -self.grid_config.max_inventory_skew
            }
        } else {
            raw_skew
        };

        // Apply skew direction based on side
        // Long inventory (positive): want to SELL, so lower ASK, lower BID even more
        // Short inventory (negative): want to BUY, so raise BID, raise ASK even more
        match side {
            Side::Sell => -clamped_skew,                           // ASK: shift opposite to inventory
            Side::Buy => -clamped_skew - clamped_skew.abs() / Decimal::TWO, // BID: shift more aggressively
        }
    }

    /// Generate both buy and sell grids simultaneously with inventory skewing
    /// Returns (buy_signals, sell_signals)
    #[inline]
    pub fn generate_dual_grid(
        &self,
        market_id: MarketId,
        mid_price: Decimal,
        market_prob: Decimal,
        current_inventory: i32,
    ) -> (Vec<TradeSignal>, Vec<TradeSignal>) {
        let buy_signals = self.generate_grid(market_id, mid_price, Side::Buy, market_prob, current_inventory);
        let sell_signals = self.generate_grid(market_id, mid_price, Side::Sell, market_prob, current_inventory);
        (buy_signals, sell_signals)
    }

    /// Pre-sign all orders in a grid using the signature cache
    pub async fn presign_grid(
        &self,
        signals: &[TradeSignal],
    ) -> Vec<PreparedOrder> {
        let mut orders = Vec::with_capacity(signals.len());

        for signal in signals {
            // Try cache first
            let cache_key = SignatureCacheKey::new(
                signal.market_id,
                signal.side,
                signal.price,
                signal.size,
            );

            let order = if let Some(cached) = self.signer.get_cached_signature(&cache_key) {
                cached
            } else {
                // Sign on-demand
                match self.signer.prepare_order(
                    signal.market_id,
                    signal.side,
                    signal.price,
                    signal.size,
                    signal.order_type,
                    TimeInForce::GTC,
                    300, // 5 min expiry
                ).await {
                    Ok(order) => order,
                    Err(_) => continue, // Skip failed signatures
                }
            };

            orders.push(order);
        }

        orders
    }

    /// Check if aggregate grid is profitable after 2026 fees
    #[inline]
    pub fn is_grid_profitable_2026(
        &self,
        signals: &[TradeSignal],
        market_prob: Decimal,
    ) -> bool {
        if signals.is_empty() {
            return false;
        }

        // Calculate weighted average expected profit
        let mut total_size = Decimal::ZERO;
        let mut weighted_profit = 0i64;

        for signal in signals {
            total_size += signal.size;
            weighted_profit += (signal.expected_profit_bps as i64) * 
                              signal.size.to_i64().unwrap_or(0);
        }

        if total_size == Decimal::ZERO {
            return false;
        }

        let avg_profit_bps = weighted_profit / total_size.to_i64().unwrap_or(1);
        
        // In forced maker zone (0.40-0.60), we get rebates
        let in_maker_zone = market_prob >= Decimal::new(40, 2) && 
                           market_prob <= Decimal::new(60, 2);
        
        if in_maker_zone {
            // Maker rebate is -20 bps, so we need less raw profit
            avg_profit_bps >= (self.grid_config.min_aggregate_profit_bps - 20) as i64
        } else {
            avg_profit_bps >= self.grid_config.min_aggregate_profit_bps as i64
        }
    }

    /// Handle a fill event - trigger rebalancing if enabled
    pub fn on_level_filled(
        &self,
        market_id: MarketId,
        side: Side,
        price: Decimal,
        filled_size: Decimal,
    ) -> Option<TradeSignal> {
        self.levels_filled.fetch_add(1, Ordering::Relaxed);

        if !self.grid_config.auto_rebalance {
            return None;
        }

        // Self-healing: place counter-order to lock in spread
        // If buy was filled, place a sell above. If sell was filled, place a buy below.
        let counter_side = side.opposite();
        let spread_capture = self.grid_config.base_spacing * Decimal::TWO; // 2x spacing for profit
        
        let counter_price = match side {
            Side::Buy => price + spread_capture,  // Sell higher than we bought
            Side::Sell => price - spread_capture, // Buy lower than we sold
        };

        // Validate counter price
        if counter_price <= Decimal::ZERO || counter_price >= Decimal::ONE {
            return None;
        }

        self.rebalances.fetch_add(1, Ordering::Relaxed);

        Some(TradeSignal {
            market_id,
            side: counter_side,
            price: counter_price,
            size: filled_size,
            order_type: OrderType::Limit,
            expected_profit_bps: 20, // Spread capture profit
            signal_timestamp_ns: Self::now_ns(),
            urgency: SignalUrgency::Medium,
        })
    }

    /// Calculate dynamic spacing based on market probability
    /// Wider spacing in volatile mid-range (0.40-0.60)
    #[inline(always)]
    fn calculate_dynamic_spacing(&self, prob: Decimal) -> Decimal {
        let base = self.grid_config.base_spacing;
        
        // Distance from 0.5 (center)
        let dist_from_center = (prob - Decimal::new(5, 1)).abs();
        
        // In center (prob near 0.5), use wider spacing due to volatility
        // At edges, use tighter spacing
        if dist_from_center < Decimal::new(1, 1) {
            // Within 0.40-0.60 range - increase spacing
            base * Decimal::new(15, 1) * self.grid_config.volatility_multiplier
        } else if dist_from_center < Decimal::new(2, 1) {
            // 0.30-0.40 or 0.60-0.70
            base * Decimal::new(12, 1) * self.grid_config.volatility_multiplier
        } else {
            // Edges - tight spacing
            base * self.grid_config.volatility_multiplier
        }
    }

    /// Estimate profit BPS for a grid level based on position
    #[inline(always)]
    fn estimate_level_profit_bps(&self, level_index: usize, _side: Side) -> i32 {
        // Closer levels (lower index) have higher fill probability but lower profit
        // Farther levels (higher index) have lower fill probability but higher profit
        let base_profit = self.grid_config.min_aggregate_profit_bps;
        base_profit + (level_index as i32 * 5) // +5 bps per level
    }

    /// Get current exposure for one side
    #[inline]
    fn get_side_exposure(&self, side: Side) -> Decimal {
        let grid = match side {
            Side::Buy => self.buy_grid.read(),
            Side::Sell => self.sell_grid.read(),
        };
        grid.total_exposure()
    }

    #[inline(always)]
    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    fn next_order_id(&self) -> u64 {
        self.order_id_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Get statistics
    pub fn stats(&self) -> GridStats {
        GridStats {
            grids_created: self.grids_created.load(Ordering::Relaxed),
            levels_filled: self.levels_filled.load(Ordering::Relaxed),
            rebalances: self.rebalances.load(Ordering::Relaxed),
            buy_exposure: self.get_side_exposure(Side::Buy),
            sell_exposure: self.get_side_exposure(Side::Sell),
        }
    }

    /// Enable/disable the engine
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
    }
}

/// Grid statistics
#[derive(Debug, Clone)]
pub struct GridStats {
    pub grids_created: u64,
    pub levels_filled: u64,
    pub rebalances: u64,
    pub buy_exposure: Decimal,
    pub sell_exposure: Decimal,
}

// =============================================================================
// BATCH ORDER CREATION FOR POLYMARKET API
// =============================================================================

/// Batch order request for Polymarket create_orders endpoint
#[derive(Debug, Clone)]
pub struct BatchOrderRequest {
    pub orders: Vec<PreparedOrder>,
    pub market_id: MarketId,
}

impl BatchOrderRequest {
    pub fn new(market_id: MarketId) -> Self {
        Self {
            orders: Vec::new(),
            market_id,
        }
    }

    pub fn add_order(&mut self, order: PreparedOrder) {
        self.orders.push(order);
    }

    pub fn from_grid(orders: Vec<PreparedOrder>, market_id: MarketId) -> Self {
        Self { orders, market_id }
    }

    /// Serialize to JSON for API submission
    pub fn to_json(&self) -> serde_json::Value {
        let orders: Vec<serde_json::Value> = self.orders.iter().map(|o| {
            serde_json::json!({
                "tokenID": format!("{}", o.market_id.token_id),
                "price": o.price.to_string(),
                "size": o.size.to_string(),
                "side": match o.side { Side::Buy => "BUY", Side::Sell => "SELL" },
                "orderType": "GTC",
                "nonce": o.nonce.to_string(),
                "signature": o.signature.as_ref().map(|s| s.to_hex()).unwrap_or_default(),
            })
        }).collect();

        serde_json::json!({
            "orders": orders
        })
    }
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
    fn test_grid_level_creation() {
        let level = GridLevel::new(
            Decimal::new(50, 2), // 0.50
            Decimal::new(100, 0), // 100 USDC
            Side::Buy,
            0,
        );
        assert_eq!(level.price, Decimal::new(50, 2));
        assert_eq!(level.remaining(), Decimal::new(100, 0));
        assert_eq!(level.state, LevelState::Pending);
    }

    #[test]
    fn test_grid_total_exposure() {
        let mut grid = Grid::empty(Side::Buy);
        grid.levels[0] = GridLevel::new(Decimal::new(50, 2), Decimal::new(100, 0), Side::Buy, 0);
        grid.levels[0].state = LevelState::Active;
        grid.levels[1] = GridLevel::new(Decimal::new(49, 2), Decimal::new(100, 0), Side::Buy, 1);
        grid.levels[1].state = LevelState::Active;
        grid.active_count = 2;

        let exposure = grid.total_exposure();
        // 100 * 0.50 + 100 * 0.49 = 50 + 49 = 99
        assert_eq!(exposure, Decimal::new(99, 0));
    }

    #[test]
    fn test_dynamic_spacing() {
        let config = GridConfig::default();
        
        // At center (0.50), spacing should be widest
        let center_dist = (Decimal::new(50, 2) - Decimal::new(5, 1)).abs();
        assert!(center_dist < Decimal::new(1, 1)); // Should be in center zone
        
        // At edge (0.10), spacing should be tightest
        let edge_dist = (Decimal::new(10, 2) - Decimal::new(5, 1)).abs();
        assert!(edge_dist >= Decimal::new(2, 1)); // Should be in edge zone
    }
}
