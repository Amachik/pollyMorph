//! Risk management module with kill switch and position limits
//! Thread-safe, lock-free where possible for hot path performance

use crate::config::{Config, RuntimeParams};
use crate::types::{MarketId, Side, ExecutionReport, OrderStatus};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use tracing::{error, info, warn};

/// Kill switch states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum KillSwitchState {
    /// Normal operation
    Armed = 0,
    /// Triggered - all trading halted
    Triggered = 1,
    /// Manually disabled
    Disabled = 2,
}

/// Risk manager with kill switch and exposure tracking
pub struct RiskManager {
    config: Arc<Config>,
    runtime_params: Arc<RuntimeParams>,
    
    /// Kill switch state (atomic for lock-free access)
    kill_switch_triggered: AtomicBool,
    
    /// Daily P&L tracking in cents (i64 for atomic operations)
    /// Positive = profit, Negative = loss
    daily_pnl_cents: AtomicI64,
    
    /// Position tracking per market
    positions: RwLock<FxHashMap<u64, Position>>,
    
    /// Total exposure across all markets
    total_exposure_cents: AtomicI64,
    
    /// Trade counter for the day
    trade_count: AtomicI64,
    
    /// Last trade timestamp for cooldown tracking
    last_loss_timestamp_ns: AtomicI64,
    
    /// Kill switch threshold in cents (pre-computed)
    kill_threshold_cents: i64,
    
    /// Max position per market in cents
    max_position_cents: i64,
    
    /// Max total exposure in cents
    max_exposure_cents: i64,
}

/// Position in a single market
#[derive(Debug, Clone, Default)]
pub struct Position {
    pub token_id: u64,
    pub size: Decimal,           // Positive = long, Negative = short
    pub avg_entry_price: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub last_update_ns: u64,
}

impl Position {
    /// Calculate position value at current price
    #[inline(always)]
    pub fn value_at(&self, price: Decimal) -> Decimal {
        self.size.abs() * price
    }
    
    /// Update unrealized P&L
    #[inline]
    pub fn update_unrealized(&mut self, current_price: Decimal) {
        if self.size.is_zero() {
            self.unrealized_pnl = Decimal::ZERO;
            return;
        }
        
        let price_diff = current_price - self.avg_entry_price;
        self.unrealized_pnl = price_diff * self.size;
    }
}

/// Result of a risk check
#[derive(Debug, Clone, Copy)]
pub enum RiskCheckResult {
    Approved,
    RejectedKillSwitch,
    RejectedExposureLimit,
    RejectedPositionLimit,
    RejectedCooldown,
    RejectedStaleData,
    RejectedGapProtection,
}

// =============================================================================
// GLOBAL STOP-LOSS / GAP PROTECTION
// =============================================================================

/// Gap protection - detects sudden price moves and triggers emergency stop
pub struct GapProtection {
    /// Last known price per market
    last_prices: RwLock<FxHashMap<u64, Decimal>>,
    /// Gap threshold as percentage (e.g., 5.0 = 5%)
    gap_threshold_pct: Decimal,
    /// Whether gap protection has been triggered
    triggered: AtomicBool,
    /// Timestamp of last trigger
    triggered_at_ns: AtomicI64,
    /// Number of gaps detected
    gaps_detected: AtomicI64,
}

impl GapProtection {
    pub fn new(gap_threshold_pct: Decimal) -> Self {
        Self {
            last_prices: RwLock::new(FxHashMap::default()),
            gap_threshold_pct,
            triggered: AtomicBool::new(false),
            triggered_at_ns: AtomicI64::new(0),
            gaps_detected: AtomicI64::new(0),
        }
    }

    /// Check a price update for gap - returns true if gap detected
    /// This is called on every WebSocket price update
    #[inline]
    pub fn check_price_update(&self, token_id: u64, new_price: Decimal) -> bool {
        let mut prices = self.last_prices.write();
        
        if let Some(&last_price) = prices.get(&token_id) {
            if last_price > Decimal::ZERO {
                // Calculate percentage change
                let change_pct = ((new_price - last_price) / last_price * Decimal::new(100, 0)).abs();
                
                if change_pct >= self.gap_threshold_pct {
                    // GAP DETECTED!
                    self.gaps_detected.fetch_add(1, Ordering::Relaxed);
                    
                    error!(
                        token_id = token_id,
                        last_price = %last_price,
                        new_price = %new_price,
                        change_pct = %change_pct,
                        threshold = %self.gap_threshold_pct,
                        "🚨 GAP DETECTED! Price moved {}% in single tick",
                        change_pct
                    );
                    
                    // Update price anyway
                    prices.insert(token_id, new_price);
                    return true;
                }
            }
        }
        
        // Update price
        prices.insert(token_id, new_price);
        false
    }

    /// Trigger gap protection - halt all trading
    pub fn trigger(&self) {
        if !self.triggered.swap(true, Ordering::SeqCst) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64;
            self.triggered_at_ns.store(now, Ordering::SeqCst);
            
            error!("🛑 GAP PROTECTION TRIGGERED - ALL TRADING HALTED");
        }
    }

    /// Check if gap protection is triggered
    #[inline(always)]
    pub fn is_triggered(&self) -> bool {
        self.triggered.load(Ordering::Relaxed)
    }

    /// Reset gap protection (manual intervention required)
    pub fn reset(&self) -> bool {
        if self.triggered.swap(false, Ordering::SeqCst) {
            info!("✅ Gap protection reset - trading can resume");
            true
        } else {
            false
        }
    }

    /// Get statistics
    pub fn stats(&self) -> GapProtectionStats {
        GapProtectionStats {
            triggered: self.is_triggered(),
            gaps_detected: self.gaps_detected.load(Ordering::Relaxed),
            triggered_at_ns: self.triggered_at_ns.load(Ordering::Relaxed),
        }
    }
}

/// Gap protection statistics
#[derive(Debug, Clone)]
pub struct GapProtectionStats {
    pub triggered: bool,
    pub gaps_detected: i64,
    pub triggered_at_ns: i64,
}

// =============================================================================
// VOLUME CIRCUIT BREAKER - Toxic Flow Protection
// =============================================================================

/// Volume circuit breaker - detects volume spikes indicating breaking news or toxic flow
/// Uses same halt logic as GapProtection
pub struct VolumeCircuitBreaker {
    /// Volume spike multiplier threshold (e.g., 10.0 = 10x average triggers halt)
    spike_multiplier: f64,
    /// Ring buffer of 1-second volume buckets (USDC)
    volume_buckets: parking_lot::Mutex<[f64; 300]>,
    /// Current bucket index
    current_bucket: AtomicI64,
    /// Current bucket timestamp (seconds)
    current_bucket_timestamp: AtomicI64,
    /// Running sum for fast MA
    running_sum: parking_lot::Mutex<f64>,
    /// Buckets filled count
    buckets_filled: AtomicI64,
    /// Circuit breaker triggered
    triggered: AtomicBool,
    /// Timestamp when triggered
    triggered_at_ns: AtomicI64,
    /// Number of spikes detected
    spikes_detected: AtomicI64,
}

impl VolumeCircuitBreaker {
    pub fn new(spike_multiplier: f64) -> Self {
        Self {
            spike_multiplier,
            volume_buckets: parking_lot::Mutex::new([0.0; 300]),
            current_bucket: AtomicI64::new(0),
            current_bucket_timestamp: AtomicI64::new(Self::current_second() as i64),
            running_sum: parking_lot::Mutex::new(0.0),
            buckets_filled: AtomicI64::new(0),
            triggered: AtomicBool::new(false),
            triggered_at_ns: AtomicI64::new(0),
            spikes_detected: AtomicI64::new(0),
        }
    }

    /// Record trade volume - returns true if circuit breaker should trigger
    /// This is called from websocket on every trade update
    pub fn record_trade_volume(&self, volume_usdc: Decimal) -> bool {
        let volume_f64 = volume_usdc.to_f64().unwrap_or(0.0);
        
        let now_sec = Self::current_second() as i64;
        let last_sec = self.current_bucket_timestamp.load(Ordering::Relaxed);
        
        let mut buckets = self.volume_buckets.lock();
        let mut running_sum = self.running_sum.lock();
        
        // Advance buckets if needed
        let elapsed = (now_sec - last_sec).max(0) as usize;
        if elapsed > 0 {
            let current = self.current_bucket.load(Ordering::Relaxed) as usize;
            let buckets_to_clear = elapsed.min(300);
            
            for i in 0..buckets_to_clear {
                let idx = (current + 1 + i) % 300;
                *running_sum -= buckets[idx];
                *running_sum = running_sum.max(0.0);
                buckets[idx] = 0.0;
            }
            
            let new_bucket = (current + buckets_to_clear) % 300;
            self.current_bucket.store(new_bucket as i64, Ordering::Relaxed);
            self.current_bucket_timestamp.store(now_sec, Ordering::Relaxed);
            
            let filled = self.buckets_filled.load(Ordering::Relaxed);
            self.buckets_filled.store((filled + buckets_to_clear as i64).min(300), Ordering::Relaxed);
        }
        
        // Add volume to current bucket
        let bucket_idx = self.current_bucket.load(Ordering::Relaxed) as usize;
        buckets[bucket_idx] += volume_f64;
        *running_sum += volume_f64;
        
        // Check for spike
        let filled = self.buckets_filled.load(Ordering::Relaxed);
        if filled < 60 {
            return false; // Need at least 1 minute of data
        }
        
        let current_1s = buckets[bucket_idx];
        let avg_per_sec = *running_sum / filled as f64;
        
        if avg_per_sec > 0.0 && current_1s > avg_per_sec * self.spike_multiplier {
            self.spikes_detected.fetch_add(1, Ordering::Relaxed);
            
            if !self.triggered.swap(true, Ordering::SeqCst) {
                self.triggered_at_ns.store(Self::now_ns() as i64, Ordering::SeqCst);
                
                error!(
                    current_1s_volume = current_1s,
                    avg_5m_volume = avg_per_sec,
                    spike_ratio = current_1s / avg_per_sec,
                    threshold = self.spike_multiplier,
                    "🚨 VOLUME SPIKE CIRCUIT BREAKER TRIGGERED! {}x average volume detected",
                    current_1s / avg_per_sec
                );
            }
            return true;
        }
        
        false
    }

    /// Check if circuit breaker is triggered
    #[inline(always)]
    pub fn is_triggered(&self) -> bool {
        self.triggered.load(Ordering::Relaxed)
    }

    /// Reset the circuit breaker (manual intervention required)
    pub fn reset(&self) -> bool {
        if self.triggered.swap(false, Ordering::SeqCst) {
            info!("✅ Volume circuit breaker reset - trading can resume");
            true
        } else {
            false
        }
    }

    /// Get statistics
    pub fn stats(&self) -> VolumeCircuitBreakerStats {
        let buckets = self.volume_buckets.lock();
        let bucket_idx = self.current_bucket.load(Ordering::Relaxed) as usize;
        let running_sum = *self.running_sum.lock();
        let filled = self.buckets_filled.load(Ordering::Relaxed);
        
        VolumeCircuitBreakerStats {
            triggered: self.is_triggered(),
            spikes_detected: self.spikes_detected.load(Ordering::Relaxed),
            current_1s_volume: buckets[bucket_idx],
            avg_5m_per_sec: if filled > 0 { running_sum / filled as f64 } else { 0.0 },
            total_5m_volume: running_sum,
            triggered_at_ns: self.triggered_at_ns.load(Ordering::Relaxed),
        }
    }

    fn current_second() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}

/// Volume circuit breaker statistics
#[derive(Debug, Clone)]
pub struct VolumeCircuitBreakerStats {
    pub triggered: bool,
    pub spikes_detected: i64,
    pub current_1s_volume: f64,
    pub avg_5m_per_sec: f64,
    pub total_5m_volume: f64,
    pub triggered_at_ns: i64,
}

impl RiskManager {
    pub fn new(config: Arc<Config>, runtime_params: Arc<RuntimeParams>) -> Self {
        // Pre-compute thresholds in cents for atomic operations
        let kill_threshold_cents = (config.kill_switch_amount() * Decimal::new(100, 0))
            .to_i64()
            .unwrap_or(5000_00); // $5000 default
        
        let max_position_cents = (config.risk.max_position_per_market * Decimal::new(100, 0))
            .to_i64()
            .unwrap_or(10000_00);
        
        let max_exposure_cents = (config.risk.max_total_exposure * Decimal::new(100, 0))
            .to_i64()
            .unwrap_or(50000_00);
        
        info!(
            "RiskManager initialized: kill_threshold=${}, max_position=${}, max_exposure=${}",
            kill_threshold_cents / 100,
            max_position_cents / 100,
            max_exposure_cents / 100
        );
        
        Self {
            config,
            runtime_params,
            kill_switch_triggered: AtomicBool::new(false),
            daily_pnl_cents: AtomicI64::new(0),
            positions: RwLock::new(FxHashMap::default()),
            total_exposure_cents: AtomicI64::new(0),
            trade_count: AtomicI64::new(0),
            last_loss_timestamp_ns: AtomicI64::new(0),
            kill_threshold_cents,
            max_position_cents,
            max_exposure_cents,
        }
    }
    
    /// Override risk limits for ARB_MODE where capital management is handled by the arb engine.
    /// Sets exposure and position limits high enough to not interfere with arb orders.
    pub fn set_arb_mode_limits(&mut self, capital_usdc: f64) {
        let capital_cents = (capital_usdc * 100.0) as i64;
        self.max_exposure_cents = capital_cents;
        self.max_position_cents = capital_cents;
        info!(
            "RiskManager ARB_MODE: max_exposure=${}, max_position=${}",
            capital_cents / 100, capital_cents / 100
        );
    }

    /// Check if trading is allowed - HOT PATH
    /// Must be extremely fast as called before every trade
    #[inline(always)]
    pub fn is_trading_allowed(&self) -> bool {
        // Fast path: check atomics first
        if self.kill_switch_triggered.load(Ordering::Relaxed) {
            return false;
        }
        
        if !self.runtime_params.is_trading_enabled() {
            return false;
        }
        
        true
    }
    
    /// Full pre-trade risk check - HOT PATH
    #[inline]
    pub fn check_order(
        &self,
        market_id: MarketId,
        side: Side,
        size: Decimal,
        price: Decimal,
    ) -> RiskCheckResult {
        // 1. Kill switch check (fastest)
        if self.kill_switch_triggered.load(Ordering::Relaxed) {
            return RiskCheckResult::RejectedKillSwitch;
        }
        
        // 2. Cooldown check after losses
        if self.is_in_cooldown() {
            return RiskCheckResult::RejectedCooldown;
        }
        
        // 3. Calculate order value
        let order_value_cents = (size * price * Decimal::new(100, 0))
            .to_i64()
            .unwrap_or(0);
        
        // 4. Check total exposure limit
        let current_exposure = self.total_exposure_cents.load(Ordering::Relaxed);
        if current_exposure + order_value_cents > self.max_exposure_cents {
            return RiskCheckResult::RejectedExposureLimit;
        }
        
        // 5. Check per-market position limit
        {
            let positions = self.positions.read();
            if let Some(pos) = positions.get(&market_id.token_id) {
                let current_position_cents = (pos.size.abs() * pos.avg_entry_price * Decimal::new(100, 0))
                    .to_i64()
                    .unwrap_or(0);
                
                // Check if adding this order would exceed limit
                let new_position_cents = match side {
                    Side::Buy if pos.size >= Decimal::ZERO => current_position_cents + order_value_cents,
                    Side::Sell if pos.size <= Decimal::ZERO => current_position_cents + order_value_cents,
                    _ => (current_position_cents - order_value_cents).abs(), // Reducing position
                };
                
                if new_position_cents > self.max_position_cents {
                    return RiskCheckResult::RejectedPositionLimit;
                }
            } else if order_value_cents > self.max_position_cents {
                return RiskCheckResult::RejectedPositionLimit;
            }
        }
        
        RiskCheckResult::Approved
    }
    
    /// Check if in cooldown period after a loss
    #[inline(always)]
    fn is_in_cooldown(&self) -> bool {
        let last_loss = self.last_loss_timestamp_ns.load(Ordering::Relaxed);
        if last_loss == 0 {
            return false;
        }
        
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        
        let cooldown_ns = (self.config.risk.loss_cooldown_ms * 1_000_000) as i64;
        
        now - last_loss < cooldown_ns
    }
    
    /// Record a completed trade and update P&L
    pub fn record_trade(&self, report: &ExecutionReport) {
        if report.status != OrderStatus::Filled && report.status != OrderStatus::PartiallyFilled {
            return;
        }
        
        self.trade_count.fetch_add(1, Ordering::Relaxed);
        
        let filled_value_cents = (report.filled_size * report.price * Decimal::new(100, 0))
            .to_i64()
            .unwrap_or(0);
        
        // Update position
        {
            let mut positions = self.positions.write();
            let pos = positions.entry(report.market_id.token_id).or_default();
            
            let old_size = pos.size;
            let new_size = match report.side {
                Side::Buy => old_size + report.filled_size,
                Side::Sell => old_size - report.filled_size,
            };
            
            // Calculate realized P&L if reducing position
            if (old_size > Decimal::ZERO && report.side == Side::Sell) ||
               (old_size < Decimal::ZERO && report.side == Side::Buy) {
                let closed_size = report.filled_size.min(old_size.abs());
                let pnl = match report.side {
                    Side::Sell => (report.price - pos.avg_entry_price) * closed_size,
                    Side::Buy => (pos.avg_entry_price - report.price) * closed_size,
                };
                pos.realized_pnl += pnl;
                
                // Update daily P&L
                let pnl_cents = (pnl * Decimal::new(100, 0))
                    .to_i64()
                    .unwrap_or(0);
                
                self.daily_pnl_cents.fetch_add(pnl_cents, Ordering::Relaxed);
                
                // Check if this was a loss and set cooldown
                if pnl_cents < 0 {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as i64;
                    self.last_loss_timestamp_ns.store(now, Ordering::Relaxed);
                }
            }
            
            // Update average entry price for new position
            if new_size.abs() > old_size.abs() {
                // Adding to position - calculate new average
                let old_value = old_size.abs() * pos.avg_entry_price;
                let new_value = report.filled_size * report.price;
                pos.avg_entry_price = (old_value + new_value) / new_size.abs();
            }
            
            pos.size = new_size;
            pos.token_id = report.market_id.token_id;
            pos.last_update_ns = report.last_update_ns;
        }
        
        // Update total exposure
        self.recalculate_exposure();
        
        // Check kill switch
        self.check_kill_switch();
    }
    
    /// Recalculate total exposure from all positions
    fn recalculate_exposure(&self) {
        let positions = self.positions.read();
        let total: i64 = positions.values()
            .map(|p| {
                (p.size.abs() * p.avg_entry_price * Decimal::new(100, 0))
                    .trunc()
                    .to_i64()
                    .unwrap_or(0)
            })
            .sum();
        
        self.total_exposure_cents.store(total, Ordering::Relaxed);
    }
    
    /// Check and trigger kill switch if necessary
    fn check_kill_switch(&self) {
        let pnl = self.daily_pnl_cents.load(Ordering::Relaxed);
        
        // Trigger if loss exceeds threshold (pnl is negative)
        if pnl < -self.kill_threshold_cents {
            self.trigger_kill_switch("Daily loss threshold exceeded");
        }
    }
    
    /// Manually trigger the kill switch
    pub fn trigger_kill_switch(&self, reason: &str) {
        if self.kill_switch_triggered.swap(true, Ordering::SeqCst) {
            return; // Already triggered
        }
        
        error!("🚨 KILL SWITCH TRIGGERED: {}", reason);
        
        // Disable trading
        self.runtime_params.disable_trading();
        
        // Log current state
        let pnl = self.daily_pnl_cents.load(Ordering::Relaxed) as f64 / 100.0;
        let exposure = self.total_exposure_cents.load(Ordering::Relaxed) as f64 / 100.0;
        let trades = self.trade_count.load(Ordering::Relaxed);
        
        error!(
            "Kill switch state: P&L=${:.2}, Exposure=${:.2}, Trades={}",
            pnl, exposure, trades
        );
    }
    
    /// Reset kill switch (requires manual intervention)
    pub fn reset_kill_switch(&self) -> bool {
        if !self.kill_switch_triggered.load(Ordering::Relaxed) {
            return true;
        }
        
        // Only reset if P&L has improved
        let pnl = self.daily_pnl_cents.load(Ordering::Relaxed);
        if pnl < -self.kill_threshold_cents / 2 {
            warn!("Cannot reset kill switch: P&L still below safe threshold");
            return false;
        }
        
        self.kill_switch_triggered.store(false, Ordering::SeqCst);
        info!("Kill switch reset");
        true
    }
    
    /// Reset daily counters (call at start of trading day)
    pub fn reset_daily(&self) {
        self.daily_pnl_cents.store(0, Ordering::Relaxed);
        self.trade_count.store(0, Ordering::Relaxed);
        self.last_loss_timestamp_ns.store(0, Ordering::Relaxed);
        
        // Don't auto-reset kill switch
        if self.kill_switch_triggered.load(Ordering::Relaxed) {
            warn!("Daily reset: kill switch remains triggered, manual reset required");
        } else {
            info!("Daily reset complete");
        }
    }
    
    /// Record a trade for Shadow Mode (increments trade count and simulates P&L)
    pub fn record_shadow_trade(&self, price: Decimal, size: Decimal, is_buy: bool) {
        self.trade_count.fetch_add(1, Ordering::Relaxed);
        
        // Calculate trade value in cents (price * size * 100)
        // Use trunc() to get integer cents, then convert properly
        let trade_value = price * size * Decimal::new(100, 0);
        let trade_value_cents = trade_value.trunc().mantissa() as i64;
        
        // Simulate small P&L for Shadow Mode (0.5% of trade value)
        let simulated_pnl = trade_value_cents / 200;
        
        self.daily_pnl_cents.fetch_add(simulated_pnl, Ordering::Relaxed);
        self.total_exposure_cents.fetch_add(trade_value_cents.abs(), Ordering::Relaxed);
    }
    
    /// Get current risk metrics
    pub fn get_metrics(&self) -> RiskMetrics {
        RiskMetrics {
            daily_pnl: Decimal::new(self.daily_pnl_cents.load(Ordering::Relaxed), 2),
            total_exposure: Decimal::new(self.total_exposure_cents.load(Ordering::Relaxed), 2),
            trade_count: self.trade_count.load(Ordering::Relaxed) as u64,
            kill_switch_triggered: self.kill_switch_triggered.load(Ordering::Relaxed),
            position_count: self.positions.read().len(),
        }
    }
    
    /// Update position with current market price for unrealized P&L
    pub fn mark_to_market(&self, token_id: u64, current_price: Decimal) {
        let mut positions = self.positions.write();
        if let Some(pos) = positions.get_mut(&token_id) {
            pos.update_unrealized(current_price);
        }
    }
}

/// Risk metrics snapshot
#[derive(Debug, Clone)]
pub struct RiskMetrics {
    pub daily_pnl: Decimal,
    pub total_exposure: Decimal,
    pub trade_count: u64,
    pub kill_switch_triggered: bool,
    pub position_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_kill_switch_threshold() {
        let config = Arc::new(Config::load_with_defaults());
        let params = Arc::new(RuntimeParams::new(&config));
        let risk = RiskManager::new(config.clone(), params);
        
        // Should allow trading initially
        assert!(risk.is_trading_allowed());
        
        // Simulate loss exceeding threshold
        let loss = -config.kill_switch_amount() * Decimal::new(100, 0) - Decimal::ONE;
        risk.daily_pnl_cents.store(
            loss.to_i64().unwrap(),
            Ordering::Relaxed
        );
        
        risk.check_kill_switch();
        
        // Kill switch should be triggered
        assert!(!risk.is_trading_allowed());
    }
}
