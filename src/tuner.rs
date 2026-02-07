//! Auto-Tuner for Shadow Mode Parameter Optimization
//!
//! Runs parallel virtual environments to find optimal grid_spacing,
//! automatically adjusts skew based on inventory health, and triggers
//! notifications when strategy is stable.
//!
//! Features:
//! - 60-minute optimization cycles testing multiple grid_spacing values
//! - Inventory delta detection with auto-skew adjustment
//! - Profitability tracking for go-live readiness
//! - Slack/Discord webhook notifications

use crate::config::Config;
use crate::grid::GridConfig;
use crate::shadow::{ShadowEngine, ShadowStats};
use crate::types::{MarketId, Side, TradeSignal};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn, error};

/// Default grid spacing values to test (in probability units)
pub const DEFAULT_SPACING_VALUES: [f64; 3] = [0.005, 0.01, 0.02];

/// Optimization cycle interval (60 minutes)
pub const OPTIMIZATION_INTERVAL_SECS: u64 = 3600;

/// Profitability check interval (1 hour segments)
pub const PROFITABILITY_CHECK_SECS: u64 = 3600;

/// Hours of consecutive profit needed for "ready for live" signal
pub const CONSECUTIVE_PROFIT_HOURS: u64 = 4;

/// Inventory delta threshold for skew adjustment (20%)
pub const INVENTORY_DELTA_THRESHOLD: f64 = 0.20;

/// Skew increase factor when inventory is imbalanced (50% increase)
pub const SKEW_INCREASE_FACTOR: f64 = 1.5;

/// Virtual environment for testing a specific parameter set
pub struct VirtualEnvironment {
    pub id: u32,
    pub grid_spacing: Decimal,
    pub shadow_engine: ShadowEngine,
    pub start_balance: Decimal,
    pub start_time: Instant,
}

impl VirtualEnvironment {
    pub fn new(id: u32, config: Arc<Config>, grid_spacing: Decimal) -> Self {
        let shadow_engine = ShadowEngine::new(config);
        let start_balance = shadow_engine.get_balance();
        
        Self {
            id,
            grid_spacing,
            shadow_engine,
            start_balance,
            start_time: Instant::now(),
        }
    }

    /// Get PnL since start
    pub fn get_pnl(&self) -> Decimal {
        self.shadow_engine.get_balance() - self.start_balance
    }

    /// Get stats
    pub fn get_stats(&self) -> ShadowStats {
        self.shadow_engine.get_stats()
    }

    /// Reset for new optimization cycle
    pub fn reset(&mut self) {
        self.shadow_engine.reset();
        self.start_balance = self.shadow_engine.get_balance();
        self.start_time = Instant::now();
    }
}

/// Optimization result for a parameter set
#[derive(Debug, Clone)]
pub struct OptimizationResult {
    pub grid_spacing: Decimal,
    pub pnl: Decimal,
    pub total_trades: u64,
    pub win_rate: f64,
    pub maker_ratio: f64,
    pub sharpe_estimate: f64,
}

/// Inventory health status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InventoryHealth {
    Balanced,
    LongBiased,
    ShortBiased,
    Critical,
}

impl InventoryHealth {
    pub fn as_str(&self) -> &'static str {
        match self {
            InventoryHealth::Balanced => "balanced",
            InventoryHealth::LongBiased => "long_biased",
            InventoryHealth::ShortBiased => "short_biased",
            InventoryHealth::Critical => "critical",
        }
    }
}

/// Profitability tracking for go-live readiness
#[derive(Debug)]
pub struct ProfitabilityTracker {
    /// Hourly PnL snapshots
    hourly_pnl: Vec<Decimal>,
    /// Last snapshot time
    last_snapshot: Instant,
    /// Consecutive profitable hours
    consecutive_profit_hours: u64,
    /// Has triggered notification
    notification_sent: AtomicBool,
}

impl ProfitabilityTracker {
    pub fn new() -> Self {
        Self {
            hourly_pnl: Vec::new(),
            last_snapshot: Instant::now(),
            consecutive_profit_hours: 0,
            notification_sent: AtomicBool::new(false),
        }
    }

    /// Record an hourly PnL snapshot
    pub fn record_snapshot(&mut self, pnl: Decimal) {
        self.hourly_pnl.push(pnl);
        self.last_snapshot = Instant::now();

        // Check consecutive profitability
        if self.hourly_pnl.len() >= 2 {
            let last_idx = self.hourly_pnl.len() - 1;
            let hourly_change = self.hourly_pnl[last_idx] - self.hourly_pnl[last_idx - 1];
            
            if hourly_change > Decimal::ZERO {
                self.consecutive_profit_hours += 1;
            } else {
                self.consecutive_profit_hours = 0;
                self.notification_sent.store(false, Ordering::SeqCst);
            }
        }
    }

    /// Check if ready for live (4 consecutive profitable hours)
    pub fn is_ready_for_live(&self) -> bool {
        self.consecutive_profit_hours >= CONSECUTIVE_PROFIT_HOURS
    }

    /// Check if notification should be sent
    pub fn should_send_notification(&self) -> bool {
        self.is_ready_for_live() && !self.notification_sent.load(Ordering::Relaxed)
    }

    /// Mark notification as sent
    pub fn mark_notification_sent(&self) {
        self.notification_sent.store(true, Ordering::SeqCst);
    }

    /// Get consecutive profitable hours
    pub fn consecutive_hours(&self) -> u64 {
        self.consecutive_profit_hours
    }
}

impl Default for ProfitabilityTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Auto-Tuner configuration
#[derive(Debug, Clone)]
pub struct TunerConfig {
    /// Grid spacing values to test
    pub spacing_values: Vec<Decimal>,
    /// Optimization interval in seconds
    pub optimization_interval_secs: u64,
    /// Inventory delta threshold for skew adjustment
    pub inventory_delta_threshold: Decimal,
    /// Skew increase factor
    pub skew_increase_factor: Decimal,
    /// Slack webhook URL (optional)
    pub slack_webhook_url: Option<String>,
    /// Discord webhook URL (optional)
    pub discord_webhook_url: Option<String>,
}

impl Default for TunerConfig {
    fn default() -> Self {
        Self {
            spacing_values: DEFAULT_SPACING_VALUES
                .iter()
                .map(|&v| Decimal::from_f64_retain(v).unwrap_or(Decimal::new(1, 2)))
                .collect(),
            optimization_interval_secs: OPTIMIZATION_INTERVAL_SECS,
            inventory_delta_threshold: Decimal::from_f64_retain(INVENTORY_DELTA_THRESHOLD)
                .unwrap_or(Decimal::new(20, 2)),
            skew_increase_factor: Decimal::from_f64_retain(SKEW_INCREASE_FACTOR)
                .unwrap_or(Decimal::new(15, 1)),
            slack_webhook_url: None,
            discord_webhook_url: None,
        }
    }
}

/// Auto-Tuner for Shadow Mode
pub struct AutoTuner {
    config: Arc<Config>,
    tuner_config: TunerConfig,
    
    /// Virtual environments for parallel testing
    environments: RwLock<Vec<VirtualEnvironment>>,
    
    /// Current best grid spacing
    best_spacing: RwLock<Decimal>,
    
    /// Optimization results history
    results_history: RwLock<Vec<OptimizationResult>>,
    
    /// Profitability tracker
    profitability_tracker: RwLock<ProfitabilityTracker>,
    
    /// Current inventory per market (token_id -> inventory)
    inventory: RwLock<HashMap<u64, i32>>,
    
    /// Current ask skew multiplier
    ask_skew_multiplier: RwLock<Decimal>,
    
    /// Tuner enabled
    enabled: AtomicBool,
    
    /// Last optimization time
    last_optimization_ns: AtomicU64,
    
    /// Optimization cycles completed
    cycles_completed: AtomicU64,
    
    /// HTTP client for webhooks
    http_client: reqwest::Client,
}

impl AutoTuner {
    pub fn new(config: Arc<Config>, tuner_config: TunerConfig) -> Self {
        // Create virtual environments for each spacing value
        let environments: Vec<VirtualEnvironment> = tuner_config
            .spacing_values
            .iter()
            .enumerate()
            .map(|(i, &spacing)| VirtualEnvironment::new(i as u32, config.clone(), spacing))
            .collect();

        let best_spacing = tuner_config
            .spacing_values
            .first()
            .copied()
            .unwrap_or(Decimal::new(1, 2));

        Self {
            config,
            tuner_config,
            environments: RwLock::new(environments),
            best_spacing: RwLock::new(best_spacing),
            results_history: RwLock::new(Vec::new()),
            profitability_tracker: RwLock::new(ProfitabilityTracker::new()),
            inventory: RwLock::new(HashMap::new()),
            ask_skew_multiplier: RwLock::new(Decimal::ONE),
            enabled: AtomicBool::new(true),
            last_optimization_ns: AtomicU64::new(0),
            cycles_completed: AtomicU64::new(0),
            http_client: reqwest::Client::new(),
        }
    }

    /// Process a trade signal through all virtual environments
    pub fn process_signal(&self, signal: &TradeSignal, is_maker: bool) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }

        let envs = self.environments.read();
        for env in envs.iter() {
            let _ = env.shadow_engine.track_virtual_order(signal, is_maker);
        }
    }

    /// Process a trade update through all virtual environments
    pub fn process_trade_update(
        &self,
        market_id: &MarketId,
        trade_side: Side,
        trade_price: Decimal,
        trade_size: Decimal,
        market_probability: Decimal,
    ) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }

        let envs = self.environments.read();
        for env in envs.iter() {
            env.shadow_engine.process_trade_update(
                market_id,
                trade_side,
                trade_price,
                trade_size,
                market_probability,
            );
        }
    }

    /// Update inventory for a market
    pub fn update_inventory(&self, token_id: u64, delta: i32) {
        let mut inv = self.inventory.write();
        let current = inv.entry(token_id).or_insert(0);
        *current += delta;

        // Check if skew adjustment needed
        self.check_inventory_health(token_id, *current);
    }

    /// Check inventory health and adjust skew if needed
    fn check_inventory_health(&self, token_id: u64, inventory: i32) {
        let health = self.calculate_inventory_health(inventory);
        
        // Update metrics
        crate::metrics::update_inventory_health(token_id, health.as_str(), inventory as f64);

        // Auto-adjust skew if inventory is imbalanced
        if matches!(health, InventoryHealth::LongBiased | InventoryHealth::Critical) && inventory > 0 {
            // Long biased - increase ask skew to sell faster
            let new_multiplier = self.tuner_config.skew_increase_factor;
            *self.ask_skew_multiplier.write() = new_multiplier;
            
            info!(
                token_id = token_id,
                inventory = inventory,
                health = health.as_str(),
                new_skew_multiplier = %new_multiplier,
                "📊 Auto-Tuner: Increasing ask skew to reduce long inventory"
            );
        } else if matches!(health, InventoryHealth::Balanced) {
            // Reset skew multiplier
            *self.ask_skew_multiplier.write() = Decimal::ONE;
        }
    }

    /// Calculate inventory health based on delta
    fn calculate_inventory_health(&self, inventory: i32) -> InventoryHealth {
        let abs_inventory = inventory.abs() as f64;
        let threshold = self.tuner_config.inventory_delta_threshold
            .to_string()
            .parse::<f64>()
            .unwrap_or(0.20) * 100.0; // Convert to percentage base

        if abs_inventory < threshold / 2.0 {
            InventoryHealth::Balanced
        } else if abs_inventory < threshold {
            if inventory > 0 {
                InventoryHealth::LongBiased
            } else {
                InventoryHealth::ShortBiased
            }
        } else {
            InventoryHealth::Critical
        }
    }

    /// Get current ask skew multiplier
    pub fn get_ask_skew_multiplier(&self) -> Decimal {
        *self.ask_skew_multiplier.read()
    }

    /// Run optimization cycle - compare all virtual environments
    pub async fn run_optimization_cycle(&self) -> Option<OptimizationResult> {
        if !self.enabled.load(Ordering::Relaxed) {
            return None;
        }

        let envs = self.environments.read();
        let mut results: Vec<OptimizationResult> = Vec::with_capacity(envs.len());

        // Collect results from all environments
        for env in envs.iter() {
            let stats = env.get_stats();
            let pnl = env.get_pnl();
            
            let win_rate = if stats.total_trades > 0 {
                stats.winning_trades as f64 / stats.total_trades as f64
            } else {
                0.0
            };

            let maker_ratio = {
                let total = stats.maker_fills + stats.taker_fills;
                if total > 0 {
                    stats.maker_fills as f64 / total as f64
                } else {
                    0.0
                }
            };

            // Simple Sharpe estimate (PnL / std dev proxy)
            let sharpe_estimate = if stats.total_trades > 0 {
                pnl.to_f64().unwrap_or(0.0) / (stats.total_trades as f64).sqrt()
            } else {
                0.0
            };

            results.push(OptimizationResult {
                grid_spacing: env.grid_spacing,
                pnl,
                total_trades: stats.total_trades,
                win_rate,
                maker_ratio,
                sharpe_estimate,
            });
        }
        drop(envs);

        // Find best performing spacing
        let best = results.iter().max_by(|a, b| {
            a.pnl.partial_cmp(&b.pnl).unwrap_or(std::cmp::Ordering::Equal)
        })?;

        // Update best spacing
        *self.best_spacing.write() = best.grid_spacing;

        // Log results
        info!(
            "📈 Auto-Tuner Optimization Cycle Complete:"
        );
        for result in &results {
            info!(
                "  spacing={}: PnL={}, trades={}, win_rate={:.1}%, maker_ratio={:.1}%",
                result.grid_spacing,
                result.pnl,
                result.total_trades,
                result.win_rate * 100.0,
                result.maker_ratio * 100.0
            );
        }
        info!(
            "  🏆 Best spacing: {} (PnL: {})",
            best.grid_spacing, best.pnl
        );

        // Store in history
        self.results_history.write().push(best.clone());

        // Update metrics
        crate::metrics::update_tuner_metrics(
            best.grid_spacing.to_f64().unwrap_or(0.01),
            best.pnl.to_f64().unwrap_or(0.0),
            self.cycles_completed.load(Ordering::Relaxed),
        );

        // Increment cycle counter
        self.cycles_completed.fetch_add(1, Ordering::Relaxed);
        self.last_optimization_ns.store(Self::now_ns(), Ordering::Relaxed);

        // Reset environments for next cycle
        let mut envs = self.environments.write();
        for env in envs.iter_mut() {
            env.reset();
        }

        Some(best.clone())
    }

    /// Check profitability and send notification if ready
    pub async fn check_profitability_and_notify(&self, current_pnl: Decimal) {
        let mut tracker = self.profitability_tracker.write();
        tracker.record_snapshot(current_pnl);

        if tracker.should_send_notification() {
            let pnl_str = current_pnl.to_string();
            let message = format!(
                "🎯 PollyMorph: Strategy Stable - Ready for Live?\n\
                 📊 PnL: +{} USDC\n\
                 ⏱️ Consecutive Profitable Hours: {}\n\
                 🔧 Best Grid Spacing: {}",
                pnl_str,
                tracker.consecutive_hours(),
                self.best_spacing.read()
            );

            // Send notifications
            self.send_notification(&message).await;
            tracker.mark_notification_sent();
        }
    }

    /// Send notification to Slack/Discord
    async fn send_notification(&self, message: &str) {
        // Slack webhook
        if let Some(ref url) = self.tuner_config.slack_webhook_url {
            let payload = serde_json::json!({
                "text": message
            });

            match self.http_client.post(url).json(&payload).send().await {
                Ok(_) => info!("📨 Sent Slack notification"),
                Err(e) => warn!("Failed to send Slack notification: {}", e),
            }
        }

        // Discord webhook
        if let Some(ref url) = self.tuner_config.discord_webhook_url {
            let payload = serde_json::json!({
                "content": message
            });

            match self.http_client.post(url).json(&payload).send().await {
                Ok(_) => info!("📨 Sent Discord notification"),
                Err(e) => warn!("Failed to send Discord notification: {}", e),
            }
        }

        // Always log
        info!("{}", message);
    }

    /// Get current best grid spacing
    pub fn get_best_spacing(&self) -> Decimal {
        *self.best_spacing.read()
    }

    /// Get tuner statistics
    pub fn get_stats(&self) -> TunerStats {
        let envs = self.environments.read();
        let best_env = envs.iter().max_by(|a, b| {
            a.get_pnl().partial_cmp(&b.get_pnl()).unwrap_or(std::cmp::Ordering::Equal)
        });

        TunerStats {
            cycles_completed: self.cycles_completed.load(Ordering::Relaxed),
            best_spacing: *self.best_spacing.read(),
            current_ask_skew: *self.ask_skew_multiplier.read(),
            environments_count: envs.len(),
            best_env_pnl: best_env.map(|e| e.get_pnl()).unwrap_or(Decimal::ZERO),
            consecutive_profit_hours: self.profitability_tracker.read().consecutive_hours(),
        }
    }

    /// Enable/disable the tuner
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
    }

    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}

/// Tuner statistics
#[derive(Debug, Clone)]
pub struct TunerStats {
    pub cycles_completed: u64,
    pub best_spacing: Decimal,
    pub current_ask_skew: Decimal,
    pub environments_count: usize,
    pub best_env_pnl: Decimal,
    pub consecutive_profit_hours: u64,
}

// =============================================================================
// BACKGROUND TUNER TASK
// =============================================================================

/// Run the auto-tuner as a background task
pub async fn run_tuner_loop(tuner: Arc<AutoTuner>, mut shutdown_rx: mpsc::Receiver<()>) {
    let optimization_interval = Duration::from_secs(tuner.tuner_config.optimization_interval_secs);
    let profitability_interval = Duration::from_secs(PROFITABILITY_CHECK_SECS);
    
    let mut optimization_timer = tokio::time::interval(optimization_interval);
    let mut profitability_timer = tokio::time::interval(profitability_interval);

    info!("🔧 Auto-Tuner started with {} virtual environments", 
          tuner.tuner_config.spacing_values.len());

    loop {
        tokio::select! {
            _ = optimization_timer.tick() => {
                if let Some(result) = tuner.run_optimization_cycle().await {
                    info!(
                        "Optimization cycle complete: best_spacing={}, pnl={}",
                        result.grid_spacing, result.pnl
                    );
                }
            }
            _ = profitability_timer.tick() => {
                // Get current PnL from best environment
                let stats = tuner.get_stats();
                tuner.check_profitability_and_notify(stats.best_env_pnl).await;
            }
            _ = shutdown_rx.recv() => {
                info!("🛑 Auto-Tuner shutting down");
                break;
            }
        }
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inventory_health_calculation() {
        let config = Arc::new(Config::load_with_defaults());
        let tuner = AutoTuner::new(config, TunerConfig::default());

        assert_eq!(tuner.calculate_inventory_health(0), InventoryHealth::Balanced);
        assert_eq!(tuner.calculate_inventory_health(5), InventoryHealth::Balanced);
        assert_eq!(tuner.calculate_inventory_health(15), InventoryHealth::LongBiased);
        assert_eq!(tuner.calculate_inventory_health(-15), InventoryHealth::ShortBiased);
        assert_eq!(tuner.calculate_inventory_health(25), InventoryHealth::Critical);
    }

    #[test]
    fn test_profitability_tracker() {
        let mut tracker = ProfitabilityTracker::new();
        
        // Record 4 profitable hours
        for i in 1..=5 {
            tracker.record_snapshot(Decimal::new(i * 100, 0));
        }

        assert!(tracker.is_ready_for_live());
        assert!(tracker.should_send_notification());
        
        tracker.mark_notification_sent();
        assert!(!tracker.should_send_notification());
    }
}
