//! Configuration module for HFT bot settings
//! Handles API keys, fee parameters, risk thresholds, and runtime configuration

use rust_decimal::Decimal;
use serde::Deserialize;
use std::time::Duration;

fn default_shadow_balance() -> Decimal {
    Decimal::new(10000, 0) // 10,000 USDC
}

/// Main configuration structure loaded from environment/config files
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub polymarket: PolymarketConfig,
    pub exchanges: ExchangeConfig,
    pub trading: TradingConfig,
    pub risk: RiskConfig,
    pub network: NetworkConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketConfig {
    /// WebSocket endpoint for CLOB
    pub ws_url: String,
    /// REST API endpoint
    pub rest_url: String,
    /// API key for authentication
    pub api_key: String,
    /// API secret (base64-encoded, used for HMAC L2 auth)
    pub api_secret: String,
    /// API passphrase (from L1 key derivation)
    pub api_passphrase: String,
    /// Private key for signing (hex-encoded, no 0x prefix)
    pub private_key: String,
    /// Polygon RPC endpoint
    pub polygon_rpc: String,
    /// CLOB contract address (standard markets)
    pub clob_address: String,
    /// Neg-risk CLOB contract address (neg-risk markets use a different exchange)
    pub neg_risk_clob_address: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeConfig {
    /// Binance WebSocket URL
    pub binance_ws: String,
    /// Coinbase WebSocket URL  
    pub coinbase_ws: String,
    /// Symbols to monitor for cross-exchange arb
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradingConfig {
    /// Shadow Mode (Paper Trading) - simulate without real execution
    #[serde(default)]
    pub is_shadow_mode: bool,
    /// Shadow mode initial virtual balance in USDC
    #[serde(default = "default_shadow_balance")]
    pub shadow_initial_balance: Decimal,
    /// Maximum taker fee (2026 dynamic fees up to 3.15%)
    pub max_taker_fee_bps: u32,
    /// Expected slippage in basis points
    pub slippage_bps: u32,
    /// Estimated gas cost in USD
    pub gas_cost_usd: Decimal,
    /// Minimum profit threshold in basis points
    pub min_profit_bps: u32,
    /// Default order size in USDC
    pub default_order_size: Decimal,
    /// Maximum order size in USDC
    pub max_order_size: Decimal,
    /// Maker spread offset (how far below market to place maker orders)
    pub maker_spread_offset_bps: u32,
    /// Enable maker mode
    pub maker_mode_enabled: bool,
    /// Enable taker/arbitrage mode
    pub taker_mode_enabled: bool,
    /// 2026 Dynamic Fee Curve settings
    #[serde(default)]
    pub fee_curve_2026: FeeCurve2026Config,
    /// Grid Layering settings for market-making
    #[serde(default)]
    pub grid: GridConfig,
}

/// 2026 Dynamic Fee Curve Configuration
/// Forces post_only=true when probability is in the high-fee zone (0.40-0.60)
#[derive(Debug, Clone, Deserialize)]
pub struct FeeCurve2026Config {
    /// Minimum probability for forced maker mode
    pub maker_force_prob_min: Decimal,
    /// Maximum probability for forced maker mode  
    pub maker_force_prob_max: Decimal,
    /// Maker rebate in basis points (negative = rebate)
    pub maker_rebate_bps: i32,
    /// Taker fee at probability edges (<0.15 or >0.85)
    pub taker_fee_at_edge_bps: u32,
    /// Taker fee in mid-range (0.40-0.60) - highest fee zone
    pub taker_fee_mid_range_bps: u32,
    /// Default taker fee for other ranges
    pub taker_fee_default_bps: u32,
    /// Pre-sign cache range (±% from current tick)
    pub presign_cache_range_pct: Decimal,
}

impl Default for FeeCurve2026Config {
    fn default() -> Self {
        Self {
            maker_force_prob_min: Decimal::new(40, 2),  // 0.40
            maker_force_prob_max: Decimal::new(60, 2),  // 0.60
            maker_rebate_bps: -20,
            taker_fee_at_edge_bps: 200,
            taker_fee_mid_range_bps: 315,
            taker_fee_default_bps: 250,
            presign_cache_range_pct: Decimal::new(2, 0), // 2%
        }
    }
}

impl FeeCurve2026Config {
    /// Check if probability is in forced maker zone (0.40-0.60)
    #[inline(always)]
    pub fn is_forced_maker_zone(&self, probability: Decimal) -> bool {
        probability >= self.maker_force_prob_min && probability <= self.maker_force_prob_max
    }
    
    /// Get the applicable taker fee for a given probability
    #[inline(always)]
    pub fn get_taker_fee_bps(&self, probability: Decimal) -> u32 {
        if probability < Decimal::new(15, 2) || probability > Decimal::new(85, 2) {
            self.taker_fee_at_edge_bps
        } else if self.is_forced_maker_zone(probability) {
            self.taker_fee_mid_range_bps
        } else {
            self.taker_fee_default_bps
        }
    }
}

/// Grid Layering Configuration for Market-Making
#[derive(Debug, Clone, Deserialize)]
pub struct GridConfig {
    /// Number of levels per side (buy/sell)
    pub levels_per_side: u8,
    /// Size per level in USDC
    pub size_per_level: Decimal,
    /// Base spacing between levels (0.01 = 1 cent)
    pub base_spacing: Decimal,
    /// Volatility multiplier for dynamic spacing
    pub volatility_multiplier: Decimal,
    /// Maximum total exposure per grid side
    pub max_grid_exposure: Decimal,
    /// Minimum aggregate profit BPS for grid
    pub min_aggregate_profit_bps: i32,
    /// Auto-rebalance on fill (place counter-order)
    pub auto_rebalance: bool,
    /// Use batch execution (create_orders endpoint)
    pub batch_execution: bool,
}

impl Default for GridConfig {
    fn default() -> Self {
        Self {
            levels_per_side: 5,
            size_per_level: Decimal::new(100, 0),
            base_spacing: Decimal::new(1, 2), // 0.01
            volatility_multiplier: Decimal::ONE,
            max_grid_exposure: Decimal::new(25, 0), // $25 max grid exposure
            min_aggregate_profit_bps: 50,
            auto_rebalance: true,
            batch_execution: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    /// Daily bankroll in USDC
    pub daily_bankroll: Decimal,
    /// Kill switch threshold as percentage of daily bankroll (e.g., 5.0 for 5%)
    pub kill_switch_threshold_pct: Decimal,
    /// Maximum position size per market
    pub max_position_per_market: Decimal,
    /// Maximum total exposure
    pub max_total_exposure: Decimal,
    /// Cooldown period after loss in milliseconds
    pub loss_cooldown_ms: u64,
    /// Gap protection threshold (% price move in single tick to trigger emergency stop)
    #[serde(default = "default_gap_threshold")]
    pub gap_threshold_pct: Decimal,
    /// Volatility spike threshold (e.g., 2.0 = 2x baseline triggers adaptive spacing)
    #[serde(default = "default_volatility_spike")]
    pub volatility_spike_threshold: f64,
    /// Volatility buffer multiplier for grid spacing when spike detected
    #[serde(default = "default_volatility_buffer")]
    pub volatility_buffer_multiplier: Decimal,
    /// Volume spike multiplier for circuit breaker (e.g., 10.0 = 10x average triggers halt)
    #[serde(default = "default_volume_spike_mult")]
    pub volume_spike_mult: f64,
}

fn default_gap_threshold() -> Decimal {
    Decimal::new(5, 0) // 5%
}

fn default_volatility_spike() -> f64 {
    2.0 // 2x baseline
}

fn default_volatility_buffer() -> Decimal {
    Decimal::new(15, 1) // 1.5x spacing
}

fn default_volume_spike_mult() -> f64 {
    10.0 // 10x average = circuit breaker
}

#[derive(Debug, Clone, Deserialize)]
pub struct NetworkConfig {
    /// WebSocket ping interval
    pub ws_ping_interval_ms: u64,
    /// Connection timeout
    pub connection_timeout_ms: u64,
    /// Order submission timeout (must be very low for HFT)
    pub order_timeout_ms: u64,
    /// Maximum reconnection attempts
    pub max_reconnect_attempts: u32,
}

impl Config {
    /// Load configuration from environment variables and config file
    pub fn load() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();
        
        let config = config::Config::builder()
            .add_source(config::File::with_name("config/default").required(false))
            .add_source(config::File::with_name("config/local").required(false))
            .add_source(config::Environment::with_prefix("POLYMORPH").separator("__"))
            .build()?;
        
        let mut cfg: Self = config.try_deserialize()?;
        
        // Override empty credentials with direct env vars (backwards compatibility).
        // The config crate expects POLYMORPH__POLYMARKET__PRIVATE_KEY format, but
        // existing VPS deployments use POLYMARKET_PRIVATE_KEY directly.
        macro_rules! env_fallback {
            ($field:expr, $var:expr) => {
                if $field.is_empty() {
                    if let Ok(val) = std::env::var($var) {
                        if !val.is_empty() {
                            $field = val;
                        }
                    }
                }
            };
        }
        env_fallback!(cfg.polymarket.api_key, "POLYMARKET_API_KEY");
        env_fallback!(cfg.polymarket.api_secret, "POLYMARKET_API_SECRET");
        env_fallback!(cfg.polymarket.api_passphrase, "POLYMARKET_API_PASSPHRASE");
        env_fallback!(cfg.polymarket.private_key, "POLYMARKET_PRIVATE_KEY");
        env_fallback!(cfg.polymarket.polygon_rpc, "POLYGON_RPC_URL");
        
        Ok(cfg)
    }
    
    /// Load with defaults for development/testing
    pub fn load_with_defaults() -> Self {
        Self {
            polymarket: PolymarketConfig {
                ws_url: std::env::var("POLYMARKET_WS_URL")
                    .unwrap_or_else(|_| "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string()),
                rest_url: std::env::var("POLYMARKET_REST_URL")
                    .unwrap_or_else(|_| "https://clob.polymarket.com".to_string()),
                api_key: std::env::var("POLYMARKET_API_KEY").unwrap_or_default(),
                api_secret: std::env::var("POLYMARKET_API_SECRET").unwrap_or_default(),
                api_passphrase: std::env::var("POLYMARKET_API_PASSPHRASE").unwrap_or_default(),
                private_key: std::env::var("POLYMARKET_PRIVATE_KEY").unwrap_or_default(),
                polygon_rpc: std::env::var("POLYGON_RPC_URL")
                    .unwrap_or_else(|_| "https://polygon-rpc.com".to_string()),
                clob_address: std::env::var("CLOB_CONTRACT_ADDRESS")
                    .unwrap_or_else(|_| "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E".to_string()),
                neg_risk_clob_address: std::env::var("NEG_RISK_CLOB_CONTRACT_ADDRESS")
                    .unwrap_or_else(|_| "0xC5d563A36AE78145C45a50134d48A1215220f80a".to_string()),
            },
            exchanges: ExchangeConfig {
                binance_ws: "wss://stream.binance.com:9443/ws".to_string(),
                coinbase_ws: "wss://ws-feed.exchange.coinbase.com".to_string(),
                symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
            },
            trading: TradingConfig {
                is_shadow_mode: false,
                shadow_initial_balance: Decimal::new(10000, 0), // 10,000 USDC
                max_taker_fee_bps: 315, // 3.15%
                slippage_bps: 10,       // 0.10%
                gas_cost_usd: Decimal::new(5, 2), // $0.05
                min_profit_bps: 15,     // 0.15% minimum profit (thin edge, high volume)
                default_order_size: Decimal::new(1, 0),   // $1 per order (scales with capital)
                max_order_size: Decimal::new(10, 0),      // $10 max (safety cap)
                maker_spread_offset_bps: 3, // 0.03% below market (tight quotes for priority)
                maker_mode_enabled: true,
                taker_mode_enabled: true,
                fee_curve_2026: FeeCurve2026Config::default(),
                grid: GridConfig::default(),
            },
            risk: RiskConfig {
                daily_bankroll: Decimal::new(50, 0), // $50 target wallet
                kill_switch_threshold_pct: Decimal::new(15, 0), // 15% = $7.50 max daily loss
                max_position_per_market: Decimal::new(20, 0), // $20 max per market (40% of bankroll)
                max_total_exposure: Decimal::new(40, 0), // $40 total (80% of bankroll, 20% reserve)
                loss_cooldown_ms: 60000, // 1 minute cooldown
                gap_threshold_pct: Decimal::new(5, 0), // 5% gap triggers stop
                volatility_spike_threshold: 2.0, // 2x baseline = spike
                volatility_buffer_multiplier: Decimal::new(15, 1), // 1.5x spacing
                volume_spike_mult: 10.0, // 10x average = circuit breaker
            },
            network: NetworkConfig {
                ws_ping_interval_ms: 15000,
                connection_timeout_ms: 5000,
                order_timeout_ms: 100, // 100ms max for order submission
                max_reconnect_attempts: 10,
            },
        }
    }
    
    /// Calculate total cost for a trade in basis points
    #[inline(always)]
    pub fn total_trade_cost_bps(&self) -> u32 {
        self.trading.max_taker_fee_bps + self.trading.slippage_bps
    }
    
    /// Get kill switch loss amount
    #[inline(always)]
    pub fn kill_switch_amount(&self) -> Decimal {
        self.risk.daily_bankroll * self.risk.kill_switch_threshold_pct / Decimal::new(100, 0)
    }
    
    /// Get WebSocket ping interval as Duration
    #[inline(always)]
    pub fn ws_ping_interval(&self) -> Duration {
        Duration::from_millis(self.network.ws_ping_interval_ms)
    }
}

/// Runtime-adjustable parameters (lock-free updates)
#[derive(Debug)]
pub struct RuntimeParams {
    pub trading_enabled: std::sync::atomic::AtomicBool,
    pub maker_mode: std::sync::atomic::AtomicBool,
    pub taker_mode: std::sync::atomic::AtomicBool,
}

impl RuntimeParams {
    pub fn new(config: &Config) -> Self {
        Self {
            trading_enabled: std::sync::atomic::AtomicBool::new(true),
            maker_mode: std::sync::atomic::AtomicBool::new(config.trading.maker_mode_enabled),
            taker_mode: std::sync::atomic::AtomicBool::new(config.trading.taker_mode_enabled),
        }
    }
    
    #[inline(always)]
    pub fn is_trading_enabled(&self) -> bool {
        self.trading_enabled.load(std::sync::atomic::Ordering::Relaxed)
    }
    
    #[inline(always)]
    pub fn disable_trading(&self) {
        self.trading_enabled.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}
