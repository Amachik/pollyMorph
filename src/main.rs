//! PollyMorph - High-Frequency Trading Bot for Polymarket
//! 
//! Low-latency arbitrage and market-making system designed for:
//! - Sub-5ms order execution via pre-signed EIP-712 orders
//! - Zero heap allocations in the hot path
//! - Cross-exchange arbitrage with Binance/Coinbase
//! - Fee-aware execution (2026 dynamic taker fees up to 3.15%)
//! - Kill switch for risk management (5% daily bankroll threshold)
//! - CPU core pinning to prevent kernel migration latency spikes
//! - mimalloc global allocator for 2-3x faster heap ops

// Use mimalloc as global allocator - 2-3x faster than system allocator
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod config;
mod types;
mod websocket;
mod signing;
mod pricing;
mod risk;
mod execution;
mod metrics;
mod hints;
mod turbo;
mod asm_ops;
mod grid;
mod shadow;
mod tuner;
mod backtester;
mod simulator;

use crate::config::{Config, RuntimeParams};
use crate::execution::{OrderExecutor, MakerManager};
use crate::metrics::MetricsServer;
use crate::pricing::PricingEngine;
use crate::risk::RiskManager;
use crate::shadow::ShadowEngine;
use crate::signing::OrderSigner;
use crate::types::{MarketId, TradeSignal, SignalUrgency, ExecutionReport, Side, TokenIdRegistry};
use crate::websocket::{PolymarketWs, PolymarketUserWs, BinanceWs, CoinbaseWs, WsEvent, MockDataGenerator, fetch_active_markets, hash_asset_id};

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn, Level};
use tracing_subscriber::EnvFilter;

/// Channel buffer sizes - tuned for low latency
const WS_EVENT_BUFFER: usize = 10_000;
const SIGNAL_BUFFER: usize = 1_000;
const REPORT_BUFFER: usize = 1_000;

#[tokio::main(flavor = "current_thread")] // Single-threaded for predictable latency
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    init_logging();
    
    info!("🚀 PollyMorph HFT Bot Starting...");
    info!("Target: Polymarket CLOB on Polygon");
    
    // Pin trading thread to a specific CPU core to prevent kernel migration
    // This eliminates 10-20ms latency spikes from context switches between cores
    pin_to_core();
    
    // Load configuration
    let config = Arc::new(Config::load().unwrap_or_else(|e| {
        warn!("Config file load failed ({}), using defaults with env vars", e);
        Config::load_with_defaults()
    }));
    info!("Configuration loaded");
    info!("  - Max taker fee: {}bps", config.trading.max_taker_fee_bps);
    info!("  - Kill switch threshold: {}%", config.risk.kill_switch_threshold_pct);
    info!("  - Default order size: ${}", config.trading.default_order_size);
    
    // Initialize runtime parameters (can be modified at runtime)
    let runtime_params = Arc::new(RuntimeParams::new(&config));
    
    // Check if running in Shadow Mode (no real trades)
    let is_shadow_mode = std::env::var("IS_SHADOW_MODE")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    
    if is_shadow_mode {
        info!("🔮 SHADOW MODE ENABLED - No real trades will be executed");
    }
    
    // Initialize order signer (use dummy signer in shadow mode if real key unavailable)
    let signer = match OrderSigner::new(&config) {
        Ok(s) => {
            info!("Order signer initialized: 0x{}", hex::encode(s.address().as_slice()));
            Arc::new(s)
        }
        Err(e) => {
            if is_shadow_mode {
                warn!("Real signer unavailable: {}. Using dummy signer for Shadow Mode.", e);
                match OrderSigner::new_dummy() {
                    Ok(s) => {
                        info!("Dummy signer initialized: 0x{}", hex::encode(s.address().as_slice()));
                        Arc::new(s)
                    }
                    Err(e2) => {
                        error!("Failed to create dummy signer: {}", e2);
                        return Err(e2);
                    }
                }
            } else {
                error!("Failed to initialize signer: {}. Set IS_SHADOW_MODE=true for testing.", e);
                return Err(e);
            }
        }
    };
    
    // Initialize risk manager
    let risk_manager = Arc::new(RiskManager::new(config.clone(), runtime_params.clone()));
    info!("Risk manager initialized with kill switch");
    
    // Create channels for inter-component communication
    let (ws_event_tx, ws_event_rx) = mpsc::channel::<WsEvent>(WS_EVENT_BUFFER);
    let (signal_tx, signal_rx) = mpsc::channel::<TradeSignal>(SIGNAL_BUFFER);
    let (report_tx, report_rx) = mpsc::channel::<ExecutionReport>(REPORT_BUFFER);
    
    // Initialize pricing engine
    let pricing_engine = Arc::new(PricingEngine::new(config.clone(), signal_tx));
    
    // Initialize token ID registry (maps u64 hash → real asset_id string)
    let token_registry = Arc::new(TokenIdRegistry::new());

    // Initialize order executor
    let executor = Arc::new(OrderExecutor::new(
        config.clone(),
        signer.clone(),
        risk_manager.clone(),
        report_tx,
        token_registry.clone(),
    ));
    
    // Initialize maker manager
    let maker_manager = Arc::new(MakerManager::new(executor.clone()));
    
    // Fetch active markets from Polymarket API and subscribe
    let polymarket_ws = {
        let mut ws = PolymarketWs::new(config.clone(), ws_event_tx.clone());
        
        // Fetch real active markets from Polymarket API
        match fetch_active_markets().await {
            Ok(token_ids) => {
                for token_id in &token_ids {
                    ws.subscribe(token_id.clone());
                    // Register in token registry so we can resolve hash → real string later
                    let hash = hash_asset_id(token_id);
                    token_registry.register(hash, token_id.clone(), false);
                    // Fetch full market info (neg_risk, fee_rate_bps, tick_size) from API
                    match executor.fetch_and_register_market_info(token_id, hash).await {
                        Ok(info) => {
                            info!("  📌 Subscribed: {}... (neg_risk={}, fee={}bps, tick={})",
                                  &token_id[..token_id.len().min(20)], info.neg_risk, info.fee_rate_bps, info.tick_size);
                        }
                        Err(e) => {
                            warn!("  ⚠️  Market info fetch failed for {}...: {} (using defaults)",
                                  &token_id[..token_id.len().min(20)], e);
                        }
                    }
                }
                info!("✅ Subscribed to {} active Polymarket markets (registry: {})",
                      token_ids.len(), token_registry.len());
            }
            Err(e) => {
                warn!("⚠️  Failed to fetch active markets: {}. Using fallback.", e);
                let fallback = "21742633143463906290569050155826241533067272736897614950488156847949938836455".to_string();
                let hash = hash_asset_id(&fallback);
                token_registry.register(hash, fallback.clone(), false);
                ws.subscribe(fallback);
            }
        }
        ws
    };
    
    let binance_ws = BinanceWs::new(config.clone(), ws_event_tx.clone());
    let coinbase_ws = CoinbaseWs::new(config.clone(), ws_event_tx.clone());
    
    // Authenticated user channel for order/trade updates (fills, cancellations)
    let user_ws = PolymarketUserWs::new(config.clone(), ws_event_tx.clone());
    
    // Create Shadow Engine for paper trading simulation
    let shadow_engine = if is_shadow_mode {
        let engine = Arc::new(ShadowEngine::new(config.clone()));
        info!("🎭 Shadow Engine initialized with $10,000 virtual balance");
        Some(engine)
    } else {
        None
    };
    
    // Mock data generator - only used as fallback if USE_MOCK_DATA=true
    // Shadow Mode uses REAL Polymarket data by default
    let use_mock_data = std::env::var("USE_MOCK_DATA").map(|v| v == "true").unwrap_or(false);
    let mock_generator = if use_mock_data {
        info!("⚠️  Using MOCK data (set USE_MOCK_DATA=false for real Polymarket data)");
        Some(MockDataGenerator::new(
            ws_event_tx.clone(),
            vec![
                "mock-market-1".to_string(),
                "mock-market-2".to_string(),
                "mock-market-3".to_string(),
            ],
        ))
    } else {
        if is_shadow_mode {
            info!("📡 Shadow Mode using REAL Polymarket WebSocket data");
        }
        None
    };
    
    info!("WebSocket handlers initialized");

    // Check USDC collateral allowance (skip in shadow mode)
    if !is_shadow_mode {
        match executor.check_collateral_allowance().await {
            Ok(allowance_raw) => {
                let usdc = allowance_raw / 1_000_000;
                if usdc < 10 {
                    warn!("🚨 USDC allowance is only ${} — orders will fail on-chain!", usdc);
                    warn!("   Approve the exchange contract for USDC.e before trading.");
                } else {
                    info!("💰 USDC allowance: ${} (sufficient)", usdc);
                }
            }
            Err(e) => {
                warn!("⚠️  Could not check USDC allowance: {} (continuing anyway)", e);
            }
        }
        // Set capital limit from actual on-chain USDC.e balance
        match executor.check_and_set_capital_from_balance().await {
            Ok(_) => {}
            Err(e) => {
                warn!("⚠️  Could not check USDC.e balance: {} (using default capital limit)", e);
            }
        }
        // Bootstrap inventory from recent trades (recover positions from previous runs)
        match executor.bootstrap_positions(&pricing_engine.inventory).await {
            Ok(n) => {
                if n > 0 {
                    info!("📦 Inventory bootstrapped with {} trade records", n);
                }
            }
            Err(e) => {
                warn!("⚠️  Could not bootstrap positions: {} (starting with empty inventory)", e);
            }
        }
    }
    
    // Spawn all async tasks
    let handles = spawn_tasks(
        config.clone(),
        runtime_params.clone(),
        risk_manager.clone(),
        shadow_engine.clone(),
        pricing_engine,
        executor,
        maker_manager,
        polymarket_ws,
        user_ws,
        binance_ws,
        coinbase_ws,
        mock_generator,
        ws_event_rx,
        signal_rx,
        report_rx,
    );
    
    info!("✅ All systems operational");
    info!("Trading enabled: {}", runtime_params.is_trading_enabled());
    
    // Wait for shutdown signal
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            warn!("Shutdown signal received");
        }
        result = futures_util::future::select_all(handles.into_iter().map(Box::pin)) => {
            if let (Err(e), _, _) = result {
                error!("Task failed: {}", e);
            }
        }
    }
    
    // Graceful shutdown
    info!("Initiating graceful shutdown...");
    runtime_params.disable_trading();
    
    // Log final metrics
    let risk_metrics = risk_manager.get_metrics();
    info!("Final Risk Metrics:");
    info!("  - Daily P&L: ${}", risk_metrics.daily_pnl);
    info!("  - Total Exposure: ${}", risk_metrics.total_exposure);
    info!("  - Trades: {}", risk_metrics.trade_count);
    info!("  - Kill Switch: {}", if risk_metrics.kill_switch_triggered { "TRIGGERED" } else { "OK" });
    
    info!("👋 PollyMorph shutdown complete");
    Ok(())
}

/// Initialize logging with JSON format for production
fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,polymorph_hft=debug"));
    
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();
}

/// Pin the current thread to a specific CPU core to prevent kernel migration
/// 
/// CPU core migration by the Linux scheduler causes 10-20ms latency spikes due to:
/// - L1/L2/L3 cache invalidation (cold cache on new core)
/// - TLB flush and repopulation
/// - NUMA memory access penalties (if crossing NUMA nodes)
/// 
/// For HFT, we pin to an isolated core (configured via `isolcpus` kernel param)
/// Recommended: Reserve cores 2-3 for trading, leave 0-1 for OS/interrupts
fn pin_to_core() {
    // Get available CPU cores
    let core_ids = core_affinity::get_core_ids();
    
    match core_ids {
        Some(cores) if !cores.is_empty() => {
            // Strategy: Pin to the last available core (usually less OS interrupt traffic)
            // For production: Use an isolated core via `isolcpus=2,3` kernel parameter
            // and set TRADING_CORE_ID env var to specify which core to use
            let target_core = std::env::var("TRADING_CORE_ID")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .and_then(|id| cores.iter().find(|c| c.id == id).copied())
                .unwrap_or_else(|| {
                    // Default: use last core (least OS activity typically)
                    cores[cores.len() - 1]
                });
            
            if core_affinity::set_for_current(target_core) {
                info!("📌 Trading thread pinned to CPU core {}", target_core.id);
                info!("   Available cores: {:?}", cores.iter().map(|c| c.id).collect::<Vec<_>>());
                info!("   Tip: Set TRADING_CORE_ID env var to specify core");
                info!("   Tip: Use 'isolcpus=N' kernel param to isolate cores from scheduler");
            } else {
                warn!("⚠️  Failed to pin thread to core {}. Running without affinity.", target_core.id);
                warn!("   This may cause 10-20ms latency spikes from CPU migration");
            }
        }
        _ => {
            warn!("⚠️  Could not detect CPU cores. Running without core affinity.");
            warn!("   This may cause latency spikes on Linux systems.");
        }
    }
}

/// Spawn all async tasks and return their handles
fn spawn_tasks(
    config: Arc<Config>,
    runtime_params: Arc<RuntimeParams>,
    risk_manager: Arc<RiskManager>,
    shadow_engine: Option<Arc<ShadowEngine>>,
    pricing_engine: Arc<PricingEngine>,
    executor: Arc<OrderExecutor>,
    maker_manager: Arc<MakerManager>,
    polymarket_ws: PolymarketWs,
    user_ws: PolymarketUserWs,
    binance_ws: BinanceWs,
    coinbase_ws: CoinbaseWs,
    mock_generator: Option<MockDataGenerator>,
    ws_event_rx: mpsc::Receiver<WsEvent>,
    signal_rx: mpsc::Receiver<TradeSignal>,
    report_rx: mpsc::Receiver<ExecutionReport>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();
    
    // WebSocket connection tasks
    handles.push(tokio::spawn(async move {
        if let Err(e) = polymarket_ws.run().await {
            error!("Polymarket WebSocket error: {}", e);
        }
    }));
    
    handles.push(tokio::spawn(async move {
        if let Err(e) = binance_ws.run().await {
            error!("Binance WebSocket error: {}", e);
        }
    }));
    
    handles.push(tokio::spawn(async move {
        if let Err(e) = coinbase_ws.run().await {
            error!("Coinbase WebSocket error: {}", e);
        }
    }));
    
    // Polymarket User Channel (authenticated order/trade updates)
    handles.push(tokio::spawn(async move {
        if let Err(e) = user_ws.run().await {
            error!("Polymarket User WS error: {}", e);
        }
    }));
    
    // Mock data generator for Shadow Mode
    if let Some(mock_gen) = mock_generator {
        handles.push(tokio::spawn(async move {
            if let Err(e) = mock_gen.run().await {
                error!("Mock data generator error: {}", e);
            }
        }));
    }
    
    // Prometheus metrics server on port 9090
    handles.push(tokio::spawn(async move {
        let metrics_server = MetricsServer::new(9090);
        metrics_server.run().await;
    }));
    
    // Event processing task (HOT PATH)
    let pricing_engine_clone = pricing_engine.clone();
    let shadow_engine_clone = shadow_engine.clone();
    let token_registry_clone = executor.token_registry.clone();
    let executor_for_events = executor.clone();
    handles.push(tokio::spawn(async move {
        run_event_processor(pricing_engine_clone, shadow_engine_clone, token_registry_clone, executor_for_events, ws_event_rx).await;
    }));
    
    // Signal execution task (HOT PATH)
    let config_clone = config.clone();
    let runtime_params_clone = runtime_params.clone();
    let shadow_engine_clone = shadow_engine.clone();
    let pricing_engine_for_signals = pricing_engine.clone();
    let executor_for_reports = executor.clone(); // Clone before move into signal executor
    let executor_for_cancel = executor.clone();
    let executor_for_metrics = executor.clone();
    handles.push(tokio::spawn(async move {
        run_signal_executor(
            config_clone,
            runtime_params_clone,
            executor,
            maker_manager,
            shadow_engine_clone,
            pricing_engine_for_signals,
            signal_rx,
        ).await;
    }));
    
    // Execution report handler (wires fills to inventory tracker)
    let risk_manager_clone = risk_manager.clone();
    let pricing_engine_clone2 = pricing_engine.clone();
    handles.push(tokio::spawn(async move {
        run_report_handler(risk_manager_clone, pricing_engine_clone2, executor_for_reports, report_rx).await;
    }));

    // Periodic stale-order cancellation loop
    let pricing_engine_for_cancel = pricing_engine.clone();
    handles.push(tokio::spawn(async move {
        run_stale_order_canceller(pricing_engine_for_cancel, executor_for_cancel).await;
    }));
    
    // Metrics reporter
    let config_for_metrics = config.clone();
    let risk_manager_clone = risk_manager.clone();
    let shadow_engine_clone = shadow_engine.clone();
    let pricing_for_metrics = pricing_engine.clone();
    handles.push(tokio::spawn(async move {
        run_metrics_reporter(config_for_metrics, risk_manager_clone, shadow_engine_clone, executor_for_metrics, pricing_for_metrics).await;
    }));
    
    handles
}

/// Event processor - processes WebSocket events and feeds pricing engine
/// This is the HOT PATH - must be extremely fast
async fn run_event_processor(
    pricing_engine: Arc<PricingEngine>,
    shadow_engine: Option<Arc<ShadowEngine>>,
    token_registry: Arc<TokenIdRegistry>,
    executor: Arc<OrderExecutor>,
    mut ws_event_rx: mpsc::Receiver<WsEvent>,
) {
    info!("Event processor started");
    
    while let Some(event) = ws_event_rx.recv().await {
        // Track WebSocket connection status
        if let WsEvent::ConnectionStatus { source, connected } = &event {
            let source_str = match source {
                websocket::ConnectionSource::Polymarket => "polymarket",
                websocket::ConnectionSource::Binance => "binance",
                websocket::ConnectionSource::Coinbase => "coinbase",
            };
            metrics::update_ws_status(source_str, *connected);
            if *connected {
                info!("WebSocket {} connected", source_str);
            } else {
                warn!("WebSocket {} disconnected", source_str);
            }
        }

        // Auto-register tokens from book updates into the registry
        if let WsEvent::BookUpdate { ref market_id, ref snapshot } = event {
            if snapshot.token_id != 0 && token_registry.get_str(snapshot.token_id).is_none() {
                token_registry.register(snapshot.token_id, market_id.clone(), false);
            }
            // Track spread per market
            if let Some(spread_bps) = snapshot.spread_bps() {
                metrics::update_market_spread(market_id, spread_bps as f64);
            }
        }

        // In Shadow Mode, check if incoming trades would fill our virtual orders
        if let Some(ref shadow) = shadow_engine {
            if let WsEvent::Trade { ref market_id, price, size, side, .. } = event {
                let token_id = crate::websocket::hash_asset_id(market_id);
                let market = MarketId {
                    token_id,
                    condition_id: [0u8; 32],
                };
                let fills = shadow.process_trade_update(&market, side, price, size, price);
                if !fills.is_empty() {
                    info!("🎯 Shadow Mode: {} virtual fill(s) triggered", fills.len());
                }
            }
        }

        // Handle order updates from user channel WebSocket
        // This wires real-time fill/cancel notifications to the executor
        if let WsEvent::OrderUpdate { ref order_id, ref status, ref size_matched, .. } = event {
            executor.process_order_update(
                order_id,
                status,
                Some(size_matched.as_str()),
            ).await;
        }
        
        // Process event through pricing engine
        pricing_engine.process_event(event).await;
    }
    
    warn!("Event processor stopped");
}

/// Signal executor - executes trade signals from pricing engine
/// This is the HOT PATH - must achieve sub-5ms execution
async fn run_signal_executor(
    config: Arc<Config>,
    runtime_params: Arc<RuntimeParams>,
    executor: Arc<OrderExecutor>,
    maker_manager: Arc<MakerManager>,
    shadow_engine: Option<Arc<ShadowEngine>>,
    pricing_engine: Arc<PricingEngine>,
    mut signal_rx: mpsc::Receiver<TradeSignal>,
) {
    info!("Signal executor started");
    
    while let Some(signal) = signal_rx.recv().await {
        // Skip if trading disabled
        if !runtime_params.is_trading_enabled() {
            continue;
        }
        
        // In Shadow Mode, track virtual orders instead of real execution
        if let Some(ref shadow) = shadow_engine {
            let is_maker = matches!(signal.urgency, SignalUrgency::Low | SignalUrgency::Medium);
            if let Some(order_id) = shadow.track_virtual_order(&signal, is_maker) {
                info!(
                    "Shadow: Virtual {} order #{} - {:?} {} @ {}",
                    if is_maker { "MAKER" } else { "TAKER" },
                    order_id,
                    signal.side,
                    signal.size,
                    signal.price
                );
            }
            continue; // Don't execute real orders in shadow mode
        }
        
        // Route based on urgency and mode (live trading only)
        match signal.urgency {
            SignalUrgency::Critical | SignalUrgency::High => {
                // Immediate execution for arbitrage opportunities
                if config.trading.taker_mode_enabled {
                    let sig_side = signal.side;
                    let sig_price = signal.price;
                    let sig_size = signal.size;
                    let sig_token_id = signal.market_id.token_id;
                    match executor.execute_signal(signal).await {
                        Ok(order_id) => {
                            pricing_engine.lifecycle.register_order(
                                order_id.clone(), sig_token_id, sig_side, sig_price, sig_size,
                            );
                            metrics::ORDERS_SUBMITTED.with_label_values(&["submitted"]).inc();
                            info!("Taker order submitted: {}", order_id);
                        }
                        Err(e) => {
                            if matches!(e, crate::execution::ExecutionError::RateLimited) {
                                metrics::ORDERS_RATE_LIMITED.inc();
                            } else {
                                metrics::ORDERS_SUBMITTED.with_label_values(&["failed"]).inc();
                                warn!("Taker order failed: {}", e);
                            }
                        }
                    }
                }
            }
            SignalUrgency::Low | SignalUrgency::Medium => {
                // Maker orders: execute directly (pricing engine already computed proper prices)
                // Do NOT use MakerManager — it double-quotes by re-offsetting already-offset prices
                // and creates 12 orders per book update instead of 2.
                if config.trading.maker_mode_enabled {
                    let sig_side = signal.side;
                    let sig_price = signal.price;
                    let sig_size = signal.size;
                    let sig_token_id = signal.market_id.token_id;
                    match executor.execute_signal(signal).await {
                        Ok(order_id) => {
                            pricing_engine.lifecycle.register_order(
                                order_id.clone(), sig_token_id, sig_side, sig_price, sig_size,
                            );
                            metrics::ORDERS_SUBMITTED.with_label_values(&["submitted"]).inc();
                            info!("Maker order submitted: {} {:?} {} @ {}", order_id, sig_side, sig_size, sig_price);
                        }
                        Err(e) => {
                            if matches!(e, crate::execution::ExecutionError::RateLimited) {
                                metrics::ORDERS_RATE_LIMITED.inc();
                            } else {
                                metrics::ORDERS_SUBMITTED.with_label_values(&["failed"]).inc();
                                warn!("Maker order failed: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }
    
    warn!("Signal executor stopped");
}

/// Periodic stale-order cancellation loop.
/// Checks every 2 seconds for orders that are too old or have drifted too far from mid,
/// and sends real cancel requests to the Polymarket API.
async fn run_stale_order_canceller(
    pricing_engine: Arc<PricingEngine>,
    executor: Arc<OrderExecutor>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
    info!("Stale order canceller started (500ms interval)");

    loop {
        interval.tick().await;

        // Build current mid prices from pricing engine market states
        let current_mids = pricing_engine.get_current_mids();

        let stale_api_ids = pricing_engine.lifecycle.cancel_stale_orders(&current_mids);
        for api_order_id in &stale_api_ids {
            match executor.cancel_order(api_order_id).await {
                Ok(()) => {
                    metrics::ORDERS_CANCELLED.inc();
                    info!("Cancelled stale order: {}", api_order_id);
                }
                Err(e) => warn!("Failed to cancel stale order {}: {}", api_order_id, e),
            }
        }
        // Update active orders gauge
        metrics::ACTIVE_ORDERS_TOTAL.set(pricing_engine.lifecycle.active_order_ids().len() as i64);
    }
}

/// Execution report handler - processes order fills and updates.
/// Wires fills to the PricingEngine inventory tracker and lifecycle manager.
async fn run_report_handler(
    risk_manager: Arc<RiskManager>,
    pricing_engine: Arc<PricingEngine>,
    executor: Arc<OrderExecutor>,
    mut report_rx: mpsc::Receiver<ExecutionReport>,
) {
    info!("Report handler started");
    
    while let Some(report) = report_rx.recv().await {
        match report.status {
            types::OrderStatus::Filled | types::OrderStatus::PartiallyFilled => {
                if report.filled_size > Decimal::ZERO
                    && report.market_id.token_id != 0
                    && report.price > Decimal::ZERO
                {
                    pricing_engine.inventory.record_fill(
                        report.market_id.token_id,
                        report.side,
                        report.filled_size,
                        report.price,
                    );
                    // Feed risk manager for P&L tracking, exposure limits, and kill switch
                    risk_manager.record_trade(&report);
                    // Track fill metrics
                    let side_str = match report.side { types::Side::Buy => "buy", types::Side::Sell => "sell" };
                    metrics::FILLS_TOTAL.with_label_values(&[side_str]).inc();
                    let fill_usdc = (report.filled_size * report.price).to_f64().unwrap_or(0.0);
                    metrics::TOTAL_USDC_TRADED.inc_by(fill_usdc);
                    metrics::USDC_TRADED_BY_SIDE.with_label_values(&[side_str]).inc_by(fill_usdc);
                    // Track maker fills + rebates (all our orders are post_only = maker)
                    // 2026 fee curve: maker rebate is ~0.20% in the 0.40-0.60 zone
                    metrics::MAKER_TRADES_TOTAL.inc();
                    let rebate_estimate = fill_usdc * 0.002; // ~0.20% maker rebate
                    metrics::REBATES_EARNED.inc_by(rebate_estimate);
                    metrics::TRADES_BY_TYPE.with_label_values(&["maker", "success"]).inc();
                    // Update inventory position gauge
                    let token_str = report.market_id.token_id.to_string();
                    let net_pos = pricing_engine.inventory.net_position(report.market_id.token_id);
                    metrics::INVENTORY_POSITION.with_label_values(&[&token_str]).set(net_pos.to_f64().unwrap_or(0.0));
                }
                pricing_engine.lifecycle.remove_by_api_id(&report.order_id);
                info!(
                    "✅ Order filled: {} {:?} {} @ {} (lifecycle+inventory updated)",
                    report.order_id,
                    report.side,
                    report.filled_size,
                    report.price
                );
            }
            types::OrderStatus::Rejected => {
                pricing_engine.lifecycle.remove_by_api_id(&report.order_id);
                warn!("❌ Order rejected: {}", report.order_id);
            }
            types::OrderStatus::Cancelled => {
                pricing_engine.lifecycle.remove_by_api_id(&report.order_id);
                info!("🚫 Order cancelled: {}", report.order_id);
            }
            _ => {}
        }
    }
    
    warn!("Report handler stopped");
}

/// Metrics reporter - periodic update of Prometheus gauges from risk manager
async fn run_metrics_reporter(
    config: Arc<Config>,
    risk_manager: Arc<RiskManager>,
    shadow_engine: Option<Arc<ShadowEngine>>,
    executor: Arc<OrderExecutor>,
    pricing_engine: Arc<PricingEngine>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    
    loop {
        interval.tick().await;
        
        let risk_metrics = risk_manager.get_metrics();
        
        // Update Prometheus gauges
        let daily_pnl = risk_metrics.daily_pnl.to_f64().unwrap_or(0.0);
        let exposure = risk_metrics.total_exposure.to_f64().unwrap_or(0.0);
        // Use actual on-chain capital limit (not config bankroll assumption)
        let wallet = executor.max_capital_usdc() + daily_pnl;

        metrics::update_health_metrics(
            wallet,
            daily_pnl,
            exposure,
            risk_metrics.kill_switch_triggered,
            risk_metrics.position_count as i64,
        );
        // Update live trading gauges
        metrics::WALLET_BALANCE.set(wallet);
        metrics::NET_PNL.set(daily_pnl);
        metrics::ACTIVE_POSITIONS.set(risk_metrics.position_count as i64);

        // Signature cache stats
        let (hits, misses, size) = executor.signer_cache_stats();
        metrics::update_cache_stats(size, hits, misses);

        // Order flow imbalance per market
        for (token_id, imbalance) in pricing_engine.get_flow_imbalances() {
            metrics::ORDER_FLOW_IMBALANCE
                .with_label_values(&[&token_id.to_string()])
                .set(imbalance);
        }
        
        // Log periodically (every 12 ticks = 60 seconds)
        static TICK_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let tick = TICK_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        if tick % 12 == 0 {
            // Show Shadow Mode stats if available
            if let Some(ref shadow) = shadow_engine {
                let stats = shadow.get_stats();
                info!("📊 Shadow Mode Metrics:");
                info!("  💰 Realized: ${:.2} | 🔻 Unrealized: ${:.2} | 🏁 Net Equity: ${:.2}",
                    stats.realized_pnl, stats.unrealized_pnl, stats.net_equity);
                info!("  � Balance: ${:.2} (started: ${:.2})", stats.current_balance, stats.initial_balance);
                info!("  📊 Trades: {} (wins: {}, losses: {})", stats.total_trades, stats.winning_trades, stats.losing_trades);
                info!("  🏷️  Fills: {} maker, {} taker", stats.maker_fills, stats.taker_fills);
                info!("  💸 Fees: ${:.4} paid, ${:.4} rebates", stats.total_fees_paid, stats.total_rebates_earned);
                info!("  🚫 Throttled: {} orders | 📉 Slippage: ${:.4}", stats.orders_throttled, stats.total_slippage);
                info!("  📋 Active Orders: {}", shadow.active_order_count());
                
                let win_rate = if stats.total_trades > 0 {
                    (stats.winning_trades as f64 / stats.total_trades as f64) * 100.0
                } else {
                    0.0
                };
                info!("  🎯 Win Rate: {:.1}%", win_rate);
            } else {
                info!("📊 Metrics Update:");
                info!("  P&L: ${:.2}", risk_metrics.daily_pnl);
                info!("  Exposure: ${:.2}", risk_metrics.total_exposure);
                info!("  Trades: {}", risk_metrics.trade_count);
                info!("  Positions: {}", risk_metrics.position_count);
            }
            
            if risk_metrics.kill_switch_triggered {
                error!("  🚨 KILL SWITCH ACTIVE");
            }
        }
    }
}

/// Pre-warm order cache for known markets
async fn warm_order_cache(
    signer: Arc<OrderSigner>,
    markets: Vec<(MarketId, rust_decimal::Decimal)>,
    size: rust_decimal::Decimal,
) {
    info!("Pre-warming order cache for {} markets...", markets.len());
    
    for (market_id, base_price) in markets {
        if let Err(e) = signer.warm_cache(
            market_id,
            base_price,
            size,
            10,  // 10 price levels
            5,   // 5 bps spacing
        ).await {
            warn!("Failed to warm cache for market {:?}: {}", market_id.token_id, e);
        }
    }
    
    info!("Order cache warm-up complete");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Side;
    
    #[test]
    fn test_config_loading() {
        let config = Config::load_with_defaults();
        assert!(config.trading.max_taker_fee_bps > 0);
        assert!(config.risk.kill_switch_threshold_pct > rust_decimal::Decimal::ZERO);
    }
    
    #[test]
    fn test_fee_calculation() {
        let config = Config::load_with_defaults();
        let total_cost = config.total_trade_cost_bps();
        // Should include taker fee + slippage
        assert!(total_cost >= config.trading.max_taker_fee_bps);
    }
    
    #[tokio::test]
    async fn test_channel_creation() {
        let (tx, mut rx) = mpsc::channel::<WsEvent>(100);
        
        tx.send(WsEvent::ConnectionStatus {
            source: websocket::ConnectionSource::Polymarket,
            connected: true,
        }).await.unwrap();
        
        let event = rx.recv().await.unwrap();
        match event {
            WsEvent::ConnectionStatus { connected, .. } => {
                assert!(connected);
            }
            _ => panic!("Wrong event type"),
        }
    }
}
