//! Oracle Front-Runner — Late-Window AMM Sweep Strategy
//!
//! Replicates the RetamzZ wallet strategy on Polymarket:
//! 1. Monitor BTC/ETH/SOL/XRP 15-minute Up/Down markets via CLOB WebSocket.
//! 2. In the final ENTRY_WINDOW_SECS seconds, identify the winning side from
//!    the AMM book price (the market's current best probability estimate).
//! 3. Sweep the ENTIRE AMM ask book on the winning side via IOC taker orders.
//! 4. Redeem for $1.00 per token after resolution.

use crate::arb::{ArbAsset, ArbMarket, ArbWsEvent, BookLevel, TokenBook, WsCommand, run_book_watcher};
use crate::config::Config;
use crate::execution::OrderExecutor;
use crate::types::{MarketId, Side, OrderType, TimeInForce, TokenIdRegistry, TradeSignal, SignalUrgency, PreparedOrder};
use crate::websocket::hash_asset_id;

use ethers::types::{Address, U256};

use futures_util::FutureExt;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const ENTRY_WINDOW_SECS: i64 = 120;
const MIN_SECS_REMAINING: i64 = 15;
const MIN_WINNING_PRICE: f64 = 0.52;
const MAX_SWEEP_PRICE: f64 = 0.85;  // 10% taker fee -> net cost 0.935, min profit $0.065/token
const MAX_BET_USDC: f64 = 500.0;
const MAX_CAPITAL_FRACTION: f64 = 0.90;
const MIN_ORDER_SIZE: f64 = 5.0;
const MARKET_SCAN_INTERVAL_SECS: u64 = 10;
const PRE_MARKET_LEAD_SECS: i64 = 300;
const WS_CHANNEL_BUFFER: usize = 1024;
const POSITIONS_FILE: &str = "oracle_positions.json";
/// Slug prefixes used in actual market slugs (e.g. "btc-updown-15m-1770801300")
/// These are also used to identify asset type from the slug.
const SERIES_SLUGS: &[&str] = &[
    "btc-up-or-down-15m",
    "eth-up-or-down-15m",
    "sol-up-or-down-15m",
    "xrp-up-or-down-15m",
];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SweptSide { Up, Down }

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct OraclePosition {
    market: ArbMarket,
    swept_side: SweptSide,
    tokens_bought: f64,
    cost_usdc: f64,
    entered_at: chrono::DateTime<chrono::Utc>,
    redeemed: bool,
}

#[derive(Debug)]
struct SweepResult {
    event_slug: String,
    market: ArbMarket,
    swept_side: SweptSide,
    estimated_cost: f64,
    tokens_total: f64,
    cost_total: f64,
    orders_ok: u32,
    orders_count: usize,
    total_elapsed_ms: f64,
    error: Option<String>,
}

#[derive(Debug)]
struct RedeemResult {
    event_slug: String,
    usdc_recovered: f64,
    profit: f64,
    error: Option<String>,
}

// ---------------------------------------------------------------------------
// Position Persistence
// ---------------------------------------------------------------------------

fn save_positions(positions: &[OraclePosition]) {
    let active: Vec<&OraclePosition> = positions.iter().filter(|p| !p.redeemed).collect();
    if let Ok(json) = serde_json::to_string_pretty(&active) {
        let tmp = format!("{}.tmp", POSITIONS_FILE);
        if std::fs::write(&tmp, &json).is_ok() {
            let _ = std::fs::rename(&tmp, POSITIONS_FILE);
        }
    }
}

fn load_positions() -> Vec<OraclePosition> {
    let path = std::path::Path::new(POSITIONS_FILE);
    if !path.exists() { return Vec::new(); }
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str::<Vec<OraclePosition>>(&s).ok())
        .unwrap_or_default()
        .into_iter()
        .filter(|p| !p.redeemed)
        .collect()
}

// ---------------------------------------------------------------------------
// Oracle Engine
// ---------------------------------------------------------------------------

pub struct OracleEngine {
    config: Arc<Config>,
    executor: Arc<OrderExecutor>,
    token_registry: Arc<TokenIdRegistry>,
    client: reqwest::Client,
    capital_usdc: f64,
    shutdown: Arc<AtomicBool>,
    total_pnl: f64,
    sweeps_completed: u64,
    positions: Vec<OraclePosition>,
    book_cache: HashMap<String, TokenBook>,
    ws_rx: mpsc::Receiver<ArbWsEvent>,
    ws_cmd_tx: mpsc::Sender<WsCommand>,
    tracked_markets: HashMap<String, ArbMarket>,
    asset_to_market: HashMap<String, String>,
    ws_subscribed: HashSet<String>,
    ws_connected: bool,
    executing_slugs: HashSet<String>,
    failed_slugs: HashSet<String>,
    last_debug_log: HashMap<String, std::time::Instant>,
    sweep_rx: mpsc::Receiver<SweepResult>,
    sweep_tx: mpsc::Sender<SweepResult>,
    redeem_rx: mpsc::Receiver<RedeemResult>,
    redeem_tx: mpsc::Sender<RedeemResult>,
    event_detail_cache: HashMap<String, Vec<ArbMarket>>,
}

impl OracleEngine {
    pub fn new(
        config: Arc<Config>,
        executor: Arc<OrderExecutor>,
        token_registry: Arc<TokenIdRegistry>,
        capital_usdc: f64,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .pool_max_idle_per_host(5)
            .tcp_nodelay(true)
            .user_agent("Mozilla/5.0")
            .build()
            .expect("Failed to create HTTP client");

        let (ws_event_tx, ws_rx) = mpsc::channel::<ArbWsEvent>(WS_CHANNEL_BUFFER);
        let (ws_cmd_tx, ws_cmd_rx) = mpsc::channel::<WsCommand>(64);
        let (sweep_tx, sweep_rx) = mpsc::channel::<SweepResult>(32);
        let (redeem_tx, redeem_rx) = mpsc::channel::<RedeemResult>(32);

        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_for_ws = shutdown_flag.clone();
        tokio::spawn(async move {
            run_book_watcher(ws_event_tx, ws_cmd_rx, shutdown_for_ws).await;
        });

        let mut recovered = load_positions();
        let now_ts = chrono::Utc::now().timestamp();
        recovered.retain(|p| p.market.event_end_ts > now_ts - 600);
        let deployed: f64 = recovered.iter().map(|p| p.cost_usdc).sum();
        let adjusted_capital = (capital_usdc - deployed).max(0.0);
        if !recovered.is_empty() {
            info!("💾 Recovered {} oracle positions (${:.2} deployed)", recovered.len(), deployed);
        }

        let mut tracked_markets: HashMap<String, ArbMarket> = HashMap::new();
        let mut asset_to_market: HashMap<String, String> = HashMap::new();
        let mut ws_subscribed: HashSet<String> = HashSet::new();
        for pos in &recovered {
            let m = &pos.market;
            let slug = m.event_slug.clone();
            asset_to_market.insert(m.up_token_id.clone(), slug.clone());
            asset_to_market.insert(m.down_token_id.clone(), slug.clone());
            ws_subscribed.insert(m.up_token_id.clone());
            ws_subscribed.insert(m.down_token_id.clone());
            tracked_markets.insert(slug, m.clone());
        }

        Self {
            config, executor, token_registry, client,
            capital_usdc: adjusted_capital,
            shutdown: shutdown_flag,
            total_pnl: 0.0,
            sweeps_completed: 0,
            positions: recovered,
            book_cache: HashMap::new(),
            ws_rx, ws_cmd_tx,
            tracked_markets, asset_to_market, ws_subscribed,
            ws_connected: false,
            executing_slugs: HashSet::new(),
            failed_slugs: HashSet::new(),
            last_debug_log: HashMap::new(),
            sweep_rx, sweep_tx, redeem_rx, redeem_tx,
            event_detail_cache: HashMap::new(),
        }
    }

    pub fn shutdown_flag(&self) -> Arc<AtomicBool> { self.shutdown.clone() }

    pub async fn run(&mut self) {
        info!("🎯 ORACLE ENGINE STARTED — Capital: ${:.2}", self.capital_usdc);
        info!("   Strategy: Sweep winning side in final {}s of 15m markets", ENTRY_WINDOW_SECS);
        info!("   Max bet: ${:.0} | Min winning price: {:.2}", MAX_BET_USDC, MIN_WINNING_PRICE);

        if !self.ws_subscribed.is_empty() {
            let ids: Vec<String> = self.ws_subscribed.iter().cloned().collect();
            info!("💾 Re-subscribing {} recovered asset IDs", ids.len());
            let _ = self.ws_cmd_tx.send(WsCommand::Subscribe(ids)).await;
        }

        let mut scan_interval = tokio::time::interval(
            std::time::Duration::from_secs(MARKET_SCAN_INTERVAL_SECS),
        );
        scan_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        scan_interval.tick().await;
        let mut status_interval = tokio::time::interval(std::time::Duration::from_secs(60));

        loop {
            tokio::select! {
                ws_event = self.ws_rx.recv() => {
                    match ws_event {
                        Some(ArbWsEvent::BookSnapshot { asset_id, book }) => {
                            self.handle_book_update(asset_id, book).await;
                        }
                        Some(ArbWsEvent::PriceUpdate { asset_id, best_bid, best_ask }) => {
                            self.handle_price_update(asset_id, best_bid, best_ask).await;
                        }
                        Some(ArbWsEvent::MarketResolved { winning_asset_id, winning_outcome, .. }) => {
                            info!("🏁 WS: Market resolved — winner: {}", winning_outcome);
                            self.trigger_redeem_by_asset(&winning_asset_id, &winning_outcome).await;
                        }
                        Some(ArbWsEvent::Connected(c)) => {
                            self.ws_connected = c;
                            if c { info!("🔌 WS: Connected to CLOB market channel"); }
                            else { warn!("🔌 WS: Disconnected — will auto-reconnect"); }
                        }
                        None => { error!("WS channel closed — book watcher died"); break; }
                    }
                }
                r = self.sweep_rx.recv() => {
                    if let Some(r) = r { self.handle_sweep_result(r); }
                }
                r = self.redeem_rx.recv() => {
                    if let Some(r) = r { self.handle_redeem_result(r); }
                }
                _ = scan_interval.tick() => {
                    if self.shutdown.load(Ordering::Relaxed) {
                        info!("🛑 ORACLE ENGINE shutting down");
                        break;
                    }
                    self.check_and_redeem().await;
                    self.discover_and_subscribe().await;
                }
                _ = status_interval.tick() => {
                    let deployed: f64 = self.positions.iter().map(|p| p.cost_usdc).sum();
                    info!("📊 ORACLE: {} positions (${:.2} deployed) | ${:.2} avail | P&L: ${:.2} | Sweeps: {} | WS: {}",
                          self.positions.len(), deployed, self.capital_usdc,
                          self.total_pnl, self.sweeps_completed,
                          if self.ws_connected { "✅" } else { "❌" });
                }
            }
        }

        info!("🏁 ORACLE ENGINE STOPPED — Final P&L: ${:.2}, Sweeps: {}",
              self.total_pnl, self.sweeps_completed);
    }

    async fn handle_book_update(&mut self, asset_id: String, book: TokenBook) {
        self.book_cache.insert(asset_id.clone(), book);
        self.check_entry_signal(asset_id).await;
    }

    async fn handle_price_update(&mut self, asset_id: String, best_bid: f64, best_ask: f64) {
        if let Some(b) = self.book_cache.get_mut(&asset_id) {
            b.best_bid = best_bid;
            b.best_ask = best_ask;
            b.updated_at = std::time::Instant::now();
        } else {
            self.book_cache.insert(asset_id.clone(), TokenBook {
                best_bid,
                best_ask,
                ask_levels: vec![BookLevel { price: best_ask, size: MIN_ORDER_SIZE }],
                total_ask_depth: MIN_ORDER_SIZE,
                updated_at: std::time::Instant::now(),
            });
        }
        self.check_entry_signal(asset_id).await;
    }

    /// Core signal check — called on every book update.
    ///
    /// Mirrors RetamzZ: enter only in the final ENTRY_WINDOW_SECS, when one side
    /// is priced >= MIN_WINNING_PRICE and the other is < 0.50 (outcome is clear).
    /// Sweep the ENTIRE ask book on the winning side up to MAX_BET_USDC.
    async fn check_entry_signal(&mut self, asset_id: String) {
        let event_slug = match self.asset_to_market.get(&asset_id) {
            Some(s) => s.clone(),
            None => return,
        };
        let market = match self.tracked_markets.get(&event_slug) {
            Some(m) => m.clone(),
            None => return,
        };
        if self.executing_slugs.contains(&market.event_slug) { return; }
        if self.failed_slugs.contains(&market.event_slug) { return; }
        if self.has_position(&market.event_slug) { return; }

        let now = chrono::Utc::now().timestamp();
        let secs_remaining = market.event_end_ts - now;
        if secs_remaining > ENTRY_WINDOW_SECS || secs_remaining < MIN_SECS_REMAINING { return; }

        let up_book = match self.book_cache.get(&market.up_token_id) {
            Some(b) => b.clone(),
            None => return,
        };
        let down_book = match self.book_cache.get(&market.down_token_id) {
            Some(b) => b.clone(),
            None => return,
        };

        // Determine winning side: the winning token is priced HIGH (near $1.00).
        // The AMM hasn't fully repriced yet — we buy before it does.
        // Only require the winning side >= MIN_WINNING_PRICE and strictly dominant.
        // Cap at MAX_SWEEP_PRICE: 10% taker fee means buying at 0.85 costs $0.935 net.
        let (winning_side, winning_book) =
            if up_book.best_ask >= MIN_WINNING_PRICE
                && up_book.best_ask <= MAX_SWEEP_PRICE
                && up_book.best_ask > down_book.best_ask
            {
                (SweptSide::Up, up_book)
            } else if down_book.best_ask >= MIN_WINNING_PRICE
                && down_book.best_ask <= MAX_SWEEP_PRICE
                && down_book.best_ask > up_book.best_ask
            {
                (SweptSide::Down, down_book)
            } else {
                let now_inst = std::time::Instant::now();
                let last = self.last_debug_log.get(&market.event_slug).copied();
                if last.map_or(true, |t| now_inst.duration_since(t).as_secs() >= 5) {
                    debug!("⏳ {} — unclear: Up={:.3} Down={:.3} ({:.0}s left)",
                           market.title, up_book.best_ask, down_book.best_ask, secs_remaining);
                    self.last_debug_log.insert(market.event_slug.clone(), now_inst);
                }
                return;
            };

        let sweep_orders = self.compute_sweep(&winning_book);
        if sweep_orders.is_empty() { return; }
        let total_tokens: f64 = sweep_orders.iter().map(|o| o.size).sum();
        let total_cost: f64 = sweep_orders.iter().map(|o| o.price * o.size).sum();
        if total_tokens < MIN_ORDER_SIZE || total_cost < 1.0 { return; }

        let side_label = match winning_side { SweptSide::Up => "UP", SweptSide::Down => "DOWN" };
        info!("🎯 ORACLE SIGNAL: {} | {} @ {:.3} | {:.0} tokens | ${:.2} cost | {:.0}s left",
              market.title, side_label, winning_book.best_ask, total_tokens, total_cost, secs_remaining);

        self.executing_slugs.insert(market.event_slug.clone());
        self.execute_sweep(&market, winning_side, sweep_orders, total_cost);
    }

    /// Compute sweep orders from a book, capped by capital limits.
    /// Takes ALL ask levels up to MAX_BET_USDC / available capital.
    fn compute_sweep(&self, book: &TokenBook) -> Vec<BookLevel> {
        if book.ask_levels.is_empty() { return Vec::new(); }
        let max_usdc = MAX_BET_USDC.min(self.capital_usdc * MAX_CAPITAL_FRACTION);
        let mut orders = Vec::new();
        let mut remaining = max_usdc;
        for level in &book.ask_levels {
            if level.price > MAX_SWEEP_PRICE { break; }
            if remaining < MIN_ORDER_SIZE * level.price { break; }
            let tokens = level.size.min(remaining / level.price);
            if tokens < MIN_ORDER_SIZE { break; }
            orders.push(BookLevel { price: level.price, size: tokens });
            remaining -= tokens * level.price;
        }
        orders
    }

    fn execute_sweep(
        &mut self,
        market: &ArbMarket,
        swept_side: SweptSide,
        orders: Vec<BookLevel>,
        estimated_cost: f64,
    ) {
        let available = self.capital_usdc.min(MAX_BET_USDC);
        if available < MIN_ORDER_SIZE {
            warn!("Insufficient capital: ${:.2}", self.capital_usdc);
            self.executing_slugs.remove(&market.event_slug);
            return;
        }
        let scale = if estimated_cost > available { available / estimated_cost } else { 1.0 };

        let token_id = match swept_side {
            SweptSide::Up => market.up_token_id.clone(),
            SweptSide::Down => market.down_token_id.clone(),
        };
        let token_hash = hash_asset_id(&token_id);
        let market_id = MarketId { token_id: token_hash, condition_id: [0u8; 32] };
        let neg_risk = market.neg_risk;

        let mut signals: Vec<TradeSignal> = Vec::new();
        let mut actual_cost = 0.0_f64;
        for level in &orders {
            let sz = level.size * scale;
            if sz < MIN_ORDER_SIZE { continue; }
            actual_cost += sz * level.price;
            signals.push(TradeSignal {
                market_id,
                side: Side::Buy,
                price: Decimal::from_f64_retain(level.price).unwrap_or(Decimal::new(50, 2)),
                size: Decimal::from_f64_retain(sz).unwrap_or(Decimal::new(5, 0)),
                order_type: OrderType::ImmediateOrCancel,
                urgency: SignalUrgency::Critical,
                expected_profit_bps: 500,
                signal_timestamp_ns: 0,
            });
        }
        if signals.is_empty() {
            self.executing_slugs.remove(&market.event_slug);
            return;
        }

        self.capital_usdc -= actual_cost;
        let side_label = match swept_side { SweptSide::Up => "UP", SweptSide::Down => "DOWN" };
        info!("🚀 SWEEPING {} {} orders on {} (${:.2} reserved)",
              signals.len(), side_label, market.title, actual_cost);

        let sweep_tx = self.sweep_tx.clone();
        let executor = self.executor.clone();
        let market_clone = market.clone();
        let event_slug = market.event_slug.clone();
        tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(
                submit_sweep_orders(
                    executor, signals, token_id, neg_risk,
                    market_clone.clone(), swept_side, event_slug.clone(), actual_cost,
                )
            ).catch_unwind().await.unwrap_or_else(|_| {
                error!("💥 PANIC in submit_sweep_orders for {}", event_slug);
                SweepResult {
                    event_slug: event_slug.clone(),
                    market: market_clone,
                    swept_side,
                    estimated_cost: actual_cost,
                    tokens_total: 0.0,
                    cost_total: 0.0,
                    orders_ok: 0,
                    orders_count: 0,
                    total_elapsed_ms: 0.0,
                    error: Some("Task panicked".to_string()),
                }
            });
            let _ = sweep_tx.send(result).await;
        });
    }

    fn handle_sweep_result(&mut self, result: SweepResult) {
        self.executing_slugs.remove(&result.event_slug);
        if let Some(ref err) = result.error {
            self.capital_usdc += result.estimated_cost;
            warn!("❌ Sweep failed for {}: {} — refunded ${:.2}",
                  result.event_slug, err, result.estimated_cost);
            return;
        }
        // Refund the difference between reserved and actually spent
        self.capital_usdc += result.estimated_cost - result.cost_total;
        let side_label = match result.swept_side { SweptSide::Up => "UP", SweptSide::Down => "DOWN" };
        info!("📊 SWEEP: {} | {}/{} filled | {:.0} {} tokens | ${:.2} | {:.0}ms",
              result.event_slug, result.orders_ok, result.orders_count,
              result.tokens_total, side_label, result.cost_total, result.total_elapsed_ms);
        if result.tokens_total > 0.0 {
            let avg = result.cost_total / result.tokens_total.max(0.001);
            info!("   ✅ {:.0} {} tokens @ avg ${:.4} | edge: ${:.4}/token",
                  result.tokens_total, side_label, avg, 1.0 - avg);
            self.positions.push(OraclePosition {
                market: result.market,
                swept_side: result.swept_side,
                tokens_bought: result.tokens_total,
                cost_usdc: result.cost_total,
                entered_at: chrono::Utc::now(),
                redeemed: false,
            });
            save_positions(&self.positions);
        } else {
            error!("❌ ALL SWEEP ORDERS FAILED for {} — marking as no-retry", result.event_slug);
            self.failed_slugs.insert(result.event_slug.clone());
            self.capital_usdc += result.estimated_cost;
        }
    }

    fn handle_redeem_result(&mut self, result: RedeemResult) {
        self.executing_slugs.remove(&result.event_slug);
        if let Some(ref err) = result.error {
            warn!("❌ Redeem failed for {}: {}", result.event_slug, err);
            return;
        }
        self.capital_usdc += result.usdc_recovered;
        self.total_pnl += result.profit;
        self.sweeps_completed += 1;
        let sign = if result.profit >= 0.0 { "+" } else { "" };
        info!("💰 REDEEM: {} | +${:.2} | {}${:.2} profit | Total P&L: ${:.2}",
              result.event_slug, result.usdc_recovered, sign, result.profit, self.total_pnl);
        self.positions.retain(|p| !(p.market.event_slug == result.event_slug && !p.redeemed));
        save_positions(&self.positions);
    }

    fn has_position(&self, event_slug: &str) -> bool {
        self.positions.iter().any(|p| p.market.event_slug == event_slug && !p.redeemed)
    }

    async fn check_and_redeem(&mut self) {
        let now = chrono::Utc::now().timestamp();
        let to_check: Vec<OraclePosition> = self.positions.iter()
            .filter(|p| {
                !p.redeemed
                    && !self.executing_slugs.contains(&p.market.event_slug)
                    && now >= p.market.event_end_ts + 60
            })
            .cloned()
            .collect();

        for pos in to_check {
            match self.check_market_resolution(&pos.market).await {
                Ok(Some(winning_outcome)) => {
                    let won = matches!(
                        (pos.swept_side, winning_outcome.as_str()),
                        (SweptSide::Up, "Up") | (SweptSide::Down, "Down")
                    );
                    let side_label = match pos.swept_side {
                        SweptSide::Up => "UP",
                        SweptSide::Down => "DOWN",
                    };
                    info!("🏆 RESOLVED: {} → {} | We swept {} | {} | Cost: ${:.2}",
                          pos.market.title, winning_outcome, side_label,
                          if won { "WON ✅" } else { "LOST ❌" }, pos.cost_usdc);

                    if won {
                        self.executing_slugs.insert(pos.market.event_slug.clone());
                        let redeem_tx = self.redeem_tx.clone();
                        let config = self.config.clone();
                        let executor = self.executor.clone();
                        let slug = pos.market.event_slug.clone();
                        let condition_id = pos.market.condition_id.clone();
                        let up_token_id = pos.market.up_token_id.clone();
                        let down_token_id = pos.market.down_token_id.clone();
                        let swept_side = pos.swept_side;
                        let tokens = pos.tokens_bought;
                        let cost = pos.cost_usdc;
                        tokio::spawn(async move {
                            let r = run_background_redeem(
                                &config, &condition_id, &up_token_id, &down_token_id,
                                swept_side, tokens, cost, &slug, &executor,
                            ).await;
                            let _ = redeem_tx.send(r).await;
                        });
                    } else {
                        // Lost — record loss and remove position
                        self.total_pnl -= pos.cost_usdc;
                        self.sweeps_completed += 1;
                        warn!("💸 LOSS: ${:.2} on {}", pos.cost_usdc, pos.market.title);
                        self.positions.retain(|p| {
                            !(p.market.event_slug == pos.market.event_slug && !p.redeemed)
                        });
                        save_positions(&self.positions);
                    }
                }
                Ok(None) => {}
                Err(e) => warn!("Resolution check failed for {}: {}", pos.market.title, e),
            }
        }
    }

    /// Called immediately on WS MarketResolved event — faster than polling.
    async fn trigger_redeem_by_asset(&mut self, winning_asset_id: &str, winning_outcome: &str) {
        let event_slug = match self.asset_to_market.get(winning_asset_id) {
            Some(s) => s.clone(),
            None => return,
        };
        if self.executing_slugs.contains(&event_slug) { return; }
        let pos = match self.positions.iter().find(
            |p| p.market.event_slug == event_slug && !p.redeemed
        ) {
            Some(p) => p.clone(),
            None => return,
        };
        let won = matches!(
            (pos.swept_side, winning_outcome),
            (SweptSide::Up, "Up") | (SweptSide::Down, "Down")
        );
        let side_label = match pos.swept_side { SweptSide::Up => "UP", SweptSide::Down => "DOWN" };
        info!("🏁 WS RESOLUTION: {} → {} | We swept {} | {}",
              event_slug, winning_outcome, side_label,
              if won { "WON ✅" } else { "LOST ❌" });

        if won {
            self.executing_slugs.insert(event_slug.clone());
            let redeem_tx = self.redeem_tx.clone();
            let config = self.config.clone();
            let executor = self.executor.clone();
            let slug = event_slug.clone();
            let condition_id = pos.market.condition_id.clone();
            let up_token_id = pos.market.up_token_id.clone();
            let down_token_id = pos.market.down_token_id.clone();
            let swept_side = pos.swept_side;
            let tokens = pos.tokens_bought;
            let cost = pos.cost_usdc;
            tokio::spawn(async move {
                // Brief delay for on-chain settlement
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                let r = run_background_redeem(
                    &config, &condition_id, &up_token_id, &down_token_id,
                    swept_side, tokens, cost, &slug, &executor,
                ).await;
                let _ = redeem_tx.send(r).await;
            });
        } else {
            self.total_pnl -= pos.cost_usdc;
            self.sweeps_completed += 1;
            warn!("💸 LOSS: ${:.2} on {}", pos.cost_usdc, pos.market.title);
            self.positions.retain(|p| !(p.market.event_slug == event_slug && !p.redeemed));
            save_positions(&self.positions);
        }
    }

    async fn check_market_resolution(&self, market: &ArbMarket) -> Result<Option<String>, String> {
        let url = format!("https://gamma-api.polymarket.com/events/{}", market.event_id);
        let resp = self.client.get(&url).send().await
            .map_err(|e| format!("Resolution check error: {}", e))?;
        if !resp.status().is_success() {
            return Err(format!("Resolution API returned {}", resp.status()));
        }
        let event: serde_json::Value = resp.json().await
            .map_err(|e| format!("Resolution JSON error: {}", e))?;
        let markets = match event.get("markets").and_then(|v| v.as_array()) {
            Some(m) => m,
            None => return Ok(None),
        };
        if let Some(m) = markets.first() {
            if !m.get("closed").and_then(|v| v.as_bool()).unwrap_or(false) {
                return Ok(None);
            }
            let prices: Vec<String> = m.get("outcomePrices")
                .and_then(|v| v.as_str())
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();
            let outcomes: Vec<String> = m.get("outcomes")
                .and_then(|v| v.as_str())
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();
            for (i, p) in prices.iter().enumerate() {
                if p.parse::<f64>().unwrap_or(0.0) > 0.99 {
                    if let Some(o) = outcomes.get(i) {
                        return Ok(Some(o.clone()));
                    }
                }
            }
        }
        Ok(None)
    }

    async fn discover_and_subscribe(&mut self) {
        let now = chrono::Utc::now().timestamp();
        let markets = match self.scan_markets().await {
            Ok(m) => m,
            Err(e) => { warn!("Market scan failed: {}", e); return; }
        };

        let mut new_ids: Vec<String> = Vec::new();
        for market in &markets {
            if market.event_start_ts > now + PRE_MARKET_LEAD_SECS { continue; }
            if market.event_end_ts < now { continue; }
            let slug = market.event_slug.clone();
            if !self.tracked_markets.contains_key(&slug) {
                info!("📡 New market: {} (ends in {:.0}m)",
                      market.title,
                      (market.event_end_ts - now) as f64 / 60.0);
                self.asset_to_market.insert(market.up_token_id.clone(), slug.clone());
                self.asset_to_market.insert(market.down_token_id.clone(), slug.clone());
                self.tracked_markets.insert(slug.clone(), market.clone());
            }
            for token_id in [&market.up_token_id, &market.down_token_id] {
                if !self.ws_subscribed.contains(token_id) {
                    new_ids.push(token_id.clone());
                    self.ws_subscribed.insert(token_id.clone());
                }
            }
        }
        if !new_ids.is_empty() {
            debug!("📡 Subscribing to {} new asset IDs", new_ids.len());
            let _ = self.ws_cmd_tx.send(WsCommand::Subscribe(new_ids)).await;
        }

        // Unsubscribe and drop expired markets (ended > 5 min ago, no open position)
        let expired_slugs: Vec<String> = self.tracked_markets.values()
            .filter(|m| m.event_end_ts < now - 300 && !self.has_position(&m.event_slug))
            .map(|m| m.event_slug.clone())
            .collect();
        for slug in &expired_slugs {
            if let Some(m) = self.tracked_markets.remove(slug) {
                let ids = vec![m.up_token_id.clone(), m.down_token_id.clone()];
                self.asset_to_market.remove(&m.up_token_id);
                self.asset_to_market.remove(&m.down_token_id);
                self.ws_subscribed.remove(&m.up_token_id);
                self.ws_subscribed.remove(&m.down_token_id);
                self.book_cache.remove(&m.up_token_id);
                self.book_cache.remove(&m.down_token_id);
                let _ = self.ws_cmd_tx.send(WsCommand::Unsubscribe(ids)).await;
                debug!("🗑️  Dropped expired market: {}", slug);
            }
        }
    }

    async fn scan_markets(&mut self) -> Result<Vec<ArbMarket>, String> {
        let mut all_markets = Vec::new();
        for series in SERIES_SLUGS {
            match self.fetch_series_events(series).await {
                Ok(mut markets) => all_markets.append(&mut markets),
                Err(e) => warn!("Failed to fetch series {}: {}", series, e),
            }
        }
        Ok(all_markets)
    }

    async fn fetch_series_events(&mut self, series_slug: &str) -> Result<Vec<ArbMarket>, String> {
        // Step 1: GET /series?slug=X&active=true — lightweight stubs with event IDs
        let series_url = format!(
            "https://gamma-api.polymarket.com/series?slug={}&active=true",
            series_slug
        );
        let resp = self.client.get(&series_url).send().await
            .map_err(|e| format!("Series API error: {}", e))?;
        if !resp.status().is_success() {
            return Err(format!("Series API returned {}", resp.status()));
        }
        let series_list: Vec<serde_json::Value> = resp.json().await
            .map_err(|e| format!("Series JSON error: {}", e))?;

        let series = match series_list.first() {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };
        let events = match series.get("events").and_then(|v| v.as_array()) {
            Some(e) => e,
            None => return Ok(Vec::new()),
        };

        let now = chrono::Utc::now().timestamp();

        // Collect non-expired event IDs, sorted by endDate ascending (soonest first)
        let mut candidates: Vec<(String, i64)> = Vec::new();
        for event in events {
            let end_ts = event.get("endDate")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|d| d.timestamp())
                .unwrap_or(i64::MAX);
            if end_ts < now - 300 { continue; } // ended > 5 min ago
            let id = event.get("id")
                .and_then(|v| v.as_str().map(|s| s.to_string())
                    .or_else(|| v.as_u64().map(|n| n.to_string())))
                .unwrap_or_default();
            if id.is_empty() { continue; }
            candidates.push((id, end_ts));
        }
        candidates.sort_by_key(|(_, ts)| *ts);
        // Keep only the next 8 events (covers ~2 hours of 15m markets)
        candidates.truncate(8);

        if candidates.is_empty() { return Ok(Vec::new()); }

        // Step 2: Fetch full event detail for uncached events
        let asset = if series_slug.starts_with("btc") { ArbAsset::BTC }
                    else if series_slug.starts_with("eth") { ArbAsset::ETH }
                    else if series_slug.starts_with("sol") { ArbAsset::SOL }
                    else { ArbAsset::XRP };

        let mut markets = Vec::new();
        let mut uncached: Vec<String> = Vec::new();

        for (id, _) in &candidates {
            if let Some(cached) = self.event_detail_cache.get(id) {
                for m in cached {
                    if m.event_end_ts > now - 300 { markets.push(m.clone()); }
                }
            } else {
                uncached.push(id.clone());
            }
        }

        // Fetch uncached events concurrently
        let fetch_futures: Vec<_> = uncached.iter().map(|event_id| {
            let url = format!("https://gamma-api.polymarket.com/events/{}", event_id);
            let client = self.client.clone();
            let eid = event_id.clone();
            async move {
                let resp = client.get(&url).send().await.ok()?;
                if !resp.status().is_success() { return None; }
                let val = resp.json::<serde_json::Value>().await.ok()?;
                Some((eid, val))
            }
        }).collect();

        let results = futures_util::future::join_all(fetch_futures).await;

        for result in results.into_iter().flatten() {
            let (event_id, event) = result;
            if let Some(m) = parse_event_to_market(&event, &event_id, asset) {
                self.event_detail_cache.insert(event_id.clone(), vec![m.clone()]);
                if m.event_end_ts > now - 300 { markets.push(m); }
            }
        }

        Ok(markets)
    }
}

// ---------------------------------------------------------------------------
// Parse a Gamma API event JSON into an ArbMarket
// ---------------------------------------------------------------------------

fn parse_event_to_market(event: &serde_json::Value, event_id: &str, asset: ArbAsset) -> Option<ArbMarket> {
    let event_slug = event.get("slug").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let title = event.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let start_ts = event.get("startDate")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.timestamp()).unwrap_or(0);
    let end_ts = event.get("endDate")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.timestamp()).unwrap_or(0);

    let inner_markets = event.get("markets").and_then(|v| v.as_array())?;

    let mut up_token_id = String::new();
    let mut down_token_id = String::new();
    let mut condition_id = String::new();
    let mut neg_risk = false;
    let mut fee_bps: u32 = 100;

    for m in inner_markets {
        let outcomes: Vec<String> = m.get("outcomes")
            .and_then(|v| v.as_str())
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        let token_ids: Vec<String> = m.get("clobTokenIds")
            .and_then(|v| v.as_str())
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        if outcomes.len() < 2 || token_ids.len() < 2 { continue; }

        condition_id = m.get("conditionId").and_then(|v| v.as_str()).unwrap_or("").to_string();
        neg_risk = m.get("negRisk").and_then(|v| v.as_bool()).unwrap_or(false);
        fee_bps = m.get("takerBaseFee")
            .or_else(|| m.get("makerBaseFee"))
            .and_then(|v| v.as_u64())
            .map(|f| f as u32)
            .unwrap_or(100);

        for (i, outcome) in outcomes.iter().enumerate() {
            if outcome.eq_ignore_ascii_case("Up") {
                up_token_id = token_ids.get(i).cloned().unwrap_or_default();
            } else if outcome.eq_ignore_ascii_case("Down") {
                down_token_id = token_ids.get(i).cloned().unwrap_or_default();
            }
        }
    }

    if up_token_id.is_empty() || down_token_id.is_empty() { return None; }

    Some(ArbMarket {
        event_id: event_id.to_string(),
        event_slug,
        title,
        asset,
        up_token_id,
        down_token_id,
        condition_id,
        neg_risk,
        up_fee_bps: fee_bps,
        down_fee_bps: fee_bps,
        tick_size: 0.01,
        min_size: 5.0,
        event_start_ts: start_ts,
        event_end_ts: end_ts,
    })
}

// ---------------------------------------------------------------------------
// Background task: submit sweep orders
// ---------------------------------------------------------------------------

async fn submit_sweep_orders(
    executor: Arc<OrderExecutor>,
    signals: Vec<TradeSignal>,
    token_id: String,
    neg_risk: bool,
    market: ArbMarket,
    swept_side: SweptSide,
    event_slug: String,
    estimated_cost: f64,
) -> SweepResult {
    let exec_start = std::time::Instant::now();
    let orders_count = signals.len();
    let fee_bps = match swept_side {
        SweptSide::Up => market.up_fee_bps,
        SweptSide::Down => market.down_fee_bps,
    };

    info!("   📝 [BG] Signing {} sweep orders for {}...", orders_count, event_slug);

    // Sign all orders concurrently
    let sign_futures: Vec<_> = signals.iter().map(|sig| {
        executor.signer().prepare_order_full(
            sig.market_id, sig.side, sig.price, sig.size,
            sig.order_type, TimeInForce::IOC, 0,
            token_id.clone(), neg_risk, fee_bps,
        )
    }).collect();

    let signed_results = futures_util::future::join_all(sign_futures).await;
    let mut signed_ok: Vec<PreparedOrder> = Vec::new();
    for res in signed_results {
        if let Ok(order) = res { signed_ok.push(order); }
    }

    if signed_ok.is_empty() {
        return SweepResult {
            event_slug, market, swept_side, estimated_cost,
            tokens_total: 0.0, cost_total: 0.0,
            orders_ok: 0, orders_count,
            total_elapsed_ms: exec_start.elapsed().as_secs_f64() * 1000.0,
            error: Some("All signing failed".to_string()),
        };
    }

    // Submit in batches of 15 (CLOB limit)
    const BATCH_LIMIT: usize = 15;
    let mut orders_ok = 0u32;
    let mut tokens_total = 0.0_f64;
    let mut cost_total = 0.0_f64;
    let side_label = match swept_side { SweptSide::Up => "Up", SweptSide::Down => "Down" };

    // Collect (order_id, req_size, req_price, batch_index) for matched orders to poll
    let mut matched_order_ids: Vec<(String, f64, f64)> = Vec::new();

    for (batch_idx, chunk) in signed_ok.chunks(BATCH_LIMIT).enumerate() {
        if batch_idx > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        let batch: Vec<PreparedOrder> = chunk.to_vec();
        let results = executor.submit_orders_batch(&batch).await;
        for (k, result) in results.into_iter().enumerate() {
            if k >= chunk.len() { break; }
            let req_size = chunk[k].size.to_f64().unwrap_or(0.0);
            let req_price = chunk[k].price.to_f64().unwrap_or(0.0);
            match result {
                Ok(fill) => {
                    orders_ok += 1;
                    // Polymarket batch POST always returns takingAmount=0 for FAK orders.
                    // Queue matched orders for a follow-up GET /order/{id} poll.
                    if !fill.order_id.is_empty() {
                        info!("   📬 {} #{}: queued for fill poll → {} [{}]",
                              side_label, k + 1, fill.order_id, fill.status);
                        matched_order_ids.push((fill.order_id.clone(), req_size, req_price));
                    } else {
                        warn!("   ⚠️  {} #{}: accepted but no order_id returned @ {:.3}",
                              side_label, k + 1, req_price);
                    }
                }
                Err(ref e) => {
                    warn!("   ❌ {} #{} failed: {}", side_label, k + 1, e);
                }
            }
        }
    }

    // Poll GET /order/{orderId} for each matched order to get actual fill amounts.
    // Polymarket typically settles FAK fills within ~500ms.
    if !matched_order_ids.is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        let rest_url = executor.rest_url();
        let http = executor.http_client();
        for (order_id, req_size, req_price) in &matched_order_ids {
            let url = format!("{}/data/order/{}", rest_url, order_id);
            match http.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<serde_json::Value>().await {
                        Ok(v) => {
                            // GET /order response: sizeFilled (decimal string, human-readable tokens)
                            // and price (decimal string)
                            let size_filled = v["sizeFilled"].as_str()
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap_or(0.0);
                            let price_used = v["price"].as_str()
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap_or(*req_price);
                            let status = v["status"].as_str().unwrap_or("UNKNOWN");
                            let cost = size_filled * price_used;
                            tokens_total += size_filled;
                            cost_total += cost;
                            info!("   ✅ {} fill: {:.1} tokens @ {:.3} = ${:.2} [{}] ({})",
                                  side_label, size_filled, price_used, cost, status, order_id);
                        }
                        Err(e) => {
                            warn!("   ⚠️  fill poll parse error for {}: {} — using req size {:.1}",
                                  order_id, e, req_size);
                            tokens_total += req_size;
                            cost_total += req_size * req_price;
                        }
                    }
                }
                Ok(resp) => {
                    warn!("   ⚠️  fill poll {} returned {}", order_id, resp.status());
                    tokens_total += req_size;
                    cost_total += req_size * req_price;
                }
                Err(e) => {
                    warn!("   ⚠️  fill poll network error for {}: {} — using req size {:.1}",
                          order_id, e, req_size);
                    tokens_total += req_size;
                    cost_total += req_size * req_price;
                }
            }
        }
    }

    let total_elapsed_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
    info!("   [BG] Sweep complete: {}/{} orders filled | {:.0} tokens | ${:.2} | {:.0}ms",
          orders_ok, orders_count, tokens_total, cost_total, total_elapsed_ms);

    SweepResult {
        event_slug, market, swept_side, estimated_cost,
        tokens_total, cost_total, orders_ok, orders_count,
        total_elapsed_ms,
        error: None,
    }
}

// ---------------------------------------------------------------------------
// Background task: redeem winning tokens
// ---------------------------------------------------------------------------

async fn run_background_redeem(
    config: &Config,
    condition_id: &str,
    _up_token_id: &str,
    _down_token_id: &str,
    _swept_side: SweptSide,
    tokens: f64,
    cost: f64,
    event_slug: &str,
    _executor: &Arc<OrderExecutor>,
) -> RedeemResult {
    // Tokens are held by the PROXY wallet (maker=proxy on all orders).
    // Direct on-chain redeemPositions from the EOA would recover $0.
    // Must use the Polymarket relayer API which executes on behalf of the proxy.
    use ethers::prelude::*;

    info!("   🔄 [BG] Redeeming {:.0} tokens for {} via relayer", tokens, event_slug);

    // --- Build redeemPositions calldata ---
    // CTF contract: 0x4D97DcD97Ec945F40CF65F87097aCe5EA0476045 (embedded in calldata via ABI encode)
    let usdc_e: Address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        .parse().expect("USDC address");

    let condition_bytes: [u8; 32] = match hex::decode(condition_id.trim_start_matches("0x")) {
        Ok(b) if b.len() == 32 => b.try_into().unwrap(),
        _ => return RedeemResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            error: Some(format!("Invalid condition_id: {}", condition_id)),
        },
    };
    let parent_collection = [0u8; 32];

    let fn_selector = &ethers::core::utils::keccak256(
        b"redeemPositions(address,bytes32,bytes32,uint256[])"
    )[..4];
    let encoded_params = ethers::abi::encode(&[
        ethers::abi::Token::Address(usdc_e),
        ethers::abi::Token::FixedBytes(parent_collection.to_vec()),
        ethers::abi::Token::FixedBytes(condition_bytes.to_vec()),
        ethers::abi::Token::Array(vec![
            ethers::abi::Token::Uint(U256::from(1u64)),
            ethers::abi::Token::Uint(U256::from(2u64)),
        ]),
    ]);
    let mut calldata = fn_selector.to_vec();
    calldata.extend_from_slice(&encoded_params);
    let calldata_hex = format!("0x{}", hex::encode(&calldata));

    // --- Proxy transaction constants (Polygon mainnet) ---
    // Source: https://github.com/Polymarket/builder-relayer-client/blob/main/src/config/index.ts
    let proxy_factory = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052";
    let relay_hub     = "0xD216153c06E857cD7f72665E0aF1d7D82172F494";
    let relayer_url   = "https://relayer-v2.polymarket.com";

    // --- Parse EOA wallet for signing ---
    let wallet: LocalWallet = match config.polymarket.private_key.parse::<LocalWallet>() {
        Ok(w) => w.with_chain_id(137u64),
        Err(e) => return RedeemResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            error: Some(format!("Wallet parse error: {}", e)),
        },
    };
    let eoa = format!("{:?}", wallet.address()); // "0x..." lowercase

    // --- Fetch relay payload (nonce + relay address) ---
    let http_client = reqwest::Client::new();
    let relay_payload: serde_json::Value = match async {
        let r = http_client
            .get(format!("{}/relay-payload", relayer_url))
            .query(&[("address", eoa.as_str()), ("type", "proxy")])
            .send().await?;
        r.json::<serde_json::Value>().await
    }.await
    {
        Ok(v) => v,
        Err(e) => return RedeemResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            error: Some(format!("relay-payload fetch failed: {}", e)),
        },
    };

    let nonce = match relay_payload.get("nonce").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => return RedeemResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            error: Some(format!("relay-payload missing nonce: {}", relay_payload)),
        },
    };
    let relay_addr = match relay_payload.get("address").and_then(|v| v.as_str()) {
        Some(a) => a.to_string(),
        None => return RedeemResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            error: Some(format!("relay-payload missing address: {}", relay_payload)),
        },
    };

    // --- Build struct hash ---
    // keccak256("rlx:" + from + to + data + txFee(0) + gasPrice(0) + gasLimit + nonce + relayHub + relay)
    // Source: https://github.com/Polymarket/builder-relayer-client/blob/main/src/builder/proxy.ts
    let gas_limit: u64 = 10_000_000;
    let tx_fee: u64 = 0;
    let gas_price: u64 = 0;

    fn pad_addr(addr: &str) -> [u8; 20] {
        let s = addr.trim_start_matches("0x");
        let b = hex::decode(s).unwrap_or_default();
        let mut out = [0u8; 20];
        let start = 20usize.saturating_sub(b.len());
        out[start..].copy_from_slice(&b[..b.len().min(20)]);
        out
    }
    fn pad_u256(n: u64) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[24..].copy_from_slice(&n.to_be_bytes());
        out
    }
    fn pad_u256_str(s: &str) -> [u8; 32] {
        let n: u64 = s.parse().unwrap_or(0);
        pad_u256(n)
    }

    let mut preimage: Vec<u8> = Vec::new();
    preimage.extend_from_slice(b"rlx:");
    preimage.extend_from_slice(&pad_addr(&eoa));
    preimage.extend_from_slice(&pad_addr(proxy_factory));
    preimage.extend_from_slice(&calldata);
    preimage.extend_from_slice(&pad_u256(tx_fee));
    preimage.extend_from_slice(&pad_u256(gas_price));
    preimage.extend_from_slice(&pad_u256(gas_limit));
    preimage.extend_from_slice(&pad_u256_str(&nonce));
    preimage.extend_from_slice(&pad_addr(relay_hub));
    preimage.extend_from_slice(&pad_addr(&relay_addr));

    let struct_hash = ethers::core::utils::keccak256(&preimage);

    // Sign the struct hash directly (not EIP-191 prefixed — proxy.ts uses signMessage on raw hash)
    let signature = match wallet.sign_hash(ethers::types::H256::from(struct_hash)) {
        Ok(sig) => format!("0x{}", sig),
        Err(e) => return RedeemResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            error: Some(format!("Signing failed: {}", e)),
        },
    };

    // --- Build and submit the proxy transaction request ---
    let request_body = serde_json::json!({
        "type": "PROXY",
        "from": eoa,
        "to": proxy_factory,
        "proxyWallet": config.polymarket.proxy_address,
        "data": calldata_hex,
        "nonce": nonce,
        "signature": signature,
        "signatureParams": {
            "gasPrice": gas_price.to_string(),
            "gasLimit": gas_limit.to_string(),
            "relayerFee": tx_fee.to_string(),
            "relayHub": relay_hub,
            "relay": relay_addr,
        },
        "metadata": format!("redeem {}", event_slug),
    });

    // Auth headers — same HMAC pattern as CLOB API
    let timestamp = chrono::Utc::now().timestamp_millis().to_string();
    let body_str = request_body.to_string();
    let hmac_msg = format!("{}{}{}{}", timestamp, "POST", "/submit", body_str);
    use base64::Engine as _;
    let hmac_key = base64::engine::general_purpose::STANDARD.decode(&config.polymarket.api_secret).unwrap_or_default();
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&hmac_key)
        .unwrap_or_else(|_| <Hmac<Sha256> as Mac>::new_from_slice(b"key").unwrap());
    mac.update(hmac_msg.as_bytes());
    let sig_bytes = mac.finalize().into_bytes();
    let builder_sig = base64::engine::general_purpose::STANDARD.encode(sig_bytes);

    let resp = http_client
        .post(format!("{}/submit", relayer_url))
        .header("Content-Type", "application/json")
        .header("POLY_BUILDER_API_KEY", &config.polymarket.api_key)
        .header("POLY_BUILDER_TIMESTAMP", &timestamp)
        .header("POLY_BUILDER_PASSPHRASE", &config.polymarket.api_passphrase)
        .header("POLY_BUILDER_SIGNATURE", &builder_sig)
        .body(body_str)
        .send().await;

    let resp = match resp {
        Ok(r) => r,
        Err(e) => return RedeemResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            error: Some(format!("Relayer submit failed: {}", e)),
        },
    };

    let status = resp.status();
    let resp_body: serde_json::Value = resp.json().await.unwrap_or_default();

    if !status.is_success() {
        return RedeemResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            error: Some(format!("Relayer returned {}: {}", status, resp_body)),
        };
    }

    let tx_id = resp_body.get("transactionID")
        .or_else(|| resp_body.get("transactionId"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    info!("   🔗 Relayer tx submitted: {} — polling for confirmation...", tx_id);

    // Poll for confirmation (up to 60s)
    for _ in 0..30 {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let poll: serde_json::Value = match async {
            let r = http_client
                .get(format!("{}/transaction", relayer_url))
                .query(&[("id", tx_id.as_str())])
                .send().await?;
            r.json::<serde_json::Value>().await
        }.await
        {
            Ok(v) => v,
            Err(_) => continue,
        };

        let txns = poll.as_array().cloned().unwrap_or_default();
        if let Some(txn) = txns.first() {
            let state = txn.get("state").and_then(|v| v.as_str()).unwrap_or("");
            let tx_hash = txn.get("transactionHash").and_then(|v| v.as_str()).unwrap_or("");
            match state {
                "STATE_CONFIRMED" | "STATE_MINED" => {
                    let usdc_recovered = tokens;
                    let profit = usdc_recovered - cost;
                    info!("   ✅ Redeem confirmed: {} | +${:.2} recovered | profit: ${:.2}",
                          tx_hash, usdc_recovered, profit);
                    return RedeemResult {
                        event_slug: event_slug.to_string(),
                        usdc_recovered, profit, error: None,
                    };
                }
                "STATE_FAILED" | "STATE_INVALID" => {
                    return RedeemResult {
                        event_slug: event_slug.to_string(),
                        usdc_recovered: 0.0, profit: 0.0,
                        error: Some(format!("Relayer tx failed: state={} hash={}", state, tx_hash)),
                    };
                }
                _ => {
                    debug!("   ⏳ Relayer tx state: {} ({})", state, tx_id);
                }
            }
        }
    }

    RedeemResult {
        event_slug: event_slug.to_string(),
        usdc_recovered: 0.0, profit: 0.0,
        error: Some(format!("Relayer tx timed out after 60s: {}", tx_id)),
    }
}
