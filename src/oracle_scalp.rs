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

use futures_util::{FutureExt, StreamExt};
use rust_decimal::Decimal;
use tokio_tungstenite::{connect_async as tungstenite_connect, tungstenite::Message as WsMessage};
use rust_decimal::prelude::ToPrimitive;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const ENTRY_WINDOW_SECS: i64 = 120;  // Enter in final 2 min — matches profitable wallet timing
const MIN_SECS_REMAINING: i64 = 10;
const MIN_WINNING_BID: f64 = 0.45;   // Winning side best_bid >= 45¢ — enter early like RetamzZ
const MAX_LOSING_BID: f64 = 0.45;    // Losing side best_bid <= 45¢ — complementary threshold
const MAX_SWEEP_PRICE: f64 = 0.97;   // Fee = 10% * 2 * min(p,1-p) → 0.6% at 0.97 → profit $0.024/token
const RESWEEP_COOLDOWN_MS: u128 = 3000; // Re-sweep same market after 3s cooldown (not permanent block)
const MAX_BET_USDC: f64 = 500.0;
const MAX_CAPITAL_FRACTION: f64 = 0.90;
const MIN_ORDER_SIZE: f64 = 1.0;   // CLOB minimum ~1 token; sizing is dynamic based on balance
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
    "btc-up-or-down-5m",
    "eth-up-or-down-5m",
    "sol-up-or-down-5m",
    "xrp-up-or-down-5m",
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
    /// Per-market last sweep attempt time — replaces permanent failed_slugs block.
    /// After RESWEEP_COOLDOWN_MS the market is eligible for another sweep.
    last_sweep_attempt: HashMap<String, std::time::Instant>,
    last_debug_log: HashMap<String, std::time::Instant>,
    sweep_rx: mpsc::Receiver<SweepResult>,
    sweep_tx: mpsc::Sender<SweepResult>,
    redeem_rx: mpsc::Receiver<RedeemResult>,
    redeem_tx: mpsc::Sender<RedeemResult>,
    event_detail_cache: HashMap<String, Vec<ArbMarket>>,
    /// Recent spot prices per asset: (timestamp, price), kept for last 90s
    spot_prices: HashMap<ArbAsset, VecDeque<(std::time::Instant, f64)>>,
    /// Binance WebSocket price feed receiver
    spot_rx: mpsc::Receiver<(ArbAsset, f64, std::time::Instant)>,
    /// Last time we reconciled positions against the data API
    last_reconcile: std::time::Instant,
    /// Set after a fill or balance error — triggers on-chain balance refresh on next scan tick
    needs_balance_refresh: bool,
    /// Cooldown: last time a signal was fired or attempted — prevents WS burst spam
    last_signal_fired: std::time::Instant,
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

        let (spot_tx, spot_rx) = mpsc::channel::<(ArbAsset, f64, std::time::Instant)>(256);
        tokio::spawn(async move {
            run_spot_watcher(spot_tx).await;
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
            last_sweep_attempt: HashMap::new(),
            last_debug_log: HashMap::new(),
            sweep_rx, sweep_tx, redeem_rx, redeem_tx,
            event_detail_cache: HashMap::new(),
            spot_prices: HashMap::new(),
            spot_rx,
            last_reconcile: std::time::Instant::now(),
            needs_balance_refresh: false,
            last_signal_fired: std::time::Instant::now() - std::time::Duration::from_secs(10),
        }
    }

    pub fn shutdown_flag(&self) -> Arc<AtomicBool> { self.shutdown.clone() }

    /// Refresh capital_usdc from on-chain USDC balance.
    /// Called after fills or balance errors to keep internal tracking in sync with reality.
    async fn refresh_onchain_balance(&mut self) {
        self.needs_balance_refresh = false;
        match self.executor.fetch_usdc_balance().await {
            Ok(balance) => {
                let deployed: f64 = self.positions.iter()
                    .filter(|p| !p.redeemed)
                    .map(|p| p.cost_usdc)
                    .sum();
                // Available capital = on-chain balance (don't double-count deployed positions
                // since that USDC is already spent on-chain)
                let old = self.capital_usdc;
                self.capital_usdc = balance;
                info!("💰 Balance refresh: on-chain ${:.2}, deployed ${:.2}, available ${:.2} (was ${:.2})",
                      balance, deployed, self.capital_usdc, old);
            }
            Err(e) => {
                warn!("⚠️  Balance refresh failed: {}", e);
            }
        }
    }

    /// Drain all pending spot price updates from the Binance WS channel into spot_prices.
    fn drain_spot_prices(&mut self) {
        let now = std::time::Instant::now();
        while let Ok((asset, price, received_at)) = self.spot_rx.try_recv() {
            let deque = self.spot_prices.entry(asset).or_insert_with(VecDeque::new);
            deque.push_back((received_at, price));
            // Keep only last 90s of data
            while deque.front().map_or(false, |(t, _)| now.duration_since(*t).as_secs() > 90) {
                deque.pop_front();
            }
        }
    }

    /// Returns spot price momentum for an asset over the last `window_secs`.
    /// Positive = price rising, Negative = price falling.
    /// Returns None if insufficient data.
    fn spot_momentum(&self, asset: ArbAsset, window_secs: u64) -> Option<f64> {
        let deque = self.spot_prices.get(&asset)?;
        if deque.len() < 2 { return None; }
        let now = std::time::Instant::now();
        let latest = deque.back().map(|(_, p)| *p)?;
        // Find oldest sample within the window
        let oldest = deque.iter()
            .find(|(t, _)| now.duration_since(*t).as_secs() <= window_secs)
            .map(|(_, p)| *p)?;
        Some((latest - oldest) / oldest * 100.0) // % change
    }

    pub async fn run(&mut self) {
        info!("🎯 ORACLE ENGINE STARTED — Capital: ${:.2}", self.capital_usdc);
        info!("   Strategy: Sweep winning side in final {}s of 15m markets", ENTRY_WINDOW_SECS);
        info!("   Max bet: ${:.0} | Win bid >= {:.2} | Lose bid <= {:.2} | Sweep <= {:.2}", MAX_BET_USDC, MIN_WINNING_BID, MAX_LOSING_BID, MAX_SWEEP_PRICE);

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
                    if self.needs_balance_refresh {
                        self.refresh_onchain_balance().await;
                    }
                }
                r = self.redeem_rx.recv() => {
                    if let Some(r) = r { self.handle_redeem_result(r); }
                }
                _ = scan_interval.tick() => {
                    if self.shutdown.load(Ordering::Relaxed) {
                        info!("🛑 ORACLE ENGINE shutting down");
                        break;
                    }
                    self.drain_spot_prices();
                    self.reconcile_positions().await;
                    self.check_and_redeem().await;
                    self.discover_and_subscribe().await;
                    self.poll_books_and_check_signals().await;
                }
                _ = status_interval.tick() => {
                    let deployed: f64 = self.positions.iter().map(|p| p.cost_usdc).sum();
                    let now_ts = chrono::Utc::now().timestamp();
                    let cached_assets = self.book_cache.len();
                    let tracked = self.tracked_markets.len();
                    // Count markets in entry window and their book data status
                    let mut in_window = 0u32;
                    let mut has_both_books = 0u32;
                    for m in self.tracked_markets.values() {
                        let secs_left = m.event_end_ts - now_ts;
                        if secs_left > 0 && secs_left <= ENTRY_WINDOW_SECS {
                            in_window += 1;
                            let has_up = self.book_cache.contains_key(&m.up_token_id);
                            let has_down = self.book_cache.contains_key(&m.down_token_id);
                            if has_up && has_down {
                                has_both_books += 1;
                                let up_b = self.book_cache.get(&m.up_token_id).unwrap();
                                let dn_b = self.book_cache.get(&m.down_token_id).unwrap();
                                debug!("📖 {} ({:.0}s left): UpBid={:.3} DownBid={:.3} asks={}+{}",
                                       m.title, secs_left, up_b.best_bid, dn_b.best_bid,
                                       up_b.ask_levels.len(), dn_b.ask_levels.len());
                            } else {
                                debug!("📖 {} ({:.0}s left): up_book={} down_book={}",
                                       m.title, secs_left, has_up, has_down);
                            }
                        }
                    }
                    info!("📊 ORACLE: {} pos (${:.2}) | ${:.2} avail | P&L: ${:.2} | Sweeps: {} | WS: {} | tracked: {} | cached: {} | in_window: {}/{} have books",
                          self.positions.len(), deployed, self.capital_usdc,
                          self.total_pnl, self.sweeps_completed,
                          if self.ws_connected { "✅" } else { "❌" },
                          tracked, cached_assets, has_both_books, in_window);
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

    /// Fetch order book from CLOB REST API for a single token.
    /// Fallback for when WS doesn't deliver book snapshots.
    async fn fetch_rest_book(&self, token_id: &str) -> Option<TokenBook> {
        let url = format!("{}/book?token_id={}", self.config.polymarket.rest_url, token_id);
        let resp = self.client.get(&url).send().await.ok()?;
        if !resp.status().is_success() { return None; }
        let body: serde_json::Value = resp.json().await.ok()?;

        let mut ask_levels = Vec::new();
        if let Some(asks) = body.get("asks").and_then(|v| v.as_array()) {
            for ask in asks {
                let price = ask.get("price").and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok()).unwrap_or(1.0);
                let size = ask.get("size").and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                if size > 0.0 {
                    ask_levels.push(BookLevel { price, size });
                }
            }
        }
        ask_levels.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));

        let best_ask = ask_levels.first().map(|l| l.price).unwrap_or(1.0);
        let total_ask_depth: f64 = ask_levels.iter().map(|l| l.size).sum();

        let best_bid = if let Some(bids) = body.get("bids").and_then(|v| v.as_array()) {
            bids.iter()
                .filter_map(|b| b.get("price").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()))
                .fold(0.0_f64, f64::max)
        } else { 0.0 };

        if ask_levels.is_empty() && best_bid == 0.0 { return None; }

        Some(TokenBook { best_bid, best_ask, ask_levels, total_ask_depth, updated_at: std::time::Instant::now() })
    }

    /// Poll REST books for markets in entry window that are missing WS book data,
    /// then run signal checks on all markets with complete book data.
    async fn poll_books_and_check_signals(&mut self) {
        let now_ts = chrono::Utc::now().timestamp();
        // Collect markets in or approaching entry window that need book data
        let markets_needing_books: Vec<ArbMarket> = self.tracked_markets.values()
            .filter(|m| {
                let secs_left = m.event_end_ts - now_ts;
                // Poll books for markets within 3 minutes of end (wider than entry window)
                secs_left > 0 && secs_left <= ENTRY_WINDOW_SECS + 60
            })
            .cloned()
            .collect();

        // Fetch missing books via REST
        for m in &markets_needing_books {
            let needs_up = !self.book_cache.contains_key(&m.up_token_id)
                || self.book_cache.get(&m.up_token_id).map_or(true, |b| b.updated_at.elapsed().as_secs() > 30);
            let needs_down = !self.book_cache.contains_key(&m.down_token_id)
                || self.book_cache.get(&m.down_token_id).map_or(true, |b| b.updated_at.elapsed().as_secs() > 30);

            if needs_up {
                if let Some(book) = self.fetch_rest_book(&m.up_token_id).await {
                    debug!("📡 REST book: {} UP bid={:.3} asks={}", m.title, book.best_bid, book.ask_levels.len());
                    self.book_cache.insert(m.up_token_id.clone(), book);
                }
            }
            if needs_down {
                if let Some(book) = self.fetch_rest_book(&m.down_token_id).await {
                    debug!("📡 REST book: {} DOWN bid={:.3} asks={}", m.title, book.best_bid, book.ask_levels.len());
                    self.book_cache.insert(m.down_token_id.clone(), book);
                }
            }
        }

        // Run signal check on all markets in entry window (use up_token_id only — 
        // check_entry_signal looks up the market from either token and evaluates both sides)
        let check_ids: Vec<String> = markets_needing_books.iter()
            .filter(|m| {
                let secs_left = m.event_end_ts - now_ts;
                secs_left > 0 && secs_left <= ENTRY_WINDOW_SECS
            })
            .map(|m| m.up_token_id.clone())
            .collect();
        for asset_id in check_ids {
            // Bypass the 2s WS-burst cooldown for the periodic scan
            self.last_signal_fired = std::time::Instant::now() - std::time::Duration::from_secs(10);
            self.check_entry_signal(asset_id).await;
        }
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
        // Block new sweeps while ANY sweep is in-flight — on-chain balance is shared
        if !self.executing_slugs.is_empty() { return; }
        // Per-market cooldown: allow re-sweep after RESWEEP_COOLDOWN_MS (not a permanent block)
        if let Some(last) = self.last_sweep_attempt.get(&market.event_slug) {
            if last.elapsed().as_millis() < RESWEEP_COOLDOWN_MS { return; }
        }
        // Global cooldown: don't re-evaluate signals within 2s of the last attempt
        if self.last_signal_fired.elapsed().as_millis() < 2000 { return; }

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

        // Determine winning side using best_bid (not best_ask).
        // In binary markets, the winning token has high bids (near $1), losing has low bids (near $0).
        // We then sweep the winning side's ask book up to MAX_SWEEP_PRICE.
        // Fee formula: fee_rate = baseFee(10%) * 2 * min(p, 1-p) — very low at high prices.
        // Spot price momentum check: require price to be moving in the oracle direction.
        let momentum = self.spot_momentum(market.asset, 30);
        let momentum_ok = |side: SweptSide| -> bool {
            match momentum {
                None => true, // no data yet — allow
                Some(pct) => match side {
                    SweptSide::Up   => pct >= -0.02,
                    SweptSide::Down => pct <= 0.02,
                },
            }
        };

        let (winning_side, winning_book) =
            if up_book.best_bid >= MIN_WINNING_BID
                && down_book.best_bid <= MAX_LOSING_BID
                && momentum_ok(SweptSide::Up)
            {
                (SweptSide::Up, up_book)
            } else if down_book.best_bid >= MIN_WINNING_BID
                && up_book.best_bid <= MAX_LOSING_BID
                && momentum_ok(SweptSide::Down)
            {
                (SweptSide::Down, down_book)
            } else {
                let now_inst = std::time::Instant::now();
                let last = self.last_debug_log.get(&market.event_slug).copied();
                if last.map_or(true, |t| now_inst.duration_since(t).as_secs() >= 5) {
                    debug!("⏳ {} — unclear: UpBid={:.3} DownBid={:.3} ({:.0}s left)",
                           market.title, up_book.best_bid, down_book.best_bid, secs_remaining);
                    self.last_debug_log.insert(market.event_slug.clone(), now_inst);
                }
                return;
            };

        // Early capital check — avoid log spam when capital is exhausted
        let available = self.capital_usdc.min(MAX_BET_USDC);
        if available < MIN_ORDER_SIZE {
            return; // silently skip — no point logging hundreds of times
        }

        let (total_tokens, total_cost) = self.compute_sweep(&winning_book);
        if total_tokens < MIN_ORDER_SIZE || total_cost < 0.50 { return; }

        // Insert slug FIRST to block all subsequent WS-triggered evaluations
        self.executing_slugs.insert(market.event_slug.clone());
        self.last_sweep_attempt.insert(market.event_slug.clone(), std::time::Instant::now());
        self.last_signal_fired = std::time::Instant::now();

        let side_label = match winning_side { SweptSide::Up => "UP", SweptSide::Down => "DOWN" };
        let mom_str = momentum.map_or("n/a".to_string(), |m| format!("{:+.3}%", m));
        info!("🎯 ORACLE SIGNAL: {} | {} @ {:.3} | {:.0} tokens | ${:.2} cost | {:.0}s left | spot {}",
              market.title, side_label, winning_book.best_ask, total_tokens, total_cost, secs_remaining, mom_str);

        self.execute_sweep(&market, winning_side, total_tokens, total_cost);
    }

    /// Compute total tokens available on the ask book up to MAX_SWEEP_PRICE,
    /// capped by capital limits. Returns (total_tokens, estimated_cost).
    fn compute_sweep(&self, book: &TokenBook) -> (f64, f64) {
        if book.ask_levels.is_empty() { return (0.0, 0.0); }
        let max_usdc = MAX_BET_USDC.min(self.capital_usdc * MAX_CAPITAL_FRACTION);
        let mut tokens = 0.0_f64;
        let mut cost = 0.0_f64;
        for level in &book.ask_levels {
            if level.price > MAX_SWEEP_PRICE { break; }
            let remaining = max_usdc - cost;
            if remaining < 0.50 { break; } // need at least 50¢ to add a level
            let t = level.size.min(remaining / level.price);
            if t < 0.5 && tokens == 0.0 { break; } // first level must have some depth
            tokens += t;
            cost += t * level.price;
        }
        (tokens, cost)
    }

    fn execute_sweep(
        &mut self,
        market: &ArbMarket,
        swept_side: SweptSide,
        total_tokens: f64,
        _estimated_cost: f64,
    ) {
        let available = self.capital_usdc.min(MAX_BET_USDC);
        if available < MIN_ORDER_SIZE {
            warn!("Insufficient capital: ${:.2}", self.capital_usdc);
            self.executing_slugs.remove(&market.event_slug);
            self.failed_slugs.insert(market.event_slug.clone());
            return;
        }

        let token_id = match swept_side {
            SweptSide::Up => market.up_token_id.clone(),
            SweptSide::Down => market.down_token_id.clone(),
        };
        let token_hash = hash_asset_id(&token_id);
        let market_id = MarketId { token_id: token_hash, condition_id: [0u8; 32] };
        let neg_risk = market.neg_risk;

        // Send ONE FAK order at MAX_SWEEP_PRICE for the full token amount.
        // The CLOB will fill against all available asks up to this price.
        // Per-level orders fail because the book moves between WS snapshot and order arrival.
        let sz_rounded = (total_tokens * 10000.0).round() / 10000.0;
        let actual_cost = sz_rounded * MAX_SWEEP_PRICE; // worst-case cost reservation
        let actual_cost = actual_cost.min(available * MAX_CAPITAL_FRACTION);
        let sz_capped = actual_cost / MAX_SWEEP_PRICE;
        let sz_final = (sz_capped * 10000.0).round() / 10000.0;
        if sz_final < MIN_ORDER_SIZE {
            self.executing_slugs.remove(&market.event_slug);
            self.failed_slugs.insert(market.event_slug.clone());
            return;
        }

        let signals = vec![TradeSignal {
            market_id,
            side: Side::Buy,
            price: Decimal::from_f64_retain(MAX_SWEEP_PRICE).unwrap_or(Decimal::new(97, 2)),
            size: Decimal::from_f64_retain(sz_final).unwrap_or(Decimal::new(5, 0)),
            order_type: OrderType::ImmediateOrCancel,
            urgency: SignalUrgency::Critical,
            expected_profit_bps: 500,
            signal_timestamp_ns: 0,
        }];

        let reserved = sz_final * MAX_SWEEP_PRICE;
        self.capital_usdc -= reserved;
        let side_label = match swept_side { SweptSide::Up => "UP", SweptSide::Down => "DOWN" };
        info!("🚀 SWEEP {} @ {:.2} | {:.0} tokens | ${:.2} reserved | {}",
              side_label, MAX_SWEEP_PRICE, sz_final, reserved, market.title);

        let sweep_tx = self.sweep_tx.clone();
        let executor = self.executor.clone();
        let market_clone = market.clone();
        let event_slug = market.event_slug.clone();
        tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(
                submit_sweep_orders(
                    executor, signals, token_id, neg_risk,
                    market_clone.clone(), swept_side, event_slug.clone(), reserved,
                )
            ).catch_unwind().await.unwrap_or_else(|_| {
                error!("💥 PANIC in submit_sweep_orders for {}", event_slug);
                SweepResult {
                    event_slug: event_slug.clone(),
                    market: market_clone,
                    swept_side,
                    estimated_cost: reserved,
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
        let side_label = match result.swept_side { SweptSide::Up => "UP", SweptSide::Down => "DOWN" };
        info!("📊 SWEEP: {} | {}/{} filled | {:.0} {} tokens | ${:.2} | {:.0}ms",
              result.event_slug, result.orders_ok, result.orders_count,
              result.tokens_total, side_label, result.cost_total, result.total_elapsed_ms);
        if result.tokens_total > 0.0 {
            // Successful fill — refresh on-chain balance to keep capital tracking accurate
            self.needs_balance_refresh = true;
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
            // All orders failed — refresh on-chain balance to detect if a previous
            // fill actually spent our USDC (the CLOB returns "not enough balance"
            // but result.error is None because individual failures aren't propagated)
            self.needs_balance_refresh = true;
            // Full refund — nothing was actually spent (if balance refresh shows
            // otherwise, it will correct capital_usdc)
            self.capital_usdc += result.estimated_cost;
            warn!("⚠️  All sweep orders failed for {} — will retry after cooldown", result.event_slug);
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

    /// Reconcile internal positions against the Polymarket data API.
    /// If a position's token no longer appears in on-chain holdings (e.g. sold via UI),
    /// drop it and refund the reserved capital so the bot can trade again.
    async fn reconcile_positions(&mut self) {
        // Throttle: only check every 30 seconds
        let now = std::time::Instant::now();
        if now.duration_since(self.last_reconcile).as_secs() < 30 { return; }
        self.last_reconcile = now;

        let now_utc = chrono::Utc::now();
        let now_ts = now_utc.timestamp();
        let active: Vec<_> = self.positions.iter()
            .filter(|p| !p.redeemed && !self.executing_slugs.contains(&p.market.event_slug))
            // Grace period: skip positions younger than 5 minutes — data API may not have indexed them yet
            .filter(|p| (now_utc - p.entered_at).num_seconds() > 300)
            // CRITICAL: skip positions in markets that have already ended.
            // After resolution, winning tokens disappear from the data API (they are settled
            // on-chain). This is NOT an external sale — check_and_redeem handles these.
            // Reconciling them here would drop the position before we can claim.
            .filter(|p| p.market.event_end_ts > now_ts)
            .cloned()
            .collect();
        if active.is_empty() { return; }

        // Query data API for actual token holdings
        let addr = self.executor.wallet_address();
        let url = format!(
            "https://data-api.polymarket.com/positions?user={}&sizeThreshold=0.1",
            addr
        );
        let resp = match self.client.get(&url).send().await {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                debug!("Reconcile: data-api returned {}", r.status());
                return;
            }
            Err(e) => {
                debug!("Reconcile: data-api error: {}", e);
                return;
            }
        };
        let held_assets: HashSet<String> = match resp.json::<serde_json::Value>().await {
            Ok(val) => {
                let arr = val.as_array()
                    .or_else(|| val.get("data").and_then(|d| d.as_array()));
                match arr {
                    Some(positions) => positions.iter()
                        .filter_map(|p| {
                            let size = p["size"].as_f64().unwrap_or(0.0);
                            if size > 0.1 {
                                p["asset"].as_str().map(|s| s.to_string())
                            } else {
                                None
                            }
                        })
                        .collect(),
                    None => return,
                }
            }
            Err(_) => return,
        };

        // Check each active position — if its token is not in held_assets, it was sold externally
        let mut dropped = Vec::new();
        for pos in &active {
            let token_id = match pos.swept_side {
                SweptSide::Up => &pos.market.up_token_id,
                SweptSide::Down => &pos.market.down_token_id,
            };
            if !held_assets.contains(token_id) {
                dropped.push(pos.clone());
            }
        }

        for pos in &dropped {
            let side_label = match pos.swept_side { SweptSide::Up => "UP", SweptSide::Down => "DOWN" };
            warn!("🔄 RECONCILE: {} {} sold externally — refunding ${:.2} reserved capital",
                  pos.market.title, side_label, pos.cost_usdc);
            self.capital_usdc += pos.cost_usdc;
            self.positions.retain(|p| {
                !(p.market.event_slug == pos.market.event_slug && !p.redeemed)
            });
        }
        if !dropped.is_empty() {
            save_positions(&self.positions);
            info!("🔄 Reconciled: dropped {} externally-sold positions, capital now ${:.2}",
                  dropped.len(), self.capital_usdc);
        }
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
            self.last_sweep_attempt.remove(slug);
            self.executing_slugs.remove(slug);
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
        // Keep only the next N events based on market duration
        let keep = if series_slug.contains("-5m") { 3 }       // 15 min coverage
                   else { 8 };                                 // 15m: 2 hour coverage
        candidates.truncate(keep);

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
                    let tokens_from_batch = fill.tokens_filled();
                    let usdc_from_batch = fill.usdc_amount();
                    if tokens_from_batch > 0.0 && usdc_from_batch > 0.0 {
                        // Batch response already has fill amounts — use directly, skip poll
                        tokens_total += tokens_from_batch;
                        cost_total += usdc_from_batch;
                        info!("   ✅ {} #{}: {:.4} tokens @ ${:.4} [{}] (batch)",
                              side_label, k + 1, tokens_from_batch, usdc_from_batch, fill.status);
                    } else if !fill.order_id.is_empty() {
                        // Batch returned 0 amounts — queue for fill poll
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

    // Poll GET /data/order/{orderId} for each matched order to get actual fill amounts.
    // Polymarket OpenOrder object uses field "size_matched" (snake_case).
    // FAK orders typically settle within 1-2s; retry up to 3 times with 1s gaps.
    if !matched_order_ids.is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        for (order_id, req_size, req_price) in &matched_order_ids {
            let path = format!("/data/order/{}", order_id);
            let mut size_filled = 0.0_f64;
            let mut price_used = *req_price;
            let mut poll_ok = false;
            for attempt in 0..3u32 {
                if attempt > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                }
                match executor.authenticated_get(&path).await {
                    Ok(resp) if resp.status().is_success() => {
                        match resp.json::<serde_json::Value>().await {
                            Ok(v) => {
                                // Polymarket OpenOrder: size_matched = tokens filled (snake_case)
                                let matched = v["size_matched"].as_str()
                                    .and_then(|s| s.parse::<f64>().ok())
                                    .unwrap_or(0.0);
                                let original = v["original_size"].as_str()
                                    .and_then(|s| s.parse::<f64>().ok())
                                    .unwrap_or(0.0);
                                let p = v["price"].as_str()
                                    .and_then(|s| s.parse::<f64>().ok())
                                    .unwrap_or(*req_price);
                                let status = v["status"].as_str().unwrap_or("UNKNOWN");
                                // Log raw fields for debugging size_matched inflation
                                info!("   📋 fill poll raw: original_size={:.4} size_matched={:.4} price={:.4} status={} req_size={:.4}",
                                      original, matched, p, status, req_size);
                                // Retry if still MATCHED/OPEN and size_matched=0
                                if matched == 0.0 && attempt < 2 {
                                    debug!("   ⏳ fill poll attempt {}: {} status={} size_matched=0 — retrying",
                                           attempt + 1, order_id, status);
                                    continue;
                                }
                                // Cap fill at original_size to avoid inflated size_matched
                                // in neg-risk markets where the CLOB may report complementary tokens
                                size_filled = if original > 0.0 { matched.min(original) } else { matched };
                                price_used = p;
                                poll_ok = true;
                                info!("   ✅ {} fill: {:.4} tokens @ {:.4} = ${:.4} [{}] ({})",
                                      side_label, size_filled, price_used,
                                      size_filled * price_used, status, order_id);
                                break;
                            }
                            Err(e) => {
                                warn!("   ⚠️  fill poll parse error for {}: {}", order_id, e);
                                break;
                            }
                        }
                    }
                    Ok(resp) => {
                        warn!("   ⚠️  fill poll {} returned HTTP {}", order_id, resp.status());
                        break;
                    }
                    Err(e) => {
                        warn!("   ⚠️  fill poll network error for {}: {}", order_id, e);
                        break;
                    }
                }
            }
            if poll_ok {
                tokens_total += size_filled;
                cost_total += size_filled * price_used;
            } else {
                // Poll failed entirely — fall back to requested size so capital isn't lost
                warn!("   ⚠️  fill poll gave up for {} — crediting req size {:.4}", order_id, req_size);
                tokens_total += req_size;
                cost_total += req_size * req_price;
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

// ---------------------------------------------------------------------------
// Background task: Binance WebSocket spot price feed
// ---------------------------------------------------------------------------

/// Connects to Binance combined aggTrade stream for BTC/ETH/SOL/XRP.
/// Sends (ArbAsset, price) on every trade event — essentially real-time.
/// Reconnects automatically on disconnect.
pub async fn run_spot_watcher(tx: mpsc::Sender<(ArbAsset, f64, std::time::Instant)>) {
    const URL: &str = "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/solusdt@aggTrade/xrpusdt@aggTrade";
    let mut backoff = 1u64;

    loop {
        match tungstenite_connect(URL).await {
            Ok((ws, _)) => {
                info!("📈 Binance spot WS connected");
                backoff = 1;
                let (_, mut read) = ws.split();
                loop {
                    match read.next().await {
                        Some(Ok(WsMessage::Text(txt))) => {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                let data = &v["data"];
                                let stream = v["stream"].as_str().unwrap_or("");
                                let asset = if stream.starts_with("btc") { ArbAsset::BTC }
                                    else if stream.starts_with("eth") { ArbAsset::ETH }
                                    else if stream.starts_with("sol") { ArbAsset::SOL }
                                    else if stream.starts_with("xrp") { ArbAsset::XRP }
                                    else { continue };
                                if let Some(price) = data["p"].as_str()
                                    .and_then(|s| s.parse::<f64>().ok())
                                {
                                    let _ = tx.try_send((asset, price, std::time::Instant::now()));
                                }
                            }
                        }
                        Some(Ok(WsMessage::Ping(d))) => {
                            // Binance sends pings; tungstenite auto-responds with pong
                            let _ = d;
                        }
                        Some(Ok(WsMessage::Close(_))) | None => {
                            warn!("📈 Binance spot WS closed — reconnecting in {}s", backoff);
                            break;
                        }
                        Some(Err(e)) => {
                            warn!("📈 Binance spot WS error: {} — reconnecting in {}s", e, backoff);
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                warn!("📈 Binance spot WS connect failed: {} — retry in {}s", e, backoff);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}
