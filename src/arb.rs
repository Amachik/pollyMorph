//! Crypto Up/Down Arbitrage Module
//!
//! Implements the Account88888 strategy: buy BOTH Up and Down tokens on
//! Polymarket's hourly BTC/ETH "Up or Down" markets. When Up + Down < $1.00,
//! merge pairs on-chain for instant $1.00 recovery, pocketing the spread.
//!
//! Flow:
//! 1. Scan Gamma API for active hourly BTC/ETH Up/Down events
//! 2. Fetch order books for both Up and Down tokens via CLOB REST
//! 3. Detect arbitrage: best_ask_up + best_ask_down < threshold
//! 4. Buy both sides simultaneously (IOC orders for maximum partial fills)
//! 5. Merge pairs on-chain via CTF mergePositions → instant $1/pair USDC recovery
//! 6. Fallback: if merge fails, wait for resolution and redeem winning side
//! 7. Repeat with next hourly market

use crate::config::Config;
use crate::execution::OrderExecutor;
use crate::metrics;
use crate::types::{MarketId, Side, OrderType, TimeInForce, TokenIdRegistry, PreparedOrder};
use crate::websocket::hash_asset_id;

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn, error, debug, trace};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum combined cost (Up + Down) we're willing to pay per pair.
/// $0.995 means we need at least 0.5% spread to enter.
/// Account88888 analysis shows profitable trades at combined costs up to ~0.995.
const MAX_PAIR_COST: f64 = 0.995;


/// Minimum order size in tokens (Polymarket enforces 5 for these markets).
/// Reduced to allow testing with smaller capital amounts.
const MIN_ORDER_SIZE: f64 = 1.0;

/// How often to poll Gamma API for new markets (seconds).
/// Reduced from 30s to 10s — 15-minute markets need faster discovery.
/// Account88888 starts trading within seconds of market open.
const MARKET_SCAN_INTERVAL_SECS: u64 = 10;


/// Seconds before market start to begin scanning the book.
/// Increased to 10 min so we pre-subscribe to WS and catch the first book snapshot.
const PRE_MARKET_LEAD_SECS: i64 = 600; // 10 minutes before

/// Fraction of market duration after start during which we'll attempt entry.
/// 0.5 = first half of the window (30 min for 1hr, 7.5 min for 15m).
const ENTRY_WINDOW_FRACTION: f64 = 0.5;

/// Series slugs we target.
const BTC_HOURLY_SERIES: &str = "btc-up-or-down-hourly";
const ETH_HOURLY_SERIES: &str = "eth-up-or-down-hourly";
const SOL_HOURLY_SERIES: &str = "solana-up-or-down-hourly";
const XRP_HOURLY_SERIES: &str = "xrp-up-or-down-hourly";
const BTC_15M_SERIES: &str = "btc-up-or-down-15m";
const ETH_15M_SERIES: &str = "eth-up-or-down-15m";
const SOL_15M_SERIES: &str = "sol-up-or-down-15m";
const XRP_15M_SERIES: &str = "xrp-up-or-down-15m";

/// CLOB WebSocket market channel URL (public, no auth needed).
/// Docs: https://docs.polymarket.com/developers/CLOB/websocket/market-channel
const CLOB_WS_MARKET_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// WebSocket keepalive interval (docs say send "PING" every 10s).
const WS_PING_INTERVAL_SECS: u64 = 10;

/// Maximum assets per WS subscribe message.
/// Polymarket resets connections when a single subscribe message is too large.
/// Batch subscriptions into chunks of this size with a small delay between.
const WS_SUB_BATCH_SIZE: usize = 100;

/// Channel buffer size for WS → engine communication.
const WS_CHANNEL_BUFFER: usize = 1024;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A discovered Up/Down market pair ready for arbitrage.
#[derive(Debug, Clone)]
pub struct ArbMarket {
    /// Human-readable title, e.g. "Bitcoin Up or Down - February 10, 1AM ET"
    pub title: String,
    /// Event ID from Gamma API (numeric, for fetching by ID)
    pub event_id: String,
    /// Event slug from Gamma API
    pub event_slug: String,
    /// Condition ID (for redemption)
    pub condition_id: String,
    /// "Up" token CLOB ID (the long string)
    pub up_token_id: String,
    /// "Down" token CLOB ID
    pub down_token_id: String,
    /// Whether this market uses neg_risk exchange
    pub neg_risk: bool,
    /// Tick size for price precision
    pub tick_size: f64,
    /// Minimum order size
    pub min_size: f64,
    /// When the candle starts (market becomes tradeable) — Unix timestamp
    pub event_start_ts: i64,
    /// When the market closes / resolves — Unix timestamp
    pub event_end_ts: i64,
    /// Asset (BTC or ETH)
    pub asset: ArbAsset,
    /// Taker fee in basis points for Up token (e.g. 200 = 2%)
    pub up_fee_bps: u32,
    /// Taker fee in basis points for Down token
    pub down_fee_bps: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbAsset {
    BTC,
    ETH,
    SOL,
    XRP,
}

impl ArbAsset {
    fn label(&self) -> &'static str {
        match self {
            ArbAsset::BTC => "btc",
            ArbAsset::ETH => "eth",
            ArbAsset::SOL => "sol",
            ArbAsset::XRP => "xrp",
        }
    }
}

/// Tracks our position in a single market cycle.
#[derive(Debug, Clone)]
struct CyclePosition {
    market: ArbMarket,
    up_tokens_bought: f64,
    down_tokens_bought: f64,
    up_cost_usdc: f64,
    down_cost_usdc: f64,
    total_cost: f64,
    expected_profit: f64,
    entered_at: chrono::DateTime<chrono::Utc>,
    /// Set to true once we've attempted redemption
    redeemed: bool,
}

/// A single price level in the order book.
#[derive(Debug, Clone, Copy)]
struct BookLevel {
    price: f64,
    size: f64,
}

/// Full order book snapshot for a single token (Up or Down).
#[derive(Debug, Clone)]
struct TokenBook {
    best_bid: f64,
    best_ask: f64,
    /// All ask levels sorted by price ascending (cheapest first)
    ask_levels: Vec<BookLevel>,
    /// Total tokens available across all ask levels
    total_ask_depth: f64,
}

/// Events sent from the WebSocket book watcher to the arb engine.
#[derive(Debug)]
enum ArbWsEvent {
    /// Full order book snapshot for an asset (event_type: "book").
    /// Sent on first subscribe and on every trade that affects the book.
    BookSnapshot {
        asset_id: String,
        book: TokenBook,
    },
    /// Top-of-book price update (event_type: "price_change").
    /// Updates best_bid/best_ask WITHOUT replacing full depth data.
    /// This is critical: price_change events are frequent but lack depth info.
    /// If we replaced the full book, we'd destroy the real multi-level depth
    /// from "book" snapshots, causing incorrect opportunity sizing.
    PriceUpdate {
        asset_id: String,
        best_bid: f64,
        best_ask: f64,
    },
    /// Market resolved event (event_type: "market_resolved").
    /// Contains the winning asset ID so we can trigger instant redemption.
    MarketResolved {
        market_condition_id: String,
        winning_asset_id: String,
        winning_outcome: String,
    },
    /// WebSocket connection status change.
    Connected(bool),
}

/// Result from a background merge/redeem task.
#[derive(Debug)]
struct MergeResult {
    event_slug: String,
    /// USDC recovered (merge_revenue or redeem_value)
    usdc_recovered: f64,
    /// Profit from this operation
    profit: f64,
    /// Excess position remaining after merge (if any)
    excess_position: Option<CyclePosition>,
    /// Whether this was a merge (true) or redeem (false)
    was_merge: bool,
    /// Error message if operation failed
    error: Option<String>,
}

/// A computed arb opportunity with execution plan.
#[derive(Debug, Clone)]
struct ArbOpportunity {
    /// Orders to place on the Up side (price, size per level)
    up_orders: Vec<BookLevel>,
    /// Orders to place on the Down side
    down_orders: Vec<BookLevel>,
    /// Total pairs we can buy
    total_pairs: f64,
    /// Total USDC cost for all pairs
    total_cost: f64,
    /// Expected profit ($1 * pairs - total_cost)
    expected_profit: f64,
    /// Weighted average pair cost
    avg_pair_cost: f64,
}

// ---------------------------------------------------------------------------
// Arb Engine
// ---------------------------------------------------------------------------

/// Commands sent from the arb engine to the WebSocket book watcher.
#[derive(Debug)]
enum WsCommand {
    /// Subscribe to new asset IDs.
    Subscribe(Vec<String>),
    /// Unsubscribe from asset IDs (expired markets).
    Unsubscribe(Vec<String>),
}

/// The main arbitrage engine. Runs as a standalone async loop.
pub struct ArbEngine {
    config: Arc<Config>,
    executor: Arc<OrderExecutor>,
    token_registry: Arc<TokenIdRegistry>,
    /// HTTP client for Gamma API and CLOB REST queries
    client: reqwest::Client,
    /// Currently active positions (one per market)
    positions: Vec<CyclePosition>,
    /// Total USDC available for trading
    capital_usdc: f64,
    /// Maximum USDC to deploy per single market cycle
    max_per_cycle: f64,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
    /// Running P&L tracker
    total_pnl: f64,
    /// Total cycles completed
    cycles_completed: u64,

    // ── WebSocket-driven real-time book tracking ──────────────────────────
    /// Cached order books keyed by asset_id (token ID string).
    /// Updated in real-time from the CLOB WebSocket market channel.
    book_cache: HashMap<String, TokenBook>,
    /// Receiver for events from the WebSocket book watcher task.
    ws_rx: mpsc::Receiver<ArbWsEvent>,
    /// Sender for commands to the WebSocket book watcher task.
    ws_cmd_tx: mpsc::Sender<WsCommand>,
    /// Markets currently being tracked (event_slug → ArbMarket).
    /// Used to map asset_id book updates back to their market.
    tracked_markets: HashMap<String, ArbMarket>,
    /// Reverse lookup: asset_id → event_slug (for both up and down tokens).
    asset_to_market: HashMap<String, String>,
    /// Set of asset_ids currently subscribed on the WebSocket.
    ws_subscribed: std::collections::HashSet<String>,
    /// Whether the WebSocket is currently connected.
    ws_connected: bool,
    /// Re-entry guard: event slugs currently being executed (order submission in progress).
    /// Prevents duplicate execution when multiple book updates arrive during slow execution.
    executing_slugs: std::collections::HashSet<String>,
    /// Receiver for background merge/redeem completion notifications.
    merge_rx: mpsc::Receiver<MergeResult>,
    /// Sender for background merge/redeem tasks (cloned into spawned tasks).
    merge_tx: mpsc::Sender<MergeResult>,
}

impl ArbEngine {
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

        // Deploy up to 90% of capital per cycle (keep 10% buffer for fees/slippage)
        let max_per_cycle = capital_usdc * 0.90;

        // Create channels for WebSocket ↔ engine communication
        let (ws_event_tx, ws_rx) = mpsc::channel::<ArbWsEvent>(WS_CHANNEL_BUFFER);
        let (ws_cmd_tx, ws_cmd_rx) = mpsc::channel::<WsCommand>(64);
        // Channel for background merge/redeem completion notifications
        let (merge_tx, merge_rx) = mpsc::channel::<MergeResult>(32);

        // Spawn the WebSocket book watcher task
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_for_ws = shutdown_flag.clone();
        tokio::spawn(async move {
            run_book_watcher(ws_event_tx, ws_cmd_rx, shutdown_for_ws).await;
        });

        Self {
            config,
            executor,
            token_registry,
            client,
            positions: Vec::new(),
            capital_usdc,
            max_per_cycle,
            shutdown: shutdown_flag,
            total_pnl: 0.0,
            cycles_completed: 0,
            book_cache: HashMap::new(),
            ws_rx,
            ws_cmd_tx,
            tracked_markets: HashMap::new(),
            asset_to_market: HashMap::new(),
            ws_subscribed: std::collections::HashSet::new(),
            ws_connected: false,
            executing_slugs: std::collections::HashSet::new(),
            merge_rx,
            merge_tx,
        }
    }

    /// Returns a clone of the shutdown flag for external control.
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        self.shutdown.clone()
    }

    // -----------------------------------------------------------------------
    // Main loop
    // -----------------------------------------------------------------------

    /// Run the arbitrage engine. This is the top-level entry point.
    ///
    /// Architecture: event-driven via tokio::select!
    /// - CLOB WebSocket `market` channel provides real-time book snapshots (~50ms latency)
    /// - REST polling (30s) only for market DISCOVERY (finding new hourly events)
    /// - On each book update: instantly check spread → execute if profitable
    /// - On market_resolved: instantly trigger redemption
    pub async fn run(&mut self) {
        info!("🎯 ARB ENGINE STARTED — Capital: ${:.2}, Max/cycle: ${:.2}",
              self.capital_usdc, self.max_per_cycle);
        info!("   Strategy: Buy both Up+Down when spread > {:.1}% (max pair cost ${:.3})", (1.0 - MAX_PAIR_COST) * 100.0, MAX_PAIR_COST);
        info!("   Targets: BTC/ETH/SOL/XRP × 1hr+15m | {}s scan | {}s pre-sub | parallel execution",
              MARKET_SCAN_INTERVAL_SECS, PRE_MARKET_LEAD_SECS);

        // Market discovery timer — REST poll Gamma API for new events
        let mut scan_interval = tokio::time::interval(
            std::time::Duration::from_secs(MARKET_SCAN_INTERVAL_SECS)
        );
        // Delay mode: if a tick is missed while discover_and_subscribe() is running,
        // don't fire immediately — wait the full interval from completion.
        // Without this, scans run back-to-back since discovery takes ~7s.
        scan_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Fire immediately on first tick
        scan_interval.tick().await;

        // Status log timer
        let mut status_interval = tokio::time::interval(
            std::time::Duration::from_secs(60)
        );

        loop {
            tokio::select! {
                // ── Real-time: WebSocket book updates ────────────────────
                // This is the HOT PATH — triggers on every book change
                ws_event = self.ws_rx.recv() => {
                    match ws_event {
                        Some(ArbWsEvent::BookSnapshot { asset_id, book }) => {
                            self.handle_book_update(asset_id, book).await;
                        }
                        Some(ArbWsEvent::PriceUpdate { asset_id, best_bid, best_ask }) => {
                            self.handle_price_update(asset_id, best_bid, best_ask).await;
                        }
                        Some(ArbWsEvent::MarketResolved { market_condition_id, winning_asset_id, winning_outcome }) => {
                            info!("🏁 WS: Market resolved — winner: {} ({})", winning_outcome, &winning_asset_id[..20.min(winning_asset_id.len())]);
                            self.handle_ws_resolution(&market_condition_id, &winning_asset_id, &winning_outcome).await;
                        }
                        Some(ArbWsEvent::Connected(connected)) => {
                            self.ws_connected = connected;
                            if connected {
                                info!("🔌 WS: Connected to CLOB market channel");
                                // No need to re-subscribe here — run_book_watcher already
                                // sends all pending_subs in batches during initial connection.
                                // Sending them again would double the subscription load.
                            } else {
                                warn!("🔌 WS: Disconnected from CLOB market channel — falling back to REST");
                            }
                        }
                        None => {
                            error!("WS channel closed — book watcher died");
                            break;
                        }
                    }
                }

                // ── Background merge/redeem completion ───────────────────
                // Non-blocking: merge operations run in spawned tasks and
                // report back here so the event loop is never blocked.
                merge_result = self.merge_rx.recv() => {
                    if let Some(result) = merge_result {
                        self.handle_merge_result(result);
                    }
                }

                // ── Periodic: Market discovery + position management ─────
                _ = scan_interval.tick() => {
                    if self.shutdown.load(Ordering::Relaxed) {
                        info!("🛑 ARB ENGINE shutting down");
                        break;
                    }

                    // Check for resolved markets and redeem
                    self.check_and_redeem().await;

                    // Discover new markets and subscribe to their books via WS
                    self.discover_and_subscribe().await;

                    // If WS is down, fall back to REST-based opportunity checking
                    if !self.ws_connected {
                        self.check_opportunities_rest().await;
                    }
                }

                // ── Periodic: Status logging + metrics update ────────────
                _ = status_interval.tick() => {
                    let total_deployed: f64 = self.positions.iter().map(|p| p.total_cost).sum();
                    let tracked = self.tracked_markets.len();
                    let subscribed = self.ws_subscribed.len();
                    info!("📊 ARB STATUS: {} positions (${:.2} deployed), ${:.2} avail, P&L: ${:.2}, Cycles: {} | WS: {} ({} markets, {} assets)",
                          self.positions.len(), total_deployed,
                          self.capital_usdc, self.total_pnl, self.cycles_completed,
                          if self.ws_connected { "✅" } else { "❌" },
                          tracked, subscribed);

                    // Update Prometheus gauges
                    metrics::ARB_CAPITAL_AVAILABLE.set(self.capital_usdc);
                    metrics::ARB_CAPITAL_DEPLOYED.set(total_deployed);
                    metrics::ARB_TOTAL_PNL.set(self.total_pnl);
                    metrics::ARB_CYCLES_COMPLETED.set(self.cycles_completed as i64);
                    metrics::ARB_ACTIVE_POSITIONS.set(self.positions.len() as i64);
                    metrics::ARB_TRACKED_MARKETS.set(tracked as i64);
                    metrics::ARB_WS_SUBSCRIBED_ASSETS.set(subscribed as i64);
                    metrics::ARB_WS_CONNECTED.set(if self.ws_connected { 1 } else { 0 });
                }
            }
        }

        info!("🏁 ARB ENGINE STOPPED — Final P&L: ${:.2}, Cycles: {}",
              self.total_pnl, self.cycles_completed);
    }

    /// Handle a real-time book update from the WebSocket.
    /// This is the HOT PATH — called on every book change for subscribed assets.
    ///
    /// CONTINUOUS ACCUMULATION: Unlike the old single-entry approach, we now
    /// allow re-entry into markets with existing positions. Account88888 executes
    /// 267 trades over 5 minutes in a single market — we must do the same.
    /// Merging is deferred until the entry window closes (see check_and_redeem).
    async fn handle_book_update(&mut self, asset_id: String, book: TokenBook) {
        // Update cache
        self.book_cache.insert(asset_id.clone(), book);

        // Find which market this asset belongs to
        let event_slug = match self.asset_to_market.get(&asset_id) {
            Some(slug) => slug.clone(),
            None => return, // Unknown asset — not tracked
        };

        let market = match self.tracked_markets.get(&event_slug) {
            Some(m) => m.clone(),
            None => return,
        };

        // Re-entry guard: skip if order submission is in-flight for this market.
        // This is a SHORT-LIVED guard (~1-2s during signing+submission), NOT a
        // long-lived block like the old merge-based guard.
        if self.executing_slugs.contains(&market.event_slug) {
            return;
        }

        // Check if we're within the entry window
        let now = chrono::Utc::now().timestamp();
        if now < market.event_start_ts - PRE_MARKET_LEAD_SECS {
            return; // Too early
        }
        let entry_window = ((market.event_end_ts - market.event_start_ts) as f64 * ENTRY_WINDOW_FRACTION) as i64;
        if now > market.event_start_ts + entry_window {
            return; // Past entry window — merging will happen in check_and_redeem
        }

        // Check if we have BOTH Up and Down books cached
        let up_book = match self.book_cache.get(&market.up_token_id) {
            Some(b) => b.clone(),
            None => return, // Waiting for the other side
        };
        let down_book = match self.book_cache.get(&market.down_token_id) {
            Some(b) => b.clone(),
            None => return,
        };

        // Track book depth metrics
        let asset_lbl = market.asset.label();
        metrics::ARB_BOOK_DEPTH.with_label_values(&[asset_lbl, "up"]).set(up_book.total_ask_depth);
        metrics::ARB_BOOK_DEPTH.with_label_values(&[asset_lbl, "down"]).set(down_book.total_ask_depth);

        // Track best pair cost and spread (fee-adjusted)
        let up_fee_mult = 1.0 + market.up_fee_bps as f64 / 10000.0;
        let down_fee_mult = 1.0 + market.down_fee_bps as f64 / 10000.0;
        let pair_cost = up_book.best_ask * up_fee_mult + down_book.best_ask * down_fee_mult;
        metrics::ARB_BEST_PAIR_COST.with_label_values(&[asset_lbl]).set(pair_cost);
        metrics::ARB_SPREAD_PCT.with_label_values(&[asset_lbl]).set((1.0 - pair_cost) * 100.0);

        // Run spread detection on cached books (no HTTP call needed!)
        match self.check_arb_from_books(&market, &up_book, &down_book) {
            Some(opp) => {
                let is_accumulating = self.has_position(&market.event_slug);
                let spread_pct = (1.0 - opp.avg_pair_cost) * 100.0;
                info!("⚡ WS ARB {}: {} | {:.0} pairs | spread {:.2}% | profit ${:.2}",
                      if is_accumulating { "ACCUMULATE" } else { "OPPORTUNITY" },
                      market.title, opp.total_pairs, spread_pct, opp.expected_profit);

                metrics::ARB_OPPORTUNITIES_DETECTED.with_label_values(&[asset_lbl, "ws"]).inc();
                metrics::ARB_AVAILABLE_PAIRS.with_label_values(&[asset_lbl]).set(opp.total_pairs);

                // Set short-lived re-entry guard during order submission
                self.executing_slugs.insert(market.event_slug.clone());
                self.execute_arb(&market, opp).await;
                // Always clear the guard immediately — we no longer tie it to merge lifecycle.
                // The guard only prevents concurrent order submissions for the same market.
                self.executing_slugs.remove(&market.event_slug);
            }
            None => {
                metrics::ARB_AVAILABLE_PAIRS.with_label_values(&[asset_lbl]).set(0.0);
            }
        }
    }

    /// Handle a price_change event — update top-of-book WITHOUT destroying depth.
    ///
    /// price_change events are more frequent than full book snapshots. The old code
    /// replaced the entire cached book with a fake 1-level/100-token book, which
    /// destroyed real multi-level depth data and caused incorrect opportunity sizing.
    ///
    /// Now we update ONLY best_bid/best_ask summary fields on the existing cached
    /// book, without touching ask_levels (preserving sort invariant).
    /// If no book is cached yet, we create a minimal placeholder
    /// (will be replaced by the next full "book" snapshot).
    async fn handle_price_update(&mut self, asset_id: String, best_bid: f64, best_ask: f64) {
        if let Some(cached) = self.book_cache.get_mut(&asset_id) {
            // Update ONLY the top-of-book summary fields — do NOT touch ask_levels.
            // ask_levels must remain sorted ascending from the last full "book" snapshot.
            // If we modified asks[0].price here, we could break the sort invariant:
            //   e.g., old asks = [(0.49, 100), (0.50, 50)], price_change says best_ask = 0.51
            //   → asks would become [(0.51, 100), (0.50, 50)] — UNSORTED, breaks sweep.
            // The spread detector uses best_ask for the quick check; find_arb_pairs uses
            // the sorted ask_levels for actual sizing. IOC orders handle any staleness.
            cached.best_bid = best_bid;
            cached.best_ask = best_ask;
        } else {
            // No cached book yet — create minimal placeholder.
            // This will be overwritten by the next full "book" snapshot.
            self.book_cache.insert(asset_id.clone(), TokenBook {
                best_bid,
                best_ask,
                ask_levels: vec![BookLevel { price: best_ask, size: 100.0 }],
                total_ask_depth: 100.0,
            });
        }

        // Now run the same spread detection as handle_book_update.
        // We need the full book from cache (which was just updated in-place).
        let event_slug = match self.asset_to_market.get(&asset_id) {
            Some(slug) => slug.clone(),
            None => return,
        };
        let market = match self.tracked_markets.get(&event_slug) {
            Some(m) => m.clone(),
            None => return,
        };

        if self.executing_slugs.contains(&market.event_slug) { return; }

        let now = chrono::Utc::now().timestamp();
        if now < market.event_start_ts - PRE_MARKET_LEAD_SECS { return; }
        let entry_window = ((market.event_end_ts - market.event_start_ts) as f64 * ENTRY_WINDOW_FRACTION) as i64;
        if now > market.event_start_ts + entry_window { return; }

        let up_book = match self.book_cache.get(&market.up_token_id) {
            Some(b) => b.clone(),
            None => return,
        };
        let down_book = match self.book_cache.get(&market.down_token_id) {
            Some(b) => b.clone(),
            None => return,
        };

        // Update metrics — use fee-adjusted pair cost for accurate spread tracking
        let asset_lbl = market.asset.label();
        let up_fee_mult = 1.0 + market.up_fee_bps as f64 / 10000.0;
        let down_fee_mult = 1.0 + market.down_fee_bps as f64 / 10000.0;
        let pair_cost = up_book.best_ask * up_fee_mult + down_book.best_ask * down_fee_mult;
        metrics::ARB_BEST_PAIR_COST.with_label_values(&[asset_lbl]).set(pair_cost);
        metrics::ARB_SPREAD_PCT.with_label_values(&[asset_lbl]).set((1.0 - pair_cost) * 100.0);

        // Quick exit: if top-of-book pair cost (with fees) is above threshold, no opportunity
        if pair_cost >= MAX_PAIR_COST { return; }

        // Spread looks promising — run full sweep on cached (real-depth) books
        match self.check_arb_from_books(&market, &up_book, &down_book) {
            Some(opp) => {
                let is_accumulating = self.has_position(&market.event_slug);
                let spread_pct = (1.0 - opp.avg_pair_cost) * 100.0;
                info!("⚡ WS ARB {}: {} | {:.0} pairs | spread {:.2}% | profit ${:.2}",
                      if is_accumulating { "ACCUMULATE" } else { "OPPORTUNITY" },
                      market.title, opp.total_pairs, spread_pct, opp.expected_profit);

                metrics::ARB_OPPORTUNITIES_DETECTED.with_label_values(&[asset_lbl, "ws"]).inc();
                metrics::ARB_AVAILABLE_PAIRS.with_label_values(&[asset_lbl]).set(opp.total_pairs);

                self.executing_slugs.insert(market.event_slug.clone());
                self.execute_arb(&market, opp).await;
                self.executing_slugs.remove(&market.event_slug);
            }
            None => {}
        }
    }

    /// Handle a market_resolved event from the WebSocket.
    /// Triggers instant redemption without waiting for the next scan cycle.
    async fn handle_ws_resolution(
        &mut self,
        _market_condition_id: &str,
        winning_asset_id: &str,
        winning_outcome: &str,
    ) {
        // Find positions that match the winning asset_id
        for pos in &mut self.positions {
            if pos.redeemed { continue; }
            let market = &pos.market;
            if market.up_token_id == winning_asset_id || market.down_token_id == winning_asset_id {
                info!("   🎯 WS resolution match: {} — winner is {}", market.title, winning_outcome);
                // The check_and_redeem cycle will pick this up on next scan,
                // or we could trigger immediate redemption here.
                // For safety, let the existing redemption flow handle it
                // since it has all the on-chain balance checks.
            }
        }
    }

    /// Handle completion of a background merge/redeem task.
    /// Updates capital, PnL, and positions based on the merge result.
    fn handle_merge_result(&mut self, result: MergeResult) {
        // Always clear the executing guard so the position can be retried
        self.executing_slugs.remove(&result.event_slug);

        if let Some(ref err) = result.error {
            warn!("   ⚠️  [BG] Merge failed for {}: {} — position remains for retry",
                  result.event_slug, err);
            if result.was_merge { metrics::ARB_MERGE_TOTAL.with_label_values(&["failure"]).inc(); }
            else { metrics::ARB_REDEEM_TOTAL.with_label_values(&["failure"]).inc(); }
            return;
        }

        if result.was_merge { metrics::ARB_MERGE_TOTAL.with_label_values(&["success"]).inc(); }
        else { metrics::ARB_REDEEM_TOTAL.with_label_values(&["success"]).inc(); }

        // Update capital and PnL
        self.capital_usdc += result.usdc_recovered;
        self.total_pnl += result.profit;
        self.cycles_completed += 1;

        // Track financial metrics
        metrics::ARB_CYCLE_PROFIT.observe(result.profit);
        if result.was_merge {
            metrics::ARB_USDC_MERGED.inc_by(result.usdc_recovered);
            metrics::ARB_PAIRS_MERGED.inc_by(result.usdc_recovered); // $1 per pair
        } else {
            metrics::ARB_USDC_REDEEMED.inc_by(result.usdc_recovered);
        }

        info!("   💰 [BG] MERGE COMPLETE: {} | +${:.2} profit | Capital: ${:.2} | P&L: ${:.2}",
              result.event_slug, result.profit, self.capital_usdc, self.total_pnl);

        // Find and update the position
        if let Some(pos_idx) = self.positions.iter().position(|p| p.market.event_slug == result.event_slug && !p.redeemed) {
            if let Some(mut excess) = result.excess_position {
                // Carry over the FULL market data from the original position
                // (the background task only has partial info like condition_id/token_ids)
                excess.market = self.positions[pos_idx].market.clone();
                self.positions[pos_idx] = excess;
                info!("   📦 Excess tokens tracked for later redemption");
            } else {
                // Clean merge — remove position
                self.positions[pos_idx].redeemed = true;
                self.positions.remove(pos_idx);
            }
        }
    }

    /// Discover new markets via REST and subscribe their tokens to the WebSocket.
    async fn discover_and_subscribe(&mut self) {
        let scan_start = std::time::Instant::now();
        match self.scan_markets().await {
            Ok(markets) => {
                let now = chrono::Utc::now().timestamp();
                let mut new_subscribe: Vec<String> = Vec::new();

                for mut market in markets {
                    // Check if within the broad monitoring window
                    if now > market.event_end_ts + 300 {
                        continue; // Already ended
                    }

                    let slug = market.event_slug.clone();
                    if !self.tracked_markets.contains_key(&slug) {
                        let pre_sub = if now < market.event_start_ts { " [PRE-OPEN]" } else { "" };
                        info!("📡 Tracking new market: {}{} (Up: {}..., Down: {}...)",
                              market.title, pre_sub,
                              &market.up_token_id[..16.min(market.up_token_id.len())],
                              &market.down_token_id[..16.min(market.down_token_id.len())]);

                        // Register in lookup maps
                        self.asset_to_market.insert(market.up_token_id.clone(), slug.clone());
                        self.asset_to_market.insert(market.down_token_id.clone(), slug.clone());

                        // Pre-register token info so execute_arb doesn't need HTTP calls.
                        // This moves ~200ms of latency off the hot path.
                        let up_hash = hash_asset_id(&market.up_token_id);
                        let down_hash = hash_asset_id(&market.down_token_id);
                        self.token_registry.register(up_hash, market.up_token_id.clone(), market.neg_risk);
                        self.token_registry.register(down_hash, market.down_token_id.clone(), market.neg_risk);
                        for (token_id, hash) in [
                            (market.up_token_id.clone(), up_hash),
                            (market.down_token_id.clone(), down_hash),
                        ] {
                            let _ = self.executor.fetch_and_register_market_info(&token_id, hash).await;
                        }

                        // Populate taker fee info from token registry into ArbMarket
                        market.up_fee_bps = self.token_registry.fee_rate_bps(up_hash);
                        market.down_fee_bps = self.token_registry.fee_rate_bps(down_hash);
                        if market.up_fee_bps > 0 || market.down_fee_bps > 0 {
                            warn!("💰 FEES DETECTED on {}: Up={}bps, Down={}bps — arb threshold adjusted",
                                  market.title, market.up_fee_bps, market.down_fee_bps);
                        }

                        // Collect for WS subscribe
                        if !self.ws_subscribed.contains(&market.up_token_id) {
                            new_subscribe.push(market.up_token_id.clone());
                            self.ws_subscribed.insert(market.up_token_id.clone());
                        }
                        if !self.ws_subscribed.contains(&market.down_token_id) {
                            new_subscribe.push(market.down_token_id.clone());
                            self.ws_subscribed.insert(market.down_token_id.clone());
                        }

                        self.tracked_markets.insert(slug, market);
                    }
                }

                // Subscribe new tokens to WebSocket
                if !new_subscribe.is_empty() {
                    info!("   📡 Subscribing {} new asset IDs to CLOB WebSocket", new_subscribe.len());
                    let _ = self.ws_cmd_tx.send(WsCommand::Subscribe(new_subscribe)).await;
                }

                // Unsubscribe expired markets
                self.cleanup_expired_markets().await;
                metrics::ARB_SCAN_DURATION.observe(scan_start.elapsed().as_secs_f64());
            }
            Err(e) => {
                warn!("Market scan failed: {}", e);
                metrics::ARB_SCAN_DURATION.observe(scan_start.elapsed().as_secs_f64());
            }
        }
    }

    /// Remove expired markets and unsubscribe their tokens from the WebSocket.
    async fn cleanup_expired_markets(&mut self) {
        let now = chrono::Utc::now().timestamp();
        let expired_slugs: Vec<String> = self.tracked_markets.iter()
            .filter(|(_, m)| now > m.event_end_ts + 600) // 10 min after end
            .map(|(slug, _)| slug.clone())
            .collect();

        if expired_slugs.is_empty() { return; }

        let mut unsub_ids: Vec<String> = Vec::new();
        for slug in &expired_slugs {
            if let Some(market) = self.tracked_markets.remove(slug) {
                self.asset_to_market.remove(&market.up_token_id);
                self.asset_to_market.remove(&market.down_token_id);
                self.book_cache.remove(&market.up_token_id);
                self.book_cache.remove(&market.down_token_id);

                if self.ws_subscribed.remove(&market.up_token_id) {
                    unsub_ids.push(market.up_token_id);
                }
                if self.ws_subscribed.remove(&market.down_token_id) {
                    unsub_ids.push(market.down_token_id);
                }
            }
        }

        if !unsub_ids.is_empty() {
            info!("   🧹 Unsubscribing {} expired asset IDs", unsub_ids.len());
            let _ = self.ws_cmd_tx.send(WsCommand::Unsubscribe(unsub_ids)).await;
        }
    }

    /// Fallback: check opportunities via REST when WebSocket is unavailable.
    /// This is the OLD polling path — only used when WS is disconnected.
    async fn check_opportunities_rest(&mut self) {
        let markets: Vec<ArbMarket> = self.tracked_markets.values().cloned().collect();
        let now = chrono::Utc::now().timestamp();

        for market in markets {
            if now < market.event_start_ts - PRE_MARKET_LEAD_SECS { continue; }
            let entry_window = ((market.event_end_ts - market.event_start_ts) as f64 * ENTRY_WINDOW_FRACTION) as i64;
            if now > market.event_start_ts + entry_window { continue; }

            // Skip if execution already in progress
            if self.executing_slugs.contains(&market.event_slug) { continue; }

            match self.check_arb_opportunity(&market).await {
                Ok(Some(opp)) => {
                    let spread_pct = (1.0 - opp.avg_pair_cost) * 100.0;
                    info!("💰 REST ARB {}: {} | {:.0} pairs | spread {:.2}% | profit ${:.2}",
                          if self.has_position(&market.event_slug) { "ACCUMULATE" } else { "OPPORTUNITY" },
                          market.title, opp.total_pairs, spread_pct, opp.expected_profit);
                    metrics::ARB_OPPORTUNITIES_DETECTED.with_label_values(&[market.asset.label(), "rest"]).inc();

                    self.executing_slugs.insert(market.event_slug.clone());
                    self.execute_arb(&market, opp).await;
                    self.executing_slugs.remove(&market.event_slug);
                }
                Ok(None) => {}
                Err(e) => { warn!("Failed to check arb for {}: {}", market.title, e); }
            }
        }
    }

    /// Check arb opportunity from pre-cached books (no HTTP calls).
    /// Used by the WebSocket-driven hot path.
    fn check_arb_from_books(
        &self,
        market: &ArbMarket,
        up_book: &TokenBook,
        down_book: &TokenBook,
    ) -> Option<ArbOpportunity> {
        if up_book.ask_levels.is_empty() || down_book.ask_levels.is_empty() {
            return None;
        }

        // Fee-adjusted quick check
        let up_fee_mult = 1.0 + market.up_fee_bps as f64 / 10000.0;
        let down_fee_mult = 1.0 + market.down_fee_bps as f64 / 10000.0;
        let best_pair_cost = up_book.best_ask * up_fee_mult + down_book.best_ask * down_fee_mult;
        if best_pair_cost >= MAX_PAIR_COST {
            return None;
        }

        // Use the same greedy sweep algorithm as check_arb_opportunity
        self.find_arb_pairs(market, up_book, down_book)
    }

    // -----------------------------------------------------------------------
    // Market scanning
    // -----------------------------------------------------------------------

    /// Scan Gamma API for active Up/Down events across all target series.
    ///
    /// Uses a 2-step approach:
    /// 1. GET /series?slug=X&active=true → discover event IDs (finds ALL events in the series)
    /// 2. GET /events/{id} for each → get full market data with clobTokenIds
    ///
    /// We do NOT filter by `closed` on stubs because Polymarket marks hourly events
    /// as closed=true once they start, even though markets inside are still trading.
    /// We rely on per-market `acceptingOrders` and duration filtering instead.
    async fn scan_markets(&self) -> Result<Vec<ArbMarket>, String> {
        let mut all_markets = Vec::new();

        for (series, asset) in [
            (BTC_HOURLY_SERIES, ArbAsset::BTC),
            (ETH_HOURLY_SERIES, ArbAsset::ETH),
            (SOL_HOURLY_SERIES, ArbAsset::SOL),
            (XRP_HOURLY_SERIES, ArbAsset::XRP),
            (BTC_15M_SERIES, ArbAsset::BTC),
            (ETH_15M_SERIES, ArbAsset::ETH),
            (SOL_15M_SERIES, ArbAsset::SOL),
            (XRP_15M_SERIES, ArbAsset::XRP),
        ] {
            match self.fetch_series_events(series, asset).await {
                Ok(mut m) => all_markets.append(&mut m),
                Err(e) => warn!("Failed to fetch {} events: {}", series, e),
            }
        }

        info!("📡 scan_markets: {} tradeable markets found (1hr + 15m, BTC/ETH/SOL/XRP)", all_markets.len());
        Ok(all_markets)
    }

    /// Fetch active events for a given series from Gamma API.
    ///
    /// Step 1: GET /series?slug=X&active=true → lightweight event stubs (no clobTokenIds)
    /// Step 2: GET /events/{id} for each non-expired event → full market data
    async fn fetch_series_events(&self, series_slug: &str, asset: ArbAsset) -> Result<Vec<ArbMarket>, String> {
        let series_url = format!(
            "https://gamma-api.polymarket.com/series?slug={}&active=true",
            series_slug
        );

        let resp = self.client.get(&series_url).send().await
            .map_err(|e| format!("Series HTTP error: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("Series API returned {}", resp.status()));
        }

        let series_list: Vec<serde_json::Value> = resp.json().await
            .map_err(|e| format!("Series JSON parse error: {}", e))?;

        let series = match series_list.first() {
            Some(s) => s,
            None => return Err(format!("No series found for slug: {}", series_slug)),
        };

        let events = match series.get("events").and_then(|v| v.as_array()) {
            Some(e) => e,
            None => return Ok(Vec::new()),
        };

        let now = chrono::Utc::now().timestamp();

        // Collect event IDs — do NOT filter by closed boolean.
        // Only skip events whose endDate is clearly in the past.
        // NOTE: `startDate` on stubs is the event *creation* date (2 days before),
        // NOT the market window start. Use `endDate` for sorting instead.
        let mut candidates: Vec<(String, i64)> = Vec::new();

        for event in events {
            let end_str = event.get("endDate").and_then(|v| v.as_str()).unwrap_or("");
            let end_ts = if !end_str.is_empty() {
                match chrono::DateTime::parse_from_rfc3339(end_str) {
                    Ok(end_dt) => {
                        let ts = end_dt.timestamp();
                        if ts < now - 300 {
                            continue; // Ended > 5 min ago
                        }
                        ts
                    }
                    Err(_) => i64::MAX,
                }
            } else {
                i64::MAX
            };

            let id_str = if let Some(id) = event.get("id").and_then(|v| v.as_str()) {
                id.to_string()
            } else if let Some(id) = event.get("id").and_then(|v| v.as_u64()) {
                id.to_string()
            } else {
                continue;
            };

            candidates.push((id_str, end_ts));
        }

        // Sort by endDate ascending: soonest-ending (currently active) first
        candidates.sort_by_key(|(_, end_ts)| *end_ts);
        // Take top 96 events (covers a full day of 15-min markets, or 24 hourly)
        let event_ids: Vec<String> = candidates.into_iter()
            .take(96)
            .map(|(id, _)| id)
            .collect();

        if event_ids.is_empty() {
            return Ok(Vec::new());
        }

        info!("📡 {}: {} candidate events, fetching full details...", series_slug, event_ids.len());

        // Step 2: Fetch full event details concurrently.
        let futures: Vec<_> = event_ids.iter().map(|event_id| {
            let url = format!("https://gamma-api.polymarket.com/events/{}", event_id);
            let client = self.client.clone();
            async move {
                let resp = client.get(&url).send().await.ok()?;
                if !resp.status().is_success() { return None; }
                resp.json::<serde_json::Value>().await.ok()
            }
        }).collect();

        let results = futures_util::future::join_all(futures).await;

        let mut markets = Vec::new();
        for event in results.into_iter().flatten() {
            let event_markets = match event.get("markets").and_then(|m| m.as_array()) {
                Some(m) => m,
                None => continue,
            };

            for market in event_markets {
                let arb = match self.parse_arb_market(&event, market, asset) {
                    Some(a) => a,
                    None => continue,
                };

                // Skip markets that ended more than 5 minutes ago
                if arb.event_end_ts > 0 && arb.event_end_ts < now - 300 {
                    continue;
                }

                // Accept ~15-minute windows (900s ± 300s) and ~1-hour windows (3600s ± 900s).
                // This excludes 4-hour windows (14400s) and other unusual durations.
                let duration = arb.event_end_ts - arb.event_start_ts;
                let is_15m = duration >= 600 && duration <= 1200;
                let is_1hr = duration >= 2700 && duration <= 4500;
                if !is_15m && !is_1hr {
                    continue;
                }

                // NOTE: We intentionally do NOT filter by acceptingOrders here.
                // Markets are created on Gamma API with clobTokenIds before they
                // start accepting orders. By subscribing to their WS channels early,
                // we receive the FIRST book snapshot the instant the market opens.
                // The entry window check in handle_book_update prevents premature execution.

                markets.push(arb);
            }
        }

        Ok(markets)
    }

    /// Parse a Gamma API market JSON into our ArbMarket struct.
    fn parse_arb_market(
        &self,
        event: &serde_json::Value,
        market: &serde_json::Value,
        asset: ArbAsset,
    ) -> Option<ArbMarket> {
        let title = event.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let event_id = event.get("id")
            .map(|v| v.as_str().map(|s| s.to_string())
                .or_else(|| v.as_u64().map(|n| n.to_string()))
                .unwrap_or_default())
            .unwrap_or_default();
        let event_slug = event.get("slug").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let condition_id = market.get("conditionId").and_then(|v| v.as_str()).unwrap_or("").to_string();

        // Parse clobTokenIds — JSON array string like '["id1", "id2"]'
        let clob_ids_str = market.get("clobTokenIds").and_then(|v| v.as_str())?;
        let clob_ids: Vec<String> = serde_json::from_str(clob_ids_str).ok()?;
        if clob_ids.len() != 2 {
            return None;
        }

        // Outcomes: ["Up", "Down"] — first token is Up, second is Down
        let outcomes_str = market.get("outcomes").and_then(|v| v.as_str()).unwrap_or("[]");
        let outcomes: Vec<String> = serde_json::from_str(outcomes_str).unwrap_or_default();

        // Determine which token is Up and which is Down
        let (up_token_id, down_token_id) = if outcomes.len() == 2 {
            if outcomes[0] == "Up" {
                (clob_ids[0].clone(), clob_ids[1].clone())
            } else {
                (clob_ids[1].clone(), clob_ids[0].clone())
            }
        } else {
            // Default: first is Up
            (clob_ids[0].clone(), clob_ids[1].clone())
        };

        let neg_risk = market.get("negRisk").and_then(|v| v.as_bool()).unwrap_or(false);

        let tick_size = market.get("orderPriceMinTickSize")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.01);

        let min_size = market.get("orderMinSize")
            .and_then(|v| v.as_f64())
            .unwrap_or(5.0);

        // Parse event start and end times
        let event_start_str = market.get("eventStartTime")
            .or_else(|| event.get("startTime"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let event_end_str = market.get("endDate")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let event_start_ts = chrono::DateTime::parse_from_rfc3339(event_start_str)
            .map(|dt| dt.timestamp())
            .unwrap_or(0);

        let event_end_ts = chrono::DateTime::parse_from_rfc3339(event_end_str)
            .map(|dt| dt.timestamp())
            .unwrap_or(0);

        Some(ArbMarket {
            title,
            event_id,
            event_slug,
            condition_id,
            up_token_id,
            down_token_id,
            neg_risk,
            tick_size,
            min_size,
            event_start_ts,
            event_end_ts,
            asset,
            up_fee_bps: 0,
            down_fee_bps: 0,
        })
    }

    // -----------------------------------------------------------------------
    // Order book fetching
    // -----------------------------------------------------------------------

    /// Fetch the full order book for a single token from the CLOB REST API.
    /// Returns all ask levels sorted by price ascending for multi-level sweeping.
    async fn fetch_token_book(&self, token_id: &str) -> Result<TokenBook, String> {
        let url = format!(
            "{}/book?token_id={}",
            self.config.polymarket.rest_url, token_id
        );

        let resp = self.client.get(&url).send().await
            .map_err(|e| format!("Book fetch error: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("Book API returned {}", resp.status()));
        }

        let book: serde_json::Value = resp.json().await
            .map_err(|e| format!("Book JSON error: {}", e))?;

        // Parse ALL ask levels (we want to BUY, so we sweep asks)
        let mut ask_levels = Vec::new();
        if let Some(asks) = book.get("asks").and_then(|v| v.as_array()) {
            for ask in asks {
                let price = ask.get("price").and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(1.0);
                let size = ask.get("size").and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                if size > 0.0 {
                    ask_levels.push(BookLevel { price, size });
                }
            }
        }
        // Sort by price ascending (cheapest first — we sweep from cheap to expensive)
        ask_levels.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));

        let total_ask_depth: f64 = ask_levels.iter().map(|l| l.size).sum();
        let best_ask = ask_levels.first().map(|l| l.price).unwrap_or(1.0);

        let best_bid = if let Some(bids) = book.get("bids").and_then(|v| v.as_array()) {
            bids.first()
                .and_then(|b| b.get("price").and_then(|v| v.as_str()))
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0)
        } else {
            0.0
        };

        Ok(TokenBook { best_bid, best_ask, ask_levels, total_ask_depth })
    }

    /// Fetch both Up and Down order books in a single HTTP request via POST /books.
    /// Returns (up_book, down_book). Falls back to parallel individual fetches on error.
    async fn fetch_both_books(&self, up_token_id: &str, down_token_id: &str)
        -> Result<(TokenBook, TokenBook), String>
    {
        let url = format!("{}/books", self.config.polymarket.rest_url);
        let body = serde_json::json!([
            {"token_id": up_token_id},
            {"token_id": down_token_id},
        ]);

        let resp = self.client.post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await;

        match resp {
            Ok(r) if r.status().is_success() => {
                let books: Vec<serde_json::Value> = r.json().await
                    .map_err(|e| format!("Books JSON error: {}", e))?;

                if books.len() < 2 {
                    return Err("Books response has fewer than 2 entries".to_string());
                }

                // Verify response order by checking asset_id field.
                // If the API returns them in a different order, match by token ID.
                let id0 = books[0].get("asset_id").and_then(|v| v.as_str()).unwrap_or("");
                let id1 = books[1].get("asset_id").and_then(|v| v.as_str()).unwrap_or("");

                let (up_json, down_json) = if id0 == up_token_id && id1 == down_token_id {
                    (&books[0], &books[1])
                } else if id0 == down_token_id && id1 == up_token_id {
                    (&books[1], &books[0])
                } else {
                    // asset_id mismatch — fall back to positional order
                    warn!("POST /books asset_id mismatch ({}..., {}...) — using positional order",
                          &id0[..id0.len().min(12)], &id1[..id1.len().min(12)]);
                    (&books[0], &books[1])
                };

                let up_book = Self::parse_book_json(up_json)?;
                let down_book = Self::parse_book_json(down_json)?;
                Ok((up_book, down_book))
            }
            Ok(r) => {
                warn!("POST /books returned {} — falling back to parallel fetches", r.status());
                let (up, down) = tokio::join!(
                    self.fetch_token_book(up_token_id),
                    self.fetch_token_book(down_token_id),
                );
                Ok((up?, down?))
            }
            Err(e) => {
                warn!("POST /books failed: {} — falling back to parallel fetches", e);
                let (up, down) = tokio::join!(
                    self.fetch_token_book(up_token_id),
                    self.fetch_token_book(down_token_id),
                );
                Ok((up?, down?))
            }
        }
    }

    /// Parse a single book JSON object into a TokenBook.
    fn parse_book_json(book: &serde_json::Value) -> Result<TokenBook, String> {
        let mut ask_levels = Vec::new();
        if let Some(asks) = book.get("asks").and_then(|v| v.as_array()) {
            for ask in asks {
                let price = ask.get("price").and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(1.0);
                let size = ask.get("size").and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                if size > 0.0 {
                    ask_levels.push(BookLevel { price, size });
                }
            }
        }
        ask_levels.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));

        let total_ask_depth: f64 = ask_levels.iter().map(|l| l.size).sum();
        let best_ask = ask_levels.first().map(|l| l.price).unwrap_or(1.0);

        let best_bid = if let Some(bids) = book.get("bids").and_then(|v| v.as_array()) {
            bids.first()
                .and_then(|b| b.get("price").and_then(|v| v.as_str()))
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0)
        } else {
            0.0
        };

        Ok(TokenBook { best_bid, best_ask, ask_levels, total_ask_depth })
    }

    /// Analyze the full arb opportunity across all book levels.
    ///
    /// For each combination of Up ask level and Down ask level, we check if
    /// the pair cost < MAX_PAIR_COST. We greedily fill from cheapest levels first.
    ///
    /// Returns the total number of profitable pairs and the execution plan
    /// (list of (price, size) orders for each side).
    async fn check_arb_opportunity(&self, market: &ArbMarket)
        -> Result<Option<ArbOpportunity>, String>
    {
        // Fetch both books in a single HTTP request via POST /books
        let (up_book, down_book) = self.fetch_both_books(
            &market.up_token_id, &market.down_token_id
        ).await?;

        Ok(self.find_arb_pairs(market, &up_book, &down_book))
    }

    /// Core greedy sweep algorithm — shared by both REST and WebSocket paths.
    ///
    /// For each combination of Up ask level and Down ask level, checks if
    /// the pair cost < MAX_PAIR_COST. Greedily fills from cheapest levels first.
    /// Returns the execution plan if profitable, or None.
    fn find_arb_pairs(
        &self,
        market: &ArbMarket,
        up_book: &TokenBook,
        down_book: &TokenBook,
    ) -> Option<ArbOpportunity> {
        if up_book.ask_levels.is_empty() || down_book.ask_levels.is_empty() {
            return None;
        }

        // Fee multipliers: buying at price P costs P * fee_mult
        let up_fee_mult = 1.0 + market.up_fee_bps as f64 / 10000.0;
        let down_fee_mult = 1.0 + market.down_fee_bps as f64 / 10000.0;

        let best_pair_cost = up_book.best_ask * up_fee_mult + down_book.best_ask * down_fee_mult;
        if best_pair_cost >= MAX_PAIR_COST {
            return None;
        }

        // Greedy sweep: buy from cheapest levels on both sides.
        // We need equal numbers of Up and Down tokens (pairs).
        // Strategy: sweep Up asks from cheapest, sweep Down asks from cheapest,
        // and match them into pairs as long as pair_cost < MAX_PAIR_COST.
        let mut up_orders: Vec<BookLevel> = Vec::new();
        let mut down_orders: Vec<BookLevel> = Vec::new();
        let mut total_up_tokens = 0.0_f64;
        let mut total_down_tokens = 0.0_f64;
        let mut total_cost = 0.0_f64;

        // We iterate through Up levels. For each Up level, we find the maximum
        // Down price we can afford: max_down_price = (MAX_PAIR_COST - up_price * up_fee_mult) / down_fee_mult.
        // Then we sweep Down levels up to that price.
        let mut down_idx = 0;
        let mut down_remaining_at_level = if !down_book.ask_levels.is_empty() {
            down_book.ask_levels[0].size
        } else {
            0.0
        };

        for up_level in &up_book.ask_levels {
            let up_cost_per_token = up_level.price * up_fee_mult;
            let max_down_price = (MAX_PAIR_COST - up_cost_per_token) / down_fee_mult;
            if max_down_price <= 0.0 {
                break; // Up price alone (with fees) exceeds threshold
            }

            let mut up_remaining = up_level.size;

            // Match with Down levels — use fractional sizes (Polymarket supports them).
            // Account88888 trades sizes like 114.934781 — flooring wastes liquidity.
            while up_remaining >= 0.01 && down_idx < down_book.ask_levels.len() {
                let down_level = &down_book.ask_levels[down_idx];
                if down_level.price > max_down_price {
                    break; // This Down level is too expensive for this Up level
                }

                // How many pairs can we make at this combination?
                let pairs = up_remaining.min(down_remaining_at_level);
                if pairs < 0.01 {
                    if down_remaining_at_level < 0.01 {
                        // Down level exhausted — advance to next
                        down_idx += 1;
                        if down_idx < down_book.ask_levels.len() {
                            down_remaining_at_level = down_book.ask_levels[down_idx].size;
                        }
                        continue;
                    } else {
                        // Up level exhausted — break to next up level
                        break;
                    }
                }

                let pair_cost = up_level.price * up_fee_mult + down_level.price * down_fee_mult;
                let cost = pairs * pair_cost;

                up_orders.push(BookLevel { price: up_level.price, size: pairs });
                down_orders.push(BookLevel { price: down_level.price, size: pairs });
                total_up_tokens += pairs;
                total_down_tokens += pairs;
                total_cost += cost;

                up_remaining -= pairs;
                down_remaining_at_level -= pairs;

                if down_remaining_at_level < 0.01 {
                    down_idx += 1;
                    if down_idx < down_book.ask_levels.len() {
                        down_remaining_at_level = down_book.ask_levels[down_idx].size;
                    }
                }
            }
        }

        // Consolidate orders at same price level
        let up_orders = Self::consolidate_levels(up_orders);
        let down_orders = Self::consolidate_levels(down_orders);

        let total_pairs = total_up_tokens.min(total_down_tokens);
        if total_pairs < MIN_ORDER_SIZE {
            return None;
        }

        let expected_profit = total_pairs - total_cost; // Each pair redeems at $1

        Some(ArbOpportunity {
            up_orders,
            down_orders,
            total_pairs,
            total_cost,
            expected_profit,
            avg_pair_cost: total_cost / total_pairs,
        })
    }

    /// Consolidate multiple fills at the same price into a single order.
    fn consolidate_levels(levels: Vec<BookLevel>) -> Vec<BookLevel> {
        let mut consolidated: Vec<BookLevel> = Vec::new();
        for level in levels {
            if let Some(last) = consolidated.last_mut() {
                if (last.price - level.price).abs() < 0.0001 {
                    last.size += level.size;
                    continue;
                }
            }
            consolidated.push(level);
        }
        consolidated
    }

    // -----------------------------------------------------------------------
    // Execution
    // -----------------------------------------------------------------------

    /// Execute the arbitrage: sweep both Up and Down order books.
    ///
    /// Like Account88888, we fire orders at EVERY profitable price level,
    /// not just the best ask. This maximizes the number of pairs we accumulate.
    ///
    /// Accumulates into existing positions. Merge is deferred to check_and_redeem().
    async fn execute_arb(
        &mut self,
        market: &ArbMarket,
        opp: ArbOpportunity,
    ) {
        let asset_lbl = market.asset.label();
        let exec_start = std::time::Instant::now();

        // Capital check — capital_usdc tracks actual available USDC (deducted on entry, added on redemption)
        let available_capital = self.capital_usdc;
        let effective_capital = available_capital.min(self.max_per_cycle);

        if effective_capital < opp.total_cost {
            // Scale down to what we can afford
            let scale = effective_capital / opp.total_cost;
            if scale < MIN_ORDER_SIZE / opp.total_pairs {
                warn!("Insufficient capital: ${:.2} available, need ${:.2}", effective_capital, opp.total_cost);
                metrics::ARB_OPPORTUNITIES_SKIPPED.with_label_values(&["insufficient_capital"]).inc();
                return;
            }
            info!("   Scaling to {:.0}% of opportunity (capital limited)", scale * 100.0);
            // We'll cap individual order sizes below
        }

        let spread_pct = (1.0 - opp.avg_pair_cost) * 100.0;
        info!("🚀 EXECUTING ARB: {} | {:.0} pairs across {} levels | spread {:.2}%",
              market.title, opp.total_pairs, opp.up_orders.len() + opp.down_orders.len(), spread_pct);

        // ── FRESH BOOK FETCH ─────────────────────────────────────────────
        // The WS-triggered opportunity was detected on cached depth data.
        // By the time we get here (~1-5ms later), levels may have been consumed.
        // Fetch fresh books via REST (~30-50ms) and re-run the sweep to get
        // accurate, up-to-date depth for order building. IOC orders protect us
        // from stale levels, but fresh data means fewer wasted order slots.
        let opp = match self.fetch_both_books(&market.up_token_id, &market.down_token_id).await {
            Ok((fresh_up, fresh_down)) => {
                // Update cache with fresh data
                self.book_cache.insert(market.up_token_id.clone(), fresh_up.clone());
                self.book_cache.insert(market.down_token_id.clone(), fresh_down.clone());

                // Re-run sweep on fresh books
                match self.find_arb_pairs(market, &fresh_up, &fresh_down) {
                    Some(fresh_opp) => {
                        if fresh_opp.total_pairs < MIN_ORDER_SIZE {
                            info!("   📉 Fresh books: opportunity vanished (< {:.0} pairs)", MIN_ORDER_SIZE);
                            metrics::ARB_OPPORTUNITIES_SKIPPED.with_label_values(&["stale_opportunity"]).inc();
                            return;
                        }
                        let fresh_spread = (1.0 - fresh_opp.avg_pair_cost) * 100.0;
                        if fresh_opp.total_pairs < opp.total_pairs * 0.5 {
                            info!("   📉 Fresh books: {:.0} → {:.0} pairs ({:.2}% spread) — significantly reduced",
                                  opp.total_pairs, fresh_opp.total_pairs, fresh_spread);
                        } else {
                            debug!("   📊 Fresh books: {:.0} → {:.0} pairs ({:.2}% spread)",
                                   opp.total_pairs, fresh_opp.total_pairs, fresh_spread);
                        }
                        fresh_opp
                    }
                    None => {
                        info!("   📉 Fresh books: no opportunity remaining — aborting");
                        metrics::ARB_OPPORTUNITIES_SKIPPED.with_label_values(&["stale_opportunity"]).inc();
                        return;
                    }
                }
            }
            Err(e) => {
                // REST fetch failed — proceed with cached data (IOC orders protect us)
                warn!("   ⚠️  Fresh book fetch failed: {} — using cached data", e);
                opp
            }
        };

        // Re-check capital against potentially updated opportunity
        let effective_capital = available_capital.min(self.max_per_cycle);
        if effective_capital < opp.total_cost {
            let scale = effective_capital / opp.total_cost;
            if scale < MIN_ORDER_SIZE / opp.total_pairs {
                warn!("Insufficient capital after fresh book: ${:.2} available, need ${:.2}", effective_capital, opp.total_cost);
                metrics::ARB_OPPORTUNITIES_SKIPPED.with_label_values(&["insufficient_capital"]).inc();
                return;
            }
        }

        // Token info was pre-registered during discover_and_subscribe — no HTTP calls needed here.
        let up_hash = hash_asset_id(&market.up_token_id);
        let down_hash = hash_asset_id(&market.down_token_id);
        let up_market_id = MarketId { token_id: up_hash, condition_id: [0u8; 32] };
        let down_market_id = MarketId { token_id: down_hash, condition_id: [0u8; 32] };

        // Build all order signals — one per price level per side
        let mut up_signals = Vec::new();
        let mut down_signals = Vec::new();
        let mut remaining_capital = effective_capital;

        // Scale orders to fit within capital
        let capital_scale = if opp.total_cost > effective_capital {
            effective_capital / opp.total_cost
        } else {
            1.0
        };

        for level in &opp.up_orders {
            let scaled_size = level.size * capital_scale;
            if scaled_size < MIN_ORDER_SIZE { continue; }

            let cost = scaled_size * level.price;
            if cost > remaining_capital { continue; }
            remaining_capital -= cost;

            let size = Decimal::from_f64_retain(scaled_size).unwrap_or(Decimal::new(5, 0));
            let price = Decimal::from_f64_retain(level.price).unwrap_or(Decimal::new(50, 2));

            up_signals.push(crate::types::TradeSignal {
                market_id: up_market_id,
                side: Side::Buy,
                price,
                size,
                order_type: OrderType::ImmediateOrCancel,
                urgency: crate::types::SignalUrgency::Critical,
                expected_profit_bps: (spread_pct * 100.0) as i32,
                signal_timestamp_ns: 0,
            });
        }

        for level in &opp.down_orders {
            let scaled_size = level.size * capital_scale;
            if scaled_size < MIN_ORDER_SIZE { continue; }

            let cost = scaled_size * level.price;
            if cost > remaining_capital { continue; }
            remaining_capital -= cost;

            let size = Decimal::from_f64_retain(scaled_size).unwrap_or(Decimal::new(5, 0));
            let price = Decimal::from_f64_retain(level.price).unwrap_or(Decimal::new(50, 2));

            down_signals.push(crate::types::TradeSignal {
                market_id: down_market_id,
                side: Side::Buy,
                price,
                size,
                order_type: OrderType::ImmediateOrCancel,
                urgency: crate::types::SignalUrgency::Critical,
                expected_profit_bps: (spread_pct * 100.0) as i32,
                signal_timestamp_ns: 0,
            });
        }

        if up_signals.is_empty() || down_signals.is_empty() {
            warn!("No valid orders after capital scaling — skipping");
            return;
        }

        let total_signals = up_signals.len() + down_signals.len();
        info!("   Signing {} Up + {} Down orders for batch submission...",
              up_signals.len(), down_signals.len());

        // ── STEP 1: Sign all orders in parallel ──────────────────────────
        // Each order needs a unique EIP-712 signature. We sign Up and Down
        // concurrently to minimize wall-clock time.
        let up_token_str = market.up_token_id.clone();
        let down_token_str = market.down_token_id.clone();
        let up_neg_risk = self.token_registry.is_neg_risk(up_hash);
        let down_neg_risk = self.token_registry.is_neg_risk(down_hash);
        let up_fee = self.token_registry.fee_rate_bps(up_hash);
        let down_fee = self.token_registry.fee_rate_bps(down_hash);

        let sign_start = std::time::Instant::now();

        // Sign all Up orders
        let up_sign_futures: Vec<_> = up_signals.iter().map(|sig| {
            self.executor.signer().prepare_order_full(
                sig.market_id, sig.side, sig.price, sig.size,
                sig.order_type, TimeInForce::IOC, 0,
                up_token_str.clone(), up_neg_risk, up_fee,
            )
        }).collect();

        // Sign all Down orders
        let down_sign_futures: Vec<_> = down_signals.iter().map(|sig| {
            self.executor.signer().prepare_order_full(
                sig.market_id, sig.side, sig.price, sig.size,
                sig.order_type, TimeInForce::IOC, 0,
                down_token_str.clone(), down_neg_risk, down_fee,
            )
        }).collect();

        // Execute all signing concurrently
        let (up_signed, down_signed) = tokio::join!(
            futures_util::future::join_all(up_sign_futures),
            futures_util::future::join_all(down_sign_futures),
        );

        let sign_elapsed = sign_start.elapsed();
        info!("   Signed {} orders in {:.1}ms", total_signals, sign_elapsed.as_secs_f64() * 1000.0);
        metrics::ARB_EXECUTION_LATENCY.with_label_values(&["signing"]).observe(sign_elapsed.as_secs_f64());

        // Collect successfully signed orders for parallel submission
        let mut up_ok: Vec<PreparedOrder> = Vec::new();
        let mut down_ok: Vec<PreparedOrder> = Vec::new();

        for (i, result) in up_signed.into_iter().enumerate() {
            match result {
                Ok(order) => up_ok.push(order),
                Err(e) => warn!("   ❌ Up #{} sign failed: {}", i + 1, e),
            }
        }
        for (i, result) in down_signed.into_iter().enumerate() {
            match result {
                Ok(order) => down_ok.push(order),
                Err(e) => warn!("   ❌ Down #{} sign failed: {}", i + 1, e),
            }
        }

        if up_ok.is_empty() || down_ok.is_empty() {
            warn!("Signing failed for one side — aborting arb (need both sides)");
            metrics::ARB_OPPORTUNITIES_SKIPPED.with_label_values(&["signing_failed"]).inc();
            return;
        }

        // ── STEP 2: Submit Up and Down batches IN PARALLEL ───────────────
        // Instead of interleaving into one serial stream, we fire Up and Down
        // as two independent parallel batch streams. This cuts submission
        // latency roughly in half — critical for catching early spreads.
        let submit_start = std::time::Instant::now();

        // Fire Up and Down batch streams in parallel
        let executor_ref = &*self.executor;
        let up_count = up_ok.len();
        let down_count = down_ok.len();
        let ((up_orders_ok, up_tokens_total, up_cost_total),
             (down_orders_ok, down_tokens_total, down_cost_total)) = tokio::join!(
            submit_order_side(executor_ref, up_ok, "Up"),
            submit_order_side(executor_ref, down_ok, "Down"),
        );

        let submit_elapsed = submit_start.elapsed();
        let up_batches = (up_count + 14) / 15;
        let down_batches = (down_count + 14) / 15;
        info!("   Submitted {} orders in {:.0}ms ({} Up + {} Down batches, parallel)",
              total_signals, submit_elapsed.as_secs_f64() * 1000.0,
              up_batches, down_batches);
        metrics::ARB_EXECUTION_LATENCY.with_label_values(&["submission"]).observe(submit_elapsed.as_secs_f64());

        let total_cost = up_cost_total + down_cost_total;
        let min_tokens = up_tokens_total.min(down_tokens_total);
        let expected_profit = min_tokens - total_cost; // Each pair redeems at $1

        // Track execution metrics
        metrics::ARB_ORDERS_SUBMITTED.with_label_values(&[asset_lbl, "up"]).inc_by(up_signals.len() as u64);
        metrics::ARB_ORDERS_SUBMITTED.with_label_values(&[asset_lbl, "down"]).inc_by(down_signals.len() as u64);
        metrics::ARB_ORDERS_FILLED.with_label_values(&[asset_lbl, "up"]).inc_by(up_orders_ok as u64);
        metrics::ARB_ORDERS_FILLED.with_label_values(&[asset_lbl, "down"]).inc_by(down_orders_ok as u64);
        let up_failed = up_signals.len() as u64 - up_orders_ok as u64;
        let down_failed = down_signals.len() as u64 - down_orders_ok as u64;
        if up_failed > 0 { metrics::ARB_ORDERS_FAILED.with_label_values(&[asset_lbl, "up"]).inc_by(up_failed); }
        if down_failed > 0 { metrics::ARB_ORDERS_FAILED.with_label_values(&[asset_lbl, "down"]).inc_by(down_failed); }
        if up_tokens_total > 0.0 { metrics::ARB_TOKENS_BOUGHT.with_label_values(&[asset_lbl, "up"]).inc_by(up_tokens_total); }
        if down_tokens_total > 0.0 { metrics::ARB_TOKENS_BOUGHT.with_label_values(&[asset_lbl, "down"]).inc_by(down_tokens_total); }
        if up_cost_total > 0.0 { metrics::ARB_USDC_SPENT.with_label_values(&[asset_lbl, "up"]).inc_by(up_cost_total); }
        if down_cost_total > 0.0 { metrics::ARB_USDC_SPENT.with_label_values(&[asset_lbl, "down"]).inc_by(down_cost_total); }
        if total_cost > 0.0 {
            metrics::ARB_AVG_PAIR_COST.observe(total_cost / min_tokens.max(1.0));
        }
        let total_exec = exec_start.elapsed();
        metrics::ARB_EXECUTION_LATENCY.with_label_values(&["total"]).observe(total_exec.as_secs_f64());

        info!("   📊 RESULT: Up {}/{} orders ({:.0} tokens ${:.2}) | Down {}/{} orders ({:.0} tokens ${:.2})",
              up_orders_ok, up_signals.len(), up_tokens_total, up_cost_total,
              down_orders_ok, down_signals.len(), down_tokens_total, down_cost_total);

        if up_orders_ok > 0 || down_orders_ok > 0 {
            if up_orders_ok > 0 && down_orders_ok > 0 {
                info!("   🎯 ARB FILLED: ${:.2} deployed, {:.0} pairs, expected profit ${:.2}",
                      total_cost, min_tokens, expected_profit);
            } else {
                warn!("   ⚠️  PARTIAL FILL: Up={} Down={} — will balance on next opportunity",
                      up_orders_ok, down_orders_ok);
            }
            metrics::ARB_OPPORTUNITIES_EXECUTED.with_label_values(&[asset_lbl]).inc();

            self.capital_usdc -= total_cost;

            // ACCUMULATE into existing position or create new one.
            // Merge is DEFERRED until the entry window closes — we keep buying
            // as long as spreads are favorable, just like account88888.
            if let Some(pos) = self.positions.iter_mut().find(|p| p.market.event_slug == market.event_slug && !p.redeemed) {
                // Accumulate into existing position
                pos.up_tokens_bought += up_tokens_total;
                pos.down_tokens_bought += down_tokens_total;
                pos.up_cost_usdc += up_cost_total;
                pos.down_cost_usdc += down_cost_total;
                pos.total_cost += total_cost;
                let new_min = pos.up_tokens_bought.min(pos.down_tokens_bought);
                pos.expected_profit = new_min - pos.total_cost;
                info!("   📦 ACCUMULATED: total {:.0} Up + {:.0} Down ({:.0} pairs) | ${:.2} deployed",
                      pos.up_tokens_bought, pos.down_tokens_bought, new_min, pos.total_cost);
            } else {
                // First entry — create new position
                self.positions.push(CyclePosition {
                    market: market.clone(),
                    up_tokens_bought: up_tokens_total,
                    down_tokens_bought: down_tokens_total,
                    up_cost_usdc: up_cost_total,
                    down_cost_usdc: down_cost_total,
                    total_cost,
                    expected_profit,
                    entered_at: chrono::Utc::now(),
                    redeemed: false,
                });
            }

            // NOTE: Merge is NOT triggered here. It happens in check_and_redeem()
            // once the entry window has closed. This allows continuous accumulation
            // throughout the market window, matching account88888's strategy.
        } else {
            error!("   ❌ ALL ORDERS FAILED — no position entered");
        }
    }

    // -----------------------------------------------------------------------
    // Redemption
    // -----------------------------------------------------------------------

    /// Check all tracked positions and recover capital.
    ///
    /// DEFERRED MERGE STRATEGY (matching account88888):
    /// - During entry window: do NOT merge — keep accumulating pairs
    /// - After entry window closes: merge all accumulated pairs at once
    /// - After market resolution: redeem (fallback for one-sided positions)
    async fn check_and_redeem(&mut self) {
        let now = chrono::Utc::now().timestamp();

        for i in 0..self.positions.len() {
            if self.positions[i].redeemed {
                continue;
            }

            let market = self.positions[i].market.clone();

            // Skip if a background merge/redeem is already in progress for this market
            if self.executing_slugs.contains(&market.event_slug) {
                continue;
            }

            let up_bought = self.positions[i].up_tokens_bought;
            let down_bought = self.positions[i].down_tokens_bought;
            let total_cost = self.positions[i].total_cost;
            let up_cost = self.positions[i].up_cost_usdc;
            let down_cost = self.positions[i].down_cost_usdc;
            let min_tokens = up_bought.min(down_bought);

            // Check if we're still in the entry window — if so, keep accumulating, don't merge yet.
            let entry_window = ((market.event_end_ts - market.event_start_ts) as f64 * ENTRY_WINDOW_FRACTION) as i64;
            let entry_deadline = market.event_start_ts + entry_window;
            let still_in_entry_window = now <= entry_deadline;

            if still_in_entry_window {
                // Still accumulating — don't merge yet.
                // Log position status periodically (handled by status_interval).
                continue;
            }

            // Entry window closed — merge all accumulated pairs at once.
            if min_tokens >= 1.0 {
                let api_pairs = min_tokens.floor();
                info!("🔄 Entry window closed — merging {:.0} pairs for {} (${:.2} deployed)",
                      api_pairs, market.title, total_cost);

                // Mark as executing to prevent duplicate spawns on next tick
                self.executing_slugs.insert(market.event_slug.clone());

                let merge_tx = self.merge_tx.clone();
                let config = self.config.clone();
                let executor = self.executor.clone();
                let event_slug = market.event_slug.clone();
                let condition_id = market.condition_id.clone();
                let up_token_id = market.up_token_id.clone();
                let down_token_id = market.down_token_id.clone();

                tokio::spawn(async move {
                    // Brief settle delay for last CLOB orders to clear
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    let result = run_background_merge(
                        &config, &condition_id, &up_token_id, &down_token_id,
                        api_pairs, up_bought, down_bought,
                        up_cost, down_cost, total_cost,
                        &event_slug, &executor,
                    ).await;
                    let _ = merge_tx.send(result).await;
                });

                continue; // Don't block — result arrives via merge_rx
            }

            // Strategy 2: Redeem after resolution (fallback for one-sided positions or merge failures)
            if now < market.event_end_ts + 300 {
                continue; // Too early for resolution check
            }

            // Resolution check is a lightweight REST call (~100ms), OK to do synchronously
            match self.check_market_resolution(&market).await {
                Ok(Some(winning_outcome)) => {
                    let redemption_value = match winning_outcome.as_str() {
                        "Up" => up_bought,
                        "Down" => down_bought,
                        _ => 0.0,
                    };
                    let profit = redemption_value - total_cost;

                    info!("🏆 MARKET RESOLVED: {} → {} | Redeem: ${:.2} | Profit: ${:.2}",
                          market.title, winning_outcome, redemption_value, profit);

                    // Spawn redeem as background task (on-chain tx can take 120s)
                    self.executing_slugs.insert(market.event_slug.clone());
                    let merge_tx = self.merge_tx.clone();
                    let config = self.config.clone();
                    let executor = self.executor.clone();
                    let event_slug = market.event_slug.clone();
                    let condition_id = market.condition_id.clone();
                    let up_token_id = market.up_token_id.clone();
                    let down_token_id = market.down_token_id.clone();

                    tokio::spawn(async move {
                        let result = run_background_redeem(
                            &config, &condition_id, &up_token_id, &down_token_id,
                            redemption_value, profit, &event_slug, &executor,
                        ).await;
                        let _ = merge_tx.send(result).await;
                    });
                    // Position NOT marked redeemed here — executing_slugs prevents
                    // duplicate spawns, and handle_merge_result marks it on success.
                    // If redeem fails, it will be retried next cycle.
                }
                Ok(None) => {} // Not resolved yet
                Err(e) => {
                    warn!("Resolution check failed for {}: {}", market.title, e);
                }
            }
        }

        // Positions are cleaned up by handle_merge_result when background tasks complete
    }

    /// Check if a market has resolved and what the winning outcome is.
    async fn check_market_resolution(&self, market: &ArbMarket) -> Result<Option<String>, String> {
        // Fetch the event by ID — this is reliable unlike slug-based queries
        let url = format!(
            "https://gamma-api.polymarket.com/events/{}",
            market.event_id
        );

        let resp = self.client.get(&url).send().await
            .map_err(|e| format!("Resolution check error: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("Resolution API returned {}", resp.status()));
        }

        let event: serde_json::Value = resp.json().await
            .map_err(|e| format!("Resolution JSON error: {}", e))?;

        // Get the first market from the event
        let markets = match event.get("markets").and_then(|v| v.as_array()) {
            Some(m) => m,
            None => return Ok(None),
        };

        if let Some(m) = markets.first() {
            let closed = m.get("closed").and_then(|v| v.as_bool()).unwrap_or(false);
            if !closed {
                return Ok(None); // Not resolved yet
            }

            // Check outcome prices — the winning outcome will be at $1.00
            let prices_str = m.get("outcomePrices").and_then(|v| v.as_str()).unwrap_or("[]");
            let prices: Vec<String> = serde_json::from_str(prices_str).unwrap_or_default();

            let outcomes_str = m.get("outcomes").and_then(|v| v.as_str()).unwrap_or("[]");
            let outcomes: Vec<String> = serde_json::from_str(outcomes_str).unwrap_or_default();

            for (i, price_str) in prices.iter().enumerate() {
                if let Ok(price) = price_str.parse::<f64>() {
                    if price > 0.99 && i < outcomes.len() {
                        return Ok(Some(outcomes[i].clone()));
                    }
                }
            }

            // Market closed but no clear winner? Shouldn't happen for Up/Down
            warn!("Market {} closed but no clear winner: prices={:?}", market.title, prices);
            return Ok(None);
        }

        Ok(None)
    }


    // -----------------------------------------------------------------------
    // On-chain merge & redemption
    // -----------------------------------------------------------------------

    /// Merge positions on-chain via CTF `mergePositions` — INSTANT capital recovery.
    ///
    /// Burns equal amounts of BOTH Up and Down tokens → returns $1 per pair in USDC.e.
    /// Works BEFORE resolution — no need to wait for the market to close.
    /// This is the core of the Account88888 strategy: buy both sides, merge pairs, pocket spread.
    ///
    /// BTC/ETH Up/Down hourly markets are negRisk=false (verified via live API),
    /// so we call the standard CTF contract directly.
    ///
    /// CTF.mergePositions(IERC20 collateralToken, bytes32 parentCollectionId,
    ///                    bytes32 conditionId, uint256[] partition, uint256 amount)
    /// Source: https://github.com/gnosis/conditional-tokens-contracts/blob/master/contracts/ConditionalTokens.sol
    /// Polymarket docs: https://docs.polymarket.com/developers/CTF/merge
    async fn merge_on_chain(
        &self,
        condition_id: &str,
        pairs_to_merge: f64,
    ) -> Result<String, String> {
        use ethers::prelude::*;

        if pairs_to_merge < 1.0 {
            return Err("Nothing to merge (< 1 pair)".to_string());
        }

        let rpc_url = &self.config.polymarket.polygon_rpc;
        let provider = Provider::<Http>::try_from(rpc_url.as_str())
            .map_err(|e| format!("RPC provider error: {}", e))?;

        let private_key = &self.config.polymarket.private_key;
        let wallet: LocalWallet = private_key.parse::<LocalWallet>()
            .map_err(|e| format!("Wallet parse error: {}", e))?
            .with_chain_id(137u64); // Polygon

        let client = SignerMiddleware::new(provider, wallet);

        // CTF (Conditional Tokens Framework) contract on Polygon
        let ctf_addr: Address = "0x4D97DcD97Ec945F40CF65F87097aCe5EA0476045"
            .parse()
            .map_err(|e| format!("CTF address parse error: {}", e))?;

        // USDC.e on Polygon — the collateral token for all Polymarket markets
        let usdc_e: Address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
            .parse()
            .map_err(|e| format!("USDC address parse error: {}", e))?;

        let condition_bytes = Self::parse_condition_id(condition_id)?;
        let parent_collection = [0u8; 32]; // bytes32(0) — always null for Polymarket

        // Amount in raw units: 1 token = 1_000_000 units (USDC.e has 6 decimals)
        let amount_raw = U256::from((pairs_to_merge * 1_000_000.0) as u64);

        // mergePositions(address,bytes32,bytes32,uint256[],uint256)
        // partition = [1, 2] — indexSet 1 (0b01) = outcome 0 (Up), indexSet 2 (0b10) = outcome 1 (Down)
        let fn_selector = &ethers::core::utils::keccak256(
            b"mergePositions(address,bytes32,bytes32,uint256[],uint256)"
        )[..4];

        let encoded_params = ethers::abi::encode(&[
            ethers::abi::Token::Address(usdc_e),
            ethers::abi::Token::FixedBytes(parent_collection.to_vec()),
            ethers::abi::Token::FixedBytes(condition_bytes.to_vec()),
            ethers::abi::Token::Array(vec![
                ethers::abi::Token::Uint(U256::from(1)),  // indexSet for outcome 0 (Up)
                ethers::abi::Token::Uint(U256::from(2)),  // indexSet for outcome 1 (Down)
            ]),
            ethers::abi::Token::Uint(amount_raw),
        ]);

        let mut calldata = fn_selector.to_vec();
        calldata.extend_from_slice(&encoded_params);

        // Estimate gas using a legacy tx (cheaper RPC call)
        let estimate_tx = TransactionRequest::new()
            .to(ctf_addr)
            .data(calldata.clone())
            .from(client.address());

        let gas_estimate = client.estimate_gas(&estimate_tx.into(), None).await
            .map_err(|e| format!("Merge gas estimation failed: {}", e))?;

        // Build EIP-1559 tx with dynamic gas pricing
        let typed_tx = Self::build_eip1559_tx(
            client.provider(), ctf_addr, calldata, client.address(), gas_estimate
        ).await?;

        let pending_tx = client.send_transaction(typed_tx, None).await
            .map_err(|e| format!("Merge tx send failed: {}", e))?;

        let tx_hash = format!("{:?}", pending_tx.tx_hash());
        info!("   🔗 Merge tx sent: {} (merging {:.0} pairs...)", tx_hash, pairs_to_merge);

        Self::wait_for_tx(pending_tx, tx_hash, "Merge").await
    }

    /// Redeem positions on-chain via CTF `redeemPositions` — POST-resolution fallback.
    ///
    /// Burns winning tokens and returns collateral. Only works after market resolution.
    /// Used as fallback when merge fails or for excess tokens on one side.
    ///
    /// BTC/ETH Up/Down hourly markets are negRisk=false (verified via live API),
    /// so we call the standard CTF contract directly.
    ///
    /// CTF.redeemPositions(IERC20 collateralToken, bytes32 parentCollectionId,
    ///                     bytes32 conditionId, uint256[] indexSets)
    /// indexSets = [1, 2] — redeems ALL held tokens for both outcomes.
    /// The contract automatically redeems based on the caller's on-chain balance.
    /// Source: https://github.com/gnosis/conditional-tokens-contracts/blob/master/contracts/ConditionalTokens.sol
    /// Polymarket docs: https://docs.polymarket.com/developers/CTF/redeem
    async fn redeem_on_chain(
        &self,
        condition_id: &str,
    ) -> Result<String, String> {
        use ethers::prelude::*;

        let rpc_url = &self.config.polymarket.polygon_rpc;
        let provider = Provider::<Http>::try_from(rpc_url.as_str())
            .map_err(|e| format!("RPC provider error: {}", e))?;

        let private_key = &self.config.polymarket.private_key;
        let wallet: LocalWallet = private_key.parse::<LocalWallet>()
            .map_err(|e| format!("Wallet parse error: {}", e))?
            .with_chain_id(137u64); // Polygon

        let client = SignerMiddleware::new(provider, wallet);

        // CTF (Conditional Tokens Framework) contract on Polygon
        let ctf_addr: Address = "0x4D97DcD97Ec945F40CF65F87097aCe5EA0476045"
            .parse()
            .map_err(|e| format!("CTF address parse error: {}", e))?;

        // USDC.e on Polygon — the collateral token
        let usdc_e: Address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
            .parse()
            .map_err(|e| format!("USDC address parse error: {}", e))?;

        let condition_bytes = Self::parse_condition_id(condition_id)?;
        let parent_collection = [0u8; 32]; // bytes32(0) — always null for Polymarket

        // redeemPositions(address,bytes32,bytes32,uint256[])
        // indexSets = [1, 2] — both outcomes. In the Polymarket case: 1|2.
        let fn_selector = &ethers::core::utils::keccak256(
            b"redeemPositions(address,bytes32,bytes32,uint256[])"
        )[..4];

        let encoded_params = ethers::abi::encode(&[
            ethers::abi::Token::Address(usdc_e),
            ethers::abi::Token::FixedBytes(parent_collection.to_vec()),
            ethers::abi::Token::FixedBytes(condition_bytes.to_vec()),
            ethers::abi::Token::Array(vec![
                ethers::abi::Token::Uint(U256::from(1)),  // indexSet for outcome 0 (Up)
                ethers::abi::Token::Uint(U256::from(2)),  // indexSet for outcome 1 (Down)
            ]),
        ]);

        let mut calldata = fn_selector.to_vec();
        calldata.extend_from_slice(&encoded_params);

        // Estimate gas using a legacy tx (cheaper RPC call)
        let estimate_tx = TransactionRequest::new()
            .to(ctf_addr)
            .data(calldata.clone())
            .from(client.address());

        let gas_estimate = client.estimate_gas(&estimate_tx.into(), None).await
            .map_err(|e| format!("Redeem gas estimation failed: {}", e))?;

        // Build EIP-1559 tx with dynamic gas pricing
        let typed_tx = Self::build_eip1559_tx(
            client.provider(), ctf_addr, calldata, client.address(), gas_estimate
        ).await?;

        let pending_tx = client.send_transaction(typed_tx, None).await
            .map_err(|e| format!("Redeem tx send failed: {}", e))?;

        let tx_hash = format!("{:?}", pending_tx.tx_hash());
        info!("   🔗 Redeem tx sent: {} (waiting for confirmation...)", tx_hash);

        Self::wait_for_tx(pending_tx, tx_hash, "Redeem").await
    }

    /// Build an EIP-1559 transaction with dynamic gas pricing from the Polygon network.
    ///
    /// Uses eth_maxPriorityFeePerGas RPC call + latest block baseFeePerGas to compute:
    /// - max_priority_fee_per_gas: tip for validators (from RPC suggestion)
    /// - max_fee_per_gas: baseFee * 2 + priority_fee (2x buffer for base fee volatility)
    ///
    /// Falls back to legacy TransactionRequest if EIP-1559 data is unavailable.
    /// Polygon has supported EIP-1559 since the London hard fork.
    /// Docs: https://docs.polygon.technology/pos/concepts/transactions/eip-1559/
    async fn build_eip1559_tx(
        provider: &ethers::providers::Provider<ethers::providers::Http>,
        to: ethers::types::Address,
        calldata: Vec<u8>,
        from: ethers::types::Address,
        gas_estimate: ethers::types::U256,
    ) -> Result<ethers::types::transaction::eip2718::TypedTransaction, String> {
        use ethers::prelude::*;

        let gas_limit = gas_estimate * 120 / 100; // 20% buffer

        // Try to get EIP-1559 gas parameters from the network
        let eip1559_result: Result<(U256, U256), String> = async {
            // Get suggested priority fee from the node
            let priority_fee: U256 = provider.request("eth_maxPriorityFeePerGas", ())
                .await
                .map_err(|e| format!("eth_maxPriorityFeePerGas failed: {}", e))?;

            // Get base fee from latest block
            let block = provider.get_block(BlockNumber::Latest)
                .await
                .map_err(|e| format!("get_block failed: {}", e))?
                .ok_or_else(|| "No latest block".to_string())?;

            let base_fee = block.base_fee_per_gas
                .ok_or_else(|| "No baseFeePerGas in block (pre-London?)".to_string())?;

            // max_fee = 2 * baseFee + priorityFee
            // The 2x multiplier on baseFee handles up to ~6 consecutive full blocks
            // of base fee increases before the tx becomes unsubmittable.
            let max_fee = base_fee * 2 + priority_fee;

            Ok((max_fee, priority_fee))
        }.await;

        match eip1559_result {
            Ok((max_fee, priority_fee)) => {
                let tx = Eip1559TransactionRequest::new()
                    .to(to)
                    .data(calldata)
                    .from(from)
                    .gas(gas_limit)
                    .max_fee_per_gas(max_fee)
                    .max_priority_fee_per_gas(priority_fee)
                    .chain_id(137u64); // Polygon

                debug!("⛽ EIP-1559 gas: maxFee={} gwei, priorityFee={} gwei, gasLimit={}",
                       max_fee.as_u64() / 1_000_000_000,
                       priority_fee.as_u64() / 1_000_000_000,
                       gas_limit);

                Ok(tx.into())
            }
            Err(e) => {
                // Fallback to legacy tx if EIP-1559 data unavailable
                warn!("⛽ EIP-1559 gas query failed: {} — using legacy tx", e);
                let tx = TransactionRequest::new()
                    .to(to)
                    .data(calldata)
                    .from(from)
                    .gas(gas_limit);
                Ok(tx.into())
            }
        }
    }

    /// Parse a hex condition ID string into a 32-byte array.
    fn parse_condition_id(condition_id: &str) -> Result<[u8; 32], String> {
        let hex_str = condition_id.strip_prefix("0x").unwrap_or(condition_id);
        let bytes = hex::decode(hex_str)
            .map_err(|e| format!("Condition ID hex decode error: {}", e))?;
        if bytes.len() != 32 {
            return Err(format!("Condition ID wrong length: {} bytes", bytes.len()));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(arr)
    }

    /// Wait for a pending transaction with timeout.
    async fn wait_for_tx(
        pending_tx: ethers::providers::PendingTransaction<'_, ethers::providers::Http>,
        tx_hash: String,
        label: &str,
    ) -> Result<String, String> {
        use ethers::prelude::*;

        match tokio::time::timeout(
            std::time::Duration::from_secs(120),
            pending_tx,
        ).await {
            Ok(Ok(Some(receipt))) => {
                if receipt.status == Some(U64::from(1)) {
                    info!("   ✅ {} confirmed in block {}", label, receipt.block_number.unwrap_or_default());
                    Ok(tx_hash)
                } else {
                    Err(format!("{} tx reverted: {}", label, tx_hash))
                }
            }
            Ok(Ok(None)) => {
                warn!("   ⏳ {} tx pending (no receipt yet): {}", label, tx_hash);
                Ok(tx_hash)
            }
            Ok(Err(e)) => Err(format!("{} tx error: {}", label, e)),
            Err(_) => {
                warn!("   ⏳ {} tx timeout (120s) — check manually: {}", label, tx_hash);
                Ok(tx_hash)
            }
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Query on-chain ERC-1155 balance for a conditional token.
    /// Returns the balance in human-readable units (NOT raw 6-decimal).
    /// Used as safety check before merge to ensure we actually hold the tokens.
    async fn query_token_balance(&self, token_id: &str) -> Result<f64, String> {
        use ethers::prelude::*;

        let rpc_url = &self.config.polymarket.polygon_rpc;
        let provider = Provider::<Http>::try_from(rpc_url.as_str())
            .map_err(|e| format!("RPC provider error: {}", e))?;

        let ctf_addr: Address = "0x4D97DcD97Ec945F40CF65F87097aCe5EA0476045"
            .parse()
            .map_err(|e| format!("CTF address parse error: {}", e))?;

        let private_key = &self.config.polymarket.private_key;
        let wallet: ethers::signers::LocalWallet = private_key.parse::<ethers::signers::LocalWallet>()
            .map_err(|e| format!("Wallet parse error: {}", e))?;
        let owner: Address = wallet.address();

        // ERC-1155 balanceOf(address,uint256) — selector 0x00fdd58e
        let token_id_u256 = U256::from_dec_str(token_id)
            .map_err(|e| format!("Token ID parse error: {}", e))?;

        let call_data = ethers::abi::encode(&[
            ethers::abi::Token::Address(owner),
            ethers::abi::Token::Uint(token_id_u256),
        ]);
        let mut full_data = vec![0x00, 0xfd, 0xd5, 0x8e]; // balanceOf selector
        full_data.extend_from_slice(&call_data);

        let tx = TransactionRequest::new()
            .to(ctf_addr)
            .data(full_data);

        let result = provider.call(&tx.into(), None).await
            .map_err(|e| format!("balanceOf call failed: {}", e))?;

        if result.len() < 32 {
            return Err(format!("balanceOf returned {} bytes, expected 32", result.len()));
        }

        let balance_raw = U256::from_big_endian(&result[..32]);
        // Convert from raw 6-decimal units to human-readable
        let balance_f = balance_raw.as_u128() as f64 / 1_000_000.0;
        Ok(balance_f)
    }

    fn has_position(&self, event_slug: &str) -> bool {
        self.positions.iter().any(|p| p.market.event_slug == event_slug)
    }

    fn deployed_capital(&self) -> f64 {
        self.positions.iter().map(|p| p.total_cost).sum()
    }
}

// ---------------------------------------------------------------------------
// Parallel order submission helper
// ---------------------------------------------------------------------------

/// Submit one side's (Up or Down) signed orders in batches of 15.
/// Returns (orders_ok, tokens_total, cost_total).
/// Defined as a standalone async fn so it can be called from tokio::join!
/// without async closure lifetime issues.
async fn submit_order_side(
    executor: &OrderExecutor,
    orders: Vec<PreparedOrder>,
    side_label: &str,
) -> (u32, f64, f64) {
    use rust_decimal::prelude::ToPrimitive;

    const BATCH_LIMIT: usize = 15;
    const INTER_BATCH_DELAY_MS: u64 = 20;

    let mut orders_ok = 0u32;
    let mut tokens_total = 0.0_f64;
    let mut cost_total = 0.0_f64;

    for (batch_idx, chunk) in orders.chunks(BATCH_LIMIT).enumerate() {
        if batch_idx > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(INTER_BATCH_DELAY_MS)).await;
        }

        let batch_orders: Vec<PreparedOrder> = chunk.to_vec();
        let results = executor.submit_orders_batch(&batch_orders).await;

        for (k, result) in results.into_iter().enumerate() {
            if k >= chunk.len() { break; }
            let requested_size = chunk[k].size.to_f64().unwrap_or(0.0);
            let requested_price = chunk[k].price.to_f64().unwrap_or(0.0);

            match result {
                Ok(fill) => {
                    let actual_tokens = fill.tokens_filled();
                    let actual_cost = fill.usdc_amount();

                    let is_delayed = fill.status == "DELAYED";
                    let tokens = if actual_tokens > 0.0 {
                        actual_tokens
                    } else if is_delayed {
                        warn!("   ⏳ {} #{}: DELAYED — using requested size {:.0} as estimate",
                              side_label, k + 1, requested_size);
                        requested_size
                    } else {
                        0.0
                    };
                    let cost = if actual_cost > 0.0 {
                        actual_cost
                    } else if is_delayed {
                        requested_size * requested_price
                    } else {
                        0.0
                    };

                    orders_ok += 1;
                    tokens_total += tokens;
                    cost_total += cost;

                    if actual_tokens > 0.0 && actual_tokens < requested_size * 0.99 {
                        info!("   ✅ {} #{}: {:.1}/{:.0} tokens @ {:.3} (partial fill) → {} [{}]",
                              side_label, k + 1, actual_tokens, requested_size,
                              requested_price, fill.order_id, fill.status);
                    } else {
                        info!("   ✅ {} #{}: {:.0} tokens @ {:.3} → {} [{}]",
                              side_label, k + 1, tokens, requested_price,
                              fill.order_id, fill.status);
                    }
                }
                Err(ref e) => {
                    warn!("   ❌ {} #{} failed: {}", side_label, k + 1, e);
                }
            }
        }
    }

    (orders_ok, tokens_total, cost_total)
}

// ---------------------------------------------------------------------------
// WebSocket Book Watcher (runs as a separate tokio task)
// ---------------------------------------------------------------------------

/// Connects to the Polymarket CLOB WebSocket market channel and streams
/// real-time order book updates to the arb engine.
///
/// Protocol (from docs: https://docs.polymarket.com/developers/CLOB/websocket/market-channel):
/// - URL: wss://ws-subscriptions-clob.polymarket.com/ws/market
/// - Subscribe: {"assets_ids": [...], "type": "market"}
/// - Dynamic subscribe: {"assets_ids": [...], "operation": "subscribe"}
/// - Dynamic unsubscribe: {"assets_ids": [...], "operation": "unsubscribe"}
/// - Keepalive: send "PING" every 10 seconds
/// - Events: book, price_change, last_trade_price, market_resolved, new_market
async fn run_book_watcher(
    event_tx: mpsc::Sender<ArbWsEvent>,
    mut cmd_rx: mpsc::Receiver<WsCommand>,
    shutdown: Arc<AtomicBool>,
) {
    // Collect initial subscriptions that arrive before we're connected
    let mut pending_subs: Vec<String> = Vec::new();
    let mut backoff_secs = 1u64;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("📡 Book watcher shutting down");
            return;
        }

        // Wait for at least one asset subscription before connecting.
        // The Polymarket CLOB WS server resets connections with no initial subscription.
        while pending_subs.is_empty() {
            match cmd_rx.recv().await {
                Some(WsCommand::Subscribe(ids)) => {
                    for id in ids {
                        if !pending_subs.contains(&id) {
                            pending_subs.push(id);
                        }
                    }
                }
                Some(WsCommand::Unsubscribe(ids)) => {
                    pending_subs.retain(|id| !ids.contains(id));
                }
                None => {
                    info!("📡 Command channel closed — stopping book watcher");
                    return;
                }
            }
            if shutdown.load(Ordering::Relaxed) { return; }
        }

        info!("📡 Connecting to CLOB WebSocket: {} ({} assets pending)", CLOB_WS_MARKET_URL, pending_subs.len());

        match connect_async(CLOB_WS_MARKET_URL).await {
            Ok((ws_stream, _)) => {
                let connect_time = std::time::Instant::now();
                metrics::ARB_WS_RECONNECTS.inc();
                let _ = event_tx.send(ArbWsEvent::Connected(true)).await;

                let (mut write, mut read) = ws_stream.split();

                // Send initial subscription in batches to avoid server reset.
                // Polymarket WS drops connections when subscribe payload is too large.
                if !pending_subs.is_empty() {
                    let total = pending_subs.len();
                    let mut sent = 0usize;
                    let mut failed = false;
                    for chunk in pending_subs.chunks(WS_SUB_BATCH_SIZE) {
                        let sub_msg = if sent == 0 {
                            // First batch uses "type": "market" for initial connection
                            serde_json::json!({
                                "assets_ids": chunk,
                                "type": "market"
                            })
                        } else {
                            // Subsequent batches use "operation": "subscribe"
                            serde_json::json!({
                                "assets_ids": chunk,
                                "operation": "subscribe"
                            })
                        };
                        if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
                            warn!("📡 WS initial subscribe batch failed: {}", e);
                            failed = true;
                            break;
                        }
                        sent += chunk.len();
                        // Small delay between batches to avoid overwhelming the server
                        if sent < total {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                    }
                    if !failed {
                        info!("📡 WS subscribed to {} assets on connect ({} batches)",
                              total, (total + WS_SUB_BATCH_SIZE - 1) / WS_SUB_BATCH_SIZE);
                    }
                }

                // Keepalive timer — docs say send "PING" every 10 seconds
                let mut ping_interval = tokio::time::interval(
                    std::time::Duration::from_secs(WS_PING_INTERVAL_SECS)
                );

                loop {
                    tokio::select! {
                        // Incoming WS message
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    parse_and_dispatch_ws_message(&text, &event_tx).await;
                                }
                                Some(Ok(Message::Binary(data))) => {
                                    if let Ok(text) = String::from_utf8(data) {
                                        parse_and_dispatch_ws_message(&text, &event_tx).await;
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    let _ = write.send(Message::Pong(data)).await;
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("📡 WS closed by server");
                                    break;
                                }
                                Some(Err(e)) => {
                                    warn!("📡 WS read error: {}", e);
                                    break;
                                }
                                None => {
                                    warn!("📡 WS stream ended");
                                    break;
                                }
                                _ => {}
                            }
                        }

                        // Commands from arb engine (subscribe/unsubscribe)
                        cmd = cmd_rx.recv() => {
                            match cmd {
                                Some(WsCommand::Subscribe(ids)) => {
                                    if ids.is_empty() { continue; }
                                    // Track for reconnect
                                    for id in &ids {
                                        if !pending_subs.contains(id) {
                                            pending_subs.push(id.clone());
                                        }
                                    }
                                    // Batch subscribe to avoid oversized messages
                                    let total = ids.len();
                                    let mut sub_failed = false;
                                    for chunk in ids.chunks(WS_SUB_BATCH_SIZE) {
                                        let sub_msg = serde_json::json!({
                                            "assets_ids": chunk,
                                            "operation": "subscribe"
                                        });
                                        if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
                                            warn!("📡 WS subscribe batch failed: {}", e);
                                            sub_failed = true;
                                            break;
                                        }
                                        if chunk.len() == WS_SUB_BATCH_SIZE {
                                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                        }
                                    }
                                    if sub_failed { break; }
                                    debug!("📡 WS subscribed to {} new assets", total);
                                }
                                Some(WsCommand::Unsubscribe(ids)) => {
                                    if ids.is_empty() { continue; }
                                    pending_subs.retain(|id| !ids.contains(id));
                                    let unsub_msg = serde_json::json!({
                                        "assets_ids": ids,
                                        "operation": "unsubscribe"
                                    });
                                    if let Err(e) = write.send(Message::Text(unsub_msg.to_string())).await {
                                        warn!("📡 WS unsubscribe failed: {}", e);
                                        break;
                                    }
                                    debug!("📡 WS unsubscribed {} assets", ids.len());
                                }
                                None => {
                                    info!("📡 Command channel closed — stopping book watcher");
                                    return;
                                }
                            }
                        }

                        // Keepalive ping
                        _ = ping_interval.tick() => {
                            if let Err(e) = write.send(Message::Text("PING".to_string())).await {
                                warn!("📡 WS ping failed: {}", e);
                                break;
                            }
                        }
                    }
                }

                // Disconnected — notify engine
                let _ = event_tx.send(ArbWsEvent::Connected(false)).await;

                // Only reset backoff if connection was stable (>5s)
                if connect_time.elapsed().as_secs() > 5 {
                    backoff_secs = 1;
                }
            }
            Err(e) => {
                warn!("📡 WS connect failed: {} — retrying in {}s", e, backoff_secs);
            }
        }

        // Drain any commands that arrived while disconnected
        while let Ok(cmd) = cmd_rx.try_recv() {
            match cmd {
                WsCommand::Subscribe(ids) => {
                    for id in ids {
                        if !pending_subs.contains(&id) {
                            pending_subs.push(id);
                        }
                    }
                }
                WsCommand::Unsubscribe(ids) => {
                    pending_subs.retain(|id| !ids.contains(id));
                }
            }
        }

        // Exponential backoff with cap at 30 seconds
        tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
        backoff_secs = (backoff_secs * 2).min(30);
    }
}

/// Build an EIP-1559 transaction with dynamic gas pricing (standalone version for background tasks).
/// Queries eth_maxPriorityFeePerGas + latest block baseFeePerGas from the Polygon RPC.
/// Falls back to legacy TransactionRequest if EIP-1559 data is unavailable.
async fn build_eip1559_tx_standalone(
    provider: &ethers::providers::Provider<ethers::providers::Http>,
    to: ethers::types::Address,
    calldata: Vec<u8>,
    from: ethers::types::Address,
    gas_estimate: ethers::types::U256,
) -> Result<ethers::types::transaction::eip2718::TypedTransaction, String> {
    use ethers::prelude::*;

    let gas_limit = gas_estimate * 120 / 100;

    let eip1559_result: Result<(U256, U256), String> = async {
        let priority_fee: U256 = provider.request("eth_maxPriorityFeePerGas", ())
            .await
            .map_err(|e| format!("eth_maxPriorityFeePerGas failed: {}", e))?;

        let block = provider.get_block(BlockNumber::Latest)
            .await
            .map_err(|e| format!("get_block failed: {}", e))?
            .ok_or_else(|| "No latest block".to_string())?;

        let base_fee = block.base_fee_per_gas
            .ok_or_else(|| "No baseFeePerGas in block".to_string())?;

        let max_fee = base_fee * 2 + priority_fee;
        Ok((max_fee, priority_fee))
    }.await;

    match eip1559_result {
        Ok((max_fee, priority_fee)) => {
            let tx = Eip1559TransactionRequest::new()
                .to(to)
                .data(calldata)
                .from(from)
                .gas(gas_limit)
                .max_fee_per_gas(max_fee)
                .max_priority_fee_per_gas(priority_fee)
                .chain_id(137u64);

            debug!("⛽ [BG] EIP-1559 gas: maxFee={} gwei, priorityFee={} gwei, gasLimit={}",
                   max_fee.as_u64() / 1_000_000_000,
                   priority_fee.as_u64() / 1_000_000_000,
                   gas_limit);

            Ok(tx.into())
        }
        Err(e) => {
            warn!("⛽ [BG] EIP-1559 gas query failed: {} — using legacy tx", e);
            let tx = TransactionRequest::new()
                .to(to)
                .data(calldata)
                .from(from)
                .gas(gas_limit);
            Ok(tx.into())
        }
    }
}

/// Run a merge operation in a background task.
/// This function performs ALL on-chain work that would otherwise block the event loop:
/// 1. Query on-chain ERC-1155 balances (verify tokens settled)
/// 2. Call CTF.mergePositions on-chain
/// 3. Wait for tx confirmation
/// 4. Notify CLOB about updated balances
/// 5. Calculate profit and excess positions
/// Returns a MergeResult for the engine to process.
async fn run_background_merge(
    config: &Config,
    condition_id: &str,
    up_token_id: &str,
    down_token_id: &str,
    api_pairs: f64,
    up_tokens_total: f64,
    down_tokens_total: f64,
    up_cost_total: f64,
    down_cost_total: f64,
    total_cost: f64,
    event_slug: &str,
    executor: &OrderExecutor,
) -> MergeResult {
    use ethers::prelude::*;

    let rpc_url = &config.polymarket.polygon_rpc;
    let private_key = &config.polymarket.private_key;

    // Helper: create provider + wallet (reused across steps)
    let provider = match Provider::<Http>::try_from(rpc_url.as_str()) {
        Ok(p) => p,
        Err(e) => return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: true,
            error: Some(format!("RPC provider error: {}", e)),
        },
    };

    let wallet: LocalWallet = match private_key.parse::<LocalWallet>() {
        Ok(w) => w.with_chain_id(137u64),
        Err(e) => return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: true,
            error: Some(format!("Wallet parse error: {}", e)),
        },
    };

    let owner: Address = wallet.address();
    let client = SignerMiddleware::new(provider.clone(), wallet);

    let ctf_addr: Address = "0x4D97DcD97Ec945F40CF65F87097aCe5EA0476045"
        .parse().unwrap();

    // Step 1: Query on-chain balances
    let query_balance = |token_id_str: &str| {
        let provider = provider.clone();
        let token_id_str = token_id_str.to_string();
        async move {
            let token_id_u256 = U256::from_dec_str(&token_id_str)
                .map_err(|e| format!("Token ID parse error: {}", e))?;
            let call_data = ethers::abi::encode(&[
                ethers::abi::Token::Address(owner),
                ethers::abi::Token::Uint(token_id_u256),
            ]);
            let mut full_data = vec![0x00, 0xfd, 0xd5, 0x8e];
            full_data.extend_from_slice(&call_data);
            let tx = TransactionRequest::new().to(ctf_addr).data(full_data);
            let result = provider.call(&tx.into(), None).await
                .map_err(|e| format!("balanceOf call failed: {}", e))?;
            if result.len() < 32 {
                return Err(format!("balanceOf returned {} bytes", result.len()));
            }
            let balance_raw = U256::from_big_endian(&result[..32]);
            Ok(balance_raw.as_u128() as f64 / 1_000_000.0)
        }
    };

    let (up_bal_result, down_bal_result) = tokio::join!(
        query_balance(up_token_id),
        query_balance(down_token_id),
    );

    let pairs_to_merge = match (up_bal_result, down_bal_result) {
        (Ok(up_bal), Ok(down_bal)) => {
            let safe_pairs = up_bal.min(down_bal).floor();
            if safe_pairs < api_pairs {
                warn!("   ⚠️  [BG] On-chain balance ({:.0} Up, {:.0} Down) < API fill ({:.0}) — using {:.0}",
                      up_bal, down_bal, api_pairs, safe_pairs);
            } else {
                info!("   ✅ [BG] On-chain balance confirmed: {:.0} Up, {:.0} Down", up_bal, down_bal);
            }
            safe_pairs
        }
        (Err(e), _) | (_, Err(e)) => {
            warn!("   ⚠️  [BG] Balance check failed: {} — using API amount {:.0}", e, api_pairs);
            api_pairs
        }
    };

    if pairs_to_merge < 1.0 {
        return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: true,
            error: Some("Not enough on-chain tokens to merge (< 1 pair)".to_string()),
        };
    }

    // Step 2: Execute merge on-chain
    info!("   🔄 [BG] Merging {:.0} pairs on-chain...", pairs_to_merge);

    let condition_bytes = match ArbEngine::parse_condition_id(condition_id) {
        Ok(b) => b,
        Err(e) => return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: true,
            error: Some(e),
        },
    };

    let usdc_e: Address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        .parse().unwrap();
    let parent_collection = [0u8; 32];
    let amount_raw = U256::from((pairs_to_merge * 1_000_000.0) as u64);

    let fn_selector = &ethers::core::utils::keccak256(
        b"mergePositions(address,bytes32,bytes32,uint256[],uint256)"
    )[..4];

    let encoded_params = ethers::abi::encode(&[
        ethers::abi::Token::Address(usdc_e),
        ethers::abi::Token::FixedBytes(parent_collection.to_vec()),
        ethers::abi::Token::FixedBytes(condition_bytes.to_vec()),
        ethers::abi::Token::Array(vec![
            ethers::abi::Token::Uint(U256::from(1)),
            ethers::abi::Token::Uint(U256::from(2)),
        ]),
        ethers::abi::Token::Uint(amount_raw),
    ]);

    let mut calldata = fn_selector.to_vec();
    calldata.extend_from_slice(&encoded_params);

    // Estimate gas using a legacy tx
    let estimate_tx = TransactionRequest::new()
        .to(ctf_addr)
        .data(calldata.clone())
        .from(client.address());

    let gas_estimate = match client.estimate_gas(&estimate_tx.into(), None).await {
        Ok(g) => g,
        Err(e) => return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: true,
            error: Some(format!("Gas estimation failed: {}", e)),
        },
    };

    // Build EIP-1559 tx with dynamic gas pricing
    let typed_tx = match build_eip1559_tx_standalone(
        client.provider(), ctf_addr, calldata, client.address(), gas_estimate
    ).await {
        Ok(tx) => tx,
        Err(e) => return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: true,
            error: Some(format!("EIP-1559 tx build failed: {}", e)),
        },
    };

    let pending_tx = match client.send_transaction(typed_tx, None).await {
        Ok(pt) => pt,
        Err(e) => return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: true,
            error: Some(format!("Tx send failed: {}", e)),
        },
    };

    let tx_hash = format!("{:?}", pending_tx.tx_hash());
    info!("   🔗 [BG] Merge tx sent: {}", tx_hash);

    // Wait for confirmation (up to 120s)
    let confirmed = match tokio::time::timeout(
        std::time::Duration::from_secs(120),
        pending_tx,
    ).await {
        Ok(Ok(Some(receipt))) => receipt.status == Some(U64::from(1)),
        Ok(Ok(None)) => {
            warn!("   ⏳ [BG] Merge tx pending (no receipt): {}", tx_hash);
            true // Optimistically assume success
        }
        Ok(Err(e)) => {
            return MergeResult {
                event_slug: event_slug.to_string(),
                usdc_recovered: 0.0, profit: 0.0,
                excess_position: None, was_merge: true,
                error: Some(format!("Tx error: {}", e)),
            };
        }
        Err(_) => {
            warn!("   ⏳ [BG] Merge tx timeout (120s): {}", tx_hash);
            true // Optimistically assume success (will verify on-chain later)
        }
    };

    if !confirmed {
        return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: true,
            error: Some(format!("Merge tx reverted: {}", tx_hash)),
        };
    }

    // Step 3: Calculate profit and excess
    let merge_revenue = pairs_to_merge; // $1 per pair
    let excess_up = up_tokens_total - pairs_to_merge;
    let excess_down = down_tokens_total - pairs_to_merge;

    let (profit, excess_position) = if excess_up > 1.0 || excess_down > 1.0 {
        // Pro-rata cost allocation for BOTH sides' excess tokens.
        // Old code only computed one side, which was wrong when both sides had excess
        // (e.g., when on-chain balance limited the merge below min(up, down)).
        let excess_up_cost = if excess_up > 0.0 && up_tokens_total > 0.0 {
            up_cost_total * (excess_up / up_tokens_total)
        } else { 0.0 };
        let excess_down_cost = if excess_down > 0.0 && down_tokens_total > 0.0 {
            down_cost_total * (excess_down / down_tokens_total)
        } else { 0.0 };
        let excess_cost = excess_up_cost + excess_down_cost;
        let merged_cost = total_cost - excess_cost;
        let profit = merge_revenue - merged_cost;

        let excess = CyclePosition {
            market: ArbMarket {
                title: String::new(),
                event_id: String::new(),
                event_slug: event_slug.to_string(),
                condition_id: condition_id.to_string(),
                up_token_id: up_token_id.to_string(),
                down_token_id: down_token_id.to_string(),
                neg_risk: false,
                tick_size: 0.01,
                min_size: 5.0,
                event_start_ts: 0,
                event_end_ts: 0,
                asset: ArbAsset::BTC, // Will be corrected by position lookup
                up_fee_bps: 0,
                down_fee_bps: 0,
            },
            up_tokens_bought: excess_up,
            down_tokens_bought: excess_down,
            up_cost_usdc: excess_up_cost,
            down_cost_usdc: excess_down_cost,
            total_cost: excess_cost,
            expected_profit: 0.0,
            entered_at: chrono::Utc::now(),
            redeemed: false,
        };
        (profit, Some(excess))
    } else {
        (merge_revenue - total_cost, None)
    };

    // Step 4: Notify CLOB about updated balance
    if let Err(e) = executor.update_clob_balance_allowance(&[
        up_token_id.to_string(), down_token_id.to_string(),
    ]).await {
        warn!("   ⚠️  [BG] CLOB balance sync failed: {}", e);
    }

    info!("   ✅ [BG] Merge confirmed: {:.0} pairs, +${:.2} profit | Tx: {}",
          pairs_to_merge, profit, tx_hash);

    MergeResult {
        event_slug: event_slug.to_string(),
        usdc_recovered: merge_revenue,
        profit,
        excess_position,
        was_merge: true,
        error: None,
    }
}

/// Run a redemption operation in a background task (post-resolution fallback).
/// Burns winning tokens on-chain and returns collateral.
async fn run_background_redeem(
    config: &Config,
    condition_id: &str,
    up_token_id: &str,
    down_token_id: &str,
    redemption_value: f64,
    profit: f64,
    event_slug: &str,
    executor: &OrderExecutor,
) -> MergeResult {
    use ethers::prelude::*;

    let rpc_url = &config.polymarket.polygon_rpc;
    let private_key = &config.polymarket.private_key;

    let provider = match Provider::<Http>::try_from(rpc_url.as_str()) {
        Ok(p) => p,
        Err(e) => return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: false,
            error: Some(format!("RPC provider error: {}", e)),
        },
    };

    let wallet: LocalWallet = match private_key.parse::<LocalWallet>() {
        Ok(w) => w.with_chain_id(137u64),
        Err(e) => return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: false,
            error: Some(format!("Wallet parse error: {}", e)),
        },
    };

    let client = SignerMiddleware::new(provider, wallet);

    let ctf_addr: Address = "0x4D97DcD97Ec945F40CF65F87097aCe5EA0476045"
        .parse().unwrap();
    let usdc_e: Address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        .parse().unwrap();

    let condition_bytes = match ArbEngine::parse_condition_id(condition_id) {
        Ok(b) => b,
        Err(e) => return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: false,
            error: Some(e),
        },
    };

    let parent_collection = [0u8; 32];

    // redeemPositions(address,bytes32,bytes32,uint256[])
    let fn_selector = &ethers::core::utils::keccak256(
        b"redeemPositions(address,bytes32,bytes32,uint256[])"
    )[..4];

    let encoded_params = ethers::abi::encode(&[
        ethers::abi::Token::Address(usdc_e),
        ethers::abi::Token::FixedBytes(parent_collection.to_vec()),
        ethers::abi::Token::FixedBytes(condition_bytes.to_vec()),
        ethers::abi::Token::Array(vec![
            ethers::abi::Token::Uint(U256::from(1)),
            ethers::abi::Token::Uint(U256::from(2)),
        ]),
    ]);

    let mut calldata = fn_selector.to_vec();
    calldata.extend_from_slice(&encoded_params);

    // Estimate gas using a legacy tx
    let estimate_tx = TransactionRequest::new()
        .to(ctf_addr)
        .data(calldata.clone())
        .from(client.address());

    let gas_estimate = match client.estimate_gas(&estimate_tx.into(), None).await {
        Ok(g) => g,
        Err(e) => return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: false,
            error: Some(format!("Redeem gas estimation failed: {}", e)),
        },
    };

    // Build EIP-1559 tx with dynamic gas pricing
    // Use client.provider() since `provider` was moved into SignerMiddleware
    let typed_tx = match build_eip1559_tx_standalone(
        client.provider(), ctf_addr, calldata, client.address(), gas_estimate
    ).await {
        Ok(tx) => tx,
        Err(e) => return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: false,
            error: Some(format!("EIP-1559 tx build failed: {}", e)),
        },
    };

    let pending_tx = match client.send_transaction(typed_tx, None).await {
        Ok(pt) => pt,
        Err(e) => return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: false,
            error: Some(format!("Redeem tx send failed: {}", e)),
        },
    };

    let tx_hash = format!("{:?}", pending_tx.tx_hash());
    info!("   🔗 [BG] Redeem tx sent: {}", tx_hash);

    let confirmed = match tokio::time::timeout(
        std::time::Duration::from_secs(120),
        pending_tx,
    ).await {
        Ok(Ok(Some(receipt))) => receipt.status == Some(U64::from(1)),
        Ok(Ok(None)) => true,
        Ok(Err(e)) => {
            return MergeResult {
                event_slug: event_slug.to_string(),
                usdc_recovered: 0.0, profit: 0.0,
                excess_position: None, was_merge: false,
                error: Some(format!("Redeem tx error: {}", e)),
            };
        }
        Err(_) => {
            warn!("   ⏳ [BG] Redeem tx timeout (120s): {}", tx_hash);
            true
        }
    };

    if !confirmed {
        return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: false,
            error: Some(format!("Redeem tx reverted: {}", tx_hash)),
        };
    }

    // Notify CLOB about updated balance
    if let Err(e) = executor.update_clob_balance_allowance(&[
        up_token_id.to_string(), down_token_id.to_string(),
    ]).await {
        warn!("   ⚠️  [BG] CLOB balance sync failed: {}", e);
    }

    info!("   ✅ [BG] Redeem confirmed: ${:.2} recovered, +${:.2} profit | Tx: {}",
          redemption_value, profit, tx_hash);

    MergeResult {
        event_slug: event_slug.to_string(),
        usdc_recovered: redemption_value,
        profit,
        excess_position: None,
        was_merge: false,
        error: None,
    }
}

/// Parse a raw WebSocket message and dispatch to the arb engine.
///
/// Message types from the market channel (per docs):
/// - "book": Full order book snapshot (bids/asks arrays)
/// - "price_change": Best bid/ask update when order placed/cancelled
/// - "market_resolved": Market resolution with winning_asset_id
/// - "last_trade_price": Trade execution
/// - "new_market": New market created
async fn parse_and_dispatch_ws_message(
    text: &str,
    event_tx: &mpsc::Sender<ArbWsEvent>,
) {
    // Skip PONG responses
    if text == "PONG" || text.is_empty() {
        return;
    }

    // The WS can send single objects or arrays (initial book snapshots come as array)
    if text.starts_with('[') {
        // Array of book snapshots (initial subscription response)
        if let Ok(msgs) = serde_json::from_str::<Vec<serde_json::Value>>(text) {
            for msg in &msgs {
                dispatch_single_ws_message(msg, event_tx).await;
            }
        }
    } else if text.starts_with('{') {
        // Single message
        if let Ok(msg) = serde_json::from_str::<serde_json::Value>(text) {
            dispatch_single_ws_message(&msg, event_tx).await;
        }
    }
}

/// Dispatch a single parsed WS message to the arb engine.
async fn dispatch_single_ws_message(
    msg: &serde_json::Value,
    event_tx: &mpsc::Sender<ArbWsEvent>,
) {
    let event_type = msg.get("event_type").and_then(|v| v.as_str()).unwrap_or("");

    match event_type {
        "book" => {
            // Full book snapshot — this is our primary data source
            // Structure: {"event_type":"book", "asset_id":"...",
            //   "bids":[{"price":"0.48","size":"30"}, ...],
            //   "asks":[{"price":"0.52","size":"25"}, ...],
            //   "timestamp":"...", "hash":"..."}
            let asset_id = match msg.get("asset_id").and_then(|v| v.as_str()) {
                Some(id) => id.to_string(),
                None => return,
            };

            let book = match parse_ws_book(msg) {
                Some(b) => b,
                None => return,
            };

            let _ = event_tx.send(ArbWsEvent::BookSnapshot { asset_id, book }).await;
        }

        "price_change" => {
            // Price change events contain best_bid/best_ask but NOT full depth.
            // We send a PriceUpdate event so the engine can update ONLY the top-of-book
            // without destroying the full depth from previous "book" snapshots.
            // This is critical for continuous accumulation — we need real depth data
            // from "book" events to sweep multiple levels accurately.

            if let Some(changes) = msg.get("price_changes").and_then(|v| v.as_array()) {
                for change in changes {
                    let asset_id = match change.get("asset_id").and_then(|v| v.as_str()) {
                        Some(id) => id.to_string(),
                        None => continue,
                    };

                    let best_bid = change.get("best_bid").and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                    let best_ask = change.get("best_ask").and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok()).unwrap_or(1.0);

                    trace!("📊 price_change: {}... bid={:.2} ask={:.2}",
                           &asset_id[..16.min(asset_id.len())], best_bid, best_ask);

                    if best_ask > 0.0 && best_ask < 1.0 {
                        let _ = event_tx.send(ArbWsEvent::PriceUpdate {
                            asset_id: asset_id.clone(),
                            best_bid,
                            best_ask,
                        }).await;
                    }
                }
            }
        }

        "market_resolved" => {
            // Market resolution event
            // Structure: {"event_type":"market_resolved", "market":"0x...",
            //   "winning_asset_id":"...", "winning_outcome":"Up/Down", ...}
            let market_condition = msg.get("market").and_then(|v| v.as_str())
                .unwrap_or("").to_string();
            let winning_asset_id = msg.get("winning_asset_id").and_then(|v| v.as_str())
                .unwrap_or("").to_string();
            let winning_outcome = msg.get("winning_outcome").and_then(|v| v.as_str())
                .unwrap_or("").to_string();

            if !winning_asset_id.is_empty() {
                let _ = event_tx.send(ArbWsEvent::MarketResolved {
                    market_condition_id: market_condition,
                    winning_asset_id,
                    winning_outcome,
                }).await;
            }
        }

        // "last_trade_price" and "new_market" are informational — not needed for arb
        _ => {}
    }
}

/// Parse a WebSocket "book" event into a TokenBook.
/// The format matches the REST /book response but comes as a WS message:
/// {"bids": [{"price": "0.48", "size": "30"}, ...], "asks": [...]}
fn parse_ws_book(msg: &serde_json::Value) -> Option<TokenBook> {
    let mut ask_levels = Vec::new();

    if let Some(asks) = msg.get("asks").and_then(|v| v.as_array()) {
        for ask in asks {
            // WS book uses object format: {"price": "0.52", "size": "25"}
            let price = ask.get("price").and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(1.0);
            let size = ask.get("size").and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            if size > 0.0 {
                ask_levels.push(BookLevel { price, size });
            }
        }
    }

    // Sort asks ascending (cheapest first)
    ask_levels.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));

    let total_ask_depth: f64 = ask_levels.iter().map(|l| l.size).sum();
    let best_ask = ask_levels.first().map(|l| l.price).unwrap_or(1.0);

    let best_bid = if let Some(bids) = msg.get("bids").and_then(|v| v.as_array()) {
        // Bids come in descending order (best first)
        bids.first()
            .and_then(|b| b.get("price").and_then(|v| v.as_str()))
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0)
    } else {
        0.0
    };

    // Only return a book if there's actual data
    if ask_levels.is_empty() && best_bid == 0.0 {
        return None;
    }

    Some(TokenBook { best_bid, best_ask, ask_levels, total_ask_depth })
}
