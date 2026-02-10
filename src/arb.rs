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
//! 4. Buy both sides simultaneously (FOK orders for guaranteed fills)
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
use tracing::{info, warn, error, debug};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum combined cost (Up + Down) we're willing to pay per pair.
/// $0.98 means we need at least 2% spread to enter.
const MAX_PAIR_COST: f64 = 0.98;


/// Minimum order size in tokens (Polymarket enforces 5 for these markets).
const MIN_ORDER_SIZE: f64 = 5.0;

/// How often to poll Gamma API for new markets (seconds).
const MARKET_SCAN_INTERVAL_SECS: u64 = 30;


/// Seconds before market start to begin scanning the book.
const PRE_MARKET_LEAD_SECS: i64 = 300; // 5 minutes before

/// Seconds after market start to stop trying to enter (prices converge).
const ENTRY_WINDOW_SECS: i64 = 1800; // first 30 minutes of the hour

/// CLOB WebSocket market channel URL (public, no auth needed).
/// Docs: https://docs.polymarket.com/developers/CLOB/websocket/market-channel
const CLOB_WS_MARKET_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// WebSocket keepalive interval (docs say send "PING" every 10s).
const WS_PING_INTERVAL_SECS: u64 = 10;

/// Channel buffer size for WS → engine communication.
const WS_CHANNEL_BUFFER: usize = 256;

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbAsset {
    BTC,
    ETH,
}

impl ArbAsset {
    fn label(&self) -> &'static str {
        match self { ArbAsset::BTC => "btc", ArbAsset::ETH => "eth" }
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
        info!("   Strategy: Buy both Up+Down when spread > {:.0}%", (1.0 - MAX_PAIR_COST) * 100.0);
        info!("   Targets: BTC hourly, ETH hourly (real-time WebSocket + 30s REST discovery)");

        // Market discovery timer — REST poll Gamma API for new events
        let mut scan_interval = tokio::time::interval(
            std::time::Duration::from_secs(MARKET_SCAN_INTERVAL_SECS)
        );
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
                        Some(ArbWsEvent::MarketResolved { market_condition_id, winning_asset_id, winning_outcome }) => {
                            info!("🏁 WS: Market resolved — winner: {} ({})", winning_outcome, &winning_asset_id[..20.min(winning_asset_id.len())]);
                            self.handle_ws_resolution(&market_condition_id, &winning_asset_id, &winning_outcome).await;
                        }
                        Some(ArbWsEvent::Connected(connected)) => {
                            self.ws_connected = connected;
                            if connected {
                                info!("🔌 WS: Connected to CLOB market channel");
                                // Re-subscribe to all tracked markets
                                if !self.ws_subscribed.is_empty() {
                                    let ids: Vec<String> = self.ws_subscribed.iter().cloned().collect();
                                    let _ = self.ws_cmd_tx.send(WsCommand::Subscribe(ids)).await;
                                }
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
    /// Checks if both sides of a market have cached books, then runs spread detection.
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

        // Skip if we already have a position in this market
        if self.has_position(&market.event_slug) {
            return;
        }

        // Re-entry guard: skip if execution is already in progress for this market
        if self.executing_slugs.contains(&market.event_slug) {
            return;
        }

        // Check if we're within the entry window
        let now = chrono::Utc::now().timestamp();
        if now < market.event_start_ts - PRE_MARKET_LEAD_SECS {
            return; // Too early
        }
        if now > market.event_start_ts + ENTRY_WINDOW_SECS {
            return; // Too late
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

        // Track best pair cost and spread
        let pair_cost = up_book.best_ask + down_book.best_ask;
        metrics::ARB_BEST_PAIR_COST.with_label_values(&[asset_lbl]).set(pair_cost);
        metrics::ARB_SPREAD_PCT.with_label_values(&[asset_lbl]).set((1.0 - pair_cost) * 100.0);

        // Run spread detection on cached books (no HTTP call needed!)
        match self.check_arb_from_books(&market, &up_book, &down_book) {
            Some(opp) => {
                let spread_pct = (1.0 - opp.avg_pair_cost) * 100.0;
                info!("⚡ WS ARB OPPORTUNITY: {} | {:.0} pairs | spread {:.2}% | profit ${:.2}",
                      market.title, opp.total_pairs, spread_pct, opp.expected_profit);

                metrics::ARB_OPPORTUNITIES_DETECTED.with_label_values(&[asset_lbl, "ws"]).inc();
                metrics::ARB_AVAILABLE_PAIRS.with_label_values(&[asset_lbl]).set(opp.total_pairs);

                // Set re-entry guard BEFORE execution
                self.executing_slugs.insert(market.event_slug.clone());
                let bg_task_spawned = self.execute_arb(&market, opp).await;
                // Only clear the guard if no background task was spawned.
                // If a merge task IS running, handle_merge_result will clear it.
                if !bg_task_spawned {
                    self.executing_slugs.remove(&market.event_slug);
                }
            }
            None => {
                metrics::ARB_AVAILABLE_PAIRS.with_label_values(&[asset_lbl]).set(0.0);
            }
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

                for market in markets {
                    // Check if within the broad monitoring window
                    if now > market.event_end_ts + 300 {
                        continue; // Already ended
                    }

                    let slug = market.event_slug.clone();
                    if !self.tracked_markets.contains_key(&slug) {
                        info!("📡 Tracking new market: {} (Up: {}..., Down: {}...)",
                              market.title,
                              &market.up_token_id[..16.min(market.up_token_id.len())],
                              &market.down_token_id[..16.min(market.down_token_id.len())]);

                        // Register in lookup maps
                        self.asset_to_market.insert(market.up_token_id.clone(), slug.clone());
                        self.asset_to_market.insert(market.down_token_id.clone(), slug.clone());

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
            if self.has_position(&market.event_slug) { continue; }
            if now < market.event_start_ts - PRE_MARKET_LEAD_SECS { continue; }
            if now > market.event_start_ts + ENTRY_WINDOW_SECS { continue; }

            // Skip if execution already in progress
            if self.executing_slugs.contains(&market.event_slug) { continue; }

            match self.check_arb_opportunity(&market).await {
                Ok(Some(opp)) => {
                    let spread_pct = (1.0 - opp.avg_pair_cost) * 100.0;
                    info!("💰 REST ARB OPPORTUNITY: {} | {:.0} pairs | spread {:.2}% | profit ${:.2}",
                          market.title, opp.total_pairs, spread_pct, opp.expected_profit);
                    metrics::ARB_OPPORTUNITIES_DETECTED.with_label_values(&[market.asset.label(), "rest"]).inc();

                    self.executing_slugs.insert(market.event_slug.clone());
                    let bg_task_spawned = self.execute_arb(&market, opp).await;
                    if !bg_task_spawned {
                        self.executing_slugs.remove(&market.event_slug);
                    }
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

        let best_pair_cost = up_book.best_ask + down_book.best_ask;
        if best_pair_cost >= MAX_PAIR_COST {
            return None;
        }

        // Use the same greedy sweep algorithm as check_arb_opportunity
        self.find_arb_pairs(market, up_book, down_book)
    }

    // -----------------------------------------------------------------------
    // Market scanning
    // -----------------------------------------------------------------------

    /// Scan Gamma API for active BTC/ETH Up/Down hourly events.
    ///
    /// Uses a 2-step approach:
    /// 1. GET /events (no closed filter) → discover event IDs by title match
    /// 2. GET /events/{id} for each match → get full market data with clobTokenIds
    ///
    /// We do NOT use closed=false because Polymarket marks hourly events as
    /// closed=true once they start, even though markets inside are still trading.
    /// We rely on per-market `acceptingOrders` and duration filtering instead.
    async fn scan_markets(&self) -> Result<Vec<ArbMarket>, String> {
        // Step 1: Discover matching event IDs from the newest events.
        // The list endpoint returns lightweight stubs (no clobTokenIds in markets).
        let url = "https://gamma-api.polymarket.com/events?order=id&ascending=false&limit=200";

        let resp = self.client.get(url).send().await
            .map_err(|e| format!("Events HTTP error: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("Events API returned {}", resp.status()));
        }

        let events: Vec<serde_json::Value> = resp.json().await
            .map_err(|e| format!("Events JSON parse error: {}", e))?;

        let now = chrono::Utc::now().timestamp();

        // Collect matching event IDs + asset type, filtering by title and endDate
        let mut candidates: Vec<(String, ArbAsset)> = Vec::new();

        for event in &events {
            let title = event.get("title").and_then(|v| v.as_str()).unwrap_or("");
            let title_lower = title.to_lowercase();

            let asset = if title_lower.contains("bitcoin up or down") {
                ArbAsset::BTC
            } else if title_lower.contains("ethereum up or down") {
                ArbAsset::ETH
            } else {
                continue;
            };

            // Quick endDate check on the stub to avoid fetching clearly-expired events
            let end_str = event.get("endDate").and_then(|v| v.as_str()).unwrap_or("");
            if !end_str.is_empty() {
                if let Ok(end_dt) = chrono::DateTime::parse_from_rfc3339(end_str) {
                    if end_dt.timestamp() < now - 300 {
                        continue;
                    }
                }
            }

            let id_str = if let Some(id) = event.get("id").and_then(|v| v.as_str()) {
                id.to_string()
            } else if let Some(id) = event.get("id").and_then(|v| v.as_u64()) {
                id.to_string()
            } else {
                continue;
            };

            candidates.push((id_str, asset));
        }

        if candidates.is_empty() {
            info!("📡 scan_markets: 0 BTC/ETH hourly events found in {} total events", events.len());
            return Ok(Vec::new());
        }

        info!("📡 scan_markets: {} candidate events from {} total, fetching full details...",
              candidates.len(), events.len());

        // Step 2: Fetch full event details concurrently.
        // Individual /events/{id} responses include nested markets[] with clobTokenIds.
        let futures: Vec<_> = candidates.iter().map(|(event_id, asset)| {
            let url = format!("https://gamma-api.polymarket.com/events/{}", event_id);
            let client = self.client.clone();
            let asset = *asset;
            async move {
                let resp = client.get(&url).send().await.ok()?;
                if !resp.status().is_success() { return None; }
                let event: serde_json::Value = resp.json().await.ok()?;
                Some((event, asset))
            }
        }).collect();

        let results = futures_util::future::join_all(futures).await;

        let mut markets = Vec::new();
        for (event, asset) in results.into_iter().flatten() {
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

                // Only target ~1-hour window markets (3600s ± 900s tolerance).
                // This excludes 15-minute windows (900s) and 4-hour windows (14400s).
                let duration = arb.event_end_ts - arb.event_start_ts;
                if duration < 2700 || duration > 4500 {
                    continue;
                }

                let accepting = market.get("acceptingOrders")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if !accepting {
                    continue;
                }

                markets.push(arb);
            }
        }

        info!("📡 scan_markets: {} tradeable 1hr markets found", markets.len());
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
        _market: &ArbMarket,
        up_book: &TokenBook,
        down_book: &TokenBook,
    ) -> Option<ArbOpportunity> {
        if up_book.ask_levels.is_empty() || down_book.ask_levels.is_empty() {
            return None;
        }

        let best_pair_cost = up_book.best_ask + down_book.best_ask;
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
        // Down price we can afford: max_down_price = MAX_PAIR_COST - up_price.
        // Then we sweep Down levels up to that price.
        let mut down_idx = 0;
        let mut down_remaining_at_level = if !down_book.ask_levels.is_empty() {
            down_book.ask_levels[0].size
        } else {
            0.0
        };

        for up_level in &up_book.ask_levels {
            let max_down_price = MAX_PAIR_COST - up_level.price;
            if max_down_price <= 0.0 {
                break; // Up price alone exceeds threshold
            }

            let mut up_remaining = up_level.size;

            // Match with Down levels
            while up_remaining >= 1.0 && down_idx < down_book.ask_levels.len() {
                let down_level = &down_book.ask_levels[down_idx];
                if down_level.price > max_down_price {
                    break; // This Down level is too expensive for this Up level
                }

                // How many pairs can we make at this combination?
                let pairs = up_remaining.min(down_remaining_at_level).floor();
                if pairs < 1.0 {
                    if down_remaining_at_level < 1.0 {
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

                let pair_cost = up_level.price + down_level.price;
                let cost = pairs * pair_cost;

                up_orders.push(BookLevel { price: up_level.price, size: pairs });
                down_orders.push(BookLevel { price: down_level.price, size: pairs });
                total_up_tokens += pairs;
                total_down_tokens += pairs;
                total_cost += cost;

                up_remaining -= pairs;
                down_remaining_at_level -= pairs;

                if down_remaining_at_level < 1.0 {
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
    /// Returns `true` if a background merge task was spawned (caller should NOT
    /// clear executing_slugs — handle_merge_result will do it).
    /// Returns `false` if no background task was spawned (caller should clear the guard).
    async fn execute_arb(
        &mut self,
        market: &ArbMarket,
        opp: ArbOpportunity,
    ) -> bool {
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
                return false;
            }
            info!("   Scaling to {:.0}% of opportunity (capital limited)", scale * 100.0);
            // We'll cap individual order sizes below
        }

        let spread_pct = (1.0 - opp.avg_pair_cost) * 100.0;
        info!("🚀 EXECUTING ARB: {} | {:.0} pairs across {} levels | spread {:.2}%",
              market.title, opp.total_pairs, opp.up_orders.len() + opp.down_orders.len(), spread_pct);

        // Register tokens in the registry
        let up_hash = hash_asset_id(&market.up_token_id);
        let down_hash = hash_asset_id(&market.down_token_id);
        self.token_registry.register(up_hash, market.up_token_id.clone(), market.neg_risk);
        self.token_registry.register(down_hash, market.down_token_id.clone(), market.neg_risk);

        // Fetch and register full market info for both tokens
        for (token_id, hash) in [
            (&market.up_token_id, up_hash),
            (&market.down_token_id, down_hash),
        ] {
            if let Err(e) = self.executor.fetch_and_register_market_info(token_id, hash).await {
                warn!("Failed to register market info for {}...: {}", &token_id[..20.min(token_id.len())], e);
            }
        }

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
            let scaled_size = (level.size * capital_scale).floor().max(0.0);
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
            let scaled_size = (level.size * capital_scale).floor().max(0.0);
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
            return false;
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
                sig.order_type, TimeInForce::FOK, 0,
                up_token_str.clone(), up_neg_risk, up_fee,
            )
        }).collect();

        // Sign all Down orders
        let down_sign_futures: Vec<_> = down_signals.iter().map(|sig| {
            self.executor.signer().prepare_order_full(
                sig.market_id, sig.side, sig.price, sig.size,
                sig.order_type, TimeInForce::FOK, 0,
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

        // Collect successfully signed orders, interleaving Up and Down
        // so both sides fill simultaneously in each batch
        struct TaggedOrder {
            order: PreparedOrder,
            side_label: &'static str,
            seq: usize,
        }

        let mut interleaved: Vec<TaggedOrder> = Vec::with_capacity(total_signals);
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
            return false;
        }

        // Interleave: Up[0], Down[0], Up[1], Down[1], ...
        let max_len = up_ok.len().max(down_ok.len());
        let mut up_seq = 0usize;
        let mut down_seq = 0usize;
        for i in 0..max_len {
            if i < up_ok.len() {
                interleaved.push(TaggedOrder {
                    order: up_ok[i].clone(),
                    side_label: "Up",
                    seq: up_seq,
                });
                up_seq += 1;
            }
            if i < down_ok.len() {
                interleaved.push(TaggedOrder {
                    order: down_ok[i].clone(),
                    side_label: "Down",
                    seq: down_seq,
                });
                down_seq += 1;
            }
        }

        // ── STEP 2: Submit in batches of 15 via POST /orders ─────────────
        // Polymarket batch API accepts up to 15 orders per request.
        // This is dramatically faster than individual POST /order calls.
        const BATCH_LIMIT: usize = 15;

        let mut up_orders_ok = 0u32;
        let mut down_orders_ok = 0u32;
        let mut up_tokens_total = 0.0_f64;
        let mut down_tokens_total = 0.0_f64;
        let mut up_cost_total = 0.0_f64;
        let mut down_cost_total = 0.0_f64;

        let submit_start = std::time::Instant::now();

        for (batch_idx, chunk) in interleaved.chunks(BATCH_LIMIT).enumerate() {
            if batch_idx > 0 {
                // Brief pause between batches to avoid Cloudflare 403
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }

            let batch_orders: Vec<PreparedOrder> = chunk.iter()
                .map(|t| t.order.clone())
                .collect();

            let results = self.executor.submit_orders_batch(&batch_orders).await;

            for (k, result) in results.into_iter().enumerate() {
                if k >= chunk.len() { break; }
                let tagged = &chunk[k];
                let requested_size = tagged.order.size.to_f64().unwrap_or(0.0);
                let requested_price = tagged.order.price.to_f64().unwrap_or(0.0);

                match result {
                    Ok(fill) => {
                        // CRITICAL: Use actual fill amounts from API, not requested size.
                        // FAK orders can partially fill — takingAmount = actual tokens received.
                        let actual_tokens = fill.tokens_filled();
                        let actual_cost = fill.usdc_amount();

                        // Only fall back to requested_size for DELAYED status where amounts
                        // aren't yet known. For UNMATCHED (no fill) or MATCHED (fill known),
                        // always use the actual amount — even if it's 0.
                        let is_delayed = fill.status == "DELAYED";
                        let tokens = if actual_tokens > 0.0 {
                            actual_tokens
                        } else if is_delayed {
                            warn!("   ⏳ {} #{}: DELAYED — using requested size {:.0} as estimate",
                                  tagged.side_label, tagged.seq + 1, requested_size);
                            requested_size
                        } else {
                            0.0 // UNMATCHED or zero-fill — count as 0
                        };
                        let cost = if actual_cost > 0.0 {
                            actual_cost
                        } else if is_delayed {
                            requested_size * requested_price
                        } else {
                            0.0
                        };

                        if tagged.side_label == "Up" {
                            up_orders_ok += 1;
                            up_tokens_total += tokens;
                            up_cost_total += cost;
                        } else {
                            down_orders_ok += 1;
                            down_tokens_total += tokens;
                            down_cost_total += cost;
                        }

                        if actual_tokens > 0.0 && actual_tokens < requested_size * 0.99 {
                            info!("   ✅ {} #{}: {:.1}/{:.0} tokens @ {:.3} (partial fill) → {} [{}]",
                                  tagged.side_label, tagged.seq + 1, actual_tokens, requested_size,
                                  requested_price, fill.order_id, fill.status);
                        } else {
                            info!("   ✅ {} #{}: {:.0} tokens @ {:.3} → {} [{}]",
                                  tagged.side_label, tagged.seq + 1, tokens, requested_price,
                                  fill.order_id, fill.status);
                        }
                    }
                    Err(ref e) => {
                        warn!("   ❌ {} #{} failed: {}", tagged.side_label, tagged.seq + 1, e);
                    }
                }
            }
        }

        let submit_elapsed = submit_start.elapsed();
        info!("   Submitted {} orders in {:.0}ms ({} batches)",
              total_signals, submit_elapsed.as_secs_f64() * 1000.0,
              (total_signals + BATCH_LIMIT - 1) / BATCH_LIMIT);
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

        if up_orders_ok > 0 && down_orders_ok > 0 {
            info!("   🎯 ARB FILLED: ${:.2} deployed, {:.0} pairs, expected profit ${:.2}",
                  total_cost, min_tokens, expected_profit);
            metrics::ARB_OPPORTUNITIES_EXECUTED.with_label_values(&[asset_lbl]).inc();

            self.capital_usdc -= total_cost;

            // Track position IMMEDIATELY (before merge) so the event loop isn't blocked.
            // The merge runs as a background task and reports back via merge_tx channel.
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

            // SPAWN BACKGROUND MERGE: don't block the event loop!
            // The merge (5s settle + balance check + tx + 120s confirmation) runs independently.
            // Results are sent back via merge_tx channel → processed in handle_merge_result.
            let api_pairs = min_tokens.floor();
            if api_pairs >= 1.0 {
                let merge_tx = self.merge_tx.clone();
                let config = self.config.clone();
                let event_slug = market.event_slug.clone();
                let condition_id = market.condition_id.clone();
                let up_token_id = market.up_token_id.clone();
                let down_token_id = market.down_token_id.clone();
                let executor = self.executor.clone();

                tokio::spawn(async move {
                    info!("   🔄 [BG] Waiting 5s for CLOB settlement before merge...");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

                    // Run the merge in the background
                    let result = run_background_merge(
                        &config, &condition_id, &up_token_id, &down_token_id,
                        api_pairs, up_tokens_total, down_tokens_total,
                        up_cost_total, down_cost_total, total_cost,
                        &event_slug, &executor,
                    ).await;
                    let _ = merge_tx.send(result).await;
                });

                info!("   📤 Merge spawned as background task — event loop free");
                return true; // Background task spawned — don't clear executing_slugs
            }
        } else if up_orders_ok > 0 || down_orders_ok > 0 {
            warn!("   ⚠️  PARTIAL ENTRY: Up={} Down={} — one-sided risk!",
                  up_orders_ok, down_orders_ok);

            // Still track it so we can redeem whatever we got
            self.capital_usdc -= total_cost;
            self.positions.push(CyclePosition {
                market: market.clone(),
                up_tokens_bought: up_tokens_total,
                down_tokens_bought: down_tokens_total,
                up_cost_usdc: up_cost_total,
                down_cost_usdc: down_cost_total,
                total_cost,
                expected_profit: 0.0, // Unknown — one-sided
                entered_at: chrono::Utc::now(),
                redeemed: false,
            });
        } else {
            error!("   ❌ ALL ORDERS FAILED — no position entered");
        }

        false // No background task spawned
    }

    // -----------------------------------------------------------------------
    // Redemption
    // -----------------------------------------------------------------------

    /// Check all tracked positions and recover capital.
    ///
    /// Strategy:
    /// 1. For positions with both sides: try merge immediately (no resolution needed)
    /// 2. For positions after market close: try redeem (post-resolution fallback)
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

            // Strategy 1: Spawn background merge (works anytime, both sides needed)
            if min_tokens >= 1.0 {
                let api_pairs = min_tokens.floor();
                info!("🔄 Spawning background merge for {}: {:.0} pairs", market.title, api_pairs);

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

        let tx = TransactionRequest::new()
            .to(ctf_addr)
            .data(calldata.clone())
            .from(client.address());

        let gas_estimate = client.estimate_gas(&tx.clone().into(), None).await
            .map_err(|e| format!("Merge gas estimation failed: {}", e))?;

        let gas_limit = gas_estimate * 120 / 100;
        let tx_with_gas = tx.gas(gas_limit);

        let pending_tx = client.send_transaction(tx_with_gas, None).await
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

        let tx = TransactionRequest::new()
            .to(ctf_addr)
            .data(calldata.clone())
            .from(client.address());

        let gas_estimate = client.estimate_gas(&tx.clone().into(), None).await
            .map_err(|e| format!("Redeem gas estimation failed: {}", e))?;

        let gas_limit = gas_estimate * 120 / 100;
        let tx_with_gas = tx.gas(gas_limit);

        let pending_tx = client.send_transaction(tx_with_gas, None).await
            .map_err(|e| format!("Redeem tx send failed: {}", e))?;

        let tx_hash = format!("{:?}", pending_tx.tx_hash());
        info!("   🔗 Redeem tx sent: {} (waiting for confirmation...)", tx_hash);

        Self::wait_for_tx(pending_tx, tx_hash, "Redeem").await
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

                // Send initial subscription if we have pending asset IDs
                if !pending_subs.is_empty() {
                    let sub_msg = serde_json::json!({
                        "assets_ids": pending_subs,
                        "type": "market"
                    });
                    if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
                        warn!("📡 WS initial subscribe failed: {}", e);
                    } else {
                        info!("📡 WS subscribed to {} assets on connect", pending_subs.len());
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
                                    let sub_msg = serde_json::json!({
                                        "assets_ids": ids,
                                        "operation": "subscribe"
                                    });
                                    if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
                                        warn!("📡 WS subscribe failed: {}", e);
                                        break;
                                    }
                                    debug!("📡 WS subscribed to {} new assets", ids.len());
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

    let tx = TransactionRequest::new()
        .to(ctf_addr)
        .data(calldata)
        .from(client.address());

    let gas_estimate = match client.estimate_gas(&tx.clone().into(), None).await {
        Ok(g) => g,
        Err(e) => return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: true,
            error: Some(format!("Gas estimation failed: {}", e)),
        },
    };

    let tx_with_gas = tx.gas(gas_estimate * 120 / 100);

    let pending_tx = match client.send_transaction(tx_with_gas, None).await {
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
        let excess_cost = if up_tokens_total > down_tokens_total {
            up_cost_total * (excess_up / up_tokens_total)
        } else {
            down_cost_total * (excess_down / down_tokens_total)
        };
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
            },
            up_tokens_bought: excess_up,
            down_tokens_bought: excess_down,
            up_cost_usdc: if excess_up > 0.0 { up_cost_total * (excess_up / up_tokens_total) } else { 0.0 },
            down_cost_usdc: if excess_down > 0.0 { down_cost_total * (excess_down / down_tokens_total) } else { 0.0 },
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

    let tx = TransactionRequest::new()
        .to(ctf_addr)
        .data(calldata)
        .from(client.address());

    let gas_estimate = match client.estimate_gas(&tx.clone().into(), None).await {
        Ok(g) => g,
        Err(e) => return MergeResult {
            event_slug: event_slug.to_string(),
            usdc_recovered: 0.0, profit: 0.0,
            excess_position: None, was_merge: false,
            error: Some(format!("Redeem gas estimation failed: {}", e)),
        },
    };

    let tx_with_gas = tx.gas(gas_estimate * 120 / 100);

    let pending_tx = match client.send_transaction(tx_with_gas, None).await {
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
            // We construct a minimal TokenBook with a single ask level so the engine's
            // book cache stays up-to-date. When a real arb opportunity is detected,
            // the execution path uses FOK orders that will fill or cancel, so stale
            // depth is safe — we just need accurate best_ask for spread detection.

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

                    debug!("📊 price_change: {}... bid={:.2} ask={:.2}",
                           &asset_id[..16.min(asset_id.len())], best_bid, best_ask);

                    // Build a minimal TokenBook from price_change so the engine cache
                    // stays current. Use a nominal depth of 100 tokens — the real depth
                    // doesn't matter for spread detection, and FOK orders handle the rest.
                    if best_ask > 0.0 && best_ask < 1.0 {
                        let book = TokenBook {
                            best_bid,
                            best_ask,
                            ask_levels: vec![BookLevel { price: best_ask, size: 100.0 }],
                            total_ask_depth: 100.0,
                        };
                        let _ = event_tx.send(ArbWsEvent::BookSnapshot {
                            asset_id: asset_id.clone(),
                            book,
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
