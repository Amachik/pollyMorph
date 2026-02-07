//! WebSocket connectivity module for Polymarket CLOB and exchange feeds
//! Optimized for low-latency message processing with zero-copy parsing

use crate::config::Config;
use crate::types::{OrderBookSnapshot, PriceLevel, PolymarketMessage, BinanceTickerMessage, MAX_BOOK_DEPTH};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

/// Hash an asset_id string into a u64 for fast lookup.
/// Uses the last 8 bytes of the string as a simple hash.
pub fn hash_asset_id(asset_id: &str) -> u64 {
    let bytes = asset_id.as_bytes();
    let mut hash: u64 = 0xcbf29ce484222325; // FNV offset basis
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3); // FNV prime
    }
    hash
}

/// Response from Polymarket Gamma API for markets
#[derive(Debug, Deserialize)]
struct GammaMarket {
    #[serde(default)]
    active: bool,
    #[serde(default)]
    closed: bool,
    #[serde(rename = "clobTokenIds", default)]
    clob_token_ids: Option<String>,
    #[serde(default)]
    question: Option<String>,
    #[serde(rename = "volumeNum", default)]
    volume: Option<f64>,
}

/// Book snapshot from Polymarket WebSocket (array format)
#[derive(Debug, Deserialize)]
struct PolymarketBookSnapshot {
    #[serde(default)]
    market: Option<String>,
    #[serde(default)]
    asset_id: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default)]
    bids: Option<Vec<[String; 2]>>,
    #[serde(default)]
    asks: Option<Vec<[String; 2]>>,
    #[serde(default)]
    hash: Option<String>,
}

/// Fetch active markets from Polymarket Gamma API
pub async fn fetch_active_markets() -> anyhow::Result<Vec<String>> {
    let url = "https://gamma-api.polymarket.com/markets?closed=false&active=true&limit=20";
    info!("📡 Fetching active markets from Polymarket API...");
    
    let response = reqwest::get(url).await?;
    let markets: Vec<GammaMarket> = response.json().await?;
    
    let mut token_ids: Vec<String> = Vec::new();
    let market_count = markets.len();
    
    for market in markets {
        if market.active && !market.closed {
            if let Some(clob_ids) = market.clob_token_ids {
                // clobTokenIds is a JSON array string like "[\"id1\", \"id2\"]"
                if let Ok(ids) = serde_json::from_str::<Vec<String>>(&clob_ids) {
                    for id in ids {
                        if !id.is_empty() {
                            token_ids.push(id);
                        }
                    }
                }
            }
        }
    }
    
    info!("📊 Found {} active token IDs from {} markets", token_ids.len(), market_count);
    
    // Limit to top 10 for now to avoid overwhelming the WebSocket
    token_ids.truncate(10);
    
    if token_ids.is_empty() {
        warn!("⚠️  No active markets found, using fallback token IDs");
        // Fallback to some known active markets (these may need updating)
        token_ids = vec![
            "21742633143463906290569050155826241533067272736897614950488156847949938836455".to_string(),
        ];
    }
    
    Ok(token_ids)
}

/// WebSocket message for internal routing
#[derive(Debug)]
pub enum WsEvent {
    /// Order book update from Polymarket
    BookUpdate {
        market_id: String,
        snapshot: OrderBookSnapshot,
    },
    /// Trade executed on Polymarket
    Trade {
        market_id: String,
        price: Decimal,
        size: Decimal,
        side: crate::types::Side,
        timestamp_ns: u64,
    },
    /// Price update from external exchange
    ExchangePrice {
        exchange: Exchange,
        symbol: String,
        bid: Decimal,
        ask: Decimal,
        timestamp_ns: u64,
    },
    /// Connection status change
    ConnectionStatus {
        source: ConnectionSource,
        connected: bool,
    },
    /// Error event
    Error {
        source: ConnectionSource,
        message: String,
    },
    /// Order update from Polymarket user channel (fills, placements, cancellations)
    OrderUpdate {
        order_id: String,
        asset_id: String,
        status: String,
        side: String,
        price: String,
        original_size: String,
        size_matched: String,
        timestamp_ns: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exchange {
    Binance,
    Coinbase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionSource {
    Polymarket,
    Binance,
    Coinbase,
}

/// Polymarket CLOB WebSocket handler
pub struct PolymarketWs {
    config: Arc<Config>,
    event_tx: mpsc::Sender<WsEvent>,
    subscribed_markets: Vec<String>,
}

impl PolymarketWs {
    pub fn new(config: Arc<Config>, event_tx: mpsc::Sender<WsEvent>) -> Self {
        Self {
            config,
            event_tx,
            subscribed_markets: Vec::new(),
        }
    }
    
    /// Subscribe to market updates
    pub fn subscribe(&mut self, market_id: String) {
        self.subscribed_markets.push(market_id);
    }
    
    /// Start the WebSocket connection and message processing loop
    pub async fn run(self) -> anyhow::Result<()> {
        let url = &self.config.polymarket.ws_url;
        info!("Connecting to Polymarket CLOB at {}", url);
        
        loop {
            match self.connect_and_process().await {
                Ok(_) => {
                    warn!("Polymarket WebSocket disconnected cleanly");
                }
                Err(e) => {
                    error!("Polymarket WebSocket error: {}", e);
                    let _ = self.event_tx.send(WsEvent::Error {
                        source: ConnectionSource::Polymarket,
                        message: e.to_string(),
                    }).await;
                }
            }
            
            let _ = self.event_tx.send(WsEvent::ConnectionStatus {
                source: ConnectionSource::Polymarket,
                connected: false,
            }).await;
            
            // Exponential backoff would be better, keeping simple for now
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            info!("Reconnecting to Polymarket...");
        }
    }
    
    async fn connect_and_process(&self) -> anyhow::Result<()> {
        let (ws_stream, _) = connect_async(&self.config.polymarket.ws_url).await?;
        let (mut write, mut read) = ws_stream.split();
        
        let _ = self.event_tx.send(WsEvent::ConnectionStatus {
            source: ConnectionSource::Polymarket,
            connected: true,
        }).await;
        
        // Subscribe to markets using Polymarket's format
        // "type": "book" gives full order book snapshots (bids/asks) — required for signal generation
        // "type": "market" only gives price_change events which don't contain book depth
        if !self.subscribed_markets.is_empty() {
            let subscribe_msg = serde_json::json!({
                "assets_ids": self.subscribed_markets,
                "type": "book"
            });
            write.send(Message::Text(subscribe_msg.to_string())).await?;
            info!("Subscribed to {} markets (book depth)", self.subscribed_markets.len());
        }
        
        // Pre-allocate buffer for SIMD JSON parsing
        let mut json_buffer = vec![0u8; 65536];
        
        // Ping interval for keepalive
        let mut ping_interval = tokio::time::interval(self.config.ws_ping_interval());
        
        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            debug!("📩 Raw WS message: {}", &text[..text.len().min(200)]);
                            self.process_message(&text, &mut json_buffer).await;
                        }
                        Some(Ok(Message::Binary(data))) => {
                            if let Ok(text) = String::from_utf8(data) {
                                self.process_message(&text, &mut json_buffer).await;
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            write.send(Message::Pong(data)).await?;
                        }
                        Some(Ok(Message::Close(_))) => {
                            info!("Polymarket WebSocket closed by server");
                            break;
                        }
                        Some(Err(e)) => {
                            return Err(e.into());
                        }
                        None => break,
                        _ => {}
                    }
                }
                _ = ping_interval.tick() => {
                    write.send(Message::Ping(vec![])).await?;
                }
            }
        }
        
        Ok(())
    }
    
    /// Process incoming WebSocket message with SIMD JSON parsing
    #[inline]
    async fn process_message(&self, text: &str, buffer: &mut Vec<u8>) {
        let ws_start = std::time::Instant::now();
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        
        // Handle array format (initial book snapshots come as array)
        if text.starts_with('[') {
            buffer.clear();
            buffer.extend_from_slice(text.as_bytes());
            if let Ok(msgs) = simd_json::from_slice::<Vec<PolymarketBookSnapshot>>(buffer) {
                for msg in msgs {
                    self.process_book_snapshot(&msg, timestamp_ns).await;
                }
            }
            crate::metrics::record_ws_latency("polymarket", ws_start.elapsed().as_secs_f64());
            return;
        }
        
        // Copy to mutable buffer for simd-json (requires mutable input)
        buffer.clear();
        buffer.extend_from_slice(text.as_bytes());
        
        // Use simd-json for fast parsing with flexible PolymarketMessage
        match simd_json::from_slice::<PolymarketMessage>(buffer) {
            Ok(msg) => {
                // Determine message type from event_type or msg_type field
                let event_type = msg.event_type.or(msg.msg_type).unwrap_or("");
                let has_price_changes = msg.price_changes.is_some();
                debug!("📦 Parsed msg: event_type='{}', has_price_changes={}", event_type, has_price_changes);
                
                match event_type {
                    "book" => {
                        // Book update with bids/asks
                        if let (Some(bids), Some(asks)) = (&msg.bids, &msg.asks) {
                            let market_id = msg.market.or(msg.asset_id).unwrap_or("unknown");
                            let snapshot = self.parse_polymarket_book(bids, asks, timestamp_ns);
                            let mut snapshot = snapshot;
                            snapshot.token_id = hash_asset_id(market_id);
                            let _ = self.event_tx.send(WsEvent::BookUpdate {
                                market_id: market_id.to_string(),
                                snapshot,
                            }).await;
                            debug!("📖 Book update: {} bids, {} asks", bids.len(), asks.len());
                        }
                    }
                    "price_change" => {
                        // Price change with price_changes array
                        if let Some(price_changes) = &msg.price_changes {
                            let market_id = msg.market.unwrap_or("unknown");
                            for pc in price_changes {
                                if let Ok(price) = Decimal::from_str(&pc.price) {
                                    let size = pc.size.as_ref()
                                        .and_then(|s| Decimal::from_str(s).ok())
                                        .unwrap_or(Decimal::ONE);
                                    let side = match pc.side.as_deref() {
                                        Some("buy") | Some("BUY") => crate::types::Side::Buy,
                                        _ => crate::types::Side::Sell,
                                    };
                                    
                                    info!("📈 Price: {} @ {} ({})", &pc.asset_id[..pc.asset_id.len().min(16)], price, market_id);
                                    
                                    let _ = self.event_tx.send(WsEvent::Trade {
                                        market_id: pc.asset_id.clone(),
                                        price,
                                        size,
                                        side,
                                        timestamp_ns,
                                    }).await;
                                }
                            }
                        }
                    }
                    "trade" | "last_trade_price" => {
                        // Trade update
                        if let (Some(price_str), Some(size_str)) = (msg.price, msg.size) {
                            if let (Ok(price), Ok(size)) = (
                                Decimal::from_str(price_str),
                                Decimal::from_str(size_str),
                            ) {
                                let side = match msg.side {
                                    Some("buy") | Some("BUY") => crate::types::Side::Buy,
                                    _ => crate::types::Side::Sell,
                                };
                                let market_id = msg.market.or(msg.asset_id).unwrap_or("unknown");
                                
                                info!("🔄 VIRTUAL FILL: {} @ {} ({:?})", size, price, side);
                                
                                let _ = self.event_tx.send(WsEvent::Trade {
                                    market_id: market_id.to_string(),
                                    price,
                                    size,
                                    side,
                                    timestamp_ns,
                                }).await;
                            }
                        }
                    }
                    "" => {
                        // Empty event_type - check for price_changes array (common format)
                        if let Some(price_changes) = &msg.price_changes {
                            let market_id = msg.market.unwrap_or("unknown");
                            for pc in price_changes {
                                if let Ok(price) = Decimal::from_str(&pc.price) {
                                    // Emit as a trade event for the pricing engine
                                    let size = pc.size.as_ref()
                                        .and_then(|s| Decimal::from_str(s).ok())
                                        .unwrap_or(Decimal::ONE);
                                    let side = match pc.side.as_deref() {
                                        Some("buy") | Some("BUY") => crate::types::Side::Buy,
                                        _ => crate::types::Side::Sell,
                                    };
                                    
                                    info!("📈 Price: {} @ {} ({})", &pc.asset_id[..pc.asset_id.len().min(16)], price, market_id);
                                    
                                    let _ = self.event_tx.send(WsEvent::Trade {
                                        market_id: pc.asset_id.clone(),
                                        price,
                                        size,
                                        side,
                                        timestamp_ns,
                                    }).await;
                                }
                            }
                        } else if let Some(price_str) = msg.price {
                            if let Ok(price) = Decimal::from_str(price_str) {
                                let market_id = msg.market.or(msg.asset_id).unwrap_or("unknown");
                                debug!("💰 Price update: {} = {}", market_id, price);
                            }
                        }
                    }
                    other => {
                        debug!("📨 Unknown event type: {}", other);
                    }
                }
            }
            Err(e) => {
                // Log parse failure with first 100 chars of message
                let preview = &text[..text.len().min(100)];
                warn!("❌ Failed to parse CLOB message: {} - preview: {}", e, preview);
            }
        }
        crate::metrics::record_ws_latency("polymarket", ws_start.elapsed().as_secs_f64());
    }
    
    /// Process book snapshot from array format
    async fn process_book_snapshot(&self, snapshot: &PolymarketBookSnapshot, timestamp_ns: u64) {
        let market_id = snapshot.asset_id.as_deref()
            .or(snapshot.market.as_deref())
            .unwrap_or("unknown");
        
        // If we have bids/asks, emit as book update
        if let (Some(bids), Some(asks)) = (&snapshot.bids, &snapshot.asks) {
            let mut book = OrderBookSnapshot::new();
            book.timestamp_ns = timestamp_ns;
            
            for (i, level) in bids.iter().take(MAX_BOOK_DEPTH).enumerate() {
                if let (Ok(price), Ok(size)) = (
                    Decimal::from_str(&level[0]),
                    Decimal::from_str(&level[1]),
                ) {
                    book.bids[i] = PriceLevel::new(price, size, 1);
                    book.bid_count = (i + 1) as u8;
                }
            }
            
            for (i, level) in asks.iter().take(MAX_BOOK_DEPTH).enumerate() {
                if let (Ok(price), Ok(size)) = (
                    Decimal::from_str(&level[0]),
                    Decimal::from_str(&level[1]),
                ) {
                    book.asks[i] = PriceLevel::new(price, size, 1);
                    book.ask_count = (i + 1) as u8;
                }
            }
            
            book.token_id = hash_asset_id(market_id);
            
            info!("📖 Book snapshot: {} ({} bids, {} asks)", 
                &market_id[..market_id.len().min(16)], book.bid_count, book.ask_count);
            
            let _ = self.event_tx.send(WsEvent::BookUpdate {
                market_id: market_id.to_string(),
                snapshot: book,
            }).await;
        }
    }
    
    /// Parse order book update into stack-allocated snapshot
    #[inline]
    fn parse_book_update(&self, update: &crate::types::BookUpdate, timestamp_ns: u64) -> OrderBookSnapshot {
        let mut snapshot = OrderBookSnapshot::new();
        snapshot.timestamp_ns = timestamp_ns;
        
        // Parse bids (highest first)
        for (i, level) in update.bids.iter().take(MAX_BOOK_DEPTH).enumerate() {
            if let (Ok(price), Ok(size)) = (
                Decimal::from_str(level[0]),
                Decimal::from_str(level[1]),
            ) {
                snapshot.bids[i] = PriceLevel::new(price, size, 1);
                snapshot.bid_count = (i + 1) as u8;
            }
        }
        
        // Parse asks (lowest first)
        for (i, level) in update.asks.iter().take(MAX_BOOK_DEPTH).enumerate() {
            if let (Ok(price), Ok(size)) = (
                Decimal::from_str(level[0]),
                Decimal::from_str(level[1]),
            ) {
                snapshot.asks[i] = PriceLevel::new(price, size, 1);
                snapshot.ask_count = (i + 1) as u8;
            }
        }
        
        snapshot
    }
    
    /// Parse Polymarket book from bids/asks arrays
    #[inline]
    fn parse_polymarket_book(&self, bids: &[[ &str; 2]], asks: &[[&str; 2]], timestamp_ns: u64) -> OrderBookSnapshot {
        let mut snapshot = OrderBookSnapshot::new();
        snapshot.timestamp_ns = timestamp_ns;
        
        // Parse bids
        for (i, level) in bids.iter().take(MAX_BOOK_DEPTH).enumerate() {
            if let (Ok(price), Ok(size)) = (
                Decimal::from_str(level[0]),
                Decimal::from_str(level[1]),
            ) {
                snapshot.bids[i] = PriceLevel::new(price, size, 1);
                snapshot.bid_count = (i + 1) as u8;
            }
        }
        
        // Parse asks
        for (i, level) in asks.iter().take(MAX_BOOK_DEPTH).enumerate() {
            if let (Ok(price), Ok(size)) = (
                Decimal::from_str(level[0]),
                Decimal::from_str(level[1]),
            ) {
                snapshot.asks[i] = PriceLevel::new(price, size, 1);
                snapshot.ask_count = (i + 1) as u8;
            }
        }
        
        snapshot
    }
}

// =============================================================================
// POLYMARKET USER CHANNEL WebSocket (authenticated, order/trade updates)
// =============================================================================

/// Deserialization struct for user channel messages (order + trade events)
#[derive(Debug, Deserialize)]
struct UserChannelMessage<'a> {
    #[serde(default)]
    event_type: Option<&'a str>,
    #[serde(default, rename = "type")]
    msg_type: Option<&'a str>,
    #[serde(default)]
    id: Option<&'a str>,
    #[serde(default)]
    asset_id: Option<&'a str>,
    #[serde(default)]
    market: Option<&'a str>,
    #[serde(default)]
    price: Option<&'a str>,
    #[serde(default)]
    side: Option<&'a str>,
    #[serde(default)]
    original_size: Option<&'a str>,
    #[serde(default)]
    size_matched: Option<&'a str>,
    #[serde(default)]
    size: Option<&'a str>,
    #[serde(default)]
    status: Option<&'a str>,
    #[serde(default)]
    outcome: Option<&'a str>,
}

/// Polymarket User Channel WebSocket handler.
/// Connects to `wss://.../ws/user` with L2 API key auth.
/// Receives order placements, updates, cancellations, and trade fills.
pub struct PolymarketUserWs {
    config: Arc<Config>,
    event_tx: mpsc::Sender<WsEvent>,
}

impl PolymarketUserWs {
    pub fn new(config: Arc<Config>, event_tx: mpsc::Sender<WsEvent>) -> Self {
        Self { config, event_tx }
    }

    /// Start the authenticated user channel WebSocket connection
    pub async fn run(self) -> anyhow::Result<()> {
        // New Polymarket real-time-data WebSocket endpoint (replaces old /ws/user)
        let url = "wss://ws-live-data.polymarket.com".to_string();
        info!("Connecting to Polymarket User Channel at {}", url);

        loop {
            match self.connect_and_process(&url).await {
                Ok(_) => warn!("Polymarket User WS disconnected cleanly"),
                Err(e) => {
                    error!("Polymarket User WS error: {}", e);
                    let _ = self.event_tx.send(WsEvent::Error {
                        source: ConnectionSource::Polymarket,
                        message: format!("User channel: {}", e),
                    }).await;
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            info!("Reconnecting to Polymarket User Channel...");
        }
    }

    async fn connect_and_process(&self, url: &str) -> anyhow::Result<()> {
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        info!("Polymarket User Channel connected, sending auth subscription");

        // Subscribe with L2 API key authentication
        // Format per Polymarket real-time-data-client SDK:
        // {"subscriptions": [{"topic": "clob_user", "type": "*", "clob_auth": {"key": ..., "secret": ..., "passphrase": ...}}]}
        let subscribe_msg = serde_json::json!({
            "subscriptions": [{
                "topic": "clob_user",
                "type": "*",
                "clob_auth": {
                    "key": self.config.polymarket.api_key,
                    "secret": self.config.polymarket.api_secret,
                    "passphrase": self.config.polymarket.api_passphrase,
                }
            }]
        });
        write.send(Message::Text(subscribe_msg.to_string())).await?;
        debug!("Sent user channel subscription with auth");

        let mut json_buffer = vec![0u8; 16384];
        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(10));

        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            // Log first messages at INFO level so we can debug the new API format
                            info!("User WS msg: {}", &text[..text.len().min(500)]);
                            self.process_user_message(&text, &mut json_buffer).await;
                        }
                        Some(Ok(Message::Binary(data))) => {
                            if let Ok(text) = String::from_utf8(data) {
                                self.process_user_message(&text, &mut json_buffer).await;
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            write.send(Message::Pong(data)).await?;
                        }
                        Some(Ok(Message::Close(_))) => {
                            info!("Polymarket User WS closed by server");
                            break;
                        }
                        Some(Err(e)) => return Err(e.into()),
                        None => break,
                        _ => {}
                    }
                }
                _ = ping_interval.tick() => {
                    // Polymarket expects text "PING" keepalive
                    write.send(Message::Text("PING".to_string())).await?;
                }
            }
        }

        Ok(())
    }

    /// Process a user channel message (order or trade event)
    async fn process_user_message(&self, text: &str, buffer: &mut Vec<u8>) {
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        buffer.clear();
        buffer.extend_from_slice(text.as_bytes());

        let msg: UserChannelMessage = match simd_json::from_slice(buffer) {
            Ok(m) => m,
            Err(e) => {
                debug!("Failed to parse user channel msg: {} - {}", e, &text[..text.len().min(100)]);
                return;
            }
        };

        let event_type = msg.event_type.unwrap_or("");

        match event_type {
            "order" => {
                // Order placement / update / cancellation
                // type field: "PLACEMENT", "UPDATE", "CANCELLATION"
                let order_type = msg.msg_type.unwrap_or("UNKNOWN");
                let order_id = msg.id.unwrap_or("");
                let asset_id = msg.asset_id.unwrap_or("");

                // Map order type to a status string for process_order_update.
                // CRITICAL: For "UPDATE", check if size_matched >= original_size
                // to distinguish full fills from partial fills. If we always say
                // "PARTIALLY_FILLED", the order is never terminal and leaks in pending_orders.
                let status = match order_type {
                    "PLACEMENT" => "OPEN",
                    "UPDATE" => {
                        // Check if fully filled: size_matched >= original_size
                        let matched = msg.size_matched.and_then(|s| Decimal::from_str(s).ok())
                            .unwrap_or(Decimal::ZERO);
                        let original = msg.original_size.and_then(|s| Decimal::from_str(s).ok())
                            .unwrap_or(Decimal::ONE); // default 1 to avoid false FILLED
                        if matched >= original && matched > Decimal::ZERO {
                            "FILLED"
                        } else {
                            "PARTIALLY_FILLED"
                        }
                    }
                    "CANCELLATION" => "CANCELLED",
                    other => other,
                };

                info!("📋 Order {}: {} {} @ {} (matched: {}, status: {})",
                    order_type,
                    msg.side.unwrap_or("?"),
                    msg.original_size.unwrap_or("0"),
                    msg.price.unwrap_or("0"),
                    msg.size_matched.unwrap_or("0"),
                    status,
                );

                let _ = self.event_tx.send(WsEvent::OrderUpdate {
                    order_id: order_id.to_string(),
                    asset_id: asset_id.to_string(),
                    status: status.to_string(),
                    side: msg.side.unwrap_or("BUY").to_string(),
                    price: msg.price.unwrap_or("0").to_string(),
                    original_size: msg.original_size.unwrap_or("0").to_string(),
                    size_matched: msg.size_matched.unwrap_or("0").to_string(),
                    timestamp_ns,
                }).await;
            }
            "trade" => {
                // Trade fill event
                let trade_status = msg.status.unwrap_or("MATCHED");
                let order_id = msg.id.unwrap_or("");
                let asset_id = msg.asset_id.unwrap_or("");

                // Only process MATCHED status (actual fills)
                if trade_status == "MATCHED" {
                    info!("💰 Trade FILL: {} {} @ {} (status: {})",
                        msg.side.unwrap_or("?"),
                        msg.size.unwrap_or("0"),
                        msg.price.unwrap_or("0"),
                        trade_status,
                    );

                    let _ = self.event_tx.send(WsEvent::OrderUpdate {
                        order_id: order_id.to_string(),
                        asset_id: asset_id.to_string(),
                        status: "MATCHED".to_string(),
                        side: msg.side.unwrap_or("BUY").to_string(),
                        price: msg.price.unwrap_or("0").to_string(),
                        original_size: msg.size.unwrap_or("0").to_string(),
                        size_matched: msg.size.unwrap_or("0").to_string(),
                        timestamp_ns,
                    }).await;
                } else {
                    debug!("Trade status update: {} for order {}", trade_status, order_id);
                }
            }
            _ => {
                debug!("User channel unknown event_type: '{}'", event_type);
            }
        }
    }
}

/// Binance WebSocket handler for price feeds
pub struct BinanceWs {
    config: Arc<Config>,
    event_tx: mpsc::Sender<WsEvent>,
}

impl BinanceWs {
    pub fn new(config: Arc<Config>, event_tx: mpsc::Sender<WsEvent>) -> Self {
        Self { config, event_tx }
    }
    
    pub async fn run(self) -> anyhow::Result<()> {
        // Build stream URL for multiple symbols
        let symbols: Vec<String> = self.config.exchanges.symbols
            .iter()
            .map(|s| format!("{}@bookTicker", s.to_lowercase()))
            .collect();
        
        let url = format!("{}/{}", self.config.exchanges.binance_ws, symbols.join("/"));
        info!("Connecting to Binance at {}", url);
        
        let mut backoff_secs = 1u64;
        loop {
            match self.connect_and_process(&url).await {
                Ok(_) => {
                    warn!("Binance WebSocket disconnected");
                    backoff_secs = 1; // reset on clean disconnect
                }
                Err(e) => {
                    let err_str = e.to_string();
                    // Binance geo-blocks US IPs with HTTP 451 — stop retrying
                    if err_str.contains("451") {
                        warn!("Binance geo-blocked (HTTP 451) — disabling Binance feed (non-critical)");
                        return Ok(());
                    }
                    error!("Binance WebSocket error: {}", err_str);
                    let _ = self.event_tx.send(WsEvent::Error {
                        source: ConnectionSource::Binance,
                        message: err_str,
                    }).await;
                }
            }
            
            let _ = self.event_tx.send(WsEvent::ConnectionStatus {
                source: ConnectionSource::Binance,
                connected: false,
            }).await;
            
            tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
            backoff_secs = (backoff_secs * 2).min(60); // exponential backoff, max 60s
        }
    }
    
    async fn connect_and_process(&self, url: &str) -> anyhow::Result<()> {
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();
        
        let _ = self.event_tx.send(WsEvent::ConnectionStatus {
            source: ConnectionSource::Binance,
            connected: true,
        }).await;
        
        let mut buffer = vec![0u8; 4096];
        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(30));
        
        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            self.process_ticker(&text, &mut buffer).await;
                        }
                        Some(Ok(Message::Ping(data))) => {
                            write.send(Message::Pong(data)).await?;
                        }
                        Some(Ok(Message::Close(_))) => break,
                        Some(Err(e)) => return Err(e.into()),
                        None => break,
                        _ => {}
                    }
                }
                _ = ping_interval.tick() => {
                    write.send(Message::Pong(vec![])).await?;
                }
            }
        }
        
        Ok(())
    }
    
    #[inline]
    async fn process_ticker(&self, text: &str, buffer: &mut Vec<u8>) {
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        
        buffer.clear();
        buffer.extend_from_slice(text.as_bytes());
        
        if let Ok(ticker) = simd_json::from_slice::<BinanceTickerMessage>(buffer) {
            if let (Ok(bid), Ok(ask)) = (
                Decimal::from_str(ticker.bid_price),
                Decimal::from_str(ticker.ask_price),
            ) {
                let _ = self.event_tx.send(WsEvent::ExchangePrice {
                    exchange: Exchange::Binance,
                    symbol: ticker.symbol.to_string(),
                    bid,
                    ask,
                    timestamp_ns,
                }).await;
            }
        }
    }
}

/// Coinbase WebSocket handler
pub struct CoinbaseWs {
    config: Arc<Config>,
    event_tx: mpsc::Sender<WsEvent>,
}

impl CoinbaseWs {
    pub fn new(config: Arc<Config>, event_tx: mpsc::Sender<WsEvent>) -> Self {
        Self { config, event_tx }
    }
    
    pub async fn run(self) -> anyhow::Result<()> {
        info!("Connecting to Coinbase at {}", self.config.exchanges.coinbase_ws);
        
        loop {
            match self.connect_and_process().await {
                Ok(_) => warn!("Coinbase WebSocket disconnected"),
                Err(e) => {
                    error!("Coinbase WebSocket error: {}", e);
                    let _ = self.event_tx.send(WsEvent::Error {
                        source: ConnectionSource::Coinbase,
                        message: e.to_string(),
                    }).await;
                }
            }
            
            let _ = self.event_tx.send(WsEvent::ConnectionStatus {
                source: ConnectionSource::Coinbase,
                connected: false,
            }).await;
            
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
    
    async fn connect_and_process(&self) -> anyhow::Result<()> {
        let (ws_stream, _) = connect_async(&self.config.exchanges.coinbase_ws).await?;
        let (mut write, mut read) = ws_stream.split();
        
        let _ = self.event_tx.send(WsEvent::ConnectionStatus {
            source: ConnectionSource::Coinbase,
            connected: true,
        }).await;
        
        // Subscribe to ticker channel
        let product_ids: Vec<&str> = self.config.exchanges.symbols
            .iter()
            .map(|s| {
                // Convert BTCUSDT -> BTC-USD format
                let base = &s[..3];
                if s.ends_with("USDT") || s.ends_with("USD") {
                    format!("{}-USD", base)
                } else {
                    s.clone()
                }
            })
            .collect::<Vec<_>>()
            .iter()
            .map(|s| s.as_str())
            .collect();
        
        let subscribe_msg = serde_json::json!({
            "type": "subscribe",
            "product_ids": self.config.exchanges.symbols.iter()
                .map(|s| format!("{}-USD", &s[..s.len()-4]))
                .collect::<Vec<_>>(),
            "channels": ["ticker"]
        });
        
        write.send(Message::Text(subscribe_msg.to_string())).await?;
        
        let mut buffer = vec![0u8; 4096];
        
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    self.process_ticker(&text, &mut buffer).await;
                }
                Ok(Message::Ping(data)) => {
                    write.send(Message::Pong(data)).await?;
                }
                Ok(Message::Close(_)) => break,
                Err(e) => return Err(e.into()),
                _ => {}
            }
        }
        
        Ok(())
    }
    
    #[inline]
    async fn process_ticker(&self, text: &str, buffer: &mut Vec<u8>) {
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        
        buffer.clear();
        buffer.extend_from_slice(text.as_bytes());
        
        if let Ok(ticker) = simd_json::from_slice::<crate::types::CoinbaseTickerMessage>(buffer) {
            if let (Ok(bid), Ok(ask)) = (
                Decimal::from_str(ticker.best_bid),
                Decimal::from_str(ticker.best_ask),
            ) {
                let _ = self.event_tx.send(WsEvent::ExchangePrice {
                    exchange: Exchange::Coinbase,
                    symbol: ticker.product_id.to_string(),
                    bid,
                    ask,
                    timestamp_ns,
                }).await;
            }
        }
    }
}

/// Mock data generator for Shadow Mode testing
/// Generates simulated market data when live feed is unavailable
pub struct MockDataGenerator {
    event_tx: mpsc::Sender<WsEvent>,
    market_ids: Vec<String>,
}

impl MockDataGenerator {
    pub fn new(event_tx: mpsc::Sender<WsEvent>, market_ids: Vec<String>) -> Self {
        Self { event_tx, market_ids }
    }
    
    /// Run the mock data generator - generates trades every 2-5 seconds
    pub async fn run(&self) -> anyhow::Result<()> {
        use ethers_core::rand::{Rng, SeedableRng};
        use ethers_core::rand::rngs::StdRng;
        
        info!("🎭 Mock data generator started for Shadow Mode testing");
        info!("   Generating simulated trades for {} markets", self.market_ids.len());
        
        // Use StdRng which is Send-safe (ThreadRng is not)
        let mut rng = StdRng::from_entropy();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        
        // Base prices for simulation (prediction market probabilities 0.01 to 0.99)
        let mut prices: Vec<Decimal> = self.market_ids.iter()
            .map(|_| Decimal::from_str("0.50").unwrap())
            .collect();
        
        loop {
            interval.tick().await;
            
            // Pick a random market
            if self.market_ids.is_empty() {
                continue;
            }
            let idx = rng.gen_range(0..self.market_ids.len());
            let market_id = &self.market_ids[idx];
            
            // Random walk the price
            let delta: i32 = rng.gen_range(-5..=5);
            let new_price = prices[idx] + Decimal::new(delta as i64, 3);
            prices[idx] = new_price.max(Decimal::new(1, 2)).min(Decimal::new(99, 2));
            
            let timestamp_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            
            // Generate a trade
            let size = Decimal::new(rng.gen_range(10..500) as i64, 0);
            let side = if rng.gen_bool(0.5) {
                crate::types::Side::Buy
            } else {
                crate::types::Side::Sell
            };
            
            info!("🎭 MOCK TRADE: {} {} @ {} ({:?})", market_id, size, prices[idx], side);
            
            let _ = self.event_tx.send(WsEvent::Trade {
                market_id: market_id.clone(),
                price: prices[idx],
                size,
                side,
                timestamp_ns,
            }).await;
            
            // Also generate book update
            let spread = Decimal::new(rng.gen_range(1..3) as i64, 2);
            let bid = prices[idx] - spread / Decimal::new(2, 0);
            let ask = prices[idx] + spread / Decimal::new(2, 0);
            
            let mut snapshot = OrderBookSnapshot::new();
            snapshot.timestamp_ns = timestamp_ns;
            snapshot.bids[0] = PriceLevel::new(bid, size, 1);
            snapshot.asks[0] = PriceLevel::new(ask, size, 1);
            snapshot.bid_count = 1;
            snapshot.ask_count = 1;
            
            let _ = self.event_tx.send(WsEvent::BookUpdate {
                market_id: market_id.clone(),
                snapshot,
            }).await;
            
            // Random delay 2-5 seconds
            let delay_ms = rng.gen_range(2000..5000);
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
    }
}
