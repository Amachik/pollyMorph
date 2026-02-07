//! Zero-copy and arena-allocated data structures for the hot path
//! Designed for minimal heap allocations during trading operations

use parking_lot::RwLock;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// Market side for orders
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

impl Side {
    #[inline(always)]
    pub fn opposite(&self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

/// Order type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OrderType {
    Limit = 0,
    Market = 1,
    GoodTilCancelled = 2,
    FillOrKill = 3,
    ImmediateOrCancel = 4,
}

/// Time in force for orders
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TimeInForce {
    GTC = 0, // Good til cancelled
    GTD = 1, // Good til date
    FOK = 2, // Fill or kill
    IOC = 3, // Immediate or cancel
}

/// Price level in the order book (optimized for cache locality)
#[derive(Debug, Clone, Copy)]
#[repr(C, align(32))] // Cache-line aligned
pub struct PriceLevel {
    pub price: Decimal,
    pub size: Decimal,
    pub order_count: u32,
    _padding: u32,
}

impl PriceLevel {
    #[inline(always)]
    pub const fn new(price: Decimal, size: Decimal, order_count: u32) -> Self {
        Self {
            price,
            size,
            order_count,
            _padding: 0,
        }
    }
    
    #[inline(always)]
    pub const fn empty() -> Self {
        Self {
            price: Decimal::ZERO,
            size: Decimal::ZERO,
            order_count: 0,
            _padding: 0,
        }
    }
}

/// Fixed-size order book snapshot (stack-allocated, no heap)
/// Stores top N levels for both bids and asks
pub const MAX_BOOK_DEPTH: usize = 10;

#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))]
pub struct OrderBookSnapshot {
    pub bids: [PriceLevel; MAX_BOOK_DEPTH],
    pub asks: [PriceLevel; MAX_BOOK_DEPTH],
    pub bid_count: u8,
    pub ask_count: u8,
    pub timestamp_ns: u64,
    pub sequence: u64,
    /// Token ID (asset_id hash) for market identification through the pipeline
    pub token_id: u64,
}

impl OrderBookSnapshot {
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            bids: [PriceLevel::empty(); MAX_BOOK_DEPTH],
            asks: [PriceLevel::empty(); MAX_BOOK_DEPTH],
            bid_count: 0,
            ask_count: 0,
            timestamp_ns: 0,
            sequence: 0,
            token_id: 0,
        }
    }
    
    /// Get best bid price
    #[inline(always)]
    pub fn best_bid(&self) -> Option<Decimal> {
        if self.bid_count > 0 {
            Some(self.bids[0].price)
        } else {
            None
        }
    }
    
    /// Get best ask price
    #[inline(always)]
    pub fn best_ask(&self) -> Option<Decimal> {
        if self.ask_count > 0 {
            Some(self.asks[0].price)
        } else {
            None
        }
    }
    
    /// Get mid price
    #[inline(always)]
    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid + ask) / Decimal::TWO),
            _ => None,
        }
    }
    
    /// Get spread in basis points
    #[inline(always)]
    pub fn spread_bps(&self) -> Option<u32> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) if bid > Decimal::ZERO => {
                let spread = (ask - bid) / bid * Decimal::new(10000, 0);
                Some(spread.to_u32().unwrap_or(0))
            }
            _ => None,
        }
    }
}

impl Default for OrderBookSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

/// Market identifier (fixed-size, stack-allocated)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct MarketId {
    /// Condition ID (32 bytes for Ethereum-style hash)
    pub condition_id: [u8; 32],
    /// Token ID for the specific outcome
    pub token_id: u64,
}

impl MarketId {
    #[inline(always)]
    pub const fn new(condition_id: [u8; 32], token_id: u64) -> Self {
        Self { condition_id, token_id }
    }
    
    /// Create from hex string (for initialization, not hot path)
    pub fn from_hex(hex: &str, token_id: u64) -> Option<Self> {
        let hex = hex.strip_prefix("0x").unwrap_or(hex);
        if hex.len() != 64 {
            return None;
        }
        
        let mut condition_id = [0u8; 32];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let s = std::str::from_utf8(chunk).ok()?;
            condition_id[i] = u8::from_str_radix(s, 16).ok()?;
        }
        
        Some(Self { condition_id, token_id })
    }
}

/// Trade signal from the pricing engine
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct TradeSignal {
    pub market_id: MarketId,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub order_type: OrderType,
    pub expected_profit_bps: i32,
    pub signal_timestamp_ns: u64,
    pub urgency: SignalUrgency,
}

/// Signal urgency level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SignalUrgency {
    Low = 0,      // Can wait, use maker order
    Medium = 1,   // Should act soon
    High = 2,     // Act immediately, use taker if needed
    Critical = 3, // Arb opportunity, take now
}

/// Pre-computed order for fast submission.
/// Contains all fields needed for both EIP-712 signing and API submission.
/// Matches the on-chain Order struct from Polymarket CTF Exchange OrderStructs.sol.
#[derive(Debug, Clone)]
pub struct PreparedOrder {
    pub market_id: MarketId,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub nonce: u64,
    /// Pre-signed EIP-712 signature (r, s, v)
    pub signature: Option<OrderSignature>,
    pub created_at_ns: u64,
    /// Real token ID string for API submission (the full numeric string)
    pub token_id_str: String,
    /// Whether this market uses neg-risk contract
    pub neg_risk: bool,
    // --- EIP-712 signed order fields (needed for API payload) ---
    /// Random salt for order uniqueness (uint256)
    pub salt: String,
    /// Maker address (the funder / wallet address)
    pub maker: String,
    /// Signer address (the EOA that signs)
    pub signer_addr: String,
    /// Taker address (zero address for public orders)
    pub taker: String,
    /// Maker amount as string (USDC or token amount in 6-decimal raw units)
    pub maker_amount: String,
    /// Taker amount as string
    pub taker_amount: String,
    /// Expiration timestamp (unix seconds), "0" for no expiration
    pub expiration: String,
    /// Fee rate in basis points
    pub fee_rate_bps: String,
    /// Signature type: 0=EOA, 1=POLY_PROXY, 2=POLY_GNOSIS_SAFE
    pub signature_type: u8,
    /// Post-only flag: if true, order is rejected if it would cross the spread (taker)
    pub post_only: bool,
}

/// EIP-712 signature components
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct OrderSignature {
    pub r: [u8; 32],
    pub s: [u8; 32],
    pub v: u8,
}

impl OrderSignature {
    /// Convert to hex string for API submission
    pub fn to_hex(&self) -> String {
        let mut result = String::with_capacity(132);
        result.push_str("0x");
        for byte in &self.r {
            result.push_str(&format!("{:02x}", byte));
        }
        for byte in &self.s {
            result.push_str(&format!("{:02x}", byte));
        }
        result.push_str(&format!("{:02x}", self.v));
        result
    }
}

/// Atomic nonce generator for order uniqueness
pub struct NonceGenerator {
    counter: AtomicU64,
}

impl NonceGenerator {
    pub fn new() -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        Self {
            counter: AtomicU64::new(timestamp),
        }
    }
    
    #[inline(always)]
    pub fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for NonceGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// WebSocket message types for Polymarket CLOB
/// Polymarket uses "event_type" field, not "type"
#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "'de: 'a"))]
pub enum ClobMessage<'a> {
    #[serde(rename = "book")]
    Book(BookUpdate<'a>),
    #[serde(rename = "trade")]
    Trade(TradeUpdate<'a>),
    #[serde(rename = "price_change")]
    PriceChange(PriceChangeUpdate<'a>),
    #[serde(rename = "order")]
    Order(OrderUpdate<'a>),
}

/// Price change entry from Polymarket WebSocket
#[derive(Debug, Default, Deserialize, Clone)]
#[serde(default)]
pub struct PriceChange {
    pub asset_id: String,
    pub price: String,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub best_bid: Option<String>,
    #[serde(default)]
    pub best_ask: Option<String>,
}

/// Polymarket WebSocket message wrapper
/// The actual format uses "event_type" for message type discrimination
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct PolymarketMessage<'a> {
    #[serde(borrow, default)]
    pub event_type: Option<&'a str>,
    #[serde(rename = "type", default)]
    pub msg_type: Option<&'a str>,
    #[serde(default)]
    pub asset_id: Option<&'a str>,
    #[serde(default)]
    pub market: Option<&'a str>,
    #[serde(default)]
    pub price: Option<&'a str>,
    #[serde(default)]
    pub size: Option<&'a str>,
    #[serde(default)]
    pub side: Option<&'a str>,
    #[serde(default)]
    pub timestamp: Option<&'a str>,
    #[serde(default)]
    pub bids: Option<Vec<[&'a str; 2]>>,
    #[serde(default)]
    pub asks: Option<Vec<[&'a str; 2]>>,
    #[serde(default)]
    pub hash: Option<&'a str>,
    #[serde(default)]
    pub price_changes: Option<Vec<PriceChange>>,
}

#[derive(Debug, Deserialize)]
pub struct BookUpdate<'a> {
    #[serde(borrow)]
    pub market: &'a str,
    pub asset_id: &'a str,
    pub bids: Vec<[&'a str; 2]>,
    pub asks: Vec<[&'a str; 2]>,
    pub timestamp: &'a str,
    pub hash: &'a str,
}

#[derive(Debug, Deserialize)]
pub struct TradeUpdate<'a> {
    #[serde(borrow)]
    pub market: &'a str,
    pub asset_id: &'a str,
    pub price: &'a str,
    pub size: &'a str,
    pub side: &'a str,
    pub timestamp: &'a str,
}

#[derive(Debug, Deserialize)]
pub struct PriceChangeUpdate<'a> {
    #[serde(borrow)]
    pub market: &'a str,
    pub asset_id: &'a str,
    pub price: &'a str,
    pub timestamp: &'a str,
}

#[derive(Debug, Deserialize)]
pub struct OrderUpdate<'a> {
    #[serde(borrow)]
    pub order_id: &'a str,
    pub status: &'a str,
    pub filled_size: Option<&'a str>,
    pub timestamp: &'a str,
}

/// Exchange price feed message (Binance format)
#[derive(Debug, Deserialize)]
pub struct BinanceTickerMessage<'a> {
    #[serde(borrow, rename = "s")]
    pub symbol: &'a str,
    #[serde(rename = "b")]
    pub bid_price: &'a str,
    #[serde(rename = "B")]
    pub bid_qty: &'a str,
    #[serde(rename = "a")]
    pub ask_price: &'a str,
    #[serde(rename = "A")]
    pub ask_qty: &'a str,
}

/// Exchange price feed message (Coinbase format)
#[derive(Debug, Deserialize)]
pub struct CoinbaseTickerMessage<'a> {
    #[serde(borrow, rename = "product_id")]
    pub product_id: &'a str,
    pub price: &'a str,
    pub best_bid: &'a str,
    pub best_ask: &'a str,
}

/// Execution report for order tracking
#[derive(Debug, Clone)]
pub struct ExecutionReport {
    pub order_id: String,
    pub market_id: MarketId,
    pub side: Side,
    pub price: Decimal,
    pub original_size: Decimal,
    pub filled_size: Decimal,
    pub status: OrderStatus,
    pub submitted_at_ns: u64,
    pub last_update_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OrderStatus {
    Pending = 0,
    Open = 1,
    PartiallyFilled = 2,
    Filled = 3,
    Cancelled = 4,
    Rejected = 5,
    Expired = 6,
}

// =============================================================================
// TOKEN ID REGISTRY — maps u64 hash to real asset_id string
// =============================================================================

/// Per-token market metadata fetched from Polymarket API.
#[derive(Debug, Clone)]
pub struct TokenMarketInfo {
    /// Real asset_id string (the full numeric token ID)
    pub asset_id: String,
    /// Whether this market uses the negRisk exchange contract
    pub neg_risk: bool,
    /// Fee rate in basis points (fetched from GET /fee-rate?token_id=...)
    pub fee_rate_bps: u32,
    /// Minimum tick size for price precision (e.g. "0.01", "0.001", "0.0001")
    pub tick_size: Decimal,
}

/// Global registry mapping u64 FNV hash → real Polymarket token metadata.
/// The hash is used internally for O(1) lookups; the metadata is needed for API calls.
pub struct TokenIdRegistry {
    /// hash → full token market info
    hash_to_info: RwLock<FxHashMap<u64, TokenMarketInfo>>,
}

impl TokenIdRegistry {
    pub fn new() -> Self {
        Self {
            hash_to_info: RwLock::new(FxHashMap::default()),
        }
    }

    /// Register a token with basic info (neg_risk defaults, fee/tick defaults).
    /// Used during initial WS subscription before full market info is fetched.
    pub fn register(&self, hash: u64, asset_id: String, neg_risk: bool) {
        let mut map = self.hash_to_info.write();
        // Don't overwrite if already registered with richer data
        if !map.contains_key(&hash) {
            map.insert(hash, TokenMarketInfo {
                asset_id,
                neg_risk,
                fee_rate_bps: 0,
                tick_size: Decimal::new(1, 2), // default 0.01
            });
        }
    }

    /// Register or update a token with full market info including fee_rate and tick_size.
    pub fn register_full(&self, hash: u64, info: TokenMarketInfo) {
        self.hash_to_info.write().insert(hash, info);
    }

    /// Look up the real asset_id string for a hash.
    pub fn get_str(&self, hash: u64) -> Option<String> {
        self.hash_to_info.read().get(&hash).map(|i| i.asset_id.clone())
    }

    /// Look up whether a token is neg-risk.
    pub fn is_neg_risk(&self, hash: u64) -> bool {
        self.hash_to_info.read().get(&hash).map(|i| i.neg_risk).unwrap_or(false)
    }

    /// Get fee rate in basis points for a token.
    pub fn fee_rate_bps(&self, hash: u64) -> u32 {
        self.hash_to_info.read().get(&hash).map(|i| i.fee_rate_bps).unwrap_or(0)
    }

    /// Get tick size for a token.
    pub fn tick_size(&self, hash: u64) -> Decimal {
        self.hash_to_info.read().get(&hash).map(|i| i.tick_size).unwrap_or(Decimal::new(1, 2))
    }

    /// Get full market info for a token.
    pub fn get_info(&self, hash: u64) -> Option<TokenMarketInfo> {
        self.hash_to_info.read().get(&hash).cloned()
    }

    /// Number of registered tokens.
    pub fn len(&self) -> usize {
        self.hash_to_info.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.hash_to_info.read().is_empty()
    }
}
