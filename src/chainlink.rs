//! Chainlink price feed reader for Polygon mainnet.
//!
//! Reads `latestRoundData()` from the on-chain Chainlink aggregator contracts
//! for BTC, ETH, SOL, XRP via a public Polygon JSON-RPC endpoint.
//! These are the same feeds used by Polymarket to resolve crypto Up/Down markets.
//!
//! Update frequency: ~7-25 seconds depending on asset volatility.
//! Precision: 8 decimal places (prices returned as integer × 10^-8).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Chainlink aggregator addresses on Polygon mainnet (from docs.chain.link)
const BTC_USD_ADDR: &str  = "0xc907E116054Ad103354f2D350FD2514433D57F6F";
const ETH_USD_ADDR: &str  = "0xF9680D99D6C9589e2a93a78A04A279e509205945";
const SOL_USD_ADDR: &str  = "0x10C8264C0935b3B9870013e057f330Ff3e9C56dC";
const XRP_USD_ADDR: &str  = "0x785ba89291f676b5386652eB12b30cF361020694";

/// Public Polygon RPC — no API key required
const POLYGON_RPC: &str = "https://1rpc.io/matic";

/// `latestRoundData()` selector
const LATEST_ROUND_DATA_SELECTOR: &str = "0xfeaf968c";

/// How often to poll each feed (milliseconds)
const POLL_INTERVAL_MS: u64 = 3_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChainlinkAsset {
    BTC,
    ETH,
    SOL,
    XRP,
}

impl ChainlinkAsset {
    pub fn address(&self) -> &'static str {
        match self {
            ChainlinkAsset::BTC => BTC_USD_ADDR,
            ChainlinkAsset::ETH => ETH_USD_ADDR,
            ChainlinkAsset::SOL => SOL_USD_ADDR,
            ChainlinkAsset::XRP => XRP_USD_ADDR,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            ChainlinkAsset::BTC => "BTC",
            ChainlinkAsset::ETH => "ETH",
            ChainlinkAsset::SOL => "SOL",
            ChainlinkAsset::XRP => "XRP",
        }
    }

    pub fn all() -> &'static [ChainlinkAsset] {
        &[ChainlinkAsset::BTC, ChainlinkAsset::ETH, ChainlinkAsset::SOL, ChainlinkAsset::XRP]
    }
}

#[derive(Debug, Clone)]
pub struct PriceSample {
    pub price: f64,
    pub updated_at: i64,   // Unix timestamp from the chain
    pub fetched_at: Instant,
}

/// Shared price state — written by the background poller, read by the engine.
#[derive(Default)]
pub struct ChainlinkPrices {
    inner: RwLock<HashMap<ChainlinkAsset, PriceSample>>,
}

impl ChainlinkPrices {
    pub fn new() -> Self {
        Self { inner: RwLock::new(HashMap::new()) }
    }

    pub fn get(&self, asset: ChainlinkAsset) -> Option<PriceSample> {
        self.inner.read().ok()?.get(&asset).cloned()
    }

    pub fn set(&self, asset: ChainlinkAsset, sample: PriceSample) {
        if let Ok(mut map) = self.inner.write() {
            map.insert(asset, sample);
        }
    }

    /// Returns price only if it was updated within `max_age_secs` seconds.
    pub fn fresh_price(&self, asset: ChainlinkAsset, max_age_secs: u64) -> Option<f64> {
        let sample = self.get(asset)?;
        if sample.fetched_at.elapsed() < Duration::from_secs(max_age_secs) {
            Some(sample.price)
        } else {
            None
        }
    }
}

/// Parse `latestRoundData()` ABI-encoded response.
/// Returns (answer, updatedAt) or None on parse failure.
/// Response layout (each field = 32 bytes = 64 hex chars):
///   [0]  roundId
///   [1]  answer       ← price × 10^8
///   [2]  startedAt
///   [3]  updatedAt    ← Unix timestamp
///   [4]  answeredInRound
fn parse_latest_round_data(hex: &str) -> Option<(f64, i64)> {
    let data = hex.strip_prefix("0x").unwrap_or(hex);
    if data.len() < 5 * 64 { return None; }
    let answer_hex = &data[64..128];
    let updated_hex = &data[192..256];
    let answer = i64::from_str_radix(answer_hex, 16).ok()?;
    let updated_at = i64::from_str_radix(updated_hex, 16).ok()?;
    Some((answer as f64 / 1e8, updated_at))
}

/// Fetch the latest price for a single asset via JSON-RPC `eth_call`.
async fn fetch_price(client: &reqwest::Client, asset: ChainlinkAsset) -> Option<PriceSample> {
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [
            { "to": asset.address(), "data": LATEST_ROUND_DATA_SELECTOR },
            "latest"
        ],
        "id": 1
    });

    let resp = client
        .post(POLYGON_RPC)
        .json(&payload)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .ok()?;

    let json: serde_json::Value = resp.json().await.ok()?;
    let result = json.get("result")?.as_str()?;
    let (price, updated_at) = parse_latest_round_data(result)?;

    if price <= 0.0 { return None; }

    Some(PriceSample { price, updated_at, fetched_at: Instant::now() })
}

/// Background task: polls all 4 Chainlink feeds every POLL_INTERVAL_MS.
/// Writes results into the shared `ChainlinkPrices` state.
pub async fn run_chainlink_poller(prices: Arc<ChainlinkPrices>) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("Failed to build HTTP client for Chainlink poller");

    loop {
        for &asset in ChainlinkAsset::all() {
            match fetch_price(&client, asset).await {
                Some(sample) => {
                    debug!("⛓️  Chainlink {}: ${:.4} (chain age: {}s)",
                           asset.label(), sample.price,
                           chrono::Utc::now().timestamp() - sample.updated_at);
                    prices.set(asset, sample);
                }
                None => {
                    warn!("⚠️  Chainlink {}: fetch failed", asset.label());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
}
