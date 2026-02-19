//! Order execution module for Polymarket CLOB
//! Handles order submission with sub-5ms latency target
//! Instrumented with TSC-based timing for nanosecond precision

use crate::config::Config;
use crate::metrics::{TscLatencyTracker, record_cache_lookup_ns, record_risk_check_ns};
use crate::signing::{OrderSigner, SignatureCacheKey};
use crate::types::{
    PreparedOrder, TradeSignal, ExecutionReport, OrderStatus, Side, OrderType, TimeInForce,
    TokenIdRegistry, TokenMarketInfo,
};
use crate::risk::{RiskManager, RiskCheckResult};
use crate::hints::{likely, unlikely, prefetch_l1, rdtsc_start, rdtsc_end, tsc_elapsed, cycles_to_ns_approx};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use base64::{Engine as _, engine::general_purpose};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering as AtomicOrdering};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

type HmacSha256 = Hmac<Sha256>;

/// Result from a batch order submission, containing actual fill amounts.
/// CRITICAL: For FAK orders, actual fill can be less than requested.
/// Always use these amounts for merge/position calculations, never the requested size.
#[derive(Debug, Clone)]
pub struct BatchFillResult {
    pub order_id: String,
    /// Actual tokens received (for BUY) or tokens sold (for SELL), in raw 6-decimal units.
    /// Parse from OrderResponse.takingAmount. "0" if not filled.
    pub taking_amount_raw: u64,
    /// Actual USDC paid (for BUY) or USDC received (for SELL), in raw 6-decimal units.
    /// Parse from OrderResponse.makingAmount. "0" if not filled.
    pub making_amount_raw: u64,
    /// Order status from the API: "MATCHED", "DELAYED", "UNMATCHED", etc.
    pub status: String,
}

impl BatchFillResult {
    /// Actual tokens filled, converted from raw 6-decimal units to human-readable.
    /// For BUY orders: this is tokens received.
    pub fn tokens_filled(&self) -> f64 {
        self.taking_amount_raw as f64 / 1_000_000.0
    }

    /// Actual USDC spent/received, converted from raw 6-decimal units to human-readable.
    /// For BUY orders: this is USDC spent.
    pub fn usdc_amount(&self) -> f64 {
        self.making_amount_raw as f64 / 1_000_000.0
    }
}

/// Order executor with connection pooling and pre-signing
/// 
/// PERF: #[repr(align(64))] ensures struct starts on cache line boundary
/// and prevents false sharing between cores accessing different fields.
#[repr(align(64))]
pub struct OrderExecutor {
    // HOT PATH DATA - accessed every trade (grouped for cache locality)
    signer: Arc<OrderSigner>,
    risk_manager: Arc<RiskManager>,
    
    // METRICS - atomic counters on separate cache lines to prevent false sharing
    orders_sent: AtomicU64,
    _pad1: [u8; 56], // Pad to 64 bytes (cache line)
    avg_latency_ns: AtomicU64,
    _pad2: [u8; 56], // Pad to 64 bytes
    
    // COLD PATH DATA - rarely accessed during hot path
    config: Arc<Config>,
    client: reqwest::Client,
    pending_orders: Mutex<FxHashMap<String, PendingOrder>>,
    report_tx: mpsc::Sender<ExecutionReport>,
    orders_filled: AtomicU64,
    orders_rejected: AtomicU64,
    /// Token ID registry for resolving u64 hash → real asset_id string
    pub token_registry: Arc<TokenIdRegistry>,
    /// Rate limiter: orders sent in current 1-second window
    rate_limit_count: AtomicU64,
    /// Rate limiter: start of current 1-second window (unix seconds)
    rate_limit_window_sec: AtomicU64,
    /// Maximum orders per second
    max_orders_per_sec: u64,
    /// Pre-formatted address string (avoids hex::encode per order)
    cached_address: String,
    /// USDC committed to open BUY orders (in micro-USDC, i.e. * 1e6)
    /// Used to prevent overcommitting limited capital
    committed_capital_micro: AtomicU64,
    /// Maximum USDC to commit to open orders (in micro-USDC)
    max_capital_micro: AtomicU64,
    /// Backoff until timestamp (epoch secs) after 403 from Cloudflare
    api_backoff_until: AtomicU64,
}

#[derive(Debug, Clone)]
struct PendingOrder {
    order: PreparedOrder,
    sent_at_ns: u64,
    expected_profit_bps: i32,
    /// Cumulative filled amount reported so far (for computing deltas on partial fills)
    cumulative_filled: Decimal,
    /// USDC cost of this order (micro-USDC) for capital release on cancel/fill
    cost_micro: u64,
}

impl OrderExecutor {
    pub fn new(
        config: Arc<Config>,
        signer: Arc<OrderSigner>,
        risk_manager: Arc<RiskManager>,
        report_tx: mpsc::Sender<ExecutionReport>,
        token_registry: Arc<TokenIdRegistry>,
    ) -> Self {
        // SSH tunnel support: if rest_url has a non-standard port (e.g. :8443),
        // resolve the hostname to localhost so traffic routes through the SSH tunnel.
        // This bypasses Cloudflare WAF blocking POST requests from datacenter IPs.
        // Usage: set POLYMARKET_REST_URL=https://clob.polymarket.com:8443
        //        and run: ssh -R 8443:clob.polymarket.com:443 user@VPS_IP -N
        let tunnel_mode = {
            let url = &config.polymarket.rest_url;
            let after_scheme = url.strip_prefix("https://").unwrap_or(url);
            after_scheme.find(':').and_then(|colon| {
                let port_str = &after_scheme[colon + 1..];
                port_str.parse::<u16>().ok().map(|port| (after_scheme[..colon].to_string(), port))
            })
        };

        // Configure HTTP client — use longer timeout in tunnel mode (round trip through home PC)
        let timeout_ms = if tunnel_mode.is_some() { 2000 } else { config.network.order_timeout_ms };
        let mut builder = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(timeout_ms))
            // HTTP/2 via ALPN — Cloudflare serves h2 much faster than HTTP/1.1
            // Falls back to HTTP/1.1 if server doesn't support h2
            .http2_adaptive_window(true)
            // Connection pooling — keep connections alive to skip TCP+TLS handshake
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .tcp_nodelay(true)
            .tcp_keepalive(std::time::Duration::from_secs(30))
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
            .default_headers({
                let mut h = reqwest::header::HeaderMap::new();
                h.insert(reqwest::header::ACCEPT, "application/json".parse().unwrap());
                h.insert(reqwest::header::ACCEPT_LANGUAGE, "en-US,en;q=0.9".parse().unwrap());
                // Note: Do NOT set Connection header — it's a hop-by-hop header
                // prohibited in HTTP/2 (RFC 7540 §8.1.2.2). Setting it causes
                // "Connection header illegal in HTTP/2" warnings from hyper on
                // every response. HTTP/2 connections are persistent by design.
                h
            });

        if let Some((ref host, port)) = tunnel_mode {
            builder = builder.resolve(
                host,
                std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            );
            info!("SSH tunnel mode: {} -> localhost:{} (timeout={}ms)", host, port, timeout_ms);
        }

        let client = builder.build().expect("Failed to create HTTP client");
        
        // Pre-compute address string before moving signer into struct
        let cached_address = format!("0x{}", hex::encode(signer.address().as_slice()));
        // Pre-compute default capital limit before moving config
        let default_max_capital = (config.trading.default_order_size.to_f64().unwrap_or(5.0) * 4.0 * 1_000_000.0) as u64;

        Self {
            // HOT PATH DATA first
            signer,
            risk_manager,
            // Padded metrics counters
            orders_sent: AtomicU64::new(0),
            _pad1: [0u8; 56],
            avg_latency_ns: AtomicU64::new(0),
            _pad2: [0u8; 56],
            // COLD PATH DATA
            config,
            client,
            pending_orders: Mutex::new(FxHashMap::default()),
            report_tx,
            orders_filled: AtomicU64::new(0),
            orders_rejected: AtomicU64::new(0),
            token_registry,
            rate_limit_count: AtomicU64::new(0),
            rate_limit_window_sec: AtomicU64::new(0),
            max_orders_per_sec: 15, // 15 orders/sec — balanced between safety and competitiveness
            cached_address,
            committed_capital_micro: AtomicU64::new(0),
            max_capital_micro: AtomicU64::new(default_max_capital),
            api_backoff_until: AtomicU64::new(0),
        }
    }
    
    /// Execute a trade signal - HOT PATH
    /// This is the critical path that must complete in sub-5ms
    /// 
    /// SAFETY: Kill switch (AtomicBool) is checked FIRST before any other operation
    #[inline(always)] // Force inline for hot path
    pub async fn execute_signal(&self, signal: TradeSignal) -> Result<String, ExecutionError> {
        // RAW RDTSC - bypasses quanta overhead (~2ns faster)
        let tsc_start = rdtsc_start();
        
        // ================================================================
        // SAFETY CHECK #1: Kill switch MUST be checked FIRST (AtomicBool)
        // This is the absolute first check in the hot path for safety
        // Branch hint: trading is almost always allowed (kill switch rarely triggers)
        // ================================================================
        if unlikely(!self.risk_manager.is_trading_allowed()) {
            return Err(ExecutionError::KillSwitchActive);
        }
        
        // Check Cloudflare backoff — skip API calls during cooldown
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let backoff_until = self.api_backoff_until.load(AtomicOrdering::Relaxed);
        if now_secs < backoff_until {
            return Err(ExecutionError::RateLimited);
        }
        
        // 1. Full risk check with TSC timing
        let risk_start = TscLatencyTracker::start_raw();
        let risk_result = self.risk_manager.check_order(
            signal.market_id,
            signal.side,
            signal.size,
            signal.price,
        );
        record_risk_check_ns(TscLatencyTracker::elapsed_ns_raw(risk_start) as f64);
        
        // Branch hint: orders are almost always approved
        if likely(matches!(risk_result, RiskCheckResult::Approved)) {
            // Fast path - continue
        } else {
            // Slow path - handle rejection (cold)
            crate::metrics::ORDERS_REJECTED_RISK.inc();
            return match risk_result {
                RiskCheckResult::Approved => unreachable!(),
                RiskCheckResult::RejectedKillSwitch => Err(ExecutionError::KillSwitchActive),
                RiskCheckResult::RejectedExposureLimit => Err(ExecutionError::ExposureLimitExceeded),
                RiskCheckResult::RejectedPositionLimit => Err(ExecutionError::PositionLimitExceeded),
                RiskCheckResult::RejectedCooldown => Err(ExecutionError::InCooldown),
                RiskCheckResult::RejectedStaleData => Err(ExecutionError::StaleData),
                RiskCheckResult::RejectedGapProtection => Err(ExecutionError::GapProtectionTriggered),
            };
        }
        
        // 2. Resolve market info and round price BEFORE cache lookup
        // so the cache key matches the actual signed price
        prefetch_l1(&self.signer as *const _);
        
        let token_id_str = self.token_registry.get_str(signal.market_id.token_id)
            .unwrap_or_else(|| format!("{}", signal.market_id.token_id));
        let neg_risk = self.token_registry.is_neg_risk(signal.market_id.token_id);
        let fee_rate_bps = self.token_registry.fee_rate_bps(signal.market_id.token_id);
        let tick_size = self.token_registry.tick_size(signal.market_id.token_id);

        // Round price to valid tick_size (Polymarket rejects invalid precision)
        let rounded_price = Self::round_to_tick_size(signal.price, tick_size);
        // Validate price is within valid range: [tick_size, 1 - tick_size]
        let max_price = Decimal::ONE - tick_size;
        if rounded_price < tick_size || rounded_price > max_price {
            return Err(ExecutionError::SigningFailed(
                format!("Price {} out of valid range [{}, {}]", rounded_price, tick_size, max_price)
            ));
        }

        // Cache lookup uses rounded_price so cache key matches the actual signed order
        let cache_start = TscLatencyTracker::start_raw();
        let cache_key = SignatureCacheKey::new(
            signal.market_id,
            signal.side,
            rounded_price,
            signal.size,
        );
        let cached = self.signer.get_cached_signature(&cache_key);
        record_cache_lookup_ns(TscLatencyTracker::elapsed_ns_raw(cache_start) as f64);

        // Branch hint: cache hit is the expected fast path (pre-warmed cache)
        // NOTE: debug! logging REMOVED from hot path - even disabled logging has ~5ns overhead
        let mut order = if likely(cached.is_some()) {
            // FAST PATH: zero-allocation cache hit
            cached.unwrap()
        } else {
            // SLOW PATH: sign on-demand (cold, acceptable to be slower)
            let sign_start = std::time::Instant::now();
            let signed = self.signer.prepare_order_full(
                signal.market_id,
                signal.side,
                rounded_price,
                signal.size,
                signal.order_type,
                TimeInForce::GTC,
                0, // no expiration for GTC (matches Python SDK)
                token_id_str.clone(),
                neg_risk,
                fee_rate_bps,
            ).await.map_err(|e| ExecutionError::SigningFailed(e.to_string()))?;
            crate::metrics::record_signing_latency(sign_start.elapsed().as_secs_f64());
            signed
        };
        // Ensure token_id_str and neg_risk are set even on cache hits
        if order.token_id_str.is_empty() {
            order.token_id_str = token_id_str;
            order.neg_risk = neg_risk;
        }

        // Rate limit: max N orders per second to avoid spamming the API
        {
            let now_sec = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let window = self.rate_limit_window_sec.load(AtomicOrdering::Relaxed);
            if now_sec != window {
                // New second window — reset counter
                self.rate_limit_window_sec.store(now_sec, AtomicOrdering::Relaxed);
                self.rate_limit_count.store(1, AtomicOrdering::Relaxed);
            } else {
                let count = self.rate_limit_count.fetch_add(1, AtomicOrdering::Relaxed);
                if count >= self.max_orders_per_sec {
                    return Err(ExecutionError::RateLimited);
                }
            }
        }

        // Force post_only for maker orders in the 0.40-0.60 high-fee zone
        // This prevents accidental taker fills at 3.15% fees
        if self.config.trading.fee_curve_2026.is_forced_maker_zone(rounded_price) {
            order.post_only = true;
        }
        // All Low/Medium urgency signals (market making) should also be post_only
        if matches!(signal.urgency, crate::types::SignalUrgency::Low | crate::types::SignalUrgency::Medium) {
            order.post_only = true;
        }
        
        // 4. Capital gate: prevent overcommitting limited USDC balance
        let order_cost_micro = if signal.side == Side::Buy {
            // BUY cost = size * price in USDC (micro-USDC = * 1e6)
            let cost = (signal.size * rounded_price).to_f64().unwrap_or(0.0);
            let cost_micro = (cost * 1_000_000.0) as u64;
            let current = self.committed_capital_micro.load(AtomicOrdering::Relaxed);
            if current + cost_micro > self.max_capital_micro.load(AtomicOrdering::Relaxed) {
                return Err(ExecutionError::ApiError(format!(
                    "Capital limit: committed ${:.2} + ${:.2} > max ${:.2}",
                    current as f64 / 1e6, cost as f64, self.max_capital_micro.load(AtomicOrdering::Relaxed) as f64 / 1e6
                )));
            }
            cost_micro
        } else {
            0 // SELL orders don't commit USDC
        };

        // 5. Submit order via REST API
        let order_id = self.submit_order(&order).await?;
        
        // 6. Commit capital and track pending order
        if order_cost_micro > 0 {
            self.committed_capital_micro.fetch_add(order_cost_micro, AtomicOrdering::Relaxed);
        }
        {
            let mut pending = self.pending_orders.lock();
            pending.insert(order_id.clone(), PendingOrder {
                order,
                sent_at_ns: Self::now_ns(),
                expected_profit_bps: signal.expected_profit_bps,
                cumulative_filled: Decimal::ZERO,
                cost_micro: order_cost_micro,
            });
        }
        
        // 6. RAW TSC latency measurement (zero overhead vs quanta)
        let tsc_end_val = rdtsc_end();
        let elapsed_cycles = tsc_elapsed(tsc_start, tsc_end_val);
        let latency_ns = cycles_to_ns_approx(elapsed_cycles);
        
        // Relaxed ordering is sufficient for counters (no synchronization needed)
        self.orders_sent.fetch_add(1, AtomicOrdering::Relaxed);
        self.update_avg_latency(latency_ns);
        
        // Record tick-to-trade latency in Prometheus histogram
        let latency_secs = latency_ns as f64 / 1_000_000_000.0;
        let side_str = match signal.side { Side::Buy => "buy", Side::Sell => "sell" };
        let otype = if signal.order_type == OrderType::Market { "market" } else { "limit" };
        crate::metrics::TICK_TO_TRADE_LATENCY
            .with_label_values(&["polymarket", side_str, otype])
            .observe(latency_secs);
        
        // NOTE: All debug! logging removed from hot path for maximum speed
        Ok(order_id)
    }
    
    /// Compute L2 HMAC signature for Polymarket API authentication.
    /// Signs: timestamp + method + path + body using the base64-decoded API secret.
    fn compute_l2_hmac(
        &self,
        timestamp: &str,
        method: &str,
        path: &str,
        body: &str,
    ) -> Result<String, ExecutionError> {
        // Polymarket SDK uses base64.urlsafe_b64decode / urlsafe_b64encode
        let secret_bytes = general_purpose::URL_SAFE
            .decode(&self.config.polymarket.api_secret)
            .map_err(|e| ExecutionError::SigningFailed(format!("Bad API secret base64: {}", e)))?;

        let mut mac = HmacSha256::new_from_slice(&secret_bytes)
            .map_err(|e| ExecutionError::SigningFailed(format!("HMAC init failed: {}", e)))?;

        // Message format: timestamp + method + path [+ body]
        let mut message = format!("{}{}{}", timestamp, method, path);
        if !body.is_empty() {
            message.push_str(body);
        }
        mac.update(message.as_bytes());

        let result = mac.finalize();
        Ok(general_purpose::URL_SAFE.encode(result.into_bytes()))
    }

    /// Build L2 authentication headers for Polymarket CLOB API.
    fn l2_headers(
        &self,
        method: &str,
        path: &str,
        body: &str,
    ) -> Result<Vec<(String, String)>, ExecutionError> {
        let timestamp = chrono::Utc::now().timestamp().to_string();
        let hmac_sig = self.compute_l2_hmac(&timestamp, method, path, body)?;

        Ok(vec![
            ("POLY_ADDRESS".to_string(), self.cached_address.clone()),
            ("POLY_SIGNATURE".to_string(), hmac_sig),
            ("POLY_TIMESTAMP".to_string(), timestamp),
            ("POLY_API_KEY".to_string(), self.config.polymarket.api_key.clone()),
            ("POLY_PASSPHRASE".to_string(), self.config.polymarket.api_passphrase.clone()),
        ])
    }

    /// Submit order to Polymarket REST API.
    /// Payload format matches the official Python SDK's `order_to_json(order, owner, orderType)`.
    /// The `order` dict contains the full signed EIP-712 order fields.
    async fn submit_order(&self, order: &PreparedOrder) -> Result<String, ExecutionError> {
        let signature = order.signature.as_ref()
            .ok_or(ExecutionError::SigningFailed("No signature".to_string()))?;

        // Use real token_id_str for API submission (NOT the internal u64 hash)
        let token_id_for_api = if order.token_id_str.is_empty() {
            self.token_registry.get_str(order.market_id.token_id)
                .unwrap_or_else(|| {
                    warn!("No real token_id_str for hash {} — order will likely be rejected",
                          order.market_id.token_id);
                    format!("{}", order.market_id.token_id)
                })
        } else {
            order.token_id_str.clone()
        };

        let order_type_str = match order.order_type {
            OrderType::Limit => "GTC",
            OrderType::FillOrKill => "FOK",
            OrderType::ImmediateOrCancel => "FAK",
            _ => "GTC",
        };

        // Build payload matching official SDK: order_to_json(signed_order, api_key, orderType)
        // signed_order.dict() contains the full EIP-712 struct fields
        let payload = serde_json::json!({
            "order": {
                "salt": order.salt,
                "maker": order.maker,
                "signer": order.signer_addr,
                "taker": order.taker,
                "tokenId": token_id_for_api,
                "makerAmount": order.maker_amount,
                "takerAmount": order.taker_amount,
                "expiration": order.expiration,
                "nonce": order.nonce.to_string(),
                "feeRateBps": order.fee_rate_bps,
                "side": match order.side {
                    Side::Buy => "BUY",
                    Side::Sell => "SELL",
                },
                "signatureType": order.signature_type,
                "signature": signature.to_hex(),
            },
            // owner = API key, which in Polymarket's system IS the CLOB proxy wallet address.
            // This matches the Python SDK: order_to_json(order, self.creds.api_key, ...)
            "owner": self.config.polymarket.api_key.clone(),
            "orderType": order_type_str,
            "postOnly": order.post_only,
        });

        // Diagnostic: log full payload for sell orders to debug "not enough balance / allowance"
        if order.side == Side::Sell {
            let debug_payload = serde_json::to_string_pretty(&payload).unwrap_or_default();
            info!("🔍 SELL payload:\n{}", debug_payload);
        }

        let path = "/order";
        let url = format!("{}{}", self.config.polymarket.rest_url, path);
        // Use compact JSON serialization (no spaces) to match Python SDK's separators=(',', ':')
        let body_str = serde_json::to_string(&payload)
            .map_err(|e| ExecutionError::SigningFailed(e.to_string()))?;


        let headers = self.l2_headers("POST", path, &body_str)?;

        let mut request = self.client
            .post(&url)
            .header("Content-Type", "application/json");

        for (key, value) in &headers {
            request = request.header(key.as_str(), value.as_str());
        }

        let response = request
            .body(body_str)
            .send()
            .await
            .map_err(|e| ExecutionError::NetworkError(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            // Truncate HTML error pages to avoid log flooding
            let body_preview = if body.len() > 200 { &body[..200] } else { &body };
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if status.as_u16() == 403 {
                // Cloudflare rate limit — back off for 10 seconds
                self.api_backoff_until.store(now + 10, AtomicOrdering::Relaxed);
                warn!("⏸️  403 from Cloudflare — backing off 10s");
            } else if body.contains("not enough balance") || body.contains("allowance") {
                // Balance/allowance issue — back off 30s (won't fix itself without on-chain action)
                self.api_backoff_until.store(now + 30, AtomicOrdering::Relaxed);
                error!("Order submission failed: {} - {} (backing off 30s)", status, body_preview);
            } else {
                error!("Order submission failed: {} - {}", status, body_preview);
            }
            return Err(ExecutionError::ApiError(format!("{}: {}", status, body_preview)));
        }

        let result: serde_json::Value = response.json().await
            .map_err(|e| ExecutionError::ParseError(e.to_string()))?;

        let order_id = result["orderID"]
            .as_str()
            .ok_or(ExecutionError::ParseError("Missing orderID".to_string()))?
            .to_string();

        Ok(order_id)
    }
    
    /// Submit multiple orders in a single batch request via POST /orders.
    /// Polymarket supports up to 15 orders per batch.
    /// Returns a Vec of Result<BatchFillResult> with actual fill amounts from the API.
    /// CRITICAL: For FAK orders, the actual fill can be less than requested.
    /// Always use the returned takingAmount/makingAmount, never the requested size.
    pub async fn submit_orders_batch(
        &self,
        orders: &[PreparedOrder],
    ) -> Vec<Result<BatchFillResult, ExecutionError>> {
        if orders.is_empty() {
            return Vec::new();
        }

        // Polymarket batch limit is 15 orders per request
        const BATCH_LIMIT: usize = 15;
        let mut all_results = Vec::with_capacity(orders.len());

        for chunk in orders.chunks(BATCH_LIMIT) {
            let mut order_payloads = Vec::with_capacity(chunk.len());

            for order in chunk {
                let signature = match order.signature.as_ref() {
                    Some(sig) => sig,
                    None => {
                        all_results.push(Err(ExecutionError::SigningFailed("No signature".to_string())));
                        continue;
                    }
                };

                let token_id_for_api = if order.token_id_str.is_empty() {
                    self.token_registry.get_str(order.market_id.token_id)
                        .unwrap_or_else(|| format!("{}", order.market_id.token_id))
                } else {
                    order.token_id_str.clone()
                };

                let order_type_str = match order.order_type {
                    OrderType::Limit => "GTC",
                    OrderType::FillOrKill => "FOK",
                    OrderType::ImmediateOrCancel => "FAK",
                    _ => "GTC",
                };

                order_payloads.push(serde_json::json!({
                    "order": {
                        "salt": order.salt,
                        "maker": order.maker,
                        "signer": order.signer_addr,
                        "taker": order.taker,
                        "tokenId": token_id_for_api,
                        "makerAmount": order.maker_amount,
                        "takerAmount": order.taker_amount,
                        "expiration": order.expiration,
                        "nonce": order.nonce.to_string(),
                        "feeRateBps": order.fee_rate_bps,
                        "side": match order.side {
                            Side::Buy => "BUY",
                            Side::Sell => "SELL",
                        },
                        "signatureType": order.signature_type,
                        "signature": signature.to_hex(),
                    },
                    "owner": self.config.polymarket.api_key.clone(),
                    "orderType": order_type_str,
                    "postOnly": order.post_only,
                }));
            }

            if order_payloads.is_empty() {
                continue;
            }

            let path = "/orders";
            let url = format!("{}{}", self.config.polymarket.rest_url, path);
            let body_str = match serde_json::to_string(&order_payloads) {
                Ok(s) => s,
                Err(e) => {
                    for _ in 0..order_payloads.len() {
                        all_results.push(Err(ExecutionError::SigningFailed(e.to_string())));
                    }
                    continue;
                }
            };

            let headers = match self.l2_headers("POST", path, &body_str) {
                Ok(h) => h,
                Err(e) => {
                    for _ in 0..order_payloads.len() {
                        all_results.push(Err(e.clone()));
                    }
                    continue;
                }
            };

            let mut request = self.client
                .post(&url)
                .header("Content-Type", "application/json");

            for (key, value) in &headers {
                request = request.header(key.as_str(), value.as_str());
            }

            let response = match request.body(body_str).send().await {
                Ok(r) => r,
                Err(e) => {
                    for _ in 0..order_payloads.len() {
                        all_results.push(Err(ExecutionError::NetworkError(e.to_string())));
                    }
                    continue;
                }
            };

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                let body_preview = if body.len() > 200 { &body[..200] } else { &body };

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if status.as_u16() == 403 {
                    self.api_backoff_until.store(now + 10, AtomicOrdering::Relaxed);
                    warn!("⏸️  403 from Cloudflare on batch — backing off 10s");
                }

                let err_msg = format!("{}: {}", status, body_preview);
                for _ in 0..order_payloads.len() {
                    all_results.push(Err(ExecutionError::ApiError(err_msg.clone())));
                }
                continue;
            }

            // Parse batch response — array of OrderResponse objects:
            // { success: bool, errorMsg: string, orderID: string,
            //   transactionsHashes: string[], status: string,
            //   takingAmount: string, makingAmount: string }
            let result: serde_json::Value = match response.json().await {
                Ok(v) => v,
                Err(e) => {
                    for _ in 0..order_payloads.len() {
                        all_results.push(Err(ExecutionError::ParseError(e.to_string())));
                    }
                    continue;
                }
            };

            if let Some(arr) = result.as_array() {
                for item in arr {
                    let success = item["success"].as_bool().unwrap_or(false);
                    if success {
                        if let Some(order_id) = item["orderID"].as_str() {
                            // Extract actual fill amounts from response
                            // For BUY FAK: takingAmount = tokens received, makingAmount = USDC paid
                            // These are in raw 6-decimal string format (e.g. "50000000" = 50 tokens)
                            let taking_raw = item["takingAmount"].as_str()
                                .and_then(|s| s.parse::<u64>().ok())
                                .unwrap_or(0);
                            let making_raw = item["makingAmount"].as_str()
                                .and_then(|s| s.parse::<u64>().ok())
                                .unwrap_or(0);
                            let status = item["status"].as_str()
                                .unwrap_or("UNKNOWN").to_string();

                            all_results.push(Ok(BatchFillResult {
                                order_id: order_id.to_string(),
                                taking_amount_raw: taking_raw,
                                making_amount_raw: making_raw,
                                status,
                            }));
                        } else {
                            all_results.push(Err(ExecutionError::ParseError(
                                "success=true but no orderID".to_string()
                            )));
                        }
                    } else {
                        let err_msg = item["errorMsg"].as_str()
                            .or_else(|| item["error"].as_str())
                            .unwrap_or("Unknown order error");
                        all_results.push(Err(ExecutionError::ApiError(err_msg.to_string())));
                    }
                }
            } else {
                // Non-array response — single error for entire batch
                let err = result["errorMsg"].as_str()
                    .or_else(|| result["error"].as_str())
                    .unwrap_or("Unknown batch error");
                for _ in 0..order_payloads.len() {
                    all_results.push(Err(ExecutionError::ApiError(err.to_string())));
                }
            }
        }

        all_results
    }

    /// Get a reference to the order signer for direct batch signing.
    pub fn signer(&self) -> &OrderSigner {
        &self.signer
    }

    /// Get signature cache statistics (hits, misses, size) for metrics reporting
    pub fn signer_cache_stats(&self) -> (u64, u64, usize) {
        self.signer.cache_stats()
    }

    /// Set maximum capital to commit (in USDC). Called at startup from CLOB balance.
    pub fn set_max_capital_usdc(&self, usdc: f64) {
        let micro = (usdc * 1_000_000.0) as u64;
        self.max_capital_micro.store(micro, AtomicOrdering::Relaxed);
        info!("Capital limit set to ${:.2}", usdc);
    }

    /// Get current committed capital in USDC
    pub fn committed_capital_usdc(&self) -> f64 {
        self.committed_capital_micro.load(AtomicOrdering::Relaxed) as f64 / 1e6
    }

    /// Get max capital in USDC
    pub fn max_capital_usdc(&self) -> f64 {
        self.max_capital_micro.load(AtomicOrdering::Relaxed) as f64 / 1e6
    }

    /// Cancel an order.
    /// Matches official SDK: DELETE /order with body {"orderID": order_id}
    pub async fn cancel_order(&self, order_id: &str) -> Result<(), ExecutionError> {
        let path = "/order";
        let url = format!("{}{}", self.config.polymarket.rest_url, path);

        let body = serde_json::json!({"orderID": order_id});
        let body_str = serde_json::to_string(&body)
            .map_err(|e| ExecutionError::SigningFailed(e.to_string()))?;

        let headers = self.l2_headers("DELETE", path, &body_str)?;

        let mut request = self.client.delete(&url)
            .header("Content-Type", "application/json");
        for (key, value) in &headers {
            request = request.header(key.as_str(), value.as_str());
        }

        let response = request
            .body(body_str)
            .send()
            .await
            .map_err(|e| ExecutionError::NetworkError(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ExecutionError::ApiError(format!("{}: {}", status, body)));
        }

        if let Some(removed) = self.pending_orders.lock().remove(order_id) {
            if removed.cost_micro > 0 {
                self.committed_capital_micro.fetch_sub(removed.cost_micro.min(
                    self.committed_capital_micro.load(AtomicOrdering::Relaxed)
                ), AtomicOrdering::Relaxed);
            }
        }

        info!("Order cancelled: {}", order_id);
        Ok(())
    }

    /// Cancel all open orders.
    /// Matches official SDK: DELETE /cancel-all
    pub async fn cancel_all(&self) -> Result<u32, ExecutionError> {
        let path = "/cancel-all";
        let url = format!("{}{}", self.config.polymarket.rest_url, path);

        let headers = self.l2_headers("DELETE", path, "")?;

        let mut request = self.client.delete(&url);
        for (key, value) in &headers {
            request = request.header(key.as_str(), value.as_str());
        }

        let response = request
            .send()
            .await
            .map_err(|e| ExecutionError::NetworkError(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ExecutionError::ApiError(format!("{}: {}", status, body)));
        }

        let mut pending = self.pending_orders.lock();
        let count = pending.len() as u32;
        let total_released: u64 = pending.values().map(|p| p.cost_micro).sum();
        pending.clear();
        drop(pending);
        if total_released > 0 {
            let current = self.committed_capital_micro.load(AtomicOrdering::Relaxed);
            self.committed_capital_micro.store(
                current.saturating_sub(total_released), AtomicOrdering::Relaxed
            );
        }

        warn!("Cancelled all {} orders", count);
        Ok(count)
    }
    
    /// Process order update from WebSocket user channel.
    ///
    /// Polymarket fires BOTH "order UPDATE" and "trade MATCHED" for each fill.
    /// We handle fills via UPDATE events (which have cumulative size_matched) and
    /// skip MATCHED events if the order is still tracked (to avoid double-counting).
    ///
    /// Only removes orders from pending_orders for terminal states so partial
    /// fills are handled correctly across multiple updates.
    pub async fn process_order_update(
        &self,
        order_id: &str,
        status: &str,
        filled_size: Option<&str>,
    ) {
        let now_ns = Self::now_ns();

        let cumulative_filled = filled_size
            .and_then(|s| Decimal::from_str_exact(s).ok())
            .unwrap_or(Decimal::ZERO);

        // Determine if this is a terminal state
        let is_terminal = matches!(status, "MATCHED" | "FILLED" | "CANCELLED" | "REJECTED" | "EXPIRED");

        // For "OPEN" (placement confirmation), just log and return — no fill to record.
        if status == "OPEN" {
            debug!("Order {} placement confirmed", order_id);
            return;
        }

        // For "MATCHED" from trade events: if order is still in pending, skip.
        // The corresponding "PARTIALLY_FILLED" or terminal UPDATE event handles it.
        // This prevents double-counting fills.
        if status == "MATCHED" {
            let has_pending = self.pending_orders.lock().contains_key(order_id);
            if has_pending {
                debug!("Skipping trade MATCHED for {} — handled via order UPDATE", order_id);
                return;
            }
        }

        let order_status = match status {
            "MATCHED" | "FILLED" => {
                self.orders_filled.fetch_add(1, AtomicOrdering::Relaxed);
                OrderStatus::Filled
            }
            "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
            "CANCELLED" => OrderStatus::Cancelled,
            "REJECTED" => {
                self.orders_rejected.fetch_add(1, AtomicOrdering::Relaxed);
                OrderStatus::Rejected
            }
            "EXPIRED" => OrderStatus::Expired,
            _ => OrderStatus::Open,
        };

        // Extract or peek at the pending order.
        // Only REMOVE for terminal states; keep for partials so future updates work.
        let pending_info = {
            let mut pending = self.pending_orders.lock();
            if is_terminal {
                let removed = pending.remove(order_id);
                // Release committed capital for terminal orders
                if let Some(ref p) = removed {
                    if p.cost_micro > 0 {
                        let current = self.committed_capital_micro.load(AtomicOrdering::Relaxed);
                        self.committed_capital_micro.store(
                            current.saturating_sub(p.cost_micro), AtomicOrdering::Relaxed
                        );
                    }
                }
                removed
            } else {
                // Peek: clone the info we need but leave the entry
                pending.get(order_id).cloned()
            }
        };

        // Build execution report
        let report = if let Some(ref pending) = pending_info {
            // Compute fill delta: cumulative_filled (from WS) minus what we've already reported
            let fill_delta = (cumulative_filled - pending.cumulative_filled).max(Decimal::ZERO);

            // Update the cumulative tracker for partial fills (non-terminal)
            if !is_terminal && fill_delta > Decimal::ZERO {
                let mut pending_map = self.pending_orders.lock();
                if let Some(p) = pending_map.get_mut(order_id) {
                    p.cumulative_filled = cumulative_filled;
                }
            }

            let latency_ms = now_ns.saturating_sub(pending.sent_at_ns) / 1_000_000;
            info!(
                "Order {}: {} (fill_delta: {}, cumulative: {}, latency: {}ms)",
                order_id, status, fill_delta, cumulative_filled, latency_ms
            );

            ExecutionReport {
                order_id: order_id.to_string(),
                market_id: pending.order.market_id,
                side: pending.order.side,
                price: pending.order.price,
                original_size: pending.order.size,
                filled_size: fill_delta,
                status: order_status,
                submitted_at_ns: pending.sent_at_ns,
                last_update_ns: now_ns,
            }
        } else {
            // Unknown order — from a previous session or external tools.
            warn!(
                "Order update for untracked order {}: {} (filled: {})",
                order_id, status, cumulative_filled
            );
            ExecutionReport {
                order_id: order_id.to_string(),
                market_id: crate::types::MarketId::new([0u8; 32], 0),
                side: Side::Buy,
                price: Decimal::ZERO,
                original_size: Decimal::ZERO,
                filled_size: cumulative_filled,
                status: order_status,
                submitted_at_ns: now_ns,
                last_update_ns: now_ns,
            }
        };

        // Send execution report — the report handler is the single authoritative place
        // for recording fills into both inventory tracker and risk manager.
        // Do NOT call risk_manager.record_trade() here to avoid double-counting.
        let _ = self.report_tx.send(report).await;
    }
    
    /// Get current nanosecond timestamp
    #[inline(always)]
    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
    
    /// Round a price to the nearest valid tick_size.
    /// Matches Python SDK's `round_normal(price, round_config.price)`.
    /// E.g., tick_size=0.01 → round to 2 decimals; tick_size=0.001 → 3 decimals.
    #[inline]
    fn round_to_tick_size(price: Decimal, tick_size: Decimal) -> Decimal {
        if tick_size.is_zero() {
            return price;
        }
        // Compute decimal places from tick_size scale
        // Decimal::new(1, 2) = 0.01 → scale()=2, Decimal::new(1, 3) = 0.001 → scale()=3
        let dp = tick_size.scale();
        price.round_dp(dp)
    }

    /// Update rolling average latency
    fn update_avg_latency(&self, new_latency_ns: u64) {
        // Simple exponential moving average
        let current = self.avg_latency_ns.load(AtomicOrdering::Relaxed);
        if current == 0 {
            self.avg_latency_ns.store(new_latency_ns, AtomicOrdering::Relaxed);
        } else {
            // EMA with alpha = 0.1
            let new_avg = (current * 9 + new_latency_ns) / 10;
            self.avg_latency_ns.store(new_avg, AtomicOrdering::Relaxed);
        }
    }
    
    /// Fetch market info (fee_rate_bps, neg_risk, tick_size) from Polymarket API
    /// for a given token_id string and register it in the token registry.
    /// Should be called once per token when first subscribing.
    pub async fn fetch_and_register_market_info(
        &self,
        token_id_str: &str,
        hash: u64,
    ) -> Result<TokenMarketInfo, ExecutionError> {
        let base_url = &self.config.polymarket.rest_url;

        // Fetch neg_risk
        let neg_risk_url = format!("{}/neg-risk?token_id={}", base_url, token_id_str);
        let neg_risk = match self.client.get(&neg_risk_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let val: serde_json::Value = resp.json().await.unwrap_or_default();
                val["neg_risk"].as_bool().unwrap_or(false)
            }
            _ => false,
        };

        // Fetch fee_rate_bps
        let fee_url = format!("{}/fee-rate?token_id={}", base_url, token_id_str);
        let fee_rate_bps = match self.client.get(&fee_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let val: serde_json::Value = resp.json().await.unwrap_or_default();
                debug!("Fee API raw response for {}...{}: {}",
                       &token_id_str[..8.min(token_id_str.len())],
                       &token_id_str[token_id_str.len().saturating_sub(4)..],
                       val);
                // Try "fee_rate_bps" field (string or number)
                let bps = val["fee_rate_bps"].as_str()
                    .and_then(|s| s.parse::<u32>().ok())
                    .or_else(|| val["fee_rate_bps"].as_u64().map(|v| v as u32))
                    // Also try "maker" / "taker" format (some API versions)
                    .or_else(|| val["taker"].as_str().and_then(|s| {
                        // Could be decimal like "0.02" (= 200 bps) or bps like "200"
                        s.parse::<f64>().ok().map(|v| {
                            if v < 1.0 { (v * 10000.0) as u32 } else { v as u32 }
                        })
                    }))
                    .unwrap_or(0);
                bps
            }
            Ok(resp) => {
                warn!("Fee API returned status {} for {}...", resp.status(),
                      &token_id_str[..16.min(token_id_str.len())]);
                0
            }
            Err(e) => {
                warn!("Fee API request failed for {}...: {}", &token_id_str[..16.min(token_id_str.len())], e);
                0
            }
        };

        // Fetch tick_size
        let tick_url = format!("{}/tick-size?token_id={}", base_url, token_id_str);
        let tick_size = match self.client.get(&tick_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let val: serde_json::Value = resp.json().await.unwrap_or_default();
                val["minimum_tick_size"].as_str()
                    .and_then(|s| s.parse::<Decimal>().ok())
                    .unwrap_or(Decimal::new(1, 2)) // default 0.01
            }
            _ => Decimal::new(1, 2),
        };

        let info = TokenMarketInfo {
            asset_id: token_id_str.to_string(),
            neg_risk,
            fee_rate_bps,
            tick_size,
        };

        self.token_registry.register_full(hash, info.clone());
        info!("Registered market info for {}: neg_risk={}, fee={}bps, tick={}",
              token_id_str, neg_risk, fee_rate_bps, tick_size);

        Ok(info)
    }

    /// Check USDC (collateral) allowance for the exchange contract on-chain.
    /// Returns the allowance in raw units (6 decimals for USDC).
    /// This is a cold-path startup check, not called during hot trading.
    pub async fn check_collateral_allowance(&self) -> Result<u128, ExecutionError> {
        use ethers::prelude::*;
        use ethers::abi::Token;

        let rpc_url = &self.config.polymarket.polygon_rpc;
        let provider = Provider::<Http>::try_from(rpc_url.as_str())
            .map_err(|e| ExecutionError::NetworkError(format!("RPC provider: {}", e)))?;

        // USDC on Polygon
        let usdc_addr: Address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
            .parse()
            .map_err(|e| ExecutionError::SigningFailed(format!("Bad USDC addr: {}", e)))?;

        let exchange_addr: Address = self.config.polymarket.clob_address
            .parse()
            .map_err(|e| ExecutionError::SigningFailed(format!("Bad exchange addr: {}", e)))?;

        let owner_addr = self.signer.address();
        let owner: Address = Address::from_slice(owner_addr.as_slice());

        // ERC20 allowance(address,address) selector = 0xdd62ed3e
        let call_data = ethers::abi::encode(&[
            Token::Address(owner),
            Token::Address(exchange_addr),
        ]);
        let mut full_data = vec![0xdd, 0x62, 0xed, 0x3e]; // allowance selector
        full_data.extend_from_slice(&call_data);

        let tx = TransactionRequest::new()
            .to(usdc_addr)
            .data(full_data);

        let result = provider.call(&tx.into(), None).await
            .map_err(|e| ExecutionError::NetworkError(format!("allowance call: {}", e)))?;

        // Decode uint256 result
        let allowance = if result.len() >= 32 {
            let mut bytes = [0u8; 16];
            // Take last 16 bytes (u128 is enough for USDC amounts)
            bytes.copy_from_slice(&result[16..32]);
            u128::from_be_bytes(bytes)
        } else {
            0
        };

        let usdc_amount = allowance / 1_000_000; // Convert to whole USDC
        if usdc_amount < 100 {
            warn!("Low USDC allowance: {} USDC — orders may fail. Approve the exchange contract.", usdc_amount);
        } else {
            info!("USDC allowance: {} USDC for exchange {}", usdc_amount, self.config.polymarket.clob_address);
        }

        Ok(allowance)
    }

    /// Check and set conditional token (CTF) approvals for both exchange contracts.
    /// Required for SELL orders — the exchange needs permission to transfer your tokens.
    /// Calls `setApprovalForAll` on the CTF ERC-1155 contract for each exchange.
    pub async fn ensure_token_approvals(&self) -> Result<(), ExecutionError> {
        use ethers::prelude::*;
        use ethers::abi::Token;

        let rpc_url = &self.config.polymarket.polygon_rpc;
        let provider = Provider::<Http>::try_from(rpc_url.as_str())
            .map_err(|e| ExecutionError::NetworkError(format!("RPC provider: {}", e)))?;

        // CTF (Conditional Token Framework) contract on Polygon
        let ctf_addr: Address = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
            .parse()
            .map_err(|e| ExecutionError::SigningFailed(format!("Bad CTF addr: {}", e)))?;

        let owner_addr = self.signer.address();
        let owner: Address = Address::from_slice(owner_addr.as_slice());

        // Exchange contracts that need approval
        let operators: Vec<(&str, Address)> = vec![
            ("CTF Exchange", self.config.polymarket.clob_address.parse()
                .map_err(|e| ExecutionError::SigningFailed(format!("Bad clob addr: {}", e)))?),
            ("NegRisk Exchange", self.config.polymarket.neg_risk_clob_address.parse()
                .map_err(|e| ExecutionError::SigningFailed(format!("Bad neg_risk addr: {}", e)))?),
            // NegRisk Adapter — required for neg_risk market sells
            ("NegRisk Adapter", "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296".parse()
                .map_err(|e| ExecutionError::SigningFailed(format!("Bad adapter addr: {}", e)))?),
        ];

        for (i, (name, operator)) in operators.iter().enumerate() {
            // Rate limit: wait 3s between RPC calls (free endpoints throttle aggressively)
            if i > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }

            // Check isApprovedForAll(owner, operator) — selector 0xe985e9c5
            let check_data = ethers::abi::encode(&[
                Token::Address(owner),
                Token::Address(*operator),
            ]);
            let mut check_call = vec![0xe9, 0x85, 0xe9, 0xc5];
            check_call.extend_from_slice(&check_data);

            let tx = TransactionRequest::new()
                .to(ctf_addr)
                .data(check_call);

            let result = match provider.call(&tx.into(), None).await {
                Ok(r) => r,
                Err(e) => {
                    warn!("⚠️  Could not check approval for {}: {} (will retry next restart)", name, e);
                    continue;
                }
            };

            // Decode bool result (last byte of 32-byte word)
            let is_approved = result.len() >= 32 && result[31] == 1;

            if is_approved {
                info!("✅ CTF approved for {} ({:?})", name, operator);
            } else {
                warn!("🔓 CTF NOT approved for {} — sending setApprovalForAll tx...", name);

                // Build setApprovalForAll(operator, true) — selector 0xa22cb465
                let approve_data = ethers::abi::encode(&[
                    Token::Address(*operator),
                    Token::Bool(true),
                ]);
                let mut approve_call = vec![0xa2, 0x2c, 0xb4, 0x65];
                approve_call.extend_from_slice(&approve_data);

                // Sign and send the transaction
                let chain_id: u64 = 137; // Polygon mainnet
                let wallet: LocalWallet = self.config.polymarket.private_key.parse::<LocalWallet>()
                    .map_err(|e| ExecutionError::SigningFailed(format!("wallet: {}", e)))?
                    .with_chain_id(chain_id);

                let client = SignerMiddleware::new(provider.clone(), wallet);

                let tx = TransactionRequest::new()
                    .to(ctf_addr)
                    .data(approve_call)
                    .gas(100_000u64) // setApprovalForAll is cheap
                    .chain_id(chain_id);

                // Wait a bit before sending tx to avoid nonce issues after RPC rate limit
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                let send_result = client.send_transaction(tx, None).await;
                match send_result {
                    Ok(pending) => {
                        let tx_hash = pending.tx_hash();
                        info!("📝 Approval tx sent for {}: {:?}", name, tx_hash);
                        let receipt_result = pending.await;
                        match receipt_result {
                            Ok(Some(receipt)) => {
                                info!("✅ Approval confirmed for {} in block {:?} (gas: {:?})",
                                      name, receipt.block_number, receipt.gas_used);
                            }
                            Ok(None) => warn!("⚠️  Approval tx for {} dropped", name),
                            Err(e) => warn!("⚠️  Approval tx for {} failed: {}", name, e),
                        }
                    }
                    Err(e) => {
                        warn!("⚠️  Could not send approval for {}: {}", name, e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Notify the CLOB server to refresh its view of on-chain token allowances.
    /// Must be called after setting on-chain approvals so the CLOB knows about them.
    /// Uses GET with query params per official Python SDK: /balance-allowance/update?asset_type=X&signature_type=0
    /// HMAC signs only the base path (no query params), matching py-clob-client behavior.
    /// For CONDITIONAL tokens, must pass each token_id individually (ERC-1155 requirement).
    pub async fn update_clob_balance_allowance(&self, conditional_token_ids: &[String]) -> Result<(), ExecutionError> {
        let base_path = "/balance-allowance/update";

        // 1. Update COLLATERAL (USDC) — no token_id needed
        {
            let headers = self.l2_headers("GET", base_path, "")?;
            let url = format!("{}{}?asset_type=COLLATERAL&signature_type=0", self.config.polymarket.rest_url, base_path);
            let mut request = self.client.get(&url);
            for (key, value) in &headers {
                request = request.header(key.as_str(), value.as_str());
            }
            match request.send().await {
                Ok(resp) if resp.status().is_success() => {
                    info!("✅ CLOB balance/allowance updated for COLLATERAL");
                }
                Ok(resp) => {
                    let text = resp.text().await.unwrap_or_default();
                    warn!("⚠️  CLOB balance-allowance/update (COLLATERAL): {}", &text[..text.len().min(200)]);
                }
                Err(e) => warn!("⚠️  CLOB balance-allowance/update (COLLATERAL) failed: {}", e),
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        // 2. Update CONDITIONAL — one call per token_id (ERC-1155)
        for token_id in conditional_token_ids {
            let headers = self.l2_headers("GET", base_path, "")?;
            let url = format!("{}{}?asset_type=CONDITIONAL&token_id={}&signature_type=0",
                self.config.polymarket.rest_url, base_path, token_id);
            let mut request = self.client.get(&url);
            for (key, value) in &headers {
                request = request.header(key.as_str(), value.as_str());
            }
            match request.send().await {
                Ok(resp) if resp.status().is_success() => {
                    info!("✅ CLOB allowance updated for token {}...{}", &token_id[..token_id.len().min(8)], &token_id[token_id.len().saturating_sub(6)..]);
                }
                Ok(resp) => {
                    let text = resp.text().await.unwrap_or_default();
                    warn!("⚠️  CLOB allowance update (token {}): {}", &token_id[..token_id.len().min(12)], &text[..text.len().min(200)]);
                }
                Err(e) => warn!("⚠️  CLOB allowance update failed: {}", e),
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        Ok(())
    }

    /// Check USDC.e balance on-chain and set capital limit accordingly.
    /// Returns balance in raw units (6 decimals).
    pub async fn check_and_set_capital_from_balance(&self) -> Result<u128, ExecutionError> {
        use ethers::prelude::*;
        use ethers::abi::Token;

        let rpc_url = &self.config.polymarket.polygon_rpc;
        let provider = Provider::<Http>::try_from(rpc_url.as_str())
            .map_err(|e| ExecutionError::NetworkError(format!("RPC provider: {}", e)))?;

        let usdc_addr: Address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
            .parse()
            .map_err(|e| ExecutionError::SigningFailed(format!("Bad USDC addr: {}", e)))?;

        // Use proxy wallet address if configured (it holds the USDC), else fall back to EOA
        let owner: Address = if !self.config.polymarket.proxy_address.is_empty() {
            self.config.polymarket.proxy_address.parse()
                .map_err(|e| ExecutionError::SigningFailed(format!("Bad proxy addr: {}", e)))?
        } else {
            let eoa = self.signer.address();
            Address::from_slice(eoa.as_slice())
        };

        // ERC20 balanceOf(address) selector = 0x70a08231
        let call_data = ethers::abi::encode(&[Token::Address(owner)]);
        let mut full_data = vec![0x70, 0xa0, 0x82, 0x31];
        full_data.extend_from_slice(&call_data);

        let tx = TransactionRequest::new()
            .to(usdc_addr)
            .data(full_data);

        let result = provider.call(&tx.into(), None).await
            .map_err(|e| ExecutionError::NetworkError(format!("balanceOf call: {}", e)))?;

        let balance = if result.len() >= 32 {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&result[16..32]);
            u128::from_be_bytes(bytes)
        } else {
            0
        };

        let usdc_balance = balance as f64 / 1_000_000.0;
        // Set capital limit to 80% of balance (keep 20% buffer for fees/slippage)
        let capital_limit = usdc_balance * 0.80;
        self.set_max_capital_usdc(capital_limit);
        info!("💰 USDC.e balance: ${:.2}, capital limit: ${:.2}", usdc_balance, capital_limit);

        Ok(balance)
    }

    /// Fetch positions from Polymarket APIs to bootstrap inventory.
    /// Tries two sources: (1) data-api /positions (current holdings), (2) CLOB /trades (fallback).
    /// Returns the number of positions bootstrapped.
    pub async fn bootstrap_positions(&self, inventory: &crate::pricing::InventoryTracker) -> Result<(usize, Vec<String>), ExecutionError> {
        let mut count = 0usize;
        let mut asset_ids = Vec::new();

        // Use a separate client with longer timeout for bootstrap (not HFT-critical)
        let bootstrap_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| self.client.clone());

        // 1. Try data-api /positions endpoint (gives current token holdings directly)
        let signer_addr = &self.cached_address;
        let positions_url = format!(
            "https://data-api.polymarket.com/positions?user={}&sizeThreshold=0.1",
            signer_addr
        );
        info!("📦 Bootstrap: querying positions for {}", signer_addr);

        match bootstrap_client.get(&positions_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                // Response could be array or object — parse as Value first
                match resp.json::<serde_json::Value>().await {
                    Ok(val) => {
                        let positions = if let Some(arr) = val.as_array() {
                            arr.clone()
                        } else if let Some(arr) = val.get("data").and_then(|d| d.as_array()) {
                            arr.clone()
                        } else {
                            warn!("📦 Data-API unexpected format: {}", &val.to_string()[..val.to_string().len().min(300)]);
                            vec![]
                        };
                        info!("📦 Data-API returned {} position entries", positions.len());
                        for pos in &positions {
                            let asset_id = pos["asset"].as_str().unwrap_or("");
                            let size = pos["size"].as_f64().unwrap_or(0.0);
                            let avg_price = pos["avgPrice"].as_f64().unwrap_or(0.0);

                            if size > 0.0 && !asset_id.is_empty() {
                                let token_id = crate::websocket::hash_asset_id(asset_id);
                                let size_dec = Decimal::from_str_exact(&format!("{:.6}", size))
                                    .unwrap_or(Decimal::ZERO);
                                let price_dec = Decimal::from_str_exact(&format!("{:.4}", avg_price))
                                    .unwrap_or(Decimal::ZERO);
                                inventory.record_fill(token_id, Side::Buy, size_dec, price_dec);
                                asset_ids.push(asset_id.to_string());
                                info!("  📌 Position: {} tokens @ ${:.4} (asset: {}...)",
                                      size, avg_price, &asset_id[..asset_id.len().min(20)]);
                                count += 1;
                            }
                        }
                        if count > 0 {
                            info!("📦 Bootstrapped {} positions from data-api", count);
                            return Ok((count, asset_ids));
                        }
                        info!("📦 Data-API returned no open positions for this address");
                    }
                    Err(e) => warn!("📦 Failed to parse positions response: {}", e),
                }
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                warn!("📦 Data-API positions failed: {} - {}", status, &body[..body.len().min(200)]);
            }
            Err(e) => warn!("📦 Data-API positions error: {}", e),
        }

        // 2. Fallback: try CLOB /trades endpoint with L2 auth
        let base_url = &self.config.polymarket.rest_url;
        let path = "/trades";
        let body_str = "";
        let headers = self.l2_headers("GET", path, body_str)?;
        let url = format!("{}{}", base_url, path);

        let mut request = bootstrap_client.get(&url);
        for (key, value) in &headers {
            request = request.header(key.as_str(), value.as_str());
        }

        match request.send().await {
            Ok(resp) if resp.status().is_success() => {
                // Response can be array or {"data": [...]} — parse as Value first
                match resp.json::<serde_json::Value>().await {
                    Ok(val) => {
                        let trades = if let Some(arr) = val.as_array() {
                            arr.clone()
                        } else if let Some(arr) = val.get("data").and_then(|d| d.as_array()) {
                            arr.clone()
                        } else {
                            warn!("📦 CLOB /trades unexpected format: {}", &val.to_string()[..val.to_string().len().min(300)]);
                            vec![]
                        };
                        info!("📦 CLOB /trades returned {} entries", trades.len());
                        for trade in &trades {
                            let side_str = trade["side"].as_str().unwrap_or("");
                            let size_str = trade["size"].as_str().unwrap_or("0");
                            let price_str = trade["price"].as_str().unwrap_or("0");
                            let asset_id = trade["asset_id"].as_str().unwrap_or("");

                            let side = match side_str {
                                "BUY" => Side::Buy,
                                "SELL" => Side::Sell,
                                _ => continue,
                            };
                            let size = Decimal::from_str_exact(size_str).unwrap_or(Decimal::ZERO);
                            let price = Decimal::from_str_exact(price_str).unwrap_or(Decimal::ZERO);

                            if size > Decimal::ZERO && price > Decimal::ZERO && !asset_id.is_empty() {
                                let token_id = crate::websocket::hash_asset_id(asset_id);
                                inventory.record_fill(token_id, side, size, price);
                                if side == Side::Buy {
                                    asset_ids.push(asset_id.to_string());
                                }
                                info!("  📌 Trade: {} {} @ ${} (asset: {}...)",
                                      side_str, size, price, &asset_id[..asset_id.len().min(20)]);
                                count += 1;
                            }
                        }
                        info!("📦 Bootstrapped {} positions from {} CLOB trades", count, trades.len());
                    }
                    Err(e) => warn!("📦 Failed to parse trades response: {}", e),
                }
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                warn!("📦 CLOB /trades failed: {} - {}", status, &body[..body.len().min(200)]);
            }
            Err(e) => {
                warn!("📦 CLOB /trades error: {}", e);
            }
        }

        Ok((count, asset_ids))
    }

    /// Get execution metrics
    pub fn get_metrics(&self) -> ExecutionMetrics {
        ExecutionMetrics {
            orders_sent: self.orders_sent.load(AtomicOrdering::Relaxed),
            orders_filled: self.orders_filled.load(AtomicOrdering::Relaxed),
            orders_rejected: self.orders_rejected.load(AtomicOrdering::Relaxed),
            avg_latency_us: self.avg_latency_ns.load(AtomicOrdering::Relaxed) / 1000,
            pending_count: self.pending_orders.lock().len(),
        }
    }
}

/// Execution error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum ExecutionError {
    #[error("Kill switch is active")]
    KillSwitchActive,
    
    #[error("Exposure limit exceeded")]
    ExposureLimitExceeded,
    
    #[error("Position limit exceeded")]
    PositionLimitExceeded,
    
    #[error("In cooldown period")]
    InCooldown,
    
    #[error("Stale market data")]
    StaleData,
    
    #[error("Gap protection triggered - trading halted")]
    GapProtectionTriggered,
    
    #[error("Rate limited: too many orders per second")]
    RateLimited,
    
    #[error("Order signing failed: {0}")]
    SigningFailed(String),
    
    #[error("Network error: {0}")]
    NetworkError(String),
    
    #[error("API error: {0}")]
    ApiError(String),
    
    #[error("Parse error: {0}")]
    ParseError(String),
}

/// Execution metrics
#[derive(Debug, Clone)]
pub struct ExecutionMetrics {
    pub orders_sent: u64,
    pub orders_filled: u64,
    pub orders_rejected: u64,
    pub avg_latency_us: u64,
    pub pending_count: usize,
}

/// Maker order manager for limit order placement
pub struct MakerManager {
    executor: Arc<OrderExecutor>,
    /// Active maker orders per market
    active_orders: Mutex<FxHashMap<u64, Vec<String>>>,
}

impl MakerManager {
    pub fn new(executor: Arc<OrderExecutor>) -> Self {
        Self {
            executor,
            active_orders: Mutex::new(FxHashMap::default()),
        }
    }
    
    /// Place maker orders around the mid price
    pub async fn place_maker_orders(
        &self,
        signal: TradeSignal,
        levels: u32,
        spread_bps: u32,
    ) -> Vec<Result<String, ExecutionError>> {
        let mut results = Vec::with_capacity(levels as usize * 2);
        let base_spread = Decimal::new(spread_bps as i64, 4);
        
        for i in 0..levels {
            let offset = base_spread * Decimal::from(i + 1);
            
            // Buy order below mid
            let buy_signal = TradeSignal {
                market_id: signal.market_id,
                side: Side::Buy,
                price: signal.price * (Decimal::ONE - offset),
                size: signal.size,
                order_type: OrderType::Limit,
                expected_profit_bps: signal.expected_profit_bps,
                signal_timestamp_ns: signal.signal_timestamp_ns,
                urgency: signal.urgency,
            };
            
            let buy_result = self.executor.execute_signal(buy_signal).await;
            if let Ok(ref order_id) = buy_result {
                self.track_order(signal.market_id.token_id, order_id.clone());
            }
            results.push(buy_result);
            
            // Sell order above mid
            let sell_signal = TradeSignal {
                market_id: signal.market_id,
                side: Side::Sell,
                price: signal.price * (Decimal::ONE + offset),
                size: signal.size,
                order_type: OrderType::Limit,
                expected_profit_bps: signal.expected_profit_bps,
                signal_timestamp_ns: signal.signal_timestamp_ns,
                urgency: signal.urgency,
            };
            
            let sell_result = self.executor.execute_signal(sell_signal).await;
            if let Ok(ref order_id) = sell_result {
                self.track_order(signal.market_id.token_id, order_id.clone());
            }
            results.push(sell_result);
        }
        
        results
    }
    
    /// Track an active maker order
    fn track_order(&self, token_id: u64, order_id: String) {
        let mut orders = self.active_orders.lock();
        orders.entry(token_id).or_default().push(order_id);
    }
    
    /// Cancel all maker orders for a market
    pub async fn cancel_market_orders(&self, token_id: u64) -> u32 {
        let order_ids = {
            let mut orders = self.active_orders.lock();
            orders.remove(&token_id).unwrap_or_default()
        };
        
        let mut cancelled = 0;
        for order_id in order_ids {
            if self.executor.cancel_order(&order_id).await.is_ok() {
                cancelled += 1;
            }
        }
        
        cancelled
    }
}
