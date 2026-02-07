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
use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering as AtomicOrdering};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

type HmacSha256 = Hmac<Sha256>;

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
}

#[derive(Debug, Clone)]
struct PendingOrder {
    order: PreparedOrder,
    sent_at_ns: u64,
    expected_profit_bps: i32,
    /// Cumulative filled amount reported so far (for computing deltas on partial fills)
    cumulative_filled: Decimal,
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
            .pool_max_idle_per_host(10)
            .tcp_nodelay(true)
            .user_agent("polymorph-hft/1.0");

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
        
        // 4. Submit order via REST API
        let order_id = self.submit_order(&order).await?;
        
        // 5. Track pending order (use TSC as timestamp - converted later if needed)
        {
            let mut pending = self.pending_orders.lock();
            pending.insert(order_id.clone(), PendingOrder {
                order,
                sent_at_ns: Self::now_ns(),
                expected_profit_bps: signal.expected_profit_bps,
                cumulative_filled: Decimal::ZERO,
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
            OrderType::ImmediateOrCancel => "FOK", // Polymarket uses FOK for IOC-like
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
            error!("Order submission failed: {} - {}", status, body);
            return Err(ExecutionError::ApiError(format!("{}: {}", status, body)));
        }

        let result: serde_json::Value = response.json().await
            .map_err(|e| ExecutionError::ParseError(e.to_string()))?;

        let order_id = result["orderID"]
            .as_str()
            .ok_or(ExecutionError::ParseError("Missing orderID".to_string()))?
            .to_string();

        Ok(order_id)
    }
    
    /// Get signature cache statistics (hits, misses, size) for metrics reporting
    pub fn signer_cache_stats(&self) -> (u64, u64, usize) {
        self.signer.cache_stats()
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

        self.pending_orders.lock().remove(order_id);

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

        let count = self.pending_orders.lock().len() as u32;
        self.pending_orders.lock().clear();

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
                pending.remove(order_id)
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

        // Record trade in risk manager — but only for known orders with valid price.
        // Unknown orders have market_id=0 and price=0 which would corrupt PnL tracking.
        if report.market_id.token_id != 0 && report.price > Decimal::ZERO {
            self.risk_manager.record_trade(&report);
        }

        // Send execution report (report handler also guards against corrupting inventory)
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
                // API returns fee_rate as a string like "150" (basis points)
                val["fee_rate_bps"].as_str()
                    .or(val["fee_rate_bps"].as_u64().map(|_| "").or(None))
                    .and_then(|s| s.parse::<u32>().ok())
                    .or_else(|| val["fee_rate_bps"].as_u64().map(|v| v as u32))
                    .unwrap_or(0)
            }
            _ => 0,
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
#[derive(Debug, thiserror::Error)]
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
