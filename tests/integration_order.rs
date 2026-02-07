//! Integration test: Sign and submit a real order to Polymarket.
//!
//! This test requires real API credentials in environment variables:
//!   POLYMARKET_API_KEY, POLYMARKET_API_SECRET, POLYMARKET_API_PASSPHRASE,
//!   POLYMARKET_PRIVATE_KEY
//!
//! Run with:
//!   cargo test --test integration_order -- --nocapture --ignored
//!
//! The test places a tiny BUY limit order at a very low price (0.01) so it will
//! NOT be filled. It then verifies the API accepted the signature and returned
//! an order ID, and immediately cancels the order.

use polymorph_hft::config::Config;
use polymorph_hft::signing::OrderSigner;
use polymorph_hft::types::{MarketId, Side, OrderType, TimeInForce};
use rust_decimal::Decimal;


/// Helper: build L2 HMAC headers for a request.
/// Mirrors OrderExecutor::l2_headers but standalone for testing.
fn l2_headers(
    config: &Config,
    signer: &OrderSigner,
    method: &str,
    path: &str,
    body: &str,
) -> Vec<(String, String)> {
    use base64::{engine::general_purpose::URL_SAFE, Engine};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let timestamp = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs())
    .to_string();

    let message = format!("{}{}{}{}", timestamp, method, path, body);

    let secret_bytes = URL_SAFE
        .decode(&config.polymarket.api_secret)
        .expect("api_secret must be valid URL-safe base64");

    let mut mac =
        Hmac::<Sha256>::new_from_slice(&secret_bytes).expect("HMAC can take key of any size");
    mac.update(message.as_bytes());
    let sig = URL_SAFE.encode(mac.finalize().into_bytes());

    let address = format!("0x{}", hex::encode(signer.address().as_slice()));

    vec![
        ("POLY_ADDRESS".to_string(), address),
        ("POLY_SIGNATURE".to_string(), sig),
        ("POLY_TIMESTAMP".to_string(), timestamp),
        (
            "POLY_API_KEY".to_string(),
            config.polymarket.api_key.clone(),
        ),
        (
            "POLY_PASSPHRASE".to_string(),
            config.polymarket.api_passphrase.clone(),
        ),
    ]
}

/// Fetch a real active market token ID from Polymarket to use in the test.
async fn fetch_one_active_token(rest_url: &str) -> Option<String> {
    let url = format!("{}/markets?active=true&limit=1", rest_url);
    let resp = reqwest::get(&url).await.ok()?;
    let body: serde_json::Value = resp.json().await.ok()?;

    // Markets endpoint returns an array; each market has "tokens" array
    let market = body.as_array()?.first()?;
    let token = market["tokens"].as_array()?.first()?;
    token["token_id"].as_str().map(|s| s.to_string())
}

#[tokio::test]
#[ignore] // Only run manually with real credentials
async fn test_sign_and_submit_order_to_polymarket() {
    // Load config from .env / environment
    dotenvy::dotenv().ok();
    let config = Config::load_with_defaults();

    // Validate credentials are present
    assert!(
        !config.polymarket.api_key.is_empty(),
        "POLYMARKET_API_KEY must be set"
    );
    assert!(
        !config.polymarket.api_secret.is_empty(),
        "POLYMARKET_API_SECRET must be set"
    );
    assert!(
        !config.polymarket.api_passphrase.is_empty(),
        "POLYMARKET_API_PASSPHRASE must be set"
    );
    assert!(
        !config.polymarket.private_key.is_empty(),
        "POLYMARKET_PRIVATE_KEY must be set"
    );

    println!("✅ Credentials loaded");
    println!("   API Key: {}...", &config.polymarket.api_key[..8.min(config.polymarket.api_key.len())]);
    println!("   REST URL: {}", config.polymarket.rest_url);

    // Create signer from real private key
    let signer = OrderSigner::new(&config).expect("Failed to create OrderSigner");
    let signer_address = format!("0x{}", hex::encode(signer.address().as_slice()));
    println!("   Signer address: {}", signer_address);

    // Fetch a real active market token ID
    let token_id_str = fetch_one_active_token(&config.polymarket.rest_url)
        .await
        .expect("Failed to fetch an active market token ID from Polymarket");
    println!("   Using token ID: {}...{}", &token_id_str[..10], &token_id_str[token_id_str.len()-10..]);

    // Build a tiny BUY order at price 0.01 (minimum) so it won't fill
    let token_id_hash = polymorph_hft::websocket::hash_asset_id(&token_id_str);
    let market_id = MarketId::new([0u8; 32], token_id_hash);

    let price = Decimal::new(1, 2); // 0.01
    let size = Decimal::new(1, 0);  // 1 token ($0.01 notional)

    println!("\n📝 Signing order: BUY {} @ {} (token={}...)", size, price, &token_id_str[..10]);

    let prepared = signer
        .prepare_order_full(
            market_id,
            Side::Buy,
            price,
            size,
            OrderType::Limit,
            TimeInForce::GTC,
            300, // 5 minute expiration
            token_id_str.clone(),
            false, // standard (not neg_risk)
            0,     // fee_rate_bps
        )
        .await
        .expect("Failed to sign order");

    // Verify signature exists
    let sig = prepared.signature.as_ref().expect("Order must have signature");
    let sig_hex = sig.to_hex();
    println!("   Signature: {}...{}", &sig_hex[..10], &sig_hex[sig_hex.len()-10..]);
    println!("   Salt: {}", &prepared.salt[..20.min(prepared.salt.len())]);
    println!("   Maker: {}", prepared.maker);
    println!("   MakerAmount: {}", prepared.maker_amount);
    println!("   TakerAmount: {}", prepared.taker_amount);
    println!("   Expiration: {}", prepared.expiration);
    println!("   v={} (expect 27 or 28)", sig.v);

    // Verify v value
    assert!(
        sig.v == 27 || sig.v == 28,
        "Signature v must be 27 or 28, got {}",
        sig.v
    );

    // Build the API payload (matching Python SDK order_to_json)
    let order_type_str = "GTC";
    let payload = serde_json::json!({
        "order": {
            "salt": prepared.salt,
            "maker": prepared.maker,
            "signer": prepared.signer_addr,
            "taker": prepared.taker,
            "tokenId": token_id_str,
            "makerAmount": prepared.maker_amount,
            "takerAmount": prepared.taker_amount,
            "expiration": prepared.expiration,
            "nonce": prepared.nonce.to_string(),
            "feeRateBps": prepared.fee_rate_bps,
            "side": "BUY",
            "signatureType": prepared.signature_type,
            "signature": sig_hex,
        },
        "owner": config.polymarket.api_key.clone(),
        "orderType": order_type_str,
    });

    let path = "/order";
    let url = format!("{}{}", config.polymarket.rest_url, path);
    let body_str = serde_json::to_string(&payload).unwrap();

    println!("\n🚀 Submitting order to {}", url);
    println!("   Payload size: {} bytes", body_str.len());

    // Compute L2 HMAC headers
    let headers = l2_headers(&config, &signer, "POST", path, &body_str);

    // Submit
    let client = reqwest::Client::new();
    let mut request = client
        .post(&url)
        .header("Content-Type", "application/json");

    for (key, value) in &headers {
        request = request.header(key.as_str(), value.as_str());
    }

    let response = request.body(body_str).send().await.expect("HTTP request failed");

    let status = response.status();
    let response_body = response.text().await.unwrap_or_default();

    println!("\n📬 Response: {} {}", status.as_u16(), status.canonical_reason().unwrap_or(""));
    println!("   Body: {}", &response_body[..response_body.len().min(500)]);

    if status.is_success() {
        let result: serde_json::Value =
            serde_json::from_str(&response_body).expect("Response must be valid JSON");
        let order_id = result["orderID"]
            .as_str()
            .expect("Response must contain orderID");
        println!("\n✅ ORDER ACCEPTED! orderID = {}", order_id);

        // Immediately cancel the order to avoid any risk
        println!("\n🗑️  Cancelling order {}...", order_id);
        let cancel_payload = serde_json::json!({ "orderID": order_id });
        let cancel_body = serde_json::to_string(&cancel_payload).unwrap();
        let cancel_path = "/order";
        let cancel_headers =
            l2_headers(&config, &signer, "DELETE", cancel_path, &cancel_body);

        let mut cancel_req = client
            .delete(&format!("{}{}", config.polymarket.rest_url, cancel_path))
            .header("Content-Type", "application/json");

        for (key, value) in &cancel_headers {
            cancel_req = cancel_req.header(key.as_str(), value.as_str());
        }

        let cancel_resp = cancel_req
            .body(cancel_body)
            .send()
            .await
            .expect("Cancel request failed");

        println!(
            "   Cancel response: {} {}",
            cancel_resp.status().as_u16(),
            cancel_resp.text().await.unwrap_or_default()
        );
    } else {
        // Print detailed error for debugging
        println!("\n❌ ORDER REJECTED: {} - {}", status, response_body);

        // Common error analysis
        if response_body.contains("signature") || response_body.contains("Signature") {
            println!("   → Likely cause: EIP-712 signature mismatch");
            println!("   → Check: domain separator, order typehash, field ordering");
        }
        if response_body.contains("auth") || response_body.contains("Auth") {
            println!("   → Likely cause: L2 HMAC authentication failure");
            println!("   → Check: api_key, api_secret, api_passphrase");
        }
        if response_body.contains("owner") {
            println!("   → Likely cause: owner field mismatch");
            println!("   → owner should be the API key (CLOB proxy address)");
        }

        panic!(
            "Order submission failed with status {}: {}",
            status, response_body
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_signing_deterministic_fields() {
    // Unit-level test: verify that signing produces valid EIP-712 output
    // Uses dummy key, no network calls
    let signer = OrderSigner::new_dummy().expect("Failed to create dummy signer");
    let signer_address = format!("0x{}", hex::encode(signer.address().as_slice()));
    println!("Dummy signer address: {}", signer_address);

    let token_id_str =
        "21742633143463906290569050155826241533067272736897614950488156847949938836455".to_string();
    let token_id_hash = polymorph_hft::websocket::hash_asset_id(&token_id_str);
    let market_id = MarketId::new([0u8; 32], token_id_hash);

    let price = Decimal::new(55, 2); // 0.55
    let size = Decimal::new(100, 0); // 100 tokens

    let order = signer
        .prepare_order_full(
            market_id,
            Side::Buy,
            price,
            size,
            OrderType::Limit,
            TimeInForce::GTC,
            300,
            token_id_str,
            false,
            0,
        )
        .await
        .expect("Signing must succeed");

    // Verify amounts match Python SDK logic:
    // BUY: maker_amount = to_token_decimals(size * price) = int(100 * 0.55 * 1e6) = 55000000
    //       taker_amount = to_token_decimals(size) = int(100 * 1e6) = 100000000
    assert_eq!(order.maker_amount, "55000000", "maker_amount (USDC) must be 55000000");
    assert_eq!(order.taker_amount, "100000000", "taker_amount (tokens) must be 100000000");

    // Verify signature components
    let sig = order.signature.as_ref().expect("Must have signature");
    assert!(sig.v == 27 || sig.v == 28, "v must be 27 or 28");
    assert_ne!(sig.r, [0u8; 32], "r must not be zero");
    assert_ne!(sig.s, [0u8; 32], "s must not be zero");

    // Verify maker/signer addresses match
    assert_eq!(order.maker, order.signer_addr, "maker and signer must match for EOA");
    assert_eq!(order.taker, "0x0000000000000000000000000000000000000000");
    assert_eq!(order.signature_type, 0, "EOA signature type");

    // Verify salt is non-zero (random)
    assert_ne!(order.salt, "0", "salt must be random, not zero");

    // Verify expiration is in the future
    let exp: u64 = order.expiration.parse().unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(exp > now, "expiration must be in the future");
    assert!(exp <= now + 600, "expiration must be within 10 minutes");

    println!("✅ All signing assertions passed");
    println!("   maker_amount: {}", order.maker_amount);
    println!("   taker_amount: {}", order.taker_amount);
    println!("   salt: {}...", &order.salt[..20.min(order.salt.len())]);
    println!("   sig: {}", sig.to_hex());
    println!("   v: {}", sig.v);
}
