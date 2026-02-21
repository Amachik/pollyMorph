//! EIP-712 order signing module for Polymarket CLOB
//! Pre-signs orders for sub-5ms submission latency
//!
//! Matches the on-chain Order struct from:
//! https://github.com/Polymarket/ctf-exchange/blob/main/src/exchange/libraries/OrderStructs.sol

use crate::config::Config;
use crate::types::{MarketId, OrderSignature, PreparedOrder, Side, OrderType, TimeInForce, NonceGenerator};
use alloy_primitives::{B256, U256, Address, keccak256};
use dashmap::DashMap;
use ethers::signers::{LocalWallet, Signer};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{info, warn};

// EIP-712 Domain — matches Hashing.sol constructor("Polymarket CTF Exchange", "1")
const DOMAIN_NAME: &str = "Polymarket CTF Exchange";
const DOMAIN_VERSION: &str = "1";
const POLYGON_CHAIN_ID: u64 = 137;

// Contract addresses (Polygon mainnet)
const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
const NEG_RISK_CTF_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

// ORDER_TYPEHASH — must match OrderStructs.sol exactly
// "Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,
//  uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,
//  uint256 feeRateBps,uint8 side,uint8 signatureType)"
const ORDER_TYPEHASH_STR: &[u8] = b"Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)";

/// Zero address constant
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Order signer with pre-computed domain separators and zero-allocation DashMap cache.
/// Maintains TWO domain separators: one for standard CTF Exchange, one for negRisk.
pub struct OrderSigner {
    wallet: LocalWallet,
    /// Domain separator for standard CTF Exchange (0x4bFb...)
    domain_separator_standard: B256,
    /// Domain separator for negRisk CTF Exchange (0xC5d5...)
    domain_separator_neg_risk: B256,
    /// Standard exchange address
    contract_address: Address,
    /// NegRisk exchange address
    neg_risk_contract_address: Address,
    /// Proxy wallet address (funder). When set, uses POLY_PROXY signature type (1).
    /// The proxy wallet holds USDC; the EOA wallet signs orders on its behalf.
    proxy_address: Option<Address>,
    nonce_generator: NonceGenerator,
    /// Zero-allocation signature cache using DashMap for lock-free concurrent access
    signature_cache: Arc<DashMap<SignatureCacheKey, CachedSignature, ahash::RandomState>>,
    /// Cache statistics
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    /// Maximum cache entries
    max_cache_size: usize,
}

/// Zero-copy cache key for pre-signed orders
/// Packed into a single struct for minimal memory footprint
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SignatureCacheKey {
    /// Market token ID
    pub token_id: u64,
    /// Order side (0 = Buy, 1 = Sell)
    pub side: u8,
    /// Price in basis points (0-10000 for 0.00-1.00)
    pub price_bps: u16,
    /// Size in cents (allows up to $655k orders)
    pub size_cents: u32,
}

impl SignatureCacheKey {
    /// Create cache key from order parameters - zero allocation
    #[inline(always)]
    pub fn new(market_id: MarketId, side: Side, price: Decimal, size: Decimal) -> Self {
        Self {
            token_id: market_id.token_id,
            side: side as u8,
            price_bps: (price * Decimal::new(10000, 0)).to_u16().unwrap_or(5000),
            size_cents: (size * Decimal::new(100, 0)).to_u32().unwrap_or(0),
        }
    }
    
    /// Create key from raw values - zero allocation hot path
    #[inline(always)]
    pub const fn from_raw(token_id: u64, side: u8, price_bps: u16, size_cents: u32) -> Self {
        Self { token_id, side, price_bps, size_cents }
    }
}

/// Cached signature with expiration tracking
#[derive(Debug, Clone)]
pub struct CachedSignature {
    pub order: PreparedOrder,
    pub created_at_ns: u64,
    pub expires_at_ns: u64,
}

impl OrderSigner {
    /// Create a new order signer from private key
    pub fn new(config: &Config) -> anyhow::Result<Self> {
        let private_key = &config.polymarket.private_key;
        let wallet: LocalWallet = private_key.parse()?;

        let contract_address: Address = config.polymarket.clob_address.parse()?;
        let neg_risk_contract_address: Address = config.polymarket.neg_risk_clob_address.parse()?;

        let domain_separator_standard = Self::compute_domain_separator(contract_address);
        let domain_separator_neg_risk = Self::compute_domain_separator(neg_risk_contract_address);

        let proxy_address = std::env::var("POLYMARKET_PROXY_ADDRESS")
            .ok()
            .filter(|s| !s.is_empty())
            .and_then(|s| s.parse::<Address>().ok());

        if proxy_address.is_some() {
            info!("🔑 Using POLY_PROXY signature type (proxy wallet as maker)");
        }

        Ok(Self {
            wallet,
            domain_separator_standard,
            domain_separator_neg_risk,
            contract_address,
            neg_risk_contract_address,
            proxy_address,
            nonce_generator: NonceGenerator::new(),
            signature_cache: Arc::new(DashMap::with_hasher(ahash::RandomState::new())),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            max_cache_size: 50000,
        })
    }

    /// Create a dummy signer for Shadow Mode testing (no real private key required)
    pub fn new_dummy() -> anyhow::Result<Self> {
        let test_key = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let wallet: LocalWallet = test_key.parse()?;

        let contract_address: Address = CTF_EXCHANGE.parse()?;
        let neg_risk_contract_address: Address = NEG_RISK_CTF_EXCHANGE.parse()?;

        let domain_separator_standard = Self::compute_domain_separator(contract_address);
        let domain_separator_neg_risk = Self::compute_domain_separator(neg_risk_contract_address);

        info!("🧪 Shadow Mode: Using dummy signer (test key, no real funds)");

        Ok(Self {
            wallet,
            domain_separator_standard,
            domain_separator_neg_risk,
            contract_address,
            neg_risk_contract_address,
            proxy_address: None,
            nonce_generator: NonceGenerator::new(),
            signature_cache: Arc::new(DashMap::with_hasher(ahash::RandomState::new())),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            max_cache_size: 50000,
        })
    }
    
    /// Pre-warm signature cache for price range ±2% from current tick
    /// This is the key to zero-allocation hot path execution
    pub async fn warm_signature_cache(
        &self,
        market_id: MarketId,
        current_price: Decimal,
        size: Decimal,
        range_pct: Decimal,
    ) -> anyhow::Result<u32> {
        let range = current_price * range_pct / Decimal::new(100, 0);
        let min_price = (current_price - range).max(Decimal::new(1, 4)); // Min 0.0001
        let max_price = (current_price + range).min(Decimal::new(9999, 4)); // Max 0.9999
        
        // Pre-sign at 1 basis point intervals within range
        let step = Decimal::new(1, 4); // 0.0001 = 1 bps
        let mut count = 0u32;
        let mut price = min_price;
        
        while price <= max_price {
            // Pre-sign both buy and sell at each price level
            for side in [Side::Buy, Side::Sell] {
                let key = SignatureCacheKey::new(market_id, side, price, size);
                
                // Skip if already cached and not expired
                if let Some(cached) = self.signature_cache.get(&key) {
                    let now_ns = Self::now_ns();
                    if cached.expires_at_ns > now_ns {
                        continue;
                    }
                }
                
                // Sign and cache
                let order = self.prepare_order(
                    market_id,
                    side,
                    price,
                    size,
                    OrderType::Limit,
                    TimeInForce::GTC,
                    3600, // 1 hour expiration
                ).await?;
                
                let now_ns = Self::now_ns();
                self.signature_cache.insert(key, CachedSignature {
                    order,
                    created_at_ns: now_ns,
                    expires_at_ns: now_ns + 3600_000_000_000, // 1 hour
                });
                count += 1;
            }
            
            price += step;
            
            // Enforce cache size limit
            if self.signature_cache.len() >= self.max_cache_size {
                self.evict_expired();
                if self.signature_cache.len() >= self.max_cache_size {
                    warn!("Signature cache at capacity: {}", self.max_cache_size);
                    break;
                }
            }
        }
        
        info!("Pre-signed {} orders for market {} at price {:.4} (±{}%)", 
              count, market_id.token_id, current_price, range_pct);
        Ok(count)
    }
    
    /// Get pre-signed order from cache - ZERO ALLOCATION HOT PATH
    #[inline(always)]
    pub fn get_cached_signature(&self, key: &SignatureCacheKey) -> Option<PreparedOrder> {
        if let Some(cached) = self.signature_cache.get(key) {
            let now_ns = Self::now_ns();
            if cached.expires_at_ns > now_ns {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Some(cached.order.clone());
            }
        }
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        None
    }
    
    /// Evict expired entries from cache
    fn evict_expired(&self) {
        let now_ns = Self::now_ns();
        self.signature_cache.retain(|_, v| v.expires_at_ns > now_ns);
    }
    
    /// Get cache statistics
    pub fn cache_stats(&self) -> (u64, u64, usize) {
        (
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_misses.load(Ordering::Relaxed),
            self.signature_cache.len(),
        )
    }
    
    #[inline(always)]
    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
    
    /// Compute the EIP-712 domain separator.
    /// Matches OpenZeppelin EIP712._domainSeparatorV4() used by Hashing.sol.
    fn compute_domain_separator(contract_address: Address) -> B256 {
        let domain_type_hash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
        );

        let name_hash = keccak256(DOMAIN_NAME.as_bytes());
        let version_hash = keccak256(DOMAIN_VERSION.as_bytes());

        // abi.encode(typeHash, nameHash, versionHash, chainId, verifyingContract)
        let mut encoded = Vec::with_capacity(160);
        encoded.extend_from_slice(domain_type_hash.as_slice());
        encoded.extend_from_slice(name_hash.as_slice());
        encoded.extend_from_slice(version_hash.as_slice());
        encoded.extend_from_slice(&U256::from(POLYGON_CHAIN_ID).to_be_bytes::<32>());
        // address is left-padded to 32 bytes in abi.encode
        encoded.extend_from_slice(&[0u8; 12]);
        encoded.extend_from_slice(contract_address.as_slice());

        keccak256(&encoded)
    }

    /// Get the correct domain separator based on neg_risk flag
    #[inline(always)]
    fn domain_separator_for(&self, neg_risk: bool) -> &B256 {
        if neg_risk {
            &self.domain_separator_neg_risk
        } else {
            &self.domain_separator_standard
        }
    }
    
    /// Get the signer's address
    pub fn address(&self) -> Address {
        Address::from_slice(self.wallet.address().as_bytes())
    }
    
    /// Pre-sign an order for later instant submission
    /// This is the critical path - must be as fast as possible
    #[inline]
    pub async fn prepare_order(
        &self,
        market_id: MarketId,
        side: Side,
        price: Decimal,
        size: Decimal,
        order_type: OrderType,
        time_in_force: TimeInForce,
        expiration_secs: u64,
    ) -> anyhow::Result<PreparedOrder> {
        self.prepare_order_full(market_id, side, price, size, order_type, time_in_force, expiration_secs, String::new(), false, 0).await
    }

    /// Full prepare_order with token_id_str, neg_risk, and fee_rate_bps for API submission.
    /// Generates a random salt, computes the correct EIP-712 hash matching OrderStructs.sol,
    /// signs it, and populates all fields needed for the API payload.
    pub async fn prepare_order_full(
        &self,
        market_id: MarketId,
        side: Side,
        price: Decimal,
        size: Decimal,
        order_type: OrderType,
        time_in_force: TimeInForce,
        expiration_secs: u64,
        token_id_str: String,
        neg_risk: bool,
        fee_rate_bps: u32,
    ) -> anyhow::Result<PreparedOrder> {
        // Polymarket API expects nonce=0 for new orders (salt provides uniqueness)
        let nonce: u64 = 0;
        let created_at_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        // Generate random salt (matches Python SDK: random int that fits in JSON number)
        let salt = Self::generate_salt();
        let salt_u256 = U256::from(salt);

        // Calculate amounts based on side (6-decimal USDC units)
        let (maker_amount_u256, taker_amount_u256) = self.calculate_amounts(side, price, size);

        // Compute expiration: 0 means no expiration, otherwise now + secs
        let expiration = if expiration_secs == 0 {
            0u64
        } else {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() + expiration_secs
        };

        let (maker_addr, signature_type) = if let Some(proxy) = self.proxy_address {
            (proxy, 1u8) // POLY_PROXY: proxy wallet is maker, EOA signs
        } else {
            (self.address(), 0u8) // EOA: signer is also maker
        };
        let signer_addr = self.address();
        let taker_addr = Address::ZERO;

        // Build the EIP-712 struct hash matching OrderStructs.sol
        let order_hash = self.compute_order_hash(
            &salt_u256,
            maker_addr,
            signer_addr,
            taker_addr,
            &token_id_str,
            maker_amount_u256,
            taker_amount_u256,
            expiration,
            nonce,
            fee_rate_bps,
            side,
            signature_type,
            neg_risk,
        );

        // Sign the order hash
        let signature = self.sign_hash(order_hash).await?;

        Ok(PreparedOrder {
            market_id,
            side,
            price,
            size,
            order_type,
            time_in_force,
            nonce,
            signature: Some(signature),
            created_at_ns,
            token_id_str,
            neg_risk,
            salt,
            maker: maker_addr.to_checksum(None),
            signer_addr: signer_addr.to_checksum(None),
            taker: ZERO_ADDRESS.to_string(),
            maker_amount: maker_amount_u256.to_string(),
            taker_amount: taker_amount_u256.to_string(),
            expiration: expiration.to_string(),
            fee_rate_bps: fee_rate_bps.to_string(),
            signature_type,
            post_only: false,
        })
    }
    
    /// Calculate maker and taker amounts matching the Python SDK exactly.
    ///
    /// Python SDK logic (from `order_builder/builder.py` + `helpers.py`):
    ///   `to_token_decimals(x) = int(x * 10^6)`
    ///
    ///   BUY:  taker_amount = to_token_decimals(size)          [conditional tokens we receive]
    ///         maker_amount = to_token_decimals(size * price)   [USDC we pay]
    ///
    ///   SELL: maker_amount = to_token_decimals(size)           [conditional tokens we pay]
    ///         taker_amount = to_token_decimals(size * price)   [USDC we receive]
    ///
    /// Both conditional tokens and USDC use 6 decimals on Polymarket.
    /// `size` is in number of conditional tokens (e.g. 100.0 = 100 tokens).
    /// `price` is the probability price (e.g. 0.55).
    #[inline(always)]
    fn calculate_amounts(&self, side: Side, price: Decimal, size: Decimal) -> (U256, U256) {
        // Polymarket CLOB precision rules (confirmed from API error messages):
        //
        //   BUY:  makerAmount = USDC paid       → max 2 decimal places
        //         takerAmount = tokens received  → max 4 decimal places
        //
        //   SELL: makerAmount = tokens paid      → max 2 decimal places
        //         takerAmount = USDC received    → max 4 decimal places
        let size_2dp = size.round_dp(2);

        match side {
            Side::Buy => {
                // BUY: maker=USDC(2dp), taker=tokens(4dp)
                let usdc_2dp   = (size_2dp * price).round_dp(2);
                let tokens_4dp = size_2dp.round_dp(4);
                (U256::from(Self::to_token_decimals(usdc_2dp)),
                 U256::from(Self::to_token_decimals(tokens_4dp)))
            }
            Side::Sell => {
                // SELL: maker=tokens(2dp), taker=USDC(4dp)
                let tokens_2dp = size_2dp;
                let usdc_4dp   = (size_2dp * price).round_dp(4);
                (U256::from(Self::to_token_decimals(tokens_2dp)),
                 U256::from(Self::to_token_decimals(usdc_4dp)))
            }
        }
    }

    /// Convert a decimal amount to on-chain token units (6 decimals).
    /// Matches Python SDK: `to_token_decimals(x) = int(round(x * 10^6))`
    #[inline(always)]
    fn to_token_decimals(x: Decimal) -> u128 {
        let scaled = x * Decimal::new(1_000_000, 0);
        // Round to nearest integer (matches Python round_normal with 0 decimals)
        let rounded = scaled.round();
        rounded.to_u128().unwrap_or(0)
    }
    
    /// Generate a cryptographically random salt for order uniqueness.
    /// Uses the `rand` crate for proper entropy — two orders in the same nanosecond
    /// will still get different salts.
    fn generate_salt() -> u64 {
        use rand::Rng;
        // Cap at 2^53-1 (JS Number.MAX_SAFE_INTEGER) so the Polymarket server
        // (Node.js) can parse the JSON number without precision loss.
        rand::thread_rng().gen_range(0..=9_007_199_254_740_991u64)
    }

    /// Compute the EIP-712 order hash matching OrderStructs.sol exactly.
    ///
    /// On-chain struct field order:
    /// Order(uint256 salt, address maker, address signer, address taker,
    ///       uint256 tokenId, uint256 makerAmount, uint256 takerAmount,
    ///       uint256 expiration, uint256 nonce, uint256 feeRateBps,
    ///       uint8 side, uint8 signatureType)
    ///
    /// In abi.encode, uint8 values are left-padded to 32 bytes.
    #[inline]
    fn compute_order_hash(
        &self,
        salt: &U256,
        maker: Address,
        signer_addr: Address,
        taker: Address,
        token_id_str: &str,
        maker_amount: U256,
        taker_amount: U256,
        expiration: u64,
        nonce: u64,
        fee_rate_bps: u32,
        side: Side,
        signature_type: u8,
        neg_risk: bool,
    ) -> B256 {
        let order_type_hash = keccak256(ORDER_TYPEHASH_STR);

        // Parse token_id_str to U256 (the real on-chain token ID)
        let token_id_u256 = if token_id_str.is_empty() {
            U256::ZERO
        } else {
            token_id_str.parse::<U256>().unwrap_or(U256::ZERO)
        };

        // abi.encode all fields — addresses are left-padded to 32 bytes,
        // uint8 values are also left-padded to 32 bytes in abi.encode
        // STACK-ALLOCATED: 14 * 32 = 448 bytes (no heap allocation)
        let mut encoded = [0u8; 448];
        let mut pos = 0;
        macro_rules! write32 {
            ($data:expr) => { encoded[pos..pos+32].copy_from_slice(&$data); pos += 32; }
        }
        write32!(order_type_hash.0);                                   // ORDER_TYPEHASH
        write32!(salt.to_be_bytes::<32>());                            // salt
        // maker address: left-pad 12 zeros + 20-byte address
        encoded[pos..pos+12].fill(0); encoded[pos+12..pos+32].copy_from_slice(maker.as_slice()); pos += 32;
        // signer address
        encoded[pos..pos+12].fill(0); encoded[pos+12..pos+32].copy_from_slice(signer_addr.as_slice()); pos += 32;
        // taker address
        encoded[pos..pos+12].fill(0); encoded[pos+12..pos+32].copy_from_slice(taker.as_slice()); pos += 32;
        write32!(token_id_u256.to_be_bytes::<32>());                   // tokenId
        write32!(maker_amount.to_be_bytes::<32>());                    // makerAmount
        write32!(taker_amount.to_be_bytes::<32>());                    // takerAmount
        write32!(U256::from(expiration).to_be_bytes::<32>());          // expiration
        write32!(U256::from(nonce).to_be_bytes::<32>());               // nonce
        write32!(U256::from(fee_rate_bps).to_be_bytes::<32>());        // feeRateBps
        write32!(U256::from(side as u8).to_be_bytes::<32>());          // side (uint8, padded)
        write32!(U256::from(signature_type).to_be_bytes::<32>());      // signatureType (uint8, padded)

        let struct_hash = keccak256(&encoded[..pos]);

        // EIP-712 final hash: keccak256("\x19\x01" || domainSeparator || structHash)
        // STACK-ALLOCATED: 2 + 32 + 32 = 66 bytes
        let domain_sep = self.domain_separator_for(neg_risk);
        let mut final_buf = [0u8; 66];
        final_buf[0] = 0x19;
        final_buf[1] = 0x01;
        final_buf[2..34].copy_from_slice(domain_sep.as_slice());
        final_buf[34..66].copy_from_slice(struct_hash.as_slice());

        keccak256(&final_buf)
    }
    
    /// Sign a hash using the wallet
    #[inline]
    async fn sign_hash(&self, hash: B256) -> anyhow::Result<OrderSignature> {
        let hash_bytes: [u8; 32] = hash.into();
        let signature = self.wallet.sign_hash(ethers::types::H256::from(hash_bytes))?;
        
        let mut r = [0u8; 32];
        let mut s = [0u8; 32];
        signature.r.to_big_endian(&mut r);
        signature.s.to_big_endian(&mut s);
        
        Ok(OrderSignature {
            r,
            s,
            v: signature.v as u8,
        })
    }
    
    /// Legacy API - delegates to new signature cache
    pub async fn warm_cache(
        &self,
        market_id: MarketId,
        base_price: Decimal,
        size: Decimal,
        _levels: u32,
        _spread_bps: u32,
    ) -> anyhow::Result<()> {
        // Use the new ±2% pre-signing strategy
        self.warm_signature_cache(market_id, base_price, size, Decimal::new(2, 0)).await?;
        Ok(())
    }
    
    /// Legacy API - Get a pre-signed order from cache if available
    #[inline(always)]
    pub fn get_cached_order(
        &self,
        market_id: MarketId,
        side: Side,
        price: Decimal,
        size: Decimal,
    ) -> Option<PreparedOrder> {
        let key = SignatureCacheKey::new(market_id, side, price, size);
        self.get_cached_signature(&key)
    }
    
    /// Clear expired orders from cache
    pub fn clear_expired(&self) {
        self.evict_expired();
    }
}

/// Batch signer for pre-signing multiple orders
pub struct BatchSigner {
    signer: Arc<OrderSigner>,
}

impl BatchSigner {
    pub fn new(signer: Arc<OrderSigner>) -> Self {
        Self { signer }
    }
    
    /// Pre-sign a batch of orders concurrently
    pub async fn prepare_batch(
        &self,
        orders: Vec<(MarketId, Side, Decimal, Decimal)>,
    ) -> Vec<anyhow::Result<PreparedOrder>> {
        let futures: Vec<_> = orders
            .into_iter()
            .map(|(market_id, side, price, size)| {
                let signer = self.signer.clone();
                async move {
                    signer.prepare_order(
                        market_id,
                        side,
                        price,
                        size,
                        OrderType::Limit,
                        TimeInForce::GTC,
                        3600,
                    ).await
                }
            })
            .collect();
        
        futures_util::future::join_all(futures).await
    }
}
