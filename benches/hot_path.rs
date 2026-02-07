//! Benchmarks for the hot path components
//! Measures execution time in nanoseconds for critical HFT operations
//! Run with: cargo bench
//!
//! Key benchmarks:
//! - Signature cache lookup (target: <100ns)
//! - Kill switch check (target: <10ns)
//! - Risk check (target: <500ns)
//! - Order book operations (target: <50ns)
//! - TSC timestamp (target: <20ns)
//! - 2026 fee curve calculation (target: <100ns)

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use dashmap::DashMap;
use quanta::Clock;

// Simulate the types we need for benchmarking
#[derive(Clone, Copy)]
#[repr(C, align(32))]
struct PriceLevel {
    price: Decimal,
    size: Decimal,
    order_count: u32,
    _padding: u32,
}

const MAX_BOOK_DEPTH: usize = 10;

#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct OrderBookSnapshot {
    bids: [PriceLevel; MAX_BOOK_DEPTH],
    asks: [PriceLevel; MAX_BOOK_DEPTH],
    bid_count: u8,
    ask_count: u8,
    timestamp_ns: u64,
    sequence: u64,
}

impl OrderBookSnapshot {
    fn new() -> Self {
        Self {
            bids: [PriceLevel {
                price: Decimal::ZERO,
                size: Decimal::ZERO,
                order_count: 0,
                _padding: 0,
            }; MAX_BOOK_DEPTH],
            asks: [PriceLevel {
                price: Decimal::ZERO,
                size: Decimal::ZERO,
                order_count: 0,
                _padding: 0,
            }; MAX_BOOK_DEPTH],
            bid_count: 0,
            ask_count: 0,
            timestamp_ns: 0,
            sequence: 0,
        }
    }
    
    #[inline(always)]
    fn best_bid(&self) -> Option<Decimal> {
        if self.bid_count > 0 {
            Some(self.bids[0].price)
        } else {
            None
        }
    }
    
    #[inline(always)]
    fn best_ask(&self) -> Option<Decimal> {
        if self.ask_count > 0 {
            Some(self.asks[0].price)
        } else {
            None
        }
    }
    
    #[inline(always)]
    fn spread_bps(&self) -> Option<u32> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) if bid > Decimal::ZERO => {
                let spread = (ask - bid) / bid * Decimal::new(10000, 0);
                Some(spread.to_string().parse().unwrap_or(0))
            }
            _ => None,
        }
    }
}

fn bench_order_book_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("order_book");
    
    // Create a realistic order book
    let mut book = OrderBookSnapshot::new();
    book.bid_count = 5;
    book.ask_count = 5;
    
    for i in 0..5 {
        book.bids[i] = PriceLevel {
            price: Decimal::from_str(&format!("0.{}", 50 - i)).unwrap(),
            size: Decimal::new(1000, 0),
            order_count: 10,
            _padding: 0,
        };
        book.asks[i] = PriceLevel {
            price: Decimal::from_str(&format!("0.{}", 51 + i)).unwrap(),
            size: Decimal::new(1000, 0),
            order_count: 10,
            _padding: 0,
        };
    }
    
    group.bench_function("best_bid", |b| {
        b.iter(|| black_box(book.best_bid()))
    });
    
    group.bench_function("best_ask", |b| {
        b.iter(|| black_box(book.best_ask()))
    });
    
    group.bench_function("spread_bps", |b| {
        b.iter(|| black_box(book.spread_bps()))
    });
    
    group.bench_function("snapshot_copy", |b| {
        b.iter(|| {
            let copy = black_box(book);
            black_box(copy)
        })
    });
    
    group.finish();
}

fn bench_decimal_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("decimal");
    
    let price = Decimal::from_str("0.5234").unwrap();
    let size = Decimal::new(1000, 0);
    let fee_bps = Decimal::new(315, 0);
    
    group.bench_function("multiply", |b| {
        b.iter(|| black_box(price * size))
    });
    
    group.bench_function("divide", |b| {
        b.iter(|| black_box(price / size))
    });
    
    group.bench_function("fee_calculation", |b| {
        b.iter(|| {
            let value = price * size;
            let fee = value * fee_bps / Decimal::new(10000, 0);
            black_box(fee)
        })
    });
    
    group.finish();
}

fn bench_json_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_parsing");
    
    // Typical order book update message
    let json_msg = r#"{"type":"book","market":"0x1234","asset_id":"12345","bids":[["0.50","1000"],["0.49","2000"]],"asks":[["0.51","1000"],["0.52","2000"]],"timestamp":"1704067200000","hash":"0xabc"}"#;
    
    group.bench_function("simd_json_parse", |b| {
        b.iter(|| {
            let mut buffer = json_msg.as_bytes().to_vec();
            let result: Result<serde_json::Value, _> = simd_json::from_slice(&mut buffer);
            black_box(result)
        })
    });
    
    group.bench_function("serde_json_parse", |b| {
        b.iter(|| {
            let result: Result<serde_json::Value, _> = serde_json::from_str(json_msg);
            black_box(result)
        })
    });
    
    group.finish();
}

fn bench_atomic_operations(c: &mut Criterion) {
    use std::sync::atomic::{AtomicI64, AtomicBool, Ordering};
    
    let mut group = c.benchmark_group("atomics");
    
    let counter = AtomicI64::new(0);
    let flag = AtomicBool::new(false);
    
    group.bench_function("load_relaxed", |b| {
        b.iter(|| black_box(counter.load(Ordering::Relaxed)))
    });
    
    group.bench_function("load_seqcst", |b| {
        b.iter(|| black_box(counter.load(Ordering::SeqCst)))
    });
    
    group.bench_function("fetch_add", |b| {
        b.iter(|| black_box(counter.fetch_add(1, Ordering::Relaxed)))
    });
    
    group.bench_function("bool_load", |b| {
        b.iter(|| black_box(flag.load(Ordering::Relaxed)))
    });
    
    group.finish();
}

fn bench_timestamp(c: &mut Criterion) {
    let mut group = c.benchmark_group("timestamp");
    
    group.bench_function("system_time_ns", |b| {
        b.iter(|| {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            black_box(ts)
        })
    });
    
    group.bench_function("instant_now", |b| {
        b.iter(|| black_box(std::time::Instant::now()))
    });
    
    // TSC-based timing using quanta
    let clock = Clock::new();
    group.bench_function("quanta_tsc_now", |b| {
        b.iter(|| black_box(clock.now()))
    });
    
    group.bench_function("quanta_tsc_raw", |b| {
        b.iter(|| black_box(clock.raw()))
    });
    
    group.bench_function("quanta_elapsed_ns", |b| {
        let start = clock.now();
        b.iter(|| {
            let elapsed = start.elapsed().as_nanos();
            black_box(elapsed)
        })
    });
    
    group.finish();
}

/// Benchmark signature cache operations (DashMap)
/// Target: <100ns for cache hit
fn bench_signature_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("signature_cache");
    
    // Simulate SignatureCacheKey
    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    struct CacheKey {
        token_id: u64,
        side: u8,
        price_bps: u16,
        size_cents: u32,
    }
    
    // Create cache with pre-populated entries
    let cache: DashMap<CacheKey, u64, ahash::RandomState> = 
        DashMap::with_hasher(ahash::RandomState::new());
    
    // Pre-populate with 10k entries (simulating pre-signed orders)
    for token_id in 0..100u64 {
        for price_bps in 4000..4200u16 { // 200 price levels
            for side in 0..2u8 {
                let key = CacheKey {
                    token_id,
                    side,
                    price_bps,
                    size_cents: 10000,
                };
                cache.insert(key, token_id * 1000 + price_bps as u64);
            }
        }
    }
    
    let lookup_key = CacheKey {
        token_id: 50,
        side: 0,
        price_bps: 4100,
        size_cents: 10000,
    };
    
    let miss_key = CacheKey {
        token_id: 999,
        side: 0,
        price_bps: 9999,
        size_cents: 10000,
    };
    
    group.bench_function("cache_hit", |b| {
        b.iter(|| {
            let result = cache.get(&lookup_key);
            black_box(result)
        })
    });
    
    group.bench_function("cache_miss", |b| {
        b.iter(|| {
            let result = cache.get(&miss_key);
            black_box(result)
        })
    });
    
    group.bench_function("cache_insert", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = CacheKey {
                token_id: 1000 + i,
                side: 0,
                price_bps: 5000,
                size_cents: 10000,
            };
            cache.insert(key, i);
            i += 1;
            black_box(())
        })
    });
    
    group.finish();
}

/// Benchmark kill switch check (AtomicBool)
/// Target: <10ns - this MUST be extremely fast
fn bench_kill_switch(c: &mut Criterion) {
    let mut group = c.benchmark_group("kill_switch");
    
    let kill_switch = AtomicBool::new(false);
    let trading_enabled = AtomicBool::new(true);
    
    group.bench_function("single_check", |b| {
        b.iter(|| {
            let triggered = kill_switch.load(Ordering::Relaxed);
            black_box(!triggered)
        })
    });
    
    group.bench_function("combined_check", |b| {
        b.iter(|| {
            // Simulates is_trading_allowed()
            let allowed = !kill_switch.load(Ordering::Relaxed) 
                && trading_enabled.load(Ordering::Relaxed);
            black_box(allowed)
        })
    });
    
    group.bench_function("trigger_swap", |b| {
        b.iter(|| {
            let was_triggered = kill_switch.swap(true, Ordering::SeqCst);
            kill_switch.store(false, Ordering::SeqCst); // Reset
            black_box(was_triggered)
        })
    });
    
    group.finish();
}

/// Benchmark 2026 fee curve calculations
/// Target: <100ns for fee determination
fn bench_fee_curve_2026(c: &mut Criterion) {
    let mut group = c.benchmark_group("fee_curve_2026");
    
    let maker_force_min = Decimal::new(40, 2); // 0.40
    let maker_force_max = Decimal::new(60, 2); // 0.60
    
    let prob_in_zone = Decimal::new(50, 2);    // 0.50 - in forced maker zone
    let prob_outside = Decimal::new(25, 2);    // 0.25 - outside zone
    let prob_edge = Decimal::new(10, 2);       // 0.10 - at edge
    
    group.bench_function("is_forced_maker_zone", |b| {
        b.iter(|| {
            let in_zone = prob_in_zone >= maker_force_min 
                && prob_in_zone <= maker_force_max;
            black_box(in_zone)
        })
    });
    
    group.bench_function("get_dynamic_fee", |b| {
        b.iter(|| {
            let fee = if prob_in_zone < Decimal::new(15, 2) 
                || prob_in_zone > Decimal::new(85, 2) {
                200u32 // edge fee
            } else if prob_in_zone >= maker_force_min 
                && prob_in_zone <= maker_force_max {
                315u32 // mid-range fee (3.15%)
            } else {
                250u32 // default fee
            };
            black_box(fee)
        })
    });
    
    group.bench_function("full_profitability_check", |b| {
        let spread_bps = 100u32;
        let gas_bps = 5u32;
        let slippage_bps = 10u32;
        let min_profit_bps = 50u32;
        
        b.iter(|| {
            // Simulate is_profitable_2026()
            let force_maker = prob_in_zone >= maker_force_min 
                && prob_in_zone <= maker_force_max;
            
            let profitable = if force_maker {
                spread_bps as i32 > 20 // maker rebate
            } else {
                let dynamic_fee = 250u32;
                spread_bps > dynamic_fee + slippage_bps + gas_bps + min_profit_bps
            };
            black_box(profitable)
        })
    });
    
    group.finish();
}

/// Benchmark simulated risk check
/// Target: <500ns for full check
fn bench_risk_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("risk_check");
    
    let kill_switch = AtomicBool::new(false);
    let daily_pnl_cents = AtomicI64::new(-100000); // -$1000
    let total_exposure_cents = AtomicI64::new(2500000); // $25,000
    let trade_count = AtomicI64::new(150);
    
    let max_exposure_cents = 5000000i64; // $50,000
    let max_position_cents = 1000000i64; // $10,000
    let kill_threshold_cents = 500000i64; // $5,000
    
    group.bench_function("full_risk_check", |b| {
        let order_value_cents = 50000i64; // $500 order
        
        b.iter(|| {
            // 1. Kill switch check
            if kill_switch.load(Ordering::Relaxed) {
                return black_box(1u8); // Rejected
            }
            
            // 2. Daily P&L check
            let pnl = daily_pnl_cents.load(Ordering::Relaxed);
            if pnl < -kill_threshold_cents {
                return black_box(2u8); // Rejected
            }
            
            // 3. Exposure check
            let exposure = total_exposure_cents.load(Ordering::Relaxed);
            if exposure + order_value_cents > max_exposure_cents {
                return black_box(3u8); // Rejected
            }
            
            // 4. Position check (simplified)
            if order_value_cents > max_position_cents {
                return black_box(4u8); // Rejected
            }
            
            black_box(0u8) // Approved
        })
    });
    
    group.bench_function("kill_switch_only", |b| {
        b.iter(|| {
            let blocked = kill_switch.load(Ordering::Relaxed);
            black_box(!blocked)
        })
    });
    
    group.finish();
}

/// Benchmark full hot path simulation
/// This simulates the execute_signal critical path
fn bench_hot_path_simulation(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_path_full");
    group.sample_size(1000); // More samples for accuracy
    
    // Setup simulated state
    let kill_switch = AtomicBool::new(false);
    let clock = Clock::new();
    
    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    struct CacheKey {
        token_id: u64,
        side: u8,
        price_bps: u16,
        size_cents: u32,
    }
    
    let cache: DashMap<CacheKey, u64, ahash::RandomState> = 
        DashMap::with_hasher(ahash::RandomState::new());
    
    // Pre-populate cache
    let key = CacheKey {
        token_id: 1,
        side: 0,
        price_bps: 5000,
        size_cents: 10000,
    };
    cache.insert(key, 12345);
    
    group.bench_function("cache_hit_path", |b| {
        b.iter(|| {
            // 1. TSC timestamp
            let _start = clock.now();
            
            // 2. Kill switch check
            if kill_switch.load(Ordering::Relaxed) {
                return black_box(0u64);
            }
            
            // 3. Cache lookup (hit)
            let result = cache.get(&key);
            
            // 4. Return
            black_box(result.map(|r| *r).unwrap_or(0))
        })
    });
    
    let miss_key = CacheKey {
        token_id: 999,
        side: 1,
        price_bps: 9999,
        size_cents: 99999,
    };
    
    group.bench_function("cache_miss_path", |b| {
        b.iter(|| {
            // 1. TSC timestamp
            let _start = clock.now();
            
            // 2. Kill switch check
            if kill_switch.load(Ordering::Relaxed) {
                return black_box(0u64);
            }
            
            // 3. Cache lookup (miss)
            let result = cache.get(&miss_key);
            
            // 4. Return (would trigger signing in real code)
            black_box(result.map(|r| *r).unwrap_or(0))
        })
    });
    
    group.finish();
}

// =============================================================================
// TURBO MODE BENCHMARKS - Beyond Human Limits
// =============================================================================

fn bench_turbo_order(c: &mut Criterion) {
    use std::hint::black_box;
    
    let mut group = c.benchmark_group("turbo");
    group.sample_size(1000);
    
    // Benchmark TurboOrder creation (stack allocation)
    group.bench_function("turbo_order_create", |b| {
        b.iter(|| {
            // Zero-copy stack allocation
            #[repr(C, align(64))]
            struct TurboOrder {
                market_hash: u64,
                price_raw: i64,
                size_raw: i64,
                side: u8,
                flags: u8,
                _pad: [u8; 46],
            }
            black_box(TurboOrder {
                market_hash: 0xcbf29ce484222325,
                price_raw: 50_000_000_000,
                size_raw: 1_000_000_000,
                side: 0,
                flags: 0x01,
                _pad: [0; 46],
            })
        })
    });
    
    // Benchmark compile-time hash vs runtime hash
    group.bench_function("const_hash_lookup", |b| {
        const KNOWN_HASH: u64 = 0xcbf29ce484222325;
        let hashes = [KNOWN_HASH, KNOWN_HASH + 1, KNOWN_HASH + 2, KNOWN_HASH + 3];
        b.iter(|| {
            let target = black_box(KNOWN_HASH);
            hashes.iter().position(|&h| h == target)
        })
    });
    
    // Benchmark SPSC queue push (simulated)
    group.bench_function("spsc_push_simulated", |b| {
        use std::sync::atomic::{AtomicU64, Ordering};
        
        #[repr(align(64))]
        struct PaddedAtomic(AtomicU64);
        
        let head = PaddedAtomic(AtomicU64::new(0));
        let tail = PaddedAtomic(AtomicU64::new(0));
        let buffer: [u64; 1024] = [0; 1024];
        
        b.iter(|| {
            let h = head.0.load(Ordering::Relaxed);
            let t = tail.0.load(Ordering::Acquire);
            let next = (h + 1) & 1023;
            if next != t {
                black_box(buffer[h as usize]);
                head.0.store(next, Ordering::Release);
                true
            } else {
                false
            }
        })
    });
    
    // Benchmark SPSC vs mpsc channel
    group.bench_function("mpsc_send", |b| {
        let (tx, rx) = std::sync::mpsc::sync_channel::<u64>(1024);
        std::thread::spawn(move || {
            while let Ok(_) = rx.recv() {}
        });
        b.iter(|| {
            let _ = tx.try_send(black_box(42u64));
        })
    });
    
    group.finish();
}

fn bench_simd_operations(c: &mut Criterion) {
    use wide::f64x4;
    
    let mut group = c.benchmark_group("simd");
    group.sample_size(1000);
    
    // Benchmark SIMD profitability check (4 prices at once)
    group.bench_function("simd_profit_check_x4", |b| {
        let our_prices = f64x4::from([100.0, 100.0, 100.0, 100.0]);
        let market_prices = f64x4::from([101.0, 100.5, 100.2, 99.5]);
        let fees = f64x4::from([50.0, 50.0, 50.0, 50.0]);
        
        b.iter(|| {
            let spread = (black_box(market_prices) - black_box(our_prices)) 
                / black_box(our_prices) * f64x4::splat(10000.0);
            let _profitable = spread - black_box(fees);
            black_box(spread)
        })
    });
    
    // Benchmark scalar profitability check (for comparison)
    group.bench_function("scalar_profit_check_x4", |b| {
        let our = [100.0f64, 100.0, 100.0, 100.0];
        let market = [101.0f64, 100.5, 100.2, 99.5];
        let fees = [50.0f64, 50.0, 50.0, 50.0];
        
        b.iter(|| {
            let mut results = [false; 4];
            for i in 0..4 {
                let spread = (black_box(market[i]) - black_box(our[i])) 
                    / black_box(our[i]) * 10000.0;
                results[i] = spread > black_box(fees[i]);
            }
            black_box(results)
        })
    });
    
    // Benchmark raw RDTSC vs quanta
    group.bench_function("raw_rdtsc", |b| {
        b.iter(|| {
            let start: u64;
            unsafe {
                std::arch::asm!(
                    "rdtsc",
                    "shl rdx, 32",
                    "or rax, rdx",
                    out("rax") start,
                    out("rdx") _,
                    options(nostack, nomem)
                );
            }
            black_box(start)
        })
    });
    
    group.finish();
}

// =============================================================================
// ASM OPERATIONS BENCHMARKS - Inline Assembly vs Rust
// =============================================================================

fn bench_asm_operations(c: &mut Criterion) {
    use std::hint::black_box;
    
    let mut group = c.benchmark_group("asm");
    group.sample_size(1000);
    
    // CMOV vs branching
    group.bench_function("cmov_select", |b| {
        let cond = black_box(true);
        let a = black_box(100u64);
        let val_b = black_box(200u64);
        b.iter(|| {
            // Inline asm CMOV
            let result: u64;
            unsafe {
                std::arch::asm!(
                    "test {cond:l}, {cond:l}",
                    "cmovnz {result}, {a}",
                    cond = in(reg) cond as u64,
                    a = in(reg) a,
                    result = inout(reg) val_b => result,
                    options(pure, nomem, nostack)
                );
            }
            black_box(result)
        })
    });
    
    group.bench_function("branch_select", |b| {
        let cond = black_box(true);
        let a = black_box(100u64);
        let val_b = black_box(200u64);
        b.iter(|| {
            // Standard Rust branch
            let result = if black_box(cond) { a } else { val_b };
            black_box(result)
        })
    });
    
    // CRC32 vs FNV hash
    group.bench_function("crc32_hash", |b| {
        let value = black_box(0x123456789ABCDEFu64);
        b.iter(|| {
            let result: u64;
            unsafe {
                std::arch::asm!(
                    "crc32 {result:r}, {value:r}",
                    result = inout(reg) 0xDEADBEEFu64 => result,
                    value = in(reg) value,
                    options(pure, nomem, nostack)
                );
            }
            black_box(result)
        })
    });
    
    group.bench_function("fnv_hash", |b| {
        let value = black_box(0x123456789ABCDEFu64);
        b.iter(|| {
            // FNV-1a hash
            const FNV_OFFSET: u64 = 0xcbf29ce484222325;
            const FNV_PRIME: u64 = 0x100000001b3;
            let bytes = value.to_le_bytes();
            let mut hash = FNV_OFFSET;
            for byte in bytes {
                hash ^= byte as u64;
                hash = hash.wrapping_mul(FNV_PRIME);
            }
            black_box(hash)
        })
    });
    
    // Fast div10 vs regular division
    group.bench_function("fast_div10", |b| {
        let n = black_box(123456789u64);
        b.iter(|| {
            let result: u64;
            unsafe {
                std::arch::asm!(
                    "mov rax, 0xCCCCCCCCCCCCCCCD",
                    "mul {n}",
                    "shr rdx, 3",
                    n = in(reg) n,
                    out("rax") _,
                    out("rdx") result,
                    options(pure, nomem, nostack)
                );
            }
            black_box(result)
        })
    });
    
    group.bench_function("regular_div10", |b| {
        let n = black_box(123456789u64);
        b.iter(|| {
            black_box(n / 10)
        })
    });
    
    // POPCNT intrinsic vs asm
    group.bench_function("popcnt_asm", |b| {
        let value = black_box(0xDEADBEEFCAFEBABEu64);
        b.iter(|| {
            let result: u64;
            unsafe {
                std::arch::asm!(
                    "popcnt {result}, {value}",
                    value = in(reg) value,
                    result = out(reg) result,
                    options(pure, nomem, nostack)
                );
            }
            black_box(result)
        })
    });
    
    group.bench_function("popcnt_rust", |b| {
        let value = black_box(0xDEADBEEFCAFEBABEu64);
        b.iter(|| {
            black_box(value.count_ones())
        })
    });
    
    // PAUSE instruction in spin loop
    group.bench_function("pause_instruction", |b| {
        b.iter(|| {
            unsafe {
                std::arch::asm!("pause", options(nomem, nostack));
            }
        })
    });
    
    group.finish();
}

// =============================================================================
// GRID GENERATION BENCHMARKS - Target: <5us for 10 levels
// =============================================================================

fn bench_grid_generation(c: &mut Criterion) {
    use std::hint::black_box;
    
    let mut group = c.benchmark_group("grid");
    group.sample_size(1000);
    
    // Simulate grid level generation (matching GridEngine logic)
    group.bench_function("generate_10_levels", |b| {
        let mid_price = Decimal::new(50, 2); // 0.50
        let base_spacing = Decimal::new(1, 2); // 0.01
        let size_per_level = Decimal::new(100, 0); // 100 USDC
        
        b.iter(|| {
            let mut signals = Vec::with_capacity(10);
            let spacing = black_box(base_spacing) * Decimal::new(15, 1); // Dynamic spacing
            
            for i in 0..10usize {
                let level_offset = Decimal::from(i as i64 + 1) * spacing;
                let price = black_box(mid_price) - level_offset;
                
                // Skip invalid prices
                if price <= Decimal::ZERO || price >= Decimal::ONE {
                    continue;
                }
                
                // Create signal (simulated TradeSignal fields)
                signals.push((
                    price,
                    black_box(size_per_level),
                    0u8, // side
                    50i32 + (i as i32 * 5), // expected_profit_bps
                ));
            }
            black_box(signals)
        })
    });
    
    // Benchmark dual grid (buy + sell)
    group.bench_function("generate_dual_grid_10x2", |b| {
        let mid_price = Decimal::new(50, 2);
        let base_spacing = Decimal::new(1, 2);
        let size_per_level = Decimal::new(100, 0);
        
        b.iter(|| {
            let mut buy_signals = Vec::with_capacity(10);
            let mut sell_signals = Vec::with_capacity(10);
            let spacing = black_box(base_spacing) * Decimal::new(15, 1);
            
            // Buy side
            for i in 0..10usize {
                let level_offset = Decimal::from(i as i64 + 1) * spacing;
                let price = black_box(mid_price) - level_offset;
                if price > Decimal::ZERO {
                    buy_signals.push((price, black_box(size_per_level), 0u8));
                }
            }
            
            // Sell side
            for i in 0..10usize {
                let level_offset = Decimal::from(i as i64 + 1) * spacing;
                let price = black_box(mid_price) + level_offset;
                if price < Decimal::ONE {
                    sell_signals.push((price, black_box(size_per_level), 1u8));
                }
            }
            
            black_box((buy_signals, sell_signals))
        })
    });
    
    // Benchmark exposure calculation
    group.bench_function("calculate_grid_exposure", |b| {
        // Pre-create grid levels
        let levels: Vec<(Decimal, Decimal)> = (0..10)
            .map(|i| {
                let price = Decimal::new(50 - i, 2);
                let size = Decimal::new(100, 0);
                (price, size)
            })
            .collect();
        
        b.iter(|| {
            let mut total_exposure = Decimal::ZERO;
            for (price, size) in black_box(&levels) {
                total_exposure += *price * *size;
            }
            black_box(total_exposure)
        })
    });
    
    // Benchmark dynamic spacing calculation
    group.bench_function("dynamic_spacing_calc", |b| {
        let base_spacing = Decimal::new(1, 2);
        let volatility_mult = Decimal::ONE;
        
        b.iter(|| {
            let prob = black_box(Decimal::new(45, 2)); // 0.45
            let dist_from_center = (prob - Decimal::new(5, 1)).abs();
            
            let spacing = if dist_from_center < Decimal::new(1, 1) {
                base_spacing * Decimal::new(15, 1) * volatility_mult
            } else if dist_from_center < Decimal::new(2, 1) {
                base_spacing * Decimal::new(12, 1) * volatility_mult
            } else {
                base_spacing * volatility_mult
            };
            black_box(spacing)
        })
    });
    
    // Benchmark rebalance signal generation
    group.bench_function("rebalance_signal", |b| {
        let filled_price = Decimal::new(48, 2); // 0.48
        let filled_size = Decimal::new(100, 0);
        let spread_capture = Decimal::new(2, 2); // 0.02
        
        b.iter(|| {
            let counter_price = black_box(filled_price) + black_box(spread_capture);
            if counter_price > Decimal::ZERO && counter_price < Decimal::ONE {
                Some((counter_price, black_box(filled_size), 1u8)) // Sell signal
            } else {
                None
            }
        })
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_order_book_operations,
    bench_decimal_operations,
    bench_json_parsing,
    bench_atomic_operations,
    bench_timestamp,
    bench_signature_cache,
    bench_kill_switch,
    bench_fee_curve_2026,
    bench_risk_check,
    bench_hot_path_simulation,
    bench_turbo_order,
    bench_simd_operations,
    bench_asm_operations,
    bench_grid_generation,
);

criterion_main!(benches);
