//! Low-level performance hints for the hot path
//!
//! Provides branch prediction hints, prefetch instructions, and other
//! micro-optimizations for sub-microsecond latency.
//!
//! EXTREME OPTIMIZATIONS:
//! - Raw RDTSC/RDTSCP inline assembly (bypasses quanta overhead)
//! - Cache-line aligned hot data hints
//! - Forced inlining on all hot functions

#![allow(unused_unsafe)]

use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0, _MM_HINT_T1, _mm_lfence, __rdtscp, _rdtsc};

/// Branch prediction hint: likely to be true
/// 
/// Use this for the fast path in hot code:
/// ```rust
/// if likely(cache.contains_key(&key)) {
///     // fast path - cache hit
/// }
/// ```
#[inline(always)]
#[cold]
pub fn unlikely(b: bool) -> bool {
    if b {
        std::hint::black_box(b)
    } else {
        b
    }
}

/// Branch prediction hint: likely to be true
#[inline(always)]
pub fn likely(b: bool) -> bool {
    if !b {
        std::hint::black_box(b)
    } else {
        b
    }
}

/// Prefetch data into L1 cache (temporal - will be used soon and often)
/// 
/// Call this ~100-200 cycles before you need the data.
/// Good for: order book data, signature cache entries
#[inline(always)]
pub fn prefetch_l1<T>(ptr: *const T) {
    unsafe {
        _mm_prefetch(ptr as *const i8, _MM_HINT_T0);
    }
}

/// Prefetch data into L2 cache (less temporal)
/// 
/// Use for data that will be accessed but not immediately
#[inline(always)]
pub fn prefetch_l2<T>(ptr: *const T) {
    unsafe {
        _mm_prefetch(ptr as *const i8, _MM_HINT_T1);
    }
}

/// Spin-wait hint for busy loops
/// 
/// Reduces power consumption and improves performance on hyperthreaded cores
/// when spinning on an atomic variable.
#[inline(always)]
pub fn spin_hint() {
    std::hint::spin_loop();
}

/// Memory fence - ensure all prior writes are visible
#[inline(always)]
pub fn memory_fence() {
    std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
}

/// Compiler fence - prevent reordering across this point
#[inline(always)]
pub fn compiler_fence() {
    std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
}

// =============================================================================
// RAW TSC ACCESS - BYPASSES QUANTA OVERHEAD (~2ns faster)
// =============================================================================

/// Read TSC (Time Stamp Counter) directly via RDTSC instruction
/// 
/// ~7 cycles latency. Use for relative timing only.
/// WARNING: May not be synchronized across cores - use rdtscp for cross-core safety.
#[inline(always)]
pub fn rdtsc() -> u64 {
    unsafe { _rdtsc() }
}

/// Read TSC with serialization (RDTSCP) - safe across cores
/// 
/// ~20-30 cycles latency but guarantees ordering.
/// Returns (tsc_value, processor_id)
#[inline(always)]
pub fn rdtscp() -> (u64, u32) {
    let mut aux: u32 = 0;
    let tsc = unsafe { __rdtscp(&mut aux) };
    (tsc, aux)
}

/// Serializing fence + RDTSC for accurate measurement start
/// 
/// Use this at the START of a timed section to ensure all prior
/// instructions complete before reading TSC.
#[inline(always)]
pub fn rdtsc_start() -> u64 {
    unsafe {
        _mm_lfence(); // Serialize - wait for all prior instructions
        _rdtsc()
    }
}

/// RDTSCP for accurate measurement end
/// 
/// Use this at the END of a timed section. RDTSCP is naturally serializing.
#[inline(always)]
pub fn rdtsc_end() -> u64 {
    let mut aux: u32 = 0;
    unsafe { __rdtscp(&mut aux) }
}

/// Calculate elapsed cycles between start and end TSC values
#[inline(always)]
pub fn tsc_elapsed(start: u64, end: u64) -> u64 {
    end.wrapping_sub(start)
}

/// Convert TSC cycles to nanoseconds (approximate)
/// 
/// Assumes ~3GHz CPU. For precise conversion, calibrate at startup.
/// Most modern CPUs: 1 cycle ≈ 0.33ns at 3GHz
#[inline(always)]
pub fn cycles_to_ns_approx(cycles: u64) -> u64 {
    // Divide by 3 for ~3GHz CPU (shift right by ~1.58, approximate with *11/32)
    (cycles * 11) >> 5
}

// =============================================================================
// CACHE LINE UTILITIES
// =============================================================================

/// Cache line size on x86-64 (64 bytes)
pub const CACHE_LINE_SIZE: usize = 64;

/// Pad a value to cache line boundary to prevent false sharing
#[repr(align(64))]
pub struct CacheLinePadded<T>(pub T);

impl<T> CacheLinePadded<T> {
    #[inline(always)]
    pub const fn new(val: T) -> Self {
        Self(val)
    }
    
    #[inline(always)]
    pub fn get(&self) -> &T {
        &self.0
    }
    
    #[inline(always)]
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T: Clone> Clone for CacheLinePadded<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Default> Default for CacheLinePadded<T> {
    fn default() -> Self {
        Self(T::default())
    }
}

// =============================================================================
// ZERO-COST ABSTRACTIONS FOR HOT PATH
// =============================================================================

/// Force a value to be kept in register (prevents spilling to stack)
#[inline(always)]
pub fn keep_in_register<T>(val: T) -> T {
    std::hint::black_box(val)
}

/// Assume condition is true (UB if false!) - enables aggressive optimization
/// 
/// SAFETY: Only use when you are 100% certain the condition is always true.
/// Using this incorrectly causes undefined behavior.
#[inline(always)]
pub unsafe fn assume(cond: bool) {
    if !cond {
        std::hint::unreachable_unchecked();
    }
}

/// Unreachable code hint - stronger than unreachable!()
/// 
/// SAFETY: Must truly be unreachable or UB occurs.
#[inline(always)]
pub unsafe fn unreachable_unchecked() -> ! {
    std::hint::unreachable_unchecked()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_likely_unlikely() {
        assert!(likely(true));
        assert!(!likely(false));
        assert!(unlikely(true));
        assert!(!unlikely(false));
    }

    #[test]
    fn test_prefetch() {
        let data = [1u64; 64];
        prefetch_l1(data.as_ptr());
        prefetch_l2(data.as_ptr());
    }
}
