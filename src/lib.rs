//! PollyMorph - High-Frequency Trading Bot for Polymarket
//! Library crate exposing modules for integration tests.
#![recursion_limit = "512"]

pub mod config;
pub mod types;
pub mod websocket;
pub mod signing;
pub mod pricing;
pub mod risk;
pub mod execution;
pub mod metrics;
pub mod hints;
pub mod turbo;
pub mod asm_ops;
pub mod grid;
pub mod shadow;
pub mod tuner;
pub mod backtester;
pub mod simulator;
pub mod arb;
