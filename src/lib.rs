#![feature(default_free_fn)]

pub use arrow;
pub use arrow_flight;
pub use datafusion;
pub use parquet;

pub const WHALE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const ABOUT: &str = "Whale is a powerful query engine.";

pub mod backup;
pub mod catalog;
pub mod common;
pub mod connector;
pub mod engine;
pub mod network;
pub mod restore;
pub mod security;
pub mod utils;
