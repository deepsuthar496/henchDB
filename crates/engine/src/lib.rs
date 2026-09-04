//! Core library for the database engine.
//!
//! Generic naming rule: this crate is intentionally named `engine` and never
//! references the product name in code. The single user-visible product name
//! lives in [`PRODUCT_NAME`]; renaming the project means changing that one
//! constant plus crate/folder names.

pub mod btree;
pub mod catalog;
pub mod db;
pub mod epoch;
pub mod error;
pub mod latch;
pub mod page;
pub mod sql;
pub mod table;
pub mod types;
pub mod wal;

/// The single place where the product name is defined.
/// Rename the project here (and in folder names / Cargo metadata) only.
pub const PRODUCT_NAME: &str = "henchDB";

/// One-line product tagline, shown in the server banner.
pub const PRODUCT_TAGLINE: &str = "high-throughput relational engine";

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub use db::{Database, Output, Session};
pub use error::{Error, Result};
pub use types::Datum;
