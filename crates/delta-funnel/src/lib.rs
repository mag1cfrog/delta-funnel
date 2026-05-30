//! Core library for DeltaFunnel.
//!
//! This crate will own the high-level export orchestration from table formats
//! such as Delta Lake into Microsoft SQL Server. Low-level Arrow to TDS bulk
//! loading is expected to stay in `arrow-tiberius`.

mod delta_kernel_adapter;
#[cfg(test)]
mod dependency_guard;
pub mod error;
mod named_source;
mod source_name;
mod source_snapshot;
mod source_uri;

pub use error::DeltaFunnelError;
pub use named_source::{
    DeltaSourceConfig, PlannedDeltaSource, load_delta_source, load_delta_sources,
};
pub use source_name::validate_delta_source_names;
pub use source_snapshot::{LoadedDeltaTableSnapshot, load_delta_table_snapshot};
pub use source_uri::normalize_delta_table_uri;

/// Current crate version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the current crate version.
#[must_use]
pub fn version() -> &'static str {
    VERSION
}
