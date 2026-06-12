//! Core library for DeltaFunnel.
//!
//! This crate will own the high-level export orchestration from table formats
//! such as Delta Lake into Microsoft SQL Server. Low-level Arrow to TDS bulk
//! loading is expected to stay in `arrow-tiberius`.
//!
//! Module boundaries should follow the load workflow. `table_formats` owns
//! upstream table-format integrations, starting with Delta source configuration
//! and snapshot loading. Later DataFusion provider, query execution, SQL Server
//! sink, and orchestration work should land in their own modules when the first
//! real implementation slice needs them.

pub mod error;
pub(crate) mod query_engine;
mod redaction;
mod table_formats;

pub use error::DeltaFunnelError;
pub use query_engine::{
    DeltaScanPartitionTargetDiagnosticInput, DeltaScanPartitionTargetDiagnosticOutput,
    DeltaScanPartitionTargetDiagnosticSource, DeltaTableProviderConfig, RegisteredDeltaSource,
    RegisteredDeltaSources, derive_delta_scan_partition_target_diagnostic, register_delta_sources,
};
pub use table_formats::{
    DeltaProtocolReport, DeltaSourceConfig, PlannedDeltaSource, ProtocolPreflight,
    load_delta_source, load_delta_sources, preflight_delta_protocol, preflight_delta_sources,
};

/// Current crate version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the current crate version.
#[must_use]
pub fn version() -> &'static str {
    VERSION
}
