//! Read-only Delta Lake to Arrow support.

mod config;
mod deletion_vector;
mod error;
mod kernel;
mod metrics;
mod protocol;
mod snapshot;
mod uri;

pub use config::{
    DeltaReaderBackend, DeltaReaderExecutionOptions, DeltaSnapshotSelection, DeltaStorageOptions,
};
pub use error::{DeltaReaderError, DeltaReaderPhase};
pub use metrics::{DeltaReadMetrics, DeltaReadMetricsSnapshot};
pub use protocol::DeltaProtocolInfo;

/// The crate version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the crate version.
pub const fn version() -> &'static str {
    VERSION
}
