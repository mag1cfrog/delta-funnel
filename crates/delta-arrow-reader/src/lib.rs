//! Read-only Delta Lake to Arrow support.

mod config;
mod deletion_vector;
mod error;
mod kernel;
#[cfg(feature = "native-async")]
#[allow(dead_code)]
mod metered_object_store;
mod metrics;
#[cfg(feature = "native-async")]
#[allow(dead_code)]
mod native_async_reader;
#[cfg(feature = "native-async")]
#[allow(dead_code)]
mod native_async_row_group_pruning;
#[cfg(feature = "official-kernel")]
#[allow(dead_code)]
mod official_kernel_reader;
mod partition_target;
mod planning;
mod predicate;
mod protocol;
#[allow(dead_code)]
mod scheduling;
mod snapshot;
mod transform;
mod uri;

pub use config::{
    DeltaReaderBackend, DeltaReaderExecutionOptions, DeltaSnapshotSelection, DeltaStorageOptions,
};
pub use error::{DeltaReaderError, DeltaReaderPhase};
pub use metrics::{DeltaReadMetrics, DeltaReadMetricsSnapshot};
#[doc(hidden)]
pub use partition_target::{
    DeltaScanPartitionTargetDiagnosticInput, DeltaScanPartitionTargetDiagnosticOutput,
    DeltaScanPartitionTargetDiagnosticSource, DeltaScanPartitionTargetLocalEnvironmentDiagnostic,
    DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus,
    delta_scan_partition_target_local_environment_diagnostic,
    derive_delta_scan_partition_target_diagnostic,
};
pub use predicate::{DeltaComparison, DeltaPredicate, DeltaScalar};
pub use protocol::DeltaProtocolInfo;

/// The crate version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the crate version.
pub const fn version() -> &'static str {
    VERSION
}
