//! Query engine integration.

pub(crate) mod datafusion;

pub use datafusion::{
    DeltaScanPartitionTargetDiagnosticInput, DeltaScanPartitionTargetDiagnosticOutput,
    DeltaScanPartitionTargetDiagnosticSource, DeltaTableProviderConfig, RegisteredDeltaSource,
    RegisteredDeltaSources, derive_delta_scan_partition_target_diagnostic, register_delta_sources,
};
