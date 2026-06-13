//! Query engine integration.

pub(crate) mod datafusion;

pub use datafusion::{
    DeltaProviderScanExecutionOptions, DeltaScanPartitionTargetDiagnosticInput,
    DeltaScanPartitionTargetDiagnosticOutput, DeltaScanPartitionTargetDiagnosticSource,
    DeltaScanPartitionTargetLocalEnvironmentDiagnostic,
    DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus, DeltaTableProviderConfig,
    RegisteredDeltaSource, RegisteredDeltaSources,
    delta_scan_partition_target_local_environment_diagnostic,
    derive_delta_scan_partition_target_diagnostic, register_delta_sources,
    register_delta_sources_with_scan_execution_options,
};
