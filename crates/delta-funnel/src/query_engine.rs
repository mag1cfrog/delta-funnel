//! Query engine integration.

pub(crate) mod datafusion;

pub use datafusion::{
    DeltaProviderReadStatsSnapshot, DeltaProviderReaderBackend, DeltaProviderScanExecutionOptions,
    DeltaScanPartitionTargetDiagnosticInput, DeltaScanPartitionTargetDiagnosticOutput,
    DeltaScanPartitionTargetDiagnosticSource, DeltaScanPartitionTargetLocalEnvironmentDiagnostic,
    DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus, DeltaTableProviderConfig,
    QueryOptions, RegisteredDeltaSource, RegisteredDeltaSources, collect_delta_provider_read_stats,
    datafusion_query_output_stream, datafusion_session_config, datafusion_session_context,
    delta_scan_partition_target_local_environment_diagnostic,
    derive_delta_scan_partition_target_diagnostic, register_delta_sources,
    register_delta_sources_with_scan_execution_options,
};
