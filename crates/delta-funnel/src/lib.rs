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

mod batch_pipeline;
pub mod error;
pub(crate) mod query_engine;
mod redaction;
mod sql_server;
mod table_formats;

pub use batch_pipeline::{
    BatchHandoffError, BatchHandoffOutcome, BatchHandoffStats, BatchPipelinePhase,
    RecordBatchConsumer, handoff_datafusion_query_output, handoff_record_batch_stream,
};
pub use error::DeltaFunnelError;
pub use query_engine::{
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
pub use sql_server::{
    LoadMode, MssqlBinaryPolicy, MssqlConnectionConfig, MssqlConnectionSource,
    MssqlConnectionSummary, MssqlDate64Policy, MssqlDdlPlan, MssqlDecimal256Policy,
    MssqlDecimalPolicy, MssqlFloatPolicy, MssqlLifecycleExecutionGuardrail,
    MssqlLifecycleGuardrailPolicy, MssqlLifecyclePlan, MssqlNanosecondPolicy,
    MssqlSchemaDiagnostic, MssqlSchemaDiagnosticField, MssqlSchemaPlan, MssqlSchemaPlanOptions,
    MssqlStringPolicy, MssqlTargetConfig, MssqlTargetOutputPlan, MssqlTargetResolutionContext,
    MssqlTargetSummary, MssqlTargetTable, MssqlTargetTableState, MssqlTimezonePolicy,
    MssqlUInt64Policy, ResolvedMssqlTarget, mssql_schema_diagnostic_reports,
    plan_mssql_create_table_ddl, plan_mssql_lifecycle, plan_mssql_output_schema,
    plan_mssql_target_output,
};
pub use table_formats::{
    DeltaProtocolReport, DeltaSourceConfig, DeltaStorageOptions, PlannedDeltaSource,
    ProtocolPreflight, load_delta_source, load_delta_sources, preflight_delta_protocol,
    preflight_delta_sources,
};

/// Current crate version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the current crate version.
#[must_use]
pub fn version() -> &'static str {
    VERSION
}
