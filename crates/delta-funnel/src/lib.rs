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
mod orchestrator;
mod pipeline;
pub(crate) mod query_engine;
mod report;
mod sql_server;
mod support;
mod table_formats;

pub use error::{DeltaFunnelError, SqlTablePhase};
pub use orchestrator::{
    DeltaFunnelRuntime, DeltaFunnelSession, LazyTable, LazyTableKind, MssqlOutputTarget,
    OutputWritePlan, PlannedMssqlOutput, RegisteredDerivedTable, RegisteredSessionSource, RunMode,
    SessionOptions, WriteAllCacheAliasReport, WriteAllCacheAliasStatus, WriteAllCacheCandidateSkip,
    WriteAllCacheCandidateSkipReason, WriteAllCacheMode, WriteAllCacheReport,
    WriteAllNoCacheReason, WriteAllOptions, WriteAllReport,
};
pub use pipeline::{
    BatchHandoffError, BatchHandoffOutcome, BatchHandoffStats, BatchPipelinePhase,
    RecordBatchConsumer, handoff_datafusion_query_output, handoff_record_batch_stream,
};
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
pub use report::{
    DeltaProtocolReport, DeltaProviderSchedulingReport, DeltaSourceReport, DryRunScanSummaryMode,
    FileCount, FileCountKind, MssqlDryRunOutputFieldReport, MssqlDryRunOutputReport,
    MssqlDryRunSqlIdentityReport, MssqlDryRunSqlIdentityState, MssqlDryRunWorkflowReport,
    OutputStatus, OutputStatusKind, PhaseStatus, PhaseStatusKind, ReportReasonCode, RowCount,
    RowCountKind, SourceUsageStatus, TargetValidationMode, ValidationOptions, ValidationStatus,
    ValidationStatusKind, WorkflowStatus, WorkflowStatusKind, u128_to_u64_saturating,
    usize_to_u64_saturating,
};
pub use sql_server::{
    LoadMode, MssqlBatchShapingReport, MssqlBinaryPolicy, MssqlConnectionConfig,
    MssqlConnectionSource, MssqlConnectionSummary, MssqlDate64Policy, MssqlDdlPlan,
    MssqlDecimal256Policy, MssqlDecimalPolicy, MssqlFloatPolicy, MssqlLifecycleExecutionGuardrail,
    MssqlLifecycleGuardrailPolicy, MssqlLifecyclePlan, MssqlNanosecondPolicy,
    MssqlOutputBatchStream, MssqlOutputBatchStreamFactory, MssqlOutputBatchValidationReport,
    MssqlOutputFieldReport, MssqlOutputWriteJob, MssqlOutputWriteStatus, MssqlPreparedTarget,
    MssqlPreparedTargetAction, MssqlPreparedTargetReport, MssqlSchemaDiagnostic,
    MssqlSchemaDiagnosticField, MssqlSchemaPlan, MssqlSchemaPlanOptions, MssqlStringPolicy,
    MssqlTargetCleanupStatus, MssqlTargetConfig, MssqlTargetOutputPlan,
    MssqlTargetResolutionContext, MssqlTargetSummary, MssqlTargetTable, MssqlTargetTableState,
    MssqlTimezonePolicy, MssqlUInt64Policy, MssqlWorkflowWriteOptions, MssqlWorkflowWriteReport,
    MssqlWriteFailureContext, MssqlWriteFailureReport, MssqlWriteOptions, MssqlWritePhase,
    MssqlWriteReport, MssqlWriteSkippedReason, MssqlWriteSkippedReport, MssqlWriteStats,
    ResolvedMssqlTarget, default_mssql_write_options, mssql_schema_diagnostic_reports,
    mssql_write_options_for_output_plan, plan_mssql_create_table_ddl, plan_mssql_lifecycle,
    plan_mssql_output_schema, plan_mssql_target_for_output, plan_mssql_target_for_resolved_output,
    plan_mssql_target_output, validate_mssql_output_record_batch, validate_mssql_output_schema,
    write_mssql_outputs_to_mssql, write_output_batches_to_mssql,
};
pub(crate) use sql_server::{MssqlWorkflowOutputWriter, write_mssql_outputs_with_writer};
pub use table_formats::{
    DeltaSourceConfig, DeltaStorageOptions, PlannedDeltaSource, ProtocolPreflight,
    load_delta_source, load_delta_sources, preflight_delta_protocol, preflight_delta_sources,
};

/// Current crate version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the current crate version.
#[must_use]
pub fn version() -> &'static str {
    VERSION
}
