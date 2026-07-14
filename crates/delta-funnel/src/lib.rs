//! Read Delta Lake tables, query them with DataFusion SQL, preview results, and
//! load selected outputs into Microsoft SQL Server with high-throughput native
//! TDS bulk writes, without ODBC or JDBC.
//!
//! Delta Funnel provides a session-oriented workflow for:
//!
//! - registering one or more Delta Lake sources;
//! - building lazy tables from DataFusion SQL;
//! - executing bounded previews without contacting SQL Server;
//! - validating output plans with dry runs;
//! - streaming single or multiple outputs through native TDS bulk writes; and
//! - collecting structured source, validation, cache, and write reports.
//!
//! # Quickstart
//!
//! [`DeltaFunnelSession`] owns registered sources and lazy tables. Synchronous
//! applications can use [`DeltaFunnelRuntime`] to run the session's async query
//! actions.
//!
//! ```no_run
//! use delta_funnel::{
//!     DeltaFunnelRuntime, DeltaFunnelSession, DeltaSourceConfig, LoadMode,
//!     MssqlConnectionConfig, MssqlOutputTarget, MssqlTargetConfig,
//!     MssqlTargetTable, OutputWritePlan, RunMode, SessionOptions,
//! };
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let ado_connection_string = concat!(
//!         "server=tcp:localhost,1433;",
//!         "database=warehouse;",
//!         "User ID=etl_user;",
//!         "Password=REPLACE_ME;",
//!         "encrypt=true;",
//!         "TrustServerCertificate=yes",
//!     );
//!
//!     let default_connection = MssqlConnectionConfig::new(ado_connection_string)?
//!         .with_display_label("warehouse");
//!     let mut session = DeltaFunnelSession::new(
//!         SessionOptions::new().with_default_mssql_connection(default_connection),
//!     )?;
//!     let runtime = DeltaFunnelRuntime::new()?;
//!
//!     // Register the Delta table as "orders" so SQL can reference it.
//!     let _orders = session.delta_lake(DeltaSourceConfig::new(
//!         "orders",
//!         "file:///path/to/orders-delta",
//!     ))?;
//!
//!     // Build a lazy DataFusion SQL query. No rows are read yet.
//!     let daily_orders = runtime.table_from_sql(
//!         &mut session,
//!         r#"
//!         select customer_id, order_date, total_amount
//!         from orders
//!         where order_date >= date '2026-01-01'
//!         "#,
//!     )?;
//!
//!     // Preview executes the DataFusion query with a limit.
//!     let preview = runtime.preview_table(&session, &daily_orders, 20)?;
//!     println!("{}", preview.text());
//!
//!     // Write executes the query and loads the result into SQL Server.
//!     let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "daily_orders")?)
//!         .with_load_mode(LoadMode::CreateAndLoad);
//!     let output = OutputWritePlan::new(
//!         daily_orders,
//!         MssqlOutputTarget::new("daily_orders", target, RunMode::Execute),
//!     );
//!     let report = runtime.write_to_mssql(&session, &output)?;
//!
//!     println!("wrote output {}", report.output_name());
//!     Ok(())
//! }
//! ```
//!
//! Previewing executes the DataFusion query with the requested row limit. It
//! does not contact SQL Server or write rows.
//!
//! # Dry runs and multiple outputs
//!
//! Build an [`OutputWritePlan`] with [`RunMode::DryRun`] and pass it to
//! [`DeltaFunnelRuntime::dry_run_to_mssql`] to validate an output plan without
//! writing rows. [`DeltaFunnelRuntime::write_all`] can execute multiple outputs
//! while caching shared lazy-query dependencies when beneficial.
//!
//! See the [Rust quickstart](https://github.com/mag1cfrog/delta-funnel#rust-quickstart)
//! for a complete write example and the
//! [project documentation](https://mag1cfrog.github.io/delta-funnel/) for
//! installation, SQL Server, and workflow guidance.

pub mod error;
mod observability;
mod orchestrator;
mod pipeline;
#[doc(hidden)]
pub mod progress;
pub(crate) mod query_engine;
mod report;
mod sql_server;
mod support;
mod table_formats;

pub use error::{DeltaFunnelError, SqlTablePhase};
pub use orchestrator::{
    DeltaFunnelRuntime, DeltaFunnelSession, LazyTable, LazyTableKind, MssqlOutputTarget,
    OutputWritePlan, PlannedMssqlOutput, PreviewFailureContext, PreviewOptions,
    RegisteredDerivedTable, RegisteredSessionSource, RunMode, SessionOptions, TablePreview,
    WriteAllCacheMode, WriteAllOptions,
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
    ExecutionProfileMode, FileCount, FileCountKind, MssqlDryRunOutputFieldReport,
    MssqlDryRunOutputReport, MssqlDryRunSqlIdentityReport, MssqlDryRunSqlIdentityState,
    MssqlDryRunWorkflowReport, OutputStatus, OutputStatusKind, PhaseStatus, PhaseStatusKind,
    PhaseTimingReport, QueryExecutionMetric, QueryExecutionMetricCategory,
    QueryExecutionMetricValue, QueryExecutionOperatorProfile, QueryExecutionOutcome,
    QueryExecutionProfile, QueryExecutionScope, ReportReasonCode, RowCount, RowCountKind,
    SourceUsageStatus, TargetValidationMode, ValidationOptions, ValidationStatus,
    ValidationStatusKind, WorkflowStatus, WorkflowStatusKind, WriteAllCacheAliasReport,
    WriteAllCacheAliasStatus, WriteAllCacheCandidateSkip, WriteAllCacheCandidateSkipReason,
    WriteAllCacheReport, WriteAllNoCacheReason, WriteAllReport, duration_to_micros_saturating,
    u128_to_u64_saturating, usize_to_u64_saturating,
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
    MssqlTableName, MssqlTargetCleanupStatus, MssqlTargetConfig, MssqlTargetOutputPlan,
    MssqlTargetResolutionContext, MssqlTargetSummary, MssqlTargetTable, MssqlTargetTableState,
    MssqlTimestampPolicy, MssqlTimezonePolicy, MssqlUInt64Policy, MssqlWorkflowWriteOptions,
    MssqlWorkflowWriteReport, MssqlWriteBackend, MssqlWriteFailureContext, MssqlWriteFailureReport,
    MssqlWritePhase, MssqlWriteReport, MssqlWriteSkippedReason, MssqlWriteSkippedReport,
    MssqlWriteStats, ResolvedMssqlTarget, connect_mssql_client_from_ado_string,
    default_mssql_write_backend, mssql_schema_diagnostic_reports,
    mssql_write_backend_for_output_plan, plan_mssql_create_table_ddl, plan_mssql_lifecycle,
    plan_mssql_output_schema, plan_mssql_target_for_output, plan_mssql_target_for_resolved_output,
    plan_mssql_target_output, validate_mssql_output_record_batch, validate_mssql_output_schema,
    write_mssql_outputs_to_mssql, write_output_batches_to_mssql,
};
pub(crate) use sql_server::{
    MssqlStreamBenchmarkOutputWriter, MssqlWorkflowOutputWriter, MssqlWorkflowSinkWriter,
    write_mssql_outputs_with_writer,
};
pub use support::sanitize_uri_for_display;
pub use table_formats::{
    DeltaSourceConfig, DeltaStorageOptions, PlannedDeltaSource, ProtocolPreflight,
    load_delta_source, load_delta_sources, preflight_delta_protocol, preflight_delta_sources,
};
pub use table_formats::{load_delta_source_with_tracing, preflight_delta_protocol_with_tracing};

/// Current crate version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the current crate version.
#[must_use]
pub fn version() -> &'static str {
    VERSION
}
