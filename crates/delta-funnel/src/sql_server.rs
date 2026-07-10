//! SQL Server target planning primitives.
//!
//! This module owns Delta Funnel configuration and reporting shapes around SQL
//! Server targets. Schema mapping, identifier quoting, DDL rendering, and
//! Arrow-to-TDS writing remain owned by `arrow-tiberius`.

mod execution;
mod planning;
mod target;

pub use arrow_tiberius::{TableName as MssqlTableName, connect_mssql_client_from_ado_string};
pub(crate) use execution::MssqlBulkLoadWriter;
pub(crate) use execution::table_name_from_target;
pub use execution::{
    MssqlBatchShapingReport, MssqlOutputBatchStream, MssqlOutputBatchStreamFactory,
    MssqlOutputBatchValidationReport, MssqlOutputFieldReport, MssqlOutputWriteJob,
    MssqlOutputWriteStatus, MssqlPreparedTarget, MssqlPreparedTargetAction,
    MssqlPreparedTargetReport, MssqlTargetCleanupStatus, MssqlWorkflowWriteOptions,
    MssqlWorkflowWriteReport, MssqlWriteBackend, MssqlWriteFailureContext, MssqlWriteFailureReport,
    MssqlWritePhase, MssqlWriteReport, MssqlWriteSkippedReason, MssqlWriteSkippedReport,
    MssqlWriteStats, default_mssql_write_backend, mssql_write_backend_for_output_plan,
    validate_mssql_output_record_batch, validate_mssql_output_schema, write_mssql_outputs_to_mssql,
    write_output_batches_to_mssql,
};
pub(crate) use execution::{
    MssqlStreamBenchmarkOutputWriter, MssqlWorkflowOutputWriter, write_mssql_outputs_with_writer,
    write_output_batches_to_mssql_with_reporter,
    write_output_batches_to_mssql_with_validation_options,
};
pub use planning::{
    MssqlBinaryPolicy, MssqlDate64Policy, MssqlDdlPlan, MssqlDecimal256Policy, MssqlDecimalPolicy,
    MssqlFloatPolicy, MssqlLifecycleExecutionGuardrail, MssqlLifecycleGuardrailPolicy,
    MssqlLifecyclePlan, MssqlNanosecondPolicy, MssqlSchemaDiagnostic, MssqlSchemaDiagnosticField,
    MssqlSchemaPlan, MssqlSchemaPlanOptions, MssqlStringPolicy, MssqlTargetOutputPlan,
    MssqlTargetTableState, MssqlTimestampPolicy, MssqlTimezonePolicy, MssqlUInt64Policy,
    mssql_schema_diagnostic_reports, plan_mssql_create_table_ddl, plan_mssql_lifecycle,
    plan_mssql_output_schema, plan_mssql_target_for_output, plan_mssql_target_for_resolved_output,
    plan_mssql_target_output,
};
pub use target::{
    LoadMode, MssqlConnectionConfig, MssqlConnectionSource, MssqlConnectionSummary,
    MssqlTargetConfig, MssqlTargetResolutionContext, MssqlTargetSummary, MssqlTargetTable,
    ResolvedMssqlTarget,
};
