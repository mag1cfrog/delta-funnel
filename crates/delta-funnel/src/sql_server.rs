//! SQL Server target planning primitives.
//!
//! This module owns DeltaFunnel-side configuration and reporting shapes around
//! SQL Server targets. Schema mapping, identifier quoting, DDL rendering, and
//! Arrow-to-TDS writing remain owned by `arrow-tiberius`.

mod execution;
mod planning;
mod target;

pub(crate) use execution::MssqlBulkLoadWriter;
pub(crate) use execution::table_name_from_target;
pub use execution::{
    MssqlOutputBatchStream, MssqlOutputBatchStreamFactory, MssqlOutputBatchValidationReport,
    MssqlOutputWriteJob, MssqlOutputWriteStatus, MssqlPreparedTarget, MssqlPreparedTargetAction,
    MssqlPreparedTargetReport, MssqlTargetCleanupStatus, MssqlWorkflowWriteOptions,
    MssqlWorkflowWriteReport, MssqlWriteFailureContext, MssqlWriteFailureReport, MssqlWriteOptions,
    MssqlWritePhase, MssqlWriteReport, MssqlWriteSkippedReason, MssqlWriteSkippedReport,
    MssqlWriteStats, default_mssql_write_options, mssql_write_options_for_output_plan,
    validate_mssql_output_record_batch, validate_mssql_output_schema, write_mssql_outputs_to_mssql,
    write_output_batches_to_mssql,
};
pub(crate) use execution::{MssqlWorkflowOutputWriter, write_mssql_outputs_with_writer};
pub use planning::{
    MssqlBinaryPolicy, MssqlDate64Policy, MssqlDdlPlan, MssqlDecimal256Policy, MssqlDecimalPolicy,
    MssqlFloatPolicy, MssqlLifecycleExecutionGuardrail, MssqlLifecycleGuardrailPolicy,
    MssqlLifecyclePlan, MssqlNanosecondPolicy, MssqlSchemaDiagnostic, MssqlSchemaDiagnosticField,
    MssqlSchemaPlan, MssqlSchemaPlanOptions, MssqlStringPolicy, MssqlTargetOutputPlan,
    MssqlTargetTableState, MssqlTimezonePolicy, MssqlUInt64Policy, mssql_schema_diagnostic_reports,
    plan_mssql_create_table_ddl, plan_mssql_lifecycle, plan_mssql_output_schema,
    plan_mssql_target_for_output, plan_mssql_target_for_resolved_output, plan_mssql_target_output,
};
pub use target::{
    LoadMode, MssqlConnectionConfig, MssqlConnectionSource, MssqlConnectionSummary,
    MssqlTargetConfig, MssqlTargetResolutionContext, MssqlTargetSummary, MssqlTargetTable,
    ResolvedMssqlTarget,
};
