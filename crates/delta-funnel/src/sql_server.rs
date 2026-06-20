//! SQL Server target planning primitives.
//!
//! This module owns DeltaFunnel-side configuration and reporting shapes around
//! SQL Server targets. Schema mapping, identifier quoting, DDL rendering, and
//! Arrow-to-TDS writing remain owned by `arrow-tiberius`.

#[allow(dead_code)]
mod connection;
mod ddl_planning;
mod lifecycle_execution;
mod lifecycle_planning;
mod output_planning;
mod schema_planning;
mod target;
mod write;

pub use ddl_planning::{MssqlDdlPlan, plan_mssql_create_table_ddl};
pub(crate) use lifecycle_execution::{MssqlConnectedLifecycleClient, table_name_from_target};
pub use lifecycle_execution::{
    MssqlPreparedTarget, MssqlPreparedTargetAction, MssqlPreparedTargetReport,
};
pub use lifecycle_planning::{
    MssqlLifecycleExecutionGuardrail, MssqlLifecycleGuardrailPolicy, MssqlLifecyclePlan,
    MssqlTargetTableState, plan_mssql_lifecycle,
};
pub use output_planning::{
    MssqlTargetOutputPlan, plan_mssql_target_for_output, plan_mssql_target_output,
};
pub use schema_planning::{
    MssqlBinaryPolicy, MssqlDate64Policy, MssqlDecimal256Policy, MssqlDecimalPolicy,
    MssqlFloatPolicy, MssqlNanosecondPolicy, MssqlSchemaDiagnostic, MssqlSchemaDiagnosticField,
    MssqlSchemaPlan, MssqlSchemaPlanOptions, MssqlStringPolicy, MssqlTimezonePolicy,
    MssqlUInt64Policy, mssql_schema_diagnostic_reports, plan_mssql_output_schema,
};
pub use target::{
    LoadMode, MssqlConnectionConfig, MssqlConnectionSource, MssqlConnectionSummary,
    MssqlTargetConfig, MssqlTargetResolutionContext, MssqlTargetSummary, MssqlTargetTable,
    ResolvedMssqlTarget,
};
pub(crate) use write::MssqlBulkLoadWriter;
pub use write::{
    MssqlOutputBatchValidationReport, MssqlTargetCleanupStatus, MssqlWriteFailureContext,
    MssqlWriteOptions, MssqlWritePhase, MssqlWriteReport, MssqlWriteStats,
    default_mssql_write_options, mssql_write_options_for_output_plan,
    validate_mssql_output_record_batch, validate_mssql_output_schema,
};
