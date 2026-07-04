//! SQL Server target planning modules.

mod ddl;
mod lifecycle;
mod output;
mod schema;

pub use ddl::{MssqlDdlPlan, plan_mssql_create_table_ddl};
pub use lifecycle::{
    MssqlLifecycleExecutionGuardrail, MssqlLifecycleGuardrailPolicy, MssqlLifecyclePlan,
    MssqlTargetTableState, plan_mssql_lifecycle,
};
pub use output::{
    MssqlTargetOutputPlan, plan_mssql_target_for_output, plan_mssql_target_for_resolved_output,
    plan_mssql_target_output,
};
pub use schema::{
    MssqlBinaryPolicy, MssqlDate64Policy, MssqlDecimal256Policy, MssqlDecimalPolicy,
    MssqlFloatPolicy, MssqlNanosecondPolicy, MssqlSchemaDiagnostic, MssqlSchemaDiagnosticField,
    MssqlSchemaPlan, MssqlSchemaPlanOptions, MssqlStringPolicy, MssqlTimestampPolicy,
    MssqlTimezonePolicy, MssqlUInt64Policy, mssql_schema_diagnostic_reports,
    plan_mssql_output_schema,
};

pub(super) use super::{
    LoadMode, MssqlConnectionConfig, MssqlConnectionSource, MssqlConnectionSummary,
    MssqlTargetConfig, MssqlTargetResolutionContext, MssqlTargetSummary, MssqlTargetTable,
    ResolvedMssqlTarget, table_name_from_target,
};
