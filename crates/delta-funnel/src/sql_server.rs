//! SQL Server target planning primitives.
//!
//! This module owns DeltaFunnel-side configuration and reporting shapes around
//! SQL Server targets. Schema mapping, identifier quoting, DDL rendering, and
//! Arrow-to-TDS writing remain owned by `arrow-tiberius`.

mod schema_planning;
mod target;

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
