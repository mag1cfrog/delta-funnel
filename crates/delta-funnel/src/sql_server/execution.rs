//! SQL Server connection, lifecycle, sink, and write execution modules.

#[allow(dead_code)]
mod connection;
mod lifecycle;
mod sink;
mod workflow;
mod write;

pub(crate) use lifecycle::{MssqlConnectedLifecycleClient, table_name_from_target};
pub use lifecycle::{MssqlPreparedTarget, MssqlPreparedTargetAction, MssqlPreparedTargetReport};
pub use sink::write_output_batches_to_mssql;
pub use workflow::{
    MssqlOutputBatchStream, MssqlOutputBatchStreamFactory, MssqlOutputWriteJob,
    MssqlOutputWriteStatus, MssqlWorkflowWriteOptions, MssqlWorkflowWriteReport,
    MssqlWriteFailureReport, MssqlWriteSkippedReason, MssqlWriteSkippedReport,
    write_mssql_outputs_to_mssql,
};
pub(crate) use write::MssqlBulkLoadWriter;
pub use write::{
    MssqlOutputBatchValidationReport, MssqlTargetCleanupStatus, MssqlWriteFailureContext,
    MssqlWriteOptions, MssqlWritePhase, MssqlWriteReport, MssqlWriteStats,
    default_mssql_write_options, mssql_write_options_for_output_plan,
    validate_mssql_output_record_batch, validate_mssql_output_schema,
};

pub(super) use super::{
    LoadMode, MssqlConnectionConfig, MssqlConnectionSource, MssqlConnectionSummary,
    MssqlLifecycleExecutionGuardrail, MssqlSchemaPlanOptions, MssqlTargetOutputPlan,
    MssqlTargetSummary, MssqlTargetTable, MssqlTargetTableState, ResolvedMssqlTarget,
    plan_mssql_output_schema, plan_mssql_target_output,
};
