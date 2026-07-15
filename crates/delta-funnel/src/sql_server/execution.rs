//! SQL Server connection, lifecycle, sink, and write execution modules.

#[allow(dead_code)]
mod connection;
mod lifecycle;
mod sink;
mod workflow;
mod write;

pub use crate::report::sql_server::{
    MssqlBatchShapingReport, MssqlOutputBatchValidationReport, MssqlOutputFieldReport,
    MssqlTargetCleanupStatus, MssqlWriteFailureContext, MssqlWriteReport, MssqlWriteStats,
};
pub(crate) use lifecycle::{MssqlConnectedLifecycleClient, table_name_from_target};
pub use lifecycle::{MssqlPreparedTarget, MssqlPreparedTargetAction, MssqlPreparedTargetReport};
pub use sink::write_output_batches_to_mssql;
pub(crate) use sink::{
    write_output_batches_to_mssql_for_workflow, write_planned_output_batches_to_mssql_for_workflow,
};
pub use workflow::{
    MssqlOutputBatchStream, MssqlOutputBatchStreamFactory, MssqlOutputWriteJob,
    MssqlOutputWriteStatus, MssqlWorkflowWriteOptions, MssqlWorkflowWriteReport,
    MssqlWriteFailureReport, MssqlWriteSkippedReason, MssqlWriteSkippedReport,
    write_mssql_outputs_to_mssql,
};
pub(crate) use workflow::{
    MssqlOutputProfileCallback, MssqlOutputQueryError, MssqlOutputQueryExecution,
    MssqlOutputQueryFuture, MssqlStreamBenchmarkOutputWriter, MssqlWorkflowOutputWriter,
    MssqlWorkflowSinkWriter, write_mssql_outputs_with_writer,
};
pub(crate) use write::{MssqlBulkLoadWriter, drain_mssql_batches_for_stream_benchmark};
pub use write::{
    MssqlWriteBackend, MssqlWritePhase, default_mssql_write_backend,
    mssql_write_backend_for_output_plan, validate_mssql_output_record_batch,
    validate_mssql_output_schema,
};

pub(super) use super::{
    LoadMode, MssqlConnectionConfig, MssqlConnectionSource, MssqlConnectionSummary,
    MssqlLifecycleExecutionGuardrail, MssqlSchemaPlanOptions, MssqlTargetOutputPlan,
    MssqlTargetSummary, MssqlTargetTable, MssqlTargetTableState, ResolvedMssqlTarget,
    plan_mssql_output_schema, plan_mssql_target_output,
};
