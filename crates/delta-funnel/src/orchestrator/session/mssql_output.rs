use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;

use crate::{
    DeltaFunnelError, MssqlOutputBatchStream, MssqlTargetOutputPlan, MssqlWriteOptions,
    MssqlWriteReport, ResolvedMssqlTarget, plan_mssql_target_for_resolved_output,
    write_output_batches_to_mssql,
};

use super::{DeltaFunnelSession, OutputWritePlan, PlannedMssqlOutput, RunMode};

impl DeltaFunnelSession {
    /// Plans one lazy table as an MSSQL output without executing the table.
    ///
    /// The selected table must be owned by this session. The method uses the
    /// table's logical Arrow schema, resolves the effective SQL Server
    /// connection from the output override or session default, and reuses the
    /// SQL Server schema, DDL, and lifecycle planning rules. It intentionally
    /// performs no SQL Server I/O, physical DataFusion planning, row reads,
    /// batch streaming, or writer construction.
    ///
    /// # Errors
    ///
    /// Returns an MSSQL planning error when the selected table is not known to
    /// this session, the target has no effective connection, the output or
    /// target config is invalid, the schema cannot be mapped to SQL Server, or
    /// the requested load mode is not supported by the current target planner.
    pub fn plan_mssql_output(
        &self,
        request: &OutputWritePlan,
    ) -> Result<PlannedMssqlOutput, DeltaFunnelError> {
        let schema = self.schema_for_lazy_table(request.table())?;
        let resolved_target =
            request
                .target()
                .target()
                .resolve(crate::MssqlTargetResolutionContext {
                    output_name: Some(request.target().output_name()),
                    default_connection: self.options.default_mssql_connection(),
                })?;
        let output_plan = plan_mssql_target_for_resolved_output(
            schema.as_ref(),
            &resolved_target,
            self.options.mssql_schema_options(),
        )?;

        Ok(PlannedMssqlOutput::new(
            request.clone(),
            resolved_target,
            output_plan,
        ))
    }

    /// Writes one selected lazy table to SQL Server.
    ///
    /// The method reuses the session output planner, builds a DataFusion physical
    /// plan for the selected lazy table, exposes DataFusion's merged
    /// `RecordBatch` stream, and hands that stream directly to the existing
    /// one-output MSSQL sink. It does not implement SQL Server lifecycle,
    /// writer, cleanup, retry, or stream buffering behavior itself.
    ///
    /// # Errors
    ///
    /// Returns the first planning, DataFusion stream setup, upstream stream, SQL
    /// Server connection, lifecycle, schema validation, write, or cleanup error.
    pub async fn write_to_mssql(
        &self,
        request: &OutputWritePlan,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        self.write_to_mssql_with_writer(request, &mut MssqlPublicOneOutputWriter)
            .await
    }

    pub(crate) async fn write_to_mssql_with_writer<W>(
        &self,
        request: &OutputWritePlan,
        writer: &mut W,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>
    where
        W: OrchestratorMssqlOutputWriter,
    {
        ensure_execute_run_mode(request.target().run_mode())?;
        let planned = self.plan_mssql_output(request)?;
        let output_schema = Arc::clone(self.schema_for_lazy_table(planned.table())?);
        let batches = self.batch_stream_for_lazy_table(planned.table()).await?;

        writer
            .write_output(
                output_schema,
                planned.output_plan().clone(),
                planned.resolved_target().clone(),
                batches,
                self.options.mssql_write_options(),
            )
            .await
    }
}

#[async_trait]
pub(crate) trait OrchestratorMssqlOutputWriter: Send {
    async fn write_output(
        &mut self,
        output_schema: SchemaRef,
        output_plan: MssqlTargetOutputPlan,
        resolved_target: ResolvedMssqlTarget,
        batches: MssqlOutputBatchStream,
        write_options: MssqlWriteOptions,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>;
}

struct MssqlPublicOneOutputWriter;

#[async_trait]
impl OrchestratorMssqlOutputWriter for MssqlPublicOneOutputWriter {
    async fn write_output(
        &mut self,
        output_schema: SchemaRef,
        output_plan: MssqlTargetOutputPlan,
        resolved_target: ResolvedMssqlTarget,
        batches: MssqlOutputBatchStream,
        write_options: MssqlWriteOptions,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        write_output_batches_to_mssql(
            output_schema.as_ref(),
            resolved_target,
            output_plan.schema_plan_options(),
            batches,
            write_options,
        )
        .await
    }
}

fn ensure_execute_run_mode(run_mode: RunMode) -> Result<(), DeltaFunnelError> {
    match run_mode {
        RunMode::Execute => Ok(()),
        RunMode::DryRun => Err(DeltaFunnelError::MssqlWorkflowPlanning {
            message:
                "write_to_mssql requires RunMode::Execute; use dry_run_to_mssql for dry-run planning"
                    .to_owned(),
        }),
    }
}
