use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;

use crate::{
    DeltaFunnelError, MssqlOutputBatchStream, MssqlOutputWriteJob, MssqlSchemaPlanOptions,
    MssqlWorkflowOutputWriter, MssqlWorkflowWriteReport, MssqlWriteBackend, MssqlWriteReport,
    ResolvedMssqlTarget, ValidationOptions, write_mssql_outputs_with_writer,
    write_output_batches_to_mssql_with_validation_options,
};

use super::super::super::{
    DeltaFunnelSession, PlannedMssqlOutput, query_handoff::SharedProviderReadStats,
};
use super::{
    MssqlDerivedCacheAliasPlan,
    cache_alias::{
        cache_error_with_restore_error, restore_mssql_cache_aliases,
        restore_mssql_cache_aliases_after_error,
    },
};

pub(super) struct MssqlWorkflowPublicOutputWriter;

#[async_trait]
impl MssqlWorkflowOutputWriter for MssqlWorkflowPublicOutputWriter {
    async fn write_output(
        &mut self,
        output_schema: SchemaRef,
        resolved_target: ResolvedMssqlTarget,
        schema_options: MssqlSchemaPlanOptions,
        batches: MssqlOutputBatchStream,
        write_backend: MssqlWriteBackend,
        validation_options: ValidationOptions,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        write_output_batches_to_mssql_with_validation_options(
            output_schema.as_ref(),
            resolved_target,
            schema_options,
            batches,
            write_backend,
            validation_options,
        )
        .await
    }
}

impl DeltaFunnelSession {
    #[allow(dead_code)]
    pub(crate) fn build_write_all_baseline_jobs(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
    ) -> Result<Vec<MssqlOutputWriteJob>, DeltaFunnelError> {
        self.build_write_all_baseline_jobs_with_provider_stats(planned_outputs, None)
    }

    pub(super) fn build_write_all_baseline_jobs_with_provider_stats(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        provider_stats: Option<SharedProviderReadStats>,
    ) -> Result<Vec<MssqlOutputWriteJob>, DeltaFunnelError> {
        planned_outputs
            .iter()
            .map(|planned| {
                let output_schema = Arc::clone(self.schema_for_lazy_table(planned.table())?);
                let batches = self.lazy_table_batch_stream_factory_with_provider_stats(
                    planned.table().clone(),
                    provider_stats.clone(),
                );

                Ok(MssqlOutputWriteJob::new(
                    output_schema,
                    planned.resolved_target().clone(),
                    planned.output_plan().schema_plan_options(),
                    batches,
                    self.options.mssql_write_backend(),
                    self.options.validation_options(),
                )
                .with_phase_timings(planned.phase_timings().to_vec()))
            })
            .collect()
    }

    #[allow(dead_code)]
    pub(super) fn build_write_all_cached_jobs(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        active_aliases: &[MssqlDerivedCacheAliasPlan],
    ) -> Result<Vec<MssqlOutputWriteJob>, DeltaFunnelError> {
        self.build_write_all_cached_jobs_with_provider_stats(planned_outputs, active_aliases, None)
    }

    pub(super) fn build_write_all_cached_jobs_with_provider_stats(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        active_aliases: &[MssqlDerivedCacheAliasPlan],
        provider_stats: Option<SharedProviderReadStats>,
    ) -> Result<Vec<MssqlOutputWriteJob>, DeltaFunnelError> {
        planned_outputs
            .iter()
            .map(|planned| {
                let output_schema = Arc::clone(self.schema_for_lazy_table(planned.table())?);
                let batches = self.cached_output_batch_stream_factory_with_provider_stats(
                    planned.request(),
                    active_aliases,
                    provider_stats.clone(),
                )?;

                Ok(MssqlOutputWriteJob::new(
                    output_schema,
                    planned.resolved_target().clone(),
                    planned.output_plan().schema_plan_options(),
                    batches,
                    self.options.mssql_write_backend(),
                    self.options.validation_options(),
                )
                .with_phase_timings(planned.phase_timings().to_vec()))
            })
            .collect()
    }

    #[allow(dead_code)]
    pub(crate) async fn write_all_baseline_with_writer<W>(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        writer: W,
    ) -> Result<MssqlWorkflowWriteReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        self.write_all_baseline_with_writer_and_provider_stats(planned_outputs, writer, None)
            .await
    }

    pub(super) async fn write_all_baseline_with_writer_and_provider_stats<W>(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        writer: W,
        provider_stats: Option<SharedProviderReadStats>,
    ) -> Result<MssqlWorkflowWriteReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        let jobs = self
            .build_write_all_baseline_jobs_with_provider_stats(planned_outputs, provider_stats)?;

        write_mssql_outputs_with_writer(jobs, self.options.mssql_workflow_options(), writer).await
    }

    /// Runs the auto-cache path with an injected workflow writer.
    ///
    /// Tests use this to inject a fake writer while the public path supplies a
    /// writer that calls the existing one-output SQL Server sink.
    #[allow(dead_code)]
    async fn write_all_cached_with_writer<W>(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        cache_aliases: &[MssqlDerivedCacheAliasPlan],
        writer: W,
    ) -> Result<MssqlWorkflowWriteReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        self.write_all_cached_with_writer_and_provider_stats(
            planned_outputs,
            cache_aliases,
            writer,
            None,
        )
        .await
    }

    pub(super) async fn write_all_cached_with_writer_and_provider_stats<W>(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        cache_aliases: &[MssqlDerivedCacheAliasPlan],
        writer: W,
        provider_stats: Option<SharedProviderReadStats>,
    ) -> Result<MssqlWorkflowWriteReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        let replacements = self.replace_mssql_cache_aliases(cache_aliases).await?;
        let jobs = match self.build_write_all_cached_jobs_with_provider_stats(
            planned_outputs,
            cache_aliases,
            provider_stats,
        ) {
            Ok(jobs) => jobs,
            Err(error) => {
                return Err(restore_mssql_cache_aliases_after_error(error, replacements).await);
            }
        };
        let write_result =
            write_mssql_outputs_with_writer(jobs, self.options.mssql_workflow_options(), writer)
                .await;
        let restore_result = restore_mssql_cache_aliases(replacements).await;

        match (write_result, restore_result) {
            (Ok(report), Ok(_restorations)) => Ok(report),
            (Ok(_report), Err(restore_error)) => Err(restore_error),
            (Err(write_error), Ok(_restorations)) => Err(write_error),
            (Err(write_error), Err(restore_error)) => {
                Err(cache_error_with_restore_error(write_error, restore_error))
            }
        }
    }

    #[allow(dead_code)]
    async fn write_all_baseline(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
    ) -> Result<MssqlWorkflowWriteReport, DeltaFunnelError> {
        self.write_all_baseline_with_writer(planned_outputs, MssqlWorkflowPublicOutputWriter)
            .await
    }

    #[allow(dead_code)]
    async fn write_all_cached(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        cache_aliases: &[MssqlDerivedCacheAliasPlan],
    ) -> Result<MssqlWorkflowWriteReport, DeltaFunnelError> {
        self.write_all_cached_with_writer(
            planned_outputs,
            cache_aliases,
            MssqlWorkflowPublicOutputWriter,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::super::super::super::{
        DeltaFunnelSession, SessionOptions,
        test_support::{
            execute_output_request, scan_counting_marker_region_provider, secret_connection,
        },
    };
    use crate::LoadMode;

    #[tokio::test]
    async fn build_write_all_baseline_jobs_preserves_output_metadata_without_stream_setup()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let west = session
            .table_from_sql("select marker from orders_source where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from orders_source where region = 'east'")
            .await?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east =
            execute_output_request(east, "east_output", "east_orders", LoadMode::CreateAndLoad)?;
        let planned = session.plan_write_all_outputs(&[west, east])?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);

        let jobs = session.build_write_all_baseline_jobs(&planned)?;

        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].output_name(), "west_output");
        assert_eq!(jobs[0].target_summary().table().table(), "west_orders");
        assert_eq!(jobs[0].phase_timings(), planned[0].phase_timings());
        assert_eq!(jobs[1].output_name(), "east_output");
        assert_eq!(jobs[1].target_summary().table().table(), "east_orders");
        assert_eq!(jobs[1].phase_timings(), planned[1].phase_timings());
        Ok(())
    }
}
