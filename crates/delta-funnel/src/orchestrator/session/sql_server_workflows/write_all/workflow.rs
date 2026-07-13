use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;

use crate::{
    DeltaFunnelError, MssqlOutputBatchStream, MssqlOutputBatchStreamFactory, MssqlOutputWriteJob,
    MssqlSchemaPlanOptions, MssqlWorkflowOutputWriter, MssqlWorkflowWriteReport, MssqlWriteBackend,
    MssqlWriteReport, ResolvedMssqlTarget, ValidationOptions, progress::ProgressReporter,
    usize_to_u64_saturating, write_mssql_outputs_with_writer,
    write_output_batches_to_mssql_for_workflow,
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
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        write_output_batches_to_mssql_for_workflow(
            output_schema.as_ref(),
            resolved_target,
            schema_options,
            batches,
            write_backend,
            validation_options,
            reporter,
        )
        .await
    }
}

impl DeltaFunnelSession {
    /// Combines one planned output, its resolved schema, and its deferred batch
    /// stream into a write job.
    fn build_write_all_job(
        &self,
        planned: &PlannedMssqlOutput,
        output_schema: SchemaRef,
        batches: MssqlOutputBatchStreamFactory,
        progress: Option<ProgressReporter>,
    ) -> MssqlOutputWriteJob {
        MssqlOutputWriteJob::new(
            output_schema,
            planned.resolved_target().clone(),
            planned.output_plan().schema_plan_options(),
            batches,
            self.options.mssql_write_backend(),
            self.options.validation_options(),
        )
        .with_phase_timings(planned.phase_timings().to_vec())
        .with_progress_reporter(progress)
    }

    /// Builds deferred baseline jobs and binds each optional progress reporter
    /// to that output's requested position.
    pub(crate) fn build_write_all_baseline_jobs(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        provider_stats: Option<SharedProviderReadStats>,
        reporter: Option<&ProgressReporter>,
    ) -> Result<Vec<MssqlOutputWriteJob>, DeltaFunnelError> {
        let output_count = usize_to_u64_saturating(planned_outputs.len());
        planned_outputs
            .iter()
            .enumerate()
            .map(|(output_index, planned)| {
                let output_schema = Arc::clone(self.schema_for_lazy_table(planned.table())?);
                let progress = reporter.and_then(|reporter| {
                    reporter.for_output(
                        usize_to_u64_saturating(output_index.saturating_add(1)),
                        output_count,
                    )
                });
                let batches = self.lazy_table_batch_stream_factory_for_write_all(
                    planned.table().clone(),
                    provider_stats.clone(),
                    progress.clone().map(|reporter| {
                        (reporter, planned.resolved_target().output_name().to_owned())
                    }),
                );

                Ok(self.build_write_all_job(planned, output_schema, batches, progress))
            })
            .collect()
    }

    /// Builds deferred cached jobs and binds each optional progress reporter to
    /// that output's requested position.
    pub(super) fn build_write_all_cached_jobs(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        active_aliases: &[MssqlDerivedCacheAliasPlan],
        provider_stats: Option<SharedProviderReadStats>,
        reporter: Option<&ProgressReporter>,
    ) -> Result<Vec<MssqlOutputWriteJob>, DeltaFunnelError> {
        let output_count = usize_to_u64_saturating(planned_outputs.len());
        planned_outputs
            .iter()
            .enumerate()
            .map(|(output_index, planned)| {
                let output_schema = Arc::clone(self.schema_for_lazy_table(planned.table())?);
                let progress = reporter.and_then(|reporter| {
                    reporter.for_output(
                        usize_to_u64_saturating(output_index.saturating_add(1)),
                        output_count,
                    )
                });
                let batches = self.cached_output_batch_stream_factory_for_write_all(
                    planned.request(),
                    active_aliases,
                    provider_stats.clone(),
                    progress.clone(),
                )?;

                Ok(self.build_write_all_job(planned, output_schema, batches, progress))
            })
            .collect()
    }

    /// Runs the baseline path with an injected writer, optionally collecting
    /// provider statistics and reporting progress.
    pub(crate) async fn write_all_baseline_with_writer<W>(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        writer: W,
        provider_stats: Option<SharedProviderReadStats>,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlWorkflowWriteReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        let jobs = self.build_write_all_baseline_jobs(planned_outputs, provider_stats, reporter)?;

        write_mssql_outputs_with_writer(jobs, self.options.mssql_workflow_options(), writer).await
    }

    /// Runs the auto-cache path with an injected writer, optionally collecting
    /// provider statistics and reporting progress.
    pub(super) async fn write_all_cached_with_writer<W>(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        cache_aliases: &[MssqlDerivedCacheAliasPlan],
        writer: W,
        provider_stats: Option<SharedProviderReadStats>,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlWorkflowWriteReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        let replacements = self
            .replace_mssql_cache_aliases(cache_aliases, reporter)
            .await?;
        let jobs = match self.build_write_all_cached_jobs(
            planned_outputs,
            cache_aliases,
            provider_stats,
            reporter,
        ) {
            Ok(jobs) => jobs,
            Err(error) => {
                return Err(
                    restore_mssql_cache_aliases_after_error(error, replacements, reporter).await,
                );
            }
        };
        let write_result =
            write_mssql_outputs_with_writer(jobs, self.options.mssql_workflow_options(), writer)
                .await;
        let restore_result = restore_mssql_cache_aliases(replacements, reporter).await;

        match (write_result, restore_result) {
            (Ok(report), Ok(_restorations)) => Ok(report),
            (Ok(_report), Err(restore_error)) => Err(restore_error),
            (Err(write_error), Ok(_restorations)) => Err(write_error),
            (Err(write_error), Err(restore_error)) => {
                Err(cache_error_with_restore_error(write_error, restore_error))
            }
        }
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

        let jobs = session.build_write_all_baseline_jobs(&planned, None, None)?;

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
