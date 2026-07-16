use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;

use crate::{
    DeltaFunnelError, ExecutionProfileMode, MssqlOutputQueryFuture, MssqlOutputWriteJob,
    MssqlWorkflowOutputWriter, MssqlWorkflowWriteReport, WriteAllCacheAliasReport,
    progress::ProgressReporter, report::OperationTimelineRecorder, usize_to_u64_saturating,
    write_mssql_outputs_with_writer,
};

use super::super::super::{
    DeltaFunnelSession, PlannedMssqlOutput, query_handoff::SharedProviderStatsSnapshots,
};
use super::{
    MssqlDerivedCacheAliasPlan,
    cache_alias::{
        restore_cache_aliases_after_failure, restore_mssql_cache_aliases_with_reports,
        write_all_cache_failure,
    },
};

impl DeltaFunnelSession {
    /// Combines one planned output, its resolved schema, and its deferred query
    /// execution into a write job.
    fn build_write_all_job(
        &self,
        planned: &PlannedMssqlOutput,
        output_schema: SchemaRef,
        create_query_execution: Box<dyn FnOnce() -> MssqlOutputQueryFuture + Send>,
        progress: Option<ProgressReporter>,
        timeline: Option<OperationTimelineRecorder>,
    ) -> MssqlOutputWriteJob {
        MssqlOutputWriteJob::new_with_query_execution_factory(
            output_schema,
            planned.resolved_target().clone(),
            planned.output_plan().schema_plan_options(),
            create_query_execution,
            self.options.mssql_write_backend(),
            self.options.validation_options(),
        )
        .with_phase_timings(planned.phase_timings().to_vec())
        .with_progress_reporter(progress)
        .with_operation_timeline(timeline)
    }

    /// Builds deferred uncached jobs and binds each optional progress reporter
    /// to that output's requested position.
    #[cfg(test)]
    pub(crate) fn build_write_all_uncached_jobs(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
        reporter: Option<&ProgressReporter>,
        profile_mode: ExecutionProfileMode,
    ) -> Result<Vec<MssqlOutputWriteJob>, DeltaFunnelError> {
        self.build_write_all_uncached_jobs_with_timeline(
            planned_outputs,
            provider_stats_snapshots,
            reporter,
            profile_mode,
            None,
        )
    }

    fn build_write_all_uncached_jobs_with_timeline(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
        reporter: Option<&ProgressReporter>,
        profile_mode: ExecutionProfileMode,
        timeline: Option<OperationTimelineRecorder>,
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
                let create_query_execution = self.mssql_output_query_factory_with_timeline(
                    planned.clone(),
                    provider_stats_snapshots.clone(),
                    progress.clone(),
                    profile_mode,
                    timeline.clone(),
                );

                Ok(self.build_write_all_job(
                    planned,
                    output_schema,
                    create_query_execution,
                    progress,
                    timeline.clone(),
                ))
            })
            .collect()
    }

    fn build_write_all_cached_jobs_with_timeline(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        active_aliases: &[MssqlDerivedCacheAliasPlan],
        provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
        reporter: Option<&ProgressReporter>,
        profile_mode: ExecutionProfileMode,
        timeline: Option<OperationTimelineRecorder>,
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
                let create_query_execution = self.cached_output_query_factory_with_timeline(
                    planned,
                    active_aliases,
                    provider_stats_snapshots.clone(),
                    progress.clone(),
                    profile_mode,
                    timeline.clone(),
                )?;

                Ok(self.build_write_all_job(
                    planned,
                    output_schema,
                    create_query_execution,
                    progress,
                    timeline.clone(),
                ))
            })
            .collect()
    }

    pub(super) async fn write_all_uncached_with_writer_and_timeline<W>(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        writer: W,
        provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
        reporter: Option<&ProgressReporter>,
        profile_mode: ExecutionProfileMode,
        timeline: Option<OperationTimelineRecorder>,
    ) -> Result<MssqlWorkflowWriteReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        let jobs = self.build_write_all_uncached_jobs_with_timeline(
            planned_outputs,
            provider_stats_snapshots,
            reporter,
            profile_mode,
            timeline,
        )?;

        write_mssql_outputs_with_writer(jobs, self.options.mssql_workflow_options(), writer).await
    }

    /// Runs the auto-cache path with an injected writer, optionally collecting
    /// provider statistics and reporting progress.
    #[cfg(test)]
    pub(super) async fn write_all_cached_with_writer<W>(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        cache_aliases: &[MssqlDerivedCacheAliasPlan],
        writer: W,
        provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
        reporter: Option<&ProgressReporter>,
        profile_mode: ExecutionProfileMode,
    ) -> Result<(MssqlWorkflowWriteReport, Vec<WriteAllCacheAliasReport>), DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        self.write_all_cached_with_writer_and_timeline(
            planned_outputs,
            cache_aliases,
            writer,
            provider_stats_snapshots,
            reporter,
            profile_mode,
            None,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "cached workflow execution carries cache, reporting, and profiling state"
    )]
    pub(super) async fn write_all_cached_with_writer_and_timeline<W>(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        cache_aliases: &[MssqlDerivedCacheAliasPlan],
        writer: W,
        provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
        reporter: Option<&ProgressReporter>,
        profile_mode: ExecutionProfileMode,
        timeline: Option<OperationTimelineRecorder>,
    ) -> Result<(MssqlWorkflowWriteReport, Vec<WriteAllCacheAliasReport>), DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        let replacements = self
            .replace_mssql_cache_aliases(cache_aliases, reporter, profile_mode, timeline.clone())
            .await?;
        let jobs = match self.build_write_all_cached_jobs_with_timeline(
            planned_outputs,
            cache_aliases,
            provider_stats_snapshots,
            reporter,
            profile_mode,
            timeline,
        ) {
            Ok(jobs) => jobs,
            Err(error) => {
                return Err(restore_cache_aliases_after_failure(
                    error,
                    replacements,
                    None,
                    reporter,
                ));
            }
        };
        let write_result =
            write_mssql_outputs_with_writer(jobs, self.options.mssql_workflow_options(), writer)
                .await;
        let (alias_reports, restore_result) =
            restore_mssql_cache_aliases_with_reports(replacements, reporter);

        match (write_result, restore_result) {
            (Ok(report), Ok(())) => Ok((report, alias_reports)),
            (Ok(report), Err((table_id, restore_error))) => Err(write_all_cache_failure(
                restore_error,
                alias_reports,
                Some(table_id),
                Some(report),
            )),
            (Err(write_error), _) => Err(write_all_cache_failure(
                write_error,
                alias_reports,
                None,
                None,
            )),
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
    use crate::{ExecutionProfileMode, LoadMode};

    #[tokio::test]
    async fn build_write_all_uncached_jobs_preserves_output_metadata_without_stream_setup()
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

        let jobs = session.build_write_all_uncached_jobs(
            &planned,
            None,
            None,
            ExecutionProfileMode::Disabled,
        )?;

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
