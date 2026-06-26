use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;

use crate::{
    DeltaFunnelError, MssqlOutputBatchStream, MssqlOutputWriteJob, MssqlSchemaPlanOptions,
    MssqlWorkflowOutputWriter, MssqlWorkflowWriteReport, MssqlWriteOptions, MssqlWriteReport,
    ResolvedMssqlTarget, support::sanitize_text_for_display, write_mssql_outputs_with_writer,
    write_output_batches_to_mssql,
};

use super::super::{
    DeltaFunnelSession, OutputWritePlan, PlannedMssqlOutput, RunMode,
    streams::{SharedProviderReadStats, provider_read_stats_snapshot, shared_provider_read_stats},
};

mod cache_alias;
mod cache_plan;
mod report;

#[cfg(test)]
use cache_alias::MssqlScopedCacheAliasReplacement;
use cache_alias::{
    cache_error_with_restore_error, restore_mssql_cache_aliases,
    restore_mssql_cache_aliases_after_error,
};
pub(crate) use cache_plan::{
    MssqlCacheCandidateSkip, MssqlCacheCandidateSkipReason, MssqlCachedOutputStreamRoute,
    MssqlDerivedCacheAliasPlan, MssqlNoCacheReason, MssqlOutputCacheDecision, MssqlOutputCachePlan,
};
pub use report::{
    WriteAllCacheAliasReport, WriteAllCacheAliasStatus, WriteAllCacheCandidateSkip,
    WriteAllCacheCandidateSkipReason, WriteAllCacheReport, WriteAllNoCacheReason, WriteAllReport,
};

struct MssqlWorkflowPublicOutputWriter;

#[async_trait]
impl MssqlWorkflowOutputWriter for MssqlWorkflowPublicOutputWriter {
    async fn write_output(
        &mut self,
        output_schema: SchemaRef,
        resolved_target: ResolvedMssqlTarget,
        schema_options: MssqlSchemaPlanOptions,
        batches: MssqlOutputBatchStream,
        write_options: MssqlWriteOptions,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        write_output_batches_to_mssql(
            output_schema.as_ref(),
            resolved_target,
            schema_options,
            batches,
            write_options,
        )
        .await
    }
}

/// Cache policy for one multi-output `write_all` call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WriteAllCacheMode {
    /// Select and materialize conservative shared derived aliases when safe.
    #[default]
    Auto,
    /// Use the baseline sequential workflow without cache planning or materialization.
    Disabled,
}

/// Execution options for one multi-output `write_all` call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteAllOptions {
    cache_mode: WriteAllCacheMode,
}

impl WriteAllOptions {
    /// Creates default `write_all` options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cache_mode: WriteAllCacheMode::Auto,
        }
    }

    /// Sets the cache policy for this `write_all` call.
    #[must_use]
    pub const fn with_cache_mode(mut self, cache_mode: WriteAllCacheMode) -> Self {
        self.cache_mode = cache_mode;
        self
    }

    /// Returns the cache policy for this `write_all` call.
    #[must_use]
    pub const fn cache_mode(&self) -> WriteAllCacheMode {
        self.cache_mode
    }
}

impl DeltaFunnelSession {
    #[allow(dead_code)]
    pub(crate) fn plan_write_all_outputs(
        &self,
        requests: &[OutputWritePlan],
    ) -> Result<Vec<PlannedMssqlOutput>, DeltaFunnelError> {
        ensure_unique_write_all_output_names(requests)?;

        requests
            .iter()
            .map(|request| {
                ensure_write_all_execute_run_mode(request.target().run_mode())?;
                self.plan_mssql_output(request)
            })
            .collect()
    }

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
                    self.options.mssql_write_options(),
                ))
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
                    self.options.mssql_write_options(),
                ))
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

    async fn write_all_baseline_with_writer_and_provider_stats<W>(
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

    async fn write_all_cached_with_writer_and_provider_stats<W>(
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
    pub(crate) async fn write_all_with_options_and_writer<W>(
        &self,
        requests: &[OutputWritePlan],
        options: WriteAllOptions,
        writer: W,
    ) -> Result<WriteAllReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        let planned_outputs = self.plan_write_all_outputs(requests)?;

        match options.cache_mode() {
            WriteAllCacheMode::Auto => {
                self.write_all_auto_with_writer(requests, &planned_outputs, writer)
                    .await
            }
            WriteAllCacheMode::Disabled => {
                let provider_stats = shared_provider_read_stats();
                let workflow = self
                    .write_all_baseline_with_writer_and_provider_stats(
                        &planned_outputs,
                        writer,
                        Some(Arc::clone(&provider_stats)),
                    )
                    .await?;
                let sources = self.source_reports_for_planned_outputs_with_provider_stats(
                    &planned_outputs,
                    provider_read_stats_snapshot(&provider_stats),
                )?;
                Ok(WriteAllReport::new(
                    workflow,
                    WriteAllCacheReport::disabled(),
                    sources,
                ))
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn write_all_with_writer<W>(
        &self,
        requests: &[OutputWritePlan],
        writer: W,
    ) -> Result<WriteAllReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        self.write_all_with_options_and_writer(requests, WriteAllOptions::default(), writer)
            .await
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
    async fn write_all_auto_with_writer<W>(
        &self,
        requests: &[OutputWritePlan],
        planned_outputs: &[PlannedMssqlOutput],
        writer: W,
    ) -> Result<WriteAllReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        let cache_plan = self.plan_mssql_output_cache(requests);

        self.write_all_auto_plan_with_writer(planned_outputs, &cache_plan, writer)
            .await
    }

    #[allow(dead_code)]
    async fn write_all_auto_plan_with_writer<W>(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        cache_plan: &MssqlOutputCachePlan,
        writer: W,
    ) -> Result<WriteAllReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        match cache_plan.decision() {
            MssqlOutputCacheDecision::NoCache { .. } => {
                let cache = WriteAllCacheReport::from_plan(cache_plan);
                let provider_stats = shared_provider_read_stats();
                let workflow = self
                    .write_all_baseline_with_writer_and_provider_stats(
                        planned_outputs,
                        writer,
                        Some(Arc::clone(&provider_stats)),
                    )
                    .await?;
                let sources = self.source_reports_for_planned_outputs_with_provider_stats(
                    planned_outputs,
                    provider_read_stats_snapshot(&provider_stats),
                )?;
                Ok(WriteAllReport::new(workflow, cache, sources))
            }
            MssqlOutputCacheDecision::CacheAliases(cache_aliases) => {
                let provider_stats = shared_provider_read_stats();
                let workflow = self
                    .write_all_cached_with_writer_and_provider_stats(
                        planned_outputs,
                        cache_aliases,
                        writer,
                        Some(Arc::clone(&provider_stats)),
                    )
                    .await?;
                let cache = WriteAllCacheReport::from_executed_plan(cache_plan);
                let sources = self.source_reports_for_planned_outputs_with_provider_stats(
                    planned_outputs,
                    provider_read_stats_snapshot(&provider_stats),
                )?;
                Ok(WriteAllReport::new(workflow, cache, sources))
            }
        }
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

    async fn write_all_auto(
        &self,
        requests: &[OutputWritePlan],
        planned_outputs: &[PlannedMssqlOutput],
    ) -> Result<WriteAllReport, DeltaFunnelError> {
        let cache_plan = self.plan_mssql_output_cache(requests);

        self.write_all_auto_plan(planned_outputs, &cache_plan).await
    }

    async fn write_all_auto_plan(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        cache_plan: &MssqlOutputCachePlan,
    ) -> Result<WriteAllReport, DeltaFunnelError> {
        match cache_plan.decision() {
            MssqlOutputCacheDecision::NoCache { .. } => {
                let cache = WriteAllCacheReport::from_plan(cache_plan);
                let provider_stats = shared_provider_read_stats();
                let workflow = self
                    .write_all_baseline_with_writer_and_provider_stats(
                        planned_outputs,
                        MssqlWorkflowPublicOutputWriter,
                        Some(Arc::clone(&provider_stats)),
                    )
                    .await?;
                let sources = self.source_reports_for_planned_outputs_with_provider_stats(
                    planned_outputs,
                    provider_read_stats_snapshot(&provider_stats),
                )?;
                Ok(WriteAllReport::new(workflow, cache, sources))
            }
            MssqlOutputCacheDecision::CacheAliases(cache_aliases) => {
                let provider_stats = shared_provider_read_stats();
                let workflow = self
                    .write_all_cached_with_writer_and_provider_stats(
                        planned_outputs,
                        cache_aliases,
                        MssqlWorkflowPublicOutputWriter,
                        Some(Arc::clone(&provider_stats)),
                    )
                    .await?;
                let cache = WriteAllCacheReport::from_executed_plan(cache_plan);
                let sources = self.source_reports_for_planned_outputs_with_provider_stats(
                    planned_outputs,
                    provider_read_stats_snapshot(&provider_stats),
                )?;
                Ok(WriteAllReport::new(workflow, cache, sources))
            }
        }
    }

    /// Writes multiple selected lazy tables to SQL Server sequentially.
    ///
    /// The default mode performs conservative automatic cache planning for
    /// shared registered derived aliases. The returned report wraps the
    /// lower-level SQL Server workflow report and includes cache selection
    /// metadata for this call.
    ///
    /// # Errors
    ///
    /// Returns the first pre-execution validation/planning error, or a workflow
    /// execution error from the lower-level SQL Server workflow.
    #[allow(dead_code)]
    pub async fn write_all(
        &self,
        requests: &[OutputWritePlan],
    ) -> Result<WriteAllReport, DeltaFunnelError> {
        self.write_all_with_options(requests, WriteAllOptions::default())
            .await
    }

    /// Writes multiple selected lazy tables to SQL Server sequentially with explicit options.
    ///
    /// `WriteAllCacheMode::Disabled` uses the baseline no-cache path. The
    /// default `Auto` mode performs conservative shared-cache planning and
    /// reports the selected or skipped cache decision.
    ///
    /// # Errors
    ///
    /// Returns the first pre-execution validation/planning error, or a workflow
    /// execution error from the lower-level SQL Server workflow.
    #[allow(dead_code)]
    pub async fn write_all_with_options(
        &self,
        requests: &[OutputWritePlan],
        options: WriteAllOptions,
    ) -> Result<WriteAllReport, DeltaFunnelError> {
        let planned_outputs = self.plan_write_all_outputs(requests)?;

        match options.cache_mode() {
            WriteAllCacheMode::Auto => self.write_all_auto(requests, &planned_outputs).await,
            WriteAllCacheMode::Disabled => {
                let provider_stats = shared_provider_read_stats();
                let workflow = self
                    .write_all_baseline_with_writer_and_provider_stats(
                        &planned_outputs,
                        MssqlWorkflowPublicOutputWriter,
                        Some(Arc::clone(&provider_stats)),
                    )
                    .await?;
                let sources = self.source_reports_for_planned_outputs_with_provider_stats(
                    &planned_outputs,
                    provider_read_stats_snapshot(&provider_stats),
                )?;
                Ok(WriteAllReport::new(
                    workflow,
                    WriteAllCacheReport::disabled(),
                    sources,
                ))
            }
        }
    }
}

fn ensure_write_all_execute_run_mode(run_mode: RunMode) -> Result<(), DeltaFunnelError> {
    match run_mode {
        RunMode::Execute => Ok(()),
        RunMode::DryRun => Err(DeltaFunnelError::MssqlWorkflowPlanning {
            message:
                "write_all requires RunMode::Execute; use dry_run_all_to_mssql for dry-run planning"
                    .to_owned(),
        }),
    }
}

pub(crate) fn ensure_unique_write_all_output_names(
    requests: &[OutputWritePlan],
) -> Result<(), DeltaFunnelError> {
    let mut output_names = BTreeSet::new();
    for request in requests {
        let output_name = request.target().output_name();
        if !output_names.insert(output_name) {
            return Err(DeltaFunnelError::MssqlWorkflowPlanning {
                message: format!(
                    "write_all output names must be unique; duplicate output name `{}`",
                    sanitize_text_for_display(output_name)
                ),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, atomic::Ordering};

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use futures_util::StreamExt;

    use super::super::super::{
        DeltaFunnelSession, LazyTable, LazyTableKind, MssqlOutputTarget, OutputWritePlan, RunMode,
        SessionOptions, SourceUsageStatus,
        registry::{DerivedTableDependency, DerivedTableLineage},
        test_support::{
            DeltaLogTable, collect_stream_marker_values, collect_stream_row_count,
            execute_output_request, failing_scan_marker_region_provider, marker_region_provider,
            marker_values_from_batches, output_request, override_connection,
            scan_counting_marker_region_provider, secret_connection,
        },
    };
    use super::{
        MssqlCacheCandidateSkipReason, MssqlCachedOutputStreamRoute, MssqlDerivedCacheAliasPlan,
        MssqlNoCacheReason, MssqlOutputCacheDecision, MssqlScopedCacheAliasReplacement,
        WriteAllCacheAliasStatus, WriteAllCacheMode, WriteAllCacheReport, WriteAllNoCacheReason,
        WriteAllOptions, cache_error_with_restore_error, restore_mssql_cache_aliases_after_error,
    };
    use crate::{
        DeltaFunnelError, DeltaSourceConfig, LoadMode, MssqlOutputBatchStream,
        MssqlSchemaPlanOptions, MssqlTargetCleanupStatus, MssqlTargetConfig, MssqlTargetTable,
        MssqlWorkflowOutputWriter, MssqlWriteOptions, MssqlWriteReport, ResolvedMssqlTarget,
        plan_mssql_target_for_resolved_output, table_formats::RealParquetDeltaTable,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct FakeOrchestratorWriteCall {
        output_name: String,
        target_table: MssqlTargetTable,
        rows: u64,
        batches: u64,
    }

    #[derive(Clone, Default)]
    struct FakeWorkflowWriter {
        calls: Arc<Mutex<Vec<FakeOrchestratorWriteCall>>>,
        fail_output_name: Option<String>,
    }

    impl FakeWorkflowWriter {
        fn failing_on(output_name: &str) -> Self {
            Self {
                fail_output_name: Some(output_name.to_owned()),
                ..Self::default()
            }
        }

        fn calls(&self) -> Arc<Mutex<Vec<FakeOrchestratorWriteCall>>> {
            Arc::clone(&self.calls)
        }
    }

    #[async_trait]
    impl MssqlWorkflowOutputWriter for FakeWorkflowWriter {
        async fn write_output(
            &mut self,
            output_schema: SchemaRef,
            resolved_target: ResolvedMssqlTarget,
            schema_options: MssqlSchemaPlanOptions,
            mut batches: MssqlOutputBatchStream,
            _write_options: MssqlWriteOptions,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            let mut rows = 0_u64;
            let mut batch_count = 0_u64;

            while let Some(batch) = batches.next().await {
                let batch = batch?;
                rows = rows.saturating_add(u64::try_from(batch.num_rows()).map_err(|_| {
                    DeltaFunnelError::Config {
                        message: "fake workflow writer row count overflowed u64".to_owned(),
                    }
                })?);
                batch_count = batch_count.saturating_add(1);
            }

            let output_plan = plan_mssql_target_for_resolved_output(
                output_schema.as_ref(),
                &resolved_target,
                schema_options,
            )?;
            self.calls
                .lock()
                .map_err(|_| DeltaFunnelError::Config {
                    message: "fake workflow writer call lock poisoned".to_owned(),
                })?
                .push(FakeOrchestratorWriteCall {
                    output_name: resolved_target.output_name().to_owned(),
                    target_table: resolved_target.table().clone(),
                    rows,
                    batches: batch_count,
                });

            if self
                .fail_output_name
                .as_deref()
                .is_some_and(|output_name| output_name == resolved_target.output_name())
            {
                return Err(DeltaFunnelError::MssqlWorkflowPlanning {
                    message: format!(
                        "fake workflow writer failed for `{}`",
                        resolved_target.output_name()
                    ),
                });
            }

            Ok(MssqlWriteReport::from_output_plan(
                &output_plan,
                rows,
                batch_count,
                0,
                false,
                MssqlTargetCleanupStatus::NotApplicable,
            ))
        }
    }

    #[test]
    fn write_all_options_default_to_auto_cache_mode() {
        let options = WriteAllOptions::default();

        assert_eq!(options.cache_mode(), WriteAllCacheMode::Auto);
        assert_eq!(
            WriteAllOptions::new()
                .with_cache_mode(WriteAllCacheMode::Disabled)
                .cache_mode(),
            WriteAllCacheMode::Disabled
        );
    }

    #[tokio::test]
    async fn plan_write_all_outputs_plans_valid_outputs_in_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east =
            execute_output_request(east, "east_output", "east_orders", LoadMode::CreateAndLoad)?;

        let planned = session.plan_write_all_outputs(&[west, east])?;

        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].output_plan().output_name(), "west_output");
        assert_eq!(
            planned[0].output_plan().target_table().table(),
            "west_orders"
        );
        assert_eq!(planned[1].output_plan().output_name(), "east_output");
        assert_eq!(
            planned[1].output_plan().target_table().table(),
            "east_orders"
        );
        Ok(())
    }

    #[tokio::test]
    async fn plan_write_all_outputs_rejects_duplicate_output_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west = execute_output_request(
            west,
            "orders_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = execute_output_request(
            east,
            "orders_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let error = session.plan_write_all_outputs(&[west, east]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("write_all output names must be unique")
                    && message.contains("orders_output")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn plan_write_all_outputs_rejects_missing_connection_before_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let error = session.plan_write_all_outputs(&[west, east]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "west_output"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn plan_write_all_outputs_rejects_replace_before_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = execute_output_request(east, "east_output", "east_orders", LoadMode::Replace)?;

        let error = session.plan_write_all_outputs(&[west, east]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlLifecyclePlanning { output_name, message })
                if output_name == "east_output"
                    && message.contains("replace load mode is reserved")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn plan_write_all_outputs_rejects_dry_run_before_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;

        let error = session.plan_write_all_outputs(&[west]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("write_all requires RunMode::Execute")
                    && message.contains("dry_run_all_to_mssql")
        ));
        Ok(())
    }

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
        assert_eq!(jobs[1].output_name(), "east_output");
        assert_eq!(jobs[1].target_summary().table().table(), "east_orders");
        Ok(())
    }

    #[tokio::test]
    async fn write_all_with_writer_executes_valid_outputs_in_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session
            .table_from_sql("select 1 as id union all select 2 as id")
            .await?;
        let east = session.table_from_sql("select 3 as id").await?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east =
            execute_output_request(east, "east_output", "east_orders", LoadMode::CreateAndLoad)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session.write_all_with_writer(&[west, east], writer).await?;
        let calls = calls
            .lock()
            .map_err(|_| "fake workflow call lock poisoned")?;

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].output_name, "west_output");
        assert_eq!(calls[0].target_table.table(), "west_orders");
        assert_eq!(calls[0].rows, 2);
        assert_eq!(calls[1].output_name, "east_output");
        assert_eq!(calls[1].target_table.table(), "east_orders");
        assert_eq!(calls[1].rows, 1);
        assert_eq!(report.len(), 2);
        assert!(report.all_succeeded());
        assert_eq!(report.outputs()[0].output_name(), "west_output");
        assert_eq!(report.outputs()[1].output_name(), "east_output");
        assert_eq!(report.workflow().outputs(), report.outputs());
        let crate::sql_server::MssqlOutputWriteStatus::Succeeded(west_report) =
            &report.outputs()[0]
        else {
            return Err(format!("expected succeeded status, got {:?}", report.outputs()[0]).into());
        };
        assert_eq!(west_report.stats().rows_written(), 2);
        assert_eq!(west_report.stats().batches_written(), calls[0].batches);
        assert_eq!(
            west_report.cleanup(),
            MssqlTargetCleanupStatus::NotApplicable
        );
        Ok(())
    }

    #[tokio::test]
    async fn write_all_with_writer_reports_delta_sources_for_executed_outputs()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let selected_orders = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let request = execute_output_request(
            selected_orders,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let writer = FakeWorkflowWriter::default();

        let report = session
            .write_all_with_options_and_writer(
                &[request],
                WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                writer,
            )
            .await?;

        assert_eq!(report.outputs().len(), 1);
        assert_eq!(report.outputs()[0].output_name(), "orders_output");
        assert_eq!(report.sources().len(), 1);
        let source = &report.sources()[0];
        assert_eq!(source.source_name(), "orders");
        assert_eq!(source.usage_status(), SourceUsageStatus::Used);
        assert_eq!(source.used_by_output_names(), &["orders_output".to_owned()]);
        assert_eq!(source.provider_stats_reason(), None);
        let stats = source
            .provider_read_stats()
            .ok_or("expected execution provider stats")?;
        assert_eq!(stats.source_name, "orders");
        assert_eq!(stats.snapshot_version, source.snapshot_version());
        assert!(stats.files_started > 0);
        assert_eq!(stats.files_started, stats.files_completed);
        assert!(stats.rows_produced > 0);
        assert!(stats.batches_produced > 0);
        match stats.scan_metadata_exhausted {
            Some(true) => {
                assert_eq!(
                    source.file_count(),
                    crate::FileCount::exact(stats.files_planned)
                );
                assert_eq!(source.file_count_reason(), None);
            }
            Some(false) => {
                assert_eq!(
                    source.file_count(),
                    crate::FileCount::estimated(stats.files_planned)
                );
                assert_eq!(source.file_count_reason(), None);
            }
            None => {
                assert_eq!(source.file_count(), crate::FileCount::unavailable());
                assert_eq!(
                    source.file_count_reason(),
                    Some(crate::ReportReasonCode::CapabilityUnavailable)
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn write_all_keeps_source_rows_separate_from_output_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let aggregate = session
            .table_from_sql("select count(*) as order_count from orders")
            .await?;
        let request = execute_output_request(
            aggregate,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let writer = FakeWorkflowWriter::default();

        let report = session
            .write_all_with_options_and_writer(
                &[request],
                WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                writer,
            )
            .await?;

        let crate::sql_server::MssqlOutputWriteStatus::Succeeded(output_report) =
            &report.outputs()[0]
        else {
            return Err(format!("expected succeeded status, got {:?}", report.outputs()[0]).into());
        };
        assert_eq!(output_report.stats().rows_written(), 1);
        let source = report
            .sources()
            .first()
            .ok_or("expected executed source report")?;
        let stats = source
            .provider_read_stats()
            .ok_or("expected execution provider stats")?;
        assert_eq!(stats.rows_produced, u64::try_from(table.rows())?);
        assert_ne!(stats.rows_produced, output_report.stats().rows_written());
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_no_candidate_uses_baseline_path()
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
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session.write_all_with_writer(&[west, east], writer).await?;
        let calls = calls
            .lock()
            .map_err(|_| "fake workflow call lock poisoned")?;

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].output_name, "west_output");
        assert_eq!(calls[0].rows, 1);
        assert_eq!(calls[1].output_name, "east_output");
        assert_eq!(calls[1].rows, 1);
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        assert!(report.all_succeeded());
        assert!(matches!(
            report.cache(),
            WriteAllCacheReport::NoCache {
                reason: WriteAllNoCacheReason::NoSharedRegisteredDerivedAlias,
                skipped_candidates
            } if skipped_candidates.is_empty()
        ));
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_caches_shared_alias_for_direct_and_dependent_outputs()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output = execute_output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[big_output, west_output], writer)
            .await?;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0].output_name, "big_output");
            assert_eq!(calls[0].rows, 2);
            assert_eq!(calls[1].output_name, "west_output");
            assert_eq!(calls[1].rows, 1);
            assert_eq!(source_scans.load(Ordering::SeqCst), 1);
            assert!(report.all_succeeded());
        }
        let WriteAllCacheReport::CacheAliases {
            aliases,
            skipped_candidates,
        } = report.cache()
        else {
            return Err(format!("expected cache aliases report, got {:?}", report.cache()).into());
        };
        assert!(skipped_candidates.is_empty());
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].table_id(), big.id());
        assert_eq!(aliases[0].alias(), "big");
        assert_eq!(aliases[0].output_indexes(), &[0, 1]);
        assert_eq!(
            aliases[0].status(),
            WriteAllCacheAliasStatus::MaterializedAndRestored
        );

        let restored_big_factory = session.lazy_table_batch_stream_factory(big);
        let restored_big_rows = collect_stream_row_count(restored_big_factory().await?).await?;
        assert_eq!(restored_big_rows, 2);
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_report_debug_redacts_connections_and_retained_sql()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, _source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql(
                "select 'super-secret-literal' as marker, region \
                 from big_source",
            )
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output =
            execute_output_request(big, "big_output", "big_orders", LoadMode::AppendExisting)?;
        let override_target_config =
            MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "west_orders")?)
                .with_load_mode(LoadMode::AppendExisting)
                .with_connection(override_connection()?);
        let west_output = OutputWritePlan::new(
            west,
            MssqlOutputTarget::new("west_output", override_target_config, RunMode::Execute),
        );
        let writer = FakeWorkflowWriter::default();

        let report = session
            .write_all_with_writer(&[big_output, west_output], writer)
            .await?;

        let debug = format!("{report:?}");
        assert!(debug.contains("warehouse-primary"));
        assert!(debug.contains("warehouse-override"));
        assert!(debug.contains("CacheAliases"));
        assert!(debug.contains("MaterializedAndRestored"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("override-secret"));
        assert!(!debug.contains("super-secret-literal"));
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_caches_multiple_shared_aliases_for_dependent_outputs()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (big_source_provider, big_source_scans) = scan_counting_marker_region_provider("big")?;
        let (names_source_provider, names_source_scans) =
            scan_counting_marker_region_provider("names")?;
        session
            .context()
            .register_table("big_source", big_source_provider)?;
        session
            .context()
            .register_table("names_source", names_source_provider)?;
        let pending_big = session
            .table_from_sql(
                "select marker as big_marker, region, \
                 case when region = 'west' then 1 else 2 end as id \
                 from big_source",
            )
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql(
                "select marker as name_marker, region, \
                 case when region = 'west' then 1 else 2 end as id \
                 from names_source",
            )
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let west = session
            .table_from_sql(
                "select big.id, big.big_marker, names.name_marker \
                 from big join names on big.id = names.id \
                 where big.region = 'west'",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.id, big.big_marker, names.name_marker \
                 from big join names on big.id = names.id \
                 where big.region = 'east'",
            )
            .await?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east_output =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[west_output, east_output], writer)
            .await?;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0].output_name, "west_output");
            assert_eq!(calls[0].rows, 1);
            assert_eq!(calls[1].output_name, "east_output");
            assert_eq!(calls[1].rows, 1);
            assert_eq!(big_source_scans.load(Ordering::SeqCst), 1);
            assert_eq!(names_source_scans.load(Ordering::SeqCst), 1);
            assert!(report.all_succeeded());
        }
        let WriteAllCacheReport::CacheAliases {
            aliases,
            skipped_candidates,
        } = report.cache()
        else {
            return Err(format!("expected cache aliases report, got {:?}", report.cache()).into());
        };
        assert!(skipped_candidates.is_empty());
        assert_eq!(aliases.len(), 2);
        assert_eq!(aliases[0].table_id(), big.id());
        assert_eq!(aliases[0].alias(), "big");
        assert_eq!(aliases[0].output_indexes(), &[0, 1]);
        assert_eq!(
            aliases[0].status(),
            WriteAllCacheAliasStatus::MaterializedAndRestored
        );
        assert_eq!(aliases[1].table_id(), names.id());
        assert_eq!(aliases[1].alias(), "names");
        assert_eq!(aliases[1].output_indexes(), &[0, 1]);
        assert_eq!(
            aliases[1].status(),
            WriteAllCacheAliasStatus::MaterializedAndRestored
        );

        let restored_big_factory = session.lazy_table_batch_stream_factory(big);
        let restored_names_factory = session.lazy_table_batch_stream_factory(names);
        assert_eq!(
            collect_stream_row_count(restored_big_factory().await?).await?,
            2
        );
        assert_eq!(
            collect_stream_row_count(restored_names_factory().await?).await?,
            2
        );
        assert_eq!(big_source_scans.load(Ordering::SeqCst), 2);
        assert_eq!(names_source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_keeps_unrelated_output_on_normal_stream_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (shared_provider, shared_scans) = scan_counting_marker_region_provider("shared")?;
        let (unrelated_provider, unrelated_scans) =
            scan_counting_marker_region_provider("unrelated")?;
        session
            .context()
            .register_table("big_source", shared_provider)?;
        session
            .context()
            .register_table("unrelated_source", unrelated_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let unrelated = session
            .table_from_sql("select marker from unrelated_source where region = 'west'")
            .await?;
        let big_output =
            execute_output_request(big, "big_output", "big_orders", LoadMode::AppendExisting)?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let unrelated_output = execute_output_request(
            unrelated,
            "unrelated_output",
            "unrelated_orders",
            LoadMode::AppendExisting,
        )?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[big_output, unrelated_output, west_output], writer)
            .await?;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 3);
            assert_eq!(calls[0].output_name, "big_output");
            assert_eq!(calls[0].rows, 2);
            assert_eq!(calls[1].output_name, "unrelated_output");
            assert_eq!(calls[1].rows, 1);
            assert_eq!(calls[2].output_name, "west_output");
            assert_eq!(calls[2].rows, 1);
            assert_eq!(shared_scans.load(Ordering::SeqCst), 1);
            assert_eq!(unrelated_scans.load(Ordering::SeqCst), 1);
            assert!(report.all_succeeded());
        }
        Ok(())
    }

    #[tokio::test]
    async fn write_all_disabled_cache_mode_uses_baseline_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output =
            execute_output_request(big, "big_output", "big_orders", LoadMode::AppendExisting)?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session
            .write_all_with_options_and_writer(
                &[big_output, west_output],
                WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                writer,
            )
            .await?;
        let calls = calls
            .lock()
            .map_err(|_| "fake workflow call lock poisoned")?;

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].rows, 2);
        assert_eq!(calls[1].rows, 1);
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        assert!(report.all_succeeded());
        assert_eq!(report.cache(), &WriteAllCacheReport::Disabled);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_restores_cache_alias_after_output_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;
        let big_output = execute_output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east_output =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::failing_on("west_output");
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[big_output, west_output, east_output], writer)
            .await?;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0].output_name, "big_output");
            assert_eq!(calls[0].rows, 2);
            assert_eq!(calls[1].output_name, "west_output");
            assert_eq!(calls[1].rows, 1);
            assert_eq!(source_scans.load(Ordering::SeqCst), 1);
            assert_eq!(report.succeeded_count(), 1);
            assert_eq!(report.failed_count(), 1);
            assert_eq!(report.skipped_count(), 1);
            assert!(report.outputs()[0].is_succeeded());
            assert_eq!(report.outputs()[0].output_name(), "big_output");
            assert!(report.outputs()[1].is_failed());
            assert_eq!(report.outputs()[1].output_name(), "west_output");
            assert!(report.outputs()[2].is_skipped());
            assert_eq!(report.outputs()[2].output_name(), "east_output");
        }
        let WriteAllCacheReport::CacheAliases {
            aliases,
            skipped_candidates,
        } = report.cache()
        else {
            return Err(format!("expected cache aliases report, got {:?}", report.cache()).into());
        };
        assert!(skipped_candidates.is_empty());
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].table_id(), big.id());
        assert_eq!(aliases[0].alias(), "big");
        assert_eq!(aliases[0].output_indexes(), &[0, 1, 2]);
        assert_eq!(
            aliases[0].status(),
            WriteAllCacheAliasStatus::MaterializedAndRestored
        );

        let restored_big_factory = session.lazy_table_batch_stream_factory(big);
        let restored_big_rows = collect_stream_row_count(restored_big_factory().await?).await?;
        assert_eq!(restored_big_rows, 2);
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_cache_materialization_failure_prevents_output_attempts()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = failing_scan_marker_region_provider();
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east_output =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let error = session
            .write_all_with_writer(&[west_output, east_output], writer)
            .await;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert!(matches!(
                error,
                Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                    if message.contains("scoped MSSQL cache alias materialize failed")
                        && message.contains("big")
            ));
            assert!(calls.is_empty());
            assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        }

        let restored_error = match session.batch_stream_for_lazy_table(&big).await {
            Ok(stream) => match collect_stream_row_count(stream).await {
                Ok(rows) => {
                    return Err(
                        format!("expected restored big read to fail, got {rows} rows").into(),
                    );
                }
                Err(error) => error,
            },
            Err(error) => error,
        };
        assert!(matches!(
            &restored_error,
            DeltaFunnelError::BatchPipeline { message, .. }
                if message.contains("forced scan planning failure")
        ));
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_restores_replaced_alias_after_later_cache_materialization_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (big_source_provider, big_source_scans) = scan_counting_marker_region_provider("big")?;
        let (names_source_provider, names_source_scans) = failing_scan_marker_region_provider();
        session
            .context()
            .register_table("big_source", big_source_provider)?;
        session
            .context()
            .register_table("names_source", names_source_provider)?;
        let pending_big = session
            .table_from_sql(
                "select marker as big_marker, region, \
                 case when region = 'west' then 1 else 2 end as id \
                 from big_source",
            )
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql(
                "select marker as name_marker, region, \
                 case when region = 'west' then 1 else 2 end as id \
                 from names_source",
            )
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let west = session
            .table_from_sql(
                "select big.id, big.big_marker, names.name_marker \
                 from big join names on big.id = names.id \
                 where big.region = 'west'",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.id, big.big_marker, names.name_marker \
                 from big join names on big.id = names.id \
                 where big.region = 'east'",
            )
            .await?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east_output =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let error = session
            .write_all_with_writer(&[west_output, east_output], writer)
            .await;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert!(matches!(
                error,
                Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                    if message.contains("scoped MSSQL cache alias materialize failed")
                        && message.contains("names")
            ));
            assert!(calls.is_empty());
            assert_eq!(big_source_scans.load(Ordering::SeqCst), 1);
            assert_eq!(names_source_scans.load(Ordering::SeqCst), 1);
        }

        let restored_big_factory = session.lazy_table_batch_stream_factory(big);
        assert_eq!(
            collect_stream_row_count(restored_big_factory().await?).await?,
            2
        );
        assert_eq!(big_source_scans.load(Ordering::SeqCst), 2);

        let restored_names_error = match session.batch_stream_for_lazy_table(&names).await {
            Ok(stream) => match collect_stream_row_count(stream).await {
                Ok(rows) => {
                    return Err(
                        format!("expected restored names read to fail, got {rows} rows").into(),
                    );
                }
                Err(error) => error,
            },
            Err(error) => error,
        };
        assert!(matches!(
            &restored_names_error,
            DeltaFunnelError::BatchPipeline { message, .. }
                if message.contains("forced scan planning failure")
        ));
        assert_eq!(names_source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_reports_dependent_stream_setup_failure_before_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;
        let pending_west = session
            .pending_derived_tables
            .iter_mut()
            .find(|pending| pending.table.id() == west.id())
            .ok_or("expected pending west table")?;
        pending_west.schema = Arc::new(Schema::new(vec![Field::new(
            "different_marker",
            DataType::Utf8,
            false,
        )]));
        let big_output = execute_output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east_output =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[big_output, west_output, east_output], writer)
            .await?;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].output_name, "big_output");
            assert_eq!(calls[0].rows, 2);
            assert_eq!(source_scans.load(Ordering::SeqCst), 1);
            assert_eq!(report.succeeded_count(), 1);
            assert_eq!(report.failed_count(), 1);
            assert_eq!(report.skipped_count(), 1);
            assert!(report.outputs()[0].is_succeeded());
            assert_eq!(report.outputs()[0].output_name(), "big_output");
            assert!(report.outputs()[1].is_failed());
            assert_eq!(report.outputs()[1].output_name(), "west_output");
            let failure_message = match &report.outputs()[1] {
                crate::sql_server::MssqlOutputWriteStatus::Failed(failure) => failure.error(),
                status => return Err(format!("expected failed status, got {status:?}").into()),
            };
            assert!(
                failure_message.contains("cached output stream setup failed for `west_output`")
            );
            assert!(failure_message.contains("replanned output schema does not match"));
            assert!(report.outputs()[2].is_skipped());
            assert_eq!(report.outputs()[2].output_name(), "east_output");
        }
        let WriteAllCacheReport::CacheAliases {
            aliases,
            skipped_candidates,
        } = report.cache()
        else {
            return Err(format!("expected cache aliases report, got {:?}", report.cache()).into());
        };
        assert!(skipped_candidates.is_empty());
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].table_id(), big.id());
        assert_eq!(aliases[0].alias(), "big");
        assert_eq!(aliases[0].output_indexes(), &[0, 1, 2]);
        assert_eq!(
            aliases[0].status(),
            WriteAllCacheAliasStatus::MaterializedAndRestored
        );

        let restored_big_factory = session.lazy_table_batch_stream_factory(big);
        let restored_big_rows = collect_stream_row_count(restored_big_factory().await?).await?;
        assert_eq!(restored_big_rows, 2);
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_with_writer_skips_later_outputs_after_writer_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (first_provider, first_scans) = scan_counting_marker_region_provider("first")?;
        let (second_provider, second_scans) = scan_counting_marker_region_provider("second")?;
        let (third_provider, third_scans) = scan_counting_marker_region_provider("third")?;
        session
            .context()
            .register_table("first_source", first_provider)?;
        session
            .context()
            .register_table("second_source", second_provider)?;
        session
            .context()
            .register_table("third_source", third_provider)?;
        let first = session
            .table_from_sql("select marker from first_source where region = 'west'")
            .await?;
        let second = session
            .table_from_sql("select marker from second_source where region = 'west'")
            .await?;
        let third = session
            .table_from_sql("select marker from third_source where region = 'west'")
            .await?;
        let first = execute_output_request(
            first,
            "first_output",
            "first_orders",
            LoadMode::AppendExisting,
        )?;
        let second = execute_output_request(
            second,
            "second_output",
            "second_orders",
            LoadMode::AppendExisting,
        )?;
        let third = execute_output_request(
            third,
            "third_output",
            "third_orders",
            LoadMode::AppendExisting,
        )?;
        let writer = FakeWorkflowWriter::failing_on("second_output");
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[first, second, third], writer)
            .await?;
        let calls = calls
            .lock()
            .map_err(|_| "fake workflow call lock poisoned")?;

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].output_name, "first_output");
        assert_eq!(calls[1].output_name, "second_output");
        assert_eq!(first_scans.load(Ordering::SeqCst), 1);
        assert_eq!(second_scans.load(Ordering::SeqCst), 1);
        assert_eq!(third_scans.load(Ordering::SeqCst), 0);
        assert_eq!(report.succeeded_count(), 1);
        assert_eq!(report.failed_count(), 1);
        assert_eq!(report.skipped_count(), 1);
        assert!(report.outputs()[0].is_succeeded());
        assert_eq!(report.outputs()[0].output_name(), "first_output");
        assert!(report.outputs()[1].is_failed());
        assert_eq!(report.outputs()[1].output_name(), "second_output");
        assert!(report.outputs()[2].is_skipped());
        assert_eq!(report.outputs()[2].output_name(), "third_output");
        Ok(())
    }

    #[tokio::test]
    async fn write_all_with_writer_reports_stream_setup_failure_before_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (first_provider, first_scans) = scan_counting_marker_region_provider("first")?;
        let (failing_provider, failing_scans) = failing_scan_marker_region_provider();
        let (third_provider, third_scans) = scan_counting_marker_region_provider("third")?;
        session
            .context()
            .register_table("first_source", first_provider)?;
        session
            .context()
            .register_table("failing_source", failing_provider)?;
        session
            .context()
            .register_table("third_source", third_provider)?;
        let first = session
            .table_from_sql("select marker from first_source where region = 'west'")
            .await?;
        let failing = session
            .table_from_sql("select marker from failing_source where region = 'west'")
            .await?;
        let third = session
            .table_from_sql("select marker from third_source where region = 'west'")
            .await?;
        let first = execute_output_request(
            first,
            "first_output",
            "first_orders",
            LoadMode::AppendExisting,
        )?;
        let failing = execute_output_request(
            failing,
            "failing_output",
            "failing_orders",
            LoadMode::AppendExisting,
        )?;
        let third = execute_output_request(
            third,
            "third_output",
            "third_orders",
            LoadMode::AppendExisting,
        )?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[first, failing, third], writer)
            .await?;
        let calls = calls
            .lock()
            .map_err(|_| "fake workflow call lock poisoned")?;

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].output_name, "first_output");
        assert_eq!(first_scans.load(Ordering::SeqCst), 1);
        assert_eq!(failing_scans.load(Ordering::SeqCst), 1);
        assert_eq!(third_scans.load(Ordering::SeqCst), 0);
        assert_eq!(report.succeeded_count(), 1);
        assert_eq!(report.failed_count(), 1);
        assert_eq!(report.skipped_count(), 1);
        assert!(report.outputs()[0].is_succeeded());
        assert_eq!(report.outputs()[0].output_name(), "first_output");
        assert!(report.outputs()[1].is_failed());
        assert_eq!(report.outputs()[1].output_name(), "failing_output");
        assert!(report.outputs()[2].is_skipped());
        assert_eq!(report.outputs()[2].output_name(), "third_output");
        Ok(())
    }

    #[tokio::test]
    async fn write_all_rejects_duplicate_output_names_before_stream_setup()
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
        let west = execute_output_request(
            west,
            "orders_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = execute_output_request(
            east,
            "orders_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let error = session.write_all(&[west, east]).await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("write_all output names must be unique")
                    && message.contains("orders_output")
        ));
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_validation_errors_redact_connection_material()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west = execute_output_request(
            west,
            "orders_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = execute_output_request(
            east,
            "orders_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let error = session
            .write_all(&[west, east])
            .await
            .map(|_| ())
            .map_err(|error| format!("{error:?} {error}"));

        assert!(
            matches!(error, Err(display) if display.contains("orders_output")
                && !display.contains("secret-token")
                && !display.contains("password")
                && !display.contains("server=tcp"))
        );
        Ok(())
    }

    #[test]
    fn cache_plan_shell_preserves_selected_output_order() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let west = output_request(
            LazyTable::placeholder(7, LazyTableKind::DerivedSql),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = output_request(
            LazyTable::placeholder(8, LazyTableKind::DerivedSql),
            "east_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::NoSharedRegisteredDerivedAlias,
            }
        );
        assert!(plan.skipped_candidates().is_empty());
        assert_eq!(plan.selected_outputs().len(), 2);
        assert_eq!(plan.selected_outputs()[0].index(), 0);
        assert_eq!(plan.selected_outputs()[0].table_id(), 7);
        assert_eq!(plan.selected_outputs()[0].table_name(), "table_7");
        assert_eq!(plan.selected_outputs()[0].output_name(), "west_output");
        assert_eq!(plan.selected_outputs()[1].index(), 1);
        assert_eq!(plan.selected_outputs()[1].table_id(), 8);
        assert_eq!(plan.selected_outputs()[1].table_name(), "table_8");
        assert_eq!(plan.selected_outputs()[1].output_name(), "east_output");
        Ok(())
    }

    #[test]
    fn cache_plan_shell_reports_single_output_as_not_shared() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let output = output_request(
            LazyTable::placeholder(7, LazyTableKind::DerivedSql),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;

        let plan = session.plan_mssql_output_cache(&[output]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::FewerThanTwoOutputs,
            }
        );
        assert_eq!(plan.selected_outputs().len(), 1);
        Ok(())
    }

    #[test]
    fn cache_plan_debug_omits_target_connection_material() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?)
            .with_connection(secret_connection()?);
        let output = OutputWritePlan::new(
            LazyTable::placeholder(7, LazyTableKind::DerivedSql),
            MssqlOutputTarget::new("orders\noutput", target_config, RunMode::DryRun),
        );

        let debug = format!("{:?}", session.plan_mssql_output_cache(&[output]));

        assert!(debug.contains("orders"));
        assert!(debug.contains("output"));
        assert!(!debug.contains('\n'));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_selects_shared_registered_derived_dependency()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select id from big where customer_name = 'alice'")
            .await?;
        let east = session
            .table_from_sql("select id from big where customer_name = 'bob'")
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        assert!(plan.skipped_candidates().is_empty());
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        assert_eq!(caches.len(), 1);
        let cache = &caches[0];
        assert_eq!(cache.table_id(), big.id());
        assert_eq!(cache.alias(), "big");
        assert_eq!(cache.output_indexes(), &[0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_counts_direct_selected_alias_use() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select id from big where customer_name = 'alice'")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[big_output, west_output]);

        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        assert_eq!(caches.len(), 1);
        let cache = &caches[0];
        assert_eq!(cache.table_id(), big.id());
        assert_eq!(cache.alias(), "big");
        assert_eq!(cache.output_indexes(), &[0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_skips_unshared_registered_derived_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let unrelated = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let unrelated_output = output_request(
            unrelated,
            "unrelated_output",
            "unrelated_orders",
            LoadMode::AppendExisting,
        )?;

        let plan = session.plan_mssql_output_cache(&[big_output, unrelated_output]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::NoSharedRegisteredDerivedAlias,
            }
        );
        assert_eq!(plan.skipped_candidates().len(), 1);
        let skipped = &plan.skipped_candidates()[0];
        assert_eq!(skipped.table_id(), big.id());
        assert_eq!(skipped.alias(), "big");
        assert_eq!(
            skipped.reason(),
            &MssqlCacheCandidateSkipReason::NotShared { output_count: 1 }
        );
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_prefers_deepest_shared_registered_derived_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_filtered = session
            .table_from_sql("select id, customer_name from big where id > 0")
            .await?;
        let filtered_big = session.register_alias("filtered_big", &pending_filtered)?;
        let west = session
            .table_from_sql("select id from filtered_big where customer_name = 'alice'")
            .await?;
        let east = session
            .table_from_sql("select id from filtered_big where customer_name = 'bob'")
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        assert_eq!(caches.len(), 1);
        let cache = &caches[0];
        assert_eq!(cache.table_id(), filtered_big.id());
        assert_eq!(cache.alias(), "filtered_big");
        assert_eq!(cache.output_indexes(), &[0, 1]);
        assert_eq!(plan.skipped_candidates().len(), 1);
        let skipped = &plan.skipped_candidates()[0];
        assert_eq!(skipped.table_id(), big.id());
        assert_eq!(skipped.alias(), "big");
        assert_eq!(
            skipped.reason(),
            &MssqlCacheCandidateSkipReason::CoveredByDeeperSharedAlias {
                selected_table_id: filtered_big.id(),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_selects_independent_shared_aliases_with_same_output_indexes()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let west = session
            .table_from_sql(
                "select big.id from big join names on big.customer_name = names.customer_name",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.id from big join names on big.customer_name = names.customer_name",
            )
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        // Sharing the same selected output indexes is not ambiguity when the
        // aliases are independent in the derived lineage graph.
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        assert!(plan.skipped_candidates().is_empty());
        assert_eq!(caches.len(), 2);
        assert_eq!(caches[0].table_id(), big.id());
        assert_eq!(caches[0].alias(), "big");
        assert_eq!(caches[0].output_indexes(), &[0, 1]);
        assert_eq!(caches[1].table_id(), names.id());
        assert_eq!(caches[1].alias(), "names");
        assert_eq!(caches[1].output_indexes(), &[0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_rejects_cyclic_shared_candidate_relationships()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        for derived in &mut session.derived_tables {
            if derived.table().id() == big.id() {
                derived.lineage = DerivedTableLineage::complete(
                    vec![DerivedTableDependency::RegisteredDerived {
                        table_id: names.id(),
                        name: "names".to_owned(),
                    }],
                    Vec::new(),
                    Vec::new(),
                );
            } else if derived.table().id() == names.id() {
                derived.lineage = DerivedTableLineage::complete(
                    vec![DerivedTableDependency::RegisteredDerived {
                        table_id: big.id(),
                        name: "big".to_owned(),
                    }],
                    Vec::new(),
                    Vec::new(),
                );
            }
        }
        let west = session
            .table_from_sql(
                "select big.id from big join names on big.customer_name = names.customer_name",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.id from big join names on big.customer_name = names.customer_name",
            )
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::AmbiguousSharedDerivedAlias,
            }
        );
        assert_eq!(plan.skipped_candidates().len(), 2);
        assert_eq!(plan.skipped_candidates()[0].table_id(), big.id());
        assert_eq!(
            plan.skipped_candidates()[0].reason(),
            &MssqlCacheCandidateSkipReason::AmbiguousDepth
        );
        assert_eq!(plan.skipped_candidates()[1].table_id(), names.id());
        assert_eq!(
            plan.skipped_candidates()[1].reason(),
            &MssqlCacheCandidateSkipReason::AmbiguousDepth
        );
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_rejects_partially_ambiguous_shared_candidate_graph()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let pending_regions = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let regions = session.register_alias("regions", &pending_regions)?;
        for derived in &mut session.derived_tables {
            if derived.table().id() == big.id() {
                derived.lineage = DerivedTableLineage::complete(
                    vec![DerivedTableDependency::RegisteredDerived {
                        table_id: names.id(),
                        name: "names".to_owned(),
                    }],
                    Vec::new(),
                    Vec::new(),
                );
            } else if derived.table().id() == names.id() {
                derived.lineage = DerivedTableLineage::complete(
                    vec![DerivedTableDependency::RegisteredDerived {
                        table_id: big.id(),
                        name: "big".to_owned(),
                    }],
                    Vec::new(),
                    Vec::new(),
                );
            }
        }
        let west = session
            .table_from_sql(
                "select big.id from big \
                 join names on big.customer_name = names.customer_name \
                 join regions on big.id = regions.id",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.id from big \
                 join names on big.customer_name = names.customer_name \
                 join regions on big.id = regions.id",
            )
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::AmbiguousSharedDerivedAlias,
            }
        );
        assert_eq!(plan.skipped_candidates().len(), 3);
        assert_eq!(plan.skipped_candidates()[0].table_id(), big.id());
        assert_eq!(
            plan.skipped_candidates()[0].reason(),
            &MssqlCacheCandidateSkipReason::AmbiguousDepth
        );
        assert_eq!(plan.skipped_candidates()[1].table_id(), names.id());
        assert_eq!(
            plan.skipped_candidates()[1].reason(),
            &MssqlCacheCandidateSkipReason::AmbiguousDepth
        );
        assert_eq!(plan.skipped_candidates()[2].table_id(), regions.id());
        assert_eq!(
            plan.skipped_candidates()[2].reason(),
            &MssqlCacheCandidateSkipReason::AmbiguousDepth
        );
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_does_not_consider_shared_raw_source_as_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let orders = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let west = output_request(
            orders.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = output_request(
            orders,
            "east_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::NoSharedRegisteredDerivedAlias,
            }
        );
        assert!(plan.skipped_candidates().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_skips_registered_derived_alias_with_incomplete_lineage()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let registered = session
            .derived_tables
            .iter_mut()
            .find(|derived| derived.table().id() == big.id())
            .ok_or("registered derived alias missing")?;
        registered.lineage = DerivedTableLineage::incomplete("forced incomplete lineage");
        let west = session
            .table_from_sql("select id from big where customer_name = 'alice'")
            .await?;
        let east = session
            .table_from_sql("select id from big where customer_name = 'bob'")
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::NoSharedRegisteredDerivedAlias,
            }
        );
        assert_eq!(plan.skipped_candidates().len(), 1);
        let skipped = &plan.skipped_candidates()[0];
        assert_eq!(skipped.table_id(), big.id());
        assert_eq!(skipped.alias(), "big");
        assert_eq!(
            skipped.reason(),
            &MssqlCacheCandidateSkipReason::IncompleteLineage
        );
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_skips_registered_derived_alias_with_missing_sql_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let registered = session
            .derived_tables
            .iter_mut()
            .find(|derived| derived.table().id() == big.id())
            .ok_or("registered derived alias missing")?;
        registered.sql_text.clear();
        let west = session
            .table_from_sql("select id from big where customer_name = 'alice'")
            .await?;
        let east = session
            .table_from_sql("select id from big where customer_name = 'bob'")
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let plan = session.plan_mssql_output_cache(&[west, east]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::NoSharedRegisteredDerivedAlias,
            }
        );
        assert_eq!(plan.skipped_candidates().len(), 1);
        let skipped = &plan.skipped_candidates()[0];
        assert_eq!(skipped.table_id(), big.id());
        assert_eq!(skipped.alias(), "big");
        assert_eq!(
            skipped.reason(),
            &MssqlCacheCandidateSkipReason::MissingSqlText
        );
        Ok(())
    }

    #[tokio::test]
    async fn cache_plan_skips_independent_unshared_registered_derived_aliases()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let west = session
            .table_from_sql("select id from big where customer_name = 'alice'")
            .await?;
        let name_output = session
            .table_from_sql("select customer_name from names")
            .await?;
        let west = output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let name_output = output_request(
            name_output,
            "name_output",
            "name_orders",
            LoadMode::AppendExisting,
        )?;

        let plan = session.plan_mssql_output_cache(&[west, name_output]);

        assert_eq!(
            plan.decision(),
            &MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::NoSharedRegisteredDerivedAlias,
            }
        );
        assert_eq!(plan.skipped_candidates().len(), 2);
        assert_eq!(plan.skipped_candidates()[0].table_id(), big.id());
        assert_eq!(plan.skipped_candidates()[0].alias(), "big");
        assert_eq!(
            plan.skipped_candidates()[0].reason(),
            &MssqlCacheCandidateSkipReason::NotShared { output_count: 1 }
        );
        assert_eq!(plan.skipped_candidates()[1].table_id(), names.id());
        assert_eq!(plan.skipped_candidates()[1].alias(), "names");
        assert_eq!(
            plan.skipped_candidates()[1].reason(),
            &MssqlCacheCandidateSkipReason::NotShared { output_count: 1 }
        );
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_replacement_materializes_cache_and_restores_original_provider()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);

        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;

        assert_eq!(replacement.table_id(), big.id());
        assert_eq!(replacement.alias_name(), "big");
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let direct_cached_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_cached_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let restoration = replacement.restore().await?;

        assert_eq!(restoration.table_id(), big.id());
        assert_eq!(restoration.alias_name(), "big");
        assert!(restoration.cached_alias_was_present());

        let direct_restored_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_restored_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_replacement_restores_original_after_cached_register_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;
        let original_provider = session
            .context()
            .deregister_table("big")?
            .ok_or("expected original provider")?;

        let error = session.restore_original_after_cached_register_failure(
            "big",
            original_provider,
            "injected cached register failure",
        );

        assert!(matches!(
            &error,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("failed to register cached provider")
                    && message.contains("injected cached register failure")
        ));

        let direct_restored_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_restored_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_replacement_explicit_restore_cleans_up_after_later_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let later_error = DeltaFunnelError::MssqlWorkflowPlanning {
            message: "simulated downstream planning failure".to_owned(),
        };
        let restoration = replacement.restore().await?;

        assert!(matches!(
            later_error,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("simulated downstream planning failure")
        ));
        assert_eq!(restoration.alias_name(), "big");

        let direct_restored_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_restored_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_restore_reinstalls_original_when_cached_alias_is_missing()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let removed_cached = session.context().deregister_table("big")?;
        assert!(removed_cached.is_some());

        let restoration = replacement.restore().await?;

        assert_eq!(restoration.alias_name(), "big");
        assert!(!restoration.cached_alias_was_present());

        let direct_restored_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_restored_big)?,
            vec!["shared"]
        );
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test]
    fn cache_error_with_restore_error_preserves_both_contexts() {
        let primary_error = DeltaFunnelError::MssqlWorkflowPlanning {
            message: "simulated output workflow failure".to_owned(),
        };
        let restore_error = DeltaFunnelError::MssqlWorkflowPlanning {
            message: "simulated restore failure for alias big".to_owned(),
        };

        let error = cache_error_with_restore_error(primary_error, restore_error);

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("write_all auto cache failed")
                    && message.contains("simulated output workflow failure")
                    && message.contains("also failed to restore cache aliases")
                    && message.contains("simulated restore failure for alias big")
        ));
    }

    #[tokio::test]
    async fn restore_mssql_cache_aliases_after_error_preserves_broken_restore_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let broken_replacement = MssqlScopedCacheAliasReplacement::broken_for_test(
            session.context(),
            42,
            "big".to_owned(),
        );
        let primary_error = DeltaFunnelError::MssqlWorkflowPlanning {
            message: "simulated cached workflow failure".to_owned(),
        };

        let error =
            restore_mssql_cache_aliases_after_error(primary_error, vec![broken_replacement]).await;

        assert!(matches!(
            error,
            DeltaFunnelError::MssqlWorkflowPlanning { message }
                if message.contains("write_all auto cache failed")
                    && message.contains("simulated cached workflow failure")
                    && message.contains("also failed to restore cache aliases")
                    && message.contains("scoped MSSQL cache alias restore failed")
                    && message.contains("big")
                    && message.contains("original provider was already restored")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn cached_alias_replacement_does_not_feed_existing_downstream_derived_tables()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session
            .context()
            .register_table("big_source", marker_region_provider("original")?)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;

        let replacement = session
            .context()
            .read_table(marker_region_provider("replacement")?)?
            .cache()
            .await?
            .into_view();
        let removed_big = session.context().deregister_table("big")?;
        assert!(removed_big.is_some());
        session.context().register_table("big", replacement)?;

        let direct_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_big)?,
            vec!["replacement"]
        );

        let west_stream = session.batch_stream_for_lazy_table(&west).await?;
        let east_stream = session.batch_stream_for_lazy_table(&east).await?;
        let west_markers = collect_stream_marker_values(west_stream).await?;
        let east_markers = collect_stream_marker_values(east_stream).await?;

        // Conclusion for #245: existing downstream ViewTable providers keep the
        // original resolved provider; catalog replacement alone does not rewire them.
        assert_eq!(west_markers, vec!["original"]);
        assert_eq!(east_markers, vec!["original"]);
        Ok(())
    }

    #[tokio::test]
    async fn replanned_downstream_sql_uses_cached_alias_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        const WEST_SQL: &str = "select marker from big where region = 'west'";
        const EAST_SQL: &str = "select marker from big where region = 'east'";

        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;
        let _old_west = session.table_from_sql(WEST_SQL).await?;
        let _old_east = session.table_from_sql(EAST_SQL).await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);

        let cached_big = session
            .context()
            .table("big")
            .await?
            .cache()
            .await?
            .into_view();
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let removed_big = session.context().deregister_table("big")?;
        assert!(removed_big.is_some());
        session.context().register_table("big", cached_big)?;

        let direct_big = session.context().sql(WEST_SQL).await?.collect().await?;
        assert_eq!(marker_values_from_batches(&direct_big)?, vec!["shared"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let replanned_west = session.table_from_sql(WEST_SQL).await?;
        let replanned_east = session.table_from_sql(EAST_SQL).await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let west_stream = session.batch_stream_for_lazy_table(&replanned_west).await?;
        let east_stream = session.batch_stream_for_lazy_table(&replanned_east).await?;
        let west_markers = collect_stream_marker_values(west_stream).await?;
        let east_markers = collect_stream_marker_values(east_stream).await?;

        // Conclusion for #247: after cached big is installed under alias big,
        // replanning downstream SQL reads the cached provider and does not
        // rescan the original upstream provider per output.
        assert_eq!(west_markers, vec!["shared"]);
        assert_eq!(east_markers, vec!["shared"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_route_classifies_direct_dependent_and_unrelated_outputs()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select id from big where customer_name = 'alice'")
            .await?;
        let unrelated = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let unrelated_output = output_request(
            unrelated,
            "unrelated_output",
            "unrelated_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[big_output.clone(), west_output.clone()]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };

        assert_eq!(
            session.cached_output_stream_route(&big_output, caches)?,
            MssqlCachedOutputStreamRoute::DirectCachedAlias(caches[0].clone())
        );
        assert_eq!(
            session.cached_output_stream_route(&west_output, caches)?,
            MssqlCachedOutputStreamRoute::ReplannedCachedDependency(vec![caches[0].clone()])
        );
        assert_eq!(
            session.cached_output_stream_route(&unrelated_output, caches)?,
            MssqlCachedOutputStreamRoute::UncachedLazyTable
        );
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_route_keeps_multiple_active_dependency_aliases()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let west = session
            .table_from_sql(
                "select big.id from big join names on big.customer_name = names.customer_name",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.id from big join names on big.customer_name = names.customer_name",
            )
            .await?;
        let west_output = output_request(
            west.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east_output =
            output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let plan = session.plan_mssql_output_cache(&[west_output.clone(), east_output]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };

        assert_eq!(caches.len(), 2);
        assert_eq!(caches[0].table_id(), big.id());
        assert_eq!(caches[1].table_id(), names.id());
        assert_eq!(
            session.cached_output_stream_route(&west_output, caches)?,
            MssqlCachedOutputStreamRoute::ReplannedCachedDependency(caches.clone())
        );
        Ok(())
    }

    #[test]
    fn cached_output_stream_route_rejects_unknown_active_alias() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let output = output_request(
            LazyTable::placeholder(7, LazyTableKind::DerivedSql),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let aliases = vec![MssqlDerivedCacheAliasPlan::new(
            252,
            "missing_cache".to_owned(),
            vec![0],
        )];

        let error = session.cached_output_stream_route(&output, &aliases);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("missing_cache")
                    && message.contains("not registered in this session")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_direct_alias_reads_active_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let plan = session.plan_mssql_output_cache(&[big_output.clone(), west_output]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let factory = session.cached_output_batch_stream_factory(&big_output, caches)?;
        let markers = collect_stream_marker_values(factory().await?).await?;

        assert_eq!(markers, vec!["shared", "shared"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        let _restoration = replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_unrelated_output_uses_existing_lazy_table_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let unrelated = session
            .table_from_sql("select 'unrelated' as marker, 'north' as region")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let unrelated_output = output_request(
            unrelated,
            "unrelated_output",
            "unrelated_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[big_output, west_output]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let factory = session.cached_output_batch_stream_factory(&unrelated_output, caches)?;
        let markers = collect_stream_marker_values(factory().await?).await?;

        assert_eq!(markers, vec!["unrelated"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        let _restoration = replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_replans_dependent_outputs_against_active_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output = output_request(
            west.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east_output = output_request(
            east.clone(),
            "east_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[
            big_output.clone(),
            west_output.clone(),
            east_output.clone(),
        ]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let big_factory = session.cached_output_batch_stream_factory(&big_output, caches)?;
        let west_factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let east_factory = session.cached_output_batch_stream_factory(&east_output, caches)?;
        let big_markers = collect_stream_marker_values(big_factory().await?).await?;
        let west_markers = collect_stream_marker_values(west_factory().await?).await?;
        let east_markers = collect_stream_marker_values(east_factory().await?).await?;

        assert_eq!(big_markers, vec!["shared", "shared"]);
        assert_eq!(west_markers, vec!["shared"]);
        assert_eq!(east_markers, vec!["shared"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        let _restoration = replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_replans_dependent_output_against_multiple_active_caches()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (big_source_provider, big_source_scans) = scan_counting_marker_region_provider("big")?;
        let (names_source_provider, names_source_scans) =
            scan_counting_marker_region_provider("name")?;
        session
            .context()
            .register_table("big_source", big_source_provider)?;
        session
            .context()
            .register_table("names_source", names_source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql("select marker, region from names_source")
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let west = session
            .table_from_sql(
                "select big.marker from big join names on big.region = names.region where big.region = 'west' and names.marker = 'name'",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.marker from big join names on big.region = names.region where big.region = 'east' and names.marker = 'name'",
            )
            .await?;
        let west_output = output_request(
            west.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east_output =
            output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let plan = session.plan_mssql_output_cache(&[west_output.clone(), east_output]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        assert_eq!(caches.len(), 2);
        assert_eq!(caches[0].table_id(), big.id());
        assert_eq!(caches[1].table_id(), names.id());
        let big_replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;
        let names_replacement = session
            .replace_registered_derived_alias_with_cache(&names)
            .await?;
        assert_eq!(big_source_scans.load(Ordering::SeqCst), 1);
        assert_eq!(names_source_scans.load(Ordering::SeqCst), 1);

        let factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let markers = collect_stream_marker_values(factory().await?).await?;

        assert_eq!(markers, vec!["big"]);
        assert_eq!(big_source_scans.load(Ordering::SeqCst), 1);
        assert_eq!(names_source_scans.load(Ordering::SeqCst), 1);
        let _names_restoration = names_replacement.restore().await?;
        let _big_restoration = big_replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_rejects_replanned_schema_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, _source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output = output_request(
            west.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[big_output, west_output.clone()]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let pending_west = session
            .pending_derived_tables
            .iter_mut()
            .find(|pending| pending.table.id() == west.id())
            .ok_or("expected pending west table")?;
        pending_west.schema = Arc::new(Schema::new(vec![Field::new(
            "different_marker",
            DataType::Utf8,
            false,
        )]));
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;

        let factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let error = factory().await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("cached output stream setup failed for `west_output`")
                    && message.contains("replanned output schema does not match")
        ));
        let _restoration = replacement.restore().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cached_output_stream_factory_returns_async_error_for_unreplayable_sql()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, _source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let big_output = output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output = output_request(
            west.clone(),
            "west_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let plan = session.plan_mssql_output_cache(&[big_output, west_output.clone()]);
        let MssqlOutputCacheDecision::CacheAliases(caches) = plan.decision() else {
            return Err("expected cache aliases decision".into());
        };
        let pending_west = session
            .pending_derived_tables
            .iter_mut()
            .find(|pending| pending.table.id() == west.id())
            .ok_or("expected pending west table")?;
        pending_west.sql_text.clear();
        let replacement = session
            .replace_registered_derived_alias_with_cache(&big)
            .await?;

        let factory = session.cached_output_batch_stream_factory(&west_output, caches)?;
        let error = factory().await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("cached output stream setup failed for `west_output`")
        ));
        let _restoration = replacement.restore().await?;
        Ok(())
    }
}
