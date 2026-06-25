use std::{collections::BTreeSet, fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::{arrow::datatypes::SchemaRef, datasource::TableProvider, prelude::SessionContext};

use crate::{
    DeltaFunnelError, MssqlOutputBatchStream, MssqlOutputWriteJob, MssqlOutputWriteStatus,
    MssqlSchemaPlanOptions, MssqlWorkflowOutputWriter, MssqlWorkflowWriteReport, MssqlWriteOptions,
    MssqlWriteReport, ResolvedMssqlTarget, support::sanitize_text_for_display,
    write_mssql_outputs_with_writer, write_output_batches_to_mssql,
};

use super::{
    DeltaFunnelSession, DeltaSourceReport, LazyTable, LazyTableKind, OutputWritePlan,
    PlannedMssqlOutput, RegisteredDerivedTable, RunMode,
    errors::mssql_scoped_cache_alias_error,
    errors::unknown_cached_alias_error,
    registry::DerivedTableDependency,
    streams::{SharedProviderReadStats, provider_read_stats_snapshot, shared_provider_read_stats},
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

/// Report for one `write_all` call that reached the sequential workflow.
///
/// Planning and cache setup failures are returned as errors before this report
/// exists. Once the workflow starts, output write failures and dependent-output
/// stream setup failures are represented in the wrapped workflow report while
/// cache metadata remains available through [`WriteAllReport::cache`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteAllReport {
    workflow: MssqlWorkflowWriteReport,
    cache: WriteAllCacheReport,
    sources: Vec<DeltaSourceReport>,
}

impl WriteAllReport {
    pub(super) fn new(
        workflow: MssqlWorkflowWriteReport,
        cache: WriteAllCacheReport,
        sources: Vec<DeltaSourceReport>,
    ) -> Self {
        Self {
            workflow,
            cache,
            sources,
        }
    }

    /// Returns the lower-level SQL Server workflow report.
    #[must_use]
    pub const fn workflow(&self) -> &MssqlWorkflowWriteReport {
        &self.workflow
    }

    /// Returns cache planning, selection, and lifecycle metadata for this call.
    #[must_use]
    pub const fn cache(&self) -> &WriteAllCacheReport {
        &self.cache
    }

    /// Returns Delta source reports in session registration order.
    #[must_use]
    pub fn sources(&self) -> &[DeltaSourceReport] {
        &self.sources
    }

    /// Returns the number of selected outputs represented by this report.
    #[must_use]
    pub fn len(&self) -> usize {
        self.workflow.len()
    }

    /// Returns whether this report contains no selected outputs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.workflow.is_empty()
    }

    /// Returns per-output SQL Server workflow statuses in caller-provided order.
    #[must_use]
    pub fn outputs(&self) -> &[MssqlOutputWriteStatus] {
        self.workflow.outputs()
    }

    /// Returns whether every selected output completed successfully.
    #[must_use]
    pub fn all_succeeded(&self) -> bool {
        self.workflow.all_succeeded()
    }

    /// Returns the number of outputs that completed successfully.
    #[must_use]
    pub fn succeeded_count(&self) -> usize {
        self.workflow.succeeded_count()
    }

    /// Returns the number of outputs that failed.
    #[must_use]
    pub fn failed_count(&self) -> usize {
        self.workflow.failed_count()
    }

    /// Returns the number of outputs skipped after a previous output failed.
    #[must_use]
    pub fn skipped_count(&self) -> usize {
        self.workflow.skipped_count()
    }
}

/// Cache metadata for one `write_all` call.
///
/// This report describes the conservative cache decision for calls that reached
/// the sequential output workflow. Cache materialization failures occur before
/// the workflow can start, so they are returned as errors instead of as
/// `WriteAllCacheReport` values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteAllCacheReport {
    /// Cache planning was disabled for this call.
    Disabled,
    /// Cache planning ran but did not select a safe cache frontier.
    NoCache {
        /// Conservative reason no cache aliases were selected.
        reason: WriteAllNoCacheReason,
        /// Registered derived aliases skipped by conservative cache planning.
        skipped_candidates: Vec<WriteAllCacheCandidateSkip>,
    },
    /// Cache planning selected registered derived aliases for this call.
    CacheAliases {
        /// Selected registered derived aliases in deterministic planner order.
        aliases: Vec<WriteAllCacheAliasReport>,
        /// Registered derived aliases skipped by conservative cache planning.
        skipped_candidates: Vec<WriteAllCacheCandidateSkip>,
    },
}

impl WriteAllCacheReport {
    pub(super) fn disabled() -> Self {
        Self::Disabled
    }

    pub(super) fn from_plan(plan: &MssqlOutputCachePlan) -> Self {
        Self::from_plan_with_alias_status(plan, WriteAllCacheAliasStatus::Selected)
    }

    pub(super) fn from_executed_plan(plan: &MssqlOutputCachePlan) -> Self {
        Self::from_plan_with_alias_status(plan, WriteAllCacheAliasStatus::MaterializedAndRestored)
    }

    fn from_plan_with_alias_status(
        plan: &MssqlOutputCachePlan,
        alias_status: WriteAllCacheAliasStatus,
    ) -> Self {
        let skipped_candidates = plan
            .skipped_candidates()
            .iter()
            .map(WriteAllCacheCandidateSkip::from_internal)
            .collect::<Vec<_>>();

        match plan.decision() {
            MssqlOutputCacheDecision::NoCache { reason } => Self::NoCache {
                reason: WriteAllNoCacheReason::from_internal(reason),
                skipped_candidates,
            },
            MssqlOutputCacheDecision::CacheAliases(aliases) => Self::CacheAliases {
                aliases: aliases
                    .iter()
                    .map(|alias| WriteAllCacheAliasReport::from_internal(alias, alias_status))
                    .collect(),
                skipped_candidates,
            },
        }
    }
}

/// Conservative reason no cache alias was selected for `write_all`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteAllNoCacheReason {
    /// Cache selection only helps when at least two outputs use a candidate.
    FewerThanTwoOutputs,
    /// No registered derived alias is shared by at least two selected outputs.
    NoSharedRegisteredDerivedAlias,
    /// Candidate relationships could not produce a deterministic cache frontier.
    AmbiguousSharedDerivedAlias,
}

impl WriteAllNoCacheReason {
    fn from_internal(reason: &MssqlNoCacheReason) -> Self {
        match reason {
            MssqlNoCacheReason::FewerThanTwoOutputs => Self::FewerThanTwoOutputs,
            MssqlNoCacheReason::NoSharedRegisteredDerivedAlias => {
                Self::NoSharedRegisteredDerivedAlias
            }
            MssqlNoCacheReason::AmbiguousSharedDerivedAlias => Self::AmbiguousSharedDerivedAlias,
        }
    }
}

/// Selected registered derived alias cache metadata.
///
/// `output_indexes` uses caller-provided `write_all` request indexes. It
/// includes direct writes of the selected alias and dependent outputs whose
/// retained SQL was replanned against the active cached alias.
#[derive(Clone, PartialEq, Eq)]
pub struct WriteAllCacheAliasReport {
    table_id: u64,
    alias: String,
    output_indexes: Vec<usize>,
    status: WriteAllCacheAliasStatus,
}

impl WriteAllCacheAliasReport {
    fn from_internal(alias: &MssqlDerivedCacheAliasPlan, status: WriteAllCacheAliasStatus) -> Self {
        Self {
            table_id: alias.table_id(),
            alias: alias.alias().to_owned(),
            output_indexes: alias.output_indexes().to_vec(),
            status,
        }
    }

    /// Returns the selected registered derived table id.
    #[must_use]
    pub const fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Returns the selected registered derived alias.
    #[must_use]
    pub fn alias(&self) -> &str {
        &self.alias
    }

    /// Returns selected output indexes that use this alias.
    #[must_use]
    pub fn output_indexes(&self) -> &[usize] {
        &self.output_indexes
    }

    /// Returns this alias cache lifecycle status for the `write_all` call.
    #[must_use]
    pub const fn status(&self) -> WriteAllCacheAliasStatus {
        self.status
    }
}

impl fmt::Debug for WriteAllCacheAliasReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WriteAllCacheAliasReport")
            .field("table_id", &self.table_id)
            .field("alias", &sanitize_text_for_display(&self.alias))
            .field("output_indexes", &self.output_indexes)
            .field("status", &self.status)
            .finish()
    }
}

/// Cache lifecycle status for one selected alias in a `write_all` report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteAllCacheAliasStatus {
    /// The alias was selected by cache planning but has no completed workflow.
    ///
    /// This status is reserved for plan-shaped metadata. Normal successful
    /// public `write_all` reports use [`Self::MaterializedAndRestored`] for
    /// selected aliases because the scoped catalog replacement has already
    /// been cleaned up before the report is returned.
    Selected,
    /// The alias was materialized, used for the workflow, and restored.
    ///
    /// The workflow may still contain per-output failures. This status only
    /// states that cache setup and restoration completed for the alias.
    MaterializedAndRestored,
}

/// Registered derived alias skipped during conservative cache selection.
#[derive(Clone, PartialEq, Eq)]
pub struct WriteAllCacheCandidateSkip {
    table_id: u64,
    alias: String,
    reason: WriteAllCacheCandidateSkipReason,
}

impl WriteAllCacheCandidateSkip {
    fn from_internal(skip: &MssqlCacheCandidateSkip) -> Self {
        Self {
            table_id: skip.table_id(),
            alias: skip.alias().to_owned(),
            reason: WriteAllCacheCandidateSkipReason::from_internal(skip.reason()),
        }
    }

    /// Returns the skipped registered derived table id.
    #[must_use]
    pub const fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Returns the skipped registered derived alias.
    #[must_use]
    pub fn alias(&self) -> &str {
        &self.alias
    }

    /// Returns why this candidate was skipped.
    #[must_use]
    pub const fn reason(&self) -> &WriteAllCacheCandidateSkipReason {
        &self.reason
    }
}

impl fmt::Debug for WriteAllCacheCandidateSkip {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WriteAllCacheCandidateSkip")
            .field("table_id", &self.table_id)
            .field("alias", &sanitize_text_for_display(&self.alias))
            .field("reason", &self.reason)
            .finish()
    }
}

/// Reason a cache candidate was skipped by conservative `write_all` planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteAllCacheCandidateSkipReason {
    /// Fewer than two selected outputs use this candidate.
    NotShared {
        /// Number of selected outputs that use this candidate.
        output_count: usize,
    },
    /// Retained SQL text was missing, so later replanning would be unsafe.
    MissingSqlText,
    /// Lineage was incomplete or could not be trusted.
    IncompleteLineage,
    /// A deeper shared alias is closer to all dependent outputs.
    CoveredByDeeperSharedAlias {
        /// Table id of the selected deeper alias that covers this candidate.
        selected_table_id: u64,
    },
    /// The candidate's relative depth could not be ordered deterministically.
    AmbiguousDepth,
}

impl WriteAllCacheCandidateSkipReason {
    fn from_internal(reason: &MssqlCacheCandidateSkipReason) -> Self {
        match reason {
            MssqlCacheCandidateSkipReason::NotShared { output_count } => Self::NotShared {
                output_count: *output_count,
            },
            MssqlCacheCandidateSkipReason::MissingSqlText => Self::MissingSqlText,
            MssqlCacheCandidateSkipReason::IncompleteLineage => Self::IncompleteLineage,
            MssqlCacheCandidateSkipReason::CoveredByDeeperSharedAlias { selected_table_id } => {
                Self::CoveredByDeeperSharedAlias {
                    selected_table_id: *selected_table_id,
                }
            }
            MssqlCacheCandidateSkipReason::AmbiguousDepth => Self::AmbiguousDepth,
        }
    }
}

/// Active replacement of one registered derived alias with a cached provider.
///
/// The original provider is owned by this scope until `restore` is awaited.
/// Callers must not rely on `Drop` for restoration.
#[allow(dead_code)]
pub(crate) struct MssqlScopedCacheAliasReplacement<'a> {
    context: &'a SessionContext,
    table_id: u64,
    alias_name: String,
    original_provider: Option<Arc<dyn TableProvider>>,
}

#[allow(dead_code)]
impl<'a> MssqlScopedCacheAliasReplacement<'a> {
    pub(super) fn new(
        context: &'a SessionContext,
        table_id: u64,
        alias_name: String,
        original_provider: Arc<dyn TableProvider>,
    ) -> Self {
        Self {
            context,
            table_id,
            alias_name,
            original_provider: Some(original_provider),
        }
    }

    #[cfg(test)]
    pub(super) fn broken_for_test(
        context: &'a SessionContext,
        table_id: u64,
        alias_name: String,
    ) -> Self {
        Self {
            context,
            table_id,
            alias_name,
            original_provider: None,
        }
    }

    /// Returns the session table id for the replaced alias.
    #[must_use]
    pub(crate) const fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Returns the registered alias name currently backed by the cached provider.
    #[must_use]
    pub(crate) fn alias_name(&self) -> &str {
        &self.alias_name
    }

    /// Restores the original provider under the alias and consumes the scope.
    ///
    /// This method transitions the catalog from "alias points at cached
    /// provider" back to "alias points at the original provider". It is async
    /// by design so later cache cleanup can remain awaitable even if DataFusion
    /// changes or additional async cleanup is needed.
    ///
    /// Callers should await this method on both success and error paths that
    /// leave the scoped replacement active.
    pub(crate) async fn restore(
        mut self,
    ) -> Result<MssqlScopedCacheAliasRestoration, DeltaFunnelError> {
        let Some(original_provider) = self.original_provider.take() else {
            return Err(mssql_scoped_cache_alias_error(
                "restore",
                &self.alias_name,
                "original provider was already restored",
            ));
        };

        let removed_cached = self
            .context
            .deregister_table(self.alias_name.as_str())
            .map_err(|error| {
                mssql_scoped_cache_alias_error("restore_deregister", &self.alias_name, error)
            })?;

        self.context
            .register_table(self.alias_name.as_str(), original_provider)
            .map_err(|error| {
                mssql_scoped_cache_alias_error("restore_register", &self.alias_name, error)
            })?;

        Ok(MssqlScopedCacheAliasRestoration {
            table_id: self.table_id,
            alias_name: self.alias_name,
            cached_alias_was_present: removed_cached.is_some(),
        })
    }
}

/// Report returned after a scoped cache alias replacement restores the original alias.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct MssqlScopedCacheAliasRestoration {
    table_id: u64,
    alias_name: String,
    cached_alias_was_present: bool,
}

#[allow(dead_code)]
impl MssqlScopedCacheAliasRestoration {
    /// Returns the restored session table id.
    #[must_use]
    pub(crate) const fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Returns the restored alias name.
    #[must_use]
    pub(crate) fn alias_name(&self) -> &str {
        &self.alias_name
    }

    /// Returns whether a cached alias was present when restoration started.
    #[must_use]
    pub(crate) const fn cached_alias_was_present(&self) -> bool {
        self.cached_alias_was_present
    }
}

/// Planner output for one `write_all` cache-selection pass.
#[allow(dead_code)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MssqlOutputCachePlan {
    selected_outputs: Vec<MssqlOutputCachePlanOutput>,
    decision: MssqlOutputCacheDecision,
    skipped_candidates: Vec<MssqlCacheCandidateSkip>,
}

#[allow(dead_code)]
impl MssqlOutputCachePlan {
    pub(super) fn new(
        selected_outputs: Vec<MssqlOutputCachePlanOutput>,
        decision: MssqlOutputCacheDecision,
        skipped_candidates: Vec<MssqlCacheCandidateSkip>,
    ) -> Self {
        Self {
            selected_outputs,
            decision,
            skipped_candidates,
        }
    }

    pub(super) fn no_cache(
        selected_outputs: Vec<MssqlOutputCachePlanOutput>,
        reason: MssqlNoCacheReason,
    ) -> Self {
        Self {
            selected_outputs,
            decision: MssqlOutputCacheDecision::NoCache { reason },
            skipped_candidates: Vec::new(),
        }
    }

    /// Returns selected outputs in caller-provided order.
    #[must_use]
    pub(crate) fn selected_outputs(&self) -> &[MssqlOutputCachePlanOutput] {
        &self.selected_outputs
    }

    /// Returns the cache choice for this planning pass.
    #[must_use]
    pub(crate) const fn decision(&self) -> &MssqlOutputCacheDecision {
        &self.decision
    }

    /// Returns candidates skipped for explicit conservative reasons.
    #[must_use]
    pub(crate) fn skipped_candidates(&self) -> &[MssqlCacheCandidateSkip] {
        &self.skipped_candidates
    }
}

impl fmt::Debug for MssqlOutputCachePlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlOutputCachePlan")
            .field("selected_outputs", &self.selected_outputs)
            .field("decision", &self.decision)
            .field("skipped_candidates", &self.skipped_candidates)
            .finish()
    }
}

/// Selected output identity captured for cache planning.
#[allow(dead_code)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MssqlOutputCachePlanOutput {
    index: usize,
    table_id: u64,
    table_name: String,
    output_name: String,
}

#[allow(dead_code)]
impl MssqlOutputCachePlanOutput {
    pub(super) fn from_request(index: usize, request: &OutputWritePlan) -> Self {
        Self {
            index,
            table_id: request.table().id(),
            table_name: request.table().name().to_owned(),
            output_name: request.target().output_name().to_owned(),
        }
    }

    /// Returns the output index from the caller-provided request list.
    #[must_use]
    pub(crate) const fn index(&self) -> usize {
        self.index
    }

    /// Returns the selected lazy table id.
    #[must_use]
    pub(crate) const fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Returns the selected lazy table name.
    #[must_use]
    pub(crate) fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Returns the selected output name.
    #[must_use]
    pub(crate) fn output_name(&self) -> &str {
        &self.output_name
    }
}

impl fmt::Debug for MssqlOutputCachePlanOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlOutputCachePlanOutput")
            .field("index", &self.index)
            .field("table_id", &self.table_id)
            .field("table_name", &sanitize_text_for_display(&self.table_name))
            .field("output_name", &sanitize_text_for_display(&self.output_name))
            .finish()
    }
}

/// Cache decision for one `write_all` cache-selection pass.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MssqlOutputCacheDecision {
    /// No safe shared cache candidate was selected.
    NoCache { reason: MssqlNoCacheReason },
    /// Registered derived aliases that should be cached for selected outputs.
    ///
    /// This vector represents the cache frontier: eligible shared derived
    /// aliases that are not covered by any deeper eligible shared alias.
    CacheAliases(Vec<MssqlDerivedCacheAliasPlan>),
}

/// Conservative reason no cache alias was selected.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MssqlNoCacheReason {
    /// Cache selection only helps when at least two outputs use a candidate.
    FewerThanTwoOutputs,
    /// No registered derived alias is shared by at least two selected outputs.
    NoSharedRegisteredDerivedAlias,
    /// Candidate relationships could not produce a deterministic cache frontier.
    AmbiguousSharedDerivedAlias,
}

/// Selected registered derived alias cache candidate.
#[allow(dead_code)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MssqlDerivedCacheAliasPlan {
    table_id: u64,
    alias: String,
    output_indexes: Vec<usize>,
}

#[allow(dead_code)]
impl MssqlDerivedCacheAliasPlan {
    pub(super) fn new(table_id: u64, alias: String, output_indexes: Vec<usize>) -> Self {
        Self {
            table_id,
            alias,
            output_indexes,
        }
    }

    pub(super) fn from_registered(
        derived: &RegisteredDerivedTable,
        output_indexes: Vec<usize>,
    ) -> Self {
        Self {
            table_id: derived.table().id(),
            alias: derived.name().to_owned(),
            output_indexes,
        }
    }

    /// Returns the selected registered derived table id.
    #[must_use]
    pub(crate) const fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Returns the selected registered derived alias.
    #[must_use]
    pub(crate) fn alias(&self) -> &str {
        &self.alias
    }

    /// Returns selected output indexes that use this alias.
    #[must_use]
    pub(crate) fn output_indexes(&self) -> &[usize] {
        &self.output_indexes
    }
}

impl fmt::Debug for MssqlDerivedCacheAliasPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlDerivedCacheAliasPlan")
            .field("table_id", &self.table_id)
            .field("alias", &sanitize_text_for_display(&self.alias))
            .field("output_indexes", &self.output_indexes)
            .finish()
    }
}

/// Stream construction route for one output while cache aliases are active.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MssqlCachedOutputStreamRoute {
    /// The selected output table is itself an active cached alias.
    DirectCachedAlias(MssqlDerivedCacheAliasPlan),
    /// The selected output depends on one or more active cached aliases.
    ReplannedCachedDependency(Vec<MssqlDerivedCacheAliasPlan>),
    /// The selected output does not use any active cached alias.
    UncachedLazyTable,
}

/// Candidate skipped during conservative cache selection.
#[allow(dead_code)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MssqlCacheCandidateSkip {
    table_id: u64,
    alias: String,
    reason: MssqlCacheCandidateSkipReason,
}

#[allow(dead_code)]
impl MssqlCacheCandidateSkip {
    pub(super) fn from_registered(
        derived: &RegisteredDerivedTable,
        reason: MssqlCacheCandidateSkipReason,
    ) -> Self {
        Self {
            table_id: derived.table().id(),
            alias: derived.name().to_owned(),
            reason,
        }
    }

    /// Returns the skipped registered derived table id.
    #[must_use]
    pub(crate) const fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Returns the skipped registered derived alias.
    #[must_use]
    pub(crate) fn alias(&self) -> &str {
        &self.alias
    }

    /// Returns why the candidate was skipped.
    #[must_use]
    pub(crate) const fn reason(&self) -> &MssqlCacheCandidateSkipReason {
        &self.reason
    }
}

impl fmt::Debug for MssqlCacheCandidateSkip {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlCacheCandidateSkip")
            .field("table_id", &self.table_id)
            .field("alias", &sanitize_text_for_display(&self.alias))
            .field("reason", &self.reason)
            .finish()
    }
}

/// Reason a candidate was not eligible for cache selection.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MssqlCacheCandidateSkipReason {
    /// Fewer than two selected outputs use this candidate.
    NotShared { output_count: usize },
    /// Retained SQL text was missing, so later replanning would be unsafe.
    MissingSqlText,
    /// Lineage was incomplete or could not be trusted.
    IncompleteLineage,
    /// A deeper shared alias is closer to all dependent outputs.
    CoveredByDeeperSharedAlias { selected_table_id: u64 },
    /// The candidate's relative depth could not be ordered deterministically.
    AmbiguousDepth,
}

// Result of selecting the cache frontier from already eligible shared
// candidates.
pub(super) enum MssqlCacheFrontierSelection {
    Selected {
        // Candidates that remain on the frontier and should be cached.
        selected_aliases: Vec<MssqlDerivedCacheAliasPlan>,
        // Broader upstream candidates removed because one selected alias is
        // deeper and can cover the same upstream work more precisely.
        covered_aliases: Vec<MssqlCoveredCacheAlias>,
    },
    Ambiguous {
        // Candidates whose ordering could not produce a deterministic frontier.
        ambiguous_aliases: Vec<MssqlDerivedCacheAliasPlan>,
    },
}

pub(super) struct MssqlCoveredCacheAlias {
    // The skipped upstream alias.
    pub(super) alias: MssqlDerivedCacheAliasPlan,
    // The selected downstream alias that covered it.
    pub(super) selected_table_id: u64,
}

impl DeltaFunnelSession {
    #[allow(dead_code)]
    pub(crate) fn plan_mssql_output_cache(
        &self,
        requests: &[OutputWritePlan],
    ) -> MssqlOutputCachePlan {
        let selected_outputs = requests
            .iter()
            .enumerate()
            .map(|(index, request)| MssqlOutputCachePlanOutput::from_request(index, request))
            .collect::<Vec<_>>();
        if selected_outputs.len() < 2 {
            return MssqlOutputCachePlan::no_cache(
                selected_outputs,
                MssqlNoCacheReason::FewerThanTwoOutputs,
            );
        }

        let mut shared_candidates = Vec::new();
        let mut skipped_candidates = Vec::new();
        for derived in &self.derived_tables {
            if derived.sql_text().trim().is_empty() {
                skipped_candidates.push(MssqlCacheCandidateSkip::from_registered(
                    derived,
                    MssqlCacheCandidateSkipReason::MissingSqlText,
                ));
                continue;
            }
            if !derived.lineage().is_complete() {
                skipped_candidates.push(MssqlCacheCandidateSkip::from_registered(
                    derived,
                    MssqlCacheCandidateSkipReason::IncompleteLineage,
                ));
                continue;
            }

            let output_indexes = requests
                .iter()
                .enumerate()
                .filter_map(|(index, request)| {
                    self.cache_output_uses_registered_derived(request.table(), derived)
                        .then_some(index)
                })
                .collect::<Vec<_>>();
            if output_indexes.len() >= 2 {
                shared_candidates.push(MssqlDerivedCacheAliasPlan::from_registered(
                    derived,
                    output_indexes,
                ));
            } else {
                skipped_candidates.push(MssqlCacheCandidateSkip::from_registered(
                    derived,
                    MssqlCacheCandidateSkipReason::NotShared {
                        output_count: output_indexes.len(),
                    },
                ));
            }
        }

        if shared_candidates.len() == 1 {
            return MssqlOutputCachePlan::new(
                selected_outputs,
                MssqlOutputCacheDecision::CacheAliases(vec![shared_candidates.remove(0)]),
                skipped_candidates,
            );
        }
        if shared_candidates.len() > 1 {
            match self.select_shared_cache_frontier(shared_candidates) {
                MssqlCacheFrontierSelection::Selected {
                    selected_aliases,
                    covered_aliases,
                } => {
                    skipped_candidates.extend(covered_aliases.into_iter().filter_map(|covered| {
                        self.registered_derived_table_by_id(covered.alias.table_id())
                            .map(|derived| {
                                MssqlCacheCandidateSkip::from_registered(
                                    derived,
                                    MssqlCacheCandidateSkipReason::CoveredByDeeperSharedAlias {
                                        selected_table_id: covered.selected_table_id,
                                    },
                                )
                            })
                    }));
                    return MssqlOutputCachePlan::new(
                        selected_outputs,
                        MssqlOutputCacheDecision::CacheAliases(selected_aliases),
                        skipped_candidates,
                    );
                }
                MssqlCacheFrontierSelection::Ambiguous { ambiguous_aliases } => {
                    skipped_candidates.extend(ambiguous_aliases.into_iter().filter_map(
                        |candidate| {
                            self.registered_derived_table_by_id(candidate.table_id())
                                .map(|derived| {
                                    MssqlCacheCandidateSkip::from_registered(
                                        derived,
                                        MssqlCacheCandidateSkipReason::AmbiguousDepth,
                                    )
                                })
                        },
                    ));
                    return MssqlOutputCachePlan::new(
                        selected_outputs,
                        MssqlOutputCacheDecision::NoCache {
                            reason: MssqlNoCacheReason::AmbiguousSharedDerivedAlias,
                        },
                        skipped_candidates,
                    );
                }
            }
        }

        MssqlOutputCachePlan::new(
            selected_outputs,
            MssqlOutputCacheDecision::NoCache {
                reason: MssqlNoCacheReason::NoSharedRegisteredDerivedAlias,
            },
            skipped_candidates,
        )
    }

    /// Returns whether a selected output uses a registered derived candidate.
    ///
    /// Direct use and lineage use both count for cache selection. Direct use
    /// covers `big.to_mssql(...)`; lineage use covers downstream SQL such as
    /// `west` reading from `big`, including transitive derived dependencies.
    fn cache_output_uses_registered_derived(
        &self,
        table: &LazyTable,
        candidate: &RegisteredDerivedTable,
    ) -> bool {
        // The selected output itself can be the cache candidate.
        if table.id() == candidate.table().id() {
            return true;
        }
        // Raw Delta sources cannot depend on registered derived aliases.
        if table.kind() == LazyTableKind::DeltaSource {
            return false;
        }

        // Pending or registered derived outputs use captured lineage. If lineage
        // lookup fails, the candidate is not counted for this output.
        self.transitive_registered_derived_dependencies(table)
            .map(|dependencies| {
                dependencies.iter().any(|dependency| {
                    matches!(
                        dependency,
                        DerivedTableDependency::RegisteredDerived { table_id, .. }
                            if *table_id == candidate.table().id()
                    )
                })
            })
            .unwrap_or(false)
    }

    /// Selects the cache frontier from eligible shared derived aliases.
    ///
    /// The frontier is every shared alias that is not covered by a deeper
    /// shared alias. Chain-shaped candidates collapse to the deepest alias,
    /// while independent candidates are kept together even when they serve the
    /// same selected output indexes.
    fn select_shared_cache_frontier(
        &self,
        candidates: Vec<MssqlDerivedCacheAliasPlan>,
    ) -> MssqlCacheFrontierSelection {
        // A candidate is covered when another shared candidate depends on it.
        // Covered aliases are useful upstream work, but caching the deeper
        // shared alias is closer to the final selected outputs.
        let deepest_indexes = candidates
            .iter()
            .enumerate()
            .filter_map(|(candidate_index, candidate)| {
                let covered_by_deeper_candidate =
                    candidates.iter().enumerate().any(|(other_index, other)| {
                        candidate_index != other_index
                            && self.cache_candidate_is_deeper_than(other, candidate)
                    });
                (!covered_by_deeper_candidate).then_some(candidate_index)
            })
            .collect::<Vec<_>>();

        match deepest_indexes.as_slice() {
            [] => MssqlCacheFrontierSelection::Ambiguous {
                ambiguous_aliases: candidates,
            },
            [_, ..] => {
                // The indexes that remain are the frontier. More than one means
                // independent shared aliases, not ambiguity, because no selected
                // alias can replace the work represented by another.
                let selected_aliases = deepest_indexes
                    .iter()
                    .map(|index| candidates[*index].clone())
                    .collect::<Vec<_>>();
                // Keep covered aliases visible in the plan so later reports can
                // explain why broader upstream candidates were not selected.
                let covered_aliases = candidates
                    .iter()
                    .enumerate()
                    .filter_map(|(candidate_index, candidate)| {
                        if deepest_indexes.contains(&candidate_index) {
                            return None;
                        }
                        selected_aliases
                            .iter()
                            .find(|selected| {
                                self.cache_candidate_is_deeper_than(selected, candidate)
                            })
                            .map(|selected| MssqlCoveredCacheAlias {
                                alias: candidate.clone(),
                                selected_table_id: selected.table_id(),
                            })
                    })
                    .collect::<Vec<_>>();
                if deepest_indexes.len() + covered_aliases.len() != candidates.len() {
                    return MssqlCacheFrontierSelection::Ambiguous {
                        ambiguous_aliases: candidates,
                    };
                }
                MssqlCacheFrontierSelection::Selected {
                    selected_aliases,
                    covered_aliases,
                }
            }
        }
    }

    /// Returns whether `candidate` is a downstream derived alias of `other`.
    ///
    /// This is the ordering test used by frontier selection. It is intentionally
    /// based on session-owned table identity plus captured lineage, not on alias
    /// names, SQL text, registration order, or output order.
    fn cache_candidate_is_deeper_than(
        &self,
        candidate: &MssqlDerivedCacheAliasPlan,
        other: &MssqlDerivedCacheAliasPlan,
    ) -> bool {
        if candidate.table_id() == other.table_id() {
            return false;
        }

        // Missing metadata should not create a deeper-than relationship. The
        // caller treats unprovable ordering conservatively when needed.
        let Some(candidate_table) = self.registered_derived_table_by_id(candidate.table_id())
        else {
            return false;
        };
        let Some(other_table) = self.registered_derived_table_by_id(other.table_id()) else {
            return false;
        };

        // Reuse the same direct-or-transitive dependency check as output
        // classification. If candidate depends on other, candidate is closer to
        // outputs that use candidate and can cover the broader upstream alias.
        self.cache_output_uses_registered_derived(candidate_table.table(), other_table)
    }

    /// Materializes one registered derived alias and temporarily replaces that alias.
    ///
    /// The method leaves the original catalog alias active while DataFusion
    /// builds the cache, then swaps the catalog entry to the cached provider.
    /// The returned scope owns the original provider and must be restored with
    /// `MssqlScopedCacheAliasReplacement::restore`.
    ///
    /// This is intentionally a one-alias primitive. It does not choose cache
    /// candidates, replan downstream SQL, or execute any outputs.
    #[allow(dead_code)]
    pub(crate) async fn replace_registered_derived_alias_with_cache(
        &self,
        table: &LazyTable,
    ) -> Result<MssqlScopedCacheAliasReplacement<'_>, DeltaFunnelError> {
        let registered = self.registered_derived_for_scoped_cache_alias(table)?;
        let table_id = registered.table().id();
        let alias_name = registered.name().to_owned();

        let cached_provider = self
            .context
            .table(alias_name.as_str())
            .await
            .map_err(|error| mssql_scoped_cache_alias_error("resolve", alias_name.as_str(), error))?
            .cache()
            .await
            .map_err(|error| {
                mssql_scoped_cache_alias_error("materialize", alias_name.as_str(), error)
            })?
            .into_view();

        let original_provider =
            self.install_scoped_cache_alias_provider(alias_name.as_str(), cached_provider)?;

        Ok(MssqlScopedCacheAliasReplacement::new(
            &self.context,
            table_id,
            alias_name,
            original_provider,
        ))
    }

    pub(super) async fn replace_mssql_cache_aliases(
        &self,
        cache_aliases: &[MssqlDerivedCacheAliasPlan],
    ) -> Result<Vec<MssqlScopedCacheAliasReplacement<'_>>, DeltaFunnelError> {
        let mut replacements = Vec::new();

        for cache_alias in cache_aliases {
            let table = self
                .registered_derived_table_by_id(cache_alias.table_id())
                .ok_or_else(|| unknown_cached_alias_error(cache_alias))?
                .table()
                .clone();

            match self
                .replace_registered_derived_alias_with_cache(&table)
                .await
            {
                Ok(replacement) => replacements.push(replacement),
                Err(error) => {
                    return Err(restore_mssql_cache_aliases_after_error(error, replacements).await);
                }
            }
        }

        Ok(replacements)
    }

    /// Swaps a catalog alias from its original provider to a cached provider.
    ///
    /// On success, the alias points at `cached_provider` and the original
    /// provider is returned to the caller for later restoration. If registering
    /// the cached provider fails after the original provider has been removed,
    /// this helper attempts to put the original provider back before returning
    /// the error.
    fn install_scoped_cache_alias_provider(
        &self,
        alias_name: &str,
        cached_provider: Arc<dyn TableProvider>,
    ) -> Result<Arc<dyn TableProvider>, DeltaFunnelError> {
        let original_provider = self
            .context
            .deregister_table(alias_name)
            .map_err(|error| mssql_scoped_cache_alias_error("deregister", alias_name, error))?
            .ok_or_else(|| {
                mssql_scoped_cache_alias_error(
                    "deregister",
                    alias_name,
                    "registered alias was missing from the catalog",
                )
            })?;

        if let Err(register_error) = self.context.register_table(alias_name, cached_provider) {
            return Err(self.restore_original_after_cached_register_failure(
                alias_name,
                original_provider,
                register_error,
            ));
        }

        Ok(original_provider)
    }

    /// Restores the original provider after cached-provider registration fails.
    ///
    /// This helper is used only for the narrow failure window where the
    /// original provider has already been deregistered but the cached provider
    /// could not be registered. The returned error reports the cached register
    /// failure and, if restoration also fails, includes that cleanup failure.
    pub(super) fn restore_original_after_cached_register_failure(
        &self,
        alias_name: &str,
        original_provider: Arc<dyn TableProvider>,
        register_error: impl fmt::Display,
    ) -> DeltaFunnelError {
        let restore_result = self.context.register_table(alias_name, original_provider);
        let message = match restore_result {
            Ok(_) => format!(
                "failed to register cached provider for alias `{}`: {}",
                sanitize_text_for_display(alias_name),
                sanitize_text_for_display(&register_error.to_string())
            ),
            Err(restore_error) => format!(
                "failed to register cached provider for alias `{}`: {}; also failed to restore original provider: {}",
                sanitize_text_for_display(alias_name),
                sanitize_text_for_display(&register_error.to_string()),
                sanitize_text_for_display(&restore_error.to_string())
            ),
        };

        DeltaFunnelError::MssqlWorkflowPlanning { message }
    }
}

pub(super) async fn restore_mssql_cache_aliases(
    replacements: Vec<MssqlScopedCacheAliasReplacement<'_>>,
) -> Result<Vec<MssqlScopedCacheAliasRestoration>, DeltaFunnelError> {
    let mut restorations = Vec::new();
    let mut first_error = None;

    for replacement in replacements.into_iter().rev() {
        match replacement.restore().await {
            Ok(restoration) => restorations.push(restoration),
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(restorations),
    }
}

pub(super) async fn restore_mssql_cache_aliases_after_error(
    error: DeltaFunnelError,
    replacements: Vec<MssqlScopedCacheAliasReplacement<'_>>,
) -> DeltaFunnelError {
    match restore_mssql_cache_aliases(replacements).await {
        Ok(_restorations) => error,
        Err(restore_error) => cache_error_with_restore_error(error, restore_error),
    }
}

pub(super) fn cache_error_with_restore_error(
    error: DeltaFunnelError,
    restore_error: DeltaFunnelError,
) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWorkflowPlanning {
        message: format!(
            "write_all auto cache failed: {}; also failed to restore cache aliases: {}",
            sanitize_text_for_display(&error.to_string()),
            sanitize_text_for_display(&restore_error.to_string())
        ),
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

pub(super) fn ensure_unique_write_all_output_names(
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
    use super::{WriteAllCacheMode, WriteAllOptions};

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
}
