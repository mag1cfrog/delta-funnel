//! Rust backing model for lazy query-load orchestration.
//!
//! This module owns the high-level session and request data shapes that will
//! later back the Python `Session` and `Table` API. Metadata report helpers do
//! not contact SQL Server or execute rows unless a write path explicitly does so.

mod dry_run_report;
mod errors;
mod handles;
mod options;
mod registry;
mod source_report;
mod streams;
mod write_all;

use std::{collections::BTreeSet, fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::DataFrame;
use datafusion::prelude::{SQLOptions, SessionContext};
use futures_util::StreamExt;

use crate::{
    DeltaFunnelError, DeltaSourceConfig, DeltaTableProviderConfig, MssqlOutputBatchStream,
    MssqlOutputWriteJob, MssqlSchemaPlanOptions, MssqlTargetOutputPlan, MssqlWorkflowOutputWriter,
    MssqlWorkflowWriteReport, MssqlWriteOptions, MssqlWriteReport, ResolvedMssqlTarget,
    SqlTablePhase, datafusion_query_output_stream, datafusion_session_context, load_delta_source,
    plan_mssql_target_for_resolved_output, preflight_delta_protocol,
    register_delta_sources_with_scan_execution_options, support::sanitize_text_for_display,
    table_formats::validate_table_source_names, write_mssql_outputs_with_writer,
    write_output_batches_to_mssql,
};

pub use handles::{
    LazyTable, LazyTableKind, MssqlOutputTarget, OutputWritePlan, PlannedMssqlOutput, RunMode,
};
pub use options::SessionOptions;
pub use registry::{RegisteredDerivedTable, RegisteredSessionSource};
pub use source_report::{DeltaProviderSchedulingReport, DeltaSourceReport, SourceUsageStatus};
pub use write_all::{
    WriteAllCacheAliasReport, WriteAllCacheAliasStatus, WriteAllCacheCandidateSkip,
    WriteAllCacheCandidateSkipReason, WriteAllCacheMode, WriteAllCacheReport,
    WriteAllNoCacheReason, WriteAllOptions, WriteAllReport,
};

pub use dry_run_report::{
    MssqlDryRunOutputFieldReport, MssqlDryRunOutputReport, MssqlDryRunSqlIdentityReport,
    MssqlDryRunSqlIdentityState, MssqlDryRunWorkflowReport,
};
use errors::{datafusion_handoff_setup_error, sql_table_error, unknown_lazy_table_error};
use registry::PendingDerivedTable;
use streams::{
    SharedProviderReadStats, dataframe_for_lazy_table_from_session_parts,
    provider_read_stats_snapshot, shared_provider_read_stats,
};
use write_all::{
    MssqlDerivedCacheAliasPlan, MssqlOutputCacheDecision, MssqlOutputCachePlan,
    cache_error_with_restore_error, restore_mssql_cache_aliases,
    restore_mssql_cache_aliases_after_error,
};

/// Rust backing session for lazy query-load workflows.
pub struct DeltaFunnelSession {
    options: SessionOptions,
    context: SessionContext,
    next_table_id: u64,
    sources: Vec<RegisteredSessionSource>,
    derived_tables: Vec<RegisteredDerivedTable>,
    pending_derived_tables: Vec<PendingDerivedTable>,
}

impl DeltaFunnelSession {
    /// Builds a new session with validated local options.
    ///
    /// # Errors
    ///
    /// Returns the first local option validation failure before any source
    /// loading, SQL planning, SQL Server connection, or row execution.
    pub fn new(options: SessionOptions) -> Result<Self, DeltaFunnelError> {
        options.validate()?;
        let context = datafusion_session_context(options.query_options())?;
        Ok(Self {
            options,
            context,
            next_table_id: 0,
            sources: Vec::new(),
            derived_tables: Vec::new(),
            pending_derived_tables: Vec::new(),
        })
    }

    /// Returns the validated session options.
    #[must_use]
    pub const fn options(&self) -> &SessionOptions {
        &self.options
    }

    /// Returns the DataFusion session context owned by this orchestrator.
    ///
    /// The session context is exposed so later planning steps can analyze SQL
    /// against registered session aliases. Delta source registration should
    /// still go through [`DeltaFunnelSession::delta_lake`] so the orchestrator's
    /// source reports stay aligned with the DataFusion catalog.
    #[must_use]
    pub const fn context(&self) -> &SessionContext {
        &self.context
    }

    /// Returns the next unassigned session-local lazy table id.
    #[must_use]
    pub const fn next_table_id(&self) -> u64 {
        self.next_table_id
    }

    /// Builds a lazy SQL-derived table without registering a query alias.
    ///
    /// The SQL must be one read-only tabular query. Planning uses DataFusion to
    /// produce a lazy table provider and does not execute rows, contact SQL
    /// Server, or create an output target.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaFunnelError::SqlTable`] when the SQL is empty, contains
    /// an unsupported or non-read-only statement, or cannot be planned against
    /// the session's registered aliases.
    pub async fn table_from_sql(&mut self, sql: &str) -> Result<LazyTable, DeltaFunnelError> {
        let sql = sql.trim();
        if sql.is_empty() {
            return sql_table_error(SqlTablePhase::ValidateSql, "SQL text must not be empty");
        }

        let dataframe = self.plan_read_only_sql(sql).await?;
        let schema = Arc::new(dataframe.schema().as_arrow().clone());
        let provider = dataframe.into_view();
        let lineage = self.derive_table_lineage_from_sql(sql);
        let table = self.allocate_derived_sql_table();
        self.pending_derived_tables.push(PendingDerivedTable {
            table: table.clone(),
            provider,
            schema,
            sql_text: sql.to_owned(),
            lineage,
        });
        Ok(table)
    }

    /// Registers a session-owned alias for a lazy SQL-derived table.
    ///
    /// Alias names use the same unquoted identifier rules as Delta source
    /// aliases. The alias is registered into the session's DataFusion catalog
    /// only after all local validation succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaFunnelError::InvalidSourceName`] or
    /// [`DeltaFunnelError::DuplicateSourceName`] for invalid or ambiguous
    /// aliases, and [`DeltaFunnelError::SqlTable`] when the table handle is not
    /// a pending SQL-derived table owned by this session or DataFusion rejects
    /// the alias registration.
    pub fn register_alias(
        &mut self,
        name: impl Into<String>,
        table: &LazyTable,
    ) -> Result<LazyTable, DeltaFunnelError> {
        let name = name.into();
        validate_table_source_names([name.as_str()])?;
        self.reject_registered_alias_name(&name)?;

        let Some(index) = self.find_pending_derived_table(table) else {
            return sql_table_error(
                SqlTablePhase::RegisterDerivedAlias,
                "lazy table is not a pending SQL-derived table owned by this session",
            );
        };
        let pending = self.pending_derived_tables.remove(index);

        if let Err(error) = self
            .context
            .register_table(name.as_str(), Arc::clone(&pending.provider))
        {
            let message = error.to_string();
            self.pending_derived_tables.push(pending);
            return sql_table_error(SqlTablePhase::RegisterDerivedAlias, message);
        }

        let alias_table = pending.table.with_name(name);
        self.derived_tables.push(RegisteredDerivedTable::new(
            alias_table.clone(),
            pending.schema,
            pending.sql_text,
            pending.lineage,
        ));
        Ok(alias_table)
    }

    /// Registers one Delta source and returns its lazy table handle.
    ///
    /// The method performs source setup only: Delta snapshot metadata loading,
    /// protocol preflight, and DataFusion table registration. It does not scan
    /// data files for row production, parse user SQL, contact SQL Server, or
    /// execute an output action.
    ///
    /// # Errors
    ///
    /// Returns the first Delta source loading, protocol preflight, duplicate
    /// alias, schema conversion, or DataFusion registration error. Session
    /// source state is updated only after the DataFusion registration succeeds.
    pub fn delta_lake(&mut self, source: DeltaSourceConfig) -> Result<LazyTable, DeltaFunnelError> {
        self.reject_registered_alias_name(&source.name)?;
        let planned = load_delta_source(source)?;
        let preflight = preflight_delta_protocol(&planned)?;
        let registered = register_delta_sources_with_scan_execution_options(
            &self.context,
            vec![DeltaTableProviderConfig {
                source: planned,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            self.options.provider_scan_options(),
        )?;
        let registered =
            registered
                .sources
                .into_iter()
                .next()
                .ok_or_else(|| DeltaFunnelError::Config {
                    message: "Delta source registration returned no registered source".to_owned(),
                })?;
        let table = self.allocate_delta_source_table(registered.name.clone());
        let session_source = RegisteredSessionSource::from_registered(table.clone(), registered);
        self.sources.push(session_source);
        Ok(table)
    }

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

    fn build_write_all_baseline_jobs_with_provider_stats(
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
    fn build_write_all_cached_jobs(
        &self,
        planned_outputs: &[PlannedMssqlOutput],
        active_aliases: &[MssqlDerivedCacheAliasPlan],
    ) -> Result<Vec<MssqlOutputWriteJob>, DeltaFunnelError> {
        self.build_write_all_cached_jobs_with_provider_stats(planned_outputs, active_aliases, None)
    }

    fn build_write_all_cached_jobs_with_provider_stats(
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

    async fn plan_read_only_sql(&self, sql: &str) -> Result<DataFrame, DeltaFunnelError> {
        self.context
            .sql_with_options(sql, read_only_sql_options())
            .await
            .map_err(|error| DeltaFunnelError::SqlTable {
                phase: classify_sql_error_phase(&error.to_string()),
                message: error.to_string(),
            })
    }

    fn allocate_delta_source_table(&mut self, name: String) -> LazyTable {
        let id = self.next_table_id;
        self.next_table_id = self.next_table_id.saturating_add(1);
        LazyTable::delta_source(id, name)
    }

    fn allocate_derived_sql_table(&mut self) -> LazyTable {
        let id = self.next_table_id;
        self.next_table_id = self.next_table_id.saturating_add(1);
        LazyTable::derived_sql(id)
    }

    fn reject_registered_alias_name(&self, name: &str) -> Result<(), DeltaFunnelError> {
        if self.registered_source(name).is_some() || self.registered_derived_table(name).is_some() {
            return Err(DeltaFunnelError::DuplicateSourceName {
                name: name.to_owned(),
            });
        }
        Ok(())
    }

    fn find_pending_derived_table(&self, table: &LazyTable) -> Option<usize> {
        if table.kind() != LazyTableKind::DerivedSql {
            return None;
        }

        self.pending_derived_tables
            .iter()
            .position(|pending| pending.table.id() == table.id())
    }

    pub(crate) async fn batch_stream_for_lazy_table(
        &self,
        table: &LazyTable,
    ) -> Result<MssqlOutputBatchStream, DeltaFunnelError> {
        let dataframe = self.dataframe_for_lazy_table(table).await?;
        let physical_plan = dataframe
            .create_physical_plan()
            .await
            .map_err(|error| datafusion_handoff_setup_error("physical_plan", error))?;
        let stream = datafusion_query_output_stream(physical_plan, self.context.task_ctx())
            .map_err(|error| datafusion_handoff_setup_error("query_output_stream", error))?;

        Ok(Box::pin(stream.map(|batch| {
            batch.map_err(|error| datafusion_handoff_setup_error("query_output_stream", error))
        })))
    }

    async fn dataframe_for_lazy_table(
        &self,
        table: &LazyTable,
    ) -> Result<DataFrame, DeltaFunnelError> {
        dataframe_for_lazy_table_from_session_parts(
            &self.context,
            table,
            &self.sources,
            &self.derived_tables,
            &self.pending_derived_tables,
        )
        .await
    }

    fn schema_for_lazy_table(&self, table: &LazyTable) -> Result<&SchemaRef, DeltaFunnelError> {
        match table.kind() {
            LazyTableKind::DeltaSource => self
                .sources
                .iter()
                .find(|source| source.table().id() == table.id())
                .map(RegisteredSessionSource::schema),
            LazyTableKind::DerivedSql => self
                .derived_tables
                .iter()
                .find(|derived| derived.table().id() == table.id())
                .map(RegisteredDerivedTable::schema)
                .or_else(|| {
                    self.pending_derived_tables
                        .iter()
                        .find(|pending| pending.table.id() == table.id())
                        .map(|pending| &pending.schema)
                }),
        }
        .ok_or_else(|| unknown_lazy_table_error(table))
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

impl fmt::Debug for DeltaFunnelSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaFunnelSession")
            .field("options", &self.options)
            .field("sources", &self.sources)
            .field("derived_tables", &self.derived_tables)
            .field("next_table_id", &self.next_table_id)
            .finish_non_exhaustive()
    }
}

fn read_only_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

fn classify_sql_error_phase(error: &str) -> SqlTablePhase {
    if error.contains("DDL not supported")
        || error.contains("DML not supported")
        || error.contains("Statement not supported")
        || error.contains("only supports a single SQL statement")
    {
        SqlTablePhase::ValidateSql
    } else {
        SqlTablePhase::PlanSql
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

fn ensure_unique_write_all_output_names(
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
    use std::{
        any::Any,
        fs,
        path::{Path, PathBuf},
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::registry::{DerivedTableDependency, DerivedTableLineage};
    use super::write_all::MssqlScopedCacheAliasReplacement;
    use super::write_all::{
        MssqlCacheCandidateSkipReason, MssqlCachedOutputStreamRoute, MssqlNoCacheReason,
    };
    use super::*;
    use crate::{
        DeltaProviderReaderBackend, DeltaProviderScanExecutionOptions, DeltaStorageOptions,
        LoadMode, MssqlConnectionConfig, MssqlConnectionSource, MssqlTargetCleanupStatus,
        MssqlTargetConfig, MssqlTargetTable, OutputStatus, QueryOptions, ReportReasonCode,
        ValidationOptions, ValidationStatus, WorkflowStatus, table_formats::RealParquetDeltaTable,
    };
    use async_trait::async_trait;
    use datafusion::{
        arrow::{
            array::{Array, ArrayRef, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        },
        catalog::Session,
        common::tree_node::{TreeNode, TreeNodeRecursion},
        datasource::{MemTable, TableProvider},
        error::{DataFusionError, Result as DataFusionResult},
        logical_expr::{Expr, LogicalPlan, TableType},
        physical_plan::ExecutionPlan,
        sql::{parser::DFParser, resolve::resolve_table_references},
    };

    struct DeltaLogTable {
        path: PathBuf,
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    impl DeltaLogTable {
        fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            Self::new_with_protocol_and_schema(name, PROTOCOL_JSON, DEFAULT_SCHEMA_FIELDS_JSON)
        }

        fn new_with_protocol(
            name: &str,
            protocol_json: &str,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            Self::new_with_protocol_and_schema(name, protocol_json, DEFAULT_SCHEMA_FIELDS_JSON)
        }

        fn new_with_schema(
            name: &str,
            schema_fields_json: &str,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            Self::new_with_protocol_and_schema(name, PROTOCOL_JSON, schema_fields_json)
        }

        fn new_with_protocol_and_schema(
            name: &str,
            protocol_json: &str,
            schema_fields_json: &str,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-funnel-orchestrator-tests")
                .join(unique_name(name)?);
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{}\n{}\n", protocol_json, metadata_json(schema_fields_json)),
            )?;
            fs::write(
                log_path.join("00000000000000000001.json"),
                format!("{}\n", add_json("part-00000.parquet")),
            )?;

            Ok(Self { path })
        }

        fn uri(&self) -> String {
            self.path.to_string_lossy().to_string()
        }

        fn file_uri_with_secret_parts(&self) -> Result<String, Box<dyn std::error::Error>> {
            let path = fs::canonicalize(&self.path)?;

            Ok(format!(
                "file://{}?token=super-secret#debug-secret",
                path.to_string_lossy()
            ))
        }
    }

    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const UNSUPPORTED_PROTOCOL_JSON: &str =
        r#"{"protocol":{"minReaderVersion":99,"minWriterVersion":2}}"#;
    const DEFAULT_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
    const UNSUPPORTED_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]"#;

    fn metadata_json(schema_fields_json: &str) -> String {
        format!(
            r#"{{"metaData":{{"id":"delta-funnel-test","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":{schema_fields_json}}}","partitionColumns":[],"configuration":{{}},"createdTime":1587968585495}}}}"#
        )
    }

    fn add_json(path: &str) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":0,"modificationTime":1587968586000,"dataChange":true}}}}"#
        )
    }

    fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        Ok(format!("{}-{name}-{nanos}", std::process::id()))
    }

    fn secret_connection() -> Result<MssqlConnectionConfig, DeltaFunnelError> {
        Ok(MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label("warehouse-primary"))
    }

    fn override_connection() -> Result<MssqlConnectionConfig, DeltaFunnelError> {
        Ok(MssqlConnectionConfig::new(
            "server=tcp:override.example.com;database=warehouse;user=writer;password=override-secret",
        )?
        .with_display_label("warehouse-override"))
    }

    fn output_request(
        table: LazyTable,
        output_name: &str,
        target_table: &str,
        load_mode: LoadMode,
    ) -> Result<OutputWritePlan, DeltaFunnelError> {
        output_request_with_run_mode(table, output_name, target_table, load_mode, RunMode::DryRun)
    }

    fn execute_output_request(
        table: LazyTable,
        output_name: &str,
        target_table: &str,
        load_mode: LoadMode,
    ) -> Result<OutputWritePlan, DeltaFunnelError> {
        output_request_with_run_mode(
            table,
            output_name,
            target_table,
            load_mode,
            RunMode::Execute,
        )
    }

    fn output_request_with_run_mode(
        table: LazyTable,
        output_name: &str,
        target_table: &str,
        load_mode: LoadMode,
        run_mode: RunMode,
    ) -> Result<OutputWritePlan, DeltaFunnelError> {
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", target_table)?)
            .with_load_mode(load_mode);
        Ok(OutputWritePlan::new(
            table,
            MssqlOutputTarget::new(output_name, target_config, run_mode),
        ))
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct FakeOrchestratorWriteCall {
        output_name: String,
        target_table: MssqlTargetTable,
        connection_source: MssqlConnectionSource,
        rows: u64,
        batches: u64,
        schema_fields: usize,
    }

    #[derive(Default)]
    struct FakeOrchestratorWriter {
        calls: Vec<FakeOrchestratorWriteCall>,
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
    impl OrchestratorMssqlOutputWriter for FakeOrchestratorWriter {
        async fn write_output(
            &mut self,
            output_schema: SchemaRef,
            output_plan: MssqlTargetOutputPlan,
            resolved_target: ResolvedMssqlTarget,
            mut batches: MssqlOutputBatchStream,
            _write_options: MssqlWriteOptions,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            let mut rows = 0_u64;
            let mut batch_count = 0_u64;

            while let Some(batch) = batches.next().await {
                let batch = batch?;
                rows = rows.saturating_add(u64::try_from(batch.num_rows()).map_err(|_| {
                    DeltaFunnelError::Config {
                        message: "fake writer row count overflowed u64".to_owned(),
                    }
                })?);
                batch_count = batch_count.saturating_add(1);
            }

            self.calls.push(FakeOrchestratorWriteCall {
                output_name: resolved_target.output_name().to_owned(),
                target_table: resolved_target.table().clone(),
                connection_source: resolved_target.connection_source(),
                rows,
                batches: batch_count,
                schema_fields: output_schema.fields().len(),
            });

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
                    connection_source: resolved_target.connection_source(),
                    rows,
                    batches: batch_count,
                    schema_fields: output_schema.fields().len(),
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

    async fn collect_stream_row_count(
        mut stream: MssqlOutputBatchStream,
    ) -> Result<usize, DeltaFunnelError> {
        let mut rows = 0_usize;

        while let Some(batch) = stream.next().await {
            rows = rows.saturating_add(batch?.num_rows());
        }

        Ok(rows)
    }

    async fn collect_stream_marker_values(
        mut stream: MssqlOutputBatchStream,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut batches = Vec::new();

        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        marker_values_from_batches(&batches)
    }

    fn marker_values_from_batches(
        batches: &[RecordBatch],
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut markers = Vec::new();

        for batch in batches {
            let column = batch
                .column_by_name("marker")
                .ok_or_else(|| std::io::Error::other("expected marker column"))?;
            let strings = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| std::io::Error::other("expected marker StringArray"))?;

            for row in 0..strings.len() {
                markers.push(strings.value(row).to_owned());
            }
        }

        Ok(markers)
    }

    fn marker_region_provider(
        marker: &str,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("marker", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![marker, marker])) as ArrayRef,
                Arc::new(StringArray::from(vec!["west", "east"])) as ArrayRef,
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    #[derive(Debug)]
    struct ScanCountingProvider {
        table: MemTable,
        scans: Arc<AtomicUsize>,
    }

    #[derive(Debug)]
    struct FailingScanProvider {
        schema: SchemaRef,
        scans: Arc<AtomicUsize>,
    }

    type CountedProvider = (Arc<dyn TableProvider>, Arc<AtomicUsize>);

    #[async_trait]
    impl TableProvider for ScanCountingProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.table.schema()
        }

        fn table_type(&self) -> TableType {
            self.table.table_type()
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.scans.fetch_add(1, Ordering::SeqCst);
            self.table.scan(state, projection, filters, limit).await
        }
    }

    #[async_trait]
    impl TableProvider for FailingScanProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.scans.fetch_add(1, Ordering::SeqCst);
            Err(DataFusionError::Execution(
                "forced scan planning failure".to_owned(),
            ))
        }
    }

    fn scan_counting_marker_region_provider(
        marker: &str,
    ) -> Result<CountedProvider, Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("marker", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![marker, marker])) as ArrayRef,
                Arc::new(StringArray::from(vec!["west", "east"])) as ArrayRef,
            ],
        )?;
        let scans = Arc::new(AtomicUsize::new(0));
        let provider = ScanCountingProvider {
            table: MemTable::try_new(schema, vec![vec![batch]])?,
            scans: Arc::clone(&scans),
        };

        Ok((Arc::new(provider), scans))
    }

    fn failing_scan_marker_region_provider() -> CountedProvider {
        let schema = Arc::new(Schema::new(vec![
            Field::new("marker", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let scans = Arc::new(AtomicUsize::new(0));
        let provider = FailingScanProvider {
            schema,
            scans: Arc::clone(&scans),
        };

        (Arc::new(provider), scans)
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TableScanProofReference {
        table_name: String,
        nested_table_names: Vec<String>,
    }

    fn table_scan_proof_references(
        plan: &LogicalPlan,
    ) -> DataFusionResult<Vec<TableScanProofReference>> {
        let mut references = Vec::new();

        plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                let nested_table_names = scan
                    .source
                    .get_logical_plan()
                    .map(|nested| table_scan_table_names(nested.as_ref()))
                    .transpose()?
                    .unwrap_or_default();
                references.push(TableScanProofReference {
                    table_name: scan.table_name.table().to_owned(),
                    nested_table_names,
                });
            }

            Ok(TreeNodeRecursion::Continue)
        })?;

        Ok(references)
    }

    fn table_scan_table_names(plan: &LogicalPlan) -> DataFusionResult<Vec<String>> {
        let mut names = Vec::new();

        plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                names.push(scan.table_name.table().to_owned());
            }

            Ok(TreeNodeRecursion::Continue)
        })?;

        Ok(names)
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct AstReferenceProof {
        relations: Vec<String>,
        ctes: Vec<String>,
    }

    fn ast_reference_proof(sql: &str) -> Result<AstReferenceProof, Box<dyn std::error::Error>> {
        let mut statements = DFParser::parse_sql(sql)?;
        if statements.len() != 1 {
            return Err(std::io::Error::other("expected exactly one parsed statement").into());
        }
        let statement = statements
            .pop_front()
            .ok_or_else(|| std::io::Error::other("expected parsed statement"))?;
        let (relations, ctes) = resolve_table_references(&statement, true)?;

        Ok(AstReferenceProof {
            relations: relations
                .into_iter()
                .map(|reference| reference.to_string())
                .collect(),
            ctes: ctes
                .into_iter()
                .map(|reference| reference.to_string())
                .collect(),
        })
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

    #[test]
    fn plan_mssql_output_uses_source_schema_and_session_connection()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source.clone(),
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let planned = session.plan_mssql_output(&request)?;

        assert_eq!(planned.request(), &request);
        assert_eq!(planned.table(), &source);
        assert_eq!(planned.target().run_mode(), RunMode::DryRun);
        assert_eq!(planned.output_plan().output_name(), "orders_output");
        assert_eq!(planned.output_plan().target_table().schema(), Some("dbo"));
        assert_eq!(planned.output_plan().target_table().table(), "orders_sink");
        assert_eq!(
            planned.output_plan().connection_source(),
            MssqlConnectionSource::ContextDefault
        );
        assert_eq!(
            planned.output_plan().connection().display_label(),
            Some("warehouse-primary")
        );
        assert_eq!(planned.output_plan().schema_mappings().len(), 2);
        assert_eq!(
            planned.output_plan().schema_mappings()[0].arrow().name(),
            "id"
        );
        assert_eq!(
            planned.output_plan().schema_mappings()[1].arrow().name(),
            "customer_name"
        );
        assert_eq!(planned.output_plan().create_table_sql(), None);
        Ok(())
    }

    #[tokio::test]
    async fn plan_mssql_output_uses_pending_derived_schema_without_row_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session.table_from_sql("select id from orders").await?;
        let request = output_request(
            derived.clone(),
            "derived_orders_output",
            "derived_orders",
            LoadMode::CreateAndLoad,
        )?;

        let planned = session.plan_mssql_output(&request)?;

        assert_eq!(planned.table(), &derived);
        assert_eq!(planned.output_plan().load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(planned.output_plan().schema_mappings().len(), 1);
        assert_eq!(
            planned.output_plan().schema_mappings()[0].arrow().name(),
            "id"
        );
        let create_table_sql = planned
            .output_plan()
            .create_table_sql()
            .ok_or("expected create table SQL")?;
        assert!(create_table_sql.contains("[dbo].[derived_orders]"));
        Ok(())
    }

    #[tokio::test]
    async fn plan_mssql_output_accepts_registered_derived_alias_handle()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let alias = session.register_alias("customer_names", &derived)?;
        let request = output_request(
            alias.clone(),
            "customer_names_output",
            "customer_names_sink",
            LoadMode::AppendExisting,
        )?;

        let planned = session.plan_mssql_output(&request)?;

        assert_eq!(planned.table(), &alias);
        assert_eq!(planned.output_plan().schema_mappings().len(), 1);
        assert_eq!(
            planned.output_plan().schema_mappings()[0].arrow().name(),
            "customer_name"
        );
        Ok(())
    }

    #[test]
    fn plan_mssql_output_connection_override_wins_without_mutating_session_default()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders_sink")?)
            .with_connection(override_connection()?);
        let request = OutputWritePlan::new(
            source,
            MssqlOutputTarget::new("orders_output", target_config, RunMode::DryRun),
        );

        let planned = session.plan_mssql_output(&request)?;

        assert_eq!(
            planned.output_plan().connection_source(),
            MssqlConnectionSource::TargetOverride
        );
        assert_eq!(
            planned.output_plan().connection().display_label(),
            Some("warehouse-override")
        );
        assert_eq!(
            session
                .options()
                .default_mssql_connection()
                .ok_or("expected default connection")?
                .summary()
                .display_label(),
            Some("warehouse-primary")
        );
        Ok(())
    }

    #[test]
    fn plan_mssql_output_missing_effective_connection_fails_before_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = execute_output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "orders_output"
        ));
        Ok(())
    }

    #[test]
    fn plan_mssql_output_rejects_replace_before_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(source, "orders_output", "orders_sink", LoadMode::Replace)?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlLifecyclePlanning { output_name, message })
                if output_name == "orders_output" && message.contains("replace load mode")
        ));
        Ok(())
    }

    #[test]
    fn plan_mssql_output_rejects_unknown_lazy_table_before_target_planning()
    -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let request = output_request(
            LazyTable::placeholder(42, LazyTableKind::DeltaSource),
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("not registered in this session")
        ));
        Ok(())
    }

    #[test]
    fn plan_mssql_output_rejects_invalid_output_name() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(source, "  ", "orders_sink", LoadMode::AppendExisting)?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::InvalidMssqlOutputIdentity { output_name, .. })
                if output_name == "  "
        ));
        Ok(())
    }

    #[test]
    fn plan_mssql_output_rejects_invalid_target_identifier_before_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source,
            "orders_output",
            "orders\narchive",
            LoadMode::CreateAndLoad,
        )?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            &error,
            Err(DeltaFunnelError::MssqlDdlTargetIdentifier { output_name, .. })
                if output_name == "orders_output"
        ));
        let display = error.err().ok_or("expected error")?.to_string();
        assert!(!display.contains('\n'));
        assert!(display.contains("control characters"));
        Ok(())
    }

    #[test]
    fn plan_mssql_output_reports_unsupported_source_schema()
    -> Result<(), Box<dyn std::error::Error>> {
        let table =
            DeltaLogTable::new_with_schema("unsupported-schema", UNSUPPORTED_SCHEMA_FIELDS_JSON)?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source =
            session.delta_lake(DeltaSourceConfig::new("unsupported_schema", table.uri()))?;
        let request = output_request(
            source,
            "unsupported_output",
            "unsupported_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlSchemaPlanning {
                output_name,
                diagnostics,
            }) if output_name == "unsupported_output" && !diagnostics.is_empty()
        ));
        Ok(())
    }

    #[test]
    fn planned_mssql_output_debug_redacts_connection_material()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders_sink")?)
            .with_connection(override_connection()?);
        let request = OutputWritePlan::new(
            source,
            MssqlOutputTarget::new("orders_output", target_config, RunMode::DryRun),
        );

        let planned = session.plan_mssql_output(&request)?;
        let debug = format!("{planned:?}");

        assert!(debug.contains("orders_output"));
        assert!(debug.contains("warehouse-override"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("override-secret"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        Ok(())
    }

    #[tokio::test]
    async fn batch_stream_for_lazy_table_reads_registered_delta_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;

        let stream = session.batch_stream_for_lazy_table(&source).await?;
        let rows = collect_stream_row_count(stream).await?;

        assert_eq!(rows, table.rows());
        Ok(())
    }

    #[tokio::test]
    async fn batch_stream_for_lazy_table_reads_pending_derived_provider()
    -> Result<(), Box<dyn std::error::Error>> {
        let session_options = SessionOptions::new().with_query_options(QueryOptions {
            target_partitions: None,
            output_batch_size: Some(1),
        });
        let mut session = DeltaFunnelSession::new(session_options)?;
        let derived = session
            .table_from_sql("select 1 as id union all select 2 as id")
            .await?;

        let stream = session.batch_stream_for_lazy_table(&derived).await?;
        let rows = collect_stream_row_count(stream).await?;

        assert_eq!(rows, 2);
        Ok(())
    }

    #[tokio::test]
    async fn batch_stream_for_lazy_table_reads_registered_derived_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let derived = session
            .table_from_sql("select 'alice' as customer_name")
            .await?;
        let alias = session.register_alias("customer_names", &derived)?;

        let stream = session.batch_stream_for_lazy_table(&alias).await?;
        let rows = collect_stream_row_count(stream).await?;

        assert_eq!(rows, 1);
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_guard_accepts_registered_derived_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let derived = session.table_from_sql("select 1 as id").await?;
        let alias = session.register_alias("cached_candidate", &derived)?;

        let registered = session.registered_derived_for_scoped_cache_alias(&alias)?;

        assert_eq!(registered.table(), &alias);
        assert_eq!(registered.name(), "cached_candidate");
        Ok(())
    }

    #[test]
    fn scoped_cache_alias_guard_rejects_raw_source_before_catalog_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let error = session.registered_derived_for_scoped_cache_alias(&source);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("not registered in this session")
        ));
        assert!(session.registered_source("orders").is_some());
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_guard_rejects_pending_derived_before_catalog_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let pending = session.table_from_sql("select 1 as id").await?;

        let error = session.registered_derived_for_scoped_cache_alias(&pending);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("not registered in this session")
        ));
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[test]
    fn scoped_cache_alias_guard_rejects_unknown_derived_handle() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let unknown = LazyTable::placeholder(252, LazyTableKind::DerivedSql);

        let error = session.registered_derived_for_scoped_cache_alias(&unknown);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("not registered in this session")
        ));
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
    async fn planned_downstream_sql_expands_registered_derived_alias_reference()
    -> Result<(), Box<dyn std::error::Error>> {
        const MARKER_REGION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"marker\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]"#;

        let table = DeltaLogTable::new_with_schema("orders", MARKER_REGION_SCHEMA_FIELDS_JSON)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let source_dataframe = session
            .plan_read_only_sql("select marker from orders")
            .await?;
        let source_references = table_scan_proof_references(source_dataframe.logical_plan())?;
        assert_eq!(
            source_references,
            vec![TableScanProofReference {
                table_name: "orders".to_owned(),
                nested_table_names: Vec::new(),
            }]
        );
        assert!(session.registered_source("orders").is_some());
        assert!(session.registered_derived_table("orders").is_none());

        let pending_big = session
            .table_from_sql("select marker, region from orders")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;
        let west_dataframe = session
            .plan_read_only_sql("select marker from BIG where region = 'west'")
            .await?;
        let east_dataframe = session
            .plan_read_only_sql("select marker from big where region = 'east'")
            .await?;
        let west_references = table_scan_proof_references(west_dataframe.logical_plan())?;
        let east_references = table_scan_proof_references(east_dataframe.logical_plan())?;

        for references in [&west_references, &east_references] {
            assert_eq!(
                references,
                &vec![TableScanProofReference {
                    table_name: "orders".to_owned(),
                    nested_table_names: Vec::new(),
                }]
            );
            assert!(
                session
                    .registered_source(&references[0].table_name)
                    .is_some()
            );
            assert!(
                session
                    .registered_derived_table(&references[0].table_name)
                    .is_none()
            );
        }

        // Conclusion for #257: DataFusion expands the registered derived
        // alias during SQL planning, so planned LogicalPlan table scans do not
        // preserve a structured west/east -> big dependency for #250.
        assert!(
            !west_references
                .iter()
                .any(|reference| reference.table_name.eq_ignore_ascii_case("big"))
        );
        assert!(
            !east_references
                .iter()
                .any(|reference| reference.table_name.eq_ignore_ascii_case("big"))
        );
        assert!(session.registered_derived_table("big").is_some());
        assert!(session.registered_source("big").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn datafusion_sql_ast_captures_session_alias_dependencies_before_planning()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;

        assert_eq!(
            ast_reference_proof("select * from big where customer_name = 'alice'")?,
            AstReferenceProof {
                relations: vec!["big".to_owned()],
                ctes: Vec::new(),
            }
        );
        assert_eq!(
            ast_reference_proof("select * from BIG where customer_name = 'alice'")?,
            AstReferenceProof {
                relations: vec!["big".to_owned()],
                ctes: Vec::new(),
            }
        );
        assert_eq!(
            ast_reference_proof("select * from big b")?,
            AstReferenceProof {
                relations: vec!["big".to_owned()],
                ctes: Vec::new(),
            }
        );
        assert_eq!(
            ast_reference_proof("select * from big join other_alias on big.id = other_alias.id")?,
            AstReferenceProof {
                relations: vec!["big".to_owned(), "other_alias".to_owned()],
                ctes: Vec::new(),
            }
        );
        assert_eq!(
            ast_reference_proof("select * from (select * from big) b")?,
            AstReferenceProof {
                relations: vec!["big".to_owned()],
                ctes: Vec::new(),
            }
        );

        let shadowed = ast_reference_proof("with big as (select * from orders) select * from big")?;
        assert_eq!(
            shadowed,
            AstReferenceProof {
                relations: vec!["orders".to_owned()],
                ctes: vec!["big".to_owned()],
            }
        );

        assert!(session.registered_derived_table("big").is_some());
        assert!(session.registered_source("big").is_none());
        assert!(session.registered_source("orders").is_some());
        assert!(session.registered_derived_table("orders").is_none());

        // Conclusion for #259: DataFusion's DFParser plus
        // resolve_table_references provides a structured pre-planning AST path
        // that captures session alias dependencies and CTE shadowing for #250.
        let derived_dependency = ast_reference_proof("select * from big")?
            .relations
            .into_iter()
            .any(|name| session.registered_derived_table(&name).is_some());
        let shadowed_derived_dependency = shadowed
            .relations
            .iter()
            .any(|name| session.registered_derived_table(name).is_some());
        assert!(derived_dependency);
        assert!(!shadowed_derived_dependency);
        Ok(())
    }

    #[tokio::test]
    async fn batch_stream_for_lazy_table_rejects_unknown_table_before_execution()
    -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session
            .batch_stream_for_lazy_table(&LazyTable::placeholder(42, LazyTableKind::DeltaSource))
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("not registered in this session")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_to_mssql_plans_output_without_row_or_writer_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let output = session
            .table_from_sql("select marker from orders_source where region = 'west'")
            .await?;
        let request = output_request(
            output,
            "west_output",
            "west_orders",
            LoadMode::CreateAndLoad,
        )?;

        let report = session.dry_run_to_mssql(&request)?;

        assert_eq!(report.output_name(), "west_output");
        assert_eq!(report.run_mode(), RunMode::DryRun);
        assert_eq!(report.status(), OutputStatus::dry_run_planned());
        assert_eq!(
            report.validation_status(),
            ValidationStatus::skipped(ReportReasonCode::DryRun)
        );
        assert_eq!(report.target_table().schema(), Some("dbo"));
        assert_eq!(report.target_table().table(), "west_orders");
        assert_eq!(report.load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(report.target_schema_plan().mappings().len(), 1);
        assert!(report.target_ddl_plan().create_table_sql().is_some());
        assert!(report.target_lifecycle_plan().create_table_sql_required());
        assert_eq!(
            report.target_lifecycle_plan().expected_target_state(),
            crate::MssqlTargetTableState::Absent
        );
        assert_eq!(
            report.planned_output().output_plan().target_table().table(),
            "west_orders"
        );
        assert_eq!(
            report
                .planned_output()
                .output_plan()
                .schema_mappings()
                .len(),
            1
        );
        assert_eq!(report.output_schema().len(), 1);
        assert_eq!(report.output_schema()[0].index(), 0);
        assert_eq!(report.output_schema()[0].name(), "marker");
        assert_eq!(report.output_schema()[0].arrow_type(), "Utf8");
        assert!(!report.output_schema()[0].nullable());
        assert_eq!(report.source_usage_status(), SourceUsageStatus::NotUsed);
        assert!(report.used_source_names().is_empty());
        assert_eq!(report.output_row_count(), crate::RowCount::unavailable());
        assert_eq!(
            report.output_row_count_reason(),
            Some(ReportReasonCode::NotExecuted)
        );
        assert_eq!(
            report.sql_identity().state(),
            MssqlDryRunSqlIdentityState::Present
        );
        assert_eq!(report.sql_identity().hash(), Some("a65390dacb7eb6f1"));
        assert_eq!(report.sql_identity().reason(), None);
        let debug = format!("{report:?}");
        assert!(debug.contains("a65390dacb7eb6f1"));
        assert!(!debug.contains("select marker"));
        assert!(!debug.contains("region = 'west'"));
        assert!(!report.sql_server_contacted());
        assert!(!report.row_production_started());
        assert!(!report.table_lifecycle_started());
        assert!(!report.bulk_writer_started());
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_to_mssql_rejects_execute_request_before_planning()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let output = session.table_from_sql("select 1 as id").await?;
        let request = execute_output_request(
            output,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.dry_run_to_mssql(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("dry_run_to_mssql requires RunMode::DryRun")
        ));
        Ok(())
    }

    #[test]
    fn dry_run_to_mssql_rejects_missing_connection_before_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.dry_run_to_mssql(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "orders_output"
        ));
        Ok(())
    }

    #[test]
    fn dry_run_to_mssql_rejects_replace_before_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(source, "orders_output", "orders_sink", LoadMode::Replace)?;

        let error = session.dry_run_to_mssql(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlLifecyclePlanning { output_name, message })
                if output_name == "orders_output" && message.contains("replace load mode")
        ));
        Ok(())
    }

    #[test]
    fn dry_run_to_mssql_report_debug_redacts_connection_material()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders_sink")?)
            .with_connection(override_connection()?);
        let request = OutputWritePlan::new(
            source,
            MssqlOutputTarget::new("orders_output", target_config, RunMode::DryRun),
        );

        let report = session.dry_run_to_mssql(&request)?;
        let debug = format!("{report:?}");

        assert!(debug.contains("orders_output"));
        assert!(debug.contains("warehouse-override"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("override-secret"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_reports_all_outputs_without_row_side_effects()
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
        let west_table_id = west.id();
        let west_table_name = west.name().to_owned();
        let west = output_request(west, "west_output", "west_orders", LoadMode::CreateAndLoad)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let report = session.dry_run_all_to_mssql(&[west, east])?;

        assert_eq!(report.run_mode(), RunMode::DryRun);
        assert_eq!(report.status(), WorkflowStatus::success());
        assert_eq!(report.len(), 2);
        assert!(!report.is_empty());
        assert_eq!(report.outputs()[0].output_name(), "west_output");
        assert_eq!(report.outputs()[1].output_name(), "east_output");
        assert_eq!(report.outputs()[0].table_id(), west_table_id);
        assert_eq!(report.outputs()[0].table_kind(), LazyTableKind::DerivedSql);
        assert_eq!(report.outputs()[0].table_name(), west_table_name);
        assert_eq!(
            report.outputs()[0].status(),
            OutputStatus::dry_run_planned()
        );
        assert_eq!(
            report.outputs()[0].validation_status(),
            ValidationStatus::skipped(ReportReasonCode::DryRun)
        );
        assert_eq!(
            report.outputs()[0].output_row_count(),
            crate::RowCount::unavailable()
        );
        assert_eq!(
            report.outputs()[0].output_row_count_reason(),
            Some(ReportReasonCode::NotExecuted)
        );
        assert!(report.sources().is_empty());
        assert_eq!(report.outputs()[0].target_table().table(), "west_orders");
        assert_eq!(report.outputs()[0].load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(
            report.outputs()[0]
                .target_lifecycle_plan()
                .expected_target_state(),
            crate::MssqlTargetTableState::Absent
        );
        assert_eq!(report.outputs()[1].target_table().table(), "east_orders");
        assert_eq!(report.outputs()[1].load_mode(), LoadMode::AppendExisting);
        assert_eq!(
            report.outputs()[1]
                .target_lifecycle_plan()
                .expected_target_state(),
            crate::MssqlTargetTableState::Exists
        );
        assert!(!report.sql_server_contacted());
        assert!(!report.row_production_started());
        assert!(!report.table_lifecycle_started());
        assert!(!report.bulk_writer_started());
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_includes_registered_delta_source_reports()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::CreateAndLoad,
        )?;

        let report = session.dry_run_all_to_mssql(&[request])?;

        assert_eq!(report.outputs().len(), 1);
        assert!(!report.query_used_source_scan_metadata_exhausted());
        assert_eq!(
            report.outputs()[0].sql_identity().state(),
            MssqlDryRunSqlIdentityState::Absent
        );
        assert_eq!(report.outputs()[0].sql_identity().hash(), None);
        assert_eq!(report.outputs()[0].sql_identity().reason(), None);
        assert_eq!(
            report.outputs()[0].source_usage_status(),
            SourceUsageStatus::Used
        );
        assert_eq!(
            report.outputs()[0].used_source_names(),
            &["orders".to_owned()]
        );
        assert_eq!(report.sources().len(), 1);
        let source = &report.sources()[0];
        assert_eq!(source.source_name(), "orders");
        assert_eq!(source.snapshot_version(), 1);
        assert_eq!(source.protocol().source_name, "orders");
        assert_eq!(source.file_count(), crate::FileCount::unavailable());
        assert_eq!(
            source.file_count_reason(),
            Some(crate::ReportReasonCode::CostAvoidance)
        );
        assert!(!source.scan_metadata_exhausted());
        assert_eq!(source.usage_status(), SourceUsageStatus::Used);
        assert_eq!(source.used_by_output_names(), &["orders_output".to_owned()]);
        assert!(!report.row_production_started());
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_scan_summary_exhausts_provider_metadata_without_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new()
                .with_default_mssql_connection(secret_connection()?)
                .with_validation_options(ValidationOptions::new().with_dry_run_scan_summary_mode(
                    crate::DryRunScanSummaryMode::ExhaustScanMetadata,
                )),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::CreateAndLoad,
        )?;

        let report = session
            .dry_run_all_to_mssql_with_scan_summary(&[request])
            .await?;

        assert_eq!(report.outputs().len(), 1);
        assert!(!report.outputs()[0].row_production_started());
        assert_eq!(report.sources().len(), 1);
        let source = &report.sources()[0];
        assert_eq!(source.source_name(), "orders");
        assert_eq!(source.usage_status(), SourceUsageStatus::Used);
        assert_eq!(source.used_by_output_names(), &["orders_output".to_owned()]);
        assert_eq!(source.provider_stats_reason(), None);
        let stats = source
            .provider_read_stats()
            .ok_or("expected provider stats from dry-run scan summary")?;
        assert_eq!(stats.source_name, "orders");
        assert_eq!(stats.files_started, 0);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.batches_produced, 0);
        assert_eq!(stats.rows_produced, 0);
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
        assert_eq!(
            report.query_used_source_scan_metadata_exhausted(),
            source.scan_metadata_exhausted()
        );
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_reports_multi_source_usage_when_lineage_is_known()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders_table = DeltaLogTable::new("orders")?;
        let customers_table = DeltaLogTable::new("customers")?;
        let inventory_table = DeltaLogTable::new("inventory")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new("orders", orders_table.uri()))?;
        session.delta_lake(DeltaSourceConfig::new("customers", customers_table.uri()))?;
        session.delta_lake(DeltaSourceConfig::new("inventory", inventory_table.uri()))?;
        let joined = session
            .table_from_sql(
                "select orders.id from orders inner join customers on orders.id = customers.id",
            )
            .await?;
        let request = output_request(
            joined,
            "joined_output",
            "joined_sink",
            LoadMode::CreateAndLoad,
        )?;

        let report = session.dry_run_all_to_mssql(&[request])?;

        assert!(!report.query_used_source_scan_metadata_exhausted());
        assert_eq!(
            report.outputs()[0].source_usage_status(),
            SourceUsageStatus::Used
        );
        assert_eq!(
            report.outputs()[0].used_source_names(),
            &["orders".to_owned(), "customers".to_owned()]
        );
        assert_eq!(report.sources().len(), 3);
        for source in report.sources() {
            match source.source_name() {
                "orders" | "customers" => {
                    assert_eq!(source.usage_status(), SourceUsageStatus::Used);
                    assert_eq!(source.used_by_output_names(), &["joined_output".to_owned()]);
                }
                "inventory" => {
                    assert_eq!(source.usage_status(), SourceUsageStatus::NotUsed);
                    assert!(source.used_by_output_names().is_empty());
                }
                name => return Err(format!("unexpected source report: {name}").into()),
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_rejects_execute_request_before_row_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let output = session
            .table_from_sql("select marker from orders_source")
            .await?;
        let request = execute_output_request(
            output,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.dry_run_all_to_mssql(&[request]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("dry_run_all_to_mssql requires RunMode::DryRun")
        ));
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_rejects_missing_connection_before_row_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let output = session
            .table_from_sql("select marker from orders_source")
            .await?;
        let request = output_request(
            output,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.dry_run_all_to_mssql(&[request]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "orders_output"
        ));
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_rejects_duplicate_output_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west = output_request(
            west,
            "orders_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = output_request(
            east,
            "orders_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let error = session.dry_run_all_to_mssql(&[west, east]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("write_all output names must be unique")
                    && message.contains("orders_output")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_requires_effective_connection_before_stream_setup()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = execute_output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.write_to_mssql(&request).await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "orders_output"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_with_writer_hands_query_stream_to_one_output_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let derived = session
            .table_from_sql("select 1 as id union all select 2 as id")
            .await?;
        let request = execute_output_request(
            derived,
            "orders_output",
            "orders_sink",
            LoadMode::CreateAndLoad,
        )?;
        let mut writer = FakeOrchestratorWriter::default();

        let report = session
            .write_to_mssql_with_writer(&request, &mut writer)
            .await?;

        assert_eq!(writer.calls.len(), 1);
        let call = writer.calls.first().ok_or("expected fake writer call")?;
        assert_eq!(call.output_name, "orders_output");
        assert_eq!(call.target_table.schema(), Some("dbo"));
        assert_eq!(call.target_table.table(), "orders_sink");
        assert_eq!(
            call.connection_source,
            MssqlConnectionSource::ContextDefault
        );
        assert_eq!(call.rows, 2);
        assert!(call.batches >= 1);
        assert_eq!(call.schema_fields, 1);
        assert_eq!(report.output_name(), "orders_output");
        assert_eq!(report.stats().rows_written(), 2);
        assert_eq!(report.stats().batches_written(), call.batches);
        assert_eq!(report.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_with_writer_executes_real_delta_source_fixture()
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
        let mut writer = FakeOrchestratorWriter::default();

        let report = session
            .write_to_mssql_with_writer(&request, &mut writer)
            .await?;

        assert_eq!(writer.calls.len(), 1);
        let call = writer.calls.first().ok_or("expected fake writer call")?;
        assert_eq!(call.output_name, "orders_output");
        assert_eq!(call.rows, u64::try_from(table.rows())?);
        assert!(call.batches >= 1);
        assert_eq!(call.schema_fields, 2);
        assert_eq!(report.stats().rows_written(), u64::try_from(table.rows())?);
        assert_eq!(report.stats().batches_written(), call.batches);
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_with_writer_executes_multi_source_delta_join_fixture()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = RealParquetDeltaTable::new_default("orders")?;
        let customers = RealParquetDeltaTable::new_default("customers")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new(
            "orders",
            orders.path().to_string_lossy().to_string(),
        ))?;
        session.delta_lake(DeltaSourceConfig::new(
            "customers",
            customers.path().to_string_lossy().to_string(),
        ))?;
        let joined = session
            .table_from_sql(
                "select o.id, c.customer_name \
                 from orders o \
                 join customers c on o.id = c.id",
            )
            .await?;
        let request = execute_output_request(
            joined,
            "joined_output",
            "joined_sink",
            LoadMode::AppendExisting,
        )?;
        let mut writer = FakeOrchestratorWriter::default();

        let report = session
            .write_to_mssql_with_writer(&request, &mut writer)
            .await?;

        assert_eq!(writer.calls.len(), 1);
        let call = writer.calls.first().ok_or("expected fake writer call")?;
        assert_eq!(call.output_name, "joined_output");
        assert_eq!(call.rows, 3);
        assert!(call.batches >= 1);
        assert_eq!(call.schema_fields, 2);
        assert_eq!(report.stats().rows_written(), 3);
        assert_eq!(report.stats().batches_written(), call.batches);
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_rejects_dry_run_before_planning_or_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let derived = session.table_from_sql("select 1 as id").await?;
        let request = output_request(
            derived,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let mut writer = FakeOrchestratorWriter::default();

        let error = session
            .write_to_mssql_with_writer(&request, &mut writer)
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("RunMode::Execute")
                    && message.contains("dry_run_to_mssql")
        ));
        assert!(writer.calls.is_empty());
        Ok(())
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
    fn delta_lake_registers_source_and_returns_lazy_table() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let lazy = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        assert_eq!(lazy.id(), 0);
        assert_eq!(lazy.kind(), LazyTableKind::DeltaSource);
        assert_eq!(lazy.name(), "orders");
        assert_eq!(session.next_table_id(), 1);
        assert_eq!(session.sources().len(), 1);
        let registered = session
            .registered_source("ORDERS")
            .ok_or("expected registered source")?;
        assert_eq!(registered.table(), &lazy);
        assert_eq!(registered.name(), "orders");
        assert!(registered.source_uri().starts_with("file://"));
        assert_eq!(registered.snapshot_version(), 1);
        assert_eq!(registered.protocol().source_name, "orders");
        assert_eq!(registered.schema().fields().len(), 2);
        let source_reports = session.source_reports();
        assert_eq!(source_reports.len(), 1);
        let report = &source_reports[0];
        assert_eq!(report.source_name(), "orders");
        assert_eq!(report.source_uri(), registered.source_uri());
        assert_eq!(report.snapshot_version(), 1);
        assert_eq!(report.protocol().source_name, "orders");
        assert_eq!(report.scheduling().query_target_partitions(), None);
        assert_eq!(
            report.scheduling().reader_backend(),
            DeltaProviderReaderBackend::NativeAsync
        );
        assert_eq!(report.file_count(), crate::FileCount::unavailable());
        assert_eq!(
            report.file_count_reason(),
            Some(crate::ReportReasonCode::CostAvoidance)
        );
        assert!(!report.scan_metadata_exhausted());
        assert_eq!(report.usage_status(), SourceUsageStatus::Unknown);
        assert!(report.used_by_output_names().is_empty());
        assert!(report.provider_read_stats().is_none());
        assert_eq!(
            report.provider_stats_reason(),
            Some(crate::ReportReasonCode::NotExecuted)
        );

        Ok(())
    }

    #[tokio::test]
    async fn source_reports_for_lazy_table_plan_include_provider_stats_without_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let reports = session.source_reports_for_lazy_table_plan(&source).await?;

        assert_eq!(reports.len(), 1);
        let report = &reports[0];
        assert_eq!(report.source_name(), "orders");
        assert_eq!(report.provider_stats_reason(), None);
        let stats = report
            .provider_read_stats()
            .ok_or("expected provider read stats")?;
        assert_eq!(stats.source_name, "orders");
        assert_eq!(stats.snapshot_version, report.snapshot_version());
        assert_eq!(stats.files_started, 0);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.batches_produced, 0);
        assert_eq!(stats.rows_produced, 0);
        match stats.scan_metadata_exhausted {
            Some(true) => {
                assert_eq!(
                    report.file_count(),
                    crate::FileCount::exact(stats.files_planned)
                );
                assert_eq!(report.file_count_reason(), None);
                assert!(report.scan_metadata_exhausted());
            }
            Some(false) => {
                assert_eq!(
                    report.file_count(),
                    crate::FileCount::estimated(stats.files_planned)
                );
                assert_eq!(report.file_count_reason(), None);
                assert!(!report.scan_metadata_exhausted());
            }
            None => {
                assert_eq!(report.file_count(), crate::FileCount::unavailable());
                assert_eq!(
                    report.file_count_reason(),
                    Some(crate::ReportReasonCode::CapabilityUnavailable)
                );
                assert!(!report.scan_metadata_exhausted());
            }
        }
        Ok(())
    }

    #[test]
    fn delta_lake_registers_multiple_distinct_sources() -> Result<(), Box<dyn std::error::Error>> {
        let orders = DeltaLogTable::new("orders")?;
        let customers = DeltaLogTable::new("customers")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let orders = session.delta_lake(DeltaSourceConfig::new("orders", orders.uri()))?;
        let customers = session.delta_lake(DeltaSourceConfig::new("customers", customers.uri()))?;

        assert_eq!(orders.id(), 0);
        assert_eq!(customers.id(), 1);
        assert_eq!(session.sources().len(), 2);
        assert!(session.registered_source("orders").is_some());
        assert!(session.registered_source("customers").is_some());
        Ok(())
    }

    #[test]
    fn duplicate_source_alias_fails_before_loading_second_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let error = session.delta_lake(DeltaSourceConfig::new("ORDERS", ""));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "ORDERS"
        ));
        assert_eq!(session.sources().len(), 1);
        assert_eq!(session.next_table_id(), 1);
        Ok(())
    }

    #[test]
    fn invalid_source_alias_fails_before_registration() -> Result<(), DeltaFunnelError> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session.delta_lake(DeltaSourceConfig::new("select", ""));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::InvalidSourceName { name, .. }) if name == "select"
        ));
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[test]
    fn protocol_preflight_failure_does_not_register_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol("unsupported", UNSUPPORTED_PROTOCOL_JSON)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session.delta_lake(DeltaSourceConfig::new("unsupported", table.uri()));

        let display = format!("{}", error.as_ref().err().ok_or("expected error")?);
        assert!(display.contains("unsupported"));
        assert!(display.contains("unsupported Delta minReaderVersion"));
        assert!(matches!(
            error,
            Err(DeltaFunnelError::DeltaProtocolCompatibility { .. })
        ));
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[test]
    fn protocol_preflight_failure_redacts_secret_uri_parts()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol("unsupported", UNSUPPORTED_PROTOCOL_JSON)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session
            .delta_lake(DeltaSourceConfig::new(
                "unsupported",
                table.file_uri_with_secret_parts()?,
            ))
            .map(|_| ())
            .map_err(|error| error.to_string());

        assert!(
            matches!(error, Err(display) if display.contains("unsupported")
            && display.contains("unsupported Delta minReaderVersion")
            && !display.contains("super-secret")
            && !display.contains("debug-secret")
            && !display.contains("token"))
        );
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn protocol_preflight_failure_does_not_leak_datafusion_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol("unsupported", UNSUPPORTED_PROTOCOL_JSON)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session.delta_lake(DeltaSourceConfig::new("unsupported", table.uri()));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::DeltaProtocolCompatibility { .. })
        ));
        assert!(session.context().table("unsupported").await.is_err());
        assert!(session.sources().is_empty());
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn registered_source_sql_analysis_does_not_read_data_files_for_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let dataframe = session
            .context()
            .sql("select id, customer_name from orders")
            .await?;
        let schema = dataframe.schema();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "customer_name");
        assert_eq!(session.sources().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn table_from_sql_builds_lazy_derived_table_without_row_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let derived = session
            .table_from_sql("select id, customer_name from orders")
            .await?;

        assert_eq!(derived.id(), 1);
        assert_eq!(derived.kind(), LazyTableKind::DerivedSql);
        assert_eq!(derived.name(), "table_1");
        assert_eq!(session.next_table_id(), 2);
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn table_from_sql_retains_trimmed_pending_sql_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let derived = session
            .table_from_sql(" \n\t select id from orders \t ")
            .await?;

        assert_eq!(
            session.sql_text_for_derived_table(&derived)?,
            "select id from orders"
        );
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn registered_derived_alias_can_be_referenced_by_later_sql()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session
            .table_from_sql("select id, customer_name from orders")
            .await?;

        let alias = session.register_alias("recent_orders", &derived)?;
        let second = session
            .table_from_sql("select id from recent_orders")
            .await?;

        assert_eq!(alias.id(), derived.id());
        assert_eq!(alias.name(), "recent_orders");
        assert_eq!(second.id(), 2);
        assert_eq!(second.kind(), LazyTableKind::DerivedSql);
        assert_eq!(session.derived_tables().len(), 1);
        let registered = session
            .registered_derived_table("RECENT_ORDERS")
            .ok_or("registered derived alias missing")?;
        assert_eq!(registered.table(), &alias);
        assert_eq!(registered.schema().fields().len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn registered_derived_alias_retains_sql_text() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session
            .table_from_sql("select id, customer_name from orders")
            .await?;

        let alias = session.register_alias("recent_orders", &derived)?;
        let registered = session
            .registered_derived_table("RECENT_ORDERS")
            .ok_or("registered derived alias missing")?;

        assert_eq!(
            session.sql_text_for_derived_table(&alias)?,
            "select id, customer_name from orders"
        );
        assert_eq!(
            registered.sql_text(),
            "select id, customer_name from orders"
        );
        assert_eq!(
            session.sql_text_for_derived_table(&derived)?,
            "select id, customer_name from orders"
        );
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_records_raw_source_dependency()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let orders = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let lineage = session.lineage_for_derived_table(&big)?;

        assert!(lineage.is_complete());
        assert_eq!(
            lineage.direct_dependencies(),
            &[DerivedTableDependency::RegisteredSource {
                table_id: orders.id(),
                name: "orders".to_owned(),
            }]
        );
        assert!(lineage.unknown_references().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_records_registered_derived_dependency()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;

        let west = session
            .table_from_sql("select * from BIG b where customer_name = 'alice'")
            .await?;
        let lineage = session.lineage_for_derived_table(&west)?;

        assert!(lineage.is_complete());
        assert_eq!(
            lineage.direct_dependencies(),
            &[DerivedTableDependency::RegisteredDerived {
                table_id: big.id(),
                name: "big".to_owned(),
            }]
        );
        assert!(lineage.unknown_references().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_deduplicates_repeated_dependencies()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;

        let repeated = session
            .table_from_sql("select * from big where id in (select id from big)")
            .await?;
        let lineage = session.lineage_for_derived_table(&repeated)?;

        assert_eq!(
            lineage.direct_dependencies(),
            &[DerivedTableDependency::RegisteredDerived {
                table_id: big.id(),
                name: "big".to_owned(),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_records_dependency_inside_from_subquery()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;

        let derived = session
            .table_from_sql("select id from (select id from big) nested")
            .await?;
        let lineage = session.lineage_for_derived_table(&derived)?;

        assert_eq!(
            lineage.direct_dependencies(),
            &[DerivedTableDependency::RegisteredDerived {
                table_id: big.id(),
                name: "big".to_owned(),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_finds_transitive_registered_derived_dependencies()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_regional = session
            .table_from_sql("select id, customer_name from big")
            .await?;
        let regional = session.register_alias("regional", &pending_regional)?;

        let west = session
            .table_from_sql("select id from regional where customer_name = 'alice'")
            .await?;
        let dependencies = session.transitive_registered_derived_dependencies(&west)?;

        assert_eq!(
            dependencies,
            vec![
                DerivedTableDependency::RegisteredDerived {
                    table_id: big.id(),
                    name: "big".to_owned(),
                },
                DerivedTableDependency::RegisteredDerived {
                    table_id: regional.id(),
                    name: "regional".to_owned(),
                },
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_matches_shared_transitive_dependency_for_multiple_outputs()
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
        let expected = vec![DerivedTableDependency::RegisteredDerived {
            table_id: big.id(),
            name: "big".to_owned(),
        }];

        assert_eq!(
            session.transitive_registered_derived_dependencies(&west)?,
            expected
        );
        assert_eq!(
            session.transitive_registered_derived_dependencies(&east)?,
            expected
        );
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_checks_registered_derived_candidate_dependency()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let orders = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_regional = session
            .table_from_sql("select id, customer_name from big")
            .await?;
        let regional = session.register_alias("regional", &pending_regional)?;
        let west = session
            .table_from_sql("select id from regional where customer_name = 'alice'")
            .await?;
        let unrelated = session
            .table_from_sql("select customer_name from orders")
            .await?;

        assert!(session.lazy_table_depends_on_registered_derived(&west, &big)?);
        assert!(session.lazy_table_depends_on_registered_derived(&west, &regional)?);
        assert!(session.lazy_table_depends_on_registered_derived(&big, &big)?);
        assert!(!session.lazy_table_depends_on_registered_derived(&unrelated, &big)?);
        assert!(!session.lazy_table_depends_on_registered_derived(&orders, &big)?);
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_treats_cte_shadowing_as_local_reference()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let orders = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let pending_big = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;

        let shadowed = session
            .table_from_sql("with big as (select id from orders) select id from big")
            .await?;
        let lineage = session.lineage_for_derived_table(&shadowed)?;

        assert_eq!(lineage.local_references(), &["big".to_owned()]);
        assert_eq!(
            lineage.direct_dependencies(),
            &[DerivedTableDependency::RegisteredSource {
                table_id: orders.id(),
                name: "orders".to_owned(),
            }]
        );
        assert!(
            !lineage
                .direct_dependencies()
                .iter()
                .any(|dependency| matches!(
                    dependency,
                    DerivedTableDependency::RegisteredDerived { name, .. } if name == "big"
                ))
        );
        Ok(())
    }

    #[tokio::test]
    async fn derived_lineage_records_unknown_external_references()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session
            .context()
            .register_table("external_orders", marker_region_provider("external")?)?;

        let derived = session
            .table_from_sql("select marker from external_orders")
            .await?;
        let lineage = session.lineage_for_derived_table(&derived)?;

        assert!(lineage.is_complete());
        assert!(lineage.direct_dependencies().is_empty());
        assert_eq!(
            lineage.unknown_references(),
            &["external_orders".to_owned()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn empty_sql_fails_before_lazy_table_allocation() -> Result<(), DeltaFunnelError> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session.table_from_sql(" \n\t ").await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::ValidateSql,
                message,
            }) if message.contains("must not be empty")
        ));
        assert_eq!(session.next_table_id(), 0);
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn multiple_sql_statements_fail_before_lazy_table_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let error = session
            .table_from_sql("select id from orders; select id from orders")
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::ValidateSql,
                ..
            })
        ));
        assert_eq!(session.next_table_id(), 1);
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn ddl_sql_fails_before_alias_registration() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let error = session
            .table_from_sql("create table created_orders as select id from orders")
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::ValidateSql,
                ..
            })
        ));
        assert_eq!(session.next_table_id(), 1);
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn dml_sql_fails_before_alias_registration() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let error = session
            .table_from_sql("insert into orders select id, customer_name from orders")
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::ValidateSql,
                ..
            })
        ));
        assert_eq!(session.next_table_id(), 1);
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn missing_table_sql_fails_with_planning_context() -> Result<(), DeltaFunnelError> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session
            .table_from_sql("select id from missing_orders")
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::PlanSql,
                message,
            }) if message.contains("missing_orders")
        ));
        assert_eq!(session.next_table_id(), 0);
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn derived_alias_duplicate_with_source_fails_before_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session.table_from_sql("select id from orders").await?;

        let error = session.register_alias("ORDERS", &derived);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "ORDERS"
        ));
        assert!(session.derived_tables().is_empty());
        assert!(session.context().table("ORDERS").await.is_ok());
        let alias = session.register_alias("recent_orders", &derived)?;
        assert_eq!(alias.name(), "recent_orders");
        assert_eq!(session.derived_tables().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_derived_alias_fails_without_consuming_pending_table()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session.table_from_sql("select id from orders").await?;

        let error = session.register_alias("select", &derived);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::InvalidSourceName { name, .. }) if name == "select"
        ));
        assert!(session.derived_tables().is_empty());
        let alias = session.register_alias("recent_orders", &derived)?;
        assert_eq!(alias.name(), "recent_orders");
        assert_eq!(session.derived_tables().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_derived_alias_preserves_pending_sql_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session.table_from_sql("select id from orders").await?;

        let error = session.register_alias("select", &derived);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::InvalidSourceName { name, .. }) if name == "select"
        ));
        assert_eq!(
            session.sql_text_for_derived_table(&derived)?,
            "select id from orders"
        );
        let alias = session.register_alias("recent_orders", &derived)?;
        assert_eq!(
            session.sql_text_for_derived_table(&alias)?,
            "select id from orders"
        );
        Ok(())
    }

    #[tokio::test]
    async fn register_alias_rejects_non_pending_table_handle()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let error = session.register_alias("recent_orders", &source);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::RegisterDerivedAlias,
                message,
            }) if message.contains("pending SQL-derived table")
        ));
        assert!(session.derived_tables().is_empty());
        assert!(session.context().table("recent_orders").await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn derived_alias_duplicate_with_derived_alias_fails_before_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let first = session.table_from_sql("select id from orders").await?;
        session.register_alias("recent_orders", &first)?;
        let second = session.table_from_sql("select id from orders").await?;

        let error = session.register_alias("RECENT_ORDERS", &second);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "RECENT_ORDERS"
        ));
        assert_eq!(session.derived_tables().len(), 1);
        assert!(session.context().table("RECENT_ORDERS").await.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn registered_derived_debug_redacts_retained_sql_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let derived = session
            .table_from_sql("select 'super-secret-literal' as marker")
            .await?;
        session.register_alias("secret_marker", &derived)?;
        let registered = session
            .registered_derived_table("secret_marker")
            .ok_or("registered derived alias missing")?;

        let debug = format!("{registered:?}");

        assert!(debug.contains("sql_text"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("super-secret-literal"));
        assert!(!debug.contains("select '"));
        Ok(())
    }

    #[tokio::test]
    async fn source_alias_duplicate_with_derived_alias_fails_before_loading_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session.table_from_sql("select id from orders").await?;
        session.register_alias("recent_orders", &derived)?;

        let error = session.delta_lake(DeltaSourceConfig::new("RECENT_ORDERS", ""));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::DuplicateSourceName { name }) if name == "RECENT_ORDERS"
        ));
        assert_eq!(session.sources().len(), 1);
        assert_eq!(session.derived_tables().len(), 1);
        Ok(())
    }

    #[test]
    fn source_debug_does_not_expose_storage_option_values() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("storage-options")?;
        let mut storage_options = DeltaStorageOptions::new();
        storage_options.insert(
            "AWS_SECRET_ACCESS_KEY".to_owned(),
            "super-secret".to_owned(),
        );
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;

        session.delta_lake(
            DeltaSourceConfig::new("orders", table.uri()).with_storage_options(storage_options),
        )?;

        let debug = format!("{session:?}");
        assert!(debug.contains("orders"));
        assert!(!debug.contains("super-secret"));
        assert!(!debug.contains("AWS_SECRET_ACCESS_KEY"));
        let report_debug = format!("{:?}", session.source_reports());
        assert!(report_debug.contains("orders"));
        assert!(!report_debug.contains("super-secret"));
        assert!(!report_debug.contains("AWS_SECRET_ACCESS_KEY"));
        Ok(())
    }

    #[test]
    fn source_registration_honors_configured_provider_options()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("configured-provider")?;
        let provider_scan_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::OfficialKernel,
            2,
            1,
        )?
        .with_output_buffer_capacity_per_partition(3)?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new()
                .with_query_options(QueryOptions {
                    target_partitions: Some(4),
                    output_batch_size: None,
                })
                .with_provider_scan_options(provider_scan_options),
        )?;

        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        assert_eq!(session.sources().len(), 1);
        assert!(session.registered_source("orders").is_some());
        let reports = session.source_reports();
        assert_eq!(reports.len(), 1);
        let scheduling = reports[0].scheduling();
        assert_eq!(scheduling.query_target_partitions(), Some(4));
        assert_eq!(
            scheduling.reader_backend(),
            DeltaProviderReaderBackend::OfficialKernel
        );
        assert_eq!(scheduling.max_concurrent_file_reads_per_scan(), 2);
        assert_eq!(scheduling.max_concurrent_file_reads_per_partition(), 1);
        assert_eq!(scheduling.output_buffer_capacity_per_partition(), 3);
        assert_eq!(
            scheduling.native_async_prefetch_file_count_per_partition(),
            0
        );
        Ok(())
    }
}
