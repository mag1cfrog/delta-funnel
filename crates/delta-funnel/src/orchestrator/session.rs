//! Rust backing model for lazy query-load orchestration.
//!
//! This module owns the high-level session and request data shapes that will
//! later back the Python `Session` and `Table` API. It intentionally does not
//! contact SQL Server, produce physical query plans, or execute rows.

use std::{collections::BTreeSet, fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::TableReference;
use datafusion::prelude::{SQLOptions, SessionContext};
use datafusion::sql::{parser::DFParser, resolve::resolve_table_references};
use datafusion::{datasource::TableProvider, prelude::DataFrame};
use futures_util::StreamExt;

use crate::{
    BatchPipelinePhase, DeltaFunnelError, DeltaProtocolReport, DeltaProviderScanExecutionOptions,
    DeltaSourceConfig, DeltaTableProviderConfig, MssqlConnectionConfig, MssqlOutputBatchStream,
    MssqlSchemaPlanOptions, MssqlTargetConfig, MssqlTargetOutputPlan, MssqlWorkflowWriteOptions,
    MssqlWriteOptions, MssqlWriteReport, QueryOptions, RegisteredDeltaSource, ResolvedMssqlTarget,
    SqlTablePhase, datafusion_query_output_stream, datafusion_session_context,
    default_mssql_write_options, load_delta_source, plan_mssql_target_for_resolved_output,
    preflight_delta_protocol, register_delta_sources_with_scan_execution_options,
    support::sanitize_text_for_display, table_formats::validate_table_source_names,
    write_output_batches_to_mssql,
};

/// Query-load action mode requested by a caller.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RunMode {
    /// Plan and execute the selected output workflow.
    #[default]
    Execute,
    /// Reuse planning paths without row production or SQL Server write effects.
    DryRun,
}

/// Validation options that can be checked before workflow side effects.
///
/// Rich row-count and target-side validation belongs to issue #10. This type
/// exists so the session API can carry validation intent without starting
/// validation I/O in the session-model slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationOptions {
    require_successful_planning: bool,
}

impl Default for ValidationOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidationOptions {
    /// Creates default local validation options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            require_successful_planning: true,
        }
    }

    /// Returns whether planning failures should be treated as terminal.
    #[must_use]
    pub const fn require_successful_planning(&self) -> bool {
        self.require_successful_planning
    }

    /// Validates local validation options before workflow side effects.
    ///
    /// # Errors
    ///
    /// Currently returns `Ok(())` for all representable values. The method is
    /// intentionally present so later validation options can be wired through
    /// the same pre-side-effect path.
    pub const fn validate(&self) -> Result<(), DeltaFunnelError> {
        let _ = self.require_successful_planning;
        Ok(())
    }
}

/// Session-wide options for lazy query-load orchestration.
#[derive(Clone)]
pub struct SessionOptions {
    query_options: QueryOptions,
    provider_scan_options: DeltaProviderScanExecutionOptions,
    mssql_schema_options: MssqlSchemaPlanOptions,
    mssql_write_options: MssqlWriteOptions,
    mssql_workflow_options: MssqlWorkflowWriteOptions,
    validation_options: ValidationOptions,
    default_mssql_connection: Option<MssqlConnectionConfig>,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            query_options: QueryOptions::default(),
            provider_scan_options: DeltaProviderScanExecutionOptions::default(),
            mssql_schema_options: MssqlSchemaPlanOptions::default(),
            mssql_write_options: default_mssql_write_options(),
            mssql_workflow_options: MssqlWorkflowWriteOptions::default(),
            validation_options: ValidationOptions::default(),
            default_mssql_connection: None,
        }
    }
}

impl SessionOptions {
    /// Creates default session options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets DataFusion query execution options.
    #[must_use]
    pub const fn with_query_options(mut self, query_options: QueryOptions) -> Self {
        self.query_options = query_options;
        self
    }

    /// Sets Delta provider scan execution options.
    #[must_use]
    pub const fn with_provider_scan_options(
        mut self,
        provider_scan_options: DeltaProviderScanExecutionOptions,
    ) -> Self {
        self.provider_scan_options = provider_scan_options;
        self
    }

    /// Sets SQL Server schema planning options.
    #[must_use]
    pub const fn with_mssql_schema_options(
        mut self,
        mssql_schema_options: MssqlSchemaPlanOptions,
    ) -> Self {
        self.mssql_schema_options = mssql_schema_options;
        self
    }

    /// Sets SQL Server write options.
    #[must_use]
    pub const fn with_mssql_write_options(
        mut self,
        mssql_write_options: MssqlWriteOptions,
    ) -> Self {
        self.mssql_write_options = mssql_write_options;
        self
    }

    /// Sets SQL Server multi-output workflow options.
    #[must_use]
    pub const fn with_mssql_workflow_options(
        mut self,
        mssql_workflow_options: MssqlWorkflowWriteOptions,
    ) -> Self {
        self.mssql_workflow_options = mssql_workflow_options;
        self
    }

    /// Sets locally checkable validation options.
    #[must_use]
    pub const fn with_validation_options(mut self, validation_options: ValidationOptions) -> Self {
        self.validation_options = validation_options;
        self
    }

    /// Sets the session-level default SQL Server connection.
    #[must_use]
    pub fn with_default_mssql_connection(
        mut self,
        default_mssql_connection: MssqlConnectionConfig,
    ) -> Self {
        self.default_mssql_connection = Some(default_mssql_connection);
        self
    }

    /// Returns DataFusion query execution options.
    #[must_use]
    pub const fn query_options(&self) -> QueryOptions {
        self.query_options
    }

    /// Returns Delta provider scan execution options.
    #[must_use]
    pub const fn provider_scan_options(&self) -> DeltaProviderScanExecutionOptions {
        self.provider_scan_options
    }

    /// Returns SQL Server schema planning options.
    #[must_use]
    pub const fn mssql_schema_options(&self) -> MssqlSchemaPlanOptions {
        self.mssql_schema_options
    }

    /// Returns SQL Server write options.
    #[must_use]
    pub const fn mssql_write_options(&self) -> MssqlWriteOptions {
        self.mssql_write_options
    }

    /// Returns SQL Server multi-output workflow options.
    #[must_use]
    pub const fn mssql_workflow_options(&self) -> MssqlWorkflowWriteOptions {
        self.mssql_workflow_options
    }

    /// Returns locally checkable validation options.
    #[must_use]
    pub const fn validation_options(&self) -> ValidationOptions {
        self.validation_options
    }

    /// Returns the optional session-level default SQL Server connection.
    #[must_use]
    pub fn default_mssql_connection(&self) -> Option<&MssqlConnectionConfig> {
        self.default_mssql_connection.as_ref()
    }

    /// Validates local options before workflow side effects.
    ///
    /// # Errors
    ///
    /// Returns the first validation error from query, provider, workflow, or
    /// local validation options.
    pub fn validate(&self) -> Result<(), DeltaFunnelError> {
        self.query_options.validate()?;
        self.provider_scan_options.validate()?;
        self.mssql_workflow_options.validate()?;
        self.validation_options.validate()?;
        Ok(())
    }
}

impl fmt::Debug for SessionOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionOptions")
            .field("query_options", &self.query_options)
            .field("provider_scan_options", &self.provider_scan_options)
            .field("mssql_schema_options", &self.mssql_schema_options)
            .field("mssql_write_options", &self.mssql_write_options)
            .field("mssql_workflow_options", &self.mssql_workflow_options)
            .field("validation_options", &self.validation_options)
            .field(
                "default_mssql_connection",
                &self
                    .default_mssql_connection
                    .as_ref()
                    .map(MssqlConnectionConfig::summary),
            )
            .finish()
    }
}

/// Lazy table identity owned by a query-load session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LazyTable {
    id: LazyTableId,
    kind: LazyTableKind,
    name: String,
}

impl LazyTable {
    /// Creates a placeholder lazy table handle for future registration slices.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn placeholder(id: u64, kind: LazyTableKind) -> Self {
        Self {
            id: LazyTableId(id),
            kind,
            name: format!("table_{id}"),
        }
    }

    fn delta_source(id: u64, name: String) -> Self {
        Self {
            id: LazyTableId(id),
            kind: LazyTableKind::DeltaSource,
            name,
        }
    }

    fn derived_sql(id: u64) -> Self {
        Self {
            id: LazyTableId(id),
            kind: LazyTableKind::DerivedSql,
            name: format!("table_{id}"),
        }
    }

    fn with_name(&self, name: String) -> Self {
        Self {
            id: self.id,
            kind: self.kind,
            name,
        }
    }

    /// Returns the stable session-local table id.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id.0
    }

    /// Returns the lazy table kind.
    #[must_use]
    pub const fn kind(&self) -> LazyTableKind {
        self.kind
    }

    /// Returns the session-owned table name for this lazy table.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LazyTableId(u64);

/// Kind of lazy table represented by a session handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LazyTableKind {
    /// Registered Delta source table.
    DeltaSource,
    /// SQL-derived table.
    DerivedSql,
}

/// MSSQL output target selected from a lazy table.
#[derive(Clone, PartialEq, Eq)]
pub struct MssqlOutputTarget {
    output_name: String,
    target: MssqlTargetConfig,
    run_mode: RunMode,
}

impl MssqlOutputTarget {
    /// Creates an MSSQL output target request.
    #[must_use]
    pub fn new(
        output_name: impl Into<String>,
        target: MssqlTargetConfig,
        run_mode: RunMode,
    ) -> Self {
        Self {
            output_name: output_name.into(),
            target,
            run_mode,
        }
    }

    /// Returns the selected output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns the SQL Server target config.
    #[must_use]
    pub const fn target(&self) -> &MssqlTargetConfig {
        &self.target
    }

    /// Returns the requested run mode.
    #[must_use]
    pub const fn run_mode(&self) -> RunMode {
        self.run_mode
    }
}

impl fmt::Debug for MssqlOutputTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlOutputTarget")
            .field("output_name", &sanitize_text_for_display(&self.output_name))
            .field("target", &self.target)
            .field("run_mode", &self.run_mode)
            .finish()
    }
}

/// Planned output write request before schema planning or execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputWritePlan {
    table: LazyTable,
    target: MssqlOutputTarget,
}

impl OutputWritePlan {
    /// Creates an output write request for a lazy table.
    #[must_use]
    pub const fn new(table: LazyTable, target: MssqlOutputTarget) -> Self {
        Self { table, target }
    }

    /// Returns the selected lazy table.
    #[must_use]
    pub const fn table(&self) -> &LazyTable {
        &self.table
    }

    /// Returns the selected MSSQL output target.
    #[must_use]
    pub const fn target(&self) -> &MssqlOutputTarget {
        &self.target
    }
}

/// Planned MSSQL output request for one selected lazy table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedMssqlOutput {
    request: OutputWritePlan,
    resolved_target: ResolvedMssqlTarget,
    output_plan: MssqlTargetOutputPlan,
}

impl PlannedMssqlOutput {
    fn new(
        request: OutputWritePlan,
        resolved_target: ResolvedMssqlTarget,
        output_plan: MssqlTargetOutputPlan,
    ) -> Self {
        Self {
            request,
            resolved_target,
            output_plan,
        }
    }

    /// Returns the original lazy-table output request.
    #[must_use]
    pub const fn request(&self) -> &OutputWritePlan {
        &self.request
    }

    /// Returns the selected lazy table.
    #[must_use]
    pub const fn table(&self) -> &LazyTable {
        self.request.table()
    }

    /// Returns the selected MSSQL target request.
    #[must_use]
    pub const fn target(&self) -> &MssqlOutputTarget {
        self.request.target()
    }

    /// Returns the resolved SQL Server target, including the private connection config.
    #[must_use]
    pub const fn resolved_target(&self) -> &ResolvedMssqlTarget {
        &self.resolved_target
    }

    /// Returns the complete SQL Server target output plan.
    #[must_use]
    pub const fn output_plan(&self) -> &MssqlTargetOutputPlan {
        &self.output_plan
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
    fn new(
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

    fn no_cache(
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
    fn from_request(index: usize, request: &OutputWritePlan) -> Self {
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
    fn from_registered(derived: &RegisteredDerivedTable, output_indexes: Vec<usize>) -> Self {
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
    fn from_registered(
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
enum MssqlCacheFrontierSelection {
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

struct MssqlCoveredCacheAlias {
    // The skipped upstream alias.
    alias: MssqlDerivedCacheAliasPlan,
    // The selected downstream alias that covered it.
    selected_table_id: u64,
}

/// Registered Delta source tracked by a query-load session.
#[derive(Clone, PartialEq, Eq)]
pub struct RegisteredSessionSource {
    table: LazyTable,
    snapshot_version: u64,
    schema: SchemaRef,
    protocol: DeltaProtocolReport,
}

impl RegisteredSessionSource {
    fn from_registered(table: LazyTable, registered: RegisteredDeltaSource) -> Self {
        Self {
            table,
            snapshot_version: registered.snapshot_version,
            schema: registered.schema,
            protocol: registered.protocol,
        }
    }

    /// Returns the lazy table handle for this registered source.
    #[must_use]
    pub const fn table(&self) -> &LazyTable {
        &self.table
    }

    /// Returns the DataFusion table name for this source.
    #[must_use]
    pub fn name(&self) -> &str {
        self.table.name()
    }

    /// Returns the resolved Delta snapshot version.
    #[must_use]
    pub const fn snapshot_version(&self) -> u64 {
        self.snapshot_version
    }

    /// Returns the logical Arrow schema exposed to DataFusion.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the sanitized protocol report captured before registration.
    #[must_use]
    pub const fn protocol(&self) -> &DeltaProtocolReport {
        &self.protocol
    }
}

impl fmt::Debug for RegisteredSessionSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredSessionSource")
            .field("table", &self.table)
            .field("snapshot_version", &self.snapshot_version)
            .field("schema", &self.schema)
            .field("protocol", &self.protocol)
            .finish()
    }
}

/// Registered SQL-derived table alias tracked by a query-load session.
#[derive(Clone, PartialEq, Eq)]
pub struct RegisteredDerivedTable {
    table: LazyTable,
    schema: SchemaRef,
    sql_text: String,
    lineage: DerivedTableLineage,
}

impl RegisteredDerivedTable {
    fn new(
        table: LazyTable,
        schema: SchemaRef,
        sql_text: String,
        lineage: DerivedTableLineage,
    ) -> Self {
        Self {
            table,
            schema,
            sql_text,
            lineage,
        }
    }

    /// Returns the lazy table handle for this registered derived alias.
    #[must_use]
    pub const fn table(&self) -> &LazyTable {
        &self.table
    }

    /// Returns the DataFusion table name for this derived alias.
    #[must_use]
    pub fn name(&self) -> &str {
        self.table.name()
    }

    /// Returns the logical Arrow schema exposed to DataFusion.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the retained SQL text used to create this derived alias.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn sql_text(&self) -> &str {
        &self.sql_text
    }

    /// Returns dependency lineage captured from the retained SQL text.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) const fn lineage(&self) -> &DerivedTableLineage {
        &self.lineage
    }
}

impl fmt::Debug for RegisteredDerivedTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredDerivedTable")
            .field("table", &self.table)
            .field("schema", &self.schema)
            .field("sql_text", &"<redacted>")
            .field("lineage", &self.lineage)
            .finish()
    }
}

/// Direct dependency captured for one SQL-derived table.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum DerivedTableDependency {
    /// Reference to a registered Delta source alias.
    RegisteredSource { table_id: u64, name: String },
    /// Reference to a registered SQL-derived alias.
    RegisteredDerived { table_id: u64, name: String },
}

impl DerivedTableDependency {
    fn registered_source(source: &RegisteredSessionSource) -> Self {
        Self::RegisteredSource {
            table_id: source.table().id(),
            name: source.name().to_owned(),
        }
    }

    fn registered_derived(derived: &RegisteredDerivedTable) -> Self {
        Self::RegisteredDerived {
            table_id: derived.table().id(),
            name: derived.name().to_owned(),
        }
    }
}

/// Dependency lineage captured for one SQL-derived table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DerivedTableLineage {
    /// Session-owned source or derived aliases that this SQL-derived table reads
    /// directly.
    direct_dependencies: Vec<DerivedTableDependency>,
    /// Names declared inside this SQL statement, such as CTE names.
    ///
    /// These names are query-local and can shadow session aliases, so they are
    /// tracked separately to avoid treating them as session-owned dependencies.
    local_references: Vec<String>,
    /// Table references that DataFusion found in the SQL but that do not map to
    /// session-owned metadata or query-local names.
    unknown_references: Vec<String>,
    /// Reason lineage extraction could not complete while preserving
    /// table_from_sql behavior.
    incomplete_reason: Option<String>,
}

impl DerivedTableLineage {
    fn complete(
        direct_dependencies: Vec<DerivedTableDependency>,
        local_references: Vec<String>,
        unknown_references: Vec<String>,
    ) -> Self {
        Self {
            direct_dependencies,
            local_references,
            unknown_references,
            incomplete_reason: None,
        }
    }

    fn incomplete(message: impl Into<String>) -> Self {
        Self {
            incomplete_reason: Some(message.into()),
            ..Self::default()
        }
    }

    /// Returns direct session-owned source or derived dependencies.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn direct_dependencies(&self) -> &[DerivedTableDependency] {
        &self.direct_dependencies
    }

    /// Returns local query-scope references, such as CTE names.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn local_references(&self) -> &[String] {
        &self.local_references
    }

    /// Returns references that did not map to session-owned metadata.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn unknown_references(&self) -> &[String] {
        &self.unknown_references
    }

    /// Returns whether lineage capture completed without an extractor error.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) const fn is_complete(&self) -> bool {
        self.incomplete_reason.is_none()
    }
}

struct PendingDerivedTable {
    table: LazyTable,
    provider: Arc<dyn TableProvider>,
    schema: SchemaRef,
    sql_text: String,
    lineage: DerivedTableLineage,
}

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

    /// Returns registered Delta source reports in registration order.
    #[must_use]
    pub fn sources(&self) -> &[RegisteredSessionSource] {
        &self.sources
    }

    /// Finds a registered Delta source by alias using unquoted SQL semantics.
    #[must_use]
    pub fn registered_source(&self, name: &str) -> Option<&RegisteredSessionSource> {
        self.sources
            .iter()
            .find(|source| source.name().eq_ignore_ascii_case(name))
    }

    /// Returns registered SQL-derived aliases in registration order.
    #[must_use]
    pub fn derived_tables(&self) -> &[RegisteredDerivedTable] {
        &self.derived_tables
    }

    /// Finds a registered SQL-derived alias by name using unquoted SQL semantics.
    #[must_use]
    pub fn registered_derived_table(&self, name: &str) -> Option<&RegisteredDerivedTable> {
        self.derived_tables
            .iter()
            .find(|table| table.name().eq_ignore_ascii_case(name))
    }

    fn registered_derived_table_by_id(&self, table_id: u64) -> Option<&RegisteredDerivedTable> {
        self.derived_tables
            .iter()
            .find(|table| table.table().id() == table_id)
    }

    /// Resolves the session metadata for an alias that is eligible for scoped caching.
    ///
    /// The cache primitive only supports registered SQL-derived aliases. Raw
    /// sources, pending derived tables, and foreign or stale table handles are
    /// rejected before any DataFusion catalog mutation can happen.
    fn registered_derived_for_scoped_cache_alias(
        &self,
        table: &LazyTable,
    ) -> Result<&RegisteredDerivedTable, DeltaFunnelError> {
        if table.kind() != LazyTableKind::DerivedSql {
            return Err(unknown_lazy_table_error(table));
        }

        self.registered_derived_table_by_id(table.id())
            .ok_or_else(|| unknown_lazy_table_error(table))
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

        Ok(MssqlScopedCacheAliasReplacement {
            context: &self.context,
            table_id,
            alias_name,
            original_provider: Some(original_provider),
        })
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
    fn restore_original_after_cached_register_failure(
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

    /// Classifies one selected output relative to active cached aliases.
    ///
    /// Direct selected-alias use wins over lineage use because the normal
    /// registered-alias stream path should read the active cached provider.
    /// Dependent outputs are identified from captured lineage so later stream
    /// construction can replan from retained SQL while all cache aliases are
    /// installed.
    #[allow(dead_code)]
    fn cached_output_stream_route(
        &self,
        request: &OutputWritePlan,
        active_aliases: &[MssqlDerivedCacheAliasPlan],
    ) -> Result<MssqlCachedOutputStreamRoute, DeltaFunnelError> {
        if active_aliases.is_empty() {
            return Ok(MssqlCachedOutputStreamRoute::UncachedLazyTable);
        }

        for alias in active_aliases {
            self.registered_derived_table_by_id(alias.table_id())
                .ok_or_else(|| unknown_cached_alias_error(alias))?;
        }

        if let Some(alias) = active_aliases
            .iter()
            .find(|alias| request.table().id() == alias.table_id())
        {
            return Ok(MssqlCachedOutputStreamRoute::DirectCachedAlias(
                alias.clone(),
            ));
        }

        if request.table().kind() == LazyTableKind::DeltaSource {
            return Ok(MssqlCachedOutputStreamRoute::UncachedLazyTable);
        }

        let dependencies = self.transitive_registered_derived_dependencies(request.table())?;
        let dependent_aliases = active_aliases
            .iter()
            .filter(|alias| {
                dependencies.iter().any(|dependency| {
                    matches!(
                        dependency,
                        DerivedTableDependency::RegisteredDerived { table_id, .. }
                            if *table_id == alias.table_id()
                    )
                })
            })
            .cloned()
            .collect::<Vec<_>>();

        if dependent_aliases.is_empty() {
            Ok(MssqlCachedOutputStreamRoute::UncachedLazyTable)
        } else {
            Ok(MssqlCachedOutputStreamRoute::ReplannedCachedDependency(
                dependent_aliases,
            ))
        }
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
            .position(|pending| pending.table.id == table.id)
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
        match table.kind() {
            LazyTableKind::DeltaSource => {
                let source = self
                    .sources
                    .iter()
                    .find(|source| source.table.id == table.id)
                    .ok_or_else(|| unknown_lazy_table_error(table))?;

                self.context
                    .table(source.name())
                    .await
                    .map_err(|error| datafusion_handoff_setup_error("registered_table", error))
            }
            LazyTableKind::DerivedSql => {
                if let Some(derived) = self
                    .derived_tables
                    .iter()
                    .find(|derived| derived.table.id == table.id)
                {
                    return self.context.table(derived.name()).await.map_err(|error| {
                        datafusion_handoff_setup_error("registered_table", error)
                    });
                }

                let pending = self
                    .pending_derived_tables
                    .iter()
                    .find(|pending| pending.table.id == table.id)
                    .ok_or_else(|| unknown_lazy_table_error(table))?;

                self.context
                    .read_table(Arc::clone(&pending.provider))
                    .map_err(|error| datafusion_handoff_setup_error("pending_table", error))
            }
        }
    }

    fn schema_for_lazy_table(&self, table: &LazyTable) -> Result<&SchemaRef, DeltaFunnelError> {
        match table.kind() {
            LazyTableKind::DeltaSource => self
                .sources
                .iter()
                .find(|source| source.table.id == table.id)
                .map(RegisteredSessionSource::schema),
            LazyTableKind::DerivedSql => self
                .derived_tables
                .iter()
                .find(|derived| derived.table.id == table.id)
                .map(RegisteredDerivedTable::schema)
                .or_else(|| {
                    self.pending_derived_tables
                        .iter()
                        .find(|pending| pending.table.id == table.id)
                        .map(|pending| &pending.schema)
                }),
        }
        .ok_or_else(|| unknown_lazy_table_error(table))
    }

    #[allow(dead_code)]
    pub(crate) fn sql_text_for_derived_table(
        &self,
        table: &LazyTable,
    ) -> Result<&str, DeltaFunnelError> {
        if table.kind() != LazyTableKind::DerivedSql {
            return Err(unknown_lazy_table_error(table));
        }

        self.derived_tables
            .iter()
            .find(|derived| derived.table.id == table.id)
            .map(RegisteredDerivedTable::sql_text)
            .or_else(|| {
                self.pending_derived_tables
                    .iter()
                    .find(|pending| pending.table.id == table.id)
                    .map(|pending| pending.sql_text.as_str())
            })
            .ok_or_else(|| unknown_lazy_table_error(table))
    }

    #[allow(dead_code)]
    pub(crate) fn lineage_for_derived_table(
        &self,
        table: &LazyTable,
    ) -> Result<&DerivedTableLineage, DeltaFunnelError> {
        if table.kind() != LazyTableKind::DerivedSql {
            return Err(unknown_lazy_table_error(table));
        }

        self.derived_tables
            .iter()
            .find(|derived| derived.table.id == table.id)
            .map(RegisteredDerivedTable::lineage)
            .or_else(|| {
                self.pending_derived_tables
                    .iter()
                    .find(|pending| pending.table.id == table.id)
                    .map(|pending| &pending.lineage)
            })
            .ok_or_else(|| unknown_lazy_table_error(table))
    }

    #[allow(dead_code)]
    pub(crate) fn transitive_registered_derived_dependencies(
        &self,
        table: &LazyTable,
    ) -> Result<Vec<DerivedTableDependency>, DeltaFunnelError> {
        let lineage = self.lineage_for_derived_table(table)?;
        let mut visited_table_ids = BTreeSet::new();
        let mut dependencies = BTreeSet::new();

        self.collect_transitive_registered_derived_dependencies(
            lineage,
            &mut visited_table_ids,
            &mut dependencies,
        )?;

        Ok(dependencies.into_iter().collect())
    }

    #[allow(dead_code)]
    pub(crate) fn lazy_table_depends_on_registered_derived(
        &self,
        table: &LazyTable,
        candidate: &LazyTable,
    ) -> Result<bool, DeltaFunnelError> {
        self.schema_for_lazy_table(table)?;
        if candidate.kind() != LazyTableKind::DerivedSql {
            return Err(unknown_lazy_table_error(candidate));
        }

        self.registered_derived_table_by_id(candidate.id())
            .ok_or_else(|| unknown_lazy_table_error(candidate))?;
        if table.id() == candidate.id() {
            return Ok(true);
        }
        if table.kind() == LazyTableKind::DeltaSource {
            return Ok(false);
        }

        Ok(self
            .transitive_registered_derived_dependencies(table)?
            .iter()
            .any(|dependency| {
                matches!(
                    dependency,
                    DerivedTableDependency::RegisteredDerived { table_id, .. }
                        if *table_id == candidate.id()
                )
            }))
    }

    fn collect_transitive_registered_derived_dependencies(
        &self,
        lineage: &DerivedTableLineage,
        visited_table_ids: &mut BTreeSet<u64>,
        dependencies: &mut BTreeSet<DerivedTableDependency>,
    ) -> Result<(), DeltaFunnelError> {
        for dependency in lineage.direct_dependencies() {
            let DerivedTableDependency::RegisteredDerived { table_id, name } = dependency else {
                continue;
            };
            if !visited_table_ids.insert(*table_id) {
                continue;
            }

            dependencies.insert(dependency.clone());
            let derived = self.registered_derived_table_by_id(*table_id).ok_or_else(|| {
                DeltaFunnelError::MssqlWorkflowPlanning {
                    message: format!(
                        "registered derived lineage dependency `{}` is not registered in this session",
                        sanitize_text_for_display(name)
                    ),
                }
            })?;
            self.collect_transitive_registered_derived_dependencies(
                derived.lineage(),
                visited_table_ids,
                dependencies,
            )?;
        }

        Ok(())
    }

    fn derive_table_lineage_from_sql(&self, sql: &str) -> DerivedTableLineage {
        match self.extract_table_lineage_from_sql(sql) {
            Ok(lineage) => lineage,
            // Lineage is advisory metadata for later cache planning. Keep the
            // existing table_from_sql behavior intact if extraction fails.
            Err(error) => DerivedTableLineage::incomplete(error.to_string()),
        }
    }

    fn extract_table_lineage_from_sql(
        &self,
        sql: &str,
    ) -> Result<DerivedTableLineage, DeltaFunnelError> {
        // Reuse DataFusion's SQL parser and table-reference resolver so lineage
        // follows the same SQL dialect and CTE scoping rules as planning.
        let mut statements =
            DFParser::parse_sql(sql).map_err(|error| DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::ValidateSql,
                message: error.to_string(),
            })?;
        if statements.len() != 1 {
            return sql_table_error(
                SqlTablePhase::ValidateSql,
                "expected exactly one SQL statement for lineage extraction",
            );
        }
        let statement = statements
            .pop_front()
            .ok_or_else(|| DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::ValidateSql,
                message: "expected parsed SQL statement for lineage extraction".to_owned(),
            })?;
        let (relations, ctes) = resolve_table_references(&statement, true).map_err(|error| {
            DeltaFunnelError::SqlTable {
                phase: SqlTablePhase::ValidateSql,
                message: error.to_string(),
            }
        })?;

        Ok(self.classify_lineage_references(relations, ctes))
    }

    fn classify_lineage_references(
        &self,
        relations: Vec<TableReference>,
        ctes: Vec<TableReference>,
    ) -> DerivedTableLineage {
        // CTE names are local to this SQL statement. They can shadow session
        // aliases, so classify them before checking registered session tables.
        let local_references = sorted_reference_strings(ctes.into_iter());
        let local_reference_names = local_references
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let mut dependencies = BTreeSet::new();
        let mut unknown_references = BTreeSet::new();

        for relation in relations {
            // Session aliases are currently registered as bare names. Qualified
            // references might be external catalog/schema names, so keep them
            // visible as unknown instead of guessing.
            let Some(name) = bare_table_reference_name(&relation) else {
                unknown_references.insert(relation.to_string());
                continue;
            };
            if local_reference_names.contains(name) {
                continue;
            }
            // Alias registration rejects source/derived name collisions, so a
            // bare name can map to at most one session-owned object.
            if let Some(derived) = self.registered_derived_table(name) {
                dependencies.insert(DerivedTableDependency::registered_derived(derived));
            } else if let Some(source) = self.registered_source(name) {
                dependencies.insert(DerivedTableDependency::registered_source(source));
            } else {
                unknown_references.insert(relation.to_string());
            }
        }

        DerivedTableLineage::complete(
            dependencies.into_iter().collect(),
            local_references,
            unknown_references.into_iter().collect(),
        )
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

fn sql_table_error<T>(
    phase: SqlTablePhase,
    message: impl Into<String>,
) -> Result<T, DeltaFunnelError> {
    Err(DeltaFunnelError::SqlTable {
        phase,
        message: message.into(),
    })
}

fn unknown_lazy_table_error(table: &LazyTable) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWorkflowPlanning {
        message: format!(
            "lazy table `{}` is not registered in this session",
            sanitize_text_for_display(table.name())
        ),
    }
}

fn unknown_cached_alias_error(alias: &MssqlDerivedCacheAliasPlan) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWorkflowPlanning {
        message: format!(
            "cached alias `{}` is not registered in this session",
            sanitize_text_for_display(alias.alias())
        ),
    }
}

fn mssql_scoped_cache_alias_error(
    phase: &'static str,
    alias_name: &str,
    error: impl fmt::Display,
) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWorkflowPlanning {
        message: format!(
            "scoped MSSQL cache alias {phase} failed for `{}`: {}",
            sanitize_text_for_display(alias_name),
            sanitize_text_for_display(&error.to_string())
        ),
    }
}

fn datafusion_handoff_setup_error(
    option: &'static str,
    error: impl fmt::Display,
) -> DeltaFunnelError {
    DeltaFunnelError::BatchPipeline {
        phase: BatchPipelinePhase::HandoffSetup,
        option,
        message: error.to_string(),
    }
}

fn ensure_execute_run_mode(run_mode: RunMode) -> Result<(), DeltaFunnelError> {
    match run_mode {
        RunMode::Execute => Ok(()),
        RunMode::DryRun => Err(DeltaFunnelError::MssqlWorkflowPlanning {
            message: "write_to_mssql requires RunMode::Execute; use plan_mssql_output for dry-run planning".to_owned(),
        }),
    }
}

fn sorted_reference_strings(references: impl Iterator<Item = TableReference>) -> Vec<String> {
    // Stable ordering and de-duplication make lineage deterministic for tests,
    // debug output, and later cache candidate comparisons.
    references
        .map(|reference| reference.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn bare_table_reference_name(reference: &TableReference) -> Option<&str> {
    match reference {
        TableReference::Bare { table } => Some(table.as_ref()),
        // Do not collapse catalog/schema-qualified names into a bare alias.
        // That would make external tables look session-owned.
        TableReference::Partial { .. } | TableReference::Full { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicUsize, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        BatchPipelinePhase, DeltaProviderReaderBackend, DeltaStorageOptions, LoadMode,
        MssqlConnectionSource, MssqlTargetCleanupStatus, MssqlTargetTable,
        table_formats::RealParquetDeltaTable,
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
        datasource::MemTable,
        error::Result as DataFusionResult,
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
    fn default_session_constructs_datafusion_context() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;

        assert_eq!(session.options().query_options(), QueryOptions::default());
        assert!(
            session
                .options()
                .validation_options()
                .require_successful_planning()
        );
        assert_eq!(
            session.context().state().config().target_partitions(),
            datafusion::prelude::SessionConfig::new().target_partitions()
        );
        assert_eq!(session.next_table_id(), 0);
        Ok(())
    }

    #[test]
    fn session_applies_query_options_to_datafusion_context() -> Result<(), DeltaFunnelError> {
        let session =
            DeltaFunnelSession::new(SessionOptions::new().with_query_options(QueryOptions {
                target_partitions: Some(3),
                output_batch_size: Some(11),
            }))?;

        assert_eq!(session.context().state().config().target_partitions(), 3);
        assert_eq!(session.context().state().config().batch_size(), 11);
        Ok(())
    }

    #[test]
    fn query_option_validation_failure_reaches_session_construction() {
        let error =
            DeltaFunnelSession::new(SessionOptions::new().with_query_options(QueryOptions {
                target_partitions: Some(0),
                output_batch_size: None,
            }));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::BatchPipeline {
                phase: BatchPipelinePhase::Configuration,
                option: "target_partitions",
                ..
            })
        ));
    }

    #[test]
    fn provider_option_validation_failure_reaches_session_construction() {
        let error = DeltaFunnelSession::new(SessionOptions::new().with_provider_scan_options(
            DeltaProviderScanExecutionOptions {
                reader_backend: DeltaProviderReaderBackend::OfficialKernel,
                max_concurrent_file_reads_per_scan: 1,
                max_concurrent_file_reads_per_partition: 1,
                output_buffer_capacity_per_partition: 0,
                native_async_prefetch_file_count_per_partition: 0,
            },
        ));

        assert!(matches!(error, Err(DeltaFunnelError::Config { .. })));
    }

    #[test]
    fn workflow_parallelism_validation_failure_reaches_session_construction() {
        let error = DeltaFunnelSession::new(SessionOptions::new().with_mssql_workflow_options(
            MssqlWorkflowWriteOptions::new().with_max_parallel_outputs(2),
        ));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("parallel MSSQL output writers are not supported")
        ));
    }

    #[test]
    fn workflow_zero_parallelism_validation_failure_reaches_session_construction() {
        let error = DeltaFunnelSession::new(SessionOptions::new().with_mssql_workflow_options(
            MssqlWorkflowWriteOptions::new().with_max_parallel_outputs(0),
        ));

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("max_parallel_outputs must be at least 1")
        ));
    }

    #[test]
    fn session_debug_redacts_default_mssql_connection() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;

        let debug = format!("{session:?}");
        assert!(debug.contains("warehouse-primary"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        Ok(())
    }

    #[test]
    fn output_request_shapes_preserve_table_target_and_run_mode() -> Result<(), DeltaFunnelError> {
        let table = LazyTable::placeholder(7, LazyTableKind::DerivedSql);
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?)
            .with_load_mode(LoadMode::CreateAndLoad)
            .with_connection(secret_connection()?);
        let target = MssqlOutputTarget::new("orders_output", target_config, RunMode::DryRun);
        let plan = OutputWritePlan::new(table.clone(), target.clone());

        assert_eq!(table.id(), 7);
        assert_eq!(table.kind(), LazyTableKind::DerivedSql);
        assert_eq!(target.output_name(), "orders_output");
        assert_eq!(target.run_mode(), RunMode::DryRun);
        assert_eq!(target.target().load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(plan.table(), &table);
        assert_eq!(plan.target(), &target);

        let debug = format!("{target:?}");
        assert!(debug.contains("orders_output"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
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
        let aliases = vec![MssqlDerivedCacheAliasPlan {
            table_id: 252,
            alias: "missing_cache".to_owned(),
            output_indexes: vec![0],
        }];

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
                    && message.contains("plan_mssql_output")
        ));
        assert!(writer.calls.is_empty());
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
        assert_eq!(registered.snapshot_version(), 1);
        assert_eq!(registered.protocol().source_name, "orders");
        assert_eq!(registered.schema().fields().len(), 2);

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
        Ok(())
    }

    #[test]
    fn source_registration_honors_configured_provider_options()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("configured-provider")?;
        let mut session =
            DeltaFunnelSession::new(SessionOptions::new().with_provider_scan_options(
                DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
                    DeltaProviderReaderBackend::OfficialKernel,
                    2,
                    1,
                )?,
            ))?;

        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        assert_eq!(session.sources().len(), 1);
        assert!(session.registered_source("orders").is_some());
        Ok(())
    }
}
