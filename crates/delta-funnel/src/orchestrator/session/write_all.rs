use std::{fmt, sync::Arc};

use datafusion::{datasource::TableProvider, prelude::SessionContext};

use crate::{
    DeltaFunnelError, MssqlOutputWriteStatus, MssqlWorkflowWriteReport,
    support::sanitize_text_for_display,
};

use super::{
    DeltaSourceReport, OutputWritePlan, RegisteredDerivedTable, mssql_scoped_cache_alias_error,
};

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
