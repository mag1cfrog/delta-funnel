use std::fmt;

use crate::{MssqlOutputWriteStatus, MssqlWorkflowWriteReport, support::sanitize_text_for_display};

use super::{
    DeltaSourceReport, MssqlCacheCandidateSkip, MssqlCacheCandidateSkipReason,
    MssqlDerivedCacheAliasPlan, MssqlNoCacheReason, MssqlOutputCacheDecision, MssqlOutputCachePlan,
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
