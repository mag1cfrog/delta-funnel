use std::fmt;

use crate::{
    DeltaSourceReport, MssqlOutputWriteStatus, MssqlWorkflowWriteReport, PhaseTimingReport,
    QueryExecutionProfile, support::sanitize_text_for_display,
};

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
    phase_timings: Vec<PhaseTimingReport>,
}

impl WriteAllReport {
    pub(crate) fn new(
        workflow: MssqlWorkflowWriteReport,
        cache: WriteAllCacheReport,
        sources: Vec<DeltaSourceReport>,
    ) -> Self {
        Self {
            workflow,
            cache,
            sources,
            phase_timings: Vec::new(),
        }
    }

    pub(crate) fn with_phase_timings(mut self, phase_timings: Vec<PhaseTimingReport>) -> Self {
        self.phase_timings = phase_timings;
        self
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

    /// Returns top-level `write_all` workflow phase timing reports.
    #[must_use]
    pub fn phase_timings(&self) -> &[PhaseTimingReport] {
        &self.phase_timings
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
    pub(crate) fn disabled() -> Self {
        Self::Disabled
    }

    pub(crate) fn no_cache(
        reason: WriteAllNoCacheReason,
        skipped_candidates: Vec<WriteAllCacheCandidateSkip>,
    ) -> Self {
        Self::NoCache {
            reason,
            skipped_candidates,
        }
    }

    pub(crate) fn cache_aliases(
        aliases: Vec<WriteAllCacheAliasReport>,
        skipped_candidates: Vec<WriteAllCacheCandidateSkip>,
    ) -> Self {
        Self::CacheAliases {
            aliases,
            skipped_candidates,
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
    phase_timings: Vec<PhaseTimingReport>,
    failed_phase: Option<String>,
    execution_profile: Option<QueryExecutionProfile>,
}

impl WriteAllCacheAliasReport {
    pub(crate) fn selected(
        table_id: u64,
        alias: impl Into<String>,
        output_indexes: Vec<usize>,
    ) -> Self {
        Self {
            table_id,
            alias: alias.into(),
            output_indexes,
            status: WriteAllCacheAliasStatus::Selected,
            phase_timings: Vec::new(),
            failed_phase: None,
            execution_profile: None,
        }
    }

    pub(crate) fn executed(
        table_id: u64,
        alias: impl Into<String>,
        output_indexes: Vec<usize>,
        status: WriteAllCacheAliasStatus,
        phase_timings: Vec<PhaseTimingReport>,
        failed_phase: Option<&'static str>,
    ) -> Self {
        debug_assert_ne!(status, WriteAllCacheAliasStatus::Selected);
        debug_assert_eq!(
            status == WriteAllCacheAliasStatus::Failed,
            failed_phase.is_some()
        );
        Self {
            table_id,
            alias: alias.into(),
            output_indexes,
            status,
            phase_timings,
            failed_phase: failed_phase.map(str::to_owned),
            execution_profile: None,
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

    /// Returns lifecycle timings for an attempted cache alias.
    ///
    /// Plan-shaped [`WriteAllCacheAliasStatus::Selected`] reports return an
    /// empty slice because no lifecycle phase was attempted.
    #[must_use]
    pub fn phase_timings(&self) -> &[PhaseTimingReport] {
        &self.phase_timings
    }

    /// Returns the primary failed lifecycle phase for a failed alias.
    #[must_use]
    pub fn failed_phase(&self) -> Option<&str> {
        self.failed_phase.as_deref()
    }

    /// Returns the terminal cache materialization profile when collection was enabled.
    #[must_use]
    pub const fn execution_profile(&self) -> Option<&QueryExecutionProfile> {
        self.execution_profile.as_ref()
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
            .field("phase_timings", &self.phase_timings)
            .field("failed_phase", &self.failed_phase)
            .field("execution_profile", &self.execution_profile)
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
    /// Cache materialization, installation, or restoration failed.
    Failed,
}

/// Structured details retained when a cache-enabled `write_all` call fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteAllCacheFailure {
    aliases: Vec<WriteAllCacheAliasReport>,
    primary_failed_alias_table_id: Option<u64>,
    workflow: Option<MssqlWorkflowWriteReport>,
}

impl WriteAllCacheFailure {
    pub(crate) fn new(
        aliases: Vec<WriteAllCacheAliasReport>,
        primary_failed_alias_table_id: Option<u64>,
        workflow: Option<MssqlWorkflowWriteReport>,
    ) -> Self {
        Self {
            aliases,
            primary_failed_alias_table_id,
            workflow,
        }
    }

    /// Returns every attempted cache alias in deterministic selection order.
    #[must_use]
    pub fn aliases(&self) -> &[WriteAllCacheAliasReport] {
        &self.aliases
    }

    /// Returns the table id whose cache phase caused the primary failure.
    #[must_use]
    pub const fn primary_failed_alias_table_id(&self) -> Option<u64> {
        self.primary_failed_alias_table_id
    }

    /// Returns the completed output workflow when restoration later failed.
    #[must_use]
    pub const fn workflow(&self) -> Option<&MssqlWorkflowWriteReport> {
        self.workflow.as_ref()
    }
}

impl WriteAllCacheAliasStatus {
    /// Returns the stable lower-snake-case report value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Selected => "selected",
            Self::MaterializedAndRestored => "materialized_and_restored",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for WriteAllCacheAliasStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Registered derived alias skipped during conservative cache selection.
#[derive(Clone, PartialEq, Eq)]
pub struct WriteAllCacheCandidateSkip {
    table_id: u64,
    alias: String,
    reason: WriteAllCacheCandidateSkipReason,
}

impl WriteAllCacheCandidateSkip {
    pub(crate) fn new(
        table_id: u64,
        alias: impl Into<String>,
        reason: WriteAllCacheCandidateSkipReason,
    ) -> Self {
        Self {
            table_id,
            alias: alias.into(),
            reason,
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
