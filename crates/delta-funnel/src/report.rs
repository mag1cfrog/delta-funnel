//! Shared report primitives for query-load readiness.
//!
//! This module owns the vocabulary that later dry-run, execution, validation,
//! timing, tracing, Rust, and Python report slices reuse. The types are kept
//! serializable-friendly by exposing stable string codes and primitive values
//! without adding a serialization dependency in this slice.

use std::fmt;

use crate::error::DeltaFunnelError;

/// Saturates a platform-sized count into the public `u64` report shape.
#[must_use]
pub const fn usize_to_u64_saturating(value: usize) -> u64 {
    if size_of::<usize>() > size_of::<u64>() && value > u64::MAX as usize {
        u64::MAX
    } else {
        value as u64
    }
}

/// Saturates a wide count into the public `u64` report shape.
#[must_use]
pub const fn u128_to_u64_saturating(value: u128) -> u64 {
    if value > u64::MAX as u128 {
        u64::MAX
    } else {
        value as u64
    }
}

/// Controls whether target-side validation should run when a workflow supports it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TargetValidationMode {
    /// Do not run target-side validation.
    Disabled,
    /// Run target-side validation when the selected workflow can do so.
    #[default]
    ValidateIfPossible,
    /// Require target-side validation and fail when it cannot be completed.
    Require,
}

impl TargetValidationMode {
    /// Returns a stable lower-snake-case code for report serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::ValidateIfPossible => "validate_if_possible",
            Self::Require => "require",
        }
    }
}

impl fmt::Display for TargetValidationMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Controls how much source scan metadata a dry-run report should collect.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DryRunScanSummaryMode {
    /// Use metadata available from normal planning paths.
    #[default]
    MetadataOnly,
    /// Exhaust scan metadata paths when callers need fuller source summaries.
    ExhaustScanMetadata,
}

impl DryRunScanSummaryMode {
    /// Returns a stable lower-snake-case code for report serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::ExhaustScanMetadata => "exhaust_scan_metadata",
        }
    }
}

impl fmt::Display for DryRunScanSummaryMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Classification for a reported row count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowCountKind {
    /// The count is exact for the reported scope.
    Exact,
    /// The count is an estimate from metadata, planning, or another non-exact source.
    Estimated,
    /// The count is an observed partial total and must not be treated as exact.
    Partial,
    /// No row count is available.
    Unavailable,
}

impl RowCountKind {
    /// Returns a stable lower-snake-case code for report serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Estimated => "estimated",
            Self::Partial => "partial",
            Self::Unavailable => "unavailable",
        }
    }
}

impl fmt::Display for RowCountKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Row count evidence for a report field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowCount {
    /// An exact row count for the reported scope.
    Exact(u64),
    /// A row count estimate from metadata, planning, or another non-exact source.
    Estimated(u64),
    /// A partial observed count. This is not proof of the final total.
    Partial(u64),
    /// No row count is available.
    Unavailable,
}

impl RowCount {
    /// Creates an exact row count.
    #[must_use]
    pub const fn exact(value: u64) -> Self {
        Self::Exact(value)
    }

    /// Creates an exact row count from a platform-sized value, saturating at `u64::MAX`.
    #[must_use]
    pub const fn exact_from_usize(value: usize) -> Self {
        Self::Exact(usize_to_u64_saturating(value))
    }

    /// Creates an exact row count from a wide value, saturating at `u64::MAX`.
    #[must_use]
    pub const fn exact_from_u128(value: u128) -> Self {
        Self::Exact(u128_to_u64_saturating(value))
    }

    /// Creates an estimated row count.
    #[must_use]
    pub const fn estimated(value: u64) -> Self {
        Self::Estimated(value)
    }

    /// Creates an estimated row count from a platform-sized value, saturating at `u64::MAX`.
    #[must_use]
    pub const fn estimated_from_usize(value: usize) -> Self {
        Self::Estimated(usize_to_u64_saturating(value))
    }

    /// Creates an estimated row count from a wide value, saturating at `u64::MAX`.
    #[must_use]
    pub const fn estimated_from_u128(value: u128) -> Self {
        Self::Estimated(u128_to_u64_saturating(value))
    }

    /// Creates a partial observed row count.
    #[must_use]
    pub const fn partial(value: u64) -> Self {
        Self::Partial(value)
    }

    /// Creates a partial observed row count from a platform-sized value, saturating at `u64::MAX`.
    #[must_use]
    pub const fn partial_from_usize(value: usize) -> Self {
        Self::Partial(usize_to_u64_saturating(value))
    }

    /// Creates a partial observed row count from a wide value, saturating at `u64::MAX`.
    #[must_use]
    pub const fn partial_from_u128(value: u128) -> Self {
        Self::Partial(u128_to_u64_saturating(value))
    }

    /// Creates an unavailable row count.
    #[must_use]
    pub const fn unavailable() -> Self {
        Self::Unavailable
    }

    /// Returns the count classification.
    #[must_use]
    pub const fn kind(&self) -> RowCountKind {
        match self {
            Self::Exact(_) => RowCountKind::Exact,
            Self::Estimated(_) => RowCountKind::Estimated,
            Self::Partial(_) => RowCountKind::Partial,
            Self::Unavailable => RowCountKind::Unavailable,
        }
    }

    /// Returns the numeric count when the report carries one.
    #[must_use]
    pub const fn value(&self) -> Option<u64> {
        match self {
            Self::Exact(value) | Self::Estimated(value) | Self::Partial(value) => Some(*value),
            Self::Unavailable => None,
        }
    }

    /// Returns the exact count, if this value proves one.
    #[must_use]
    pub const fn exact_value(&self) -> Option<u64> {
        match self {
            Self::Exact(value) => Some(*value),
            Self::Estimated(_) | Self::Partial(_) | Self::Unavailable => None,
        }
    }

    /// Returns the estimated count, if this value carries an estimate.
    #[must_use]
    pub const fn estimated_value(&self) -> Option<u64> {
        match self {
            Self::Estimated(value) => Some(*value),
            Self::Exact(_) | Self::Partial(_) | Self::Unavailable => None,
        }
    }

    /// Returns the partial count, if this value carries a partial observation.
    #[must_use]
    pub const fn partial_value(&self) -> Option<u64> {
        match self {
            Self::Partial(value) => Some(*value),
            Self::Exact(_) | Self::Estimated(_) | Self::Unavailable => None,
        }
    }

    /// Returns whether the count is exact.
    #[must_use]
    pub const fn is_exact(&self) -> bool {
        matches!(self, Self::Exact(_))
    }

    /// Returns whether the count is estimated.
    #[must_use]
    pub const fn is_estimated(&self) -> bool {
        matches!(self, Self::Estimated(_))
    }

    /// Returns whether the count is partial.
    #[must_use]
    pub const fn is_partial(&self) -> bool {
        matches!(self, Self::Partial(_))
    }

    /// Returns whether no row count is available.
    #[must_use]
    pub const fn is_unavailable(&self) -> bool {
        matches!(self, Self::Unavailable)
    }
}

impl fmt::Display for RowCount {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.value() {
            Some(value) => write!(formatter, "{}:{value}", self.kind()),
            None => formatter.write_str(self.kind().as_str()),
        }
    }
}

/// Classification for a reported file count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileCountKind {
    /// The count is exact for the reported scope.
    Exact,
    /// The count is an estimate from metadata, planning, or another non-exact source.
    Estimated,
    /// No file count is available.
    Unavailable,
    /// File counting was intentionally skipped.
    Skipped,
    /// The workflow step that would count files did not execute.
    NotExecuted,
}

impl FileCountKind {
    /// Returns a stable lower-snake-case code for report serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Estimated => "estimated",
            Self::Unavailable => "unavailable",
            Self::Skipped => "skipped",
            Self::NotExecuted => "not_executed",
        }
    }
}

impl fmt::Display for FileCountKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// File count evidence for a report field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileCount {
    /// An exact file count for the reported scope.
    Exact(u64),
    /// A file count estimate from metadata, planning, or another non-exact source.
    Estimated(u64),
    /// No file count is available.
    Unavailable,
    /// File counting was intentionally skipped.
    Skipped,
    /// The workflow step that would count files did not execute.
    NotExecuted,
}

impl FileCount {
    /// Creates an exact file count.
    #[must_use]
    pub const fn exact(value: u64) -> Self {
        Self::Exact(value)
    }

    /// Creates an exact file count from a platform-sized value, saturating at `u64::MAX`.
    #[must_use]
    pub const fn exact_from_usize(value: usize) -> Self {
        Self::Exact(usize_to_u64_saturating(value))
    }

    /// Creates an exact file count from a wide value, saturating at `u64::MAX`.
    #[must_use]
    pub const fn exact_from_u128(value: u128) -> Self {
        Self::Exact(u128_to_u64_saturating(value))
    }

    /// Creates an estimated file count.
    #[must_use]
    pub const fn estimated(value: u64) -> Self {
        Self::Estimated(value)
    }

    /// Creates an estimated file count from a platform-sized value, saturating at `u64::MAX`.
    #[must_use]
    pub const fn estimated_from_usize(value: usize) -> Self {
        Self::Estimated(usize_to_u64_saturating(value))
    }

    /// Creates an estimated file count from a wide value, saturating at `u64::MAX`.
    #[must_use]
    pub const fn estimated_from_u128(value: u128) -> Self {
        Self::Estimated(u128_to_u64_saturating(value))
    }

    /// Creates an unavailable file count.
    #[must_use]
    pub const fn unavailable() -> Self {
        Self::Unavailable
    }

    /// Creates a skipped file count.
    #[must_use]
    pub const fn skipped() -> Self {
        Self::Skipped
    }

    /// Creates a not-executed file count.
    #[must_use]
    pub const fn not_executed() -> Self {
        Self::NotExecuted
    }

    /// Returns the count classification.
    #[must_use]
    pub const fn kind(&self) -> FileCountKind {
        match self {
            Self::Exact(_) => FileCountKind::Exact,
            Self::Estimated(_) => FileCountKind::Estimated,
            Self::Unavailable => FileCountKind::Unavailable,
            Self::Skipped => FileCountKind::Skipped,
            Self::NotExecuted => FileCountKind::NotExecuted,
        }
    }

    /// Returns the numeric count when the report carries one.
    #[must_use]
    pub const fn value(&self) -> Option<u64> {
        match self {
            Self::Exact(value) | Self::Estimated(value) => Some(*value),
            Self::Unavailable | Self::Skipped | Self::NotExecuted => None,
        }
    }

    /// Returns the exact count, if this value proves one.
    #[must_use]
    pub const fn exact_value(&self) -> Option<u64> {
        match self {
            Self::Exact(value) => Some(*value),
            Self::Estimated(_) | Self::Unavailable | Self::Skipped | Self::NotExecuted => None,
        }
    }

    /// Returns the estimated count, if this value carries an estimate.
    #[must_use]
    pub const fn estimated_value(&self) -> Option<u64> {
        match self {
            Self::Estimated(value) => Some(*value),
            Self::Exact(_) | Self::Unavailable | Self::Skipped | Self::NotExecuted => None,
        }
    }

    /// Returns whether the count is exact.
    #[must_use]
    pub const fn is_exact(&self) -> bool {
        matches!(self, Self::Exact(_))
    }

    /// Returns whether the count is estimated.
    #[must_use]
    pub const fn is_estimated(&self) -> bool {
        matches!(self, Self::Estimated(_))
    }

    /// Returns whether no file count is available.
    #[must_use]
    pub const fn is_unavailable(&self) -> bool {
        matches!(self, Self::Unavailable)
    }

    /// Returns whether file counting was intentionally skipped.
    #[must_use]
    pub const fn is_skipped(&self) -> bool {
        matches!(self, Self::Skipped)
    }

    /// Returns whether the workflow step that would count files did not execute.
    #[must_use]
    pub const fn is_not_executed(&self) -> bool {
        matches!(self, Self::NotExecuted)
    }
}

impl fmt::Display for FileCount {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.value() {
            Some(value) => write!(formatter, "{}:{value}", self.kind()),
            None => formatter.write_str(self.kind().as_str()),
        }
    }
}

/// Stable reason codes for skipped, unavailable, and not-executed report states.
///
/// These codes are intentionally reusable across validation, source summary,
/// output execution, and workflow phases so later reports do not need parallel
/// reason vocabularies for the same state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportReasonCode {
    /// Validation was disabled by caller configuration.
    ValidationDisabled,
    /// The workflow ran in dry-run mode.
    DryRun,
    /// A required system, provider, or output capability was unavailable.
    CapabilityUnavailable,
    /// A required permission was unavailable.
    PermissionUnavailable,
    /// A prior failure made this report state unreachable.
    PriorFailure,
    /// The requested load mode does not support this report state.
    UnsupportedLoadMode,
    /// The target could not be accessed for this report state.
    MissingTargetAccess,
    /// Exact output row evidence was required but not available.
    MissingExactOutputRows,
    /// Work was skipped to avoid expensive or invasive reads.
    CostAvoidance,
    /// The workflow step was not executed.
    NotExecuted,
    /// The workflow failed before validation could run.
    FailureBeforeValidation,
}

impl ReportReasonCode {
    /// Returns a stable lower-snake-case code for report serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ValidationDisabled => "validation_disabled",
            Self::DryRun => "dry_run",
            Self::CapabilityUnavailable => "capability_unavailable",
            Self::PermissionUnavailable => "permission_unavailable",
            Self::PriorFailure => "prior_failure",
            Self::UnsupportedLoadMode => "unsupported_load_mode",
            Self::MissingTargetAccess => "missing_target_access",
            Self::MissingExactOutputRows => "missing_exact_output_rows",
            Self::CostAvoidance => "cost_avoidance",
            Self::NotExecuted => "not_executed",
            Self::FailureBeforeValidation => "failure_before_validation",
        }
    }
}

impl fmt::Display for ReportReasonCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Classification for target-side validation status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationStatusKind {
    /// Validation was disabled by caller configuration.
    Disabled,
    /// Validation ran and passed.
    Passed,
    /// Validation ran and failed.
    Failed,
    /// Validation did not run because it was intentionally skipped.
    Skipped,
    /// Validation could not run because required evidence was unavailable.
    Unavailable,
    /// Validation was required and could not pass.
    RequiredButFailed,
}

impl ValidationStatusKind {
    /// Returns a stable lower-snake-case code for report serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Unavailable => "unavailable",
            Self::RequiredButFailed => "required_but_failed",
        }
    }
}

impl fmt::Display for ValidationStatusKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Target-side validation status for a report scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationStatus {
    /// Validation was disabled by caller configuration.
    Disabled {
        /// Stable reason code explaining why validation is disabled.
        reason: ReportReasonCode,
    },
    /// Validation ran and passed.
    Passed,
    /// Validation ran and failed.
    Failed,
    /// Validation did not run because it was intentionally skipped.
    Skipped {
        /// Stable reason code explaining why validation was skipped.
        reason: ReportReasonCode,
    },
    /// Validation could not run because required evidence was unavailable.
    Unavailable {
        /// Stable reason code explaining why validation was unavailable.
        reason: ReportReasonCode,
    },
    /// Validation was required and could not pass.
    RequiredButFailed {
        /// Stable reason code explaining why required validation failed.
        reason: ReportReasonCode,
    },
}

impl ValidationStatus {
    /// Creates a disabled validation status.
    #[must_use]
    pub const fn disabled() -> Self {
        Self::Disabled {
            reason: ReportReasonCode::ValidationDisabled,
        }
    }

    /// Creates a passed validation status.
    #[must_use]
    pub const fn passed() -> Self {
        Self::Passed
    }

    /// Creates a failed validation status.
    #[must_use]
    pub const fn failed() -> Self {
        Self::Failed
    }

    /// Creates a skipped validation status with a stable reason code.
    #[must_use]
    pub const fn skipped(reason: ReportReasonCode) -> Self {
        Self::Skipped { reason }
    }

    /// Creates an unavailable validation status with a stable reason code.
    #[must_use]
    pub const fn unavailable(reason: ReportReasonCode) -> Self {
        Self::Unavailable { reason }
    }

    /// Creates a required-but-failed validation status with a stable reason code.
    #[must_use]
    pub const fn required_but_failed(reason: ReportReasonCode) -> Self {
        Self::RequiredButFailed { reason }
    }

    /// Returns the validation status classification.
    #[must_use]
    pub const fn kind(&self) -> ValidationStatusKind {
        match self {
            Self::Disabled { .. } => ValidationStatusKind::Disabled,
            Self::Passed => ValidationStatusKind::Passed,
            Self::Failed => ValidationStatusKind::Failed,
            Self::Skipped { .. } => ValidationStatusKind::Skipped,
            Self::Unavailable { .. } => ValidationStatusKind::Unavailable,
            Self::RequiredButFailed { .. } => ValidationStatusKind::RequiredButFailed,
        }
    }

    /// Returns the stable reason code when this status carries one.
    #[must_use]
    pub const fn reason(&self) -> Option<ReportReasonCode> {
        match self {
            Self::Disabled { reason }
            | Self::Skipped { reason }
            | Self::Unavailable { reason }
            | Self::RequiredButFailed { reason } => Some(*reason),
            Self::Passed | Self::Failed => None,
        }
    }

    /// Returns whether validation was disabled.
    #[must_use]
    pub const fn is_disabled(&self) -> bool {
        matches!(self, Self::Disabled { .. })
    }

    /// Returns whether validation passed.
    #[must_use]
    pub const fn is_passed(&self) -> bool {
        matches!(self, Self::Passed)
    }

    /// Returns whether validation failed after running.
    #[must_use]
    pub const fn is_failed(&self) -> bool {
        matches!(self, Self::Failed)
    }

    /// Returns whether validation was skipped.
    #[must_use]
    pub const fn is_skipped(&self) -> bool {
        matches!(self, Self::Skipped { .. })
    }

    /// Returns whether validation was unavailable.
    #[must_use]
    pub const fn is_unavailable(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }

    /// Returns whether required validation failed.
    #[must_use]
    pub const fn is_required_but_failed(&self) -> bool {
        matches!(self, Self::RequiredButFailed { .. })
    }

    /// Returns whether this status represents successful validation.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Passed)
    }
}

impl fmt::Display for ValidationStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.reason() {
            Some(reason) => write!(formatter, "{}:{reason}", self.kind()),
            None => formatter.write_str(self.kind().as_str()),
        }
    }
}

/// Classification for a workflow phase status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhaseStatusKind {
    /// The phase completed successfully.
    Completed,
    /// The phase failed.
    Failed,
    /// The phase was intentionally skipped.
    Skipped,
    /// The phase had not started.
    NotStarted,
    /// The phase status was unavailable.
    Unavailable,
}

impl PhaseStatusKind {
    /// Returns a stable lower-snake-case code for report serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::NotStarted => "not_started",
            Self::Unavailable => "unavailable",
        }
    }
}

impl fmt::Display for PhaseStatusKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Status for a workflow phase such as planning, loading, writing, or validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhaseStatus {
    /// The phase completed successfully.
    Completed,
    /// The phase failed.
    Failed,
    /// The phase was intentionally skipped.
    Skipped {
        /// Stable reason code explaining why the phase was skipped.
        reason: ReportReasonCode,
    },
    /// The phase had not started.
    NotStarted {
        /// Stable reason code explaining why the phase did not start.
        reason: ReportReasonCode,
    },
    /// The phase status was unavailable.
    Unavailable {
        /// Stable reason code explaining why the phase status was unavailable.
        reason: ReportReasonCode,
    },
}

impl PhaseStatus {
    /// Creates a completed phase status.
    #[must_use]
    pub const fn completed() -> Self {
        Self::Completed
    }

    /// Creates a failed phase status.
    #[must_use]
    pub const fn failed() -> Self {
        Self::Failed
    }

    /// Creates a skipped phase status with a stable reason code.
    #[must_use]
    pub const fn skipped(reason: ReportReasonCode) -> Self {
        Self::Skipped { reason }
    }

    /// Creates a not-started phase status with a stable reason code.
    #[must_use]
    pub const fn not_started(reason: ReportReasonCode) -> Self {
        Self::NotStarted { reason }
    }

    /// Creates an unavailable phase status with a stable reason code.
    #[must_use]
    pub const fn unavailable(reason: ReportReasonCode) -> Self {
        Self::Unavailable { reason }
    }

    /// Returns the phase status classification.
    #[must_use]
    pub const fn kind(&self) -> PhaseStatusKind {
        match self {
            Self::Completed => PhaseStatusKind::Completed,
            Self::Failed => PhaseStatusKind::Failed,
            Self::Skipped { .. } => PhaseStatusKind::Skipped,
            Self::NotStarted { .. } => PhaseStatusKind::NotStarted,
            Self::Unavailable { .. } => PhaseStatusKind::Unavailable,
        }
    }

    /// Returns the stable reason code when this status carries one.
    #[must_use]
    pub const fn reason(&self) -> Option<ReportReasonCode> {
        match self {
            Self::Skipped { reason }
            | Self::NotStarted { reason }
            | Self::Unavailable { reason } => Some(*reason),
            Self::Completed | Self::Failed => None,
        }
    }

    /// Returns whether the phase completed successfully.
    #[must_use]
    pub const fn is_completed(&self) -> bool {
        matches!(self, Self::Completed)
    }

    /// Returns whether the phase failed.
    #[must_use]
    pub const fn is_failed(&self) -> bool {
        matches!(self, Self::Failed)
    }

    /// Returns whether the phase was skipped.
    #[must_use]
    pub const fn is_skipped(&self) -> bool {
        matches!(self, Self::Skipped { .. })
    }

    /// Returns whether the phase had not started.
    #[must_use]
    pub const fn is_not_started(&self) -> bool {
        matches!(self, Self::NotStarted { .. })
    }

    /// Returns whether the phase status was unavailable.
    #[must_use]
    pub const fn is_unavailable(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }
}

impl fmt::Display for PhaseStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.reason() {
            Some(reason) => write!(formatter, "{}:{reason}", self.kind()),
            None => formatter.write_str(self.kind().as_str()),
        }
    }
}

/// Classification for an output-level workflow status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStatusKind {
    /// The output was planned but not executed yet.
    Planned,
    /// The output completed successfully.
    Succeeded,
    /// The output failed during planning, execution, or reporting.
    Failed,
    /// The output was intentionally skipped.
    Skipped,
    /// The output was planned as part of a dry run.
    DryRunPlanned,
    /// The output failed validation.
    ValidationFailed,
}

impl OutputStatusKind {
    /// Returns a stable lower-snake-case code for report serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::DryRunPlanned => "dry_run_planned",
            Self::ValidationFailed => "validation_failed",
        }
    }
}

impl fmt::Display for OutputStatusKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Output-level workflow status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStatus {
    /// The output was planned but not executed yet.
    Planned,
    /// The output completed successfully.
    Succeeded,
    /// The output failed during planning, execution, or reporting.
    Failed,
    /// The output was intentionally skipped.
    Skipped {
        /// Stable reason code explaining why the output was skipped.
        reason: ReportReasonCode,
    },
    /// The output was planned as part of a dry run.
    DryRunPlanned,
    /// The output failed validation.
    ValidationFailed {
        /// Validation status explaining the validation failure.
        validation: ValidationStatus,
    },
}

impl OutputStatus {
    /// Creates a planned output status.
    #[must_use]
    pub const fn planned() -> Self {
        Self::Planned
    }

    /// Creates a succeeded output status.
    #[must_use]
    pub const fn succeeded() -> Self {
        Self::Succeeded
    }

    /// Creates a failed output status.
    #[must_use]
    pub const fn failed() -> Self {
        Self::Failed
    }

    /// Creates a skipped output status with a stable reason code.
    #[must_use]
    pub const fn skipped(reason: ReportReasonCode) -> Self {
        Self::Skipped { reason }
    }

    /// Creates a dry-run-planned output status.
    #[must_use]
    pub const fn dry_run_planned() -> Self {
        Self::DryRunPlanned
    }

    /// Creates a validation-failed output status.
    #[must_use]
    pub const fn validation_failed(validation: ValidationStatus) -> Self {
        Self::ValidationFailed { validation }
    }

    /// Returns the output status classification.
    #[must_use]
    pub const fn kind(&self) -> OutputStatusKind {
        match self {
            Self::Planned => OutputStatusKind::Planned,
            Self::Succeeded => OutputStatusKind::Succeeded,
            Self::Failed => OutputStatusKind::Failed,
            Self::Skipped { .. } => OutputStatusKind::Skipped,
            Self::DryRunPlanned => OutputStatusKind::DryRunPlanned,
            Self::ValidationFailed { .. } => OutputStatusKind::ValidationFailed,
        }
    }

    /// Returns the stable reason code when this status carries one.
    #[must_use]
    pub const fn reason(&self) -> Option<ReportReasonCode> {
        match self {
            Self::Skipped { reason } => Some(*reason),
            Self::Planned
            | Self::Succeeded
            | Self::Failed
            | Self::DryRunPlanned
            | Self::ValidationFailed { .. } => None,
        }
    }

    /// Returns the validation status when this output failed validation.
    #[must_use]
    pub const fn validation(&self) -> Option<ValidationStatus> {
        match self {
            Self::ValidationFailed { validation } => Some(*validation),
            Self::Planned
            | Self::Succeeded
            | Self::Failed
            | Self::Skipped { .. }
            | Self::DryRunPlanned => None,
        }
    }

    /// Returns whether the output was planned but not executed yet.
    #[must_use]
    pub const fn is_planned(&self) -> bool {
        matches!(self, Self::Planned)
    }

    /// Returns whether the output succeeded.
    #[must_use]
    pub const fn is_succeeded(&self) -> bool {
        matches!(self, Self::Succeeded)
    }

    /// Returns whether the output failed.
    #[must_use]
    pub const fn is_failed(&self) -> bool {
        matches!(self, Self::Failed | Self::ValidationFailed { .. })
    }

    /// Returns whether the output was skipped.
    #[must_use]
    pub const fn is_skipped(&self) -> bool {
        matches!(self, Self::Skipped { .. })
    }

    /// Returns whether the output was planned as part of a dry run.
    #[must_use]
    pub const fn is_dry_run_planned(&self) -> bool {
        matches!(self, Self::DryRunPlanned)
    }

    /// Returns whether the output failed validation.
    #[must_use]
    pub const fn is_validation_failed(&self) -> bool {
        matches!(self, Self::ValidationFailed { .. })
    }
}

impl fmt::Display for OutputStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Skipped { reason } => write!(formatter, "{}:{reason}", self.kind()),
            Self::ValidationFailed { validation } => {
                write!(formatter, "{}:{validation}", self.kind())
            }
            Self::Planned | Self::Succeeded | Self::Failed | Self::DryRunPlanned => {
                formatter.write_str(self.kind().as_str())
            }
        }
    }
}

/// Classification for a workflow-level status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowStatusKind {
    /// The workflow completed successfully.
    Success,
    /// The workflow completed with at least one successful output and at least one non-success.
    PartialSuccess,
    /// The workflow failed without a complete successful result.
    Failure,
    /// The workflow was intentionally skipped.
    Skipped,
    /// The workflow had no work to perform.
    NoOp,
}

impl WorkflowStatusKind {
    /// Returns a stable lower-snake-case code for report serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::PartialSuccess => "partial_success",
            Self::Failure => "failure",
            Self::Skipped => "skipped",
            Self::NoOp => "no_op",
        }
    }
}

impl fmt::Display for WorkflowStatusKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Workflow-level status for a report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowStatus {
    /// The workflow completed successfully.
    Success,
    /// The workflow completed with at least one successful output and at least one non-success.
    PartialSuccess,
    /// The workflow failed without a complete successful result.
    Failure,
    /// The workflow was intentionally skipped.
    Skipped {
        /// Stable reason code explaining why the workflow was skipped.
        reason: ReportReasonCode,
    },
    /// The workflow had no work to perform.
    NoOp {
        /// Stable reason code explaining why the workflow had no work.
        reason: ReportReasonCode,
    },
}

impl WorkflowStatus {
    /// Creates a successful workflow status.
    #[must_use]
    pub const fn success() -> Self {
        Self::Success
    }

    /// Creates a partial-success workflow status.
    #[must_use]
    pub const fn partial_success() -> Self {
        Self::PartialSuccess
    }

    /// Creates a failed workflow status.
    #[must_use]
    pub const fn failure() -> Self {
        Self::Failure
    }

    /// Creates a skipped workflow status with a stable reason code.
    #[must_use]
    pub const fn skipped(reason: ReportReasonCode) -> Self {
        Self::Skipped { reason }
    }

    /// Creates a no-op workflow status with a stable reason code.
    #[must_use]
    pub const fn no_op(reason: ReportReasonCode) -> Self {
        Self::NoOp { reason }
    }

    /// Returns the workflow status classification.
    #[must_use]
    pub const fn kind(&self) -> WorkflowStatusKind {
        match self {
            Self::Success => WorkflowStatusKind::Success,
            Self::PartialSuccess => WorkflowStatusKind::PartialSuccess,
            Self::Failure => WorkflowStatusKind::Failure,
            Self::Skipped { .. } => WorkflowStatusKind::Skipped,
            Self::NoOp { .. } => WorkflowStatusKind::NoOp,
        }
    }

    /// Returns the stable reason code when this status carries one.
    #[must_use]
    pub const fn reason(&self) -> Option<ReportReasonCode> {
        match self {
            Self::Skipped { reason } | Self::NoOp { reason } => Some(*reason),
            Self::Success | Self::PartialSuccess | Self::Failure => None,
        }
    }

    /// Returns whether the workflow completed successfully.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }

    /// Returns whether the workflow partially succeeded.
    #[must_use]
    pub const fn is_partial_success(&self) -> bool {
        matches!(self, Self::PartialSuccess)
    }

    /// Returns whether the workflow failed.
    #[must_use]
    pub const fn is_failure(&self) -> bool {
        matches!(self, Self::Failure)
    }

    /// Returns whether the workflow was skipped.
    #[must_use]
    pub const fn is_skipped(&self) -> bool {
        matches!(self, Self::Skipped { .. })
    }

    /// Returns whether the workflow had no work to perform.
    #[must_use]
    pub const fn is_no_op(&self) -> bool {
        matches!(self, Self::NoOp { .. })
    }
}

impl fmt::Display for WorkflowStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.reason() {
            Some(reason) => write!(formatter, "{}:{reason}", self.kind()),
            None => formatter.write_str(self.kind().as_str()),
        }
    }
}

/// Validation and scan-summary options checked before workflow side effects.
///
/// This type carries validation intent without starting validation I/O. Target
/// row-count validation, source scan summaries, and related reports are wired in
/// later issue slices through these stable options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationOptions {
    target_validation_mode: TargetValidationMode,
    dry_run_scan_summary_mode: DryRunScanSummaryMode,
    require_successful_planning: bool,
}

impl Default for ValidationOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidationOptions {
    /// Creates default validation options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            target_validation_mode: TargetValidationMode::ValidateIfPossible,
            dry_run_scan_summary_mode: DryRunScanSummaryMode::MetadataOnly,
            require_successful_planning: true,
        }
    }

    /// Sets target-side validation behavior.
    #[must_use]
    pub const fn with_target_validation_mode(
        mut self,
        target_validation_mode: TargetValidationMode,
    ) -> Self {
        self.target_validation_mode = target_validation_mode;
        self
    }

    /// Sets dry-run source scan summary behavior.
    #[must_use]
    pub const fn with_dry_run_scan_summary_mode(
        mut self,
        dry_run_scan_summary_mode: DryRunScanSummaryMode,
    ) -> Self {
        self.dry_run_scan_summary_mode = dry_run_scan_summary_mode;
        self
    }

    /// Sets whether local planning failures should be terminal.
    #[must_use]
    pub const fn with_require_successful_planning(
        mut self,
        require_successful_planning: bool,
    ) -> Self {
        self.require_successful_planning = require_successful_planning;
        self
    }

    /// Returns target-side validation behavior.
    #[must_use]
    pub const fn target_validation_mode(&self) -> TargetValidationMode {
        self.target_validation_mode
    }

    /// Returns dry-run source scan summary behavior.
    #[must_use]
    pub const fn dry_run_scan_summary_mode(&self) -> DryRunScanSummaryMode {
        self.dry_run_scan_summary_mode
    }

    /// Returns whether local planning failures should be terminal.
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
        let _ = self.target_validation_mode;
        let _ = self.dry_run_scan_summary_mode;
        let _ = self.require_successful_planning;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DryRunScanSummaryMode, FileCount, FileCountKind, OutputStatus, OutputStatusKind,
        PhaseStatus, PhaseStatusKind, ReportReasonCode, RowCount, RowCountKind,
        TargetValidationMode, ValidationOptions, ValidationStatus, ValidationStatusKind,
        WorkflowStatus, WorkflowStatusKind, u128_to_u64_saturating,
    };

    #[test]
    fn target_validation_modes_expose_stable_codes() {
        assert_eq!(TargetValidationMode::Disabled.as_str(), "disabled");
        assert_eq!(
            TargetValidationMode::ValidateIfPossible.as_str(),
            "validate_if_possible"
        );
        assert_eq!(TargetValidationMode::Require.as_str(), "require");
        assert_eq!(
            TargetValidationMode::ValidateIfPossible.to_string(),
            "validate_if_possible"
        );
    }

    #[test]
    fn dry_run_scan_summary_modes_expose_stable_codes() {
        assert_eq!(
            DryRunScanSummaryMode::MetadataOnly.as_str(),
            "metadata_only"
        );
        assert_eq!(
            DryRunScanSummaryMode::ExhaustScanMetadata.as_str(),
            "exhaust_scan_metadata"
        );
        assert_eq!(
            DryRunScanSummaryMode::ExhaustScanMetadata.to_string(),
            "exhaust_scan_metadata"
        );
    }

    #[test]
    fn validation_options_default_preserves_planning_intent() {
        let options = ValidationOptions::default();

        assert_eq!(
            options.target_validation_mode(),
            TargetValidationMode::ValidateIfPossible
        );
        assert_eq!(
            options.dry_run_scan_summary_mode(),
            DryRunScanSummaryMode::MetadataOnly
        );
        assert!(options.require_successful_planning());
        assert!(options.validate().is_ok());
    }

    #[test]
    fn validation_options_accessors_return_configured_values() {
        let options = ValidationOptions::new()
            .with_target_validation_mode(TargetValidationMode::Require)
            .with_dry_run_scan_summary_mode(DryRunScanSummaryMode::ExhaustScanMetadata)
            .with_require_successful_planning(false);

        assert_eq!(
            options.target_validation_mode(),
            TargetValidationMode::Require
        );
        assert_eq!(
            options.dry_run_scan_summary_mode(),
            DryRunScanSummaryMode::ExhaustScanMetadata
        );
        assert!(!options.require_successful_planning());
    }

    #[test]
    fn validation_options_debug_does_not_include_external_values() {
        let options = ValidationOptions::new()
            .with_target_validation_mode(TargetValidationMode::Disabled)
            .with_dry_run_scan_summary_mode(DryRunScanSummaryMode::MetadataOnly);

        let debug = format!("{options:?}");

        assert!(debug.contains("Disabled"));
        assert!(debug.contains("MetadataOnly"));
        assert!(!debug.contains("server="));
        assert!(!debug.contains("password"));
    }

    #[test]
    fn row_count_variants_expose_kind_and_values() {
        let exact = RowCount::exact(10);
        let estimated = RowCount::estimated(20);
        let partial = RowCount::partial(30);
        let unavailable = RowCount::unavailable();

        assert_eq!(exact.kind(), RowCountKind::Exact);
        assert_eq!(exact.value(), Some(10));
        assert_eq!(exact.exact_value(), Some(10));
        assert!(exact.is_exact());

        assert_eq!(estimated.kind(), RowCountKind::Estimated);
        assert_eq!(estimated.value(), Some(20));
        assert_eq!(estimated.estimated_value(), Some(20));
        assert!(estimated.is_estimated());

        assert_eq!(partial.kind(), RowCountKind::Partial);
        assert_eq!(partial.value(), Some(30));
        assert_eq!(partial.partial_value(), Some(30));
        assert_eq!(partial.exact_value(), None);
        assert!(partial.is_partial());

        assert_eq!(unavailable.kind(), RowCountKind::Unavailable);
        assert_eq!(unavailable.value(), None);
        assert!(unavailable.is_unavailable());
    }

    #[test]
    fn file_count_variants_expose_kind_and_values() {
        let exact = FileCount::exact(2);
        let estimated = FileCount::estimated(3);
        let unavailable = FileCount::unavailable();
        let skipped = FileCount::skipped();
        let not_executed = FileCount::not_executed();

        assert_eq!(exact.kind(), FileCountKind::Exact);
        assert_eq!(exact.value(), Some(2));
        assert_eq!(exact.exact_value(), Some(2));
        assert!(exact.is_exact());

        assert_eq!(estimated.kind(), FileCountKind::Estimated);
        assert_eq!(estimated.value(), Some(3));
        assert_eq!(estimated.estimated_value(), Some(3));
        assert!(estimated.is_estimated());

        assert_eq!(unavailable.kind(), FileCountKind::Unavailable);
        assert_eq!(unavailable.value(), None);
        assert!(unavailable.is_unavailable());

        assert_eq!(skipped.kind(), FileCountKind::Skipped);
        assert_eq!(skipped.value(), None);
        assert!(skipped.is_skipped());

        assert_eq!(not_executed.kind(), FileCountKind::NotExecuted);
        assert_eq!(not_executed.value(), None);
        assert!(not_executed.is_not_executed());
    }

    #[test]
    fn count_kinds_expose_stable_codes() {
        assert_eq!(RowCountKind::Exact.as_str(), "exact");
        assert_eq!(RowCountKind::Estimated.as_str(), "estimated");
        assert_eq!(RowCountKind::Partial.as_str(), "partial");
        assert_eq!(RowCountKind::Unavailable.as_str(), "unavailable");
        assert_eq!(RowCountKind::Partial.to_string(), "partial");

        assert_eq!(FileCountKind::Exact.as_str(), "exact");
        assert_eq!(FileCountKind::Estimated.as_str(), "estimated");
        assert_eq!(FileCountKind::Unavailable.as_str(), "unavailable");
        assert_eq!(FileCountKind::Skipped.as_str(), "skipped");
        assert_eq!(FileCountKind::NotExecuted.as_str(), "not_executed");
        assert_eq!(FileCountKind::NotExecuted.to_string(), "not_executed");
    }

    #[test]
    fn count_display_is_stable_and_safe() {
        assert_eq!(RowCount::exact(12).to_string(), "exact:12");
        assert_eq!(RowCount::estimated(34).to_string(), "estimated:34");
        assert_eq!(RowCount::partial(56).to_string(), "partial:56");
        assert_eq!(RowCount::unavailable().to_string(), "unavailable");

        assert_eq!(FileCount::exact(7).to_string(), "exact:7");
        assert_eq!(FileCount::estimated(8).to_string(), "estimated:8");
        assert_eq!(FileCount::unavailable().to_string(), "unavailable");
        assert_eq!(FileCount::skipped().to_string(), "skipped");
        assert_eq!(FileCount::not_executed().to_string(), "not_executed");

        let debug = format!("{:?} {:?}", RowCount::partial(1), FileCount::not_executed());
        assert!(!debug.contains("server="));
        assert!(!debug.contains("password"));
    }

    #[test]
    fn count_constructors_saturate_wide_values() {
        assert_eq!(u128_to_u64_saturating(u128::from(u64::MAX) + 1), u64::MAX);
        assert_eq!(RowCount::exact_from_u128(u128::MAX).value(), Some(u64::MAX));
        assert_eq!(
            RowCount::estimated_from_u128(u128::MAX).value(),
            Some(u64::MAX)
        );
        assert_eq!(
            RowCount::partial_from_u128(u128::MAX).value(),
            Some(u64::MAX)
        );
        assert_eq!(
            FileCount::exact_from_u128(u128::MAX).value(),
            Some(u64::MAX)
        );
        assert_eq!(
            FileCount::estimated_from_u128(u128::MAX).value(),
            Some(u64::MAX)
        );

        assert_eq!(RowCount::exact_from_usize(5).value(), Some(5));
        assert_eq!(RowCount::estimated_from_usize(6).value(), Some(6));
        assert_eq!(RowCount::partial_from_usize(7).value(), Some(7));
        assert_eq!(FileCount::exact_from_usize(8).value(), Some(8));
        assert_eq!(FileCount::estimated_from_usize(9).value(), Some(9));
    }

    #[test]
    fn report_reason_codes_cover_stable_skip_and_unavailable_reasons() {
        let cases = [
            (ReportReasonCode::ValidationDisabled, "validation_disabled"),
            (ReportReasonCode::DryRun, "dry_run"),
            (
                ReportReasonCode::CapabilityUnavailable,
                "capability_unavailable",
            ),
            (
                ReportReasonCode::PermissionUnavailable,
                "permission_unavailable",
            ),
            (ReportReasonCode::PriorFailure, "prior_failure"),
            (
                ReportReasonCode::UnsupportedLoadMode,
                "unsupported_load_mode",
            ),
            (
                ReportReasonCode::MissingTargetAccess,
                "missing_target_access",
            ),
            (
                ReportReasonCode::MissingExactOutputRows,
                "missing_exact_output_rows",
            ),
            (ReportReasonCode::CostAvoidance, "cost_avoidance"),
            (ReportReasonCode::NotExecuted, "not_executed"),
            (
                ReportReasonCode::FailureBeforeValidation,
                "failure_before_validation",
            ),
        ];

        for (reason, code) in cases {
            assert_eq!(reason.as_str(), code);
            assert_eq!(reason.to_string(), code);
        }
    }

    #[test]
    fn report_reason_debug_does_not_include_external_values() {
        let debug = format!("{:?}", ReportReasonCode::MissingTargetAccess);

        assert!(debug.contains("MissingTargetAccess"));
        assert!(!debug.contains("server="));
        assert!(!debug.contains("password"));
    }

    #[test]
    fn validation_status_kinds_expose_stable_codes() {
        assert_eq!(ValidationStatusKind::Disabled.as_str(), "disabled");
        assert_eq!(ValidationStatusKind::Passed.as_str(), "passed");
        assert_eq!(ValidationStatusKind::Failed.as_str(), "failed");
        assert_eq!(ValidationStatusKind::Skipped.as_str(), "skipped");
        assert_eq!(ValidationStatusKind::Unavailable.as_str(), "unavailable");
        assert_eq!(
            ValidationStatusKind::RequiredButFailed.as_str(),
            "required_but_failed"
        );
        assert_eq!(
            ValidationStatusKind::RequiredButFailed.to_string(),
            "required_but_failed"
        );
    }

    #[test]
    fn validation_status_variants_expose_kind_reasons_and_helpers() {
        let disabled = ValidationStatus::disabled();
        let passed = ValidationStatus::passed();
        let failed = ValidationStatus::failed();
        let skipped = ValidationStatus::skipped(ReportReasonCode::DryRun);
        let unavailable = ValidationStatus::unavailable(ReportReasonCode::MissingTargetAccess);
        let required =
            ValidationStatus::required_but_failed(ReportReasonCode::MissingExactOutputRows);

        assert_eq!(disabled.kind(), ValidationStatusKind::Disabled);
        assert_eq!(
            disabled.reason(),
            Some(ReportReasonCode::ValidationDisabled)
        );
        assert!(disabled.is_disabled());

        assert_eq!(passed.kind(), ValidationStatusKind::Passed);
        assert_eq!(passed.reason(), None);
        assert!(passed.is_passed());
        assert!(passed.is_success());

        assert_eq!(failed.kind(), ValidationStatusKind::Failed);
        assert_eq!(failed.reason(), None);
        assert!(failed.is_failed());
        assert!(!failed.is_success());

        assert_eq!(skipped.kind(), ValidationStatusKind::Skipped);
        assert_eq!(skipped.reason(), Some(ReportReasonCode::DryRun));
        assert!(skipped.is_skipped());

        assert_eq!(unavailable.kind(), ValidationStatusKind::Unavailable);
        assert_eq!(
            unavailable.reason(),
            Some(ReportReasonCode::MissingTargetAccess)
        );
        assert!(unavailable.is_unavailable());

        assert_eq!(required.kind(), ValidationStatusKind::RequiredButFailed);
        assert_eq!(
            required.reason(),
            Some(ReportReasonCode::MissingExactOutputRows)
        );
        assert!(required.is_required_but_failed());
    }

    #[test]
    fn validation_status_display_is_stable_and_safe() {
        assert_eq!(
            ValidationStatus::disabled().to_string(),
            "disabled:validation_disabled"
        );
        assert_eq!(ValidationStatus::passed().to_string(), "passed");
        assert_eq!(ValidationStatus::failed().to_string(), "failed");
        assert_eq!(
            ValidationStatus::skipped(ReportReasonCode::DryRun).to_string(),
            "skipped:dry_run"
        );
        assert_eq!(
            ValidationStatus::unavailable(ReportReasonCode::PermissionUnavailable).to_string(),
            "unavailable:permission_unavailable"
        );
        assert_eq!(
            ValidationStatus::required_but_failed(ReportReasonCode::MissingExactOutputRows)
                .to_string(),
            "required_but_failed:missing_exact_output_rows"
        );

        let debug = format!(
            "{:?}",
            ValidationStatus::required_but_failed(ReportReasonCode::MissingTargetAccess)
        );
        assert!(debug.contains("RequiredButFailed"));
        assert!(!debug.contains("server="));
        assert!(!debug.contains("password"));
    }

    #[test]
    fn phase_status_kinds_expose_stable_codes() {
        assert_eq!(PhaseStatusKind::Completed.as_str(), "completed");
        assert_eq!(PhaseStatusKind::Failed.as_str(), "failed");
        assert_eq!(PhaseStatusKind::Skipped.as_str(), "skipped");
        assert_eq!(PhaseStatusKind::NotStarted.as_str(), "not_started");
        assert_eq!(PhaseStatusKind::Unavailable.as_str(), "unavailable");
        assert_eq!(PhaseStatusKind::NotStarted.to_string(), "not_started");
    }

    #[test]
    fn phase_status_variants_expose_kind_reasons_and_helpers() {
        let completed = PhaseStatus::completed();
        let failed = PhaseStatus::failed();
        let skipped = PhaseStatus::skipped(ReportReasonCode::DryRun);
        let not_started = PhaseStatus::not_started(ReportReasonCode::PriorFailure);
        let unavailable = PhaseStatus::unavailable(ReportReasonCode::CapabilityUnavailable);

        assert_eq!(completed.kind(), PhaseStatusKind::Completed);
        assert_eq!(completed.reason(), None);
        assert!(completed.is_completed());

        assert_eq!(failed.kind(), PhaseStatusKind::Failed);
        assert_eq!(failed.reason(), None);
        assert!(failed.is_failed());

        assert_eq!(skipped.kind(), PhaseStatusKind::Skipped);
        assert_eq!(skipped.reason(), Some(ReportReasonCode::DryRun));
        assert!(skipped.is_skipped());

        assert_eq!(not_started.kind(), PhaseStatusKind::NotStarted);
        assert_eq!(not_started.reason(), Some(ReportReasonCode::PriorFailure));
        assert!(not_started.is_not_started());

        assert_eq!(unavailable.kind(), PhaseStatusKind::Unavailable);
        assert_eq!(
            unavailable.reason(),
            Some(ReportReasonCode::CapabilityUnavailable)
        );
        assert!(unavailable.is_unavailable());
    }

    #[test]
    fn phase_status_display_is_stable_and_safe() {
        assert_eq!(PhaseStatus::completed().to_string(), "completed");
        assert_eq!(PhaseStatus::failed().to_string(), "failed");
        assert_eq!(
            PhaseStatus::skipped(ReportReasonCode::DryRun).to_string(),
            "skipped:dry_run"
        );
        assert_eq!(
            PhaseStatus::not_started(ReportReasonCode::PriorFailure).to_string(),
            "not_started:prior_failure"
        );
        assert_eq!(
            PhaseStatus::unavailable(ReportReasonCode::CapabilityUnavailable).to_string(),
            "unavailable:capability_unavailable"
        );

        let debug = format!(
            "{:?}",
            PhaseStatus::unavailable(ReportReasonCode::MissingTargetAccess)
        );
        assert!(debug.contains("Unavailable"));
        assert!(!debug.contains("server="));
        assert!(!debug.contains("password"));
    }

    #[test]
    fn output_status_kinds_expose_stable_codes() {
        assert_eq!(OutputStatusKind::Planned.as_str(), "planned");
        assert_eq!(OutputStatusKind::Succeeded.as_str(), "succeeded");
        assert_eq!(OutputStatusKind::Failed.as_str(), "failed");
        assert_eq!(OutputStatusKind::Skipped.as_str(), "skipped");
        assert_eq!(OutputStatusKind::DryRunPlanned.as_str(), "dry_run_planned");
        assert_eq!(
            OutputStatusKind::ValidationFailed.as_str(),
            "validation_failed"
        );
        assert_eq!(
            OutputStatusKind::DryRunPlanned.to_string(),
            "dry_run_planned"
        );
    }

    #[test]
    fn output_status_variants_expose_kind_reasons_validation_and_helpers() {
        let planned = OutputStatus::planned();
        let succeeded = OutputStatus::succeeded();
        let failed = OutputStatus::failed();
        let skipped = OutputStatus::skipped(ReportReasonCode::PriorFailure);
        let dry_run = OutputStatus::dry_run_planned();
        let validation =
            ValidationStatus::required_but_failed(ReportReasonCode::MissingExactOutputRows);
        let validation_failed = OutputStatus::validation_failed(validation);

        assert_eq!(planned.kind(), OutputStatusKind::Planned);
        assert_eq!(planned.reason(), None);
        assert!(planned.is_planned());

        assert_eq!(succeeded.kind(), OutputStatusKind::Succeeded);
        assert!(succeeded.is_succeeded());

        assert_eq!(failed.kind(), OutputStatusKind::Failed);
        assert!(failed.is_failed());
        assert!(!failed.is_validation_failed());

        assert_eq!(skipped.kind(), OutputStatusKind::Skipped);
        assert_eq!(skipped.reason(), Some(ReportReasonCode::PriorFailure));
        assert!(skipped.is_skipped());

        assert_eq!(dry_run.kind(), OutputStatusKind::DryRunPlanned);
        assert!(dry_run.is_dry_run_planned());

        assert_eq!(validation_failed.kind(), OutputStatusKind::ValidationFailed);
        assert_eq!(validation_failed.validation(), Some(validation));
        assert!(validation_failed.is_failed());
        assert!(validation_failed.is_validation_failed());
    }

    #[test]
    fn output_status_display_is_stable_and_safe() {
        assert_eq!(OutputStatus::planned().to_string(), "planned");
        assert_eq!(OutputStatus::succeeded().to_string(), "succeeded");
        assert_eq!(OutputStatus::failed().to_string(), "failed");
        assert_eq!(
            OutputStatus::skipped(ReportReasonCode::PriorFailure).to_string(),
            "skipped:prior_failure"
        );
        assert_eq!(
            OutputStatus::dry_run_planned().to_string(),
            "dry_run_planned"
        );
        assert_eq!(
            OutputStatus::validation_failed(ValidationStatus::required_but_failed(
                ReportReasonCode::MissingExactOutputRows
            ))
            .to_string(),
            "validation_failed:required_but_failed:missing_exact_output_rows"
        );

        let debug = format!(
            "{:?}",
            OutputStatus::validation_failed(ValidationStatus::unavailable(
                ReportReasonCode::MissingTargetAccess
            ))
        );
        assert!(debug.contains("ValidationFailed"));
        assert!(!debug.contains("server="));
        assert!(!debug.contains("password"));
    }

    #[test]
    fn workflow_status_kinds_expose_stable_codes() {
        assert_eq!(WorkflowStatusKind::Success.as_str(), "success");
        assert_eq!(
            WorkflowStatusKind::PartialSuccess.as_str(),
            "partial_success"
        );
        assert_eq!(WorkflowStatusKind::Failure.as_str(), "failure");
        assert_eq!(WorkflowStatusKind::Skipped.as_str(), "skipped");
        assert_eq!(WorkflowStatusKind::NoOp.as_str(), "no_op");
        assert_eq!(
            WorkflowStatusKind::PartialSuccess.to_string(),
            "partial_success"
        );
    }

    #[test]
    fn workflow_status_variants_expose_kind_reasons_and_helpers() {
        let success = WorkflowStatus::success();
        let partial = WorkflowStatus::partial_success();
        let failure = WorkflowStatus::failure();
        let skipped = WorkflowStatus::skipped(ReportReasonCode::DryRun);
        let no_op = WorkflowStatus::no_op(ReportReasonCode::NotExecuted);

        assert_eq!(success.kind(), WorkflowStatusKind::Success);
        assert_eq!(success.reason(), None);
        assert!(success.is_success());

        assert_eq!(partial.kind(), WorkflowStatusKind::PartialSuccess);
        assert_eq!(partial.reason(), None);
        assert!(partial.is_partial_success());

        assert_eq!(failure.kind(), WorkflowStatusKind::Failure);
        assert_eq!(failure.reason(), None);
        assert!(failure.is_failure());

        assert_eq!(skipped.kind(), WorkflowStatusKind::Skipped);
        assert_eq!(skipped.reason(), Some(ReportReasonCode::DryRun));
        assert!(skipped.is_skipped());

        assert_eq!(no_op.kind(), WorkflowStatusKind::NoOp);
        assert_eq!(no_op.reason(), Some(ReportReasonCode::NotExecuted));
        assert!(no_op.is_no_op());
    }

    #[test]
    fn workflow_status_display_is_stable_and_safe() {
        assert_eq!(WorkflowStatus::success().to_string(), "success");
        assert_eq!(
            WorkflowStatus::partial_success().to_string(),
            "partial_success"
        );
        assert_eq!(WorkflowStatus::failure().to_string(), "failure");
        assert_eq!(
            WorkflowStatus::skipped(ReportReasonCode::DryRun).to_string(),
            "skipped:dry_run"
        );
        assert_eq!(
            WorkflowStatus::no_op(ReportReasonCode::NotExecuted).to_string(),
            "no_op:not_executed"
        );

        let debug = format!(
            "{:?}",
            WorkflowStatus::skipped(ReportReasonCode::PriorFailure)
        );
        assert!(debug.contains("Skipped"));
        assert!(!debug.contains("server="));
        assert!(!debug.contains("password"));
    }
}
