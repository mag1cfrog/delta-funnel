//! Shared report primitives for query-load readiness.
//!
//! This module owns the vocabulary that later dry-run, execution, validation,
//! timing, tracing, Rust, and Python report slices reuse. The types are kept
//! serializable-friendly by exposing stable string codes and primitive values
//! without adding a serialization dependency in this slice.

use std::fmt;

use crate::error::DeltaFunnelError;

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
    use super::{DryRunScanSummaryMode, TargetValidationMode, ValidationOptions};

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
}
