//! Default target derivation for Delta scan file task partitions.
//!
//! The policy is intentionally conservative and CPU-oriented. Stable execution
//! configuration wins first, and machine-derived parallelism is only a bounded
//! fallback so large machines do not create explosive partition counts by
//! default. Unit tests inject machine context instead of reading host state.

use crate::{DeltaFunnelError, error::DeltaScanFileTaskPartitionPlanningSnafu};

use super::file_task_partition::DeltaScanFileTaskPartitionOptions;

const DEFAULT_MIN_PARTITIONS: usize = 4;
const DEFAULT_MAX_PARTITIONS: usize = 64;
const DEFAULT_PARALLELISM_MULTIPLIER: usize = 1;

/// Provider scan context used to report target-derivation failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct DeltaScanPartitionTargetContext<'a> {
    /// DataFusion table name for this source.
    pub(crate) source_name: &'a str,
    /// Normalized Delta table URI for this source.
    pub(crate) table_uri: &'a str,
    /// Resolved Delta snapshot version.
    pub(crate) snapshot_version: u64,
}

/// Inputs used to derive the scan file task partition target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct DeltaScanPartitionTargetConfig {
    /// User-provided DeltaFunnel override, if configured.
    pub(crate) explicit_target_partitions: Option<usize>,
    /// DataFusion execution target partition count, if available.
    pub(crate) datafusion_target_partitions: Option<usize>,
    /// Host parallelism observed by the caller, injected for deterministic tests.
    pub(crate) available_parallelism: Option<usize>,
}

/// Conservative fallback policy for deriving scan partition targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct DeltaScanPartitionTargetPolicy {
    /// Lower bound for machine-derived fallback targets.
    pub(crate) min_default_partitions: usize,
    /// Upper bound for machine-derived fallback targets.
    pub(crate) max_default_partitions: usize,
    /// Multiplier applied to available parallelism before clamping.
    pub(crate) parallelism_multiplier: usize,
}

/// Source that selected the final scan partition target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum DeltaScanPartitionTargetSource {
    /// User-provided DeltaFunnel override selected the target.
    ExplicitOverride,
    /// DataFusion execution configuration selected the target.
    DataFusionConfig,
    /// Bounded host parallelism fallback selected the target.
    AvailableParallelismFallback,
    /// Static fallback selected the target because host parallelism was unavailable.
    StaticFallback,
}

/// Derived scan partition target plus diagnostic inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct DeltaScanPartitionTargetDecision {
    /// Final target partition count passed to file task grouping.
    pub(crate) target_partitions: usize,
    /// Input source that selected the final target.
    pub(crate) source: DeltaScanPartitionTargetSource,
    /// Original user override input.
    pub(crate) explicit_target_partitions: Option<usize>,
    /// Original DataFusion execution target input.
    pub(crate) datafusion_target_partitions: Option<usize>,
    /// Original host parallelism input.
    pub(crate) available_parallelism: Option<usize>,
    /// Policy used for fallback target derivation.
    pub(crate) policy: DeltaScanPartitionTargetPolicy,
}

impl Default for DeltaScanPartitionTargetPolicy {
    fn default() -> Self {
        Self {
            min_default_partitions: DEFAULT_MIN_PARTITIONS,
            max_default_partitions: DEFAULT_MAX_PARTITIONS,
            parallelism_multiplier: DEFAULT_PARALLELISM_MULTIPLIER,
        }
    }
}

impl DeltaScanPartitionTargetConfig {
    /// Builds config from DataFusion execution state and local host parallelism.
    #[allow(dead_code)]
    pub(crate) fn from_datafusion_target(datafusion_target_partitions: usize) -> Self {
        Self {
            explicit_target_partitions: None,
            datafusion_target_partitions: Some(datafusion_target_partitions),
            available_parallelism: local_available_parallelism(),
        }
    }
}

impl DeltaScanPartitionTargetPolicy {
    /// Derives the target partition count for one provider scan.
    #[allow(dead_code)]
    pub(crate) fn derive_target(
        self,
        context: DeltaScanPartitionTargetContext<'_>,
        config: DeltaScanPartitionTargetConfig,
    ) -> Result<DeltaScanPartitionTargetDecision, DeltaFunnelError> {
        self.validate(context)?;

        if let Some(target_partitions) = config.explicit_target_partitions {
            self.validate_target(context, "explicit target_partitions", target_partitions)?;
            return Ok(self.decision(
                DeltaScanPartitionTargetSource::ExplicitOverride,
                target_partitions,
                config,
            ));
        }

        if let Some(target_partitions) = config.datafusion_target_partitions {
            self.validate_target(context, "DataFusion target_partitions", target_partitions)?;
            return Ok(self.decision(
                DeltaScanPartitionTargetSource::DataFusionConfig,
                target_partitions,
                config,
            ));
        }

        let (source, target_partitions) = match config.available_parallelism {
            Some(available_parallelism) => {
                self.validate_target(context, "available_parallelism", available_parallelism)?;
                let multiplied = available_parallelism
                    .saturating_mul(self.parallelism_multiplier)
                    .clamp(self.min_default_partitions, self.max_default_partitions);

                (
                    DeltaScanPartitionTargetSource::AvailableParallelismFallback,
                    multiplied,
                )
            }
            None => (
                DeltaScanPartitionTargetSource::StaticFallback,
                self.min_default_partitions,
            ),
        };

        Ok(self.decision(source, target_partitions, config))
    }

    fn validate(
        self,
        context: DeltaScanPartitionTargetContext<'_>,
    ) -> Result<(), DeltaFunnelError> {
        if self.min_default_partitions == 0 {
            return target_planning_error(
                context,
                "min_default_partitions must be greater than zero",
            );
        }
        if self.max_default_partitions < self.min_default_partitions {
            return target_planning_error(
                context,
                "max_default_partitions must be greater than or equal to min_default_partitions",
            );
        }
        if self.parallelism_multiplier == 0 {
            return target_planning_error(
                context,
                "parallelism_multiplier must be greater than zero",
            );
        }

        Ok(())
    }

    fn validate_target(
        self,
        context: DeltaScanPartitionTargetContext<'_>,
        target_name: &str,
        target_partitions: usize,
    ) -> Result<(), DeltaFunnelError> {
        if target_partitions == 0 {
            return target_planning_error(
                context,
                format!("{target_name} must be greater than zero"),
            );
        }

        Ok(())
    }

    fn decision(
        self,
        source: DeltaScanPartitionTargetSource,
        target_partitions: usize,
        config: DeltaScanPartitionTargetConfig,
    ) -> DeltaScanPartitionTargetDecision {
        DeltaScanPartitionTargetDecision {
            target_partitions,
            source,
            explicit_target_partitions: config.explicit_target_partitions,
            datafusion_target_partitions: config.datafusion_target_partitions,
            available_parallelism: config.available_parallelism,
            policy: self,
        }
    }
}

impl DeltaScanPartitionTargetDecision {
    /// Builds grouping options while preserving diagnostics on this decision.
    #[allow(dead_code)]
    pub(crate) fn file_task_partition_options(&self) -> DeltaScanFileTaskPartitionOptions {
        DeltaScanFileTaskPartitionOptions {
            target_partitions: self.target_partitions,
        }
    }
}

fn local_available_parallelism() -> Option<usize> {
    std::thread::available_parallelism()
        .ok()
        .map(std::num::NonZeroUsize::get)
}

fn target_planning_error<T>(
    context: DeltaScanPartitionTargetContext<'_>,
    reason: impl Into<String>,
) -> Result<T, DeltaFunnelError> {
    DeltaScanFileTaskPartitionPlanningSnafu {
        source_name: context.source_name.to_owned(),
        table_uri: context.table_uri.to_owned(),
        snapshot_version: context.snapshot_version,
        reason: reason.into(),
    }
    .fail()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> DeltaScanPartitionTargetContext<'static> {
        DeltaScanPartitionTargetContext {
            source_name: "orders",
            table_uri: "file:///tmp/table",
            snapshot_version: 42,
        }
    }

    fn config(
        explicit_target_partitions: Option<usize>,
        datafusion_target_partitions: Option<usize>,
        available_parallelism: Option<usize>,
    ) -> DeltaScanPartitionTargetConfig {
        DeltaScanPartitionTargetConfig {
            explicit_target_partitions,
            datafusion_target_partitions,
            available_parallelism,
        }
    }

    fn test_policy() -> DeltaScanPartitionTargetPolicy {
        DeltaScanPartitionTargetPolicy {
            min_default_partitions: 4,
            max_default_partitions: 64,
            parallelism_multiplier: 1,
        }
    }

    #[test]
    fn explicit_target_wins_over_datafusion_and_available_parallelism()
    -> Result<(), Box<dyn std::error::Error>> {
        let decision =
            test_policy().derive_target(context(), config(Some(12), Some(8), Some(4)))?;

        assert_eq!(decision.target_partitions, 12);
        assert_eq!(
            decision.source,
            DeltaScanPartitionTargetSource::ExplicitOverride
        );
        assert_eq!(decision.explicit_target_partitions, Some(12));
        assert_eq!(decision.datafusion_target_partitions, Some(8));
        assert_eq!(decision.available_parallelism, Some(4));
        assert_eq!(decision.file_task_partition_options().target_partitions, 12);

        Ok(())
    }

    #[test]
    fn explicit_zero_target_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let error = test_policy()
            .derive_target(context(), config(Some(0), Some(8), Some(4)))
            .err()
            .ok_or("expected explicit zero target to fail")?;

        assert!(matches!(
            error,
            DeltaFunnelError::DeltaScanFileTaskPartitionPlanning { .. }
        ));
        assert!(error.to_string().contains("explicit target_partitions"));
        assert!(error.to_string().contains("greater than zero"));

        Ok(())
    }

    #[test]
    fn datafusion_target_wins_when_no_explicit_override() -> Result<(), Box<dyn std::error::Error>>
    {
        let decision = test_policy().derive_target(context(), config(None, Some(8), Some(4)))?;

        assert_eq!(decision.target_partitions, 8);
        assert_eq!(
            decision.source,
            DeltaScanPartitionTargetSource::DataFusionConfig
        );

        Ok(())
    }

    #[test]
    fn datafusion_zero_target_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let error = test_policy()
            .derive_target(context(), config(None, Some(0), Some(4)))
            .err()
            .ok_or("expected DataFusion zero target to fail")?;

        assert!(matches!(
            error,
            DeltaFunnelError::DeltaScanFileTaskPartitionPlanning { .. }
        ));
        assert!(error.to_string().contains("DataFusion target_partitions"));
        assert!(error.to_string().contains("greater than zero"));

        Ok(())
    }

    #[test]
    fn available_parallelism_fallback_is_clamped_by_policy_bounds()
    -> Result<(), Box<dyn std::error::Error>> {
        let low = test_policy().derive_target(context(), config(None, None, Some(1)))?;
        let high = test_policy().derive_target(context(), config(None, None, Some(512)))?;

        assert_eq!(low.target_partitions, 4);
        assert_eq!(
            low.source,
            DeltaScanPartitionTargetSource::AvailableParallelismFallback
        );
        assert_eq!(high.target_partitions, 64);
        assert_eq!(
            high.source,
            DeltaScanPartitionTargetSource::AvailableParallelismFallback
        );

        Ok(())
    }

    #[test]
    fn available_parallelism_fallback_applies_policy_multiplier()
    -> Result<(), Box<dyn std::error::Error>> {
        let policy = DeltaScanPartitionTargetPolicy {
            min_default_partitions: 4,
            max_default_partitions: 64,
            parallelism_multiplier: 2,
        };

        let decision = policy.derive_target(context(), config(None, None, Some(8)))?;

        assert_eq!(decision.target_partitions, 16);
        assert_eq!(
            decision.source,
            DeltaScanPartitionTargetSource::AvailableParallelismFallback
        );

        Ok(())
    }

    #[test]
    fn missing_available_parallelism_uses_static_nonzero_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let decision = test_policy().derive_target(context(), config(None, None, None))?;

        assert_eq!(decision.target_partitions, 4);
        assert_eq!(
            decision.source,
            DeltaScanPartitionTargetSource::StaticFallback
        );

        Ok(())
    }

    #[test]
    fn invalid_policy_is_rejected_before_target_derivation()
    -> Result<(), Box<dyn std::error::Error>> {
        let policy = DeltaScanPartitionTargetPolicy {
            min_default_partitions: 0,
            max_default_partitions: 64,
            parallelism_multiplier: 1,
        };

        let error = policy
            .derive_target(context(), config(None, None, Some(8)))
            .err()
            .ok_or("expected invalid policy to fail")?;

        assert!(matches!(
            error,
            DeltaFunnelError::DeltaScanFileTaskPartitionPlanning { .. }
        ));
        assert!(error.to_string().contains("min_default_partitions"));

        Ok(())
    }
}
