//! Default target derivation for Delta scan file task partitions.
//!
//! The policy is intentionally conservative and CPU-oriented. Stable execution
//! configuration wins first, and machine-derived parallelism is only a bounded
//! fallback so large machines do not create explosive partition counts by
//! default. Unit tests inject machine context instead of reading host state.

use crate::{DeltaFunnelError, error::DeltaScanFileTaskPartitionPlanningSnafu};

use super::execution_environment::{
    DeltaExecutionEnvironmentProfile, DeltaMemoryHint, DeltaUnixResourceLimit,
};
use super::file_task_partition::DeltaScanFileTaskPartitionOptions;

const DEFAULT_MIN_PARTITIONS: usize = 4;
const DEFAULT_MAX_PARTITIONS: usize = 64;
const DEFAULT_PARALLELISM_MULTIPLIER: usize = 1;
// Conservative pre-benchmark guards used to keep fallback partition counts from
// over-consuming per-partition OS resources. Issue #128's benchmark matrix is
// expected to validate or replace these policy variables before they become
// performance-tuned defaults.
const DEFAULT_FILE_DESCRIPTORS_PER_PARTITION: usize = 16;
const DEFAULT_AVAILABLE_MEMORY_BYTES_PER_PARTITION: u64 = 256 * 1024 * 1024;

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
    /// Local execution environment profile used for fallback target derivation.
    pub(crate) environment_profile: DeltaExecutionEnvironmentProfile,
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
    /// File descriptors reserved per fallback scan partition on Unix platforms.
    ///
    /// This is a conservative policy variable for benchmark validation, not a
    /// measured optimum.
    pub(crate) file_descriptors_per_partition: usize,
    /// Available memory reserved per fallback scan partition when memory is known.
    ///
    /// This is a conservative policy variable for benchmark validation, not a
    /// measured optimum.
    pub(crate) available_memory_bytes_per_partition: u64,
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
    /// Execution environment profile used by this decision.
    pub(crate) environment_profile: DeltaExecutionEnvironmentProfile,
    /// Resource caps that affected fallback target derivation.
    pub(crate) applied_caps: DeltaScanPartitionTargetAppliedCaps,
    /// Policy used for fallback target derivation.
    pub(crate) policy: DeltaScanPartitionTargetPolicy,
}

/// Resource caps applied to fallback target derivation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct DeltaScanPartitionTargetAppliedCaps {
    /// Cap derived from Unix file descriptor limits.
    pub(crate) unix_file_descriptor_limit: Option<usize>,
    /// Cap derived from available memory hints.
    pub(crate) memory_hint: Option<usize>,
}

impl Default for DeltaScanPartitionTargetPolicy {
    fn default() -> Self {
        Self {
            min_default_partitions: DEFAULT_MIN_PARTITIONS,
            max_default_partitions: DEFAULT_MAX_PARTITIONS,
            parallelism_multiplier: DEFAULT_PARALLELISM_MULTIPLIER,
            file_descriptors_per_partition: DEFAULT_FILE_DESCRIPTORS_PER_PARTITION,
            available_memory_bytes_per_partition: DEFAULT_AVAILABLE_MEMORY_BYTES_PER_PARTITION,
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
            environment_profile: DeltaExecutionEnvironmentProfile::from_local_environment(),
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

        let (source, target_partitions, applied_caps) =
            match config.environment_profile.available_parallelism {
                Some(available_parallelism) => {
                    self.validate_target(context, "available_parallelism", available_parallelism)?;
                    let multiplied = available_parallelism
                        .saturating_mul(self.parallelism_multiplier)
                        .clamp(self.min_default_partitions, self.max_default_partitions);
                    let (capped, applied_caps) = self.apply_fallback_caps(multiplied, config);

                    (
                        DeltaScanPartitionTargetSource::AvailableParallelismFallback,
                        capped,
                        applied_caps,
                    )
                }
                None => {
                    let (capped, applied_caps) =
                        self.apply_fallback_caps(self.min_default_partitions, config);
                    (
                        DeltaScanPartitionTargetSource::StaticFallback,
                        capped,
                        applied_caps,
                    )
                }
            };

        Ok(self.fallback_decision(source, target_partitions, config, applied_caps))
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
        if self.file_descriptors_per_partition == 0 {
            return target_planning_error(
                context,
                "file_descriptors_per_partition must be greater than zero",
            );
        }
        if self.available_memory_bytes_per_partition == 0 {
            return target_planning_error(
                context,
                "available_memory_bytes_per_partition must be greater than zero",
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
            environment_profile: config.environment_profile,
            applied_caps: DeltaScanPartitionTargetAppliedCaps {
                unix_file_descriptor_limit: None,
                memory_hint: None,
            },
            policy: self,
        }
    }

    fn fallback_decision(
        self,
        source: DeltaScanPartitionTargetSource,
        target_partitions: usize,
        config: DeltaScanPartitionTargetConfig,
        applied_caps: DeltaScanPartitionTargetAppliedCaps,
    ) -> DeltaScanPartitionTargetDecision {
        DeltaScanPartitionTargetDecision {
            target_partitions,
            source,
            explicit_target_partitions: config.explicit_target_partitions,
            datafusion_target_partitions: config.datafusion_target_partitions,
            environment_profile: config.environment_profile,
            applied_caps,
            policy: self,
        }
    }

    fn apply_fallback_caps(
        self,
        target_partitions: usize,
        config: DeltaScanPartitionTargetConfig,
    ) -> (usize, DeltaScanPartitionTargetAppliedCaps) {
        let unix_file_descriptor_limit = self.unix_file_descriptor_cap(config);
        let memory_hint = self.memory_cap(config.environment_profile.memory_hint);
        let mut capped_target = target_partitions;

        if let Some(cap) = unix_file_descriptor_limit {
            capped_target = capped_target.min(cap);
        }
        if let Some(cap) = memory_hint {
            capped_target = capped_target.min(cap);
        }

        (
            capped_target.max(1),
            DeltaScanPartitionTargetAppliedCaps {
                unix_file_descriptor_limit,
                memory_hint,
            },
        )
    }

    fn unix_file_descriptor_cap(self, config: DeltaScanPartitionTargetConfig) -> Option<usize> {
        let limit = config.environment_profile.unix_file_descriptor_limit?;
        let DeltaUnixResourceLimit::Finite(soft_limit) = limit.soft_limit else {
            return None;
        };
        let soft_limit = usize::try_from(soft_limit).ok()?;

        Some((soft_limit / self.file_descriptors_per_partition).max(1))
    }

    fn memory_cap(self, memory_hint: Option<DeltaMemoryHint>) -> Option<usize> {
        let available_bytes = memory_hint?.available_bytes?;
        let cap = available_bytes / self.available_memory_bytes_per_partition;

        usize::try_from(cap).ok().map(|cap| cap.max(1))
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
    use super::super::execution_environment::{
        DeltaExecutionOsFamily, DeltaMemoryHint, DeltaUnixFileDescriptorLimit,
    };
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
            environment_profile: profile(available_parallelism, None, None),
        }
    }

    fn profile(
        available_parallelism: Option<usize>,
        memory_hint: Option<DeltaMemoryHint>,
        unix_file_descriptor_limit: Option<DeltaUnixFileDescriptorLimit>,
    ) -> DeltaExecutionEnvironmentProfile {
        DeltaExecutionEnvironmentProfile {
            available_parallelism,
            os_family: DeltaExecutionOsFamily::Other,
            memory_hint,
            unix_file_descriptor_limit,
            io_latency_hint: None,
            runtime_probe: None,
        }
    }

    fn test_policy() -> DeltaScanPartitionTargetPolicy {
        DeltaScanPartitionTargetPolicy {
            min_default_partitions: 4,
            max_default_partitions: 64,
            parallelism_multiplier: 1,
            file_descriptors_per_partition: 16,
            available_memory_bytes_per_partition: 256 * 1024 * 1024,
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
        assert_eq!(decision.environment_profile.available_parallelism, Some(4));
        assert_eq!(
            decision.applied_caps,
            DeltaScanPartitionTargetAppliedCaps {
                unix_file_descriptor_limit: None,
                memory_hint: None,
            }
        );
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
            file_descriptors_per_partition: 16,
            available_memory_bytes_per_partition: 256 * 1024 * 1024,
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
    fn unix_file_descriptor_limit_caps_fallback_target() -> Result<(), Box<dyn std::error::Error>> {
        let config = DeltaScanPartitionTargetConfig {
            explicit_target_partitions: None,
            datafusion_target_partitions: None,
            environment_profile: profile(
                Some(64),
                None,
                Some(DeltaUnixFileDescriptorLimit {
                    soft_limit: DeltaUnixResourceLimit::Finite(64),
                    hard_limit: DeltaUnixResourceLimit::Finite(256),
                }),
            ),
        };

        let decision = test_policy().derive_target(context(), config)?;

        assert_eq!(decision.target_partitions, 4);
        assert_eq!(decision.applied_caps.unix_file_descriptor_limit, Some(4));
        assert_eq!(decision.applied_caps.memory_hint, None);

        Ok(())
    }

    #[test]
    fn memory_hint_caps_fallback_target() -> Result<(), Box<dyn std::error::Error>> {
        let config = DeltaScanPartitionTargetConfig {
            explicit_target_partitions: None,
            datafusion_target_partitions: None,
            environment_profile: profile(
                Some(64),
                Some(DeltaMemoryHint {
                    total_bytes: Some(4 * 1024 * 1024 * 1024),
                    available_bytes: Some(512 * 1024 * 1024),
                }),
                None,
            ),
        };

        let decision = test_policy().derive_target(context(), config)?;

        assert_eq!(decision.target_partitions, 2);
        assert_eq!(decision.applied_caps.unix_file_descriptor_limit, None);
        assert_eq!(decision.applied_caps.memory_hint, Some(2));

        Ok(())
    }

    #[test]
    fn resource_caps_do_not_override_explicit_target() -> Result<(), Box<dyn std::error::Error>> {
        let config = DeltaScanPartitionTargetConfig {
            explicit_target_partitions: Some(32),
            datafusion_target_partitions: None,
            environment_profile: profile(
                Some(64),
                Some(DeltaMemoryHint {
                    total_bytes: Some(4 * 1024 * 1024 * 1024),
                    available_bytes: Some(512 * 1024 * 1024),
                }),
                Some(DeltaUnixFileDescriptorLimit {
                    soft_limit: DeltaUnixResourceLimit::Finite(64),
                    hard_limit: DeltaUnixResourceLimit::Finite(256),
                }),
            ),
        };

        let decision = test_policy().derive_target(context(), config)?;

        assert_eq!(decision.target_partitions, 32);
        assert_eq!(
            decision.source,
            DeltaScanPartitionTargetSource::ExplicitOverride
        );
        assert_eq!(decision.applied_caps.unix_file_descriptor_limit, None);
        assert_eq!(decision.applied_caps.memory_hint, None);

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
            file_descriptors_per_partition: 16,
            available_memory_bytes_per_partition: 256 * 1024 * 1024,
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
