//! Scan partition target selection and diagnostics.

use crate::{DeltaReaderError, error::InvalidConfigurationSnafu};

const DEFAULT_MIN_PARTITIONS: usize = 1;
const DEFAULT_PARALLELISM_MULTIPLIER: usize = 1;
const DEFAULT_FILE_DESCRIPTORS_PER_PARTITION: usize = 16;
const DEFAULT_AVAILABLE_MEMORY_BYTES_PER_PARTITION: u64 = 256 * 1024 * 1024;

/// Diagnostic input for scan partition target tools.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaScanPartitionTargetDiagnosticInput {
    /// Explicit scan target override.
    pub explicit_target_partitions: Option<usize>,
    /// DataFusion execution target, used as an upper cap during fallback.
    pub datafusion_target_partitions: Option<usize>,
    /// Available host parallelism used as the fallback baseline.
    pub available_parallelism: Option<usize>,
    /// Available memory in bytes, used as an upper cap when present.
    pub available_memory_bytes: Option<u64>,
    /// Unix soft file descriptor limit, used as an upper cap when present.
    pub unix_soft_file_descriptor_limit: Option<u64>,
    /// Minimum fallback partition count.
    pub min_default_partitions: usize,
    /// Multiplier applied to available parallelism before caps.
    pub parallelism_multiplier: usize,
    /// File descriptors reserved per fallback scan partition.
    pub file_descriptors_per_partition: usize,
    /// Available memory reserved per fallback scan partition.
    pub available_memory_bytes_per_partition: u64,
}

/// Diagnostic output for scan partition target tools.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaScanPartitionTargetDiagnosticOutput {
    /// Final target partition count.
    pub target_partitions: usize,
    /// Source that selected the uncapped target.
    pub source: DeltaScanPartitionTargetDiagnosticSource,
    /// Explicit scan target override from the input.
    pub explicit_target_partitions: Option<usize>,
    /// DataFusion execution target from the input.
    pub datafusion_target_partitions: Option<usize>,
    /// Available host parallelism from the input.
    pub available_parallelism: Option<usize>,
    /// DataFusion cap applied during fallback.
    pub datafusion_target_cap: Option<usize>,
    /// Unix file descriptor cap applied during fallback.
    pub unix_file_descriptor_cap: Option<usize>,
    /// Memory cap applied during fallback.
    pub memory_cap: Option<usize>,
}

/// Diagnostic source that selected the uncapped scan target.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaScanPartitionTargetDiagnosticSource {
    /// Explicit override selected the target.
    ExplicitOverride,
    /// Available host parallelism selected the fallback target.
    AvailableParallelismFallback,
    /// Static fallback selected the target.
    StaticFallback,
}

impl Default for DeltaScanPartitionTargetDiagnosticInput {
    fn default() -> Self {
        Self {
            explicit_target_partitions: None,
            datafusion_target_partitions: None,
            available_parallelism: None,
            available_memory_bytes: None,
            unix_soft_file_descriptor_limit: None,
            min_default_partitions: DEFAULT_MIN_PARTITIONS,
            parallelism_multiplier: DEFAULT_PARALLELISM_MULTIPLIER,
            file_descriptors_per_partition: DEFAULT_FILE_DESCRIPTORS_PER_PARTITION,
            available_memory_bytes_per_partition: DEFAULT_AVAILABLE_MEMORY_BYTES_PER_PARTITION,
        }
    }
}

/// Derives a scan partition target using the production policy.
#[doc(hidden)]
pub fn derive_delta_scan_partition_target_diagnostic(
    input: DeltaScanPartitionTargetDiagnosticInput,
) -> Result<DeltaScanPartitionTargetDiagnosticOutput, DeltaReaderError> {
    let decision = DeltaScanPartitionTargetPolicy::from(input).derive(input)?;

    Ok(DeltaScanPartitionTargetDiagnosticOutput {
        target_partitions: decision.target_partitions,
        source: decision.source,
        explicit_target_partitions: input.explicit_target_partitions,
        datafusion_target_partitions: input.datafusion_target_partitions,
        available_parallelism: input.available_parallelism,
        datafusion_target_cap: decision.datafusion_target_cap,
        unix_file_descriptor_cap: decision.unix_file_descriptor_cap,
        memory_cap: decision.memory_cap,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeltaScanPartitionTargetDecision {
    pub(crate) target_partitions: usize,
    pub(crate) source: DeltaScanPartitionTargetDiagnosticSource,
    pub(crate) datafusion_target_cap: Option<usize>,
    pub(crate) unix_file_descriptor_cap: Option<usize>,
    pub(crate) memory_cap: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeltaScanPartitionTargetPolicy {
    min_default_partitions: usize,
    parallelism_multiplier: usize,
    file_descriptors_per_partition: usize,
    available_memory_bytes_per_partition: u64,
}

impl From<DeltaScanPartitionTargetDiagnosticInput> for DeltaScanPartitionTargetPolicy {
    fn from(input: DeltaScanPartitionTargetDiagnosticInput) -> Self {
        Self {
            min_default_partitions: input.min_default_partitions,
            parallelism_multiplier: input.parallelism_multiplier,
            file_descriptors_per_partition: input.file_descriptors_per_partition,
            available_memory_bytes_per_partition: input.available_memory_bytes_per_partition,
        }
    }
}

impl DeltaScanPartitionTargetPolicy {
    pub(crate) fn derive(
        self,
        input: DeltaScanPartitionTargetDiagnosticInput,
    ) -> Result<DeltaScanPartitionTargetDecision, DeltaReaderError> {
        self.validate()?;

        if let Some(target_partitions) = input.explicit_target_partitions {
            validate_positive(
                target_partitions,
                "explicit_target_partitions_must_be_positive",
            )?;
            return Ok(DeltaScanPartitionTargetDecision {
                target_partitions,
                source: DeltaScanPartitionTargetDiagnosticSource::ExplicitOverride,
                datafusion_target_cap: None,
                unix_file_descriptor_cap: None,
                memory_cap: None,
            });
        }

        if let Some(target_partitions) = input.datafusion_target_partitions {
            validate_positive(
                target_partitions,
                "datafusion_target_partitions_must_be_positive",
            )?;
        }

        let (source, target_partitions) = match input.available_parallelism {
            Some(available_parallelism) => {
                validate_positive(
                    available_parallelism,
                    "available_parallelism_must_be_positive",
                )?;
                (
                    DeltaScanPartitionTargetDiagnosticSource::AvailableParallelismFallback,
                    available_parallelism
                        .saturating_mul(self.parallelism_multiplier)
                        .max(self.min_default_partitions),
                )
            }
            None => (
                DeltaScanPartitionTargetDiagnosticSource::StaticFallback,
                self.min_default_partitions,
            ),
        };
        let datafusion_target_cap = input.datafusion_target_partitions;
        let unix_file_descriptor_cap = input
            .unix_soft_file_descriptor_limit
            .and_then(|limit| usize::try_from(limit).ok())
            .map(|limit| (limit / self.file_descriptors_per_partition).max(1));
        let memory_cap = input
            .available_memory_bytes
            .map(|bytes| bytes / self.available_memory_bytes_per_partition)
            .and_then(|partitions| usize::try_from(partitions).ok())
            .map(|partitions| partitions.max(1));
        let target_partitions = [datafusion_target_cap, unix_file_descriptor_cap, memory_cap]
            .into_iter()
            .flatten()
            .fold(target_partitions, usize::min)
            .max(1);

        Ok(DeltaScanPartitionTargetDecision {
            target_partitions,
            source,
            datafusion_target_cap,
            unix_file_descriptor_cap,
            memory_cap,
        })
    }

    fn validate(self) -> Result<(), DeltaReaderError> {
        validate_positive(
            self.min_default_partitions,
            "min_default_partitions_must_be_positive",
        )?;
        validate_positive(
            self.parallelism_multiplier,
            "parallelism_multiplier_must_be_positive",
        )?;
        validate_positive(
            self.file_descriptors_per_partition,
            "file_descriptors_per_partition_must_be_positive",
        )?;
        if self.available_memory_bytes_per_partition == 0 {
            return InvalidConfigurationSnafu {
                reason: "available_memory_bytes_per_partition_must_be_positive",
            }
            .fail();
        }
        Ok(())
    }
}

fn validate_positive(value: usize, reason: &'static str) -> Result<(), DeltaReaderError> {
    if value == 0 {
        return InvalidConfigurationSnafu { reason }.fail();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DeltaReaderPhase;

    #[test]
    fn public_defaults_and_precedence_match_the_frozen_policy()
    -> Result<(), Box<dyn std::error::Error>> {
        let defaults = DeltaScanPartitionTargetDiagnosticInput::default();
        assert_eq!(defaults.min_default_partitions, 1);
        assert_eq!(defaults.parallelism_multiplier, 1);
        assert_eq!(defaults.file_descriptors_per_partition, 16);
        assert_eq!(
            defaults.available_memory_bytes_per_partition,
            256 * 1024 * 1024
        );

        let explicit = derive_delta_scan_partition_target_diagnostic(
            DeltaScanPartitionTargetDiagnosticInput {
                explicit_target_partitions: Some(12),
                datafusion_target_partitions: Some(8),
                available_parallelism: Some(4),
                available_memory_bytes: Some(1),
                unix_soft_file_descriptor_limit: Some(1),
                ..defaults
            },
        )?;
        assert_eq!(explicit.target_partitions, 12);
        assert_eq!(
            explicit.source,
            DeltaScanPartitionTargetDiagnosticSource::ExplicitOverride
        );
        assert_eq!(explicit.datafusion_target_cap, None);
        assert_eq!(explicit.unix_file_descriptor_cap, None);
        assert_eq!(explicit.memory_cap, None);

        let static_fallback = derive_delta_scan_partition_target_diagnostic(defaults)?;
        assert_eq!(static_fallback.target_partitions, 1);
        assert_eq!(
            static_fallback.source,
            DeltaScanPartitionTargetDiagnosticSource::StaticFallback
        );
        Ok(())
    }

    #[test]
    fn fallback_applies_every_cap_without_raising_a_lower_target()
    -> Result<(), Box<dyn std::error::Error>> {
        let output = derive_delta_scan_partition_target_diagnostic(
            DeltaScanPartitionTargetDiagnosticInput {
                datafusion_target_partitions: Some(32),
                available_parallelism: Some(64),
                available_memory_bytes: Some(512 * 1024 * 1024),
                unix_soft_file_descriptor_limit: Some(128),
                ..Default::default()
            },
        )?;
        assert_eq!(output.target_partitions, 2);
        assert_eq!(output.datafusion_target_cap, Some(32));
        assert_eq!(output.unix_file_descriptor_cap, Some(8));
        assert_eq!(output.memory_cap, Some(2));

        let lower = derive_delta_scan_partition_target_diagnostic(
            DeltaScanPartitionTargetDiagnosticInput {
                datafusion_target_partitions: Some(8),
                available_parallelism: Some(4),
                ..Default::default()
            },
        )?;
        assert_eq!(lower.target_partitions, 4);
        assert_eq!(
            lower.source,
            DeltaScanPartitionTargetDiagnosticSource::AvailableParallelismFallback
        );
        Ok(())
    }

    #[test]
    fn invalid_and_hostile_inputs_are_safe_and_redacted() -> Result<(), Box<dyn std::error::Error>>
    {
        for input in [
            DeltaScanPartitionTargetDiagnosticInput {
                explicit_target_partitions: Some(0),
                ..Default::default()
            },
            DeltaScanPartitionTargetDiagnosticInput {
                datafusion_target_partitions: Some(0),
                ..Default::default()
            },
            DeltaScanPartitionTargetDiagnosticInput {
                available_parallelism: Some(0),
                ..Default::default()
            },
            DeltaScanPartitionTargetDiagnosticInput {
                parallelism_multiplier: 0,
                ..Default::default()
            },
        ] {
            let error = derive_delta_scan_partition_target_diagnostic(input)
                .expect_err("zero diagnostic input must fail");
            assert_eq!(error.as_str(), "invalid_configuration");
            assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
        }

        let huge = derive_delta_scan_partition_target_diagnostic(
            DeltaScanPartitionTargetDiagnosticInput {
                available_parallelism: Some(usize::MAX),
                parallelism_multiplier: usize::MAX,
                ..Default::default()
            },
        )?;
        assert_eq!(huge.target_partitions, usize::MAX);
        Ok(())
    }
}
