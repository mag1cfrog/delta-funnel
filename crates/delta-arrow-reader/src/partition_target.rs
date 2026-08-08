//! Scan partition target selection and diagnostics.

#[cfg(target_os = "linux")]
use std::fs;
#[cfg(windows)]
use std::mem;
#[cfg(unix)]
use std::mem::MaybeUninit;
#[cfg(windows)]
use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};

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

/// Local environment diagnostic used by scan partition tools.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaScanPartitionTargetLocalEnvironmentDiagnostic {
    /// Production diagnostic policy input derived from cheap local host signals.
    pub policy_input: DeltaScanPartitionTargetDiagnosticInput,
    /// Total physical memory in bytes, when available.
    pub memory_total_bytes: Option<u64>,
    /// Available memory in bytes, when available.
    pub memory_available_bytes: Option<u64>,
    /// Unix soft file descriptor limit, when finite and available.
    pub unix_soft_file_descriptor_limit: Option<u64>,
    /// Status of the Unix soft file descriptor limit probe.
    pub unix_soft_file_descriptor_limit_status:
        DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus,
}

/// Diagnostic status for the local Unix file descriptor soft limit probe.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus {
    /// The current platform does not expose a Unix file descriptor limit.
    Unsupported,
    /// The probe failed or returned no usable value.
    Unknown,
    /// The Unix soft file descriptor limit is finite.
    Finite,
    /// The Unix soft file descriptor limit is unlimited.
    Unlimited,
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

/// Collects cheap local host signals for scan partition target diagnostics.
#[doc(hidden)]
pub fn delta_scan_partition_target_local_environment_diagnostic()
-> DeltaScanPartitionTargetLocalEnvironmentDiagnostic {
    let available_parallelism = std::thread::available_parallelism()
        .ok()
        .map(std::num::NonZeroUsize::get);
    let memory = local_memory_hint();
    let (unix_soft_file_descriptor_limit, unix_soft_file_descriptor_limit_status) =
        unix_soft_file_descriptor_diagnostic(local_unix_file_descriptor_limit());

    DeltaScanPartitionTargetLocalEnvironmentDiagnostic {
        policy_input: DeltaScanPartitionTargetDiagnosticInput {
            datafusion_target_partitions: available_parallelism,
            available_parallelism,
            available_memory_bytes: memory.and_then(|memory| memory.available_bytes),
            unix_soft_file_descriptor_limit,
            ..Default::default()
        },
        memory_total_bytes: memory.and_then(|memory| memory.total_bytes),
        memory_available_bytes: memory.and_then(|memory| memory.available_bytes),
        unix_soft_file_descriptor_limit,
        unix_soft_file_descriptor_limit_status,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemoryHint {
    total_bytes: Option<u64>,
    available_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnixResourceLimit {
    Finite(u64),
    Unlimited,
}

#[cfg(target_os = "linux")]
fn local_memory_hint() -> Option<MemoryHint> {
    fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|contents| parse_linux_meminfo(&contents))
}

#[cfg(windows)]
fn local_memory_hint() -> Option<MemoryHint> {
    let mut status = MEMORYSTATUSEX {
        dwLength: mem::size_of::<MEMORYSTATUSEX>() as u32,
        dwMemoryLoad: 0,
        ullTotalPhys: 0,
        ullAvailPhys: 0,
        ullTotalPageFile: 0,
        ullAvailPageFile: 0,
        ullTotalVirtual: 0,
        ullAvailVirtual: 0,
        ullAvailExtendedVirtual: 0,
    };
    // SAFETY: `status` is a valid MEMORYSTATUSEX with the required length field.
    if unsafe { GlobalMemoryStatusEx(&mut status) } == 0 {
        return None;
    }
    memory_hint(nonzero(status.ullTotalPhys), nonzero(status.ullAvailPhys))
}

#[cfg(not(any(target_os = "linux", windows)))]
fn local_memory_hint() -> Option<MemoryHint> {
    None
}

#[cfg(target_os = "linux")]
fn parse_linux_meminfo(contents: &str) -> Option<MemoryHint> {
    let mut total_bytes = None;
    let mut available_bytes = None;

    for line in contents.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        match name {
            "MemTotal" => total_bytes = Some(parse_linux_kib(value)?),
            "MemAvailable" => available_bytes = Some(parse_linux_kib(value)?),
            _ => {}
        }
    }

    memory_hint(total_bytes, available_bytes)
}

#[cfg(target_os = "linux")]
fn parse_linux_kib(value: &str) -> Option<u64> {
    let mut fields = value.split_whitespace();
    let kib = fields.next()?.parse::<u64>().ok()?;
    (fields.next()? == "kB").then(|| kib.checked_mul(1024))?
}

fn memory_hint(total_bytes: Option<u64>, available_bytes: Option<u64>) -> Option<MemoryHint> {
    (total_bytes.is_some() || available_bytes.is_some()).then_some(MemoryHint {
        total_bytes,
        available_bytes,
    })
}

#[cfg(windows)]
fn nonzero(value: u64) -> Option<u64> {
    (value != 0).then_some(value)
}

#[cfg(unix)]
fn local_unix_file_descriptor_limit() -> Option<UnixResourceLimit> {
    let mut limit = MaybeUninit::<libc::rlimit>::uninit();
    // SAFETY: `getrlimit` initializes the valid pointer when it returns success.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: `getrlimit` succeeded, so `limit` is initialized.
    let limit = unsafe { limit.assume_init() }.rlim_cur;

    Some(if limit == libc::RLIM_INFINITY {
        UnixResourceLimit::Unlimited
    } else {
        UnixResourceLimit::Finite(limit)
    })
}

#[cfg(not(unix))]
fn local_unix_file_descriptor_limit() -> Option<UnixResourceLimit> {
    None
}

fn unix_soft_file_descriptor_diagnostic(
    limit: Option<UnixResourceLimit>,
) -> (
    Option<u64>,
    DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus,
) {
    match limit {
        Some(UnixResourceLimit::Finite(limit)) => (
            Some(limit),
            DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Finite,
        ),
        Some(UnixResourceLimit::Unlimited) => (
            None,
            DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unlimited,
        ),
        None if cfg!(unix) => (
            None,
            DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unknown,
        ),
        None => (
            None,
            DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unsupported,
        ),
    }
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

    #[test]
    fn local_environment_diagnostic_feeds_the_same_policy() -> Result<(), Box<dyn std::error::Error>>
    {
        let diagnostic = delta_scan_partition_target_local_environment_diagnostic();
        let output = derive_delta_scan_partition_target_diagnostic(diagnostic.policy_input)?;

        assert_eq!(
            diagnostic.policy_input.datafusion_target_partitions,
            diagnostic.policy_input.available_parallelism
        );
        assert_eq!(
            diagnostic.policy_input.available_memory_bytes,
            diagnostic.memory_available_bytes
        );
        assert_eq!(
            diagnostic.policy_input.unix_soft_file_descriptor_limit,
            diagnostic.unix_soft_file_descriptor_limit
        );
        assert!(output.target_partitions > 0);
        Ok(())
    }

    #[test]
    fn unix_file_descriptor_diagnostic_preserves_every_status() {
        let (value, status) = unix_soft_file_descriptor_diagnostic(None);
        assert_eq!(value, None);
        assert!(matches!(
            status,
            DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unknown
                | DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unsupported
        ));
        assert_eq!(
            unix_soft_file_descriptor_diagnostic(Some(UnixResourceLimit::Finite(128))),
            (
                Some(128),
                DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Finite
            )
        );
        assert_eq!(
            unix_soft_file_descriptor_diagnostic(Some(UnixResourceLimit::Unlimited)),
            (
                None,
                DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unlimited
            )
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_memory_parser_preserves_valid_values_and_rejects_invalid_units() {
        let hint = parse_linux_meminfo(
            "MemTotal: 16384000 kB\nMemFree: 1000000 kB\nMemAvailable: 8192000 kB\n",
        )
        .expect("valid Linux memory hint");
        assert_eq!(hint.total_bytes, Some(16_777_216_000));
        assert_eq!(hint.available_bytes, Some(8_388_608_000));
        assert_eq!(parse_linux_meminfo("SwapTotal: 1024 kB\n"), None);
        assert_eq!(parse_linux_meminfo("MemTotal: 1 MB\n"), None);
    }
}
