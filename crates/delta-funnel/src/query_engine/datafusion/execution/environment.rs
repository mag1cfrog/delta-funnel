//! Local execution environment signals for scan planning heuristics.
//!
//! The profile collects stable, cheap, local-only signals. Provider scan
//! planning must not run network, disk latency, or runtime stress probes by
//! default, so optional probe-oriented fields stay unset unless a future caller
//! explicitly supplies them.

#[cfg(target_os = "linux")]
use std::fs;
#[cfg(windows)]
use std::mem;
#[cfg(unix)]
use std::mem::MaybeUninit;
#[cfg(windows)]
use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};

/// Local execution environment profile used by scan target policy decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct DeltaExecutionEnvironmentProfile {
    /// Host parallelism reported by Rust, usually respecting process CPU quotas.
    pub(crate) available_parallelism: Option<usize>,
    /// Operating system family used for diagnostics and platform-specific hints.
    pub(crate) os_family: DeltaExecutionOsFamily,
    /// Memory information collected from cheap local OS signals when available.
    pub(crate) memory_hint: Option<DeltaMemoryHint>,
    /// Unix process file descriptor limit, when available on this platform.
    pub(crate) unix_file_descriptor_limit: Option<DeltaUnixFileDescriptorLimit>,
    /// Optional IO latency hint supplied by explicit benchmark or calibration probes.
    pub(crate) io_latency_hint: Option<DeltaIoLatencyHint>,
    /// Optional runtime probe result supplied by explicit benchmark or calibration probes.
    pub(crate) runtime_probe: Option<DeltaRuntimeProbeResult>,
}

/// Operating system family for execution environment diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum DeltaExecutionOsFamily {
    /// Linux target.
    Linux,
    /// macOS target.
    Macos,
    /// Windows target.
    Windows,
    /// Other Unix target.
    Unix,
    /// Other non-Unix target.
    Other,
}

/// Local memory values useful as conservative planning caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct DeltaMemoryHint {
    /// Physical memory in bytes, when cheaply available.
    pub(crate) total_bytes: Option<u64>,
    /// OS-reported available memory in bytes, when cheaply available.
    pub(crate) available_bytes: Option<u64>,
}

/// Unix process file descriptor limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct DeltaUnixFileDescriptorLimit {
    /// Soft file descriptor limit enforced for this process.
    pub(crate) soft_limit: DeltaUnixResourceLimit,
    /// Hard file descriptor limit for this process.
    pub(crate) hard_limit: DeltaUnixResourceLimit,
}

/// Unix resource limit value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum DeltaUnixResourceLimit {
    /// Finite resource limit value.
    Finite(u64),
    /// Unlimited resource limit.
    Unlimited,
}

/// Optional IO latency probe hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct DeltaIoLatencyHint {
    /// Probe latency in microseconds.
    pub(crate) latency_micros: u64,
    /// Source that produced this latency hint.
    pub(crate) source: DeltaIoLatencyHintSource,
}

/// Source of an IO latency hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum DeltaIoLatencyHintSource {
    /// Explicit local IO probe.
    LocalIoProbe,
    /// Explicit HTTP or object-store probe.
    RemoteIoProbe,
    /// User-provided latency hint.
    UserProvided,
}

/// Optional runtime calibration result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct DeltaRuntimeProbeResult {
    /// Highest concurrency the probe considered stable, if measured.
    pub(crate) stable_concurrency_hint: Option<usize>,
}

impl DeltaExecutionEnvironmentProfile {
    /// Collects cheap local environment signals for provider scan planning.
    ///
    /// This method never performs network probes, disk latency probes, or runtime
    /// stress probes. Collection failures are represented as missing optional
    /// fields instead of planning errors.
    #[allow(dead_code)]
    pub(crate) fn from_local_environment() -> Self {
        Self {
            available_parallelism: local_available_parallelism(),
            os_family: local_os_family(),
            memory_hint: local_memory_hint(),
            unix_file_descriptor_limit: local_unix_file_descriptor_limit(),
            io_latency_hint: None,
            runtime_probe: None,
        }
    }
}

fn local_available_parallelism() -> Option<usize> {
    std::thread::available_parallelism()
        .ok()
        .map(std::num::NonZeroUsize::get)
}

fn local_os_family() -> DeltaExecutionOsFamily {
    if cfg!(target_os = "linux") {
        DeltaExecutionOsFamily::Linux
    } else if cfg!(target_os = "macos") {
        DeltaExecutionOsFamily::Macos
    } else if cfg!(target_os = "windows") {
        DeltaExecutionOsFamily::Windows
    } else if cfg!(unix) {
        DeltaExecutionOsFamily::Unix
    } else {
        DeltaExecutionOsFamily::Other
    }
}

#[cfg(target_os = "linux")]
fn local_memory_hint() -> Option<DeltaMemoryHint> {
    fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|contents| parse_linux_meminfo(&contents))
}

#[cfg(windows)]
fn local_memory_hint() -> Option<DeltaMemoryHint> {
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
    // SAFETY: `status` is a valid pointer to a MEMORYSTATUSEX whose `dwLength`
    // field is initialized as required by GlobalMemoryStatusEx.
    let result = unsafe { GlobalMemoryStatusEx(&mut status) };
    if result == 0 {
        return None;
    }

    memory_hint_from_values(status.ullTotalPhys, status.ullAvailPhys)
}

#[cfg(not(any(target_os = "linux", windows)))]
fn local_memory_hint() -> Option<DeltaMemoryHint> {
    None
}

#[cfg(target_os = "linux")]
fn parse_linux_meminfo(contents: &str) -> Option<DeltaMemoryHint> {
    let mut total_bytes = None;
    let mut available_bytes = None;

    for line in contents.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        match name {
            "MemTotal" => total_bytes = Some(linux_meminfo_kib_value_to_bytes(value)?),
            "MemAvailable" => available_bytes = Some(linux_meminfo_kib_value_to_bytes(value)?),
            _ => {}
        }
    }

    memory_hint_from_options(total_bytes, available_bytes)
}

#[cfg(target_os = "linux")]
fn linux_meminfo_kib_value_to_bytes(value: &str) -> Option<u64> {
    let mut fields = value.split_whitespace();
    let kib = fields.next()?.parse::<u64>().ok()?;
    let unit = fields.next()?;
    if unit != "kB" {
        return None;
    }

    kib.checked_mul(1024)
}

#[cfg(windows)]
fn memory_hint_from_values(total_bytes: u64, available_bytes: u64) -> Option<DeltaMemoryHint> {
    memory_hint_from_options(nonzero_u64(total_bytes), nonzero_u64(available_bytes))
}

fn memory_hint_from_options(
    total_bytes: Option<u64>,
    available_bytes: Option<u64>,
) -> Option<DeltaMemoryHint> {
    if total_bytes.is_none() && available_bytes.is_none() {
        None
    } else {
        Some(DeltaMemoryHint {
            total_bytes,
            available_bytes,
        })
    }
}

#[cfg(any(windows, test))]
fn nonzero_u64(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

#[cfg(unix)]
fn local_unix_file_descriptor_limit() -> Option<DeltaUnixFileDescriptorLimit> {
    let mut limit = MaybeUninit::<libc::rlimit>::uninit();
    // SAFETY: `getrlimit` initializes the provided rlimit pointer when it
    // returns 0. The pointer is valid for the duration of the call.
    let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) };
    if result != 0 {
        return None;
    }
    // SAFETY: `getrlimit` returned success, so `limit` has been initialized.
    let limit = unsafe { limit.assume_init() };

    Some(DeltaUnixFileDescriptorLimit {
        soft_limit: unix_resource_limit_from_raw(limit.rlim_cur),
        hard_limit: unix_resource_limit_from_raw(limit.rlim_max),
    })
}

#[cfg(not(unix))]
fn local_unix_file_descriptor_limit() -> Option<DeltaUnixFileDescriptorLimit> {
    None
}

#[cfg(unix)]
fn unix_resource_limit_from_raw(limit: libc::rlim_t) -> DeltaUnixResourceLimit {
    if limit == libc::RLIM_INFINITY {
        DeltaUnixResourceLimit::Unlimited
    } else {
        DeltaUnixResourceLimit::Finite(limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_environment_profile_collection_is_best_effort() {
        let profile = DeltaExecutionEnvironmentProfile::from_local_environment();

        assert!(matches!(
            profile.os_family,
            DeltaExecutionOsFamily::Linux
                | DeltaExecutionOsFamily::Macos
                | DeltaExecutionOsFamily::Windows
                | DeltaExecutionOsFamily::Unix
                | DeltaExecutionOsFamily::Other
        ));
        assert!(profile.io_latency_hint.is_none());
        assert!(profile.runtime_probe.is_none());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_meminfo_parser_extracts_total_and_available_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let hint = parse_linux_meminfo(
            "\
MemTotal:       16384000 kB
MemFree:         1000000 kB
MemAvailable:    8192000 kB
",
        )
        .ok_or("expected linux memory hint")?;

        assert_eq!(hint.total_bytes, Some(16_777_216_000));
        assert_eq!(hint.available_bytes, Some(8_388_608_000));

        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_meminfo_parser_ignores_unrelated_non_kib_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let hint = parse_linux_meminfo(
            "\
MemTotal:       16384000 kB
HugePages_Total:       0
MemAvailable:    8192000 kB
",
        )
        .ok_or("expected linux memory hint")?;

        assert_eq!(hint.total_bytes, Some(16_777_216_000));
        assert_eq!(hint.available_bytes, Some(8_388_608_000));

        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_meminfo_parser_returns_none_without_known_memory_fields() {
        let hint = parse_linux_meminfo("SwapTotal: 1024 kB\n");

        assert_eq!(hint, None);
    }

    #[test]
    fn zero_memory_values_are_treated_as_missing() {
        assert_eq!(nonzero_u64(0), None);
        assert_eq!(nonzero_u64(1), Some(1));
    }

    #[cfg(unix)]
    #[test]
    fn unix_resource_limit_preserves_finite_and_unlimited_values() {
        assert_eq!(
            unix_resource_limit_from_raw(512),
            DeltaUnixResourceLimit::Finite(512)
        );
        assert_eq!(
            unix_resource_limit_from_raw(libc::RLIM_INFINITY),
            DeltaUnixResourceLimit::Unlimited
        );
    }
}
