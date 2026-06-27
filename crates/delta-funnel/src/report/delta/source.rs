use std::fmt;

use crate::{
    DeltaProtocolReport, DeltaProviderReadStatsSnapshot, DeltaProviderReaderBackend,
    DeltaProviderScanExecutionOptions, FileCount, PhaseTimingReport, QueryOptions,
    ReportReasonCode,
};

/// Conservative source usage status for a workflow report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceUsageStatus {
    /// Source usage is known and the source was used by at least one selected output.
    Used,
    /// Source usage is known and the source was not used by selected outputs.
    NotUsed,
    /// Source usage could not be proven from available workflow analysis.
    Unknown,
}

impl SourceUsageStatus {
    /// Returns a stable lower-snake-case code for report serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Used => "used",
            Self::NotUsed => "not_used",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for SourceUsageStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Source-level readiness report for one registered Delta source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaSourceReport {
    source_name: String,
    source_uri: String,
    snapshot_version: u64,
    protocol: DeltaProtocolReport,
    scheduling: DeltaProviderSchedulingReport,
    file_count: FileCount,
    file_count_reason: Option<ReportReasonCode>,
    scan_metadata_exhausted: bool,
    usage_status: SourceUsageStatus,
    used_by_output_names: Vec<String>,
    provider_read_stats: Option<DeltaProviderReadStatsSnapshot>,
    provider_stats_reason: Option<ReportReasonCode>,
    phase_timings: Vec<PhaseTimingReport>,
}

/// Configured provider scheduling options included with a Delta source report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaProviderSchedulingReport {
    query_target_partitions: Option<u64>,
    reader_backend: DeltaProviderReaderBackend,
    max_concurrent_file_reads_per_scan: u64,
    max_concurrent_file_reads_per_partition: u64,
    output_buffer_capacity_per_partition: u64,
    native_async_prefetch_file_count_per_partition: u64,
}

impl DeltaProviderSchedulingReport {
    pub(crate) fn from_options(
        query_options: QueryOptions,
        scan_options: DeltaProviderScanExecutionOptions,
    ) -> Self {
        Self {
            query_target_partitions: query_options
                .target_partitions
                .map(crate::usize_to_u64_saturating),
            reader_backend: scan_options.reader_backend,
            max_concurrent_file_reads_per_scan: crate::usize_to_u64_saturating(
                scan_options.max_concurrent_file_reads_per_scan,
            ),
            max_concurrent_file_reads_per_partition: crate::usize_to_u64_saturating(
                scan_options.max_concurrent_file_reads_per_partition,
            ),
            output_buffer_capacity_per_partition: crate::usize_to_u64_saturating(
                scan_options.output_buffer_capacity_per_partition,
            ),
            native_async_prefetch_file_count_per_partition: crate::usize_to_u64_saturating(
                scan_options.native_async_prefetch_file_count_per_partition,
            ),
        }
    }

    /// Returns the configured DataFusion target partition count, when set.
    #[must_use]
    pub const fn query_target_partitions(&self) -> Option<u64> {
        self.query_target_partitions
    }

    /// Returns the provider reader backend configured for source scans.
    #[must_use]
    pub const fn reader_backend(&self) -> DeltaProviderReaderBackend {
        self.reader_backend
    }

    /// Returns the configured scan-wide file-read concurrency cap.
    #[must_use]
    pub const fn max_concurrent_file_reads_per_scan(&self) -> u64 {
        self.max_concurrent_file_reads_per_scan
    }

    /// Returns the configured per-partition file-read concurrency cap.
    #[must_use]
    pub const fn max_concurrent_file_reads_per_partition(&self) -> u64 {
        self.max_concurrent_file_reads_per_partition
    }

    /// Returns the configured per-partition provider output buffer capacity.
    #[must_use]
    pub const fn output_buffer_capacity_per_partition(&self) -> u64 {
        self.output_buffer_capacity_per_partition
    }

    /// Returns the native async prefetch depth per execution partition.
    #[must_use]
    pub const fn native_async_prefetch_file_count_per_partition(&self) -> u64 {
        self.native_async_prefetch_file_count_per_partition
    }
}

impl DeltaSourceReport {
    pub(crate) fn metadata_only(
        source_name: impl Into<String>,
        source_uri: impl Into<String>,
        snapshot_version: u64,
        protocol: DeltaProtocolReport,
        scheduling: DeltaProviderSchedulingReport,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            source_uri: source_uri.into(),
            snapshot_version,
            protocol,
            scheduling,
            file_count: FileCount::unavailable(),
            file_count_reason: Some(ReportReasonCode::CostAvoidance),
            scan_metadata_exhausted: false,
            usage_status: SourceUsageStatus::Unknown,
            used_by_output_names: Vec::new(),
            provider_read_stats: None,
            provider_stats_reason: Some(ReportReasonCode::NotExecuted),
            phase_timings: Vec::new(),
        }
    }

    pub(crate) fn with_phase_timings(mut self, phase_timings: Vec<PhaseTimingReport>) -> Self {
        self.phase_timings = phase_timings;
        self
    }

    pub(crate) fn with_usage(
        mut self,
        usage_status: SourceUsageStatus,
        used_by_output_names: Vec<String>,
    ) -> Self {
        self.usage_status = usage_status;
        self.used_by_output_names = used_by_output_names;
        self
    }

    pub(crate) fn with_provider_read_stats(
        mut self,
        stats: DeltaProviderReadStatsSnapshot,
    ) -> Self {
        self.scan_metadata_exhausted = stats.scan_metadata_exhausted.unwrap_or(false);
        self.file_count = match stats.scan_metadata_exhausted {
            Some(true) => FileCount::exact(stats.files_planned),
            Some(false) => FileCount::estimated(stats.files_planned),
            None => FileCount::unavailable(),
        };
        self.file_count_reason = match self.file_count {
            FileCount::Exact(_) | FileCount::Estimated(_) => None,
            FileCount::Unavailable => Some(ReportReasonCode::CapabilityUnavailable),
            FileCount::Skipped | FileCount::NotExecuted => Some(ReportReasonCode::NotExecuted),
        };
        self.provider_read_stats = Some(stats);
        self.provider_stats_reason = None;
        self
    }

    pub(crate) fn with_provider_stats_reason(mut self, reason: ReportReasonCode) -> Self {
        self.provider_read_stats = None;
        self.provider_stats_reason = Some(reason);
        self
    }

    /// Returns the DataFusion table name for this source.
    #[must_use]
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    /// Returns the sanitized Delta source URI or display summary.
    #[must_use]
    pub fn source_uri(&self) -> &str {
        &self.source_uri
    }

    /// Returns the resolved Delta snapshot version.
    #[must_use]
    pub const fn snapshot_version(&self) -> u64 {
        self.snapshot_version
    }

    /// Returns the Delta protocol report captured for this source.
    #[must_use]
    pub const fn protocol(&self) -> &DeltaProtocolReport {
        &self.protocol
    }

    /// Returns the configured provider scheduling options for this source.
    #[must_use]
    pub const fn scheduling(&self) -> &DeltaProviderSchedulingReport {
        &self.scheduling
    }

    /// Returns file-count evidence for this source.
    #[must_use]
    pub const fn file_count(&self) -> FileCount {
        self.file_count
    }

    /// Returns the stable reason code for unavailable, skipped, or not-executed file counts.
    #[must_use]
    pub const fn file_count_reason(&self) -> Option<ReportReasonCode> {
        self.file_count_reason
    }

    /// Returns whether scan metadata was exhausted while building this report.
    #[must_use]
    pub const fn scan_metadata_exhausted(&self) -> bool {
        self.scan_metadata_exhausted
    }

    /// Returns known source usage status for the workflow scope that produced this report.
    #[must_use]
    pub const fn usage_status(&self) -> SourceUsageStatus {
        self.usage_status
    }

    /// Returns selected output names known to use this source.
    #[must_use]
    pub fn used_by_output_names(&self) -> &[String] {
        &self.used_by_output_names
    }

    /// Returns provider read statistics when planning or execution made them available.
    #[must_use]
    pub const fn provider_read_stats(&self) -> Option<&DeltaProviderReadStatsSnapshot> {
        self.provider_read_stats.as_ref()
    }

    /// Returns the stable reason code when provider read statistics are absent.
    #[must_use]
    pub const fn provider_stats_reason(&self) -> Option<ReportReasonCode> {
        self.provider_stats_reason
    }

    /// Returns durable phase timings captured while registering this source.
    #[must_use]
    pub fn phase_timings(&self) -> &[PhaseTimingReport] {
        &self.phase_timings
    }
}

#[cfg(test)]
mod tests {
    use super::SourceUsageStatus;

    #[test]
    fn source_usage_status_exposes_stable_codes() {
        assert_eq!(SourceUsageStatus::Used.as_str(), "used");
        assert_eq!(SourceUsageStatus::NotUsed.as_str(), "not_used");
        assert_eq!(SourceUsageStatus::Unknown.as_str(), "unknown");
        assert_eq!(SourceUsageStatus::Unknown.to_string(), "unknown");
    }
}
