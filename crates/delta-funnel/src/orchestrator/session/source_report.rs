use std::{collections::BTreeMap, fmt};

use crate::{
    DeltaFunnelError, DeltaProtocolReport, DeltaProviderReaderBackend,
    DeltaProviderScanExecutionOptions, QueryOptions, ReportReasonCode,
    collect_delta_provider_read_stats,
};

use super::{
    DeltaFunnelSession, LazyTable, MssqlDryRunOutputReport, PlannedMssqlOutput,
    RegisteredSessionSource, errors::datafusion_handoff_setup_error,
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
    file_count: crate::FileCount,
    file_count_reason: Option<ReportReasonCode>,
    scan_metadata_exhausted: bool,
    usage_status: SourceUsageStatus,
    used_by_output_names: Vec<String>,
    provider_read_stats: Option<crate::DeltaProviderReadStatsSnapshot>,
    provider_stats_reason: Option<ReportReasonCode>,
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
    pub(super) fn from_options(
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
    pub(super) fn metadata_only(
        source: &RegisteredSessionSource,
        scheduling: DeltaProviderSchedulingReport,
    ) -> Self {
        Self {
            source_name: source.name().to_owned(),
            source_uri: source.source_uri().to_owned(),
            snapshot_version: source.snapshot_version(),
            protocol: source.protocol().clone(),
            scheduling,
            file_count: crate::FileCount::unavailable(),
            file_count_reason: Some(ReportReasonCode::CostAvoidance),
            scan_metadata_exhausted: false,
            usage_status: SourceUsageStatus::Unknown,
            used_by_output_names: Vec::new(),
            provider_read_stats: None,
            provider_stats_reason: Some(ReportReasonCode::NotExecuted),
        }
    }

    pub(super) fn with_usage(
        mut self,
        usage_status: SourceUsageStatus,
        used_by_output_names: Vec<String>,
    ) -> Self {
        self.usage_status = usage_status;
        self.used_by_output_names = used_by_output_names;
        self
    }

    pub(super) fn with_provider_read_stats(
        mut self,
        stats: crate::DeltaProviderReadStatsSnapshot,
    ) -> Self {
        self.scan_metadata_exhausted = stats.scan_metadata_exhausted.unwrap_or(false);
        self.file_count = match stats.scan_metadata_exhausted {
            Some(true) => crate::FileCount::exact(stats.files_planned),
            Some(false) => crate::FileCount::estimated(stats.files_planned),
            None => crate::FileCount::unavailable(),
        };
        self.file_count_reason = match self.file_count {
            crate::FileCount::Exact(_) | crate::FileCount::Estimated(_) => None,
            crate::FileCount::Unavailable => Some(ReportReasonCode::CapabilityUnavailable),
            crate::FileCount::Skipped | crate::FileCount::NotExecuted => {
                Some(ReportReasonCode::NotExecuted)
            }
        };
        self.provider_read_stats = Some(stats);
        self.provider_stats_reason = None;
        self
    }

    pub(super) fn with_provider_stats_reason(mut self, reason: ReportReasonCode) -> Self {
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
    pub const fn file_count(&self) -> crate::FileCount {
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
    pub const fn provider_read_stats(&self) -> Option<&crate::DeltaProviderReadStatsSnapshot> {
        self.provider_read_stats.as_ref()
    }

    /// Returns the stable reason code when provider read statistics are absent.
    #[must_use]
    pub const fn provider_stats_reason(&self) -> Option<ReportReasonCode> {
        self.provider_stats_reason
    }
}

impl DeltaFunnelSession {
    /// Returns registered Delta source reports in registration order.
    #[must_use]
    pub fn sources(&self) -> &[RegisteredSessionSource] {
        &self.sources
    }

    /// Returns metadata-only Delta source readiness reports in registration order.
    #[must_use]
    pub fn source_reports(&self) -> Vec<DeltaSourceReport> {
        let scheduling = self.delta_source_scheduling_report();
        self.sources
            .iter()
            .map(|source| DeltaSourceReport::metadata_only(source, scheduling))
            .collect()
    }

    /// Returns Delta source reports enriched from the physical plan for `table`.
    ///
    /// This method resolves the lazy table and builds a DataFusion physical plan
    /// so provider planning metadata can be reported. It does not execute row
    /// streams or contact SQL Server.
    ///
    /// # Errors
    ///
    /// Returns an error when the table is unknown or DataFusion cannot build the
    /// physical plan.
    pub async fn source_reports_for_lazy_table_plan(
        &self,
        table: &LazyTable,
    ) -> Result<Vec<DeltaSourceReport>, DeltaFunnelError> {
        let dataframe = self.dataframe_for_lazy_table(table).await?;
        let physical_plan = dataframe
            .create_physical_plan()
            .await
            .map_err(|error| datafusion_handoff_setup_error("physical_plan", error))?;
        let provider_stats = collect_delta_provider_read_stats(physical_plan.as_ref());

        Ok(self.source_reports_with_provider_read_stats(provider_stats))
    }

    fn source_reports_with_provider_read_stats(
        &self,
        provider_stats: Vec<crate::DeltaProviderReadStatsSnapshot>,
    ) -> Vec<DeltaSourceReport> {
        let scheduling = self.delta_source_scheduling_report();
        let mut provider_stats_by_source = BTreeMap::new();
        for stats in provider_stats {
            provider_stats_by_source.insert(stats.source_name.clone(), stats);
        }

        self.sources
            .iter()
            .map(|source| {
                let report = DeltaSourceReport::metadata_only(source, scheduling);
                if let Some(stats) = provider_stats_by_source.remove(source.name()) {
                    report.with_provider_read_stats(stats)
                } else {
                    report
                }
            })
            .collect()
    }

    pub(super) fn source_reports_for_dry_run_outputs(
        &self,
        outputs: &[MssqlDryRunOutputReport],
    ) -> Result<Vec<DeltaSourceReport>, DeltaFunnelError> {
        self.source_reports_for_output_tables(outputs.iter().map(|output| {
            (
                output.output_name().to_owned(),
                output.planned_output().table(),
            )
        }))
    }

    pub(super) fn source_reports_for_dry_run_outputs_with_provider_stats(
        &self,
        outputs: &[MssqlDryRunOutputReport],
        provider_stats: Vec<crate::DeltaProviderReadStatsSnapshot>,
    ) -> Result<Vec<DeltaSourceReport>, DeltaFunnelError> {
        let mut provider_stats_by_source = BTreeMap::new();
        for stats in provider_stats {
            provider_stats_by_source.insert(stats.source_name.clone(), stats);
        }

        Ok(self
            .source_reports_for_dry_run_outputs(outputs)?
            .into_iter()
            .map(|report| {
                if let Some(stats) = provider_stats_by_source.remove(report.source_name()) {
                    report.with_provider_read_stats(stats)
                } else {
                    report.with_provider_stats_reason(ReportReasonCode::CapabilityUnavailable)
                }
            })
            .collect())
    }

    pub(super) async fn provider_read_stats_for_dry_run_outputs(
        &self,
        outputs: &[MssqlDryRunOutputReport],
    ) -> Result<Vec<crate::DeltaProviderReadStatsSnapshot>, DeltaFunnelError> {
        let mut provider_stats = Vec::new();
        for output in outputs {
            provider_stats.extend(
                self.provider_read_stats_for_lazy_table(output.planned_output().table())
                    .await?,
            );
        }
        Ok(provider_stats)
    }

    async fn provider_read_stats_for_lazy_table(
        &self,
        table: &LazyTable,
    ) -> Result<Vec<crate::DeltaProviderReadStatsSnapshot>, DeltaFunnelError> {
        let dataframe = self.dataframe_for_lazy_table(table).await?;
        let physical_plan = dataframe
            .create_physical_plan()
            .await
            .map_err(|error| datafusion_handoff_setup_error("physical_plan", error))?;

        Ok(collect_delta_provider_read_stats(physical_plan.as_ref()))
    }

    pub(super) fn source_reports_for_planned_outputs_with_provider_stats(
        &self,
        outputs: &[PlannedMssqlOutput],
        provider_stats: Vec<crate::DeltaProviderReadStatsSnapshot>,
    ) -> Result<Vec<DeltaSourceReport>, DeltaFunnelError> {
        let mut provider_stats_by_source = BTreeMap::new();
        for stats in provider_stats {
            provider_stats_by_source.insert(stats.source_name.clone(), stats);
        }

        Ok(self
            .source_reports_for_output_tables(outputs.iter().map(|output| {
                (
                    output.output_plan().output_name().to_owned(),
                    output.table(),
                )
            }))?
            .into_iter()
            .map(|report| {
                if let Some(stats) = provider_stats_by_source.remove(report.source_name()) {
                    report.with_provider_read_stats(stats)
                } else {
                    report.with_provider_stats_reason(ReportReasonCode::CapabilityUnavailable)
                }
            })
            .collect())
    }

    fn source_reports_for_output_tables<'a>(
        &self,
        outputs: impl IntoIterator<Item = (String, &'a LazyTable)>,
    ) -> Result<Vec<DeltaSourceReport>, DeltaFunnelError> {
        let outputs = outputs.into_iter().collect::<Vec<_>>();
        let mut output_sources = Vec::with_capacity(outputs.len());
        let mut all_usage_known = true;

        for (output_name, table) in outputs {
            match self.known_source_dependencies_for_table(table)? {
                Some(source_ids) => {
                    output_sources.push((output_name, source_ids));
                }
                None => {
                    all_usage_known = false;
                }
            }
        }

        Ok(self
            .sources
            .iter()
            .map(|source| {
                let used_by_output_names = output_sources
                    .iter()
                    .filter(|(_, source_ids)| source_ids.contains(&source.table().id()))
                    .map(|(output_name, _)| output_name.clone())
                    .collect::<Vec<_>>();
                let usage_status = if used_by_output_names.is_empty() {
                    if all_usage_known {
                        SourceUsageStatus::NotUsed
                    } else {
                        SourceUsageStatus::Unknown
                    }
                } else {
                    SourceUsageStatus::Used
                };

                DeltaSourceReport::metadata_only(source, self.delta_source_scheduling_report())
                    .with_usage(usage_status, used_by_output_names)
            })
            .collect())
    }

    fn delta_source_scheduling_report(&self) -> DeltaProviderSchedulingReport {
        DeltaProviderSchedulingReport::from_options(
            self.options.query_options(),
            self.options.provider_scan_options(),
        )
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
