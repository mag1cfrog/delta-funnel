use std::collections::BTreeMap;

use crate::{
    DeltaFunnelError, ReportReasonCode, collect_delta_provider_read_stats,
    report::delta::{DeltaProviderSchedulingReport, DeltaSourceReport, SourceUsageStatus},
};

use super::{
    DeltaFunnelSession, LazyTable, MssqlDryRunOutputReport, PlannedMssqlOutput,
    RegisteredSessionSource, errors::datafusion_handoff_setup_error,
};

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
            .map(|source| delta_source_report_metadata_only(source, scheduling))
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
                let report = delta_source_report_metadata_only(source, scheduling);
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

                delta_source_report_metadata_only(source, self.delta_source_scheduling_report())
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

fn delta_source_report_metadata_only(
    source: &RegisteredSessionSource,
    scheduling: DeltaProviderSchedulingReport,
) -> DeltaSourceReport {
    DeltaSourceReport::metadata_only(
        source.name(),
        source.source_uri(),
        source.snapshot_version(),
        source.protocol().clone(),
        scheduling,
    )
    .with_phase_timings(source.phase_timings().to_vec())
}

#[cfg(test)]
mod tests {
    use super::super::{SessionOptions, test_support::DeltaLogTable};
    use super::DeltaFunnelSession;
    use crate::{DeltaSourceConfig, FileCount, ReportReasonCode};

    #[tokio::test]
    async fn source_reports_for_lazy_table_plan_include_provider_stats_without_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let reports = session.source_reports_for_lazy_table_plan(&source).await?;

        assert_eq!(reports.len(), 1);
        let report = &reports[0];
        assert_eq!(report.source_name(), "orders");
        assert_eq!(report.provider_stats_reason(), None);
        let stats = report
            .provider_read_stats()
            .ok_or("expected provider read stats")?;
        assert_eq!(stats.source_name, "orders");
        assert_eq!(stats.snapshot_version, report.snapshot_version());
        assert_eq!(stats.files_started, 0);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.batches_produced, 0);
        assert_eq!(stats.rows_produced, 0);
        match stats.scan_metadata_exhausted {
            Some(true) => {
                assert_eq!(report.file_count(), FileCount::exact(stats.files_planned));
                assert_eq!(report.file_count_reason(), None);
                assert!(report.scan_metadata_exhausted());
            }
            Some(false) => {
                assert_eq!(
                    report.file_count(),
                    FileCount::estimated(stats.files_planned)
                );
                assert_eq!(report.file_count_reason(), None);
                assert!(!report.scan_metadata_exhausted());
            }
            None => {
                assert_eq!(report.file_count(), FileCount::unavailable());
                assert_eq!(
                    report.file_count_reason(),
                    Some(ReportReasonCode::CapabilityUnavailable)
                );
                assert!(!report.scan_metadata_exhausted());
            }
        }
        Ok(())
    }
}
