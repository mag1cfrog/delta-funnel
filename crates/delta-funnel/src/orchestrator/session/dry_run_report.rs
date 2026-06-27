use crate::{
    DeltaFunnelError, PhaseTimingReport, ReportReasonCode,
    report::PhaseTimer,
    report::sql_server::{
        MssqlDryRunOutputReport, MssqlDryRunSqlIdentityReport, MssqlDryRunWorkflowReport,
    },
};

use super::{
    DeltaFunnelSession, LazyTable, LazyTableKind, OutputWritePlan, PlannedMssqlOutput, RunMode,
    SourceUsageStatus, sql_server_workflows::ensure_unique_write_all_output_names,
};

pub(super) fn stable_sql_identity_hash(sql: &str) -> String {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in sql.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    format!("{hash:016x}")
}

const SQL_TARGET_PLANNING_PHASE: &str = "sql_target_planning";
const QUERY_EXECUTION_PHASE: &str = "query_execution";
const BATCH_SHAPING_PHASE: &str = "batch_shaping";
const SQL_WRITE_PHASE: &str = "sql_write";
const FINALIZE_PHASE: &str = "finalize";
const VALIDATION_PHASE: &str = "validation";

impl DeltaFunnelSession {
    /// Dry-runs one selected lazy table as an MSSQL output.
    ///
    /// The method reuses the same session output planner as execute mode, then
    /// stops before physical DataFusion planning, row production, SQL Server
    /// lifecycle work, bulk writer construction, or row writes.
    ///
    /// # Errors
    ///
    /// Returns an MSSQL planning error when the request is not in
    /// [`RunMode::DryRun`], or the first error from session output planning.
    pub fn dry_run_to_mssql(
        &self,
        request: &OutputWritePlan,
    ) -> Result<MssqlDryRunOutputReport, DeltaFunnelError> {
        ensure_dry_run_mode(request.target().run_mode())?;

        self.plan_dry_run_output(request)
    }

    /// Dry-runs multiple selected lazy tables as one MSSQL output workflow.
    ///
    /// The method plans each selected output in caller-provided order and stops
    /// before cache materialization, physical DataFusion planning, row
    /// production, SQL Server lifecycle work, bulk writer construction, or row
    /// writes.
    ///
    /// # Errors
    ///
    /// Returns the first duplicate-output, run-mode, or output-planning error.
    pub fn dry_run_all_to_mssql(
        &self,
        requests: &[OutputWritePlan],
    ) -> Result<MssqlDryRunWorkflowReport, DeltaFunnelError> {
        let outputs = self.plan_dry_run_all_outputs(requests)?;
        let sources = self.source_reports_for_dry_run_outputs(&outputs)?;

        Ok(MssqlDryRunWorkflowReport::new(outputs, sources))
    }

    /// Dry-runs multiple selected lazy tables and honors source scan-summary options.
    ///
    /// This method is async because
    /// [`crate::DryRunScanSummaryMode::ExhaustScanMetadata`] requires
    /// DataFusion physical planning to expose provider scan metadata. It still
    /// stops before row production, SQL Server lifecycle work, bulk writer
    /// construction, or row writes.
    ///
    /// # Errors
    ///
    /// Returns the first duplicate-output, run-mode, output-planning, or
    /// DataFusion physical-planning error.
    pub async fn dry_run_all_to_mssql_with_scan_summary(
        &self,
        requests: &[OutputWritePlan],
    ) -> Result<MssqlDryRunWorkflowReport, DeltaFunnelError> {
        let outputs = self.plan_dry_run_all_outputs(requests)?;
        let sources = match self
            .options
            .validation_options()
            .dry_run_scan_summary_mode()
        {
            crate::DryRunScanSummaryMode::MetadataOnly => {
                self.source_reports_for_dry_run_outputs(&outputs)?
            }
            crate::DryRunScanSummaryMode::ExhaustScanMetadata => self
                .source_reports_for_dry_run_outputs_with_provider_stats(
                    &outputs,
                    self.provider_read_stats_for_dry_run_outputs(&outputs)
                        .await?,
                )?,
        };

        Ok(MssqlDryRunWorkflowReport::new(outputs, sources))
    }

    fn plan_dry_run_all_outputs(
        &self,
        requests: &[OutputWritePlan],
    ) -> Result<Vec<MssqlDryRunOutputReport>, DeltaFunnelError> {
        ensure_unique_write_all_output_names(requests)?;

        requests
            .iter()
            .map(|request| {
                ensure_write_all_dry_run_mode(request.target().run_mode())?;

                self.plan_dry_run_output(request)
            })
            .collect()
    }

    fn plan_dry_run_output(
        &self,
        request: &OutputWritePlan,
    ) -> Result<MssqlDryRunOutputReport, DeltaFunnelError> {
        let planning_timer = PhaseTimer::start(SQL_TARGET_PLANNING_PHASE);
        let planned = self.plan_mssql_output(request)?;
        let planning_timing = planning_timer.completed();

        self.dry_run_output_report_for_plan(planned, dry_run_output_phase_timings(planning_timing))
    }

    fn dry_run_output_report_for_plan(
        &self,
        planned_output: PlannedMssqlOutput,
        phase_timings: Vec<PhaseTimingReport>,
    ) -> Result<MssqlDryRunOutputReport, DeltaFunnelError> {
        let sql_identity = self.sql_identity_for_lazy_table(planned_output.table());
        let (source_usage_status, used_source_names) =
            self.source_usage_for_lazy_table(planned_output.table())?;
        Ok(MssqlDryRunOutputReport::new(
            planned_output,
            sql_identity,
            source_usage_status,
            used_source_names,
            phase_timings,
        ))
    }

    fn source_usage_for_lazy_table(
        &self,
        table: &LazyTable,
    ) -> Result<(SourceUsageStatus, Vec<String>), DeltaFunnelError> {
        let Some(source_ids) = self.known_source_dependencies_for_table(table)? else {
            return Ok((SourceUsageStatus::Unknown, Vec::new()));
        };

        let used_source_names = self
            .sources
            .iter()
            .filter(|source| source_ids.contains(&source.table().id()))
            .map(|source| source.name().to_owned())
            .collect::<Vec<_>>();
        let source_usage_status = if used_source_names.is_empty() {
            SourceUsageStatus::NotUsed
        } else {
            SourceUsageStatus::Used
        };

        Ok((source_usage_status, used_source_names))
    }

    fn sql_identity_for_lazy_table(&self, table: &LazyTable) -> MssqlDryRunSqlIdentityReport {
        if table.kind() != LazyTableKind::DerivedSql {
            return MssqlDryRunSqlIdentityReport::absent();
        }

        match self.sql_text_for_derived_table(table) {
            Ok(sql_text) => {
                MssqlDryRunSqlIdentityReport::present(stable_sql_identity_hash(sql_text))
            }
            Err(_) => {
                MssqlDryRunSqlIdentityReport::unavailable(ReportReasonCode::CapabilityUnavailable)
            }
        }
    }
}

fn dry_run_output_phase_timings(planning_timing: PhaseTimingReport) -> Vec<PhaseTimingReport> {
    vec![
        planning_timing,
        PhaseTimingReport::skipped(QUERY_EXECUTION_PHASE, ReportReasonCode::DryRun),
        PhaseTimingReport::skipped(BATCH_SHAPING_PHASE, ReportReasonCode::DryRun),
        PhaseTimingReport::skipped(SQL_WRITE_PHASE, ReportReasonCode::DryRun),
        PhaseTimingReport::skipped(FINALIZE_PHASE, ReportReasonCode::DryRun),
        PhaseTimingReport::skipped(VALIDATION_PHASE, ReportReasonCode::DryRun),
    ]
}

fn ensure_dry_run_mode(run_mode: RunMode) -> Result<(), DeltaFunnelError> {
    match run_mode {
        RunMode::DryRun => Ok(()),
        RunMode::Execute => Err(DeltaFunnelError::MssqlWorkflowPlanning {
            message: "dry_run_to_mssql requires RunMode::DryRun; use write_to_mssql for execution"
                .to_owned(),
        }),
    }
}

fn ensure_write_all_dry_run_mode(run_mode: RunMode) -> Result<(), DeltaFunnelError> {
    match run_mode {
        RunMode::DryRun => Ok(()),
        RunMode::Execute => Err(DeltaFunnelError::MssqlWorkflowPlanning {
            message: "dry_run_all_to_mssql requires RunMode::DryRun; use write_all for execution"
                .to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use crate::{
        DeltaFunnelError, DeltaSourceConfig, LoadMode, MssqlOutputTarget, MssqlTargetConfig,
        MssqlTargetTable, OutputStatus, ReportReasonCode, ValidationOptions, ValidationStatus,
        WorkflowStatus,
    };

    use super::super::{
        DeltaFunnelSession, LazyTableKind, OutputWritePlan, RunMode, SessionOptions,
        SourceUsageStatus,
        test_support::{
            DeltaLogTable, execute_output_request, output_request, override_connection,
            scan_counting_marker_region_provider, secret_connection,
        },
    };
    use super::{
        BATCH_SHAPING_PHASE, FINALIZE_PHASE, QUERY_EXECUTION_PHASE, SQL_TARGET_PLANNING_PHASE,
        SQL_WRITE_PHASE, VALIDATION_PHASE, stable_sql_identity_hash,
    };
    use crate::MssqlDryRunSqlIdentityState;

    #[test]
    fn sql_identity_status_and_hash_are_stable() {
        assert_eq!(MssqlDryRunSqlIdentityState::Present.as_str(), "present");
        assert_eq!(MssqlDryRunSqlIdentityState::Absent.to_string(), "absent");
        assert_eq!(
            MssqlDryRunSqlIdentityState::Unavailable.as_str(),
            "unavailable"
        );
        assert_eq!(
            stable_sql_identity_hash("select marker where region = 'west'"),
            "cbd6889e027b0f88"
        );
    }

    fn phase_timing<'a>(
        report: &'a crate::MssqlDryRunOutputReport,
        phase_name: &str,
    ) -> Result<&'a crate::PhaseTimingReport, Box<dyn std::error::Error>> {
        report
            .phase_timings()
            .iter()
            .find(|timing| timing.phase_name() == phase_name)
            .ok_or_else(|| format!("missing phase timing `{phase_name}`").into())
    }

    #[tokio::test]
    async fn dry_run_to_mssql_plans_output_without_row_or_writer_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let output = session
            .table_from_sql("select marker from orders_source where region = 'west'")
            .await?;
        let request = output_request(
            output,
            "west_output",
            "west_orders",
            LoadMode::CreateAndLoad,
        )?;

        let report = session.dry_run_to_mssql(&request)?;

        assert_eq!(report.output_name(), "west_output");
        assert_eq!(report.run_mode(), RunMode::DryRun);
        assert_eq!(report.status(), OutputStatus::dry_run_planned());
        assert_eq!(
            report.validation_status(),
            ValidationStatus::skipped(ReportReasonCode::DryRun)
        );
        assert_eq!(report.target_table().schema(), Some("dbo"));
        assert_eq!(report.target_table().table(), "west_orders");
        assert_eq!(report.load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(report.target_schema_plan().mappings().len(), 1);
        assert!(report.target_ddl_plan().create_table_sql().is_some());
        assert!(report.target_lifecycle_plan().create_table_sql_required());
        assert_eq!(
            report.target_lifecycle_plan().expected_target_state(),
            crate::MssqlTargetTableState::Absent
        );
        assert_eq!(
            report.planned_output().output_plan().target_table().table(),
            "west_orders"
        );
        assert_eq!(
            report
                .planned_output()
                .output_plan()
                .schema_mappings()
                .len(),
            1
        );
        assert_eq!(report.output_schema().len(), 1);
        assert_eq!(report.output_schema()[0].index(), 0);
        assert_eq!(report.output_schema()[0].name(), "marker");
        assert_eq!(report.output_schema()[0].arrow_type(), "Utf8");
        assert!(!report.output_schema()[0].nullable());
        assert_eq!(report.source_usage_status(), SourceUsageStatus::NotUsed);
        assert!(report.used_source_names().is_empty());
        assert_eq!(report.output_row_count(), crate::RowCount::unavailable());
        assert_eq!(
            report.output_row_count_reason(),
            Some(ReportReasonCode::NotExecuted)
        );
        assert_eq!(
            report.sql_identity().state(),
            MssqlDryRunSqlIdentityState::Present
        );
        assert_eq!(report.sql_identity().hash(), Some("a65390dacb7eb6f1"));
        assert_eq!(report.sql_identity().reason(), None);
        let debug = format!("{report:?}");
        assert!(debug.contains("a65390dacb7eb6f1"));
        assert!(!debug.contains("select marker"));
        assert!(!debug.contains("region = 'west'"));
        assert!(!report.sql_server_contacted());
        assert!(!report.row_production_started());
        assert!(!report.table_lifecycle_started());
        assert!(!report.bulk_writer_started());
        assert_eq!(report.phase_timings().len(), 6);
        let planning = phase_timing(&report, SQL_TARGET_PLANNING_PHASE)?;
        assert!(planning.status().is_completed());
        assert!(planning.elapsed_micros().is_some());
        for phase_name in [
            QUERY_EXECUTION_PHASE,
            BATCH_SHAPING_PHASE,
            SQL_WRITE_PHASE,
            FINALIZE_PHASE,
            VALIDATION_PHASE,
        ] {
            let timing = phase_timing(&report, phase_name)?;
            assert_eq!(
                timing.status(),
                crate::PhaseStatus::skipped(ReportReasonCode::DryRun)
            );
            assert_eq!(timing.elapsed_micros(), None);
        }
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_reports_all_outputs_without_row_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let west = session
            .table_from_sql("select marker from orders_source where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from orders_source where region = 'east'")
            .await?;
        let west_table_id = west.id();
        let west_table_name = west.name().to_owned();
        let west = output_request(west, "west_output", "west_orders", LoadMode::CreateAndLoad)?;
        let east = output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;

        let report = session.dry_run_all_to_mssql(&[west, east])?;

        assert_eq!(report.run_mode(), RunMode::DryRun);
        assert_eq!(report.status(), WorkflowStatus::success());
        assert_eq!(report.len(), 2);
        assert!(!report.is_empty());
        assert_eq!(report.outputs()[0].output_name(), "west_output");
        assert_eq!(report.outputs()[1].output_name(), "east_output");
        assert_eq!(report.outputs()[0].table_id(), west_table_id);
        assert_eq!(report.outputs()[0].table_kind(), LazyTableKind::DerivedSql);
        assert_eq!(report.outputs()[0].table_name(), west_table_name);
        assert_eq!(
            report.outputs()[0].status(),
            OutputStatus::dry_run_planned()
        );
        assert_eq!(
            report.outputs()[0].validation_status(),
            ValidationStatus::skipped(ReportReasonCode::DryRun)
        );
        assert_eq!(
            report.outputs()[0].output_row_count(),
            crate::RowCount::unavailable()
        );
        assert_eq!(
            report.outputs()[0].output_row_count_reason(),
            Some(ReportReasonCode::NotExecuted)
        );
        assert!(report.sources().is_empty());
        assert_eq!(report.outputs()[0].target_table().table(), "west_orders");
        assert_eq!(report.outputs()[0].load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(
            report.outputs()[0]
                .target_lifecycle_plan()
                .expected_target_state(),
            crate::MssqlTargetTableState::Absent
        );
        assert_eq!(report.outputs()[1].target_table().table(), "east_orders");
        assert_eq!(report.outputs()[1].load_mode(), LoadMode::AppendExisting);
        assert_eq!(
            report.outputs()[1]
                .target_lifecycle_plan()
                .expected_target_state(),
            crate::MssqlTargetTableState::Exists
        );
        assert!(!report.sql_server_contacted());
        assert!(!report.row_production_started());
        assert!(!report.table_lifecycle_started());
        assert!(!report.bulk_writer_started());
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_includes_registered_delta_source_reports()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::CreateAndLoad,
        )?;

        let report = session.dry_run_all_to_mssql(&[request])?;

        assert_eq!(report.outputs().len(), 1);
        assert!(!report.query_used_source_scan_metadata_exhausted());
        assert_eq!(
            report.outputs()[0].sql_identity().state(),
            MssqlDryRunSqlIdentityState::Absent
        );
        assert_eq!(report.outputs()[0].sql_identity().hash(), None);
        assert_eq!(report.outputs()[0].sql_identity().reason(), None);
        assert_eq!(
            report.outputs()[0].source_usage_status(),
            SourceUsageStatus::Used
        );
        assert_eq!(
            report.outputs()[0].used_source_names(),
            &["orders".to_owned()]
        );
        assert_eq!(report.sources().len(), 1);
        let source = &report.sources()[0];
        assert_eq!(source.source_name(), "orders");
        assert_eq!(source.snapshot_version(), 1);
        assert_eq!(source.protocol().source_name, "orders");
        assert_eq!(source.file_count(), crate::FileCount::unavailable());
        assert_eq!(
            source.file_count_reason(),
            Some(crate::ReportReasonCode::CostAvoidance)
        );
        assert!(!source.scan_metadata_exhausted());
        assert_eq!(source.usage_status(), SourceUsageStatus::Used);
        assert_eq!(source.used_by_output_names(), &["orders_output".to_owned()]);
        assert!(!report.row_production_started());
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_scan_summary_exhausts_provider_metadata_without_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new()
                .with_default_mssql_connection(secret_connection()?)
                .with_validation_options(ValidationOptions::new().with_dry_run_scan_summary_mode(
                    crate::DryRunScanSummaryMode::ExhaustScanMetadata,
                )),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::CreateAndLoad,
        )?;

        let report = session
            .dry_run_all_to_mssql_with_scan_summary(&[request])
            .await?;

        assert_eq!(report.outputs().len(), 1);
        assert!(!report.outputs()[0].row_production_started());
        assert_eq!(report.sources().len(), 1);
        let source = &report.sources()[0];
        assert_eq!(source.source_name(), "orders");
        assert_eq!(source.usage_status(), SourceUsageStatus::Used);
        assert_eq!(source.used_by_output_names(), &["orders_output".to_owned()]);
        assert_eq!(source.provider_stats_reason(), None);
        let stats = source
            .provider_read_stats()
            .ok_or("expected provider stats from dry-run scan summary")?;
        assert_eq!(stats.source_name, "orders");
        assert_eq!(stats.files_started, 0);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.batches_produced, 0);
        assert_eq!(stats.rows_produced, 0);
        match stats.scan_metadata_exhausted {
            Some(true) => {
                assert_eq!(
                    source.file_count(),
                    crate::FileCount::exact(stats.files_planned)
                );
                assert_eq!(source.file_count_reason(), None);
            }
            Some(false) => {
                assert_eq!(
                    source.file_count(),
                    crate::FileCount::estimated(stats.files_planned)
                );
                assert_eq!(source.file_count_reason(), None);
            }
            None => {
                assert_eq!(source.file_count(), crate::FileCount::unavailable());
                assert_eq!(
                    source.file_count_reason(),
                    Some(crate::ReportReasonCode::CapabilityUnavailable)
                );
            }
        }
        assert_eq!(
            report.query_used_source_scan_metadata_exhausted(),
            source.scan_metadata_exhausted()
        );
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_reports_multi_source_usage_when_lineage_is_known()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders_table = DeltaLogTable::new("orders")?;
        let customers_table = DeltaLogTable::new("customers")?;
        let inventory_table = DeltaLogTable::new("inventory")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new("orders", orders_table.uri()))?;
        session.delta_lake(DeltaSourceConfig::new("customers", customers_table.uri()))?;
        session.delta_lake(DeltaSourceConfig::new("inventory", inventory_table.uri()))?;
        let joined = session
            .table_from_sql(
                "select orders.id from orders inner join customers on orders.id = customers.id",
            )
            .await?;
        let request = output_request(
            joined,
            "joined_output",
            "joined_sink",
            LoadMode::CreateAndLoad,
        )?;

        let report = session.dry_run_all_to_mssql(&[request])?;

        assert!(!report.query_used_source_scan_metadata_exhausted());
        assert_eq!(
            report.outputs()[0].source_usage_status(),
            SourceUsageStatus::Used
        );
        assert_eq!(
            report.outputs()[0].used_source_names(),
            &["orders".to_owned(), "customers".to_owned()]
        );
        assert_eq!(report.sources().len(), 3);
        for source in report.sources() {
            match source.source_name() {
                "orders" | "customers" => {
                    assert_eq!(source.usage_status(), SourceUsageStatus::Used);
                    assert_eq!(source.used_by_output_names(), &["joined_output".to_owned()]);
                }
                "inventory" => {
                    assert_eq!(source.usage_status(), SourceUsageStatus::NotUsed);
                    assert!(source.used_by_output_names().is_empty());
                }
                name => return Err(format!("unexpected source report: {name}").into()),
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_rejects_execute_request_before_row_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let output = session
            .table_from_sql("select marker from orders_source")
            .await?;
        let request = execute_output_request(
            output,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.dry_run_all_to_mssql(&[request]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("dry_run_all_to_mssql requires RunMode::DryRun")
        ));
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_rejects_missing_connection_before_row_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let output = session
            .table_from_sql("select marker from orders_source")
            .await?;
        let request = output_request(
            output,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.dry_run_all_to_mssql(&[request]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "orders_output"
        ));
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_all_to_mssql_rejects_duplicate_output_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west = output_request(
            west,
            "orders_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = output_request(
            east,
            "orders_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let error = session.dry_run_all_to_mssql(&[west, east]);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("write_all output names must be unique")
                    && message.contains("orders_output")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_to_mssql_rejects_execute_request_before_planning()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let output = session.table_from_sql("select 1 as id").await?;
        let request = execute_output_request(
            output,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.dry_run_to_mssql(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("dry_run_to_mssql requires RunMode::DryRun")
        ));
        Ok(())
    }

    #[test]
    fn dry_run_to_mssql_rejects_missing_connection_before_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.dry_run_to_mssql(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "orders_output"
        ));
        Ok(())
    }

    #[test]
    fn dry_run_to_mssql_rejects_replace_before_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(source, "orders_output", "orders_sink", LoadMode::Replace)?;

        let error = session.dry_run_to_mssql(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlLifecyclePlanning { output_name, message })
                if output_name == "orders_output" && message.contains("replace load mode")
        ));
        Ok(())
    }

    #[test]
    fn dry_run_to_mssql_report_debug_redacts_connection_material()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders_sink")?)
            .with_connection(override_connection()?);
        let request = OutputWritePlan::new(
            source,
            MssqlOutputTarget::new("orders_output", target_config, RunMode::DryRun),
        );

        let report = session.dry_run_to_mssql(&request)?;
        let debug = format!("{report:?}");

        assert!(debug.contains("orders_output"));
        assert!(debug.contains("warehouse-override"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("override-secret"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        Ok(())
    }
}
