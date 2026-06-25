use std::fmt;

use crate::{
    DeltaFunnelError, LoadMode, MssqlDdlPlan, MssqlLifecyclePlan, MssqlSchemaPlan,
    MssqlTargetTable, OutputStatus, ReportReasonCode, ValidationStatus, WorkflowStatus,
};

use super::{
    DeltaFunnelSession, DeltaSourceReport, LazyTable, LazyTableKind, OutputWritePlan,
    PlannedMssqlOutput, RunMode, SourceUsageStatus, mssql::ensure_unique_write_all_output_names,
};

/// Output schema field included in an MSSQL dry-run report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlDryRunOutputFieldReport {
    index: u64,
    name: String,
    arrow_type: String,
    nullable: bool,
}

impl MssqlDryRunOutputFieldReport {
    pub(super) fn from_mapping(mapping: &arrow_tiberius::SchemaMapping) -> Self {
        Self {
            index: crate::usize_to_u64_saturating(mapping.arrow().index()),
            name: mapping.arrow().name().to_owned(),
            arrow_type: mapping.arrow().data_type().to_string(),
            nullable: mapping.arrow().nullable(),
        }
    }

    /// Returns the zero-based output field index.
    #[must_use]
    pub const fn index(&self) -> u64 {
        self.index
    }

    /// Returns the output field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the Arrow data type as a stable display string.
    #[must_use]
    pub fn arrow_type(&self) -> &str {
        &self.arrow_type
    }

    /// Returns true when the output field is nullable.
    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

/// SQL identity state included in an MSSQL dry-run output report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MssqlDryRunSqlIdentityState {
    /// A stable SQL identity hash is available.
    Present,
    /// No SQL identity applies to the selected lazy table.
    Absent,
    /// A SQL identity applies, but could not be reported from available metadata.
    Unavailable,
}

impl MssqlDryRunSqlIdentityState {
    /// Returns a stable lower-snake-case code for report serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Absent => "absent",
            Self::Unavailable => "unavailable",
        }
    }
}

impl fmt::Display for MssqlDryRunSqlIdentityState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Redacted SQL identity included in an MSSQL dry-run output report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlDryRunSqlIdentityReport {
    state: MssqlDryRunSqlIdentityState,
    hash: Option<String>,
    reason: Option<ReportReasonCode>,
}

impl MssqlDryRunSqlIdentityReport {
    pub(super) fn present(hash: String) -> Self {
        Self {
            state: MssqlDryRunSqlIdentityState::Present,
            hash: Some(hash),
            reason: None,
        }
    }

    pub(super) fn absent() -> Self {
        Self {
            state: MssqlDryRunSqlIdentityState::Absent,
            hash: None,
            reason: None,
        }
    }

    pub(super) fn unavailable(reason: ReportReasonCode) -> Self {
        Self {
            state: MssqlDryRunSqlIdentityState::Unavailable,
            hash: None,
            reason: Some(reason),
        }
    }

    /// Returns whether a SQL identity hash is present, absent, or unavailable.
    #[must_use]
    pub const fn state(&self) -> MssqlDryRunSqlIdentityState {
        self.state
    }

    /// Returns the stable SQL identity hash when retained SQL is available.
    #[must_use]
    pub fn hash(&self) -> Option<&str> {
        self.hash.as_deref()
    }

    /// Returns the reason when SQL identity reporting is unavailable.
    #[must_use]
    pub const fn reason(&self) -> Option<ReportReasonCode> {
        self.reason
    }
}

/// Dry-run planning report for one selected MSSQL output.
///
/// This report is produced after the session has resolved the output schema and
/// planned the SQL Server target, but before any row production, SQL Server
/// lifecycle action, bulk writer construction, or validation I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlDryRunOutputReport {
    planned_output: PlannedMssqlOutput,
    output_schema: Vec<MssqlDryRunOutputFieldReport>,
    sql_identity: MssqlDryRunSqlIdentityReport,
    source_usage_status: SourceUsageStatus,
    used_source_names: Vec<String>,
    output_row_count: crate::RowCount,
    output_row_count_reason: Option<ReportReasonCode>,
    status: OutputStatus,
    validation_status: ValidationStatus,
    sql_server_contacted: bool,
    row_production_started: bool,
    table_lifecycle_started: bool,
    bulk_writer_started: bool,
}

impl MssqlDryRunOutputReport {
    pub(super) fn new(
        planned_output: PlannedMssqlOutput,
        sql_identity: MssqlDryRunSqlIdentityReport,
        source_usage_status: SourceUsageStatus,
        used_source_names: Vec<String>,
    ) -> Self {
        let output_schema = planned_output
            .output_plan()
            .schema_mappings()
            .iter()
            .map(MssqlDryRunOutputFieldReport::from_mapping)
            .collect();

        Self {
            planned_output,
            output_schema,
            sql_identity,
            source_usage_status,
            used_source_names,
            output_row_count: crate::RowCount::unavailable(),
            output_row_count_reason: Some(ReportReasonCode::NotExecuted),
            status: OutputStatus::dry_run_planned(),
            validation_status: ValidationStatus::skipped(ReportReasonCode::DryRun),
            sql_server_contacted: false,
            row_production_started: false,
            table_lifecycle_started: false,
            bulk_writer_started: false,
        }
    }

    /// Returns the planned output request and target plan.
    #[must_use]
    pub const fn planned_output(&self) -> &PlannedMssqlOutput {
        &self.planned_output
    }

    /// Returns the selected output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        self.planned_output.output_plan().output_name()
    }

    /// Returns the selected lazy table id.
    #[must_use]
    pub const fn table_id(&self) -> u64 {
        self.planned_output.table().id()
    }

    /// Returns the selected lazy table kind.
    #[must_use]
    pub const fn table_kind(&self) -> LazyTableKind {
        self.planned_output.table().kind()
    }

    /// Returns the selected lazy table name.
    #[must_use]
    pub fn table_name(&self) -> &str {
        self.planned_output.table().name()
    }

    /// Returns the planned Arrow output schema in output field order.
    #[must_use]
    pub fn output_schema(&self) -> &[MssqlDryRunOutputFieldReport] {
        &self.output_schema
    }

    /// Returns the planned SQL Server target table.
    #[must_use]
    pub fn target_table(&self) -> &MssqlTargetTable {
        self.planned_output.output_plan().target_table()
    }

    /// Returns the requested target load mode.
    #[must_use]
    pub fn load_mode(&self) -> LoadMode {
        self.planned_output.output_plan().load_mode()
    }

    /// Returns the planned Arrow-to-MSSQL schema mapping artifact.
    #[must_use]
    pub fn target_schema_plan(&self) -> &MssqlSchemaPlan {
        self.planned_output.output_plan().schema_plan()
    }

    /// Returns the planned SQL Server DDL artifact.
    #[must_use]
    pub fn target_ddl_plan(&self) -> &MssqlDdlPlan {
        self.planned_output.output_plan().ddl_plan()
    }

    /// Returns the planned SQL Server table lifecycle artifact.
    #[must_use]
    pub fn target_lifecycle_plan(&self) -> &MssqlLifecyclePlan {
        self.planned_output.output_plan().lifecycle_plan()
    }

    /// Returns the redacted SQL identity for the selected lazy table.
    #[must_use]
    pub const fn sql_identity(&self) -> &MssqlDryRunSqlIdentityReport {
        &self.sql_identity
    }

    /// Returns known source usage status for this selected output.
    #[must_use]
    pub const fn source_usage_status(&self) -> SourceUsageStatus {
        self.source_usage_status
    }

    /// Returns registered source names known to be used by this selected output.
    #[must_use]
    pub fn used_source_names(&self) -> &[String] {
        &self.used_source_names
    }

    /// Returns output row-count evidence for this dry-run output.
    #[must_use]
    pub const fn output_row_count(&self) -> crate::RowCount {
        self.output_row_count
    }

    /// Returns the stable reason code when output row count is unavailable.
    #[must_use]
    pub const fn output_row_count_reason(&self) -> Option<ReportReasonCode> {
        self.output_row_count_reason
    }

    /// Returns the dry-run output status.
    #[must_use]
    pub const fn status(&self) -> OutputStatus {
        self.status
    }

    /// Returns the target validation status for this dry-run output.
    #[must_use]
    pub const fn validation_status(&self) -> ValidationStatus {
        self.validation_status
    }

    /// Returns the dry-run action mode.
    #[must_use]
    pub const fn run_mode(&self) -> RunMode {
        RunMode::DryRun
    }

    /// Returns whether dry-run planning contacted SQL Server.
    #[must_use]
    pub const fn sql_server_contacted(&self) -> bool {
        self.sql_server_contacted
    }

    /// Returns whether dry-run planning started DataFusion row production.
    #[must_use]
    pub const fn row_production_started(&self) -> bool {
        self.row_production_started
    }

    /// Returns whether dry-run planning started SQL Server table lifecycle work.
    #[must_use]
    pub const fn table_lifecycle_started(&self) -> bool {
        self.table_lifecycle_started
    }

    /// Returns whether dry-run planning opened a SQL Server bulk writer.
    #[must_use]
    pub const fn bulk_writer_started(&self) -> bool {
        self.bulk_writer_started
    }
}

/// Dry-run planning report for a multi-output MSSQL workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlDryRunWorkflowReport {
    outputs: Vec<MssqlDryRunOutputReport>,
    sources: Vec<DeltaSourceReport>,
    status: WorkflowStatus,
}

impl MssqlDryRunWorkflowReport {
    pub(super) fn new(
        outputs: Vec<MssqlDryRunOutputReport>,
        sources: Vec<DeltaSourceReport>,
    ) -> Self {
        let status = if outputs.is_empty() {
            WorkflowStatus::no_op(ReportReasonCode::NotExecuted)
        } else {
            WorkflowStatus::success()
        };

        Self {
            outputs,
            sources,
            status,
        }
    }

    /// Returns the dry-run action mode.
    #[must_use]
    pub const fn run_mode(&self) -> RunMode {
        RunMode::DryRun
    }

    /// Returns the dry-run workflow status.
    #[must_use]
    pub const fn status(&self) -> WorkflowStatus {
        self.status
    }

    /// Returns the number of selected outputs represented by this report.
    #[must_use]
    pub fn len(&self) -> usize {
        self.outputs.len()
    }

    /// Returns whether this report contains no selected outputs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.outputs.is_empty()
    }

    /// Returns per-output dry-run reports in caller-provided order.
    #[must_use]
    pub fn outputs(&self) -> &[MssqlDryRunOutputReport] {
        &self.outputs
    }

    /// Returns source-level reports in session registration order.
    #[must_use]
    pub fn sources(&self) -> &[DeltaSourceReport] {
        &self.sources
    }

    /// Returns whether scan metadata was exhausted for every known query-used source.
    ///
    /// This returns false when no source is known to be used by the selected
    /// outputs, or when any used source only has metadata-only or unavailable
    /// scan evidence.
    #[must_use]
    pub fn query_used_source_scan_metadata_exhausted(&self) -> bool {
        let mut used_source_seen = false;
        for source in &self.sources {
            if source.usage_status() == SourceUsageStatus::Used {
                used_source_seen = true;
                if !source.scan_metadata_exhausted() {
                    return false;
                }
            }
        }

        used_source_seen
    }

    /// Returns whether dry-run planning contacted SQL Server for any output.
    #[must_use]
    pub fn sql_server_contacted(&self) -> bool {
        self.outputs
            .iter()
            .any(MssqlDryRunOutputReport::sql_server_contacted)
    }

    /// Returns whether dry-run planning started row production for any output.
    #[must_use]
    pub fn row_production_started(&self) -> bool {
        self.outputs
            .iter()
            .any(MssqlDryRunOutputReport::row_production_started)
    }

    /// Returns whether dry-run planning started table lifecycle work for any output.
    #[must_use]
    pub fn table_lifecycle_started(&self) -> bool {
        self.outputs
            .iter()
            .any(MssqlDryRunOutputReport::table_lifecycle_started)
    }

    /// Returns whether dry-run planning opened a bulk writer for any output.
    #[must_use]
    pub fn bulk_writer_started(&self) -> bool {
        self.outputs
            .iter()
            .any(MssqlDryRunOutputReport::bulk_writer_started)
    }
}

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
        let planned = self.plan_mssql_output(request)?;

        self.dry_run_output_report_for_plan(planned)
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
                let planned = self.plan_mssql_output(request)?;

                self.dry_run_output_report_for_plan(planned)
            })
            .collect()
    }

    fn dry_run_output_report_for_plan(
        &self,
        planned_output: PlannedMssqlOutput,
    ) -> Result<MssqlDryRunOutputReport, DeltaFunnelError> {
        let sql_identity = self.sql_identity_for_lazy_table(planned_output.table());
        let (source_usage_status, used_source_names) =
            self.source_usage_for_lazy_table(planned_output.table())?;
        Ok(MssqlDryRunOutputReport::new(
            planned_output,
            sql_identity,
            source_usage_status,
            used_source_names,
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
    use super::{MssqlDryRunSqlIdentityState, stable_sql_identity_hash};

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
