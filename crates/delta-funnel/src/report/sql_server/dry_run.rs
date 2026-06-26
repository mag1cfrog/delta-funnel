use std::fmt;

use crate::{
    DeltaSourceReport, LazyTableKind, LoadMode, MssqlDdlPlan, MssqlLifecyclePlan, MssqlSchemaPlan,
    MssqlTargetTable, OutputStatus, PlannedMssqlOutput, ReportReasonCode, RowCount, RunMode,
    SourceUsageStatus, ValidationStatus, WorkflowStatus,
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
    pub(crate) fn from_mapping(mapping: &arrow_tiberius::SchemaMapping) -> Self {
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
    pub(crate) fn present(hash: String) -> Self {
        Self {
            state: MssqlDryRunSqlIdentityState::Present,
            hash: Some(hash),
            reason: None,
        }
    }

    pub(crate) fn absent() -> Self {
        Self {
            state: MssqlDryRunSqlIdentityState::Absent,
            hash: None,
            reason: None,
        }
    }

    pub(crate) fn unavailable(reason: ReportReasonCode) -> Self {
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
    output_row_count: RowCount,
    output_row_count_reason: Option<ReportReasonCode>,
    status: OutputStatus,
    validation_status: ValidationStatus,
    sql_server_contacted: bool,
    row_production_started: bool,
    table_lifecycle_started: bool,
    bulk_writer_started: bool,
}

impl MssqlDryRunOutputReport {
    pub(crate) fn new(
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
            output_row_count: RowCount::unavailable(),
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
    pub const fn output_row_count(&self) -> RowCount {
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
    pub(crate) fn new(
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

#[cfg(test)]
mod tests {
    use super::MssqlDryRunSqlIdentityState;

    #[test]
    fn sql_identity_status_exposes_stable_codes() {
        assert_eq!(MssqlDryRunSqlIdentityState::Present.as_str(), "present");
        assert_eq!(MssqlDryRunSqlIdentityState::Absent.to_string(), "absent");
        assert_eq!(
            MssqlDryRunSqlIdentityState::Unavailable.as_str(),
            "unavailable"
        );
    }
}
