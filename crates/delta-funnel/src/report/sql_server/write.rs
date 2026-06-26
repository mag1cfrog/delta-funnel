use std::fmt;

use crate::{
    MssqlConnectionSource, MssqlConnectionSummary, MssqlTargetOutputPlan, MssqlTargetTable,
    MssqlWritePhase, PhaseStatus, ReportReasonCode, RowCount, sql_server::LoadMode,
};

/// Per-output SQL Server write statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlWriteStats {
    output_name: String,
    rows_written: u64,
    batches_written: u64,
    elapsed_ms: u64,
}

impl MssqlWriteStats {
    /// Builds write statistics for one selected output.
    #[must_use]
    pub fn new(
        output_name: impl Into<String>,
        rows_written: u64,
        batches_written: u64,
        elapsed_ms: u64,
    ) -> Self {
        Self {
            output_name: output_name.into(),
            rows_written,
            batches_written,
            elapsed_ms,
        }
    }

    /// Returns the selected output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns the number of rows accepted by SQL Server writing.
    #[must_use]
    pub const fn rows_written(&self) -> u64 {
        self.rows_written
    }

    /// Returns the number of batches accepted by SQL Server writing.
    #[must_use]
    pub const fn batches_written(&self) -> u64 {
        self.batches_written
    }

    /// Returns elapsed write time in milliseconds.
    #[must_use]
    pub const fn elapsed_ms(&self) -> u64 {
        self.elapsed_ms
    }
}

/// Output schema field included in an MSSQL execute report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlOutputFieldReport {
    index: u64,
    name: String,
    arrow_type: String,
    nullable: bool,
}

impl MssqlOutputFieldReport {
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

/// Per-output query stream and identity batch-shaping counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MssqlBatchShapingReport {
    status: PhaseStatus,
    input_batches: u64,
    input_rows: u64,
    output_batches: u64,
    output_rows: u64,
}

impl MssqlBatchShapingReport {
    pub(crate) fn completed(
        input_batches: u64,
        input_rows: u64,
        output_batches: u64,
        output_rows: u64,
    ) -> Self {
        Self {
            status: PhaseStatus::completed(),
            input_batches,
            input_rows,
            output_batches,
            output_rows,
        }
    }

    pub(crate) fn failed(
        input_batches: u64,
        input_rows: u64,
        output_batches: u64,
        output_rows: u64,
    ) -> Self {
        Self {
            status: PhaseStatus::failed(),
            input_batches,
            input_rows,
            output_batches,
            output_rows,
        }
    }

    pub(crate) fn not_started(reason: ReportReasonCode) -> Self {
        Self {
            status: PhaseStatus::not_started(reason),
            input_batches: 0,
            input_rows: 0,
            output_batches: 0,
            output_rows: 0,
        }
    }

    pub(crate) fn skipped(reason: ReportReasonCode) -> Self {
        Self {
            status: PhaseStatus::skipped(reason),
            input_batches: 0,
            input_rows: 0,
            output_batches: 0,
            output_rows: 0,
        }
    }

    /// Returns the batch shaping phase status.
    #[must_use]
    pub const fn status(&self) -> PhaseStatus {
        self.status
    }

    /// Returns batches consumed from the selected output stream.
    #[must_use]
    pub const fn input_batches(&self) -> u64 {
        self.input_batches
    }

    /// Returns rows consumed from the selected output stream.
    #[must_use]
    pub const fn input_rows(&self) -> u64 {
        self.input_rows
    }

    /// Returns batches emitted after batch shaping.
    #[must_use]
    pub const fn output_batches(&self) -> u64 {
        self.output_batches
    }

    /// Returns rows emitted after batch shaping.
    #[must_use]
    pub const fn output_rows(&self) -> u64 {
        self.output_rows
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MssqlWriteReportMetrics {
    pub(crate) output_row_count: RowCount,
    pub(crate) batch_shaping: MssqlBatchShapingReport,
    pub(crate) rows_written: u64,
    pub(crate) batches_written: u64,
    pub(crate) elapsed_ms: u64,
    pub(crate) partial_write_possible: bool,
    pub(crate) cleanup: MssqlTargetCleanupStatus,
}

impl MssqlWriteReportMetrics {
    pub(crate) const fn new(
        output_row_count: RowCount,
        batch_shaping: MssqlBatchShapingReport,
        rows_written: u64,
        batches_written: u64,
        elapsed_ms: u64,
        partial_write_possible: bool,
        cleanup: MssqlTargetCleanupStatus,
    ) -> Self {
        Self {
            output_row_count,
            batch_shaping,
            rows_written,
            batches_written,
            elapsed_ms,
            partial_write_possible,
            cleanup,
        }
    }
}

/// Cleanup reporting state for a SQL Server target owned by create-and-load.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MssqlTargetCleanupStatus {
    /// No cleanup is owned by this output, such as append-existing mode.
    NotApplicable,
    /// Cleanup would be owned by this output, but the target was not created.
    NotAttempted,
    /// Cleanup was required, attempted, and succeeded.
    Succeeded,
    /// Cleanup was required, attempted, and failed.
    Failed,
}

impl fmt::Display for MssqlTargetCleanupStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotApplicable => "not applicable",
            Self::NotAttempted => "not attempted",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        })
    }
}

/// Redacted per-output SQL Server write report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlWriteReport {
    output_name: String,
    target_table: MssqlTargetTable,
    load_mode: LoadMode,
    connection_source: MssqlConnectionSource,
    connection: MssqlConnectionSummary,
    output_schema: Vec<MssqlOutputFieldReport>,
    output_row_count: RowCount,
    batch_shaping: MssqlBatchShapingReport,
    stats: MssqlWriteStats,
    partial_write_possible: bool,
    cleanup: MssqlTargetCleanupStatus,
}

impl MssqlWriteReport {
    /// Builds a write report from the already planned SQL Server output target.
    #[must_use]
    pub fn from_output_plan(
        output_plan: &MssqlTargetOutputPlan,
        rows_written: u64,
        batches_written: u64,
        elapsed_ms: u64,
        partial_write_possible: bool,
        cleanup: MssqlTargetCleanupStatus,
    ) -> Self {
        Self::from_output_plan_with_metrics(
            output_plan,
            MssqlWriteReportMetrics::new(
                RowCount::exact(rows_written),
                MssqlBatchShapingReport::completed(
                    batches_written,
                    rows_written,
                    batches_written,
                    rows_written,
                ),
                rows_written,
                batches_written,
                elapsed_ms,
                partial_write_possible,
                cleanup,
            ),
        )
    }

    pub(crate) fn from_output_plan_with_metrics(
        output_plan: &MssqlTargetOutputPlan,
        metrics: MssqlWriteReportMetrics,
    ) -> Self {
        let output_name = output_plan.output_name().to_owned();
        let output_schema = output_plan
            .schema_mappings()
            .iter()
            .map(MssqlOutputFieldReport::from_mapping)
            .collect();

        Self {
            output_name: output_name.clone(),
            target_table: output_plan.target_table().clone(),
            load_mode: output_plan.load_mode(),
            connection_source: output_plan.connection_source(),
            connection: output_plan.connection().clone(),
            output_schema,
            output_row_count: metrics.output_row_count,
            batch_shaping: metrics.batch_shaping,
            stats: MssqlWriteStats::new(
                output_name,
                metrics.rows_written,
                metrics.batches_written,
                metrics.elapsed_ms,
            ),
            partial_write_possible: metrics.partial_write_possible,
            cleanup: metrics.cleanup,
        }
    }

    /// Returns the selected output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns the effective target table.
    #[must_use]
    pub fn target_table(&self) -> &MssqlTargetTable {
        &self.target_table
    }

    /// Returns the requested target lifecycle mode.
    #[must_use]
    pub const fn load_mode(&self) -> LoadMode {
        self.load_mode
    }

    /// Returns where the effective connection came from.
    #[must_use]
    pub const fn connection_source(&self) -> MssqlConnectionSource {
        self.connection_source
    }

    /// Returns the redacted effective connection summary.
    #[must_use]
    pub const fn connection(&self) -> &MssqlConnectionSummary {
        &self.connection
    }

    /// Returns per-output write statistics.
    #[must_use]
    pub const fn stats(&self) -> &MssqlWriteStats {
        &self.stats
    }

    /// Returns the selected output schema fields.
    #[must_use]
    pub fn output_schema(&self) -> &[MssqlOutputFieldReport] {
        &self.output_schema
    }

    /// Returns query output row evidence for the selected output stream.
    #[must_use]
    pub const fn output_row_count(&self) -> RowCount {
        self.output_row_count
    }

    /// Returns identity batch-shaping counters for the selected output stream.
    #[must_use]
    pub const fn batch_shaping(&self) -> MssqlBatchShapingReport {
        self.batch_shaping
    }

    /// Returns whether the target may contain a partial write after failure.
    #[must_use]
    pub const fn partial_write_possible(&self) -> bool {
        self.partial_write_possible
    }

    /// Returns cleanup reporting state for DeltaFunnel-owned target cleanup.
    #[must_use]
    pub const fn cleanup(&self) -> MssqlTargetCleanupStatus {
        self.cleanup
    }
}

/// Redacted report for a successful planned-output schema validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlOutputBatchValidationReport {
    output_name: String,
    target_table: MssqlTargetTable,
    load_mode: LoadMode,
    connection_source: MssqlConnectionSource,
    connection: MssqlConnectionSummary,
}

impl MssqlOutputBatchValidationReport {
    /// Builds a validation report from the already planned SQL Server output target.
    #[must_use]
    pub fn from_output_plan(output_plan: &MssqlTargetOutputPlan) -> Self {
        Self {
            output_name: output_plan.output_name().to_owned(),
            target_table: output_plan.target_table().clone(),
            load_mode: output_plan.load_mode(),
            connection_source: output_plan.connection_source(),
            connection: output_plan.connection().clone(),
        }
    }

    /// Returns the selected output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns the effective target table.
    #[must_use]
    pub const fn target_table(&self) -> &MssqlTargetTable {
        &self.target_table
    }

    /// Returns the requested target lifecycle mode.
    #[must_use]
    pub const fn load_mode(&self) -> LoadMode {
        self.load_mode
    }

    /// Returns where the effective connection came from.
    #[must_use]
    pub const fn connection_source(&self) -> MssqlConnectionSource {
        self.connection_source
    }

    /// Returns the redacted effective connection summary.
    #[must_use]
    pub const fn connection(&self) -> &MssqlConnectionSummary {
        &self.connection
    }
}

/// Structured context for a one-output SQL Server write failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlWriteFailureContext {
    phase: MssqlWritePhase,
    report: MssqlWriteReport,
}

impl MssqlWriteFailureContext {
    /// Builds failure context from the already planned SQL Server output target.
    #[must_use]
    pub fn from_output_plan(
        output_plan: &MssqlTargetOutputPlan,
        phase: MssqlWritePhase,
        rows_written: u64,
        batches_written: u64,
        elapsed_ms: u64,
        partial_write_possible: bool,
        cleanup: MssqlTargetCleanupStatus,
    ) -> Self {
        Self::from_output_plan_with_metrics(
            output_plan,
            phase,
            MssqlWriteReportMetrics::new(
                RowCount::partial(rows_written),
                MssqlBatchShapingReport::failed(
                    batches_written,
                    rows_written,
                    batches_written,
                    rows_written,
                ),
                rows_written,
                batches_written,
                elapsed_ms,
                partial_write_possible,
                cleanup,
            ),
        )
    }

    pub(crate) fn from_output_plan_with_metrics(
        output_plan: &MssqlTargetOutputPlan,
        phase: MssqlWritePhase,
        metrics: MssqlWriteReportMetrics,
    ) -> Self {
        Self {
            phase,
            report: MssqlWriteReport::from_output_plan_with_metrics(output_plan, metrics),
        }
    }

    /// Returns the write phase associated with the failure.
    #[must_use]
    pub const fn phase(&self) -> MssqlWritePhase {
        self.phase
    }

    /// Returns the selected output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        self.report.output_name()
    }

    /// Returns the effective target table.
    #[must_use]
    pub fn target_table(&self) -> &MssqlTargetTable {
        self.report.target_table()
    }

    /// Returns the requested target lifecycle mode.
    #[must_use]
    pub const fn load_mode(&self) -> LoadMode {
        self.report.load_mode()
    }

    /// Returns where the effective connection came from.
    #[must_use]
    pub const fn connection_source(&self) -> MssqlConnectionSource {
        self.report.connection_source()
    }

    /// Returns the redacted effective connection summary.
    #[must_use]
    pub const fn connection(&self) -> &MssqlConnectionSummary {
        self.report.connection()
    }

    /// Returns accepted write statistics known at failure time.
    #[must_use]
    pub const fn stats(&self) -> &MssqlWriteStats {
        self.report.stats()
    }

    /// Returns query output row evidence known at failure time.
    #[must_use]
    pub const fn output_row_count(&self) -> RowCount {
        self.report.output_row_count()
    }

    /// Returns identity batch-shaping counters known at failure time.
    #[must_use]
    pub const fn batch_shaping(&self) -> MssqlBatchShapingReport {
        self.report.batch_shaping()
    }

    /// Returns whether the target may contain a partial write after failure.
    #[must_use]
    pub const fn partial_write_possible(&self) -> bool {
        self.report.partial_write_possible()
    }

    /// Returns cleanup reporting state for DeltaFunnel-owned target cleanup.
    #[must_use]
    pub const fn cleanup(&self) -> MssqlTargetCleanupStatus {
        self.report.cleanup()
    }

    /// Returns the redacted write report associated with the failure.
    #[must_use]
    pub const fn report(&self) -> &MssqlWriteReport {
        &self.report
    }
}
