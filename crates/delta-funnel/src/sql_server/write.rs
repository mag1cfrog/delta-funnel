//! SQL Server write options.
//!
//! This module owns DeltaFunnel-side write defaults around `arrow-tiberius`.

use std::fmt;

pub use arrow_tiberius::WriteOptions as MssqlWriteOptions;

use super::{
    LoadMode, MssqlConnectionSource, MssqlConnectionSummary, MssqlTargetOutputPlan,
    MssqlTargetTable,
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

/// Phase of one-output SQL Server write execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MssqlWritePhase {
    /// Establish the SQL Server connection.
    Connect,
    /// Execute target lifecycle preparation before writer construction.
    PrepareTargetLifecycle,
    /// Construct the SQL Server bulk writer and validate target metadata.
    InitializeWriter,
    /// Poll the selected output batch stream.
    PollBatchStream,
    /// Validate an incoming batch schema against the planned schema.
    ValidateBatchSchema,
    /// Write an accepted batch into SQL Server.
    WriteBatch,
    /// Finalize the SQL Server bulk writer.
    Finalize,
    /// Clean up a DeltaFunnel-created target after a later failure.
    Cleanup,
}

impl fmt::Display for MssqlWritePhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Connect => "connect",
            Self::PrepareTargetLifecycle => "prepare target lifecycle",
            Self::InitializeWriter => "initialize writer",
            Self::PollBatchStream => "poll batch stream",
            Self::ValidateBatchSchema => "validate batch schema",
            Self::WriteBatch => "write batch",
            Self::Finalize => "finalize",
            Self::Cleanup => "cleanup",
        })
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
        let output_name = output_plan.output_name().to_owned();

        Self {
            output_name: output_name.clone(),
            target_table: output_plan.target_table().clone(),
            load_mode: output_plan.load_mode(),
            connection_source: output_plan.connection_source(),
            connection: output_plan.connection().clone(),
            stats: MssqlWriteStats::new(output_name, rows_written, batches_written, elapsed_ms),
            partial_write_possible,
            cleanup,
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

/// Returns DeltaFunnel's default SQL Server write options.
#[must_use]
pub fn default_mssql_write_options() -> MssqlWriteOptions {
    MssqlWriteOptions {
        backend: arrow_tiberius::WriteBackend::DirectRawBulk,
        ..MssqlWriteOptions::default()
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use arrow_tiberius::{PlanOptions, SchemaCheck, WriteBackend, WriteOptions};

    use super::*;
    use crate::{
        DeltaFunnelError, MssqlConnectionConfig, MssqlTargetConfig, MssqlTargetTable,
        plan_mssql_target_for_output,
    };

    fn secret_connection() -> Result<MssqlConnectionConfig, DeltaFunnelError> {
        Ok(MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label("warehouse-primary"))
    }

    fn orders_schema() -> Schema {
        Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
        ])
    }

    #[test]
    fn write_stats_preserve_output_counts_and_elapsed_time() {
        let stats = MssqlWriteStats::new("orders", 42, 3, 125);

        assert_eq!(stats.output_name(), "orders");
        assert_eq!(stats.rows_written(), 42);
        assert_eq!(stats.batches_written(), 3);
        assert_eq!(stats.elapsed_ms(), 125);
    }

    #[test]
    fn write_phase_display_is_stable() {
        let phases = [
            (MssqlWritePhase::Connect, "connect"),
            (
                MssqlWritePhase::PrepareTargetLifecycle,
                "prepare target lifecycle",
            ),
            (MssqlWritePhase::InitializeWriter, "initialize writer"),
            (MssqlWritePhase::PollBatchStream, "poll batch stream"),
            (
                MssqlWritePhase::ValidateBatchSchema,
                "validate batch schema",
            ),
            (MssqlWritePhase::WriteBatch, "write batch"),
            (MssqlWritePhase::Finalize, "finalize"),
            (MssqlWritePhase::Cleanup, "cleanup"),
        ];

        for (phase, expected) in phases {
            assert_eq!(phase.to_string(), expected);
            assert!(!format!("{phase:?}").contains("password"));
        }
    }

    #[test]
    fn cleanup_status_display_is_stable() {
        let statuses = [
            (MssqlTargetCleanupStatus::NotApplicable, "not applicable"),
            (MssqlTargetCleanupStatus::NotAttempted, "not attempted"),
            (MssqlTargetCleanupStatus::Succeeded, "succeeded"),
            (MssqlTargetCleanupStatus::Failed, "failed"),
        ];

        for (status, expected) in statuses {
            assert_eq!(status.to_string(), expected);
            assert!(!format!("{status:?}").contains("password"));
        }
    }

    #[test]
    fn write_report_preserves_plan_context_stats_and_cleanup() -> Result<(), DeltaFunnelError> {
        let connection = secret_connection()?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?);
        let output_plan = plan_mssql_target_for_output(
            orders_schema(),
            "orders_output",
            &target_config,
            Some(&connection),
            PlanOptions::default(),
        )?;

        let report = MssqlWriteReport::from_output_plan(
            &output_plan,
            42,
            3,
            125,
            true,
            MssqlTargetCleanupStatus::NotApplicable,
        );

        assert_eq!(report.output_name(), "orders_output");
        assert_eq!(report.target_table().schema(), Some("dbo"));
        assert_eq!(report.target_table().table(), "orders");
        assert_eq!(report.load_mode(), LoadMode::AppendExisting);
        assert_eq!(
            report.connection_source(),
            MssqlConnectionSource::ContextDefault
        );
        assert_eq!(
            report.connection().display_label(),
            Some("warehouse-primary")
        );
        assert_eq!(report.stats().output_name(), "orders_output");
        assert_eq!(report.stats().rows_written(), 42);
        assert_eq!(report.stats().batches_written(), 3);
        assert_eq!(report.stats().elapsed_ms(), 125);
        assert!(report.partial_write_possible());
        assert_eq!(report.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        Ok(())
    }

    #[test]
    fn write_report_debug_redacts_connection_secret() -> Result<(), DeltaFunnelError> {
        let connection = secret_connection()?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?);
        let output_plan = plan_mssql_target_for_output(
            orders_schema(),
            "orders_output",
            &target_config,
            Some(&connection),
            PlanOptions::default(),
        )?;

        let report = MssqlWriteReport::from_output_plan(
            &output_plan,
            0,
            0,
            0,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
        );
        let debug = format!("{report:?}");

        assert!(debug.contains("warehouse-primary"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        Ok(())
    }

    #[test]
    fn default_options_pin_direct_raw_bulk_backend() {
        let options = default_mssql_write_options();

        assert_eq!(options.backend, WriteBackend::DirectRawBulk);
    }

    #[test]
    fn default_options_preserve_arrow_tiberius_schema_check_default() {
        let options = default_mssql_write_options();

        assert_eq!(options.schema_check, WriteOptions::default().schema_check);
        assert_eq!(options.schema_check, SchemaCheck::Strict);
    }

    #[test]
    fn default_options_preserve_arrow_tiberius_plan_options_default() {
        let options = default_mssql_write_options();

        assert_eq!(options.plan_options, WriteOptions::default().plan_options);
        assert_eq!(options.plan_options, PlanOptions::default());
    }
}
