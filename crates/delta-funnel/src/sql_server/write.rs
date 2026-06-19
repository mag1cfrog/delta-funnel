//! SQL Server write options.
//!
//! This module owns DeltaFunnel-side write defaults around `arrow-tiberius`.

use std::fmt;

pub use arrow_tiberius::WriteOptions as MssqlWriteOptions;

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
    use arrow_tiberius::{PlanOptions, SchemaCheck, WriteBackend, WriteOptions};

    use super::*;

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
