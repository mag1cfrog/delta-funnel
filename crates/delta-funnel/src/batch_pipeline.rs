//! Batch pipeline foundation for query-result handoff.
//!
//! This module intentionally does not poll DataFusion streams or rewrite
//! Arrow batches. It only owns shared concepts that later query-output handoff
//! and MSSQL writer integration can build on.

use std::fmt;

use crate::DeltaFunnelError;

/// Phase for batch pipeline setup and configuration failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchPipelinePhase {
    /// Caller-supplied batch pipeline or query-output configuration is invalid.
    Configuration,
    /// The handoff between a query output and downstream consumer cannot be set up.
    HandoffSetup,
}

impl fmt::Display for BatchPipelinePhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Configuration => "configuration",
            Self::HandoffSetup => "handoff setup",
        })
    }
}

/// Per-output batch handoff counters.
///
/// `input_*` counters describe batches observed from the upstream DataFusion
/// stream. `output_*` counters describe batches accepted by the downstream
/// consumer. Keeping the two sides separate lets later sink-failure handling
/// report only work that was actually accepted downstream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchHandoffStats {
    /// Batches observed from the upstream query output.
    pub input_batches: u64,
    /// Rows observed from the upstream query output.
    pub input_rows: u64,
    /// Batches accepted by the downstream consumer.
    pub output_batches: u64,
    /// Rows accepted by the downstream consumer.
    pub output_rows: u64,
}

impl BatchHandoffStats {
    /// Records one batch observed from the upstream query output.
    pub fn record_input_batch(&mut self, row_count: usize) {
        self.input_batches = self.input_batches.saturating_add(1);
        self.input_rows = self.input_rows.saturating_add(rows_to_u64(row_count));
    }

    /// Records one batch accepted by the downstream consumer.
    pub fn record_output_batch(&mut self, row_count: usize) {
        self.output_batches = self.output_batches.saturating_add(1);
        self.output_rows = self.output_rows.saturating_add(rows_to_u64(row_count));
    }
}

/// Validates a future batch/query/handoff `usize` option that must be nonzero.
#[allow(dead_code)]
pub(crate) fn validate_nonzero_usize_option(
    phase: BatchPipelinePhase,
    option: &'static str,
    value: usize,
) -> Result<(), DeltaFunnelError> {
    if value == 0 {
        return Err(DeltaFunnelError::BatchPipeline {
            phase,
            option,
            message: "must be greater than zero".to_owned(),
        });
    }

    Ok(())
}

fn rows_to_u64(row_count: usize) -> u64 {
    u64::try_from(row_count).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{
        BatchHandoffStats, BatchPipelinePhase, rows_to_u64, validate_nonzero_usize_option,
    };
    use crate::DeltaFunnelError;

    #[test]
    fn stats_start_at_zero() {
        let stats = BatchHandoffStats::default();

        assert_eq!(stats.input_batches, 0);
        assert_eq!(stats.input_rows, 0);
        assert_eq!(stats.output_batches, 0);
        assert_eq!(stats.output_rows, 0);
    }

    #[test]
    fn stats_update_input_and_output_separately() {
        let mut stats = BatchHandoffStats::default();

        stats.record_input_batch(5);
        stats.record_input_batch(7);
        stats.record_output_batch(5);

        assert_eq!(stats.input_batches, 2);
        assert_eq!(stats.input_rows, 12);
        assert_eq!(stats.output_batches, 1);
        assert_eq!(stats.output_rows, 5);
    }

    #[test]
    fn stats_updates_saturate() {
        let mut stats = BatchHandoffStats {
            input_batches: u64::MAX,
            input_rows: u64::MAX - 1,
            output_batches: u64::MAX,
            output_rows: u64::MAX - 1,
        };

        stats.record_input_batch(10);
        stats.record_output_batch(10);

        assert_eq!(stats.input_batches, u64::MAX);
        assert_eq!(stats.input_rows, u64::MAX);
        assert_eq!(stats.output_batches, u64::MAX);
        assert_eq!(stats.output_rows, u64::MAX);
    }

    #[test]
    fn rows_to_u64_returns_exact_normal_values() {
        assert_eq!(rows_to_u64(42), 42);
    }

    #[test]
    fn validation_accepts_nonzero_values() -> Result<(), DeltaFunnelError> {
        validate_nonzero_usize_option(BatchPipelinePhase::Configuration, "output_batch_size", 1)
    }

    #[test]
    fn validation_rejects_zero_values() {
        let error = validate_nonzero_usize_option(
            BatchPipelinePhase::Configuration,
            "output_batch_size",
            0,
        );

        assert!(matches!(
            error,
            Err(DeltaFunnelError::BatchPipeline {
                phase: BatchPipelinePhase::Configuration,
                option: "output_batch_size",
                ..
            })
        ));
    }

    #[test]
    fn phase_display_is_stable() {
        assert_eq!(
            BatchPipelinePhase::Configuration.to_string(),
            "configuration"
        );
        assert_eq!(
            BatchPipelinePhase::HandoffSetup.to_string(),
            "handoff setup"
        );
    }
}
