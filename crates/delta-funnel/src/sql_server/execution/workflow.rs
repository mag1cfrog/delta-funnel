//! Sequential multi-output SQL Server write orchestration.
//!
//! This module keeps the multi-output workflow layer separate from the
//! one-output sink. The MVP runs outputs sequentially, stops on the first
//! failure, and marks later outputs as skipped without invoking their lazy batch
//! stream factories.

use std::{fmt, future::Future, pin::Pin};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use futures_util::Stream;
use tracing::Instrument;

use crate::{
    DeltaFunnelError, PhaseTimingReport, ReportReasonCode, RowCount, ValidationOptions,
    ValidationStatus, observability, plan_mssql_target_for_resolved_output,
    progress::{ProgressEvent, ProgressPhase, ProgressReporter},
    report::{OperationTimelineRecorder, OperationTimelineSpanRecorder, PhaseTimer},
    support::sanitize_text_for_display,
};

use super::{
    LoadMode, MssqlBatchShapingReport, MssqlConnectionSource, MssqlConnectionSummary,
    MssqlSchemaPlanOptions, MssqlTargetSummary, MssqlTargetTable, MssqlWriteBackend,
    MssqlWriteFailureContext, MssqlWriteReport, ResolvedMssqlTarget, default_mssql_write_backend,
    drain_mssql_batches_for_stream_benchmark, write_output_batches_to_mssql_for_workflow,
    write_output_batches_to_mssql_for_workflow_with_timeline,
};

const OUTPUT_STREAM_SETUP_PHASE: &str = "output_stream_setup";
const SQL_WRITE_PHASE: &str = "sql_write";
const VALIDATION_PHASE: &str = "validation";

/// Lazy stream produced only when a SQL Server output is attempted.
pub type MssqlOutputBatchStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send>>;

/// Fallible future that constructs a direct batch stream for one attempted output.
pub type MssqlOutputBatchStreamFuture =
    Pin<Box<dyn Future<Output = Result<MssqlOutputBatchStream, DeltaFunnelError>> + Send>>;

/// Async factory that constructs a direct batch stream for one attempted output.
pub type MssqlOutputBatchStreamFactory = Box<dyn FnOnce() -> MssqlOutputBatchStreamFuture + Send>;

/// One output query execution and its terminal reporting state.
pub(crate) struct MssqlOutputQueryExecution {
    pub(crate) stream: MssqlOutputBatchStream,
    pub(crate) query_phase_timings: Vec<PhaseTimingReport>,
    pub(crate) attach_profile_to_result: Option<MssqlOutputProfileCallback>,
}

/// Callback that attaches a terminal query profile to one writer result.
pub(crate) type MssqlOutputProfileCallback = Box<
    dyn FnOnce(
            Result<MssqlWriteReport, DeltaFunnelError>,
        ) -> Result<MssqlWriteReport, DeltaFunnelError>
        + Send,
>;

/// Failed output query plus timings completed before the failure.
pub(crate) struct MssqlOutputQueryError {
    pub(crate) error: DeltaFunnelError,
    pub(crate) query_phase_timings: Vec<PhaseTimingReport>,
}

/// Future returned when an orchestrated write-all output starts its query.
pub(crate) type MssqlOutputQueryFuture =
    Pin<Box<dyn Future<Output = Result<MssqlOutputQueryExecution, MssqlOutputQueryError>> + Send>>;

/// One deferred SQL Server output write job.
///
/// The job owns an already resolved SQL Server target plus a lazy batch stream
/// factory. The workflow awaits the factory only after the output becomes the
/// next attempted output. Skipped jobs keep their stream factories uncalled, so
/// skipped outputs do not start source reads, DataFusion execution, stream
/// setup, SQL connections, lifecycle preparation, writer initialization, or
/// batch polling through this API.
pub struct MssqlOutputWriteJob {
    output_schema: SchemaRef,
    resolved_target: ResolvedMssqlTarget,
    schema_options: MssqlSchemaPlanOptions,
    create_query_execution: Box<dyn FnOnce() -> MssqlOutputQueryFuture + Send>,
    write_backend: MssqlWriteBackend,
    validation_options: ValidationOptions,
    phase_timings: Vec<PhaseTimingReport>,
    progress_reporter: Option<ProgressReporter>,
    operation_timeline: Option<OperationTimelineRecorder>,
}

impl MssqlOutputWriteJob {
    /// Creates a deferred SQL Server output write job.
    pub fn new<F, Fut, S>(
        output_schema: SchemaRef,
        resolved_target: ResolvedMssqlTarget,
        schema_options: MssqlSchemaPlanOptions,
        stream_factory: F,
        write_backend: MssqlWriteBackend,
        validation_options: ValidationOptions,
    ) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, DeltaFunnelError>> + Send + 'static,
        S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send + 'static,
    {
        Self::new_with_query_execution_factory(
            output_schema,
            resolved_target,
            schema_options,
            Box::new(move || {
                Box::pin(async move {
                    let stream = stream_factory()
                        .await
                        .map_err(|error| MssqlOutputQueryError {
                            error,
                            query_phase_timings: Vec::new(),
                        })?;
                    Ok(MssqlOutputQueryExecution {
                        stream: Box::pin(stream),
                        query_phase_timings: Vec::new(),
                        attach_profile_to_result: None,
                    })
                })
            }),
            write_backend,
            validation_options,
        )
    }

    /// Creates an internal job whose query execution carries its stream,
    /// phase timings, and terminal profile callback.
    pub(crate) fn new_with_query_execution_factory(
        output_schema: SchemaRef,
        resolved_target: ResolvedMssqlTarget,
        schema_options: MssqlSchemaPlanOptions,
        create_query_execution: Box<dyn FnOnce() -> MssqlOutputQueryFuture + Send>,
        write_backend: MssqlWriteBackend,
        validation_options: ValidationOptions,
    ) -> Self {
        Self {
            output_schema,
            resolved_target,
            schema_options,
            create_query_execution,
            write_backend,
            validation_options,
            phase_timings: Vec::new(),
            progress_reporter: None,
            operation_timeline: None,
        }
    }

    /// Adds phase timings that completed before this deferred write job runs.
    #[must_use]
    pub fn with_phase_timings(mut self, phase_timings: Vec<PhaseTimingReport>) -> Self {
        self.phase_timings = phase_timings;
        self
    }

    /// Adds the reporter used only if this deferred output is attempted.
    #[must_use]
    pub(crate) fn with_progress_reporter(
        mut self,
        progress_reporter: Option<ProgressReporter>,
    ) -> Self {
        self.progress_reporter = progress_reporter;
        self
    }

    /// Adds the shared write-all timeline used only if this output is attempted.
    #[must_use]
    pub(crate) fn with_operation_timeline(
        mut self,
        operation_timeline: Option<OperationTimelineRecorder>,
    ) -> Self {
        self.operation_timeline = operation_timeline;
        self
    }

    /// Creates a deferred SQL Server output write job using the default write backend.
    pub fn with_default_write_backend<F, Fut, S>(
        output_schema: SchemaRef,
        resolved_target: ResolvedMssqlTarget,
        schema_options: MssqlSchemaPlanOptions,
        batches: F,
    ) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, DeltaFunnelError>> + Send + 'static,
        S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send + 'static,
    {
        Self::new(
            output_schema,
            resolved_target,
            schema_options,
            batches,
            default_mssql_write_backend(),
            ValidationOptions::default(),
        )
    }

    /// Returns the selected output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        self.resolved_target.output_name()
    }

    /// Returns a redacted target summary for reports.
    #[must_use]
    pub fn target_summary(&self) -> MssqlTargetSummary {
        self.resolved_target.summary()
    }

    /// Returns phase timings that completed before this deferred write job runs.
    #[must_use]
    pub fn phase_timings(&self) -> &[PhaseTimingReport] {
        &self.phase_timings
    }
}

/// SQL Server multi-output workflow options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MssqlWorkflowWriteOptions {
    max_parallel_outputs: usize,
}

impl Default for MssqlWorkflowWriteOptions {
    fn default() -> Self {
        Self {
            max_parallel_outputs: 1,
        }
    }
}

impl MssqlWorkflowWriteOptions {
    /// Creates default sequential workflow options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_parallel_outputs: 1,
        }
    }

    /// Sets the requested maximum number of parallel output writers.
    ///
    /// The current MVP supports only `1`. Values greater than `1` are rejected
    /// explicitly so callers do not mistake the workflow for a parallel writer
    /// pool or cross-output transaction boundary.
    #[must_use]
    pub const fn with_max_parallel_outputs(mut self, max_parallel_outputs: usize) -> Self {
        self.max_parallel_outputs = max_parallel_outputs;
        self
    }

    /// Returns the requested maximum number of parallel output writers.
    #[must_use]
    pub const fn max_parallel_outputs(&self) -> usize {
        self.max_parallel_outputs
    }

    /// Validates workflow write options before any output write side effects.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaFunnelError::MssqlWorkflowPlanning`] when no output
    /// writer is allowed or when parallel output writers are requested. The
    /// current MVP is intentionally single-writer so callers cannot mistake
    /// this workflow for a parallel writer pool or cross-output transaction.
    pub fn validate(&self) -> Result<(), DeltaFunnelError> {
        match self.max_parallel_outputs() {
            1 => Ok(()),
            0 => Err(DeltaFunnelError::MssqlWorkflowPlanning {
                message: "max_parallel_outputs must be at least 1".to_owned(),
            }),
            max_parallel_outputs => Err(DeltaFunnelError::MssqlWorkflowPlanning {
                message: format!(
                    "parallel MSSQL output writers are not supported; requested {max_parallel_outputs}"
                ),
            }),
        }
    }
}

/// Structured report for a multi-output SQL Server write workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlWorkflowWriteReport {
    outputs: Vec<MssqlOutputWriteStatus>,
}

impl MssqlWorkflowWriteReport {
    fn new(outputs: Vec<MssqlOutputWriteStatus>) -> Self {
        Self { outputs }
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

    /// Returns per-output statuses in caller-provided order.
    #[must_use]
    pub fn outputs(&self) -> &[MssqlOutputWriteStatus] {
        &self.outputs
    }

    /// Returns whether every selected output completed successfully.
    #[must_use]
    pub fn all_succeeded(&self) -> bool {
        self.outputs
            .iter()
            .all(MssqlOutputWriteStatus::is_succeeded)
    }

    /// Returns the number of outputs that completed successfully.
    #[must_use]
    pub fn succeeded_count(&self) -> usize {
        self.outputs
            .iter()
            .filter(|status| status.is_succeeded())
            .count()
    }

    /// Returns the number of outputs that failed.
    #[must_use]
    pub fn failed_count(&self) -> usize {
        self.outputs
            .iter()
            .filter(|status| status.is_failed())
            .count()
    }

    /// Returns the number of outputs skipped after a previous output failed.
    #[must_use]
    pub fn skipped_count(&self) -> usize {
        self.outputs
            .iter()
            .filter(|status| status.is_skipped())
            .count()
    }
}

impl fmt::Display for MssqlWorkflowWriteReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let succeeded = self.succeeded_count();
        let failed = self.failed_count();
        let skipped = self.skipped_count();

        write!(
            formatter,
            "MSSQL workflow write report: {succeeded} succeeded, {failed} failed, {skipped} skipped"
        )
    }
}

/// Final write status for one selected SQL Server output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MssqlOutputWriteStatus {
    /// The output completed successfully.
    Succeeded(MssqlWriteReport),
    /// The output was the first attempted output to fail.
    Failed(MssqlWriteFailureReport),
    /// The output was not attempted because an earlier output failed.
    Skipped(MssqlWriteSkippedReport),
}

impl MssqlOutputWriteStatus {
    /// Returns the selected output name for this status.
    #[must_use]
    pub fn output_name(&self) -> &str {
        match self {
            Self::Succeeded(report) => report.output_name(),
            Self::Failed(report) => report.output_name(),
            Self::Skipped(report) => report.output_name(),
        }
    }

    /// Returns the effective SQL Server target table for this status.
    #[must_use]
    pub fn target_table(&self) -> &MssqlTargetTable {
        match self {
            Self::Succeeded(report) => report.target_table(),
            Self::Failed(report) => report.target().table(),
            Self::Skipped(report) => report.target().table(),
        }
    }

    /// Returns the requested lifecycle mode for this output.
    #[must_use]
    pub fn load_mode(&self) -> LoadMode {
        match self {
            Self::Succeeded(report) => report.load_mode(),
            Self::Failed(report) => report.target().load_mode(),
            Self::Skipped(report) => report.target().load_mode(),
        }
    }

    /// Returns where the effective connection came from.
    #[must_use]
    pub fn connection_source(&self) -> MssqlConnectionSource {
        match self {
            Self::Succeeded(report) => report.connection_source(),
            Self::Failed(report) => report.target().connection_source(),
            Self::Skipped(report) => report.target().connection_source(),
        }
    }

    /// Returns the redacted effective connection summary.
    #[must_use]
    pub fn connection(&self) -> &MssqlConnectionSummary {
        match self {
            Self::Succeeded(report) => report.connection(),
            Self::Failed(report) => report.target().connection(),
            Self::Skipped(report) => report.target().connection(),
        }
    }

    /// Returns query output row evidence for this output.
    #[must_use]
    pub fn output_row_count(&self) -> RowCount {
        match self {
            Self::Succeeded(report) => report.output_row_count(),
            Self::Failed(report) => report.output_row_count(),
            Self::Skipped(report) => report.output_row_count(),
        }
    }

    /// Returns target-side row count evidence for this output.
    #[must_use]
    pub fn target_row_count(&self) -> RowCount {
        match self {
            Self::Succeeded(report) => report.target_row_count(),
            Self::Failed(report) => report.target_row_count(),
            Self::Skipped(report) => report.target_row_count(),
        }
    }

    /// Returns target-side validation status for this output.
    #[must_use]
    pub fn validation_status(&self) -> ValidationStatus {
        match self {
            Self::Succeeded(report) => report.validation_status(),
            Self::Failed(report) => report.validation_status(),
            Self::Skipped(report) => report.validation_status(),
        }
    }

    /// Returns batch-shaping counters for this output.
    #[must_use]
    pub fn batch_shaping(&self) -> MssqlBatchShapingReport {
        match self {
            Self::Succeeded(report) => report.batch_shaping(),
            Self::Failed(report) => report.batch_shaping(),
            Self::Skipped(report) => report.batch_shaping(),
        }
    }

    /// Returns workflow phase timing reports for this output when available.
    #[must_use]
    pub fn phase_timings(&self) -> &[PhaseTimingReport] {
        match self {
            Self::Succeeded(report) => report.phase_timings(),
            Self::Failed(report) => report.phase_timings(),
            Self::Skipped(report) => report.phase_timings(),
        }
    }

    /// Returns whether this output succeeded.
    #[must_use]
    pub const fn is_succeeded(&self) -> bool {
        matches!(self, Self::Succeeded(_))
    }

    /// Returns whether this output failed.
    #[must_use]
    pub const fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }

    /// Returns whether this output was skipped before any work was attempted.
    #[must_use]
    pub const fn is_skipped(&self) -> bool {
        matches!(self, Self::Skipped(_))
    }
}

/// Structured report for the first failed SQL Server output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlWriteFailureReport {
    target: MssqlTargetSummary,
    error: String,
    context: Option<Box<MssqlWriteFailureContext>>,
    output_row_count: RowCount,
    target_row_count: RowCount,
    validation_status: ValidationStatus,
    batch_shaping: MssqlBatchShapingReport,
    phase_timings: Vec<PhaseTimingReport>,
}

impl MssqlWriteFailureReport {
    fn from_error(
        target: MssqlTargetSummary,
        error: DeltaFunnelError,
        phase_timings: Vec<PhaseTimingReport>,
    ) -> Self {
        let context = failure_context(&error).cloned().map(Box::new);
        let phase_timings = merged_failure_phase_timings(phase_timings, context.as_deref());
        let output_row_count = context.as_deref().map_or(
            RowCount::unavailable(),
            MssqlWriteFailureContext::output_row_count,
        );
        let target_row_count = context.as_deref().map_or(
            RowCount::unavailable(),
            MssqlWriteFailureContext::target_row_count,
        );
        let validation_status = context.as_deref().map_or(
            ValidationStatus::skipped(ReportReasonCode::FailureBeforeValidation),
            MssqlWriteFailureContext::validation_status,
        );
        let batch_shaping = context.as_deref().map_or_else(
            || MssqlBatchShapingReport::not_started(ReportReasonCode::NotExecuted),
            MssqlWriteFailureContext::batch_shaping,
        );
        Self {
            target,
            error: sanitize_text_for_display(&error.to_string()),
            context,
            output_row_count,
            target_row_count,
            validation_status,
            batch_shaping,
            phase_timings,
        }
    }

    /// Returns the redacted target summary for the failed output.
    #[must_use]
    pub const fn target(&self) -> &MssqlTargetSummary {
        &self.target
    }

    /// Returns the selected output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        self.target.output_name()
    }

    /// Returns the sanitized error message for the failed output.
    #[must_use]
    pub fn error(&self) -> &str {
        &self.error
    }

    /// Returns phase-aware write failure context when the one-output sink
    /// provided it.
    #[must_use]
    pub fn context(&self) -> Option<&MssqlWriteFailureContext> {
        self.context.as_deref()
    }

    /// Returns query output row evidence known at failure time.
    #[must_use]
    pub const fn output_row_count(&self) -> RowCount {
        self.output_row_count
    }

    /// Returns target-side row count evidence known at failure time.
    #[must_use]
    pub const fn target_row_count(&self) -> RowCount {
        self.target_row_count
    }

    /// Returns target-side validation status known at failure time.
    #[must_use]
    pub const fn validation_status(&self) -> ValidationStatus {
        self.validation_status
    }

    /// Returns batch-shaping counters known at failure time.
    #[must_use]
    pub const fn batch_shaping(&self) -> MssqlBatchShapingReport {
        self.batch_shaping
    }

    /// Returns workflow phase timing reports for this failed output.
    #[must_use]
    pub fn phase_timings(&self) -> &[PhaseTimingReport] {
        &self.phase_timings
    }
}

fn merged_failure_phase_timings(
    mut phase_timings: Vec<PhaseTimingReport>,
    context: Option<&MssqlWriteFailureContext>,
) -> Vec<PhaseTimingReport> {
    let Some(context) = context else {
        return phase_timings;
    };

    for timing in context.phase_timings() {
        if let Some(existing) = phase_timings
            .iter_mut()
            .find(|existing| existing.phase_name() == timing.phase_name())
        {
            *existing = timing.clone();
        } else {
            phase_timings.push(timing.clone());
        }
    }

    phase_timings
}

/// Structured report for a skipped SQL Server output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlWriteSkippedReport {
    target: MssqlTargetSummary,
    reason: MssqlWriteSkippedReason,
    output_row_count: RowCount,
    target_row_count: RowCount,
    validation_status: ValidationStatus,
    batch_shaping: MssqlBatchShapingReport,
    phase_timings: Vec<PhaseTimingReport>,
}

impl MssqlWriteSkippedReport {
    fn previous_output_failed(
        target: MssqlTargetSummary,
        failed_output_name: String,
        phase_timings: Vec<PhaseTimingReport>,
    ) -> Self {
        Self {
            target,
            reason: MssqlWriteSkippedReason::PreviousOutputFailed { failed_output_name },
            output_row_count: RowCount::unavailable(),
            target_row_count: RowCount::unavailable(),
            validation_status: ValidationStatus::skipped(ReportReasonCode::PriorFailure),
            batch_shaping: MssqlBatchShapingReport::skipped(ReportReasonCode::PriorFailure),
            phase_timings: skipped_after_prior_failure_phase_timings(phase_timings),
        }
    }

    /// Returns the redacted target summary for the skipped output.
    #[must_use]
    pub const fn target(&self) -> &MssqlTargetSummary {
        &self.target
    }

    /// Returns the selected output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        self.target.output_name()
    }

    /// Returns why this output was skipped before any work was attempted.
    #[must_use]
    pub const fn reason(&self) -> &MssqlWriteSkippedReason {
        &self.reason
    }

    /// Returns query output row evidence for this skipped output.
    #[must_use]
    pub const fn output_row_count(&self) -> RowCount {
        self.output_row_count
    }

    /// Returns target-side row count evidence for this skipped output.
    #[must_use]
    pub const fn target_row_count(&self) -> RowCount {
        self.target_row_count
    }

    /// Returns target-side validation status for this skipped output.
    #[must_use]
    pub const fn validation_status(&self) -> ValidationStatus {
        self.validation_status
    }

    /// Returns batch-shaping counters for this skipped output.
    #[must_use]
    pub const fn batch_shaping(&self) -> MssqlBatchShapingReport {
        self.batch_shaping
    }

    /// Returns workflow phase timing reports for this skipped output.
    #[must_use]
    pub fn phase_timings(&self) -> &[PhaseTimingReport] {
        &self.phase_timings
    }
}

/// Reason one selected SQL Server output was skipped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MssqlWriteSkippedReason {
    /// A previous output failed and the MVP stops on first failure.
    PreviousOutputFailed {
        /// Output name of the first failed output.
        failed_output_name: String,
    },
}

/// Writes multiple SQL Server outputs sequentially.
///
/// This workflow calls the public one-output sink once per attempted output,
/// in caller-provided order. It stops on the first failed output and marks all
/// later outputs as skipped without invoking their lazy batch stream factories.
/// The report is per-output and does not imply all-or-nothing transaction
/// behavior across outputs, target tables, or SQL Server connections.
pub async fn write_mssql_outputs_to_mssql(
    jobs: impl IntoIterator<Item = MssqlOutputWriteJob>,
    options: MssqlWorkflowWriteOptions,
) -> Result<MssqlWorkflowWriteReport, DeltaFunnelError> {
    write_mssql_outputs_with_writer(jobs, options, MssqlWorkflowSinkWriter).await
}

pub(crate) struct MssqlStreamBenchmarkOutputWriter;

#[async_trait]
pub(crate) trait MssqlWorkflowOutputWriter: Send {
    #[allow(
        clippy::too_many_arguments,
        reason = "the workflow writer receives one planned write plus its progress reporter"
    )]
    async fn write_output(
        &mut self,
        output_schema: SchemaRef,
        resolved_target: ResolvedMssqlTarget,
        schema_options: MssqlSchemaPlanOptions,
        batches: MssqlOutputBatchStream,
        write_backend: MssqlWriteBackend,
        validation_options: ValidationOptions,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>;

    #[allow(
        clippy::too_many_arguments,
        reason = "the workflow writer receives one planned write plus profiling state"
    )]
    async fn write_output_with_timeline(
        &mut self,
        output_schema: SchemaRef,
        resolved_target: ResolvedMssqlTarget,
        schema_options: MssqlSchemaPlanOptions,
        batches: MssqlOutputBatchStream,
        write_backend: MssqlWriteBackend,
        validation_options: ValidationOptions,
        reporter: Option<&ProgressReporter>,
        _timeline: Option<&OperationTimelineRecorder>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        self.write_output(
            output_schema,
            resolved_target,
            schema_options,
            batches,
            write_backend,
            validation_options,
            reporter,
        )
        .await
    }
}

pub(crate) struct MssqlWorkflowSinkWriter;

#[async_trait]
impl MssqlWorkflowOutputWriter for MssqlWorkflowSinkWriter {
    async fn write_output(
        &mut self,
        output_schema: SchemaRef,
        resolved_target: ResolvedMssqlTarget,
        schema_options: MssqlSchemaPlanOptions,
        batches: MssqlOutputBatchStream,
        write_backend: MssqlWriteBackend,
        validation_options: ValidationOptions,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        write_output_batches_to_mssql_for_workflow(
            output_schema.as_ref(),
            resolved_target,
            schema_options,
            batches,
            write_backend,
            validation_options,
            reporter,
        )
        .await
    }

    async fn write_output_with_timeline(
        &mut self,
        output_schema: SchemaRef,
        resolved_target: ResolvedMssqlTarget,
        schema_options: MssqlSchemaPlanOptions,
        batches: MssqlOutputBatchStream,
        write_backend: MssqlWriteBackend,
        validation_options: ValidationOptions,
        reporter: Option<&ProgressReporter>,
        timeline: Option<&OperationTimelineRecorder>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        write_output_batches_to_mssql_for_workflow_with_timeline(
            output_schema.as_ref(),
            resolved_target,
            schema_options,
            batches,
            write_backend,
            validation_options,
            reporter,
            timeline,
        )
        .await
    }
}

#[async_trait]
impl MssqlWorkflowOutputWriter for MssqlStreamBenchmarkOutputWriter {
    async fn write_output(
        &mut self,
        output_schema: SchemaRef,
        resolved_target: ResolvedMssqlTarget,
        schema_options: MssqlSchemaPlanOptions,
        batches: MssqlOutputBatchStream,
        _write_backend: MssqlWriteBackend,
        _validation_options: ValidationOptions,
        _reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        let output_plan = plan_mssql_target_for_resolved_output(
            output_schema.as_ref(),
            &resolved_target,
            schema_options,
        )?;

        drain_mssql_batches_for_stream_benchmark(&output_plan, batches).await
    }
}

pub(crate) async fn write_mssql_outputs_with_writer<W>(
    jobs: impl IntoIterator<Item = MssqlOutputWriteJob>,
    options: MssqlWorkflowWriteOptions,
    mut writer: W,
) -> Result<MssqlWorkflowWriteReport, DeltaFunnelError>
where
    W: MssqlWorkflowOutputWriter,
{
    ensure_sequential_options(options)?;

    let mut statuses = Vec::new();
    let mut failed_output_name = None::<String>;

    for job in jobs {
        if let Some(failed_output_name) = failed_output_name.as_ref() {
            statuses.push(skipped_output_status_with_tracing(
                job.target_summary(),
                failed_output_name.clone(),
                job.phase_timings().to_vec(),
            ));
            continue;
        }

        let status = write_mssql_output_job_with_tracing(job, &mut writer).await;
        if let MssqlOutputWriteStatus::Failed(failure) = &status {
            failed_output_name = Some(failure.output_name().to_owned());
        }
        statuses.push(status);
    }

    Ok(MssqlWorkflowWriteReport::new(statuses))
}

fn skipped_output_status_with_tracing(
    target: MssqlTargetSummary,
    failed_output_name: String,
    planned_phase_timings: Vec<PhaseTimingReport>,
) -> MssqlOutputWriteStatus {
    let output_span =
        observability::output_span(target.output_name(), target.table(), target.load_mode());
    output_span.in_scope(|| {
        let skipped = skipped_output_status(target, failed_output_name, planned_phase_timings);
        observability::output_skipped(
            skipped.output_name(),
            skipped.target().table(),
            skipped.target().load_mode(),
            "prior_failure",
        );
        MssqlOutputWriteStatus::Skipped(skipped)
    })
}

fn skipped_output_status(
    target: MssqlTargetSummary,
    failed_output_name: String,
    planned_phase_timings: Vec<PhaseTimingReport>,
) -> MssqlWriteSkippedReport {
    MssqlWriteSkippedReport::previous_output_failed(
        target,
        failed_output_name,
        planned_phase_timings,
    )
}

async fn write_mssql_output_job_with_tracing<W>(
    job: MssqlOutputWriteJob,
    writer: &mut W,
) -> MssqlOutputWriteStatus
where
    W: MssqlWorkflowOutputWriter,
{
    let target = job.target_summary();
    let output_span =
        observability::output_span(target.output_name(), target.table(), target.load_mode());

    async move {
        observability::output_started(target.output_name(), target.table(), target.load_mode());
        let status = write_mssql_output_job(job, writer).await;
        match &status {
            MssqlOutputWriteStatus::Succeeded(report) => {
                observability::output_completed(
                    report.output_name(),
                    report.target_table(),
                    report.load_mode(),
                );
            }
            MssqlOutputWriteStatus::Failed(failure) => {
                observability::output_failed(
                    failure.output_name(),
                    failure.target().table(),
                    failure.target().load_mode(),
                    failure.error(),
                );
            }
            MssqlOutputWriteStatus::Skipped(_) => {}
        }
        status
    }
    .instrument(output_span)
    .await
}

async fn write_mssql_output_job<W>(
    job: MssqlOutputWriteJob,
    writer: &mut W,
) -> MssqlOutputWriteStatus
where
    W: MssqlWorkflowOutputWriter,
{
    let target = job.target_summary();
    let MssqlOutputWriteJob {
        output_schema,
        resolved_target,
        schema_options,
        create_query_execution,
        write_backend,
        validation_options,
        phase_timings: mut planned_phase_timings,
        progress_reporter,
        operation_timeline,
    } = job;
    let output_timeline_span = operation_timeline.as_ref().map(|timeline| {
        timeline
            .start_span(
                format!("Write output: {}", target.output_name()),
                "delta_funnel.write_all.output",
                format!("Output: {}", target.output_name()),
            )
            .with_attribute("output_name", target.output_name().to_owned().into())
    });
    if let Some(reporter) = progress_reporter.as_ref() {
        reporter.emit(&ProgressEvent::phase_changed(
            ProgressPhase::SettingUpStream,
            Some(target.output_name()),
        ));
    }
    let stream_setup_timer = PhaseTimer::start(OUTPUT_STREAM_SETUP_PHASE);
    let (query_execution, stream_setup_timing) = match create_query_execution().await {
        Ok(query_execution) => (query_execution, stream_setup_timer.completed()),
        Err(failure) => {
            append_error_profile_to_timeline(operation_timeline.as_ref(), &failure.error);
            fail_timeline_span(output_timeline_span);
            planned_phase_timings.extend(failure.query_phase_timings);
            let failure = MssqlWriteFailureReport::from_error(
                target,
                failure.error,
                stream_setup_failure_phase_timings(
                    planned_phase_timings,
                    stream_setup_timer.failed(),
                ),
            );
            return MssqlOutputWriteStatus::Failed(failure);
        }
    };
    let MssqlOutputQueryExecution {
        stream,
        query_phase_timings,
        attach_profile_to_result,
    } = query_execution;
    planned_phase_timings.extend(query_phase_timings);

    let write_timer = PhaseTimer::start(SQL_WRITE_PHASE);
    let write_result = writer
        .write_output_with_timeline(
            output_schema,
            resolved_target,
            schema_options,
            stream,
            write_backend,
            validation_options,
            progress_reporter.as_ref(),
            operation_timeline.as_ref(),
        )
        .await;
    // The writer has now drained, failed, or dropped the stream, so the
    // callback can attach the terminal query profile.
    let write_result = match attach_profile_to_result {
        Some(attach_profile) => attach_profile(write_result),
        None => write_result,
    };
    match &write_result {
        Ok(report) => {
            if let (Some(timeline), Some(profile)) =
                (operation_timeline.as_ref(), report.execution_profile())
            {
                timeline.append_operator_lifecycles(profile);
            }
            complete_timeline_span(output_timeline_span);
        }
        Err(error) => {
            append_error_profile_to_timeline(operation_timeline.as_ref(), error);
            fail_timeline_span(output_timeline_span);
        }
    }
    match write_result {
        Ok(report) => {
            let report = report.with_phase_timings(output_write_phase_timings(
                planned_phase_timings,
                stream_setup_timing,
                write_timer.completed(),
            ));
            MssqlOutputWriteStatus::Succeeded(report)
        }
        Err(error) => {
            let failure = MssqlWriteFailureReport::from_error(
                target,
                error,
                output_write_failure_phase_timings(
                    planned_phase_timings,
                    stream_setup_timing,
                    write_timer.failed(),
                ),
            );
            MssqlOutputWriteStatus::Failed(failure)
        }
    }
}

fn append_error_profile_to_timeline(
    timeline: Option<&OperationTimelineRecorder>,
    error: &DeltaFunnelError,
) {
    if let (Some(timeline), Some(profile)) = (
        timeline,
        failure_context(error).and_then(|context| context.report().execution_profile()),
    ) {
        timeline.append_operator_lifecycles(profile);
    }
}

fn complete_timeline_span(span: Option<OperationTimelineSpanRecorder>) {
    if let Some(span) = span {
        span.completed();
    }
}

fn fail_timeline_span(span: Option<OperationTimelineSpanRecorder>) {
    if let Some(span) = span {
        span.failed();
    }
}

fn ensure_sequential_options(options: MssqlWorkflowWriteOptions) -> Result<(), DeltaFunnelError> {
    options.validate()
}

fn failure_context(error: &DeltaFunnelError) -> Option<&MssqlWriteFailureContext> {
    match error {
        DeltaFunnelError::MssqlWritePhase { context, .. }
        | DeltaFunnelError::MssqlQueryPhase { context, .. }
        | DeltaFunnelError::MssqlBatchSchemaValidation { context, .. } => Some(context.as_ref()),
        _ => None,
    }
}

fn output_write_phase_timings(
    mut phase_timings: Vec<PhaseTimingReport>,
    stream_setup_timing: PhaseTimingReport,
    write_timing: PhaseTimingReport,
) -> Vec<PhaseTimingReport> {
    phase_timings.extend([stream_setup_timing, write_timing]);
    phase_timings
}

fn output_write_failure_phase_timings(
    mut phase_timings: Vec<PhaseTimingReport>,
    stream_setup_timing: PhaseTimingReport,
    write_timing: PhaseTimingReport,
) -> Vec<PhaseTimingReport> {
    phase_timings.extend([
        stream_setup_timing,
        write_timing,
        PhaseTimingReport::not_started(VALIDATION_PHASE, ReportReasonCode::FailureBeforeValidation),
    ]);
    phase_timings
}

fn stream_setup_failure_phase_timings(
    mut phase_timings: Vec<PhaseTimingReport>,
    stream_setup_timing: PhaseTimingReport,
) -> Vec<PhaseTimingReport> {
    phase_timings.extend([
        stream_setup_timing,
        PhaseTimingReport::not_started(SQL_WRITE_PHASE, ReportReasonCode::NotExecuted),
        PhaseTimingReport::not_started(VALIDATION_PHASE, ReportReasonCode::FailureBeforeValidation),
    ]);
    phase_timings
}

fn skipped_after_prior_failure_phase_timings(
    mut phase_timings: Vec<PhaseTimingReport>,
) -> Vec<PhaseTimingReport> {
    phase_timings.extend([
        PhaseTimingReport::skipped(OUTPUT_STREAM_SETUP_PHASE, ReportReasonCode::PriorFailure),
        PhaseTimingReport::skipped(SQL_WRITE_PHASE, ReportReasonCode::PriorFailure),
        PhaseTimingReport::skipped(VALIDATION_PHASE, ReportReasonCode::PriorFailure),
    ]);
    phase_timings
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::time::Duration;

    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use futures_util::{StreamExt, stream};

    use super::*;
    use crate::{
        LoadMode, MssqlConnectionConfig, MssqlTargetCleanupStatus, MssqlTargetConfig,
        MssqlTargetOutputPlan, MssqlTargetResolutionContext, MssqlTargetTable, MssqlWritePhase,
        PhaseStatus, PhaseTimingReport, ValidationStatus, plan_mssql_target_for_output,
        report::sql_server::MssqlWriteReportMetrics,
    };

    const PLANNED_PHASE: &str = "planned_phase";
    const DEFERRED_QUERY_PHASE: &str = "deferred_query_phase";

    #[derive(Default)]
    struct FakeWorkflowWriter {
        outcomes: VecDeque<Result<MssqlWriteReport, DeltaFunnelError>>,
        attempted_outputs: Arc<Mutex<Vec<String>>>,
    }

    #[derive(Default)]
    struct StreamPollingWorkflowWriter {
        attempted_outputs: Arc<Mutex<Vec<String>>>,
    }

    impl FakeWorkflowWriter {
        fn new(outcomes: Vec<Result<MssqlWriteReport, DeltaFunnelError>>) -> Self {
            Self {
                outcomes: outcomes.into(),
                attempted_outputs: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn attempted_outputs(&self) -> Arc<Mutex<Vec<String>>> {
            Arc::clone(&self.attempted_outputs)
        }
    }

    impl StreamPollingWorkflowWriter {
        fn attempted_outputs(&self) -> Arc<Mutex<Vec<String>>> {
            Arc::clone(&self.attempted_outputs)
        }
    }

    #[async_trait]
    impl MssqlWorkflowOutputWriter for FakeWorkflowWriter {
        async fn write_output(
            &mut self,
            _output_schema: SchemaRef,
            resolved_target: ResolvedMssqlTarget,
            _schema_options: MssqlSchemaPlanOptions,
            _batches: MssqlOutputBatchStream,
            _write_backend: MssqlWriteBackend,
            _validation_options: ValidationOptions,
            _reporter: Option<&ProgressReporter>,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            self.attempted_outputs
                .lock()
                .map_err(|_| test_error("attempted output lock poisoned"))?
                .push(resolved_target.output_name().to_owned());

            self.outcomes
                .pop_front()
                .ok_or_else(|| test_error("missing fake writer outcome"))?
        }
    }

    #[async_trait]
    impl MssqlWorkflowOutputWriter for StreamPollingWorkflowWriter {
        async fn write_output(
            &mut self,
            _output_schema: SchemaRef,
            resolved_target: ResolvedMssqlTarget,
            _schema_options: MssqlSchemaPlanOptions,
            mut batches: MssqlOutputBatchStream,
            _write_backend: MssqlWriteBackend,
            _validation_options: ValidationOptions,
            _reporter: Option<&ProgressReporter>,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            self.attempted_outputs
                .lock()
                .map_err(|_| test_error("attempted output lock poisoned"))?
                .push(resolved_target.output_name().to_owned());

            match batches.next().await {
                Some(Ok(_batch)) => Err(test_error("expected stream polling error")),
                Some(Err(error)) => Err(error),
                None => Err(test_error("expected at least one stream item")),
            }
        }
    }

    #[tokio::test]
    async fn empty_workflow_report_has_zero_counts() -> Result<(), DeltaFunnelError> {
        let writer = FakeWorkflowWriter::default();

        let report = write_mssql_outputs_with_writer(
            Vec::new(),
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        assert!(report.is_empty());
        assert_eq!(report.len(), 0);
        assert_eq!(report.outputs(), []);
        assert!(report.all_succeeded());
        assert_eq!(report.succeeded_count(), 0);
        assert_eq!(report.failed_count(), 0);
        assert_eq!(report.skipped_count(), 0);
        assert_eq!(
            report.to_string(),
            "MSSQL workflow write report: 0 succeeded, 0 failed, 0 skipped"
        );

        Ok(())
    }

    #[tokio::test]
    async fn two_successful_outputs_produce_two_success_statuses() -> Result<(), DeltaFunnelError> {
        let first = output_plan("first", LoadMode::AppendExisting)?;
        let second = output_plan("second", LoadMode::AppendExisting)?;
        let first_report =
            write_report(&first, 2, 1, false, MssqlTargetCleanupStatus::NotApplicable);
        let second_report = write_report(
            &second,
            3,
            2,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
        );
        let writer = FakeWorkflowWriter::new(vec![Ok(first_report), Ok(second_report)]);
        let attempted = writer.attempted_outputs();

        let report = write_mssql_outputs_with_writer(
            vec![job(first)?, job(second)?],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        assert_eq!(report.outputs().len(), 2);
        assert!(report.all_succeeded());
        assert_status_output(report.outputs(), 0, "first")?;
        assert_status_output(report.outputs(), 1, "second")?;
        assert_eq!(report.outputs()[0].output_row_count(), RowCount::exact(2));
        assert_batch_shaping(
            report.outputs()[0].batch_shaping(),
            PhaseStatus::completed(),
            1,
            2,
            1,
            2,
        );
        assert_eq!(report.outputs()[1].output_row_count(), RowCount::exact(3));
        assert_batch_shaping(
            report.outputs()[1].batch_shaping(),
            PhaseStatus::completed(),
            2,
            3,
            2,
            3,
        );
        assert_phase_timing(
            &report.outputs()[0],
            PLANNED_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_phase_timing(
            &report.outputs()[0],
            OUTPUT_STREAM_SETUP_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_phase_timing(
            &report.outputs()[0],
            SQL_WRITE_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_eq!(
            locked(&attempted)?.as_slice(),
            ["first".to_owned(), "second".to_owned()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn first_success_remains_successful_when_second_output_fails()
    -> Result<(), DeltaFunnelError> {
        let first = output_plan("first", LoadMode::AppendExisting)?;
        let second = output_plan("second", LoadMode::AppendExisting)?;
        let first_report =
            write_report(&first, 2, 1, false, MssqlTargetCleanupStatus::NotApplicable);
        let failure_context = MssqlWriteFailureContext::from_output_plan(
            &second,
            MssqlWritePhase::WriteBatch,
            1,
            1,
            0,
            true,
            MssqlTargetCleanupStatus::NotApplicable,
        )
        .with_phase_timings(vec![
            PhaseTimingReport::completed("prepare_target_lifecycle", Duration::from_micros(10)),
            PhaseTimingReport::failed("write_batch", Duration::from_micros(20)),
            PhaseTimingReport::not_started(
                VALIDATION_PHASE,
                ReportReasonCode::FailureBeforeValidation,
            ),
        ]);
        let failure = phase_error_with_context(failure_context, "write failed");
        let writer = FakeWorkflowWriter::new(vec![Ok(first_report), Err(failure)]);

        let report = write_mssql_outputs_with_writer(
            vec![job(first)?, job(second)?],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        let [first_status, second_status] = report.outputs() else {
            return Err(test_error("expected two output statuses"));
        };
        assert!(matches!(first_status, MssqlOutputWriteStatus::Succeeded(_)));
        let MssqlOutputWriteStatus::Failed(failure) = second_status else {
            return Err(test_error("expected second output to fail"));
        };
        assert_eq!(failure.output_name(), "second");
        let context = failure
            .context()
            .ok_or_else(|| test_error("expected write failure context"))?;
        assert_eq!(context.phase(), MssqlWritePhase::WriteBatch);
        assert!(context.partial_write_possible());
        assert_eq!(context.stats().rows_written(), 1);
        assert_eq!(context.stats().batches_written(), 1);
        assert_phase_timing(
            second_status,
            OUTPUT_STREAM_SETUP_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_phase_timing(second_status, SQL_WRITE_PHASE, PhaseStatus::failed())?;
        assert_phase_timing(second_status, "write_batch", PhaseStatus::failed())?;
        assert_phase_timing(
            second_status,
            VALIDATION_PHASE,
            PhaseStatus::not_started(ReportReasonCode::FailureBeforeValidation),
        )?;
        assert_eq!(
            second_status
                .phase_timings()
                .iter()
                .filter(|timing| timing.phase_name() == VALIDATION_PHASE)
                .count(),
            1
        );

        Ok(())
    }

    #[tokio::test]
    async fn batch_schema_validation_failure_preserves_failure_context()
    -> Result<(), DeltaFunnelError> {
        let output = output_plan("schema_failure", LoadMode::AppendExisting)?;
        let context = MssqlWriteFailureContext::from_output_plan(
            &output,
            MssqlWritePhase::ValidateBatchSchema,
            0,
            0,
            0,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
        );
        let failure = DeltaFunnelError::MssqlBatchSchemaValidation {
            context: Box::new(context),
            source: arrow_tiberius::Error::BackendUnavailable {
                backend: arrow_tiberius::WriteBackend::DirectRawBulk,
                reason: "schema mismatch".to_owned(),
            },
        };
        let writer = FakeWorkflowWriter::new(vec![Err(failure)]);

        let report = write_mssql_outputs_with_writer(
            vec![job(output)?],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        let [MssqlOutputWriteStatus::Failed(failure)] = report.outputs() else {
            return Err(test_error("expected failed output status"));
        };
        let context = failure
            .context()
            .ok_or_else(|| test_error("expected schema validation context"))?;
        assert_eq!(failure.output_name(), "schema_failure");
        assert_eq!(context.phase(), MssqlWritePhase::ValidateBatchSchema);
        assert!(!context.partial_write_possible());
        assert_eq!(context.stats().rows_written(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn first_failure_marks_later_outputs_skipped_without_attempting_them()
    -> Result<(), DeltaFunnelError> {
        let first = output_plan("first", LoadMode::AppendExisting)?;
        let second = output_plan("second", LoadMode::AppendExisting)?;
        let third = output_plan("third", LoadMode::AppendExisting)?;
        let failure = phase_error(
            &first,
            MssqlWritePhase::Connect,
            0,
            0,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
            "connect failed",
        );
        let writer = FakeWorkflowWriter::new(vec![Err(failure)]);
        let attempted = writer.attempted_outputs();

        let report = write_mssql_outputs_with_writer(
            vec![job(first)?, job(second)?, job(third)?],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        assert_eq!(locked(&attempted)?.as_slice(), ["first".to_owned()]);
        let [failed, skipped_second, skipped_third] = report.outputs() else {
            return Err(test_error("expected three output statuses"));
        };
        assert_eq!(report.len(), 3);
        assert_eq!(report.succeeded_count(), 0);
        assert_eq!(report.failed_count(), 1);
        assert_eq!(report.skipped_count(), 2);
        assert!(matches!(failed, MssqlOutputWriteStatus::Failed(_)));
        assert_eq!(failed.output_row_count(), RowCount::partial(0));
        assert_batch_shaping(failed.batch_shaping(), PhaseStatus::failed(), 0, 0, 0, 0);
        assert_phase_timing(failed, OUTPUT_STREAM_SETUP_PHASE, PhaseStatus::completed())?;
        assert_phase_timing(failed, SQL_WRITE_PHASE, PhaseStatus::failed())?;
        assert_skipped_after(skipped_second, "second", "first")?;
        assert_skipped_after(skipped_third, "third", "first")?;

        Ok(())
    }

    #[tokio::test]
    async fn output_status_accessors_cover_success_failure_and_skipped_variants()
    -> Result<(), DeltaFunnelError> {
        let first = output_plan("first", LoadMode::AppendExisting)?;
        let second = output_plan("second", LoadMode::CreateAndLoad)?;
        let third = output_plan("third", LoadMode::AppendExisting)?;
        let first_report =
            write_report(&first, 2, 1, false, MssqlTargetCleanupStatus::NotApplicable);
        let failure = phase_error(
            &second,
            MssqlWritePhase::InitializeWriter,
            0,
            0,
            false,
            MssqlTargetCleanupStatus::NotAttempted,
            "writer init failed",
        );
        let writer = FakeWorkflowWriter::new(vec![Ok(first_report), Err(failure)]);

        let report = write_mssql_outputs_with_writer(
            vec![job(first)?, job(second)?, job(third)?],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        let [success, failed, skipped] = report.outputs() else {
            return Err(test_error("expected three output statuses"));
        };
        assert!(success.is_succeeded());
        assert_eq!(success.output_name(), "first");
        assert_eq!(success.target_table().table(), "first_orders");
        assert_eq!(success.load_mode(), LoadMode::AppendExisting);
        assert_eq!(
            success.connection().display_label(),
            Some("test connection")
        );
        assert_eq!(success.target_row_count(), RowCount::unavailable());
        assert_eq!(
            success.validation_status(),
            ValidationStatus::skipped(ReportReasonCode::NotExecuted)
        );

        assert!(failed.is_failed());
        assert_eq!(failed.output_name(), "second");
        assert_eq!(failed.target_table().table(), "second_orders");
        assert_eq!(failed.load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(failed.connection().display_label(), Some("test connection"));
        assert_eq!(failed.target_row_count(), RowCount::unavailable());
        assert_eq!(
            failed.validation_status(),
            ValidationStatus::skipped(ReportReasonCode::NotExecuted)
        );

        assert!(skipped.is_skipped());
        assert_eq!(skipped.output_name(), "third");
        assert_eq!(skipped.target_table().table(), "third_orders");
        assert_eq!(skipped.load_mode(), LoadMode::AppendExisting);
        assert_eq!(
            skipped.connection().display_label(),
            Some("test connection")
        );
        assert_eq!(skipped.target_row_count(), RowCount::unavailable());
        assert_eq!(
            skipped.validation_status(),
            ValidationStatus::skipped(ReportReasonCode::PriorFailure)
        );

        Ok(())
    }

    #[tokio::test]
    async fn failed_output_status_exposes_validation_evidence() -> Result<(), DeltaFunnelError> {
        let output = output_plan("validation_failed", LoadMode::CreateAndLoad)?;
        let metrics = MssqlWriteReportMetrics::new(
            RowCount::exact(3),
            MssqlBatchShapingReport::completed(1, 3, 1, 3),
            3,
            1,
            0,
            false,
            MssqlTargetCleanupStatus::Succeeded,
        )
        .with_target_validation(RowCount::exact(4), ValidationStatus::failed())
        .with_phase_timings(vec![PhaseTimingReport::failed(
            VALIDATION_PHASE,
            Duration::from_micros(5),
        )]);
        let failure = phase_error_with_context(
            MssqlWriteFailureContext::from_output_plan_with_metrics(
                &output,
                MssqlWritePhase::Validation,
                metrics,
            ),
            "target row count did not match exact output rows",
        );
        let writer = FakeWorkflowWriter::new(vec![Err(failure)]);

        let report = write_mssql_outputs_with_writer(
            vec![job(output)?],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        let [status] = report.outputs() else {
            return Err(test_error("expected one output status"));
        };
        let MssqlOutputWriteStatus::Failed(failure) = status else {
            return Err(test_error("expected failed output status"));
        };
        assert_eq!(failure.target_row_count(), RowCount::exact(4));
        assert_eq!(failure.validation_status(), ValidationStatus::failed());
        assert_eq!(status.target_row_count(), RowCount::exact(4));
        assert_eq!(status.validation_status(), ValidationStatus::failed());
        assert_phase_timing(status, VALIDATION_PHASE, PhaseStatus::failed())?;
        Ok(())
    }

    #[tokio::test]
    async fn skipped_output_stream_factories_are_not_invoked() -> Result<(), DeltaFunnelError> {
        let first = output_plan("first", LoadMode::AppendExisting)?;
        let second = output_plan("second", LoadMode::AppendExisting)?;
        let factory_calls = Arc::new(Mutex::new(Vec::new()));
        let failure = phase_error(
            &first,
            MssqlWritePhase::Connect,
            0,
            0,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
            "connect failed",
        );
        let writer = FakeWorkflowWriter::new(vec![Err(failure)]);

        let report = write_mssql_outputs_with_writer(
            vec![
                counted_job(first, Arc::clone(&factory_calls))?,
                counted_job(second, Arc::clone(&factory_calls))?,
            ],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        assert_eq!(locked(&factory_calls)?.as_slice(), ["first".to_owned()]);
        assert_eq!(report.outputs().len(), 2);
        assert!(report.outputs()[1].is_skipped());

        Ok(())
    }

    #[tokio::test]
    async fn deferred_query_execution_metadata_follows_writer_results()
    -> Result<(), DeltaFunnelError> {
        let first = output_plan("first", LoadMode::AppendExisting)?;
        let second = output_plan("second", LoadMode::AppendExisting)?;
        let first_report =
            write_report(&first, 1, 1, false, MssqlTargetCleanupStatus::NotApplicable);
        let second_failure = phase_error(
            &second,
            MssqlWritePhase::Connect,
            0,
            0,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
            "connect failed",
        );
        let attachment_calls = Arc::new(Mutex::new(Vec::new()));
        let writer = FakeWorkflowWriter::new(vec![Ok(first_report), Err(second_failure)]);

        let report = write_mssql_outputs_with_writer(
            vec![
                query_execution_job(first, Arc::clone(&attachment_calls))?,
                query_execution_job(second, Arc::clone(&attachment_calls))?,
            ],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        assert_eq!(
            locked(&attachment_calls)?.as_slice(),
            ["first:succeeded".to_owned(), "second:failed".to_owned()]
        );
        for status in report.outputs() {
            let phase_names = status
                .phase_timings()
                .iter()
                .map(PhaseTimingReport::phase_name)
                .collect::<Vec<_>>();
            assert_eq!(
                &phase_names[..4],
                [
                    PLANNED_PHASE,
                    DEFERRED_QUERY_PHASE,
                    OUTPUT_STREAM_SETUP_PHASE,
                    SQL_WRITE_PHASE,
                ]
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn deferred_query_failure_keeps_its_timings_before_workflow_failure_timings()
    -> Result<(), DeltaFunnelError> {
        let output = output_plan("failed_setup", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let job = failed_query_execution_job(output)?;

        let report = write_mssql_outputs_with_writer(
            vec![job],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        let [status] = report.outputs() else {
            return Err(test_error("expected one output status"));
        };
        assert!(status.is_failed());
        let phase_names = status
            .phase_timings()
            .iter()
            .map(PhaseTimingReport::phase_name)
            .collect::<Vec<_>>();
        assert_eq!(
            phase_names,
            [
                PLANNED_PHASE,
                DEFERRED_QUERY_PHASE,
                OUTPUT_STREAM_SETUP_PHASE,
                SQL_WRITE_PHASE,
                VALIDATION_PHASE,
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn skipped_outputs_do_not_reach_one_output_writer() -> Result<(), DeltaFunnelError> {
        let first = output_plan("first", LoadMode::AppendExisting)?;
        let second = output_plan("second", LoadMode::AppendExisting)?;
        let failure = phase_error(
            &first,
            MssqlWritePhase::PrepareTargetLifecycle,
            0,
            0,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
            "prepare failed",
        );
        let writer = FakeWorkflowWriter::new(vec![Err(failure)]);
        let attempted = writer.attempted_outputs();

        let report = write_mssql_outputs_with_writer(
            vec![job(first)?, job(second)?],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        assert_eq!(locked(&attempted)?.as_slice(), ["first".to_owned()]);
        assert!(report.outputs()[1].is_skipped());

        Ok(())
    }

    #[tokio::test]
    async fn stream_factory_setup_failure_fails_output_before_writer_and_skips_later_factories()
    -> Result<(), DeltaFunnelError> {
        let first = output_plan("first", LoadMode::AppendExisting)?;
        let second = output_plan("second", LoadMode::AppendExisting)?;
        let third = output_plan("third", LoadMode::AppendExisting)?;
        let first_report =
            write_report(&first, 1, 1, false, MssqlTargetCleanupStatus::NotApplicable);
        let factory_calls = Arc::new(Mutex::new(Vec::new()));
        let writer = FakeWorkflowWriter::new(vec![Ok(first_report)]);
        let attempted = writer.attempted_outputs();

        let report = write_mssql_outputs_with_writer(
            vec![
                counted_job(first, Arc::clone(&factory_calls))?,
                failing_factory_job(
                    second,
                    Arc::clone(&factory_calls),
                    "stream setup failed before SQL writer",
                )?,
                counted_job(third, Arc::clone(&factory_calls))?,
            ],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        assert_eq!(
            locked(&factory_calls)?.as_slice(),
            ["first".to_owned(), "second".to_owned()]
        );
        assert_eq!(locked(&attempted)?.as_slice(), ["first".to_owned()]);
        let [first_status, second_status, third_status] = report.outputs() else {
            return Err(test_error("expected three output statuses"));
        };
        assert!(first_status.is_succeeded());
        let MssqlOutputWriteStatus::Failed(failure) = second_status else {
            return Err(test_error("expected second output to fail"));
        };
        assert_eq!(failure.output_name(), "second");
        assert!(failure.context().is_none());
        assert_eq!(failure.output_row_count(), RowCount::unavailable());
        assert_batch_shaping(
            failure.batch_shaping(),
            PhaseStatus::not_started(ReportReasonCode::NotExecuted),
            0,
            0,
            0,
            0,
        );
        assert_phase_timing(second_status, PLANNED_PHASE, PhaseStatus::completed())?;
        assert_phase_timing(
            second_status,
            OUTPUT_STREAM_SETUP_PHASE,
            PhaseStatus::failed(),
        )?;
        assert_phase_timing(
            second_status,
            SQL_WRITE_PHASE,
            PhaseStatus::not_started(ReportReasonCode::NotExecuted),
        )?;
        assert_phase_timing(
            second_status,
            VALIDATION_PHASE,
            PhaseStatus::not_started(ReportReasonCode::FailureBeforeValidation),
        )?;
        assert!(
            failure
                .error()
                .contains("stream setup failed before SQL writer")
        );
        assert_skipped_after(third_status, "third", "second")?;

        Ok(())
    }

    #[tokio::test]
    async fn stream_polling_failure_after_setup_reaches_writer_boundary()
    -> Result<(), DeltaFunnelError> {
        let output = output_plan("poll_failure", LoadMode::AppendExisting)?;
        let factory_calls = Arc::new(Mutex::new(Vec::new()));
        let writer = StreamPollingWorkflowWriter::default();
        let attempted = writer.attempted_outputs();

        let report = write_mssql_outputs_with_writer(
            vec![polling_error_job(output, Arc::clone(&factory_calls))?],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        assert_eq!(
            locked(&factory_calls)?.as_slice(),
            ["poll_failure".to_owned()]
        );
        assert_eq!(locked(&attempted)?.as_slice(), ["poll_failure".to_owned()]);
        let [MssqlOutputWriteStatus::Failed(failure)] = report.outputs() else {
            return Err(test_error("expected failed output status"));
        };
        assert_eq!(failure.output_name(), "poll_failure");
        assert!(failure.error().contains("stream failed during polling"));
        assert!(failure.context().is_none());
        assert_eq!(failure.output_row_count(), RowCount::unavailable());
        assert_batch_shaping(
            failure.batch_shaping(),
            PhaseStatus::not_started(ReportReasonCode::NotExecuted),
            0,
            0,
            0,
            0,
        );
        assert_phase_timing(
            &report.outputs()[0],
            OUTPUT_STREAM_SETUP_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_phase_timing(&report.outputs()[0], SQL_WRITE_PHASE, PhaseStatus::failed())?;

        Ok(())
    }

    #[tokio::test]
    async fn failed_create_and_load_cleanup_status_is_preserved() -> Result<(), DeltaFunnelError> {
        let output = output_plan("created", LoadMode::CreateAndLoad)?;
        let failure = phase_error(
            &output,
            MssqlWritePhase::Finalize,
            2,
            1,
            false,
            MssqlTargetCleanupStatus::Succeeded,
            "finalize failed",
        );
        let writer = FakeWorkflowWriter::new(vec![Err(failure)]);

        let report = write_mssql_outputs_with_writer(
            vec![job(output)?],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        let [MssqlOutputWriteStatus::Failed(failure)] = report.outputs() else {
            return Err(test_error("expected failed output status"));
        };
        let context = failure
            .context()
            .ok_or_else(|| test_error("expected write failure context"))?;
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::Succeeded);
        assert_eq!(context.stats().rows_written(), 2);
        assert_eq!(context.stats().batches_written(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn parallel_writer_configuration_is_rejected() -> Result<(), DeltaFunnelError> {
        let output = output_plan("first", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::new(vec![Ok(write_report(
            &output,
            1,
            1,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
        ))]);

        let error = write_mssql_outputs_with_writer(
            vec![job(output)?],
            MssqlWorkflowWriteOptions::new().with_max_parallel_outputs(2),
            writer,
        )
        .await;
        let Err(error) = error else {
            return Err(test_error("parallel writer config should be rejected"));
        };

        assert!(error.to_string().contains("parallel MSSQL output writers"));

        Ok(())
    }

    #[tokio::test]
    async fn zero_parallel_writer_configuration_is_rejected_before_attempting_outputs()
    -> Result<(), DeltaFunnelError> {
        let output = output_plan("first", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::new(vec![Ok(write_report(
            &output,
            1,
            1,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
        ))]);
        let attempted = writer.attempted_outputs();

        let error = write_mssql_outputs_with_writer(
            vec![job(output)?],
            MssqlWorkflowWriteOptions::new().with_max_parallel_outputs(0),
            writer,
        )
        .await;
        let Err(error) = error else {
            return Err(test_error("zero writer config should be rejected"));
        };

        assert!(error.to_string().contains("must be at least 1"));
        assert!(locked(&attempted)?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn workflow_report_debug_and_display_redact_connection_credentials()
    -> Result<(), DeltaFunnelError> {
        let output = output_plan("first", LoadMode::AppendExisting)?;
        let report = write_report(
            &output,
            1,
            1,
            false,
            MssqlTargetCleanupStatus::NotApplicable,
        );
        let writer = FakeWorkflowWriter::new(vec![Ok(report)]);

        let report = write_mssql_outputs_with_writer(
            vec![job(output)?],
            MssqlWorkflowWriteOptions::default(),
            writer,
        )
        .await?;

        let debug = format!("{report:?}");
        let display = report.to_string();
        assert!(!debug.contains("secret"));
        assert!(!display.contains("secret"));
        assert!(display.contains("1 succeeded"));
        assert!(!display.to_lowercase().contains("transaction"));

        Ok(())
    }

    fn job(output_plan: MssqlTargetOutputPlan) -> Result<MssqlOutputWriteJob, DeltaFunnelError> {
        counted_job(output_plan, Arc::new(Mutex::new(Vec::new())))
    }

    fn counted_job(
        output_plan: MssqlTargetOutputPlan,
        factory_calls: Arc<Mutex<Vec<String>>>,
    ) -> Result<MssqlOutputWriteJob, DeltaFunnelError> {
        let output_name = output_plan.output_name().to_owned();
        Ok(MssqlOutputWriteJob::with_default_write_backend(
            output_schema(),
            resolved_target(output_plan)?,
            MssqlSchemaPlanOptions::default(),
            move || {
                if let Ok(mut calls) = factory_calls.lock() {
                    calls.push(output_name);
                }
                async { Ok(stream::empty()) }
            },
        )
        .with_phase_timings(planned_phase_timings()))
    }

    fn query_execution_job(
        output_plan: MssqlTargetOutputPlan,
        attachment_calls: Arc<Mutex<Vec<String>>>,
    ) -> Result<MssqlOutputWriteJob, DeltaFunnelError> {
        let attachment_output_name = output_plan.output_name().to_owned();
        Ok(MssqlOutputWriteJob::new_with_query_execution_factory(
            output_schema(),
            resolved_target(output_plan)?,
            MssqlSchemaPlanOptions::default(),
            Box::new(move || {
                Box::pin(async move {
                    Ok(MssqlOutputQueryExecution {
                        stream: Box::pin(stream::empty()),
                        query_phase_timings: vec![PhaseTimingReport::completed(
                            DEFERRED_QUERY_PHASE,
                            Duration::from_micros(1),
                        )],
                        attach_profile_to_result: Some(Box::new(move |result| {
                            if let Ok(mut calls) = attachment_calls.lock() {
                                let outcome = if result.is_ok() {
                                    "succeeded"
                                } else {
                                    "failed"
                                };
                                calls.push(format!("{attachment_output_name}:{outcome}"));
                            }
                            result
                        })),
                    })
                })
            }),
            default_mssql_write_backend(),
            ValidationOptions::default(),
        )
        .with_phase_timings(planned_phase_timings()))
    }

    fn failed_query_execution_job(
        output_plan: MssqlTargetOutputPlan,
    ) -> Result<MssqlOutputWriteJob, DeltaFunnelError> {
        Ok(MssqlOutputWriteJob::new_with_query_execution_factory(
            output_schema(),
            resolved_target(output_plan)?,
            MssqlSchemaPlanOptions::default(),
            Box::new(|| {
                Box::pin(async {
                    Err(MssqlOutputQueryError {
                        error: test_error("deferred setup failed"),
                        query_phase_timings: vec![PhaseTimingReport::failed(
                            DEFERRED_QUERY_PHASE,
                            Duration::from_micros(1),
                        )],
                    })
                })
            }),
            default_mssql_write_backend(),
            ValidationOptions::default(),
        )
        .with_phase_timings(planned_phase_timings()))
    }

    fn failing_factory_job(
        output_plan: MssqlTargetOutputPlan,
        factory_calls: Arc<Mutex<Vec<String>>>,
        message: &'static str,
    ) -> Result<MssqlOutputWriteJob, DeltaFunnelError> {
        let output_name = output_plan.output_name().to_owned();
        Ok(MssqlOutputWriteJob::with_default_write_backend(
            output_schema(),
            resolved_target(output_plan)?,
            MssqlSchemaPlanOptions::default(),
            move || {
                if let Ok(mut calls) = factory_calls.lock() {
                    calls.push(output_name);
                }
                async move {
                    Err::<stream::Empty<Result<RecordBatch, DeltaFunnelError>>, DeltaFunnelError>(
                        test_error(message),
                    )
                }
            },
        )
        .with_phase_timings(planned_phase_timings()))
    }

    fn polling_error_job(
        output_plan: MssqlTargetOutputPlan,
        factory_calls: Arc<Mutex<Vec<String>>>,
    ) -> Result<MssqlOutputWriteJob, DeltaFunnelError> {
        let output_name = output_plan.output_name().to_owned();
        Ok(MssqlOutputWriteJob::with_default_write_backend(
            output_schema(),
            resolved_target(output_plan)?,
            MssqlSchemaPlanOptions::default(),
            move || {
                if let Ok(mut calls) = factory_calls.lock() {
                    calls.push(output_name);
                }
                async {
                    Ok(stream::iter(vec![Err(
                        DeltaFunnelError::MssqlWorkflowPlanning {
                            message: "stream failed during polling".to_owned(),
                        },
                    )]))
                }
            },
        )
        .with_phase_timings(planned_phase_timings()))
    }

    fn planned_phase_timings() -> Vec<PhaseTimingReport> {
        vec![PhaseTimingReport::completed(
            PLANNED_PHASE,
            Duration::from_micros(1),
        )]
    }

    fn resolved_target(
        output_plan: MssqlTargetOutputPlan,
    ) -> Result<ResolvedMssqlTarget, DeltaFunnelError> {
        let connection = secret_connection()?;

        MssqlTargetConfig::new(output_plan.target_table().clone())
            .with_load_mode(output_plan.load_mode())
            .resolve(MssqlTargetResolutionContext {
                output_name: Some(output_plan.output_name()),
                default_connection: Some(&connection),
            })
    }

    fn output_plan(
        output_name: &str,
        load_mode: LoadMode,
    ) -> Result<MssqlTargetOutputPlan, DeltaFunnelError> {
        let connection = secret_connection()?;
        let target = MssqlTargetConfig::new(MssqlTargetTable::new(
            "dbo",
            format!("{output_name}_orders"),
        )?)
        .with_load_mode(load_mode);
        plan_mssql_target_for_output(
            output_schema(),
            output_name,
            &target,
            Some(&connection),
            MssqlSchemaPlanOptions::default(),
        )
    }

    fn output_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "order_id",
            DataType::Int64,
            false,
        )]))
    }

    fn secret_connection() -> Result<MssqlConnectionConfig, DeltaFunnelError> {
        Ok(MssqlConnectionConfig::new(
            "server=tcp:example.invalid,1433;user id=sa;password=secret",
        )?
        .with_display_label("test connection"))
    }

    fn write_report(
        output_plan: &MssqlTargetOutputPlan,
        rows_written: u64,
        batches_written: u64,
        partial_write_possible: bool,
        cleanup: MssqlTargetCleanupStatus,
    ) -> MssqlWriteReport {
        MssqlWriteReport::from_output_plan(
            output_plan,
            rows_written,
            batches_written,
            0,
            partial_write_possible,
            cleanup,
        )
    }

    fn phase_error(
        output_plan: &MssqlTargetOutputPlan,
        phase: MssqlWritePhase,
        rows_written: u64,
        batches_written: u64,
        partial_write_possible: bool,
        cleanup: MssqlTargetCleanupStatus,
        message: &str,
    ) -> DeltaFunnelError {
        phase_error_with_context(
            MssqlWriteFailureContext::from_output_plan(
                output_plan,
                phase,
                rows_written,
                batches_written,
                0,
                partial_write_possible,
                cleanup,
            ),
            message,
        )
    }

    fn phase_error_with_context(
        context: MssqlWriteFailureContext,
        message: &str,
    ) -> DeltaFunnelError {
        DeltaFunnelError::MssqlWritePhase {
            context: Box::new(context),
            message: message.to_owned(),
        }
    }

    fn assert_status_output(
        statuses: &[MssqlOutputWriteStatus],
        index: usize,
        expected_output_name: &str,
    ) -> Result<(), DeltaFunnelError> {
        match statuses.get(index) {
            Some(MssqlOutputWriteStatus::Succeeded(report)) => {
                assert_eq!(report.output_name(), expected_output_name);
                Ok(())
            }
            Some(other) => Err(test_error(format!(
                "expected success at index {index}, got {other:?}"
            ))),
            None => Err(test_error(format!("missing status at index {index}"))),
        }
    }

    fn assert_skipped_after(
        status: &MssqlOutputWriteStatus,
        expected_output_name: &str,
        expected_failed_output_name: &str,
    ) -> Result<(), DeltaFunnelError> {
        let MssqlOutputWriteStatus::Skipped(skipped) = status else {
            return Err(test_error(format!(
                "expected skipped status, got {status:?}"
            )));
        };
        assert_eq!(skipped.output_name(), expected_output_name);
        assert_eq!(
            skipped.reason(),
            &MssqlWriteSkippedReason::PreviousOutputFailed {
                failed_output_name: expected_failed_output_name.to_owned()
            }
        );
        assert_eq!(status.output_row_count(), RowCount::unavailable());
        assert_batch_shaping(
            status.batch_shaping(),
            PhaseStatus::skipped(ReportReasonCode::PriorFailure),
            0,
            0,
            0,
            0,
        );
        assert_phase_timing(status, PLANNED_PHASE, PhaseStatus::completed())?;
        assert_phase_timing(
            status,
            OUTPUT_STREAM_SETUP_PHASE,
            PhaseStatus::skipped(ReportReasonCode::PriorFailure),
        )?;
        assert_phase_timing(
            status,
            SQL_WRITE_PHASE,
            PhaseStatus::skipped(ReportReasonCode::PriorFailure),
        )?;
        assert_phase_timing(
            status,
            VALIDATION_PHASE,
            PhaseStatus::skipped(ReportReasonCode::PriorFailure),
        )?;
        Ok(())
    }

    fn assert_phase_timing(
        status: &MssqlOutputWriteStatus,
        phase_name: &str,
        expected_status: PhaseStatus,
    ) -> Result<(), DeltaFunnelError> {
        let timing = status
            .phase_timings()
            .iter()
            .find(|timing| timing.phase_name() == phase_name)
            .ok_or_else(|| test_error(format!("missing phase timing {phase_name}")))?;

        assert_eq!(timing.status(), expected_status);
        if expected_status.is_completed() || expected_status.is_failed() {
            assert!(timing.elapsed_micros().is_some());
        } else {
            assert_eq!(timing.elapsed_micros(), None);
        }
        Ok(())
    }

    fn assert_batch_shaping(
        report: MssqlBatchShapingReport,
        expected_status: PhaseStatus,
        expected_input_batches: u64,
        expected_input_rows: u64,
        expected_output_batches: u64,
        expected_output_rows: u64,
    ) {
        assert_eq!(report.status(), expected_status);
        assert_eq!(report.input_batches(), expected_input_batches);
        assert_eq!(report.input_rows(), expected_input_rows);
        assert_eq!(report.output_batches(), expected_output_batches);
        assert_eq!(report.output_rows(), expected_output_rows);
    }

    fn locked<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, DeltaFunnelError> {
        mutex.lock().map_err(|_| test_error("mutex lock poisoned"))
    }

    fn test_error(message: impl Into<String>) -> DeltaFunnelError {
        DeltaFunnelError::MssqlWorkflowPlanning {
            message: message.into(),
        }
    }
}
