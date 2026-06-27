//! SQL Server write options.
//!
//! This module owns DeltaFunnel-side write defaults around `arrow-tiberius`.

use std::{
    fmt,
    time::{Duration, Instant},
};

use arrow_schema::Schema;
pub use arrow_tiberius::WriteOptions as MssqlWriteOptions;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use futures_util::{
    Stream, StreamExt,
    io::{AsyncRead, AsyncWrite},
    pin_mut,
};

use crate::{
    DeltaFunnelError, PhaseTimingReport, ReportReasonCode, RowCount,
    report::sql_server::{
        MssqlBatchShapingReport, MssqlOutputBatchValidationReport, MssqlTargetCleanupStatus,
        MssqlWriteFailureContext, MssqlWriteReport, MssqlWriteReportMetrics,
    },
};

const POLL_BATCH_STREAM_PHASE: &str = "poll_batch_stream";
const VALIDATE_BATCH_SCHEMA_PHASE: &str = "validate_batch_schema";
const WRITE_BATCH_PHASE: &str = "write_batch";
const FINALIZE_PHASE: &str = "finalize";
const VALIDATION_PHASE: &str = "validation";

use super::{
    LoadMode, MssqlLifecycleExecutionGuardrail, MssqlPreparedTarget, MssqlPreparedTargetAction,
    MssqlTargetOutputPlan,
};

/// Fakeable bulk-load writer boundary for one planned SQL Server output.
#[async_trait]
pub(crate) trait MssqlBulkLoadWriter: Sized + Send {
    /// Writes one record batch.
    async fn write_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<arrow_tiberius::WriteStats, arrow_tiberius::Error>;

    /// Finalizes the writer and consumes it, matching `arrow-tiberius`.
    async fn finish(self) -> Result<arrow_tiberius::WriteStats, arrow_tiberius::Error>;
}

#[async_trait]
impl<'client, S> MssqlBulkLoadWriter for arrow_tiberius::BulkWriter<'client, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    async fn write_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<arrow_tiberius::WriteStats, arrow_tiberius::Error> {
        arrow_tiberius::BulkWriter::write_batch(self, batch).await
    }

    async fn finish(self) -> Result<arrow_tiberius::WriteStats, arrow_tiberius::Error> {
        arrow_tiberius::BulkWriter::finish(self).await
    }
}

#[async_trait]
impl MssqlBulkLoadWriter for arrow_tiberius::ConnectedBulkWriter<'_> {
    async fn write_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<arrow_tiberius::WriteStats, arrow_tiberius::Error> {
        arrow_tiberius::ConnectedBulkWriter::write_batch(self, batch).await
    }

    async fn finish(self) -> Result<arrow_tiberius::WriteStats, arrow_tiberius::Error> {
        arrow_tiberius::ConnectedBulkWriter::finish(self).await
    }
}

/// Fakeable boundary for constructing one SQL Server bulk writer.
#[allow(dead_code)]
#[async_trait]
pub(crate) trait MssqlBulkWriterFactory: Send {
    /// Writer type returned by this factory.
    type Writer: MssqlBulkLoadWriter;

    /// Constructs a bulk writer from an already prepared target request.
    async fn initialize(
        self,
        request: MssqlBulkWriterInitializationRequest,
    ) -> Result<Self::Writer, arrow_tiberius::Error>;
}

/// Production bulk writer factory for an already connected SQL Server client.
#[allow(dead_code)]
pub(crate) struct MssqlConnectedBulkWriterFactory<'client> {
    client: &'client mut arrow_tiberius::ConnectedMssqlClient,
}

impl<'client> MssqlConnectedBulkWriterFactory<'client> {
    /// Wraps the already connected SQL Server client used for lifecycle work.
    #[must_use]
    pub(crate) const fn new(client: &'client mut arrow_tiberius::ConnectedMssqlClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl<'client> MssqlBulkWriterFactory for MssqlConnectedBulkWriterFactory<'client> {
    type Writer = arrow_tiberius::ConnectedBulkWriter<'client>;

    async fn initialize(
        self,
        request: MssqlBulkWriterInitializationRequest,
    ) -> Result<Self::Writer, arrow_tiberius::Error> {
        let MssqlBulkWriterInitializationRequest {
            table,
            mappings,
            options,
            ..
        } = request;

        self.client.bulk_writer(table, mappings, options).await
    }
}

/// Planned inputs for constructing one SQL Server bulk writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MssqlBulkWriterInitializationRequest {
    output_name: String,
    table: arrow_tiberius::TableName,
    mappings: Vec<arrow_tiberius::SchemaMapping>,
    options: MssqlWriteOptions,
    prepared_action: MssqlPreparedTargetAction,
    cleanup: MssqlTargetCleanupStatus,
}

#[allow(dead_code)]
impl MssqlBulkWriterInitializationRequest {
    /// Builds writer initialization inputs from a previously prepared target.
    pub(crate) fn from_prepared_target(
        output_plan: &MssqlTargetOutputPlan,
        prepared_target: &MssqlPreparedTarget,
        options: MssqlWriteOptions,
    ) -> Result<Self, DeltaFunnelError> {
        ensure_prepared_target_matches_output_plan(output_plan, prepared_target)?;

        Ok(Self {
            output_name: output_plan.output_name().to_owned(),
            table: prepared_target.table_name().clone(),
            mappings: output_plan.schema_mappings().to_vec(),
            options,
            prepared_action: prepared_target.report().action(),
            cleanup: prepared_target.report().cleanup(),
        })
    }

    /// Returns the selected output name.
    #[must_use]
    pub(crate) fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns the prepared SQL Server table identity.
    #[must_use]
    pub(crate) const fn table(&self) -> &arrow_tiberius::TableName {
        &self.table
    }

    /// Returns the planned schema mappings passed to the writer.
    #[must_use]
    pub(crate) fn mappings(&self) -> &[arrow_tiberius::SchemaMapping] {
        &self.mappings
    }

    /// Returns the write options passed to the writer.
    #[must_use]
    pub(crate) const fn options(&self) -> MssqlWriteOptions {
        self.options
    }

    /// Returns the prepared lifecycle action that permits writer initialization.
    #[must_use]
    pub(crate) const fn prepared_action(&self) -> MssqlPreparedTargetAction {
        self.prepared_action
    }

    /// Returns cleanup state if writer initialization fails.
    #[must_use]
    pub(crate) const fn cleanup(&self) -> MssqlTargetCleanupStatus {
        self.cleanup
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

/// Returns DeltaFunnel's default SQL Server write options.
#[must_use]
pub fn default_mssql_write_options() -> MssqlWriteOptions {
    MssqlWriteOptions {
        backend: arrow_tiberius::WriteBackend::DirectRawBulk,
        ..MssqlWriteOptions::default()
    }
}

/// Builds write options from a planned SQL Server output target.
#[must_use]
pub fn mssql_write_options_for_output_plan(
    output_plan: &MssqlTargetOutputPlan,
) -> MssqlWriteOptions {
    MssqlWriteOptions {
        plan_options: output_plan.schema_plan_options(),
        ..default_mssql_write_options()
    }
}

/// Initializes one SQL Server bulk writer after target lifecycle preparation.
#[allow(dead_code)]
pub(crate) async fn initialize_mssql_bulk_writer<'client>(
    client: &'client mut arrow_tiberius::ConnectedMssqlClient,
    output_plan: &MssqlTargetOutputPlan,
    prepared_target: &MssqlPreparedTarget,
    options: MssqlWriteOptions,
) -> Result<arrow_tiberius::ConnectedBulkWriter<'client>, DeltaFunnelError> {
    initialize_mssql_bulk_writer_with_factory(
        output_plan,
        prepared_target,
        options,
        MssqlConnectedBulkWriterFactory::new(client),
    )
    .await
}

/// Initializes one SQL Server bulk writer through an injected factory.
#[allow(dead_code)]
pub(crate) async fn initialize_mssql_bulk_writer_with_factory<F>(
    output_plan: &MssqlTargetOutputPlan,
    prepared_target: &MssqlPreparedTarget,
    options: MssqlWriteOptions,
    factory: F,
) -> Result<F::Writer, DeltaFunnelError>
where
    F: MssqlBulkWriterFactory,
{
    let request = MssqlBulkWriterInitializationRequest::from_prepared_target(
        output_plan,
        prepared_target,
        options,
    )?;
    let cleanup = request.cleanup();
    let prepared_action = request.prepared_action();

    factory.initialize(request).await.map_err(|source| {
        mssql_writer_initialization_error(output_plan, prepared_action, cleanup, source.to_string())
    })
}

/// Validates a runtime Arrow schema against a planned SQL Server output.
///
/// DeltaFunnel owns the output context and redacted report shape, while the
/// schema contract comparison is delegated to `arrow-tiberius`.
pub fn validate_mssql_output_schema(
    output_plan: &MssqlTargetOutputPlan,
    schema: &Schema,
) -> Result<MssqlOutputBatchValidationReport, DeltaFunnelError> {
    arrow_tiberius::validate_arrow_schema_against_mappings(schema, output_plan.schema_mappings())
        .map_err(|source| {
        mssql_batch_schema_validation_error(
            output_plan,
            source,
            write_report_metrics(
                RowCount::unavailable(),
                MssqlBatchShapingReport::not_started(ReportReasonCode::NotExecuted),
                MssqlWriteProgress::zero(),
                false,
                MssqlTargetCleanupStatus::NotApplicable,
            ),
        )
    })?;

    Ok(MssqlOutputBatchValidationReport::from_output_plan(
        output_plan,
    ))
}

/// Validates a runtime `RecordBatch` schema against a planned SQL Server output.
///
/// This helper validates `batch.schema()` before row writes and does not inspect
/// row values, connect to SQL Server, or construct a writer.
pub fn validate_mssql_output_record_batch(
    output_plan: &MssqlTargetOutputPlan,
    batch: &RecordBatch,
) -> Result<MssqlOutputBatchValidationReport, DeltaFunnelError> {
    arrow_tiberius::validate_record_batch_schema_against_mappings(
        batch,
        output_plan.schema_mappings(),
    )
    .map_err(|source| {
        mssql_batch_schema_validation_error(
            output_plan,
            source,
            write_report_metrics(
                RowCount::unavailable(),
                MssqlBatchShapingReport::not_started(ReportReasonCode::NotExecuted),
                MssqlWriteProgress::zero(),
                false,
                MssqlTargetCleanupStatus::NotApplicable,
            ),
        )
    })?;

    Ok(MssqlOutputBatchValidationReport::from_output_plan(
        output_plan,
    ))
}

fn mssql_batch_schema_validation_error(
    output_plan: &MssqlTargetOutputPlan,
    source: arrow_tiberius::Error,
    metrics: MssqlWriteReportMetrics,
) -> DeltaFunnelError {
    DeltaFunnelError::MssqlBatchSchemaValidation {
        context: Box::new(MssqlWriteFailureContext::from_output_plan_with_metrics(
            output_plan,
            MssqlWritePhase::ValidateBatchSchema,
            metrics,
        )),
        source,
    }
}

fn write_report_metrics(
    output_row_count: RowCount,
    batch_shaping: MssqlBatchShapingReport,
    progress: MssqlWriteProgress,
    partial_write_possible: bool,
    cleanup: MssqlTargetCleanupStatus,
) -> MssqlWriteReportMetrics {
    MssqlWriteReportMetrics::new(
        output_row_count,
        batch_shaping,
        progress.rows_written,
        progress.batches_written,
        progress.elapsed_ms,
        partial_write_possible,
        cleanup,
    )
}

#[derive(Default)]
struct MssqlWriteLoopPhaseTimings {
    poll_batch_stream: Duration,
    validate_batch_schema: Duration,
    write_batch: Duration,
    validation_attempted: bool,
    write_attempted: bool,
}

impl MssqlWriteLoopPhaseTimings {
    fn add_poll_batch_stream(&mut self, elapsed: Duration) {
        self.poll_batch_stream = self.poll_batch_stream.saturating_add(elapsed);
    }

    fn add_validate_batch_schema(&mut self, elapsed: Duration) {
        self.validation_attempted = true;
        self.validate_batch_schema = self.validate_batch_schema.saturating_add(elapsed);
    }

    fn add_write_batch(&mut self, elapsed: Duration) {
        self.write_attempted = true;
        self.write_batch = self.write_batch.saturating_add(elapsed);
    }

    fn completed(&self, finalize_elapsed: Duration) -> Vec<PhaseTimingReport> {
        vec![
            PhaseTimingReport::completed(POLL_BATCH_STREAM_PHASE, self.poll_batch_stream),
            self.validate_batch_schema_completed_or_not_started(),
            self.write_batch_completed_or_not_started(),
            PhaseTimingReport::completed(FINALIZE_PHASE, finalize_elapsed),
            PhaseTimingReport::not_started(VALIDATION_PHASE, ReportReasonCode::NotExecuted),
        ]
    }

    fn poll_batch_stream_failed(&self) -> Vec<PhaseTimingReport> {
        vec![
            PhaseTimingReport::failed(POLL_BATCH_STREAM_PHASE, self.poll_batch_stream),
            self.validate_batch_schema_completed_or_not_started(),
            self.write_batch_completed_or_not_started(),
            PhaseTimingReport::not_started(FINALIZE_PHASE, ReportReasonCode::NotExecuted),
            PhaseTimingReport::not_started(
                VALIDATION_PHASE,
                ReportReasonCode::FailureBeforeValidation,
            ),
        ]
    }

    fn validate_batch_schema_failed(&self) -> Vec<PhaseTimingReport> {
        vec![
            PhaseTimingReport::completed(POLL_BATCH_STREAM_PHASE, self.poll_batch_stream),
            PhaseTimingReport::failed(VALIDATE_BATCH_SCHEMA_PHASE, self.validate_batch_schema),
            self.write_batch_completed_or_not_started(),
            PhaseTimingReport::not_started(FINALIZE_PHASE, ReportReasonCode::NotExecuted),
            PhaseTimingReport::not_started(
                VALIDATION_PHASE,
                ReportReasonCode::FailureBeforeValidation,
            ),
        ]
    }

    fn write_batch_failed(&self) -> Vec<PhaseTimingReport> {
        vec![
            PhaseTimingReport::completed(POLL_BATCH_STREAM_PHASE, self.poll_batch_stream),
            self.validate_batch_schema_completed_or_not_started(),
            PhaseTimingReport::failed(WRITE_BATCH_PHASE, self.write_batch),
            PhaseTimingReport::not_started(FINALIZE_PHASE, ReportReasonCode::NotExecuted),
            PhaseTimingReport::not_started(
                VALIDATION_PHASE,
                ReportReasonCode::FailureBeforeValidation,
            ),
        ]
    }

    fn finalize_failed(&self, finalize_elapsed: Duration) -> Vec<PhaseTimingReport> {
        vec![
            PhaseTimingReport::completed(POLL_BATCH_STREAM_PHASE, self.poll_batch_stream),
            self.validate_batch_schema_completed_or_not_started(),
            self.write_batch_completed_or_not_started(),
            PhaseTimingReport::failed(FINALIZE_PHASE, finalize_elapsed),
            PhaseTimingReport::not_started(
                VALIDATION_PHASE,
                ReportReasonCode::FailureBeforeValidation,
            ),
        ]
    }

    fn validate_batch_schema_completed_or_not_started(&self) -> PhaseTimingReport {
        if self.validation_attempted {
            PhaseTimingReport::completed(VALIDATE_BATCH_SCHEMA_PHASE, self.validate_batch_schema)
        } else {
            PhaseTimingReport::not_started(
                VALIDATE_BATCH_SCHEMA_PHASE,
                ReportReasonCode::NotExecuted,
            )
        }
    }

    fn write_batch_completed_or_not_started(&self) -> PhaseTimingReport {
        if self.write_attempted {
            PhaseTimingReport::completed(WRITE_BATCH_PHASE, self.write_batch)
        } else {
            PhaseTimingReport::not_started(WRITE_BATCH_PHASE, ReportReasonCode::NotExecuted)
        }
    }
}

/// Writes one planned SQL Server output through an injected bulk-load writer.
#[allow(dead_code)]
pub(crate) async fn write_mssql_batches_with_writer<W, S>(
    output_plan: &MssqlTargetOutputPlan,
    batches: S,
    mut writer: W,
    _options: MssqlWriteOptions,
) -> Result<MssqlWriteReport, DeltaFunnelError>
where
    W: MssqlBulkLoadWriter,
    S: Stream<Item = Result<RecordBatch, DeltaFunnelError>>,
{
    let mut rows_written = 0_u64;
    let mut batches_written = 0_u64;
    let mut input_rows = 0_u64;
    let mut input_batches = 0_u64;
    let mut shaped_rows = 0_u64;
    let mut shaped_batches = 0_u64;
    let cleanup = MssqlTargetCleanupStatus::NotApplicable;
    let started_at = Instant::now();
    let mut phase_timings = MssqlWriteLoopPhaseTimings::default();
    pin_mut!(batches);

    loop {
        let poll_started_at = Instant::now();
        let Some(batch) = batches.next().await else {
            phase_timings.add_poll_batch_stream(poll_started_at.elapsed());
            break;
        };
        phase_timings.add_poll_batch_stream(poll_started_at.elapsed());

        let batch = batch.map_err(|source| {
            let elapsed_ms = elapsed_ms_since(started_at);
            mssql_write_phase_error(
                output_plan,
                MssqlWritePhase::PollBatchStream,
                write_report_metrics(
                    RowCount::partial(input_rows),
                    MssqlBatchShapingReport::failed(
                        input_batches,
                        input_rows,
                        shaped_batches,
                        shaped_rows,
                    ),
                    MssqlWriteProgress::new(rows_written, batches_written, elapsed_ms),
                    partial_write_possible(output_plan, rows_written, batches_written),
                    cleanup,
                )
                .with_phase_timings(phase_timings.poll_batch_stream_failed()),
                source.to_string(),
            )
        })?;

        let row_count = batch_row_count(batch.num_rows());
        input_rows = input_rows.saturating_add(row_count);
        input_batches = input_batches.saturating_add(1);

        let validation_started_at = Instant::now();
        let validation_result = arrow_tiberius::validate_record_batch_schema_against_mappings(
            &batch,
            output_plan.schema_mappings(),
        );
        phase_timings.add_validate_batch_schema(validation_started_at.elapsed());
        validation_result.map_err(|source| {
            let elapsed_ms = elapsed_ms_since(started_at);
            mssql_batch_schema_validation_error(
                output_plan,
                source,
                write_report_metrics(
                    RowCount::partial(input_rows),
                    MssqlBatchShapingReport::failed(
                        input_batches,
                        input_rows,
                        shaped_batches,
                        shaped_rows,
                    ),
                    MssqlWriteProgress::new(rows_written, batches_written, elapsed_ms),
                    partial_write_possible(output_plan, rows_written, batches_written),
                    cleanup,
                )
                .with_phase_timings(phase_timings.validate_batch_schema_failed()),
            )
        })?;

        let write_batch_started_at = Instant::now();
        let write_batch_result = MssqlBulkLoadWriter::write_batch(&mut writer, &batch).await;
        phase_timings.add_write_batch(write_batch_started_at.elapsed());
        write_batch_result.map_err(|source| {
            let elapsed_ms = elapsed_ms_since(started_at);
            mssql_write_phase_error(
                output_plan,
                MssqlWritePhase::WriteBatch,
                write_report_metrics(
                    RowCount::partial(input_rows),
                    MssqlBatchShapingReport::failed(
                        input_batches,
                        input_rows,
                        shaped_batches,
                        shaped_rows,
                    ),
                    MssqlWriteProgress::new(rows_written, batches_written, elapsed_ms),
                    partial_write_possible(output_plan, rows_written, batches_written),
                    cleanup,
                )
                .with_phase_timings(phase_timings.write_batch_failed()),
                source.to_string(),
            )
        })?;

        rows_written = rows_written.saturating_add(row_count);
        batches_written = batches_written.saturating_add(1);
        shaped_rows = shaped_rows.saturating_add(row_count);
        shaped_batches = shaped_batches.saturating_add(1);
    }

    let finalize_started_at = Instant::now();
    let finish_result = MssqlBulkLoadWriter::finish(writer).await;
    let finalize_elapsed = finalize_started_at.elapsed();
    finish_result.map_err(|source| {
        let elapsed_ms = elapsed_ms_since(started_at);
        mssql_write_phase_error(
            output_plan,
            MssqlWritePhase::Finalize,
            write_report_metrics(
                RowCount::exact(input_rows),
                MssqlBatchShapingReport::completed(
                    input_batches,
                    input_rows,
                    shaped_batches,
                    shaped_rows,
                ),
                MssqlWriteProgress::new(rows_written, batches_written, elapsed_ms),
                partial_write_possible(output_plan, rows_written, batches_written),
                cleanup,
            )
            .with_phase_timings(phase_timings.finalize_failed(finalize_elapsed)),
            source.to_string(),
        )
    })?;

    Ok(MssqlWriteReport::from_output_plan_with_metrics(
        output_plan,
        write_report_metrics(
            RowCount::exact(input_rows),
            MssqlBatchShapingReport::completed(
                input_batches,
                input_rows,
                shaped_batches,
                shaped_rows,
            ),
            MssqlWriteProgress::new(rows_written, batches_written, elapsed_ms_since(started_at)),
            false,
            cleanup,
        )
        .with_phase_timings(phase_timings.completed(finalize_elapsed)),
    ))
}

fn mssql_write_phase_error(
    output_plan: &MssqlTargetOutputPlan,
    phase: MssqlWritePhase,
    metrics: MssqlWriteReportMetrics,
    message: String,
) -> DeltaFunnelError {
    DeltaFunnelError::MssqlWritePhase {
        context: Box::new(MssqlWriteFailureContext::from_output_plan_with_metrics(
            output_plan,
            phase,
            metrics,
        )),
        message,
    }
}

fn mssql_writer_initialization_error(
    output_plan: &MssqlTargetOutputPlan,
    prepared_action: MssqlPreparedTargetAction,
    cleanup: MssqlTargetCleanupStatus,
    message: String,
) -> DeltaFunnelError {
    mssql_write_phase_error(
        output_plan,
        MssqlWritePhase::InitializeWriter,
        write_report_metrics(
            RowCount::unavailable(),
            MssqlBatchShapingReport::not_started(ReportReasonCode::NotExecuted),
            MssqlWriteProgress::zero(),
            false,
            cleanup,
        ),
        format!("prepared target action {prepared_action:?}: {message}"),
    )
}

fn ensure_prepared_target_matches_output_plan(
    output_plan: &MssqlTargetOutputPlan,
    prepared_target: &MssqlPreparedTarget,
) -> Result<(), DeltaFunnelError> {
    let report = prepared_target.report();
    let matches_plan = report.output_name() == output_plan.output_name()
        && report.target_table() == output_plan.target_table()
        && report.load_mode() == output_plan.load_mode()
        && report.connection_source() == output_plan.connection_source()
        && report.connection() == output_plan.connection()
        && report.expected_target_state() == output_plan.lifecycle_plan().expected_target_state();

    if !matches_plan {
        return Err(mssql_writer_initialization_error(
            output_plan,
            report.action(),
            report.cleanup(),
            "prepared target does not match output plan".to_owned(),
        ));
    }

    if !output_plan
        .lifecycle_plan()
        .execution_guardrails()
        .contains(&MssqlLifecycleExecutionGuardrail::BulkWriterConstruction)
    {
        return Err(mssql_writer_initialization_error(
            output_plan,
            report.action(),
            report.cleanup(),
            "target lifecycle plan does not allow BulkWriterConstruction".to_owned(),
        ));
    }

    Ok(())
}

fn partial_write_possible(
    output_plan: &MssqlTargetOutputPlan,
    rows_written: u64,
    batches_written: u64,
) -> bool {
    output_plan.load_mode() == LoadMode::AppendExisting && (rows_written > 0 || batches_written > 0)
}

#[derive(Clone, Copy)]
struct MssqlWriteProgress {
    rows_written: u64,
    batches_written: u64,
    elapsed_ms: u64,
}

impl MssqlWriteProgress {
    const fn new(rows_written: u64, batches_written: u64, elapsed_ms: u64) -> Self {
        Self {
            rows_written,
            batches_written,
            elapsed_ms,
        }
    }

    const fn zero() -> Self {
        Self::new(0, 0, 0)
    }
}

fn elapsed_ms_since(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn batch_row_count(row_count: usize) -> u64 {
    u64::try_from(row_count).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex, MutexGuard},
        time::Duration,
    };

    use arrow_schema::{DataType, Field, Schema};
    use arrow_tiberius::{
        DiagnosticCode, PlanOptions, SchemaCheck, StringPolicy, WriteBackend, WriteOptions,
    };
    use datafusion::arrow::{
        array::{Int64Array, StringArray},
        record_batch::RecordBatch,
    };
    use futures_util::stream;

    use super::*;
    use crate::{
        DeltaFunnelError, MssqlBatchShapingReport, MssqlConnectionConfig, MssqlConnectionSource,
        MssqlOutputFieldReport, MssqlTargetConfig, MssqlTargetTable, MssqlWriteStats, PhaseStatus,
        PhaseTimingReport, ReportReasonCode, ValidationStatus, plan_mssql_target_for_output,
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

    #[derive(Debug, Default)]
    struct FakeBulkLoadWriter {
        accepted_rows: u64,
        accepted_batches: u64,
        batch_rows: Vec<usize>,
        log: Option<Arc<Mutex<FakeBulkLoadWriterLog>>>,
        fail_on_write_batch: Option<u64>,
        fail_on_finish: bool,
        write_delay: Option<Duration>,
    }

    #[derive(Debug, Default, PartialEq, Eq)]
    struct FakeBulkLoadWriterLog {
        batch_rows: Vec<usize>,
        finish_count: u64,
    }

    #[derive(Clone, Default)]
    struct RecordingBulkWriterFactory {
        request: Arc<Mutex<Option<MssqlBulkWriterInitializationRequest>>>,
        error: Option<String>,
    }

    impl FakeBulkLoadWriter {
        fn with_log(log: Arc<Mutex<FakeBulkLoadWriterLog>>) -> Self {
            Self {
                log: Some(log),
                ..Self::default()
            }
        }

        fn fail_on_write_batch(mut self, write_batch_call: u64) -> Self {
            self.fail_on_write_batch = Some(write_batch_call);
            self
        }

        fn delay_writes_by(mut self, delay: Duration) -> Self {
            self.write_delay = Some(delay);
            self
        }

        fn fail_on_finish(mut self) -> Self {
            self.fail_on_finish = true;
            self
        }
    }

    impl RecordingBulkWriterFactory {
        fn with_request_log(
            request: Arc<Mutex<Option<MssqlBulkWriterInitializationRequest>>>,
        ) -> Self {
            Self {
                request,
                error: None,
            }
        }

        fn fail_with(mut self, error: impl Into<String>) -> Self {
            self.error = Some(error.into());
            self
        }
    }

    #[async_trait]
    impl MssqlBulkLoadWriter for FakeBulkLoadWriter {
        async fn write_batch(
            &mut self,
            batch: &RecordBatch,
        ) -> Result<arrow_tiberius::WriteStats, arrow_tiberius::Error> {
            let row_count = batch.num_rows();
            self.batch_rows.push(row_count);
            if let Some(log) = &self.log {
                let Ok(mut log) = log.lock() else {
                    return Err(arrow_tiberius::Error::BackendUnavailable {
                        backend: arrow_tiberius::WriteBackend::DirectRawBulk,
                        reason: "fake writer log mutex was poisoned".to_owned(),
                    });
                };
                log.batch_rows.push(row_count);
            }
            if let Some(delay) = self.write_delay {
                tokio::time::sleep(delay).await;
            }
            let write_batch_call = self.accepted_batches.saturating_add(1);
            if self.fail_on_write_batch == Some(write_batch_call) {
                return Err(arrow_tiberius::Error::BackendUnavailable {
                    backend: arrow_tiberius::WriteBackend::DirectRawBulk,
                    reason: format!("fake writer failed on batch {write_batch_call}"),
                });
            }
            self.accepted_rows = self.accepted_rows.saturating_add(row_count as u64);
            self.accepted_batches = self.accepted_batches.saturating_add(1);

            Ok(arrow_tiberius::WriteStats {
                rows_written: self.accepted_rows,
                batches_written: self.accepted_batches,
            })
        }

        async fn finish(self) -> Result<arrow_tiberius::WriteStats, arrow_tiberius::Error> {
            if let Some(log) = &self.log {
                let Ok(mut log) = log.lock() else {
                    return Err(arrow_tiberius::Error::BackendUnavailable {
                        backend: arrow_tiberius::WriteBackend::DirectRawBulk,
                        reason: "fake writer log mutex was poisoned".to_owned(),
                    });
                };
                log.finish_count = log.finish_count.saturating_add(1);
            }
            if self.fail_on_finish {
                return Err(arrow_tiberius::Error::BackendUnavailable {
                    backend: arrow_tiberius::WriteBackend::DirectRawBulk,
                    reason: "fake writer failed on finish".to_owned(),
                });
            }

            Ok(arrow_tiberius::WriteStats {
                rows_written: self.accepted_rows,
                batches_written: self.accepted_batches,
            })
        }
    }

    #[async_trait]
    impl MssqlBulkWriterFactory for RecordingBulkWriterFactory {
        type Writer = FakeBulkLoadWriter;

        async fn initialize(
            self,
            request: MssqlBulkWriterInitializationRequest,
        ) -> Result<Self::Writer, arrow_tiberius::Error> {
            let Ok(mut request_log) = self.request.lock() else {
                return Err(arrow_tiberius::Error::BackendUnavailable {
                    backend: arrow_tiberius::WriteBackend::DirectRawBulk,
                    reason: "fake initializer request log mutex was poisoned".to_owned(),
                });
            };
            *request_log = Some(request);

            if let Some(reason) = self.error {
                return Err(arrow_tiberius::Error::BackendUnavailable {
                    backend: arrow_tiberius::WriteBackend::DirectRawBulk,
                    reason,
                });
            }

            Ok(FakeBulkLoadWriter::default())
        }
    }

    fn orders_batch(
        order_ids: Vec<i64>,
        statuses: Vec<Option<&str>>,
    ) -> Result<RecordBatch, DeltaFunnelError> {
        RecordBatch::try_new(
            Arc::new(orders_schema()),
            vec![
                Arc::new(Int64Array::from(order_ids)),
                Arc::new(StringArray::from(statuses)),
            ],
        )
        .map_err(|error| DeltaFunnelError::Config {
            message: error.to_string(),
        })
    }

    fn orders_batch_with_int32_order_id() -> Result<RecordBatch, DeltaFunnelError> {
        let schema = Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("status", DataType::Utf8, true),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1_i32])),
                Arc::new(StringArray::from(vec![Some("open")])),
            ],
        )
        .map_err(|error| DeltaFunnelError::Config {
            message: error.to_string(),
        })
    }

    fn lock_fake_writer_log(
        log: &Arc<Mutex<FakeBulkLoadWriterLog>>,
    ) -> Result<MutexGuard<'_, FakeBulkLoadWriterLog>, DeltaFunnelError> {
        log.lock().map_err(|_| DeltaFunnelError::Config {
            message: "fake writer log mutex was poisoned".to_owned(),
        })
    }

    fn take_initialization_request(
        request: &Arc<Mutex<Option<MssqlBulkWriterInitializationRequest>>>,
    ) -> Result<MssqlBulkWriterInitializationRequest, DeltaFunnelError> {
        let mut request = request.lock().map_err(|_| DeltaFunnelError::Config {
            message: "fake initializer request log mutex was poisoned".to_owned(),
        })?;

        request.take().ok_or_else(|| DeltaFunnelError::Config {
            message: "expected fake initializer request".to_owned(),
        })
    }

    fn output_plan_for_orders_schema() -> Result<MssqlTargetOutputPlan, DeltaFunnelError> {
        output_plan_for_orders_schema_with_load_mode(LoadMode::AppendExisting)
    }

    fn output_plan_for_orders_schema_with_load_mode(
        load_mode: LoadMode,
    ) -> Result<MssqlTargetOutputPlan, DeltaFunnelError> {
        let connection = secret_connection()?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?)
            .with_load_mode(load_mode);

        plan_mssql_target_for_output(
            orders_schema(),
            "orders_output",
            &target_config,
            Some(&connection),
            PlanOptions::default(),
        )
    }

    fn assert_batch_schema_validation_error(
        error: DeltaFunnelError,
        expected_field: Option<(usize, &str)>,
    ) -> Result<Box<MssqlWriteFailureContext>, DeltaFunnelError> {
        let DeltaFunnelError::MssqlBatchSchemaValidation { context, source } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected MSSQL batch schema validation error".to_owned(),
            });
        };

        assert_eq!(context.phase(), MssqlWritePhase::ValidateBatchSchema);
        assert_eq!(context.output_name(), "orders_output");
        assert_eq!(context.target_table().schema(), Some("dbo"));
        assert_eq!(context.target_table().table(), "orders");
        assert_eq!(context.load_mode(), LoadMode::AppendExisting);
        assert!(!context.partial_write_possible());

        let arrow_tiberius::Error::ValueConversion { diagnostics } = source else {
            return Err(DeltaFunnelError::Config {
                message: "expected arrow-tiberius value conversion error".to_owned(),
            });
        };
        assert_eq!(diagnostics.len(), 1);
        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.code(), DiagnosticCode::SchemaMismatch);
        assert_eq!(
            diagnostic
                .field()
                .map(|field| (field.index(), field.name())),
            expected_field
        );
        Ok(context)
    }

    fn assert_write_phase_error(
        error: DeltaFunnelError,
        expected_phase: MssqlWritePhase,
        expected_rows_written: u64,
        expected_batches_written: u64,
        expected_partial_write_possible: bool,
    ) -> Result<Box<MssqlWriteFailureContext>, DeltaFunnelError> {
        let DeltaFunnelError::MssqlWritePhase { context, .. } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected MSSQL write phase error".to_owned(),
            });
        };

        assert_eq!(context.phase(), expected_phase);
        assert_eq!(context.output_name(), "orders_output");
        assert_eq!(context.target_table().schema(), Some("dbo"));
        assert_eq!(context.target_table().table(), "orders");
        assert_eq!(context.load_mode(), LoadMode::AppendExisting);
        assert_eq!(context.stats().rows_written(), expected_rows_written);
        assert_eq!(context.stats().batches_written(), expected_batches_written);
        assert_eq!(
            context.partial_write_possible(),
            expected_partial_write_possible
        );
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        Ok(context)
    }

    fn assert_output_schema(fields: &[MssqlOutputFieldReport]) {
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].index(), 0);
        assert_eq!(fields[0].name(), "order_id");
        assert_eq!(fields[0].arrow_type(), "Int64");
        assert!(!fields[0].nullable());
        assert_eq!(fields[1].index(), 1);
        assert_eq!(fields[1].name(), "status");
        assert_eq!(fields[1].arrow_type(), "Utf8");
        assert!(fields[1].nullable());
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

    fn assert_initialize_writer_error(
        error: DeltaFunnelError,
        expected_message: &str,
        expected_cleanup: MssqlTargetCleanupStatus,
    ) -> Result<String, DeltaFunnelError> {
        let display = error.to_string();
        assert!(display.contains("orders_output"));
        assert!(display.contains("initialize writer"));
        assert!(display.contains(expected_message));
        assert!(!display.contains("secret-token"));
        assert!(!display.contains("password"));
        assert!(!display.contains("server=tcp"));
        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected MSSQL write phase error".to_owned(),
            });
        };

        assert_eq!(context.phase(), MssqlWritePhase::InitializeWriter);
        assert_eq!(context.output_name(), "orders_output");
        assert_eq!(context.stats().rows_written(), 0);
        assert_eq!(context.stats().batches_written(), 0);
        assert!(!context.partial_write_possible());
        assert_eq!(context.cleanup(), expected_cleanup);
        Ok(message)
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
    fn connected_bulk_writer_adapts_to_bulk_load_writer_trait() {
        fn assert_bulk_load_writer<W: MssqlBulkLoadWriter>() {}

        assert_bulk_load_writer::<arrow_tiberius::ConnectedBulkWriter<'static>>();
    }

    #[tokio::test]
    async fn bulk_writer_initialization_passes_prepared_identity_mappings_and_options()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema()?;
        let prepared_target = MssqlPreparedTarget::from_output_plan(
            &output_plan,
            MssqlPreparedTargetAction::VerifiedExisting,
        )?;
        let options = WriteOptions {
            backend: WriteBackend::BaselineTokenRow,
            schema_check: SchemaCheck::Strict,
            plan_options: PlanOptions {
                string_policy: StringPolicy::NVarChar(128),
                ..PlanOptions::default()
            },
        };
        let request_log = Arc::new(Mutex::new(None));
        let factory = RecordingBulkWriterFactory::with_request_log(Arc::clone(&request_log));

        let writer = initialize_mssql_bulk_writer_with_factory(
            &output_plan,
            &prepared_target,
            options,
            factory,
        )
        .await?;
        let final_stats = MssqlBulkLoadWriter::finish(writer)
            .await
            .map_err(|source| DeltaFunnelError::MssqlWrite { source })?;
        let request = take_initialization_request(&request_log)?;

        assert_eq!(final_stats.rows_written, 0);
        assert_eq!(final_stats.batches_written, 0);
        assert_eq!(request.output_name(), "orders_output");
        assert_eq!(request.table().quoted_sql(), "[dbo].[orders]");
        assert_eq!(request.mappings(), output_plan.schema_mappings());
        assert_eq!(request.options(), options);
        assert_eq!(
            request.prepared_action(),
            MssqlPreparedTargetAction::VerifiedExisting
        );
        assert_eq!(request.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        Ok(())
    }

    #[tokio::test]
    async fn bulk_writer_initialization_errors_map_to_initialize_writer_phase()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema_with_load_mode(LoadMode::CreateAndLoad)?;
        let prepared_target = MssqlPreparedTarget::from_output_plan(
            &output_plan,
            MssqlPreparedTargetAction::CreatedTable,
        )?;
        let request_log = Arc::new(Mutex::new(None));
        let factory = RecordingBulkWriterFactory::with_request_log(Arc::clone(&request_log))
            .fail_with("target metadata failed\nfor test");

        let error = initialize_mssql_bulk_writer_with_factory(
            &output_plan,
            &prepared_target,
            default_mssql_write_options(),
            factory,
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected bulk writer initialization error".to_owned(),
        })?;
        let message = assert_initialize_writer_error(
            error,
            r"target metadata failed\nfor test",
            MssqlTargetCleanupStatus::NotAttempted,
        )?;
        let request = take_initialization_request(&request_log)?;

        assert!(message.contains("CreatedTable"));
        assert_eq!(request.cleanup(), MssqlTargetCleanupStatus::NotAttempted);
        assert_eq!(
            request.prepared_action(),
            MssqlPreparedTargetAction::CreatedTable
        );
        Ok(())
    }

    #[tokio::test]
    async fn bulk_writer_initialization_rejects_mismatched_prepared_target_before_factory()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema()?;
        let connection = secret_connection()?;
        let other_target_config =
            MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "other_orders")?);
        let other_output_plan = plan_mssql_target_for_output(
            orders_schema(),
            "other_output",
            &other_target_config,
            Some(&connection),
            PlanOptions::default(),
        )?;
        let prepared_target = MssqlPreparedTarget::from_output_plan(
            &other_output_plan,
            MssqlPreparedTargetAction::VerifiedExisting,
        )?;
        let request_log = Arc::new(Mutex::new(None));
        let factory = RecordingBulkWriterFactory::with_request_log(Arc::clone(&request_log));

        let error = initialize_mssql_bulk_writer_with_factory(
            &output_plan,
            &prepared_target,
            default_mssql_write_options(),
            factory,
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected mismatched prepared target error".to_owned(),
        })?;

        assert_initialize_writer_error(
            error,
            "prepared target does not match output plan",
            MssqlTargetCleanupStatus::NotApplicable,
        )?;
        assert!(take_initialization_request(&request_log).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn bulk_load_writer_trait_consumes_writer_on_finish() -> Result<(), DeltaFunnelError> {
        let mut writer = FakeBulkLoadWriter::default();
        let first = orders_batch(vec![1, 2], vec![Some("open"), Some("closed")])?;
        let second = orders_batch(vec![3], vec![None])?;

        let first_stats = MssqlBulkLoadWriter::write_batch(&mut writer, &first)
            .await
            .map_err(|source| DeltaFunnelError::MssqlWrite { source })?;
        let second_stats = MssqlBulkLoadWriter::write_batch(&mut writer, &second)
            .await
            .map_err(|source| DeltaFunnelError::MssqlWrite { source })?;
        assert_eq!(writer.batch_rows, vec![2, 1]);

        let final_stats = MssqlBulkLoadWriter::finish(writer)
            .await
            .map_err(|source| DeltaFunnelError::MssqlWrite { source })?;

        assert_eq!(first_stats.rows_written, 2);
        assert_eq!(first_stats.batches_written, 1);
        assert_eq!(second_stats.rows_written, 3);
        assert_eq!(second_stats.batches_written, 2);
        assert_eq!(final_stats.rows_written, 3);
        assert_eq!(final_stats.batches_written, 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_loop_writes_batches_in_order_counts_accepted_and_finishes()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema()?;
        let log = Arc::new(Mutex::new(FakeBulkLoadWriterLog::default()));
        let writer = FakeBulkLoadWriter::with_log(Arc::clone(&log))
            .delay_writes_by(Duration::from_millis(2));
        let first = orders_batch(vec![1, 2], vec![Some("open"), Some("closed")])?;
        let second = orders_batch(vec![3], vec![None])?;
        let batches = stream::iter(vec![Ok(first), Ok(second)]);

        let report = write_mssql_batches_with_writer(
            &output_plan,
            batches,
            writer,
            default_mssql_write_options(),
        )
        .await?;

        assert_eq!(report.output_name(), "orders_output");
        assert_eq!(report.stats().rows_written(), 3);
        assert_eq!(report.stats().batches_written(), 2);
        assert!(report.stats().elapsed_ms() > 0);
        assert!(!report.partial_write_possible());
        assert_eq!(report.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        assert_output_schema(report.output_schema());
        assert_eq!(report.output_row_count(), RowCount::exact(3));
        assert_batch_shaping(report.batch_shaping(), PhaseStatus::completed(), 2, 3, 2, 3);
        assert_phase_timing(
            report.phase_timings(),
            POLL_BATCH_STREAM_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_phase_timing(
            report.phase_timings(),
            VALIDATE_BATCH_SCHEMA_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_phase_timing(
            report.phase_timings(),
            WRITE_BATCH_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_phase_timing(
            report.phase_timings(),
            FINALIZE_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_phase_timing(
            report.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::not_started(ReportReasonCode::NotExecuted),
        )?;
        let debug = format!("{report:?}");
        assert!(!debug.contains("open"));
        assert!(!debug.contains("closed"));

        let log = lock_fake_writer_log(&log)?;
        assert_eq!(log.batch_rows, vec![2, 1]);
        assert_eq!(log.finish_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn write_loop_empty_stream_finishes_with_zero_stats() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema()?;
        let log = Arc::new(Mutex::new(FakeBulkLoadWriterLog::default()));
        let writer = FakeBulkLoadWriter::with_log(Arc::clone(&log));
        let batches = stream::empty::<Result<RecordBatch, DeltaFunnelError>>();

        let report = write_mssql_batches_with_writer(
            &output_plan,
            batches,
            writer,
            default_mssql_write_options(),
        )
        .await?;

        assert_eq!(report.output_name(), "orders_output");
        assert_eq!(report.stats().rows_written(), 0);
        assert_eq!(report.stats().batches_written(), 0);
        assert!(!report.partial_write_possible());
        assert_eq!(report.output_row_count(), RowCount::exact(0));
        assert_batch_shaping(report.batch_shaping(), PhaseStatus::completed(), 0, 0, 0, 0);
        let log = lock_fake_writer_log(&log)?;
        assert!(log.batch_rows.is_empty());
        assert_eq!(log.finish_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn write_loop_stream_error_preserves_stats_and_skips_finish()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema()?;
        let log = Arc::new(Mutex::new(FakeBulkLoadWriterLog::default()));
        let writer = FakeBulkLoadWriter::with_log(Arc::clone(&log));
        let first = orders_batch(vec![1, 2], vec![Some("open"), Some("closed")])?;
        let batches = stream::iter(vec![
            Ok(first),
            Err(DeltaFunnelError::Config {
                message: "stream failed after first batch".to_owned(),
            }),
        ]);

        let error = match write_mssql_batches_with_writer(
            &output_plan,
            batches,
            writer,
            default_mssql_write_options(),
        )
        .await
        {
            Ok(_) => {
                return Err(DeltaFunnelError::Config {
                    message: "expected stream error".to_owned(),
                });
            }
            Err(error) => error,
        };

        let context =
            assert_write_phase_error(error, MssqlWritePhase::PollBatchStream, 2, 1, true)?;
        assert_eq!(context.output_row_count(), RowCount::partial(2));
        assert_batch_shaping(context.batch_shaping(), PhaseStatus::failed(), 1, 2, 1, 2);
        assert_phase_timing(
            context.phase_timings(),
            POLL_BATCH_STREAM_PHASE,
            PhaseStatus::failed(),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            FINALIZE_PHASE,
            PhaseStatus::not_started(ReportReasonCode::NotExecuted),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::not_started(ReportReasonCode::FailureBeforeValidation),
        )?;
        let log = lock_fake_writer_log(&log)?;
        assert_eq!(log.batch_rows, vec![2]);
        assert_eq!(log.finish_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn write_loop_schema_mismatch_skips_writer_and_finish() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema()?;
        let log = Arc::new(Mutex::new(FakeBulkLoadWriterLog::default()));
        let writer = FakeBulkLoadWriter::with_log(Arc::clone(&log));
        let batches = stream::iter(vec![Ok(orders_batch_with_int32_order_id()?)]);

        let error = match write_mssql_batches_with_writer(
            &output_plan,
            batches,
            writer,
            default_mssql_write_options(),
        )
        .await
        {
            Ok(_) => {
                return Err(DeltaFunnelError::Config {
                    message: "expected batch schema validation error".to_owned(),
                });
            }
            Err(error) => error,
        };

        let context = assert_batch_schema_validation_error(error, Some((0, "order_id")))?;
        assert_eq!(context.output_row_count(), RowCount::partial(1));
        assert_batch_shaping(context.batch_shaping(), PhaseStatus::failed(), 1, 1, 0, 0);
        assert_phase_timing(
            context.phase_timings(),
            VALIDATE_BATCH_SCHEMA_PHASE,
            PhaseStatus::failed(),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            WRITE_BATCH_PHASE,
            PhaseStatus::not_started(ReportReasonCode::NotExecuted),
        )?;
        let log = lock_fake_writer_log(&log)?;
        assert!(log.batch_rows.is_empty());
        assert_eq!(log.finish_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn write_loop_write_failure_preserves_stats_and_skips_finish()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema()?;
        let log = Arc::new(Mutex::new(FakeBulkLoadWriterLog::default()));
        let writer = FakeBulkLoadWriter::with_log(Arc::clone(&log))
            .fail_on_write_batch(2)
            .delay_writes_by(Duration::from_millis(2));
        let first = orders_batch(vec![1, 2], vec![Some("open"), Some("closed")])?;
        let second = orders_batch(vec![3], vec![None])?;
        let batches = stream::iter(vec![Ok(first), Ok(second)]);

        let error = match write_mssql_batches_with_writer(
            &output_plan,
            batches,
            writer,
            default_mssql_write_options(),
        )
        .await
        {
            Ok(_) => {
                return Err(DeltaFunnelError::Config {
                    message: "expected write batch error".to_owned(),
                });
            }
            Err(error) => error,
        };

        let DeltaFunnelError::MssqlWritePhase { context, .. } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected MSSQL write phase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::WriteBatch);
        assert_eq!(context.output_name(), "orders_output");
        assert_eq!(context.stats().rows_written(), 2);
        assert_eq!(context.stats().batches_written(), 1);
        assert_eq!(context.output_row_count(), RowCount::partial(3));
        assert_batch_shaping(context.batch_shaping(), PhaseStatus::failed(), 2, 3, 1, 2);
        assert!(context.stats().elapsed_ms() > 0);
        assert!(context.partial_write_possible());
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        assert_phase_timing(
            context.phase_timings(),
            WRITE_BATCH_PHASE,
            PhaseStatus::failed(),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            FINALIZE_PHASE,
            PhaseStatus::not_started(ReportReasonCode::NotExecuted),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::not_started(ReportReasonCode::FailureBeforeValidation),
        )?;
        let log = lock_fake_writer_log(&log)?;
        assert_eq!(log.batch_rows, vec![2, 1]);
        assert_eq!(log.finish_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn write_loop_create_and_load_write_failure_does_not_report_partial_write()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema_with_load_mode(LoadMode::CreateAndLoad)?;
        let log = Arc::new(Mutex::new(FakeBulkLoadWriterLog::default()));
        let writer = FakeBulkLoadWriter::with_log(Arc::clone(&log)).fail_on_write_batch(2);
        let first = orders_batch(vec![1, 2], vec![Some("open"), Some("closed")])?;
        let second = orders_batch(vec![3], vec![None])?;
        let batches = stream::iter(vec![Ok(first), Ok(second)]);

        let error = match write_mssql_batches_with_writer(
            &output_plan,
            batches,
            writer,
            default_mssql_write_options(),
        )
        .await
        {
            Ok(_) => {
                return Err(DeltaFunnelError::Config {
                    message: "expected create-and-load write batch error".to_owned(),
                });
            }
            Err(error) => error,
        };

        let DeltaFunnelError::MssqlWritePhase { context, .. } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected MSSQL write phase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::WriteBatch);
        assert_eq!(context.load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(context.stats().rows_written(), 2);
        assert_eq!(context.stats().batches_written(), 1);
        assert!(!context.partial_write_possible());
        let log = lock_fake_writer_log(&log)?;
        assert_eq!(log.batch_rows, vec![2, 1]);
        assert_eq!(log.finish_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn write_loop_finalize_failure_preserves_stats() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema()?;
        let log = Arc::new(Mutex::new(FakeBulkLoadWriterLog::default()));
        let writer = FakeBulkLoadWriter::with_log(Arc::clone(&log)).fail_on_finish();
        let first = orders_batch(vec![1, 2], vec![Some("open"), Some("closed")])?;
        let second = orders_batch(vec![3], vec![None])?;
        let batches = stream::iter(vec![Ok(first), Ok(second)]);

        let error = match write_mssql_batches_with_writer(
            &output_plan,
            batches,
            writer,
            default_mssql_write_options(),
        )
        .await
        {
            Ok(_) => {
                return Err(DeltaFunnelError::Config {
                    message: "expected finalize error".to_owned(),
                });
            }
            Err(error) => error,
        };

        let context = assert_write_phase_error(error, MssqlWritePhase::Finalize, 3, 2, true)?;
        assert_eq!(context.output_row_count(), RowCount::exact(3));
        assert_batch_shaping(
            context.batch_shaping(),
            PhaseStatus::completed(),
            2,
            3,
            2,
            3,
        );
        assert_phase_timing(
            context.phase_timings(),
            FINALIZE_PHASE,
            PhaseStatus::failed(),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::not_started(ReportReasonCode::FailureBeforeValidation),
        )?;
        let log = lock_fake_writer_log(&log)?;
        assert_eq!(log.batch_rows, vec![2, 1]);
        assert_eq!(log.finish_count, 1);
        Ok(())
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
        assert_output_schema(report.output_schema());
        assert_eq!(report.output_row_count(), RowCount::exact(42));
        assert_eq!(report.target_row_count(), RowCount::unavailable());
        assert_eq!(
            report.validation_status(),
            ValidationStatus::skipped(ReportReasonCode::NotExecuted)
        );
        assert_batch_shaping(
            report.batch_shaping(),
            PhaseStatus::completed(),
            3,
            42,
            3,
            42,
        );
        assert!(report.partial_write_possible());
        assert_eq!(report.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        Ok(())
    }

    #[test]
    fn write_report_records_target_validation_outcome() -> Result<(), DeltaFunnelError> {
        let connection = secret_connection()?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?);
        let output_plan = plan_mssql_target_for_output(
            orders_schema(),
            "orders_output",
            &target_config,
            Some(&connection),
            PlanOptions::default(),
        )?;
        let report = MssqlWriteReport::from_output_plan_with_metrics(
            &output_plan,
            MssqlWriteReportMetrics::new(
                RowCount::exact(42),
                MssqlBatchShapingReport::completed(3, 42, 3, 42),
                42,
                3,
                125,
                false,
                MssqlTargetCleanupStatus::NotApplicable,
            )
            .with_phase_timings(vec![PhaseTimingReport::not_started(
                VALIDATION_PHASE,
                ReportReasonCode::NotExecuted,
            )]),
        );

        let report = report.with_target_validation(
            RowCount::exact(42),
            ValidationStatus::passed(),
            PhaseTimingReport::completed(VALIDATION_PHASE, Duration::from_micros(7)),
        );

        assert_eq!(report.target_row_count(), RowCount::exact(42));
        assert_eq!(report.validation_status(), ValidationStatus::passed());
        assert_eq!(
            report
                .phase_timings()
                .iter()
                .filter(|timing| timing.phase_name() == VALIDATION_PHASE)
                .count(),
            1
        );
        assert_phase_timing(
            report.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::completed(),
        )?;
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
    fn write_failure_context_preserves_phase_report_and_accepted_stats()
    -> Result<(), DeltaFunnelError> {
        let connection = secret_connection()?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?);
        let output_plan = plan_mssql_target_for_output(
            orders_schema(),
            "orders_output",
            &target_config,
            Some(&connection),
            PlanOptions::default(),
        )?;

        let context = MssqlWriteFailureContext::from_output_plan(
            &output_plan,
            MssqlWritePhase::WriteBatch,
            42,
            3,
            125,
            true,
            MssqlTargetCleanupStatus::NotApplicable,
        );

        assert_eq!(context.phase(), MssqlWritePhase::WriteBatch);
        assert_eq!(context.output_name(), "orders_output");
        assert_eq!(context.target_table().table(), "orders");
        assert_eq!(context.load_mode(), LoadMode::AppendExisting);
        assert_eq!(
            context.connection_source(),
            MssqlConnectionSource::ContextDefault
        );
        assert_eq!(
            context.connection().display_label(),
            Some("warehouse-primary")
        );
        assert_eq!(context.stats().rows_written(), 42);
        assert_eq!(context.stats().batches_written(), 3);
        assert_eq!(context.stats().elapsed_ms(), 125);
        assert_eq!(context.output_row_count(), RowCount::partial(42));
        assert_eq!(context.target_row_count(), RowCount::unavailable());
        assert_eq!(
            context.validation_status(),
            ValidationStatus::skipped(ReportReasonCode::NotExecuted)
        );
        assert_batch_shaping(context.batch_shaping(), PhaseStatus::failed(), 3, 42, 3, 42);
        assert!(context.partial_write_possible());
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        assert_eq!(context.report().output_name(), "orders_output");
        Ok(())
    }

    fn assert_phase_timing(
        timings: &[PhaseTimingReport],
        phase_name: &str,
        expected_status: PhaseStatus,
    ) -> Result<(), DeltaFunnelError> {
        let timing = timings
            .iter()
            .find(|timing| timing.phase_name() == phase_name)
            .ok_or_else(|| DeltaFunnelError::Config {
                message: format!("missing phase timing {phase_name}"),
            })?;

        assert_eq!(timing.status(), expected_status);
        if expected_status.is_completed() || expected_status.is_failed() {
            assert!(timing.elapsed_micros().is_some());
        } else {
            assert_eq!(timing.elapsed_micros(), None);
        }
        Ok(())
    }

    #[test]
    fn write_failure_context_debug_redacts_connection_secret() -> Result<(), DeltaFunnelError> {
        let connection = secret_connection()?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?);
        let output_plan = plan_mssql_target_for_output(
            orders_schema(),
            "orders_output",
            &target_config,
            Some(&connection),
            PlanOptions::default(),
        )?;

        let context = MssqlWriteFailureContext::from_output_plan(
            &output_plan,
            MssqlWritePhase::InitializeWriter,
            0,
            0,
            0,
            false,
            MssqlTargetCleanupStatus::NotAttempted,
        );
        let debug = format!("{context:?}");

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

    #[test]
    fn write_options_for_output_plan_preserve_schema_plan_options() -> Result<(), DeltaFunnelError>
    {
        let connection = secret_connection()?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?);
        let plan_options = PlanOptions {
            string_policy: StringPolicy::NVarChar(128),
            ..PlanOptions::default()
        };
        let output_plan = plan_mssql_target_for_output(
            orders_schema(),
            "orders_output",
            &target_config,
            Some(&connection),
            plan_options,
        )?;

        let write_options = mssql_write_options_for_output_plan(&output_plan);

        assert_eq!(write_options.backend, WriteBackend::DirectRawBulk);
        assert_eq!(write_options.schema_check, SchemaCheck::Strict);
        assert_eq!(write_options.plan_options, plan_options);
        Ok(())
    }

    #[test]
    fn output_record_batch_validation_accepts_matching_planned_schema()
    -> Result<(), DeltaFunnelError> {
        let schema = Arc::new(orders_schema());
        let output_plan = output_plan_for_orders_schema()?;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])),
                Arc::new(StringArray::from(vec![Some("open"), None])),
            ],
        )
        .map_err(|error| DeltaFunnelError::Config {
            message: error.to_string(),
        })?;

        let report = validate_mssql_output_record_batch(&output_plan, &batch)?;

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
        assert!(!format!("{report:?}").contains("secret-token"));
        Ok(())
    }

    #[test]
    fn output_schema_validation_accepts_aliased_output_field_names() -> Result<(), DeltaFunnelError>
    {
        let connection = secret_connection()?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?);
        let schema = Schema::new(vec![
            Field::new("gross_total", DataType::Float64, true),
            Field::new("order_id", DataType::Int32, false),
        ]);
        let output_plan = plan_mssql_target_for_output(
            schema.clone(),
            "orders_output",
            &target_config,
            Some(&connection),
            PlanOptions::default(),
        )?;

        let report = validate_mssql_output_schema(&output_plan, &schema)?;

        assert_eq!(
            output_plan.schema_mappings()[0].arrow().name(),
            "gross_total"
        );
        assert_eq!(output_plan.schema_mappings()[1].arrow().name(), "order_id");
        assert_eq!(report.output_name(), "orders_output");
        assert_eq!(report.target_table().schema(), Some("dbo"));
        assert_eq!(report.target_table().table(), "orders");
        Ok(())
    }

    #[test]
    fn output_schema_validation_rejects_reordered_fields() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema()?;
        let schema = Schema::new(vec![
            Field::new("status", DataType::Utf8, true),
            Field::new("order_id", DataType::Int64, false),
        ]);

        let error = validate_mssql_output_schema(&output_plan, &schema).unwrap_err();

        assert_batch_schema_validation_error(error, Some((0, "order_id")))?;
        Ok(())
    }

    #[test]
    fn output_schema_validation_rejects_type_mismatch() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema()?;
        let schema = Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("status", DataType::Utf8, true),
        ]);

        let error = validate_mssql_output_schema(&output_plan, &schema).unwrap_err();

        assert_batch_schema_validation_error(error, Some((0, "order_id")))?;
        Ok(())
    }

    #[test]
    fn output_schema_validation_rejects_missing_field() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema()?;
        let schema = Schema::new(vec![Field::new("order_id", DataType::Int64, false)]);

        let error = validate_mssql_output_schema(&output_plan, &schema).unwrap_err();

        assert_batch_schema_validation_error(error, Some((1, "status")))?;
        Ok(())
    }

    #[test]
    fn output_schema_validation_rejects_extra_field() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema()?;
        let schema = Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ]);

        let error = validate_mssql_output_schema(&output_plan, &schema).unwrap_err();

        assert_batch_schema_validation_error(error, None)?;
        Ok(())
    }

    #[test]
    fn output_schema_validation_rejects_nullability_mismatch() -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_for_orders_schema()?;
        let schema = Schema::new(vec![
            Field::new("order_id", DataType::Int64, true),
            Field::new("status", DataType::Utf8, true),
        ]);

        let error = validate_mssql_output_schema(&output_plan, &schema).unwrap_err();

        assert_batch_schema_validation_error(error, Some((0, "order_id")))?;
        Ok(())
    }
}
