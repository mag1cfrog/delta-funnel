//! Sequential multi-output SQL Server write orchestration.
//!
//! This module keeps the multi-output workflow layer separate from the
//! one-output sink. The MVP runs outputs sequentially, stops on the first
//! failure, and marks later outputs as skipped without invoking their lazy batch
//! stream factories.

use std::{fmt, pin::Pin};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use futures_util::Stream;

use crate::{DeltaFunnelError, redaction::sanitize_text_for_display};

use super::{
    LoadMode, MssqlConnectionSource, MssqlConnectionSummary, MssqlSchemaPlanOptions,
    MssqlTargetSummary, MssqlTargetTable, MssqlWriteFailureContext, MssqlWriteOptions,
    MssqlWriteReport, ResolvedMssqlTarget, default_mssql_write_options,
    write_output_batches_to_mssql,
};

/// Lazy stream produced only when a SQL Server output is attempted.
pub type MssqlOutputBatchStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send>>;

/// Factory that constructs a shaped batch stream for one attempted output.
pub type MssqlOutputBatchStreamFactory = Box<dyn FnOnce() -> MssqlOutputBatchStream + Send>;

/// One deferred SQL Server output write job.
///
/// The job owns an already resolved SQL Server target plus a lazy batch stream
/// factory. The workflow calls the factory only after the output becomes the
/// next attempted output. Skipped jobs keep their stream factories uncalled, so
/// skipped outputs do not start source reads, DataFusion execution, stream
/// setup, SQL connections, lifecycle preparation, writer initialization, or
/// batch polling through this API.
pub struct MssqlOutputWriteJob {
    output_schema: SchemaRef,
    resolved_target: ResolvedMssqlTarget,
    schema_options: MssqlSchemaPlanOptions,
    batches: MssqlOutputBatchStreamFactory,
    write_options: MssqlWriteOptions,
}

impl MssqlOutputWriteJob {
    /// Creates a deferred SQL Server output write job.
    pub fn new<F, S>(
        output_schema: SchemaRef,
        resolved_target: ResolvedMssqlTarget,
        schema_options: MssqlSchemaPlanOptions,
        batches: F,
        write_options: MssqlWriteOptions,
    ) -> Self
    where
        F: FnOnce() -> S + Send + 'static,
        S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send + 'static,
    {
        Self {
            output_schema,
            resolved_target,
            schema_options,
            batches: Box::new(move || Box::pin(batches())),
            write_options,
        }
    }

    /// Creates a deferred SQL Server output write job using default write options.
    pub fn with_default_write_options<F, S>(
        output_schema: SchemaRef,
        resolved_target: ResolvedMssqlTarget,
        schema_options: MssqlSchemaPlanOptions,
        batches: F,
    ) -> Self
    where
        F: FnOnce() -> S + Send + 'static,
        S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send + 'static,
    {
        Self::new(
            output_schema,
            resolved_target,
            schema_options,
            batches,
            default_mssql_write_options(),
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

    fn into_parts(
        self,
    ) -> (
        SchemaRef,
        ResolvedMssqlTarget,
        MssqlSchemaPlanOptions,
        MssqlOutputBatchStreamFactory,
        MssqlWriteOptions,
    ) {
        (
            self.output_schema,
            self.resolved_target,
            self.schema_options,
            self.batches,
            self.write_options,
        )
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
}

impl fmt::Display for MssqlWorkflowWriteReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let succeeded = self
            .outputs
            .iter()
            .filter(|status| status.is_succeeded())
            .count();
        let failed = self
            .outputs
            .iter()
            .filter(|status| status.is_failed())
            .count();
        let skipped = self
            .outputs
            .iter()
            .filter(|status| status.is_skipped())
            .count();

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
    context: Option<MssqlWriteFailureContext>,
}

impl MssqlWriteFailureReport {
    fn from_error(target: MssqlTargetSummary, error: DeltaFunnelError) -> Self {
        let context = failure_context(&error).cloned();
        Self {
            target,
            error: sanitize_text_for_display(&error.to_string()),
            context,
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
    pub const fn context(&self) -> Option<&MssqlWriteFailureContext> {
        self.context.as_ref()
    }
}

/// Structured report for a skipped SQL Server output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlWriteSkippedReport {
    target: MssqlTargetSummary,
    reason: MssqlWriteSkippedReason,
}

impl MssqlWriteSkippedReport {
    fn previous_output_failed(target: MssqlTargetSummary, failed_output_name: String) -> Self {
        Self {
            target,
            reason: MssqlWriteSkippedReason::PreviousOutputFailed { failed_output_name },
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
    write_mssql_outputs_with_writer(jobs, options, MssqlPublicOneOutputWriter).await
}

#[async_trait]
pub(crate) trait MssqlWorkflowOutputWriter: Send {
    async fn write_output(
        &mut self,
        job: MssqlOutputWriteJob,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>;
}

struct MssqlPublicOneOutputWriter;

#[async_trait]
impl MssqlWorkflowOutputWriter for MssqlPublicOneOutputWriter {
    async fn write_output(
        &mut self,
        job: MssqlOutputWriteJob,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        let (output_schema, resolved_target, schema_options, batches, write_options) =
            job.into_parts();

        write_output_batches_to_mssql(
            output_schema.as_ref(),
            resolved_target,
            schema_options,
            batches(),
            write_options,
        )
        .await
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
        let target = job.target_summary();

        if let Some(failed_output_name) = failed_output_name.as_ref() {
            statuses.push(MssqlOutputWriteStatus::Skipped(
                MssqlWriteSkippedReport::previous_output_failed(target, failed_output_name.clone()),
            ));
            continue;
        }

        match writer.write_output(job).await {
            Ok(report) => statuses.push(MssqlOutputWriteStatus::Succeeded(report)),
            Err(error) => {
                let failure = MssqlWriteFailureReport::from_error(target, error);
                failed_output_name = Some(failure.output_name().to_owned());
                statuses.push(MssqlOutputWriteStatus::Failed(failure));
            }
        }
    }

    Ok(MssqlWorkflowWriteReport::new(statuses))
}

fn ensure_sequential_options(options: MssqlWorkflowWriteOptions) -> Result<(), DeltaFunnelError> {
    match options.max_parallel_outputs() {
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

fn failure_context(error: &DeltaFunnelError) -> Option<&MssqlWriteFailureContext> {
    match error {
        DeltaFunnelError::MssqlWritePhase { context, .. }
        | DeltaFunnelError::MssqlBatchSchemaValidation { context, .. } => Some(context.as_ref()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex, MutexGuard};

    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use futures_util::stream;

    use super::*;
    use crate::{
        LoadMode, MssqlConnectionConfig, MssqlTargetCleanupStatus, MssqlTargetConfig,
        MssqlTargetOutputPlan, MssqlTargetResolutionContext, MssqlTargetTable, MssqlWritePhase,
        plan_mssql_target_for_output,
    };

    #[derive(Default)]
    struct FakeWorkflowWriter {
        outcomes: VecDeque<Result<MssqlWriteReport, DeltaFunnelError>>,
        attempted_outputs: Arc<Mutex<Vec<String>>>,
        invoke_stream_factories: bool,
    }

    impl FakeWorkflowWriter {
        fn new(outcomes: Vec<Result<MssqlWriteReport, DeltaFunnelError>>) -> Self {
            Self {
                outcomes: outcomes.into(),
                attempted_outputs: Arc::new(Mutex::new(Vec::new())),
                invoke_stream_factories: true,
            }
        }

        fn without_stream_factory_invocation(
            outcomes: Vec<Result<MssqlWriteReport, DeltaFunnelError>>,
        ) -> Self {
            Self {
                outcomes: outcomes.into(),
                attempted_outputs: Arc::new(Mutex::new(Vec::new())),
                invoke_stream_factories: false,
            }
        }

        fn attempted_outputs(&self) -> Arc<Mutex<Vec<String>>> {
            Arc::clone(&self.attempted_outputs)
        }
    }

    #[async_trait]
    impl MssqlWorkflowOutputWriter for FakeWorkflowWriter {
        async fn write_output(
            &mut self,
            job: MssqlOutputWriteJob,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            self.attempted_outputs
                .lock()
                .map_err(|_| test_error("attempted output lock poisoned"))?
                .push(job.output_name().to_owned());
            let (_schema, _target, _schema_options, batches, _write_options) = job.into_parts();
            if self.invoke_stream_factories {
                let _stream = batches();
            }

            self.outcomes
                .pop_front()
                .ok_or_else(|| test_error("missing fake writer outcome"))?
        }
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
        let failure = phase_error(
            &second,
            MssqlWritePhase::WriteBatch,
            1,
            1,
            true,
            MssqlTargetCleanupStatus::NotApplicable,
            "write failed",
        );
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
        assert!(matches!(failed, MssqlOutputWriteStatus::Failed(_)));
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

        assert!(failed.is_failed());
        assert_eq!(failed.output_name(), "second");
        assert_eq!(failed.target_table().table(), "second_orders");
        assert_eq!(failed.load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(failed.connection().display_label(), Some("test connection"));

        assert!(skipped.is_skipped());
        assert_eq!(skipped.output_name(), "third");
        assert_eq!(skipped.target_table().table(), "third_orders");
        assert_eq!(skipped.load_mode(), LoadMode::AppendExisting);
        assert_eq!(
            skipped.connection().display_label(),
            Some("test connection")
        );

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
        let writer = FakeWorkflowWriter::without_stream_factory_invocation(vec![Err(failure)]);
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
        Ok(MssqlOutputWriteJob::with_default_write_options(
            output_schema(),
            resolved_target(output_plan)?,
            MssqlSchemaPlanOptions::default(),
            move || {
                if let Ok(mut calls) = factory_calls.lock() {
                    calls.push(output_name);
                }
                stream::empty()
            },
        ))
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
        DeltaFunnelError::MssqlWritePhase {
            context: Box::new(MssqlWriteFailureContext::from_output_plan(
                output_plan,
                phase,
                rows_written,
                batches_written,
                0,
                partial_write_possible,
                cleanup,
            )),
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
        Ok(())
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
