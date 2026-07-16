use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use datafusion::{
    arrow::{
        datatypes::{DataType, SchemaRef},
        error::ArrowError,
        record_batch::RecordBatch,
        util::{
            display::{ArrayFormatter, FormatOptions},
            pretty::pretty_format_batches,
        },
    },
    physical_plan::ExecutionPlan,
    prelude::{DataFrame, SessionContext},
};
use futures_util::{Stream, StreamExt, TryStreamExt};

#[cfg(test)]
use crate::MssqlOutputBatchStreamFactory;
use crate::{
    DeltaFunnelError, ExecutionProfileMode, MssqlOutputBatchStream, OperationTimeline,
    PhaseTimingReport, PreviewFailureContext, QueryExecutionProfile, QueryExecutionScope,
    ReportReasonCode, TimelineSpanStatus,
    observability::{DeltaProviderScanOutcome, delta_provider_parquet_io_summary},
    progress::{ProgressEvent, ProgressOperation, ProgressPhase, ProgressReporter},
    query_engine::datafusion::{
        DFQueryExecution, DeltaProviderReadStatsHandle, collect_delta_provider_read_stats_handles,
        datafusion_query_output_stream_with_effective_root,
        execution_profile::{
            QueryExecutionProfileConsumer, QueryExecutionProfileResult,
            delta_provider_read_stats_snapshot_set,
        },
        snapshot_delta_provider_read_stats,
    },
    report::{OperationTimelineRecorder, OperationTimelineSpanRecorder},
    usize_to_u64_saturating,
};

use super::{
    DeltaFunnelSession, LazyTable, LazyTableKind, PendingDerivedTable, PreviewOptions,
    RegisteredDerivedTable, RegisteredSessionSource, TablePreview,
    errors::{datafusion_handoff_setup_error, unknown_lazy_table_error},
    handles::{
        PREVIEW_DATAFRAME_PLANNING_PHASE, PREVIEW_EXECUTE_COLLECT_PHASE, PREVIEW_FORMAT_HTML_PHASE,
        PREVIEW_FORMAT_TEXT_PHASE, PREVIEW_PHASE_NAMES, PREVIEW_PHYSICAL_PLANNING_PHASE,
        PREVIEW_STREAM_SETUP_PHASE, PREVIEW_TOTAL_PHASE,
    },
};

pub(super) type SharedProviderStatsSnapshots =
    Arc<Mutex<Vec<crate::DeltaProviderReadStatsSnapshot>>>;

pub(super) fn shared_provider_stats_snapshots() -> SharedProviderStatsSnapshots {
    Arc::new(Mutex::new(Vec::new()))
}

pub(super) fn provider_stats_snapshots(
    provider_stats_snapshots: &SharedProviderStatsSnapshots,
) -> Vec<crate::DeltaProviderReadStatsSnapshot> {
    match provider_stats_snapshots.lock() {
        Ok(provider_stats_snapshots) => provider_stats_snapshots.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

/// Finalizes retained Delta scans and an optional profile from one snapshot set.
pub(super) fn finalize_tracked_query_execution(
    read_stats_handles: &[DeltaProviderReadStatsHandle],
    provider_stats_snapshots: Option<&SharedProviderStatsSnapshots>,
    profile_consumer: Option<QueryExecutionProfileConsumer>,
    outcome: DeltaProviderScanOutcome,
) {
    let snapshots = snapshot_delta_provider_read_stats(read_stats_handles);
    let terminal_snapshots = delta_provider_read_stats_snapshot_set(read_stats_handles, &snapshots);
    if terminal_snapshots.is_empty() && profile_consumer.is_none() {
        return;
    }

    if let Some(provider_stats_snapshots) = provider_stats_snapshots {
        let mut retained = match provider_stats_snapshots.lock() {
            Ok(retained) => retained,
            Err(poisoned) => poisoned.into_inner(),
        };
        retained.extend(
            terminal_snapshots
                .iter()
                .map(|(_, snapshot)| snapshot.clone()),
        );
    }
    for (_, snapshot) in &terminal_snapshots {
        delta_provider_parquet_io_summary(snapshot, outcome);
    }
    if let Some(profile_consumer) = profile_consumer {
        profile_consumer.consume_terminal(outcome.query_execution_outcome(), &terminal_snapshots);
    }
}

/// Finalizes one merged query execution when its batch stream stops.
///
/// The stream snapshots shared read counters when it ends, fails, or is dropped
/// by its downstream consumer. An optional profile consumer retains the
/// effective physical-plan root until that same terminal transition.
struct QueryExecutionTerminalStream {
    inner: MssqlOutputBatchStream,
    // Taking these handles records the single terminal transition, including
    // executions whose handle set is empty.
    read_stats_handles: Option<Vec<DeltaProviderReadStatsHandle>>,
    provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
    profile_consumer: Option<QueryExecutionProfileConsumer>,
}

impl QueryExecutionTerminalStream {
    fn new(
        inner: MssqlOutputBatchStream,
        read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
        provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
        profile_consumer: Option<QueryExecutionProfileConsumer>,
    ) -> Self {
        Self {
            inner,
            read_stats_handles: Some(read_stats_handles),
            provider_stats_snapshots,
            profile_consumer,
        }
    }

    fn finalize_if_needed(&mut self, outcome: DeltaProviderScanOutcome) {
        let Some(read_stats_handles) = self.read_stats_handles.take() else {
            return;
        };
        let provider_stats_snapshots = self.provider_stats_snapshots.take();
        let profile_consumer = self.profile_consumer.take();
        finalize_tracked_query_execution(
            &read_stats_handles,
            provider_stats_snapshots.as_ref(),
            profile_consumer,
            outcome,
        );
    }
}

impl Stream for QueryExecutionTerminalStream {
    type Item = Result<RecordBatch, DeltaFunnelError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(context) {
            Poll::Ready(None) => {
                self.finalize_if_needed(DeltaProviderScanOutcome::Success);
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                self.finalize_if_needed(DeltaProviderScanOutcome::Error);
                Poll::Ready(Some(Err(error)))
            }
            other => other,
        }
    }
}

impl Drop for QueryExecutionTerminalStream {
    fn drop(&mut self) {
        self.finalize_if_needed(DeltaProviderScanOutcome::Cancelled);
    }
}

/// Coordinates one terminal outcome across all partitions of one execution.
struct PartitionExecutionCoordinator {
    state: Mutex<PartitionExecutionState>,
}

struct PartitionExecutionState {
    // Finalization waits until every returned partition stream is terminal.
    remaining_streams: usize,
    // Each terminal result can only strengthen success -> cancelled -> error.
    outcome: DeltaProviderScanOutcome,
    // The last terminal stream takes these handles and releases them after use.
    read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
    // The same terminal stream consumes the optional execution profile.
    profile_consumer: Option<QueryExecutionProfileConsumer>,
}

impl PartitionExecutionCoordinator {
    fn new(
        read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
        profile_consumer: Option<QueryExecutionProfileConsumer>,
        stream_count: usize,
    ) -> Self {
        Self {
            state: Mutex::new(PartitionExecutionState {
                remaining_streams: stream_count,
                outcome: DeltaProviderScanOutcome::Success,
                read_stats_handles,
                profile_consumer,
            }),
        }
    }

    fn record_stream_terminal(&self, outcome: DeltaProviderScanOutcome) {
        let finalization = match self.state.lock() {
            Ok(mut state) => state.record_stream_terminal(outcome),
            Err(poisoned) => poisoned.into_inner().record_stream_terminal(outcome),
        };
        if let Some((read_stats_handles, profile_consumer, outcome)) = finalization {
            finalize_tracked_query_execution(&read_stats_handles, None, profile_consumer, outcome);
        }
    }
}

impl PartitionExecutionState {
    fn record_stream_terminal(
        &mut self,
        outcome: DeltaProviderScanOutcome,
    ) -> Option<(
        Vec<DeltaProviderReadStatsHandle>,
        Option<QueryExecutionProfileConsumer>,
        DeltaProviderScanOutcome,
    )> {
        if self.remaining_streams == 0 {
            return None;
        }

        self.outcome = strongest_provider_scan_outcome(self.outcome, outcome);
        self.remaining_streams -= 1;
        if self.remaining_streams != 0 {
            return None;
        }

        Some((
            std::mem::take(&mut self.read_stats_handles),
            self.profile_consumer.take(),
            self.outcome,
        ))
    }
}

const fn strongest_provider_scan_outcome(
    current: DeltaProviderScanOutcome,
    next: DeltaProviderScanOutcome,
) -> DeltaProviderScanOutcome {
    match (current, next) {
        (DeltaProviderScanOutcome::Error, _) | (_, DeltaProviderScanOutcome::Error) => {
            DeltaProviderScanOutcome::Error
        }
        (DeltaProviderScanOutcome::Cancelled, _) | (_, DeltaProviderScanOutcome::Cancelled) => {
            DeltaProviderScanOutcome::Cancelled
        }
        _ => DeltaProviderScanOutcome::Success,
    }
}

/// Reports one partition's terminal state to its shared execution coordinator.
struct PartitionExecutionStream {
    inner: MssqlOutputBatchStream,
    coordinator: Option<Arc<PartitionExecutionCoordinator>>,
}

impl PartitionExecutionStream {
    fn record_terminal_once(&mut self, outcome: DeltaProviderScanOutcome) {
        if let Some(coordinator) = self.coordinator.take() {
            coordinator.record_stream_terminal(outcome);
        }
    }
}

impl Stream for PartitionExecutionStream {
    type Item = Result<RecordBatch, DeltaFunnelError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(context) {
            Poll::Ready(None) => {
                self.record_terminal_once(DeltaProviderScanOutcome::Success);
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                self.record_terminal_once(DeltaProviderScanOutcome::Error);
                Poll::Ready(Some(Err(error)))
            }
            other => other,
        }
    }
}

impl Drop for PartitionExecutionStream {
    fn drop(&mut self) {
        self.record_terminal_once(DeltaProviderScanOutcome::Cancelled);
    }
}

/// Adds one shared terminal outcome tracker to a partitioned execution.
pub(super) fn track_partitioned_query_execution_completion(
    streams: Vec<MssqlOutputBatchStream>,
    read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
    profile_consumer: Option<QueryExecutionProfileConsumer>,
) -> Vec<MssqlOutputBatchStream> {
    if read_stats_handles.is_empty() && profile_consumer.is_none() {
        return streams;
    }
    if streams.is_empty() {
        finalize_tracked_query_execution(
            &read_stats_handles,
            None,
            profile_consumer,
            DeltaProviderScanOutcome::Success,
        );
        return streams;
    }

    let coordinator = Arc::new(PartitionExecutionCoordinator::new(
        read_stats_handles,
        profile_consumer,
        streams.len(),
    ));
    streams
        .into_iter()
        .map(|inner| {
            Box::pin(PartitionExecutionStream {
                inner,
                coordinator: Some(Arc::clone(&coordinator)),
            }) as MssqlOutputBatchStream
        })
        .collect()
}

/// Reports Delta file progress while forwarding one batch stream.
///
/// The stream owns the progress coordinator. It samples the
/// existing provider counters without retaining the plan, running another
/// query, or reading Delta metadata again.
struct DeltaFileProgressStream {
    inner: MssqlOutputBatchStream,
    sampler: DeltaFileProgressSampler,
}

/// Samples one plan's counters and emits file progress when they change.
pub(super) struct DeltaFileProgressSampler {
    // One live counter handle for each Delta scan in the output plan.
    read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
    reporter: ProgressReporter,
    phase: ProgressPhase,
    output_name: Option<String>,
    // The last emitted handled and total counts, used to suppress duplicates.
    last_file_progress: Option<(u64, u64)>,
}

impl DeltaFileProgressSampler {
    /// Creates a file progress sampler for one physical plan.
    pub(super) fn new(
        read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
        reporter: ProgressReporter,
        phase: ProgressPhase,
        output_name: Option<String>,
    ) -> Self {
        Self {
            read_stats_handles,
            reporter,
            phase,
            output_name,
            last_file_progress: None,
        }
    }

    /// Emits the current file counts when they have changed since the last sample.
    pub(super) fn emit_if_changed(&mut self) {
        let snapshots = snapshot_delta_provider_read_stats(&self.read_stats_handles);
        let Some(event) = ProgressEvent::file_progress_from_provider_stats(
            self.phase,
            self.output_name.as_deref(),
            &snapshots,
        ) else {
            return;
        };
        let Some(file_progress) = event.files_handled().zip(event.files_total()) else {
            return;
        };
        if self.last_file_progress == Some(file_progress) {
            return;
        }
        self.last_file_progress = Some(file_progress);
        self.reporter.emit(&event);
    }
}

/// Wraps a stream so polling it samples the plan's file progress counters.
fn track_delta_file_progress(
    stream: MssqlOutputBatchStream,
    sampler: DeltaFileProgressSampler,
) -> MssqlOutputBatchStream {
    Box::pin(DeltaFileProgressStream {
        inner: stream,
        sampler,
    })
}

/// Attaches one shared terminal transition to a merged execution.
fn track_query_execution_completion(
    stream: MssqlOutputBatchStream,
    read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
    provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
    profile_consumer: Option<QueryExecutionProfileConsumer>,
) -> MssqlOutputBatchStream {
    if read_stats_handles.is_empty() && profile_consumer.is_none() {
        return stream;
    }
    Box::pin(QueryExecutionTerminalStream::new(
        stream,
        read_stats_handles,
        provider_stats_snapshots,
        profile_consumer,
    ))
}

/// Adds optional live file progress and terminal tracking to one plan stream.
///
/// The provider and progress layers reuse the same live Delta scan counters and
/// do not execute another query. When present, the profile consumer retains the
/// effective physical-plan root until the shared terminal transition.
/// `progress` contains the reporter and output name used for live events.
/// Callers collect the handles before merged stream construction so setup
/// failures can snapshot the same handles.
pub(super) fn wrap_stream_with_query_execution_tracking(
    mut stream: MssqlOutputBatchStream,
    read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
    provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
    progress: Option<(ProgressReporter, String)>,
    profile_consumer: Option<QueryExecutionProfileConsumer>,
) -> MssqlOutputBatchStream {
    if let Some((reporter, output_name)) = progress.filter(|_| !read_stats_handles.is_empty()) {
        let sampler = DeltaFileProgressSampler::new(
            read_stats_handles.clone(),
            reporter,
            ProgressPhase::Writing,
            Some(output_name),
        );
        stream = track_delta_file_progress(stream, sampler);
    }
    track_query_execution_completion(
        stream,
        read_stats_handles,
        provider_stats_snapshots,
        profile_consumer,
    )
}

impl Stream for DeltaFileProgressStream {
    type Item = Result<RecordBatch, DeltaFunnelError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(context) {
            Poll::Ready(None) => {
                // Capture final progress before the provider counters are dropped.
                self.sampler.emit_if_changed();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                // Keep the last partial progress when query execution fails.
                self.sampler.emit_if_changed();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(batch))) => {
                // A ready batch is the existing foreground sampling boundary.
                self.sampler.emit_if_changed();
                Poll::Ready(Some(Ok(batch)))
            }
            // Do not add a timer or wake the stream only to refresh progress.
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
async fn batch_stream_for_lazy_table_from_session_parts(
    context: &SessionContext,
    table: &LazyTable,
    sources: &[RegisteredSessionSource],
    derived_tables: &[RegisteredDerivedTable],
    pending_derived_tables: &[PendingDerivedTable],
    provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
    progress: Option<(ProgressReporter, String)>,
) -> Result<MssqlOutputBatchStream, DeltaFunnelError> {
    let dataframe = dataframe_for_lazy_table_from_session_parts(
        context,
        table,
        sources,
        derived_tables,
        pending_derived_tables,
    )
    .await?;
    let physical_plan = dataframe
        .create_physical_plan()
        .await
        .map_err(|error| datafusion_handoff_setup_error("physical_plan", error))?;

    batch_stream_for_physical_plan(
        context,
        physical_plan,
        provider_stats_snapshots,
        progress,
        None,
    )
    .map(|setup| setup.stream)
    .map_err(|failure| *failure.source)
}

/// A tracked query stream plus the optional terminal profile result handle.
///
/// The result becomes available only after the stream reaches EOF, errors, or
/// is dropped.
pub(super) struct QueryStreamSetup {
    pub(super) stream: MssqlOutputBatchStream,
    pub(super) profile_result: Option<QueryExecutionProfileResult>,
}

/// A stream setup failure plus any profile finalized at that boundary.
pub(super) struct QueryStreamSetupFailure {
    pub(super) source: Box<DeltaFunnelError>,
    pub(super) execution_profile: Option<QueryExecutionProfile>,
}

/// Creates the merged stream and installs its shared terminal observers.
pub(super) fn batch_stream_for_physical_plan(
    context: &SessionContext,
    physical_plan: Arc<dyn ExecutionPlan>,
    provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
    progress: Option<(ProgressReporter, String)>,
    profile_scope: Option<QueryExecutionScope>,
) -> Result<QueryStreamSetup, QueryStreamSetupFailure> {
    // These handles must exist before merged stream setup can fail.
    let read_stats_handles = collect_delta_provider_read_stats_handles(physical_plan.as_ref());
    let DFQueryExecution {
        stream,
        effective_profile_root,
    } = match datafusion_query_output_stream_with_effective_root(
        Arc::clone(&physical_plan),
        context.task_ctx(),
    ) {
        Ok(execution) => execution,
        Err(error) => {
            let (profile_consumer, profile_result) = match profile_scope {
                Some(scope) => {
                    let (consumer, result) =
                        QueryExecutionProfileConsumer::register(physical_plan, scope, None);
                    (Some(consumer), Some(result))
                }
                None => (None, None),
            };
            finalize_tracked_query_execution(
                &read_stats_handles,
                provider_stats_snapshots.as_ref(),
                profile_consumer,
                DeltaProviderScanOutcome::Error,
            );
            return Err(QueryStreamSetupFailure {
                source: Box::new(datafusion_handoff_setup_error("query_output_stream", error)),
                execution_profile: clone_terminal_execution_profile(profile_result),
            });
        }
    };
    let (profile_consumer, profile_result) = match profile_scope {
        Some(scope) => {
            let (consumer, result) =
                QueryExecutionProfileConsumer::register(effective_profile_root, scope, None);
            (Some(consumer), Some(result))
        }
        None => (None, None),
    };
    let stream = Box::pin(stream.map(|batch| {
        batch.map_err(|error| datafusion_handoff_setup_error("query_output_stream", error))
    }));

    Ok(QueryStreamSetup {
        stream: wrap_stream_with_query_execution_tracking(
            stream,
            read_stats_handles,
            provider_stats_snapshots,
            progress,
            profile_consumer,
        ),
        profile_result,
    })
}

struct PreviewTimingTracker {
    phase_timings: Vec<PhaseTimingReport>,
    timeline: OperationTimelineRecorder,
}

struct PreviewPhaseTimer {
    phase_name: &'static str,
    timeline_span: OperationTimelineSpanRecorder,
}

impl PreviewTimingTracker {
    fn start() -> Self {
        Self {
            phase_timings: Vec::with_capacity(PREVIEW_PHASE_NAMES.len()),
            timeline: OperationTimelineRecorder::start(),
        }
    }

    fn start_phase(&self, phase_name: &'static str) -> PreviewPhaseTimer {
        let display_name = preview_phase_display_name(phase_name);
        PreviewPhaseTimer {
            phase_name,
            timeline_span: self
                .timeline
                .start_span(display_name, "delta_funnel.preview.phase", display_name)
                .with_attribute(
                    "phase_name",
                    serde_json::Value::String(phase_name.to_owned()),
                ),
        }
    }

    fn record_completed(&mut self, timer: PreviewPhaseTimer) {
        let timing = self.record_timing(timer, TimelineSpanStatus::Completed);
        self.phase_timings.push(timing);
    }

    fn record_timing(
        &mut self,
        timer: PreviewPhaseTimer,
        status: TimelineSpanStatus,
    ) -> PhaseTimingReport {
        let duration = timer.timeline_span.finish_with_duration(status);
        match status {
            TimelineSpanStatus::Completed => {
                PhaseTimingReport::completed(timer.phase_name, duration)
            }
            TimelineSpanStatus::Failed | TimelineSpanStatus::Cancelled => {
                PhaseTimingReport::failed(timer.phase_name, duration)
            }
        }
    }

    fn completed(
        mut self,
        execution_profile: Option<&QueryExecutionProfile>,
    ) -> (Vec<PhaseTimingReport>, OperationTimeline) {
        if let Some(execution_profile) = execution_profile {
            self.timeline.append_operator_lifecycles(execution_profile);
        }
        let timeline = self
            .timeline
            .finish("Preview total", TimelineSpanStatus::Completed);
        self.phase_timings.push(PhaseTimingReport::completed(
            PREVIEW_TOTAL_PHASE,
            Duration::from_micros(timeline.total_duration_micros()),
        ));
        (self.phase_timings, timeline)
    }

    fn failed(
        mut self,
        timer: PreviewPhaseTimer,
        execution_profile: Option<QueryExecutionProfile>,
        source: DeltaFunnelError,
    ) -> DeltaFunnelError {
        let failed_timing = self.record_timing(timer, TimelineSpanStatus::Failed);
        let failed_phase = failed_timing.phase_name().to_owned();
        self.phase_timings.push(failed_timing);
        let next_phase_index = self.phase_timings.len();
        let non_total_phases = &PREVIEW_PHASE_NAMES[..PREVIEW_PHASE_NAMES.len() - 1];
        debug_assert!(next_phase_index <= non_total_phases.len());
        let remaining_non_total_phases =
            non_total_phases.get(next_phase_index..).unwrap_or_default();
        self.phase_timings
            .extend(remaining_non_total_phases.iter().map(|phase_name| {
                PhaseTimingReport::not_started(*phase_name, ReportReasonCode::PriorFailure)
            }));
        if let Some(execution_profile) = execution_profile.as_ref() {
            self.timeline.append_operator_lifecycles(execution_profile);
        }
        let timeline = self
            .timeline
            .finish("Preview total", TimelineSpanStatus::Failed);
        self.phase_timings.push(PhaseTimingReport::failed(
            PREVIEW_TOTAL_PHASE,
            Duration::from_micros(timeline.total_duration_micros()),
        ));

        DeltaFunnelError::PreviewFailed {
            context: Box::new(
                PreviewFailureContext::new(failed_phase, self.phase_timings, execution_profile)
                    .with_operation_timeline(timeline),
            ),
            source: Box::new(source),
        }
    }
}

fn preview_phase_display_name(phase_name: &str) -> &str {
    match phase_name {
        PREVIEW_DATAFRAME_PLANNING_PHASE => "DataFrame planning",
        PREVIEW_PHYSICAL_PLANNING_PHASE => "Physical planning",
        PREVIEW_STREAM_SETUP_PHASE => "Stream setup",
        PREVIEW_EXECUTE_COLLECT_PHASE => "Execute and collect",
        PREVIEW_FORMAT_TEXT_PHASE => "Format text",
        PREVIEW_FORMAT_HTML_PHASE => "Format HTML",
        _ => phase_name,
    }
}

fn register_preview_execution_profile(
    root: Arc<dyn ExecutionPlan>,
    options: PreviewOptions,
) -> (
    Option<QueryExecutionProfileConsumer>,
    Option<QueryExecutionProfileResult>,
) {
    match options.execution_profile_mode() {
        ExecutionProfileMode::Disabled => (None, None),
        ExecutionProfileMode::Detailed => {
            let (consumer, result) = QueryExecutionProfileConsumer::register(
                root,
                QueryExecutionScope::Preview,
                Some(usize_to_u64_saturating(options.limit())),
            );
            (Some(consumer), Some(result))
        }
    }
}

pub(super) fn clone_terminal_execution_profile(
    result: Option<QueryExecutionProfileResult>,
) -> Option<QueryExecutionProfile> {
    result
        .as_ref()
        .and_then(QueryExecutionProfileResult::profile)
        .cloned()
}

impl DeltaFunnelSession {
    /// Builds a batch stream and optionally reports Delta file progress while
    /// that stream is consumed.
    ///
    /// `progress` contains the reporter and output name used for emitted
    /// events. `None` returns the normal stream without progress sampling.
    #[cfg(test)]
    pub(crate) async fn batch_stream_for_lazy_table(
        &self,
        table: &LazyTable,
        progress: Option<(&ProgressReporter, &str)>,
    ) -> Result<MssqlOutputBatchStream, DeltaFunnelError> {
        batch_stream_for_lazy_table_from_session_parts(
            &self.context,
            table,
            &self.sources,
            &self.derived_tables,
            &self.pending_derived_tables,
            None,
            progress.map(|(reporter, output_name)| (reporter.clone(), output_name.to_owned())),
        )
        .await
    }

    /// Executes a bounded preview of a lazy table and returns DataFusion's
    /// formatted table output.
    ///
    /// # Errors
    ///
    /// Returns an error when the lazy table is unknown, DataFusion cannot apply
    /// the limit, or preview execution fails.
    pub async fn preview_table(
        &self,
        table: &LazyTable,
        limit: usize,
    ) -> Result<TablePreview, DeltaFunnelError> {
        self.preview_table_with_options(table, PreviewOptions::new(limit))
            .await
    }

    /// Executes a bounded preview with explicit profiling options.
    ///
    /// # Errors
    ///
    /// Returns an error when the lazy table is unknown, DataFusion cannot apply
    /// the limit, or preview execution or formatting fails.
    pub async fn preview_table_with_options(
        &self,
        table: &LazyTable,
        options: PreviewOptions,
    ) -> Result<TablePreview, DeltaFunnelError> {
        self.build_preview(table, options, None).await
    }

    /// Executes the same bounded preview while reporting its live lifecycle.
    pub(crate) async fn preview_table_with_progress(
        &self,
        table: &LazyTable,
        limit: usize,
        reporter: ProgressReporter,
    ) -> Result<TablePreview, DeltaFunnelError> {
        self.preview_table_with_options_and_progress(table, PreviewOptions::new(limit), reporter)
            .await
    }

    /// Executes the same option-bearing preview while reporting its lifecycle.
    pub(crate) async fn preview_table_with_options_and_progress(
        &self,
        table: &LazyTable,
        options: PreviewOptions,
        reporter: ProgressReporter,
    ) -> Result<TablePreview, DeltaFunnelError> {
        reporter.emit(&ProgressEvent::started(ProgressOperation::PreviewTable));
        let result = self.build_preview(table, options, Some(&reporter)).await;
        reporter.emit(&if result.is_ok() {
            ProgressEvent::completed()
        } else {
            ProgressEvent::failed()
        });
        result
    }

    /// Runs preview phases and optionally announces each live boundary.
    ///
    /// This future owns phase timing and rendering. After stream setup, the
    /// terminal stream owns profile and provider finalization on EOF, error, or
    /// cancellation.
    async fn build_preview(
        &self,
        table: &LazyTable,
        options: PreviewOptions,
        reporter: Option<&ProgressReporter>,
    ) -> Result<TablePreview, DeltaFunnelError> {
        let mut timings = PreviewTimingTracker::start();

        emit_preview_phase(reporter, ProgressPhase::PreparingPreview);
        let dataframe_timer = timings.start_phase(PREVIEW_DATAFRAME_PLANNING_PHASE);
        let dataframe = match self.dataframe_for_lazy_table(table).await {
            Ok(dataframe) => dataframe,
            Err(source) => return Err(timings.failed(dataframe_timer, None, source)),
        };
        let schema = Arc::new(dataframe.schema().as_arrow().clone());
        let dataframe = match dataframe.limit(0, Some(options.limit())) {
            Ok(dataframe) => dataframe,
            Err(error) => {
                let source = datafusion_handoff_setup_error("preview_limit", error);
                return Err(timings.failed(dataframe_timer, None, source));
            }
        };
        let task_context = Arc::new(dataframe.task_ctx());
        timings.record_completed(dataframe_timer);

        let physical_plan_timer = timings.start_phase(PREVIEW_PHYSICAL_PLANNING_PHASE);
        let physical_plan = match dataframe.create_physical_plan().await {
            Ok(physical_plan) => physical_plan,
            Err(error) => {
                let source = datafusion_handoff_setup_error("preview_collect", error);
                return Err(timings.failed(physical_plan_timer, None, source));
            }
        };
        timings.record_completed(physical_plan_timer);

        let stream_setup_timer = timings.start_phase(PREVIEW_STREAM_SETUP_PHASE);
        let read_stats_handles = collect_delta_provider_read_stats_handles(physical_plan.as_ref());
        emit_preview_phase(reporter, ProgressPhase::CollectingPreview);
        let DFQueryExecution {
            stream,
            effective_profile_root,
        } = match datafusion_query_output_stream_with_effective_root(
            Arc::clone(&physical_plan),
            task_context,
        ) {
            Ok(execution) => execution,
            Err(error) => {
                let (profile_consumer, profile_result) =
                    register_preview_execution_profile(physical_plan, options);
                finalize_tracked_query_execution(
                    &read_stats_handles,
                    None,
                    profile_consumer,
                    DeltaProviderScanOutcome::Error,
                );
                let execution_profile = clone_terminal_execution_profile(profile_result);
                let source = datafusion_handoff_setup_error("preview_collect", error);
                return Err(timings.failed(stream_setup_timer, execution_profile, source));
            }
        };
        drop(physical_plan);
        let (profile_consumer, profile_result) =
            register_preview_execution_profile(effective_profile_root, options);
        let stream: MssqlOutputBatchStream = Box::pin(stream.map(|batch| {
            batch.map_err(|error| datafusion_handoff_setup_error("preview_collect", error))
        }));
        let stream = match reporter {
            Some(reporter) => {
                let sampler = DeltaFileProgressSampler::new(
                    read_stats_handles.clone(),
                    reporter.clone(),
                    ProgressPhase::CollectingPreview,
                    None,
                );
                track_delta_file_progress(stream, sampler)
            }
            None => stream,
        };
        let stream =
            track_query_execution_completion(stream, read_stats_handles, None, profile_consumer);
        timings.record_completed(stream_setup_timer);

        let execute_collect_timer = timings.start_phase(PREVIEW_EXECUTE_COLLECT_PHASE);
        let batches = match stream.try_collect::<Vec<_>>().await {
            Ok(batches) => batches,
            Err(source) => {
                let execution_profile = clone_terminal_execution_profile(profile_result);
                return Err(timings.failed(execute_collect_timer, execution_profile, source));
            }
        };
        timings.record_completed(execute_collect_timer);
        let execution_profile = clone_terminal_execution_profile(profile_result);

        emit_preview_phase(reporter, ProgressPhase::FormattingPreview);
        format_preview_result(
            &schema,
            &batches,
            timings,
            execution_profile,
            preview_batches_to_text,
            preview_batches_to_html,
        )
    }

    pub(super) async fn dataframe_for_lazy_table(
        &self,
        table: &LazyTable,
    ) -> Result<DataFrame, DeltaFunnelError> {
        dataframe_for_lazy_table_from_session_parts(
            &self.context,
            table,
            &self.sources,
            &self.derived_tables,
            &self.pending_derived_tables,
        )
        .await
    }

    pub(super) fn schema_for_lazy_table(
        &self,
        table: &LazyTable,
    ) -> Result<&SchemaRef, DeltaFunnelError> {
        match table.kind() {
            LazyTableKind::DeltaSource => self
                .sources
                .iter()
                .find(|source| source.table().id() == table.id())
                .map(RegisteredSessionSource::schema),
            LazyTableKind::DerivedSql => self
                .derived_tables
                .iter()
                .find(|derived| derived.table().id() == table.id())
                .map(RegisteredDerivedTable::schema)
                .or_else(|| {
                    self.pending_derived_tables
                        .iter()
                        .find(|pending| pending.table.id() == table.id())
                        .map(|pending| &pending.schema)
                }),
        }
        .ok_or_else(|| unknown_lazy_table_error(table))
    }

    /// Builds a deferred stream factory with optional final stats and live file
    /// progress tracking.
    ///
    /// `progress` contains an output-scoped reporter and that output's name.
    #[cfg(test)]
    pub(super) fn lazy_table_batch_stream_factory(
        &self,
        table: LazyTable,
        provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
        progress: Option<(ProgressReporter, String)>,
    ) -> MssqlOutputBatchStreamFactory {
        let context = self.context.clone();
        let sources = self.sources.clone();
        let derived_tables = self.derived_tables.clone();
        let pending_derived_tables = self.pending_derived_tables.clone();

        Box::new(move || {
            Box::pin(async move {
                batch_stream_for_lazy_table_from_session_parts(
                    &context,
                    &table,
                    &sources,
                    &derived_tables,
                    &pending_derived_tables,
                    provider_stats_snapshots,
                    progress,
                )
                .await
            })
        })
    }
}

fn emit_preview_phase(reporter: Option<&ProgressReporter>, phase: ProgressPhase) {
    if let Some(reporter) = reporter {
        reporter.emit(&ProgressEvent::phase_changed(phase, None));
    }
}

fn format_preview_result(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    mut timings: PreviewTimingTracker,
    execution_profile: Option<QueryExecutionProfile>,
    text_formatter: fn(&[RecordBatch]) -> Result<String, ArrowError>,
    html_formatter: fn(&SchemaRef, &[RecordBatch]) -> Result<String, ArrowError>,
) -> Result<TablePreview, DeltaFunnelError> {
    let format_text_timer = timings.start_phase(PREVIEW_FORMAT_TEXT_PHASE);
    let text = match text_formatter(batches) {
        Ok(text) => text,
        Err(error) => {
            let source = datafusion_handoff_setup_error("preview_text", error);
            return Err(timings.failed(format_text_timer, execution_profile, source));
        }
    };
    timings.record_completed(format_text_timer);

    let format_html_timer = timings.start_phase(PREVIEW_FORMAT_HTML_PHASE);
    let html = match html_formatter(schema, batches) {
        Ok(html) => html,
        Err(error) => {
            let source = datafusion_handoff_setup_error("preview_html", error);
            return Err(timings.failed(format_html_timer, execution_profile, source));
        }
    };
    timings.record_completed(format_html_timer);

    let (phase_timings, operation_timeline) = timings.completed(execution_profile.as_ref());
    Ok(TablePreview::from_execution(
        text,
        html,
        phase_timings,
        execution_profile,
        Some(operation_timeline),
    ))
}

fn preview_batches_to_text(batches: &[RecordBatch]) -> Result<String, ArrowError> {
    pretty_format_batches(batches).map(|text| text.to_string())
}

fn preview_batches_to_html(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<String, ArrowError> {
    let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let column_count = schema.fields().len();
    let mut html = String::new();
    html.push_str("<div class=\"deltafunnel-preview\"><style>");
    html.push_str(".deltafunnel-preview{font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,\"Segoe UI\",sans-serif;font-size:12px;line-height:1.35;color:var(--vscode-editor-foreground,#111827)}");
    html.push_str(".deltafunnel-preview .df-wrap{display:inline-block;max-width:100%;overflow:auto;border:1px solid rgba(127,127,127,.35);border-radius:6px;background:var(--vscode-editor-background,#fff)}");
    html.push_str(".deltafunnel-preview table{border-collapse:separate;border-spacing:0}");
    html.push_str(".deltafunnel-preview th,.deltafunnel-preview td{padding:6px 10px;border-bottom:1px solid rgba(127,127,127,.18);white-space:nowrap;text-align:left;vertical-align:top}");
    html.push_str(".deltafunnel-preview th{position:sticky;top:0;background:var(--vscode-editor-background,#fff);font-weight:600;border-bottom:1px solid rgba(127,127,127,.35)}");
    html.push_str(
        ".deltafunnel-preview tbody tr:nth-child(even){background:rgba(127,127,127,.05)}",
    );
    html.push_str(".deltafunnel-preview .df-type,.deltafunnel-preview .df-footer{color:var(--vscode-descriptionForeground,#64748b);font-size:11px}");
    html.push_str(
        ".deltafunnel-preview .df-num{text-align:right;font-variant-numeric:tabular-nums}",
    );
    html.push_str(".deltafunnel-preview .df-footer{margin-top:6px}");
    html.push_str("@media (prefers-color-scheme:dark){.deltafunnel-preview{color:var(--vscode-editor-foreground,#e5e7eb)}.deltafunnel-preview .df-wrap,.deltafunnel-preview th{background:var(--vscode-editor-background,#0b1220)}}");
    html.push_str("</style>");

    if column_count == 0 {
        html.push_str("<div class=\"df-wrap\" style=\"padding:10px\">(No columns)</div>");
        html.push_str(&format!(
            "<div class=\"df-footer\">Showing <b>{row_count}</b> rows, <b>0</b> columns.</div></div>"
        ));
        return Ok(html);
    }

    html.push_str("<div class=\"df-wrap\"><table><thead><tr>");
    for field in schema.fields() {
        let class = if is_numeric_type(field.data_type()) {
            " class=\"df-num\""
        } else {
            ""
        };
        html.push_str(&format!("<th{class}><span>"));
        push_html_escaped(&mut html, field.name());
        html.push_str("</span><br><span class=\"df-type\">");
        push_html_escaped(&mut html, &field.data_type().to_string());
        html.push_str("</span></th>");
    }
    html.push_str("</tr></thead><tbody>");

    let options = FormatOptions::default().with_null("null");
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            html.push_str("<tr>");
            for (field, formatter) in schema.fields().iter().zip(&formatters) {
                let class = if is_numeric_type(field.data_type()) {
                    " class=\"df-num\""
                } else {
                    ""
                };
                html.push_str(&format!("<td{class}>"));
                push_html_escaped(&mut html, &formatter.value(row).to_string());
                html.push_str("</td>");
            }
            html.push_str("</tr>");
        }
    }

    html.push_str(&format!(
        "</tbody></table></div><div class=\"df-footer\">Showing <b>{row_count}</b> rows, <b>{column_count}</b> columns.</div></div>"
    ));

    Ok(html)
}

fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn push_html_escaped(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&#39;"),
            _ => output.push(character),
        }
    }
}

pub(super) async fn dataframe_for_lazy_table_from_session_parts(
    context: &SessionContext,
    table: &LazyTable,
    sources: &[RegisteredSessionSource],
    derived_tables: &[RegisteredDerivedTable],
    pending_derived_tables: &[PendingDerivedTable],
) -> Result<DataFrame, DeltaFunnelError> {
    match table.kind() {
        LazyTableKind::DeltaSource => {
            let source = sources
                .iter()
                .find(|source| source.table().id() == table.id())
                .ok_or_else(|| unknown_lazy_table_error(table))?;

            context
                .table(source.name())
                .await
                .map_err(|error| datafusion_handoff_setup_error("registered_table", error))
        }
        LazyTableKind::DerivedSql => {
            if let Some(derived) = derived_tables
                .iter()
                .find(|derived| derived.table().id() == table.id())
            {
                return context
                    .table(derived.name())
                    .await
                    .map_err(|error| datafusion_handoff_setup_error("registered_table", error));
            }

            let pending = pending_derived_tables
                .iter()
                .find(|pending| pending.table.id() == table.id())
                .ok_or_else(|| unknown_lazy_table_error(table))?;

            context
                .read_table(Arc::clone(&pending.provider))
                .map_err(|error| datafusion_handoff_setup_error("pending_table", error))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use crate::{
        DeltaFunnelError, DeltaProviderReaderBackend, DeltaProviderScanExecutionOptions,
        DeltaSourceConfig, ExecutionProfileMode, PhaseStatus, PreviewFailureContext,
        PreviewOptions, QueryExecutionOutcome, QueryExecutionProfile, QueryExecutionScope,
        QueryOptions, ReportReasonCode, TimelineSpanStatus, TimelineSpanTimeSemantics,
        observability::test_capture::{CapturedEvent, CapturedEvents, TracingCapture},
        progress::{ProgressEventKind, ProgressOperation, ProgressPhase, ProgressReporter},
        query_engine::datafusion::execution_profile::{
            QueryExecutionProfileConsumer, QueryExecutionProfileResult,
        },
        table_formats::RealParquetDeltaTable,
    };
    use datafusion::{
        arrow::{
            datatypes::{DataType, Schema},
            error::ArrowError,
            record_batch::RecordBatch,
        },
        logical_expr::{Volatility, create_udf},
        physical_plan::{ExecutionPlan, empty::EmptyExec, union::UnionExec},
    };
    use futures_util::StreamExt;
    use tracing::Level;

    use super::super::{
        DeltaFunnelSession, LazyTable, LazyTableKind, SessionOptions,
        test_support::{
            DeltaLogTable, StreamSetupFailingPlan, blocking_marker_provider,
            collect_stream_marker_values, collect_stream_row_count,
            failing_scan_marker_region_provider, marker_region_provider,
            marker_values_from_batches, plan_lifetime_tracking_marker_region_provider,
            scan_counting_marker_region_provider, stream_setup_failing_marker_region_provider,
        },
    };

    type RecordedFileProgress = (String, u64, u64);
    type RecordedPreviewFileProgress = (u64, u64);
    type RecordedPreviewProgress = (
        ProgressEventKind,
        Option<ProgressOperation>,
        Option<ProgressPhase>,
    );
    const PARQUET_IO_FIELDS: [&str; 4] = [
        "parquet_data_file_range_get_operations",
        "parquet_data_file_full_get_operations",
        "parquet_data_file_bytes_received",
        "parquet_data_file_opened_bytes",
    ];
    const PREVIEW_PHASES: [&str; 7] = [
        "preview_dataframe_planning",
        "preview_physical_planning",
        "preview_stream_setup",
        "preview_execute_collect",
        "preview_format_text",
        "preview_format_html",
        "preview_total",
    ];

    fn preview_failure_parts(
        error: &DeltaFunnelError,
    ) -> Result<(&PreviewFailureContext, &DeltaFunnelError), Box<dyn std::error::Error>> {
        match error {
            DeltaFunnelError::PreviewFailed { context, source } => Ok((context, source)),
            other => Err(format!("expected PreviewFailed, got {other:?}").into()),
        }
    }

    fn assert_preview_failure_context(
        context: &PreviewFailureContext,
        failed_phase: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let failed_index = PREVIEW_PHASES
            .iter()
            .position(|phase| *phase == failed_phase)
            .ok_or("unknown preview failure phase")?;

        assert_eq!(context.failed_phase(), failed_phase);
        assert_eq!(context.phase_timings().len(), PREVIEW_PHASES.len());
        for (index, timing) in context.phase_timings().iter().enumerate() {
            assert_eq!(timing.phase_name(), PREVIEW_PHASES[index]);
            let expected_status = if index < failed_index {
                PhaseStatus::completed()
            } else if index == failed_index || index == PREVIEW_PHASES.len() - 1 {
                PhaseStatus::failed()
            } else {
                PhaseStatus::not_started(ReportReasonCode::PriorFailure)
            };
            assert_eq!(timing.status(), expected_status);
            assert_eq!(
                timing.elapsed_micros().is_some(),
                !expected_status.is_not_started()
            );
        }
        let timeline = context
            .operation_timeline()
            .ok_or("expected preview failure timeline")?;
        assert_eq!(timeline.status(), TimelineSpanStatus::Failed);
        assert_eq!(
            timeline.total_duration_micros(),
            context
                .phase_timings()
                .last()
                .and_then(crate::PhaseTimingReport::elapsed_micros)
                .ok_or("expected failed preview total duration")?
        );
        assert_eq!(
            timeline
                .spans()
                .iter()
                .filter(|span| span.category() == "delta_funnel.preview.phase")
                .count(),
            failed_index + 1
        );
        assert!(timeline.spans().iter().all(|span| {
            span.start_offset_micros()
                .saturating_add(span.duration_micros())
                <= timeline.total_duration_micros()
        }));
        Ok(())
    }

    fn preview_timings_before_formatting() -> super::PreviewTimingTracker {
        let mut timings = super::PreviewTimingTracker::start();
        for &phase_name in &PREVIEW_PHASES[..4] {
            let timer = timings.start_phase(phase_name);
            timings.record_completed(timer);
        }
        timings
    }

    fn successful_preview_profile() -> QueryExecutionProfile {
        QueryExecutionProfile::preview(QueryExecutionOutcome::Success, 1, Vec::new())
    }

    fn fail_preview_text(_batches: &[RecordBatch]) -> Result<String, ArrowError> {
        Err(ArrowError::ComputeError(
            "text formatting failed".to_owned(),
        ))
    }

    fn keep_preview_text(_batches: &[RecordBatch]) -> Result<String, ArrowError> {
        Ok("preview text".to_owned())
    }

    fn fail_preview_html(
        _schema: &datafusion::arrow::datatypes::SchemaRef,
        _batches: &[RecordBatch],
    ) -> Result<String, ArrowError> {
        Err(ArrowError::ComputeError(
            "HTML formatting failed".to_owned(),
        ))
    }

    async fn report_tracked_stream(
        session: &DeltaFunnelSession,
        table: &LazyTable,
        provider_stats_snapshots: &super::SharedProviderStatsSnapshots,
    ) -> Result<crate::MssqlOutputBatchStream, DeltaFunnelError> {
        super::batch_stream_for_lazy_table_from_session_parts(
            &session.context,
            table,
            &session.sources,
            &session.derived_tables,
            &session.pending_derived_tables,
            Some(Arc::clone(provider_stats_snapshots)),
            None,
        )
        .await
    }

    fn recording_preview_progress() -> (ProgressReporter, Arc<Mutex<Vec<RecordedPreviewProgress>>>)
    {
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback_events = Arc::clone(&events);
        let reporter = ProgressReporter::new(move |event| {
            let event = (event.kind(), event.operation(), event.phase());
            match callback_events.lock() {
                Ok(mut events) => events.push(event),
                Err(poisoned) => poisoned.into_inner().push(event),
            }
        });
        (reporter, events)
    }

    fn recording_preview_file_progress() -> (
        ProgressReporter,
        Arc<Mutex<Vec<RecordedPreviewFileProgress>>>,
    ) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback_events = Arc::clone(&events);
        let reporter = ProgressReporter::new(move |event| {
            let Some(file_progress) = event.files_handled().zip(event.files_total()) else {
                return;
            };
            match callback_events.lock() {
                Ok(mut events) => events.push(file_progress),
                Err(poisoned) => poisoned.into_inner().push(file_progress),
            }
        });
        (reporter, events)
    }

    fn recording_file_progress() -> (ProgressReporter, Arc<Mutex<Vec<RecordedFileProgress>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback_events = Arc::clone(&events);
        let reporter = ProgressReporter::new(move |event| {
            let (Some(output_name), Some(handled), Some(total)) = (
                event.output_name(),
                event.files_handled(),
                event.files_total(),
            ) else {
                return;
            };
            match callback_events.lock() {
                Ok(mut events) => events.push((output_name.to_owned(), handled, total)),
                Err(poisoned) => {
                    poisoned
                        .into_inner()
                        .push((output_name.to_owned(), handled, total));
                }
            }
        });
        (reporter, events)
    }

    fn recorded_file_progress(
        events: &Mutex<Vec<RecordedFileProgress>>,
    ) -> Vec<RecordedFileProgress> {
        match events.lock() {
            Ok(events) => events.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn provider_io_events(events: &CapturedEvents) -> Vec<CapturedEvent> {
        events
            .events()
            .into_iter()
            .filter(|event| {
                event.fields.get("telemetry_event").map(String::as_str)
                    == Some("delta_provider_parquet_io_summary")
            })
            .collect()
    }

    fn execution_profile_events(events: &CapturedEvents) -> Vec<CapturedEvent> {
        events
            .events()
            .into_iter()
            .filter(|event| {
                event.fields.get("telemetry_event").map(String::as_str)
                    == Some("query_execution_profile_terminal")
            })
            .collect()
    }

    fn assert_provider_io_event_matches_snapshot(
        event: &CapturedEvent,
        snapshot: &crate::DeltaProviderReadStatsSnapshot,
        outcome: &str,
    ) {
        let reader_backend = match snapshot.reader_backend {
            crate::DeltaProviderReaderBackend::OfficialKernel => "official_kernel",
            crate::DeltaProviderReaderBackend::NativeAsync => "native_async",
        };
        let metrics = [
            snapshot.parquet_data_file_range_get_operations,
            snapshot.parquet_data_file_full_get_operations,
            snapshot.parquet_data_file_bytes_received,
            snapshot.parquet_data_file_opened_bytes,
        ];
        let metrics_available = metrics.iter().all(Option::is_some);

        assert_eq!(event.target, "delta_funnel");
        assert_eq!(event.level, Level::DEBUG);
        assert_eq!(
            event.fields.get("source_name").map(String::as_str),
            Some(snapshot.source_name.as_str())
        );
        assert_eq!(
            event
                .fields
                .get("snapshot_version")
                .and_then(|value| value.parse::<u64>().ok()),
            Some(snapshot.snapshot_version)
        );
        assert_eq!(
            event.fields.get("reader_backend").map(String::as_str),
            Some(reader_backend)
        );
        assert_eq!(
            event.fields.get("outcome").map(String::as_str),
            Some(outcome)
        );
        assert_eq!(
            event.fields.get("metrics_available").map(String::as_str),
            Some(if metrics_available { "true" } else { "false" })
        );
        for (field, expected) in PARQUET_IO_FIELDS.iter().zip(metrics) {
            let expected = if metrics_available { expected } else { None };
            assert_eq!(
                event
                    .fields
                    .get(*field)
                    .and_then(|value| value.parse::<u64>().ok()),
                expected
            );
        }
    }

    async fn unexecuted_delta_read_stats_handles(
        fixture_name: &str,
        source_name: &str,
    ) -> Result<Vec<super::DeltaProviderReadStatsHandle>, Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default(fixture_name)?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            source_name,
            table.path().to_string_lossy().to_string(),
        ))?;
        let dataframe = session.dataframe_for_lazy_table(&source).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let handles = super::collect_delta_provider_read_stats_handles(physical_plan.as_ref());
        assert_eq!(handles.len(), 1);
        Ok(handles)
    }

    fn partition_stream(
        coordinator: &Arc<super::PartitionExecutionCoordinator>,
        batches: Vec<Result<RecordBatch, DeltaFunnelError>>,
    ) -> crate::MssqlOutputBatchStream {
        Box::pin(super::PartitionExecutionStream {
            inner: Box::pin(futures_util::stream::iter(batches)),
            coordinator: Some(Arc::clone(coordinator)),
        })
    }

    fn partitioned_coordinator_state(
        coordinator: &super::PartitionExecutionCoordinator,
    ) -> (usize, crate::observability::DeltaProviderScanOutcome) {
        match coordinator.state.lock() {
            Ok(state) => (state.remaining_streams, state.outcome),
            Err(poisoned) => {
                let state = poisoned.into_inner();
                (state.remaining_streams, state.outcome)
            }
        }
    }

    fn cache_alias_profile_consumer() -> (QueryExecutionProfileConsumer, QueryExecutionProfileResult)
    {
        let root: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        QueryExecutionProfileConsumer::register(root, QueryExecutionScope::WriteAllCacheAlias, None)
    }

    #[tokio::test]
    async fn partitioned_terminal_stream_finishes_once_after_repeated_eof_and_drop() {
        use crate::observability::DeltaProviderScanOutcome;

        let (consumer, profile_result) = cache_alias_profile_consumer();
        let coordinator = Arc::new(super::PartitionExecutionCoordinator::new(
            Vec::new(),
            Some(consumer),
            2,
        ));
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let mut first = partition_stream(&coordinator, vec![Ok(batch)]);
        let mut second = partition_stream(&coordinator, Vec::new());
        let capture = TracingCapture::start();

        assert!(first.next().await.is_some_and(|batch| batch.is_ok()));
        assert!(first.next().await.is_none());
        assert!(first.next().await.is_none());
        drop(first);
        assert_eq!(
            partitioned_coordinator_state(&coordinator),
            (1, DeltaProviderScanOutcome::Success)
        );
        assert_eq!(profile_result.profile(), None);

        assert!(second.next().await.is_none());
        drop(second);
        assert_eq!(
            partitioned_coordinator_state(&coordinator),
            (0, DeltaProviderScanOutcome::Success)
        );
        assert_eq!(
            profile_result.profile().map(QueryExecutionProfile::outcome),
            Some(QueryExecutionOutcome::Success)
        );
        assert_eq!(execution_profile_events(capture.captured()).len(), 1);
    }

    #[tokio::test]
    async fn partitioned_terminal_stream_emits_error_after_cleanup_drops_other_streams()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::observability::DeltaProviderScanOutcome;

        let handles =
            unexecuted_delta_read_stats_handles("partition-error-summary", "orders").await?;
        let snapshots = super::snapshot_delta_provider_read_stats(&handles);
        let expected = snapshots.first().ok_or("expected provider snapshot")?;
        let (consumer, profile_result) = cache_alias_profile_consumer();
        let coordinator = Arc::new(super::PartitionExecutionCoordinator::new(
            handles,
            Some(consumer),
            2,
        ));
        let mut errored = partition_stream(
            &coordinator,
            vec![Err(DeltaFunnelError::Config {
                message: "injected partition failure".to_owned(),
            })],
        );
        let unconsumed = partition_stream(&coordinator, Vec::new());
        let capture = TracingCapture::start();

        assert!(errored.next().await.is_some_and(|batch| batch.is_err()));
        drop(errored);
        assert_eq!(
            partitioned_coordinator_state(&coordinator),
            (1, DeltaProviderScanOutcome::Error)
        );
        assert!(provider_io_events(capture.captured()).is_empty());
        assert_eq!(profile_result.profile(), None);

        drop(unconsumed);
        assert_eq!(
            partitioned_coordinator_state(&coordinator),
            (0, DeltaProviderScanOutcome::Error)
        );
        let summaries = provider_io_events(capture.captured());
        assert_eq!(summaries.len(), 1);
        assert_provider_io_event_matches_snapshot(&summaries[0], expected, "error");
        assert_eq!(
            profile_result.profile().map(QueryExecutionProfile::outcome),
            Some(QueryExecutionOutcome::Error)
        );
        assert_eq!(execution_profile_events(capture.captured()).len(), 1);
        drop(coordinator);
        assert_eq!(provider_io_events(capture.captured()).len(), 1);
        assert_eq!(execution_profile_events(capture.captured()).len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn partitioned_terminal_stream_emits_cancelled_for_unconsumed_stream()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::observability::DeltaProviderScanOutcome;

        let handles =
            unexecuted_delta_read_stats_handles("partition-cancelled-summary", "orders").await?;
        let snapshots = super::snapshot_delta_provider_read_stats(&handles);
        let expected = snapshots.first().ok_or("expected provider snapshot")?;
        let (consumer, profile_result) = cache_alias_profile_consumer();
        let coordinator = Arc::new(super::PartitionExecutionCoordinator::new(
            handles,
            Some(consumer),
            2,
        ));
        let mut completed = partition_stream(&coordinator, Vec::new());
        let unconsumed = partition_stream(&coordinator, Vec::new());
        let capture = TracingCapture::start();

        assert!(completed.next().await.is_none());
        assert!(completed.next().await.is_none());
        drop(completed);
        assert!(provider_io_events(capture.captured()).is_empty());
        assert_eq!(profile_result.profile(), None);
        drop(unconsumed);

        assert_eq!(
            partitioned_coordinator_state(&coordinator),
            (0, DeltaProviderScanOutcome::Cancelled)
        );
        let summaries = provider_io_events(capture.captured());
        assert_eq!(summaries.len(), 1);
        assert_provider_io_event_matches_snapshot(&summaries[0], expected, "cancelled");
        assert_eq!(
            profile_result.profile().map(QueryExecutionProfile::outcome),
            Some(QueryExecutionOutcome::Cancelled)
        );
        assert_eq!(execution_profile_events(capture.captured()).len(), 1);
        drop(coordinator);
        assert_eq!(provider_io_events(capture.captured()).len(), 1);
        assert_eq!(execution_profile_events(capture.captured()).len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn merged_terminal_stream_releases_finalization_ownership_after_eof()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("terminal-ownership-release")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let dataframe = session.dataframe_for_lazy_table(&source).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let handles = super::collect_delta_provider_read_stats_handles(physical_plan.as_ref());
        assert_eq!(handles.len(), 1);
        let handle = Arc::downgrade(&handles[0]);
        drop(physical_plan);

        let retained = super::shared_provider_stats_snapshots();
        let inner: crate::MssqlOutputBatchStream = Box::pin(futures_util::stream::empty());
        let mut stream = super::QueryExecutionTerminalStream::new(
            inner,
            handles,
            Some(Arc::clone(&retained)),
            None,
        );

        assert!(stream.next().await.is_none());
        assert!(stream.read_stats_handles.is_none());
        assert!(stream.provider_stats_snapshots.is_none());
        assert!(handle.upgrade().is_none());
        assert!(stream.next().await.is_none());
        drop(stream);
        assert_eq!(super::provider_stats_snapshots(&retained).len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn merged_terminal_consumer_receives_each_outcome_once_without_delta_scans()
    -> Result<(), Box<dyn std::error::Error>> {
        let register = |scope: QueryExecutionScope, limit: Option<u64>| {
            let root: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
            QueryExecutionProfileConsumer::register(root, scope, limit)
        };
        let capture = TracingCapture::start();

        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::new(Schema::empty())).with_partitions(2));
        let execution =
            crate::query_engine::datafusion::datafusion_query_output_stream_with_effective_root(
                plan,
                Arc::new(datafusion::execution::TaskContext::default()),
            )?;
        let (consumer, success_result) = QueryExecutionProfileConsumer::register(
            execution.effective_profile_root,
            QueryExecutionScope::Preview,
            Some(20),
        );
        let inner: crate::MssqlOutputBatchStream = Box::pin(execution.stream.map(|batch| {
            batch.map_err(|error| super::datafusion_handoff_setup_error("profile_test", error))
        }));
        let mut success = super::wrap_stream_with_query_execution_tracking(
            inner,
            Vec::new(),
            None,
            None,
            Some(consumer),
        );
        assert!(success.next().await.is_none());
        assert!(success.next().await.is_none());
        drop(success);

        let (consumer, error_result) = register(QueryExecutionScope::MssqlOutput, None);
        let inner: crate::MssqlOutputBatchStream = Box::pin(futures_util::stream::iter([Err(
            DeltaFunnelError::Config {
                message: "injected stream failure".to_owned(),
            },
        )]));
        let mut error =
            super::QueryExecutionTerminalStream::new(inner, Vec::new(), None, Some(consumer));
        assert!(error.next().await.is_some_and(|batch| batch.is_err()));
        drop(error);

        let (consumer, cancelled_result) = register(QueryExecutionScope::WriteAllCacheAlias, None);
        let inner: crate::MssqlOutputBatchStream = Box::pin(futures_util::stream::pending());
        let cancelled =
            super::QueryExecutionTerminalStream::new(inner, Vec::new(), None, Some(consumer));
        drop(cancelled);

        let success_profile = success_result.profile().ok_or("expected success profile")?;
        assert_eq!(success_profile.outcome(), QueryExecutionOutcome::Success);
        assert_eq!(
            success_profile.operators()[0].operator_name(),
            "CoalescePartitionsExec"
        );
        assert_eq!(
            error_result
                .profile()
                .ok_or("expected error profile")?
                .outcome(),
            QueryExecutionOutcome::Error
        );
        assert_eq!(
            cancelled_result
                .profile()
                .ok_or("expected cancelled profile")?
                .outcome(),
            QueryExecutionOutcome::Cancelled
        );
        let events = execution_profile_events(capture.captured());
        assert_eq!(events.len(), 3);
        assert_eq!(
            events
                .iter()
                .filter_map(|event| event.fields.get("outcome").map(String::as_str))
                .collect::<Vec<_>>(),
            ["success", "error", "cancelled"]
        );
        assert!(provider_io_events(capture.captured()).is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn setup_failure_shares_one_terminal_snapshot_with_provider_and_profile()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("profile-stream-setup-error")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let dataframe = session.dataframe_for_lazy_table(&source).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let failing_plan: Arc<dyn ExecutionPlan> =
            Arc::new(StreamSetupFailingPlan::new(physical_plan));
        let handles = super::collect_delta_provider_read_stats_handles(failing_plan.as_ref());
        let retained = super::shared_provider_stats_snapshots();
        let (consumer, result) = QueryExecutionProfileConsumer::register(
            Arc::clone(&failing_plan),
            QueryExecutionScope::MssqlOutput,
            None,
        );
        let capture = TracingCapture::start();

        let stream =
            crate::datafusion_query_output_stream(failing_plan, session.context.task_ctx());
        assert!(stream.is_err());
        super::finalize_tracked_query_execution(
            &handles,
            Some(&retained),
            Some(consumer),
            crate::observability::DeltaProviderScanOutcome::Error,
        );

        let retained = super::provider_stats_snapshots(&retained);
        assert_eq!(retained.len(), 1);
        let profile = result.profile().ok_or("expected setup failure profile")?;
        assert_eq!(profile.outcome(), QueryExecutionOutcome::Error);
        assert_eq!(
            profile.operators()[0].operator_name(),
            "StreamSetupFailingPlan"
        );
        let profile_snapshot = profile
            .operators()
            .iter()
            .find_map(|operator| operator.delta_provider_read_stats())
            .ok_or("expected profile provider snapshot")?;
        assert_eq!(profile_snapshot, &retained[0]);
        let provider_events = provider_io_events(capture.captured());
        assert_eq!(provider_events.len(), 1);
        assert_provider_io_event_matches_snapshot(&provider_events[0], &retained[0], "error");
        let profile_events = execution_profile_events(capture.captured());
        assert_eq!(profile_events.len(), 1);
        assert_eq!(
            profile_events[0].fields.get("outcome").map(String::as_str),
            Some("error")
        );
        Ok(())
    }

    #[tokio::test]
    async fn batch_stream_for_lazy_table_reads_registered_delta_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;

        let stream = session.batch_stream_for_lazy_table(&source, None).await?;
        let rows = collect_stream_row_count(stream).await?;

        assert_eq!(rows, table.rows());
        Ok(())
    }

    #[tokio::test]
    async fn eof_records_one_snapshot_and_matching_success_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("final-stats-eof")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let shared_provider_stats = super::shared_provider_stats_snapshots();
        let capture = TracingCapture::start();
        let stream = report_tracked_stream(&session, &source, &shared_provider_stats).await?;

        let rows = collect_stream_row_count(stream).await?;
        let snapshots = super::provider_stats_snapshots(&shared_provider_stats);
        let summaries = provider_io_events(capture.captured());

        assert_eq!(rows, table.rows());
        assert_eq!(snapshots.len(), 1);
        assert_eq!(summaries.len(), 1);
        assert_provider_io_event_matches_snapshot(&summaries[0], &snapshots[0], "success");
        assert_eq!(snapshots[0].files_completed, 1);
        assert!(
            snapshots[0]
                .parquet_data_file_bytes_received
                .is_some_and(|bytes| bytes > 0)
        );
        Ok(())
    }

    #[tokio::test]
    async fn downstream_drop_records_one_snapshot_and_matching_cancelled_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let table =
            RealParquetDeltaTable::new_with_two_large_files("final-stats-downstream-drop", 20_000)?;
        let mut session =
            DeltaFunnelSession::new(SessionOptions::new().with_query_options(QueryOptions {
                target_partitions: Some(1),
                output_batch_size: Some(1),
            }))?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let shared_provider_stats = super::shared_provider_stats_snapshots();
        let capture = TracingCapture::start();
        let mut stream = report_tracked_stream(&session, &source, &shared_provider_stats).await?;

        assert!(stream.next().await.transpose()?.is_some());
        drop(stream);
        let snapshots = super::provider_stats_snapshots(&shared_provider_stats);
        let summaries = provider_io_events(capture.captured());

        assert_eq!(snapshots.len(), 1);
        assert_eq!(summaries.len(), 1);
        assert_provider_io_event_matches_snapshot(&summaries[0], &snapshots[0], "cancelled");
        assert!(snapshots[0].files_started > 0);
        assert!(snapshots[0].rows_produced > 0);
        assert!(
            snapshots[0]
                .parquet_data_file_bytes_received
                .is_some_and(|bytes| bytes > 0)
        );
        assert!(
            snapshots[0]
                .parquet_data_file_opened_bytes
                .is_some_and(|bytes| bytes > 0)
        );
        Ok(())
    }

    #[tokio::test]
    async fn upstream_error_then_drop_records_one_snapshot_and_matching_error_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("final-stats-upstream-error")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let shared_provider_stats = super::shared_provider_stats_snapshots();
        let capture = TracingCapture::start();
        let mut stream = report_tracked_stream(&session, &source, &shared_provider_stats).await?;

        let result = stream.next().await.ok_or("expected upstream error")?;
        assert!(result.is_err());
        drop(stream);
        let snapshots = super::provider_stats_snapshots(&shared_provider_stats);
        let summaries = provider_io_events(capture.captured());

        assert_eq!(snapshots.len(), 1);
        assert_eq!(summaries.len(), 1);
        assert_provider_io_event_matches_snapshot(&summaries[0], &snapshots[0], "error");
        assert_eq!(snapshots[0].files_started, 1);
        assert_eq!(snapshots[0].files_completed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn stream_setup_error_records_one_snapshot_and_error_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("final-stats-stream-setup-error")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let dataframe = session.dataframe_for_lazy_table(&source).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let failing_plan: Arc<dyn ExecutionPlan> =
            Arc::new(StreamSetupFailingPlan::new(physical_plan));
        let shared_provider_stats = super::shared_provider_stats_snapshots();
        let capture = TracingCapture::start();

        let result = super::batch_stream_for_physical_plan(
            &session.context,
            failing_plan,
            Some(Arc::clone(&shared_provider_stats)),
            None,
            Some(QueryExecutionScope::MssqlOutput),
        );
        let snapshots = super::provider_stats_snapshots(&shared_provider_stats);
        let summaries = provider_io_events(capture.captured());
        let profile_events = execution_profile_events(capture.captured());

        let failure = result.err().ok_or("expected stream setup failure")?;
        let profile = failure
            .execution_profile
            .ok_or("expected setup failure profile")?;
        assert_eq!(profile.scope(), QueryExecutionScope::MssqlOutput);
        assert_eq!(profile.outcome(), QueryExecutionOutcome::Error);
        assert!(profile.partial());
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].source_name, "orders");
        assert_eq!(snapshots[0].files_started, 0);
        assert_eq!(snapshots[0].parquet_data_file_range_get_operations, Some(0));
        assert_eq!(snapshots[0].parquet_data_file_bytes_received, Some(0));
        assert_eq!(snapshots[0].parquet_data_file_opened_bytes, Some(0));
        assert_eq!(summaries.len(), 1);
        assert_provider_io_event_matches_snapshot(&summaries[0], &snapshots[0], "error");
        assert_eq!(profile_events.len(), 1);
        assert_eq!(
            profile_events[0].fields.get("outcome").map(String::as_str),
            Some("error")
        );
        Ok(())
    }

    #[tokio::test]
    async fn zero_partition_tracking_emits_one_success_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("zero-partition-summary")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let dataframe = session.dataframe_for_lazy_table(&source).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let handles = super::collect_delta_provider_read_stats_handles(physical_plan.as_ref());
        assert_eq!(handles.len(), 1);

        let capture = TracingCapture::start();
        let streams =
            super::track_partitioned_query_execution_completion(Vec::new(), handles, None);
        let summaries = provider_io_events(capture.captured());

        assert!(streams.is_empty());
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].target, "delta_funnel");
        assert_eq!(summaries[0].level, Level::DEBUG);
        assert_eq!(
            summaries[0].fields.get("source_name").map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            summaries[0].fields.get("outcome").map(String::as_str),
            Some("success")
        );
        assert_eq!(
            summaries[0]
                .fields
                .get("metrics_available")
                .map(String::as_str),
            Some("true")
        );
        assert!(
            PARQUET_IO_FIELDS
                .iter()
                .all(|field| summaries[0].fields.contains_key(*field))
        );
        Ok(())
    }

    #[tokio::test]
    async fn merged_execution_emits_one_summary_per_distinct_scan_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("distinct-summary-identities")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let dataframe = session.dataframe_for_lazy_table(&source).await?;
        let first_plan = dataframe.clone().create_physical_plan().await?;
        let second_plan = dataframe.create_physical_plan().await?;

        // The second scan appears through two child paths. Its shared handle
        // must still produce one event, while the first scan remains distinct.
        let combined_plan =
            UnionExec::try_new(vec![Arc::clone(&second_plan), first_plan, second_plan])?;
        assert_eq!(
            super::collect_delta_provider_read_stats_handles(combined_plan.as_ref()).len(),
            2
        );

        let capture = TracingCapture::start();
        let stream = super::batch_stream_for_physical_plan(
            &session.context,
            combined_plan,
            None,
            None,
            None,
        )
        .map_err(|failure| *failure.source)?
        .stream;
        let rows = collect_stream_row_count(stream).await?;
        let summaries = provider_io_events(capture.captured());

        assert_eq!(rows, table.rows() * 3);
        assert_eq!(summaries.len(), 2);
        assert!(summaries.iter().all(|event| {
            event.fields.get("source_name").map(String::as_str) == Some("orders")
                && event.fields.get("outcome").map(String::as_str) == Some("success")
        }));
        Ok(())
    }

    #[tokio::test]
    async fn planning_error_before_handles_exist_records_no_provider_snapshot()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (provider, scans) = failing_scan_marker_region_provider();
        session
            .context()
            .register_table("broken_source", provider)?;
        let table = session
            .table_from_sql("select marker from broken_source")
            .await?;
        let shared_provider_stats = super::shared_provider_stats_snapshots();
        let capture = TracingCapture::start();

        let result = report_tracked_stream(&session, &table, &shared_provider_stats).await;

        assert!(result.is_err());
        assert_eq!(scans.load(Ordering::SeqCst), 1);
        assert!(super::provider_stats_snapshots(&shared_provider_stats).is_empty());
        assert!(execution_profile_events(capture.captured()).is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn batch_stream_reports_changing_delta_file_progress()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let (reporter, events) = recording_file_progress();

        let stream = session
            .batch_stream_for_lazy_table(&source, Some((&reporter, "orders output")))
            .await?;
        let rows = collect_stream_row_count(stream).await?;

        assert_eq!(rows, table.rows());
        let events = recorded_file_progress(&events);
        let Some(last) = events.last() else {
            return Err("Delta stream did not report file progress".into());
        };
        assert_eq!(last.0, "orders output");
        assert!(last.2 > 0);
        assert_eq!(last.1, last.2);
        assert!(events.windows(2).all(|events| events[0] != events[1]));
        Ok(())
    }

    #[tokio::test]
    async fn batch_stream_sums_file_progress_across_delta_scans()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = RealParquetDeltaTable::new_default("orders")?;
        let customers = RealParquetDeltaTable::new_default("customers")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.delta_lake(DeltaSourceConfig::new(
            "orders",
            orders.path().to_string_lossy().to_string(),
        ))?;
        session.delta_lake(DeltaSourceConfig::new(
            "customers",
            customers.path().to_string_lossy().to_string(),
        ))?;
        let joined = session
            .table_from_sql(
                "select o.id \
                 from orders o \
                 join customers c on o.id = c.id",
            )
            .await?;
        let (reporter, events) = recording_file_progress();

        let stream = session
            .batch_stream_for_lazy_table(&joined, Some((&reporter, "joined output")))
            .await?;
        let rows = collect_stream_row_count(stream).await?;

        assert_eq!(rows, 3);
        let events = recorded_file_progress(&events);
        let Some(last) = events.last() else {
            return Err("joined Delta stream did not report file progress".into());
        };
        assert_eq!(last, &("joined output".to_owned(), 2, 2));
        assert!(events.windows(2).all(|events| events[0] != events[1]));
        Ok(())
    }

    #[tokio::test]
    async fn batch_stream_for_lazy_table_reads_pending_derived_provider()
    -> Result<(), Box<dyn std::error::Error>> {
        let session_options = SessionOptions::new().with_query_options(QueryOptions {
            target_partitions: None,
            output_batch_size: Some(1),
        });
        let mut session = DeltaFunnelSession::new(session_options)?;
        let derived = session
            .table_from_sql("select 1 as id union all select 2 as id")
            .await?;

        let stream = session.batch_stream_for_lazy_table(&derived, None).await?;
        let rows = collect_stream_row_count(stream).await?;

        assert_eq!(rows, 2);
        Ok(())
    }

    #[tokio::test]
    async fn batch_stream_for_lazy_table_reads_registered_derived_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let derived = session
            .table_from_sql("select 'alice' as customer_name")
            .await?;
        let alias = session.register_alias("customer_names", &derived)?;

        let stream = session.batch_stream_for_lazy_table(&alias, None).await?;
        let rows = collect_stream_row_count(stream).await?;

        assert_eq!(rows, 1);
        Ok(())
    }

    #[tokio::test]
    async fn non_delta_execution_emits_no_provider_summary()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let derived = session
            .table_from_sql("select 1 as id union all select 2 as id")
            .await?;
        let capture = TracingCapture::start();

        let stream = session.batch_stream_for_lazy_table(&derived, None).await?;
        let rows = collect_stream_row_count(stream).await?;
        let summaries = provider_io_events(capture.captured());

        assert_eq!(rows, 2);
        assert!(summaries.is_empty());
        assert!(execution_profile_events(capture.captured()).is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn preview_table_returns_limited_formatted_rows() -> Result<(), DeltaFunnelError> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let table = session
            .table_from_sql("select 1 as id union all select 2 as id order by id")
            .await?;
        let capture = TracingCapture::start();

        let preview = session.preview_table(&table, 1).await?;

        assert!(preview.text().contains("| id |"));
        assert!(preview.text().lines().any(|line| line.contains("| 1  |")));
        assert!(!preview.text().lines().any(|line| line.contains("| 2  |")));
        assert!(preview.html().contains("class=\"deltafunnel-preview\""));
        assert!(
            preview
                .html()
                .contains("<th class=\"df-num\"><span>id</span>")
        );
        assert!(preview.html().contains("<td class=\"df-num\">1</td>"));
        assert!(!preview.html().contains("<td class=\"df-num\">2</td>"));
        assert_eq!(
            preview
                .phase_timings()
                .iter()
                .map(crate::PhaseTimingReport::phase_name)
                .collect::<Vec<_>>(),
            PREVIEW_PHASES
        );
        assert!(preview.phase_timings().iter().all(|timing| {
            timing.status() == PhaseStatus::completed() && timing.elapsed_micros().is_some()
        }));
        assert_eq!(preview.execution_profile(), None);
        assert!(preview.to_trace_event_json_value().is_none());
        let timeline = preview
            .operation_timeline()
            .ok_or(DeltaFunnelError::Config {
                message: "expected preview operation timeline".to_owned(),
            })?;
        assert_eq!(timeline.status(), TimelineSpanStatus::Completed);
        assert_eq!(timeline.spans().len(), PREVIEW_PHASES.len() - 1);
        assert!(timeline.spans().iter().all(|span| {
            span.time_semantics() == TimelineSpanTimeSemantics::WallClock
                && span
                    .start_offset_micros()
                    .saturating_add(span.duration_micros())
                    <= timeline.total_duration_micros()
        }));
        assert!(execution_profile_events(capture.captured()).is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn preview_unknown_table_returns_dataframe_failure_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;

        let result = session
            .preview_table_with_options(
                &LazyTable::placeholder(42, LazyTableKind::DerivedSql),
                PreviewOptions::new(1).with_execution_profile_mode(ExecutionProfileMode::Detailed),
            )
            .await;
        let error = match result {
            Ok(_) => return Err("expected preview failure".into()),
            Err(error) => error,
        };
        let (context, source) = preview_failure_parts(&error)?;

        assert_preview_failure_context(context, "preview_dataframe_planning")?;
        assert_eq!(context.execution_profile(), None);
        assert!(matches!(
            source,
            DeltaFunnelError::MssqlWorkflowPlanning { .. }
        ));
        assert!(std::error::Error::source(&error).is_some());
        let context_json = context.to_json_value();
        assert_eq!(context_json["failed_phase"], "preview_dataframe_planning");
        assert_eq!(context_json.as_object().map(serde_json::Map::len), Some(4));
        assert_eq!(context_json["operation_timeline"]["status"], "failed");
        assert!(context_json.get("source").is_none());
        Ok(())
    }

    #[test]
    fn preview_text_failure_retains_the_completed_execution_profile()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::empty());
        let expected_profile = successful_preview_profile();

        let result = super::format_preview_result(
            &schema,
            &[],
            preview_timings_before_formatting(),
            Some(expected_profile.clone()),
            fail_preview_text,
            super::preview_batches_to_html,
        );
        let error = match result {
            Ok(_) => return Err("expected text formatting failure".into()),
            Err(error) => error,
        };
        let (context, _) = preview_failure_parts(&error)?;

        assert_preview_failure_context(context, "preview_format_text")?;
        assert_eq!(context.execution_profile(), Some(&expected_profile));
        assert_eq!(expected_profile.outcome(), QueryExecutionOutcome::Success);
        assert!(!expected_profile.partial());
        Ok(())
    }

    #[test]
    fn preview_html_failure_retains_the_completed_execution_profile()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::empty());
        let expected_profile = successful_preview_profile();

        let result = super::format_preview_result(
            &schema,
            &[],
            preview_timings_before_formatting(),
            Some(expected_profile.clone()),
            keep_preview_text,
            fail_preview_html,
        );
        let error = match result {
            Ok(_) => return Err("expected HTML formatting failure".into()),
            Err(error) => error,
        };
        let (context, _) = preview_failure_parts(&error)?;

        assert_preview_failure_context(context, "preview_format_html")?;
        assert_eq!(context.execution_profile(), Some(&expected_profile));
        assert_eq!(expected_profile.outcome(), QueryExecutionOutcome::Success);
        assert!(!expected_profile.partial());
        Ok(())
    }

    #[tokio::test]
    async fn detailed_preview_attaches_one_success_profile_with_the_exact_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let table = session
            .table_from_sql("select 1 as id union all select 2 as id")
            .await?;
        let capture = TracingCapture::start();

        for limit in [0, 1] {
            let preview = session
                .preview_table_with_options(
                    &table,
                    PreviewOptions::new(limit)
                        .with_execution_profile_mode(ExecutionProfileMode::Detailed),
                )
                .await?;
            let profile = preview
                .execution_profile()
                .ok_or("expected detailed preview profile")?;

            assert_eq!(profile.scope(), QueryExecutionScope::Preview);
            assert_eq!(profile.outcome(), QueryExecutionOutcome::Success);
            assert!(!profile.partial());
            assert_eq!(
                profile.delta_funnel_row_limit(),
                Some(crate::usize_to_u64_saturating(limit))
            );
            assert!(!profile.operators().is_empty());
            assert!(
                preview
                    .phase_timings()
                    .iter()
                    .all(|timing| timing.status() == PhaseStatus::completed())
            );
            let timeline = preview
                .operation_timeline()
                .ok_or("expected detailed preview timeline")?;
            assert_eq!(timeline.status(), TimelineSpanStatus::Completed);
            let phase_spans = timeline
                .spans()
                .iter()
                .filter(|span| span.category() == "delta_funnel.preview.phase")
                .collect::<Vec<_>>();
            assert_eq!(phase_spans.len(), PREVIEW_PHASES.len() - 1);
            for (timing, span) in preview.phase_timings().iter().zip(phase_spans) {
                assert_eq!(timing.elapsed_micros(), Some(span.duration_micros()));
            }
            assert!(
                timeline
                    .spans()
                    .iter()
                    .filter(|span| span.category() == "datafusion.operator.lifecycle")
                    .all(|span| { span.time_semantics() == TimelineSpanTimeSemantics::Lifecycle })
            );
            let trace = preview
                .to_trace_event_json_value()
                .ok_or("expected detailed preview trace")?;
            assert_eq!(
                trace["delta_funnel_timeline"]["total_duration_micros"],
                timeline.total_duration_micros()
            );
            assert_eq!(trace["delta_funnel_profile"]["scope"], "preview");
        }

        assert_eq!(execution_profile_events(capture.captured()).len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn limited_delta_preview_emits_one_successful_terminal_summary()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_two_files("limited-preview-summary")?;
        let mut session =
            DeltaFunnelSession::new(SessionOptions::new().with_query_options(QueryOptions {
                target_partitions: Some(1),
                output_batch_size: Some(1),
            }))?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let capture = TracingCapture::start();

        let preview = session.preview_table(&source, 1).await?;
        let summaries = provider_io_events(capture.captured());

        assert!(preview.text().lines().any(|line| line.contains("| 1  |")));
        assert_eq!(summaries.len(), 1);
        assert_eq!(
            summaries[0].fields.get("source_name").map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            summaries[0].fields.get("outcome").map(String::as_str),
            Some("success")
        );
        Ok(())
    }

    #[tokio::test]
    async fn bounded_preview_projects_where_only_columns_from_multi_partition_delta_scan()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_mixed_timestamp_physical_types(
            "preview-output-projection",
        )?;

        for reader_backend in [
            DeltaProviderReaderBackend::OfficialKernel,
            DeltaProviderReaderBackend::NativeAsync,
        ] {
            let provider_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
                reader_backend,
                2,
                1,
            )?;
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new()
                    .with_query_options(QueryOptions {
                        target_partitions: Some(2),
                        output_batch_size: Some(8),
                    })
                    .with_provider_scan_options(provider_options),
            )?;
            let _source = session.delta_lake(DeltaSourceConfig::new(
                "metrics",
                table.path().to_string_lossy().to_string(),
            ))?;
            let filtered = session
                .table_from_sql(
                    "select event_date, amount from metrics \
                     where id > 0 and customer_name < 'z'",
                )
                .await?;

            let preview = session
                .preview_table_with_options(
                    &filtered,
                    PreviewOptions::new(3)
                        .with_execution_profile_mode(ExecutionProfileMode::Detailed),
                )
                .await?;

            assert!(preview.text().contains("| event_date | amount |"));
            assert!(!preview.text().contains("customer_name"));
            assert!(!preview.text().contains("| id"));
            assert_eq!(
                preview
                    .text()
                    .lines()
                    .filter(|line| line.starts_with("| 2024-"))
                    .count(),
                3
            );
            assert!(
                preview
                    .html()
                    .contains("Showing <b>3</b> rows, <b>2</b> columns.")
            );

            let profile = preview
                .execution_profile()
                .ok_or("expected detailed Delta preview profile")?;
            assert_eq!(profile.delta_funnel_row_limit(), Some(3));
            let snapshot = profile
                .operators()
                .iter()
                .find_map(crate::QueryExecutionOperatorProfile::delta_provider_read_stats)
                .ok_or("expected terminal provider snapshot")?;
            assert_eq!(snapshot.reader_backend, reader_backend);
            assert_eq!(snapshot.scan_partitions_planned, 2);
            assert_eq!(snapshot.files_planned, 2);
            assert_eq!(snapshot.datafusion_output_batch_size, Some(8));
        }
        Ok(())
    }

    #[tokio::test]
    async fn detailed_delta_preview_attaches_the_terminal_provider_snapshot()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_two_files("detailed-preview-profile")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let capture = TracingCapture::start();

        let preview = session
            .preview_table_with_options(
                &source,
                PreviewOptions::new(1).with_execution_profile_mode(ExecutionProfileMode::Detailed),
            )
            .await?;
        let profile = preview
            .execution_profile()
            .ok_or("expected detailed Delta preview profile")?;
        let snapshot = profile
            .operators()
            .iter()
            .find_map(crate::QueryExecutionOperatorProfile::delta_provider_read_stats)
            .ok_or("expected terminal provider snapshot")?;

        assert_eq!(snapshot.source_name, "orders");
        assert!(snapshot.files_planned > 0);
        assert!(snapshot.rows_produced > 0);
        let provider_events = provider_io_events(capture.captured());
        assert_eq!(provider_events.len(), 1);
        assert_provider_io_event_matches_snapshot(&provider_events[0], snapshot, "success");
        assert_eq!(execution_profile_events(capture.captured()).len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn detailed_preview_progress_preserves_profile_and_timing_shape()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_two_files("detailed-progress-parity")?;
        let provider_options = DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
            DeltaProviderReaderBackend::NativeAsync,
            1,
            1,
        )?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new()
                .with_query_options(QueryOptions {
                    target_partitions: Some(1),
                    output_batch_size: Some(1),
                })
                .with_provider_scan_options(provider_options),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let options =
            PreviewOptions::new(1).with_execution_profile_mode(ExecutionProfileMode::Detailed);
        let capture = TracingCapture::start();

        let unreported = session.preview_table_with_options(&source, options).await?;
        assert_eq!(execution_profile_events(capture.captured()).len(), 1);
        assert_eq!(provider_io_events(capture.captured()).len(), 1);
        let (reporter, progress_events) = recording_preview_progress();
        let reported = session
            .preview_table_with_options_and_progress(&source, options, reporter)
            .await?;

        assert_eq!(reported.text(), unreported.text());
        assert_eq!(reported.html(), unreported.html());
        assert_eq!(
            reported.phase_timings().len(),
            unreported.phase_timings().len()
        );
        for (reported, unreported) in reported
            .phase_timings()
            .iter()
            .zip(unreported.phase_timings())
        {
            assert_eq!(reported.phase_name(), unreported.phase_name());
            assert_eq!(reported.status(), unreported.status());
            assert_eq!(
                reported.elapsed_micros().is_some(),
                unreported.elapsed_micros().is_some()
            );
        }

        let reported_profile = reported
            .execution_profile()
            .ok_or("expected reported detailed preview profile")?;
        let unreported_profile = unreported
            .execution_profile()
            .ok_or("expected unreported detailed preview profile")?;
        assert_eq!(reported_profile.scope(), unreported_profile.scope());
        assert_eq!(reported_profile.outcome(), unreported_profile.outcome());
        assert_eq!(reported_profile.partial(), unreported_profile.partial());
        assert_eq!(
            reported_profile.delta_funnel_row_limit(),
            unreported_profile.delta_funnel_row_limit()
        );
        assert_eq!(
            reported_profile.operators().len(),
            unreported_profile.operators().len()
        );
        for (reported, unreported) in reported_profile
            .operators()
            .iter()
            .zip(unreported_profile.operators())
        {
            assert_eq!(reported.node_id(), unreported.node_id());
            assert_eq!(reported.parent_node_id(), unreported.parent_node_id());
            assert_eq!(reported.operator_name(), unreported.operator_name());
            assert_eq!(
                reported.output_partition_count(),
                unreported.output_partition_count()
            );
            assert_eq!(reported.metrics_available(), unreported.metrics_available());
            assert_eq!(
                reported.delta_provider_read_stats(),
                unreported.delta_provider_read_stats()
            );
        }

        assert_eq!(execution_profile_events(capture.captured()).len(), 2);
        assert_eq!(provider_io_events(capture.captured()).len(), 2);
        let progress_events = progress_events
            .lock()
            .map_err(|_| "preview event lock poisoned")?;
        assert!(progress_events.iter().any(|event| {
            event.0 == ProgressEventKind::PhaseChanged
                && event.2 == Some(ProgressPhase::FormattingPreview)
        }));
        Ok(())
    }

    #[tokio::test]
    async fn preview_progress_follows_existing_preview_boundaries()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let table = session.table_from_sql("select 1 as id").await?;
        let (reporter, events) = recording_preview_progress();

        let preview = session
            .preview_table_with_progress(&table, 1, reporter)
            .await?;

        assert!(preview.text().contains("| 1  |"));
        let events = events.lock().map_err(|_| "preview event lock poisoned")?;
        assert_eq!(
            events.as_slice(),
            [
                (
                    ProgressEventKind::Started,
                    Some(ProgressOperation::PreviewTable),
                    None,
                ),
                (
                    ProgressEventKind::PhaseChanged,
                    None,
                    Some(ProgressPhase::PreparingPreview),
                ),
                (
                    ProgressEventKind::PhaseChanged,
                    None,
                    Some(ProgressPhase::CollectingPreview),
                ),
                (
                    ProgressEventKind::PhaseChanged,
                    None,
                    Some(ProgressPhase::FormattingPreview),
                ),
                (ProgressEventKind::Completed, None, None),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn preview_progress_executes_the_bounded_plan_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (provider, scans) = scan_counting_marker_region_provider("observed")?;
        session
            .context()
            .register_table("preview_source", provider)?;
        let udf_calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = Arc::clone(&udf_calls);
        session.context().register_udf(create_udf(
            "observe_preview_execution",
            vec![DataType::Utf8],
            DataType::Utf8,
            Volatility::Volatile,
            Arc::new(move |arguments| {
                callback_calls.fetch_add(1, Ordering::SeqCst);
                Ok(arguments[0].clone())
            }),
        ));
        let table = session
            .table_from_sql(
                "select observe_preview_execution(marker) as marker from preview_source",
            )
            .await?;
        let (reporter, _events) = recording_preview_progress();

        let preview = session
            .preview_table_with_progress(&table, 1, reporter)
            .await?;

        assert!(preview.text().contains("observed"));
        assert_eq!(scans.load(Ordering::SeqCst), 1);
        assert_eq!(udf_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn preview_releases_the_execution_plan_before_formatting()
    -> Result<(), Box<dyn std::error::Error>> {
        for mode in [
            ExecutionProfileMode::Disabled,
            ExecutionProfileMode::Detailed,
        ] {
            let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
            let (provider, last_plan_marker) =
                plan_lifetime_tracking_marker_region_provider("observed")?;
            session
                .context()
                .register_table("preview_source", provider)?;
            let table = session
                .table_from_sql("select marker from preview_source")
                .await?;
            let formatting_observed = Arc::new(AtomicBool::new(false));
            let plan_released = Arc::new(AtomicBool::new(false));
            let callback_formatting_observed = Arc::clone(&formatting_observed);
            let callback_plan_released = Arc::clone(&plan_released);
            let reporter = ProgressReporter::new(move |event| {
                if event.phase() != Some(ProgressPhase::FormattingPreview) {
                    return;
                }
                callback_formatting_observed.store(true, Ordering::SeqCst);
                let released = match last_plan_marker.lock() {
                    Ok(last_plan_marker) => last_plan_marker
                        .as_ref()
                        .is_some_and(|marker| marker.upgrade().is_none()),
                    Err(poisoned) => poisoned
                        .into_inner()
                        .as_ref()
                        .is_some_and(|marker| marker.upgrade().is_none()),
                };
                callback_plan_released.store(released, Ordering::SeqCst);
            });

            let preview = session
                .preview_table_with_options_and_progress(
                    &table,
                    PreviewOptions::new(1).with_execution_profile_mode(mode),
                    reporter,
                )
                .await?;

            assert!(preview.text().contains("observed"));
            assert!(formatting_observed.load(Ordering::SeqCst));
            assert!(
                plan_released.load(Ordering::SeqCst),
                "{mode:?} preview retained its execution plan during formatting"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn dropping_detailed_preview_after_stream_setup_emits_one_cancelled_profile()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (provider, execution_refs) = blocking_marker_provider();
        session
            .context()
            .register_table("blocking_source", provider)?;
        let table = session
            .table_from_sql("select marker from blocking_source")
            .await?;
        let capture = TracingCapture::start();

        {
            let preview = session.preview_table_with_options(
                &table,
                PreviewOptions::new(1).with_execution_profile_mode(ExecutionProfileMode::Detailed),
            );
            tokio::pin!(preview);
            let completed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    if execution_refs.strong_count() > 1 {
                        return None;
                    }
                    tokio::select! {
                        result = &mut preview => return Some(result),
                        () = tokio::task::yield_now() => {}
                    }
                }
            })
            .await
            .map_err(|_| "preview stream setup timed out")?;
            if let Some(result) = completed {
                return Err(format!("preview completed before cancellation: {result:?}").into());
            }
        }

        let events = execution_profile_events(capture.captured());
        assert_eq!(events.len(), 1);
        for (field, value) in [
            ("scope", "preview"),
            ("outcome", "cancelled"),
            ("partial", "true"),
            ("delta_funnel_row_limit", "1"),
        ] {
            assert_eq!(events[0].fields.get(field).map(String::as_str), Some(value));
        }
        assert!(provider_io_events(capture.captured()).is_empty());
        tokio::task::yield_now().await;
        assert_eq!(execution_profile_events(capture.captured()).len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn preview_progress_keeps_physical_planning_in_the_preparing_phase_on_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (provider, scans) = failing_scan_marker_region_provider();
        session
            .context()
            .register_table("broken_source", provider)?;
        let table = session
            .table_from_sql("select marker from broken_source")
            .await?;
        let (reporter, events) = recording_preview_progress();

        let result = session
            .preview_table_with_options_and_progress(
                &table,
                PreviewOptions::new(1).with_execution_profile_mode(ExecutionProfileMode::Detailed),
                reporter,
            )
            .await;
        let error = match result {
            Ok(_) => return Err("expected physical planning failure".into()),
            Err(error) => error,
        };
        let (context, _) = preview_failure_parts(&error)?;

        assert_preview_failure_context(context, "preview_physical_planning")?;
        assert_eq!(context.execution_profile(), None);
        assert_eq!(scans.load(Ordering::SeqCst), 1);
        let events = events.lock().map_err(|_| "preview event lock poisoned")?;
        assert_eq!(
            events.as_slice(),
            [
                (
                    ProgressEventKind::Started,
                    Some(ProgressOperation::PreviewTable),
                    None,
                ),
                (
                    ProgressEventKind::PhaseChanged,
                    None,
                    Some(ProgressPhase::PreparingPreview),
                ),
                (ProgressEventKind::Failed, None, None),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn detailed_preview_stream_setup_failure_attaches_an_error_profile()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session.context().register_table(
            "setup_failure_source",
            stream_setup_failing_marker_region_provider()?,
        )?;
        let table = session
            .table_from_sql("select marker from setup_failure_source")
            .await?;
        let capture = TracingCapture::start();

        let result = session
            .preview_table_with_options(
                &table,
                PreviewOptions::new(1).with_execution_profile_mode(ExecutionProfileMode::Detailed),
            )
            .await;
        let error = match result {
            Ok(_) => return Err("expected stream setup failure".into()),
            Err(error) => error,
        };
        let (context, _) = preview_failure_parts(&error)?;
        let profile = context
            .execution_profile()
            .ok_or("expected stream setup failure profile")?;

        assert_preview_failure_context(context, "preview_stream_setup")?;
        assert_eq!(profile.outcome(), QueryExecutionOutcome::Error);
        assert!(profile.partial());
        assert!(
            profile
                .operators()
                .iter()
                .any(|operator| operator.operator_name() == "StreamSetupFailingPlan")
        );
        assert_eq!(execution_profile_events(capture.captured()).len(), 1);
        assert!(provider_io_events(capture.captured()).is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn preview_progress_stops_before_formatting_when_execution_fails()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let table = session
            .table_from_sql("select cast(1 as bigint) / cast(0 as bigint) as value")
            .await?;
        let (reporter, events) = recording_preview_progress();
        let capture = TracingCapture::start();

        let result = session
            .preview_table_with_options_and_progress(
                &table,
                PreviewOptions::new(1).with_execution_profile_mode(ExecutionProfileMode::Detailed),
                reporter,
            )
            .await;
        let error = match result {
            Ok(_) => return Err("expected preview execution failure".into()),
            Err(error) => error,
        };
        let (context, _) = preview_failure_parts(&error)?;
        let profile = context
            .execution_profile()
            .ok_or("expected execution failure profile")?;

        assert_preview_failure_context(context, "preview_execute_collect")?;
        assert_eq!(profile.outcome(), QueryExecutionOutcome::Error);
        assert!(profile.partial());
        assert_eq!(execution_profile_events(capture.captured()).len(), 1);
        let events = events.lock().map_err(|_| "preview event lock poisoned")?;
        assert_eq!(
            events.as_slice(),
            [
                (
                    ProgressEventKind::Started,
                    Some(ProgressOperation::PreviewTable),
                    None,
                ),
                (
                    ProgressEventKind::PhaseChanged,
                    None,
                    Some(ProgressPhase::PreparingPreview),
                ),
                (
                    ProgressEventKind::PhaseChanged,
                    None,
                    Some(ProgressPhase::CollectingPreview),
                ),
                (ProgressEventKind::Failed, None, None),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn preview_file_progress_uses_the_limited_physical_plan()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_two_files("preview-progress")?;
        let mut session =
            DeltaFunnelSession::new(SessionOptions::new().with_query_options(QueryOptions {
                target_partitions: Some(1),
                output_batch_size: Some(1),
            }))?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;

        let (reporter, full_events) = recording_preview_file_progress();
        let full = session
            .preview_table_with_progress(&source, 20, reporter)
            .await?;
        assert!(full.text().lines().any(|line| line.contains("| 4  |")));
        assert_eq!(
            full_events
                .lock()
                .map_err(|_| "full preview event lock poisoned")?
                .last(),
            Some(&(2, 2))
        );

        let (reporter, limited_events) = recording_preview_file_progress();
        session
            .preview_table_with_progress(&source, 1, reporter)
            .await?;
        {
            let limited_events = limited_events
                .lock()
                .map_err(|_| "limited preview event lock poisoned")?;
            let (handled, total) = limited_events
                .last()
                .copied()
                .ok_or("limited preview did not report file progress")?;
            assert_eq!(total, 2);
            assert!(handled < total);
        }

        let (reporter, zero_events) = recording_preview_file_progress();
        session
            .preview_table_with_progress(&source, 0, reporter)
            .await?;
        assert!(
            zero_events
                .lock()
                .map_err(|_| "zero preview event lock poisoned")?
                .is_empty()
        );

        let derived = session.table_from_sql("select 1 as id").await?;
        let (reporter, derived_events) = recording_preview_file_progress();
        session
            .preview_table_with_progress(&derived, 1, reporter)
            .await?;
        assert!(
            derived_events
                .lock()
                .map_err(|_| "derived preview event lock poisoned")?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn delta_preview_emits_no_file_event_after_its_single_terminal_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_with_two_files("preview-terminal-order")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let (reporter, events) = recording_preview_progress();

        session
            .preview_table_with_progress(&source, 20, reporter)
            .await?;

        let events = events.lock().map_err(|_| "preview event lock poisoned")?;
        assert!(events.iter().any(|event| {
            event.0 == ProgressEventKind::Progress
                && event.2 == Some(ProgressPhase::CollectingPreview)
        }));
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    matches!(
                        event.0,
                        ProgressEventKind::Completed
                            | ProgressEventKind::CompletedWithFailures
                            | ProgressEventKind::Failed
                            | ProgressEventKind::Cancelled
                    )
                })
                .count(),
            1
        );
        assert_eq!(
            events.last(),
            Some(&(ProgressEventKind::Completed, None, None))
        );
        Ok(())
    }

    #[tokio::test]
    async fn preview_table_reads_registered_derived_alias() -> Result<(), DeltaFunnelError> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let pending = session.table_from_sql("select 'west' as region").await?;
        let alias = session.register_alias("regions", &pending)?;

        let preview = session.preview_table(&alias, 20).await?;

        assert!(preview.text().contains("| region |"));
        assert!(
            preview
                .text()
                .lines()
                .any(|line| line.contains("| west   |"))
        );
        assert!(preview.html().contains("<td>west</td>"));
        Ok(())
    }

    #[tokio::test]
    async fn preview_table_html_escapes_cell_values() -> Result<(), DeltaFunnelError> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let table = session.table_from_sql("select '<tag>' as marker").await?;

        let preview = session.preview_table(&table, 20).await?;

        assert!(preview.html().contains("&lt;tag&gt;"));
        assert!(!preview.html().contains("<td><tag></td>"));
        Ok(())
    }

    #[tokio::test]
    async fn batch_stream_for_lazy_table_rejects_unknown_table_before_execution()
    -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;

        let error = session
            .batch_stream_for_lazy_table(
                &LazyTable::placeholder(42, LazyTableKind::DeltaSource),
                None,
            )
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("not registered in this session")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_guard_accepts_registered_derived_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let derived = session.table_from_sql("select 1 as id").await?;
        let alias = session.register_alias("cached_candidate", &derived)?;

        let registered = session.registered_derived_for_scoped_cache_alias(&alias)?;

        assert_eq!(registered.table(), &alias);
        assert_eq!(registered.name(), "cached_candidate");
        Ok(())
    }

    #[test]
    fn scoped_cache_alias_guard_rejects_raw_source_before_catalog_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let error = session.registered_derived_for_scoped_cache_alias(&source);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("not registered in this session")
        ));
        assert!(session.registered_source("orders").is_some());
        Ok(())
    }

    #[tokio::test]
    async fn scoped_cache_alias_guard_rejects_pending_derived_before_catalog_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let pending = session.table_from_sql("select 1 as id").await?;

        let error = session.registered_derived_for_scoped_cache_alias(&pending);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("not registered in this session")
        ));
        assert!(session.derived_tables().is_empty());
        Ok(())
    }

    #[test]
    fn scoped_cache_alias_guard_rejects_unknown_derived_handle() -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(SessionOptions::default())?;
        let unknown = LazyTable::placeholder(252, LazyTableKind::DerivedSql);

        let error = session.registered_derived_for_scoped_cache_alias(&unknown);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("not registered in this session")
        ));
        Ok(())
    }
    #[tokio::test]
    async fn cached_alias_replacement_does_not_feed_existing_downstream_derived_tables()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        session
            .context()
            .register_table("big_source", marker_region_provider("original")?)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;

        let replacement = session
            .context()
            .read_table(marker_region_provider("replacement")?)?
            .cache()
            .await?
            .into_view();
        let removed_big = session.context().deregister_table("big")?;
        assert!(removed_big.is_some());
        session.context().register_table("big", replacement)?;

        let direct_big = session
            .context()
            .sql("select marker from big where region = 'west'")
            .await?
            .collect()
            .await?;
        assert_eq!(
            marker_values_from_batches(&direct_big)?,
            vec!["replacement"]
        );

        let west_stream = session.batch_stream_for_lazy_table(&west, None).await?;
        let east_stream = session.batch_stream_for_lazy_table(&east, None).await?;
        let west_markers = collect_stream_marker_values(west_stream).await?;
        let east_markers = collect_stream_marker_values(east_stream).await?;

        // Conclusion for #245: existing downstream ViewTable providers keep the
        // original resolved provider; catalog replacement alone does not rewire them.
        assert_eq!(west_markers, vec!["original"]);
        assert_eq!(east_markers, vec!["original"]);
        Ok(())
    }

    #[tokio::test]
    async fn replanned_downstream_sql_uses_cached_alias_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        const WEST_SQL: &str = "select marker from big where region = 'west'";
        const EAST_SQL: &str = "select marker from big where region = 'east'";

        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let _big = session.register_alias("big", &pending_big)?;
        let _old_west = session.table_from_sql(WEST_SQL).await?;
        let _old_east = session.table_from_sql(EAST_SQL).await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);

        let cached_big = session
            .context()
            .table("big")
            .await?
            .cache()
            .await?
            .into_view();
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let removed_big = session.context().deregister_table("big")?;
        assert!(removed_big.is_some());
        session.context().register_table("big", cached_big)?;

        let direct_big = session.context().sql(WEST_SQL).await?.collect().await?;
        assert_eq!(marker_values_from_batches(&direct_big)?, vec!["shared"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let replanned_west = session.table_from_sql(WEST_SQL).await?;
        let replanned_east = session.table_from_sql(EAST_SQL).await?;
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);

        let west_stream = session
            .batch_stream_for_lazy_table(&replanned_west, None)
            .await?;
        let east_stream = session
            .batch_stream_for_lazy_table(&replanned_east, None)
            .await?;
        let west_markers = collect_stream_marker_values(west_stream).await?;
        let east_markers = collect_stream_marker_values(east_stream).await?;

        // Conclusion for #247: after cached big is installed under alias big,
        // replanning downstream SQL reads the cached provider and does not
        // rescan the original upstream provider per output.
        assert_eq!(west_markers, vec!["shared"]);
        assert_eq!(east_markers, vec!["shared"]);
        assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
