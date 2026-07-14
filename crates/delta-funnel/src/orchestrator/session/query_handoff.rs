use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use datafusion::{
    arrow::{
        datatypes::{DataType, SchemaRef},
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

use crate::{
    DeltaFunnelError, MssqlOutputBatchStream, MssqlOutputBatchStreamFactory,
    datafusion_query_output_stream,
    observability::{DeltaProviderScanOutcome, delta_provider_parquet_io_summary},
    progress::{ProgressEvent, ProgressOperation, ProgressPhase, ProgressReporter},
    query_engine::datafusion::{
        DeltaProviderReadStatsHandle, collect_delta_provider_read_stats_handles,
        snapshot_delta_provider_read_stats,
    },
};

use super::{
    DeltaFunnelSession, LazyTable, LazyTableKind, PendingDerivedTable, RegisteredDerivedTable,
    RegisteredSessionSource, TablePreview,
    errors::{datafusion_handoff_setup_error, unknown_lazy_table_error},
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

/// Finalizes every retained Delta scan from one point-in-time snapshot set.
pub(super) fn finalize_provider_scan_execution(
    read_stats_handles: &[DeltaProviderReadStatsHandle],
    provider_stats_snapshots: Option<&SharedProviderStatsSnapshots>,
    outcome: DeltaProviderScanOutcome,
) {
    let snapshots = snapshot_delta_provider_read_stats(read_stats_handles);
    if snapshots.is_empty() {
        return;
    }

    if let Some(provider_stats_snapshots) = provider_stats_snapshots {
        match provider_stats_snapshots.lock() {
            Ok(mut retained) => retained.extend(snapshots.iter().cloned()),
            Err(poisoned) => poisoned.into_inner().extend(snapshots.iter().cloned()),
        }
    }
    for snapshot in &snapshots {
        delta_provider_parquet_io_summary(snapshot, outcome);
    }
}

/// Finalizes one merged Delta provider execution when its batch stream stops.
///
/// The stream keeps only shared read counters and snapshots them when it ends,
/// fails, or is dropped by its downstream consumer.
struct DeltaProviderScanTerminalStream {
    inner: MssqlOutputBatchStream,
    // Taking these handles records the single terminal transition and releases
    // the counters as soon as that transition completes.
    read_stats_handles: Option<Vec<DeltaProviderReadStatsHandle>>,
    provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
}

impl DeltaProviderScanTerminalStream {
    fn new(
        inner: MssqlOutputBatchStream,
        read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
        provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
    ) -> Self {
        Self {
            inner,
            read_stats_handles: Some(read_stats_handles),
            provider_stats_snapshots,
        }
    }

    fn finalize_if_needed(&mut self, outcome: DeltaProviderScanOutcome) {
        let Some(read_stats_handles) = self.read_stats_handles.take() else {
            return;
        };
        let provider_stats_snapshots = self.provider_stats_snapshots.take();
        finalize_provider_scan_execution(
            &read_stats_handles,
            provider_stats_snapshots.as_ref(),
            outcome,
        );
    }
}

impl Stream for DeltaProviderScanTerminalStream {
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

impl Drop for DeltaProviderScanTerminalStream {
    fn drop(&mut self) {
        self.finalize_if_needed(DeltaProviderScanOutcome::Cancelled);
    }
}

/// Coordinates one terminal outcome across all partitions of one execution.
struct PartitionScanCoordinator {
    state: Mutex<PartitionScanState>,
}

struct PartitionScanState {
    // Finalization waits until every returned partition stream is terminal.
    remaining_streams: usize,
    // Each terminal result can only strengthen success -> cancelled -> error.
    outcome: DeltaProviderScanOutcome,
    // The last terminal stream takes these handles and releases them after use.
    read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
}

impl PartitionScanCoordinator {
    fn new(read_stats_handles: Vec<DeltaProviderReadStatsHandle>, stream_count: usize) -> Self {
        Self {
            state: Mutex::new(PartitionScanState {
                remaining_streams: stream_count,
                outcome: DeltaProviderScanOutcome::Success,
                read_stats_handles,
            }),
        }
    }

    fn record_stream_terminal(&self, outcome: DeltaProviderScanOutcome) {
        let finalization = match self.state.lock() {
            Ok(mut state) => state.record_stream_terminal(outcome),
            Err(poisoned) => poisoned.into_inner().record_stream_terminal(outcome),
        };
        if let Some((read_stats_handles, outcome)) = finalization {
            finalize_provider_scan_execution(&read_stats_handles, None, outcome);
        }
    }
}

impl PartitionScanState {
    fn record_stream_terminal(
        &mut self,
        outcome: DeltaProviderScanOutcome,
    ) -> Option<(Vec<DeltaProviderReadStatsHandle>, DeltaProviderScanOutcome)> {
        if self.remaining_streams == 0 {
            return None;
        }

        self.outcome = strongest_provider_scan_outcome(self.outcome, outcome);
        self.remaining_streams -= 1;
        if self.remaining_streams != 0 {
            return None;
        }

        Some((std::mem::take(&mut self.read_stats_handles), self.outcome))
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
struct PartitionScanStream {
    inner: MssqlOutputBatchStream,
    coordinator: Option<Arc<PartitionScanCoordinator>>,
}

impl PartitionScanStream {
    fn record_terminal_once(&mut self, outcome: DeltaProviderScanOutcome) {
        if let Some(coordinator) = self.coordinator.take() {
            coordinator.record_stream_terminal(outcome);
        }
    }
}

impl Stream for PartitionScanStream {
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

impl Drop for PartitionScanStream {
    fn drop(&mut self) {
        self.record_terminal_once(DeltaProviderScanOutcome::Cancelled);
    }
}

/// Adds one shared terminal outcome tracker to a partitioned execution.
pub(super) fn track_partitioned_scan_completion(
    streams: Vec<MssqlOutputBatchStream>,
    read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
) -> Vec<MssqlOutputBatchStream> {
    if read_stats_handles.is_empty() {
        return streams;
    }
    if streams.is_empty() {
        finalize_provider_scan_execution(
            &read_stats_handles,
            None,
            DeltaProviderScanOutcome::Success,
        );
        return streams;
    }

    let coordinator = Arc::new(PartitionScanCoordinator::new(
        read_stats_handles,
        streams.len(),
    ));
    streams
        .into_iter()
        .map(|inner| {
            Box::pin(PartitionScanStream {
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

/// Finalizes one merged execution when its stream ends, fails, or is dropped.
fn track_delta_provider_scan_completion(
    stream: MssqlOutputBatchStream,
    read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
    provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
) -> MssqlOutputBatchStream {
    if read_stats_handles.is_empty() {
        return stream;
    }
    Box::pin(DeltaProviderScanTerminalStream::new(
        stream,
        read_stats_handles,
        provider_stats_snapshots,
    ))
}

/// Adds optional live file progress and terminal provider scan tracking to one
/// physical-plan stream.
///
/// Both layers reuse the same live Delta scan counters. They do not execute
/// another query or retain the physical plan. `progress` contains the reporter
/// and output name used for live events. Callers collect the handles before
/// merged stream construction so setup failures can snapshot the same handles.
pub(super) fn wrap_stream_with_delta_read_tracking(
    mut stream: MssqlOutputBatchStream,
    read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
    provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
    progress: Option<(ProgressReporter, String)>,
) -> MssqlOutputBatchStream {
    if read_stats_handles.is_empty() {
        return stream;
    }

    if let Some((reporter, output_name)) = progress {
        let sampler = DeltaFileProgressSampler::new(
            read_stats_handles.clone(),
            reporter,
            ProgressPhase::Writing,
            Some(output_name),
        );
        stream = track_delta_file_progress(stream, sampler);
    }
    track_delta_provider_scan_completion(stream, read_stats_handles, provider_stats_snapshots)
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

    batch_stream_for_physical_plan(context, physical_plan, provider_stats_snapshots, progress)
}

fn batch_stream_for_physical_plan(
    context: &SessionContext,
    physical_plan: Arc<dyn ExecutionPlan>,
    provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
    progress: Option<(ProgressReporter, String)>,
) -> Result<MssqlOutputBatchStream, DeltaFunnelError> {
    // These handles must exist before merged stream setup can fail.
    let read_stats_handles = collect_delta_provider_read_stats_handles(physical_plan.as_ref());
    let stream = match datafusion_query_output_stream(physical_plan, context.task_ctx()) {
        Ok(stream) => stream,
        Err(error) => {
            finalize_provider_scan_execution(
                &read_stats_handles,
                provider_stats_snapshots.as_ref(),
                DeltaProviderScanOutcome::Error,
            );
            return Err(datafusion_handoff_setup_error("query_output_stream", error));
        }
    };
    let stream = Box::pin(stream.map(|batch| {
        batch.map_err(|error| datafusion_handoff_setup_error("query_output_stream", error))
    }));

    Ok(wrap_stream_with_delta_read_tracking(
        stream,
        read_stats_handles,
        provider_stats_snapshots,
        progress,
    ))
}

impl DeltaFunnelSession {
    /// Builds a batch stream and optionally reports Delta file progress while
    /// that stream is consumed.
    ///
    /// `progress` contains the reporter and output name used for emitted
    /// events. `None` returns the normal stream without progress sampling.
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
        self.build_preview(table, limit, None).await
    }

    /// Executes the same bounded preview while reporting its live lifecycle.
    pub(crate) async fn preview_table_with_progress(
        &self,
        table: &LazyTable,
        limit: usize,
        reporter: ProgressReporter,
    ) -> Result<TablePreview, DeltaFunnelError> {
        reporter.emit(&ProgressEvent::started(ProgressOperation::PreviewTable));
        let result = self.build_preview(table, limit, Some(&reporter)).await;
        reporter.emit(&if result.is_ok() {
            ProgressEvent::completed()
        } else {
            ProgressEvent::failed()
        });
        result
    }

    /// Runs the existing preview steps and optionally announces each boundary.
    async fn build_preview(
        &self,
        table: &LazyTable,
        limit: usize,
        reporter: Option<&ProgressReporter>,
    ) -> Result<TablePreview, DeltaFunnelError> {
        emit_preview_phase(reporter, ProgressPhase::PreparingPreview);
        let dataframe = self.dataframe_for_lazy_table(table).await?;
        let schema = Arc::new(dataframe.schema().as_arrow().clone());
        let dataframe = dataframe
            .limit(0, Some(limit))
            .map_err(|error| datafusion_handoff_setup_error("preview_limit", error))?;
        let task_context = Arc::new(dataframe.task_ctx());
        let physical_plan = dataframe
            .create_physical_plan()
            .await
            .map_err(|error| datafusion_handoff_setup_error("preview_collect", error))?;
        let read_stats_handles = collect_delta_provider_read_stats_handles(physical_plan.as_ref());
        emit_preview_phase(reporter, ProgressPhase::CollectingPreview);
        let stream = match datafusion_query_output_stream(physical_plan, task_context) {
            Ok(stream) => stream,
            Err(error) => {
                finalize_provider_scan_execution(
                    &read_stats_handles,
                    None,
                    DeltaProviderScanOutcome::Error,
                );
                return Err(datafusion_handoff_setup_error("preview_collect", error));
            }
        };
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
        let stream = track_delta_provider_scan_completion(stream, read_stats_handles, None);
        let batches = stream.try_collect::<Vec<_>>().await?;
        emit_preview_phase(reporter, ProgressPhase::FormattingPreview);
        let text = pretty_format_batches(&batches)
            .map_err(|error| datafusion_handoff_setup_error("preview_text", error))?
            .to_string();
        let html = preview_batches_to_html(&schema, &batches)
            .map_err(|error| datafusion_handoff_setup_error("preview_html", error))?;

        Ok(TablePreview::new(text, html))
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

fn preview_batches_to_html(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<String, datafusion::arrow::error::ArrowError> {
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

async fn dataframe_for_lazy_table_from_session_parts(
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
        atomic::{AtomicUsize, Ordering},
    };

    use crate::{
        DeltaFunnelError, DeltaSourceConfig, QueryOptions,
        observability::test_capture::{CapturedEvent, CapturedEvents, TracingCapture},
        progress::{ProgressEventKind, ProgressOperation, ProgressPhase, ProgressReporter},
        table_formats::RealParquetDeltaTable,
    };
    use datafusion::{
        arrow::{
            datatypes::{DataType, Schema},
            record_batch::RecordBatch,
        },
        logical_expr::{Volatility, create_udf},
        physical_plan::{ExecutionPlan, union::UnionExec},
    };
    use futures_util::StreamExt;
    use tracing::Level;

    use super::super::{
        DeltaFunnelSession, LazyTable, LazyTableKind, SessionOptions,
        test_support::{
            DeltaLogTable, StreamSetupFailingPlan, collect_stream_marker_values,
            collect_stream_row_count, failing_scan_marker_region_provider, marker_region_provider,
            marker_values_from_batches, scan_counting_marker_region_provider,
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

    fn partition_stream(
        coordinator: &Arc<super::PartitionScanCoordinator>,
        batches: Vec<Result<RecordBatch, DeltaFunnelError>>,
    ) -> crate::MssqlOutputBatchStream {
        Box::pin(super::PartitionScanStream {
            inner: Box::pin(futures_util::stream::iter(batches)),
            coordinator: Some(Arc::clone(coordinator)),
        })
    }

    fn partitioned_coordinator_state(
        coordinator: &super::PartitionScanCoordinator,
    ) -> (usize, crate::observability::DeltaProviderScanOutcome) {
        match coordinator.state.lock() {
            Ok(state) => (state.remaining_streams, state.outcome),
            Err(poisoned) => {
                let state = poisoned.into_inner();
                (state.remaining_streams, state.outcome)
            }
        }
    }

    #[tokio::test]
    async fn partitioned_terminal_stream_finishes_once_after_repeated_eof_and_drop() {
        use crate::observability::DeltaProviderScanOutcome;

        let coordinator = Arc::new(super::PartitionScanCoordinator::new(Vec::new(), 2));
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let mut first = partition_stream(&coordinator, vec![Ok(batch)]);
        let mut second = partition_stream(&coordinator, Vec::new());

        assert!(first.next().await.is_some_and(|batch| batch.is_ok()));
        assert!(first.next().await.is_none());
        assert!(first.next().await.is_none());
        drop(first);
        assert_eq!(
            partitioned_coordinator_state(&coordinator),
            (1, DeltaProviderScanOutcome::Success)
        );

        assert!(second.next().await.is_none());
        drop(second);
        assert_eq!(
            partitioned_coordinator_state(&coordinator),
            (0, DeltaProviderScanOutcome::Success)
        );
    }

    #[tokio::test]
    async fn partitioned_terminal_stream_keeps_error_when_cleanup_drops_other_streams() {
        use crate::observability::DeltaProviderScanOutcome;

        let coordinator = Arc::new(super::PartitionScanCoordinator::new(Vec::new(), 2));
        let mut errored = partition_stream(
            &coordinator,
            vec![Err(DeltaFunnelError::Config {
                message: "injected partition failure".to_owned(),
            })],
        );
        let unconsumed = partition_stream(&coordinator, Vec::new());

        assert!(errored.next().await.is_some_and(|batch| batch.is_err()));
        drop(errored);
        assert_eq!(
            partitioned_coordinator_state(&coordinator),
            (1, DeltaProviderScanOutcome::Error)
        );

        drop(unconsumed);
        assert_eq!(
            partitioned_coordinator_state(&coordinator),
            (0, DeltaProviderScanOutcome::Error)
        );
    }

    #[tokio::test]
    async fn partitioned_terminal_stream_reports_unconsumed_stream_as_cancelled() {
        use crate::observability::DeltaProviderScanOutcome;

        let coordinator = Arc::new(super::PartitionScanCoordinator::new(Vec::new(), 2));
        let mut completed = partition_stream(&coordinator, Vec::new());
        let unconsumed = partition_stream(&coordinator, Vec::new());

        assert!(completed.next().await.is_none());
        drop(completed);
        drop(unconsumed);

        assert_eq!(
            partitioned_coordinator_state(&coordinator),
            (0, DeltaProviderScanOutcome::Cancelled)
        );
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
        let mut stream = super::DeltaProviderScanTerminalStream::new(
            inner,
            handles,
            Some(Arc::clone(&retained)),
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
    async fn final_provider_stats_are_recorded_once_after_eof_and_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("final-stats-eof")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let shared_provider_stats = super::shared_provider_stats_snapshots();
        let stream = report_tracked_stream(&session, &source, &shared_provider_stats).await?;

        let rows = collect_stream_row_count(stream).await?;
        let snapshots = super::provider_stats_snapshots(&shared_provider_stats);

        assert_eq!(rows, table.rows());
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].files_completed, 1);
        assert!(
            snapshots[0]
                .parquet_data_file_bytes_received
                .is_some_and(|bytes| bytes > 0)
        );
        Ok(())
    }

    #[tokio::test]
    async fn dropping_downstream_stream_records_one_partial_provider_snapshot()
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
        let mut stream = report_tracked_stream(&session, &source, &shared_provider_stats).await?;

        assert!(stream.next().await.transpose()?.is_some());
        drop(stream);
        let snapshots = super::provider_stats_snapshots(&shared_provider_stats);

        assert_eq!(snapshots.len(), 1);
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
    async fn upstream_error_then_drop_records_provider_stats_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("final-stats-upstream-error")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let shared_provider_stats = super::shared_provider_stats_snapshots();
        let mut stream = report_tracked_stream(&session, &source, &shared_provider_stats).await?;

        let result = stream.next().await.ok_or("expected upstream error")?;
        assert!(result.is_err());
        drop(stream);
        let snapshots = super::provider_stats_snapshots(&shared_provider_stats);

        assert_eq!(snapshots.len(), 1);
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
        );
        let snapshots = super::provider_stats_snapshots(&shared_provider_stats);
        let summaries = provider_io_events(capture.captured());

        assert!(result.is_err());
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].source_name, "orders");
        assert_eq!(snapshots[0].files_started, 0);
        assert_eq!(snapshots[0].parquet_data_file_range_get_operations, Some(0));
        assert_eq!(snapshots[0].parquet_data_file_bytes_received, Some(0));
        assert_eq!(snapshots[0].parquet_data_file_opened_bytes, Some(0));
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].target, "delta_funnel");
        assert_eq!(summaries[0].level, Level::DEBUG);
        assert_eq!(
            summaries[0].fields.get("source_name").map(String::as_str),
            Some(snapshots[0].source_name.as_str())
        );
        assert_eq!(
            summaries[0].fields.get("outcome").map(String::as_str),
            Some("error")
        );
        for (field, expected) in PARQUET_IO_FIELDS.iter().zip([
            snapshots[0].parquet_data_file_range_get_operations,
            snapshots[0].parquet_data_file_full_get_operations,
            snapshots[0].parquet_data_file_bytes_received,
            snapshots[0].parquet_data_file_opened_bytes,
        ]) {
            assert_eq!(
                summaries[0]
                    .fields
                    .get(*field)
                    .and_then(|value| value.parse::<u64>().ok()),
                expected
            );
        }
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
        let streams = super::track_partitioned_scan_completion(Vec::new(), handles);
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
        let stream =
            super::batch_stream_for_physical_plan(&session.context, combined_plan, None, None)?;
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

        let result = report_tracked_stream(&session, &table, &shared_provider_stats).await;

        assert!(result.is_err());
        assert_eq!(scans.load(Ordering::SeqCst), 1);
        assert!(super::provider_stats_snapshots(&shared_provider_stats).is_empty());
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
    async fn preview_table_returns_limited_formatted_rows() -> Result<(), DeltaFunnelError> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let table = session
            .table_from_sql("select 1 as id union all select 2 as id order by id")
            .await?;

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
            .preview_table_with_progress(&table, 1, reporter)
            .await;

        assert!(result.is_err());
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
    async fn preview_progress_stops_before_formatting_when_execution_fails()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let table = session
            .table_from_sql("select cast(1 as bigint) / cast(0 as bigint) as value")
            .await?;
        let (reporter, events) = recording_preview_progress();

        let result = session
            .preview_table_with_progress(&table, 1, reporter)
            .await;

        assert!(result.is_err());
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
