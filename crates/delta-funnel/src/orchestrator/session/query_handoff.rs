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

pub(super) type SharedProviderReadStats = Arc<Mutex<Vec<crate::DeltaProviderReadStatsSnapshot>>>;

pub(super) fn shared_provider_read_stats() -> SharedProviderReadStats {
    Arc::new(Mutex::new(Vec::new()))
}

pub(super) fn provider_read_stats_snapshot(
    provider_stats: &SharedProviderReadStats,
) -> Vec<crate::DeltaProviderReadStatsSnapshot> {
    match provider_stats.lock() {
        Ok(provider_stats) => provider_stats.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

/// Records final Delta provider statistics for a write-all report.
///
/// The recorder keeps only shared read counters and snapshots them when the
/// wrapped stream ends or fails.
pub(crate) struct FinalDeltaReadStatsRecorder {
    inner: MssqlOutputBatchStream,
    // One live counter handle for each Delta scan in the output plan.
    read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
    provider_stats: SharedProviderReadStats,
    recorded: bool,
}

impl FinalDeltaReadStatsRecorder {
    pub(crate) fn new(
        inner: MssqlOutputBatchStream,
        read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
        provider_stats: SharedProviderReadStats,
    ) -> Self {
        Self {
            inner,
            read_stats_handles,
            provider_stats,
            recorded: false,
        }
    }

    fn record_if_needed(&mut self) {
        if self.recorded {
            return;
        }
        self.recorded = true;
        let snapshots = snapshot_delta_provider_read_stats(&self.read_stats_handles);
        if snapshots.is_empty() {
            return;
        }
        match self.provider_stats.lock() {
            Ok(mut provider_stats) => provider_stats.extend(snapshots),
            Err(poisoned) => poisoned.into_inner().extend(snapshots),
        }
    }
}

impl Stream for FinalDeltaReadStatsRecorder {
    type Item = Result<RecordBatch, DeltaFunnelError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(context) {
            Poll::Ready(None) => {
                self.record_if_needed();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                self.record_if_needed();
                Poll::Ready(Some(Err(error)))
            }
            other => other,
        }
    }
}

/// Reports Delta file progress while forwarding one batch stream.
///
/// Trackers for separate partitions can share one state so file counts remain
/// monotonic across the whole physical plan. The state keeps only provider
/// counters and does not retain the plan, run another query, or read Delta
/// metadata again.
struct DeltaFileProgressTracker {
    inner: MssqlOutputBatchStream,
    state: SharedDeltaFileProgressState,
}

/// File progress state shared by every tracked stream in one physical plan.
pub(super) struct DeltaFileProgressState {
    // One live counter handle for each Delta scan in the output plan.
    read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
    reporter: ProgressReporter,
    phase: ProgressPhase,
    output_name: Option<String>,
    // The last emitted handled and total counts, used to suppress duplicates.
    last_file_progress: Option<(u64, u64)>,
}

pub(super) type SharedDeltaFileProgressState = Arc<Mutex<DeltaFileProgressState>>;

/// Creates one shared file progress state for a physical plan.
pub(super) fn shared_delta_file_progress_state(
    read_stats_handles: Vec<DeltaProviderReadStatsHandle>,
    reporter: ProgressReporter,
    phase: ProgressPhase,
    output_name: Option<String>,
) -> SharedDeltaFileProgressState {
    Arc::new(Mutex::new(DeltaFileProgressState {
        read_stats_handles,
        reporter,
        phase,
        output_name,
        last_file_progress: None,
    }))
}

/// Wraps a stream so polling it samples the shared file progress state.
pub(super) fn track_delta_file_progress(
    stream: MssqlOutputBatchStream,
    state: SharedDeltaFileProgressState,
) -> MssqlOutputBatchStream {
    Box::pin(DeltaFileProgressTracker {
        inner: stream,
        state,
    })
}

/// Adds optional live file progress and final provider statistics tracking to
/// one physical-plan stream.
///
/// Both trackers reuse the same live Delta scan counters. They do not execute
/// another query or retain the physical plan. `progress` contains the reporter
/// and output name used for live events.
pub(super) fn wrap_stream_with_delta_read_tracking(
    mut stream: MssqlOutputBatchStream,
    physical_plan: &dyn ExecutionPlan,
    provider_stats: Option<SharedProviderReadStats>,
    progress: Option<(ProgressReporter, String)>,
) -> MssqlOutputBatchStream {
    if provider_stats.is_none() && progress.is_none() {
        return stream;
    }

    let read_stats_handles = collect_delta_provider_read_stats_handles(physical_plan);
    if let Some((reporter, output_name)) = progress {
        let state = shared_delta_file_progress_state(
            read_stats_handles.clone(),
            reporter,
            ProgressPhase::Writing,
            Some(output_name),
        );
        stream = track_delta_file_progress(stream, state);
    }
    if let Some(provider_stats) = provider_stats {
        stream = Box::pin(FinalDeltaReadStatsRecorder::new(
            stream,
            read_stats_handles,
            provider_stats,
        ));
    }
    stream
}

impl DeltaFileProgressTracker {
    /// Emits only when the visible handled and total file counts have changed.
    fn emit_if_changed(&self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(event) = state.pending_event() {
            // Partition streams may run in separate Tokio tasks. Keep delivery
            // inside this lock so one reporter callback finishes before the
            // next partition can start another callback.
            state.reporter.emit(&event);
        }
    }
}

impl DeltaFileProgressState {
    fn pending_event(&mut self) -> Option<ProgressEvent> {
        let snapshots = snapshot_delta_provider_read_stats(&self.read_stats_handles);
        let event = ProgressEvent::file_progress_from_provider_stats(
            self.phase,
            self.output_name.as_deref(),
            &snapshots,
        )?;
        let file_progress = event.files_handled().zip(event.files_total())?;
        if self.last_file_progress == Some(file_progress) {
            return None;
        }
        self.last_file_progress = Some(file_progress);
        Some(event)
    }
}

impl Stream for DeltaFileProgressTracker {
    type Item = Result<RecordBatch, DeltaFunnelError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(context) {
            Poll::Ready(None) => {
                // Capture final progress before the shared counters are dropped.
                self.emit_if_changed();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                // Keep the last partial progress when query execution fails.
                self.emit_if_changed();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(batch))) => {
                // A ready batch is the existing foreground sampling boundary.
                self.emit_if_changed();
                Poll::Ready(Some(Ok(batch)))
            }
            // Do not add a timer or wake the stream only to refresh progress.
            Poll::Pending => Poll::Pending,
        }
    }
}

pub(super) async fn batch_stream_for_lazy_table_from_session_parts(
    context: SessionContext,
    table: LazyTable,
    sources: Vec<RegisteredSessionSource>,
    derived_tables: Vec<RegisteredDerivedTable>,
    pending_derived_tables: Vec<PendingDerivedTable>,
    provider_stats: Option<SharedProviderReadStats>,
    progress: Option<(ProgressReporter, String)>,
) -> Result<MssqlOutputBatchStream, DeltaFunnelError> {
    let dataframe = dataframe_for_lazy_table_from_session_parts(
        &context,
        &table,
        &sources,
        &derived_tables,
        &pending_derived_tables,
    )
    .await?;
    let physical_plan = dataframe
        .create_physical_plan()
        .await
        .map_err(|error| datafusion_handoff_setup_error("physical_plan", error))?;
    let stream = datafusion_query_output_stream(Arc::clone(&physical_plan), context.task_ctx())
        .map_err(|error| datafusion_handoff_setup_error("query_output_stream", error))?;
    let stream = Box::pin(stream.map(|batch| {
        batch.map_err(|error| datafusion_handoff_setup_error("query_output_stream", error))
    }));

    Ok(wrap_stream_with_delta_read_tracking(
        stream,
        physical_plan.as_ref(),
        provider_stats,
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
        let dataframe = self.dataframe_for_lazy_table(table).await?;
        let physical_plan = dataframe
            .create_physical_plan()
            .await
            .map_err(|error| datafusion_handoff_setup_error("physical_plan", error))?;
        let stream =
            datafusion_query_output_stream(Arc::clone(&physical_plan), self.context.task_ctx())
                .map_err(|error| datafusion_handoff_setup_error("query_output_stream", error))?;
        let stream = Box::pin(stream.map(|batch| {
            batch.map_err(|error| datafusion_handoff_setup_error("query_output_stream", error))
        }));
        let Some((reporter, output_name)) = progress else {
            return Ok(stream);
        };
        let read_stats_handles = collect_delta_provider_read_stats_handles(physical_plan.as_ref());
        let state = shared_delta_file_progress_state(
            read_stats_handles,
            reporter.clone(),
            ProgressPhase::Writing,
            Some(output_name.to_owned()),
        );

        Ok(track_delta_file_progress(stream, state))
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
        emit_preview_phase(reporter, ProgressPhase::CollectingPreview);
        let stream = datafusion_query_output_stream(Arc::clone(&physical_plan), task_context)
            .map_err(|error| datafusion_handoff_setup_error("preview_collect", error))?;
        let stream: MssqlOutputBatchStream = Box::pin(stream.map(|batch| {
            batch.map_err(|error| datafusion_handoff_setup_error("preview_collect", error))
        }));
        let stream = match reporter {
            Some(reporter) => {
                let read_stats_handles =
                    collect_delta_provider_read_stats_handles(physical_plan.as_ref());
                let state = shared_delta_file_progress_state(
                    read_stats_handles,
                    reporter.clone(),
                    ProgressPhase::CollectingPreview,
                    None,
                );
                track_delta_file_progress(stream, state)
            }
            None => stream,
        };
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

    #[cfg(test)]
    pub(super) fn lazy_table_batch_stream_factory(
        &self,
        table: LazyTable,
    ) -> MssqlOutputBatchStreamFactory {
        self.lazy_table_batch_stream_factory_for_write_all(table, None, None)
    }

    /// Builds a deferred stream factory with write-all's optional final stats
    /// and live file progress tracking.
    ///
    /// `progress` contains an output-scoped reporter and that output's name.
    pub(super) fn lazy_table_batch_stream_factory_for_write_all(
        &self,
        table: LazyTable,
        provider_stats: Option<SharedProviderReadStats>,
        progress: Option<(ProgressReporter, String)>,
    ) -> MssqlOutputBatchStreamFactory {
        let context = self.context.clone();
        let sources = self.sources.clone();
        let derived_tables = self.derived_tables.clone();
        let pending_derived_tables = self.pending_derived_tables.clone();

        Box::new(move || {
            Box::pin(async move {
                batch_stream_for_lazy_table_from_session_parts(
                    context,
                    table,
                    sources,
                    derived_tables,
                    pending_derived_tables,
                    provider_stats,
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
        atomic::{AtomicUsize, Ordering},
    };

    use datafusion::{
        arrow::datatypes::DataType,
        logical_expr::{Volatility, create_udf},
    };

    use crate::{
        DeltaFunnelError, DeltaSourceConfig, QueryOptions,
        progress::{ProgressEventKind, ProgressOperation, ProgressPhase, ProgressReporter},
        table_formats::RealParquetDeltaTable,
    };

    use super::super::{
        DeltaFunnelSession, LazyTable, LazyTableKind, SessionOptions,
        test_support::{
            DeltaLogTable, collect_stream_marker_values, collect_stream_row_count,
            failing_scan_marker_region_provider, marker_region_provider,
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
