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
use futures_util::{Stream, StreamExt};

use crate::{
    DeltaFunnelError, MssqlOutputBatchStream, MssqlOutputBatchStreamFactory,
    collect_delta_provider_read_stats, datafusion_query_output_stream,
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

pub(crate) struct ProviderStatsRecordingStream {
    inner: MssqlOutputBatchStream,
    physical_plan: Arc<dyn ExecutionPlan>,
    provider_stats: SharedProviderReadStats,
    recorded: bool,
}

impl ProviderStatsRecordingStream {
    pub(crate) fn new(
        inner: MssqlOutputBatchStream,
        physical_plan: Arc<dyn ExecutionPlan>,
        provider_stats: SharedProviderReadStats,
    ) -> Self {
        Self {
            inner,
            physical_plan,
            provider_stats,
            recorded: false,
        }
    }

    fn record_if_needed(&mut self) {
        if self.recorded {
            return;
        }
        self.recorded = true;
        let snapshots = collect_delta_provider_read_stats(self.physical_plan.as_ref());
        if snapshots.is_empty() {
            return;
        }
        match self.provider_stats.lock() {
            Ok(mut provider_stats) => provider_stats.extend(snapshots),
            Err(poisoned) => poisoned.into_inner().extend(snapshots),
        }
    }
}

impl Stream for ProviderStatsRecordingStream {
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

pub(super) async fn batch_stream_for_lazy_table_from_session_parts(
    context: SessionContext,
    table: LazyTable,
    sources: Vec<RegisteredSessionSource>,
    derived_tables: Vec<RegisteredDerivedTable>,
    pending_derived_tables: Vec<PendingDerivedTable>,
    provider_stats: Option<SharedProviderReadStats>,
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
    if let Some(provider_stats) = provider_stats {
        return Ok(Box::pin(ProviderStatsRecordingStream::new(
            Box::pin(stream.map(|batch| {
                batch.map_err(|error| datafusion_handoff_setup_error("query_output_stream", error))
            })),
            physical_plan,
            provider_stats,
        )));
    }

    Ok(Box::pin(stream.map(|batch| {
        batch.map_err(|error| datafusion_handoff_setup_error("query_output_stream", error))
    })))
}

impl DeltaFunnelSession {
    pub(crate) async fn batch_stream_for_lazy_table(
        &self,
        table: &LazyTable,
    ) -> Result<MssqlOutputBatchStream, DeltaFunnelError> {
        let dataframe = self.dataframe_for_lazy_table(table).await?;
        let physical_plan = dataframe
            .create_physical_plan()
            .await
            .map_err(|error| datafusion_handoff_setup_error("physical_plan", error))?;
        let stream = datafusion_query_output_stream(physical_plan, self.context.task_ctx())
            .map_err(|error| datafusion_handoff_setup_error("query_output_stream", error))?;

        Ok(Box::pin(stream.map(|batch| {
            batch.map_err(|error| datafusion_handoff_setup_error("query_output_stream", error))
        })))
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
        let dataframe = self.dataframe_for_lazy_table(table).await?;
        let schema = Arc::new(dataframe.schema().as_arrow().clone());
        let batches = dataframe
            .limit(0, Some(limit))
            .map_err(|error| datafusion_handoff_setup_error("preview_limit", error))?
            .collect()
            .await
            .map_err(|error| datafusion_handoff_setup_error("preview_collect", error))?;
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

    #[allow(dead_code)]
    pub(super) fn lazy_table_batch_stream_factory(
        &self,
        table: LazyTable,
    ) -> MssqlOutputBatchStreamFactory {
        self.lazy_table_batch_stream_factory_with_provider_stats(table, None)
    }

    pub(super) fn lazy_table_batch_stream_factory_with_provider_stats(
        &self,
        table: LazyTable,
        provider_stats: Option<SharedProviderReadStats>,
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
                )
                .await
            })
        })
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
    use std::sync::atomic::Ordering;

    use crate::{
        DeltaFunnelError, DeltaSourceConfig, QueryOptions, table_formats::RealParquetDeltaTable,
    };

    use super::super::{
        DeltaFunnelSession, LazyTable, LazyTableKind, SessionOptions,
        test_support::{
            DeltaLogTable, collect_stream_marker_values, collect_stream_row_count,
            marker_region_provider, marker_values_from_batches,
            scan_counting_marker_region_provider,
        },
    };

    #[tokio::test]
    async fn batch_stream_for_lazy_table_reads_registered_delta_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;

        let stream = session.batch_stream_for_lazy_table(&source).await?;
        let rows = collect_stream_row_count(stream).await?;

        assert_eq!(rows, table.rows());
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

        let stream = session.batch_stream_for_lazy_table(&derived).await?;
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

        let stream = session.batch_stream_for_lazy_table(&alias).await?;
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
            .batch_stream_for_lazy_table(&LazyTable::placeholder(42, LazyTableKind::DeltaSource))
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

        let west_stream = session.batch_stream_for_lazy_table(&west).await?;
        let east_stream = session.batch_stream_for_lazy_table(&east).await?;
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

        let west_stream = session.batch_stream_for_lazy_table(&replanned_west).await?;
        let east_stream = session.batch_stream_for_lazy_table(&replanned_east).await?;
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
