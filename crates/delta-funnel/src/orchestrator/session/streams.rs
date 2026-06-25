use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    physical_plan::ExecutionPlan,
    prelude::{DataFrame, SessionContext},
};
use futures_util::{Stream, StreamExt};

use crate::{
    DeltaFunnelError, MssqlOutputBatchStream, MssqlOutputBatchStreamFactory,
    collect_delta_provider_read_stats, datafusion_query_output_stream,
};

use super::{
    LazyTable, LazyTableKind, PendingDerivedTable, RegisteredDerivedTable, RegisteredSessionSource,
    cached_output_stream_setup_error, datafusion_handoff_setup_error, read_only_sql_options,
    unknown_lazy_table_error,
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

struct ProviderStatsRecordingStream {
    inner: MssqlOutputBatchStream,
    physical_plan: Arc<dyn ExecutionPlan>,
    provider_stats: SharedProviderReadStats,
    recorded: bool,
}

impl ProviderStatsRecordingStream {
    fn new(
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

pub(super) fn failing_cached_output_batch_stream_factory(
    output_name: String,
    error: DeltaFunnelError,
) -> MssqlOutputBatchStreamFactory {
    Box::new(move || {
        Box::pin(async move { Err(cached_output_stream_setup_error(&output_name, error)) })
    })
}

pub(super) async fn replanned_sql_batch_stream(
    context: SessionContext,
    output_name: String,
    sql_text: String,
    expected_schema: SchemaRef,
    provider_stats: Option<SharedProviderReadStats>,
) -> Result<MssqlOutputBatchStream, DeltaFunnelError> {
    let dataframe = context
        .sql_with_options(sql_text.as_str(), read_only_sql_options())
        .await
        .map_err(|error| cached_output_stream_setup_error(&output_name, error))?;
    validate_replanned_output_schema(
        &output_name,
        dataframe.schema().as_arrow(),
        &expected_schema,
    )?;
    let physical_plan = dataframe
        .create_physical_plan()
        .await
        .map_err(|error| cached_output_stream_setup_error(&output_name, error))?;
    let stream = datafusion_query_output_stream(Arc::clone(&physical_plan), context.task_ctx())
        .map_err(|error| cached_output_stream_setup_error(&output_name, error))?;
    if let Some(provider_stats) = provider_stats {
        let error_output_name = output_name.clone();
        return Ok(Box::pin(ProviderStatsRecordingStream::new(
            Box::pin(stream.map(move |batch| {
                batch.map_err(|error| cached_output_stream_setup_error(&error_output_name, error))
            })),
            physical_plan,
            provider_stats,
        )));
    }

    Ok(Box::pin(stream.map(move |batch| {
        batch.map_err(|error| cached_output_stream_setup_error(&output_name, error))
    })))
}

fn validate_replanned_output_schema(
    output_name: &str,
    replanned_schema: &datafusion::arrow::datatypes::Schema,
    expected_schema: &SchemaRef,
) -> Result<(), DeltaFunnelError> {
    if replanned_schema == expected_schema.as_ref() {
        return Ok(());
    }

    Err(cached_output_stream_setup_error(
        output_name,
        "replanned output schema does not match the original output schema",
    ))
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
