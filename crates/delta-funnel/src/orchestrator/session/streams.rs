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
    DeltaFunnelSession, LazyTable, LazyTableKind, OutputWritePlan, PendingDerivedTable,
    RegisteredDerivedTable, RegisteredSessionSource,
    errors::{
        cached_output_stream_setup_error, datafusion_handoff_setup_error,
        unknown_cached_alias_error, unknown_lazy_table_error,
    },
    registry::{DerivedTableDependency, read_only_sql_options},
    write_all::{MssqlCachedOutputStreamRoute, MssqlDerivedCacheAliasPlan},
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

impl DeltaFunnelSession {
    /// Classifies one selected output relative to active cached aliases.
    ///
    /// Direct selected-alias use wins over lineage use because the normal
    /// registered-alias stream path should read the active cached provider.
    /// Dependent outputs are identified from captured lineage so later stream
    /// construction can replan from retained SQL while all cache aliases are
    /// installed.
    #[allow(dead_code)]
    pub(super) fn cached_output_stream_route(
        &self,
        request: &OutputWritePlan,
        active_aliases: &[MssqlDerivedCacheAliasPlan],
    ) -> Result<MssqlCachedOutputStreamRoute, DeltaFunnelError> {
        if active_aliases.is_empty() {
            return Ok(MssqlCachedOutputStreamRoute::UncachedLazyTable);
        }

        for alias in active_aliases {
            self.registered_derived_table_by_id(alias.table_id())
                .ok_or_else(|| unknown_cached_alias_error(alias))?;
        }

        if let Some(alias) = active_aliases
            .iter()
            .find(|alias| request.table().id() == alias.table_id())
        {
            return Ok(MssqlCachedOutputStreamRoute::DirectCachedAlias(
                alias.clone(),
            ));
        }

        if request.table().kind() == LazyTableKind::DeltaSource {
            return Ok(MssqlCachedOutputStreamRoute::UncachedLazyTable);
        }

        let dependencies = self.transitive_registered_derived_dependencies(request.table())?;
        let dependent_aliases = active_aliases
            .iter()
            .filter(|alias| {
                dependencies.iter().any(|dependency| {
                    matches!(
                        dependency,
                        DerivedTableDependency::RegisteredDerived { table_id, .. }
                            if *table_id == alias.table_id()
                    )
                })
            })
            .cloned()
            .collect::<Vec<_>>();

        if dependent_aliases.is_empty() {
            Ok(MssqlCachedOutputStreamRoute::UncachedLazyTable)
        } else {
            Ok(MssqlCachedOutputStreamRoute::ReplannedCachedDependency(
                dependent_aliases,
            ))
        }
    }

    /// Builds an async stream factory for one output while cache aliases are active.
    ///
    /// The returned factory performs DataFusion stream setup when the workflow
    /// attempts this output. Direct cached aliases and unrelated outputs reuse
    /// the normal lazy-table stream path. Dependent outputs replan from the
    /// retained SQL text so active scoped cache aliases can replace the
    /// registered providers referenced by that SQL.
    #[allow(dead_code)]
    pub(super) fn cached_output_batch_stream_factory(
        &self,
        request: &OutputWritePlan,
        active_aliases: &[MssqlDerivedCacheAliasPlan],
    ) -> Result<MssqlOutputBatchStreamFactory, DeltaFunnelError> {
        self.cached_output_batch_stream_factory_with_provider_stats(request, active_aliases, None)
    }

    pub(super) fn cached_output_batch_stream_factory_with_provider_stats(
        &self,
        request: &OutputWritePlan,
        active_aliases: &[MssqlDerivedCacheAliasPlan],
        provider_stats: Option<SharedProviderReadStats>,
    ) -> Result<MssqlOutputBatchStreamFactory, DeltaFunnelError> {
        let route = self.cached_output_stream_route(request, active_aliases)?;
        match route {
            MssqlCachedOutputStreamRoute::DirectCachedAlias(_)
            | MssqlCachedOutputStreamRoute::UncachedLazyTable => Ok(self
                .lazy_table_batch_stream_factory_with_provider_stats(
                    request.table().clone(),
                    provider_stats,
                )),
            MssqlCachedOutputStreamRoute::ReplannedCachedDependency(_) => {
                let output_name = request.target().output_name().to_owned();
                let sql_text = match self.sql_text_for_derived_table(request.table()) {
                    Ok(sql_text) => sql_text.to_owned(),
                    Err(error) => {
                        return Ok(failing_cached_output_batch_stream_factory(
                            output_name,
                            error,
                        ));
                    }
                };
                let expected_schema = match self.schema_for_lazy_table(request.table()) {
                    Ok(schema) => Arc::clone(schema),
                    Err(error) => {
                        return Ok(failing_cached_output_batch_stream_factory(
                            output_name,
                            error,
                        ));
                    }
                };
                let context = self.context.clone();
                Ok(Box::new(move || {
                    Box::pin(async move {
                        replanned_sql_batch_stream(
                            context,
                            output_name,
                            sql_text,
                            expected_schema,
                            provider_stats,
                        )
                        .await
                    })
                }))
            }
        }
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

#[cfg(test)]
mod tests {
    use crate::{
        DeltaFunnelError, DeltaSourceConfig, QueryOptions, table_formats::RealParquetDeltaTable,
    };

    use super::super::{
        DeltaFunnelSession, LazyTable, LazyTableKind, SessionOptions,
        test_support::collect_stream_row_count,
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
}
