//! Rust backing model for lazy query-load orchestration.
//!
//! This module owns the high-level session and request data shapes that will
//! later back the Python `Session` and `Table` API. Metadata report helpers do
//! not contact SQL Server or execute rows unless a write path explicitly does so.

mod dry_run_report;
mod errors;
mod handles;
mod mssql_output;
mod options;
mod registry;
mod source_report;
mod streams;
#[cfg(test)]
mod test_support;
mod write_all;

use std::fmt;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::{DataFrame, SessionContext};
use futures_util::StreamExt;

use crate::{
    DeltaFunnelError, DeltaSourceConfig, DeltaTableProviderConfig, MssqlOutputBatchStream,
    datafusion_query_output_stream, datafusion_session_context, load_delta_source,
    preflight_delta_protocol, register_delta_sources_with_scan_execution_options,
};

pub use handles::{
    LazyTable, LazyTableKind, MssqlOutputTarget, OutputWritePlan, PlannedMssqlOutput, RunMode,
};
pub use options::SessionOptions;
pub use registry::{RegisteredDerivedTable, RegisteredSessionSource};
pub use source_report::{DeltaProviderSchedulingReport, DeltaSourceReport, SourceUsageStatus};
pub use write_all::{
    WriteAllCacheAliasReport, WriteAllCacheAliasStatus, WriteAllCacheCandidateSkip,
    WriteAllCacheCandidateSkipReason, WriteAllCacheMode, WriteAllCacheReport,
    WriteAllNoCacheReason, WriteAllOptions, WriteAllReport,
};

pub use dry_run_report::{
    MssqlDryRunOutputFieldReport, MssqlDryRunOutputReport, MssqlDryRunSqlIdentityReport,
    MssqlDryRunSqlIdentityState, MssqlDryRunWorkflowReport,
};
use errors::{datafusion_handoff_setup_error, unknown_lazy_table_error};
#[cfg(test)]
pub(crate) use mssql_output::OrchestratorMssqlOutputWriter;
use registry::PendingDerivedTable;
use streams::dataframe_for_lazy_table_from_session_parts;

/// Rust backing session for lazy query-load workflows.
pub struct DeltaFunnelSession {
    options: SessionOptions,
    context: SessionContext,
    next_table_id: u64,
    sources: Vec<RegisteredSessionSource>,
    derived_tables: Vec<RegisteredDerivedTable>,
    pending_derived_tables: Vec<PendingDerivedTable>,
}

impl DeltaFunnelSession {
    /// Builds a new session with validated local options.
    ///
    /// # Errors
    ///
    /// Returns the first local option validation failure before any source
    /// loading, SQL planning, SQL Server connection, or row execution.
    pub fn new(options: SessionOptions) -> Result<Self, DeltaFunnelError> {
        options.validate()?;
        let context = datafusion_session_context(options.query_options())?;
        Ok(Self {
            options,
            context,
            next_table_id: 0,
            sources: Vec::new(),
            derived_tables: Vec::new(),
            pending_derived_tables: Vec::new(),
        })
    }

    /// Returns the validated session options.
    #[must_use]
    pub const fn options(&self) -> &SessionOptions {
        &self.options
    }

    /// Returns the DataFusion session context owned by this orchestrator.
    ///
    /// The session context is exposed so later planning steps can analyze SQL
    /// against registered session aliases. Delta source registration should
    /// still go through [`DeltaFunnelSession::delta_lake`] so the orchestrator's
    /// source reports stay aligned with the DataFusion catalog.
    #[must_use]
    pub const fn context(&self) -> &SessionContext {
        &self.context
    }

    /// Returns the next unassigned session-local lazy table id.
    #[must_use]
    pub const fn next_table_id(&self) -> u64 {
        self.next_table_id
    }

    /// Registers one Delta source and returns its lazy table handle.
    ///
    /// The method performs source setup only: Delta snapshot metadata loading,
    /// protocol preflight, and DataFusion table registration. It does not scan
    /// data files for row production, parse user SQL, contact SQL Server, or
    /// execute an output action.
    ///
    /// # Errors
    ///
    /// Returns the first Delta source loading, protocol preflight, duplicate
    /// alias, schema conversion, or DataFusion registration error. Session
    /// source state is updated only after the DataFusion registration succeeds.
    pub fn delta_lake(&mut self, source: DeltaSourceConfig) -> Result<LazyTable, DeltaFunnelError> {
        self.reject_registered_alias_name(&source.name)?;
        let planned = load_delta_source(source)?;
        let preflight = preflight_delta_protocol(&planned)?;
        let registered = register_delta_sources_with_scan_execution_options(
            &self.context,
            vec![DeltaTableProviderConfig {
                source: planned,
                protocol: preflight,
                scan_target_partitions: None,
            }],
            self.options.provider_scan_options(),
        )?;
        let registered =
            registered
                .sources
                .into_iter()
                .next()
                .ok_or_else(|| DeltaFunnelError::Config {
                    message: "Delta source registration returned no registered source".to_owned(),
                })?;
        let table = self.allocate_delta_source_table(registered.name.clone());
        let session_source = RegisteredSessionSource::from_registered(table.clone(), registered);
        self.sources.push(session_source);
        Ok(table)
    }

    fn allocate_delta_source_table(&mut self, name: String) -> LazyTable {
        let id = self.next_table_id;
        self.next_table_id = self.next_table_id.saturating_add(1);
        LazyTable::delta_source(id, name)
    }

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

    async fn dataframe_for_lazy_table(
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

    fn schema_for_lazy_table(&self, table: &LazyTable) -> Result<&SchemaRef, DeltaFunnelError> {
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
}

impl fmt::Debug for DeltaFunnelSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaFunnelSession")
            .field("options", &self.options)
            .field("sources", &self.sources)
            .field("derived_tables", &self.derived_tables)
            .field("next_table_id", &self.next_table_id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, atomic::Ordering};

    use super::test_support::{
        DeltaLogTable, collect_stream_row_count, execute_output_request,
        failing_scan_marker_region_provider, scan_counting_marker_region_provider,
        secret_connection,
    };
    use super::*;
    use crate::{
        LoadMode, MssqlSchemaPlanOptions, MssqlTargetCleanupStatus, MssqlWorkflowOutputWriter,
        MssqlWriteOptions, MssqlWriteReport, ResolvedMssqlTarget,
        plan_mssql_target_for_resolved_output,
    };
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct FakeOrchestratorWriteCall {
        output_name: String,
        rows: u64,
    }

    #[derive(Clone, Default)]
    struct FakeWorkflowWriter {
        calls: Arc<Mutex<Vec<FakeOrchestratorWriteCall>>>,
        fail_output_name: Option<String>,
    }

    impl FakeWorkflowWriter {
        fn failing_on(output_name: &str) -> Self {
            Self {
                fail_output_name: Some(output_name.to_owned()),
                ..Self::default()
            }
        }

        fn calls(&self) -> Arc<Mutex<Vec<FakeOrchestratorWriteCall>>> {
            Arc::clone(&self.calls)
        }
    }

    #[async_trait]
    impl MssqlWorkflowOutputWriter for FakeWorkflowWriter {
        async fn write_output(
            &mut self,
            output_schema: SchemaRef,
            resolved_target: ResolvedMssqlTarget,
            schema_options: MssqlSchemaPlanOptions,
            mut batches: MssqlOutputBatchStream,
            _write_options: MssqlWriteOptions,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            let mut rows = 0_u64;
            let mut batch_count = 0_u64;

            while let Some(batch) = batches.next().await {
                let batch = batch?;
                rows = rows.saturating_add(u64::try_from(batch.num_rows()).map_err(|_| {
                    DeltaFunnelError::Config {
                        message: "fake workflow writer row count overflowed u64".to_owned(),
                    }
                })?);
                batch_count = batch_count.saturating_add(1);
            }

            let output_plan = plan_mssql_target_for_resolved_output(
                output_schema.as_ref(),
                &resolved_target,
                schema_options,
            )?;
            self.calls
                .lock()
                .map_err(|_| DeltaFunnelError::Config {
                    message: "fake workflow writer call lock poisoned".to_owned(),
                })?
                .push(FakeOrchestratorWriteCall {
                    output_name: resolved_target.output_name().to_owned(),
                    rows,
                });

            if self
                .fail_output_name
                .as_deref()
                .is_some_and(|output_name| output_name == resolved_target.output_name())
            {
                return Err(DeltaFunnelError::MssqlWorkflowPlanning {
                    message: format!(
                        "fake workflow writer failed for `{}`",
                        resolved_target.output_name()
                    ),
                });
            }

            Ok(MssqlWriteReport::from_output_plan(
                &output_plan,
                rows,
                batch_count,
                0,
                false,
                MssqlTargetCleanupStatus::NotApplicable,
            ))
        }
    }

    #[tokio::test]
    async fn write_all_auto_restores_cache_alias_after_output_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;
        let big_output = execute_output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east_output =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::failing_on("west_output");
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[big_output, west_output, east_output], writer)
            .await?;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0].output_name, "big_output");
            assert_eq!(calls[0].rows, 2);
            assert_eq!(calls[1].output_name, "west_output");
            assert_eq!(calls[1].rows, 1);
            assert_eq!(source_scans.load(Ordering::SeqCst), 1);
            assert_eq!(report.succeeded_count(), 1);
            assert_eq!(report.failed_count(), 1);
            assert_eq!(report.skipped_count(), 1);
            assert!(report.outputs()[0].is_succeeded());
            assert_eq!(report.outputs()[0].output_name(), "big_output");
            assert!(report.outputs()[1].is_failed());
            assert_eq!(report.outputs()[1].output_name(), "west_output");
            assert!(report.outputs()[2].is_skipped());
            assert_eq!(report.outputs()[2].output_name(), "east_output");
        }
        let WriteAllCacheReport::CacheAliases {
            aliases,
            skipped_candidates,
        } = report.cache()
        else {
            return Err(format!("expected cache aliases report, got {:?}", report.cache()).into());
        };
        assert!(skipped_candidates.is_empty());
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].table_id(), big.id());
        assert_eq!(aliases[0].alias(), "big");
        assert_eq!(aliases[0].output_indexes(), &[0, 1, 2]);
        assert_eq!(
            aliases[0].status(),
            WriteAllCacheAliasStatus::MaterializedAndRestored
        );

        let restored_big_factory = session.lazy_table_batch_stream_factory(big);
        let restored_big_rows = collect_stream_row_count(restored_big_factory().await?).await?;
        assert_eq!(restored_big_rows, 2);
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_cache_materialization_failure_prevents_output_attempts()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = failing_scan_marker_region_provider();
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east_output =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let error = session
            .write_all_with_writer(&[west_output, east_output], writer)
            .await;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert!(matches!(
                error,
                Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                    if message.contains("scoped MSSQL cache alias materialize failed")
                        && message.contains("big")
            ));
            assert!(calls.is_empty());
            assert_eq!(source_scans.load(Ordering::SeqCst), 1);
        }

        let restored_error = match session.batch_stream_for_lazy_table(&big).await {
            Ok(stream) => match collect_stream_row_count(stream).await {
                Ok(rows) => {
                    return Err(
                        format!("expected restored big read to fail, got {rows} rows").into(),
                    );
                }
                Err(error) => error,
            },
            Err(error) => error,
        };
        assert!(matches!(
            &restored_error,
            DeltaFunnelError::BatchPipeline { message, .. }
                if message.contains("forced scan planning failure")
        ));
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_restores_replaced_alias_after_later_cache_materialization_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (big_source_provider, big_source_scans) = scan_counting_marker_region_provider("big")?;
        let (names_source_provider, names_source_scans) = failing_scan_marker_region_provider();
        session
            .context()
            .register_table("big_source", big_source_provider)?;
        session
            .context()
            .register_table("names_source", names_source_provider)?;
        let pending_big = session
            .table_from_sql(
                "select marker as big_marker, region, \
                 case when region = 'west' then 1 else 2 end as id \
                 from big_source",
            )
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let pending_names = session
            .table_from_sql(
                "select marker as name_marker, region, \
                 case when region = 'west' then 1 else 2 end as id \
                 from names_source",
            )
            .await?;
        let names = session.register_alias("names", &pending_names)?;
        let west = session
            .table_from_sql(
                "select big.id, big.big_marker, names.name_marker \
                 from big join names on big.id = names.id \
                 where big.region = 'west'",
            )
            .await?;
        let east = session
            .table_from_sql(
                "select big.id, big.big_marker, names.name_marker \
                 from big join names on big.id = names.id \
                 where big.region = 'east'",
            )
            .await?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east_output =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let error = session
            .write_all_with_writer(&[west_output, east_output], writer)
            .await;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert!(matches!(
                error,
                Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                    if message.contains("scoped MSSQL cache alias materialize failed")
                        && message.contains("names")
            ));
            assert!(calls.is_empty());
            assert_eq!(big_source_scans.load(Ordering::SeqCst), 1);
            assert_eq!(names_source_scans.load(Ordering::SeqCst), 1);
        }

        let restored_big_factory = session.lazy_table_batch_stream_factory(big);
        assert_eq!(
            collect_stream_row_count(restored_big_factory().await?).await?,
            2
        );
        assert_eq!(big_source_scans.load(Ordering::SeqCst), 2);

        let restored_names_error = match session.batch_stream_for_lazy_table(&names).await {
            Ok(stream) => match collect_stream_row_count(stream).await {
                Ok(rows) => {
                    return Err(
                        format!("expected restored names read to fail, got {rows} rows").into(),
                    );
                }
                Err(error) => error,
            },
            Err(error) => error,
        };
        assert!(matches!(
            &restored_names_error,
            DeltaFunnelError::BatchPipeline { message, .. }
                if message.contains("forced scan planning failure")
        ));
        assert_eq!(names_source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_auto_reports_dependent_stream_setup_failure_before_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("big_source", source_provider)?;
        let pending_big = session
            .table_from_sql("select marker, region from big_source")
            .await?;
        let big = session.register_alias("big", &pending_big)?;
        let west = session
            .table_from_sql("select marker from big where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from big where region = 'east'")
            .await?;
        let pending_west = session
            .pending_derived_tables
            .iter_mut()
            .find(|pending| pending.table.id() == west.id())
            .ok_or("expected pending west table")?;
        pending_west.schema = Arc::new(Schema::new(vec![Field::new(
            "different_marker",
            DataType::Utf8,
            false,
        )]));
        let big_output = execute_output_request(
            big.clone(),
            "big_output",
            "big_orders",
            LoadMode::AppendExisting,
        )?;
        let west_output =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east_output =
            execute_output_request(east, "east_output", "east_orders", LoadMode::AppendExisting)?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[big_output, west_output, east_output], writer)
            .await?;
        {
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].output_name, "big_output");
            assert_eq!(calls[0].rows, 2);
            assert_eq!(source_scans.load(Ordering::SeqCst), 1);
            assert_eq!(report.succeeded_count(), 1);
            assert_eq!(report.failed_count(), 1);
            assert_eq!(report.skipped_count(), 1);
            assert!(report.outputs()[0].is_succeeded());
            assert_eq!(report.outputs()[0].output_name(), "big_output");
            assert!(report.outputs()[1].is_failed());
            assert_eq!(report.outputs()[1].output_name(), "west_output");
            let failure_message = match &report.outputs()[1] {
                crate::sql_server::MssqlOutputWriteStatus::Failed(failure) => failure.error(),
                status => return Err(format!("expected failed status, got {status:?}").into()),
            };
            assert!(
                failure_message.contains("cached output stream setup failed for `west_output`")
            );
            assert!(failure_message.contains("replanned output schema does not match"));
            assert!(report.outputs()[2].is_skipped());
            assert_eq!(report.outputs()[2].output_name(), "east_output");
        }
        let WriteAllCacheReport::CacheAliases {
            aliases,
            skipped_candidates,
        } = report.cache()
        else {
            return Err(format!("expected cache aliases report, got {:?}", report.cache()).into());
        };
        assert!(skipped_candidates.is_empty());
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].table_id(), big.id());
        assert_eq!(aliases[0].alias(), "big");
        assert_eq!(aliases[0].output_indexes(), &[0, 1, 2]);
        assert_eq!(
            aliases[0].status(),
            WriteAllCacheAliasStatus::MaterializedAndRestored
        );

        let restored_big_factory = session.lazy_table_batch_stream_factory(big);
        let restored_big_rows = collect_stream_row_count(restored_big_factory().await?).await?;
        assert_eq!(restored_big_rows, 2);
        assert_eq!(source_scans.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_with_writer_skips_later_outputs_after_writer_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (first_provider, first_scans) = scan_counting_marker_region_provider("first")?;
        let (second_provider, second_scans) = scan_counting_marker_region_provider("second")?;
        let (third_provider, third_scans) = scan_counting_marker_region_provider("third")?;
        session
            .context()
            .register_table("first_source", first_provider)?;
        session
            .context()
            .register_table("second_source", second_provider)?;
        session
            .context()
            .register_table("third_source", third_provider)?;
        let first = session
            .table_from_sql("select marker from first_source where region = 'west'")
            .await?;
        let second = session
            .table_from_sql("select marker from second_source where region = 'west'")
            .await?;
        let third = session
            .table_from_sql("select marker from third_source where region = 'west'")
            .await?;
        let first = execute_output_request(
            first,
            "first_output",
            "first_orders",
            LoadMode::AppendExisting,
        )?;
        let second = execute_output_request(
            second,
            "second_output",
            "second_orders",
            LoadMode::AppendExisting,
        )?;
        let third = execute_output_request(
            third,
            "third_output",
            "third_orders",
            LoadMode::AppendExisting,
        )?;
        let writer = FakeWorkflowWriter::failing_on("second_output");
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[first, second, third], writer)
            .await?;
        let calls = calls
            .lock()
            .map_err(|_| "fake workflow call lock poisoned")?;

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].output_name, "first_output");
        assert_eq!(calls[1].output_name, "second_output");
        assert_eq!(first_scans.load(Ordering::SeqCst), 1);
        assert_eq!(second_scans.load(Ordering::SeqCst), 1);
        assert_eq!(third_scans.load(Ordering::SeqCst), 0);
        assert_eq!(report.succeeded_count(), 1);
        assert_eq!(report.failed_count(), 1);
        assert_eq!(report.skipped_count(), 1);
        assert!(report.outputs()[0].is_succeeded());
        assert_eq!(report.outputs()[0].output_name(), "first_output");
        assert!(report.outputs()[1].is_failed());
        assert_eq!(report.outputs()[1].output_name(), "second_output");
        assert!(report.outputs()[2].is_skipped());
        assert_eq!(report.outputs()[2].output_name(), "third_output");
        Ok(())
    }

    #[tokio::test]
    async fn write_all_with_writer_reports_stream_setup_failure_before_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (first_provider, first_scans) = scan_counting_marker_region_provider("first")?;
        let (failing_provider, failing_scans) = failing_scan_marker_region_provider();
        let (third_provider, third_scans) = scan_counting_marker_region_provider("third")?;
        session
            .context()
            .register_table("first_source", first_provider)?;
        session
            .context()
            .register_table("failing_source", failing_provider)?;
        session
            .context()
            .register_table("third_source", third_provider)?;
        let first = session
            .table_from_sql("select marker from first_source where region = 'west'")
            .await?;
        let failing = session
            .table_from_sql("select marker from failing_source where region = 'west'")
            .await?;
        let third = session
            .table_from_sql("select marker from third_source where region = 'west'")
            .await?;
        let first = execute_output_request(
            first,
            "first_output",
            "first_orders",
            LoadMode::AppendExisting,
        )?;
        let failing = execute_output_request(
            failing,
            "failing_output",
            "failing_orders",
            LoadMode::AppendExisting,
        )?;
        let third = execute_output_request(
            third,
            "third_output",
            "third_orders",
            LoadMode::AppendExisting,
        )?;
        let writer = FakeWorkflowWriter::default();
        let calls = writer.calls();

        let report = session
            .write_all_with_writer(&[first, failing, third], writer)
            .await?;
        let calls = calls
            .lock()
            .map_err(|_| "fake workflow call lock poisoned")?;

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].output_name, "first_output");
        assert_eq!(first_scans.load(Ordering::SeqCst), 1);
        assert_eq!(failing_scans.load(Ordering::SeqCst), 1);
        assert_eq!(third_scans.load(Ordering::SeqCst), 0);
        assert_eq!(report.succeeded_count(), 1);
        assert_eq!(report.failed_count(), 1);
        assert_eq!(report.skipped_count(), 1);
        assert!(report.outputs()[0].is_succeeded());
        assert_eq!(report.outputs()[0].output_name(), "first_output");
        assert!(report.outputs()[1].is_failed());
        assert_eq!(report.outputs()[1].output_name(), "failing_output");
        assert!(report.outputs()[2].is_skipped());
        assert_eq!(report.outputs()[2].output_name(), "third_output");
        Ok(())
    }

    #[tokio::test]
    async fn write_all_rejects_duplicate_output_names_before_stream_setup()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (source_provider, source_scans) = scan_counting_marker_region_provider("shared")?;
        session
            .context()
            .register_table("orders_source", source_provider)?;
        let west = session
            .table_from_sql("select marker from orders_source where region = 'west'")
            .await?;
        let east = session
            .table_from_sql("select marker from orders_source where region = 'east'")
            .await?;
        let west = execute_output_request(
            west,
            "orders_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = execute_output_request(
            east,
            "orders_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let error = session.write_all(&[west, east]).await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("write_all output names must be unique")
                    && message.contains("orders_output")
        ));
        assert_eq!(source_scans.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn write_all_validation_errors_redact_connection_material()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = session.table_from_sql("select 1 as id").await?;
        let east = session.table_from_sql("select 2 as id").await?;
        let west = execute_output_request(
            west,
            "orders_output",
            "west_orders",
            LoadMode::AppendExisting,
        )?;
        let east = execute_output_request(
            east,
            "orders_output",
            "east_orders",
            LoadMode::AppendExisting,
        )?;

        let error = session
            .write_all(&[west, east])
            .await
            .map(|_| ())
            .map_err(|error| format!("{error:?} {error}"));

        assert!(
            matches!(error, Err(display) if display.contains("orders_output")
                && !display.contains("secret-token")
                && !display.contains("password")
                && !display.contains("server=tcp"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn source_reports_for_lazy_table_plan_include_provider_stats_without_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        let reports = session.source_reports_for_lazy_table_plan(&source).await?;

        assert_eq!(reports.len(), 1);
        let report = &reports[0];
        assert_eq!(report.source_name(), "orders");
        assert_eq!(report.provider_stats_reason(), None);
        let stats = report
            .provider_read_stats()
            .ok_or("expected provider read stats")?;
        assert_eq!(stats.source_name, "orders");
        assert_eq!(stats.snapshot_version, report.snapshot_version());
        assert_eq!(stats.files_started, 0);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.batches_produced, 0);
        assert_eq!(stats.rows_produced, 0);
        match stats.scan_metadata_exhausted {
            Some(true) => {
                assert_eq!(
                    report.file_count(),
                    crate::FileCount::exact(stats.files_planned)
                );
                assert_eq!(report.file_count_reason(), None);
                assert!(report.scan_metadata_exhausted());
            }
            Some(false) => {
                assert_eq!(
                    report.file_count(),
                    crate::FileCount::estimated(stats.files_planned)
                );
                assert_eq!(report.file_count_reason(), None);
                assert!(!report.scan_metadata_exhausted());
            }
            None => {
                assert_eq!(report.file_count(), crate::FileCount::unavailable());
                assert_eq!(
                    report.file_count_reason(),
                    Some(crate::ReportReasonCode::CapabilityUnavailable)
                );
                assert!(!report.scan_metadata_exhausted());
            }
        }
        Ok(())
    }
}
