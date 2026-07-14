//! Explicit runtime boundary for blocking host integrations.
//!
//! The core session API remains async-friendly. This wrapper owns the Tokio
//! runtime needed by synchronous hosts such as a future PyO3 package, while
//! provider, handoff, and sink modules stay async/pull-driven and do not own a
//! hidden process-level runtime.

use tokio::runtime::{Builder, Handle, Runtime};

#[cfg(test)]
use super::session::OrchestratorMssqlOutputWriter;
#[cfg(test)]
use crate::MssqlWorkflowOutputWriter;
use crate::{
    DeltaFunnelError, DeltaFunnelSession, DeltaSourceConfig, LazyTable, MssqlDryRunOutputReport,
    MssqlDryRunWorkflowReport, MssqlWriteReport, OutputWritePlan, PreviewOptions, TablePreview,
    WriteAllOptions, WriteAllReport, progress::ProgressReporter,
};

/// Blocking runtime boundary for high-level Delta Funnel session actions.
///
/// This type is intended to be owned by synchronous host bindings. Constructing
/// it only creates a Tokio runtime; it does not register sources, plan SQL,
/// execute DataFusion, contact SQL Server, or write rows.
///
/// The blocking methods are intended for non-async host threads. Rust async
/// callers should use [`DeltaFunnelSession`] async methods directly. Calling
/// these methods from inside an active Tokio runtime returns a configuration
/// error instead of relying on Tokio's nested-runtime panic.
pub struct DeltaFunnelRuntime {
    runtime: Runtime,
}

impl DeltaFunnelRuntime {
    /// Creates a multi-threaded Tokio runtime for blocking host integrations.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if Tokio cannot create the runtime.
    pub fn new() -> Result<Self, DeltaFunnelError> {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .thread_name("delta-funnel-runtime")
            .build()
            .map_err(|error| DeltaFunnelError::Config {
                message: format!("failed to create DeltaFunnel runtime: {error}"),
            })?;

        Ok(Self { runtime })
    }

    /// Runs async SQL table planning for a synchronous host.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`DeltaFunnelSession::table_from_sql`].
    pub fn table_from_sql(
        &self,
        session: &mut DeltaFunnelSession,
        sql: &str,
    ) -> Result<LazyTable, DeltaFunnelError> {
        reject_nested_runtime()?;
        self.runtime.block_on(session.table_from_sql(sql))
    }

    /// Registers one Delta source and reports its live lifecycle to the caller.
    ///
    /// Registration is synchronous and does not enter this type's Tokio
    /// runtime. The runtime wrapper exists as the public bridge used by
    /// synchronous host bindings.
    #[doc(hidden)]
    pub fn delta_lake_with_progress(
        &self,
        session: &mut DeltaFunnelSession,
        source: DeltaSourceConfig,
        reporter: ProgressReporter,
    ) -> Result<LazyTable, DeltaFunnelError> {
        session.delta_lake_with_progress(source, reporter)
    }

    /// Runs a bounded lazy table preview for a synchronous host.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`DeltaFunnelSession::preview_table`].
    pub fn preview_table(
        &self,
        session: &DeltaFunnelSession,
        table: &LazyTable,
        limit: usize,
    ) -> Result<TablePreview, DeltaFunnelError> {
        self.preview_table_with_options(session, table, PreviewOptions::new(limit))
    }

    /// Runs a bounded preview with explicit profiling options.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`DeltaFunnelSession::preview_table_with_options`].
    pub fn preview_table_with_options(
        &self,
        session: &DeltaFunnelSession,
        table: &LazyTable,
        options: PreviewOptions,
    ) -> Result<TablePreview, DeltaFunnelError> {
        reject_nested_runtime()?;
        self.runtime
            .block_on(session.preview_table_with_options(table, options))
    }

    /// Runs a bounded preview and reports its live lifecycle to the caller.
    #[doc(hidden)]
    pub fn preview_table_with_progress(
        &self,
        session: &DeltaFunnelSession,
        table: &LazyTable,
        limit: usize,
        reporter: ProgressReporter,
    ) -> Result<TablePreview, DeltaFunnelError> {
        reject_nested_runtime()?;
        self.runtime
            .block_on(session.preview_table_with_progress(table, limit, reporter))
    }

    /// Runs an option-bearing bounded preview with live progress reporting.
    #[doc(hidden)]
    pub fn preview_table_with_options_and_progress(
        &self,
        session: &DeltaFunnelSession,
        table: &LazyTable,
        options: PreviewOptions,
        reporter: ProgressReporter,
    ) -> Result<TablePreview, DeltaFunnelError> {
        reject_nested_runtime()?;
        self.runtime
            .block_on(session.preview_table_with_options_and_progress(table, options, reporter))
    }

    /// Runs a single-output dry run through the high-level session API.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`DeltaFunnelSession::dry_run_to_mssql`].
    pub fn dry_run_to_mssql(
        &self,
        session: &DeltaFunnelSession,
        request: &OutputWritePlan,
    ) -> Result<MssqlDryRunOutputReport, DeltaFunnelError> {
        reject_nested_runtime()?;
        session.dry_run_to_mssql(request)
    }

    /// Runs a single-output dry run with a workspace host progress reporter.
    #[doc(hidden)]
    pub fn dry_run_to_mssql_with_progress(
        &self,
        session: &DeltaFunnelSession,
        request: &OutputWritePlan,
        reporter: ProgressReporter,
    ) -> Result<MssqlDryRunOutputReport, DeltaFunnelError> {
        reject_nested_runtime()?;
        session.dry_run_to_mssql_with_reporter(request, reporter)
    }

    /// Runs a multi-output dry run through the high-level session API.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`DeltaFunnelSession::dry_run_all_to_mssql`].
    pub fn dry_run_all_to_mssql(
        &self,
        session: &DeltaFunnelSession,
        requests: &[OutputWritePlan],
    ) -> Result<MssqlDryRunWorkflowReport, DeltaFunnelError> {
        reject_nested_runtime()?;
        session.dry_run_all_to_mssql_with_tracing(requests)
    }

    /// Runs a multi-output dry run and reports its live progress to the caller.
    ///
    /// Progress describes work while it is happening. The returned report
    /// describes the completed dry-run result. Reporting progress does not
    /// change that result.
    #[doc(hidden)]
    pub fn dry_run_all_to_mssql_with_progress(
        &self,
        session: &DeltaFunnelSession,
        requests: &[OutputWritePlan],
        reporter: ProgressReporter,
    ) -> Result<MssqlDryRunWorkflowReport, DeltaFunnelError> {
        reject_nested_runtime()?;
        session.dry_run_all_to_mssql_with_observability(requests, Some(reporter))
    }

    /// Runs a multi-output dry run with source scan-summary options.
    ///
    /// # Errors
    ///
    /// Returns the same error as
    /// [`DeltaFunnelSession::dry_run_all_to_mssql_with_scan_summary`].
    pub fn dry_run_all_to_mssql_with_scan_summary(
        &self,
        session: &DeltaFunnelSession,
        requests: &[OutputWritePlan],
    ) -> Result<MssqlDryRunWorkflowReport, DeltaFunnelError> {
        reject_nested_runtime()?;
        self.runtime
            .block_on(session.dry_run_all_to_mssql_with_scan_summary_with_tracing(requests))
    }

    /// Blocks on one selected output write.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`DeltaFunnelSession::write_to_mssql`].
    pub fn write_to_mssql(
        &self,
        session: &DeltaFunnelSession,
        request: &OutputWritePlan,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        reject_nested_runtime()?;
        self.runtime.block_on(session.write_to_mssql(request))
    }

    /// Blocks on one selected output write with a workspace host progress reporter.
    #[doc(hidden)]
    pub fn write_to_mssql_with_progress(
        &self,
        session: &DeltaFunnelSession,
        request: &OutputWritePlan,
        reporter: ProgressReporter,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        reject_nested_runtime()?;
        self.runtime
            .block_on(session.write_to_mssql_with_reporter(request, reporter))
    }

    #[cfg(test)]
    pub(crate) fn write_to_mssql_with_writer<W>(
        &self,
        session: &DeltaFunnelSession,
        request: &OutputWritePlan,
        writer: &mut W,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>
    where
        W: OrchestratorMssqlOutputWriter,
    {
        reject_nested_runtime()?;
        self.runtime
            .block_on(session.write_to_mssql_with_writer(request, writer))
    }

    /// Blocks on the default multi-output write workflow.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`DeltaFunnelSession::write_all`].
    pub fn write_all(
        &self,
        session: &DeltaFunnelSession,
        requests: &[OutputWritePlan],
    ) -> Result<WriteAllReport, DeltaFunnelError> {
        reject_nested_runtime()?;
        self.runtime.block_on(session.write_all(requests))
    }

    /// Blocks on the multi-output write workflow with explicit options.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`DeltaFunnelSession::write_all_with_options`].
    pub fn write_all_with_options(
        &self,
        session: &DeltaFunnelSession,
        requests: &[OutputWritePlan],
        options: WriteAllOptions,
    ) -> Result<WriteAllReport, DeltaFunnelError> {
        reject_nested_runtime()?;
        self.runtime
            .block_on(session.write_all_with_options(requests, options))
    }

    /// Runs a multi-output write and reports its live progress to the caller.
    ///
    /// Progress describes work while it is happening. The returned report
    /// describes the completed per-output results. Reporting progress does not
    /// change those results.
    #[doc(hidden)]
    pub fn write_all_with_progress(
        &self,
        session: &DeltaFunnelSession,
        requests: &[OutputWritePlan],
        options: WriteAllOptions,
        reporter: ProgressReporter,
    ) -> Result<WriteAllReport, DeltaFunnelError> {
        reject_nested_runtime()?;
        self.runtime
            .block_on(session.write_all_with_progress(requests, options, reporter))
    }

    #[cfg(test)]
    pub(crate) fn write_all_with_writer<W>(
        &self,
        session: &DeltaFunnelSession,
        requests: &[OutputWritePlan],
        writer: W,
    ) -> Result<WriteAllReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        reject_nested_runtime()?;
        self.runtime
            .block_on(session.write_all_with_writer(requests, writer))
    }
}

fn reject_nested_runtime() -> Result<(), DeltaFunnelError> {
    if Handle::try_current().is_ok() {
        return Err(DeltaFunnelError::Config {
            message: "DeltaFunnelRuntime blocking methods cannot be called from inside an active Tokio runtime; use DeltaFunnelSession async APIs directly".to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::SchemaRef;
    use futures_util::StreamExt;

    use crate::{
        DeltaSourceConfig, DryRunScanSummaryMode, LoadMode, MssqlConnectionConfig,
        MssqlConnectionSource, MssqlOutputBatchStream, MssqlOutputTarget, MssqlTargetCleanupStatus,
        MssqlTargetConfig, MssqlTargetOutputPlan, MssqlTargetTable, MssqlWriteBackend,
        ResolvedMssqlTarget, RunMode, SessionOptions, ValidationOptions,
        table_formats::RealParquetDeltaTable,
    };

    fn secret_connection() -> Result<MssqlConnectionConfig, DeltaFunnelError> {
        Ok(MssqlConnectionConfig::new(
            "server=tcp:sql.example.com;database=warehouse;user=admin;password=secret-token",
        )?
        .with_display_label("warehouse-primary"))
    }

    fn output_request(
        table: LazyTable,
        output_name: &str,
        target_table: &str,
    ) -> Result<OutputWritePlan, DeltaFunnelError> {
        let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", target_table)?)
            .with_load_mode(LoadMode::AppendExisting);

        Ok(OutputWritePlan::new(
            table,
            MssqlOutputTarget::new(output_name, target, RunMode::DryRun),
        ))
    }

    fn execute_output_request(
        table: LazyTable,
        output_name: &str,
        target_table: &str,
        load_mode: LoadMode,
    ) -> Result<OutputWritePlan, DeltaFunnelError> {
        let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", target_table)?)
            .with_load_mode(load_mode);

        Ok(OutputWritePlan::new(
            table,
            MssqlOutputTarget::new(output_name, target, RunMode::Execute),
        ))
    }

    #[test]
    fn runtime_preview_table_returns_limited_formatted_rows() -> Result<(), DeltaFunnelError> {
        let runtime = DeltaFunnelRuntime::new()?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let table = runtime.table_from_sql(
            &mut session,
            "select 'open' as status union all select 'closed' as status order by status desc",
        )?;

        let preview = runtime.preview_table(&session, &table, 1)?;

        assert!(preview.text().contains("| status |"));
        assert!(
            preview
                .text()
                .lines()
                .any(|line| line.contains("| open   |"))
        );
        assert!(
            !preview
                .text()
                .lines()
                .any(|line| line.contains("| closed |"))
        );
        Ok(())
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct FakeRuntimeWriteCall {
        output_name: String,
        target_table: MssqlTargetTable,
        connection_source: MssqlConnectionSource,
        rows: u64,
        batches: u64,
        schema_fields: usize,
    }

    #[derive(Default)]
    struct FakeRuntimeWriter {
        calls: Vec<FakeRuntimeWriteCall>,
    }

    #[async_trait]
    impl OrchestratorMssqlOutputWriter for FakeRuntimeWriter {
        async fn write_output(
            &mut self,
            output_schema: SchemaRef,
            output_plan: MssqlTargetOutputPlan,
            resolved_target: ResolvedMssqlTarget,
            mut batches: MssqlOutputBatchStream,
            _write_backend: MssqlWriteBackend,
            _validation_options: ValidationOptions,
            _reporter: Option<&ProgressReporter>,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            let mut rows = 0_u64;
            let mut batch_count = 0_u64;

            while let Some(batch) = batches.next().await {
                let batch = batch?;
                rows = rows.saturating_add(u64::try_from(batch.num_rows()).map_err(|_| {
                    DeltaFunnelError::Config {
                        message: "fake runtime writer row count overflowed u64".to_owned(),
                    }
                })?);
                batch_count = batch_count.saturating_add(1);
            }

            self.calls.push(FakeRuntimeWriteCall {
                output_name: resolved_target.output_name().to_owned(),
                target_table: resolved_target.table().clone(),
                connection_source: resolved_target.connection_source(),
                rows,
                batches: batch_count,
                schema_fields: output_schema.fields().len(),
            });

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

    #[async_trait]
    impl MssqlWorkflowOutputWriter for FakeRuntimeWriter {
        async fn write_output(
            &mut self,
            output_schema: SchemaRef,
            resolved_target: ResolvedMssqlTarget,
            schema_options: crate::MssqlSchemaPlanOptions,
            batches: MssqlOutputBatchStream,
            write_backend: MssqlWriteBackend,
            validation_options: ValidationOptions,
            reporter: Option<&ProgressReporter>,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            let output_plan = crate::plan_mssql_target_for_resolved_output(
                output_schema.as_ref(),
                &resolved_target,
                schema_options,
            )?;

            OrchestratorMssqlOutputWriter::write_output(
                self,
                output_schema,
                output_plan,
                resolved_target,
                batches,
                write_backend,
                validation_options,
                reporter,
            )
            .await
        }
    }

    #[test]
    fn runtime_constructs_without_starting_session_work() -> Result<(), DeltaFunnelError> {
        let _runtime = DeltaFunnelRuntime::new()?;

        Ok(())
    }

    #[test]
    fn runtime_drives_table_sql_and_single_output_dry_run() -> Result<(), DeltaFunnelError> {
        let runtime = DeltaFunnelRuntime::new()?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let output = runtime.table_from_sql(&mut session, "select 1 as id")?;
        let request = output_request(output, "orders_output", "orders_sink")?;
        let deliveries = Arc::new(AtomicUsize::new(0));
        let callback_deliveries = Arc::clone(&deliveries);
        let reporter = ProgressReporter::new(move |_| {
            callback_deliveries.fetch_add(1, Ordering::Relaxed);
        });

        let report = runtime.dry_run_to_mssql_with_progress(&session, &request, reporter)?;

        assert_eq!(report.output_name(), "orders_output");
        assert_eq!(report.run_mode(), RunMode::DryRun);
        assert!(!report.sql_server_contacted());
        assert!(!report.row_production_started());
        assert_eq!(deliveries.load(Ordering::Relaxed), 3);
        Ok(())
    }

    #[test]
    fn runtime_drives_multi_output_dry_run() -> Result<(), DeltaFunnelError> {
        let runtime = DeltaFunnelRuntime::new()?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west = runtime.table_from_sql(&mut session, "select 1 as id")?;
        let east = runtime.table_from_sql(&mut session, "select 2 as id")?;
        let west = output_request(west, "west_output", "west_orders")?;
        let east = output_request(east, "east_output", "east_orders")?;

        let report = runtime.dry_run_all_to_mssql(&session, &[west, east])?;

        assert_eq!(report.len(), 2);
        assert_eq!(report.outputs()[0].output_name(), "west_output");
        assert_eq!(report.outputs()[1].output_name(), "east_output");
        assert!(!report.sql_server_contacted());
        assert!(!report.row_production_started());
        Ok(())
    }

    #[test]
    fn runtime_drives_dry_run_scan_summary() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = DeltaFunnelRuntime::new()?;
        let table = RealParquetDeltaTable::new_default("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new()
                .with_default_mssql_connection(secret_connection()?)
                .with_validation_options(
                    ValidationOptions::new()
                        .with_dry_run_scan_summary_mode(DryRunScanSummaryMode::ExhaustScanMetadata),
                ),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let request = output_request(source, "orders_output", "orders_sink")?;

        let report = runtime.dry_run_all_to_mssql_with_scan_summary(&session, &[request])?;

        assert_eq!(report.sources().len(), 1);
        assert!(report.sources()[0].provider_read_stats().is_some());
        assert!(!report.row_production_started());
        Ok(())
    }

    #[test]
    fn runtime_preserves_sanitized_session_errors() -> Result<(), DeltaFunnelError> {
        let runtime = DeltaFunnelRuntime::new()?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let output = runtime.table_from_sql(&mut session, "select 1 as id")?;
        let request = output_request(output, "orders_output", "orders_sink")?;

        let error = runtime.dry_run_to_mssql(&session, &request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "orders_output"
        ));
        Ok(())
    }

    #[test]
    fn runtime_drives_single_output_write_with_injected_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = DeltaFunnelRuntime::new()?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let output =
            runtime.table_from_sql(&mut session, "select 1 as id union all select 2 as id")?;
        let request = execute_output_request(
            output,
            "orders_output",
            "orders_sink",
            LoadMode::CreateAndLoad,
        )?;
        let mut writer = FakeRuntimeWriter::default();

        let report = runtime.write_to_mssql_with_writer(&session, &request, &mut writer)?;

        assert_eq!(writer.calls.len(), 1);
        let call = writer
            .calls
            .first()
            .ok_or("expected fake runtime writer call")?;
        assert_eq!(call.output_name, "orders_output");
        assert_eq!(call.target_table.table(), "orders_sink");
        assert_eq!(
            call.connection_source,
            MssqlConnectionSource::ContextDefault
        );
        assert_eq!(call.rows, 2);
        assert!(call.batches >= 1);
        assert_eq!(call.schema_fields, 1);
        assert_eq!(report.output_name(), "orders_output");
        assert_eq!(report.stats().rows_written(), 2);
        assert_eq!(report.stats().batches_written(), call.batches);
        Ok(())
    }

    #[test]
    fn runtime_drives_multi_output_write_with_injected_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = DeltaFunnelRuntime::new()?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let west =
            runtime.table_from_sql(&mut session, "select 1 as id union all select 2 as id")?;
        let east = runtime.table_from_sql(&mut session, "select 3 as id")?;
        let west =
            execute_output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;
        let east =
            execute_output_request(east, "east_output", "east_orders", LoadMode::CreateAndLoad)?;
        let writer = FakeRuntimeWriter::default();

        let report = runtime.write_all_with_writer(&session, &[west, east], writer)?;

        assert_eq!(report.len(), 2);
        assert!(report.all_succeeded());
        assert_eq!(report.outputs()[0].output_name(), "west_output");
        assert_eq!(report.outputs()[1].output_name(), "east_output");
        let crate::MssqlOutputWriteStatus::Succeeded(west_report) = &report.outputs()[0] else {
            return Err(format!("expected succeeded status, got {:?}", report.outputs()[0]).into());
        };
        let crate::MssqlOutputWriteStatus::Succeeded(east_report) = &report.outputs()[1] else {
            return Err(format!("expected succeeded status, got {:?}", report.outputs()[1]).into());
        };
        assert_eq!(west_report.stats().rows_written(), 2);
        assert_eq!(east_report.stats().rows_written(), 1);
        Ok(())
    }

    #[test]
    fn runtime_rejects_blocking_calls_inside_active_tokio_runtime()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = DeltaFunnelRuntime::new()?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let output = runtime.table_from_sql(&mut session, "select 1 as id")?;
        let request = output_request(output, "orders_output", "orders_sink")?;
        let host_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let error = host_runtime.block_on(async { runtime.dry_run_to_mssql(&session, &request) });

        assert!(matches!(
            error,
            Err(DeltaFunnelError::Config { message })
                if message.contains("cannot be called from inside an active Tokio runtime")
                    && message.contains("DeltaFunnelSession async APIs")
        ));
        Ok(())
    }

    #[test]
    fn lower_level_modules_do_not_create_hidden_tokio_runtimes() {
        let low_level_sources = [
            include_str!("../pipeline/batch_handoff.rs"),
            include_str!("../query_engine/datafusion/execution.rs"),
            include_str!("../query_engine/datafusion/execution/planning_exec.rs"),
            include_str!("../sql_server/execution/connection.rs"),
            include_str!("../sql_server/execution/sink.rs"),
            include_str!("../sql_server/execution/workflow.rs"),
            include_str!("../sql_server/execution/write.rs"),
        ];

        for source in low_level_sources {
            assert!(!source.contains("tokio::runtime"));
            assert!(!source.contains("Runtime::new"));
            assert!(!source.contains("block_on"));
        }
    }
}
