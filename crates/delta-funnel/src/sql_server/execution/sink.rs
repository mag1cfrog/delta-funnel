//! One-output SQL Server sink orchestration.
//!
//! This module wires the already planned SQL Server target, connected client,
//! target lifecycle preparation, writer initialization, and batch write loop.

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use futures_util::Stream;

use crate::{DeltaFunnelError, report::sql_server::MssqlWriteReportMetrics};

use super::{
    LoadMode, MssqlBulkLoadWriter, MssqlPreparedTarget, MssqlSchemaPlanOptions,
    MssqlTargetCleanupStatus, MssqlTargetOutputPlan, MssqlWriteOptions, MssqlWriteReport,
    ResolvedMssqlTarget,
};
use super::{
    connection::{
        MssqlConnectedOutputClient, MssqlOutputConnectionRequest, connect_mssql_output_client,
        plan_mssql_output_connection_request,
    },
    lifecycle::{cleanup_mssql_prepared_target, prepare_mssql_target_lifecycle},
    write::write_mssql_batches_with_writer,
};

/// Writes one resolved output to SQL Server from an Arrow record batch stream.
///
/// Use this when the caller has already selected one output, resolved its SQL Server target,
/// and can provide the output schema plus a stream of `RecordBatch` values for that output.
/// The function plans the private connection request, opens the SQL Server connection,
/// prepares the target table lifecycle, initializes the bulk writer, writes each batch, and
/// returns a redacted `MssqlWriteReport`.
///
/// The batch stream must already match the planned output schema. This API does not load Delta
/// tables, run DataFusion queries, choose among multiple outputs, retry failed writes, or perform
/// destructive replace behavior. `LoadMode::Replace` is rejected before connecting to SQL Server.
/// Connection string material stays inside the resolved target and private connection request;
/// reports and errors use the redacted connection summary.
pub async fn write_output_batches_to_mssql<S>(
    output_schema: impl AsRef<arrow_schema::Schema>,
    resolved_target: ResolvedMssqlTarget,
    schema_options: MssqlSchemaPlanOptions,
    batches: S,
    write_options: MssqlWriteOptions,
) -> Result<MssqlWriteReport, DeltaFunnelError>
where
    S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send,
{
    let request =
        plan_mssql_output_connection_request(output_schema, resolved_target, schema_options)?;

    write_mssql_output_connection_request(request, batches, write_options).await
}

/// Connected one-output SQL Server sink boundary.
#[allow(dead_code)]
#[async_trait]
pub(crate) trait MssqlOneOutputSinkConnection: Send {
    /// Writer type initialized from this connection.
    type Writer<'connection>: MssqlBulkLoadWriter
    where
        Self: 'connection;

    /// Prepares the target lifecycle before writer construction.
    async fn prepare_target_lifecycle(
        &mut self,
        output_plan: &MssqlTargetOutputPlan,
    ) -> Result<MssqlPreparedTarget, DeltaFunnelError>;

    /// Initializes the writer after target lifecycle preparation succeeds.
    async fn initialize_writer<'connection>(
        &'connection mut self,
        output_plan: &MssqlTargetOutputPlan,
        prepared_target: &MssqlPreparedTarget,
        options: MssqlWriteOptions,
    ) -> Result<Self::Writer<'connection>, DeltaFunnelError>;

    /// Cleans up a prepared target after a later failure.
    async fn cleanup_prepared_target(
        &mut self,
        output_plan: &MssqlTargetOutputPlan,
        prepared_target: Option<&MssqlPreparedTarget>,
    ) -> Result<MssqlTargetCleanupStatus, DeltaFunnelError>;

    /// Initializes the writer and writes batches after lifecycle preparation.
    ///
    /// The concrete production writer borrows the connected SQL Server client while it is alive.
    /// Keeping writer initialization and the write loop inside this method prevents that borrowed
    /// writer type from escaping into the outer orchestration scope. After this method returns,
    /// the caller can safely borrow the same connection again to clean up a prepared target after
    /// either writer initialization or batch writing fails.
    async fn write_prepared_batches<S>(
        &mut self,
        output_plan: &MssqlTargetOutputPlan,
        prepared_target: &MssqlPreparedTarget,
        batches: S,
        options: MssqlWriteOptions,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>
    where
        S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send,
    {
        let writer = self
            .initialize_writer(output_plan, prepared_target, options)
            .await?;

        write_mssql_batches_with_writer(output_plan, batches, writer, options).await
    }
}

#[async_trait]
impl MssqlOneOutputSinkConnection for MssqlConnectedOutputClient {
    type Writer<'connection>
        = arrow_tiberius::ConnectedBulkWriter<'connection>
    where
        Self: 'connection;

    async fn prepare_target_lifecycle(
        &mut self,
        output_plan: &MssqlTargetOutputPlan,
    ) -> Result<MssqlPreparedTarget, DeltaFunnelError> {
        let mut lifecycle_client = self.lifecycle_client();

        prepare_mssql_target_lifecycle(output_plan, &mut lifecycle_client).await
    }

    async fn initialize_writer<'connection>(
        &'connection mut self,
        _output_plan: &MssqlTargetOutputPlan,
        prepared_target: &MssqlPreparedTarget,
        options: MssqlWriteOptions,
    ) -> Result<Self::Writer<'connection>, DeltaFunnelError> {
        self.initialize_bulk_writer(prepared_target, options).await
    }

    async fn cleanup_prepared_target(
        &mut self,
        output_plan: &MssqlTargetOutputPlan,
        prepared_target: Option<&MssqlPreparedTarget>,
    ) -> Result<MssqlTargetCleanupStatus, DeltaFunnelError> {
        let mut lifecycle_client = self.lifecycle_client();

        cleanup_mssql_prepared_target(output_plan, prepared_target, &mut lifecycle_client).await
    }
}

/// Writes one planned SQL Server output from a private connection request.
#[allow(dead_code)]
pub(crate) async fn write_mssql_output_connection_request<S>(
    request: MssqlOutputConnectionRequest,
    batches: S,
    options: MssqlWriteOptions,
) -> Result<MssqlWriteReport, DeltaFunnelError>
where
    S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send,
{
    let output_plan = request.output_plan().clone();
    ensure_supported_output_mode(&output_plan)?;
    let connection = connect_mssql_output_client(request).await?;

    write_mssql_output_batches_on_connection(output_plan, connection, batches, options).await
}

/// Writes one planned SQL Server output through an already connected boundary.
#[allow(dead_code)]
pub(crate) async fn write_mssql_output_batches_on_connection<C, S>(
    output_plan: MssqlTargetOutputPlan,
    mut connection: C,
    batches: S,
    options: MssqlWriteOptions,
) -> Result<MssqlWriteReport, DeltaFunnelError>
where
    C: MssqlOneOutputSinkConnection,
    S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send,
{
    ensure_supported_output_mode(&output_plan)?;
    let prepared_target = connection.prepare_target_lifecycle(&output_plan).await?;

    match connection
        .write_prepared_batches(&output_plan, &prepared_target, batches, options)
        .await
    {
        Ok(report) => Ok(write_report_with_cleanup(
            &report,
            prepared_target.report().cleanup(),
        )),
        Err(error) => Err(cleanup_after_prepared_target_failure(
            &mut connection,
            &output_plan,
            &prepared_target,
            error,
        )
        .await),
    }
}

fn ensure_supported_output_mode(
    output_plan: &MssqlTargetOutputPlan,
) -> Result<(), DeltaFunnelError> {
    if output_plan.load_mode() != LoadMode::Replace {
        return Ok(());
    }

    Err(DeltaFunnelError::MssqlLifecyclePlanning {
        output_name: output_plan.output_name().to_owned(),
        message: "replace load mode is reserved and cannot write one MSSQL output".to_owned(),
    })
}

async fn cleanup_after_prepared_target_failure<C>(
    connection: &mut C,
    output_plan: &MssqlTargetOutputPlan,
    prepared_target: &MssqlPreparedTarget,
    original_error: DeltaFunnelError,
) -> DeltaFunnelError
where
    C: MssqlOneOutputSinkConnection,
{
    match connection
        .cleanup_prepared_target(output_plan, Some(prepared_target))
        .await
    {
        Ok(cleanup) => error_with_cleanup(output_plan, original_error, cleanup),
        Err(cleanup_error) => {
            error_with_cleanup_failure(output_plan, original_error, cleanup_error)
        }
    }
}

fn write_report_with_cleanup(
    report: &MssqlWriteReport,
    cleanup: MssqlTargetCleanupStatus,
) -> MssqlWriteReport {
    report.clone().with_cleanup(cleanup)
}

fn error_with_cleanup(
    output_plan: &MssqlTargetOutputPlan,
    error: DeltaFunnelError,
    cleanup: MssqlTargetCleanupStatus,
) -> DeltaFunnelError {
    match error {
        DeltaFunnelError::MssqlWritePhase { context, message } => {
            DeltaFunnelError::MssqlWritePhase {
                context: Box::new(context_with_cleanup(output_plan, context.as_ref(), cleanup)),
                message,
            }
        }
        DeltaFunnelError::MssqlBatchSchemaValidation { context, source } => {
            DeltaFunnelError::MssqlBatchSchemaValidation {
                context: Box::new(context_with_cleanup(output_plan, context.as_ref(), cleanup)),
                source,
            }
        }
        other => other,
    }
}

fn error_with_cleanup_failure(
    output_plan: &MssqlTargetOutputPlan,
    error: DeltaFunnelError,
    cleanup_error: DeltaFunnelError,
) -> DeltaFunnelError {
    match error {
        DeltaFunnelError::MssqlWritePhase { context, message } => {
            DeltaFunnelError::MssqlWritePhase {
                context: Box::new(context_with_cleanup(
                    output_plan,
                    context.as_ref(),
                    MssqlTargetCleanupStatus::Failed,
                )),
                message: format!("{message}; cleanup failed: {cleanup_error}"),
            }
        }
        DeltaFunnelError::MssqlBatchSchemaValidation { context, source } => {
            DeltaFunnelError::MssqlWritePhase {
                context: Box::new(context_with_cleanup(
                    output_plan,
                    context.as_ref(),
                    MssqlTargetCleanupStatus::Failed,
                )),
                message: format!(
                    "batch schema validation failed: {source}; cleanup failed: {cleanup_error}"
                ),
            }
        }
        other => other,
    }
}

fn context_with_cleanup(
    output_plan: &MssqlTargetOutputPlan,
    context: &crate::MssqlWriteFailureContext,
    cleanup: MssqlTargetCleanupStatus,
) -> crate::MssqlWriteFailureContext {
    crate::MssqlWriteFailureContext::from_output_plan_with_metrics(
        output_plan,
        context.phase(),
        MssqlWriteReportMetrics::new(
            context.output_row_count(),
            context.batch_shaping(),
            context.stats().rows_written(),
            context.stats().batches_written(),
            context.stats().elapsed_ms(),
            context.partial_write_possible(),
            cleanup,
        )
        .with_phase_timings(context.phase_timings().to_vec()),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use datafusion::arrow::{
        array::{Int64Array, StringArray},
        record_batch::RecordBatch,
    };
    use futures_util::stream;

    use super::*;
    use crate::{
        MssqlConnectionConfig, MssqlPreparedTargetAction, MssqlTargetConfig,
        MssqlTargetResolutionContext, MssqlTargetTable, MssqlWritePhase, PhaseStatus,
        default_mssql_write_options, plan_mssql_target_for_output,
    };

    #[derive(Default)]
    struct FakeSinkConnection {
        log: Arc<Mutex<Vec<String>>>,
        prepare_error: Option<DeltaFunnelError>,
        initialize_error: Option<DeltaFunnelError>,
        cleanup_error: Option<DeltaFunnelError>,
        fail_write: bool,
        fail_finish: bool,
    }

    #[derive(Default)]
    struct FakeSinkWriter {
        log: Arc<Mutex<Vec<String>>>,
        fail_write: bool,
        fail_finish: bool,
    }

    impl FakeSinkConnection {
        fn with_log(log: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                log,
                ..Self::default()
            }
        }

        fn fail_prepare(mut self, error: DeltaFunnelError) -> Self {
            self.prepare_error = Some(error);
            self
        }

        fn fail_initialize(mut self, error: DeltaFunnelError) -> Self {
            self.initialize_error = Some(error);
            self
        }

        fn fail_cleanup(mut self, error: DeltaFunnelError) -> Self {
            self.cleanup_error = Some(error);
            self
        }

        fn fail_write(mut self) -> Self {
            self.fail_write = true;
            self
        }

        fn fail_finish(mut self) -> Self {
            self.fail_finish = true;
            self
        }

        fn record(&self, event: impl Into<String>) -> Result<(), DeltaFunnelError> {
            self.log
                .lock()
                .map_err(|_| DeltaFunnelError::Config {
                    message: "fake sink log mutex was poisoned".to_owned(),
                })?
                .push(event.into());
            Ok(())
        }
    }

    impl FakeSinkWriter {
        fn record(&self, event: impl Into<String>) -> Result<(), arrow_tiberius::Error> {
            self.log
                .lock()
                .map_err(|_| arrow_tiberius::Error::BackendUnavailable {
                    backend: arrow_tiberius::WriteBackend::DirectRawBulk,
                    reason: "fake sink log mutex was poisoned".to_owned(),
                })?
                .push(event.into());
            Ok(())
        }
    }

    #[async_trait]
    impl MssqlOneOutputSinkConnection for FakeSinkConnection {
        type Writer<'connection>
            = FakeSinkWriter
        where
            Self: 'connection;

        async fn prepare_target_lifecycle(
            &mut self,
            output_plan: &MssqlTargetOutputPlan,
        ) -> Result<MssqlPreparedTarget, DeltaFunnelError> {
            self.record("prepare")?;
            if let Some(error) = self.prepare_error.take() {
                return Err(error);
            }

            MssqlPreparedTarget::from_output_plan(
                output_plan,
                match output_plan.load_mode() {
                    LoadMode::AppendExisting => MssqlPreparedTargetAction::VerifiedExisting,
                    LoadMode::CreateAndLoad => MssqlPreparedTargetAction::CreatedTable,
                    LoadMode::Replace => MssqlPreparedTargetAction::VerifiedExisting,
                },
            )
        }

        async fn initialize_writer<'connection>(
            &'connection mut self,
            _output_plan: &MssqlTargetOutputPlan,
            _prepared_target: &MssqlPreparedTarget,
            _options: MssqlWriteOptions,
        ) -> Result<Self::Writer<'connection>, DeltaFunnelError> {
            self.record("initialize")?;
            if let Some(error) = self.initialize_error.take() {
                return Err(error);
            }

            Ok(FakeSinkWriter {
                log: Arc::clone(&self.log),
                fail_write: self.fail_write,
                fail_finish: self.fail_finish,
            })
        }

        async fn cleanup_prepared_target(
            &mut self,
            _output_plan: &MssqlTargetOutputPlan,
            prepared_target: Option<&MssqlPreparedTarget>,
        ) -> Result<MssqlTargetCleanupStatus, DeltaFunnelError> {
            let Some(prepared_target) = prepared_target else {
                self.record("cleanup none")?;
                return Ok(MssqlTargetCleanupStatus::NotAttempted);
            };

            self.record(format!("cleanup {:?}", prepared_target.report().action()))?;
            if let Some(error) = self.cleanup_error.take() {
                return Err(error);
            }

            Ok(match prepared_target.report().action() {
                MssqlPreparedTargetAction::VerifiedExisting => {
                    MssqlTargetCleanupStatus::NotApplicable
                }
                MssqlPreparedTargetAction::CreatedTable => MssqlTargetCleanupStatus::Succeeded,
            })
        }
    }

    #[async_trait]
    impl MssqlBulkLoadWriter for FakeSinkWriter {
        async fn write_batch(
            &mut self,
            batch: &RecordBatch,
        ) -> Result<arrow_tiberius::WriteStats, arrow_tiberius::Error> {
            self.record(format!("write {}", batch.num_rows()))?;
            if self.fail_write {
                return Err(arrow_tiberius::Error::BackendUnavailable {
                    backend: arrow_tiberius::WriteBackend::DirectRawBulk,
                    reason: "fake sink writer failed on write".to_owned(),
                });
            }

            Ok(arrow_tiberius::WriteStats {
                rows_written: u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                batches_written: 1,
            })
        }

        async fn finish(self) -> Result<arrow_tiberius::WriteStats, arrow_tiberius::Error> {
            self.record("finish")?;
            if self.fail_finish {
                return Err(arrow_tiberius::Error::BackendUnavailable {
                    backend: arrow_tiberius::WriteBackend::DirectRawBulk,
                    reason: "fake sink writer failed on finish".to_owned(),
                });
            }

            Ok(arrow_tiberius::WriteStats {
                rows_written: 0,
                batches_written: 0,
            })
        }
    }

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

    fn output_plan() -> Result<MssqlTargetOutputPlan, DeltaFunnelError> {
        output_plan_with_load_mode(LoadMode::AppendExisting)
    }

    fn output_plan_with_load_mode(
        load_mode: LoadMode,
    ) -> Result<MssqlTargetOutputPlan, DeltaFunnelError> {
        let connection = secret_connection()?;
        let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?)
            .with_load_mode(load_mode);

        plan_mssql_target_for_output(
            orders_schema(),
            "orders_output",
            &target,
            Some(&connection),
            arrow_tiberius::PlanOptions::default(),
        )
    }

    fn resolved_target_with_load_mode(
        load_mode: LoadMode,
        connection: &MssqlConnectionConfig,
    ) -> Result<ResolvedMssqlTarget, DeltaFunnelError> {
        MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?)
            .with_load_mode(load_mode)
            .resolve(MssqlTargetResolutionContext {
                output_name: Some("orders_output"),
                default_connection: Some(connection),
            })
    }

    fn orders_batch(row_count: usize) -> Result<RecordBatch, DeltaFunnelError> {
        let order_ids = (0..row_count)
            .map(|value| i64::try_from(value).unwrap_or(i64::MAX))
            .collect::<Vec<_>>();
        let statuses = (0..row_count).map(|_| Some("open")).collect::<Vec<_>>();

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

    fn invalid_orders_batch(row_count: usize) -> Result<RecordBatch, DeltaFunnelError> {
        let statuses = (0..row_count).map(|_| Some("open")).collect::<Vec<_>>();

        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "status",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(statuses))],
        )
        .map_err(|error| DeltaFunnelError::Config {
            message: error.to_string(),
        })
    }

    fn logged_events(log: &Arc<Mutex<Vec<String>>>) -> Result<Vec<String>, DeltaFunnelError> {
        log.lock()
            .map(|events| events.clone())
            .map_err(|_| DeltaFunnelError::Config {
                message: "fake sink log mutex was poisoned".to_owned(),
            })
    }

    fn phase_error(
        output_plan: &MssqlTargetOutputPlan,
        phase: MssqlWritePhase,
        message: &str,
    ) -> DeltaFunnelError {
        DeltaFunnelError::MssqlWritePhase {
            context: Box::new(crate::MssqlWriteFailureContext::from_output_plan(
                output_plan,
                phase,
                0,
                0,
                0,
                false,
                crate::MssqlTargetCleanupStatus::NotApplicable,
            )),
            message: message.to_owned(),
        }
    }

    #[tokio::test]
    async fn one_output_sink_runs_lifecycle_before_writer_initialization()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan()?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log));
        let batches = stream::iter(vec![Ok(orders_batch(2)?), Ok(orders_batch(1)?)]);

        let report = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await?;

        assert_eq!(
            logged_events(&log)?,
            vec!["prepare", "initialize", "write 2", "write 1", "finish"]
        );
        assert_eq!(report.output_name(), "orders_output");
        assert_eq!(report.stats().rows_written(), 3);
        assert_eq!(report.stats().batches_written(), 2);
        let write_batch_timing = report
            .phase_timings()
            .iter()
            .find(|timing| timing.phase_name() == "write_batch")
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "missing write_batch phase timing".to_owned(),
            })?;
        assert_eq!(write_batch_timing.status(), PhaseStatus::completed());
        Ok(())
    }

    #[tokio::test]
    async fn public_one_output_api_rejects_replace_before_connection()
    -> Result<(), DeltaFunnelError> {
        let connection = secret_connection()?;
        let resolved_target = resolved_target_with_load_mode(LoadMode::Replace, &connection)?;
        let batches = stream::once(async { orders_batch(1) });

        let error = write_output_batches_to_mssql(
            orders_schema(),
            resolved_target,
            arrow_tiberius::PlanOptions::default(),
            batches,
            default_mssql_write_options(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected replace load mode to fail before connection".to_owned(),
        })?;

        let error_message = error.to_string();
        let error_debug = format!("{error:?}");
        let DeltaFunnelError::MssqlLifecyclePlanning {
            output_name,
            message,
        } = &error
        else {
            return Err(DeltaFunnelError::Config {
                message: format!("expected MssqlLifecyclePlanning error, got {error:?}"),
            });
        };
        assert_eq!(output_name, "orders_output");
        assert!(message.contains("replace"));
        assert!(error_message.contains("replace"));
        assert!(!error_message.contains("secret-token"));
        assert!(!error_debug.contains("secret-token"));

        Ok(())
    }

    #[test]
    fn public_one_output_sink_stays_before_delta_reads_and_datafusion_execution() {
        let source = include_str!("sink.rs");
        let forbidden_patterns = [
            concat!("load", "_delta", "_source"),
            concat!("load", "_delta", "_sources"),
            concat!("datafusion", "_query", "_output", "_stream"),
            concat!("datafusion", "_session", "_context"),
            concat!("handoff", "_datafusion", "_query", "_output"),
            concat!("Delta", "Table", "Provider"),
            concat!("Session", "Context"),
            concat!("Data", "Frame"),
        ];

        for pattern in forbidden_patterns {
            assert!(!source.contains(pattern), "unexpected `{pattern}`");
        }
    }

    #[tokio::test]
    async fn one_output_sink_stops_before_writer_initialization_when_lifecycle_fails()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan()?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log)).fail_prepare(phase_error(
            &output_plan,
            MssqlWritePhase::PrepareTargetLifecycle,
            "prepare failed",
        ));
        let batches = stream::iter(vec![Ok(orders_batch(1)?)]);

        let error = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected lifecycle failure".to_owned(),
        })?;

        assert!(error.to_string().contains("prepare failed"));
        assert_eq!(logged_events(&log)?, vec!["prepare"]);
        Ok(())
    }

    #[tokio::test]
    async fn one_output_sink_stops_before_batch_polling_when_writer_initialization_fails()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan()?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection =
            FakeSinkConnection::with_log(Arc::clone(&log)).fail_initialize(phase_error(
                &output_plan,
                MssqlWritePhase::InitializeWriter,
                "initialize failed",
            ));
        let batches = stream::iter(vec![Ok(orders_batch(1)?)]);

        let error = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected initialization failure".to_owned(),
        })?;

        assert!(error.to_string().contains("initialize failed"));
        assert_eq!(
            logged_events(&log)?,
            vec!["prepare", "initialize", "cleanup VerifiedExisting"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn one_output_sink_cleans_up_created_target_after_writer_initialization_failure()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::CreateAndLoad)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection =
            FakeSinkConnection::with_log(Arc::clone(&log)).fail_initialize(phase_error(
                &output_plan,
                MssqlWritePhase::InitializeWriter,
                "initialize failed",
            ));
        let batches = stream::iter(vec![Ok(orders_batch(1)?)]);

        let error = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected initialization failure".to_owned(),
        })?;

        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected MssqlWritePhase initialization failure".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::InitializeWriter);
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::Succeeded);
        assert!(message.contains("initialize failed"));
        assert_eq!(
            logged_events(&log)?,
            vec!["prepare", "initialize", "cleanup CreatedTable"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn one_output_sink_cleans_up_created_target_after_stream_failure()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::CreateAndLoad)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log));
        let batches = stream::iter(vec![Err(DeltaFunnelError::Config {
            message: "stream failed".to_owned(),
        })]);

        let error = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected stream failure".to_owned(),
        })?;

        let DeltaFunnelError::MssqlWritePhase { context, .. } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected write phase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::PollBatchStream);
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::Succeeded);
        let poll_timing = context
            .phase_timings()
            .iter()
            .find(|timing| timing.phase_name() == "poll_batch_stream")
            .ok_or_else(|| DeltaFunnelError::Config {
                message: "missing poll_batch_stream phase timing".to_owned(),
            })?;
        assert_eq!(poll_timing.status(), PhaseStatus::failed());
        assert_eq!(
            logged_events(&log)?,
            vec!["prepare", "initialize", "cleanup CreatedTable"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn one_output_sink_cleans_up_created_target_after_schema_validation_failure()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::CreateAndLoad)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log));
        let batches = stream::iter(vec![Ok(invalid_orders_batch(1)?)]);

        let error = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected schema validation failure".to_owned(),
        })?;

        let DeltaFunnelError::MssqlBatchSchemaValidation { context, .. } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected MssqlBatchSchemaValidation error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::ValidateBatchSchema);
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::Succeeded);
        assert_eq!(
            logged_events(&log)?,
            vec!["prepare", "initialize", "cleanup CreatedTable"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn one_output_sink_cleans_up_created_target_after_write_failure()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::CreateAndLoad)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log)).fail_write();
        let batches = stream::iter(vec![Ok(orders_batch(1)?)]);

        let error = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected write failure".to_owned(),
        })?;

        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected MssqlWritePhase write failure".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::WriteBatch);
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::Succeeded);
        assert!(message.contains("fake sink writer failed on write"));
        assert_eq!(
            logged_events(&log)?,
            vec!["prepare", "initialize", "write 1", "cleanup CreatedTable"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn one_output_sink_cleans_up_created_target_after_finalize_failure()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::CreateAndLoad)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log)).fail_finish();
        let batches = stream::iter(vec![Ok(orders_batch(1)?)]);

        let error = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected finalize failure".to_owned(),
        })?;

        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected MssqlWritePhase finalize failure".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::Finalize);
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::Succeeded);
        assert!(message.contains("fake sink writer failed on finish"));
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "initialize",
                "write 1",
                "finish",
                "cleanup CreatedTable"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn one_output_sink_preserves_original_failure_when_cleanup_fails()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::CreateAndLoad)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log)).fail_cleanup(phase_error(
            &output_plan,
            MssqlWritePhase::Cleanup,
            "cleanup failed",
        ));
        let batches = stream::iter(vec![Err(DeltaFunnelError::Config {
            message: "stream failed".to_owned(),
        })]);

        let error = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected stream failure".to_owned(),
        })?;
        let display = error.to_string();

        assert!(display.contains("stream failed"));
        assert!(display.contains("cleanup failed"));
        let DeltaFunnelError::MssqlWritePhase { context, .. } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected write phase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::PollBatchStream);
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::Failed);
        assert_eq!(
            logged_events(&log)?,
            vec!["prepare", "initialize", "cleanup CreatedTable"]
        );
        Ok(())
    }
}
