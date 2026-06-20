//! One-output SQL Server sink orchestration.
//!
//! This module wires the already planned SQL Server target, connected client,
//! target lifecycle preparation, writer initialization, and batch write loop.

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use futures_util::Stream;

use crate::DeltaFunnelError;

use super::{
    LoadMode, MssqlBulkLoadWriter, MssqlPreparedTarget, MssqlTargetOutputPlan, MssqlWriteOptions,
    MssqlWriteReport,
};
use super::{
    connection::{
        MssqlConnectedOutputClient, MssqlOutputConnectionRequest, connect_mssql_output_client,
    },
    lifecycle_execution::prepare_mssql_target_lifecycle,
    write::write_mssql_batches_with_writer,
};

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
}

/// Writes one planned SQL Server output from a private connection request.
#[allow(dead_code)]
pub(crate) async fn write_mssql_output_connection_request<S>(
    request: MssqlOutputConnectionRequest,
    batches: S,
    options: MssqlWriteOptions,
) -> Result<MssqlWriteReport, DeltaFunnelError>
where
    S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send + Unpin,
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
    S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send + Unpin,
{
    ensure_supported_output_mode(&output_plan)?;
    let prepared_target = connection.prepare_target_lifecycle(&output_plan).await?;
    let writer = connection
        .initialize_writer(&output_plan, &prepared_target, options)
        .await?;

    write_mssql_batches_with_writer(&output_plan, batches, writer, options).await
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
        MssqlConnectionConfig, MssqlPreparedTargetAction, MssqlTargetConfig, MssqlTargetTable,
        MssqlWritePhase, default_mssql_write_options, plan_mssql_target_for_output,
    };

    #[derive(Default)]
    struct FakeSinkConnection {
        log: Arc<Mutex<Vec<String>>>,
        prepare_error: Option<DeltaFunnelError>,
        initialize_error: Option<DeltaFunnelError>,
    }

    #[derive(Default)]
    struct FakeSinkWriter {
        log: Arc<Mutex<Vec<String>>>,
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
                MssqlPreparedTargetAction::VerifiedExisting,
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

            Ok(arrow_tiberius::WriteStats {
                rows_written: u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                batches_written: 1,
            })
        }

        async fn finish(self) -> Result<arrow_tiberius::WriteStats, arrow_tiberius::Error> {
            self.record("finish")?;

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
        let connection = secret_connection()?;
        let target = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders")?);

        plan_mssql_target_for_output(
            orders_schema(),
            "orders_output",
            &target,
            Some(&connection),
            arrow_tiberius::PlanOptions::default(),
        )
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
        Ok(())
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
        assert_eq!(logged_events(&log)?, vec!["prepare", "initialize"]);
        Ok(())
    }
}
