use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use tracing::Instrument;

use crate::{
    DeltaFunnelError, MssqlOutputBatchStream, MssqlTargetOutputPlan, MssqlWriteBackend,
    MssqlWriteReport, PhaseTimingReport, ReportReasonCode, ResolvedMssqlTarget, ValidationOptions,
    observability, plan_mssql_target_for_resolved_output,
    progress::{ProgressEvent, ProgressOperation, ProgressPhase, ProgressReporter},
    report::PhaseTimer,
    write_output_batches_to_mssql_for_workflow,
};

use super::super::{DeltaFunnelSession, OutputWritePlan, PlannedMssqlOutput, RunMode};

pub(in crate::orchestrator::session) const OUTPUT_SCHEMA_PLANNING_PHASE: &str =
    "output_schema_planning";
pub(in crate::orchestrator::session) const SQL_TARGET_PLANNING_PHASE: &str = "sql_target_planning";
const VALIDATION_PHASE: &str = "validation";

impl DeltaFunnelSession {
    /// Plans one lazy table as an MSSQL output without executing the table.
    ///
    /// The selected table must be owned by this session. The method uses the
    /// table's logical Arrow schema, resolves the effective SQL Server
    /// connection from the output override or session default, and reuses the
    /// SQL Server schema, DDL, and lifecycle planning rules. It intentionally
    /// performs no SQL Server I/O, physical DataFusion planning, row reads,
    /// batch streaming, or writer construction.
    ///
    /// # Errors
    ///
    /// Returns an MSSQL planning error when the selected table is not known to
    /// this session, the target has no effective connection, the output or
    /// target config is invalid, the schema cannot be mapped to SQL Server, or
    /// the requested load mode is not supported by the current target planner.
    pub fn plan_mssql_output(
        &self,
        request: &OutputWritePlan,
    ) -> Result<PlannedMssqlOutput, DeltaFunnelError> {
        let mut phase_timings = self.phase_timings_for_lazy_table(request.table())?;

        let schema_timer = PhaseTimer::start(OUTPUT_SCHEMA_PLANNING_PHASE);
        let schema = match self.schema_for_lazy_table(request.table()) {
            Ok(schema) => {
                phase_timings.push(schema_timer.completed());
                schema
            }
            Err(error) => {
                phase_timings.push(schema_timer.failed());
                return Err(error);
            }
        };

        let target_timer = PhaseTimer::start(SQL_TARGET_PLANNING_PHASE);
        let resolved_target =
            match request
                .target()
                .target()
                .resolve(crate::MssqlTargetResolutionContext {
                    output_name: Some(request.target().output_name()),
                    default_connection: self.options.default_mssql_connection(),
                }) {
                Ok(resolved_target) => resolved_target,
                Err(error) => {
                    phase_timings.push(target_timer.failed());
                    return Err(error);
                }
            };
        let output_plan = match plan_mssql_target_for_resolved_output(
            schema.as_ref(),
            &resolved_target,
            self.options.mssql_schema_options(),
        ) {
            Ok(output_plan) => {
                phase_timings.push(target_timer.completed());
                output_plan
            }
            Err(error) => {
                phase_timings.push(target_timer.failed());
                return Err(error);
            }
        };

        Ok(PlannedMssqlOutput::new(
            request.clone(),
            resolved_target,
            output_plan,
            phase_timings,
        ))
    }

    /// Writes one selected lazy table to SQL Server.
    ///
    /// The method reuses the session output planner, builds a DataFusion physical
    /// plan for the selected lazy table, exposes DataFusion's merged
    /// `RecordBatch` stream, and hands that stream directly to the existing
    /// one-output MSSQL sink. It does not implement SQL Server lifecycle,
    /// writer, cleanup, retry, or stream buffering behavior itself.
    ///
    /// # Errors
    ///
    /// Returns the first planning, DataFusion stream setup, upstream stream, SQL
    /// Server connection, lifecycle, schema validation, write, or cleanup error.
    pub async fn write_to_mssql(
        &self,
        request: &OutputWritePlan,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        self.run_mssql_write_with_tracing(request, &mut MssqlPublicOneOutputWriter, None)
            .await
    }

    pub(crate) async fn write_to_mssql_with_reporter(
        &self,
        request: &OutputWritePlan,
        reporter: ProgressReporter,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        self.run_mssql_write_with_tracing(request, &mut MssqlPublicOneOutputWriter, Some(&reporter))
            .await
    }

    async fn run_mssql_write_with_tracing<W>(
        &self,
        request: &OutputWritePlan,
        writer: &mut W,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>
    where
        W: OrchestratorMssqlOutputWriter,
    {
        ensure_execute_run_mode(request.target().run_mode())?;
        let target = request.target();
        let target_config = target.target();
        let output_span = observability::output_span(
            target.output_name(),
            target_config.table(),
            target_config.load_mode(),
        );

        async move {
            observability::output_started(
                target.output_name(),
                target_config.table(),
                target_config.load_mode(),
            );
            let result = match reporter {
                Some(reporter) => {
                    self.run_mssql_write_with_reporter(request, writer, reporter)
                        .await
                }
                None => {
                    self.plan_and_write_mssql_output(request, writer, None)
                        .await
                }
            };
            match &result {
                Ok(report) => observability::output_completed(
                    report.output_name(),
                    report.target_table(),
                    report.load_mode(),
                ),
                Err(error) => observability::output_failed(
                    target.output_name(),
                    target_config.table(),
                    target_config.load_mode(),
                    &error.to_string(),
                ),
            }

            result
        }
        .instrument(output_span)
        .await
    }

    #[cfg(test)]
    pub(crate) async fn write_to_mssql_with_writer<W>(
        &self,
        request: &OutputWritePlan,
        writer: &mut W,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>
    where
        W: OrchestratorMssqlOutputWriter,
    {
        ensure_execute_run_mode(request.target().run_mode())?;
        self.plan_and_write_mssql_output(request, writer, None)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn write_to_mssql_with_writer_and_reporter<W>(
        &self,
        request: &OutputWritePlan,
        writer: &mut W,
        reporter: ProgressReporter,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>
    where
        W: OrchestratorMssqlOutputWriter,
    {
        ensure_execute_run_mode(request.target().run_mode())?;
        self.run_mssql_write_with_reporter(request, writer, &reporter)
            .await
    }

    async fn run_mssql_write_with_reporter<W>(
        &self,
        request: &OutputWritePlan,
        writer: &mut W,
        reporter: &ProgressReporter,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>
    where
        W: OrchestratorMssqlOutputWriter,
    {
        reporter.emit(&ProgressEvent::started(ProgressOperation::WriteToMssql));
        let result = self
            .plan_and_write_mssql_output(request, writer, Some(reporter))
            .await;
        reporter.emit(&if result.is_ok() {
            ProgressEvent::completed()
        } else {
            ProgressEvent::failed()
        });
        result
    }

    async fn plan_and_write_mssql_output<W>(
        &self,
        request: &OutputWritePlan,
        writer: &mut W,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>
    where
        W: OrchestratorMssqlOutputWriter,
    {
        let output_name = Some(request.target().output_name());
        if let Some(reporter) = reporter {
            reporter.emit(&ProgressEvent::phase_changed(
                ProgressPhase::PlanningOutput,
                output_name,
            ));
        }
        let planned = self.plan_mssql_output(request)?;
        if let Some(reporter) = reporter {
            reporter.emit(&ProgressEvent::phase_changed(
                ProgressPhase::SettingUpStream,
                output_name,
            ));
        }
        let output_schema = Arc::clone(self.schema_for_lazy_table(planned.table())?);
        let batches = self
            .batch_stream_for_lazy_table(
                planned.table(),
                reporter.map(|reporter| (reporter, request.target().output_name())),
            )
            .await?;

        let phase_timings = planned.phase_timings().to_vec();
        writer
            .write_output(
                output_schema,
                planned.output_plan().clone(),
                planned.resolved_target().clone(),
                batches,
                self.options.mssql_write_backend(),
                self.options.validation_options(),
                reporter,
            )
            .await
            .map(ensure_validation_phase_timing)
            .map(|report| report.with_phase_timings(phase_timings))
    }
}

fn ensure_validation_phase_timing(report: MssqlWriteReport) -> MssqlWriteReport {
    if report
        .phase_timings()
        .iter()
        .any(|timing| timing.phase_name() == VALIDATION_PHASE)
    {
        return report;
    }

    report.with_appended_phase_timings(vec![PhaseTimingReport::not_started(
        VALIDATION_PHASE,
        ReportReasonCode::NotExecuted,
    )])
}

#[async_trait]
pub(crate) trait OrchestratorMssqlOutputWriter: Send {
    #[allow(
        clippy::too_many_arguments,
        reason = "the injected writer boundary receives one planned write plus its reporter"
    )]
    async fn write_output(
        &mut self,
        output_schema: SchemaRef,
        output_plan: MssqlTargetOutputPlan,
        resolved_target: ResolvedMssqlTarget,
        batches: MssqlOutputBatchStream,
        write_backend: MssqlWriteBackend,
        validation_options: ValidationOptions,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>;
}

struct MssqlPublicOneOutputWriter;

#[async_trait]
impl OrchestratorMssqlOutputWriter for MssqlPublicOneOutputWriter {
    async fn write_output(
        &mut self,
        output_schema: SchemaRef,
        output_plan: MssqlTargetOutputPlan,
        resolved_target: ResolvedMssqlTarget,
        batches: MssqlOutputBatchStream,
        write_backend: MssqlWriteBackend,
        validation_options: ValidationOptions,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        write_output_batches_to_mssql_for_workflow(
            output_schema.as_ref(),
            resolved_target,
            output_plan.schema_plan_options(),
            batches,
            write_backend,
            validation_options,
            reporter,
        )
        .await
    }
}

fn ensure_execute_run_mode(run_mode: RunMode) -> Result<(), DeltaFunnelError> {
    match run_mode {
        RunMode::Execute => Ok(()),
        RunMode::DryRun => Err(DeltaFunnelError::MssqlWorkflowPlanning {
            message:
                "write_to_mssql requires RunMode::Execute; use dry_run_to_mssql for dry-run planning"
                    .to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::SchemaRef;
    use futures_util::StreamExt;

    use crate::{
        DeltaFunnelError, DeltaSourceConfig, LoadMode, MssqlConnectionSource,
        MssqlOutputBatchStream, MssqlOutputTarget, MssqlTargetCleanupStatus, MssqlTargetConfig,
        MssqlTargetOutputPlan, MssqlTargetTable, MssqlWriteBackend, MssqlWriteReport,
        ResolvedMssqlTarget, TargetValidationMode, ValidationOptions,
        progress::{ProgressEventKind, ProgressOperation, ProgressPhase, ProgressReporter},
        table_formats::RealParquetDeltaTable,
    };

    use super::super::super::{
        DeltaFunnelSession, LazyTable, LazyTableKind, OutputWritePlan, RunMode, SessionOptions,
        test_support::{
            DeltaLogTable, UNSUPPORTED_SCHEMA_FIELDS_JSON, execute_output_request, output_request,
            override_connection, secret_connection,
        },
    };
    use super::{
        OUTPUT_SCHEMA_PLANNING_PHASE, OrchestratorMssqlOutputWriter, SQL_TARGET_PLANNING_PHASE,
        VALIDATION_PHASE,
    };

    type RecordedProgress = (
        ProgressEventKind,
        Option<ProgressOperation>,
        Option<ProgressPhase>,
    );

    fn recording_reporter() -> (ProgressReporter, Arc<Mutex<Vec<RecordedProgress>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback_events = Arc::clone(&events);
        let reporter = ProgressReporter::new(move |event| {
            let mut events = match callback_events.lock() {
                Ok(events) => events,
                Err(poisoned) => poisoned.into_inner(),
            };
            events.push((event.kind(), event.operation(), event.phase()));
        });
        (reporter, events)
    }

    fn progress_events(events: &Mutex<Vec<RecordedProgress>>) -> Vec<RecordedProgress> {
        match events.lock() {
            Ok(events) => events.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct FakeOrchestratorWriteCall {
        output_name: String,
        target_table: MssqlTargetTable,
        connection_source: MssqlConnectionSource,
        rows: u64,
        batches: u64,
        schema_fields: usize,
        validation_options: ValidationOptions,
    }

    #[derive(Default)]
    struct FakeOrchestratorWriter {
        calls: Vec<FakeOrchestratorWriteCall>,
        failure: Option<DeltaFunnelError>,
    }

    impl FakeOrchestratorWriter {
        fn failing_with(message: &str) -> Self {
            Self {
                failure: Some(DeltaFunnelError::Config {
                    message: message.to_owned(),
                }),
                ..Self::default()
            }
        }
    }

    #[async_trait]
    impl OrchestratorMssqlOutputWriter for FakeOrchestratorWriter {
        async fn write_output(
            &mut self,
            output_schema: SchemaRef,
            output_plan: MssqlTargetOutputPlan,
            resolved_target: ResolvedMssqlTarget,
            mut batches: MssqlOutputBatchStream,
            _write_backend: MssqlWriteBackend,
            validation_options: ValidationOptions,
            _reporter: Option<&ProgressReporter>,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            let mut rows = 0_u64;
            let mut batch_count = 0_u64;

            while let Some(batch) = batches.next().await {
                let batch = batch?;
                rows = rows.saturating_add(u64::try_from(batch.num_rows()).map_err(|_| {
                    DeltaFunnelError::Config {
                        message: "fake writer row count overflowed u64".to_owned(),
                    }
                })?);
                batch_count = batch_count.saturating_add(1);
            }

            if let Some(error) = self.failure.take() {
                return Err(error);
            }

            self.calls.push(FakeOrchestratorWriteCall {
                output_name: resolved_target.output_name().to_owned(),
                target_table: resolved_target.table().clone(),
                connection_source: resolved_target.connection_source(),
                rows,
                batches: batch_count,
                schema_fields: output_schema.fields().len(),
                validation_options,
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

    #[test]
    fn plan_mssql_output_uses_source_schema_and_session_connection()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source.clone(),
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let planned = session.plan_mssql_output(&request)?;

        assert_eq!(planned.request(), &request);
        assert_eq!(planned.table(), &source);
        assert_eq!(planned.target().run_mode(), RunMode::DryRun);
        assert_eq!(planned.output_plan().output_name(), "orders_output");
        assert_eq!(planned.output_plan().target_table().schema(), Some("dbo"));
        assert_eq!(planned.output_plan().target_table().table(), "orders_sink");
        assert_eq!(
            planned.output_plan().connection_source(),
            MssqlConnectionSource::ContextDefault
        );
        assert_eq!(
            planned.output_plan().connection().display_label(),
            Some("warehouse-primary")
        );
        assert_eq!(planned.output_plan().schema_mappings().len(), 2);
        assert_eq!(
            planned.output_plan().schema_mappings()[0].arrow().name(),
            "id"
        );
        assert_eq!(
            planned.output_plan().schema_mappings()[1].arrow().name(),
            "customer_name"
        );
        assert_eq!(planned.output_plan().create_table_sql(), None);
        assert_eq!(
            planned
                .phase_timings()
                .iter()
                .map(crate::PhaseTimingReport::phase_name)
                .collect::<Vec<_>>(),
            vec![OUTPUT_SCHEMA_PLANNING_PHASE, SQL_TARGET_PLANNING_PHASE]
        );
        assert!(
            planned
                .phase_timings()
                .iter()
                .all(|timing| timing.status().is_completed())
        );
        assert!(
            planned
                .phase_timings()
                .iter()
                .all(|timing| timing.elapsed_micros().is_some())
        );
        Ok(())
    }

    #[tokio::test]
    async fn plan_mssql_output_uses_pending_derived_schema_without_row_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session.table_from_sql("select id from orders").await?;
        let request = output_request(
            derived.clone(),
            "derived_orders_output",
            "derived_orders",
            LoadMode::CreateAndLoad,
        )?;

        let planned = session.plan_mssql_output(&request)?;

        assert_eq!(planned.table(), &derived);
        assert_eq!(planned.output_plan().load_mode(), LoadMode::CreateAndLoad);
        assert_eq!(planned.output_plan().schema_mappings().len(), 1);
        assert_eq!(
            planned.output_plan().schema_mappings()[0].arrow().name(),
            "id"
        );
        let create_table_sql = planned
            .output_plan()
            .create_table_sql()
            .ok_or("expected create table SQL")?;
        assert!(create_table_sql.contains("[dbo].[derived_orders]"));
        assert_eq!(
            planned
                .phase_timings()
                .iter()
                .map(crate::PhaseTimingReport::phase_name)
                .collect::<Vec<_>>(),
            vec![
                "lazy_sql_planning",
                OUTPUT_SCHEMA_PLANNING_PHASE,
                SQL_TARGET_PLANNING_PHASE
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn plan_mssql_output_accepts_registered_derived_alias_handle()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let derived = session
            .table_from_sql("select customer_name from orders")
            .await?;
        let alias = session.register_alias("customer_names", &derived)?;
        let request = output_request(
            alias.clone(),
            "customer_names_output",
            "customer_names_sink",
            LoadMode::AppendExisting,
        )?;

        let planned = session.plan_mssql_output(&request)?;

        assert_eq!(planned.table(), &alias);
        assert_eq!(planned.output_plan().schema_mappings().len(), 1);
        assert_eq!(
            planned.output_plan().schema_mappings()[0].arrow().name(),
            "customer_name"
        );
        Ok(())
    }

    #[test]
    fn plan_mssql_output_connection_override_wins_without_mutating_session_default()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders_sink")?)
            .with_connection(override_connection()?);
        let request = OutputWritePlan::new(
            source,
            MssqlOutputTarget::new("orders_output", target_config, RunMode::DryRun),
        );

        let planned = session.plan_mssql_output(&request)?;

        assert_eq!(
            planned.output_plan().connection_source(),
            MssqlConnectionSource::TargetOverride
        );
        assert_eq!(
            planned.output_plan().connection().display_label(),
            Some("warehouse-override")
        );
        assert_eq!(
            session
                .options()
                .default_mssql_connection()
                .ok_or("expected default connection")?
                .summary()
                .display_label(),
            Some("warehouse-primary")
        );
        Ok(())
    }

    #[test]
    fn plan_mssql_output_missing_effective_connection_fails_before_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = execute_output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "orders_output"
        ));
        Ok(())
    }

    #[test]
    fn plan_mssql_output_accepts_replace_before_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(source, "orders_output", "orders_sink", LoadMode::Replace)?;

        let planned = session.plan_mssql_output(&request)?;

        assert_eq!(planned.output_plan().load_mode(), LoadMode::Replace);
        assert_eq!(planned.output_plan().target_table().table(), "orders_sink");
        assert!(
            planned
                .output_plan()
                .lifecycle_plan()
                .create_table_sql_required()
        );
        Ok(())
    }

    #[test]
    fn plan_mssql_output_rejects_unknown_lazy_table_before_target_planning()
    -> Result<(), DeltaFunnelError> {
        let session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let request = output_request(
            LazyTable::placeholder(42, LazyTableKind::DeltaSource),
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("not registered in this session")
        ));
        Ok(())
    }

    #[test]
    fn plan_mssql_output_rejects_invalid_output_name() -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(source, "  ", "orders_sink", LoadMode::AppendExisting)?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::InvalidMssqlOutputIdentity { output_name, .. })
                if output_name == "  "
        ));
        Ok(())
    }

    #[test]
    fn plan_mssql_output_rejects_invalid_target_identifier_before_side_effects()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source,
            "orders_output",
            "orders\narchive",
            LoadMode::CreateAndLoad,
        )?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            &error,
            Err(DeltaFunnelError::MssqlDdlTargetIdentifier { output_name, .. })
                if output_name == "orders_output"
        ));
        let display = error.err().ok_or("expected error")?.to_string();
        assert!(!display.contains('\n'));
        assert!(display.contains("control characters"));
        Ok(())
    }

    #[test]
    fn plan_mssql_output_reports_unsupported_source_schema()
    -> Result<(), Box<dyn std::error::Error>> {
        let table =
            DeltaLogTable::new_with_schema("unsupported-schema", UNSUPPORTED_SCHEMA_FIELDS_JSON)?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source =
            session.delta_lake(DeltaSourceConfig::new("unsupported_schema", table.uri()))?;
        let request = output_request(
            source,
            "unsupported_output",
            "unsupported_sink",
            LoadMode::AppendExisting,
        )?;

        let error = session.plan_mssql_output(&request);

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlSchemaPlanning {
                output_name,
                diagnostics,
            }) if output_name == "unsupported_output" && !diagnostics.is_empty()
        ));
        Ok(())
    }

    #[test]
    fn planned_mssql_output_debug_redacts_connection_material()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let target_config = MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "orders_sink")?)
            .with_connection(override_connection()?);
        let request = OutputWritePlan::new(
            source,
            MssqlOutputTarget::new("orders_output", target_config, RunMode::DryRun),
        );

        let planned = session.plan_mssql_output(&request)?;
        let debug = format!("{planned:?}");

        assert!(debug.contains("orders_output"));
        assert!(debug.contains("warehouse-override"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("override-secret"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("server=tcp"));
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_requires_effective_connection_before_stream_setup()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = execute_output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let (reporter, events) = recording_reporter();

        let error = session
            .write_to_mssql_with_reporter(&request, reporter)
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                if output_name == "orders_output"
        ));
        assert_eq!(
            progress_events(&events),
            vec![
                (
                    ProgressEventKind::Started,
                    Some(ProgressOperation::WriteToMssql),
                    None,
                ),
                (
                    ProgressEventKind::PhaseChanged,
                    None,
                    Some(ProgressPhase::PlanningOutput),
                ),
                (ProgressEventKind::Failed, None, None),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_with_writer_hands_query_stream_to_one_output_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new()
                .with_default_mssql_connection(secret_connection()?)
                .with_validation_options(
                    ValidationOptions::new()
                        .with_target_validation_mode(TargetValidationMode::Require),
                ),
        )?;
        let derived = session
            .table_from_sql("select 1 as id union all select 2 as id")
            .await?;
        let request = execute_output_request(
            derived,
            "orders_output",
            "orders_sink",
            LoadMode::CreateAndLoad,
        )?;
        let mut writer = FakeOrchestratorWriter::default();
        let (reporter, events) = recording_reporter();

        let report = session
            .write_to_mssql_with_writer_and_reporter(&request, &mut writer, reporter)
            .await?;

        assert_eq!(writer.calls.len(), 1);
        let call = writer.calls.first().ok_or("expected fake writer call")?;
        assert_eq!(call.output_name, "orders_output");
        assert_eq!(call.target_table.schema(), Some("dbo"));
        assert_eq!(call.target_table.table(), "orders_sink");
        assert_eq!(
            call.connection_source,
            MssqlConnectionSource::ContextDefault
        );
        assert_eq!(call.rows, 2);
        assert!(call.batches >= 1);
        assert_eq!(call.schema_fields, 1);
        assert_eq!(
            call.validation_options.target_validation_mode(),
            TargetValidationMode::Require
        );
        assert_eq!(report.output_name(), "orders_output");
        assert_eq!(report.stats().rows_written(), 2);
        assert_eq!(report.stats().batches_written(), call.batches);
        assert_eq!(report.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        assert_eq!(
            report
                .phase_timings()
                .iter()
                .take(3)
                .map(crate::PhaseTimingReport::phase_name)
                .collect::<Vec<_>>(),
            vec![
                "lazy_sql_planning",
                OUTPUT_SCHEMA_PLANNING_PHASE,
                SQL_TARGET_PLANNING_PHASE
            ]
        );
        let validation_timing = report
            .phase_timings()
            .iter()
            .find(|timing| timing.phase_name() == VALIDATION_PHASE)
            .ok_or("expected validation phase timing")?;
        assert_eq!(
            validation_timing.status(),
            crate::PhaseStatus::not_started(crate::ReportReasonCode::NotExecuted)
        );
        assert_eq!(
            progress_events(&events),
            vec![
                (
                    ProgressEventKind::Started,
                    Some(ProgressOperation::WriteToMssql),
                    None,
                ),
                (
                    ProgressEventKind::PhaseChanged,
                    None,
                    Some(ProgressPhase::PlanningOutput),
                ),
                (
                    ProgressEventKind::PhaseChanged,
                    None,
                    Some(ProgressPhase::SettingUpStream),
                ),
                (ProgressEventKind::Completed, None, None),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn write_failure_emits_one_terminal_without_sensitive_progress_payloads()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let derived = session
            .table_from_sql(
                "select 'row-secret' as payload, \
                 'file:///tmp/orders?token=uri-secret' as source_uri",
            )
            .await?;
        let request = execute_output_request(
            derived,
            "orders_output",
            "orders_sink",
            LoadMode::CreateAndLoad,
        )?;
        let mut writer = FakeOrchestratorWriter::failing_with("raw-error-secret");
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback_events = Arc::clone(&events);
        let reporter = ProgressReporter::new(move |event| {
            let mut events = match callback_events.lock() {
                Ok(events) => events,
                Err(poisoned) => poisoned.into_inner(),
            };
            events.push((event.kind(), format!("{event:?}")));
        });

        let error = session
            .write_to_mssql_with_writer_and_reporter(&request, &mut writer, reporter)
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::Config { message }) if message == "raw-error-secret"
        ));
        let events = match events.lock() {
            Ok(events) => events,
            Err(poisoned) => poisoned.into_inner(),
        };
        assert_eq!(
            events.iter().map(|(kind, _)| *kind).collect::<Vec<_>>(),
            vec![
                ProgressEventKind::Started,
                ProgressEventKind::PhaseChanged,
                ProgressEventKind::PhaseChanged,
                ProgressEventKind::Failed,
            ]
        );
        let payloads = events
            .iter()
            .map(|(_, payload)| payload.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        for sensitive in [
            "secret-token",
            "row-secret",
            "uri-secret",
            "raw-error-secret",
            "server=tcp",
        ] {
            assert!(!payloads.contains(sensitive));
        }
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_with_writer_executes_real_delta_source_fixture()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.delta_lake(DeltaSourceConfig::new(
            "orders",
            table.path().to_string_lossy().to_string(),
        ))?;
        let selected_orders = session
            .table_from_sql("select id, customer_name from orders")
            .await?;
        let request = execute_output_request(
            selected_orders,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let mut writer = FakeOrchestratorWriter::default();

        let report = session
            .write_to_mssql_with_writer(&request, &mut writer)
            .await?;

        assert_eq!(writer.calls.len(), 1);
        let call = writer.calls.first().ok_or("expected fake writer call")?;
        assert_eq!(call.output_name, "orders_output");
        assert_eq!(call.rows, u64::try_from(table.rows())?);
        assert!(call.batches >= 1);
        assert_eq!(call.schema_fields, 2);
        assert_eq!(report.stats().rows_written(), u64::try_from(table.rows())?);
        assert_eq!(report.stats().batches_written(), call.batches);
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_with_writer_executes_multi_source_delta_join_fixture()
    -> Result<(), Box<dyn std::error::Error>> {
        let orders = RealParquetDeltaTable::new_default("orders")?;
        let customers = RealParquetDeltaTable::new_default("customers")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
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
                "select o.id, c.customer_name \
                 from orders o \
                 join customers c on o.id = c.id",
            )
            .await?;
        let request = execute_output_request(
            joined,
            "joined_output",
            "joined_sink",
            LoadMode::AppendExisting,
        )?;
        let mut writer = FakeOrchestratorWriter::default();

        let report = session
            .write_to_mssql_with_writer(&request, &mut writer)
            .await?;

        assert_eq!(writer.calls.len(), 1);
        let call = writer.calls.first().ok_or("expected fake writer call")?;
        assert_eq!(call.output_name, "joined_output");
        assert_eq!(call.rows, 3);
        assert!(call.batches >= 1);
        assert_eq!(call.schema_fields, 2);
        assert_eq!(report.stats().rows_written(), 3);
        assert_eq!(report.stats().batches_written(), call.batches);
        Ok(())
    }

    #[tokio::test]
    async fn write_to_mssql_rejects_dry_run_before_planning_or_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
        let derived = session.table_from_sql("select 1 as id").await?;
        let request = output_request(
            derived,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let mut writer = FakeOrchestratorWriter::default();
        let (reporter, events) = recording_reporter();

        let error = session
            .write_to_mssql_with_writer_and_reporter(&request, &mut writer, reporter)
            .await;

        assert!(matches!(
            error,
            Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                if message.contains("RunMode::Execute")
                    && message.contains("dry_run_to_mssql")
        ));
        assert!(writer.calls.is_empty());
        assert!(progress_events(&events).is_empty());
        Ok(())
    }
}
