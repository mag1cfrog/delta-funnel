use std::{collections::BTreeSet, sync::Arc};

use crate::{
    DeltaFunnelError, MssqlStreamBenchmarkOutputWriter, MssqlWorkflowOutputWriter,
    MssqlWorkflowSinkWriter, PhaseTimingReport, ReportReasonCode, observability,
    progress::{ProgressEvent, ProgressOperation, ProgressPhase, ProgressReporter},
    report::{PhaseTimer, sql_server::WriteAllReport},
    support::sanitize_text_for_display,
    usize_to_u64_saturating,
};

use super::super::super::{
    DeltaFunnelSession, OutputWritePlan, PlannedMssqlOutput, RunMode,
    query_handoff::{provider_stats_snapshots, shared_provider_stats_snapshots},
};
use super::{MssqlOutputCacheDecision, WriteAllCacheMode, WriteAllOptions, cache_report};
use tracing::Instrument;

const OUTPUT_PLANNING_PHASE: &str = "output_planning";
const CACHE_PLANNING_PHASE: &str = "cache_planning";
const WORKFLOW_EXECUTION_PHASE: &str = "workflow_execution";
const SOURCE_REPORTING_PHASE: &str = "source_reporting";

impl DeltaFunnelSession {
    #[cfg(test)]
    pub(crate) fn plan_write_all_outputs(
        &self,
        requests: &[OutputWritePlan],
    ) -> Result<Vec<PlannedMssqlOutput>, DeltaFunnelError> {
        validate_write_all_requests(requests)?;
        self.plan_write_outputs(requests, None)
    }

    fn plan_write_outputs(
        &self,
        requests: &[OutputWritePlan],
        reporter: Option<&ProgressReporter>,
    ) -> Result<Vec<PlannedMssqlOutput>, DeltaFunnelError> {
        let output_count = usize_to_u64_saturating(requests.len());

        requests
            .iter()
            .enumerate()
            .map(|(output_index, request)| {
                let output_index = usize_to_u64_saturating(output_index.saturating_add(1));
                let output_reporter =
                    reporter.and_then(|reporter| reporter.for_output(output_index, output_count));
                if let Some(output_reporter) = output_reporter {
                    output_reporter.emit(&ProgressEvent::phase_changed(
                        ProgressPhase::PlanningOutput,
                        Some(request.target().output_name()),
                    ));
                }
                self.plan_mssql_output(request)
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) async fn write_all_with_options_and_writer<W>(
        &self,
        requests: &[OutputWritePlan],
        options: WriteAllOptions,
        writer: W,
    ) -> Result<WriteAllReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        self.execute_write_all_with_writer(requests, options, writer, None)
            .await
    }

    /// Validates and executes one multi-output request with an injected writer.
    ///
    /// The optional reporter adds live progress to the same execution path.
    /// Empty requests still return the normal empty report without emitting a
    /// progress lifecycle.
    async fn execute_write_all_with_writer<W>(
        &self,
        requests: &[OutputWritePlan],
        options: WriteAllOptions,
        writer: W,
        reporter: Option<&ProgressReporter>,
    ) -> Result<WriteAllReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        validate_write_all_requests(requests)?;
        let active_reporter = if requests.is_empty() { None } else { reporter };

        if let Some(reporter) = active_reporter {
            reporter.emit(&ProgressEvent::started(ProgressOperation::WriteAllToMssql));
        }

        let result = async {
            // Resolve every output before cache planning or execution starts.
            let planning_timer = PhaseTimer::start(OUTPUT_PLANNING_PHASE);
            let planned_outputs = self.plan_write_outputs(requests, active_reporter)?;
            let mut phase_timings = vec![planning_timer.completed()];

            // Cache planning selects the execution route. Disabled mode records
            // the skipped phase so reports keep the same phase structure.
            let automatic_cache_plan = match options.cache_mode() {
                WriteAllCacheMode::Auto => {
                    let cache_timer = PhaseTimer::start(CACHE_PLANNING_PHASE);
                    let cache_plan = self.plan_mssql_output_cache(requests);
                    phase_timings.push(cache_timer.completed());
                    Some(cache_plan)
                }
                WriteAllCacheMode::Disabled => {
                    phase_timings.push(PhaseTimingReport::skipped(
                        CACHE_PLANNING_PHASE,
                        ReportReasonCode::NotExecuted,
                    ));
                    None
                }
            };

            // Run exactly one route while all outputs share the same source
            // statistics snapshot collection used by the final source report.
            let shared_provider_stats = shared_provider_stats_snapshots();
            let workflow_timer = PhaseTimer::start(WORKFLOW_EXECUTION_PHASE);
            let (workflow, cache) = match automatic_cache_plan.as_ref() {
                Some(cache_plan) => match cache_plan.decision() {
                    MssqlOutputCacheDecision::NoCache { .. } => {
                        let workflow = self
                            .write_all_uncached_with_writer(
                                &planned_outputs,
                                writer,
                                Some(Arc::clone(&shared_provider_stats)),
                                active_reporter,
                                options.execution_profile_mode(),
                            )
                            .await?;
                        (workflow, cache_report::from_plan(cache_plan))
                    }
                    MssqlOutputCacheDecision::CacheAliases(cache_aliases) => {
                        let workflow = self
                            .write_all_cached_with_writer(
                                &planned_outputs,
                                cache_aliases,
                                writer,
                                Some(Arc::clone(&shared_provider_stats)),
                                active_reporter,
                                options.execution_profile_mode(),
                            )
                            .await?;
                        (workflow, cache_report::from_executed_plan(cache_plan))
                    }
                },
                None => {
                    let workflow = self
                        .write_all_uncached_with_writer(
                            &planned_outputs,
                            writer,
                            Some(Arc::clone(&shared_provider_stats)),
                            active_reporter,
                            options.execution_profile_mode(),
                        )
                        .await?;
                    (workflow, cache_report::disabled())
                }
            };
            phase_timings.push(workflow_timer.completed());

            // Source reporting happens once after either execution route and
            // uses the provider statistics collected above.
            if let Some(reporter) = active_reporter {
                reporter.emit(&ProgressEvent::phase_changed(
                    ProgressPhase::ReportingSources,
                    None,
                ));
            }
            let source_timer = PhaseTimer::start(SOURCE_REPORTING_PHASE);
            let sources = self.source_reports_for_planned_outputs_with_provider_stats(
                &planned_outputs,
                provider_stats_snapshots(&shared_provider_stats),
            )?;
            phase_timings.push(source_timer.completed());

            Ok(WriteAllReport::new(workflow, cache, sources).with_phase_timings(phase_timings))
        }
        .await;

        if let Some(reporter) = active_reporter {
            reporter.emit(&match &result {
                Ok(report) if report.all_succeeded() => ProgressEvent::completed(),
                Ok(_) => ProgressEvent::completed_with_failures(),
                Err(_) => ProgressEvent::failed(),
            });
        }
        result
    }

    /// Runs the shared write path inside its tracing and observability boundary.
    async fn write_all_with_observability<W>(
        &self,
        requests: &[OutputWritePlan],
        options: WriteAllOptions,
        writer: W,
        reporter: Option<&ProgressReporter>,
    ) -> Result<WriteAllReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        let output_count = requests.len();
        async move {
            observability::workflow_started(RunMode::Execute, output_count);
            let result = self
                .execute_write_all_with_writer(requests, options, writer, reporter)
                .await;
            observability::workflow_finished(RunMode::Execute, output_count, &result);
            result
        }
        .instrument(observability::workflow_span(RunMode::Execute, output_count))
        .await
    }

    /// Runs the multi-output write while emitting one top-level progress
    /// lifecycle.
    ///
    /// Progress describes work while it is happening. The returned report
    /// contains the final per-output results. A report containing failed or
    /// skipped outputs is still returned successfully and ends with a
    /// completed-with-failures progress event.
    #[cfg(test)]
    pub(crate) async fn write_all_with_progress_and_writer<W>(
        &self,
        requests: &[OutputWritePlan],
        options: WriteAllOptions,
        reporter: ProgressReporter,
        writer: W,
    ) -> Result<WriteAllReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        self.execute_write_all_with_writer(requests, options, writer, Some(&reporter))
            .await
    }

    #[cfg(test)]
    pub(crate) async fn write_all_with_writer<W>(
        &self,
        requests: &[OutputWritePlan],
        writer: W,
    ) -> Result<WriteAllReport, DeltaFunnelError>
    where
        W: MssqlWorkflowOutputWriter,
    {
        self.execute_write_all_with_writer(requests, WriteAllOptions::default(), writer, None)
            .await
    }

    /// Writes multiple selected lazy tables to SQL Server sequentially.
    ///
    /// The default mode performs conservative automatic cache planning for
    /// shared registered derived aliases. The returned report wraps the
    /// lower-level SQL Server workflow report and includes cache selection
    /// metadata for this call.
    ///
    /// # Errors
    ///
    /// Returns the first pre-execution validation/planning error, or a workflow
    /// execution error from the lower-level SQL Server workflow.
    pub async fn write_all(
        &self,
        requests: &[OutputWritePlan],
    ) -> Result<WriteAllReport, DeltaFunnelError> {
        self.write_all_with_options(requests, WriteAllOptions::default())
            .await
    }

    /// Writes multiple selected lazy tables to SQL Server sequentially with explicit options.
    ///
    /// `WriteAllCacheMode::Disabled` uses the uncached path. The
    /// default `Auto` mode performs conservative shared-cache planning and
    /// reports the selected or skipped cache decision.
    ///
    /// # Errors
    ///
    /// Returns the first pre-execution validation/planning error, or a workflow
    /// execution error from the lower-level SQL Server workflow.
    pub async fn write_all_with_options(
        &self,
        requests: &[OutputWritePlan],
        options: WriteAllOptions,
    ) -> Result<WriteAllReport, DeltaFunnelError> {
        self.write_all_with_observability(requests, options, MssqlWorkflowSinkWriter, None)
            .await
    }

    /// Writes multiple outputs and reports one consolidated progress lifecycle.
    pub(crate) async fn write_all_with_progress(
        &self,
        requests: &[OutputWritePlan],
        options: WriteAllOptions,
        reporter: ProgressReporter,
    ) -> Result<WriteAllReport, DeltaFunnelError> {
        self.write_all_with_observability(
            requests,
            options,
            MssqlWorkflowSinkWriter,
            Some(&reporter),
        )
        .await
    }

    /// Runs the multi-output write workflow through a local stream-draining
    /// writer for benchmark phase timing without connecting to SQL Server.
    ///
    /// # Errors
    ///
    /// Returns planning, source, SQL, or stream execution errors from the
    /// normal `write_all` path. It does not perform SQL Server lifecycle,
    /// writer initialization, target validation, or cleanup.
    #[doc(hidden)]
    pub async fn write_all_for_stream_benchmark(
        &self,
        requests: &[OutputWritePlan],
        options: WriteAllOptions,
    ) -> Result<WriteAllReport, DeltaFunnelError> {
        self.write_all_with_observability(requests, options, MssqlStreamBenchmarkOutputWriter, None)
            .await
    }
}

fn ensure_write_all_execute_run_mode(run_mode: RunMode) -> Result<(), DeltaFunnelError> {
    match run_mode {
        RunMode::Execute => Ok(()),
        RunMode::DryRun => Err(DeltaFunnelError::MssqlWorkflowPlanning {
            message:
                "write_all requires RunMode::Execute; use dry_run_all_to_mssql for dry-run planning"
                    .to_owned(),
        }),
    }
}

fn validate_write_all_requests(requests: &[OutputWritePlan]) -> Result<(), DeltaFunnelError> {
    ensure_unique_write_all_output_names(requests)?;
    for request in requests {
        ensure_write_all_execute_run_mode(request.target().run_mode())?;
    }
    Ok(())
}

pub(crate) fn ensure_unique_write_all_output_names(
    requests: &[OutputWritePlan],
) -> Result<(), DeltaFunnelError> {
    let mut output_names = BTreeSet::new();
    for request in requests {
        let output_name = request.target().output_name();
        if !output_names.insert(output_name) {
            return Err(DeltaFunnelError::MssqlWorkflowPlanning {
                message: format!(
                    "write_all output names must be unique; duplicate output name `{}`",
                    sanitize_text_for_display(output_name)
                ),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, atomic::Ordering};

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use futures_util::StreamExt;

    use super::super::super::super::{
        DeltaFunnelSession, MssqlOutputTarget, OutputWritePlan, RunMode, SessionOptions,
        SourceUsageStatus,
        test_support::{
            collect_stream_row_count, execute_output_request, failing_scan_marker_region_provider,
            output_request, override_connection, scan_counting_marker_region_provider,
            secret_connection, stream_setup_failing_marker_region_provider,
        },
    };
    use super::super::{WriteAllCacheMode, WriteAllOptions};
    use super::{
        CACHE_PLANNING_PHASE, OUTPUT_PLANNING_PHASE, SOURCE_REPORTING_PHASE,
        WORKFLOW_EXECUTION_PHASE,
    };
    use crate::{
        DeltaFunnelError, DeltaSourceConfig, ExecutionProfileMode, LoadMode,
        MssqlBatchShapingReport, MssqlOutputBatchStream, MssqlSchemaPlanOptions,
        MssqlTargetCleanupStatus, MssqlTargetConfig, MssqlTargetTable, MssqlTargetTableState,
        MssqlWorkflowOutputWriter, MssqlWriteBackend, MssqlWriteFailureContext, MssqlWritePhase,
        MssqlWriteReport, PhaseStatus, PhaseTimingReport, QueryExecutionOutcome,
        QueryExecutionScope, ReportReasonCode, ResolvedMssqlTarget, RowCount,
        WriteAllCacheAliasStatus, WriteAllCacheReport, WriteAllNoCacheReason,
        observability::test_capture::{CapturedEvent, TracingCapture},
        plan_mssql_target_for_resolved_output,
        progress::{
            ProgressEvent, ProgressEventKind, ProgressOperation, ProgressPhase, ProgressReporter,
        },
        table_formats::RealParquetDeltaTable,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedProgress {
        kind: ProgressEventKind,
        operation: Option<ProgressOperation>,
        phase: Option<ProgressPhase>,
        output_name: Option<String>,
        output_index: Option<u64>,
        output_count: Option<u64>,
        rows: Option<u64>,
        batches: Option<u64>,
        files_handled: Option<u64>,
        files_total: Option<u64>,
        task_id: Option<tokio::task::Id>,
    }

    fn recording_progress() -> (ProgressReporter, Arc<Mutex<Vec<RecordedProgress>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&events);
        let reporter = ProgressReporter::new(move |event| {
            if let Ok(mut events) = recorded.lock() {
                events.push(RecordedProgress {
                    kind: event.kind(),
                    operation: event.operation(),
                    phase: event.phase(),
                    output_name: event.output_name().map(str::to_owned),
                    output_index: event.output_index(),
                    output_count: event.output_count(),
                    rows: event.rows(),
                    batches: event.batches(),
                    files_handled: event.files_handled(),
                    files_total: event.files_total(),
                    task_id: tokio::task::try_id(),
                });
            }
        });
        (reporter, events)
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct FakeOrchestratorWriteCall {
        output_name: String,
        target_table: MssqlTargetTable,
        rows: u64,
        batches: u64,
    }

    #[derive(Clone, Default)]
    struct FakeWorkflowWriter {
        calls: Arc<Mutex<Vec<FakeOrchestratorWriteCall>>>,
        fail_output_name: Option<String>,
        fail_before_poll_output_name: Option<String>,
    }

    impl FakeWorkflowWriter {
        fn failing_on(output_name: &str) -> Self {
            Self {
                fail_output_name: Some(output_name.to_owned()),
                ..Self::default()
            }
        }

        fn failing_before_poll_on(output_name: &str) -> Self {
            Self {
                fail_before_poll_output_name: Some(output_name.to_owned()),
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
            _write_backend: MssqlWriteBackend,
            _validation_options: crate::ValidationOptions,
            reporter: Option<&ProgressReporter>,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            let output_plan = plan_mssql_target_for_resolved_output(
                output_schema.as_ref(),
                &resolved_target,
                schema_options,
            )?;
            if self
                .fail_before_poll_output_name
                .as_deref()
                .is_some_and(|output_name| output_name == resolved_target.output_name())
            {
                drop(batches);
                return Err(DeltaFunnelError::MssqlWritePhase {
                    context: Box::new(MssqlWriteFailureContext::from_output_plan(
                        &output_plan,
                        MssqlWritePhase::Connect,
                        0,
                        0,
                        0,
                        false,
                        MssqlTargetCleanupStatus::NotApplicable,
                    )),
                    message: format!(
                        "fake workflow writer failed before polling `{}`",
                        resolved_target.output_name()
                    ),
                });
            }

            let mut rows = 0_u64;
            let mut batch_count = 0_u64;

            while let Some(batch) = batches.next().await {
                let batch = batch.map_err(|error| DeltaFunnelError::MssqlWritePhase {
                    context: Box::new(MssqlWriteFailureContext::from_output_plan(
                        &output_plan,
                        MssqlWritePhase::PollBatchStream,
                        rows,
                        batch_count,
                        0,
                        false,
                        MssqlTargetCleanupStatus::NotApplicable,
                    )),
                    message: format!(
                        "fake workflow writer failed while polling `{}`: {error}",
                        resolved_target.output_name()
                    ),
                })?;
                rows = rows.saturating_add(u64::try_from(batch.num_rows()).map_err(|_| {
                    DeltaFunnelError::Config {
                        message: "fake workflow writer row count overflowed u64".to_owned(),
                    }
                })?);
                batch_count = batch_count.saturating_add(1);
            }

            self.calls
                .lock()
                .map_err(|_| DeltaFunnelError::Config {
                    message: "fake workflow writer call lock poisoned".to_owned(),
                })?
                .push(FakeOrchestratorWriteCall {
                    output_name: resolved_target.output_name().to_owned(),
                    target_table: resolved_target.table().clone(),
                    rows,
                    batches: batch_count,
                });
            if let Some(reporter) = reporter {
                reporter.emit(&ProgressEvent::progress(
                    ProgressPhase::Writing,
                    Some(resolved_target.output_name()),
                    rows,
                    batch_count,
                ));
            }

            if self
                .fail_output_name
                .as_deref()
                .is_some_and(|output_name| output_name == resolved_target.output_name())
            {
                let partial_write_possible = output_plan.load_mode() == LoadMode::AppendExisting
                    && (rows > 0 || batch_count > 0);
                return Err(DeltaFunnelError::MssqlWritePhase {
                    context: Box::new(MssqlWriteFailureContext::from_output_plan(
                        &output_plan,
                        MssqlWritePhase::WriteBatch,
                        rows,
                        batch_count,
                        0,
                        partial_write_possible,
                        MssqlTargetCleanupStatus::NotApplicable,
                    )),
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

    fn assert_batch_shaping(
        report: MssqlBatchShapingReport,
        expected_status: PhaseStatus,
        expected_input_batches: u64,
        expected_input_rows: u64,
        expected_output_batches: u64,
        expected_output_rows: u64,
    ) {
        assert_eq!(report.status(), expected_status);
        assert_eq!(report.input_batches(), expected_input_batches);
        assert_eq!(report.input_rows(), expected_input_rows);
        assert_eq!(report.output_batches(), expected_output_batches);
        assert_eq!(report.output_rows(), expected_output_rows);
    }

    fn assert_phase_timing(
        timings: &[PhaseTimingReport],
        phase_name: &str,
        expected_status: PhaseStatus,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut matching = timings
            .iter()
            .filter(|timing| timing.phase_name() == phase_name);
        let timing = matching
            .next()
            .ok_or_else(|| format!("missing phase timing {phase_name}"))?;
        if matching.next().is_some() {
            return Err(format!("duplicate phase timing {phase_name}").into());
        }

        assert_eq!(timing.status(), expected_status);
        if expected_status.is_completed() || expected_status.is_failed() {
            assert!(timing.elapsed_micros().is_some());
        } else {
            assert_eq!(timing.elapsed_micros(), None);
        }
        Ok(())
    }

    fn execution_profile_events(capture: &TracingCapture) -> Vec<CapturedEvent> {
        capture
            .captured()
            .events()
            .into_iter()
            .filter(|event| {
                event.fields.get("telemetry_event").map(String::as_str)
                    == Some("query_execution_profile_terminal")
            })
            .collect()
    }

    const fn detailed_uncached_write_all_options() -> WriteAllOptions {
        WriteAllOptions::new()
            .with_cache_mode(WriteAllCacheMode::Disabled)
            .with_execution_profile_mode(ExecutionProfileMode::Detailed)
    }

    async fn run_two_output_failure_case(
        session: &mut DeltaFunnelSession,
        failing_sql: &str,
        writer: FakeWorkflowWriter,
    ) -> Result<crate::WriteAllReport, Box<dyn std::error::Error>> {
        let failing = session.table_from_sql(failing_sql).await?;
        let skipped = session.table_from_sql("select 2 as id").await?;
        let failing = execute_output_request(
            failing,
            "failing_output",
            "failing_orders",
            LoadMode::AppendExisting,
        )?;
        let skipped = execute_output_request(
            skipped,
            "skipped_output",
            "skipped_orders",
            LoadMode::AppendExisting,
        )?;

        Ok(session
            .write_all_with_options_and_writer(
                &[failing, skipped],
                detailed_uncached_write_all_options(),
                writer,
            )
            .await?)
    }

    fn assert_profiled_failure(
        report: &crate::WriteAllReport,
        phase: MssqlWritePhase,
        outcome: QueryExecutionOutcome,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let [crate::MssqlOutputWriteStatus::Failed(failure), skipped] = report.outputs() else {
            return Err("expected one failed and one skipped output".into());
        };
        let context = failure.context().ok_or("expected write failure context")?;
        assert_eq!(context.phase(), phase);
        let profile = context
            .report()
            .execution_profile()
            .ok_or("expected failed output profile")?;
        assert_eq!(profile.outcome(), outcome);
        assert!(profile.partial());
        assert!(skipped.is_skipped());
        Ok(())
    }

    mod planning {
        use super::*;

        #[tokio::test]
        async fn plan_write_all_outputs_plans_valid_outputs_in_order()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let west = session.table_from_sql("select 1 as id").await?;
            let east = session.table_from_sql("select 2 as id").await?;
            let west = execute_output_request(
                west,
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let east = execute_output_request(
                east,
                "east_output",
                "east_orders",
                LoadMode::CreateAndLoad,
            )?;

            let planned = session.plan_write_all_outputs(&[west, east])?;

            assert_eq!(planned.len(), 2);
            assert_eq!(planned[0].output_plan().output_name(), "west_output");
            assert_eq!(
                planned[0].output_plan().target_table().table(),
                "west_orders"
            );
            assert_eq!(planned[1].output_plan().output_name(), "east_output");
            assert_eq!(
                planned[1].output_plan().target_table().table(),
                "east_orders"
            );
            Ok(())
        }

        #[tokio::test]
        async fn plan_write_all_outputs_rejects_duplicate_output_names()
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

            let error = session.plan_write_all_outputs(&[west, east]);

            assert!(matches!(
                error,
                Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                    if message.contains("write_all output names must be unique")
                        && message.contains("orders_output")
            ));
            Ok(())
        }

        #[tokio::test]
        async fn plan_write_all_outputs_rejects_missing_connection_before_execution()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(SessionOptions::default())?;
            let west = session.table_from_sql("select 1 as id").await?;
            let east = session.table_from_sql("select 2 as id").await?;
            let west = execute_output_request(
                west,
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let east = execute_output_request(
                east,
                "east_output",
                "east_orders",
                LoadMode::AppendExisting,
            )?;

            let error = session.plan_write_all_outputs(&[west, east]);

            assert!(matches!(
                error,
                Err(DeltaFunnelError::MissingMssqlConnection { output_name })
                    if output_name == "west_output"
            ));
            Ok(())
        }

        #[tokio::test]
        async fn plan_write_all_outputs_accepts_replace_before_execution()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let west = session.table_from_sql("select 1 as id").await?;
            let east = session.table_from_sql("select 2 as id").await?;
            let west = execute_output_request(
                west,
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let east =
                execute_output_request(east, "east_output", "east_orders", LoadMode::Replace)?;

            let planned = session.plan_write_all_outputs(&[west, east])?;

            assert_eq!(planned.len(), 2);
            assert_eq!(planned[1].output_plan().output_name(), "east_output");
            assert_eq!(planned[1].output_plan().load_mode(), LoadMode::Replace);
            assert_eq!(
                planned[1]
                    .output_plan()
                    .lifecycle_plan()
                    .expected_target_state(),
                MssqlTargetTableState::ExistsOrAbsent
            );
            assert!(
                planned[1]
                    .output_plan()
                    .lifecycle_plan()
                    .create_table_sql_required()
            );
            Ok(())
        }

        #[tokio::test]
        async fn plan_write_all_outputs_rejects_dry_run_before_execution()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let west = session.table_from_sql("select 1 as id").await?;
            let west =
                output_request(west, "west_output", "west_orders", LoadMode::AppendExisting)?;

            let error = session.plan_write_all_outputs(&[west]);

            assert!(matches!(
                error,
                Err(DeltaFunnelError::MssqlWorkflowPlanning { message })
                    if message.contains("write_all requires RunMode::Execute")
                        && message.contains("dry_run_all_to_mssql")
            ));
            Ok(())
        }
    }

    mod progress {
        use super::*;

        #[tokio::test]
        async fn write_all_progress_reports_planned_output_positions_and_success()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let west = session.table_from_sql("select 1 as id").await?;
            let east = session.table_from_sql("select 2 as id").await?;
            let west = execute_output_request(
                west,
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let east = execute_output_request(
                east,
                "east_output",
                "east_orders",
                LoadMode::AppendExisting,
            )?;
            let (reporter, events) = recording_progress();

            let report = session
                .write_all_with_progress_and_writer(
                    &[west, east],
                    WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                    reporter,
                    FakeWorkflowWriter::default(),
                )
                .await?;
            assert!(report.all_succeeded());

            let events = events.lock().map_err(|_| "progress event lock poisoned")?;
            assert_eq!(events.len(), 9);
            assert_eq!(events[0].kind, ProgressEventKind::Started);
            assert_eq!(
                events[0].operation,
                Some(ProgressOperation::WriteAllToMssql)
            );
            assert_eq!(events[1].phase, Some(ProgressPhase::PlanningOutput));
            assert_eq!(events[1].output_name.as_deref(), Some("west_output"));
            assert_eq!(events[1].output_index, Some(1));
            assert_eq!(events[1].output_count, Some(2));
            assert_eq!(events[2].phase, Some(ProgressPhase::PlanningOutput));
            assert_eq!(events[2].output_name.as_deref(), Some("east_output"));
            assert_eq!(events[2].output_index, Some(2));
            assert_eq!(events[2].output_count, Some(2));
            assert_eq!(events[3].phase, Some(ProgressPhase::SettingUpStream));
            assert_eq!(events[3].output_name.as_deref(), Some("west_output"));
            assert_eq!(events[3].output_index, Some(1));
            assert_eq!(events[3].output_count, Some(2));
            assert_eq!(events[4].phase, Some(ProgressPhase::Writing));
            assert_eq!(events[4].output_index, Some(1));
            assert_eq!(events[4].rows, Some(1));
            assert_eq!(events[4].batches, Some(1));
            assert_eq!(events[5].phase, Some(ProgressPhase::SettingUpStream));
            assert_eq!(events[5].output_name.as_deref(), Some("east_output"));
            assert_eq!(events[5].output_index, Some(2));
            assert_eq!(events[5].output_count, Some(2));
            assert_eq!(events[6].phase, Some(ProgressPhase::Writing));
            assert_eq!(events[6].output_index, Some(2));
            assert_eq!(events[6].rows, Some(1));
            assert_eq!(events[6].batches, Some(1));
            assert_eq!(events[7].phase, Some(ProgressPhase::ReportingSources));
            assert_eq!(events[7].output_name, None);
            assert_eq!(events[7].output_index, None);
            assert_eq!(events[7].output_count, None);
            assert_eq!(events[8].kind, ProgressEventKind::Completed);
            Ok(())
        }

        #[tokio::test]
        async fn write_all_progress_preserves_profile_shape_and_event_count()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let output = session
                .table_from_sql("select 1 as id union all select 2 as id")
                .await?;
            let output = execute_output_request(
                output,
                "orders_output",
                "orders",
                LoadMode::AppendExisting,
            )?;
            let options = detailed_uncached_write_all_options();

            let capture = TracingCapture::start();
            let without_progress = session
                .write_all_with_options_and_writer(
                    std::slice::from_ref(&output),
                    options,
                    FakeWorkflowWriter::default(),
                )
                .await?;
            assert_eq!(execution_profile_events(&capture).len(), 1);
            drop(capture);

            let (reporter, _progress_events) = recording_progress();
            let capture = TracingCapture::start();
            let with_progress = session
                .write_all_with_progress_and_writer(
                    &[output],
                    options,
                    reporter,
                    FakeWorkflowWriter::default(),
                )
                .await?;
            assert_eq!(execution_profile_events(&capture).len(), 1);

            let [crate::MssqlOutputWriteStatus::Succeeded(without_progress)] =
                without_progress.outputs()
            else {
                return Err("expected successful output without progress".into());
            };
            let [crate::MssqlOutputWriteStatus::Succeeded(with_progress)] = with_progress.outputs()
            else {
                return Err("expected successful output with progress".into());
            };
            let without_progress = without_progress
                .execution_profile()
                .ok_or("expected profile without progress")?;
            let with_progress = with_progress
                .execution_profile()
                .ok_or("expected profile with progress")?;
            assert_eq!(without_progress.scope(), with_progress.scope());
            assert_eq!(without_progress.outcome(), with_progress.outcome());
            assert_eq!(
                without_progress.delta_funnel_row_limit(),
                with_progress.delta_funnel_row_limit()
            );
            let operator_shape = |profile: &crate::QueryExecutionProfile| {
                profile
                    .operators()
                    .iter()
                    .map(|operator| {
                        (
                            operator.operator_name().to_owned(),
                            operator.parent_node_id(),
                            operator.output_partition_count(),
                        )
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                operator_shape(without_progress),
                operator_shape(with_progress)
            );
            Ok(())
        }

        #[tokio::test]
        async fn empty_write_all_emits_no_progress() -> Result<(), Box<dyn std::error::Error>> {
            let session = DeltaFunnelSession::new(SessionOptions::new())?;
            let (reporter, events) = recording_progress();
            let capture = TracingCapture::start();

            let report = session
                .write_all_with_progress_and_writer(
                    &[],
                    detailed_uncached_write_all_options(),
                    reporter,
                    FakeWorkflowWriter::default(),
                )
                .await?;

            assert!(report.is_empty());
            assert!(
                events
                    .lock()
                    .map_err(|_| "progress event lock poisoned")?
                    .is_empty()
            );
            assert!(execution_profile_events(&capture).is_empty());
            Ok(())
        }

        #[tokio::test]
        async fn write_all_progress_distinguishes_reported_and_top_level_failures()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let output = session.table_from_sql("select 1 as id").await?;
            let skipped = session.table_from_sql("select 2 as id").await?;
            let output = execute_output_request(
                output,
                "orders_output",
                "orders",
                LoadMode::AppendExisting,
            )?;
            let skipped = execute_output_request(
                skipped,
                "skipped_output",
                "skipped_orders",
                LoadMode::AppendExisting,
            )?;
            let (reporter, events) = recording_progress();
            let report = session
                .write_all_with_progress_and_writer(
                    &[output, skipped],
                    WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                    reporter,
                    FakeWorkflowWriter::failing_on("orders_output"),
                )
                .await?;
            assert_eq!(report.failed_count(), 1);
            assert_eq!(report.skipped_count(), 1);
            {
                let events = events.lock().map_err(|_| "progress event lock poisoned")?;
                assert_eq!(
                    events.last().map(|event| event.kind),
                    Some(ProgressEventKind::CompletedWithFailures)
                );
                let attempted_outputs = events
                    .iter()
                    .filter(|event| event.phase == Some(ProgressPhase::SettingUpStream))
                    .collect::<Vec<_>>();
                assert_eq!(attempted_outputs.len(), 1);
                assert_eq!(
                    attempted_outputs[0].output_name.as_deref(),
                    Some("orders_output")
                );
                assert_eq!(attempted_outputs[0].output_index, Some(1));
                assert_eq!(attempted_outputs[0].output_count, Some(2));
            }

            let mut session = DeltaFunnelSession::new(SessionOptions::new())?;
            let missing_connection = session.table_from_sql("select 1 as id").await?;
            let missing_connection = execute_output_request(
                missing_connection,
                "missing_connection",
                "missing_connection",
                LoadMode::AppendExisting,
            )?;
            let (reporter, events) = recording_progress();
            let result = session
                .write_all_with_progress_and_writer(
                    &[missing_connection],
                    WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                    reporter,
                    FakeWorkflowWriter::default(),
                )
                .await;
            assert!(result.is_err());
            let events = events.lock().map_err(|_| "progress event lock poisoned")?;
            assert_eq!(events[0].kind, ProgressEventKind::Started);
            assert_eq!(
                events.last().map(|event| event.kind),
                Some(ProgressEventKind::Failed)
            );
            Ok(())
        }

        #[tokio::test]
        async fn write_all_file_progress_is_scoped_to_each_attempted_output()
        -> Result<(), Box<dyn std::error::Error>> {
            let table = RealParquetDeltaTable::new_default("orders")?;
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            session.delta_lake(DeltaSourceConfig::new(
                "orders",
                table.path().to_string_lossy().to_string(),
            ))?;
            let west = session
                .table_from_sql("select id from orders where id <= 2")
                .await?;
            let east = session
                .table_from_sql("select id from orders where id > 2")
                .await?;
            let west = execute_output_request(
                west,
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let east = execute_output_request(
                east,
                "east_output",
                "east_orders",
                LoadMode::AppendExisting,
            )?;
            let (reporter, events) = recording_progress();

            let report = session
                .write_all_with_progress_and_writer(
                    &[west, east],
                    WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                    reporter,
                    FakeWorkflowWriter::default(),
                )
                .await?;
            assert!(report.all_succeeded());

            let events = events.lock().map_err(|_| "progress event lock poisoned")?;
            for output_index in [1, 2] {
                let Some(last) = events.iter().rev().find(|event| {
                    event.output_index == Some(output_index) && event.files_total.is_some()
                }) else {
                    return Err(
                        format!("output {output_index} did not report file progress").into(),
                    );
                };
                assert_eq!(last.files_handled, last.files_total);
                assert!(last.files_total.is_some_and(|total| total > 0));
            }
            Ok(())
        }

        #[tokio::test]
        async fn write_all_cache_file_progress_is_action_level_and_resets_for_outputs()
        -> Result<(), Box<dyn std::error::Error>> {
            let table = RealParquetDeltaTable::new_default("orders")?;
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            session.delta_lake(DeltaSourceConfig::new(
                "orders",
                table.path().to_string_lossy().to_string(),
            ))?;
            let pending_big = session
                .table_from_sql("select id, customer_name from orders")
                .await?;
            let big = session.register_alias("big", &pending_big)?;
            let selected = session
                .table_from_sql("select id from big where id <= 2")
                .await?;
            let big_output =
                execute_output_request(big, "big_output", "big_orders", LoadMode::AppendExisting)?;
            let selected_output = execute_output_request(
                selected,
                "selected_output",
                "selected_orders",
                LoadMode::AppendExisting,
            )?;
            let (reporter, events) = recording_progress();
            let action_task_id = tokio::task::try_id();

            let report = session
                .write_all_with_progress_and_writer(
                    &[big_output, selected_output],
                    WriteAllOptions::default(),
                    reporter,
                    FakeWorkflowWriter::default(),
                )
                .await?;
            assert!(report.all_succeeded());
            assert!(matches!(
                report.cache(),
                WriteAllCacheReport::CacheAliases { .. }
            ));

            let events = events.lock().map_err(|_| "progress event lock poisoned")?;
            let cache_events = events
                .iter()
                .filter(|event| event.phase == Some(ProgressPhase::MaterializingCache))
                .collect::<Vec<_>>();
            assert!(!cache_events.is_empty());
            assert!(
                cache_events
                    .iter()
                    .all(|event| event.task_id == action_task_id)
            );
            assert!(cache_events.iter().all(|event| {
                event.output_name.is_none()
                    && event.output_index.is_none()
                    && event.output_count.is_none()
            }));
            let last_cache_file_event = cache_events
                .iter()
                .rev()
                .find(|event| event.files_total.is_some())
                .ok_or("cache materialization did not report file progress")?;
            assert_eq!(
                last_cache_file_event.files_handled,
                last_cache_file_event.files_total
            );
            let first_output = events
                .iter()
                .find(|event| event.phase == Some(ProgressPhase::SettingUpStream))
                .ok_or("first output did not start")?;
            assert_eq!(first_output.output_name.as_deref(), Some("big_output"));
            assert_eq!(first_output.output_index, Some(1));
            assert_eq!(first_output.output_count, Some(2));
            assert_eq!(first_output.files_total, None);
            let restoration_index = events
                .iter()
                .position(|event| event.phase == Some(ProgressPhase::RestoringCache))
                .ok_or("cache restoration was not reported")?;
            let restoration = &events[restoration_index];
            assert_eq!(restoration.output_name, None);
            assert_eq!(restoration.output_index, None);
            assert_eq!(restoration.output_count, None);
            assert_eq!(restoration.rows, None);
            assert_eq!(restoration.batches, None);
            assert_eq!(restoration.files_total, None);
            let source_reporting_index = events
                .iter()
                .position(|event| event.phase == Some(ProgressPhase::ReportingSources))
                .ok_or("source reporting was not reported")?;
            assert!(restoration_index < source_reporting_index);
            Ok(())
        }

        #[tokio::test]
        async fn write_all_preflight_errors_emit_no_progress()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let west = session.table_from_sql("select 1 as id").await?;
            let east = session.table_from_sql("select 2 as id").await?;
            let west =
                execute_output_request(west, "duplicate", "west_orders", LoadMode::AppendExisting)?;
            let east =
                execute_output_request(east, "duplicate", "east_orders", LoadMode::AppendExisting)?;
            let (reporter, events) = recording_progress();

            let result = session
                .write_all_with_progress_and_writer(
                    &[west, east],
                    WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                    reporter,
                    FakeWorkflowWriter::default(),
                )
                .await;

            assert!(result.is_err());
            assert!(
                events
                    .lock()
                    .map_err(|_| "progress event lock poisoned")?
                    .is_empty()
            );

            let dry_run = session.table_from_sql("select 3 as id").await?;
            let dry_run = output_request(
                dry_run,
                "dry_run",
                "dry_run_orders",
                LoadMode::AppendExisting,
            )?;
            let (reporter, events) = recording_progress();
            let result = session
                .write_all_with_progress_and_writer(
                    &[dry_run],
                    WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                    reporter,
                    FakeWorkflowWriter::default(),
                )
                .await;

            assert!(result.is_err());
            assert!(
                events
                    .lock()
                    .map_err(|_| "progress event lock poisoned")?
                    .is_empty()
            );
            Ok(())
        }
    }

    mod execution_reports {
        use super::*;

        #[tokio::test]
        async fn write_all_with_writer_executes_valid_outputs_in_order()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let west = session
                .table_from_sql("select 1 as id union all select 2 as id")
                .await?;
            let east = session.table_from_sql("select 3 as id").await?;
            let west = execute_output_request(
                west,
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let east = execute_output_request(
                east,
                "east_output",
                "east_orders",
                LoadMode::CreateAndLoad,
            )?;
            let writer = FakeWorkflowWriter::default();
            let calls = writer.calls();
            let capture = TracingCapture::start();

            let report = session.write_all_with_writer(&[west, east], writer).await?;
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0].output_name, "west_output");
            assert_eq!(calls[0].target_table.table(), "west_orders");
            assert_eq!(calls[0].rows, 2);
            assert_eq!(calls[1].output_name, "east_output");
            assert_eq!(calls[1].target_table.table(), "east_orders");
            assert_eq!(calls[1].rows, 1);
            assert_eq!(report.len(), 2);
            assert!(report.all_succeeded());
            assert_eq!(report.outputs()[0].output_name(), "west_output");
            assert_eq!(report.outputs()[1].output_name(), "east_output");
            assert_eq!(report.workflow().outputs(), report.outputs());
            let crate::sql_server::MssqlOutputWriteStatus::Succeeded(west_report) =
                &report.outputs()[0]
            else {
                return Err(
                    format!("expected succeeded status, got {:?}", report.outputs()[0]).into(),
                );
            };
            assert_eq!(west_report.stats().rows_written(), 2);
            assert_eq!(west_report.stats().batches_written(), calls[0].batches);
            assert_eq!(west_report.execution_profile(), None);
            for phase_name in [
                "query_dataframe_planning",
                "query_physical_planning",
                "query_stream_setup",
            ] {
                assert_phase_timing(
                    west_report.phase_timings(),
                    phase_name,
                    PhaseStatus::completed(),
                )?;
            }
            assert_eq!(report.outputs()[0].output_row_count(), RowCount::exact(2));
            assert_batch_shaping(
                report.outputs()[0].batch_shaping(),
                PhaseStatus::completed(),
                calls[0].batches,
                2,
                calls[0].batches,
                2,
            );
            assert_eq!(report.outputs()[1].output_row_count(), RowCount::exact(1));
            assert_batch_shaping(
                report.outputs()[1].batch_shaping(),
                PhaseStatus::completed(),
                calls[1].batches,
                1,
                calls[1].batches,
                1,
            );
            assert_eq!(
                west_report.cleanup(),
                MssqlTargetCleanupStatus::NotApplicable
            );
            assert!(execution_profile_events(&capture).is_empty());
            assert_phase_timing(
                report.phase_timings(),
                OUTPUT_PLANNING_PHASE,
                PhaseStatus::completed(),
            )?;
            assert_phase_timing(
                report.phase_timings(),
                CACHE_PLANNING_PHASE,
                PhaseStatus::completed(),
            )?;
            assert_phase_timing(
                report.phase_timings(),
                WORKFLOW_EXECUTION_PHASE,
                PhaseStatus::completed(),
            )?;
            assert_phase_timing(
                report.phase_timings(),
                SOURCE_REPORTING_PHASE,
                PhaseStatus::completed(),
            )?;
            Ok(())
        }

        #[tokio::test]
        async fn uncached_detailed_mode_profiles_each_output_query()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let (provider, scans) = scan_counting_marker_region_provider("shared")?;
            session
                .context()
                .register_table("shared_source", provider)?;
            let shared = session
                .table_from_sql("select marker from shared_source")
                .await?;
            let first = execute_output_request(
                shared.clone(),
                "first_output",
                "first_orders",
                LoadMode::AppendExisting,
            )?;
            let second = execute_output_request(
                shared,
                "second_output",
                "second_orders",
                LoadMode::AppendExisting,
            )?;
            let capture = TracingCapture::start();

            let report = session
                .write_all_with_options_and_writer(
                    &[first, second],
                    detailed_uncached_write_all_options(),
                    FakeWorkflowWriter::default(),
                )
                .await?;

            assert_eq!(
                report
                    .outputs()
                    .iter()
                    .map(crate::MssqlOutputWriteStatus::output_name)
                    .collect::<Vec<_>>(),
                vec!["first_output", "second_output"]
            );
            for status in report.outputs() {
                let crate::MssqlOutputWriteStatus::Succeeded(output_report) = status else {
                    return Err(format!("expected succeeded status, got {status:?}").into());
                };
                let profile = output_report
                    .execution_profile()
                    .ok_or("expected output query profile")?;
                assert_eq!(profile.scope(), QueryExecutionScope::MssqlOutput);
                assert_eq!(profile.outcome(), QueryExecutionOutcome::Success);
                assert!(!profile.partial());
                assert!(!profile.operators().is_empty());
                for phase_name in [
                    "query_dataframe_planning",
                    "query_physical_planning",
                    "query_stream_setup",
                ] {
                    assert_phase_timing(
                        output_report.phase_timings(),
                        phase_name,
                        PhaseStatus::completed(),
                    )?;
                }
            }
            assert_eq!(scans.load(Ordering::SeqCst), 2);
            let events = execution_profile_events(&capture);
            assert_eq!(events.len(), 2);
            assert!(events.iter().all(|event| {
                event.fields.get("scope").map(String::as_str) == Some("mssql_output")
                    && event.fields.get("outcome").map(String::as_str) == Some("success")
            }));

            Ok(())
        }

        #[tokio::test]
        async fn uncached_detailed_mode_nests_failed_profile_and_skipped_null_once()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let first = session.table_from_sql("select 1 as id").await?;
            let second = session.table_from_sql("select 2 as id").await?;
            let third = session.table_from_sql("select 3 as id").await?;
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
            let capture = TracingCapture::start();

            let report = session
                .write_all_with_options_and_writer(
                    &[first, second, third],
                    detailed_uncached_write_all_options(),
                    FakeWorkflowWriter::failing_on("second_output"),
                )
                .await?;

            let [
                crate::MssqlOutputWriteStatus::Succeeded(success),
                crate::MssqlOutputWriteStatus::Failed(failure),
                skipped,
            ] = report.outputs()
            else {
                return Err("expected one succeeded, one failed, and one skipped output".into());
            };
            assert_eq!(
                success
                    .execution_profile()
                    .ok_or("expected successful output profile")?
                    .outcome(),
                QueryExecutionOutcome::Success
            );
            let context = failure.context().ok_or("expected failure context")?;
            let profile = context
                .report()
                .execution_profile()
                .ok_or("expected failed output query profile")?;
            assert_eq!(profile.scope(), QueryExecutionScope::MssqlOutput);
            assert_eq!(profile.outcome(), QueryExecutionOutcome::Success);
            assert!(!profile.partial());
            assert!(skipped.is_skipped());
            assert_eq!(execution_profile_events(&capture).len(), 2);

            let value = report.to_json_value();
            let failed = &value["workflow"]["outputs"][1];
            assert!(value.get("execution_profile").is_none());
            assert!(value["workflow"].get("execution_profile").is_none());
            assert!(failed.get("execution_profile").is_none());
            assert!(failed["failure"].get("execution_profile").is_none());
            assert!(
                failed["failure"]["context"]
                    .get("execution_profile")
                    .is_none()
            );
            assert_eq!(
                failed["failure"]["context"]["report"]["execution_profile"]["outcome"],
                "success"
            );
            let skipped = &value["workflow"]["outputs"][2];
            assert!(skipped.get("execution_profile").is_none());
            assert!(skipped["skipped"]["execution_profile"].is_null());

            Ok(())
        }

        #[tokio::test]
        async fn write_all_with_writer_reports_delta_sources_for_executed_outputs()
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
            let writer = FakeWorkflowWriter::default();

            let report = session
                .write_all_with_options_and_writer(
                    &[request],
                    WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                    writer,
                )
                .await?;

            assert_eq!(report.outputs().len(), 1);
            assert_eq!(report.outputs()[0].output_name(), "orders_output");
            assert_eq!(report.sources().len(), 1);
            let source = &report.sources()[0];
            assert_eq!(source.source_name(), "orders");
            assert_eq!(source.usage_status(), SourceUsageStatus::Used);
            assert_eq!(source.used_by_output_names(), &["orders_output".to_owned()]);
            assert_eq!(source.provider_stats_reason(), None);
            let stats = source
                .provider_read_stats()
                .ok_or("expected execution provider stats")?;
            assert_eq!(stats.source_name, "orders");
            assert_eq!(stats.snapshot_version, source.snapshot_version());
            assert!(stats.files_started > 0);
            assert_eq!(stats.files_started, stats.files_completed);
            assert!(stats.rows_produced > 0);
            assert!(stats.batches_produced > 0);
            match stats.scan_metadata_exhausted {
                Some(true) => {
                    assert_eq!(
                        source.file_count(),
                        crate::FileCount::exact(stats.files_planned)
                    );
                    assert_eq!(source.file_count_reason(), None);
                }
                Some(false) => {
                    assert_eq!(
                        source.file_count(),
                        crate::FileCount::estimated(stats.files_planned)
                    );
                    assert_eq!(source.file_count_reason(), None);
                }
                None => {
                    assert_eq!(source.file_count(), crate::FileCount::unavailable());
                    assert_eq!(
                        source.file_count_reason(),
                        Some(crate::ReportReasonCode::CapabilityUnavailable)
                    );
                }
            }
            Ok(())
        }

        #[tokio::test]
        async fn write_all_keeps_source_rows_separate_from_output_rows()
        -> Result<(), Box<dyn std::error::Error>> {
            let table = RealParquetDeltaTable::new_default("orders")?;
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            session.delta_lake(DeltaSourceConfig::new(
                "orders",
                table.path().to_string_lossy().to_string(),
            ))?;
            let aggregate = session
                .table_from_sql("select count(*) as order_count from orders")
                .await?;
            let request = execute_output_request(
                aggregate,
                "orders_output",
                "orders_sink",
                LoadMode::AppendExisting,
            )?;
            let writer = FakeWorkflowWriter::default();

            let report = session
                .write_all_with_options_and_writer(
                    &[request],
                    WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                    writer,
                )
                .await?;

            let crate::sql_server::MssqlOutputWriteStatus::Succeeded(output_report) =
                &report.outputs()[0]
            else {
                return Err(
                    format!("expected succeeded status, got {:?}", report.outputs()[0]).into(),
                );
            };
            assert_eq!(output_report.stats().rows_written(), 1);
            assert_eq!(output_report.output_row_count(), RowCount::exact(1));
            let source = report
                .sources()
                .first()
                .ok_or("expected executed source report")?;
            let stats = source
                .provider_read_stats()
                .ok_or("expected execution provider stats")?;
            assert_eq!(stats.rows_produced, u64::try_from(table.rows())?);
            assert_ne!(stats.rows_produced, output_report.stats().rows_written());
            assert_ne!(
                stats.rows_produced,
                output_report.output_row_count().exact_value().unwrap_or(0)
            );
            Ok(())
        }
    }

    mod cache_behavior {
        use super::*;

        #[tokio::test]
        async fn write_all_auto_no_candidate_uses_uncached_path()
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
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let east = execute_output_request(
                east,
                "east_output",
                "east_orders",
                LoadMode::AppendExisting,
            )?;
            let writer = FakeWorkflowWriter::default();
            let calls = writer.calls();

            let report = session.write_all_with_writer(&[west, east], writer).await?;
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0].output_name, "west_output");
            assert_eq!(calls[0].rows, 1);
            assert_eq!(calls[1].output_name, "east_output");
            assert_eq!(calls[1].rows, 1);
            assert_eq!(source_scans.load(Ordering::SeqCst), 2);
            assert!(report.all_succeeded());
            assert!(matches!(
                report.cache(),
                WriteAllCacheReport::NoCache {
                    reason: WriteAllNoCacheReason::NoSharedRegisteredDerivedAlias,
                    skipped_candidates
                } if skipped_candidates.is_empty()
            ));
            Ok(())
        }

        #[tokio::test]
        async fn write_all_progress_caches_shared_alias_once_for_direct_and_dependent_outputs()
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
            let big_output = execute_output_request(
                big.clone(),
                "big_output",
                "big_orders",
                LoadMode::AppendExisting,
            )?;
            let west_output = execute_output_request(
                west,
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let writer = FakeWorkflowWriter::default();
            let calls = writer.calls();
            let (reporter, _events) = recording_progress();

            let report = session
                .write_all_with_progress_and_writer(
                    &[big_output, west_output],
                    WriteAllOptions::default(),
                    reporter,
                    writer,
                )
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
                assert!(report.all_succeeded());
            }
            let WriteAllCacheReport::CacheAliases {
                aliases,
                skipped_candidates,
            } = report.cache()
            else {
                return Err(
                    format!("expected cache aliases report, got {:?}", report.cache()).into(),
                );
            };
            assert!(skipped_candidates.is_empty());
            assert_eq!(aliases.len(), 1);
            assert_eq!(aliases[0].table_id(), big.id());
            assert_eq!(aliases[0].alias(), "big");
            assert_eq!(aliases[0].output_indexes(), &[0, 1]);
            assert_eq!(
                aliases[0].status(),
                WriteAllCacheAliasStatus::MaterializedAndRestored
            );

            let restored_big_factory = session.lazy_table_batch_stream_factory(big, None, None);
            let restored_big_rows = collect_stream_row_count(restored_big_factory().await?).await?;
            assert_eq!(restored_big_rows, 2);
            assert_eq!(source_scans.load(Ordering::SeqCst), 2);
            Ok(())
        }

        #[tokio::test]
        async fn write_all_report_debug_redacts_connections_and_retained_sql()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let (source_provider, _source_scans) = scan_counting_marker_region_provider("shared")?;
            session
                .context()
                .register_table("big_source", source_provider)?;
            let pending_big = session
                .table_from_sql(
                    "select 'super-secret-literal' as marker, region \
                 from big_source",
                )
                .await?;
            let big = session.register_alias("big", &pending_big)?;
            let west = session
                .table_from_sql("select marker from big where region = 'west'")
                .await?;
            let big_output =
                execute_output_request(big, "big_output", "big_orders", LoadMode::AppendExisting)?;
            let override_target_config =
                MssqlTargetConfig::new(MssqlTargetTable::new("dbo", "west_orders")?)
                    .with_load_mode(LoadMode::AppendExisting)
                    .with_connection(override_connection()?);
            let west_output = OutputWritePlan::new(
                west,
                MssqlOutputTarget::new("west_output", override_target_config, RunMode::Execute),
            );
            let writer = FakeWorkflowWriter::default();

            let report = session
                .write_all_with_writer(&[big_output, west_output], writer)
                .await?;

            let debug = format!("{report:?}");
            assert!(debug.contains("warehouse-primary"));
            assert!(debug.contains("warehouse-override"));
            assert!(debug.contains("CacheAliases"));
            assert!(debug.contains("MaterializedAndRestored"));
            assert!(!debug.contains("secret-token"));
            assert!(!debug.contains("override-secret"));
            assert!(!debug.contains("super-secret-literal"));
            Ok(())
        }

        #[tokio::test]
        async fn write_all_auto_caches_multiple_shared_aliases_for_dependent_outputs()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let (big_source_provider, big_source_scans) =
                scan_counting_marker_region_provider("big")?;
            let (names_source_provider, names_source_scans) =
                scan_counting_marker_region_provider("names")?;
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
            let west_output = execute_output_request(
                west,
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let east_output = execute_output_request(
                east,
                "east_output",
                "east_orders",
                LoadMode::AppendExisting,
            )?;
            let writer = FakeWorkflowWriter::default();
            let calls = writer.calls();

            let report = session
                .write_all_with_writer(&[west_output, east_output], writer)
                .await?;
            {
                let calls = calls
                    .lock()
                    .map_err(|_| "fake workflow call lock poisoned")?;

                assert_eq!(calls.len(), 2);
                assert_eq!(calls[0].output_name, "west_output");
                assert_eq!(calls[0].rows, 1);
                assert_eq!(calls[1].output_name, "east_output");
                assert_eq!(calls[1].rows, 1);
                assert_eq!(big_source_scans.load(Ordering::SeqCst), 1);
                assert_eq!(names_source_scans.load(Ordering::SeqCst), 1);
                assert!(report.all_succeeded());
            }
            for status in report.outputs() {
                let crate::MssqlOutputWriteStatus::Succeeded(output_report) = status else {
                    return Err(format!("expected succeeded status, got {status:?}").into());
                };
                assert_eq!(output_report.execution_profile(), None);
            }
            let WriteAllCacheReport::CacheAliases {
                aliases,
                skipped_candidates,
            } = report.cache()
            else {
                return Err(
                    format!("expected cache aliases report, got {:?}", report.cache()).into(),
                );
            };
            assert!(skipped_candidates.is_empty());
            assert_eq!(aliases.len(), 2);
            assert_eq!(aliases[0].table_id(), big.id());
            assert_eq!(aliases[0].alias(), "big");
            assert_eq!(aliases[0].output_indexes(), &[0, 1]);
            assert_eq!(
                aliases[0].status(),
                WriteAllCacheAliasStatus::MaterializedAndRestored
            );
            assert_eq!(aliases[1].table_id(), names.id());
            assert_eq!(aliases[1].alias(), "names");
            assert_eq!(aliases[1].output_indexes(), &[0, 1]);
            assert_eq!(
                aliases[1].status(),
                WriteAllCacheAliasStatus::MaterializedAndRestored
            );

            let restored_big_factory = session.lazy_table_batch_stream_factory(big, None, None);
            let restored_names_factory = session.lazy_table_batch_stream_factory(names, None, None);
            assert_eq!(
                collect_stream_row_count(restored_big_factory().await?).await?,
                2
            );
            assert_eq!(
                collect_stream_row_count(restored_names_factory().await?).await?,
                2
            );
            assert_eq!(big_source_scans.load(Ordering::SeqCst), 2);
            assert_eq!(names_source_scans.load(Ordering::SeqCst), 2);
            Ok(())
        }

        #[tokio::test]
        async fn write_all_auto_profiles_direct_replanned_and_unrelated_output_queries()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let (shared_provider, shared_scans) = scan_counting_marker_region_provider("shared")?;
            let (unrelated_provider, unrelated_scans) =
                scan_counting_marker_region_provider("unrelated")?;
            session
                .context()
                .register_table("big_source", shared_provider)?;
            session
                .context()
                .register_table("unrelated_source", unrelated_provider)?;
            let pending_big = session
                .table_from_sql("select marker, region from big_source")
                .await?;
            let big = session.register_alias("big", &pending_big)?;
            let west = session
                .table_from_sql("select marker from big where region = 'west'")
                .await?;
            let unrelated = session
                .table_from_sql("select marker from unrelated_source where region = 'west'")
                .await?;
            let big_output =
                execute_output_request(big, "big_output", "big_orders", LoadMode::AppendExisting)?;
            let west_output = execute_output_request(
                west,
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let unrelated_output = execute_output_request(
                unrelated,
                "unrelated_output",
                "unrelated_orders",
                LoadMode::AppendExisting,
            )?;
            let writer = FakeWorkflowWriter::default();
            let calls = writer.calls();

            let report = session
                .write_all_with_options_and_writer(
                    &[big_output, unrelated_output, west_output],
                    WriteAllOptions::new()
                        .with_execution_profile_mode(ExecutionProfileMode::Detailed),
                    writer,
                )
                .await?;
            {
                let calls = calls
                    .lock()
                    .map_err(|_| "fake workflow call lock poisoned")?;

                assert_eq!(calls.len(), 3);
                assert_eq!(calls[0].output_name, "big_output");
                assert_eq!(calls[0].rows, 2);
                assert_eq!(calls[1].output_name, "unrelated_output");
                assert_eq!(calls[1].rows, 1);
                assert_eq!(calls[2].output_name, "west_output");
                assert_eq!(calls[2].rows, 1);
                assert_eq!(shared_scans.load(Ordering::SeqCst), 1);
                assert_eq!(unrelated_scans.load(Ordering::SeqCst), 1);
                assert!(report.all_succeeded());
            }
            for status in report.outputs() {
                let crate::MssqlOutputWriteStatus::Succeeded(output_report) = status else {
                    return Err(format!("expected succeeded status, got {status:?}").into());
                };
                let profile = output_report
                    .execution_profile()
                    .ok_or("expected cached output query profile")?;
                assert_eq!(profile.scope(), QueryExecutionScope::MssqlOutput);
                assert_eq!(profile.outcome(), QueryExecutionOutcome::Success);
                assert!(!profile.partial());
                assert!(!profile.operators().is_empty());
                for phase_name in [
                    "query_dataframe_planning",
                    "query_physical_planning",
                    "query_stream_setup",
                ] {
                    assert_phase_timing(
                        output_report.phase_timings(),
                        phase_name,
                        PhaseStatus::completed(),
                    )?;
                }
            }
            Ok(())
        }

        #[tokio::test]
        async fn write_all_disabled_cache_mode_uses_uncached_path()
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
            let big_output =
                execute_output_request(big, "big_output", "big_orders", LoadMode::AppendExisting)?;
            let west_output = execute_output_request(
                west,
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let writer = FakeWorkflowWriter::default();
            let calls = writer.calls();

            let report = session
                .write_all_with_options_and_writer(
                    &[big_output, west_output],
                    WriteAllOptions::new().with_cache_mode(WriteAllCacheMode::Disabled),
                    writer,
                )
                .await?;
            let calls = calls
                .lock()
                .map_err(|_| "fake workflow call lock poisoned")?;

            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0].rows, 2);
            assert_eq!(calls[1].rows, 1);
            assert_eq!(source_scans.load(Ordering::SeqCst), 2);
            assert!(report.all_succeeded());
            assert_eq!(report.cache(), &WriteAllCacheReport::Disabled);
            Ok(())
        }
    }

    mod failure_behavior {
        use super::*;

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
            let west_output = execute_output_request(
                west,
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let east_output = execute_output_request(
                east,
                "east_output",
                "east_orders",
                LoadMode::AppendExisting,
            )?;
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
                return Err(
                    format!("expected cache aliases report, got {:?}", report.cache()).into(),
                );
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

            let restored_big_factory = session.lazy_table_batch_stream_factory(big, None, None);
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
            let west_output = execute_output_request(
                west,
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let east_output = execute_output_request(
                east,
                "east_output",
                "east_orders",
                LoadMode::AppendExisting,
            )?;
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

            let restored_error = match session.batch_stream_for_lazy_table(&big, None).await {
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
            let (big_source_provider, big_source_scans) =
                scan_counting_marker_region_provider("big")?;
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
            let west_output = execute_output_request(
                west,
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let east_output = execute_output_request(
                east,
                "east_output",
                "east_orders",
                LoadMode::AppendExisting,
            )?;
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

            let restored_big_factory = session.lazy_table_batch_stream_factory(big, None, None);
            assert_eq!(
                collect_stream_row_count(restored_big_factory().await?).await?,
                2
            );
            assert_eq!(big_source_scans.load(Ordering::SeqCst), 2);

            let restored_names_error = match session.batch_stream_for_lazy_table(&names, None).await
            {
                Ok(stream) => match collect_stream_row_count(stream).await {
                    Ok(rows) => {
                        return Err(format!(
                            "expected restored names read to fail, got {rows} rows"
                        )
                        .into());
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
            let west_output = execute_output_request(
                west,
                "west_output",
                "west_orders",
                LoadMode::AppendExisting,
            )?;
            let east_output = execute_output_request(
                east,
                "east_output",
                "east_orders",
                LoadMode::AppendExisting,
            )?;
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
                return Err(
                    format!("expected cache aliases report, got {:?}", report.cache()).into(),
                );
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

            let restored_big_factory = session.lazy_table_batch_stream_factory(big, None, None);
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
            let capture = TracingCapture::start();

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
            let crate::MssqlOutputWriteStatus::Failed(failure) = &report.outputs()[1] else {
                return Err("expected failed second output".into());
            };
            assert_eq!(
                failure
                    .context()
                    .ok_or("expected failure context")?
                    .report()
                    .execution_profile(),
                None
            );
            assert_eq!(
                report.outputs()[1].output_row_count(),
                RowCount::partial(calls[1].rows)
            );
            assert_batch_shaping(
                report.outputs()[1].batch_shaping(),
                PhaseStatus::failed(),
                calls[1].batches,
                calls[1].rows,
                calls[1].batches,
                calls[1].rows,
            );
            assert!(report.outputs()[2].is_skipped());
            assert_eq!(report.outputs()[2].output_name(), "third_output");
            assert_eq!(
                report.outputs()[2].output_row_count(),
                RowCount::unavailable()
            );
            assert_batch_shaping(
                report.outputs()[2].batch_shaping(),
                PhaseStatus::skipped(ReportReasonCode::PriorFailure),
                0,
                0,
                0,
                0,
            );
            assert!(execution_profile_events(&capture).is_empty());
            Ok(())
        }

        #[tokio::test]
        async fn write_all_with_writer_reports_physical_planning_failure_before_writer()
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
            let capture = TracingCapture::start();

            let report = session
                .write_all_with_options_and_writer(
                    &[first, failing, third],
                    detailed_uncached_write_all_options(),
                    writer,
                )
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
            let crate::MssqlOutputWriteStatus::Succeeded(first_report) = &report.outputs()[0]
            else {
                return Err("expected succeeded first output".into());
            };
            assert_eq!(
                first_report
                    .execution_profile()
                    .ok_or("expected first output profile")?
                    .outcome(),
                QueryExecutionOutcome::Success
            );
            let crate::MssqlOutputWriteStatus::Failed(failure) = &report.outputs()[1] else {
                return Err("expected failed output status".into());
            };
            assert_eq!(failure.output_name(), "failing_output");
            let context = failure.context().ok_or("expected query failure context")?;
            assert_eq!(context.phase(), MssqlWritePhase::QueryPhysicalPlanning);
            assert_eq!(context.report().execution_profile(), None);
            assert_eq!(report.outputs()[1].output_row_count(), RowCount::partial(0));
            assert_batch_shaping(
                report.outputs()[1].batch_shaping(),
                PhaseStatus::failed(),
                0,
                0,
                0,
                0,
            );
            assert_phase_timing(
                report.outputs()[1].phase_timings(),
                "query_dataframe_planning",
                PhaseStatus::completed(),
            )?;
            assert_phase_timing(
                report.outputs()[1].phase_timings(),
                "query_physical_planning",
                PhaseStatus::failed(),
            )?;
            assert_phase_timing(
                report.outputs()[1].phase_timings(),
                "query_stream_setup",
                PhaseStatus::not_started(ReportReasonCode::PriorFailure),
            )?;
            assert!(report.outputs()[2].is_skipped());
            assert_eq!(report.outputs()[2].output_name(), "third_output");
            assert_eq!(
                report.outputs()[2].output_row_count(),
                RowCount::unavailable()
            );
            assert_eq!(execution_profile_events(&capture).len(), 1);
            assert_batch_shaping(
                report.outputs()[2].batch_shaping(),
                PhaseStatus::skipped(ReportReasonCode::PriorFailure),
                0,
                0,
                0,
                0,
            );
            Ok(())
        }

        #[tokio::test]
        async fn detailed_write_all_profiles_stream_setup_failure_as_error()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            session.context().register_table(
                "setup_failure_source",
                stream_setup_failing_marker_region_provider()?,
            )?;
            let writer = FakeWorkflowWriter::default();
            let calls = writer.calls();
            let capture = TracingCapture::start();

            let report = run_two_output_failure_case(
                &mut session,
                "select marker from setup_failure_source",
                writer,
            )
            .await?;

            assert_profiled_failure(
                &report,
                MssqlWritePhase::QueryStreamSetup,
                QueryExecutionOutcome::Error,
            )?;
            assert!(
                calls
                    .lock()
                    .map_err(|_| "fake workflow call lock poisoned")?
                    .is_empty()
            );
            let events = execution_profile_events(&capture);
            assert_eq!(events.len(), 1);
            assert_eq!(
                events[0].fields.get("outcome").map(String::as_str),
                Some("error")
            );
            Ok(())
        }

        #[tokio::test]
        async fn detailed_write_all_profiles_upstream_failure_as_error()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let capture = TracingCapture::start();

            let report = run_two_output_failure_case(
                &mut session,
                "select cast(1 as bigint) / cast(0 as bigint) as value",
                FakeWorkflowWriter::default(),
            )
            .await?;

            assert_profiled_failure(
                &report,
                MssqlWritePhase::PollBatchStream,
                QueryExecutionOutcome::Error,
            )?;
            assert_eq!(execution_profile_events(&capture).len(), 1);
            Ok(())
        }

        #[tokio::test]
        async fn detailed_write_all_profiles_early_writer_stop_as_cancelled()
        -> Result<(), Box<dyn std::error::Error>> {
            let mut session = DeltaFunnelSession::new(
                SessionOptions::new().with_default_mssql_connection(secret_connection()?),
            )?;
            let capture = TracingCapture::start();

            let report = run_two_output_failure_case(
                &mut session,
                "select 1 as id",
                FakeWorkflowWriter::failing_before_poll_on("failing_output"),
            )
            .await?;

            assert_profiled_failure(
                &report,
                MssqlWritePhase::Connect,
                QueryExecutionOutcome::Cancelled,
            )?;
            let events = execution_profile_events(&capture);
            assert_eq!(events.len(), 1);
            assert_eq!(
                events[0].fields.get("outcome").map(String::as_str),
                Some("cancelled")
            );
            Ok(())
        }
    }

    mod validation {
        use super::*;

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
    }
}
