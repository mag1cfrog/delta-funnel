use async_trait::async_trait;
use datafusion::prelude::{DataFrame, SessionContext};
use tracing::Instrument;

use crate::{
    DeltaFunnelError, ExecutionProfileMode, LoadMode, MssqlOutputBatchStream,
    MssqlOutputProfileCallback, MssqlOutputQueryError, MssqlOutputQueryExecution,
    MssqlOutputQueryFuture, MssqlTargetCleanupStatus, MssqlTargetOutputPlan, MssqlWriteBackend,
    MssqlWriteFailureContext, MssqlWritePhase, MssqlWriteReport, PhaseTimingReport,
    QueryExecutionProfile, QueryExecutionScope, ReportReasonCode, ResolvedMssqlTarget,
    TimelineSpanStatus, ValidationOptions, observability, plan_mssql_target_for_resolved_output,
    profiling::{
        OperationStageContext, OperationStageTrace, OperationTraceContext, OperationTraceKind,
        OperationTracePhase, ProcessOperationPhaseTracker,
    },
    progress::{ProgressEvent, ProgressOperation, ProgressPhase, ProgressReporter},
    query_engine::datafusion::{QueryTraceIdentity, with_query_planning_activity},
    report::{OperationTimelineRecorder, PhaseTimer},
    sql_server::write_planned_output_batches_to_mssql_for_workflow,
};

use super::super::{
    DeltaFunnelSession, OutputWritePlan, PlannedMssqlOutput, RegisteredDerivedTable,
    RegisteredSessionSource, RunMode,
    errors::datafusion_handoff_setup_error,
    query_handoff::{
        QueryStreamSetup, SharedProviderStatsSnapshots, batch_stream_for_physical_plan,
        dataframe_for_lazy_table_from_session_parts,
    },
    registry::PendingDerivedTable,
};

pub(in crate::orchestrator::session) const OUTPUT_SCHEMA_PLANNING_PHASE: &str =
    "output_schema_planning";
pub(in crate::orchestrator::session) const SQL_TARGET_PLANNING_PHASE: &str = "sql_target_planning";
const VALIDATION_PHASE: &str = "validation";
const SINGLE_OUTPUT_STAGE_OWNER_ID: u64 = 1;
pub(super) const QUERY_DATAFRAME_PLANNING_PHASE: &str = "query_dataframe_planning";
pub(super) const QUERY_PHYSICAL_PLANNING_PHASE: &str = "query_physical_planning";
pub(super) const QUERY_STREAM_SETUP_PHASE: &str = "query_stream_setup";
pub(super) const QUERY_PHASE_NAMES: [&str; 3] = [
    QUERY_DATAFRAME_PLANNING_PHASE,
    QUERY_PHYSICAL_PLANNING_PHASE,
    QUERY_STREAM_SETUP_PHASE,
];

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
        self.plan_mssql_output_with_timeline(request, None, None, None)
    }

    pub(super) fn plan_mssql_output_with_timeline(
        &self,
        request: &OutputWritePlan,
        timeline: Option<&OperationTimelineRecorder>,
        trace_context: Option<&OperationTraceContext>,
        stage_owner_id: Option<u64>,
    ) -> Result<PlannedMssqlOutput, DeltaFunnelError> {
        let mut phase_timings = self.phase_timings_for_lazy_table(request.table())?;

        let schema_timer = PhaseTimer::start(OUTPUT_SCHEMA_PLANNING_PHASE);
        let schema_span = start_write_span(
            trace_context,
            timeline,
            "Plan output schema",
            "delta_funnel.write.planning",
            "Output schema planning",
            stage_owner_id,
        )
        .map(|span| {
            span.with_attribute(
                "output_name",
                request.target().output_name().to_owned().into(),
            )
        });
        let schema = match self.schema_for_lazy_table(request.table()) {
            Ok(schema) => {
                phase_timings.push(schema_timer.completed());
                complete_write_span(schema_span);
                schema
            }
            Err(error) => {
                phase_timings.push(schema_timer.failed());
                fail_write_span(schema_span);
                return Err(error);
            }
        };

        let target_timer = PhaseTimer::start(SQL_TARGET_PLANNING_PHASE);
        let target_span = start_write_span(
            trace_context,
            timeline,
            "Plan SQL Server target",
            "delta_funnel.write.planning",
            "SQL Server target planning",
            stage_owner_id,
        )
        .map(|span| {
            span.with_attribute(
                "output_name",
                request.target().output_name().to_owned().into(),
            )
        });
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
                    fail_write_span(target_span);
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
                complete_write_span(target_span);
                output_plan
            }
            Err(error) => {
                phase_timings.push(target_timer.failed());
                fail_write_span(target_span);
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
        self.write_to_mssql_with_profile_mode(request, ExecutionProfileMode::Disabled)
            .await
    }

    /// Writes one selected lazy table with optional detailed query profiling.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`DeltaFunnelSession::write_to_mssql`].
    pub async fn write_to_mssql_with_profile_mode(
        &self,
        request: &OutputWritePlan,
        profile_mode: ExecutionProfileMode,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        self.run_mssql_write_with_tracing(
            request,
            &mut MssqlOneOutputSinkWriter,
            profile_mode,
            None,
        )
        .await
    }

    pub(crate) async fn write_to_mssql_with_reporter(
        &self,
        request: &OutputWritePlan,
        reporter: ProgressReporter,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        self.write_to_mssql_with_profile_mode_and_reporter(
            request,
            ExecutionProfileMode::Disabled,
            reporter,
        )
        .await
    }

    pub(crate) async fn write_to_mssql_with_profile_mode_and_reporter(
        &self,
        request: &OutputWritePlan,
        profile_mode: ExecutionProfileMode,
        reporter: ProgressReporter,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        self.run_mssql_write_with_tracing(
            request,
            &mut MssqlOneOutputSinkWriter,
            profile_mode,
            Some(&reporter),
        )
        .await
    }

    async fn run_mssql_write_with_tracing<W>(
        &self,
        request: &OutputWritePlan,
        writer: &mut W,
        profile_mode: ExecutionProfileMode,
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
                    self.run_mssql_write_with_reporter(request, writer, profile_mode, reporter)
                        .await
                }
                None => {
                    self.plan_and_write_mssql_output(request, writer, profile_mode, None)
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
        self.write_to_mssql_with_writer_and_profile_mode(
            request,
            writer,
            ExecutionProfileMode::Disabled,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn write_to_mssql_with_writer_and_profile_mode<W>(
        &self,
        request: &OutputWritePlan,
        writer: &mut W,
        profile_mode: ExecutionProfileMode,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>
    where
        W: OrchestratorMssqlOutputWriter,
    {
        ensure_execute_run_mode(request.target().run_mode())?;
        self.plan_and_write_mssql_output(request, writer, profile_mode, None)
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
        self.run_mssql_write_with_reporter(
            request,
            writer,
            ExecutionProfileMode::Disabled,
            &reporter,
        )
        .await
    }

    async fn run_mssql_write_with_reporter<W>(
        &self,
        request: &OutputWritePlan,
        writer: &mut W,
        profile_mode: ExecutionProfileMode,
        reporter: &ProgressReporter,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>
    where
        W: OrchestratorMssqlOutputWriter,
    {
        reporter.emit(&ProgressEvent::started(ProgressOperation::WriteToMssql));
        let result = self
            .plan_and_write_mssql_output(request, writer, profile_mode, Some(reporter))
            .await;
        reporter.emit(&if result.is_ok() {
            ProgressEvent::completed()
        } else {
            ProgressEvent::failed()
        });
        result
    }

    pub(super) fn mssql_output_query_factory_with_trace_context(
        &self,
        planned: PlannedMssqlOutput,
        provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
        progress: Option<ProgressReporter>,
        profile_mode: ExecutionProfileMode,
        trace_context: Option<OperationTraceContext>,
        stage_owner_id: Option<u64>,
    ) -> Box<dyn FnOnce() -> MssqlOutputQueryFuture + Send> {
        let context = self.context.clone();
        let sources = self.sources.clone();
        let derived_tables = self.derived_tables.clone();
        let pending_derived_tables = self.pending_derived_tables.clone();

        Box::new(move || {
            Box::pin(async move {
                create_mssql_output_query_execution_with_trace_context(
                    &context,
                    &sources,
                    &derived_tables,
                    &pending_derived_tables,
                    &planned,
                    provider_stats_snapshots,
                    progress,
                    profile_mode,
                    trace_context.as_ref(),
                    stage_owner_id,
                    None,
                )
                .await
            })
        })
    }

    async fn plan_and_write_mssql_output<W>(
        &self,
        request: &OutputWritePlan,
        writer: &mut W,
        profile_mode: ExecutionProfileMode,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>
    where
        W: OrchestratorMssqlOutputWriter,
    {
        let trace_context = OperationTraceContext::start(
            OperationTraceKind::MssqlWrite,
            (profile_mode == ExecutionProfileMode::Detailed).then(OperationTimelineRecorder::start),
        );
        let timeline = trace_context
            .as_ref()
            .and_then(OperationTraceContext::timeline);
        let mut process_phases = ProcessOperationPhaseTracker::start(
            trace_context.as_ref(),
            OperationTracePhase::Planning,
        );
        let result = async {
            let output_name = Some(request.target().output_name());
            if let Some(reporter) = reporter {
                reporter.emit(&ProgressEvent::phase_changed(
                    ProgressPhase::PlanningOutput,
                    output_name,
                ));
            }
            let planned = self.plan_mssql_output_with_timeline(
                request,
                timeline,
                trace_context.as_ref(),
                Some(SINGLE_OUTPUT_STAGE_OWNER_ID),
            )?;
            if let Some(reporter) = reporter {
                reporter.emit(&ProgressEvent::phase_changed(
                    ProgressPhase::SettingUpStream,
                    output_name,
                ));
            }
            let MssqlOutputQueryExecution {
                stream: batches,
                query_phase_timings,
                attach_profile_to_result,
            } = create_mssql_output_query_execution_with_trace_context(
                &self.context,
                &self.sources,
                &self.derived_tables,
                &self.pending_derived_tables,
                &planned,
                None,
                reporter.cloned(),
                profile_mode,
                trace_context.as_ref(),
                Some(SINGLE_OUTPUT_STAGE_OWNER_ID),
                Some(&mut process_phases),
            )
            .await
            .map_err(|failure| {
                finish_mssql_write_error_timeline(
                    failure.error,
                    timeline,
                    request.target().output_name(),
                )
            })?;

            let mut phase_timings = planned.phase_timings().to_vec();
            phase_timings.extend(query_phase_timings);
            let result = writer
                .write_output_with_stage_context(
                    planned.output_plan().clone(),
                    planned.resolved_target().clone(),
                    batches,
                    self.options.mssql_write_backend(),
                    self.options.validation_options(),
                    reporter,
                    OperationStageContext::new(
                        trace_context.as_ref(),
                        Some(SINGLE_OUTPUT_STAGE_OWNER_ID),
                    ),
                )
                .await;
            process_phases.transition_with_result(
                if result.is_ok() { "ok" } else { "error" },
                OperationTracePhase::Finalization,
            );
            let result = match attach_profile_to_result {
                Some(attach_profile) => attach_profile(result),
                None => result,
            };
            let result = match result {
                Ok(report) => {
                    Ok(ensure_validation_phase_timing(report).with_phase_timings(phase_timings))
                }
                Err(error) => {
                    let error = prepend_mssql_write_phase_timings(error, phase_timings);
                    Err(error)
                }
            };
            let result =
                finish_mssql_write_timeline(result, timeline, request.target().output_name());
            process_phases.finish("ok");
            result
        }
        .await;
        process_phases.finish(if result.is_ok() { "ok" } else { "error" });
        if let Some(context) = &trace_context {
            context.record_process_result(if result.is_ok() { "ok" } else { "error" });
        }
        result
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "query creation needs the session registries plus per-output reporting state"
)]
pub(super) async fn create_mssql_output_query_execution_with_trace_context(
    context: &SessionContext,
    sources: &[RegisteredSessionSource],
    derived_tables: &[RegisteredDerivedTable],
    pending_derived_tables: &[PendingDerivedTable],
    planned: &PlannedMssqlOutput,
    provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
    progress: Option<ProgressReporter>,
    profile_mode: ExecutionProfileMode,
    trace_context: Option<&OperationTraceContext>,
    stage_owner_id: Option<u64>,
    process_phases: Option<&mut ProcessOperationPhaseTracker>,
) -> Result<MssqlOutputQueryExecution, MssqlOutputQueryError> {
    let timeline = trace_context.and_then(OperationTraceContext::timeline);
    let mut query_phase_timings = Vec::with_capacity(QUERY_PHASE_NAMES.len());

    let dataframe_timer = PhaseTimer::start(QUERY_DATAFRAME_PLANNING_PHASE);
    let dataframe_span = start_write_span(
        trace_context,
        timeline,
        "Build query DataFrame",
        "delta_funnel.write.query",
        "Query DataFrame planning",
        stage_owner_id,
    )
    .map(|span| {
        span.with_attribute(
            "output_name",
            planned.resolved_target().output_name().to_owned().into(),
        )
    });
    let dataframe = match dataframe_for_lazy_table_from_session_parts(
        context,
        planned.table(),
        sources,
        derived_tables,
        pending_derived_tables,
    )
    .await
    {
        Ok(dataframe) => dataframe,
        Err(source) => {
            fail_write_span(dataframe_span);
            return Err(mssql_output_query_error(
                planned,
                MssqlWritePhase::QueryDataFramePlanning,
                query_phase_timings,
                dataframe_timer,
                source,
                None,
            ));
        }
    };
    complete_write_span(dataframe_span);
    query_phase_timings.push(dataframe_timer.completed());

    create_mssql_output_query_execution_from_dataframe_with_trace_context(
        context,
        planned,
        dataframe,
        query_phase_timings,
        provider_stats_snapshots,
        progress,
        profile_mode,
        trace_context,
        stage_owner_id,
        process_phases,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "query creation carries timing, progress, and profiling state"
)]
pub(super) async fn create_mssql_output_query_execution_from_dataframe_with_trace_context(
    context: &SessionContext,
    planned: &PlannedMssqlOutput,
    dataframe: DataFrame,
    mut query_phase_timings: Vec<PhaseTimingReport>,
    provider_stats_snapshots: Option<SharedProviderStatsSnapshots>,
    progress: Option<ProgressReporter>,
    profile_mode: ExecutionProfileMode,
    trace_context: Option<&OperationTraceContext>,
    stage_owner_id: Option<u64>,
    process_phases: Option<&mut ProcessOperationPhaseTracker>,
) -> Result<MssqlOutputQueryExecution, MssqlOutputQueryError> {
    let timeline = trace_context.and_then(OperationTraceContext::timeline);
    let trace_identity = trace_context.cloned().and_then(|context| {
        QueryTraceIdentity::new(
            context,
            QueryExecutionScope::MssqlOutput,
            Some(planned.resolved_target().output_name()),
        )
    });
    let physical_plan_timer = PhaseTimer::start(QUERY_PHYSICAL_PLANNING_PHASE);
    let physical_plan_span = start_write_span(
        trace_context,
        timeline,
        "Build physical plan",
        "delta_funnel.write.query",
        "Query physical planning",
        stage_owner_id,
    )
    .map(|span| {
        span.with_attribute(
            "output_name",
            planned.resolved_target().output_name().to_owned().into(),
        )
    });
    let physical_plan_result = match &trace_identity {
        Some(trace_identity) => {
            with_query_planning_activity(trace_identity.clone(), dataframe.create_physical_plan())
                .await
        }
        None => dataframe.create_physical_plan().await,
    };
    let physical_plan = match physical_plan_result {
        Ok(physical_plan) => physical_plan,
        Err(error) => {
            fail_write_span(physical_plan_span);
            return Err(mssql_output_query_error(
                planned,
                MssqlWritePhase::QueryPhysicalPlanning,
                query_phase_timings,
                physical_plan_timer,
                datafusion_handoff_setup_error("physical_plan", error),
                None,
            ));
        }
    };
    complete_write_span(physical_plan_span);
    query_phase_timings.push(physical_plan_timer.completed());
    if let Some(process_phases) = process_phases {
        process_phases.transition(OperationTracePhase::Execution);
    }

    let stream_setup_timer = PhaseTimer::start(QUERY_STREAM_SETUP_PHASE);
    let stream_setup_span = start_write_span(
        trace_context,
        timeline,
        "Set up query stream",
        "delta_funnel.write.query",
        "Query stream setup",
        stage_owner_id,
    )
    .map(|span| {
        span.with_attribute(
            "output_name",
            planned.resolved_target().output_name().to_owned().into(),
        )
    });
    let profile_scope = match profile_mode {
        ExecutionProfileMode::Disabled => None,
        ExecutionProfileMode::Detailed => Some(QueryExecutionScope::MssqlOutput),
    };
    let QueryStreamSetup {
        stream,
        profile_result,
    } = match batch_stream_for_physical_plan(
        context,
        physical_plan,
        provider_stats_snapshots,
        progress.map(|reporter| (reporter, planned.resolved_target().output_name().to_owned())),
        profile_scope,
        trace_identity,
    ) {
        Ok(execution) => execution,
        Err(failure) => {
            fail_write_span(stream_setup_span);
            return Err(mssql_output_query_error(
                planned,
                MssqlWritePhase::QueryStreamSetup,
                query_phase_timings,
                stream_setup_timer,
                *failure.source,
                failure.execution_profile,
            ));
        }
    };
    complete_write_span(stream_setup_span);
    query_phase_timings.push(stream_setup_timer.completed());

    let attach_profile_to_result = profile_result.map(|profile_result| {
        Box::new(move |result: Result<MssqlWriteReport, DeltaFunnelError>| {
            let execution_profile = profile_result.profile().cloned();
            match result {
                Ok(report) => Ok(report.with_execution_profile(execution_profile)),
                Err(error) => Err(with_mssql_write_execution_profile(error, execution_profile)),
            }
        }) as MssqlOutputProfileCallback
    });

    Ok(MssqlOutputQueryExecution {
        stream,
        query_phase_timings,
        attach_profile_to_result,
    })
}

#[cfg(test)]
fn mssql_query_phase_error(
    planned: &PlannedMssqlOutput,
    phase: MssqlWritePhase,
    query_phase_timings: Vec<PhaseTimingReport>,
    timer: PhaseTimer,
    source: DeltaFunnelError,
    execution_profile: Option<QueryExecutionProfile>,
) -> DeltaFunnelError {
    mssql_output_query_error(
        planned,
        phase,
        query_phase_timings,
        timer,
        source,
        execution_profile,
    )
    .error
}

pub(super) fn mssql_output_query_error(
    planned: &PlannedMssqlOutput,
    phase: MssqlWritePhase,
    mut query_phase_timings: Vec<PhaseTimingReport>,
    timer: PhaseTimer,
    source: DeltaFunnelError,
    execution_profile: Option<QueryExecutionProfile>,
) -> MssqlOutputQueryError {
    query_phase_timings.push(timer.failed());
    let next_phase_index = query_phase_timings.len();
    debug_assert!(next_phase_index <= QUERY_PHASE_NAMES.len());
    query_phase_timings.extend(
        QUERY_PHASE_NAMES
            .get(next_phase_index..)
            .unwrap_or_default()
            .iter()
            .map(|name| PhaseTimingReport::not_started(*name, ReportReasonCode::PriorFailure)),
    );
    let mut phase_timings = planned.phase_timings().to_vec();
    phase_timings.extend(query_phase_timings.iter().cloned());
    let cleanup = match planned.output_plan().load_mode() {
        LoadMode::AppendExisting => MssqlTargetCleanupStatus::NotApplicable,
        LoadMode::CreateAndLoad | LoadMode::Replace => MssqlTargetCleanupStatus::NotAttempted,
    };

    let context = MssqlWriteFailureContext::from_output_plan(
        planned.output_plan(),
        phase,
        0,
        0,
        0,
        false,
        cleanup,
    )
    .with_phase_timings(phase_timings)
    .with_execution_profile(execution_profile);
    MssqlOutputQueryError {
        error: DeltaFunnelError::MssqlQueryPhase {
            context: Box::new(context),
            source: Box::new(source),
        },
        query_phase_timings,
    }
}

fn with_mssql_write_execution_profile(
    error: DeltaFunnelError,
    execution_profile: Option<QueryExecutionProfile>,
) -> DeltaFunnelError {
    match error {
        DeltaFunnelError::MssqlWritePhase { context, message } => {
            DeltaFunnelError::MssqlWritePhase {
                context: Box::new((*context).with_execution_profile(execution_profile)),
                message,
            }
        }
        DeltaFunnelError::MssqlQueryPhase { context, source } => {
            DeltaFunnelError::MssqlQueryPhase {
                context: Box::new((*context).with_execution_profile(execution_profile)),
                source,
            }
        }
        DeltaFunnelError::MssqlBatchSchemaValidation { context, source } => {
            DeltaFunnelError::MssqlBatchSchemaValidation {
                context: Box::new((*context).with_execution_profile(execution_profile)),
                source,
            }
        }
        other => other,
    }
}

fn finish_mssql_write_timeline(
    result: Result<MssqlWriteReport, DeltaFunnelError>,
    timeline: Option<&OperationTimelineRecorder>,
    output_name: &str,
) -> Result<MssqlWriteReport, DeltaFunnelError> {
    let Some(timeline) = timeline else {
        return result;
    };
    if let Some(profile) = mssql_write_execution_profile(&result) {
        timeline.append_operator_lifecycles(profile);
    }
    let status = if result.is_ok() {
        TimelineSpanStatus::Completed
    } else {
        TimelineSpanStatus::Failed
    };
    let operation_timeline = timeline.finish(format!("SQL Server write: {output_name}"), status);
    match result {
        Ok(report) => Ok(report.with_operation_timeline(Some(operation_timeline))),
        Err(error) => Err(with_mssql_write_operation_timeline(
            error,
            operation_timeline,
        )),
    }
}

fn finish_mssql_write_error_timeline(
    error: DeltaFunnelError,
    timeline: Option<&OperationTimelineRecorder>,
    output_name: &str,
) -> DeltaFunnelError {
    let Some(timeline) = timeline else {
        return error;
    };
    if let Some(profile) = mssql_write_error_execution_profile(&error) {
        timeline.append_operator_lifecycles(profile);
    }
    let operation_timeline = timeline.finish(
        format!("SQL Server write: {output_name}"),
        TimelineSpanStatus::Failed,
    );
    with_mssql_write_operation_timeline(error, operation_timeline)
}

fn mssql_write_execution_profile(
    result: &Result<MssqlWriteReport, DeltaFunnelError>,
) -> Option<&QueryExecutionProfile> {
    match result {
        Ok(report) => report.execution_profile(),
        Err(error) => mssql_write_error_execution_profile(error),
    }
}

fn mssql_write_error_execution_profile(error: &DeltaFunnelError) -> Option<&QueryExecutionProfile> {
    match error {
        DeltaFunnelError::MssqlWritePhase { context, .. }
        | DeltaFunnelError::MssqlQueryPhase { context, .. }
        | DeltaFunnelError::MssqlBatchSchemaValidation { context, .. } => {
            context.report().execution_profile()
        }
        _ => None,
    }
}

fn with_mssql_write_operation_timeline(
    error: DeltaFunnelError,
    operation_timeline: crate::OperationTimeline,
) -> DeltaFunnelError {
    match error {
        DeltaFunnelError::MssqlWritePhase { context, message } => {
            DeltaFunnelError::MssqlWritePhase {
                context: Box::new((*context).with_operation_timeline(Some(operation_timeline))),
                message,
            }
        }
        DeltaFunnelError::MssqlQueryPhase { context, source } => {
            DeltaFunnelError::MssqlQueryPhase {
                context: Box::new((*context).with_operation_timeline(Some(operation_timeline))),
                source,
            }
        }
        DeltaFunnelError::MssqlBatchSchemaValidation { context, source } => {
            DeltaFunnelError::MssqlBatchSchemaValidation {
                context: Box::new((*context).with_operation_timeline(Some(operation_timeline))),
                source,
            }
        }
        other => other,
    }
}

fn start_write_span(
    trace_context: Option<&OperationTraceContext>,
    timeline: Option<&OperationTimelineRecorder>,
    name: &'static str,
    category: &'static str,
    track_name: &str,
    stage_owner_id: Option<u64>,
) -> Option<OperationStageTrace> {
    OperationStageTrace::start(
        trace_context,
        timeline,
        name,
        category,
        track_name,
        stage_owner_id,
    )
}

fn complete_write_span(span: Option<OperationStageTrace>) {
    if let Some(span) = span {
        span.completed();
    }
}

fn fail_write_span(span: Option<OperationStageTrace>) {
    if let Some(span) = span {
        span.failed();
    }
}

fn prepend_mssql_write_phase_timings(
    error: DeltaFunnelError,
    phase_timings: Vec<PhaseTimingReport>,
) -> DeltaFunnelError {
    match error {
        DeltaFunnelError::MssqlWritePhase { context, message } => {
            DeltaFunnelError::MssqlWritePhase {
                context: Box::new((*context).with_phase_timings(phase_timings)),
                message,
            }
        }
        DeltaFunnelError::MssqlBatchSchemaValidation { context, source } => {
            DeltaFunnelError::MssqlBatchSchemaValidation {
                context: Box::new((*context).with_phase_timings(phase_timings)),
                source,
            }
        }
        other => other,
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
    /// Writes one planned output.
    ///
    /// After stream setup, implementations must return failures through a
    /// phase-aware SQL write error variant so timings and profiles can be
    /// attached to its [`MssqlWriteFailureContext`].
    async fn write_output(
        &mut self,
        output_plan: MssqlTargetOutputPlan,
        resolved_target: ResolvedMssqlTarget,
        batches: MssqlOutputBatchStream,
        write_backend: MssqlWriteBackend,
        validation_options: ValidationOptions,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError>;

    #[allow(
        clippy::too_many_arguments,
        reason = "the writer boundary receives one planned output plus optional profiling state"
    )]
    async fn write_output_with_stage_context(
        &mut self,
        output_plan: MssqlTargetOutputPlan,
        resolved_target: ResolvedMssqlTarget,
        batches: MssqlOutputBatchStream,
        write_backend: MssqlWriteBackend,
        validation_options: ValidationOptions,
        reporter: Option<&ProgressReporter>,
        _stage_context: OperationStageContext<'_>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        self.write_output(
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

struct MssqlOneOutputSinkWriter;

#[async_trait]
impl OrchestratorMssqlOutputWriter for MssqlOneOutputSinkWriter {
    async fn write_output(
        &mut self,
        output_plan: MssqlTargetOutputPlan,
        resolved_target: ResolvedMssqlTarget,
        batches: MssqlOutputBatchStream,
        write_backend: MssqlWriteBackend,
        validation_options: ValidationOptions,
        reporter: Option<&ProgressReporter>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        write_planned_output_batches_to_mssql_for_workflow(
            output_plan,
            resolved_target,
            batches,
            write_backend,
            validation_options,
            reporter,
        )
        .await
    }

    async fn write_output_with_stage_context(
        &mut self,
        output_plan: MssqlTargetOutputPlan,
        resolved_target: ResolvedMssqlTarget,
        batches: MssqlOutputBatchStream,
        write_backend: MssqlWriteBackend,
        validation_options: ValidationOptions,
        reporter: Option<&ProgressReporter>,
        stage_context: OperationStageContext<'_>,
    ) -> Result<MssqlWriteReport, DeltaFunnelError> {
        crate::sql_server::write_planned_output_batches_to_mssql_for_workflow_with_stage_context(
            output_plan,
            resolved_target,
            batches,
            write_backend,
            validation_options,
            reporter,
            stage_context,
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
    use std::{
        error::Error,
        sync::{Arc, Mutex, atomic::Ordering},
        time::Duration,
    };

    use async_trait::async_trait;
    use futures_util::StreamExt;

    use crate::{
        DeltaFunnelError, DeltaSourceConfig, ExecutionProfileMode, LoadMode, MssqlConnectionSource,
        MssqlOutputBatchStream, MssqlOutputTarget, MssqlTargetCleanupStatus, MssqlTargetConfig,
        MssqlTargetOutputPlan, MssqlTargetTable, MssqlWriteBackend, MssqlWriteFailureContext,
        MssqlWritePhase, MssqlWriteReport, PhaseStatus, PhaseTimingReport, QueryExecutionOutcome,
        QueryExecutionScope, ReportReasonCode, ResolvedMssqlTarget, TargetValidationMode,
        ValidationOptions,
        observability::test_capture::{CapturedEvent, TracingCapture},
        progress::{ProgressEventKind, ProgressOperation, ProgressPhase, ProgressReporter},
        table_formats::RealParquetDeltaTable,
    };

    use super::super::super::{
        DeltaFunnelSession, LazyTable, LazyTableKind, OutputWritePlan, RunMode, SessionOptions,
        test_support::{
            DeltaLogTable, UNSUPPORTED_SCHEMA_FIELDS_JSON, execute_output_request,
            failing_scan_marker_region_provider, output_request, override_connection,
            plan_lifetime_tracking_marker_region_provider, secret_connection,
            stream_setup_failing_marker_region_provider,
        },
    };
    use super::{
        OUTPUT_SCHEMA_PLANNING_PHASE, OrchestratorMssqlOutputWriter,
        QUERY_DATAFRAME_PLANNING_PHASE, QUERY_PHASE_NAMES, QUERY_PHYSICAL_PLANNING_PHASE,
        QUERY_STREAM_SETUP_PHASE, SINGLE_OUTPUT_STAGE_OWNER_ID, SQL_TARGET_PLANNING_PHASE,
        VALIDATION_PHASE, mssql_query_phase_error, prepend_mssql_write_phase_timings,
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

    fn cleanup_before_stream_write(
        output_plan: &MssqlTargetOutputPlan,
    ) -> MssqlTargetCleanupStatus {
        match output_plan.load_mode() {
            LoadMode::AppendExisting => MssqlTargetCleanupStatus::NotApplicable,
            LoadMode::CreateAndLoad | LoadMode::Replace => MssqlTargetCleanupStatus::NotAttempted,
        }
    }

    fn injected_write_error(
        output_plan: &MssqlTargetOutputPlan,
        phase: MssqlWritePhase,
        rows: u64,
        batches: u64,
        partial_write_possible: bool,
        cleanup: MssqlTargetCleanupStatus,
        message: impl Into<String>,
    ) -> DeltaFunnelError {
        DeltaFunnelError::MssqlWritePhase {
            context: Box::new(MssqlWriteFailureContext::from_output_plan(
                output_plan,
                phase,
                rows,
                batches,
                0,
                partial_write_possible,
                cleanup,
            )),
            message: message.into(),
        }
    }

    async fn drain_batches(
        output_plan: &MssqlTargetOutputPlan,
        batches: &mut MssqlOutputBatchStream,
    ) -> Result<(u64, u64), DeltaFunnelError> {
        let mut rows = 0_u64;
        let mut batch_count = 0_u64;
        while let Some(batch) = batches.next().await {
            let batch = match batch {
                Ok(batch) => batch,
                Err(error) => {
                    return Err(injected_write_error(
                        output_plan,
                        MssqlWritePhase::PollBatchStream,
                        rows,
                        batch_count,
                        rows != 0,
                        cleanup_before_stream_write(output_plan),
                        error.to_string(),
                    ));
                }
            };
            rows = rows.saturating_add(crate::usize_to_u64_saturating(batch.num_rows()));
            batch_count = batch_count.saturating_add(1);
        }
        Ok((rows, batch_count))
    }

    #[derive(Default)]
    struct FakeOrchestratorWriter {
        calls: Vec<FakeOrchestratorWriteCall>,
    }

    #[async_trait]
    impl OrchestratorMssqlOutputWriter for FakeOrchestratorWriter {
        async fn write_output(
            &mut self,
            output_plan: MssqlTargetOutputPlan,
            resolved_target: ResolvedMssqlTarget,
            mut batches: MssqlOutputBatchStream,
            _write_backend: MssqlWriteBackend,
            validation_options: ValidationOptions,
            _reporter: Option<&ProgressReporter>,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            let (rows, batch_count) = drain_batches(&output_plan, &mut batches).await?;

            self.calls.push(FakeOrchestratorWriteCall {
                output_name: resolved_target.output_name().to_owned(),
                target_table: resolved_target.table().clone(),
                connection_source: resolved_target.connection_source(),
                rows,
                batches: batch_count,
                schema_fields: output_plan.schema_mappings().len(),
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

    struct EarlyFailureWriter(MssqlWritePhase);

    #[async_trait]
    impl OrchestratorMssqlOutputWriter for EarlyFailureWriter {
        async fn write_output(
            &mut self,
            output_plan: MssqlTargetOutputPlan,
            _resolved_target: ResolvedMssqlTarget,
            batches: MssqlOutputBatchStream,
            _write_backend: MssqlWriteBackend,
            _validation_options: ValidationOptions,
            _reporter: Option<&ProgressReporter>,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            drop(batches);
            Err(injected_write_error(
                &output_plan,
                self.0,
                0,
                0,
                false,
                cleanup_before_stream_write(&output_plan),
                "injected pre-stream writer failure",
            ))
        }
    }

    struct PreEofFailureWriter(MssqlWritePhase);

    #[async_trait]
    impl OrchestratorMssqlOutputWriter for PreEofFailureWriter {
        async fn write_output(
            &mut self,
            output_plan: MssqlTargetOutputPlan,
            _resolved_target: ResolvedMssqlTarget,
            mut batches: MssqlOutputBatchStream,
            _write_backend: MssqlWriteBackend,
            _validation_options: ValidationOptions,
            _reporter: Option<&ProgressReporter>,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            let (rows, batch_count) = match batches.next().await {
                Some(Ok(batch)) => (crate::usize_to_u64_saturating(batch.num_rows()), 1),
                Some(Err(error)) => {
                    return Err(injected_write_error(
                        &output_plan,
                        MssqlWritePhase::PollBatchStream,
                        0,
                        0,
                        false,
                        cleanup_before_stream_write(&output_plan),
                        error.to_string(),
                    ));
                }
                None => (0, 0),
            };
            drop(batches);
            Err(injected_write_error(
                &output_plan,
                self.0,
                rows,
                batch_count,
                self.0 == MssqlWritePhase::WriteBatch,
                cleanup_before_stream_write(&output_plan),
                "injected pre-EOF writer failure",
            ))
        }
    }

    struct PostEofFailureWriter {
        phase: MssqlWritePhase,
        message: &'static str,
    }

    impl PostEofFailureWriter {
        const fn new(phase: MssqlWritePhase, message: &'static str) -> Self {
            Self { phase, message }
        }
    }

    #[async_trait]
    impl OrchestratorMssqlOutputWriter for PostEofFailureWriter {
        async fn write_output(
            &mut self,
            output_plan: MssqlTargetOutputPlan,
            _resolved_target: ResolvedMssqlTarget,
            mut batches: MssqlOutputBatchStream,
            _write_backend: MssqlWriteBackend,
            _validation_options: ValidationOptions,
            _reporter: Option<&ProgressReporter>,
        ) -> Result<MssqlWriteReport, DeltaFunnelError> {
            let (rows, batch_count) = drain_batches(&output_plan, &mut batches).await?;
            let cleanup = if self.phase == MssqlWritePhase::Cleanup {
                MssqlTargetCleanupStatus::Failed
            } else {
                cleanup_before_stream_write(&output_plan)
            };
            Err(injected_write_error(
                &output_plan,
                self.phase,
                rows,
                batch_count,
                false,
                cleanup,
                self.message,
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
        let capture = TracingCapture::start_with_profile_spans_enabled();

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
        let spans = capture.captured().spans();
        let root = spans
            .iter()
            .find(|span| span.name == "Delta Funnel SQL Server write")
            .ok_or("expected failed SQL Server write root")?;
        assert_eq!(root.fields["result"], "error");
        let stages = spans
            .iter()
            .filter(|span| span.name == "Delta Funnel operation stage")
            .collect::<Vec<_>>();
        assert_eq!(stages.len(), 2);
        assert_eq!(stages[0].fields["stage_name"], "Plan output schema");
        assert_eq!(stages[0].fields["result"], "ok");
        assert_eq!(stages[1].fields["stage_name"], "Plan SQL Server target");
        assert_eq!(stages[1].fields["result"], "error");
        assert!(stages.iter().all(|stage| {
            stage.parent_id == Some(root.id)
                && stage.fields["stage_owner_id"] == SINGLE_OUTPUT_STAGE_OWNER_ID.to_string()
        }));
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
        let capture = TracingCapture::start_with_profile_spans_enabled();

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
        assert_eq!(report.execution_profile(), None);
        assert!(execution_profile_events(&capture).is_empty());
        let spans = capture
            .captured()
            .spans()
            .into_iter()
            .filter(|span| span.target == crate::profiling::PROFILE_TARGET)
            .collect::<Vec<_>>();
        let root = spans
            .iter()
            .find(|span| span.name == "Delta Funnel SQL Server write")
            .ok_or("expected SQL Server write process root")?;
        assert_eq!(root.fields["result"], "ok");
        assert!(root.closed);
        let phases = spans
            .iter()
            .filter(|span| span.name == "Delta Funnel operation phase")
            .collect::<Vec<_>>();
        assert_eq!(
            phases
                .iter()
                .map(|span| (
                    span.fields["phase"].as_str(),
                    span.fields["result"].as_str()
                ))
                .collect::<Vec<_>>(),
            [
                ("planning", "ok"),
                ("execution", "ok"),
                ("finalization", "ok"),
            ]
        );
        assert!(phases.iter().all(|phase| {
            phase.parent_id == Some(root.id)
                && phase.fields["operation_id"] == root.fields["operation_id"]
                && phase.closed
        }));
        let stages = spans
            .iter()
            .filter(|span| span.name == "Delta Funnel operation stage")
            .collect::<Vec<_>>();
        assert_eq!(
            stages
                .iter()
                .map(|span| span.fields["stage_name"].as_str())
                .collect::<Vec<_>>(),
            [
                "Plan output schema",
                "Plan SQL Server target",
                "Build query DataFrame",
                "Build physical plan",
                "Set up query stream",
            ]
        );
        assert!(stages.iter().all(|stage| {
            stage.parent_id == Some(root.id)
                && stage.fields["operation_id"] == root.fields["operation_id"]
                && stage.fields["operation_kind"] == "mssql_write"
                && stage.fields["stage_owner_id"] == SINGLE_OUTPUT_STAGE_OWNER_ID.to_string()
                && stage.fields["result"] == "ok"
                && stage.fields["time_semantics"] == "wall_clock"
                && stage.closed
        }));
        assert_eq!(
            report
                .phase_timings()
                .iter()
                .take(6)
                .map(crate::PhaseTimingReport::phase_name)
                .collect::<Vec<_>>(),
            vec![
                "lazy_sql_planning",
                OUTPUT_SCHEMA_PLANNING_PHASE,
                SQL_TARGET_PLANNING_PHASE,
                QUERY_DATAFRAME_PLANNING_PHASE,
                QUERY_PHYSICAL_PLANNING_PHASE,
                QUERY_STREAM_SETUP_PHASE,
            ]
        );
        assert!(
            report.phase_timings()[3..6]
                .iter()
                .all(|timing| timing.status().is_completed() && timing.elapsed_micros().is_some())
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
    async fn detailed_write_attaches_one_successful_terminal_profile()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
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
        let capture = TracingCapture::start();

        let report = session
            .write_to_mssql_with_writer_and_profile_mode(
                &request,
                &mut writer,
                ExecutionProfileMode::Detailed,
            )
            .await?;

        let profile = report.execution_profile().ok_or("expected profile")?;
        assert_eq!(profile.scope(), QueryExecutionScope::MssqlOutput);
        assert_eq!(profile.outcome(), QueryExecutionOutcome::Success);
        assert!(!profile.partial());
        assert_eq!(profile.delta_funnel_row_limit(), None);
        assert!(!profile.operators().is_empty());
        let timeline = report.operation_timeline().ok_or("expected timeline")?;
        crate::report::trace_contract::validate_operation_trace(timeline)?;
        assert_eq!(timeline.status(), crate::TimelineSpanStatus::Completed);
        assert!(timeline.total_duration_micros() > 0);
        assert!(timeline.spans().iter().any(|span| {
            span.name() == "Build query DataFrame" && span.category() == "delta_funnel.write.query"
        }));
        assert!(
            timeline
                .spans()
                .iter()
                .any(|span| span.category() == "datafusion.operator.lifecycle")
        );
        let activity_spans = timeline
            .spans()
            .iter()
            .filter(|span| span.category() == "datafusion.operator.activity")
            .collect::<Vec<_>>();
        assert!(!activity_spans.is_empty());
        assert!(activity_spans.iter().all(|span| {
            span.attributes()["query_execution_id"].is_u64()
                && span.attributes()["worker_lane_id"].is_u64()
                && span.attributes()["worker_kind"].is_string()
                && span.track_name().starts_with("DataFusion query ")
        }));
        assert!(timeline.spans().iter().all(|span| {
            span.start_offset_micros()
                .saturating_add(span.duration_micros())
                <= timeline.total_duration_micros()
        }));
        let value = report.to_json_value();
        assert_eq!(value["execution_profile"]["scope"], "mssql_output");
        assert_eq!(value["execution_profile"]["outcome"], "success");
        assert_eq!(value["operation_timeline"]["status"], "completed");
        let trace = report.to_trace_event_json_value().ok_or("expected trace")?;
        assert_eq!(
            trace["delta_funnel_timeline"]["total_duration_micros"],
            timeline.total_duration_micros()
        );
        assert_eq!(trace["delta_funnel_profile"]["scope"], "mssql_output");
        assert_eq!(
            value
                .as_object()
                .map(|object| object.contains_key("execution_profile")),
            Some(true)
        );
        let events = execution_profile_events(&capture);
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].fields.get("scope").map(String::as_str),
            Some("mssql_output")
        );
        assert_eq!(
            events[0].fields.get("outcome").map(String::as_str),
            Some("success")
        );
        Ok(())
    }

    #[tokio::test]
    async fn detailed_write_with_progress_keeps_the_same_profile_shape()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
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
        let (reporter, progress) = recording_reporter();
        let capture = TracingCapture::start();

        let report = session
            .run_mssql_write_with_reporter(
                &request,
                &mut writer,
                ExecutionProfileMode::Detailed,
                &reporter,
            )
            .await?;

        let profile = report.execution_profile().ok_or("expected profile")?;
        assert_eq!(profile.scope(), QueryExecutionScope::MssqlOutput);
        assert_eq!(profile.outcome(), QueryExecutionOutcome::Success);
        assert!(!profile.partial());
        assert_eq!(profile.delta_funnel_row_limit(), None);
        assert!(!profile.operators().is_empty());
        assert_eq!(execution_profile_events(&capture).len(), 1);
        assert_eq!(
            progress_events(&progress),
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
    async fn detailed_write_attaches_cancelled_profile_when_writer_never_polls_stream()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let derived = session.table_from_sql("select 1 as id").await?;
        let request = execute_output_request(
            derived,
            "orders_output",
            "orders_sink",
            LoadMode::CreateAndLoad,
        )?;
        for phase in [
            MssqlWritePhase::Connect,
            MssqlWritePhase::PrepareTargetLifecycle,
            MssqlWritePhase::InitializeWriter,
        ] {
            let capture = TracingCapture::start();
            let error = session
                .write_to_mssql_with_writer_and_profile_mode(
                    &request,
                    &mut EarlyFailureWriter(phase),
                    ExecutionProfileMode::Detailed,
                )
                .await
                .err()
                .ok_or("expected writer failure")?;

            let DeltaFunnelError::MssqlWritePhase { context, .. } = error else {
                return Err("expected MSSQL write phase failure".into());
            };
            assert_eq!(context.phase(), phase);
            assert!(!context.partial_write_possible());
            let profile = context
                .report()
                .execution_profile()
                .ok_or("expected failure profile")?;
            assert_eq!(profile.scope(), QueryExecutionScope::MssqlOutput);
            assert_eq!(profile.outcome(), QueryExecutionOutcome::Cancelled);
            assert!(profile.partial());
            let timeline = context
                .operation_timeline()
                .ok_or("expected failure timeline")?;
            crate::report::trace_contract::validate_operation_trace(timeline)?;
            assert_eq!(timeline.status(), crate::TimelineSpanStatus::Failed);
            let value = context.to_json_value();
            assert!(value.get("execution_profile").is_none());
            assert_eq!(value["report"]["execution_profile"]["outcome"], "cancelled");
            assert_eq!(value["report"]["operation_timeline"]["status"], "failed");
            let events = execution_profile_events(&capture);
            assert_eq!(events.len(), 1);
            assert_eq!(
                events[0].fields.get("outcome").map(String::as_str),
                Some("cancelled")
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn detailed_write_keeps_successful_profile_for_failure_after_eof()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
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
        for phase in [
            MssqlWritePhase::Finalize,
            MssqlWritePhase::Validation,
            MssqlWritePhase::SwapTarget,
            MssqlWritePhase::Cleanup,
        ] {
            let capture = TracingCapture::start();
            let error = session
                .write_to_mssql_with_writer_and_profile_mode(
                    &request,
                    &mut PostEofFailureWriter::new(phase, "injected post-EOF writer failure"),
                    ExecutionProfileMode::Detailed,
                )
                .await
                .err()
                .ok_or("expected post-EOF writer failure")?;

            let DeltaFunnelError::MssqlWritePhase { context, .. } = error else {
                return Err("expected MSSQL write phase failure".into());
            };
            assert_eq!(context.phase(), phase);
            let profile = context
                .report()
                .execution_profile()
                .ok_or("expected failure profile")?;
            assert_eq!(profile.scope(), QueryExecutionScope::MssqlOutput);
            assert_eq!(profile.outcome(), QueryExecutionOutcome::Success);
            assert!(!profile.partial());
            let events = execution_profile_events(&capture);
            assert_eq!(events.len(), 1);
            assert_eq!(
                events[0].fields.get("outcome").map(String::as_str),
                Some("success")
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn detailed_write_reports_real_dataframe_planning_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = execute_output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let removed = session.context().deregister_table("orders")?;
        assert!(removed.is_some());
        let mut writer = FakeOrchestratorWriter::default();
        let capture = TracingCapture::start();

        let error = session
            .write_to_mssql_with_writer_and_profile_mode(
                &request,
                &mut writer,
                ExecutionProfileMode::Detailed,
            )
            .await
            .err()
            .ok_or("expected DataFrame planning failure")?;

        let DeltaFunnelError::MssqlQueryPhase { context, .. } = error else {
            return Err("expected MSSQL query phase failure".into());
        };
        assert_eq!(context.phase(), MssqlWritePhase::QueryDataFramePlanning);
        assert_eq!(context.report().execution_profile(), None);
        assert!(execution_profile_events(&capture).is_empty());
        assert!(writer.calls.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn detailed_write_reports_real_physical_planning_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (provider, scans) = failing_scan_marker_region_provider();
        session
            .context()
            .register_table("broken_source", provider)?;
        let derived = session
            .table_from_sql("select marker from broken_source")
            .await?;
        let request = execute_output_request(
            derived,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let mut writer = FakeOrchestratorWriter::default();
        let capture = TracingCapture::start();

        let error = session
            .write_to_mssql_with_writer_and_profile_mode(
                &request,
                &mut writer,
                ExecutionProfileMode::Detailed,
            )
            .await
            .err()
            .ok_or("expected physical planning failure")?;

        let DeltaFunnelError::MssqlQueryPhase { context, .. } = error else {
            return Err("expected MSSQL query phase failure".into());
        };
        assert_eq!(context.phase(), MssqlWritePhase::QueryPhysicalPlanning);
        assert_eq!(context.report().execution_profile(), None);
        assert_eq!(scans.load(Ordering::SeqCst), 1);
        assert!(execution_profile_events(&capture).is_empty());
        assert!(writer.calls.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn detailed_write_reports_real_stream_setup_failure_profile()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        session.context().register_table(
            "setup_failure_source",
            stream_setup_failing_marker_region_provider()?,
        )?;
        let derived = session
            .table_from_sql("select marker from setup_failure_source")
            .await?;
        let request = execute_output_request(
            derived,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let mut writer = FakeOrchestratorWriter::default();
        let capture = TracingCapture::start();

        let error = session
            .write_to_mssql_with_writer_and_profile_mode(
                &request,
                &mut writer,
                ExecutionProfileMode::Detailed,
            )
            .await
            .err()
            .ok_or("expected stream setup failure")?;

        let DeltaFunnelError::MssqlQueryPhase { context, .. } = error else {
            return Err("expected MSSQL query phase failure".into());
        };
        assert_eq!(context.phase(), MssqlWritePhase::QueryStreamSetup);
        let profile = context
            .report()
            .execution_profile()
            .ok_or("expected stream setup failure profile")?;
        assert_eq!(profile.outcome(), QueryExecutionOutcome::Error);
        assert!(profile.partial());
        assert_eq!(execution_profile_events(&capture).len(), 1);
        assert!(writer.calls.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn detailed_write_reports_upstream_execution_error_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let derived = session
            .table_from_sql("select cast(1 as bigint) / cast(0 as bigint) as value")
            .await?;
        let request = execute_output_request(
            derived,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let mut writer = FakeOrchestratorWriter::default();
        let capture = TracingCapture::start();

        let error = session
            .write_to_mssql_with_writer_and_profile_mode(
                &request,
                &mut writer,
                ExecutionProfileMode::Detailed,
            )
            .await
            .err()
            .ok_or("expected upstream execution failure")?;

        let DeltaFunnelError::MssqlWritePhase { context, .. } = error else {
            return Err("expected MSSQL write phase failure".into());
        };
        assert_eq!(context.phase(), MssqlWritePhase::PollBatchStream);
        let profile = context
            .report()
            .execution_profile()
            .ok_or("expected execution failure profile")?;
        assert_eq!(profile.outcome(), QueryExecutionOutcome::Error);
        assert!(profile.partial());
        assert_eq!(execution_profile_events(&capture).len(), 1);
        tokio::task::yield_now().await;
        assert_eq!(execution_profile_events(&capture).len(), 1);
        assert!(writer.calls.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn detailed_write_reports_pre_eof_validation_and_write_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let derived = session
            .table_from_sql("select 1 as id union all select 2 as id")
            .await?;
        let request = execute_output_request(
            derived,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;

        for phase in [
            MssqlWritePhase::ValidateBatchSchema,
            MssqlWritePhase::WriteBatch,
        ] {
            let capture = TracingCapture::start();
            let error = session
                .write_to_mssql_with_writer_and_profile_mode(
                    &request,
                    &mut PreEofFailureWriter(phase),
                    ExecutionProfileMode::Detailed,
                )
                .await
                .err()
                .ok_or("expected pre-EOF writer failure")?;

            let DeltaFunnelError::MssqlWritePhase { context, .. } = error else {
                return Err("expected MSSQL write phase failure".into());
            };
            assert_eq!(context.phase(), phase);
            assert_eq!(
                context.partial_write_possible(),
                phase == MssqlWritePhase::WriteBatch
            );
            let profile = context
                .report()
                .execution_profile()
                .ok_or("expected cancellation profile")?;
            assert_eq!(profile.outcome(), QueryExecutionOutcome::Cancelled);
            assert!(profile.partial());
            assert_eq!(execution_profile_events(&capture).len(), 1);
        }
        Ok(())
    }

    #[tokio::test]
    async fn detailed_write_releases_execution_plan_after_report_capture()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let (provider, last_plan_marker) =
            plan_lifetime_tracking_marker_region_provider("observed")?;
        session
            .context()
            .register_table("profile_source", provider)?;
        let derived = session
            .table_from_sql("select marker from profile_source")
            .await?;
        let request = execute_output_request(
            derived,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let mut writer = FakeOrchestratorWriter::default();

        let report = session
            .write_to_mssql_with_writer_and_profile_mode(
                &request,
                &mut writer,
                ExecutionProfileMode::Detailed,
            )
            .await?;

        assert!(report.execution_profile().is_some());
        let marker = match last_plan_marker.lock() {
            Ok(marker) => marker,
            Err(poisoned) => poisoned.into_inner(),
        };
        assert!(
            marker
                .as_ref()
                .is_some_and(|marker| marker.upgrade().is_none())
        );
        Ok(())
    }

    #[test]
    fn query_phase_failures_keep_source_and_mark_remaining_phases_not_started()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let planned = session.plan_mssql_output(&request)?;
        for (failed_index, phase) in [
            MssqlWritePhase::QueryDataFramePlanning,
            MssqlWritePhase::QueryPhysicalPlanning,
            MssqlWritePhase::QueryStreamSetup,
        ]
        .into_iter()
        .enumerate()
        {
            let completed_timings = QUERY_PHASE_NAMES[..failed_index]
                .iter()
                .map(|name| PhaseTimingReport::completed(*name, Duration::ZERO))
                .collect();
            let error = mssql_query_phase_error(
                &planned,
                phase,
                completed_timings,
                crate::report::PhaseTimer::start(QUERY_PHASE_NAMES[failed_index]),
                DeltaFunnelError::Config {
                    message: "query preparation failed".to_owned(),
                },
                None,
            );

            assert!(Error::source(&error).is_some());
            let DeltaFunnelError::MssqlQueryPhase { context, source } = error else {
                return Err("expected MSSQL query phase error".into());
            };
            assert_eq!(context.phase(), phase);
            assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
            assert!(!context.partial_write_possible());
            assert!(matches!(
                source.as_ref(),
                DeltaFunnelError::Config { message } if message == "query preparation failed"
            ));
            assert_eq!(
                context
                    .phase_timings()
                    .iter()
                    .map(PhaseTimingReport::phase_name)
                    .collect::<Vec<_>>(),
                vec![
                    OUTPUT_SCHEMA_PLANNING_PHASE,
                    SQL_TARGET_PLANNING_PHASE,
                    QUERY_DATAFRAME_PLANNING_PHASE,
                    QUERY_PHYSICAL_PLANNING_PHASE,
                    QUERY_STREAM_SETUP_PHASE,
                ]
            );
            for (index, timing) in context.phase_timings()[2..].iter().enumerate() {
                let expected = if index < failed_index {
                    PhaseStatus::completed()
                } else if index == failed_index {
                    PhaseStatus::failed()
                } else {
                    PhaseStatus::not_started(ReportReasonCode::PriorFailure)
                };
                assert_eq!(timing.status(), expected);
            }
            assert!(
                context.phase_timings()[..2]
                    .iter()
                    .all(|timing| timing.status().is_completed())
            );
        }
        Ok(())
    }

    #[test]
    fn query_phase_failure_reports_cleanup_status_for_each_load_mode()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;

        for (load_mode, expected_cleanup) in [
            (
                LoadMode::AppendExisting,
                MssqlTargetCleanupStatus::NotApplicable,
            ),
            (
                LoadMode::CreateAndLoad,
                MssqlTargetCleanupStatus::NotAttempted,
            ),
            (LoadMode::Replace, MssqlTargetCleanupStatus::NotAttempted),
        ] {
            let request =
                output_request(source.clone(), "orders_output", "orders_sink", load_mode)?;
            let planned = session.plan_mssql_output(&request)?;
            let error = mssql_query_phase_error(
                &planned,
                MssqlWritePhase::QueryDataFramePlanning,
                Vec::new(),
                crate::report::PhaseTimer::start(QUERY_DATAFRAME_PLANNING_PHASE),
                DeltaFunnelError::Config {
                    message: "query preparation failed".to_owned(),
                },
                None,
            );

            let DeltaFunnelError::MssqlQueryPhase { context, .. } = error else {
                return Err("expected MSSQL query phase error".into());
            };
            assert_eq!(context.cleanup(), expected_cleanup);
            assert!(!context.partial_write_possible());
        }
        Ok(())
    }

    #[test]
    fn write_phase_failure_keeps_query_and_output_planning_timings()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("orders")?;
        let mut session = DeltaFunnelSession::new(
            SessionOptions::new().with_default_mssql_connection(secret_connection()?),
        )?;
        let source = session.delta_lake(DeltaSourceConfig::new("orders", table.uri()))?;
        let request = output_request(
            source,
            "orders_output",
            "orders_sink",
            LoadMode::AppendExisting,
        )?;
        let planned = session.plan_mssql_output(&request)?;
        let context = crate::MssqlWriteFailureContext::from_output_plan(
            planned.output_plan(),
            MssqlWritePhase::WriteBatch,
            1,
            1,
            0,
            true,
            MssqlTargetCleanupStatus::NotApplicable,
        )
        .with_phase_timings(vec![PhaseTimingReport::failed(
            "write_batch",
            Duration::ZERO,
        )]);
        let error = DeltaFunnelError::MssqlWritePhase {
            context: Box::new(context),
            message: "write failed".to_owned(),
        };
        let mut planning_timings = planned.phase_timings().to_vec();
        planning_timings.extend(
            QUERY_PHASE_NAMES
                .iter()
                .map(|name| PhaseTimingReport::completed(*name, Duration::ZERO)),
        );

        let error = prepend_mssql_write_phase_timings(error, planning_timings);

        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err("expected MSSQL write phase error".into());
        };
        assert_eq!(message, "write failed");
        assert_eq!(context.phase(), MssqlWritePhase::WriteBatch);
        assert!(context.partial_write_possible());
        assert_eq!(
            context
                .phase_timings()
                .iter()
                .map(PhaseTimingReport::phase_name)
                .collect::<Vec<_>>(),
            vec![
                OUTPUT_SCHEMA_PLANNING_PHASE,
                SQL_TARGET_PLANNING_PHASE,
                QUERY_DATAFRAME_PLANNING_PHASE,
                QUERY_PHYSICAL_PLANNING_PHASE,
                QUERY_STREAM_SETUP_PHASE,
                "write_batch",
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
        let mut writer = PostEofFailureWriter::new(MssqlWritePhase::Finalize, "raw-error-secret");
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
            Err(DeltaFunnelError::MssqlWritePhase { message, .. })
                if message == "raw-error-secret"
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
    async fn detailed_write_executes_real_delta_source_fixture_and_profiles_planning()
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
            .write_to_mssql_with_writer_and_profile_mode(
                &request,
                &mut writer,
                ExecutionProfileMode::Detailed,
            )
            .await?;

        assert_eq!(writer.calls.len(), 1);
        let call = writer.calls.first().ok_or("expected fake writer call")?;
        assert_eq!(call.output_name, "orders_output");
        assert_eq!(call.rows, u64::try_from(table.rows())?);
        assert!(call.batches >= 1);
        assert_eq!(call.schema_fields, 2);
        assert_eq!(report.stats().rows_written(), u64::try_from(table.rows())?);
        assert_eq!(report.stats().batches_written(), call.batches);
        let timeline = report.operation_timeline().ok_or("expected timeline")?;
        let planning_spans = timeline
            .spans()
            .iter()
            .filter(|span| span.category() == "datafusion.planning.activity")
            .collect::<Vec<_>>();
        assert!(
            planning_spans
                .iter()
                .any(|span| span.name() == "Delta scan planning")
        );
        assert!(planning_spans.iter().all(|span| {
            span.track_name() == "DataFusion query planning / SQL output: orders_output"
                && span.attributes()["query_scope"] == "mssql_output"
                && span.attributes()["query_owner"] == "orders_output"
        }));
        Ok(())
    }

    #[tokio::test]
    async fn post_stream_writer_failure_keeps_successful_provider_scan_outcome()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = RealParquetDeltaTable::new_default("post-stream-writer-failure")?;
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
        // This writer drains the batch stream to EOF before returning its
        // injected failure, matching a later SQL finalization failure.
        let mut writer =
            PostEofFailureWriter::new(MssqlWritePhase::Finalize, "post-stream failure");
        let capture = TracingCapture::start();

        let result = session
            .write_to_mssql_with_writer(&request, &mut writer)
            .await;
        let summaries = capture
            .captured()
            .events()
            .into_iter()
            .filter(|event| {
                event.fields.get("telemetry_event").map(String::as_str)
                    == Some("delta_provider_parquet_io_summary")
            })
            .collect::<Vec<_>>();

        assert!(matches!(
            result,
            Err(DeltaFunnelError::MssqlWritePhase { message, .. })
                if message == "post-stream failure"
        ));
        assert_eq!(summaries.len(), 1);
        assert_eq!(
            summaries[0].fields.get("source_name").map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            summaries[0].fields.get("outcome").map(String::as_str),
            Some("success")
        );
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
