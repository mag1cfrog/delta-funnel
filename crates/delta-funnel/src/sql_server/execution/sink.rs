//! One-output SQL Server sink orchestration.
//!
//! This module wires the already planned SQL Server target, connected client,
//! target lifecycle preparation, writer initialization, and batch write loop.

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use futures_util::Stream;

use crate::{
    DeltaFunnelError, PhaseTimingReport, ReportReasonCode, RowCount, TargetValidationMode,
    ValidationOptions, ValidationStatus,
    error::MssqlWritePhaseSnafu,
    observability,
    report::{
        PhaseTimer,
        sql_server::{MssqlBatchShapingReport, MssqlWriteReportMetrics},
    },
};

use super::{
    LoadMode, MssqlBulkLoadWriter, MssqlPreparedTarget, MssqlSchemaPlanOptions,
    MssqlTargetCleanupStatus, MssqlTargetOutputPlan, MssqlWriteFailureContext, MssqlWriteOptions,
    MssqlWritePhase, MssqlWriteReport, ResolvedMssqlTarget,
};
use super::{
    connection::{
        MssqlConnectedOutputClient, MssqlOutputConnectionRequest, MssqlTargetRowCountFailure,
        connect_mssql_output_client, plan_mssql_output_connection_request,
    },
    lifecycle::{
        cleanup_mssql_prepared_target, prepare_mssql_target_lifecycle, swap_mssql_replace_target,
    },
    write::write_mssql_batches_with_writer,
};

const PREPARE_TARGET_LIFECYCLE_PHASE: &str = "prepare_target_lifecycle";
const INITIALIZE_WRITER_PHASE: &str = "initialize_writer";
const CLEANUP_PHASE: &str = "cleanup";
const VALIDATION_PHASE: &str = "validation";
const SWAP_TARGET_PHASE: &str = "swap_target";

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
/// destructive replace behavior. Replace writes through a private staging table and then swaps it
/// into the final target name.
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

pub(crate) async fn write_output_batches_to_mssql_with_validation_options<S>(
    output_schema: impl AsRef<arrow_schema::Schema>,
    resolved_target: ResolvedMssqlTarget,
    schema_options: MssqlSchemaPlanOptions,
    batches: S,
    write_options: MssqlWriteOptions,
    validation_options: ValidationOptions,
) -> Result<MssqlWriteReport, DeltaFunnelError>
where
    S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send,
{
    let request =
        plan_mssql_output_connection_request(output_schema, resolved_target, schema_options)?;

    write_mssql_output_connection_request_with_validation_options(
        request,
        batches,
        write_options,
        validation_options,
    )
    .await
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

    /// Counts target rows after a successful write and finalize.
    async fn target_row_count(
        &mut self,
        output_plan: &MssqlTargetOutputPlan,
        prepared_target: &MssqlPreparedTarget,
    ) -> Result<u64, MssqlTargetRowCountFailure>;

    /// Swaps a prepared replace staging table into the final target name.
    async fn swap_prepared_replace_target(
        &mut self,
        output_plan: &MssqlTargetOutputPlan,
        prepared_target: &MssqlPreparedTarget,
    ) -> Result<(), DeltaFunnelError>;

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
        Self: 'static,
        S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send,
    {
        let initialize_timer = PhaseTimer::start(INITIALIZE_WRITER_PHASE);
        let writer = match self
            .initialize_writer(output_plan, prepared_target, options)
            .await
        {
            Ok(writer) => writer,
            Err(error) => {
                return Err(error_with_phase_timings(
                    error,
                    vec![initialize_timer.failed()],
                ));
            }
        };
        let initialize_timing = initialize_timer.completed();

        match write_mssql_batches_with_writer(output_plan, batches, writer, options).await {
            Ok(report) => Ok(report.with_phase_timings(vec![initialize_timing])),
            Err(error) => Err(error_with_phase_timings(error, vec![initialize_timing])),
        }
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

    async fn target_row_count(
        &mut self,
        _output_plan: &MssqlTargetOutputPlan,
        prepared_target: &MssqlPreparedTarget,
    ) -> Result<u64, MssqlTargetRowCountFailure> {
        self.target_row_count(prepared_target).await
    }

    async fn swap_prepared_replace_target(
        &mut self,
        output_plan: &MssqlTargetOutputPlan,
        prepared_target: &MssqlPreparedTarget,
    ) -> Result<(), DeltaFunnelError> {
        let mut lifecycle_client = self.lifecycle_client();

        swap_mssql_replace_target(output_plan, prepared_target, &mut lifecycle_client).await
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
    write_mssql_output_connection_request_with_validation_options(
        request,
        batches,
        options,
        ValidationOptions::default(),
    )
    .await
}

pub(crate) async fn write_mssql_output_connection_request_with_validation_options<S>(
    request: MssqlOutputConnectionRequest,
    batches: S,
    options: MssqlWriteOptions,
    validation_options: ValidationOptions,
) -> Result<MssqlWriteReport, DeltaFunnelError>
where
    S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send,
{
    let output_plan = request.output_plan().clone();
    let connection = connect_mssql_output_client(request).await?;
    let phase_timings = connection.phase_timings().to_vec();

    write_mssql_output_batches_on_connection_with_phase_timings(
        output_plan,
        connection,
        batches,
        options,
        validation_options,
        phase_timings,
    )
    .await
}

/// Writes one planned SQL Server output through an already connected boundary.
#[allow(dead_code)]
pub(crate) async fn write_mssql_output_batches_on_connection<C, S>(
    output_plan: MssqlTargetOutputPlan,
    connection: C,
    batches: S,
    options: MssqlWriteOptions,
) -> Result<MssqlWriteReport, DeltaFunnelError>
where
    C: MssqlOneOutputSinkConnection + 'static,
    S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send,
{
    write_mssql_output_batches_on_connection_with_phase_timings(
        output_plan,
        connection,
        batches,
        options,
        ValidationOptions::default(),
        Vec::new(),
    )
    .await
}

async fn write_mssql_output_batches_on_connection_with_phase_timings<C, S>(
    output_plan: MssqlTargetOutputPlan,
    mut connection: C,
    batches: S,
    options: MssqlWriteOptions,
    validation_options: ValidationOptions,
    mut phase_timings: Vec<PhaseTimingReport>,
) -> Result<MssqlWriteReport, DeltaFunnelError>
where
    C: MssqlOneOutputSinkConnection + 'static,
    S: Stream<Item = Result<RecordBatch, DeltaFunnelError>> + Send,
{
    let prepare_timer = PhaseTimer::start(PREPARE_TARGET_LIFECYCLE_PHASE);
    let prepared_target = match connection.prepare_target_lifecycle(&output_plan).await {
        Ok(prepared_target) => prepared_target,
        Err(error) => {
            phase_timings.push(prepare_timer.failed());
            return Err(error_with_phase_timings(error, phase_timings));
        }
    };
    phase_timings.push(prepare_timer.completed());

    let target_row_count_before_write = match target_row_count_before_append_write(
        &mut connection,
        &output_plan,
        &prepared_target,
        validation_options,
    )
    .await
    {
        Ok(target_row_count_before_write) => target_row_count_before_write,
        Err(error) => {
            return Err(cleanup_after_prepared_target_failure(
                &mut connection,
                &output_plan,
                &prepared_target,
                error_with_phase_timings(error, phase_timings),
            )
            .await);
        }
    };

    match connection
        .write_prepared_batches(&output_plan, &prepared_target, batches, options)
        .await
    {
        Ok(report) => {
            let report = write_report_with_cleanup(&report, prepared_target.report().cleanup())
                .with_phase_timings(phase_timings);
            match validate_written_target(
                &mut connection,
                &output_plan,
                &prepared_target,
                report,
                validation_options,
                target_row_count_before_write,
            )
            .await
            {
                Ok(report) => match swap_replace_target_after_validation(
                    &mut connection,
                    &output_plan,
                    &prepared_target,
                    report,
                )
                .await
                {
                    Ok(report) => Ok(report),
                    Err(error) => Err(cleanup_after_prepared_target_failure(
                        &mut connection,
                        &output_plan,
                        &prepared_target,
                        error,
                    )
                    .await),
                },
                Err(error) => Err(cleanup_after_prepared_target_failure(
                    &mut connection,
                    &output_plan,
                    &prepared_target,
                    error,
                )
                .await),
            }
        }
        Err(error) => Err(cleanup_after_prepared_target_failure(
            &mut connection,
            &output_plan,
            &prepared_target,
            error_with_phase_timings(error, phase_timings),
        )
        .await),
    }
}

async fn validate_written_target<C>(
    connection: &mut C,
    output_plan: &MssqlTargetOutputPlan,
    prepared_target: &MssqlPreparedTarget,
    report: MssqlWriteReport,
    validation_options: ValidationOptions,
    target_row_count_before_write: TargetRowCountBeforeWrite,
) -> Result<MssqlWriteReport, DeltaFunnelError>
where
    C: MssqlOneOutputSinkConnection,
{
    match validation_options.target_validation_mode() {
        TargetValidationMode::Disabled => Ok(finish_validation_report(
            output_plan,
            report.with_target_validation(
                RowCount::unavailable(),
                ValidationStatus::disabled(),
                PhaseTimingReport::skipped(VALIDATION_PHASE, ReportReasonCode::ValidationDisabled),
            ),
        )),
        TargetValidationMode::ValidateIfPossible | TargetValidationMode::Require => {
            let validation_required =
                validation_options.target_validation_mode() == TargetValidationMode::Require;
            let Some(output_rows) = report.output_row_count().exact_value() else {
                return missing_exact_output_rows_validation(
                    output_plan,
                    report,
                    validation_required,
                    target_row_count_before_write,
                );
            };

            if output_plan.load_mode() == LoadMode::AppendExisting {
                return validate_append_existing_target_delta(
                    connection,
                    output_plan,
                    prepared_target,
                    report,
                    validation_required,
                    target_row_count_before_write,
                    output_rows,
                )
                .await;
            }

            if !matches!(
                output_plan.load_mode(),
                LoadMode::CreateAndLoad | LoadMode::Replace
            ) {
                return unsupported_target_validation(output_plan, report, validation_required);
            }

            observability::validation_started(output_plan.target_table(), output_plan.load_mode());
            let validation_timer = PhaseTimer::start(VALIDATION_PHASE);
            let target_rows = match connection
                .target_row_count(output_plan, prepared_target)
                .await
            {
                Ok(target_rows) => target_rows,
                Err(failure) => {
                    return validation_unavailable_or_required_failure(
                        output_plan,
                        report,
                        failure.reason(),
                        validation_required,
                        failure.message(),
                    );
                }
            };
            let target_row_count = RowCount::exact(target_rows);

            if target_rows == output_rows {
                return Ok(finish_validation_report(
                    output_plan,
                    report.with_target_validation(
                        target_row_count,
                        ValidationStatus::passed(),
                        validation_timer.completed(),
                    ),
                ));
            }

            let report = report.with_target_validation(
                target_row_count,
                ValidationStatus::failed(),
                validation_timer.failed(),
            );
            Err(validation_error(
                output_plan,
                &report,
                "target row count did not match exact output rows",
            ))
        }
    }
}

async fn swap_replace_target_after_validation<C>(
    connection: &mut C,
    output_plan: &MssqlTargetOutputPlan,
    prepared_target: &MssqlPreparedTarget,
    report: MssqlWriteReport,
) -> Result<MssqlWriteReport, DeltaFunnelError>
where
    C: MssqlOneOutputSinkConnection,
{
    if output_plan.load_mode() != LoadMode::Replace {
        return Ok(report);
    }

    let swap_timer = PhaseTimer::start(SWAP_TARGET_PHASE);
    match connection
        .swap_prepared_replace_target(output_plan, prepared_target)
        .await
    {
        Ok(()) => Ok(report.with_appended_phase_timings(vec![swap_timer.completed()])),
        Err(error) => Err(error_with_appended_phase_timings(
            error_with_report_metrics(output_plan, error, MssqlWritePhase::SwapTarget, &report),
            vec![swap_timer.failed()],
        )),
    }
}

fn missing_exact_output_rows_validation(
    output_plan: &MssqlTargetOutputPlan,
    report: MssqlWriteReport,
    validation_required: bool,
    target_row_count_before_write: TargetRowCountBeforeWrite,
) -> Result<MssqlWriteReport, DeltaFunnelError> {
    if output_plan.load_mode() == LoadMode::AppendExisting
        && let TargetRowCountBeforeWrite::Exact(target_rows_before) = target_row_count_before_write
    {
        return target_delta_validation_unavailable_or_required_failure(
            output_plan,
            report,
            ReportReasonCode::MissingExactOutputRows,
            validation_required,
            "target row-count validation requires exact output rows",
            RowCount::exact(target_rows_before),
        );
    }

    validation_unavailable_or_required_failure(
        output_plan,
        report,
        ReportReasonCode::MissingExactOutputRows,
        validation_required,
        "target row-count validation requires exact output rows",
    )
}

async fn target_row_count_before_append_write<C>(
    connection: &mut C,
    output_plan: &MssqlTargetOutputPlan,
    prepared_target: &MssqlPreparedTarget,
    validation_options: ValidationOptions,
) -> Result<TargetRowCountBeforeWrite, DeltaFunnelError>
where
    C: MssqlOneOutputSinkConnection,
{
    if validation_options.target_validation_mode() == TargetValidationMode::Disabled
        || output_plan.load_mode() != LoadMode::AppendExisting
    {
        return Ok(TargetRowCountBeforeWrite::NotRequired);
    }

    match connection
        .target_row_count(output_plan, prepared_target)
        .await
    {
        Ok(target_rows) => Ok(TargetRowCountBeforeWrite::Exact(target_rows)),
        Err(failure)
            if validation_options.target_validation_mode() == TargetValidationMode::Require =>
        {
            Err(pre_write_required_validation_error(
                output_plan,
                prepared_target,
                failure.reason(),
                failure.message(),
            ))
        }
        Err(failure) => Ok(TargetRowCountBeforeWrite::Unavailable {
            reason: failure.reason(),
            message: failure.message().to_owned(),
        }),
    }
}

async fn validate_append_existing_target_delta<C>(
    connection: &mut C,
    output_plan: &MssqlTargetOutputPlan,
    prepared_target: &MssqlPreparedTarget,
    report: MssqlWriteReport,
    validation_required: bool,
    target_row_count_before_write: TargetRowCountBeforeWrite,
    output_rows: u64,
) -> Result<MssqlWriteReport, DeltaFunnelError>
where
    C: MssqlOneOutputSinkConnection,
{
    let target_rows_before = match target_row_count_before_write {
        TargetRowCountBeforeWrite::Exact(target_rows_before) => target_rows_before,
        TargetRowCountBeforeWrite::Unavailable { reason, message } => {
            return validation_unavailable_or_required_failure(
                output_plan,
                report,
                reason,
                validation_required,
                &message,
            );
        }
        TargetRowCountBeforeWrite::NotRequired => {
            return validation_unavailable_or_required_failure(
                output_plan,
                report,
                ReportReasonCode::MissingTargetAccess,
                validation_required,
                "target row-count validation requires target rows before append write",
            );
        }
    };

    observability::validation_started(output_plan.target_table(), output_plan.load_mode());
    let validation_timer = PhaseTimer::start(VALIDATION_PHASE);
    let target_rows_after = match connection
        .target_row_count(output_plan, prepared_target)
        .await
    {
        Ok(target_rows_after) => target_rows_after,
        Err(failure) => {
            return target_delta_validation_unavailable_or_required_failure(
                output_plan,
                report,
                failure.reason(),
                validation_required,
                failure.message(),
                RowCount::exact(target_rows_before),
            );
        }
    };
    let target_delta = target_rows_after.checked_sub(target_rows_before);
    let target_row_count_before_write = RowCount::exact(target_rows_before);
    let target_row_count_after_write = RowCount::exact(target_rows_after);

    if target_delta == Some(output_rows) {
        return Ok(finish_validation_report(
            output_plan,
            report.with_target_delta_validation(
                target_row_count_before_write,
                target_row_count_after_write,
                ValidationStatus::passed(),
                validation_timer.completed(),
            ),
        ));
    }

    let report = report.with_target_delta_validation(
        target_row_count_before_write,
        target_row_count_after_write,
        ValidationStatus::failed(),
        validation_timer.failed(),
    );
    Err(validation_error(
        output_plan,
        &report,
        "target row-count delta did not match exact output rows",
    ))
}

fn unsupported_target_validation(
    output_plan: &MssqlTargetOutputPlan,
    report: MssqlWriteReport,
    validation_required: bool,
) -> Result<MssqlWriteReport, DeltaFunnelError> {
    if validation_required {
        let report = report.with_target_validation(
            RowCount::unavailable(),
            ValidationStatus::required_but_failed(ReportReasonCode::UnsupportedLoadMode),
            PhaseTimingReport::skipped(VALIDATION_PHASE, ReportReasonCode::UnsupportedLoadMode),
        );
        Err(validation_error(
            output_plan,
            &report,
            "target row-count validation is not implemented for this load mode",
        ))
    } else {
        Ok(finish_validation_report(
            output_plan,
            report.with_target_validation(
                RowCount::unavailable(),
                ValidationStatus::unavailable(ReportReasonCode::UnsupportedLoadMode),
                PhaseTimingReport::skipped(VALIDATION_PHASE, ReportReasonCode::UnsupportedLoadMode),
            ),
        ))
    }
}

fn validation_unavailable_or_required_failure(
    output_plan: &MssqlTargetOutputPlan,
    report: MssqlWriteReport,
    reason: ReportReasonCode,
    validation_required: bool,
    message: &str,
) -> Result<MssqlWriteReport, DeltaFunnelError> {
    let validation_status = if validation_required {
        ValidationStatus::required_but_failed(reason)
    } else {
        ValidationStatus::unavailable(reason)
    };
    let report = report.with_target_validation(
        RowCount::unavailable(),
        validation_status,
        PhaseTimingReport::unavailable(VALIDATION_PHASE, reason),
    );

    if validation_required {
        return Err(validation_error(output_plan, &report, message));
    }

    Ok(finish_validation_report(output_plan, report))
}

fn target_delta_validation_unavailable_or_required_failure(
    output_plan: &MssqlTargetOutputPlan,
    report: MssqlWriteReport,
    reason: ReportReasonCode,
    validation_required: bool,
    message: &str,
    target_row_count_before_write: RowCount,
) -> Result<MssqlWriteReport, DeltaFunnelError> {
    let validation_status = if validation_required {
        ValidationStatus::required_but_failed(reason)
    } else {
        ValidationStatus::unavailable(reason)
    };
    let report = report.with_target_delta_validation(
        target_row_count_before_write,
        RowCount::unavailable(),
        validation_status,
        PhaseTimingReport::unavailable(VALIDATION_PHASE, reason),
    );

    if validation_required {
        return Err(validation_error(output_plan, &report, message));
    }

    Ok(finish_validation_report(output_plan, report))
}

fn pre_write_required_validation_error(
    output_plan: &MssqlTargetOutputPlan,
    prepared_target: &MssqlPreparedTarget,
    reason: ReportReasonCode,
    message: &str,
) -> DeltaFunnelError {
    let report = MssqlWriteReport::from_output_plan_with_metrics(
        output_plan,
        MssqlWriteReportMetrics::new(
            RowCount::unavailable(),
            MssqlBatchShapingReport::not_started(ReportReasonCode::NotExecuted),
            0,
            0,
            0,
            false,
            prepared_target.report().cleanup(),
        )
        .with_target_delta_validation(
            RowCount::unavailable(),
            RowCount::unavailable(),
            ValidationStatus::required_but_failed(reason),
        )
        .with_phase_timings(vec![PhaseTimingReport::unavailable(
            VALIDATION_PHASE,
            reason,
        )]),
    );
    validation_error(output_plan, &report, message)
}

fn validation_error(
    output_plan: &MssqlTargetOutputPlan,
    report: &MssqlWriteReport,
    message: &str,
) -> DeltaFunnelError {
    observability::validation_finished(
        output_plan.target_table(),
        output_plan.load_mode(),
        report.validation_status(),
    );

    MssqlWritePhaseSnafu {
        context: Box::new(MssqlWriteFailureContext::from_output_plan_with_metrics(
            output_plan,
            MssqlWritePhase::Validation,
            report_metrics_from_report(report),
        )),
        message: message.to_owned(),
    }
    .build()
}

fn finish_validation_report(
    output_plan: &MssqlTargetOutputPlan,
    report: MssqlWriteReport,
) -> MssqlWriteReport {
    observability::validation_finished(
        output_plan.target_table(),
        output_plan.load_mode(),
        report.validation_status(),
    );
    report
}

fn report_metrics_from_report(report: &MssqlWriteReport) -> MssqlWriteReportMetrics {
    report_metrics_from_report_with_partial_write_possible(report, report.partial_write_possible())
}

fn report_metrics_from_report_with_partial_write_possible(
    report: &MssqlWriteReport,
    partial_write_possible: bool,
) -> MssqlWriteReportMetrics {
    MssqlWriteReportMetrics::new(
        report.output_row_count(),
        report.batch_shaping(),
        report.stats().rows_written(),
        report.stats().batches_written(),
        report.stats().elapsed_ms(),
        partial_write_possible,
        report.cleanup(),
    )
    .with_target_delta_validation(
        report.target_row_count_before_write(),
        report.target_row_count_after_write(),
        report.validation_status(),
    )
    .with_phase_timings(report.phase_timings().to_vec())
}

fn error_with_report_metrics(
    output_plan: &MssqlTargetOutputPlan,
    error: DeltaFunnelError,
    phase: MssqlWritePhase,
    report: &MssqlWriteReport,
) -> DeltaFunnelError {
    let (message, partial_write_possible) = match error {
        DeltaFunnelError::MssqlWritePhase { context, message } => {
            (message, context.partial_write_possible())
        }
        other => (other.to_string(), true),
    };

    DeltaFunnelError::MssqlWritePhase {
        context: Box::new(MssqlWriteFailureContext::from_output_plan_with_metrics(
            output_plan,
            phase,
            report_metrics_from_report_with_partial_write_possible(report, partial_write_possible),
        )),
        message,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TargetRowCountBeforeWrite {
    NotRequired,
    Exact(u64),
    Unavailable {
        reason: ReportReasonCode,
        message: String,
    },
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
    let cleanup_timer = PhaseTimer::start(CLEANUP_PHASE);
    match connection
        .cleanup_prepared_target(output_plan, Some(prepared_target))
        .await
    {
        Ok(cleanup) => error_with_appended_phase_timings(
            error_with_cleanup(output_plan, original_error, cleanup),
            vec![cleanup_timer.completed()],
        ),
        Err(cleanup_error) => error_with_appended_phase_timings(
            error_with_cleanup_failure(output_plan, original_error, cleanup_error),
            vec![cleanup_timer.failed()],
        ),
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

fn error_with_phase_timings(
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

fn error_with_appended_phase_timings(
    error: DeltaFunnelError,
    phase_timings: Vec<PhaseTimingReport>,
) -> DeltaFunnelError {
    match error {
        DeltaFunnelError::MssqlWritePhase { context, message } => {
            DeltaFunnelError::MssqlWritePhase {
                context: Box::new((*context).with_appended_phase_timings(phase_timings)),
                message,
            }
        }
        DeltaFunnelError::MssqlBatchSchemaValidation { context, source } => {
            DeltaFunnelError::MssqlBatchSchemaValidation {
                context: Box::new((*context).with_appended_phase_timings(phase_timings)),
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
        .with_target_delta_validation(
            context.target_row_count_before_write(),
            context.target_row_count_after_write(),
            context.validation_status(),
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
        MssqlConnectionConfig, MssqlPreparedTargetAction, MssqlTargetConfig, MssqlTargetTable,
        MssqlWritePhase, PhaseStatus, PhaseTimingReport, default_mssql_write_options,
        plan_mssql_target_for_output,
    };

    #[derive(Default)]
    struct FakeSinkConnection {
        log: Arc<Mutex<Vec<String>>>,
        prepare_error: Option<DeltaFunnelError>,
        initialize_error: Option<DeltaFunnelError>,
        cleanup_error: Option<DeltaFunnelError>,
        swap_error: Option<DeltaFunnelError>,
        target_row_count_results: Vec<Result<u64, MssqlTargetRowCountFailure>>,
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

        fn fail_swap(mut self, error: DeltaFunnelError) -> Self {
            self.swap_error = Some(error);
            self
        }

        fn with_target_row_count(mut self, target_row_count: u64) -> Self {
            self.target_row_count_results = vec![Ok(target_row_count)];
            self
        }

        fn with_target_row_counts(mut self, target_row_counts: Vec<u64>) -> Self {
            self.target_row_count_results = target_row_counts.into_iter().map(Ok).collect();
            self
        }

        fn fail_target_row_count(mut self, failure: MssqlTargetRowCountFailure) -> Self {
            self.target_row_count_results = vec![Err(failure)];
            self
        }

        fn with_target_row_count_results(
            mut self,
            results: Vec<Result<u64, MssqlTargetRowCountFailure>>,
        ) -> Self {
            self.target_row_count_results = results;
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
                    LoadMode::Replace => MssqlPreparedTargetAction::CreatedStagingTable,
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
                MssqlPreparedTargetAction::CreatedTable
                | MssqlPreparedTargetAction::CreatedStagingTable => {
                    MssqlTargetCleanupStatus::Succeeded
                }
            })
        }

        async fn target_row_count(
            &mut self,
            _output_plan: &MssqlTargetOutputPlan,
            _prepared_target: &MssqlPreparedTarget,
        ) -> Result<u64, MssqlTargetRowCountFailure> {
            self.record("count target rows").map_err(|error| {
                MssqlTargetRowCountFailure::new(
                    ReportReasonCode::CapabilityUnavailable,
                    error.to_string(),
                )
            })?;

            if self.target_row_count_results.is_empty() {
                return Ok(0);
            }

            self.target_row_count_results.remove(0)
        }

        async fn swap_prepared_replace_target(
            &mut self,
            _output_plan: &MssqlTargetOutputPlan,
            _prepared_target: &MssqlPreparedTarget,
        ) -> Result<(), DeltaFunnelError> {
            self.record("swap")?;
            if let Some(error) = self.swap_error.take() {
                return Err(error);
            }

            Ok(())
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

    fn assert_phase_timing(
        timings: &[PhaseTimingReport],
        phase_name: &str,
        expected_status: PhaseStatus,
    ) -> Result<(), DeltaFunnelError> {
        let timing = timings
            .iter()
            .find(|timing| timing.phase_name() == phase_name)
            .ok_or_else(|| DeltaFunnelError::Config {
                message: format!("missing phase timing {phase_name}"),
            })?;

        assert_eq!(timing.status(), expected_status);
        if expected_status.is_completed() || expected_status.is_failed() {
            assert!(timing.elapsed_micros().is_some());
        } else {
            assert_eq!(timing.elapsed_micros(), None);
        }
        Ok(())
    }

    fn phase_error(
        output_plan: &MssqlTargetOutputPlan,
        phase: MssqlWritePhase,
        message: &str,
    ) -> DeltaFunnelError {
        phase_error_with_partial_write(output_plan, phase, message, false)
    }

    fn phase_error_with_partial_write(
        output_plan: &MssqlTargetOutputPlan,
        phase: MssqlWritePhase,
        message: &str,
        partial_write_possible: bool,
    ) -> DeltaFunnelError {
        DeltaFunnelError::MssqlWritePhase {
            context: Box::new(crate::MssqlWriteFailureContext::from_output_plan(
                output_plan,
                phase,
                0,
                0,
                0,
                partial_write_possible,
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
        let validation_options =
            ValidationOptions::new().with_target_validation_mode(TargetValidationMode::Disabled);

        let report = write_mssql_output_batches_on_connection_with_phase_timings(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
            validation_options,
            Vec::new(),
        )
        .await?;

        assert_eq!(
            logged_events(&log)?,
            vec!["prepare", "initialize", "write 2", "write 1", "finish"]
        );
        assert_eq!(report.output_name(), "orders_output");
        assert_eq!(report.stats().rows_written(), 3);
        assert_eq!(report.stats().batches_written(), 2);
        assert_phase_timing(
            report.phase_timings(),
            PREPARE_TARGET_LIFECYCLE_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_phase_timing(
            report.phase_timings(),
            INITIALIZE_WRITER_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_phase_timing(
            report.phase_timings(),
            "write_batch",
            PhaseStatus::completed(),
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn validation_disabled_skips_target_validation_after_success()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::CreateAndLoad)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log));
        let batches = stream::iter(vec![Ok(orders_batch(3)?)]);
        let validation_options =
            ValidationOptions::new().with_target_validation_mode(TargetValidationMode::Disabled);

        let report = write_mssql_output_batches_on_connection_with_phase_timings(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
            validation_options,
            Vec::new(),
        )
        .await?;

        assert_eq!(report.target_row_count(), RowCount::unavailable());
        assert_eq!(report.validation_status(), ValidationStatus::disabled());
        assert_phase_timing(
            report.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::skipped(ReportReasonCode::ValidationDisabled),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec!["prepare", "initialize", "write 3", "finish"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn create_and_load_matching_target_rows_passes_validation() -> Result<(), DeltaFunnelError>
    {
        let output_plan = output_plan_with_load_mode(LoadMode::CreateAndLoad)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log)).with_target_row_count(3);
        let batches = stream::iter(vec![Ok(orders_batch(2)?), Ok(orders_batch(1)?)]);

        let report = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await?;

        assert_eq!(report.output_row_count(), RowCount::exact(3));
        assert_eq!(report.target_row_count(), RowCount::exact(3));
        assert_eq!(report.validation_status(), ValidationStatus::passed());
        assert_phase_timing(
            report.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "initialize",
                "write 2",
                "write 1",
                "finish",
                "count target rows"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn replace_matching_staging_rows_swaps_after_validation() -> Result<(), DeltaFunnelError>
    {
        let output_plan = output_plan_with_load_mode(LoadMode::Replace)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log)).with_target_row_count(3);
        let batches = stream::iter(vec![Ok(orders_batch(2)?), Ok(orders_batch(1)?)]);

        let report = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await?;

        assert_eq!(report.load_mode(), LoadMode::Replace);
        assert_eq!(report.output_row_count(), RowCount::exact(3));
        assert_eq!(report.target_row_count(), RowCount::exact(3));
        assert_eq!(report.validation_status(), ValidationStatus::passed());
        assert_eq!(report.cleanup(), MssqlTargetCleanupStatus::NotAttempted);
        assert_phase_timing(
            report.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_phase_timing(
            report.phase_timings(),
            SWAP_TARGET_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "initialize",
                "write 2",
                "write 1",
                "finish",
                "count target rows",
                "swap"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn replace_validation_failure_cleans_up_staging_without_swap()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::Replace)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log)).with_target_row_count(4);
        let batches = stream::iter(vec![Ok(orders_batch(3)?)]);

        let error = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected replace validation mismatch failure".to_owned(),
        })?;

        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected validation write phase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::Validation);
        assert_eq!(context.output_row_count(), RowCount::exact(3));
        assert_eq!(context.target_row_count(), RowCount::exact(4));
        assert_eq!(context.validation_status(), ValidationStatus::failed());
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::Succeeded);
        assert!(message.contains("target row count did not match"));
        assert_phase_timing(
            context.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::failed(),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            CLEANUP_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "initialize",
                "write 3",
                "finish",
                "count target rows",
                "cleanup CreatedStagingTable"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn replace_swap_failure_reports_swap_phase_and_cleans_up_staging()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::Replace)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log))
            .with_target_row_count(3)
            .fail_swap(phase_error_with_partial_write(
                &output_plan,
                MssqlWritePhase::SwapTarget,
                "swap failed",
                true,
            ));
        let batches = stream::iter(vec![Ok(orders_batch(3)?)]);

        let error = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected replace swap failure".to_owned(),
        })?;

        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected swap write phase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::SwapTarget);
        assert_eq!(context.output_row_count(), RowCount::exact(3));
        assert_eq!(context.target_row_count(), RowCount::exact(3));
        assert_eq!(context.validation_status(), ValidationStatus::passed());
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::Succeeded);
        assert!(context.partial_write_possible());
        assert!(message.contains("swap failed"));
        assert_phase_timing(
            context.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            SWAP_TARGET_PHASE,
            PhaseStatus::failed(),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            CLEANUP_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "initialize",
                "write 3",
                "finish",
                "count target rows",
                "swap",
                "cleanup CreatedStagingTable"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn create_and_load_mismatched_target_rows_fails_validation_and_cleans_up()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::CreateAndLoad)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log)).with_target_row_count(4);
        let batches = stream::iter(vec![Ok(orders_batch(3)?)]);

        let error = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected validation mismatch failure".to_owned(),
        })?;

        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected validation write phase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::Validation);
        assert_eq!(context.output_row_count(), RowCount::exact(3));
        assert_eq!(context.target_row_count(), RowCount::exact(4));
        assert_eq!(context.validation_status(), ValidationStatus::failed());
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::Succeeded);
        assert!(message.contains("target row count did not match"));
        assert_phase_timing(
            context.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::failed(),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            CLEANUP_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "initialize",
                "write 3",
                "finish",
                "count target rows",
                "cleanup CreatedTable"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn validate_if_possible_records_unavailable_target_row_count()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::CreateAndLoad)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log)).fail_target_row_count(
            MssqlTargetRowCountFailure::new(
                ReportReasonCode::PermissionUnavailable,
                "permission denied",
            ),
        );
        let batches = stream::iter(vec![Ok(orders_batch(3)?)]);

        let report = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await?;

        assert_eq!(report.target_row_count(), RowCount::unavailable());
        assert_eq!(
            report.validation_status(),
            ValidationStatus::unavailable(ReportReasonCode::PermissionUnavailable)
        );
        assert_phase_timing(
            report.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::unavailable(ReportReasonCode::PermissionUnavailable),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "initialize",
                "write 3",
                "finish",
                "count target rows"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn validate_if_possible_treats_unsupported_target_count_as_unavailable()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::CreateAndLoad)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log)).fail_target_row_count(
            MssqlTargetRowCountFailure::new(
                ReportReasonCode::CapabilityUnavailable,
                "COUNT_BIG(*) conversion overflow",
            ),
        );
        let batches = stream::iter(vec![Ok(orders_batch(3)?)]);

        let report = write_mssql_output_batches_on_connection(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
        )
        .await?;

        assert_eq!(report.target_row_count(), RowCount::unavailable());
        assert_eq!(
            report.validation_status(),
            ValidationStatus::unavailable(ReportReasonCode::CapabilityUnavailable)
        );
        assert_phase_timing(
            report.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::unavailable(ReportReasonCode::CapabilityUnavailable),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "initialize",
                "write 3",
                "finish",
                "count target rows"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn require_validation_turns_unavailable_target_row_count_into_failure()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::CreateAndLoad)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log)).fail_target_row_count(
            MssqlTargetRowCountFailure::new(
                ReportReasonCode::PermissionUnavailable,
                "permission denied",
            ),
        );
        let batches = stream::iter(vec![Ok(orders_batch(3)?)]);
        let validation_options =
            ValidationOptions::new().with_target_validation_mode(TargetValidationMode::Require);

        let error = write_mssql_output_batches_on_connection_with_phase_timings(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
            validation_options,
            Vec::new(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected required validation failure".to_owned(),
        })?;

        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected validation write phase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::Validation);
        assert_eq!(context.target_row_count(), RowCount::unavailable());
        assert_eq!(
            context.validation_status(),
            ValidationStatus::required_but_failed(ReportReasonCode::PermissionUnavailable)
        );
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::Succeeded);
        assert!(message.contains("permission denied"));
        assert_phase_timing(
            context.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::unavailable(ReportReasonCode::PermissionUnavailable),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "initialize",
                "write 3",
                "finish",
                "count target rows",
                "cleanup CreatedTable"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn missing_exact_output_rows_prevents_false_validation_pass()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::CreateAndLoad)?;
        let prepared_target = MssqlPreparedTarget::from_output_plan(
            &output_plan,
            MssqlPreparedTargetAction::CreatedTable,
        )?;
        let report = MssqlWriteReport::from_output_plan_with_metrics(
            &output_plan,
            MssqlWriteReportMetrics::new(
                RowCount::partial(3),
                crate::MssqlBatchShapingReport::completed(1, 3, 1, 3),
                3,
                1,
                0,
                false,
                MssqlTargetCleanupStatus::Succeeded,
            ),
        );
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut connection =
            FakeSinkConnection::with_log(Arc::clone(&log)).with_target_row_count(3);

        let report = validate_written_target(
            &mut connection,
            &output_plan,
            &prepared_target,
            report,
            ValidationOptions::new(),
            TargetRowCountBeforeWrite::NotRequired,
        )
        .await?;

        assert_eq!(report.target_row_count(), RowCount::unavailable());
        assert_eq!(
            report.validation_status(),
            ValidationStatus::unavailable(ReportReasonCode::MissingExactOutputRows)
        );
        assert_eq!(logged_events(&log)?, Vec::<String>::new());
        Ok(())
    }

    #[tokio::test]
    async fn append_existing_missing_exact_output_rows_preserves_pre_count()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::AppendExisting)?;
        let prepared_target = MssqlPreparedTarget::from_output_plan(
            &output_plan,
            MssqlPreparedTargetAction::VerifiedExisting,
        )?;
        let report = MssqlWriteReport::from_output_plan_with_metrics(
            &output_plan,
            MssqlWriteReportMetrics::new(
                RowCount::partial(3),
                crate::MssqlBatchShapingReport::completed(1, 3, 1, 3),
                3,
                1,
                0,
                false,
                MssqlTargetCleanupStatus::NotApplicable,
            ),
        );
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut connection =
            FakeSinkConnection::with_log(Arc::clone(&log)).with_target_row_count(13);

        let report = validate_written_target(
            &mut connection,
            &output_plan,
            &prepared_target,
            report,
            ValidationOptions::new(),
            TargetRowCountBeforeWrite::Exact(10),
        )
        .await?;

        assert_eq!(report.target_row_count_before_write(), RowCount::exact(10));
        assert_eq!(
            report.target_row_count_after_write(),
            RowCount::unavailable()
        );
        assert_eq!(
            report.validation_status(),
            ValidationStatus::unavailable(ReportReasonCode::MissingExactOutputRows)
        );
        assert_phase_timing(
            report.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::unavailable(ReportReasonCode::MissingExactOutputRows),
        )?;
        assert_eq!(logged_events(&log)?, Vec::<String>::new());
        Ok(())
    }

    #[tokio::test]
    async fn append_existing_required_missing_exact_output_rows_fails_with_pre_count()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::AppendExisting)?;
        let prepared_target = MssqlPreparedTarget::from_output_plan(
            &output_plan,
            MssqlPreparedTargetAction::VerifiedExisting,
        )?;
        let report = MssqlWriteReport::from_output_plan_with_metrics(
            &output_plan,
            MssqlWriteReportMetrics::new(
                RowCount::partial(3),
                crate::MssqlBatchShapingReport::completed(1, 3, 1, 3),
                3,
                1,
                0,
                false,
                MssqlTargetCleanupStatus::NotApplicable,
            ),
        );
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut connection =
            FakeSinkConnection::with_log(Arc::clone(&log)).with_target_row_count(13);
        let validation_options =
            ValidationOptions::new().with_target_validation_mode(TargetValidationMode::Require);

        let error = validate_written_target(
            &mut connection,
            &output_plan,
            &prepared_target,
            report,
            validation_options,
            TargetRowCountBeforeWrite::Exact(10),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected required append missing exact rows validation failure".to_owned(),
        })?;

        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected validation write phase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::Validation);
        assert_eq!(context.output_row_count(), RowCount::partial(3));
        assert_eq!(context.target_row_count_before_write(), RowCount::exact(10));
        assert_eq!(
            context.target_row_count_after_write(),
            RowCount::unavailable()
        );
        assert_eq!(
            context.validation_status(),
            ValidationStatus::required_but_failed(ReportReasonCode::MissingExactOutputRows)
        );
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        assert!(message.contains("exact output rows"));
        assert_phase_timing(
            context.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::unavailable(ReportReasonCode::MissingExactOutputRows),
        )?;
        assert_eq!(logged_events(&log)?, Vec::<String>::new());
        Ok(())
    }

    #[tokio::test]
    async fn append_existing_validation_disabled_skips_pre_and_post_counts()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::AppendExisting)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection =
            FakeSinkConnection::with_log(Arc::clone(&log)).with_target_row_counts(vec![10, 13]);
        let batches = stream::iter(vec![Ok(orders_batch(3)?)]);
        let validation_options =
            ValidationOptions::new().with_target_validation_mode(TargetValidationMode::Disabled);

        let report = write_mssql_output_batches_on_connection_with_phase_timings(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
            validation_options,
            Vec::new(),
        )
        .await?;

        assert_eq!(report.output_row_count(), RowCount::exact(3));
        assert_eq!(
            report.target_row_count_before_write(),
            RowCount::unavailable()
        );
        assert_eq!(
            report.target_row_count_after_write(),
            RowCount::unavailable()
        );
        assert_eq!(report.validation_status(), ValidationStatus::disabled());
        assert_phase_timing(
            report.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::skipped(ReportReasonCode::ValidationDisabled),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec!["prepare", "initialize", "write 3", "finish"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_existing_matching_target_delta_passes_validation()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::AppendExisting)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection =
            FakeSinkConnection::with_log(Arc::clone(&log)).with_target_row_counts(vec![10, 13]);
        let batches = stream::iter(vec![Ok(orders_batch(3)?)]);

        let report = write_mssql_output_batches_on_connection_with_phase_timings(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
            ValidationOptions::new(),
            Vec::new(),
        )
        .await?;

        assert_eq!(report.output_row_count(), RowCount::exact(3));
        assert_eq!(report.target_row_count_before_write(), RowCount::exact(10));
        assert_eq!(report.target_row_count_after_write(), RowCount::exact(13));
        assert_eq!(report.target_row_count(), RowCount::exact(13));
        assert_eq!(report.validation_status(), ValidationStatus::passed());
        assert_phase_timing(
            report.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "count target rows",
                "initialize",
                "write 3",
                "finish",
                "count target rows"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_existing_mismatched_target_delta_fails_validation_and_reports_counts()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::AppendExisting)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection =
            FakeSinkConnection::with_log(Arc::clone(&log)).with_target_row_counts(vec![10, 14]);
        let batches = stream::iter(vec![Ok(orders_batch(3)?)]);

        let error = write_mssql_output_batches_on_connection_with_phase_timings(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
            ValidationOptions::new(),
            Vec::new(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected append delta validation mismatch".to_owned(),
        })?;

        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected validation write phase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::Validation);
        assert_eq!(context.output_row_count(), RowCount::exact(3));
        assert_eq!(context.target_row_count_before_write(), RowCount::exact(10));
        assert_eq!(context.target_row_count_after_write(), RowCount::exact(14));
        assert_eq!(context.target_row_count(), RowCount::exact(14));
        assert_eq!(context.validation_status(), ValidationStatus::failed());
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        assert!(message.contains("target row-count delta did not match"));
        assert_phase_timing(
            context.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::failed(),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            CLEANUP_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "count target rows",
                "initialize",
                "write 3",
                "finish",
                "count target rows",
                "cleanup VerifiedExisting"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_existing_validate_if_possible_reports_unavailable_when_pre_count_fails()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::AppendExisting)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log)).fail_target_row_count(
            MssqlTargetRowCountFailure::new(
                ReportReasonCode::PermissionUnavailable,
                "permission denied",
            ),
        );
        let batches = stream::iter(vec![Ok(orders_batch(3)?)]);

        let report = write_mssql_output_batches_on_connection_with_phase_timings(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
            ValidationOptions::new(),
            Vec::new(),
        )
        .await?;

        assert_eq!(report.output_row_count(), RowCount::exact(3));
        assert_eq!(
            report.target_row_count_before_write(),
            RowCount::unavailable()
        );
        assert_eq!(
            report.target_row_count_after_write(),
            RowCount::unavailable()
        );
        assert_eq!(
            report.validation_status(),
            ValidationStatus::unavailable(ReportReasonCode::PermissionUnavailable)
        );
        assert_phase_timing(
            report.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::unavailable(ReportReasonCode::PermissionUnavailable),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "count target rows",
                "initialize",
                "write 3",
                "finish"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_existing_required_pre_count_failure_stops_before_write()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::AppendExisting)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log)).fail_target_row_count(
            MssqlTargetRowCountFailure::new(
                ReportReasonCode::PermissionUnavailable,
                "permission denied",
            ),
        );
        let batches = stream::iter(vec![Ok(orders_batch(3)?)]);
        let validation_options =
            ValidationOptions::new().with_target_validation_mode(TargetValidationMode::Require);

        let error = write_mssql_output_batches_on_connection_with_phase_timings(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
            validation_options,
            Vec::new(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected required append pre-count validation failure".to_owned(),
        })?;

        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected validation write phase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::Validation);
        assert_eq!(context.output_row_count(), RowCount::unavailable());
        assert_eq!(
            context.target_row_count_before_write(),
            RowCount::unavailable()
        );
        assert_eq!(
            context.target_row_count_after_write(),
            RowCount::unavailable()
        );
        assert_eq!(
            context.validation_status(),
            ValidationStatus::required_but_failed(ReportReasonCode::PermissionUnavailable)
        );
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        assert!(message.contains("permission denied"));
        assert_phase_timing(
            context.phase_timings(),
            PREPARE_TARGET_LIFECYCLE_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::unavailable(ReportReasonCode::PermissionUnavailable),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            CLEANUP_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec!["prepare", "count target rows", "cleanup VerifiedExisting"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_existing_validate_if_possible_reports_unavailable_when_post_count_fails()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::AppendExisting)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log))
            .with_target_row_count_results(vec![
                Ok(10),
                Err(MssqlTargetRowCountFailure::new(
                    ReportReasonCode::PermissionUnavailable,
                    "permission denied",
                )),
            ]);
        let batches = stream::iter(vec![Ok(orders_batch(3)?)]);

        let report = write_mssql_output_batches_on_connection_with_phase_timings(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
            ValidationOptions::new(),
            Vec::new(),
        )
        .await?;

        assert_eq!(report.output_row_count(), RowCount::exact(3));
        assert_eq!(report.target_row_count_before_write(), RowCount::exact(10));
        assert_eq!(
            report.target_row_count_after_write(),
            RowCount::unavailable()
        );
        assert_eq!(
            report.validation_status(),
            ValidationStatus::unavailable(ReportReasonCode::PermissionUnavailable)
        );
        assert_phase_timing(
            report.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::unavailable(ReportReasonCode::PermissionUnavailable),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "count target rows",
                "initialize",
                "write 3",
                "finish",
                "count target rows"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_existing_required_post_count_failure_reports_validation_error()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::AppendExisting)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log))
            .with_target_row_count_results(vec![
                Ok(10),
                Err(MssqlTargetRowCountFailure::new(
                    ReportReasonCode::PermissionUnavailable,
                    "permission denied",
                )),
            ]);
        let batches = stream::iter(vec![Ok(orders_batch(3)?)]);
        let validation_options =
            ValidationOptions::new().with_target_validation_mode(TargetValidationMode::Require);

        let error = write_mssql_output_batches_on_connection_with_phase_timings(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
            validation_options,
            Vec::new(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected required append post-count validation failure".to_owned(),
        })?;

        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected validation write phase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::Validation);
        assert_eq!(context.output_row_count(), RowCount::exact(3));
        assert_eq!(context.target_row_count_before_write(), RowCount::exact(10));
        assert_eq!(
            context.target_row_count_after_write(),
            RowCount::unavailable()
        );
        assert_eq!(
            context.validation_status(),
            ValidationStatus::required_but_failed(ReportReasonCode::PermissionUnavailable)
        );
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        assert!(message.contains("permission denied"));
        assert_phase_timing(
            context.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::unavailable(ReportReasonCode::PermissionUnavailable),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            CLEANUP_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "count target rows",
                "initialize",
                "write 3",
                "finish",
                "count target rows",
                "cleanup VerifiedExisting"
            ]
        );
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

        let DeltaFunnelError::MssqlWritePhase { context, .. } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected write phase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::PrepareTargetLifecycle);
        assert_phase_timing(
            context.phase_timings(),
            PREPARE_TARGET_LIFECYCLE_PHASE,
            PhaseStatus::failed(),
        )?;
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
        let validation_options =
            ValidationOptions::new().with_target_validation_mode(TargetValidationMode::Disabled);

        let error = write_mssql_output_batches_on_connection_with_phase_timings(
            output_plan,
            connection,
            batches,
            default_mssql_write_options(),
            validation_options,
            Vec::new(),
        )
        .await
        .err()
        .ok_or_else(|| DeltaFunnelError::Config {
            message: "expected initialization failure".to_owned(),
        })?;

        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected write phase error".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::InitializeWriter);
        assert!(message.contains("initialize failed"));
        assert_phase_timing(
            context.phase_timings(),
            PREPARE_TARGET_LIFECYCLE_PHASE,
            PhaseStatus::completed(),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            INITIALIZE_WRITER_PHASE,
            PhaseStatus::failed(),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            CLEANUP_PHASE,
            PhaseStatus::completed(),
        )?;
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
        assert_phase_timing(
            context.phase_timings(),
            INITIALIZE_WRITER_PHASE,
            PhaseStatus::failed(),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            CLEANUP_PHASE,
            PhaseStatus::completed(),
        )?;
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
        assert_phase_timing(
            context.phase_timings(),
            "poll_batch_stream",
            PhaseStatus::failed(),
        )?;
        assert_phase_timing(
            context.phase_timings(),
            CLEANUP_PHASE,
            PhaseStatus::completed(),
        )?;
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
    async fn append_existing_finalize_failure_marks_partial_write_risk()
    -> Result<(), DeltaFunnelError> {
        let output_plan = output_plan_with_load_mode(LoadMode::AppendExisting)?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let connection = FakeSinkConnection::with_log(Arc::clone(&log))
            .with_target_row_count(10)
            .fail_finish();
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
            message: "expected append finalize failure".to_owned(),
        })?;

        let DeltaFunnelError::MssqlWritePhase { context, message } = error else {
            return Err(DeltaFunnelError::Config {
                message: "expected MssqlWritePhase finalize failure".to_owned(),
            });
        };
        assert_eq!(context.phase(), MssqlWritePhase::Finalize);
        assert_eq!(context.cleanup(), MssqlTargetCleanupStatus::NotApplicable);
        assert!(context.partial_write_possible());
        assert_eq!(
            context.validation_status(),
            ValidationStatus::skipped(ReportReasonCode::FailureBeforeValidation)
        );
        assert!(message.contains("fake sink writer failed on finish"));
        assert_phase_timing(
            context.phase_timings(),
            VALIDATION_PHASE,
            PhaseStatus::not_started(ReportReasonCode::FailureBeforeValidation),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec![
                "prepare",
                "count target rows",
                "initialize",
                "write 1",
                "finish",
                "cleanup VerifiedExisting"
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
        assert_phase_timing(
            context.phase_timings(),
            CLEANUP_PHASE,
            PhaseStatus::failed(),
        )?;
        assert_eq!(
            logged_events(&log)?,
            vec!["prepare", "initialize", "cleanup CreatedTable"]
        );
        Ok(())
    }
}
