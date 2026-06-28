//! Internal tracing vocabulary for DeltaFunnel workflow observability.

use crate::{
    DeltaFunnelError, LoadMode, MssqlBatchShapingReport, MssqlTargetTable, RunMode,
    ValidationStatus,
};

pub(crate) const TRACING_TARGET: &str = "delta_funnel";

const OUTPUT_COMPLETED_EVENT: &str = "output.completed";
const OUTPUT_FAILED_EVENT: &str = "output.failed";
const OUTPUT_SKIPPED_EVENT: &str = "output.skipped";
const OUTPUT_SPAN: &str = "delta_funnel.output";
const OUTPUT_STARTED_EVENT: &str = "output.started";
const DATAFUSION_BATCH_STREAM_COMPLETED_EVENT: &str = "datafusion_batch_stream.completed";
const DATAFUSION_BATCH_STREAM_FAILED_EVENT: &str = "datafusion_batch_stream.failed";
const DATAFUSION_BATCH_STREAM_STARTED_EVENT: &str = "datafusion_batch_stream.started";
const DATAFUSION_REGISTRATION_COMPLETED_EVENT: &str = "datafusion_registration.completed";
const DATAFUSION_REGISTRATION_FAILED_EVENT: &str = "datafusion_registration.failed";
const DATAFUSION_REGISTRATION_STARTED_EVENT: &str = "datafusion_registration.started";
const PROTOCOL_PREFLIGHT_COMPLETED_EVENT: &str = "protocol_preflight.completed";
const PROTOCOL_PREFLIGHT_FAILED_EVENT: &str = "protocol_preflight.failed";
const PROTOCOL_PREFLIGHT_STARTED_EVENT: &str = "protocol_preflight.started";
const SOURCE_LOADING_COMPLETED_EVENT: &str = "source_loading.completed";
const SOURCE_LOADING_FAILED_EVENT: &str = "source_loading.failed";
const SOURCE_LOADING_STARTED_EVENT: &str = "source_loading.started";
const VALIDATION_COMPLETED_EVENT: &str = "validation.completed";
const VALIDATION_FAILED_EVENT: &str = "validation.failed";
const VALIDATION_SKIPPED_EVENT: &str = "validation.skipped";
const VALIDATION_STARTED_EVENT: &str = "validation.started";
const WORKFLOW_COMPLETED_EVENT: &str = "workflow.completed";
const WORKFLOW_FAILED_EVENT: &str = "workflow.failed";
const WORKFLOW_SPAN: &str = "delta_funnel.workflow";
const WORKFLOW_STARTED_EVENT: &str = "workflow.started";

pub(crate) fn workflow_span(run_mode: RunMode, output_count: usize) -> tracing::Span {
    tracing::info_span!(
        target: TRACING_TARGET,
        WORKFLOW_SPAN,
        run_mode = run_mode.as_str(),
        output_count
    )
}

pub(crate) fn workflow_started(run_mode: RunMode, output_count: usize) {
    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event = WORKFLOW_STARTED_EVENT,
        run_mode = run_mode.as_str(),
        output_count,
        WORKFLOW_STARTED_EVENT
    );
}

pub(crate) fn workflow_finished<T>(
    run_mode: RunMode,
    output_count: usize,
    result: &Result<T, DeltaFunnelError>,
) {
    match result {
        Ok(_) => tracing::info!(
            target: TRACING_TARGET,
            telemetry_event = WORKFLOW_COMPLETED_EVENT,
            run_mode = run_mode.as_str(),
            output_count,
            WORKFLOW_COMPLETED_EVENT
        ),
        Err(error) => tracing::info!(
            target: TRACING_TARGET,
            telemetry_event = WORKFLOW_FAILED_EVENT,
            run_mode = run_mode.as_str(),
            output_count,
            error_category = "delta_funnel_error",
            error_summary = %error,
            WORKFLOW_FAILED_EVENT
        ),
    }
}

pub(crate) fn output_span(
    output_name: &str,
    target_table: &MssqlTargetTable,
    load_mode: LoadMode,
) -> tracing::Span {
    tracing::info_span!(
        target: TRACING_TARGET,
        OUTPUT_SPAN,
        output_name,
        target_schema = target_table.schema().unwrap_or(""),
        target_table = target_table.table(),
        load_mode = load_mode_as_str(load_mode)
    )
}

pub(crate) fn output_started(
    output_name: &str,
    target_table: &MssqlTargetTable,
    load_mode: LoadMode,
) {
    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event = OUTPUT_STARTED_EVENT,
        output_name,
        target_schema = target_table.schema().unwrap_or(""),
        target_table = target_table.table(),
        load_mode = load_mode_as_str(load_mode),
        OUTPUT_STARTED_EVENT
    );
}

pub(crate) fn output_completed(
    output_name: &str,
    target_table: &MssqlTargetTable,
    load_mode: LoadMode,
) {
    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event = OUTPUT_COMPLETED_EVENT,
        output_name,
        target_schema = target_table.schema().unwrap_or(""),
        target_table = target_table.table(),
        load_mode = load_mode_as_str(load_mode),
        OUTPUT_COMPLETED_EVENT
    );
}

pub(crate) fn output_failed(
    output_name: &str,
    target_table: &MssqlTargetTable,
    load_mode: LoadMode,
    error_summary: &str,
) {
    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event = OUTPUT_FAILED_EVENT,
        output_name,
        target_schema = target_table.schema().unwrap_or(""),
        target_table = target_table.table(),
        load_mode = load_mode_as_str(load_mode),
        error_category = "delta_funnel_error",
        error_summary,
        OUTPUT_FAILED_EVENT
    );
}

pub(crate) fn output_skipped(
    output_name: &str,
    target_table: &MssqlTargetTable,
    load_mode: LoadMode,
    skipped_reason: &str,
) {
    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event = OUTPUT_SKIPPED_EVENT,
        output_name,
        target_schema = target_table.schema().unwrap_or(""),
        target_table = target_table.table(),
        load_mode = load_mode_as_str(load_mode),
        skipped_reason,
        OUTPUT_SKIPPED_EVENT
    );
}

pub(crate) fn datafusion_batch_stream_started(
    output_name: &str,
    target_table: &MssqlTargetTable,
    load_mode: LoadMode,
) {
    output_target_event(
        DATAFUSION_BATCH_STREAM_STARTED_EVENT,
        output_name,
        target_table,
        load_mode,
    );
}

pub(crate) fn datafusion_batch_stream_finished(
    output_name: &str,
    target_table: &MssqlTargetTable,
    load_mode: LoadMode,
    report: MssqlBatchShapingReport,
) {
    let telemetry_event = match report.status().kind() {
        crate::PhaseStatusKind::Completed => DATAFUSION_BATCH_STREAM_COMPLETED_EVENT,
        crate::PhaseStatusKind::Failed => DATAFUSION_BATCH_STREAM_FAILED_EVENT,
        crate::PhaseStatusKind::Skipped
        | crate::PhaseStatusKind::NotStarted
        | crate::PhaseStatusKind::Unavailable => DATAFUSION_BATCH_STREAM_COMPLETED_EVENT,
    };
    let datafusion_batch_stream_reason = report
        .status()
        .reason()
        .map(|reason| reason.as_str())
        .unwrap_or("");

    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event,
        output_name,
        target_schema = target_table.schema().unwrap_or(""),
        target_table = target_table.table(),
        load_mode = load_mode_as_str(load_mode),
        datafusion_batch_stream_status = report.status().kind().as_str(),
        datafusion_batch_stream_reason,
        input_batches = report.input_batches(),
        input_rows = report.input_rows(),
        output_batches = report.output_batches(),
        output_rows = report.output_rows(),
        message = telemetry_event
    );
}

pub(crate) fn source_loading_started(source_name: &str) {
    source_phase_started(SOURCE_LOADING_STARTED_EVENT, source_name);
}

pub(crate) fn source_loading_completed(source_name: &str, snapshot_version: u64) {
    source_phase_completed(
        SOURCE_LOADING_COMPLETED_EVENT,
        source_name,
        snapshot_version,
    );
}

pub(crate) fn source_loading_failed(source_name: &str, error: &DeltaFunnelError) {
    source_phase_failed(SOURCE_LOADING_FAILED_EVENT, source_name, error);
}

pub(crate) fn protocol_preflight_started(source_name: &str, snapshot_version: u64) {
    source_phase_started_with_snapshot(
        PROTOCOL_PREFLIGHT_STARTED_EVENT,
        source_name,
        snapshot_version,
    );
}

pub(crate) fn protocol_preflight_completed(source_name: &str, snapshot_version: u64) {
    source_phase_completed(
        PROTOCOL_PREFLIGHT_COMPLETED_EVENT,
        source_name,
        snapshot_version,
    );
}

pub(crate) fn protocol_preflight_failed(
    source_name: &str,
    snapshot_version: u64,
    error: &DeltaFunnelError,
) {
    source_phase_failed_with_snapshot(
        PROTOCOL_PREFLIGHT_FAILED_EVENT,
        source_name,
        snapshot_version,
        error,
    );
}

pub(crate) fn datafusion_registration_started(source_name: &str, snapshot_version: u64) {
    source_phase_started_with_snapshot(
        DATAFUSION_REGISTRATION_STARTED_EVENT,
        source_name,
        snapshot_version,
    );
}

pub(crate) fn datafusion_registration_completed(source_name: &str, snapshot_version: u64) {
    source_phase_completed(
        DATAFUSION_REGISTRATION_COMPLETED_EVENT,
        source_name,
        snapshot_version,
    );
}

pub(crate) fn datafusion_registration_failed(
    source_name: &str,
    snapshot_version: u64,
    error: &DeltaFunnelError,
) {
    source_phase_failed_with_snapshot(
        DATAFUSION_REGISTRATION_FAILED_EVENT,
        source_name,
        snapshot_version,
        error,
    );
}

pub(crate) fn validation_started(target_table: &MssqlTargetTable, load_mode: LoadMode) {
    validation_event(VALIDATION_STARTED_EVENT, target_table, load_mode, None);
}

pub(crate) fn validation_finished(
    target_table: &MssqlTargetTable,
    load_mode: LoadMode,
    status: ValidationStatus,
) {
    let telemetry_event = match status.kind() {
        crate::ValidationStatusKind::Passed => VALIDATION_COMPLETED_EVENT,
        crate::ValidationStatusKind::Failed | crate::ValidationStatusKind::RequiredButFailed => {
            VALIDATION_FAILED_EVENT
        }
        crate::ValidationStatusKind::Disabled
        | crate::ValidationStatusKind::Skipped
        | crate::ValidationStatusKind::Unavailable => VALIDATION_SKIPPED_EVENT,
    };

    validation_event(telemetry_event, target_table, load_mode, Some(status));
}

impl RunMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Execute => "execute",
            Self::DryRun => "dry_run",
        }
    }
}

fn source_phase_started(telemetry_event: &str, source_name: &str) {
    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event,
        source_name,
        message = telemetry_event
    );
}

fn output_target_event(
    telemetry_event: &str,
    output_name: &str,
    target_table: &MssqlTargetTable,
    load_mode: LoadMode,
) {
    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event,
        output_name,
        target_schema = target_table.schema().unwrap_or(""),
        target_table = target_table.table(),
        load_mode = load_mode_as_str(load_mode),
        message = telemetry_event
    );
}

fn source_phase_started_with_snapshot(
    telemetry_event: &str,
    source_name: &str,
    snapshot_version: u64,
) {
    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event,
        source_name,
        snapshot_version,
        message = telemetry_event
    );
}

fn source_phase_completed(telemetry_event: &str, source_name: &str, snapshot_version: u64) {
    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event,
        source_name,
        snapshot_version,
        message = telemetry_event
    );
}

fn source_phase_failed(telemetry_event: &str, source_name: &str, error: &DeltaFunnelError) {
    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event,
        source_name,
        error_category = "delta_funnel_error",
        error_summary = %error,
        message = telemetry_event
    );
}

fn source_phase_failed_with_snapshot(
    telemetry_event: &str,
    source_name: &str,
    snapshot_version: u64,
    error: &DeltaFunnelError,
) {
    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event,
        source_name,
        snapshot_version,
        error_category = "delta_funnel_error",
        error_summary = %error,
        message = telemetry_event
    );
}

fn validation_event(
    telemetry_event: &str,
    target_table: &MssqlTargetTable,
    load_mode: LoadMode,
    status: Option<ValidationStatus>,
) {
    let validation_status = status.map(|status| status.kind().as_str()).unwrap_or("");
    let validation_reason = status
        .and_then(|status| status.reason())
        .map(|reason| reason.as_str())
        .unwrap_or("");

    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event,
        target_schema = target_table.schema().unwrap_or(""),
        target_table = target_table.table(),
        load_mode = load_mode_as_str(load_mode),
        validation_status,
        validation_reason,
        message = telemetry_event
    );
}

const fn load_mode_as_str(load_mode: LoadMode) -> &'static str {
    match load_mode {
        LoadMode::AppendExisting => "append_existing",
        LoadMode::CreateAndLoad => "create_and_load",
        LoadMode::Replace => "replace",
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fmt,
        sync::{Arc, Mutex},
    };

    use super::*;
    use tracing::{
        Event, Level, Subscriber,
        field::{Field, Visit},
        span::{Attributes, Id},
    };
    use tracing_subscriber::{Layer, Registry, layer::Context, prelude::*, registry::LookupSpan};

    #[test]
    fn observability_event_helpers_do_not_require_subscriber() -> Result<(), DeltaFunnelError> {
        workflow_started(RunMode::Execute, 2);
        workflow_started(RunMode::DryRun, 0);
        workflow_finished(RunMode::Execute, 2, &Ok(()));
        workflow_finished::<()>(
            RunMode::DryRun,
            0,
            &Err(DeltaFunnelError::Config {
                message: "missing option".to_owned(),
            }),
        );
        let target_table = MssqlTargetTable::new("dbo", "orders")?;
        output_started("orders_output", &target_table, LoadMode::AppendExisting);
        output_completed("orders_output", &target_table, LoadMode::AppendExisting);
        output_failed(
            "orders_output",
            &target_table,
            LoadMode::AppendExisting,
            "write failed",
        );
        output_skipped(
            "orders_output",
            &target_table,
            LoadMode::AppendExisting,
            "prior_failure",
        );
        datafusion_batch_stream_started("orders_output", &target_table, LoadMode::AppendExisting);
        datafusion_batch_stream_finished(
            "orders_output",
            &target_table,
            LoadMode::AppendExisting,
            MssqlBatchShapingReport::completed(2, 5, 2, 5),
        );
        source_loading_started("orders");
        source_loading_completed("orders", 7);
        protocol_preflight_started("orders", 7);
        protocol_preflight_completed("orders", 7);
        datafusion_registration_started("orders", 7);
        datafusion_registration_completed("orders", 7);
        validation_started(&target_table, LoadMode::AppendExisting);
        validation_finished(
            &target_table,
            LoadMode::AppendExisting,
            ValidationStatus::passed(),
        );
        validation_finished(
            &target_table,
            LoadMode::AppendExisting,
            ValidationStatus::required_but_failed(crate::ReportReasonCode::MissingTargetAccess),
        );
        validation_finished(
            &target_table,
            LoadMode::AppendExisting,
            ValidationStatus::disabled(),
        );
        Ok(())
    }

    #[test]
    fn observability_uses_stable_workflow_vocabulary() {
        assert_eq!(TRACING_TARGET, "delta_funnel");
        assert_eq!(WORKFLOW_COMPLETED_EVENT, "workflow.completed");
        assert_eq!(WORKFLOW_FAILED_EVENT, "workflow.failed");
        assert_eq!(WORKFLOW_SPAN, "delta_funnel.workflow");
        assert_eq!(WORKFLOW_STARTED_EVENT, "workflow.started");
        assert_eq!(OUTPUT_COMPLETED_EVENT, "output.completed");
        assert_eq!(OUTPUT_FAILED_EVENT, "output.failed");
        assert_eq!(OUTPUT_SKIPPED_EVENT, "output.skipped");
        assert_eq!(OUTPUT_SPAN, "delta_funnel.output");
        assert_eq!(OUTPUT_STARTED_EVENT, "output.started");
        assert_eq!(
            DATAFUSION_BATCH_STREAM_COMPLETED_EVENT,
            "datafusion_batch_stream.completed"
        );
        assert_eq!(
            DATAFUSION_BATCH_STREAM_FAILED_EVENT,
            "datafusion_batch_stream.failed"
        );
        assert_eq!(
            DATAFUSION_BATCH_STREAM_STARTED_EVENT,
            "datafusion_batch_stream.started"
        );
        assert_eq!(SOURCE_LOADING_COMPLETED_EVENT, "source_loading.completed");
        assert_eq!(SOURCE_LOADING_FAILED_EVENT, "source_loading.failed");
        assert_eq!(SOURCE_LOADING_STARTED_EVENT, "source_loading.started");
        assert_eq!(
            PROTOCOL_PREFLIGHT_COMPLETED_EVENT,
            "protocol_preflight.completed"
        );
        assert_eq!(PROTOCOL_PREFLIGHT_FAILED_EVENT, "protocol_preflight.failed");
        assert_eq!(
            PROTOCOL_PREFLIGHT_STARTED_EVENT,
            "protocol_preflight.started"
        );
        assert_eq!(
            DATAFUSION_REGISTRATION_COMPLETED_EVENT,
            "datafusion_registration.completed"
        );
        assert_eq!(
            DATAFUSION_REGISTRATION_FAILED_EVENT,
            "datafusion_registration.failed"
        );
        assert_eq!(
            DATAFUSION_REGISTRATION_STARTED_EVENT,
            "datafusion_registration.started"
        );
        assert_eq!(VALIDATION_COMPLETED_EVENT, "validation.completed");
        assert_eq!(VALIDATION_FAILED_EVENT, "validation.failed");
        assert_eq!(VALIDATION_SKIPPED_EVENT, "validation.skipped");
        assert_eq!(VALIDATION_STARTED_EVENT, "validation.started");
        assert_eq!(RunMode::Execute.as_str(), "execute");
        assert_eq!(RunMode::DryRun.as_str(), "dry_run");
        assert_eq!(
            load_mode_as_str(LoadMode::AppendExisting),
            "append_existing"
        );
        assert_eq!(load_mode_as_str(LoadMode::CreateAndLoad), "create_and_load");
        assert_eq!(load_mode_as_str(LoadMode::Replace), "replace");
    }

    #[test]
    fn scoped_capture_records_workflow_event_fields() {
        let events = CapturedEvents::default();
        let subscriber = Registry::default().with(CaptureLayer {
            events: events.clone(),
        });

        tracing::subscriber::with_default(subscriber, || {
            workflow_started(RunMode::Execute, 2);
            workflow_finished(RunMode::Execute, 2, &Ok(()));
            workflow_finished::<()>(
                RunMode::DryRun,
                1,
                &Err(DeltaFunnelError::Config {
                    message: "missing option".to_owned(),
                }),
            );
        });

        let events = events.events();
        assert_eq!(events.len(), 3);
        assert_workflow_event(&events[0], WORKFLOW_STARTED_EVENT, RunMode::Execute, "2");
        assert_workflow_event(&events[1], WORKFLOW_COMPLETED_EVENT, RunMode::Execute, "2");
        assert_workflow_event(&events[2], WORKFLOW_FAILED_EVENT, RunMode::DryRun, "1");
        assert_eq!(
            events[2].fields.get("error_category").map(String::as_str),
            Some("delta_funnel_error")
        );
        assert_eq!(
            events[2].fields.get("error_summary").map(String::as_str),
            Some("configuration error: missing option")
        );
    }

    #[test]
    fn scoped_capture_records_workflow_span_fields() {
        let events = CapturedEvents::default();
        let subscriber = Registry::default().with(CaptureLayer {
            events: events.clone(),
        });

        tracing::subscriber::with_default(subscriber, || {
            let span = workflow_span(RunMode::Execute, 2);
            let _guard = span.enter();
            workflow_started(RunMode::Execute, 2);
        });

        let spans = events.spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].target, TRACING_TARGET);
        assert_eq!(spans[0].name, WORKFLOW_SPAN);
        assert_eq!(spans[0].level, Level::INFO);
        assert_eq!(
            spans[0].fields.get("run_mode").map(String::as_str),
            Some("execute")
        );
        assert_eq!(
            spans[0].fields.get("output_count").map(String::as_str),
            Some("2")
        );
    }

    #[test]
    fn scoped_capture_records_output_event_fields() -> Result<(), DeltaFunnelError> {
        let events = CapturedEvents::default();
        let subscriber = Registry::default().with(CaptureLayer {
            events: events.clone(),
        });
        let target_table = MssqlTargetTable::new("dbo", "orders")?;

        tracing::subscriber::with_default(subscriber, || {
            output_started("orders_output", &target_table, LoadMode::AppendExisting);
            output_completed("orders_output", &target_table, LoadMode::AppendExisting);
            output_failed(
                "orders_output",
                &target_table,
                LoadMode::AppendExisting,
                "write failed",
            );
            output_skipped(
                "orders_output",
                &target_table,
                LoadMode::AppendExisting,
                "prior_failure",
            );
        });

        let events = events.events();
        assert_eq!(events.len(), 4);
        assert_output_event(&events[0], OUTPUT_STARTED_EVENT);
        assert_output_event(&events[1], OUTPUT_COMPLETED_EVENT);
        assert_output_event(&events[2], OUTPUT_FAILED_EVENT);
        assert_eq!(
            events[2].fields.get("error_summary").map(String::as_str),
            Some("write failed")
        );
        assert_output_event(&events[3], OUTPUT_SKIPPED_EVENT);
        assert_eq!(
            events[3].fields.get("skipped_reason").map(String::as_str),
            Some("prior_failure")
        );
        Ok(())
    }

    #[test]
    fn scoped_capture_records_output_span_fields() -> Result<(), DeltaFunnelError> {
        let events = CapturedEvents::default();
        let subscriber = Registry::default().with(CaptureLayer {
            events: events.clone(),
        });
        let target_table = MssqlTargetTable::new("dbo", "orders")?;

        tracing::subscriber::with_default(subscriber, || {
            let span = output_span("orders_output", &target_table, LoadMode::AppendExisting);
            let _guard = span.enter();
            output_started("orders_output", &target_table, LoadMode::AppendExisting);
        });

        let spans = events.spans();
        assert_eq!(spans.len(), 1);
        assert_output_span(&spans[0]);
        Ok(())
    }

    #[test]
    fn scoped_capture_records_nested_downstream_event_scope() -> Result<(), DeltaFunnelError> {
        let events = CapturedEvents::default();
        let subscriber = Registry::default().with(CaptureLayer {
            events: events.clone(),
        });
        let target_table = MssqlTargetTable::new("dbo", "orders")?;

        tracing::subscriber::with_default(subscriber, || {
            let workflow_span = workflow_span(RunMode::Execute, 1);
            let _workflow_guard = workflow_span.enter();
            let output_span = output_span("orders_output", &target_table, LoadMode::CreateAndLoad);
            let _output_guard = output_span.enter();

            tracing::info!(
                target: "arrow_tiberius",
                telemetry_event = "batch_write.completed",
                "batch_write.completed"
            );
        });

        let events = events.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].target, "arrow_tiberius");
        assert_eq!(events[0].span_names, vec![WORKFLOW_SPAN, OUTPUT_SPAN]);
        Ok(())
    }

    #[test]
    fn scoped_capture_records_datafusion_batch_stream_event_fields() -> Result<(), DeltaFunnelError>
    {
        let events = CapturedEvents::default();
        let subscriber = Registry::default().with(CaptureLayer {
            events: events.clone(),
        });
        let target_table = MssqlTargetTable::new("dbo", "orders")?;

        tracing::subscriber::with_default(subscriber, || {
            datafusion_batch_stream_started(
                "orders_output",
                &target_table,
                LoadMode::AppendExisting,
            );
            datafusion_batch_stream_finished(
                "orders_output",
                &target_table,
                LoadMode::AppendExisting,
                MssqlBatchShapingReport::completed(2, 5, 2, 5),
            );
            datafusion_batch_stream_finished(
                "orders_output",
                &target_table,
                LoadMode::AppendExisting,
                MssqlBatchShapingReport::failed(3, 8, 2, 5),
            );
        });

        let events = events.events();
        assert_eq!(events.len(), 3);
        assert_output_event(&events[0], DATAFUSION_BATCH_STREAM_STARTED_EVENT);
        assert_datafusion_batch_stream_event(
            &events[1],
            DATAFUSION_BATCH_STREAM_COMPLETED_EVENT,
            "completed",
            "2",
            "5",
            "2",
            "5",
        );
        assert_datafusion_batch_stream_event(
            &events[2],
            DATAFUSION_BATCH_STREAM_FAILED_EVENT,
            "failed",
            "3",
            "8",
            "2",
            "5",
        );

        Ok(())
    }

    #[test]
    fn scoped_capture_records_source_phase_event_fields() {
        let events = CapturedEvents::default();
        let subscriber = Registry::default().with(CaptureLayer {
            events: events.clone(),
        });

        tracing::subscriber::with_default(subscriber, || {
            source_loading_started("orders");
            source_loading_completed("orders", 7);
            protocol_preflight_started("orders", 7);
            protocol_preflight_failed(
                "orders",
                7,
                &DeltaFunnelError::Config {
                    message: "bad source".to_owned(),
                },
            );
            datafusion_registration_started("orders", 7);
            datafusion_registration_completed("orders", 7);
        });

        let events = events.events();
        assert_eq!(events.len(), 6);
        assert_source_phase_event(&events[0], SOURCE_LOADING_STARTED_EVENT, None);
        assert_source_phase_event(&events[1], SOURCE_LOADING_COMPLETED_EVENT, Some("7"));
        assert_source_phase_event(&events[2], PROTOCOL_PREFLIGHT_STARTED_EVENT, Some("7"));
        assert_source_phase_event(&events[3], PROTOCOL_PREFLIGHT_FAILED_EVENT, Some("7"));
        assert_eq!(
            events[3].fields.get("error_category").map(String::as_str),
            Some("delta_funnel_error")
        );
        assert_eq!(
            events[3].fields.get("error_summary").map(String::as_str),
            Some("configuration error: bad source")
        );
        assert_source_phase_event(&events[4], DATAFUSION_REGISTRATION_STARTED_EVENT, Some("7"));
        assert_source_phase_event(
            &events[5],
            DATAFUSION_REGISTRATION_COMPLETED_EVENT,
            Some("7"),
        );
    }

    #[test]
    fn scoped_capture_records_validation_event_fields() -> Result<(), DeltaFunnelError> {
        let events = CapturedEvents::default();
        let subscriber = Registry::default().with(CaptureLayer {
            events: events.clone(),
        });
        let target_table = MssqlTargetTable::new("dbo", "orders")?;

        tracing::subscriber::with_default(subscriber, || {
            validation_started(&target_table, LoadMode::CreateAndLoad);
            validation_finished(
                &target_table,
                LoadMode::CreateAndLoad,
                ValidationStatus::passed(),
            );
            validation_finished(
                &target_table,
                LoadMode::CreateAndLoad,
                ValidationStatus::required_but_failed(
                    crate::ReportReasonCode::MissingExactOutputRows,
                ),
            );
            validation_finished(
                &target_table,
                LoadMode::CreateAndLoad,
                ValidationStatus::disabled(),
            );
        });

        let events = events.events();
        assert_eq!(events.len(), 4);
        assert_validation_event(&events[0], VALIDATION_STARTED_EVENT, "", "");
        assert_validation_event(&events[1], VALIDATION_COMPLETED_EVENT, "passed", "");
        assert_validation_event(
            &events[2],
            VALIDATION_FAILED_EVENT,
            "required_but_failed",
            "missing_exact_output_rows",
        );
        assert_validation_event(
            &events[3],
            VALIDATION_SKIPPED_EVENT,
            "disabled",
            "validation_disabled",
        );

        Ok(())
    }

    fn assert_workflow_event(
        event: &CapturedEvent,
        telemetry_event: &str,
        run_mode: RunMode,
        output_count: &str,
    ) {
        assert_eq!(event.target, TRACING_TARGET);
        assert_eq!(event.level, Level::INFO);
        assert_eq!(
            event.fields.get("telemetry_event").map(String::as_str),
            Some(telemetry_event)
        );
        assert_eq!(
            event.fields.get("run_mode").map(String::as_str),
            Some(run_mode.as_str())
        );
        assert_eq!(
            event.fields.get("output_count").map(String::as_str),
            Some(output_count)
        );
    }

    fn assert_output_event(event: &CapturedEvent, telemetry_event: &str) {
        assert_eq!(event.target, TRACING_TARGET);
        assert_eq!(event.level, Level::INFO);
        assert_eq!(
            event.fields.get("telemetry_event").map(String::as_str),
            Some(telemetry_event)
        );
        assert_eq!(
            event.fields.get("output_name").map(String::as_str),
            Some("orders_output")
        );
        assert_eq!(
            event.fields.get("target_schema").map(String::as_str),
            Some("dbo")
        );
        assert_eq!(
            event.fields.get("target_table").map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            event.fields.get("load_mode").map(String::as_str),
            Some("append_existing")
        );
    }

    fn assert_output_span(span: &CapturedSpan) {
        assert_eq!(span.target, TRACING_TARGET);
        assert_eq!(span.name, OUTPUT_SPAN);
        assert_eq!(span.level, Level::INFO);
        assert_eq!(
            span.fields.get("output_name").map(String::as_str),
            Some("orders_output")
        );
        assert_eq!(
            span.fields.get("target_schema").map(String::as_str),
            Some("dbo")
        );
        assert_eq!(
            span.fields.get("target_table").map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            span.fields.get("load_mode").map(String::as_str),
            Some("append_existing")
        );
    }

    fn assert_datafusion_batch_stream_event(
        event: &CapturedEvent,
        telemetry_event: &str,
        datafusion_batch_stream_status: &str,
        input_batches: &str,
        input_rows: &str,
        output_batches: &str,
        output_rows: &str,
    ) {
        assert_output_event(event, telemetry_event);
        assert_eq!(
            event
                .fields
                .get("datafusion_batch_stream_status")
                .map(String::as_str),
            Some(datafusion_batch_stream_status)
        );
        assert_eq!(
            event
                .fields
                .get("datafusion_batch_stream_reason")
                .map(String::as_str),
            Some("")
        );
        assert_eq!(
            event.fields.get("input_batches").map(String::as_str),
            Some(input_batches)
        );
        assert_eq!(
            event.fields.get("input_rows").map(String::as_str),
            Some(input_rows)
        );
        assert_eq!(
            event.fields.get("output_batches").map(String::as_str),
            Some(output_batches)
        );
        assert_eq!(
            event.fields.get("output_rows").map(String::as_str),
            Some(output_rows)
        );
    }

    fn assert_source_phase_event(
        event: &CapturedEvent,
        telemetry_event: &str,
        snapshot_version: Option<&str>,
    ) {
        assert_eq!(event.target, TRACING_TARGET);
        assert_eq!(event.level, Level::INFO);
        assert_eq!(
            event.fields.get("telemetry_event").map(String::as_str),
            Some(telemetry_event)
        );
        assert_eq!(
            event.fields.get("source_name").map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            event.fields.get("snapshot_version").map(String::as_str),
            snapshot_version
        );
    }

    fn assert_validation_event(
        event: &CapturedEvent,
        telemetry_event: &str,
        validation_status: &str,
        validation_reason: &str,
    ) {
        assert_eq!(event.target, TRACING_TARGET);
        assert_eq!(event.level, Level::INFO);
        assert_eq!(
            event.fields.get("telemetry_event").map(String::as_str),
            Some(telemetry_event)
        );
        assert_eq!(
            event.fields.get("target_schema").map(String::as_str),
            Some("dbo")
        );
        assert_eq!(
            event.fields.get("target_table").map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            event.fields.get("load_mode").map(String::as_str),
            Some("create_and_load")
        );
        assert_eq!(
            event.fields.get("validation_status").map(String::as_str),
            Some(validation_status)
        );
        assert_eq!(
            event.fields.get("validation_reason").map(String::as_str),
            Some(validation_reason)
        );
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct CapturedEvent {
        target: &'static str,
        level: Level,
        fields: BTreeMap<String, String>,
        span_names: Vec<&'static str>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct CapturedSpan {
        target: &'static str,
        name: &'static str,
        level: Level,
        fields: BTreeMap<String, String>,
    }

    #[derive(Clone, Default)]
    struct CapturedEvents {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
        spans: Arc<Mutex<Vec<CapturedSpan>>>,
    }

    impl CapturedEvents {
        fn events(&self) -> Vec<CapturedEvent> {
            match self.events.lock() {
                Ok(events) => events.clone(),
                Err(_) => Vec::new(),
            }
        }

        fn spans(&self) -> Vec<CapturedSpan> {
            match self.spans.lock() {
                Ok(spans) => spans.clone(),
                Err(_) => Vec::new(),
            }
        }
    }

    struct CaptureLayer {
        events: CapturedEvents,
    }

    impl<S> Layer<S> for CaptureLayer
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
            let mut visitor = FieldVisitor::default();
            attrs.record(&mut visitor);
            let captured = CapturedSpan {
                target: attrs.metadata().target(),
                name: attrs.metadata().name(),
                level: *attrs.metadata().level(),
                fields: visitor.fields,
            };

            if let Ok(mut spans) = self.events.spans.lock() {
                spans.push(captured);
            }
        }

        fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
            let mut visitor = FieldVisitor::default();
            event.record(&mut visitor);
            let span_names = ctx
                .event_scope(event)
                .map(|scope| {
                    scope
                        .from_root()
                        .map(|span| span.metadata().name())
                        .collect()
                })
                .unwrap_or_default();
            let captured = CapturedEvent {
                target: event.metadata().target(),
                level: *event.metadata().level(),
                fields: visitor.fields,
                span_names,
            };

            if let Ok(mut events) = self.events.events.lock() {
                events.push(captured);
            }
        }
    }

    #[derive(Default)]
    struct FieldVisitor {
        fields: BTreeMap<String, String>,
    }

    impl FieldVisitor {
        fn record_value(&mut self, field: &Field, value: impl Into<String>) {
            self.fields.insert(field.name().to_owned(), value.into());
        }
    }

    impl Visit for FieldVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.record_value(field, format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.record_value(field, value);
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            self.record_value(field, value.to_string());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.record_value(field, value.to_string());
        }

        fn record_bool(&mut self, field: &Field, value: bool) {
            self.record_value(field, value.to_string());
        }
    }
}
