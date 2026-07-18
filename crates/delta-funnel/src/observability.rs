//! Internal tracing vocabulary for DeltaFunnel workflow observability.

use crate::{
    DeltaFunnelError, DeltaProviderReadStatsSnapshot, DeltaProviderReaderBackend, LoadMode,
    MssqlBatchShapingReport, MssqlTargetTable, QueryExecutionMetricValue, QueryExecutionOutcome,
    QueryExecutionProfile, RunMode, ValidationStatus,
    support::{sanitize_text_for_display, sanitize_uri_for_display},
    usize_to_u64_saturating,
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
const DELTA_PROVIDER_PARQUET_IO_SUMMARY_EVENT: &str = "delta_provider_parquet_io_summary";
const QUERY_EXECUTION_PROFILE_TERMINAL_EVENT: &str = "query_execution_profile_terminal";
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

/// Terminal outcome for one Delta provider-scan execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeltaProviderScanOutcome {
    /// Every required output stream reached normal end-of-stream.
    Success,
    /// At least one required output stream yielded an upstream error.
    Error,
    /// A required output stream was dropped before normal end-of-stream.
    Cancelled,
}

impl DeltaProviderScanOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }

    pub(crate) const fn query_execution_outcome(self) -> QueryExecutionOutcome {
        match self {
            Self::Success => QueryExecutionOutcome::Success,
            Self::Error => QueryExecutionOutcome::Error,
            Self::Cancelled => QueryExecutionOutcome::Cancelled,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParquetIoMetrics {
    Available {
        range_get_operations: u64,
        full_get_operations: u64,
        bytes_received: u64,
        opened_bytes: u64,
    },
    Unavailable,
    Mixed,
}

/// Emits one bounded terminal Parquet I/O summary for a Delta provider scan.
pub(crate) fn delta_provider_parquet_io_summary(
    snapshot: &DeltaProviderReadStatsSnapshot,
    outcome: DeltaProviderScanOutcome,
) {
    let source_name = sanitize_observability_summary(&snapshot.source_name);
    let reader_backend = provider_reader_backend_as_str(snapshot.reader_backend);
    let outcome = outcome.as_str();

    match parquet_io_metrics(snapshot) {
        ParquetIoMetrics::Available {
            range_get_operations,
            full_get_operations,
            bytes_received,
            opened_bytes,
        } => tracing::debug!(
            target: TRACING_TARGET,
            telemetry_event = DELTA_PROVIDER_PARQUET_IO_SUMMARY_EVENT,
            source_name,
            snapshot_version = snapshot.snapshot_version,
            reader_backend,
            outcome,
            metrics_available = true,
            parquet_data_file_range_get_operations = range_get_operations,
            parquet_data_file_full_get_operations = full_get_operations,
            parquet_data_file_bytes_received = bytes_received,
            parquet_data_file_opened_bytes = opened_bytes,
            message = DELTA_PROVIDER_PARQUET_IO_SUMMARY_EVENT
        ),
        ParquetIoMetrics::Unavailable | ParquetIoMetrics::Mixed => tracing::debug!(
            target: TRACING_TARGET,
            telemetry_event = DELTA_PROVIDER_PARQUET_IO_SUMMARY_EVENT,
            source_name,
            snapshot_version = snapshot.snapshot_version,
            reader_backend,
            outcome,
            metrics_available = false,
            message = DELTA_PROVIDER_PARQUET_IO_SUMMARY_EVENT
        ),
    }
}

pub(crate) fn query_execution_profile_terminal(profile: &QueryExecutionProfile) {
    let operator_count = usize_to_u64_saturating(profile.operators().len());
    let operators_with_metrics = usize_to_u64_saturating(
        profile
            .operators()
            .iter()
            .filter(|operator| operator.metrics_available())
            .count(),
    );
    let root_output_rows = profile.operators().first().and_then(|operator| {
        operator.aggregated_metrics().iter().find_map(|metric| {
            match (metric.name(), metric.value()) {
                ("output_rows", QueryExecutionMetricValue::Count(value)) => Some(*value),
                _ => None,
            }
        })
    });
    let mut max_elapsed_compute = None;
    for operator in profile.operators() {
        for metric in operator.aggregated_metrics() {
            let ("elapsed_compute", QueryExecutionMetricValue::Nanoseconds(nanos)) =
                (metric.name(), metric.value())
            else {
                continue;
            };
            if max_elapsed_compute.is_none_or(|(_, max_nanos)| *nanos > max_nanos) {
                max_elapsed_compute = Some((operator.operator_name(), *nanos));
            }
        }
    }
    let max_elapsed_compute_operator = max_elapsed_compute.map(|(operator, _)| operator);
    let max_elapsed_compute_nanos = max_elapsed_compute.map(|(_, nanos)| nanos);

    tracing::debug!(
        target: TRACING_TARGET,
        telemetry_event = QUERY_EXECUTION_PROFILE_TERMINAL_EVENT,
        scope = profile.scope().as_str(),
        outcome = profile.outcome().as_str(),
        partial = profile.partial(),
        delta_funnel_row_limit = profile.delta_funnel_row_limit(),
        operator_count,
        operators_with_metrics,
        root_output_rows,
        max_elapsed_compute_operator,
        max_elapsed_compute_nanos,
    );
}

fn parquet_io_metrics(snapshot: &DeltaProviderReadStatsSnapshot) -> ParquetIoMetrics {
    match (
        snapshot.parquet_data_file_range_get_operations,
        snapshot.parquet_data_file_full_get_operations,
        snapshot.parquet_data_file_bytes_received,
        snapshot.parquet_data_file_opened_bytes,
    ) {
        (Some(range), Some(full), Some(received), Some(opened)) => ParquetIoMetrics::Available {
            range_get_operations: range,
            full_get_operations: full,
            bytes_received: received,
            opened_bytes: opened,
        },
        (None, None, None, None) => ParquetIoMetrics::Unavailable,
        _ => ParquetIoMetrics::Mixed,
    }
}

const fn provider_reader_backend_as_str(backend: DeltaProviderReaderBackend) -> &'static str {
    match backend {
        DeltaProviderReaderBackend::OfficialKernel => "official_kernel",
        DeltaProviderReaderBackend::NativeAsync => "native_async",
    }
}

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
            error_summary = sanitize_error_summary(error),
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
    let error_summary = sanitize_observability_summary(error_summary);
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

pub(crate) fn source_loading_started(source_name: &str, derived_s3_auth_mode: Option<&str>) {
    let derived_s3_auth_mode = derived_s3_auth_mode.unwrap_or("");
    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event = SOURCE_LOADING_STARTED_EVENT,
        source_name,
        derived_s3_auth_mode,
        message = SOURCE_LOADING_STARTED_EVENT
    );
}

pub(crate) fn source_loading_completed(
    source_name: &str,
    snapshot_version: u64,
    derived_s3_auth_mode: Option<&str>,
) {
    let derived_s3_auth_mode = derived_s3_auth_mode.unwrap_or("");
    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event = SOURCE_LOADING_COMPLETED_EVENT,
        source_name,
        snapshot_version,
        derived_s3_auth_mode,
        message = SOURCE_LOADING_COMPLETED_EVENT
    );
}

pub(crate) fn source_loading_failed(
    source_name: &str,
    error: &DeltaFunnelError,
    derived_s3_auth_mode: Option<&str>,
) {
    let derived_s3_auth_mode = derived_s3_auth_mode.unwrap_or("");
    tracing::info!(
        target: TRACING_TARGET,
        telemetry_event = SOURCE_LOADING_FAILED_EVENT,
        source_name,
        derived_s3_auth_mode,
        error_category = "delta_funnel_error",
        error_summary = sanitize_error_summary(error),
        message = SOURCE_LOADING_FAILED_EVENT
    );
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
        error_summary = sanitize_error_summary(error),
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

fn sanitize_error_summary(error: &DeltaFunnelError) -> String {
    sanitize_observability_summary(&error.to_string())
}

fn sanitize_observability_summary(summary: &str) -> String {
    let summary = sanitize_text_for_display(summary);
    if looks_like_raw_sql(&summary) {
        return "<redacted>".to_owned();
    }

    summary
        .split_whitespace()
        .map(sanitize_observability_token)
        .collect::<Vec<_>>()
        .join(" ")
}

fn sanitize_observability_token(token: &str) -> String {
    let sanitized = if token.contains("://") {
        sanitize_uri_for_display(token)
    } else {
        token.to_owned()
    };

    if contains_secret_marker(&sanitized) {
        "<redacted>".to_owned()
    } else {
        sanitized
    }
}

fn contains_secret_marker(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    [
        "access_key",
        "accesskey",
        "credential",
        "password",
        "pwd=",
        "secret",
        "session_token",
        "token=",
    ]
    .iter()
    .any(|marker| value.contains(marker))
}

fn looks_like_raw_sql(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    ["select ", "insert ", "update ", "delete ", "merge "]
        .iter()
        .any(|marker| value.contains(marker))
}

/// Shared tracing capture support for unit tests in this crate.
#[cfg(test)]
pub(crate) mod test_capture {
    use std::{
        collections::BTreeMap,
        fmt,
        sync::{Arc, Mutex, MutexGuard, Once},
    };

    use tracing::{
        Event, Level, Subscriber,
        field::{Field, Visit},
        span::{Attributes, Id, Record},
    };
    use tracing_subscriber::{Layer, Registry, layer::Context, prelude::*, registry::LookupSpan};

    static TRACING_TEST_LOCK: Mutex<()> = Mutex::new(());
    static TRACING_TEST_GLOBAL_SUBSCRIBER: Once = Once::new();

    pub(crate) struct TracingTestGuard {
        _guard: MutexGuard<'static, ()>,
    }

    impl Drop for TracingTestGuard {
        fn drop(&mut self) {
            tracing::callsite::rebuild_interest_cache();
        }
    }

    pub(crate) fn tracing_test_guard() -> TracingTestGuard {
        let guard = match TRACING_TEST_LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        tracing::callsite::rebuild_interest_cache();
        TracingTestGuard { _guard: guard }
    }

    pub(crate) fn capture_events(run: impl FnOnce()) -> CapturedEvents {
        let capture = TracingCapture::start();
        run();
        capture.captured.clone()
    }

    /// Keeps one thread-local tracing subscriber active for an async test.
    pub(crate) struct TracingCapture {
        // Restore the subscriber before rebuilding the global callsite cache.
        _subscriber_guard: tracing::subscriber::DefaultGuard,
        _test_guard: TracingTestGuard,
        captured: CapturedEvents,
    }

    impl TracingCapture {
        pub(crate) fn start() -> Self {
            Self::start_with_profile_spans(false)
        }

        pub(crate) fn start_with_profile_spans_enabled() -> Self {
            Self::start_with_profile_spans(true)
        }

        fn start_with_profile_spans(profile_spans_enabled: bool) -> Self {
            let test_guard = tracing_test_guard();
            // A no-layer global registry keeps callsites enabled when an
            // ordinary parallel test is the first thread to reach them.
            // Captures replace it only on their own serialized thread.
            TRACING_TEST_GLOBAL_SUBSCRIBER.call_once(|| {
                let _ = tracing::subscriber::set_global_default(Registry::default());
            });
            tracing::callsite::rebuild_interest_cache();
            let captured = CapturedEvents::default();
            let subscriber = Registry::default()
                .with(CaptureLayer::new(captured.clone(), profile_spans_enabled));
            let subscriber_guard = tracing::subscriber::set_default(subscriber);
            Self {
                _subscriber_guard: subscriber_guard,
                _test_guard: test_guard,
                captured,
            }
        }

        pub(crate) const fn captured(&self) -> &CapturedEvents {
            &self.captured
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub(crate) struct CapturedEvent {
        pub(crate) target: &'static str,
        pub(crate) level: Level,
        pub(crate) fields: BTreeMap<String, String>,
        pub(crate) span_names: Vec<&'static str>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub(crate) struct CapturedSpan {
        pub(crate) id: u64,
        pub(crate) parent_id: Option<u64>,
        pub(crate) target: &'static str,
        pub(crate) name: &'static str,
        pub(crate) level: Level,
        pub(crate) fields: BTreeMap<String, String>,
        pub(crate) enter_count: u64,
        pub(crate) exit_count: u64,
        pub(crate) closed: bool,
    }

    #[derive(Clone, Default)]
    pub(crate) struct CapturedEvents {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
        spans: Arc<Mutex<Vec<CapturedSpan>>>,
    }

    impl CapturedEvents {
        pub(crate) fn events(&self) -> Vec<CapturedEvent> {
            match self.events.lock() {
                Ok(events) => events.clone(),
                Err(_) => Vec::new(),
            }
        }

        pub(crate) fn spans(&self) -> Vec<CapturedSpan> {
            match self.spans.lock() {
                Ok(spans) => spans.clone(),
                Err(_) => Vec::new(),
            }
        }

        fn update_span(&self, id: &Id, update: impl FnOnce(&mut CapturedSpan)) {
            if let Ok(mut spans) = self.spans.lock()
                && let Some(span) = spans
                    .iter_mut()
                    .find(|span| span.id == id.clone().into_u64())
            {
                update(span);
            }
        }
    }

    struct CaptureLayer {
        events: CapturedEvents,
        profile_spans_enabled: bool,
    }

    impl CaptureLayer {
        fn new(events: CapturedEvents, profile_spans_enabled: bool) -> Self {
            Self {
                events,
                profile_spans_enabled,
            }
        }
    }

    impl<S> Layer<S> for CaptureLayer
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn register_callsite(
            &self,
            _metadata: &'static tracing::Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            tracing::subscriber::Interest::sometimes()
        }

        fn enabled(&self, metadata: &tracing::Metadata<'_>, _ctx: Context<'_, S>) -> bool {
            metadata.target() != crate::profiling::PROFILE_TARGET || self.profile_spans_enabled
        }

        fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
            let mut visitor = FieldVisitor::default();
            attrs.record(&mut visitor);
            let captured = CapturedSpan {
                id: id.clone().into_u64(),
                parent_id: ctx
                    .span(id)
                    .and_then(|span| span.parent())
                    .map(|parent| parent.id().clone().into_u64()),
                target: attrs.metadata().target(),
                name: attrs.metadata().name(),
                level: *attrs.metadata().level(),
                fields: visitor.fields,
                enter_count: 0,
                exit_count: 0,
                closed: false,
            };

            if let Ok(mut spans) = self.events.spans.lock() {
                spans.push(captured);
            }
        }

        fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
            self.events.update_span(id, |span| {
                let mut visitor = FieldVisitor {
                    fields: std::mem::take(&mut span.fields),
                };
                values.record(&mut visitor);
                span.fields = visitor.fields;
            });
        }

        fn on_enter(&self, id: &Id, _ctx: Context<'_, S>) {
            self.events.update_span(id, |span| {
                span.enter_count = span.enter_count.saturating_add(1);
            });
        }

        fn on_exit(&self, id: &Id, _ctx: Context<'_, S>) {
            self.events.update_span(id, |span| {
                span.exit_count = span.exit_count.saturating_add(1);
            });
        }

        fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
            self.events.update_span(&id, |span| span.closed = true);
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

#[cfg(test)]
mod tests {
    use super::test_capture::{
        CapturedEvent, CapturedEvents, CapturedSpan, TracingCapture, capture_events,
        tracing_test_guard,
    };
    use super::*;
    use crate::{
        QueryExecutionMetric, QueryExecutionMetricCategory, QueryExecutionOperatorProfile,
        QueryExecutionOutcome,
    };
    use futures_util::StreamExt;
    use tracing::Level;

    const PARQUET_IO_FIELDS: [&str; 4] = [
        "parquet_data_file_range_get_operations",
        "parquet_data_file_full_get_operations",
        "parquet_data_file_bytes_received",
        "parquet_data_file_opened_bytes",
    ];

    #[test]
    fn observability_event_helpers_do_not_require_capture_layer() -> Result<(), DeltaFunnelError> {
        let _guard = tracing_test_guard();
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
        delta_provider_parquet_io_summary(
            &provider_stats_snapshot(),
            DeltaProviderScanOutcome::Success,
        );
        query_execution_profile_terminal(&QueryExecutionProfile::mssql_output(
            QueryExecutionOutcome::Success,
            Vec::new(),
        ));
        source_loading_started("orders", None);
        source_loading_completed("orders", 7, None);
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
            DELTA_PROVIDER_PARQUET_IO_SUMMARY_EVENT,
            "delta_provider_parquet_io_summary"
        );
        assert_eq!(
            QUERY_EXECUTION_PROFILE_TERMINAL_EVENT,
            "query_execution_profile_terminal"
        );
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
        assert_eq!(DeltaProviderScanOutcome::Success.as_str(), "success");
        assert_eq!(DeltaProviderScanOutcome::Error.as_str(), "error");
        assert_eq!(DeltaProviderScanOutcome::Cancelled.as_str(), "cancelled");
    }

    #[test]
    fn terminal_profile_summary_selects_exact_bounded_fields_and_first_maximum() {
        let metric = |name, value| {
            QueryExecutionMetric::new(
                name,
                QueryExecutionMetricCategory::Summary,
                None,
                None,
                value,
            )
        };
        let operator = |node_id, name, metrics_available, aggregated_metrics| {
            QueryExecutionOperatorProfile::new(
                node_id,
                (node_id != 0).then_some(0),
                name,
                1,
                metrics_available,
                aggregated_metrics,
                Vec::new(),
                None,
            )
        };
        let profile = QueryExecutionProfile::preview(
            QueryExecutionOutcome::Error,
            20,
            vec![
                operator(
                    0,
                    "RootExec",
                    true,
                    vec![
                        metric("output_rows", QueryExecutionMetricValue::Count(42)),
                        metric(
                            "elapsed_compute",
                            QueryExecutionMetricValue::Nanoseconds(50),
                        ),
                    ],
                ),
                operator(1, "NoMetricsExec", false, Vec::new()),
                operator(
                    2,
                    "FirstSlowExec",
                    true,
                    vec![metric(
                        "elapsed_compute",
                        QueryExecutionMetricValue::Nanoseconds(100),
                    )],
                ),
                operator(
                    3,
                    "TiedSlowExec",
                    true,
                    vec![metric(
                        "elapsed_compute",
                        QueryExecutionMetricValue::Nanoseconds(100),
                    )],
                ),
            ],
        );

        let events = capture_events(|| query_execution_profile_terminal(&profile)).events();

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.target, TRACING_TARGET);
        assert_eq!(event.level, Level::DEBUG);
        let expected_fields = [
            "telemetry_event",
            "scope",
            "outcome",
            "partial",
            "delta_funnel_row_limit",
            "operator_count",
            "operators_with_metrics",
            "root_output_rows",
            "max_elapsed_compute_operator",
            "max_elapsed_compute_nanos",
        ];
        assert_eq!(event.fields.len(), expected_fields.len());
        assert!(
            expected_fields
                .iter()
                .all(|field| event.fields.contains_key(*field))
        );
        for (field, value) in [
            ("telemetry_event", "query_execution_profile_terminal"),
            ("scope", "preview"),
            ("outcome", "error"),
            ("partial", "true"),
            ("delta_funnel_row_limit", "20"),
            ("operator_count", "4"),
            ("operators_with_metrics", "3"),
            ("root_output_rows", "42"),
            ("max_elapsed_compute_operator", "FirstSlowExec"),
            ("max_elapsed_compute_nanos", "100"),
        ] {
            assert_eq!(event.fields.get(field).map(String::as_str), Some(value));
        }
    }

    #[test]
    fn terminal_profile_summary_omits_unavailable_optional_fields() {
        let profile = QueryExecutionProfile::mssql_output(
            QueryExecutionOutcome::Success,
            vec![QueryExecutionOperatorProfile::new(
                0,
                None,
                "RootExec",
                1,
                false,
                Vec::new(),
                Vec::new(),
                None,
            )],
        );

        let events = capture_events(|| query_execution_profile_terminal(&profile)).events();

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.fields.len(), 6);
        for absent in [
            "delta_funnel_row_limit",
            "root_output_rows",
            "max_elapsed_compute_operator",
            "max_elapsed_compute_nanos",
        ] {
            assert!(!event.fields.contains_key(absent));
        }
        for (field, value) in [
            ("telemetry_event", "query_execution_profile_terminal"),
            ("scope", "mssql_output"),
            ("outcome", "success"),
            ("partial", "false"),
            ("operator_count", "1"),
            ("operators_with_metrics", "0"),
        ] {
            assert_eq!(event.fields.get(field).map(String::as_str), Some(value));
        }
    }

    #[test]
    fn parquet_io_metric_classifier_distinguishes_complete_unavailable_and_mixed() {
        let available = provider_stats_snapshot();
        assert_eq!(
            parquet_io_metrics(&available),
            ParquetIoMetrics::Available {
                range_get_operations: 0,
                full_get_operations: 2,
                bytes_received: 512,
                opened_bytes: 2048,
            }
        );

        let mut unavailable = available.clone();
        unavailable.parquet_data_file_range_get_operations = None;
        unavailable.parquet_data_file_full_get_operations = None;
        unavailable.parquet_data_file_bytes_received = None;
        unavailable.parquet_data_file_opened_bytes = None;
        assert_eq!(
            parquet_io_metrics(&unavailable),
            ParquetIoMetrics::Unavailable
        );

        unavailable.parquet_data_file_range_get_operations = Some(0);
        assert_eq!(parquet_io_metrics(&unavailable), ParquetIoMetrics::Mixed);

        let mut mixed = available;
        mixed.parquet_data_file_bytes_received = None;
        assert_eq!(parquet_io_metrics(&mixed), ParquetIoMetrics::Mixed);
    }

    #[test]
    fn native_async_summary_records_typed_metrics_for_every_outcome() {
        let snapshot = provider_stats_snapshot();
        let events = capture_events(|| {
            for outcome in [
                DeltaProviderScanOutcome::Success,
                DeltaProviderScanOutcome::Error,
                DeltaProviderScanOutcome::Cancelled,
            ] {
                delta_provider_parquet_io_summary(&snapshot, outcome);
            }
        })
        .events();

        assert_eq!(events.len(), 3);
        for (event, outcome) in events.iter().zip(["success", "error", "cancelled"]) {
            assert_provider_io_event(event, outcome, "native_async", "true");
            assert_eq!(
                event.fields.get(PARQUET_IO_FIELDS[0]).map(String::as_str),
                Some("0")
            );
            assert_eq!(
                event.fields.get(PARQUET_IO_FIELDS[1]).map(String::as_str),
                Some("2")
            );
            assert_eq!(
                event.fields.get(PARQUET_IO_FIELDS[2]).map(String::as_str),
                Some("512")
            );
            assert_eq!(
                event.fields.get(PARQUET_IO_FIELDS[3]).map(String::as_str),
                Some("2048")
            );
        }
    }

    #[test]
    fn unavailable_and_mixed_summaries_emit_no_numeric_subset() {
        let mut unavailable = provider_stats_snapshot();
        unavailable.reader_backend = DeltaProviderReaderBackend::OfficialKernel;
        unavailable.parquet_data_file_range_get_operations = None;
        unavailable.parquet_data_file_full_get_operations = None;
        unavailable.parquet_data_file_bytes_received = None;
        unavailable.parquet_data_file_opened_bytes = None;
        let mut mixed = provider_stats_snapshot();
        mixed.parquet_data_file_bytes_received = None;

        let events = capture_events(|| {
            delta_provider_parquet_io_summary(&unavailable, DeltaProviderScanOutcome::Success);
            delta_provider_parquet_io_summary(&mixed, DeltaProviderScanOutcome::Error);
        })
        .events();

        assert_eq!(events.len(), 2);
        assert_provider_io_event(&events[0], "success", "official_kernel", "false");
        assert_provider_io_event(&events[1], "error", "native_async", "false");
        assert!(events.iter().all(|event| {
            PARQUET_IO_FIELDS
                .iter()
                .all(|field| !event.fields.contains_key(*field))
        }));
    }

    #[tokio::test]
    async fn merged_stream_emits_one_summary_for_success_error_and_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let capture = TracingCapture::start();

        let success_table =
            crate::table_formats::RealParquetDeltaTable::new_default("summary-success")?;
        let mut success_session = native_async_session(crate::QueryOptions::default())?;
        let success_source = success_session.delta_lake(crate::DeltaSourceConfig::new(
            "success_orders",
            success_table.path().to_string_lossy().to_string(),
        ))?;
        let mut success_stream = success_session
            .batch_stream_for_lazy_table(&success_source, None)
            .await?;
        while let Some(batch) = success_stream.next().await {
            batch?;
        }
        assert!(success_stream.next().await.is_none());
        drop(success_stream);

        let cancelled_table =
            crate::table_formats::RealParquetDeltaTable::new_with_two_large_files(
                "summary-cancelled",
                20_000,
            )?;
        let mut cancelled_session = native_async_session(crate::QueryOptions {
            target_partitions: Some(1),
            output_batch_size: Some(1),
        })?;
        let cancelled_source = cancelled_session.delta_lake(crate::DeltaSourceConfig::new(
            "cancelled_orders",
            cancelled_table.path().to_string_lossy().to_string(),
        ))?;
        let mut cancelled_stream = cancelled_session
            .batch_stream_for_lazy_table(&cancelled_source, None)
            .await?;
        assert!(cancelled_stream.next().await.transpose()?.is_some());
        drop(cancelled_stream);

        let error_table =
            crate::query_engine::datafusion::test_support::DeltaLogTable::new("summary-error")?;
        let mut error_session = native_async_session(crate::QueryOptions::default())?;
        let error_source = error_session.delta_lake(crate::DeltaSourceConfig::new(
            "error_orders",
            error_table.path().to_string_lossy().to_string(),
        ))?;
        let mut error_stream = error_session
            .batch_stream_for_lazy_table(&error_source, None)
            .await?;
        let error = error_stream.next().await.ok_or("expected upstream error")?;
        assert!(error.is_err());
        drop(error_stream);
        let summaries = provider_io_events(capture.captured());
        assert_eq!(summaries.len(), 3);
        for (event, (source_name, outcome)) in summaries.iter().zip([
            ("success_orders", "success"),
            ("cancelled_orders", "cancelled"),
            ("error_orders", "error"),
        ]) {
            assert_eq!(event.target, TRACING_TARGET);
            assert_eq!(event.level, Level::DEBUG);
            assert_eq!(
                event.fields.get("source_name").map(String::as_str),
                Some(source_name)
            );
            assert_eq!(
                event.fields.get("outcome").map(String::as_str),
                Some(outcome)
            );
            assert_eq!(
                event.fields.get("metrics_available").map(String::as_str),
                Some("true")
            );
            assert!(
                PARQUET_IO_FIELDS
                    .iter()
                    .all(|field| event.fields.contains_key(*field))
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn partitioned_cache_emits_the_same_summary_with_and_without_progress()
    -> Result<(), Box<dyn std::error::Error>> {
        let capture = TracingCapture::start();

        let reporter = crate::progress::ProgressReporter::default();
        materialize_cache_for_trace("cache-no-progress", "cache_no_progress", None).await?;
        materialize_cache_for_trace("cache-progress", "cache_progress", Some(&reporter)).await?;
        let summaries = provider_io_events(capture.captured());
        assert_eq!(summaries.len(), 2);
        for (event, source_name) in summaries
            .iter()
            .zip(["cache_no_progress", "cache_progress"])
        {
            assert_eq!(event.target, TRACING_TARGET);
            assert_eq!(event.level, Level::DEBUG);
            assert_eq!(
                event.fields.get("source_name").map(String::as_str),
                Some(source_name)
            );
            assert_eq!(
                event.fields.get("outcome").map(String::as_str),
                Some("success")
            );
            assert_eq!(
                event.fields.get("metrics_available").map(String::as_str),
                Some("true")
            );
            assert!(
                PARQUET_IO_FIELDS
                    .iter()
                    .all(|field| event.fields.contains_key(*field))
            );
        }
        for field in PARQUET_IO_FIELDS {
            assert_eq!(
                summaries[0].fields.get(field),
                summaries[1].fields.get(field)
            );
        }
        Ok(())
    }

    #[test]
    fn provider_io_summary_sanitizes_hostile_source_names() {
        let mut uri = provider_stats_snapshot();
        uri.source_name = "s3://user:password@example.com/table?token=secret#debug".to_owned();
        let mut control = provider_stats_snapshot();
        control.source_name = "orders\nsource".to_owned();

        let events = capture_events(|| {
            delta_provider_parquet_io_summary(&uri, DeltaProviderScanOutcome::Success);
            delta_provider_parquet_io_summary(&control, DeltaProviderScanOutcome::Success);
        })
        .events();

        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].fields.get("source_name").map(String::as_str),
            Some("s3://example.com/table")
        );
        assert_eq!(
            events[1].fields.get("source_name").map(String::as_str),
            Some("orders\\nsource")
        );
        assert_no_forbidden_tracing_text(&events);
    }

    #[test]
    fn scoped_capture_records_workflow_event_fields() {
        let events = capture_events(|| {
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
        let events = capture_events(|| {
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
    fn progress_dry_run_all_keeps_workflow_observability() -> Result<(), DeltaFunnelError> {
        let runtime = crate::DeltaFunnelRuntime::new()?;
        let session = crate::DeltaFunnelSession::new(crate::SessionOptions::default())?;
        let reporter = crate::progress::ProgressReporter::new(|_| {});

        let captured = capture_events(|| {
            let result = runtime.dry_run_all_to_mssql_with_progress(&session, &[], reporter);
            assert!(result.is_ok());
        });

        let spans = captured.spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, WORKFLOW_SPAN);
        assert_eq!(
            spans[0].fields.get("run_mode").map(String::as_str),
            Some("dry_run")
        );
        assert_eq!(
            spans[0].fields.get("output_count").map(String::as_str),
            Some("0")
        );

        let events = captured.events();
        assert_eq!(events.len(), 2);
        assert_workflow_event(&events[0], WORKFLOW_STARTED_EVENT, RunMode::DryRun, "0");
        assert_workflow_event(&events[1], WORKFLOW_COMPLETED_EVENT, RunMode::DryRun, "0");
        assert!(
            events
                .iter()
                .all(|event| event.span_names == [WORKFLOW_SPAN])
        );
        Ok(())
    }

    #[test]
    fn scoped_capture_records_output_event_fields() -> Result<(), DeltaFunnelError> {
        let target_table = MssqlTargetTable::new("dbo", "orders")?;

        let events = capture_events(|| {
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
        let target_table = MssqlTargetTable::new("dbo", "orders")?;

        let events = capture_events(|| {
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
        let target_table = MssqlTargetTable::new("dbo", "orders")?;

        let events = capture_events(|| {
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
        let target_table = MssqlTargetTable::new("dbo", "orders")?;

        let events = capture_events(|| {
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
        let events = capture_events(|| {
            source_loading_started("orders", None);
            source_loading_completed("orders", 7, None);
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
    fn scoped_capture_records_source_loading_s3_auth_mode_hint() {
        let events = capture_events(|| {
            source_loading_started("orders", Some("implicit_provider_chain"));
            source_loading_completed("orders", 7, Some("implicit_provider_chain"));
            source_loading_failed(
                "orders",
                &DeltaFunnelError::DeltaSnapshotLoad {
                    reason: concat!(
                        "snapshot could not be loaded: missing credentials. ",
                        "S3 credential hint: no explicit S3 credentials were supplied through ",
                        "storage_options; local shells may need explicit AWS_ACCESS_KEY_ID, ",
                        "AWS_SECRET_ACCESS_KEY, optional AWS_SESSION_TOKEN, and AWS_REGION."
                    )
                    .to_owned(),
                },
                Some("implicit_provider_chain"),
            );
        });

        let events = events.events();
        assert_eq!(events.len(), 3);
        for event in &events {
            assert_eq!(
                event.fields.get("derived_s3_auth_mode").map(String::as_str),
                Some("implicit_provider_chain")
            );
        }
        assert_no_forbidden_tracing_text(&events);
    }

    #[test]
    fn scoped_capture_records_validation_event_fields() -> Result<(), DeltaFunnelError> {
        let target_table = MssqlTargetTable::new("dbo", "orders")?;

        let events = capture_events(|| {
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

    #[test]
    fn scoped_capture_redacts_sensitive_error_summary_text() -> Result<(), DeltaFunnelError> {
        let target_table = MssqlTargetTable::new("dbo", "orders")?;

        let events = capture_events(|| {
            workflow_finished::<()>(
                RunMode::Execute,
                1,
                &Err(DeltaFunnelError::Config {
                    message: concat!(
                        "connection server=tcp:sql.example.com;user=admin;password=secret-token ",
                        "uses access_key=AKIASECRET"
                    )
                    .to_owned(),
                }),
            );
            output_failed(
                "orders_output",
                &target_table,
                LoadMode::CreateAndLoad,
                "select * from dbo.customers where email = 'alice@example.com'",
            );
            source_loading_failed(
                "orders",
                &DeltaFunnelError::DataFusionRegistration {
                    source_name: "orders".to_owned(),
                    table_uri: "s3://user:password@example.com/table?token=secret".to_owned(),
                    reason: "credential token=secret".to_owned(),
                },
                None,
            );
        });

        let events = events.events();
        assert_eq!(events.len(), 3);
        assert_no_forbidden_tracing_text(&events);
        for event in events {
            assert_eq!(
                event.fields.get("error_category").map(String::as_str),
                Some("delta_funnel_error")
            );
            assert!(event.fields.contains_key("error_summary"));
        }

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

    fn assert_provider_io_event(
        event: &CapturedEvent,
        outcome: &str,
        reader_backend: &str,
        metrics_available: &str,
    ) {
        assert_eq!(event.target, TRACING_TARGET);
        assert_eq!(event.level, Level::DEBUG);
        assert_eq!(
            event.fields.get("telemetry_event").map(String::as_str),
            Some(DELTA_PROVIDER_PARQUET_IO_SUMMARY_EVENT)
        );
        assert_eq!(
            event.fields.get("source_name").map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            event.fields.get("snapshot_version").map(String::as_str),
            Some("3")
        );
        assert_eq!(
            event.fields.get("reader_backend").map(String::as_str),
            Some(reader_backend)
        );
        assert_eq!(
            event.fields.get("outcome").map(String::as_str),
            Some(outcome)
        );
        assert_eq!(
            event.fields.get("metrics_available").map(String::as_str),
            Some(metrics_available)
        );
    }

    fn provider_io_events(events: &CapturedEvents) -> Vec<CapturedEvent> {
        events
            .events()
            .into_iter()
            .filter(|event| {
                event.fields.get("telemetry_event").map(String::as_str)
                    == Some(DELTA_PROVIDER_PARQUET_IO_SUMMARY_EVENT)
            })
            .collect()
    }

    async fn materialize_cache_for_trace(
        fixture_name: &str,
        source_name: &str,
        reporter: Option<&crate::progress::ProgressReporter>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let table = crate::table_formats::RealParquetDeltaTable::new_default(fixture_name)?;
        let mut session = native_async_session(crate::QueryOptions::default())?;
        session.delta_lake(crate::DeltaSourceConfig::new(
            source_name,
            table.path().to_string_lossy().to_string(),
        ))?;
        let sql = format!("select * from {source_name}");
        let query = session.table_from_sql(&sql).await?;
        let alias_name = format!("cached_{source_name}");
        let alias = session.register_alias(&alias_name, &query)?;

        session
            .replace_registered_derived_alias_with_cache(&alias, reporter)
            .await?
            .restore()?;
        Ok(())
    }

    fn provider_stats_snapshot() -> DeltaProviderReadStatsSnapshot {
        DeltaProviderReadStatsSnapshot {
            source_name: "orders".to_owned(),
            snapshot_version: 3,
            reader_backend: DeltaProviderReaderBackend::NativeAsync,
            scan_metadata_exhausted: Some(true),
            scan_partitions_planned: 1,
            files_planned: 2,
            files_filtered_during_planning: Some(4),
            estimated_rows: Some(10),
            estimated_bytes: Some(2048),
            parquet_data_file_range_get_operations: Some(0),
            parquet_data_file_full_get_operations: Some(2),
            parquet_data_file_bytes_received: Some(512),
            parquet_data_file_opened_bytes: Some(2048),
            datafusion_output_batch_size: Some(8192),
            scan_partitions_started: 1,
            scan_partitions_completed: 1,
            files_started: 2,
            files_completed: 2,
            dynamic_partition_files_pruned: 0,
            dynamic_partition_files_kept: 2,
            dynamic_filters_received: 0,
            dynamic_filters_accepted: 0,
            dynamic_filters_unsupported: 0,
            dynamic_filter_snapshots: 0,
            dynamic_partition_files_not_pruned_missing_metadata: 0,
            dynamic_partition_files_not_pruned_unsupported_expression: 0,
            batches_produced: 1,
            rows_produced: 10,
            deletion_vector_payloads_loaded: 0,
            deletion_vectors_applied: 0,
            deletion_vector_rows_deleted: 0,
            deletion_vector_failures: 0,
            deletion_vector_rejections: 0,
        }
    }

    fn native_async_session(
        query_options: crate::QueryOptions,
    ) -> Result<crate::DeltaFunnelSession, DeltaFunnelError> {
        let provider_scan_options =
            crate::DeltaProviderScanExecutionOptions::try_new_with_reader_backend(
                DeltaProviderReaderBackend::NativeAsync,
                1,
                1,
            )?;
        crate::DeltaFunnelSession::new(
            crate::SessionOptions::new()
                .with_query_options(query_options)
                .with_provider_scan_options(provider_scan_options),
        )
    }

    fn assert_no_forbidden_tracing_text(events: &[CapturedEvent]) {
        let forbidden = [
            "AKIASECRET",
            "access_key",
            "alice@example.com",
            "password",
            "secret",
            "select *",
            "token=",
            "user:password",
        ];

        for event in events {
            for value in event.fields.values() {
                for forbidden_text in forbidden {
                    assert!(
                        !value.contains(forbidden_text),
                        "tracing field leaked forbidden text `{forbidden_text}` in `{value}`"
                    );
                }
            }
        }
    }
}
