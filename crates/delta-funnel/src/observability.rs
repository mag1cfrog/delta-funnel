//! Internal tracing vocabulary for DeltaFunnel workflow observability.

use crate::{DeltaFunnelError, LoadMode, MssqlTargetTable, RunMode};

pub(crate) const TRACING_TARGET: &str = "delta_funnel";

const OUTPUT_COMPLETED_EVENT: &str = "output.completed";
const OUTPUT_FAILED_EVENT: &str = "output.failed";
const OUTPUT_SKIPPED_EVENT: &str = "output.skipped";
const OUTPUT_STARTED_EVENT: &str = "output.started";
const WORKFLOW_COMPLETED_EVENT: &str = "workflow.completed";
const WORKFLOW_FAILED_EVENT: &str = "workflow.failed";
const WORKFLOW_STARTED_EVENT: &str = "workflow.started";

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

impl RunMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Execute => "execute",
            Self::DryRun => "dry_run",
        }
    }
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
    };
    use tracing_subscriber::{Layer, Registry, layer::Context, prelude::*};

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
        Ok(())
    }

    #[test]
    fn observability_uses_stable_workflow_vocabulary() {
        assert_eq!(TRACING_TARGET, "delta_funnel");
        assert_eq!(WORKFLOW_COMPLETED_EVENT, "workflow.completed");
        assert_eq!(WORKFLOW_FAILED_EVENT, "workflow.failed");
        assert_eq!(WORKFLOW_STARTED_EVENT, "workflow.started");
        assert_eq!(OUTPUT_COMPLETED_EVENT, "output.completed");
        assert_eq!(OUTPUT_FAILED_EVENT, "output.failed");
        assert_eq!(OUTPUT_SKIPPED_EVENT, "output.skipped");
        assert_eq!(OUTPUT_STARTED_EVENT, "output.started");
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

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct CapturedEvent {
        target: &'static str,
        level: Level,
        fields: BTreeMap<String, String>,
    }

    #[derive(Clone, Default)]
    struct CapturedEvents {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
    }

    impl CapturedEvents {
        fn events(&self) -> Vec<CapturedEvent> {
            match self.events.lock() {
                Ok(events) => events.clone(),
                Err(_) => Vec::new(),
            }
        }
    }

    struct CaptureLayer {
        events: CapturedEvents,
    }

    impl<S> Layer<S> for CaptureLayer
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = FieldVisitor::default();
            event.record(&mut visitor);
            let captured = CapturedEvent {
                target: event.metadata().target(),
                level: *event.metadata().level(),
                fields: visitor.fields,
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
