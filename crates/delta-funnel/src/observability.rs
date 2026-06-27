//! Internal tracing vocabulary for DeltaFunnel workflow observability.

use crate::{DeltaFunnelError, RunMode};

pub(crate) const TRACING_TARGET: &str = "delta_funnel";

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

impl RunMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Execute => "execute",
            Self::DryRun => "dry_run",
        }
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
    fn observability_event_helpers_do_not_require_subscriber() {
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
    }

    #[test]
    fn observability_uses_stable_workflow_vocabulary() {
        assert_eq!(TRACING_TARGET, "delta_funnel");
        assert_eq!(WORKFLOW_COMPLETED_EVENT, "workflow.completed");
        assert_eq!(WORKFLOW_FAILED_EVENT, "workflow.failed");
        assert_eq!(WORKFLOW_STARTED_EVENT, "workflow.started");
        assert_eq!(RunMode::Execute.as_str(), "execute");
        assert_eq!(RunMode::DryRun.as_str(), "dry_run");
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
