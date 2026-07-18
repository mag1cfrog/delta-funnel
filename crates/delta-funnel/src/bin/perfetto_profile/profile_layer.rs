//! Direct adapter from Delta Funnel profiling spans to Perfetto Track Events.

use std::ffi::CString;

use perfetto_sdk::track_event::{
    EventContext, TrackEventDebugArg, TrackEventTrack, TrackEventType,
};
use perfetto_sdk::{track_event, track_event_begin, track_event_end, track_event_instant};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

use super::{
    SemanticTrack, diagnostics_track, operation_track, perfetto_te_ns, phase_track, query_track,
    worker_track,
};

pub(crate) const PROFILE_TARGET: &str = "delta_funnel::profile";

#[derive(Debug, Default)]
pub(crate) struct PerfettoProfileLayer;

impl<S> Layer<S> for PerfettoProfileLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        let mut fields = ProfileFields::default();
        attributes.record(&mut fields);
        let Some(active) = ActiveProfileSpan::from_fields(attributes.metadata().name(), fields)
        else {
            return;
        };
        let Some(span) = context.span(id) else {
            return;
        };
        active.emit_begin();
        span.extensions_mut().insert(active);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, context: Context<'_, S>) {
        let Some(span) = context.span(id) else {
            return;
        };
        let mut fields = ProfileFields::default();
        values.record(&mut fields);
        if let Some(active) = span.extensions_mut().get_mut::<ActiveProfileSpan>()
            && fields.result.is_some()
        {
            active.result = fields.result;
        }
    }

    fn on_close(&self, id: Id, context: Context<'_, S>) {
        let Some(span) = context.span(&id) else {
            return;
        };
        if let Some(active) = span.extensions_mut().remove::<ActiveProfileSpan>() {
            active.emit_end();
        }
    }

    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        if event.metadata().name() != "Operator activity trace truncated" {
            return;
        }
        let mut fields = ProfileFields::default();
        event.record(&mut fields);
        let (Some(operation_id), Some(maximum_spans)) = (fields.operation_id, fields.maximum_spans)
        else {
            return;
        };
        let diagnostics = diagnostics_track(TrackEventTrack::process_track_uuid());
        let operation = operation_track(operation_id, diagnostics.uuid);
        track_event_instant!(
            "delta_funnel.perfetto_spike",
            "Operator activity trace truncated",
            |context: &mut EventContext| {
                operation.set_on(context);
                context.add_debug_arg("maximum_spans", TrackEventDebugArg::Uint64(maximum_spans));
            }
        );
    }
}

#[derive(Debug, Default)]
struct ProfileFields {
    operation_id: Option<u64>,
    query_execution_id: Option<u64>,
    worker_lane_id: Option<u64>,
    maximum_spans: Option<u64>,
    operator_name: Option<String>,
    phase: Option<String>,
    result: Option<String>,
}

impl Visit for ProfileFields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "operation_id" => self.operation_id = Some(value),
            "query_execution_id" => self.query_execution_id = Some(value),
            "worker_lane_id" => self.worker_lane_id = Some(value),
            "maximum_spans" => self.maximum_spans = Some(value),
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "operator_name" => self.operator_name = Some(value.to_owned()),
            "phase" => self.phase = Some(value.to_owned()),
            "result" => self.result = Some(value.to_owned()),
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

#[derive(Debug)]
struct ActiveProfileSpan {
    event: ProfileEvent,
    operation_id: u64,
    query_execution_id: Option<u64>,
    worker_lane_id: Option<u64>,
    result: Option<String>,
}

impl ActiveProfileSpan {
    fn from_fields(name: &str, fields: ProfileFields) -> Option<Self> {
        let operation_id = fields.operation_id?;
        let diagnostics = diagnostics_track(TrackEventTrack::process_track_uuid());
        let operation = operation_track(operation_id, diagnostics.uuid);
        let event = match name {
            "Delta Funnel preview" => ProfileEvent::Operation {
                kind: OperationKind::Preview,
                diagnostics,
                operation,
            },
            "Delta Funnel SQL Server write" => ProfileEvent::Operation {
                kind: OperationKind::MssqlWrite,
                diagnostics,
                operation,
            },
            "Delta Funnel SQL Server write_all" => ProfileEvent::Operation {
                kind: OperationKind::WriteAll,
                diagnostics,
                operation,
            },
            "DataFusion query planning" => {
                let query_execution_id = fields.query_execution_id?;
                ProfileEvent::Planning {
                    phases: phase_track(operation_id, operation.uuid),
                    query: query_track(operation_id, query_execution_id, operation.uuid),
                }
            }
            "Delta Funnel operation phase" => ProfileEvent::Phase {
                kind: OperationPhaseKind::from_str(fields.phase.as_deref()?)?,
                phases: phase_track(operation_id, operation.uuid),
            },
            "DataFusion operator poll" => {
                let query_execution_id = fields.query_execution_id?;
                let worker_lane_id = fields.worker_lane_id?;
                let query = query_track(operation_id, query_execution_id, operation.uuid);
                ProfileEvent::Operator {
                    name: fields.operator_name.clone()?,
                    worker: worker_track(
                        operation_id,
                        query_execution_id,
                        worker_lane_id,
                        query.uuid,
                        worker_lane_id,
                    ),
                }
            }
            _ => return None,
        };
        Some(Self {
            event,
            operation_id,
            query_execution_id: fields.query_execution_id,
            worker_lane_id: fields.worker_lane_id,
            result: fields.result,
        })
    }

    fn emit_begin(&self) {
        match &self.event {
            ProfileEvent::Operation {
                kind,
                diagnostics,
                operation,
            } => {
                track_event_instant!(
                    "delta_funnel.perfetto_spike",
                    "Delta Funnel diagnostic group",
                    |context: &mut EventContext| diagnostics.set_on(context)
                );
                match kind {
                    OperationKind::Preview => track_event_begin!(
                        "delta_funnel.perfetto_spike",
                        "Delta Funnel preview",
                        |context: &mut EventContext| self.set_operation_on(context, operation)
                    ),
                    OperationKind::MssqlWrite => track_event_begin!(
                        "delta_funnel.perfetto_spike",
                        "Delta Funnel SQL Server write",
                        |context: &mut EventContext| self.set_operation_on(context, operation)
                    ),
                    OperationKind::WriteAll => track_event_begin!(
                        "delta_funnel.perfetto_spike",
                        "Delta Funnel SQL Server write_all",
                        |context: &mut EventContext| self.set_operation_on(context, operation)
                    ),
                }
            }
            ProfileEvent::Planning { phases, query } => {
                track_event_instant!(
                    "delta_funnel.perfetto_spike",
                    "DataFusion query",
                    |context: &mut EventContext| {
                        query.set_on(context);
                        self.add_identity_args(context);
                    }
                );
                track_event_begin!(
                    "delta_funnel.perfetto_spike",
                    "DataFusion query planning",
                    |context: &mut EventContext| {
                        phases.set_on(context);
                        self.add_identity_args(context);
                    }
                );
            }
            ProfileEvent::Phase { kind, phases } => match kind {
                OperationPhaseKind::Planning => track_event_begin!(
                    "delta_funnel.perfetto_spike",
                    "Planning",
                    |context: &mut EventContext| {
                        phases.set_on(context);
                        self.add_identity_args(context);
                    }
                ),
                OperationPhaseKind::Execution => track_event_begin!(
                    "delta_funnel.perfetto_spike",
                    "Execution",
                    |context: &mut EventContext| {
                        phases.set_on(context);
                        self.add_identity_args(context);
                    }
                ),
                OperationPhaseKind::Finalization => track_event_begin!(
                    "delta_funnel.perfetto_spike",
                    "Finalization",
                    |context: &mut EventContext| {
                        phases.set_on(context);
                        self.add_identity_args(context);
                    }
                ),
            },
            ProfileEvent::Operator { name, worker } => {
                if let Ok(name) = CString::new(name.as_str()) {
                    track_event!(
                        "delta_funnel.perfetto_spike",
                        TrackEventType::SliceBegin(name.as_ptr()),
                        |context: &mut EventContext| {
                            worker.set_on(context);
                            self.add_identity_args(context);
                        }
                    );
                } else {
                    track_event_begin!(
                        "delta_funnel.perfetto_spike",
                        "DataFusion operator",
                        |context: &mut EventContext| {
                            worker.set_on(context);
                            self.add_identity_args(context);
                        }
                    );
                }
            }
        }
    }

    fn emit_end(self) {
        let (track, flush) = match &self.event {
            ProfileEvent::Operation { operation, .. } => (operation, true),
            ProfileEvent::Planning { phases, .. } => (phases, false),
            ProfileEvent::Phase { phases, .. } => (phases, false),
            ProfileEvent::Operator { worker, .. } => (worker, false),
        };
        track_event_end!(
            "delta_funnel.perfetto_spike",
            |context: &mut EventContext| {
                track.set_on(context);
                self.add_identity_args(context);
                if let Some(result) = &self.result {
                    context.add_debug_arg("result", TrackEventDebugArg::String(result));
                }
                if flush {
                    context.set_flush();
                }
            }
        );
    }

    fn set_operation_on(&self, context: &mut EventContext, operation: &SemanticTrack) {
        operation.set_on(context);
        self.add_identity_args(context);
    }

    fn add_identity_args(&self, context: &mut EventContext) {
        context.add_debug_arg(
            "operation_id",
            TrackEventDebugArg::Uint64(self.operation_id),
        );
        if let Some(query_execution_id) = self.query_execution_id {
            context.add_debug_arg(
                "query_execution_id",
                TrackEventDebugArg::Uint64(query_execution_id),
            );
        }
        if let Some(worker_lane_id) = self.worker_lane_id {
            context.add_debug_arg("worker_lane_id", TrackEventDebugArg::Uint64(worker_lane_id));
        }
    }

    #[cfg(test)]
    fn track(&self) -> &SemanticTrack {
        match &self.event {
            ProfileEvent::Operation { operation, .. } => operation,
            ProfileEvent::Planning { phases, .. } => phases,
            ProfileEvent::Phase { phases, .. } => phases,
            ProfileEvent::Operator { worker, .. } => worker,
        }
    }
}

#[derive(Debug)]
enum ProfileEvent {
    Operation {
        kind: OperationKind,
        diagnostics: SemanticTrack,
        operation: SemanticTrack,
    },
    Planning {
        phases: SemanticTrack,
        query: SemanticTrack,
    },
    Phase {
        kind: OperationPhaseKind,
        phases: SemanticTrack,
    },
    Operator {
        name: String,
        worker: SemanticTrack,
    },
}

#[derive(Debug)]
enum OperationKind {
    Preview,
    MssqlWrite,
    WriteAll,
}

#[derive(Debug)]
enum OperationPhaseKind {
    Planning,
    Execution,
    Finalization,
}

impl OperationPhaseKind {
    fn from_str(value: &str) -> Option<Self> {
        match value {
            "planning" => Some(Self::Planning),
            "execution" => Some(Self::Execution),
            "finalization" => Some(Self::Finalization),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tracing_subscriber::{filter::filter_fn, prelude::*};

    use super::*;

    fn fields(operation_id: u64, query_execution_id: u64, worker_lane_id: u64) -> ProfileFields {
        ProfileFields {
            operation_id: Some(operation_id),
            query_execution_id: Some(query_execution_id),
            worker_lane_id: Some(worker_lane_id),
            operator_name: Some("FilterExec".to_owned()),
            ..ProfileFields::default()
        }
    }

    #[test]
    fn canonical_fields_map_to_deterministic_exact_worker_tracks() {
        let first = ActiveProfileSpan::from_fields("DataFusion operator poll", fields(1, 1, 1))
            .expect("complete operator identity should map");
        let duplicate = ActiveProfileSpan::from_fields("DataFusion operator poll", fields(1, 1, 1))
            .expect("the same operator identity should map");
        let worker_10 =
            ActiveProfileSpan::from_fields("DataFusion operator poll", fields(1, 1, 10))
                .expect("a second worker should map");

        assert_eq!(first.track(), duplicate.track());
        assert_ne!(first.track().uuid, worker_10.track().uuid);
        assert!(
            first
                .track()
                .name
                .contains("worker [w-00000000000000000001]")
        );
        assert!(
            !worker_10
                .track()
                .name
                .contains("worker [w-00000000000000000001]")
        );
    }

    #[test]
    fn incomplete_or_unknown_spans_are_ignored() {
        assert!(
            ActiveProfileSpan::from_fields("DataFusion operator poll", ProfileFields::default())
                .is_none()
        );
        assert!(ActiveProfileSpan::from_fields("application span", fields(1, 1, 1)).is_none());
    }

    #[test]
    fn operation_phases_share_one_deterministic_track() {
        let phase = |value: &str| ProfileFields {
            operation_id: Some(7),
            phase: Some(value.to_owned()),
            ..ProfileFields::default()
        };
        let planning =
            ActiveProfileSpan::from_fields("Delta Funnel operation phase", phase("planning"))
                .expect("a known phase should map");
        let execution =
            ActiveProfileSpan::from_fields("Delta Funnel operation phase", phase("execution"))
                .expect("a known phase should map");
        let finalization =
            ActiveProfileSpan::from_fields("Delta Funnel operation phase", phase("finalization"))
                .expect("a known phase should map");

        assert_eq!(planning.track(), execution.track());
        assert_eq!(execution.track(), finalization.track());
        assert!(
            ActiveProfileSpan::from_fields("Delta Funnel operation phase", phase("unknown"))
                .is_none()
        );
        assert!(
            ActiveProfileSpan::from_fields(
                "Delta Funnel operation phase",
                ProfileFields {
                    operation_id: Some(7),
                    ..ProfileFields::default()
                }
            )
            .is_none()
        );
    }

    #[derive(Clone)]
    struct EventCounter(Arc<AtomicUsize>);

    impl<S: Subscriber> Layer<S> for EventCounter {
        fn on_event(&self, _event: &Event<'_>, _context: Context<'_, S>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn profile_filter_does_not_hide_events_from_other_layers() {
        let count = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry()
            .with(EventCounter(Arc::clone(&count)))
            .with(
                PerfettoProfileLayer
                    .with_filter(filter_fn(|metadata| metadata.target() == PROFILE_TARGET)),
            );

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "application", "visible to the application layer");
        });

        assert_eq!(count.load(Ordering::Relaxed), 1);
    }
}
