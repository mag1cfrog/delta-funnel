//! Nested wall-clock activity for DataFusion physical planning.

use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use tracing::Instrument;

use crate::profiling::{OperationStageTrace, PROFILE_TARGET};

use super::QueryTraceIdentity;

#[derive(Clone)]
struct PlanningActivityContext {
    identity: QueryTraceIdentity,
    process_parent: tracing::Span,
    active_spans: Arc<Mutex<Vec<ActivePlanningSpan>>>,
}

#[derive(Clone)]
struct ActivePlanningSpan {
    process_span: tracing::Span,
}

impl ActivePlanningSpan {
    fn key(&self) -> Option<u64> {
        self.process_span.id().map(|id| id.into_u64())
    }
}

tokio::task_local! {
    static PLANNING_ACTIVITY: PlanningActivityContext;
}

pub(crate) async fn with_query_planning_activity<F, T, E>(
    identity: QueryTraceIdentity,
    future: F,
) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let process_span = process_query_planning_span(&identity);
    let process_parent = process_span.span.clone();
    let planning = PLANNING_ACTIVITY.scope(
        PlanningActivityContext {
            identity,
            process_parent,
            active_spans: Arc::new(Mutex::new(Vec::new())),
        },
        future,
    );
    let result = planning.instrument(process_span.span.clone()).await;
    process_span.finish(if result.is_ok() { "ok" } else { "error" });
    result
}

fn process_query_planning_span(identity: &QueryTraceIdentity) -> ProcessPlanningSpan {
    let parent = identity.process_root_span();
    let span = tracing::trace_span!(
        target: crate::profiling::PROFILE_TARGET,
        parent: parent,
        "DataFusion query planning",
        operation_id = identity.operation_id(),
        query_execution_id = identity.query_execution_id(),
        query_scope = identity.query_scope().as_str(),
        query_owner = identity.query_owner(),
        result = tracing::field::Empty,
        time_semantics = "wall_clock",
    );
    ProcessPlanningSpan {
        span,
        result_recorded: false,
    }
}

struct ProcessPlanningSpan {
    span: tracing::Span,
    result_recorded: bool,
}

impl ProcessPlanningSpan {
    fn finish(mut self, result: &'static str) {
        self.span.record("result", result);
        self.result_recorded = true;
    }
}

impl Drop for ProcessPlanningSpan {
    fn drop(&mut self) {
        if !self.result_recorded {
            self.span.record("result", "cancelled");
        }
    }
}

pub(crate) fn profile_query_planning_sync_result<T, E>(
    name: &'static str,
    activity: &'static str,
    operation: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let context = PLANNING_ACTIVITY.try_with(Clone::clone).ok();
    let span = context
        .as_ref()
        .map(|context| context.start_span(name, activity));
    let result = match &span {
        Some(span) => span.in_process_scope(operation),
        None => operation(),
    };
    if let Some(span) = span {
        if result.is_err() {
            span.failed();
        } else {
            span.completed();
        }
    }
    result
}

impl PlanningActivityContext {
    fn start_span(
        &self,
        name: &'static str,
        activity: &'static str,
    ) -> PlanningActivitySpanRecorder {
        let process_parent = self
            .active_spans
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .last()
            .map_or_else(
                || self.process_parent.clone(),
                |parent| parent.process_span.clone(),
            );
        let span = tracing::trace_span!(
            target: PROFILE_TARGET,
            parent: &process_parent,
            "DataFusion planning activity",
            operation_id = self.identity.operation_id(),
            query_execution_id = self.identity.query_execution_id(),
            query_scope = self.identity.query_scope().as_str(),
            query_owner = self.identity.query_owner(),
            planning_activity_name = name,
            activity,
            result = tracing::field::Empty,
            time_semantics = "wall_clock",
        );
        let active_span = ActivePlanningSpan {
            process_span: span.clone(),
        };
        let key = active_span.key();
        let stage = OperationStageTrace::from_process_span((span, process_parent));
        self.active_spans
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(active_span);
        PlanningActivitySpanRecorder {
            stage: Some(stage),
            context: self.clone(),
            key,
        }
    }
}

struct PlanningActivitySpanRecorder {
    stage: Option<OperationStageTrace>,
    context: PlanningActivityContext,
    key: Option<u64>,
}

impl PlanningActivitySpanRecorder {
    fn in_process_scope<T>(&self, operation: impl FnOnce() -> T) -> T {
        match &self.stage {
            Some(stage) => stage.in_process_scope(operation),
            None => operation(),
        }
    }

    fn completed(mut self) {
        if let Some(span) = self.stage.take() {
            span.completed();
        }
    }

    fn failed(mut self) {
        if let Some(span) = self.stage.take() {
            span.failed();
        }
    }
}

impl Drop for PlanningActivitySpanRecorder {
    fn drop(&mut self) {
        let popped = self
            .context
            .active_spans
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pop();
        debug_assert_eq!(popped.as_ref().map(ActivePlanningSpan::key), Some(self.key));
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        QueryExecutionScope, observability::test_capture::TracingCapture,
        profiling::OperationTraceContext,
    };

    use super::*;

    #[tokio::test]
    async fn planning_activity_records_nested_process_spans_and_results() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let context =
            OperationTraceContext::start_for_test(true).expect("profile tracing should start");
        let identity = QueryTraceIdentity::new(
            context.clone(),
            QueryExecutionScope::Preview,
            Some("orders"),
        )
        .expect("query identity should be available");

        let result: Result<(), &'static str> = with_query_planning_activity(identity, async {
            profile_query_planning_sync_result(
                "Build scan",
                "build_scan",
                || -> Result<(), &'static str> {
                    profile_query_planning_sync_result("Read metadata", "read_metadata", || Ok(()))
                },
            )
        })
        .await;
        assert_eq!(result, Ok(()));
        context.record_process_result("ok");
        drop(context);

        let spans = capture.captured().spans();
        let planning = spans
            .iter()
            .find(|span| span.name == "DataFusion query planning")
            .expect("query planning should be captured");
        assert_eq!(planning.fields["query_scope"], "preview");
        assert_eq!(planning.fields["query_owner"], "orders");
        assert_eq!(planning.fields["result"], "ok");

        let activities = spans
            .iter()
            .filter(|span| span.name == "DataFusion planning activity")
            .collect::<Vec<_>>();
        assert_eq!(activities.len(), 2);
        assert_eq!(activities[0].parent_id, Some(planning.id));
        assert_eq!(activities[0].fields["planning_activity_name"], "Build scan");
        assert_eq!(activities[0].fields["result"], "ok");
        assert_eq!(activities[1].parent_id, Some(activities[0].id));
        assert_eq!(
            activities[1].fields["planning_activity_name"],
            "Read metadata"
        );
        assert_eq!(activities[1].fields["result"], "ok");
    }

    #[tokio::test]
    async fn planning_activity_records_failures() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let context =
            OperationTraceContext::start_for_test(true).expect("profile tracing should start");
        let identity = QueryTraceIdentity::new(context.clone(), QueryExecutionScope::Preview, None)
            .expect("query identity should be available");

        let result = with_query_planning_activity(identity, async {
            profile_query_planning_sync_result("Build scan", "build_scan", || {
                Err::<(), _>("planning failed")
            })
        })
        .await;
        assert_eq!(result, Err("planning failed"));
        drop(context);

        let spans = capture.captured().spans();
        let planning = spans
            .iter()
            .find(|span| span.name == "DataFusion query planning")
            .expect("query planning should be captured");
        let activity = spans
            .iter()
            .find(|span| span.name == "DataFusion planning activity")
            .expect("planning activity should be captured");
        assert_eq!(planning.fields["result"], "error");
        assert_eq!(activity.fields["result"], "error");
    }
}
