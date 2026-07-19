//! Nested wall-clock activity for DataFusion physical planning.

use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use serde_json::Value;

use crate::{QueryExecutionScope, report::OperationTimelineSpanRecorder};

use super::QueryTraceIdentity;

const PLANNING_ACTIVITY_CATEGORY: &str = "datafusion.planning.activity";

#[derive(Clone)]
struct PlanningActivityContext {
    identity: QueryTraceIdentity,
    track_name: Arc<str>,
    active_spans: Arc<Mutex<Vec<u64>>>,
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
    let track_name =
        query_planning_track_name(identity.query_scope(), identity.query_owner()).into();
    let process_span = process_query_planning_span(&identity);
    let result = PLANNING_ACTIVITY
        .scope(
            PlanningActivityContext {
                identity,
                track_name,
                active_spans: Arc::new(Mutex::new(Vec::new())),
            },
            future,
        )
        .await;
    if let Some(span) = process_span {
        span.finish(if result.is_ok() { "ok" } else { "error" });
    }
    result
}

fn process_query_planning_span(identity: &QueryTraceIdentity) -> Option<ProcessPlanningSpan> {
    let parent = identity.process_root_span()?;
    let span = tracing::trace_span!(
        target: crate::profiling::PROFILE_TARGET,
        parent: parent,
        "DataFusion query planning",
        operation_id = identity.operation_id(),
        query_execution_id = identity.query_execution_id(),
        query_scope = identity.query_scope().as_str(),
        query_owner = tracing::field::Empty,
        result = tracing::field::Empty,
        time_semantics = "wall_clock",
    );
    if let Some(owner) = identity.query_owner() {
        span.record("query_owner", owner);
    }
    Some(ProcessPlanningSpan {
        span,
        result_recorded: false,
    })
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

fn query_planning_track_name(scope: QueryExecutionScope, owner: Option<&str>) -> String {
    let scope_name = match scope {
        QueryExecutionScope::Preview => "preview",
        QueryExecutionScope::MssqlOutput => "SQL output",
        QueryExecutionScope::WriteAllCacheAlias => "cache alias",
    };
    match owner {
        Some(owner) => format!("DataFusion query planning / {scope_name}: {owner}"),
        None => format!("DataFusion query planning / {scope_name}"),
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
        .and_then(|context| context.start_span(name, activity));
    let result = operation();
    if let Some(span) = span {
        if result.is_err() {
            span.with_attribute("result", Value::String("error".to_owned()))
                .failed();
        } else {
            span.with_attribute("result", Value::String("ok".to_owned()))
                .completed();
        }
    }
    result
}

impl PlanningActivityContext {
    fn start_span(
        &self,
        name: &'static str,
        activity: &'static str,
    ) -> Option<PlanningActivitySpanRecorder> {
        let parent_id = self
            .active_spans
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .last()
            .copied();
        let mut timeline_span = self
            .identity
            .timeline()?
            .start_span(name, PLANNING_ACTIVITY_CATEGORY, self.track_name.as_ref())
            .with_parent_id(parent_id)
            .with_attribute("activity", Value::String(activity.to_owned()))
            .with_attribute(
                "query_execution_id",
                Value::from(self.identity.query_execution_id()),
            )
            .with_attribute(
                "query_scope",
                Value::String(self.identity.query_scope().as_str().to_owned()),
            );
        if let Some(query_owner) = self.identity.query_owner() {
            timeline_span =
                timeline_span.with_attribute("query_owner", Value::String(query_owner.to_string()));
        }
        let id = timeline_span.id()?;
        self.active_spans
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(id);
        Some(PlanningActivitySpanRecorder {
            timeline_span: Some(timeline_span),
            context: self.clone(),
            id,
        })
    }
}

struct PlanningActivitySpanRecorder {
    timeline_span: Option<OperationTimelineSpanRecorder>,
    context: PlanningActivityContext,
    id: u64,
}

impl PlanningActivitySpanRecorder {
    fn with_attribute(mut self, name: impl Into<String>, value: Value) -> Self {
        if let Some(span) = self.timeline_span.take() {
            self.timeline_span = Some(span.with_attribute(name, value));
        }
        self
    }

    fn completed(mut self) {
        if let Some(span) = self.timeline_span.take() {
            span.completed();
        }
    }

    fn failed(mut self) {
        if let Some(span) = self.timeline_span.take() {
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
        debug_assert_eq!(popped, Some(self.id));
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        QueryExecutionScope, TimelineSpanStatus, observability::test_capture::TracingCapture,
        profiling::OperationTraceContext, report::OperationTimelineRecorder,
    };

    use super::*;

    #[tokio::test]
    async fn nested_planning_failures_keep_parentage_and_status() {
        let recorder = OperationTimelineRecorder::start();
        let context = OperationTraceContext::start_for_test(Some(recorder.clone()), false)
            .expect("semantic tracing should create a context");

        let identity =
            QueryTraceIdentity::new(context, QueryExecutionScope::MssqlOutput, Some("orders"))
                .expect("query trace identity should be available");
        let result: Result<(), &str> = with_query_planning_activity(identity, async {
            profile_query_planning_sync_result("parent", "parent_activity", || {
                profile_query_planning_sync_result("child", "child_activity", || Err("boom"))
            })
        })
        .await;

        assert_eq!(result, Err("boom"));
        let timeline = recorder.finish("test", TimelineSpanStatus::Failed);
        assert_eq!(timeline.spans().len(), 2);
        let parent = &timeline.spans()[0];
        let child = &timeline.spans()[1];
        assert_eq!(parent.name(), "parent");
        assert_eq!(parent.parent_id(), None);
        assert_eq!(parent.status(), TimelineSpanStatus::Failed);
        assert_eq!(parent.attributes()["result"], "error");
        assert_eq!(child.name(), "child");
        assert_eq!(child.parent_id(), Some(parent.id()));
        assert_eq!(child.status(), TimelineSpanStatus::Failed);
        assert_eq!(child.attributes()["result"], "error");
        assert_eq!(
            parent.track_name(),
            "DataFusion query planning / SQL output: orders"
        );
        assert_eq!(child.track_name(), parent.track_name());
        for span in timeline.spans() {
            assert_eq!(span.attributes()["query_execution_id"], 1);
            assert_eq!(span.attributes()["query_scope"], "mssql_output");
            assert_eq!(span.attributes()["query_owner"], "orders");
        }
    }

    #[tokio::test]
    async fn process_planning_span_uses_lifetime_without_crossing_an_await() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let context =
            OperationTraceContext::start(crate::profiling::OperationTraceKind::Preview, None)
                .expect("process tracing should create a context");
        let identity = QueryTraceIdentity::new(
            context.clone(),
            QueryExecutionScope::MssqlOutput,
            Some("orders"),
        )
        .expect("query trace identity should be available");

        let result: Result<(), &str> = with_query_planning_activity(identity, async {
            tokio::task::yield_now().await;
            Err("boom")
        })
        .await;
        assert_eq!(result, Err("boom"));
        context.record_process_result("error");
        drop(context);

        let spans = capture.captured().spans();
        assert_eq!(spans.len(), 2);
        let root = spans
            .iter()
            .find(|span| span.name == "Delta Funnel preview")
            .expect("operation root span should be captured");
        let planning = spans
            .iter()
            .find(|span| span.name == "DataFusion query planning")
            .expect("planning span should be captured");
        assert_eq!(planning.parent_id, Some(root.id));
        assert_eq!(planning.target, crate::profiling::PROFILE_TARGET);
        assert_eq!(planning.level, tracing::Level::TRACE);
        assert_eq!(planning.fields["operation_id"], root.fields["operation_id"]);
        assert_eq!(planning.fields["query_execution_id"], "1");
        assert_eq!(planning.fields["query_scope"], "mssql_output");
        assert_eq!(planning.fields["query_owner"], "orders");
        assert_eq!(planning.fields["result"], "error");
        assert_eq!(planning.fields["time_semantics"], "wall_clock");
        assert_eq!(planning.enter_count, 0);
        assert_eq!(planning.exit_count, 0);
        assert!(planning.closed);
    }

    #[tokio::test]
    async fn cancelled_process_planning_span_closes_with_a_result() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let context =
            OperationTraceContext::start(crate::profiling::OperationTraceKind::Preview, None)
                .expect("process tracing should create a context");
        let identity = QueryTraceIdentity::new(context.clone(), QueryExecutionScope::Preview, None)
            .expect("query trace identity should be available");
        let mut planning = Box::pin(with_query_planning_activity(
            identity,
            std::future::pending::<Result<(), &str>>(),
        ));

        assert!(matches!(
            futures_util::poll!(planning.as_mut()),
            std::task::Poll::Pending
        ));
        drop(planning);
        drop(context);

        let spans = capture.captured().spans();
        let planning = spans
            .iter()
            .find(|span| span.name == "DataFusion query planning")
            .expect("planning span should be captured");
        assert_eq!(planning.fields["result"], "cancelled");
        assert!(planning.closed);
    }
}
