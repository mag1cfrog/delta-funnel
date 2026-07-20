//! Nested wall-clock activity for DataFusion physical planning.

use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use serde_json::Value;

use crate::{
    QueryExecutionScope,
    profiling::{OperationStageTrace, PROFILE_TARGET},
};

use super::QueryTraceIdentity;

const PLANNING_ACTIVITY_CATEGORY: &str = "datafusion.planning.activity";

#[derive(Clone)]
struct PlanningActivityContext {
    identity: QueryTraceIdentity,
    track_name: Arc<str>,
    process_parent: Option<tracing::Span>,
    active_spans: Arc<Mutex<Vec<ActivePlanningSpan>>>,
}

#[derive(Clone)]
struct ActivePlanningSpan {
    timeline_id: Option<u64>,
    process_span: Option<tracing::Span>,
}

impl ActivePlanningSpan {
    fn key(&self) -> (Option<u64>, Option<u64>) {
        (
            self.timeline_id,
            self.process_span
                .as_ref()
                .and_then(tracing::Span::id)
                .map(|id| id.into_u64()),
        )
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
    let track_name =
        query_planning_track_name(identity.query_scope(), identity.query_owner()).into();
    let process_span = process_query_planning_span(&identity);
    let process_parent = process_span.as_ref().map(|span| span.span.clone());
    let result = PLANNING_ACTIVITY
        .scope(
            PlanningActivityContext {
                identity,
                track_name,
                process_parent,
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
        let parent = self
            .active_spans
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .last()
            .cloned();
        let parent_id = parent.as_ref().and_then(|parent| parent.timeline_id);
        let timeline_span = self
            .identity
            .timeline()
            .map(|timeline| {
                timeline
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
                    )
            })
            .map(|span| match self.identity.query_owner() {
                Some(query_owner) => {
                    span.with_attribute("query_owner", Value::String(query_owner.to_string()))
                }
                None => span,
            });
        let timeline_id = timeline_span
            .as_ref()
            .and_then(crate::report::OperationTimelineSpanRecorder::id);
        let process_parent = parent
            .and_then(|parent| parent.process_span)
            .or_else(|| self.process_parent.clone());
        let process_span = process_parent.map(|parent| {
            let span = tracing::trace_span!(
                target: PROFILE_TARGET,
                parent: &parent,
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
            (span, parent)
        });
        let active_span = ActivePlanningSpan {
            timeline_id,
            process_span: process_span.as_ref().map(|(span, _)| span.clone()),
        };
        let key = active_span.key();
        let stage = OperationStageTrace::from_parts(timeline_span, process_span)?;
        self.active_spans
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(active_span);
        Some(PlanningActivitySpanRecorder {
            stage: Some(stage),
            context: self.clone(),
            key,
        })
    }
}

struct PlanningActivitySpanRecorder {
    stage: Option<OperationStageTrace>,
    context: PlanningActivityContext,
    key: (Option<u64>, Option<u64>),
}

impl PlanningActivitySpanRecorder {
    fn with_attribute(mut self, name: impl Into<String>, value: Value) -> Self {
        if let Some(span) = self.stage.take() {
            self.stage = Some(span.with_attribute(name, value));
        }
        self
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
    use std::collections::BTreeSet;

    use crate::{
        QueryExecutionScope, TimelineSpanStatus, observability::test_capture::TracingCapture,
        profiling::OperationTraceContext, report::OperationTimelineRecorder,
    };

    use super::*;

    fn without_timing(mut value: Value) -> Value {
        value["total_duration_micros"] = Value::from(0);
        for span in value["spans"]
            .as_array_mut()
            .expect("timeline spans should be an array")
        {
            span["start_offset_micros"] = Value::from(0);
            span["duration_micros"] = Value::from(0);
        }
        value
    }

    #[tokio::test]
    async fn nested_planning_failures_keep_parentage_and_status() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let recorder = OperationTimelineRecorder::start();
        let context = OperationTraceContext::start_for_test(Some(recorder.clone()), true)
            .expect("semantic tracing should create a context");

        let identity = QueryTraceIdentity::new(
            context.clone(),
            QueryExecutionScope::MssqlOutput,
            Some("orders"),
        )
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

        context.record_process_result("error");
        drop(context);
        let spans = capture.captured().spans();
        let query_planning = spans
            .iter()
            .find(|span| span.name == "DataFusion query planning")
            .expect("query planning should be captured");
        let activities = spans
            .iter()
            .filter(|span| span.name == "DataFusion planning activity")
            .collect::<Vec<_>>();
        assert_eq!(activities.len(), 2);
        let process_parent = activities
            .iter()
            .find(|span| span.fields["activity"] == "parent_activity")
            .expect("the parent activity should be captured");
        let process_child = activities
            .iter()
            .find(|span| span.fields["activity"] == "child_activity")
            .expect("the child activity should be captured");
        assert_eq!(process_parent.parent_id, Some(query_planning.id));
        assert_eq!(process_child.parent_id, Some(process_parent.id));
        assert_eq!(process_parent.fields["planning_activity_name"], "parent");
        assert_eq!(process_child.fields["planning_activity_name"], "child");
        for span in activities {
            assert_eq!(span.target, PROFILE_TARGET);
            assert_eq!(span.level, tracing::Level::TRACE);
            assert_eq!(span.fields["query_execution_id"], "1");
            assert_eq!(span.fields["query_scope"], "mssql_output");
            assert_eq!(span.fields["query_owner"], "orders");
            assert_eq!(span.fields["result"], "error");
            assert_eq!(span.fields["time_semantics"], "wall_clock");
            assert_eq!(span.enter_count, 0);
            assert_eq!(span.exit_count, 0);
            assert!(span.closed);
        }
    }

    #[tokio::test]
    async fn planning_activities_route_all_activation_modes() {
        let capture = TracingCapture::start_with_profile_spans_enabled();

        let disabled: Result<(), &str> =
            profile_query_planning_sync_result("disabled", "disabled", || Ok(()));
        assert_eq!(disabled, Ok(()));

        let timeline_only = OperationTimelineRecorder::start();
        let timeline_context =
            OperationTraceContext::start_for_test(Some(timeline_only.clone()), false)
                .expect("the timeline should create a context");
        let timeline_identity = QueryTraceIdentity::new(
            timeline_context,
            QueryExecutionScope::MssqlOutput,
            Some("orders"),
        )
        .expect("timeline identity should be available");
        with_query_planning_activity(timeline_identity, async {
            profile_query_planning_sync_result("shared", "shared", || Ok::<_, &str>(()))
        })
        .await
        .expect("timeline activity should succeed");
        let timeline = timeline_only.finish("stable", TimelineSpanStatus::Completed);
        assert_eq!(timeline.spans().len(), 1);
        assert_eq!(timeline.spans()[0].name(), "shared");

        let process_context = OperationTraceContext::start_for_test(None, true)
            .expect("process diagnostics should create a context");
        let process_identity =
            QueryTraceIdentity::new(process_context.clone(), QueryExecutionScope::Preview, None)
                .expect("process identity should be available");
        with_query_planning_activity(process_identity, async {
            profile_query_planning_sync_result("process", "process", || Ok::<_, &str>(()))
        })
        .await
        .expect("process activity should succeed");
        process_context.record_process_result("ok");
        drop(process_context);

        let combined_timeline = OperationTimelineRecorder::start();
        let combined_context =
            OperationTraceContext::start_for_test(Some(combined_timeline.clone()), true)
                .expect("both diagnostics should create a context");
        let combined_identity = QueryTraceIdentity::new(
            combined_context.clone(),
            QueryExecutionScope::MssqlOutput,
            Some("orders"),
        )
        .expect("combined identity should be available");
        with_query_planning_activity(combined_identity, async {
            profile_query_planning_sync_result("shared", "shared", || Ok::<_, &str>(()))
        })
        .await
        .expect("combined activity should succeed");
        combined_context.record_process_result("ok");
        drop(combined_context);
        let combined = combined_timeline.finish("stable", TimelineSpanStatus::Completed);
        assert_eq!(combined.spans().len(), 1);
        assert_eq!(combined.spans()[0].name(), "shared");
        assert_eq!(
            without_timing(timeline.to_json_value()),
            without_timing(combined.to_json_value())
        );

        let process_activities = capture
            .captured()
            .spans()
            .into_iter()
            .filter(|span| span.name == "DataFusion planning activity")
            .collect::<Vec<_>>();
        assert_eq!(process_activities.len(), 2);
        assert!(
            process_activities
                .iter()
                .any(|span| span.fields["planning_activity_name"] == "process"
                    && !span.fields.contains_key("query_owner"))
        );
        assert!(
            process_activities
                .iter()
                .any(|span| span.fields["planning_activity_name"] == "shared"
                    && span.fields["query_owner"] == "orders")
        );
        assert!(
            process_activities
                .iter()
                .all(|span| span.fields["result"] == "ok")
        );
    }

    #[tokio::test]
    async fn dropped_planning_activity_cancels_both_outputs() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let recorder = OperationTimelineRecorder::start();
        let context = OperationTraceContext::start_for_test(Some(recorder.clone()), true)
            .expect("both diagnostics should create a context");
        let identity = QueryTraceIdentity::new(context.clone(), QueryExecutionScope::Preview, None)
            .expect("query trace identity should be available");

        with_query_planning_activity(identity, async {
            let activity = PLANNING_ACTIVITY
                .with(|context| context.start_span("dropped", "dropped"))
                .expect("the activity should start");
            drop(activity);
            Ok::<_, &str>(())
        })
        .await
        .expect("query planning should continue after the owned activity is dropped");
        context.record_process_result("ok");
        drop(context);

        let timeline = recorder.finish("dropped", TimelineSpanStatus::Completed);
        assert_eq!(timeline.spans().len(), 1);
        assert_eq!(timeline.spans()[0].status(), TimelineSpanStatus::Cancelled);
        let process = capture
            .captured()
            .spans()
            .into_iter()
            .find(|span| {
                span.name == "DataFusion planning activity"
                    && span.fields["planning_activity_name"] == "dropped"
            })
            .expect("the process activity should be captured");
        assert_eq!(process.fields["result"], "cancelled");
        assert!(process.closed);
    }

    #[tokio::test]
    async fn successful_nesting_and_siblings_do_not_cross() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let recorder = OperationTimelineRecorder::start();
        let context = OperationTraceContext::start_for_test(Some(recorder.clone()), true)
            .expect("both diagnostics should create a context");
        let identity = QueryTraceIdentity::new(
            context.clone(),
            QueryExecutionScope::MssqlOutput,
            Some("orders"),
        )
        .expect("query trace identity should be available");

        with_query_planning_activity(identity, async {
            profile_query_planning_sync_result("parent", "parent", || {
                profile_query_planning_sync_result("child", "child", || Ok::<_, &str>(()))
            })?;
            profile_query_planning_sync_result("sibling", "sibling", || Ok::<_, &str>(()))
        })
        .await
        .expect("planning activities should succeed");
        context.record_process_result("ok");
        drop(context);

        let timeline = recorder.finish("nested", TimelineSpanStatus::Completed);
        let parent = timeline
            .spans()
            .iter()
            .find(|span| span.name() == "parent")
            .expect("the parent should be recorded");
        let child = timeline
            .spans()
            .iter()
            .find(|span| span.name() == "child")
            .expect("the child should be recorded");
        let sibling = timeline
            .spans()
            .iter()
            .find(|span| span.name() == "sibling")
            .expect("the sibling should be recorded");
        assert_eq!(child.parent_id(), Some(parent.id()));
        assert_eq!(sibling.parent_id(), None);
        assert!(parent.start_offset_micros() <= child.start_offset_micros());
        assert!(
            child
                .start_offset_micros()
                .saturating_add(child.duration_micros())
                <= parent
                    .start_offset_micros()
                    .saturating_add(parent.duration_micros())
        );
        assert!(
            parent
                .start_offset_micros()
                .saturating_add(parent.duration_micros())
                <= sibling.start_offset_micros()
        );

        let spans = capture.captured().spans();
        let query_planning = spans
            .iter()
            .find(|span| span.name == "DataFusion query planning")
            .expect("query planning should be captured");
        let activities = spans
            .iter()
            .filter(|span| span.name == "DataFusion planning activity")
            .collect::<Vec<_>>();
        let process_parent = activities
            .iter()
            .find(|span| span.fields["activity"] == "parent")
            .expect("the process parent should be captured");
        let process_child = activities
            .iter()
            .find(|span| span.fields["activity"] == "child")
            .expect("the process child should be captured");
        let process_sibling = activities
            .iter()
            .find(|span| span.fields["activity"] == "sibling")
            .expect("the process sibling should be captured");
        assert_eq!(process_parent.parent_id, Some(query_planning.id));
        assert_eq!(process_child.parent_id, Some(process_parent.id));
        assert_eq!(process_sibling.parent_id, Some(query_planning.id));
        let expected_fields = BTreeSet::from([
            "activity",
            "operation_id",
            "planning_activity_name",
            "query_execution_id",
            "query_owner",
            "query_scope",
            "result",
            "time_semantics",
        ]);
        assert!(activities.iter().all(|span| {
            span.fields
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                == expected_fields
                && span.fields["result"] == "ok"
        }));
    }

    #[tokio::test]
    async fn concurrent_operations_keep_task_local_planning_parents() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let first = OperationTraceContext::start_for_test(None, true)
            .expect("the first process context should start");
        let second = OperationTraceContext::start_for_test(None, true)
            .expect("the second process context should start");
        let first_operation_id = first.operation_id();
        let second_operation_id = second.operation_id();
        let first_identity = QueryTraceIdentity::new(
            first.clone(),
            QueryExecutionScope::MssqlOutput,
            Some("first"),
        )
        .expect("the first query identity should start");
        let second_identity = QueryTraceIdentity::new(
            second.clone(),
            QueryExecutionScope::MssqlOutput,
            Some("second"),
        )
        .expect("the second query identity should start");

        let first_planning = with_query_planning_activity(first_identity, async {
            tokio::task::yield_now().await;
            profile_query_planning_sync_result("first", "first", || Ok::<_, &str>(()))
        });
        let second_planning = with_query_planning_activity(second_identity, async {
            tokio::task::yield_now().await;
            profile_query_planning_sync_result("second", "second", || Ok::<_, &str>(()))
        });
        let (first_result, second_result) = tokio::join!(first_planning, second_planning);
        assert_eq!(first_result, Ok(()));
        assert_eq!(second_result, Ok(()));
        first.record_process_result("ok");
        second.record_process_result("ok");
        drop(first);
        drop(second);

        let spans = capture.captured().spans();
        let activities = spans
            .iter()
            .filter(|span| span.name == "DataFusion planning activity")
            .collect::<Vec<_>>();
        assert_eq!(activities.len(), 2);
        for (activity, operation_id, owner) in [
            ("first", first_operation_id, "first"),
            ("second", second_operation_id, "second"),
        ] {
            let child = activities
                .iter()
                .find(|span| span.fields["activity"] == activity)
                .expect("the planning activity should be captured");
            let parent = spans
                .iter()
                .find(|span| Some(span.id) == child.parent_id)
                .expect("the query planning parent should be captured");
            assert_eq!(parent.name, "DataFusion query planning");
            assert_eq!(child.fields["operation_id"], operation_id.to_string());
            assert_eq!(parent.fields["operation_id"], child.fields["operation_id"]);
            assert_eq!(child.fields["query_execution_id"], "1");
            assert_eq!(child.fields["query_owner"], owner);
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
