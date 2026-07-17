//! Nested wall-clock activity for DataFusion physical planning.

use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use serde_json::Value;

use crate::{
    QueryExecutionScope,
    report::{OperationTimelineRecorder, OperationTimelineSpanRecorder},
};

const PLANNING_ACTIVITY_CATEGORY: &str = "datafusion.planning.activity";

#[derive(Clone)]
struct PlanningActivityContext {
    timeline: OperationTimelineRecorder,
    track_name: Arc<str>,
    query_scope: QueryExecutionScope,
    query_owner: Option<Arc<str>>,
    active_spans: Arc<Mutex<Vec<u64>>>,
}

tokio::task_local! {
    static PLANNING_ACTIVITY: PlanningActivityContext;
}

pub(crate) async fn with_query_planning_activity<F>(
    timeline: OperationTimelineRecorder,
    query_scope: QueryExecutionScope,
    query_owner: Option<&str>,
    future: F,
) -> F::Output
where
    F: Future,
{
    let query_owner = query_owner.map(Arc::<str>::from);
    let track_name = query_planning_track_name(query_scope, query_owner.as_deref()).into();
    PLANNING_ACTIVITY
        .scope(
            PlanningActivityContext {
                timeline,
                track_name,
                query_scope,
                query_owner,
                active_spans: Arc::new(Mutex::new(Vec::new())),
            },
            future,
        )
        .await
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
            .timeline
            .start_span(name, PLANNING_ACTIVITY_CATEGORY, self.track_name.as_ref())
            .with_parent_id(parent_id)
            .with_attribute("activity", Value::String(activity.to_owned()))
            .with_attribute(
                "query_scope",
                Value::String(self.query_scope.as_str().to_owned()),
            );
        if let Some(query_owner) = &self.query_owner {
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
    use crate::TimelineSpanStatus;

    use super::*;

    #[tokio::test]
    async fn nested_planning_failures_keep_parentage_and_status() {
        let recorder = OperationTimelineRecorder::start();

        let result: Result<(), &str> = with_query_planning_activity(
            recorder.clone(),
            QueryExecutionScope::MssqlOutput,
            Some("orders"),
            async {
                profile_query_planning_sync_result("parent", "parent_activity", || {
                    profile_query_planning_sync_result("child", "child_activity", || Err("boom"))
                })
            },
        )
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
            assert_eq!(span.attributes()["query_scope"], "mssql_output");
            assert_eq!(span.attributes()["query_owner"], "orders");
        }
    }
}
