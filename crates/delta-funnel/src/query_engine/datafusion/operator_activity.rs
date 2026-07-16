//! Wall-clock activity spans for finalized DataFusion physical plans.

use std::{
    any::Any,
    cell::RefCell,
    collections::HashMap,
    fmt,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    thread::ThreadId,
};

use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    common::Result as DataFusionResult,
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
        SendableRecordBatchStream, metrics::MetricsSet,
    },
};
use futures_util::Stream;
use serde_json::Value;

use crate::{
    report::{OperationTimelineRecorder, OperationTimelineSpanRecorder},
    usize_to_u64_saturating,
};

const OPERATOR_ACTIVITY_CATEGORY: &str = "datafusion.operator.activity";
const MAX_OPERATOR_ACTIVITY_SPANS: u64 = 100_000;
static NEXT_QUERY_EXECUTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ActivityTask {
    Tokio(tokio::task::Id),
    External(ThreadId),
}

#[derive(Debug)]
struct OperatorActivityIdentityState {
    next_stream_id: u64,
    next_task_lane_id: u64,
    task_lanes: HashMap<ActivityTask, u64>,
}

impl Default for OperatorActivityIdentityState {
    fn default() -> Self {
        Self {
            next_stream_id: 1,
            next_task_lane_id: 1,
            task_lanes: HashMap::new(),
        }
    }
}

#[derive(Debug)]
struct ActivityExecutionContext {
    task_lane_id: u64,
    task_kind: &'static str,
    runtime_task_id: Option<String>,
    worker_thread_id: String,
    worker_thread_name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActiveOperatorActivitySpan {
    query_execution_id: u64,
    task_lane_id: u64,
    span_id: u64,
}

thread_local! {
    static ACTIVE_OPERATOR_ACTIVITY_SPANS: RefCell<Vec<ActiveOperatorActivitySpan>> =
        const { RefCell::new(Vec::new()) };
}

#[derive(Debug, Clone)]
struct OperatorActivityRecorder {
    timeline: OperationTimelineRecorder,
    query_execution_id: u64,
    identities: Arc<Mutex<OperatorActivityIdentityState>>,
    maximum_spans: u64,
    remaining_spans: Arc<AtomicU64>,
    truncation_reported: Arc<AtomicBool>,
}

impl OperatorActivityRecorder {
    fn new(timeline: OperationTimelineRecorder) -> Self {
        Self::with_max_spans(timeline, MAX_OPERATOR_ACTIVITY_SPANS)
    }

    fn with_max_spans(timeline: OperationTimelineRecorder, maximum_spans: u64) -> Self {
        Self {
            timeline,
            query_execution_id: NEXT_QUERY_EXECUTION_ID.fetch_add(1, Ordering::Relaxed),
            identities: Arc::new(Mutex::new(OperatorActivityIdentityState::default())),
            maximum_spans,
            remaining_spans: Arc::new(AtomicU64::new(maximum_spans)),
            truncation_reported: Arc::new(AtomicBool::new(false)),
        }
    }

    fn start_span(
        &self,
        operator_name: &str,
        node_id: u64,
        parent_node_id: Option<u64>,
        partition: usize,
        stream_id: u64,
        activity: &'static str,
    ) -> Option<OperatorActivitySpanRecorder> {
        if self
            .remaining_spans
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .is_err()
        {
            self.report_truncation();
            return None;
        }

        let context = self.execution_context();
        let parent_id = ACTIVE_OPERATOR_ACTIVITY_SPANS.with(|active| {
            active.borrow().last().and_then(|parent| {
                (parent.query_execution_id == self.query_execution_id
                    && parent.task_lane_id == context.task_lane_id)
                    .then_some(parent.span_id)
            })
        });
        let track_name = format!(
            "DataFusion query {} / task {}",
            self.query_execution_id, context.task_lane_id
        );
        let timeline_span = self
            .timeline
            .start_span(operator_name, OPERATOR_ACTIVITY_CATEGORY, track_name)
            .with_parent_id(parent_id)
            .with_attribute("query_execution_id", Value::from(self.query_execution_id))
            .with_attribute("task_lane_id", Value::from(context.task_lane_id))
            .with_attribute("task_kind", Value::String(context.task_kind.to_owned()))
            .with_attribute(
                "runtime_task_id",
                context.runtime_task_id.map_or(Value::Null, Value::String),
            )
            .with_attribute("execution_stream_id", Value::from(stream_id))
            .with_attribute("node_id", Value::from(node_id))
            .with_attribute(
                "parent_node_id",
                parent_node_id.map_or(Value::Null, Value::from),
            )
            .with_attribute(
                "operator_partition",
                Value::from(usize_to_u64_saturating(partition)),
            )
            .with_attribute("worker_thread_id", Value::String(context.worker_thread_id))
            .with_attribute(
                "worker_thread_name",
                context
                    .worker_thread_name
                    .map_or(Value::Null, Value::String),
            )
            .with_attribute("activity", Value::String(activity.to_owned()));
        let active = ActiveOperatorActivitySpan {
            query_execution_id: self.query_execution_id,
            task_lane_id: context.task_lane_id,
            span_id: timeline_span.id()?,
        };
        ACTIVE_OPERATOR_ACTIVITY_SPANS.with(|spans| spans.borrow_mut().push(active));
        Some(OperatorActivitySpanRecorder {
            timeline_span: Some(timeline_span),
            active,
        })
    }

    fn next_stream_id(&self) -> u64 {
        let mut identities = self
            .identities
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let stream_id = identities.next_stream_id;
        identities.next_stream_id = identities.next_stream_id.saturating_add(1);
        stream_id
    }

    fn execution_context(&self) -> ActivityExecutionContext {
        let thread = std::thread::current();
        let (task, task_kind, runtime_task_id) = match tokio::task::try_id() {
            Some(id) => (ActivityTask::Tokio(id), "tokio", Some(id.to_string())),
            None => (ActivityTask::External(thread.id()), "external", None),
        };
        let task_lane_id = {
            let mut identities = self
                .identities
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match identities.task_lanes.get(&task) {
                Some(task_lane_id) => *task_lane_id,
                None => {
                    let task_lane_id = identities.next_task_lane_id;
                    identities.next_task_lane_id = identities.next_task_lane_id.saturating_add(1);
                    identities.task_lanes.insert(task, task_lane_id);
                    task_lane_id
                }
            }
        };
        ActivityExecutionContext {
            task_lane_id,
            task_kind,
            runtime_task_id,
            worker_thread_id: format!("{:?}", thread.id()),
            worker_thread_name: thread.name().map(str::to_owned),
        }
    }

    fn report_truncation(&self) {
        if !self.truncation_reported.swap(true, Ordering::Relaxed) {
            self.timeline
                .start_span(
                    "Operator activity trace truncated",
                    OPERATOR_ACTIVITY_CATEGORY,
                    "DataFusion operator activity",
                )
                .with_attribute("query_execution_id", Value::from(self.query_execution_id))
                .with_attribute("maximum_spans", Value::from(self.maximum_spans))
                .completed();
        }
    }
}

struct OperatorActivitySpanRecorder {
    timeline_span: Option<OperationTimelineSpanRecorder>,
    active: ActiveOperatorActivitySpan,
}

impl OperatorActivitySpanRecorder {
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

impl Drop for OperatorActivitySpanRecorder {
    fn drop(&mut self) {
        let _ = ACTIVE_OPERATOR_ACTIVITY_SPANS.try_with(|spans| {
            let popped = spans.borrow_mut().pop();
            debug_assert_eq!(popped, Some(self.active));
        });
    }
}

/// Adds transparent execute and poll instrumentation to one finalized plan.
pub(super) fn instrument_query_execution_plan(
    root: Arc<dyn ExecutionPlan>,
    timeline: OperationTimelineRecorder,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let activity = OperatorActivityRecorder::new(timeline);
    let mut next_node_id = 0;
    let mut instrumented = HashMap::new();
    instrument_query_execution_node(root, None, &mut next_node_id, &mut instrumented, &activity)
}

fn instrument_query_execution_node(
    plan: Arc<dyn ExecutionPlan>,
    parent_node_id: Option<u64>,
    next_node_id: &mut u64,
    instrumented: &mut HashMap<usize, Arc<dyn ExecutionPlan>>,
    activity: &OperatorActivityRecorder,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let identity = plan_identity(&plan);
    if let Some(plan) = instrumented.get(&identity) {
        return Ok(Arc::clone(plan));
    }

    let node_id = *next_node_id;
    *next_node_id = next_node_id.saturating_add(1);
    let children = plan
        .children()
        .into_iter()
        .map(Arc::clone)
        .map(|child| {
            instrument_query_execution_node(
                child,
                Some(node_id),
                next_node_id,
                instrumented,
                activity,
            )
        })
        .collect::<DataFusionResult<Vec<_>>>()?;
    let inner = plan.with_new_children(children)?;
    let plan: Arc<dyn ExecutionPlan> = Arc::new(ProfiledOperatorExec {
        inner,
        node_id,
        parent_node_id,
        activity: activity.clone(),
    });
    instrumented.insert(identity, Arc::clone(&plan));
    Ok(plan)
}

fn plan_identity(plan: &Arc<dyn ExecutionPlan>) -> usize {
    Arc::as_ptr(plan) as *const () as usize
}

pub(super) fn unprofiled_execution_plan(plan: &dyn ExecutionPlan) -> &dyn ExecutionPlan {
    plan.as_any()
        .downcast_ref::<ProfiledOperatorExec>()
        .map_or(plan, |profiled| profiled.inner.as_ref())
}

#[derive(Debug)]
struct ProfiledOperatorExec {
    inner: Arc<dyn ExecutionPlan>,
    node_id: u64,
    parent_node_id: Option<u64>,
    activity: OperatorActivityRecorder,
}

impl DisplayAs for ProfiledOperatorExec {
    fn fmt_as(
        &self,
        display_type: DisplayFormatType,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        self.inner.fmt_as(display_type, formatter)
    }
}

impl ExecutionPlan for ProfiledOperatorExec {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let inner = Arc::clone(&self.inner).with_new_children(children)?;
        Ok(Arc::new(Self {
            inner,
            node_id: self.node_id,
            parent_node_id: self.parent_node_id,
            activity: self.activity.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let stream_id = self.activity.next_stream_id();
        let span = self.activity.start_span(
            self.name(),
            self.node_id,
            self.parent_node_id,
            partition,
            stream_id,
            "execute",
        );
        let result = self.inner.execute(partition, context);
        if let Some(span) = span {
            let span = span.with_attribute(
                "result",
                Value::String(if result.is_ok() { "stream" } else { "error" }.to_owned()),
            );
            if result.is_ok() {
                span.completed();
            } else {
                span.failed();
            }
        }
        result.map(|inner| {
            Box::pin(ProfiledRecordBatchStream {
                schema: inner.schema(),
                inner,
                operator_name: self.name().to_owned(),
                node_id: self.node_id,
                parent_node_id: self.parent_node_id,
                partition,
                stream_id,
                activity: self.activity.clone(),
            }) as SendableRecordBatchStream
        })
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }
}

struct ProfiledRecordBatchStream {
    schema: SchemaRef,
    inner: SendableRecordBatchStream,
    operator_name: String,
    node_id: u64,
    parent_node_id: Option<u64>,
    partition: usize,
    stream_id: u64,
    activity: OperatorActivityRecorder,
}

impl Stream for ProfiledRecordBatchStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let span = self.activity.start_span(
            &self.operator_name,
            self.node_id,
            self.parent_node_id,
            self.partition,
            self.stream_id,
            "poll_next",
        );
        let poll = self.inner.as_mut().poll_next(context);
        if let Some(span) = span {
            let (result, failed) = match &poll {
                Poll::Pending => ("pending", false),
                Poll::Ready(Some(Ok(_))) => ("batch", false),
                Poll::Ready(Some(Err(_))) => ("error", true),
                Poll::Ready(None) => ("eof", false),
            };
            let span = span.with_attribute("result", Value::String(result.to_owned()));
            if failed {
                span.failed();
            } else {
                span.completed();
            }
        }
        poll
    }
}

impl RecordBatchStream for ProfiledRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use datafusion::{physical_plan::collect, prelude::SessionContext};

    use crate::{
        QueryExecutionOutcome, QueryExecutionScope, TimelineSpanStatus, TimelineSpanTimeSemantics,
        query_engine::datafusion::execution_profile::collect_query_execution_profile,
    };

    use super::*;

    #[test]
    fn activity_limit_records_one_visible_truncation_marker() {
        let timeline = OperationTimelineRecorder::start();
        let activity = OperatorActivityRecorder::with_max_spans(timeline.clone(), 1);

        activity
            .start_span("FilterExec", 0, None, 0, 1, "poll_next")
            .expect("first activity should fit")
            .completed();
        assert!(
            activity
                .start_span("FilterExec", 0, None, 0, 1, "poll_next")
                .is_none()
        );
        assert!(
            activity
                .start_span("FilterExec", 0, None, 0, 1, "poll_next")
                .is_none()
        );

        let timeline = timeline.finish("test", TimelineSpanStatus::Completed);
        assert_eq!(
            timeline
                .spans()
                .iter()
                .filter(|span| span.name() == "Operator activity trace truncated")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn instrumented_plan_preserves_results_and_records_nested_activity()
    -> Result<(), Box<dyn Error>> {
        let context = SessionContext::new();
        let dataframe = context
            .sql("select 1 as id union all select 2 as id")
            .await?;
        let task_context = Arc::new(dataframe.task_ctx());
        let plan = dataframe.create_physical_plan().await?;
        let timeline = OperationTimelineRecorder::start();
        let plan = instrument_query_execution_plan(plan, timeline.clone())?;

        let batches = collect(Arc::clone(&plan), task_context).await?;

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let profile = collect_query_execution_profile(
            &plan,
            QueryExecutionScope::Preview,
            QueryExecutionOutcome::Success,
            2,
            None,
        );
        let timeline = timeline.finish("test", TimelineSpanStatus::Completed);
        let spans = timeline
            .spans()
            .iter()
            .filter(|span| span.category() == OPERATOR_ACTIVITY_CATEGORY)
            .collect::<Vec<_>>();
        assert!(!spans.is_empty());
        assert!(spans.iter().all(|span| {
            let query_execution_id = span.attributes()["query_execution_id"].as_u64();
            let task_lane_id = span.attributes()["task_lane_id"].as_u64();
            span.time_semantics() == TimelineSpanTimeSemantics::WallClock
                && query_execution_id.is_some()
                && task_lane_id.is_some()
                && span.track_name()
                    == format!(
                        "DataFusion query {} / task {}",
                        query_execution_id.unwrap_or_default(),
                        task_lane_id.unwrap_or_default()
                    )
                && span.attributes()["execution_stream_id"].is_u64()
                && span.attributes()["operator_partition"].is_u64()
                && span.attributes()["worker_thread_id"].is_string()
                && matches!(
                    span.attributes()["activity"].as_str(),
                    Some("execute" | "poll_next")
                )
        }));
        for span in &spans {
            let node_id = span.attributes()["node_id"]
                .as_u64()
                .ok_or("expected activity node ID")?;
            let operator = profile
                .operators()
                .iter()
                .find(|operator| operator.node_id() == node_id)
                .ok_or("expected matching profile operator")?;
            assert_eq!(span.name(), operator.operator_name());
            assert_eq!(
                span.attributes()["parent_node_id"].as_u64(),
                operator.parent_node_id()
            );
            if span.attributes()["activity"] == "poll_next" {
                assert!(spans.iter().any(|candidate| {
                    candidate.attributes()["activity"] == "execute"
                        && candidate.attributes()["execution_stream_id"]
                            == span.attributes()["execution_stream_id"]
                        && candidate.attributes()["node_id"] == span.attributes()["node_id"]
                        && candidate.attributes()["operator_partition"]
                            == span.attributes()["operator_partition"]
                }));
            }
        }
        let nested = spans
            .iter()
            .filter_map(|span| span.parent_id().map(|parent_id| (span, parent_id)))
            .collect::<Vec<_>>();
        assert!(!nested.is_empty());
        for (span, parent_id) in nested {
            let parent = spans
                .iter()
                .find(|candidate| candidate.id() == parent_id)
                .ok_or("expected activity parent span")?;
            assert_eq!(parent.track_name(), span.track_name());
            assert_eq!(
                parent.attributes()["query_execution_id"],
                span.attributes()["query_execution_id"]
            );
            assert_eq!(
                parent.attributes()["task_lane_id"],
                span.attributes()["task_lane_id"]
            );
            assert!(parent.start_offset_micros() <= span.start_offset_micros());
            assert!(
                parent
                    .start_offset_micros()
                    .saturating_add(parent.duration_micros())
                    >= span
                        .start_offset_micros()
                        .saturating_add(span.duration_micros())
            );
        }
        for (index, left) in spans.iter().enumerate() {
            for right in spans.iter().skip(index + 1) {
                if left.track_name() != right.track_name() {
                    continue;
                }
                let left_start = left.start_offset_micros();
                let left_end = left_start.saturating_add(left.duration_micros());
                let right_start = right.start_offset_micros();
                let right_end = right_start.saturating_add(right.duration_micros());
                assert!(
                    !(left_start < right_start && right_start < left_end && left_end < right_end)
                        && !(right_start < left_start
                            && left_start < right_end
                            && right_end < left_end),
                    "activity spans on one task lane must not cross"
                );
            }
        }

        Ok(())
    }
}
