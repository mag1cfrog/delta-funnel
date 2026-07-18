//! Wall-clock activity spans for finalized DataFusion physical plans.

use std::{
    any::Any,
    cell::RefCell,
    collections::HashMap,
    fmt,
    pin::Pin,
    sync::{Arc, Mutex},
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
    QueryExecutionScope, profiling::OperationTraceContext, report::OperationTimelineSpanRecorder,
    usize_to_u64_saturating,
};

use super::QueryTraceIdentity;

const OPERATOR_ACTIVITY_CATEGORY: &str = "datafusion.operator.activity";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivityWorkerKind {
    Coordinator,
    Runtime,
    External,
}

impl ActivityWorkerKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Coordinator => "coordinator",
            Self::Runtime => "runtime",
            Self::External => "external",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActivityWorkerLane {
    id: u64,
    kind: ActivityWorkerKind,
}

impl ActivityWorkerLane {
    fn track_name(self, query_execution_id: u64) -> String {
        match self.kind {
            ActivityWorkerKind::Coordinator => {
                format!("DataFusion query [{query_execution_id}] / coordinator")
            }
            ActivityWorkerKind::Runtime => format!(
                "DataFusion query [{query_execution_id}] / worker [{}]",
                self.id
            ),
            ActivityWorkerKind::External => format!(
                "DataFusion query [{query_execution_id}] / external worker [{}]",
                self.id
            ),
        }
    }
}

#[derive(Debug)]
struct OperatorActivityIdentityState {
    next_stream_id: u64,
    next_worker_lane_id: u64,
    coordinator_thread: Option<ThreadId>,
    worker_lanes: HashMap<ThreadId, ActivityWorkerLane>,
}

impl Default for OperatorActivityIdentityState {
    fn default() -> Self {
        Self {
            next_stream_id: 1,
            next_worker_lane_id: 1,
            coordinator_thread: None,
            worker_lanes: HashMap::new(),
        }
    }
}

#[derive(Debug)]
struct ActivityExecutionContext {
    worker_lane: ActivityWorkerLane,
    task_kind: &'static str,
    runtime_task_id: Option<String>,
    worker_thread_id: String,
    worker_thread_name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActiveOperatorActivitySpan {
    operation_id: u64,
    query_execution_id: u64,
    worker_lane_id: u64,
    timeline_span_id: Option<u64>,
    process_span_active: bool,
}

thread_local! {
    static ACTIVE_OPERATOR_ACTIVITY_SPANS: RefCell<Vec<ActiveOperatorActivitySpan>> =
        const { RefCell::new(Vec::new()) };
}

#[derive(Debug, Clone)]
struct OperatorActivityRecorder {
    context: OperationTraceContext,
    query_execution_id: u64,
    query_scope: QueryExecutionScope,
    query_owner: Option<Arc<str>>,
    identities: Arc<Mutex<OperatorActivityIdentityState>>,
}

impl OperatorActivityRecorder {
    fn new(identity: QueryTraceIdentity) -> Self {
        let QueryTraceIdentity {
            context,
            query_execution_id,
            query_scope,
            query_owner,
        } = identity;
        Self {
            context,
            query_execution_id,
            query_scope,
            query_owner,
            identities: Arc::new(Mutex::new(OperatorActivityIdentityState::default())),
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
        let records_semantic_span = self.context.timeline().is_some();
        let records_process_span = self.context.process_spans_enabled() && activity == "poll_next";
        if !records_semantic_span && !records_process_span {
            return None;
        }
        if let Err(limit) = self.context.reserve_operator_activity() {
            if limit.should_report {
                self.report_truncation(limit.maximum_spans);
            }
            return None;
        }

        let context = self.execution_context();
        let (parent_id, process_parent_active) = ACTIVE_OPERATOR_ACTIVITY_SPANS.with(|active| {
            let active = active.borrow();
            let matches_parent = |parent: &ActiveOperatorActivitySpan| {
                parent.operation_id == self.context.operation_id()
                    && parent.query_execution_id == self.query_execution_id
                    && parent.worker_lane_id == context.worker_lane.id
            };
            let parent_id = active
                .iter()
                .rev()
                .find(|parent| matches_parent(parent))
                .and_then(|parent| parent.timeline_span_id);
            let process_parent_active = active
                .last()
                .is_some_and(|parent| matches_parent(parent) && parent.process_span_active);
            (parent_id, process_parent_active)
        });
        let track_name = context.worker_lane.track_name(self.query_execution_id);
        let timeline_span = self.context.timeline().map(|timeline| {
            self.with_query_identity(timeline.start_span(
                operator_name,
                OPERATOR_ACTIVITY_CATEGORY,
                track_name,
            ))
            .with_parent_id(parent_id)
            .with_attribute("worker_lane_id", Value::from(context.worker_lane.id))
            .with_attribute(
                "worker_kind",
                Value::String(context.worker_lane.kind.as_str().to_owned()),
            )
            .with_attribute("task_kind", Value::String(context.task_kind.to_owned()))
            .with_attribute(
                "runtime_task_id",
                context
                    .runtime_task_id
                    .clone()
                    .map_or(Value::Null, Value::String),
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
            .with_attribute(
                "worker_thread_id",
                Value::String(context.worker_thread_id.clone()),
            )
            .with_attribute(
                "worker_thread_name",
                context
                    .worker_thread_name
                    .clone()
                    .map_or(Value::Null, Value::String),
            )
            .with_attribute("activity", Value::String(activity.to_owned()))
        });
        let timeline_span_id = timeline_span
            .as_ref()
            .and_then(OperationTimelineSpanRecorder::id);
        let process_span = records_process_span
            .then(|| {
                self.process_poll_span(
                    operator_name,
                    node_id,
                    parent_node_id,
                    partition,
                    stream_id,
                    &context,
                    process_parent_active,
                )
            })
            .flatten();
        if timeline_span_id.is_none() && process_span.is_none() {
            return None;
        }
        let active = ActiveOperatorActivitySpan {
            operation_id: self.context.operation_id(),
            query_execution_id: self.query_execution_id,
            worker_lane_id: context.worker_lane.id,
            timeline_span_id,
            process_span_active: process_span.is_some(),
        };
        ACTIVE_OPERATOR_ACTIVITY_SPANS.with(|spans| spans.borrow_mut().push(active));
        Some(OperatorActivitySpanRecorder {
            timeline_span,
            process_span,
            process_result_recorded: false,
            active,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the process span records the canonical operator activity identity"
    )]
    fn process_poll_span(
        &self,
        operator_name: &str,
        node_id: u64,
        parent_node_id: Option<u64>,
        partition: usize,
        stream_id: u64,
        context: &ActivityExecutionContext,
        process_parent_active: bool,
    ) -> Option<tracing::Span> {
        let operation_root = self.context.process_root_span()?;
        let current = tracing::Span::current();
        let parent = if process_parent_active {
            &current
        } else {
            operation_root
        };
        let span = tracing::trace_span!(
            target: crate::profiling::PROFILE_TARGET,
            parent: parent,
            "DataFusion operator poll",
            operation_id = self.context.operation_id(),
            query_execution_id = self.query_execution_id,
            query_scope = self.query_scope.as_str(),
            query_owner = tracing::field::Empty,
            operator_name,
            worker_lane_id = context.worker_lane.id,
            worker_kind = context.worker_lane.kind.as_str(),
            node_id,
            parent_node_id = tracing::field::Empty,
            operator_partition = usize_to_u64_saturating(partition),
            execution_stream_id = stream_id,
            activity = "poll_next",
            result = tracing::field::Empty,
            time_semantics = "active",
        );
        if let Some(query_owner) = &self.query_owner {
            span.record("query_owner", query_owner.as_ref());
        }
        if let Some(parent_node_id) = parent_node_id {
            span.record("parent_node_id", parent_node_id);
        }
        Some(span)
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
        let runtime_task_id = tokio::task::try_id().map(|id| id.to_string());
        let task_kind = if runtime_task_id.is_some() {
            "tokio"
        } else {
            "external"
        };
        let thread_id = thread.id();
        let worker_lane = {
            let mut identities = self
                .identities
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match identities.worker_lanes.get(&thread_id) {
                Some(worker_lane) => *worker_lane,
                None => {
                    let worker_lane =
                        if runtime_task_id.is_none() && identities.coordinator_thread.is_none() {
                            identities.coordinator_thread = Some(thread_id);
                            ActivityWorkerLane {
                                id: 0,
                                kind: ActivityWorkerKind::Coordinator,
                            }
                        } else {
                            let id = identities.next_worker_lane_id;
                            identities.next_worker_lane_id = id.saturating_add(1);
                            ActivityWorkerLane {
                                id,
                                kind: if runtime_task_id.is_some() {
                                    ActivityWorkerKind::Runtime
                                } else {
                                    ActivityWorkerKind::External
                                },
                            }
                        };
                    identities.worker_lanes.insert(thread_id, worker_lane);
                    worker_lane
                }
            }
        };
        ActivityExecutionContext {
            worker_lane,
            task_kind,
            runtime_task_id,
            worker_thread_id: format!("{thread_id:?}"),
            worker_thread_name: thread.name().map(str::to_owned),
        }
    }

    fn report_truncation(&self, maximum_spans: u64) {
        if let Some(timeline) = self.context.timeline() {
            self.with_query_identity(timeline.start_span(
                "Operator activity trace truncated",
                OPERATOR_ACTIVITY_CATEGORY,
                format!(
                    "DataFusion query [{}] / trace status",
                    self.query_execution_id
                ),
            ))
            .with_attribute("maximum_spans", Value::from(maximum_spans))
            .completed();
        }
        if let Some(root) = self.context.process_root_span() {
            tracing::event!(
                name: "Operator activity trace truncated",
                target: crate::profiling::PROFILE_TARGET,
                parent: root.id(),
                tracing::Level::TRACE,
                operation_id = self.context.operation_id(),
                maximum_spans,
            );
        }
    }

    fn with_query_identity(
        &self,
        mut span: OperationTimelineSpanRecorder,
    ) -> OperationTimelineSpanRecorder {
        span = span
            .with_attribute("query_execution_id", Value::from(self.query_execution_id))
            .with_attribute(
                "query_scope",
                Value::String(self.query_scope.as_str().to_owned()),
            );
        if let Some(query_owner) = &self.query_owner {
            span = span.with_attribute("query_owner", Value::String(query_owner.to_string()));
        }
        span
    }
}

struct OperatorActivitySpanRecorder {
    timeline_span: Option<OperationTimelineSpanRecorder>,
    process_span: Option<tracing::Span>,
    process_result_recorded: bool,
    active: ActiveOperatorActivitySpan,
}

impl OperatorActivitySpanRecorder {
    fn in_process_scope<T>(&self, operation: impl FnOnce() -> T) -> T {
        match &self.process_span {
            Some(span) => span.in_scope(operation),
            None => operation(),
        }
    }

    fn finish(mut self, result: &'static str, failed: bool) {
        if let Some(span) = self.timeline_span.take() {
            let span = span.with_attribute("result", Value::String(result.to_owned()));
            if failed {
                span.failed();
            } else {
                span.completed();
            }
        }
        if let Some(span) = &self.process_span {
            span.record("result", result);
            self.process_result_recorded = true;
        }
    }
}

impl Drop for OperatorActivitySpanRecorder {
    fn drop(&mut self) {
        if !self.process_result_recorded
            && let Some(span) = &self.process_span
        {
            span.record("result", "cancelled");
        }
        let _ = ACTIVE_OPERATOR_ACTIVITY_SPANS.try_with(|spans| {
            let popped = spans.borrow_mut().pop();
            debug_assert_eq!(popped, Some(self.active));
        });
    }
}

/// Adds transparent execute and poll instrumentation to one finalized plan.
pub(crate) fn instrument_query_execution_plan(
    root: Arc<dyn ExecutionPlan>,
    identity: QueryTraceIdentity,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let activity = OperatorActivityRecorder::new(identity);
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
            span.finish(
                if result.is_ok() { "stream" } else { "error" },
                result.is_err(),
            );
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
        let poll = match &span {
            Some(span) => span.in_process_scope(|| self.inner.as_mut().poll_next(context)),
            None => self.inner.as_mut().poll_next(context),
        };
        if let Some(span) = span {
            let (result, failed) = match &poll {
                Poll::Pending => ("pending", false),
                Poll::Ready(Some(Ok(_))) => ("batch", false),
                Poll::Ready(Some(Err(_))) => ("error", true),
                Poll::Ready(None) => ("eof", false),
            };
            span.finish(result, failed);
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
    use std::{collections::BTreeSet, error::Error, sync::Arc};

    use datafusion::{physical_plan::collect, prelude::SessionContext};

    use crate::{
        QueryExecutionOutcome, QueryExecutionScope, TimelineSpanStatus, TimelineSpanTimeSemantics,
        observability::test_capture::TracingCapture, profiling::OperationTraceContext,
        query_engine::datafusion::execution_profile::collect_query_execution_profile,
        report::OperationTimelineRecorder,
    };

    use super::*;

    fn test_trace_identity(timeline: OperationTimelineRecorder) -> QueryTraceIdentity {
        let context = OperationTraceContext::start_for_test(Some(timeline), false)
            .expect("semantic tracing should create a context");
        QueryTraceIdentity::new(context, QueryExecutionScope::Preview, None)
            .expect("query trace identity should be available")
    }

    fn test_trace_identity_with_limit(
        timeline: Option<OperationTimelineRecorder>,
        process_spans_enabled: bool,
        maximum_spans: u64,
    ) -> QueryTraceIdentity {
        let context = OperationTraceContext::start_for_test_with_operator_activity_limit(
            timeline,
            process_spans_enabled,
            maximum_spans,
        )
        .expect("profiling should create a context");
        QueryTraceIdentity::new(context, QueryExecutionScope::Preview, None)
            .expect("query trace identity should be available")
    }

    #[test]
    fn query_execution_ids_are_local_to_each_operation_context() {
        let first_timeline = OperationTimelineRecorder::start();
        let first_context = OperationTraceContext::start_for_test(Some(first_timeline), false)
            .expect("semantic tracing should create a context");
        let first_query = OperatorActivityRecorder::new(
            QueryTraceIdentity::new(first_context.clone(), QueryExecutionScope::Preview, None)
                .expect("first query identity should be available"),
        );
        let second_query = OperatorActivityRecorder::new(
            QueryTraceIdentity::new(first_context, QueryExecutionScope::Preview, None)
                .expect("second query identity should be available"),
        );
        let separate_timeline = OperationTimelineRecorder::start();
        let separate_query = OperatorActivityRecorder::new(test_trace_identity(separate_timeline));

        assert_eq!(first_query.query_execution_id, 1);
        assert_eq!(second_query.query_execution_id, 2);
        assert_eq!(separate_query.query_execution_id, 1);
    }

    #[test]
    fn activity_limit_records_one_visible_truncation_marker() {
        let timeline = OperationTimelineRecorder::start();
        let activity = OperatorActivityRecorder::new(test_trace_identity_with_limit(
            Some(timeline.clone()),
            false,
            1,
        ));

        activity
            .start_span("FilterExec", 0, None, 0, 1, "poll_next")
            .expect("first activity should fit")
            .finish("batch", false);
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
        crate::report::trace_contract::validate_truncated_operation_trace(&timeline)
            .expect("truncated operator timeline should satisfy the trace contract");
        let activity_span = timeline
            .spans()
            .iter()
            .find(|span| span.name() == "FilterExec")
            .expect("first activity should be recorded");
        assert!(activity_span.track_name().ends_with(" / coordinator"));
        assert_eq!(activity_span.attributes()["worker_lane_id"], 0);
        assert_eq!(activity_span.attributes()["worker_kind"], "coordinator");
        assert_eq!(activity_span.attributes()["runtime_task_id"], Value::Null);
        let truncation_markers = timeline
            .spans()
            .iter()
            .filter(|span| span.name() == "Operator activity trace truncated")
            .collect::<Vec<_>>();
        assert_eq!(truncation_markers.len(), 1);
        assert_eq!(
            truncation_markers[0].track_name(),
            "DataFusion query [1] / trace status"
        );
    }

    #[test]
    fn operation_activity_limit_is_shared_by_queries() {
        let timeline = OperationTimelineRecorder::start();
        let context = OperationTraceContext::start_for_test_with_operator_activity_limit(
            Some(timeline.clone()),
            false,
            1,
        )
        .expect("semantic profiling should create a context");
        let first = OperatorActivityRecorder::new(
            QueryTraceIdentity::new(context.clone(), QueryExecutionScope::Preview, None)
                .expect("first query identity should be available"),
        );
        let second = OperatorActivityRecorder::new(
            QueryTraceIdentity::new(context, QueryExecutionScope::Preview, None)
                .expect("second query identity should be available"),
        );

        first
            .start_span("FirstExec", 0, None, 0, 1, "poll_next")
            .expect("first query should consume the operation budget")
            .finish("batch", false);
        assert!(
            second
                .start_span("SecondExec", 0, None, 0, 1, "poll_next")
                .is_none()
        );
        assert!(
            first
                .start_span("FirstExec", 0, None, 0, 1, "poll_next")
                .is_none()
        );

        let timeline = timeline.finish("test", TimelineSpanStatus::Completed);
        let markers = timeline
            .spans()
            .iter()
            .filter(|span| span.name() == "Operator activity trace truncated")
            .collect::<Vec<_>>();
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].attributes()["query_execution_id"], 2);
        assert_eq!(markers[0].attributes()["maximum_spans"], 1);
    }

    #[test]
    fn concurrent_operations_emit_independent_truncation_markers() {
        let first_timeline = OperationTimelineRecorder::start();
        let second_timeline = OperationTimelineRecorder::start();
        let first = OperatorActivityRecorder::new(test_trace_identity_with_limit(
            Some(first_timeline.clone()),
            false,
            1,
        ));
        let second = OperatorActivityRecorder::new(test_trace_identity_with_limit(
            Some(second_timeline.clone()),
            false,
            1,
        ));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let exceed = |activity: OperatorActivityRecorder, barrier: Arc<std::sync::Barrier>| {
            std::thread::spawn(move || {
                barrier.wait();
                activity
                    .start_span("FilterExec", 0, None, 0, 1, "poll_next")
                    .expect("each operation should have its own first reservation")
                    .finish("batch", false);
                assert!(
                    activity
                        .start_span("FilterExec", 0, None, 0, 1, "poll_next")
                        .is_none()
                );
            })
        };

        let first = exceed(first, Arc::clone(&barrier));
        let second = exceed(second, barrier);
        first.join().expect("first operation should finish");
        second.join().expect("second operation should finish");

        for timeline in [first_timeline, second_timeline] {
            let timeline = timeline.finish("test", TimelineSpanStatus::Completed);
            let markers = timeline
                .spans()
                .iter()
                .filter(|span| span.name() == "Operator activity trace truncated")
                .collect::<Vec<_>>();
            assert_eq!(markers.len(), 1);
            assert_eq!(markers[0].attributes()["maximum_spans"], 1);
            assert_eq!(
                timeline
                    .spans()
                    .iter()
                    .filter(|span| span.name() == "FilterExec")
                    .count(),
                1
            );
        }
    }

    #[test]
    fn combined_activity_limit_reserves_once_and_reports_each_output_once() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let timeline = OperationTimelineRecorder::start();
        let activity = OperatorActivityRecorder::new(test_trace_identity_with_limit(
            Some(timeline.clone()),
            true,
            1,
        ));
        let operation_id = activity.context.operation_id();

        activity
            .start_span("FilterExec", 0, None, 0, 1, "poll_next")
            .expect("one logical poll should fit both active outputs")
            .finish("batch", false);
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
        drop(activity);

        let timeline = timeline.finish("test", TimelineSpanStatus::Completed);
        let semantic_markers = timeline
            .spans()
            .iter()
            .filter(|span| span.name() == "Operator activity trace truncated")
            .collect::<Vec<_>>();
        assert_eq!(semantic_markers.len(), 1);
        assert_eq!(semantic_markers[0].attributes()["maximum_spans"], 1);

        let process_markers = capture
            .captured()
            .events()
            .into_iter()
            .filter(|event| event.name == "Operator activity trace truncated")
            .collect::<Vec<_>>();
        assert_eq!(process_markers.len(), 1);
        assert_eq!(process_markers[0].target, crate::profiling::PROFILE_TARGET);
        assert_eq!(process_markers[0].level, tracing::Level::TRACE);
        assert_eq!(
            process_markers[0].fields["operation_id"],
            operation_id.to_string()
        );
        assert_eq!(process_markers[0].fields["maximum_spans"], "1");
        assert_eq!(process_markers[0].span_names, ["Delta Funnel preview"]);
    }

    #[test]
    fn process_only_activity_limit_reports_without_a_semantic_timeline() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let activity = OperatorActivityRecorder::new(test_trace_identity_with_limit(None, true, 1));

        activity
            .start_span("FilterExec", 0, None, 0, 1, "poll_next")
            .expect("first process activity should fit")
            .finish("batch", false);
        assert!(
            activity
                .start_span("FilterExec", 0, None, 0, 1, "poll_next")
                .is_none()
        );
        drop(activity);

        assert_eq!(
            capture
                .captured()
                .events()
                .iter()
                .filter(|event| event.name == "Operator activity trace truncated")
                .count(),
            1
        );
    }

    #[test]
    fn track_names_delimit_query_and_worker_ids() {
        let worker_1 = ActivityWorkerLane {
            id: 1,
            kind: ActivityWorkerKind::Runtime,
        }
        .track_name(1);
        let worker_10 = ActivityWorkerLane {
            id: 10,
            kind: ActivityWorkerKind::Runtime,
        }
        .track_name(1);
        let query_10 = ActivityWorkerLane {
            id: 1,
            kind: ActivityWorkerKind::Runtime,
        }
        .track_name(10);

        assert_eq!(worker_1, "DataFusion query [1] / worker [1]");
        assert!(!worker_10.contains("worker [1]"));
        assert!(!query_10.contains("query [1]"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_tracks_are_bounded_by_executor_threads_not_task_count()
    -> Result<(), Box<dyn Error>> {
        const TASK_COUNT: usize = 32;
        let timeline = OperationTimelineRecorder::start();
        let activity = OperatorActivityRecorder::new(test_trace_identity(timeline.clone()));
        let barrier = Arc::new(tokio::sync::Barrier::new(TASK_COUNT));
        let mut tasks = Vec::with_capacity(TASK_COUNT);

        for _ in 0..TASK_COUNT {
            let activity = activity.clone();
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                activity
                    .start_span("WorkerExec", 0, None, 0, 1, "poll_next")
                    .expect("worker activity should fit")
                    .finish("batch", false);
            }));
        }
        for task in tasks {
            task.await?;
        }

        let timeline = timeline.finish("workers", TimelineSpanStatus::Completed);
        let spans = timeline
            .spans()
            .iter()
            .filter(|span| span.category() == OPERATOR_ACTIVITY_CATEGORY)
            .collect::<Vec<_>>();
        let runtime_task_ids = spans
            .iter()
            .filter_map(|span| span.attributes()["runtime_task_id"].as_str())
            .collect::<BTreeSet<_>>();
        let worker_lane_ids = spans
            .iter()
            .filter_map(|span| span.attributes()["worker_lane_id"].as_u64())
            .collect::<BTreeSet<_>>();
        let worker_thread_ids = spans
            .iter()
            .filter_map(|span| span.attributes()["worker_thread_id"].as_str())
            .collect::<BTreeSet<_>>();

        assert_eq!(spans.len(), TASK_COUNT);
        assert_eq!(runtime_task_ids.len(), TASK_COUNT);
        assert!(!worker_lane_ids.is_empty());
        assert!(worker_lane_ids.len() <= 2);
        assert_eq!(worker_thread_ids.len(), worker_lane_ids.len());
        assert!(runtime_task_ids.len() > worker_lane_ids.len());
        assert!(spans.iter().all(|span| {
            span.attributes()["worker_kind"] == "runtime"
                && span.track_name().starts_with("DataFusion query ")
                && span.track_name().contains(" / worker ")
        }));

        Ok(())
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
        let plan = instrument_query_execution_plan(plan, test_trace_identity(timeline.clone()))?;

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
            let worker_lane_id = span.attributes()["worker_lane_id"].as_u64();
            let expected_track_name = match (
                query_execution_id,
                worker_lane_id,
                span.attributes()["worker_kind"].as_str(),
            ) {
                (Some(query), Some(_), Some("coordinator")) => {
                    Some(format!("DataFusion query [{query}] / coordinator"))
                }
                (Some(query), Some(worker), Some("runtime")) => {
                    Some(format!("DataFusion query [{query}] / worker [{worker}]"))
                }
                (Some(query), Some(worker), Some("external")) => Some(format!(
                    "DataFusion query [{query}] / external worker [{worker}]"
                )),
                _ => None,
            };
            span.time_semantics() == TimelineSpanTimeSemantics::WallClock
                && query_execution_id.is_some()
                && worker_lane_id.is_some()
                && expected_track_name.as_deref() == Some(span.track_name())
                && span.attributes()["task_kind"].is_string()
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
                parent.attributes()["worker_lane_id"],
                span.attributes()["worker_lane_id"]
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
                let left_crosses_right =
                    left_start < right_start && right_start < left_end && left_end < right_end;
                let right_crosses_left =
                    right_start < left_start && left_start < right_end && right_end < left_end;
                assert!(
                    !(left_crosses_right || right_crosses_left),
                    "activity spans on one worker lane must not cross"
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn process_only_operator_polls_are_active_and_properly_nested()
    -> Result<(), Box<dyn Error>> {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let session = SessionContext::new();
        let dataframe = session
            .sql("select 1 as id union all select 2 as id")
            .await?;
        let task_context = Arc::new(dataframe.task_ctx());
        let plan = dataframe.create_physical_plan().await?;
        let context =
            OperationTraceContext::start(crate::profiling::OperationTraceKind::Preview, None)
                .expect("process tracing should create a context");
        let operation_id = context.operation_id();
        let identity = QueryTraceIdentity::new(context.clone(), QueryExecutionScope::Preview, None)
            .expect("query trace identity should be available");
        let plan = instrument_query_execution_plan(plan, identity)?;

        let batches = collect(Arc::clone(&plan), task_context).await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        context.record_process_result("ok");
        drop(plan);
        drop(context);

        let spans = capture.captured().spans();
        let root = spans
            .iter()
            .find(|span| span.name == "Delta Funnel preview")
            .expect("operation root span should be captured");
        let polls = spans
            .iter()
            .filter(|span| span.name == "DataFusion operator poll")
            .collect::<Vec<_>>();
        assert!(!polls.is_empty());
        assert!(polls.iter().all(|span| {
            span.target == crate::profiling::PROFILE_TARGET
                && span.level == tracing::Level::TRACE
                && span.fields["operation_id"] == operation_id.to_string()
                && span.fields["query_execution_id"] == "1"
                && span.fields["query_scope"] == "preview"
                && span.fields["activity"] == "poll_next"
                && span.fields["time_semantics"] == "active"
                && span.fields.contains_key("operator_name")
                && span.fields.contains_key("worker_lane_id")
                && span.fields.contains_key("worker_kind")
                && span.fields.contains_key("node_id")
                && span.fields.contains_key("operator_partition")
                && span.fields.contains_key("execution_stream_id")
                && matches!(
                    span.fields["result"].as_str(),
                    "pending" | "batch" | "error" | "eof"
                )
                && span.enter_count == 1
                && span.exit_count == 1
                && span.closed
        }));
        assert!(polls.iter().all(|span| {
            span.parent_id == Some(root.id)
                || polls.iter().any(|parent| Some(parent.id) == span.parent_id)
        }));
        assert!(
            polls
                .iter()
                .any(|span| polls.iter().any(|parent| Some(parent.id) == span.parent_id)),
            "at least one child operator poll should nest under its caller"
        );

        Ok(())
    }
}
