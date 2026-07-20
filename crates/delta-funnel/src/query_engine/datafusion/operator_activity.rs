//! Wall-clock activity spans for finalized DataFusion physical plans.

use std::{
    any::Any,
    cell::RefCell,
    collections::HashMap,
    fmt,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
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
    QueryExecutionScope,
    profiling::{OperationStageTrace, OperationTraceContext},
    report::OperationTimelineSpanRecorder,
    usize_to_u64_saturating,
};

use super::{QueryTraceIdentity, execution::DeltaScanPlanningExec};

const OPERATOR_ACTIVITY_CATEGORY: &str = "datafusion.operator.activity";
const DELTA_SCAN_OUTPUT_WAIT_CATEGORY: &str = "datafusion.execution.activity";
const DELTA_SCAN_OUTPUT_WAIT_NAME: &str = "Await Delta scan output";
const DELTA_SCAN_OUTPUT_WAIT_ACTIVITY: &str = "await_output";

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
    worker_lanes: Vec<ActivityWorkerLaneState>,
}

#[derive(Debug, Clone, Copy)]
struct ActivityWorkerLaneState {
    lane: ActivityWorkerLane,
    active: bool,
}

impl Default for OperatorActivityIdentityState {
    fn default() -> Self {
        Self {
            next_stream_id: 1,
            next_worker_lane_id: 1,
            worker_lanes: Vec::new(),
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
    worker_lane: ActivityWorkerLane,
    owns_worker_lane: bool,
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

        let (context, owns_worker_lane) = self.execution_context(partition);
        let (parent_id, process_parent_active) = ACTIVE_OPERATOR_ACTIVITY_SPANS.with(|active| {
            let active = active.borrow();
            let matches_parent = |parent: &ActiveOperatorActivitySpan| {
                parent.operation_id == self.context.operation_id()
                    && parent.query_execution_id == self.query_execution_id
                    && parent.worker_lane.id == context.worker_lane.id
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
            if owns_worker_lane {
                Self::release_worker_lane(&self.identities, context.worker_lane);
            }
            return None;
        }
        let active = ActiveOperatorActivitySpan {
            operation_id: self.context.operation_id(),
            query_execution_id: self.query_execution_id,
            worker_lane: context.worker_lane,
            owns_worker_lane,
            timeline_span_id,
            process_span_active: process_span.is_some(),
        };
        ACTIVE_OPERATOR_ACTIVITY_SPANS.with(|spans| spans.borrow_mut().push(active));
        Some(OperatorActivitySpanRecorder {
            timeline_span,
            process_span,
            process_result_recorded: false,
            identities: Arc::clone(&self.identities),
            active,
        })
    }

    fn start_delta_scan_output_wait(
        &self,
        node_id: u64,
        partition: usize,
        stream_id: u64,
    ) -> Option<ExecutionActivitySpanRecorder> {
        if self.context.timeline().is_none() && !self.context.process_spans_enabled() {
            return None;
        }
        if let Err(limit) = self.context.reserve_operator_activity() {
            if limit.should_report {
                self.report_truncation(limit.maximum_spans);
            }
            return None;
        }

        let track_name = format!(
            "DataFusion query [{}] / Delta scan output [{}]",
            self.query_execution_id, stream_id
        );
        let timeline_span = self.context.timeline().map(|timeline| {
            self.with_query_identity(timeline.start_span(
                DELTA_SCAN_OUTPUT_WAIT_NAME,
                DELTA_SCAN_OUTPUT_WAIT_CATEGORY,
                track_name,
            ))
            .with_attribute(
                "activity",
                Value::String(DELTA_SCAN_OUTPUT_WAIT_ACTIVITY.to_owned()),
            )
            .with_attribute("node_id", Value::from(node_id))
            .with_attribute(
                "operator_partition",
                Value::from(usize_to_u64_saturating(partition)),
            )
            .with_attribute("execution_stream_id", Value::from(stream_id))
        });
        let process_span = self.context.process_root_span().map(|parent| {
            let span = tracing::trace_span!(
                target: crate::profiling::PROFILE_TARGET,
                parent: parent,
                "DataFusion execution activity",
                operation_id = self.context.operation_id(),
                query_execution_id = self.query_execution_id,
                query_scope = self.query_scope.as_str(),
                query_owner = tracing::field::Empty,
                execution_activity_name = DELTA_SCAN_OUTPUT_WAIT_NAME,
                node_id,
                operator_partition = usize_to_u64_saturating(partition),
                execution_stream_id = stream_id,
                activity = DELTA_SCAN_OUTPUT_WAIT_ACTIVITY,
                result = tracing::field::Empty,
                time_semantics = "wall_clock",
            );
            if let Some(query_owner) = &self.query_owner {
                span.record("query_owner", query_owner.as_ref());
            }
            (span, parent.clone())
        });
        let stage = OperationStageTrace::from_parts(timeline_span, process_span)?;
        Some(ExecutionActivitySpanRecorder { stage: Some(stage) })
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

    fn execution_context(&self, partition: usize) -> (ActivityExecutionContext, bool) {
        let thread = std::thread::current();
        let runtime_task_id = tokio::task::try_id().map(|id| id.to_string());
        let task_kind = if runtime_task_id.is_some() {
            "tokio"
        } else {
            "external"
        };
        let thread_id = thread.id();
        let inherited_worker_lane = ACTIVE_OPERATOR_ACTIVITY_SPANS.with(|active| {
            active.borrow().iter().rev().find_map(|parent| {
                (parent.operation_id == self.context.operation_id()
                    && parent.query_execution_id == self.query_execution_id)
                    .then_some(parent.worker_lane)
            })
        });
        let (worker_lane, owns_worker_lane) = match inherited_worker_lane {
            Some(worker_lane) => (worker_lane, false),
            None if runtime_task_id.is_none() && partition == 0 => (
                ActivityWorkerLane {
                    id: 0,
                    kind: ActivityWorkerKind::Coordinator,
                },
                false,
            ),
            None => {
                let kind = if runtime_task_id.is_some() {
                    ActivityWorkerKind::Runtime
                } else {
                    ActivityWorkerKind::External
                };
                (self.acquire_worker_lane(kind), true)
            }
        };
        (
            ActivityExecutionContext {
                worker_lane,
                task_kind,
                runtime_task_id,
                worker_thread_id: format!("{thread_id:?}"),
                worker_thread_name: thread.name().map(str::to_owned),
            },
            owns_worker_lane,
        )
    }

    fn acquire_worker_lane(&self, kind: ActivityWorkerKind) -> ActivityWorkerLane {
        let mut identities = self
            .identities
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // Logical lanes represent bounded active executor slots. Reusing an
        // inactive lane keeps identity independent from Tokio tasks and OS
        // threads while preventing overlapping slices on one lane.
        if let Some(state) = identities
            .worker_lanes
            .iter_mut()
            .find(|state| state.lane.kind == kind && !state.active)
        {
            state.active = true;
            return state.lane;
        }

        let lane = ActivityWorkerLane {
            id: identities.next_worker_lane_id,
            kind,
        };
        identities.next_worker_lane_id = identities.next_worker_lane_id.saturating_add(1);
        identities
            .worker_lanes
            .push(ActivityWorkerLaneState { lane, active: true });
        lane
    }

    fn release_worker_lane(
        identities: &Mutex<OperatorActivityIdentityState>,
        lane: ActivityWorkerLane,
    ) {
        let mut identities = identities.lock().unwrap_or_else(|error| error.into_inner());
        let state = identities
            .worker_lanes
            .iter_mut()
            .find(|state| state.lane == lane);
        debug_assert!(state.as_ref().is_some_and(|state| state.active));
        if let Some(state) = state {
            state.active = false;
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
    identities: Arc<Mutex<OperatorActivityIdentityState>>,
    active: ActiveOperatorActivitySpan,
}

struct ExecutionActivitySpanRecorder {
    stage: Option<OperationStageTrace>,
}

impl ExecutionActivitySpanRecorder {
    fn finish(mut self, result: &'static str, failed: bool) {
        let Some(stage) = self.stage.take() else {
            return;
        };
        let stage = stage.with_attribute("result", Value::String(result.to_owned()));
        if failed {
            stage.failed();
        } else {
            stage.completed();
        }
    }
}

impl Drop for ExecutionActivitySpanRecorder {
    fn drop(&mut self) {
        if let Some(stage) = self.stage.take() {
            drop(stage.with_attribute("result", Value::String("cancelled".to_owned())));
        }
    }
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
        drop(self.timeline_span.take());
        drop(self.process_span.take());
        if self.active.owns_worker_lane {
            OperatorActivityRecorder::release_worker_lane(
                &self.identities,
                self.active.worker_lane,
            );
        }
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
    // Instrument only the provider-owned output boundary. Name matching could
    // accidentally include an unrelated third-party plan with the same label.
    let records_delta_scan_output_wait = inner.as_any().is::<DeltaScanPlanningExec>();
    let plan: Arc<dyn ExecutionPlan> = Arc::new(ProfiledOperatorExec {
        inner,
        node_id,
        parent_node_id,
        activity: activity.clone(),
        records_delta_scan_output_wait,
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
    records_delta_scan_output_wait: bool,
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
            records_delta_scan_output_wait: self.records_delta_scan_output_wait,
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
                records_delta_scan_output_wait: self.records_delta_scan_output_wait,
                pending_delta_scan_output_wait: None,
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
    records_delta_scan_output_wait: bool,
    pending_delta_scan_output_wait: Option<ExecutionActivitySpanRecorder>,
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
        if self.records_delta_scan_output_wait {
            // One wait spans Pending through the first later Ready. A spurious
            // wake that polls Pending again keeps the same interval open.
            match &poll {
                Poll::Pending if self.pending_delta_scan_output_wait.is_none() => {
                    self.pending_delta_scan_output_wait = self
                        .activity
                        .start_delta_scan_output_wait(self.node_id, self.partition, self.stream_id);
                }
                Poll::Pending => {}
                Poll::Ready(Some(Ok(_))) => {
                    if let Some(span) = self.pending_delta_scan_output_wait.take() {
                        span.finish("ok", false);
                    }
                }
                Poll::Ready(Some(Err(_))) => {
                    if let Some(span) = self.pending_delta_scan_output_wait.take() {
                        span.finish("error", true);
                    }
                }
                Poll::Ready(None) => {
                    if let Some(span) = self.pending_delta_scan_output_wait.take() {
                        span.finish("ok", false);
                    }
                }
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
    use std::{
        collections::BTreeSet,
        error::Error,
        sync::{Arc, mpsc},
        time::{Duration, Instant},
    };

    use datafusion::{
        arrow::datatypes::Schema,
        common::DataFusionError,
        physical_plan::{
            collect,
            stream::{RecordBatchReceiverStreamBuilder, RecordBatchStreamAdapter},
        },
        prelude::SessionContext,
    };
    use futures_util::StreamExt;

    use crate::{
        QueryExecutionOutcome, QueryExecutionScope, TimelineSpanStatus, TimelineSpanTimeSemantics,
        observability::test_capture::TracingCapture,
        profiling::OperationTraceContext,
        query_engine::datafusion::{
            execution_profile::collect_query_execution_profile,
            test_support::register_fixture_source,
        },
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

    fn delayed_record_batch_stream(delay: Duration) -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::empty());
        let mut builder = RecordBatchReceiverStreamBuilder::new(Arc::clone(&schema), 1);
        let output = builder.tx();
        builder.spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = output.send(Ok(RecordBatch::new_empty(schema))).await;
            Ok(())
        });
        builder.build()
    }

    fn delayed_error_record_batch_stream(
        delay: Duration,
        message: &'static str,
    ) -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::empty());
        let mut builder = RecordBatchReceiverStreamBuilder::new(schema, 1);
        let output = builder.tx();
        builder.spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = output
                .send(Err(DataFusionError::Execution(message.to_owned())))
                .await;
            Ok(())
        });
        builder.build()
    }

    fn pending_record_batch_stream() -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::empty());
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures_util::stream::pending::<DataFusionResult<RecordBatch>>(),
        ))
    }

    fn delayed_closed_record_batch_stream(delay: Duration) -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::empty());
        let mut builder = RecordBatchReceiverStreamBuilder::new(schema, 1);
        let output = builder.tx();
        builder.spawn(async move {
            tokio::time::sleep(delay).await;
            drop(output);
            Ok(())
        });
        builder.build()
    }

    fn profiled_delta_scan_stream(
        activity: OperatorActivityRecorder,
        inner: SendableRecordBatchStream,
    ) -> Pin<Box<ProfiledRecordBatchStream>> {
        profiled_delta_scan_stream_on(activity, 2, 7, inner)
    }

    fn profiled_delta_scan_stream_on(
        activity: OperatorActivityRecorder,
        partition: usize,
        stream_id: u64,
        inner: SendableRecordBatchStream,
    ) -> Pin<Box<ProfiledRecordBatchStream>> {
        Box::pin(ProfiledRecordBatchStream {
            schema: inner.schema(),
            inner,
            operator_name: "DeltaScanPlanningExec".to_owned(),
            node_id: 4,
            parent_node_id: Some(3),
            partition,
            stream_id,
            activity,
            records_delta_scan_output_wait: true,
            pending_delta_scan_output_wait: None,
        })
    }

    fn collect_profiled_operators<'a>(
        plan: &'a dyn ExecutionPlan,
        operators: &mut Vec<&'a ProfiledOperatorExec>,
    ) {
        if let Some(operator) = plan.as_any().downcast_ref::<ProfiledOperatorExec>() {
            operators.push(operator);
        }
        for child in plan.children() {
            collect_profiled_operators(child.as_ref(), operators);
        }
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
    async fn worker_tracks_are_bounded_by_active_parallelism_not_task_count()
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
        assert!(!worker_thread_ids.is_empty());
        assert!(worker_thread_ids.len() <= 2);
        assert!(runtime_task_ids.len() > worker_lane_ids.len());
        assert!(spans.iter().all(|span| {
            span.attributes()["worker_kind"] == "runtime"
                && span.track_name().starts_with("DataFusion query ")
                && span.track_name().contains(" / worker ")
        }));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn logical_worker_lane_is_reused_across_os_threads() -> Result<(), Box<dyn Error>> {
        let timeline = OperationTimelineRecorder::start();
        let activity = OperatorActivityRecorder::new(test_trace_identity(timeline.clone()));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let first_activity = activity.clone();
        let first = tokio::spawn(async move {
            first_activity
                .start_span("WorkerExec", 0, None, 0, 1, "poll_next")
                .expect("first worker activity should fit")
                .finish("batch", false);
            let thread_id = format!("{:?}", std::thread::current().id());
            let _ = started_tx.send(thread_id);
            let _ = release_rx.recv_timeout(Duration::from_secs(5));
        });

        let first_thread_id = started_rx.await?;
        let second_activity = activity.clone();
        let second_thread_id = tokio::spawn(async move {
            second_activity
                .start_span("WorkerExec", 0, None, 0, 2, "poll_next")
                .expect("second worker activity should fit")
                .finish("batch", false);
            format!("{:?}", std::thread::current().id())
        })
        .await?;
        release_tx.send(())?;
        first.await?;

        assert_ne!(first_thread_id, second_thread_id);
        let timeline = timeline.finish("workers", TimelineSpanStatus::Completed);
        let spans = timeline
            .spans()
            .iter()
            .filter(|span| span.category() == OPERATOR_ACTIVITY_CATEGORY)
            .collect::<Vec<_>>();
        assert_eq!(spans.len(), 2);
        assert!(spans.iter().all(|span| {
            span.attributes()["worker_lane_id"] == 1
                && span.attributes()["worker_kind"] == "runtime"
        }));
        assert_eq!(
            spans
                .iter()
                .filter_map(|span| span.attributes()["worker_thread_id"].as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );

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
    async fn only_delta_scan_plan_marks_its_output_stream_for_wait_tracing()
    -> Result<(), Box<dyn Error>> {
        let context = SessionContext::new();
        let _table =
            register_fixture_source(&context, "orders", "delta-scan-output-wait-instrumentation")?;
        let dataframe = context.sql("select id from orders").await?;
        let plan = dataframe.create_physical_plan().await?;
        let timeline = OperationTimelineRecorder::start();
        let plan = instrument_query_execution_plan(plan, test_trace_identity(timeline))?;
        let mut operators = Vec::new();
        collect_profiled_operators(plan.as_ref(), &mut operators);

        let marked = operators
            .iter()
            .filter(|operator| operator.records_delta_scan_output_wait)
            .collect::<Vec<_>>();
        assert_eq!(marked.len(), 1);
        assert!(marked[0].inner.as_any().is::<DeltaScanPlanningExec>());
        assert!(operators.iter().all(|operator| {
            operator.records_delta_scan_output_wait
                == operator.inner.as_any().is::<DeltaScanPlanningExec>()
        }));

        Ok(())
    }

    #[tokio::test]
    async fn delta_scan_output_wait_records_three_second_pending_gap_in_both_outputs()
    -> Result<(), Box<dyn Error>> {
        const DELAY: Duration = Duration::from_secs(3);
        const MINIMUM_DELAY_MICROS: u64 = 2_800_000;
        const MAXIMUM_DELAY_MICROS: u64 = 5_000_000;

        let capture = TracingCapture::start_with_profile_spans_enabled();
        let timeline = OperationTimelineRecorder::start();
        let activity = OperatorActivityRecorder::new(test_trace_identity_with_limit(
            Some(timeline.clone()),
            true,
            100,
        ));
        let operation_id = activity.context.operation_id();
        let mut stream =
            profiled_delta_scan_stream(activity.clone(), delayed_record_batch_stream(DELAY));

        let started_at = Instant::now();
        let batch = stream.next().await.ok_or("expected delayed batch")??;
        assert_eq!(batch.num_rows(), 0);
        assert!(started_at.elapsed() >= DELAY);
        drop(stream);
        activity.context.record_process_result("ok");
        drop(activity);

        let timeline = timeline.finish("test", TimelineSpanStatus::Completed);
        let waits = timeline
            .spans()
            .iter()
            .filter(|span| span.category() == DELTA_SCAN_OUTPUT_WAIT_CATEGORY)
            .collect::<Vec<_>>();
        assert_eq!(waits.len(), 1);
        let wait = waits[0];
        assert_eq!(wait.name(), DELTA_SCAN_OUTPUT_WAIT_NAME);
        assert_eq!(
            wait.track_name(),
            "DataFusion query [1] / Delta scan output [7]"
        );
        assert_eq!(wait.parent_id(), None);
        assert_eq!(wait.status(), TimelineSpanStatus::Completed);
        assert_eq!(wait.time_semantics(), TimelineSpanTimeSemantics::WallClock);
        assert!(
            (MINIMUM_DELAY_MICROS..=MAXIMUM_DELAY_MICROS).contains(&wait.duration_micros()),
            "three-second wait recorded {} microseconds",
            wait.duration_micros()
        );
        assert_eq!(wait.attributes()["query_execution_id"], 1);
        assert_eq!(wait.attributes()["query_scope"], "preview");
        assert_eq!(wait.attributes()["node_id"], 4);
        assert_eq!(wait.attributes()["operator_partition"], 2);
        assert_eq!(wait.attributes()["execution_stream_id"], 7);
        assert_eq!(
            wait.attributes()["activity"],
            DELTA_SCAN_OUTPUT_WAIT_ACTIVITY
        );
        assert_eq!(wait.attributes()["result"], "ok");

        let spans = capture.captured().spans();
        let root = spans
            .iter()
            .find(|span| span.name == "Delta Funnel preview")
            .expect("operation root span should be captured");
        let process_waits = spans
            .iter()
            .filter(|span| span.name == "DataFusion execution activity")
            .collect::<Vec<_>>();
        assert_eq!(process_waits.len(), 1);
        let process_wait = process_waits[0];
        assert_eq!(process_wait.parent_id, Some(root.id));
        assert_eq!(
            process_wait.fields["operation_id"],
            operation_id.to_string()
        );
        assert_eq!(process_wait.fields["query_execution_id"], "1");
        assert_eq!(process_wait.fields["query_scope"], "preview");
        assert_eq!(
            process_wait.fields["execution_activity_name"],
            DELTA_SCAN_OUTPUT_WAIT_NAME
        );
        assert_eq!(process_wait.fields["node_id"], "4");
        assert_eq!(process_wait.fields["operator_partition"], "2");
        assert_eq!(process_wait.fields["execution_stream_id"], "7");
        assert_eq!(
            process_wait.fields["activity"],
            DELTA_SCAN_OUTPUT_WAIT_ACTIVITY
        );
        assert_eq!(process_wait.fields["result"], "ok");
        assert_eq!(process_wait.fields["time_semantics"], "wall_clock");
        assert_eq!(process_wait.enter_count, 0);
        assert_eq!(process_wait.exit_count, 0);
        assert!(process_wait.closed);

        Ok(())
    }

    #[tokio::test]
    async fn delta_scan_output_wait_reports_error_without_error_details()
    -> Result<(), Box<dyn Error>> {
        const SENSITIVE_ERROR: &str = "secret object path";

        let capture = TracingCapture::start_with_profile_spans_enabled();
        let timeline = OperationTimelineRecorder::start();
        let activity = OperatorActivityRecorder::new(test_trace_identity_with_limit(
            Some(timeline.clone()),
            true,
            100,
        ));
        let mut stream = profiled_delta_scan_stream(
            activity.clone(),
            delayed_error_record_batch_stream(Duration::from_millis(10), SENSITIVE_ERROR),
        );

        let error = stream
            .next()
            .await
            .ok_or("expected delayed error")?
            .expect_err("the delayed item should fail");
        assert!(error.to_string().contains(SENSITIVE_ERROR));
        drop(stream);
        activity.context.record_process_result("error");
        drop(activity);

        let timeline = timeline.finish("test", TimelineSpanStatus::Failed);
        let wait = timeline
            .spans()
            .iter()
            .find(|span| span.category() == DELTA_SCAN_OUTPUT_WAIT_CATEGORY)
            .ok_or("expected failed output wait")?;
        assert_eq!(wait.status(), TimelineSpanStatus::Failed);
        assert_eq!(wait.attributes()["result"], "error");
        assert!(
            wait.attributes()
                .values()
                .all(|value| !value.to_string().contains(SENSITIVE_ERROR))
        );

        let process_wait = capture
            .captured()
            .spans()
            .into_iter()
            .find(|span| span.name == "DataFusion execution activity")
            .ok_or("expected failed process wait")?;
        assert_eq!(process_wait.fields["result"], "error");
        assert!(
            process_wait
                .fields
                .values()
                .all(|value| !value.contains(SENSITIVE_ERROR))
        );

        Ok(())
    }

    #[tokio::test]
    async fn closed_delta_scan_output_producer_finishes_pending_wait_as_eof()
    -> Result<(), Box<dyn Error>> {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let timeline = OperationTimelineRecorder::start();
        let activity = OperatorActivityRecorder::new(test_trace_identity_with_limit(
            Some(timeline.clone()),
            true,
            100,
        ));
        let mut stream = profiled_delta_scan_stream(
            activity.clone(),
            delayed_closed_record_batch_stream(Duration::from_millis(10)),
        );

        assert!(stream.next().await.is_none());
        drop(stream);
        activity.context.record_process_result("ok");
        drop(activity);

        let timeline = timeline.finish("test", TimelineSpanStatus::Completed);
        let wait = timeline
            .spans()
            .iter()
            .find(|span| span.category() == DELTA_SCAN_OUTPUT_WAIT_CATEGORY)
            .ok_or("expected completed output wait")?;
        assert_eq!(wait.status(), TimelineSpanStatus::Completed);
        assert_eq!(wait.attributes()["result"], "ok");

        let process_wait = capture
            .captured()
            .spans()
            .into_iter()
            .find(|span| span.name == "DataFusion execution activity")
            .ok_or("expected completed process wait")?;
        assert_eq!(process_wait.fields["result"], "ok");
        assert_eq!(process_wait.enter_count, 0);
        assert_eq!(process_wait.exit_count, 0);
        assert!(process_wait.closed);

        Ok(())
    }

    #[test]
    fn dropping_pending_delta_scan_output_stream_cancels_both_outputs() -> Result<(), Box<dyn Error>>
    {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let timeline = OperationTimelineRecorder::start();
        let activity = OperatorActivityRecorder::new(test_trace_identity_with_limit(
            Some(timeline.clone()),
            true,
            100,
        ));
        let mut stream =
            profiled_delta_scan_stream(activity.clone(), pending_record_batch_stream());
        let waker = futures_util::task::noop_waker_ref();
        let mut poll_context = Context::from_waker(waker);

        assert!(matches!(
            stream.as_mut().poll_next(&mut poll_context),
            Poll::Pending
        ));
        drop(stream);
        activity.context.record_process_result("cancelled");
        drop(activity);

        let timeline = timeline.finish("test", TimelineSpanStatus::Cancelled);
        let wait = timeline
            .spans()
            .iter()
            .find(|span| span.category() == DELTA_SCAN_OUTPUT_WAIT_CATEGORY)
            .ok_or("expected cancelled output wait")?;
        assert_eq!(wait.status(), TimelineSpanStatus::Cancelled);
        assert_eq!(wait.attributes()["result"], "cancelled");

        let process_wait = capture
            .captured()
            .spans()
            .into_iter()
            .find(|span| span.name == "DataFusion execution activity")
            .ok_or("expected cancelled process wait")?;
        assert_eq!(process_wait.fields["result"], "cancelled");
        assert_eq!(process_wait.enter_count, 0);
        assert_eq!(process_wait.exit_count, 0);
        assert!(process_wait.closed);

        Ok(())
    }

    #[tokio::test]
    async fn concurrent_operations_keep_delta_scan_output_waits_isolated()
    -> Result<(), Box<dyn Error>> {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let first_timeline = OperationTimelineRecorder::start();
        let second_timeline = OperationTimelineRecorder::start();
        let first_activity = OperatorActivityRecorder::new(test_trace_identity_with_limit(
            Some(first_timeline.clone()),
            true,
            100,
        ));
        let second_activity = OperatorActivityRecorder::new(test_trace_identity_with_limit(
            Some(second_timeline.clone()),
            true,
            100,
        ));
        let operation_ids = [
            first_activity.context.operation_id(),
            second_activity.context.operation_id(),
        ];
        let mut first_stream = profiled_delta_scan_stream_on(
            first_activity.clone(),
            0,
            1,
            delayed_record_batch_stream(Duration::from_millis(20)),
        );
        let mut second_stream = profiled_delta_scan_stream_on(
            second_activity.clone(),
            0,
            1,
            delayed_record_batch_stream(Duration::from_millis(20)),
        );

        let (first, second) = tokio::join!(first_stream.next(), second_stream.next());
        first.ok_or("expected first operation batch")??;
        second.ok_or("expected second operation batch")??;
        drop(first_stream);
        drop(second_stream);
        first_activity.context.record_process_result("ok");
        second_activity.context.record_process_result("ok");
        drop(first_activity);
        drop(second_activity);

        for timeline in [first_timeline, second_timeline] {
            let timeline = timeline.finish("test", TimelineSpanStatus::Completed);
            let waits = timeline
                .spans()
                .iter()
                .filter(|span| span.category() == DELTA_SCAN_OUTPUT_WAIT_CATEGORY)
                .collect::<Vec<_>>();
            assert_eq!(waits.len(), 1);
            assert_eq!(waits[0].attributes()["query_execution_id"], 1);
            assert_eq!(waits[0].attributes()["execution_stream_id"], 1);
        }

        let process_wait_operation_ids = capture
            .captured()
            .spans()
            .iter()
            .filter(|span| span.name == "DataFusion execution activity")
            .map(|span| span.fields["operation_id"].parse::<u64>())
            .collect::<Result<BTreeSet<_>, _>>()?;
        assert_eq!(
            process_wait_operation_ids,
            operation_ids.into_iter().collect()
        );

        Ok(())
    }

    #[tokio::test]
    async fn parallel_delta_scan_partitions_keep_overlapping_waits_on_separate_tracks()
    -> Result<(), Box<dyn Error>> {
        let timeline = OperationTimelineRecorder::start();
        let activity = OperatorActivityRecorder::new(test_trace_identity(timeline.clone()));
        let mut first = profiled_delta_scan_stream_on(
            activity.clone(),
            0,
            11,
            delayed_record_batch_stream(Duration::from_millis(50)),
        );
        let mut second = profiled_delta_scan_stream_on(
            activity,
            1,
            12,
            delayed_record_batch_stream(Duration::from_millis(50)),
        );

        let (first_batch, second_batch) = tokio::join!(first.next(), second.next());
        first_batch.ok_or("expected first partition batch")??;
        second_batch.ok_or("expected second partition batch")??;
        drop(first);
        drop(second);

        let timeline = timeline.finish("test", TimelineSpanStatus::Completed);
        let waits = timeline
            .spans()
            .iter()
            .filter(|span| span.category() == DELTA_SCAN_OUTPUT_WAIT_CATEGORY)
            .collect::<Vec<_>>();
        assert_eq!(waits.len(), 2);
        assert_ne!(waits[0].track_name(), waits[1].track_name());
        assert_eq!(
            waits
                .iter()
                .map(|span| span.attributes()["operator_partition"].as_u64())
                .collect::<BTreeSet<_>>(),
            [Some(0), Some(1)].into_iter().collect()
        );
        let latest_start = waits
            .iter()
            .map(|span| span.start_offset_micros())
            .max()
            .ok_or("expected latest wait start")?;
        let earliest_end = waits
            .iter()
            .map(|span| {
                span.start_offset_micros()
                    .saturating_add(span.duration_micros())
            })
            .min()
            .ok_or("expected earliest wait end")?;
        assert!(latest_start < earliest_end);

        Ok(())
    }

    #[test]
    fn delta_scan_output_waits_share_one_operation_activity_budget_and_marker() {
        let timeline = OperationTimelineRecorder::start();
        let activity = OperatorActivityRecorder::new(test_trace_identity_with_limit(
            Some(timeline.clone()),
            false,
            1,
        ));

        activity
            .start_delta_scan_output_wait(4, 0, 1)
            .expect("first output wait should fit")
            .finish("ok", false);
        assert!(
            activity
                .start_span("FilterExec", 0, None, 0, 2, "poll_next")
                .is_none()
        );
        assert!(activity.start_delta_scan_output_wait(4, 1, 3).is_none());

        let timeline = timeline.finish("test", TimelineSpanStatus::Completed);
        assert_eq!(
            timeline
                .spans()
                .iter()
                .filter(|span| span.category() == DELTA_SCAN_OUTPUT_WAIT_CATEGORY)
                .count(),
            1
        );
        let markers = timeline
            .spans()
            .iter()
            .filter(|span| span.name() == "Operator activity trace truncated")
            .collect::<Vec<_>>();
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].attributes()["maximum_spans"], 1);
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
