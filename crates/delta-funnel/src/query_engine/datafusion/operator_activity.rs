//! Wall-clock activity spans for finalized DataFusion physical plans.

use std::{
    cell::RefCell,
    collections::HashMap,
    fmt,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use crate::{
    QueryExecutionScope,
    profiling::{OperationStageTrace, OperationTraceContext},
    usize_to_u64_saturating,
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

use super::{QueryTraceIdentity, execution::DeltaScanPlanningExec};

#[cfg(feature = "perfetto-profile")]
mod task_tracing;

#[cfg(feature = "perfetto-profile")]
use task_tracing::DataFusionTaskTraceContext;
#[cfg(feature = "perfetto-profile")]
pub(crate) use task_tracing::{
    current_datafusion_object_store_transport_span, initialize_datafusion_task_tracing,
};

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
}

#[derive(Debug, Clone)]
struct ActiveOperatorActivitySpan {
    operation_id: u64,
    query_execution_id: u64,
    worker_lane: ActivityWorkerLane,
    owns_worker_lane: bool,
    #[cfg(feature = "perfetto-profile")]
    task_trace_context: DataFusionTaskTraceContext,
}

impl ActiveOperatorActivitySpan {
    fn same_scope(&self, other: &Self) -> bool {
        self.operation_id == other.operation_id
            && self.query_execution_id == other.query_execution_id
            && self.worker_lane == other.worker_lane
            && self.owns_worker_lane == other.owns_worker_lane
    }
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
        operator_name: impl Into<Arc<str>>,
        node_id: u64,
        parent_node_id: Option<u64>,
        partition: usize,
        stream_id: u64,
        activity: &'static str,
    ) -> Option<OperatorActivitySpanRecorder> {
        let operator_name = operator_name.into();
        let (context, owns_worker_lane) = self.execution_context(partition, true);
        let process_parent_active = ACTIVE_OPERATOR_ACTIVITY_SPANS.with(|active| {
            let active = active.borrow();
            let matches_parent = |parent: &ActiveOperatorActivitySpan| {
                parent.operation_id == self.context.operation_id()
                    && parent.query_execution_id == self.query_execution_id
                    && parent.worker_lane.id == context.worker_lane.id
            };
            active.last().is_some_and(matches_parent)
        });
        let records_detail = match self.context.reserve_operator_activity() {
            Ok(()) => true,
            Err(limit) => {
                if limit.should_report {
                    self.report_truncation(limit.maximum_spans);
                }
                false
            }
        };
        // Once detailed spans are capped, keep only the outermost activity on
        // each executor task so sampled stacks retain query and worker identity.
        let records_process_span = records_detail || !process_parent_active;
        if !records_process_span {
            if owns_worker_lane {
                Self::release_worker_lane(&self.identities, context.worker_lane);
            }
            return None;
        }
        let process_span = self.process_operator_span(
            operator_name.as_ref(),
            node_id,
            parent_node_id,
            partition,
            stream_id,
            activity,
            &context,
            process_parent_active,
        );
        let active = ActiveOperatorActivitySpan {
            operation_id: self.context.operation_id(),
            query_execution_id: self.query_execution_id,
            worker_lane: context.worker_lane,
            owns_worker_lane,
            #[cfg(feature = "perfetto-profile")]
            task_trace_context: DataFusionTaskTraceContext::new(
                self.clone(),
                operator_name,
                node_id,
                parent_node_id,
                partition,
                stream_id,
            ),
        };
        ACTIVE_OPERATOR_ACTIVITY_SPANS.with(|spans| spans.borrow_mut().push(active.clone()));
        Some(OperatorActivitySpanRecorder {
            process_span: Some(process_span),
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
        if let Err(limit) = self.context.reserve_operator_activity() {
            if limit.should_report {
                self.report_truncation(limit.maximum_spans);
            }
            return None;
        }

        let parent = self.context.process_root_span();
        let span = tracing::trace_span!(
            target: crate::profiling::PROFILE_TARGET,
            parent: parent,
            "DataFusion execution activity",
            operation_id = self.context.operation_id(),
            query_execution_id = self.query_execution_id,
            query_scope = self.query_scope.as_str(),
            query_owner = self.query_owner.as_deref(),
            execution_activity_name = DELTA_SCAN_OUTPUT_WAIT_NAME,
            node_id,
            operator_partition = usize_to_u64_saturating(partition),
            execution_stream_id = stream_id,
            activity = DELTA_SCAN_OUTPUT_WAIT_ACTIVITY,
            result = tracing::field::Empty,
            time_semantics = "wall_clock",
        );
        let stage = OperationStageTrace::from_process_span((span, parent.clone()));
        Some(ExecutionActivitySpanRecorder { stage: Some(stage) })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the process span records the canonical operator activity identity"
    )]
    fn process_operator_span(
        &self,
        operator_name: &str,
        node_id: u64,
        parent_node_id: Option<u64>,
        partition: usize,
        stream_id: u64,
        activity: &'static str,
        context: &ActivityExecutionContext,
        process_parent_active: bool,
    ) -> tracing::Span {
        let operation_root = self.context.process_root_span();
        let current = tracing::Span::current();
        let parent = if process_parent_active {
            &current
        } else {
            operation_root
        };
        let span = tracing::trace_span!(
            target: crate::profiling::PROFILE_TARGET,
            parent: parent,
            "DataFusion operator activity",
            operation_id = self.context.operation_id(),
            query_execution_id = self.query_execution_id,
            query_scope = self.query_scope.as_str(),
            query_owner = self.query_owner.as_deref(),
            operator_name,
            worker_lane_id = context.worker_lane.id,
            worker_kind = context.worker_lane.kind.as_str(),
            node_id,
            parent_node_id = tracing::field::Empty,
            operator_partition = usize_to_u64_saturating(partition),
            execution_stream_id = stream_id,
            activity,
            result = tracing::field::Empty,
            time_semantics = "active",
        );
        if let Some(parent_node_id) = parent_node_id {
            span.record("parent_node_id", parent_node_id);
        }
        span
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

    fn execution_context(
        &self,
        partition: usize,
        allow_coordinator: bool,
    ) -> (ActivityExecutionContext, bool) {
        let is_runtime_task = tokio::task::try_id().is_some();
        let inherited_worker_lane = ACTIVE_OPERATOR_ACTIVITY_SPANS.with(|active| {
            active.borrow().iter().rev().find_map(|parent| {
                (parent.operation_id == self.context.operation_id()
                    && parent.query_execution_id == self.query_execution_id)
                    .then_some(parent.worker_lane)
            })
        });
        let (worker_lane, owns_worker_lane) = match inherited_worker_lane {
            Some(worker_lane) => (worker_lane, false),
            None if allow_coordinator && !is_runtime_task && partition == 0 => (
                ActivityWorkerLane {
                    id: 0,
                    kind: ActivityWorkerKind::Coordinator,
                },
                false,
            ),
            None => {
                let kind = if is_runtime_task {
                    ActivityWorkerKind::Runtime
                } else {
                    ActivityWorkerKind::External
                };
                (self.acquire_worker_lane(kind), true)
            }
        };
        (ActivityExecutionContext { worker_lane }, owns_worker_lane)
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
        let root = self.context.process_root_span();
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

struct OperatorActivitySpanRecorder {
    process_span: Option<tracing::Span>,
    process_result_recorded: bool,
    identities: Arc<Mutex<OperatorActivityIdentityState>>,
    active: ActiveOperatorActivitySpan,
}

struct ExecutionActivitySpanRecorder {
    stage: Option<OperationStageTrace>,
}

impl ExecutionActivitySpanRecorder {
    fn finish(mut self, _result: &'static str, failed: bool) {
        let Some(stage) = self.stage.take() else {
            return;
        };
        if failed {
            stage.failed();
        } else {
            stage.completed();
        }
    }
}

impl Drop for ExecutionActivitySpanRecorder {
    fn drop(&mut self) {
        drop(self.stage.take());
    }
}

impl OperatorActivitySpanRecorder {
    fn in_process_scope<T>(&self, operation: impl FnOnce() -> T) -> T {
        match &self.process_span {
            Some(span) => span.in_scope(operation),
            None => operation(),
        }
    }

    fn finish(mut self, result: &'static str) {
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
            debug_assert!(
                popped
                    .as_ref()
                    .is_some_and(|popped| popped.same_scope(&self.active))
            );
        });
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
    let records_delta_scan_output_wait = inner.is::<DeltaScanPlanningExec>();
    let operator_name = Arc::<str>::from(inner.name());
    let plan: Arc<dyn ExecutionPlan> = Arc::new(ProfiledOperatorExec {
        inner,
        operator_name,
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
    plan.downcast_ref::<ProfiledOperatorExec>()
        .map_or(plan, |profiled| profiled.inner.as_ref())
}

#[derive(Debug)]
struct ProfiledOperatorExec {
    inner: Arc<dyn ExecutionPlan>,
    operator_name: Arc<str>,
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
        &self.operator_name
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
            operator_name: Arc::clone(&self.operator_name),
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
            Arc::clone(&self.operator_name),
            self.node_id,
            self.parent_node_id,
            partition,
            stream_id,
            "execute",
        );
        let result = match &span {
            Some(span) => span.in_process_scope(|| self.inner.execute(partition, context)),
            None => self.inner.execute(partition, context),
        };
        if let Some(span) = span {
            span.finish(if result.is_ok() { "stream" } else { "error" });
        }
        result.map(|inner| {
            Box::pin(ProfiledRecordBatchStream {
                schema: inner.schema(),
                inner,
                operator_name: Arc::clone(&self.operator_name),
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
    operator_name: Arc<str>,
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
            Arc::clone(&self.operator_name),
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
            let result = match &poll {
                Poll::Pending => "pending",
                Poll::Ready(Some(Ok(_))) => "batch",
                Poll::Ready(Some(Err(_))) => "error",
                Poll::Ready(None) => "eof",
            };
            span.finish(result);
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
    use crate::{
        QueryExecutionScope, observability::test_capture::TracingCapture,
        profiling::OperationTraceContext,
    };

    use super::*;

    fn test_activity(maximum_spans: u64) -> OperatorActivityRecorder {
        let context =
            OperationTraceContext::start_for_test_with_operator_activity_limit(true, maximum_spans)
                .expect("profile tracing should start");
        let identity =
            QueryTraceIdentity::new(context, QueryExecutionScope::Preview, Some("preview query"))
                .expect("query identity should be available");
        OperatorActivityRecorder::new(identity)
    }

    #[test]
    fn operator_activity_records_identity_and_nested_parentage() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let activity = test_activity(10);
        let parent = activity
            .start_span("ProjectionExec", 1, None, 0, 1, "execute")
            .expect("parent activity should start");
        parent.in_process_scope(|| {
            activity
                .start_span("FilterExec", 2, Some(1), 0, 1, "poll_next")
                .expect("child activity should start")
                .finish("batch");
        });
        parent.finish("stream");
        activity.context.record_process_result("ok");
        drop(activity);

        let spans = capture.captured().spans();
        let operators = spans
            .iter()
            .filter(|span| span.name == "DataFusion operator activity")
            .collect::<Vec<_>>();
        assert_eq!(operators.len(), 2);
        assert_eq!(operators[0].fields["operator_name"], "ProjectionExec");
        assert_eq!(operators[0].fields["worker_lane_id"], "0");
        assert_eq!(operators[0].fields["result"], "stream");
        assert_eq!(operators[1].parent_id, Some(operators[0].id));
        assert_eq!(operators[1].fields["operator_name"], "FilterExec");
        assert_eq!(operators[1].fields["parent_node_id"], "1");
        assert_eq!(operators[1].fields["result"], "batch");
    }

    #[test]
    fn activity_budget_reports_one_truncation_event() {
        let capture = TracingCapture::start_with_profile_spans_enabled();
        let activity = test_activity(1);
        let parent = activity
            .start_span("ProjectionExec", 1, None, 0, 1, "execute")
            .expect("parent activity should consume the budget");
        parent.in_process_scope(|| {
            assert!(
                activity
                    .start_span("FilterExec", 2, Some(1), 0, 1, "poll_next")
                    .is_none()
            );
            assert!(
                activity
                    .start_span("FilterExec", 2, Some(1), 0, 1, "poll_next")
                    .is_none()
            );
        });
        parent.finish("stream");

        let truncations = capture
            .captured()
            .events()
            .into_iter()
            .filter(|event| event.name == "Operator activity trace truncated")
            .collect::<Vec<_>>();
        assert_eq!(truncations.len(), 1);
        assert_eq!(truncations[0].fields["maximum_spans"], "1");
    }
}
