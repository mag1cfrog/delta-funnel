//! DataFusion task tracing for Perfetto diagnostics.

use std::{
    any::Any,
    sync::{Arc, Mutex},
};

use datafusion::common::runtime::{JoinSetTracer, JoinSetTracerError, set_join_set_tracer};
use futures_util::{FutureExt, future::BoxFuture, future::poll_fn};

use crate::{
    profiling::{OBJECT_STORE_TRANSPORT_ACTIVITY, OBJECT_STORE_TRANSPORT_CONTEXT_NAME},
    usize_to_u64_saturating,
};

use super::{
    ACTIVE_OPERATOR_ACTIVITY_SPANS, ActiveOperatorActivitySpan, OperatorActivityIdentityState,
    OperatorActivityRecorder,
};

#[derive(Debug, Clone)]
pub(super) struct DataFusionTaskTraceContext {
    activity: OperatorActivityRecorder,
    operator_name: Arc<str>,
    node_id: u64,
    parent_node_id: Option<u64>,
    partition: usize,
    stream_id: u64,
}

impl DataFusionTaskTraceContext {
    pub(super) fn new(
        activity: OperatorActivityRecorder,
        operator_name: Arc<str>,
        node_id: u64,
        parent_node_id: Option<u64>,
        partition: usize,
        stream_id: u64,
    ) -> Self {
        Self {
            activity,
            operator_name,
            node_id,
            parent_node_id,
            partition,
            stream_id,
        }
    }

    fn process_span(&self) -> tracing::Span {
        tracing::trace_span!(
            target: crate::profiling::PROFILE_TARGET,
            parent: None,
            "DataFusion task context",
            operation_id = self.activity.context.operation_id(),
            query_execution_id = self.activity.query_execution_id,
            query_scope = self.activity.query_scope.as_str(),
            query_owner = self.activity.query_owner.as_deref(),
            operator_name = self.operator_name.as_ref(),
            worker_lane_id = tracing::field::Empty,
            worker_kind = tracing::field::Empty,
            node_id = self.node_id,
            parent_node_id = tracing::field::Empty,
            operator_partition = usize_to_u64_saturating(self.partition),
            execution_stream_id = self.stream_id,
            activity = "spawned_task",
            time_semantics = "active",
        )
    }

    fn in_process_scope<T>(&self, span: &tracing::Span, operation: impl FnOnce() -> T) -> T {
        let (context, owns_worker_lane) = self.activity.execution_context(self.partition, false);
        span.record("worker_lane_id", context.worker_lane.id);
        span.record("worker_kind", context.worker_lane.kind.as_str());
        if let Some(parent_node_id) = self.parent_node_id {
            span.record("parent_node_id", parent_node_id);
        }
        let active = ActiveOperatorActivitySpan {
            operation_id: self.activity.context.operation_id(),
            query_execution_id: self.activity.query_execution_id,
            worker_lane: context.worker_lane,
            owns_worker_lane,
            timeline_span_id: None,
            process_span_active: true,
            task_trace_context: Some(self.clone()),
        };
        ACTIVE_OPERATOR_ACTIVITY_SPANS.with(|spans| spans.borrow_mut().push(active.clone()));
        let guard = DataFusionTaskScope {
            identities: Arc::clone(&self.activity.identities),
            active,
        };
        let result = span.in_scope(operation);
        drop(guard);
        result
    }

    fn object_store_transport_span(&self) -> tracing::Span {
        tracing::trace_span!(
            target: crate::profiling::PROFILE_TARGET,
            parent: None,
            OBJECT_STORE_TRANSPORT_CONTEXT_NAME,
            operation_id = self.activity.context.operation_id(),
            query_execution_id = self.activity.query_execution_id,
            execution_stream_id = self.stream_id,
            activity = OBJECT_STORE_TRANSPORT_ACTIVITY,
        )
    }
}

struct DataFusionTaskScope {
    identities: Arc<Mutex<OperatorActivityIdentityState>>,
    active: ActiveOperatorActivitySpan,
}

impl Drop for DataFusionTaskScope {
    fn drop(&mut self) {
        let _ = ACTIVE_OPERATOR_ACTIVITY_SPANS.try_with(|spans| {
            let popped = spans.borrow_mut().pop();
            debug_assert!(
                popped
                    .as_ref()
                    .is_some_and(|popped| popped.same_scope(&self.active))
            );
        });
        if self.active.owns_worker_lane {
            OperatorActivityRecorder::release_worker_lane(
                &self.identities,
                self.active.worker_lane,
            );
        }
    }
}

fn current_datafusion_task_trace_context() -> Option<DataFusionTaskTraceContext> {
    ACTIVE_OPERATOR_ACTIVITY_SPANS
        .try_with(|spans| {
            spans
                .borrow()
                .last()
                .and_then(|active| active.task_trace_context.clone())
        })
        .ok()
        .flatten()
}

pub(crate) fn current_datafusion_object_store_transport_span() -> Option<tracing::Span> {
    current_datafusion_task_trace_context().map(|context| context.object_store_transport_span())
}

pub(super) struct DeltaFunnelDataFusionTaskTracer;

impl JoinSetTracer for DeltaFunnelDataFusionTaskTracer {
    fn trace_future(
        &self,
        mut future: BoxFuture<'static, Box<dyn Any + Send>>,
    ) -> BoxFuture<'static, Box<dyn Any + Send>> {
        let Some(context) = current_datafusion_task_trace_context() else {
            return future;
        };
        let span = context.process_span();
        poll_fn(move |poll_context| {
            context.in_process_scope(&span, || future.as_mut().poll(poll_context))
        })
        .boxed()
    }

    fn trace_block(
        &self,
        operation: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> {
        let Some(context) = current_datafusion_task_trace_context() else {
            return operation;
        };
        let span = context.process_span();
        Box::new(move || context.in_process_scope(&span, operation))
    }
}

pub(super) static DATAFUSION_TASK_TRACER: DeltaFunnelDataFusionTaskTracer =
    DeltaFunnelDataFusionTaskTracer;

pub(crate) fn initialize_datafusion_task_tracing() -> Result<(), JoinSetTracerError> {
    set_join_set_tracer(&DATAFUSION_TASK_TRACER)
}
