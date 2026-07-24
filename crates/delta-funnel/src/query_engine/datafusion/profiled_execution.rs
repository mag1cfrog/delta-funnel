//! Profiled DataFusion query output execution.

use std::sync::Arc;

use datafusion::{execution::TaskContext, physical_plan::ExecutionPlan};

use crate::{
    QueryExecutionScope, profiling::OperationTraceContext, report::OperationTimelineRecorder,
};

use super::{
    DFQueryExecution, DFQueryExecutionSetupError, execute_datafusion_query_output,
    instrument_query_execution_plan, prepare_datafusion_query_output,
};

/// Shared identity for the planning and execution events of one query.
#[derive(Debug, Clone)]
pub(crate) struct QueryTraceIdentity {
    pub(super) context: OperationTraceContext,
    pub(super) query_execution_id: u64,
    pub(super) query_scope: QueryExecutionScope,
    pub(super) query_owner: Option<Arc<str>>,
}

impl QueryTraceIdentity {
    pub(crate) fn new(
        context: OperationTraceContext,
        query_scope: QueryExecutionScope,
        query_owner: Option<&str>,
    ) -> Option<Self> {
        debug_assert_ne!(context.operation_id(), 0);
        debug_assert!(context.timeline().is_some() || context.process_spans_enabled());
        let query_execution_id = context.next_query_execution_id()?;
        Some(Self {
            context,
            query_execution_id,
            query_scope,
            query_owner: query_owner.map(Arc::<str>::from),
        })
    }

    pub(super) const fn timeline(&self) -> Option<&OperationTimelineRecorder> {
        self.context.timeline()
    }

    pub(super) const fn operation_id(&self) -> u64 {
        self.context.operation_id()
    }

    pub(super) fn process_root_span(&self) -> Option<&tracing::Span> {
        self.context.process_root_span()
    }

    pub(super) const fn query_execution_id(&self) -> u64 {
        self.query_execution_id
    }

    pub(super) const fn query_scope(&self) -> QueryExecutionScope {
        self.query_scope
    }

    pub(super) fn query_owner(&self) -> Option<&str> {
        self.query_owner.as_deref()
    }
}

pub(crate) fn profiled_datafusion_query_output_stream_with_effective_root(
    plan: Arc<dyn ExecutionPlan>,
    task_context: Arc<TaskContext>,
    trace_identity: QueryTraceIdentity,
) -> Result<DFQueryExecution, DFQueryExecutionSetupError> {
    let (effective_profile_root, execute) = prepare_datafusion_query_output(plan);
    let effective_profile_root =
        instrument_query_execution_plan(Arc::clone(&effective_profile_root), trace_identity)
            .map_err(|source| DFQueryExecutionSetupError {
                source,
                effective_profile_root,
            })?;
    execute_datafusion_query_output(effective_profile_root, execute, task_context)
}
