//! Profiled DataFusion query output execution.

use std::sync::Arc;

use datafusion::{execution::TaskContext, physical_plan::ExecutionPlan};

use super::{
    DFQueryExecution, DFQueryExecutionSetupError, QueryTraceIdentity,
    execute_datafusion_query_output, instrument_query_execution_plan,
    prepare_datafusion_query_output,
};

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
