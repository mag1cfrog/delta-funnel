//! Compile-time coverage for the public execution-profile API.

use std::sync::Arc;

use datafusion::{
    common::DataFusionError,
    execution::TaskContext,
    physical_plan::{ExecutionPlan, SendableRecordBatchStream},
};
use delta_funnel::{
    DeltaFunnelError, DeltaFunnelRuntime, DeltaFunnelSession, DeltaProviderReadStatsSnapshot,
    ExecutionProfileMode, MssqlWriteReport, OutputWritePlan, QueryExecutionMetric,
    QueryExecutionMetricCategory, QueryExecutionMetricValue, QueryExecutionOperatorProfile,
    QueryExecutionOutcome, QueryExecutionProfile, QueryExecutionScope,
    datafusion_query_output_stream, progress::ProgressReporter,
};
use serde_json::Value;

type QueryOutputStreamFn = fn(
    Arc<dyn ExecutionPlan>,
    Arc<TaskContext>,
) -> Result<SendableRecordBatchStream, DataFusionError>;

type RuntimeMssqlProfileFn = fn(
    &DeltaFunnelRuntime,
    &DeltaFunnelSession,
    &OutputWritePlan,
    ExecutionProfileMode,
) -> Result<MssqlWriteReport, DeltaFunnelError>;

type RuntimeMssqlProfileProgressFn = fn(
    &DeltaFunnelRuntime,
    &DeltaFunnelSession,
    &OutputWritePlan,
    ExecutionProfileMode,
    ProgressReporter,
) -> Result<MssqlWriteReport, DeltaFunnelError>;

#[allow(dead_code)]
async fn session_mssql_profile_route_compiles(
    session: &DeltaFunnelSession,
    request: &OutputWritePlan,
) -> Result<MssqlWriteReport, DeltaFunnelError> {
    session
        .write_to_mssql_with_profile_mode(request, ExecutionProfileMode::Detailed)
        .await
}

#[test]
fn execution_profile_types_and_accessors_are_exported_from_the_crate_root() {
    let _: QueryOutputStreamFn = datafusion_query_output_stream;
    let _: RuntimeMssqlProfileFn = DeltaFunnelRuntime::write_to_mssql_with_profile_mode;
    let _: RuntimeMssqlProfileProgressFn =
        DeltaFunnelRuntime::write_to_mssql_with_profile_mode_and_progress;

    let _: ExecutionProfileMode = ExecutionProfileMode::default();
    let _: for<'a> fn(&'a MssqlWriteReport) -> Option<&'a QueryExecutionProfile> =
        MssqlWriteReport::execution_profile;
    let _: fn(&QueryExecutionProfile) -> QueryExecutionScope = QueryExecutionProfile::scope;
    let _: fn(&QueryExecutionProfile) -> QueryExecutionOutcome = QueryExecutionProfile::outcome;
    let _: fn(&QueryExecutionProfile) -> bool = QueryExecutionProfile::partial;
    let _: fn(&QueryExecutionProfile) -> Option<u64> =
        QueryExecutionProfile::delta_funnel_row_limit;
    let _: for<'a> fn(&'a QueryExecutionProfile) -> &'a [QueryExecutionOperatorProfile] =
        QueryExecutionProfile::operators;
    let _: fn(&QueryExecutionProfile) -> Value = QueryExecutionProfile::to_json_value;

    let _: fn(&QueryExecutionOperatorProfile) -> u64 = QueryExecutionOperatorProfile::node_id;
    let _: fn(&QueryExecutionOperatorProfile) -> Option<u64> =
        QueryExecutionOperatorProfile::parent_node_id;
    let _: for<'a> fn(&'a QueryExecutionOperatorProfile) -> &'a str =
        QueryExecutionOperatorProfile::operator_name;
    let _: fn(&QueryExecutionOperatorProfile) -> u64 =
        QueryExecutionOperatorProfile::output_partition_count;
    let _: fn(&QueryExecutionOperatorProfile) -> bool =
        QueryExecutionOperatorProfile::metrics_available;
    let _: for<'a> fn(&'a QueryExecutionOperatorProfile) -> &'a [QueryExecutionMetric] =
        QueryExecutionOperatorProfile::aggregated_metrics;
    let _: for<'a> fn(&'a QueryExecutionOperatorProfile) -> &'a [QueryExecutionMetric] =
        QueryExecutionOperatorProfile::metrics;
    let _: for<'a> fn(
        &'a QueryExecutionOperatorProfile,
    ) -> Option<&'a DeltaProviderReadStatsSnapshot> =
        QueryExecutionOperatorProfile::delta_provider_read_stats;
    let _: fn(&QueryExecutionOperatorProfile) -> Value =
        QueryExecutionOperatorProfile::to_json_value;

    let _: for<'a> fn(&'a QueryExecutionMetric) -> &'a str = QueryExecutionMetric::name;
    let _: fn(&QueryExecutionMetric) -> QueryExecutionMetricCategory =
        QueryExecutionMetric::category;
    let _: fn(&QueryExecutionMetric) -> Option<u64> = QueryExecutionMetric::partition;
    let _: fn(&QueryExecutionMetric) -> Option<u64> = QueryExecutionMetric::output_partition;
    let _: for<'a> fn(&'a QueryExecutionMetric) -> &'a QueryExecutionMetricValue =
        QueryExecutionMetric::value;
    let _: fn(&QueryExecutionMetric) -> Value = QueryExecutionMetric::to_json_value;
    let _: for<'a> fn(&'a QueryExecutionMetricValue) -> &'static str =
        QueryExecutionMetricValue::value_kind;

    assert_eq!(QueryExecutionScope::Preview.as_str(), "preview");
}
