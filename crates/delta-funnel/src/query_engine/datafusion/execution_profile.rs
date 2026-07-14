use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    sync::{Arc, OnceLock},
};

use datafusion::physical_plan::{
    ExecutionPlan,
    metrics::{Metric, MetricType, MetricValue, MetricsSet},
};

use crate::{
    DeltaProviderReadStatsSnapshot, QueryExecutionMetric, QueryExecutionMetricCategory,
    QueryExecutionMetricValue, QueryExecutionOperatorProfile, QueryExecutionOutcome,
    QueryExecutionProfile, QueryExecutionScope, usize_to_u64_saturating,
};

use super::{DeltaProviderReadStatsHandle, execution::DeltaScanPlanningExec};

pub(crate) type DeltaProviderReadStatsSnapshotSet =
    Vec<(DeltaProviderReadStatsHandle, DeltaProviderReadStatsSnapshot)>;

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct QueryExecutionProfileResult {
    profile: Arc<OnceLock<QueryExecutionProfile>>,
}

#[allow(dead_code)]
impl QueryExecutionProfileResult {
    pub(crate) fn profile(&self) -> Option<&QueryExecutionProfile> {
        self.profile.get()
    }
}

#[allow(dead_code)]
pub(crate) struct QueryExecutionProfileConsumer {
    root: Arc<dyn ExecutionPlan>,
    scope: QueryExecutionScope,
    delta_funnel_row_limit: Option<u64>,
    result: QueryExecutionProfileResult,
}

#[allow(dead_code)]
impl QueryExecutionProfileConsumer {
    pub(crate) fn register(
        root: Arc<dyn ExecutionPlan>,
        scope: QueryExecutionScope,
        delta_funnel_row_limit: Option<u64>,
    ) -> (Self, QueryExecutionProfileResult) {
        let result = QueryExecutionProfileResult {
            profile: Arc::new(OnceLock::new()),
        };
        (
            Self {
                root,
                scope,
                delta_funnel_row_limit,
                result: result.clone(),
            },
            result,
        )
    }

    pub(crate) fn consume_terminal(
        self,
        outcome: QueryExecutionOutcome,
        terminal_provider_snapshots: &DeltaProviderReadStatsSnapshotSet,
    ) {
        let Self {
            root,
            scope,
            delta_funnel_row_limit,
            result,
        } = self;
        let profile = collect_query_execution_profile(
            &root,
            scope,
            outcome,
            delta_funnel_row_limit.unwrap_or_default(),
            Some(terminal_provider_snapshots),
        );
        drop(root);
        let profile = result.profile.get_or_init(|| profile);
        crate::observability::query_execution_profile_terminal(profile);
    }
}

#[allow(dead_code)]
pub(crate) fn delta_provider_read_stats_snapshot_set(
    handles: &[DeltaProviderReadStatsHandle],
    snapshots: &[DeltaProviderReadStatsSnapshot],
) -> DeltaProviderReadStatsSnapshotSet {
    let mut seen = HashSet::new();

    handles
        .iter()
        .zip(snapshots)
        .filter(|(handle, _)| seen.insert(handle_identity(handle)))
        .map(|(handle, snapshot)| (Arc::clone(handle), snapshot.clone()))
        .collect()
}

#[allow(dead_code)]
pub(crate) fn collect_query_execution_profile(
    root: &Arc<dyn ExecutionPlan>,
    scope: QueryExecutionScope,
    outcome: QueryExecutionOutcome,
    delta_funnel_row_limit: u64,
    terminal_provider_snapshots: Option<&DeltaProviderReadStatsSnapshotSet>,
) -> QueryExecutionProfile {
    let supplied_snapshots = terminal_provider_snapshots.map(|snapshots| {
        let mut supplied = HashMap::new();
        for (handle, snapshot) in snapshots {
            supplied.entry(handle_identity(handle)).or_insert(snapshot);
        }
        supplied
    });
    let mut fallback_snapshots = HashMap::new();
    let mut seen = HashSet::new();
    let mut stack = vec![(Arc::clone(root), None)];
    let mut operators = Vec::new();

    while let Some((plan, parent_node_id)) = stack.pop() {
        if !seen.insert(plan_identity(&plan)) {
            continue;
        }

        let node_id = usize_to_u64_saturating(operators.len());
        let (metrics_available, aggregated_metrics, metrics) = match plan.metrics() {
            Some(metrics) => {
                let (aggregated, raw) = collect_query_execution_metrics(&metrics);
                (true, aggregated, raw)
            }
            None => (false, Vec::new(), Vec::new()),
        };
        let provider_snapshot = provider_snapshot(
            plan.as_ref(),
            node_id,
            supplied_snapshots.as_ref(),
            &mut fallback_snapshots,
        );

        operators.push(QueryExecutionOperatorProfile::new(
            node_id,
            parent_node_id,
            plan.name(),
            usize_to_u64_saturating(plan.properties().output_partitioning().partition_count()),
            metrics_available,
            aggregated_metrics,
            metrics,
            provider_snapshot,
        ));

        let children = plan
            .children()
            .into_iter()
            .map(Arc::clone)
            .collect::<Vec<_>>();
        stack.extend(
            children
                .into_iter()
                .rev()
                .map(|child| (child, Some(node_id))),
        );
    }

    match scope {
        QueryExecutionScope::Preview => {
            QueryExecutionProfile::preview(outcome, delta_funnel_row_limit, operators)
        }
        QueryExecutionScope::MssqlOutput => QueryExecutionProfile::mssql_output(outcome, operators),
        QueryExecutionScope::WriteAllCacheAlias => {
            QueryExecutionProfile::write_all_cache_alias(outcome, operators)
        }
    }
}

fn provider_snapshot(
    plan: &dyn ExecutionPlan,
    node_id: u64,
    supplied_snapshots: Option<&HashMap<usize, &DeltaProviderReadStatsSnapshot>>,
    fallback_snapshots: &mut HashMap<usize, DeltaProviderReadStatsSnapshot>,
) -> Option<DeltaProviderReadStatsSnapshot> {
    let scan = plan.as_any().downcast_ref::<DeltaScanPlanningExec>()?;
    let handle = scan.read_stats_handle();
    let identity = handle_identity(&handle);

    if let Some(supplied_snapshots) = supplied_snapshots {
        let snapshot = supplied_snapshots.get(&identity).copied().cloned();
        if snapshot.is_none() {
            tracing::debug!(
                target: "delta_funnel",
                telemetry_event = "query_execution_profile_provider_snapshot_missing",
                node_id,
                operator_name = plan.name(),
                "Delta scan profile omitted a missing terminal provider snapshot"
            );
        }
        return snapshot;
    }

    if let Some(snapshot) = fallback_snapshots.get(&identity) {
        return Some(snapshot.clone());
    }

    let snapshot = handle.snapshot();
    fallback_snapshots.insert(identity, snapshot.clone());
    Some(snapshot)
}

fn plan_identity(plan: &Arc<dyn ExecutionPlan>) -> usize {
    Arc::as_ptr(plan) as *const () as usize
}

fn handle_identity(handle: &DeltaProviderReadStatsHandle) -> usize {
    Arc::as_ptr(handle) as *const () as usize
}

pub(super) fn collect_query_execution_metrics(
    metrics: &MetricsSet,
) -> (Vec<QueryExecutionMetric>, Vec<QueryExecutionMetric>) {
    let aggregated_metrics = metrics.aggregate_by_name();

    (
        convert_metrics(&aggregated_metrics, true),
        convert_metrics(metrics, false),
    )
}

fn convert_metrics(metrics: &MetricsSet, aggregated: bool) -> Vec<QueryExecutionMetric> {
    let mut converted = metrics
        .iter()
        .enumerate()
        .map(|(position, metric)| (position, convert_metric(metric.as_ref(), aggregated)))
        .collect::<Vec<_>>();

    converted.sort_by(|(left_position, left), (right_position, right)| {
        metric_category_rank(left.category())
            .cmp(&metric_category_rank(right.category()))
            .then_with(|| left.name().cmp(right.name()))
            .then_with(|| left.partition().cmp(&right.partition()))
            .then_with(|| left.output_partition().cmp(&right.output_partition()))
            .then_with(|| left.value().value_kind().cmp(right.value().value_kind()))
            .then_with(|| compare_metric_values(left.value(), right.value()))
            .then_with(|| left_position.cmp(right_position))
    });

    converted.into_iter().map(|(_, metric)| metric).collect()
}

fn convert_metric(metric: &Metric, aggregated: bool) -> QueryExecutionMetric {
    QueryExecutionMetric::new(
        metric.value().name(),
        match metric.metric_type() {
            MetricType::SUMMARY => QueryExecutionMetricCategory::Summary,
            MetricType::DEV => QueryExecutionMetricCategory::Dev,
        },
        if aggregated {
            None
        } else {
            metric.partition().map(usize_to_u64_saturating)
        },
        if aggregated {
            None
        } else {
            output_partition(metric)
        },
        convert_metric_value(metric.value()),
    )
}

fn output_partition(metric: &Metric) -> Option<u64> {
    metric
        .labels()
        .iter()
        .filter(|label| label.name() == "outputPartition")
        .find_map(|label| parse_output_partition(label.value()))
}

fn parse_output_partition(value: &str) -> Option<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    value.parse().ok()
}

fn convert_metric_value(value: &MetricValue) -> QueryExecutionMetricValue {
    match value {
        MetricValue::OutputRows(value)
        | MetricValue::OutputBatches(value)
        | MetricValue::SpillCount(value)
        | MetricValue::SpilledRows(value)
        | MetricValue::Count { count: value, .. } => {
            QueryExecutionMetricValue::Count(usize_to_u64_saturating(value.value()))
        }
        MetricValue::SpilledBytes(value) | MetricValue::OutputBytes(value) => {
            QueryExecutionMetricValue::Bytes(usize_to_u64_saturating(value.value()))
        }
        MetricValue::CurrentMemoryUsage(value) => {
            QueryExecutionMetricValue::Bytes(usize_to_u64_saturating(value.value()))
        }
        MetricValue::ElapsedCompute(value) | MetricValue::Time { time: value, .. } => {
            QueryExecutionMetricValue::Nanoseconds(usize_to_u64_saturating(value.value()))
        }
        MetricValue::Gauge { gauge: value, .. } => {
            QueryExecutionMetricValue::Gauge(usize_to_u64_saturating(value.value()))
        }
        MetricValue::StartTimestamp(value) | MetricValue::EndTimestamp(value) => {
            QueryExecutionMetricValue::TimestampNanoseconds(
                value
                    .value()
                    .and_then(|timestamp| timestamp.timestamp_nanos_opt()),
            )
        }
        MetricValue::PruningMetrics {
            pruning_metrics, ..
        } => QueryExecutionMetricValue::Pruning {
            pruned: usize_to_u64_saturating(pruning_metrics.pruned()),
            matched: usize_to_u64_saturating(pruning_metrics.matched()),
            fully_matched: usize_to_u64_saturating(pruning_metrics.fully_matched()),
        },
        MetricValue::Ratio { ratio_metrics, .. } => QueryExecutionMetricValue::Ratio {
            part: usize_to_u64_saturating(ratio_metrics.part()),
            total: usize_to_u64_saturating(ratio_metrics.total()),
        },
        MetricValue::Custom { value, .. } => {
            QueryExecutionMetricValue::Custom(usize_to_u64_saturating(value.as_usize()))
        }
    }
}

const fn metric_category_rank(category: QueryExecutionMetricCategory) -> u8 {
    match category {
        QueryExecutionMetricCategory::Summary => 0,
        QueryExecutionMetricCategory::Dev => 1,
    }
}

fn compare_metric_values(
    left: &QueryExecutionMetricValue,
    right: &QueryExecutionMetricValue,
) -> Ordering {
    match (left, right) {
        (QueryExecutionMetricValue::Count(left), QueryExecutionMetricValue::Count(right))
        | (QueryExecutionMetricValue::Bytes(left), QueryExecutionMetricValue::Bytes(right))
        | (
            QueryExecutionMetricValue::Nanoseconds(left),
            QueryExecutionMetricValue::Nanoseconds(right),
        )
        | (QueryExecutionMetricValue::Gauge(left), QueryExecutionMetricValue::Gauge(right))
        | (QueryExecutionMetricValue::Custom(left), QueryExecutionMetricValue::Custom(right)) => {
            left.cmp(right)
        }
        (
            QueryExecutionMetricValue::TimestampNanoseconds(left),
            QueryExecutionMetricValue::TimestampNanoseconds(right),
        ) => left.cmp(right),
        (
            QueryExecutionMetricValue::Pruning {
                pruned: left_pruned,
                matched: left_matched,
                fully_matched: left_fully_matched,
            },
            QueryExecutionMetricValue::Pruning {
                pruned: right_pruned,
                matched: right_matched,
                fully_matched: right_fully_matched,
            },
        ) => (left_pruned, left_matched, left_fully_matched).cmp(&(
            right_pruned,
            right_matched,
            right_fully_matched,
        )),
        (
            QueryExecutionMetricValue::Ratio {
                part: left_part,
                total: left_total,
            },
            QueryExecutionMetricValue::Ratio {
                part: right_part,
                total: right_total,
            },
        ) => (left_part, left_total).cmp(&(right_part, right_total)),
        _ => Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        borrow::Cow,
        error::Error,
        fmt,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering as AtomicOrdering},
        },
        time::Duration,
    };

    use chrono::{DateTime, Utc};
    use datafusion::{
        arrow::datatypes::Schema,
        common::{DataFusionError, Result as DataFusionResult},
        execution::TaskContext,
        physical_plan::{
            DisplayAs, DisplayFormatType, PlanProperties, SendableRecordBatchStream,
            empty::EmptyExec,
            metrics::{
                Count, CustomMetricValue, Gauge, Label, PruningMetrics, RatioMetrics, Time,
                Timestamp,
            },
            union::UnionExec,
        },
        prelude::SessionContext,
    };
    use tracing::Level;

    use crate::observability::test_capture::TracingCapture;

    use super::super::{
        collect_delta_provider_read_stats_handles, snapshot_delta_provider_read_stats,
        test_support::register_fixture_source,
    };
    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    #[derive(Debug)]
    struct TestCustomMetric {
        value: AtomicUsize,
    }

    impl TestCustomMetric {
        fn new(value: usize) -> Self {
            Self {
                value: AtomicUsize::new(value),
            }
        }
    }

    impl fmt::Display for TestCustomMetric {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("secret-custom-display")
        }
    }

    impl CustomMetricValue for TestCustomMetric {
        fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
            Arc::new(Self::new(0))
        }

        fn aggregate(&self, other: Arc<dyn CustomMetricValue>) {
            if let Some(other) = other.as_any().downcast_ref::<Self>() {
                self.value
                    .fetch_add(other.as_usize(), AtomicOrdering::Relaxed);
            }
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_usize(&self) -> usize {
            self.value.load(AtomicOrdering::Relaxed)
        }

        fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .is_some_and(|other| self.as_usize() == other.as_usize())
        }
    }

    #[derive(Debug)]
    struct ProfileTestExec {
        name: &'static str,
        display_text: &'static str,
        children: Vec<Arc<dyn ExecutionPlan>>,
        properties: EmptyExec,
        metrics: Option<MetricsSet>,
        metrics_calls: Arc<AtomicUsize>,
        execute_calls: Arc<AtomicUsize>,
    }

    impl ProfileTestExec {
        fn new(
            name: &'static str,
            display_text: &'static str,
            children: Vec<Arc<dyn ExecutionPlan>>,
            output_partition_count: usize,
            metrics_available: bool,
        ) -> Arc<Self> {
            Arc::new(Self {
                name,
                display_text,
                children,
                properties: EmptyExec::new(Arc::new(Schema::empty()))
                    .with_partitions(output_partition_count),
                metrics: metrics_available.then(MetricsSet::new),
                metrics_calls: Arc::new(AtomicUsize::new(0)),
                execute_calls: Arc::new(AtomicUsize::new(0)),
            })
        }
    }

    impl DisplayAs for ProfileTestExec {
        fn fmt_as(
            &self,
            _format_type: DisplayFormatType,
            formatter: &mut fmt::Formatter<'_>,
        ) -> fmt::Result {
            formatter.write_str(self.display_text)
        }
    }

    impl ExecutionPlan for ProfileTestExec {
        fn name(&self) -> &str {
            self.name
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.properties.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            self.children.iter().collect()
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self {
                name: self.name,
                display_text: self.display_text,
                children,
                properties: self.properties.clone(),
                metrics: self.metrics.clone(),
                metrics_calls: Arc::clone(&self.metrics_calls),
                execute_calls: Arc::clone(&self.execute_calls),
            }))
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> DataFusionResult<SendableRecordBatchStream> {
            self.execute_calls.fetch_add(1, AtomicOrdering::Relaxed);
            Err(DataFusionError::Execution(
                "profile collection must not execute the plan".to_owned(),
            ))
        }

        fn metrics(&self) -> Option<MetricsSet> {
            self.metrics_calls.fetch_add(1, AtomicOrdering::Relaxed);
            self.metrics.clone()
        }
    }

    #[test]
    fn terminal_consumer_collects_once_stores_result_and_releases_root() -> TestResult {
        let root = ProfileTestExec::new("RootExec", "secret plan text", Vec::new(), 1, true);
        let metrics_calls = Arc::clone(&root.metrics_calls);
        let root: Arc<dyn ExecutionPlan> = root;
        let weak_root = Arc::downgrade(&root);
        let (consumer, result) = QueryExecutionProfileConsumer::register(
            Arc::clone(&root),
            QueryExecutionScope::Preview,
            Some(20),
        );
        let terminal_snapshots = Vec::new();
        assert!(result.profile().is_none());
        drop(root);
        let capture = TracingCapture::start();

        consumer.consume_terminal(QueryExecutionOutcome::Cancelled, &terminal_snapshots);

        let profile = result.profile().ok_or("expected terminal profile")?;
        assert_eq!(profile.scope(), QueryExecutionScope::Preview);
        assert_eq!(profile.outcome(), QueryExecutionOutcome::Cancelled);
        assert_eq!(profile.delta_funnel_row_limit(), Some(20));
        assert_eq!(profile.operators().len(), 1);
        assert_eq!(metrics_calls.load(AtomicOrdering::Relaxed), 1);
        assert!(weak_root.upgrade().is_none());
        let events = capture
            .captured()
            .events()
            .into_iter()
            .filter(|event| {
                event.fields.get("telemetry_event").map(String::as_str)
                    == Some("query_execution_profile_terminal")
            })
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].target, "delta_funnel");
        assert_eq!(events[0].level, Level::DEBUG);

        Ok(())
    }

    #[test]
    fn terminal_consumer_registrations_keep_scope_limit_and_outcome_distinct() -> TestResult {
        for (scope, limit, outcome) in [
            (
                QueryExecutionScope::Preview,
                Some(25),
                QueryExecutionOutcome::Success,
            ),
            (
                QueryExecutionScope::MssqlOutput,
                None,
                QueryExecutionOutcome::Error,
            ),
            (
                QueryExecutionScope::WriteAllCacheAlias,
                None,
                QueryExecutionOutcome::Cancelled,
            ),
        ] {
            let root: Arc<dyn ExecutionPlan> =
                ProfileTestExec::new("RootExec", "redacted", Vec::new(), 1, false);
            let (consumer, result) = QueryExecutionProfileConsumer::register(root, scope, limit);

            consumer.consume_terminal(outcome, &Vec::new());

            let profile = result.profile().ok_or("expected terminal profile")?;
            assert_eq!(profile.scope(), scope);
            assert_eq!(profile.delta_funnel_row_limit(), limit);
            assert_eq!(profile.outcome(), outcome);
        }

        Ok(())
    }

    #[test]
    fn collects_unique_plan_nodes_in_first_seen_preorder_without_execution() -> TestResult {
        let first_leaf = ProfileTestExec::new(
            "LeafExec",
            "path=/secret/first.parquet",
            Vec::new(),
            1,
            true,
        );
        let second_leaf =
            ProfileTestExec::new("LeafExec", "credential=secret-second", Vec::new(), 2, false);
        let first_leaf_plan: Arc<dyn ExecutionPlan> = first_leaf.clone();
        let second_leaf_plan: Arc<dyn ExecutionPlan> = second_leaf.clone();
        let branch = ProfileTestExec::new(
            "BranchExec",
            "filter=secret_column = 'token'",
            vec![Arc::clone(&first_leaf_plan), second_leaf_plan],
            3,
            false,
        );
        let branch_plan: Arc<dyn ExecutionPlan> = branch.clone();
        let root = ProfileTestExec::new(
            "RootExec",
            "sql=SELECT secret_column FROM secret_table",
            vec![branch_plan, first_leaf_plan],
            0,
            true,
        );
        let root_plan: Arc<dyn ExecutionPlan> = root.clone();
        let terminal_snapshots = Vec::new();

        let profile = collect_query_execution_profile(
            &root_plan,
            QueryExecutionScope::Preview,
            QueryExecutionOutcome::Error,
            20,
            Some(&terminal_snapshots),
        );

        assert_eq!(profile.scope(), QueryExecutionScope::Preview);
        assert_eq!(profile.outcome(), QueryExecutionOutcome::Error);
        assert!(profile.partial());
        assert_eq!(profile.delta_funnel_row_limit(), Some(20));
        assert_eq!(
            profile
                .operators()
                .iter()
                .map(|operator| {
                    (
                        operator.node_id(),
                        operator.parent_node_id(),
                        operator.operator_name(),
                    )
                })
                .collect::<Vec<_>>(),
            [
                (0, None, "RootExec"),
                (1, Some(0), "BranchExec"),
                (2, Some(1), "LeafExec"),
                (3, Some(1), "LeafExec"),
            ]
        );
        assert_eq!(profile.operators()[0].output_partition_count(), 0);
        assert_eq!(
            profile
                .operators()
                .iter()
                .map(QueryExecutionOperatorProfile::metrics_available)
                .collect::<Vec<_>>(),
            [true, false, true, false]
        );
        assert!(profile.operators().iter().all(|operator| {
            operator.aggregated_metrics().is_empty()
                && operator.metrics().is_empty()
                && operator.delta_provider_read_stats().is_none()
        }));
        for plan in [&root, &branch, &first_leaf, &second_leaf] {
            assert_eq!(plan.metrics_calls.load(AtomicOrdering::Relaxed), 1);
            assert_eq!(plan.execute_calls.load(AtomicOrdering::Relaxed), 0);
        }

        let json = serde_json::to_string(&profile.to_json_value())?;
        let debug = format!("{profile:?}");
        for secret in [
            "/secret/first.parquet",
            "secret-second",
            "secret_column",
            "token",
            "secret_table",
        ] {
            assert!(!json.contains(secret));
            assert!(!debug.contains(secret));
        }

        Ok(())
    }

    #[tokio::test]
    async fn terminal_provider_snapshots_attach_by_exact_handle_identity() -> TestResult {
        let context = SessionContext::new();
        let _table = register_fixture_source(&context, "orders", "profile-provider-identity")?;
        let first_plan = delta_plan(&context, "orders").await?;
        let second_plan = delta_plan(&context, "orders").await?;
        let first_handles = collect_delta_provider_read_stats_handles(first_plan.as_ref());
        let second_handles = collect_delta_provider_read_stats_handles(second_plan.as_ref());
        let first_handle = Arc::clone(first_handles.first().ok_or("expected first scan handle")?);
        let second_handle = Arc::clone(
            second_handles
                .first()
                .ok_or("expected second scan handle")?,
        );
        assert_eq!(first_handles.len(), 1);
        assert_eq!(second_handles.len(), 1);
        assert!(!Arc::ptr_eq(&first_handle, &second_handle));

        let mut first_snapshot = snapshot_delta_provider_read_stats(&[Arc::clone(&first_handle)])
            .into_iter()
            .next()
            .ok_or("expected first provider snapshot")?;
        first_snapshot.rows_produced = 11;
        first_snapshot.parquet_data_file_bytes_received = Some(111);
        let mut second_snapshot = snapshot_delta_provider_read_stats(&[Arc::clone(&second_handle)])
            .into_iter()
            .next()
            .ok_or("expected second provider snapshot")?;
        second_snapshot.rows_produced = 22;
        second_snapshot.parquet_data_file_bytes_received = Some(222);
        let mut duplicate_first_snapshot = first_snapshot.clone();
        duplicate_first_snapshot.rows_produced = 999;

        let terminal_snapshots = delta_provider_read_stats_snapshot_set(
            &[
                Arc::clone(&second_handle),
                Arc::clone(&first_handle),
                Arc::clone(&first_handle),
            ],
            &[second_snapshot, first_snapshot, duplicate_first_snapshot],
        );
        assert_eq!(terminal_snapshots.len(), 2);
        assert!(Arc::ptr_eq(&terminal_snapshots[0].0, &second_handle));
        assert!(Arc::ptr_eq(&terminal_snapshots[1].0, &first_handle));

        let root: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![first_plan, second_plan])?;
        let profile = collect_query_execution_profile(
            &root,
            QueryExecutionScope::MssqlOutput,
            QueryExecutionOutcome::Success,
            0,
            Some(&terminal_snapshots),
        );
        let scans = profile
            .operators()
            .iter()
            .filter(|operator| operator.operator_name() == "DeltaScanPlanningExec")
            .collect::<Vec<_>>();
        assert_eq!(scans.len(), 2);
        assert_eq!(
            scans
                .iter()
                .filter_map(|operator| operator.delta_provider_read_stats())
                .map(|stats| stats.rows_produced)
                .collect::<Vec<_>>(),
            [11, 22]
        );
        let json = profile.to_json_value();
        assert_eq!(
            json["operators"]
                .as_array()
                .ok_or("expected profile operators")?
                .iter()
                .filter(|operator| operator["operator_name"] == "DeltaScanPlanningExec")
                .map(|operator| {
                    operator["delta_provider_read_stats"]["parquet_data_file_bytes_received"]
                        .as_u64()
                })
                .collect::<Vec<_>>(),
            [Some(111), Some(222)]
        );

        Ok(())
    }

    #[tokio::test]
    async fn missing_terminal_provider_snapshot_is_redacted_and_does_not_fallback() -> TestResult {
        let context = SessionContext::new();
        let _table = register_fixture_source(
            &context,
            "profile_secret_source",
            "profile-provider-missing-secret",
        )?;
        let target_plan = delta_plan(&context, "profile_secret_source").await?;
        let unrelated_plan = delta_plan(&context, "profile_secret_source").await?;
        let target_handles = collect_delta_provider_read_stats_handles(target_plan.as_ref());
        let unrelated_handles = collect_delta_provider_read_stats_handles(unrelated_plan.as_ref());
        let unrelated_handle = Arc::clone(
            unrelated_handles
                .first()
                .ok_or("expected unrelated scan handle")?,
        );
        assert_eq!(target_handles.len(), 1);
        assert_eq!(unrelated_handles.len(), 1);
        assert!(!Arc::ptr_eq(&target_handles[0], &unrelated_handle));
        let terminal_snapshots = delta_provider_read_stats_snapshot_set(
            &[unrelated_handle],
            &snapshot_delta_provider_read_stats(&unrelated_handles),
        );
        let capture = TracingCapture::start();

        let profile = collect_query_execution_profile(
            &target_plan,
            QueryExecutionScope::WriteAllCacheAlias,
            QueryExecutionOutcome::Cancelled,
            0,
            Some(&terminal_snapshots),
        );

        let scan = profile
            .operators()
            .iter()
            .find(|operator| operator.operator_name() == "DeltaScanPlanningExec")
            .ok_or("expected target Delta scan profile")?;
        assert!(scan.delta_provider_read_stats().is_none());
        let diagnostics = capture
            .captured()
            .events()
            .into_iter()
            .filter(|event| {
                event.fields.get("telemetry_event").map(String::as_str)
                    == Some("query_execution_profile_provider_snapshot_missing")
            })
            .collect::<Vec<_>>();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].target, "delta_funnel");
        assert_eq!(diagnostics[0].level, Level::DEBUG);
        let captured_text = format!("{:?}", diagnostics[0].fields);
        assert!(!captured_text.contains("profile_secret_source"));
        assert!(!captured_text.contains("profile-provider-missing-secret"));

        let fallback_profile = collect_query_execution_profile(
            &target_plan,
            QueryExecutionScope::WriteAllCacheAlias,
            QueryExecutionOutcome::Success,
            0,
            None,
        );
        assert_eq!(
            fallback_profile
                .operators()
                .iter()
                .find(|operator| operator.operator_name() == "DeltaScanPlanningExec")
                .and_then(|operator| operator.delta_provider_read_stats())
                .map(|stats| stats.source_name.as_str()),
            Some("profile_secret_source")
        );

        Ok(())
    }

    #[test]
    fn converts_every_datafusion_metric_value_without_display_parsing() -> TestResult {
        let Some(start_time) = DateTime::from_timestamp(1, 2) else {
            return Err("test timestamp should be valid".into());
        };
        let out_of_range_time =
            DateTime::parse_from_rfc3339("2500-01-01T00:00:00Z")?.with_timezone(&Utc);
        assert!(out_of_range_time.timestamp_nanos_opt().is_none());

        let start_timestamp = Timestamp::new();
        start_timestamp.set(start_time);
        let unset_timestamp = Timestamp::new();
        let out_of_range_timestamp = Timestamp::new();
        out_of_range_timestamp.set(out_of_range_time);

        let pruning = PruningMetrics::new();
        pruning.add_pruned(12);
        pruning.add_matched(13);
        pruning.add_fully_matched(14);

        let ratio = RatioMetrics::new();
        ratio.add_part(15);
        ratio.add_total(16);

        let values = [
            MetricValue::OutputRows(count(1)),
            MetricValue::ElapsedCompute(time(2)),
            MetricValue::SpillCount(count(3)),
            MetricValue::SpilledBytes(count(4)),
            MetricValue::OutputBytes(count(5)),
            MetricValue::OutputBatches(count(6)),
            MetricValue::SpilledRows(count(7)),
            MetricValue::CurrentMemoryUsage(gauge(8)),
            MetricValue::Count {
                name: Cow::Borrowed("generic_count"),
                count: count(9),
            },
            MetricValue::Gauge {
                name: Cow::Borrowed("generic_gauge"),
                gauge: gauge(10),
            },
            MetricValue::Time {
                name: Cow::Borrowed("generic_time"),
                time: time(11),
            },
            MetricValue::StartTimestamp(start_timestamp),
            MetricValue::EndTimestamp(unset_timestamp),
            MetricValue::EndTimestamp(out_of_range_timestamp),
            MetricValue::PruningMetrics {
                name: Cow::Borrowed("pruning"),
                pruning_metrics: pruning,
            },
            MetricValue::Ratio {
                name: Cow::Borrowed("ratio"),
                ratio_metrics: ratio,
            },
            MetricValue::Custom {
                name: Cow::Borrowed("custom"),
                value: Arc::new(TestCustomMetric::new(17)),
            },
        ];
        let mut metrics = MetricsSet::new();
        for (partition, value) in values.into_iter().enumerate() {
            metrics.push(metric(value, Some(partition), MetricType::DEV, Vec::new()));
        }

        let (_, raw) = collect_query_execution_metrics(&metrics);
        let expected = [
            ("output_rows", 0, QueryExecutionMetricValue::Count(1)),
            (
                "elapsed_compute",
                1,
                QueryExecutionMetricValue::Nanoseconds(2),
            ),
            ("spill_count", 2, QueryExecutionMetricValue::Count(3)),
            ("spilled_bytes", 3, QueryExecutionMetricValue::Bytes(4)),
            ("output_bytes", 4, QueryExecutionMetricValue::Bytes(5)),
            ("output_batches", 5, QueryExecutionMetricValue::Count(6)),
            ("spilled_rows", 6, QueryExecutionMetricValue::Count(7)),
            ("mem_used", 7, QueryExecutionMetricValue::Bytes(8)),
            ("generic_count", 8, QueryExecutionMetricValue::Count(9)),
            ("generic_gauge", 9, QueryExecutionMetricValue::Gauge(10)),
            (
                "generic_time",
                10,
                QueryExecutionMetricValue::Nanoseconds(11),
            ),
            (
                "start_timestamp",
                11,
                QueryExecutionMetricValue::TimestampNanoseconds(Some(1_000_000_002)),
            ),
            (
                "end_timestamp",
                12,
                QueryExecutionMetricValue::TimestampNanoseconds(None),
            ),
            (
                "end_timestamp",
                13,
                QueryExecutionMetricValue::TimestampNanoseconds(None),
            ),
            (
                "pruning",
                14,
                QueryExecutionMetricValue::Pruning {
                    pruned: 12,
                    matched: 13,
                    fully_matched: 14,
                },
            ),
            (
                "ratio",
                15,
                QueryExecutionMetricValue::Ratio {
                    part: 15,
                    total: 16,
                },
            ),
            ("custom", 16, QueryExecutionMetricValue::Custom(17)),
        ];

        for (name, partition, value) in expected {
            assert_metric_value(&raw, name, Some(partition), value);
        }
        let custom_debug = raw
            .iter()
            .find(|metric| metric.name() == "custom")
            .map(|metric| format!("{metric:?}"));
        assert!(
            custom_debug
                .as_deref()
                .is_some_and(|debug| !debug.contains("secret-custom-display"))
        );

        Ok(())
    }

    #[test]
    fn normalizes_only_valid_output_partition_and_redacts_other_labels() -> TestResult {
        let mut metrics = MetricsSet::new();
        metrics.push(metric(
            MetricValue::Count {
                name: Cow::Borrowed("labelled"),
                count: count(usize::MAX),
            },
            Some(usize::MAX),
            MetricType::SUMMARY,
            vec![
                Label::new("outputPartition", u64::MAX.to_string()),
                Label::new("filename", "/secret/path/file.parquet"),
                Label::new("expr", "secret_column = 'token'"),
                Label::new("unknown", "secret-header"),
            ],
        ));
        metrics.push(metric(
            MetricValue::Count {
                name: Cow::Borrowed("leading_zero"),
                count: count(1),
            },
            None,
            MetricType::DEV,
            vec![Label::new("outputPartition", "001")],
        ));
        for (index, value) in ["", "+1", "-1", " 1", "1.0", "18446744073709551616"]
            .into_iter()
            .enumerate()
        {
            metrics.push(metric(
                MetricValue::Count {
                    name: Cow::Owned(format!("malformed_{index}")),
                    count: count(index),
                },
                Some(index),
                MetricType::DEV,
                vec![Label::new("outputPartition", value.to_owned())],
            ));
        }

        let (_, raw) = collect_query_execution_metrics(&metrics);
        let labelled = raw.iter().find(|metric| metric.name() == "labelled");
        assert_eq!(
            labelled.map(QueryExecutionMetric::partition),
            Some(Some(usize_to_u64_saturating(usize::MAX)))
        );
        assert_eq!(
            labelled.map(QueryExecutionMetric::output_partition),
            Some(Some(u64::MAX))
        );
        assert_eq!(
            labelled.map(|metric| metric.value()),
            Some(&QueryExecutionMetricValue::Count(usize_to_u64_saturating(
                usize::MAX
            )))
        );
        assert_eq!(
            raw.iter()
                .find(|metric| metric.name() == "leading_zero")
                .map(QueryExecutionMetric::output_partition),
            Some(Some(1))
        );
        assert!(
            raw.iter()
                .filter(|metric| metric.name().starts_with("malformed_"))
                .all(|metric| metric.output_partition().is_none())
        );

        let json = serde_json::to_string(
            &raw.iter()
                .map(QueryExecutionMetric::to_json_value)
                .collect::<Vec<_>>(),
        )?;
        let debug = format!("{raw:?}");
        for secret in [
            "/secret/path/file.parquet",
            "secret_column",
            "token",
            "secret-header",
            "filename",
            "expr",
            "unknown",
        ] {
            assert!(!json.contains(secret));
            assert!(!debug.contains(secret));
        }

        Ok(())
    }

    #[test]
    fn aggregates_by_name_with_null_partition_fields() {
        let mut metrics = MetricsSet::new();
        metrics.push(metric(
            MetricValue::Count {
                name: Cow::Borrowed("rows_seen"),
                count: count(2),
            },
            Some(1),
            MetricType::SUMMARY,
            vec![Label::new("outputPartition", "4")],
        ));
        metrics.push(metric(
            MetricValue::Count {
                name: Cow::Borrowed("rows_seen"),
                count: count(1),
            },
            Some(0),
            MetricType::SUMMARY,
            vec![Label::new("outputPartition", "3")],
        ));
        metrics.push(metric(
            MetricValue::Gauge {
                name: Cow::Borrowed("z_gauge"),
                gauge: gauge(5),
            },
            Some(2),
            MetricType::DEV,
            Vec::new(),
        ));

        let (aggregated, raw) = collect_query_execution_metrics(&metrics);

        assert_eq!(
            raw.iter()
                .map(|metric| (metric.name(), metric.partition()))
                .collect::<Vec<_>>(),
            [
                ("rows_seen", Some(0)),
                ("rows_seen", Some(1)),
                ("z_gauge", Some(2))
            ]
        );
        assert_eq!(aggregated.len(), 2);
        assert!(
            aggregated
                .iter()
                .all(|metric| metric.partition().is_none() && metric.output_partition().is_none())
        );
        assert_eq!(
            aggregated
                .iter()
                .find(|metric| metric.name() == "rows_seen")
                .map(|metric| (metric.category(), metric.value())),
            Some((
                QueryExecutionMetricCategory::Summary,
                &QueryExecutionMetricValue::Count(3)
            ))
        );
    }

    #[test]
    fn metric_sorting_is_deterministic_through_value_components() -> TestResult {
        let mut metrics = MetricsSet::new();
        let name = Cow::Borrowed("output_bytes");
        metrics.push(metric(
            MetricValue::Count {
                name: name.clone(),
                count: count(2),
            },
            Some(1),
            MetricType::SUMMARY,
            Vec::new(),
        ));
        metrics.push(metric(
            MetricValue::Count {
                name: name.clone(),
                count: count(1),
            },
            Some(1),
            MetricType::SUMMARY,
            Vec::new(),
        ));
        metrics.push(metric(
            MetricValue::Count {
                name: name.clone(),
                count: count(9),
            },
            None,
            MetricType::SUMMARY,
            Vec::new(),
        ));
        metrics.push(metric(
            MetricValue::OutputBytes(count(5)),
            Some(1),
            MetricType::SUMMARY,
            Vec::new(),
        ));
        metrics.push(metric(
            MetricValue::PruningMetrics {
                name: name.clone(),
                pruning_metrics: pruning(2, 0, 0),
            },
            Some(1),
            MetricType::SUMMARY,
            Vec::new(),
        ));
        metrics.push(metric(
            MetricValue::PruningMetrics {
                name: name.clone(),
                pruning_metrics: pruning(1, 9, 9),
            },
            Some(1),
            MetricType::SUMMARY,
            Vec::new(),
        ));
        metrics.push(metric(
            MetricValue::Ratio {
                name,
                ratio_metrics: ratio(1, 2),
            },
            Some(1),
            MetricType::SUMMARY,
            Vec::new(),
        ));
        metrics.push(metric(
            MetricValue::Count {
                name: Cow::Borrowed("output_bytes"),
                count: count(0),
            },
            Some(1),
            MetricType::SUMMARY,
            vec![Label::new("outputPartition", "3")],
        ));

        let first = convert_metrics(&metrics, false);
        let second = convert_metrics(&metrics, false);
        assert_eq!(
            first
                .iter()
                .map(|metric| metric.value().clone())
                .collect::<Vec<_>>(),
            [
                QueryExecutionMetricValue::Count(9),
                QueryExecutionMetricValue::Bytes(5),
                QueryExecutionMetricValue::Count(1),
                QueryExecutionMetricValue::Count(2),
                QueryExecutionMetricValue::Pruning {
                    pruned: 1,
                    matched: 9,
                    fully_matched: 9,
                },
                QueryExecutionMetricValue::Pruning {
                    pruned: 2,
                    matched: 0,
                    fully_matched: 0,
                },
                QueryExecutionMetricValue::Ratio { part: 1, total: 2 },
                QueryExecutionMetricValue::Count(0),
            ]
        );
        assert_eq!(
            serde_json::to_string(
                &first
                    .iter()
                    .map(QueryExecutionMetric::to_json_value)
                    .collect::<Vec<_>>()
            )?,
            serde_json::to_string(
                &second
                    .iter()
                    .map(QueryExecutionMetric::to_json_value)
                    .collect::<Vec<_>>()
            )?
        );

        Ok(())
    }

    async fn delta_plan(
        context: &SessionContext,
        source_name: &str,
    ) -> Result<Arc<dyn ExecutionPlan>, Box<dyn Error>> {
        Ok(context
            .sql(&format!("select * from {source_name}"))
            .await?
            .create_physical_plan()
            .await?)
    }

    fn metric(
        value: MetricValue,
        partition: Option<usize>,
        metric_type: MetricType,
        labels: Vec<Label>,
    ) -> Arc<Metric> {
        Arc::new(Metric::new_with_labels(value, partition, labels).with_type(metric_type))
    }

    fn count(value: usize) -> Count {
        let count = Count::new();
        count.add(value);
        count
    }

    fn gauge(value: usize) -> Gauge {
        let gauge = Gauge::new();
        gauge.set(value);
        gauge
    }

    fn time(value: u64) -> Time {
        let time = Time::new();
        time.add_duration(Duration::from_nanos(value));
        time
    }

    fn pruning(pruned: usize, matched: usize, fully_matched: usize) -> PruningMetrics {
        let metrics = PruningMetrics::new();
        metrics.add_pruned(pruned);
        metrics.add_matched(matched);
        metrics.add_fully_matched(fully_matched);
        metrics
    }

    fn ratio(part: usize, total: usize) -> RatioMetrics {
        let metrics = RatioMetrics::new();
        metrics.add_part(part);
        metrics.add_total(total);
        metrics
    }

    fn assert_metric_value(
        metrics: &[QueryExecutionMetric],
        name: &str,
        partition: Option<usize>,
        expected: QueryExecutionMetricValue,
    ) {
        assert_eq!(
            metrics
                .iter()
                .find(|metric| {
                    metric.name() == name
                        && metric.partition() == partition.map(usize_to_u64_saturating)
                })
                .map(QueryExecutionMetric::value),
            Some(&expected)
        );
    }
}
