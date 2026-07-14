use crate::DeltaProviderReadStatsSnapshot;

/// Controls whether a query execution profile is collected.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ExecutionProfileMode {
    /// Do not collect detailed query execution metrics.
    #[default]
    Disabled,
    /// Collect a detailed query execution profile.
    Detailed,
}

/// Identifies why Delta Funnel executed a query plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryExecutionScope {
    /// A bounded table preview.
    Preview,
    /// One SQL Server output write.
    MssqlOutput,
    /// Materialization of a selected write-all cache alias.
    WriteAllCacheAlias,
}

impl QueryExecutionScope {
    /// Returns the stable JSON spelling for this execution scope.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Preview => "preview",
            Self::MssqlOutput => "mssql_output",
            Self::WriteAllCacheAlias => "write_all_cache_alias",
        }
    }
}

/// Describes how query execution reached its terminal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryExecutionOutcome {
    /// The query stream reached normal exhaustion.
    Success,
    /// Query execution returned an error.
    Error,
    /// Required stream ownership was dropped before normal exhaustion.
    Cancelled,
}

impl QueryExecutionOutcome {
    /// Returns the stable JSON spelling for this execution outcome.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Stability category assigned to a DataFusion execution metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryExecutionMetricCategory {
    /// A stable metric intended for normal profile consumers.
    Summary,
    /// A development metric that may change with DataFusion.
    Dev,
}

impl QueryExecutionMetricCategory {
    /// Returns the stable JSON spelling for this metric category.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Summary => "summary",
            Self::Dev => "dev",
        }
    }
}

/// Typed value captured for one DataFusion execution metric.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryExecutionMetricValue {
    /// An unsigned item count.
    Count(u64),
    /// An unsigned byte count.
    Bytes(u64),
    /// An unsigned elapsed duration in nanoseconds.
    Nanoseconds(u64),
    /// An unsigned point-in-time gauge.
    Gauge(u64),
    /// An optional signed Unix epoch timestamp in nanoseconds.
    TimestampNanoseconds(Option<i64>),
    /// File-pruning counters.
    Pruning {
        /// Files pruned.
        pruned: u64,
        /// Files partially matched.
        matched: u64,
        /// Files fully matched.
        fully_matched: u64,
    },
    /// The two unsigned components of a ratio.
    Ratio {
        /// Ratio numerator.
        part: u64,
        /// Ratio denominator.
        total: u64,
    },
    /// An unsigned custom metric value.
    Custom(u64),
}

impl QueryExecutionMetricValue {
    /// Returns the stable JSON kind for this metric value.
    #[must_use]
    pub const fn value_kind(&self) -> &'static str {
        match self {
            Self::Count(_) => "count",
            Self::Bytes(_) => "bytes",
            Self::Nanoseconds(_) => "nanoseconds",
            Self::Gauge(_) => "gauge",
            Self::TimestampNanoseconds(_) => "timestamp_nanoseconds",
            Self::Pruning { .. } => "pruning",
            Self::Ratio { .. } => "ratio",
            Self::Custom(_) => "custom",
        }
    }
}

/// One redacted metric attached to a query execution operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryExecutionMetric {
    name: String,
    category: QueryExecutionMetricCategory,
    partition: Option<u64>,
    output_partition: Option<u64>,
    value: QueryExecutionMetricValue,
}

#[allow(dead_code)]
impl QueryExecutionMetric {
    pub(crate) fn new(
        name: impl Into<String>,
        category: QueryExecutionMetricCategory,
        partition: Option<u64>,
        output_partition: Option<u64>,
        value: QueryExecutionMetricValue,
    ) -> Self {
        Self {
            name: name.into(),
            category,
            partition,
            output_partition,
            value,
        }
    }

    /// Returns the DataFusion metric name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the metric stability category.
    #[must_use]
    pub const fn category(&self) -> QueryExecutionMetricCategory {
        self.category
    }

    /// Returns the DataFusion execution partition, when present.
    #[must_use]
    pub const fn partition(&self) -> Option<u64> {
        self.partition
    }

    /// Returns the normalized output partition label, when present.
    #[must_use]
    pub const fn output_partition(&self) -> Option<u64> {
        self.output_partition
    }

    /// Returns the typed metric value.
    #[must_use]
    pub const fn value(&self) -> &QueryExecutionMetricValue {
        &self.value
    }
}

/// Immutable profile for one unique physical execution-plan node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryExecutionOperatorProfile {
    node_id: u64,
    parent_node_id: Option<u64>,
    operator_name: String,
    output_partition_count: u64,
    metrics_available: bool,
    aggregated_metrics: Vec<QueryExecutionMetric>,
    metrics: Vec<QueryExecutionMetric>,
    delta_provider_read_stats: Option<DeltaProviderReadStatsSnapshot>,
}

#[allow(dead_code)]
impl QueryExecutionOperatorProfile {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        node_id: u64,
        parent_node_id: Option<u64>,
        operator_name: impl Into<String>,
        output_partition_count: u64,
        metrics_available: bool,
        aggregated_metrics: Vec<QueryExecutionMetric>,
        metrics: Vec<QueryExecutionMetric>,
        delta_provider_read_stats: Option<DeltaProviderReadStatsSnapshot>,
    ) -> Self {
        Self {
            node_id,
            parent_node_id,
            operator_name: operator_name.into(),
            output_partition_count,
            metrics_available,
            aggregated_metrics,
            metrics,
            delta_provider_read_stats,
        }
    }

    /// Returns the profile-local node identifier.
    #[must_use]
    pub const fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Returns the first-seen parent node identifier, or `None` for the root.
    #[must_use]
    pub const fn parent_node_id(&self) -> Option<u64> {
        self.parent_node_id
    }

    /// Returns the exact DataFusion execution-plan short name.
    #[must_use]
    pub fn operator_name(&self) -> &str {
        &self.operator_name
    }

    /// Returns the number of output partitions produced by this operator.
    #[must_use]
    pub const fn output_partition_count(&self) -> u64 {
        self.output_partition_count
    }

    /// Returns whether DataFusion exposed a metric set for this operator.
    #[must_use]
    pub const fn metrics_available(&self) -> bool {
        self.metrics_available
    }

    /// Returns operator metrics aggregated by name.
    #[must_use]
    pub fn aggregated_metrics(&self) -> &[QueryExecutionMetric] {
        &self.aggregated_metrics
    }

    /// Returns the original per-partition operator metrics.
    #[must_use]
    pub fn metrics(&self) -> &[QueryExecutionMetric] {
        &self.metrics
    }

    /// Returns the immutable provider snapshot for an exact Delta scan node.
    #[must_use]
    pub const fn delta_provider_read_stats(&self) -> Option<&DeltaProviderReadStatsSnapshot> {
        self.delta_provider_read_stats.as_ref()
    }
}

/// Immutable execution profile for one Delta Funnel query scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryExecutionProfile {
    scope: QueryExecutionScope,
    outcome: QueryExecutionOutcome,
    delta_funnel_row_limit: Option<u64>,
    operators: Vec<QueryExecutionOperatorProfile>,
}

#[allow(dead_code)]
impl QueryExecutionProfile {
    pub(crate) fn preview(
        outcome: QueryExecutionOutcome,
        delta_funnel_row_limit: usize,
        operators: Vec<QueryExecutionOperatorProfile>,
    ) -> Self {
        Self::new(
            QueryExecutionScope::Preview,
            outcome,
            Some(crate::usize_to_u64_saturating(delta_funnel_row_limit)),
            operators,
        )
    }

    pub(crate) fn mssql_output(
        outcome: QueryExecutionOutcome,
        operators: Vec<QueryExecutionOperatorProfile>,
    ) -> Self {
        Self::new(QueryExecutionScope::MssqlOutput, outcome, None, operators)
    }

    pub(crate) fn write_all_cache_alias(
        outcome: QueryExecutionOutcome,
        operators: Vec<QueryExecutionOperatorProfile>,
    ) -> Self {
        Self::new(
            QueryExecutionScope::WriteAllCacheAlias,
            outcome,
            None,
            operators,
        )
    }

    fn new(
        scope: QueryExecutionScope,
        outcome: QueryExecutionOutcome,
        delta_funnel_row_limit: Option<u64>,
        operators: Vec<QueryExecutionOperatorProfile>,
    ) -> Self {
        Self {
            scope,
            outcome,
            delta_funnel_row_limit,
            operators,
        }
    }

    /// Returns why Delta Funnel executed the plan.
    #[must_use]
    pub const fn scope(&self) -> QueryExecutionScope {
        self.scope
    }

    /// Returns how query execution reached its terminal state.
    #[must_use]
    pub const fn outcome(&self) -> QueryExecutionOutcome {
        self.outcome
    }

    /// Returns whether execution ended before successful exhaustion.
    #[must_use]
    pub const fn partial(&self) -> bool {
        !matches!(self.outcome, QueryExecutionOutcome::Success)
    }

    /// Returns the exact Delta Funnel preview limit for preview profiles.
    #[must_use]
    pub const fn delta_funnel_row_limit(&self) -> Option<u64> {
        self.delta_funnel_row_limit
    }

    /// Returns unique physical operators in first-seen pre-order.
    #[must_use]
    pub fn operators(&self) -> &[QueryExecutionOperatorProfile] {
        &self.operators
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn profile_mode_defaults_to_disabled() {
        assert_eq!(
            ExecutionProfileMode::default(),
            ExecutionProfileMode::Disabled
        );
        assert_ne!(
            ExecutionProfileMode::Detailed,
            ExecutionProfileMode::Disabled
        );
    }

    #[test]
    fn enums_expose_stable_json_spellings() {
        assert_eq!(QueryExecutionScope::Preview.as_str(), "preview");
        assert_eq!(QueryExecutionScope::MssqlOutput.as_str(), "mssql_output");
        assert_eq!(
            QueryExecutionScope::WriteAllCacheAlias.as_str(),
            "write_all_cache_alias"
        );
        assert_eq!(QueryExecutionOutcome::Success.as_str(), "success");
        assert_eq!(QueryExecutionOutcome::Error.as_str(), "error");
        assert_eq!(QueryExecutionOutcome::Cancelled.as_str(), "cancelled");
        assert_eq!(QueryExecutionMetricCategory::Summary.as_str(), "summary");
        assert_eq!(QueryExecutionMetricCategory::Dev.as_str(), "dev");
    }

    #[test]
    fn profile_derives_partial_and_normalizes_scope_limit() {
        let success =
            QueryExecutionProfile::preview(QueryExecutionOutcome::Success, 20, Vec::new());
        let error = QueryExecutionProfile::mssql_output(QueryExecutionOutcome::Error, Vec::new());
        let cancelled = QueryExecutionProfile::write_all_cache_alias(
            QueryExecutionOutcome::Cancelled,
            Vec::new(),
        );

        assert!(!success.partial());
        assert_eq!(success.delta_funnel_row_limit(), Some(20));
        assert!(error.partial());
        assert_eq!(error.delta_funnel_row_limit(), None);
        assert!(cancelled.partial());
        assert_eq!(cancelled.delta_funnel_row_limit(), None);
    }

    #[test]
    fn profile_and_nested_models_expose_typed_accessors_and_json() {
        let metric = QueryExecutionMetric::new(
            "output_rows",
            QueryExecutionMetricCategory::Summary,
            Some(0),
            None,
            QueryExecutionMetricValue::Count(42),
        );
        let operator = QueryExecutionOperatorProfile::new(
            0,
            None,
            "GlobalLimitExec",
            1,
            true,
            Vec::new(),
            vec![metric],
            None,
        );
        let profile =
            QueryExecutionProfile::preview(QueryExecutionOutcome::Success, 20, vec![operator]);

        let operator = &profile.operators()[0];
        let metric = &operator.metrics()[0];
        assert_eq!(profile.scope(), QueryExecutionScope::Preview);
        assert_eq!(profile.outcome(), QueryExecutionOutcome::Success);
        assert_eq!(operator.node_id(), 0);
        assert_eq!(operator.parent_node_id(), None);
        assert_eq!(operator.operator_name(), "GlobalLimitExec");
        assert_eq!(operator.output_partition_count(), 1);
        assert!(operator.metrics_available());
        assert!(operator.aggregated_metrics().is_empty());
        assert_eq!(operator.delta_provider_read_stats(), None);
        assert_eq!(metric.name(), "output_rows");
        assert_eq!(metric.category(), QueryExecutionMetricCategory::Summary);
        assert_eq!(metric.partition(), Some(0));
        assert_eq!(metric.output_partition(), None);
        assert_eq!(metric.value(), &QueryExecutionMetricValue::Count(42));
        assert_eq!(
            profile.to_json_value(),
            json!({
                "scope": "preview",
                "outcome": "success",
                "partial": false,
                "delta_funnel_row_limit": 20,
                "operators": [{
                    "node_id": 0,
                    "parent_node_id": null,
                    "operator_name": "GlobalLimitExec",
                    "output_partition_count": 1,
                    "metrics_available": true,
                    "aggregated_metrics": [],
                    "metrics": [{
                        "name": "output_rows",
                        "category": "summary",
                        "partition": 0,
                        "output_partition": null,
                        "value_kind": "count",
                        "value": 42,
                        "components": null
                    }],
                    "delta_provider_read_stats": null
                }]
            })
        );
    }

    #[test]
    fn metric_values_expose_typed_kinds_and_json_shapes() {
        let cases = [
            (
                QueryExecutionMetricValue::Count(1),
                "count",
                json!(1),
                json!(null),
            ),
            (
                QueryExecutionMetricValue::Bytes(2),
                "bytes",
                json!(2),
                json!(null),
            ),
            (
                QueryExecutionMetricValue::Nanoseconds(3),
                "nanoseconds",
                json!(3),
                json!(null),
            ),
            (
                QueryExecutionMetricValue::Gauge(4),
                "gauge",
                json!(4),
                json!(null),
            ),
            (
                QueryExecutionMetricValue::TimestampNanoseconds(Some(-5)),
                "timestamp_nanoseconds",
                json!(-5),
                json!(null),
            ),
            (
                QueryExecutionMetricValue::TimestampNanoseconds(None),
                "timestamp_nanoseconds",
                json!(null),
                json!(null),
            ),
            (
                QueryExecutionMetricValue::Pruning {
                    pruned: 6,
                    matched: 7,
                    fully_matched: 8,
                },
                "pruning",
                json!(null),
                json!({"pruned": 6, "matched": 7, "fully_matched": 8}),
            ),
            (
                QueryExecutionMetricValue::Ratio { part: 9, total: 10 },
                "ratio",
                json!(null),
                json!({"part": 9, "total": 10}),
            ),
            (
                QueryExecutionMetricValue::Custom(11),
                "custom",
                json!(11),
                json!(null),
            ),
        ];

        for (value, kind, scalar, components) in cases {
            let metric = QueryExecutionMetric::new(
                "metric",
                QueryExecutionMetricCategory::Dev,
                None,
                Some(12),
                value,
            );
            let json = metric.to_json_value();

            assert_eq!(metric.value().value_kind(), kind);
            assert_eq!(json["value_kind"], kind);
            assert_eq!(json["value"], scalar);
            assert_eq!(json["components"], components);
        }
    }
}
