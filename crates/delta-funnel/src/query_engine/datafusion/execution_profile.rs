use std::cmp::Ordering;

use datafusion::physical_plan::metrics::{Metric, MetricType, MetricValue, MetricsSet};

use crate::{
    QueryExecutionMetric, QueryExecutionMetricCategory, QueryExecutionMetricValue,
    usize_to_u64_saturating,
};

#[allow(dead_code)]
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
    use datafusion::physical_plan::metrics::{
        Count, CustomMetricValue, Gauge, Label, PruningMetrics, RatioMetrics, Time, Timestamp,
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
