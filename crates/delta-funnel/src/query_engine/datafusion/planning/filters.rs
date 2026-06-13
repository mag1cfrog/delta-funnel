//! Filter pushdown planning for the Delta DataFusion provider.

mod analysis;
mod partition_pushdown;
mod stats_pushdown;

use std::collections::HashSet;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};

use self::analysis::DeltaFilterAnalysis;
use crate::table_formats::DeltaKernelPredicate;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeltaFilterPushdownOutcome {
    Exact,
    Inexact,
    Unsupported,
}

impl DeltaFilterPushdownOutcome {
    fn to_datafusion(self) -> TableProviderFilterPushDown {
        match self {
            Self::Exact => TableProviderFilterPushDown::Exact,
            Self::Inexact => TableProviderFilterPushDown::Inexact,
            Self::Unsupported => TableProviderFilterPushDown::Unsupported,
        }
    }
}

/// Conservative reason a provider-boundary filter was not accepted for pushdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeltaFilterPushdownRejectionReason {
    /// The expression uses known provider columns and an understood predicate
    /// shape, but current pushdown policy does not accept it as provider-enforced.
    UnsupportedByPolicy,
    /// The expression shape is outside the supported provider filter grammar.
    ExpressionShape,
    /// The expression references a provider-internal synthetic column.
    InternalColumn,
    /// The expression references at least one column that the provider schema
    /// cannot resolve as a top-level field.
    UnknownColumn,
}

impl DeltaFilterPushdownRejectionReason {
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::UnsupportedByPolicy => "unsupported_by_policy",
            Self::ExpressionShape => "unsupported_expression_shape",
            Self::InternalColumn => "unsupported_internal_column",
            Self::UnknownColumn => "unsupported_unknown_column",
        }
    }
}

/// Kind of metadata pruning payload used by kernel scan planning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KernelScanFilterKind {
    /// Exact partition metadata pruning.
    Partition,
    /// Inexact data-column file-statistics pruning.
    DataStats,
}

#[derive(Clone, Debug, PartialEq)]
/// Metadata filter payload used by kernel scan planning.
pub(crate) struct ExactPartitionKernelFilter {
    /// DataFusion expression after any kernel-safe rewrites.
    pub(crate) datafusion_expr: Expr,
    /// Converted Delta kernel predicate for the same expression.
    pub(crate) kernel_predicate: DeltaKernelPredicate,
    /// Metadata pruning path that owns this kernel predicate.
    pub(crate) kind: KernelScanFilterKind,
}

#[derive(Clone, Debug, PartialEq)]
/// Provider decision for one input filter, preserving input order.
pub(crate) struct DeltaFilterPushdownDecision {
    /// Pushdown status reported back to DataFusion for this input filter.
    pub(crate) outcome: DeltaFilterPushdownOutcome,
    /// Whether DataFusion must still evaluate the original filter above the scan.
    pub(crate) residual: bool,
    /// Conservative rejection reason when the filter is unsupported.
    pub(crate) rejection_reason: Option<DeltaFilterPushdownRejectionReason>,
    /// Provider-boundary diagnostics and column classification for the original filter.
    pub(crate) filter_analysis: DeltaFilterAnalysis,
    /// Exact partition filter converted for Delta kernel scan planning.
    ///
    /// For most exact filters this is the original filter. Some exact filters
    /// use an equivalent expression, such as empty list predicates whose
    /// kernel-safe form must preserve null partition behavior.
    pub(crate) kernel_scan_filter: Option<ExactPartitionKernelFilter>,
}

#[derive(Clone, Debug, Default, PartialEq)]
/// Ordered provider pushdown plan for a batch of DataFusion input filters.
pub(crate) struct DeltaFilterPushdownPlan {
    /// One decision per input filter, preserving input order.
    pub(crate) decisions: Vec<DeltaFilterPushdownDecision>,
    /// Number of filters reported as exact.
    pub(crate) exact_count: usize,
    /// Number of filters reported as inexact.
    pub(crate) inexact_count: usize,
    /// Number of filters reported as unsupported.
    pub(crate) unsupported_count: usize,
    /// Number of filters accepted for provider-side work.
    pub(crate) pushed_filter_count: usize,
    /// Number of filters DataFusion must keep as residual filters.
    pub(crate) residual_filter_count: usize,
}

impl DeltaFilterPushdownPlan {
    #[must_use]
    /// Plans the exact static partition operator policy.
    ///
    /// The same policy is used by `supports_filters_pushdown` and by direct
    /// `scan` filter validation so the public support callback and scan
    /// boundary cannot drift apart.
    pub(crate) fn partition_operator_pushdown(
        filters: &[&Expr],
        schema: &SchemaRef,
        partition_columns: &HashSet<String>,
    ) -> Self {
        partition_pushdown::plan_partition_operator_pushdown(filters, schema, partition_columns)
    }

    fn from_decisions(decisions: Vec<DeltaFilterPushdownDecision>) -> Self {
        let exact_count = decisions
            .iter()
            .filter(|decision| decision.outcome == DeltaFilterPushdownOutcome::Exact)
            .count();
        let inexact_count = decisions
            .iter()
            .filter(|decision| decision.outcome == DeltaFilterPushdownOutcome::Inexact)
            .count();
        let unsupported_count = decisions
            .iter()
            .filter(|decision| decision.outcome == DeltaFilterPushdownOutcome::Unsupported)
            .count();
        let residual_filter_count = decisions
            .iter()
            .filter(|decision| decision.residual)
            .count();
        let pushed_filter_count = decisions.len().saturating_sub(unsupported_count);

        Self {
            decisions,
            exact_count,
            inexact_count,
            unsupported_count,
            pushed_filter_count,
            residual_filter_count,
        }
    }

    pub(crate) fn datafusion_pushdowns(&self) -> Vec<TableProviderFilterPushDown> {
        self.decisions
            .iter()
            .map(|decision| decision.outcome.to_datafusion())
            .collect()
    }

    #[must_use]
    pub(crate) fn has_data_stats_filter(&self) -> bool {
        self.decisions.iter().any(|decision| {
            decision
                .kernel_scan_filter
                .as_ref()
                .is_some_and(|filter| filter.kind == KernelScanFilterKind::DataStats)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::ScalarValue;
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, col, lit};

    use super::super::super::catalog::provider::DeltaTableProvider;
    use super::*;
    use crate::query_engine::datafusion::test_support::{
        DeltaLogTable, PARTITIONED_SCHEMA_FIELDS_JSON,
    };
    use crate::{DeltaSourceConfig, load_delta_source, preflight_delta_protocol};

    fn int8_lit(value: i8) -> Expr {
        Expr::Literal(ScalarValue::Int8(Some(value)), None)
    }

    fn int16_lit(value: i16) -> Expr {
        Expr::Literal(ScalarValue::Int16(Some(value)), None)
    }

    fn int64_lit(value: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), None)
    }

    fn date_lit(value: i32) -> Expr {
        Expr::Literal(ScalarValue::Date32(Some(value)), None)
    }

    fn timestamp_lit(value: i64, timezone: &str) -> Expr {
        Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(value), Some(timezone.into())),
            None,
        )
    }

    fn timestamp_ntz_lit(value: i64) -> Expr {
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(value), None), None)
    }

    fn decimal_lit(value: i128) -> Expr {
        Expr::Literal(ScalarValue::Decimal128(Some(value), 10, 2), None)
    }

    fn decimal_lit_with_type(value: i128, precision: u8, scale: i8) -> Expr {
        Expr::Literal(ScalarValue::Decimal128(Some(value), precision, scale), None)
    }

    fn large_utf8_lit(value: &str) -> Expr {
        Expr::Literal(ScalarValue::LargeUtf8(Some(value.to_owned())), None)
    }

    fn binary_lit(value: &[u8]) -> Expr {
        Expr::Literal(ScalarValue::Binary(Some(value.to_vec())), None)
    }

    fn large_binary_lit(value: &[u8]) -> Expr {
        Expr::Literal(ScalarValue::LargeBinary(Some(value.to_vec())), None)
    }

    fn fixed_size_binary_lit(size: i32, value: &[u8]) -> Expr {
        Expr::Literal(
            ScalarValue::FixedSizeBinary(size, Some(value.to_vec())),
            None,
        )
    }

    #[derive(Clone, Copy)]
    enum ExpectedStatsPushdown {
        InexactDataStats,
        Unsupported,
    }

    #[test]
    fn filter_pushdown_documents_data_stats_type_operator_matrix()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("filter-pushdown-data-stats-type-operator-matrix")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let mut provider = DeltaTableProvider::try_new(source, preflight)?;
        provider.set_schema_for_tests(Arc::new(Schema::new(vec![
            Field::new("byte_count", DataType::Int8, true),
            Field::new("short_count", DataType::Int16, true),
            Field::new("int_count", DataType::Int32, true),
            Field::new("long_count", DataType::Int64, true),
            Field::new("is_current", DataType::Boolean, true),
            Field::new("event_date", DataType::Date32, true),
            Field::new(
                "event_ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "event_ts_ntz",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("amount256", DataType::Decimal256(38, 18), true),
            Field::new("customer_name", DataType::Utf8, true),
            Field::new("large_customer_name", DataType::LargeUtf8, true),
            Field::new("float_score", DataType::Float32, true),
            Field::new("double_score", DataType::Float64, true),
            Field::new("payload", DataType::Binary, true),
            Field::new("large_payload", DataType::LargeBinary, true),
            Field::new("fixed_payload", DataType::FixedSizeBinary(3), true),
            Field::new(
                "profile",
                DataType::Struct(vec![Field::new("age", DataType::Int32, true)].into()),
                true,
            ),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "properties",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
            ),
            Field::new(
                "dict_name",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("unsigned_count", DataType::UInt32, true),
            Field::new("date64_value", DataType::Date64, true),
        ])));

        let int32_value = lit(7_i32);
        let decimal_value = decimal_lit(200);
        let date_value = date_lit(20_454);
        let timestamp_value = timestamp_lit(1_767_225_600_123_456, "UTC");
        let timestamp_ntz_value = timestamp_ntz_lit(1_767_225_600_123_456);
        let utf8_value = lit("alice");
        let large_utf8_value = large_utf8_lit("alice");
        let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
        let double_value = Expr::Literal(ScalarValue::Float64(Some(2.25)), None);
        let binary_value = binary_lit(b"hello");
        let decimal256_value =
            Expr::Literal(ScalarValue::Decimal256(Some(200.into()), 38, 18), None);

        use ExpectedStatsPushdown::{InexactDataStats, Unsupported};
        let cases = [
            (
                "int8 equality",
                col("byte_count").eq(int8_lit(7)),
                InexactDataStats,
            ),
            (
                "int16 ordering",
                col("short_count").lt(int16_lit(7)),
                InexactDataStats,
            ),
            (
                "int32 inequality",
                col("int_count").not_eq(int32_value.clone()),
                InexactDataStats,
            ),
            (
                "int64 reversed ordering",
                int64_lit(7).lt(col("long_count")),
                InexactDataStats,
            ),
            (
                "integer null count unsupported",
                col("int_count").is_null(),
                Unsupported,
            ),
            (
                "integer membership unsupported",
                col("int_count").in_list(vec![int32_value.clone()], false),
                Unsupported,
            ),
            (
                "integer between unsupported",
                col("int_count").between(lit(1_i32), int32_value.clone()),
                Unsupported,
            ),
            (
                "boolean null count",
                col("is_current").is_not_null(),
                InexactDataStats,
            ),
            (
                "boolean equality unsupported",
                col("is_current").eq(lit(true)),
                Unsupported,
            ),
            (
                "boolean shorthand unsupported",
                col("is_current"),
                Unsupported,
            ),
            (
                "date equality",
                col("event_date").eq(date_value.clone()),
                InexactDataStats,
            ),
            (
                "date inequality",
                col("event_date").not_eq(date_value.clone()),
                InexactDataStats,
            ),
            (
                "date between unsupported",
                col("event_date").between(date_lit(20_000), date_value.clone()),
                Unsupported,
            ),
            (
                "timestamp equality",
                col("event_ts").eq(timestamp_value.clone()),
                InexactDataStats,
            ),
            (
                "timestamp ordering",
                col("event_ts").gt(timestamp_value.clone()),
                InexactDataStats,
            ),
            (
                "timestamp inequality unsupported",
                col("event_ts").not_eq(timestamp_value.clone()),
                Unsupported,
            ),
            (
                "timestamp ntz equality",
                col("event_ts_ntz").eq(timestamp_ntz_value.clone()),
                InexactDataStats,
            ),
            (
                "timestamp null count",
                col("event_ts").is_null(),
                InexactDataStats,
            ),
            (
                "decimal equality",
                col("amount").eq(decimal_value.clone()),
                InexactDataStats,
            ),
            (
                "decimal ordering",
                col("amount").gt(decimal_value.clone()),
                InexactDataStats,
            ),
            (
                "decimal null count",
                col("amount").is_null(),
                InexactDataStats,
            ),
            (
                "decimal256 unsupported",
                col("amount256").eq(decimal256_value),
                Unsupported,
            ),
            (
                "string equality",
                col("customer_name").eq(utf8_value.clone()),
                InexactDataStats,
            ),
            (
                "string ordering",
                col("customer_name").lt(utf8_value.clone()),
                InexactDataStats,
            ),
            (
                "large string equality",
                col("large_customer_name").eq(large_utf8_value),
                InexactDataStats,
            ),
            (
                "string membership unsupported",
                col("customer_name").in_list(vec![utf8_value.clone()], false),
                Unsupported,
            ),
            (
                "float equality",
                col("float_score").eq(float_value.clone()),
                InexactDataStats,
            ),
            (
                "float zero unsupported",
                col("float_score").eq(lit(0.0_f32)),
                Unsupported,
            ),
            (
                "double ordering",
                col("double_score").gt(double_value),
                InexactDataStats,
            ),
            (
                "floating null count",
                col("float_score").is_not_null(),
                InexactDataStats,
            ),
            (
                "binary null count",
                col("payload").is_null(),
                InexactDataStats,
            ),
            (
                "large binary null count",
                col("large_payload").is_not_null(),
                InexactDataStats,
            ),
            (
                "fixed binary null count",
                col("fixed_payload").is_null(),
                InexactDataStats,
            ),
            (
                "binary equality unsupported",
                col("payload").eq(binary_value.clone()),
                Unsupported,
            ),
            (
                "binary ordering unsupported",
                col("payload").gt(binary_value.clone()),
                Unsupported,
            ),
            (
                "binary membership unsupported",
                col("payload").in_list(vec![binary_value], false),
                Unsupported,
            ),
            ("struct unsupported", col("profile").is_null(), Unsupported),
            ("list unsupported", col("tags").is_null(), Unsupported),
            ("map unsupported", col("properties").is_null(), Unsupported),
            (
                "dictionary unsupported",
                col("dict_name").is_null(),
                Unsupported,
            ),
            (
                "unsigned integer unsupported",
                col("unsigned_count").eq(lit(7_u32)),
                Unsupported,
            ),
            (
                "date64 unsupported",
                col("date64_value").is_null(),
                Unsupported,
            ),
        ];

        for (name, filter, expected) in cases {
            let filter_refs = [&filter];
            let support = provider.supports_filters_pushdown(&filter_refs)?;
            let plan = provider.plan_supports_filters_pushdown(&filter_refs);

            match expected {
                InexactDataStats => {
                    assert_eq!(
                        support,
                        vec![TableProviderFilterPushDown::Inexact],
                        "{name}"
                    );
                    assert_eq!(plan.exact_count, 0, "{name}");
                    assert_eq!(plan.inexact_count, 1, "{name}");
                    assert_eq!(plan.unsupported_count, 0, "{name}");
                    assert_eq!(plan.pushed_filter_count, 1, "{name}");
                    assert_eq!(plan.residual_filter_count, 1, "{name}");
                    let decision = &plan.decisions[0];
                    assert!(decision.residual, "{name}");
                    assert_eq!(decision.rejection_reason, None, "{name}");
                    assert!(
                        decision
                            .kernel_scan_filter
                            .as_ref()
                            .is_some_and(|filter| filter.kind == KernelScanFilterKind::DataStats),
                        "{name}"
                    );
                }
                Unsupported => {
                    assert_eq!(
                        support,
                        vec![TableProviderFilterPushDown::Unsupported],
                        "{name}"
                    );
                    assert_eq!(plan.exact_count, 0, "{name}");
                    assert_eq!(plan.inexact_count, 0, "{name}");
                    assert_eq!(plan.unsupported_count, 1, "{name}");
                    assert_eq!(plan.pushed_filter_count, 0, "{name}");
                    assert_eq!(plan.residual_filter_count, 1, "{name}");
                    let decision = &plan.decisions[0];
                    assert_eq!(
                        decision.outcome,
                        DeltaFilterPushdownOutcome::Unsupported,
                        "{name}"
                    );
                    assert!(decision.residual, "{name}");
                    assert!(decision.rejection_reason.is_some(), "{name}");
                    assert!(decision.kernel_scan_filter.is_none(), "{name}");
                }
            }
        }

        Ok(())
    }

    #[test]
    fn filter_pushdown_matrix_unsupported_entries_keep_stable_policy_reason()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("filter-pushdown-data-stats-matrix-reasons")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let mut provider = DeltaTableProvider::try_new(source, preflight)?;
        provider.set_schema_for_tests(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("customer_name", DataType::Utf8, true),
            Field::new("amount256", DataType::Decimal256(38, 18), true),
            Field::new("float_score", DataType::Float32, true),
            Field::new("payload", DataType::Binary, true),
            Field::new(
                "profile",
                DataType::Struct(vec![Field::new("age", DataType::Int32, true)].into()),
                true,
            ),
        ])));
        let decimal256 = Expr::Literal(ScalarValue::Decimal256(Some(200.into()), 38, 18), None);
        let filters = [
            col("id").is_null(),
            col("customer_name").in_list(vec![lit("alice")], false),
            col("amount256").eq(decimal256),
            col("float_score").eq(lit(0.0_f32)),
            col("payload").eq(binary_lit(b"hello")),
            col("profile").is_null(),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(plan.unsupported_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Unsupported
                && decision.residual
                && decision.kernel_scan_filter.is_none()
                && decision.rejection_reason
                    == Some(DeltaFilterPushdownRejectionReason::UnsupportedByPolicy)
                && decision
                    .rejection_reason
                    .map(DeltaFilterPushdownRejectionReason::code)
                    == Some("unsupported_by_policy")
        }));

        Ok(())
    }

    #[test]
    fn filter_pushdown_reports_exact_for_supported_partition_equality()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-exact-partition-equality",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            r#""partitionValues":{"region":"us-west"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let filter = col("region").eq(lit("us-west"));

        let support = provider.supports_filters_pushdown(&[&filter])?;
        let plan = provider.plan_supports_filters_pushdown(&[&filter]);

        assert_eq!(support, vec![TableProviderFilterPushDown::Exact]);
        assert_eq!(plan.exact_count, 1);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.residual_filter_count, 0);

        Ok(())
    }

    #[test]
    fn filter_pushdown_reports_one_exact_status_for_partition_equality_and()
    -> Result<(), Box<dyn std::error::Error>> {
        const TWO_PARTITION_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-exact-partition-and",
            TWO_PARTITION_SCHEMA_FIELDS_JSON,
            r#"["region","day"]"#,
            r#""partitionValues":{"region":"us-west","day":"2026-05-31"}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let filter = col("region")
            .eq(lit("us-west"))
            .and(col("day").eq(lit("2026-05-31")));

        let support = provider.supports_filters_pushdown(&[&filter])?;
        let plan = provider.plan_supports_filters_pushdown(&[&filter]);

        assert_eq!(support, vec![TableProviderFilterPushDown::Exact]);
        assert_eq!(plan.exact_count, 1);
        assert_eq!(plan.pushed_filter_count, 1);
        assert_eq!(plan.residual_filter_count, 0);

        Ok(())
    }

    #[test]
    fn filter_pushdown_accepts_integer_data_stats_filters_as_inexact()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("filter-pushdown-integer-data-stats")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let id_filter = col("id").gt(lit(1_i32));
        let cross_width_id_filter = col("id").gt(lit(1_i64));

        let support = provider.supports_filters_pushdown(&[&id_filter])?;
        let plan = provider.plan_supports_filters_pushdown(&[&id_filter]);

        assert_eq!(support, vec![TableProviderFilterPushDown::Inexact]);
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 1);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, 1);
        assert_eq!(plan.residual_filter_count, 1);
        assert!(plan.decisions[0].kernel_scan_filter.is_some());

        let support = provider.supports_filters_pushdown(&[&cross_width_id_filter])?;

        assert_eq!(support, vec![TableProviderFilterPushDown::Unsupported]);

        Ok(())
    }

    #[test]
    fn filter_pushdown_accepts_same_width_integer_data_stats_widths()
    -> Result<(), Box<dyn std::error::Error>> {
        const INTEGER_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"byte_count\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"short_count\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"int_count\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"long_count\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-integer-data-stats-widths",
            INTEGER_DATA_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let byte_filter = col("byte_count").gt(int8_lit(1));
        let short_filter = col("short_count").gt(int16_lit(1));
        let int_filter = col("int_count").gt(lit(1_i32));
        let long_filter = col("long_count").gt(int64_lit(1));
        let cross_width_filter = col("int_count").gt(int64_lit(1));

        let support = provider.supports_filters_pushdown(&[
            &byte_filter,
            &short_filter,
            &int_filter,
            &long_filter,
            &cross_width_filter,
        ])?;

        assert_eq!(
            support,
            vec![
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
            ]
        );

        Ok(())
    }

    #[test]
    fn filter_pushdown_accepts_simple_integer_data_stats_operators()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("filter-pushdown-integer-data-stats-operators")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let filters = [
            col("id").eq(lit(7_i32)),
            col("id").not_eq(lit(7_i32)),
            col("id").lt(lit(7_i32)),
            col("id").lt_eq(lit(7_i32)),
            col("id").gt(lit(7_i32)),
            col("id").gt_eq(lit(7_i32)),
            lit(7_i32).lt(col("id")),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        assert_eq!(
            provider.supports_filters_pushdown(&filter_refs)?,
            vec![TableProviderFilterPushDown::Inexact; filters.len()]
        );

        Ok(())
    }

    #[test]
    fn filter_pushdown_accepts_boolean_null_count_data_stats_filters_as_inexact()
    -> Result<(), Box<dyn std::error::Error>> {
        const BOOLEAN_DATA_SCHEMA_FIELDS_JSON: &str =
            r#"[{\"name\":\"is_current\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-boolean-data-stats-null-counts",
            BOOLEAN_DATA_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let filters = [col("is_current").is_null(), col("is_current").is_not_null()];
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            provider.supports_filters_pushdown(&filter_refs)?,
            vec![TableProviderFilterPushDown::Inexact; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, filters.len());
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| {
            decision.residual
                && decision
                    .kernel_scan_filter
                    .as_ref()
                    .is_some_and(|filter| filter.kind == KernelScanFilterKind::DataStats)
        }));

        Ok(())
    }

    #[test]
    fn filter_pushdown_rejects_unproven_boolean_data_stats_shapes()
    -> Result<(), Box<dyn std::error::Error>> {
        const BOOLEAN_DATA_SCHEMA_FIELDS_JSON: &str =
            r#"[{\"name\":\"is_current\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-boolean-data-stats-unsupported",
            BOOLEAN_DATA_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let filters = [
            col("is_current").eq(lit(true)),
            col("is_current").not_eq(lit(true)),
            col("is_current"),
            Expr::Not(Box::new(col("is_current"))),
            col("is_current").in_list(vec![lit(true)], false),
            col("is_current").lt(lit(true)),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            provider.supports_filters_pushdown(&filter_refs)?,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Unsupported
                && decision.residual
                && decision.kernel_scan_filter.is_none()
        }));

        Ok(())
    }

    #[test]
    fn filter_pushdown_accepts_binary_null_count_data_stats_filters_as_inexact()
    -> Result<(), Box<dyn std::error::Error>> {
        const BINARY_DATA_SCHEMA_FIELDS_JSON: &str =
            r#"[{\"name\":\"payload\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-binary-data-stats-null-counts",
            BINARY_DATA_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let mut provider = DeltaTableProvider::try_new(source, preflight)?;
        provider.set_schema_for_tests(Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Binary, true),
            Field::new("large_payload", DataType::LargeBinary, true),
            Field::new("fixed_payload", DataType::FixedSizeBinary(3), true),
        ])));
        let filters = [
            col("payload").is_null(),
            col("payload").is_not_null(),
            col("large_payload").is_null(),
            col("large_payload").is_not_null(),
            col("fixed_payload").is_null(),
            col("fixed_payload").is_not_null(),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            provider.supports_filters_pushdown(&filter_refs)?,
            vec![TableProviderFilterPushDown::Inexact; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, filters.len());
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| {
            decision.residual
                && decision
                    .kernel_scan_filter
                    .as_ref()
                    .is_some_and(|filter| filter.kind == KernelScanFilterKind::DataStats)
        }));

        Ok(())
    }

    #[test]
    fn filter_pushdown_rejects_unproven_binary_data_stats_shapes()
    -> Result<(), Box<dyn std::error::Error>> {
        const BINARY_DATA_SCHEMA_FIELDS_JSON: &str =
            r#"[{\"name\":\"payload\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-binary-data-stats-unsupported",
            BINARY_DATA_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let mut provider = DeltaTableProvider::try_new(source, preflight)?;
        provider.set_schema_for_tests(Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Binary, true),
            Field::new("large_payload", DataType::LargeBinary, true),
            Field::new("fixed_payload", DataType::FixedSizeBinary(3), true),
        ])));
        let payload = binary_lit(b"hello");
        let filters = [
            col("payload").eq(payload.clone()),
            col("payload").not_eq(payload.clone()),
            col("payload").lt(payload.clone()),
            col("payload").gt_eq(payload.clone()),
            payload.clone().gt(col("payload")),
            col("payload").eq(binary_lit(b"")),
            col("payload").in_list(vec![payload.clone()], false),
            col("payload").between(binary_lit(b"a"), payload.clone()),
            col("large_payload").eq(large_binary_lit(b"hello")),
            col("fixed_payload").eq(fixed_size_binary_lit(3, b"hey")),
            col("fixed_payload").eq(fixed_size_binary_lit(4, b"heyo")),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            provider.supports_filters_pushdown(&filter_refs)?,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Unsupported
                && decision.residual
                && decision.kernel_scan_filter.is_none()
        }));

        Ok(())
    }

    #[test]
    fn filter_pushdown_accepts_decimal_data_stats_filters_as_inexact()
    -> Result<(), Box<dyn std::error::Error>> {
        const DECIMAL_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"amount\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-decimal-data-stats",
            DECIMAL_DATA_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let amount = decimal_lit(200);
        let filters = [
            col("amount").eq(amount.clone()),
            col("amount").not_eq(amount.clone()),
            col("amount").lt(amount.clone()),
            col("amount").lt_eq(amount.clone()),
            col("amount").gt(amount.clone()),
            col("amount").gt_eq(amount.clone()),
            amount.gt(col("amount")),
            col("amount").is_null(),
            col("amount").is_not_null(),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            provider.supports_filters_pushdown(&filter_refs)?,
            vec![TableProviderFilterPushDown::Inexact; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, filters.len());
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| {
            decision.residual
                && decision
                    .kernel_scan_filter
                    .as_ref()
                    .is_some_and(|filter| filter.kind == KernelScanFilterKind::DataStats)
        }));

        Ok(())
    }

    #[test]
    fn filter_pushdown_rejects_unproven_decimal_data_stats_shapes()
    -> Result<(), Box<dyn std::error::Error>> {
        const DECIMAL_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"amount\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-decimal-data-stats-unsupported",
            DECIMAL_DATA_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let amount = decimal_lit(200);
        let filters = [
            col("amount").eq(decimal_lit_with_type(2_000, 11, 3)),
            col("amount").eq(decimal_lit_with_type(200, 11, 2)),
            col("amount").eq(lit("2.00")),
            col("amount").in_list(vec![amount.clone()], false),
            col("amount").between(decimal_lit(0), amount.clone()),
            col("amount")
                .gt(decimal_lit(0))
                .or(col("amount").lt(amount.clone())),
            col("amount").eq(Expr::Literal(
                ScalarValue::Decimal256(Some(200.into()), 10, 2),
                None,
            )),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            provider.supports_filters_pushdown(&filter_refs)?,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Unsupported
                && decision.residual
                && decision.kernel_scan_filter.is_none()
        }));

        Ok(())
    }

    #[test]
    fn filter_pushdown_accepts_string_data_stats_filters_as_inexact()
    -> Result<(), Box<dyn std::error::Error>> {
        const STRING_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"large_customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-string-data-stats",
            STRING_DATA_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let mut provider = DeltaTableProvider::try_new(source, preflight)?;
        provider.set_schema_for_tests(Arc::new(Schema::new(vec![
            Field::new("customer_name", DataType::Utf8, true),
            Field::new("large_customer_name", DataType::LargeUtf8, true),
        ])));
        let name = lit("alice");
        let large_name = large_utf8_lit("alice");
        let filters = [
            col("customer_name").eq(name.clone()),
            col("customer_name").not_eq(name.clone()),
            col("customer_name").lt(name.clone()),
            col("customer_name").lt_eq(name.clone()),
            col("customer_name").gt(name.clone()),
            col("customer_name").gt_eq(name.clone()),
            name.clone().gt(col("customer_name")),
            col("customer_name").eq(large_name.clone()),
            col("large_customer_name").eq(name),
            col("customer_name").is_null(),
            col("customer_name").is_not_null(),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            provider.supports_filters_pushdown(&filter_refs)?,
            vec![TableProviderFilterPushDown::Inexact; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, filters.len());
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| {
            decision.residual
                && decision
                    .kernel_scan_filter
                    .as_ref()
                    .is_some_and(|filter| filter.kind == KernelScanFilterKind::DataStats)
        }));

        Ok(())
    }

    #[test]
    fn filter_pushdown_rejects_unproven_string_data_stats_shapes()
    -> Result<(), Box<dyn std::error::Error>> {
        const STRING_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-string-data-stats-unsupported",
            STRING_DATA_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let name = lit("alice");
        let filters = [
            col("customer_name").in_list(vec![name.clone()], false),
            col("customer_name").between(lit("a"), name.clone()),
            col("customer_name").like(lit("a%")),
            col("customer_name").eq(lit(7_i32)),
            col("customer_name")
                .gt(lit("a"))
                .or(col("customer_name").lt(name)),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            provider.supports_filters_pushdown(&filter_refs)?,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Unsupported
                && decision.residual
                && decision.kernel_scan_filter.is_none()
        }));

        Ok(())
    }

    #[test]
    fn filter_pushdown_accepts_floating_data_stats_filters_as_inexact()
    -> Result<(), Box<dyn std::error::Error>> {
        const FLOATING_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"float_score\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"double_score\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-floating-data-stats",
            FLOATING_DATA_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
        let double_value = Expr::Literal(ScalarValue::Float64(Some(2.25)), None);
        let filters = [
            col("float_score").eq(float_value.clone()),
            col("float_score").not_eq(float_value.clone()),
            col("float_score").lt(float_value.clone()),
            col("float_score").lt_eq(float_value.clone()),
            col("float_score").gt(float_value.clone()),
            col("float_score").gt_eq(float_value.clone()),
            float_value.gt(col("float_score")),
            col("double_score").eq(double_value.clone()),
            col("double_score").gt_eq(double_value),
            col("float_score").is_null(),
            col("float_score").is_not_null(),
            col("double_score").is_null(),
            col("double_score").is_not_null(),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            provider.supports_filters_pushdown(&filter_refs)?,
            vec![TableProviderFilterPushDown::Inexact; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, filters.len());
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| {
            decision.residual
                && decision
                    .kernel_scan_filter
                    .as_ref()
                    .is_some_and(|filter| filter.kind == KernelScanFilterKind::DataStats)
        }));

        Ok(())
    }

    #[test]
    fn filter_pushdown_rejects_unproven_floating_data_stats_shapes()
    -> Result<(), Box<dyn std::error::Error>> {
        const FLOATING_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"float_score\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"double_score\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]"#;
        let table = DeltaLogTable::new_with_schema(
            "filter-pushdown-floating-data-stats-unsupported",
            FLOATING_DATA_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            r#""partitionValues":{}"#,
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let float_zero = Expr::Literal(ScalarValue::Float32(Some(0.0)), None);
        let float_neg_zero = Expr::Literal(ScalarValue::Float32(Some(-0.0)), None);
        let float_nan = Expr::Literal(ScalarValue::Float32(Some(f32::NAN)), None);
        let float_inf = Expr::Literal(ScalarValue::Float32(Some(f32::INFINITY)), None);
        let double_value = Expr::Literal(ScalarValue::Float64(Some(1.5)), None);
        let filters = [
            col("float_score").eq(float_zero.clone()),
            col("float_score").lt(float_zero.clone()),
            col("float_score").gt_eq(float_zero),
            col("float_score").eq(float_neg_zero.clone()),
            col("float_score").gt(float_neg_zero),
            col("float_score").eq(float_nan),
            col("float_score").gt(float_inf),
            col("float_score").eq(double_value),
            col("double_score").eq(lit(1_i32)),
            col("float_score").between(lit(0.0_f32), lit(1.0_f32)),
            col("float_score").in_list(vec![lit(1.0_f32)], false),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            provider.supports_filters_pushdown(&filter_refs)?,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Unsupported
                && decision.residual
                && decision.rejection_reason
                    == Some(DeltaFilterPushdownRejectionReason::UnsupportedByPolicy)
                && decision.kernel_scan_filter.is_none()
        }));

        Ok(())
    }

    #[test]
    fn filter_pushdown_accepts_temporal_data_stats_filters_as_inexact()
    -> Result<(), Box<dyn std::error::Error>> {
        const TEMPORAL_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"event_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_ts_ntz\",\"type\":\"timestamp_ntz\",\"nullable\":true,\"metadata\":{}}]"#;
        const TIMESTAMP_NTZ_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}}"#;
        let table = DeltaLogTable::new_with_schema_protocol_and_adds(
            "filter-pushdown-temporal-data-stats",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TEMPORAL_DATA_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            &[r#""partitionValues":{}"#],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let date = date_lit(20_454);
        let timestamp = timestamp_lit(1_767_225_600_123_456, "UTC");
        let timestamp_ntz = timestamp_ntz_lit(1_767_225_600_123_456);
        let filters = [
            col("event_date").eq(date.clone()),
            col("event_date").not_eq(date.clone()),
            col("event_date").lt(date.clone()),
            col("event_date").lt_eq(date.clone()),
            col("event_date").gt(date.clone()),
            col("event_date").gt_eq(date.clone()),
            date.gt(col("event_date")),
            col("event_date").is_null(),
            col("event_date").is_not_null(),
            col("event_ts").eq(timestamp.clone()),
            col("event_ts").lt(timestamp.clone()),
            col("event_ts").lt_eq(timestamp.clone()),
            col("event_ts").gt(timestamp.clone()),
            col("event_ts").gt_eq(timestamp.clone()),
            timestamp.gt(col("event_ts")),
            col("event_ts").is_null(),
            col("event_ts").is_not_null(),
            col("event_ts_ntz").eq(timestamp_ntz.clone()),
            col("event_ts_ntz").lt(timestamp_ntz.clone()),
            col("event_ts_ntz").lt_eq(timestamp_ntz.clone()),
            col("event_ts_ntz").gt(timestamp_ntz.clone()),
            col("event_ts_ntz").gt_eq(timestamp_ntz.clone()),
            timestamp_ntz.gt(col("event_ts_ntz")),
            col("event_ts_ntz").is_null(),
            col("event_ts_ntz").is_not_null(),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            provider.supports_filters_pushdown(&filter_refs)?,
            vec![TableProviderFilterPushDown::Inexact; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, filters.len());
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| {
            decision.residual
                && decision
                    .kernel_scan_filter
                    .as_ref()
                    .is_some_and(|filter| filter.kind == KernelScanFilterKind::DataStats)
        }));

        Ok(())
    }

    #[test]
    fn filter_pushdown_rejects_unproven_temporal_data_stats_shapes()
    -> Result<(), Box<dyn std::error::Error>> {
        const TEMPORAL_DATA_SCHEMA_FIELDS_JSON: &str = r#"[{\"name\":\"event_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event_ts_ntz\",\"type\":\"timestamp_ntz\",\"nullable\":true,\"metadata\":{}}]"#;
        const TIMESTAMP_NTZ_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}}"#;
        let table = DeltaLogTable::new_with_schema_protocol_and_adds(
            "filter-pushdown-temporal-data-stats-unsupported",
            TIMESTAMP_NTZ_PROTOCOL_JSON,
            TEMPORAL_DATA_SCHEMA_FIELDS_JSON,
            r#"[]"#,
            &[r#""partitionValues":{}"#],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let date = date_lit(20_454);
        let timestamp = timestamp_lit(1_767_225_600_123_456, "UTC");
        let timestamp_ntz = timestamp_ntz_lit(1_767_225_600_123_456);
        let filters = [
            col("event_date").in_list(vec![date.clone()], false),
            col("event_date").between(date_lit(19_782), date.clone()),
            col("event_date").eq(lit("2026-01-01")),
            col("event_ts").not_eq(timestamp.clone()),
            col("event_ts").eq(timestamp_ntz.clone()),
            col("event_ts").eq(timestamp_lit(1_767_225_600_123_456, "America/Phoenix")),
            col("event_ts_ntz").not_eq(timestamp_ntz.clone()),
            col("event_ts_ntz").eq(timestamp),
            col("event_ts_ntz").eq(date),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = provider.plan_supports_filters_pushdown(&filter_refs);

        assert_eq!(
            provider.supports_filters_pushdown(&filter_refs)?,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Unsupported
                && decision.residual
                && decision.kernel_scan_filter.is_none()
        }));

        Ok(())
    }

    #[test]
    fn filter_pushdown_rejects_unproven_integer_data_stats_shapes()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("filter-pushdown-integer-data-stats-unsupported")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let filters = [
            col("id").in_list(vec![lit(1_i32), lit(2_i32)], false),
            col("id").between(lit(1_i32), lit(2_i32)),
            col("id").is_null(),
            col("id").is_not_null(),
            col("id").gt(lit(1_i32)).or(col("id").lt(lit(0_i32))),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        assert_eq!(
            provider.supports_filters_pushdown(&filter_refs)?,
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );

        Ok(())
    }

    #[test]
    fn filter_pushdown_accepts_string_null_checks_and_rejects_other_shapes()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_schema_and_adds(
            "filter-pushdown-partition-in",
            PARTITIONED_SCHEMA_FIELDS_JSON,
            r#"["region"]"#,
            &[
                r#""partitionValues":{"region":"us-west"}"#,
                r#""partitionValues":{"region":""}"#,
            ],
        )?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let partition_in_filter =
            col("region").in_list(vec![lit("us-west"), lit("us-east")], false);
        let data_between_filter = col("id").between(lit(10), lit(20));
        let mixed_and_filter = col("region").eq(lit("us-west")).and(col("id").gt(lit(10)));
        let mixed_or_filter = col("region").eq(lit("us-west")).or(col("id").gt(lit(10)));
        let not_filter = Expr::Not(Box::new(col("id").gt(lit(10))));
        let null_check_filter = col("region").is_not_null();

        let filters = [
            &partition_in_filter,
            &data_between_filter,
            &mixed_and_filter,
            &mixed_or_filter,
            &not_filter,
            &null_check_filter,
        ];
        let support = provider.supports_filters_pushdown(&filters)?;
        let plan = provider.plan_supports_filters_pushdown(&filters);

        assert_eq!(
            support,
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Exact,
            ]
        );
        assert_eq!(plan.exact_count, 2);
        assert_eq!(plan.inexact_count, 1);
        assert_eq!(plan.unsupported_count, 3);
        assert_eq!(plan.residual_filter_count, 4);

        Ok(())
    }

    #[test]
    fn filter_plan_empty_input_has_consistent_zero_counts() -> Result<(), Box<dyn std::error::Error>>
    {
        let table = DeltaLogTable::new("empty-filter-plan")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;

        let plan = provider.plan_supports_filters_pushdown(&[]);

        assert!(plan.datafusion_pushdowns().is_empty());
        assert!(plan.decisions.is_empty());
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, 0);

        Ok(())
    }

    #[test]
    fn filter_planning_contract_does_not_call_scan_or_read_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let filter_module_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("query_engine")
            .join("datafusion")
            .join("planning");
        let filter_source_files = [
            filter_module_root.join("filters.rs"),
            filter_module_root.join("filters").join("analysis.rs"),
            filter_module_root
                .join("filters")
                .join("partition_pushdown.rs"),
        ];
        let production_source = filter_source_files
            .iter()
            .map(fs::read_to_string)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|source| {
                source
                    .split("\n#[cfg(test)]")
                    .next()
                    .unwrap_or(source.as_str())
                    .to_owned()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(!production_source.contains("with_predicate"));
        assert!(!production_source.contains("with_filter"));
        assert!(!production_source.contains("DvInfo"));
        assert!(!production_source.contains("deletionVector"));
        assert!(!production_source.contains("get_selection_vector"));
        assert!(!production_source.contains("get_row_indexes"));
        assert!(!production_source.contains("RecordBatch"));
        assert!(!production_source.to_ascii_lowercase().contains("parquet"));

        Ok(())
    }

    #[test]
    fn filter_plan_reason_codes_are_control_character_safe()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("filter-plan-control-characters")?;
        let source = load_delta_source(DeltaSourceConfig {
            name: "orders".to_owned(),
            table_uri: table.path().to_string_lossy().to_string(),
            version: None,
        })?;
        let preflight = preflight_delta_protocol(&source)?;
        let provider = DeltaTableProvider::try_new(source, preflight)?;
        let hostile_filter = col("ghost\ncolumn").eq(lit("x"));

        let plan = provider.plan_supports_filters_pushdown(&[&hostile_filter]);
        let reason_code = plan.decisions[0]
            .rejection_reason
            .map(DeltaFilterPushdownRejectionReason::code);

        assert_eq!(
            plan.decisions[0].rejection_reason,
            Some(DeltaFilterPushdownRejectionReason::UnknownColumn)
        );
        assert_eq!(reason_code.map(|code| code.contains('\n')), Some(false));
        assert_eq!(reason_code.map(|code| code.contains('\r')), Some(false));
        assert_eq!(reason_code.map(|code| code.contains('\t')), Some(false));
        assert_eq!(reason_code, Some("unsupported_unknown_column"));

        Ok(())
    }
}
