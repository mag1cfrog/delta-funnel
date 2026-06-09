//! Static partition filter pushdown policy.

use std::collections::HashSet;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{Expr, Operator, lit};

use super::analysis::{DeltaFilterColumnScope, analyze_filter_for_pushdown};
use super::{DeltaFilterPushdownDecision, DeltaFilterPushdownOutcome, DeltaFilterPushdownPlan};

/// Plans the exact static partition operator policy for kernel-native pruning.
///
/// A filter can be exact here only when it is partition-only and accepted by
/// the kernel predicate path. #66 keeps the production subset intentionally
/// conservative: string equality, inequality, ordering, `IN`, `NOT IN`,
/// `BETWEEN`, `NOT BETWEEN`, `NOT`, string null checks, and boolean `AND`/`OR`
/// composition where every leaf is in that subset. All other shapes stay
/// `Unsupported` so DataFusion keeps them as residual filters.
pub(super) fn plan_partition_operator_pushdown(
    filters: &[&Expr],
    schema: &SchemaRef,
    partition_columns: &HashSet<String>,
) -> DeltaFilterPushdownPlan {
    let decisions = filters
        .iter()
        .enumerate()
        .map(|(input_index, filter)| {
            partition_operator_decision(input_index, filter, schema, partition_columns)
        })
        .collect::<Vec<_>>();

    DeltaFilterPushdownPlan::from_decisions(decisions)
}

/// Converts one candidate filter into either an exact partition decision or a
/// conservative unsupported decision.
///
/// Filter analysis is still preserved for unsupported decisions so diagnostics
/// remain useful, but unsupported predicates are not provider owned and must not
/// affect scan planning.
fn partition_operator_decision(
    input_index: usize,
    filter: &Expr,
    schema: &SchemaRef,
    partition_columns: &HashSet<String>,
) -> DeltaFilterPushdownDecision {
    let (filter_analysis, rejection_reason) =
        analyze_filter_for_pushdown(filter, schema, partition_columns);
    let is_partition_only = filter_analysis.scope == DeltaFilterColumnScope::PartitionOnly;

    if is_partition_only
        && let Some(kernel_scan_filter) =
            supported_partition_operator_kernel_scan_filter(filter, schema)
    {
        return DeltaFilterPushdownDecision {
            input_index,
            outcome: DeltaFilterPushdownOutcome::Exact,
            residual: false,
            rejection_reason: None,
            filter_analysis,
            kernel_scan_filter: Some(kernel_scan_filter),
        };
    }

    DeltaFilterPushdownDecision {
        input_index,
        outcome: DeltaFilterPushdownOutcome::Unsupported,
        residual: true,
        rejection_reason: Some(rejection_reason),
        filter_analysis,
        kernel_scan_filter: None,
    }
}

/// Returns the DataFusion expression that should be converted and passed to
/// kernel scan planning for an exact partition filter. Most accepted filters
/// are passed through unchanged, but empty `IN` and `NOT IN` lists need
/// explicit rewrites so `NOT IN ()` does not become a literal true predicate
/// that includes null partitions.
fn supported_partition_operator_kernel_scan_filter(
    filter: &Expr,
    schema: &SchemaRef,
) -> Option<Expr> {
    match filter {
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
            let left =
                supported_partition_operator_kernel_scan_filter(binary.left.as_ref(), schema)?;
            let right =
                supported_partition_operator_kernel_scan_filter(binary.right.as_ref(), schema)?;

            Some(match binary.op {
                Operator::And => left.and(right),
                Operator::Or => left.or(right),
                _ => return None,
            })
        }
        Expr::InList(in_list) if in_list.list.is_empty() => {
            let Expr::Column(column) = in_list.expr.as_ref() else {
                return None;
            };

            if !is_supported_string_partition_column(column, schema) {
                return None;
            }

            if in_list.negated {
                Some(in_list.expr.as_ref().clone().is_not_null())
            } else {
                Some(lit(false))
            }
        }
        Expr::Not(inner) => {
            let inner = supported_partition_operator_kernel_scan_filter(inner.as_ref(), schema)?;
            Some(Expr::Not(Box::new(inner)))
        }
        _ if is_supported_partition_operator_filter(filter, schema) => Some(filter.clone()),
        _ => None,
    }
}

/// Checks whether the expression shape is supported by the kernel-native exact
/// partition operator policy for this migration slice.
///
/// Column membership is intentionally checked by `analyze_filter_for_pushdown`;
/// this helper verifies accepted leaf predicates or boolean composition whose
/// leaves are accepted predicates. The production subset stays narrow: string
/// equality, inequality, ordering, `IN`, `NOT IN`, `BETWEEN`, `NOT BETWEEN`,
/// `NOT`, string null checks, and boolean `AND`/`OR` composition only.
fn is_supported_partition_operator_filter(filter: &Expr, schema: &SchemaRef) -> bool {
    match filter {
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
            is_supported_partition_operator_filter(binary.left.as_ref(), schema)
                && is_supported_partition_operator_filter(binary.right.as_ref(), schema)
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            is_supported_partition_equality(binary.left.as_ref(), binary.right.as_ref(), schema)
                || is_supported_partition_equality(
                    binary.right.as_ref(),
                    binary.left.as_ref(),
                    schema,
                )
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::NotEq => {
            is_supported_partition_equality(binary.left.as_ref(), binary.right.as_ref(), schema)
                || is_supported_partition_equality(
                    binary.right.as_ref(),
                    binary.left.as_ref(),
                    schema,
                )
        }
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
            ) =>
        {
            is_supported_partition_comparison(binary.left.as_ref(), binary.right.as_ref(), schema)
                || is_supported_partition_comparison(
                    binary.right.as_ref(),
                    binary.left.as_ref(),
                    schema,
                )
        }
        Expr::InList(in_list) => is_supported_partition_in_list(in_list, schema),
        Expr::Between(between) => is_supported_partition_between(between, schema),
        Expr::Not(inner) => is_supported_partition_operator_filter(inner.as_ref(), schema),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            is_supported_partition_null_check(inner.as_ref(), schema)
        }
        _ => false,
    }
}

/// Accepts one column/literal equality if the metadata type policy can prove it.
fn is_supported_partition_equality(column: &Expr, literal: &Expr, schema: &SchemaRef) -> bool {
    is_supported_partition_column_literal_pair(column, literal, schema)
}

fn is_supported_partition_comparison(column: &Expr, literal: &Expr, schema: &SchemaRef) -> bool {
    is_supported_partition_column_literal_pair(column, literal, schema)
}

/// Accepts one partition column paired with a literal whose metadata semantics are proven.
fn is_supported_partition_column_literal_pair(
    column: &Expr,
    literal: &Expr,
    schema: &SchemaRef,
) -> bool {
    let Expr::Column(column) = column else {
        return false;
    };

    is_supported_partition_literal_for_column(column, literal, schema)
}

/// Accepts null checks only for string partition columns in the #66 subset.
fn is_supported_partition_null_check(expr: &Expr, schema: &SchemaRef) -> bool {
    let Expr::Column(column) = expr else {
        return false;
    };

    is_supported_string_partition_column(column, schema)
}

fn is_supported_partition_in_list(
    in_list: &datafusion::logical_expr::expr::InList,
    schema: &SchemaRef,
) -> bool {
    if in_list.list.is_empty() {
        return false;
    }

    let Expr::Column(column) = in_list.expr.as_ref() else {
        return false;
    };

    if !is_supported_string_partition_column(column, schema) {
        return false;
    }

    in_list
        .list
        .iter()
        .all(|literal| is_supported_partition_literal_for_column(column, literal, schema))
}

fn is_supported_partition_between(
    between: &datafusion::logical_expr::expr::Between,
    schema: &SchemaRef,
) -> bool {
    let Expr::Column(column) = between.expr.as_ref() else {
        return false;
    };

    is_supported_partition_literal_for_column(column, between.low.as_ref(), schema)
        && is_supported_partition_literal_for_column(column, between.high.as_ref(), schema)
}

fn is_supported_string_partition_column(column: &Column, schema: &SchemaRef) -> bool {
    if column.relation.is_some() || column.name.contains('.') {
        return false;
    }

    schema
        .field_with_name(&column.name)
        .is_ok_and(|field| matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8))
}

fn is_supported_partition_literal_for_column(
    column: &Column,
    literal: &Expr,
    schema: &SchemaRef,
) -> bool {
    if !is_supported_string_partition_column(column, schema) {
        return false;
    }

    let Ok(field) = schema.field_with_name(&column.name) else {
        return false;
    };

    matches!(
        (field.data_type(), literal),
        (
            DataType::Utf8 | DataType::LargeUtf8,
            Expr::Literal(ScalarValue::Utf8(Some(_)), _)
                | Expr::Literal(ScalarValue::LargeUtf8(Some(_)), _),
        )
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{
        ColumnarValue, Expr, TableProviderFilterPushDown, Volatility, col, create_udf, lit,
    };

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("large_region", DataType::LargeUtf8, true),
            Field::new("day", DataType::Utf8, true),
            Field::new("is_current", DataType::Boolean, true),
            Field::new("event_date", DataType::Date32, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("float_part", DataType::Float32, true),
            Field::new("double_part", DataType::Float64, true),
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
            Field::new("payload", DataType::Binary, true),
        ]))
    }

    fn partition_columns(names: &[&str]) -> HashSet<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    fn assert_all_unsupported(plan: &DeltaFilterPushdownPlan, len: usize) {
        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Unsupported; len]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, len);
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, len);
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Unsupported
                && decision.residual
                && decision.kernel_scan_filter.is_none()
        }));
    }

    #[test]
    fn supported_partition_equality_filter_is_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let filter = col("region").eq(lit("us-west"));

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[&filter],
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Exact]
        );
        assert_eq!(plan.exact_count, 1);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, 1);
        assert_eq!(plan.residual_filter_count, 0);
        assert_eq!(plan.decisions[0].outcome, DeltaFilterPushdownOutcome::Exact);
        assert!(!plan.decisions[0].residual);
        assert!(plan.decisions[0].rejection_reason.is_none());
        assert_eq!(plan.decisions[0].kernel_scan_filter, Some(filter));
        assert_eq!(
            plan.decisions[0].filter_analysis.scope,
            DeltaFilterColumnScope::PartitionOnly
        );
        assert!(plan.decisions[0].filter_analysis.kernel_predicate.is_some());
    }

    #[test]
    fn supported_partition_equality_and_filter_is_exact_as_one_input() {
        let schema = schema();
        let partition_columns = partition_columns(&["region", "day"]);
        let filter = col("region")
            .eq(lit("us-west"))
            .and(col("day").eq(lit("2026-05-31")));

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[&filter],
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Exact]
        );
        assert_eq!(plan.exact_count, 1);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.residual_filter_count, 0);
        assert_eq!(
            plan.decisions[0].filter_analysis.partition_columns,
            vec!["day", "region"]
        );
    }

    #[test]
    fn partition_operator_planner_accepts_string_inequality_and_in_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let filters = [
            col("region").not_eq(lit("us-west")),
            lit("us-west").not_eq(col("region")),
            col("region").in_list(vec![lit("us-west"), lit("us-east"), lit("us-west")], false),
            col("region").in_list(vec![lit("us-west"), lit("us-east")], true),
            col("region")
                .not_eq(lit("us-west"))
                .and(col("region").in_list(vec![lit("us-east")], false)),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Exact; filters.len()]
        );
        assert_eq!(plan.exact_count, filters.len());
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, filters.len());
        assert_eq!(plan.residual_filter_count, 0);
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Exact
                && !decision.residual
                && decision.rejection_reason.is_none()
                && decision.filter_analysis.scope == DeltaFilterColumnScope::PartitionOnly
                && decision.filter_analysis.kernel_predicate.is_some()
                && decision.filter_analysis.kernel_adapter_error.is_none()
                && decision.kernel_scan_filter.is_some()
        }));
    }

    #[test]
    fn partition_operator_planner_accepts_large_utf8_string_operators_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["large_region"]);
        let large_west = Expr::Literal(ScalarValue::LargeUtf8(Some("us-west".to_owned())), None);
        let large_east = Expr::Literal(ScalarValue::LargeUtf8(Some("us-east".to_owned())), None);
        let filters = [
            col("large_region").eq(large_west.clone()),
            col("large_region").not_eq(large_west.clone()),
            col("large_region").in_list(vec![large_west.clone(), large_east], false),
            col("large_region").in_list(vec![large_west.clone()], true),
            col("large_region").lt(large_west.clone()),
            col("large_region").between(lit("a"), large_west.clone()),
            col("large_region").not_between(lit("a"), large_west.clone()),
            col("large_region").is_null(),
            col("large_region")
                .eq(large_west.clone())
                .or(col("large_region").is_null()),
            col("large_region")
                .not_eq(large_west)
                .and(col("large_region").is_not_null()),
            Expr::Not(Box::new(col("large_region").is_null())),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Exact; filters.len()]
        );
        assert_eq!(plan.exact_count, filters.len());
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, filters.len());
        assert_eq!(plan.residual_filter_count, 0);
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Exact
                && !decision.residual
                && decision.filter_analysis.partition_columns == vec!["large_region"]
                && decision.filter_analysis.kernel_predicate.is_some()
                && decision.kernel_scan_filter.is_some()
        }));
    }

    #[test]
    fn partition_operator_planner_rejects_unsafe_string_membership_shapes() {
        let schema = schema();
        let partition_columns = partition_columns(&["region", "day"]);
        let large_null = Expr::Literal(ScalarValue::LargeUtf8(None), None);
        let large_west = Expr::Literal(ScalarValue::LargeUtf8(Some("us-west".to_owned())), None);
        let filters = [
            col("region").in_list(vec![Expr::Literal(ScalarValue::Utf8(None), None)], false),
            col("region").in_list(
                vec![lit("us-west"), Expr::Literal(ScalarValue::Utf8(None), None)],
                false,
            ),
            col("region").in_list(
                vec![lit("us-west"), Expr::Literal(ScalarValue::Utf8(None), None)],
                true,
            ),
            col("region").in_list(vec![lit(1_i64)], false),
            col("region").in_list(vec![col("day")], false),
            col("large_region").in_list(Vec::<Expr>::new(), false),
            col("large_region").in_list(Vec::<Expr>::new(), true),
            col("large_region").in_list(vec![large_west, large_null], false),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_accepts_string_or_as_full_kernel_predicate() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let filter = col("region")
            .eq(lit("us-west"))
            .or(col("region").eq(lit("us-east")));

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[&filter],
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Exact]
        );
        assert_eq!(plan.exact_count, 1);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.residual_filter_count, 0);
        assert_eq!(plan.decisions[0].kernel_scan_filter, Some(filter));
    }

    #[test]
    fn partition_operator_planner_accepts_string_null_checks_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let filters = [col("region").is_null(), col("region").is_not_null()];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
            ]
        );
        assert_eq!(plan.exact_count, 2);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, 2);
        assert_eq!(plan.residual_filter_count, 0);
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Exact
                && !decision.residual
                && decision.rejection_reason.is_none()
                && decision.filter_analysis.scope == DeltaFilterColumnScope::PartitionOnly
                && decision.filter_analysis.kernel_predicate.is_some()
                && decision.filter_analysis.kernel_adapter_error.is_none()
                && decision.kernel_scan_filter.is_some()
        }));
    }

    #[test]
    fn partition_operator_planner_downgrades_boolean_null_checks_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["is_current"]);
        let filters = [col("is_current").is_null(), col("is_current").is_not_null()];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_decimal_null_checks_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["amount"]);
        let filters = [col("amount").is_null(), col("amount").is_not_null()];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_decimal_equality_and_membership_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["amount"]);
        let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
        let same_amount_different_scale =
            Expr::Literal(ScalarValue::Decimal128(Some(123_450), 12, 3), None);
        let filters = [
            col("amount").eq(amount.clone()),
            amount.clone().eq(col("amount")),
            col("amount").not_eq(amount.clone()),
            col("amount").in_list(
                vec![amount.clone(), same_amount_different_scale, amount.clone()],
                false,
            ),
            col("amount").in_list(vec![amount], true),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_decimal_comparisons_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["amount"]);
        let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
        let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
        let negative = Expr::Literal(ScalarValue::Decimal128(Some(-1_230), 12, 3), None);
        let filters = [
            col("amount").lt(amount.clone()),
            col("amount").lt_eq(negative),
            col("amount").gt(zero),
            amount.clone().lt_eq(col("amount")),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_floating_null_checks_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["float_part", "double_part"]);
        let filters = [
            col("float_part").is_null(),
            col("float_part").is_not_null(),
            col("double_part").is_null(),
            col("double_part").is_not_null(),
            col("float_part")
                .is_null()
                .and(col("double_part").is_not_null()),
            Expr::Not(Box::new(col("float_part").is_null())),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_timestamp_null_checks_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["event_ts"]);
        let filters = [
            col("event_ts").is_null(),
            col("event_ts").is_not_null(),
            Expr::Not(Box::new(col("event_ts").is_null())),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_timestamp_ntz_null_checks_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["event_ts_ntz"]);
        let filters = [
            col("event_ts_ntz").is_null(),
            col("event_ts_ntz").is_not_null(),
            Expr::Not(Box::new(col("event_ts_ntz").is_null())),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_binary_null_checks_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["payload"]);
        let filters = [
            col("payload").is_null(),
            col("payload").is_not_null(),
            Expr::Not(Box::new(col("payload").is_null())),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_binary_equality_and_membership_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["payload"]);
        let payload = Expr::Literal(ScalarValue::Binary(Some(b"hello".to_vec())), None);
        let other = Expr::Literal(ScalarValue::Binary(Some(b"/=%".to_vec())), None);
        let filters = [
            col("payload").eq(payload.clone()),
            payload.clone().eq(col("payload")),
            col("payload").not_eq(payload.clone()),
            col("payload").in_list(vec![payload.clone(), other, payload.clone()], false),
            col("payload").in_list(vec![payload], true),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_timestamp_equality_membership_and_ranges_until_typed_child()
     {
        let schema = schema();
        let partition_columns = partition_columns(&["event_ts"]);
        let timestamp = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), Some("UTC".into())),
            None,
        );
        let other = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_000_000), Some("UTC".into())),
            None,
        );
        let filters = [
            col("event_ts").eq(timestamp.clone()),
            timestamp.clone().eq(col("event_ts")),
            col("event_ts").not_eq(timestamp.clone()),
            col("event_ts").in_list(vec![timestamp.clone(), other], false),
            col("event_ts").in_list(vec![timestamp.clone()], true),
            col("event_ts").gt(timestamp.clone()),
            timestamp.clone().gt(col("event_ts")),
            col("event_ts").between(timestamp.clone(), timestamp.clone()),
            col("event_ts").not_between(timestamp.clone(), timestamp),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_timestamp_ntz_equality_membership_and_ranges_until_typed_child()
     {
        let schema = schema();
        let partition_columns = partition_columns(&["event_ts_ntz"]);
        let timestamp = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
            None,
        );
        let other = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_000_000), None),
            None,
        );
        let filters = [
            col("event_ts_ntz").eq(timestamp.clone()),
            timestamp.clone().eq(col("event_ts_ntz")),
            col("event_ts_ntz").not_eq(timestamp.clone()),
            col("event_ts_ntz").in_list(vec![timestamp.clone(), other], false),
            col("event_ts_ntz").in_list(vec![timestamp.clone()], true),
            col("event_ts_ntz").gt(timestamp.clone()),
            timestamp.clone().gt(col("event_ts_ntz")),
            col("event_ts_ntz").between(timestamp.clone(), timestamp.clone()),
            col("event_ts_ntz").not_between(timestamp.clone(), timestamp),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_rejects_unproven_timestamp_literal_filters() {
        let schema = schema();
        let partition_columns = partition_columns(&["event_ts"]);
        let timestamp = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), Some("UTC".into())),
            None,
        );
        let low = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_000_000), Some("UTC".into())),
            None,
        );
        let timestamp_without_timezone = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
            None,
        );
        let timestamp_non_utc_timezone = Expr::Literal(
            ScalarValue::TimestampMicrosecond(
                Some(1_767_225_600_123_456),
                Some("America/Phoenix".into()),
            ),
            None,
        );
        let null_timestamp = Expr::Literal(
            ScalarValue::TimestampMicrosecond(None, Some("UTC".into())),
            None,
        );
        let filters = [
            col("event_ts").eq(timestamp_without_timezone.clone()),
            col("event_ts").eq(timestamp_non_utc_timezone.clone()),
            col("event_ts").eq(null_timestamp.clone()),
            col("event_ts").gt(timestamp_without_timezone),
            col("event_ts").gt(timestamp_non_utc_timezone),
            col("event_ts").between(low, null_timestamp),
            col("event_ts").between(col("id"), timestamp),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
    }

    #[test]
    fn partition_operator_planner_rejects_unproven_timestamp_ntz_literal_filters() {
        let schema = schema();
        let partition_columns = partition_columns(&["event_ts_ntz"]);
        let timestamp = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
            None,
        );
        let timestamp_utc = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), Some("UTC".into())),
            None,
        );
        let null_timestamp = Expr::Literal(ScalarValue::TimestampMicrosecond(None, None), None);
        let filters = [
            col("event_ts_ntz").eq(timestamp_utc.clone()),
            col("event_ts_ntz").gt(timestamp_utc),
            col("event_ts_ntz").between(timestamp, null_timestamp),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_floating_equality_and_membership_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["float_part", "double_part"]);
        let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
        let negative_zero = Expr::Literal(ScalarValue::Float32(Some(-0.0)), None);
        let double_value = Expr::Literal(ScalarValue::Float64(Some(-2.25)), None);
        let filters = [
            col("float_part").eq(float_value.clone()),
            float_value.clone().eq(col("float_part")),
            col("float_part").not_eq(float_value.clone()),
            col("float_part").in_list(vec![float_value.clone(), negative_zero], false),
            col("float_part").in_list(vec![float_value], true),
            col("double_part").eq(double_value.clone()),
            col("double_part").in_list(vec![double_value.clone(), double_value], false),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_floating_comparisons_and_between_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["float_part", "double_part"]);
        let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
        let negative_zero = Expr::Literal(ScalarValue::Float32(Some(-0.0)), None);
        let double_value = Expr::Literal(ScalarValue::Float64(Some(-2.25)), None);
        let double_high = Expr::Literal(ScalarValue::Float64(Some(0.0)), None);
        let filters = [
            col("float_part").lt(float_value.clone()),
            col("float_part").lt_eq(negative_zero.clone()),
            col("float_part").gt(negative_zero.clone()),
            float_value.clone().lt_eq(col("float_part")),
            col("float_part").between(negative_zero.clone(), float_value.clone()),
            col("float_part").not_between(negative_zero, float_value),
            col("double_part").gt_eq(double_value.clone()),
            col("double_part").between(double_value, double_high),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_decimal_between_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["amount"]);
        let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
        let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
        let filters = [
            col("amount").between(zero.clone(), amount.clone()),
            col("amount").not_between(zero, amount),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_rejects_unproven_decimal_literal_shapes() {
        let schema = schema();
        let partition_columns = partition_columns(&["amount"]);
        let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
        let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
        let non_exact_scale = Expr::Literal(ScalarValue::Decimal128(Some(12_346), 10, 3), None);
        let filters = [
            col("amount").eq(non_exact_scale.clone()),
            col("amount").in_list(vec![amount.clone(), non_exact_scale.clone()], false),
            col("amount").lt(non_exact_scale.clone()),
            col("amount").between(zero, non_exact_scale),
            col("amount").eq(lit("123.45")),
            col("amount").eq(lit(123_i64)),
            col("amount").eq(lit(123.45_f64)),
            col("amount").eq(Expr::Literal(ScalarValue::Decimal128(None, 10, 2), None)),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_boolean_equality_and_membership_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["is_current"]);
        let filters = [
            col("is_current").eq(lit(true)),
            lit(false).eq(col("is_current")),
            col("is_current").not_eq(lit(false)),
            col("is_current").in_list(vec![lit(true), lit(false), lit(true)], false),
            col("is_current").in_list(vec![lit(true)], true),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_rejects_unproven_boolean_literal_shapes() {
        let schema = schema();
        let partition_columns = partition_columns(&["is_current"]);
        let filters = [
            col("is_current").eq(lit("true")),
            col("is_current").eq(Expr::Literal(ScalarValue::Boolean(None), None)),
            col("is_current").in_list(
                vec![lit(true), Expr::Literal(ScalarValue::Boolean(None), None)],
                false,
            ),
            col("is_current").in_list(vec![lit(true), lit("false")], false),
            col("is_current").in_list(vec![col("region")], false),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_boolean_shorthand_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["is_current"]);
        let filters = [col("is_current"), Expr::Not(Box::new(col("is_current")))];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_rejects_non_boolean_shorthand() {
        let schema = schema();
        let partition_columns = partition_columns(&["region", "id"]);
        let filters = [col("region"), col("id"), Expr::Not(Box::new(col("region")))];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
    }

    #[test]
    fn partition_operator_planner_rejects_boolean_ordering_and_between() {
        let schema = schema();
        let partition_columns = partition_columns(&["is_current"]);
        let filters = [
            col("is_current").lt(lit(true)),
            col("is_current").lt_eq(lit(false)),
            col("is_current").gt(lit(false)),
            col("is_current").gt_eq(lit(true)),
            col("is_current").between(lit(false), lit(true)),
            col("is_current").not_between(lit(false), lit(true)),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_date_equality_and_membership_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["event_date"]);
        let new_year_2026 = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
        let leap_day_2024 = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
        let filters = [
            col("event_date").eq(new_year_2026.clone()),
            new_year_2026.clone().eq(col("event_date")),
            col("event_date").not_eq(new_year_2026.clone()),
            col("event_date").in_list(
                vec![new_year_2026.clone(), leap_day_2024, new_year_2026.clone()],
                false,
            ),
            col("event_date").in_list(vec![new_year_2026], true),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_rejects_unproven_date_shapes() {
        let schema = schema();
        let partition_columns = partition_columns(&["event_date"]);
        let date = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
        let filters = [
            col("event_date").eq(lit("2026-01-01")),
            col("event_date").eq(Expr::Literal(ScalarValue::Date32(None), None)),
            col("event_date").eq(Expr::Literal(
                ScalarValue::Date64(Some(1_767_225_600_000)),
                None,
            )),
            col("event_date").in_list(
                vec![date.clone(), Expr::Literal(ScalarValue::Date32(None), None)],
                false,
            ),
            col("event_date").in_list(vec![date.clone(), lit("2024-02-29")], false),
            col("event_date").in_list(vec![col("day")], false),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_date_partition_comparisons_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["event_date"]);
        let new_year_2026 = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
        let leap_day_2024 = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
        let pre_epoch_day = Expr::Literal(ScalarValue::Date32(Some(-1)), None);
        let filters = [
            col("event_date").lt(new_year_2026.clone()),
            col("event_date").lt_eq(pre_epoch_day),
            col("event_date").gt(leap_day_2024),
            col("event_date").gt_eq(new_year_2026.clone()),
            new_year_2026.gt(col("event_date")),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_downgrades_date_partition_between_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["event_date"]);
        let new_year_2026 = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
        let leap_day_2024 = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
        let filters = [
            col("event_date").between(leap_day_2024.clone(), new_year_2026.clone()),
            col("event_date").not_between(leap_day_2024, new_year_2026),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_accepts_or_null_composition_as_full_kernel_predicate() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let filter = col("region").is_null().or(col("region").eq(lit("us-west")));

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[&filter],
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Exact]
        );
        assert_eq!(plan.exact_count, 1);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.residual_filter_count, 0);
        assert_eq!(plan.decisions[0].kernel_scan_filter, Some(filter));
    }

    #[test]
    fn partition_operator_planner_accepts_negated_string_wrappers_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let filters = [
            Expr::Not(Box::new(col("region").eq(lit("us-west")))),
            col("region").in_list(vec![lit("us-west"), lit("us-east")], true),
            Expr::Not(Box::new(
                col("region").in_list(vec![lit("us-west"), lit("us-east")], false),
            )),
            Expr::Not(Box::new(col("region").between(lit("a"), lit("z")))),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Exact; filters.len()]
        );
        assert_eq!(plan.exact_count, filters.len());
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.residual_filter_count, 0);
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Exact
                && !decision.residual
                && decision.filter_analysis.kernel_predicate.is_some()
                && decision.kernel_scan_filter.is_some()
        }));
    }

    #[test]
    fn partition_operator_planner_accepts_string_partition_comparisons_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let filters = [
            col("region").lt(lit("us-west")),
            col("region").lt_eq(lit("us-west")),
            col("region").gt(lit("us-east")),
            col("region").gt_eq(lit("us-east")),
            col("region").gt(lit("")),
            lit("us-east").lt(col("region")),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Exact; filters.len()]
        );
        assert_eq!(plan.exact_count, filters.len());
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.residual_filter_count, 0);
    }

    #[test]
    fn partition_operator_planner_downgrades_integer_partition_comparisons_until_typed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["id"]);
        let filters = [
            col("id").lt(lit(10_i64)),
            col("id").lt_eq(lit(10_i64)),
            col("id").gt(lit(-10_i64)),
            col("id").gt_eq(lit(-10_i64)),
            lit(10_i64).gt(col("id")),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_accepts_string_between_and_rejects_integer_between() {
        let schema = schema();
        let partition_columns = partition_columns(&["region", "id"]);
        let filters = [
            col("region").between(lit(""), lit("z")),
            col("region").between(lit("a"), lit("z")),
            col("region").not_between(lit("a"), lit("z")),
            col("id").between(lit(1_i64), lit(9_i64)),
            col("id").not_between(lit(1_i64), lit(9_i64)),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
            ]
        );
        assert_eq!(plan.exact_count, 3);
        assert_eq!(plan.unsupported_count, 2);
        assert_eq!(plan.residual_filter_count, 2);
    }

    #[test]
    fn partition_operator_planner_accepts_empty_string_equality_and_in_literal() {
        let schema = schema();
        let partition_columns = partition_columns(&["region", "id"]);
        let empty_in = col("region").in_list(Vec::<Expr>::new(), false);
        let empty_not_in = col("region").in_list(Vec::<Expr>::new(), true);
        let null_in = col("region").in_list(
            vec![lit("us-west"), Expr::Literal(ScalarValue::Utf8(None), None)],
            false,
        );
        let empty_string_equality = col("region").eq(lit(""));
        let empty_string_in = col("region").in_list(vec![lit("us-west"), lit("")], false);
        let empty_string_not_in = col("region").in_list(vec![lit("us-west"), lit("")], true);
        let null_not_in = col("region").in_list(
            vec![lit("us-west"), Expr::Literal(ScalarValue::Utf8(None), None)],
            true,
        );
        let non_string_literal_in = col("region").in_list(vec![lit(7_i64)], false);
        let non_string_partition_in = col("id").in_list(vec![lit("7")], false);
        let non_literal_in = col("region").in_list(vec![col("day")], false);

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[
                &empty_in,
                &empty_not_in,
                &null_in,
                &empty_string_equality,
                &empty_string_in,
                &empty_string_not_in,
                &null_not_in,
                &non_string_literal_in,
                &non_string_partition_in,
                &non_literal_in,
            ],
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
            ]
        );
        assert_eq!(plan.exact_count, 5);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 5);
        assert_eq!(plan.pushed_filter_count, 5);
        assert_eq!(plan.residual_filter_count, 5);
        assert!(!plan.decisions[0].residual);
        assert!(!plan.decisions[1].residual);
        assert!(!plan.decisions[3].residual);
        assert!(!plan.decisions[4].residual);
        assert!(!plan.decisions[5].residual);
        assert_eq!(plan.decisions[0].kernel_scan_filter, Some(lit(false)));
        assert_eq!(
            plan.decisions[1].kernel_scan_filter,
            Some(col("region").is_not_null())
        );
        assert_eq!(
            plan.decisions[3].kernel_scan_filter,
            Some(empty_string_equality)
        );
        assert_eq!(plan.decisions[4].kernel_scan_filter, Some(empty_string_in));
        assert_eq!(
            plan.decisions[5].kernel_scan_filter,
            Some(empty_string_not_in)
        );
        assert!(
            plan.decisions
                .iter()
                .enumerate()
                .filter(|(index, _)| !matches!(*index, 0..=1 | 3..=5))
                .all(|(_, decision)| decision.residual
                    && decision.outcome == DeltaFilterPushdownOutcome::Unsupported
                    && decision.kernel_scan_filter.is_none())
        );
    }

    #[test]
    fn partition_operator_planner_preserves_multiple_input_statuses() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let exact = col("region").eq(lit("us-west"));
        let unsupported_data = col("id").eq(lit(7_i64));
        let duplicate_exact = col("region").eq(lit("us-west"));

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[&exact, &unsupported_data, &duplicate_exact],
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Exact,
            ]
        );
        assert_eq!(plan.exact_count, 2);
        assert_eq!(plan.unsupported_count, 1);
        assert_eq!(plan.pushed_filter_count, 2);
        assert_eq!(plan.residual_filter_count, 1);
        assert_eq!(
            plan.decisions
                .iter()
                .map(|decision| decision.input_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn partition_operator_planner_downgrades_mixed_and_until_mixed_extraction_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["region", "day"]);
        let region_filter = col("region").eq(lit("us-west"));
        let day_filter = col("day").eq(lit("2026-05-31"));
        let data_filter = col("id").gt(lit(1_i64));
        let mixed_filter = region_filter
            .clone()
            .and(data_filter.clone())
            .and(day_filter.clone());

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[&mixed_filter],
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, 1);
    }

    #[test]
    fn partition_operator_planner_downgrades_boolean_data_shorthand_residuals_until_mixed_child() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let partition_filter = col("region").eq(lit("us-west"));
        let filters = [
            partition_filter.clone().and(col("is_current")),
            partition_filter.and(Expr::Not(Box::new(col("is_current")))),
        ];

        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_rejects_unsafe_mixed_extraction_shapes() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let partition_filter = col("region").eq(lit("us-west"));
        let scalar_udf = create_udf(
            "is_interesting",
            vec![DataType::Int64],
            DataType::Boolean,
            Volatility::Immutable,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))),
        );
        let scalar_function_filter =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(scalar_udf),
                vec![col("id")],
            ));
        let filters = [
            partition_filter.clone().or(col("id").gt(lit(1_i64))),
            Expr::Not(Box::new(
                partition_filter.clone().and(col("id").gt(lit(1_i64))),
            )),
            partition_filter.clone().and(col("ghost").gt(lit(1_i64))),
            partition_filter
                .clone()
                .and(col("profile.age").gt(lit(1_i64))),
            partition_filter
                .clone()
                .and(datafusion::logical_expr::cast(col("id"), DataType::Int64).gt(lit(1_i64))),
            partition_filter
                .clone()
                .and(col("id").gt(lit(1_i64)).alias("id_is_large")),
            partition_filter.clone().and(col("id")),
            partition_filter.clone().and(Expr::Not(Box::new(col("id")))),
            partition_filter.and(scalar_function_filter),
        ];

        let filter_refs = filters.iter().collect::<Vec<_>>();
        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(
            plan.decisions
                .iter()
                .all(|decision| decision.kernel_scan_filter.is_none())
        );
    }

    #[test]
    fn partition_operator_planner_rejects_unsupported_types_and_unsafe_shapes() {
        let schema = schema();
        let partition_columns = partition_columns(&["region", "id"]);
        let numeric_partition_literal = col("region").eq(lit(7_i64));
        let integer_partition_with_string_literal = col("id").eq(lit("7"));
        let null_between =
            col("region").between(Expr::Literal(ScalarValue::Utf8(None), None), lit("us-west"));
        let numeric_comparison = col("region").lt(lit(7_i64));
        let numeric_between = col("region").between(lit(7_i64), lit("us-west"));
        let integer_partition_with_string_between = col("id").between(lit("1"), lit("9"));
        let non_literal_between = col("region").between(col("day"), lit("us-west"));
        let not_filter = Expr::Not(Box::new(col("id").eq(lit("7"))));
        let null_literal = col("region").eq(Expr::Literal(ScalarValue::Utf8(None), None));

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[
                &numeric_partition_literal,
                &integer_partition_with_string_literal,
                &null_between,
                &numeric_comparison,
                &numeric_between,
                &integer_partition_with_string_between,
                &non_literal_between,
                &not_filter,
                &null_literal,
            ],
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, 9);
    }

    #[test]
    fn partition_operator_planner_handles_mixed_boolean_whole_filters() {
        let schema = schema();
        let partition_columns = partition_columns(&["region", "day"]);
        let partition_in = col("region").in_list(vec![lit("us-west"), lit("us-east")], false);
        let exact_partition_or = partition_in.clone().or(col("region").eq(lit("eu-central")));
        let filters = [
            partition_in.clone().and(col("id").gt(lit(1_i64))),
            partition_in.clone().or(col("id").eq(lit(1_i64))),
            col("region")
                .eq(lit("us-west"))
                .or(col("id").eq(lit(1_i64))),
            partition_in.clone().or(col("ghost").eq(lit("x"))),
            partition_in.or(col("profile.age").eq(lit(1_i64))),
            exact_partition_or.and(col("id").gt(lit(1_i64))),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_rejects_unproven_binary_literal_filters() {
        let schema = schema();
        let partition_columns = partition_columns(&["payload"]);
        let payload = Expr::Literal(ScalarValue::Binary(Some(b"hello".to_vec())), None);
        let filters = [
            col("payload").gt(payload.clone()),
            col("payload").between(payload.clone(), payload.clone()),
            col("payload").in_list(
                vec![
                    payload.clone(),
                    Expr::Literal(ScalarValue::Binary(Some(Vec::new())), None),
                ],
                false,
            ),
            col("payload").eq(Expr::Literal(ScalarValue::Binary(None), None)),
            col("payload").eq(Expr::Literal(ScalarValue::Binary(Some(Vec::new())), None)),
            col("payload").eq(lit("hello")),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.pushed_filter_count, 0);
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| decision.residual));
    }

    #[test]
    fn partition_operator_planner_rejects_unknown_and_qualified_columns() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let unknown = col("ghost").eq(lit("us-west"));
        let qualified = Expr::Column(datafusion::common::Column::new(Some("orders"), "region"))
            .eq(lit("us-west"));

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[&unknown, &qualified],
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
            ]
        );
        assert_eq!(plan.unsupported_count, 2);
        assert_eq!(plan.residual_filter_count, 2);
        assert!(plan.decisions[0].filter_analysis.kernel_predicate.is_none());
        assert!(plan.decisions[1].filter_analysis.kernel_predicate.is_none());
    }
}
