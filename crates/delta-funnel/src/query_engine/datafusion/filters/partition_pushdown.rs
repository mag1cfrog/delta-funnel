//! Static partition predicate pushdown policy.

use std::collections::HashSet;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{Expr, Operator};

use crate::table_formats::supports_partition_metadata_logical_type;

use super::analysis::{DeltaKernelPredicateScope, analyze_filter_for_pushdown};
use super::{DeltaFilterPushdownDecision, DeltaFilterPushdownOutcome, DeltaFilterPushdownPlan};

/// Plans the exact static partition operator policy.
///
/// A filter can be exact here only when it is partition-only and accepted by
/// the provider metadata semantics policy. The proven exact subset includes
/// supported logical partition column types for equality, inequality, range
/// comparisons, `BETWEEN`, `NOT BETWEEN`, `IN`, `NOT IN`, `IS NULL`, `IS NOT
/// NULL`, negation, and boolean composition of exact partition predicates.
/// Additional types and operators should be added only with semantic tests. All
/// other shapes stay `Unsupported` so DataFusion keeps them as residual filters.
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
/// The kernel predicate analysis is still preserved for unsupported decisions
/// so diagnostics remain useful, but unsupported predicates are not provider
/// owned and must not affect scan planning.
fn partition_operator_decision(
    input_index: usize,
    filter: &Expr,
    schema: &SchemaRef,
    partition_columns: &HashSet<String>,
) -> DeltaFilterPushdownDecision {
    let (kernel_predicate, rejection_reason) =
        analyze_filter_for_pushdown(filter, schema, partition_columns);
    let is_partition_only = kernel_predicate.scope == DeltaKernelPredicateScope::PartitionOnly;

    if is_partition_only && is_supported_partition_operator_filter(filter, schema) {
        return DeltaFilterPushdownDecision {
            input_index,
            outcome: DeltaFilterPushdownOutcome::Exact,
            residual: false,
            rejection_reason: None,
            kernel_predicate,
        };
    }

    DeltaFilterPushdownDecision {
        input_index,
        outcome: DeltaFilterPushdownOutcome::Unsupported,
        residual: true,
        rejection_reason: Some(rejection_reason),
        kernel_predicate,
    }
}

/// Checks whether the expression shape is supported by the provider exact
/// partition operator policy.
///
/// Column membership is intentionally checked by `analyze_filter_for_pushdown`;
/// this helper verifies accepted leaf predicates or boolean composition whose
/// leaves are accepted predicates. Exact partition predicates are provider-owned
/// and applied to Delta scan-file partition metadata, not to delta_kernel's
/// predicate path.
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
        Expr::InList(_) => is_supported_partition_in_list(filter, schema),
        Expr::Between(_) => is_supported_partition_between(filter, schema),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            is_supported_partition_null_check(inner.as_ref(), schema)
        }
        Expr::Not(inner) => is_supported_partition_operator_filter(inner.as_ref(), schema),
        _ => false,
    }
}

/// Accepts one column/literal range comparison if the metadata type policy can prove it.
fn is_supported_partition_comparison(column: &Expr, literal: &Expr, schema: &SchemaRef) -> bool {
    let Expr::Column(column) = column else {
        return false;
    };

    is_supported_partition_literal_for_column(column, literal, schema)
}

/// Accepts inclusive partition ranges when both literal bounds are proven.
fn is_supported_partition_between(filter: &Expr, schema: &SchemaRef) -> bool {
    let Expr::Between(between) = filter else {
        return false;
    };
    let Expr::Column(column) = between.expr.as_ref() else {
        return false;
    };

    is_supported_partition_literal_for_column(column, between.low.as_ref(), schema)
        && is_supported_partition_literal_for_column(column, between.high.as_ref(), schema)
}

/// Accepts one column/literal equality if the metadata type policy can prove it.
fn is_supported_partition_equality(column: &Expr, literal: &Expr, schema: &SchemaRef) -> bool {
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

/// Accepts an `IN` or `NOT IN` list for proven partition literals.
///
/// `IN` is exact for the same non-empty, non-null literal subset as equality:
/// it is equivalent to a disjunction of equality checks. `NOT IN` is represented
/// by the provider metadata evaluator as `NOT(IN(...))`, preserving SQL null
/// propagation. Empty, null-containing, empty-string, mixed-type, or non-literal
/// lists stay unsupported unless their metadata semantics are proven.
fn is_supported_partition_in_list(filter: &Expr, schema: &SchemaRef) -> bool {
    let Expr::InList(in_list) = filter else {
        return false;
    };
    let Expr::Column(column) = in_list.expr.as_ref() else {
        return false;
    };

    !in_list.list.is_empty()
        && is_supported_partition_column_type(column, schema)
        && in_list
            .list
            .iter()
            .all(|literal| is_supported_partition_literal_for_column(column, literal, schema))
}

/// Accepts null checks only for logical partition columns whose metadata
/// representation is supported by the provider evaluator.
///
/// delta_kernel 0.23.0 treats raw empty partition values as null for its own
/// predicate path, while SQL semantics distinguish missing/null from a present
/// raw empty string. The provider-owned metadata evaluator is the authority for
/// these predicates.
fn is_supported_partition_null_check(expr: &Expr, schema: &SchemaRef) -> bool {
    let Expr::Column(column) = expr else {
        return false;
    };

    is_supported_partition_column_type(column, schema)
}

/// Restricts exactness to supported logical partition column types.
///
/// Delta serializes all partition values as text in the log, but this check is
/// about the logical table schema type. The supported type set is centralized
/// in the Delta partition metadata evaluator so this planner and the evaluator
/// expand together.
fn is_supported_partition_column_type(column: &Column, schema: &SchemaRef) -> bool {
    if column.relation.is_some() || column.name.contains('.') {
        return false;
    }

    schema
        .field_with_name(&column.name)
        .is_ok_and(|field| supports_partition_metadata_logical_type(field.data_type()))
}

/// Restricts operators to type/literal pairs whose exactness is proven.
///
/// This must evolve together with `is_supported_partition_column_type`; exact
/// pushdown should only be claimed for type pairs whose Delta partition
/// metadata semantics are tested.
fn is_supported_partition_literal_for_column(
    column: &Column,
    literal: &Expr,
    schema: &SchemaRef,
) -> bool {
    if !is_supported_partition_column_type(column, schema) {
        return false;
    }

    let Ok(field) = schema.field_with_name(&column.name) else {
        return false;
    };

    match (field.data_type(), literal) {
        (
            DataType::Utf8 | DataType::LargeUtf8,
            Expr::Literal(ScalarValue::Utf8(Some(value)), _)
            | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _),
        ) => !value.is_empty(),
        (DataType::Boolean, Expr::Literal(ScalarValue::Boolean(Some(_)), _)) => true,
        (data_type, literal) => signed_integer_bounds(data_type)
            .zip(signed_integer_literal_value(literal))
            .is_some_and(|((min, max), value)| min <= value && value <= max),
    }
}

fn signed_integer_bounds(data_type: &DataType) -> Option<(i64, i64)> {
    match data_type {
        DataType::Int8 => Some((i64::from(i8::MIN), i64::from(i8::MAX))),
        DataType::Int16 => Some((i64::from(i16::MIN), i64::from(i16::MAX))),
        DataType::Int32 => Some((i64::from(i32::MIN), i64::from(i32::MAX))),
        DataType::Int64 => Some((i64::MIN, i64::MAX)),
        _ => None,
    }
}

fn signed_integer_literal_value(literal: &Expr) -> Option<i64> {
    match literal {
        Expr::Literal(ScalarValue::Int8(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Some(*value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, col, lit};

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("day", DataType::Utf8, true),
            Field::new("is_current", DataType::Boolean, true),
        ]))
    }

    fn partition_columns(names: &[&str]) -> HashSet<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
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
        assert_eq!(
            plan.decisions[0].kernel_predicate.scope,
            DeltaKernelPredicateScope::PartitionOnly
        );
        assert!(plan.decisions[0].kernel_predicate.predicate.is_some());
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
            plan.decisions[0].kernel_predicate.partition_columns,
            vec!["day", "region"]
        );
    }

    #[test]
    fn supported_partition_in_filter_is_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let filter = col("region").in_list(vec![lit("us-west"), lit("us-east")], false);

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
            plan.decisions[0].kernel_predicate.scope,
            DeltaKernelPredicateScope::PartitionOnly
        );
        assert_eq!(
            plan.decisions[0].kernel_predicate.partition_columns,
            vec!["region"]
        );
        assert!(plan.decisions[0].kernel_predicate.predicate.is_some());
    }

    #[test]
    fn supported_partition_or_filter_is_exact_when_every_branch_is_exact() {
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
        assert_eq!(
            plan.decisions[0].kernel_predicate.scope,
            DeltaKernelPredicateScope::PartitionOnly
        );
        assert!(plan.decisions[0].kernel_predicate.predicate.is_some());
    }

    #[test]
    fn partition_operator_planner_accepts_null_checks_as_metadata_only_exact() {
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
                && decision.kernel_predicate.scope == DeltaKernelPredicateScope::PartitionOnly
                && decision.kernel_predicate.predicate.is_some()
                && decision.kernel_predicate.adapter_error.is_none()
        }));
    }

    #[test]
    fn partition_operator_planner_accepts_boolean_null_checks_as_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["is_current"]);
        let filters = [col("is_current").is_null(), col("is_current").is_not_null()];
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
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.residual_filter_count, 0);
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Exact
                && !decision.residual
                && decision.kernel_predicate.scope == DeltaKernelPredicateScope::PartitionOnly
        }));
    }

    #[test]
    fn partition_operator_planner_accepts_boolean_equality_and_membership() {
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

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Exact; filters.len()]
        );
        assert_eq!(plan.exact_count, filters.len());
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.residual_filter_count, 0);
        assert!(plan.decisions.iter().all(|decision| {
            decision.kernel_predicate.scope == DeltaKernelPredicateScope::PartitionOnly
        }));
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
    fn partition_operator_planner_keeps_null_composition_metadata_only() {
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
        assert_eq!(
            plan.decisions[0].kernel_predicate.scope,
            DeltaKernelPredicateScope::PartitionOnly
        );
    }

    #[test]
    fn partition_operator_planner_accepts_negated_string_partition_predicates() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let filters = [
            col("region").not_eq(lit("us-west")),
            Expr::Not(Box::new(col("region").eq(lit("us-west")))),
            col("region").in_list(vec![lit("us-west"), lit("us-east")], true),
            Expr::Not(Box::new(
                col("region").in_list(vec![lit("us-west"), lit("us-east")], false),
            )),
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
            decision.kernel_predicate.scope == DeltaKernelPredicateScope::PartitionOnly
                && decision.kernel_predicate.adapter_error.is_none()
        }));
    }

    #[test]
    fn partition_operator_planner_accepts_string_partition_comparisons() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let filters = [
            col("region").lt(lit("us-west")),
            col("region").lt_eq(lit("us-west")),
            col("region").gt(lit("us-east")),
            col("region").gt_eq(lit("us-east")),
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
        assert!(plan.decisions.iter().all(|decision| {
            decision.kernel_predicate.scope == DeltaKernelPredicateScope::PartitionOnly
                && decision.kernel_predicate.adapter_error.is_none()
        }));
    }

    #[test]
    fn partition_operator_planner_accepts_integer_partition_comparisons() {
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

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Exact; filters.len()]
        );
        assert_eq!(plan.exact_count, filters.len());
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.residual_filter_count, 0);
        assert!(plan.decisions.iter().all(|decision| {
            decision.kernel_predicate.scope == DeltaKernelPredicateScope::PartitionOnly
                && decision.kernel_predicate.adapter_error.is_none()
        }));
    }

    #[test]
    fn partition_operator_planner_accepts_string_and_integer_partition_between() {
        let schema = schema();
        let partition_columns = partition_columns(&["region", "id"]);
        let filters = [
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
            vec![TableProviderFilterPushDown::Exact; filters.len()]
        );
        assert_eq!(plan.exact_count, filters.len());
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.residual_filter_count, 0);
        assert!(plan.decisions.iter().all(|decision| {
            decision.kernel_predicate.scope == DeltaKernelPredicateScope::PartitionOnly
                && decision.kernel_predicate.adapter_error.is_none()
        }));
    }

    #[test]
    fn partition_operator_planner_rejects_unproven_in_list_shapes() {
        let schema = schema();
        let partition_columns = partition_columns(&["region", "id"]);
        let empty_in = col("region").in_list(Vec::<Expr>::new(), false);
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
            vec![TableProviderFilterPushDown::Unsupported; 9]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, 9);
        assert_eq!(plan.residual_filter_count, 9);
        assert!(plan.decisions.iter().all(|decision| decision.residual));
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
    fn partition_operator_planner_rejects_unsupported_types_and_unsafe_shapes() {
        let schema = schema();
        let partition_columns = partition_columns(&["region", "id"]);
        let numeric_partition_literal = col("region").eq(lit(7_i64));
        let integer_partition_with_string_literal = col("id").eq(lit("7"));
        let empty_string_comparison = col("region").lt(lit(""));
        let empty_string_between = col("region").between(lit(""), lit("us-west"));
        let null_between =
            col("region").between(Expr::Literal(ScalarValue::Utf8(None), None), lit("us-west"));
        let numeric_comparison = col("region").lt(lit(7_i64));
        let numeric_between = col("region").between(lit(7_i64), lit("us-west"));
        let integer_partition_with_string_between = col("id").between(lit("1"), lit("9"));
        let non_literal_between = col("region").between(col("day"), lit("us-west"));
        let mixed_and = col("region")
            .eq(lit("us-west"))
            .and(col("day").gt(lit("2026-01-01")));
        let mixed_or = col("region")
            .eq(lit("us-west"))
            .or(col("day").gt(lit("2026-01-01")));
        let not_filter = Expr::Not(Box::new(col("id").eq(lit("7"))));
        let null_literal = col("region").eq(Expr::Literal(ScalarValue::Utf8(None), None));

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[
                &numeric_partition_literal,
                &integer_partition_with_string_literal,
                &empty_string_comparison,
                &empty_string_between,
                &null_between,
                &numeric_comparison,
                &numeric_between,
                &integer_partition_with_string_between,
                &non_literal_between,
                &mixed_and,
                &mixed_or,
                &not_filter,
                &null_literal,
            ],
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
            ]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, 13);
        assert_eq!(plan.residual_filter_count, 13);
    }

    #[test]
    fn partition_operator_planner_rejects_mixed_boolean_whole_filters() {
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
        assert!(plan.decisions[0].kernel_predicate.predicate.is_none());
        assert!(plan.decisions[1].kernel_predicate.predicate.is_none());
    }
}
