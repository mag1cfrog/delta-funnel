//! Static partition predicate pushdown policy.

use std::collections::HashSet;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{Expr, Operator};

use super::analysis::{DeltaKernelPredicateScope, analyze_filter_for_pushdown};
use super::{DeltaFilterPushdownDecision, DeltaFilterPushdownOutcome, DeltaFilterPushdownPlan};

/// Plans the exact partition-equality policy for issue 33.
///
/// A filter can be exact here only when it is partition-only, kernel
/// convertible, and accepted by the current partition-equality type policy.
/// The type policy starts with non-null logical string partition equality and
/// can expand later without changing this planner boundary. All other shapes
/// stay `Unsupported` so DataFusion keeps them as residual filters.
pub(super) fn plan_partition_equality_pushdown(
    filters: &[&Expr],
    schema: &SchemaRef,
    partition_columns: &HashSet<String>,
) -> DeltaFilterPushdownPlan {
    let decisions = filters
        .iter()
        .enumerate()
        .map(|(input_index, filter)| {
            partition_equality_decision(input_index, filter, schema, partition_columns)
        })
        .collect::<Vec<_>>();

    DeltaFilterPushdownPlan::from_decisions(decisions)
}

/// Converts one candidate filter into either an exact partition-equality
/// decision or a conservative unsupported decision.
///
/// The kernel predicate analysis is still preserved for unsupported decisions
/// so diagnostics remain useful, but unsupported predicates are not provider
/// owned and must not affect scan planning.
fn partition_equality_decision(
    input_index: usize,
    filter: &Expr,
    schema: &SchemaRef,
    partition_columns: &HashSet<String>,
) -> DeltaFilterPushdownDecision {
    let (kernel_predicate, rejection_reason) =
        analyze_filter_for_pushdown(filter, schema, partition_columns);

    if kernel_predicate.scope == DeltaKernelPredicateScope::PartitionOnly
        && kernel_predicate.predicate.is_some()
        && kernel_predicate.adapter_error.is_none()
        && is_supported_partition_equality_filter(filter, schema)
    {
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

/// Checks whether the expression shape is supported by the current exact
/// partition-equality type policy.
///
/// Column membership is intentionally checked by `analyze_filter_for_pushdown`;
/// this helper verifies a single accepted equality or an `AND` tree whose
/// leaves are accepted equalities.
fn is_supported_partition_equality_filter(filter: &Expr, schema: &SchemaRef) -> bool {
    match filter {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            is_supported_partition_equality_filter(binary.left.as_ref(), schema)
                && is_supported_partition_equality_filter(binary.right.as_ref(), schema)
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            is_supported_partition_equality(binary.left.as_ref(), binary.right.as_ref(), schema)
                || is_supported_partition_equality(
                    binary.right.as_ref(),
                    binary.left.as_ref(),
                    schema,
                )
        }
        _ => false,
    }
}

/// Accepts one column/literal equality if the current type policy can prove it.
fn is_supported_partition_equality(column: &Expr, literal: &Expr, schema: &SchemaRef) -> bool {
    let Expr::Column(column) = column else {
        return false;
    };

    is_supported_partition_column_type(column, schema) && is_supported_partition_literal(literal)
}

/// Restricts issue 33 exactness to string-typed logical partition columns.
///
/// Delta serializes all partition values as text in the log, but this check is
/// about the logical table schema type. Other primitive partition types can be
/// added here after their typed metadata semantics are tested.
fn is_supported_partition_column_type(column: &Column, schema: &SchemaRef) -> bool {
    if column.relation.is_some() || column.name.contains('.') {
        return false;
    }

    schema
        .field_with_name(&column.name)
        .is_ok_and(|field| matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8))
}

/// Restricts issue 33 exactness to non-null string literals.
///
/// This must evolve together with `is_supported_partition_column_type`; exact
/// pushdown should only be claimed for type pairs whose Delta partition
/// metadata semantics are tested.
fn is_supported_partition_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(
            ScalarValue::Utf8(Some(_)) | ScalarValue::LargeUtf8(Some(_)),
            _
        )
    )
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

        let plan = DeltaFilterPushdownPlan::partition_equality_pushdown(
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

        let plan = DeltaFilterPushdownPlan::partition_equality_pushdown(
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
    fn partition_equality_planner_preserves_multiple_input_statuses() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let exact = col("region").eq(lit("us-west"));
        let unsupported_data = col("id").eq(lit(7_i64));
        let duplicate_exact = col("region").eq(lit("us-west"));

        let plan = DeltaFilterPushdownPlan::partition_equality_pushdown(
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
    fn partition_equality_planner_rejects_unsupported_types_and_unsafe_shapes() {
        let schema = schema();
        let partition_columns = partition_columns(&["region", "id"]);
        let numeric_partition_literal = col("region").eq(lit(7_i64));
        let integer_partition_with_string_literal = col("id").eq(lit("7"));
        let mixed_and = col("region").eq(lit("us-west")).and(col("id").gt(lit(10)));
        let mixed_or = col("region").eq(lit("us-west")).or(col("id").gt(lit(10)));
        let not_filter = Expr::Not(Box::new(col("region").eq(lit("us-west"))));
        let null_literal = col("region").eq(Expr::Literal(ScalarValue::Utf8(None), None));

        let plan = DeltaFilterPushdownPlan::partition_equality_pushdown(
            &[
                &numeric_partition_literal,
                &integer_partition_with_string_literal,
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
            ]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, 6);
        assert_eq!(plan.residual_filter_count, 6);
    }

    #[test]
    fn partition_equality_planner_rejects_unknown_and_qualified_columns() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let unknown = col("ghost").eq(lit("us-west"));
        let qualified = Expr::Column(datafusion::common::Column::new(Some("orders"), "region"))
            .eq(lit("us-west"));

        let plan = DeltaFilterPushdownPlan::partition_equality_pushdown(
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
