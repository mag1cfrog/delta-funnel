//! Static partition filter pushdown policy.

use std::collections::HashSet;

use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{Expr, Operator, lit};

use crate::table_formats::{DeltaKernelPredicate, datafusion_expr_to_kernel_predicate};

use super::analysis::{DeltaFilterColumnScope, analyze_filter_for_pushdown};
use super::{
    DeltaFilterPushdownDecision, DeltaFilterPushdownOutcome, DeltaFilterPushdownPlan,
    ExactPartitionKernelFilter,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KernelPartitionTypeGroup {
    String,
    Integer,
    Boolean,
    Date,
    Decimal,
    Decimal256,
    Floating,
    Binary,
    Timestamp,
    TimestampNtz,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KernelPartitionOperatorFamily {
    Equality,
    Ordering,
    Membership,
    Between,
    NullCheck,
    BooleanShorthand,
}

/// Plans the static partition operator policy for kernel-native pruning.
///
/// A filter can be exact only when it is partition-only and accepted by the
/// kernel predicate path. A mixed top-level `AND` can be inexact when at least
/// one top-level term is accepted by the exact partition policy and every
/// remaining term is data-only residual work for DataFusion.
pub(super) fn plan_partition_operator_pushdown(
    filters: &[&Expr],
    schema: &SchemaRef,
    partition_columns: &HashSet<String>,
) -> DeltaFilterPushdownPlan {
    let decisions = filters
        .iter()
        .map(|filter| partition_operator_decision(filter, schema, partition_columns))
        .collect::<Vec<_>>();

    DeltaFilterPushdownPlan::from_decisions(decisions)
}

/// Converts one candidate filter into either an exact partition decision or a
/// conservative unsupported decision.
///
/// Filter analysis is still preserved for unsupported decisions so diagnostics
/// remain useful, but unsupported predicates are not accepted and must not
/// affect kernel scan planning.
fn partition_operator_decision(
    filter: &Expr,
    schema: &SchemaRef,
    partition_columns: &HashSet<String>,
) -> DeltaFilterPushdownDecision {
    let (filter_analysis, rejection_reason) =
        analyze_filter_for_pushdown(filter, schema, partition_columns);
    let is_partition_only = filter_analysis.scope == DeltaFilterColumnScope::PartitionOnly;

    if is_partition_only
        && let Some(kernel_scan_filter) = try_exact_partition_kernel_filter(filter, schema)
    {
        return DeltaFilterPushdownDecision {
            outcome: DeltaFilterPushdownOutcome::Exact,
            residual: false,
            rejection_reason: None,
            filter_analysis,
            kernel_scan_filter: Some(kernel_scan_filter),
        };
    }

    if filter_analysis.scope == DeltaFilterColumnScope::PartitionAndData
        && let Some(kernel_scan_filter) =
            try_mixed_and_partition_kernel_filter(filter, schema, partition_columns)
    {
        return DeltaFilterPushdownDecision {
            outcome: DeltaFilterPushdownOutcome::Inexact,
            residual: true,
            rejection_reason: None,
            filter_analysis,
            kernel_scan_filter: Some(kernel_scan_filter),
        };
    }

    DeltaFilterPushdownDecision {
        outcome: DeltaFilterPushdownOutcome::Unsupported,
        residual: true,
        rejection_reason: Some(rejection_reason),
        filter_analysis,
        kernel_scan_filter: None,
    }
}

/// Builds the exact filter payload for kernel scan planning. Most accepted
/// filters are passed through unchanged, but empty `IN` and `NOT IN` lists
/// need explicit rewrites so `NOT IN ()` does not become a literal true
/// predicate that includes null partitions.
fn try_exact_partition_kernel_filter(
    filter: &Expr,
    schema: &SchemaRef,
) -> Option<ExactPartitionKernelFilter> {
    let datafusion_expr = exact_partition_kernel_expr(filter, schema)?;
    let kernel_predicate = datafusion_expr_to_kernel_predicate(&datafusion_expr).ok()?;

    Some(ExactPartitionKernelFilter {
        datafusion_expr,
        kernel_predicate,
    })
}

fn try_mixed_and_partition_kernel_filter(
    filter: &Expr,
    schema: &SchemaRef,
    partition_columns: &HashSet<String>,
) -> Option<ExactPartitionKernelFilter> {
    if !matches!(filter, Expr::BinaryExpr(binary) if binary.op == Operator::And) {
        return None;
    }

    let mut terms = Vec::new();
    collect_top_level_and_terms(filter, &mut terms);

    let mut extracted_filters = Vec::new();
    let mut residual_term_count = 0_usize;

    for term in terms {
        let (term_analysis, _rejection_reason) =
            analyze_filter_for_pushdown(term, schema, partition_columns);

        match term_analysis.scope {
            DeltaFilterColumnScope::PartitionOnly => {
                extracted_filters.push(try_exact_partition_kernel_filter(term, schema)?);
            }
            DeltaFilterColumnScope::DataOnly => {
                if !is_safe_data_residual_term(term, schema) {
                    return None;
                }

                residual_term_count = residual_term_count.saturating_add(1);
            }
            DeltaFilterColumnScope::PartitionAndData | DeltaFilterColumnScope::Unsupported => {
                return None;
            }
        }
    }

    if extracted_filters.is_empty() || residual_term_count == 0 {
        return None;
    }

    let datafusion_expr = extracted_filters
        .iter()
        .map(|filter| filter.datafusion_expr.clone())
        .reduce(Expr::and)?;
    let kernel_predicate = DeltaKernelPredicate::and_from(
        extracted_filters
            .into_iter()
            .map(|filter| filter.kernel_predicate),
    )?;

    Some(ExactPartitionKernelFilter {
        datafusion_expr,
        kernel_predicate,
    })
}

fn collect_top_level_and_terms<'a>(filter: &'a Expr, terms: &mut Vec<&'a Expr>) {
    match filter {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_top_level_and_terms(binary.left.as_ref(), terms);
            collect_top_level_and_terms(binary.right.as_ref(), terms);
        }
        _ => terms.push(filter),
    }
}

fn is_safe_data_residual_term(term: &Expr, schema: &SchemaRef) -> bool {
    match term {
        Expr::Column(column) => is_boolean_column(column, schema),
        Expr::Not(inner) if matches!(inner.as_ref(), Expr::Column(_)) => {
            let Expr::Column(column) = inner.as_ref() else {
                return false;
            };

            is_boolean_column(column, schema)
        }
        _ => true,
    }
}

fn is_boolean_column(column: &Column, schema: &SchemaRef) -> bool {
    if column.relation.is_some() || column.name.contains('.') {
        return false;
    }

    schema
        .field_with_name(&column.name)
        .is_ok_and(|field| matches!(field.data_type(), DataType::Boolean))
}

fn exact_partition_kernel_expr(filter: &Expr, schema: &SchemaRef) -> Option<Expr> {
    match filter {
        Expr::Column(column) => {
            if is_kernel_exact_partition_column_for_operator(
                column,
                schema,
                KernelPartitionOperatorFamily::BooleanShorthand,
            ) {
                Some(filter.clone().eq(lit(true)))
            } else {
                None
            }
        }
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
            let left = exact_partition_kernel_expr(binary.left.as_ref(), schema)?;
            let right = exact_partition_kernel_expr(binary.right.as_ref(), schema)?;

            Some(match binary.op {
                Operator::And => left.and(right),
                Operator::Or => left.or(right),
                _ => return None,
            })
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            if is_supported_partition_equality(binary.left.as_ref(), binary.right.as_ref(), schema)
                || is_supported_partition_equality(
                    binary.right.as_ref(),
                    binary.left.as_ref(),
                    schema,
                )
            {
                Some(filter.clone())
            } else {
                None
            }
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::NotEq => {
            if is_supported_partition_equality(binary.left.as_ref(), binary.right.as_ref(), schema)
                || is_supported_partition_equality(
                    binary.right.as_ref(),
                    binary.left.as_ref(),
                    schema,
                )
            {
                Some(filter.clone())
            } else {
                None
            }
        }
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
            ) =>
        {
            if is_supported_partition_comparison(
                binary.left.as_ref(),
                binary.right.as_ref(),
                schema,
            ) || is_supported_partition_comparison(
                binary.right.as_ref(),
                binary.left.as_ref(),
                schema,
            ) {
                Some(filter.clone())
            } else {
                None
            }
        }
        Expr::InList(in_list) if in_list.list.is_empty() => {
            let Expr::Column(column) = in_list.expr.as_ref() else {
                return None;
            };

            if !is_supported_string_empty_in_rewrite_column(column, schema) {
                return None;
            }

            if in_list.negated {
                Some(in_list.expr.as_ref().clone().is_not_null())
            } else {
                Some(lit(false))
            }
        }
        Expr::InList(in_list) => {
            if is_supported_partition_in_list(in_list, schema) {
                Some(filter.clone())
            } else {
                None
            }
        }
        Expr::Between(between) => {
            if is_supported_partition_between(between, schema) {
                Some(filter.clone())
            } else {
                None
            }
        }
        Expr::Not(inner) => {
            let inner = exact_partition_kernel_expr(inner.as_ref(), schema)?;
            Some(Expr::Not(Box::new(inner)))
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            if is_supported_partition_null_check(inner.as_ref(), schema) {
                Some(filter.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Accepts one column/literal equality if the kernel policy can prove it.
fn is_supported_partition_equality(column: &Expr, literal: &Expr, schema: &SchemaRef) -> bool {
    is_supported_partition_column_literal_pair(
        column,
        literal,
        schema,
        KernelPartitionOperatorFamily::Equality,
    )
}

fn is_supported_partition_comparison(column: &Expr, literal: &Expr, schema: &SchemaRef) -> bool {
    is_supported_partition_column_literal_pair(
        column,
        literal,
        schema,
        KernelPartitionOperatorFamily::Ordering,
    )
}

/// Accepts one partition column paired with a literal under the kernel policy.
fn is_supported_partition_column_literal_pair(
    column: &Expr,
    literal: &Expr,
    schema: &SchemaRef,
    operator_family: KernelPartitionOperatorFamily,
) -> bool {
    let Expr::Column(column) = column else {
        return false;
    };

    is_supported_partition_literal_for_column(column, literal, schema, operator_family)
}

/// Accepts null checks only for kernel-exact partition columns.
fn is_supported_partition_null_check(expr: &Expr, schema: &SchemaRef) -> bool {
    let Expr::Column(column) = expr else {
        return false;
    };

    is_kernel_exact_partition_column_for_operator(
        column,
        schema,
        KernelPartitionOperatorFamily::NullCheck,
    )
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

    if !is_kernel_exact_partition_column_for_operator(
        column,
        schema,
        KernelPartitionOperatorFamily::Membership,
    ) {
        return false;
    }

    in_list.list.iter().all(|literal| {
        is_supported_partition_literal_for_column(
            column,
            literal,
            schema,
            KernelPartitionOperatorFamily::Membership,
        )
    })
}

fn is_supported_partition_between(
    between: &datafusion::logical_expr::expr::Between,
    schema: &SchemaRef,
) -> bool {
    let Expr::Column(column) = between.expr.as_ref() else {
        return false;
    };

    is_supported_partition_literal_for_column(
        column,
        between.low.as_ref(),
        schema,
        KernelPartitionOperatorFamily::Between,
    ) && is_supported_partition_literal_for_column(
        column,
        between.high.as_ref(),
        schema,
        KernelPartitionOperatorFamily::Between,
    )
}

fn is_kernel_exact_partition_column_for_operator(
    column: &Column,
    schema: &SchemaRef,
    operator_family: KernelPartitionOperatorFamily,
) -> bool {
    if column.relation.is_some() || column.name.contains('.') {
        return false;
    }

    schema.field_with_name(&column.name).is_ok_and(|field| {
        is_supported_kernel_partition_operator(field.data_type(), operator_family)
    })
}

fn is_supported_string_empty_in_rewrite_column(column: &Column, schema: &SchemaRef) -> bool {
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
    operator_family: KernelPartitionOperatorFamily,
) -> bool {
    if !is_kernel_exact_partition_column_for_operator(column, schema, operator_family) {
        return false;
    }

    let Ok(field) = schema.field_with_name(&column.name) else {
        return false;
    };

    match (field.data_type(), literal) {
        (
            DataType::Utf8 | DataType::LargeUtf8,
            Expr::Literal(ScalarValue::Utf8(Some(_)) | ScalarValue::LargeUtf8(Some(_)), _),
        )
        | (DataType::Int8, Expr::Literal(ScalarValue::Int8(Some(_)), _))
        | (DataType::Int16, Expr::Literal(ScalarValue::Int16(Some(_)), _))
        | (DataType::Int32, Expr::Literal(ScalarValue::Int32(Some(_)), _))
        | (DataType::Int64, Expr::Literal(ScalarValue::Int64(Some(_)), _))
        | (DataType::Boolean, Expr::Literal(ScalarValue::Boolean(Some(_)), _))
        | (DataType::Date32, Expr::Literal(ScalarValue::Date32(Some(_)), _)) => true,
        (DataType::Float32, Expr::Literal(ScalarValue::Float32(Some(value)), _)) => {
            value.is_finite() && *value != 0.0
        }
        (DataType::Float64, Expr::Literal(ScalarValue::Float64(Some(value)), _)) => {
            value.is_finite() && *value != 0.0
        }
        (
            DataType::Decimal128(precision, scale),
            Expr::Literal(ScalarValue::Decimal128(Some(_), literal_precision, literal_scale), _),
        ) => precision == literal_precision && scale == literal_scale,
        (DataType::Binary, Expr::Literal(ScalarValue::Binary(Some(value)), _))
        | (DataType::LargeBinary, Expr::Literal(ScalarValue::LargeBinary(Some(value)), _)) => {
            !value.is_empty()
        }
        (
            DataType::FixedSizeBinary(size),
            Expr::Literal(ScalarValue::FixedSizeBinary(literal_size, Some(value)), _),
        ) => {
            size == literal_size
                && usize::try_from(*size).is_ok_and(|size| value.len() == size)
                && !value.is_empty()
        }
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
            Expr::Literal(ScalarValue::TimestampMicrosecond(Some(_), Some(timezone)), _),
        ) => !timezone.is_empty(),
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Expr::Literal(ScalarValue::TimestampMicrosecond(Some(_), None), _),
        ) => true,
        _ => false,
    }
}

fn is_supported_kernel_partition_operator(
    data_type: &DataType,
    operator_family: KernelPartitionOperatorFamily,
) -> bool {
    use KernelPartitionOperatorFamily::{
        Between, BooleanShorthand, Equality, Membership, NullCheck, Ordering,
    };
    use KernelPartitionTypeGroup::{
        Binary, Boolean, Date, Decimal, Decimal256, Floating, Integer, String, Timestamp,
        TimestampNtz,
    };

    match kernel_partition_type_group(data_type) {
        String => match operator_family {
            Equality | Ordering | Membership | Between | NullCheck => true,
            BooleanShorthand => false,
        },
        Integer => match operator_family {
            Equality | Ordering | Membership | Between | NullCheck => true,
            BooleanShorthand => false,
        },
        Date => match operator_family {
            Equality | Ordering | Membership | Between | NullCheck => true,
            BooleanShorthand => false,
        },
        Decimal => match operator_family {
            Equality | Ordering | Membership | Between | NullCheck => true,
            BooleanShorthand => false,
        },
        Decimal256 => false,
        Timestamp | TimestampNtz => match operator_family {
            Equality | Ordering | Membership | Between | NullCheck => true,
            BooleanShorthand => false,
        },
        Floating => match operator_family {
            Equality | Membership | NullCheck => true,
            Ordering | Between | BooleanShorthand => false,
        },
        Boolean => match operator_family {
            Equality | Membership | NullCheck | BooleanShorthand => true,
            Ordering | Between => false,
        },
        Binary => match operator_family {
            Equality | Membership | NullCheck => true,
            Ordering | Between | BooleanShorthand => false,
        },
        KernelPartitionTypeGroup::Unsupported => false,
    }
}

fn kernel_partition_type_group(data_type: &DataType) -> KernelPartitionTypeGroup {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => KernelPartitionTypeGroup::String,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            KernelPartitionTypeGroup::Integer
        }
        DataType::Boolean => KernelPartitionTypeGroup::Boolean,
        DataType::Date32 => KernelPartitionTypeGroup::Date,
        DataType::Decimal128(_, _) => KernelPartitionTypeGroup::Decimal,
        DataType::Decimal256(_, _) => KernelPartitionTypeGroup::Decimal256,
        DataType::Float32 | DataType::Float64 => KernelPartitionTypeGroup::Floating,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            KernelPartitionTypeGroup::Binary
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)) if !timezone.is_empty() => {
            KernelPartitionTypeGroup::Timestamp
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => KernelPartitionTypeGroup::TimestampNtz,
        _ => KernelPartitionTypeGroup::Unsupported,
    }
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
            Field::new("byte_part", DataType::Int8, true),
            Field::new("short_part", DataType::Int16, true),
            Field::new("int_part", DataType::Int32, true),
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

    fn kernel_scan_expr(decision: &DeltaFilterPushdownDecision) -> Option<&Expr> {
        decision
            .kernel_scan_filter
            .as_ref()
            .map(|filter| &filter.datafusion_expr)
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

    fn assert_all_exact(plan: &DeltaFilterPushdownPlan, len: usize) {
        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Exact; len]
        );
        assert_eq!(plan.exact_count, len);
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, len);
        assert_eq!(plan.residual_filter_count, 0);
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Exact
                && !decision.residual
                && decision.rejection_reason.is_none()
                && decision.filter_analysis.scope == DeltaFilterColumnScope::PartitionOnly
                && decision.kernel_scan_filter.is_some()
        }));
    }

    fn assert_type_operator_admission(
        data_type: DataType,
        expected_group: KernelPartitionTypeGroup,
        expected_admission: &[(KernelPartitionOperatorFamily, bool)],
    ) {
        assert_eq!(kernel_partition_type_group(&data_type), expected_group);

        for (operator_family, supported) in expected_admission {
            assert_eq!(
                is_supported_kernel_partition_operator(&data_type, *operator_family),
                *supported,
                "{data_type:?} {operator_family:?}"
            );
        }
    }

    #[test]
    fn kernel_partition_admission_documents_supported_type_operator_families() {
        use KernelPartitionOperatorFamily::{
            Between, BooleanShorthand, Equality, Membership, NullCheck, Ordering,
        };
        use KernelPartitionTypeGroup::{
            Binary, Boolean, Date, Decimal, Decimal256, Floating, Integer, String, Timestamp,
            TimestampNtz,
        };

        let string_admission = [
            (Equality, true),
            (Ordering, true),
            (Membership, true),
            (Between, true),
            (NullCheck, true),
            (BooleanShorthand, false),
        ];
        let integer_admission = [
            (Equality, true),
            (Ordering, true),
            (Membership, true),
            (Between, true),
            (NullCheck, true),
            (BooleanShorthand, false),
        ];
        let boolean_admission = [
            (Equality, true),
            (Ordering, false),
            (Membership, true),
            (Between, false),
            (NullCheck, true),
            (BooleanShorthand, true),
        ];
        let floating_admission = [
            (Equality, true),
            (Ordering, false),
            (Membership, true),
            (Between, false),
            (NullCheck, true),
            (BooleanShorthand, false),
        ];
        let date_admission = [
            (Equality, true),
            (Ordering, true),
            (Membership, true),
            (Between, true),
            (NullCheck, true),
            (BooleanShorthand, false),
        ];
        let decimal_admission = [
            (Equality, true),
            (Ordering, true),
            (Membership, true),
            (Between, true),
            (NullCheck, true),
            (BooleanShorthand, false),
        ];
        let timestamp_admission = [
            (Equality, true),
            (Ordering, true),
            (Membership, true),
            (Between, true),
            (NullCheck, true),
            (BooleanShorthand, false),
        ];
        let binary_admission = [
            (Equality, true),
            (Ordering, false),
            (Membership, true),
            (Between, false),
            (NullCheck, true),
            (BooleanShorthand, false),
        ];
        let unsupported_admission = [
            (Equality, false),
            (Ordering, false),
            (Membership, false),
            (Between, false),
            (NullCheck, false),
            (BooleanShorthand, false),
        ];

        for data_type in [DataType::Utf8, DataType::LargeUtf8] {
            assert_type_operator_admission(data_type, String, &string_admission);
        }
        for data_type in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            assert_type_operator_admission(data_type, Integer, &integer_admission);
        }
        assert_type_operator_admission(DataType::Boolean, Boolean, &boolean_admission);
        assert_type_operator_admission(DataType::Date32, Date, &date_admission);
        assert_type_operator_admission(DataType::Decimal128(10, 2), Decimal, &decimal_admission);
        assert_type_operator_admission(
            DataType::Decimal256(38, 18),
            Decimal256,
            &unsupported_admission,
        );
        for data_type in [DataType::Float32, DataType::Float64] {
            assert_type_operator_admission(data_type, Floating, &floating_admission);
        }
        for data_type in [
            DataType::Binary,
            DataType::LargeBinary,
            DataType::FixedSizeBinary(16),
        ] {
            assert_type_operator_admission(data_type, Binary, &binary_admission);
        }
        assert_type_operator_admission(
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Timestamp,
            &timestamp_admission,
        );
        assert_type_operator_admission(
            DataType::Timestamp(TimeUnit::Microsecond, None),
            TimestampNtz,
            &timestamp_admission,
        );
        assert_type_operator_admission(
            DataType::Timestamp(TimeUnit::Microsecond, Some("".into())),
            TimestampNtz,
            &timestamp_admission,
        );
        assert_type_operator_admission(
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            KernelPartitionTypeGroup::Unsupported,
            &unsupported_admission,
        );
        assert_type_operator_admission(
            DataType::Null,
            KernelPartitionTypeGroup::Unsupported,
            &unsupported_admission,
        );
        assert_type_operator_admission(
            DataType::Struct(vec![Field::new("child", DataType::Utf8, true)].into()),
            KernelPartitionTypeGroup::Unsupported,
            &unsupported_admission,
        );
        assert_type_operator_admission(
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            KernelPartitionTypeGroup::Unsupported,
            &unsupported_admission,
        );
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
        assert_eq!(kernel_scan_expr(&plan.decisions[0]), Some(&filter));
        assert_eq!(
            plan.decisions[0].filter_analysis.scope,
            DeltaFilterColumnScope::PartitionOnly
        );
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
        assert_eq!(kernel_scan_expr(&plan.decisions[0]), Some(&filter));
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
                && decision.kernel_scan_filter.is_some()
        }));
    }

    #[test]
    fn partition_operator_planner_accepts_boolean_null_checks_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["is_current"]);
        let filters = [col("is_current").is_null(), col("is_current").is_not_null()];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_date_null_checks_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["event_date"]);
        let filters = [
            col("event_date").is_null(),
            col("event_date").is_not_null(),
            Expr::Not(Box::new(col("event_date").is_null())),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_decimal_null_checks_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["amount"]);
        let filters = [
            col("amount").is_null(),
            col("amount").is_not_null(),
            Expr::Not(Box::new(col("amount").is_null())),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_decimal_equality_and_membership_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["amount"]);
        let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
        let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
        let negative = Expr::Literal(ScalarValue::Decimal128(Some(-123), 10, 2), None);
        let filters = [
            col("amount").eq(amount.clone()),
            amount.clone().eq(col("amount")),
            col("amount").not_eq(amount.clone()),
            col("amount").in_list(vec![amount.clone(), zero, amount.clone()], false),
            negative.eq(col("amount")),
            col("amount").in_list(vec![amount], true),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_decimal_comparisons_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["amount"]);
        let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
        let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
        let negative = Expr::Literal(ScalarValue::Decimal128(Some(-123), 10, 2), None);
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

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_floating_null_checks_as_kernel_exact() {
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

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_timestamp_null_checks_as_kernel_exact() {
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

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_timestamp_ntz_null_checks_as_kernel_exact() {
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

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_binary_null_checks_as_kernel_exact() {
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

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_binary_equality_and_membership_as_kernel_exact() {
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

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_extracts_binary_partition_term_from_mixed_and() {
        let schema = schema();
        let partition_columns = partition_columns(&["payload"]);
        let payload = Expr::Literal(ScalarValue::Binary(Some(b"hello".to_vec())), None);
        let partition_filter = col("payload").eq(payload);
        let data_filter = col("id").gt(lit(10_i32));
        let filter = partition_filter.clone().and(data_filter);

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[&filter],
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Inexact]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 1);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, 1);
        assert_eq!(plan.residual_filter_count, 1);
        assert!(plan.decisions[0].residual);
        assert_eq!(
            kernel_scan_expr(&plan.decisions[0]),
            Some(&partition_filter)
        );
        assert!(plan.decisions[0].kernel_scan_filter.is_some());
    }

    #[test]
    fn partition_operator_planner_accepts_timestamp_equality_membership_ranges_and_composition_as_kernel_exact()
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
            col("event_ts").in_list(vec![timestamp.clone(), other.clone()], false),
            col("event_ts").in_list(vec![timestamp.clone()], true),
            col("event_ts").gt(timestamp.clone()),
            timestamp.clone().gt(col("event_ts")),
            col("event_ts").between(timestamp.clone(), timestamp.clone()),
            col("event_ts").not_between(other.clone(), timestamp.clone()),
            col("event_ts")
                .gt(other.clone())
                .and(col("event_ts").lt(timestamp.clone())),
            col("event_ts")
                .eq(timestamp.clone())
                .or(col("event_ts").is_null()),
            Expr::Not(Box::new(col("event_ts").eq(timestamp))),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_extracts_timestamp_partition_term_from_mixed_and() {
        let schema = schema();
        let partition_columns = partition_columns(&["event_ts"]);
        let timestamp = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), Some("UTC".into())),
            None,
        );
        let partition_filter = col("event_ts").eq(timestamp);
        let data_filter = col("id").gt(lit(10_i32));
        let filter = partition_filter.clone().and(data_filter);

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[&filter],
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Inexact]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 1);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, 1);
        assert_eq!(plan.residual_filter_count, 1);
        assert!(plan.decisions[0].residual);
        assert_eq!(
            kernel_scan_expr(&plan.decisions[0]),
            Some(&partition_filter)
        );
        assert!(plan.decisions[0].kernel_scan_filter.is_some());
    }

    #[test]
    fn partition_operator_planner_extracts_timestamp_ntz_partition_term_from_mixed_and() {
        let schema = schema();
        let partition_columns = partition_columns(&["event_ts_ntz"]);
        let timestamp = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
            None,
        );
        let partition_filter = col("event_ts_ntz").eq(timestamp);
        let data_filter = col("id").gt(lit(10_i32));
        let filter = partition_filter.clone().and(data_filter);

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[&filter],
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Inexact]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 1);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, 1);
        assert_eq!(plan.residual_filter_count, 1);
        assert!(plan.decisions[0].residual);
        assert_eq!(
            kernel_scan_expr(&plan.decisions[0]),
            Some(&partition_filter)
        );
        assert!(plan.decisions[0].kernel_scan_filter.is_some());
    }

    #[test]
    fn partition_operator_planner_accepts_timestamp_ntz_equality_membership_ranges_and_composition_as_kernel_exact()
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
            col("event_ts_ntz").in_list(vec![timestamp.clone(), other.clone()], false),
            col("event_ts_ntz").in_list(vec![timestamp.clone()], true),
            col("event_ts_ntz").gt(timestamp.clone()),
            timestamp.clone().gt(col("event_ts_ntz")),
            col("event_ts_ntz").between(timestamp.clone(), timestamp.clone()),
            col("event_ts_ntz").not_between(other.clone(), timestamp.clone()),
            col("event_ts_ntz")
                .gt(other.clone())
                .and(col("event_ts_ntz").lt(timestamp.clone())),
            col("event_ts_ntz")
                .eq(timestamp.clone())
                .or(col("event_ts_ntz").is_null()),
            Expr::Not(Box::new(col("event_ts_ntz").eq(timestamp))),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
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
        let timestamp_empty_timezone = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), Some("".into())),
            None,
        );
        let timestamp_second = Expr::Literal(
            ScalarValue::TimestampSecond(Some(1), Some("UTC".into())),
            None,
        );
        let timestamp_nanosecond = Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(1_767_225_600_123_456_000), Some("UTC".into())),
            None,
        );
        let null_timestamp = Expr::Literal(
            ScalarValue::TimestampMicrosecond(None, Some("UTC".into())),
            None,
        );
        let filters = [
            col("event_ts").eq(timestamp_without_timezone.clone()),
            col("event_ts").eq(timestamp_empty_timezone.clone()),
            col("event_ts").eq(timestamp_second.clone()),
            col("event_ts").eq(timestamp_nanosecond.clone()),
            col("event_ts").eq(null_timestamp.clone()),
            col("event_ts").gt(timestamp_without_timezone),
            col("event_ts").gt(timestamp_empty_timezone),
            col("event_ts").gt(timestamp_second),
            col("event_ts").gt(timestamp_nanosecond),
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
        let timestamp_empty_timezone = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), Some("".into())),
            None,
        );
        let timestamp_second = Expr::Literal(ScalarValue::TimestampSecond(Some(1), None), None);
        let timestamp_nanosecond = Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(1_767_225_600_123_456_000), None),
            None,
        );
        let null_timestamp = Expr::Literal(ScalarValue::TimestampMicrosecond(None, None), None);
        let filters = [
            col("event_ts_ntz").eq(timestamp_utc.clone()),
            col("event_ts_ntz").eq(timestamp_empty_timezone.clone()),
            col("event_ts_ntz").eq(timestamp_second.clone()),
            col("event_ts_ntz").eq(timestamp_nanosecond.clone()),
            col("event_ts_ntz").eq(null_timestamp.clone()),
            col("event_ts_ntz").gt(timestamp_utc),
            col("event_ts_ntz").gt(timestamp_empty_timezone),
            col("event_ts_ntz").gt(timestamp_second),
            col("event_ts_ntz").gt(timestamp_nanosecond),
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
    fn partition_operator_planner_accepts_floating_equality_and_membership_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["float_part", "double_part"]);
        let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
        let negative_float = Expr::Literal(ScalarValue::Float32(Some(-1.5)), None);
        let double_value = Expr::Literal(ScalarValue::Float64(Some(-2.25)), None);
        let double_other = Expr::Literal(ScalarValue::Float64(Some(4.0)), None);
        let filters = [
            col("float_part").eq(float_value.clone()),
            float_value.clone().eq(col("float_part")),
            col("float_part").not_eq(float_value.clone()),
            col("float_part").in_list(vec![float_value.clone(), negative_float], false),
            col("float_part").in_list(vec![float_value], true),
            col("double_part").eq(double_value.clone()),
            col("double_part").in_list(vec![double_value.clone(), double_other], false),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_rejects_floating_comparisons_and_between() {
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
    fn partition_operator_planner_rejects_unproven_floating_literal_shapes() {
        let schema = schema();
        let partition_columns = partition_columns(&["float_part", "double_part"]);
        let float_value = Expr::Literal(ScalarValue::Float32(Some(1.5)), None);
        let positive_zero = Expr::Literal(ScalarValue::Float32(Some(0.0)), None);
        let negative_zero = Expr::Literal(ScalarValue::Float32(Some(-0.0)), None);
        let float_nan = Expr::Literal(ScalarValue::Float32(Some(f32::NAN)), None);
        let float_infinity = Expr::Literal(ScalarValue::Float32(Some(f32::INFINITY)), None);
        let double_value = Expr::Literal(ScalarValue::Float64(Some(1.5)), None);
        let filters = [
            col("float_part").eq(positive_zero.clone()),
            col("float_part").eq(negative_zero.clone()),
            col("float_part").eq(float_nan.clone()),
            col("float_part").eq(float_infinity.clone()),
            col("float_part").eq(Expr::Literal(ScalarValue::Float32(None), None)),
            col("float_part").eq(double_value),
            col("double_part").eq(float_value.clone()),
            col("float_part").eq(lit("1.5")),
            col("float_part").eq(lit(1_i64)),
            col("float_part").in_list(vec![float_value.clone(), positive_zero], false),
            col("float_part").in_list(vec![float_value.clone(), negative_zero], true),
            col("float_part").in_list(vec![float_value.clone(), float_nan], false),
            col("float_part").in_list(vec![float_value, float_infinity], true),
            col("float_part").in_list(vec![], false),
            col("float_part").in_list(vec![], true),
        ];

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filters.iter().collect::<Vec<_>>(),
            &schema,
            &partition_columns,
        );

        assert_all_unsupported(&plan, filters.len());
    }

    #[test]
    fn partition_operator_planner_accepts_decimal_between_as_kernel_exact() {
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

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_decimal_composition_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["amount"]);
        let amount = Expr::Literal(ScalarValue::Decimal128(Some(12_345), 10, 2), None);
        let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
        let negative = Expr::Literal(ScalarValue::Decimal128(Some(-123), 10, 2), None);
        let filters = [
            col("amount")
                .gt_eq(zero.clone())
                .and(col("amount").lt(amount.clone())),
            col("amount")
                .eq(amount.clone())
                .or(col("amount").eq(negative)),
            Expr::Not(Box::new(col("amount").eq(amount))),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_extracts_decimal_partition_term_from_mixed_and() {
        let schema = schema();
        let partition_columns = partition_columns(&["amount"]);
        let zero = Expr::Literal(ScalarValue::Decimal128(Some(0), 10, 2), None);
        let partition_filter = col("amount").gt_eq(zero);
        let data_filter = col("region").eq(lit("us-west"));
        let filter = partition_filter.clone().and(data_filter);

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[&filter],
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Inexact]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 1);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, 1);
        assert_eq!(plan.residual_filter_count, 1);
        assert!(plan.decisions[0].residual);
        assert_eq!(
            kernel_scan_expr(&plan.decisions[0]),
            Some(&partition_filter)
        );
        assert!(plan.decisions[0].kernel_scan_filter.is_some());
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
            col("amount").in_list(Vec::<Expr>::new(), false),
            col("amount").in_list(Vec::<Expr>::new(), true),
            col("amount").eq(lit("123.45")),
            col("amount").eq(lit(123_i64)),
            col("amount").eq(lit(123.45_f64)),
            col("amount").eq(Expr::Literal(ScalarValue::Decimal128(None, 10, 2), None)),
            col("amount").eq(Expr::Literal(
                ScalarValue::Decimal256(Some(12_345.into()), 10, 2),
                None,
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
            vec![TableProviderFilterPushDown::Unsupported; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.unsupported_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
    }

    #[test]
    fn partition_operator_planner_accepts_boolean_equality_and_membership_as_kernel_exact() {
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

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
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
            col("is_current").in_list(Vec::<Expr>::new(), false),
            col("is_current").in_list(Vec::<Expr>::new(), true),
            datafusion::logical_expr::cast(col("is_current"), DataType::Boolean).eq(lit(true)),
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
    fn partition_operator_planner_accepts_boolean_shorthand_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["is_current"]);
        let filters = [col("is_current"), Expr::Not(Box::new(col("is_current")))];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        let expected_filters = [
            col("is_current").eq(lit(true)),
            Expr::Not(Box::new(col("is_current").eq(lit(true)))),
        ];

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Exact; filters.len()]
        );
        assert_eq!(plan.exact_count, filters.len());
        assert_eq!(plan.inexact_count, 0);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, filters.len());
        assert_eq!(plan.residual_filter_count, 0);
        for (decision, filter) in plan.decisions.iter().zip(expected_filters.iter()) {
            assert_eq!(decision.outcome, DeltaFilterPushdownOutcome::Exact);
            assert!(!decision.residual);
            assert!(decision.rejection_reason.is_none());
            assert!(decision.kernel_scan_filter.is_some());
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_boolean_composition_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["is_current"]);
        let filters = [
            col("is_current")
                .eq(lit(true))
                .or(col("is_current").is_null()),
            col("is_current")
                .not_eq(lit(false))
                .and(col("is_current").is_not_null()),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_extracts_boolean_partition_term_from_mixed_and() {
        let schema = schema();
        let partition_columns = partition_columns(&["is_current"]);
        let partition_filter = col("is_current").eq(lit(true));
        let data_filter = col("region").eq(lit("us-west"));
        let filter = partition_filter.clone().and(data_filter);

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[&filter],
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Inexact]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 1);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, 1);
        assert_eq!(plan.residual_filter_count, 1);
        assert!(plan.decisions[0].residual);
        assert_eq!(
            kernel_scan_expr(&plan.decisions[0]),
            Some(&partition_filter)
        );
        assert!(plan.decisions[0].kernel_scan_filter.is_some());
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
    fn partition_operator_planner_accepts_date_equality_and_membership_as_kernel_exact() {
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

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
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
            datafusion::logical_expr::cast(col("event_date"), DataType::Date32).eq(date.clone()),
            col("event_date").in_list(Vec::<Expr>::new(), false),
            col("event_date").in_list(Vec::<Expr>::new(), true),
            col("event_date").in_list(
                vec![date.clone(), Expr::Literal(ScalarValue::Date32(None), None)],
                false,
            ),
            col("event_date").in_list(vec![date.clone(), lit("2024-02-29")], false),
            col("event_date").in_list(vec![col("day")], false),
            col("event_date").between(Expr::Literal(ScalarValue::Date32(None), None), date.clone()),
            col("event_date").between(col("day"), date),
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
    fn partition_operator_planner_accepts_date_partition_comparisons_as_kernel_exact() {
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

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_date_partition_between_as_kernel_exact() {
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

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_date_composition_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["event_date"]);
        let new_year_2026 = Expr::Literal(ScalarValue::Date32(Some(20_454)), None);
        let leap_day_2024 = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
        let next_day = Expr::Literal(ScalarValue::Date32(Some(20_455)), None);
        let filters = [
            col("event_date")
                .eq(new_year_2026.clone())
                .or(col("event_date").is_null()),
            col("event_date")
                .gt_eq(leap_day_2024)
                .and(col("event_date").lt(next_day)),
            Expr::Not(Box::new(col("event_date").eq(new_year_2026))),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_extracts_date_partition_term_from_mixed_and() {
        let schema = schema();
        let partition_columns = partition_columns(&["event_date"]);
        let leap_day_2024 = Expr::Literal(ScalarValue::Date32(Some(19_782)), None);
        let partition_filter = col("event_date").gt_eq(leap_day_2024);
        let data_filter = col("region").eq(lit("us-west"));
        let filter = partition_filter.clone().and(data_filter);

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &[&filter],
            &schema,
            &partition_columns,
        );

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Inexact]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 1);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, 1);
        assert_eq!(plan.residual_filter_count, 1);
        assert!(plan.decisions[0].residual);
        assert_eq!(
            kernel_scan_expr(&plan.decisions[0]),
            Some(&partition_filter)
        );
        assert!(plan.decisions[0].kernel_scan_filter.is_some());
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
        assert_eq!(kernel_scan_expr(&plan.decisions[0]), Some(&filter));
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
    fn partition_operator_planner_accepts_integer_equality_and_membership_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["byte_part", "short_part", "int_part", "id"]);
        let byte_value = Expr::Literal(ScalarValue::Int8(Some(i8::MIN)), None);
        let short_value = Expr::Literal(ScalarValue::Int16(Some(-1024)), None);
        let int_value = Expr::Literal(ScalarValue::Int32(Some(0)), None);
        let long_value = Expr::Literal(ScalarValue::Int64(Some(i64::MAX)), None);
        let filters = [
            col("byte_part").eq(byte_value.clone()),
            byte_value.eq(col("byte_part")),
            col("byte_part").not_eq(Expr::Literal(ScalarValue::Int8(Some(i8::MAX)), None)),
            col("short_part").in_list(
                vec![
                    short_value.clone(),
                    Expr::Literal(ScalarValue::Int16(Some(0)), None),
                    short_value,
                ],
                false,
            ),
            col("int_part").in_list(vec![int_value.clone()], true),
            col("id").eq(long_value.clone()),
            col("id").in_list(
                vec![Expr::Literal(ScalarValue::Int64(Some(0)), None), long_value],
                false,
            ),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_integer_null_checks_as_kernel_exact() {
        let schema = schema();
        let partition_columns = partition_columns(&["byte_part", "short_part", "int_part", "id"]);
        let filters = [
            col("byte_part").is_null(),
            col("short_part").is_not_null(),
            Expr::Not(Box::new(col("int_part").is_null())),
            col("id").is_null().or(col("id").eq(lit(7_i64))),
        ];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let plan = DeltaFilterPushdownPlan::partition_operator_pushdown(
            &filter_refs,
            &schema,
            &partition_columns,
        );

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_integer_partition_comparisons_as_kernel_exact() {
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

        assert_all_exact(&plan, filters.len());
        for (decision, filter) in plan.decisions.iter().zip(filters.iter()) {
            assert_eq!(kernel_scan_expr(decision), Some(filter));
        }
    }

    #[test]
    fn partition_operator_planner_accepts_string_and_integer_between_as_kernel_exact() {
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
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
            ]
        );
        assert_eq!(plan.exact_count, filters.len());
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.residual_filter_count, 0);
    }

    #[test]
    fn partition_operator_planner_rejects_unproven_integer_literal_shapes() {
        let schema = schema();
        let partition_columns = partition_columns(&["byte_part", "short_part", "int_part", "id"]);
        let filters = [
            col("byte_part").eq(Expr::Literal(ScalarValue::Int16(Some(7)), None)),
            col("short_part").eq(Expr::Literal(ScalarValue::Int32(Some(7)), None)),
            col("int_part").eq(lit(7_i64)),
            col("id").eq(Expr::Literal(ScalarValue::Int32(Some(7)), None)),
            col("id").eq(lit("7")),
            col("id").eq(Expr::Literal(ScalarValue::Int64(None), None)),
            col("id").in_list(Vec::<Expr>::new(), false),
            col("id").in_list(Vec::<Expr>::new(), true),
            col("id").in_list(
                vec![
                    Expr::Literal(ScalarValue::Int64(Some(7)), None),
                    Expr::Literal(ScalarValue::Int32(Some(8)), None),
                ],
                false,
            ),
            col("id").between(
                Expr::Literal(ScalarValue::Int32(Some(1)), None),
                Expr::Literal(ScalarValue::Int64(Some(9)), None),
            ),
            datafusion::logical_expr::cast(col("id"), DataType::Int64).eq(lit(7_i64)),
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
        assert_eq!(kernel_scan_expr(&plan.decisions[0]), Some(&lit(false)));
        assert_eq!(
            kernel_scan_expr(&plan.decisions[1]),
            Some(&col("region").is_not_null())
        );
        assert_eq!(
            kernel_scan_expr(&plan.decisions[3]),
            Some(&empty_string_equality)
        );
        assert_eq!(kernel_scan_expr(&plan.decisions[4]), Some(&empty_string_in));
        assert_eq!(
            kernel_scan_expr(&plan.decisions[5]),
            Some(&empty_string_not_in)
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
        assert_eq!(kernel_scan_expr(&plan.decisions[0]), Some(&exact));
        assert!(plan.decisions[1].kernel_scan_filter.is_none());
        assert_eq!(kernel_scan_expr(&plan.decisions[2]), Some(&duplicate_exact));
    }

    #[test]
    fn partition_operator_planner_extracts_top_level_mixed_and_partition_terms_as_inexact() {
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

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Inexact]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 1);
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, 1);
        assert_eq!(plan.residual_filter_count, 1);
        assert!(plan.decisions[0].residual);
        assert_eq!(
            kernel_scan_expr(&plan.decisions[0]),
            Some(&region_filter.and(day_filter))
        );
        assert!(plan.decisions[0].kernel_scan_filter.is_some());
    }

    #[test]
    fn partition_operator_planner_accepts_boolean_data_shorthand_residuals_in_mixed_and() {
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

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![TableProviderFilterPushDown::Inexact; filters.len()]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, filters.len());
        assert_eq!(plan.unsupported_count, 0);
        assert_eq!(plan.pushed_filter_count, filters.len());
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions.iter().all(|decision| {
            decision.outcome == DeltaFilterPushdownOutcome::Inexact
                && decision.residual
                && decision.rejection_reason.is_none()
                && decision.kernel_scan_filter.is_some()
        }));
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
            col("region")
                .in_list(
                    vec![lit("us-west"), Expr::Literal(ScalarValue::Utf8(None), None)],
                    false,
                )
                .and(col("id").gt(lit(1_i64))),
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

        assert_eq!(
            plan.datafusion_pushdowns(),
            vec![
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Inexact,
            ]
        );
        assert_eq!(plan.exact_count, 0);
        assert_eq!(plan.inexact_count, 2);
        assert_eq!(plan.unsupported_count, 4);
        assert_eq!(plan.pushed_filter_count, 2);
        assert_eq!(plan.residual_filter_count, filters.len());
        assert!(plan.decisions[0].kernel_scan_filter.is_some());
        assert!(plan.decisions[5].kernel_scan_filter.is_some());
        assert!(
            plan.decisions[1..5]
                .iter()
                .all(|decision| decision.kernel_scan_filter.is_none())
        );
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
            col("payload").eq(Expr::Literal(
                ScalarValue::LargeBinary(Some(b"hello".to_vec())),
                None,
            )),
            col("payload").eq(Expr::Literal(
                ScalarValue::FixedSizeBinary(5, Some(b"hello".to_vec())),
                None,
            )),
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
        assert!(plan.decisions[0].kernel_scan_filter.is_none());
        assert!(plan.decisions[1].kernel_scan_filter.is_none());
    }
}
