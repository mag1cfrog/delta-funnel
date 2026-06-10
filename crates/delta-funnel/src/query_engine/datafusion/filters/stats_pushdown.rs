//! File-statistics filter pushdown policy.

use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{Expr, Operator};

use crate::table_formats::datafusion_expr_to_kernel_predicate;

use super::{ExactPartitionKernelFilter, KernelScanFilterKind};

pub(super) fn try_data_stats_kernel_filter(
    filter: &Expr,
    schema: &SchemaRef,
) -> Option<ExactPartitionKernelFilter> {
    if !is_supported_data_stats_filter(filter, schema) {
        return None;
    }

    Some(ExactPartitionKernelFilter {
        datafusion_expr: filter.clone(),
        kernel_predicate: datafusion_expr_to_kernel_predicate(filter).ok()?,
        kind: KernelScanFilterKind::DataStats,
    })
}

fn is_supported_data_stats_filter(filter: &Expr, schema: &SchemaRef) -> bool {
    is_supported_integer_data_stats_filter(filter, schema)
        || is_supported_boolean_null_count_stats_filter(filter, schema)
        || is_supported_decimal_data_stats_filter(filter, schema)
        || is_supported_string_data_stats_filter(filter, schema)
        || is_supported_temporal_data_stats_filter(filter, schema)
}

fn is_supported_integer_data_stats_filter(filter: &Expr, schema: &SchemaRef) -> bool {
    let Expr::BinaryExpr(binary) = filter else {
        return false;
    };

    if !matches!(
        binary.op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    ) {
        return false;
    }

    is_same_width_integer_data_column_literal(binary.left.as_ref(), binary.right.as_ref(), schema)
        || is_same_width_integer_data_column_literal(
            binary.right.as_ref(),
            binary.left.as_ref(),
            schema,
        )
}

fn is_supported_boolean_null_count_stats_filter(filter: &Expr, schema: &SchemaRef) -> bool {
    match filter {
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            is_data_column_with_type(inner.as_ref(), schema, |data_type| {
                matches!(data_type, DataType::Boolean)
            })
        }
        _ => false,
    }
}

fn is_supported_decimal_data_stats_filter(filter: &Expr, schema: &SchemaRef) -> bool {
    match filter {
        Expr::BinaryExpr(binary) => {
            if !matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) {
                return false;
            }

            is_same_type_decimal_data_column_literal(
                binary.left.as_ref(),
                binary.right.as_ref(),
                schema,
            ) || is_same_type_decimal_data_column_literal(
                binary.right.as_ref(),
                binary.left.as_ref(),
                schema,
            )
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            is_data_column_with_type(inner.as_ref(), schema, |data_type| {
                matches!(data_type, DataType::Decimal128(_, _))
            })
        }
        _ => false,
    }
}

fn is_same_type_decimal_data_column_literal(
    column: &Expr,
    literal: &Expr,
    schema: &SchemaRef,
) -> bool {
    let Some(data_type) = data_column_type(column, schema) else {
        return false;
    };

    matches!(
        (data_type, literal),
        (
            DataType::Decimal128(precision, scale),
            Expr::Literal(ScalarValue::Decimal128(Some(_), literal_precision, literal_scale), _),
        ) if precision == literal_precision && scale == literal_scale
    )
}

fn is_supported_string_data_stats_filter(filter: &Expr, schema: &SchemaRef) -> bool {
    match filter {
        Expr::BinaryExpr(binary) => {
            if !matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) {
                return false;
            }

            is_string_data_column_literal(binary.left.as_ref(), binary.right.as_ref(), schema)
                || is_string_data_column_literal(
                    binary.right.as_ref(),
                    binary.left.as_ref(),
                    schema,
                )
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            is_data_column_with_type(inner.as_ref(), schema, |data_type| {
                matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
            })
        }
        _ => false,
    }
}

fn is_string_data_column_literal(column: &Expr, literal: &Expr, schema: &SchemaRef) -> bool {
    is_data_column_with_type(column, schema, |data_type| {
        matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
    }) && matches!(
        literal,
        Expr::Literal(
            ScalarValue::Utf8(Some(_)) | ScalarValue::LargeUtf8(Some(_)),
            _
        )
    )
}

fn is_supported_temporal_data_stats_filter(filter: &Expr, schema: &SchemaRef) -> bool {
    match filter {
        Expr::BinaryExpr(binary) => {
            let is_date_not_equals = binary.op == Operator::NotEq
                && (is_same_type_date_data_column_literal(
                    binary.left.as_ref(),
                    binary.right.as_ref(),
                    schema,
                ) || is_same_type_date_data_column_literal(
                    binary.right.as_ref(),
                    binary.left.as_ref(),
                    schema,
                ));
            let is_supported_operator = matches!(
                binary.op,
                Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
            ) || is_date_not_equals;

            if !is_supported_operator {
                return false;
            }

            is_same_type_temporal_data_column_literal(
                binary.left.as_ref(),
                binary.right.as_ref(),
                schema,
            ) || is_same_type_temporal_data_column_literal(
                binary.right.as_ref(),
                binary.left.as_ref(),
                schema,
            )
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            is_data_column_with_type(inner.as_ref(), schema, is_temporal_data_type)
        }
        _ => false,
    }
}

fn is_same_type_temporal_data_column_literal(
    column: &Expr,
    literal: &Expr,
    schema: &SchemaRef,
) -> bool {
    is_same_type_date_data_column_literal(column, literal, schema)
        || is_same_type_timestamp_data_column_literal(column, literal, schema)
}

fn is_same_type_date_data_column_literal(
    column: &Expr,
    literal: &Expr,
    schema: &SchemaRef,
) -> bool {
    is_data_column_with_type(column, schema, |data_type| {
        matches!(data_type, DataType::Date32)
    }) && matches!(literal, Expr::Literal(ScalarValue::Date32(Some(_)), _))
}

fn is_same_type_timestamp_data_column_literal(
    column: &Expr,
    literal: &Expr,
    schema: &SchemaRef,
) -> bool {
    let Some(data_type) = data_column_type(column, schema) else {
        return false;
    };

    match (data_type, literal) {
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some(field_timezone)),
            Expr::Literal(ScalarValue::TimestampMicrosecond(Some(_), Some(literal_timezone)), _),
        ) => !field_timezone.is_empty() && field_timezone == literal_timezone,
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Expr::Literal(ScalarValue::TimestampMicrosecond(Some(_), None), _),
        ) => true,
        _ => false,
    }
}

fn is_temporal_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, Some(_))
            | DataType::Timestamp(TimeUnit::Microsecond, None)
    )
}

fn is_data_column_with_type(
    expr: &Expr,
    schema: &SchemaRef,
    predicate: impl FnOnce(&DataType) -> bool,
) -> bool {
    data_column_type(expr, schema).is_some_and(predicate)
}

fn data_column_type<'a>(expr: &Expr, schema: &'a SchemaRef) -> Option<&'a DataType> {
    let Expr::Column(column) = expr else {
        return None;
    };

    if !is_unqualified_top_level_column(column) {
        return None;
    }

    schema
        .field_with_name(&column.name)
        .ok()
        .map(|field| field.data_type())
}

fn is_same_width_integer_data_column_literal(
    column: &Expr,
    literal: &Expr,
    schema: &SchemaRef,
) -> bool {
    let Expr::Column(column) = column else {
        return false;
    };

    if !is_unqualified_top_level_column(column) {
        return false;
    }

    let Ok(field) = schema.field_with_name(&column.name) else {
        return false;
    };

    matches!(
        (field.data_type(), literal),
        (DataType::Int8, Expr::Literal(ScalarValue::Int8(Some(_)), _))
            | (
                DataType::Int16,
                Expr::Literal(ScalarValue::Int16(Some(_)), _)
            )
            | (
                DataType::Int32,
                Expr::Literal(ScalarValue::Int32(Some(_)), _)
            )
            | (
                DataType::Int64,
                Expr::Literal(ScalarValue::Int64(Some(_)), _)
            )
    )
}

fn is_unqualified_top_level_column(column: &Column) -> bool {
    column.relation.is_none() && !column.name.contains('.')
}
