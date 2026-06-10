//! File-statistics filter pushdown policy.

use datafusion::arrow::datatypes::{DataType, SchemaRef};
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
            is_boolean_data_column(inner.as_ref(), schema)
        }
        _ => false,
    }
}

fn is_boolean_data_column(expr: &Expr, schema: &SchemaRef) -> bool {
    let Expr::Column(column) = expr else {
        return false;
    };

    if !is_unqualified_top_level_column(column) {
        return false;
    }

    schema
        .field_with_name(&column.name)
        .is_ok_and(|field| matches!(field.data_type(), DataType::Boolean))
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
