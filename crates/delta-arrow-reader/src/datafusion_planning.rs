//! Private static planning for the optional DataFusion adapter.

#![allow(dead_code)]

use std::{collections::HashSet, sync::Arc};

use arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion::{
    common::{
        Column, ScalarValue,
        tree_node::{Transformed, TransformedResult, TreeNode},
    },
    logical_expr::{Expr, Operator, TableProviderFilterPushDown},
};

use crate::{
    DeltaComparison, DeltaPredicate, DeltaReaderError, DeltaScalar,
    error::{InvalidProjectionSnafu, UnsupportedPredicateSnafu},
    predicate::validate_predicate,
};

#[derive(Clone, Copy, Default)]
pub(crate) struct DataFusionFilterCapabilities {
    pub(crate) exact_predicate_evaluation: bool,
}

pub(crate) struct DataFusionFilterDecision {
    pub(crate) predicate: Option<DeltaPredicate>,
    pub(crate) pushdown: TableProviderFilterPushDown,
    pub(crate) referenced_columns: Vec<String>,
}

pub(crate) struct DataFusionFilterPlan {
    pub(crate) decisions: Vec<DataFusionFilterDecision>,
    pub(crate) predicate: Option<DeltaPredicate>,
    pub(crate) row_predicate: Option<DeltaPredicate>,
    pub(crate) referenced_columns: Vec<String>,
    pub(crate) has_unresolved_predicate: bool,
}

pub(crate) struct DataFusionProjectionPlan {
    pub(crate) output_schema: SchemaRef,
    pub(crate) physical_projection: Option<Vec<String>>,
    pub(crate) hidden_columns: Vec<String>,
    pub(crate) output_projection: Option<Vec<usize>>,
}

pub(crate) struct DataFusionScanPlanning {
    pub(crate) projection: DataFusionProjectionPlan,
    pub(crate) filters: DataFusionFilterPlan,
}

pub(crate) fn plan_datafusion_scan(
    schema: &SchemaRef,
    partition_columns: &HashSet<String>,
    projection: Option<&[usize]>,
    filters: &[&Expr],
    capabilities: DataFusionFilterCapabilities,
) -> Result<DataFusionScanPlanning, DeltaReaderError> {
    validate_projection(schema, projection)?;
    let filter_plan = plan_filters(schema, partition_columns, filters, capabilities);
    validate_inexact_residual_projection(schema, projection, &filter_plan)?;
    let projection_plan = plan_projection(schema, projection, &filter_plan.referenced_columns)?;
    Ok(DataFusionScanPlanning {
        projection: projection_plan,
        filters: filter_plan,
    })
}

fn validate_inexact_residual_projection(
    schema: &Schema,
    projection: Option<&[usize]>,
    filters: &DataFusionFilterPlan,
) -> Result<(), DeltaReaderError> {
    let Some(projection) = projection else {
        return Ok(());
    };
    let projected_columns = projection
        .iter()
        .map(|&index| schema.field(index).name().as_str())
        .collect::<HashSet<_>>();
    let missing_residual_column = filters
        .decisions
        .iter()
        .filter(|decision| decision.pushdown == TableProviderFilterPushDown::Inexact)
        .flat_map(|decision| &decision.referenced_columns)
        .any(|column| !projected_columns.contains(column.as_str()));
    if missing_residual_column {
        return UnsupportedPredicateSnafu {
            reason: "inexact_filter_columns_not_projected",
        }
        .fail();
    }
    Ok(())
}

fn validate_projection(
    schema: &Schema,
    projection: Option<&[usize]>,
) -> Result<(), DeltaReaderError> {
    let Some(projection) = projection else {
        return Ok(());
    };
    let mut seen = HashSet::with_capacity(projection.len());
    if projection.iter().any(|index| !seen.insert(*index)) {
        return InvalidProjectionSnafu {
            reason: "duplicate_projection_index",
        }
        .fail();
    }
    if projection
        .iter()
        .any(|&index| index >= schema.fields().len())
    {
        return InvalidProjectionSnafu {
            reason: "projection_index_out_of_bounds",
        }
        .fail();
    }
    Ok(())
}

fn plan_projection(
    schema: &SchemaRef,
    projection: Option<&[usize]>,
    filter_columns: &[String],
) -> Result<DataFusionProjectionPlan, DeltaReaderError> {
    let Some(projection) = projection else {
        tracing::debug!(
            target: "delta_arrow_reader::datafusion",
            output_columns = schema.fields().len(),
            physical_columns = schema.fields().len(),
            hidden_columns = 0,
            "planned DataFusion projection"
        );
        return Ok(DataFusionProjectionPlan {
            output_schema: Arc::clone(schema),
            physical_projection: None,
            hidden_columns: Vec::new(),
            output_projection: None,
        });
    };

    let output_schema =
        Arc::new(
            schema
                .project(projection)
                .map_err(|_| DeltaReaderError::InvalidProjection {
                    reason: "arrow_projection_failed",
                })?,
        );
    let physical_projection = projection
        .iter()
        .map(|&index| schema.field(index).name().clone())
        .collect::<Vec<_>>();
    let output_projection = (0..projection.len()).collect::<Vec<_>>();
    let hidden_columns = filter_columns
        .iter()
        .filter(|name| !physical_projection.contains(name))
        .cloned()
        .collect::<Vec<_>>();

    tracing::debug!(
        target: "delta_arrow_reader::datafusion",
        output_columns = projection.len(),
        physical_columns = physical_projection.len() + hidden_columns.len(),
        hidden_columns = hidden_columns.len(),
        "planned DataFusion projection"
    );
    Ok(DataFusionProjectionPlan {
        output_schema,
        physical_projection: Some(physical_projection),
        hidden_columns,
        output_projection: Some(output_projection),
    })
}

fn plan_filters(
    schema: &SchemaRef,
    partition_columns: &HashSet<String>,
    filters: &[&Expr],
    capabilities: DataFusionFilterCapabilities,
) -> DataFusionFilterPlan {
    let decisions = filters
        .iter()
        .map(|filter| {
            let filter = unqualify_filter_columns((*filter).clone(), schema);
            let referenced_columns = filter_column_names(&filter, schema).unwrap_or_default();
            let Some(translation) =
                translate_filter_for_pushdown(&filter, schema, partition_columns)
            else {
                return DataFusionFilterDecision {
                    predicate: None,
                    pushdown: TableProviderFilterPushDown::Unsupported,
                    referenced_columns,
                };
            };
            let pushdown = match translation.kind {
                TranslatedFilterKind::Partition => TableProviderFilterPushDown::Exact,
                TranslatedFilterKind::DataStats if capabilities.exact_predicate_evaluation => {
                    TableProviderFilterPushDown::Exact
                }
                TranslatedFilterKind::DataStats | TranslatedFilterKind::MixedAnd => {
                    TableProviderFilterPushDown::Inexact
                }
            };
            DataFusionFilterDecision {
                predicate: Some(translation.predicate),
                pushdown,
                referenced_columns: translation.referenced_columns,
            }
        })
        .collect::<Vec<_>>();
    let predicates = decisions
        .iter()
        .filter_map(|decision| decision.predicate.clone())
        .collect::<Vec<_>>();
    let predicate = and_predicates(predicates);
    let row_predicate = and_predicates(
        decisions
            .iter()
            .filter(|decision| decision.pushdown == TableProviderFilterPushDown::Exact)
            .filter(|decision| {
                decision
                    .referenced_columns
                    .iter()
                    .all(|column| !partition_columns.contains(column))
            })
            .filter_map(|decision| decision.predicate.clone())
            .collect(),
    );
    let mut referenced_columns = Vec::new();
    for column in decisions
        .iter()
        .filter(|decision| decision.predicate.is_some())
        .flat_map(|decision| &decision.referenced_columns)
    {
        if !referenced_columns.contains(column) {
            referenced_columns.push(column.clone());
        }
    }
    let has_unresolved_predicate = decisions
        .iter()
        .any(|decision| decision.pushdown != TableProviderFilterPushDown::Exact);
    let exact = decisions
        .iter()
        .filter(|decision| decision.pushdown == TableProviderFilterPushDown::Exact)
        .count();
    let inexact = decisions
        .iter()
        .filter(|decision| decision.pushdown == TableProviderFilterPushDown::Inexact)
        .count();
    let unsupported = decisions.len() - exact - inexact;
    tracing::debug!(
        target: "delta_arrow_reader::datafusion",
        filters = decisions.len(),
        exact,
        inexact,
        unsupported,
        "planned DataFusion filters"
    );
    DataFusionFilterPlan {
        decisions,
        predicate,
        row_predicate,
        referenced_columns,
        has_unresolved_predicate,
    }
}

fn unqualify_filter_columns(filter: Expr, schema: &SchemaRef) -> Expr {
    let original_filter = filter.clone();
    match filter
        .transform(|expr| {
            if let Expr::Column(column) = expr {
                if is_relation_qualified_top_level_column(&column, schema) {
                    Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                        column.name,
                    ))))
                } else {
                    Ok(Transformed::no(Expr::Column(column)))
                }
            } else {
                Ok(Transformed::no(expr))
            }
        })
        .data()
    {
        Ok(filter) => filter,
        Err(_) => original_filter,
    }
}

fn is_relation_qualified_top_level_column(column: &Column, schema: &SchemaRef) -> bool {
    let flat_name = column.flat_name();
    let Some((first_segment, _)) = flat_name.split_once('.') else {
        return false;
    };
    schema.field_with_name(&column.name).is_ok() && schema.field_with_name(first_segment).is_err()
}

struct TranslatedFilter {
    predicate: DeltaPredicate,
    referenced_columns: Vec<String>,
    kind: TranslatedFilterKind,
}

#[derive(Clone, Copy)]
enum TranslatedFilterKind {
    Partition,
    DataStats,
    MixedAnd,
}

fn translate_filter_for_pushdown(
    filter: &Expr,
    schema: &Schema,
    partition_columns: &HashSet<String>,
) -> Option<TranslatedFilter> {
    let referenced_columns = filter_column_names(filter, schema)?;
    if referenced_columns.is_empty() {
        return None;
    }
    let partition_only = referenced_columns
        .iter()
        .all(|column| partition_columns.contains(column));
    let data_only = referenced_columns
        .iter()
        .all(|column| !partition_columns.contains(column));

    if partition_only && let Some(predicate) = exact_partition_predicate(filter, schema) {
        return Some(TranslatedFilter {
            predicate,
            referenced_columns,
            kind: TranslatedFilterKind::Partition,
        });
    }
    if data_only && let Some(predicate) = data_stats_predicate(filter, schema) {
        return Some(TranslatedFilter {
            predicate,
            referenced_columns,
            kind: TranslatedFilterKind::DataStats,
        });
    }

    Some(TranslatedFilter {
        predicate: mixed_and_predicate(filter, schema, partition_columns)?,
        referenced_columns,
        kind: TranslatedFilterKind::MixedAnd,
    })
}

fn filter_column_names(filter: &Expr, schema: &Schema) -> Option<Vec<String>> {
    if filter.column_refs().iter().any(|column| {
        column.name.starts_with("__delta_arrow_reader_")
            || column.name.starts_with("__delta_funnel_")
    }) {
        return None;
    }
    let mut columns = filter
        .column_refs()
        .iter()
        .map(|column| column_name(column))
        .collect::<Option<Vec<_>>>()?;
    if columns
        .iter()
        .any(|column| schema.field_with_name(column).is_err())
    {
        return None;
    }
    columns.sort();
    columns.dedup();
    Some(columns)
}

fn mixed_and_predicate(
    filter: &Expr,
    schema: &Schema,
    partition_columns: &HashSet<String>,
) -> Option<DeltaPredicate> {
    if !matches!(filter, Expr::BinaryExpr(binary) if binary.op == Operator::And) {
        return None;
    }

    let mut terms = Vec::new();
    collect_top_level_and_terms(filter, &mut terms);
    let mut predicates = Vec::new();
    let mut residual_term_count = 0_usize;
    for term in terms {
        if !is_filter_candidate(term) {
            return None;
        }
        let columns = filter_column_names(term, schema)?;
        if columns.is_empty() {
            return None;
        }
        let partition_only = columns
            .iter()
            .all(|column| partition_columns.contains(column));
        let data_only = columns
            .iter()
            .all(|column| !partition_columns.contains(column));

        if partition_only {
            predicates.push(exact_partition_predicate(term, schema)?);
        } else if data_only {
            if let Some(predicate) = data_stats_predicate(term, schema) {
                predicates.push(predicate);
                residual_term_count = residual_term_count.saturating_add(1);
            } else if is_safe_data_residual_term(term, schema) {
                residual_term_count = residual_term_count.saturating_add(1);
            } else {
                return None;
            }
        } else {
            return None;
        }
    }

    if predicates.is_empty() || residual_term_count == 0 {
        return None;
    }
    and_predicates(predicates)
}

fn collect_top_level_and_terms<'a>(filter: &'a Expr, terms: &mut Vec<&'a Expr>) {
    match filter {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_top_level_and_terms(&binary.left, terms);
            collect_top_level_and_terms(&binary.right, terms);
        }
        filter => terms.push(filter),
    }
}

fn is_safe_data_residual_term(term: &Expr, schema: &Schema) -> bool {
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

fn is_filter_candidate(filter: &Expr) -> bool {
    match filter {
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
            is_filter_candidate(&binary.left) && is_filter_candidate(&binary.right)
        }
        Expr::BinaryExpr(binary) if comparison(binary.op).is_some() => {
            is_column_or_literal(&binary.left) && is_column_or_literal(&binary.right)
        }
        Expr::Not(inner) => is_filter_candidate(inner),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => is_column_or_literal(inner),
        Expr::Between(between) => {
            is_column_or_literal(&between.expr)
                && is_column_or_literal(&between.low)
                && is_column_or_literal(&between.high)
        }
        Expr::InList(in_list) => {
            is_column_or_literal(&in_list.expr) && in_list.list.iter().all(is_column_or_literal)
        }
        Expr::Column(_) => true,
        _ => false,
    }
}

fn is_column_or_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_) | Expr::Literal(_, _))
}

fn is_boolean_column(column: &datafusion::common::Column, schema: &Schema) -> bool {
    column_name(column).is_some_and(|name| {
        schema
            .field_with_name(&name)
            .is_ok_and(|field| matches!(field.data_type(), arrow::datatypes::DataType::Boolean))
    })
}

fn exact_partition_predicate(filter: &Expr, schema: &Schema) -> Option<DeltaPredicate> {
    if !is_exact_partition_filter(filter, schema) {
        return None;
    }
    validated_predicate(filter, schema)
}

fn is_exact_partition_filter(filter: &Expr, schema: &Schema) -> bool {
    match filter {
        Expr::Column(column) => {
            partition_column_supports(column, schema, PartitionOperatorFamily::BooleanShorthand)
        }
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
            is_exact_partition_filter(&binary.left, schema)
                && is_exact_partition_filter(&binary.right, schema)
        }
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) =>
        {
            let family = if matches!(binary.op, Operator::Eq | Operator::NotEq) {
                PartitionOperatorFamily::Equality
            } else {
                PartitionOperatorFamily::Ordering
            };
            partition_column_literal(&binary.left, &binary.right, schema, family)
                || partition_column_literal(&binary.right, &binary.left, schema, family)
        }
        Expr::InList(in_list) if in_list.list.is_empty() => {
            matches!(in_list.expr.as_ref(), Expr::Column(column) if column_name(column).is_some_and(|name| {
                schema.field_with_name(&name).is_ok_and(|field| {
                    matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8)
                })
            }))
        }
        Expr::InList(in_list) => {
            let Expr::Column(column) = in_list.expr.as_ref() else {
                return false;
            };
            partition_column_supports(column, schema, PartitionOperatorFamily::Membership)
                && in_list.list.iter().all(|literal| {
                    partition_literal_matches(
                        column,
                        literal,
                        schema,
                        PartitionOperatorFamily::Membership,
                    )
                })
        }
        Expr::Between(between) => {
            let Expr::Column(column) = between.expr.as_ref() else {
                return false;
            };
            partition_literal_matches(
                column,
                &between.low,
                schema,
                PartitionOperatorFamily::Between,
            ) && partition_literal_matches(
                column,
                &between.high,
                schema,
                PartitionOperatorFamily::Between,
            )
        }
        Expr::Not(inner) => is_exact_partition_filter(inner, schema),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            matches!(inner.as_ref(), Expr::Column(column) if partition_column_supports(
                column,
                schema,
                PartitionOperatorFamily::NullCheck,
            ))
        }
        _ => false,
    }
}

#[derive(Clone, Copy)]
enum PartitionOperatorFamily {
    Equality,
    Ordering,
    Membership,
    Between,
    NullCheck,
    BooleanShorthand,
}

fn partition_column_literal(
    column: &Expr,
    literal: &Expr,
    schema: &Schema,
    family: PartitionOperatorFamily,
) -> bool {
    matches!(column, Expr::Column(column) if partition_literal_matches(
        column, literal, schema, family
    ))
}

fn partition_literal_matches(
    column: &Column,
    literal: &Expr,
    schema: &Schema,
    family: PartitionOperatorFamily,
) -> bool {
    if !partition_column_supports(column, schema, family) {
        return false;
    }
    let Some(name) = column_name(column) else {
        return false;
    };
    let Ok(field) = schema.field_with_name(&name) else {
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
            Expr::Literal(ScalarValue::Decimal128(Some(_), other_precision, other_scale), _),
        ) => *scale >= 0 && precision == other_precision && scale == other_scale,
        (
            DataType::Binary | DataType::LargeBinary,
            Expr::Literal(
                ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value)),
                _,
            ),
        ) => !value.is_empty(),
        (
            DataType::FixedSizeBinary(size),
            Expr::Literal(ScalarValue::FixedSizeBinary(other_size, Some(value)), _),
        ) => {
            size == other_size
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

fn partition_column_supports(
    column: &Column,
    schema: &Schema,
    family: PartitionOperatorFamily,
) -> bool {
    let Some(name) = column_name(column) else {
        return false;
    };
    schema
        .field_with_name(&name)
        .is_ok_and(|field| partition_type_supports(field.data_type(), family))
}

fn partition_type_supports(data_type: &DataType, family: PartitionOperatorFamily) -> bool {
    use PartitionOperatorFamily::{
        Between, BooleanShorthand, Equality, Membership, NullCheck, Ordering,
    };

    match data_type {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Date32
        | DataType::Decimal128(_, _)
        | DataType::Timestamp(TimeUnit::Microsecond, _) => {
            matches!(
                family,
                Equality | Ordering | Membership | Between | NullCheck
            )
        }
        DataType::Float32
        | DataType::Float64
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_) => {
            matches!(family, Equality | Membership | NullCheck)
        }
        DataType::Boolean => {
            matches!(family, Equality | Membership | NullCheck | BooleanShorthand)
        }
        _ => false,
    }
}

fn data_stats_predicate(filter: &Expr, schema: &Schema) -> Option<DeltaPredicate> {
    if !is_supported_data_stats_filter(filter, schema) {
        return None;
    }
    validated_predicate(filter, schema)
}

fn is_supported_data_stats_filter(filter: &Expr, schema: &Schema) -> bool {
    match filter {
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) =>
        {
            data_stats_column_literal(&binary.left, binary.op, &binary.right, schema)
                || data_stats_column_literal(&binary.right, binary.op, &binary.left, schema)
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => data_column_type(inner, schema)
            .is_some_and(|data_type| {
                matches!(
                    data_type,
                    DataType::Boolean
                        | DataType::Binary
                        | DataType::LargeBinary
                        | DataType::FixedSizeBinary(_)
                        | DataType::Decimal128(_, _)
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Float32
                        | DataType::Float64
                        | DataType::Date32
                        | DataType::Timestamp(TimeUnit::Microsecond, _)
                )
            }),
        _ => false,
    }
}

fn data_stats_column_literal(column: &Expr, op: Operator, literal: &Expr, schema: &Schema) -> bool {
    let Some(data_type) = data_column_type(column, schema) else {
        return false;
    };
    match (data_type, literal) {
        (DataType::Int8, Expr::Literal(ScalarValue::Int8(Some(_)), _))
        | (DataType::Int16, Expr::Literal(ScalarValue::Int16(Some(_)), _))
        | (DataType::Int32, Expr::Literal(ScalarValue::Int32(Some(_)), _))
        | (DataType::Int64, Expr::Literal(ScalarValue::Int64(Some(_)), _)) => true,
        (
            DataType::Decimal128(precision, scale),
            Expr::Literal(ScalarValue::Decimal128(Some(_), other_precision, other_scale), _),
        ) => *scale >= 0 && precision == other_precision && scale == other_scale,
        (
            DataType::Utf8 | DataType::LargeUtf8,
            Expr::Literal(ScalarValue::Utf8(Some(_)) | ScalarValue::LargeUtf8(Some(_)), _),
        ) => true,
        (DataType::Float32, Expr::Literal(ScalarValue::Float32(Some(value)), _)) => {
            value.is_finite() && *value != 0.0
        }
        (DataType::Float64, Expr::Literal(ScalarValue::Float64(Some(value)), _)) => {
            value.is_finite() && *value != 0.0
        }
        (DataType::Date32, Expr::Literal(ScalarValue::Date32(Some(_)), _)) => true,
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some(field_timezone)),
            Expr::Literal(ScalarValue::TimestampMicrosecond(Some(_), Some(timezone)), _),
        ) => op != Operator::NotEq && !field_timezone.is_empty() && field_timezone == timezone,
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Expr::Literal(ScalarValue::TimestampMicrosecond(Some(_), None), _),
        ) => op != Operator::NotEq,
        _ => false,
    }
}

fn data_column_type<'a>(expr: &Expr, schema: &'a Schema) -> Option<&'a DataType> {
    let Expr::Column(column) = expr else {
        return None;
    };
    let name = column_name(column)?;
    schema
        .field_with_name(&name)
        .ok()
        .map(|field| field.data_type())
}

fn validated_predicate(filter: &Expr, schema: &Schema) -> Option<DeltaPredicate> {
    let mut predicate = translate_expr(filter)?;
    normalize_equivalent_scalars(&mut predicate, schema)?;
    validate_predicate(&predicate, schema).ok()?;
    Some(predicate)
}

fn normalize_equivalent_scalars(predicate: &mut DeltaPredicate, schema: &Schema) -> Option<()> {
    match predicate {
        DeltaPredicate::Compare { column, value, .. } => {
            let data_type = schema.field_with_name(column).ok()?.data_type();
            *value = match (data_type, &*value) {
                (DataType::Utf8, DeltaScalar::LargeUtf8(value)) => DeltaScalar::Utf8(value.clone()),
                (DataType::LargeUtf8, DeltaScalar::Utf8(value)) => {
                    DeltaScalar::LargeUtf8(value.clone())
                }
                (DataType::Binary, DeltaScalar::LargeBinary(value)) => {
                    DeltaScalar::Binary(value.clone())
                }
                (DataType::LargeBinary, DeltaScalar::Binary(value)) => {
                    DeltaScalar::LargeBinary(value.clone())
                }
                (
                    DataType::Timestamp(TimeUnit::Microsecond, Some(field_timezone)),
                    DeltaScalar::TimestampMicrosecond {
                        value,
                        timezone: Some(_),
                    },
                ) => DeltaScalar::TimestampMicrosecond {
                    value: *value,
                    timezone: Some(field_timezone.to_string()),
                },
                _ => value.clone(),
            };
        }
        DeltaPredicate::And(children) | DeltaPredicate::Or(children) => {
            for child in children {
                normalize_equivalent_scalars(child, schema)?;
            }
        }
        DeltaPredicate::Not(child) => normalize_equivalent_scalars(child, schema)?,
        DeltaPredicate::Boolean(_)
        | DeltaPredicate::IsNull { .. }
        | DeltaPredicate::IsNotNull { .. } => {}
    }
    Some(())
}

fn and_predicates(predicates: Vec<DeltaPredicate>) -> Option<DeltaPredicate> {
    match predicates.as_slice() {
        [] => None,
        [predicate] => Some(predicate.clone()),
        _ => Some(DeltaPredicate::And(predicates)),
    }
}

fn translate_expr(expr: &Expr) -> Option<DeltaPredicate> {
    match unalias(expr) {
        Expr::Literal(ScalarValue::Boolean(Some(value)), _) => {
            Some(DeltaPredicate::Boolean(*value))
        }
        Expr::Column(column) => Some(DeltaPredicate::Compare {
            column: column_name(column)?,
            op: DeltaComparison::Eq,
            value: DeltaScalar::Boolean(true),
        }),
        Expr::Not(child) => translate_expr(child).map(Box::new).map(DeltaPredicate::Not),
        Expr::IsNull(child) => Some(DeltaPredicate::IsNull {
            column: expr_column_name(child)?,
        }),
        Expr::IsNotNull(child) => Some(DeltaPredicate::IsNotNull {
            column: expr_column_name(child)?,
        }),
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::And | Operator::Or => {
                let children = vec![
                    translate_expr(&binary.left)?,
                    translate_expr(&binary.right)?,
                ];
                Some(if binary.op == Operator::And {
                    DeltaPredicate::And(children)
                } else {
                    DeltaPredicate::Or(children)
                })
            }
            op => translate_comparison(&binary.left, op, &binary.right),
        },
        Expr::Between(between) => {
            let column = expr_column_name(&between.expr)?;
            let low = scalar_value(expr_literal(&between.low)?)?;
            let high = scalar_value(expr_literal(&between.high)?)?;
            let predicate = DeltaPredicate::And(vec![
                DeltaPredicate::Compare {
                    column: column.clone(),
                    op: DeltaComparison::GtEq,
                    value: low,
                },
                DeltaPredicate::Compare {
                    column,
                    op: DeltaComparison::LtEq,
                    value: high,
                },
            ]);
            Some(if between.negated {
                DeltaPredicate::Not(Box::new(predicate))
            } else {
                predicate
            })
        }
        Expr::InList(in_list) => {
            let column = expr_column_name(&in_list.expr)?;
            let values = in_list
                .list
                .iter()
                .map(|item| scalar_value(expr_literal(item)?))
                .collect::<Option<Vec<_>>>()?;
            let predicate = match values.as_slice() {
                [] => DeltaPredicate::Boolean(false),
                _ => DeltaPredicate::Or(
                    values
                        .into_iter()
                        .map(|value| DeltaPredicate::Compare {
                            column: column.clone(),
                            op: DeltaComparison::Eq,
                            value,
                        })
                        .collect(),
                ),
            };
            Some(if in_list.negated {
                match predicate {
                    DeltaPredicate::Boolean(false) => DeltaPredicate::IsNotNull { column },
                    predicate => DeltaPredicate::Not(Box::new(predicate)),
                }
            } else {
                predicate
            })
        }
        _ => None,
    }
}

fn translate_comparison(left: &Expr, op: Operator, right: &Expr) -> Option<DeltaPredicate> {
    let comparison = comparison(op)?;
    if let (Some(column), Some(value)) = (
        expr_column_name(left),
        expr_literal(right).and_then(scalar_value),
    ) {
        return Some(DeltaPredicate::Compare {
            column,
            op: comparison,
            value,
        });
    }
    let column = expr_column_name(right)?;
    let value = scalar_value(expr_literal(left)?)?;
    Some(DeltaPredicate::Compare {
        column,
        op: reverse_comparison(comparison),
        value,
    })
}

fn comparison(op: Operator) -> Option<DeltaComparison> {
    match op {
        Operator::Eq => Some(DeltaComparison::Eq),
        Operator::NotEq => Some(DeltaComparison::NotEq),
        Operator::Lt => Some(DeltaComparison::Lt),
        Operator::LtEq => Some(DeltaComparison::LtEq),
        Operator::Gt => Some(DeltaComparison::Gt),
        Operator::GtEq => Some(DeltaComparison::GtEq),
        _ => None,
    }
}

const fn reverse_comparison(op: DeltaComparison) -> DeltaComparison {
    match op {
        DeltaComparison::Eq => DeltaComparison::Eq,
        DeltaComparison::NotEq => DeltaComparison::NotEq,
        DeltaComparison::Lt => DeltaComparison::Gt,
        DeltaComparison::LtEq => DeltaComparison::GtEq,
        DeltaComparison::Gt => DeltaComparison::Lt,
        DeltaComparison::GtEq => DeltaComparison::LtEq,
    }
}

fn unalias(mut expr: &Expr) -> &Expr {
    while let Expr::Alias(alias) = expr {
        expr = &alias.expr;
    }
    expr
}

fn expr_column_name(expr: &Expr) -> Option<String> {
    match unalias(expr) {
        Expr::Column(column) => column_name(column),
        _ => None,
    }
}

fn column_name(column: &datafusion::common::Column) -> Option<String> {
    (column.relation.is_none() && !column.name.contains('.')).then(|| column.name.clone())
}

fn expr_literal(expr: &Expr) -> Option<&ScalarValue> {
    match unalias(expr) {
        Expr::Literal(value, _) => Some(value),
        _ => None,
    }
}

fn scalar_value(value: &ScalarValue) -> Option<DeltaScalar> {
    match value {
        ScalarValue::Boolean(Some(value)) => Some(DeltaScalar::Boolean(*value)),
        ScalarValue::Int8(Some(value)) => Some(DeltaScalar::Int8(*value)),
        ScalarValue::Int16(Some(value)) => Some(DeltaScalar::Int16(*value)),
        ScalarValue::Int32(Some(value)) => Some(DeltaScalar::Int32(*value)),
        ScalarValue::Int64(Some(value)) => Some(DeltaScalar::Int64(*value)),
        ScalarValue::Float32(Some(value)) if value.is_finite() => {
            Some(DeltaScalar::Float32(*value))
        }
        ScalarValue::Float64(Some(value)) if value.is_finite() => {
            Some(DeltaScalar::Float64(*value))
        }
        ScalarValue::Date32(Some(value)) => Some(DeltaScalar::Date32(*value)),
        ScalarValue::Decimal128(Some(value), precision, scale) => Some(DeltaScalar::Decimal128 {
            value: *value,
            precision: *precision,
            scale: *scale,
        }),
        ScalarValue::Utf8(Some(value)) => Some(DeltaScalar::Utf8(value.clone())),
        ScalarValue::LargeUtf8(Some(value)) => Some(DeltaScalar::LargeUtf8(value.clone())),
        ScalarValue::Binary(Some(value)) => Some(DeltaScalar::Binary(value.clone())),
        ScalarValue::LargeBinary(Some(value)) => Some(DeltaScalar::LargeBinary(value.clone())),
        ScalarValue::FixedSizeBinary(size, Some(value)) => Some(DeltaScalar::FixedSizeBinary {
            size: *size,
            value: value.clone(),
        }),
        ScalarValue::TimestampMicrosecond(Some(value), timezone) => {
            Some(DeltaScalar::TimestampMicrosecond {
                value: *value,
                timezone: timezone.as_ref().map(ToString::to_string),
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };

    use arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use datafusion::{
        common::{Column, ScalarValue},
        logical_expr::{Expr, TableProviderFilterPushDown, cast, col, lit},
    };

    use super::*;
    use crate::predicate::evaluate_predicate;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("flag", DataType::Boolean, true),
                Field::new("i8", DataType::Int8, true),
                Field::new("i16", DataType::Int16, true),
                Field::new("i32", DataType::Int32, true),
                Field::new("i64", DataType::Int64, true),
                Field::new("f32", DataType::Float32, true),
                Field::new("f64", DataType::Float64, true),
                Field::new("date", DataType::Date32, true),
                Field::new("decimal", DataType::Decimal128(10, 2), true),
                Field::new("text", DataType::Utf8, true),
                Field::new("large_text", DataType::LargeUtf8, true),
                Field::new("bytes", DataType::Binary, true),
                Field::new("large_bytes", DataType::LargeBinary, true),
                Field::new("fixed", DataType::FixedSizeBinary(2), true),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                Field::new(
                    "ts_ntz",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                Field::new("u32", DataType::UInt32, true),
                Field::new("decimal256", DataType::Decimal256(10, 2), true),
                Field::new("negative_decimal", DataType::Decimal128(10, -2), true),
            ],
            HashMap::from([("owner".to_owned(), "reader".to_owned())]),
        ))
    }

    fn scalar(value: ScalarValue) -> Expr {
        Expr::Literal(value, None)
    }

    fn partition_columns(names: &[&str]) -> HashSet<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    fn all_partition_columns(schema: &Schema) -> HashSet<String> {
        schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    fn plan_scan(
        schema: &SchemaRef,
        partitions: &HashSet<String>,
        projection: Option<&[usize]>,
        filters: &[&Expr],
        exact_predicate_evaluation: bool,
    ) -> DataFusionScanPlanning {
        plan_datafusion_scan(
            schema,
            partitions,
            projection,
            filters,
            DataFusionFilterCapabilities {
                exact_predicate_evaluation,
            },
        )
        .expect("DataFusion scan should plan")
    }

    fn pushdowns(plan: &DataFusionScanPlanning) -> Vec<TableProviderFilterPushDown> {
        plan.filters
            .decisions
            .iter()
            .map(|decision| decision.pushdown.clone())
            .collect()
    }

    #[test]
    fn projection_preserves_order_empty_and_metadata() {
        let schema = schema();
        let plan = plan_scan(&schema, &HashSet::new(), Some(&[3, 1]), &[], false);
        assert_eq!(
            plan.projection
                .output_schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["i32", "i8"]
        );
        assert_eq!(
            plan.projection.physical_projection.as_deref(),
            Some(["i32".to_owned(), "i8".to_owned()].as_slice())
        );
        assert_eq!(
            plan.projection.output_projection.as_deref(),
            Some([0, 1].as_slice())
        );
        assert_eq!(
            plan.projection.output_schema.metadata().get("owner"),
            Some(&"reader".to_owned())
        );

        let empty = plan_scan(&schema, &HashSet::new(), Some(&[]), &[], false);
        assert!(empty.projection.output_schema.fields().is_empty());
        assert_eq!(empty.projection.physical_projection, Some(Vec::new()));
        assert_eq!(empty.projection.output_projection, Some(Vec::new()));

        let full = plan_scan(&schema, &HashSet::new(), None, &[], false);
        assert_eq!(full.projection.output_schema, schema);
        assert!(full.projection.physical_projection.is_none());
        assert!(full.projection.output_projection.is_none());
    }

    #[test]
    fn projection_rejects_duplicates_invalid_and_hostile_indexes_first() {
        let schema = schema();
        let duplicate = plan_datafusion_scan(
            &schema,
            &HashSet::new(),
            Some(&[1, 1]),
            &[&col("missing").eq(lit(1_i32))],
            Default::default(),
        )
        .err()
        .expect("duplicate projection should fail");
        assert_eq!(
            duplicate.to_string(),
            "delta reader error: phase=scan_planning error=invalid_projection reason=duplicate_projection_index"
        );

        for index in [schema.fields().len(), usize::MAX] {
            let error = plan_datafusion_scan(
                &schema,
                &HashSet::new(),
                Some(&[index]),
                &[],
                Default::default(),
            )
            .err()
            .expect("invalid projection should fail");
            assert_eq!(
                error.to_string(),
                "delta reader error: phase=scan_planning error=invalid_projection reason=projection_index_out_of_bounds"
            );
        }
    }

    #[test]
    fn projection_adds_only_accepted_filter_columns_as_hidden_inputs() {
        let schema = schema();
        let partitions = partition_columns(&["text"]);
        let partition_filter = col("text").eq(lit("west"));
        let data_stats_filter = col("i64").gt(lit(1_i64));
        let unsupported = col("flag").eq(lit(true));
        let plan = plan_scan(
            &schema,
            &partitions,
            Some(&[3]),
            &[&partition_filter, &data_stats_filter, &unsupported],
            true,
        );

        assert_eq!(
            pushdowns(&plan),
            [
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported,
            ]
        );
        assert_eq!(
            plan.projection.physical_projection.as_deref(),
            Some(["i32".to_owned()].as_slice())
        );
        assert_eq!(plan.projection.hidden_columns, ["text", "i64"]);
        assert_eq!(
            plan.filters.referenced_columns,
            ["text".to_owned(), "i64".to_owned()]
        );
    }

    #[test]
    fn filters_preserve_input_order_duplicates_and_empty_input() {
        let schema = schema();
        let supported = col("i32").eq(lit(1_i32));
        let unsupported = col("missing").eq(lit(1_i32));
        let plan = plan_scan(
            &schema,
            &HashSet::new(),
            None,
            &[&supported, &unsupported, &supported],
            false,
        );
        assert_eq!(
            pushdowns(&plan),
            [
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Inexact,
            ]
        );
        let expected = DeltaPredicate::Compare {
            column: "i32".to_owned(),
            op: DeltaComparison::Eq,
            value: DeltaScalar::Int32(1),
        };
        assert_eq!(
            plan.filters.predicate,
            Some(DeltaPredicate::And(vec![expected.clone(), expected]))
        );
        assert!(plan.filters.row_predicate.is_none());
        assert!(plan.filters.has_unresolved_predicate);

        let empty = plan_scan(&schema, &HashSet::new(), None, &[], false);
        assert!(empty.filters.decisions.is_empty());
        assert!(empty.filters.predicate.is_none());
        assert!(empty.filters.row_predicate.is_none());
        assert!(empty.filters.referenced_columns.is_empty());
        assert!(!empty.filters.has_unresolved_predicate);
    }

    #[test]
    fn qualifier_normalization_matches_the_frozen_provider_boundary() {
        let schema = schema();
        let partitions = partition_columns(&["text"]);
        let qualified = Expr::Column(Column::new(Some("orders"), "text")).eq(lit("west"));
        let nested = Expr::Column(Column::from_name("text.child")).eq(lit("west"));
        let unknown = col("missing").eq(lit(1_i32));
        let internal = col("__delta_funnel_row_index").eq(lit(1_i32));
        let plan = plan_scan(
            &schema,
            &partitions,
            None,
            &[&qualified, &nested, &unknown, &internal],
            false,
        );

        assert_eq!(
            pushdowns(&plan),
            [
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
            ]
        );
        assert_eq!(
            plan.filters.decisions[0].predicate,
            Some(DeltaPredicate::Compare {
                column: "text".to_owned(),
                op: DeltaComparison::Eq,
                value: DeltaScalar::Utf8("west".to_owned()),
            })
        );
    }

    #[test]
    fn partition_policy_accepts_the_frozen_type_operator_matrix() {
        let schema = schema();
        let partitions = all_partition_columns(&schema);
        let filters = vec![
            col("flag"),
            Expr::Not(Box::new(col("flag"))),
            col("flag").eq(lit(true)),
            col("flag").in_list(vec![lit(true), lit(false)], false),
            col("flag").is_not_null(),
            col("flag").eq(lit(true)).and(col("flag").is_not_null()),
            col("text").eq(lit("a")),
            col("text").lt(lit("b")),
            col("text").in_list(vec![lit("a"), lit("b")], false),
            col("text").between(lit("a"), lit("z")),
            col("text").is_null(),
            col("large_text").not_eq(scalar(ScalarValue::Utf8(Some("a".to_owned())))),
            col("i8").eq(scalar(ScalarValue::Int8(Some(1)))),
            col("i16").lt(scalar(ScalarValue::Int16(Some(1)))),
            col("i32").gt_eq(lit(1_i32)),
            col("i32").in_list(vec![lit(1_i32), lit(2_i32)], true),
            col("i32").between(lit(1_i32), lit(2_i32)),
            col("i32").is_null(),
            lit(1_i64).lt(col("i64")),
            col("date").eq(scalar(ScalarValue::Date32(Some(1)))),
            col("date").in_list(vec![scalar(ScalarValue::Date32(Some(1)))], false),
            col("date").not_between(
                scalar(ScalarValue::Date32(Some(1))),
                scalar(ScalarValue::Date32(Some(2))),
            ),
            col("date").is_null(),
            col("decimal").eq(scalar(ScalarValue::Decimal128(Some(1), 10, 2))),
            col("decimal").lt(scalar(ScalarValue::Decimal128(Some(1), 10, 2))),
            col("decimal").in_list(vec![scalar(ScalarValue::Decimal128(Some(1), 10, 2))], false),
            col("decimal").between(
                scalar(ScalarValue::Decimal128(Some(1), 10, 2)),
                scalar(ScalarValue::Decimal128(Some(2), 10, 2)),
            ),
            col("decimal").is_not_null(),
            col("f32").eq(scalar(ScalarValue::Float32(Some(1.5)))),
            col("f64").in_list(vec![scalar(ScalarValue::Float64(Some(1.5)))], true),
            col("f32").is_null(),
            col("bytes").eq(scalar(ScalarValue::LargeBinary(Some(vec![1])))),
            col("large_bytes").in_list(vec![scalar(ScalarValue::Binary(Some(vec![1])))], false),
            col("fixed").eq(scalar(ScalarValue::FixedSizeBinary(2, Some(vec![1, 2])))),
            col("bytes").is_not_null(),
            col("ts").eq(scalar(ScalarValue::TimestampMicrosecond(
                Some(1),
                Some("America/Phoenix".into()),
            ))),
            col("ts").in_list(
                vec![scalar(ScalarValue::TimestampMicrosecond(
                    Some(1),
                    Some("UTC".into()),
                ))],
                false,
            ),
            col("ts").between(
                scalar(ScalarValue::TimestampMicrosecond(
                    Some(1),
                    Some("UTC".into()),
                )),
                scalar(ScalarValue::TimestampMicrosecond(
                    Some(2),
                    Some("UTC".into()),
                )),
            ),
            col("ts").is_null(),
            col("ts_ntz").gt(scalar(ScalarValue::TimestampMicrosecond(Some(1), None))),
            col("ts_ntz").in_list(
                vec![scalar(ScalarValue::TimestampMicrosecond(Some(1), None))],
                true,
            ),
            col("ts_ntz").between(
                scalar(ScalarValue::TimestampMicrosecond(Some(1), None)),
                scalar(ScalarValue::TimestampMicrosecond(Some(2), None)),
            ),
            col("ts_ntz").is_not_null(),
            col("i32").eq(lit(1_i32)).or(col("i32").gt(lit(2_i32))),
            Expr::Not(Box::new(col("i32").eq(lit(1_i32)))),
            col("text").in_list(Vec::new(), false),
            col("text").in_list(Vec::new(), true),
        ];
        let refs = filters.iter().collect::<Vec<_>>();
        let plan = plan_scan(&schema, &partitions, None, &refs, false);

        for (index, pushdown) in pushdowns(&plan).iter().enumerate() {
            assert_eq!(
                *pushdown,
                TableProviderFilterPushDown::Exact,
                "partition filter {index}: {:?}",
                filters[index]
            );
        }
        assert!(!plan.filters.has_unresolved_predicate);
        assert!(plan.filters.row_predicate.is_none());
        assert_eq!(
            plan.filters
                .decisions
                .last()
                .and_then(|decision| decision.predicate.clone()),
            Some(DeltaPredicate::IsNotNull {
                column: "text".to_owned(),
            })
        );
    }

    #[test]
    fn partition_policy_rejects_every_unproven_matrix_entry() {
        let schema = schema();
        let partitions = all_partition_columns(&schema);
        let filters = vec![
            scalar(ScalarValue::Boolean(Some(true))),
            col("i32"),
            col("i32").alias("renamed").eq(lit(1_i32)),
            col("i32").eq(lit(1_i64)),
            col("i32").is_null().alias("renamed"),
            col("i32").in_list(Vec::new(), false),
            col("i32").in_list(Vec::new(), true),
            col("flag").lt(lit(true)),
            col("flag").between(lit(false), lit(true)),
            col("f32").lt(scalar(ScalarValue::Float32(Some(1.0)))),
            col("f32").between(
                scalar(ScalarValue::Float32(Some(1.0))),
                scalar(ScalarValue::Float32(Some(2.0))),
            ),
            col("f32").eq(scalar(ScalarValue::Float32(Some(0.0)))),
            col("f64").eq(scalar(ScalarValue::Float64(Some(f64::NAN)))),
            col("bytes").lt(scalar(ScalarValue::Binary(Some(vec![1])))),
            col("bytes").eq(scalar(ScalarValue::Binary(Some(Vec::new())))),
            col("fixed").eq(scalar(ScalarValue::FixedSizeBinary(3, Some(vec![1, 2, 3])))),
            col("decimal").eq(scalar(ScalarValue::Decimal128(Some(1), 9, 2))),
            col("negative_decimal").eq(scalar(ScalarValue::Decimal128(Some(100), 10, -2))),
            col("text").in_list(vec![scalar(ScalarValue::Utf8(None))], false),
            col("text").in_list(vec![col("large_text")], false),
            col("ts").eq(scalar(ScalarValue::TimestampMicrosecond(Some(1), None))),
            col("ts").eq(scalar(ScalarValue::TimestampSecond(
                Some(1),
                Some("UTC".into()),
            ))),
            col("ts_ntz").eq(scalar(ScalarValue::TimestampMicrosecond(
                Some(1),
                Some("UTC".into()),
            ))),
            col("u32").eq(scalar(ScalarValue::UInt32(Some(1)))),
            col("decimal256").is_null(),
        ];
        let refs = filters.iter().collect::<Vec<_>>();
        let plan = plan_scan(&schema, &partitions, None, &refs, false);

        assert!(
            pushdowns(&plan)
                .iter()
                .all(|pushdown| *pushdown == TableProviderFilterPushDown::Unsupported)
        );
        assert!(plan.filters.predicate.is_none());
    }

    #[test]
    fn data_statistics_policy_accepts_only_the_frozen_matrix() {
        let schema = schema();
        let filters = vec![
            col("i8").eq(scalar(ScalarValue::Int8(Some(1)))),
            col("i16").not_eq(scalar(ScalarValue::Int16(Some(1)))),
            col("i32").eq(lit(1_i32)),
            col("i32").not_eq(lit(1_i32)),
            col("i32").lt(lit(1_i32)),
            col("i32").lt_eq(lit(1_i32)),
            col("i32").gt(lit(1_i32)),
            col("i32").gt_eq(lit(1_i32)),
            lit(1_i64).lt_eq(col("i64")),
            col("flag").is_null(),
            col("bytes").is_not_null(),
            col("large_bytes").is_null(),
            col("fixed").is_not_null(),
            col("decimal").gt(scalar(ScalarValue::Decimal128(Some(1), 10, 2))),
            col("decimal").is_null(),
            col("text").not_eq(scalar(ScalarValue::LargeUtf8(Some("a".to_owned())))),
            col("large_text").lt(scalar(ScalarValue::Utf8(Some("b".to_owned())))),
            col("text").is_not_null(),
            col("f32").gt(scalar(ScalarValue::Float32(Some(1.0)))),
            col("f64").lt_eq(scalar(ScalarValue::Float64(Some(1.0)))),
            col("f64").is_null(),
            col("date").not_eq(scalar(ScalarValue::Date32(Some(1)))),
            col("date").is_not_null(),
            col("ts").gt(scalar(ScalarValue::TimestampMicrosecond(
                Some(1),
                Some("UTC".into()),
            ))),
            col("ts_ntz").eq(scalar(ScalarValue::TimestampMicrosecond(Some(1), None))),
            col("ts").is_null(),
        ];
        let refs = filters.iter().collect::<Vec<_>>();
        let inexact = plan_scan(&schema, &HashSet::new(), None, &refs, false);
        for (index, pushdown) in pushdowns(&inexact).iter().enumerate() {
            assert_eq!(
                *pushdown,
                TableProviderFilterPushDown::Inexact,
                "data-statistics filter {index}: {:?}",
                filters[index]
            );
        }
        assert!(inexact.filters.has_unresolved_predicate);

        let exact = plan_scan(&schema, &HashSet::new(), None, &refs, true);
        assert!(
            pushdowns(&exact)
                .iter()
                .all(|pushdown| *pushdown == TableProviderFilterPushDown::Exact)
        );
        assert!(exact.filters.row_predicate.is_some());
        assert!(!exact.filters.has_unresolved_predicate);
    }

    #[test]
    fn data_statistics_policy_rejects_unproven_shapes_and_values() {
        let schema = schema();
        let filters = vec![
            col("flag").eq(lit(true)),
            col("flag"),
            col("bytes").eq(scalar(ScalarValue::Binary(Some(vec![1])))),
            col("i32").is_null(),
            col("i32").eq(lit(1_i64)),
            col("i32").in_list(vec![lit(1_i32)], false),
            col("i32").between(lit(1_i32), lit(2_i32)),
            col("text").in_list(vec![lit("a")], false),
            col("text").between(lit("a"), lit("z")),
            col("f32").eq(scalar(ScalarValue::Float32(Some(0.0)))),
            col("f64").eq(scalar(ScalarValue::Float64(Some(f64::INFINITY)))),
            col("decimal").eq(scalar(ScalarValue::Decimal128(Some(1), 9, 2))),
            col("negative_decimal").eq(scalar(ScalarValue::Decimal128(Some(100), 10, -2))),
            col("text").eq(lit(1_i32)),
            col("ts").not_eq(scalar(ScalarValue::TimestampMicrosecond(
                Some(1),
                Some("UTC".into()),
            ))),
            col("ts").eq(scalar(ScalarValue::TimestampMicrosecond(
                Some(1),
                Some("Etc/UTC".into()),
            ))),
            col("u32").eq(scalar(ScalarValue::UInt32(Some(1)))),
            col("decimal256").is_null(),
            cast(col("i32"), DataType::Int64).eq(lit(1_i64)),
            (col("i32") + lit(1_i32)).eq(lit(2_i32)),
        ];
        let refs = filters.iter().collect::<Vec<_>>();
        let plan = plan_scan(&schema, &HashSet::new(), None, &refs, true);

        assert!(
            pushdowns(&plan)
                .iter()
                .all(|pushdown| *pushdown == TableProviderFilterPushDown::Unsupported)
        );
        assert!(plan.filters.predicate.is_none());
    }

    #[test]
    fn mixed_and_preserves_safe_pruning_and_full_residual_columns() {
        let schema = schema();
        let partitions = partition_columns(&["text"]);
        let partition = col("text").eq(lit("west"));
        let stats = col("i32").gt(lit(0_i32));
        let safe_residual = col("i64").eq(lit(1_i32));
        let filter = partition
            .clone()
            .and(stats.clone())
            .and(safe_residual.clone());
        let plan = plan_scan(&schema, &partitions, Some(&[9, 3, 4]), &[&filter], true);

        assert_eq!(
            plan.filters.decisions[0].pushdown,
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            plan.filters.decisions[0].predicate,
            Some(DeltaPredicate::And(vec![
                DeltaPredicate::Compare {
                    column: "text".to_owned(),
                    op: DeltaComparison::Eq,
                    value: DeltaScalar::Utf8("west".to_owned()),
                },
                DeltaPredicate::Compare {
                    column: "i32".to_owned(),
                    op: DeltaComparison::Gt,
                    value: DeltaScalar::Int32(0),
                },
            ]))
        );
        assert_eq!(
            plan.filters.referenced_columns,
            ["i32".to_owned(), "i64".to_owned(), "text".to_owned()]
        );
        assert!(plan.projection.hidden_columns.is_empty());
        assert!(plan.filters.has_unresolved_predicate);
    }

    #[test]
    fn inexact_filters_require_every_residual_column_in_the_output_projection() {
        let schema = schema();
        let partitions = partition_columns(&["text"]);
        let filter = col("text").eq(lit("west")).and(col("i32").gt(lit(0_i32)));
        let error = plan_datafusion_scan(
            &schema,
            &partitions,
            Some(&[0]),
            &[&filter],
            DataFusionFilterCapabilities {
                exact_predicate_evaluation: true,
            },
        )
        .err()
        .expect("missing residual columns should fail");

        assert_eq!(
            error.to_string(),
            "delta reader error: phase=scan_planning error=unsupported_predicate reason=inexact_filter_columns_not_projected"
        );
    }

    #[test]
    fn mixed_and_never_extracts_through_unsafe_or_not_shapes() {
        let schema = schema();
        let partitions = partition_columns(&["text"]);
        let partition = col("text").eq(lit("west"));
        let safe_data = col("i64").eq(lit(1_i32));
        let arithmetic = (col("i64") + lit(1_i64)).eq(lit(2_i64));
        let unsupported_partition = col("text").eq(lit(1_i32));
        let filters = [
            partition.clone().and(arithmetic),
            partition.clone().or(safe_data.clone()),
            Expr::Not(Box::new(partition.clone().and(safe_data))),
            partition.and(unsupported_partition),
        ];
        let refs = filters.iter().collect::<Vec<_>>();
        let plan = plan_scan(&schema, &partitions, None, &refs, true);

        assert!(
            pushdowns(&plan)
                .iter()
                .all(|pushdown| *pushdown == TableProviderFilterPushDown::Unsupported)
        );
        assert!(plan.filters.predicate.is_none());
    }

    #[test]
    fn every_reversed_comparison_uses_the_inverse_operator() {
        let schema = schema();
        let partitions = partition_columns(&["i32"]);
        let filters = [
            lit(1_i32).eq(col("i32")),
            lit(1_i32).not_eq(col("i32")),
            lit(1_i32).lt(col("i32")),
            lit(1_i32).lt_eq(col("i32")),
            lit(1_i32).gt(col("i32")),
            lit(1_i32).gt_eq(col("i32")),
        ];
        let expected = [
            DeltaComparison::Eq,
            DeltaComparison::NotEq,
            DeltaComparison::Gt,
            DeltaComparison::GtEq,
            DeltaComparison::Lt,
            DeltaComparison::LtEq,
        ];

        for (filter, expected) in filters.iter().zip(expected) {
            let plan = plan_scan(&schema, &partitions, None, &[filter], false);
            assert!(matches!(
                plan.filters.decisions[0].predicate,
                Some(DeltaPredicate::Compare { op, .. }) if op == expected
            ));
        }
    }

    #[test]
    fn partition_between_and_in_rewrites_preserve_null_truth() {
        let batch_schema = Arc::new(Schema::new(vec![Field::new("i32", DataType::Int32, true)]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(1), Some(2)]));
        let batch = RecordBatch::try_new(Arc::clone(&batch_schema), vec![values])
            .expect("batch should build");
        let filters = [
            col("i32").between(lit(1_i32), lit(1_i32)),
            col("i32").not_between(lit(1_i32), lit(1_i32)),
            col("i32").in_list(vec![lit(1_i32)], false),
            col("i32").in_list(vec![lit(1_i32)], true),
        ];
        let expected_rows = [1, 1, 1, 1];
        for (filter, expected_rows) in filters.iter().zip(expected_rows) {
            let predicate = exact_partition_predicate(filter, batch_schema.as_ref())
                .expect("partition filter should translate");
            assert_eq!(
                evaluate_predicate(&batch, &predicate)
                    .expect("predicate should evaluate")
                    .num_rows(),
                expected_rows
            );
        }

        let string_schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let empty = col("text").in_list(Vec::new(), false);
        let not_empty = col("text").in_list(Vec::new(), true);
        assert_eq!(
            exact_partition_predicate(&empty, string_schema.as_ref()),
            Some(DeltaPredicate::Boolean(false))
        );
        assert_eq!(
            exact_partition_predicate(&not_empty, string_schema.as_ref()),
            Some(DeltaPredicate::IsNotNull {
                column: "text".to_owned(),
            })
        );
    }

    #[test]
    fn unsupported_scalar_families_never_enter_core_predicates() {
        let unsupported = [
            ScalarValue::Null,
            ScalarValue::Float16(None),
            ScalarValue::Decimal32(None, 1, 0),
            ScalarValue::Decimal64(None, 1, 0),
            ScalarValue::Decimal256(None, 1, 0),
            ScalarValue::UInt8(Some(1)),
            ScalarValue::UInt16(Some(1)),
            ScalarValue::UInt32(Some(1)),
            ScalarValue::UInt64(Some(1)),
            ScalarValue::Utf8View(Some("value".to_owned())),
            ScalarValue::BinaryView(Some(vec![1])),
            ScalarValue::Date64(Some(1)),
            ScalarValue::Time32Second(Some(1)),
            ScalarValue::Time32Millisecond(Some(1)),
            ScalarValue::Time64Microsecond(Some(1)),
            ScalarValue::Time64Nanosecond(Some(1)),
            ScalarValue::TimestampSecond(Some(1), None),
            ScalarValue::TimestampMillisecond(Some(1), None),
            ScalarValue::TimestampNanosecond(Some(1), None),
            ScalarValue::IntervalYearMonth(None),
            ScalarValue::IntervalDayTime(None),
            ScalarValue::IntervalMonthDayNano(None),
            ScalarValue::DurationSecond(Some(1)),
            ScalarValue::DurationMillisecond(Some(1)),
            ScalarValue::DurationMicrosecond(Some(1)),
            ScalarValue::DurationNanosecond(Some(1)),
        ];

        for value in unsupported {
            assert!(
                scalar_value(&value).is_none(),
                "unexpected scalar: {value:?}"
            );
        }
    }

    #[test]
    fn static_planner_has_no_provider_runtime_statistics_limit_or_read_path() {
        let production = include_str!("datafusion_planning.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production source should precede tests");
        for forbidden in [
            "impl TableProvider",
            "ExecutionPlan",
            "DynamicFilterPhysicalExpr",
            "Statistics",
            "ParquetRecordBatch",
            "object_store",
            "limit:",
        ] {
            assert!(
                !production.contains(forbidden),
                "static planner contains forbidden runtime surface: {forbidden}"
            );
        }
        assert!(!production.contains("tracing::debug!(?"));
    }
}
