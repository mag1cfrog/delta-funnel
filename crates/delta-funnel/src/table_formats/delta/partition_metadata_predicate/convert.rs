use std::collections::HashSet;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{Expr, Operator};
use snafu::Snafu;

use super::expr::{PartitionComparisonOperator, PartitionMetadataExpr};
use super::names::{DeltaPartitionNameMap, PhysicalPartitionColumn};
use super::supports_partition_metadata_logical_type;
use super::value::{PartitionMetadataValueKind, PartitionScalar};

/// Typed rejection from the provider-owned partition metadata evaluator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum DeltaPartitionMetadataPredicateError {
    #[snafu(display("unsupported DataFusion expression for partition metadata evaluation"))]
    UnsupportedExpression,
    #[snafu(display("unsupported DataFusion operator for partition metadata evaluation"))]
    UnsupportedOperator,
    #[snafu(display("unsupported DataFusion column reference for partition metadata evaluation"))]
    UnsupportedColumnReference,
    #[snafu(display("unsupported DataFusion literal for partition metadata evaluation"))]
    UnsupportedLiteral,
    #[snafu(display("unsupported partition column type for partition metadata evaluation"))]
    UnsupportedColumnType,
    #[snafu(display("DataFusion column is not a Delta partition column"))]
    NonPartitionColumn,
    #[snafu(display("Delta partition physical name is missing"))]
    MissingPhysicalName,
}

pub(super) fn convert_expr(
    expr: &Expr,
    logical_schema: &SchemaRef,
    partition_columns: &HashSet<String>,
    physical_name_lookup: &DeltaPartitionNameMap,
) -> Result<PartitionMetadataExpr, DeltaPartitionMetadataPredicateError> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => Ok(PartitionMetadataExpr::And(
            Box::new(convert_expr(
                binary.left.as_ref(),
                logical_schema,
                partition_columns,
                physical_name_lookup,
            )?),
            Box::new(convert_expr(
                binary.right.as_ref(),
                logical_schema,
                partition_columns,
                physical_name_lookup,
            )?),
        )),
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => Ok(PartitionMetadataExpr::Or(
            Box::new(convert_expr(
                binary.left.as_ref(),
                logical_schema,
                partition_columns,
                physical_name_lookup,
            )?),
            Box::new(convert_expr(
                binary.right.as_ref(),
                logical_schema,
                partition_columns,
                physical_name_lookup,
            )?),
        )),
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => convert_equality(
            binary.left.as_ref(),
            binary.right.as_ref(),
            logical_schema,
            partition_columns,
            physical_name_lookup,
        )
        .or_else(|left_error| {
            convert_equality(
                binary.right.as_ref(),
                binary.left.as_ref(),
                logical_schema,
                partition_columns,
                physical_name_lookup,
            )
            .map_err(|_| left_error)
        }),
        Expr::BinaryExpr(binary) if binary.op == Operator::NotEq => convert_equality(
            binary.left.as_ref(),
            binary.right.as_ref(),
            logical_schema,
            partition_columns,
            physical_name_lookup,
        )
        .or_else(|left_error| {
            convert_equality(
                binary.right.as_ref(),
                binary.left.as_ref(),
                logical_schema,
                partition_columns,
                physical_name_lookup,
            )
            .map_err(|_| left_error)
        })
        .map(|expr| PartitionMetadataExpr::Not(Box::new(expr))),
        Expr::BinaryExpr(binary) if comparison_operator(binary.op).is_some() => {
            let Some(op) = comparison_operator(binary.op) else {
                return Err(DeltaPartitionMetadataPredicateError::UnsupportedOperator);
            };
            convert_comparison(
                binary.left.as_ref(),
                binary.right.as_ref(),
                op,
                logical_schema,
                partition_columns,
                physical_name_lookup,
            )
            .or_else(|left_error| {
                convert_comparison(
                    binary.right.as_ref(),
                    binary.left.as_ref(),
                    op.reverse(),
                    logical_schema,
                    partition_columns,
                    physical_name_lookup,
                )
                .map_err(|_| left_error)
            })
        }
        Expr::BinaryExpr(_) => Err(DeltaPartitionMetadataPredicateError::UnsupportedOperator),
        Expr::InList(in_list) => convert_in_list(
            in_list.expr.as_ref(),
            &in_list.list,
            in_list.negated,
            logical_schema,
            partition_columns,
            physical_name_lookup,
        ),
        Expr::Between(between) => convert_between(
            between.expr.as_ref(),
            between.low.as_ref(),
            between.high.as_ref(),
            between.negated,
            logical_schema,
            partition_columns,
            physical_name_lookup,
        ),
        Expr::IsNull(inner) => Ok(PartitionMetadataExpr::IsNull(convert_column(
            inner.as_ref(),
            logical_schema,
            partition_columns,
            physical_name_lookup,
        )?)),
        Expr::IsNotNull(inner) => Ok(PartitionMetadataExpr::IsNotNull(convert_column(
            inner.as_ref(),
            logical_schema,
            partition_columns,
            physical_name_lookup,
        )?)),
        Expr::Not(inner) => Ok(PartitionMetadataExpr::Not(Box::new(convert_expr(
            inner.as_ref(),
            logical_schema,
            partition_columns,
            physical_name_lookup,
        )?))),
        _ => Err(DeltaPartitionMetadataPredicateError::UnsupportedExpression),
    }
}

fn convert_between(
    column: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    logical_schema: &SchemaRef,
    partition_columns: &HashSet<String>,
    physical_name_lookup: &DeltaPartitionNameMap,
) -> Result<PartitionMetadataExpr, DeltaPartitionMetadataPredicateError> {
    // BETWEEN is inclusive. Expressing it through the same comparison nodes
    // keeps ordering and SQL null behavior in one place.
    let lower_bound = convert_comparison(
        column,
        low,
        PartitionComparisonOperator::GtEq,
        logical_schema,
        partition_columns,
        physical_name_lookup,
    )?;
    let upper_bound = convert_comparison(
        column,
        high,
        PartitionComparisonOperator::LtEq,
        logical_schema,
        partition_columns,
        physical_name_lookup,
    )?;
    let between = PartitionMetadataExpr::And(Box::new(lower_bound), Box::new(upper_bound));

    if negated {
        Ok(PartitionMetadataExpr::Not(Box::new(between)))
    } else {
        Ok(between)
    }
}

fn comparison_operator(op: Operator) -> Option<PartitionComparisonOperator> {
    match op {
        Operator::Lt => Some(PartitionComparisonOperator::Lt),
        Operator::LtEq => Some(PartitionComparisonOperator::LtEq),
        Operator::Gt => Some(PartitionComparisonOperator::Gt),
        Operator::GtEq => Some(PartitionComparisonOperator::GtEq),
        _ => None,
    }
}

fn convert_in_list(
    column: &Expr,
    literals: &[Expr],
    negated: bool,
    logical_schema: &SchemaRef,
    partition_columns: &HashSet<String>,
    physical_name_lookup: &DeltaPartitionNameMap,
) -> Result<PartitionMetadataExpr, DeltaPartitionMetadataPredicateError> {
    if literals.is_empty() {
        return Err(DeltaPartitionMetadataPredicateError::UnsupportedExpression);
    }

    let column = convert_column(
        column,
        logical_schema,
        partition_columns,
        physical_name_lookup,
    )?;
    let expr = PartitionMetadataExpr::In {
        column: column.clone(),
        literals: literals
            .iter()
            .map(|literal| convert_partition_literal(literal, column.value_kind()))
            .collect::<Result<HashSet<_>, _>>()?,
    };

    if negated {
        Ok(PartitionMetadataExpr::Not(Box::new(expr)))
    } else {
        Ok(expr)
    }
}

fn convert_comparison(
    column: &Expr,
    literal: &Expr,
    op: PartitionComparisonOperator,
    logical_schema: &SchemaRef,
    partition_columns: &HashSet<String>,
    physical_name_lookup: &DeltaPartitionNameMap,
) -> Result<PartitionMetadataExpr, DeltaPartitionMetadataPredicateError> {
    let column = convert_column(
        column,
        logical_schema,
        partition_columns,
        physical_name_lookup,
    )?;
    Ok(PartitionMetadataExpr::Compare {
        literal: convert_partition_literal(literal, column.value_kind())?,
        column,
        op,
    })
}

fn convert_equality(
    column: &Expr,
    literal: &Expr,
    logical_schema: &SchemaRef,
    partition_columns: &HashSet<String>,
    physical_name_lookup: &DeltaPartitionNameMap,
) -> Result<PartitionMetadataExpr, DeltaPartitionMetadataPredicateError> {
    let column = convert_column(
        column,
        logical_schema,
        partition_columns,
        physical_name_lookup,
    )?;
    Ok(PartitionMetadataExpr::Eq {
        literal: convert_partition_literal(literal, column.value_kind())?,
        column,
    })
}

fn convert_column(
    expr: &Expr,
    logical_schema: &SchemaRef,
    partition_columns: &HashSet<String>,
    physical_name_lookup: &DeltaPartitionNameMap,
) -> Result<PhysicalPartitionColumn, DeltaPartitionMetadataPredicateError> {
    let Expr::Column(column) = expr else {
        return Err(DeltaPartitionMetadataPredicateError::UnsupportedColumnReference);
    };
    let logical_name = top_level_column_name(column)?;

    if !partition_columns.contains(logical_name) {
        return Err(DeltaPartitionMetadataPredicateError::NonPartitionColumn);
    }

    let field = logical_schema
        .field_with_name(logical_name)
        .map_err(|_| DeltaPartitionMetadataPredicateError::UnsupportedColumnReference)?;
    if !supports_partition_metadata_logical_type(field.data_type()) {
        return Err(DeltaPartitionMetadataPredicateError::UnsupportedColumnType);
    }
    let value_kind = PartitionMetadataValueKind::from_supported_data_type(field.data_type())
        .ok_or(DeltaPartitionMetadataPredicateError::UnsupportedColumnType)?;

    let physical_name = physical_name_lookup
        .physical_name(logical_name)
        .ok_or(DeltaPartitionMetadataPredicateError::MissingPhysicalName)?;

    Ok(PhysicalPartitionColumn::new(
        physical_name.to_owned(),
        value_kind,
    ))
}

fn top_level_column_name(column: &Column) -> Result<&str, DeltaPartitionMetadataPredicateError> {
    if column.relation.is_some() || column.name.contains('.') {
        Err(DeltaPartitionMetadataPredicateError::UnsupportedColumnReference)
    } else {
        Ok(&column.name)
    }
}

fn convert_partition_literal(
    expr: &Expr,
    value_kind: PartitionMetadataValueKind,
) -> Result<PartitionScalar, DeltaPartitionMetadataPredicateError> {
    match value_kind {
        PartitionMetadataValueKind::String => match expr {
            Expr::Literal(ScalarValue::Utf8(Some(value)), _)
            | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _) => {
                Ok(PartitionScalar::String(value.clone()))
            }
            _ => Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral),
        },
        PartitionMetadataValueKind::SignedInteger { min, max } => {
            convert_signed_integer_literal(expr)
                .filter(|value| min <= *value && *value <= max)
                .map(PartitionScalar::SignedInteger)
                .ok_or(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        }
    }
}

fn convert_signed_integer_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int8(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Some(*value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::{col, lit};

    use super::super::expr::SqlBool;
    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("small_id", DataType::Int8, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("day", DataType::LargeUtf8, true),
        ]))
    }

    fn partition_columns(names: &[&str]) -> HashSet<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    fn identity_map(names: &[&str]) -> DeltaPartitionNameMap {
        DeltaPartitionNameMap::identity(&partition_columns(names))
    }

    fn values(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn predicate_expr(
        expr: &Expr,
        partitions: &[&str],
    ) -> Result<PartitionMetadataExpr, DeltaPartitionMetadataPredicateError> {
        let partition_columns = partition_columns(partitions);
        let name_map = DeltaPartitionNameMap::identity(&partition_columns);

        convert_expr(expr, &schema(), &partition_columns, &name_map)
    }

    fn matches_scan_file(
        expr: &PartitionMetadataExpr,
        partition_values: &HashMap<String, String>,
    ) -> bool {
        expr.eval(partition_values) == SqlBool::True
    }

    #[test]
    fn converts_null_checks_with_sql_semantics_for_missing_and_raw_empty_values() {
        let is_null = predicate_expr(&col("region").is_null(), &["region"]).unwrap();
        let is_not_null = predicate_expr(&col("region").is_not_null(), &["region"]).unwrap();
        let normal = values(&[("region", "us-west")]);
        let raw_empty = values(&[("region", "")]);
        let missing = HashMap::new();

        assert!(!matches_scan_file(&is_null, &normal));
        assert!(!matches_scan_file(&is_null, &raw_empty));
        assert!(matches_scan_file(&is_null, &missing));
        assert!(matches_scan_file(&is_not_null, &normal));
        assert!(matches_scan_file(&is_not_null, &raw_empty));
        assert!(!matches_scan_file(&is_not_null, &missing));
    }

    #[test]
    fn converts_integer_null_checks_without_promoting_integer_literals() {
        let is_null = predicate_expr(&col("id").is_null(), &["id"]).unwrap();
        let is_not_null = predicate_expr(&col("id").is_not_null(), &["id"]).unwrap();
        let normal = values(&[("id", "7")]);
        let raw_empty = values(&[("id", "")]);
        let invalid_integer = values(&[("id", "not-an-integer")]);
        let missing = HashMap::new();

        assert!(!matches_scan_file(&is_null, &normal));
        assert!(!matches_scan_file(&is_null, &raw_empty));
        assert!(!matches_scan_file(&is_null, &invalid_integer));
        assert!(matches_scan_file(&is_null, &missing));
        assert!(matches_scan_file(&is_not_null, &normal));
        assert!(matches_scan_file(&is_not_null, &raw_empty));
        assert!(matches_scan_file(&is_not_null, &invalid_integer));
        assert!(!matches_scan_file(&is_not_null, &missing));
    }

    #[test]
    fn converts_integer_equality_and_in_lists_with_typed_width_bounds() {
        let eq = predicate_expr(&col("id").eq(lit(7_i64)), &["id"]).unwrap();
        let reversed = predicate_expr(&lit(7_i64).eq(col("id")), &["id"]).unwrap();
        let not_eq = predicate_expr(&col("id").not_eq(lit(7_i64)), &["id"]).unwrap();
        let in_list = predicate_expr(
            &col("id").in_list(vec![lit(7_i64), lit(-1_i64)], false),
            &["id"],
        )
        .unwrap();
        let not_in = predicate_expr(
            &col("id").in_list(vec![lit(7_i64), lit(-1_i64)], true),
            &["id"],
        )
        .unwrap();
        let seven = values(&[("id", "7")]);
        let negative_one = values(&[("id", "-1")]);
        let raw_empty = values(&[("id", "")]);
        let invalid_integer = values(&[("id", "not-an-integer")]);
        let missing = HashMap::new();

        assert!(matches_scan_file(&eq, &seven));
        assert!(matches_scan_file(&reversed, &seven));
        assert!(!matches_scan_file(&eq, &negative_one));
        assert!(!matches_scan_file(&eq, &raw_empty));
        assert!(!matches_scan_file(&eq, &invalid_integer));
        assert!(!matches_scan_file(&eq, &missing));
        assert!(!matches_scan_file(&not_eq, &seven));
        assert!(matches_scan_file(&not_eq, &negative_one));
        assert!(!matches_scan_file(&not_eq, &raw_empty));
        assert!(!matches_scan_file(&not_eq, &invalid_integer));
        assert!(!matches_scan_file(&not_eq, &missing));
        assert!(matches_scan_file(&in_list, &seven));
        assert!(matches_scan_file(&in_list, &negative_one));
        assert!(!matches_scan_file(&not_in, &seven));
        assert!(!matches_scan_file(&not_in, &negative_one));
        assert!(!matches_scan_file(&not_in, &raw_empty));
        assert!(!matches_scan_file(&not_in, &invalid_integer));
        assert!(!matches_scan_file(&not_in, &missing));

        assert_eq!(
            predicate_expr(&col("id").eq(lit("7")), &["id"]),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            predicate_expr(
                &col("id").eq(Expr::Literal(ScalarValue::Int64(None), None)),
                &["id"]
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
    }

    #[test]
    fn converts_integer_comparisons_with_typed_ordering_and_width_bounds() {
        let lt = predicate_expr(&col("id").lt(lit(7_i64)), &["id"]).unwrap();
        let lt_eq = predicate_expr(&col("id").lt_eq(lit(-1_i64)), &["id"]).unwrap();
        let gt = predicate_expr(&col("id").gt(lit(-1_i64)), &["id"]).unwrap();
        let gt_eq = predicate_expr(&lit(7_i64).lt_eq(col("id")), &["id"]).unwrap();
        let seven = values(&[("id", "7")]);
        let negative_one = values(&[("id", "-1")]);
        let raw_empty = values(&[("id", "")]);
        let invalid_integer = values(&[("id", "not-an-integer")]);
        let missing = HashMap::new();

        assert!(!matches_scan_file(&lt, &seven));
        assert!(matches_scan_file(&lt, &negative_one));
        assert!(matches_scan_file(&lt_eq, &negative_one));
        assert!(!matches_scan_file(&lt_eq, &seven));
        assert!(matches_scan_file(&gt, &seven));
        assert!(!matches_scan_file(&gt, &negative_one));
        assert!(matches_scan_file(&gt_eq, &seven));
        assert!(!matches_scan_file(&gt_eq, &negative_one));
        assert!(!matches_scan_file(&lt, &raw_empty));
        assert!(!matches_scan_file(&lt, &invalid_integer));
        assert!(!matches_scan_file(&lt, &missing));
        assert_eq!(
            predicate_expr(&col("id").lt(lit("7")), &["id"]),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            predicate_expr(&col("small_id").lt(lit(128_i16)), &["small_id"]),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
    }

    #[test]
    fn converts_integer_between_with_inclusive_and_negated_semantics() {
        let between = predicate_expr(&col("id").between(lit(-1_i64), lit(7_i64)), &["id"]).unwrap();
        let not_between =
            predicate_expr(&col("id").not_between(lit(-1_i64), lit(7_i64)), &["id"]).unwrap();
        let contradictory =
            predicate_expr(&col("id").between(lit(10_i64), lit(-10_i64)), &["id"]).unwrap();
        let contradictory_not =
            predicate_expr(&col("id").not_between(lit(10_i64), lit(-10_i64)), &["id"]).unwrap();
        let seven = values(&[("id", "7")]);
        let negative_one = values(&[("id", "-1")]);
        let twenty = values(&[("id", "20")]);
        let raw_empty = values(&[("id", "")]);
        let invalid_integer = values(&[("id", "not-an-integer")]);
        let missing = HashMap::new();

        assert!(matches_scan_file(&between, &seven));
        assert!(matches_scan_file(&between, &negative_one));
        assert!(!matches_scan_file(&between, &twenty));
        assert!(!matches_scan_file(&between, &raw_empty));
        assert!(!matches_scan_file(&between, &invalid_integer));
        assert!(!matches_scan_file(&between, &missing));
        assert!(!matches_scan_file(&not_between, &seven));
        assert!(!matches_scan_file(&not_between, &negative_one));
        assert!(matches_scan_file(&not_between, &twenty));
        assert!(!matches_scan_file(&not_between, &raw_empty));
        assert!(!matches_scan_file(&not_between, &invalid_integer));
        assert!(!matches_scan_file(&not_between, &missing));
        assert!(!matches_scan_file(&contradictory, &seven));
        assert!(!matches_scan_file(&contradictory, &negative_one));
        assert!(!matches_scan_file(&contradictory, &twenty));
        assert!(matches_scan_file(&contradictory_not, &seven));
        assert!(matches_scan_file(&contradictory_not, &negative_one));
        assert!(matches_scan_file(&contradictory_not, &twenty));
        assert!(!matches_scan_file(&contradictory_not, &raw_empty));
        assert_eq!(
            predicate_expr(&col("id").between(lit("1"), lit("9")), &["id"]),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
    }

    #[test]
    fn converts_equality_for_non_empty_empty_and_reversed_string_literals() {
        let equals_west = predicate_expr(&col("region").eq(lit("us-west")), &["region"]).unwrap();
        let equals_empty = predicate_expr(&col("region").eq(lit("")), &["region"]).unwrap();
        let reversed = predicate_expr(&lit("us-west").eq(col("region")), &["region"]).unwrap();
        let normal = values(&[("region", "us-west")]);
        let raw_empty = values(&[("region", "")]);
        let missing = HashMap::new();

        assert!(matches_scan_file(&equals_west, &normal));
        assert!(!matches_scan_file(&equals_west, &raw_empty));
        assert!(!matches_scan_file(&equals_west, &missing));
        assert!(!matches_scan_file(&equals_empty, &normal));
        assert!(matches_scan_file(&equals_empty, &raw_empty));
        assert!(!matches_scan_file(&equals_empty, &missing));
        assert!(matches_scan_file(&reversed, &normal));
        assert!(!matches_scan_file(
            &reversed,
            &values(&[("region", "us-east")])
        ));
    }

    #[test]
    fn converts_in_lists_with_sql_semantics_for_present_missing_and_raw_empty_values() {
        let predicate = predicate_expr(
            &col("region").in_list(vec![lit("us-west"), lit("us-east"), lit("")], false),
            &["region"],
        )
        .unwrap();

        assert!(matches_scan_file(
            &predicate,
            &values(&[("region", "us-west")])
        ));
        assert!(matches_scan_file(
            &predicate,
            &values(&[("region", "us-east")])
        ));
        assert!(matches_scan_file(&predicate, &values(&[("region", "")])));
        assert!(!matches_scan_file(
            &predicate,
            &values(&[("region", "eu-central")])
        ));
        assert!(!matches_scan_file(&predicate, &HashMap::new()));
    }

    #[test]
    fn converts_in_lists_with_duplicate_literals_without_changing_matches() {
        let predicate = predicate_expr(
            &col("region").in_list(vec![lit("us-west"), lit("us-west")], false),
            &["region"],
        )
        .unwrap();

        assert!(matches_scan_file(
            &predicate,
            &values(&[("region", "us-west")])
        ));
        assert!(!matches_scan_file(
            &predicate,
            &values(&[("region", "us-east")])
        ));
    }

    #[test]
    fn converts_negated_equality_and_in_list_with_sql_null_semantics() {
        let not_eq = predicate_expr(&col("region").not_eq(lit("us-west")), &["region"]).unwrap();
        let not_in = predicate_expr(
            &col("region").in_list(vec![lit("us-west"), lit("us-east")], true),
            &["region"],
        )
        .unwrap();
        let west = values(&[("region", "us-west")]);
        let east = values(&[("region", "us-east")]);
        let empty = values(&[("region", "")]);
        let missing = HashMap::new();

        assert!(!matches_scan_file(&not_eq, &west));
        assert!(matches_scan_file(&not_eq, &east));
        assert!(matches_scan_file(&not_eq, &empty));
        assert!(!matches_scan_file(&not_eq, &missing));
        assert!(!matches_scan_file(&not_in, &west));
        assert!(!matches_scan_file(&not_in, &east));
        assert!(matches_scan_file(&not_in, &empty));
        assert!(!matches_scan_file(&not_in, &missing));
    }

    #[test]
    fn converts_comparisons_with_string_ordering_and_sql_null_semantics() {
        let lt = predicate_expr(&col("region").lt(lit("us-west")), &["region"]).unwrap();
        let lt_eq = predicate_expr(&col("region").lt_eq(lit("us-east")), &["region"]).unwrap();
        let gt = predicate_expr(&col("region").gt(lit("us-east")), &["region"]).unwrap();
        let gt_eq = predicate_expr(&lit("us-east").lt_eq(col("region")), &["region"]).unwrap();
        let west = values(&[("region", "us-west")]);
        let east = values(&[("region", "us-east")]);
        let empty = values(&[("region", "")]);
        let missing = HashMap::new();

        assert!(!matches_scan_file(&lt, &west));
        assert!(matches_scan_file(&lt, &east));
        assert!(matches_scan_file(&lt, &empty));
        assert!(matches_scan_file(&lt_eq, &east));
        assert!(matches_scan_file(&lt_eq, &empty));
        assert!(!matches_scan_file(&lt_eq, &west));
        assert!(matches_scan_file(&gt, &west));
        assert!(!matches_scan_file(&gt, &east));
        assert!(!matches_scan_file(&gt, &empty));
        assert!(matches_scan_file(&gt_eq, &west));
        assert!(matches_scan_file(&gt_eq, &east));
        assert!(!matches_scan_file(&gt_eq, &empty));
        assert!(!matches_scan_file(&lt, &missing));
        assert!(!matches_scan_file(&gt, &missing));
    }

    #[test]
    fn converts_between_with_inclusive_bounds_and_sql_null_semantics() {
        let between = predicate_expr(
            &col("region").between(lit("us-east"), lit("us-west")),
            &["region"],
        )
        .unwrap();
        let not_between = predicate_expr(
            &col("region").not_between(lit("us-east"), lit("us-west")),
            &["region"],
        )
        .unwrap();
        let west = values(&[("region", "us-west")]);
        let east = values(&[("region", "us-east")]);
        let empty = values(&[("region", "")]);
        let missing = HashMap::new();

        assert!(matches_scan_file(&between, &west));
        assert!(matches_scan_file(&between, &east));
        assert!(!matches_scan_file(&between, &empty));
        assert!(!matches_scan_file(&between, &missing));
        assert!(!matches_scan_file(&not_between, &west));
        assert!(!matches_scan_file(&not_between, &east));
        assert!(matches_scan_file(&not_between, &empty));
        assert!(!matches_scan_file(&not_between, &missing));
    }

    #[test]
    fn converts_contradictory_between_bounds_with_sql_boolean_semantics() {
        let between =
            predicate_expr(&col("region").between(lit("z"), lit("a")), &["region"]).unwrap();
        let not_between =
            predicate_expr(&col("region").not_between(lit("z"), lit("a")), &["region"]).unwrap();
        let west = values(&[("region", "us-west")]);
        let east = values(&[("region", "us-east")]);
        let empty = values(&[("region", "")]);
        let missing = HashMap::new();

        assert!(!matches_scan_file(&between, &west));
        assert!(!matches_scan_file(&between, &east));
        assert!(!matches_scan_file(&between, &empty));
        assert!(!matches_scan_file(&between, &missing));
        assert!(matches_scan_file(&not_between, &west));
        assert!(matches_scan_file(&not_between, &east));
        assert!(matches_scan_file(&not_between, &empty));
        assert!(!matches_scan_file(&not_between, &missing));
    }

    #[test]
    fn converts_boolean_composition_with_sql_three_valued_logic() {
        let filter = col("region")
            .eq(lit("us-west"))
            .or(col("region").is_null())
            .and(Expr::Not(Box::new(col("day").eq(lit("2026-05-31")))));
        let predicate = predicate_expr(&filter, &["region", "day"]).unwrap();

        assert!(matches_scan_file(
            &predicate,
            &values(&[("region", "us-west"), ("day", "")])
        ));
        assert!(matches_scan_file(&predicate, &values(&[("day", "")])));
        assert!(!matches_scan_file(
            &predicate,
            &values(&[("region", "us-west"), ("day", "2026-05-31")])
        ));
        assert!(!matches_scan_file(
            &predicate,
            &values(&[("region", "us-east"), ("day", "")])
        ));
    }

    #[test]
    fn physical_name_lookup_controls_converted_metadata_key_access() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let name_map =
            DeltaPartitionNameMap::new([("region".to_owned(), "col-physical-region".to_owned())]);
        let predicate = convert_expr(
            &col("region").eq(lit("us-west")),
            &schema,
            &partition_columns,
            &name_map,
        )
        .unwrap();

        assert!(matches_scan_file(
            &predicate,
            &values(&[("col-physical-region", "us-west")])
        ));
        assert!(!matches_scan_file(
            &predicate,
            &values(&[("region", "us-west")])
        ));
    }

    #[test]
    fn unsupported_shapes_return_typed_errors() {
        let schema = schema();
        let region_partition_columns = partition_columns(&["region"]);
        let name_map = identity_map(&["region"]);
        let id_partition_columns = partition_columns(&["id"]);
        let id_name_map = identity_map(&["id"]);
        let qualified = Expr::Column(Column::new(Some("orders"), "region")).eq(lit("us-west"));
        let dotted = col("region.value").eq(lit("us-west"));
        let non_partition = col("id").eq(lit("1"));
        let null_literal = col("region").eq(Expr::Literal(ScalarValue::Utf8(None), None));
        let wrong_literal_type_comparison = col("region").lt(lit(7_i64));
        let empty_in = col("region").in_list(Vec::<Expr>::new(), false);
        let null_in = col("region").in_list(
            vec![lit("us-west"), Expr::Literal(ScalarValue::Utf8(None), None)],
            false,
        );
        let non_literal_in = col("region").in_list(vec![col("day")], false);
        let null_between =
            col("region").between(Expr::Literal(ScalarValue::Utf8(None), None), lit("us-west"));
        let non_literal_between = col("region").between(col("day"), lit("us-west"));
        let integer_null_in = col("id").in_list(
            vec![lit(7_i64), Expr::Literal(ScalarValue::Int64(None), None)],
            false,
        );
        let integer_mixed_type_in = col("id").in_list(vec![lit(7_i64), lit("7")], false);
        let integer_non_literal_in = col("id").in_list(vec![col("region")], false);
        let integer_null_between =
            col("id").between(Expr::Literal(ScalarValue::Int64(None), None), lit(9_i64));
        let integer_non_literal_between = col("id").between(col("region"), lit(9_i64));
        let integer_cast_operand =
            col("id").eq(datafusion::logical_expr::cast(lit(7_i64), DataType::Int64));

        assert_eq!(
            convert_expr(&qualified, &schema, &region_partition_columns, &name_map),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedColumnReference)
        );
        assert_eq!(
            convert_expr(&dotted, &schema, &region_partition_columns, &name_map),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedColumnReference)
        );
        assert_eq!(
            convert_expr(
                &non_partition,
                &schema,
                &region_partition_columns,
                &name_map
            ),
            Err(DeltaPartitionMetadataPredicateError::NonPartitionColumn)
        );
        assert_eq!(
            convert_expr(
                &col("id").eq(lit("1")),
                &schema,
                &id_partition_columns,
                &id_name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            convert_expr(&null_literal, &schema, &region_partition_columns, &name_map),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            convert_expr(
                &wrong_literal_type_comparison,
                &schema,
                &region_partition_columns,
                &name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            convert_expr(&empty_in, &schema, &region_partition_columns, &name_map),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedExpression)
        );
        assert_eq!(
            convert_expr(&null_in, &schema, &region_partition_columns, &name_map),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            convert_expr(
                &non_literal_in,
                &schema,
                &region_partition_columns,
                &name_map
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            convert_expr(&null_between, &schema, &region_partition_columns, &name_map),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            convert_expr(
                &non_literal_between,
                &schema,
                &region_partition_columns,
                &name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            convert_expr(
                &integer_null_in,
                &schema,
                &id_partition_columns,
                &id_name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            convert_expr(
                &integer_mixed_type_in,
                &schema,
                &id_partition_columns,
                &id_name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            convert_expr(
                &integer_non_literal_in,
                &schema,
                &id_partition_columns,
                &id_name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            convert_expr(
                &integer_null_between,
                &schema,
                &id_partition_columns,
                &id_name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            convert_expr(
                &integer_non_literal_between,
                &schema,
                &id_partition_columns,
                &id_name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            convert_expr(
                &integer_cast_operand,
                &schema,
                &id_partition_columns,
                &id_name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
    }
}
