//! SQL-compatible Delta partition metadata predicate evaluation.

// Scan plans can carry this predicate before scan metadata expansion consumes
// it, so keep dead-code warnings quiet until file-level pruning calls it.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{Expr, Operator};
use snafu::Snafu;

/// Returns whether this provider can evaluate a Delta partition column type from metadata.
///
/// Delta stores partition values as serialized text in add-file metadata, but
/// exact SQL pushdown also depends on the logical schema type. Today only
/// string-like partition columns have proven metadata semantics in this
/// provider. When numeric, decimal, boolean, binary, date, and timestamp
/// partition columns are promoted, this function is the single type gate to
/// update for both support planning and metadata evaluation.
#[must_use]
pub(crate) fn supports_partition_metadata_logical_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
}

/// Logical-to-physical partition column names for Delta scan metadata.
///
/// Delta scan files expose partition values by physical column name. Most
/// currently supported tables use the logical name as the physical name, but
/// keeping the lookup explicit prevents future column-mapping support from
/// leaking into provider planning code.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DeltaPartitionNameMap {
    logical_to_physical: HashMap<String, String>,
}

impl DeltaPartitionNameMap {
    /// Builds an identity lookup for tables where logical and physical
    /// partition names are the same.
    #[must_use]
    pub(crate) fn identity(partition_columns: &HashSet<String>) -> Self {
        Self {
            logical_to_physical: partition_columns
                .iter()
                .map(|name| (name.clone(), name.clone()))
                .collect(),
        }
    }

    #[cfg(test)]
    fn new(logical_to_physical: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            logical_to_physical: logical_to_physical.into_iter().collect(),
        }
    }

    fn physical_name(&self, logical_name: &str) -> Option<&str> {
        self.logical_to_physical
            .get(logical_name)
            .map(String::as_str)
    }
}

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

/// Provider-owned predicate over serialized Delta partition metadata.
///
/// This is intentionally independent from `delta_kernel` predicate pruning.
/// It evaluates `ScanFile.partition_values` with DataFusion SQL semantics:
/// missing partition keys are treated as SQL null, while a present raw empty
/// string remains a non-null empty string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeltaPartitionMetadataPredicate {
    expr: PartitionMetadataExpr,
}

impl DeltaPartitionMetadataPredicate {
    /// Converts a supported DataFusion expression into a metadata predicate.
    ///
    /// The current policy supports string partition columns, string equality,
    /// non-negated string `IN`, `IS NULL`, `IS NOT NULL`, and boolean
    /// composition over supported child predicates. Unsupported expressions
    /// return a typed error so the caller can keep DataFusion residual
    /// filtering instead of guessing.
    pub(crate) fn from_datafusion_expr(
        expr: &Expr,
        logical_schema: &SchemaRef,
        partition_columns: &HashSet<String>,
        physical_name_lookup: &DeltaPartitionNameMap,
    ) -> Result<Self, DeltaPartitionMetadataPredicateError> {
        Ok(Self {
            expr: convert_expr(
                expr,
                logical_schema,
                partition_columns,
                physical_name_lookup,
            )?,
        })
    }

    /// Combines multiple metadata predicates with logical `AND`.
    ///
    /// DataFusion may push multiple exact filters into one scan. The scan plan
    /// stores one metadata predicate for that whole provider-owned filter set,
    /// so each accepted input filter becomes a child of this conjunction.
    #[must_use]
    pub(crate) fn and_from(predicates: impl IntoIterator<Item = Self>) -> Option<Self> {
        let mut predicates = predicates
            .into_iter()
            .map(|predicate| predicate.expr)
            .collect::<Vec<_>>();
        let first = predicates.pop()?;

        Some(Self {
            expr: predicates.into_iter().fold(first, |right, left| {
                PartitionMetadataExpr::And(Box::new(left), Box::new(right))
            }),
        })
    }

    /// Returns whether one scan file should be kept by this predicate.
    ///
    /// SQL three-valued logic is collapsed using WHERE semantics: only `TRUE`
    /// keeps a file. `FALSE` and `NULL` both prune it. The input map is the raw
    /// partition metadata attached to a Delta `ScanFile`: missing keys are SQL
    /// nulls, while present empty strings are non-null empty strings.
    #[must_use]
    pub(crate) fn matches_scan_file(&self, partition_values: &HashMap<String, String>) -> bool {
        self.expr.eval(partition_values) == SqlBool::True
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PartitionMetadataExpr {
    Eq {
        column: PhysicalPartitionColumn,
        literal: String,
    },
    In {
        column: PhysicalPartitionColumn,
        literals: HashSet<String>,
    },
    IsNull(PhysicalPartitionColumn),
    IsNotNull(PhysicalPartitionColumn),
    And(Box<PartitionMetadataExpr>, Box<PartitionMetadataExpr>),
    Or(Box<PartitionMetadataExpr>, Box<PartitionMetadataExpr>),
    Not(Box<PartitionMetadataExpr>),
}

impl PartitionMetadataExpr {
    fn eval(&self, partition_values: &HashMap<String, String>) -> SqlBool {
        match self {
            Self::Eq { column, literal } => column
                .value(partition_values)
                .map(|value| SqlBool::from(value == literal.as_str()))
                .unwrap_or(SqlBool::Null),
            Self::In { column, literals } => column
                .value(partition_values)
                .map(|value| SqlBool::from(literals.contains(value)))
                .unwrap_or(SqlBool::Null),
            Self::IsNull(column) => SqlBool::from(column.value(partition_values).is_none()),
            Self::IsNotNull(column) => SqlBool::from(column.value(partition_values).is_some()),
            Self::And(left, right) => left
                .eval(partition_values)
                .and(right.eval(partition_values)),
            Self::Or(left, right) => left.eval(partition_values).or(right.eval(partition_values)),
            Self::Not(inner) => inner.eval(partition_values).not(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PhysicalPartitionColumn {
    physical_name: String,
}

impl PhysicalPartitionColumn {
    fn value<'a>(&self, partition_values: &'a HashMap<String, String>) -> Option<&'a str> {
        partition_values
            .get(&self.physical_name)
            .map(String::as_str)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SqlBool {
    True,
    False,
    Null,
}

impl SqlBool {
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            (Self::Null, _) | (_, Self::Null) => Self::Null,
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            (Self::Null, _) | (_, Self::Null) => Self::Null,
        }
    }

    fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Null => Self::Null,
        }
    }
}

impl From<bool> for SqlBool {
    fn from(value: bool) -> Self {
        if value { Self::True } else { Self::False }
    }
}

fn convert_expr(
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
        Expr::BinaryExpr(_) => Err(DeltaPartitionMetadataPredicateError::UnsupportedOperator),
        Expr::InList(in_list) => convert_in_list(
            in_list.expr.as_ref(),
            &in_list.list,
            in_list.negated,
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

fn convert_in_list(
    column: &Expr,
    literals: &[Expr],
    negated: bool,
    logical_schema: &SchemaRef,
    partition_columns: &HashSet<String>,
    physical_name_lookup: &DeltaPartitionNameMap,
) -> Result<PartitionMetadataExpr, DeltaPartitionMetadataPredicateError> {
    if negated || literals.is_empty() {
        return Err(DeltaPartitionMetadataPredicateError::UnsupportedExpression);
    }

    Ok(PartitionMetadataExpr::In {
        column: convert_column(
            column,
            logical_schema,
            partition_columns,
            physical_name_lookup,
        )?,
        literals: literals
            .iter()
            .map(convert_string_literal)
            .collect::<Result<HashSet<_>, _>>()?,
    })
}

fn convert_equality(
    column: &Expr,
    literal: &Expr,
    logical_schema: &SchemaRef,
    partition_columns: &HashSet<String>,
    physical_name_lookup: &DeltaPartitionNameMap,
) -> Result<PartitionMetadataExpr, DeltaPartitionMetadataPredicateError> {
    Ok(PartitionMetadataExpr::Eq {
        column: convert_column(
            column,
            logical_schema,
            partition_columns,
            physical_name_lookup,
        )?,
        literal: convert_string_literal(literal)?,
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

    let physical_name = physical_name_lookup
        .physical_name(logical_name)
        .ok_or(DeltaPartitionMetadataPredicateError::MissingPhysicalName)?;

    Ok(PhysicalPartitionColumn {
        physical_name: physical_name.to_owned(),
    })
}

fn top_level_column_name(column: &Column) -> Result<&str, DeltaPartitionMetadataPredicateError> {
    if column.relation.is_some() || column.name.contains('.') {
        Err(DeltaPartitionMetadataPredicateError::UnsupportedColumnReference)
    } else {
        Ok(&column.name)
    }
}

fn convert_string_literal(expr: &Expr) -> Result<String, DeltaPartitionMetadataPredicateError> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _) => Ok(value.clone()),
        _ => Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::TimeUnit;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{Expr, col, lit};

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
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

    #[test]
    fn metadata_type_policy_documents_current_string_only_scope() {
        let supported = [DataType::Utf8, DataType::LargeUtf8];
        let unsupported = [
            DataType::Int64,
            DataType::Int32,
            DataType::Int16,
            DataType::Int8,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(10, 2),
            DataType::Boolean,
            DataType::Binary,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ];

        assert!(
            supported
                .iter()
                .all(supports_partition_metadata_logical_type)
        );
        assert!(
            unsupported
                .iter()
                .all(|data_type| !supports_partition_metadata_logical_type(data_type))
        );
    }

    fn predicate(
        expr: &Expr,
        partitions: &[&str],
    ) -> Result<DeltaPartitionMetadataPredicate, DeltaPartitionMetadataPredicateError> {
        let partition_columns = partition_columns(partitions);
        let name_map = DeltaPartitionNameMap::identity(&partition_columns);

        DeltaPartitionMetadataPredicate::from_datafusion_expr(
            expr,
            &schema(),
            &partition_columns,
            &name_map,
        )
    }

    #[test]
    fn is_null_uses_sql_semantics_for_missing_and_raw_empty_values() {
        let is_null = predicate(&col("region").is_null(), &["region"]).unwrap();
        let is_not_null = predicate(&col("region").is_not_null(), &["region"]).unwrap();
        let normal = values(&[("region", "us-west")]);
        let raw_empty = values(&[("region", "")]);
        let missing = HashMap::new();

        assert!(!is_null.matches_scan_file(&normal));
        assert!(!is_null.matches_scan_file(&raw_empty));
        assert!(is_null.matches_scan_file(&missing));
        assert!(is_not_null.matches_scan_file(&normal));
        assert!(is_not_null.matches_scan_file(&raw_empty));
        assert!(!is_not_null.matches_scan_file(&missing));
    }

    #[test]
    fn equality_supports_non_empty_and_empty_string_literals() {
        let equals_west = predicate(&col("region").eq(lit("us-west")), &["region"]).unwrap();
        let equals_empty = predicate(&col("region").eq(lit("")), &["region"]).unwrap();
        let normal = values(&[("region", "us-west")]);
        let raw_empty = values(&[("region", "")]);
        let missing = HashMap::new();

        assert!(equals_west.matches_scan_file(&normal));
        assert!(!equals_west.matches_scan_file(&raw_empty));
        assert!(!equals_west.matches_scan_file(&missing));
        assert!(!equals_empty.matches_scan_file(&normal));
        assert!(equals_empty.matches_scan_file(&raw_empty));
        assert!(!equals_empty.matches_scan_file(&missing));
    }

    #[test]
    fn equality_supports_reversed_column_and_literal_order() {
        let equals_west = predicate(&lit("us-west").eq(col("region")), &["region"]).unwrap();

        assert!(equals_west.matches_scan_file(&values(&[("region", "us-west")])));
        assert!(!equals_west.matches_scan_file(&values(&[("region", "us-east")])));
    }

    #[test]
    fn in_list_matches_sql_semantics_for_present_missing_and_raw_empty_values() {
        let predicate = predicate(
            &col("region").in_list(vec![lit("us-west"), lit("us-east"), lit("")], false),
            &["region"],
        )
        .unwrap();

        assert!(predicate.matches_scan_file(&values(&[("region", "us-west")])));
        assert!(predicate.matches_scan_file(&values(&[("region", "us-east")])));
        assert!(predicate.matches_scan_file(&values(&[("region", "")])));
        assert!(!predicate.matches_scan_file(&values(&[("region", "eu-central")])));
        assert!(!predicate.matches_scan_file(&HashMap::new()));
    }

    #[test]
    fn in_list_deduplicates_literals_without_changing_matches() {
        let predicate = predicate(
            &col("region").in_list(vec![lit("us-west"), lit("us-west")], false),
            &["region"],
        )
        .unwrap();

        assert!(predicate.matches_scan_file(&values(&[("region", "us-west")])));
        assert!(!predicate.matches_scan_file(&values(&[("region", "us-east")])));
    }

    #[test]
    fn boolean_composition_uses_sql_three_valued_logic() {
        let filter = col("region")
            .eq(lit("us-west"))
            .or(col("region").is_null())
            .and(Expr::Not(Box::new(col("day").eq(lit("2026-05-31")))));
        let predicate = predicate(&filter, &["region", "day"]).unwrap();

        assert!(predicate.matches_scan_file(&values(&[("region", "us-west"), ("day", "")])));
        assert!(predicate.matches_scan_file(&values(&[("day", "")])));
        assert!(
            !predicate.matches_scan_file(&values(&[("region", "us-west"), ("day", "2026-05-31")]))
        );
        assert!(!predicate.matches_scan_file(&values(&[("region", "us-east"), ("day", "")])));
    }

    #[test]
    fn physical_name_lookup_controls_metadata_key_access() {
        let schema = schema();
        let partition_columns = partition_columns(&["region"]);
        let name_map =
            DeltaPartitionNameMap::new([("region".to_owned(), "col-physical-region".to_owned())]);
        let predicate = DeltaPartitionMetadataPredicate::from_datafusion_expr(
            &col("region").eq(lit("us-west")),
            &schema,
            &partition_columns,
            &name_map,
        )
        .unwrap();

        assert!(predicate.matches_scan_file(&values(&[("col-physical-region", "us-west")])));
        assert!(!predicate.matches_scan_file(&values(&[("region", "us-west")])));
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
        let not_eq = col("region").not_eq(lit("us-west"));
        let empty_in = col("region").in_list(Vec::<Expr>::new(), false);
        let null_in = col("region").in_list(
            vec![lit("us-west"), Expr::Literal(ScalarValue::Utf8(None), None)],
            false,
        );
        let negated_in = col("region").in_list(vec![lit("us-west")], true);
        let non_literal_in = col("region").in_list(vec![col("day")], false);

        assert_eq!(
            DeltaPartitionMetadataPredicate::from_datafusion_expr(
                &qualified,
                &schema,
                &region_partition_columns,
                &name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedColumnReference)
        );
        assert_eq!(
            DeltaPartitionMetadataPredicate::from_datafusion_expr(
                &dotted,
                &schema,
                &region_partition_columns,
                &name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedColumnReference)
        );
        assert_eq!(
            DeltaPartitionMetadataPredicate::from_datafusion_expr(
                &non_partition,
                &schema,
                &region_partition_columns,
                &name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::NonPartitionColumn)
        );
        assert_eq!(
            DeltaPartitionMetadataPredicate::from_datafusion_expr(
                &col("id").eq(lit(1_i64)),
                &schema,
                &id_partition_columns,
                &id_name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedColumnType)
        );
        assert_eq!(
            DeltaPartitionMetadataPredicate::from_datafusion_expr(
                &null_literal,
                &schema,
                &region_partition_columns,
                &name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            DeltaPartitionMetadataPredicate::from_datafusion_expr(
                &not_eq,
                &schema,
                &region_partition_columns,
                &name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedOperator)
        );
        assert_eq!(
            DeltaPartitionMetadataPredicate::from_datafusion_expr(
                &empty_in,
                &schema,
                &region_partition_columns,
                &name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedExpression)
        );
        assert_eq!(
            DeltaPartitionMetadataPredicate::from_datafusion_expr(
                &null_in,
                &schema,
                &region_partition_columns,
                &name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
        assert_eq!(
            DeltaPartitionMetadataPredicate::from_datafusion_expr(
                &negated_in,
                &schema,
                &region_partition_columns,
                &name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedExpression)
        );
        assert_eq!(
            DeltaPartitionMetadataPredicate::from_datafusion_expr(
                &non_literal_in,
                &schema,
                &region_partition_columns,
                &name_map,
            ),
            Err(DeltaPartitionMetadataPredicateError::UnsupportedLiteral)
        );
    }
}
