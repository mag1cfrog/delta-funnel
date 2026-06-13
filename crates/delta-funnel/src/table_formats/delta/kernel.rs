//! Private boundary for stability-sensitive `delta_kernel` APIs.

#![allow(unused_imports)]

use std::sync::Arc;

use datafusion::common::{Column as DataFusionColumn, ScalarValue};
use datafusion::logical_expr::{Expr as DataFusionExpr, Operator as DataFusionOperator};
#[cfg(test)]
pub(crate) use delta_kernel::actions::deletion_vector::{
    DeletionVectorDescriptor, DeletionVectorStorageType,
};
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
pub(crate) use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::arrow::error::ArrowError;
pub(crate) use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
pub(crate) use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
pub(crate) use delta_kernel::engine::default::DefaultEngineBuilder;
pub(crate) use delta_kernel::engine::default::storage::store_from_url_opts;
pub(crate) use delta_kernel::expressions::{
    ColumnName, Expression, ExpressionRef, Predicate, PredicateRef, Scalar,
};
pub(crate) use delta_kernel::scan::Scan;
pub(crate) use delta_kernel::scan::ScanMetadata;
pub(crate) use delta_kernel::scan::state::{DvInfo, ScanFile, transform_to_logical};
pub(crate) use delta_kernel::schema::SchemaRef as KernelSchemaRef;
pub(crate) use delta_kernel::table_features::TABLE_FEATURES_MIN_READER_VERSION;
use delta_kernel::table_features::TableFeature;
pub(crate) use delta_kernel::{
    Engine, Error as DeltaKernelError, FileMeta, ParquetHandler, Snapshot, SnapshotRef, Version,
    try_parse_uri,
};
use snafu::Snafu;

/// Protocol details extracted through the private kernel adapter boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeltaKernelProtocol {
    pub(crate) min_reader_version: i32,
    pub(crate) min_writer_version: i32,
    pub(crate) reader_features: Vec<String>,
    pub(crate) writer_features: Vec<String>,
}

/// Typed rejection from the private DataFusion-to-kernel predicate adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum DeltaKernelPredicateAdapterError {
    #[snafu(display("unsupported DataFusion expression"))]
    UnsupportedExpression,
    #[snafu(display("unsupported DataFusion operator"))]
    UnsupportedOperator,
    #[snafu(display("unsupported DataFusion column reference"))]
    UnsupportedColumnReference,
    #[snafu(display("unsupported DataFusion literal"))]
    UnsupportedLiteral,
    #[snafu(display("null DataFusion literal is unsupported"))]
    NullLiteral,
}

/// Extracts the Delta protocol from a loaded snapshot.
#[must_use]
pub(crate) fn snapshot_protocol_report(snapshot: &SnapshotRef) -> DeltaKernelProtocol {
    let protocol = snapshot.table_configuration().protocol();

    DeltaKernelProtocol {
        min_reader_version: protocol.min_reader_version(),
        min_writer_version: protocol.min_writer_version(),
        reader_features: feature_names(protocol.reader_features()),
        writer_features: feature_names(protocol.writer_features()),
    }
}

/// Converts the loaded snapshot logical Delta schema to an Arrow schema.
pub(crate) fn snapshot_arrow_schema(snapshot: &SnapshotRef) -> Result<ArrowSchemaRef, ArrowError> {
    let schema: ArrowSchema = snapshot.schema().as_ref().try_into_arrow()?;

    Ok(Arc::new(schema))
}

/// Builds kernel scan state for the selected logical Delta columns.
#[allow(dead_code)]
pub(crate) fn build_projected_scan(
    snapshot: &SnapshotRef,
    projected_column_names: Option<&[String]>,
) -> delta_kernel::DeltaResult<(Scan, KernelSchemaRef)> {
    build_projected_predicated_scan(snapshot, projected_column_names, None)
}

/// Builds kernel scan state for selected logical Delta columns and an optional predicate.
///
/// This helper intentionally leaves parsed stats output disabled. `delta_kernel`
/// 0.23.0 supports combining `ScanBuilder::with_predicate` with
/// `ScanBuilder::include_all_stats_columns`, and a later scan-metadata slice
/// should choose that path when it needs parsed file stats output.
#[allow(dead_code)]
pub(crate) fn build_projected_predicated_scan(
    snapshot: &SnapshotRef,
    projected_column_names: Option<&[String]>,
    predicate: Option<DeltaKernelPredicate>,
) -> delta_kernel::DeltaResult<(Scan, KernelSchemaRef)> {
    let schema = match projected_column_names {
        Some(names) => snapshot.schema().project(names)?,
        None => snapshot.schema(),
    };
    let scan = Arc::clone(snapshot)
        .scan_builder()
        .with_schema(Arc::clone(&schema))
        .with_predicate(predicate.map(DeltaKernelPredicate::into_inner))
        .build()?;

    Ok((scan, schema))
}

/// Builds scan state with parsed file statistics exposed in scan metadata.
pub(crate) fn build_projected_predicated_stats_scan(
    snapshot: &SnapshotRef,
    projected_column_names: Option<&[String]>,
    predicate: Option<DeltaKernelPredicate>,
) -> delta_kernel::DeltaResult<(Scan, KernelSchemaRef)> {
    let schema = match projected_column_names {
        Some(names) => snapshot.schema().project(names)?,
        None => snapshot.schema(),
    };
    let scan = Arc::clone(snapshot)
        .scan_builder()
        .with_schema(Arc::clone(&schema))
        .with_predicate(predicate.map(DeltaKernelPredicate::into_inner))
        .include_all_stats_columns()
        .build()?;

    Ok((scan, schema))
}

/// Private wrapper around an official `delta_kernel` predicate.
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DeltaKernelPredicate {
    inner: PredicateRef,
}

#[allow(dead_code)]
impl DeltaKernelPredicate {
    #[must_use]
    pub(crate) fn new(predicate: Predicate) -> Self {
        Self {
            inner: Arc::new(predicate),
        }
    }

    #[must_use]
    pub(crate) fn as_ref(&self) -> &PredicateRef {
        &self.inner
    }

    #[must_use]
    pub(crate) fn into_inner(self) -> PredicateRef {
        self.inner
    }

    /// Combines multiple adapter-owned predicates with logical `AND`.
    ///
    /// delta_kernel's scan builder accepts one predicate. Provider planning can
    /// receive multiple exact pushed filters, so this keeps the combination
    /// inside the Delta table-format boundary instead of exposing kernel
    /// predicate internals to DataFusion modules.
    #[must_use]
    pub(crate) fn and_from(predicates: impl IntoIterator<Item = Self>) -> Option<Self> {
        let predicates = predicates
            .into_iter()
            .map(|predicate| predicate.inner.as_ref().clone())
            .collect::<Vec<_>>();

        if predicates.is_empty() {
            None
        } else {
            Some(Self::new(Predicate::and_from(predicates)))
        }
    }
}

/// Converts a supported DataFusion filter expression into an official kernel predicate.
///
/// This adapter is intentionally conservative. It only accepts unqualified
/// top-level column references here; provider-side schema analysis can widen
/// that later without guessing about nested or qualified column semantics.
#[allow(dead_code)]
pub(crate) fn datafusion_expr_to_kernel_predicate(
    filter: &DataFusionExpr,
) -> Result<DeltaKernelPredicate, DeltaKernelPredicateAdapterError> {
    datafusion_expr_to_kernel_predicate_inner(filter).map(DeltaKernelPredicate::new)
}

fn datafusion_expr_to_kernel_predicate_inner(
    filter: &DataFusionExpr,
) -> Result<Predicate, DeltaKernelPredicateAdapterError> {
    match filter {
        DataFusionExpr::Literal(ScalarValue::Boolean(Some(value)), _) => {
            Ok(Predicate::literal(*value))
        }
        DataFusionExpr::BinaryExpr(binary) => match binary.op {
            DataFusionOperator::And => Ok(Predicate::and(
                datafusion_expr_to_kernel_predicate_inner(binary.left.as_ref())?,
                datafusion_expr_to_kernel_predicate_inner(binary.right.as_ref())?,
            )),
            DataFusionOperator::Or => Ok(Predicate::or(
                datafusion_expr_to_kernel_predicate_inner(binary.left.as_ref())?,
                datafusion_expr_to_kernel_predicate_inner(binary.right.as_ref())?,
            )),
            DataFusionOperator::Eq => Ok(Predicate::eq(
                datafusion_expr_to_kernel_expression(binary.left.as_ref())?,
                datafusion_expr_to_kernel_expression(binary.right.as_ref())?,
            )),
            DataFusionOperator::NotEq => Ok(Predicate::ne(
                datafusion_expr_to_kernel_expression(binary.left.as_ref())?,
                datafusion_expr_to_kernel_expression(binary.right.as_ref())?,
            )),
            DataFusionOperator::Lt => Ok(Predicate::lt(
                datafusion_expr_to_kernel_expression(binary.left.as_ref())?,
                datafusion_expr_to_kernel_expression(binary.right.as_ref())?,
            )),
            DataFusionOperator::LtEq => Ok(Predicate::le(
                datafusion_expr_to_kernel_expression(binary.left.as_ref())?,
                datafusion_expr_to_kernel_expression(binary.right.as_ref())?,
            )),
            DataFusionOperator::Gt => Ok(Predicate::gt(
                datafusion_expr_to_kernel_expression(binary.left.as_ref())?,
                datafusion_expr_to_kernel_expression(binary.right.as_ref())?,
            )),
            DataFusionOperator::GtEq => Ok(Predicate::ge(
                datafusion_expr_to_kernel_expression(binary.left.as_ref())?,
                datafusion_expr_to_kernel_expression(binary.right.as_ref())?,
            )),
            _ => Err(DeltaKernelPredicateAdapterError::UnsupportedOperator),
        },
        DataFusionExpr::Not(inner) => Ok(Predicate::not(
            datafusion_expr_to_kernel_predicate_inner(inner.as_ref())?,
        )),
        DataFusionExpr::IsNull(inner) => Ok(Predicate::is_null(
            datafusion_expr_to_kernel_expression(inner.as_ref())?,
        )),
        DataFusionExpr::IsNotNull(inner) => Ok(Predicate::is_not_null(
            datafusion_expr_to_kernel_expression(inner.as_ref())?,
        )),
        DataFusionExpr::Between(between) => {
            let expr = datafusion_expr_to_kernel_expression(between.expr.as_ref())?;
            let low = datafusion_expr_to_kernel_expression(between.low.as_ref())?;
            let high = datafusion_expr_to_kernel_expression(between.high.as_ref())?;

            if between.negated {
                Ok(Predicate::or(
                    Predicate::lt(expr.clone(), low),
                    Predicate::gt(expr, high),
                ))
            } else {
                Ok(Predicate::and(
                    Predicate::ge(expr.clone(), low),
                    Predicate::le(expr, high),
                ))
            }
        }
        DataFusionExpr::InList(in_list) => datafusion_in_list_to_kernel_predicate(in_list),
        _ => Err(DeltaKernelPredicateAdapterError::UnsupportedExpression),
    }
}

fn datafusion_in_list_to_kernel_predicate(
    in_list: &datafusion::logical_expr::expr::InList,
) -> Result<Predicate, DeltaKernelPredicateAdapterError> {
    if in_list.list.is_empty() {
        return if in_list.negated {
            Ok(Predicate::is_not_null(
                datafusion_expr_to_kernel_expression(in_list.expr.as_ref())?,
            ))
        } else {
            Ok(Predicate::literal(false))
        };
    }

    let expr = datafusion_expr_to_kernel_expression(in_list.expr.as_ref())?;
    let predicates = in_list
        .list
        .iter()
        .map(|item| {
            let item = datafusion_in_list_item_to_kernel_expression(item)?;

            if in_list.negated {
                Ok(Predicate::ne(expr.clone(), item))
            } else {
                Ok(Predicate::eq(expr.clone(), item))
            }
        })
        .collect::<Result<Vec<_>, DeltaKernelPredicateAdapterError>>()?;

    if in_list.negated {
        Ok(Predicate::and_from(predicates))
    } else {
        Ok(Predicate::or_from(predicates))
    }
}

fn datafusion_in_list_item_to_kernel_expression(
    expr: &DataFusionExpr,
) -> Result<Expression, DeltaKernelPredicateAdapterError> {
    match expr {
        DataFusionExpr::Literal(value, _) => Ok(Expression::Literal(
            datafusion_scalar_to_kernel_scalar(value)?,
        )),
        _ => Err(DeltaKernelPredicateAdapterError::UnsupportedExpression),
    }
}

fn datafusion_expr_to_kernel_expression(
    expr: &DataFusionExpr,
) -> Result<Expression, DeltaKernelPredicateAdapterError> {
    match expr {
        DataFusionExpr::Column(column) => datafusion_column_to_kernel_expression(column),
        DataFusionExpr::Literal(value, _) => Ok(Expression::Literal(
            datafusion_scalar_to_kernel_scalar(value)?,
        )),
        _ => Err(DeltaKernelPredicateAdapterError::UnsupportedExpression),
    }
}

fn datafusion_column_to_kernel_expression(
    column: &DataFusionColumn,
) -> Result<Expression, DeltaKernelPredicateAdapterError> {
    if column.relation.is_some() || column.name.contains('.') {
        return Err(DeltaKernelPredicateAdapterError::UnsupportedColumnReference);
    }

    Ok(Expression::Column(ColumnName::new([column.name.as_str()])))
}

fn datafusion_scalar_to_kernel_scalar(
    value: &ScalarValue,
) -> Result<Scalar, DeltaKernelPredicateAdapterError> {
    if value.is_null() {
        return Err(DeltaKernelPredicateAdapterError::NullLiteral);
    }

    match value {
        ScalarValue::Boolean(Some(value)) => Ok(Scalar::Boolean(*value)),
        ScalarValue::Int8(Some(value)) => Ok(Scalar::Byte(*value)),
        ScalarValue::Int16(Some(value)) => Ok(Scalar::Short(*value)),
        ScalarValue::Int32(Some(value)) => Ok(Scalar::Integer(*value)),
        ScalarValue::Int64(Some(value)) => Ok(Scalar::Long(*value)),
        ScalarValue::Float32(Some(value)) if value.is_finite() => Ok(Scalar::Float(*value)),
        ScalarValue::Float64(Some(value)) if value.is_finite() => Ok(Scalar::Double(*value)),
        ScalarValue::Date32(Some(value)) => Ok(Scalar::Date(*value)),
        ScalarValue::Decimal128(Some(value), precision, scale) => {
            let scale = u8::try_from(*scale)
                .map_err(|_| DeltaKernelPredicateAdapterError::UnsupportedLiteral)?;
            Scalar::decimal(*value, *precision, scale)
                .map_err(|_| DeltaKernelPredicateAdapterError::UnsupportedLiteral)
        }
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            Ok(Scalar::String(value.clone()))
        }
        ScalarValue::Binary(Some(value))
        | ScalarValue::LargeBinary(Some(value))
        | ScalarValue::FixedSizeBinary(_, Some(value))
            if !value.is_empty() =>
        {
            Ok(Scalar::Binary(value.clone()))
        }
        ScalarValue::TimestampMicrosecond(Some(value), Some(timezone)) if !timezone.is_empty() => {
            Ok(Scalar::Timestamp(*value))
        }
        ScalarValue::TimestampMicrosecond(Some(value), None) => Ok(Scalar::TimestampNtz(*value)),
        _ => Err(DeltaKernelPredicateAdapterError::UnsupportedLiteral),
    }
}

#[cfg(test)]
fn scan_builder_with_predicate_symbol(
    builder: delta_kernel::scan::ScanBuilder,
    predicate: PredicateRef,
) -> delta_kernel::scan::ScanBuilder {
    builder.with_predicate(predicate)
}

#[cfg(test)]
fn scan_builder_with_predicate_and_stats_symbol(
    builder: delta_kernel::scan::ScanBuilder,
    predicate: PredicateRef,
) -> delta_kernel::scan::ScanBuilder {
    builder
        .with_predicate(predicate)
        .include_all_stats_columns()
}

fn feature_names(features: Option<&[TableFeature]>) -> Vec<String> {
    features
        .unwrap_or_default()
        .iter()
        .map(feature_name)
        .collect()
}

fn feature_name(feature: &TableFeature) -> String {
    match feature {
        TableFeature::Unknown(name) => name.clone(),
        _ => feature.as_ref().to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        ArrowEngineData, ColumnName, DefaultEngineBuilder, DeltaKernelPredicate,
        DeltaKernelPredicateAdapterError, DvInfo, EngineDataArrowExt, Expression, Predicate,
        Scalar, Scan, ScanFile, ScanMetadata, Snapshot, SnapshotRef, Version,
        scan_builder_with_predicate_and_stats_symbol, scan_builder_with_predicate_symbol,
        store_from_url_opts, transform_to_logical, try_parse_uri,
    };
    use arrow_tiberius::{MssqlProfile, PlanOptions, plan_arrow_schema_to_mssql_mappings};
    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::{Expr, cast, col, lit};
    use delta_kernel::arrow::datatypes::{DataType, Field, Schema};

    fn convert_datafusion_predicate(
        filter: &Expr,
    ) -> Result<Predicate, DeltaKernelPredicateAdapterError> {
        super::datafusion_expr_to_kernel_predicate(filter)
            .map(|predicate| predicate.as_ref().as_ref().clone())
    }

    fn kernel_column(name: &str) -> Expression {
        Expression::Column(ColumnName::new([name]))
    }

    fn convert_datafusion_scalar(
        value: &ScalarValue,
    ) -> Result<Scalar, DeltaKernelPredicateAdapterError> {
        super::datafusion_scalar_to_kernel_scalar(value)
    }

    fn collect_scan_file(files: &mut Vec<ScanFile>, file: ScanFile) {
        files.push(file);
    }

    fn snapshot_ref_version(snapshot: SnapshotRef) -> Version {
        snapshot.version()
    }

    #[test]
    fn delta_kernel_internal_api_symbols_are_available() {
        let _ = DefaultEngineBuilder::new;
        let _ = super::build_projected_scan;
        let _ = Scan::scan_metadata;
        let _ = ScanMetadata::visit_scan_files::<Vec<ScanFile>>;
        let _ = DvInfo::get_selection_vector;
        let _ = transform_to_logical;
        let _ = ArrowEngineData::new;
        let _ = <Box<dyn delta_kernel::EngineData> as EngineDataArrowExt>::try_into_record_batch;
        let _ = collect_scan_file;
        let _ = snapshot_ref_version;
        let _ = super::snapshot_arrow_schema;
        let _ = super::snapshot_protocol_report;
        let _ = super::TABLE_FEATURES_MIN_READER_VERSION;
        let _ = scan_builder_with_predicate_symbol;
        let _ = scan_builder_with_predicate_and_stats_symbol;
    }

    #[test]
    fn delta_kernel_predicate_api_symbols_are_available() {
        let id_column = Expression::Column(ColumnName::new(["id"]));
        let value = Expression::Literal(Scalar::Integer(7));
        let equality = Predicate::eq(id_column.clone(), value);
        let null_check = Predicate::is_null(id_column.clone());
        let combined = Predicate::and(equality.clone(), Predicate::not(null_check));
        let wrapped = DeltaKernelPredicate::new(Predicate::or(combined, equality));

        let _predicate_ref = wrapped.as_ref();
        let _owned_predicate_ref = wrapped.into_inner();
    }

    #[test]
    fn delta_kernel_predicate_and_from_combines_provider_predicates()
    -> Result<(), Box<dyn std::error::Error>> {
        let region = Predicate::eq(kernel_column("region"), Expression::literal("us-west"));
        let day = Predicate::eq(kernel_column("day"), Expression::literal("2026-05-31"));

        let empty = DeltaKernelPredicate::and_from(Vec::<DeltaKernelPredicate>::new());
        let Some(combined) = DeltaKernelPredicate::and_from([
            DeltaKernelPredicate::new(region.clone()),
            DeltaKernelPredicate::new(day.clone()),
        ]) else {
            return Err("expected combined predicate".into());
        };

        assert!(empty.is_none());
        assert_eq!(
            combined.as_ref().as_ref(),
            &Predicate::and_from(vec![region, day])
        );

        Ok(())
    }

    #[test]
    fn datafusion_predicate_adapter_converts_simple_comparisons() {
        let id = kernel_column("id");

        assert_eq!(
            convert_datafusion_predicate(&col("id").eq(lit(7_i32))),
            Ok(Predicate::eq(
                id.clone(),
                Expression::Literal(Scalar::Integer(7))
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("id").not_eq(lit(7_i64))),
            Ok(Predicate::ne(
                id.clone(),
                Expression::Literal(Scalar::Long(7))
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("id").lt(lit(7_i32))),
            Ok(Predicate::lt(
                id.clone(),
                Expression::Literal(Scalar::Integer(7))
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("id").lt_eq(lit(7_i32))),
            Ok(Predicate::le(
                id.clone(),
                Expression::Literal(Scalar::Integer(7))
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("id").gt(lit(7_i32))),
            Ok(Predicate::gt(
                id.clone(),
                Expression::Literal(Scalar::Integer(7))
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("id").gt_eq(lit(7_i32))),
            Ok(Predicate::ge(id, Expression::Literal(Scalar::Integer(7))))
        );
        assert_eq!(
            convert_datafusion_predicate(
                &col("event_date").eq(Expr::Literal(ScalarValue::Date32(Some(20_454)), None))
            ),
            Ok(Predicate::eq(
                kernel_column("event_date"),
                Expression::Literal(Scalar::Date(20_454))
            ))
        );
    }

    #[test]
    fn datafusion_predicate_adapter_converts_supported_literal_types()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            convert_datafusion_predicate(
                &col("byte_value").eq(Expr::Literal(ScalarValue::Int8(Some(7)), None))
            ),
            Ok(Predicate::eq(
                kernel_column("byte_value"),
                Expression::Literal(Scalar::Byte(7))
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(
                &col("short_value").eq(Expr::Literal(ScalarValue::Int16(Some(7)), None))
            ),
            Ok(Predicate::eq(
                kernel_column("short_value"),
                Expression::Literal(Scalar::Short(7))
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(
                &col("float_value").eq(Expr::Literal(ScalarValue::Float32(Some(7.5)), None))
            ),
            Ok(Predicate::eq(
                kernel_column("float_value"),
                Expression::Literal(Scalar::Float(7.5))
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(
                &col("double_value").eq(Expr::Literal(ScalarValue::Float64(Some(7.5)), None))
            ),
            Ok(Predicate::eq(
                kernel_column("double_value"),
                Expression::Literal(Scalar::Double(7.5))
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("large_string").eq(Expr::Literal(
                ScalarValue::LargeUtf8(Some("value".to_owned())),
                None
            ))),
            Ok(Predicate::eq(
                kernel_column("large_string"),
                Expression::Literal(Scalar::String("value".to_owned()))
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(
                &col("event_date").eq(Expr::Literal(ScalarValue::Date32(Some(20_454)), None))
            ),
            Ok(Predicate::eq(
                kernel_column("event_date"),
                Expression::Literal(Scalar::Date(20_454))
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("amount").eq(Expr::Literal(
                ScalarValue::Decimal128(Some(12_345), 10, 2),
                None
            ))),
            Ok(Predicate::eq(
                kernel_column("amount"),
                Expression::Literal(Scalar::decimal(12_345, 10, 2)?)
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("payload").eq(Expr::Literal(
                ScalarValue::Binary(Some(vec![1, 2, 3])),
                None
            ))),
            Ok(Predicate::eq(
                kernel_column("payload"),
                Expression::Literal(Scalar::Binary(vec![1, 2, 3]))
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("event_ts").eq(Expr::Literal(
                ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), Some("UTC".into())),
                None
            ))),
            Ok(Predicate::eq(
                kernel_column("event_ts"),
                Expression::Literal(Scalar::Timestamp(1_767_225_600_123_456))
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("event_ts_ntz").eq(Expr::Literal(
                ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
                None
            ))),
            Ok(Predicate::eq(
                kernel_column("event_ts_ntz"),
                Expression::Literal(Scalar::TimestampNtz(1_767_225_600_123_456))
            ))
        );

        Ok(())
    }

    #[test]
    fn datafusion_predicate_adapter_documents_scalar_type_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let supported = [
            (ScalarValue::Boolean(Some(true)), Scalar::Boolean(true)),
            (ScalarValue::Int8(Some(7)), Scalar::Byte(7)),
            (ScalarValue::Int16(Some(7)), Scalar::Short(7)),
            (ScalarValue::Int32(Some(7)), Scalar::Integer(7)),
            (ScalarValue::Int64(Some(7)), Scalar::Long(7)),
            (ScalarValue::Float32(Some(7.5)), Scalar::Float(7.5)),
            (ScalarValue::Float64(Some(7.5)), Scalar::Double(7.5)),
            (ScalarValue::Date32(Some(7)), Scalar::Date(7)),
            (
                ScalarValue::Decimal128(Some(12_345), 10, 2),
                Scalar::decimal(12_345, 10, 2)?,
            ),
            (
                ScalarValue::Utf8(Some("value".to_owned())),
                Scalar::String("value".to_owned()),
            ),
            (
                ScalarValue::LargeUtf8(Some("value".to_owned())),
                Scalar::String("value".to_owned()),
            ),
            (
                ScalarValue::Binary(Some(vec![1, 2, 3])),
                Scalar::Binary(vec![1, 2, 3]),
            ),
            (
                ScalarValue::LargeBinary(Some(vec![1, 2, 3])),
                Scalar::Binary(vec![1, 2, 3]),
            ),
            (
                ScalarValue::FixedSizeBinary(3, Some(vec![1, 2, 3])),
                Scalar::Binary(vec![1, 2, 3]),
            ),
            (
                ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), Some("UTC".into())),
                Scalar::Timestamp(1_767_225_600_123_456),
            ),
            (
                ScalarValue::TimestampMicrosecond(
                    Some(1_767_225_600_123_456),
                    Some("America/Phoenix".into()),
                ),
                Scalar::Timestamp(1_767_225_600_123_456),
            ),
            (
                ScalarValue::TimestampMicrosecond(
                    Some(1_767_225_600_123_456),
                    Some("-07:00".into()),
                ),
                Scalar::Timestamp(1_767_225_600_123_456),
            ),
            (
                ScalarValue::TimestampMicrosecond(Some(1_767_225_600_123_456), None),
                Scalar::TimestampNtz(1_767_225_600_123_456),
            ),
        ];

        for (datafusion_scalar, kernel_scalar) in supported {
            assert_eq!(
                convert_datafusion_scalar(&datafusion_scalar),
                Ok(kernel_scalar),
                "{datafusion_scalar:?}"
            );
        }

        let unsupported_literals = [
            ScalarValue::Float32(Some(f32::NAN)),
            ScalarValue::Float32(Some(f32::INFINITY)),
            ScalarValue::Float32(Some(f32::NEG_INFINITY)),
            ScalarValue::Float64(Some(f64::NAN)),
            ScalarValue::Float64(Some(f64::INFINITY)),
            ScalarValue::Float64(Some(f64::NEG_INFINITY)),
            ScalarValue::Decimal32(Some(12345), 10, 2),
            ScalarValue::Decimal64(Some(12345), 10, 2),
            ScalarValue::Decimal256(Some(12345.into()), 10, 2),
            ScalarValue::UInt8(Some(7)),
            ScalarValue::UInt16(Some(7)),
            ScalarValue::UInt32(Some(7)),
            ScalarValue::UInt64(Some(7)),
            ScalarValue::Utf8View(Some("value".to_owned())),
            ScalarValue::BinaryView(Some(vec![1, 2, 3])),
            ScalarValue::Binary(Some(Vec::new())),
            ScalarValue::LargeBinary(Some(Vec::new())),
            ScalarValue::FixedSizeBinary(0, Some(Vec::new())),
            ScalarValue::Date64(Some(7)),
            ScalarValue::Time32Second(Some(7)),
            ScalarValue::Time32Millisecond(Some(7)),
            ScalarValue::Time64Microsecond(Some(7)),
            ScalarValue::Time64Nanosecond(Some(7)),
            ScalarValue::TimestampSecond(Some(7), Some(Arc::from("UTC"))),
            ScalarValue::TimestampMillisecond(Some(7), Some(Arc::from("UTC"))),
            ScalarValue::TimestampMicrosecond(Some(7), Some(Arc::from(""))),
            ScalarValue::TimestampNanosecond(Some(7), Some(Arc::from("UTC"))),
        ];

        for datafusion_scalar in unsupported_literals {
            assert_eq!(
                convert_datafusion_scalar(&datafusion_scalar),
                Err(DeltaKernelPredicateAdapterError::UnsupportedLiteral),
                "{datafusion_scalar:?}"
            );
        }

        let null_literals = [
            ScalarValue::Null,
            ScalarValue::Boolean(None),
            ScalarValue::Int32(None),
            ScalarValue::Utf8(None),
            ScalarValue::Decimal128(None, 10, 2),
            ScalarValue::Binary(None),
            ScalarValue::TimestampMicrosecond(None, None),
        ];

        for datafusion_scalar in null_literals {
            assert_eq!(
                convert_datafusion_scalar(&datafusion_scalar),
                Err(DeltaKernelPredicateAdapterError::NullLiteral),
                "{datafusion_scalar:?}"
            );
        }

        Ok(())
    }

    #[test]
    fn datafusion_predicate_adapter_rejects_unproven_literal_types() {
        let timestamp_empty_timezone = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(12345), Some(Arc::from(""))),
            None,
        );
        let float_nan = Expr::Literal(ScalarValue::Float32(Some(f32::NAN)), None);
        let double_infinity = Expr::Literal(ScalarValue::Float64(Some(f64::INFINITY)), None);
        let decimal256 = Expr::Literal(ScalarValue::Decimal256(Some(12345.into()), 10, 2), None);
        let negative_scale_decimal =
            Expr::Literal(ScalarValue::Decimal128(Some(12345), 10, -2), None);
        let decimal_null = Expr::Literal(ScalarValue::Decimal128(None, 10, 2), None);
        let timestamp_null = Expr::Literal(ScalarValue::TimestampMicrosecond(None, None), None);
        let cast_filter = cast(col("id"), DataType::Int64).eq(lit(7_i64));

        for literal in [
            timestamp_empty_timezone,
            float_nan,
            double_infinity,
            decimal256,
            negative_scale_decimal,
        ] {
            assert_eq!(
                convert_datafusion_predicate(&col("value").eq(literal)),
                Err(DeltaKernelPredicateAdapterError::UnsupportedLiteral)
            );
        }

        for literal in [decimal_null, timestamp_null] {
            assert_eq!(
                convert_datafusion_predicate(&col("value").eq(literal)),
                Err(DeltaKernelPredicateAdapterError::NullLiteral)
            );
        }

        assert_eq!(
            convert_datafusion_predicate(&cast_filter),
            Err(DeltaKernelPredicateAdapterError::UnsupportedExpression)
        );
    }

    #[test]
    fn datafusion_predicate_adapter_converts_boolean_predicates() {
        let id_eq = col("id").eq(lit(7_i32));
        let active_eq = col("active").eq(lit(true));
        let id_predicate = Predicate::eq(kernel_column("id"), Expression::literal(7_i32));
        let active_predicate = Predicate::eq(kernel_column("active"), Expression::literal(true));

        assert_eq!(
            convert_datafusion_predicate(&lit(false)),
            Ok(Predicate::literal(false))
        );
        assert_eq!(
            convert_datafusion_predicate(&id_eq.clone().and(active_eq.clone())),
            Ok(Predicate::and(
                id_predicate.clone(),
                active_predicate.clone()
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(&id_eq.clone().or(active_eq)),
            Ok(Predicate::or(id_predicate.clone(), active_predicate))
        );
        assert_eq!(
            convert_datafusion_predicate(&Expr::Not(Box::new(id_eq))),
            Ok(Predicate::not(id_predicate))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("name").is_null()),
            Ok(Predicate::is_null(kernel_column("name")))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("name").is_not_null()),
            Ok(Predicate::is_not_null(kernel_column("name")))
        );
    }

    #[test]
    fn datafusion_predicate_adapter_converts_in_list_predicates() {
        let part = kernel_column("part");
        let part_eq_a = Predicate::eq(part.clone(), Expression::literal("a"));
        let part_eq_c = Predicate::eq(part.clone(), Expression::literal("c"));
        let part_ne_a = Predicate::ne(part.clone(), Expression::literal("a"));
        let part_ne_c = Predicate::ne(part.clone(), Expression::literal("c"));

        assert_eq!(
            convert_datafusion_predicate(&col("part").in_list(vec![lit("a"), lit("c")], false)),
            Ok(Predicate::or(part_eq_a.clone(), part_eq_c))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("part").in_list(vec![lit("a"), lit("c")], true)),
            Ok(Predicate::and(part_ne_a, part_ne_c))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("part").in_list(vec![lit("a"), lit("a")], false)),
            Ok(Predicate::or(part_eq_a.clone(), part_eq_a))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("part").in_list(Vec::<Expr>::new(), false)),
            Ok(Predicate::literal(false))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("part").in_list(Vec::<Expr>::new(), true)),
            Ok(Predicate::is_not_null(part))
        );
    }

    #[test]
    fn datafusion_predicate_adapter_converts_between_predicates() {
        let score = kernel_column("score");

        assert_eq!(
            convert_datafusion_predicate(&col("score").between(lit(10_i32), lit(20_i32))),
            Ok(Predicate::and(
                Predicate::ge(score.clone(), Expression::literal(10_i32)),
                Predicate::le(score.clone(), Expression::literal(20_i32))
            ))
        );
        assert_eq!(
            convert_datafusion_predicate(&col("score").not_between(lit(10_i32), lit(20_i32))),
            Ok(Predicate::or(
                Predicate::lt(score.clone(), Expression::literal(10_i32)),
                Predicate::gt(score, Expression::literal(20_i32))
            ))
        );
    }

    #[test]
    fn datafusion_predicate_adapter_rejects_unsafe_or_unproven_shapes() {
        let qualified_column = Expr::Column(Column::new(Some("orders"), "id")).eq(lit(7_i32));
        let dotted_column = col("profile.age").eq(lit(7_i32));
        let null_literal = col("id").eq(Expr::Literal(ScalarValue::Int32(None), None));
        let unsigned_literal = col("id").eq(Expr::Literal(ScalarValue::UInt64(Some(7)), None));
        let standalone_column = col("id");
        let in_list_with_null = col("part").in_list(
            vec![lit("a"), Expr::Literal(ScalarValue::Utf8(None), None)],
            false,
        );
        let in_list_with_non_literal = col("part").in_list(vec![col("other_part")], false);

        assert_eq!(
            convert_datafusion_predicate(&qualified_column),
            Err(DeltaKernelPredicateAdapterError::UnsupportedColumnReference)
        );
        assert_eq!(
            convert_datafusion_predicate(&dotted_column),
            Err(DeltaKernelPredicateAdapterError::UnsupportedColumnReference)
        );
        assert_eq!(
            convert_datafusion_predicate(&null_literal),
            Err(DeltaKernelPredicateAdapterError::NullLiteral)
        );
        assert_eq!(
            convert_datafusion_predicate(&unsigned_literal),
            Err(DeltaKernelPredicateAdapterError::UnsupportedLiteral)
        );
        assert_eq!(
            convert_datafusion_predicate(&standalone_column),
            Err(DeltaKernelPredicateAdapterError::UnsupportedExpression)
        );
        assert_eq!(
            convert_datafusion_predicate(&in_list_with_null),
            Err(DeltaKernelPredicateAdapterError::NullLiteral)
        );
        assert_eq!(
            convert_datafusion_predicate(&in_list_with_non_literal),
            Err(DeltaKernelPredicateAdapterError::UnsupportedExpression)
        );
    }

    #[test]
    fn datafusion_predicate_adapter_error_is_a_real_error_type() {
        fn accepts_error(error: &dyn std::error::Error) -> String {
            error.to_string()
        }

        let message = accepts_error(&DeltaKernelPredicateAdapterError::UnsupportedLiteral);

        assert_eq!(message, "unsupported DataFusion literal");
    }

    #[test]
    fn delta_kernel_snapshot_loading_path_is_available() -> delta_kernel::DeltaResult<()> {
        let table_url = try_parse_uri("memory:///")?;
        let store = store_from_url_opts(&table_url, std::iter::empty::<(&str, &str)>())?;
        let engine = DefaultEngineBuilder::new(store).build();

        let result = Snapshot::builder_for(table_url.as_str())
            .at_version(0)
            .build(&engine);

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn arrow_tiberius_accepts_delta_kernel_arrow_schema() -> arrow_tiberius::Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let outcome = plan_arrow_schema_to_mssql_mappings(
            &schema,
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )?;

        assert_eq!(outcome.value().len(), 1);
        Ok(())
    }
}
