//! Private boundary for stability-sensitive `delta_kernel` APIs.

#![allow(unused_imports)]

use std::sync::Arc;

use datafusion::common::{Column as DataFusionColumn, ScalarValue};
use datafusion::logical_expr::{Expr as DataFusionExpr, Operator as DataFusionOperator};
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
pub(crate) use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::arrow::error::ArrowError;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
pub(crate) use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
pub(crate) use delta_kernel::engine::default::DefaultEngineBuilder;
pub(crate) use delta_kernel::engine::default::storage::store_from_url_opts;
pub(crate) use delta_kernel::expressions::{
    ColumnName, Expression, Predicate, PredicateRef, Scalar,
};
pub(crate) use delta_kernel::scan::Scan;
pub(crate) use delta_kernel::scan::ScanMetadata;
pub(crate) use delta_kernel::scan::state::{DvInfo, ScanFile, transform_to_logical};
pub(crate) use delta_kernel::schema::SchemaRef as KernelSchemaRef;
pub(crate) use delta_kernel::table_features::TABLE_FEATURES_MIN_READER_VERSION;
use delta_kernel::table_features::TableFeature;
pub(crate) use delta_kernel::{Snapshot, SnapshotRef, Version, try_parse_uri};
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

/// Private wrapper around an official `delta_kernel` predicate.
#[allow(dead_code)]
#[derive(Clone, Debug)]
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
    match value {
        ScalarValue::Boolean(Some(value)) => Ok(Scalar::Boolean(*value)),
        ScalarValue::Int8(Some(value)) => Ok(Scalar::Byte(*value)),
        ScalarValue::Int16(Some(value)) => Ok(Scalar::Short(*value)),
        ScalarValue::Int32(Some(value)) => Ok(Scalar::Integer(*value)),
        ScalarValue::Int64(Some(value)) => Ok(Scalar::Long(*value)),
        ScalarValue::Float32(Some(value)) => Ok(Scalar::Float(*value)),
        ScalarValue::Float64(Some(value)) => Ok(Scalar::Double(*value)),
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            Ok(Scalar::String(value.clone()))
        }
        ScalarValue::Null
        | ScalarValue::Boolean(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None) => Err(DeltaKernelPredicateAdapterError::NullLiteral),
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
    use super::{
        ArrowEngineData, ColumnName, DefaultEngineBuilder, DeltaKernelPredicate,
        DeltaKernelPredicateAdapterError, DvInfo, EngineDataArrowExt, Expression, Predicate,
        Scalar, Scan, ScanFile, ScanMetadata, Snapshot, SnapshotRef, Version,
        scan_builder_with_predicate_and_stats_symbol, scan_builder_with_predicate_symbol,
        store_from_url_opts, transform_to_logical, try_parse_uri,
    };
    use arrow_tiberius::{MssqlProfile, PlanOptions, plan_arrow_schema_to_mssql_mappings};
    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::{Expr, col, lit};
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
    }

    #[test]
    fn datafusion_predicate_adapter_converts_boolean_predicates() {
        let id_eq = col("id").eq(lit(7_i32));
        let active_eq = col("active").eq(lit(true));
        let id_predicate = Predicate::eq(kernel_column("id"), Expression::literal(7_i32));
        let active_predicate = Predicate::eq(kernel_column("active"), Expression::literal(true));

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
    fn datafusion_predicate_adapter_rejects_unsafe_or_unproven_shapes() {
        let qualified_column = Expr::Column(Column::new(Some("orders"), "id")).eq(lit(7_i32));
        let dotted_column = col("profile.age").eq(lit(7_i32));
        let null_literal = col("id").eq(Expr::Literal(ScalarValue::Int32(None), None));
        let unsigned_literal = col("id").eq(Expr::Literal(ScalarValue::UInt64(Some(7)), None));
        let standalone_column = col("id");

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
