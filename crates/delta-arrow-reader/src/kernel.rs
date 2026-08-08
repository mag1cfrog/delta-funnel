//! Private boundary for stability-sensitive `delta_kernel` APIs.

use std::{collections::BTreeMap, sync::Arc};

use arrow::{array::Array as _, datatypes::SchemaRef, error::ArrowError};
use delta_kernel::{
    Engine, Snapshot, SnapshotRef,
    engine::{arrow_conversion::TryIntoArrow, arrow_data::ArrowEngineData},
    expressions::{ColumnName, Expression, ExpressionRef, Predicate, PredicateRef, Scalar},
    scan::ScanMetadata,
    scan::state::{DvInfo, ScanFile},
    scan::{Scan as DeltaKernelScan, StatsOptions},
    table_features::{TABLE_FEATURES_MIN_READER_VERSION, TableFeature},
    try_parse_uri,
};
#[cfg(test)]
pub(crate) use delta_kernel_default_engine::storage::insert_url_handler;
use delta_kernel_default_engine::{DefaultEngineBuilder, storage::store_from_url_opts};
use object_store::ObjectStore;
use url::Url;

use crate::{DeltaComparison, DeltaPredicate, DeltaScalar, DeltaStorageOptions};

#[allow(dead_code)]
pub(crate) const TABLE_FEATURES_READER_VERSION: i32 = TABLE_FEATURES_MIN_READER_VERSION;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeltaKernelProtocol {
    pub(crate) min_reader_version: i32,
    pub(crate) min_writer_version: i32,
    pub(crate) reader_features: Vec<String>,
    pub(crate) writer_features: Vec<String>,
}

pub(crate) fn parse_uri(table_uri: &str) -> delta_kernel::DeltaResult<Url> {
    try_parse_uri(table_uri)
}

/// One parsed table location, object store, and Kernel engine.
pub(crate) struct DeltaKernelEngineContext {
    table_url: Url,
    object_store: Arc<dyn ObjectStore>,
    engine: Arc<dyn Engine + Send + Sync>,
}

#[allow(dead_code)]
pub(crate) struct KernelDeletionVectorHandle(DvInfo);

#[allow(dead_code)]
pub(crate) struct KernelScanFileMetadata {
    pub(crate) path: String,
    pub(crate) size: i64,
    pub(crate) modification_time_ms: Option<i64>,
    pub(crate) estimated_rows: Option<u64>,
    pub(crate) partition_values: BTreeMap<String, String>,
    pub(crate) deletion_vector: Option<KernelDeletionVectorHandle>,
    pub(crate) transform: KernelPhysicalToLogicalTransform,
}

#[allow(dead_code)]
pub(crate) struct KernelPhysicalToLogicalTransform(Option<ExpressionRef>);

#[allow(dead_code)]
pub(crate) struct KernelScan {
    scan: DeltaKernelScan,
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
}

#[allow(dead_code)]
pub(crate) struct KernelScanMetadata {
    pub(crate) files: Vec<KernelScanFileMetadata>,
    pub(crate) files_filtered_during_planning: Option<u64>,
}

#[allow(dead_code)]
impl KernelPhysicalToLogicalTransform {
    pub(crate) fn is_required(&self) -> bool {
        self.0.is_some()
    }

    pub(crate) fn into_inner(self) -> Option<ExpressionRef> {
        self.0
    }
}

#[allow(dead_code)]
impl KernelScan {
    pub(crate) fn logical_schema(&self) -> SchemaRef {
        Arc::clone(&self.logical_schema)
    }

    pub(crate) fn physical_schema(&self) -> SchemaRef {
        Arc::clone(&self.physical_schema)
    }

    pub(crate) fn has_physical_predicate(&self) -> bool {
        self.scan.physical_predicate().is_some()
    }

    pub(crate) fn file_metadata(
        &self,
        engine_context: &DeltaKernelEngineContext,
    ) -> delta_kernel::DeltaResult<KernelScanMetadata> {
        fn collect(files: &mut Vec<KernelScanFileMetadata>, file: ScanFile) {
            files.push(KernelScanFileMetadata::from_scan_file(file));
        }

        let mut files = Vec::new();
        let mut filtered = Some(0_u64);
        let mut saw_batch = false;
        for metadata in self.scan.scan_metadata(engine_context.engine.as_ref())? {
            let metadata = metadata?;
            saw_batch = true;
            filtered = filtered.and_then(|total| {
                files_filtered(&metadata).and_then(|count| total.checked_add(count))
            });
            files = metadata.visit_scan_files(files, collect)?;
        }
        Ok(KernelScanMetadata {
            files,
            files_filtered_during_planning: saw_batch.then_some(filtered).flatten(),
        })
    }
}

fn files_filtered(metadata: &ScanMetadata) -> Option<u64> {
    let data = metadata
        .scan_files
        .data()
        .any_ref()
        .downcast_ref::<ArrowEngineData>()?;
    let paths = data.record_batch().column_by_name("path")?;
    let selection = metadata.scan_files.selection_vector();
    let count = (0..paths.len())
        .filter(|&index| !paths.is_null(index) && !selection.get(index).copied().unwrap_or(true))
        .count();
    u64::try_from(count).ok()
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DeltaKernelPredicate(PredicateRef);

#[allow(dead_code)]
impl DeltaKernelPredicate {
    pub(crate) fn as_ref(&self) -> &PredicateRef {
        &self.0
    }

    pub(crate) fn into_inner(self) -> PredicateRef {
        self.0
    }
}

#[allow(dead_code)]
pub(crate) fn preserve_deletion_vector(dv_info: DvInfo) -> Option<KernelDeletionVectorHandle> {
    dv_info
        .has_vector()
        .then_some(KernelDeletionVectorHandle(dv_info))
}

#[allow(dead_code)]
impl KernelScanFileMetadata {
    pub(crate) fn from_scan_file(file: ScanFile) -> Self {
        let ScanFile {
            path,
            size,
            modification_time,
            stats,
            dv_info,
            transform,
            partition_values,
        } = file;

        Self {
            path,
            size,
            modification_time_ms: Some(modification_time),
            estimated_rows: stats.map(|stats| stats.num_records),
            partition_values: partition_values.into_iter().collect(),
            deletion_vector: preserve_deletion_vector(dv_info),
            transform: KernelPhysicalToLogicalTransform(transform),
        }
    }
}

#[allow(dead_code)]
pub(crate) fn delta_predicate_to_kernel_pruning(
    predicate: &DeltaPredicate,
) -> Option<DeltaKernelPredicate> {
    convert_predicate(predicate)
        .map(|converted| DeltaKernelPredicate(Arc::new(converted.predicate)))
}

struct ConvertedPredicate {
    predicate: Predicate,
    exact: bool,
}

fn convert_predicate(predicate: &DeltaPredicate) -> Option<ConvertedPredicate> {
    let exact = |predicate| {
        Some(ConvertedPredicate {
            predicate,
            exact: true,
        })
    };

    match predicate {
        DeltaPredicate::Boolean(value) => exact(Predicate::literal(*value)),
        DeltaPredicate::Compare { column, op, value } => {
            let column = Expression::Column(ColumnName::new([column.as_str()]));
            let value = Expression::Literal(convert_scalar(value)?);
            exact(match op {
                DeltaComparison::Eq => Predicate::eq(column, value),
                DeltaComparison::NotEq => Predicate::ne(column, value),
                DeltaComparison::Lt => Predicate::lt(column, value),
                DeltaComparison::LtEq => Predicate::le(column, value),
                DeltaComparison::Gt => Predicate::gt(column, value),
                DeltaComparison::GtEq => Predicate::ge(column, value),
            })
        }
        DeltaPredicate::IsNull { column } => {
            exact(Predicate::is_null(Expression::Column(ColumnName::new([
                column.as_str(),
            ]))))
        }
        DeltaPredicate::IsNotNull { column } => exact(Predicate::is_not_null(Expression::Column(
            ColumnName::new([column.as_str()]),
        ))),
        DeltaPredicate::And(children) => convert_and(children),
        DeltaPredicate::Or(children) => {
            let converted = children
                .iter()
                .map(convert_predicate)
                .collect::<Option<Vec<_>>>()?;
            converted
                .iter()
                .all(|predicate| predicate.exact)
                .then(|| ConvertedPredicate {
                    predicate: Predicate::or_from(
                        converted.into_iter().map(|predicate| predicate.predicate),
                    ),
                    exact: true,
                })
        }
        DeltaPredicate::Not(child) => {
            let child = convert_predicate(child)?;
            child.exact.then(|| ConvertedPredicate {
                predicate: Predicate::not(child.predicate),
                exact: true,
            })
        }
    }
}

fn convert_and(children: &[DeltaPredicate]) -> Option<ConvertedPredicate> {
    if children.is_empty() {
        return Some(ConvertedPredicate {
            predicate: Predicate::literal(true),
            exact: true,
        });
    }

    let mut exact = true;
    let converted = children
        .iter()
        .filter_map(|child| match convert_predicate(child) {
            Some(converted) => {
                exact &= converted.exact;
                Some(converted.predicate)
            }
            None => {
                exact = false;
                None
            }
        })
        .collect::<Vec<_>>();

    (!converted.is_empty()).then(|| ConvertedPredicate {
        predicate: Predicate::and_from(converted),
        exact,
    })
}

fn convert_scalar(scalar: &DeltaScalar) -> Option<Scalar> {
    match scalar {
        DeltaScalar::Boolean(value) => Some(Scalar::Boolean(*value)),
        DeltaScalar::Int8(value) => Some(Scalar::Byte(*value)),
        DeltaScalar::Int16(value) => Some(Scalar::Short(*value)),
        DeltaScalar::Int32(value) => Some(Scalar::Integer(*value)),
        DeltaScalar::Int64(value) => Some(Scalar::Long(*value)),
        // Arrow comparisons distinguish signed zero, while Kernel metadata scalars do not.
        DeltaScalar::Float32(value) if value.is_finite() && *value != 0.0 => {
            Some(Scalar::Float(*value))
        }
        DeltaScalar::Float64(value) if value.is_finite() && *value != 0.0 => {
            Some(Scalar::Double(*value))
        }
        DeltaScalar::Float32(_) | DeltaScalar::Float64(_) => None,
        DeltaScalar::Date32(value) => Some(Scalar::Date(*value)),
        DeltaScalar::Decimal128 {
            value,
            precision,
            scale,
        } => Scalar::decimal(*value, *precision, u8::try_from(*scale).ok()?).ok(),
        DeltaScalar::Utf8(value) | DeltaScalar::LargeUtf8(value) => {
            Some(Scalar::String(value.clone()))
        }
        DeltaScalar::Binary(value)
        | DeltaScalar::LargeBinary(value)
        | DeltaScalar::FixedSizeBinary { value, .. } => Some(Scalar::Binary(value.clone())),
        DeltaScalar::TimestampMicrosecond {
            value,
            timezone: Some(_),
        } => Some(Scalar::Timestamp(*value)),
        DeltaScalar::TimestampMicrosecond {
            value,
            timezone: None,
        } => Some(Scalar::TimestampNtz(*value)),
    }
}

impl DeltaKernelEngineContext {
    pub(crate) fn build(
        table_url: Url,
        storage_options: &DeltaStorageOptions,
    ) -> delta_kernel::DeltaResult<Self> {
        let object_store = store_from_url_opts(
            &table_url,
            storage_options
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )?;
        let engine = Arc::new(DefaultEngineBuilder::new(Arc::clone(&object_store)).build());

        Ok(Self {
            table_url,
            object_store,
            engine,
        })
    }

    pub(crate) fn table_url(&self) -> &Url {
        &self.table_url
    }

    #[allow(dead_code)]
    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }

    pub(crate) fn load_snapshot(
        &self,
        version: Option<u64>,
    ) -> delta_kernel::DeltaResult<KernelSnapshot> {
        let mut builder = Snapshot::builder_for(self.table_url.clone());
        if let Some(version) = version {
            builder = builder.at_version(version);
        }
        builder.build(self.engine.as_ref()).map(KernelSnapshot)
    }

    #[allow(dead_code)]
    pub(crate) fn load_deletion_vector_row_indexes(
        &self,
        deletion_vector: &KernelDeletionVectorHandle,
    ) -> delta_kernel::DeltaResult<Vec<u64>> {
        deletion_vector
            .0
            .get_row_indexes(self.engine.as_ref(), &self.table_url)
            .map(Option::unwrap_or_default)
    }
}

#[derive(Clone)]
pub(crate) struct KernelSnapshot(SnapshotRef);

impl KernelSnapshot {
    pub(crate) fn version(&self) -> u64 {
        self.0.version()
    }

    pub(crate) fn build_scan(
        &self,
        projection: Option<&[String]>,
        predicate: Option<DeltaKernelPredicate>,
        include_stats: bool,
    ) -> delta_kernel::DeltaResult<KernelScan> {
        let logical_schema = match projection {
            Some(names) => self.0.schema().project(names)?,
            None => self.0.schema(),
        };
        let mut builder = Arc::clone(&self.0)
            .scan_builder()
            .with_schema(logical_schema)
            .with_predicate(predicate.map(DeltaKernelPredicate::into_inner));
        if include_stats {
            builder = builder.with_stats(StatsOptions::all());
        }
        let scan = builder.build()?;
        let logical_schema = Arc::new(scan.logical_schema().as_ref().try_into_arrow()?);
        let physical_schema = Arc::new(scan.physical_schema().as_ref().try_into_arrow()?);

        Ok(KernelScan {
            scan,
            logical_schema,
            physical_schema,
        })
    }
}

pub(crate) fn snapshot_protocol_report(snapshot: &KernelSnapshot) -> DeltaKernelProtocol {
    let protocol = snapshot.0.table_configuration().protocol();

    DeltaKernelProtocol {
        min_reader_version: protocol.min_reader_version(),
        min_writer_version: protocol.min_writer_version(),
        reader_features: feature_names(protocol.reader_features()),
        writer_features: feature_names(protocol.writer_features()),
    }
}

pub(crate) fn snapshot_arrow_schema(snapshot: &KernelSnapshot) -> Result<SchemaRef, ArrowError> {
    snapshot.0.schema().as_ref().try_into_arrow().map(Arc::new)
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
pub(crate) fn is_kernel_error(error: &(dyn std::error::Error + 'static)) -> bool {
    error.downcast_ref::<delta_kernel::Error>().is_some()
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{BooleanArray, Float64Array, Int32Array, RecordBatch},
        compute::filter_record_batch,
    };
    use delta_kernel::{
        EvaluationHandler,
        engine::{
            arrow_conversion::TryFromArrow, arrow_data::ArrowEngineData,
            arrow_expression::ArrowEvaluationHandler,
        },
        schema::StructType,
    };

    use super::*;
    use crate::predicate::evaluate_predicate;

    fn column(name: &str) -> Expression {
        Expression::Column(ColumnName::new([name]))
    }

    fn compare(column: &str, op: DeltaComparison, value: DeltaScalar) -> DeltaPredicate {
        DeltaPredicate::Compare {
            column: column.to_owned(),
            op,
            value,
        }
    }

    fn converted(predicate: &DeltaPredicate) -> Option<Predicate> {
        delta_predicate_to_kernel_pruning(predicate)
            .map(|predicate| predicate.into_inner().as_ref().clone())
    }

    fn apply_kernel_pruning(
        batch: &RecordBatch,
        predicate: &DeltaKernelPredicate,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let schema = StructType::try_from_arrow(batch.schema())?;
        let evaluator = ArrowEvaluationHandler
            .new_predicate_evaluator(schema.into(), Arc::clone(predicate.as_ref()))?;
        let selection = ArrowEngineData::try_from_engine_data(
            evaluator.evaluate(&ArrowEngineData::new(batch.clone()))?,
        )?;
        let selection = selection
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("Kernel predicate evaluator must return Boolean");

        Ok(filter_record_batch(batch, selection)?)
    }

    #[test]
    fn converts_exact_predicate_shapes() {
        let id = column("id");
        let seven = Expression::Literal(Scalar::Integer(7));
        let comparisons = [
            (
                DeltaComparison::Eq,
                Predicate::eq(id.clone(), seven.clone()),
            ),
            (
                DeltaComparison::NotEq,
                Predicate::ne(id.clone(), seven.clone()),
            ),
            (
                DeltaComparison::Lt,
                Predicate::lt(id.clone(), seven.clone()),
            ),
            (
                DeltaComparison::LtEq,
                Predicate::le(id.clone(), seven.clone()),
            ),
            (
                DeltaComparison::Gt,
                Predicate::gt(id.clone(), seven.clone()),
            ),
            (
                DeltaComparison::GtEq,
                Predicate::ge(id.clone(), seven.clone()),
            ),
        ];

        for (op, expected) in comparisons {
            assert_eq!(
                converted(&compare("id", op, DeltaScalar::Int32(7))),
                Some(expected)
            );
        }

        assert_eq!(
            converted(&DeltaPredicate::IsNull {
                column: "id".to_owned(),
            }),
            Some(Predicate::is_null(id.clone()))
        );
        assert_eq!(
            converted(&DeltaPredicate::IsNotNull {
                column: "id".to_owned(),
            }),
            Some(Predicate::is_not_null(id.clone()))
        );
        assert_eq!(
            converted(&DeltaPredicate::Not(Box::new(DeltaPredicate::Boolean(
                true
            )))),
            Some(Predicate::not(Predicate::literal(true)))
        );
        assert_eq!(
            converted(&DeltaPredicate::Or(vec![
                DeltaPredicate::Boolean(false),
                DeltaPredicate::IsNull {
                    column: "id".to_owned(),
                },
            ])),
            Some(Predicate::or(
                Predicate::literal(false),
                Predicate::is_null(id)
            ))
        );
        assert_eq!(
            converted(&DeltaPredicate::And(vec![])),
            Some(Predicate::literal(true))
        );
        assert_eq!(
            converted(&DeltaPredicate::Or(vec![])),
            Some(Predicate::literal(false))
        );
    }

    #[test]
    fn converts_only_kernel_safe_scalars() {
        let scalars = [
            (DeltaScalar::Boolean(true), Scalar::Boolean(true)),
            (DeltaScalar::Int8(1), Scalar::Byte(1)),
            (DeltaScalar::Int16(2), Scalar::Short(2)),
            (DeltaScalar::Int32(3), Scalar::Integer(3)),
            (DeltaScalar::Int64(4), Scalar::Long(4)),
            (DeltaScalar::Float32(1.5), Scalar::Float(1.5)),
            (DeltaScalar::Float64(-2.5), Scalar::Double(-2.5)),
            (DeltaScalar::Date32(20_454), Scalar::Date(20_454)),
            (
                DeltaScalar::Decimal128 {
                    value: 12_345,
                    precision: 10,
                    scale: 2,
                },
                Scalar::decimal(12_345, 10, 2).expect("valid decimal"),
            ),
            (
                DeltaScalar::Utf8("value".to_owned()),
                Scalar::String("value".to_owned()),
            ),
            (
                DeltaScalar::LargeUtf8(String::new()),
                Scalar::String(String::new()),
            ),
            (DeltaScalar::Binary(vec![1, 2]), Scalar::Binary(vec![1, 2])),
            (DeltaScalar::LargeBinary(vec![]), Scalar::Binary(vec![])),
            (
                DeltaScalar::FixedSizeBinary {
                    size: 2,
                    value: vec![3, 4],
                },
                Scalar::Binary(vec![3, 4]),
            ),
            (
                DeltaScalar::TimestampMicrosecond {
                    value: 123,
                    timezone: Some("UTC".to_owned()),
                },
                Scalar::Timestamp(123),
            ),
            (
                DeltaScalar::TimestampMicrosecond {
                    value: 456,
                    timezone: None,
                },
                Scalar::TimestampNtz(456),
            ),
        ];

        for (logical, kernel) in scalars {
            assert_eq!(convert_scalar(&logical), Some(kernel));
        }

        assert_eq!(convert_scalar(&DeltaScalar::Float32(0.0)), None);
        assert_eq!(convert_scalar(&DeltaScalar::Float64(-0.0)), None);
        assert_eq!(convert_scalar(&DeltaScalar::Float64(f64::NAN)), None);
        assert_eq!(
            convert_scalar(&DeltaScalar::Decimal128 {
                value: 1,
                precision: 3,
                scale: -1,
            }),
            None
        );
    }

    #[test]
    fn partial_conversion_is_limited_to_safe_and_conjuncts() {
        let safe = compare("id", DeltaComparison::Gt, DeltaScalar::Int32(1));
        let unsupported = compare("score", DeltaComparison::NotEq, DeltaScalar::Float64(0.0));
        let expected = Predicate::gt(column("id"), Expression::Literal(Scalar::Integer(1)));

        assert_eq!(
            converted(&DeltaPredicate::And(vec![
                safe.clone(),
                unsupported.clone()
            ])),
            Some(expected)
        );
        assert_eq!(
            converted(&DeltaPredicate::Or(vec![safe.clone(), unsupported.clone()])),
            None
        );
        assert_eq!(
            converted(&DeltaPredicate::Not(Box::new(DeltaPredicate::And(vec![
                safe,
                unsupported.clone()
            ])))),
            None
        );
        assert_eq!(converted(&unsupported), None);
    }

    #[test]
    fn kernel_pruning_never_replaces_the_logical_residual() -> Result<(), Box<dyn std::error::Error>>
    {
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![0, 2, 3, 4])) as _),
            (
                "score",
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    Some(-0.0),
                    Some(0.0),
                    None,
                ])) as _,
            ),
        ])?;
        let safe = compare("id", DeltaComparison::Gt, DeltaScalar::Int32(1));
        let unsupported = compare("score", DeltaComparison::NotEq, DeltaScalar::Float64(0.0));
        let predicates = [
            safe.clone(),
            DeltaPredicate::And(vec![safe, unsupported.clone()]),
            unsupported,
        ];

        for predicate in predicates {
            let without_pruning = evaluate_predicate(&batch, &predicate)?;
            let candidates = match delta_predicate_to_kernel_pruning(&predicate) {
                Some(kernel) => apply_kernel_pruning(&batch, &kernel)?,
                None => batch.clone(),
            };
            let with_pruning = evaluate_predicate(&candidates, &predicate)?;

            assert_eq!(with_pruning, without_pruning);
        }

        Ok(())
    }
}
