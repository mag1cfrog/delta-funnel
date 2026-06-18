//! Dynamic partition pruning decisions for retained physical filters.
//!
//! This module is intentionally provider-local. It snapshots a retained
//! DataFusion dynamic physical filter, materializes one synthetic provider row
//! from Delta partition metadata, and asks DataFusion to evaluate that snapshot.
//! The evaluator only returns `Prune` when the snapshot proves the file task
//! cannot satisfy the filter. Every missing, unsupported, incomplete, or failed
//! case is a structured conservative `Keep` decision.

use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, new_null_array};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::Literal;

use crate::query_engine::datafusion::planning::dynamic_filters::DeltaRetainedDynamicFilter;
use crate::query_engine::datafusion::planning::file_task::DeltaScanFileTask;
use crate::table_formats::{
    DeltaKernelPartitionScalarAdapterError, arrow_partition_type_to_kernel_primitive,
    kernel_partition_scalar_to_datafusion_scalar,
};

/// Conservative pruning decision for one retained dynamic filter and file task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeltaDynamicPartitionPruningDecision {
    /// The dynamic snapshot evaluated to boolean false for this partition row.
    Prune(DeltaDynamicPartitionPruneReason),
    /// The file task must remain because pruning was not proven.
    Keep(DeltaDynamicPartitionKeepReason),
}

/// Reason a file task can be removed before data-file scheduling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeltaDynamicPartitionPruneReason {
    /// DataFusion evaluated the dynamic snapshot to false for the partition row.
    FilterRejectedPartition,
}

/// Reason a file task must remain after dynamic partition evaluation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeltaDynamicPartitionKeepReason {
    /// The snapshot evaluated to true for this partition row.
    FilterAllowedPartition,
    /// The dynamic filter is still at DataFusion's literal-true placeholder.
    SnapshotPlaceholder,
    /// DataFusion could not produce a complete snapshot.
    SnapshotUnavailable,
    /// A retained partition column does not match the provider schema anymore.
    PartitionMetadataInvalid,
    /// A required partition value was absent from Delta file metadata.
    PartitionValueMissing,
    /// A partition value could not be parsed according to Delta protocol rules.
    PartitionValueUnparseable,
    /// The partition field uses a type this evaluator does not support.
    UnsupportedPartitionType,
    /// DataFusion expression evaluation failed, so the file must be kept.
    EvaluationFailed,
    /// The snapshot did not produce a boolean result.
    NonBooleanResult,
    /// SQL three-valued logic produced null, which cannot prove pruning.
    NullResult,
}

/// Evaluates one retained dynamic filter against one file task's partitions.
///
/// The retained expression uses provider output column indexes. To preserve
/// those indexes, this builds a one-row batch with the full provider schema and
/// fills unreferenced columns with null arrays. Only retained partition columns
/// receive parsed values from Delta metadata.
#[must_use]
pub(crate) fn evaluate_dynamic_partition_filter(
    filter: &DeltaRetainedDynamicFilter,
    task: &DeltaScanFileTask,
) -> DeltaDynamicPartitionPruningDecision {
    let snapshot = match filter.physical_expr.snapshot() {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            return DeltaDynamicPartitionPruningDecision::Keep(
                DeltaDynamicPartitionKeepReason::SnapshotUnavailable,
            );
        }
        Err(_) => {
            return DeltaDynamicPartitionPruningDecision::Keep(
                DeltaDynamicPartitionKeepReason::EvaluationFailed,
            );
        }
    };
    let is_literal_true_placeholder = is_literal_true(snapshot.as_ref());

    let batch = match materialize_partition_batch(filter, task) {
        Ok(batch) => batch,
        Err(reason) => return DeltaDynamicPartitionPruningDecision::Keep(reason),
    };

    match snapshot.evaluate(&batch) {
        Ok(value) => boolean_decision(value, is_literal_true_placeholder),
        Err(_) => DeltaDynamicPartitionPruningDecision::Keep(
            DeltaDynamicPartitionKeepReason::EvaluationFailed,
        ),
    }
}

/// Builds the one-row Arrow input batch used for dynamic filter evaluation.
///
/// DataFusion physical expressions address fields by provider output index, so
/// this batch must have the same number and order of columns as the provider
/// schema. Only retained partition columns are populated from Delta file
/// metadata. Every other column is represented as a typed null array because
/// the evaluator is not allowed to inspect row data and #183 already proved
/// retained filters only reference partition columns.
///
/// The returned batch is synthetic: it is not a data-file row, only the
/// partition metadata view for one `DeltaScanFileTask`.
fn materialize_partition_batch(
    filter: &DeltaRetainedDynamicFilter,
    task: &DeltaScanFileTask,
) -> Result<RecordBatch, DeltaDynamicPartitionKeepReason> {
    // Start with a full-width provider batch so physical column indexes in the
    // snapshot remain valid. Unreferenced data columns are intentionally null
    // and should never affect evaluation for a retained partition-only filter.
    let mut columns = filter
        .provider_schema
        .fields()
        .iter()
        .map(|field| new_null_array(field.data_type(), 1))
        .collect::<Vec<_>>();

    for partition_column in &filter.partition_columns {
        // Revalidate the retained index/name pair before reading metadata. This
        // keeps a stale retained filter from silently using the wrong field.
        let Some(field) = filter.provider_schema.fields().get(partition_column.index) else {
            return Err(DeltaDynamicPartitionKeepReason::PartitionMetadataInvalid);
        };
        if field.name() != &partition_column.name {
            return Err(DeltaDynamicPartitionKeepReason::PartitionMetadataInvalid);
        }

        let Some(raw_value) = task.partition_values.get(&partition_column.name) else {
            return Err(DeltaDynamicPartitionKeepReason::PartitionValueMissing);
        };
        // Delta stores partition values as protocol strings in file metadata.
        // Parse them back to Arrow scalar values using the provider field type
        // before DataFusion evaluates the physical expression.
        let scalar = parse_partition_scalar(field, raw_value)?;
        columns[partition_column.index] = scalar
            .to_array_of_size(1)
            .map_err(|_| DeltaDynamicPartitionKeepReason::EvaluationFailed)?;
    }

    RecordBatch::try_new(nullable_schema(&filter.provider_schema), columns)
        .map_err(|_| DeltaDynamicPartitionKeepReason::EvaluationFailed)
}

/// Returns a schema shape suitable for a synthetic metadata row.
///
/// Provider schemas can contain non-nullable data columns, but this evaluator
/// fills non-partition columns with null arrays. Marking the synthetic schema
/// nullable preserves names, types, metadata, and indexes while allowing the
/// placeholder nulls needed for DataFusion expression evaluation.
fn nullable_schema(schema: &SchemaRef) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone().with_nullable(true))
        .collect::<Vec<_>>();

    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Parses one Delta partition metadata string into a DataFusion scalar.
///
/// Delta file metadata stores partition values as strings, while DataFusion
/// physical expressions compare Arrow-typed values. This function uses Delta
/// Kernel's protocol parser first so date, timestamp, decimal, binary, empty
/// string, and boolean formatting follow the same source of truth as static
/// partition pushdown. The parsed Kernel scalar is then converted into the
/// exact Arrow scalar shape required by the provider field.
fn parse_partition_scalar(
    field: &Field,
    raw_value: &str,
) -> Result<ScalarValue, DeltaDynamicPartitionKeepReason> {
    let Some(primitive_type) = arrow_partition_type_to_kernel_primitive(field.data_type()) else {
        return Err(DeltaDynamicPartitionKeepReason::UnsupportedPartitionType);
    };
    let scalar = primitive_type
        .parse_scalar(raw_value)
        .map_err(|_| DeltaDynamicPartitionKeepReason::PartitionValueUnparseable)?;

    kernel_partition_scalar_to_datafusion_scalar(scalar, field.data_type()).map_err(|err| match err
    {
        DeltaKernelPartitionScalarAdapterError::UnsupportedArrowType => {
            DeltaDynamicPartitionKeepReason::UnsupportedPartitionType
        }
        DeltaKernelPartitionScalarAdapterError::UnsupportedScalarValue => {
            DeltaDynamicPartitionKeepReason::PartitionValueUnparseable
        }
    })
}

/// Converts a DataFusion expression result into a conservative pruning decision.
///
/// The only pruning proof is boolean false. Boolean true keeps the file because
/// the partition may contain matching rows. SQL null keeps the file because the
/// filter did not prove rejection. Non-boolean outputs are treated as evaluator
/// incompatibilities and also keep the file.
fn boolean_decision(
    value: ColumnarValue,
    is_literal_true_placeholder: bool,
) -> DeltaDynamicPartitionPruningDecision {
    match value {
        // DataFusion dynamic filters start as literal true before their
        // producer publishes a real predicate. Treat that placeholder as a
        // distinct keep reason so later diagnostics can separate "not ready"
        // from "real predicate allowed this partition".
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) if is_literal_true_placeholder => {
            DeltaDynamicPartitionPruningDecision::Keep(
                DeltaDynamicPartitionKeepReason::SnapshotPlaceholder,
            )
        }
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
            DeltaDynamicPartitionPruningDecision::Keep(
                DeltaDynamicPartitionKeepReason::FilterAllowedPartition,
            )
        }
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))) => {
            DeltaDynamicPartitionPruningDecision::Prune(
                DeltaDynamicPartitionPruneReason::FilterRejectedPartition,
            )
        }
        ColumnarValue::Scalar(ScalarValue::Boolean(None) | ScalarValue::Null) => {
            DeltaDynamicPartitionPruningDecision::Keep(DeltaDynamicPartitionKeepReason::NullResult)
        }
        ColumnarValue::Scalar(_) => DeltaDynamicPartitionPruningDecision::Keep(
            DeltaDynamicPartitionKeepReason::NonBooleanResult,
        ),
        ColumnarValue::Array(array) => boolean_array_decision(array.as_ref()),
    }
}

/// Handles array results from DataFusion physical expression evaluation.
///
/// The synthetic batch has exactly one row, so any array result must be a
/// one-element boolean array. Other array shapes indicate an expression or
/// evaluator mismatch and cannot be used to prune.
fn boolean_array_decision(array: &dyn Array) -> DeltaDynamicPartitionPruningDecision {
    let Some(boolean_array) = array.as_any().downcast_ref::<BooleanArray>() else {
        return DeltaDynamicPartitionPruningDecision::Keep(
            DeltaDynamicPartitionKeepReason::NonBooleanResult,
        );
    };
    if boolean_array.len() != 1 {
        return DeltaDynamicPartitionPruningDecision::Keep(
            DeltaDynamicPartitionKeepReason::NonBooleanResult,
        );
    }
    if boolean_array.is_null(0) {
        return DeltaDynamicPartitionPruningDecision::Keep(
            DeltaDynamicPartitionKeepReason::NullResult,
        );
    }
    if boolean_array.value(0) {
        DeltaDynamicPartitionPruningDecision::Keep(
            DeltaDynamicPartitionKeepReason::FilterAllowedPartition,
        )
    } else {
        DeltaDynamicPartitionPruningDecision::Prune(
            DeltaDynamicPartitionPruneReason::FilterRejectedPartition,
        )
    }
}

/// Detects DataFusion's dynamic-filter placeholder snapshot.
///
/// `DynamicFilterPhysicalExpr` snapshots to its current expression. Before the
/// producer has published a selective predicate, that current expression is the
/// literal true placeholder installed by #183. Literal false is not a
/// placeholder and remains a valid pruning proof.
fn is_literal_true(expr: &dyn datafusion::physical_plan::PhysicalExpr) -> bool {
    expr.as_any()
        .downcast_ref::<Literal>()
        .is_some_and(|literal| matches!(literal.value(), ScalarValue::Boolean(Some(true))))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, Column, DynamicFilterPhysicalExpr, in_list, lit,
    };
    use datafusion::physical_plan::PhysicalExpr;

    use super::*;
    use crate::query_engine::datafusion::planning::dynamic_filters::DeltaDynamicFilterPlan;
    use crate::table_formats::{
        KernelPhysicalToLogicalTransform, KernelScanDeletionVectorMetadata,
    };

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("event_year", DataType::Int32, true),
            Field::new("event_date", DataType::Date32, true),
        ]))
    }

    fn column(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    fn retained_filter(
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<(Arc<DynamicFilterPhysicalExpr>, DeltaRetainedDynamicFilter), String> {
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(children, lit(true)));
        let filter: Arc<dyn PhysicalExpr> = dynamic.clone();
        let plan = DeltaDynamicFilterPlan::from_filters(
            std::slice::from_ref(&filter),
            &test_schema(),
            &[
                "region".to_owned(),
                "event_year".to_owned(),
                "event_date".to_owned(),
            ],
        );
        let retained = plan
            .accepted_filters
            .first()
            .cloned()
            .ok_or_else(|| "dynamic filter was not retained".to_owned())?;

        Ok((dynamic, retained))
    }

    fn file_task(partition_values: &[(&str, &str)]) -> DeltaScanFileTask {
        DeltaScanFileTask {
            source_name: "orders".to_owned(),
            table_uri: "file:///tmp/orders".to_owned(),
            snapshot_version: 1,
            path: "part-000.parquet".to_owned(),
            estimated_bytes: None,
            estimated_rows: None,
            modification_time_ms: None,
            partition_values: partition_values
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect::<BTreeMap<_, _>>(),
            stats: None,
            deletion_vector: KernelScanDeletionVectorMetadata::NotPresent,
            transform: KernelPhysicalToLogicalTransform::NotRequired,
        }
    }

    #[test]
    fn equality_snapshot_prunes_nonmatching_string_partition() -> Result<(), String> {
        let (dynamic, retained) = retained_filter(vec![column("region", 1)])?;
        dynamic
            .update(Arc::new(BinaryExpr::new(
                column("region", 1),
                Operator::Eq,
                lit("us-west"),
            )))
            .map_err(|err| err.to_string())?;

        let decision =
            evaluate_dynamic_partition_filter(&retained, &file_task(&[("region", "us-east")]));

        assert_eq!(
            decision,
            DeltaDynamicPartitionPruningDecision::Prune(
                DeltaDynamicPartitionPruneReason::FilterRejectedPartition
            )
        );
        Ok(())
    }

    #[test]
    fn equality_snapshot_keeps_matching_string_partition() -> Result<(), String> {
        let (dynamic, retained) = retained_filter(vec![column("region", 1)])?;
        dynamic
            .update(Arc::new(BinaryExpr::new(
                column("region", 1),
                Operator::Eq,
                lit("us-west"),
            )))
            .map_err(|err| err.to_string())?;

        let decision =
            evaluate_dynamic_partition_filter(&retained, &file_task(&[("region", "us-west")]));

        assert_eq!(
            decision,
            DeltaDynamicPartitionPruningDecision::Keep(
                DeltaDynamicPartitionKeepReason::FilterAllowedPartition
            )
        );
        Ok(())
    }

    #[test]
    fn in_list_snapshot_evaluates_partition_value() -> Result<(), String> {
        let (dynamic, retained) = retained_filter(vec![column("region", 1)])?;
        let snapshot = in_list(
            column("region", 1),
            vec![lit("us-west"), lit("us-central")],
            &false,
            test_schema().as_ref(),
        )
        .map_err(|err| err.to_string())?;
        dynamic.update(snapshot).map_err(|err| err.to_string())?;

        let decision =
            evaluate_dynamic_partition_filter(&retained, &file_task(&[("region", "us-east")]));

        assert_eq!(
            decision,
            DeltaDynamicPartitionPruningDecision::Prune(
                DeltaDynamicPartitionPruneReason::FilterRejectedPartition
            )
        );
        Ok(())
    }

    #[test]
    fn multi_column_snapshot_uses_all_referenced_partitions() -> Result<(), String> {
        let (dynamic, retained) =
            retained_filter(vec![column("region", 1), column("event_year", 2)])?;
        let region_match = Arc::new(BinaryExpr::new(
            column("region", 1),
            Operator::Eq,
            lit("us-west"),
        ));
        let year_match = Arc::new(BinaryExpr::new(
            column("event_year", 2),
            Operator::Eq,
            lit(2026_i32),
        ));
        dynamic
            .update(Arc::new(BinaryExpr::new(
                region_match,
                Operator::And,
                year_match,
            )))
            .map_err(|err| err.to_string())?;

        let decision = evaluate_dynamic_partition_filter(
            &retained,
            &file_task(&[("region", "us-west"), ("event_year", "2025")]),
        );

        assert_eq!(
            decision,
            DeltaDynamicPartitionPruningDecision::Prune(
                DeltaDynamicPartitionPruneReason::FilterRejectedPartition
            )
        );
        Ok(())
    }

    #[test]
    fn literal_true_snapshot_keeps_as_placeholder() -> Result<(), String> {
        let (_dynamic, retained) = retained_filter(vec![column("region", 1)])?;

        let decision =
            evaluate_dynamic_partition_filter(&retained, &file_task(&[("region", "us-west")]));

        assert_eq!(
            decision,
            DeltaDynamicPartitionPruningDecision::Keep(
                DeltaDynamicPartitionKeepReason::SnapshotPlaceholder
            )
        );
        Ok(())
    }

    #[test]
    fn literal_false_snapshot_prunes() -> Result<(), String> {
        let (dynamic, retained) = retained_filter(vec![column("region", 1)])?;
        dynamic.update(lit(false)).map_err(|err| err.to_string())?;

        let decision =
            evaluate_dynamic_partition_filter(&retained, &file_task(&[("region", "us-west")]));

        assert_eq!(
            decision,
            DeltaDynamicPartitionPruningDecision::Prune(
                DeltaDynamicPartitionPruneReason::FilterRejectedPartition
            )
        );
        Ok(())
    }

    #[test]
    fn missing_partition_value_keeps_file() -> Result<(), String> {
        let (dynamic, retained) = retained_filter(vec![column("region", 1)])?;
        dynamic
            .update(Arc::new(BinaryExpr::new(
                column("region", 1),
                Operator::Eq,
                lit("us-west"),
            )))
            .map_err(|err| err.to_string())?;

        let decision = evaluate_dynamic_partition_filter(&retained, &file_task(&[]));

        assert_eq!(
            decision,
            DeltaDynamicPartitionPruningDecision::Keep(
                DeltaDynamicPartitionKeepReason::PartitionValueMissing
            )
        );
        Ok(())
    }

    #[test]
    fn unparseable_partition_value_keeps_file() -> Result<(), String> {
        let (dynamic, retained) = retained_filter(vec![column("event_year", 2)])?;
        dynamic
            .update(Arc::new(BinaryExpr::new(
                column("event_year", 2),
                Operator::Eq,
                lit(2026_i32),
            )))
            .map_err(|err| err.to_string())?;

        let decision =
            evaluate_dynamic_partition_filter(&retained, &file_task(&[("event_year", "soon")]));

        assert_eq!(
            decision,
            DeltaDynamicPartitionPruningDecision::Keep(
                DeltaDynamicPartitionKeepReason::PartitionValueUnparseable
            )
        );
        Ok(())
    }

    #[test]
    fn null_partition_value_keeps_with_sql_null_result() -> Result<(), String> {
        let (dynamic, retained) = retained_filter(vec![column("region", 1)])?;
        dynamic
            .update(Arc::new(BinaryExpr::new(
                column("region", 1),
                Operator::Eq,
                lit("us-west"),
            )))
            .map_err(|err| err.to_string())?;

        let decision = evaluate_dynamic_partition_filter(&retained, &file_task(&[("region", "")]));

        assert_eq!(
            decision,
            DeltaDynamicPartitionPruningDecision::Keep(DeltaDynamicPartitionKeepReason::NullResult)
        );
        Ok(())
    }

    #[test]
    fn non_boolean_snapshot_keeps_file() -> Result<(), String> {
        let (dynamic, retained) = retained_filter(vec![column("region", 1)])?;
        dynamic.update(lit(7_i32)).map_err(|err| err.to_string())?;

        let decision =
            evaluate_dynamic_partition_filter(&retained, &file_task(&[("region", "us-west")]));

        assert_eq!(
            decision,
            DeltaDynamicPartitionPruningDecision::Keep(
                DeltaDynamicPartitionKeepReason::NonBooleanResult
            )
        );
        Ok(())
    }
}
